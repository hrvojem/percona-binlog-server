#!/usr/bin/env python3
# Copyright (c) 2026 Percona and/or its affiliates.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License, version 2.0,
# as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License, version 2.0, for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

"""An HTTP proxy in front of an S3-compatible server that injects faults
into chosen requests, for testing how the Binlog Server's S3 storage backend
copes with them.

Requests are forwarded unchanged (the Host header included, so AWS
signatures stay valid). Fault rules are set and read through a control path
on the same port, which S3 never uses (bucket names cannot contain '_'):

  PUT /__faults__   body: {"rules": [<rule>, ...]}   replaces the rules
                    (and with "reset_traffic": true, zeroes the counters)
  GET /__faults__   returns the rules, per-rule statistics and traffic
                    counters: requests and request / response body bytes
                    per method, and the body bytes of PUT requests for
                    binlog files (object names containing "binlog.0" and
                    not ending with ".json")

A rule applies to requests whose method matches "method" (or any method for
"*") and whose path contains "match" (empty matches every path). It skips
the first "nth" - 1 matching requests, then applies "action" to the next
"count" matching requests:

  error500        answer 500 InternalError without forwarding
  error503        answer 503 SlowDown without forwarding
  reset_before    close the connection without forwarding (request lost)
  reset_after     forward, then close the connection without answering
                  (the request took effect, the client does not know)
  stall           wait "seconds", then forward
  truncate_body   forward, then send only half of the response body and
                  close the connection

Example:
  python3 s3_fault_proxy.py --listen 127.0.0.1:9100 --backend 127.0.0.1:9000
"""

import argparse
import http.client
import json
import socket
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

CONTROL_PATH = "/__faults__"
ACTIONS = {"error500", "error503", "reset_before", "reset_after", "stall",
           "truncate_body"}
HOP_BY_HOP = {"connection", "keep-alive", "proxy-connection", "te",
              "trailer", "upgrade"}

S3_ERROR = ('<?xml version="1.0" encoding="UTF-8"?>\n<Error><Code>{code}</Code>'
            '<Message>{message}</Message><RequestId>fault-proxy</RequestId>'
            '</Error>')


class FaultRules:
    def __init__(self):
        self.lock = threading.Lock()
        self.rules = []
        self.requests = 0
        self.traffic = {}

    def count(self, method, path, request_bytes, response_bytes):
        with self.lock:
            entry = self.traffic.setdefault(
                method, {"requests": 0, "request_bytes": 0,
                         "response_bytes": 0})
            entry["requests"] += 1
            entry["request_bytes"] += request_bytes
            entry["response_bytes"] += response_bytes
            object_name = path.split("?")[0].rsplit("/", 1)[-1]
            if method == "PUT" and "binlog.0" in object_name and \
                    not object_name.endswith(".json"):
                binlog = self.traffic.setdefault(
                    "binlog_puts", {"requests": 0, "request_bytes": 0})
                binlog["requests"] += 1
                binlog["request_bytes"] += request_bytes

    def reset_traffic(self):
        with self.lock:
            self.traffic = {}

    def replace(self, rules):
        for rule in rules:
            if rule.get("action") not in ACTIONS:
                raise ValueError(f"unknown action in {rule}")
        with self.lock:
            self.rules = [dict(rule, matched=0, applied=0) for rule in rules]

    def snapshot(self):
        with self.lock:
            return {"requests": self.requests, "rules": list(self.rules),
                    "traffic": dict(self.traffic)}

    def action_for(self, method, path):
        """The rule to apply to this request, if any."""
        with self.lock:
            self.requests += 1
            for rule in self.rules:
                if rule.get("method", "*") not in ("*", method):
                    continue
                if rule.get("match", "") not in path:
                    continue
                rule["matched"] += 1
                first = rule.get("nth", 1)
                if first <= rule["matched"] < first + rule.get("count", 1):
                    rule["applied"] += 1
                    return dict(rule)
            return None


def make_handler(backend_host, backend_port, rules):
    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *args):
            pass

        def _read_body(self):
            if self.headers.get("Transfer-Encoding", "").lower() == "chunked":
                body = bytearray()
                while True:
                    size_line = self.rfile.readline()
                    size = int(size_line.split(b";")[0].strip(), 16)
                    chunk = self.rfile.read(size + 2)[:size]
                    body += chunk
                    if size == 0:
                        # trailers end with an empty line
                        while self.rfile.readline() not in (b"\r\n", b"\n",
                                                            b""):
                            pass
                        return bytes(body), True
            length = int(self.headers.get("Content-Length", 0) or 0)
            return (self.rfile.read(length) if length else b""), False

        def _send(self, status, body, content_type="application/xml"):
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _reset(self):
            # an abortive close (RST) instead of an orderly shutdown
            try:
                self.connection.setsockopt(
                    socket.SOL_SOCKET, socket.SO_LINGER,
                    b"\x01\x00\x00\x00\x00\x00\x00\x00")
            except OSError:
                pass
            self.close_connection = True
            self.connection.close()

        def _control(self, body):
            if self.command == "PUT":
                try:
                    request = json.loads(body or b"{}")
                    rules.replace(request.get("rules", []))
                    if request.get("reset_traffic"):
                        rules.reset_traffic()
                except (ValueError, AttributeError) as error:
                    self._send(400, str(error).encode(), "text/plain")
                    return
            self._send(200, json.dumps(rules.snapshot()).encode(),
                       "application/json")

        def _forward(self, body, was_chunked):
            connection = http.client.HTTPConnection(backend_host, backend_port,
                                                    timeout=300)
            headers = {name: value for name, value in self.headers.items()
                       if name.lower() not in HOP_BY_HOP}
            if was_chunked:
                headers.pop("Transfer-Encoding", None)
                headers["Content-Length"] = str(len(body))
            connection.request(self.command, self.path, body=body or None,
                               headers=headers)
            response = connection.getresponse()
            data = response.read()
            connection.close()
            return response, data

        def _relay(self, response, data, truncate=False):
            self.send_response_only(response.status, response.reason)
            for name, value in response.getheaders():
                if name.lower() in HOP_BY_HOP or \
                        name.lower() == "transfer-encoding":
                    continue
                if name.lower() == "content-length":
                    continue
                self.send_header(name, value)
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            if truncate and len(data) > 1:
                self.wfile.write(data[:len(data) // 2])
                self.wfile.flush()
                self._reset()
                return
            if self.command != "HEAD":
                self.wfile.write(data)

        def _handle(self):
            body, was_chunked = self._read_body()
            if self.path.startswith(CONTROL_PATH):
                self._control(body)
                return
            rule = rules.action_for(self.command, self.path)
            action = rule["action"] if rule else None
            if action == "error500":
                self._send(500, S3_ERROR.format(
                    code="InternalError",
                    message="injected by s3_fault_proxy").encode())
                return
            if action == "error503":
                self._send(503, S3_ERROR.format(
                    code="SlowDown",
                    message="injected by s3_fault_proxy").encode())
                return
            if action == "reset_before":
                self._reset()
                return
            if action == "stall":
                time.sleep(float(rule.get("seconds", 1)))
            try:
                response, data = self._forward(body, was_chunked)
            except OSError as error:
                self._send(502, str(error).encode(), "text/plain")
                return
            rules.count(self.command, self.path, len(body), len(data))
            if action == "reset_after":
                self._reset()
                return
            self._relay(response, data, truncate=(action == "truncate_body"))

        def do_GET(self):
            self._handle()

        def do_PUT(self):
            self._handle()

        def do_POST(self):
            self._handle()

        def do_DELETE(self):
            self._handle()

        def do_HEAD(self):
            self._handle()

    return Handler


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--listen", default="127.0.0.1:9100")
    parser.add_argument("--backend", default="127.0.0.1:9000")
    args = parser.parse_args()
    listen_host, listen_port = args.listen.rsplit(":", 1)
    backend_host, backend_port = args.backend.rsplit(":", 1)
    rules = FaultRules()
    server = ThreadingHTTPServer(
        (listen_host, int(listen_port)),
        make_handler(backend_host, int(backend_port), rules))
    server.daemon_threads = True
    print(f"forwarding {args.listen} -> {args.backend}", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    sys.exit(main())
