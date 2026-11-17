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

"""Can someone on the network path change what the Binlog Server stores in,
or reads from, an S3 storage reached through an 'http://' endpoint?

With an 'http://' endpoint the S3 backend's requests are signed (SigV4), but
the signature covers the request line and some headers only: the uploads say
'x-amz-content-sha256: UNSIGNED-PAYLOAD' and carry no Content-MD5 and no
checksum header or trailer, and the downloads are not compared with anything
(ticket 78, PBS-154). This harness puts its own HTTP proxy between
'binlog_server fetch' and the local S3 server. The proxy forwards every
request with its request line and all of its headers unchanged (the Host
header included, so
the signatures stay valid) and, when a case asks for it, replaces bytes of a
body with other bytes of the same length. Cases:

- 'put_body_changed': the source's binlog holds a row with a marker value.
  In every PUT of a binlog data object the proxy replaces the marker with
  another value of the same length (and recomputes the CRC32 of that binlog
  event, which needs no secret). The case fails when the S3 server answers
  the changed uploads with a success status, 'fetch' exits 0 and the stored
  object holds the proxy's bytes instead of the source's. It records the
  headers of the changed request, the response statuses, what 'fetch'
  logged, where the stored object differs from the source's binlog, and
  whether a second, untouched 'fetch' notices anything.
- 'put_metadata_changed': the same for the binlog's '.json' metadata
  object: the proxy changes the 'size' value into another number with the
  same number of digits, three ways: the offset at which the binlog's last
  transaction starts, one byte less, one byte more. Also recorded: what the
  next, untouched 'fetch' does with that storage (it trusts the metadata:
  it cuts the binlog object down to a smaller size and resumes from there,
  and refuses to start on a larger one).
- 'get_body_changed': the storage is filled without interference, the
  source writes more, and 'fetch' resumes: it downloads the last binlog
  object to append to it. The proxy changes the marker in the body of that
  GET response. The case fails when 'fetch' exits 0 and the object it
  uploads afterwards holds the proxy's bytes.
- 'server_checks_signed_hash': not about PBS but about where the gap is:
  hand-made SigV4 PUT requests sent straight to the S3 server, one with the
  right 'x-amz-content-sha256', one whose signed hash does not match the
  body, one with UNSIGNED-PAYLOAD. If the server refuses the second one, a
  client that signed its payload would be protected, and the gap is the
  client's choice of UNSIGNED-PAYLOAD.
- 'https_endpoint': nothing is run (no TLS proxy is built). It reports from
  s3_storage_backend.cpp what protects an 'https://' endpoint: the scheme
  is taken from the URI and the SDK's certificate verification is left at
  its default (on).

A JSON line per case, then a summary; exit 1 if any case failed. Example:

  PYTHONPATH=~/ws/qa/pylib python3 s3_upload_tamper_test.py \\
    --mysql-basedir ~/ws/dist/mysql-8.4.8-linux-glibc2.28-x86_64-minimal \\
    --binsrv ~/ws/percona-binlog-server-build-debug_gcc14/binlog_server \\
    --workdir ~/ws/qa/s3-tamper --s3-bucket-dir ~/ws/s3/data/pbs-mtr
"""

import argparse
import datetime
import hashlib
import hmac
import http.client
import json
import os
import re
import shutil
import struct
import subprocess
import sys
import threading
import urllib.parse
import zlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import fetch_crash_test as harness  # noqa: E402

MARKER_ORIGINAL = b"MARKER-ROW-ORIGINAL-VALUE-0001"
MARKER_CHANGED = b"MARKER-ROW-ATTACKER-VALUE-0001"
assert len(MARKER_ORIGINAL) == len(MARKER_CHANGED)
WORKLOAD = [{"rows": [300, 5000], "ddl": True}]
BINLOG_DATA = re.compile(r"binlog\.\d{6}$")
BINLOG_METADATA = re.compile(r"binlog\.\d{6}\.json$")
SIZE_FIELD = re.compile(rb'("size"\s*:\s*)(\d+)')
HOP_BY_HOP = {"connection", "keep-alive", "proxy-connection", "te",
              "trailer", "upgrade"}
# what tells whether a body is protected: recorded for changed requests
INTEGRITY_HEADERS = ("authorization", "x-amz-content-sha256", "content-md5",
                     "content-encoding", "content-length",
                     "transfer-encoding", "expect", "x-amz-trailer",
                     "x-amz-decoded-content-length",
                     "x-amz-sdk-checksum-algorithm", "etag")


# ---------------------------------------------------------------------------
# the proxy
# ---------------------------------------------------------------------------

def object_name(path: str) -> str:
    return urllib.parse.unquote(path.split("?")[0]).rsplit("/", 1)[-1]


def integrity_headers(headers) -> dict:
    """The headers that could protect a body; of Authorization only the
    list of signed headers."""
    result = {}
    for name, value in headers:
        lower = name.lower()
        if lower == "authorization":
            signed = re.search(r"SignedHeaders=([^,\s]+)", value)
            result["signed_headers"] = signed.group(1) if signed else "?"
        elif lower in INTEGRITY_HEADERS or lower.startswith("x-amz-checksum"):
            result[lower] = value
    return result


def with_marker_changed(binlog: bytes) -> bytes:
    """The binlog with the marker row value replaced and the checksum of
    the events it is in recomputed (CRC32 of the event without its last
    four bytes: no secret is needed)."""
    data = bytearray(binlog.replace(MARKER_ORIGINAL, MARKER_CHANGED))
    try:
        events = harness.parse_events(bytes(data))
    except harness.Failure:
        return bytes(data)
    for offset, _, size in events:
        if MARKER_CHANGED in data[offset:offset + size]:
            end = offset + size - harness.CHECKSUM_SIZE
            struct.pack_into("<I", data, end, zlib.crc32(data[offset:end]))
    return bytes(data)


def events_with_bad_checksum(binlog: bytes) -> int:
    bad = 0
    for offset, _, size in harness.parse_events(binlog):
        end = offset + size - harness.CHECKSUM_SIZE
        if struct.unpack_from("<I", binlog, end)[0] != \
                zlib.crc32(binlog[offset:end]):
            bad += 1
    return bad


class Tamper:
    """What the proxy changes at the moment, and what it saw."""

    def __init__(self):
        self.lock = threading.Lock()
        self.request_change = None    # (method, path, body) -> body | None
        self.response_change = None   # (method, path, status, body) -> ...
        self.changed = []
        self.requests = 0
        self.transfer_encodings = set()
        self.last_binlog_body = b""
        self.wire_log = None

    def wire(self, handler, body: bytes, response, data: bytes):
        """--wire-log: every request and response as the proxy saw them
        (headers in full, of the bodies only the length)."""
        if self.wire_log is None:
            return
        text = [f"{handler.command} {handler.path}  body {len(body)} bytes"]
        text += [f"  > {name}: {value}"
                 for name, value in handler.headers.items()]
        text.append(f"  < {response.status} {response.reason}  "
                    f"body {len(data)} bytes")
        text += [f"  < {name}: {value}"
                 for name, value in response.getheaders()]
        with self.lock:
            self.wire_log.write("\n".join(text) + "\n\n")
            self.wire_log.flush()

    def start(self, request_change=None, response_change=None):
        with self.lock:
            self.request_change = request_change
            self.response_change = response_change
            self.changed = []
            self.requests = 0

    def stop(self) -> list:
        with self.lock:
            self.request_change = self.response_change = None
            return list(self.changed)


def make_handler(backend_host: str, backend_port: int, tamper: Tamper):
    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *args):
            pass

        def _read_body(self):
            """The body as sent, and whether it came chunked. An 'Expect:
            100-continue' was already answered by the base class."""
            encoding = self.headers.get("Transfer-Encoding", "").lower()
            if encoding:
                tamper.transfer_encodings.add(encoding)
            if encoding == "chunked":
                body = bytearray()
                while True:
                    size = int(self.rfile.readline().split(b";")[0].strip(),
                               16)
                    body += self.rfile.read(size + 2)[:size]
                    if size == 0:
                        return bytes(body), True
            length = int(self.headers.get("Content-Length", 0) or 0)
            return (self.rfile.read(length) if length else b""), False

        def _forward(self, body: bytes, was_chunked: bool):
            connection = http.client.HTTPConnection(backend_host,
                                                    backend_port, timeout=300)
            # the request line and every end-to-end header as received:
            # whatever is signed stays as the client signed it
            headers = {name: value for name, value in self.headers.items()
                       if name.lower() not in HOP_BY_HOP and
                       name.lower() != "expect"}
            if was_chunked:
                headers.pop("Transfer-Encoding", None)
                headers["Content-Length"] = str(len(body))
            connection.request(self.command, self.path, body=body or None,
                               headers=headers)
            response = connection.getresponse()
            data = response.read()
            connection.close()
            return response, data

        def _handle(self):
            body, was_chunked = self._read_body()
            name = object_name(self.path)
            with tamper.lock:
                tamper.requests += 1
                request_change = tamper.request_change
                response_change = tamper.response_change
            if self.command == "PUT" and BINLOG_DATA.search(name):
                tamper.last_binlog_body = body
            record = None
            if request_change is not None:
                changed = request_change(self.command, self.path, body)
                if changed is not None and changed != body:
                    assert len(changed) == len(body)
                    record = {
                        "method": self.command, "object": name,
                        "body_bytes": len(body),
                        "bytes_changed": sum(a != b for a, b in
                                             zip(body, changed)),
                        "request_headers":
                            integrity_headers(self.headers.items())}
                    body = changed
            try:
                response, data = self._forward(body, was_chunked)
            except OSError as error:
                data = str(error).encode()
                self.send_response(502)
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)
                return
            tamper.wire(self, body, response, data)
            if record is not None:
                record["response_status"] = response.status
            if response_change is not None:
                changed = response_change(self.command, self.path,
                                          response.status, data)
                if changed is not None and changed != data:
                    assert len(changed) == len(data)
                    record = {
                        "method": self.command, "object": name,
                        "range": self.headers.get("Range"),
                        "response_status": response.status,
                        "body_bytes": len(data),
                        "bytes_changed": sum(a != b for a, b in
                                             zip(data, changed)),
                        "response_headers":
                            integrity_headers(response.getheaders())}
                    data = changed
            if record is not None:
                with tamper.lock:
                    tamper.changed.append(record)
            self.send_response_only(response.status, response.reason)
            for header, value in response.getheaders():
                if header.lower() in HOP_BY_HOP or header.lower() in (
                        "transfer-encoding", "content-length"):
                    continue
                self.send_header(header, value)
            if self.command == "HEAD":
                # no body, but the object's own length
                self.send_header("Content-Length",
                                 response.getheader("Content-Length", "0"))
            else:
                self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            if self.command != "HEAD":
                self.wfile.write(data)

        do_GET = do_PUT = do_POST = do_DELETE = do_HEAD = _handle

    return Handler


def start_proxy(listen_port: int, backend: str, tamper: Tamper):
    backend_host, backend_port = backend.rsplit(":", 1)
    server = ThreadingHTTPServer(
        ("127.0.0.1", listen_port),
        make_handler(backend_host, int(backend_port), tamper))
    server.daemon_threads = True
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server


# ---------------------------------------------------------------------------
# helpers of the cases
# ---------------------------------------------------------------------------

def run_fetch(binsrv) -> dict:
    """Runs 'fetch' to its end; its exit code and what it logged at warning
    level or above."""
    binsrv.log_path.unlink(missing_ok=True)
    process = binsrv.start_fetch()
    try:
        _, stderr = process.communicate(
            timeout=harness.FETCH_TIMEOUT_SECONDS)
    except subprocess.TimeoutExpired:
        process.kill()
        _, stderr = process.communicate()
    lines = binsrv.log_path.read_text(errors="replace").splitlines() \
        if binsrv.log_path.exists() else []
    notable = [line[:300] for line in lines
               if re.search(r"\[ *(warning|error|fatal)\]", line)]
    return {"exit": process.returncode,
            "log_lines": len(lines),
            "log_warnings_and_errors": notable[:8],
            "log_last_line": lines[-1][:200] if lines else "",
            "stderr": stderr.decode(errors="replace").strip()[-300:]}


def marker_workload(machine):
    machine.run_workload(WORKLOAD)
    harness.SERVER.sql("INSERT INTO qa.t (payload) VALUES "
                       f"('{MARKER_ORIGINAL.decode()}')")
    machine.run_workload([{"rows": [200], "ddl": False}])


def differences(stored: bytes, source: bytes) -> dict:
    positions = [index for index, (a, b) in enumerate(zip(stored, source))
                 if a != b]
    return {"stored_bytes": len(stored), "source_bytes": len(source),
            "differing_bytes": len(positions) + abs(len(stored) -
                                                    len(source)),
            "first_difference_at": positions[0] if positions else None,
            "last_difference_at": positions[-1] if positions else None}


def only_binlog() -> str:
    names = harness.SERVER.binlog_names()
    if len(names) != 1:
        raise harness.Failure(f"expected one binlog on the source: {names}")
    return names[0]


def describe_object(stored: bytes, source: bytes) -> dict:
    result = differences(stored, source)
    result.update({
        "holds_changed_marker": MARKER_CHANGED in stored,
        "holds_original_marker": MARKER_ORIGINAL in stored,
        "events_with_bad_checksum": events_with_bad_checksum(stored)})
    return result


# ---------------------------------------------------------------------------
# the cases
# ---------------------------------------------------------------------------

def case_put_body_changed(binsrv, tamper: Tamper) -> dict:
    machine = harness.FetchCrashMachine()
    machine.start_from_scratch()
    marker_workload(machine)
    name = only_binlog()

    def change(method, path, body):
        if method == "PUT" and BINLOG_DATA.search(object_name(path)) and \
                MARKER_ORIGINAL in body:
            return with_marker_changed(body)
        return None

    tamper.start(request_change=change)
    try:
        fetch = run_fetch(binsrv)
    finally:
        changed = tamper.stop()
    source = harness.SERVER.binlog_as_sent(name)
    stored_path = binsrv.storage / name
    stored = stored_path.read_bytes() if stored_path.exists() else b""
    result = {"case": "put_body_changed", "binlog": name,
              "changed": f"{MARKER_ORIGINAL.decode()} -> "
                         f"{MARKER_CHANGED.decode()} in the body of every "
                         "PUT of the binlog data object, and that event's "
                         "CRC32",
              "puts_changed": len(changed),
              "response_statuses": sorted({item["response_status"]
                                           for item in changed}),
              "changed_request": changed[-1] if changed else None,
              "fetch": fetch,
              "stored_object": describe_object(stored, source)}
    # does anything notice later? an untouched fetch on the same storage
    again = run_fetch(binsrv)
    stored_again = stored_path.read_bytes() if stored_path.exists() else b""
    result["second_fetch_untouched"] = {
        "exit": again["exit"],
        "log_warnings_and_errors": again["log_warnings_and_errors"],
        "stored_object_still_changed": MARKER_CHANGED in stored_again}
    accepted = bool(changed) and all(200 <= item["response_status"] < 300
                                     for item in changed)
    result["upload_accepted"] = accepted
    if not changed:
        result["ok"] = False
        result["why"] = "the proxy saw no PUT of a binlog object with the " \
                        "marker: nothing was tested"
    elif accepted and fetch["exit"] == 0 and MARKER_CHANGED in stored and \
            MARKER_ORIGINAL not in stored:
        result["ok"] = False
        result["why"] = (
            f"the S3 server accepted {len(changed)} upload(s) of {name} "
            "whose body was changed on the way (status "
            f"{result['response_statuses']}), fetch exited 0 and logged "
            "nothing about it, and the stored binlog holds the changed row "
            f"value ({result['stored_object']['differing_bytes']} bytes "
            "differ from the source's binlog)")
    else:
        result["ok"] = True
    return result


def metadata_try(binsrv, tamper: Tamper, how: str) -> dict:
    """A fetch whose binlog metadata uploads get another 'size', then an
    untouched fetch on the storage that leaves behind."""
    machine = harness.FetchCrashMachine()
    machine.start_from_scratch()
    marker_workload(machine)
    name = only_binlog()
    sizes = {}

    def other_size(size: int) -> int:
        """Another size with as many digits."""
        candidates = {"one_byte_less": [size - 1],
                      "one_byte_more": [size + 1, size]}.get(how, [])
        if how == "last_transaction_start":
            # where the last transaction starts in the binlog the proxy
            # saw going by just before
            try:
                candidates = [
                    offset for offset, type_code, _ in
                    harness.parse_events(tamper.last_binlog_body)
                    if type_code in harness.TRANSACTION_START_EVENTS and
                    offset < size][-1:]
            except harness.Failure:
                pass
            candidates.append(size - 1)
        for candidate in candidates:
            if len(str(candidate)) == len(str(size)):
                return candidate
        return size

    def change(method, path, body):
        if method != "PUT" or not BINLOG_METADATA.search(object_name(path)):
            return None
        match = SIZE_FIELD.search(body)
        if match is None:
            return None
        size = int(match.group(2))
        sizes[size] = other_size(size)
        return (body[:match.start(2)] + str(sizes[size]).encode() +
                body[match.end(2):])

    tamper.start(request_change=change)
    try:
        fetch = run_fetch(binsrv)
    finally:
        changed = tamper.stop()
    source = harness.SERVER.binlog_as_sent(name)
    read = lambda path: path.read_bytes() if path.exists() else b""
    metadata = read(binsrv.storage / (name + ".json"))
    stored_size = json.loads(metadata).get("size") if metadata else None
    result = {
        "object": name + ".json", "puts_changed": len(changed),
        "response_statuses": sorted({item["response_status"]
                                     for item in changed}),
        "changed_request": changed[-1] if changed else None,
        "sizes_sent_and_forwarded": {str(key): value for key, value
                                     in sorted(sizes.items())},
        "fetch": fetch, "source_binlog_bytes": len(source),
        "stored_binlog_bytes": len(read(binsrv.storage / name)),
        "stored_metadata_size": stored_size,
        "upload_accepted": bool(changed) and all(
            200 <= item["response_status"] < 300 for item in changed)}
    result["accepted_unnoticed"] = (
        result["upload_accepted"] and fetch["exit"] == 0 and
        stored_size not in (None, len(source)))
    # what the wrong size does to the next start: an untouched fetch
    again = run_fetch(binsrv)
    stored_again = read(binsrv.storage / name)
    result["second_fetch_untouched"] = {
        "exit": again["exit"],
        "log_warnings_and_errors": again["log_warnings_and_errors"],
        "log_last_line": again["log_last_line"],
        "stderr": again["stderr"],
        "stored_binlog_bytes": len(stored_again),
        "stored_binlog_equals_source": stored_again == source}
    return result


def case_put_metadata_changed(binsrv, tamper: Tamper) -> dict:
    result = {"case": "put_metadata_changed",
              "changed": "the 'size' value in the body of every PUT of the "
                         "binlog's metadata object, into another number "
                         "with the same number of digits"}
    tries = {how: metadata_try(binsrv, tamper, how)
             for how in ("last_transaction_start", "one_byte_less",
                         "one_byte_more")}
    result["sizes"] = tries
    if not all(item["puts_changed"] for item in tries.values()):
        result["ok"] = False
        result["why"] = "the proxy saw no PUT of a binlog metadata " \
                        "object: nothing was tested"
        return result
    unnoticed = [how for how, item in tries.items()
                 if item["accepted_unnoticed"]]
    result["ok"] = not unnoticed
    if unnoticed:
        first = tries[unnoticed[0]]
        later = "; ".join(
            f"{how}: exit {item['second_fetch_untouched']['exit']}, the "
            f"binlog object is then "
            f"{item['second_fetch_untouched']['stored_binlog_bytes']} bytes"
            + ("" if item["second_fetch_untouched"]
               ["stored_binlog_equals_source"] else
               " and differs from the source's")
            for how, item in tries.items())
        result["why"] = (
            f"the S3 server accepted the uploads of {first['object']} whose "
            "'size' was changed on the way (status "
            f"{first['response_statuses']}), fetch exited 0, and the stored "
            f"metadata says size {first['stored_metadata_size']} for a "
            f"binlog of {first['source_binlog_bytes']} bytes "
            f"({', '.join(unnoticed)}). The next, untouched fetch: {later}")
    return result


def case_get_body_changed(binsrv, tamper: Tamper) -> dict:
    machine = harness.FetchCrashMachine()
    machine.start_from_scratch()
    marker_workload(machine)
    name = only_binlog()
    first = run_fetch(binsrv)
    stored_path = binsrv.storage / name
    if first["exit"] != 0 or not stored_path.exists() or \
            MARKER_ORIGINAL not in stored_path.read_bytes():
        raise harness.Failure("the untouched first fetch did not store "
                              f"the binlog: {first}")
    # more on the source, so that the resumed fetch appends and uploads
    machine.run_workload([{"rows": [150], "ddl": False}])

    def change(method, path, status, data):
        if method == "GET" and status in (200, 206) and \
                BINLOG_DATA.search(object_name(path)) and \
                MARKER_ORIGINAL in data:
            return with_marker_changed(data)
        return None

    tamper.start(response_change=change)
    try:
        fetch = run_fetch(binsrv)
    finally:
        changed = tamper.stop()
    source = harness.SERVER.binlog_as_sent(name)
    stored = stored_path.read_bytes()
    result = {"case": "get_body_changed", "binlog": name,
              "changed": f"{MARKER_ORIGINAL.decode()} -> "
                         f"{MARKER_CHANGED.decode()} in the body of the GET "
                         "response with which the resumed fetch downloads "
                         "the binlog object, and that event's CRC32",
              "gets_changed": len(changed),
              "changed_response": changed[-1] if changed else None,
              "fetch": fetch,
              "stored_object_after": describe_object(stored, source)}
    if not changed:
        result["ok"] = False
        result["why"] = "the proxy saw no GET of the binlog object with " \
                        "the marker: nothing was tested"
    elif fetch["exit"] == 0 and MARKER_CHANGED in stored:
        result["ok"] = False
        result["why"] = (
            "the resumed fetch took the binlog object whose GET response "
            "body was changed on the way, appended to it, exited 0 and "
            "uploaded it: the stored binlog now holds the changed row "
            f"value ({result['stored_object_after']['differing_bytes']} "
            "bytes differ from the source's binlog)")
    else:
        result["ok"] = True
    return result


def signed_request(endpoint: str, method: str, path: str, body: bytes,
                   declared_hash: str, access_key: str, secret_key: str
                   ) -> "tuple[int, str]":
    """One hand-made SigV4 request; (status, start of the answer)."""
    now = datetime.datetime.now(datetime.timezone.utc)
    amz_date = now.strftime("%Y%m%dT%H%M%SZ")
    day = now.strftime("%Y%m%d")
    region, service = "us-east-1", "s3"
    headers = {"host": endpoint, "x-amz-content-sha256": declared_hash,
               "x-amz-date": amz_date}
    signed_headers = ";".join(sorted(headers))
    canonical = "\n".join([
        method, urllib.parse.quote(path), "",
        "".join(f"{name}:{headers[name]}\n" for name in sorted(headers)),
        signed_headers, declared_hash])
    scope = f"{day}/{region}/{service}/aws4_request"
    to_sign = "\n".join(["AWS4-HMAC-SHA256", amz_date, scope,
                         hashlib.sha256(canonical.encode()).hexdigest()])
    key = ("AWS4" + secret_key).encode()
    for part in (day, region, service, "aws4_request"):
        key = hmac.new(key, part.encode(), hashlib.sha256).digest()
    signature = hmac.new(key, to_sign.encode(), hashlib.sha256).hexdigest()
    headers["Authorization"] = (
        f"AWS4-HMAC-SHA256 Credential={access_key}/{scope}, "
        f"SignedHeaders={signed_headers}, Signature={signature}")
    host, port = endpoint.rsplit(":", 1)
    connection = http.client.HTTPConnection(host, int(port), timeout=30)
    try:
        connection.request(method, urllib.parse.quote(path),
                           body=body or None, headers=headers)
        response = connection.getresponse()
        answer = response.read().decode(errors="replace")
    finally:
        connection.close()
    code = re.search(r"<Code>([^<]+)</Code>", answer)
    return response.status, code.group(1) if code else answer[:80]


def case_server_checks_signed_hash(args, prefix: str) -> dict:
    body = b"the body that was signed\n"
    other = b"the body that was SENT..\n"
    key = f"/{args.s3_bucket}/{prefix}/signed-hash-check"
    send = lambda method, payload, declared: signed_request(
        args.s3_endpoint, method, key, payload, declared,
        args.s3_access_key, args.s3_secret_key)
    sha = lambda data: hashlib.sha256(data).hexdigest()
    result = {"case": "server_checks_signed_hash",
              "sent_to": args.s3_endpoint + " (not through the proxy)"}
    try:
        right = send("PUT", body, sha(body))
        wrong = send("PUT", other, sha(body))
        object_path = args.s3_bucket_dir.expanduser() / prefix / \
            "signed-hash-check"
        after_wrong = object_path.read_bytes() \
            if object_path.exists() else None
        unsigned = send("PUT", other, "UNSIGNED-PAYLOAD")
        after_unsigned = object_path.read_bytes() \
            if object_path.exists() else None
        removed = send("DELETE", b"", sha(b""))
    except OSError as error:
        return dict(result, ok=False, why=f"request failed: {error}")
    result.update({
        "signed_hash_matches_body": {"status": right[0], "code": right[1]},
        "signed_hash_of_another_body": {
            "status": wrong[0], "code": wrong[1],
            "object_replaced": after_wrong == other},
        "unsigned_payload": {"status": unsigned[0], "code": unsigned[1],
                             "object_replaced": after_unsigned == other},
        "delete": {"status": removed[0]}})
    rejects = not 200 <= wrong[0] < 300 and after_wrong != other
    result["server_rejects_body_not_matching_signed_hash"] = rejects
    if not 200 <= right[0] < 300:
        result["ok"] = False
        result["why"] = ("the hand-made signed PUT was not accepted "
                         f"({right}): the check says nothing")
    else:
        # information about the S3 server, not a check of PBS
        result["ok"] = True
        result["conclusion"] = (
            "the server compares a signed x-amz-content-sha256 with the "
            "body and refuses a mismatch; it accepts any body with "
            "UNSIGNED-PAYLOAD: the uploads are unprotected because the "
            "client sends UNSIGNED-PAYLOAD" if rejects else
            "the server accepted a body that does not match the signed "
            "x-amz-content-sha256: this server would not protect a signed "
            "payload either")
    return result


def case_https_endpoint() -> dict:
    source = Path(__file__).resolve().parents[2] / "src" / "binsrv" / \
        "s3_storage_backend.cpp"
    result = {"case": "https_endpoint", "run": False,
              "from_code": str(source.name)}
    if not source.exists():
        return dict(result, ok=True, skipped="the source file is not there")
    lines = source.read_text().splitlines()
    find = lambda needle: [number for number, line in enumerate(lines, 1)
                           if needle in line]
    result.update({
        "scheme_set_at_lines": find("configuration_.scheme ="),
        "lines_touching_verifySSL": find("verifySSL"),
        "lines_touching_ca_settings": find("caFile") + find("caPath"),
        "statement": (
            "Not run. The endpoint constructor sets the scheme to HTTPS "
            "for an 'https://' URI and changes nothing else of the SDK's "
            "ClientConfiguration: verifySSL keeps its default (true) and "
            "the system trust store is used, so the body is protected by "
            "TLS on the way (an earlier test showed that a self-signed "
            "endpoint is refused). The requests themselves are the same: "
            "UNSIGNED-PAYLOAD, no checksum; whoever terminates the TLS "
            "connection (a load balancer, a gateway) can still change a "
            "body unnoticed.")})
    result["ok"] = True
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--mysql-basedir", type=Path, required=True)
    parser.add_argument("--binsrv", type=Path, required=True)
    parser.add_argument("--workdir", type=Path, required=True)
    parser.add_argument("--port", type=int, default=23580)
    parser.add_argument("--mode", choices=["gtid", "position"],
                        default="position")
    parser.add_argument("--proxy-port", type=int,
                        help="where the harness's own proxy listens "
                             "(default: --port + 1)")
    parser.add_argument("--s3-endpoint", default="127.0.0.1:9000")
    parser.add_argument("--s3-bucket", default="pbs-mtr")
    parser.add_argument("--s3-bucket-dir", type=Path, required=True,
                        help="the bucket's directory on the local S3 server")
    parser.add_argument("--s3-access-key", default="pbsaccesskey")
    parser.add_argument("--s3-secret-key", default="pbs/secret+key=1")
    parser.add_argument("--wire-log", type=Path,
                        help="write every request and response the proxy "
                             "forwards (headers only) to this file")
    parser.add_argument("--only", action="append", metavar="CASE",
                        help="run only this case (repeatable); default: all")
    args = parser.parse_args()
    args.workdir.mkdir(parents=True, exist_ok=True)
    gtid_mode = args.mode == "gtid"
    proxy_port = args.proxy_port or args.port + 1
    prefix = f"s3-upload-tamper-{os.getpid()}"
    tamper = Tamper()
    if args.wire_log is not None:
        tamper.wire_log = args.wire_log.open("w")
    proxy = start_proxy(proxy_port, args.s3_endpoint, tamper)
    # binlog_server talks to the proxy; the objects are read as files
    s3 = harness.S3Storage(f"127.0.0.1:{proxy_port}", args.s3_bucket,
                           args.s3_bucket_dir.expanduser(),
                           args.s3_access_key, args.s3_secret_key, prefix)
    harness.SERVER = harness.MySQLServer(args.mysql_basedir.expanduser(),
                                         args.workdir / "mysql", args.port,
                                         gtid_mode, server_id=1)
    harness.BINSRV = harness.BinlogServer(args.binsrv.expanduser(),
                                          args.workdir / "binsrv", args.port,
                                          gtid_mode, s3)
    binsrv = harness.BINSRV
    cases = (
        ("put_body_changed", lambda: case_put_body_changed(binsrv, tamper)),
        ("put_metadata_changed",
         lambda: case_put_metadata_changed(binsrv, tamper)),
        ("get_body_changed", lambda: case_get_body_changed(binsrv, tamper)),
        ("server_checks_signed_hash",
         lambda: case_server_checks_signed_hash(args, prefix)),
        ("https_endpoint", case_https_endpoint))
    results = []
    harness.SERVER.start()
    try:
        for name, case in cases:
            if args.only and name not in args.only:
                continue
            try:
                result = case()
            except (harness.Failure, OSError, RuntimeError) as failure:
                result = {"case": name, "ok": False,
                          "why": harness.describe_failure(failure, 600)}
            finally:
                tamper.stop()
            print(json.dumps(result), flush=True)
            results.append(result)
    finally:
        harness.SERVER.stop()
        proxy.shutdown()
        shutil.rmtree(binsrv.storage, ignore_errors=True)
    failures = [result for result in results if not result.get("ok")]
    print(json.dumps({"cases": len(results), "failures": len(failures),
                      "passed": not failures}), flush=True)
    (args.workdir / "result.json").write_text(json.dumps(results, indent=2))
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
