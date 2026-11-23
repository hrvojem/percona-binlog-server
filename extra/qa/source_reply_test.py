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

"""Odd answers from the source to the queries 'fetch' sends before it
replicates.

Before it starts the binlog dump, the Binlog Server sends the source
'SET @source_binlog_checksum = ...' (src/easymysql/connection.cpp,
set_binlog_checksum) and, in GTID mode on an empty storage,
'SELECT @@GLOBAL.gtid_purged' (src/operations/collector_context.cpp). A real
server answers both, so the error paths after them (a failed query, a result
that is not one non-NULL value, a binlog dump that cannot be opened) are not
reached by any other test. A proxy between 'fetch' and a real MySQL source
passes everything through except what a case changes: it answers the query
itself instead of forwarding it, or resets the connection.

Cases (one JSON line each, then a summary):
  control                  nothing is changed: fetch exits 0 with the
                           source's transactions stored;
  checksum_query_error     the SET query gets an ERR packet (1227, access
                           denied);
  gtid_purged_error        (GTID mode) SELECT @@GLOBAL.gtid_purged gets an
                           ERR packet;
  gtid_purged_no_rows      ... a result set without rows;
  gtid_purged_two_rows     ... two rows;
  gtid_purged_two_columns  ... one row of two columns;
  gtid_purged_null         ... one NULL value;
  gtid_purged_garbage      ... one value that is not a GTID set;
  binlog_open_reset        the connection is reset (RST) right after the SET
                           query's answer, so sending the binlog dump command
                           fails.
For every case but 'control', fetch must exit with 1 (no signal, no
timeout), store no transaction, and its log must name the reason: the
server's error message or the problem with the result. Whether the reason
is logged is a separate field ('reason_logged'); the case fails without it
(ticket 47, PBS-121, describes the same gap for a failed connect).

Position mode runs the cases without a GTID query; GTID mode runs all.
Example:

  PYTHONPATH=~/ws/qa/pylib python3 source_reply_test.py \\
    --mysql-basedir ~/ws/dist/mysql-8.4.8-linux-glibc2.28-x86_64-minimal \\
    --binsrv ~/ws/percona-binlog-server-build-debug_gcc14/binlog_server \\
    --workdir ~/ws/qa/source-reply --mode gtid
"""

import argparse
import json
import socket
import struct
import subprocess
import sys
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import fetch_crash_test as harness  # noqa: E402

COM_QUERY = 0x03
COM_BINLOG_DUMP = 0x12
COM_BINLOG_DUMP_GTID = 0x1E
CLIENT_DEPRECATE_EOF = 1 << 24
SERVER_STATUS_AUTOCOMMIT = 0x0002
TYPE_VAR_STRING = 0xFD
FETCH_TIMEOUT = 120
# a few transactions of a few rows, then a rotation
WORKLOAD = [{"rows": [20, 30], "ddl": False}, {"rows": [10], "ddl": True}]

CASES = {
    # name: (query prefix it answers, answer, GTID mode only)
    "control": (None, None, False),
    "checksum_query_error": ("SET @source_binlog_checksum", "error", False),
    "gtid_purged_error": ("SELECT @@GLOBAL.gtid_purged", "error", True),
    "gtid_purged_no_rows": ("SELECT @@GLOBAL.gtid_purged", "no_rows", True),
    "gtid_purged_two_rows": ("SELECT @@GLOBAL.gtid_purged", "two_rows", True),
    "gtid_purged_two_columns": ("SELECT @@GLOBAL.gtid_purged", "two_columns",
                                True),
    "gtid_purged_null": ("SELECT @@GLOBAL.gtid_purged", "null", True),
    "gtid_purged_garbage": ("SELECT @@GLOBAL.gtid_purged", "garbage", True),
    "binlog_open_reset": ("SET @source_binlog_checksum", "reset_after", False),
}
ERROR_CODE = 1227
ERROR_MESSAGE = (b"Access denied; you need (at least one of) the SUPER "
                 b"privilege(s) for this operation (from source_reply_test)")
# what the log must contain for the reason to count as logged
REASON = {
    "error": ["Access denied", "1227"],
    "no_rows": ["exactly one row"],
    "two_rows": ["exactly one row"],
    "two_columns": ["exactly one column"],
    "null": ["NULL"],
    "garbage": ["gtid", "GTID"],
    "reset_after": ["binary log", "Lost connection", "reset", "Broken pipe"],
}


def lenenc_int(value: int) -> bytes:
    if value < 251:
        return bytes([value])
    if value < 1 << 16:
        return b"\xfc" + struct.pack("<H", value)
    if value < 1 << 24:
        return b"\xfd" + struct.pack("<I", value)[:3]
    return b"\xfe" + struct.pack("<Q", value)


def lenenc_str(value: bytes) -> bytes:
    return lenenc_int(len(value)) + value


def packet(sequence: int, payload: bytes) -> bytes:
    return struct.pack("<I", len(payload))[:3] + bytes([sequence & 0xFF]) + \
        payload


def column_definition(name: bytes) -> bytes:
    return (lenenc_str(b"def") + lenenc_str(b"") + lenenc_str(b"") +
            lenenc_str(b"") + lenenc_str(name) + lenenc_str(name) +
            b"\x0c" + struct.pack("<HIBHB", 0x21, 1024, TYPE_VAR_STRING, 0,
                                  0) + b"\x00\x00")


def eof_or_ok(deprecate_eof: bool) -> bytes:
    if deprecate_eof:
        # an OK packet with the 0xFE header ends a result set
        return b"\xfe" + lenenc_int(0) + lenenc_int(0) + \
            struct.pack("<HH", SERVER_STATUS_AUTOCOMMIT, 0)
    return b"\xfe" + struct.pack("<HH", 0, SERVER_STATUS_AUTOCOMMIT)


def result_set(columns: list, rows: list, deprecate_eof: bool) -> list:
    """The payloads of a text result set; a row value of None is NULL."""
    payloads = [lenenc_int(len(columns))]
    payloads += [column_definition(name) for name in columns]
    if not deprecate_eof:
        payloads.append(eof_or_ok(False))
    for row in rows:
        payloads.append(b"".join(b"\xfb" if value is None else
                                 lenenc_str(value) for value in row))
    payloads.append(eof_or_ok(deprecate_eof))
    return payloads


def answer(kind: str, deprecate_eof: bool, real_purged: bytes) -> list:
    if kind == "error":
        return [b"\xff" + struct.pack("<H", ERROR_CODE) + b"#42000" +
                ERROR_MESSAGE]
    column = [b"@@GLOBAL.gtid_purged"]
    if kind == "no_rows":
        return result_set(column, [], deprecate_eof)
    if kind == "two_rows":
        return result_set(column, [[real_purged], [real_purged]],
                          deprecate_eof)
    if kind == "two_columns":
        return result_set(column + [b"extra"], [[real_purged, b"x"]],
                          deprecate_eof)
    if kind == "null":
        return result_set(column, [[None]], deprecate_eof)
    if kind == "garbage":
        return result_set(column, [[b"this is not a GTID set"]],
                          deprecate_eof)
    raise ValueError(kind)


class ReplyProxy:
    """A TCP proxy in front of the source that answers one query itself
    (or resets the connection after its answer); everything else passes."""

    def __init__(self, listen_port: int, source_port: int, prefix,
                 kind, real_purged: bytes):
        self.source_port = source_port
        self.prefix = prefix.encode() if prefix else None
        self.kind = kind
        self.real_purged = real_purged
        self.answered = 0
        self.dump_received = False
        self.queries = []
        self.listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.listener.bind(("127.0.0.1", listen_port))
        self.listener.listen(4)
        self.thread = threading.Thread(target=self._accept, daemon=True)
        self.thread.start()

    def close(self):
        for action in (lambda: self.listener.shutdown(socket.SHUT_RDWR),
                       self.listener.close):
            try:
                action()
            except OSError:
                pass
        self.thread.join(timeout=5)

    def _accept(self):
        while True:
            try:
                client, _ = self.listener.accept()
            except OSError:
                return
            server = socket.create_connection(("127.0.0.1", self.source_port))
            session = {"deprecate_eof": False, "reset_pending": False,
                       "lock": threading.Lock()}
            threading.Thread(target=self._client_to_server,
                             args=(client, server, session),
                             daemon=True).start()
            threading.Thread(target=self._server_to_client,
                             args=(server, client, session),
                             daemon=True).start()

    @staticmethod
    def _read_packet(sock: socket.socket):
        header = b""
        while len(header) < 4:
            try:
                piece = sock.recv(4 - len(header))
            except OSError:
                return None
            if not piece:
                return None
            header += piece
        length = header[0] | header[1] << 8 | header[2] << 16
        payload = b""
        while len(payload) < length:
            try:
                piece = sock.recv(length - len(payload))
            except OSError:
                return None
            if not piece:
                return None
            payload += piece
        return header, payload

    @staticmethod
    def _shutdown(*sockets):
        for sock in sockets:
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass

    @staticmethod
    def _reset(sock: socket.socket):
        """Closes 'sock' with an RST instead of a FIN."""
        try:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER,
                            struct.pack("ii", 1, 0))
            sock.close()
        except OSError:
            pass

    def _client_to_server(self, client, server, session):
        first = True
        try:
            while True:
                read = self._read_packet(client)
                if read is None:
                    break
                header, payload = read
                if first and header[3] == 1 and len(payload) >= 4:
                    # the handshake response: the client's capability flags
                    capabilities = struct.unpack_from("<I", payload, 0)[0]
                    session["deprecate_eof"] = bool(
                        capabilities & CLIENT_DEPRECATE_EOF)
                    first = False
                if header[3] == 0 and payload:
                    command = payload[0]
                    if command in (COM_BINLOG_DUMP, COM_BINLOG_DUMP_GTID):
                        self.dump_received = True
                    if command == COM_QUERY:
                        # with CLIENT_QUERY_ATTRIBUTES (MySQL 8 clients) the
                        # text follows the parameter count and the number of
                        # parameter sets: b"\x00\x01" for a plain query
                        text = payload[1:]
                        start = 2 if text[:2] == b"\x00\x01" else 0
                        text = text[start:]
                        self.queries.append(text[:80].decode(errors="replace"))
                        if self.prefix and text.startswith(self.prefix):
                            if self.kind == "reset_after":
                                session["reset_pending"] = True
                            else:
                                self.answered += 1
                                with session["lock"]:
                                    for index, reply in enumerate(answer(
                                            self.kind, session["deprecate_eof"],
                                            self.real_purged)):
                                        client.sendall(packet(index + 1, reply))
                                continue
                server.sendall(header + payload)
        finally:
            self._shutdown(client, server)

    def _server_to_client(self, server, client, session):
        try:
            while True:
                read = self._read_packet(server)
                if read is None:
                    break
                header, payload = read
                with session["lock"]:
                    client.sendall(header + payload)
                if session["reset_pending"] and payload and \
                        payload[0] in (0x00, 0xFF):
                    # the answer to the SET query went out: reset now, so the
                    # binlog dump command the client sends next fails
                    self.answered += 1
                    self._reset(client)
                    break
        finally:
            self._shutdown(server)


def stored_transactions(binsrv) -> int:
    total = 0
    for name in binsrv.indexed_binlogs():
        path = binsrv.storage / name
        if path.exists():
            total += len(harness.transactions_of(path.read_bytes()))
    return total


def run_case(args, name: str, source, real_purged: bytes) -> dict:
    prefix, kind, _ = CASES[name]
    binsrv = harness.BINSRV
    binsrv.reset()
    binsrv.write_config()
    config = json.loads(binsrv.config_path.read_text())
    proxy_port = args.port + 5
    config["connection"]["port"] = proxy_port
    # the proxy reads the queries, so the connection stays plain
    config["connection"]["ssl"] = {"mode": "disabled"}
    config["connection"]["connect_timeout"] = 5
    binsrv.config_path.write_text(json.dumps(config, indent=2))
    proxy = ReplyProxy(proxy_port, source.port, prefix, kind, real_purged)
    started = time.monotonic()
    try:
        try:
            completed = subprocess.run(
                [str(binsrv.binary), "fetch", str(binsrv.config_path)],
                capture_output=True, text=True, timeout=FETCH_TIMEOUT,
                check=False)
            code = completed.returncode
            stderr = completed.stderr
        except subprocess.TimeoutExpired:
            code, stderr = "timeout", ""
    finally:
        proxy.close()
    log = binsrv.log_path.read_text(errors="replace") \
        if binsrv.log_path.exists() else ""
    errors = [line.strip()[-220:] for line in log.splitlines()
              if "[  error]" in line or "exception caught" in line]
    result = {"case": name, "exit_code": code,
              "seconds": round(time.monotonic() - started, 1),
              "answered_by_proxy": proxy.answered,
              "dump_received": proxy.dump_received,
              "queries_seen": proxy.queries[:4],
              "stored_transactions": stored_transactions(binsrv),
              "log_errors": errors[-3:]}
    if name == "control":
        expected = len(harness.transactions_of(b"".join(
            harness.SERVER.binlog_as_sent(binlog)
            for binlog in harness.SERVER.binlog_names())))
        result["source_transactions"] = expected
        result["ok"] = code == 0 and result["stored_transactions"] == expected
        return result
    hit = proxy.answered > 0
    clean = code == 1
    reason = any(word in line for line in errors + [stderr]
                 for word in REASON[kind])
    result.update(hit=hit, clean_exit=clean, reason_logged=reason,
                  ok=hit and clean and result["stored_transactions"] == 0 and
                  reason)
    if not result["ok"]:
        parts = []
        if not hit:
            parts.append("the proxy never answered the query (case not hit)")
        if not clean:
            parts.append(f"fetch exited with {code}")
        if result["stored_transactions"]:
            parts.append(f"{result['stored_transactions']} transactions "
                         "were stored")
        if hit and clean and not reason:
            parts.append("fetch exited 1 but its log does not say why "
                         f"(last error lines: {errors[-2:]})")
        result["why"] = "; ".join(parts)
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--mysql-basedir", type=Path, required=True)
    parser.add_argument("--binsrv", type=Path, required=True)
    parser.add_argument("--workdir", type=Path, required=True)
    parser.add_argument("--port", type=int, default=26800)
    parser.add_argument("--mode", choices=["position", "gtid"],
                        default="position")
    parser.add_argument("--cases", nargs="*", choices=list(CASES))
    args = parser.parse_args()
    gtid_mode = args.mode == "gtid"
    work = args.workdir.expanduser().resolve()
    work.mkdir(parents=True, exist_ok=True)
    source = harness.MySQLServer(args.mysql_basedir.expanduser(),
                                 work / "source", args.port, gtid_mode,
                                 server_id=1)
    harness.SERVER = source
    harness.BINSRV = harness.BinlogServer(args.binsrv.expanduser(),
                                          work / "binsrv", args.port,
                                          gtid_mode)
    names = args.cases or [name for name, (_, _, gtid_only) in CASES.items()
                           if gtid_mode or not gtid_only]
    results = []
    source.start()
    try:
        machine = harness.FetchCrashMachine()
        machine.start_from_scratch()
        machine.run_workload(WORKLOAD)
        source.sql("FLUSH BINARY LOGS")
        real_purged = (source.sql("SELECT @@GLOBAL.gtid_purged").strip()
                       .encode() if gtid_mode else b"")
        for name in names:
            try:
                result = run_case(args, name, source, real_purged)
            except Exception as error:  # every case is reported
                result = {"case": name, "ok": False,
                          "error": f"{type(error).__name__}: {error}"[:400]}
            results.append(result)
            print(json.dumps(result), flush=True)
    finally:
        source.stop()
    failed = [result["case"] for result in results if not result.get("ok")]
    summary = {"cases": len(results), "failed": failed, "mode": args.mode,
               "passed": not failed}
    print(json.dumps(summary), flush=True)
    (work / "result.json").write_text(json.dumps(results, indent=2))
    return 0 if not failed else 1


if __name__ == "__main__":
    sys.exit(main())
