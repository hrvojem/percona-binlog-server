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

"""Binlog names chosen by a hostile source, against the filesystem storage.

'fetch' / 'pull' name the binlogs they store after the ROTATE events the
source sends. composite_binlog_name::parse() rejects a '/' in the name but
keeps any other byte, and the filesystem backend hands the name to the C
file API, which stops at a NUL byte. A source that is compromised, or an
attacker between PBS and the source on a connection without TLS
verification, picks those names.

The test runs a real MySQL source and puts a man-in-the-middle proxy between
it and 'fetch'. The proxy passes everything through, except that in the
binlog dump it rewrites the binlog name of the ROTATE events that announce
the next binlog (the one at the end of a binlog and the artificial one at the
start of the next), fixing the event size, its log_pos, the CRC32 footer and
the packet length. Each case:

1. 'fetch' stores the source's binlogs directly (with TLS), and the bytes of
   every stored file are recorded;
2. the source runs FLUSH BINARY LOGS and a workload, so the next binlog is
   announced by a ROTATE;
3. 'fetch' resumes through the proxy (ssl mode 'disabled'), which renames
   that next binlog;
4. every binlog stored before must keep its data (it may only grow by the
   ROTATE appended at its end), metadata.json must be unchanged, and the
   storage must still open ('list'); the error a direct 'fetch' then logs
   is reported (it cannot resume after any rename: the real source has no
   binlog of that name).

The names (the sequence number is the real next one, N):
  nul_existing_binlog  'binlog.000001\\0.N': opened as 'binlog.000001'
  nul_metadata         'metadata.json\\0.N': opened as 'metadata.json'
  nul_index            'binlog.index\\0.N': opened as 'binlog.index'
  newline              'binlog.000077\\nbinlog.N': one line of binlog.index
                       becomes two
  dot_dot              '...N' (base name '..'): stays in the storage
  long_name            a 3000-byte base name: the file system refuses it

The binlog dump is read without TLS, so 'fetch' authenticates as the
caching_sha2_password user with the server's cached hash (a mysql client
login with --get-server-public-key first), as in the MTR test
fetch_ssl_disabled_caching_sha2. A JSON line per case, then a summary; exit
1 if any case failed. Example:

  PYTHONPATH=~/ws/qa/pylib python3 hostile_source_test.py \\
    --mysql-basedir ~/ws/dist/mysql-8.4.8-linux-glibc2.28-x86_64-minimal \\
    --binsrv ~/ws/percona-binlog-server-build-debug_gcc14/binlog_server \\
    --workdir ~/ws/qa/hostile-source
"""

import argparse
import json
import os
import re
import socket
import struct
import subprocess
import sys
import threading
import zlib
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import fetch_crash_test as harness  # noqa: E402

WORKLOAD = [{"rows": [200], "ddl": True}]
COM_BINLOG_DUMP = 0x12
COM_BINLOG_DUMP_GTID = 0x1E
ROTATE_EVENT = 4
HEADER_SIZE = 19
ROTATE_POST_HEADER_SIZE = 8
BINLOG_NAME = re.compile(r"^binlog\.(\d{6})$")

CASES = {
    "nul_existing_binlog": lambda n: b"binlog.000001\x00" + b".%06d" % n,
    "nul_metadata": lambda n: b"metadata.json\x00" + b".%06d" % n,
    "nul_index": lambda n: b"binlog.index\x00" + b".%06d" % n,
    "newline": lambda n: b"binlog.000077\nbinlog" + b".%06d" % n,
    "dot_dot": lambda n: b".." + b".%06d" % n,
    "long_name": lambda n: b"b" * 3000 + b".%06d" % n,
}


class RenamingProxy:
    """A TCP proxy in front of the source: client bytes pass unchanged; once
    the client has sent COM_BINLOG_DUMP(_GTID), every server packet is parsed
    and a ROTATE event naming 'old_name' is rewritten to name 'new_name'."""

    def __init__(self, listen_port: int, source_port: int, old_name: bytes,
                 new_name: bytes):
        self.source_port = source_port
        self.old_name = old_name
        self.new_name = new_name
        self.rewritten = 0
        self.dumping = threading.Event()
        self.listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.listener.bind(("127.0.0.1", listen_port))
        self.listener.listen(4)
        self.thread = threading.Thread(target=self._accept, daemon=True)
        self.thread.start()

    def close(self):
        # shutdown() wakes the accept() blocked in the other thread, so the
        # port is really released
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
            threading.Thread(target=self._client_to_server,
                             args=(client, server), daemon=True).start()
            threading.Thread(target=self._server_to_client,
                             args=(server, client), daemon=True).start()

    @staticmethod
    def _read_exactly(sock: socket.socket, count: int) -> "bytes | None":
        data = bytearray()
        while len(data) < count:
            try:
                piece = sock.recv(count - len(data))
            except OSError:
                return None
            if not piece:
                return None
            data += piece
        return bytes(data)

    def _client_to_server(self, client, server):
        try:
            while True:
                header = self._read_exactly(client, 4)
                if header is None:
                    break
                length = header[0] | header[1] << 8 | header[2] << 16
                payload = self._read_exactly(client, length) if length else b""
                if payload is None:
                    break
                if header[3] == 0 and payload and \
                        payload[0] in (COM_BINLOG_DUMP, COM_BINLOG_DUMP_GTID):
                    self.dumping.set()
                server.sendall(header + payload)
        finally:
            for sock in (client, server):
                try:
                    sock.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass

    def _rewrite(self, payload: bytes) -> bytes:
        """The packet with its ROTATE event renamed, or unchanged."""
        if len(payload) < 1 + HEADER_SIZE + ROTATE_POST_HEADER_SIZE or \
                payload[0] != 0 or payload[1 + 4] != ROTATE_EVENT:
            return payload
        event = payload[1:]
        has_crc = (len(event) >= HEADER_SIZE + ROTATE_POST_HEADER_SIZE + 4 and
                   zlib.crc32(event[:-4]) == struct.unpack("<I",
                                                           event[-4:])[0])
        body_end = len(event) - (4 if has_crc else 0)
        name = event[HEADER_SIZE + ROTATE_POST_HEADER_SIZE:body_end]
        if name != self.old_name:
            return payload
        delta = len(self.new_name) - len(name)
        new_event = bytearray(event[:HEADER_SIZE + ROTATE_POST_HEADER_SIZE] +
                              self.new_name)
        struct.pack_into("<I", new_event, 9, len(new_event) +
                         (4 if has_crc else 0))
        log_pos = struct.unpack_from("<I", new_event, 13)[0]
        if log_pos != 0:
            struct.pack_into("<I", new_event, 13, log_pos + delta)
        if has_crc:
            new_event += struct.pack("<I", zlib.crc32(bytes(new_event)))
        self.rewritten += 1
        return b"\x00" + bytes(new_event)

    def _server_to_client(self, server, client):
        try:
            while True:
                header = self._read_exactly(server, 4)
                if header is None:
                    break
                length = header[0] | header[1] << 8 | header[2] << 16
                payload = self._read_exactly(server, length) if length else b""
                if payload is None:
                    break
                if self.dumping.is_set() and length < 0xFFFFFF:
                    payload = self._rewrite(payload)
                client.sendall(struct.pack("<I", len(payload))[:3] +
                               header[3:4] + payload)
        finally:
            for sock in (client, server):
                try:
                    sock.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass


def snapshot(storage: Path) -> dict:
    return {path.name: path.read_bytes()
            for path in storage.iterdir() if path.is_file()}


def run_fetch(binsrv, config_path: Path) -> "tuple[int, str]":
    try:
        completed = subprocess.run(
            [str(binsrv.binary), "fetch", str(config_path)],
            capture_output=True, timeout=120, check=False,
            env=dict(os.environ, AWS_EC2_METADATA_DISABLED="true"))
    except subprocess.TimeoutExpired:
        return -1, "timeout"
    return completed.returncode, completed.stderr.decode(
        errors="replace")[-600:]


def cache_password_hash(basedir: Path, port: int):
    """A plain-TCP login as the replication user with the server's public
    key: the server caches the password hash, so that 'fetch' (which cannot
    request the key, ticket 23) gets the fast path without TLS."""
    subprocess.run(
        [str(basedir / "bin" / "mysql"), "--no-defaults",
         "--ssl-mode=DISABLED", "--get-server-public-key",
         "--host=127.0.0.1", f"--port={port}",
         f"--user={harness.REPLICATION_USER}",
         f"--password={harness.REPLICATION_PASSWORD}", "-e", "SELECT 1"],
        capture_output=True, timeout=30, check=True)


def run_case(name: str, args, proxy_port: int) -> dict:
    machine = harness.FetchCrashMachine()
    machine.start_from_scratch()
    machine.run_workload(WORKLOAD)
    machine.fetch_to_the_end()
    binsrv = harness.BINSRV
    stored = sorted(int(m.group(1)) for m in
                    (BINLOG_NAME.match(p.name)
                     for p in binsrv.storage.iterdir()) if m)
    before = snapshot(binsrv.storage)
    upcoming = stored[-1] + 1
    old_name = b"binlog.%06d" % upcoming
    new_name = CASES[name](upcoming)
    harness.SERVER.sql("FLUSH BINARY LOGS")
    machine.run_workload(WORKLOAD)

    # the proxied configuration: through the proxy, without TLS
    binsrv.write_config()
    config = json.loads(binsrv.config_path.read_text())
    config["connection"]["port"] = proxy_port
    config["connection"]["ssl"] = {"mode": "disabled"}
    proxied_config = binsrv.workdir / "binsrv-proxied.json"
    proxied_config.write_text(json.dumps(config, indent=2))
    cache_password_hash(args.mysql_basedir, args.port)
    proxy = RenamingProxy(proxy_port, args.port, old_name, new_name)
    try:
        proxied_exit, proxied_stderr = run_fetch(binsrv, proxied_config)
    finally:
        proxy.close()
    after = snapshot(binsrv.storage)
    # a stored binlog may only grow (the ROTATE at its end is appended) and
    # its metadata follows it, so binlog data must keep its old bytes as a
    # prefix; metadata.json never changes on a resume
    changed = sorted(
        object_name for object_name, content in before.items()
        if BINLOG_NAME.match(object_name) and
        not after.get(object_name, b"").startswith(content))
    damaged_shared = sorted(
        object_name for object_name in ("metadata.json",)
        if after.get(object_name) != before.get(object_name))
    # the storage must still open ('list'), whatever the name; resuming from
    # the real source cannot work after any rename, as the source has no
    # binlog of that name, so its error is only reported
    try:
        listed = subprocess.run(
            [str(binsrv.binary), "list", str(binsrv.config_path)],
            capture_output=True, timeout=60, check=False,
            env=dict(os.environ, AWS_EC2_METADATA_DISABLED="true"))
        list_exit = listed.returncode
        list_output = (listed.stdout + listed.stderr).decode(
            errors="replace")
    except subprocess.TimeoutExpired:
        list_exit, list_output = -1, "timeout"
    direct_exit, direct_stderr = run_fetch(binsrv, binsrv.config_path)
    errors = [line.strip()[-220:] for line in
              binsrv.log_path.read_text(errors="replace").splitlines()
              if "[  error]" in line or "exception caught" in line][-3:] \
        if binsrv.log_path.exists() else []
    result = {
        "case": name, "hostile_name": repr(new_name)[:120],
        "rotates_rewritten": proxy.rewritten,
        "proxied_fetch_exit": proxied_exit,
        "files_changed": changed, "shared_files_changed": damaged_shared,
        "storage_entries": sorted(repr(entry)[1:-1][:60] for entry in
                                  os.listdir(binsrv.storage))[-10:],
        "list_exit": list_exit,
        "direct_fetch_exit": direct_exit,
        "last_errors": errors,
    }
    if proxy.rewritten == 0:
        result["ok"] = False
        result["why"] = ("the proxy saw no ROTATE naming the next binlog "
                         f"({old_name!r}): the case did not run")
        result["proxied_stderr"] = proxied_stderr
        return result
    problems = []
    if changed:
        problems.append("stored binlogs lost their data: " +
                        ", ".join(changed))
    if damaged_shared:
        problems.append("the storage metadata was overwritten")
    if list_exit != 0:
        problems.append("the storage no longer opens ('list' exit "
                        f"{list_exit}: {list_output.strip()[-200:]})")
    result["ok"] = not problems
    if problems:
        result["why"] = "; ".join(problems)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--mysql-basedir", type=Path, required=True)
    parser.add_argument("--binsrv", type=Path, required=True)
    parser.add_argument("--workdir", type=Path, required=True)
    parser.add_argument("--port", type=int, default=23580)
    parser.add_argument("--mode", choices=["position"], default="position",
                        help="position only: the GTID-mode resume differs and "
                             "is not covered yet")
    parser.add_argument("--cases", nargs="*", default=list(CASES),
                        choices=list(CASES))
    args = parser.parse_args()
    args.mysql_basedir = args.mysql_basedir.expanduser()
    args.workdir.mkdir(parents=True, exist_ok=True)
    harness.SERVER = harness.MySQLServer(args.mysql_basedir,
                                         args.workdir / "mysql", args.port,
                                         False, server_id=1)
    harness.BINSRV = harness.BinlogServer(args.binsrv.expanduser(),
                                          args.workdir / "binsrv", args.port,
                                          False)
    results = []
    harness.SERVER.start()
    try:
        for index, name in enumerate(args.cases):
            try:
                result = run_case(name, args, args.port + 3 + index)
            except (harness.Failure, subprocess.SubprocessError,
                    OSError) as failure:
                result = {"case": name, "ok": False,
                          "why": harness.describe_failure(failure, 500)}
            print(json.dumps(result), flush=True)
            results.append(result)
    finally:
        harness.SERVER.stop()
    failures = [result for result in results if not result.get("ok")]
    print(json.dumps({"cases": len(results), "failures": len(failures),
                      "passed": not failures}), flush=True)
    (args.workdir / "result.json").write_text(json.dumps(results, indent=2))
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
