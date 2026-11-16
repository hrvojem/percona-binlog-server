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
  reannounce           the current binlog's own name: after its closing
                       ROTATE the source announces the same binlog again at
                       position 4; the stored binlog must not get a second
                       FORMAT_DESCRIPTION appended (ticket 69)
  lower_sequence       'binlog.000002' for a storage that starts at
                       'binlog.000004' (the source purged its first binlogs
                       before the first fetch); the storage must still open
                       for the next fetch (ticket 70: the start-up recovery
                       takes the new binlog for the leftover of a purge)
  case_variant         'BINLOG.000001', with the storage on a file system
                       that ignores case (--case-insensitive-dir, e.g. a
                       vfat mount; not run without it): the stored
                       'binlog.000001' must keep its data (ticket 71)

Two attacks on resources run in the same way: 'size_lie' makes the first
ordinary event of the next binlog claim 0xFFFFFF00 bytes (its CRC32 kept
valid), and 'gtid_bloat' (GTID mode) replaces the next binlog's
PREVIOUS_GTIDS_LOG with 900,000 intervals and shifts the later positions to
match; for these the peak memory and the duration of the proxied 'fetch' are
recorded as well (at most 2 GiB and 120 s). --mode gtid runs every case in
GTID mode, and --storage s3 against the local S3 server.

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
import shutil
import socket
import struct
import subprocess
import sys
import threading
import time
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
    "reannounce": lambda n: b"binlog.%06d" % (n - 1),
    "lower_sequence": lambda n: b"binlog.000002",
    "case_variant": lambda n: b"BINLOG.000001",
}
# the binlogs the source purges before the first fetch of 'lower_sequence'
PURGED_BINLOGS = 3
# what a start for writing logs when 'binlog.index' names a binlog the
# start-up recovery does not accept
STORAGE_REFUSED = "binlog index contains a reference to a non-existing object"
# attacks on resources rather than names: 'size_lie' makes the first
# ordinary event of the next binlog claim 0xFFFFFF00 bytes (CRC32 kept
# valid); 'gtid_bloat' (GTID mode) replaces the next binlog's
# PREVIOUS_GTIDS_LOG with GTID_BLOAT_INTERVALS intervals (about 14 MB) and
# shifts the positions of the events after it to match
RESOURCE_CASES = ["size_lie", "gtid_bloat"]
GTID_BLOAT_INTERVALS = 900_000
FORMAT_DESCRIPTION_EVENT = 15
PREVIOUS_GTIDS_LOG_EVENT = 35
ARTIFICIAL_FLAG = 0x20


class RenamingProxy:
    """A TCP proxy in front of the source: client bytes pass unchanged; once
    the client has sent COM_BINLOG_DUMP(_GTID), every server packet is parsed
    and a ROTATE event naming 'old_name' is rewritten to name 'new_name'."""

    def __init__(self, listen_port: int, source_port: int, old_name: bytes,
                 new_name: bytes, attack: str = "rename"):
        self.source_port = source_port
        self.old_name = old_name
        self.new_name = new_name
        self.attack = attack
        self.rewritten = 0
        # set by the artificial ROTATE that starts the next binlog
        self.in_next_binlog = False
        # bytes added to the next binlog so far (gtid_bloat)
        self.shift = 0
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

    @staticmethod
    def _finish(event: bytearray, has_crc: bool) -> bytes:
        """'event' (without a footer) with its size field and, if the
        stream has them, a fresh CRC32 footer, as a packet payload."""
        struct.pack_into("<I", event, 9, len(event) + (4 if has_crc else 0))
        if has_crc:
            event += struct.pack("<I", zlib.crc32(bytes(event)))
        return b"\x00" + bytes(event)

    def _rewrite(self, payload: bytes) -> bytes:
        """The packet with its event changed by the attack, or unchanged."""
        if len(payload) < 1 + HEADER_SIZE or payload[0] != 0:
            return payload
        event = payload[1:]
        code = event[4]
        flags = struct.unpack_from("<H", event, 17)[0]
        has_crc = (len(event) >= HEADER_SIZE + 4 and
                   zlib.crc32(event[:-4]) == struct.unpack("<I",
                                                           event[-4:])[0])
        body_end = len(event) - (4 if has_crc else 0)
        if code == ROTATE_EVENT and \
                len(event) >= HEADER_SIZE + ROTATE_POST_HEADER_SIZE:
            name = event[HEADER_SIZE + ROTATE_POST_HEADER_SIZE:body_end]
            if name == self.old_name and flags & ARTIFICIAL_FLAG:
                self.in_next_binlog = True
            if self.attack == "rename" and name == self.old_name:
                new_event = bytearray(
                    event[:HEADER_SIZE + ROTATE_POST_HEADER_SIZE] +
                    self.new_name)
                log_pos = struct.unpack_from("<I", new_event, 13)[0]
                if log_pos != 0:
                    struct.pack_into("<I", new_event, 13, log_pos +
                                     len(self.new_name) - len(name))
                self.rewritten += 1
                return self._finish(new_event, has_crc)
            return payload
        if not self.in_next_binlog or code == FORMAT_DESCRIPTION_EVENT:
            return payload
        new_event = bytearray(event[:body_end])
        if self.attack == "gtid_bloat" and \
                code == PREVIOUS_GTIDS_LOG_EVENT and self.shift == 0:
            uuid = (event[HEADER_SIZE + 8:HEADER_SIZE + 24]
                    if body_end >= HEADER_SIZE + 24 else b"\x5a" * 16)
            base = 1_000_000_000
            body = (struct.pack("<q", 1) + uuid +
                    struct.pack("<q", GTID_BLOAT_INTERVALS) +
                    b"".join(struct.pack("<qq", base + 2 * k,
                                         base + 2 * k + 1)
                             for k in range(GTID_BLOAT_INTERVALS)))
            new_event = bytearray(event[:HEADER_SIZE]) + body
            self.shift = len(new_event) - body_end
            log_pos = struct.unpack_from("<I", new_event, 13)[0]
            struct.pack_into("<I", new_event, 13, log_pos + self.shift)
            self.rewritten += 1
            return self._finish(new_event, has_crc)
        if self.attack == "gtid_bloat" and self.shift:
            log_pos = struct.unpack_from("<I", new_event, 13)[0]
            if log_pos != 0:
                struct.pack_into("<I", new_event, 13, log_pos + self.shift)
                return self._finish(new_event, has_crc)
            return payload
        if self.attack == "size_lie" and self.rewritten == 0 and \
                code != PREVIOUS_GTIDS_LOG_EVENT:
            self.rewritten += 1
            event_with_lie = bytearray(event[:body_end])
            if has_crc:
                event_with_lie += b"\x00" * 4
            struct.pack_into("<I", event_with_lie, 9, 0xFFFFFF00)
            if has_crc:
                struct.pack_into("<I", event_with_lie,
                                 len(event_with_lie) - 4,
                                 zlib.crc32(bytes(event_with_lie[:-4])))
            return b"\x00" + bytes(event_with_lie)
        return payload

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


def run_fetch_sampled(binsrv, config_path: Path) -> dict:
    """Runs fetch, sampling its resident memory; returns its exit code,
    peak memory (MiB), duration and stderr tail."""
    started = time.monotonic()
    process = subprocess.Popen(
        [str(binsrv.binary), "fetch", str(config_path)],
        stdout=subprocess.DEVNULL, stderr=subprocess.PIPE,
        env=dict(os.environ, AWS_EC2_METADATA_DISABLED="true"))
    peak = 0
    stderr = b""
    while process.poll() is None:
        try:
            for line in Path(f"/proc/{process.pid}/status").read_text(
            ).splitlines():
                if line.startswith("VmRSS:"):
                    peak = max(peak, int(line.split()[1]))
        except OSError:
            pass
        if time.monotonic() - started > 180:
            process.kill()
            break
        time.sleep(0.1)
    _, stderr = process.communicate()
    return {"exit": process.returncode, "peak_rss_mib": peak // 1024,
            "seconds": round(time.monotonic() - started, 1),
            "stderr": stderr.decode(errors="replace")[-600:]}


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


def format_descriptions(data: bytes) -> int:
    """The number of FORMAT_DESCRIPTION events in a stored binlog (1 in a
    well-formed one), or -1 if its events cannot be walked."""
    try:
        return sum(1 for _, code, _ in harness.parse_events(data)
                   if code == FORMAT_DESCRIPTION_EVENT)
    except Exception:  # pylint: disable=broad-except
        return -1


def run_case(name: str, args, proxy_port: int) -> dict:
    if name == "case_variant":
        if args.case_insensitive_dir is None or args.storage != "file":
            return {"case": name, "ok": True,
                    "skipped": "needs --case-insensitive-dir and a file "
                               "storage"}
        ordinary_storage = harness.BINSRV.storage
        harness.BINSRV.storage = \
            args.case_insensitive_dir.expanduser() / "storage"
        try:
            return run_case_on_storage(name, args, proxy_port)
        finally:
            shutil.rmtree(harness.BINSRV.storage, ignore_errors=True)
            harness.BINSRV.storage = ordinary_storage
    return run_case_on_storage(name, args, proxy_port)


def run_case_on_storage(name: str, args, proxy_port: int) -> dict:
    machine = harness.FetchCrashMachine()
    machine.start_from_scratch()
    if name == "lower_sequence":
        # the storage's first binlog is then not binlog.000001, as after a
        # purge on either side
        for _ in range(PURGED_BINLOGS):
            harness.SERVER.sql("FLUSH BINARY LOGS")
        harness.SERVER.sql("PURGE BINARY LOGS TO 'binlog.%06d'" %
                           (PURGED_BINLOGS + 1))
    machine.run_workload(WORKLOAD)
    machine.fetch_to_the_end()
    binsrv = harness.BINSRV
    stored = sorted(int(m.group(1)) for m in
                    (BINLOG_NAME.match(p.name)
                     for p in binsrv.storage.iterdir()) if m)
    before = snapshot(binsrv.storage)
    upcoming = stored[-1] + 1
    old_name = b"binlog.%06d" % upcoming
    attack = "rename" if name in CASES else name
    new_name = CASES[name](upcoming) if attack == "rename" else old_name
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
    proxy = RenamingProxy(proxy_port, args.port, old_name, new_name, attack)
    try:
        proxied = run_fetch_sampled(binsrv, proxied_config)
    finally:
        proxy.close()
    proxied_exit, proxied_stderr = proxied["exit"], proxied["stderr"]
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
        "case": name, "attack": attack,
        "hostile_name": repr(new_name)[:120],
        "events_rewritten": proxy.rewritten,
        "proxied_fetch_exit": proxied_exit,
        "proxied_fetch_peak_rss_mib": proxied["peak_rss_mib"],
        "proxied_fetch_seconds": proxied["seconds"],
        "files_changed": changed, "shared_files_changed": damaged_shared,
        "storage_entries": sorted(repr(entry)[1:-1][:60] for entry in
                                  os.listdir(binsrv.storage))[-10:],
        "list_exit": list_exit,
        "direct_fetch_exit": direct_exit,
        "last_errors": errors,
    }
    if proxy.rewritten == 0:
        result["ok"] = False
        result["why"] = ("the proxy changed nothing in the next binlog "
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
    # a binlog stored before the attack has one FORMAT_DESCRIPTION event
    twice = sorted(
        object_name for object_name, content in before.items()
        if BINLOG_NAME.match(object_name) and
        format_descriptions(content) == 1 and
        format_descriptions(after.get(object_name, b"")) > 1)
    result["second_preamble_appended"] = twice
    if twice:
        problems.append("a second FORMAT_DESCRIPTION and what follows it "
                        "were appended to " + ", ".join(twice))
    if list_exit == 0 and any(STORAGE_REFUSED in line for line in errors):
        problems.append("the storage no longer opens for writing (the next "
                        f"fetch: '{STORAGE_REFUSED}')")
    if proxied["peak_rss_mib"] > 2048:
        problems.append(f"fetch used {proxied['peak_rss_mib']} MiB")
    if proxied["seconds"] > 120:
        problems.append(f"fetch took {proxied['seconds']} s")
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
    parser.add_argument("--mode", choices=["position", "gtid"],
                        default="position")
    parser.add_argument("--storage", choices=["file", "s3"], default="file")
    parser.add_argument("--s3-endpoint", default="127.0.0.1:9000")
    parser.add_argument("--s3-bucket", default="pbs-mtr")
    parser.add_argument("--s3-bucket-dir", type=Path)
    parser.add_argument("--s3-access-key", default="pbsaccesskey")
    parser.add_argument("--s3-secret-key", default="pbs/secret+key=1")
    parser.add_argument("--cases", nargs="*",
                        choices=list(CASES) + RESOURCE_CASES)
    parser.add_argument("--case-insensitive-dir", type=Path,
                        help="a directory on a file system that ignores "
                             "case; 'case_variant' runs only with it")
    args = parser.parse_args()
    args.mysql_basedir = args.mysql_basedir.expanduser()
    args.workdir.mkdir(parents=True, exist_ok=True)
    gtid_mode = args.mode == "gtid"
    if args.cases is None:
        args.cases = [
            name for name in CASES if name != "case_variant" or
            args.case_insensitive_dir is not None] + [
            name for name in RESOURCE_CASES
            if name != "gtid_bloat" or gtid_mode]
    s3 = None
    if args.storage == "s3":
        if args.s3_bucket_dir is None:
            parser.error("--storage s3 needs --s3-bucket-dir")
        s3 = harness.S3Storage(args.s3_endpoint, args.s3_bucket,
                               args.s3_bucket_dir.expanduser(),
                               args.s3_access_key, args.s3_secret_key,
                               f"hostile-source-{args.mode}-{os.getpid()}")
    harness.SERVER = harness.MySQLServer(args.mysql_basedir,
                                         args.workdir / "mysql", args.port,
                                         gtid_mode, server_id=1)
    harness.BINSRV = harness.BinlogServer(args.binsrv.expanduser(),
                                          args.workdir / "binsrv", args.port,
                                          gtid_mode, s3)
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
