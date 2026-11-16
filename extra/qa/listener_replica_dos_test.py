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

"""Denial of service of the 'binlog_server pull' replication source by
replicas that have logged in.

PBS is often the one source for many replicas, so one replica with valid
credentials must not be able to stall the others or exhaust the process.
Each case runs a hostile, authenticated client against 'pull' (GTID mode, a
storage of several MB) and, while it does, measures how long an ordinary
client (mysqlbinlog dumping binlog.000001) takes to be served, and the
resident memory and descriptors of 'pull'. Cases:

- 'huge_gtid_set': a COM_BINLOG_DUMP_GTID whose GTID set has --intervals
  disjoint intervals (default 1,000,000, a packet just under 16 MB); the
  purged-GTID check and the auto-positioning work on it.
- 'huge_gtid_sets_parallel': the same from 8 clients at once.
- 'many_dumps': --dumps (default 300) blocking dumps that stay open.
- 'stalled_readers': 20 blocking dumps from the start of the storage that
  never read, so the server's sends block on full socket buffers until
  write_timeout.

A case passes when 'pull' stays up, the ordinary client is served within
--max-wait seconds (default 10) and memory grows by less than --max-rss-mib
(default 1024). A JSON line per case, then a summary; exit 1 if any case
failed. Example:

  PYTHONPATH=~/ws/qa/pylib python3 listener_replica_dos_test.py \\
    --mysql-basedir ~/ws/dist/mysql-8.4.8-linux-glibc2.28-x86_64-minimal \\
    --binsrv ~/ws/percona-binlog-server-build-debug_gcc14/binlog_server \\
    --workdir ~/ws/qa/listener-replica-dos
"""

import argparse
import json
import os
import signal
import socket
import struct
import subprocess
import sys
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import fetch_crash_test as harness  # noqa: E402
import listener_fuzz_test as listener  # noqa: E402

# about 30 MB of binlog ('rows' are row sizes in bytes), so that dumps
# that are not read fill the socket buffers
WORKLOAD = [{"rows": [1_000_000] * 10, "ddl": False}] * 3 + [
    {"rows": [300], "ddl": True}]
TIMEOUT = 30


def proc_status(pid: int) -> dict:
    try:
        fds = len(os.listdir(f"/proc/{pid}/fd"))
    except OSError:
        fds = -1
    rss = -1
    try:
        for line in Path(f"/proc/{pid}/status").read_text().splitlines():
            if line.startswith("VmRSS:"):
                rss = int(line.split()[1])
    except OSError:
        pass
    return {"fds": fds, "rss_kib": rss}


class Sampler:
    """Samples the peak resident memory of 'pid' every 0.2 s."""

    def __init__(self, pid: int):
        self.pid = pid
        self.peak = proc_status(pid)["rss_kib"]
        self.running = True
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

    def _run(self):
        while self.running:
            self.peak = max(self.peak, proc_status(self.pid)["rss_kib"])
            time.sleep(0.2)

    def stop(self) -> int:
        self.running = False
        self.thread.join(timeout=2)
        return self.peak


def logged_in(port: int) -> "listener.Listener | None":
    client = listener.Listener("127.0.0.1", port, timeout=TIMEOUT)
    if not client.handshake():
        client.close()
        return None
    # @source_binlog_checksum must be declared before a dump
    client.send(listener.frame(0, bytes([listener.COM_QUERY]) +
                               b"SET @source_binlog_checksum = 'NONE', "
                               b"@master_binlog_checksum = 'NONE'"))
    reply = client.read_frame()
    if reply is None or reply[1][:1] != b"\x00":
        client.close()
        return None
    return client


def huge_gtid_set(intervals: int) -> bytes:
    """The binary GTID set of COM_BINLOG_DUMP_GTID: one UUID with
    'intervals' disjoint intervals [2k+1, 2k+2)."""
    uuid = bytes.fromhex("5a5a5a5a111122223333444455556666")
    body = b"".join(struct.pack("<qq", 2 * k + 1, 2 * k + 2)
                    for k in range(intervals))
    return struct.pack("<q", 1) + uuid + struct.pack("<q", intervals) + body


def dump_gtid_packet(gtids: bytes, blocking: bool) -> bytes:
    flags = 0 if blocking else 1  # BINLOG_DUMP_NON_BLOCK
    payload = (bytes([listener.COM_BINLOG_DUMP_GTID]) +
               struct.pack("<HI", flags, 7000) + struct.pack("<I", 0) +
               struct.pack("<Q", 4) + struct.pack("<I", len(gtids)) + gtids)
    return listener.frame(0, payload)


def dump_packet(blocking: bool, server_id: int) -> bytes:
    flags = 0 if blocking else 1
    payload = (bytes([listener.COM_BINLOG_DUMP]) + struct.pack("<I", 4) +
               struct.pack("<HI", flags, server_id) + b"binlog.000001")
    return listener.frame(0, payload)


def time_to_serve(port: int, limit: float) -> "float | None":
    """Seconds until mysqlbinlog streams binlog.000001 (one attempt may take
    longer than 'limit' when the listener is stalled: the time is still
    reported); None if no attempt succeeded within 3 x 'limit'."""
    started = time.monotonic()
    while time.monotonic() - started < 3 * limit:
        if listener.streams_binlogs("127.0.0.1", port):
            return round(time.monotonic() - started, 2)
        time.sleep(0.5)
    return None


def judge(name: str, pull, before: dict, peak: int, served: "float | None",
          args, extra: dict) -> dict:
    growth_mib = (peak - before["rss_kib"]) // 1024 if peak >= 0 else -1
    result = {"case": name, "pull_alive": pull.poll() is None,
              "ordinary_client_served_s": served,
              "rss_before_mib": before["rss_kib"] // 1024,
              "rss_growth_mib": growth_mib, **extra}
    problems = []
    if not result["pull_alive"]:
        problems.append("pull exited")
    if served is None or served > args.max_wait:
        problems.append(f"an ordinary client was not served within "
                        f"{args.max_wait} s" +
                        ("" if served is None else f" (it took {served} s)"))
    if growth_mib > args.max_rss_mib:
        problems.append(f"resident memory grew by {growth_mib} MiB")
    result["ok"] = not problems
    if problems:
        result["why"] = "; ".join(problems)
    return result


def case_huge_gtid_sets(pull, port: int, args, clients: int) -> dict:
    gtids = huge_gtid_set(args.intervals)
    packet = dump_gtid_packet(gtids, blocking=False)
    sessions = [client for client in
                (logged_in(port) for _ in range(clients)) if client]
    before = proc_status(pull.pid)
    sampler = Sampler(pull.pid)
    replies = []

    def send_and_wait(client):
        started = time.monotonic()
        client.send(packet)
        reply = client.read_frame()
        replies.append((round(time.monotonic() - started, 2),
                        None if reply is None else reply[1][:1].hex()))

    threads = [threading.Thread(target=send_and_wait, args=(client,))
               for client in sessions]
    for thread in threads:
        thread.start()
    time.sleep(0.5)
    served = time_to_serve(port, args.max_wait)
    for thread in threads:
        thread.join(timeout=TIMEOUT * 4)
    peak = sampler.stop()
    for client in sessions:
        client.close()
    name = "huge_gtid_set" if clients == 1 else "huge_gtid_sets_parallel"
    return judge(name, pull, before, peak, served, args, {
        "clients": len(sessions), "intervals": args.intervals,
        "packet_bytes": len(packet),
        "replies_s_and_first_byte": sorted(replies)[:3]})


def case_many_dumps(pull, port: int, args) -> dict:
    sessions = []
    before = proc_status(pull.pid)
    sampler = Sampler(pull.pid)
    for index in range(args.dumps):
        client = logged_in(port)
        if client is None:
            break
        client.send(dump_packet(blocking=True, server_id=10_000 + index))
        sessions.append(client)
    time.sleep(2)
    during = proc_status(pull.pid)
    served = time_to_serve(port, args.max_wait)
    peak = sampler.stop()
    for client in sessions:
        client.close()
    return judge("many_dumps", pull, before, peak, served, args, {
        "dumps_open": len(sessions), "fds_during": during["fds"]})


def case_stalled_readers(pull, port: int, args) -> dict:
    sessions = []
    before = proc_status(pull.pid)
    sampler = Sampler(pull.pid)
    for index in range(20):
        client = logged_in(port)
        if client is None:
            break
        # a small receive buffer, and nothing is ever read
        client.sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
        client.send(dump_packet(blocking=True, server_id=20_000 + index))
        sessions.append(client)
    time.sleep(3)
    served = time_to_serve(port, args.max_wait)
    peak = sampler.stop()
    for client in sessions:
        client.close()
    return judge("stalled_readers", pull, before, peak, served, args,
                 {"stalled": len(sessions)})


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--mysql-basedir", type=Path, required=True)
    parser.add_argument("--binsrv", type=Path, required=True)
    parser.add_argument("--workdir", type=Path, required=True)
    parser.add_argument("--port", type=int, default=23680)
    parser.add_argument("--mode", choices=["gtid"], default="gtid")
    parser.add_argument("--intervals", type=int, default=1_000_000)
    parser.add_argument("--dumps", type=int, default=300)
    parser.add_argument("--max-wait", type=float, default=10.0)
    parser.add_argument("--max-rss-mib", type=int, default=1024)
    parser.add_argument("--cases", nargs="*", default=[
        "huge_gtid_set", "huge_gtid_sets_parallel", "many_dumps",
        "stalled_readers"])
    args = parser.parse_args()
    args.mysql_basedir = args.mysql_basedir.expanduser()
    listener.MYSQLBINLOG = args.mysql_basedir / "bin" / "mysqlbinlog"
    args.workdir.mkdir(parents=True, exist_ok=True)
    harness.SERVER = harness.MySQLServer(args.mysql_basedir,
                                         args.workdir / "mysql", args.port,
                                         True, server_id=1)
    binsrv = harness.BinlogServer(args.binsrv.expanduser(),
                                  args.workdir / "binsrv", args.port, True)
    harness.BINSRV = binsrv
    pull_port = args.port + 2
    results = []
    pull = None
    harness.SERVER.start()
    try:
        machine = harness.FetchCrashMachine()
        machine.start_from_scratch()
        machine.run_workload(WORKLOAD)
        binsrv.checkpoint_size = "1"
        machine.fetch_to_the_end()
        binsrv.write_config()
        config = json.loads(binsrv.config_path.read_text())
        config["replication_source"] = {
            "port": pull_port, "server_id": 142,
            "server_uuid": "00000000-0000-0000-0000-00000000008e",
            "read_timeout": TIMEOUT, "write_timeout": TIMEOUT,
            "require_secure_transport": False,
            "auth_rsa_private_key": str(listener.AUTH_RSA_PRIVATE_KEY),
            "authentication": {"user": listener.PULL_USER,
                               "password": listener.PULL_PASSWORD,
                               "plugin": "caching_sha2_password"}}
        binsrv.config_path.write_text(json.dumps(config, indent=2))
        pull_log = (args.workdir / "pull.log").open("wb")
        pull = subprocess.Popen(
            [str(binsrv.binary), "pull", str(binsrv.config_path)],
            stdout=pull_log, stderr=subprocess.STDOUT,
            env=dict(os.environ, AWS_EC2_METADATA_DISABLED="true"))
        if time_to_serve(pull_port, 30) is None:
            raise harness.Failure("the listener does not serve before the "
                                  "test")
        stored_mib = sum(path.stat().st_size
                         for path in binsrv.storage.iterdir()
                         if path.name.startswith("binlog.") and
                         path.suffix not in (".json", ".index")) // (1 << 20)
        for name in args.cases:
            if pull.poll() is not None:
                results.append({"case": name, "ok": False,
                                "why": "pull had exited"})
                continue
            if name == "huge_gtid_set":
                result = case_huge_gtid_sets(pull, pull_port, args, 1)
            elif name == "huge_gtid_sets_parallel":
                result = case_huge_gtid_sets(pull, pull_port, args, 8)
            elif name == "many_dumps":
                result = case_many_dumps(pull, pull_port, args)
            elif name == "stalled_readers":
                result = case_stalled_readers(pull, pull_port, args)
            else:
                raise SystemExit(f"unknown case {name}")
            result["stored_mib"] = stored_mib
            # memory once the case's sessions have ended
            time.sleep(5)
            result["rss_after_mib"] = proc_status(pull.pid)["rss_kib"] // 1024
            print(json.dumps(result), flush=True)
            results.append(result)
            # wait until the listener serves promptly again
            time_to_serve(pull_port, 60)
    finally:
        if pull is not None and pull.poll() is None:
            pull.send_signal(signal.SIGTERM)
            try:
                pull.wait(timeout=60)
            except subprocess.TimeoutExpired:
                pull.kill()
        harness.SERVER.stop()
    failures = [result for result in results if not result.get("ok")]
    print(json.dumps({"cases": len(results), "failures": len(failures),
                      "passed": not failures}), flush=True)
    (args.workdir / "result.json").write_text(json.dumps(results, indent=2))
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
