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

"""Resource exhaustion of the 'binlog_server pull' replication source listener
by clients that never log in.

The listener accepts every TCP connection and serves each in a coroutine with
no limit on their number; a client that has not logged in holds a socket (a
file descriptor) and a little memory until 'read_timeout'. This test checks
that clients which never log in cannot take the listener down for good, and
that what they hold stays bounded. Cases:

- 'idle_flood': opens more idle connections than 'pull' has file descriptors
  (its RLIMIT_NOFILE is lowered with --fd-limit, so a few hundred connections
  are enough), holds them, then closes them all. While they are held, new
  clients may be refused; afterwards the listener must serve again. A
  listener whose accept loop gave up on EMFILE stays deaf after the flood.
- 'oversized_headers': many connections each send a frame header announcing
  a payload just under the 16 MB limit and then nothing; the resident memory
  of 'pull' must not grow by anything near connections x 16 MB (no
  allocation ahead of the data), and the listener serves afterwards.
- 'payload_drip': one connection announces a payload and sends it one byte
  a second; the per-frame read timeout must close it within read_timeout
  (plus slack), however long the drip would go on.
- 'header_drip': one connection completes nothing but sends one valid frame
  header byte at a time just under read_timeout apart; the session must end
  within a bounded number of read timeouts (a client that never logs in has
  at most a few frames to send before the handshake fails).

Set-up as in listener_fuzz_test.py: a source MySQL server with a small
workload, 'fetch' stores its binlogs, 'binlog_server pull' serves them with
a caching_sha2_password user. A JSON line per case, then a summary ('pull' logs to <workdir>/pull.log); exit 1
if any case failed. Example:

  PYTHONPATH=~/ws/qa/pylib python3 listener_exhaustion_test.py \\
    --mysql-basedir ~/ws/dist/mysql-8.4.8-linux-glibc2.28-x86_64-minimal \\
    --binsrv ~/ws/percona-binlog-server-build-debug_gcc14/binlog_server \\
    --workdir ~/ws/qa/listener-exhaustion
"""

import argparse
import json
import os
import resource
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import fetch_crash_test as harness  # noqa: E402
import listener_fuzz_test as listener  # noqa: E402

WORKLOAD = [{"rows": [500], "ddl": True}]
# the pull session read timeout: long enough to hold a flood of idle clients
# while the test measures, short enough for the drip cases
READ_TIMEOUT = 8


def proc_status(pid: int) -> dict:
    """The open file descriptors and the resident memory (KiB) of 'pid'."""
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


def open_idle(port: int, count: int) -> "tuple[list, int]":
    """Opens up to 'count' TCP connections that send nothing; returns them
    and the number that the kernel refused (which only happens once the
    accept backlog is full)."""
    sockets = []
    refused = 0
    for _ in range(count):
        try:
            sock = socket.create_connection(("127.0.0.1", port), timeout=2)
            sockets.append(sock)
        except OSError:
            refused += 1
    return sockets, refused


def close_all(sockets: list):
    for sock in sockets:
        try:
            sock.close()
        except OSError:
            pass


def serves_within(port: int, seconds: float) -> "float | None":
    """Seconds until a clean mysqlbinlog dump succeeds, None if it does not
    within 'seconds'."""
    started = time.monotonic()
    while time.monotonic() - started < seconds:
        if listener.streams_binlogs("127.0.0.1", port):
            return round(time.monotonic() - started, 1)
        time.sleep(1)
    return None


def closed_within(sock: socket.socket, seconds: float,
                  feed=None) -> "float | None":
    """Seconds until the server closes 'sock' (EOF or reset), None if it is
    still open after 'seconds'; 'feed(elapsed)' is called once a second and
    may send bytes."""
    started = time.monotonic()
    sock.settimeout(1.0)
    while True:
        elapsed = time.monotonic() - started
        if elapsed >= seconds:
            return None
        if feed is not None:
            try:
                feed(elapsed)
            except OSError:
                return round(elapsed, 1)
        try:
            data = sock.recv(65536)
            if not data:
                return round(time.monotonic() - started, 1)
        except socket.timeout:
            continue
        except OSError:
            return round(time.monotonic() - started, 1)


def case_idle_flood(pull, port: int, fd_limit: int) -> dict:
    flood = fd_limit + 200
    before = proc_status(pull.pid)
    sockets, refused = open_idle(port, flood)
    # let the listener accept what it can
    time.sleep(2)
    during = proc_status(pull.pid)
    serves_during = listener.streams_binlogs("127.0.0.1", port)
    close_all(sockets)
    # the sessions of the closed sockets end on their next read; the accept
    # loop, if it still runs, takes new connections at once
    recovered_after = serves_within(port, READ_TIMEOUT * 3)
    after = proc_status(pull.pid)
    result = {"case": "idle_flood", "fd_limit": fd_limit,
              "connections": flood, "connected": len(sockets),
              "refused_by_kernel": refused, "before": before,
              "during": during, "serves_during_flood": serves_during,
              "pull_alive": pull.poll() is None,
              "serves_after_s": recovered_after, "after": after}
    result["ok"] = result["pull_alive"] and recovered_after is not None
    if not result["ok"]:
        result["why"] = ("pull exited" if not result["pull_alive"] else
                         "the listener no longer accepts connections after "
                         "the flood ended")
    return result


def case_oversized_headers(pull, port: int, count: int) -> dict:
    before = proc_status(pull.pid)
    sockets = []
    announced = listener.MAX_PAYLOAD - 1
    header = bytes([announced & 0xFF, (announced >> 8) & 0xFF,
                    (announced >> 16) & 0xFF, 1])
    for _ in range(count):
        try:
            client = listener.Listener("127.0.0.1", port, timeout=2)
        except OSError:
            break
        # the server greeting first, then a header of a client greeting
        # that announces almost 16 MB and never sends it
        client.read_frame()
        try:
            client.sock.sendall(header + b"\x00" * 64)
        except OSError:
            pass
        sockets.append(client.sock)
    time.sleep(2)
    during = proc_status(pull.pid)
    close_all(sockets)
    recovered_after = serves_within(port, READ_TIMEOUT * 3)
    growth = (during["rss_kib"] - before["rss_kib"]
              if during["rss_kib"] >= 0 and before["rss_kib"] >= 0 else -1)
    # a pre-allocation would be ~16 MiB per connection; allow 256 KiB each
    budget = 256 * len(sockets)
    result = {"case": "oversized_headers", "connections": len(sockets),
              "announced_bytes": announced, "before": before,
              "during": during, "rss_growth_kib": growth,
              "rss_budget_kib": budget, "pull_alive": pull.poll() is None,
              "serves_after_s": recovered_after}
    result["ok"] = (result["pull_alive"] and recovered_after is not None and
                    0 <= growth <= budget)
    if not result["ok"]:
        result["why"] = ("pull exited" if not result["pull_alive"] else
                         "not serving after the connections closed"
                         if recovered_after is None else
                         f"resident memory grew by {growth} KiB for "
                         f"{len(sockets)} connections")
    return result


def case_payload_drip(pull, port: int) -> dict:
    client = listener.Listener("127.0.0.1", port, timeout=2)
    client.read_frame()
    announced = 4096
    client.sock.sendall(bytes([announced & 0xFF, (announced >> 8) & 0xFF,
                               0, 1]))

    def feed(_elapsed):
        client.sock.sendall(b"\x00")

    limit = READ_TIMEOUT * 4
    closed = closed_within(client.sock, limit, feed)
    client.close()
    result = {"case": "payload_drip", "read_timeout": READ_TIMEOUT,
              "closed_after_s": closed, "pull_alive": pull.poll() is None}
    result["ok"] = (result["pull_alive"] and closed is not None and
                    closed <= READ_TIMEOUT + 3)
    if not result["ok"]:
        result["why"] = (f"a payload sent one byte a second was still being "
                         f"read after {limit} s" if closed is None else
                         f"closed only after {closed} s")
    return result


def case_header_drip(pull, port: int) -> dict:
    client = listener.Listener("127.0.0.1", port, timeout=2)
    client.read_frame()
    # one frame of 1 byte (a garbage greeting) every read_timeout - 1 s: each
    # header and payload arrives in time, so only the protocol ends it
    interval = READ_TIMEOUT - 1
    state = {"next": 0.0, "sequence": 1, "frames": 0}

    def feed(elapsed):
        if elapsed >= state["next"]:
            client.sock.sendall(listener.frame(state["sequence"] & 0xFF,
                                               b"\x00"))
            state["sequence"] += 1
            state["frames"] += 1
            state["next"] = elapsed + interval

    limit = interval * 12
    closed = closed_within(client.sock, limit, feed)
    client.close()
    result = {"case": "header_drip", "interval_s": interval,
              "frames_sent": state["frames"], "closed_after_s": closed,
              "pull_alive": pull.poll() is None}
    result["ok"] = result["pull_alive"] and closed is not None
    if not result["ok"]:
        result["why"] = (f"a client that never logs in was kept for "
                         f"{limit} s ({state['frames']} frames)")
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--mysql-basedir", type=Path, required=True)
    parser.add_argument("--binsrv", type=Path, required=True)
    parser.add_argument("--workdir", type=Path, required=True)
    parser.add_argument("--port", type=int, default=23520)
    parser.add_argument("--mode", choices=["gtid", "position"],
                        default="position")
    parser.add_argument("--fd-limit", type=int, default=256,
                        help="RLIMIT_NOFILE of the pull process")
    parser.add_argument("--header-connections", type=int, default=150)
    parser.add_argument("--cases", default="oversized_headers,payload_drip,"
                        "header_drip,idle_flood",
                        help="comma-separated; idle_flood runs last by "
                        "default as it may leave the listener deaf")
    args = parser.parse_args()
    args.mysql_basedir = args.mysql_basedir.expanduser()
    listener.MYSQLBINLOG = args.mysql_basedir / "bin" / "mysqlbinlog"
    args.binsrv = args.binsrv.expanduser()
    args.workdir.mkdir(parents=True, exist_ok=True)

    gtid_mode = args.mode == "gtid"
    source = harness.MySQLServer(args.mysql_basedir, args.workdir / "mysql",
                                 args.port, gtid_mode, server_id=1)
    harness.SERVER = source
    binsrv = harness.BinlogServer(args.binsrv, args.workdir / "binsrv",
                                  args.port, gtid_mode)
    harness.BINSRV = binsrv
    pull_port = args.port + 2
    results = []
    pull = None
    source.start()
    try:
        machine = harness.FetchCrashMachine()
        machine.start_from_scratch()
        machine.run_workload(WORKLOAD)
        machine.fetch_to_the_end()
        binsrv.write_config()
        config = json.loads(binsrv.config_path.read_text())
        config["replication_source"] = {
            "port": pull_port, "server_id": 142,
            "server_uuid": "00000000-0000-0000-0000-00000000008e",
            "read_timeout": READ_TIMEOUT, "write_timeout": READ_TIMEOUT,
            "require_secure_transport": False,
            "auth_rsa_private_key": str(listener.AUTH_RSA_PRIVATE_KEY),
            "authentication": {"user": listener.PULL_USER,
                               "password": listener.PULL_PASSWORD,
                               "plugin": "caching_sha2_password"}}
        binsrv.config_path.write_text(json.dumps(config, indent=2))
        fd_limit = args.fd_limit

        def lower_fd_limit():
            resource.setrlimit(resource.RLIMIT_NOFILE, (fd_limit, fd_limit))

        pull_log = (args.workdir / "pull.log").open("wb")
        pull = subprocess.Popen(
            [str(binsrv.binary), "pull", str(binsrv.config_path)],
            stdout=pull_log, stderr=subprocess.STDOUT,
            preexec_fn=lower_fd_limit,
            env=dict(os.environ, AWS_EC2_METADATA_DISABLED="true",
                     ASAN_OPTIONS="abort_on_error=1:exitcode=134"))
        if serves_within(pull_port, 30) is None:
            raise harness.Failure("the listener does not serve before the "
                                  "test")
        for case in args.cases.split(","):
            if pull.poll() is not None:
                results.append({"case": case, "ok": False,
                                "why": "pull had exited"})
                continue
            if case == "idle_flood":
                result = case_idle_flood(pull, pull_port, fd_limit)
            elif case == "oversized_headers":
                result = case_oversized_headers(pull, pull_port,
                                                args.header_connections)
            elif case == "payload_drip":
                result = case_payload_drip(pull, pull_port)
            elif case == "header_drip":
                result = case_header_drip(pull, pull_port)
            else:
                raise SystemExit(f"unknown case {case}")
            print(json.dumps(result), flush=True)
            results.append(result)
    finally:
        if pull is not None and pull.poll() is None:
            pull.send_signal(signal.SIGTERM)
            try:
                pull.wait(timeout=60)
            except subprocess.TimeoutExpired:
                pull.kill()
        source.stop()
    failures = [result for result in results if not result.get("ok")]
    print(json.dumps({"cases": len(results), "failures": len(failures),
                      "passed": not failures}), flush=True)
    (args.workdir / "result.json").write_text(json.dumps(results, indent=2))
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
