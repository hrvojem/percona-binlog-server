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

"""Network faults between 'binlog_server pull' and a MySQL replica.

network_fault_test.py puts Toxiproxy between the source and the Binlog
Server. Here it sits between 'pull' and a MySQL replica of it: the replica
connects to the proxy, which forwards to the listener. The source keeps
writing while one fault after another hits the replica's connection:

  reset              the connection is reset at once;
  cut_1k, cut_100k   the stream to the replica stops after 1 KB / 100 KB
                     and the connection closes;
  cut_mid_event      ... after 3 MB, inside an 8 MB row event;
  cut_handshake      the replica's side stops after 200 bytes, and the
                     connection is reset once so that the replica logs in
                     again into that limit (an established replica sends
                     almost nothing);
  stall              the connection stays open but carries nothing, longer
                     than the replica's replica_net_timeout;
  refused            the proxy refuses connections for 6 s;
  sliced             the stream arrives in 1-byte pieces (no outage);
  latency            300 ms latency with jitter (no outage);
  bandwidth          100 KB/s (no outage).

After each fault is removed the replica must reconnect on its own and end
with the source's data (row count and checksum; in GTID mode also
GTID_EXECUTED) with no applier error, its I/O thread running, and 'pull'
still up without leaking file descriptors. A position-mode replica resumes
at the position it had, so a transaction applied twice or skipped shows up
as an applier error or a different checksum. One JSON line per fault.

Uses the helpers of fetch_crash_test.py and pull_replica_test.py and a
Toxiproxy server (--toxiproxy-api). Example:

  PYTHONPATH=~/ws/qa/pylib python3 pull_replica_fault_test.py \\
    --mysql-basedir ~/ws/dist/mysql-8.4.8-linux-glibc2.28-x86_64-minimal \\
    --binsrv ~/ws/percona-binlog-server-build-debug_gcc14/binlog_server \\
    --workdir ~/ws/qa/pull-replica-fault --mode gtid
"""

import argparse
import json
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import fetch_crash_test as harness  # noqa: E402
import pull_replica_test as replication  # noqa: E402

PROXY_NAME = "pull-replica"
REPLICA_NET_TIMEOUT = 8
HEARTBEAT_PERIOD = 2
CATCH_UP_SECONDS = 180
ROW_BYTES = 20_000
BIG_ROW_BYTES = 8 * 1024 * 1024

# faults that must break the replica's connection (the others only slow it
# down); a run where one of them does not counts as a failure, since the
# fault then tested nothing
OUTAGES = {"reset", "cut_1k", "cut_100k", "cut_mid_event", "cut_handshake",
           "stall", "refused"}
# (name, toxic type or "disabled", stream, attributes, seconds the fault
# stays, rows written meanwhile, whether an 8 MB row is written meanwhile)
FAULTS = [
    ("reset", "reset_peer", "downstream", {"timeout": 0}, 3, 40, False),
    ("cut_1k", "limit_data", "downstream", {"bytes": 1_000}, 3, 40, False),
    ("cut_100k", "limit_data", "downstream", {"bytes": 100_000}, 3, 40,
     False),
    ("cut_mid_event", "limit_data", "downstream", {"bytes": 3_000_000}, 4,
     10, True),
    ("cut_handshake", "limit_data", "upstream", {"bytes": 200}, 3, 40,
     False),
    ("stall", "timeout", "downstream", {"timeout": 0},
     REPLICA_NET_TIMEOUT + 6, 40, False),
    ("refused", "disabled", "", {}, 6, 40, False),
    ("sliced", "slicer", "downstream",
     {"average_size": 1, "size_variation": 0, "delay": 0}, 6, 20, False),
    ("latency", "latency", "downstream", {"latency": 300, "jitter": 100}, 6,
     40, False),
    ("bandwidth", "bandwidth", "downstream", {"rate": 100}, 6, 20, True),
]


class Toxiproxy:
    """One proxy of a Toxiproxy server, named PROXY_NAME."""

    def __init__(self, api: str):
        self.api = api.rstrip("/")

    def _call(self, method: str, path: str, body=None):
        request = urllib.request.Request(
            self.api + path, method=method,
            data=None if body is None else json.dumps(body).encode(),
            headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(request, timeout=10) as response:
            text = response.read()
            return json.loads(text) if text else None

    def create(self, listen: str, upstream: str):
        self.delete()
        self._call("POST", "/proxies", {"name": PROXY_NAME, "listen": listen,
                                        "upstream": upstream,
                                        "enabled": True})

    def delete(self):
        try:
            self._call("DELETE", f"/proxies/{PROXY_NAME}")
        except urllib.error.HTTPError:
            pass

    def apply(self, toxic_type: str, stream: str, attributes: dict):
        if toxic_type == "disabled":
            self._call("POST", f"/proxies/{PROXY_NAME}", {"enabled": False})
            return
        self._call("POST", f"/proxies/{PROXY_NAME}/toxics",
                   {"name": "fault", "type": toxic_type, "stream": stream,
                    "toxicity": 1.0, "attributes": attributes})

    def kick(self, send_data):
        """Resets the open connections once, so that clients reconnect;
        reset_peer acts when data passes, so 'send_data' makes some."""
        self._call("POST", f"/proxies/{PROXY_NAME}/toxics",
                   {"name": "kick", "type": "reset_peer",
                    "stream": "downstream", "toxicity": 1.0,
                    "attributes": {"timeout": 0}})
        send_data()
        time.sleep(1)
        self._call("DELETE", f"/proxies/{PROXY_NAME}/toxics/kick")

    def clear(self):
        for name in ("fault", "kick"):
            try:
                self._call("DELETE", f"/proxies/{PROXY_NAME}/toxics/{name}")
            except urllib.error.HTTPError:
                pass
        self._call("POST", f"/proxies/{PROXY_NAME}", {"enabled": True})


def open_descriptors(pid: int) -> int:
    return len(list(Path(f"/proc/{pid}/fd").iterdir()))


def write_rows(source, rows: int, big: bool) -> None:
    statements = [f"INSERT INTO qa.t (payload) VALUES (REPEAT('r', "
                  f"{ROW_BYTES}));" for _ in range(rows)]
    for start in range(0, len(statements), 20):
        source.sql(" ".join(statements[start:start + 20]) +
                   " FLUSH BINARY LOGS;")
    if big:
        source.sql(f"INSERT INTO qa.t (payload) VALUES "
                   f"(REPEAT('B', {BIG_ROW_BYTES}));")


def executed(server) -> str:
    return server.sql("SELECT @@GLOBAL.GTID_EXECUTED").strip().replace(
        "\\n", "").replace("\n", "")


def replica_state(replica) -> dict:
    io_state = replica.sql(
        "SELECT SERVICE_STATE FROM performance_schema."
        "replication_connection_status").strip()
    applier_errors = []
    for line in replica.sql(
            "SELECT LAST_ERROR_NUMBER, LAST_ERROR_MESSAGE FROM "
            "performance_schema.replication_applier_status_by_worker"
            ).splitlines():
        number, _, message = line.partition("\t")
        if number not in ("", "0"):
            applier_errors.append(f"{number}: {message[:300]}")
    # the replica clears its connection error once it reconnects, so a
    # lost connection is counted in its error log instead
    return {"io_state": io_state, "applier_errors": applier_errors,
            "disconnects": disconnects(replica)}


def disconnects(replica) -> int:
    """Lost connections to the source so far, from the replica's log."""
    if not replica.error_log.exists():
        return 0
    text = replica.error_log.read_text(errors="replace")
    return (text.count("Error reading packet from server") +
            text.count("Error reconnecting to source"))


def catch_up(source, replica, gtid_mode: bool) -> "float | None":
    """Seconds until the replica holds the source's data, None if it does
    not within CATCH_UP_SECONDS or its applier fails."""
    started = time.monotonic()
    expected = replication.checksum(source)
    source_executed = executed(source) if gtid_mode else ""
    while time.monotonic() - started < CATCH_UP_SECONDS:
        if replication.checksum(replica) == expected and (
                not gtid_mode or replica.sql(
                    f"SELECT GTID_SUBSET('{source_executed}', "
                    f"'{executed(replica)}')").strip() == "1"):
            return time.monotonic() - started
        if replica_state(replica)["applier_errors"]:
            return None
        time.sleep(1)
    return None


def run(args) -> list:
    work = args.workdir
    gtid_mode = args.mode == "gtid"
    source = harness.MySQLServer(args.mysql_basedir, work / "source",
                                 args.port, gtid_mode, server_id=1)
    replica = harness.MySQLServer(args.mysql_basedir, work / "replica",
                                  args.port + 10, gtid_mode, server_id=10)
    binsrv = harness.BinlogServer(args.binsrv, work / "binsrv", args.port,
                                  gtid_mode)
    binsrv.reset()
    binsrv.checkpoint_size = "1"
    pull_port = args.port + 2
    proxy_port = args.port + 4
    proxy = Toxiproxy(args.toxiproxy_api)
    proxy.create(f"127.0.0.1:{proxy_port}", f"127.0.0.1:{pull_port}")
    start = ("SOURCE_AUTO_POSITION=1" if gtid_mode else
             "SOURCE_LOG_FILE='binlog.000001', SOURCE_LOG_POS=4")
    start += (f", SOURCE_HEARTBEAT_PERIOD={HEARTBEAT_PERIOD}, "
              "SOURCE_RETRY_COUNT=100000")
    results = []
    pull = None
    source.start()
    replica.start()
    try:
        source.reset()
        replica.sql(f"SET GLOBAL replica_net_timeout = {REPLICA_NET_TIMEOUT}")
        pull = replication.start_pull(binsrv, pull_port)
        write_rows(source, 40, False)
        replication.replicate_from_pull(replica, proxy_port, gtid_mode,
                                        start)
        first = catch_up(source, replica, gtid_mode)
        results.append({"case": "baseline", "catch_up_seconds":
                        None if first is None else round(first, 1),
                        **replica_state(replica),
                        "ok": first is not None})
        print(json.dumps(results[-1]), flush=True)
        baseline_descriptors = open_descriptors(pull.pid)
        wanted = args.faults.split(",") if args.faults else None
        for (name, toxic_type, stream, attributes, seconds, rows,
             big) in FAULTS:
            if wanted is not None and name not in wanted:
                continue
            result = {"case": name}
            try:
                before = disconnects(replica)
                proxy.apply(toxic_type, stream, attributes)
                if name == "cut_handshake":
                    proxy.kick(lambda: write_rows(source, 1, False))
                write_rows(source, rows, big)
                time.sleep(seconds)
                proxy.clear()
                took = catch_up(source, replica, gtid_mode)
                state = replica_state(replica)
                state["disconnects"] -= before
                descriptors = (open_descriptors(pull.pid)
                               if pull.poll() is None else None)
                result.update({
                    "catch_up_seconds": None if took is None else
                    round(took, 1),
                    **state, "pull_alive": pull.poll() is None,
                    "pull_descriptors": descriptors,
                    "pull_descriptors_at_start": baseline_descriptors})
                hit = name not in OUTAGES or state["disconnects"] > 0
                result["fault_hit"] = hit
                result["ok"] = (took is not None and hit and
                                state["io_state"] == "ON" and
                                not state["applier_errors"] and
                                pull.poll() is None and
                                descriptors <= baseline_descriptors + 20)
            except Exception as error:  # reported as the case's failure
                proxy.clear()
                result.update({"error": harness.describe_failure(error, 600),
                               "ok": False})
            results.append(result)
            print(json.dumps(result), flush=True)
            if pull.poll() is not None:
                break
    finally:
        proxy.delete()
        if pull is not None and pull.poll() is None:
            pull.terminate()
            pull.wait(timeout=120)
        source.stop()
        replica.stop()
    return results


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--mysql-basedir", type=Path, required=True)
    parser.add_argument("--binsrv", type=Path, required=True)
    parser.add_argument("--workdir", type=Path, required=True)
    parser.add_argument("--port", type=int, default=24030)
    parser.add_argument("--mode", choices=["position", "gtid"],
                        default="gtid")
    parser.add_argument("--toxiproxy-api", default="http://127.0.0.1:8474")
    parser.add_argument("--faults", default="",
                        help="comma-separated fault names (default: all)")
    args = parser.parse_args()
    args.mysql_basedir = args.mysql_basedir.expanduser()
    args.binsrv = args.binsrv.expanduser()
    args.workdir = args.workdir.expanduser().resolve()
    args.workdir.mkdir(parents=True, exist_ok=True)
    results = run(args)
    (args.workdir / "results.json").write_text(json.dumps(results, indent=2))
    return 0 if all(result.get("ok") for result in results) else 1


if __name__ == "__main__":
    sys.exit(main())
