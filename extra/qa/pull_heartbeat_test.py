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

"""Checks that a MySQL replica of 'binlog_server pull' keeps receiving
heartbeats while the source is quiet after a binlog rotation (PBS-30).

A replica asks for a heartbeat every PERIOD seconds and gives up on a silent
source after NET_TIMEOUT seconds (replica_net_timeout), then reconnects. After
the replica has caught up, the source stays quiet for a while; in that time
the replica must receive about one heartbeat per period and must not
reconnect. Cases ('pull' checkpoints after every transaction in all of them):

  quiet                  the source just stays quiet;
  quiet_after_rotation   the source rotates its binlog (FLUSH BINARY LOGS)
                         first. The collector keeps the new binlog's
                         FORMAT_DESCRIPTION and PREVIOUS_GTIDS_LOG buffered
                         until its first transaction, so the stored binlog
                         holds only the binlog magic, and no heartbeats are
                         sent from such a binlog (ticket 40, PBS-113).

Position mode only (pull does not serve GTID auto-position replicas yet).
Uses the helpers of fetch_crash_test.py and pull_replica_test.py. Example:

  PYTHONPATH=~/ws/qa/pylib python3 pull_heartbeat_test.py \\
    --mysql-basedir ~/ws/dist/mysql-8.4.8-linux-glibc2.28-x86_64-minimal \\
    --binsrv ~/ws/percona-binlog-server-build-debug_gcc14/binlog_server \\
    --workdir ~/ws/qa/pull-heartbeat
"""

import argparse
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import fetch_crash_test as harness  # noqa: E402
import pull_replica_test as replication  # noqa: E402

# whether the source rotates its binlog before staying quiet
CASES = {"quiet": False, "quiet_after_rotation": True}
WORKLOAD = [{"rows": [200], "ddl": False}]
PERIOD = 1
NET_TIMEOUT = 4
CATCH_UP_SECONDS = 60


def counters(replica, binsrv) -> dict:
    heartbeats = replica.sql(
        "SELECT COUNT_RECEIVED_HEARTBEATS "
        "FROM performance_schema.replication_connection_status").strip()
    replica_log = replica.error_log.read_text(errors="replace")
    pull_log = binsrv.log_path.read_text(errors="replace")
    return {
        "heartbeats": int(heartbeats or 0),
        # the replica's I/O thread logs this at every (re)connection
        "replica_connections": replica_log.count(
            "Replica receiver thread for channel"),
        # every replica connection registers itself (COM_REGISTER_SLAVE)
        "pull_connections": pull_log.count("ok (register replica)")}


def run_case(args, case: str) -> dict:
    work = args.workdir / case
    source = harness.MySQLServer(args.mysql_basedir, work / "source",
                                 args.port, False, server_id=1)
    replica = harness.MySQLServer(args.mysql_basedir, work / "replica",
                                  args.port + 1, False, server_id=2)
    harness.SERVER = source
    harness.BINSRV = binsrv = harness.BinlogServer(
        args.binsrv, work / "binsrv", args.port, False)
    pull_port = args.port + 2
    result = {"case": case}
    pull = None
    source.start()
    replica.start()
    try:
        machine = harness.FetchCrashMachine()
        machine.start_from_scratch()
        binsrv.checkpoint_size = "1"
        machine.run_workload(WORKLOAD)
        first_binlog = source.binlog_names()[0]
        pull = replication.start_pull(binsrv, pull_port)
        replica.sql(f"SET GLOBAL replica_net_timeout = {NET_TIMEOUT}")
        replication.replicate_from_pull(
            replica, pull_port, False,
            f"SOURCE_LOG_FILE='{first_binlog}', SOURCE_LOG_POS=4, "
            f"SOURCE_HEARTBEAT_PERIOD={PERIOD}")
        if CASES[case]:
            machine.flush_binary_logs()

        expected = replication.checksum(source)
        deadline = time.monotonic() + CATCH_UP_SECONDS
        while (replication.checksum(replica) != expected and
               time.monotonic() < deadline):
            time.sleep(1)
        result["caught_up"] = replication.checksum(replica) == expected
        time.sleep(1)

        before = counters(replica, binsrv)
        time.sleep(args.quiet)
        after = counters(replica, binsrv)
        result["quiet_seconds"] = args.quiet
        result.update({f"{name}_during_quiet": after[name] - before[name]
                       for name in before})
        # COUNT_RECEIVED_HEARTBEATS belongs to the current connection, so it
        # restarts after a reconnect; report the raw values too
        result["heartbeats_before_after"] = [before["heartbeats"],
                                             after["heartbeats"]]
        status = replica.sql(
            "SELECT SERVICE_STATE, LAST_ERROR_NUMBER, LAST_ERROR_MESSAGE "
            "FROM performance_schema.replication_connection_status"
        ).strip().split("\t")
        result["replica_io_thread"] = status
        minimum_heartbeats = args.quiet // PERIOD // 2
        result["ok"] = (result["caught_up"] and
                        result["replica_connections_during_quiet"] == 0 and
                        result["pull_connections_during_quiet"] == 0 and
                        result["heartbeats_during_quiet"] >= minimum_heartbeats)
        replica.sql("STOP REPLICA")
    finally:
        if pull is not None:
            pull.terminate()
            pull.wait(timeout=60)
        source.stop()
        replica.stop()
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--mysql-basedir", type=Path, required=True)
    parser.add_argument("--binsrv", type=Path, required=True)
    parser.add_argument("--workdir", type=Path, required=True)
    parser.add_argument("--port", type=int, default=23490)
    parser.add_argument("--cases", default=",".join(CASES))
    parser.add_argument("--quiet", type=int, default=20,
                        help="seconds the source stays quiet after rotating")
    args = parser.parse_args()
    args.mysql_basedir = args.mysql_basedir.expanduser()
    args.binsrv = args.binsrv.expanduser()
    args.workdir.mkdir(parents=True, exist_ok=True)
    results = [run_case(args, case) for case in args.cases.split(",")]
    for result in results:
        print(json.dumps(result), flush=True)
    (args.workdir / "results.json").write_text(json.dumps(results, indent=2))
    return 0 if all(result["ok"] for result in results) else 1


if __name__ == "__main__":
    sys.exit(main())
