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

"""GTID auto-position replicas of 'binlog_server pull' (PBS-29) at the edges.

A GTID-mode source and 'pull' on a fresh storage; replicas connect with
SOURCE_AUTO_POSITION=1. Cases (one JSON line each):

  catch_up   pull starts on an empty storage and the replica connects at
             once, while the source writes for --duration seconds with a
             rotation every ROTATE_EVERY transactions: the replica ends with
             the source's data;
  restart    during that workload the replica is stopped and started again
             RESTARTS times (auto-positioning resumes from its own GTID
             set): it still ends with the source's data, nothing missing or
             applied twice;
  purged     pull is stopped, its oldest binlogs are purged
             (purge_binlogs, with nothing else using the storage), pull is
             started again, and a new replica with an empty GTID set
             connects: as a MySQL source does, pull refuses it with ER 1236
             (the replica lacks GTIDs the source no longer has) instead of
             streaming what is left;
  ahead      a replica that has a GTID of the source's UUID beyond anything
             the source wrote connects: as a MySQL source does, pull refuses
             it with ER 1236 (the replica has more GTIDs than the source).

Uses the helpers of fetch_crash_test.py and pull_replica_test.py. Example:

  PYTHONPATH=~/ws/qa/pylib python3 pull_gtid_test.py \\
    --mysql-basedir ~/ws/dist/mysql-8.4.8-linux-glibc2.28-x86_64-minimal \\
    --binsrv ~/ws/percona-binlog-server-build-debug_gcc14/binlog_server \\
    --workdir ~/ws/qa/pull-gtid
"""

import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import fetch_crash_test as harness  # noqa: E402
import pull_replica_test as replication  # noqa: E402

TRANSACTION = [{"rows": [2000, 500], "ddl": False}]
TRANSACTION_SECONDS = 0.2
ROTATE_EVERY = 25
RESTARTS = 2
CATCH_UP_SECONDS = 120
REFUSAL_SECONDS = 30


def io_state(replica) -> list:
    return replica.sql(
        "SELECT SERVICE_STATE, LAST_ERROR_NUMBER, LAST_ERROR_MESSAGE FROM "
        "performance_schema.replication_connection_status").strip().split(
            "\t")


def wait_matching(source, replica) -> bool:
    expected = replication.checksum(source)
    deadline = time.monotonic() + CATCH_UP_SECONDS
    while time.monotonic() < deadline:
        if replication.checksum(replica) == expected:
            return True
        time.sleep(2)
    return False


def wait_refusal(replica) -> list:
    """The replica's I/O state once it reports an error, or the last state
    after REFUSAL_SECONDS."""
    deadline = time.monotonic() + REFUSAL_SECONDS
    state = io_state(replica)
    while time.monotonic() < deadline:
        state = io_state(replica)
        if len(state) > 1 and state[1] not in ("", "0"):
            break
        time.sleep(1)
    return state


def refusal(state: list) -> bool:
    """Whether the replica's I/O thread stopped on ER 1236 from the source
    (reported by the replica as ER_SERVER_SOURCE_FATAL_ERROR_READING_BINLOG,
    13114)."""
    return len(state) > 1 and state[1] == "13114" or \
        len(state) > 2 and "1236" in state[2]


def run(args) -> list:
    work = args.workdir
    source = harness.MySQLServer(args.mysql_basedir, work / "source",
                                 args.port, True, server_id=1)
    replica = harness.MySQLServer(args.mysql_basedir, work / "replica",
                                  args.port + 10, True, server_id=10)
    harness.SERVER = source
    harness.BINSRV = binsrv = harness.BinlogServer(
        args.binsrv, work / "binsrv", args.port, True)
    binsrv.checkpoint_size = "1"
    pull_port = args.port + 2
    results = {case: {"case": case}
               for case in ("catch_up", "restart", "purged", "ahead")}
    pull = None
    source.start()
    replica.start()
    try:
        machine = harness.FetchCrashMachine()
        machine.start_from_scratch()
        for _ in range(3):
            machine.run_workload(TRANSACTION)
        pull = replication.start_pull(binsrv, pull_port)
        replication.replicate_from_pull(replica, pull_port, True,
                                        "SOURCE_AUTO_POSITION=1")

        transactions = int(args.duration / TRANSACTION_SECONDS)
        restart_at = {transactions * (index + 1) // (RESTARTS + 1)
                      for index in range(RESTARTS)}
        restarts = 0
        for number in range(transactions):
            if number in restart_at:
                replica.sql("STOP REPLICA")
                time.sleep(1)
                replica.sql("START REPLICA")
                restarts += 1
            machine.run_workload(TRANSACTION)
            if number % ROTATE_EVERY == ROTATE_EVERY - 1:
                machine.flush_binary_logs()
            time.sleep(TRANSACTION_SECONDS)
        machine.flush_binary_logs()
        matched = wait_matching(source, replica)
        state = io_state(replica)
        results["catch_up"].update(transactions=transactions,
                                   binlogs=len(source.binlog_names()),
                                   data_matches=matched, io_state=state,
                                   ok=matched)
        executed = replica.sql("SELECT @@GLOBAL.GTID_EXECUTED").strip()
        source_executed = source.sql("SELECT @@GLOBAL.GTID_EXECUTED").strip()
        results["restart"].update(restarts=restarts, data_matches=matched,
                                  gtid_executed_matches=(
                                      executed == source_executed),
                                  ok=matched and executed == source_executed
                                  and restarts == RESTARTS)
        replica.sql("STOP REPLICA")

        # purged: the oldest binlogs go, with nothing else using the storage
        pull.terminate()
        pull.wait(timeout=120)
        pull = None
        names = sorted(path.name for path in binsrv.storage.iterdir()
                       if path.name.startswith("binlog.") and
                       path.suffix not in (".json", ".index", ".tmp"))
        purge = subprocess.run(
            [str(binsrv.binary), "purge_binlogs", str(binsrv.config_path),
             names[len(names) // 2]],
            capture_output=True, text=True, timeout=120,
            env=dict(os.environ, AWS_EC2_METADATA_DISABLED="true"))
        pull = replication.start_pull(binsrv, pull_port)
        replication.replicate_from_pull(replica, pull_port, True,
                                        "SOURCE_AUTO_POSITION=1")
        state = wait_refusal(replica)
        refused = refusal(state)
        time.sleep(5)
        results["purged"].update(purge_exit=purge.returncode,
                                 purged_up_to=names[len(names) // 2],
                                 io_state=state, received=replica.sql(
                                     "SELECT RECEIVED_TRANSACTION_SET FROM "
                                     "performance_schema."
                                     "replication_connection_status").strip(),
                                 ok=purge.returncode == 0 and refused)
        replica.sql("STOP REPLICA")

        # ahead: a GTID of the source's UUID the source never wrote
        source_uuid = source.sql("SELECT @@GLOBAL.SERVER_UUID").strip()
        # it has everything the source wrote, so that only being ahead can
        # be refused
        replication.replicate_from_pull(
            replica, pull_port, True, "SOURCE_AUTO_POSITION=1",
            f"SET GLOBAL GTID_PURGED='{source_executed}'; "
            f"SET GTID_NEXT='{source_uuid}:1000000'; BEGIN; COMMIT; "
            "SET GTID_NEXT='AUTOMATIC';")
        state = wait_refusal(replica)
        refused = refusal(state)
        results["ahead"].update(io_state=state,
                                source_gtid_executed=source_executed,
                                ok=refused)
        replica.sql("STOP REPLICA")
    finally:
        if pull is not None and pull.poll() is None:
            pull.terminate()
            pull.wait(timeout=120)
        source.stop()
        replica.stop()
    for result in results.values():
        result.setdefault("ok", False)
    return list(results.values())


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--mysql-basedir", type=Path, required=True)
    parser.add_argument("--binsrv", type=Path, required=True)
    parser.add_argument("--workdir", type=Path, required=True)
    parser.add_argument("--port", type=int, default=23850)
    parser.add_argument("--mode", choices=["gtid"], default="gtid")
    parser.add_argument("--duration", type=int, default=60,
                        help="seconds of workload")
    args = parser.parse_args()
    args.mysql_basedir = args.mysql_basedir.expanduser()
    args.binsrv = args.binsrv.expanduser()
    args.workdir.mkdir(parents=True, exist_ok=True)
    results = run(args)
    for result in results:
        print(json.dumps(result), flush=True)
    (args.workdir / "results.json").write_text(json.dumps(results, indent=2))
    return 0 if all(result.get("ok") for result in results) else 1


if __name__ == "__main__":
    sys.exit(main())
