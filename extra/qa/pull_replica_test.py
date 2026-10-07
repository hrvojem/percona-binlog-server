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

"""Replicates from 'binlog_server pull' with a real MySQL replica.

A source MySQL server gets a workload, 'binlog_server pull' streams it into
its storage and serves it on its replication source port, and a second MySQL
server is set up as a replica of 'pull' (CHANGE REPLICATION SOURCE TO ...,
START REPLICA), in position mode (from the first binlog, position 4) and in
GTID auto-position mode. The replica's data must become identical to the
source's within a timeout. When it does not, the replica's I/O thread error
and the statements 'pull' rejected are reported.

Uses the helpers of fetch_crash_test.py. Example:

  PYTHONPATH=~/ws/qa/pylib python3 pull_replica_test.py \\
    --mysql-basedir ~/ws/dist/mysql-8.4.8-linux-glibc2.28-x86_64-minimal \\
    --binsrv ~/ws/percona-binlog-server-build-debug_gcc14/binlog_server \\
    --workdir ~/ws/qa/pull-replica
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

PULL_USER = "rpl"
PULL_PASSWORD = "password"
WORKLOAD = [{"rows": [3000, 150_000], "ddl": False},
            {"rows": [500], "ddl": True}]
CATCH_UP_SECONDS = 60


def checksum(server) -> str:
    try:
        return server.sql("SELECT COUNT(*), COALESCE(SUM(CRC32(payload)), 0) "
                          "FROM qa.t").strip()
    except RuntimeError as error:
        return f"error: {str(error).splitlines()[0][:120]}"


def run_case(args, gtid_mode: bool) -> dict:
    mode = "gtid" if gtid_mode else "position"
    work = args.workdir / mode
    source = harness.MySQLServer(args.mysql_basedir, work / "source",
                                 args.port, gtid_mode, server_id=1)
    replica = harness.MySQLServer(args.mysql_basedir, work / "replica",
                                  args.port + 1, gtid_mode, server_id=2)
    harness.SERVER = source
    harness.BINSRV = binsrv = harness.BinlogServer(
        args.binsrv, work / "binsrv", args.port, gtid_mode)
    binsrv.checkpoint_size = "1"
    pull_port = args.port + 2
    result = {"mode": mode}
    pull = None
    source.start()
    replica.start()
    try:
        machine = harness.FetchCrashMachine()
        machine.start_from_scratch()
        machine.run_workload(WORKLOAD)
        binsrv.write_config()
        config = json.loads(binsrv.config_path.read_text())
        config["logger"]["level"] = "debug"
        config["replication_source"] = {
            "port": pull_port, "read_timeout": 60, "write_timeout": 60,
            "authentication": {"user": PULL_USER, "password": PULL_PASSWORD,
                               "plugin": "caching_sha2_password"}}
        binsrv.config_path.write_text(json.dumps(config, indent=2))
        pull = subprocess.Popen(
            [str(binsrv.binary), "pull", str(binsrv.config_path)],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            env=dict(os.environ, AWS_EC2_METADATA_DISABLED="true"))
        time.sleep(3)

        replica.sql("STOP REPLICA; RESET REPLICA ALL; "
                    "DROP DATABASE IF EXISTS qa;")
        if gtid_mode:
            replica.sql("RESET BINARY LOGS AND GTIDS"
                        if replica.version() >= (8, 2, 0) else "RESET MASTER")
        start = ("SOURCE_AUTO_POSITION=1" if gtid_mode else
                 f"SOURCE_LOG_FILE='{source.binlog_names()[0]}', "
                 "SOURCE_LOG_POS=4")
        replica.sql(
            f"CHANGE REPLICATION SOURCE TO SOURCE_HOST='127.0.0.1', "
            f"SOURCE_PORT={pull_port}, SOURCE_USER='{PULL_USER}', "
            f"SOURCE_PASSWORD='{PULL_PASSWORD}', GET_SOURCE_PUBLIC_KEY=1, "
            f"SOURCE_CONNECT_RETRY=2, {start}; START REPLICA;")

        expected = checksum(source)
        deadline = time.monotonic() + CATCH_UP_SECONDS
        actual = checksum(replica)
        while actual != expected and time.monotonic() < deadline:
            time.sleep(2)
            actual = checksum(replica)
        result["source"] = expected
        result["replica"] = actual
        result["replicated"] = actual == expected
        if not result["replicated"]:
            status = replica.sql(
                "SELECT SERVICE_STATE, LAST_ERROR_NUMBER, LAST_ERROR_MESSAGE "
                "FROM performance_schema.replication_connection_status"
            ).strip().split("\t")
            result["replica_io_thread"] = status
            log = binsrv.log_path.read_text(errors="replace").splitlines()
            rejected = []
            for index, line in enumerate(log):
                if "statement       :" in line:
                    statement = line.split("statement       :", 1)[1].strip()
                    following = " ".join(log[index + 1:index + 4])
                    if "syntax error" in following and \
                            statement not in rejected:
                        rejected.append(statement)
                if "unknown command" in line and \
                        "unknown command" not in rejected:
                    rejected.append("unknown command")
            result["rejected_by_pull"] = rejected
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
    parser.add_argument("--port", type=int, default=23470)
    parser.add_argument("--modes", default="position,gtid")
    args = parser.parse_args()
    args.mysql_basedir = args.mysql_basedir.expanduser()
    args.binsrv = args.binsrv.expanduser()
    args.workdir.mkdir(parents=True, exist_ok=True)
    results = [run_case(args, mode == "gtid")
               for mode in args.modes.split(",")]
    for result in results:
        print(json.dumps(result), flush=True)
    (args.workdir / "results.json").write_text(json.dumps(results, indent=2))
    return 0 if all(result["replicated"] for result in results) else 1


if __name__ == "__main__":
    sys.exit(main())
