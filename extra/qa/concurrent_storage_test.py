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

"""Runs several 'binlog_server fetch' instances against one storage at once.

The storage has only an in-process mutex, no cross-process lock (no flock,
lock file or O_EXCL), so nothing stops two utilities from writing to the same
storage directory. This is the shape of a real accident: a cron'd 'fetch'
that overruns its interval and overlaps the next run. This test starts
several 'fetch' instances on the same storage while a workload runs on the
source, then checks what the storage looks like.

Each instance gets its own replication server id, so the source serves them
all at once and they genuinely write concurrently (with a single shared
server id the source would instead keep dropping the older dump connection).

After the instances finish, two things are checked, with the helpers of
fetch_crash_test.py:
  * immediately: every stored binlog must still be a prefix of the source's,
    ending at a transaction boundary (storage_holds_prefixes). A larger file,
    a mismatch or a metadata size past the end means interleaved writes
    corrupted the storage.
  * after healing: one more workload and a single clean 'fetch' must bring
    the storage to an exact copy of the source (check_complete_copy), i.e.
    the damage, if any, is not permanent.

Results are printed as JSON. A non-zero exit means a run left the storage
corrupt or unrecoverable. Example:

  PYTHONPATH=~/ws/qa/pylib python3 concurrent_storage_test.py \\
    --mysql-basedir ~/ws/dist/mysql-8.4.8-linux-glibc2.28-x86_64-minimal \\
    --binsrv ~/ws/percona-binlog-server-build-asan_gcc14/binlog_server \\
    --workdir ~/ws/qa/concurrent --instances 3
"""

import argparse
import copy
import json
import os
import subprocess
import sys
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import fetch_crash_test as harness  # noqa: E402

# enough rows that a single fetch takes a few seconds, so the instances
# really overlap; a small checkpoint makes them rewrite the index and the
# metadata often, which is where interleaving hurts
PRIMING_WORKLOAD = [{"rows": [4000, 120_000], "ddl": False},
                    {"rows": [600, 90_000], "ddl": True},
                    {"rows": [300_000], "ddl": False}]
CONCURRENT_WORKLOAD = [{"rows": [2000, 80_000], "ddl": False},
                       {"rows": [250_000], "ddl": False},
                       {"rows": [400, 60_000], "ddl": True}]
FETCH_TIMEOUT = 180
SHARED_SERVER_ID = False


def start_instances(binsrv, count: int) -> list:
    """Writes one config per instance (distinct server id and log) pointing at
    the same storage, and starts a 'fetch' for each at once."""
    binsrv.checkpoint_size = "4K"
    binsrv.idle_time = 1
    binsrv.write_config()
    base = json.loads(binsrv.config_path.read_text())
    environment = dict(os.environ, AWS_EC2_METADATA_DISABLED="true")
    processes = []
    for index in range(count):
        config = copy.deepcopy(base)
        config["replication"]["server_id"] = (
            4242 if SHARED_SERVER_ID else 4242 + index)
        config["logger"]["file"] = str(binsrv.workdir / f"instance_{index}.log")
        config_path = binsrv.workdir / f"config_{index}.json"
        config_path.write_text(json.dumps(config, indent=2))
        processes.append(subprocess.Popen(
            [str(binsrv.binary), "fetch", str(config_path)],
            stdout=subprocess.DEVNULL, stderr=subprocess.PIPE,
            env=environment))
    return processes


def run_case(args) -> dict:
    gtid_mode = args.mode == "gtid"
    source = harness.MySQLServer(args.mysql_basedir, args.workdir / "mysql",
                                 args.port, gtid_mode, server_id=1)
    harness.SERVER = source
    binsrv = harness.BinlogServer(args.binsrv, args.workdir / "binsrv",
                                  args.port, gtid_mode)
    harness.BINSRV = binsrv
    result = {"mode": args.mode, "instances": args.instances}
    source.start()
    try:
        machine = harness.FetchCrashMachine()
        machine.start_from_scratch()
        machine.run_workload(PRIMING_WORKLOAD)

        # keep feeding the source while the instances run, so they all have
        # events to write at the same time
        workload_error = []

        def feed():
            try:
                for statement in harness.workload_sql(CONCURRENT_WORKLOAD):
                    source.sql(statement)
            except Exception as error:  # reported after the join
                workload_error.append(error)

        processes = start_instances(binsrv, args.instances)
        feeder = threading.Thread(target=feed)
        feeder.start()

        exit_codes = []
        stderrs = []
        for process in processes:
            try:
                _, stderr = process.communicate(timeout=FETCH_TIMEOUT)
            except subprocess.TimeoutExpired:
                process.kill()
                _, stderr = process.communicate()
                exit_codes.append("timeout")
                stderrs.append(stderr.decode(errors="replace")[-400:])
                continue
            exit_codes.append(process.returncode)
            if process.returncode not in (0, 1):
                stderrs.append(stderr.decode(errors="replace")[-400:])
        feeder.join()
        if workload_error:
            raise workload_error[0]
        result["exit_codes"] = exit_codes
        if stderrs:
            result["stderr_tails"] = stderrs

        # integrity right after the concurrent run, before any healing fetch
        try:
            machine.storage_holds_prefixes()
            result["corrupt_after_concurrent_run"] = False
        except harness.Failure as failure:
            result["corrupt_after_concurrent_run"] = True
            result["corruption"] = str(failure)[:600]

        # can a single clean fetch heal it to an exact copy of the source?
        source.sql("FLUSH BINARY LOGS")
        machine.run_workload([{"rows": [1000], "ddl": False}])
        try:
            machine.fetch_to_the_end()
            result["recovered_to_exact_copy"] = True
        except harness.Failure as failure:
            result["recovered_to_exact_copy"] = False
            result["recovery_error"] = str(failure)[:600]
    finally:
        source.stop()
    result["ok"] = (not result.get("corrupt_after_concurrent_run", True)
                    and result.get("recovered_to_exact_copy", False))
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--mysql-basedir", type=Path, required=True)
    parser.add_argument("--binsrv", type=Path, required=True)
    parser.add_argument("--workdir", type=Path, required=True)
    parser.add_argument("--port", type=int, default=23480)
    parser.add_argument("--mode", choices=["gtid", "position"],
                        default="position")
    parser.add_argument("--instances", type=int, default=3)
    parser.add_argument("--repeat", type=int, default=1)
    parser.add_argument("--shared-server-id", action="store_true",
                        help="all instances use one server id (one shared "
                             "config), as an overlapping cron job would")
    args = parser.parse_args()
    global SHARED_SERVER_ID
    SHARED_SERVER_ID = args.shared_server_id
    args.mysql_basedir = args.mysql_basedir.expanduser()
    args.binsrv = args.binsrv.expanduser()
    args.workdir.mkdir(parents=True, exist_ok=True)

    ok = True
    for attempt in range(args.repeat):
        result = run_case(args)
        result["attempt"] = attempt + 1
        print(json.dumps(result), flush=True)
        ok = ok and result["ok"]
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
