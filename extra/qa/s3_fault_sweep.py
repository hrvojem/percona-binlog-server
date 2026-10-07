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

"""Runs every S3 fault of s3_fault_proxy.py against every kind of S3 request
'binlog_server fetch' makes, and checks what happens.

For each fault (action) x request kind (method and object) x repetition
(once, or 6 consecutive requests, which outlasts the AWS SDK's retries):

1. a workload runs on the MySQL server;
2. 'fetch' runs while the proxy injects the fault into the first matching
   request: it must exit with 0 or 1 within the timeout;
3. the stored binlogs must be prefixes of the server's, ending at
   transaction boundaries (the checks of fetch_crash_test.py);
4. with the fault cleared, 'fetch' must exit with 0 and leave exact copies.

The result of every case (whether the fault was hit, the exit code of the
faulty fetch, and any failure) is printed as one JSON line and collected in
'<workdir>/sweep.json'. Uses the helpers of fetch_crash_test.py.
"""

import argparse
import itertools
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import fetch_crash_test as harness  # noqa: E402

ACTIONS = ["error500", "error503", "reset_before", "reset_after", "stall",
           "truncate_body"]
TARGETS = [("PUT", "binlog.0"), ("PUT", "binlog.index"), ("PUT", ".json"),
           ("GET", "binlog.0"), ("GET", "binlog.index"), ("GET", ".json"),
           ("GET", "list-type")]
COUNTS = [1, 6]
WORKLOAD = [{"rows": [3000, 150_000], "ddl": False},
            {"rows": [500], "ddl": True}]


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--mysql-basedir", type=Path, required=True)
    parser.add_argument("--binsrv", type=Path, required=True)
    parser.add_argument("--workdir", type=Path, required=True)
    parser.add_argument("--port", type=int, default=23366)
    parser.add_argument("--mode", choices=["gtid", "position"],
                        default="position")
    parser.add_argument("--s3-endpoint", required=True,
                        help="host:port of s3_fault_proxy.py")
    parser.add_argument("--s3-fault-control", required=True)
    parser.add_argument("--s3-bucket", default="pbs-mtr")
    parser.add_argument("--s3-bucket-dir", type=Path, required=True)
    parser.add_argument("--s3-access-key", default="pbsaccesskey")
    parser.add_argument("--s3-secret-key", default="pbs/secret+key=1")
    args = parser.parse_args()

    gtid_mode = args.mode == "gtid"
    args.workdir.mkdir(parents=True, exist_ok=True)
    harness.SERVER = harness.MySQLServer(args.mysql_basedir.expanduser(),
                                         args.workdir / "mysql", args.port,
                                         gtid_mode)
    faults = harness.FaultProxy(args.s3_fault_control)
    s3 = harness.S3Storage(args.s3_endpoint, args.s3_bucket,
                           args.s3_bucket_dir.expanduser(), args.s3_access_key,
                           args.s3_secret_key, f"fault-sweep-{args.mode}")
    harness.BINSRV = harness.BinlogServer(args.binsrv.expanduser(),
                                          args.workdir / "binsrv", args.port,
                                          gtid_mode, s3)
    results = []
    harness.SERVER.start()
    try:
        for action, (method, match), count in itertools.product(
                ACTIONS, TARGETS, COUNTS):
            fault = {"action": action, "method": method, "match": match,
                     "nth": 1, "count": count, "seconds": 3}
            machine = harness.FetchCrashMachine()
            result = {"fault": fault}
            try:
                machine.start_from_scratch()
                # a binlog with data on S3 first, so that later requests
                # read and extend existing objects
                machine.run_workload(WORKLOAD)
                machine.fetch_to_the_end()
                harness.FAULTS = faults
                harness.FAULT_LOG = args.workdir / "fault_outcomes.jsonl"
                machine.fetch_with_fault(fault, WORKLOAD)
                outcome = json.loads(harness.FAULT_LOG.read_text()
                                     .splitlines()[-1])
                result["hit"] = outcome["applied"] > 0
                result["exit"] = outcome["exit"]
                machine.storage_holds_prefixes()
                machine.fetch_to_the_end()
                machine.storage_holds_prefixes()
                result["recovered"] = True
            except Exception as error:  # every case is reported
                result["failure"] = f"{type(error).__name__}: {error}"[:600]
            finally:
                faults.set_rules([])
            print(json.dumps(result), flush=True)
            results.append(result)
    finally:
        harness.SERVER.stop()
    (args.workdir / "sweep.json").write_text(json.dumps(results, indent=2))
    failures = [result for result in results if "failure" in result]
    print(f"{len(results)} cases, {len(failures)} failures")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
