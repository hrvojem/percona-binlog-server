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

"""Makes every kind of file system call 'binlog_server fetch' makes in its
storage directory fail, and checks what happens.

For each operation (open for writing, write, fsync, rename, truncate) x
storage file (binlog data, binlog metadata, binlog index, storage
metadata) x error (ENOSPC, EIO) x repetition (the first matching call only,
or every matching call) the fsfault LD_PRELOAD library injects the failure
into one 'fetch' run:

1. a workload runs on the MySQL server;
2. 'fetch' runs with the failure: it must exit with 0 or 1 within the
   timeout;
3. the stored binlogs must be prefixes of the server's, ending at
   transaction boundaries (the checks of fetch_crash_test.py);
4. without the failure, 'fetch' must exit with 0 and leave exact copies.

The result of every case is printed as one JSON line and collected in
'<workdir>/sweep.json'. Uses the helpers of fetch_crash_test.py.
"""

import argparse
import itertools
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import fetch_crash_test as harness  # noqa: E402

OPERATIONS = ["open", "write", "fsync", "rename", "truncate"]
ERRORS = ["ENOSPC", "EIO"]
COUNTS = [1, 1000]
WORKLOAD = [{"rows": [3000, 150_000], "ddl": False},
            {"rows": [500], "ddl": True}]


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--mysql-basedir", type=Path, required=True)
    parser.add_argument("--binsrv", type=Path, required=True)
    parser.add_argument("--fsfault-lib", type=Path, required=True)
    parser.add_argument("--workdir", type=Path, required=True)
    parser.add_argument("--port", type=int, default=23406)
    parser.add_argument("--mode", choices=["gtid", "position"],
                        default="position")
    args = parser.parse_args()

    gtid_mode = args.mode == "gtid"
    args.workdir.mkdir(parents=True, exist_ok=True)
    harness.FSFAULT_LIB = args.fsfault_lib.expanduser().resolve()
    harness.SERVER = harness.MySQLServer(args.mysql_basedir.expanduser(),
                                         args.workdir / "mysql", args.port,
                                         gtid_mode)
    harness.BINSRV = harness.BinlogServer(args.binsrv.expanduser(),
                                          args.workdir / "binsrv", args.port,
                                          gtid_mode)
    log = args.workdir / "fsfault.log"
    results = []
    harness.SERVER.start()
    try:
        for operation, target, error, count in itertools.product(
                OPERATIONS, sorted(harness.DISK_TARGETS), ERRORS, COUNTS):
            fault = {"op": operation, "target": target, "nth": 1,
                     "count": count, "errno": error}
            machine = harness.FetchCrashMachine()
            result = {"fault": fault}
            try:
                machine.start_from_scratch()
                # a binlog with data first, so that later calls extend and
                # replace existing files
                machine.run_workload(WORKLOAD)
                machine.fetch_to_the_end()
                for statement in harness.workload_sql(WORKLOAD):
                    harness.SERVER.sql(statement)
                exit_code, injected = harness.fetch_with_disk_fault_once(
                    fault, log)
                result["hit"] = injected > 0
                result["exit"] = exit_code
                machine.storage_holds_prefixes()
                machine.fetch_to_the_end()
                machine.storage_holds_prefixes()
                result["recovered"] = True
            except Exception as failure:  # every case is reported
                result["failure"] = f"{type(failure).__name__}: {failure}"[:600]
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
