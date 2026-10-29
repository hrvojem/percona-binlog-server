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

Then two recovery cases: failing uploads of the binlog index leave the
objects of an interrupted binlog creation behind, the next 'fetch' (whose
start removes them, PBS-59) fails to delete the binlog metadata object, and
a third 'fetch' without a fault must still recover and leave exact copies
(ticket 37). Only the metadata delete is failed: a 'binlog.0' match would
hit both objects.

Last, one read case: 'pull' serves its S3 storage while the proxy answers
every ranged GET of binlog data with half of the range (shrink_body, a
well-formed but short answer): a mysqlbinlog client must fail rather than
receive wrong bytes, 'pull' must keep running, and with the fault cleared a
client must copy every closed binlog exactly.

The result of every case (whether the fault was hit, the exit code of the
faulty fetch, and any failure) is printed as one JSON line and collected in
'<workdir>/sweep.json'. Uses the helpers of fetch_crash_test.py.
"""

import argparse
import itertools
import json
import shutil
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import fetch_crash_test as harness  # noqa: E402
import pull_replica_test as replication  # noqa: E402

AUTH_RSA_PUBLIC_KEY = (Path(__file__).resolve().parents[2] / "mtr" /
                       "binlog_streaming" / "std_data" /
                       "caching_sha2_rsa_pub.pem")

ACTIONS = ["error500", "error503", "reset_before", "reset_after", "stall",
           "truncate_body"]
TARGETS = [("PUT", "binlog.0"), ("PUT", "binlog.index"), ("PUT", ".json"),
           ("GET", "binlog.0"), ("GET", "binlog.index"), ("GET", ".json"),
           ("GET", "list-type")]
COUNTS = [1, 6]
WORKLOAD = [{"rows": [3000, 150_000], "ddl": False},
            {"rows": [500], "ddl": True}]
RECOVERY_FIRST = [{"action": action, "method": "PUT", "match": "binlog.index",
                   "nth": 1, "count": 6, "seconds": 3}
                  for action in ["error500", "reset_before"]]
# the S3 CRT client retries a DELETE more often than the 6 times that make a
# PUT fail, so the delete fails for the whole start
RECOVERY_SECOND = {"action": "error500", "method": "DELETE", "match": ".json",
                   "nth": 1, "count": 50, "seconds": 3}


def run_recovery_case(first: dict, faults, fault_log: Path) -> dict:
    """One recovery case: 'first' during a rotation, RECOVERY_SECOND during
    the start after it, then a start without a fault."""
    machine = harness.FetchCrashMachine()
    result = {"fault": first, "recovery_fault": RECOVERY_SECOND}
    try:
        machine.start_from_scratch()
        machine.run_workload(WORKLOAD)
        machine.fetch_to_the_end()
        harness.FAULTS = faults
        harness.FAULT_LOG = fault_log
        machine.fetch_with_fault(first, WORKLOAD)
        outcome = json.loads(fault_log.read_text().splitlines()[-1])
        result["hit"] = outcome["applied"] > 0
        result["exit"] = outcome["exit"]
        machine.fetch_with_fault(RECOVERY_SECOND, [])
        outcome = json.loads(fault_log.read_text().splitlines()[-1])
        result["recovery_hit"] = outcome["applied"] > 0
        result["recovery_exit"] = outcome["exit"]
        machine.storage_holds_prefixes()
        machine.fetch_to_the_end()
        machine.storage_holds_prefixes()
        result["recovered"] = True
    except Exception as error:  # every case is reported
        result["failure"] = harness.describe_failure(error)
    finally:
        faults.set_rules([])
    return result


def run_pull_read_case(args, faults) -> dict:
    """pull serving its S3 storage while the proxy shrinks every ranged GET
    of binlog data (shrink_body): a client must fail rather than receive
    wrong bytes, pull must keep running, and once the fault is cleared a
    client must copy every closed binlog exactly."""
    fault = {"action": "shrink_body", "method": "GET", "match": "binlog.0",
             "nth": 1, "count": 1000}
    result = {"case": "pull_read_shrunk_range", "fault": fault}
    pull = None
    try:
        machine = harness.FetchCrashMachine()
        machine.start_from_scratch()
        machine.run_workload(WORKLOAD)
        machine.flush_binary_logs()
        machine.run_workload(WORKLOAD)
        machine.fetch_to_the_end()
        names = harness.SERVER.binlog_names()
        closed = names[:-1]
        pull_port = args.port + 2
        pull = replication.start_pull(harness.BINSRV, pull_port)

        def copy(directory: Path) -> subprocess.CompletedProcess:
            shutil.rmtree(directory, ignore_errors=True)
            directory.mkdir(parents=True)
            try:
                return subprocess.run(
                    [str(args.mysql_basedir.expanduser() / "bin" /
                         "mysqlbinlog"),
                     "--no-defaults", "--read-from-remote-server",
                     "--host=127.0.0.1", f"--port={pull_port}",
                     f"--user={replication.PULL_USER}",
                     f"--password={replication.PULL_PASSWORD}",
                     f"--server-public-key-path={AUTH_RSA_PUBLIC_KEY}",
                     "--ssl-mode=DISABLED", "--raw", "--to-last-log",
                     f"--result-file={directory}/", names[0]],
                    capture_output=True, text=True, timeout=120)
            except subprocess.TimeoutExpired as error:
                return subprocess.CompletedProcess(error.cmd, 124, "",
                                                   "timed out")

        def exact(directory: Path) -> bool:
            return all((directory / name).exists() and
                       (directory / name).read_bytes() ==
                       harness.SERVER.binlog_as_sent(name)
                       for name in closed)

        faults.set_rules([fault])
        shrunk = copy(args.workdir / "pull-copy-shrunk")
        applied = faults.stats()["rules"][0]["applied"]
        faults.set_rules([])
        result.update(hit=applied > 0, client_exit=shrunk.returncode,
                      client_error=shrunk.stderr.strip()[-200:],
                      wrong_bytes_streamed=shrunk.returncode == 0 and
                      not exact(args.workdir / "pull-copy-shrunk"),
                      pull_alive=pull.poll() is None)
        time.sleep(1)
        clean = copy(args.workdir / "pull-copy-clean")
        result.update(clean_client_exit=clean.returncode,
                      clean_copy_exact=exact(args.workdir /
                                             "pull-copy-clean"))
        if not (applied > 0 and not result["wrong_bytes_streamed"] and
                result["pull_alive"] and clean.returncode == 0 and
                result["clean_copy_exact"]):
            result["failure"] = "see the fields"
    except Exception as error:  # every case is reported
        result["failure"] = harness.describe_failure(error)
    finally:
        faults.set_rules([])
        if pull is not None and pull.poll() is None:
            pull.kill()
            pull.wait()
    return result


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
                result["failure"] = harness.describe_failure(error)
            finally:
                faults.set_rules([])
            print(json.dumps(result), flush=True)
            results.append(result)
        for first in RECOVERY_FIRST:
            result = run_recovery_case(first, faults,
                                       args.workdir / "fault_outcomes.jsonl")
            print(json.dumps(result), flush=True)
            results.append(result)
        result = run_pull_read_case(args, faults)
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
