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

"""Is a failing final flush of 'fetch' reported?

Without 'checkpoint_size' / 'checkpoint_interval', 'fetch' keeps what it
receives for the current binlog in memory and writes it when it exits. The
source has one binlog; a first 'fetch' stores it, more transactions follow
(no rotation, so nothing is flushed before the exit), and a second 'fetch'
runs with a persistent fault on what that final flush does:

- 'disk/<target>/<operation>/<errno>' (needs --fsfault-lib, the LD_PRELOAD
  library of extra/qa/fsfault): ENOSPC or EIO on the write or fsync of the
  binlog data, and on the open, write, fsync or rename of the binlog
  metadata;
- 's3/<object>/<action>' (with --s3-fault-proxy, a running
  s3_fault_proxy.py, and --s3-bucket-dir): every PUT of the binlog object or
  of its metadata answered with HTTP 500, or its connection reset.

A case passes when the failure is reported (a non-zero exit code) or the
data is stored after all; it fails when 'fetch' exits 0 while the new data
is not recorded in the storage (ticket 73: the flush runs in the storage
destructor, which drops the error, and the log ends with "successfully
fetched everything"). Each result also shows what the next 'fetch' without a
fault does.

A JSON line per case, then a summary; exit 1 if any case failed. Example:

  PYTHONPATH=~/ws/qa/pylib python3 final_flush_test.py \\
    --mysql-basedir ~/ws/dist/mysql-8.4.8-linux-glibc2.28-x86_64-minimal \\
    --binsrv ~/ws/percona-binlog-server-build-debug_gcc14/binlog_server \\
    --workdir ~/ws/qa/final-flush \\
    --fsfault-lib ~/ws/qa/fsfault/libfsfault.so
"""

import argparse
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import fetch_crash_test as harness  # noqa: E402

WORKLOAD = [{"rows": [200, 2000], "ddl": False}]
DISK_FAULTS = [(target, op, errno)
               for target, ops in (("binlog data", ("write", "fsync")),
                                   ("binlog metadata",
                                    ("open", "write", "fsync", "rename")))
               for op in ops for errno in ("ENOSPC", "EIO")]
S3_FAULTS = [(match, action) for match in ("binlog.0", ".json")
             for action in ("error500", "reset_before")]


def stored_state(binsrv) -> dict:
    name = (binsrv.indexed_binlogs() or ["?"])[-1]
    data = binsrv.storage / name
    metadata = binsrv.storage / (name + ".json")
    size = None
    if metadata.exists():
        try:
            size = json.loads(metadata.read_text()).get("size")
        except ValueError:
            size = "unreadable"
    return {"binlog": name,
            "data_bytes": data.stat().st_size if data.exists() else None,
            "recorded_size": size}


def log_end(binsrv, lines: int = 2) -> list:
    if not binsrv.log_path.exists():
        return []
    return [line.strip()[-170:] for line in
            binsrv.log_path.read_text(errors="replace").splitlines()
            if "[  error]" in line or "[warning]" in line or
            "exception caught" in line or "successfully" in line][-lines:]


def prepare(machine):
    machine.start_from_scratch()
    machine.run_workload(WORKLOAD)
    machine.fetch_to_the_end()
    machine.run_workload(WORKLOAD)


def judge(result: dict, code: int, before: dict, after: dict):
    source_size = len(harness.SERVER.binlog_as_sent(before["binlog"]))
    recorded = after["recorded_size"] == source_size and \
        after["data_bytes"] == source_size
    result.update({"fetch_exit": code, "before": before, "after": after,
                   "source_bytes": source_size, "log": log_end(harness.BINSRV),
                   "reported": code != 0, "recorded": recorded,
                   "ok": code != 0 or recorded})
    if not result["ok"]:
        result["why"] = (
            f"fetch exited 0 (\"{(result['log'] or ['?'])[-1][-60:]}\") while "
            f"{source_size - (after['recorded_size'] or 0)} of the "
            f"{source_size - before['recorded_size']} new bytes are not "
            "recorded in the storage")


def clean_fetch_after(result: dict):
    binsrv = harness.BINSRV
    process = binsrv.start_fetch()
    try:
        process.communicate(timeout=harness.FETCH_TIMEOUT_SECONDS)
    except subprocess.TimeoutExpired:
        process.kill()
        process.communicate()
    state = stored_state(binsrv)
    result["next_fetch_without_a_fault"] = {
        "exit": process.returncode,
        "exact_copy": (binsrv.storage / state["binlog"]).read_bytes() ==
        harness.SERVER.binlog_as_sent(state["binlog"])}


def disk_case(machine, target: str, op: str, errno: str, log: Path) -> dict:
    result = {"case": f"disk/{target.replace(' ', '_')}/{op}/{errno}"}
    prepare(machine)
    before = stored_state(harness.BINSRV)
    code, injected = harness.fetch_with_disk_fault_once(
        {"op": op, "target": target, "nth": 1, "count": 1000,
         "errno": errno}, log)
    result["failures_injected"] = injected
    judge(result, code, before, stored_state(harness.BINSRV))
    if injected == 0:
        result["ok"] = False
        result["why"] = "the fault was not reached: the case did not run"
    clean_fetch_after(result)
    return result


def s3_case(machine, proxy, match: str, action: str) -> dict:
    result = {"case": f"s3/{'binlog' if match == 'binlog.0' else 'metadata'}"
                      f"/{action}"}
    binsrv = harness.BINSRV
    proxy.set_rules([])
    prepare(machine)
    before = stored_state(binsrv)
    proxy.set_rules([{"action": action, "method": "PUT", "match": match,
                      "nth": 1, "count": 1000, "seconds": 1}])
    process = binsrv.start_fetch()
    try:
        process.communicate(timeout=300)
    except subprocess.TimeoutExpired:
        process.kill()
        process.communicate()
    finally:
        proxy.set_rules([])
    judge(result, process.returncode, before, stored_state(binsrv))
    clean_fetch_after(result)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--mysql-basedir", type=Path, required=True)
    parser.add_argument("--binsrv", type=Path, required=True)
    parser.add_argument("--workdir", type=Path, required=True)
    parser.add_argument("--port", type=int, default=23740)
    parser.add_argument("--mode", choices=["gtid", "position"],
                        default="position")
    parser.add_argument("--fsfault-lib", type=Path,
                        help="libfsfault.so; without it the disk cases are "
                             "skipped")
    parser.add_argument("--s3-fault-proxy",
                        help="host:port of a running s3_fault_proxy.py; "
                             "runs the S3 cases instead of the disk ones")
    parser.add_argument("--s3-bucket", default="pbs-mtr")
    parser.add_argument("--s3-bucket-dir", type=Path)
    parser.add_argument("--s3-access-key", default="pbsaccesskey")
    parser.add_argument("--s3-secret-key", default="pbs/secret+key=1")
    args = parser.parse_args()
    args.workdir.mkdir(parents=True, exist_ok=True)
    gtid_mode = args.mode == "gtid"
    s3 = None
    if args.s3_fault_proxy:
        if args.s3_bucket_dir is None:
            parser.error("--s3-fault-proxy needs --s3-bucket-dir")
        s3 = harness.S3Storage(args.s3_fault_proxy, args.s3_bucket,
                               args.s3_bucket_dir.expanduser(),
                               args.s3_access_key, args.s3_secret_key,
                               f"final-flush-{args.mode}-{os.getpid()}")
    harness.SERVER = harness.MySQLServer(args.mysql_basedir.expanduser(),
                                         args.workdir / "mysql", args.port,
                                         gtid_mode, server_id=1)
    harness.BINSRV = harness.BinlogServer(args.binsrv.expanduser(),
                                          args.workdir / "binsrv", args.port,
                                          gtid_mode, s3)
    results = []
    harness.SERVER.start()
    try:
        machine = harness.FetchCrashMachine()
        if s3 is not None:
            proxy = harness.FaultProxy(
                f"http://{args.s3_fault_proxy}/__faults__")
            cases = [(f"s3/{match}/{action}",
                      lambda m=match, a=action: s3_case(machine, proxy, m, a))
                     for match, action in S3_FAULTS]
        elif args.fsfault_lib is not None:
            harness.FSFAULT_LIB = args.fsfault_lib.expanduser().resolve()
            log = args.workdir / "fsfault.log"
            cases = [(f"disk/{target}/{op}/{errno}",
                      lambda t=target, o=op, e=errno:
                      disk_case(machine, t, o, e, log))
                     for target, op, errno in DISK_FAULTS]
        else:
            cases = []
            result = {"case": "disk", "ok": True,
                      "skipped": "no --fsfault-lib"}
            print(json.dumps(result), flush=True)
            results.append(result)
        for name, case in cases:
            try:
                result = case()
            except (harness.Failure, RuntimeError,
                    subprocess.SubprocessError, OSError) as failure:
                result = {"case": name.replace(" ", "_"), "ok": False,
                          "why": harness.describe_failure(failure, 500)}
            print(json.dumps(result), flush=True)
            results.append(result)
    finally:
        harness.SERVER.stop()
        if s3 is not None:
            shutil.rmtree(harness.BINSRV.storage, ignore_errors=True)
    failures = [result for result in results if not result.get("ok")]
    print(json.dumps({"cases": len(results), "failures": len(failures),
                      "passed": not failures}), flush=True)
    (args.workdir / "result.json").write_text(json.dumps(results, indent=2))
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
