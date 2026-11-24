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

"""Deterministic network and timing scenarios under the Shadow simulator.

A MySQL source, 'pull' and a workload client run under Shadow
(https://shadow.github.io), each on its own simulated host, with simulated
time and a simulated network. A run depends only on its seed: the same seed
gives the same storage and the same PBS log byte for byte, so a failing seed
replays exactly. Shadow needs the patches in shadow/shadow-mysql.patch to run
mysqld (see shadow/README.md).

Each case runs shadow/shadow_pbs.sh, which compares PBS's storage with the
source's binlogs and checks the timestamp range recorded for every binlog
(shadow/check_binlog_timestamps.py). Cases (one JSON line each, then a
summary):
  basic        30 rounds of inserts with binlog rotations;
  determinism  basic again with the same seed: the outcome (binlogs and
               their sizes, transactions received, how each process ended,
               the verdict) must be the first run's. Not the bytes: native
               preemption, which mysqld needs, makes simulated times jitter
               by microseconds between runs (shadow/README.md);
  restart      the source stopped with SQL SHUTDOWN at 40 s and started
               again at 55 s;
  crash        the source killed with SIGKILL right after FLUSH BINARY LOGS,
               before it writes to the new binlog, then started again
               (ticket 6, PBS-72: a debug build aborts on an assertion in
               storage::open_binlog(), a release build records the dropped
               events' timestamps in the next binlog);
  lossy        basic with 2 % packet loss and 20 ms latency between the
               source and the other hosts;
  idle         a short workload, then an hour of simulated idle time
               (about 2 minutes of wall time): PBS reconnects after every
               read timeout;
  quiet        basic without the closing FLUSH BINARY LOGS the other cases
               end with, so the last transaction is followed by silence
               (ticket 94, PBS-167: with checkpoint_interval it is not
               written until another event arrives).

GTID mode is rewrite mode, where PBS writes its own binlogs: there the
storage is compared with the source by its GTIDs instead of byte by byte.
"""

import argparse
import concurrent.futures
import json
import os
import re
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
SCRIPT = HERE / "shadow" / "shadow_pbs.sh"

# case: (scenario of shadow_pbs.sh, extra environment)
CASES = {
    "basic": ("basic", {}),
    "determinism": ("basic", {}),
    "restart": ("restart", {}),
    "crash": ("crash", {}),
    "lossy": ("lossy", {}),
    "idle": ("idle", {}),
    "quiet": ("quiet", {}),
}

VERDICT = re.compile(r"^-- verdict: (PASS|FAIL) \(shadow exit (-?\d+), "
                     r"storage mismatches (\d+), timestamp check (\d+)\)")
HASH = re.compile(r"^-- (outcome hash|storage hash|PBS log hash[^:]*): "
                  r"([0-9a-f]+)")


def run_scenario(args, name: str, scenario: str, extra: dict) -> dict:
    env = dict(os.environ)
    env.update({
        "PBS": str(args.binsrv), "MYSQL_BASEDIR": str(args.mysql_basedir),
        "SHADOW": str(args.shadow), "EXP_DIR": str(args.workdir),
        "MODE": args.mode, "SEED": str(args.seed),
        "WALL": str(args.case_timeout)})
    env.update(extra)
    started = time.monotonic()
    proc = subprocess.run(["bash", str(SCRIPT), name, scenario], env=env,
                          capture_output=True, text=True,
                          timeout=args.case_timeout + 120)
    output = proc.stdout + proc.stderr
    (args.workdir / f"{name}.out").write_text(output)
    result = {"case": name, "seed": args.seed,
              "seconds": round(time.monotonic() - started)}
    hashes = {}
    verdict = None
    for line in output.splitlines():
        match = VERDICT.match(line)
        if match:
            verdict = match
        match = HASH.match(line)
        if match:
            hashes[match.group(1).split(" ")[0]] = match.group(2)
    result["hashes"] = hashes
    if not verdict:
        result.update(ok=False, error="no verdict line",
                      tail=output.strip().splitlines()[-5:])
        return result
    result["ok"] = verdict.group(1) == "PASS"
    result.update(shadow_exit=int(verdict.group(2)),
                  storage_mismatches=int(verdict.group(3)),
                  timestamp_check=int(verdict.group(4)))
    if not result["ok"]:
        problems = [line.strip() for line in output.splitlines()
                    if re.search(r"differs:|missing in storage|^\s*BAD |"
                                 r"GTIDs DIFFER|expected end state", line)]
        # an assertion or other last words of PBS
        for stderr in (args.workdir / name / "shadow.data" / "hosts" /
                       "pbs").glob("*.stderr"):
            problems += [line.strip() for line in
                         stderr.read_text(errors="replace").splitlines()
                         if line.strip()][-2:]
        result["problems"] = sorted(set(problems))[:8]
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--mysql-basedir", type=Path, required=True)
    parser.add_argument("--binsrv", type=Path, required=True)
    parser.add_argument("--workdir", type=Path, required=True)
    parser.add_argument("--port", type=int, default=0,
                        help="unused: the simulated hosts have their own "
                        "network")
    parser.add_argument("--mode", choices=["position", "gtid"],
                        default="position")
    parser.add_argument("--shadow", type=Path,
                        default=Path.home() / "ws/shadow/install/bin/shadow",
                        help="the shadow binary, built with "
                        "shadow/shadow-mysql.patch")
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument("--jobs", type=int, default=1,
                        help="simulations run at the same time")
    parser.add_argument("--case-timeout", type=int, default=900,
                        help="wall-clock seconds for one simulation")
    parser.add_argument("--cases", nargs="*", choices=list(CASES))
    args = parser.parse_args()
    args.workdir = args.workdir.expanduser().resolve()
    args.workdir.mkdir(parents=True, exist_ok=True)
    args.binsrv = args.binsrv.expanduser().resolve()
    args.mysql_basedir = args.mysql_basedir.expanduser().resolve()
    if not args.shadow.is_file():
        print(json.dumps({"case": "setup", "ok": False,
                          "error": f"no shadow binary at {args.shadow}"}))
        return 1
    names = args.cases or list(CASES)
    # the template data directory is made by the first run; make it before
    # running simulations side by side
    first = [name for name in names if name != "determinism"][:1]
    rest = [name for name in names if name not in first]
    results = {}

    def one(name):
        scenario, extra = CASES[name]
        try:
            return run_scenario(args, name, scenario, extra)
        except Exception as error:  # every case is reported
            return {"case": name, "ok": False,
                    "error": f"{type(error).__name__}: {error}"[:400]}

    for name in first:
        results[name] = one(name)
        print(json.dumps(results[name]), flush=True)
    with concurrent.futures.ThreadPoolExecutor(args.jobs) as pool:
        for result in pool.map(one, rest):
            results[result["case"]] = result
            if result["case"] != "determinism":
                print(json.dumps(result), flush=True)
    if "determinism" in results:
        result = results["determinism"]
        first_run = results.get("basic")
        if first_run is None:
            result.update(ok=False, error="needs the basic case")
        elif (result.get("hashes", {}).get("outcome")
              != first_run.get("hashes", {}).get("outcome")):
            # the outcome, not the bytes: native preemption makes simulated
            # times jitter by microseconds (shadow/README.md)
            result.update(ok=False, error="a second run with the same seed "
                          "has another outcome than the first",
                          first=first_run.get("hashes"))
        else:
            result["ok"] = True
            result.pop("problems", None)
        print(json.dumps(result), flush=True)
    ordered = [results[name] for name in names if name in results]
    failed = [result["case"] for result in ordered if not result.get("ok")]
    summary = {"cases": len(ordered), "failed": failed, "mode": args.mode,
               "seed": args.seed, "passed": not failed}
    print(json.dumps(summary), flush=True)
    (args.workdir / "result.json").write_text(json.dumps(ordered, indent=2))
    return 0 if not failed else 1


if __name__ == "__main__":
    sys.exit(main())
