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
               written until another event arrives);
  replica     a MySQL replica of pull's listener on a fourth host: it must
               end with the source's rows and no replication error (in
               position mode the source and replica run without GTIDs:
               ticket 30, PBS-102);
  replica_early  the replica connects while pull has stored nothing, not
               even a binlog file (the source starts late); in position
               mode it names binlog.000001 (ticket 38, PBS-111);
  heartbeat    the replica with a heartbeat period of 1 s must receive
               heartbeats during 20 quiet seconds after the closing FLUSH
               BINARY LOGS (ticket 40, PBS-113);
  stop         SIGTERM to pull while the source is idle: it must exit 0
               within 5 s (ticket 43, PBS-116);
  slow         one 20 MB transaction over a 2 Mbit link to the source;
  cron         'fetch' every 5 minutes for 3 simulated hours of inserts,
               the source restarted every hour: a fetch may fail only
               during a restart, and the storage must end equal to the
               source;
  replica_restart  the replica killed (SIGKILL) at 40 s and started again,
               resuming from its relay log position;
  replica_lossy  2 % packet loss and 20 ms latency between pull and the
               replica;
  replicas3    three replicas at once;
  pause_short, pause_long  pull reaches the source through a relay that
               goes silent (the connection stays open, nothing arrives, no
               RST: a network partition as the endpoints see it) for 10 s,
               less than the read timeout, or for 120 s, through read
               timeouts and reconnects;
  pause_stop   SIGTERM to pull 30 s into the 120 s pause: it must exit 0
               within 5 s;
  replica_pause  the same relay between pull's listener and the replica,
               silent for 40 s (the replica's net timeout is 10 s);
  pause_purge_safe  during the 120 s pause the source purges the binlogs
               before the one it wrote 2 s before the pause: pull must
               resume and end with everything;
  pause_purge_lost  the source purges up to its newest binlog, so what pull
               still needs is gone: pull must exit with an error and leave
               a valid prefix of the source's binlogs;
  s3_basic     basic with an S3 storage (versitygw, posix backend, on its
               own host);
  s3_lossy     2 % packet loss and 20 ms latency between pull and the S3
               server;
  s3_pause     a relay between pull and the S3 server silent for 60 s;
  s3_restart   the S3 server killed with SIGKILL at 40 s and started again
               20 s later;
  failover     a planned switchover at 45 s to db2, a GTID replica of the
               source: the source read-only, db2 caught up and promoted,
               the relay in front of pull pointed at db2, the source shut
               down; in GTID mode the storage must hold the source's
               transactions and then db2's, each once; in position mode
               (db2's binlogs are other files) a prefix of the source's
               binlogs, whatever pull does;
  failover_overlap  the same, with db2's binlogs rotated twice first so
               they carry the names of the source's: in position mode pull
               then asks db2 for an offset in another file;
  soak         6 simulated hours of inserts every 10 s (about 300
               rotations): pull's memory (RSS of its native process),
               threads and files open in the storage, sampled from outside,
               must not grow from the second hour to the last;
  soak_s3      the same on an S3 storage.

Kill sets ('<set>/<offset>us', --kill-points of them per set, picked by
the seed): pull killed with SIGKILL at a microsecond around an anchor line
of a reference run's log, and started again 1 s later; a snapshot keeps
what the kill left in the storage:
  pbs_kill                 the first binlog rotation (basic run);
  pbs_kill_checkpoint      the checkpoint at the 5th transaction;
  pbs_kill_reconnect       pull's reconnect after the source restart of
                           the restart case;
  enc_pbs_kill, enc_pbs_kill_checkpoint  the same on an encrypted storage,
                           checked through a replica; the bytes the kill
                           left past the recorded size must not be written
                           again differently under the same encryption
                           record (ticket 32, PBS-104: AES-CTR keystream
                           reuse);
  replica_pbs_kill         one kill at the rotation while a replica
                           streams from pull: the replica must catch up;
  s3_pbs_kill, enc_s3_pbs_kill  the first rotation on an S3 storage (the
                           s3_basic run), plain and encrypted.

GTID mode is rewrite mode, where PBS writes its own binlogs: there the
storage is compared with the source by its GTIDs instead of byte by byte.
"""

import argparse
import concurrent.futures
import json
import os
import random
import re
import shutil
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
    "replica": ("replica", {}),
    "replica_early": ("replica_early", {}),
    "heartbeat": ("heartbeat", {}),
    "stop": ("stop", {}),
    "slow": ("slow", {}),
    # 3 simulated hours: about 7 minutes alone, more than 15 next to 4
    # other simulations
    "cron": ("cron", {"WALL": "2400"}),
    "replica_restart": ("replica", {"REPLICA_KILL_AT": "40"}),
    "replica_lossy": ("replica", {"REPLICA_LOSS": "0.02",
                                  "REPLICA_LATENCY": "20 ms"}),
    "replicas3": ("replica", {"REPLICAS": "3"}),
    # a relay between pull and the source that goes silent (pause_relay.py)
    "pause_short": ("basic", {"PAUSE": "40-50"}),
    "pause_long": ("basic", {"PAUSE": "40-160"}),
    "pause_stop": ("stop", {"PAUSE": "40-160", "PBS_TERM_AT": "70"}),
    "replica_pause": ("replica", {"REPLICA_PAUSE": "60-100"}),
    # the source purges binlogs while pull cannot reach it
    "pause_purge_safe": ("basic", {"PAUSE": "40-160", "PURGE": "safe"}),
    "pause_purge_lost": ("basic", {"PAUSE": "40-160", "PURGE": "lost"}),
    # an S3 storage on its own host
    "s3_basic": ("basic", {"S3": "1"}),
    "s3_lossy": ("basic", {"S3": "1", "S3_LOSS": "0.02",
                           "S3_LATENCY": "20 ms"}),
    "s3_pause": ("basic", {"S3": "1", "S3_PAUSE": "40-100"}),
    "s3_restart": ("basic", {"S3": "1", "S3_KILL_AT": "40"}),
    # a planned switchover to a promoted GTID replica of the source
    "failover": ("basic", {"FAILOVER": "45"}),
    "failover_overlap": ("basic", {"FAILOVER": "45", "DB2_FLUSHES": "2"}),
    # 6 simulated hours: about 22 minutes alone
    "soak": ("soak", {"SOAK_HOURS": "6", "WALL": "4800"}),
    "soak_s3": ("soak", {"SOAK_HOURS": "6", "S3": "1", "WALL": "4800"}),
}

# kill sets: pull killed with SIGKILL at microseconds around an anchor, a
# line of a reference run's PBS log (its nth match), and started again 1 s
# later. A rotation's file operations (data write and fsync, metadata
# write, fsync and rename, the new binlog, its metadata and the index) span
# about 50 us of simulated time, a checkpoint about 10 us.
# set: (reference case, pattern, nth match, environment, kill points or None
# for --kill-points)
STAMP = r"^\[2000-01-01T(\d\d):(\d\d):(\d\d)\.(\d{6})\] "
ROTATION = re.compile(STAMP + r"\[ *info\] storage: closed binlog file")
# the 5th transaction (round 3, in the first binlog): with 2 s between
# rounds and a 1 s checkpoint_interval its first event makes PBS write the
# transaction before it
CHECKPOINT = re.compile(STAMP + r"\[ *debug\] event  : gtid_log")
# pull's reconnect after the source restart of the restart case
RECONNECT = re.compile(STAMP + r"\[ *info\] established connection to mysql")
KILL_SETS = {
    "pbs_kill": ("basic", ROTATION, 1, {}, None),
    "pbs_kill_checkpoint": ("basic", CHECKPOINT, 5, {}, None),
    "pbs_kill_reconnect": ("restart", RECONNECT, 2, {"KILL_BASE": "restart"},
                           None),
    # encrypted storage: the kill leaves bytes past the recorded size, the
    # restart writes that range again (ticket 32, PBS-104)
    "enc_pbs_kill": ("basic", ROTATION, 1, {"ENCRYPT": "1"}, None),
    "enc_pbs_kill_checkpoint": ("basic", CHECKPOINT, 5, {"ENCRYPT": "1"},
                                None),
    # a replica streaming from pull while pull is killed
    "replica_pbs_kill": ("basic", ROTATION, 1, {"WITH_REPLICA": "1"}, 1),
    # an S3 storage: the anchor from the s3_basic run
    "s3_pbs_kill": ("s3_basic", ROTATION, 1, {"S3": "1"}, None),
    "enc_s3_pbs_kill": ("s3_basic", ROTATION, 1, {"S3": "1", "ENCRYPT": "1"},
                        None),
}
KILL_OFFSETS = range(-20, 41)

VERDICT = re.compile(r"^-- verdict: (PASS|FAIL) \(shadow exit (-?\d+), "
                     r"storage mismatches (\d+), timestamp check (\d+)"
                     r"(?:, (.*))?\)$")
HASH = re.compile(r"^-- (outcome hash|storage hash|kill snapshot hash|"
                  r"PBS log hash[^:]*): "
                  r"([0-9a-f]+)")


def kill_anchor(args, reference: str, pattern, nth: int):
    """microseconds of simulated time of the nth line matching 'pattern' in
    the reference run's PBS log, or None"""
    try:
        lines = (args.workdir / reference / "binsrv.log").read_text(
            errors="replace").splitlines()
    except OSError:
        return None
    for line in lines:
        match = pattern.match(line)
        if match:
            nth -= 1
            if nth == 0:
                h, m, sec, us = (int(group) for group in match.groups())
                return ((h * 60 + m) * 60 + sec) * 1000000 + us
    return None


def run_scenario(args, name: str, scenario: str, extra: dict,
                 run_name: str = "") -> dict:
    env = dict(os.environ)
    env.update({
        "PBS": str(args.binsrv), "MYSQL_BASEDIR": str(args.mysql_basedir),
        "SHADOW": str(args.shadow), "EXP_DIR": str(args.workdir),
        "MODE": args.mode, "SEED": str(args.seed),
        "WALL": str(args.case_timeout)})
    env.update(extra)
    started = time.monotonic()
    run_name = run_name or name
    # a case may set a longer wall-clock limit of its own (WALL)
    proc = subprocess.run(["bash", str(SCRIPT), run_name, scenario], env=env,
                          capture_output=True, text=True,
                          timeout=int(env["WALL"]) + 120)
    output = proc.stdout + proc.stderr
    (args.workdir / f"{run_name}.out").write_text(output)
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
            key = match.group(1)
            hashes["kill_snapshot" if key.startswith("kill")
                   else key.split(" ")[0]] = match.group(2)
    result["hashes"] = hashes
    if not verdict:
        result.update(ok=False, error="no verdict line",
                      tail=output.strip().splitlines()[-5:])
        return result
    result["ok"] = verdict.group(1) == "PASS"
    result.update(shadow_exit=int(verdict.group(2)),
                  storage_mismatches=int(verdict.group(3)),
                  timestamp_check=int(verdict.group(4)))
    if verdict.group(5):
        result["checks"] = verdict.group(5)
    if "PBS_KILL_US" in extra:
        result["kill_us"] = int(extra["PBS_KILL_US"])
    if not result["ok"]:
        problems = [line.strip() for line in output.splitlines()
                    if re.search(r"differs:|missing in storage|^\s*BAD |"
                                 r"GTIDs DIFFER|expected end state|"
                                 r"Last_(IO|SQL)_Error: .|^-- stop:|^-- pull after", line)]
        # an assertion or other last words of PBS
        for stderr in (args.workdir / run_name / "shadow.data" / "hosts" /
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
    parser.add_argument("--cases", nargs="*",
                        choices=list(CASES) + list(KILL_SETS))
    parser.add_argument("--kill-points", default="4",
                        help="each kill set: how many kill points of the "
                        f"{len(KILL_OFFSETS)} (-20 to +40 us around its "
                        "anchor) to run, picked by the seed; offset 0 is "
                        "always run; 'all' runs every one")
    args = parser.parse_args()
    args.workdir = args.workdir.expanduser().resolve()
    args.workdir.mkdir(parents=True, exist_ok=True)
    args.binsrv = args.binsrv.expanduser().resolve()
    args.mysql_basedir = args.mysql_basedir.expanduser().resolve()
    if not args.shadow.is_file():
        print(json.dumps({"case": "setup", "ok": False,
                          "error": f"no shadow binary at {args.shadow}"}))
        return 1
    names = list(args.cases or (list(CASES) + list(KILL_SETS)))
    kill_sets = [name for name in names if name in KILL_SETS]
    # the reference runs of the kill sets, and determinism's, run first;
    # basic also makes the template data directory
    references = {KILL_SETS[name][0] for name in kill_sets}
    if "determinism" in names:
        references.add("basic")
    for reference in sorted(references, reverse=True):
        if reference not in names:
            names.insert(0, reference)
    first = ["basic"] if "basic" in names else []
    first += [name for name in names if name in references and
              name not in first]
    if not first:
        first = [name for name in names if name != "determinism" and
                 name not in KILL_SETS][:1]
    results = {}
    runs = {}  # case: (scenario, extra environment, run directory name)

    def one(name):
        scenario, extra, run_name = (runs[name] if name in runs else
                                     CASES[name] + (name,))
        try:
            return run_scenario(args, name, scenario, extra, run_name)
        except Exception as error:  # every case is reported
            return {"case": name, "ok": False,
                    "error": f"{type(error).__name__}: {error}"[:400]}

    for name in first:
        results[name] = one(name)
        print(json.dumps(results[name]), flush=True)
    rest = [name for name in names if name not in first and
            name not in KILL_SETS]
    for kill_set in kill_sets:
        reference, pattern, nth, env, points = KILL_SETS[kill_set]
        names.remove(kill_set)
        anchor = kill_anchor(args, reference, pattern, nth)
        if anchor is None:
            results[kill_set] = {"case": kill_set, "ok": False,
                                 "error": f"no anchor line in the {reference} "
                                 "run's PBS log to place the kill points at"}
            names.append(kill_set)
            continue
        offsets = list(KILL_OFFSETS)
        count = points or (None if args.kill_points == "all"
                           else int(args.kill_points))
        if count is not None:
            # a different pick for each set, the same for each seed
            rng = random.Random(f"{args.seed}/{kill_set}")
            if points:
                offsets = sorted(rng.sample(offsets, count))
            else:
                offsets = sorted([0] + rng.sample(
                    [offset for offset in offsets if offset != 0],
                    max(0, count - 1)))
        for offset in offsets:
            name = f"{kill_set}/{offset:+d}us"
            runs[name] = ("pbskill",
                          dict(env, PBS_KILL_US=str(anchor + offset)),
                          f"{kill_set}{offset:+d}")
            rest.append(name)
            names.append(name)
    # the long cases (a WALL of their own) first, so they do not stretch the
    # end of the stage
    rest.sort(key=lambda name: -int(CASES.get(name, ("", {}))[1].get(
        "WALL", 0)))
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
    # the template data directory is made again by the next run
    for template in args.workdir.glob("template-*"):
        shutil.rmtree(template, ignore_errors=True)
    return 0 if not failed else 1


if __name__ == "__main__":
    sys.exit(main())
