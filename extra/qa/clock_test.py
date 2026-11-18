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

"""What Percona Binlog Server does when its clock jumps, runs ahead of or
behind the source's, sits far in the future, or the time zone changes.

Only the binlog_server process sees a fake clock: libfaketime
(https://github.com/wolfcw/libfaketime, '--faketime-lib', the
'libfaketime.so.1' it builds) is LD_PRELOADed into it, never into mysqld,
so the source and the replicas keep real time. The offset is read from a
control file (FAKETIME_TIMESTAMP_FILE, re-read at most once a second), so
the harness can make the clock jump while PBS runs. A 'wall' jump moves
CLOCK_REALTIME only (FAKETIME_DONT_FAKE_MONOTONIC=1), as settimeofday() or
an NTP step does; a 'mono' jump moves CLOCK_MONOTONIC (std::chrono::
steady_clock) as well, which a real system never does backwards and does
forwards only as far as the kernel's monotonic clock can (it is the case
a time namespace or a VM pause would come closest to). Use a build without
a sanitizer (the debug build): libfaketime is not ordered before the ASan
runtime here.

Cases, named '<mode>/<group>/<variant>'; a jump variant is one of
wall_fwd_1d, wall_back_2h, wall_back_10m, mono_fwd_1d, mono_back_2h:

  heartbeat/<jump>     (position mode only) a MySQL replica of 'pull' with
                       SOURCE_HEARTBEAT_PERIOD=1 and replica_net_timeout=4
                       sits idle for 24 s; PBS's clock jumps 6 s in. The
                       replica must get about one heartbeat a second
                       before and after the jump, no gap over 2.5 s, no
                       burst, and must not reconnect;
  reconnect/<jump>     'pull' with connection.read_timeout=3 and
                       replication.idle_time=5 against an idle source:
                       it times out, sleeps idle_time and reconnects, about
                       every 8 s. The clock jumps during a sleep. Every
                       sleep must last idle_time and every cycle about
                       read_timeout + idle_time (real time, measured by the
                       harness as the log lines appear);
  checkpoint/idle_tail 'pull' with only storage.checkpoint_interval=10s: one
                       transaction after 12 quiet seconds, then 15 more
                       quiet seconds: it must be stored by then. A
                       time-based checkpoint is checked only when an event
                       arrives and covers the transactions completed before
                       that event, so the transaction that ends a quiet
                       spell stays unflushed while the source stays quiet;
  checkpoint/<jump>    the same 'pull'; a transaction at T0 triggers a
                       checkpoint, then one comes every 0.5 s and the clock
                       jumps at T0+1. The next checkpoint (the stored
                       transaction count growing) must come at T0+10 (9 to
                       12 s), none between T0+1.2 and T0+9;
  telemetry/<jump>     'pull' with telemetry grace 20s, scrape 10s,
                       history 60s for 56 s; the clock jumps at 25 s. Reports
                       must appear about 20, 30, 40, 50 s after the start
                       (real time), their 'uptime' must follow real time,
                       and no report may be removed less than
                       history_keep_interval after it was written;
  skew/{ahead_1d,behind_1d,none}
                       the source keeps real time, PBS runs a day ahead,
                       a day behind, or in step, for a whole 'fetch'. The
                       stored binlogs' min_timestamp / max_timestamp
                       ('list') must be those of the source's events (in
                       GTID mode, rewrite mode, of the transactions) and
                       search_by_timestamp must answer like the truth
                       (binlogs whose first event is at or before the
                       time; "too old" before the first one);
  tz/zones             one fetch, then 'list' and search_by_timestamp under
                       TZ=UTC, America/New_York, Europe/Zagreb,
                       Asia/Kolkata, Pacific/Chatham, Australia/Lord_Howe:
                       every answer must be the same as under UTC, also for
                       a probe at a local time that occurs twice on the DST
                       end in New York (2026-11-01T01:30:00);
  tz/fetch_in_zone     a second storage fetched under TZ=America/New_York
                       must have the same metadata (position mode: the same
                       files; GTID mode: the same min_timestamp) as the one
                       fetched under UTC;
  tz/dst_log           'fetch' under TZ=America/New_York with its clock set
                       3 s before the DST end (2026-11-01T05:59:57Z): its
                       log timestamps must be UTC (05:59:5x / 06:00:0x), not
                       New York time;
  future/{y2100,y2106,y2300}
                       PBS starts with its clock at 2100-01-01, at
                       2106-02-07T06:28:10Z (6 s before the 32-bit event
                       timestamp wraps) or at 2300-01-01 (past the 2262
                       limit of std::chrono::system_clock in int64
                       nanoseconds) and fetches from a real-time source:
                       fetch must exit 0 and store every transaction, and
                       list / search_by_timestamp must still give the
                       source's times; then 'pull' runs 24 s with the same
                       clock: its telemetry report must be named
                       '<unix seconds>-<tag>.json' with the fake time, the
                       listener's answer to SELECT UNIX_TIMESTAMP() must be
                       the fake time, and the log timestamps must carry the
                       fake year.

Every case prints one JSON line with its numbers and "ok"; a summary line
ends the run; the exit code is 1 when a case fails. Example (test-infra):

  PYTHONPATH=~/ws/qa/pylib python3 extra/qa/clock_test.py \\
    --mysql-basedir ~/ws/dist/mysql-8.4.8-linux-glibc2.28-x86_64-minimal \\
    --binsrv ~/ws/build-merge-debug/binlog_server \\
    --faketime-lib ~/ws/clock/libfaketime/src/libfaketime.so.1 \\
    --workdir ~/ws/qa/clock --mode position

'--cases' takes comma-separated shell patterns over the case names without
the mode (default: all), e.g. '--cases "heartbeat/*,skew/*"'.
"""

import argparse
import calendar
import datetime
import fnmatch
import json
import os
import re
import signal
import struct
import subprocess
import sys
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import fetch_crash_test as harness  # noqa: E402
import pull_replica_test as replication  # noqa: E402

JUMPS = {
    "wall_fwd_1d": ("+1d", False),
    "wall_back_2h": ("-2h", False),
    "wall_back_10m": ("-10m", False),
    "mono_fwd_1d": ("+1d", True),
    "mono_back_2h": ("-2h", True),
}
SKEWS = {"ahead_1d": "+1d", "behind_1d": "-1d", "none": "+0"}
FUTURES = {"y2100": "@2100-01-01 00:00:00",
           "y2106": "@2106-02-07 06:28:10",
           "y2300": "@2300-01-01 00:00:00"}
ZONES = ["UTC", "America/New_York", "Europe/Zagreb", "Asia/Kolkata",
         "Pacific/Chatham", "Australia/Lord_Howe"]
DST_PROBE = "2026-11-01T01:30:00"

HEARTBEAT_PERIOD = 1
NET_TIMEOUT = 4
HEARTBEAT_QUIET = 24
HEARTBEAT_JUMP_AT = 6
READ_TIMEOUT = 3
IDLE_TIME = 5
RECONNECT_SECONDS = 40
CHECKPOINT_INTERVAL = 10
TELEMETRY_SECONDS = 56
TELEMETRY_JUMP_AT = 25
TELEMETRY = {"grace_interval": "20s", "scrape_interval": "10s",
             "history_keep_interval": "60s"}
COMMAND_TIMEOUT = 120

ARGS = None
LOG_TIMESTAMP = re.compile(r"^\[(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d+)?)\]")


# ---------------------------------------------------------------------------
# the fake clock
# ---------------------------------------------------------------------------

class FakeClock:
    """A libfaketime control file and the environment that makes a
    binlog_server process read it."""

    def __init__(self, path: Path, monotonic: bool, start: str = "+0"):
        self.path = path
        self.monotonic = monotonic
        self.set(start)

    def set(self, spec: str) -> None:
        temporary = self.path.with_suffix(".tmp")
        temporary.write_text(spec + "\n")
        temporary.rename(self.path)

    def environment(self) -> dict:
        environment = {"LD_PRELOAD": str(ARGS.faketime_lib),
                       "FAKETIME_TIMESTAMP_FILE": str(self.path),
                       "FAKETIME_CACHE_DURATION": "1"}
        if not self.monotonic:
            environment["FAKETIME_DONT_FAKE_MONOTONIC"] = "1"
        return environment


class LogWatcher:
    """Follows a binlog_server log file and stamps every line with the
    harness's real monotonic time when it appears."""

    def __init__(self, path: Path):
        self.path = path
        self.lines = []
        self.stop_event = threading.Event()
        self.thread = threading.Thread(target=self.run, daemon=True)
        self.thread.start()

    def run(self):
        position = 0
        pending = ""
        while not self.stop_event.is_set():
            if self.path.exists():
                with self.path.open("r", errors="replace") as log:
                    log.seek(position)
                    chunk = log.read()
                    position = log.tell()
                now = time.monotonic()
                pending += chunk
                *complete, pending = pending.split("\n")
                self.lines.extend((now, line) for line in complete)
            time.sleep(0.05)

    def stop(self):
        self.stop_event.set()
        self.thread.join()

    def times(self, text: str) -> list:
        return [stamp for stamp, line in list(self.lines) if text in line]


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def iso(timestamp: int) -> str:
    return datetime.datetime.fromtimestamp(
        timestamp, datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")


def from_iso(text: str) -> int:
    return calendar.timegm(time.strptime(text[:19], "%Y-%m-%dT%H:%M:%S"))


def setup(name: str, replica: bool = False):
    """A fresh source (and replica) and binlog server for one case."""
    work = ARGS.workdir / ARGS.mode / name.replace("/", "_")
    gtid = ARGS.mode == "gtid"
    source = harness.MySQLServer(ARGS.mysql_basedir, work / "source",
                                 ARGS.port, gtid, server_id=1)
    other = (harness.MySQLServer(ARGS.mysql_basedir, work / "replica",
                                 ARGS.port + 1, False, server_id=2)
             if replica else None)
    binsrv = harness.BinlogServer(ARGS.binsrv, work / "binsrv", ARGS.port,
                                  gtid)
    harness.SERVER = source
    harness.BINSRV = binsrv
    source.start()
    if other is not None:
        other.start()
    machine = harness.FetchCrashMachine()
    machine.start_from_scratch()
    return work, source, other, binsrv, machine


def pull_config(binsrv, pull_port: int, telemetry: "dict | None" = None
                ) -> None:
    binsrv.write_config()
    config = json.loads(binsrv.config_path.read_text())
    config["logger"]["level"] = "debug"
    config["replication_source"] = {
        "port": pull_port, "server_id": 142,
        "server_uuid": "00000000-0000-0000-0000-00000000008e",
        "read_timeout": 60, "write_timeout": 60,
        "require_secure_transport": False,
        "auth_rsa_private_key": str(replication.AUTH_RSA_PRIVATE_KEY),
        "authentication": {"user": replication.PULL_USER,
                           "password": replication.PULL_PASSWORD,
                           "plugin": "caching_sha2_password"}}
    if telemetry is not None:
        config["telemetry"] = dict(telemetry, path=str(binsrv.telemetry))
    binsrv.config_path.write_text(json.dumps(config, indent=2))


def start_pull(binsrv, pull_port: int, clock: "FakeClock | None",
               telemetry: "dict | None" = None) -> subprocess.Popen:
    pull_config(binsrv, pull_port, telemetry)
    environment = dict(os.environ, AWS_EC2_METADATA_DISABLED="true")
    if clock is not None:
        environment.update(clock.environment())
    return subprocess.Popen(
        [str(binsrv.binary), "pull", str(binsrv.config_path)],
        stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, env=environment)


def stop(process: "subprocess.Popen | None") -> "int | None":
    if process is None:
        return None
    if process.poll() is None:
        process.send_signal(signal.SIGTERM)
        try:
            process.wait(timeout=30)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()
    return process.returncode


def run_binsrv(binsrv, command: str, *arguments: str,
               environment: "dict | None" = None) -> dict:
    """Runs a one-shot command; returns exit code, parsed answer, names."""
    full_environment = dict(os.environ, AWS_EC2_METADATA_DISABLED="true",
                            **(environment or {}))
    try:
        completed = subprocess.run(
            [str(binsrv.binary), command, str(binsrv.config_path),
             *arguments], capture_output=True, timeout=COMMAND_TIMEOUT,
            env=full_environment, check=False)
    except subprocess.TimeoutExpired:
        return {"exit": "timeout", "answer": None, "names": None}
    output = completed.stdout.decode(errors="replace").strip()
    try:
        answer = json.loads(output)
    except ValueError:
        answer = None
    record = {"exit": completed.returncode, "answer": answer, "names": None}
    if isinstance(answer, dict) and isinstance(answer.get("result"), list):
        record["names"] = [entry.get("name") for entry in answer["result"]]
    if answer is None:
        record["stderr"] = completed.stderr.decode(errors="replace")[-400:]
    return record


def stored_files(binsrv) -> list:
    """(name, data cut at the size in its metadata) of each indexed binlog."""
    result = []
    for name in binsrv.indexed_binlogs():
        data = (binsrv.storage / name).read_bytes()
        metadata = binsrv.storage / f"{name}.json"
        if metadata.exists():
            size = json.loads(metadata.read_text()).get("size")
            if isinstance(size, int):
                data = data[:size]
        result.append((name, data))
    return result


def stored_transaction_count(binsrv) -> int:
    return sum(len(harness.transactions_of(data))
               for _, data in stored_files(binsrv))


def source_transaction_count(source) -> int:
    return sum(len(harness.transactions_of(source.binlog_as_sent(name)))
               for name in source.binlog_names())


def truth_ranges(binsrv) -> dict:
    """The true time range of each stored binlog: every nonzero event
    timestamp in position mode (the files are the source's), the events of
    its transactions in GTID mode (rewrite mode writes its own preamble and
    ROTATE events)."""
    result = {}
    for name, data in stored_files(binsrv):
        stamps = []
        in_transaction = False
        for offset, type_code, _ in harness.parse_events(data):
            stamp = struct.unpack_from("<I", data, offset)[0]
            if type_code in harness.TRANSACTION_START_EVENTS:
                in_transaction = True
            elif type_code in (harness.ROTATE_EVENT, harness.STOP_EVENT,
                               harness.FORMAT_DESCRIPTION_EVENT,
                               harness.PREVIOUS_GTIDS_LOG_EVENT):
                in_transaction = False
            if stamp and (in_transaction or ARGS.mode != "gtid"):
                stamps.append(stamp)
        if stamps:
            result[name] = (min(stamps), max(stamps))
    return result


def search_truth(ranges: dict, names: list, stamp: int):
    result = [name for name in names
              if name in ranges and ranges[name][0] <= stamp]
    return result or None


def check_metadata_and_search(binsrv, environment=None) -> dict:
    """Compares 'list' and search_by_timestamp with the truth."""
    names = binsrv.indexed_binlogs()
    ranges = truth_ranges(binsrv)
    listed = run_binsrv(binsrv, "list", environment=environment)
    wrong = []
    listed_ranges = {}
    if listed["exit"] != 0 or not listed["names"]:
        wrong.append(f"list failed: {listed}")
    else:
        for entry in listed["answer"]["result"]:
            name = entry["name"]
            stored = (entry.get("min_timestamp"), entry.get("max_timestamp"))
            listed_ranges[name] = stored
            if name not in ranges:
                continue
            truth = (iso(ranges[name][0]), iso(ranges[name][1]))
            if stored[0] != truth[0]:
                wrong.append(f"{name} min_timestamp {stored[0]}, events "
                             f"start {truth[0]}")
            if stored[1] != truth[1]:
                wrong.append(f"{name} max_timestamp {stored[1]}, events "
                             f"end {truth[1]}")
    probes = []
    if ranges:
        first = min(low for low, _ in ranges.values())
        last = max(high for _, high in ranges.values())
        probes += [("before_first_-1d", first - 86400),
                   ("before_first_-1h", first - 3600),
                   ("before_first_-1s", first - 1),
                   ("after_last_+1h", last + 3600),
                   ("after_last_+2d", last + 2 * 86400)]
        for name, (low, _) in ranges.items():
            probes += [(f"{name}_min", low), (f"{name}_min-1", low - 1)]
    search_wrong = []
    for label, stamp in probes:
        truth = search_truth(ranges, names, stamp)
        run = run_binsrv(binsrv, "search_by_timestamp", iso(stamp),
                         environment=environment)
        answer = run["names"] if run["exit"] == 0 else None
        if answer != truth:
            search_wrong.append(f"{label} {iso(stamp)}: answered "
                                f"{answer if run['exit'] == 0 else 'error'}"
                                f", truth {truth}")
    return {"truth": {name: [iso(low), iso(high)]
                      for name, (low, high) in ranges.items()},
            "listed": listed_ranges, "metadata_wrong": wrong,
            "search_wrong": search_wrong, "probes": len(probes)}


def fetch(binsrv, environment: "dict | None" = None) -> dict:
    binsrv.write_config()
    process = binsrv.start_fetch(extra_environment=environment)
    try:
        _, stderr = process.communicate(timeout=COMMAND_TIMEOUT)
    except subprocess.TimeoutExpired:
        process.kill()
        process.communicate()
        return {"exit": "timeout"}
    return {"exit": process.returncode,
            "stderr": stderr.decode(errors="replace")[-300:]}


def spaced_workload(source, machine, binlogs: int = 3,
                    pause: float = 2.0) -> None:
    """Transactions in several source binlogs, a pause between binlogs so
    the binlogs' time ranges differ."""
    for index in range(binlogs):
        machine.run_workload([{"rows": [3000], "ddl": False},
                              {"rows": [3000, 3000], "ddl": False}])
        time.sleep(pause)
        if index != binlogs - 1:
            machine.flush_binary_logs()


def wait_for(predicate, timeout: float, step: float = 0.2) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(step)
    return predicate()


# ---------------------------------------------------------------------------
# cases
# ---------------------------------------------------------------------------

def case_heartbeat(name: str, variant: str) -> dict:
    spec, monotonic = JUMPS[variant]
    work, source, replica, binsrv, machine = setup(name, replica=True)
    clock = FakeClock(work / "faketime", monotonic)
    pull_port = ARGS.port + 2
    result = {"jump": spec, "monotonic": monotonic}
    pull = None
    try:
        binsrv.checkpoint_size = "1"
        machine.run_workload([{"rows": [200], "ddl": False}])
        first_binlog = source.binlog_names()[0]
        pull = start_pull(binsrv, pull_port, clock)
        time.sleep(3)
        replica.sql(f"SET GLOBAL replica_net_timeout = {NET_TIMEOUT}")
        replication.replicate_from_pull(
            replica, pull_port, False,
            f"SOURCE_LOG_FILE='{first_binlog}', SOURCE_LOG_POS=4, "
            f"SOURCE_HEARTBEAT_PERIOD={HEARTBEAT_PERIOD}")
        expected = replication.checksum(source)
        result["caught_up"] = wait_for(
            lambda: replication.checksum(replica) == expected, 60, 1)
        time.sleep(1)

        def sample():
            heartbeats = replica.sql(
                "SELECT COUNT_RECEIVED_HEARTBEATS FROM "
                "performance_schema.replication_connection_status").strip()
            connections = replica.error_log.read_text(
                errors="replace").count("Replica receiver thread for channel")
            return int(heartbeats or 0), connections

        samples = []
        start = time.monotonic()
        jumped = False
        while time.monotonic() - start < HEARTBEAT_QUIET:
            elapsed = time.monotonic() - start
            if not jumped and elapsed >= HEARTBEAT_JUMP_AT:
                clock.set(spec)
                jumped = True
                result["jump_at"] = round(elapsed, 2)
            samples.append((round(elapsed, 2), *sample()))
            time.sleep(0.25)
        connections = samples[-1][2] - samples[0][2]
        before = [s for s in samples if s[0] < HEARTBEAT_JUMP_AT]
        after = [s for s in samples if s[0] >= HEARTBEAT_JUMP_AT + 1.5]
        # time between heartbeat counter increments (same connection only)
        gaps, bursts = [], 0
        last_change = samples[0][0]
        for previous, current in zip(samples, samples[1:]):
            if current[2] != previous[2]:
                last_change = current[0]
                continue
            if current[1] > previous[1]:
                gaps.append(round(current[0] - last_change, 2))
                last_change = current[0]
                bursts = max(bursts, current[1] - previous[1])
        gaps.append(round(samples[-1][0] - last_change, 2))
        result.update({
            "heartbeats_before_jump": before[-1][1] - before[0][1],
            "heartbeats_after_jump": (after[-1][1] - after[0][1]
                                      if after and after[-1][2] == after[0][2]
                                      else None),
            "max_gap_seconds": max(gaps),
            "max_heartbeats_per_sample": bursts,
            "replica_reconnects": connections,
            "pull_alive": pull.poll() is None})
        after_seconds = (after[-1][0] - after[0][0]) if after else 0
        result["ok"] = bool(
            result["caught_up"] and connections == 0 and
            result["heartbeats_after_jump"] is not None and
            result["heartbeats_after_jump"] >= after_seconds /
            HEARTBEAT_PERIOD / 2 and result["max_gap_seconds"] <= 2.5 and
            bursts <= 2 and result["pull_alive"])
        replica.sql("STOP REPLICA")
    finally:
        stop(pull)
        replica.stop()
        source.stop()
    return result


def case_reconnect(name: str, variant: str) -> dict:
    spec, monotonic = JUMPS[variant]
    work, source, _, binsrv, machine = setup(name)
    clock = FakeClock(work / "faketime", monotonic)
    binsrv.read_timeout = READ_TIMEOUT
    binsrv.idle_time = IDLE_TIME
    result = {"jump": spec, "monotonic": monotonic}
    pull = None
    watcher = None
    try:
        machine.run_workload([{"rows": [200], "ddl": False}])
        pull = start_pull(binsrv, ARGS.port + 2, clock)
        watcher = LogWatcher(binsrv.log_path)
        start = time.monotonic()
        # jump 2 s into the second idle sleep
        jumped_at = None
        while time.monotonic() - start < RECONNECT_SECONDS:
            sleeps = watcher.times("entering idle mode")
            if jumped_at is None and len(sleeps) >= 2 and \
                    time.monotonic() - sleeps[1] >= 2:
                clock.set(spec)
                jumped_at = time.monotonic()
            time.sleep(0.1)
        sleeps = watcher.times("entering idle mode")
        wakes = watcher.times("awoke after sleeping")
        durations = []
        for begin in sleeps:
            later = [wake for wake in wakes if wake > begin]
            if later:
                durations.append(round(later[0] - begin, 2))
        cycles = [round(b - a, 2) for a, b in zip(sleeps, sleeps[1:])]
        result.update({
            "jump_at": round(jumped_at - start, 2) if jumped_at else None,
            "idle_sleeps_seconds": durations,
            "cycle_seconds": cycles,
            "expected_sleep": IDLE_TIME,
            "expected_cycle": READ_TIMEOUT + IDLE_TIME,
            "pull_alive": pull.poll() is None})
        result["ok"] = bool(
            jumped_at is not None and len(durations) >= 3 and
            all(abs(d - IDLE_TIME) <= 1.5 for d in durations) and
            all(abs(c - READ_TIMEOUT - IDLE_TIME) <= 3 for c in cycles) and
            result["pull_alive"])
    finally:
        if watcher is not None:
            watcher.stop()
        stop(pull)
        source.stop()
    return result


def case_checkpoint(name: str, variant: str) -> dict:
    """A time-based checkpoint is taken when an event arrives, and it
    covers the transactions completed before that event (see idle_tail),
    so the case keeps transactions coming every 0.5 s and records when the
    stored transaction count grows."""
    work, source, _, binsrv, machine = setup(name)
    monotonic = JUMPS[variant][1] if variant in JUMPS else False
    clock = FakeClock(work / "faketime", monotonic)
    binsrv.checkpoint_interval = f"{CHECKPOINT_INTERVAL}s"
    result = {}
    pull = None
    one = [{"rows": [500], "ddl": False}]
    try:
        machine.run_workload(one)
        pull = start_pull(binsrv, ARGS.port + 2, clock)
        time.sleep(CHECKPOINT_INTERVAL + 2)
        if variant == "idle_tail":
            # the first transaction after a quiet interval triggers a
            # checkpoint of what came before it, but not of itself
            machine.run_workload(one)
            time.sleep(CHECKPOINT_INTERVAL + 5)
            result["stored_after_idle"] = stored_transaction_count(binsrv)
            result["source_after_idle"] = source_transaction_count(source)
            result["seconds_idle"] = CHECKPOINT_INTERVAL + 5
            result["pull_alive"] = pull.poll() is None
            result["ok"] = bool(result["stored_after_idle"] ==
                                result["source_after_idle"] and
                                result["pull_alive"])
            return result
        spec = JUMPS[variant][0]
        result.update({"jump": spec, "monotonic": monotonic})
        machine.run_workload(one)            # checkpoint at T0
        t0 = time.monotonic()
        previous = stored_transaction_count(binsrv)
        growth = []
        jumped = False
        next_transaction = t0 + 0.5
        while time.monotonic() - t0 < CHECKPOINT_INTERVAL + 4:
            now = time.monotonic()
            if not jumped and now - t0 >= 1:
                clock.set(spec)
                jumped = True
            if now >= next_transaction:
                machine.run_workload(one)
                next_transaction = now + 0.5
            count = stored_transaction_count(binsrv)
            if count != previous:
                growth.append(round(now - t0, 2))
                previous = count
            time.sleep(0.1)
        result["checkpoints_at"] = growth
        result["expected_at"] = CHECKPOINT_INTERVAL
        early = [g for g in growth if 1.2 < g < CHECKPOINT_INTERVAL - 1]
        on_time = [g for g in growth if CHECKPOINT_INTERVAL - 1 <= g <=
                   CHECKPOINT_INTERVAL + 2]
        result["early"] = early
        result["pull_alive"] = pull.poll() is None
        result["ok"] = bool(not early and on_time and result["pull_alive"])
    finally:
        stop(pull)
        source.stop()
    return result


def case_telemetry(name: str, variant: str) -> dict:
    spec, monotonic = JUMPS[variant]
    work, source, _, binsrv, machine = setup(name)
    clock = FakeClock(work / "faketime", monotonic)
    result = {"jump": spec, "monotonic": monotonic}
    pull = None
    try:
        machine.run_workload([{"rows": [200], "ddl": False}])
        pull = start_pull(binsrv, ARGS.port + 2, clock, TELEMETRY)
        start = time.monotonic()
        seen = {}       # name -> (first seen, last seen)
        jumped = False
        while time.monotonic() - start < TELEMETRY_SECONDS:
            now = time.monotonic() - start
            if not jumped and now >= TELEMETRY_JUMP_AT:
                clock.set(spec)
                jumped = True
            present = set()
            if binsrv.telemetry.exists():
                present = {path.name for path in binsrv.telemetry.iterdir()
                           if path.suffix == ".json"}
            for report in present:
                if report not in seen:
                    uptime = None
                    try:
                        uptime = int(json.loads(
                            (binsrv.telemetry / report).read_text())
                                     ["uptime"])
                    except (OSError, ValueError, KeyError):
                        pass
                    seen[report] = [round(now, 1), round(now, 1), uptime]
                else:
                    seen[report][1] = round(now, 1)
            time.sleep(0.2)
        end = round(time.monotonic() - start, 1)
        reports = sorted(seen.items(), key=lambda item: item[1][0])
        appeared = [info[0] for _, info in reports]
        removed_early = [
            {"report": report, "written": info[0], "gone": info[1]}
            for report, info in reports
            if info[1] < end - 1 and info[1] - info[0] < 60]
        uptime_error = [round(info[2] - info[0], 1) for _, info in reports
                        if info[2] is not None]
        result.update({
            "reports_appeared_at": appeared,
            "report_names": [report for report, _ in reports],
            "uptime_minus_real": uptime_error,
            "removed_before_history_keep": removed_early,
            "pull_alive": pull.poll() is None})
        expected = [20, 30, 40, 50]
        result["ok"] = bool(
            len(appeared) == len(expected) and
            all(abs(a - e) <= 2.5 for a, e in zip(appeared, expected)) and
            all(abs(u) <= 3 for u in uptime_error) and not removed_early and
            result["pull_alive"])
    finally:
        stop(pull)
        source.stop()
    return result


def case_skew(name: str, variant: str) -> dict:
    work, source, _, binsrv, machine = setup(name)
    clock = FakeClock(work / "faketime", False, SKEWS[variant])
    binsrv.rewrite_file_size = "8K"
    result = {"pbs_offset": SKEWS[variant]}
    try:
        spaced_workload(source, machine)
        result["fetch"] = fetch(binsrv, clock.environment())
        result["stored_transactions"] = stored_transaction_count(binsrv)
        result["source_transactions"] = source_transaction_count(source)
        if ARGS.mode == "position":
            result["files_identical"] = all(
                data == source.binlog_as_sent(name)
                for name, data in stored_files(binsrv))
        result.update(check_metadata_and_search(binsrv))
        result["ok"] = bool(
            result["fetch"]["exit"] == 0 and
            result["stored_transactions"] == result["source_transactions"]
            and result.get("files_identical", True) and
            not result["metadata_wrong"] and not result["search_wrong"])
    finally:
        source.stop()
    return result


def case_tz(name: str, variant: str) -> dict:
    work, source, _, binsrv, machine = setup(name)
    binsrv.rewrite_file_size = "8K"
    result = {}
    try:
        spaced_workload(source, machine, binlogs=2, pause=1.5)
        if variant == "dst_log":
            # libfaketime reads '@...' in the process's TZ, so give the
            # start as an offset: 3 s before the DST end in New York
            offset = calendar.timegm((2026, 11, 1, 5, 59, 57)) - \
                int(time.time())
            clock = FakeClock(work / "faketime", False, f"{offset:+d}")
            environment = dict(clock.environment(), TZ="America/New_York")
            result["fetch"] = fetch(binsrv, environment)
            stamps = [match.group(1) for match in
                      (LOG_TIMESTAMP.match(line) for line in
                       binsrv.log_path.read_text(errors="replace")
                       .splitlines()) if match]
            result["log_first_last"] = [stamps[0], stamps[-1]] if stamps \
                else None
            utc = [s for s in stamps if s.startswith(("2026-11-01T05:59",
                                                      "2026-11-01T06:00"))]
            result["ok"] = bool(result["fetch"]["exit"] == 0 and stamps and
                                len(utc) == len(stamps))
            return result
        result["fetch"] = fetch(binsrv, {"TZ": "UTC"})
        if variant == "zones":
            reference = None
            differences = []
            probes = [DST_PROBE, "2026-03-08T02:30:00",
                      "2026-10-25T02:30:00"]
            ranges = truth_ranges(binsrv)
            for low, high in ranges.values():
                probes += [iso(low - 1), iso(low), iso(high)]
            for zone in ZONES:
                environment = {"TZ": zone}
                answers = {"list": run_binsrv(binsrv, "list",
                                              environment=environment)
                           ["answer"]}
                for probe in probes:
                    answers[probe] = run_binsrv(
                        binsrv, "search_by_timestamp", probe,
                        environment=environment)["answer"]
                if reference is None:
                    reference = answers
                    continue
                for key, value in answers.items():
                    if value != reference[key]:
                        differences.append(f"TZ={zone} {key}: {value} "
                                           f"vs UTC {reference[key]}")
            result.update(check_metadata_and_search(binsrv))
            result["zones"] = ZONES
            result["probes_per_zone"] = len(probes) + 1
            result["differences"] = differences
            result["ok"] = bool(result["fetch"]["exit"] == 0 and
                                not differences and
                                not result["metadata_wrong"] and
                                not result["search_wrong"])
            return result
        # fetch_in_zone: a second storage fetched under New York time
        first = {name: json.loads((binsrv.storage / f"{name}.json")
                                  .read_text())
                 for name in binsrv.indexed_binlogs()}
        first_files = dict(stored_files(binsrv))
        harness.BINSRV = second = harness.BinlogServer(
            ARGS.binsrv, work / "binsrv-ny", ARGS.port, ARGS.mode == "gtid")
        second.rewrite_file_size = "8K"
        second.reset()
        result["fetch_ny"] = fetch(second, {"TZ": "America/New_York"})
        again = {name: json.loads((second.storage / f"{name}.json")
                                  .read_text())
                 for name in second.indexed_binlogs()}
        differences = []
        if ARGS.mode == "position":
            if again != first:
                differences.append(f"metadata {again} vs {first}")
            if dict(stored_files(second)) != first_files:
                differences.append("binlog files differ")
        else:
            for name in first:
                if again.get(name, {}).get("min_timestamp") != \
                        first[name].get("min_timestamp"):
                    differences.append(
                        f"{name} min_timestamp "
                        f"{again.get(name, {}).get('min_timestamp')} vs "
                        f"{first[name].get('min_timestamp')}")
        result["differences"] = differences
        result["ok"] = bool(result["fetch"]["exit"] == 0 and
                            result["fetch_ny"]["exit"] == 0 and
                            not differences)
    finally:
        source.stop()
    return result


def listener_unix_timestamp(pull_port: int) -> "int | None":
    completed = subprocess.run(
        [str(ARGS.mysql_basedir / "bin" / "mysql"), "--no-defaults",
         "-h127.0.0.1", f"-P{pull_port}", f"-u{replication.PULL_USER}",
         f"-p{replication.PULL_PASSWORD}", "--get-server-public-key",
         "--ssl-mode=DISABLED", "-N", "-B", "-e", "SELECT UNIX_TIMESTAMP()"],
        capture_output=True, text=True, timeout=30, check=False)
    try:
        return int(completed.stdout.strip())
    except ValueError:
        return None


def case_future(name: str, variant: str) -> dict:
    work, source, _, binsrv, machine = setup(name)
    start = FUTURES[variant]
    fake_start = from_iso(start[1:].replace(" ", "T"))
    clock = FakeClock(work / "faketime", False, start)
    binsrv.rewrite_file_size = "8K"
    result = {"pbs_clock_start": start[1:]}
    pull = None
    try:
        spaced_workload(source, machine, binlogs=2, pause=1.5)
        clock.set(start)
        result["fetch"] = fetch(binsrv, clock.environment())
        result["stored_transactions"] = stored_transaction_count(binsrv)
        result["source_transactions"] = source_transaction_count(source)
        stamps = [m.group(1) for m in
                  (LOG_TIMESTAMP.match(line) for line in
                   binsrv.log_path.read_text(errors="replace").splitlines())
                  if m] if binsrv.log_path.exists() else []
        result["fetch_log_first"] = stamps[0] if stamps else None
        result.update(check_metadata_and_search(binsrv))
        # generated-event timestamps in the stored files (rewrite mode)
        generated = set()
        for _, data in stored_files(binsrv):
            for offset, type_code, _ in harness.parse_events(data):
                if type_code not in harness.TRANSACTION_START_EVENTS:
                    stamp = struct.unpack_from("<I", data, offset)[0]
                    if stamp and abs(stamp - time.time()) > 86400:
                        generated.add(iso(stamp))
        result["non_source_event_times"] = sorted(generated)
        # pull with the same clock: telemetry report name, listener time
        clock.set(start)
        pull_port = ARGS.port + 2
        pull = start_pull(binsrv, pull_port, clock,
                          {"grace_interval": "20s", "scrape_interval": "10s",
                           "history_keep_interval": "60s"})
        time.sleep(5)
        result["listener_unix_timestamp"] = listener_unix_timestamp(pull_port)
        time.sleep(19)
        reports = sorted(path.name for path in binsrv.telemetry.iterdir()
                         if path.suffix == ".json") \
            if binsrv.telemetry.exists() else []
        result["telemetry_reports"] = reports
        result["pull_alive"] = pull.poll() is None
        expected_now = fake_start + 5
        listener = result["listener_unix_timestamp"]
        report_ok = bool(reports) and all(
            re.match(r"^\d+-", report) and
            abs(int(report.split("-")[0]) - fake_start - 21) < 30
            for report in reports)
        result["telemetry_name_ok"] = report_ok
        result["listener_time_ok"] = (listener is not None and
                                      abs(listener - expected_now) < 30)
        result["ok"] = bool(
            result["fetch"]["exit"] == 0 and
            result["stored_transactions"] == result["source_transactions"]
            and not result["metadata_wrong"] and
            not result["search_wrong"] and result["pull_alive"] and
            report_ok and result["listener_time_ok"] and
            result["fetch_log_first"] and
            result["fetch_log_first"][:4] == start[1:5])
    finally:
        stop(pull)
        source.stop()
    return result


GROUPS = {
    "heartbeat": (case_heartbeat, list(JUMPS)),
    "reconnect": (case_reconnect, list(JUMPS)),
    "checkpoint": (case_checkpoint, ["idle_tail", *JUMPS]),
    "telemetry": (case_telemetry, list(JUMPS)),
    "skew": (case_skew, list(SKEWS)),
    "tz": (case_tz, ["zones", "fetch_in_zone", "dst_log"]),
    "future": (case_future, list(FUTURES)),
}


def main():
    global ARGS
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--mysql-basedir", type=Path, required=True)
    parser.add_argument("--binsrv", type=Path, required=True)
    parser.add_argument("--faketime-lib", type=Path, required=True)
    parser.add_argument("--workdir", type=Path, required=True)
    parser.add_argument("--mode", choices=["position", "gtid"],
                        default="position")
    parser.add_argument("--port", type=int, default=24610)
    parser.add_argument("--cases", default="*")
    ARGS = parser.parse_args()
    for attribute in ("mysql_basedir", "binsrv", "faketime_lib", "workdir"):
        setattr(ARGS, attribute, getattr(ARGS, attribute).expanduser())
    ARGS.workdir.mkdir(parents=True, exist_ok=True)
    patterns = ARGS.cases.split(",")
    failed = []
    total = 0
    for group, (function, variants) in GROUPS.items():
        if group == "heartbeat" and ARGS.mode == "gtid":
            continue    # the heartbeat case places its replica by file
                        # and position; pull serves auto-position
                        # replicas too (pull_soak_test --mode gtid)
        for variant in variants:
            name = f"{group}/{variant}"
            if not any(fnmatch.fnmatchcase(name, pattern)
                       for pattern in patterns):
                continue
            total += 1
            started = time.monotonic()
            try:
                result = function(name, variant)
            except Exception as error:  # pylint: disable=broad-except
                result = {"ok": False,
                          "error": harness.describe_failure(error)}
            result = {"case": f"{ARGS.mode}/{name}", **result,
                      "seconds": round(time.monotonic() - started, 1)}
            print(json.dumps(result), flush=True)
            if not result["ok"]:
                failed.append(result["case"])
    print(json.dumps({"summary": True, "cases": total,
                      "failed": len(failed), "failed_cases": failed}),
          flush=True)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
