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

"""Telemetry reports of 'binlog_server pull' and the instance_id (PBS-41).

The MTR suite covers one report, a missing telemetry directory, cleanup of
a stale and a recent foreign report, 'enabled: false' and a spoiled
instance_id. This harness covers the rest, one JSON line per case.

'pull' cases (a position-mode source, 'pull' run for a case's time with its
own telemetry section, then stopped with SIGTERM; every case also checks
that 'pull' stayed up and still serves a binlog to mysqlbinlog). Since
PBS-122 'pull' clamps the intervals into ranges (grace 20s..2d, scrape
10s..7d, history 60s..7d) and logs the values it uses, which the cases
report as 'effective' and the clamping cases check; reports therefore come
20 s after the start at the earliest and then every 10 s:

  reports              grace 20s, scrape 10s, 45 s: several reports, each named
                       '<unix_ts>-<tag>.json' with one tag, mode 0644, no
                       '.tmp' left, exactly the fields db_instance_id (the
                       instance_id file), pillar_version and a growing
                       uptime;
  scrape_zero          grace and scrape_interval 0: clamped to 20 s and
                       10 s (logged), so 2 to 4 reports in 45 s, not a
                       tight loop (ticket 49, fixed by PBS-122);
  scrape_negative      a scrape_interval past INT64_MAX seconds
                       ('106751991167301d', ~292 million years) is clamped
                       to the 7-day maximum, not wrapped to a negative or
                       zero interval: one report in 45 s;
  scrape_huge          scrape_interval HUGE_INTERVAL ('200000d', ~550
                       years, past the ~292 years of int64 nanoseconds):
                       clamped to 7 days, one report after the grace
                       interval and no more (no overflow into the past);
  grace_huge           grace_interval HUGE_INTERVAL: clamped to 2 days, no
                       report in 25 s (a grace clamped to its minimum
                       would give one);
  path_is_file         the telemetry path is a regular file: no report,
                       a warning, pull keeps serving;
  path_unwritable      the telemetry directory is mode 0555: the same;
  cleanup_odd_names    history and scrape at their minimums: a recent foreign
                       report is kept and an 8-day-old one removed, and
                       files that only look like reports (no delimiter,
                       a leading '-', a timestamp past int64, another
                       extension) are left alone;
  shared_directory     two 'pull' instances (own storages, local
                       settings and server ids) with history 60s share one telemetry
                       directory: each keeps writing and neither removes
                       the other's recent reports;
  settings_in_storage  instance.local_settings_path is the storage
                       directory itself: fetch, list and pull still work
                       with the instance_id file among the binlogs;
  damaged_id           a damaged instance_id is replaced by a new
                       canonical UUID on a 'fetch' (README); whether the
                       new identity is logged is reported, not checked.

File system fault cases (with --fsfault-lib; pull with reports from 20 s on,
every 10 s, for FAULT_SECONDS while every matching call inside the telemetry
directory fails, through the fsfault LD_PRELOAD library; pull must keep
serving):

  fault_write_enospc   writes fail with ENOSPC: no report, every failure
                       logged, the history cleanup removes the temporary
                       files left behind (at most 3 remain);
  fault_open_erofs     opening a report fails with EROFS: the same;
  fault_fsync_eio      fsync fails with EIO: the same;
  fault_rename         renaming a finished report fails: no report, each
                       one logged as not finalized, its temporary file left
                       for the history cleanup;
  fault_unlink         removing expired reports (an 8-day-old foreign
                       report is planted, as the 60 s history minimum leaves
                       no own report expired in the run) fails: each failure
                       logged, new reports are still written;
  fault_transient      the first two writes fail: both logged, later
                       reports are written and complete.

instance_id cases ('list' on an empty storage, no MySQL needed):

  readonly_without_id  local_settings_path is mode 0555 with no
                       instance_id yet: the directory must be writable
                       (README), so even 'list' fails, with a message
                       naming the instance id;
  readonly_with_id     the same with an instance_id present: 'list' works
                       and the id is kept;
  padded_id            the id surrounded by blanks and empty lines is
                       kept (and not rewritten);
  uppercase_id         an upper-case canonical UUID is kept as the same
                       instance (not silently replaced by a new id);
  id_is_directory      'instance_id' is a directory: the operation fails
                       with a message naming the instance id;
  leftover_tmp         an 'instance_id.tmp' left by a crash does not stop
                       a new id from being created;
  no_instance_section  a configuration of an earlier release (no
                       'instance' section) fails with a message naming
                       'instance'.

Uses the helpers of fetch_crash_test.py and pull_replica_test.py. Example:

  PYTHONPATH=~/ws/qa/pylib python3 telemetry_test.py \\
    --mysql-basedir ~/ws/dist/mysql-8.4.8-linux-glibc2.28-x86_64-minimal \\
    --binsrv ~/ws/percona-binlog-server-build-debug_gcc14/binlog_server \\
    --workdir ~/ws/qa/telemetry
"""

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import fetch_crash_test as harness  # noqa: E402
import pull_replica_test as replication  # noqa: E402

PULL_SECONDS = 8
# PBS-122's minimums: the first report 20 s after the start, then every 10 s
REPORT_SECONDS = 45
SINGLE_REPORT_SECONDS = 35
FAULT_SECONDS = 45
EFFECTIVE = re.compile(r"telemetry: reporting to .* with grace_interval=(\d+)s, "
                       r"scrape_interval=(\d+)s, history_keep_interval=(\d+)s")
DAY = 86400
# pull exits on SIGTERM only after the source read_timeout (PBS-116)
SOURCE_READ_TIMEOUT = 5
INT64_MAX = 2**63 - 1
# more seconds than int64 nanoseconds hold (~292 years)
HUGE_INTERVAL = "200000d"
REPORT_NAME = re.compile(r"^(\d+)-([0-9a-f-]{36})\.json$")
UUID = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-"
                  r"[0-9a-f]{12}$")
WORKLOAD = [{"rows": [2000, 500], "ddl": False}]
MYSQLBINLOG = Path("mysqlbinlog")  # set from --mysql-basedir in main()


def environment() -> dict:
    return dict(os.environ, AWS_EC2_METADATA_DISABLED="true")


def cpu_seconds(pid: int) -> float:
    fields = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
    # utime and stime, the 14th and 15th fields of the whole line
    return (int(fields[11]) + int(fields[12])) / os.sysconf("SC_CLK_TCK")


def streams_binlogs(port: int) -> bool:
    """A clean login plus a non-blocking dump of binlog.000001 with the real
    mysqlbinlog; True if pull streamed at least one binlog event."""
    try:
        completed = subprocess.run(
            [str(MYSQLBINLOG), "--no-defaults", "--read-from-remote-server",
             "--host=127.0.0.1", f"--port={port}",
             f"--user={replication.PULL_USER}",
             f"--password={replication.PULL_PASSWORD}",
             "--default-auth=caching_sha2_password", "--get-server-public-key",
             "--ssl-mode=DISABLED", "binlog.000001"],
            capture_output=True, timeout=30, check=False)
    except subprocess.TimeoutExpired:
        return False
    return completed.returncode == 0 and b"Start: binlog" in completed.stdout


def reports(directory: Path) -> list:
    """(timestamp, tag, path) of the report files in 'directory', oldest
    first."""
    found = []
    if not directory.is_dir():
        return found
    for path in directory.iterdir():
        match = REPORT_NAME.match(path.name)
        if match:
            found.append((int(match.group(1)), match.group(2), path))
    return sorted(found)


def effective(binsrv) -> "dict | None":
    """The intervals 'pull' logged that it uses (after PBS-122's clamp)."""
    if not binsrv.log_path.exists():
        return None
    match = EFFECTIVE.search(binsrv.log_path.read_text(errors="replace"))
    if match is None:
        return None
    return dict(zip(("grace", "scrape", "history"),
                    (int(value) for value in match.groups())))


def log_count(binsrv, pattern: str) -> int:
    if not binsrv.log_path.exists():
        return 0
    return binsrv.log_path.read_text(errors="replace").count(pattern)


class Pull:
    """'binlog_server pull' with a given telemetry section, its listener on
    'port'."""

    def __init__(self, binsrv, port: int, telemetry: dict,
                 local_settings: "Path | None" = None, server_id: int = 4242,
                 extra_environment: "dict | None" = None):
        self.binsrv = binsrv
        self.port = port
        binsrv.read_timeout = SOURCE_READ_TIMEOUT
        binsrv.write_config()
        config = json.loads(binsrv.config_path.read_text())
        config["replication_source"] = {
            "port": port, "server_id": 142,
            "server_uuid": "00000000-0000-0000-0000-00000000008e",
            "read_timeout": 60, "write_timeout": 60,
            "require_secure_transport": False,
            "auth_rsa_private_key": str(replication.AUTH_RSA_PRIVATE_KEY),
            "authentication": {"user": replication.PULL_USER,
                               "password": replication.PULL_PASSWORD,
                               "plugin": "caching_sha2_password"}}
        config["telemetry"] = telemetry
        # the source drops the older of two dumps with one server id
        config["replication"]["server_id"] = server_id
        if local_settings is not None:
            config["instance"]["local_settings_path"] = str(local_settings)
        binsrv.config_path.write_text(json.dumps(config, indent=2))
        self.process = subprocess.Popen(
            [str(binsrv.binary), "pull", str(binsrv.config_path)],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            env=dict(environment(), **(extra_environment or {})))
        self.started = time.monotonic()

    def alive(self) -> bool:
        return self.process.poll() is None

    def stop(self) -> int:
        if self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=60)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait()
        return self.process.returncode


class Cases:
    def __init__(self, args):
        self.args = args
        self.work = args.workdir
        self.source = harness.MySQLServer(args.mysql_basedir,
                                          self.work / "source", args.port,
                                          False, server_id=1)
        harness.SERVER = self.source
        self.next_port = args.port + 2

    def binsrv(self, name: str):
        """A fresh BinlogServer work directory for one case."""
        binsrv = harness.BinlogServer(self.args.binsrv, self.work / name,
                                      self.args.port, False)
        shutil.rmtree(binsrv.workdir, ignore_errors=True)
        binsrv.reset()
        return binsrv

    def port(self) -> int:
        self.next_port += 1
        return self.next_port

    def run_pull(self, name: str, telemetry: dict, seconds=PULL_SECONDS,
                 prepare=None, fault_rule: "str | None" = None) -> dict:
        """Runs pull for 'seconds' and returns what it did; 'fault_rule'
        (fsfault) makes file system calls in the telemetry directory
        fail."""
        binsrv = self.binsrv(name)
        telemetry = dict(telemetry)
        telemetry.setdefault("path", str(binsrv.telemetry))
        binsrv.telemetry.mkdir(parents=True, exist_ok=True)
        if prepare is not None:
            prepare(binsrv)
        fault_log = binsrv.workdir / "fsfault.log"
        extra_environment = None
        if fault_rule is not None:
            extra_environment = {
                "LD_PRELOAD": str(self.args.fsfault_lib),
                "FSFAULT_ROOT": str(binsrv.telemetry),
                "FSFAULT_RULES": fault_rule, "FSFAULT_LOG": str(fault_log)}
        pull = Pull(binsrv, self.port(), telemetry,
                    extra_environment=extra_environment)
        time.sleep(2)
        cpu_before = cpu_seconds(pull.process.pid) if pull.alive() else 0.0
        time.sleep(seconds - 2)
        cpu = (cpu_seconds(pull.process.pid) - cpu_before
               if pull.alive() else None)
        serves = pull.alive() and streams_binlogs(pull.port)
        alive = pull.alive()
        exit_code = pull.stop()
        return {"binsrv": binsrv, "alive": alive, "serves": serves,
                "exit_code": exit_code,
                "cpu_share": None if cpu is None else
                round(cpu / (seconds - 2), 3),
                "effective": effective(binsrv),
                "written": log_count(binsrv, "telemetry: wrote report"),
                "failed": log_count(binsrv, "telemetry: report write failed"),
                "not_finalized": log_count(binsrv,
                                           "telemetry: could not finalize"),
                "not_removed": log_count(binsrv,
                                         "telemetry: could not remove"),
                "injected": (len(fault_log.read_text().splitlines())
                             if fault_log.exists() else 0)}

    # 'pull' cases

    def reports(self) -> dict:
        run = self.run_pull("reports", {"grace_interval": "20s",
                                        "scrape_interval": "10s",
                                        "history_keep_interval": "1h"},
                            seconds=REPORT_SECONDS)
        binsrv = run["binsrv"]
        found = reports(binsrv.telemetry)
        instance_id = (binsrv.local_settings / "instance_id").read_text(
        ).strip()
        wrong = []
        tags = {tag for _, tag, _ in found}
        if len(found) < 2:
            wrong.append(f"{len(found)} reports")
        if len(tags) > 1:
            wrong.append(f"{len(tags)} tags")
        uptimes = []
        for _, _, path in found:
            mode = path.stat().st_mode & 0o777
            if mode != 0o644:
                wrong.append(f"{path.name} mode {oct(mode)}")
            try:
                body = json.loads(path.read_text())
            except ValueError as error:
                wrong.append(f"{path.name}: {error}")
                continue
            if set(body) != {"db_instance_id", "pillar_version", "uptime"}:
                wrong.append(f"{path.name} fields {sorted(body)}")
                continue
            if body["db_instance_id"] != instance_id:
                wrong.append(f"{path.name} db_instance_id "
                             f"{body['db_instance_id']}")
            if not body["pillar_version"]:
                wrong.append(f"{path.name} empty pillar_version")
            if not str(body["uptime"]).isdigit():
                wrong.append(f"{path.name} uptime {body['uptime']!r}")
            else:
                uptimes.append(int(body["uptime"]))
        if uptimes != sorted(uptimes) or len(set(uptimes)) != len(uptimes):
            wrong.append(f"uptimes {uptimes}")
        leftovers = [path.name for path in binsrv.telemetry.iterdir()
                     if path.suffix == ".tmp"]
        if leftovers:
            wrong.append(f"left {leftovers}")
        return {"reports": len(found), "uptimes": uptimes, "wrong": wrong,
                "effective": run["effective"],
                "alive": run["alive"], "serves": run["serves"],
                "ok": not wrong and run["serves"]}

    def clamped(self, name: str, telemetry: dict, seconds: int,
                expected: dict, writes: range) -> dict:
        """pull with intervals out of range: the logged intervals must be
        'expected' and the reports written within 'writes'."""
        run = self.run_pull(name, telemetry, seconds=seconds)
        # reports may overwrite each other within a second, so count the
        # writes in the log
        return {**telemetry, "effective": run["effective"],
                "expected": expected, "writes": run["written"],
                "write_range": [writes.start, writes.stop - 1],
                "cpu_share": run["cpu_share"],
                "log_bytes": run["binsrv"].log_path.stat().st_size,
                "alive": run["alive"], "serves": run["serves"],
                "ok": run["serves"] and run["written"] in writes and
                run["effective"] is not None and
                all(run["effective"][key] == value
                    for key, value in expected.items())}

    def scrape_zero(self) -> dict:
        return self.clamped("scrape_zero",
                            {"grace_interval": "0", "scrape_interval": "0"},
                            REPORT_SECONDS, {"grace": 20, "scrape": 10},
                            range(2, 5))

    def scrape_negative(self) -> dict:
        # 106751991167301 days are more than INT64_MAX seconds
        assert 106751991167301 * DAY > INT64_MAX
        return self.clamped("scrape_negative",
                            {"grace_interval": "0",
                             "scrape_interval": "106751991167301d"},
                            REPORT_SECONDS, {"grace": 20, "scrape": 7 * DAY},
                            range(1, 2))

    def scrape_huge(self) -> dict:
        return self.clamped("scrape_huge",
                            {"grace_interval": "20s",
                             "scrape_interval": HUGE_INTERVAL},
                            SINGLE_REPORT_SECONDS,
                            {"grace": 20, "scrape": 7 * DAY}, range(1, 2))

    def grace_huge(self) -> dict:
        return self.clamped("grace_huge", {"grace_interval": HUGE_INTERVAL},
                            25, {"grace": 2 * DAY}, range(0, 1))

    def unusable_path(self, name: str, prepare, path=None) -> dict:
        telemetry = {"grace_interval": "20s", "scrape_interval": "10s"}
        if path is not None:
            telemetry["path"] = str(path)
        run = self.run_pull(name, telemetry, seconds=SINGLE_REPORT_SECONDS,
                            prepare=prepare)
        return {"writes": run["written"], "failures_logged": run["failed"],
                "alive": run["alive"], "serves": run["serves"],
                "ok": run["serves"] and run["written"] == 0 and
                run["failed"] > 0}

    def path_is_file(self) -> dict:
        path = self.work / "path_is_file" / "telemetry_file"
        return self.unusable_path(
            "path_is_file",
            lambda binsrv: path.write_text("not a directory\n"), path)

    def path_unwritable(self) -> dict:
        if os.geteuid() == 0:
            return {"skipped": "running as root", "ok": True}
        result = self.unusable_path(
            "path_unwritable", lambda binsrv: binsrv.telemetry.chmod(0o555))
        (self.work / "path_unwritable" / "telemetry").chmod(0o755)
        return result

    def cleanup_odd_names(self) -> dict:
        now = int(time.time())
        foreign = "11111111-2222-3333-4444-555555555555"
        planted = {
            "recent_foreign": f"{now - 86400}-{foreign}.json",
            "stale_foreign": f"{now - 8 * 86400}-{foreign}.json",
            "stale_foreign_tmp": f"{now - 8 * 86400}-{foreign}.tmp",
        }
        # files that only look like reports
        odd = {
            "no_delimiter": "1.json",
            "leading_delimiter": f"-{now - 8 * 86400}.json",
            "trailing_delimiter": f"{now - 8 * 86400}-.json",
            "past_int64": f"{2**64}-{foreign}.json",
            "other_extension": f"{now - 8 * 86400}-{foreign}.txt",
            "not_a_number": f"yesterday-{foreign}.json",
        }

        def prepare(binsrv):
            for name in list(planted.values()) + list(odd.values()):
                (binsrv.telemetry / name).write_text("{}")

        run = self.run_pull("cleanup_odd_names",
                            {"grace_interval": "0", "scrape_interval": "0",
                             "history_keep_interval": "0"},
                            seconds=SINGLE_REPORT_SECONDS, prepare=prepare)
        directory = run["binsrv"].telemetry
        present = {path.name for path in directory.iterdir()}
        own = [entry for entry in reports(directory) if entry[1] != foreign]
        wrong = []
        if planted["recent_foreign"] not in present:
            wrong.append("recent foreign report removed")
        for key in ("stale_foreign", "stale_foreign_tmp"):
            if planted[key] in present:
                wrong.append(f"{key} kept")
        for key, name in odd.items():
            if name not in present:
                wrong.append(f"{key} ({name}) removed")
        # every report written in the run is younger than the 60 s history
        if len(own) != run["written"]:
            wrong.append(f"{len(own)} own reports kept of "
                         f"{run['written']} written")
        if not run["written"]:
            wrong.append("no report written, so no cleanup ran")
        return {"own_reports": len(own), "writes": run["written"],
                "wrong": wrong, "alive": run["alive"],
                "serves": run["serves"], "ok": not wrong and run["serves"]}

    def shared_directory(self) -> dict:
        shared = self.work / "shared_telemetry"
        shutil.rmtree(shared, ignore_errors=True)
        shared.mkdir(parents=True)
        telemetry = {"path": str(shared), "grace_interval": "0",
                     "scrape_interval": "0", "history_keep_interval": "0"}
        pulls = [Pull(self.binsrv(f"shared_{index}"), self.port(), telemetry,
                      server_id=4242 + index) for index in range(2)]
        seen = set()
        deadline = time.monotonic() + SINGLE_REPORT_SECONDS
        while time.monotonic() < deadline:
            seen.update(tag for _, tag, _ in reports(shared))
            time.sleep(0.2)
        alive = [pull.alive() for pull in pulls]
        serves = [pull.alive() and streams_binlogs(pull.port)
                  for pull in pulls]
        final = {tag for _, tag, _ in reports(shared)}
        writes = [log_count(pull.binsrv, "telemetry: wrote report")
                  for pull in pulls]
        for pull in pulls:
            pull.stop()
        return {"tags_seen": len(seen), "tags_at_end": len(final),
                "writes": writes, "alive": alive, "serves": serves,
                "ok": all(serves) and len(seen) == 2 and len(final) == 2
                and min(writes) >= 2}

    def settings_in_storage(self) -> dict:
        binsrv = self.binsrv("settings_in_storage")
        binsrv.local_settings = binsrv.storage
        binsrv.write_config()
        fetch = subprocess.run(
            [str(binsrv.binary), "fetch", str(binsrv.config_path)],
            capture_output=True, text=True, timeout=120, env=environment())
        listed = subprocess.run(
            [str(binsrv.binary), "list", str(binsrv.config_path)],
            capture_output=True, text=True, timeout=120, env=environment())
        try:
            binlogs = [entry["name"] for entry in
                       json.loads(listed.stdout)["result"]]
        except (ValueError, KeyError, TypeError):
            binlogs = None
        pull = Pull(binsrv, self.port(), {"grace_interval": "0",
                                          "scrape_interval": "1s"},
                    local_settings=binsrv.storage)
        time.sleep(4)
        serves = pull.alive() and streams_binlogs(pull.port)
        pull.stop()
        return {"fetch_exit": fetch.returncode, "list_exit": listed.returncode,
                "listed": binlogs, "list_output": listed.stdout[:300],
                "pull_serves": serves,
                "ok": fetch.returncode == 0 and listed.returncode == 0 and
                bool(binlogs) and serves}

    # instance_id cases

    def list_run(self, name: str, prepare=None, config_edit=None) -> dict:
        binsrv = self.binsrv(name)
        binsrv.write_config()
        if config_edit is not None:
            config = json.loads(binsrv.config_path.read_text())
            config_edit(config)
            binsrv.config_path.write_text(json.dumps(config, indent=2))
        if prepare is not None:
            prepare(binsrv)
        completed = subprocess.run(
            [str(binsrv.binary), "list", str(binsrv.config_path)],
            capture_output=True, text=True, timeout=120, env=environment())
        id_path = binsrv.local_settings / "instance_id"
        content = id_path.read_text() if id_path.is_file() else None
        binsrv.local_settings.chmod(0o755)
        return {"binsrv": binsrv, "exit_code": completed.returncode,
                "output": (completed.stdout + completed.stderr)[-400:],
                "id": None if content is None else content.strip(),
                "content": content}

    def readonly_without_id(self) -> dict:
        if os.geteuid() == 0:
            return {"skipped": "running as root", "ok": True}
        run = self.list_run("readonly_without_id",
                            lambda binsrv: binsrv.local_settings.chmod(0o555))
        return {"exit_code": run["exit_code"], "output": run["output"],
                "ok": run["exit_code"] != 0 and
                "instance id" in run["output"].lower()}

    def readonly_with_id(self) -> dict:
        if os.geteuid() == 0:
            return {"skipped": "running as root", "ok": True}
        planted = "0f0e0d0c-0b0a-4908-8706-050403020100"

        def prepare(binsrv):
            (binsrv.local_settings / "instance_id").write_text(planted + "\n")
            binsrv.local_settings.chmod(0o555)
        run = self.list_run("readonly_with_id", prepare)
        return {"exit_code": run["exit_code"], "kept": run["id"] == planted,
                "output": run["output"],
                "ok": run["exit_code"] == 0 and run["id"] == planted}

    def planted_id(self, name: str, text: str, expected: "str | None"
                   ) -> dict:
        def prepare(binsrv):
            (binsrv.local_settings / "instance_id").write_text(text)
        run = self.list_run(name, prepare)
        result = {"exit_code": run["exit_code"], "id_after": run["id"]}
        if expected is None:
            return {**result, "binsrv": run["binsrv"]}
        result["kept"] = run["id"] is not None and \
            run["id"].lower() == expected.lower()
        result["rewritten"] = run["content"] != text
        result["ok"] = run["exit_code"] == 0 and result["kept"]
        return result

    def padded_id(self) -> dict:
        uuid = "0f0e0d0c-0b0a-4908-8706-050403020100"
        result = self.planted_id("padded_id", f"\n  {uuid}\t \n\n", uuid)
        result["ok"] = result["ok"] and not result["rewritten"]
        return result

    def uppercase_id(self) -> dict:
        uuid = "0F0E0D0C-0B0A-4908-8706-0504030201AB"
        return self.planted_id("uppercase_id", uuid + "\n", uuid)

    def damaged_id(self) -> dict:
        # 'fetch' rather than 'list', which writes no log
        binsrv = self.binsrv("damaged_id")
        binsrv.write_config()
        (binsrv.local_settings / "instance_id").write_text(
            "not-an-instance-id\n")
        completed = subprocess.run(
            [str(binsrv.binary), "fetch", str(binsrv.config_path)],
            capture_output=True, text=True, timeout=120, env=environment())
        id_after = (binsrv.local_settings / "instance_id").read_text().strip()
        log = (binsrv.log_path.read_text(errors="replace")
               if binsrv.log_path.exists() else "")
        logged = "instance" in log.lower() and any(
            word in log.lower() for word in ("invalid", "spoiled", "damaged",
                                             "regenerat", "replac"))
        replaced = bool(UUID.match(id_after))
        return {"exit_code": completed.returncode, "id_after": id_after,
                "log_lines": log.count("\n"), "replaced": replaced,
                "replacement_logged": logged,
                "ok": completed.returncode == 0 and replaced}

    def id_is_directory(self) -> dict:
        run = self.list_run(
            "id_is_directory",
            lambda binsrv: (binsrv.local_settings / "instance_id").mkdir())
        return {"exit_code": run["exit_code"], "output": run["output"],
                "ok": run["exit_code"] != 0 and
                "instance id" in run["output"].lower()}

    def leftover_tmp(self) -> dict:
        run = self.list_run(
            "leftover_tmp",
            lambda binsrv: (binsrv.local_settings / "instance_id.tmp"
                            ).write_text("0f0e0d0c-0b0a-4908-87"))
        return {"exit_code": run["exit_code"], "id": run["id"],
                "output": run["output"] if run["exit_code"] else "",
                "ok": run["exit_code"] == 0 and run["id"] is not None and
                bool(UUID.match(run["id"]))}

    def no_instance_section(self) -> dict:
        run = self.list_run("no_instance_section",
                            config_edit=lambda config: config.pop("instance"))
        return {"exit_code": run["exit_code"], "output": run["output"],
                "ok": run["exit_code"] != 0 and "instance" in run["output"]}

    # file system fault cases (--fsfault-lib): the failures stay inside
    # the telemetry directory and must never affect pull

    def telemetry_fault(self, name: str, rule: str, prepare=None,
                        seconds: int = FAULT_SECONDS) -> dict:
        """pull with reports from 20 s on, every 10 s, while every matching
        call in the telemetry directory fails."""
        run = self.run_pull(name, {"grace_interval": "0",
                                   "scrape_interval": "0",
                                   "history_keep_interval": "0"},
                            seconds=seconds, prepare=prepare,
                            fault_rule=rule)
        directory = run["binsrv"].telemetry
        tmp = sorted(path.name for path in directory.iterdir()
                     if path.suffix == ".tmp")
        return {"rule": rule, "injected": run["injected"],
                "writes": run["written"], "failures_logged": run["failed"],
                "not_finalized": run["not_finalized"],
                "not_removed": run["not_removed"],
                "reports": len(reports(directory)), "tmp_left": len(tmp),
                "alive": run["alive"], "serves": run["serves"],
                "exit_code": run["exit_code"], "run": run}

    def fault_failed_writes(self, name: str, rule: str) -> dict:
        result = self.telemetry_fault(name, rule)
        result.pop("run")
        # each failure is logged, nothing is finalized, and the history
        # cleanup removes the temporary files left behind
        result["ok"] = (result["serves"] and result["injected"] > 0 and
                        result["writes"] == 0 and
                        result["failures_logged"] > 0 and
                        result["reports"] == 0 and result["tmp_left"] <= 3)
        return result

    def fault_write_enospc(self) -> dict:
        return self.fault_failed_writes(
            "fault_write_enospc", "op=write,count=1000000,errno=ENOSPC")

    def fault_open_erofs(self) -> dict:
        return self.fault_failed_writes(
            "fault_open_erofs", "op=open,count=1000000,errno=EROFS")

    def fault_fsync_eio(self) -> dict:
        return self.fault_failed_writes(
            "fault_fsync_eio", "op=fsync,count=1000000,errno=EIO")

    def fault_rename(self) -> dict:
        result = self.telemetry_fault("fault_rename",
                                      "op=rename,count=1000000,errno=EIO")
        result.pop("run")
        # a report that cannot be renamed is reported as not finalized and
        # its temporary file is left for the history cleanup
        result["ok"] = (result["serves"] and result["injected"] > 0 and
                        result["writes"] == 0 and
                        result["not_finalized"] > 0 and
                        result["reports"] == 0 and result["tmp_left"] <= 3)
        return result

    def fault_unlink(self) -> dict:
        # the 60 s history minimum leaves no own report expired during the
        # run, so an 8-day-old foreign report is there to be removed
        stale = (f"{int(time.time()) - 8 * DAY}-"
                 "11111111-2222-3333-4444-555555555555.json")
        result = self.telemetry_fault(
            "fault_unlink", "op=unlink,count=1000000,errno=EIO",
            prepare=lambda binsrv: (binsrv.telemetry / stale).write_text("{}"))
        result.pop("run")
        # expired reports cannot be removed: each failure is logged, and
        # new reports are still written
        result["ok"] = (result["serves"] and result["injected"] > 0 and
                        result["writes"] > 0 and result["not_removed"] > 0)
        return result

    def fault_transient(self) -> dict:
        # reports at about 20, 30, 40 and 50 s: the first two fail
        result = self.telemetry_fault(
            "fault_transient", "op=write,nth=1,count=2,errno=EIO",
            seconds=FAULT_SECONDS + 15)
        run = result.pop("run")
        found = reports(run["binsrv"].telemetry)
        latest_fields = None
        if found:
            try:
                latest_fields = sorted(json.loads(found[-1][2].read_text()))
            except ValueError as error:
                latest_fields = f"not JSON: {error}"
        result["latest_fields"] = latest_fields
        # the two failed reports are logged, the later ones are written
        result["ok"] = (result["serves"] and result["injected"] == 2 and
                        result["failures_logged"] >= 1 and
                        result["writes"] >= 2 and
                        latest_fields == ["db_instance_id", "pillar_version",
                                          "uptime"])
        return result


PULL_CASES = ["reports", "scrape_zero", "scrape_negative", "scrape_huge",
              "grace_huge", "path_is_file", "path_unwritable",
              "cleanup_odd_names", "shared_directory", "settings_in_storage",
              "damaged_id"]
FAULT_CASES = ["fault_write_enospc", "fault_open_erofs", "fault_fsync_eio",
               "fault_rename", "fault_unlink", "fault_transient"]
ID_CASES = ["readonly_without_id", "readonly_with_id", "padded_id",
            "uppercase_id", "id_is_directory", "leftover_tmp",
            "no_instance_section"]


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--mysql-basedir", type=Path, required=True)
    parser.add_argument("--binsrv", type=Path, required=True)
    parser.add_argument("--workdir", type=Path, required=True)
    parser.add_argument("--port", type=int, default=23960)
    parser.add_argument("--mode", choices=["position"], default="position")
    parser.add_argument("--fsfault-lib", type=Path,
                        help="the fsfault LD_PRELOAD library: enables the "
                             "file system fault cases")
    parser.add_argument("--cases", nargs="*",
                        help="run only these cases (default: all)")
    args = parser.parse_args()
    args.mysql_basedir = args.mysql_basedir.expanduser()
    args.binsrv = args.binsrv.expanduser()
    if args.fsfault_lib is not None:
        args.fsfault_lib = args.fsfault_lib.expanduser().resolve()
    args.workdir.mkdir(parents=True, exist_ok=True)
    global MYSQLBINLOG
    MYSQLBINLOG = args.mysql_basedir / "bin" / "mysqlbinlog"

    selected = args.cases or (PULL_CASES + ID_CASES + (
        FAULT_CASES if args.fsfault_lib is not None else []))
    if any(name in FAULT_CASES for name in selected) and \
            args.fsfault_lib is None:
        parser.error("the fault cases need --fsfault-lib")
    cases = Cases(args)
    results = []
    pull_cases = [name for name in selected
                  if name in PULL_CASES + FAULT_CASES]
    if pull_cases:
        cases.source.start()
    try:
        if pull_cases:
            # one binlog with a few transactions for pull to fetch and
            # serve; its storage is per case
            cases.source.reset()
            for statement in harness.workload_sql(WORKLOAD * 3):
                cases.source.sql(statement)
        for name in selected:
            try:
                result = getattr(cases, name)()
            except Exception as error:  # reported as the case's failure
                result = {"error": harness.describe_failure(error, 600),
                          "ok": False}
            result = {"case": name, **result}
            print(json.dumps(result), flush=True)
            results.append(result)
    finally:
        if pull_cases:
            cases.source.stop()
    (args.workdir / "results.json").write_text(json.dumps(results, indent=2))
    return 0 if all(result.get("ok") for result in results) else 1


if __name__ == "__main__":
    sys.exit(main())
