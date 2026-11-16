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

"""File permissions and symlink handling of the storage, the S3 backend's
local buffer and the telemetry directory.

The storage holds a copy of every change made on the source (binlog data,
encrypted or not, and its metadata). The filesystem backend creates its files
with std::ofstream and a write-temp-then-rename helper, so their modes come
from the process umask and an existing symlink is followed
(filesystem_storage_backend.cpp has a 'TODO: check permissions here').
Cases:

- 'modes/<umask>': 'fetch' runs with umask 022 (the usual default) and with
  umask 000 (a permissive service manager or container), and the mode of
  every file and directory it creates is recorded: the storage, the local
  settings, the telemetry directory and the log. Under umask 000 nothing may
  be world-writable; under umask 022 binlog data and metadata may not be
  world-readable (MySQL creates its binlogs 0640 whatever the umask).
- 'symlinks': while 'pull' runs, someone who can write the storage directory
  but not the files PBS's account owns elsewhere plants symlinks under the
  names PBS will create next: the next binlog, its metadata's temporary file
  and the index's temporary file, each pointing at a file outside the
  storage. The source then rotates its binlog and writes. The targets must be
  left unchanged: PBS must not write through a symlink it did not create.
- 'telemetry_symlinks': 'pull' writes a telemetry report every 10 s as
  '<unix seconds>-<tag>.tmp', renamed to '.json'. The tag is in the name of
  every finished report, so someone who can write the telemetry directory
  (by design a directory another component reads and cleans) plants
  symlinks under the next reports' temporary names, pointing at a file of
  mode 0600 outside it. That file must keep its content and its mode
  (ticket 66).
- 's3_buffer' (with --s3-bucket-dir, against the local S3 server): the S3
  backend keeps the binlog it writes in a local file. Without
  'fs_buffer_directory' that file is created under $TMPDIR; under umask 022
  neither it nor its directory may be accessible to other users, and an
  existing 'fs_buffer_directory' that anyone can write to must be refused
  (ticket 65).

A JSON line per case, then a summary; exit 1 if any case failed. Example:

  PYTHONPATH=~/ws/qa/pylib python3 file_security_test.py \\
    --mysql-basedir ~/ws/dist/mysql-8.4.8-linux-glibc2.28-x86_64-minimal \\
    --binsrv ~/ws/percona-binlog-server-build-debug_gcc14/binlog_server \\
    --workdir ~/ws/qa/file-security
"""

import argparse
import json
import os
import re
import shutil
import signal
import stat
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import fetch_crash_test as harness  # noqa: E402

WORKLOAD = [{"rows": [300], "ddl": True}]
BINLOG_NAME = re.compile(r"^binlog\.(\d{6})$")
VICTIM_CONTENT = b"VICTIM FILE: PBS must not write here\n"


def mode_of(path: Path) -> str:
    return oct(stat.S_IMODE(path.lstat().st_mode))


def category(relative: str) -> str:
    name = relative.rsplit("/", 1)[-1]
    if relative.startswith("storage/"):
        if BINLOG_NAME.match(name):
            return "binlog data"
        if name.endswith(".json") and name != "metadata.json":
            return "binlog metadata"
        return "storage " + name
    return relative.split("/", 1)[0]


def created_modes(workdir: Path) -> "list[dict]":
    entries = []
    for path in sorted(workdir.rglob("*")):
        relative = path.relative_to(workdir).as_posix()
        if relative.startswith("mysql"):
            continue
        entries.append({"path": relative, "dir": path.is_dir(),
                        "mode": mode_of(path), "category": category(relative)})
    return entries


def case_modes(umask: int) -> dict:
    machine = harness.FetchCrashMachine()
    machine.start_from_scratch()
    machine.run_workload(WORKLOAD)
    previous = os.umask(umask)
    try:
        machine.fetch_to_the_end()
    finally:
        os.umask(previous)
    workdir = harness.BINSRV.workdir
    entries = [entry for entry in created_modes(workdir)
               if entry["path"] != "binsrv.json"]
    problems = []
    for entry in entries:
        mode = int(entry["mode"], 8)
        if mode & stat.S_IWOTH:
            problems.append(f"{entry['path']} is world-writable "
                            f"({entry['mode']})")
        elif (umask == 0o022 and mode & stat.S_IROTH and not entry["dir"] and
              entry["category"] in ("binlog data", "binlog metadata")):
            problems.append(f"{entry['path']} ({entry['category']}) is "
                            f"world-readable ({entry['mode']})")
    by_category: dict = {}
    for entry in entries:
        key = entry["category"] + (" (dir)" if entry["dir"] else "")
        by_category.setdefault(key, set()).add(entry["mode"])
    return {"case": f"modes/{umask:03o}", "umask": f"{umask:03o}",
            "modes": {key: sorted(value)
                      for key, value in sorted(by_category.items())},
            "problems": problems[:20], "problem_count": len(problems),
            "ok": not problems}


def stored_binlog_numbers(storage: Path) -> "list[int]":
    numbers = []
    for path in storage.iterdir():
        match = BINLOG_NAME.match(path.name)
        if match:
            numbers.append(int(match.group(1)))
    return sorted(numbers)


def case_symlinks(workdir: Path, pull_port: int) -> dict:
    machine = harness.FetchCrashMachine()
    machine.start_from_scratch()
    machine.run_workload(WORKLOAD)
    machine.fetch_to_the_end()
    binsrv = harness.BINSRV
    binsrv.write_config()
    config = json.loads(binsrv.config_path.read_text())
    config["replication_source"]["port"] = pull_port
    binsrv.config_path.write_text(json.dumps(config, indent=2))

    victims_dir = workdir / "outside-the-storage"
    victims_dir.mkdir(parents=True, exist_ok=True)
    result = {"case": "symlinks"}
    pull = subprocess.Popen(
        [str(binsrv.binary), "pull", str(binsrv.config_path)],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        env=dict(os.environ, AWS_EC2_METADATA_DISABLED="true"))
    try:
        # let pull open the storage and catch up with the source
        time.sleep(5)
        numbers = stored_binlog_numbers(binsrv.storage)
        if not numbers or pull.poll() is not None:
            raise harness.Failure("pull did not start on the storage")
        upcoming = f"binlog.{numbers[-1] + 1:06d}"
        planted = {upcoming: "next binlog data",
                   upcoming + ".json.tmp": "next binlog metadata (temporary)",
                   "binlog.index.tmp": "binlog index (temporary)"}
        victims = {}
        for name, what in planted.items():
            victim = victims_dir / (name + ".victim")
            victim.write_bytes(VICTIM_CONTENT)
            link = binsrv.storage / name
            if link.exists() or link.is_symlink():
                link.unlink()
            link.symlink_to(victim)
            victims[name] = (victim, what)
        harness.SERVER.sql("FLUSH BINARY LOGS")
        machine.run_workload(WORKLOAD)
        time.sleep(6)
        written_through = []
        for name, (victim, what) in victims.items():
            content = victim.read_bytes()
            if content != VICTIM_CONTENT:
                written_through.append({"planted": name, "what": what,
                                        "victim_size": len(content)})
        result.update({
            "upcoming_binlog": upcoming,
            "planted": list(planted),
            "written_through": written_through,
            "pull_alive": pull.poll() is None,
            "storage_entries": sorted(
                f"{path.name}{' -> symlink' if path.is_symlink() else ''}"
                for path in binsrv.storage.iterdir())[-12:]})
        result["ok"] = not written_through
        if written_through:
            result["why"] = ("PBS wrote through symlinks planted in the "
                             "storage directory into files outside it: " +
                             ", ".join(item["what"]
                                       for item in written_through))
    finally:
        if pull.poll() is None:
            pull.send_signal(signal.SIGTERM)
            try:
                pull.wait(timeout=60)
            except subprocess.TimeoutExpired:
                pull.kill()
    return result


def start_pull(binsrv, pull_port: int, edit=None,
               environment: "dict | None" = None) -> subprocess.Popen:
    """'pull' with its listener on 'pull_port'; 'edit' changes the
    configuration before it is written."""
    binsrv.write_config()
    config = json.loads(binsrv.config_path.read_text())
    config["replication_source"]["port"] = pull_port
    if edit is not None:
        edit(config)
    binsrv.config_path.write_text(json.dumps(config, indent=2))
    return subprocess.Popen(
        [str(binsrv.binary), "pull", str(binsrv.config_path)],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        env=dict(os.environ, AWS_EC2_METADATA_DISABLED="true",
                 **(environment or {})))


def stop_pull(pull: subprocess.Popen):
    if pull.poll() is None:
        pull.send_signal(signal.SIGTERM)
        try:
            pull.wait(timeout=60)
        except subprocess.TimeoutExpired:
            pull.kill()
            pull.wait()


def case_telemetry_symlinks(workdir: Path, pull_port: int) -> dict:
    machine = harness.FetchCrashMachine()
    machine.start_from_scratch()
    machine.run_workload(WORKLOAD)
    machine.fetch_to_the_end()
    binsrv = harness.BINSRV
    shutil.rmtree(binsrv.telemetry, ignore_errors=True)
    victim = workdir / "outside-the-telemetry-directory.victim"
    victim.write_bytes(VICTIM_CONTENT)
    victim.chmod(0o600)

    def smallest_intervals(config):
        # clamped to the smallest ones: a first report after 20 s, then
        # one every 10 s (PBS-122)
        config["telemetry"].update({"grace_interval": "0",
                                    "scrape_interval": "0"})

    result = {"case": "telemetry_symlinks"}
    pull = start_pull(binsrv, pull_port, smallest_intervals)
    try:
        reports = []
        deadline = time.monotonic() + 60
        while not reports and time.monotonic() < deadline:
            reports = sorted(binsrv.telemetry.glob("*.json")) \
                if binsrv.telemetry.exists() else []
            time.sleep(0.5)
        if not reports:
            raise harness.Failure("pull wrote no telemetry report in 60 s\n" +
                                  binsrv.log_tail())
        # what the finished report's name gives away: the time of the next
        # ones and the tag in their names
        stamp, tag = reports[-1].stem.split("-", 1)
        planted = 0
        for second in range(int(stamp) + 1, int(stamp) + 31):
            link = binsrv.telemetry / f"{second}-{tag}.tmp"
            if not link.exists() and not link.is_symlink():
                link.symlink_to(victim)
                planted += 1
        time.sleep(25)
        content = victim.read_bytes()
        problems = []
        if content != VICTIM_CONTENT:
            problems.append("PBS wrote a telemetry report through a planted "
                            "symlink into a file outside the telemetry "
                            f"directory ({len(content)} bytes)")
        if mode_of(victim) != "0o600":
            problems.append("the file outside the telemetry directory "
                            f"went from mode 0o600 to {mode_of(victim)}")
        result.update({
            "first_report": reports[-1].name, "symlinks_planted": planted,
            "victim_changed": content != VICTIM_CONTENT,
            "victim_mode": mode_of(victim),
            "reports_that_are_symlinks": sorted(
                path.name for path in binsrv.telemetry.glob("*.json")
                if path.is_symlink()),
            "pull_alive": pull.poll() is None, "ok": not problems})
        if problems:
            result["why"] = "; ".join(problems)
    finally:
        stop_pull(pull)
    return result


def case_s3_buffer(args, workdir: Path, pull_port: int) -> dict:
    if args.s3_bucket_dir is None:
        return {"case": "s3_buffer", "ok": True,
                "skipped": "no --s3-bucket-dir"}
    machine = harness.FetchCrashMachine()
    machine.start_from_scratch()
    machine.run_workload(WORKLOAD)
    s3 = harness.S3Storage(args.s3_endpoint, args.s3_bucket,
                           args.s3_bucket_dir.expanduser(),
                           args.s3_access_key, args.s3_secret_key,
                           f"file-security-{os.getpid()}")
    binsrv = harness.BinlogServer(harness.BINSRV.binary,
                                  workdir / "binsrv-s3", harness.BINSRV.port,
                                  harness.BINSRV.gtid_mode, s3)
    result = {"case": "s3_buffer"}
    problems = []
    previous_umask = os.umask(0o022)
    try:
        # 1. no fs_buffer_directory: the buffer is created under $TMPDIR
        binsrv.reset()
        tmpdir = workdir / "tmpdir-of-pull"
        shutil.rmtree(tmpdir, ignore_errors=True)
        tmpdir.mkdir(parents=True)

        def default_buffer(config):
            del config["storage"]["fs_buffer_directory"]

        pull = start_pull(binsrv, pull_port, default_buffer,
                          {"TMPDIR": str(tmpdir)})
        try:
            time.sleep(8)
            if pull.poll() is not None:
                raise harness.Failure("pull did not start on the S3 "
                                      "storage\n" + binsrv.log_tail())
            entries = [{"path": path.relative_to(tmpdir).as_posix(),
                        "dir": path.is_dir(), "mode": mode_of(path)}
                       for path in sorted(tmpdir.rglob("*"))]
        finally:
            stop_pull(pull)
        if not entries:
            raise harness.Failure("pull created no buffer under $TMPDIR")
        for entry in entries:
            if int(entry["mode"], 8) & (stat.S_IRWXG | stat.S_IRWXO):
                problems.append(
                    f"the buffer {'directory' if entry['dir'] else 'file'} "
                    f"under $TMPDIR is {entry['mode']} under umask 022")
        result["default_buffer"] = entries[:6]

        # 2. an existing fs_buffer_directory that anyone can write to
        binsrv.reset()
        shared = workdir / "buffer-anyone-can-write"
        shutil.rmtree(shared, ignore_errors=True)
        shared.mkdir(parents=True)
        shared.chmod(0o777)

        def shared_buffer(config):
            config["storage"]["fs_buffer_directory"] = str(shared)

        pull = start_pull(binsrv, pull_port, shared_buffer)
        try:
            time.sleep(8)
            accepted = pull.poll() is None
            used = sorted(path.name for path in shared.iterdir())
        finally:
            stop_pull(pull)
        result["world_writable_buffer_directory"] = {
            "accepted": accepted, "files_created": len(used)}
        if accepted or used:
            problems.append("an existing fs_buffer_directory of mode 0777 "
                            "was accepted and used")
    finally:
        os.umask(previous_umask)
        shutil.rmtree(binsrv.storage, ignore_errors=True)
    result["problems"] = problems
    result["ok"] = not problems
    if problems:
        result["why"] = "; ".join(problems)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--mysql-basedir", type=Path, required=True)
    parser.add_argument("--binsrv", type=Path, required=True)
    parser.add_argument("--workdir", type=Path, required=True)
    parser.add_argument("--port", type=int, default=23560)
    parser.add_argument("--mode", choices=["gtid", "position"],
                        default="position")
    parser.add_argument("--s3-endpoint", default="127.0.0.1:9000")
    parser.add_argument("--s3-bucket", default="pbs-mtr")
    parser.add_argument("--s3-bucket-dir", type=Path,
                        help="the bucket's directory on the local S3 "
                             "server; without it 's3_buffer' is skipped")
    parser.add_argument("--s3-access-key", default="pbsaccesskey")
    parser.add_argument("--s3-secret-key", default="pbs/secret+key=1")
    args = parser.parse_args()
    args.workdir.mkdir(parents=True, exist_ok=True)
    gtid_mode = args.mode == "gtid"
    harness.SERVER = harness.MySQLServer(args.mysql_basedir.expanduser(),
                                         args.workdir / "mysql", args.port,
                                         gtid_mode, server_id=1)
    harness.BINSRV = harness.BinlogServer(args.binsrv.expanduser(),
                                          args.workdir / "binsrv", args.port,
                                          gtid_mode)
    results = []
    harness.SERVER.start()
    try:
        for umask in (0o022, 0o000):
            try:
                result = case_modes(umask)
            except harness.Failure as failure:
                result = {"case": f"modes/{umask:03o}", "ok": False,
                          "why": str(failure)[:500]}
            print(json.dumps(result), flush=True)
            results.append(result)
        for name, case in (
                ("symlinks",
                 lambda: case_symlinks(args.workdir, args.port + 2)),
                ("telemetry_symlinks",
                 lambda: case_telemetry_symlinks(args.workdir,
                                                 args.port + 3)),
                ("s3_buffer",
                 lambda: case_s3_buffer(args, args.workdir,
                                        args.port + 4))):
            try:
                result = case()
            except (harness.Failure, OSError) as failure:
                result = {"case": name, "ok": False,
                          "why": str(failure)[:500]}
            print(json.dumps(result), flush=True)
            results.append(result)
    finally:
        harness.SERVER.stop()
    failures = [result for result in results if not result.get("ok")]
    print(json.dumps({"cases": len(results), "failures": len(failures),
                      "passed": not failures}), flush=True)
    (args.workdir / "result.json").write_text(json.dumps(results, indent=2))
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
