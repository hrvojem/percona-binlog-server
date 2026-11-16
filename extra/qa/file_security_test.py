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

"""File permissions and symlink handling of the filesystem storage.

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


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--mysql-basedir", type=Path, required=True)
    parser.add_argument("--binsrv", type=Path, required=True)
    parser.add_argument("--workdir", type=Path, required=True)
    parser.add_argument("--port", type=int, default=23560)
    parser.add_argument("--mode", choices=["gtid", "position"],
                        default="position")
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
        try:
            result = case_symlinks(args.workdir, args.port + 2)
        except harness.Failure as failure:
            result = {"case": "symlinks", "ok": False,
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
