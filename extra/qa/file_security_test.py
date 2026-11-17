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
  (ticket 65, PBS-138).
- 's3_buffer_swap' (with --s3-bucket-dir): what that 0777 buffer directory
  allows. On a resumed 'fetch' the backend downloads the last binlog object
  into '<fs_buffer_directory>/<random uuid>' and then opens that PATH again
  for appending (do_open_stream; do_resize_object does the same around a
  resize). A watcher thread stands in for another account that can write
  the directory: as soon as the buffer file appears it renames something
  else over it, 5 times each:
  'regular_file': a file of its own content (and of the object's size);
  'symlink': a symlink to a canary "keyring" file outside the directory;
  'symlink_size_matched': the same with a canary of the object's size (a
  build with assertions compares the size of the path with the downloaded
  length before it opens the path, and aborts on a difference; a release
  build has no such check);
  'resize_symlink': the 'size' in the binlog's metadata is lowered first
  (through the S3 API), so that the next start cuts the object down to it
  (do_resize_object: download, resize_file() by path, open by path,
  upload); the temporary file of that is swapped for a symlink to a canary
  of the object's size.
  The case fails when the canary file was changed, or when the bucket object
  holds bytes that PBS did not download or receive from the source. The
  result says how many of the tries won the race. It needs no fault proxy:
  the window is the whole download plus the gap before the open.

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
import hashlib
import sys
import threading
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


SWAP_TRIES = 5
SWAPPED_BY = ("the same account, standing in for another account that can "
              "write the 0777 directory")


class BufferSwapper(threading.Thread):
    """Stands in for another account that can write the buffer directory.

    Everything it does is something any account may do in a directory of
    mode 0777 without the sticky bit, whoever owns the files in it: list the
    directory (r-x), create its own file or symlink in it (-w-), and rename
    its own entry over an entry it does not own (rename and unlink need
    write permission on the DIRECTORY only; the sticky bit, as on /tmp,
    would restrict them to the entry's owner, and fs.protected_symlinks
    applies only in sticky world-writable directories). It never opens,
    reads or writes the file PBS created, and uses no descriptor of PBS.
    The replacement is prepared in the directory beforehand; the swap itself
    is one rename(2), as soon as a new name shows up."""

    def __init__(self, directory: Path, prepared: Path):
        super().__init__(daemon=True)
        self.directory = str(directory)
        self.prepared = str(prepared)
        self.known = set(os.listdir(self.directory))
        self.stop_requested = False
        self.swapped = None
        self.error = None

    def run(self):
        while not self.stop_requested:
            try:
                names = os.listdir(self.directory)
            except OSError as error:
                self.error = str(error)
                return
            for name in names:
                if name in self.known:
                    continue
                try:
                    os.rename(self.prepared,
                              os.path.join(self.directory, name))
                    self.swapped = name
                except OSError as error:
                    self.error = str(error)
                return

    def stop(self):
        self.stop_requested = True
        self.join(timeout=10)


def run_fetch(binsrv, edit) -> "tuple[int, str]":
    binsrv.write_config()
    config = json.loads(binsrv.config_path.read_text())
    edit(config)
    binsrv.config_path.write_text(json.dumps(config, indent=2))
    process = subprocess.Popen(
        [str(binsrv.binary), "fetch", str(binsrv.config_path)],
        stdout=subprocess.DEVNULL, stderr=subprocess.PIPE,
        env=dict(os.environ, AWS_EC2_METADATA_DISABLED="true"))
    try:
        _, stderr = process.communicate(
            timeout=harness.FETCH_TIMEOUT_SECONDS)
    except subprocess.TimeoutExpired:
        process.kill()
        _, stderr = process.communicate()
    return process.returncode, stderr.decode(errors="replace")


def swap_try(binsrv, shared: Path, outside: Path, variant: str) -> dict:
    """One resumed fetch whose buffer file is swapped; what came of it."""
    machine = harness.FetchCrashMachine()
    machine.start_from_scratch()
    shutil.rmtree(shared, ignore_errors=True)
    shared.mkdir(parents=True)
    shared.chmod(0o777)

    def shared_buffer(config):
        config["storage"]["fs_buffer_directory"] = str(shared)

    # the first fetch stores a binlog; the second one resumes it: it
    # downloads the object into the buffer file and appends to it
    machine.run_workload([{"rows": [300, 20000, 30000], "ddl": True}])
    exit_code, stderr = run_fetch(binsrv, shared_buffer)
    if exit_code != 0:
        raise harness.Failure(f"the first fetch exited with {exit_code}\n"
                              f"{stderr[-500:]}\n{binsrv.log_tail()}")
    names = harness.SERVER.binlog_names()
    last = names[-1]
    stored_before = (binsrv.storage / last).read_bytes()
    if len(names) != 1 or len(stored_before) < 1000:
        raise harness.Failure("the workload did not leave one binlog with "
                              f"content to resume: {names}")
    machine.run_workload([{"rows": [400], "ddl": False}])
    new_size = None
    if variant == "resize_symlink":
        # make the next start resize the object: a 'size' in the binlog's
        # metadata that is smaller than the object (as after a crash
        # between the two uploads), here the start of its last transaction
        new_size = [offset for offset, type_code, _ in
                    harness.parse_events(stored_before)
                    if type_code in harness.TRANSACTION_START_EVENTS][-1]
        metadata = json.loads((binsrv.storage / (last + ".json")).read_text())
        metadata["size"] = new_size
        body = json.dumps(metadata, separators=(",", ":")).encode()
        # through the S3 API, not by writing the server's files
        from s3_upload_tamper_test import signed_request
        status, answer = signed_request(
            binsrv.s3.endpoint, "PUT",
            f"/{binsrv.s3.bucket}/{binsrv.s3.prefix}/{last}.json", body,
            hashlib.sha256(body).hexdigest(), binsrv.s3.access_key,
            binsrv.s3.secret_key)
        if status != 200:
            raise harness.Failure("could not lower 'size' in the binlog "
                                  f"metadata: {status} {answer}")

    # What the other account prepares. It can learn the object's size from
    # the buffer file of an earlier run (PBS creates it readable by anyone,
    # see 's3_buffer' and 'modes'); the test reads it from the bucket.
    prepared = shared / "prepared-by-the-other-account"
    victim = outside / "keyring.victim"
    victim_before = None
    if variant == "regular_file":
        filler = b"NOT WRITTEN BY PBS "
        content = (harness.BINLOG_MAGIC +
                   filler * (len(stored_before) // len(filler) + 1)
                   )[:len(stored_before)]
        prepared.write_bytes(content)
        prepared.chmod(0o666)
    else:
        victim_before = b"KEYRING CANARY: PBS must not touch this file\n" * 40
        if variant != "symlink":
            victim_before = (victim_before *
                             (len(stored_before) // len(victim_before) + 1)
                             )[:len(stored_before)]
        victim.write_bytes(victim_before)
        victim.chmod(0o600)
        prepared.symlink_to(victim)

    swapper = BufferSwapper(shared, prepared)
    swapper.start()
    try:
        exit_code, stderr = run_fetch(binsrv, shared_buffer)
    finally:
        swapper.stop()
    stored_after = (binsrv.storage / last).read_bytes()
    source = harness.SERVER.binlog_as_sent(last)
    outcome = {"swapped": swapper.swapped is not None,
               **({"metadata_size_lowered_to": new_size}
                  if new_size is not None else {}),
               "fetch_exit": exit_code,
               "object_size_before": len(stored_before),
               "object_size_after": len(stored_after),
               # the object is what the source sent, as far as it goes
               "object_is_prefix_of_source":
                   source.startswith(stored_after)}
    if swapper.error:
        outcome["swap_error"] = swapper.error
    if exit_code != 0:
        outcome["fetch_stderr"] = stderr.strip().splitlines()[-1:][0][:300] \
            if stderr.strip() else ""
        outcome["log"] = [line[:300] for line in
                          binsrv.log_tail(3).splitlines()]
    effects = []
    if variant == "regular_file":
        if stored_after[:len(content)] == content and \
                len(stored_after) > len(content):
            effects.append(
                f"the bucket object starts with the {len(content)} bytes of "
                "the other account's file, followed by "
                f"{len(stored_after) - len(content)} bytes of new events")
        elif content[:64] in stored_after:
            effects.append("the bucket object holds bytes of the other "
                           "account's file")
    else:
        victim_after = victim.read_bytes()
        outcome["victim_size_before"] = len(victim_before)
        outcome["victim_size_after"] = len(victim_after)
        outcome["victim_mode_after"] = mode_of(victim)
        if victim_after != victim_before:
            if victim_after.startswith(victim_before):
                how = (f"appended to ({len(victim_after) - len(victim_before)}"
                       " bytes of binlog events)")
            elif victim_before.startswith(victim_after):
                how = (f"truncated from {len(victim_before)} to "
                       f"{len(victim_after)} bytes")
            else:
                how = f"rewritten ({len(victim_after)} bytes)"
            effects.append("the file outside the buffer directory was " + how)
        if victim_before[:64] in stored_after:
            effects.append(
                "the bucket object holds the content of the file outside "
                f"the buffer directory ({len(stored_after)} bytes, starting "
                f"with {stored_after[:24]!r})")
    outcome["effects"] = effects
    outcome["won"] = bool(effects)
    return outcome


def case_s3_buffer_swap(args, workdir: Path) -> dict:
    if args.s3_bucket_dir is None:
        return {"case": "s3_buffer_swap", "ok": True,
                "skipped": "no --s3-bucket-dir"}
    s3 = harness.S3Storage(args.s3_endpoint, args.s3_bucket,
                           args.s3_bucket_dir.expanduser(),
                           args.s3_access_key, args.s3_secret_key,
                           f"file-security-swap-{os.getpid()}")
    binsrv = harness.BinlogServer(harness.BINSRV.binary,
                                  workdir / "binsrv-swap", harness.BINSRV.port,
                                  harness.BINSRV.gtid_mode, s3)
    shared = workdir / "buffer-swap"
    outside = workdir / "outside-the-buffer"
    outside.mkdir(parents=True, exist_ok=True)
    result = {"case": "s3_buffer_swap", "swapped_by": SWAPPED_BY,
              "operations": "listdir, then one rename(2) of a prepared file "
                            "or symlink over the buffer file's name",
              "step": "resumed fetch, do_open_stream: between the creation "
                      "of the buffer file for the download and its re-open "
                      "by path for appending; for 'resize_symlink' "
                      "do_resize_object at start-up: between that creation "
                      "and resize_file() / the re-open by path",
              "tries_per_variant": SWAP_TRIES, "variants": {}}
    problems = []
    saved = harness.BINSRV
    harness.BINSRV = binsrv   # the machine's reset() works on this storage
    try:
        for variant in ("regular_file", "symlink", "symlink_size_matched",
                        "resize_symlink"):
            outcomes = [swap_try(binsrv, shared, outside, variant)
                        for _ in range(SWAP_TRIES)]
            won = [outcome for outcome in outcomes if outcome["won"]]
            exits = sorted({outcome["fetch_exit"] for outcome in outcomes})
            summary = {"swapped": sum(o["swapped"] for o in outcomes),
                       "won": len(won), "fetch_exits": exits,
                       "example": (won or outcomes)[0]}
            if any(code == -signal.SIGABRT for code in exits):
                summary["note"] = (
                    "fetch was aborted (SIGABRT) after the swap: this build "
                    "has assertions, and get_object_into_file asserts that "
                    "the size of the PATH equals the downloaded length")
            result["variants"][variant] = summary
            if won:
                problems.append(
                    f"{variant}: {len(won)} of {SWAP_TRIES} swaps won: " +
                    "; ".join(won[0]["effects"]))
    finally:
        harness.BINSRV = saved
        shutil.rmtree(binsrv.storage, ignore_errors=True)
        shutil.rmtree(shared, ignore_errors=True)
    result["ok"] = not problems
    if problems:
        result["why"] = ("a buffer file swapped by someone who can write "
                         "fs_buffer_directory was used: " +
                         " | ".join(problems))
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
                             "server; without it 's3_buffer' and "
                             "'s3_buffer_swap' are skipped")
    parser.add_argument("--s3-access-key", default="pbsaccesskey")
    parser.add_argument("--s3-secret-key", default="pbs/secret+key=1")
    parser.add_argument("--only", action="append", metavar="CASE",
                        help="run only this case (repeatable; 'modes' for "
                             "both umasks); default: all")
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
            if args.only and "modes" not in args.only:
                continue
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
                                        args.port + 4)),
                ("s3_buffer_swap",
                 lambda: case_s3_buffer_swap(args, args.workdir))):
            if args.only and name not in args.only:
                continue
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
