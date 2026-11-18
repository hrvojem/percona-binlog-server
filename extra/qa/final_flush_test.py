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
is not recorded in the storage (ticket 73 (PBS-143): the flush runs in the storage
destructor, which drops the error, and the log ends with "successfully
fetched everything"). Each result also shows what the next 'fetch' without a
fault does.

Further kinds of cases look for the same defect elsewhere (is a storage
error dropped, is success reported for something that was not done?). They
run on the file storage with --fsfault-lib and on S3 with --s3-fault-proxy
(their names then end with '/on_s3'); '<target>' is the binlog data or its
metadata, the fault ENOSPC on its write or HTTP 500 on its PUT:

- 'pull_sigterm/<target>', 'pull_sigint/data': 'pull' without checkpointing
  holds received transactions in memory and is stopped by the signal while
  the flush that follows fails;
- 'pull_running/rotation/<target>', 'pull_running/read_timeout/data': the
  flush 'pull' runs at a binlog rotation, or when the idle connection times
  out, fails: 'pull' must stop with an error, not keep running;
- 'checkpoint/<fetch|pull>/<size|interval>/<checkpoint_flush|final_flush>/
  <target>': with 'checkpoint_size' or 'checkpoint_interval', a checkpoint
  flush fails after an earlier one succeeded, or the flush at exit fails
  after all the checkpoints succeeded;
- 'partial/...': one flush fails half way, once: only its second write (the
  data is written, saving the metadata fails), or the second or third write
  of a 'fetch' with checkpoints (fsfault cannot make a write short);
- 'first_fetch/<object>': the first 'fetch' into an empty storage cannot
  create the storage metadata, the first binlog, its metadata or the binlog
  index;
- 'purge/<fault>': 'purge_binlogs' cannot delete a purged binlog's data or
  metadata, or cannot rewrite the binlog index: its exit code and JSON
  status against what was deleted;
- 'logger/<open|write>': the log file ('logger.file') cannot be created, or
  written after its first lines; 'telemetry/write': the telemetry reports
  of 'pull' cannot be written (neither may affect storing);
- 's3_buffer/write' (both options): the local buffer file of the S3 storage
  cannot be written at the flush at exit;
- 'query/<list|search_by_timestamp>/unreadable_binlog_metadata': the
  metadata of one stored binlog cannot be parsed (no fault library needed):
  the query must not answer "success" with that binlog left out.

These pass when the failure is reported (a non-zero exit code; for
'purge_binlogs' also a status other than "success") or the work was done
after all, and fail when success is reported for work that was not done, when
'pull' keeps running after a flush failed, or when the next 'fetch' without
a fault does not end with exact copies. "verdict" is REPORTED, STORED ANYWAY
or SILENT.

A JSON line per case, then a summary; exit 1 if any case failed. --cases
runs only the cases whose names contain one of the given texts. Example:

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
import signal
import subprocess
import sys
import time
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


def stored_state(binsrv, name: "str | None" = None) -> dict:
    if name is None:
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


# ---------------------------------------------------------------------------
# further kinds of cases: 'pull', checkpoints, 'purge_binlogs', the log file,
# telemetry, partial failures, the first 'fetch'
# ---------------------------------------------------------------------------

# 'pull' notices a signal when its read from the source returns, which with
# an idle source takes up to 'connection.read_timeout' (ticket 43, PBS-116)
PULL_READ_TIMEOUT = 8
CHECKPOINT_SIZE = "64K"
SMALL = {"rows": [2000], "ddl": False}
BIG = {"rows": [100_000], "ddl": False}
# with checkpoint_size 64K every BIG transaction is flushed as it ends
# (flushes 1 to 3) and the SMALL one stays in memory for the flush at exit
SIZE_PLAN = [BIG, BIG, BIG, SMALL]
# with checkpoint_interval 1s a transaction is flushed when the first event
# of the next one arrives a second later (flushes 1 to 3; there is no timer),
# and the last one stays in memory for the flush at exit
INTERVAL_PLAN = [SMALL, 1.3, SMALL, 1.3, SMALL, 1.3, SMALL]
# the sample key of the MTR suite, for the listener of 'pull'
AUTH_RSA_PRIVATE_KEY = (Path(__file__).resolve().parents[2] / "mtr" /
                        "binlog_streaming" / "std_data" /
                        "caching_sha2_rsa_priv.pem")
DISK_FAULTS_BY_TARGET = {"data": "binlog data", "metadata": "binlog metadata",
                         "index": "binlog index",
                         "storage_metadata": "storage metadata"}
FIRST_BINLOG = harness.BINLOG_BASE_NAME + ".000001"


class Fault:
    """The fault of one case: fsfault rules for the process to start, or
    rules of the S3 fault proxy."""

    def __init__(self, lab, disk_rules=None, s3_rules=None, root=None):
        self.lab = lab
        self.disk_rules = disk_rules
        self.s3_rules = s3_rules
        self.root = root
        self.before = 0

    def _log_lines(self) -> int:
        log = self.lab.fsfault_log
        return len(log.read_text().splitlines()) if log.exists() else 0

    def describe(self) -> list:
        return self.disk_rules if self.disk_rules is not None else \
            [f"{rule['method']} {rule['match'] or '*'} {rule['action']} "
             f"nth={rule['nth']} count={rule['count']}"
             for rule in self.s3_rules]

    def environment(self) -> dict:
        """Arms the fault; the environment of the process it is for."""
        if self.disk_rules is None:
            self.lab.proxy.set_rules(self.s3_rules)
            return {}
        self.before = self._log_lines()
        return {"LD_PRELOAD": str(harness.FSFAULT_LIB),
                "FSFAULT_ROOT": str(self.root or harness.BINSRV.storage),
                "FSFAULT_RULES": ";".join(self.disk_rules),
                "FSFAULT_LOG": str(self.lab.fsfault_log)}

    def finish(self) -> int:
        """Disarms the fault; how many failures were injected."""
        if self.disk_rules is not None:
            return self._log_lines() - self.before
        applied = sum(rule["applied"]
                      for rule in self.lab.proxy.stats()["rules"])
        self.lab.proxy.set_rules([])
        return applied


class Lab:
    """What the further cases share: the kind of storage and how a fault is
    put on it."""

    def __init__(self, machine, workdir: Path, proxy, pull_port: int):
        self.machine = machine
        self.proxy = proxy
        self.pull_port = pull_port
        self.fsfault_log = workdir / "fsfault.log"

    @property
    def on_s3(self) -> bool:
        return self.proxy is not None

    def name(self, case: str) -> str:
        return case + ("/on_s3" if self.on_s3 else "")

    def fault(self, target: str, flush: int = 1, count: int = 1000) -> Fault:
        """Saving 'target' ("data" / "metadata" of the first binlog, "index",
        "storage_metadata") fails from its 'flush'-th save on, 'count'
        times."""
        if not self.on_s3:
            return Fault(self, disk_rules=[harness.fsfault_rule({
                "op": "write", "target": DISK_FAULTS_BY_TARGET[target],
                "nth": flush, "count": count, "errno": "ENOSPC"})])
        # the proxy matches a text in the path: the binlog's name also
        # matches its metadata, which is PUT after the data in every flush
        match, nth = {
            "data": (FIRST_BINLOG, 2 * flush - 1),
            "metadata": (FIRST_BINLOG + ".json", flush),
            "index": (harness.BINLOG_BASE_NAME + ".index", flush),
            "storage_metadata": ("metadata.json", flush)}[target]
        return self.s3_fault("PUT", match, nth, count)

    def s3_fault(self, method: str, match: str, nth: int = 1,
                 count: int = 1000) -> Fault:
        return Fault(self, s3_rules=[{
            "action": "error500", "method": method, "match": match,
            "nth": nth, "count": count, "seconds": 1}])

    def disk_fault(self, rule: str, root: "Path | None" = None) -> Fault:
        return Fault(self, disk_rules=[rule], root=root)


def start_binsrv(lab, operation: str, *arguments, environment=None,
                 patch=None) -> subprocess.Popen:
    binsrv = harness.BINSRV
    binsrv.write_config()
    config = json.loads(binsrv.config_path.read_text())
    if operation == "pull":
        config["replication_source"]["port"] = lab.pull_port
        config["replication_source"]["auth_rsa_private_key"] = \
            str(AUTH_RSA_PRIVATE_KEY)
    # wait_for_log counts the '[end_of_transaction]' line of every received
    # transaction, a debug message since PBS-118
    config["logger"]["level"] = "debug"
    if patch is not None:
        patch(config)
    binsrv.config_path.write_text(json.dumps(config, indent=2))
    return subprocess.Popen(
        [str(binsrv.binary), operation, str(binsrv.config_path), *arguments],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        env=dict(os.environ, AWS_EC2_METADATA_DISABLED="true",
                 **(environment or {})))


def wait_exit(process, seconds: float) -> tuple:
    """(exit code or None if it is still running, stdout, stderr)."""
    try:
        output, errors = process.communicate(timeout=seconds)
    except subprocess.TimeoutExpired:
        return None, "", ""
    return (process.returncode, output.decode(errors="replace"),
            errors.decode(errors="replace"))


def stop_for_good(process) -> None:
    if process is not None and process.poll() is None:
        process.kill()
    if process is not None:
        process.communicate()


def log_count(text: str) -> int:
    log = harness.BINSRV.log_path
    if not log.exists():
        return 0
    return sum(text in line
               for line in log.read_text(errors="replace").splitlines())


def wait_for_log(text: str, count: int, seconds: float, process) -> bool:
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if log_count(text) >= count:
            return True
        if process.poll() is not None:
            break
        time.sleep(0.1)
    return log_count(text) >= count


def run_plan(lab, plan: list, pauses: bool) -> None:
    for step in plan:
        if isinstance(step, dict):
            lab.machine.run_workload([step])
        elif pauses:
            time.sleep(step)


def prepare_one_binlog(lab) -> None:
    """A storage holding the source's only binlog, which stays the only one
    whatever the size of the transactions that follow."""
    lab.machine.start_from_scratch()
    harness.SERVER.sql("SET GLOBAL max_binlog_size = 1073741824")
    lab.machine.run_workload(WORKLOAD)
    lab.machine.fetch_to_the_end()


def restore_source() -> None:
    harness.BINSRV.read_timeout = 60
    harness.SERVER.sql(
        f"SET GLOBAL max_binlog_size = {harness.MAX_BINLOG_SIZE}")


def judge_run(result: dict, operation: str, code, before: dict, after: dict,
              injected: int) -> None:
    """'code' is the exit code of 'operation', None if it kept running."""
    source_size = len(harness.SERVER.binlog_as_sent(before["binlog"]))
    recorded = after["recorded_size"] == source_size and \
        after["data_bytes"] == source_size
    reported = code not in (0, None)
    result.update({
        f"{operation}_exit": code, "before": before, "after": after,
        "source_bytes": source_size, "log": log_end(harness.BINSRV, 3),
        "failures_injected": injected, "reported": reported,
        "recorded": recorded,
        "verdict": ("REPORTED" if reported else
                    "STORED ANYWAY" if recorded else "SILENT"),
        "ok": reported or recorded})
    if not result["ok"]:
        size = after["recorded_size"] \
            if isinstance(after["recorded_size"], int) else 0
        result["why"] = (
            f"{operation} "
            f"{'kept running' if code is None else 'exited 0'} "
            f"(\"{(result['log'] or ['?'])[-1][-60:]}\") while "
            f"{source_size - size} of the "
            f"{source_size - (before['recorded_size'] or 0)} new bytes are "
            "not recorded in the storage")
    if injected == 0:
        result["ok"] = False
        result["why"] = "the fault was not reached: the case did not run"


def recover(lab, result: dict) -> None:
    """The next 'fetch' without a fault must end with exact copies."""
    process = harness.BINSRV.start_fetch()
    try:
        process.communicate(timeout=harness.FETCH_TIMEOUT_SECONDS)
    except subprocess.TimeoutExpired:
        process.kill()
        process.communicate()
    outcome = {"exit": process.returncode, "exact_copy": True}
    try:
        lab.machine.check_complete_copy()
    except (harness.Failure, OSError) as failure:
        outcome["exact_copy"] = False
        outcome["difference"] = harness.describe_failure(failure, 300)
    result["next_fetch_without_a_fault"] = outcome
    if outcome["exit"] != 0 or not outcome["exact_copy"]:
        result["ok"] = False
        result["why"] = (result.get("why", "") + "; the next fetch without "
                         "a fault does not recover the storage").lstrip("; ")


def data_case(lab, name: str, operation: str, fault: Fault, plan: list,
              checkpoint: tuple = (None, None), stop=None, rotate=False,
              read_timeout: int = PULL_READ_TIMEOUT,
              control: bool = False) -> dict:
    """'operation' receives the transactions of 'plan' on top of a stored
    binlog while 'fault' is armed. 'fetch' runs to its end. 'pull' is
    stopped with the signal 'stop'; without one it must stop by itself when
    the flush fails (at the rotation of 'rotate', at a checkpoint, or when
    its connection times out). 'control': no failure is injected (what the
    case does when nothing is wrong), so the data must be stored and the
    exit code must be 0."""
    result = {"case": lab.name(name), "fault": fault.describe()}
    binsrv = harness.BINSRV
    process = None
    injected = None
    try:
        prepare_one_binlog(lab)
        binsrv.checkpoint_size, binsrv.checkpoint_interval = checkpoint
        binsrv.read_timeout = read_timeout
        before = stored_state(binsrv)
        transactions = sum(isinstance(step, dict) for step in plan)
        if operation == "fetch":
            run_plan(lab, plan, pauses=False)
            process = start_binsrv(lab, "fetch",
                                   environment=fault.environment())
            code, _, _ = wait_exit(process, harness.FETCH_TIMEOUT_SECONDS)
        else:
            process = start_binsrv(lab, "pull",
                                   environment=fault.environment())
            # the checkpoint interval counts from the opening of the binlog
            if not wait_for_log("storage: opened an existing", 1, 60,
                                process):
                raise harness.Failure("pull did not open the stored binlog\n"
                                      + binsrv.log_tail())
            run_plan(lab, plan, pauses=True)
            result["transactions_received"] = wait_for_log(
                "[end_of_transaction]", transactions, 30, process)
            if rotate:
                harness.SERVER.sql("FLUSH BINARY LOGS")
            if stop is None:
                code, _, _ = wait_exit(process, read_timeout + 20)
            else:
                time.sleep(0.5)
                result["running_before_the_signal"] = process.poll() is None
                started = time.monotonic()
                process.send_signal(stop)
                code, _, _ = wait_exit(process, read_timeout + 30)
                result["seconds_to_stop"] = round(
                    time.monotonic() - started, 1)
        if code is None and operation == "pull":
            # what it says when it is told to stop
            process.send_signal(signal.SIGTERM)
            result["exit_after_a_later_sigterm"] = wait_exit(
                process, read_timeout + 30)[0]
        injected = fault.finish()
        stop_for_good(process)
        judge_run(result, operation, code, before,
                  stored_state(binsrv, before["binlog"]),
                  1 if control else injected)
        if control:
            result["failures_injected"] = injected
            result["ok"] = result["recorded"] and code == 0
            if not result["ok"]:
                result["why"] = (f"without a fault {operation} exited with "
                                 f"{code} and the new data is"
                                 f"{'' if result['recorded'] else ' not'} "
                                 "recorded")
        recover(lab, result)
    finally:
        if injected is None:
            fault.finish()
        stop_for_good(process)
        restore_source()
    return result


def first_fetch_case(lab, name: str, fault: Fault) -> dict:
    """The first 'fetch' into an empty storage with a fault on creating one
    of its objects."""
    result = {"case": lab.name(name), "fault": fault.describe()}
    binsrv = harness.BINSRV
    lab.machine.start_from_scratch()
    lab.machine.run_workload(WORKLOAD)
    before = {"binlog": FIRST_BINLOG, "data_bytes": None,
              "recorded_size": None}
    process = start_binsrv(lab, "fetch", environment=fault.environment())
    try:
        code, _, _ = wait_exit(process, harness.FETCH_TIMEOUT_SECONDS)
    finally:
        injected = fault.finish()
        stop_for_good(process)
    judge_run(result, "fetch", code, before,
              stored_state(binsrv, FIRST_BINLOG), injected)
    result["objects_after"] = sorted(
        path.name for path in binsrv.storage.iterdir())
    recover(lab, result)
    return result


def purge_case(lab, name: str, fault: Fault) -> dict:
    """'purge_binlogs' of the first two of three stored binlogs with a fault
    on deleting their objects or on rewriting the binlog index."""
    result = {"case": lab.name(name), "fault": fault.describe()}
    binsrv = harness.BINSRV
    machine = lab.machine
    machine.start_from_scratch()
    for _ in range(2):
        machine.run_workload(WORKLOAD)
        machine.flush_binary_logs()
    machine.run_workload(WORKLOAD)
    machine.fetch_to_the_end()
    names = binsrv.indexed_binlogs()
    victims = names[:2]
    process = start_binsrv(lab, "purge_binlogs", victims[-1],
                           environment=fault.environment())
    try:
        code, output, _ = wait_exit(process, 120)
    finally:
        injected = fault.finish()
        stop_for_good(process)
    try:
        response = json.loads(output)
    except ValueError:
        response = {"unparsable": output[-300:]}
    status = response.get("status")
    objects = sorted(path.name for path in binsrv.storage.iterdir())
    index = binsrv.indexed_binlogs()
    victim_objects = [name for name in objects
                      if name.split(".json")[0] in victims]
    purged = index == names[2:] and not victim_objects
    untouched = index == names and len(victim_objects) == 4
    reported = code != 0 or status != "success"
    result.update({
        "purge_exit": code, "status": status,
        "message": str(response.get("message", ""))[-200:],
        "index_after": index, "victim_objects_left": victim_objects,
        "failures_injected": injected, "reported": reported,
        "purged": purged,
        "verdict": ("REPORTED" if reported else
                    "STORED ANYWAY" if purged else "SILENT"),
        "ok": reported or purged})
    if not result["ok"]:
        result["why"] = (f"purge_binlogs exited 0 with status \"{status}\" "
                         f"while {victim_objects} are left and binlog.index "
                         f"lists {index}")
    elif code != 0 and not untouched:
        # an error must mean that nothing was purged
        result["ok"] = False
        result["why"] = (f"purge_binlogs failed (exit {code}) but changed "
                         f"the storage: binlog.index lists {index}, "
                         f"{victim_objects} are left")
    if injected == 0:
        result["ok"] = False
        result["why"] = "the fault was not reached: the case did not run"
    # the next start removes what an interrupted purge left
    process = binsrv.start_fetch()
    try:
        process.communicate(timeout=harness.FETCH_TIMEOUT_SECONDS)
    except subprocess.TimeoutExpired:
        process.kill()
        process.communicate()
    index = binsrv.indexed_binlogs()
    outcome = {
        "exit": process.returncode, "index": index,
        "objects": sorted(path.name for path in binsrv.storage.iterdir()),
        "exact_copy": all(
            (binsrv.storage / name).exists() and
            (binsrv.storage / name).read_bytes() ==
            harness.SERVER.binlog_as_sent(name) for name in index)}
    result["next_fetch_without_a_fault"] = outcome
    if outcome["exit"] != 0 or not outcome["exact_copy"]:
        result["ok"] = False
        result["why"] = (result.get("why", "") + "; the next fetch without "
                         "a fault does not recover the storage").lstrip("; ")
    return result


def logger_case(lab, name: str, rule: str) -> dict:
    """'fetch' whose log file cannot be created or written: nothing is wrong
    with the storage, so the data must be stored (or the failure reported)."""
    result = {"case": lab.name(name)}
    binsrv = harness.BINSRV
    prepare(lab.machine)
    before = stored_state(binsrv)
    binsrv.log_path.unlink()
    fault = lab.disk_fault(f"{rule},match={binsrv.log_path.name}",
                           root=binsrv.workdir)
    result["fault"] = fault.describe()
    process = start_binsrv(lab, "fetch", environment=fault.environment())
    try:
        code, output, errors = wait_exit(process,
                                         harness.FETCH_TIMEOUT_SECONDS)
    finally:
        injected = fault.finish()
        stop_for_good(process)
    judge_run(result, "fetch", code, before, stored_state(binsrv), injected)
    log = binsrv.log_path
    result["log_lines"] = len(log.read_text(errors="replace").splitlines()) \
        if log.exists() else None
    # what the console got (the log goes there until the file is opened)
    result["console"] = (output + errors).strip()[-200:]
    recover(lab, result)
    return result


def telemetry_case(lab, name: str) -> dict:
    """'pull' whose telemetry reports cannot be written must keep storing."""
    result = {"case": lab.name(name)}
    binsrv = harness.BINSRV
    process = None
    injected = None
    fault = lab.disk_fault("op=write,nth=1,count=100000,errno=ENOSPC",
                           root=binsrv.telemetry)
    result["fault"] = fault.describe()

    def telemetry_on(config):
        config["telemetry"].update({"grace_interval": "20s",
                                    "scrape_interval": "10s"})

    try:
        prepare_one_binlog(lab)
        binsrv.read_timeout = PULL_READ_TIMEOUT
        process = start_binsrv(lab, "pull", environment=fault.environment(),
                               patch=telemetry_on)
        result["report_failure_logged"] = wait_for_log(
            "telemetry: report write failed", 1, 60, process)
        lab.machine.run_workload(WORKLOAD)
        harness.SERVER.sql("FLUSH BINARY LOGS")
        source_size = len(harness.SERVER.binlog_as_sent(FIRST_BINLOG))
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline and stored_state(
                binsrv, FIRST_BINLOG)["recorded_size"] != source_size:
            time.sleep(0.2)
        after = stored_state(binsrv, FIRST_BINLOG)
        running = process.poll() is None
        process.send_signal(signal.SIGTERM)
        code, _, _ = wait_exit(process, PULL_READ_TIMEOUT + 60)
        injected = fault.finish()
        recorded = after["recorded_size"] == source_size and \
            after["data_bytes"] == source_size
        result.update({
            "pull_running_after_the_failure": running, "pull_exit": code,
            "after": after, "source_bytes": source_size,
            "failures_injected": injected, "recorded": recorded,
            "log": log_end(binsrv, 3),
            "verdict": "STORED ANYWAY" if recorded else
            "REPORTED" if code not in (0, None) else "SILENT",
            "ok": recorded and running and code == 0})
        if not result["ok"]:
            result["why"] = ("pull did not keep storing after a telemetry "
                             "report could not be written")
        if injected == 0 or not result["report_failure_logged"]:
            result["ok"] = False
            result["why"] = "the fault was not reached: the case did not run"
        stop_for_good(process)
        recover(lab, result)
    finally:
        if injected is None:
            fault.finish()
        stop_for_good(process)
        restore_source()
    return result


def query_case(lab, name: str, operation: str, *arguments) -> dict:
    """A query on a storage of three binlogs where the metadata of the
    second one cannot be parsed: the answer must not be a "success" that
    leaves the binlog out."""
    result = {"case": lab.name(name)}
    binsrv = harness.BINSRV
    machine = lab.machine
    machine.start_from_scratch()
    for _ in range(2):
        machine.run_workload(WORKLOAD)
        machine.flush_binary_logs()
    machine.run_workload(WORKLOAD)
    machine.fetch_to_the_end()
    names = binsrv.indexed_binlogs()
    metadata = binsrv.storage / (names[1] + ".json")
    intact = metadata.read_bytes()
    metadata.write_bytes(intact[:20])
    try:
        code, output, _ = wait_exit(
            start_binsrv(lab, operation, *arguments), 120)
    finally:
        metadata.write_bytes(intact)
    try:
        response = json.loads(output)
    except ValueError:
        response = {"unparsable": output[-300:]}
    status = response.get("status")
    listed = [entry.get("name") for entry in response.get("result", [])
              if isinstance(entry, dict)] \
        if isinstance(response.get("result"), list) else []
    reported = code != 0 or status != "success"
    result.update({
        f"{operation}_exit": code, "status": status,
        "message": str(response.get("message", ""))[-200:],
        "stored": names, "listed": listed, "reported": reported,
        "verdict": ("REPORTED" if reported else
                    "STORED ANYWAY" if listed == names else "SILENT"),
        "ok": reported or listed == names})
    if not result["ok"]:
        result["why"] = (f"{operation} exited 0 with status \"{status}\" "
                         f"and lists {listed} of {names}: the binlog whose "
                         "metadata cannot be read is left out without a "
                         "word")
    recover(lab, result)
    return result


def further_cases(lab, with_fsfault: bool) -> list:
    """(name, function) of the cases for the storage of 'lab'."""
    size = (CHECKPOINT_SIZE, None)
    interval = (None, "1s")
    cases = []

    def add(name, function, *arguments, **options):
        cases.append((lab.name(name), lambda: function(
            lab, name, *arguments, **options)))

    for target in ("data", "metadata"):
        add(f"pull_sigterm/{target}", data_case, "pull", lab.fault(target),
            [SMALL], stop=signal.SIGTERM)
    # never reached: 'pull' stores what it holds when it is told to stop
    never = lab.s3_fault("PUT", "no-such-object") if lab.on_s3 else \
        lab.disk_fault("op=write,match=no-such-file,errno=EIO")
    add("pull_sigterm/no_fault", data_case, "pull", never, [SMALL],
        stop=signal.SIGTERM, control=True)
    add("pull_sigint/data", data_case, "pull", lab.fault("data"), [SMALL],
        stop=signal.SIGINT)
    for target in ("data", "metadata"):
        add(f"pull_running/rotation/{target}", data_case, "pull",
            lab.fault(target), [SMALL], rotate=True)
    add("pull_running/read_timeout/data", data_case, "pull",
        lab.fault("data"), [SMALL], read_timeout=4)
    for target in ("data", "metadata"):
        add(f"checkpoint/fetch/size/checkpoint_flush/{target}", data_case,
            "fetch", lab.fault(target, flush=2), SIZE_PLAN, checkpoint=size)
        add(f"checkpoint/fetch/size/final_flush/{target}", data_case,
            "fetch", lab.fault(target, flush=4), SIZE_PLAN, checkpoint=size)
    add("checkpoint/fetch/interval/final_flush/data", data_case, "fetch",
        lab.fault("data"), [SMALL], checkpoint=interval)
    add("checkpoint/pull/size/checkpoint_flush/data", data_case, "pull",
        lab.fault("data", flush=2), SIZE_PLAN, checkpoint=size,
        read_timeout=60)
    add("checkpoint/pull/size/final_flush/data", data_case, "pull",
        lab.fault("data", flush=4), SIZE_PLAN, checkpoint=size,
        stop=signal.SIGTERM)
    add("checkpoint/pull/interval/checkpoint_flush/data", data_case, "pull",
        lab.fault("data", flush=2), INTERVAL_PLAN, checkpoint=interval,
        read_timeout=60)
    add("checkpoint/pull/interval/final_flush/data", data_case, "pull",
        lab.fault("data", flush=4), INTERVAL_PLAN, checkpoint=interval,
        stop=signal.SIGTERM)

    def once(nth: int) -> Fault:
        """Only the nth write to the storage (PUT on S3) fails, once."""
        if lab.on_s3:
            return lab.s3_fault("PUT", "", nth, 1)
        return lab.disk_fault(f"op=write,nth={nth},count=1,errno=ENOSPC")

    add("partial/fetch/final_flush/write_2", data_case, "fetch", once(2),
        [SMALL])
    add("partial/fetch/checkpoint/write_2", data_case, "fetch", once(2),
        SIZE_PLAN, checkpoint=size)
    add("partial/fetch/checkpoint/write_3", data_case, "fetch", once(3),
        SIZE_PLAN, checkpoint=size)

    add("first_fetch/storage_metadata", first_fetch_case,
        lab.fault("storage_metadata"))
    add("first_fetch/binlog_data", first_fetch_case, lab.fault("data"))
    add("first_fetch/binlog_metadata", first_fetch_case,
        lab.fault("metadata"))
    add("first_fetch/binlog_index", first_fetch_case, lab.fault("index"))

    add("query/list/unreadable_binlog_metadata", query_case, "list")
    add("query/search_by_timestamp/unreadable_binlog_metadata", query_case,
        "search_by_timestamp", "2037-01-01T00:00:00")

    if lab.on_s3:
        add("purge/data_and_metadata_delete", purge_case,
            lab.s3_fault("DELETE", FIRST_BINLOG))
        add("purge/metadata_delete", purge_case,
            lab.s3_fault("DELETE", FIRST_BINLOG + ".json"))
        add("purge/index_put", purge_case, lab.fault("index"))
        if with_fsfault:
            add("s3_buffer/write", data_case, "fetch", lab.disk_fault(
                "op=write,nth=2,count=1000,errno=ENOSPC",
                root=harness.BINSRV.workdir / "s3-buffer"), [SMALL])
    else:
        add("purge/data_unlink", purge_case, lab.disk_fault(
            f"op=unlink,match={FIRST_BINLOG},exclude=.json,count=1000,"
            "errno=EIO"))
        add("purge/metadata_unlink", purge_case, lab.disk_fault(
            f"op=unlink,match={FIRST_BINLOG}.json,count=1000,errno=EIO"))
        add("purge/index_write", purge_case, lab.fault("index"))
        add("purge/index_rename", purge_case, lab.disk_fault(
            "op=rename,match=binlog.index,count=1000,errno=EIO"))
        add("logger/open", logger_case,
            "op=open,nth=1,count=1000,errno=ENOSPC")
        add("logger/write", logger_case,
            "op=write,nth=12,count=100000,errno=ENOSPC")
        add("telemetry/write", telemetry_case)
    return cases


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
    parser.add_argument("--pull-port", type=int,
                        help="the listener of 'pull' (default: --port + 2)")
    parser.add_argument("--cases", default="",
                        help="run only the cases whose names contain one "
                             "of these comma-separated texts")
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
        proxy = None
        if args.fsfault_lib is not None:
            harness.FSFAULT_LIB = args.fsfault_lib.expanduser().resolve()
        if s3 is not None:
            proxy = harness.FaultProxy(
                f"http://{args.s3_fault_proxy}/__faults__")
            cases = [(f"s3/{match}/{action}",
                      lambda m=match, a=action: s3_case(machine, proxy, m, a))
                     for match, action in S3_FAULTS]
        elif args.fsfault_lib is not None:
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
        if s3 is not None or args.fsfault_lib is not None:
            lab = Lab(machine, args.workdir, proxy,
                      args.pull_port or args.port + 2)
            cases += further_cases(lab, args.fsfault_lib is not None)
        wanted = [text for text in args.cases.split(",") if text]
        if wanted:
            cases = [(name, case) for name, case in cases
                     if any(text in name.replace(" ", "_")
                            for text in wanted)]
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
        if proxy is not None:
            proxy.set_rules([])
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
