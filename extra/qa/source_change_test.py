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

"""Checks how 'binlog_server fetch' reacts when the source changes under it.

Four things that happen to a real source, each run against a storage that
already holds the source's binlogs, with the helpers of fetch_crash_test.py:

  reset    - RESET BINARY LOGS AND GTIDS on the source, which restarts its
             binlog sequence at binlog.000001 with new content. The storage
             still holds the old binlog.000001. What does the next fetch do?
  identity - the source is replaced by a different server (a different
             server_uuid, its own binlog.000001) on the same host and port,
             as a failover to a promoted replica would look. Does fetch
             notice it is a different server, or mix the two?
  purge    - the early binlogs are PURGEd on the source before anything is
             fetched, so binlog.000001 is gone. Can fetch start?
  restart  - the source mysqld is restarted while fetch is streaming. fetch
             should reconnect and end with an exact copy.

For each, the report says what fetch did (exit code, any error) and whether
the storage is still a valid copy of the current source. A non-zero exit
means at least one scenario left the storage corrupt or behaved wrongly.
"""

import argparse
import json
import os
import subprocess
import sys
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import fetch_crash_test as harness  # noqa: E402

WORKLOAD = [{"rows": [2000, 60_000], "ddl": False},
            {"rows": [400], "ddl": True}]
BIG_WORKLOAD = [{"rows": [3000, 90_000], "ddl": False},
                {"rows": [300_000], "ddl": False},
                {"rows": [500, 70_000], "ddl": True}]
FETCH_TIMEOUT = 180


def fetch_once(binsrv) -> dict:
    process = binsrv.start_fetch()
    try:
        _, stderr = process.communicate(timeout=FETCH_TIMEOUT)
        tail = stderr.decode(errors="replace")[-500:]
    except subprocess.TimeoutExpired:
        process.kill()
        process.communicate()
        return {"exit": "timeout"}
    return {"exit": process.returncode, "stderr": tail if tail.strip() else ""}


def integrity(machine) -> dict:
    """The storage as a prefix of the current source, and as an exact copy
    after one more clean fetch."""
    report = {}
    try:
        machine.storage_holds_prefixes()
        report["storage_is_prefix_of_source"] = True
    except harness.Failure as failure:
        report["storage_is_prefix_of_source"] = False
        report["mismatch"] = str(failure)[:400]
    try:
        machine.fetch_to_the_end()
        report["reaches_exact_copy"] = True
    except harness.Failure as failure:
        report["reaches_exact_copy"] = False
        report["copy_error"] = str(failure)[:400]
    return report


def new_machine() -> "harness.FetchCrashMachine":
    machine = harness.FetchCrashMachine()
    machine.steps = []
    return machine


def scenario_reset(args) -> dict:
    gtid = args.mode == "gtid"
    source = harness.MySQLServer(args.mysql_basedir, args.workdir / "reset",
                                 args.port, gtid, server_id=1)
    harness.SERVER = source
    harness.BINSRV = binsrv = harness.BinlogServer(
        args.binsrv, args.workdir / "reset-binsrv", args.port, gtid)
    result = {"scenario": "reset"}
    source.start()
    try:
        machine = harness.FetchCrashMachine()
        machine.start_from_scratch()
        machine.run_workload(WORKLOAD)
        machine.fetch_to_the_end()
        result["stored_before"] = binsrv.indexed_binlogs()

        # the source restarts its binlog sequence; binlog.000001 is now a new,
        # unrelated file, while the storage still holds the old one
        source.sql("RESET BINARY LOGS AND GTIDS")
        machine.run_workload(WORKLOAD)
        result["fetch_after_reset"] = fetch_once(binsrv)
        result.update(integrity(machine))
    finally:
        source.stop()
    result["ok"] = result.get("reaches_exact_copy", False)
    return result


def scenario_identity(args) -> dict:
    gtid = args.mode == "gtid"
    first = harness.MySQLServer(args.mysql_basedir, args.workdir / "identity-a",
                                args.port, gtid, server_id=1)
    harness.SERVER = first
    harness.BINSRV = binsrv = harness.BinlogServer(
        args.binsrv, args.workdir / "identity-binsrv", args.port, gtid)
    result = {"scenario": "identity"}
    first.start()
    try:
        machine = harness.FetchCrashMachine()
        machine.start_from_scratch()
        machine.run_workload(WORKLOAD)
        machine.fetch_to_the_end()
        result["first_uuid"] = first.sql("SELECT @@GLOBAL.server_uuid").strip()
    finally:
        first.stop()

    # a different server (its own data directory, so a different server_uuid)
    # takes the same host and port, as a promoted replica would
    second = harness.MySQLServer(args.mysql_basedir, args.workdir / "identity-b",
                                 args.port, gtid, server_id=2)
    harness.SERVER = second
    second.start()
    try:
        second.reset()
        result["second_uuid"] = \
            second.sql("SELECT @@GLOBAL.server_uuid").strip()
        machine = new_machine()
        machine.run_workload(WORKLOAD)
        result["fetch_after_identity_change"] = fetch_once(binsrv)
        result.update(integrity(machine))
    finally:
        second.stop()
    result["ok"] = result.get("reaches_exact_copy", False)
    return result


def scenario_purge(args) -> dict:
    gtid = args.mode == "gtid"
    source = harness.MySQLServer(args.mysql_basedir, args.workdir / "purge",
                                 args.port, gtid, server_id=1)
    harness.SERVER = source
    harness.BINSRV = binsrv = harness.BinlogServer(
        args.binsrv, args.workdir / "purge-binsrv", args.port, gtid)
    result = {"scenario": "purge"}
    source.start()
    try:
        machine = harness.FetchCrashMachine()
        machine.start_from_scratch()
        # a few binlogs, then purge the early ones before any fetch
        for _ in range(3):
            machine.run_workload(WORKLOAD)
            source.sql("FLUSH BINARY LOGS")
        names = source.binlog_names()
        source.sql(f"PURGE BINARY LOGS TO '{names[2]}'")
        result["purged_to"] = names[2]
        result["available_binlogs"] = source.binlog_names()
        result["fetch_after_purge"] = fetch_once(binsrv)
        result["stored"] = binsrv.indexed_binlogs()
        # whatever fetch stored must be a valid suffix/prefix of what the
        # source still has; a clean error is also an acceptable outcome
        if result["fetch_after_purge"].get("exit") == 0:
            result.update(integrity(machine))
    finally:
        source.stop()
    # starting from a purged source should give a clear error or a correct
    # copy of what remains, never a corrupt storage
    outcome = result["fetch_after_purge"].get("exit")
    result["ok"] = outcome in (0, 1)
    return result


def scenario_restart(args) -> dict:
    gtid = args.mode == "gtid"
    source = harness.MySQLServer(args.mysql_basedir, args.workdir / "restart",
                                 args.port, gtid, server_id=1)
    harness.SERVER = source
    harness.BINSRV = binsrv = harness.BinlogServer(
        args.binsrv, args.workdir / "restart-binsrv", args.port, gtid)
    binsrv.idle_time = 2
    result = {"scenario": "restart"}
    source.start()
    try:
        machine = harness.FetchCrashMachine()
        machine.start_from_scratch()
        machine.run_workload(BIG_WORKLOAD)
        binsrv.checkpoint_size = "4K"
        binsrv.write_config()
        process = subprocess.Popen(
            [str(binsrv.binary), "pull", str(binsrv.config_path)],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            env=dict(os.environ, AWS_EC2_METADATA_DISABLED="true"))
        try:
            time.sleep(3)
            result["running_before_restart"] = process.poll() is None
            # restart the source while pull is streaming
            source.stop()
            time.sleep(1)
            source.start()
            machine.run_workload(WORKLOAD)
            # give pull time to reconnect (idle_time) and catch up
            deadline = time.monotonic() + 90
            caught_up = False
            while time.monotonic() < deadline:
                try:
                    machine.check_complete_copy()
                    caught_up = True
                    break
                except (harness.Failure, FileNotFoundError):
                    time.sleep(2)
            result["running_after_restart"] = process.poll() is None
            result["reaches_exact_copy"] = caught_up
            if not caught_up:
                try:
                    machine.check_complete_copy()
                except harness.Failure as failure:
                    result["copy_error"] = str(failure)[:400]
        finally:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=60)
                except subprocess.TimeoutExpired:
                    process.kill()
    finally:
        source.stop()
    result["ok"] = result.get("reaches_exact_copy", False)
    return result


SCENARIOS = {"reset": scenario_reset, "identity": scenario_identity,
             "purge": scenario_purge, "restart": scenario_restart}


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--mysql-basedir", type=Path, required=True)
    parser.add_argument("--binsrv", type=Path, required=True)
    parser.add_argument("--workdir", type=Path, required=True)
    parser.add_argument("--port", type=int, default=23460)
    parser.add_argument("--mode", choices=["gtid", "position"],
                        default="position")
    parser.add_argument("--scenarios", default="reset,identity,purge,restart")
    args = parser.parse_args()
    args.mysql_basedir = args.mysql_basedir.expanduser()
    args.binsrv = args.binsrv.expanduser()
    args.workdir.mkdir(parents=True, exist_ok=True)

    ok = True
    for name in args.scenarios.split(","):
        try:
            result = SCENARIOS[name](args)
        except Exception as error:  # a scenario crashing is itself a result
            result = {"scenario": name, "ok": False,
                      "exception": f"{type(error).__name__}: {error}"[:400]}
        print(json.dumps(result), flush=True)
        ok = ok and result.get("ok", False)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
