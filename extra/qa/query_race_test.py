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

"""Runs 'binlog_server list' over and over next to a writing 'pull'.

A source MySQL server gets a steady stream of small inserts with a
'FLUSH BINARY LOGS' every few of them, 'binlog_server pull' streams them into
a filesystem storage with checkpoint_size "1K" (so it saves its metadata and
renames its temporary files all the time), and 'binlog_server list' runs
--runs times against the same storage. Every 'list' run must exit with 0 and
print a "success" response. Ticket 29: a query next to a writer fails
stat()ing a '.tmp' file the writer renamed away, so a few percent of the
runs fail today.

Prints one JSON line (the runs, the failures and each distinct error message,
with runs of 3 or more digits replaced by "N" so that file names do not split
them, and how often it occurred) and exits with 1 when any 'list' run failed
or 'pull' died.

Uses the helpers of fetch_crash_test.py. Example:

  PYTHONPATH=~/ws/qa/pylib python3 query_race_test.py \\
    --mysql-basedir ~/ws/dist/mysql-8.4.8-linux-glibc2.28-x86_64-minimal \\
    --binsrv ~/ws/percona-binlog-server-build-debug_gcc14/binlog_server \\
    --workdir ~/ws/qa/qrace
"""

import argparse
import json
import os
import re
import subprocess
import sys
import threading
import time
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import fetch_crash_test as harness  # noqa: E402

# a rotation every this many inserts, as in the original shell reproducer
INSERTS_PER_ROTATION = 20
# between two statements written to the load session
INSERT_PAUSE_SECONDS = 0.002
WARM_UP_SECONDS = 3
LIST_TIMEOUT_SECONDS = 60


def load(server, stop: threading.Event, counters: dict):
    """Small inserts and a rotation every INSERTS_PER_ROTATION of them,
    through one client session (a client per statement would only manage a
    few dozen inserts a second), until stopped."""
    client = subprocess.Popen(
        [str(server.basedir / "bin" / "mysql"), "--no-defaults",
         f"--socket={server.socket}", "-uroot", "--force"],
        stdin=subprocess.PIPE, stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL, text=True)
    try:
        while not stop.is_set() and client.poll() is None:
            client.stdin.write(
                "INSERT INTO qa.t (payload) VALUES (REPEAT('a', 150));\n")
            counters["inserts"] += 1
            if counters["inserts"] % INSERTS_PER_ROTATION == 0:
                client.stdin.write("FLUSH BINARY LOGS;\n")
                counters["rotations"] += 1
            client.stdin.flush()
            time.sleep(INSERT_PAUSE_SECONDS)
    except (BrokenPipeError, OSError):
        counters["load_errors"] += 1
    finally:
        try:
            client.stdin.close()
        except OSError:
            pass
        try:
            client.wait(timeout=30)
        except subprocess.TimeoutExpired:
            client.kill()
            client.wait()


def list_once(binsrv, environment) -> "str | None":
    """None when 'list' succeeded, the error otherwise."""
    try:
        run = subprocess.run(
            [str(binsrv.binary), "list", str(binsrv.config_path)],
            capture_output=True, text=True, timeout=LIST_TIMEOUT_SECONDS,
            env=environment)
    except subprocess.TimeoutExpired:
        return f"timed out after {LIST_TIMEOUT_SECONDS} s"
    try:
        response = json.loads(run.stdout)
    except ValueError:
        response = None
    if run.returncode == 0 and isinstance(response, dict) and \
            response.get("status") == "success":
        return None
    if isinstance(response, dict) and response.get("message"):
        return str(response["message"])
    output = (run.stdout + run.stderr).strip().splitlines()
    return (f"exit code {run.returncode}: "
            f"{output[-1][:200] if output else '<no output>'}")


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--mysql-basedir", type=Path, required=True)
    parser.add_argument("--binsrv", type=Path, required=True)
    parser.add_argument("--workdir", type=Path, required=True,
                        help="keep it short: the mysqld socket lives in it")
    parser.add_argument("--port", type=int, default=23480,
                        help="the MySQL port; 'pull' listens on the next one")
    parser.add_argument("--runs", type=int, default=500,
                        help="how many times to run 'list'")
    parser.add_argument("--mode", choices=["gtid", "position"],
                        default="position")
    args = parser.parse_args()
    args.mysql_basedir = args.mysql_basedir.expanduser()
    args.binsrv = args.binsrv.expanduser()
    args.workdir = args.workdir.expanduser()
    args.workdir.mkdir(parents=True, exist_ok=True)

    gtid_mode = args.mode == "gtid"
    server = harness.MySQLServer(args.mysql_basedir, args.workdir / "source",
                                 args.port, gtid_mode)
    binsrv = harness.BinlogServer(args.binsrv, args.workdir / "binsrv",
                                  args.port, gtid_mode)
    environment = dict(os.environ, AWS_EC2_METADATA_DISABLED="true")
    result = {"test": "query_race", "mode": args.mode, "runs": args.runs}
    errors = Counter()
    samples = {}
    counters = Counter()
    stop = threading.Event()
    loader = None
    pull = None
    server.start()
    try:
        server.reset()
        binsrv.reset()
        binsrv.checkpoint_size = "1K"
        binsrv.write_config()
        config = json.loads(binsrv.config_path.read_text())
        # 'pull' must listen somewhere free; nothing connects to it here
        config["replication_source"]["port"] = args.port + 1
        binsrv.config_path.write_text(json.dumps(config, indent=2))
        pull = subprocess.Popen(
            [str(binsrv.binary), "pull", str(binsrv.config_path)],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            env=environment)
        loader = threading.Thread(target=load, args=(server, stop, counters),
                                  daemon=True)
        loader.start()
        time.sleep(WARM_UP_SECONDS)
        if pull.poll() is not None:
            raise RuntimeError(f"'pull' exited with {pull.returncode}, "
                               f"see {binsrv.log_path}")

        started = time.monotonic()
        for _ in range(args.runs):
            error = list_once(binsrv, environment)
            if error is not None:
                key = re.sub(r"\d{3,}", "N", error)
                errors[key] += 1
                samples.setdefault(key, error)
        result["seconds"] = round(time.monotonic() - started, 1)
        result["pull_alive"] = pull.poll() is None
    finally:
        stop.set()
        if loader is not None:
            loader.join(timeout=30)
        if pull is not None and pull.poll() is None:
            pull.terminate()
            try:
                pull.wait(timeout=10)
            except subprocess.TimeoutExpired:
                pull.kill()
                pull.wait()
        server.stop()

    result["failed"] = sum(errors.values())
    result["errors"] = [{"message": key, "count": count,
                         "example": samples[key]}
                        for key, count in errors.most_common()]
    result["inserts"] = counters["inserts"]
    result["rotations"] = counters["rotations"]
    result["load_errors"] = counters["load_errors"]
    result["binlogs_stored"] = len(binsrv.indexed_binlogs())
    result["pull_exit_code"] = pull.returncode if pull is not None else None
    result["passed"] = result["failed"] == 0 and result["pull_alive"]
    print(json.dumps(result), flush=True)
    (args.workdir / "result.json").write_text(json.dumps(result, indent=2))
    return 0 if result["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
