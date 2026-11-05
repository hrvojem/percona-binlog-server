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

"""A MySQL replica of 'binlog_server pull' across restarts of 'pull'.

The replica connects straight to the listener of 'pull' (auto-positioning
in GTID mode, binlog name and position in position mode) while the source
keeps writing, and 'pull' is stopped and started again:

  term_idle                    SIGTERM while the replica is caught up, rows
                               written while 'pull' is down;
  kill_idle                    the same with SIGKILL;
  kill_while_writing           SIGKILL during a stream of rows and binlog
                               rotations, restarted at once;
  kill_mid_big_event           SIGKILL while an 8 MB row is being received
                               (as soon as part of it is stored; the case
                               fails if the kill came outside the event);
  down_with_rotations          SIGKILL, then on the source FLUSH BINARY LOGS
                               three times, rows, and two more rotations
                               (binlogs without transactions);
  kill_during_replica_catchup  the replica's I/O thread is stopped while
                               100 rows are written, then started, and
                               'pull' is killed while it catches up;
  flapping                     five SIGKILL / restart cycles a second apart
                               while the source writes.

After each case the replica must reconnect to the new 'pull' on its own
(SOURCE_CONNECT_RETRY) and end with the source's data (row count and
checksum; in GTID mode the source's GTID_EXECUTED a subset of its own) with
no applier error and its I/O thread running; its error log must show that
it lost the connection; the restarted 'pull' must be running, and a 'pull'
stopped with SIGTERM must have exited in time. A transaction applied twice
or skipped shows up as an applier error or a different checksum. One JSON
line per case.

Uses the helpers of fetch_crash_test.py, pull_replica_test.py and
pull_replica_fault_test.py. Example:

  PYTHONPATH=~/ws/qa/pylib python3 pull_restart_test.py \\
    --mysql-basedir ~/ws/dist/mysql-8.4.8-linux-glibc2.28-x86_64-minimal \\
    --binsrv ~/ws/percona-binlog-server-build-debug_gcc14/binlog_server \\
    --workdir ~/ws/qa/pull-restart --mode gtid
"""

import argparse
import json
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import fetch_crash_test as harness  # noqa: E402
import pull_replica_fault_test as faults  # noqa: E402
import pull_replica_test as replication  # noqa: E402

REPLICA_NET_TIMEOUT = 8
TERM_SECONDS = 60
CASES = ["term_idle", "kill_idle", "kill_while_writing", "kill_mid_big_event",
         "down_with_rotations", "kill_during_replica_catchup", "flapping"]


class Pull:
    """The running 'pull' process, restarted by the cases."""

    def __init__(self, binsrv, port: int):
        self.binsrv = binsrv
        self.port = port
        self.process = replication.start_pull(binsrv, port)
        self.term_exit = None

    def alive(self) -> bool:
        return self.process.poll() is None

    def stop(self, signal_number: int) -> None:
        self.process.send_signal(signal_number)
        try:
            code = self.process.wait(timeout=TERM_SECONDS)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.wait()
            code = "timeout"
        if signal_number == signal.SIGTERM:
            self.term_exit = code

    def restart(self) -> None:
        if self.alive():
            self.stop(signal.SIGKILL)
        self.process = replication.start_pull(self.binsrv, self.port)


class Writer:
    """Runs 'function' in a thread and keeps its exception."""

    def __init__(self, function):
        self.error = None

        def run():
            try:
                function()
            except Exception as error:  # reported by the case
                self.error = error

        self.thread = threading.Thread(target=run)
        self.thread.start()

    def join(self) -> None:
        self.thread.join()
        if self.error is not None:
            raise self.error


def run_case(name, source, replica, binsrv, pull: Pull) -> dict:
    """Runs case 'name'; returns what it measured."""
    pull.term_exit = None
    if name == "term_idle":
        pull.stop(signal.SIGTERM)
        faults.write_rows(source, 30, False)
        pull.restart()
    elif name == "kill_idle":
        pull.stop(signal.SIGKILL)
        faults.write_rows(source, 30, False)
        pull.restart()
    elif name == "kill_while_writing":
        writer = Writer(lambda: faults.write_rows(source, 80, False))
        time.sleep(1.5)
        pull.restart()
        writer.join()
    elif name == "kill_mid_big_event":
        before = binsrv.stored_data_size()
        writer = Writer(lambda: faults.write_rows(source, 0, True))
        deadline = time.monotonic() + 60
        # the storage grows in large steps, so the kill comes at the first
        while binsrv.stored_data_size() == before and \
                time.monotonic() < deadline:
            time.sleep(0.002)
        stored = binsrv.stored_data_size() - before
        pull.restart()
        writer.join()
        # the kill came inside the row event only if part of it was stored
        return {"stored_at_kill": stored,
                "inside_event": 0 < stored < faults.BIG_ROW_BYTES}
    elif name == "down_with_rotations":
        pull.stop(signal.SIGKILL)
        source.sql("FLUSH BINARY LOGS; FLUSH BINARY LOGS; FLUSH BINARY LOGS;")
        faults.write_rows(source, 10, False)
        source.sql("FLUSH BINARY LOGS; FLUSH BINARY LOGS;")
        pull.restart()
    elif name == "kill_during_replica_catchup":
        replica.sql("STOP REPLICA IO_THREAD")
        faults.write_rows(source, 100, False)
        replica.sql("START REPLICA IO_THREAD")
        time.sleep(1)
        pull.restart()
    elif name == "flapping":
        writer = Writer(lambda: faults.write_rows(source, 100, False))
        for _ in range(5):
            time.sleep(1)
            pull.process.send_signal(signal.SIGKILL)
            pull.process.wait()
            pull.restart()
        writer.join()
    else:
        raise ValueError(f"unknown case {name}")
    return {}


def run(args) -> list:
    work = args.workdir
    gtid_mode = args.mode == "gtid"
    source = harness.MySQLServer(args.mysql_basedir, work / "source",
                                 args.port, gtid_mode, server_id=1)
    replica = harness.MySQLServer(args.mysql_basedir, work / "replica",
                                  args.port + 10, gtid_mode, server_id=10)
    binsrv = harness.BinlogServer(args.binsrv, work / "binsrv", args.port,
                                  gtid_mode)
    binsrv.reset()
    binsrv.checkpoint_size = "1"
    pull_port = args.port + 2
    start = ("SOURCE_AUTO_POSITION=1" if gtid_mode else
             "SOURCE_LOG_FILE='binlog.000001', SOURCE_LOG_POS=4")
    start += (f", SOURCE_HEARTBEAT_PERIOD={faults.HEARTBEAT_PERIOD}, "
              "SOURCE_RETRY_COUNT=100000")
    results = []
    pull = None
    source.start()
    replica.start()
    try:
        source.reset()
        replica.sql(f"SET GLOBAL replica_net_timeout = {REPLICA_NET_TIMEOUT}")
        pull = Pull(binsrv, pull_port)
        faults.write_rows(source, 40, False)
        replication.replicate_from_pull(replica, pull_port, gtid_mode, start)
        first = faults.catch_up(source, replica, gtid_mode)
        results.append({"case": "baseline", "catch_up_seconds":
                        None if first is None else round(first, 1),
                        **faults.replica_state(replica),
                        "ok": first is not None})
        print(json.dumps(results[-1]), flush=True)
        wanted = args.cases.split(",") if args.cases else CASES
        for name in wanted:
            result = {"case": name}
            try:
                before = faults.disconnects(replica)
                result.update(run_case(name, source, replica, binsrv, pull))
                took = faults.catch_up(source, replica, gtid_mode)
                state = faults.replica_state(replica)
                state["disconnects"] -= before
                result.update({
                    "catch_up_seconds": None if took is None else
                    round(took, 1),
                    **state, "pull_alive": pull.alive()})
                if pull.term_exit is not None:
                    result["term_exit"] = pull.term_exit
                result["ok"] = (took is not None and
                                state["disconnects"] > 0 and
                                state["io_state"] == "ON" and
                                not state["applier_errors"] and
                                pull.alive() and
                                pull.term_exit in (None, 0) and
                                result.get("inside_event", True))
            except Exception as error:  # reported as the case's failure
                result.update({"error": harness.describe_failure(error, 600),
                               "ok": False})
            if not result["ok"]:
                result["pull_log"] = binsrv.log_tail(8)[-1500:]
            results.append(result)
            print(json.dumps(result), flush=True)
            if not pull.alive():
                pull.restart()
    finally:
        if pull is not None and pull.alive():
            pull.process.terminate()
            pull.process.wait(timeout=120)
        source.stop()
        replica.stop()
    return results


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--mysql-basedir", type=Path, required=True)
    parser.add_argument("--binsrv", type=Path, required=True)
    parser.add_argument("--workdir", type=Path, required=True)
    parser.add_argument("--port", type=int, default=24070)
    parser.add_argument("--mode", choices=["position", "gtid"],
                        default="gtid")
    parser.add_argument("--cases", default="",
                        help="comma-separated case names (default: all)")
    args = parser.parse_args()
    args.mysql_basedir = args.mysql_basedir.expanduser()
    args.binsrv = args.binsrv.expanduser()
    args.workdir = args.workdir.expanduser().resolve()
    args.workdir.mkdir(parents=True, exist_ok=True)
    results = run(args)
    (args.workdir / "results.json").write_text(json.dumps(results, indent=2))
    return 0 if all(result.get("ok") for result in results) else 1


if __name__ == "__main__":
    sys.exit(main())
