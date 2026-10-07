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

"""Runs 'binlog_server fetch' over binlogs made of less common event types.

The crash test (fetch_crash_test.py) only writes plain row-based
transactions. A source produces several other kinds of event that the
collector has to place into transactions and checkpoint at the right
boundaries. This test drives each kind, then checks the storage with the
helpers of fetch_crash_test.py:

  compression      - binlog_transaction_compression=ON (TRANSACTION_PAYLOAD
                     events that wrap a whole transaction);
  minimal_row_image- binlog_row_image=MINIMAL, with UPDATE and DELETE;
  partial_json     - binlog_row_value_options=PARTIAL_JSON (PARTIAL_UPDATE_
                     ROWS events from JSON_SET);
  xa               - XA transactions (XA PREPARE / XA COMMIT, each its own
                     transaction with an XA_PREPARE_LOG event);
  tagged_gtid      - tagged GTIDs (GTID_TAGGED_LOG events, MySQL 8.4+).

For each kind: a full fetch must produce a byte-identical copy of the
source's binlogs (check_complete_copy); then 'fetch' is killed mid-stream
and resumed, and the storage must stay a prefix ending at a transaction
boundary and heal to an exact copy - this is what exercises the collector's
boundary detection for the event type. Runs in GTID mode, where a
transaction always starts with a GTID event.

A non-zero exit means one kind of event was stored wrong. Example:

  PYTHONPATH=~/ws/qa/pylib python3 event_variety_test.py \\
    --mysql-basedir ~/ws/dist/mysql-8.4.8-linux-glibc2.28-x86_64-minimal \\
    --binsrv ~/ws/percona-binlog-server-build-asan_gcc14/binlog_server \\
    --workdir ~/ws/qa/variety
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

# a payload big enough that a handful of transactions cross the 64 KiB
# max-binlog-size, so there are several binlogs and a real resume
BLOB = "REPEAT('abcdef1234567890', 3000)"  # ~48 KiB, compresses well


def plain(count: int) -> list:
    return [f"BEGIN; INSERT INTO qa.t (payload) VALUES ({BLOB}); COMMIT;"
            for _ in range(count)]


def compression(count: int) -> list:
    return ["SET SESSION binlog_transaction_compression = ON;"] + plain(count)


def minimal_row_image(count: int) -> list:
    statements = ["SET SESSION binlog_row_image = MINIMAL;"]
    for index in range(count):
        statements.append(
            f"BEGIN; INSERT INTO qa.t (payload) VALUES ({BLOB}); "
            f"UPDATE qa.t SET payload = {BLOB} WHERE id = LAST_INSERT_ID(); "
            f"COMMIT;")
        if index % 3 == 2:
            statements.append("DELETE FROM qa.t ORDER BY id LIMIT 1;")
    return statements


def partial_json(count: int) -> list:
    statements = [
        "CREATE TABLE IF NOT EXISTS qa.j (id INT UNSIGNED NOT NULL "
        "AUTO_INCREMENT PRIMARY KEY, doc JSON) ENGINE=InnoDB;",
        "SET SESSION binlog_row_value_options = PARTIAL_JSON;"]
    for _ in range(count):
        statements.append(
            "BEGIN; INSERT INTO qa.j (doc) VALUES "
            "(JSON_OBJECT('a', REPEAT('x', 2000), 'b', 1)); "
            "UPDATE qa.j SET doc = JSON_SET(doc, '$.b', "
            "REPEAT('y', 2000)) WHERE id = LAST_INSERT_ID(); COMMIT;")
    return statements


def xa(count: int) -> list:
    statements = []
    for index in range(count):
        xid = f"'variety_xa_{index}'"
        statements.append(
            f"XA START {xid}; INSERT INTO qa.t (payload) VALUES ({BLOB}); "
            f"XA END {xid}; XA PREPARE {xid}; XA COMMIT {xid};")
    return statements


def tagged_gtid(count: int) -> list:
    # tagged GTIDs: the server assigns an automatic GNO under the given tag
    statements = []
    for index in range(count):
        tag = "varietytag" if index % 2 == 0 else "othertag"
        statements.append(
            f"SET GTID_NEXT = 'AUTOMATIC:{tag}'; "
            f"BEGIN; INSERT INTO qa.t (payload) VALUES ({BLOB}); COMMIT; "
            f"SET GTID_NEXT = 'AUTOMATIC';")
    return statements


VARIETIES = {"compression": compression,
             "minimal_row_image": minimal_row_image,
             "partial_json": partial_json,
             "xa": xa,
             "tagged_gtid": tagged_gtid}


def run_sql(source, statements: list):
    for statement in statements:
        source.sql(statement)


def fetch_and_kill(binsrv, source, statements: list, kill_after_bytes: int):
    """Starts fetch, feeds the workload, and SIGKILLs fetch once the storage
    has grown by 'kill_after_bytes'."""
    start_size = binsrv.stored_data_size()
    process = binsrv.start_fetch()
    error = []

    def feed():
        try:
            run_sql(source, statements)
        except Exception as problem:  # reported after the join
            error.append(problem)

    thread = threading.Thread(target=feed)
    thread.start()
    deadline = time.monotonic() + 40
    while (process.poll() is None and time.monotonic() < deadline and
           binsrv.stored_data_size() - start_size < kill_after_bytes):
        time.sleep(0.005)
    if process.poll() is None:
        process.send_signal(signal.SIGKILL)
    process.communicate()
    thread.join()
    if error:
        raise error[0]


def run_variety(args, name: str, generator) -> dict:
    gtid = args.mode == "gtid"
    source = harness.MySQLServer(args.mysql_basedir, args.workdir / name,
                                 args.port, gtid, server_id=1)
    harness.SERVER = source
    harness.BINSRV = binsrv = harness.BinlogServer(
        args.binsrv, args.workdir / (name + "-binsrv"), args.port, gtid)
    result = {"variety": name, "mode": args.mode}
    source.start()
    try:
        if name == "tagged_gtid" and source.version() < (8, 4, 0):
            result["skipped"] = "tagged GTIDs need MySQL 8.4+"
            result["ok"] = True
            return result

        machine = harness.FetchCrashMachine()
        machine.start_from_scratch()
        machine.steps = []

        # 1. a full fetch of this event type must be byte-identical
        try:
            run_sql(source, generator(12))
        except RuntimeError as problem:
            result["workload_error"] = str(problem)[:300]
            result["ok"] = False
            return result
        binsrv.checkpoint_size = "4K"
        try:
            machine.fetch_to_the_end()
            result["exact_copy"] = True
        except harness.Failure as failure:
            result["exact_copy"] = False
            result["copy_error"] = str(failure)[:400]

        # 2. kill mid-stream and resume: tests boundary detection for the type
        try:
            fetch_and_kill(binsrv, source, generator(10),
                           kill_after_bytes=70_000)
            machine.storage_holds_prefixes()
            result["prefix_after_kill"] = True
        except harness.Failure as failure:
            result["prefix_after_kill"] = False
            result["prefix_error"] = str(failure)[:400]

        try:
            machine.fetch_to_the_end()
            result["exact_copy_after_resume"] = True
        except harness.Failure as failure:
            result["exact_copy_after_resume"] = False
            result["resume_error"] = str(failure)[:400]

        result["binlogs"] = len(binsrv.indexed_binlogs())
    finally:
        source.stop()
    result["ok"] = (result.get("exact_copy") and
                    result.get("prefix_after_kill") and
                    result.get("exact_copy_after_resume"))
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--mysql-basedir", type=Path, required=True)
    parser.add_argument("--binsrv", type=Path, required=True)
    parser.add_argument("--workdir", type=Path, required=True)
    parser.add_argument("--port", type=int, default=23464)
    parser.add_argument("--mode", choices=["gtid", "position"],
                        default="gtid")
    parser.add_argument("--varieties", default=",".join(VARIETIES))
    args = parser.parse_args()
    args.mysql_basedir = args.mysql_basedir.expanduser()
    args.binsrv = args.binsrv.expanduser()
    args.workdir.mkdir(parents=True, exist_ok=True)

    ok = True
    for name in args.varieties.split(","):
        try:
            result = run_variety(args, name, VARIETIES[name])
        except Exception as error:  # a variety crashing is itself a result
            result = {"variety": name, "ok": False,
                      "exception": f"{type(error).__name__}: {error}"[:400]}
        print(json.dumps(result), flush=True)
        ok = ok and result.get("ok", False)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
