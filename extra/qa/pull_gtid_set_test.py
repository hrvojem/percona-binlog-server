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

"""GTID auto-positioning of 'binlog_server pull' with uneven replica sets.

pull_gtid_test.py starts replicas with an empty GTID set or one contiguous
range of a single UUID. Here the source writes BLOCKS binlogs, each holding
U_PER_BLOCK transactions of its own UUID U, V_PER_BLOCK of a second UUID V
(set with GTID_NEXT) and, on 8.3 and later, TAGGED_PER_BLOCK tagged GTIDs
'V:qa:N'. Every transaction inserts one row with a known id, so the rows a
replica must end with follow from the GTIDs it lacks.

'pull' streams that into a fresh storage. For every case a replica is reset,
given a GTID set (GTID_PURGED, with the table created outside the binlog when
U:1, the CREATE TABLE, counts as executed) and connected with
SOURCE_AUTO_POSITION=1. 'pull' must start the dump early enough that every
missing transaction arrives; the replica skips the ones it has. The case
passes when the replica's GTID_EXECUTED equals the source's, its table holds
exactly the rows of the missing transactions and its threads report no
error. Cases (one JSON line each):

  full            the replica has nothing;
  gap_spanning    it lacks ten U transactions across a binlog boundary;
  mid_binlog      it lacks every U transaction from the middle of the
                  second binlog on;
  two_gaps        it lacks two short U ranges in different binlogs;
  single_first    it lacks only the first insert (first binlog);
  single_last     it lacks only the last U insert (last binlog);
  other_uuid      it has all of U but none of V;
  tagged          (8.3+) it has everything but the tagged GTIDs;
  nothing_missing it has everything; three transactions written after it
                  connects must still arrive (the dump starts at the last
                  binlog).

Uses the helpers of fetch_crash_test.py and pull_replica_test.py. Example:

  PYTHONPATH=~/ws/qa/pylib python3 pull_gtid_set_test.py \\
    --mysql-basedir ~/ws/dist/mysql-8.4.8-linux-glibc2.28-x86_64-minimal \\
    --binsrv ~/ws/percona-binlog-server-build-debug_gcc14/binlog_server \\
    --workdir ~/ws/qa/pull-gtid-set
"""

import argparse
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import fetch_crash_test as harness  # noqa: E402
import pull_replica_test as replication  # noqa: E402

BLOCKS = 6
U_PER_BLOCK = 10
V_PER_BLOCK = 3
TAGGED_PER_BLOCK = 2
TAG = "qa"
V_UUID = "11111111-2222-4333-8444-555555555555"
CATCH_UP_SECONDS = 90
TABLE = ("CREATE TABLE qa.t (id INT UNSIGNED NOT NULL AUTO_INCREMENT "
         "PRIMARY KEY, payload LONGBLOB) ENGINE=InnoDB")


class Gtid:
    def __init__(self, sid: str, tag: str, gno: int, row: "int | None"):
        self.sid = sid
        self.tag = tag
        self.gno = gno
        self.row = row  # the id inserted, None for the CREATE TABLE

    def key(self) -> tuple:
        return (self.sid, self.tag, self.gno)


def ranges(numbers: list) -> str:
    parts = []
    numbers = sorted(numbers)
    start = previous = numbers[0]
    for number in numbers[1:] + [None]:
        if number is not None and number == previous + 1:
            previous = number
            continue
        parts.append(str(start) if start == previous else
                     f"{start}-{previous}")
        if number is not None:
            start = previous = number
    return ":".join(parts)


def gtid_text(gtids: list) -> str:
    """The MySQL text form of a set of Gtid ('sid:1-5:7:tag:1-3,...')."""
    by_sid = {}
    for gtid in gtids:
        by_sid.setdefault(gtid.sid, {}).setdefault(gtid.tag, []).append(
            gtid.gno)
    parts = []
    for sid in sorted(by_sid):
        tags = by_sid[sid]
        text = sid
        if "" in tags:
            text += ":" + ranges(tags[""])
        for tag in sorted(name for name in tags if name):
            text += f":{tag}:" + ranges(tags[tag])
        parts.append(text)
    return ",".join(parts)


def write_source(source, tagged: bool) -> list:
    """Writes the workload and returns its GTIDs in order."""
    source.reset()
    u_uuid = source.sql("SELECT @@GLOBAL.SERVER_UUID").strip()
    gtids = [Gtid(u_uuid, "", 1, None)]  # the CREATE TABLE of reset()
    row = 0
    v_gno = 0
    tag_gno = 0
    for _ in range(BLOCKS):
        statements = []
        for _ in range(U_PER_BLOCK):
            row += 1
            statements.append(f"INSERT INTO qa.t VALUES ({row}, 'u');")
            gtids.append(Gtid(u_uuid, "", row + 1, row))
        for _ in range(V_PER_BLOCK):
            v_gno += 1
            statements.append(
                f"SET GTID_NEXT='{V_UUID}:{v_gno}'; "
                f"INSERT INTO qa.t VALUES ({1000 + v_gno}, 'v'); "
                "SET GTID_NEXT='AUTOMATIC';")
            gtids.append(Gtid(V_UUID, "", v_gno, 1000 + v_gno))
        if tagged:
            for _ in range(TAGGED_PER_BLOCK):
                tag_gno += 1
                statements.append(
                    f"SET GTID_NEXT='{V_UUID}:{TAG}:{tag_gno}'; "
                    f"INSERT INTO qa.t VALUES ({2000 + tag_gno}, 't'); "
                    "SET GTID_NEXT='AUTOMATIC';")
                gtids.append(Gtid(V_UUID, TAG, tag_gno, 2000 + tag_gno))
        source.sql(" ".join(statements))
        source.sql("FLUSH BINARY LOGS")
    return gtids


def executed(server) -> str:
    """GTID_EXECUTED on one line (mysql -B escapes its newlines)."""
    return server.sql("SELECT @@GLOBAL.GTID_EXECUTED").strip().replace(
        "\\n", "").replace("\n", "")


def same_set(server, first: str, second: str) -> bool:
    return server.sql(
        f"SELECT GTID_SUBSET('{first}', '{second}') AND "
        f"GTID_SUBSET('{second}', '{first}')").strip() == "1"


def thread_errors(replica) -> list:
    errors = []
    for table, column in (
            ("replication_connection_status", "LAST_ERROR_MESSAGE"),
            ("replication_applier_status_by_worker", "LAST_ERROR_MESSAGE")):
        for line in replica.sql(
                f"SELECT LAST_ERROR_NUMBER, {column} FROM "
                f"performance_schema.{table}").splitlines():
            number, _, message = line.partition("\t")
            if number not in ("", "0"):
                errors.append(f"{number}: {message[:200]}")
    return errors


def replica_rows(replica) -> list:
    text = replica.sql("SELECT id FROM qa.t ORDER BY id")
    return [int(line) for line in text.split()]


def run_case(source, replica, pull_port: int, gtids: list, missing: list,
             later: int = 0) -> dict:
    """Gives the replica every GTID but 'missing', connects it, writes
    'later' more U inserts on the source and checks the outcome."""
    missing_keys = {gtid.key() for gtid in missing}
    had = [gtid for gtid in gtids if gtid.key() not in missing_keys]
    prepare = []
    if any(gtid.row is None for gtid in had):
        # the CREATE TABLE counts as executed: create the table outside
        # the binlog
        prepare.append(f"SET SESSION SQL_LOG_BIN=0; {TABLE}; "
                       "SET SESSION SQL_LOG_BIN=1;")
    if had:
        prepare.append(f"SET GLOBAL GTID_PURGED='{gtid_text(had)}';")
    replication.replicate_from_pull(replica, pull_port, True,
                                    "SOURCE_AUTO_POSITION=1",
                                    " ".join(prepare))
    expected_rows = sorted(gtid.row for gtid in missing
                           if gtid.row is not None)
    if later:
        first = max(gtid.row for gtid in gtids
                     if gtid.sid != V_UUID and gtid.row is not None) + 1
        for row in range(first, first + later):
            source.sql(f"INSERT INTO qa.t VALUES ({row}, 'later')")
            expected_rows.append(row)
    source_executed = executed(source)
    deadline = time.monotonic() + CATCH_UP_SECONDS
    caught_up = False
    while time.monotonic() < deadline:
        if same_set(replica, executed(replica), source_executed):
            caught_up = True
            break
        if thread_errors(replica):
            break
        time.sleep(1)
    replica_executed = executed(replica)
    try:
        rows = replica_rows(replica)
    except RuntimeError as error:
        rows = [f"error: {str(error).splitlines()[0][:120]}"]
    errors = thread_errors(replica)
    replica.sql("STOP REPLICA")
    lacking = sorted(set(expected_rows) - set(row for row in rows
                                               if isinstance(row, int)))
    extra = [row for row in rows if row not in expected_rows]
    return {"replica_had": gtid_text(had) if had else "",
            "missing": gtid_text(missing) if missing else "",
            "caught_up": caught_up, "rows_lacking": lacking[:20],
            "rows_extra": extra[:20], "errors": errors,
            "replica_executed": replica_executed[:300],
            "source_executed": source_executed[:300],
            "ok": caught_up and not lacking and not extra and not errors}


def run(args) -> list:
    work = args.workdir
    source = harness.MySQLServer(args.mysql_basedir, work / "source",
                                 args.port, True, server_id=1)
    replica = harness.MySQLServer(args.mysql_basedir, work / "replica",
                                  args.port + 10, True, server_id=10)
    harness.SERVER = source
    harness.BINSRV = binsrv = harness.BinlogServer(
        args.binsrv, work / "binsrv", args.port, True)
    binsrv.reset()
    binsrv.checkpoint_size = "1"
    pull_port = args.port + 2
    results = []
    pull = None
    source.start()
    replica.start()
    try:
        tagged = source.version() >= (8, 3, 0)
        gtids = write_source(source, tagged)
        pull = replication.start_pull(binsrv, pull_port)

        def u_rows(first: int, last: int) -> list:
            return [gtid for gtid in gtids if gtid.sid != V_UUID and
                    gtid.row is not None and first <= gtid.row <= last]

        u_last = max(gtid.row for gtid in gtids
                     if gtid.sid != V_UUID and gtid.row is not None)
        cases = {
            "full": gtids,
            "gap_spanning": u_rows(U_PER_BLOCK * 2 - 4, U_PER_BLOCK * 2 + 5),
            "mid_binlog": u_rows(U_PER_BLOCK + U_PER_BLOCK // 2, u_last),
            "two_gaps": u_rows(5, 7) + u_rows(U_PER_BLOCK * 4 + 1,
                                              U_PER_BLOCK * 4 + 3),
            "single_first": u_rows(1, 1),
            "single_last": u_rows(u_last, u_last),
            "other_uuid": [gtid for gtid in gtids
                           if gtid.sid == V_UUID and not gtid.tag],
        }
        if tagged:
            cases["tagged"] = [gtid for gtid in gtids if gtid.tag]
        for name, missing in cases.items():
            try:
                result = run_case(source, replica, pull_port, gtids, missing)
            except Exception as error:  # reported as the case's failure
                result = {"error": harness.describe_failure(error, 600),
                          "ok": False}
            results.append({"case": name, **result})
            print(json.dumps(results[-1]), flush=True)
        try:
            result = run_case(source, replica, pull_port, gtids, [], later=3)
        except Exception as error:  # reported as the case's failure
            result = {"error": harness.describe_failure(error, 600),
                      "ok": False}
        results.append({"case": "nothing_missing", **result})
        print(json.dumps(results[-1]), flush=True)
        results.append({"case": "pull_alive", "ok": pull.poll() is None})
        print(json.dumps(results[-1]), flush=True)
    finally:
        if pull is not None and pull.poll() is None:
            pull.terminate()
            pull.wait(timeout=120)
        source.stop()
        replica.stop()
    return results


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--mysql-basedir", type=Path, required=True)
    parser.add_argument("--binsrv", type=Path, required=True)
    parser.add_argument("--workdir", type=Path, required=True)
    parser.add_argument("--port", type=int, default=23870)
    parser.add_argument("--mode", choices=["gtid"], default="gtid")
    args = parser.parse_args()
    args.mysql_basedir = args.mysql_basedir.expanduser()
    args.binsrv = args.binsrv.expanduser()
    args.workdir.mkdir(parents=True, exist_ok=True)
    results = run(args)
    (args.workdir / "results.json").write_text(json.dumps(results, indent=2))
    return 0 if all(result.get("ok") for result in results) else 1


if __name__ == "__main__":
    sys.exit(main())
