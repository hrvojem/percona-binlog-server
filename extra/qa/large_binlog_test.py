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

"""Checks binlogs and transactions larger than 4 GiB end to end.

Binlog positions above 4 GiB do not fit the 32-bit fields of the protocol
(the next_event_position of every event header, the COM_BINLOG_DUMP
position), so offsets in the storage and the replication source have to be
64-bit, as does the transaction_length of a GTID event. MySQL rotates a
binlog (max_binlog_size 1 GiB here) only after the transaction that crosses
max_binlog_size. The large transaction is built from UPDATEs of a table of
ROWS rows of ROW_SIZE bytes (each UPDATE logs a before and an after image of
every row), which keeps every event at about 2 MiB, below the 16 MiB a
replica of 'pull' can receive (ticket 10). Cases:

  transaction_over_4gib  one transaction of about 4.7 GB (ticket 41: the
                         collector refuses a transaction_length above 32
                         bits);
  binlog_over_4gib       about 1 GB of 1 MiB transactions, then one of about
                         3.5 GB: a binlog of about 4.5 GB made of
                         transactions below 4 GiB.

For each case:

  fetch_copies_match   'fetch' stores every closed binlog byte for byte;
  refetch              a second 'fetch' opens the storage holding it;
  list_size            'list' reports its size;
  pull_raw_copy        'mysqlbinlog --read-from-remote-server --raw' through
                       'pull' copies every closed binlog byte for byte,
                       streaming through the 4 GiB mark in one dump.

Resuming inside such a binlog is not covered: it is closed when the
transaction commits, and the 32-bit COM_BINLOG_DUMP position could not
express the offset anyway. Needs about 20 GB of free disk space. Uses the
helpers of fetch_crash_test.py and pull_replica_test.py. Example:

  PYTHONPATH=~/ws/qa/pylib python3 large_binlog_test.py \\
    --mysql-basedir ~/ws/dist/mysql-8.4.8-linux-glibc2.28-x86_64-minimal \\
    --binsrv ~/ws/percona-binlog-server-build-debug_gcc14/binlog_server \\
    --workdir ~/ws/qa/large-binlog
"""

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import fetch_crash_test as harness  # noqa: E402
import pull_replica_test as replication  # noqa: E402

GIB = 1 << 30
ROWS = 100
ROW_SIZE = 1 << 20
# case: (1 MiB transactions before the large one, UPDATEs in the large one)
CASES = {"transaction_over_4gib": (0, 22), "binlog_over_4gib": (1000, 16)}
REQUIRED_FREE_BYTES = 20 * GIB
COMMAND_TIMEOUT = 1800
AUTH_RSA_PUBLIC_KEY = (Path(__file__).resolve().parents[2] / "mtr" /
                       "binlog_streaming" / "std_data" /
                       "caching_sha2_rsa_pub.pem")


def digest(path: Path) -> tuple:
    """(size, SHA-256) of a file, read in chunks."""
    hasher = hashlib.sha256()
    with path.open("rb") as file:
        while chunk := file.read(16 << 20):
            hasher.update(chunk)
    return path.stat().st_size, hasher.hexdigest()


def compare(source, names: list, copy_dir: Path) -> list:
    """The closed binlogs whose copy in 'copy_dir' differs from the
    source's file (or is missing)."""
    mismatches = []
    for name in names:
        copy = copy_dir / name
        if not copy.exists():
            mismatches.append(f"{name}: missing")
            continue
        expected = digest(source.binlog_path(name))
        actual = digest(copy)
        if actual != expected:
            mismatches.append(f"{name}: {actual[0]} bytes, expected "
                              f"{expected[0]}" if actual[0] != expected[0]
                              else f"{name}: same size, different bytes")
    return mismatches


def run_binsrv(binsrv, mode: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        [str(binsrv.binary), mode, str(binsrv.config_path)],
        capture_output=True, text=True, timeout=COMMAND_TIMEOUT,
        env=dict(os.environ, AWS_EC2_METADATA_DISABLED="true"))


def run_case(args, case: str) -> dict:
    fill_transactions, updates = CASES[case]
    work = args.workdir / case
    work.mkdir(parents=True, exist_ok=True)
    result = {"case": case}
    free = shutil.disk_usage(work).free
    if free < REQUIRED_FREE_BYTES:
        result.update(ok=False, skipped=True,
                      reason=f"only {free // GIB} GiB free, "
                             f"{REQUIRED_FREE_BYTES // GIB} needed")
        return result
    harness.MAX_BINLOG_SIZE = GIB
    source = harness.MySQLServer(args.mysql_basedir, work / "source",
                                 args.port, False, server_id=1)
    harness.SERVER = source
    harness.BINSRV = binsrv = harness.BinlogServer(
        args.binsrv, work / "binsrv", args.port, False)
    source.start()
    pull = None
    try:
        source.reset()
        binsrv.reset()
        source.sql("CREATE TABLE qa.big (id INT UNSIGNED NOT NULL "
                   "AUTO_INCREMENT PRIMARY KEY, payload LONGBLOB) "
                   "ENGINE=InnoDB")
        # one transaction: the rows, then UPDATEs that change every row (the
        # body has semicolons, so the client needs another delimiter)
        source.sql(f"""
            DELIMITER //
            CREATE PROCEDURE qa.fill()
            BEGIN
              DECLARE i INT DEFAULT 0;
              WHILE i < {fill_transactions} DO
                INSERT INTO qa.t (payload) VALUES (REPEAT('f', {ROW_SIZE}));
                SET i = i + 1;
              END WHILE;
            END //
            CREATE PROCEDURE qa.grow()
            BEGIN
              DECLARE i INT DEFAULT 0;
              START TRANSACTION;
              WHILE i < {ROWS} DO
                INSERT INTO qa.big (payload) VALUES (REPEAT('a', {ROW_SIZE}));
                SET i = i + 1;
              END WHILE;
              SET i = 0;
              WHILE i < {updates} DO
                UPDATE qa.big
                  SET payload = REPEAT(CHAR(66 + i % 24), {ROW_SIZE});
                SET i = i + 1;
              END WHILE;
              COMMIT;
            END //
            DELIMITER ;""")
        # autocommit: each INSERT of the procedure is its own transaction
        source.sql("CALL qa.fill()")
        started = time.monotonic()
        source.sql("CALL qa.grow()")
        result["transaction_seconds"] = round(time.monotonic() - started)
        # a transaction in the next binlog, then close it too
        source.sql("INSERT INTO qa.t (payload) VALUES ('after')")
        source.sql("FLUSH BINARY LOGS")
        names = source.binlog_names()
        closed = names[:-1]
        sizes = {name: source.binlog_path(name).stat().st_size
                 for name in closed}
        large = [name for name, size in sizes.items() if size > 4 * GIB]
        result["large_binlog"] = {name: sizes[name] for name in large}
        if not large:
            result.update(ok=False, reason="no binlog grew past 4 GiB")
            return result

        binsrv.write_config()
        started = time.monotonic()
        fetch = run_binsrv(binsrv, "fetch")
        result["fetch_seconds"] = round(time.monotonic() - started)
        result["fetch_exit"] = fetch.returncode
        mismatches = compare(source, closed, binsrv.storage)
        result["fetch_copies_match"] = (fetch.returncode == 0 and
                                        not mismatches)
        if mismatches:
            result["fetch_mismatches"] = mismatches

        refetch = run_binsrv(binsrv, "fetch")
        result["refetch"] = refetch.returncode == 0
        if not result["refetch"]:
            result["refetch_error"] = refetch.stderr.strip()[-300:]

        listed = run_binsrv(binsrv, "list")
        reported = {}
        try:
            for entry in json.loads(listed.stdout).get("result", []):
                reported[entry.get("name")] = entry.get("size")
        except (json.JSONDecodeError, AttributeError):
            pass
        result["list_size"] = all(reported.get(name) == sizes[name]
                                  for name in large)
        if not result["list_size"]:
            result["list_output"] = listed.stdout.strip()[-300:]

        pull_port = args.port + 2
        pull = replication.start_pull(binsrv, pull_port)
        raw_dir = work / "raw"
        raw_dir.mkdir()
        started = time.monotonic()
        raw = subprocess.run(
            [str(args.mysql_basedir / "bin" / "mysqlbinlog"), "--no-defaults",
             "--read-from-remote-server", "--host=127.0.0.1",
             f"--port={pull_port}", f"--user={replication.PULL_USER}",
             f"--password={replication.PULL_PASSWORD}",
             f"--server-public-key-path={AUTH_RSA_PUBLIC_KEY}",
             "--ssl-mode=DISABLED", "--raw", "--to-last-log",
             f"--result-file={raw_dir}/", names[0]],
            capture_output=True, text=True, timeout=COMMAND_TIMEOUT)
        result["raw_seconds"] = round(time.monotonic() - started)
        result["raw_exit"] = raw.returncode
        raw_mismatches = compare(source, closed, raw_dir)
        result["pull_raw_copy"] = raw.returncode == 0 and not raw_mismatches
        if not result["pull_raw_copy"]:
            result["raw_mismatches"] = raw_mismatches
            result["raw_error"] = raw.stderr.strip()[-300:]

        result["ok"] = all(result[check] for check in (
            "fetch_copies_match", "refetch", "list_size", "pull_raw_copy"))
    finally:
        if pull is not None:
            pull.terminate()
            pull.wait(timeout=60)
        source.stop()
        if not args.keep:
            shutil.rmtree(work / "source", ignore_errors=True)
            shutil.rmtree(work / "binsrv", ignore_errors=True)
            shutil.rmtree(work / "raw", ignore_errors=True)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--mysql-basedir", type=Path, required=True)
    parser.add_argument("--binsrv", type=Path, required=True)
    parser.add_argument("--workdir", type=Path, required=True)
    parser.add_argument("--port", type=int, default=23500)
    # position only: the size of a binlog does not depend on the mode
    parser.add_argument("--mode", choices=["position"], default="position")
    parser.add_argument("--cases", default=",".join(CASES))
    parser.add_argument("--keep", action="store_true",
                        help="keep the multi-GB data directories")
    args = parser.parse_args()
    args.mysql_basedir = args.mysql_basedir.expanduser()
    args.binsrv = args.binsrv.expanduser()
    args.workdir.mkdir(parents=True, exist_ok=True)
    results = [run_case(args, case) for case in args.cases.split(",")]
    for result in results:
        print(json.dumps(result), flush=True)
    (args.workdir / "results.json").write_text(json.dumps(results, indent=2))
    return 0 if all(result.get("ok") for result in results) else 1


if __name__ == "__main__":
    sys.exit(main())
