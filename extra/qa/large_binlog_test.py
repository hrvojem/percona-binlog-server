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

With '--storage s3' the storage is a prefix in a bucket of an
S3-compatible server whose objects are files under '--s3-bucket-dir' (as
with the POSIX backend of Versity S3 Gateway). The harness puts
s3_fault_proxy.py (no faults) between binlog_server and the server to count
the requests, and adds:

  s3_put_size          no request uploads more than 5 GiB, the limit of a
                       single PUT on AWS S3 (a larger object must go up as a
                       multipart upload, whose parts the proxy sees as PUTs
                       with a partNumber); local servers do not enforce it.

With S3 storage only binlog_over_4gib runs by default: the large
transaction fails in the collector before the storage is involved.

Resuming inside such a binlog is not covered: it is closed when the
transaction commits, and the 32-bit COM_BINLOG_DUMP position could not
express the offset anyway. Needs about 20 GB of free disk space (30 GB with
S3 storage, which also keeps a local buffer of the open binlog). Uses the
helpers of fetch_crash_test.py and pull_replica_test.py. Example:

  PYTHONPATH=~/ws/qa/pylib python3 large_binlog_test.py \\
    --mysql-basedir ~/ws/dist/mysql-8.4.8-linux-glibc2.28-x86_64-minimal \\
    --binsrv ~/ws/percona-binlog-server-build-debug_gcc14/binlog_server \\
    --workdir ~/ws/qa/large-binlog
  (add '--storage s3 --s3-bucket-dir ~/ws/s3/data/pbs-mtr' for S3 storage)
"""

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import fetch_crash_test as harness  # noqa: E402
import pull_replica_test as replication  # noqa: E402

GIB = 1 << 30
ROWS = 100
ROW_SIZE = 1 << 20
# case: (1 MiB transactions before the large one, UPDATEs in the large one)
CASES = {"transaction_over_4gib": (0, 22), "binlog_over_4gib": (1000, 16)}
CHECKS = {"file": ("fetch_copies_match", "refetch", "list_size",
                   "pull_raw_copy")}
CHECKS["s3"] = CHECKS["file"] + ("s3_put_size",)
REQUIRED_FREE_BYTES = 20 * GIB
REQUIRED_FREE_BYTES_S3 = 30 * GIB
# the largest object a single PUT may upload to AWS S3
MAX_SINGLE_PUT = 5 * GIB
# how long the local S3 server may take to finish a multipart upload it has
# already confirmed (see wait_for_objects)
S3_SETTLE_SECONDS = 300
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


def wait_for_objects(source, names: list, bucket_dir: Path) -> float:
    """Waits until every closed binlog's object file in the bucket has the
    source's size, for up to S3_SETTLE_SECONDS; returns how long that took
    (0 if they were complete at once).

    Versity S3 Gateway 1.8.0 needs about a minute to join the parts of a
    4.5 GB multipart upload. The AWS SDK's CRT client sends the
    CompleteMultipartUpload again after a few seconds, and the gateway
    answers that duplicate with 200 at once, before the object exists, so
    'fetch' can finish before the object is complete. AWS S3 itself confirms
    a completion only once the object exists."""
    started = time.monotonic()
    while True:
        complete = all(
            (bucket_dir / name).exists() and
            (bucket_dir / name).stat().st_size ==
            source.binlog_path(name).stat().st_size for name in names)
        elapsed = time.monotonic() - started
        if complete or elapsed > S3_SETTLE_SECONDS:
            return round(elapsed, 1) if elapsed >= 1 else 0
        time.sleep(1)


def run_binsrv(binsrv, mode: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        [str(binsrv.binary), mode, str(binsrv.config_path)],
        capture_output=True, text=True, timeout=COMMAND_TIMEOUT,
        env=dict(os.environ, AWS_EC2_METADATA_DISABLED="true"))


class S3Proxy:
    """s3_fault_proxy.py in front of the S3 server, counting requests."""

    def __init__(self, args, log_path: Path):
        self.endpoint = f"127.0.0.1:{args.s3_proxy_port}"
        self.control = f"http://{self.endpoint}/__faults__"
        self.log = log_path.open("w")
        self.process = subprocess.Popen(
            [sys.executable,
             str(Path(__file__).resolve().parent / "s3_fault_proxy.py"),
             "--listen", self.endpoint, "--backend", args.s3_endpoint],
            stdout=self.log, stderr=subprocess.STDOUT)
        deadline = time.monotonic() + 20
        while True:
            try:
                self.stats()
                return
            except OSError:
                if self.process.poll() is not None or \
                        time.monotonic() > deadline:
                    self.stop()
                    raise RuntimeError(f"s3_fault_proxy.py did not come up "
                                       f"(see {log_path})")
                time.sleep(0.3)

    def stats(self) -> dict:
        with urllib.request.urlopen(self.control, timeout=10) as response:
            return json.loads(response.read())

    def reset(self):
        request = urllib.request.Request(
            self.control, method="PUT",
            data=json.dumps({"rules": [], "reset_traffic": True}).encode())
        with urllib.request.urlopen(request, timeout=10) as response:
            response.read()

    def stop(self):
        self.process.terminate()
        try:
            self.process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.wait()
        self.log.close()


def run_case(args, case: str, proxy: "S3Proxy | None") -> dict:
    fill_transactions, updates = CASES[case]
    work = args.workdir / case
    work.mkdir(parents=True, exist_ok=True)
    result = {"case": case, "storage": args.storage}
    required = REQUIRED_FREE_BYTES if proxy is None else REQUIRED_FREE_BYTES_S3
    free = shutil.disk_usage(work).free
    if free < required:
        result.update(ok=False, skipped=True,
                      reason=f"only {free // GIB} GiB free, "
                             f"{required // GIB} needed")
        return result
    harness.MAX_BINLOG_SIZE = GIB
    s3 = None
    if proxy is not None:
        s3 = harness.S3Storage(proxy.endpoint, args.s3_bucket,
                               args.s3_bucket_dir, args.s3_access_key,
                               args.s3_secret_key, f"large-binlog-{case}")
    source = harness.MySQLServer(args.mysql_basedir, work / "source",
                                 args.port, False, server_id=1)
    harness.SERVER = source
    harness.BINSRV = binsrv = harness.BinlogServer(
        args.binsrv, work / "binsrv", args.port, False, s3)
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
        if proxy is not None:
            proxy.reset()
        started = time.monotonic()
        fetch = run_binsrv(binsrv, "fetch")
        result["fetch_seconds"] = round(time.monotonic() - started)
        result["fetch_exit"] = fetch.returncode
        if fetch.returncode != 0:
            result["fetch_error"] = fetch.stderr.strip()[-300:]
        if proxy is not None:
            put = proxy.stats()["traffic"].get("PUT", {})
            result["s3_put_requests"] = put.get("requests", 0)
            result["s3_put_bytes"] = put.get("request_bytes", 0)
            result["s3_largest_put"] = put.get("max_request_bytes", 0)
            result["s3_part_uploads"] = put.get("part_requests", 0)
            result["s3_put_size"] = (put.get("requests", 0) > 0 and
                                     result["s3_largest_put"] <=
                                     MAX_SINGLE_PUT)
        if proxy is not None:
            late = wait_for_objects(source, closed, binsrv.storage)
            if late:
                result["s3_object_late_seconds"] = late
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

        result["ok"] = all(result[check] for check in CHECKS[args.storage])
    finally:
        if pull is not None:
            pull.terminate()
            pull.wait(timeout=60)
        source.stop()
        if not args.keep:
            shutil.rmtree(work / "source", ignore_errors=True)
            shutil.rmtree(work / "binsrv", ignore_errors=True)
            shutil.rmtree(work / "raw", ignore_errors=True)
            shutil.rmtree(binsrv.storage, ignore_errors=True)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--mysql-basedir", type=Path, required=True)
    parser.add_argument("--binsrv", type=Path, required=True)
    parser.add_argument("--workdir", type=Path, required=True)
    parser.add_argument("--port", type=int, default=23500)
    # position only: the size of a binlog does not depend on the mode
    parser.add_argument("--mode", choices=["position"], default="position")
    parser.add_argument("--cases",
                        help="default: all, binlog_over_4gib with S3 storage")
    parser.add_argument("--keep", action="store_true",
                        help="keep the multi-GB data directories")
    parser.add_argument("--storage", choices=["file", "s3"], default="file")
    parser.add_argument("--s3-endpoint", default="127.0.0.1:9000",
                        help="host:port of the S3 server")
    parser.add_argument("--s3-proxy-port", type=int, default=24595,
                        help="where the counting s3_fault_proxy.py listens")
    parser.add_argument("--s3-bucket", default="pbs-mtr")
    parser.add_argument("--s3-bucket-dir", type=Path,
                        help="the directory of the S3 server's bucket")
    parser.add_argument("--s3-access-key", default="pbsaccesskey")
    parser.add_argument("--s3-secret-key", default="pbs/secret+key=1")
    args = parser.parse_args()
    args.mysql_basedir = args.mysql_basedir.expanduser()
    args.binsrv = args.binsrv.expanduser()
    args.workdir.mkdir(parents=True, exist_ok=True)
    if args.cases is None:
        args.cases = ("binlog_over_4gib" if args.storage == "s3"
                      else ",".join(CASES))
    proxy = None
    if args.storage == "s3":
        if args.s3_bucket_dir is None:
            parser.error("--storage s3 needs --s3-bucket-dir")
        args.s3_bucket_dir = args.s3_bucket_dir.expanduser()
        proxy = S3Proxy(args, args.workdir / "s3_fault_proxy.log")
    try:
        results = [run_case(args, case, proxy)
                   for case in args.cases.split(",")]
    finally:
        if proxy is not None:
            proxy.stop()
    for result in results:
        print(json.dumps(result), flush=True)
    (args.workdir / "results.json").write_text(json.dumps(results, indent=2))
    return 0 if all(result.get("ok") for result in results) else 1


if __name__ == "__main__":
    sys.exit(main())
