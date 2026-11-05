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

"""Upgrade from a released binlog_server to the current one.

A storage written by the previous release (--old-binsrv, e.g. pbs-0.4.1) is
taken over by the current build (--binsrv) with the configuration the
current build needs: the same settings plus the 'instance' and
'replication_source' sections (both mandatory since the release). Cases (one
JSON line each):

  old_config       the release's configuration, unchanged, given to the
                   current build: reported with the error it gives (it must
                   fail and name what is missing);
  plain_takeover   the release fetches several binlogs; the current build
                   must list them exactly as the release does, answer the
                   searches the same way, resume the fetch and end with a
                   byte-identical copy of the source's binlogs, and serve
                   all of it to a MySQL replica through 'pull';
  encrypted_takeover
                   the same with an encrypted storage (keyring KEK 'alpha',
                   AES-256-CTR); the copy is checked through the replica;
  s3_takeover      (with --s3-bucket-dir) plain_takeover on S3 storage of
                   the local S3-compatible server;
  rewrite_takeover (GTID mode) plain_takeover with a 'replication.rewrite'
                   section (binlogs of their own names and 32K size); the
                   copy is checked through the replica;
  killed_takeover  the release is killed with SIGKILL in the middle of a
                   fetch (checkpoint_size 64K, so it leaves a partly written
                   binlog); the current build must recover the storage and
                   complete the copy;
                   With --tolerate-ticket-14 (GTID mode, as the nightly
                   runs it) the copy may lack a closing ROTATE where the
                   kill came between a binlog's last transaction and its
                   ROTATE (ticket 14: the GTID-mode resume starts in the
                   next binlog); hit on 9.7 on 2026-11-05;
  downgrade        (reported, not checked) the release lists the storage
                   after the current build has written to it.

Uses the helpers of fetch_crash_test.py and pull_replica_test.py. Example:

  PYTHONPATH=~/ws/qa/pylib python3 upgrade_test.py \\
    --mysql-basedir ~/ws/dist/mysql-8.4.8-linux-glibc2.28-x86_64-minimal \\
    --old-binsrv ~/ws/pbs-0.4.1-build/binlog_server \\
    --binsrv ~/ws/percona-binlog-server-build-debug_gcc14/binlog_server \\
    --workdir ~/ws/qa/upgrade --mode gtid
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
import pull_replica_test as replication  # noqa: E402

# the 'alpha' record of the repository's sample keyring: its AES-256-GCM
# record 'beta' holds 31 bytes, so the release and the current build alike
# reject the whole file ("key data length mismatch", ticket 52)
KEYRING_SOURCE = Path(__file__).resolve().parents[2] / "keyring_data.json"
KEYRING: Path = None  # written into the work directory by main()
ENCRYPTION = {"format": "generic", "kek_id": "alpha", "cipher": "AES-256-CTR"}
# rewrite mode (GTID mode only): binlogs of its own names and size
REWRITE = {"base_file_name": "rewritten", "file_size": "32K"}
CATCH_UP_SECONDS = 120
# sections the release does not know (its configuration parser rejects
# unknown sections)
NEW_SECTIONS = ("instance", "replication_source", "telemetry")


class Takeover:
    """One storage, written by 'old' and then 'new' binlog_server."""

    def __init__(self, args, name: str, encrypted: bool = False,
                 s3: bool = False, rewrite: bool = False):
        self.args = args
        self.gtid_mode = args.mode == "gtid"
        storage = None
        if s3:
            storage = harness.S3Storage(
                args.s3_endpoint, args.s3_bucket, args.s3_bucket_dir,
                args.s3_access_key, args.s3_secret_key,
                f"upgrade-{args.mode}-{name}-{os.getpid()}")
        self.binsrv = harness.BinlogServer(
            args.binsrv, args.workdir / name, args.port, self.gtid_mode,
            storage)
        self.binsrv.reset()
        self.encrypted = encrypted
        # rewrite mode names the binlogs itself, so the copy is not
        # byte-identical to the source's files
        self.byte_identical = not encrypted and not rewrite
        self.old_config = self.binsrv.workdir / "old.json"
        if encrypted or rewrite:
            # every configuration written for this storage (also the one
            # of start_pull) carries these settings
            write_config = self.binsrv.write_config

            def write_extended_config():
                write_config()
                config = json.loads(self.binsrv.config_path.read_text())
                if encrypted:
                    config["keyring"] = {"uri": f"file://{KEYRING}"}
                    config["storage"]["encryption"] = dict(ENCRYPTION)
                if rewrite:
                    config["replication"]["rewrite"] = dict(REWRITE)
                self.binsrv.config_path.write_text(
                    json.dumps(config, indent=2))

            self.binsrv.write_config = write_extended_config

    def write_configs(self) -> None:
        self.binsrv.write_config()
        config = json.loads(self.binsrv.config_path.read_text())
        old = {key: value for key, value in config.items()
               if key not in NEW_SECTIONS}
        self.old_config.write_text(json.dumps(old, indent=2))

    def run(self, binary: Path, mode: str, config: Path, *extra,
            timeout: float = harness.FETCH_TIMEOUT_SECONDS) -> tuple:
        environment = dict(os.environ, AWS_EC2_METADATA_DISABLED="true")
        try:
            result = subprocess.run(
                [str(binary), mode, str(config), *extra],
                capture_output=True, text=True, timeout=timeout,
                env=environment)
        except subprocess.TimeoutExpired:
            return None, "", f"{mode} did not finish in {timeout} s"
        return result.returncode, result.stdout, result.stderr

    def old(self, mode: str, *extra) -> tuple:
        self.write_configs()
        return self.run(self.args.old_binsrv, mode, self.old_config, *extra)

    def new(self, mode: str, *extra) -> tuple:
        self.write_configs()
        return self.run(self.args.binsrv, mode, self.binsrv.config_path,
                        *extra)


def workload(source, rows: int, first: int, per_binlog: int,
             payload: int) -> None:
    statements = []
    for row in range(first, first + rows):
        statements.append(f"INSERT INTO qa.t VALUES ({row}, "
                          f"REPEAT('{chr(97 + row % 26)}', {payload}));")
        if (row - first + 1) % per_binlog == 0:
            statements.append("FLUSH BINARY LOGS;")
    for start in range(0, len(statements), 200):
        source.sql(" ".join(statements[start:start + 200]))


def query_result(output: tuple) -> "list | str":
    """The 'result' list of a list / search_by_* run, or why there is
    none."""
    code, stdout, stderr = output
    if code != 0:
        return f"exit {code}: {(stderr or stdout).strip()[-300:]}"
    try:
        document = json.loads(stdout)
    except ValueError:
        return f"not JSON: {stdout[:300]}"
    if document.get("status") != "success":
        return f"status {document.get('status')}: {stdout[:300]}"
    return document.get("result")


def compare_queries(takeover: Takeover, source) -> dict:
    """The release and the current build query the same storage: their
    answers must agree. The URIs are left out (both print the same storage
    path, but the comparison should not depend on it)."""
    queries = {"list": ()}
    last = takeover.binsrv.indexed_binlogs()
    if last:
        listed = query_result(takeover.old("list"))
        if isinstance(listed, list) and listed:
            middle = listed[len(listed) // 2]
            queries["search_by_timestamp"] = (middle["max_timestamp"],)
    if takeover.gtid_mode:
        uuid = source.sql("SELECT @@GLOBAL.SERVER_UUID").strip()
        queries["search_by_gtid_set"] = (f"{uuid}:5-15",)
    differences = {}
    for mode, extra in queries.items():
        old = query_result(takeover.old(mode, *extra))
        new = query_result(takeover.new(mode, *extra))
        strip = lambda result: ([{key: value for key, value in entry.items()
                                  if key != "uri"} for entry in result]
                                if isinstance(result, list) else result)
        if strip(old) != strip(new) or not isinstance(new, list):
            differences[mode] = {"release": str(old)[:400],
                                 "current": str(new)[:400]}
    return differences


def fetch_to_the_end(takeover: Takeover, current: bool) -> "str | None":
    code, _, stderr = (takeover.new("fetch") if current else
                       takeover.old("fetch"))
    if code != 0:
        return (f"{'current' if current else 'release'} fetch exited with "
                f"{code}: {stderr.strip()[-400:]}\n"
                f"{takeover.binsrv.log_tail(8)}")
    return None


def complete_copy(source, takeover: Takeover) -> "str | None":
    """The copy error of fetch_crash_test's check (with
    --tolerate-ticket-14, ticket 14's effects are accepted there)."""
    harness.SERVER = source
    harness.BINSRV = takeover.binsrv
    try:
        harness.FetchCrashMachine.check_complete_copy(None)
    except harness.Failure as error:
        return str(error)
    return None


def served_to_replica(source, replica, takeover: Takeover,
                      pull_port: int) -> dict:
    """Starts the current build's 'pull' on the storage and replicates all
    of it into a fresh replica; the replica's table must match the
    source's."""
    pull = replication.start_pull(takeover.binsrv, pull_port)
    try:
        start = ("SOURCE_AUTO_POSITION=1" if takeover.gtid_mode else
                 "SOURCE_LOG_FILE='binlog.000001', SOURCE_LOG_POS=4")
        replication.replicate_from_pull(replica, pull_port,
                                        takeover.gtid_mode, start)
        expected = replication.checksum(source)
        deadline = time.monotonic() + CATCH_UP_SECONDS
        got = None
        while time.monotonic() < deadline:
            got = replication.checksum(replica)
            if got == expected or pull.poll() is not None:
                break
            time.sleep(1)
        status = replica.sql(
            "SELECT LAST_ERROR_NUMBER, LAST_ERROR_MESSAGE FROM "
            "performance_schema.replication_connection_status").strip()
        replica.sql("STOP REPLICA")
        return {"replica_matches": got == expected, "source": expected,
                "replica": got, "replica_io_error": status[:300],
                "pull_alive": pull.poll() is None}
    finally:
        if pull.poll() is None:
            pull.terminate()
            pull.wait(timeout=120)


def old_config_case(args, source) -> dict:
    takeover = Takeover(args, "old-config")
    takeover.write_configs()
    code, stdout, stderr = takeover.run(args.binsrv, "list",
                                        takeover.old_config)
    message = (stderr + stdout).strip()
    # the next step of an upgrade: the 'instance' section added
    config = json.loads(takeover.old_config.read_text())
    config["instance"] = {
        "local_settings_path": str(takeover.binsrv.local_settings)}
    with_instance = takeover.binsrv.workdir / "old-with-instance.json"
    with_instance.write_text(json.dumps(config, indent=2))
    code_instance, stdout, stderr = takeover.run(args.binsrv, "list",
                                                 with_instance)
    return {"exit": code, "message": message[-400:],
            "names_instance": "instance" in message,
            "with_instance_exit": code_instance,
            "with_instance_message": (stderr + stdout).strip()[-400:],
            "ok": code not in (0, None) and "instance" in message}


def takeover_case(args, source, replica, name: str, first_row: int,
                  **storage) -> dict:
    takeover = Takeover(args, name, **storage)
    takeover.binsrv.checkpoint_size = "16K"
    result = {}
    workload(source, 120, first_row, 30, 800)
    error = fetch_to_the_end(takeover, current=False)
    if error:
        return {"error": error, "ok": False}
    result["release_binlogs"] = len(takeover.binsrv.indexed_binlogs())
    result["query_differences"] = compare_queries(takeover, source)
    workload(source, 120, first_row + 120, 30, 800)
    error = fetch_to_the_end(takeover, current=True)
    if error:
        return {**result, "error": error, "ok": False}
    result["binlogs"] = len(takeover.binsrv.indexed_binlogs())
    copy_error = (complete_copy(source, takeover)
                  if takeover.byte_identical else None)
    result["copy_error"] = copy_error
    served = served_to_replica(source, replica, takeover, args.port + 2)
    result.update(served)
    code, stdout, stderr = takeover.old("list")
    result["downgrade_list"] = (
        f"exit {code}" if code != 0 else
        f"lists {len(query_result((code, stdout, stderr)) or [])} binlogs")
    result["ok"] = (not result["query_differences"] and copy_error is None
                    and served["replica_matches"] and served["pull_alive"])
    if result["ok"] and takeover.binsrv.s3 is not None:
        # the objects live in a shared bucket; a failed case keeps them
        shutil.rmtree(takeover.binsrv.storage, ignore_errors=True)
    return result


def killed_case(args, source, replica, first_row: int) -> dict:
    takeover = Takeover(args, "killed")
    takeover.binsrv.checkpoint_size = "64K"
    workload(source, 1500, first_row, 100, 4000)
    takeover.write_configs()
    process = subprocess.Popen(
        [str(args.old_binsrv), "fetch", str(takeover.old_config)],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        env=dict(os.environ, AWS_EC2_METADATA_DISABLED="true"))
    deadline = time.monotonic() + harness.FETCH_TIMEOUT_SECONDS
    killed_at = None
    while time.monotonic() < deadline and process.poll() is None:
        size = takeover.binsrv.stored_data_size()
        if size > 1_500_000:
            process.send_signal(signal.SIGKILL)
            killed_at = size
            break
        time.sleep(0.05)
    process.wait()
    if killed_at is None:
        return {"error": "the release's fetch ended before it was killed "
                         f"(exit {process.returncode})", "ok": False}
    # what the killed release left beside its index (temporary files, a
    # binlog it had not indexed yet)
    indexed = takeover.binsrv.indexed_binlogs()
    leftovers = sorted(
        path.name for path in takeover.binsrv.storage.iterdir()
        if path.name != "binlog.index" and
        path.name.removesuffix(".json") not in indexed)
    error = fetch_to_the_end(takeover, current=True)
    if error:
        return {"killed_at_bytes": killed_at, "leftovers": leftovers,
                "error": error, "ok": False}
    copy_error = complete_copy(source, takeover)
    return {"killed_at_bytes": killed_at, "leftovers": leftovers,
            "binlogs": len(takeover.binsrv.indexed_binlogs()),
            "copy_error": copy_error, "ok": copy_error is None}


def run(args) -> list:
    work = args.workdir
    gtid_mode = args.mode == "gtid"
    source = harness.MySQLServer(args.mysql_basedir, work / "source",
                                 args.port, gtid_mode, server_id=1)
    replica = harness.MySQLServer(args.mysql_basedir, work / "replica",
                                  args.port + 10, gtid_mode, server_id=10)
    results = []

    def report(name: str, function, *function_args, **keywords) -> None:
        try:
            result = function(*function_args, **keywords)
        except Exception as error:  # reported as the case's failure
            result = {"error": harness.describe_failure(error, 600),
                      "ok": False}
        results.append({"case": name, **result})
        print(json.dumps(results[-1]), flush=True)

    source.start()
    replica.start()
    try:
        cases = args.cases.split(",") if args.cases else (
            ["old_config", "plain_takeover", "encrypted_takeover",
             "killed_takeover"] +
            (["s3_takeover"] if args.s3_bucket_dir else []) +
            (["rewrite_takeover"] if gtid_mode else []))
        if "rewrite_takeover" in cases and not gtid_mode:
            raise SystemExit("rewrite_takeover needs --mode gtid (rewrite "
                             "mode requires GTID replication)")
        for name in cases:
            # every case starts from fresh source binlogs
            source.reset()
            if name == "old_config":
                report(name, old_config_case, args, source)
            elif name == "plain_takeover":
                report(name, takeover_case, args, source, replica, name, 1)
            elif name == "encrypted_takeover":
                report(name, takeover_case, args, source, replica, name, 1,
                       encrypted=True)
            elif name == "s3_takeover":
                report(name, takeover_case, args, source, replica, name, 1,
                       s3=True)
            elif name == "rewrite_takeover":
                report(name, takeover_case, args, source, replica, name, 1,
                       rewrite=True)
            elif name == "killed_takeover":
                report(name, killed_case, args, source, replica, 1)
            else:
                raise SystemExit(f"unknown case {name}")
    finally:
        source.stop()
        replica.stop()
    return results


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--mysql-basedir", type=Path, required=True)
    parser.add_argument("--old-binsrv", type=Path, required=True,
                        help="binlog_server of the previous release")
    parser.add_argument("--binsrv", type=Path, required=True)
    parser.add_argument("--workdir", type=Path, required=True)
    parser.add_argument("--port", type=int, default=23990)
    parser.add_argument("--mode", choices=["position", "gtid"],
                        default="position")
    parser.add_argument("--s3-endpoint", default="127.0.0.1:9000",
                        help="host:port of the S3-compatible server")
    parser.add_argument("--s3-bucket", default="pbs-mtr")
    parser.add_argument("--s3-bucket-dir", type=Path,
                        help="the directory holding the bucket's objects; "
                             "enables s3_takeover")
    parser.add_argument("--s3-access-key", default="pbsaccesskey")
    parser.add_argument("--s3-secret-key", default="pbs/secret+key=1")
    parser.add_argument("--cases", default="",
                        help="comma-separated case names (default: all)")
    parser.add_argument("--tolerate-ticket-14", action="store_true",
                        help="GTID mode: accept a closed binlog stored "
                             "without its closing ROTATE and missing binlogs "
                             "without transactions (ticket 14), as "
                             "fetch_crash_test does")
    args = parser.parse_args()
    if args.tolerate_ticket_14 and args.mode != "gtid":
        parser.error("--tolerate-ticket-14 needs --mode gtid")
    harness.TOLERATE_TICKET_14 = args.tolerate_ticket_14
    args.mysql_basedir = args.mysql_basedir.expanduser()
    args.old_binsrv = args.old_binsrv.expanduser()
    args.binsrv = args.binsrv.expanduser()
    if args.s3_bucket_dir is not None:
        args.s3_bucket_dir = args.s3_bucket_dir.expanduser().resolve()
    # mysqld resolves a relative datadir against its basedir
    args.workdir = args.workdir.expanduser().resolve()
    args.workdir.mkdir(parents=True, exist_ok=True)
    global KEYRING
    KEYRING = args.workdir / "keyring.json"
    keyring = json.loads(KEYRING_SOURCE.read_text())
    keyring["keys"] = [key for key in keyring["keys"] if key["id"] == "alpha"]
    KEYRING.write_text(json.dumps(keyring, indent=2))
    results = run(args)
    if args.tolerate_ticket_14:
        print(json.dumps({"ticket_14_accepted": harness.TICKET_14_HITS}),
              flush=True)
    (args.workdir / "results.json").write_text(json.dumps(results, indent=2))
    return 0 if all(result.get("ok") for result in results) else 1


if __name__ == "__main__":
    sys.exit(main())
