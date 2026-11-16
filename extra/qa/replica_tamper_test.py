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

"""Does data tampered with in the storage reach a MySQL replica of 'pull'?

The other attack tests stop at the storage. Here a real MySQL replica
replicates from 'binlog_server pull' after the stored binlogs were changed
by someone who holds no key, and its data is compared with the source's.

- 'forged_row': an encrypted storage (AES-256-CTR). The source commits one
  row whose value is known. In the stored ciphertext the bytes of that value
  are XORed with (old value XOR new value), and the four bytes of the
  event's CRC32 footer with crc32(delta) XOR crc32(zeros), which keeps the
  checksum valid without the plaintext (CRC32 is affine). The replica must
  not end up with the new value: either 'pull' refuses the binlog, or the
  replica stops with an error (ticket 53: the replica applies the forged
  row, with no error anywhere).
- 'forged_row_bad_checksum': the same change without the footer patch. A
  replica verifies event checksums by default, so it must stop, and its
  data must not change; recorded: whether 'pull' noticed first (it does not,
  ticket 53).
- 'doubled_preamble': the storage a hostile source leaves in ticket 69 (the
  next binlog's preamble and events appended to the binlog that was just
  closed, made with the renaming proxy of hostile_source_test.py), served to
  a replica while the source is down. The replica's data is compared with
  the source's and its errors are recorded; the case does not fail on the
  outcome.

A JSON line per case, then a summary; exit 1 if any case failed. Position
mode only (the replica starts at the first binlog, position 4). Example:

  PYTHONPATH=~/ws/qa/pylib python3 replica_tamper_test.py \\
    --mysql-basedir ~/ws/dist/mysql-8.4.8-linux-glibc2.28-x86_64-minimal \\
    --binsrv ~/ws/percona-binlog-server-build-debug_gcc14/binlog_server \\
    --workdir ~/ws/qa/replica-tamper
"""

import argparse
import json
import os
import signal
import socket
import subprocess
import sys
import time
import zlib
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parent))

import fetch_crash_test as harness  # noqa: E402
import hostile_source_test as hostile  # noqa: E402
import pull_replica_test as replication  # noqa: E402

KEK_ID = "replica_tamper_kek"
KEYRING = {"version": 1, "keys": [
    {"id": KEK_ID, "cipher": "AES-256-CTR",
     "data_hex": "3C9A7E15D0B24F68A1C7093E5B8D42F6" * 2}]}
ENCRYPTION = {"format": "generic", "kek_id": KEK_ID, "cipher": "AES-256-CTR"}
WRITE_ROWS_EVENT = 30
OLD_VALUE = b"HONEST-VALUE-0001" + b"." * 47
NEW_VALUE = b"FORGED-VALUE-9999" + b"." * 47
CATCH_UP_SECONDS = 40


def configure(binsrv, encrypted: bool, pull_port: "int | None" = None) -> Path:
    binsrv.write_config()
    config = json.loads(binsrv.config_path.read_text())
    if encrypted:
        keyring = binsrv.workdir / "keyring.json"
        keyring.write_text(json.dumps(KEYRING))
        config["keyring"] = {"uri": f"file://{keyring}"}
        config["storage"]["encryption"] = dict(ENCRYPTION)
    if pull_port is not None:
        # the source may be down: retry quickly, serve meanwhile
        config["connection"]["connect_timeout"] = 2
        config["replication_source"] = {
            "port": pull_port, "server_id": 142,
            "server_uuid": "00000000-0000-0000-0000-00000000008e",
            "read_timeout": 60, "write_timeout": 60,
            "require_secure_transport": False,
            "auth_rsa_private_key": str(replication.AUTH_RSA_PRIVATE_KEY),
            "authentication": {"user": replication.PULL_USER,
                               "password": replication.PULL_PASSWORD,
                               "plugin": "caching_sha2_password"}}
    path = binsrv.workdir / "binsrv-case.json"
    path.write_text(json.dumps(config, indent=2))
    return path


def fetch(binsrv, config: Path):
    completed = subprocess.run(
        [str(binsrv.binary), "fetch", str(config)], capture_output=True,
        timeout=harness.FETCH_TIMEOUT_SECONDS, check=False,
        env=dict(os.environ, AWS_EC2_METADATA_DISABLED="true"))
    if completed.returncode != 0:
        raise harness.Failure(f"fetch exited with {completed.returncode}\n" +
                              binsrv.log_tail())


def start_pull(binsrv, config: Path, pull_port: int) -> subprocess.Popen:
    pull = subprocess.Popen(
        [str(binsrv.binary), "pull", str(config)],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        env=dict(os.environ, AWS_EC2_METADATA_DISABLED="true"))
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        try:
            socket.create_connection(("127.0.0.1", pull_port),
                                     timeout=1).close()
            return pull
        except OSError:
            if pull.poll() is not None:
                raise harness.Failure("pull exited before it listened\n" +
                                      binsrv.log_tail())
            time.sleep(0.2)
    pull.kill()
    raise harness.Failure("pull did not start listening in 30 s")


def stop_pull(pull: "subprocess.Popen | None"):
    if pull is not None and pull.poll() is None:
        pull.send_signal(signal.SIGTERM)
        try:
            pull.wait(timeout=20)
        except subprocess.TimeoutExpired:
            pull.kill()
            pull.wait()


def replica_state(replica) -> dict:
    """What the replica holds and what its two threads report."""
    try:
        rows = [line for line in replica.sql(
            "SELECT payload FROM qa.t ORDER BY id").splitlines() if line]
        table_error = ""
    except RuntimeError as error:
        # the table is not there until the replica has applied its creation
        rows = []
        table_error = str(error).splitlines()[0][:120]
    try:
        receiver = replica.sql(
            "SELECT SERVICE_STATE, LAST_ERROR_NUMBER, LAST_ERROR_MESSAGE "
            "FROM performance_schema.replication_connection_status"
        ).strip().split("\t")
        applier = replica.sql(
            "SELECT SERVICE_STATE, LAST_ERROR_NUMBER, LAST_ERROR_MESSAGE "
            "FROM performance_schema.replication_applier_status_by_worker "
            "ORDER BY LAST_ERROR_NUMBER DESC LIMIT 1").strip().split("\t")
    except RuntimeError as error:
        receiver = applier = [str(error)[:120]]
    return {"rows": [row[:40] for row in rows[:5]], "row_count": len(rows),
            "table_error": table_error,
            "receiver": [field[:160] for field in receiver],
            "applier": [field[:160] for field in applier]}


def wait_for_replica(replica, done) -> dict:
    """The replica's state once 'done(state)' holds, or after the timeout,
    or once one of its threads has stopped with an error."""
    deadline = time.monotonic() + CATCH_UP_SECONDS
    state = replica_state(replica)
    while time.monotonic() < deadline and not done(state):
        errors = [fields for fields in (state["receiver"], state["applier"])
                  if len(fields) > 1 and fields[1] not in ("0", "")]
        if errors and time.monotonic() > deadline - CATCH_UP_SECONDS + 6:
            break
        time.sleep(1)
        state = replica_state(replica)
    return state


def pull_errors(binsrv) -> list:
    if not binsrv.log_path.exists():
        return []
    return [line.strip()[-170:] for line in
            binsrv.log_path.read_text(errors="replace").splitlines()
            if "[  error]" in line and
            # a client closing its connection is logged as an error (ticket 42)
            "system error caught in session" not in line and
            "establish connection" not in line and
            "did not reach EOF" not in line][-3:]


def case_forged_row(args, source, replica, patch_checksum: bool) -> dict:
    name = "forged_row" if patch_checksum else "forged_row_bad_checksum"
    binsrv = harness.BINSRV
    source.reset()
    binsrv.reset()
    source.sql(f"INSERT INTO qa.t (payload) VALUES ('{OLD_VALUE.decode()}')")
    config = configure(binsrv, encrypted=True)
    fetch(binsrv, config)
    binlog = binsrv.indexed_binlogs()[-1]
    plain = source.binlog_as_sent(binlog)
    stored_path = binsrv.storage / binlog
    stored = bytearray(stored_path.read_bytes())
    if OLD_VALUE in bytes(stored) or len(stored) != len(plain):
        raise harness.Failure(f"{binlog} is not stored encrypted with the "
                              "source's size, so the case cannot run")
    # where the row's value lies: what an attacker who knows the table's
    # layout and the binlog's event sizes can work out
    events = [(offset, size) for offset, code, size in
              harness.parse_events(plain)
              if code == WRITE_ROWS_EVENT and
              OLD_VALUE in plain[offset:offset + size]]
    if len(events) != 1:
        raise harness.Failure("expected one WRITE_ROWS event with the value")
    offset, size = events[0]
    value_at = plain.index(OLD_VALUE, offset)
    delta = bytearray(size - 4)
    for index, (old, new) in enumerate(zip(OLD_VALUE, NEW_VALUE)):
        delta[value_at - offset + index] = old ^ new
        stored[value_at + index] ^= old ^ new
    if patch_checksum:
        # crc32(m ^ d) = crc32(m) ^ crc32(d) ^ crc32(zeros): no key needed
        footer_delta = zlib.crc32(bytes(delta)) ^ zlib.crc32(bytes(size - 4))
        for index, byte in enumerate(footer_delta.to_bytes(4, "little")):
            stored[offset + size - 4 + index] ^= byte
    stored_path.write_bytes(bytes(stored))

    source_rows = [line for line in source.sql(
        "SELECT payload FROM qa.t ORDER BY id").splitlines() if line]
    # the source stays up; pull has nothing new to fetch
    pull = None
    try:
        pull = start_pull(binsrv, configure(binsrv, True, args.port + 2),
                          args.port + 2)
        replication.replicate_from_pull(
            replica, args.port + 2, False,
            f"SOURCE_LOG_FILE='{binlog}', SOURCE_LOG_POS=4")
        state = wait_for_replica(replica, lambda s: s["row_count"] >= 1)
    finally:
        replica.sql("STOP REPLICA")
        stop_pull(pull)
    forged = any(row.startswith(NEW_VALUE[:17].decode())
                 for row in state["rows"])
    result = {"case": name, "binlog": binlog,
              "event_offset_and_size": [offset, size],
              "bytes_changed_in_the_ciphertext":
                  len(OLD_VALUE) + (4 if patch_checksum else 0),
              "source_rows": [row[:40] for row in source_rows],
              "replica": state, "pull_errors": pull_errors(binsrv),
              "pull_refused_the_binlog": bool(pull_errors(binsrv)),
              "replica_has_the_forged_value": forged, "ok": not forged}
    if forged:
        result["why"] = (
            "the replica applied a row whose value was changed in the "
            "encrypted storage without the key ('%s...' for the source's "
            "'%s...'); neither pull nor the replica reported an error" %
            (NEW_VALUE[:17].decode(), OLD_VALUE[:17].decode()))
    return result


def case_doubled_preamble(args, source, replica) -> dict:
    binsrv = harness.BINSRV
    hostile_args = SimpleNamespace(
        mysql_basedir=args.mysql_basedir, port=args.port, storage="file",
        case_insensitive_dir=None)
    made = hostile.run_case("reannounce", hostile_args, args.port + 3)
    doubled = made.get("second_preamble_appended") or []
    if not doubled:
        return {"case": "doubled_preamble", "ok": True,
                "skipped": "the hostile source's rename left no doubled "
                           "preamble (ticket 69 fixed?)",
                "storage": made.get("storage_entries")}
    binlog = doubled[0]
    expected = replication.checksum(source)
    # the source is down while the replica reads: pull cannot resume from it
    # after the rename anyway, and only serves what it has stored
    source.stop()
    pull = None
    try:
        pull = start_pull(binsrv, configure(binsrv, False, args.port + 2),
                          args.port + 2)
        replication.replicate_from_pull(
            replica, args.port + 2, False,
            f"SOURCE_LOG_FILE='{binlog}', SOURCE_LOG_POS=4")
        deadline = time.monotonic() + CATCH_UP_SECONDS
        actual = replication.checksum(replica)
        while actual != expected and time.monotonic() < deadline:
            time.sleep(2)
            actual = replication.checksum(replica)
        state = replica_state(replica)
    finally:
        replica.sql("STOP REPLICA")
        stop_pull(pull)
        source.start()
    return {"case": "doubled_preamble", "binlog": binlog,
            "stored_bytes": (binsrv.storage / binlog).stat().st_size,
            "source_count_and_checksum": expected,
            "replica_count_and_checksum": actual,
            "replica_matches_the_source": actual == expected,
            "replica_receiver": state["receiver"],
            "replica_applier": state["applier"],
            "pull_errors": pull_errors(binsrv),
            # recorded, not judged: the stored binlog is malformed either way
            "ok": True}


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--mysql-basedir", type=Path, required=True)
    parser.add_argument("--binsrv", type=Path, required=True)
    parser.add_argument("--workdir", type=Path, required=True)
    parser.add_argument("--port", type=int, default=23700)
    parser.add_argument("--mode", choices=["position"], default="position")
    parser.add_argument("--cases", nargs="*",
                        default=["forged_row", "forged_row_bad_checksum",
                                 "doubled_preamble"])
    args = parser.parse_args()
    args.mysql_basedir = args.mysql_basedir.expanduser()
    args.workdir.mkdir(parents=True, exist_ok=True)
    source = harness.MySQLServer(args.mysql_basedir, args.workdir / "source",
                                 args.port, False, server_id=1)
    replica = harness.MySQLServer(args.mysql_basedir,
                                  args.workdir / "replica", args.port + 1,
                                  False, server_id=2)
    harness.SERVER = source
    harness.BINSRV = harness.BinlogServer(args.binsrv.expanduser(),
                                          args.workdir / "binsrv", args.port,
                                          False)
    cases = {
        "forged_row": lambda: case_forged_row(args, source, replica, True),
        "forged_row_bad_checksum":
            lambda: case_forged_row(args, source, replica, False),
        "doubled_preamble":
            lambda: case_doubled_preamble(args, source, replica),
    }
    results = []
    source.start()
    replica.start()
    try:
        for name in args.cases:
            try:
                result = cases[name]()
            except (harness.Failure, RuntimeError,
                    subprocess.SubprocessError, OSError) as failure:
                result = {"case": name, "ok": False,
                          "why": harness.describe_failure(failure, 600)}
            print(json.dumps(result), flush=True)
            results.append(result)
    finally:
        replica.stop()
        source.stop()
    failures = [result for result in results if not result.get("ok")]
    print(json.dumps({"cases": len(results), "failures": len(failures),
                      "passed": not failures}), flush=True)
    (args.workdir / "result.json").write_text(json.dumps(results, indent=2))
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
