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

"""Binlog metadata of an encrypted storage edited by someone without the key.

The encryption record of a stored binlog (the wrapped file key, the IV, the
cipher and the KEK id) lives in its metadata file, 'binlog.NNNNNN.json',
which nothing authenticates. Someone who can write to the storage (a shared
directory, a bucket) but does not hold the keyring edits the metadata of the
last binlog between two 'fetch' runs; the configuration and the keyring stay
as they are. Cases:

- 'strip_encryption': the 'encryption' member is removed. The next 'fetch'
  resumes that binlog; what it appends must not be readable (ticket 62:
  it is appended in plaintext, and fetch exits 0).
- 'transplant_encryption': the 'encryption' member is replaced by the
  previous binlog's. What the next 'fetch' appends must not be encrypted
  with the previous binlog's keystream (ticket 63: it is, so the keystream
  the previous binlog's known plaintext gives decrypts the appended data).

A refused 'fetch' (a non-zero exit with the binlog left as it was) passes
either case. A JSON line per case, then a summary; exit 1 if any case
failed. Example:

  PYTHONPATH=~/ws/qa/pylib python3 metadata_tamper_test.py \\
    --mysql-basedir ~/ws/dist/mysql-8.4.8-linux-glibc2.28-x86_64-minimal \\
    --binsrv ~/ws/percona-binlog-server-build-debug_gcc14/binlog_server \\
    --workdir ~/ws/qa/metadata-tamper
"""

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import fetch_crash_test as harness  # noqa: E402

KEK_ID = "tamper_kek"
KEYRING = {"version": 1, "keys": [
    {"id": KEK_ID, "cipher": "AES-256-CTR",
     "data_hex": "7A3E91C05D2B48F6A1097C3E5B8D42F0" * 2}]}
ENCRYPTION = {"format": "generic", "kek_id": KEK_ID, "cipher": "AES-256-CTR"}


def write_config(binsrv) -> Path:
    binsrv.write_config()
    config = json.loads(binsrv.config_path.read_text())
    keyring = binsrv.workdir / "keyring.json"
    keyring.write_text(json.dumps(KEYRING))
    config["keyring"] = {"uri": f"file://{keyring}"}
    config["storage"]["encryption"] = dict(ENCRYPTION)
    path = binsrv.workdir / "binsrv-encrypted.json"
    path.write_text(json.dumps(config, indent=2))
    return path


def fetch(binsrv, config: Path) -> "tuple[int, str]":
    try:
        completed = subprocess.run(
            [str(binsrv.binary), "fetch", str(config)], capture_output=True,
            timeout=harness.FETCH_TIMEOUT_SECONDS, check=False,
            env=dict(os.environ, AWS_EC2_METADATA_DISABLED="true"))
    except subprocess.TimeoutExpired:
        return -1, "timeout"
    return completed.returncode, completed.stderr.decode(
        errors="replace")[-400:]


def insert(marker: str, rows: int):
    values = ",".join(f"('{marker}-{row:04d}-{'x' * 64}')"
                      for row in range(rows))
    harness.SERVER.sql(f"INSERT INTO qa.t (payload) VALUES {values}")


def first_fetch(binsrv, config: Path):
    code, stderr = fetch(binsrv, config)
    if code != 0:
        raise harness.Failure(f"the first fetch exited with {code}: "
                              f"{stderr}\n{binsrv.log_tail()}")


def last_errors(binsrv) -> list:
    if not binsrv.log_path.exists():
        return []
    return [line.strip()[-200:] for line in
            binsrv.log_path.read_text(errors="replace").splitlines()
            if "[  error]" in line or "exception caught" in line][-2:]


def case_strip_encryption() -> dict:
    binsrv = harness.BINSRV
    harness.SERVER.reset()
    binsrv.reset()
    config = write_config(binsrv)
    insert("FIRSTMARKER", 20)
    first_fetch(binsrv, config)
    tail = binsrv.indexed_binlogs()[-1]
    data_path = binsrv.storage / tail
    metadata_path = binsrv.storage / (tail + ".json")
    before = data_path.read_bytes()
    metadata = json.loads(metadata_path.read_text())
    if "encryption" not in metadata or b"FIRSTMARKER" in before:
        raise harness.Failure(f"{tail} was not stored encrypted, so the "
                              "case cannot run")
    # the attacker's edit
    del metadata["encryption"]
    metadata_path.write_text(json.dumps(metadata))
    insert("SECONDMARKER", 20)
    code, _ = fetch(binsrv, config)
    after = data_path.read_bytes()
    appended = after[len(before):] if after.startswith(before) else after
    readable = b"SECONDMARKER" in after
    result = {"case": "strip_encryption", "binlog": tail,
              "fetch_exit_after_the_edit": code,
              "appended_bytes": len(after) - len(before),
              "appended_data_readable": readable,
              "last_errors": last_errors(binsrv) if code != 0 else [],
              "ok": not readable}
    if readable:
        result["why"] = (
            f"with the 'encryption' record removed from {tail}.json, fetch "
            f"(exit {code}) appended {len(appended)} bytes to the encrypted "
            "storage in plaintext")
    return result


def case_transplant_encryption() -> dict:
    binsrv = harness.BINSRV
    harness.SERVER.reset()
    binsrv.reset()
    config = write_config(binsrv)
    insert("OLDBINLOG", 60)
    harness.SERVER.sql("FLUSH BINARY LOGS")
    insert("TAILFIRST", 2)
    first_fetch(binsrv, config)
    indexed = binsrv.indexed_binlogs()
    if len(indexed) < 2:
        raise harness.Failure(f"expected two stored binlogs, got {indexed}")
    previous, tail = indexed[-2], indexed[-1]
    previous_cipher = (binsrv.storage / previous).read_bytes()
    previous_plain = harness.SERVER.binlog_as_sent(previous)
    tail_before = (binsrv.storage / tail).read_bytes()
    previous_metadata = json.loads(
        (binsrv.storage / (previous + ".json")).read_text())
    tail_metadata_path = binsrv.storage / (tail + ".json")
    tail_metadata = json.loads(tail_metadata_path.read_text())
    if "encryption" not in previous_metadata or \
            "encryption" not in tail_metadata:
        raise harness.Failure("the binlogs were not stored encrypted, so "
                              "the case cannot run")
    # the attacker's edit
    tail_metadata["encryption"] = previous_metadata["encryption"]
    tail_metadata_path.write_text(json.dumps(tail_metadata))
    insert("TAILSECOND", 10)
    code, _ = fetch(binsrv, config)
    tail_after = (binsrv.storage / tail).read_bytes()
    tail_plain = harness.SERVER.binlog_as_sent(tail)
    start = len(tail_before)
    end = min(len(tail_after), len(previous_cipher), len(previous_plain),
              len(tail_plain))
    # the previous binlog's keystream, from its known plaintext: what an
    # attacker who knows that binlog's content can compute without the key
    keystream = bytes(c ^ p for c, p in zip(previous_cipher[start:end],
                                            previous_plain[start:end]))
    recovered = bytes(c ^ k for c, k in zip(tail_after[start:end], keystream))
    matching = sum(1 for a, b in zip(recovered, tail_plain[start:end])
                   if a == b)
    compared = max(end - start, 0)
    # by chance one byte in 256 matches
    reused = compared >= 64 and matching > compared // 2
    result = {"case": "transplant_encryption", "previous_binlog": previous,
              "binlog": tail, "fetch_exit_after_the_edit": code,
              "binlog_size_before_and_after": [len(tail_before),
                                               len(tail_after)],
              "bytes_compared": compared,
              "bytes_decrypted_with_the_previous_keystream": matching,
              "last_errors": last_errors(binsrv) if code != 0 else [],
              "ok": not reused}
    if reused:
        result["why"] = (
            f"with {previous}'s 'encryption' record copied into {tail}.json, "
            f"fetch (exit {code}) encrypted the {compared} bytes it appended "
            f"with {previous}'s keystream: {matching} of them decrypt with "
            "the keystream that binlog's plaintext gives")
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--mysql-basedir", type=Path, required=True)
    parser.add_argument("--binsrv", type=Path, required=True)
    parser.add_argument("--workdir", type=Path, required=True)
    parser.add_argument("--port", type=int, default=23640)
    parser.add_argument("--mode", choices=["gtid", "position"],
                        default="position")
    args = parser.parse_args()
    args.workdir.mkdir(parents=True, exist_ok=True)
    gtid_mode = args.mode == "gtid"
    harness.SERVER = harness.MySQLServer(args.mysql_basedir.expanduser(),
                                         args.workdir / "mysql", args.port,
                                         gtid_mode, server_id=1)
    harness.BINSRV = harness.BinlogServer(args.binsrv.expanduser(),
                                          args.workdir / "binsrv", args.port,
                                          gtid_mode)
    results = []
    harness.SERVER.start()
    try:
        for case in (case_strip_encryption, case_transplant_encryption):
            try:
                result = case()
            except (harness.Failure, RuntimeError,
                    subprocess.SubprocessError, OSError) as failure:
                result = {"case": case.__name__[len("case_"):], "ok": False,
                          "why": harness.describe_failure(failure, 500)}
            print(json.dumps(result), flush=True)
            results.append(result)
    finally:
        harness.SERVER.stop()
    failures = [result for result in results if not result.get("ok")]
    print(json.dumps({"cases": len(results), "failures": len(failures),
                      "passed": not failures}), flush=True)
    (args.workdir / "result.json").write_text(json.dumps(results, indent=2))
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
