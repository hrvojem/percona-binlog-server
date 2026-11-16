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
which nothing authenticates; neither are 'binlog.index' and 'metadata.json'.
Someone who can write to the storage (a shared directory, a bucket) but does
not hold the keyring edits one of these files between two 'fetch' runs; the
source gets new rows in between, the configuration and the keyring stay as
they are. Cases:

- 'strip_encryption': the 'encryption' member of the last binlog's metadata
  is removed. The next 'fetch' resumes that binlog; what it appends must not
  be readable (ticket 62: it is appended in plaintext, and fetch exits 0).
- 'transplant_encryption': the 'encryption' member is replaced by the
  previous binlog's. What the next 'fetch' appends must not be encrypted
  with the previous binlog's keystream (ticket 63: it is, so the keystream
  the previous binlog's known plaintext gives decrypts the appended data).
- 'size': 'size' of the last binlog's metadata is lowered to the start of
  its second transaction (a copy of the ciphertext is kept first). Start-up
  recovery truncates the binlog to it. In GTID mode fetch resumes from the
  stored GTID set, which the edit left alone, so the source sends only NEW
  transactions and they land on offsets that already were encrypted once:
  old ciphertext XOR new ciphertext must not equal old plaintext XOR new
  plaintext there. Without 'replication.rewrite' it cannot come to that:
  every event carries its end position, which no longer matches the
  storage's, so fetch stops with an error (after the truncation, and so
  does every later fetch: the case reports that as 'fetch_again'). In
  position mode fetch asks the source for the binlog from the lowered
  position and gets the same bytes again, so the same ciphertext comes
  back and the case passes when the binlog ends as an exact copy.
- 'size_rewrite' (GTID mode only): the same edit on a storage in rewrite
  mode ('replication.rewrite' in the configuration from the start), where
  the Binlog Server sets the event positions itself, so the new
  transactions are written over the cut-off offsets (ticket 63's other
  variant: with the keystream used there before, and fetch exits 0 with
  the cut-off transactions gone). The plaintexts of this case are what
  'pull' serves of the binlog before the edit and after the fetch.
- the sweep (every other case, see SWEEP; 'control' is the sweep without an
  edit): one stored field is edited at a time - each member of the last
  binlog's metadata (size, GTID sets, timestamps, last_sequence_number,
  version, every member of the encryption record, unknown members, the
  whole file), 'binlog.index' and 'metadata.json'. The 'gcm_*' cases run
  with an AES-256-GCM KEK, whose wrapped file key has a tag. After the edit
  'list', 'fetch' and 'list' run, and:
    (a) no command may die on a signal or a sanitizer report, exit with
        anything but 0 or 1, run into its timeout or use more than
        RSS_LIMIT_MB of memory;
    (b) no plaintext marker row may be found in the storage;
    (c) nothing written after the edit may be encrypted with a keystream
        the storage already used (the keystream that a binlog stored before
        the edit and its known plaintext give must not decrypt it);
    (d) the binlogs stored before the edit, other than the last one, keep
        their bytes;
    (e) a clean refusal - fetch exits 1 with an error message and leaves
        every storage file as the edit left it - passes (b) to (f);
    (f) if fetch exits 0, the storage must still serve prefix-consistent
        copies of the source's binlogs: 'pull' is started on it and
        'mysqlbinlog --read-from-remote-server --raw' dumps every binlog,
        each of which must be a prefix of the source's; and the fetch must
        have stored something of what the source got.
  A failed case names the invariants in 'failed_invariants', with the
  numbers in 'why'.

With '--storage s3' the storage is a prefix in a bucket of an S3-compatible
server whose objects are files under '--s3-bucket-dir' (where the checks
read them); the edits are made through the S3 API, as a holder of the
bucket's write access would, and the prefix is deleted at the end.

A JSON line per case, then a summary; exit 1 if any case failed. Example:

  PYTHONPATH=~/ws/qa/pylib python3 metadata_tamper_test.py \\
    --mysql-basedir ~/ws/dist/mysql-8.4.8-linux-glibc2.28-x86_64-minimal \\
    --binsrv ~/ws/percona-binlog-server-build-debug_gcc14/binlog_server \\
    --workdir ~/ws/qa/metadata-tamper
"""

import argparse
import datetime
import hashlib
import hmac
import http.client
import json
import os
import re
import resource
import shutil
import signal
import socket
import subprocess
import sys
import threading
import time
import urllib.parse
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import fetch_crash_test as harness  # noqa: E402

KEK_ID = "tamper_kek"
# other KEKs of the same keyring, for the edits that point a binlog at
# "another existing KEK" and for the cases that need a tag ('gcm_*')
OTHER_KEK_ID = "other_kek"
SMALL_KEK_ID = "small_kek"
GCM_KEK_ID = "gcm_kek"
KEYRING = {"version": 1, "keys": [
    {"id": KEK_ID, "cipher": "AES-256-CTR",
     "data_hex": "7A3E91C05D2B48F6A1097C3E5B8D42F0" * 2},
    {"id": OTHER_KEK_ID, "cipher": "AES-256-CTR",
     "data_hex": "11C4E06A9F3B7D5208E1B6A4C7D93F5E" * 2},
    {"id": SMALL_KEK_ID, "cipher": "AES-128-CTR",
     "data_hex": "5B0D9E2C71A4F6388C1E4D7A9B3F6021"},
    {"id": GCM_KEK_ID, "cipher": "AES-256-GCM",
     "data_hex": "C3A1F07E2D4B5968B7E6D5C4A3F2E1D0" * 2}]}
ENCRYPTION = {"format": "generic", "kek_id": KEK_ID, "cipher": "AES-256-CTR"}

# the rows the cases insert start with one of these
MARKERS = (b"OLDBINLOG", b"MIDBINLOG", b"TAILFIRST", b"SECONDMARKER",
           b"FIRSTMARKER", b"TAILSECOND")
LIST_TIMEOUT_SECONDS = 60
SWEEP_FETCH_TIMEOUT_SECONDS = 90
DUMP_TIMEOUT_SECONDS = 30
# invariant (a): more than this is "absurd" for a storage of a few kilobytes
# (the debug build peaks near 60 MB here, the ASan build near 250 MB); a
# process that reaches RSS_KILL_MB is killed
RSS_LIMIT_MB = 1024
RSS_KILL_MB = 4096
PULL_USER = "tamper"
PULL_PASSWORD = "tamper_password"
# the sample key of the MTR suite: the listener of 'pull' always does
# caching_sha2_password full authentication, which over a non-TLS connection
# needs its RSA key
AUTH_RSA_PRIVATE_KEY = (Path(__file__).resolve().parents[2] / "mtr" /
                        "binlog_streaming" / "std_data" /
                        "caching_sha2_rsa_priv.pem")
S3_REGION = "us-east-1"
# set by main()
MYSQLBINLOG: Path = None
PULL_PORT = 0


def write_config(binsrv, kek_id: str = KEK_ID) -> Path:
    binsrv.write_config()
    config = json.loads(binsrv.config_path.read_text())
    keyring = binsrv.workdir / "keyring.json"
    keyring.write_text(json.dumps(KEYRING))
    config["keyring"] = {"uri": f"file://{keyring}"}
    config["storage"]["encryption"] = dict(ENCRYPTION, kek_id=kek_id)
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


# ---------------------------------------------------------------------------
# the storage, as the attacker and the checks see it
# ---------------------------------------------------------------------------

def s3_request(s3, method: str, key: str, body: bytes = b"") -> None:
    """One signed (SigV4) path-style S3 request on an object of the
    storage prefix."""
    path = "/" + urllib.parse.quote(f"{s3.bucket}/{s3.prefix}/{key}",
                                    safe="/")
    now = datetime.datetime.now(datetime.timezone.utc)
    amz_date = now.strftime("%Y%m%dT%H%M%SZ")
    date = now.strftime("%Y%m%d")
    payload_hash = hashlib.sha256(body).hexdigest()
    headers = {"host": s3.endpoint, "x-amz-content-sha256": payload_hash,
               "x-amz-date": amz_date}
    signed = ";".join(sorted(headers))
    canonical = "\n".join(
        [method, path, "",
         "".join(f"{name}:{headers[name]}\n" for name in sorted(headers)),
         signed, payload_hash])
    scope = f"{date}/{S3_REGION}/s3/aws4_request"
    to_sign = "\n".join(["AWS4-HMAC-SHA256", amz_date, scope,
                         hashlib.sha256(canonical.encode()).hexdigest()])
    key_bytes = ("AWS4" + s3.secret_key).encode()
    for part in (date, S3_REGION, "s3", "aws4_request"):
        key_bytes = hmac.new(key_bytes, part.encode(),
                             hashlib.sha256).digest()
    signature = hmac.new(key_bytes, to_sign.encode(),
                         hashlib.sha256).hexdigest()
    headers["Authorization"] = (
        f"AWS4-HMAC-SHA256 Credential={s3.access_key}/{scope}, "
        f"SignedHeaders={signed}, Signature={signature}")
    connection = http.client.HTTPConnection(s3.endpoint, timeout=60)
    try:
        connection.request(method, path, body=body, headers=headers)
        response = connection.getresponse()
        text = response.read()
    finally:
        connection.close()
    if response.status not in (200, 204):
        raise harness.Failure(f"S3 {method} {key}: HTTP {response.status} "
                              f"{text[:200]!r}")


def put_object(binsrv, name: str, data: bytes) -> None:
    """The attacker's write: a file of the storage directory, or an S3 PUT
    (a file written under the bucket directory would keep the checksums the
    S3 server stored for the object it replaces)."""
    if binsrv.s3 is not None:
        s3_request(binsrv.s3, "PUT", name, data)
    else:
        (binsrv.storage / name).write_bytes(data)


def delete_object(binsrv, name: str) -> None:
    if binsrv.s3 is not None:
        s3_request(binsrv.s3, "DELETE", name)
    else:
        (binsrv.storage / name).unlink()


def snapshot(binsrv) -> dict:
    """The content of every storage object."""
    result = {}
    if not binsrv.storage.is_dir():
        return result
    for path in sorted(binsrv.storage.rglob("*")):
        if path.is_file():
            result[str(path.relative_to(binsrv.storage))] = path.read_bytes()
    return result


def data_files(files: dict) -> dict:
    return {name: data for name, data in files.items()
            if re.fullmatch(r"binlog\.\d{6}", name)}


def xor(left: bytes, right: bytes) -> bytes:
    return bytes(a ^ b for a, b in zip(left, right))


def first_difference(left: bytes, right: bytes) -> int:
    """The length of the common prefix."""
    common = min(len(left), len(right))
    if left[:common] == right[:common]:
        return common
    return next(index for index in range(common)
                if left[index] != right[index])


def describe_changes(old: dict, new: dict) -> list:
    changes = []
    for name in sorted(set(old) | set(new)):
        if name not in new:
            changes.append(f"{name} removed")
        elif name not in old:
            changes.append(f"{name} created ({len(new[name])} bytes)")
        elif old[name] != new[name]:
            changes.append(
                f"{name} {len(old[name])} -> {len(new[name])} bytes, same "
                f"up to {first_difference(old[name], new[name])}")
    return changes


# ---------------------------------------------------------------------------
# running binlog_server
# ---------------------------------------------------------------------------

def binsrv_environment() -> dict:
    environment = dict(os.environ, AWS_EC2_METADATA_DISABLED="true")
    environment.setdefault("ASAN_OPTIONS", "abort_on_error=1:exitcode=134")
    return environment


def watch_memory(process: subprocess.Popen, peak: list) -> None:
    """Records the peak resident set size (MB) of 'process' and kills it at
    RSS_KILL_MB."""
    status = Path(f"/proc/{process.pid}/status")
    while process.poll() is None:
        try:
            for line in status.read_text().splitlines():
                if line.startswith("VmHWM:"):
                    peak[0] = max(peak[0], int(line.split()[1]) // 1024)
        except (OSError, ValueError, IndexError):
            pass
        if peak[0] >= RSS_KILL_MB:
            process.kill()
            return
        time.sleep(0.02)


def run_command(binsrv, command: str, config: Path, timeout: float,
                label: str) -> dict:
    """Runs 'binlog_server <command>' and returns what invariant (a) needs:
    the exit code, whether it timed out, its peak memory, any sanitizer
    report (saved to a file in the work directory) and its error text."""
    # binlog_server starts its log file anew every time it logs to it
    log_before = (binsrv.log_path.stat().st_mtime_ns
                  if binsrv.log_path.exists() else None)
    children_before = resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss
    started = time.monotonic()
    process = subprocess.Popen([str(binsrv.binary), command, str(config)],
                               stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE,
                               env=binsrv_environment())
    peak = [0]
    watcher = threading.Thread(target=watch_memory, args=(process, peak),
                               daemon=True)
    watcher.start()
    timed_out = False
    try:
        stdout, stderr = process.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        timed_out = True
        process.kill()
        stdout, stderr = process.communicate()
    watcher.join(timeout=5)
    children_after = resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss
    if children_after > children_before:
        # the largest child so far is this one
        peak[0] = max(peak[0], children_after // 1024)
    stderr_text = stderr.decode(errors="replace")
    stdout_text = stdout.decode(errors="replace")
    log_text = ""
    if binsrv.log_path.exists() and \
            binsrv.log_path.stat().st_mtime_ns != log_before:
        log_text = binsrv.log_path.read_text(errors="replace")
    errors = [line.strip()[-300:] for line in log_text.splitlines()
              if "[  error]" in line or "exception caught" in line]
    message = ""
    status = None
    try:
        answer = json.loads(stdout_text)
        status = answer.get("status")
        message = str(answer.get("message", ""))[:300]
    except (ValueError, AttributeError):
        pass
    if not message and errors:
        # without the timestamp and the severity
        message = errors[-1].split("] ", 2)[-1]
    if not message:
        message = stderr_text.strip()[-300:]
    result = {"exit": process.returncode,
              "seconds": round(time.monotonic() - started, 2),
              "peak_rss_mb": peak[0], "message": message}
    if command != "list" and log_text:
        result["log_end"] = [line.split("] ", 1)[-1][:200]
                             for line in log_text.splitlines()[-2:]]
    if status is not None:
        result["status"] = status
        if command == "list" and isinstance(answer.get("result"), list):
            result["binlogs_listed"] = [
                f"{entry.get('name')}:{entry.get('size')}"
                for entry in answer["result"] if isinstance(entry, dict)]
    if timed_out:
        result["timed_out_after_seconds"] = timeout
    sanitizer = [marker for marker in ("AddressSanitizer", "LeakSanitizer",
                                       "UndefinedBehaviorSanitizer",
                                       "runtime error:", "Assertion")
                 if marker in stderr_text]
    if sanitizer or process.returncode not in (0, 1):
        report = binsrv.workdir.parent / f"abnormal-{label}.txt"
        report.write_text(f"binlog_server {command}: exit "
                          f"{process.returncode}\n--- stderr\n{stderr_text}"
                          f"\n--- stdout\n{stdout_text[-4000:]}"
                          f"\n--- log\n{log_text[-8000:]}")
        result["report_file"] = str(report)
        result["stderr_tail"] = stderr_text.strip()[-600:]
    if sanitizer:
        result["sanitizer"] = sanitizer
    return result


def command_violation(name: str, result: dict) -> "str | None":
    """Invariant (a) for one command."""
    code = result["exit"]
    if result.get("timed_out_after_seconds"):
        return (f"{name} did not finish in "
                f"{result['timed_out_after_seconds']} s")
    if result.get("sanitizer"):
        return (f"{name} (exit {code}) printed a "
                f"{'/'.join(result['sanitizer'])} report, see "
                f"{result.get('report_file')}")
    if code < 0:
        return (f"{name} died on signal {-code} "
                f"({signal.Signals(-code).name}): "
                f"{result.get('stderr_tail', '')[-200:]}")
    if code not in (0, 1):
        return (f"{name} exited with {code}: "
                f"{result.get('stderr_tail', '')[-200:]}")
    if result["peak_rss_mb"] > RSS_LIMIT_MB:
        return (f"{name} used {result['peak_rss_mb']} MB of memory (limit "
                f"{RSS_LIMIT_MB} MB)")
    return None


def serve_and_dump(binsrv, config: Path, first_binlog: str,
                   label: str) -> dict:
    """Starts 'pull' on the storage and dumps the binlogs it serves, from
    'first_binlog' to the last one, with 'mysqlbinlog --raw'. Returns the
    served files and how 'pull' and mysqlbinlog ended."""
    pull_config = json.loads(config.read_text())
    pull_config["replication_source"] = {
        "port": PULL_PORT, "server_id": 142,
        "server_uuid": "00000000-0000-0000-0000-00000000008e",
        "read_timeout": 5, "write_timeout": 5,
        "require_secure_transport": False,
        "auth_rsa_private_key": str(AUTH_RSA_PRIVATE_KEY),
        "authentication": {"user": PULL_USER, "password": PULL_PASSWORD,
                           "plugin": "caching_sha2_password"}}
    pull_config_path = binsrv.workdir / "binsrv-pull.json"
    pull_config_path.write_text(json.dumps(pull_config, indent=2))
    dump_directory = binsrv.workdir / "dump"
    shutil.rmtree(dump_directory, ignore_errors=True)
    dump_directory.mkdir(parents=True)
    pull_stderr_path = binsrv.workdir / "pull.stderr"
    result = {"served": {}, "pull_exit": None, "dump_exit": None,
              "dump_error": "", "pull_error": ""}
    with pull_stderr_path.open("wb") as pull_stderr:
        pull = subprocess.Popen(
            [str(binsrv.binary), "pull", str(pull_config_path)],
            stdout=subprocess.DEVNULL, stderr=pull_stderr,
            env=binsrv_environment())
        try:
            deadline = time.monotonic() + 30
            listening = False
            while time.monotonic() < deadline and pull.poll() is None:
                try:
                    socket.create_connection(("127.0.0.1", PULL_PORT),
                                             timeout=1).close()
                    listening = True
                    break
                except OSError:
                    time.sleep(0.1)
            if listening:
                try:
                    dump = subprocess.run(
                        [str(MYSQLBINLOG), "--no-defaults",
                         "--read-from-remote-server", "--host=127.0.0.1",
                         f"--port={PULL_PORT}", f"--user={PULL_USER}",
                         f"--password={PULL_PASSWORD}",
                         "--default-auth=caching_sha2_password",
                         "--get-server-public-key", "--ssl-mode=DISABLED",
                         "--raw", "--to-last-log",
                         f"--result-file={dump_directory}/", first_binlog],
                        capture_output=True, timeout=DUMP_TIMEOUT_SECONDS,
                        check=False)
                    result["dump_exit"] = dump.returncode
                    result["dump_error"] = "; ".join(
                        line for line in dump.stderr.decode(
                            errors="replace").splitlines()
                        if "[Warning]" not in line)[-300:]
                except subprocess.TimeoutExpired:
                    result["dump_exit"] = "timeout"
                    result["dump_error"] = (f"mysqlbinlog did not finish in "
                                            f"{DUMP_TIMEOUT_SECONDS} s")
            # 'pull' is expected to be still running: an exit by now is its
            # own (1 for a refused storage, a signal for a crash)
            result["pull_exit"] = pull.poll()
        finally:
            if pull.poll() is None:
                pull.send_signal(signal.SIGTERM)
                try:
                    # with an idle source 'pull' may take up to
                    # connection.read_timeout to notice (ticket 43)
                    pull.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    pull.kill()
                    pull.wait()
    pull_stderr_text = pull_stderr_path.read_text(errors="replace")
    sanitizer = [marker for marker in ("AddressSanitizer",
                                       "UndefinedBehaviorSanitizer",
                                       "runtime error:", "Assertion")
                 if marker in pull_stderr_text]
    if sanitizer or (result["pull_exit"] not in (None, 0, 1)):
        report = binsrv.workdir.parent / f"abnormal-{label}-pull.txt"
        report.write_text(pull_stderr_text + "\n--- log\n" +
                          binsrv.log_tail(80))
        result["pull_report_file"] = str(report)
        result["pull_sanitizer"] = sanitizer
    if result["pull_exit"] is not None:
        result["pull_error"] = (last_errors(binsrv) or
                                [pull_stderr_text.strip()[-300:]])[-1]
    for path in sorted(dump_directory.iterdir()):
        result["served"][path.name] = path.read_bytes()
    return result



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
    put_object(binsrv, tail + ".json", json.dumps(metadata).encode())
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
    put_object(binsrv, tail + ".json", json.dumps(tail_metadata).encode())
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


# ---------------------------------------------------------------------------
# the sweep: one edit, then list / fetch / list and the invariants
# ---------------------------------------------------------------------------

class Environment:
    """A source with three binlogs (the last one holds three transactions)
    and an encrypted storage that 'fetch' has just filled from it."""

    def __init__(self, kek_id: str = KEK_ID):
        self.binsrv = binsrv = harness.BINSRV
        harness.SERVER.reset()
        binsrv.reset()
        self.config = write_config(binsrv, kek_id)
        insert("OLDBINLOG", 60)
        harness.SERVER.sql("FLUSH BINARY LOGS")
        insert("MIDBINLOG", 20)
        harness.SERVER.sql("FLUSH BINARY LOGS")
        for _ in range(3):
            insert("TAILFIRST", 8)
        first_fetch(binsrv, self.config)
        self.gtid_mode = binsrv.gtid_mode
        self.uuid = harness.SERVER.sql("SELECT @@server_uuid").strip()
        self.names = binsrv.indexed_binlogs()
        if len(self.names) != 3:
            raise harness.Failure(f"expected three stored binlogs, got "
                                  f"{self.names}")
        self.tail = self.names[-1]
        self.before = snapshot(binsrv)
        self.meta = json.loads(self.before[self.tail + ".json"])
        self.size = len(self.before[self.tail])
        if "encryption" not in self.meta or self.meta["size"] != self.size \
                or any(marker in data for marker in MARKERS
                       for data in self.before.values()):
            raise harness.Failure("the binlogs were not stored encrypted, "
                                  "so the case cannot run")
        # what the source holds of every stored binlog: the plaintext of
        # the stored ciphertext
        self.plain_before = {name: harness.SERVER.binlog_as_sent(name)
                             for name in self.names}
        # the keystream reuse check also tries these distances between a
        # file offset and the offset of the keystream used for it
        self.shifts = [0]

    def put(self, name: str, data) -> None:
        if isinstance(data, (dict, list)):
            data = json.dumps(data, separators=(",", ":"))
        if isinstance(data, str):
            data = data.encode()
        put_object(self.binsrv, name, data)

    def put_tail_meta(self, data) -> None:
        self.put(self.tail + ".json", data)

    def second_transaction_start(self) -> int:
        """Where the second transaction of the last binlog starts."""
        plain = self.plain_before[self.tail][:self.size]
        starts = [offset for offset, type_code, _ in
                  harness.parse_events(plain)
                  if type_code in harness.TRANSACTION_START_EVENTS]
        if len(starts) < 3:
            raise harness.Failure(f"{self.tail} holds {len(starts)} "
                                  "transactions, expected three")
        return starts[1]


def default_workload(_environment) -> None:
    """The rows the source gets between the two fetch runs: two
    transactions."""
    insert("SECONDMARKER", 10)
    insert("SECONDMARKER", 10)


def find_plaintext(files: dict) -> list:
    """Invariant (b)."""
    found = []
    for name, data in sorted(files.items()):
        for marker in MARKERS:
            count = data.count(marker)
            if count:
                found.append(f"{count} '{marker.decode()}' row(s) readable "
                             f"in {name} (first at offset "
                             f"{data.find(marker)} of {len(data)})")
    return found


def find_keystream_reuse(before: dict, after: dict, plain_before: dict,
                         plain_after: dict, shifts: list) -> list:
    """Invariant (c). For every binlog stored before the edit, its
    ciphertext and its plaintext (the source's binlog) give the keystream
    it was encrypted with. If that keystream turns what was written after
    the edit into marker rows, the storage used it twice. Returns a
    description per reuse, with the number of bytes that decrypt to the
    source's bytes."""
    found = []
    all_plain = b"".join(plain_after.values())
    for new_name, new in sorted(data_files(after).items()):
        written_from = first_difference(new, before.get(new_name, b""))
        if written_from >= len(new):
            continue
        for old_name, old in sorted(data_files(before).items()):
            old_plain = plain_before.get(old_name, b"")
            if len(old_plain) < len(old):
                continue
            keystream = xor(old, old_plain)
            for shift in shifts:
                start = max(written_from, shift)
                length = min(len(new) - start, len(keystream) -
                             (start - shift))
                if length < 64:
                    continue
                recovered = xor(new[start:start + length],
                                keystream[start - shift:])
                positions = [recovered.find(marker) for marker in MARKERS
                             if marker in recovered]
                if not positions:
                    continue
                # where the recovered rows are in the source's binlogs
                position = min(positions)
                where = all_plain.find(recovered[position:position + 32])
                matching = sum(
                    1 for a, b in zip(recovered, all_plain[
                        max(where - position, 0):]) if a == b) \
                    if where >= position else recovered.count(b"x")
                found.append(
                    f"{matching} of the {length} bytes at offset {start} of "
                    f"{new_name}, written after the edit, decrypt to the "
                    f"source's bytes (marker rows included) with the "
                    f"keystream that {old_name}'s old ciphertext and "
                    f"plaintext give"
                    + (f" for the offset {shift} bytes lower" if shift
                       else ""))
    return found


def check_served(environment, dump: dict, plain_after: dict) -> tuple:
    """Invariant (f): returns (failures, numbers)."""
    failures = []
    numbers = {}
    if dump["pull_exit"] is not None:
        failures.append(f"pull exited with {dump['pull_exit']} on the "
                        f"storage: {dump['pull_error'][-200:]}")
    if dump["dump_exit"] != 0:
        failures.append(f"mysqlbinlog ended with {dump['dump_exit']}: "
                        f"{dump['dump_error'][-200:]}")
    for name in sorted(set(environment.names) | set(dump["served"])):
        source = plain_after.get(name)
        served = dump["served"].get(name)
        if served is None:
            numbers[name] = "not served"
            failures.append(f"{name}, stored before the edit, is not "
                            "served")
            continue
        if source is None:
            numbers[name] = f"{len(served)} bytes, not on the source"
            failures.append(f"{name} is served but the source has no such "
                            "binlog")
            continue
        same = first_difference(served, source)
        numbers[name] = (f"served {len(served)}, source {len(source)}, "
                         f"same up to {same}")
        if same < len(served):
            failures.append(
                f"{name} as served ({len(served)} bytes) differs from the "
                f"source's ({len(source)} bytes) from offset {same}")
        elif len(served) < len(environment.before.get(name, b"")):
            numbers[name] += (f", shorter than the "
                              f"{len(environment.before[name])} bytes "
                              f"stored before the edit")
    return failures, numbers


def run_sweep_case(name: str, spec: dict) -> dict:
    environment = Environment(spec.get("kek", KEK_ID))
    binsrv = environment.binsrv
    before = environment.before
    edit = spec["edit"](environment)
    edited = snapshot(binsrv)
    result = {"case": name, "edit": edit,
              "binlog": environment.tail, "storage_objects_changed_by_the_"
              "edit": describe_changes(before, edited)}
    if not edit.startswith("none") and edited == before:
        raise harness.Failure(f"the edit ({edit}) changed nothing")
    failed = {}

    def violation(invariant: str, text: str) -> None:
        failed.setdefault(invariant, []).append(text)

    commands = {}
    commands["list"] = run_command(binsrv, "list", environment.config,
                                   LIST_TIMEOUT_SECONDS, f"{name}-list1")
    spec.get("workload", default_workload)(environment)
    commands["fetch"] = run_command(binsrv, "fetch", environment.config,
                                    SWEEP_FETCH_TIMEOUT_SECONDS,
                                    f"{name}-fetch")
    after = snapshot(binsrv)
    commands["list_again"] = run_command(binsrv, "list", environment.config,
                                         LIST_TIMEOUT_SECONDS,
                                         f"{name}-list2")
    after_list = snapshot(binsrv)
    result.update(commands)
    for command, outcome in commands.items():
        text = command_violation(command, outcome)
        if text:
            violation("a", text)
    if after_list != after:
        result["storage_objects_changed_by_list"] = describe_changes(
            after, after_list)
    plain_after = {binlog: harness.SERVER.binlog_as_sent(binlog)
                   for binlog in harness.SERVER.binlog_names()}
    fetch_exit = commands["fetch"]["exit"]
    changes = describe_changes(edited, after)
    result["storage_objects_changed_by_fetch"] = changes
    tail_after = after.get(environment.tail, b"")
    result["binlog_size_before_and_after"] = [environment.size,
                                              len(tail_after)]
    clean_refusal = (fetch_exit == 1 and not changes and
                     bool(commands["fetch"]["message"]))
    result["fetch_outcome"] = (
        "clean refusal" if clean_refusal else
        "refusal that changed the storage" if fetch_exit == 1 else
        "resumed" if fetch_exit == 0 else "abnormal")
    if not clean_refusal:
        for text in find_plaintext(after):
            violation("b", text)
        for text in find_keystream_reuse(before, after,
                                         environment.plain_before,
                                         plain_after, environment.shifts):
            violation("c", text)
        for binlog in environment.names[:-1]:
            if after.get(binlog) != before[binlog]:
                violation("d", f"{binlog}, stored before the edit: " + (
                    "removed" if binlog not in after else
                    f"{len(before[binlog])} -> {len(after[binlog])} bytes, "
                    f"same up to "
                    f"{first_difference(before[binlog], after[binlog])}"))
        if fetch_exit == 0:
            grown = sum(len(data) for data in plain_after.values()) - \
                sum(len(data) for data in environment.plain_before.values())
            stored = sum(
                max(len(data) - len(edited.get(binlog, b"")), 0)
                for binlog, data in data_files(after).items())
            result["bytes_the_source_got_and_bytes_stored"] = [grown, stored]
            if grown and stored <= 0:
                violation("f", f"fetch exited 0 but stored nothing: the "
                          f"source's binlogs got {grown} bytes after the "
                          f"first fetch, the stored ones {stored} (the end "
                          f"of its log: {commands['fetch'].get('log_end')})")
            indexed = [binlog for binlog in binsrv.indexed_binlogs()
                       if re.fullmatch(r"binlog\.\d{6}", binlog)]
            dump = serve_and_dump(binsrv, environment.config,
                                  indexed[0] if indexed else
                                  environment.names[0], name)
            failures, numbers = check_served(environment, dump, plain_after)
            result["served"] = numbers
            result["served_copies_are_prefixes_of_the_source's"] = \
                not failures
            for text in failures:
                violation("f", text)
            if dump["pull_exit"] not in (None, 0, 1) or \
                    dump.get("pull_sanitizer"):
                violation("a", f"pull ended with {dump['pull_exit']} "
                          f"{dump.get('pull_sanitizer') or ''}, see "
                          f"{dump.get('pull_report_file')}")
    if "check" in spec:
        spec["check"](environment, result, after, plain_after, violation)
    result["failed_invariants"] = sorted(failed)
    result["ok"] = not failed
    if failed:
        result["why"] = "; ".join(
            f"({invariant}) {text}" for invariant in sorted(failed)
            for text in failed[invariant])
    return result


# -- the edits --------------------------------------------------------------

REMOVE = object()


def tail_member(path: str, value, description: str):
    """An edit that sets (or with REMOVE, removes) one member of the last
    binlog's metadata; 'path' separates nested member names with '/', and
    'value' may be a function of the environment."""
    def edit(environment) -> str:
        meta = json.loads(json.dumps(environment.meta))
        parent = meta
        *parents, member = path.split("/")
        for step in parents:
            parent = parent[step]
        new = value(environment) if callable(value) else value
        old = parent.get(member, "<absent>")
        if new is REMOVE:
            del parent[member]
        else:
            parent[member] = new
        environment.put_tail_meta(meta)
        shown = "<removed>" if new is REMOVE else json.dumps(new)
        return (f"{environment.tail}.json: '{path}' {json.dumps(old)[:70]} "
                f"-> {shown[:90]}: {description}")
    return edit


def tail_file(make, description: str):
    """An edit that replaces the last binlog's metadata file ('make'
    returns the new content, or None to delete it)."""
    def edit(environment) -> str:
        data = make(environment)
        if data is None:
            delete_object(environment.binsrv, environment.tail + ".json")
        else:
            environment.put_tail_meta(data)
        return f"{environment.tail}.json: {description}"
    return edit


def index_lines(change, description: str):
    def edit(environment) -> str:
        lines = environment.before["binlog.index"].decode().splitlines()
        new = change(list(lines), environment)
        environment.put("binlog.index", "".join(f"{line}\n"
                                                for line in new))
        return f"binlog.index: {description}: {lines} -> {new}"
    return edit


def storage_member(member: str, value, description: str):
    def edit(environment) -> str:
        meta = json.loads(environment.before["metadata.json"])
        old = meta.get(member, "<absent>")
        if value is REMOVE:
            del meta[member]
        else:
            meta[member] = value(meta) if callable(value) else value
        environment.put("metadata.json", meta)
        return (f"metadata.json: '{member}' {json.dumps(old)} -> "
                f"{json.dumps(meta.get(member, '<removed>'))}: "
                f"{description}")
    return edit


def flipped(hex_text: str) -> str:
    """The same bytes with every bit of the first one inverted."""
    return f"{int(hex_text[:2], 16) ^ 0xFF:02X}" + hex_text[2:]


def gtid_value(kind: str):
    """A value for 'previous_gtids' / 'added_gtids'."""
    def value(environment) -> str:
        last = 0
        if environment.gtid_mode:
            last = int(re.split(r"[:-]", environment.meta["added_gtids"])[-1])
        return {
            "emptied": "",
            # the next transaction of the source, which the storage does
            # not hold
            "extended": (f"{environment.uuid}:1-{last + 1}"
                         if environment.gtid_mode else
                         f"{environment.uuid}:1-5"),
            "extended_far": f"{environment.uuid}:1-1000000",
            "malformed": "not-a-uuid:1-x",
            "other_uuid": "11111111-2222-3333-4444-555555555555:1-5",
        }[kind]
    return value


def shifted_timestamps(environment) -> dict:
    form = "%Y-%m-%dT%H:%M:%S"
    low = datetime.datetime.strptime(environment.meta["min_timestamp"], form)
    high = datetime.datetime.strptime(environment.meta["max_timestamp"], form)
    meta = dict(environment.meta)
    meta["min_timestamp"] = (high + datetime.timedelta(days=1)).strftime(form)
    meta["max_timestamp"] = (low - datetime.timedelta(days=1)).strftime(form)
    return meta


def lowered_counter(environment) -> str:
    """The data IV with its block counter (the low 8 bytes, to which the
    storage adds offset / 16) lowered by size / 16: what is appended at the
    end of the binlog then gets the keystream of the start of the binlog,
    whose plaintext (the binlog magic and the FORMAT_DESCRIPTION event) is
    close to public."""
    iv = environment.meta["encryption"]["file_data_envelope"]["iv_hex"]
    blocks = environment.size // 16
    environment.shifts.append(blocks * 16)
    counter = (int(iv[16:], 16) - blocks) % (1 << 64)
    return iv[:16] + f"{counter:016X}"


def size_edit(environment) -> str:
    lowered = environment.second_transaction_start()
    environment.lowered_size = lowered
    meta = dict(environment.meta, size=lowered)
    environment.put_tail_meta(meta)
    return (f"{environment.tail}.json: 'size' {environment.size} -> "
            f"{lowered} (the start of the binlog's second transaction; the "
            f"GTID sets are left as they were)")


def size_workload(_environment) -> None:
    # three transactions, so that the new data is longer than what the
    # lowered size cuts off
    for _ in range(3):
        insert("SECONDMARKER", 10)


def size_check(environment, result, after, plain_after, violation) -> None:
    """The XOR numbers of the 'size' case: over the offsets that were
    encrypted before the edit and written again after it, old ciphertext
    XOR new ciphertext against old plaintext XOR new plaintext, where the
    new plaintext is what the source holds past the old end of the binlog
    (GTID mode: only new transactions are sent) or at the same offsets
    (position mode: the same bytes are sent again)."""
    tail = environment.tail
    lowered = environment.lowered_size
    old_cipher = environment.before[tail]
    new_cipher = after.get(tail, b"")
    old_plain = environment.plain_before[tail]
    source = plain_after[tail]
    end = min(len(old_cipher), len(new_cipher))
    overlap = max(end - lowered, 0)
    ciphertext_xor = xor(old_cipher[lowered:end], new_cipher[lowered:end])
    differing = sum(1 for byte in ciphertext_xor if byte)
    # where only new transactions are sent (GTID mode), the byte at
    # lowered + i is the source's at old size + i
    moved = xor(old_plain[lowered:end], source[environment.size:])
    matching_moved = sum(1 for a, b in zip(ciphertext_xor, moved) if a == b)
    recovered = xor(ciphertext_xor, old_plain[lowered:end])
    same_bytes_again = new_cipher[:end] == old_cipher[:end] and \
        result.get("served_copies_are_prefixes_of_the_source's", False)
    result["size_case"] = {
        "size_before": environment.size, "size_set_by_the_edit": lowered,
        "size_after_fetch": len(new_cipher),
        "overlap_bytes": overlap,
        "overlap_bytes_whose_ciphertext_changed": differing,
        "overlap_bytes_where_ciphertext_xor_equals_old_plaintext_xor_the_"
        "source's_next_bytes": matching_moved if differing else 0,
        "marker_rows_recovered_from_the_overlap_with_the_old_plaintext":
            recovered.count(b"SECONDMARKER")}
    if differing == 0 and overlap:
        result["size_case"]["note"] = (
            "the same ciphertext came back at every offset written again"
            + (" and the binlog is a copy of the source's: the source sent "
               "the same bytes again, and the same bytes encrypted with "
               "the same keystream reveal nothing"
               if same_bytes_again else ""))
    if result["fetch"]["exit"] == 1:
        # does the storage recover on its own?
        again = run_command(environment.binsrv, "fetch", environment.config,
                            SWEEP_FETCH_TIMEOUT_SECONDS,
                            "size-fetch-again")
        result["fetch_again"] = again
        text = command_violation("the second fetch", again)
        if text:
            violation("a", text)
    reused = overlap >= 64 and differing and matching_moved > overlap // 2
    if reused:
        violation("c", (
            f"with 'size' lowered from {environment.size} to {lowered}, "
            f"fetch (exit {result['fetch']['exit']}) truncated {tail} and "
            f"wrote new transactions over {overlap} offsets that already "
            f"were encrypted: at {matching_moved} of them old ciphertext "
            f"XOR new ciphertext equals old plaintext XOR new plaintext, "
            f"so whoever knows the cut-off transactions reads the new ones "
            f"({recovered.count(b'SECONDMARKER')} marker rows recovered)"))


REWRITE = {"base_file_name": "binlog", "file_size": "1M"}


def case_size_rewrite() -> dict:
    """The 'size' case on a rewrite-mode storage (GTID mode only), where
    the Binlog Server itself sets the position of every event it stores, so
    that new transactions do land on the offsets a lowered 'size' cuts off.
    The plaintext of the stored binlog is not the source's binlog here: it
    is what 'pull' serves of it, before the edit and after the fetch."""
    binsrv = harness.BINSRV
    harness.SERVER.reset()
    binsrv.reset()
    config = write_config(binsrv)
    content = json.loads(config.read_text())
    content["replication"]["rewrite"] = dict(REWRITE)
    config.write_text(json.dumps(content, indent=2))
    for _ in range(3):
        insert("TAILFIRST", 8)
    first_fetch(binsrv, config)
    indexed = binsrv.indexed_binlogs()
    tail = indexed[-1]
    old_cipher = (binsrv.storage / tail).read_bytes()
    metadata = json.loads((binsrv.storage / (tail + ".json")).read_text())
    served = serve_and_dump(binsrv, config, indexed[0], "size_rewrite-0")
    old_plain = served["served"].get(tail, b"")
    if "encryption" not in metadata or b"TAILFIRST" in old_cipher or \
            len(old_plain) != len(old_cipher) or \
            old_plain.count(b"TAILFIRST") != 24:
        raise harness.Failure(
            f"{tail} was not stored encrypted or is not served as stored "
            f"({len(old_cipher)} bytes stored, {len(old_plain)} served, "
            f"pull exit {served['pull_exit']}, mysqlbinlog "
            f"{served['dump_exit']} {served['dump_error']}), so the case "
            "cannot run")
    starts = [offset for offset, type_code, _ in
              harness.parse_events(old_plain)
              if type_code in harness.TRANSACTION_START_EVENTS]
    # cut off the last two transactions
    lowered = starts[-2]
    # the attacker's edit
    put_object(binsrv, tail + ".json",
               json.dumps(dict(metadata, size=lowered)).encode())
    result = {"case": "size_rewrite", "binlog": tail,
              "edit": f"{tail}.json: 'size' {len(old_cipher)} -> {lowered} "
                      "(the start of the second-to-last transaction; the "
                      "GTID sets are left as they were), rewrite mode"}
    failed = {}
    result["list"] = run_command(binsrv, "list", config,
                                 LIST_TIMEOUT_SECONDS, "size_rewrite-list1")
    for _ in range(3):
        insert("SECONDMARKER", 10)
    result["fetch"] = run_command(binsrv, "fetch", config,
                                  SWEEP_FETCH_TIMEOUT_SECONDS,
                                  "size_rewrite-fetch")
    new_cipher = (binsrv.storage / tail).read_bytes()
    result["list_again"] = run_command(binsrv, "list", config,
                                       LIST_TIMEOUT_SECONDS,
                                       "size_rewrite-list2")
    for command in ("list", "fetch", "list_again"):
        text = command_violation(command, result[command])
        if text:
            failed.setdefault("a", []).append(text)
    if b"SECONDMARKER" in new_cipher:
        failed.setdefault("b", []).append(
            f"{new_cipher.count(b'SECONDMARKER')} new rows readable in "
            f"{tail}")
    end = min(len(old_cipher), len(new_cipher))
    overlap = max(end - lowered, 0)
    ciphertext_xor = xor(old_cipher[lowered:end], new_cipher[lowered:end])
    differing = sum(1 for byte in ciphertext_xor if byte)
    # what someone who knows the cut-off transactions computes
    recovered = xor(ciphertext_xor, old_plain[lowered:end])
    numbers = {"size_before": len(old_cipher),
               "size_set_by_the_edit": lowered,
               "size_after_fetch": len(new_cipher),
               "overlap_bytes": overlap,
               "overlap_bytes_whose_ciphertext_changed": differing,
               "marker_rows_recovered_from_the_overlap_with_the_old_"
               "plaintext": recovered.count(b"SECONDMARKER")}
    matching = 0
    if result["fetch"]["exit"] == 0:
        served = serve_and_dump(binsrv, config, indexed[0], "size_rewrite-1")
        new_plain = served["served"].get(tail, b"")
        plaintext_xor = xor(old_plain[lowered:end], new_plain[lowered:end])
        matching = sum(1 for a, b in zip(ciphertext_xor, plaintext_xor)
                       if a == b)
        numbers["overlap_bytes_where_ciphertext_xor_equals_old_plaintext_"
                "xor_new_plaintext"] = matching
        numbers["rows_served_before_and_after"] = {
            "TAILFIRST": [old_plain.count(b"TAILFIRST"),
                          new_plain.count(b"TAILFIRST")],
            "SECONDMARKER": [0, new_plain.count(b"SECONDMARKER")]}
        numbers["gtids_listed_after"] = json.loads(
            (binsrv.storage / (tail + ".json")).read_text()).get(
                "added_gtids")
        if served["pull_exit"] is not None or served["dump_exit"] != 0:
            failed.setdefault("f", []).append(
                f"the storage is not served after the fetch: pull exit "
                f"{served['pull_exit']} {served['pull_error'][-150:]}, "
                f"mysqlbinlog {served['dump_exit']} "
                f"{served['dump_error'][-150:]}")
        elif new_plain.count(b"TAILFIRST") < old_plain.count(b"TAILFIRST"):
            failed.setdefault("f", []).append(
                f"fetch exited 0, but of the {old_plain.count(b'TAILFIRST')}"
                f" rows {tail} held before the edit it serves "
                f"{new_plain.count(b'TAILFIRST')}, while its metadata "
                f"still lists their GTIDs "
                f"({numbers['gtids_listed_after']}): the transactions are "
                f"gone and will not be fetched again")
    result["size_case"] = numbers
    if overlap >= 64 and differing and (
            b"SECONDMARKER" in recovered or matching > overlap // 2):
        failed.setdefault("c", []).append(
            f"with 'size' lowered from {len(old_cipher)} to {lowered}, "
            f"fetch (exit {result['fetch']['exit']}) truncated {tail} and "
            f"wrote new transactions over {overlap} offsets that already "
            f"were encrypted, with the same keystream: at {matching} of "
            f"them old ciphertext XOR new ciphertext equals old plaintext "
            f"XOR new plaintext, and XORing in the cut-off transactions "
            f"gives {recovered.count(b'SECONDMARKER')} of the new rows")
    result["failed_invariants"] = sorted(failed)
    result["ok"] = not failed
    if failed:
        result["why"] = "; ".join(
            f"({invariant}) {text}" for invariant in sorted(failed)
            for text in failed[invariant])
    return result


KEY = "encryption/file_key_envelope/"
DATA = "encryption/file_data_envelope/"


def key_member(name: str):
    return lambda environment: \
        environment.meta["encryption"]["file_key_envelope"][name]


def data_iv(environment) -> str:
    return environment.meta["encryption"]["file_data_envelope"]["iv_hex"]


# name -> {"edit": ..., and optionally "workload", "check", "kek", "modes"}
SWEEP = {
    "control": {"edit": lambda environment: "none"},
    "size": {"edit": size_edit, "workload": size_workload,
             "check": size_check},
    # -- size
    "size_raised": {"edit": tail_member(
        "size", lambda e: e.size + 1000, "more than the binlog holds")},
    "size_lowered": {"edit": tail_member(
        "size", lambda e: e.size - 7, "7 bytes less: inside the last event")},
    "size_zero": {"edit": tail_member("size", 0, "zero")},
    "size_huge": {"edit": tail_member("size", 2 ** 64 - 1, "2^64 - 1")},
    "size_negative": {"edit": tail_member("size", -1, "negative")},
    "size_non_numeric": {"edit": tail_member("size", "12ab", "a string")},
    # -- GTID sets (in position mode the members do not exist: they are
    # added)
    "previous_gtids_emptied": {"edit": tail_member(
        "previous_gtids", gtid_value("emptied"), "empty set")},
    "previous_gtids_extended": {"edit": tail_member(
        "previous_gtids", gtid_value("extended_far"),
        "GTIDs the source never had")},
    "previous_gtids_malformed": {"edit": tail_member(
        "previous_gtids", gtid_value("malformed"), "not a GTID set")},
    "previous_gtids_other_uuid": {"edit": tail_member(
        "previous_gtids", gtid_value("other_uuid"), "another UUID")},
    "added_gtids_emptied": {"edit": tail_member(
        "added_gtids", gtid_value("emptied"), "empty set")},
    "added_gtids_extended": {"edit": tail_member(
        "added_gtids", gtid_value("extended"),
        "one transaction more than is stored (the source's next one)")},
    "added_gtids_extended_far": {"edit": tail_member(
        "added_gtids", gtid_value("extended_far"),
        "GTIDs the source never had")},
    "added_gtids_malformed": {"edit": tail_member(
        "added_gtids", gtid_value("malformed"), "not a GTID set")},
    "added_gtids_other_uuid": {"edit": tail_member(
        "added_gtids", gtid_value("other_uuid"), "another UUID")},
    "gtids_removed": {"modes": ("gtid",), "edit": tail_file(
        lambda e: {name: value for name, value in e.meta.items()
                   if name not in ("previous_gtids", "added_gtids")},
        "'previous_gtids' and 'added_gtids' removed")},
    # -- timestamps
    "timestamps_swapped": {"edit": tail_file(
        shifted_timestamps,
        "'min_timestamp' set a day after 'max_timestamp', 'max_timestamp' "
        "a day before 'min_timestamp'")},
    "timestamps_absurd": {"edit": tail_file(
        lambda e: dict(e.meta, min_timestamp="0000-00-00T00:00:00",
                       max_timestamp="99999-99-99T99:99:99"),
        "'min_timestamp' 0000-00-00T00:00:00, 'max_timestamp' "
        "99999-99-99T99:99:99")},
    "timestamps_far_future": {"edit": tail_file(
        lambda e: dict(e.meta, min_timestamp="1970-01-01T00:00:00",
                       max_timestamp="9999-12-31T23:59:59"),
        "'min_timestamp' 1970-01-01T00:00:00, 'max_timestamp' "
        "9999-12-31T23:59:59")},
    "timestamp_not_a_string": {"edit": tail_member(
        "min_timestamp", -1, "a number")},
    # -- last_sequence_number
    "last_sequence_number_zero": {"edit": tail_member(
        "last_sequence_number", 0, "zero")},
    "last_sequence_number_huge": {"edit": tail_member(
        "last_sequence_number", 2 ** 64 - 1, "2^64 - 1")},
    "last_sequence_number_negative": {"edit": tail_member(
        "last_sequence_number", -5, "negative")},
    "last_sequence_number_non_numeric": {"edit": tail_member(
        "last_sequence_number", "many", "a string")},
    # -- version
    "version_changed": {"edit": tail_member("version", 2, "another one")},
    "version_removed": {"edit": tail_member("version", REMOVE, "removed")},
    "version_non_numeric": {"edit": tail_member("version", "1",
                                                "a string")},
    # -- the encryption record
    "encryption_null": {"edit": tail_member("encryption", None, "null")},
    "encryption_empty_object": {"edit": tail_member("encryption", {},
                                                    "an empty object")},
    "encryption_data_envelope_removed": {"edit": tail_member(
        "encryption/file_data_envelope", REMOVE, "removed")},
    "kek_id_other_existing": {"edit": tail_member(
        KEY + "kek_id", OTHER_KEK_ID,
        "another KEK of the keyring (AES-256-CTR as well)")},
    "kek_id_other_existing_smaller": {"edit": tail_member(
        KEY + "kek_id", SMALL_KEK_ID,
        "another KEK of the keyring (AES-128-CTR)")},
    "kek_id_other_existing_gcm": {"edit": tail_member(
        KEY + "kek_id", GCM_KEK_ID,
        "another KEK of the keyring (AES-256-GCM)")},
    "kek_id_missing": {"edit": tail_member(
        KEY + "kek_id", "no_such_kek", "a KEK the keyring does not have")},
    "cipher_cbc": {"edit": tail_member(DATA + "cipher", "AES-256-CBC",
                                       "not a CTR mode")},
    "cipher_ecb": {"edit": tail_member(DATA + "cipher", "AES-256-ECB",
                                       "not a CTR mode, no IV")},
    "cipher_gcm": {"edit": tail_member(DATA + "cipher", "AES-256-GCM",
                                       "not a CTR mode")},
    "cipher_weaker": {"edit": tail_member(DATA + "cipher", "AES-128-CTR",
                                          "a smaller key size")},
    "cipher_unknown": {"edit": tail_member(DATA + "cipher", "ROT13",
                                           "not a cipher")},
    "data_iv_changed": {"edit": tail_member(
        DATA + "iv_hex", lambda e: flipped(data_iv(e)),
        "first byte inverted")},
    "data_iv_counter_lowered": {"edit": tail_member(
        DATA + "iv_hex", lowered_counter,
        "block counter lowered by size / 16, so that the end of the "
        "binlog gets the keystream of its start")},
    "data_iv_shortened": {"edit": tail_member(
        DATA + "iv_hex", lambda e: data_iv(e)[:16], "8 bytes of 16")},
    "data_iv_zeroed": {"edit": tail_member(DATA + "iv_hex", "00" * 16,
                                           "all zero")},
    "data_iv_emptied": {"edit": tail_member(DATA + "iv_hex", "", "empty")},
    "data_iv_not_hex": {"edit": tail_member(DATA + "iv_hex", "ZZ" * 16,
                                            "not hexadecimal")},
    "data_tag_added": {"edit": tail_member(DATA + "tag_hex", "AB" * 16,
                                           "a tag for a CTR cipher")},
    "file_key_changed": {"edit": tail_member(
        KEY + "data_hex", lambda e: flipped(key_member("data_hex")(e)),
        "first byte of the wrapped file key inverted")},
    "file_key_shortened": {"edit": tail_member(
        KEY + "data_hex", lambda e: key_member("data_hex")(e)[:32],
        "16 bytes of 32")},
    "file_key_emptied": {"edit": tail_member(KEY + "data_hex", "",
                                             "empty")},
    "file_key_odd_length": {"edit": tail_member(
        KEY + "data_hex", lambda e: key_member("data_hex")(e)[:-1],
        "one hexadecimal digit removed")},
    "key_iv_changed": {"edit": tail_member(
        KEY + "iv_hex", lambda e: flipped(key_member("iv_hex")(e)),
        "first byte of the key-wrapping IV inverted")},
    "key_iv_shortened": {"edit": tail_member(
        KEY + "iv_hex", lambda e: key_member("iv_hex")(e)[:16],
        "8 bytes of 16")},
    "key_iv_removed": {"edit": tail_member(KEY + "iv_hex", REMOVE,
                                           "removed")},
    "key_tag_added": {"edit": tail_member(KEY + "tag_hex", "AB" * 16,
                                          "a tag for a CTR-wrapped key")},
    # with a GCM KEK the wrapped file key has a tag
    "gcm_control": {"kek": GCM_KEK_ID,
                    "edit": lambda environment: "none (AES-256-GCM KEK)"},
    "gcm_file_key_changed": {"kek": GCM_KEK_ID, "edit": tail_member(
        KEY + "data_hex", lambda e: flipped(key_member("data_hex")(e)),
        "first byte of the wrapped file key inverted")},
    "gcm_key_tag_changed": {"kek": GCM_KEK_ID, "edit": tail_member(
        KEY + "tag_hex", lambda e: flipped(key_member("tag_hex")(e)),
        "first byte of the tag inverted")},
    "gcm_key_tag_shortened": {"kek": GCM_KEK_ID, "edit": tail_member(
        KEY + "tag_hex", lambda e: key_member("tag_hex")(e)[:8],
        "4 bytes of 16")},
    "gcm_key_tag_removed": {"kek": GCM_KEK_ID, "edit": tail_member(
        KEY + "tag_hex", REMOVE, "removed")},
    "gcm_key_iv_changed": {"kek": GCM_KEK_ID, "edit": tail_member(
        KEY + "iv_hex", lambda e: flipped(key_member("iv_hex")(e)),
        "first byte of the key-wrapping IV inverted")},
    # -- unknown members
    "unknown_member": {"edit": tail_member(
        "attacker", {"note": [1, 2, 3]}, "a member the format does not "
        "have")},
    "unknown_member_in_encryption": {"edit": tail_member(
        KEY + "attacker", "x" * 100, "a member the format does not have")},
    # -- the whole file
    "file_empty": {"edit": tail_file(lambda e: b"", "truncated to 0 bytes")},
    "file_array": {"edit": tail_file(lambda e: b"[]", "replaced by '[]'")},
    "file_nesting": {"edit": tail_file(
        lambda e: b"[" * (5 * 1024 * 1024),
        "replaced by 5 MB of '[' (5242880 nested arrays)")},
    "file_nesting_objects": {"edit": tail_file(
        lambda e: b'{"version":' * (5 * 1024 * 1024 // 11),
        "replaced by 5 MB of '{\"version\":' (476625 nested objects)")},
    "file_removed": {"edit": tail_file(lambda e: None, "deleted")},
    # -- binlog.index
    "index_duplicated_entry": {"edit": index_lines(
        lambda lines, e: lines + [lines[-1]], "last entry twice")},
    "index_reordered_pair": {"edit": index_lines(
        lambda lines, e: lines[:-2] + [lines[-1], lines[-2]],
        "last two entries swapped")},
    "index_missing_file": {"edit": index_lines(
        lambda lines, e: lines + ["./binlog.000009"],
        "an entry for a binlog that is not stored")},
    "index_absolute_path": {"edit": index_lines(
        lambda lines, e: lines[:-1] + [f"{e.binsrv.storage}/{e.tail}"],
        "last entry as an absolute path")},
    "index_parent_path": {"edit": index_lines(
        lambda lines, e: lines + ["../x"], "an entry '../x'")},
    "index_first_entry_dropped": {"edit": index_lines(
        lambda lines, e: lines[1:], "first entry removed")},
    "index_last_entry_dropped": {"edit": index_lines(
        lambda lines, e: lines[:-1], "last entry removed")},
    "index_emptied": {"edit": index_lines(lambda lines, e: [],
                                          "truncated to 0 bytes")},
    # -- metadata.json
    "storage_mode_flipped": {"edit": storage_member(
        "mode", lambda meta: "position" if meta["mode"] == "gtid" else "gtid",
        "the other replication mode")},
    "storage_encryption_removed": {"edit": storage_member(
        "encryption", REMOVE, "removed")},
    "storage_encryption_changed": {"edit": storage_member(
        "encryption", "none", "not a format")},
    "storage_version_changed": {"edit": storage_member("version", 2,
                                                       "another one")},
}


# the cases that are not in SWEEP; 'size_rewrite' needs GTID mode
OWN_CASES = ["strip_encryption", "transplant_encryption", "size_rewrite"]


def all_cases(gtid_mode: bool) -> list:
    return [name for name in OWN_CASES
            if name != "size_rewrite" or gtid_mode] + [
        name for name, spec in SWEEP.items()
        if ("gtid" if gtid_mode else "position") in
        spec.get("modes", ("gtid", "position"))]


def run_case(name: str) -> dict:
    if name == "strip_encryption":
        return case_strip_encryption()
    if name == "transplant_encryption":
        return case_transplant_encryption()
    if name == "size_rewrite":
        return case_size_rewrite()
    return run_sweep_case(name, SWEEP[name])


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--mysql-basedir", type=Path, required=True)
    parser.add_argument("--binsrv", type=Path, required=True)
    parser.add_argument("--workdir", type=Path, required=True)
    parser.add_argument("--port", type=int, default=23640,
                        help="the source's port; 'pull' listens on the "
                             "port two above it")
    parser.add_argument("--mode", choices=["gtid", "position"],
                        default="position")
    parser.add_argument("--cases", nargs="*",
                        choices=OWN_CASES + list(SWEEP),
                        help="the cases to run (default: all of the mode)")
    parser.add_argument("--storage", choices=["file", "s3"], default="file")
    parser.add_argument("--s3-endpoint", default="127.0.0.1:9000")
    parser.add_argument("--s3-bucket", default="pbs-mtr")
    parser.add_argument("--s3-bucket-dir", type=Path)
    parser.add_argument("--s3-access-key", default="pbsaccesskey")
    parser.add_argument("--s3-secret-key", default="pbs/secret+key=1")
    args = parser.parse_args()
    args.workdir.mkdir(parents=True, exist_ok=True)
    args.mysql_basedir = args.mysql_basedir.expanduser()
    gtid_mode = args.mode == "gtid"
    global MYSQLBINLOG, PULL_PORT  # pylint: disable=global-statement
    MYSQLBINLOG = args.mysql_basedir / "bin" / "mysqlbinlog"
    PULL_PORT = args.port + 2
    s3 = None
    if args.storage == "s3":
        if args.s3_bucket_dir is None:
            parser.error("--storage s3 needs --s3-bucket-dir")
        s3 = harness.S3Storage(args.s3_endpoint, args.s3_bucket,
                               args.s3_bucket_dir.expanduser(),
                               args.s3_access_key, args.s3_secret_key,
                               f"metadata-tamper-{args.mode}-{os.getpid()}")
    harness.SERVER = harness.MySQLServer(args.mysql_basedir,
                                         args.workdir / "mysql", args.port,
                                         gtid_mode, server_id=1)
    harness.BINSRV = harness.BinlogServer(args.binsrv.expanduser(),
                                          args.workdir / "binsrv", args.port,
                                          gtid_mode, s3)
    cases = args.cases or all_cases(gtid_mode)
    results = []
    started = time.monotonic()
    harness.SERVER.start()
    try:
        for name in cases:
            case_started = time.monotonic()
            try:
                result = run_case(name)
            except (harness.Failure, RuntimeError,
                    subprocess.SubprocessError, OSError) as failure:
                result = {"case": name, "ok": False,
                          "why": harness.describe_failure(failure, 500)}
            result["seconds"] = round(time.monotonic() - case_started, 1)
            print(json.dumps(result), flush=True)
            results.append(result)
    finally:
        harness.SERVER.stop()
        if s3 is not None:
            # the prefix of this run
            shutil.rmtree(harness.BINSRV.storage, ignore_errors=True)
    failures = [result for result in results if not result.get("ok")]
    print(json.dumps({"cases": len(results), "failures": len(failures),
                      "failed": [result["case"] for result in failures],
                      "mode": args.mode, "storage": args.storage,
                      "seconds": round(time.monotonic() - started),
                      "passed": not failures}), flush=True)
    (args.workdir / "result.json").write_text(json.dumps(results, indent=2))
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
