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

"""A hostile source and a storage writer against the REWRITE mode.

With 'replication.rewrite' (GTID mode only) the Binlog Server does not copy
the source's binlog files: it stores the transactions in binlog files of its
own ('<base_file_name>.NNNNNN', rotated at 'file_size'), with
FORMAT_DESCRIPTION, PREVIOUS_GTIDS_LOG and ROTATE events it generates itself
and with every event position recomputed. So a stored file cannot be compared
with a file of the source. The cases here judge the storage by the SET OF
TRANSACTIONS it holds instead:

- the stored stream: every stored binlog (read from the storage directory,
  or, for an encrypted storage, as 'pull' serves it to 'mysqlbinlog
  --read-from-remote-server --raw') is walked event by event - checksums,
  end positions, the PREVIOUS_GTIDS_LOG of each file against the GTIDs of the
  files before it, the declared length of each transaction, a BEGIN without
  its commit - and its GTIDs, in order, are compared with the source's
  GTID_EXECUTED (missing, twice, out of order, unknown to the source);
- the metadata: 'list' (the 'added_gtids' of every binlog) must name the
  GTIDs the files hold, and 'search_by_gtid_set' must cover the source's set;
- a replica: a second MySQL server, empty at the start, replicates from
  'pull' with SOURCE_AUTO_POSITION=1; its GTID_EXECUTED must equal what is
  stored and its rows (count and CRC32 of every row) the source's. If the
  replica server itself dies of what 'pull' sends, the case fails and the
  replica is set up again ('storage_control' and 'storage_older_added_
  emptied' also run a replica that starts with the first binlog's GTIDs).

A case PASSES when the storage holds exactly the source's transactions, or
when the tool refuses cleanly (exit 1 and an error message). It FAILS when a
transaction is silently missing, stored twice or altered, when a keystream
range is used for two different plaintexts, when files outside the rewritten
sequence are created or changed, or when a command dies on a signal, prints
a sanitizer report or runs into its timeout.

Cases ('--cases' selects; the default runs all of them):

source_* - a man in the middle between 'fetch' and the source (the proxy of
hostile_source_test.py; the storage is not encrypted). 'fetch' first stores
four transactions directly, the source rotates its binlog and gets five
more (the third and the fourth update the same rows), 'fetch' resumes
through the proxy, the source gets two more, and 'fetch' runs directly
again (the attacker is gone); then the storage is judged. Without rewrite
the end position every event carries must match the storage's, which stops
a transaction sent twice or out of order; in rewrite mode the positions are
recomputed, so nothing does.
  source_control             the proxy changes nothing
  source_name_*              the ROTATE events that announce the source's
                             next binlog name it '<own file>\\0...',
                             'metadata.json\\0...', 'binlog.index\\0...',
                             a name with a newline, '..', 3000 bytes, the
                             current name again, a lower sequence number,
                             the name of a file the storage holds
                             ('own_file') and of the one it creates next
                             ('own_next'): tickets 59, 69, 70 without
                             rewrite. In rewrite mode the name must not
                             matter
  source_size_lie            an event claims 0xFFFFFF00 bytes
  source_gtid_bloat          PREVIOUS_GTIDS_LOG with 900,000 intervals
  source_previous_gtids_more / _empty
                             PREVIOUS_GTIDS_LOG claims a million GTIDs the
                             source never sent / none at all
  source_gtid_twice          the second transaction is sent twice
  source_gtid_swapped        the third and fourth transaction change places
  source_gtid_other_uuid     the second transaction's GTID gets another UUID
  source_length_larger / _smaller / _larger_last
                             'transaction_length' of the GTID event of the
                             second (or of the last) transaction is raised by
                             1000 / lowered to end after its second event
  source_clock_invalid       last_committed = sequence_number in a GTID
                             event (the rewriter recomputes both)
  source_fde_checksum_off / source_fde_version
                             the FORMAT_DESCRIPTION of the next binlog says
                             'no checksums' / server version 5.7.44
  source_checksum_stripped   the events of the next binlog lose their
                             checksums (the FORMAT_DESCRIPTION still
                             promises them)
  source_heartbeat           a HEARTBEAT_LOG event between two transactions
  source_checksum_none       no proxy: the real source runs SET GLOBAL
                             binlog_checksum = NONE (documented as not
                             supported in rewrite mode: must be refused)

storage_* - someone who can write the storage but has no key edits the
metadata of one binlog of an ENCRYPTED rewrite storage (four binlogs, the
last one holds GTIDs 8-9) between two 'fetch' runs; the source gets three
transactions (GTIDs 10-12) in between.
  storage_control
  storage_tail_added_emptied / _truncated / _extended / _other_uuid
                             'added_gtids' of the last binlog: '', without
                             its last GTID, with GTIDs 10-11 added (never
                             stored), with another UUID
  storage_tail_previous_emptied / _extended
                             'previous_gtids' of the last binlog
  storage_older_added_emptied / _extended, storage_older_previous_emptied
                             the same edits on the second binlog
  storage_tail_size_lowered  'size' lowered to the start of the last
                             transaction (metadata_tamper_test.py's
                             'size_rewrite', here with the replica)
  storage_tail_size_raised, storage_older_size_lowered
  storage_tail_encryption_removed / _transplanted, storage_tail_iv_lowered
                             the encryption record removed (ticket 62, PBS-136),
                             replaced by the first binlog's (ticket 63, PBS-137),
                             its IV counter lowered by 64 blocks
In rewrite mode nothing ties the resume point to the stored bytes: 'fetch'
asks the source for everything outside 'previous_gtids' + 'added_gtids' of
the LAST binlog and appends what it gets at 'size'.

crash_* - no attacker: 'fetch' is killed (SIGKILL) on an encrypted rewrite
storage and run again.
  crash_file_block           no checkpoints: a whole binlog reaches the file
                             in one write when it is closed. 'fetch' is
                             stopped the moment a data file is longer than
                             its metadata says and killed there. The next
                             'fetch' truncates the file and writes it again;
                             the events the Binlog Server generates carry
                             its clock, so the bytes differ: the ciphertext
                             before and after is compared over the range
                             written twice (same key, IV and offsets, so
                             every differing byte is one keystream byte
                             used for two plaintexts), and the plaintext
                             before the crash is reconstructed from the two
                             ciphertexts and the plaintext 'pull' serves
                             now and validated by its event checksums
  crash_rotate_block         checkpoint_size 1: killed when only the
                             generated ROTATE (under 120 bytes) is beyond
                             the metadata; the next 'fetch' resumes the same
                             file and writes a transaction there
  crash_random_kills         '--random-kills' (default 10; a few dozen take
                             a few minutes) kills at random points with
                             random checkpoint settings while the source
                             gets more transactions; the first ciphertext
                             seen at every offset of every file is
                             remembered and compared with each later one

rotation_* - what decides when a new file starts, and the name limit.
  rotation_gtid_intervals    the source's GTID set has 300 intervals, so
                             the PREVIOUS_GTIDS_LOG that starts every file
                             is larger than file_size (4K): how many files,
                             and how many bytes, do 30 small transactions
                             take? (an observation: the case fails only if
                             transactions are lost)
  rotation_large_transaction a 600 KB transaction with file_size 2K
  rotation_sequence_limit    the only stored binlog is renamed to
                             '<base>.999998' (data file, metadata, index);
                             after '<base>.999999' the name cannot be
                             incremented: nothing may be overwritten or
                             wrapped

A JSON line per case ('case', 'ok', and 'why' with the evidence on failure),
then a summary; result.json in the work directory; exit 1 if any case
failed. The default run takes about 7 minutes with a debug build. Ports:
--port (the source), +1 (the replica), +2 ('pull'), +3 (the proxy). Example:

  PYTHONPATH=~/ws/qa/pylib python3 rewrite_attack_test.py \\
    --mysql-basedir ~/ws/dist/mysql-8.4.8-linux-glibc2.28-x86_64-minimal \\
    --binsrv ~/ws/percona-binlog-server-build-debug_gcc14/binlog_server \\
    --workdir ~/ws/qa/rewrite-attack
"""

import argparse
import collections
import json
import os
import random
import re
import shutil
import signal
import socket
import struct
import subprocess
import sys
import time
import uuid as uuid_module
import zlib
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import fetch_crash_test as harness  # noqa: E402
import hostile_source_test as hostile  # noqa: E402
import metadata_tamper_test as tamper  # noqa: E402
import pull_replica_test as replication  # noqa: E402
import replica_tamper_test as replica_tamper  # noqa: E402

BASE = "rw"
DATA_NAME = re.compile(r"rw\.\d{6}")
STORAGE_NAME = re.compile(r"rw\.\d{6}(\.json)?|binlog\.index|metadata\.json")
OTHER_UUID = "11111111-2222-4333-8444-555555555555"
FETCH_TIMEOUT = 120
LIST_TIMEOUT = 60
DUMP_TIMEOUT = 60
# a replica is waited for as long as its GTID_EXECUTED keeps growing, up to
# this cap: on a loaded host a replica that catches up through hundreds of
# binlogs (rotation_gtid_intervals) needs more than the half minute a fixed
# limit gave it, while one that stalls is still let go after STABLE_SECONDS
CATCH_UP_SECONDS = 300
# how long a replica's GTID_EXECUTED must stand still before it is taken for
# all the replica will get
STABLE_SECONDS = 4
QUERY_EVENT = 2
XID_EVENT = 16
HEARTBEAT_EVENT = 27
HEARTBEAT_V2_EVENT = 41
GTID_POST_HEADER = 42
CONTROL_EVENTS = (harness.STOP_EVENT, harness.ROTATE_EVENT,
                  harness.FORMAT_DESCRIPTION_EVENT,
                  harness.PREVIOUS_GTIDS_LOG_EVENT, HEARTBEAT_EVENT,
                  HEARTBEAT_V2_EVENT)

# set by main()
REPLICA: harness.MySQLServer = None
MYSQL_BASEDIR: Path = None
PULL_PORT = 0
PROXY_PORT = 0
RANDOM_KILLS = 10


# ---------------------------------------------------------------------------
# GTID sets
# ---------------------------------------------------------------------------

def parse_gtid_set(text: str) -> dict:
    """'uuid:1-3:7,uuid2:5' as {uuid: {1, 2, 3, 7}, uuid2: {5}}."""
    result = {}
    # the mysql client prints a line break as a backslash and an 'n'
    for part in text.replace("\\n", "").replace("\n", "").split(","):
        fields = part.strip().split(":")
        if len(fields) < 2:
            continue
        numbers = result.setdefault(fields[0].lower(), set())
        for interval in fields[1:]:
            first, _, last = interval.partition("-")
            first, last = int(first), int(last or first)
            if last - first > 5_000_000:
                raise harness.Failure(f"GTID interval too large: {part}")
            numbers.update(range(first, last + 1))
    return {sid: numbers for sid, numbers in result.items() if numbers}


def format_gtid_set(gtids: dict) -> str:
    parts = []
    for sid in sorted(gtids):
        numbers = sorted(gtids[sid])
        if not numbers:
            continue
        intervals = []
        first = previous = numbers[0]
        for number in numbers[1:] + [None]:
            if number is not None and number == previous + 1:
                previous = number
                continue
            intervals.append(str(first) if first == previous else
                             f"{first}-{previous}")
            first = previous = number
        parts.append(sid + ":" + ":".join(intervals))
    return ",".join(parts)


def subtract(left: dict, right: dict) -> dict:
    result = {sid: numbers - right.get(sid, set())
              for sid, numbers in left.items()}
    return {sid: numbers for sid, numbers in result.items() if numbers}


def union(left: dict, right: dict) -> dict:
    result = {sid: set(numbers) for sid, numbers in left.items()}
    for sid, numbers in right.items():
        result.setdefault(sid, set()).update(numbers)
    return result


def as_set(gtid_list: list) -> dict:
    result = {}
    for gtid in gtid_list:
        sid, _, number = gtid.rpartition(":")
        result.setdefault(sid, set()).add(int(number))
    return result


def short(text: str, limit: int = 160) -> str:
    return text if len(text) <= limit else text[:limit] + "..."


# ---------------------------------------------------------------------------
# stored binlogs, event by event
# ---------------------------------------------------------------------------

def length_field(event: bytes) -> tuple:
    """(offset, width, value) of 'transaction_length' in a GTID_LOG event."""
    position = harness.COMMON_HEADER_SIZE + GTID_POST_HEADER + 7
    if event[position - 1] & 0x80:
        # an original commit timestamp follows the immediate one
        position += 7
    first = event[position]
    if first < 251:
        return position, 1, first
    width = {0xFC: 3, 0xFD: 4}.get(first, 9)
    return position, width, int.from_bytes(
        event[position + 1:position + width], "little")


def query_text(data: bytes, event: tuple) -> bytes:
    """The statement of a QUERY event (b"" for any other event)."""
    offset, code, size = event
    if code != QUERY_EVENT:
        return b""
    start = offset + harness.COMMON_HEADER_SIZE
    database_length = data[start + 8]
    status_length = struct.unpack_from("<H", data, start + 11)[0]
    return bytes(data[start + 13 + status_length + database_length + 1:
                      offset + size - 4])


def decode_previous_gtids(body: bytes) -> dict:
    result = {}
    count = struct.unpack_from("<q", body, 0)[0]
    position = 8
    for _ in range(count):
        sid = str(uuid_module.UUID(bytes=bytes(body[position:position + 16])))
        intervals = struct.unpack_from("<q", body, position + 16)[0]
        position += 24
        numbers = result.setdefault(sid, set())
        for _ in range(intervals):
            first, end = struct.unpack_from("<qq", body, position)
            position += 16
            numbers.update(range(first, end))
    return result


def examine_stream(files: dict) -> dict:
    """Walks the stored binlogs in name order. Returns the GTIDs in stream
    order and everything that is wrong with the stream itself."""
    gtids = []
    problems = []
    transactions = collections.Counter()
    before = None
    for name in sorted(files):
        data = files[name]
        try:
            events = harness.parse_events(data)
        except harness.Failure:
            problems.append(f"{name} ({len(data)} bytes) does not start "
                            "with the binlog magic")
            continue
        end = events[-1][0] + events[-1][2] if events else 4
        if end != len(data):
            problems.append(f"{name}: {len(data) - end} bytes after the "
                            f"last complete event (at {end})")
        codes = [code for _, code, _ in events]
        if codes[:2] != [harness.FORMAT_DESCRIPTION_EVENT,
                         harness.PREVIOUS_GTIDS_LOG_EVENT]:
            problems.append(f"{name} starts with events {codes[:2]}, not "
                            "FORMAT_DESCRIPTION and PREVIOUS_GTIDS_LOG")
        if codes.count(harness.FORMAT_DESCRIPTION_EVENT) > 1:
            problems.append(f"{name} holds "
                            f"{codes.count(harness.FORMAT_DESCRIPTION_EVENT)}"
                            " FORMAT_DESCRIPTION events")
        bad_checksums = bad_positions = 0
        starts = [offset for offset, code, _ in events
                  if code in harness.TRANSACTION_START_EVENTS or
                  code in (harness.ROTATE_EVENT, harness.STOP_EVENT)] + [end]
        for offset, code, size in events:
            if zlib.crc32(data[offset:offset + size - 4]) != \
                    struct.unpack_from("<I", data, offset + size - 4)[0]:
                bad_checksums += 1
            if struct.unpack_from("<I", data, offset + 13)[0] != \
                    (offset + size) & 0xFFFFFFFF:
                bad_positions += 1
            if code == harness.PREVIOUS_GTIDS_LOG_EVENT:
                previous = decode_previous_gtids(
                    data[offset + harness.COMMON_HEADER_SIZE:
                         offset + size - 4])
                if before is None:
                    # the first stored binlog: what the source had purged
                    before = previous
                elif previous != before:
                    problems.append(
                        f"{name}: PREVIOUS_GTIDS_LOG is "
                        f"'{short(format_gtid_set(previous))}', the "
                        f"binlogs before it hold "
                        f"'{short(format_gtid_set(before))}'")
            if code == harness.GTID_LOG_EVENT:
                event = data[offset:offset + size]
                sid = str(uuid_module.UUID(bytes=bytes(event[20:36])))
                number = struct.unpack_from("<q", event, 36)[0]
                gtids.append(f"{sid}:{number}")
                before = union(before or {}, {sid: {number}})
                transactions[name] += 1
                declared = length_field(event)[2]
                following = min(start for start in starts if start > offset)
                if offset + declared != following:
                    problems.append(
                        f"{name}: the transaction {sid}:{number} at "
                        f"{offset} declares {declared} bytes but the next "
                        f"one starts after {following - offset}")
                group = [(o, c, s) for o, c, s in events
                         if offset < o < following]
                if group and group[0][1] == QUERY_EVENT and \
                        query_text(data, group[0]) == b"BEGIN" and (
                            len(group) < 2 or
                            group[-1][1] != XID_EVENT and
                            query_text(data, group[-1]) != b"COMMIT"):
                    problems.append(
                        f"{name}: the transaction {sid}:{number} at "
                        f"{offset} is cut off: BEGIN and "
                        f"{len(group) - 1} more event(s), no commit")
        if bad_checksums:
            problems.append(f"{name}: {bad_checksums} event(s) with a wrong "
                            "checksum")
        if bad_positions:
            problems.append(f"{name}: {bad_positions} event(s) whose end "
                            "position is not where they end")
    return {"gtids": gtids, "problems": problems,
            "transactions_per_binlog": dict(transactions)}


# ---------------------------------------------------------------------------
# configuration and commands
# ---------------------------------------------------------------------------

def make_config(file_size: str, encrypted: bool, checkpoint_size=None,
                name: str = "binsrv-rw.json", proxied: bool = False) -> Path:
    binsrv = harness.BINSRV
    binsrv.checkpoint_size = checkpoint_size
    binsrv.write_config()
    config = json.loads(binsrv.config_path.read_text())
    config["replication"]["rewrite"] = {"base_file_name": BASE,
                                        "file_size": file_size}
    if encrypted:
        keyring = binsrv.workdir / "keyring.json"
        keyring.write_text(json.dumps(tamper.KEYRING))
        config["keyring"] = {"uri": f"file://{keyring}"}
        config["storage"]["encryption"] = dict(tamper.ENCRYPTION)
    if proxied:
        config["connection"]["port"] = PROXY_PORT
        config["connection"]["ssl"] = {"mode": "disabled"}
    # used by 'pull' only
    config["replication_source"] = {
        "port": PULL_PORT, "server_id": 142,
        "server_uuid": "00000000-0000-0000-0000-00000000008e",
        "read_timeout": 5, "write_timeout": 5,
        "require_secure_transport": False,
        "auth_rsa_private_key": str(replication.AUTH_RSA_PRIVATE_KEY),
        "authentication": {"user": replication.PULL_USER,
                           "password": replication.PULL_PASSWORD,
                           "plugin": "caching_sha2_password"}}
    path = binsrv.workdir / name
    path.write_text(json.dumps(config, indent=2))
    return path


def command(name: str, config: Path, label: str,
            timeout: float = FETCH_TIMEOUT) -> dict:
    return tamper.run_command(harness.BINSRV, name, config, timeout, label)


def brief(result: dict) -> dict:
    return {key: result[key] for key in
            ("exit", "seconds", "message", "peak_rss_mb", "sanitizer",
             "report_file", "timed_out_after_seconds") if key in result}


def must_fetch(config: Path, label: str) -> None:
    result = command("fetch", config, label)
    if result["exit"] != 0:
        raise harness.Failure(f"the preparing fetch ({label}) exited with "
                              f"{result['exit']}: {result['message']}\n"
                              f"{harness.BINSRV.log_tail()}")


def query(config: Path, *arguments: str) -> dict:
    """'list' / 'search_by_gtid_set': exit code and parsed answer."""
    try:
        completed = subprocess.run(
            [str(harness.BINSRV.binary), arguments[0], str(config),
             *arguments[1:]], capture_output=True, timeout=LIST_TIMEOUT,
            check=False, env=tamper.binsrv_environment())
    except subprocess.TimeoutExpired:
        return {"exit": "timeout", "status": None, "message": "timeout",
                "result": []}
    try:
        answer = json.loads(completed.stdout.decode(errors="replace"))
    except ValueError:
        answer = {}
    entries = answer.get("result")
    return {"exit": completed.returncode, "status": answer.get("status"),
            "message": str(answer.get("message", ""))[:200] or
            completed.stderr.decode(errors="replace").strip()[-200:],
            "result": entries if isinstance(entries, list) else []}


def insert_many(marker: str, transactions: int, rows: int) -> None:
    """'transactions' single-statement transactions in one client call."""
    values = ",".join(f"('{marker}-{row:04d}-{'x' * 64}')"
                      for row in range(rows))
    harness.SERVER.sql(f"INSERT INTO qa.t (payload) VALUES {values};" *
                       transactions)


def source_executed() -> str:
    return harness.SERVER.sql("SELECT @@GLOBAL.gtid_executed").replace(
        "\\n", "").replace("\n", "").strip()


def rows_digest(server) -> str:
    try:
        return server.sql(
            "SELECT COUNT(*), COALESCE(SUM(CRC32(CONCAT(id, ':', payload))), "
            "0) FROM qa.t").strip().replace("\t", " rows, crc sum ")
    except RuntimeError as error:
        return f"error: {str(error).splitlines()[0][:100]}"


def stored_bytes() -> int:
    total = 0
    for entry in os.scandir(harness.BINSRV.storage):
        if DATA_NAME.fullmatch(entry.name):
            try:
                total += entry.stat().st_size
            except OSError:
                pass
    return total


def sanitizer_markers(text: str) -> list:
    return [marker for marker in ("AddressSanitizer", "LeakSanitizer",
                                  "UndefinedBehaviorSanitizer",
                                  "runtime error:", "Assertion")
            if marker in text]


# ---------------------------------------------------------------------------
# 'pull', what it serves, and a replica of it
# ---------------------------------------------------------------------------

class PullSession:
    """'binlog_server pull' on the storage, for one dump and one replica."""

    def __init__(self, config: Path, label: str):
        self.binsrv = harness.BINSRV
        self.label = label
        self.stderr_path = self.binsrv.workdir / "pull.stderr"
        self.stderr = self.stderr_path.open("wb")
        self.process = subprocess.Popen(
            [str(self.binsrv.binary), "pull", str(config)],
            stdout=subprocess.DEVNULL, stderr=self.stderr,
            env=tamper.binsrv_environment())
        self.listening = False
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline and self.process.poll() is None:
            try:
                socket.create_connection(("127.0.0.1", PULL_PORT),
                                         timeout=1).close()
                self.listening = True
                break
            except OSError:
                time.sleep(0.1)

    def dump(self, first_binlog: str) -> dict:
        """What 'mysqlbinlog --raw' gets from 'first_binlog' on."""
        directory = self.binsrv.workdir / "dump"
        shutil.rmtree(directory, ignore_errors=True)
        directory.mkdir(parents=True)
        result = {"files": {}, "exit": None, "error": ""}
        try:
            dump = subprocess.run(
                [str(MYSQL_BASEDIR / "bin" / "mysqlbinlog"), "--no-defaults",
                 "--read-from-remote-server", "--host=127.0.0.1",
                 f"--port={PULL_PORT}", f"--user={replication.PULL_USER}",
                 f"--password={replication.PULL_PASSWORD}",
                 "--default-auth=caching_sha2_password",
                 "--get-server-public-key", "--ssl-mode=DISABLED", "--raw",
                 "--to-last-log", f"--result-file={directory}/",
                 first_binlog],
                capture_output=True, timeout=DUMP_TIMEOUT, check=False)
            result["exit"] = dump.returncode
            result["error"] = "; ".join(
                line for line in dump.stderr.decode(
                    errors="replace").splitlines()
                if "[Warning]" not in line)[-300:]
        except subprocess.TimeoutExpired:
            result["exit"] = "timeout"
            result["error"] = f"mysqlbinlog did not finish in {DUMP_TIMEOUT} s"
        for path in sorted(directory.iterdir()):
            result["files"][path.name] = path.read_bytes()
        return result

    def close(self) -> dict:
        """How 'pull' ended: 'exit' is None if it was still serving."""
        code = self.process.poll()
        if code is None:
            self.process.send_signal(signal.SIGTERM)
            try:
                # with an idle source 'pull' may take up to
                # connection.read_timeout to notice (ticket 43, PBS-116)
                self.process.wait(timeout=2)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait()
        self.stderr.close()
        text = self.stderr_path.read_text(errors="replace")
        result = {"exit": code, "sanitizer": sanitizer_markers(text)}
        if result["sanitizer"] or code not in (None, 0, 1):
            report = self.binsrv.workdir.parent / \
                f"abnormal-{self.label}-pull.txt"
            report.write_text(text + "\n--- log\n" +
                              self.binsrv.log_tail(80))
            result["report_file"] = str(report)
        if code is not None:
            result["error"] = (tamper.last_errors(self.binsrv) or
                               [text.strip()[-300:]])[-1]
        return result


def replica_check(source: dict, prepare: str = "") -> dict:
    """Replicates from 'pull' with auto-positioning until the replica has
    the source's GTIDs, stands still or reports an error. If the replica
    server itself dies of what it is sent, that is reported ('died') and
    the replica is set up again from scratch."""
    try:
        return replica_run(source, prepare)
    except RuntimeError as error:
        if REPLICA.process is None or REPLICA.process.poll() is None:
            raise
        code = REPLICA.process.wait()
        log = REPLICA.error_log.read_text(errors="replace").splitlines()
        died = (f"mysqld exited with {code}"
                + (f" (signal {signal.Signals(-code).name})" if code < 0
                   else "") + ": " + " | ".join(
                       line[:200] for line in log
                       if "MY-010443" in line or "MY-010590" in line or
                       "terminate called" in line or "what():" in line or
                       "got signal" in line)[-700:])
        crash_log = harness.BINSRV.workdir.parent / "replica-crash.err"
        crash_log.write_text("\n".join(log[-120:]))
        REPLICA.process = None
        shutil.rmtree(REPLICA.datadir, ignore_errors=True)
        REPLICA.error_log.unlink(missing_ok=True)
        REPLICA.start()
        return {"executed": {}, "rows": "", "errors": [], "seconds": 0,
                "died": died, "error": str(error).splitlines()[0][:160]}


def replica_run(source: dict, prepare: str) -> dict:
    replication.replicate_from_pull(REPLICA, PULL_PORT, True,
                                    "SOURCE_AUTO_POSITION=1", prepare)
    started = time.monotonic()
    executed = last = None
    stable_since = started
    while time.monotonic() - started < CATCH_UP_SECONDS:
        executed = parse_gtid_set(REPLICA.sql("SELECT @@GLOBAL.gtid_executed"))
        if executed and not subtract(source, executed):
            break
        now = time.monotonic()
        if executed != last:
            last, stable_since = executed, now
        elif now - stable_since > STABLE_SECONDS and now - started > 6:
            break
        time.sleep(0.25)
    state = replica_tamper.replica_state(REPLICA)
    errors = [" ".join(fields)[:240]
              for fields in (state["receiver"], state["applier"])
              if len(fields) > 1 and fields[1] not in ("0", "")]
    rows = rows_digest(REPLICA)
    REPLICA.sql("STOP REPLICA")
    return {"executed": executed or {}, "rows": rows, "errors": errors,
            "seconds": round(time.monotonic() - started, 1)}


def judge(config: Path, encrypted: bool, label: str,
          with_replica: bool = True, serve: bool = True) -> dict:
    """The storage against the source. Returns 'facts' (for the result
    line), 'problems' (each one fails the case), 'missing' (the source's
    GTIDs the stored stream lacks, as text) and 'plain' (the stored binlogs
    in plaintext)."""
    binsrv = harness.BINSRV
    source_text = source_executed()
    source = parse_gtid_set(source_text)
    facts = {}
    problems = []
    listing = query(config, "list")
    entries = [entry for entry in listing["result"]
               if isinstance(entry, dict)]
    names = [str(entry.get("name")) for entry in entries]
    facts["binlogs"] = len(names)
    facts["last_binlogs"] = [f"{entry.get('name')}:{entry.get('size')}"
                             for entry in entries[-3:]]
    if listing["exit"] != 0:
        problems.append(f"'list' refuses the storage (exit "
                        f"{listing['exit']}: {listing['message']})")
    search = query(config, "search_by_gtid_set", source_text)
    facts["search_by_gtid_set"] = search["status"] or search["message"]
    plain = {}
    unreadable = False
    if not encrypted:
        # before 'pull' runs, which fetches as well
        plain = {name: (binsrv.storage / name).read_bytes()
                 for name in binsrv.indexed_binlogs()
                 if (binsrv.storage / name).is_file()}
    replica = None
    ended = {"exit": None, "sanitizer": []}
    if serve:
        session = PullSession(config, label)
        try:
            if not session.listening:
                problems.append("'pull' does not serve the storage")
            else:
                if encrypted and names:
                    dump = session.dump(names[0])
                    plain = dump["files"]
                    if dump["exit"] != 0:
                        unreadable = True
                        problems.append(
                            f"mysqlbinlog cannot read the storage through "
                            f"'pull' (exit {dump['exit']}: "
                            f"{dump['error'][-160:]})")
                if with_replica:
                    replica = replica_check(source)
        finally:
            ended = session.close()
    if ended["exit"] is not None or ended["sanitizer"]:
        problems.append(
            f"'pull' ended by itself (exit {ended['exit']}, "
            f"{'/'.join(ended['sanitizer']) or 'no sanitizer report'}): "
            f"{ended.get('error', '')[-200:]} {ended.get('report_file', '')}")
    stream = examine_stream(plain)
    stored = as_set(stream["gtids"])
    counts = collections.Counter(stream["gtids"])
    twice = sorted(gtid for gtid, count in counts.items() if count > 1)
    missing = subtract(source, stored)
    unknown = subtract(stored, source)
    facts["transactions_stored"] = len(stream["gtids"])
    facts["source_gtids"] = short(source_text)
    facts["stored_gtids"] = short(format_gtid_set(stored))
    problems += stream["problems"][:4]
    if twice:
        problems.append(f"{len(twice)} transaction(s) are stored twice: "
                        f"{short(format_gtid_set(as_set(twice)))}")
    order = [int(gtid.rpartition(":")[2]) for gtid in stream["gtids"]
             if gtid.rpartition(":")[0] in source and
             len(source) == 1]
    late = [f"{later} before {earlier}" for earlier, later in
            zip(order[1:], order) if later > earlier]
    if late:
        problems.append("transactions are stored in another order than "
                        "the source committed them: GTID " +
                        ", ".join(late[:3]))
    if unknown:
        problems.append("the storage holds GTIDs the source never "
                        f"executed: {short(format_gtid_set(unknown))}")
    listed = {}
    for entry in entries:
        listed = union(listed, parse_gtid_set(str(entry.get("added_gtids")
                                                  or "")))
    if unreadable:
        # what the binlogs hold is not known
        facts["stored_gtids"] = "unknown: the storage cannot be read"
        missing = {}
    elif entries and plain:
        listed_only = subtract(listed, stored)
        stored_only = subtract(stored, listed)
        if listed_only:
            problems.append(
                f"'added_gtids' lists "
                f"{short(format_gtid_set(listed_only))}, which no stored "
                "binlog holds")
        if stored_only:
            problems.append(
                f"the binlogs hold {short(format_gtid_set(stored_only))}, "
                "which no 'added_gtids' lists")
    if not missing and plain and not unreadable and \
            search["status"] != "success":
        problems.append(f"search_by_gtid_set does not cover the source's "
                        f"GTIDs: {search['message']}")
    if replica is not None and replica.get("died"):
        facts["replica"] = {"died": replica["died"]}
        problems.append("the replica server itself died while replicating "
                        "from 'pull': " + replica["died"])
    elif replica is not None:
        lacks = subtract(source if unreadable else stored,
                         replica["executed"])
        extra = subtract(replica["executed"],
                         source if unreadable else stored)
        facts["replica"] = {
            "gtid_executed": short(format_gtid_set(replica["executed"])),
            "rows": replica["rows"], "source_rows": rows_digest(
                harness.SERVER), "seconds": replica["seconds"]}
        if replica["errors"]:
            facts["replica"]["errors"] = replica["errors"]
            problems.append("the replica of 'pull' stopped with an error: "
                            + replica["errors"][0])
        if lacks or extra:
            problems.append(
                "a replica of 'pull' (auto-position, empty at the start) "
                f"ends with GTID_EXECUTED "
                f"'{short(format_gtid_set(replica['executed']))}': it "
                f"lacks '{short(format_gtid_set(lacks))}' of what "
                f"{'the source has' if unreadable else 'is stored'}" +
                (f" and has '{short(format_gtid_set(extra))}' that is not"
                 if extra else ""))
        elif not missing and not replica["errors"] and \
                replica["rows"] != facts["replica"]["source_rows"]:
            problems.append(
                f"the replica has the source's GTIDs but not its rows: "
                f"{replica['rows']} against "
                f"{facts['replica']['source_rows']}")
    if problems and serve and binsrv.log_path.exists():
        # the log of 'pull', which the next command overwrites
        kept = binsrv.workdir.parent / f"pull-{label}.log"
        shutil.copyfile(binsrv.log_path, kept)
        facts["pull_log"] = str(kept)
    return {"facts": facts, "problems": problems,
            "missing": format_gtid_set(missing), "plain": plain}


def finish(result: dict, problems: list) -> dict:
    result["ok"] = not problems
    if problems:
        result["why"] = "; ".join(problems)
    return result


def violations(result: dict, *names: str) -> list:
    """Crashes, sanitizer reports, timeouts and absurd memory use of the
    commands of a case."""
    found = []
    for name in names:
        if name in result:
            text = tamper.command_violation(name, result[name])
            if text:
                found.append(text)
    return found


# ---------------------------------------------------------------------------
# the man in the middle
# ---------------------------------------------------------------------------

class AttackProxy(hostile.RenamingProxy):
    """The proxy of hostile_source_test.py, working on whole transactions:
    the events of a transaction are held back until its end, so that it can
    be sent twice, late or changed. Every packet of the dump is renumbered,
    as the number of packets changes."""

    def __init__(self, listen_port: int, source_port: int, attack: str,
                 source_uuid: str, target: int = 2):
        self.sequence = 0
        self.pending = []
        self.seen = 0
        self.held = None
        self.target = target
        self.continuation = False
        self.source_uuid = uuid_module.UUID(source_uuid).bytes
        super().__init__(listen_port, source_port, b"", b"", attack)

    def _server_to_client(self, server, client):
        try:
            while True:
                header = self._read_exactly(server, 4)
                if header is None:
                    break
                length = header[0] | header[1] << 8 | header[2] << 16
                payload = self._read_exactly(server, length) if length else b""
                if payload is None:
                    break
                if not self.dumping.is_set():
                    client.sendall(header + payload)
                    continue
                for out in self._process(payload, length):
                    self.sequence = (self.sequence + 1) & 0xFF
                    client.sendall(struct.pack("<I", len(out))[:3] +
                                   bytes([self.sequence]) + out)
        except OSError:
            # the client has gone
            pass
        finally:
            for sock in (client, server):
                try:
                    sock.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass

    def _process(self, payload: bytes, length: int) -> list:
        if length == 0xFFFFFF or self.continuation:
            # an event of 16 MB or more: passed as it is
            self.continuation = length == 0xFFFFFF
            return self._flush(False) + [payload]
        if len(payload) < 1 + harness.COMMON_HEADER_SIZE or payload[0] != 0:
            # the end of the dump (EOF or an error)
            return self._flush(True) + [payload]
        code = payload[5]
        if code in harness.TRANSACTION_START_EVENTS:
            out = self._flush(False)
            self.pending = [payload]
            return out
        if self.pending and code not in CONTROL_EVENTS:
            self.pending.append(payload)
            return []
        return self._flush(False) + [self._single(payload, code)]

    def _flush(self, final: bool) -> list:
        out = []
        if self.pending:
            transaction, self.pending = self.pending, []
            self.seen += 1
            out = self._transaction(transaction, final)
        if final and self.held is not None:
            out += self.held
            self.held = None
        return out

    def _patched(self, payload: bytes, edit) -> bytes:
        """The event of 'payload' (which ends with a checksum) after
        'edit(event)', with its size and checksum set again."""
        event = bytearray(payload[1:-4])
        edit(event)
        self.rewritten += 1
        return self._finish(event, True)

    def _stripped(self, payload: bytes) -> bytes:
        if payload[5] == harness.FORMAT_DESCRIPTION_EVENT:
            return payload
        self.rewritten += 1
        return self._finish(bytearray(payload[1:-4]), False)

    def _transaction(self, transaction: list, final: bool) -> list:
        attack = self.attack
        hit = self.seen == self.target

        def set_length(change):
            def edit(event):
                position, width, value = length_field(event)
                if width != 3:
                    raise harness.Failure("transaction_length is not a "
                                          "3-byte field")
                event[position + 1:position + 3] = \
                    change(value).to_bytes(2, "little")
            return edit

        if attack == "checksum_stripped":
            return [self._stripped(payload) for payload in transaction]
        if attack == "gtid_twice" and hit:
            self.rewritten += 1
            return transaction + transaction
        if attack == "gtid_swapped":
            if hit:
                self.held = transaction
                return []
            if self.held is not None:
                held, self.held = self.held, None
                self.rewritten += 1
                return transaction + held
        if attack == "gtid_other_uuid" and hit:
            def edit(event):
                event[20:36] = uuid_module.UUID(OTHER_UUID).bytes
            transaction[0] = self._patched(transaction[0], edit)
        if attack == "length_larger" and hit or \
                attack == "length_larger_last" and final:
            transaction[0] = self._patched(
                transaction[0], set_length(lambda value: value + 1000))
        if attack == "length_smaller" and hit:
            first_two = len(transaction[0]) + len(transaction[1]) - 2
            transaction[0] = self._patched(
                transaction[0], set_length(lambda value: first_two))
        if attack == "clock_invalid" and hit:
            def edit(event):
                # last_committed = sequence_number
                event[45:53] = event[53:61]
            transaction[0] = self._patched(transaction[0], edit)
        if attack == "heartbeat" and hit:
            name = b"binlog.000002"
            event = bytearray(struct.pack(
                "<IBIIIH", 0, HEARTBEAT_EVENT, 1, 0, 0, 0) + name)
            self.rewritten += 1
            return [self._finish(event, True)] + transaction
        return transaction

    def _single(self, payload: bytes, code: int) -> bytes:
        attack = self.attack
        if attack == "checksum_stripped":
            return self._stripped(payload)
        if code == harness.PREVIOUS_GTIDS_LOG_EVENT and \
                attack in ("previous_gtids_more", "previous_gtids_empty"):
            body = struct.pack("<q", 0) if attack.endswith("empty") else (
                struct.pack("<q", 1) + self.source_uuid +
                struct.pack("<qqq", 1, 1, 1_000_001))

            def edit(event):
                event[harness.COMMON_HEADER_SIZE:] = body
            return self._patched(payload, edit)
        if code == harness.FORMAT_DESCRIPTION_EVENT and \
                attack == "fde_checksum_off":
            def edit(event):
                # the checksum algorithm is the last byte before the footer
                event[-1] = 0
            return self._patched(payload, edit)
        if code == harness.FORMAT_DESCRIPTION_EVENT and \
                attack == "fde_version":
            def edit(event):
                start = harness.COMMON_HEADER_SIZE + 2
                event[start:start + 50] = b"5.7.44-hostile".ljust(50, b"\0")
            return self._patched(payload, edit)
        return payload


def storage_file_names(count: int, upcoming: int) -> dict:
    """The names for the source's next binlog (its real sequence number is
    'upcoming'; the storage holds 'count' binlogs of its own)."""
    names = {name: make(upcoming) for name, make in hostile.CASES.items()
             if name not in ("nul_existing_binlog", "case_variant",
                             "lower_sequence")}
    names["nul_own_binlog"] = b"rw.000001\x00" + b".%06d" % upcoming
    names["newline"] = b"rw.000077\nrw" + b".%06d" % upcoming
    names["lower_sequence"] = b"binlog.000001"
    names["own_file"] = b"rw.000001"
    names["own_next"] = b"rw.%06d" % (count + 1)
    return names


NAME_CASES = ["nul_own_binlog", "nul_metadata", "nul_index", "newline",
              "dot_dot", "long_name", "reannounce", "lower_sequence",
              "own_file", "own_next"]
TRANSACTION_ATTACKS = [
    "gtid_twice", "gtid_swapped", "gtid_other_uuid", "length_larger",
    "length_smaller", "length_larger_last", "clock_invalid",
    "previous_gtids_more", "previous_gtids_empty", "fde_checksum_off",
    "fde_version", "checksum_stripped", "heartbeat"]


def run_source_case(name: str) -> dict:
    binsrv = harness.BINSRV
    attack = name[len("source_"):]
    harness.SERVER.reset()
    binsrv.reset()
    direct = make_config("2K", False)
    proxied_config = make_config("2K", False, name="binsrv-proxied.json",
                                 proxied=True)
    insert_many("BEFORE", 4, 8)
    must_fetch(direct, name + "-first")
    before = tamper.snapshot(binsrv)
    harness.SERVER.sql("FLUSH BINARY LOGS")
    result = {"case": name}
    problems = []
    proxy = None
    try:
        if attack == "checksum_none":
            harness.SERVER.sql("SET GLOBAL binlog_checksum = NONE")
        upcoming = int(harness.SERVER.binlog_names()[-1].rsplit(".", 1)[1])
        old_name = b"binlog.%06d" % upcoming
        # the third and the fourth transaction change the same rows
        insert_many("DURING", 2, 8)
        harness.SERVER.sql(
            "UPDATE qa.t SET payload = CONCAT('THIRD-', id, REPEAT('x', "
            "60)) WHERE id <= 3; UPDATE qa.t SET payload = "
            "CONCAT('FOURTH-', id, REPEAT('x', 60)) WHERE id <= 3")
        insert_many("DURING", 1, 8)
        if attack == "checksum_none":
            result["attacked_fetch"] = command("fetch", direct,
                                               name + "-attacked")
        else:
            hostile.cache_password_hash(MYSQL_BASEDIR, harness.SERVER.port)
            if attack.startswith("name_"):
                new_name = storage_file_names(
                    len(binsrv.indexed_binlogs()),
                    upcoming)[attack[len("name_"):]]
                result["hostile_name"] = repr(new_name)[:80]
                proxy = hostile.RenamingProxy(
                    PROXY_PORT, harness.SERVER.port, old_name, new_name)
            elif attack in hostile.RESOURCE_CASES:
                proxy = hostile.RenamingProxy(
                    PROXY_PORT, harness.SERVER.port, old_name, old_name,
                    attack)
            else:
                proxy = AttackProxy(
                    PROXY_PORT, harness.SERVER.port, attack,
                    harness.SERVER.sql("SELECT @@server_uuid").strip(),
                    3 if attack == "gtid_swapped" else 2)
            result["attacked_fetch"] = command("fetch", proxied_config,
                                               name + "-attacked")
            proxy.close()
            result["events_changed"] = proxy.rewritten
            if proxy.rewritten == 0 and attack != "control":
                problems.append("the proxy changed nothing: the case did "
                                "not run")
        result["attacked_fetch"] = brief(result["attacked_fetch"])
        after_attack = tamper.snapshot(binsrv)
        insert_many("AFTER", 2, 8)
        # the attacker is gone
        result["honest_fetch"] = brief(command("fetch", direct,
                                               name + "-honest"))
        if attack == "checksum_none":
            harness.SERVER.sql("SET GLOBAL binlog_checksum = CRC32")
    finally:
        if proxy is not None:
            proxy.close()
        if attack == "checksum_none":
            harness.SERVER.sql("SET GLOBAL binlog_checksum = CRC32")
    problems += violations(result, "attacked_fetch", "honest_fetch")
    strangers = sorted(repr(entry)[1:-1][:60]
                       for entry in os.listdir(binsrv.storage)
                       if not STORAGE_NAME.fullmatch(entry))
    if strangers:
        problems.append("the storage directory got entries outside the "
                        "rewritten sequence: " + ", ".join(strangers[:4]))
    damaged = sorted(
        object_name for object_name, content in before.items()
        if DATA_NAME.fullmatch(object_name) and
        not after_attack.get(object_name, b"").startswith(content))
    if damaged or after_attack.get("metadata.json") != \
            before.get("metadata.json"):
        problems.append("binlogs stored before the attack lost bytes: " +
                        (", ".join(damaged) or "metadata.json changed"))
    # with a binlog without checksums on the source 'pull' refuses to
    # start, as 'fetch' does
    verdict = judge(direct, False, name, serve=attack != "checksum_none")
    result.update(verdict["facts"])
    problems += verdict["problems"]
    refused = result["honest_fetch"]["exit"] == 1 and \
        result["honest_fetch"]["message"]
    if verdict["missing"]:
        result["missing"] = short(verdict["missing"])
        if refused:
            result["note"] = ("fetch refuses to continue (exit 1): the "
                              "storage holds less than the source, but not "
                              "silently")
        else:
            problems.append(
                f"the honest fetch exited with "
                f"{result['honest_fetch']['exit']}, but the storage lacks "
                f"{short(verdict['missing'])}")
    return finish(result, problems)


# ---------------------------------------------------------------------------
# the storage writer
# ---------------------------------------------------------------------------

BASE_STATE = {}


def storage_base() -> dict:
    """An encrypted rewrite storage of four binlogs (GTID 1 creates the
    table, 2-9 insert) that 'fetch' has just filled. Built once; later
    cases get the same source transactions again and a copy of the stored
    files."""
    binsrv = harness.BINSRV
    harness.SERVER.reset()
    binsrv.reset()
    config = make_config("2K", True)
    insert_many("BASEMARKER", 8, 8)
    if not BASE_STATE:
        must_fetch(config, "storage-base")
        files = tamper.snapshot(binsrv)
        names = binsrv.indexed_binlogs()
        session = PullSession(config, "storage-base")
        try:
            dump = session.dump(names[0]) if session.listening else \
                {"files": {}, "exit": "no pull", "error": ""}
        finally:
            session.close()
        plain = dump["files"]
        stream = examine_stream(plain)
        meta = {name: json.loads(files[name + ".json"]) for name in names}
        tail = names[-1]
        if len(names) < 3 or stream["problems"] or \
                stream["transactions_per_binlog"].get(tail, 0) < 2 or \
                any(len(plain.get(name, b"")) != len(files[name])
                    for name in names) or \
                any(b"BASEMARKER" in files[name] or
                    "encryption" not in meta[name] for name in names):
            raise harness.Failure(
                f"the base storage is not as the cases need it: binlogs "
                f"{names}, per binlog {stream['transactions_per_binlog']}, "
                f"problems {stream['problems'][:2]}, mysqlbinlog "
                f"{dump['exit']} {dump['error']}")
        BASE_STATE.update(files=files, names=names, plain=plain, meta=meta,
                          uuid=harness.SERVER.sql(
                              "SELECT @@server_uuid").strip())
    else:
        for name, data in BASE_STATE["files"].items():
            (binsrv.storage / name).write_bytes(data)
    state = dict(BASE_STATE, config=config)
    state["tail"] = state["names"][-1]
    state["older"] = state["names"][1]
    state["donor"] = state["names"][0]
    state["shifts"] = [0]
    return state


def edit_meta(state: dict, name: str, description: str, **members) -> str:
    meta = dict(state["meta"][name])
    for member, value in members.items():
        if value is None:
            meta.pop(member, None)
        else:
            meta[member] = value
    tamper.put_object(harness.BINSRV, name + ".json",
                      json.dumps(meta, separators=(",", ":")).encode())
    old = {member: state["meta"][name].get(member) for member in members
           if member != "encryption"}
    return f"{name}.json: {description} (was {old})" if old else \
        f"{name}.json: {description}"


def last_transaction_start(state: dict) -> int:
    return [offset for offset, code, _ in
            harness.parse_events(state["plain"][state["tail"]])
            if code in harness.TRANSACTION_START_EVENTS][-1]


def lowered_iv(state: dict) -> dict:
    record = json.loads(json.dumps(state["meta"][state["tail"]]["encryption"]))
    iv = record["file_data_envelope"]["iv_hex"]
    blocks = 64
    state["shifts"].append(blocks * 16)
    record["file_data_envelope"]["iv_hex"] = \
        iv[:16] + f"{(int(iv[16:], 16) - blocks) % (1 << 64):016X}"
    return record


def gtid_edit(which: str, member: str, make, description: str):
    def edit(state: dict) -> str:
        name = state[which]
        value = make(state, state["meta"][name])
        return edit_meta(state, name, f"'{member}' = '{value}' "
                         f"({description})", **{member: value})
    return edit


def last_number(meta: dict) -> int:
    return int(re.split(r"[:-]", meta["added_gtids"])[-1])


STORAGE_EDITS = {
    "control": lambda state: "nothing",
    "tail_added_emptied": gtid_edit(
        "tail", "added_gtids", lambda state, meta: "", "empty"),
    "tail_added_truncated": gtid_edit(
        "tail", "added_gtids",
        lambda state, meta: f"{state['uuid']}:{last_number(meta) - 1}",
        "without the last stored GTID"),
    "tail_added_extended": gtid_edit(
        "tail", "added_gtids",
        lambda state, meta: f"{state['uuid']}:{last_number(meta) - 1}-"
                            f"{last_number(meta) + 2}",
        "with the source's next two GTIDs, which are not stored"),
    "tail_added_other_uuid": gtid_edit(
        "tail", "added_gtids",
        lambda state, meta: OTHER_UUID + ":" +
        meta["added_gtids"].split(":", 1)[1], "another UUID"),
    "tail_previous_emptied": gtid_edit(
        "tail", "previous_gtids", lambda state, meta: "", "empty"),
    "tail_previous_extended": gtid_edit(
        "tail", "previous_gtids",
        lambda state, meta: f"{meta['previous_gtids']}:"
                            f"{last_number(meta) + 1}-"
                            f"{last_number(meta) + 2}",
        "with the source's next two GTIDs, which are not stored"),
    "older_added_emptied": gtid_edit(
        "older", "added_gtids", lambda state, meta: "", "empty"),
    "older_added_extended": gtid_edit(
        "older", "added_gtids",
        lambda state, meta: f"{meta['added_gtids']}:10-11",
        "with the source's next two GTIDs"),
    "older_previous_emptied": gtid_edit(
        "older", "previous_gtids", lambda state, meta: "", "empty"),
    "tail_size_lowered": lambda state: edit_meta(
        state, state["tail"], "'size' lowered to the start of the last "
        "stored transaction, the GTID sets left alone",
        size=last_transaction_start(state)),
    "tail_size_raised": lambda state: edit_meta(
        state, state["tail"], "'size' raised by 100",
        size=state["meta"][state["tail"]]["size"] + 100),
    "older_size_lowered": lambda state: edit_meta(
        state, state["older"], "'size' lowered by 100",
        size=state["meta"][state["older"]]["size"] - 100),
    "tail_encryption_removed": lambda state: edit_meta(
        state, state["tail"], "'encryption' removed", encryption=None),
    "tail_encryption_transplanted": lambda state: edit_meta(
        state, state["tail"], f"'encryption' replaced by the one of "
        f"{state['donor']}",
        encryption=state["meta"][state["donor"]]["encryption"]),
    "tail_iv_lowered": lambda state: edit_meta(
        state, state["tail"], "the block counter of the data IV lowered by "
        "64 (1024 bytes)", encryption=lowered_iv(state)),
}


def keystream_reuse(state: dict, after: dict, plain_after: dict) -> list:
    """Bytes written after the edit that are encrypted with a keystream the
    storage had used before: the old ciphertext and plaintext of a binlog
    give its keystream; if that turns new ciphertext into the plaintext
    'pull' serves now (or into marker rows), it was used twice."""
    found = []
    before = state["files"]
    for old_name in state["names"]:
        old = before[old_name]
        keystream = tamper.xor(old, state["plain"][old_name])
        for new_name in sorted(name for name in after
                               if DATA_NAME.fullmatch(name)):
            new = after[new_name]
            previous = before.get(new_name, b"")
            new_plain = plain_after.get(new_name, b"")
            for shift in state["shifts"]:
                if shift == 0 and old_name != new_name and \
                        state["meta"].get(new_name, {}).get("encryption") \
                        == json.loads(after.get(
                            new_name + ".json", b"{}")).get("encryption"):
                    # another binlog with its own, unchanged key
                    continue
                written = [index for index in range(len(new))
                           if (index >= len(previous) or
                               previous[index] != new[index]) and
                           0 <= index - shift < len(keystream)]
                if len(written) < 32:
                    continue
                recovered = bytes(new[index] ^ keystream[index - shift]
                                  for index in written)
                matching = sum(1 for position, index in enumerate(written)
                               if index < len(new_plain) and
                               recovered[position] == new_plain[index])
                markers = recovered.count(b"SECONDMARKER")
                if matching >= max(32, len(written) // 2) or markers:
                    found.append(
                        f"{len(written)} bytes written to {new_name} after "
                        f"the edit (offsets {written[0]}-{written[-1]}) "
                        f"use the keystream of {old_name}"
                        + (f" {shift} bytes lower" if shift else "") +
                        f": XORed with it, {matching} bytes equal the "
                        f"plaintext 'pull' serves and {markers} new rows "
                        f"can be read")
    return found


def resuming_replica(state: dict, result: dict, name: str) -> list:
    """A replica that already has the GTIDs of the first binlog (and the
    table) connects with auto-positioning: 'pull' picks the binlog to start
    from by the 'added_gtids' of the metadata."""
    have = parse_gtid_set(state["meta"][state["donor"]]["added_gtids"])
    source = parse_gtid_set(source_executed())
    session = PullSession(state["config"], name + "-resume")
    try:
        replica = replica_check(
            source, "SET sql_log_bin = 0; CREATE TABLE qa.t (id INT UNSIGNED "
            "NOT NULL AUTO_INCREMENT PRIMARY KEY, payload LONGBLOB) "
            "ENGINE=InnoDB; SET sql_log_bin = 1; SET @@GLOBAL.gtid_purged = "
            f"'+{format_gtid_set(have)}'")
    finally:
        session.close()
    lacks = subtract(source, replica["executed"])
    result["replica_that_had_the_first_binlog"] = {
        "had": format_gtid_set(have),
        "gtid_executed": short(format_gtid_set(replica["executed"])),
        "errors": replica["errors"], "died": replica.get("died")}
    if replica.get("died") or replica["errors"]:
        return ["the replica that had the first binlog stopped: " +
                str(replica.get("died") or replica["errors"][0])]
    if lacks:
        return [f"a replica that has '{format_gtid_set(have)}' (the first "
                f"binlog) and connects to 'pull' with auto-positioning ends "
                f"with GTID_EXECUTED "
                f"'{short(format_gtid_set(replica['executed']))}', without "
                f"an error: it never gets '{format_gtid_set(lacks)}', the "
                f"transactions of {state['older']}"]
    return []


def run_storage_case(name: str) -> dict:
    binsrv = harness.BINSRV
    state = storage_base()
    config = state["config"]
    result = {"case": name,
              "edit": STORAGE_EDITS[name[len("storage_"):]](state)}
    edited = tamper.snapshot(binsrv)
    result["list"] = brief(command("list", config, name + "-list",
                                   LIST_TIMEOUT))
    insert_many("SECONDMARKER", 3, 6)
    result["fetch"] = brief(command("fetch", config, name + "-fetch"))
    after = tamper.snapshot(binsrv)
    problems = violations(result, "list", "fetch")
    changed = tamper.describe_changes(
        {key: value for key, value in edited.items()
         if DATA_NAME.fullmatch(key)},
        {key: value for key, value in after.items()
         if DATA_NAME.fullmatch(key)})
    result["data_files_changed_by_fetch"] = changed
    untouchable = [change for change in changed
                   if change.split()[0] in state["names"][:-1]]
    if untouchable:
        problems.append("fetch changed binlogs other than the last one: " +
                        "; ".join(untouchable))
    if result["fetch"]["exit"] != 0:
        result["refused"] = True
        if not result["fetch"]["message"]:
            problems.append("fetch exited with "
                            f"{result['fetch']['exit']} without an error")
        return finish(result, problems)
    readable = tamper.find_plaintext(
        {key: value for key, value in after.items()
         if DATA_NAME.fullmatch(key)})
    if readable:
        problems.append("stored in plaintext: " + "; ".join(readable[:2]))
    verdict = judge(config, True, name)
    result.update(verdict["facts"])
    problems += keystream_reuse(state, after, verdict["plain"])[:2]
    problems += verdict["problems"]
    if name in ("storage_control", "storage_older_added_emptied"):
        problems += resuming_replica(state, result, name)
    if verdict["missing"]:
        result["missing"] = short(verdict["missing"])
        tail_meta = json.loads(after[state["tail"] + ".json"])
        problems.append(
            f"fetch exited 0, but the storage lacks "
            f"{short(verdict['missing'])} of the source's transactions; "
            f"the last binlog's metadata lists "
            f"'{tail_meta.get('added_gtids')}' after "
            f"'{tail_meta.get('previous_gtids')}', so they will not be "
            "fetched again")
    return finish(result, problems)


# ---------------------------------------------------------------------------
# crashes
# ---------------------------------------------------------------------------

def recorded_size(storage: Path, name: str) -> "int | None":
    try:
        return int(json.loads((storage / (name + ".json")).read_text())
                   ["size"])
    except (OSError, ValueError, KeyError, TypeError):
        return None


def wait_until_stopped(pid: int) -> None:
    deadline = time.monotonic() + 1
    while time.monotonic() < deadline:
        try:
            stat = Path(f"/proc/{pid}/stat").read_text()
        except OSError:
            return
        if stat.rsplit(")", 1)[1].split()[0] in ("T", "t", "Z", "X"):
            return


def kill_in_window(config: Path, small_block: bool) -> "dict | None":
    """Runs 'fetch' and stops it whenever a data file has grown. If the
    file is then longer than its metadata records (by less than 120 bytes
    with 'small_block'), 'fetch' is killed in that state; otherwise it
    goes on. Returns the file, the recorded and the real size, or None if
    'fetch' ended without such a moment."""
    storage = harness.BINSRV.storage
    stderr_path = harness.BINSRV.workdir / "fetch.stderr"
    with stderr_path.open("wb") as stderr:
        process = subprocess.Popen(
            [str(harness.BINSRV.binary), "fetch", str(config)],
            stdout=subprocess.DEVNULL, stderr=stderr,
            env=tamper.binsrv_environment())
        known = {}
        deadline = time.monotonic() + FETCH_TIMEOUT
        try:
            while process.poll() is None and time.monotonic() < deadline:
                for entry in os.scandir(storage):
                    if not DATA_NAME.fullmatch(entry.name):
                        continue
                    try:
                        size = entry.stat().st_size
                    except OSError:
                        continue
                    if size <= 4 or size == known.get(entry.name):
                        continue
                    os.kill(process.pid, signal.SIGSTOP)
                    wait_until_stopped(process.pid)
                    size = (storage / entry.name).stat().st_size
                    recorded = recorded_size(storage, entry.name)
                    beyond = size - recorded if recorded is not None else 0
                    if beyond > 0 and (beyond < 120 or not small_block):
                        process.kill()
                        process.wait()
                        return {"binlog": entry.name,
                                "size_in_metadata": recorded,
                                "size_of_file": size}
                    known[entry.name] = size
                    os.kill(process.pid, signal.SIGCONT)
        finally:
            if process.poll() is None:
                process.kill()
            process.wait()
    return None


def written_twice(name: str, recorded: int, at_kill: dict, after: dict,
                  plain_after: dict) -> dict:
    """The range of 'name' beyond 'recorded' as the killed fetch left it
    against what the next fetch wrote there."""
    old = at_kill[name][recorded:]
    new = after.get(name, b"")[recorded:recorded + len(old)]
    overlap = min(len(old), len(new))
    differing = [index for index in range(overlap) if old[index] != new[index]]
    same_key = json.loads(at_kill[name + ".json"]).get("encryption") == \
        json.loads(after.get(name + ".json", b"{}")).get("encryption")
    facts = {"bytes_written_twice": overlap,
             "bytes_whose_ciphertext_differs": len(differing),
             "same_key_and_iv": same_key}
    new_plain = plain_after.get(name, b"")
    if not differing or len(new_plain) < recorded + overlap:
        return facts
    # ciphertext XOR ciphertext = plaintext XOR plaintext, if the keystream
    # is the same: then this is the plaintext the killed fetch had written
    old_plain = new_plain[:recorded] + tamper.xor(
        tamper.xor(old[:overlap], new[:overlap]),
        new_plain[recorded:recorded + overlap])
    events = []
    offset = recorded
    valid = total = 0
    while offset + harness.COMMON_HEADER_SIZE <= len(old_plain):
        size = struct.unpack_from("<I", old_plain, offset + 9)[0]
        if size < harness.COMMON_HEADER_SIZE + 4 or \
                offset + size > len(old_plain):
            break
        total += 1
        good = zlib.crc32(old_plain[offset:offset + size - 4]) == \
            struct.unpack_from("<I", old_plain, offset + size - 4)[0]
        valid += good
        changed = [index - offset for index in
                   (position + recorded for position in differing)
                   if offset <= index < offset + size]
        if changed:
            events.append({
                "event_type_before_the_crash": old_plain[offset + 4],
                "event_type_now": new_plain[offset + 4],
                "offset": offset, "size": size,
                "bytes_differing": len(changed),
                "timestamp_before": struct.unpack_from(
                    "<I", old_plain, offset)[0],
                "timestamp_now": struct.unpack_from(
                    "<I", new_plain, offset)[0],
                "checksum_valid": bool(good)})
        offset += size
    facts["events_of_the_reconstructed_old_plaintext"] = total
    facts["of_them_with_a_valid_checksum"] = valid
    facts["events_that_differ"] = events[:6]
    return facts


def run_crash_window_case(name: str) -> dict:
    binsrv = harness.BINSRV
    small_block = name == "crash_rotate_block"
    result = {"case": name}
    problems = []
    killed = None
    for attempt in range(1, 9):
        harness.SERVER.reset()
        binsrv.reset()
        config = make_config("2K", True, "1" if small_block else None)
        insert_many("CRASHMARKER", 40, 8)
        killed = kill_in_window(config, small_block)
        if killed is not None:
            result["attempts"] = attempt
            break
    if killed is None:
        result["skipped"] = ("fetch was never caught with a data file "
                             "longer than its metadata in 8 runs")
        return finish(result, [])
    result["killed"] = killed
    at_kill = tamper.snapshot(binsrv)
    # the generated events carry the clock in seconds
    time.sleep(1.2)
    result["fetch"] = brief(command("fetch", config, name + "-fetch"))
    problems += violations(result, "fetch")
    after = tamper.snapshot(binsrv)
    verdict = judge(config, True, name)
    result.update(verdict["facts"])
    problems += verdict["problems"]
    if result["fetch"]["exit"] != 0:
        problems.append(f"fetch after the crash exited with "
                        f"{result['fetch']['exit']}: "
                        f"{result['fetch']['message']}")
    elif verdict["missing"]:
        problems.append("after the crash and a complete fetch the storage "
                        f"lacks {short(verdict['missing'])}")
    facts = written_twice(killed["binlog"], killed["size_in_metadata"],
                          at_kill, after, verdict["plain"])
    result["written_twice"] = facts
    if facts["bytes_whose_ciphertext_differs"] and facts["same_key_and_iv"]:
        problems.append(
            f"no attacker: fetch was killed with {killed['binlog']} "
            f"{killed['size_of_file'] - killed['size_in_metadata']} bytes "
            f"longer than its metadata; the next fetch wrote "
            f"{facts['bytes_written_twice']} of those offsets again with "
            f"the same key and IV, and "
            f"{facts['bytes_whose_ciphertext_differs']} of them hold "
            f"another plaintext byte than before (the old plaintext, "
            f"rebuilt from the two ciphertexts, has "
            f"{facts.get('of_them_with_a_valid_checksum')} of "
            f"{facts.get('events_of_the_reconstructed_old_plaintext')} "
            f"events with a valid checksum)")
    return finish(result, problems)


def run_crash_random_case(name: str) -> dict:
    binsrv = harness.BINSRV
    harness.SERVER.reset()
    binsrv.reset()
    seed = int(time.time())
    rng = random.Random(seed)
    result = {"case": name, "kills": RANDOM_KILLS, "seed": seed}
    problems = []
    # the first ciphertext seen of every binlog, by name and data IV
    first_seen = {}
    reused = collections.Counter()

    def remember():
        for object_name, data in tamper.snapshot(binsrv).items():
            if not DATA_NAME.fullmatch(object_name):
                continue
            try:
                iv = json.loads((binsrv.storage / (object_name + ".json")
                                 ).read_text())["encryption"][
                                     "file_data_envelope"]["iv_hex"]
            except (OSError, ValueError, KeyError, TypeError):
                continue
            known = first_seen.setdefault((object_name, iv), bytearray())
            common = min(len(known), len(data))
            differing = sum(1 for index in range(common)
                            if known[index] != data[index])
            if differing:
                reused[object_name] += differing
            known += data[len(known):]

    config = make_config("2K", True)
    killed = 0
    for _ in range(RANDOM_KILLS):
        config = make_config("2K", True, rng.choice([None, "1", "1K"]))
        insert_many("CRASHMARKER", 20, 6)
        threshold = rng.randrange(0, 22_000)
        start = stored_bytes()
        process = subprocess.Popen(
            [str(binsrv.binary), "fetch", str(config)],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            env=tamper.binsrv_environment())
        deadline = time.monotonic() + 10
        while process.poll() is None and time.monotonic() < deadline and \
                stored_bytes() - start < threshold:
            time.sleep(0.0005)
        if process.poll() is None:
            process.kill()
            killed += 1
        process.wait()
        if process.returncode not in (0, 1, -signal.SIGKILL):
            problems.append(f"fetch ended with {process.returncode}")
        remember()
        time.sleep(rng.choice([0, 0, 1.1]))
    result["fetches_killed"] = killed
    result["fetch"] = brief(command("fetch", config, name + "-fetch"))
    problems += violations(result, "fetch")
    remember()
    verdict = judge(config, True, name)
    result.update(verdict["facts"])
    problems += verdict["problems"]
    if result["fetch"]["exit"] != 0:
        problems.append(f"the final fetch exited with "
                        f"{result['fetch']['exit']}: "
                        f"{result['fetch']['message']}")
    elif verdict["missing"]:
        problems.append("after the kills and a complete fetch the storage "
                        f"lacks {short(verdict['missing'])}")
    result["bytes_rewritten_with_other_content"] = dict(reused)
    if reused:
        problems.append(
            f"no attacker: {sum(reused.values())} bytes in "
            f"{len(reused)} binlog(s) were written again after a kill "
            f"with another ciphertext under the same key, IV and offset "
            f"({dict(reused)})")
    return finish(result, problems)


# ---------------------------------------------------------------------------
# rotation
# ---------------------------------------------------------------------------

def run_rotation_gtid_intervals(name: str) -> dict:
    binsrv = harness.BINSRV
    harness.SERVER.reset()
    binsrv.reset()
    config = make_config("4K", False)
    intervals = 300
    harness.SERVER.sql("".join(
        f"SET GTID_NEXT='{OTHER_UUID}:{2 * number + 1}'; BEGIN; COMMIT; "
        for number in range(intervals)) + "SET GTID_NEXT='AUTOMATIC';")
    insert_many("ROTATIONMARKER", 30, 4)
    result = {"case": name, "file_size": 4096,
              "gtid_intervals_of_the_source": intervals + 1}
    result["fetch"] = brief(command("fetch", config, name + "-fetch"))
    problems = violations(result, "fetch")
    source_bytes = sum(int(line.split("\t")[1]) for line in
                       harness.SERVER.sql("SHOW BINARY LOGS").splitlines())
    snapshot = tamper.snapshot(binsrv)
    data = sum(len(content) for object_name, content in snapshot.items()
               if DATA_NAME.fullmatch(object_name))
    verdict = judge(config, False, name)
    result.update(verdict["facts"])
    problems += verdict["problems"]
    if verdict["missing"]:
        problems.append(f"the storage lacks {short(verdict['missing'])}")
    stream = examine_stream(verdict["plain"])
    last_files = sorted(stream["transactions_per_binlog"].items())[-30:]
    result["observation"] = {
        "source_binlog_bytes": source_bytes,
        "stored_binlog_bytes": data,
        "stored_bytes_with_metadata": sum(len(content)
                                          for content in snapshot.values()),
        "binlogs_stored": result["binlogs"],
        "binlogs_expected_from_the_size":
            -(-source_bytes // result["file_size"]),
        "transactions_in_each_of_the_last_30_binlogs":
            sorted(set(count for _, count in last_files)),
        "text": "once PREVIOUS_GTIDS_LOG alone exceeds file_size, every "
                "transaction gets a binlog of its own"}
    return finish(result, problems)


def run_rotation_large_transaction(name: str) -> dict:
    binsrv = harness.BINSRV
    harness.SERVER.reset()
    binsrv.reset()
    config = make_config("2K", False)
    insert_many("ROTATIONMARKER", 2, 4)
    harness.SERVER.sql("INSERT INTO qa.t (payload) VALUES "
                       "(REPEAT('L', 200000)), (REPEAT('M', 200000)), "
                       "(REPEAT('N', 200000))")
    insert_many("ROTATIONMARKER", 3, 4)
    result = {"case": name, "file_size": 2048}
    result["fetch"] = brief(command("fetch", config, name + "-fetch"))
    problems = violations(result, "fetch")
    verdict = judge(config, False, name)
    result.update(verdict["facts"])
    problems += verdict["problems"]
    if verdict["missing"]:
        problems.append(f"the storage lacks {short(verdict['missing'])}")
    result["binlog_sizes"] = {object_name: len(content) for
                              object_name, content in
                              verdict["plain"].items()}
    return finish(result, problems)


def run_rotation_sequence_limit(name: str) -> dict:
    binsrv = harness.BINSRV
    harness.SERVER.reset()
    binsrv.reset()
    config = make_config("1K", False)
    result = {"case": name}
    must_fetch(config, name + "-first")
    names = binsrv.indexed_binlogs()
    if names != [BASE + ".000001"]:
        raise harness.Failure(f"expected one stored binlog, got {names}")
    # the storage writer's renaming (a storage that had rotated 999,997
    # times looks the same)
    high = BASE + ".999998"
    for suffix in ("", ".json"):
        (binsrv.storage / (names[0] + suffix)).rename(
            binsrv.storage / (high + suffix))
    (binsrv.storage / "binlog.index").write_text(f"./{high}\n")
    result["edit"] = f"{names[0]} renamed to {high} (file, metadata, index)"
    insert_many("LIMITMARKER", 8, 8)
    problems = []
    runs = []
    for run in range(4):
        fetched = brief(command("fetch", config, f"{name}-fetch{run}"))
        result[f"fetch{run}"] = fetched
        problems += violations(result, f"fetch{run}")
        runs.append((fetched["exit"], fetched["message"][-90:]))
    result["storage_entries"] = sorted(os.listdir(binsrv.storage))
    strangers = [entry for entry in result["storage_entries"]
                 if not STORAGE_NAME.fullmatch(entry) or
                 DATA_NAME.fullmatch(entry.replace(".json", "")) and
                 entry.split(".")[1] not in ("999998", "999999")]
    if strangers:
        problems.append("after the last name the storage got: " +
                        ", ".join(strangers))
    # 'pull' fetches as well and stops with the same error
    verdict = judge(config, False, name, serve=False)
    result.update(verdict["facts"])
    problems += verdict["problems"]
    result["missing"] = short(verdict["missing"])
    if verdict["missing"] and any(code == 0 for code, _ in runs[-1:]):
        problems.append("the last fetch exited 0, but the storage lacks "
                        + short(verdict["missing"]))
    result["observation"] = {
        "fetch_runs": runs,
        "text": f"after {BASE}.999999 every fetch stops with an error at "
                "the next rotation (the transactions it had received by "
                "then are stored); nothing is wrapped or overwritten"}
    return finish(result, problems)


# ---------------------------------------------------------------------------

CASES = {"source_control": run_source_case}
CASES.update({f"source_name_{name}": run_source_case for name in NAME_CASES})
CASES.update({f"source_{name}": run_source_case
              for name in hostile.RESOURCE_CASES + TRANSACTION_ATTACKS +
              ["checksum_none"]})
CASES.update({f"storage_{name}": run_storage_case for name in STORAGE_EDITS})
CASES.update({
    "crash_file_block": run_crash_window_case,
    "crash_rotate_block": run_crash_window_case,
    "crash_random_kills": run_crash_random_case,
    "rotation_gtid_intervals": run_rotation_gtid_intervals,
    "rotation_large_transaction": run_rotation_large_transaction,
    "rotation_sequence_limit": run_rotation_sequence_limit,
})


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--mysql-basedir", type=Path, required=True)
    parser.add_argument("--binsrv", type=Path, required=True)
    parser.add_argument("--workdir", type=Path, required=True)
    parser.add_argument("--port", type=int, default=23900,
                        help="the source's port; the replica, 'pull' and "
                             "the proxy use the three ports above it")
    parser.add_argument("--mode", choices=["gtid"], default="gtid",
                        help="rewrite needs GTID mode")
    parser.add_argument("--cases", nargs="*", choices=list(CASES),
                        help="the cases to run (default: all)")
    parser.add_argument("--random-kills", type=int, default=10,
                        help="kills of 'crash_random_kills' (a few dozen "
                             "take a few minutes)")
    args = parser.parse_args()
    args.workdir = args.workdir.expanduser()
    args.workdir.mkdir(parents=True, exist_ok=True)
    global REPLICA, MYSQL_BASEDIR, PULL_PORT, PROXY_PORT  # pylint: disable=global-statement
    global RANDOM_KILLS  # pylint: disable=global-statement
    MYSQL_BASEDIR = args.mysql_basedir.expanduser()
    PULL_PORT = args.port + 2
    PROXY_PORT = args.port + 3
    RANDOM_KILLS = args.random_kills
    harness.SERVER = harness.MySQLServer(MYSQL_BASEDIR,
                                         args.workdir / "mysql", args.port,
                                         True, server_id=1)
    REPLICA = harness.MySQLServer(MYSQL_BASEDIR, args.workdir / "replica",
                                  args.port + 1, True, server_id=2)
    harness.BINSRV = harness.BinlogServer(args.binsrv.expanduser(),
                                          args.workdir / "binsrv", args.port,
                                          True, None)
    cases = args.cases or list(CASES)
    results = []
    started = time.monotonic()
    # a replica that died of what it was sent may die again when it starts
    shutil.rmtree(REPLICA.datadir, ignore_errors=True)
    harness.SERVER.start()
    try:
        REPLICA.start()
        for name in cases:
            case_started = time.monotonic()
            try:
                result = CASES[name](name)
            except (harness.Failure, RuntimeError,
                    subprocess.SubprocessError, OSError) as failure:
                result = {"case": name, "ok": False,
                          "why": harness.describe_failure(failure, 600)}
            result["seconds"] = round(time.monotonic() - case_started, 1)
            print(json.dumps(result), flush=True)
            results.append(result)
    finally:
        REPLICA.stop()
        harness.SERVER.stop()
    failures = [result for result in results if not result.get("ok")]
    print(json.dumps({"cases": len(results), "failures": len(failures),
                      "failed": [result["case"] for result in failures],
                      "seconds": round(time.monotonic() - started),
                      "passed": not failures}), flush=True)
    (args.workdir / "result.json").write_text(json.dumps(results, indent=2))
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
