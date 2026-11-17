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

"""What an operator can do with a storage that no later run can continue.

Several defects (and two honest accidents) leave a storage on which every
'fetch' and every start of 'pull' exits 1, or which is silently wrong. The
Binlog Server has no 'verify' or 'repair' command, so the operator repairs
the files by hand. This harness produces each such state with the code of
the harness that found it, then tries the repairs an operator would
plausibly try, each on a fresh copy of the damaged storage directory, and
judges what the repair leaves behind.

States ('--cases' selects by substring of '<state>/<repair>'):

  nul_existing_binlog, nul_metadata, nul_index, newline
                    hostile_source_test.py: a binlog name from the source
                    with a NUL byte or a line feed (ticket 59)
  long_name_pull    a ROTATE with a 3000-byte name, against a running 'pull'
                    (hostile_source_pull_test.py's proxy; ticket 59)
  gtid_bloat_pull   a PREVIOUS_GTIDS_LOG of 900,000 intervals, against a
                    running 'pull' (--no-rewrite only)
  reannounce, reannounce_midstream
                    the open binlog announced again (ticket 69)
  lower_sequence    a binlog with a lower sequence number than the first
                    stored one (ticket 70)
  cutoff_stub       truncated_transaction_test.py 'lie_after_begin': the
                    first events of a transaction stored as all of it
                    (ticket 82)
  fragments_pull    the proxy ends the dump after GTID + BEGIN, every time,
                    against a running 'pull', which is then killed (ticket
                    83; --all: on 2026-11-17 the storage this leaves was
                    exact after the next 'fetch')
  short_tail        the last binlog's data file is shorter than its metadata
                    says (lost page cache, a cut-off upload; like ticket 19)
  source_reset      the source was initialised again and has fewer binlogs
                    than the storage (ticket 25)
  source_reset_longer
                    ... and by now has more binlogs and GTIDs than the
                    storage (ticket 25)
  source_gap        the source purged binlogs that were not fetched yet
  interrupted_rotation
                    the newest binlog holds the binlog magic only and is not
                    in binlog.index, the one before it ends with its ROTATE:
                    what a kill during a rotation leaves, and what the
                    start-up recovery of the README is for
  tail_deleted      the newest stored binlog's data and metadata files were
                    deleted by mistake; binlog.index still lists it
  tail_deleted_index_too
                    ... and its line of binlog.index as well
  gtid_claims_extended, gtid_size_lowered
                    GTID mode: the last binlog's metadata lists GTIDs the
                    files do not hold (tickets 81 / 63)
  healthy           nothing is wrong (the control of the judge; its R7 shows
                    what 'pull' serves when it cannot reach its source)

Repairs (each ends with 'fetch', twice if the first run fails, and once more
after a run that exits 0):

  R1         nothing: just run again (the baseline: is the state stuck, what
             does the message say, does it name the object and what to do)
  R2         delete the last line of binlog.index and that binlog's data and
             metadata files
  R3         cut the last binlog back to the last good transaction boundary
             and set 'size', 'added_gtids' and 'last_sequence_number' of its
             metadata to match (a line of binlog.index that names no stored
             binlog is removed first). The boundary is what a 'mysqlbinlog
             <file>' shows as the end_log_pos of the last Xid / COMMIT before
             the first Rotate or second Start event; the harness computes it
             itself and records whether mysqlbinlog's output gives the same
             number. With --encrypted the operator cannot read the file: the
             harness unwraps the file key and decrypts with 'openssl enc', as
             an operator with the keyring would have to
  R2+R3      R2, then R3 on the binlog that is the last one after it
  R4         delete everything from the damaged binlog onward (and every
             object binlog.index then no longer names)
  R4+R3      R4, then R3
  R5-before, R5-after
             'purge_binlogs' up to the binlog before the damaged one / up to
             the damaged one
  R6         write metadata.json again (as a healthy storage of this mode has
             it) and rebuild binlog.index from the directory listing
  R6+R3      R6, then R3
  R7         leave the storage alone, start a fresh one next to it; the old
             one is listed and served by a 'pull' whose source address is
             unreachable
  R8, R8-late  (the states whose metadata lists GTIDs the files lack) set
             'added_gtids' of every binlog to the GTIDs its file holds;
             R8-late does so only after a 'fetch' has already appended to
             the storage
  R9         (not an obvious one; it is what the start-up recovery after an
             interrupted rotation relies on) make the last binlog an empty
             one: its data file the four bytes of the binlog magic, its
             metadata 'size' 4 and no added GTIDs, its line kept in
             binlog.index; 'fetch' then fills the binlog of that name again
             from its start

After a repair the storage is read back (every indexed binlog up to its
metadata size) and compared, transaction by transaction, with everything the
source ever committed (recorded before the source purges or resets
anything); in position mode a complete storage must also be byte-identical
to the source's files. 'list', 'search_by_timestamp' and 'search_by_gtid_set'
must agree with the files. A replica that replicates from 'pull' from
scratch (auto-position in GTID mode) and a 'mysqlbinlog
--read-from-remote-server --raw' through 'pull' must get the same: in GTID
mode after every repair that makes 'fetch' run, in position mode after the
first exact repair of a state and after every inexact one; a storage that
holds a transaction that is not the source's is served only once per state,
after R1 (--replica-all: always).

In rewrite mode the Binlog Server writes its own binlog files, so
transactions that are fetched again may land at other positions than
before. When the end of the storage before the damage is no longer the same
point of the stream, a replica that has exactly what the storage held then
is pointed at that file and position (SOURCE_AUTO_POSITION=0), as a replica
that followed 'pull' by position would continue; if it ends with other data
than the source and no error, the repair is SILENTLY-WRONG (one such replica
per state and kind of move; --no-resume-check: the move is only recorded).

Outcome of every (state, repair):

  RECOVERS-EXACT             'fetch' runs and the storage holds everything
  RECOVERS-WITH-VISIBLE-GAP  'fetch' runs, what is stored is the source's,
                             and 'list' / 'search_by_gtid_set' / the two
                             storages show what is missing ('tells')
  STILL-STUCK                'fetch' still exits 1
  SILENTLY-WRONG  (fails)    'fetch' exits 0 but the storage holds something
                             that is not the source's, holds it twice, lacks
                             transactions without any sign, lists GTIDs it
                             does not hold, or is not served as it is stored
  DESTROYS-MORE   (fails)    transactions the damaged storage still held are
                             gone and not fetched again

One result line per (state, repair), 'case' '<mode>/<state>/<repair>', and
one per state, 'case' '<mode>/<state>', which passes when at least one repair
recovers (exactly or with a visible gap) and no repair is SILENTLY-WRONG
(R1 is not counted as a repair in the states that are known to be wrong
rather than stuck); a state that this binary does not reproduce is reported
as such and passes.

--mode position (default) or gtid. GTID mode runs with 'replication.rewrite'
(mandatory since PBS-80 / PBS-98; --rewrite is accepted and implied);
--no-rewrite runs GTID mode without it, for binaries built before that.
--encrypted runs on an encrypted storage (without the states produced by
hostile_source_test.py and truncated_transaction_test.py, which read the
stored files as plaintext). --all adds the states and
combinations that rarely matter. The default run takes about ten minutes.

Requirements as for truncated_transaction_test.py, plus 'openssl' with
--encrypted. Example:

  PYTHONPATH=~/ws/qa/pylib python3 stuck_storage_recovery_test.py \\
    --mysql-basedir ~/ws/dist/mysql-8.4.8-linux-glibc2.28-x86_64-minimal \\
    --binsrv ~/ws/percona-binlog-server-build-debug_gcc14/binlog_server \\
    --workdir ~/ws/qa/stuck --port 24300 --mode gtid
"""

import argparse
import collections
import json
import os
import re
import shutil
import signal
import struct
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import fetch_crash_test as harness  # noqa: E402
import hostile_source_pull_test as hostile_pull  # noqa: E402
import hostile_source_test as hostile  # noqa: E402
import metadata_tamper_test as tamper  # noqa: E402
import pull_replica_test as replication  # noqa: E402
import rewrite_attack_test as rw  # noqa: E402
import truncated_transaction_test as tt  # noqa: E402

NAME = re.compile(rb"binlog\.\d{6}")
TEXT_NAME = re.compile(r"binlog\.\d{6}")
OBJECT_IN_MESSAGE = re.compile(r"binlog\.\d{6}|binlog\.index|metadata\.json")
ADVICE_IN_MESSAGE = re.compile(
    r"\b(remove|delete|restore|repair|truncate|purge|re-?create|rename|"
    r"please|must be|should be|use )", re.IGNORECASE)
FETCH_TIMEOUT = 90
TRANSACTION_PAYLOAD_EVENT = 40
FAR_FUTURE = "2037-01-01T00:00:00"
GOOD = ("RECOVERS-EXACT", "RECOVERS-WITH-VISIBLE-GAP")
BAD = ("SILENTLY-WRONG", "DESTROYS-MORE")
REPAIRS = ["R1", "R2", "R3", "R2+R3", "R4", "R4+R3", "R5-before", "R5-after",
           "R6", "R6+R3", "R7", "R8", "R8-late", "R9"]

# set by main()
ARGS = None
GTID = False
REWRITE = False
LABEL = "position"
DEAD_PORT = 0
PROXY_PORTS = []
METADATA_TEMPLATE = b""
# everything the source ever committed, in commit order: the transactions of
# tt.read_transactions(); EPOCH counts the source's re-initialisations
HISTORY = []
HISTORY_EPOCH = []
BY_KEY = {}
SEEN = {}
EPOCH = 0


# ---------------------------------------------------------------------------
# configuration and commands
# ---------------------------------------------------------------------------

def next_proxy_port() -> int:
    PROXY_PORTS.append(PROXY_PORTS.pop(0))
    return PROXY_PORTS[-1]


def config(name: str = "direct", storage: "Path | None" = None,
           source_port: "int | None" = None, proxied: bool = False) -> Path:
    """A configuration file: direct to the source unless 'source_port' says
    otherwise ('proxied': without TLS), on the storage of the harness or
    on 'storage'."""
    binsrv = harness.BINSRV
    path = tt.make_config(f"{name}.json")
    settings = json.loads(path.read_text())
    settings["connection"]["connect_timeout"] = 3
    if source_port is not None:
        settings["connection"]["port"] = source_port
    if proxied:
        settings["connection"]["ssl"] = {"mode": "disabled"}
    if storage is not None:
        settings["storage"]["uri"] = f"file://{storage}"
    path.write_text(json.dumps(settings, indent=2))
    return path


def log_warnings() -> list:
    binsrv = harness.BINSRV
    if not binsrv.log_path.exists():
        return []
    return [line.split("] ", 2)[-1].strip()[:200] for line in
            binsrv.log_path.read_text(errors="replace").splitlines()
            if "[warning]" in line][:3]


def run_fetch(settings: Path, label: str) -> dict:
    """'fetch', again if it fails, and once more after a run that exits 0
    (a repair must not work only once)."""
    runs = []
    warnings = []
    for attempt in range(2):
        run = tamper.run_command(harness.BINSRV, "fetch", settings,
                                 FETCH_TIMEOUT, f"{label}-{attempt}")
        runs.append({"exit": run["exit"], "message": run["message"][:300]})
        if "sanitizer" in run:
            runs[-1]["sanitizer"] = run["sanitizer"]
        warnings += log_warnings()
        if run["exit"] == 0:
            break
    result = {"ok": runs[-1]["exit"] == 0, "runs": runs}
    if result["ok"]:
        again = tamper.run_command(harness.BINSRV, "fetch", settings,
                                   FETCH_TIMEOUT, f"{label}-again")
        if again["exit"] != 0:
            result["ok"] = False
            runs.append({"exit": again["exit"],
                         "message": again["message"][:300],
                         "note": "the run after a run that exited 0"})
    if warnings:
        result["warnings"] = warnings[:3]
    return result


def must_fetch(settings: Path, label: str) -> None:
    rw.must_fetch(settings, label)


# ---------------------------------------------------------------------------
# the source's history
# ---------------------------------------------------------------------------

def begin_state() -> None:
    global EPOCH  # pylint: disable=global-statement
    HISTORY.clear()
    HISTORY_EPOCH.clear()
    BY_KEY.clear()
    SEEN.clear()
    EPOCH = 0
    binsrv = harness.BINSRV
    binsrv.checkpoint_size = None
    binsrv.checkpoint_interval = None
    binsrv.idle_time = 1
    binsrv.read_timeout = 60
    CONSUMED.clear()


def fresh() -> None:
    begin_state()
    harness.SERVER.reset()
    harness.BINSRV.reset()


def remember() -> set:
    """Adds the transactions of the source's binlogs to the history;
    returns the history numbers of those the source has right now."""
    now = set()
    for transaction in tt.source_transactions():
        key = (EPOCH, transaction["key"])
        if key not in SEEN:
            SEEN[key] = len(HISTORY)
            HISTORY.append(transaction)
            HISTORY_EPOCH.append(EPOCH)
            BY_KEY.setdefault(transaction["key"], []).append(SEEN[key])
        now.add(SEEN[key])
    return now


def new_epoch() -> None:
    """The source is initialised again: remembered first."""
    global EPOCH  # pylint: disable=global-statement
    remember()
    EPOCH += 1
    harness.SERVER.reset()


def history_number(transaction: dict) -> "int | None":
    for number in BY_KEY.get(transaction["key"], []):
        known = HISTORY[number]
        if known["digests"] == transaction["digests"] and \
                known["codes"] == transaction["codes"]:
            return number
    return None


def keys(numbers) -> str:
    """History numbers as GTIDs / 'file:offset' keys, shortened."""
    numbers = sorted(numbers)
    gtids = [HISTORY[number]["gtid"] for number in numbers]
    if gtids and all(gtids) and \
            len({HISTORY_EPOCH[number] for number in numbers}) == 1:
        return rw.short(rw.format_gtid_set(rw.as_set(gtids)), 120)
    shown = [HISTORY[number]["key"] for number in numbers[:3]]
    return ", ".join(shown) + (f" and {len(numbers) - 3} more"
                               if len(numbers) > 3 else "")


# ---------------------------------------------------------------------------
# the storage as files
# ---------------------------------------------------------------------------

def raw_index(storage: Path) -> list:
    try:
        return [line for line in
                (storage / "binlog.index").read_bytes().split(b"\n") if line]
    except OSError:
        return []


def write_index(storage: Path, lines: list) -> None:
    (storage / "binlog.index").write_bytes(
        b"".join(line + b"\n" for line in lines))


def entry_name(line: bytes) -> bytes:
    return line.rsplit(b"/", 1)[-1]


def indexed(storage: Path) -> list:
    """The binlogs binlog.index names that are stored, in its order."""
    names = []
    for line in raw_index(storage):
        name = entry_name(line)
        if NAME.fullmatch(name) and (storage / name.decode()).is_file():
            names.append(name.decode())
    return names


def metadata_of(storage: Path, name: str) -> "dict | None":
    try:
        value = json.loads((storage / (name + ".json")).read_text())
    except (OSError, ValueError):
        return None
    return value if isinstance(value, dict) else None


def openssl(cipher: str, key_hex: str, iv_hex: str, data: bytes) -> bytes:
    return subprocess.run(
        ["openssl", "enc", "-d", "-" + cipher.lower(), "-K", key_hex, "-iv",
         iv_hex, "-nopad"], input=data, capture_output=True,
        check=True).stdout


def decrypted(data: bytes, record: dict) -> bytes:
    """What an operator with the keyring can do without the Binlog Server:
    unwrap the file key with the KEK, then decrypt the data with it."""
    key = record["file_key_envelope"]
    envelope = record["file_data_envelope"]
    kek = next(entry for entry in tamper.KEYRING["keys"]
               if entry["id"] == key["kek_id"])
    file_key = openssl(kek["cipher"], kek["data_hex"], key["iv_hex"],
                       bytes.fromhex(key["data_hex"]))
    return openssl(envelope["cipher"], file_key.hex(), envelope["iv_hex"],
                   data)


def stored_plain(storage: Path, name: str, cut: bool = True) -> bytes:
    """A stored binlog in plaintext, up to its metadata size."""
    data = (storage / name).read_bytes()
    metadata = metadata_of(storage, name) or {}
    if isinstance(metadata.get("encryption"), dict):
        try:
            data = decrypted(data, metadata["encryption"])
        except (subprocess.SubprocessError, KeyError, ValueError,
                StopIteration, OSError):
            return b""
    if cut and isinstance(metadata.get("size"), int):
        data = data[:metadata["size"]]
    return data


def successor(name: str) -> str:
    return "binlog.%06d" % (int(name[-6:]) + 1)


def complete(data: bytes, group: list) -> bool:
    """The events after a GTID event are a whole transaction."""
    if not group:
        return False
    if group[0][1] == TRANSACTION_PAYLOAD_EVENT:
        return True
    if group[0][1] != rw.QUERY_EVENT:
        return False
    if rw.query_text(data, group[0]) != b"BEGIN":
        return len(group) == 1
    return len(group) > 1 and (
        group[-1][1] == rw.XID_EVENT or
        rw.query_text(data, group[-1]) == b"COMMIT")


def good_boundary(data: bytes, name: str, keep_rotate: bool = True) -> int:
    """The length of the longest prefix of a binlog that is one preamble
    followed by whole transactions (and, with 'keep_rotate', the ROTATE to
    the next binlog, if that is what ends it)."""
    try:
        events = harness.parse_events(data)
    except harness.Failure:
        return 0
    boundary = 4
    index = 0
    while index < len(events):
        offset, code, size = events[index]
        if code == harness.FORMAT_DESCRIPTION_EVENT:
            if index != 0:
                break
        elif code == harness.PREVIOUS_GTIDS_LOG_EVENT:
            if index != 1:
                break
        elif code in (harness.ROTATE_EVENT, harness.STOP_EVENT):
            target = data[offset + 27:offset + size - 4].decode(
                errors="replace")
            if index == len(events) - 1 and keep_rotate and (
                    code == harness.STOP_EVENT or target == successor(name)):
                boundary = offset + size
            break
        elif code in harness.TRANSACTION_START_EVENTS:
            end = index + 1
            while end < len(events) and events[end][1] not in \
                    harness.TRANSACTION_START_EVENTS and \
                    events[end][1] not in rw.CONTROL_EVENTS:
                end += 1
            if not complete(data, events[index + 1:end]):
                break
            index = end - 1
            offset, code, size = events[index]
        else:
            break
        boundary = offset + size
        index += 1
    return boundary


def structure(files: dict) -> list:
    """What is wrong with the stored binlogs as binlog files."""
    problems = []
    for name, data in files.items():
        try:
            events = harness.parse_events(data)
        except harness.Failure:
            problems.append(f"{name} ({len(data)} bytes) does not start "
                            "with the binlog magic")
            continue
        end = events[-1][0] + events[-1][2] if events else 4
        if end != len(data):
            problems.append(f"{name}: {len(data) - end} bytes of a partial "
                            f"event after {end}")
        codes = [code for _, code, _ in events]
        if codes.count(harness.FORMAT_DESCRIPTION_EVENT) > 1:
            problems.append(f"{name} holds a second FORMAT_DESCRIPTION")
        closing = [index for index, code in enumerate(codes)
                   if code in (harness.ROTATE_EVENT, harness.STOP_EVENT)]
        if closing and closing[0] != len(events) - 1:
            problems.append(
                f"{name}: {len(events) - 1 - closing[0]} event(s) after the "
                f"ROTATE / STOP at {events[closing[0]][0]}")
        elif closing and codes[-1] == harness.ROTATE_EVENT:
            offset, _, size = events[-1]
            target = data[offset + 27:offset + size - 4]
            if target.decode(errors="replace") != successor(name):
                problems.append(f"{name} ends with a ROTATE to "
                                f"{bytes(target[:40])!r}")
        if good_boundary(data, name) < end and not any(
                name in problem for problem in problems):
            problems.append(f"{name}: a cut-off transaction or a stray "
                            f"event after {good_boundary(data, name)}")
    return problems


def examine(storage: Path) -> dict:
    """The transactions of a storage against the history."""
    names = indexed(storage)
    files = {name: stored_plain(storage, name) for name in names}
    stored = tt.read_transactions(files, names)
    sequence = []
    junk = []
    for transaction in stored:
        number = history_number(transaction)
        if number is None:
            junk.append(
                f"{transaction['key']} "
                f"({'+'.join(map(tt.event_name, transaction['codes']))} at "
                f"{transaction['file']}:{transaction['offset']})")
        else:
            sequence.append(number)
    counts = collections.Counter(sequence)
    stream = rw.examine_stream({name: data for name, data in files.items()
                                if len(data) > 4})
    return {
        "names": names, "files": files, "held": set(sequence),
        "sequence": sequence, "junk": junk,
        "twice": [HISTORY[number]["key"] for number, count in counts.items()
                  if count > 1],
        "ordered": sequence == sorted(sequence),
        "gtids_in_files": stream["gtids"],
        "chain": [problem for problem in stream["problems"]
                  if "PREVIOUS_GTIDS_LOG is" in problem],
        "structure": structure(files) + [
            problem for problem in stream["problems"]
            if "checksum" in problem or "end position" in problem or
            "declares" in problem]}


def byte_differences(storage: Path, names: list) -> list:
    """Position mode: the stored files against the source's."""
    server = harness.SERVER
    differences = []
    source_names = server.binlog_names()
    for name in source_names:
        source = server.binlog_as_sent(name)
        empty_tail = name == source_names[-1] and \
            not harness.holds_transactions(source)
        if name not in names:
            if not empty_tail:
                differences.append(f"{name} of the source is not stored")
            continue
        stored = stored_plain(storage, name, cut=False)
        if stored == source or (empty_tail and
                                stored == harness.BINLOG_MAGIC):
            continue
        differences.append(
            f"{name}: {len(stored)} bytes stored, the source's has "
            f"{len(source)}, first difference at byte "
            f"{hostile_pull.common_prefix(stored, source)}")
    return differences


def restore(copy: Path, storage: Path) -> None:
    shutil.rmtree(storage, ignore_errors=True)
    shutil.copytree(copy, storage)


# ---------------------------------------------------------------------------
# 'list', the searches, 'pull' and its clients
# ---------------------------------------------------------------------------

def queries(settings: Path, storage: Path, exam: dict, lost: list,
            whole: bool) -> dict:
    """Whether 'list' and the searches agree with the files: 'problems'
    (they do not), 'claims' (they name what no file holds) and 'tells'
    (they show what is missing)."""
    out = {"problems": [], "claims": [], "tells": []}
    listing = rw.query(settings, "list")
    out["list_exit"] = listing["exit"]
    if listing["exit"] != 0 or listing["status"] != "success":
        out["problems"].append(f"'list' fails: {listing['message'][:160]}")
        return out
    entries = [entry for entry in listing["result"]
               if isinstance(entry, dict)]
    names = [entry.get("name") for entry in entries]
    if names != exam["names"]:
        out["problems"].append(f"'list' names {names[-3:]}, binlog.index "
                               f"{exam['names'][-3:]}")
    for entry in entries:
        path = storage / str(entry.get("name"))
        if path.is_file() and path.stat().st_size != entry.get("size"):
            out["problems"].append(
                f"'list' gives {entry.get('name')} {entry.get('size')} "
                f"bytes, the file has {path.stat().st_size}")
    numbers = [int(name[-6:]) for name in exam["names"]]
    for before, after in zip(numbers, numbers[1:]):
        if after != before + 1:
            out["tells"].append("'list' and binlog.index go from binlog."
                                f"{before:06d} to binlog.{after:06d}")
    if GTID and entries:
        claimed = {}
        expected = rw.parse_gtid_set(str(entries[0].get("previous_gtids")
                                         or ""))
        for entry in entries:
            previous = rw.parse_gtid_set(str(entry.get("previous_gtids")
                                             or ""))
            added = rw.parse_gtid_set(str(entry.get("added_gtids") or ""))
            if previous != expected:
                out["tells"].append(
                    f"'list': previous_gtids of {entry.get('name')} is "
                    f"'{rw.short(rw.format_gtid_set(previous), 80)}', the "
                    "binlogs before it add up to "
                    f"'{rw.short(rw.format_gtid_set(expected), 80)}'")
            expected = rw.union(previous, added)
            claimed = rw.union(claimed, added)
        holes = {sid: set(range(1, max(found) + 1)) - found
                 for sid, found in expected.items()}
        holes = {sid: found for sid, found in holes.items() if found}
        if holes:
            out["tells"].append("'list': no binlog's GTID sets name "
                                f"{rw.short(rw.format_gtid_set(holes), 100)}")
        actual = rw.as_set(exam["gtids_in_files"])
        more = rw.subtract(claimed, actual)
        less = rw.subtract(actual, claimed)
        if more:
            out["claims"].append(
                f"'list' names {rw.short(rw.format_gtid_set(more), 100)} in "
                "added_gtids, but no stored binlog holds them")
        if less:
            out["problems"].append(
                "the stored binlogs hold "
                f"{rw.short(rw.format_gtid_set(less), 100)}, which 'list' "
                "does not name")
        lost_gtids = [HISTORY[number]["gtid"] for number in lost
                      if HISTORY[number]["gtid"] and
                      HISTORY_EPOCH[number] == EPOCH]
        if lost_gtids:
            wanted = rw.format_gtid_set(rw.as_set(lost_gtids))
            search = rw.query(settings, "search_by_gtid_set", wanted)
            if search["status"] == "success":
                out["claims"].append(
                    f"'search_by_gtid_set {rw.short(wanted, 80)}' answers "
                    "success, but those transactions are not stored")
            else:
                out["tells"].append(
                    f"'search_by_gtid_set {rw.short(wanted, 80)}': "
                    f"{search['message'][:100]}")
        elif EPOCH == 0 and whole and not harness.SERVER.sql(
                "SELECT @@GLOBAL.gtid_purged").strip():
            search = rw.query(settings, "search_by_gtid_set",
                              rw.source_executed())
            if search["status"] != "success":
                out["problems"].append(
                    "'search_by_gtid_set <the source's gtid_executed>': "
                    f"{search['message'][:120]}")
    stamp = rw.query(settings, "search_by_timestamp", FAR_FUTURE)
    found = [entry.get("name") for entry in stamp["result"]
             if isinstance(entry, dict)]
    if stamp["status"] == "success" and found != names:
        out["problems"].append(f"'search_by_timestamp {FAR_FUTURE}' names "
                               f"{len(found)} binlogs, 'list' {len(names)}")
    return out


def serve(settings: Path, label: str, storage: Path,
          with_replica: bool = True) -> dict:
    """'pull' on the storage: what 'mysqlbinlog --read-from-remote-server
    --raw' gets, and what a replica that starts from scratch ends with."""
    names = indexed(storage)
    if not names:
        return {"pull": "nothing to serve"}
    result = {}
    session = rw.PullSession(settings, label)
    try:
        time.sleep(0.3)
        if not session.listening or session.process.poll() is not None:
            result["pull"] = "does not serve the storage"
        else:
            dump = session.dump(names[0])
            dumped = tt.read_transactions(dump["files"],
                                          sorted(dump["files"]))
            numbers = [history_number(transaction) for transaction in dumped]
            result["dump_held"] = {number for number in numbers
                                   if number is not None}
            result["mysqlbinlog"] = {
                "exit": dump["exit"], "binlogs": len(dump["files"]),
                "transactions": len(dumped),
                "not_the_sources": sum(number is None for number in numbers)}
            if dump["error"]:
                result["mysqlbinlog"]["error"] = dump["error"][:200]
            if with_replica:
                start = ("SOURCE_AUTO_POSITION=1" if GTID else
                         f"SOURCE_LOG_FILE='{names[0]}', SOURCE_LOG_POS=4")
                source_rows = rw.rows_digest(harness.SERVER)
                wanted = rw.parse_gtid_set(rw.source_executed()) \
                    if GTID else None
                replica = tt.observe_replica(
                    lambda: replication.replicate_from_pull(
                        rw.REPLICA, rw.PULL_PORT, GTID, start),
                    source_rows, label, wanted)
                result["replica"] = {
                    key: replica[key] for key in
                    ("rows", "source_rows", "gtid_executed", "errors",
                     "died", "hangs", "seconds") if replica.get(key)}
                equal = replica.get("rows") == source_rows and not any(
                    replica.get(key) for key in ("errors", "died", "hangs"))
                if equal and GTID:
                    equal = rw.parse_gtid_set(rw.REPLICA.sql(
                        "SELECT @@GLOBAL.gtid_executed")) == wanted
                    result["replica"]["source_gtid_executed"] = rw.short(
                        rw.source_executed())
                result["replica_equal"] = equal
    finally:
        ended = session.close()
    if "pull" in result:
        result["pull"] += ": " + str(ended.get("error", ""))[-260:]
    elif ended["exit"] is not None:
        result["pull_ended"] = (f"exit {ended['exit']}: "
                                f"{str(ended.get('error', ''))[-200:]}")
    return result


def read_before(storage: Path, exam: dict) -> "set | None":
    """The history numbers of the transactions that lie before the position
    CONSUMED in the storage as it is now; None if that position is not the
    end of an event there any more."""
    name, position = CONSUMED["binlog"], CONSUMED["position"]
    if name not in exam["names"]:
        return None
    data = exam["files"][name]
    ends = {4} | {offset + size for offset, _, size in
                  harness.parse_events(data)}
    if position not in ends:
        return None
    files = {earlier: exam["files"][earlier]
             for earlier in exam["names"][:exam["names"].index(name)]}
    files[name] = data[:position]
    return {history_number(transaction) for transaction in
            tt.read_transactions(files, list(files))} - {None}


def resumed_replica(settings: Path, label: str) -> dict:
    """A replica that has what CONSUMED says and continues from that
    position of 'pull', without auto-positioning."""
    replica = rw.REPLICA
    session = rw.PullSession(settings, label)
    result = {"resumes_at": f"{CONSUMED['binlog']}:{CONSUMED['position']}"}
    try:
        common = (
            "SOURCE_HOST='127.0.0.1', "
            f"SOURCE_PORT={rw.PULL_PORT}, "
            f"SOURCE_USER='{replication.PULL_USER}', "
            f"SOURCE_PASSWORD='{replication.PULL_PASSWORD}', "
            "GET_SOURCE_PUBLIC_KEY=1, SOURCE_SSL=0, SOURCE_CONNECT_RETRY=2")
        replica.sql("STOP REPLICA; RESET REPLICA ALL; DROP DATABASE IF "
                    "EXISTS qa; CREATE DATABASE qa; RESET BINARY LOGS AND "
                    "GTIDS")
        replica.sql(f"CHANGE REPLICATION SOURCE TO {common}, "
                    "SOURCE_AUTO_POSITION=1; START REPLICA UNTIL "
                    f"SQL_AFTER_GTIDS='{CONSUMED['executed']}'")
        wanted = rw.parse_gtid_set(CONSUMED["executed"])
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline and rw.parse_gtid_set(replica.sql(
                "SELECT @@GLOBAL.gtid_executed")) != wanted:
            time.sleep(0.25)
        replica.sql("STOP REPLICA")
        result["before"] = rw.short(replica.sql(
            "SELECT @@GLOBAL.gtid_executed").strip())
        if rw.parse_gtid_set(result["before"]) != wanted:
            result["note"] = "the replica could not be brought to the old end"
            return result
        source_rows = rw.rows_digest(harness.SERVER)
        observed = tt.observe_replica(
            lambda: replica.sql(
                f"CHANGE REPLICATION SOURCE TO {common}, "
                f"SOURCE_AUTO_POSITION=0, SOURCE_LOG_FILE="
                f"'{CONSUMED['binlog']}', SOURCE_LOG_POS="
                f"{CONSUMED['position']}; START REPLICA"),
            source_rows, label, rw.parse_gtid_set(rw.source_executed()))
        result.update({key: observed[key] for key in
                       ("rows", "source_rows", "gtid_executed", "errors",
                        "died", "hangs") if observed.get(key)})
        result["source_gtid_executed"] = rw.short(rw.source_executed())
        result["equal"] = observed.get("rows") == source_rows and not any(
            observed.get(key) for key in ("errors", "died", "hangs"))
        result["silent"] = not result["equal"] and not any(
            observed.get(key) for key in ("errors", "died", "hangs"))
        if not result["equal"] and tt.last_error():
            result["pull_logged"] = tt.last_error()[:300]
    finally:
        session.close()
    return result


def position_consumers(settings: Path, storage: Path, exam: dict,
                       label: str) -> "dict | None":
    """Rewrite mode writes its own binlogs, so a repair that fetches
    transactions again may put them elsewhere: what that does to a replica
    that follows 'pull' by file and position."""
    if not (REWRITE and CONSUMED) or EPOCH != 0:
        return None
    where = f"{CONSUMED['binlog']}:{CONSUMED['position']}"
    before = read_before(storage, exam)
    if before == CONSUMED["held"]:
        return None
    if before is None:
        found = {"moved": f"{where}, the end of the storage before the "
                          "damage, is not the end of an event any more"}
    else:
        found = {"moved": f"before {where}, the end of the storage before "
                          "the damage, the binlogs now hold other "
                          "transactions than they did"}
        if before - CONSUMED["held"]:
            found["now_before_that_position"] = keys(
                before - CONSUMED["held"])
        if CONSUMED["held"] - before:
            found["now_after_that_position"] = keys(
                CONSUMED["held"] - before)
    if not ARGS.no_resume_check:
        # the same move after another repair of this state: observed once
        key = (label.split("/")[0], None if before is None else
               frozenset(before))
        if key not in RESUMED:
            RESUMED[key] = dict(
                resumed_replica(settings, label.replace("/", "-") + "-pos"),
                observed_after=label.split("/")[-1])
        found["replica"] = RESUMED[key]
    return found


def pull_start(settings: Path, seconds: float = 2.5) -> str:
    """What a start of 'pull' does within 'seconds'."""
    binsrv = harness.BINSRV
    with (binsrv.workdir / "pull-start.stderr").open("wb") as stderr:
        process = subprocess.Popen(
            [str(binsrv.binary), "pull", str(settings)],
            stdout=subprocess.DEVNULL, stderr=stderr,
            env=tamper.binsrv_environment())
        try:
            code = process.wait(timeout=seconds)
            return f"exit {code}: {tt.last_error()[:200]}"
        except subprocess.TimeoutExpired:
            errors = tt.last_error()
            return ("still running after "
                    f"{seconds} s" + (f", logging: {errors[:200]}"
                                      if errors else ""))
        finally:
            if process.poll() is None:
                process.send_signal(signal.SIGTERM)
                try:
                    process.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()


# ---------------------------------------------------------------------------
# the judge
# ---------------------------------------------------------------------------

REPLICA_DONE = set()
# the R1 line of the state being run
BASELINE = {}
# the replicas resumed by position so far: (state, what lies before the
# position) -> what was observed
RESUMED = {}
# where a replica of 'pull' stood that had read all of the storage before it
# was damaged: (binlog, position, the source's gtid_executed, the history
# numbers); set by base()
CONSUMED = {}
# set by a state whose source purged its first transactions before the
# harness saw them: a replica from scratch cannot end as the source
NO_SCRATCH = False


def judge(settings: Path, storage: Path, fetch_ok: bool, had: set,
          label: str, universe: "set | None" = None) -> tuple:
    """(outcome, evidence) of a storage after a repair and 'fetch';
    'universe' is what it should hold (default: the whole history)."""
    now = remember()
    everything = set(range(len(HISTORY)))
    universe = everything if universe is None else universe
    exam = examine(storage)
    lost = sorted(universe - exam["held"])
    destroyed = sorted((had & universe) - exam["held"])
    names = exam["names"]
    evidence = {
        "binlogs": (f"{names[0]}..{names[-1]} ({len(names)})" if names
                    else "none"),
        "transactions": f"{len(exam['held'] & universe)} of {len(universe)}"}
    if lost:
        evidence["missing"] = keys(lost)
        evidence["missing_that_the_source_still_has"] = len(set(lost) & now)
    if destroyed:
        evidence["held_before_the_repair_and_gone"] = keys(destroyed)
    if exam["junk"]:
        evidence["not_the_sources"] = exam["junk"][:3]
    if exam["twice"]:
        evidence["stored_twice"] = exam["twice"][:3]
    if exam["structure"]:
        evidence["file_problems"] = exam["structure"][:3]
    if not exam["ordered"]:
        evidence["order"] = ("the stored transactions are not in the "
                             "source's commit order: " + keys(
                                 [number for index, number in
                                  enumerate(exam["sequence"]) if index and
                                  number < exam["sequence"][index - 1]]) +
                             " come after later ones")
    if not fetch_ok:
        return ("DESTROYS-MORE" if destroyed else "STILL-STUCK"), evidence

    asked = queries(settings, storage, exam, lost, universe == everything)
    for key in ("problems", "claims"):
        if asked[key]:
            evidence["list_and_search_" + key] = asked[key][:3]
    wrong = (exam["junk"][:2] + [f"stored twice: {key}" for key in
                                 exam["twice"][:2]] +
             exam["structure"][:2] + asked["claims"][:2] +
             asked["problems"][:2])
    exact = not wrong and not lost and exam["ordered"]
    if exact and not REWRITE and EPOCH == 0 and universe == everything:
        differences = byte_differences(storage, names)
        if differences:
            evidence["bytes"] = differences[:3]
            wrong += differences[:2]
            exact = False
        else:
            evidence["bytes"] = "every stored binlog equals the source's"
    # a replica from scratch must end as the source is now when the storage
    # should hold all of the source's current life
    decisive = not NO_SCRATCH and universe >= {
        number for number in everything if HISTORY_EPOCH[number] == EPOCH}
    served = None
    state = label.split("/")[0]
    # a replica that is sent a cut-off transaction may die of it (ticket
    # 82), which takes half a minute each time: once per state is enough
    spared = bool(exam["junk"]) and not label.endswith("/R1") and \
        not ARGS.replica_all
    if names and not spared and (GTID or not exact or
                                 state not in REPLICA_DONE or
                                 ARGS.replica_all):
        served = serve(settings, label.replace("/", "-"), storage)
        if exact:
            REPLICA_DONE.add(state)
        evidence["served"] = {key: value for key, value in served.items()
                              if key != "dump_held"}
    tells = asked["tells"] + exam["chain"][:1]
    moved = None
    if universe == everything and not lost and not exam["junk"]:
        moved = position_consumers(settings, storage, exam, label)
        if moved is not None:
            evidence["position_based_consumers"] = moved
    stays = ""
    if moved is not None and moved.get("replica", {}).get("silent"):
        stays = (
            "a replica that followed 'pull' by file and position up to "
            f"{moved['replica']['resumes_at']} before the damage is pointed "
            "there again, reports no error and stays at "
            f"{moved['replica'].get('rows')} and "
            f"{moved['replica'].get('gtid_executed')} (the source: "
            f"{moved['replica'].get('source_rows')}, "
            f"{moved['replica'].get('source_gtid_executed')})")
    if exact:
        why = None
        if served is not None:
            if "pull" in served:
                why = ("'fetch' exits 0 and the storage holds everything, "
                       f"but 'pull' {served['pull']}")
            elif served["mysqlbinlog"]["exit"] != 0 or \
                    served["dump_held"] != exam["held"]:
                why = ("'fetch' exits 0 and the storage holds everything, "
                       "but mysqlbinlog through 'pull' gets "
                       f"{len(served['dump_held'])} of its "
                       f"{len(exam['held'])} transactions: "
                       f"{json.dumps(served['mysqlbinlog'])[:200]}")
            elif decisive and not served.get("replica_equal"):
                why = ("'fetch' exits 0 and the storage holds everything, "
                       "but a replica of 'pull' does not end as the source:"
                       f" {json.dumps(served.get('replica'))[:300]}")
        if why is None and stays:
            why = ("'fetch' exits 0 and by GTID the storage is exact, but "
                   "the transactions fetched again are stored at other "
                   "positions: " + stays)
        if why is None:
            return "RECOVERS-EXACT", evidence
        evidence["why"] = why
        return "SILENTLY-WRONG", evidence
    consumer = ""
    if served is not None and "replica" in served:
        errors = served["replica"].get("errors") or \
            served["replica"].get("died") or served["replica"].get("hangs")
        consumer = (f"a replica of 'pull' stops: {str(errors)[:200]}"
                    if errors else "a replica of 'pull' ends with " +
                    str(served["replica"].get("rows")) + " (the source: " +
                    str(served["replica"].get("source_rows")) +
                    ") and no error")
        evidence["consumer"] = consumer
    if wrong:
        evidence["why"] = "'fetch' exits 0, but " + "; ".join(wrong[:3]) + (
            "; " + stays if stays else "")
        return "SILENTLY-WRONG", evidence
    if lost and not tells:
        evidence["why"] = (
            f"'fetch' exits 0, {len(lost)} transaction(s) are missing "
            f"({keys(lost)}; the source still has "
            f"{len(set(lost) & now)} of them), and 'list', the searches "
            "and the log show nothing" + (f"; {consumer}" if consumer else ""))
        return "SILENTLY-WRONG", evidence
    if not lost and not exam["ordered"]:
        if served is not None and decisive and \
                not served.get("replica_equal"):
            evidence["why"] = ("'fetch' exits 0, the transactions are "
                               "stored out of order and " + consumer)
            return "SILENTLY-WRONG", evidence
        tells = tells + [
            "nothing is missing, but the fetched transactions are not in "
            "the source's commit order; only reading the binlogs "
            "(mysqlbinlog) shows it"]
    evidence["tells"] = tells[:4]
    if destroyed:
        return "DESTROYS-MORE", evidence
    return "RECOVERS-WITH-VISIBLE-GAP", evidence


# ---------------------------------------------------------------------------
# the repairs: each returns None (does not apply), or what was done
# ---------------------------------------------------------------------------

def storage_dir() -> Path:
    return harness.BINSRV.storage


def remove_binlog(storage: Path, name: bytes, commands: list) -> None:
    for suffix in (b"", b".json"):
        if b"\0" in name or b"/" in name or len(name) > 240:
            continue
        path = storage / os.fsdecode(name + suffix)
        if path.is_file():
            path.unlink()
            commands.append(f"rm {os.fsdecode(name + suffix)!r}")


def strays(storage: Path) -> list:
    """Objects that binlog.index does not name."""
    known = {"metadata.json", "binlog.index"}
    for name in indexed(storage):
        known.update((name, name + ".json"))
    return sorted(entry for entry in os.listdir(storage)
                  if entry not in known)


def repair_drop_last(_info: dict) -> "dict | None":
    storage = storage_dir()
    lines = raw_index(storage)
    if not lines:
        return None
    commands = ["sed -i '$d' binlog.index"]
    write_index(storage, lines[:-1])
    remove_binlog(storage, entry_name(lines[-1]), commands)
    return {"commands": commands,
            "last_index_entry": repr(lines[-1])[2:-1][:60]}


def last_sequence_number(data: bytes, boundary: int) -> "int | None":
    starts = [offset for offset, code, _ in harness.parse_events(data)
              if code in harness.TRANSACTION_START_EVENTS and
              offset < boundary]
    if not starts:
        return None
    return struct.unpack_from("<q", data, starts[-1] + 53)[0]


def mysqlbinlog_boundary(path: Path) -> dict:
    """What 'mysqlbinlog <file>' gives an operator who looks for the last
    good transaction boundary."""
    try:
        completed = subprocess.run(
            [str(rw.MYSQL_BASEDIR / "bin" / "mysqlbinlog"), "--no-defaults",
             "--base64-output=DECODE-ROWS", str(path)],
            capture_output=True, timeout=60, check=False)
    except subprocess.TimeoutExpired:
        return {"exit": "timeout"}
    text = completed.stdout.decode(errors="replace")
    last_commit = before_rotate = None
    rotated = False
    starts = 0
    for line in text.splitlines():
        found = re.search(r"end_log_pos (\d+)", line)
        if not found:
            continue
        if "Start: binlog" in line:
            starts += 1
        if "Rotate to" in line or starts > 1:
            rotated = True
        if "Xid = " in line:
            last_commit = int(found.group(1))
            if not rotated:
                before_rotate = last_commit
    error = "; ".join(
        line for line in completed.stderr.decode(errors="replace")
        .splitlines() if "[Warning]" not in line)[:200]
    result = {"exit": completed.returncode,
              "end_log_pos_of_the_last_xid": last_commit,
              "end_log_pos_of_the_last_xid_before_a_rotate_or_second_start":
              before_rotate}
    if error:
        result["error"] = error
    return result


def repair_trim_last(_info: dict) -> "dict | None":
    storage = storage_dir()
    commands = []
    lines = raw_index(storage)
    while lines and not (NAME.fullmatch(entry_name(lines[-1])) and
                         (storage / entry_name(lines[-1]).decode()
                          ).is_file()):
        # a line that names no stored binlog
        remove_binlog(storage, entry_name(lines[-1]), commands)
        lines.pop()
        commands.append("sed -i '$d' binlog.index")
        write_index(storage, lines)
    if not lines:
        return None
    name = entry_name(lines[-1]).decode()
    metadata = metadata_of(storage, name)
    if metadata is None:
        return {"commands": commands,
                "note": f"{name}.json is not there or not JSON: there is "
                        "no metadata to edit"}
    path = storage / name
    data = stored_plain(storage, name, cut=False)
    # in rewrite mode the next 'fetch' goes on in the last binlog, so the
    # ROTATE that closes it has to go as well
    boundary = good_boundary(data, name, keep_rotate=not REWRITE)
    done = {"binlog": name, "file_bytes": path.stat().st_size,
            "metadata_size": metadata.get("size"), "boundary": boundary}
    if ARGS.encrypted:
        done["boundary_found_with"] = (
            "the keyring: 'openssl enc -d' of the wrapped file key with "
            "the KEK, then of the data file with the file key; mysqlbinlog "
            "cannot read the stored file")
    else:
        done["mysqlbinlog"] = mysqlbinlog_boundary(path)
        done["mysqlbinlog_gives_the_boundary"] = boundary in (
            done["mysqlbinlog"].get(
                "end_log_pos_of_the_last_xid_before_a_rotate_or_second_start"),
            done["mysqlbinlog"].get("end_log_pos_of_the_last_xid"))
    if boundary < 4:
        done["note"] = f"{name} is not a binlog file: nothing to cut back to"
        done["commands"] = commands
        return done
    edits = {}
    if boundary != path.stat().st_size:
        os.truncate(path, boundary)
        commands.append(f"truncate -s {boundary} {name}")
    if metadata.get("size") != boundary:
        edits["size"] = boundary
    if GTID:
        added = rw.format_gtid_set(rw.as_set(
            rw.examine_stream({name: data[:boundary]})["gtids"]))
        if metadata.get("added_gtids") != added:
            edits["added_gtids"] = added
    number = last_sequence_number(data, boundary)
    if number is not None and "last_sequence_number" in metadata and \
            metadata["last_sequence_number"] != number:
        edits["last_sequence_number"] = number
    if edits:
        metadata.update(edits)
        (storage / (name + ".json")).write_text(
            json.dumps(metadata, separators=(",", ":")))
        commands.append(f"edit {name}.json: " + ", ".join(
            f"{key} = {json.dumps(value)[:60]}"
            for key, value in edits.items()))
    if not commands:
        return None
    done["commands"] = commands
    return done


def first_damaged(storage: Path) -> "str | None":
    """The first indexed binlog whose file or metadata shows damage."""
    for name in indexed(storage):
        metadata = metadata_of(storage, name)
        data = stored_plain(storage, name, cut=False)
        if metadata is None or metadata.get("size") != len(data) or \
                structure({name: data}):
            return name
        if GTID and len(data) > 4 and metadata.get("added_gtids") != \
                rw.format_gtid_set(rw.as_set(
                    rw.examine_stream({name: data})["gtids"])):
            return name
    return None


def damaged_binlog(info: dict) -> "str | None":
    storage = storage_dir()
    if info.get("damaged"):
        return info["damaged"]
    names = indexed(storage)
    return first_damaged(storage) or (names[-1] if names else None)


def repair_drop_from_damaged(info: dict) -> "dict | None":
    storage = storage_dir()
    lines = raw_index(storage)
    damaged = damaged_binlog(info)
    cut = next((index for index, line in enumerate(lines)
                if entry_name(line).decode(errors="replace") == damaged),
               None)
    if cut is None:
        return None
    if cut == len(lines) - 1 and not [
            entry for entry in strays(storage)
            if entry not in (damaged, damaged + ".json")]:
        # exactly what R2 does
        return None
    commands = []
    for line in lines[cut:]:
        remove_binlog(storage, entry_name(line), commands)
    if cut:
        write_index(storage, lines[:cut])
        commands.append(f"sed -i '{cut + 1},$d' binlog.index")
    else:
        (storage / "binlog.index").unlink()
        commands.append("rm binlog.index")
    for entry in strays(storage):
        (storage / entry).unlink()
        commands.append(f"rm {entry!r}")
    return {"commands": commands, "from": damaged}


def repair_purge(before: bool):
    def repair(info: dict) -> "dict | None":
        storage = storage_dir()
        names = indexed(storage)
        damaged = damaged_binlog(info)
        if damaged not in names:
            return None
        target = damaged
        if before:
            if names.index(damaged) == 0:
                return None
            target = names[names.index(damaged) - 1]
        doomed = names[:names.index(target) + 1]
        purged = {history_number(transaction) for transaction in
                  tt.read_transactions(
                      {name: stored_plain(storage, name) for name in doomed},
                      doomed)} - {None}
        answer = rw.query(config(), "purge_binlogs", target)
        return {"commands": [f"binlog_server purge_binlogs config.json "
                             f"{target}"],
                # what a purge that works removes is meant to go
                "_purged": purged if answer["status"] == "success" else
                set(),
                "purge_binlogs": {
                    "exit": answer["exit"], "status": answer["status"],
                    "message": answer["message"][:200],
                    "purged": len(answer["result"])}}
    return repair


def repair_rebuild(_info: dict) -> "dict | None":
    storage = storage_dir()
    commands = []
    try:
        current = (storage / "metadata.json").read_bytes()
    except OSError:
        current = None
    if current != METADATA_TEMPLATE:
        (storage / "metadata.json").write_bytes(METADATA_TEMPLATE)
        commands.append(f"echo -n '{METADATA_TEMPLATE.decode()}' > "
                        "metadata.json")
    names = sorted(entry for entry in os.listdir(storage)
                   if TEXT_NAME.fullmatch(entry) and
                   (storage / (entry + ".json")).is_file())
    lines = [b"./" + name.encode() for name in names]
    if raw_index(storage) != lines:
        write_index(storage, lines)
        commands.append("ls | grep -E '^binlog\\.[0-9]{6}$' | "
                        "sed 's|^|./|' > binlog.index")
    if not commands:
        return None
    return {"commands": commands, "index_rebuilt_as": names[-4:],
            "left_alone": strays(storage)[:4]}


def repair_gtid_truth(late: bool):
    def repair(_info: dict) -> "dict | None":
        storage = storage_dir()
        done = {"commands": []}
        if late:
            done["fetch_before_the_edit"] = run_fetch(config(), "R8-first")
        for name in indexed(storage):
            metadata = metadata_of(storage, name)
            if metadata is None:
                continue
            actual = rw.format_gtid_set(rw.as_set(rw.examine_stream(
                {name: stored_plain(storage, name)})["gtids"]))
            if metadata.get("added_gtids") != actual:
                done["commands"].append(
                    f"edit {name}.json: added_gtids = '{actual}' (was "
                    f"'{metadata.get('added_gtids')}')")
                metadata["added_gtids"] = actual
                (storage / (name + ".json")).write_text(
                    json.dumps(metadata, separators=(",", ":")))
        return done if done["commands"] else None
    return repair


def repair_empty_tail(_info: dict) -> "dict | None":
    """R9: the last binlog as the Binlog Server creates one - the binlog
    magic, metadata of size 4 without added GTIDs - so that 'fetch' fills
    the binlog of that name again from its start."""
    storage = storage_dir()
    commands = []
    lines = raw_index(storage)
    while lines and not NAME.fullmatch(entry_name(lines[-1])):
        remove_binlog(storage, entry_name(lines[-1]), commands)
        lines.pop()
        commands.append("sed -i '$d' binlog.index")
    if not lines:
        return None
    write_index(storage, lines)
    names = [entry_name(line).decode() for line in lines
             if NAME.fullmatch(entry_name(line))]
    name = names[-1]
    old = metadata_of(storage, name)
    before = metadata_of(storage, names[-2]) if len(names) > 1 else None
    metadata = dict(before or old or {})
    path = storage / name
    encrypted = isinstance((old or {}).get("encryption"), dict)
    if not metadata or (ARGS.encrypted and not
                        (encrypted and path.is_file())):
        return {"commands": commands,
                "note": "no metadata to write the empty binlog's from" if
                        not metadata else
                        "an encrypted storage: without the binlog's own "
                        "'encryption' record and data file an empty binlog "
                        "cannot be written by hand"}
    metadata.update(size=4, last_sequence_number=0)
    metadata.pop("encryption", None)
    if encrypted:
        # the first four bytes of the file are the encrypted magic
        metadata["encryption"] = old["encryption"]
    if GTID:
        metadata["previous_gtids"] = rw.format_gtid_set(rw.union(
            rw.parse_gtid_set(before["previous_gtids"]),
            rw.parse_gtid_set(before["added_gtids"]))) if before else \
            (old or {}).get("previous_gtids", "")
        metadata["added_gtids"] = ""
    if encrypted:
        os.truncate(path, 4)
        commands.append(f"truncate -s 4 {name}")
    else:
        path.write_bytes(harness.BINLOG_MAGIC)
        commands.append(f"printf '\\376bin' > {name}")
    (storage / (name + ".json")).write_text(
        json.dumps(metadata, separators=(",", ":")))
    commands.append(f"write {name}.json: " +
                    json.dumps({key: value for key, value in metadata.items()
                                if key != "encryption"}))
    return {"commands": commands, "binlog": name}


def two_storage_tell(old: dict, new: dict, storage: Path,
                     second: Path) -> str:
    """What shows an operator where two storages do not join."""
    if not new["names"]:
        return "the new storage is empty"
    if not old["names"]:
        return "the old storage lists no binlog"
    if GTID:
        first = metadata_of(second, new["names"][0]) or {}
        last = metadata_of(storage, old["names"][-1]) or {}
        return (f"the old storage ends with previous_gtids "
                f"'{rw.short(str(last.get('previous_gtids')), 60)}' + "
                f"added_gtids '{rw.short(str(last.get('added_gtids')), 60)}'"
                f" ({old['names'][-1]}), the new one starts with "
                f"previous_gtids "
                f"'{rw.short(str(first.get('previous_gtids')), 60)}'")
    return (f"the old storage ends with {old['names'][-1]} "
            f"({(storage / old['names'][-1]).stat().st_size} bytes), the "
            f"new one starts with {new['names'][0]}")


def repair_fresh_alongside(_info: dict, had: set, label: str) -> dict:
    """R7 judges itself: two storages."""
    storage = storage_dir()
    second = harness.BINSRV.workdir / "storage2"
    shutil.rmtree(second, ignore_errors=True)
    second.mkdir(parents=True)
    fresh_settings = config("fresh", storage=second)
    fetched = run_fetch(fresh_settings, "R7")
    now = remember()
    done = {"commands": ["mkdir storage2", "binlog_server fetch config2.json"
                         "  # storage.uri = file://.../storage2"],
            "fetch": fetched}
    outcome, evidence = judge(fresh_settings, second, fetched["ok"], set(),
                              label, universe=now)
    done["new_storage"] = evidence
    old = examine(storage)
    listing = rw.query(config(), "list")
    done["old_storage"] = {
        "list": {"exit": listing["exit"], "status": listing["status"],
                 "binlogs": len(listing["result"]),
                 "message": listing["message"][:160]},
        "valid_transactions": len(old["held"]),
        "not_the_sources": old["junk"][:2], "file_problems":
        old["structure"][:2]}
    readonly = config("readonly", source_port=DEAD_PORT)
    served = serve(readonly, label.replace("/", "-") + "-old", storage,
                   with_replica=ARGS.all)
    if "pull" in served:
        done["old_storage"]["pull_with_an_unreachable_source"] = \
            served["pull"]
    else:
        done["old_storage"]["pull_with_an_unreachable_source"] = (
            f"serves: mysqlbinlog gets {len(served['dump_held'])} of the "
            f"{len(old['held'])} valid transactions "
            f"({json.dumps(served['mysqlbinlog'])})")
        if "replica" in served:
            done["old_storage"]["replica"] = served["replica"]
    done["old_storage"]["pull_with_the_source"] = pull_start(config())
    if outcome != "RECOVERS-EXACT":
        done["outcome"] = outcome
        if "why" in evidence:
            done["why"] = "the fresh storage: " + evidence["why"]
        return done
    new = examine(second)
    everything = set(range(len(HISTORY)))
    lost = sorted(everything - (old["held"] | new["held"]))
    done["both_storages"] = {
        "transactions": f"{len(everything) - len(lost)} of {len(everything)}",
        "in_both": len(old["held"] & new["held"]),
        "only_in_the_old_one": len(old["held"] - new["held"])}
    if not lost:
        done["outcome"] = "RECOVERS-EXACT"
        return done
    done["both_storages"]["missing"] = keys(lost)
    done["tells"] = [two_storage_tell(old, new, storage, second)]
    done["outcome"] = "RECOVERS-WITH-VISIBLE-GAP"
    return done


def combined(first: str, second: str):
    """Two repairs in a row; applies only when both have something to do."""
    def repair(info: dict) -> "dict | None":
        one = REPAIR_FUNCTIONS[first](info)
        if one is None:
            return None
        two = REPAIR_FUNCTIONS[second](info)
        if two is None:
            return None
        return dict(one, **dict(two, commands=one["commands"] +
                                two["commands"]))
    return repair


REPAIR_FUNCTIONS = {
    "R1": lambda info: {"commands": []},
    "R2": repair_drop_last,
    "R3": repair_trim_last,
    "R2+R3": combined("R2", "R3"),
    "R6+R3": combined("R6", "R3"),
    "R4+R3": combined("R4", "R3"),
    "R4": repair_drop_from_damaged,
    "R5-before": repair_purge(True),
    "R5-after": repair_purge(False),
    "R6": repair_rebuild,
    "R8": repair_gtid_truth(False),
    "R8-late": repair_gtid_truth(True),
    "R9": repair_empty_tail,
}


# ---------------------------------------------------------------------------
# the states: each leaves the damaged storage in place and returns what is
# known about it
# ---------------------------------------------------------------------------

def base(binlogs: int = 3, marker: str = "BASE", tail: str = "TAIL") -> Path:
    """A healthy storage of several binlogs that 'fetch' has just filled."""
    for number in range(binlogs):
        rw.insert_many(f"{marker}{number}", 3, 8)
        harness.SERVER.sql("FLUSH BINARY LOGS")
    rw.insert_many(tail, 3, 8)
    settings = config()
    must_fetch(settings, "base")
    storage = storage_dir()
    last = indexed(storage)[-1]
    CONSUMED.update(binlog=last, position=metadata_of(storage, last)["size"],
                    executed=rw.source_executed() if GTID else "",
                    held=remember())
    return settings


def build_healthy() -> dict:
    fresh()
    base()
    rw.insert_many("AFTER", 2, 8)
    return {"how": "nothing: a healthy storage and two new transactions on "
                   "the source"}


def build_short_tail() -> dict:
    fresh()
    base()
    rw.insert_many("AFTER", 2, 8)
    storage = storage_dir()
    last = indexed(storage)[-1]
    size = (storage / last).stat().st_size
    os.truncate(storage / last, size - 150)
    return {"damaged": last,
            "how": f"{last} truncated from {size} to {size - 150} bytes "
                   "(inside its last transaction); its metadata still says "
                   f"{size}"}


def build_tail_deleted(index_too: bool):
    def build() -> dict:
        fresh()
        base()
        rw.insert_many("AFTER", 2, 8)
        storage = storage_dir()
        last = indexed(storage)[-1]
        lines = raw_index(storage)
        (storage / last).unlink()
        (storage / (last + ".json")).unlink()
        if index_too:
            write_index(storage, lines[:-1])
        return {"damaged": last,
                "how": f"rm {last} {last}.json" + (
                    " and its line of binlog.index" if index_too else "")}
    return build


def build_interrupted_rotation() -> dict:
    """What a crash between the creation of a binlog and its line in
    binlog.index leaves (README, 'Storage consistency checks'): the binlog
    before it is closed by its ROTATE, the new one holds the magic only."""
    fresh()
    base()
    rw.insert_many("AFTER", 2, 8)
    storage = storage_dir()
    last = indexed(storage)[-1]
    lines = raw_index(storage)
    metadata = metadata_of(storage, last)
    write_index(storage, lines[:-1])
    os.truncate(storage / last, 4)
    metadata["size"] = 4
    if GTID:
        metadata["added_gtids"] = ""
    (storage / (last + ".json")).write_text(
        json.dumps(metadata, separators=(",", ":")))
    # a replica of 'pull' had read the ROTATE that announces this binlog
    earlier = indexed(storage)
    CONSUMED.update(
        binlog=last, position=4,
        executed=metadata.get("previous_gtids", ""),
        held={history_number(transaction) for transaction in
              tt.read_transactions(
                  {name: stored_plain(storage, name) for name in earlier},
                  earlier)} - {None})
    return {"how": f"{last} cut to the binlog magic, its metadata to size "
                   "4, and its line removed from binlog.index: as if the "
                   "Binlog Server had been killed while it rotated to it"}


def build_source_reset(longer: bool):
    def build() -> dict:
        fresh()
        base()
        new_epoch()
        if longer:
            # the same shape as before, so that the stored position is a
            # transaction boundary of the new source's binlog as well
            for number in range(3):
                rw.insert_many(f"NEXT{number}", 3, 8)
                harness.SERVER.sql("FLUSH BINARY LOGS")
            rw.insert_many("NEWT", 3, 8)
            harness.SERVER.sql("FLUSH BINARY LOGS")
            rw.insert_many("NEWER", 4, 8)
        else:
            rw.insert_many("NEW", 2, 8)
        return {"how": "the source was initialised again (empty binary "
                       "logs and GTIDs, a new qa.t) and has " +
                       ("more" if longer else "fewer") +
                       " binlogs and transactions than the storage",
                "source_binlogs": harness.SERVER.binlog_names()}
    return build


def build_source_gap() -> dict:
    fresh()
    base()
    rw.insert_many("GAPA", 3, 8)
    harness.SERVER.sql("FLUSH BINARY LOGS")
    rw.insert_many("GAPB", 3, 8)
    harness.SERVER.sql("FLUSH BINARY LOGS")
    rw.insert_many("LATE", 2, 8)
    remember()
    names = harness.SERVER.binlog_names()
    harness.SERVER.sql(f"PURGE BINARY LOGS TO '{names[-1]}'")
    return {"how": f"the source purged its binlogs before {names[-1]}; six "
                   "transactions in them were never fetched",
            "source_binlogs": harness.SERVER.binlog_names()}


def tail_metadata() -> tuple:
    storage = storage_dir()
    last = indexed(storage)[-1]
    return storage, last, metadata_of(storage, last)


def build_gtid_claims_extended() -> dict:
    fresh()
    base()
    rw.insert_many("AFTER", 3, 8)
    storage, last, metadata = tail_metadata()
    was = metadata["added_gtids"]
    held = rw.union(rw.parse_gtid_set(metadata["previous_gtids"]),
                    rw.parse_gtid_set(was))
    sid = harness.SERVER.sql("SELECT @@server_uuid").strip()
    top = max(held[sid])
    added = rw.parse_gtid_set(was)
    added.setdefault(sid, set()).update((top + 1, top + 2))
    metadata["added_gtids"] = rw.format_gtid_set(added)
    (storage / (last + ".json")).write_text(
        json.dumps(metadata, separators=(",", ":")))
    return {"damaged": last,
            "how": f"{last}.json: added_gtids '{was}' -> "
                   f"'{metadata['added_gtids']}' (the source's next two "
                   "GTIDs, which are not stored)"}


def build_gtid_size_lowered() -> dict:
    fresh()
    base()
    rw.insert_many("AFTER", 3, 8)
    storage, last, metadata = tail_metadata()
    data = stored_plain(storage, last)
    start = [offset for offset, code, _ in harness.parse_events(data)
             if code in harness.TRANSACTION_START_EVENTS][-1]
    was = metadata["size"]
    metadata["size"] = start
    (storage / (last + ".json")).write_text(
        json.dumps(metadata, separators=(",", ":")))
    return {"damaged": last,
            "how": f"{last}.json: size {was} -> {start} (the start of the "
                   "last stored transaction), the GTID sets left alone"}


def build_hostile(name: str):
    def build() -> dict:
        begin_state()
        arguments = argparse.Namespace(
            mysql_basedir=rw.MYSQL_BASEDIR, port=harness.SERVER.port,
            storage="file", case_insensitive_dir=None,
            midstream_in_position_mode=True)
        result = hostile.run_case_on_storage(name, arguments,
                                             next_proxy_port())
        if not result.get("events_rewritten"):
            raise harness.Failure("the proxy changed nothing: " +
                                  str(result.get("why")))
        info = {"how": f"hostile_source_test.py '{name}' (name "
                       f"{result.get('hostile_name')}): "
                       f"{str(result.get('why'))[:240]}"}
        if name == "nul_existing_binlog":
            info["damaged"] = "binlog.000001"
        if name == "lower_sequence":
            # qa.t was created in a binlog the source has purged
            info["no_replica_from_scratch"] = True
        return info
    return build


def build_cutoff(scenario: str):
    def build() -> dict:
        begin_state()
        rw.PROXY_PORT = next_proxy_port()
        spec = dict(tt.SCENARIOS[scenario])
        spec.pop("replica", None)
        result, _ = tt.scenario(scenario, spec)
        return {"how": f"truncated_transaction_test.py '{scenario}': "
                       f"{result.get('verdict')}; "
                       f"{str(result.get('why'))[:240]}",
                "target_gtid": result.get("target_gtid")}
    return build


def attacked_pull(proxy, attack, observe: float, kill: bool = False) -> dict:
    """A 'pull' through 'proxy' that has stored what the source has;
    'attack()' then starts the attack and the source's next transactions."""
    binsrv = harness.BINSRV
    hostile.cache_password_hash(rw.MYSQL_BASEDIR, harness.SERVER.port)
    binsrv.checkpoint_size = "1"
    settings = config("attacked-pull",
                      source_port=proxy.listener.getsockname()[1],
                      proxied=True)
    binsrv.checkpoint_size = None
    result = {}
    with (binsrv.workdir / "attacked-pull.stderr").open("wb") as stderr:
        process = subprocess.Popen(
            [str(binsrv.binary), "pull", str(settings)],
            stdout=subprocess.DEVNULL, stderr=stderr,
            env=tamper.binsrv_environment())
        try:
            wanted = len(tt.source_transactions())
            deadline = time.monotonic() + 30
            while len(tt.read_transactions(
                    {name: stored_plain(binsrv.storage, name)
                     for name in indexed(binsrv.storage)},
                    indexed(binsrv.storage))) < wanted:
                if time.monotonic() > deadline or \
                        process.poll() is not None:
                    raise harness.Failure(
                        "before the attack 'pull' did not store the "
                        f"source's transactions (exit {process.poll()}, "
                        f"{tt.last_error()})")
                time.sleep(0.2)
            attack()
            deadline = time.monotonic() + observe
            while time.monotonic() < deadline and process.poll() is None:
                time.sleep(0.2)
            result["pull_exit_during_the_attack"] = process.poll()
            result["pull_error"] = tt.last_error()[:240]
            last = indexed(binsrv.storage)[-1]
            result["last_binlog_while_pull_runs"] = {
                "name": last,
                "file_bytes": (binsrv.storage / last).stat().st_size,
                "metadata_size": (metadata_of(binsrv.storage, last) or
                                  {}).get("size")}
        finally:
            if process.poll() is None:
                process.send_signal(signal.SIGKILL if kill else
                                    signal.SIGTERM)
            # 'pull' notices the signal only when its read returns
            proxy.close()
            try:
                process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
    return result


def build_pull_name(attack: str):
    def build() -> dict:
        fresh()
        rw.insert_many("BEFORE", 4, 8)
        proxy = hostile_pull.NameProxy(
            next_proxy_port(), harness.SERVER.port,
            "rename" if attack == "long_name" else attack)

        def start():
            upcoming = int(harness.SERVER.binlog_names()[-1][-6:]) + 1
            proxy.old_name = proxy.new_name = b"binlog.%06d" % upcoming
            if attack == "long_name":
                proxy.new_name = hostile.CASES[attack](upcoming)
            proxy.arm(drop=False)
            harness.SERVER.sql("FLUSH BINARY LOGS")
            rw.insert_many("DURING", 3, 8)

        result = attacked_pull(proxy, start, 20 if attack == "gtid_bloat"
                               else 8)
        if not proxy.changed():
            raise harness.Failure("the proxy changed nothing")
        rw.insert_many("AFTER", 2, 8)
        return {"how": f"hostile_source_pull_test.py's proxy, '{attack}', "
                       f"against a running 'pull': {json.dumps(result)}"}
    return build


def build_fragments_pull() -> dict:
    fresh()
    rw.insert_many("BEFORE", 4, 8)
    proxy = hostile_pull.LiveProxy(next_proxy_port(), harness.SERVER.port,
                                   "eof_mid_transaction")

    def start():
        proxy.arm(drop=False)
        rw.insert_many("DURING", 3, 8)

    # killed, so that nothing is tidied up on the way out
    result = attacked_pull(proxy, start, 8, kill=True)
    if not proxy.changed():
        raise harness.Failure("the proxy did nothing")
    result["dumps_ended_in_a_transaction"] = proxy.changed()
    rw.insert_many("AFTER", 2, 8)
    return {"how": "hostile_source_pull_test.py's proxy, "
                   "'eof_mid_transaction', against a running 'pull': "
                   f"{json.dumps(result)}"}


POSITION = ("position",)
GTID_ANY = ("gtid", "gtid-norewrite")
ALL_MODES = POSITION + GTID_ANY
NAMED = POSITION + ("gtid-norewrite",)
# 'known': wrong rather than stuck by a known ticket (R1 is the baseline)
STATES = {
    "healthy": {"build": build_healthy, "modes": ALL_MODES, "healthy": True},
    "nul_existing_binlog": {"build": build_hostile("nul_existing_binlog"),
                            "modes": NAMED, "ticket": "59"},
    "nul_metadata": {"build": build_hostile("nul_metadata"), "modes": NAMED,
                     "ticket": "59"},
    "nul_index": {"build": build_hostile("nul_index"), "modes": NAMED,
                  "ticket": "59"},
    "newline": {"build": build_hostile("newline"), "modes": NAMED,
                "ticket": "59"},
    "long_name_pull": {"build": build_pull_name("long_name"),
                       "modes": NAMED, "ticket": "59"},
    "gtid_bloat_pull": {"build": build_pull_name("gtid_bloat"),
                        "modes": ("gtid-norewrite",), "ticket": "59"},
    "reannounce": {"build": build_hostile("reannounce"), "modes": POSITION,
                   "ticket": "69"},
    "reannounce_midstream": {"build": build_hostile("reannounce_midstream"),
                             "modes": ("gtid-norewrite",), "ticket": "69"},
    "lower_sequence": {"build": build_hostile("lower_sequence"),
                       "modes": NAMED, "ticket": "70"},
    "cutoff_stub": {"build": build_cutoff("lie_after_begin"),
                    "modes": ALL_MODES, "ticket": "82",
                    "known": ("gtid",)},
    "cutoff_stub_drop": {"build": build_cutoff("lie_after_begin_drop"),
                         "modes": ALL_MODES, "ticket": "82",
                         "known": ("gtid",), "optional": True},
    "fragments_pull": {"build": build_fragments_pull, "modes": ("gtid",),
                       "ticket": "83", "known": ("gtid",),
                       "optional": True},
    "short_tail": {"build": build_short_tail, "modes": ALL_MODES,
                   "ticket": "19"},
    "source_reset": {"build": build_source_reset(False), "modes": ALL_MODES,
                     "ticket": "25"},
    "source_reset_longer": {"build": build_source_reset(True),
                            "modes": ALL_MODES, "ticket": "25",
                            "known": ALL_MODES},
    "source_gap": {"build": build_source_gap, "modes": ALL_MODES},
    "interrupted_rotation": {"build": build_interrupted_rotation,
                             "modes": ALL_MODES},
    "tail_deleted": {"build": build_tail_deleted(False), "modes": ALL_MODES},
    "tail_deleted_index_too": {"build": build_tail_deleted(True),
                               "modes": ALL_MODES},
    "gtid_claims_extended": {"build": build_gtid_claims_extended,
                             "modes": GTID_ANY, "ticket": "81",
                             "known": GTID_ANY, "claims": True},
    "gtid_size_lowered": {"build": build_gtid_size_lowered,
                          "modes": GTID_ANY, "ticket": "63",
                          "known": GTID_ANY, "claims": True},
}


# ---------------------------------------------------------------------------
# a state and its repairs
# ---------------------------------------------------------------------------

def wanted_repairs(name: str, spec: dict) -> list:
    repairs = [repair for repair in REPAIRS
               if not repair.startswith("R8") or
               (GTID and (spec.get("claims") or ARGS.all))]
    if ARGS.cases:
        repairs = [repair for repair in repairs if repair == "R1" or any(
            wanted in f"{name}/{repair}/" for wanted in ARGS.cases)]
    return repairs


def attempt(name: str, repair: str, info: dict, had: set,
            copy: Path) -> dict:
    line = {"case": f"{LABEL}/{name}/{repair}"}
    started = time.monotonic()
    restore(copy, storage_dir())
    try:
        if repair == "R7":
            done = repair_fresh_alongside(info, had, f"{name}/{repair}")
            line.update(done)
        else:
            done = REPAIR_FUNCTIONS[repair](info)
            if done is None:
                line["outcome"] = "NOT-APPLICABLE"
            else:
                purged = done.pop("_purged", set())
                line.update(done)
                if repair.startswith("R5") and \
                        done["purge_binlogs"]["status"] != "success" and \
                        "outcome" in BASELINE:
                    # nothing was changed: what R1 found holds
                    line["outcome"] = BASELINE["outcome"]
                    line["as"] = "R1: purge_binlogs changed nothing"
                    line["ok"] = True
                    return line
                fetched = run_fetch(config(), repair)
                line["fetch"] = fetched
                remember()
                outcome, evidence = judge(
                    config(), storage_dir(), fetched["ok"], had - purged,
                    f"{name}/{repair}",
                    set(range(len(HISTORY))) - purged if purged else None)
                line["outcome"] = outcome
                line.update(evidence)
    except (harness.Failure, RuntimeError, subprocess.SubprocessError,
            OSError, ValueError, KeyError, IndexError,
            struct.error) as failure:
        line["outcome"] = "ERROR"
        line["why"] = "the harness: " + harness.describe_failure(failure,
                                                                 400)
        if harness.SERVER.process is None:
            harness.SERVER.start()
    if repair == "R1":
        BASELINE.clear()
        BASELINE.update(line)
    line["ok"] = line["outcome"] not in BAD + ("ERROR",)
    if not line["ok"] and "why" not in line:
        line["why"] = (
            "transactions the damaged storage held are gone and were not "
            f"fetched again: {line.get('held_before_the_repair_and_gone')}"
            if line["outcome"] == "DESTROYS-MORE" else line["outcome"])
    line["seconds"] = round(time.monotonic() - started, 1)
    return line


def run_state(name: str, spec: dict) -> list:
    started = time.monotonic()
    summary = {"case": f"{LABEL}/{name}"}
    if spec.get("ticket"):
        summary["ticket"] = spec["ticket"]
    try:
        info = spec["build"]()
        remember()
    except (harness.Failure, RuntimeError, subprocess.SubprocessError,
            OSError, ValueError, KeyError, IndexError) as failure:
        summary.update(ok=False, why="the state was not produced: " +
                       harness.describe_failure(failure, 500))
        if harness.SERVER.process is None:
            harness.SERVER.start()
        return [summary]
    summary["state"] = info
    global NO_SCRATCH  # pylint: disable=global-statement
    NO_SCRATCH = bool(info.get("no_replica_from_scratch"))
    copy = harness.BINSRV.workdir / "damaged"
    shutil.rmtree(copy, ignore_errors=True)
    shutil.copytree(storage_dir(), copy)
    had = examine(copy)["held"]
    summary["transactions_in_the_damaged_storage"] = \
        f"{len(had)} of {len(HISTORY)}"
    lines = []
    outcomes = {}
    BASELINE.clear()
    unchanged = set()
    known = LABEL in spec.get("known", ())
    for repair in wanted_repairs(name, spec):
        line = attempt(name, repair, info, had, copy)
        if repair == "R1":
            fetched = line.get("fetch") or {"ok": False, "runs": [
                {"exit": None, "message": line.get("why", "")}]}
            first = fetched["runs"][0]
            stuck = not fetched["ok"]
            summary["stuck"] = stuck
            if stuck:
                message = fetched["runs"][-1]["message"]
                restore(copy, storage_dir())
                summary["message"] = {
                    "fetch": message,
                    "pull": pull_start(config()),
                    "names_the_object": bool(
                        OBJECT_IN_MESSAGE.search(message)),
                    "says_what_to_do": bool(
                        ADVICE_IN_MESSAGE.search(message))}
            elif first["exit"] != 0:
                summary["first_fetch"] = first
            if known and line["outcome"] in BAD:
                # the defect itself, not a repair
                line["ok"] = True
                line["baseline_of_ticket"] = spec.get("ticket")
        outcomes[repair] = line["outcome"]
        if "as" in line:
            unchanged.add(repair)
        lines.append(line)
        print(json.dumps(line), flush=True)
    summary["outcomes"] = outcomes
    recovered = [repair for repair, outcome in outcomes.items()
                 if outcome in GOOD and repair != "R1"]
    silent = [repair for repair, outcome in outcomes.items()
              if outcome == "SILENTLY-WRONG" and
              not ((repair == "R1" or repair in unchanged) and known)]
    summary["recovers_with"] = recovered
    problems = []
    if spec.get("healthy"):
        if outcomes.get("R1") != "RECOVERS-EXACT":
            problems.append("the control: a healthy storage is judged "
                            f"{outcomes.get('R1')}")
    elif outcomes.get("R1") == "RECOVERS-EXACT":
        summary["reproduced"] = False
        summary["note"] = ("not stuck and not wrong with this binary: "
                           "'fetch' just continues and the storage is "
                           "exact")
    else:
        if silent:
            problems.append("silently wrong after " + ", ".join(silent))
        if not recovered and len(outcomes) > 1:
            problems.append("no repair that was tried recovers the storage")
    summary["ok"] = not problems
    if problems:
        summary["why"] = "; ".join(problems)
    summary["seconds"] = round(time.monotonic() - started, 1)
    return lines + [summary]


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--mysql-basedir", type=Path, required=True)
    parser.add_argument("--binsrv", type=Path, required=True)
    parser.add_argument("--workdir", type=Path, required=True)
    parser.add_argument("--port", type=int, default=24300,
                        help="the source's port; the replica, 'pull', the "
                             "proxies and an unused port are the 20 above "
                             "it")
    parser.add_argument("--mode", choices=["position", "gtid"],
                        default="position")
    parser.add_argument("--rewrite", action="store_true",
                        help="with 'replication.rewrite' (implied by --mode "
                             "gtid)")
    parser.add_argument("--no-rewrite", action="store_true",
                        help="GTID mode without 'replication.rewrite', for "
                             "a binary built before PBS-80 / PBS-98")
    parser.add_argument("--cases", nargs="*",
                        help="substrings of '<state>/<repair>/' ('/R2/' "
                             "is R2 alone); R1 always runs for a selected "
                             "state")
    parser.add_argument("--encrypted", action="store_true",
                        help="on an encrypted storage")
    parser.add_argument("--all", action="store_true",
                        help="also the optional states, R8 for every GTID "
                             "state and a replica of the old storage in R7")
    parser.add_argument("--replica-all", action="store_true",
                        help="position mode: a replica of 'pull' after "
                             "every repair that makes 'fetch' run")
    parser.add_argument("--no-resume-check", action="store_true",
                        help="rewrite mode: do not run the replica that "
                             "resumes by file and position after a repair "
                             "that moved transactions")
    parser.add_argument("--keep", action="store_true",
                        help="keep the MySQL data directories at the end")
    args = parser.parse_args()
    if (args.rewrite or args.no_rewrite) and args.mode != "gtid":
        parser.error("--rewrite / --no-rewrite need --mode gtid")
    if args.rewrite and args.no_rewrite:
        parser.error("--rewrite and --no-rewrite exclude each other")
    args.workdir = args.workdir.expanduser()
    args.workdir.mkdir(parents=True, exist_ok=True)
    # pylint: disable-next=global-statement
    global ARGS, GTID, REWRITE, LABEL, DEAD_PORT, METADATA_TEMPLATE
    ARGS = args
    GTID = args.mode == "gtid"
    REWRITE = GTID and not args.no_rewrite
    LABEL = "position" if not GTID else "gtid" if REWRITE else \
        "gtid-norewrite"
    DEAD_PORT = args.port + 19
    PROXY_PORTS.extend(range(args.port + 3, args.port + 10))
    tt.GTID_MODE = GTID
    tt.REWRITE = False
    tt.REPLICA_ALL = False
    tt.WORKDIR = args.workdir
    rw.MYSQL_BASEDIR = args.mysql_basedir.expanduser()
    rw.PULL_PORT = args.port + 2
    rw.PROXY_PORT = args.port + 3
    harness.SERVER = harness.MySQLServer(rw.MYSQL_BASEDIR,
                                         args.workdir / "mysql", args.port,
                                         GTID, server_id=1)
    rw.REPLICA = harness.MySQLServer(rw.MYSQL_BASEDIR,
                                     args.workdir / "replica", args.port + 1,
                                     GTID, server_id=2)
    binsrv = harness.BINSRV = harness.BinlogServer(
        args.binsrv.expanduser(), args.workdir / "binsrv", args.port, GTID,
        None)
    # small enough that the storage of every state has several binlogs
    binsrv.rewrite_file_size = "2K"
    if (GTID and not REWRITE) or args.encrypted:
        write_config = binsrv.write_config

        def adjusted():
            write_config()
            settings = json.loads(binsrv.config_path.read_text())
            if not REWRITE:
                settings["replication"].pop("rewrite", None)
            if args.encrypted:
                keyring = binsrv.workdir / "keyring.json"
                keyring.write_text(json.dumps(tamper.KEYRING))
                settings["keyring"] = {"uri": f"file://{keyring}"}
                settings["storage"]["encryption"] = dict(tamper.ENCRYPTION)
            binsrv.config_path.write_text(json.dumps(settings, indent=2))
        binsrv.write_config = adjusted

    selected = [
        name for name, spec in STATES.items()
        if LABEL in spec["modes"] and
        not (args.encrypted and spec["build"].__qualname__.startswith(
            ("build_hostile", "build_cutoff"))) and
        (args.all or not spec.get("optional") or args.cases) and
        (not args.cases or any(
            wanted in f"{name}/{repair}/" for wanted in args.cases
            for repair in REPAIRS))]
    results = []
    started = time.monotonic()
    shutil.rmtree(rw.REPLICA.datadir, ignore_errors=True)
    harness.SERVER.start()
    try:
        rw.REPLICA.start()
        # what metadata.json of a healthy storage of this mode looks like
        fresh()
        rw.insert_many("PROBE", 1, 1)
        must_fetch(config(), "probe")
        METADATA_TEMPLATE = (storage_dir() / "metadata.json").read_bytes()
        for name in selected:
            lines = run_state(name, STATES[name])
            print(json.dumps(lines[-1]), flush=True)
            results += lines
    finally:
        rw.REPLICA.stop()
        harness.SERVER.stop()
        if not args.keep:
            shutil.rmtree(rw.REPLICA.datadir, ignore_errors=True)
            shutil.rmtree(harness.SERVER.datadir, ignore_errors=True)
    failures = [result for result in results if not result.get("ok")]
    print(json.dumps({"cases": len(results), "failures": len(failures),
                      "failed": [result["case"] for result in failures],
                      "mode": LABEL + ("+encrypted" if args.encrypted
                                       else ""),
                      "seconds": round(time.monotonic() - started),
                      "passed": not failures}), flush=True)
    (args.workdir / "result.json").write_text(json.dumps(results, indent=2))
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
