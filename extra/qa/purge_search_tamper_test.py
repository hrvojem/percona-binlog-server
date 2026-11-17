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

"""What edited storage metadata does to 'purge_binlogs' and the searches.

Operators pick the binlogs to replay for a point-in-time restore from the
answers of 'search_by_timestamp' / 'search_by_gtid_set', and delete old
binlogs with 'purge_binlogs'. All three decide from 'binlog.index' and the
'binlog.NNNNNN.json' metadata files, which nothing authenticates (ticket
53): someone who can write to the storage but holds no key can edit them.
'metadata_tamper_test.py' checks what such edits do to 'list', 'fetch' and
'pull'; this harness checks purge and search.

A source gets six binlogs with three transactions each, every one in a
second of its own (and, on a server that has them, one transaction with a
tagged GTID). Each transaction inserts a row and sets a counter row to its
number, so that the order of a replay shows in the data. The truth - the
time range of each binlog, the time and the GTID of each transaction - is
read from the source's own binlog files. 'fetch' copies the binlogs into a
plain and into an encrypted file storage. Every case works on a fresh copy
of one of them, with canary files next to it (among them a 'binlog.000001',
a 'keyring.json' and a 'victim' directory, the only things the path and
symlink edits point at).

- 'clean_*' (no edit): the stored metadata against the source; the searches
  at every boundary (the first and the last second of each binlog, the
  seconds around them, each transaction, before the first and after the
  last binlog; every prefix GTID set, every single GTID, the empty set, a
  foreign UUID, sets that reach past the storage, the tagged GTID); purge
  up to each binlog, refused purges, purge twice, and what the searches
  answer afterwards.
- 'purge_*': one edit ('binlog.index' with entries dropped, reordered,
  duplicated, pointing outside, naming other files; symlinked objects;
  sizes, timestamps, GTID sets, unreadable metadata), then one
  'purge_binlogs'. The directory is inventoried before and after.
- 'search_*': one edit (timestamps, GTID sets, the order and the entries of
  the index, unreadable metadata), then searches whose true answers are
  known. For the cases marked for it the binlogs a wrong answer names are
  replayed ('mysqlbinlog ... | mysql', as a restore would) into a scratch
  server, and the data is compared with the true state at that point (plain
  storage only: the files of an encrypted one cannot be given to
  mysqlbinlog).

A case passes when the command refuses (exit code 1 and an error answer)
or does exactly what was asked. It fails when
- a purge deletes or changes anything but the binlogs up to the one named
  (and the index), or changes anything at all while answering with an
  error, or touches a canary;
- a successful purge leaves a storage that 'list' cannot read back as the
  remaining binlogs;
- a search exits 0 with a list that omits a binlog needed for the point
  asked for, holds one past it, is in the wrong order, or covers a GTID
  set that is not stored;
- (without an edit) a command refuses what it should answer;
- a command dies on a signal, runs into its timeout or exits with
  anything but 0 or 1.
A refusal of something that is stored, caused by an edit, is recorded as
'denied' and passes.

On 2026-11-16 (MySQL 8.4.8, debug build) every 'clean_*' case passes and
22 edits fail in GTID mode (18 in position mode), the same on both
storages; all of them are consequences of ticket 53, three also of ticket
80 (a binlog whose metadata is unreadable is left out of a "success"):
- a 'binlog.index' without its first entries makes any 'purge_binlogs',
  even one that answers with an error, delete those binlogs as leftovers
  of an interrupted purge; a reordered index makes a purge delete by index
  position (binlog 3 for "up to 2", the newest binlog for "up to 3");
- a lowered 'size' of the newest binlog makes any 'purge_binlogs', even a
  refused one, truncate it (through a symbolic link too, ticket 57); in
  GTID mode the next fetch does not bring the cut transactions back;
- an edited 'min_timestamp' or 'added_gtids', a dropped or swapped index
  entry or an unreadable metadata file make the searches answer "success"
  with a list that omits needed binlogs or is out of order, and the replay
  of that list gives other data than the true state.

A JSON line per case and storage flavour (the full answers go to
result.json), then a summary; exit 1 if any case failed. GTID-set cases
run only with '--mode gtid'. Example:

  PYTHONPATH=~/ws/qa/pylib python3 purge_search_tamper_test.py \\
    --mysql-basedir ~/ws/dist/mysql-8.4.8-linux-glibc2.28-x86_64-minimal \\
    --binsrv ~/ws/percona-binlog-server-build-debug_gcc14/binlog_server \\
    --workdir ~/ws/qa/purge-search --mode gtid
"""

import argparse
import datetime
import hashlib
import json
import os
import shutil
import struct
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import fetch_crash_test as harness  # noqa: E402
import metadata_tamper_test as tamper  # noqa: E402

BINLOGS = 6
TRANSACTIONS = 3
# GTID mode: the rewrite file size and the filler that reaches it (see
# build_source)
REWRITE_FILE_SIZE = "8K"
REWRITE_PADDING = 10000
# the events between transactions, which rewrite mode generates itself
NON_TRANSACTION_EVENTS = {harness.ROTATE_EVENT, harness.STOP_EVENT,
                          harness.FORMAT_DESCRIPTION_EVENT,
                          harness.PREVIOUS_GTIDS_LOG_EVENT}
GTID_TAG = "qatag"
FOREIGN_UUID = "f0f0f0f0-aaaa-4bbb-8ccc-0123456789ab"
COMMAND_TIMEOUT_SECONDS = 60
FLAVOURS = ["plain", "encrypted"]
FAR_FUTURE = "2099-01-01T00:00:00"
FAR_PAST = "2000-01-01T00:00:00"

# set by main()
BINARY: Path = None
WORKDIR: Path = None
BASEDIR: Path = None
GTID_MODE = False
SOURCE: harness.MySQLServer = None
SCRATCH: "harness.MySQLServer | None" = None
SCRATCH_PORT = 0
BASE_CONFIG = {}
GOLDEN = {}
TRUTH: "Truth | None" = None
KEEP = False


def name_of(number: int) -> str:
    return f"{harness.BINLOG_BASE_NAME}.{number:06d}"


def number_of(name: str) -> int:
    return int(name.rsplit(".", 1)[1])


def short(names) -> str:
    """Binlog names as their numbers: '1,2,4'."""
    if names is None:
        return "none (not stored)"
    return ",".join(str(number_of(name)) for name in names) or "(empty)"


def iso(timestamp: int) -> str:
    return datetime.datetime.fromtimestamp(
        timestamp, datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")


# ---------------------------------------------------------------------------
# the source and the truth
# ---------------------------------------------------------------------------

class Truth:
    """What the source's binlogs hold, read from its files."""

    def __init__(self, statements: list, uuid: str):
        self.uuid = uuid
        self.binlogs = []
        self.transactions = []
        names = SOURCE.binlog_names()
        if len(names) != BINLOGS:
            raise harness.Failure(f"the source has {names}")
        pending = list(statements)
        for name in names:
            data = SOURCE.binlog_path(name).read_bytes()
            stamps = []
            binlog = {"name": name, "size": len(data), "transactions": []}
            in_transaction = False
            for offset, type_code, _ in harness.parse_events(data):
                stamp = struct.unpack_from("<I", data, offset)[0]
                if type_code in harness.TRANSACTION_START_EVENTS:
                    in_transaction = True
                elif type_code in NON_TRANSACTION_EVENTS:
                    in_transaction = False
                # rewrite mode (GTID mode) writes its own preamble and
                # ROTATE events, so only the source's transactions say what
                # the stored binlog's time range is
                if stamp and (in_transaction or not GTID_MODE):
                    stamps.append(stamp)
                if type_code in harness.TRANSACTION_START_EVENTS:
                    statement = pending.pop(0)
                    if statement["binlog"] != name:
                        raise harness.Failure(
                            f"{statement['label']} is in {name}")
                    statement.update(time=stamp, offset=offset)
                    binlog["transactions"].append(statement)
                    self.transactions.append(statement)
            binlog["min"], binlog["max"] = min(stamps), max(stamps)
            self.binlogs.append(binlog)
        if pending:
            raise harness.Failure(f"not in the source's binlogs: {pending}")
        times = [entry["time"] for entry in self.transactions
                 if entry.get("payload")]
        if times != sorted(set(times)):
            raise harness.Failure(
                f"transactions do not have a second each: {times}")

    @property
    def names(self) -> list:
        return [binlog["name"] for binlog in self.binlogs]

    def binlog(self, number: int) -> dict:
        return self.binlogs[number - 1]

    def transaction(self, label: str) -> dict:
        return next(entry for entry in self.transactions
                    if entry["label"] == label)

    def keys_of(self, binlog: dict) -> set:
        return {entry["gtid"] for entry in binlog["transactions"]}

    def gtid_text(self, keys) -> str:
        """A GTID set of GTID keys ('u:<n>' untagged, 't:<n>' tagged,
        'f:<n>' of a foreign UUID)."""
        parts = []
        plain = sorted(int(key[2:]) for key in keys if key[0] == "u")
        tagged = sorted(int(key[2:]) for key in keys if key[0] == "t")
        if plain or tagged:
            text = self.uuid
            if plain:
                text += "".join(f":{number}" for number in plain)
            if tagged:
                text += f":{GTID_TAG}" + "".join(f":{number}"
                                                 for number in tagged)
            parts.append(text)
        foreign = sorted(int(key[2:]) for key in keys if key[0] == "f")
        if foreign:
            parts.append(FOREIGN_UUID + "".join(f":{number}"
                                                for number in foreign))
        return ",".join(parts)

    def prefix_text(self, last: int) -> str:
        return f"{self.uuid}:1-{last}" if last > 1 else f"{self.uuid}:1"

    def added_text(self, binlog: dict) -> str:
        """The GTIDs of a binlog as one interval (plus the tagged one)."""
        plain = sorted(int(key[2:]) for key in self.keys_of(binlog)
                       if key[0] == "u")
        text = f"{self.uuid}:{plain[0]}-{plain[-1]}"
        if any(key[0] == "t" for key in self.keys_of(binlog)):
            text += f":{GTID_TAG}:1"
        return text

    # -- the true answers ---------------------------------------------------
    def by_timestamp(self, timestamp: int, stored=None):
        """The binlogs with an event at or before 'timestamp' (README), or
        None when there is none."""
        result = [binlog["name"] for binlog in self.binlogs
                  if binlog["min"] <= timestamp and
                  (stored is None or binlog["name"] in stored)]
        return result or None

    def by_gtids(self, keys: set, stored=None):
        """The binlogs that hold a GTID of 'keys', or None when one of the
        GTIDs is not stored."""
        result = []
        covered = set()
        for binlog in self.binlogs:
            if stored is not None and binlog["name"] not in stored:
                continue
            own = self.keys_of(binlog)
            if own & keys:
                result.append(binlog["name"])
                covered |= own
        return result if keys <= covered else None

    def state(self, included) -> dict:
        """The tables after the transactions that 'included' selects."""
        rows, counter, tables = [], None, False
        for entry in self.transactions:
            if not included(entry):
                continue
            if entry["label"] == "create_t":
                tables = True
            if entry.get("payload"):
                rows.append(entry["payload"])
            if entry.get("counter") is not None:
                counter = entry["counter"]
        if not tables:
            return {"error": "no tables"}
        return {"rows": rows, "counter": counter}


def next_second():
    """Sleeps into the first quarter of the next second, so that what
    follows gets a second of its own."""
    time.sleep(1.25 - time.time() % 1.0)


def build_source(tagged: bool) -> Truth:
    SOURCE.reset()  # CREATE TABLE qa.t is the first statement binlogged
    statements = [{"label": "create_t", "binlog": name_of(1)}]
    SOURCE.sql("CREATE TABLE qa.counter (id INT PRIMARY KEY, v INT NOT NULL)"
               " ENGINE=InnoDB")
    statements.append({"label": "create_counter", "binlog": name_of(1)})
    SOURCE.sql("INSERT INTO qa.counter VALUES (1, 0)")
    statements.append({"label": "init_counter", "binlog": name_of(1),
                       "counter": 0})
    if GTID_MODE:
        # rewrite mode (mandatory in GTID mode since PBS-80 / PBS-98) cuts
        # its own binlogs by size: the last transaction of each source
        # binlog also writes REWRITE_PADDING bytes here, which takes the
        # stored binlog past REWRITE_FILE_SIZE right where the source
        # rotates, so the stored binlogs are the source's (the rest of a
        # binlog stays far below that size)
        SOURCE.sql("CREATE TABLE qa.pad (id INT UNSIGNED NOT NULL "
                   "AUTO_INCREMENT PRIMARY KEY, filler LONGBLOB) "
                   "ENGINE=InnoDB")
        statements.append({"label": "create_pad", "binlog": name_of(1)})
    counter = 0
    for number in range(1, BINLOGS + 1):
        labels = [f"b{number}t{index}"
                  for index in range(1, TRANSACTIONS + 1)]
        if tagged and number == BINLOGS - 1:
            labels.append("tagged")
        for label in labels:
            counter += 1
            next_second()
            prefix = (f"SET gtid_next='AUTOMATIC:{GTID_TAG}';"
                      if label == "tagged" else "")
            padding = (f"INSERT INTO qa.pad (filler) VALUES "
                       f"(REPEAT('p', {REWRITE_PADDING}));"
                       if GTID_MODE and number < BINLOGS and
                       label == labels[-1] else "")
            SOURCE.sql(
                f"{prefix}BEGIN;"
                f"INSERT INTO qa.t (payload) VALUES ('{label}');"
                f"{padding}"
                f"UPDATE qa.counter SET v={counter} WHERE id=1;COMMIT")
            statements.append({"label": label, "binlog": name_of(number),
                               "payload": label, "counter": counter})
        if number < BINLOGS:
            next_second()
            SOURCE.sql("FLUSH BINARY LOGS")
    plain = 0
    for statement in statements:
        if statement["label"] == "tagged":
            statement["gtid"] = "t:1"
        else:
            plain += 1
            statement["gtid"] = f"u:{plain}"
    uuid = SOURCE.sql("SELECT @@global.server_uuid").strip()
    return Truth(statements, uuid)


# ---------------------------------------------------------------------------
# binlog_server on a copy of the storage
# ---------------------------------------------------------------------------

def make_config(storage: Path, flavour: str, path: Path) -> Path:
    config = json.loads(json.dumps(BASE_CONFIG))
    config["storage"] = {"backend": "file", "uri": f"file://{storage}"}
    if flavour == "encrypted":
        keyring = WORKDIR / "binsrv" / "keyring.json"
        keyring.write_text(json.dumps(tamper.KEYRING))
        config["keyring"] = {"uri": f"file://{keyring}"}
        config["storage"]["encryption"] = dict(tamper.ENCRYPTION)
    path.write_text(json.dumps(config, indent=2))
    return path


def run_binsrv(config: Path, command: str, *arguments: str) -> dict:
    argv = [str(BINARY), command, str(config), *arguments]
    record = {"cmd": " ".join(argv),
              "what": " ".join([command, *arguments])}
    try:
        completed = subprocess.run(
            argv, capture_output=True, timeout=COMMAND_TIMEOUT_SECONDS,
            check=False,
            env=dict(os.environ, AWS_EC2_METADATA_DISABLED="true"))
    except subprocess.TimeoutExpired:
        record.update(exit="timeout", status=None, message=None, names=None,
                      answer=None)
        return record
    output = completed.stdout.decode(errors="replace").strip()
    try:
        answer = json.loads(output)
    except ValueError:
        answer = None
    record["exit"] = completed.returncode
    if isinstance(answer, dict):
        record["status"] = answer.get("status")
        record["message"] = answer.get("message")
        result = answer.get("result")
        record["names"] = ([entry.get("name") for entry in result]
                           if isinstance(result, list) else None)
        record["answer"] = answer
    else:
        record.update(status=None, message=None, names=None,
                      answer=(output or completed.stderr.decode(
                          errors="replace"))[-400:])
    return record


def died(run: dict) -> "str | None":
    """Invariant of every command: it exits 0 with a non-error answer or 1
    with an error answer."""
    if run["exit"] == 0 and run["status"] in ("success", "warning"):
        return None
    if run["exit"] == 1 and run["status"] == "error":
        return None
    return (f"'{run['what']}' ended with exit "
            f"{run['exit']} and status {run['status']!r}")


class Box:
    """A fresh copy of a storage with canaries next to it."""

    def __init__(self, case: str, flavour: str):
        self.flavour = flavour
        self.dir = WORKDIR / "c" / f"{case}-{flavour}"
        shutil.rmtree(self.dir, ignore_errors=True)
        self.dir.mkdir(parents=True)
        self.storage = self.dir / "st"
        shutil.copytree(GOLDEN[flavour], self.storage)
        # canaries: nothing but the storage may ever change
        (self.dir / "canary.txt").write_text("canary\n")
        (self.dir / "keyring.json").write_text(json.dumps(tamper.KEYRING))
        first = name_of(1)
        shutil.copy(self.storage / first, self.dir / first)
        shutil.copy(self.storage / (first + ".json"),
                    self.dir / (first + ".json"))
        victim = self.dir / "victim"
        victim.mkdir()
        shutil.copy(self.storage / first, victim / first)
        shutil.copy(self.storage / (first + ".json"),
                    victim / (first + ".json"))
        self.config = make_config(
            self.storage, flavour,
            WORKDIR / "binsrv" / f"{case}-{flavour}.json")
        self.commands = []
        self.edit = None

    def run(self, command: str, *arguments: str) -> dict:
        record = run_binsrv(self.config, command, *arguments)
        self.commands.append(record)
        return record

    def inventory(self) -> dict:
        result = {}
        for path in sorted(self.dir.rglob("*")):
            name = str(path.relative_to(self.dir))
            if path.is_symlink():
                result[name] = f"link to {os.readlink(path)}"
            elif path.is_file():
                data = path.read_bytes()
                result[name] = (f"{len(data)} bytes "
                                f"{hashlib.sha256(data).hexdigest()[:16]}")
        return result

    # -- the attacker's edits -----------------------------------------------
    def metadata(self, number: int) -> dict:
        return json.loads(
            (self.storage / (name_of(number) + ".json")).read_text())

    def set_member(self, number: int, member: str, value) -> str:
        path = self.storage / (name_of(number) + ".json")
        metadata = json.loads(path.read_text())
        old = metadata.get(member, "<absent>")
        if value is tamper.REMOVE:
            metadata.pop(member, None)
            value = "<removed>"
        else:
            metadata[member] = value
        path.write_text(json.dumps(metadata))
        return f"{path.name} '{member}': {old!r} -> {value!r}"

    def index(self) -> list:
        return [line for line in
                (self.storage / "binlog.index").read_text().splitlines()
                if line]

    def set_index(self, lines: list) -> str:
        old = self.index()
        (self.storage / "binlog.index").write_text(
            "".join(line + "\n" for line in lines))
        return f"binlog.index: {old} -> {lines}"

    def stored_names(self) -> list:
        return [path.name for path in self.storage.iterdir()]

    def discard(self):
        if not KEEP:
            shutil.rmtree(self.dir, ignore_errors=True)
            self.config.unlink(missing_ok=True)


def compact(commands: list) -> list:
    """The commands of a case for its JSON line (result.json has the full
    answers)."""
    result = []
    for command in commands:
        entry = {key: value for key, value in command.items()
                 if key not in ("answer", "what") and value is not None}
        if isinstance(entry.get("names"), list):
            entry["names"] = short(entry["names"])
        result.append(entry)
    return result


# ---------------------------------------------------------------------------
# purge: what was asked for against what happened
# ---------------------------------------------------------------------------

def requested_objects(before: dict, target: str) -> set:
    """The objects 'purge_binlogs <target>' is documented to remove: the
    binlogs with a sequence number up to the target's, if it is stored and
    not the newest."""
    stored = sorted(name[3:] for name in before
                    if name.startswith("st/") and name[3:] in TRUTH.names)
    if target not in stored or target == stored[-1]:
        return set()
    result = set()
    for name in stored:
        if number_of(name) <= number_of(target):
            result |= {f"st/{name}", f"st/{name}.json"}
    return result


def purge_and_check(box: Box, target: str, clean: bool = False) -> list:
    before = box.inventory()
    run = box.run("purge_binlogs", target)
    after = box.inventory()
    removed = sorted(set(before) - set(after))
    created = sorted(set(after) - set(before))
    changed = sorted(name for name in set(before) & set(after)
                     if before[name] != after[name])
    requested = requested_objects(before, target)
    run["requested"] = short(sorted(
        name[3:] for name in requested if not name.endswith(".json")))
    run["removed"] = removed
    run["changed"] = [f"{name}: {before[name]} -> {after[name]}"
                      for name in changed]
    run["created"] = created
    problems = []
    violation = died(run)
    if violation:
        problems.append(violation)
    succeeded = run["exit"] == 0
    verdict = ("answered " + str(run["status"]) if succeeded else
               f"was refused ({run['message']})")
    unasked = [name for name in removed
               if not succeeded or name not in requested]
    if unasked:
        problems.append(
            f"'purge_binlogs {target}' {verdict} and deleted {unasked}, "
            f"which it was not asked to (asked: binlogs {run['requested']})")
    allowed = {"st/binlog.index"} if succeeded else set()
    modified = [entry for name, entry in zip(changed, run["changed"])
                if name not in allowed]
    if modified:
        problems.append(f"'purge_binlogs {target}' {verdict} and changed "
                        f"{modified}")
    if created:
        problems.append(f"'purge_binlogs {target}' created {created}")
    if not succeeded:
        if clean and requested:
            problems.append(f"'purge_binlogs {target}' {verdict}")
        return problems
    removed_binlogs = sorted(name[3:] for name in removed
                             if name[3:] in TRUTH.names)
    if sorted(run["names"] or []) != removed_binlogs:
        problems.append(f"the answer names {short(run['names'])} as "
                        f"removed, {short(removed_binlogs)} are gone")
    kept = sorted(requested - set(removed))
    if kept:
        run["not_removed"] = kept
        if clean:
            problems.append(f"asked to remove {kept}, which are still "
                            f"there")
    survivors = sorted(name[3:] for name in after
                       if name.startswith("st/") and name[3:] in TRUTH.names)
    listed = box.run("list")
    if listed["exit"] != 0 or sorted(listed["names"] or []) != survivors \
            or (clean and listed["names"] != survivors):
        problems.append(
            f"after the purge 'list' exits {listed['exit']} with "
            f"{listed['message'] or short(listed['names'])}; the storage "
            f"holds binlogs {short(survivors)}")
    return problems


# ---------------------------------------------------------------------------
# search: the answer against the truth
# ---------------------------------------------------------------------------

def probe(box: Box, command: str, argument: str, truth, clean: bool,
          label: str = "") -> "str | None":
    """One search. 'truth' is the list of names of the true answer, None
    when the truth is that it cannot be answered from the storage."""
    run = box.run(command, argument)
    run["probe"] = label
    run["truth"] = short(truth)
    wrong = died(run)
    if wrong is None and run["exit"] != 0:
        if truth is not None:
            run["denied"] = True
            if clean:
                wrong = (f"refused ({run['message']}); the storage holds "
                         f"the answer: binlogs {short(truth)}")
    elif wrong is None:
        names = run["names"] or []
        if truth is None:
            wrong = (f"answered success with binlogs {short(names)} for "
                     f"something the storage does not hold")
        elif names != truth:
            parts = []
            omitted = [name for name in truth if name not in names]
            extra = [name for name in names if name not in truth]
            if omitted:
                parts.append(f"omits {short(omitted)}")
            if extra:
                parts.append(f"adds {short(extra)}")
            if not parts:
                parts.append("is in the wrong order")
            wrong = (f"answered success with binlogs {short(names)}; the "
                     f"truth is {short(truth)}: " + ", ".join(parts))
    if wrong:
        run["wrong"] = wrong
        return f"{command} {argument} [{label}] {wrong}"
    return None


def timestamp_probes() -> list:
    """(label, time): the second transaction of the binlogs 2 and up, and
    a time after the last binlog."""
    probes = [(f"b{number}t2", TRUTH.transaction(f"b{number}t2")["time"])
              for number in range(2, BINLOGS + 1)]
    probes.append(("after_last", TRUTH.binlog(BINLOGS)["max"] + 3600))
    return probes


def gtid_probes() -> list:
    """(label, GTID keys): everything up to the second transaction of the
    binlogs 2 and up, and the first transaction of each binlog alone."""
    probes = []
    for number in range(2, BINLOGS + 1):
        last = int(TRUTH.transaction(f"b{number}t2")["gtid"][2:])
        probes.append((f"upto_b{number}t2",
                       {f"u:{index}" for index in range(1, last + 1)}))
    for number in range(1, BINLOGS + 1):
        probes.append((f"only_b{number}t1",
                       {TRUTH.transaction(f"b{number}t1")["gtid"]}))
    return probes


def gtid_argument(keys: set) -> str:
    plain = sorted(int(key[2:]) for key in keys)
    if plain == list(range(1, len(plain) + 1)):
        return TRUTH.prefix_text(len(plain))
    return TRUTH.gtid_text(keys)


def search_probes(box: Box, kinds: tuple, clean: bool = False,
                  stored=None) -> list:
    wrong = []
    if "timestamp" in kinds:
        for label, stamp in timestamp_probes():
            wrong.append(probe(box, "search_by_timestamp", iso(stamp),
                               TRUTH.by_timestamp(stamp, stored), clean,
                               label))
    if "gtid" in kinds and GTID_MODE:
        for label, keys in gtid_probes():
            wrong.append(probe(box, "search_by_gtid_set",
                               gtid_argument(keys),
                               TRUTH.by_gtids(keys, stored), clean, label))
    return [entry for entry in wrong if entry]


# ---------------------------------------------------------------------------
# replay: what a restore from the answer gives
# ---------------------------------------------------------------------------

def scratch_server() -> harness.MySQLServer:
    global SCRATCH  # pylint: disable=global-statement
    if SCRATCH is None:
        SCRATCH = harness.MySQLServer(BASEDIR, WORKDIR / "scratch",
                                      SCRATCH_PORT, GTID_MODE, server_id=2)
        SCRATCH.start()
    return SCRATCH


def replay(storage: Path, names: list, stop_after: "int | None" = None,
           include_gtids: "str | None" = None) -> dict:
    """Restores the scratch server to the base state (no tables, no GTIDs)
    and replays the binlogs in the order given, as a point-in-time restore
    does: up to and including the second 'stop_after', or the GTIDs of
    'include_gtids' only."""
    server = scratch_server()
    server.sql("DROP DATABASE IF EXISTS qa; CREATE DATABASE qa;")
    server.sql("RESET BINARY LOGS AND GTIDS" if server.version() >= (8, 2, 0)
               else "RESET MASTER")
    options = []
    if stop_after is not None:
        # --stop-datetime is exclusive and in the local time zone
        options.append("--stop-datetime=" +
                       iso(stop_after + 1).replace("T", " "))
    if include_gtids is not None:
        options.append(f"--include-gtids={include_gtids}")
    argv = [str(BASEDIR / "bin" / "mysqlbinlog"), "--no-defaults", *options,
            *[str(storage / name) for name in names]]
    dump = subprocess.run(argv, capture_output=True, check=False,
                          env=dict(os.environ, TZ="UTC"))
    client = subprocess.run(
        [str(BASEDIR / "bin" / "mysql"), "--no-defaults",
         f"--socket={server.socket}", "-uroot"], input=dump.stdout,
        capture_output=True, check=False)
    result = {"cmd": "mysqlbinlog " + " ".join(
        options + [name for name in names]) + " | mysql",
              "mysqlbinlog_exit": dump.returncode,
              "mysql_exit": client.returncode}
    errors = (dump.stderr + client.stderr).decode(errors="replace").strip()
    if errors:
        result["errors"] = errors[-300:]
    try:
        result["state"] = {
            "rows": server.sql("SELECT CONVERT(payload USING utf8mb4) FROM "
                               "qa.t ORDER BY id").split(),
            "counter": int(server.sql(
                "SELECT v FROM qa.counter WHERE id=1").strip() or -1)}
    except RuntimeError as error:
        result["state"] = {"error": str(error)[:160]}
    return result


def replay_evidence(box: Box, wanted_command: str, wanted_label: str) -> dict:
    """Replays what the probe 'wanted_label' was answered, and the true
    list, and compares both with the true state at that point."""
    run = next((command for command in box.commands
                if command.get("probe") == wanted_label and
                command["what"].split(" ")[0] == wanted_command), None)
    if run is None or not run.get("wrong") or run["exit"] != 0:
        return {"skipped": "the answer to replay is not a wrong success"}
    if box.flavour != "plain":
        return {"skipped": "mysqlbinlog cannot read an encrypted storage"}
    truth_names = [name for name in TRUTH.names
                   if str(number_of(name)) in run["truth"].split(",")]
    argument = run["what"].split(" ", 1)[1]
    if wanted_command == "search_by_timestamp":
        stamp = TRUTH.transaction(wanted_label)["time"]
        options = {"stop_after": stamp}
        true_state = TRUTH.state(lambda entry: entry["time"] <= stamp)
    else:
        last = int(TRUTH.transaction(
            wanted_label.replace("upto_", ""))["gtid"][2:])
        options = {"include_gtids": argument}
        true_state = TRUTH.state(lambda entry: entry["gtid"][0] == "u" and
                                 int(entry["gtid"][2:]) <= last)
    control = replay(GOLDEN["plain"], truth_names, **options)
    answered = replay(box.storage, run["names"], **options)
    evidence = {"point": f"{wanted_command} {argument}",
                "true_state": true_state, "replay_of_true_list": control,
                "replay_of_answer": answered,
                "control_matches_truth": control["state"] == true_state,
                "answer_gives_true_state": answered["state"] == true_state}
    if "rows" in answered["state"] and "rows" in true_state:
        evidence["rows_missing"] = [
            row for row in true_state["rows"]
            if row not in answered["state"]["rows"]]
        evidence["rows_extra"] = [
            row for row in answered["state"]["rows"]
            if row not in true_state["rows"]]
    return evidence


# ---------------------------------------------------------------------------
# cases without an edit
# ---------------------------------------------------------------------------

def finish(box: Box, problems: list, **extra) -> dict:
    result = {"ok": not problems}
    if problems:
        result["why"] = "; ".join(problems[:6])
        if len(problems) > 6:
            result["why"] += f"; and {len(problems) - 6} more"
        result["problems"] = len(problems)
    if box.edit:
        result["edit"] = box.edit
    denied = sum(1 for command in box.commands if command.get("denied"))
    if denied:
        result["denied"] = denied
    result.update(extra)
    result["commands"] = box.commands
    return result


def case_clean_metadata(box: Box) -> dict:
    """The stored metadata against the source's binlogs."""
    problems = []
    listed = box.run("list")
    if listed["exit"] != 0 or listed["names"] != TRUTH.names:
        problems.append(f"list: exit {listed['exit']}, "
                        f"{listed['message'] or short(listed['names'])}")
        return finish(box, problems)
    previous = set()
    table = []
    for entry, binlog in zip(listed["answer"]["result"], TRUTH.binlogs):
        expected = {"size": binlog["size"],
                    "min_timestamp": iso(binlog["min"]),
                    "max_timestamp": iso(binlog["max"])}
        if GTID_MODE:
            expected["previous_gtids"] = set(previous)
            expected["added_gtids"] = TRUTH.keys_of(binlog)
            previous |= TRUTH.keys_of(binlog)
        row = {"name": binlog["name"]}
        for member, value in expected.items():
            stored = entry.get(member)
            row[member] = stored
            if isinstance(value, set):
                # compared through the searches' own notion of the set:
                # here only its text for the usual single-interval shape
                texts = {TRUTH.gtid_text(value), gtid_interval_text(value)}
                if stored not in texts:
                    problems.append(f"{binlog['name']} {member} is "
                                    f"{stored!r}, the source has "
                                    f"{gtid_interval_text(value)!r}")
            elif stored != value:
                problems.append(f"{binlog['name']} {member} is {stored!r}, "
                                f"the source has {value!r}")
        table.append(row)
    if not GTID_MODE:
        run = box.run("search_by_gtid_set", TRUTH.prefix_text(3))
        run["truth"] = "a refusal: the storage is in position mode"
        if died(run) or run["exit"] == 0:
            problems.append(f"search_by_gtid_set on a position-mode storage "
                            f"exits {run['exit']}")
    return finish(box, problems, stored=table)


def gtid_interval_text(keys: set) -> str:
    """The canonical text of a GTID set whose untagged part is one
    interval."""
    plain = sorted(int(key[2:]) for key in keys if key[0] == "u")
    tagged = sorted(int(key[2:]) for key in keys if key[0] == "t")
    if not plain and not tagged:
        return ""
    text = TRUTH.uuid
    if plain:
        text += (f":{plain[0]}-{plain[-1]}" if len(plain) > 1
                 else f":{plain[0]}")
    if tagged:
        text += f":{GTID_TAG}:" + (f"{tagged[0]}-{tagged[-1]}"
                                   if len(tagged) > 1 else f"{tagged[0]}")
    return text


def case_clean_timestamp_boundaries(box: Box) -> dict:
    """search_by_timestamp at every boundary of an untouched storage."""
    stamps = {}
    for binlog in TRUTH.binlogs:
        number = number_of(binlog["name"])
        stamps[f"b{number}_first_event-1"] = binlog["min"] - 1
        stamps[f"b{number}_first_event"] = binlog["min"]
        stamps[f"b{number}_last_event"] = binlog["max"]
        stamps[f"b{number}_last_event+1"] = binlog["max"] + 1
        for entry in binlog["transactions"]:
            stamps[entry["label"]] = entry["time"]
            stamps[entry["label"] + "-1"] = entry["time"] - 1
    stamps["after_last+1h"] = TRUTH.binlog(BINLOGS)["max"] + 3600
    stamps["after_last+10y"] = TRUTH.binlog(BINLOGS)["max"] + 315360000
    stamps["before_first-1h"] = TRUTH.binlog(1)["min"] - 3600
    problems = []
    for label, stamp in stamps.items():
        truth = TRUTH.by_timestamp(stamp)
        wrong = probe(box, "search_by_timestamp", iso(stamp), truth, True,
                      label)
        if wrong:
            problems.append(wrong)
        # every binlog with a transaction up to that second is needed
        needed = [binlog["name"] for binlog in TRUTH.binlogs
                  if any(entry["time"] <= stamp
                         for entry in binlog["transactions"])]
        run = box.commands[-1]
        if run["exit"] == 0 and not set(needed) <= set(run["names"] or []):
            problems.append(f"{label}: binlogs {short(needed)} hold "
                            f"transactions up to {iso(stamp)}, answered "
                            f"{short(run['names'])}")
    # the same second written in other ways: answered like the plain form,
    # or refused
    stamp = TRUTH.transaction("b3t2")["time"]
    plain = iso(stamp)
    truth = TRUTH.by_timestamp(stamp)
    for label, text in (("space", plain.replace("T", " ")),
                        ("zulu", plain + "Z"),
                        ("fraction", plain + ".000")):
        wrong = probe(box, "search_by_timestamp", text, truth, False,
                      "format_" + label)
        if wrong:
            problems.append(wrong)
    return finish(box, problems, probes=len(box.commands))


def case_clean_gtid_boundaries(box: Box) -> dict:
    """search_by_gtid_set at every boundary of an untouched storage."""
    plain = sorted(int(entry["gtid"][2:]) for entry in TRUTH.transactions
                   if entry["gtid"][0] == "u")
    total = plain[-1]
    tagged = any(entry["gtid"] == "t:1" for entry in TRUTH.transactions)
    uuid = TRUTH.uuid
    sets = []
    for last in plain:
        keys = {f"u:{index}" for index in range(1, last + 1)}
        sets.append((f"prefix_{last}", TRUTH.prefix_text(last), keys))
        sets.append((f"single_{last}", f"{uuid}:{last}", {f"u:{last}"}))
    sets += [
        ("past_end_prefix", f"{uuid}:1-{total + 5}",
         {f"u:{index}" for index in range(1, total + 6)}),
        ("past_end_single", f"{uuid}:{total + 1}", {f"u:{total + 1}"}),
        ("gapped", f"{uuid}:2:{total - 1}", {"u:2", f"u:{total - 1}"}),
        ("first_and_last", f"{uuid}:1:{total}", {"u:1", f"u:{total}"}),
        ("foreign_uuid", f"{FOREIGN_UUID}:1", {"f:1"}),
        ("stored_and_foreign", f"{uuid}:1-3,{FOREIGN_UUID}:1",
         {"u:1", "u:2", "u:3", "f:1"}),
        ("unknown_tag", f"{uuid}:othertag:1", {"x:1"}),
    ]
    if tagged:
        sets += [
            ("tagged", f"{uuid}:{GTID_TAG}:1", {"t:1"}),
            ("tagged_past_end", f"{uuid}:{GTID_TAG}:1-2", {"t:1", "t:2"}),
            ("everything", f"{uuid}:1-{total}:{GTID_TAG}:1",
             {f"u:{index}" for index in range(1, total + 1)} | {"t:1"}),
        ]
    problems = []
    for label, text, keys in sets:
        wrong = probe(box, "search_by_gtid_set", text, TRUTH.by_gtids(keys),
                      True, label)
        if wrong:
            problems.append(wrong)
    # the empty set: nothing is needed to cover it - an empty list or a
    # refusal, never binlogs
    run = box.run("search_by_gtid_set", "")
    run["probe"] = "empty_set"
    run["truth"] = "(empty) or a refusal"
    if died(run) or (run["exit"] == 0 and run["names"]):
        problems.append(f"the empty set: exit {run['exit']}, "
                        f"{short(run['names'] or [])}")
    return finish(box, problems, probes=len(box.commands), tagged=tagged)


def after_purge_probes(box: Box, stored: list) -> list:
    """The searches on a storage that holds only 'stored': each
    transaction's second, each transaction's GTID, and a prefix set."""
    wrong = []
    for entry in TRUTH.transactions:
        wrong.append(probe(box, "search_by_timestamp", iso(entry["time"]),
                           TRUTH.by_timestamp(entry["time"], stored), True,
                           "after_purge_" + entry["label"]))
        if GTID_MODE:
            wrong.append(probe(
                box, "search_by_gtid_set", TRUTH.gtid_text({entry["gtid"]}),
                TRUTH.by_gtids({entry["gtid"]}, stored), True,
                "after_purge_" + entry["label"]))
    if GTID_MODE:
        last = int(TRUTH.transaction(f"b{BINLOGS}t1")["gtid"][2:])
        keys = {f"u:{index}" for index in range(1, last + 1)}
        wrong.append(probe(box, "search_by_gtid_set", TRUTH.prefix_text(last),
                           TRUTH.by_gtids(keys, stored), True,
                           "after_purge_prefix"))
    return [entry for entry in wrong if entry]


def make_clean_purge(number: int):
    def case(box: Box) -> dict:
        problems = purge_and_check(box, name_of(number), clean=True)
        stored = TRUTH.names[number:]
        problems += after_purge_probes(box, stored)
        # the storage must still be one that fetch accepts and leaves alone
        # (the source got nothing new)
        before = box.inventory()
        fetch = box.run("fetch")
        if fetch["exit"] != 0:
            problems.append(f"fetch after the purge exits {fetch['exit']}")
        elif box.inventory() != before:
            problems.append("fetch after the purge changed the storage")
        return finish(box, problems)
    case.__doc__ = f"purge_binlogs {name_of(number)} on an untouched storage."
    return case


def case_clean_purge_refusals(box: Box) -> dict:
    """Targets that must be refused, with nothing touched."""
    problems = []
    targets = [name_of(BINLOGS), name_of(BINLOGS + 1), name_of(0),
               f"other.{3:06d}", "binlog.3", "binlog.0000003",
               f"../st/{name_of(3)}", f"{box.storage}/{name_of(3)}",
               f"./{name_of(3)}", name_of(3) + ".json", "metadata.json",
               "binlog.index", "", "binlog.-00003", "binlog.+00003",
               "binlog.00003 "]
    for target in targets:
        problems += purge_and_check(box, target, clean=True)
        if box.commands[-1]["exit"] == 0:
            problems.append(f"'purge_binlogs {target!r}' was accepted")
            break
    return finish(box, problems)


def case_clean_purge_twice(box: Box) -> dict:
    """The same purge twice, an older target after it, then a newer one."""
    problems = purge_and_check(box, name_of(3), clean=True)
    for target in (name_of(3), name_of(2)):
        problems += purge_and_check(box, target, clean=True)
        if box.commands[-1]["exit"] == 0:
            problems.append(f"'purge_binlogs {target}' after the purge up "
                            f"to {name_of(3)} was accepted")
    problems += purge_and_check(box, name_of(4), clean=True)
    problems += after_purge_probes(box, TRUTH.names[4:])
    return finish(box, problems)


def case_clean_retention(box: Box) -> dict:
    """Search and purge used together: keep what a restore to 'cutoff' or
    later needs. search_by_timestamp(cutoff) ends with the binlog that
    holds the cutoff; everything before it is purged."""
    problems = []
    for label in ("b2t1", "b4t2", f"b{BINLOGS}t3"):
        cutoff = TRUTH.transaction(label)["time"]
        found = box.run("search_by_timestamp", iso(cutoff))
        found["probe"] = "retention_" + label
        names = found["names"] or []
        if found["exit"] != 0 or not names:
            problems.append(f"search for {label} failed")
            continue
        if len(names) < 2:
            continue
        target = names[-2]
        gone = [name for name in TRUTH.names
                if name in box.stored_names() and
                number_of(name) <= number_of(target)]
        problems += purge_and_check(box, target, clean=True)
        lost = [entry["label"] for entry in TRUTH.transactions
                if entry["time"] >= cutoff and entry["binlog"] in gone]
        if lost:
            problems.append(f"the purge up to {target}, chosen from the "
                            f"search for {iso(cutoff)}, removed the later "
                            f"transactions {lost}")
        again = box.run("search_by_timestamp", iso(cutoff))
        again["probe"] = "retention_after_" + label
        if again["exit"] != 0 or again["names"] != names[-1:]:
            problems.append(
                f"after the purge the search for {iso(cutoff)} gives exit "
                f"{again['exit']}, {short(again['names'] or [])}; expected "
                f"{short(names[-1:])}")
    return finish(box, problems)


# ---------------------------------------------------------------------------
# purge after an edit
# ---------------------------------------------------------------------------

def tail_cut(box: Box) -> int:
    """Where the second transaction of the newest binlog starts."""
    del box
    return TRUTH.transaction(f"b{BINLOGS}t2")["offset"]


def swap_into_victim(box: Box, name: str) -> str:
    """Replaces a storage object by a symbolic link to a copy of it in the
    canary directory next to the storage."""
    target = box.dir / "victim" / (name + ".moved")
    shutil.move(box.storage / name, target)
    (box.storage / name).symlink_to(target)
    return f"{name} moved to ../victim/{target.name}, a symlink in its place"


def reordered(order: list):
    def edit(box: Box) -> str:
        lines = box.index()
        return box.set_index([lines[number - 1] for number in order])
    return edit


def first_line(make):
    def edit(box: Box) -> str:
        return box.set_index([make(box)] + box.index()[1:])
    return edit


def member(number: int, name: str, value):
    def edit(box: Box) -> str:
        return box.set_member(number, name,
                              value(box) if callable(value) else value)
    return edit


def raw_metadata(number: int, content: "bytes | None"):
    def edit(box: Box) -> str:
        path = box.storage / (name_of(number) + ".json")
        if content is None:
            path.unlink()
            return f"{path.name} deleted"
        path.write_bytes(content)
        return f"{path.name} replaced by {content!r}"
    return edit


def tail_symlink_and_size(box: Box) -> str:
    return (swap_into_victim(box, name_of(BINLOGS)) + "; " +
            box.set_member(BINLOGS, "size", tail_cut(box)))


def flip_mode(box: Box) -> str:
    path = box.storage / "metadata.json"
    metadata = json.loads(path.read_text())
    old = metadata["mode"]
    metadata["mode"] = "position" if old == "gtid" else "gtid"
    path.write_text(json.dumps(metadata))
    return f"metadata.json 'mode': {old!r} -> {metadata['mode']!r}"


def foreign_previous(box: Box) -> str:
    return box.set_member(4, "previous_gtids", f"{FOREIGN_UUID}:1-999")


# name: (edit, target of the purge, GTID mode only, what is expected)
PURGE_CASES = {
    # -- binlog.index
    "purge_index_only_tail": (
        reordered([BINLOGS]), name_of(1), False,
        "entries of the binlogs 1-5 removed; the operator asks for 1"),
    "purge_index_head_dropped_tail_target": (
        reordered([3, 4, 5, 6]), name_of(BINLOGS), False,
        "entries of the binlogs 1-2 removed; a purge that is refused"),
    "purge_index_swapped_low": (
        reordered([1, 3, 2, 4, 5, 6]), name_of(2), False,
        "entries 2 and 3 swapped; purge up to 2"),
    "purge_index_swapped_high": (
        reordered([1, 3, 2, 4, 5, 6]), name_of(3), False,
        "entries 2 and 3 swapped; purge up to 3"),
    "purge_index_tail_moved": (
        reordered([1, 2, 6, 3, 4, 5]), name_of(3), False,
        "the newest binlog's entry moved before the third; purge up to 3"),
    "purge_index_tail_first": (
        reordered([6, 1, 2, 3, 4, 5]), name_of(2), False,
        "the newest binlog's entry moved to the front"),
    "purge_index_reversed": (
        reordered([6, 5, 4, 3, 2, 1]), name_of(3), False,
        "the index reversed"),
    "purge_index_duplicate": (
        reordered([1, 2, 2, 3, 4, 5, 6]), name_of(3), False,
        "an entry twice"),
    "purge_index_parent_path": (
        first_line(lambda box: f"../{name_of(1)}"), name_of(1), False,
        "first entry '../binlog.000001' (the canary next to the storage)"),
    "purge_index_absolute_path": (
        first_line(lambda box: f"{box.dir}/victim/{name_of(1)}"),
        name_of(1), False, "first entry an absolute path into ../victim"),
    "purge_index_nested_path": (
        first_line(lambda box: f"./../victim/{name_of(1)}"), name_of(1),
        False, "first entry './../victim/binlog.000001'"),
    "purge_index_bare_name": (
        first_line(lambda box: name_of(1)), name_of(1), False,
        "first entry without './'"),
    "purge_index_metadata_json": (
        first_line(lambda box: "./metadata.json"), name_of(2), False,
        "first entry './metadata.json'"),
    "purge_index_self": (
        first_line(lambda box: "./binlog.index"), name_of(2), False,
        "first entry './binlog.index'"),
    "purge_index_keyring_path": (
        first_line(lambda box: f"{box.dir}/keyring.json"), name_of(2),
        False, "first entry the path of the keyring canary"),
    # -- symbolic links
    "purge_symlink_data": (
        lambda box: swap_into_victim(box, name_of(2)), name_of(3), False,
        "binlog 2 a symlink to a file outside the storage"),
    "purge_symlink_metadata": (
        lambda box: swap_into_victim(box, name_of(2) + ".json"), name_of(3),
        False, "the metadata of binlog 2 a symlink to outside"),
    "purge_symlink_index": (
        lambda box: swap_into_victim(box, "binlog.index"), name_of(3),
        False, "binlog.index a symlink to a file outside the storage"),
    "purge_symlink_tail_size": (
        tail_symlink_and_size, name_of(1), False,
        "the newest binlog a symlink to outside and its size lowered"),
    # -- sizes
    "purge_size_tail_lowered": (
        member(BINLOGS, "size", tail_cut), name_of(1), False,
        "size of the newest binlog lowered to its second transaction"),
    "purge_size_tail_lowered_refused": (
        member(BINLOGS, "size", tail_cut), name_of(BINLOGS), False,
        "the same edit; a purge that is refused"),
    "purge_size_tail_raised": (
        member(BINLOGS, "size",
               lambda box: box.metadata(BINLOGS)["size"] + 100),
        name_of(1), False, "size of the newest binlog raised"),
    "purge_size_middle": (
        member(4, "size", lambda box: box.metadata(4)["size"] - 1),
        name_of(2), False, "size of binlog 4 lowered by one"),
    "purge_size_victim": (
        member(1, "size", lambda box: box.metadata(1)["size"] - 1),
        name_of(1), False, "size of the binlog to purge lowered by one"),
    # -- unreadable metadata
    "purge_metadata_unparsable_kept": (
        raw_metadata(4, b"{"), name_of(2), False,
        "metadata of a binlog that stays unparsable"),
    "purge_metadata_missing_kept": (
        raw_metadata(4, None), name_of(2), False,
        "metadata of a binlog that stays deleted"),
    "purge_metadata_unparsable_victim": (
        raw_metadata(1, b"{"), name_of(3), False,
        "metadata of a binlog to purge unparsable"),
    "purge_metadata_unparsable_tail": (
        raw_metadata(BINLOGS, b"{"), name_of(3), False,
        "metadata of the newest binlog unparsable"),
    # -- members the purge does not need
    "purge_timestamps_edited": (
        member(2, "min_timestamp", FAR_FUTURE), name_of(3), False,
        "min_timestamp of binlog 2 in 2099"),
    "purge_previous_gtids_edited": (
        foreign_previous, name_of(3), True,
        "previous_gtids of binlog 4 (the first to stay) replaced"),
    "purge_storage_mode_flipped": (
        flip_mode, name_of(3), False,
        "'mode' of metadata.json flipped"),
    "purge_added_gtids_edited": (
        member(2, "added_gtids", f"{FOREIGN_UUID}:1-999"), name_of(3), True,
        "added_gtids of a binlog to purge replaced"),
}


def aftermath(box: Box) -> dict:
    """What is left after a purge that did more than it was asked to: the
    transactions that are gone from the storage, and whether the next
    'fetch' notices or brings them back (the source still has them)."""
    def lost() -> list:
        result = []
        for entry in TRUTH.transactions:
            path = box.storage / entry["binlog"]
            if not path.exists() or entry["offset"] >= path.stat().st_size:
                result.append(entry["label"])
        return result
    result = {"transactions_lost": lost()}
    fetch = box.run("fetch")
    listed = box.run("list")
    result["fetch_exit"] = fetch["exit"]
    result["list_after_fetch"] = (short(listed["names"])
                                  if listed["exit"] == 0 and listed["names"]
                                  else listed["message"])
    result["transactions_lost_after_fetch"] = lost()
    return result


def make_purge_case(edit, target: str, description: str):
    def case(box: Box) -> dict:
        box.edit = edit(box)
        problems = purge_and_check(box, target)
        extra = {"description": description}
        if problems:
            extra["aftermath"] = aftermath(box)
        return finish(box, problems, **extra)
    case.__doc__ = description
    return case


def case_purge_retention_max_lowered(box: Box) -> dict:
    """Purging by age from what 'list' shows: the operator purges up to the
    newest binlog whose max_timestamp is older than the cutoff (the first
    transaction of binlog 3). max_timestamp of binlog 5 is set to 2000."""
    box.edit = box.set_member(5, "max_timestamp", FAR_PAST)
    cutoff = TRUTH.transaction("b3t1")["time"]
    truly_old = [binlog["name"] for binlog in TRUTH.binlogs
                 if binlog["max"] < cutoff]
    listed = box.run("list")
    problems = []
    if listed["exit"] != 0:
        return finish(box, problems, note="list refused the storage")
    old = [entry["name"] for entry in listed["answer"]["result"]
           if entry["max_timestamp"] < iso(cutoff)]
    listed["truth"] = f"older than {iso(cutoff)}: {short(truly_old)}"
    if sorted(old) != truly_old:
        listed["wrong"] = (f"list shows binlogs {short(old)} as older than "
                           f"{iso(cutoff)}; truly older: {short(truly_old)}")
    if not old:
        return finish(box, problems)
    target = max(old)
    problems += purge_and_check(box, target)
    run = next(command for command in reversed(box.commands)
               if command["what"].startswith("purge_binlogs"))
    gone = [name[3:] for name in run.get("removed", [])
            if name[3:] in TRUTH.names]
    lost = [entry["label"] for entry in TRUTH.transactions
            if entry["time"] >= cutoff and entry["binlog"] in gone]
    if lost:
        problems.append(
            f"list shows max_timestamp {FAR_PAST} for {name_of(5)}, so the "
            f"purge of what is older than {iso(cutoff)} goes up to {target} "
            f"and removes binlogs {short(gone)} with the newer transactions "
            f"{lost} (truly older: binlogs {short(truly_old)})")
    return finish(box, problems, cutoff=iso(cutoff))


# ---------------------------------------------------------------------------
# search after an edit
# ---------------------------------------------------------------------------

def superset_added(box: Box) -> str:
    return box.set_member(2, "added_gtids", f"{TRUTH.uuid}:1-100000")


def shifted_added(box: Box) -> str:
    return box.set_member(3, "added_gtids", TRUTH.added_text(TRUTH.binlog(5)))


def widened_added(box: Box) -> str:
    return box.set_member(
        3, "added_gtids",
        TRUTH.added_text(TRUTH.binlog(3)) + f",{FOREIGN_UUID}:1-5")


# name: (edit, kinds of search, GTID mode only, the probe to replay,
#        description)
SEARCH_CASES = {
    # -- timestamps
    "search_min_timestamp_future": (
        member(2, "min_timestamp", FAR_FUTURE), ("timestamp",), False,
        ("search_by_timestamp", "b4t2"), "min_timestamp of binlog 2 in 2099"),
    "search_min_timestamp_past": (
        member(5, "min_timestamp", FAR_PAST), ("timestamp",), False, None,
        "min_timestamp of binlog 5 in 2000"),
    "search_max_timestamp_past": (
        member(3, "max_timestamp", FAR_PAST), ("timestamp",), False, None,
        "max_timestamp of binlog 3 in 2000 (below its min_timestamp)"),
    "search_min_timestamp_removed": (
        member(3, "min_timestamp", tamper.REMOVE), ("timestamp", "gtid"),
        False, None, "min_timestamp of binlog 3 removed"),
    "search_min_timestamp_number": (
        member(3, "min_timestamp", 0), ("timestamp",), False, None,
        "min_timestamp of binlog 3 the number 0"),
    # -- unreadable metadata (ticket 80)
    "search_metadata_unparsable": (
        raw_metadata(3, b"{"), ("timestamp", "gtid"), False,
        ("search_by_timestamp", "b4t2"), "metadata of binlog 3 unparsable"),
    "search_metadata_missing": (
        raw_metadata(3, None), ("timestamp", "gtid"), False, None,
        "metadata of binlog 3 deleted"),
    # -- binlog.index
    "search_index_entry_dropped": (
        reordered([1, 2, 4, 5, 6]), ("timestamp", "gtid"), False, None,
        "the entry of binlog 3 removed"),
    "search_index_head_dropped": (
        reordered([3, 4, 5, 6]), ("timestamp", "gtid"), False, None,
        "the entries of the binlogs 1-2 removed"),
    "search_index_swapped": (
        reordered([1, 2, 4, 3, 5, 6]), ("timestamp", "gtid"), False,
        ("search_by_timestamp", "b4t2"), "entries 3 and 4 swapped"),
    "search_index_reversed": (
        reordered([6, 5, 4, 3, 2, 1]), ("timestamp", "gtid"), False, None,
        "the index reversed"),
    "search_index_duplicate": (
        reordered([1, 2, 3, 3, 4, 5, 6]), ("timestamp", "gtid"), False, None,
        "an entry twice"),
    "search_index_unknown_entry": (
        lambda box: box.set_index(box.index()[:3] + [f"./{name_of(9)}"] +
                                  box.index()[3:]),
        ("timestamp", "gtid"), False, None,
        "an entry for a binlog that is not stored"),
    # -- GTID sets
    "search_added_gtids_superset": (
        superset_added, ("gtid",), True,
        ("search_by_gtid_set", "upto_b4t2"),
        "added_gtids of binlog 2 widened to <uuid>:1-100000"),
    "search_added_gtids_emptied": (
        member(3, "added_gtids", ""), ("gtid",), True, None,
        "added_gtids of binlog 3 emptied"),
    "search_added_gtids_shifted": (
        shifted_added, ("gtid",), True, None,
        "added_gtids of binlog 3 replaced by those of binlog 5"),
    "search_added_gtids_foreign": (
        widened_added, ("gtid",), True, None,
        "a foreign UUID added to added_gtids of binlog 3"),
    "search_added_gtids_removed": (
        member(3, "added_gtids", tamper.REMOVE), ("timestamp", "gtid"), True,
        None, "added_gtids of binlog 3 removed"),
    "search_previous_gtids_edited": (
        member(3, "previous_gtids", f"{FOREIGN_UUID}:1-999"), ("gtid",),
        True, None, "previous_gtids of binlog 3 replaced"),
    "search_index_swapped_gtid_replay": (
        reordered([1, 2, 4, 3, 5, 6]), ("gtid",), True,
        ("search_by_gtid_set", "upto_b4t2"),
        "entries 3 and 4 swapped (the GTID search replayed)"),
}


def make_search_case(edit, kinds: tuple, to_replay, description: str):
    def case(box: Box) -> dict:
        box.edit = edit(box)
        problems = search_probes(box, kinds)
        extra = {"description": description}
        if to_replay is not None:
            evidence = replay_evidence(box, *to_replay)
            extra["replay"] = evidence
            if evidence.get("control_matches_truth") is False:
                problems.append("the replay of the true list does not give "
                                "the true state: the replay check is broken")
        return finish(box, problems, **extra)
    case.__doc__ = description
    return case


# ---------------------------------------------------------------------------

def build_cases() -> dict:
    """name -> (function, GTID mode only)."""
    cases = {
        "clean_metadata": (case_clean_metadata, False),
        "clean_timestamp_boundaries": (case_clean_timestamp_boundaries,
                                       False),
        "clean_gtid_boundaries": (case_clean_gtid_boundaries, True),
    }
    for number in range(1, BINLOGS):
        cases[f"clean_purge_upto_{number}"] = (make_clean_purge(number),
                                               False)
    cases["clean_purge_refusals"] = (case_clean_purge_refusals, False)
    cases["clean_purge_twice"] = (case_clean_purge_twice, False)
    cases["clean_retention"] = (case_clean_retention, False)
    for name, (edit, target, gtid_only, description) in PURGE_CASES.items():
        cases[name] = (make_purge_case(edit, target, description), gtid_only)
    cases["purge_retention_max_lowered"] = (case_purge_retention_max_lowered,
                                            False)
    for name, (edit, kinds, gtid_only, to_replay,
               description) in SEARCH_CASES.items():
        cases[name] = (make_search_case(edit, kinds, to_replay, description),
                       gtid_only)
    return cases


CASES = build_cases()


def build_storages(flavours: list):
    for flavour in flavours:
        storage = WORKDIR / "golden" / flavour
        shutil.rmtree(storage, ignore_errors=True)
        storage.mkdir(parents=True)
        config = make_config(storage, flavour,
                             WORKDIR / "binsrv" / f"golden-{flavour}.json")
        completed = subprocess.run(
            [str(BINARY), "fetch", str(config)], capture_output=True,
            timeout=harness.FETCH_TIMEOUT_SECONDS, check=False,
            env=dict(os.environ, AWS_EC2_METADATA_DISABLED="true"))
        if completed.returncode != 0:
            raise harness.Failure(
                f"fetch into the {flavour} storage exited with "
                f"{completed.returncode}: {completed.stderr[-400:]!r}")
        GOLDEN[flavour] = storage


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--mysql-basedir", type=Path, required=True)
    parser.add_argument("--binsrv", type=Path, required=True)
    parser.add_argument("--workdir", type=Path, required=True)
    parser.add_argument("--port", type=int, default=23660,
                        help="the source's port; the scratch server of the "
                             "replays listens on the next one")
    parser.add_argument("--mode", choices=["gtid", "position"],
                        default="position")
    parser.add_argument("--cases", nargs="*", choices=list(CASES),
                        metavar="CASE",
                        help="the cases to run (default: all of the mode)")
    parser.add_argument("--flavours", nargs="*", choices=FLAVOURS,
                        default=FLAVOURS,
                        help="the storages to run the cases on")
    parser.add_argument("--no-tagged", action="store_true",
                        help="no transaction with a tagged GTID")
    parser.add_argument("--keep", action="store_true",
                        help="keep the storage copy of every case")
    args = parser.parse_args()
    global BINARY, WORKDIR, BASEDIR, GTID_MODE, SOURCE, SCRATCH_PORT, \
        BASE_CONFIG, TRUTH, KEEP  # pylint: disable=global-statement
    WORKDIR = args.workdir.expanduser().resolve()
    WORKDIR.mkdir(parents=True, exist_ok=True)
    BASEDIR = args.mysql_basedir.expanduser()
    BINARY = args.binsrv.expanduser()
    GTID_MODE = args.mode == "gtid"
    SCRATCH_PORT = args.port + 1
    KEEP = args.keep
    SOURCE = harness.MySQLServer(BASEDIR, WORKDIR / "mysql", args.port,
                                 GTID_MODE, server_id=1)
    binsrv = harness.BinlogServer(BINARY, WORKDIR / "binsrv", args.port,
                                  GTID_MODE)
    binsrv.reset()
    binsrv.write_config()
    BASE_CONFIG = json.loads(binsrv.config_path.read_text())
    if GTID_MODE:
        BASE_CONFIG["replication"]["rewrite"]["file_size"] = \
            REWRITE_FILE_SIZE
    shutil.rmtree(WORKDIR / "c", ignore_errors=True)
    names = args.cases or list(CASES)
    results = []
    started = time.monotonic()
    setup = {}
    SOURCE.start()
    try:
        tagged = (GTID_MODE and not args.no_tagged and
                  SOURCE.version() >= (8, 3, 0))
        TRUTH = build_source(tagged)
        build_storages(args.flavours)
        setup = {"setup_seconds": round(time.monotonic() - started, 1),
                 "uuid": TRUTH.uuid, "tagged_gtid": tagged,
                 "binlogs": [
                     {"name": binlog["name"],
                      "first_event": iso(binlog["min"]),
                      "last_event": iso(binlog["max"]),
                      "transactions": {
                          entry["label"]: f"{iso(entry['time'])} "
                                          f"{entry['gtid']}"
                          for entry in binlog["transactions"]}}
                     for binlog in TRUTH.binlogs]}
        print(json.dumps({"setup": setup}), flush=True)
        for name in names:
            function, gtid_only = CASES[name]
            for flavour in args.flavours:
                result = {"case": name, "storage": flavour}
                if gtid_only and not GTID_MODE:
                    result.update(ok=True, skipped="needs --mode gtid: GTID "
                                  "sets are stored in GTID mode only")
                    print(json.dumps(result), flush=True)
                    results.append(result)
                    break
                case_started = time.monotonic()
                box = Box(name, flavour)
                try:
                    result.update(function(box))
                except (harness.Failure, RuntimeError, KeyError, ValueError,
                        subprocess.SubprocessError, OSError) as failure:
                    result.update(
                        ok=False, why=harness.describe_failure(failure, 500),
                        edit=box.edit, commands=box.commands)
                box.discard()
                result["seconds"] = round(time.monotonic() - case_started, 1)
                results.append(result)
                printed = dict(result)
                commands = compact(result.get("commands", []))
                if not result.get("ok"):
                    # of a failed case: what went wrong
                    wrong = [command for command in commands
                             if command.get("wrong") or
                             "purge_binlogs" in command["cmd"]]
                    commands = wrong[:4] or commands
                if len(commands) > 8:
                    commands = f"{len(commands)} commands, see result.json"
                printed["commands"] = commands
                print(json.dumps(printed), flush=True)
    finally:
        SOURCE.stop()
        if SCRATCH is not None:
            SCRATCH.stop()
        # the servers' data directories do not outlive the run
        shutil.rmtree(WORKDIR / "mysql", ignore_errors=True)
        shutil.rmtree(WORKDIR / "scratch", ignore_errors=True)
        if not KEEP:
            shutil.rmtree(WORKDIR / "c", ignore_errors=True)
    failures = [result for result in results if not result.get("ok")]
    skipped = [result for result in results if result.get("skipped")]
    print(json.dumps({
        "cases": len(results), "failures": len(failures),
        "failed": [f"{result['case']}/{result['storage']}"
                   for result in failures],
        "skipped": len(skipped), "mode": args.mode,
        "seconds": round(time.monotonic() - started),
        "passed": not failures}), flush=True)
    (WORKDIR / "result.json").write_text(
        json.dumps({"setup": setup, "results": results}, indent=2,
                   default=str))
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
