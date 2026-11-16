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

"""A transaction that is cut short on its way into the storage (ticket 82).

The Binlog Server takes the end of a transaction from one number: the
'transaction_length' of its GTID_LOG / ANONYMOUS_GTID_LOG event. Once the
events received add up to it, the reader calls the stream "at a transaction
boundary", and everything up to a boundary is what a checkpoint, the end of
a run and the storage's destructor (on EVERY error path) write to the
storage and, in GTID mode, record as a stored GTID. A man in the middle
between the Binlog Server and its source who lowers that number (and fixes
the event's CRC32) moves the boundary into the transaction: the first
events of the transaction are stored as if they were all of it.

Every case runs a real MySQL source and the proxy of hostile_source_test.py,
here working on one transaction of the dump (the "target"):

1. 'fetch' stores two transactions directly;
2. the source gets one more, the target, and two after it;
3. 'fetch' (or a running 'pull', with 'checkpoint_size' 1) reads them
   through the proxy, which lies about the target, cuts the connection in
   it or damages one of its events;
4. the storage is decoded: is the target there, in full or as a stub (which
   events, how many bytes), what do the metadata of the last binlog, 'list'
   and 'search_by_gtid_set <the target's GTID>' say;
5. the source gets one more transaction and 'fetch' runs directly, twice if
   the first run fails (the attacker is gone);
6. the storage is decoded again, and for some cases (all with a stub with
   --replica-all) a replica of 'pull' is run on it.

A case PASSES when the target is not recorded before it is complete: the
run is refused or interrupted, and the next honest run stores the target and
everything after it. It FAILS when a stub is stored (and its GTID recorded),
when transactions are missing although the honest 'fetch' exited 0 ("silent
loss"), when the honest 'fetch' cannot continue ("stuck but visible"), or
when a replica of 'pull' dies, stops with an error or ends with other rows
than the source.

Cases ('--cases' selects; the default runs all but the replica_* / mysql_*
ones). The target is 'INSERT' of three rows of 3000 bytes: GTID, QUERY
(BEGIN), TABLE_MAP, two WRITE_ROWS, XID.

lie_* - 'transaction_length' of the target is changed; everything else is
sent as it is unless the name says otherwise:
  control                    nothing is changed
  lie_after_gtid / _begin / _table_map / _first_rows
                             the length ends after that event
  lie_end_minus_1, lie_zero, lie_one, lie_larger
                             the true length - 1, 0, 1, the true length
                             + 1000
  lie_multi_after_statement  a transaction of three statements; the length
                             ends after the first one's ROWS event
  lie_ddl_after_gtid         the target is a CREATE TABLE (GTID, QUERY)
  lie_compressed_after_gtid  binlog_transaction_compression=ON (GTID,
                             TRANSACTION_PAYLOAD)
  lie_last_after_begin       the target is the last transaction of the dump
  lie_after_begin_eof / lie_after_gtid_eof
                             after the false end the proxy ends the dump as
                             the source does (an EOF packet): nothing is
                             left to "exceed" the length
  lie_after_begin_drop       ... the proxy closes the connection instead
  lie_after_begin_rest_removed
                             ... the proxy leaves out the rest of the target
                             and goes on with the transactions after it
  pull_control, pull_lie_after_begin, pull_lie_after_begin_drop
                             the same against a running 'pull' (which
                             reconnects after a drop)

fault_* - no lie, no attacker needed: what an ordinary failure in the
middle of the target leaves behind.
  fault_drop_after_begin / _after_first_rows
                             the connection closes between two events
  fault_drop_mid_event       ... in the middle of an event
  fault_timeout_mid_event    half an event, then silence: the read timeout
                             (3 s)
  fault_sigterm / fault_sigkill
                             'fetch' gets the signal after the first
                             WRITE_ROWS
  fault_source_killed        the source is killed (SIGKILL) there and
                             started again
  pull_fault_drop, pull_fault_timeout, pull_fault_sigterm,
  pull_fault_sigkill         the same against a running 'pull'

throw_* - no lie: the first WRITE_ROWS of the target makes the collector
throw for another reason; the destructor then flushes up to the last
boundary, which must be the end of the transaction before the target.
  throw_bad_checksum, throw_size_lie (the event's size field + 8),
  throw_second_format_description (a FORMAT_DESCRIPTION before it),
  throw_artificial_flag, throw_bad_prefix (the packet starts with 0x01),
  throw_wrong_position (end position + 1), throw_zero_position

Optional (--replica-crash, or named in --cases), GTID mode only:
  replica_matrix             (with --rewrite) the storage with the stub of
                             'lie_after_begin' is served to a replica with
                             replica_parallel_workers 0 / 1 / 4, with and
                             without auto-position; then the stubs of
                             'lie_after_gtid' and 'lie_after_first_rows',
                             and the latter with a small 'file_size' (the
                             stub is followed by a ROTATE)
  mysql_minimal              no Binlog Server: a second MySQL server whose
                             binlog file is edited while it is down (the
                             events after BEGIN of one transaction are
                             removed, positions and checksums fixed) and a
                             replica of it; variants of the edit, and
                             'mysqlbinlog <file> | mysql'. The edited files
                             are kept in <workdir>/minimal

A JSON line per case ('case', 'ok', 'verdict', and 'why' with the evidence
on failure), then a summary; result.json in the work directory; exit 1 if
any case failed. The default run takes 3 to 5 minutes with a debug build
(--replica-crash adds about 10).
Ports: --port (the source), +1 (the replica), +2 ('pull'), +3 (the proxy),
+4 (the second source of 'mysql_minimal'). Example:

  PYTHONPATH=~/ws/qa/pylib python3 truncated_transaction_test.py \\
    --mysql-basedir ~/ws/dist/mysql-8.4.8-linux-glibc2.28-x86_64-minimal \\
    --binsrv ~/ws/percona-binlog-server-build-debug_gcc14/binlog_server \\
    --workdir ~/ws/qa/truncated --mode gtid --rewrite
"""

import argparse
import json
import shutil
import signal
import socket
import struct
import subprocess
import sys
import threading
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
import rewrite_attack_test as rw  # noqa: E402

QUERY_EVENT = 2
TABLE_MAP_EVENT = 19
ROWS_EVENTS = (23, 24, 25, 30, 31, 32)
EVENT_NAMES = {2: "QUERY", 16: "XID", 19: "TABLE_MAP", 30: "WRITE_ROWS",
               31: "UPDATE_ROWS", 32: "DELETE_ROWS", 33: "GTID",
               34: "ANONYMOUS_GTID", 40: "TRANSACTION_PAYLOAD"}
ARTIFICIAL_FLAG = 0x20
EOF_PACKET = b"\xfe\x00\x00\x00\x00"
# the target is the second transaction of the proxied dump
TARGET_INDEX = 2
# how long the proxy keeps a stalled connection open at most
STALL_LIMIT = 60
# the read timeout of the faulted run of 'fault_timeout_mid_event'
SHORT_READ_TIMEOUT = 3
# how long an attacked 'pull' gets to store what it will store
PULL_SETTLE_SECONDS = 6
CATCH_UP_SECONDS = 25
STABLE_SECONDS = 4
STOP_REPLICA_SECONDS = 20

TARGET_SQL = {
    "dml": "INSERT INTO qa.t (payload) VALUES (REPEAT('T', 3000)), "
           "(REPEAT('U', 3000)), (REPEAT('V', 3000))",
    "multi": "BEGIN; INSERT INTO qa.t (payload) VALUES ('MULTI-FIRST'); "
             "UPDATE qa.t SET payload = 'MULTI-SECOND' WHERE id = 1; "
             "INSERT INTO qa.t (payload) VALUES ('MULTI-THIRD'); COMMIT",
    "ddl": "CREATE TABLE qa.cut_ddl (x INT)",
    "compressed": "SET SESSION binlog_transaction_compression = ON; "
                  "INSERT INTO qa.t (payload) VALUES (REPEAT('C', 3000)), "
                  "(REPEAT('D', 3000))",
}

# set by main()
GTID_MODE = False
REWRITE = False
FILE_SIZE = "1M"
REPLICA_ALL = False
WORKDIR: Path = None
EDITED_SOURCE: harness.MySQLServer = None


def event_name(code: int) -> str:
    return EVENT_NAMES.get(code, f"event {code}")


# ---------------------------------------------------------------------------
# the man in the middle
# ---------------------------------------------------------------------------

def cut_index(codes: list, where: str) -> int:
    """How many events of a transaction lie before the cut 'where'."""
    if where == "after_gtid":
        return 1
    if where == "after_begin" and len(codes) > 2 and codes[1] == QUERY_EVENT:
        return 2
    if where == "after_table_map" and TABLE_MAP_EVENT in codes:
        return codes.index(TABLE_MAP_EVENT) + 1
    rows = [index for index, code in enumerate(codes) if code in ROWS_EVENTS]
    if where == "after_first_rows" and len(rows) > 1:
        return rows[0] + 1
    maps = [index for index, code in enumerate(codes)
            if code == TABLE_MAP_EVENT]
    if where == "after_statement" and len(maps) > 1:
        return maps[1]
    raise harness.Failure(f"no cut '{where}' in a transaction of "
                          f"{[event_name(code) for code in codes]}")


def set_length(event: bytearray, value: int) -> None:
    """Writes 'transaction_length' in the width the field has."""
    position, width, _ = rw.length_field(event)
    if width == 1:
        if not 0 <= value < 251:
            raise harness.Failure(f"{value} does not fit the 1-byte length")
        event[position] = value
    else:
        event[position + 1:position + width] = value.to_bytes(width - 1,
                                                              "little")


def finished(event: bytearray, size: "int | None" = None) -> bytes:
    """'event' (without its footer) as a packet payload, with its size
    field ('size' if given) and a fresh CRC32."""
    struct.pack_into("<I", event, 9, len(event) + 4 if size is None else size)
    return b"\x00" + bytes(event) + struct.pack("<I", zlib.crc32(bytes(event)))


class CutProxy(hostile.RenamingProxy):
    """The proxy of hostile_source_test.py, per connection: the packets of
    the binlog dump pass unchanged except for transaction number 'target',
    which is held back until its last event (the honest 'transaction_length'
    says which one that is) and then sent as 'plan' says:
      lie     the new transaction_length: a cut_index() name, "minus1",
              "plus1000" or a number
      inject  what is done to the first ROWS event
      send    a cut_index() name: only the events before it are sent
      then    after them: "eof" (an EOF packet, then close), "drop" (close),
              "half_drop" / "half_stall" (half of the next event first),
              "stall" (keep the connection open and silent until 'release'),
              "skip" (go on with the next transaction)
    After the target every connection passes unchanged."""

    def __init__(self, listen_port: int, source_port: int, plan: dict,
                 target: int = TARGET_INDEX):
        self.plan = plan
        self.target = target
        self.seen = 0
        self.fired = False
        self.error = ""
        self.note = {}
        self.format_description = None
        self.reached = threading.Event()
        self.release = threading.Event()
        super().__init__(listen_port, source_port, b"", b"", "cut")

    def close(self):
        self.release.set()
        super().close()

    def _accept(self):
        while True:
            try:
                client, _ = self.listener.accept()
            except OSError:
                return
            try:
                server = socket.create_connection(("127.0.0.1",
                                                   self.source_port))
            except OSError:
                client.close()
                continue
            dumping = threading.Event()
            threading.Thread(target=self._upstream,
                             args=(client, server, dumping),
                             daemon=True).start()
            threading.Thread(target=self._downstream,
                             args=(server, client, dumping),
                             daemon=True).start()

    @staticmethod
    def _hang_up(*sockets):
        for sock in sockets:
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass

    def _upstream(self, client, server, dumping):
        try:
            while True:
                header = self._read_exactly(client, 4)
                if header is None:
                    break
                length = header[0] | header[1] << 8 | header[2] << 16
                payload = self._read_exactly(client, length) if length else b""
                if payload is None:
                    break
                if header[3] == 0 and payload and payload[0] in (
                        hostile.COM_BINLOG_DUMP, hostile.COM_BINLOG_DUMP_GTID):
                    dumping.set()
                server.sendall(header + payload)
        except OSError:
            pass
        finally:
            self._hang_up(client, server)

    def _downstream(self, server, client, dumping):
        sequence = 0
        pending = None
        remaining = 0
        continuation = False

        def send(payload: bytes, part: "int | None" = None) -> None:
            nonlocal sequence
            sequence = (sequence + 1) & 0xFF
            client.sendall(struct.pack("<I", len(payload))[:3] +
                           bytes([sequence]) + payload[:part])

        try:
            while True:
                header = self._read_exactly(server, 4)
                if header is None:
                    break
                length = header[0] | header[1] << 8 | header[2] << 16
                payload = self._read_exactly(server, length) if length else b""
                if payload is None:
                    break
                if not dumping.is_set():
                    client.sendall(header + payload)
                    continue
                if length == 0xFFFFFF or continuation or self.fired or \
                        len(payload) < 1 + harness.COMMON_HEADER_SIZE or \
                        payload[0] != 0:
                    continuation = length == 0xFFFFFF
                    for held in pending or []:
                        send(held)
                    pending = None
                    send(payload)
                    continue
                code = payload[5]
                if code == harness.FORMAT_DESCRIPTION_EVENT:
                    self.format_description = payload
                if pending is None:
                    if code not in harness.TRANSACTION_START_EVENTS:
                        send(payload)
                        continue
                    pending = [payload]
                    remaining = rw.length_field(payload[1:])[2]
                else:
                    pending.append(payload)
                remaining -= len(payload) - 1
                if remaining > 0:
                    continue
                transaction, pending = pending, None
                self.seen += 1
                if self.seen != self.target:
                    for held in transaction:
                        send(held)
                    continue
                self.fired = True
                try:
                    out, count = self._prepare(transaction)
                except (harness.Failure, struct.error, IndexError) as error:
                    self.error = str(error)
                    out, count = transaction, len(transaction)
                for held in out[:count]:
                    send(held)
                then = self.plan.get("then") if not self.error else None
                if then == "eof":
                    send(EOF_PACKET)
                if then in ("half_drop", "half_stall"):
                    send(out[count], len(out[count]) // 2)
                self.reached.set()
                if then in ("stall", "half_stall"):
                    self.release.wait(STALL_LIMIT)
                if then not in (None, "skip"):
                    break
        except OSError:
            pass
        finally:
            self.reached.set()
            self._hang_up(client, server)

    def _prepare(self, transaction: list) -> tuple:
        """The packets of the target as the plan wants them, and how many
        of them to send."""
        plan = self.plan
        codes = [payload[5] for payload in transaction]
        true_length = sum(len(payload) - 1 for payload in transaction)
        self.note = {"events": [event_name(code) for code in codes],
                     "sizes": [len(payload) - 1 for payload in transaction],
                     "true_length": true_length}
        out = list(transaction)
        lie = plan.get("lie")
        if lie is not None:
            if lie == "minus1":
                value = true_length - 1
            elif lie == "plus1000":
                value = true_length + 1000
            elif isinstance(lie, int):
                value = lie
            else:
                value = sum(len(payload) - 1 for payload in
                            transaction[:cut_index(codes, lie)])
            event = bytearray(transaction[0][1:-4])
            set_length(event, value)
            out[0] = finished(event)
            self.note["sent_length"] = value
            self.rewritten += 1
        inject = plan.get("inject")
        if inject is not None:
            index = cut_index(codes, "after_table_map")
            payload = transaction[index]
            event = bytearray(payload[1:-4])
            self.note["damaged"] = f"{event_name(codes[index])} ({inject})"
            self.rewritten += 1
            if inject == "bad_checksum":
                event[-1] ^= 0x01
                out[index] = b"\x00" + bytes(event) + payload[-4:]
            elif inject == "size_lie":
                out[index] = finished(event, len(event) + 4 + 8)
            elif inject == "second_format_description":
                if self.format_description is None:
                    raise harness.Failure("no FORMAT_DESCRIPTION seen")
                out.insert(index, self.format_description)
            elif inject == "artificial_flag":
                flags = struct.unpack_from("<H", event, 17)[0]
                struct.pack_into("<H", event, 17, flags | ARTIFICIAL_FLAG)
                out[index] = finished(event)
            elif inject == "bad_prefix":
                out[index] = b"\x01" + payload[1:]
            elif inject in ("wrong_position", "zero_position"):
                position = struct.unpack_from("<I", event, 13)[0]
                struct.pack_into("<I", event, 13, 0 if inject.startswith(
                    "zero") else position + 1)
                out[index] = finished(event)
            else:
                raise harness.Failure(f"unknown damage '{inject}'")
        count = len(out)
        if plan.get("send") is not None:
            count = cut_index(codes, plan["send"])
            self.note["events_sent"] = count
        return out, count


# ---------------------------------------------------------------------------
# the source's and the storage's transactions
# ---------------------------------------------------------------------------

def read_transactions(files: dict, order: list) -> list:
    """The transactions of binlog files, in order: a key (the GTID, or
    'file:offset' for an anonymous one), the event codes and sizes, and a
    digest of every event after the first that leaves out what a rewrite
    changes (the end position and the checksum)."""
    transactions = []
    for name in order:
        data = files.get(name, b"")
        try:
            events = harness.parse_events(data)
        except harness.Failure:
            continue
        current = None
        for offset, code, size in events:
            if code in harness.TRANSACTION_START_EVENTS:
                event = data[offset:offset + size]
                gtid = None
                if code == harness.GTID_LOG_EVENT:
                    gtid = (str(uuid_module.UUID(bytes=bytes(event[20:36])))
                            + ":" + str(struct.unpack_from("<q", event,
                                                           36)[0]))
                current = {"key": gtid or f"{name}:{offset}", "gtid": gtid,
                           "file": name, "offset": offset,
                           "declared": rw.length_field(event)[2],
                           "codes": [code], "sizes": [size], "digests": []}
                transactions.append(current)
            elif code in rw.CONTROL_EVENTS:
                current = None
            elif current is not None:
                current["codes"].append(code)
                current["sizes"].append(size)
                current["digests"].append(zlib.crc32(
                    data[offset:offset + 13] +
                    data[offset + 17:offset + size - 4]))
    return transactions


def source_transactions() -> list:
    server = harness.SERVER
    names = server.binlog_names()
    return read_transactions({name: server.binlog_path(name).read_bytes()
                              for name in names}, names)


def stored_transactions() -> tuple:
    """The storage's transactions and what is wrong with its files."""
    binsrv = harness.BINSRV
    names = binsrv.indexed_binlogs()
    files = {name: (binsrv.storage / name).read_bytes() for name in names
             if (binsrv.storage / name).is_file()}
    problems = []
    for name, data in files.items():
        try:
            events = harness.parse_events(data)
        except harness.Failure:
            if data:
                problems.append(f"{name} does not start with the binlog "
                                "magic")
            continue
        end = events[-1][0] + events[-1][2] if events else 4
        if end != len(data):
            problems.append(f"{name}: {len(data) - end} bytes of a partial "
                            f"event after {end}")
    return read_transactions(files, names), problems


def describe(transaction: dict) -> str:
    return (f"{'+'.join(event_name(code) for code in transaction['codes'])} "
            f"({len(transaction['codes'])} event(s), "
            f"{sum(transaction['sizes'])} bytes, declares "
            f"{transaction['declared']}) at {transaction['file']}:"
            f"{transaction['offset']}")


def compare(source: list, stored: list) -> dict:
    """The state of every source transaction in the storage: 'complete',
    'absent', 'stub' or 'altered'; and what the storage holds besides."""
    by_key = {}
    for transaction in stored:
        by_key.setdefault(transaction["key"], []).append(transaction)
    states = {}
    for transaction in source:
        copies = by_key.get(transaction["key"], [])
        if not copies:
            states[transaction["key"]] = "absent"
            continue
        copy = copies[0]
        if copy["digests"] == transaction["digests"]:
            states[transaction["key"]] = "complete"
        elif copy["digests"] == \
                transaction["digests"][:len(copy["digests"])]:
            states[transaction["key"]] = "stub"
        else:
            states[transaction["key"]] = "altered"
    known = {transaction["key"] for transaction in source}
    return {"states": states,
            "twice": sorted(key for key, copies in by_key.items()
                            if len(copies) > 1),
            "unknown": sorted(key for key in by_key if key not in known)}


def storage_facts(target: dict) -> dict:
    """What the storage says about the target right now."""
    binsrv = harness.BINSRV
    stored, problems = stored_transactions()
    comparison = compare([target], stored)
    state = comparison["states"][target["key"]]
    facts = {"target": state}
    copies = [transaction for transaction in stored
              if transaction["key"] == target["key"]]
    if copies:
        facts["target_stored_as"] = describe(copies[0])
    if problems:
        facts["file_problems"] = problems
    names = binsrv.indexed_binlogs()
    if names:
        last = names[-1]
        facts["last_binlog"] = last
        path = binsrv.storage / last
        facts["last_binlog_bytes"] = path.stat().st_size if path.is_file() \
            else None
        try:
            metadata = json.loads((binsrv.storage / (last + ".json")
                                   ).read_text())
            facts["last_binlog_metadata"] = {
                key: (value if len(str(value)) < 200 else str(value)[:200])
                for key, value in metadata.items()}
        except (OSError, ValueError) as error:
            facts["last_binlog_metadata"] = f"unreadable: {error}"
    return facts


def gtid_claims(config: Path, target: dict) -> dict:
    """What 'list' and 'search_by_gtid_set' say about the target's GTID."""
    listing = rw.query(config, "list")
    claims = {"list_exit": listing["exit"],
              "list": [f"{entry.get('name')}:{entry.get('size')}"
                       for entry in listing["result"]
                       if isinstance(entry, dict)][-2:]}
    if target["gtid"] is None:
        return claims
    sid, _, number = target["gtid"].rpartition(":")
    listed = {}
    for entry in listing["result"]:
        if isinstance(entry, dict):
            listed = rw.union(listed, rw.parse_gtid_set(
                str(entry.get("added_gtids") or "")))
    claims["list_names_the_gtid"] = int(number) in listed.get(sid, set())
    search = rw.query(config, "search_by_gtid_set", target["gtid"])
    claims["search_by_gtid_set"] = search["status"] or search["message"]
    claims["search_names"] = [str(entry.get("name")) for entry in
                              search["result"] if isinstance(entry, dict)]
    # 'search_by_gtid_set <set>' answers with the binlogs a replica that
    # lacks <set> needs: "success" means the storage claims to hold it
    claims["gtid_claimed"] = (claims["list_names_the_gtid"] or
                              search["status"] == "success")
    return claims


# ---------------------------------------------------------------------------
# configuration and runs
# ---------------------------------------------------------------------------

def make_config(name: str = "binsrv-direct.json", proxied: bool = False,
                read_timeout: "int | None" = None) -> Path:
    binsrv = harness.BINSRV
    binsrv.write_config()
    config = json.loads(binsrv.config_path.read_text())
    if REWRITE:
        config["replication"]["rewrite"] = {"base_file_name": rw.BASE,
                                            "file_size": FILE_SIZE}
    if proxied:
        config["connection"]["port"] = rw.PROXY_PORT
        config["connection"]["ssl"] = {"mode": "disabled"}
    if read_timeout is not None:
        config["connection"]["read_timeout"] = read_timeout
    # used by 'pull' only
    config["replication_source"] = {
        "port": rw.PULL_PORT, "server_id": 142,
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


def log_lines(limit: int = 3) -> list:
    """The end of the Binlog Server's log, without the event dump."""
    binsrv = harness.BINSRV
    if not binsrv.log_path.exists():
        return []
    lines = [line.split("] ", 1)[-1].strip()[:220] for line in
             binsrv.log_path.read_text(errors="replace").splitlines()]
    return [line for line in lines if line][-limit:]


def last_error() -> str:
    """The last error the Binlog Server logged, apart from those of its
    listener's sessions (the tests' own port probes end in one)."""
    binsrv = harness.BINSRV
    if not binsrv.log_path.exists():
        return ""
    errors = [line.split("] ", 2)[-1].strip()[:300] for line in
              binsrv.log_path.read_text(errors="replace").splitlines()
              if ("[  error]" in line or "exception caught" in line) and
              "net    :" not in line]
    return errors[-1] if errors else ""


def act(action: str, process: subprocess.Popen, proxy: CutProxy) -> None:
    """The failure of a fault_* case, once the proxy has stalled."""
    if action == "sigterm":
        process.send_signal(signal.SIGTERM)
    elif action == "sigkill":
        process.kill()
    elif action == "kill_source":
        harness.SERVER.process.kill()
        harness.SERVER.process.wait()
        harness.SERVER.process = None
    if action != "timeout":
        # a signal is only noticed once the blocked read returns (ticket 43)
        time.sleep(1)
        proxy.release.set()


def attacked_fetch(spec: dict, config: Path, label: str,
                   proxy: CutProxy) -> dict:
    action = spec.get("action")
    if action is None:
        return rw.brief(rw.command("fetch", config, label))
    started = time.monotonic()
    process = subprocess.Popen(
        [str(harness.BINSRV.binary), "fetch", str(config)],
        stdout=subprocess.DEVNULL, stderr=subprocess.PIPE,
        env=tamper.binsrv_environment())
    result = {}
    if not proxy.reached.wait(30):
        result["proxy"] = "the dump never reached the target"
    # what was sent is read by now
    time.sleep(0.5)
    act(action, process, proxy)
    try:
        _, stderr = process.communicate(timeout=60)
    except subprocess.TimeoutExpired:
        process.kill()
        _, stderr = process.communicate()
        result["timed_out_after_seconds"] = 60
    proxy.release.set()
    text = stderr.decode(errors="replace")
    result.update(exit=process.returncode, peak_rss_mb=0,
                  seconds=round(time.monotonic() - started, 2),
                  message=last_error() or text.strip()[-200:])
    sanitizer = rw.sanitizer_markers(text)
    if sanitizer:
        result["sanitizer"] = sanitizer
        result["report_file"] = text[-600:]
    return result


def attacked_pull(spec: dict, config: Path, label: str, proxy: CutProxy,
                  wanted: int) -> dict:
    """A 'pull' through the proxy: runs until it exits by itself, or until
    the storage holds 'wanted' transactions or stands still, and is then
    stopped."""
    action = spec.get("action")
    started = time.monotonic()
    session = rw.PullSession(config, label)
    result = {"listening": session.listening}
    if not proxy.reached.wait(30):
        result["proxy"] = "the dump never reached the target"
    if action is not None:
        time.sleep(0.5)
        act(action, session.process, proxy)
    deadline = time.monotonic() + PULL_SETTLE_SECONDS + (
        SHORT_READ_TIMEOUT + 2 if action == "timeout" else 0)
    while time.monotonic() < deadline and session.process.poll() is None:
        if len(stored_transactions()[0]) >= wanted:
            break
        time.sleep(0.25)
    proxy.release.set()
    result["log_end"] = log_lines()
    message = last_error()
    ended = session.close()
    result.update(
        exit=ended["exit"], seconds=round(time.monotonic() - started, 2),
        peak_rss_mb=0, message=message)
    if ended["exit"] is None:
        result["exit"] = "still running (stopped by the test)"
    if action == "sigkill" and ended["exit"] == -signal.SIGKILL:
        result["exit"] = "killed by the test"
    if ended["sanitizer"]:
        result["sanitizer"] = ended["sanitizer"]
        result["report_file"] = ended.get("report_file")
    return result


# ---------------------------------------------------------------------------
# a replica
# ---------------------------------------------------------------------------

def quick_sql(server, statement: str, timeout: float = 5) -> str:
    """server.sql() that gives up: a server that is aborting accepts
    connections and answers nothing."""
    try:
        completed = subprocess.run(
            [str(server.basedir / "bin" / "mysql"), "--no-defaults",
             f"--socket={server.socket}", "-uroot", "-N", "-B", "-e",
             statement], capture_output=True, text=True, timeout=timeout,
            check=False)
    except subprocess.TimeoutExpired:
        raise RuntimeError(f"no answer in {timeout} s: {statement[:60]}") \
            from None
    if completed.returncode != 0:
        raise RuntimeError(f"SQL failed: {completed.stderr.strip()}")
    return completed.stdout


def aborting(server) -> bool:
    try:
        return "mysqld got signal" in server.error_log.read_text(
            errors="replace")
    except OSError:
        return False


def observe_replica(setup, source_rows: str, label: str,
                    wanted: "dict | None" = None) -> dict:
    """Starts replication with 'setup()' and watches the replica until it
    has the source's rows (and the GTIDs 'wanted'), stands still, reports
    an error or dies. A replica that died is set up again from scratch."""
    replica = rw.REPLICA
    started = time.monotonic()
    result = {"source_rows": source_rows}
    marks = ("MY-010443", "MY-010590", "MY-013122", "MY-010584",
             "terminate called", "what():", "got signal", "[ERROR]")
    try:
        logged_before = len(replica.error_log.read_text(
            errors="replace").splitlines())
    except OSError:
        logged_before = 0

    def new_log_lines() -> list:
        log = replica.error_log.read_text(errors="replace").splitlines()
        return [line[:260] for line in log[logged_before:]
                if any(mark in line for mark in marks)][-6:]

    def digest() -> str:
        try:
            return quick_sql(
                replica, "SELECT COUNT(*), COALESCE(SUM(CRC32(CONCAT(id, "
                "':', payload))), 0) FROM qa.t").strip().replace(
                    "\t", " rows, crc sum ")
        except RuntimeError as error:
            if aborting(replica) or replica.process.poll() is not None:
                raise
            return f"error: {str(error).splitlines()[0][:100]}"

    try:
        setup()
        last = None
        stable_since = started
        while time.monotonic() - started < CATCH_UP_SECONDS:
            if aborting(replica):
                raise RuntimeError("the error log reports a signal")
            executed = quick_sql(
                replica, "SELECT @@GLOBAL.gtid_executed").replace(
                    "\\n", "").strip()
            rows = digest()
            now = time.monotonic()
            if rows == source_rows and (wanted is None or not rw.subtract(
                    wanted, rw.parse_gtid_set(executed))):
                break
            if (executed, rows) != last:
                last, stable_since = (executed, rows), now
            elif now - stable_since > STABLE_SECONDS and now - started > 6:
                break
            time.sleep(0.25)
        if aborting(replica):
            raise RuntimeError("the error log reports a signal")
        state = replica_tamper.replica_state(replica)
        result.update(
            gtid_executed=rw.short(quick_sql(
                replica, "SELECT @@GLOBAL.gtid_executed").replace(
                    "\\n", "").strip()),
            rows=digest(),
            errors=[" ".join(fields)[:300]
                    for fields in (state["receiver"], state["applier"])
                    if len(fields) > 1 and fields[1] not in ("0", "")])
        if new_log_lines():
            result["error_log"] = new_log_lines()
        try:
            quick_sql(replica, "STOP REPLICA", STOP_REPLICA_SECONDS)
        except RuntimeError:
            if aborting(replica) or replica.process.poll() is not None:
                raise
            # the applier waits for the rest of a transaction that never
            # comes; a shutdown would wait for it as well
            result["hangs"] = ("STOP REPLICA did not return in "
                               f"{STOP_REPLICA_SECONDS} s (the test killed "
                               "the server)")
            replica.process.kill()
            replica.process.wait()
            replica.process = None
            shutil.rmtree(replica.datadir, ignore_errors=True)
            replica.error_log.unlink(missing_ok=True)
            replica.start()
    except RuntimeError as error:
        if replica.process is None or not (
                aborting(replica) or replica.process.poll() is not None):
            raise
        # a server that aborts can take half a minute to go
        result["died_after_seconds"] = round(time.monotonic() - started, 1)
        try:
            code = replica.process.wait(timeout=3)
            died = f"mysqld exited with {code}" + (
                f" (signal {signal.Signals(-code).name})" if code < 0 else "")
        except subprocess.TimeoutExpired:
            replica.process.kill()
            replica.process.wait()
            died = "mysqld aborted (the test killed what was left of it)"
        log = replica.error_log.read_text(errors="replace").splitlines()
        kept = WORKDIR / f"replica-crash-{label}.err"
        kept.write_text("\n".join(log[-150:]) + "\n")
        result.update(
            died=died, error_log=new_log_lines(), error_log_file=str(kept),
            sql_error=str(error).splitlines()[0][:160])
        replica.process = None
        shutil.rmtree(replica.datadir, ignore_errors=True)
        replica.error_log.unlink(missing_ok=True)
        replica.start()
    result["seconds"] = round(time.monotonic() - started, 1)
    return result


def first_stored_binlog() -> str:
    names = harness.BINSRV.indexed_binlogs()
    return names[0] if names else "binlog.000001"


def replica_of_pull(config: Path, label: str, auto_position: bool = True,
                    workers: "int | None" = None) -> dict:
    """A replica of an honest 'pull' on the storage as it is."""
    replica = rw.REPLICA
    start = ("SOURCE_AUTO_POSITION=1" if auto_position and GTID_MODE else
             f"SOURCE_LOG_FILE='{first_stored_binlog()}', SOURCE_LOG_POS=4")

    in_effect = {}

    def setup():
        if workers is not None:
            replica.sql("STOP REPLICA; SET GLOBAL replica_parallel_workers "
                        f"= {workers}")
        in_effect["workers"] = replica.sql(
            "SELECT @@GLOBAL.replica_parallel_workers").strip()
        replication.replicate_from_pull(replica, rw.PULL_PORT, GTID_MODE,
                                        start)

    source_rows = rw.rows_digest(harness.SERVER)
    session = rw.PullSession(config, label)
    try:
        time.sleep(1)
        if not session.listening or session.process.poll() is not None:
            return {"pull": "does not serve the storage: " + last_error()}
        result = observe_replica(
            setup, source_rows, label,
            rw.parse_gtid_set(rw.source_executed()) if GTID_MODE else None)
    finally:
        ended = session.close()
    result["replica_parallel_workers"] = in_effect.get("workers")
    if ended["exit"] is not None:
        result["pull_ended"] = (f"exit {ended['exit']}: "
                                f"{ended.get('error', '')[-200:]}")
    if ended["sanitizer"]:
        result["pull_sanitizer"] = ended["sanitizer"]
    return result


def replica_problems(replica: dict, complete: bool) -> list:
    """What a replica run adds to the failures of a case; 'complete' says
    whether the storage claims to hold all of the source."""
    problems = []
    if replica.get("died"):
        problems.append(
            "the replica server itself died while replicating from 'pull': "
            f"{replica['died']}: {' | '.join(replica['error_log'])[-500:]}")
    elif replica.get("hangs"):
        problems.append("the replica of 'pull' hangs: " + replica["hangs"])
    elif replica.get("errors"):
        problems.append("the replica of 'pull' stopped with an error: " +
                        replica["errors"][0])
    elif complete and "rows" in replica and \
            replica["rows"] != replica["source_rows"]:
        problems.append(
            f"the replica of 'pull' ends with other rows than the source: "
            f"{replica['rows']} against {replica['source_rows']}")
    if replica.get("pull_sanitizer"):
        problems.append("'pull' printed a sanitizer report while serving")
    return problems


# ---------------------------------------------------------------------------
# a case
# ---------------------------------------------------------------------------

def scenario(name: str, spec: dict) -> tuple:
    """Runs steps 1-6 of the module's description. Returns the result line
    and the direct configuration (the storage is left as the case ends)."""
    binsrv = harness.BINSRV
    server = harness.SERVER
    operation = spec.get("op", "fetch")
    shape = spec.get("shape", "dml")
    server.reset()
    binsrv.reset()
    if operation == "pull":
        # a running 'pull' writes to the storage at checkpoints only
        binsrv.checkpoint_size = "1"
    direct = make_config()
    proxied = make_config(
        "binsrv-proxied.json", proxied=True,
        read_timeout=SHORT_READ_TIMEOUT if spec.get("action") == "timeout"
        else None)
    result = {"case": name, "operation": operation}
    problems = []
    rw.insert_many("BEFORE", 2, 2)
    rw.must_fetch(direct, name + "-first")
    rw.insert_many("PRE", 1, 2)
    known = {transaction["key"] for transaction in source_transactions()}
    server.sql(TARGET_SQL[shape])
    new = [transaction for transaction in source_transactions()
           if transaction["key"] not in known]
    if len(new) != 1:
        raise harness.Failure(f"the target is {len(new)} transactions")
    target = new[0]
    result["target"] = describe(target)
    if target["gtid"]:
        result["target_gtid"] = target["gtid"]
    if not spec.get("last"):
        rw.insert_many("POST", 2, 2)
    wanted = len(source_transactions())

    hostile.cache_password_hash(rw.MYSQL_BASEDIR, server.port)
    plan = {key: spec[key] for key in ("lie", "inject", "send", "then")
            if key in spec}
    proxy = CutProxy(rw.PROXY_PORT, server.port, plan)
    try:
        if operation == "fetch":
            attacked = attacked_fetch(spec, proxied, name + "-attacked",
                                      proxy)
        else:
            attacked = attacked_pull(spec, proxied, name + "-attacked",
                                     proxy, wanted)
    finally:
        proxy.close()
    result["attacked_run"] = attacked
    if "log_end" not in attacked:
        attacked["log_end"] = log_lines(2)
    result["proxy"] = proxy.note
    if proxy.error:
        problems.append("the proxy could not do its part: " + proxy.error)
    elif not proxy.fired:
        problems.append("the dump never reached the target: the case did "
                        "not run")
    if spec.get("action") == "kill_source":
        server.start()
        result["source_restarted"] = True

    after_attack = storage_facts(target)
    after_attack.update(gtid_claims(direct, target))
    result["after_attacked_run"] = after_attack

    rw.insert_many("AFTER", 1, 2)
    result["honest_fetch"] = rw.brief(rw.command("fetch", direct,
                                                 name + "-honest"))
    honest = result["honest_fetch"]
    if honest["exit"] != 0:
        result["honest_fetch_again"] = rw.brief(
            rw.command("fetch", direct, name + "-honest2"))
        honest = result["honest_fetch_again"]
    problems += rw.violations(
        result, *(["attacked_run"] if spec.get("action") != "sigkill" and
                  isinstance(attacked["exit"], int) else []),
        "honest_fetch", "honest_fetch_again")

    source = source_transactions()
    stored, file_problems = stored_transactions()
    comparison = compare(source, stored)
    states = comparison["states"]
    final = storage_facts(target)
    final.update(gtid_claims(direct, target))
    result["at_the_end"] = final
    lacking = [key for key, state in states.items() if state != "complete"]
    result["source_transactions"] = len(source)
    result["not_stored_in_full"] = {key: states[key] for key in lacking[:6]}
    problems += file_problems
    if comparison["twice"]:
        problems.append("stored twice: " + ", ".join(comparison["twice"][:3]))
    if comparison["unknown"]:
        problems.append("the storage holds transactions the source does "
                        "not: " + ", ".join(comparison["unknown"][:3]))
    if "altered" in states.values():
        problems.append("stored transactions differ from the source's: " +
                        ", ".join(key for key in lacking
                                  if states[key] == "altered")[:200])

    stub_first = after_attack["target"] == "stub"
    stub_last = final["target"] == "stub"
    if stub_first or stub_last:
        where = after_attack if stub_first else final
        problems.append(
            f"the cut transaction is stored as "
            f"{where.get('target_stored_as')} of {result['target']}" + (
                "; its GTID is recorded ('list' names it: "
                f"{where.get('list_names_the_gtid')}, search_by_gtid_set: "
                f"{where.get('search_by_gtid_set')})"
                if where.get("gtid_claimed") else ""))
    if lacking and honest["exit"] == 0:
        verdict = "silent loss"
        problems.append(
            f"the honest fetch exited 0, but {len(lacking)} of the source's "
            f"{len(source)} transactions are not stored in full: "
            f"{json.dumps(result['not_stored_in_full'])[:300]}")
    elif lacking:
        verdict = "stuck but visible"
        problems.append(
            f"the honest fetch cannot continue (exit {honest['exit']}: "
            f"{honest['message'][-160:]}); {len(lacking)} transaction(s) "
            "are not stored")
    elif stub_first:
        verdict = "stub stored, repaired by the honest run"
    elif not plan:
        verdict = "nothing changed"
    elif attacked["exit"] == 0 and after_attack["target"] == "complete":
        verdict = "accepted, stored in full"
    else:
        verdict = "refused cleanly" if attacked["exit"] == 1 else \
            "interrupted cleanly"
    result["verdict"] = verdict

    if spec.get("replica") or (REPLICA_ALL and (stub_first or stub_last)):
        replica = replica_of_pull(direct, name)
        result["replica"] = replica
        problems += replica_problems(replica, not lacking)
    return rw.finish(result, problems), direct


def run_scenario(name: str) -> dict:
    return scenario(name, SCENARIOS[name])[0]


def lie(where, **extra) -> dict:
    return dict({"lie": where}, **extra)


def fault(**extra) -> dict:
    return dict({"send": "after_first_rows", "then": "drop"}, **extra)


SCENARIOS = {
    "control": {"replica": True},
    "lie_after_gtid": lie("after_gtid", replica=True),
    "lie_after_begin": lie("after_begin", replica=True),
    "lie_after_table_map": lie("after_table_map"),
    "lie_after_first_rows": lie("after_first_rows", replica=True),
    "lie_end_minus_1": lie("minus1"),
    "lie_zero": lie(0),
    "lie_one": lie(1),
    "lie_larger": lie("plus1000"),
    "lie_multi_after_statement": lie("after_statement", shape="multi"),
    "lie_ddl_after_gtid": lie("after_gtid", shape="ddl"),
    "lie_compressed_after_gtid": lie("after_gtid", shape="compressed"),
    "lie_last_after_begin": lie("after_begin", last=True),
    "lie_after_begin_eof": lie("after_begin", send="after_begin",
                               then="eof"),
    "lie_after_gtid_eof": lie("after_gtid", send="after_gtid", then="eof"),
    "lie_after_begin_drop": lie("after_begin", send="after_begin",
                                then="drop"),
    "lie_after_begin_rest_removed": lie("after_begin", send="after_begin",
                                        then="skip"),
    "pull_control": {"op": "pull"},
    "pull_lie_after_begin": lie("after_begin", op="pull"),
    "pull_lie_after_begin_drop": lie("after_begin", op="pull",
                                     send="after_begin", then="drop",
                                     replica=True),
    "fault_drop_after_begin": fault(send="after_begin"),
    "fault_drop_after_first_rows": fault(),
    "fault_drop_mid_event": fault(then="half_drop"),
    "fault_timeout_mid_event": fault(then="half_stall", action="timeout"),
    "fault_sigterm": fault(then="stall", action="sigterm"),
    "fault_sigkill": fault(then="stall", action="sigkill"),
    "fault_source_killed": fault(then="stall", action="kill_source"),
    "pull_fault_drop": fault(op="pull"),
    "pull_fault_timeout": fault(op="pull", then="half_stall",
                                action="timeout"),
    "pull_fault_sigterm": fault(op="pull", then="stall", action="sigterm"),
    "pull_fault_sigkill": fault(op="pull", then="stall", action="sigkill"),
}
SCENARIOS.update({f"throw_{damage}": {"inject": damage} for damage in (
    "bad_checksum", "size_lie", "second_format_description",
    "artificial_flag", "bad_prefix", "wrong_position", "zero_position")})


# ---------------------------------------------------------------------------
# the replica's side (optional)
# ---------------------------------------------------------------------------

def replica_outcome(replica: dict) -> str:
    if replica.get("died"):
        return replica["died"]
    if replica.get("pull"):
        return "pull " + replica["pull"]
    if replica.get("hangs"):
        return (f"{replica.get('rows')} against {replica.get('source_rows')}"
                f", no error; {replica['hangs']}")
    if replica.get("errors"):
        return "stopped: " + replica["errors"][0][:200]
    if replica.get("rows") == replica.get("source_rows"):
        return "has the source's rows"
    return (f"waits without an error: {replica.get('rows')} against "
            f"{replica.get('source_rows')}")


def run_replica_matrix(name: str) -> dict:
    """The stub of the rewrite mode, served to replicas."""
    global FILE_SIZE  # pylint: disable=global-statement
    if not (GTID_MODE and REWRITE):
        return {"case": name, "ok": True,
                "skipped": "needs --mode gtid --rewrite: only there does "
                           "'fetch' go on after the stub"}
    replica = rw.REPLICA
    result = {"case": name,
              "replica_version": replica.sql("SELECT VERSION()").strip(),
              "runs": []}
    problems = []

    def serve(label: str, built: dict, config: Path, auto_position: bool,
              workers: "int | None") -> None:
        observed = replica_of_pull(config, f"{name}-{label}", auto_position,
                                   workers)
        run = {"storage": label, "auto_position": auto_position,
               "replica_parallel_workers":
                   observed.get("replica_parallel_workers", workers),
               "stored_as": built["at_the_end"].get("target_stored_as"),
               "outcome": replica_outcome(observed)}
        run.update({key: observed[key] for key in
                    ("error_log", "error_log_file", "errors", "hangs",
                     "gtid_executed", "rows", "source_rows", "sql_error")
                    if key in observed})
        result["runs"].append(run)
        if run["outcome"] != "has the source's rows":
            problems.append(f"{label}, auto_position {auto_position}, "
                            f"workers {run['replica_parallel_workers']}: "
                            f"{run['outcome']}")

    built, config = scenario(name + "-begin", SCENARIOS["lie_after_begin"])
    built.pop("replica", None)
    for auto_position in (True, False):
        for workers in (4, 1, 0):
            try:
                serve("stub GTID+BEGIN", built, config, auto_position,
                      workers)
            except RuntimeError as error:
                result["runs"].append({
                    "auto_position": auto_position,
                    "replica_parallel_workers": workers,
                    "outcome": "not run: " + str(error).splitlines()[0][:200]})
    for label, case in (("stub GTID", "lie_after_gtid"),
                        ("stub GTID+BEGIN+TABLE_MAP+WRITE_ROWS",
                         "lie_after_first_rows")):
        built, config = scenario(f"{name}-{case}", dict(SCENARIOS[case],
                                                        replica=False))
        serve(label, built, config, True, 4)
    FILE_SIZE = "1K"
    try:
        # the stub fills the file, so the generated ROTATE and the next
        # file's preamble follow it
        built, config = scenario(
            name + "-rotate", dict(SCENARIOS["lie_after_first_rows"],
                                   replica=False))
        serve("stub GTID+BEGIN+TABLE_MAP+WRITE_ROWS, then ROTATE "
              "(file_size 1K)", built, config, True, 4)
    finally:
        FILE_SIZE = "1M"
    replica.sql("STOP REPLICA; SET GLOBAL replica_parallel_workers = 4")
    return rw.finish(result, problems)


def edited_binlog(data: bytes, transaction: int, keep: int,
                  fix_length: bool) -> bytes:
    """The binlog file 'data' with only the first 'keep' events of its
    transaction number 'transaction' (from 0); the end positions and the
    checksums of all events are set again, and with 'fix_length' the
    transaction_length of the cut transaction too."""
    victim = read_transactions({"b": data}, ["b"])[transaction]
    start = victim["offset"]
    kept_end = start + sum(victim["sizes"][:keep])
    cut_end = start + sum(victim["sizes"])
    out = bytearray(data[:4])
    for offset, _, size in harness.parse_events(data):
        if kept_end <= offset < cut_end:
            continue
        event = bytearray(data[offset:offset + size - 4])
        if offset == start and fix_length:
            set_length(event, kept_end - start)
        struct.pack_into("<I", event, 13, len(out) + size)
        out += event + struct.pack("<I", zlib.crc32(bytes(event)))
    return bytes(out)


def outline(data: bytes) -> list:
    """'<GTID number>: <events>' of every transaction of a binlog file."""
    return [transaction["key"].rpartition(":")[2] + ": " +
            "+".join(event_name(code) for code in transaction["codes"])
            for transaction in read_transactions({"b": data}, ["b"])]


MINIMAL_EDITS = {
    # name: (transaction, events kept, transaction_length fixed)
    "unedited": None,
    "begin_then_gtid": (2, 2, True),
    "begin_then_gtid_length_kept": (2, 2, False),
    "gtid_then_gtid": (2, 1, True),
    "rows_then_gtid": (2, 4, True),
    "begin_then_stop": (3, 2, True),
}


def run_mysql_minimal(name: str) -> dict:
    """No Binlog Server: a MySQL source whose binlog was cut by hand."""
    if not GTID_MODE:
        return {"case": name, "ok": True, "skipped": "needs --mode gtid"}
    source = EDITED_SOURCE
    replica = rw.REPLICA
    kept = WORKDIR / "minimal"
    kept.mkdir(parents=True, exist_ok=True)
    template = source.datadir.parent / "template"
    shutil.rmtree(source.datadir, ignore_errors=True)
    shutil.rmtree(template, ignore_errors=True)
    source.start()
    result = {"case": name, "runs": []}
    problems = []
    try:
        result["version"] = source.sql("SELECT VERSION()").strip()
        source.reset()
        source.sql("INSERT INTO qa.t (payload) VALUES ('a'); "
                   "INSERT INTO qa.t (payload) VALUES ('b'); "
                   "INSERT INTO qa.t (payload) VALUES ('c')")
        source.stop()
        shutil.copytree(source.datadir, template)
        original = (template / "binlog.000001").read_bytes()
        (kept / "original.binlog.000001").write_bytes(original)
        result["original_binlog"] = outline(original)

        def start_with(edit: str) -> Path:
            source.stop()
            shutil.rmtree(source.datadir, ignore_errors=True)
            shutil.copytree(template, source.datadir)
            data = original if MINIMAL_EDITS[edit] is None else \
                edited_binlog(original, *MINIMAL_EDITS[edit])
            path = kept / f"{edit}.binlog.000001"
            path.write_bytes(data)
            (source.datadir / "binlog.000001").write_bytes(data)
            source.start()
            return path

        def replicate(start: str):
            def setup():
                replica.sql(
                    "STOP REPLICA; RESET REPLICA ALL; DROP DATABASE IF "
                    "EXISTS qa; CREATE DATABASE qa;")
                replica.sql("RESET BINARY LOGS AND GTIDS"
                            if replica.version() >= (8, 2, 0)
                            else "RESET MASTER")
                replica.sql(
                    "CHANGE REPLICATION SOURCE TO SOURCE_HOST='127.0.0.1', "
                    f"SOURCE_PORT={source.port}, "
                    f"SOURCE_USER='{harness.REPLICATION_USER}', "
                    f"SOURCE_PASSWORD='{harness.REPLICATION_PASSWORD}', "
                    f"SOURCE_SSL=1, SOURCE_CONNECT_RETRY=2, {start}; "
                    "START REPLICA;")
            return setup

        runs = [(edit, True) for edit in MINIMAL_EDITS] + \
            [("begin_then_gtid", False)]
        for edit, auto_position in runs:
            path = start_with(edit)
            start = "SOURCE_AUTO_POSITION=1" if auto_position else \
                "SOURCE_LOG_FILE='binlog.000001', SOURCE_LOG_POS=4"
            observed = observe_replica(
                replicate(start), rw.rows_digest(source), f"{name}-{edit}",
                rw.parse_gtid_set(source.sql(
                    "SELECT @@GLOBAL.gtid_executed")))
            run = {"binlog": edit, "file": str(path),
                   "events": outline(path.read_bytes()),
                   "auto_position": auto_position,
                   "outcome": replica_outcome(observed)}
            run.update({key: observed[key] for key in
                        ("error_log", "error_log_file", "errors", "hangs",
                         "gtid_executed", "rows", "source_rows")
                        if key in observed})
            result["runs"].append(run)
            if run["outcome"] != "has the source's rows":
                problems.append(f"a replica of a MySQL source with the "
                                f"binlog '{edit}' (auto_position "
                                f"{auto_position}): {run['outcome']}")
        source.stop()

        # the same files through 'mysqlbinlog | mysql'
        for edit in ("begin_then_gtid", "rows_then_gtid", "gtid_then_gtid"):
            path = kept / f"{edit}.binlog.000001"
            replica.sql("STOP REPLICA; RESET REPLICA ALL; DROP DATABASE IF "
                        "EXISTS qa; CREATE DATABASE qa;")
            replica.sql("RESET BINARY LOGS AND GTIDS"
                        if replica.version() >= (8, 2, 0) else "RESET MASTER")
            dump = subprocess.run(
                [str(replica.basedir / "bin" / "mysqlbinlog"),
                 "--no-defaults", str(path)], capture_output=True,
                timeout=60, check=False)
            applied = subprocess.run(
                [str(replica.basedir / "bin" / "mysql"), "--no-defaults",
                 f"--socket={replica.socket}", "-uroot"], input=dump.stdout,
                capture_output=True, timeout=60, check=False)
            run = {"binlog": edit, "through": "mysqlbinlog | mysql",
                   "mysqlbinlog_exit": dump.returncode,
                   "mysqlbinlog_stderr": dump.stderr.decode(
                       errors="replace").strip()[-300:],
                   "mysql_exit": applied.returncode,
                   "mysql_stderr": applied.stderr.decode(
                       errors="replace").strip()[-300:]}
            time.sleep(1)
            if replica.process.poll() is not None:
                run["outcome"] = f"mysqld exited with {replica.process.poll()}"
                problems.append(f"mysqlbinlog | mysql of '{edit}': "
                                + run["outcome"])
                replica.process = None
                shutil.rmtree(replica.datadir, ignore_errors=True)
                replica.start()
            else:
                run["gtid_executed"] = replica.sql(
                    "SELECT @@GLOBAL.gtid_executed").strip()
                run["rows"] = rw.rows_digest(replica)
                run["outcome"] = "the server keeps running"
            result["runs"].append(run)
    finally:
        source.stop()
        shutil.rmtree(source.datadir, ignore_errors=True)
        shutil.rmtree(template, ignore_errors=True)
    return rw.finish(result, problems)


# ---------------------------------------------------------------------------

OPTIONAL_CASES = {"replica_matrix": run_replica_matrix,
                  "mysql_minimal": run_mysql_minimal}
CASES = {name: run_scenario for name in SCENARIOS}
CASES.update(OPTIONAL_CASES)


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--mysql-basedir", type=Path, required=True)
    parser.add_argument("--binsrv", type=Path, required=True)
    parser.add_argument("--workdir", type=Path, required=True)
    parser.add_argument("--port", type=int, default=24100,
                        help="the source's port; the replica, 'pull', the "
                             "proxy and the second source of "
                             "'mysql_minimal' use the four ports above it")
    parser.add_argument("--mode", choices=["position", "gtid"],
                        default="position")
    parser.add_argument("--rewrite", action="store_true",
                        help="with 'replication.rewrite' (needs --mode gtid)")
    parser.add_argument("--cases", nargs="*", choices=list(CASES),
                        help="the cases to run (default: all but the "
                             "optional ones)")
    parser.add_argument("--replica-crash", action="store_true",
                        help="also run 'replica_matrix' and 'mysql_minimal' "
                             "(several minutes; the replica server dies "
                             "and is set up again many times)")
    parser.add_argument("--replica-all", action="store_true",
                        help="run a replica of 'pull' in every case that "
                             "ends with a stub in the storage")
    args = parser.parse_args()
    if args.rewrite and args.mode != "gtid":
        parser.error("--rewrite needs --mode gtid")
    args.workdir = args.workdir.expanduser()
    args.workdir.mkdir(parents=True, exist_ok=True)
    # pylint: disable-next=global-statement
    global GTID_MODE, REWRITE, REPLICA_ALL, WORKDIR, EDITED_SOURCE
    GTID_MODE = args.mode == "gtid"
    REWRITE = args.rewrite
    REPLICA_ALL = args.replica_all
    WORKDIR = args.workdir
    rw.MYSQL_BASEDIR = args.mysql_basedir.expanduser()
    rw.PULL_PORT = args.port + 2
    rw.PROXY_PORT = args.port + 3
    harness.SERVER = harness.MySQLServer(rw.MYSQL_BASEDIR,
                                         args.workdir / "mysql", args.port,
                                         GTID_MODE, server_id=1)
    rw.REPLICA = harness.MySQLServer(rw.MYSQL_BASEDIR,
                                     args.workdir / "replica", args.port + 1,
                                     GTID_MODE, server_id=2)
    EDITED_SOURCE = harness.MySQLServer(rw.MYSQL_BASEDIR,
                                        args.workdir / "source2",
                                        args.port + 4, True, server_id=3)
    harness.BINSRV = harness.BinlogServer(args.binsrv.expanduser(),
                                          args.workdir / "binsrv", args.port,
                                          GTID_MODE, None)
    cases = args.cases
    if cases is None:
        cases = list(SCENARIOS) + (list(OPTIONAL_CASES)
                                   if args.replica_crash else [])
    results = []
    started = time.monotonic()
    # a replica that died of what it was sent may die again when it starts
    shutil.rmtree(rw.REPLICA.datadir, ignore_errors=True)
    harness.SERVER.start()
    try:
        rw.REPLICA.start()
        for name in cases:
            case_started = time.monotonic()
            try:
                result = CASES[name](name)
            except (harness.Failure, RuntimeError,
                    subprocess.SubprocessError, OSError) as failure:
                result = {"case": name, "ok": False,
                          "why": harness.describe_failure(failure, 600)}
                if harness.SERVER.process is None:
                    harness.SERVER.start()
            result["mode"] = args.mode + ("+rewrite" if REWRITE else "")
            result["seconds"] = round(time.monotonic() - case_started, 1)
            print(json.dumps(result), flush=True)
            results.append(result)
    finally:
        rw.REPLICA.stop()
        harness.SERVER.stop()
        EDITED_SOURCE.stop()
    failures = [result for result in results if not result.get("ok")]
    print(json.dumps({"cases": len(results), "failures": len(failures),
                      "failed": [result["case"] for result in failures],
                      "seconds": round(time.monotonic() - started),
                      "passed": not failures}), flush=True)
    (args.workdir / "result.json").write_text(json.dumps(results, indent=2))
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
