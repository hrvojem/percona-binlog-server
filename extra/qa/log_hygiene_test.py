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

"""What 'binlog_server pull' writes to its log at the default 'info' level
(PBS-118), and what a logged-in client can make it write.

Runs a source MySQL server and 'pull' (logger level 'info', as in
main_config.json and the README) with its replication source listener, and
counts and inspects the lines 'pull' appends to its log file. Cases:

- 'no_per_event_lines': a real MySQL replica (CHANGE REPLICATION SOURCE TO
  the listener) and a raw blocking COM_BINLOG_DUMP client stream from 'pull'
  while TRANSACTIONS and then 2 x TRANSACTIONS transactions run on the
  source. The README says info-level messages "are not written for every
  individual binary log event": no line of the per-event kinds (collector
  'event  :' lines, 'sent server binlog event', 'generated binlog event
  packet', sender block lines, heartbeat packets, parsed dumps) may appear,
  and the lines added must not grow with the number of transactions (at
  most MAX_LINES_PER_TRANSACTION more per extra transaction, not counting
  the lines written once per binlog file: created / closed, switched to,
  fetched EOF at its end). The lines of
  20 s with both replicas idle are counted as well (reported only).
- 'idle_eof_once': one raw blocking dump client. 'fetched EOF at
  <binlog>:<position>' must be logged once per idle period (the sender
  polls every 500 ms; before PBS-118 every poll logged): after catching up
  and idling IDLE_SECONDS, again after new transactions arrive and the
  client idles again, across a FLUSH BINARY LOGS on the source, and for a
  client that asked for a heartbeat every second (heartbeats must not
  re-arm the message). No '<binlog>:<position>' may be logged twice.
  Then 'pull' is restarted on an empty storage with the source stopped: a
  blocking dump must log 'waiting for first binlog in empty storage' once
  (not every poll), and once the source is started and the client has
  received the binlogs, one 'fetched EOF' follows. The lines 'pull' writes
  per minute while the source is down are reported.
- 'log_injection': since PBS-118 a client command is logged on one info
  line with the statement, the binlog file name and the GTID set of a dump
  request written as received. A logged-in client sends COM_QUERY with a
  line feed (followed by a forged log line), CR LF, a lone CR, ANSI escape
  sequences and a NUL; statements of 1 MB and 4 MB; COM_BINLOG_DUMP with a
  line feed in the binlog name; and COM_BINLOG_DUMP_GTID with large GTID
  sets (900,000 intervals of one UUID, 50,000 UUIDs) and odd ones (reversed
  interval, GNO 0, GNO 2^63, more TSIDs or intervals than the body holds,
  an unknown format byte, a 40-byte tag). Each probe must leave no line
  beginning with the forged prefix, no raw CR, ESC or NUL byte in the log,
  add at most MAX_BYTES_PER_COMMAND bytes to the log, be answered (or the
  connection closed) within 60 s, and leave 'pull' running and serving
  logins. The time to the answer is reported: a long statement is matched
  against the handshake regular expressions on the listener's thread
  (ticket 74 (PBS-156)), and a GTID set is decoded and printed in full for
  the log line before the command is checked. (Ticket 76 (PBS-152) covers the same for the
  user name of a client that has not logged in; this is the post-login,
  info-level variant.)
- 'log_flood': one logged-in client sends 'SET @qa_flood = 1' (answered
  with a syntax error) in a tight loop for --flood-seconds (default 30),
  then COM_PING and 64 KB statements for a third of that each. Reported:
  commands per second, log bytes and lines per command and per second, the
  projected log growth per day, and whether the rate dropped over the run
  (a rate limit). Fails if the log grows faster than --flood-limit bytes per
  second (default 65536) in any of the three loops.

With an ASan build 'pull' runs with abort_on_error=1 and its standard error
is kept in '<workdir>/pull.stderr'; a sanitizer report fails the case that
was running.

A JSON line per case, then a summary; exit 1 if any case failed. Example:

  PYTHONPATH=~/ws/qa/pylib python3 log_hygiene_test.py \\
    --mysql-basedir ~/ws/dist/mysql-8.4.8-linux-glibc2.28-x86_64-minimal \\
    --binsrv ~/ws/percona-binlog-server-build-debug_gcc14/binlog_server \\
    --workdir ~/ws/qa/log-hygiene [--mode gtid] [--cases a,b]
"""

import argparse
import collections
import json
import os
import re
import signal
import socket
import struct
import subprocess
import sys
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import fetch_crash_test as harness  # noqa: E402
import listener_fuzz_test as listener  # noqa: E402
from listener_command_fuzz_test import (  # noqa: E402
    CHECKSUM_ON, com_binlog_dump, com_binlog_dump_gtid, gtid_set)

CASES = ("no_per_event_lines", "idle_eof_once", "log_injection",
         "log_flood")
TRANSACTIONS = 150
MAX_LINES_PER_TRANSACTION = 0.05
IDLE_SECONDS = 6.0
CATCH_UP_SECONDS = 90.0
MAX_BYTES_PER_COMMAND = 64 * 1024
XID_EVENT = 16
ROTATE_EVENT = 4
HEARTBEAT_EVENTS = (27, 41)
COM_PING = 0x0e
FORGED_PREFIX = b"[2026-01-01T00:00:00.000000]"
FORGED_LINE = FORGED_PREFIX + b" [  error] forged by a replication client"
UUID = bytes(range(0x10, 0x20))
LINE = re.compile(r"^\[[^\]]*\] \[\s*(\w+)\] (.*)$")
EOF_LINE = re.compile(r"sender : fetched EOF at (\S+):(\d+)")
WAITING_TEXT = "sender : waiting for first binlog in empty storage"
# the kinds of line that before PBS-118 were written at info level for
# every binlog event (or every block / poll), and must not be any more
PER_EVENT_PATTERNS = {
    "collector event": re.compile(r"\] event  : "),
    "sent server binlog event": re.compile(r"sent server binlog event"),
    "generated binlog event packet":
        re.compile(r"generated binlog event packet"),
    "sender block": re.compile(
        r"sender : (fetched event block|parsed event block|reusing |"
        r"block too small)"),
    "heartbeat packet": re.compile(r"generated heartbeat event packet"),
    "fetched N-byte event": re.compile(r"fetched \d+-byte\(s\) event"),
    "rewrite per-event": re.compile(r"rewrite: (encountered|generated)"),
}


# lines written once per binlog file (rotation), not per event: excluded
# from the per-transaction growth
PER_FILE_LINE = re.compile(
    r"storage: (created a new|closed|reused already open) binlog file|"
    r"sender : switched to a new binlog file|sender : fetched EOF at")


class Failure(harness.Failure):
    pass


# ---------------------------------------------------------------------------
# the log
# ---------------------------------------------------------------------------

class Log:
    """Reads what 'pull' appended to its log file since a mark."""

    def __init__(self, path: Path):
        self.path = path

    def mark(self) -> int:
        try:
            return self.path.stat().st_size
        except FileNotFoundError:
            return 0

    def since(self, mark: int) -> bytes:
        try:
            with self.path.open("rb") as stream:
                stream.seek(mark)
                return stream.read()
        except FileNotFoundError:
            return b""

    def lines_since(self, mark: int) -> list:
        return self.since(mark).decode(errors="replace").splitlines()


def level_counts(lines: list) -> dict:
    counts = collections.Counter()
    for line in lines:
        match = LINE.match(line)
        counts[match.group(1) if match else "continuation"] += 1
    return dict(counts)


def per_event_lines(lines: list) -> dict:
    found = {}
    for name, pattern in PER_EVENT_PATTERNS.items():
        hits = [line for line in lines if pattern.search(line)]
        if hits:
            found[name] = {"count": len(hits), "first": hits[0][:200]}
    return found


def eof_lines(lines: list) -> list:
    """(binlog, position) of every 'fetched EOF' line, in order."""
    result = []
    for line in lines:
        match = EOF_LINE.search(line)
        if match:
            result.append((match.group(1), int(match.group(2))))
    return result


# ---------------------------------------------------------------------------
# pull
# ---------------------------------------------------------------------------

class Pull:
    def __init__(self, binsrv, port: int, stderr_path: Path):
        self.binsrv, self.port, self.process = binsrv, port, None
        self.stderr_path = stderr_path
        self.stderr = None
        self.stderr_mark = 0

    def start(self):
        self.stop()
        binsrv = self.binsrv
        binsrv.write_config()
        config = json.loads(binsrv.config_path.read_text())
        config["logger"]["level"] = "info"
        config["replication_source"] = {
            "port": self.port, "server_id": 142,
            "server_uuid": "00000000-0000-0000-0000-00000000008e",
            "read_timeout": 60, "write_timeout": 60,
            "require_secure_transport": False,
            "auth_rsa_private_key": str(listener.AUTH_RSA_PRIVATE_KEY),
            "authentication": {"user": listener.PULL_USER,
                               "password": listener.PULL_PASSWORD,
                               "plugin": "caching_sha2_password"}}
        binsrv.config_path.write_text(json.dumps(config, indent=2))
        self.stderr = self.stderr_path.open("ab")
        self.process = subprocess.Popen(
            [str(binsrv.binary), "pull", str(binsrv.config_path)],
            stdout=subprocess.DEVNULL, stderr=self.stderr,
            env=dict(os.environ, AWS_EC2_METADATA_DISABLED="true",
                     ASAN_OPTIONS="abort_on_error=1:exitcode=134"))
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            try:
                socket.create_connection(("127.0.0.1", self.port),
                                         timeout=1).close()
                return
            except OSError:
                if self.process.poll() is not None:
                    raise Failure("pull exited before it listened\n" +
                                  binsrv.log_tail())
                time.sleep(0.2)
        raise Failure("pull did not start listening in 30 s")

    def alive(self) -> bool:
        return self.process is not None and self.process.poll() is None

    def stop(self):
        if self.process is not None:
            if self.process.poll() is None:
                self.process.send_signal(signal.SIGTERM)
                try:
                    self.process.wait(timeout=60)
                except subprocess.TimeoutExpired:
                    self.process.kill()
                    self.process.wait()
            self.process = None
        if self.stderr is not None:
            self.stderr.close()
            self.stderr = None

    def sanitizer_report(self) -> "str | None":
        """The first sanitizer line written since the previous call."""
        try:
            data = self.stderr_path.read_bytes()
        except FileNotFoundError:
            return None
        new, self.stderr_mark = data[self.stderr_mark:], len(data)
        for line in new.decode(errors="replace").splitlines():
            if "Sanitizer" in line or "runtime error:" in line:
                return line[:300]
        return None


def logs_in(port: int) -> bool:
    client = listener.Listener("127.0.0.1", port, timeout=10.0)
    try:
        return client.handshake()
    finally:
        client.close()


def read_frame(sock: socket.socket, timeout: float):
    """(sequence, body), or 'timeout' / 'closed'."""
    def receive(count: int):
        data = bytearray()
        while len(data) < count:
            try:
                piece = sock.recv(count - len(data))
            except socket.timeout:
                if not data:
                    return "timeout"
                continue
            except OSError:
                return "closed"
            if not piece:
                return "closed"
            data += piece
        return bytes(data)

    sock.settimeout(timeout)
    header = receive(4)
    if isinstance(header, str):
        return header
    length = int.from_bytes(header[:3], "little")
    body = receive(length) if length else b""
    return body if isinstance(body, str) else (header[3], body)


def reply_kind(read) -> str:
    if isinstance(read, str):
        return read
    body = read[1]
    if body[:1] == b"\xff":
        return (f"ERR {int.from_bytes(body[1:3], 'little')} "
                f"{body[9:89].decode(errors='replace')}")
    if body[:1] == b"\x00":
        return "OK"
    if body[:1] == b"\xfe" and len(body) < 9:
        return "EOF"
    return "data " + body[:4].hex()


class Session:
    """A logged-in raw client."""

    def __init__(self, port: int, timeout: float = 30.0):
        self.client = listener.Listener("127.0.0.1", port, timeout=timeout)
        if not self.client.handshake():
            self.client.close()
            raise Failure("could not log in to the listener")

    def command(self, payload: bytes, timeout: float = 30.0) -> str:
        self.client.send(listener.frame(0, payload))
        return reply_kind(read_frame(self.client.sock, timeout))

    def query(self, statement: bytes, timeout: float = 30.0) -> str:
        return self.command(bytes([listener.COM_QUERY]) + statement, timeout)

    def close(self):
        self.client.close()


class DumpClient:
    """A raw blocking dump: logs in, declares the checksum (and a heartbeat
    period), sends COM_BINLOG_DUMP (position mode) or COM_BINLOG_DUMP_GTID
    with an empty set (GTID mode) from the first binlog, and counts the
    events it receives on a thread."""

    def __init__(self, port: int, gtid_mode: bool,
                 heartbeat_ns: "int | None" = None, server_id: int = 9001):
        self.session = Session(port)
        reply = self.session.query(CHECKSUM_ON)
        if reply != "OK":
            raise Failure(f"declaring the checksum got {reply}")
        if heartbeat_ns is not None:
            reply = self.session.query(
                b"SET @master_heartbeat_period = %d, "
                b"@source_heartbeat_period = %d" % (heartbeat_ns,
                                                    heartbeat_ns))
            if reply != "OK":
                raise Failure(f"setting the heartbeat period got {reply}")
        self.counts = collections.Counter()
        self.last_data_event = time.monotonic()
        self.closed = False
        self.error = None
        self.lock = threading.Lock()
        if gtid_mode:
            payload = com_binlog_dump_gtid(0x04, server_id, b"", 4,
                                           gtid_set([]))
        else:
            payload = com_binlog_dump(4, 0, server_id, b"")
        self.session.client.send(listener.frame(0, payload))
        self.thread = threading.Thread(target=self._read, daemon=True)
        self.thread.start()

    def _read(self):
        sock = self.session.client.sock
        while True:
            read = read_frame(sock, 1.0)
            if read == "timeout":
                if self.closed:
                    return
                continue
            if isinstance(read, str):
                self.closed = True
                return
            body = read[1]
            if body[:1] == b"\xff":
                self.error = reply_kind(read)
                self.closed = True
                return
            if body[:1] == b"\xfe" and len(body) < 9:
                self.closed = True
                return
            if body[:1] == b"\x00" and len(body) >= 6:
                event_type = body[5]
                with self.lock:
                    self.counts[event_type] += 1
                    if event_type not in HEARTBEAT_EVENTS:
                        self.last_data_event = time.monotonic()

    def xids(self) -> int:
        with self.lock:
            return self.counts[XID_EVENT]

    def heartbeats(self) -> int:
        with self.lock:
            return sum(self.counts[code] for code in HEARTBEAT_EVENTS)

    def wait_for_xids(self, expected: int, timeout: float = CATCH_UP_SECONDS):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self.xids() >= expected:
                return
            if self.closed:
                raise Failure(f"the dump ended ({self.error}) after "
                              f"{self.xids()} of {expected} transactions")
            time.sleep(0.2)
        raise Failure(f"the dump client received {self.xids()} of "
                      f"{expected} transactions in {timeout:.0f} s")

    def close(self):
        self.closed = True
        self.session.close()
        self.thread.join(timeout=5)


# ---------------------------------------------------------------------------
# the source
# ---------------------------------------------------------------------------

class Workload:
    """Transactions on the source, one row each, counted."""

    def __init__(self, server):
        self.server = server
        self.count = 0

    def run(self, transactions: int, batch: int = 50):
        done = 0
        while done < transactions:
            size = min(batch, transactions - done)
            self.server.sql(" ".join(
                "BEGIN; INSERT INTO qa.t (payload) VALUES "
                f"(REPEAT('x', {64 + (self.count + index) % 200})); COMMIT;"
                for index in range(size)))
            done += size
            self.count += size


def replicate_from_pull(replica, pull_port: int, gtid_mode: bool,
                        first_binlog: str):
    # the source creates the 'qa' database before resetting its binary logs,
    # so only the table is in the binlogs
    replica.sql("STOP REPLICA; RESET REPLICA ALL; "
                "DROP DATABASE IF EXISTS qa; CREATE DATABASE qa;")
    if gtid_mode:
        replica.sql("RESET BINARY LOGS AND GTIDS"
                    if replica.version() >= (8, 2, 0) else "RESET MASTER")
    start = ("SOURCE_AUTO_POSITION=1" if gtid_mode else
             f"SOURCE_LOG_FILE='{first_binlog}', SOURCE_LOG_POS=4")
    replica.sql(
        f"CHANGE REPLICATION SOURCE TO SOURCE_HOST='127.0.0.1', "
        f"SOURCE_PORT={pull_port}, SOURCE_USER='{listener.PULL_USER}', "
        f"SOURCE_PASSWORD='{listener.PULL_PASSWORD}', "
        f"GET_SOURCE_PUBLIC_KEY=1, SOURCE_SSL=0, SOURCE_CONNECT_RETRY=2, "
        f"{start}; START REPLICA;")


def replica_rows(replica) -> int:
    try:
        return int(replica.sql("SELECT COUNT(*) FROM qa.t").strip())
    except (RuntimeError, ValueError):
        return -1


def replica_status(replica) -> str:
    try:
        return " ".join(replica.sql(
            "SELECT SERVICE_STATE, LAST_ERROR_NUMBER, LAST_ERROR_MESSAGE "
            "FROM performance_schema.replication_connection_status; "
            "SELECT SERVICE_STATE, LAST_ERROR_NUMBER, LAST_ERROR_MESSAGE "
            "FROM performance_schema.replication_applier_status_by_worker"
        ).split())[:400]
    except RuntimeError as error:
        return str(error)[:200]


def wait_until(condition, timeout: float, what: str, detail=None):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if condition():
            return
        time.sleep(0.3)
    raise Failure(f"{what} did not happen in {timeout:.0f} s" +
                  (f" ({detail()})" if detail else ""))


# ---------------------------------------------------------------------------
# cases
# ---------------------------------------------------------------------------

def case_no_per_event_lines(ctx) -> dict:
    pull, log, workload = ctx["pull"], ctx["log"], ctx["workload"]
    replica = harness.MySQLServer(ctx["args"].mysql_basedir,
                                  ctx["args"].workdir / "replica",
                                  ctx["args"].port + 1, ctx["gtid_mode"],
                                  server_id=2)
    result = {"case": "no_per_event_lines"}
    replica.start()
    dump = None
    try:
        replicate_from_pull(replica, pull.port, ctx["gtid_mode"],
                            ctx["source"].binlog_names()[0])
        dump = DumpClient(pull.port, ctx["gtid_mode"])
        dump.wait_for_xids(workload.count)
        wait_until(lambda: replica_rows(replica) == workload.count,
                   CATCH_UP_SECONDS, "the replica catching up",
                   lambda: f"{replica_rows(replica)} of {workload.count} "
                   f"rows; {replica_status(replica)}")
        time.sleep(3)
        rounds = []
        for transactions in (TRANSACTIONS, 2 * TRANSACTIONS):
            mark = log.mark()
            started = time.monotonic()
            workload.run(transactions)
            dump.wait_for_xids(workload.count)
            wait_until(lambda: replica_rows(replica) == workload.count,
                       CATCH_UP_SECONDS, "the replica catching up",
                   lambda: f"{replica_rows(replica)} of {workload.count} "
                   f"rows; {replica_status(replica)}")
            streamed = time.monotonic() - started
            time.sleep(IDLE_SECONDS)
            lines = log.lines_since(mark)
            rounds.append({
                "transactions": transactions,
                "streaming_seconds": round(streamed, 1),
                "lines": len(lines), "bytes": len(log.since(mark)),
                "levels": level_counts(lines),
                "per_event": per_event_lines(lines),
                "eof_lines": len(eof_lines(lines)),
                "per_file_lines": sum(bool(PER_FILE_LINE.search(line))
                                      for line in lines),
            })
        mark = log.mark()
        time.sleep(20)
        idle_lines = log.lines_since(mark)
        result["rounds"] = rounds
        result["idle_20s_lines"] = len(idle_lines)
        result["idle_20s_sample"] = idle_lines[:5]
        slope = (((rounds[1]["lines"] - rounds[1]["per_file_lines"]) -
                  (rounds[0]["lines"] - rounds[0]["per_file_lines"])) /
                 (rounds[1]["transactions"] - rounds[0]["transactions"]))
        result["extra_lines_per_transaction"] = round(slope, 4)
        problems = []
        for one in rounds:
            if one["per_event"]:
                problems.append(f"per-event lines at info level with "
                                f"{one['transactions']} transactions: "
                                f"{sorted(one['per_event'])}")
        if slope > MAX_LINES_PER_TRANSACTION:
            problems.append(f"the log grows by {slope:.3f} lines per "
                            "transaction")
        if per_event_lines(idle_lines):
            problems.append("per-event lines while idle")
        result["ok"] = not problems
        if problems:
            result["why"] = "; ".join(problems)
    finally:
        if dump is not None:
            dump.close()
        try:
            replica.sql("STOP REPLICA")
        except RuntimeError:
            pass
        replica.stop()
    return result


def idle_period(log, dump, mark: int, expected_xids: int) -> dict:
    """Waits for the client to catch up and idle; what was logged."""
    dump.wait_for_xids(expected_xids)
    time.sleep(IDLE_SECONDS)
    lines = log.lines_since(mark)
    eofs = eof_lines(lines)
    repeated = [f"{name}:{position} x{count}" for (name, position), count
                in collections.Counter(eofs).items() if count > 1]
    return {"lines": len(lines), "eof_lines": [f"{n}:{p}" for n, p in eofs],
            "repeated": repeated,
            "waiting_lines": sum(WAITING_TEXT in line for line in lines)}


def case_idle_eof_once(ctx) -> dict:
    pull, log, workload = ctx["pull"], ctx["log"], ctx["workload"]
    source, binsrv = ctx["source"], ctx["binsrv"]
    result = {"case": "idle_eof_once"}
    problems = []
    periods = {}

    def last_binlog() -> str:
        names = binsrv.indexed_binlogs()
        return names[-1] if names else ""

    def check(label: str, period: dict, expect_eof: bool = True):
        periods[label] = period
        if period["repeated"]:
            problems.append(f"{label}: repeated {period['repeated']}")
        if expect_eof:
            if not period["eof_lines"]:
                problems.append(f"{label}: no 'fetched EOF' line")
            elif not period["eof_lines"][-1].startswith(last_binlog() + ":"):
                problems.append(f"{label}: last 'fetched EOF' names "
                                f"{period['eof_lines'][-1]}, the last "
                                f"binlog is {last_binlog()}")

    # (a) catch up and idle; (b) new transactions, idle again; (c) across a
    # rotation of the source
    dump = DumpClient(pull.port, ctx["gtid_mode"])
    try:
        check("first_idle", idle_period(log, dump,
                                        ctx["log_mark_before_case"],
                                        workload.count))
        mark = log.mark()
        workload.run(5)
        check("after_new_events", idle_period(log, dump, mark,
                                              workload.count))
        mark = log.mark()
        source.sql("FLUSH BINARY LOGS")
        workload.run(5)
        check("after_rotation", idle_period(log, dump, mark, workload.count))
        mark = log.mark()
        time.sleep(IDLE_SECONDS)
        quiet = log.lines_since(mark)
        periods["still_idle"] = {"lines": len(quiet),
                                 "eof_lines": len(eof_lines(quiet))}
        if eof_lines(quiet):
            problems.append("'fetched EOF' logged again without new events")
    finally:
        dump.close()

    # (d) heartbeats every second must not re-arm the message
    mark = log.mark()
    dump = DumpClient(pull.port, ctx["gtid_mode"], heartbeat_ns=10**9,
                      server_id=9002)
    try:
        period = idle_period(log, dump, mark, workload.count)
        period["heartbeats"] = dump.heartbeats()
        check("heartbeat_idle", period)
        mark = log.mark()
        workload.run(5)
        period = idle_period(log, dump, mark, workload.count)
        period["heartbeats"] = dump.heartbeats()
        check("heartbeat_after_new_events", period)
        if len(period["eof_lines"]) > 1:
            problems.append("heartbeat_after_new_events: "
                            f"{len(period['eof_lines'])} 'fetched EOF' lines")
        if dump.heartbeats() < 3:
            problems.append(f"only {dump.heartbeats()} heartbeats received")
    finally:
        dump.close()

    # (e) empty storage, source down
    pull.stop()
    source.stop()
    binsrv.reset()
    binsrv.checkpoint_size = "1"
    pull.start()
    mark = log.mark()
    time.sleep(1)
    dump = DumpClient(pull.port, ctx["gtid_mode"], server_id=9003)
    try:
        started = time.monotonic()
        time.sleep(IDLE_SECONDS)
        down_lines = log.lines_since(mark)
        down_seconds = time.monotonic() - started + 1
        waiting = sum(WAITING_TEXT in line for line in down_lines)
        periods["empty_storage"] = {
            "lines": len(down_lines), "waiting_lines": waiting,
            "lines_per_minute_source_down":
                round(len(down_lines) * 60 / down_seconds, 1),
            "sample": [line[:160] for line in down_lines[-4:]]}
        if waiting != 1:
            problems.append(f"empty_storage: {waiting} 'waiting for first "
                            "binlog' lines in "
                            f"{IDLE_SECONDS:.0f} s (expected 1)")
        mark2 = log.mark()
        source.start()
        period = idle_period(log, dump, mark2, workload.count)
        check("empty_then_filled", period)
        if period["waiting_lines"]:
            problems.append("empty_then_filled: 'waiting for first binlog' "
                            "logged again after the storage got binlogs")
    finally:
        dump.close()
    result["periods"] = periods
    result["ok"] = not problems
    if problems:
        result["why"] = "; ".join(problems)
    return result


def analyse_probe(log, mark: int) -> dict:
    data = log.since(mark)
    lines = data.split(b"\n")
    return {
        "bytes_logged": len(data),
        "lines_logged": len(lines) - 1,
        "forged_lines": sum(line.startswith(FORGED_PREFIX) for line in lines),
        "raw_cr": data.count(b"\r"),
        "raw_esc": data.count(b"\x1b"),
        "raw_nul": data.count(b"\x00"),
        "error_lines": [line[:200].decode(errors="replace")
                        for line in lines if b"[  error]" in line][:3],
    }


def large_gtid_set(tsids: list) -> bytes:
    """An untagged encoded GTID set of (uuid, [(lower, upper), ...]) built
    in linear time (gtid_set() of listener_command_fuzz_test appends to a
    bytes object, which is quadratic for 900,000 intervals)."""
    parts = [struct.pack("<Q", len(tsids))]
    for uuid, intervals in tsids:
        parts.append(uuid)
        parts.append(struct.pack("<Q", len(intervals)))
        parts.extend(struct.pack("<QQ", lower, upper)
                     for lower, upper in intervals)
    return b"".join(parts)


def injection_probes(gtid_mode: bool) -> list:
    """(name, payload); the dumps are refused without the checksum (or in
    position mode for GTID dumps), after the command has been logged."""
    query = bytes([listener.COM_QUERY])
    max_gno = (1 << 63) - 1
    probes = [
        ("query_line_feed", query + b"SELECT 1\n" + FORGED_LINE),
        ("query_cr_lf", query + b"SELECT 1\r\n" + FORGED_LINE),
        ("query_cr", query + b"SELECT 1\r" + FORGED_LINE),
        ("query_ansi", query + b"SELECT 1 \x1b[2K\x1b[1G\x1b[31mFAKE"
                                b"\x1b[0m\x1b]0;title\x07"),
        ("query_nul", query + b"SELECT 1\x00hidden after NUL"),
        ("query_1mb", query + b"SELECT '" + b"A" * 1_000_000 + b"'"),
        ("query_4mb", query + b"SELECT '" + b"A" * 4_000_000 + b"'"),
        ("dump_name_line_feed",
         com_binlog_dump(4, 0, 9100, b"binlog.000001\n" + FORGED_LINE)),
        ("dump_gtid_name_line_feed",
         com_binlog_dump_gtid(0x04, 9100, b"binlog.000001\n" + FORGED_LINE,
                              4, gtid_set([]))),
        ("dump_gtid_900k_intervals",
         com_binlog_dump_gtid(0x04, 9100, b"", 4, large_gtid_set(
             [(UUID, [(2 * i + 1, 2 * i + 2) for i in range(900_000)])]))),
        ("dump_gtid_50k_uuids",
         com_binlog_dump_gtid(0x04, 9100, b"", 4, large_gtid_set(
             [(struct.pack(">QQ", 1, i), [(1, 2)])
              for i in range(50_000)]))),
        ("dump_gtid_reversed_interval",
         com_binlog_dump_gtid(0x04, 9100, b"", 4, gtid_set(
             [(UUID, b"", [(10, 5)], None)]))),
        ("dump_gtid_gno_zero",
         com_binlog_dump_gtid(0x04, 9100, b"", 4, gtid_set(
             [(UUID, b"", [(0, 1)], None)]))),
        ("dump_gtid_gno_2_63",
         com_binlog_dump_gtid(0x04, 9100, b"", 4, gtid_set(
             [(UUID, b"", [(max_gno, max_gno + 1)], None)]))),
        ("dump_gtid_tsid_count_lies",
         com_binlog_dump_gtid(0x04, 9100, b"", 4, gtid_set(
             [], count=(1 << 56) - 1))),
        ("dump_gtid_interval_count_lies",
         com_binlog_dump_gtid(0x04, 9100, b"", 4, gtid_set(
             [(UUID, b"", [(1, 2)], (1 << 64) - 1)]))),
        ("dump_gtid_unknown_format",
         com_binlog_dump_gtid(0x04, 9100, b"", 4,
                              b"\x07" + b"\x00" * 6 + b"\x07")),
        ("dump_gtid_long_tag",
         com_binlog_dump_gtid(0x04, 9100, b"", 4, gtid_set(
             [(UUID, b"t" * 40, [(1, 2)], None)], tagged=True))),
        ("dump_gtid_tag_line_feed",
         com_binlog_dump_gtid(0x04, 9100, b"", 4, gtid_set(
             [(UUID, b"a\nb", [(1, 2)], None)], tagged=True))),
    ]
    return probes


def case_log_injection(ctx) -> dict:
    pull, log = ctx["pull"], ctx["log"]
    probes = []
    problems = []
    for name, payload in injection_probes(ctx["gtid_mode"]):
        if not pull.alive():
            pull.start()
        mark = log.mark()
        probe = {"probe": name, "payload_bytes": len(payload)}
        try:
            session = Session(pull.port)
        except (Failure, OSError) as error:
            probe["reply"] = f"no login: {error}"
            problems.append(f"{name}: no login before the probe")
            probes.append(probe)
            continue
        started = time.monotonic()
        try:
            probe["reply"] = session.command(payload, timeout=60)
        finally:
            session.close()
        probe["reply_seconds"] = round(time.monotonic() - started, 2)
        time.sleep(0.5)
        probe.update(analyse_probe(log, mark))
        probe["pull_alive"] = pull.alive()
        probe["login_after"] = probe["pull_alive"] and logs_in(pull.port)
        report = pull.sanitizer_report()
        if report:
            probe["sanitizer"] = report
        bad = [what for what, failed in (
            ("forged line", probe["forged_lines"]),
            ("raw CR", probe["raw_cr"]), ("raw ESC", probe["raw_esc"]),
            ("raw NUL", probe["raw_nul"]),
            (f"{probe['bytes_logged']} bytes logged",
             probe["bytes_logged"] > MAX_BYTES_PER_COMMAND),
            ("no reply in 60 s (the listener was busy with the command)",
             probe["reply"] == "timeout"),
            ("pull ended", not probe["pull_alive"]),
            ("no login after", not probe["login_after"]),
            ("sanitizer report", report)) if failed]
        if bad:
            problems.append(f"{name}: {', '.join(bad)}")
        probe["ok"] = not bad
        probes.append(probe)
    result = {"case": "log_injection", "probes": probes, "ok": not problems}
    if problems:
        result["why"] = "; ".join(problems)
    return result


def flood_loop(port: int, log, payload: bytes, seconds: float) -> dict:
    session = Session(port)
    mark = log.mark()
    started = time.monotonic()
    sent = 0
    per_second = []
    second_start, second_count = started, 0
    replies = collections.Counter()
    try:
        while time.monotonic() - started < seconds:
            reply = session.command(payload, timeout=10)
            replies[reply.split(" ")[0] + (" " + reply.split(" ")[1]
                                           if reply.startswith("ERR") else "")
                    ] += 1
            if reply in ("closed", "timeout"):
                break
            sent += 1
            second_count += 1
            now = time.monotonic()
            if now - second_start >= 1.0:
                per_second.append(second_count / (now - second_start))
                second_start, second_count = now, 0
    finally:
        session.close()
    elapsed = time.monotonic() - started
    time.sleep(0.5)
    data = log.since(mark)
    lines = data.count(b"\n")
    third = max(1, len(per_second) // 3)
    first_rate = sum(per_second[:third]) / third if per_second else 0
    last_rate = sum(per_second[-third:]) / third if per_second else 0
    return {
        "payload_bytes": len(payload), "seconds": round(elapsed, 1),
        "commands": sent, "replies": dict(replies),
        "commands_per_second": round(sent / elapsed, 1),
        "log_bytes": len(data), "log_lines": lines,
        "log_bytes_per_second": round(len(data) / elapsed),
        "log_bytes_per_command": round(len(data) / max(sent, 1), 1),
        "log_lines_per_command": round(lines / max(sent, 1), 3),
        "projected_gb_per_day": round(len(data) / elapsed * 86400 / 1e9, 2),
        "rate_first_third": round(first_rate, 1),
        "rate_last_third": round(last_rate, 1),
        "throttled": bool(per_second) and last_rate < 0.5 * first_rate,
    }


def case_log_flood(ctx) -> dict:
    pull, log, args = ctx["pull"], ctx["log"], ctx["args"]
    loops = {
        "short_query": flood_loop(pull.port, log, bytes(
            [listener.COM_QUERY]) + b"SET @qa_flood = 1", args.flood_seconds),
        "ping": flood_loop(pull.port, log, bytes([COM_PING]),
                           args.flood_seconds / 3),
        "query_64kb": flood_loop(pull.port, log, bytes(
            [listener.COM_QUERY]) + b"SET @qa_flood = '" + b"F" * 65536 +
            b"'", args.flood_seconds / 3),
    }
    problems = [f"{name}: {loop['log_bytes_per_second']} log bytes/s"
                for name, loop in loops.items()
                if loop["log_bytes_per_second"] > args.flood_limit]
    alive = pull.alive()
    report = pull.sanitizer_report()
    if not alive:
        problems.append("pull ended")
    if report:
        problems.append(f"sanitizer: {report}")
    result = {"case": "log_flood", "loops": loops, "pull_alive": alive,
              "ok": not problems}
    if problems:
        result["why"] = "; ".join(problems)
    return result


# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--mysql-basedir", type=Path, required=True)
    parser.add_argument("--binsrv", type=Path, required=True)
    parser.add_argument("--workdir", type=Path, required=True)
    parser.add_argument("--port", type=int, default=24760)
    parser.add_argument("--mode", choices=["gtid", "position"],
                        default="position")
    parser.add_argument("--cases", default=",".join(CASES),
                        help="comma-separated, run in the documented order")
    parser.add_argument("--flood-seconds", type=float, default=30.0)
    parser.add_argument("--flood-limit", type=int, default=65536,
                        help="log bytes per second a client may cause")
    args = parser.parse_args()
    args.mysql_basedir = args.mysql_basedir.expanduser()
    args.workdir = args.workdir.expanduser()
    args.workdir.mkdir(parents=True, exist_ok=True)
    gtid_mode = args.mode == "gtid"
    source = harness.SERVER = harness.MySQLServer(
        args.mysql_basedir, args.workdir / "mysql", args.port, gtid_mode,
        server_id=1)
    binsrv = harness.BINSRV = harness.BinlogServer(
        args.binsrv.expanduser(), args.workdir / "binsrv", args.port,
        gtid_mode)
    (args.workdir / "pull.stderr").unlink(missing_ok=True)
    pull = Pull(binsrv, args.port + 2, args.workdir / "pull.stderr")
    log = Log(binsrv.log_path)
    workload = Workload(source)
    ctx = {"args": args, "gtid_mode": gtid_mode, "source": source,
           "binsrv": binsrv, "pull": pull, "log": log, "workload": workload}
    results = []
    source.start()
    try:
        chosen = args.cases.split(",")
        for name in (name for name in CASES if name in chosen):
            try:
                # every case starts from a fresh source, storage and pull
                pull.stop()
                if source.process is None:
                    source.start()
                harness.FetchCrashMachine().start_from_scratch()
                # after start_from_scratch (BinlogServer.reset() clears it):
                # without checkpointing 'pull' writes what it received only
                # when its connection to the source ends (read_timeout), so
                # the sender would not see new events for up to 60 s
                binsrv.checkpoint_size = "1"
                workload.count = 0
                workload.run(20)
                pull.start()
                pull.sanitizer_report()
                wait_until(lambda: len(binsrv.indexed_binlogs()) > 0, 30,
                           "pull storing the first binlog")
                ctx["log_mark_before_case"] = log.mark()
                result = globals()["case_" + name](ctx)
                report = pull.sanitizer_report()
                if report:
                    result["ok"] = False
                    result["sanitizer"] = report
            except (harness.Failure, subprocess.SubprocessError, OSError,
                    RuntimeError, ValueError) as failure:
                result = {"case": name, "ok": False,
                          "why": harness.describe_failure(failure, 800)}
            result["mode"] = args.mode
            print(json.dumps(result), flush=True)
            results.append(result)
    finally:
        pull.stop()
        source.stop()
    failures = [result for result in results if not result.get("ok")]
    print(json.dumps({"cases": len(results), "failures": len(failures),
                      "passed": not failures}), flush=True)
    (args.workdir / "result.json").write_text(json.dumps(results, indent=2))
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
