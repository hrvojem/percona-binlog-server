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

"""What a client of the 'pull' listener can do after it has logged in.

Runs 'pull' on a small storage and talks to its listener with the raw client
of listener_fuzz_test.py. Every case logs in first, sends its commands and
then checks that 'pull' still runs, that a new client can log in within
--max-wait seconds and that the resident memory of 'pull' has grown by less
than --max-rss-mib since it started. An error packet or a closed session is
an acceptable answer to any of the inputs. Three groups of cases:

1. Deterministic commands (the names are those of the JSON lines):

- 'known_queries': every statement the listener knows, and the checksum
  variable read back before and after it is set.
- 'heartbeat_period_values': 'SET @master_heartbeat_period=' with zero, one,
  values around 2^63 and 2^64, 40 digits, signs, fractions, spaces.
- 'regex_long_text': the statements matched by the regular expressions of
  handshake_query_patterns.hpp with 1,000,000 characters after the matched
  prefix ('SET NAMES', '@slave_uuid', '@replica_uuid'), and long runs of
  spaces and digits in statements that do not start with SET.
- 'regex_short_runs': runs of 1,000 spaces, tabs, new lines and digits at
  every place a pattern has '\\s+', '\\s*' or '[0-9]+'.
- 'query_max_frame': a COM_QUERY of 0xFFFFFF bytes (the largest frame) of
  text no pattern matches, followed by an empty continuation frame. Until
  it is answered other clients log in; the longest wait of one of them must
  stay below --max-wait (every pattern is tried at every character of the
  statement, on the one thread that serves all clients).
- 'max_frames_repeated': five frames of 0xFFFFFF bytes with an unknown
  command byte on one connection.
- 'heartbeat_<period>ns_blocking_dump': a heartbeat period of 0, 1, 1,000
  and 2^63-1 ns, then a blocking COM_BINLOG_DUMP read for a few seconds.
  Once the stored events are sent the dump idles: the heartbeats received,
  the CPU time 'pull' used and the growth of its log are reported, and
  'pull' must not use more than --max-idle-cpu of one CPU (default 0.5).
- 'register_replica_truncated', 'register_replica_fields':
  COM_REGISTER_SLAVE cut at every length, with 255-byte fields, with length
  bytes that point past the end and with 3,000 trailing bytes.
- 'binlog_dump_no_checksum', 'binlog_dump_truncated',
  'binlog_dump_positions', 'binlog_dump_flags', 'binlog_dump_names':
  COM_BINLOG_DUMP without @source_binlog_checksum, cut at every length,
  from positions below 4, inside events, at and after the end of the file
  and at 2^32-1, with every flag, and with names that contain NUL, path
  separators, format specifiers or 3,000 bytes. Only the name of a stored
  binlog may be answered with events.
- 'binlog_dump_gtid_truncated', 'binlog_dump_gtid_lengths',
  'binlog_dump_gtid_positions', 'binlog_dump_gtid_names',
  'binlog_dump_gtid_sets': COM_BINLOG_DUMP_GTID cut at every length, with
  name and data lengths that point past the end, with absurd positions and
  names, and with GTID sets whose counts do not agree with the body (2^56-1
  TSIDs and no body, 2^64-1 intervals and one interval, an empty or reversed
  interval, a tag longer than 32 bytes, an unknown format byte, trailing
  bytes). The GTID set decoder is reached with --mode gtid only: a storage
  in position mode refuses the command before it looks at the set.
- 'change_user', 'change_user_capabilities': COM_CHANGE_USER to the same
  user, to another user, cut at every length and with garbage; the session
  must stay usable or be closed. Repeated for clients that logged in with
  other capabilities (connection attributes, no secure connection), which
  take other branches of the decoder.
- 'quit_then_data', 'ping', 'reset_connection', 'empty_packet',
  'sequence_ids', 'second_handshake', 'commands_during_dump',
  'command_bytes', 'command_bytes_with_body': COM_QUIT followed by more
  frames, 1,000 pipelined COM_PING, COM_RESET_CONNECTION, an empty frame,
  wrong sequence numbers, a client greeting and an SSL request after the
  login, commands sent while a dump is being streamed, and every command
  byte from 0x00 to 0xFF alone and with 16 random bytes.

2. 'fuzz': --connections (default 300) seeded random sessions; each logs
   in, declares the checksum (most do) and sends one to five commands built
   from the valid encodings above, most of them mutated (cut, extended, bytes replaced, length fields set to
   extremes, wrong sequence numbers). After every connection 'pull' must
   still run; on a crash the bytes sent are written to '<workdir>/crash.bin'
   and the exit code or signal and the end of the standard error and log of
   'pull' are reported. Runs of white space or digits stay below 2,000
   characters here, so that ticket 72 does not hide other findings.

3. Last, with a restart of 'pull' after every crash (skipped with
   --no-long-runs): 'regex_run_threshold', which looks for the shortest run
   of digits in 'SET @master_heartbeat_period=<digits>' that ends 'pull',
   and 'regex_run_<place>', a run of 100,000 characters at each '\\s+',
   '\\s*' and '[0-9]+' of the patterns. These are the known ticket 72
   (std::regex recurses once per character and overflows the stack); their
   lines carry "known": "ticket 72".

A JSON line per case, then a summary; exit 1 if any case failed. When 'pull'
ends in a case, what its last client sent after the login is written to
'<workdir>/crash-<case>.bin' (and 'crash.bin') and the standard error of
'pull' to 'crash-<case>.stderr'. Run against the AddressSanitizer build to
catch memory errors. Example:

  PYTHONPATH=~/ws/qa/pylib python3 listener_command_fuzz_test.py \\
    --mysql-basedir ~/ws/dist/mysql-8.4.8-linux-glibc2.28-x86_64-minimal \\
    --binsrv ~/ws/percona-binlog-server-build-asan_gcc14/binlog_server \\
    --workdir ~/ws/qa/listener-command-fuzz --mode gtid
"""

import argparse
import json
import os
import random
import re
import select
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
import listener_login_test as login  # noqa: E402
from listener_fuzz_test import Listener, frame, greeting_payload  # noqa: E402

# one list per binlog: the source rotates after each but the last
WORKLOADS = [[{"rows": [200, 2000], "ddl": True}],
             [{"rows": [300, 40_000], "ddl": False}],
             [{"rows": [50], "ddl": False}]]

COM_QUIT = 0x01
COM_QUERY = listener.COM_QUERY
COM_PING = 0x0e
COM_CHANGE_USER = 0x11
COM_BINLOG_DUMP = listener.COM_BINLOG_DUMP
COM_REGISTER_SLAVE = 0x15
COM_BINLOG_DUMP_GTID = listener.COM_BINLOG_DUMP_GTID
COM_RESET_CONNECTION = 0x1f

CLIENT_CONNECT_ATTRS = 0x100000

NON_BLOCKING = 0x01          # BINLOG_DUMP_NON_BLOCK
HEARTBEAT_EVENTS = (27, 41)  # HEARTBEAT_LOG_EVENT, HEARTBEAT_LOG_EVENT_V2
MAX_FRAME = 0xFFFFFF
CHECKSUM_ON = (b"SET @source_binlog_checksum = 'CRC32', "
               b"@master_binlog_checksum = 'CRC32'")
KNOWN_QUERIES = [
    b"select * from tbl", b"select @@version_comment limit 1",
    b"SELECT VERSION()", b"SELECT UNIX_TIMESTAMP()",
    b"SELECT @@GLOBAL.SERVER_ID", b"SELECT @@GLOBAL.SERVER_UUID",
    b"SELECT @@GLOBAL.GTID_MODE", b"SELECT @source_binlog_checksum",
    b"SELECT @master_binlog_checksum", CHECKSUM_ON,
    b"SET @master_binlog_checksum = 'NONE', @source_binlog_checksum = 'NONE'",
    b"SET @master_binlog_checksum = @@global.binlog_checksum, "
    b"@source_binlog_checksum = @@global.binlog_checksum",
    b"SET NAMES utf8mb4", b"SET @slave_uuid = 'a'", b"SET @replica_uuid = 'a'",
    b"SET @master_heartbeat_period = 30000000000, "
    b"@source_heartbeat_period = 30000000000"]
UUID = bytes(range(0x10, 0x20))
# the longest run of white space or digits the fuzz phase sends: far below
# what ends pull through ticket 72
FUZZ_MAX_RUN = 2000
LONG_RUN = re.compile(rb"[ \t\n\v\f\r]{%d,}|[0-9]{%d,}" %
                      (FUZZ_MAX_RUN + 1, FUZZ_MAX_RUN + 1))
TICKET_72 = "ticket 72"


class PullDied(Exception):
    """'pull' ended while a case was talking to it."""


# ---------------------------------------------------------------------------
# encoders of the commands
# ---------------------------------------------------------------------------

def com_query(statement: bytes) -> bytes:
    return bytes([COM_QUERY]) + statement


def com_binlog_dump(position: int, flags: int, server_id: int,
                    name: bytes) -> bytes:
    return (bytes([COM_BINLOG_DUMP]) + struct.pack("<IHI", position, flags,
                                                   server_id) + name)


def com_binlog_dump_gtid(flags: int, server_id: int, name: bytes,
                         position: int, data: bytes,
                         name_length: "int | None" = None,
                         data_length: "int | None" = None) -> bytes:
    """The length fields can lie: 'name_length' and 'data_length' replace
    the real ones."""
    return (bytes([COM_BINLOG_DUMP_GTID]) + struct.pack("<HI", flags,
                                                        server_id) +
            struct.pack("<I", len(name) if name_length is None
                        else name_length) + name +
            struct.pack("<Q", position) +
            struct.pack("<I", len(data) if data_length is None
                        else data_length) + data)


def com_register_replica(server_id: int = 77, host: bytes = b"replica-host",
                         user: bytes = b"repl", password: bytes = b"secret",
                         port: int = 3306, rank: int = 0,
                         master_id: int = 0) -> bytes:
    return (bytes([COM_REGISTER_SLAVE]) + struct.pack("<I", server_id) +
            bytes([len(host) & 0xFF]) + host +
            bytes([len(user) & 0xFF]) + user +
            bytes([len(password) & 0xFF]) + password +
            struct.pack("<HII", port, rank, master_id))


def com_change_user(user: bytes, auth: bytes, schema: bytes = b"",
                    plugin: bytes = b"caching_sha2_password",
                    attributes: "bytes | None" = None,
                    secure_connection: bool = True) -> bytes:
    body = bytes([COM_CHANGE_USER]) + user + b"\x00"
    body += (bytes([len(auth) & 0xFF]) + auth if secure_connection
             else auth + b"\x00")
    body += schema + b"\x00" + struct.pack("<H", 0xff) + plugin + b"\x00"
    if attributes is not None:
        body += listener.lenenc(len(attributes)) + attributes
    return body


def gtid_set(tsids, tagged: bool = False,
             count: "int | None" = None) -> bytes:
    """An encoded GTID set. 'tsids' is a list of (uuid, tag, intervals,
    number of intervals or None for the real one); 'count' replaces the
    number of TSIDs in the header."""
    number = len(tsids) if count is None else count
    if tagged:
        data = b"\x01" + struct.pack("<Q", number)[:6] + b"\x01"
    else:
        data = struct.pack("<Q", number)
    for uuid, tag, intervals, interval_count in tsids:
        data += uuid
        if tagged:
            # the length of the tag as a variable-length integer: one byte
            # below 128, two bytes below 16,384
            data += (bytes([len(tag) << 1]) if len(tag) < 128 else
                     struct.pack("<H", (len(tag) << 2) | 1)) + tag
        data += struct.pack("<Q", len(intervals) if interval_count is None
                            else interval_count)
        for lower, upper in intervals:
            data += struct.pack("<QQ", lower, upper)
    return data


# ---------------------------------------------------------------------------
# reading the answers
# ---------------------------------------------------------------------------

def read_frame(client: Listener, timeout: float = 5.0):
    """(sequence, body) of the next frame, or 'closed' or 'timeout'."""
    def receive(count: int):
        data = bytearray()
        while len(data) < count:
            try:
                piece = client.sock.recv(count - len(data))
            except socket.timeout:
                return "timeout"
            except OSError:
                return "closed"
            if not piece:
                return "closed"
            data += piece
        return bytes(data)

    client.sock.settimeout(timeout)
    header = receive(listener.HEADER_LENGTH)
    if isinstance(header, str):
        return header
    body = receive(int.from_bytes(header[:3], "little"))
    return body if isinstance(body, str) else (header[3], body)


def is_eof(body: bytes) -> bool:
    return body[:1] == b"\xfe" and len(body) < 9


def kind(read) -> str:
    """What a frame is, for a result line."""
    if isinstance(read, str):
        return read
    body = read[1]
    if body[:1] == b"\xff":
        return (f"ERR {int.from_bytes(body[1:3], 'little')} "
                f"{body[9:69].decode(errors='replace')}")
    if is_eof(body):
        return "EOF"
    if body[:1] == b"\x00" and len(body) >= 20:
        return f"event {body[5]}"
    if body[:1] == b"\x00":
        return "OK"
    return "bytes " + body[:8].hex()


def command(client: Listener, payload: bytes, sequence: int = 0,
            timeout: float = 5.0) -> str:
    """Sends one command and returns the kind of its answer; the frames of
    a result set are read to its end and reported as 'resultset'."""
    client.send(frame(sequence, payload))
    read = read_frame(client, timeout)
    answer = kind(read)
    if not answer.startswith("bytes "):
        return answer
    ends = 0
    for _ in range(64):
        read = read_frame(client, timeout)
        if isinstance(read, str) or read[1][:1] == b"\xff":
            return "resultset cut short: " + kind(read)
        if is_eof(read[1]):
            ends += 1
            if ends == 2:
                return "resultset"
    return "resultset without an end"


def read_stream(client: Listener, seconds: float,
                quiet: "float | None" = None,
                answers: "dict | None" = None) -> dict:
    """Reads what a dump sends for 'seconds' (or until nothing came for
    'quiet' seconds) and counts it. It stops at the first frame that is not
    an event; with 'answers' it goes on to the end of the session and
    counts the kinds of those frames there."""
    counts = {"events": 0, "heartbeats": 0, "bytes": 0, "end": "still open"}
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        read = read_frame(client, quiet if quiet is not None else
                          max(0.05, deadline - time.monotonic()))
        if read == "timeout":
            if quiet is not None:
                break
            continue
        if isinstance(read, str):
            counts["end"] = read
            break
        body = read[1]
        counts["bytes"] += len(body) + listener.HEADER_LENGTH
        if body[:1] == b"\x00" and len(body) >= 20:
            counts["heartbeats" if body[5] in HEARTBEAT_EVENTS
                   else "events"] += 1
        elif answers is not None:
            answer = " ".join(kind(read).split(" ")[:2])
            answers[answer] = answers.get(answer, 0) + 1
        else:
            counts["end"] = kind(read)
            break
    return counts


def served_events(answer: dict) -> bool:
    return answer["events"] > 0


# ---------------------------------------------------------------------------
# pull and its state
# ---------------------------------------------------------------------------

class Context:
    """'pull', its listener and what the cases need to know about them."""

    def __init__(self, args, binsrv):
        self.args = args
        self.binsrv = binsrv
        self.port = args.port + 2
        self.pull = None
        self.stderr = bytearray()
        self.stderr_reader = None
        self.baseline_rss = 0
        self.last_client = None
        self.last_label = "the start of pull"
        self.clock_ticks = os.sysconf("SC_CLK_TCK")
        pattern = re.compile(
            rf"^{re.escape(harness.BINLOG_BASE_NAME)}\.\d{{6}}$")
        self.binlogs = sorted(
            (path.name.encode(), path.stat().st_size)
            for path in binsrv.storage.iterdir() if pattern.match(path.name))

    def ensure_pull(self):
        if self.pull is not None and self.pull.poll() is None:
            return
        self.pull = login.start_pull(self.binsrv, self.port)
        # the standard error of pull is read all the time: a sanitizer
        # report is longer than a pipe holds, and pull would block on it
        self.stderr = collected = bytearray()
        stream = self.pull.stderr

        def collect():
            for piece in iter(lambda: stream.read1(65536), b""):
                collected.extend(piece)
            stream.close()
        self.stderr_reader = threading.Thread(target=collect, daemon=True)
        self.stderr_reader.start()
        # the first login does the full authentication and puts the user
        # into the authentication cache: later logins are answered at once
        if not login.logs_in(self.port):
            raise harness.Failure("no login possible on a fresh pull\n" +
                                  self.binsrv.log_tail())
        self.baseline_rss = self.rss()

    def alive(self) -> bool:
        return self.pull.poll() is None

    def rss(self) -> int:
        """The resident memory of pull in kB (0 if it cannot be read)."""
        try:
            status = Path(f"/proc/{self.pull.pid}/status").read_text()
        except OSError:
            return 0
        found = re.search(r"^VmRSS:\s+(\d+) kB", status, re.MULTILINE)
        return int(found.group(1)) if found else 0

    def cpu_seconds(self) -> float:
        """The user and system CPU time pull has used so far."""
        try:
            stat = Path(f"/proc/{self.pull.pid}/stat").read_text()
        except OSError:
            return 0.0
        fields = stat.rsplit(")", 1)[1].split()
        return (int(fields[11]) + int(fields[12])) / self.clock_ticks

    def connect(self, timeout: float = 5.0) -> Listener:
        client = Listener("127.0.0.1", self.port, timeout=timeout)
        self.last_client = client
        return client

    def session(self, capabilities: "int | None" = None,
                timeout: float = 5.0, checksum: bool = False) -> Listener:
        """A logged-in client; with 'capabilities', one that sent them in
        its greeting; with 'checksum', one that has declared
        @source_binlog_checksum, as a dump needs."""
        previous = self.last_client
        client = self.connect(timeout)
        if capabilities is None:
            logged_in = client.handshake()
        else:
            logged_in = handshake_with(client, capabilities)
        if not logged_in:
            client.close()
            time.sleep(0.3)
            if not self.alive():
                # it was the client before this one that ended pull
                self.last_client = previous
                raise PullDied(f"{self.last_label} (seen at the next login)")
            raise harness.Failure("could not log in")
        client.sent.clear()  # crash.bin starts after the login
        if checksum and command(client, com_query(CHECKSUM_ON)) != "OK":
            client.close()
            raise harness.Failure("the checksum declaration was refused")
        return client

    def check(self, label: str):
        """After an input: pull must still run."""
        self.last_label = label
        if not self.alive():
            raise PullDied(label)

    def settle(self, label: str):
        """Waits for pull to have processed what was sent on a connection
        that is closed by now, then checks that it still runs."""
        self.last_label = label
        try:
            probe = Listener("127.0.0.1", self.port, timeout=10.0)
            probe.read_frame()
            probe.close()
        except OSError:
            pass
        time.sleep(0.2)
        if not self.alive():
            raise PullDied(label)

    def login_seconds(self, timeout: float) -> "float | None":
        """How long a new client takes to log in; None if it cannot."""
        started = time.monotonic()
        try:
            client = Listener("127.0.0.1", self.port, timeout=timeout)
        except OSError:
            return None
        try:
            return (round(time.monotonic() - started, 3)
                    if client.handshake() else None)
        finally:
            client.close()

    def death(self) -> str:
        """How pull ended, with the end of its standard error and log."""
        code = self.pull.wait()
        how = (f"signal {-code} ({signal.Signals(-code).name})" if code < 0
               else f"exit {code}")
        self.stderr_reader.join(10)
        stderr = bytes(self.stderr).decode(errors="replace")
        # a sanitizer report starts with its error and the top of the stack
        if len(stderr) > 2400:
            stderr = stderr[:1600] + "\n...\n" + stderr[-800:]
        return (f"pull ended with {how}; stderr: {stderr!r}; log: "
                f"{self.binsrv.log_tail(6)[-900:]!r}")


def handshake_with(client: Listener, capabilities: int) -> bool:
    """A login with other capabilities than those of Listener.handshake():
    the fast authentication only, so a client must have logged in before."""
    salt = client.server_salt()
    if salt is None:
        return False
    payload = greeting_payload(
        capabilities, listener.PULL_USER.encode(),
        listener.scramble(listener.PULL_PASSWORD, salt),
        b"caching_sha2_password")
    if capabilities & CLIENT_CONNECT_ATTRS:
        attributes = b"\x04_pid\x0212"
        payload += listener.lenenc(len(attributes)) + attributes
    client.send(frame(1, payload))
    for _ in range(3):
        read = client.read_frame()
        if read is None or read[1][:1] == b"\xff":
            return False
        if read[1][:1] == b"\x00":
            return True
    return False


def write_crash(ctx: Context, name: str):
    """The bytes the last client of a case sent after its login."""
    if ctx.last_client is None:
        return
    sent = bytes(ctx.last_client.sent)
    (ctx.args.workdir / "crash.bin").write_bytes(sent)
    (ctx.args.workdir / f"crash-{name}.bin").write_bytes(sent)


def run_case(ctx: Context, name: str, function, known: "str | None" = None
             ) -> dict:
    """Runs one case and the checks that follow every case."""
    result = {"case": name}
    if known:
        result["known"] = known
    problems = []
    try:
        ctx.ensure_pull()
        ctx.last_client = None
        details = function(ctx) or {}
        problems += details.pop("problems", [])
        result.update(details)
    except PullDied as died:
        problems.append(f"pull ended after: {died}")
    except (harness.Failure, subprocess.SubprocessError, OSError,
            ValueError) as failure:
        problems.append(harness.describe_failure(failure, 500))
    if ctx.pull is None:
        result.update(ok=False, why="; ".join(problems))
        return result
    time.sleep(0.1)
    if ctx.alive():
        waited = ctx.login_seconds(max(30.0, 3 * ctx.args.max_wait))
        # a crash may show only now, when pull gets to the bytes
        if not ctx.alive():
            waited = None
        elif waited is None:
            problems.append("no login possible after the case")
        elif waited > ctx.args.max_wait:
            problems.append(f"a login after the case took {waited} s")
        result["login_seconds"] = waited
    if ctx.alive():
        growth = (ctx.rss() - ctx.baseline_rss) // 1024
        result["rss_growth_mib"] = growth
        if growth > ctx.args.max_rss_mib:
            problems.append(f"the memory of pull grew by {growth} MiB")
    else:
        write_crash(ctx, name)
        problems.append(ctx.death())
        (ctx.args.workdir / f"crash-{name}.stderr").write_bytes(
            bytes(ctx.stderr))
        if ctx.last_client is not None:
            result["crash_bytes"] = len(ctx.last_client.sent)
            result["crash_file"] = f"crash-{name}.bin"
    result["ok"] = not problems
    if problems:
        result["why"] = "; ".join(problems)
    return result


def one_shot(ctx: Context, label: str, payload: bytes, sequence: int = 0,
             capabilities: "int | None" = None, checksum: bool = False,
             stream: bool = False):
    """A new session, one command, its answer (for 'stream', a count of
    what the dump sent in up to 1.5 s); pull must survive it."""
    client = ctx.session(capabilities, checksum=checksum)
    try:
        if stream:
            client.send(frame(sequence, payload))
            answer = read_stream(client, 1.5)
        else:
            answer = command(client, payload, sequence)
    finally:
        client.close()
    ctx.check(label)
    return answer


def tally(answers: dict) -> dict:
    """{answer: how many inputs got it}, for cases with many inputs."""
    counts: dict = {}
    for answer in answers.values():
        answer = answer if isinstance(answer, str) else json.dumps(answer)
        counts[answer] = counts.get(answer, 0) + 1
    return counts


# ---------------------------------------------------------------------------
# the deterministic cases
# ---------------------------------------------------------------------------

def case_known_queries(ctx: Context) -> dict:
    client = ctx.session()
    problems = []
    answers = {}
    try:
        for statement in KNOWN_QUERIES:
            answers[statement.decode()[:40]] = answer = command(
                client, com_query(statement))
            if answer not in ("OK", "resultset"):
                problems.append(f"{statement!r} was answered with {answer}")
        for statement in (b"set names utf8mb4", b"SELECT version()", b"",
                          b"\x00", b"SET", b"SET ", b"SET NAMESx",
                          b"SET @slave_uuidx", b"SET @master_heartbeat_period",
                          b"SET @master_heartbeat_period=",
                          b" SET NAMES x", b"SET\x00NAMES x"):
            answers[repr(statement)] = command(client, com_query(statement))
    finally:
        client.close()
    ctx.check("the known statements")
    return {"answers": answers, "problems": problems}


def case_heartbeat_period_values(ctx: Context) -> dict:
    values = [b"0", b"1", b"999", b"9223372036854775807",
              b"9223372036854775808", b"18446744073709551615",
              b"18446744073709551616", b"9" * 40, b"0" * 40 + b"1", b"-1",
              b"+1", b"1.5", b"1e9", b"0x10", b" 7 ", b"'7'", b"",
              b"\xef\xbc\x91", b"7, @source_heartbeat_period = 9"]
    client = ctx.session()
    answers = {}
    try:
        for value in values:
            for variable in (b"master", b"source"):
                statement = (b"SET @" + variable + b"_heartbeat_period = " +
                             value)
                answers[statement.decode(errors="replace")[5:]] = command(
                    client, com_query(statement))
                ctx.check(repr(statement))
    finally:
        client.close()
    return {"answers": answers}


def case_regex_long_text(ctx: Context) -> dict:
    million = 1_000_000
    statements = {
        "SET NAMES + 1,000,000 letters": b"SET NAMES " + b"a" * million,
        "SET NAMES + 1,000,000 spaces": b"SET NAMES" + b" " * million,
        "SET @slave_uuid= + 1,000,000 letters":
            b"SET @slave_uuid='" + b"a" * million + b"'",
        "SET @replica_uuid + 1,000,000 digits":
            b"SET @replica_uuid=" + b"7" * million,
        "SELECT + 1,000,000 spaces": b"SELECT" + b" " * million,
        "1,000,000 spaces + SET NAMES x": b" " * million + b"SET NAMES x",
        "1,000,000 digits": b"1" * million,
        "SET@ + 1,000,000 spaces": b"SET@" + b" " * million,
        "250,000 times 'SET '": b"SET " * 250_000}
    answers = {}
    for label, statement in statements.items():
        answers[label] = one_shot(ctx, label, com_query(statement))
    return {"answers": answers}


def regex_run_statements(run: int) -> dict:
    """A run of 'run' characters at every '\\s+', '\\s*' and '[0-9]+' of the
    patterns of handshake_query_patterns.hpp."""
    return {
        "tabs_after_set_names": b"SET" + b"\t" * run + b"NAMES x",
        "spaces_after_set_heartbeat":
            b"SET" + b" " * run + b"@source_heartbeat_period=1",
        "spaces_before_equals":
            b"SET @master_heartbeat_period" + b" " * run + b"=1",
        "spaces_after_equals":
            b"SET @source_heartbeat_period=" + b" " * run + b"1",
        "heartbeat_digits": b"SET @master_heartbeat_period=" + b"1" * run,
        "newlines_before_slave_uuid":
            b"set" + b"\n" * run + b"@slave_uuid='a'",
        "spaces_before_replica_uuid":
            b"SET" + b" " * run + b"@replica_uuid='a'",
        "spaces_then_no_match": b"SET" + b"\r" * run + b"x"}


def case_regex_short_runs(ctx: Context) -> dict:
    client = ctx.session()
    answers = {}
    try:
        for label, statement in regex_run_statements(1000).items():
            answers[label] = command(client, com_query(statement))
            ctx.check(label + " with a run of 1,000")
    finally:
        client.close()
    return {"answers": answers}


def max_frame_case(ctx: Context, payload: bytes) -> dict:
    """One frame of 0xFFFFFF bytes and its empty continuation; until it is
    answered other clients log in, and the longest wait of one of them is
    the time pull was busy with the frame."""
    assert len(payload) == MAX_FRAME
    client = ctx.session(timeout=60.0)
    try:
        started = time.monotonic()
        client.send(frame(0, payload) + frame(1, b""))
        waited = 0.0
        while waited is not None and time.monotonic() - started < 300:
            login_time = ctx.login_seconds(300.0)
            waited = None if login_time is None else max(waited, login_time)
            if select.select([client.sock], [], [], 0)[0]:
                break
        first = kind(read_frame(client, 300.0))
        answered = round(time.monotonic() - started, 2)
        second = kind(read_frame(client, 15.0))
    finally:
        client.close()
        client.sent = client.sent[:64]  # not 16 MB in a crash file
    ctx.check("a frame of 0xFFFFFF bytes")
    problems = []
    if waited is None:
        problems.append("no login possible while pull worked on the frame")
    elif waited > ctx.args.max_wait:
        problems.append(f"one COM_QUERY of 0xFFFFFF bytes kept every other "
                        f"client waiting for {waited} s")
    return {"answer": first, "answered_after_seconds": answered,
            "after_continuation": second,
            "other_client_waited_seconds": waited, "problems": problems}


def case_query_max_frame(ctx: Context) -> dict:
    return max_frame_case(ctx, com_query(b"x" * (MAX_FRAME - 1)))


def case_max_frames_repeated(ctx: Context) -> dict:
    client = ctx.session(timeout=60.0)
    answers = []
    try:
        payload = b"\x00" + b"\xa5" * (MAX_FRAME - 1)
        for _ in range(5):
            answers.append(command(client, payload, timeout=60.0))
            if answers[-1] in ("closed", "timeout"):
                break
    finally:
        client.close()
        client.sent = client.sent[:64]
    ctx.check("five frames of 0xFFFFFF bytes")
    return {"answers": tally(dict(enumerate(answers)))}


def heartbeat_dump_case(period: int):
    def case(ctx: Context) -> dict:
        seconds = 4.0
        client = ctx.session(timeout=5.0, checksum=True)
        problems = []
        try:
            set_answer = command(client, com_query(
                b"SET @master_heartbeat_period = %d, "
                b"@source_heartbeat_period = %d" % (period, period)))
            # blocking: no flag and a server id other than 0
            client.send(frame(0, com_binlog_dump(4, 0, 4711, b"")))
            started = time.monotonic()
            counts = read_stream(client, 1.0)
            # from here on the dump has sent the stored events and idles
            idle_started = time.monotonic()
            cpu = ctx.cpu_seconds()
            log_size = ctx.binsrv.log_path.stat().st_size
            waited = ctx.login_seconds(30.0)
            rest = read_stream(client, max(
                0.1, seconds - (time.monotonic() - started)))
            for key in ("events", "heartbeats", "bytes"):
                counts[key] += rest[key]
            counts["end"] = rest["end"]
            elapsed = time.monotonic() - started
            idle = time.monotonic() - idle_started
            cpu = ctx.cpu_seconds() - cpu
            log_size = ctx.binsrv.log_path.stat().st_size - log_size
        finally:
            client.close()
        ctx.check(f"a blocking dump with a heartbeat period of {period} ns")
        if waited is None:
            problems.append("no login possible during the dump")
        elif waited > ctx.args.max_wait:
            problems.append(f"a login during the dump took {waited} s")
        if counts["events"] == 0:
            problems.append("the dump sent no events")
        if cpu > ctx.args.max_idle_cpu * idle:
            problems.append(
                f"with a heartbeat period of {period} ns one idle dump kept "
                f"pull busy for {cpu:.1f} s of CPU time in {idle:.1f} s "
                f"({counts['heartbeats']} heartbeats, {log_size} bytes of "
                "log)")
        return {"set": set_answer, "dump": counts,
                "read_seconds": round(elapsed, 1),
                "heartbeats_per_second": round(counts["heartbeats"] /
                                               elapsed),
                "idle_seconds": round(idle, 1),
                "pull_cpu_seconds_while_idle": round(cpu, 2),
                "log_bytes_while_idle": log_size,
                "login_during_dump_seconds": waited, "problems": problems}
    return case


def truncations(ctx: Context, what: str, payload: bytes,
                checksum: bool = False) -> dict:
    """The command cut at every length from its command byte on."""
    answers = {}
    for length in range(1, len(payload)):
        answers[length] = one_shot(ctx, f"{what} cut to {length} bytes",
                                   payload[:length], checksum=checksum)
    return {"lengths": len(payload) - 1, "answers": tally(answers)}


def case_register_replica_truncated(ctx: Context) -> dict:
    return truncations(ctx, "COM_REGISTER_SLAVE", com_register_replica())


def case_register_replica_fields(ctx: Context) -> dict:
    full = b"h" * 255
    valid = com_register_replica()
    inputs = {
        "valid": valid,
        "255-byte fields": com_register_replica(0xFFFFFFFF, full, full, full,
                                                0xFFFF, 0xFFFFFFFF,
                                                0xFFFFFFFF),
        "empty fields": com_register_replica(0, b"", b"", b""),
        "host length 255, body ends": valid[:5] + b"\xff" + b"short",
        "user length 255, body ends":
            valid[:5] + b"\x01h\xff" + b"u" * 20,
        "password length 255, body ends":
            valid[:5] + b"\x01h\x01u\xff" + b"p" * 254,
        "3,000 trailing bytes": valid + b"\x00" * 3000,
        "NUL and new line in the names": com_register_replica(
            1, b"a\x00b\n", b"../../x", b"{}%s\r\n")}
    answers = {label: one_shot(ctx, "COM_REGISTER_SLAVE, " + label, payload)
               for label, payload in inputs.items()}
    problems = [] if answers["valid"] == "OK" else [
        f"a valid COM_REGISTER_SLAVE was answered with {answers['valid']}"]
    # a registered replica goes on with other commands
    client = ctx.session()
    try:
        both = [command(client, valid), command(client, valid),
                command(client, bytes([COM_PING]))]
    finally:
        client.close()
    ctx.check("COM_REGISTER_SLAVE twice")
    return {"answers": answers, "twice_then_ping": both,
            "problems": problems}


def case_binlog_dump_no_checksum(ctx: Context) -> dict:
    first = ctx.binlogs[0][0]
    answers = {
        "COM_BINLOG_DUMP": one_shot(
            ctx, "a dump without a checksum declaration",
            com_binlog_dump(4, NON_BLOCKING, 5, first), stream=True),
        "COM_BINLOG_DUMP_GTID": one_shot(
            ctx, "a GTID dump without a checksum declaration",
            com_binlog_dump_gtid(NON_BLOCKING, 5, b"", 4, gtid_set([])),
            stream=True)}
    problems = [f"{label} was served without @source_binlog_checksum"
                for label, answer in answers.items() if served_events(answer)]
    return {"answers": answers, "problems": problems}


def case_binlog_dump_truncated(ctx: Context) -> dict:
    return truncations(ctx, "COM_BINLOG_DUMP", com_binlog_dump(
        4, NON_BLOCKING, 5, ctx.binlogs[0][0])[:12], checksum=True)


def case_binlog_dump_positions(ctx: Context) -> dict:
    name, size = ctx.binlogs[0]
    last_name, last_size = ctx.binlogs[-1]
    positions = [0, 1, 3, 4, 5, 19, 23, 100, 125, 126, 127, 130, 1000, 4096,
                 size - 1, size, size + 1, 0x7FFFFFFF, 0xFFFFFFFF]
    answers = {}
    for position in positions:
        answers[f"{name.decode()}:{position}"] = one_shot(
            ctx, f"a dump from position {position}",
            com_binlog_dump(position, NON_BLOCKING, 5, name), checksum=True,
            stream=True)
    for position in (last_size - 1, last_size, last_size + 1, 0xFFFFFFFF):
        for flags in (NON_BLOCKING, 0):
            answers[f"{last_name.decode()}:{position} flags {flags}"] = \
                one_shot(ctx, f"a dump of the last binlog from position "
                         f"{position}, flags {flags}",
                         com_binlog_dump(position, flags, 5, last_name),
                         checksum=True, stream=True)
    return {"answers": answers}


def case_binlog_dump_flags(ctx: Context) -> dict:
    name = ctx.binlogs[-1][0]
    answers = {}
    for flags in (0x0000, 0x0001, 0x0002, 0x0003, 0x0004, 0x0100, 0x8000,
                  0xFFFE, 0xFFFF):
        for server_id in (0, 5, 0xFFFFFFFF):
            answers[f"flags {flags:#06x} server id {server_id}"] = one_shot(
                ctx, f"a dump with flags {flags:#x}, server id {server_id}",
                com_binlog_dump(4, flags, server_id, name), checksum=True,
                stream=True)["end"]
    return {"answers": tally(answers)}


def hostile_names(ctx: Context) -> list:
    name = ctx.binlogs[0][0]
    base = harness.BINLOG_BASE_NAME.encode()
    storage = str(ctx.binsrv.storage).encode()
    return [b"\x00", name + b"\x00", b"\x00" + name, name[:-1], name + b"0",
            name + b"/", b"./" + name, b"../storage/" + name,
            storage + b"/" + name, b"/etc/passwd", b"../../../etc/passwd",
            b"/etc/passwd.000001", b"../../../../etc/hostname.000001",
            b"/proc/self/environ.000001", base + b".index",
            b"metadata.json", base + b".000000", base + b".999999",
            base + b".4294967295", base + b".4294967296", base + b".-00001",
            base + b".+00001", base + b". 00001", base + b".0x0001",
            base + b".00000" + "١".encode(), b".000001", b"000001",
            b"." * 7, b"{}{}{}.000001", b"%s%n%s.000001",
            b"a\nb\r\n2026 [error] forged.000001", b"\xff\xfe\xfd.000001",
            b"a" * 3000, b"a" * 3000 + b".000001", b"/" * 3000 + b".000001",
            name * 200]


def case_binlog_dump_names(ctx: Context) -> dict:
    answers = {}
    problems = []
    stored = {name for name, _ in ctx.binlogs}
    for name in hostile_names(ctx) + [ctx.binlogs[0][0], b""]:
        label = repr(name if len(name) < 60 else
                     name[:24] + b"..." + name[-12:])
        answer = one_shot(ctx, f"a dump of the binlog named {label}",
                          com_binlog_dump(4, NON_BLOCKING, 5, name),
                          checksum=True, stream=True)
        answers[label] = (f"{answer['events']} events, {answer['end']}")
        if name and name not in stored and served_events(answer):
            problems.append(f"events were served for the name {label}")
        if (not name or name in stored) and not served_events(answer):
            problems.append(f"no events were served for the name {label}")
    return {"answers": answers, "problems": problems}


def valid_gtid_dump(ctx: Context) -> bytes:
    return com_binlog_dump_gtid(NON_BLOCKING, 5, ctx.binlogs[0][0], 4,
                                gtid_set([(UUID, b"", [(1, 3)], None)]))


def case_binlog_dump_gtid_truncated(ctx: Context) -> dict:
    return truncations(ctx, "COM_BINLOG_DUMP_GTID", valid_gtid_dump(ctx),
                       checksum=True)


def case_binlog_dump_gtid_lengths(ctx: Context) -> dict:
    name = ctx.binlogs[0][0]
    data = gtid_set([(UUID, b"", [(1, 3)], None)])
    answers = {}
    for label, payload in {
            "name length 2^32-1": com_binlog_dump_gtid(
                NON_BLOCKING, 5, name, 4, data, name_length=0xFFFFFFFF),
            "name length 2^31": com_binlog_dump_gtid(
                NON_BLOCKING, 5, name, 4, data, name_length=0x80000000),
            "name length one too long": com_binlog_dump_gtid(
                NON_BLOCKING, 5, name, 4, data, name_length=len(name) + 1),
            "name length covers the whole rest": com_binlog_dump_gtid(
                NON_BLOCKING, 5, name, 4, data,
                name_length=len(name) + 12 + len(data)),
            "name length 0 with a name": com_binlog_dump_gtid(
                NON_BLOCKING, 5, name, 4, data, name_length=0),
            "data length 2^32-1": com_binlog_dump_gtid(
                NON_BLOCKING, 5, name, 4, data, data_length=0xFFFFFFFF),
            "data length one too long": com_binlog_dump_gtid(
                NON_BLOCKING, 5, name, 4, data, data_length=len(data) + 1),
            "data length one too short": com_binlog_dump_gtid(
                NON_BLOCKING, 5, name, 4, data, data_length=len(data) - 1),
            "data length 0 with data": com_binlog_dump_gtid(
                NON_BLOCKING, 5, name, 4, data, data_length=0),
            "no data at all": com_binlog_dump_gtid(
                NON_BLOCKING, 5, b"", 4, b""),
            "3,000 trailing bytes": com_binlog_dump_gtid(
                NON_BLOCKING, 5, name, 4, data) + b"\x00" * 3000}.items():
        answers[label] = one_shot(ctx, "COM_BINLOG_DUMP_GTID, " + label,
                                  payload, checksum=True, stream=True)["end"]
    return {"answers": answers}


def case_binlog_dump_gtid_positions(ctx: Context) -> dict:
    name, size = ctx.binlogs[0]
    data = gtid_set([])
    answers = {}
    for position in (0, 3, 4, 5, 126, size, size + 1, 1 << 32, 1 << 63,
                     (1 << 64) - 1):
        for used_name in (name, b""):
            for flags in (NON_BLOCKING, 0xFFFF):
                label = (f"{used_name.decode()}:{position} "
                         f"flags {flags:#x}")
                answers[label] = one_shot(
                    ctx, "a GTID dump from " + label, com_binlog_dump_gtid(
                        flags, 5, used_name, position, data),
                    checksum=True, stream=True)["end"]
    return {"answers": tally(answers)}


def case_binlog_dump_gtid_names(ctx: Context) -> dict:
    answers = {}
    problems = []
    stored = {name for name, _ in ctx.binlogs}
    for name in hostile_names(ctx):
        label = repr(name if len(name) < 60 else
                     name[:24] + b"..." + name[-12:])
        answer = one_shot(ctx, f"a GTID dump of the binlog named {label}",
                          com_binlog_dump_gtid(NON_BLOCKING, 5, name, 4,
                                               gtid_set([])),
                          checksum=True, stream=True)
        answers[label] = (f"{answer['events']} events, {answer['end']}")
        if name not in stored and served_events(answer):
            problems.append(f"events were served for the name {label}")
    return {"answers": tally(answers), "problems": problems}


def hostile_gtid_sets() -> dict:
    top = (1 << 64) - 1
    one = [(UUID, b"", [(1, 3)], None)]
    return {
        "empty set": gtid_set([]),
        "one interval": gtid_set(one),
        "header only, 1 TSID": gtid_set([], count=1),
        "header only, 2^56-1 TSIDs": gtid_set([], count=(1 << 56) - 1),
        "2^56-1 TSIDs, one present": gtid_set(one, count=(1 << 56) - 1),
        "tagged, 2^48-1 TSIDs, none present":
            gtid_set([], tagged=True, count=(1 << 48) - 1),
        "7 bytes of header": gtid_set([])[:7],
        "format byte 2":
            struct.pack("<Q", 1)[:7] + b"\x02" + gtid_set(one)[8:],
        "format byte 255": b"\xff" * 8,
        "format bytes disagree": b"\x00" + gtid_set(one, tagged=True)[1:],
        "uuid cut": gtid_set(one)[:8 + 9],
        "no interval count": gtid_set(one)[:8 + 16],
        "interval count cut": gtid_set(one)[:8 + 16 + 5],
        "2^64-1 intervals, one present":
            gtid_set([(UUID, b"", [(1, 3)], top)]),
        "2^32 intervals, none present": gtid_set([(UUID, b"", [], 1 << 32)]),
        "0 intervals": gtid_set([(UUID, b"", [], 0)]),
        "0 intervals, then an interval":
            gtid_set([(UUID, b"", [(1, 3)], 0)]),
        "interval cut": gtid_set(one)[:-3],
        "empty interval": gtid_set([(UUID, b"", [(5, 5)], None)]),
        "reversed interval": gtid_set([(UUID, b"", [(9, 2)], None)]),
        "interval from 0": gtid_set([(UUID, b"", [(0, 1)], None)]),
        "interval 0 to 2^64-1": gtid_set([(UUID, b"", [(0, top)], None)]),
        "interval 1 to 2^63": gtid_set([(UUID, b"", [(1, 1 << 63)], None)]),
        "interval 2^64-2 to 2^64-1":
            gtid_set([(UUID, b"", [(top - 1, top)], None)]),
        "intervals in falling order":
            gtid_set([(UUID, b"", [(50, 60), (20, 30), (1, 5)], None)]),
        "1,000 overlapping intervals": gtid_set(
            [(UUID, b"", [(1 + i, 500 + i) for i in range(1000)], None)]),
        "one uuid 500 times": gtid_set([(UUID, b"", [(i + 1, i + 2)], None)
                                        for i in range(500)]),
        "uuid of zeros": gtid_set([(b"\x00" * 16, b"", [(1, 2)], None)]),
        "uuid of 0xff": gtid_set([(b"\xff" * 16, b"", [(1, 2)], None)]),
        "trailing byte": gtid_set(one) + b"\x00",
        "tagged, tag 'tag_1'":
            gtid_set([(UUID, b"tag_1", [(1, 3)], None)], tagged=True),
        "tagged, empty tag":
            gtid_set([(UUID, b"", [(1, 3)], None)], tagged=True),
        "tagged, tag of 32 bytes":
            gtid_set([(UUID, b"t" * 32, [(1, 3)], None)], tagged=True),
        "tagged, tag of 33 bytes":
            gtid_set([(UUID, b"t" * 33, [(1, 3)], None)], tagged=True),
        "tagged, tag of 300 bytes":
            gtid_set([(UUID, b"t" * 300, [(1, 3)], None)], tagged=True),
        "tagged, tag '1bad'":
            gtid_set([(UUID, b"1bad", [(1, 3)], None)], tagged=True),
        "tagged, tag with NUL and '/'":
            gtid_set([(UUID, b"a\x00/..", [(1, 3)], None)], tagged=True),
        "tagged, tag length 60, body ends":
            gtid_set([], tagged=True, count=1) + UUID + bytes([60 << 1]) +
            b"abc",
        "tagged, tag length of 9 bytes of 0xff":
            gtid_set([], tagged=True, count=1) + UUID + b"\xff" * 9 + b"abc",
        "tagged, tag length byte 0x7f, body ends":
            gtid_set([], tagged=True, count=1) + UUID + b"\x7f",
        "untagged header, tagged body": struct.pack("<Q", 1) + gtid_set(
            [(UUID, b"tag", [(1, 3)], None)], tagged=True)[8:]}


def case_binlog_dump_gtid_sets(ctx: Context) -> dict:
    answers = {}
    for label, data in hostile_gtid_sets().items():
        for name in (b"", ctx.binlogs[0][0]):
            answer = one_shot(
                ctx, f"a GTID dump with the set '{label}' "
                f"({data[:64].hex()}), name {name!r}",
                com_binlog_dump_gtid(NON_BLOCKING, 5, name, 4, data),
                checksum=True, stream=True)
            answers[label + (", named" if name else "")] = (
                f"{answer['events']} events, {answer['end']}")
    return {"gtid_set_decoder_reached": ctx.args.mode == "gtid",
            "answers": answers}


def change_user_inputs(secure_connection: bool, attributes: bool) -> dict:
    user = listener.PULL_USER.encode()
    extra = b"\x04_pid\x0212" if attributes else None

    def build(name, auth, **more):
        return com_change_user(name, auth, attributes=extra,
                               secure_connection=secure_connection, **more)
    return {
        "same user": build(user, b"\x11" * 32),
        "same user, empty password": build(user, b""),
        "another user": build(b"root", b"\x11" * 32),
        "another user, other plugin":
            build(b"root", b"\x11" * 20, plugin=b"mysql_native_password"),
        "user of 3,000 bytes": build(b"u" * 3000, b"\x11" * 32),
        "password of 255 bytes": build(user, b"\x11" * 255),
        "schema of 3,000 bytes": build(user, b"", schema=b"s" * 3000),
        "plugin of 3,000 bytes": build(user, b"", plugin=b"p" * 3000),
        "no NUL at all": bytes([COM_CHANGE_USER]) + b"abcdef",
        "password length 255, body ends":
            bytes([COM_CHANGE_USER]) + user + b"\x00\xff" + b"\x11" * 8,
        "ends after the schema":
            bytes([COM_CHANGE_USER]) + user + b"\x00\x00\x00",
        "ends inside the collation":
            bytes([COM_CHANGE_USER]) + user + b"\x00\x00\x00\xff",
        "attributes length 2^64-1": com_change_user(
            user, b"", secure_connection=secure_connection) + b"\xfe" +
            b"\xff" * 8,
        "attributes length 251 (NULL)": com_change_user(
            user, b"", secure_connection=secure_connection) + b"\xfb",
        "garbage": bytes([COM_CHANGE_USER]) + bytes(range(1, 200)) * 3,
        "command byte only": bytes([COM_CHANGE_USER])}


def change_user_case(ctx: Context, capabilities: "int | None",
                     secure_connection: bool, attributes: bool) -> dict:
    answers = {}
    problems = []
    for label, payload in change_user_inputs(secure_connection,
                                             attributes).items():
        client = ctx.session(capabilities)
        try:
            answer = command(client, payload)
            # whoever the session belongs to now, it must answer or be closed
            after = (command(client, bytes([COM_PING]))
                     if answer != "closed" else "-")
        finally:
            client.close()
        ctx.check("COM_CHANGE_USER, " + label)
        answers[label] = f"{answer}; then ping: {after}"
        if answer == "OK" and label.startswith("another user"):
            problems.append(f"COM_CHANGE_USER to {label} was answered with "
                            "OK")
        if answer == "timeout" or after == "timeout":
            problems.append(f"no answer to COM_CHANGE_USER, {label}")
    return {"answers": answers, "problems": problems}


def case_change_user(ctx: Context) -> dict:
    result = change_user_case(ctx, None, True, False)
    result["truncated"] = truncations(
        ctx, "COM_CHANGE_USER", change_user_inputs(True, False)["same user"])
    return result


def case_change_user_capabilities(ctx: Context) -> dict:
    base = listener.BASE_CAPABILITIES
    variants = {
        "connection attributes": (base | CLIENT_CONNECT_ATTRS, True, True),
        "no secure connection":
            (base & ~listener.CLIENT_SECURE_CONNECTION, False, False),
        "attributes, no secure connection":
            ((base | CLIENT_CONNECT_ATTRS) &
             ~listener.CLIENT_SECURE_CONNECTION, False, True)}
    result = {"problems": []}
    for label, (capabilities, secure, attributes) in variants.items():
        part = change_user_case(ctx, capabilities, secure, attributes)
        result["problems"] += [f"{label}: {problem}"
                               for problem in part.pop("problems")]
        result[label] = tally(part["answers"])
        valid = change_user_inputs(secure, attributes)["same user"]
        for length in range(1, len(valid)):
            one_shot(ctx, f"COM_CHANGE_USER ({label}) cut to {length} bytes",
                     valid[:length], capabilities=capabilities)
    return result


def case_quit_then_data(ctx: Context) -> dict:
    answers = {}
    ping = frame(0, bytes([COM_PING]))
    for label, data in {
            "quit, then a ping in the same segment":
                frame(0, bytes([COM_QUIT])) + ping,
            "quit, then 1 MB of garbage":
                frame(0, bytes([COM_QUIT])) + b"\x5a" * (1 << 20),
            "quit with a body": frame(0, bytes([COM_QUIT]) + b"x" * 500),
            "quit with sequence number 1": frame(1, bytes([COM_QUIT])),
            "quit, then a dump": frame(0, bytes([COM_QUIT])) + frame(
                0, com_binlog_dump(4, 0, 5, b"")),
            "quit, then half a frame":
                frame(0, bytes([COM_QUIT])) + b"\x10\x00\x00\x00abc"}.items():
        client = ctx.session()
        try:
            client.send(data)
            answers[label] = kind(read_frame(client, 5.0))
        finally:
            client.close()
        ctx.check(label)
    # more data after the quit, when the server may have closed already
    client = ctx.session()
    try:
        client.send(frame(0, bytes([COM_QUIT])))
        time.sleep(0.3)
        for _ in range(20):
            client.send(ping * 50)
        answers["quit, a pause, 1,000 pings"] = kind(read_frame(client, 5.0))
    finally:
        client.close()
    ctx.check("pings after a quit")
    return {"answers": answers}


def case_ping(ctx: Context) -> dict:
    client = ctx.session()
    problems = []
    try:
        client.send(frame(0, bytes([COM_PING])) * 1000)
        answered = 0
        for _ in range(1000):
            if kind(read_frame(client, 5.0)) != "OK":
                break
            answered += 1
        if answered != 1000:
            problems.append(f"{answered} of 1,000 pipelined pings were "
                            "answered with OK")
        with_body = command(client, bytes([COM_PING]) + b"x" * 3000)
    finally:
        client.close()
    ctx.check("1,000 pipelined pings")
    return {"pipelined_ok": answered, "ping_with_a_body": with_body,
            "problems": problems}


def case_reset_connection(ctx: Context) -> dict:
    client = ctx.session(checksum=True)
    try:
        answers = {
            "reset": command(client, bytes([COM_RESET_CONNECTION])),
            "reset with a body":
                command(client, bytes([COM_RESET_CONNECTION]) + b"x" * 100),
            "checksum variable after the resets": command(
                client, com_query(b"SELECT @source_binlog_checksum")),
            "ping": command(client, bytes([COM_PING]))}
    finally:
        client.close()
    ctx.check("COM_RESET_CONNECTION")
    return {"answers": answers}


def case_empty_packet(ctx: Context) -> dict:
    answers = {}
    for sequence in (0, 1, 255):
        client = ctx.session()
        try:
            client.send(frame(sequence, b""))
            answers[f"sequence number {sequence}"] = kind(
                read_frame(client, 5.0))
        finally:
            client.close()
        ctx.check(f"an empty frame with sequence number {sequence}")
    client = ctx.session()
    try:
        client.send(frame(0, b"") * 1000)
        answers["1,000 empty frames"] = kind(read_frame(client, 5.0))
    finally:
        client.close()
    ctx.check("1,000 empty frames")
    return {"answers": answers}


def case_sequence_ids(ctx: Context) -> dict:
    answers = {}
    for sequence in (1, 2, 127, 128, 255):
        for label, payload in (("ping", bytes([COM_PING])),
                               ("query", com_query(b"SELECT VERSION()")),
                               ("dump", com_binlog_dump(4, NON_BLOCKING, 5,
                                                        b"")),
                               ("unknown", b"\x99")):
            answers[f"{label}, sequence number {sequence}"] = one_shot(
                ctx, f"{label} with sequence number {sequence}", payload,
                sequence=sequence, checksum=(label == "dump"))
    # the second command goes on counting where the first answer stopped
    client = ctx.session()
    try:
        first = command(client, bytes([COM_PING]))
        answers["second command with sequence number 2"] = (
            f"{first}, then " + command(client, bytes([COM_PING]),
                                        sequence=2))
    finally:
        client.close()
    ctx.check("a second command that goes on counting")
    return {"answers": tally(answers)}


def case_second_handshake(ctx: Context) -> dict:
    user = listener.PULL_USER.encode()
    greeting = greeting_payload(listener.BASE_CAPABILITIES, user,
                                b"\x11" * 32, b"caching_sha2_password")
    answers = {}
    for label, data in {
            "the greeting again, sequence number 0": frame(0, greeting),
            "the greeting again, sequence number 1": frame(1, greeting),
            "the greeting again, sequence number 3": frame(3, greeting),
            "a greeting of another user": frame(0, greeting_payload(
                listener.BASE_CAPABILITIES, b"root", b"",
                b"mysql_native_password")),
            "an SSL request": frame(0, listener.ssl_request_payload(
                listener.BASE_CAPABILITIES | listener.CLIENT_SSL)),
            "a TLS ClientHello": bytes.fromhex("160301004a0100004603030000"),
            "a public key request": frame(0, b"\x02"),
            "an auth switch response": frame(3, b"\x11" * 32),
            "256 bytes of RSA-sized data": frame(0, b"\x7f" * 256)}.items():
        client = ctx.session()
        try:
            client.send(data)
            first = kind(read_frame(client, 5.0))
            after = (command(client, bytes([COM_PING]))
                     if first not in ("closed", "timeout") else "-")
        finally:
            client.close()
        ctx.check(label + " after the login")
        answers[label] = f"{first}; then ping: {after}"
    return {"answers": answers}


def case_commands_during_dump(ctx: Context) -> dict:
    answers = {}
    dump = frame(0, com_binlog_dump(4, 0, 5, b""))
    gtid_dump = frame(0, com_binlog_dump_gtid(0, 5, b"", 4, gtid_set([])))
    for label, data in {
            "a ping": frame(0, bytes([COM_PING])),
            "a second dump": dump,
            "a GTID dump": gtid_dump,
            "a quit": frame(0, bytes([COM_QUIT])),
            "a heartbeat period of 1 ns": frame(0, com_query(
                b"SET @master_heartbeat_period=1")),
            "100 commands": (frame(0, bytes([COM_PING])) + dump +
                             frame(0, com_query(CHECKSUM_ON))) * 34,
            "1 MB of garbage": b"\xc3" * (1 << 20),
            "a frame of 0xFFFFFF bytes, cut": b"\xff\xff\xff\x00" + b"q" * 99
            }.items():
        client = ctx.session(checksum=True)
        try:
            # both in one segment, then again while the events arrive
            client.send(dump + data)
            first = read_stream(client, 0.5)
            client.send(data)
            rest = read_stream(client, 1.0)
        finally:
            client.close()
        ctx.check(f"{label} during a dump")
        answers[label] = (f"{first['events'] + rest['events']} events, "
                          f"{rest['end']}")
    return {"answers": answers}


def command_bytes_case(with_body: bool):
    def case(ctx: Context) -> dict:
        rng = random.Random(ctx.args.seed)
        answers = {}
        client = None
        try:
            for byte in range(256):
                payload = bytes([byte]) + (listener.random_bytes(rng, 16, 16)
                                           if with_body else b"")
                if client is None:
                    client = ctx.session()
                answers[f"{byte:#04x}"] = answer = command(client, payload,
                                                           timeout=3.0)
                ctx.check(f"the command {payload.hex()}")
                if answer != "ERR 1047 Unknown command":
                    # closed, or an answer after which nothing is certain
                    client.close()
                    client = None
        finally:
            if client is not None:
                client.close()
        unknown = sum(1 for answer in answers.values()
                      if answer == "ERR 1047 Unknown command")
        return {"unknown_command": unknown,
                "others": {byte: answer for byte, answer in answers.items()
                           if answer != "ERR 1047 Unknown command"}}
    return case


# ---------------------------------------------------------------------------
# the fuzz phase
# ---------------------------------------------------------------------------

def random_gtid_set(rng: random.Random) -> bytes:
    tagged = rng.random() < 0.3
    tsids = []
    for _ in range(rng.choice([0, 1, 1, 2, 5])):
        intervals = []
        for _ in range(rng.choice([0, 1, 1, 3, 20])):
            lower = rng.choice([0, 1, rng.randrange(1, 1000),
                                rng.getrandbits(64)])
            intervals.append((lower, rng.choice(
                [lower, lower + 1, lower + rng.randrange(1, 1000),
                 rng.getrandbits(64)]) & ((1 << 64) - 1)))
        tag = rng.choice([b"", b"tag", b"t" * 32, b"t" * 33, b"9x"]) \
            if tagged else b""
        tsids.append((rng.choice([UUID, listener.random_bytes(rng, 16, 16)]),
                      tag, intervals,
                      rng.choice([None, None, None, 0, 1, rng.getrandbits(64),
                                  len(intervals) + 1])))
    return gtid_set(tsids, tagged, rng.choice(
        [None, None, None, 0, len(tsids) + 1, rng.getrandbits(56)]))


def random_command(ctx: Context, rng: random.Random) -> "tuple[str, bytes]":
    """A command a replica could send, with randomly chosen fields."""
    names = [name for name, _ in ctx.binlogs]
    size = ctx.binlogs[0][1]
    name = rng.choice(names + [names[0], b"", b"", b"binlog.000099",
                               b"../" + names[0], names[0] + b"\x00"])
    position = rng.choice([4, 4, 4, rng.randrange(0, size + 10),
                           rng.randrange(0, 600), rng.getrandbits(32)])
    flags = rng.choice([NON_BLOCKING, NON_BLOCKING, NON_BLOCKING, 3, 0,
                        rng.getrandbits(16)])
    server_id = rng.choice([0, 5, rng.getrandbits(32)])
    choice = rng.randrange(12)
    if choice == 0:
        return "known query", com_query(rng.choice(KNOWN_QUERIES))
    if choice == 1:
        return "checksum", com_query(CHECKSUM_ON)
    if choice == 2:
        run = rng.choice([1, 2, 50, FUZZ_MAX_RUN])
        statement = rng.choice(list(regex_run_statements(run).values()))
        return "regex query", com_query(statement)
    if choice == 3:
        value = rng.choice([b"0", b"1", b"1000", str(rng.getrandbits(
            rng.choice([8, 32, 63, 64, 70]))).encode()])
        return "heartbeat period", com_query(
            b"SET @master_heartbeat_period = " + value +
            b", @source_heartbeat_period = " + value)
    if choice == 4:
        return "ping", bytes([COM_PING])
    if choice == 5:
        return "register replica", com_register_replica(
            rng.getrandbits(32), listener.random_bytes(rng, 0, 255),
            listener.random_bytes(rng, 0, 60),
            listener.random_bytes(rng, 0, 60), rng.getrandbits(16))
    if choice in (6, 7):
        return "binlog dump", com_binlog_dump(position, flags, server_id,
                                              name)
    if choice in (8, 9):
        return "binlog dump gtid", com_binlog_dump_gtid(
            flags, server_id, name,
            rng.choice([position, position, rng.getrandbits(64)]),
            random_gtid_set(rng))
    if choice == 10:
        return "change user", com_change_user(
            rng.choice([listener.PULL_USER.encode(), b"root",
                        listener.random_bytes(rng, 0, 40)]),
            listener.random_bytes(rng, 0, 255),
            listener.random_bytes(rng, 0, 20).replace(b"\x00", b""))
    return rng.choice([("reset connection", bytes([COM_RESET_CONNECTION])),
                       ("quit", bytes([COM_QUIT])),
                       ("unknown", bytes([rng.randrange(256)]) +
                        listener.random_bytes(rng, 0, 40))])


def mutate(rng: random.Random, payload: bytes) -> bytes:
    data = bytearray(payload)
    for _ in range(rng.choice([1, 1, 2, 4])):
        choice = rng.randrange(8)
        if choice == 0 and len(data) > 1:    # cut
            del data[rng.randrange(1, len(data)):]
        elif choice == 1:                    # extend
            data += listener.random_bytes(rng, 1, rng.choice([1, 8, 300]))
        elif choice == 2 and data:           # flip one bit
            data[rng.randrange(len(data))] ^= 1 << rng.randrange(8)
        elif choice == 3 and data:           # replace a few bytes
            start = rng.randrange(len(data))
            piece = listener.random_bytes(rng, 1, 8)
            data[start:start + len(piece)] = piece
        elif choice == 4 and len(data) > 1:  # an extreme in a length field
            start = rng.randrange(1, len(data))
            piece = rng.choice([b"\xff", b"\x00", b"\xff\xff\xff\xff",
                                b"\x00\x00\x00\x80", b"\xff" * 8, b"\xfb",
                                b"\xfe", b"\x7f"])
            data[start:start + len(piece)] = piece
        elif choice == 5 and len(data) > 2:  # repeat a part
            start = rng.randrange(1, len(data))
            end = rng.randrange(start, min(len(data), start + 64) + 1)
            data[start:start] = data[start:end] * rng.choice([1, 2, 8])
        elif choice == 6 and len(data) > 1:  # drop a part
            start = rng.randrange(1, len(data))
            del data[start:start + rng.randrange(1, 9)]
        elif choice == 7 and data:           # another command, this body
            data[0] = rng.choice([COM_QUERY, COM_BINLOG_DUMP,
                                  COM_BINLOG_DUMP_GTID, COM_REGISTER_SLAVE,
                                  COM_CHANGE_USER, rng.randrange(256)])
    mutated = bytes(data)
    # nothing that ticket 72 would answer for
    return payload if LONG_RUN.search(mutated) else mutated


def case_fuzz(ctx: Context) -> dict:
    rng = random.Random(ctx.args.seed)
    commands: dict = {}
    answers: dict = {}
    ends: dict = {}
    events = 0
    problems = []
    done = 0
    for index in range(ctx.args.connections):
        try:
            client = ctx.session(
                rng.choice([None, None, None, listener.BASE_CAPABILITIES |
                            CLIENT_CONNECT_ATTRS, listener.BASE_CAPABILITIES &
                            ~listener.CLIENT_SECURE_CONNECTION]),
                timeout=2.0)
        except harness.Failure:
            problems.append(f"connection {index} could not log in")
            break
        try:
            # most sessions declare the checksum first: without it a dump
            # is refused before the sender looks at it
            data = (frame(0, com_query(CHECKSUM_ON)) if rng.random() < 0.7
                    else b"")
            for _ in range(rng.randint(1, 5)):
                label, payload = random_command(ctx, rng)
                if rng.random() < 0.6:
                    label += ", mutated"
                    payload = mutate(rng, payload)
                commands[label] = commands.get(label, 0) + 1
                data += frame(rng.choice([0] * 9 + [rng.randrange(256)]),
                              payload)
            if rng.random() < 0.1:   # a header that promises more
                data += struct.pack("<I", rng.randrange(1, 1 << 24))[:3] + \
                    b"\x00" + listener.random_bytes(rng, 0, 20)
            if rng.random() < 0.5:   # all at once, or in pieces
                client.send(data)
            else:
                cuts = sorted(rng.randrange(len(data) + 1)
                              for _ in range(rng.randint(1, 4)))
                for start, end in zip([0] + cuts, cuts + [len(data)]):
                    client.send(data[start:end])
                    time.sleep(0.002)
            # every answer is read: a client that closes early would end
            # its session before pull got to the later commands
            read = read_stream(client, 3.0, quiet=0.15, answers=answers)
            events += read["events"] + read["heartbeats"]
            ends[read["end"]] = ends.get(read["end"], 0) + 1
        finally:
            client.close()
        done += 1
        ctx.check(f"fuzz connection {index} (seed {ctx.args.seed})")
        if index % 25 == 24 and ctx.login_seconds(30.0) is None:
            ctx.settle(f"fuzz connection {index} (seed {ctx.args.seed})")
            problems.append(f"no login possible after fuzz connection "
                            f"{index}")
            break
    if not problems:
        ctx.settle(f"the last fuzz connections (seed {ctx.args.seed})")
        if not listener.streams_binlogs("127.0.0.1", ctx.port):
            problems.append("mysqlbinlog is not served after the fuzz phase")
    return {"seed": ctx.args.seed, "connections": done, "commands": commands,
            "answers": answers, "events_received": events,
            "session_ends": ends, "problems": problems}


# ---------------------------------------------------------------------------
# ticket 72: the long runs, last
# ---------------------------------------------------------------------------

def case_regex_run_threshold(ctx: Context) -> dict:
    survived = 0
    for run in (2_000, 5_000, 10_000, 20_000, 40_000, 80_000, 160_000,
                320_000, 1_000_000):
        client = ctx.session(timeout=30.0)
        try:
            answer = command(client, com_query(
                b"SET @master_heartbeat_period=" + b"1" * run), timeout=30.0)
        finally:
            client.close()
            client.sent = client.sent[:64]
        time.sleep(0.5)
        if not ctx.alive():
            return {"longest_run_survived": survived,
                    "shortest_run_that_ended_pull": run,
                    "problems": [f"a COM_QUERY of 'SET @master_heartbeat_"
                                 f"period=' and {run} digits (answer: "
                                 f"{answer}) ended pull"]}
        survived = run
    return {"longest_run_survived": survived}


def regex_run_case(label: str, run: int):
    def case(ctx: Context) -> dict:
        statement = regex_run_statements(run)[label]
        client = ctx.session(timeout=30.0)
        try:
            answer = command(client, com_query(statement), timeout=30.0)
        finally:
            client.close()
        time.sleep(0.5)
        ctx.check(f"a COM_QUERY of {statement[:40]!r}... with a run of "
                  f"{run} characters ({len(statement)} bytes; answer: "
                  f"{answer})")
        return {"answer": answer}
    return case


def all_cases(args) -> list:
    """(name, function, known ticket or None), in the order they run."""
    cases = [
        ("known_queries", case_known_queries),
        ("heartbeat_period_values", case_heartbeat_period_values),
        ("regex_long_text", case_regex_long_text),
        ("regex_short_runs", case_regex_short_runs),
        ("query_max_frame", case_query_max_frame),
        ("max_frames_repeated", case_max_frames_repeated)]
    cases += [(f"heartbeat_{period}ns_blocking_dump",
               heartbeat_dump_case(period))
              for period in (0, 1, 1000, (1 << 63) - 1)]
    cases += [
        ("register_replica_truncated", case_register_replica_truncated),
        ("register_replica_fields", case_register_replica_fields),
        ("binlog_dump_no_checksum", case_binlog_dump_no_checksum),
        ("binlog_dump_truncated", case_binlog_dump_truncated),
        ("binlog_dump_positions", case_binlog_dump_positions),
        ("binlog_dump_flags", case_binlog_dump_flags),
        ("binlog_dump_names", case_binlog_dump_names),
        ("binlog_dump_gtid_truncated", case_binlog_dump_gtid_truncated),
        ("binlog_dump_gtid_lengths", case_binlog_dump_gtid_lengths),
        ("binlog_dump_gtid_positions", case_binlog_dump_gtid_positions),
        ("binlog_dump_gtid_names", case_binlog_dump_gtid_names),
        ("binlog_dump_gtid_sets", case_binlog_dump_gtid_sets),
        ("change_user", case_change_user),
        ("change_user_capabilities", case_change_user_capabilities),
        ("quit_then_data", case_quit_then_data),
        ("ping", case_ping),
        ("reset_connection", case_reset_connection),
        ("empty_packet", case_empty_packet),
        ("sequence_ids", case_sequence_ids),
        ("second_handshake", case_second_handshake),
        ("commands_during_dump", case_commands_during_dump),
        ("command_bytes", command_bytes_case(False)),
        ("command_bytes_with_body", command_bytes_case(True))]
    cases = [(name, function, None) for name, function in cases]
    if args.connections > 0:
        cases.append(("fuzz", case_fuzz, None))
    if not args.no_long_runs:
        cases.append(("regex_run_threshold", case_regex_run_threshold,
                      TICKET_72))
        cases += [(f"regex_run_{label}", regex_run_case(label, 100_000),
                   TICKET_72) for label in regex_run_statements(1)]
    if args.only:
        cases = [case for case in cases if case[0] in args.only]
    return cases


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--mysql-basedir", type=Path, required=True)
    parser.add_argument("--binsrv", type=Path, required=True)
    parser.add_argument("--workdir", type=Path, required=True)
    parser.add_argument("--port", type=int, default=23700)
    parser.add_argument("--mode", choices=["gtid", "position"],
                        default="position")
    parser.add_argument("--connections", type=int, default=300,
                        help="connections of the fuzz phase (0: none)")
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument("--max-wait", type=float, default=10.0,
                        help="seconds a new client may wait to log in")
    parser.add_argument("--max-rss-mib", type=int, default=1024,
                        help="how much the memory of pull may grow")
    parser.add_argument("--max-idle-cpu", type=float, default=0.5,
                        help="the share of one CPU an idle dump may use")
    parser.add_argument("--no-long-runs", action="store_true",
                        help="without the cases of ticket 72, which end pull")
    parser.add_argument("--only", action="append", metavar="CASE",
                        help="run this case only (can be repeated)")
    args = parser.parse_args()
    args.workdir = args.workdir.expanduser()
    args.workdir.mkdir(parents=True, exist_ok=True)
    listener.MYSQLBINLOG = (args.mysql_basedir.expanduser() / "bin" /
                            "mysqlbinlog")
    gtid_mode = args.mode == "gtid"
    harness.SERVER = harness.MySQLServer(args.mysql_basedir.expanduser(),
                                         args.workdir / "mysql", args.port,
                                         gtid_mode, server_id=1)
    binsrv = harness.BINSRV = harness.BinlogServer(
        args.binsrv.expanduser(), args.workdir / "binsrv", args.port,
        gtid_mode)
    for stale in args.workdir.glob("crash*"):
        stale.unlink()
    results = []
    ctx = None
    started = time.monotonic()
    # 'timeout' and the like must not leave mysqld and pull behind
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(143))
    harness.SERVER.start()
    try:
        machine = harness.FetchCrashMachine()
        machine.start_from_scratch()
        for index, workload in enumerate(WORKLOADS):
            if index:
                machine.flush_binary_logs()
            machine.run_workload(workload)
        binsrv.checkpoint_size = "1"
        machine.fetch_to_the_end()
        ctx = Context(args, binsrv)
        if not ctx.binlogs:
            raise harness.Failure("fetch stored no binlogs")
        for name, function, known in all_cases(args):
            result = run_case(ctx, name, function, known)
            print(json.dumps(result), flush=True)
            results.append(result)
    finally:
        if ctx is not None and ctx.pull is not None:
            login.stop_pull(ctx.pull)
            (args.workdir / "pull.stderr").write_bytes(bytes(ctx.stderr))
        harness.SERVER.stop()
    failures = [result for result in results if not result.get("ok")]
    print(json.dumps({"cases": len(results), "failures": len(failures),
                      "failed": [result["case"] for result in failures],
                      "mode": args.mode,
                      "seconds": round(time.monotonic() - started),
                      "passed": not failures}), flush=True)
    (args.workdir / "result.json").write_text(json.dumps(results, indent=2))
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
