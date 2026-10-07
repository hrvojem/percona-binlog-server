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

"""Fuzzes the 'binlog_server pull' replication source listener with malformed
MySQL client traffic.

The listener (minimysql::network_service / connection_context) is a
hand-written MySQL protocol server on an open TCP port. So far only
well-behaved clients (mysqlbinlog, MySQL replicas) have talked to it. This
test throws malformed and truncated packets at it at every stage of the
protocol and checks that the 'pull' process keeps running and keeps serving
correct binlogs: a single connection must never take the whole server down.

Set-up: a source MySQL server gets a workload, 'fetch' stores its binlogs,
then 'binlog_server pull' serves them on its replication source port with a
'caching_sha2_password' user this test knows the password of.

A seeded, reproducible campaign of connections follows. Each connection is
one generated scenario: random bytes in the greeting, a length header that
lies about the payload, a valid handshake followed by a garbage command, a
truncated COM_BINLOG_DUMP, a wrong sequence number, the auth-method-switch
path with a random response, a half-open connection, and so on. After every
connection the test checks that 'pull' is still alive; a crash (including an
AddressSanitizer abort when run against the ASan build) stops the campaign
and the exact bytes that triggered it are written to '<workdir>/crash.bin'.
At the end a clean handshake plus COM_BINLOG_DUMP must still stream the
stored binlogs, proving the listener still works.

Run against the AddressSanitizer build to catch memory errors. Example:

  PYTHONPATH=~/ws/qa/pylib python3 listener_fuzz_test.py \\
    --mysql-basedir ~/ws/dist/mysql-8.4.8-linux-glibc2.28-x86_64-minimal \\
    --binsrv ~/ws/percona-binlog-server-build-asan_gcc14/binlog_server \\
    --workdir ~/ws/qa/listener-fuzz --connections 4000
"""

import argparse
import hashlib
import json
import os
import random
import signal
import socket
import struct
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import fetch_crash_test as harness  # noqa: E402

PULL_USER = "fuzz"
PULL_PASSWORD = "fuzz_password"
WORKLOAD = [{"rows": [2000, 40_000], "ddl": False},
            {"rows": [300], "ddl": True}]

# client capability flags (a subset of CLIENT_* from mysql_com.h)
CLIENT_LONG_PASSWORD = 0x1
CLIENT_PROTOCOL_41 = 0x200
CLIENT_SECURE_CONNECTION = 0x8000
CLIENT_PLUGIN_AUTH = 0x80000
CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA = 0x200000
BASE_CAPABILITIES = (CLIENT_LONG_PASSWORD | CLIENT_PROTOCOL_41 |
                     CLIENT_SECURE_CONNECTION | CLIENT_PLUGIN_AUTH |
                     CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA)

COM_QUERY = 0x03
COM_BINLOG_DUMP = 0x12
COM_BINLOG_DUMP_GTID = 0x1e

HEADER_LENGTH = 4
MAX_PAYLOAD = 16 * 1024 * 1024


def scramble(password: str, salt: bytes) -> bytes:
    """The caching_sha2_password client response: SHA256(password) XOR
    SHA256(SHA256(SHA256(password)) . salt)."""
    if not password:
        return b""
    sha = hashlib.sha256(password.encode()).digest()
    double = hashlib.sha256(sha).digest()
    triple = hashlib.sha256(double + salt).digest()
    return bytes(a ^ b for a, b in zip(sha, triple))


def frame(sequence: int, payload: bytes) -> bytes:
    """Wraps a payload in a MySQL frame header (3-byte little-endian length,
    1-byte sequence number)."""
    return struct.pack("<I", len(payload))[:3] + bytes([sequence & 0xFF]) + \
        payload


def lenenc(value: int) -> bytes:
    if value < 0xFB:
        return bytes([value])
    if value <= 0xFFFF:
        return b"\xfc" + struct.pack("<H", value)
    if value <= 0xFFFFFF:
        return b"\xfd" + struct.pack("<I", value)[:3]
    return b"\xfe" + struct.pack("<Q", value)


def greeting_payload(capabilities: int, username: bytes, auth_data: bytes,
                     plugin: bytes) -> bytes:
    """A protocol-41 client greeting (HandshakeResponse41)."""
    payload = struct.pack("<I", capabilities)
    payload += struct.pack("<I", 0x01000000)  # max packet size
    payload += bytes([0xff])                  # collation (utf8mb4)
    payload += b"\x00" * 23                   # filler
    payload += username + b"\x00"
    if capabilities & CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA:
        payload += lenenc(len(auth_data)) + auth_data
    else:
        payload += bytes([len(auth_data)]) + auth_data
    if capabilities & CLIENT_PLUGIN_AUTH:
        payload += plugin + b"\x00"
    return payload


class Listener:
    """A minimal raw client for the 'pull' listener: it writes exactly the
    bytes it is told to and reads whatever comes back, so it can send things a
    real client never would."""

    def __init__(self, host: str, port: int, timeout: float = 2.0):
        self.sock = socket.create_connection((host, port), timeout=timeout)
        self.sock.settimeout(timeout)
        self.sent = bytearray()

    def read_frame(self) -> "tuple[int, bytes] | None":
        header = self._recv_exactly(HEADER_LENGTH)
        if header is None:
            return None
        length = header[0] | (header[1] << 8) | (header[2] << 16)
        body = self._recv_exactly(length) if length else b""
        return header[3], (body or b"")

    def _recv_exactly(self, count: int) -> "bytes | None":
        chunks = bytearray()
        while len(chunks) < count:
            try:
                piece = self.sock.recv(count - len(chunks))
            except (socket.timeout, OSError):
                return None
            if not piece:
                return None
            chunks += piece
        return bytes(chunks)

    def send(self, data: bytes):
        self.sent += data
        try:
            self.sock.sendall(data)
        except OSError:
            pass

    def server_salt(self) -> "bytes | None":
        """Reads the server greeting and returns its 20-byte scramble."""
        read = self.read_frame()
        if read is None:
            return None
        payload = read[1]
        # protocol(1) + version(NUL string) + connid(4) + salt1(8) + 0 + ...
        try:
            end_of_version = payload.index(b"\x00", 1)
        except ValueError:
            return None
        rest = payload[end_of_version + 1 + 4:]
        salt1 = rest[:8]
        rest = rest[8 + 1:]  # skip salt1 and its filler
        # capability-low(2) charset(1) status(2) capability-high(2)
        # auth-data-len(1) reserved(10)
        rest = rest[2 + 1 + 2 + 2 + 1 + 10:]
        salt2 = rest[:12]
        return salt1 + salt2

    def handshake(self) -> bool:
        """A clean caching_sha2_password handshake; returns True once the
        command loop is reached (the server sent its OK)."""
        salt = self.server_salt()
        if salt is None:
            return False
        response = scramble(PULL_PASSWORD, salt)
        self.send(frame(1, greeting_payload(
            BASE_CAPABILITIES, PULL_USER.encode(), response,
            b"caching_sha2_password")))
        # fast-auth packet (0x01 0x03) then OK, or more; read until an OK/ERR
        for _ in range(4):
            read = self.read_frame()
            if read is None:
                return False
            body = read[1]
            if body[:1] == b"\x00":   # OK
                return True
            if body[:1] == b"\xff":   # ERR
                return False
        return False

    def close(self):
        try:
            self.sock.close()
        except OSError:
            pass


def random_bytes(rng: random.Random, low: int, high: int) -> bytes:
    return bytes(rng.getrandbits(8) for _ in range(rng.randint(low, high)))


# each scenario returns a label; it talks to the listener through 'client'
def scenario(rng: random.Random, client: Listener) -> str:
    choice = rng.randrange(14)

    if choice == 0:  # connect and close at once
        return "immediate close"

    if choice == 1:  # partial header then close
        client.send(random_bytes(rng, 1, 3))
        return "partial header"

    if choice == 2:  # a length header that lies: claims a lot, sends little
        client.read_frame()  # consume the greeting
        length = rng.choice([0xFFFFFF, 0xFFFFFE, MAX_PAYLOAD, MAX_PAYLOAD + 1,
                             rng.randint(1000, 0xFFFFFF)]) & 0xFFFFFF
        header = struct.pack("<I", length)[:3] + bytes([rng.randint(0, 255)])
        client.send(header + random_bytes(rng, 0, 32))
        return "lying length header"

    if choice == 3:  # random bytes in place of the client greeting
        client.read_frame()
        client.send(frame(1, random_bytes(rng, 0, 300)))
        return "garbage greeting"

    if choice in (4, 5):  # a structurally plausible but wrong greeting
        client.read_frame()
        capabilities = rng.choice(
            [0, CLIENT_PROTOCOL_41, BASE_CAPABILITIES,
             BASE_CAPABILITIES & ~CLIENT_PLUGIN_AUTH,
             rng.getrandbits(32)])
        username = random_bytes(rng, 0, 40).replace(b"\x00", b"")
        auth = random_bytes(rng, 0, 260)
        plugin = rng.choice(
            [b"caching_sha2_password", b"mysql_native_password",
             b"sha256_password", random_bytes(rng, 0, 30)]
        ).replace(b"\x00", b"")
        client.send(frame(1, greeting_payload(
            capabilities, username, auth, plugin)))
        # the server may answer with an auth-method switch; reply with garbage
        read = client.read_frame()
        if read is not None and read[1][:1] == b"\xfe":
            client.send(frame(3, random_bytes(rng, 0, 64)))
        return "malformed greeting fields"

    if choice == 6:  # wrong sequence number in the greeting
        client.read_frame()
        salt = b"\x00" * 20
        client.send(frame(rng.choice([0, 2, 5, 255]), greeting_payload(
            BASE_CAPABILITIES, PULL_USER.encode(),
            scramble(PULL_PASSWORD, salt), b"caching_sha2_password")))
        client.read_frame()
        return "bad sequence number"

    if choice == 7:  # trigger the auth-method switch, answer with garbage
        salt = client.server_salt()
        client.send(frame(1, greeting_payload(
            BASE_CAPABILITIES, PULL_USER.encode(),
            random_bytes(rng, 0, 32), b"mysql_native_password")))
        read = client.read_frame()
        if read is not None:
            client.send(frame(3, random_bytes(rng, 0, 300)))
        return "auth switch garbage"

    # the rest need a completed handshake: a garbage command in the loop
    if not client.handshake():
        return "handshake did not complete"

    if choice == 8:  # a command with a random command byte and body
        client.send(frame(0, bytes([rng.randint(0, 255)]) +
                          random_bytes(rng, 0, 200)))
        client.read_frame()
        return "random command"

    if choice == 9:  # truncated or oversized COM_BINLOG_DUMP
        body = bytearray([COM_BINLOG_DUMP])
        body += random_bytes(rng, 0, rng.choice([0, 3, 6, 10, 200]))
        client.send(frame(0, bytes(body)))
        client.read_frame()
        return "malformed binlog dump"

    if choice == 10:  # COM_BINLOG_DUMP_GTID with random gtid data
        body = bytearray([COM_BINLOG_DUMP_GTID])
        body += random_bytes(rng, 0, 300)
        client.send(frame(0, bytes(body)))
        client.read_frame()
        return "malformed binlog dump gtid"

    if choice == 11:  # a COM_QUERY with a huge or odd statement
        statement = random_bytes(rng, 0, 4000)
        client.send(frame(0, bytes([COM_QUERY]) + statement))
        client.read_frame()
        return "garbage query"

    if choice == 12:  # a well-formed but unknown query string
        statement = rng.choice(
            [b"SELECT " + random_bytes(rng, 0, 50),
             b"SET @x = " + random_bytes(rng, 0, 50),
             b""]).replace(b"\x00", b"")
        client.send(frame(0, bytes([COM_QUERY]) + statement))
        client.read_frame()
        return "unknown query"

    # choice == 13: a valid binlog dump followed by an early close
    client.send(frame(0, bytes([COM_BINLOG_DUMP]) + struct.pack("<I", 4) +
                      struct.pack("<H", 1) + struct.pack("<I", 4242) +
                      b"binlog.000001"))
    client.read_frame()
    return "binlog dump then close"


def streams_binlogs(host: str, port: int) -> bool:
    """A clean handshake plus a non-blocking COM_BINLOG_DUMP; returns True if
    the listener streams at least one binlog event back."""
    client = Listener(host, port, timeout=5.0)
    try:
        if not client.handshake():
            return False
        # flags = 1 (BINLOG_DUMP_NON_BLOCK), from binlog.000001 position 4
        client.send(frame(0, bytes([COM_BINLOG_DUMP]) + struct.pack("<I", 4) +
                          struct.pack("<H", 1) + struct.pack("<I", 4242) +
                          b"binlog.000001"))
        got_event = False
        for _ in range(50):
            read = client.read_frame()
            if read is None:
                break
            body = read[1]
            if body[:1] == b"\xfe" and len(body) < 9:  # EOF
                break
            if body[:1] == b"\x00" and len(body) > 20:  # a binlog event
                got_event = True
        return got_event
    finally:
        client.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--mysql-basedir", type=Path, required=True)
    parser.add_argument("--binsrv", type=Path, required=True)
    parser.add_argument("--workdir", type=Path, required=True)
    parser.add_argument("--port", type=int, default=23490)
    parser.add_argument("--mode", choices=["gtid", "position"],
                        default="position")
    parser.add_argument("--connections", type=int, default=3000)
    parser.add_argument("--seed", type=int, default=1)
    args = parser.parse_args()
    args.mysql_basedir = args.mysql_basedir.expanduser()
    args.binsrv = args.binsrv.expanduser()
    args.workdir.mkdir(parents=True, exist_ok=True)

    gtid_mode = args.mode == "gtid"
    source = harness.MySQLServer(args.mysql_basedir, args.workdir / "mysql",
                                 args.port, gtid_mode, server_id=1)
    harness.SERVER = source
    binsrv = harness.BinlogServer(args.binsrv, args.workdir / "binsrv",
                                  args.port, gtid_mode)
    harness.BINSRV = binsrv
    pull_port = args.port + 2
    rng = random.Random(args.seed)
    result = {"mode": args.mode, "connections": args.connections,
              "seed": args.seed}
    pull = None
    source.start()
    try:
        machine = harness.FetchCrashMachine()
        machine.start_from_scratch()
        machine.run_workload(WORKLOAD)
        binsrv.checkpoint_size = "1"
        machine.fetch_to_the_end()
        binsrv.write_config()
        config = json.loads(binsrv.config_path.read_text())
        config["replication_source"] = {
            "port": pull_port, "read_timeout": 3, "write_timeout": 3,
            "authentication": {"user": PULL_USER, "password": PULL_PASSWORD,
                               "plugin": "caching_sha2_password"}}
        binsrv.config_path.write_text(json.dumps(config, indent=2))
        pull = subprocess.Popen(
            [str(binsrv.binary), "pull", str(binsrv.config_path)],
            stdout=subprocess.DEVNULL, stderr=subprocess.PIPE,
            env=dict(os.environ, AWS_EC2_METADATA_DISABLED="true",
                     ASAN_OPTIONS="abort_on_error=1:exitcode=134"))

        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            try:
                socket.create_connection(("127.0.0.1", pull_port),
                                         timeout=1).close()
                break
            except OSError:
                if pull.poll() is not None:
                    raise harness.Failure("pull exited before it listened")
                time.sleep(0.2)
        else:
            raise harness.Failure("pull did not start listening in 30 s")

        if not streams_binlogs("127.0.0.1", pull_port):
            raise harness.Failure("the listener does not stream binlogs even "
                                  "before any fuzzing")

        labels: dict = {}
        crashed_at = None
        for index in range(args.connections):
            client = Listener("127.0.0.1", pull_port)
            try:
                label = scenario(rng, client)
            except (OSError, ValueError):
                label = "client error"
            finally:
                client.close()
            labels[label] = labels.get(label, 0) + 1
            if pull.poll() is not None:
                crashed_at = index
                (args.workdir / "crash.bin").write_bytes(bytes(client.sent))
                break
            if index % 500 == 0 and index:
                print(json.dumps({"progress": index}), flush=True)

        result["scenarios"] = labels
        if crashed_at is not None:
            stderr = pull.stderr.read().decode(errors="replace")
            result["crashed"] = True
            result["crashed_at"] = crashed_at
            result["exit"] = pull.returncode
            result["last_scenario"] = label
            result["stderr_tail"] = stderr[-2000:]
        else:
            result["crashed"] = False
            result["still_serves"] = streams_binlogs("127.0.0.1", pull_port)
    finally:
        if pull is not None and pull.poll() is None:
            pull.send_signal(signal.SIGTERM)
            try:
                pull.wait(timeout=60)
            except subprocess.TimeoutExpired:
                pull.kill()
        source.stop()

    print(json.dumps(result), flush=True)
    (args.workdir / "result.json").write_text(json.dumps(result, indent=2))
    ok = not result.get("crashed", True) and result.get("still_serves", False)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
