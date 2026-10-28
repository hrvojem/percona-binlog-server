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

"""Writes seed inputs for the libFuzzer targets in this directory.

event_stream/: real binlogs from a throwaway MySQL server (GTID and position
mode, a varied workload with rotations), each turned into the stream a source
sends: the target's selector byte, an artificial ROTATE, then the binlog's
events, cut at an event boundary to stay below MAX_SEED_SIZE.

listener_packet/: client sessions in the MySQL wire format: a client
greeting for caching_sha2_password, a request for the server's public key, an
"encrypted password", then the statements and commands a replica sends.

Uses the MySQL server helper of extra/qa/fetch_crash_test.py. Example:

  PYTHONPATH=~/ws/qa/pylib python3 fuzz/make_seeds.py \\
    --mysql-basedir ~/ws/dist/mysql-8.4.8-linux-glibc2.28-x86_64-minimal \\
    --workdir /tmp/fuzz-seeds-mysql --out ~/ws/fuzz/seeds
"""

import argparse
import os
import struct
import sys
import zlib
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "extra" / "qa"))

import fetch_crash_test as harness  # noqa: E402

MAX_SEED_SIZE = 60_000
HEADER_SIZE = 19
ROTATE_EVENT = 4
ARTIFICIAL_FLAG = 0x20

WORKLOAD = [
    [{"rows": [100, 2000], "ddl": False}],
    [{"rows": [30_000], "ddl": True}],
    [{"rows": [0, 5, 500], "ddl": False}, {"rows": [70_000], "ddl": False}],
]


def artificial_rotate(name: str, with_checksum: bool) -> bytes:
    body = struct.pack("<Q", 4) + name.encode()
    size = HEADER_SIZE + len(body) + (4 if with_checksum else 0)
    event = struct.pack("<IBIIIH", 0, ROTATE_EVENT, 1, size, 0,
                        ARTIFICIAL_FLAG) + body
    if with_checksum:
        event += struct.pack("<I", zlib.crc32(event))
    return event


def events_of(data: bytes) -> list:
    """The events of a binlog file, after its 4-byte magic."""
    events, offset = [], 4
    while offset + HEADER_SIZE <= len(data):
        size = struct.unpack_from("<I", data, offset + 9)[0]
        if size < HEADER_SIZE or offset + size > len(data):
            break
        events.append(data[offset:offset + size])
        offset += size
    return events


def stream_seeds(args, out: Path) -> int:
    out.mkdir(parents=True, exist_ok=True)
    count = 0
    for gtid_mode in (False, True):
        server = harness.MySQLServer(args.mysql_basedir,
                                     args.workdir / ("gtid" if gtid_mode
                                                     else "position"),
                                     args.port, gtid_mode)
        harness.SERVER = server
        server.start()
        try:
            server.reset()
            for transactions in WORKLOAD:
                for statement in harness.workload_sql(transactions):
                    server.sql(statement)
                server.sql("FLUSH BINARY LOGS")
            for name in server.binlog_names()[:-1]:
                events = events_of(server.binlog_as_sent(name))
                for with_checksum in (False, True):
                    stream = artificial_rotate(name, with_checksum)
                    for event in events:
                        if len(stream) + len(event) + 1 > MAX_SEED_SIZE:
                            break
                        stream += event
                    # selector: mode bit, checksum bit, server version 8.4
                    selector = (1 if gtid_mode else 0) | (2 if with_checksum
                                                          else 0) | (1 << 2)
                    (out / f"{'gtid' if gtid_mode else 'pos'}-{name}-"
                           f"{int(with_checksum)}").write_bytes(
                               bytes([selector]) + stream)
                    count += 1
        finally:
            server.stop()
    return count


def frame(sequence: int, payload: bytes) -> bytes:
    return struct.pack("<I", len(payload))[:3] + bytes([sequence]) + payload


def lenenc_string(value: bytes) -> bytes:
    return bytes([len(value)]) + value


def client_greeting(user: str, plugin: str) -> bytes:
    # CLIENT_LONG_PASSWORD | CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION |
    # CLIENT_PLUGIN_AUTH | CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA
    capabilities = 0x1 | 0x200 | 0x8000 | 0x80000 | 0x200000
    return (struct.pack("<IIB", capabilities, 1 << 24, 255) + bytes(23) +
            user.encode() + b"\0" + lenenc_string(os.urandom(32)) +
            plugin.encode() + b"\0")


def binlog_dump(name: str, position: int, flags: int) -> bytes:
    return (bytes([0x12]) + struct.pack("<IHI", position, flags, 2) +
            name.encode())


def listener_seeds(out: Path) -> int:
    out.mkdir(parents=True, exist_ok=True)
    statements = [
        b"SELECT UNIX_TIMESTAMP()", b"SELECT @@GLOBAL.SERVER_ID",
        b"SET @master_heartbeat_period = 30000000000, "
        b"@source_heartbeat_period = 30000000000",
        b"SET @master_binlog_checksum = @@global.binlog_checksum, "
        b"@source_binlog_checksum = @@global.binlog_checksum",
        b"SET @source_binlog_checksum = 'NONE', "
        b"@master_binlog_checksum = 'NONE'",
        b"SELECT @source_binlog_checksum", b"SELECT @@GLOBAL.GTID_MODE",
        b"SELECT @@GLOBAL.SERVER_UUID",
        b"SET @slave_uuid = '11111111-2222-3333-4444-555555555555', "
        b"@replica_uuid = '11111111-2222-3333-4444-555555555555'",
        b"SET NAMES utf8mb4", b"select @@version_comment limit 1"]
    sessions = {
        "replica": [frame(0, b"\x03" + s) for s in statements] +
                   [frame(0, b"\x15" + struct.pack("<I", 2) + bytes(10)),
                    frame(0, binlog_dump("binlog.000001", 4, 0))],
        "mysqlbinlog": [frame(0, b"\x03" + statements[4]),
                        frame(0, b"\x0e"),
                        frame(0, binlog_dump("binlog.000002", 127, 1))],
        "ping": [frame(0, b"\x0e"), frame(0, b"\x01")],
    }
    count = 0
    for name, commands in sessions.items():
        for plugin in ("caching_sha2_password", "mysql_native_password"):
            auth = [frame(1, client_greeting("rpl", plugin))]
            if plugin == "caching_sha2_password":
                # a request for the public key, then the "encrypted password"
                auth += [frame(3, b"\x02"), frame(5, os.urandom(256))]
            else:
                auth += [frame(3, os.urandom(32))]
            (out / f"{name}-{plugin}").write_bytes(b"".join(auth + commands))
            count += 1
    return count


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--mysql-basedir", type=Path, required=True)
    parser.add_argument("--workdir", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--port", type=int, default=23580)
    args = parser.parse_args()
    args.mysql_basedir = args.mysql_basedir.expanduser()
    args.workdir.mkdir(parents=True, exist_ok=True)
    streams = stream_seeds(args, args.out / "event_stream")
    packets = listener_seeds(args.out / "listener_packet")
    print(f"{streams} event stream seeds, {packets} listener packet seeds")


if __name__ == "__main__":
    sys.exit(main())
