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

"""Randomized crash / resume test for 'binlog_server fetch' against a real
MySQL server, as a Hypothesis (https://hypothesis.works) stateful test.

Every test case starts from empty binary logs on a dedicated MySQL server and
an empty Binlog Server storage, then runs a generated sequence of steps:

- running a workload on the server: transactions with rows of various sizes
  (up to 17 MB, above the 16 MB packet limit), several statements per
  transaction, and DDL statements;
- 'FLUSH BINARY LOGS';
- running 'binlog_server fetch' until it has caught up (it must exit with 0);
- starting 'binlog_server fetch' while a workload runs and killing it with
  SIGKILL once the storage holds a generated number of bytes (or after a
  timeout);
- changing the storage checkpoint settings used by later runs.

After every step, each binlog file in the storage must be a prefix of the
server's binlog file with the same name, ending at a transaction boundary,
and listed in 'binlog.index' in order. After a 'fetch' that ran to the end,
the storage must hold exactly the server's binlog files, byte for byte.

Requirements: Python 3.10+, Hypothesis (found through PYTHONPATH or
installed), a MySQL 8.0 / 8.4 binary distribution and a 'binlog_server'
binary. Example:

  PYTHONPATH=~/ws/qa/pylib python3 fetch_crash_test.py \\
    --mysql-basedir ~/ws/dist/mysql-8.4.8-linux-glibc2.28-x86_64-minimal \\
    --binsrv ~/ws/percona-binlog-server-build-debug_gcc14/binlog_server \\
    --mode gtid --examples 20 --workdir ~/ws/qa/run-gtid
"""

import argparse
import json
import os
import shutil
import signal
import struct
import subprocess
import sys
import threading
import time
import zlib
from pathlib import Path

from hypothesis import HealthCheck, Phase, settings
from hypothesis.database import DirectoryBasedExampleDatabase
from hypothesis import strategies as st
from hypothesis.stateful import (RuleBasedStateMachine, initialize,
                                 invariant, rule, run_state_machine_as_test)

REPLICATION_USER = "rpl"
# an empty password: full caching_sha2_password authentication needs a
# secure connection, which binlog_server does not use by default
REPLICATION_PASSWORD = ""
BINLOG_BASE_NAME = "binlog"
# small binary logs, so that the server rotates them on its own as well
MAX_BINLOG_SIZE = 65536
FETCH_TIMEOUT_SECONDS = 120
KILL_TIMEOUT_SECONDS = 5.0

# binlog event type codes
STOP_EVENT = 3
ROTATE_EVENT = 4
FORMAT_DESCRIPTION_EVENT = 15
GTID_LOG_EVENT = 33
ANONYMOUS_GTID_LOG_EVENT = 34
PREVIOUS_GTIDS_LOG_EVENT = 35
GTID_TAGGED_LOG_EVENT = 42
TRANSACTION_START_EVENTS = {GTID_LOG_EVENT, ANONYMOUS_GTID_LOG_EVENT,
                            GTID_TAGGED_LOG_EVENT}
BINLOG_MAGIC = b"\xfebin"
COMMON_HEADER_SIZE = 19
FLAGS_OFFSET = 17
LOG_EVENT_BINLOG_IN_USE_F = 0x1
CHECKSUM_SIZE = 4


class Failure(AssertionError):
    pass


# ---------------------------------------------------------------------------
# MySQL server
# ---------------------------------------------------------------------------

class MySQLServer:
    def __init__(self, basedir: Path, workdir: Path, port: int,
                 gtid_mode: bool):
        self.basedir = basedir
        self.datadir = workdir / "data"
        self.socket = workdir / "mysqld.sock"
        self.error_log = workdir / "mysqld.err"
        self.port = port
        self.gtid_mode = gtid_mode
        self.process = None
        workdir.mkdir(parents=True, exist_ok=True)

    def _mysqld_args(self):
        return [
            str(self.basedir / "bin" / "mysqld"), "--no-defaults",
            f"--basedir={self.basedir}", f"--datadir={self.datadir}",
            f"--log-error={self.error_log}",
        ]

    def start(self):
        if not self.datadir.exists():
            subprocess.run(self._mysqld_args() + ["--initialize-insecure"],
                           check=True)
        gtid = "ON" if self.gtid_mode else "OFF"
        self.process = subprocess.Popen(self._mysqld_args() + [
            f"--port={self.port}", f"--socket={self.socket}",
            "--mysqlx=OFF", "--server-id=1",
            f"--log-bin={BINLOG_BASE_NAME}", "--binlog-format=ROW",
            f"--gtid-mode={gtid}", f"--enforce-gtid-consistency={gtid}",
            f"--max-binlog-size={MAX_BINLOG_SIZE}",
            "--max-allowed-packet=64M", "--innodb-buffer-pool-size=256M",
        ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                raise RuntimeError(
                    f"mysqld exited, see {self.error_log}")
            ping = subprocess.run(
                [str(self.basedir / "bin" / "mysqladmin"), "--no-defaults",
                 f"--socket={self.socket}", "-uroot", "ping"],
                capture_output=True)
            if ping.returncode == 0:
                break
            time.sleep(0.5)
        else:
            raise RuntimeError("mysqld did not start")
        self.sql(
            f"CREATE USER IF NOT EXISTS '{REPLICATION_USER}'@'%' "
            f"IDENTIFIED BY '{REPLICATION_PASSWORD}';"
            f"GRANT ALL ON *.* TO '{REPLICATION_USER}'@'%';")

    def stop(self):
        if self.process is None:
            return
        subprocess.run(
            [str(self.basedir / "bin" / "mysqladmin"), "--no-defaults",
             f"--socket={self.socket}", "-uroot", "shutdown"],
            capture_output=True)
        try:
            self.process.wait(timeout=120)
        except subprocess.TimeoutExpired:
            self.process.kill()
        self.process = None

    def sql(self, statements: str, max_packet: str = "64M") -> str:
        result = subprocess.run(
            [str(self.basedir / "bin" / "mysql"), "--no-defaults",
             f"--socket={self.socket}", "-uroot", "-N", "-B",
             f"--max-allowed-packet={max_packet}", "-e", statements],
            capture_output=True, text=True)
        if result.returncode != 0:
            raise RuntimeError(f"SQL failed: {result.stderr.strip()}\n"
                               f"{statements[:300]}")
        return result.stdout

    def version(self) -> tuple:
        text = self.sql("SELECT VERSION()").strip()
        return tuple(int(part) for part in text.split("-")[0].split("."))

    def reset(self):
        """Empty binary logs, GTID state and test tables."""
        self.sql("DROP DATABASE IF EXISTS qa; CREATE DATABASE qa;")
        if self.version() >= (8, 2, 0):
            self.sql("RESET BINARY LOGS AND GTIDS")
        else:
            self.sql("RESET MASTER")
        self.sql("CREATE TABLE qa.t (id INT UNSIGNED NOT NULL AUTO_INCREMENT "
                 "PRIMARY KEY, payload LONGBLOB) ENGINE=InnoDB")

    def binlog_names(self) -> list:
        rows = self.sql("SHOW BINARY LOGS").splitlines()
        return [row.split("\t")[0] for row in rows if row]

    def binlog_path(self, name: str) -> Path:
        return self.datadir / name

    def binlog_as_sent(self, name: str) -> bytes:
        """The binlog file as the server sends it to a replica: the server
        marks the FORMAT_DESCRIPTION event of the binlog it is writing with
        LOG_EVENT_BINLOG_IN_USE_F, and clears that flag (recomputing the
        event checksum) when sending it."""
        data = bytearray(self.binlog_path(name).read_bytes())
        events = parse_events(bytes(data))
        if events and events[0][1] == FORMAT_DESCRIPTION_EVENT:
            offset, _, size = events[0]
            flags = struct.unpack_from("<H", data, offset + FLAGS_OFFSET)[0]
            if flags & LOG_EVENT_BINLOG_IN_USE_F:
                struct.pack_into("<H", data, offset + FLAGS_OFFSET,
                                 flags & ~LOG_EVENT_BINLOG_IN_USE_F)
                # a FORMAT_DESCRIPTION event always ends with a checksum
                payload_end = offset + size - CHECKSUM_SIZE
                struct.pack_into("<I", data, payload_end,
                                 zlib.crc32(data[offset:payload_end]))
        return bytes(data)


# ---------------------------------------------------------------------------
# binlog files
# ---------------------------------------------------------------------------

def parse_events(data: bytes) -> list:
    """(offset, type code, size) of every complete event."""
    if data[:4] != BINLOG_MAGIC:
        raise Failure("binlog file does not start with the binlog magic")
    events = []
    offset = 4
    while offset + COMMON_HEADER_SIZE <= len(data):
        type_code = data[offset + 4]
        size = struct.unpack_from("<I", data, offset + 9)[0]
        if size < COMMON_HEADER_SIZE or offset + size > len(data):
            break
        events.append((offset, type_code, size))
        offset += size
    return events


def transaction_boundaries(data: bytes) -> set:
    """Lengths at which a prefix of a binlog file holds no partial
    transaction: the magic, after each preamble event, before each
    transaction, before and after the closing ROTATE / STOP, and the end of
    the file."""
    events = parse_events(data)
    result = {4}
    for offset, type_code, size in events:
        if type_code in TRANSACTION_START_EVENTS or \
                type_code in (ROTATE_EVENT, STOP_EVENT):
            result.add(offset)
        if type_code in (FORMAT_DESCRIPTION_EVENT, PREVIOUS_GTIDS_LOG_EVENT,
                         ROTATE_EVENT, STOP_EVENT):
            result.add(offset + size)
    if events:
        offset, type_code, size = events[-1]
        end = offset + size
        # the end of a complete transaction at the end of the file
        result.add(end)
    return result


# ---------------------------------------------------------------------------
# binlog_server
# ---------------------------------------------------------------------------

class BinlogServer:
    def __init__(self, binary: Path, workdir: Path, port: int,
                 gtid_mode: bool):
        self.binary = binary
        self.workdir = workdir
        self.port = port
        self.gtid_mode = gtid_mode
        self.storage = workdir / "storage"
        self.config_path = workdir / "binsrv.json"
        self.log_path = workdir / "binsrv.log"
        self.checkpoint_size = None
        self.checkpoint_interval = None

    def reset(self):
        shutil.rmtree(self.storage, ignore_errors=True)
        self.storage.mkdir(parents=True)
        self.log_path.unlink(missing_ok=True)
        self.checkpoint_size = None
        self.checkpoint_interval = None

    def write_config(self):
        storage = {"backend": "file", "uri": f"file://{self.storage}"}
        if self.checkpoint_size is not None:
            storage["checkpoint_size"] = self.checkpoint_size
        if self.checkpoint_interval is not None:
            storage["checkpoint_interval"] = self.checkpoint_interval
        config = {
            "logger": {"level": "info", "file": str(self.log_path)},
            "connection": {
                "host": "127.0.0.1", "port": self.port,
                "user": REPLICATION_USER, "password": REPLICATION_PASSWORD,
                "connect_timeout": 20, "read_timeout": 60,
                "write_timeout": 60},
            "replication": {
                "server_id": 4242, "idle_time": 1, "verify_checksum": True,
                "mode": "gtid" if self.gtid_mode else "position"},
            "replication_source": {
                "port": 0xFFFF, "read_timeout": 60, "write_timeout": 60,
                "authentication": {
                    "user": "unused", "password": "unused",
                    "plugin": "caching_sha2_password"}},
            "storage": storage,
        }
        self.config_path.write_text(json.dumps(config, indent=2))

    def start_fetch(self) -> subprocess.Popen:
        self.write_config()
        return subprocess.Popen(
            [str(self.binary), "fetch", str(self.config_path)],
            stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)

    def stored_data_size(self) -> int:
        total = 0
        for path in self.storage.iterdir():
            if path.name.startswith(BINLOG_BASE_NAME + ".") and \
                    path.suffix not in (".json", ".index", ".tmp"):
                total += path.stat().st_size
        return total

    def indexed_binlogs(self) -> list:
        index = self.storage / "binlog.index"
        if not index.exists():
            return []
        return [Path(line).name for line in index.read_text().splitlines()
                if line]

    def log_tail(self, lines: int = 15) -> str:
        if not self.log_path.exists():
            return "<no log>"
        return "\n".join(self.log_path.read_text(
            errors="replace").splitlines()[-lines:])


# ---------------------------------------------------------------------------
# the state machine
# ---------------------------------------------------------------------------

# set up once per run by main()
SERVER: MySQLServer = None
BINSRV: BinlogServer = None

transactions = st.lists(
    st.fixed_dictionaries({
        # mostly small rows, sometimes large ones, rarely one above the 16 MB
        # packet limit
        "rows": st.lists(
            st.integers(0, 30).flatmap(
                lambda kind: st.just(17 * 1024 * 1024) if kind == 0 else
                st.integers(2000, 200_000) if kind < 6 else
                st.integers(0, 2000)),
            min_size=1, max_size=4),
        "ddl": st.booleans(),
    }),
    min_size=1, max_size=6)


def workload_sql(transaction_list: list) -> list:
    statements = []
    for index, transaction in enumerate(transaction_list):
        if transaction["ddl"]:
            statements.append(
                f"CREATE TABLE IF NOT EXISTS qa.d{index} (x INT); "
                f"DROP TABLE qa.d{index};")
        rows = ", ".join(
            f"(REPEAT(CHAR({65 + (size % 26)}), {size}))"
            for size in transaction["rows"])
        statements.append(f"BEGIN; INSERT INTO qa.t (payload) VALUES {rows}; "
                          f"COMMIT;")
    return statements


class FetchCrashMachine(RuleBasedStateMachine):
    @initialize()
    def start_from_scratch(self):
        SERVER.reset()
        BINSRV.reset()
        self.steps = []

    def note(self, text: str):
        self.steps.append(text)

    @rule(transaction_list=transactions)
    def run_workload(self, transaction_list):
        self.note(f"workload {transaction_list}")
        for statement in workload_sql(transaction_list):
            SERVER.sql(statement)

    @rule()
    def flush_binary_logs(self):
        self.note("FLUSH BINARY LOGS")
        SERVER.sql("FLUSH BINARY LOGS")

    @rule(checkpoint_size=st.sampled_from([None, "1", "4K", "64K", "1M"]),
          checkpoint_interval=st.sampled_from([None, "1s"]))
    def change_checkpoints(self, checkpoint_size, checkpoint_interval):
        self.note(f"checkpoints size={checkpoint_size} "
                  f"interval={checkpoint_interval}")
        BINSRV.checkpoint_size = checkpoint_size
        BINSRV.checkpoint_interval = checkpoint_interval

    @rule()
    def fetch_to_the_end(self):
        self.note("fetch to the end")
        process = BINSRV.start_fetch()
        try:
            _, stderr = process.communicate(timeout=FETCH_TIMEOUT_SECONDS)
        except subprocess.TimeoutExpired:
            process.kill()
            process.communicate()
            raise Failure("fetch did not finish in "
                          f"{FETCH_TIMEOUT_SECONDS} s\n{BINSRV.log_tail()}")
        if process.returncode != 0:
            raise Failure(f"fetch exited with {process.returncode}\n"
                          f"{stderr.decode(errors='replace')[-2000:]}\n"
                          f"{BINSRV.log_tail()}")
        self.check_complete_copy()

    @rule(kill_after_bytes=st.integers(0, 400_000),
          transaction_list=transactions)
    def fetch_and_kill(self, kill_after_bytes, transaction_list):
        self.note(f"fetch killed after {kill_after_bytes} bytes during "
                  f"workload {transaction_list}")
        start_size = BINSRV.stored_data_size()
        process = BINSRV.start_fetch()
        workload_error = []

        def workload():
            try:
                for statement in workload_sql(transaction_list):
                    SERVER.sql(statement)
            except Exception as error:  # reported after the kill
                workload_error.append(error)

        thread = threading.Thread(target=workload)
        thread.start()
        deadline = time.monotonic() + KILL_TIMEOUT_SECONDS
        while (process.poll() is None and time.monotonic() < deadline and
               BINSRV.stored_data_size() - start_size < kill_after_bytes):
            time.sleep(0.005)
        if process.poll() is None:
            process.send_signal(signal.SIGKILL)
        process.communicate()
        thread.join()
        if workload_error:
            raise workload_error[0]

    @invariant()
    def storage_holds_prefixes(self):
        if not hasattr(self, "steps"):
            return
        indexed = BINSRV.indexed_binlogs()
        server_names = SERVER.binlog_names()
        if indexed != server_names[:len(indexed)]:
            raise Failure(f"binlog.index lists {indexed}, the server has "
                          f"{server_names}")
        for name in indexed:
            stored_path = BINSRV.storage / name
            if not stored_path.exists():
                raise Failure(f"{name} is in binlog.index but not stored")
            stored = stored_path.read_bytes()
            source = SERVER.binlog_as_sent(name)
            if source[:len(stored)] != stored:
                mismatch = next(i for i in range(len(stored))
                                if i >= len(source) or stored[i] != source[i])
                raise Failure(f"{name}: the stored file ({len(stored)} bytes) "
                              f"differs from the server's ({len(source)} "
                              f"bytes) at byte {mismatch}")
            # after a crash the last binlog may hold a partial transaction
            # until the next start truncates it to the size in its metadata,
            # so the metadata size is what must be at a transaction boundary
            metadata_path = BINSRV.storage / (name + ".json")
            size = len(stored)
            if metadata_path.exists():
                size = json.loads(metadata_path.read_text())["size"]
            if size > len(stored):
                raise Failure(f"{name}: the metadata size {size} is beyond "
                              f"the stored file ({len(stored)} bytes)")
            if size not in transaction_boundaries(source):
                raise Failure(f"{name}: the metadata size {size} is not at a "
                              f"transaction boundary of the server's file")

    def check_complete_copy(self):
        server_names = SERVER.binlog_names()
        indexed = BINSRV.indexed_binlogs()
        if indexed != server_names:
            raise Failure(f"after a complete fetch binlog.index lists "
                          f"{indexed}, the server has {server_names}")
        for name in server_names:
            stored = (BINSRV.storage / name).read_bytes()
            source = SERVER.binlog_as_sent(name)
            # the storage writes the preamble (FORMAT_DESCRIPTION,
            # PREVIOUS_GTIDS_LOG) of a binlog together with its first
            # transaction, so the binlog being written holds only the magic
            # as long as it has no transactions
            if name == server_names[-1] and stored == BINLOG_MAGIC and \
                    not any(type_code in TRANSACTION_START_EVENTS for
                            _, type_code, _ in parse_events(source)):
                continue
            if stored != source:
                mismatch = next((i for i in range(min(len(stored),
                                                      len(source)))
                                 if stored[i] != source[i]),
                                min(len(stored), len(source)))
                raise Failure(f"after a complete fetch {name} has "
                              f"{len(stored)} bytes, the server's "
                              f"{len(source)} bytes, first difference at "
                              f"byte {mismatch}")

    def teardown(self):
        if getattr(self, "steps", None) is not None:
            (BINSRV.workdir / "last_steps.txt").write_text(
                "\n".join(self.steps) + "\n")


def main():
    global SERVER, BINSRV
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--mysql-basedir", type=Path, required=True)
    parser.add_argument("--binsrv", type=Path, required=True)
    parser.add_argument("--mode", choices=["gtid", "position"],
                        required=True)
    parser.add_argument("--workdir", type=Path, required=True)
    parser.add_argument("--port", type=int, default=23306)
    parser.add_argument("--examples", type=int, default=20)
    parser.add_argument("--steps", type=int, default=10)
    args = parser.parse_args()

    gtid_mode = args.mode == "gtid"
    args.workdir.mkdir(parents=True, exist_ok=True)
    SERVER = MySQLServer(args.mysql_basedir.expanduser(),
                         args.workdir / "mysql", args.port, gtid_mode)
    BINSRV = BinlogServer(args.binsrv.expanduser(), args.workdir / "binsrv",
                          args.port, gtid_mode)
    SERVER.start()
    try:
        test_settings = settings(
            max_examples=args.examples, stateful_step_count=args.steps,
            deadline=None, suppress_health_check=list(HealthCheck),
            database=DirectoryBasedExampleDatabase(
                str(args.workdir / "hypothesis-db")),
            derandomize=False, print_blob=True,
            phases=[Phase.explicit, Phase.reuse, Phase.generate,
                    Phase.shrink])
        run_state_machine_as_test(FetchCrashMachine, settings=test_settings)
        print(f"OK: {args.examples} examples in {args.mode} mode")
        return 0
    finally:
        SERVER.stop()


if __name__ == "__main__":
    sys.exit(main())
