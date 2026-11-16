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

"""A hostile source against a long-running 'pull' that is serving replicas.

hostile_source_test.py and the source_* cases of rewrite_attack_test.py put a
man in the middle between 'fetch' and the source. 'fetch' stores and exits;
'pull' goes on: it reconnects after an error, and all the time it serves what
it has stored to replicas through its own listener. Here the same attacks
(the proxies and the attack definitions are imported from those two
harnesses) run against a 'pull' whose source connection goes through the
proxy, while a MySQL replica and a 'mysqlbinlog --read-from-remote-server
--raw --stop-never' client are connected to its listener.

Each case:

1. the source gets four transactions; 'pull' starts through the proxy (which
   changes nothing yet) and stores them; the replica (position 4 of the first
   binlog, or SOURCE_AUTO_POSITION=1) and the mysqlbinlog client connect and
   catch up;
2. the attack starts (for most attacks the proxy also drops the source
   connection, so that 'pull' resumes through the attacker as 'fetch' did)
   and the source gets five more transactions. For --observe seconds the
   harness records whether 'pull' exits (code, last errors), keeps running,
   or reconnects in a loop (connections seen by the proxy, the shortest
   interval between them, reconnects and log bytes per minute);
3. what the replica has (rows, GTID_EXECUTED, the errors of its threads) and
   what the mysqlbinlog client got is compared with the source; a second,
   one-shot 'mysqlbinlog --to-last-log' shows what 'pull' serves at that
   moment;
4. 'pull' is stopped (SIGTERM), the storage is judged ('list' works; without
   rewrite every stored binlog is a prefix of the source's binlog of that
   name and ends at a transaction boundary; with rewrite the stored GTID
   stream, walked with rewrite_attack_test.examine_stream, holds the
   source's GTIDs once and in order);
5. the attacker is gone: the source gets two more transactions and 'pull' is
   started again without the proxy. Within --recover seconds the storage
   must hold everything the source has and the replica must have the
   source's rows and GTID_EXECUTED (if its threads stopped, they are started
   once more and that is reported).

A case PASSES when 'pull' refuses cleanly (it stops with a non-zero exit and
an error, or reconnects no faster than 'replication.idle_time') and neither
the storage, the replica nor the mysqlbinlog client got anything that is not
the source's. It FAILS on storage damage, on bytes or GTIDs that are not the
source's (the ROTATE event with a name the source never used counts), on a
replica that stops with an error, diverges or silently lacks data, on a
crash by signal or a sanitizer report, on a hang, on a retry loop faster
than idle_time, and when the clean restart does not recover. Every result
carries 'fetch_known' (what the same attack does to 'fetch', from
nightly/qa_known_failures.txt) and 'vs_fetch' (same / better / worse by the
checks hostile_source_test.py and rewrite_attack_test.py make).

Cases (--cases selects; the default list runs in under ten minutes):

  control                the proxy drops the connection once and changes
                         nothing
  nul_existing_binlog, nul_metadata, nul_index, newline, dot_dot, long_name,
  reannounce, lower_sequence
                         the names of hostile_source_test.py (with
                         --rewrite: the NAME_CASES of rewrite_attack_test.py,
                         e.g. nul_own_binlog, own_file, own_next) for the
                         source's next binlog
  size_lie, gtid_bloat   an event that claims 0xFFFFFF00 bytes; a
                         PREVIOUS_GTIDS_LOG with 900,000 intervals
  reannounce_midstream   the open binlog announced again at position 4
  gtid_twice, gtid_swapped, gtid_other_uuid, length_larger, length_smaller,
  clock_invalid, previous_gtids_more, previous_gtids_empty,
  fde_checksum_off, fde_version, checksum_stripped, heartbeat
                         the transaction attacks of rewrite_attack_test.py;
                         they are repeated on every connection of 'pull'

and attacks that need a long-running process (repeated on every connection
while the attack lasts):

  stall_mid_event        after a GTID event the proxy sends half of the next
                         event and then nothing, keeping the connection
                         open: connection.read_timeout (4 s in this case)
                         must fire, the listener must keep serving, and
                         SIGTERM must stop 'pull'
  drop_mid_transaction   the connection is closed after GTID + BEGIN, every
                         time: the partial transaction must never be stored
                         or served
  replay_after_reconnect after a reconnect the proxy first sends again the
                         last transaction 'pull' has already stored, then
                         the real stream
  err_mid_transaction, eof_mid_transaction
                         an ERR packet (1236) / an EOF packet after GTID +
                         BEGIN, then the connection is closed
  err_in_preamble, eof_in_preamble
                         the same right after the artificial ROTATE that
                         starts the dump

For these the harness also stops the attack while 'pull' is still running
and records whether it catches up without a restart.

--mode position (default) or gtid; --rewrite (GTID mode only) adds
'replication.rewrite' (the results then carry "mode": "gtid_rewrite"). A JSON
line per case, then a summary; result.json in the work directory; exit 1 if
any case failed. Example:

  PYTHONPATH=~/ws/qa/pylib python3 hostile_source_pull_test.py \\
    --mysql-basedir ~/ws/dist/mysql-8.4.8-linux-glibc2.28-x86_64-minimal \\
    --binsrv ~/ws/percona-binlog-server-build-debug_gcc14/binlog_server \\
    --workdir ~/ws/qa/hostile-source-pull --mode gtid --rewrite
"""

import argparse
import json
import re
import shutil
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
import hostile_source_test as hostile  # noqa: E402
import metadata_tamper_test as tamper  # noqa: E402
import pull_replica_test as replication  # noqa: E402
import rewrite_attack_test as rewrite  # noqa: E402

IDLE_TIME = 1
READ_TIMEOUT = 30
STALL_READ_TIMEOUT = 4
CATCH_UP_SECONDS = 30
XID_EVENT = 16
# connection errors of the replica's receiver while 'pull' is down
CONNECTION_ERRORS = ("0", "", "2002", "2003", "2013")
STORAGE_NAME = re.compile(r"binlog\.\d{6}(\.json)?|binlog\.index|"
                          r"metadata\.json")
DATA_NAME = re.compile(r"binlog\.\d{6}")
ERR_PACKET = (b"\xff" + struct.pack("<H", 1236) + b"#HY000" +
              b"hostile source: an error in the middle of the dump")
EOF_PACKET = b"\xfe\x00\x00\x02\x00"

NAME_CASES = [name for name in hostile.CASES if name != "case_variant"]
LIVE_CASES = ["stall_mid_event", "drop_mid_transaction",
              "replay_after_reconnect", "err_mid_transaction",
              "eof_mid_transaction", "err_in_preamble", "eof_in_preamble"]
TRANSACTION_CASES = [name for name in rewrite.TRANSACTION_ATTACKS
                     if name != "length_larger_last"]
# 'length_larger_last' needs the end of the dump, which 'pull' never sees
DEFAULT_CASES = {
    "position": ["control"] + NAME_CASES + [
        "size_lie", "reannounce_midstream", "gtid_twice",
        "length_smaller"] + LIVE_CASES,
    "gtid": ["control"] + NAME_CASES + [
        "size_lie", "gtid_bloat", "reannounce_midstream", "gtid_twice",
        "length_smaller"] + LIVE_CASES,
    "gtid_rewrite": ["control", "nul_own_binlog", "nul_metadata", "newline",
                     "reannounce", "own_file", "own_next", "size_lie",
                     "gtid_twice", "gtid_swapped", "gtid_other_uuid",
                     "length_smaller", "length_larger", "fde_checksum_off",
                     "checksum_stripped", "heartbeat"] + LIVE_CASES,
}
# what the same attack does to 'fetch' (nightly/qa_known_failures.txt)
FETCH_KNOWN = {
    "position": {"nul_existing_binlog": 59, "nul_metadata": 59,
                 "nul_index": 59, "newline": 59, "reannounce": 69,
                 "lower_sequence": 70},
    "gtid": {"nul_existing_binlog": 59, "nul_metadata": 59, "nul_index": 59,
             "newline": 59, "lower_sequence": 70,
             "reannounce_midstream": 69},
    "gtid_rewrite": {"gtid_twice": 81, "gtid_swapped": 81,
                     "gtid_other_uuid": 81, "length_smaller": 82},
}

# set by main()
ARGS = None
MODE = "position"
CASE = ""
REPLICA: harness.MySQLServer = None
PULL_PORT = 0


def all_cases() -> list:
    names = rewrite.NAME_CASES if MODE == "gtid_rewrite" else NAME_CASES
    return (["control"] + names + hostile.RESOURCE_CASES +
            hostile.STREAM_CASES + TRANSACTION_CASES + LIVE_CASES)


def kind_of(name: str) -> str:
    if name == "control":
        return "control"
    if name in LIVE_CASES:
        return "live"
    if name in TRANSACTION_CASES:
        return "transaction"
    if name in hostile.STREAM_CASES:
        return "stream"
    if name in hostile.RESOURCE_CASES:
        return "resource"
    return "name"


def wait_for(condition, seconds: float, step: float = 0.2) -> bool:
    deadline = time.monotonic() + seconds
    while True:
        if condition():
            return True
        if time.monotonic() >= deadline:
            return False
        time.sleep(step)


# ---------------------------------------------------------------------------
# the man in the middle, for a client that reconnects
# ---------------------------------------------------------------------------

class LongRunning:
    """What the proxies of the 'fetch' harnesses need in front of 'pull':
    one source connection at a time, with the per-connection state of the
    attack set back for each (so the attack is repeated on every
    connection), nothing changed before arm(), the time of every connection,
    and drop() to close the current one."""

    def prepare(self):
        self.armed = threading.Event()
        self.closed = False
        self.generation = 0
        self.connections = []
        self.connect_times = []
        self.banked = 0

    def arm(self, drop: bool):
        self.armed.set()
        if drop:
            self.drop()

    def disarm(self):
        self.armed.clear()
        self.drop()

    def drop(self):
        self.generation += 1
        connections, self.connections = self.connections, []
        for pair in connections:
            for sock in pair:
                try:
                    sock.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass

    def close(self):
        self.closed = True
        self.drop()
        super().close()

    def changed(self) -> int:
        return self.banked + self.rewritten

    def new_connection(self):
        self.banked += self.rewritten
        self.rewritten = 0

    def _accept(self):
        while True:
            try:
                client, _ = self.listener.accept()
            except OSError:
                return
            self.drop()
            try:
                server = socket.create_connection(
                    ("127.0.0.1", self.source_port), timeout=10)
                server.settimeout(None)
            except OSError:
                client.close()
                continue
            self.dumping.clear()
            self.new_connection()
            self.connections.append((client, server))
            self.connect_times.append(time.monotonic())
            threading.Thread(target=self._client_to_server,
                             args=(client, server), daemon=True).start()
            threading.Thread(target=self._quiet_server_to_client,
                             args=(server, client), daemon=True).start()

    def _quiet_server_to_client(self, server, client):
        try:
            self._server_to_client(server, client)
        except OSError:
            # 'pull' has closed the connection
            pass


class NameProxy(LongRunning, hostile.RenamingProxy):
    """hostile_source_test.RenamingProxy (rename, size_lie, gtid_bloat,
    reannounce_midstream)."""

    def __init__(self, listen_port: int, source_port: int, attack: str):
        self.prepare()
        self.injections = []
        super().__init__(listen_port, source_port, b"\xff", b"\xff", attack)

    def new_connection(self):
        super().new_connection()
        if self.injected_at is not None:
            self.injections.append(self.injected_at)
        self.in_next_binlog = False
        self.shift = 0
        self.preamble = {}
        self.transactions = 0
        self.last_log_pos = 0
        self.injected_at = None
        self.rewind = 0

    def _rewrite(self, payload: bytes) -> bytes:
        if not self.armed.is_set():
            return payload
        return super()._rewrite(payload)

    def _midstream(self, payload: bytes) -> list:
        if not self.armed.is_set():
            return [payload]
        return super()._midstream(payload)


class TransactionProxy(LongRunning, rewrite.AttackProxy):
    """rewrite_attack_test.AttackProxy: the target transaction is counted
    from the start of every connection."""

    def __init__(self, listen_port: int, source_port: int, attack: str,
                 source_uuid: str, target: int):
        self.prepare()
        super().__init__(listen_port, source_port, attack, source_uuid,
                         target)

    def new_connection(self):
        super().new_connection()
        self.sequence = 0
        self.pending = []
        self.seen = 0
        self.held = None
        self.continuation = False

    def _process(self, payload: bytes, length: int) -> list:
        if not self.armed.is_set():
            return [payload]
        return super()._process(payload, length)


class LiveProxy(LongRunning, hostile.RenamingProxy):
    """The attacks that need a long-running client (LIVE_CASES)."""

    def __init__(self, listen_port: int, source_port: int, attack: str):
        self.prepare()
        self.acted = []
        self.last_transaction = []
        super().__init__(listen_port, source_port, b"\xff", b"\xff", attack)

    def changed(self) -> int:
        return len(self.acted)

    def _server_to_client(self, server, client):
        generation = self.generation
        inserted = 0
        # events of the transaction being sent that were passed on
        in_transaction = 0
        current = []
        first_event = True
        replayed = False

        def send(payload: bytes, number: int):
            client.sendall(struct.pack("<I", len(payload))[:3] +
                           bytes([number & 0xFF]) + payload)

        try:
            while True:
                header = self._read_exactly(server, 4)
                if header is None:
                    break
                length = header[0] | header[1] << 8 | header[2] << 16
                payload = self._read_exactly(server, length) if length else b""
                if payload is None:
                    break
                number = header[3] + inserted
                if not self.dumping.is_set() or length >= 0xFFFFFF or \
                        len(payload) < 1 + hostile.HEADER_SIZE or \
                        payload[0] != 0:
                    send(payload, number)
                    continue
                code = payload[5]
                attack = self.attack if self.armed.is_set() else ""
                starts = code in harness.TRANSACTION_START_EVENTS
                if starts:
                    in_transaction = 0
                    current = []
                    if attack == "replay_after_reconnect" and \
                            not replayed and self.last_transaction:
                        replayed = True
                        for old in self.last_transaction:
                            send(old, number)
                            inserted += 1
                            number += 1
                        self.acted.append(time.monotonic())
                if attack == "stall_mid_event" and in_transaction == 1:
                    client.sendall(struct.pack("<I", len(payload))[:3] +
                                   bytes([number & 0xFF]) +
                                   payload[:len(payload) // 2])
                    self.acted.append(time.monotonic())
                    # nothing more, and the connection stays open
                    while self.generation == generation and not self.closed:
                        time.sleep(0.1)
                    break
                send(payload, number)
                if starts or in_transaction:
                    in_transaction += 1
                    current.append(payload)
                    if code == XID_EVENT:
                        self.last_transaction = current
                        current = []
                        in_transaction = 0
                what, _, where = attack.partition("_")
                hit = (where == "mid_transaction" and in_transaction == 2 or
                       where == "in_preamble" and first_event)
                first_event = False
                if hit:
                    self.acted.append(time.monotonic())
                    if what == "err":
                        send(ERR_PACKET, number + 1)
                    elif what == "eof":
                        send(EOF_PACKET, number + 1)
                    if what != "drop":
                        time.sleep(0.2)
                    break
        finally:
            for sock in (client, server):
                try:
                    sock.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass


def make_proxy(name: str, port: int):
    kind = kind_of(name)
    source = harness.SERVER
    if kind in ("live", "control"):
        return LiveProxy(port, source.port, name)
    if kind == "transaction":
        return TransactionProxy(
            port, source.port, name,
            source.sql("SELECT @@server_uuid").strip(),
            3 if name == "gtid_swapped" else 2)
    return NameProxy(port, source.port,
                     "rename" if kind == "name" else name)


# ---------------------------------------------------------------------------
# 'pull' and the clients of its listener
# ---------------------------------------------------------------------------

def write_config(label: str, proxy_port: "int | None",
                 read_timeout: int = READ_TIMEOUT) -> Path:
    """The configuration of one binlog_server run, with a log of its own
    (binlog_server starts its log file anew every time)."""
    binsrv = harness.BINSRV
    binsrv.checkpoint_size = "1"
    binsrv.idle_time = IDLE_TIME
    binsrv.read_timeout = read_timeout
    binsrv.write_config()
    config = json.loads(binsrv.config_path.read_text())
    config["logger"]["file"] = str(binsrv.workdir / f"{label}.log")
    config["connection"]["connect_timeout"] = 3
    if MODE == "gtid_rewrite":
        config["replication"]["rewrite"] = {"base_file_name": rewrite.BASE,
                                            "file_size": "2K"}
    if proxy_port is not None:
        # through the proxy, without TLS
        config["connection"]["port"] = proxy_port
        config["connection"]["ssl"] = {"mode": "disabled"}
    config["replication_source"] = {
        "port": PULL_PORT, "server_id": 142,
        "server_uuid": "00000000-0000-0000-0000-00000000008e",
        "read_timeout": 60, "write_timeout": 60,
        "require_secure_transport": False,
        "auth_rsa_private_key": str(replication.AUTH_RSA_PRIVATE_KEY),
        "authentication": {"user": replication.PULL_USER,
                           "password": replication.PULL_PASSWORD,
                           "plugin": "caching_sha2_password"}}
    path = binsrv.workdir / f"{label}.json"
    path.write_text(json.dumps(config, indent=2))
    return path


class Pull:
    def __init__(self, label: str, proxy_port: "int | None",
                 read_timeout: int = READ_TIMEOUT):
        binsrv = harness.BINSRV
        self.label = label
        self.config = write_config(label, proxy_port, read_timeout)
        self.log_path = binsrv.workdir / f"{label}.log"
        self.log_path.unlink(missing_ok=True)
        self.stderr_path = binsrv.workdir / f"{label}.stderr"
        with self.stderr_path.open("wb") as stderr:
            self.process = subprocess.Popen(
                [str(binsrv.binary), "pull", str(self.config)],
                stdout=subprocess.DEVNULL, stderr=stderr,
                env=tamper.binsrv_environment())
        self.signalled = False
        if not wait_for(self.listening, 60, 0.1):
            self.stop(None)
            raise harness.Failure(
                f"pull ({label}) did not listen: exit "
                f"{self.process.returncode}, {self.errors()}")

    def listening(self) -> bool:
        if self.process.poll() is not None:
            return True
        try:
            socket.create_connection(("127.0.0.1", PULL_PORT),
                                     timeout=1).close()
            return True
        except OSError:
            return False

    def exit_code(self) -> "int | None":
        return self.process.poll()

    def log(self) -> str:
        try:
            return self.log_path.read_text(errors="replace")
        except OSError:
            return ""

    def errors(self, limit: int = 3) -> list:
        lines = [line.strip().split("] ", 2)[-1][:230]
                 for line in self.log().splitlines()
                 if ("[  error]" in line or "exception caught" in line) and
                 # a client that closes its connection (ticket 42)
                 "system error caught in session" not in line]
        return lines[-limit:]

    def stop(self, proxy, grace: float = 10) -> dict:
        """SIGTERM; 'proxy' (if any) closes the source connection, on which
        'pull' otherwise waits up to connection.read_timeout (ticket 43)."""
        result = {"exit_before_signal": self.process.poll()}
        if result["exit_before_signal"] is None:
            started = time.monotonic()
            self.signalled = True
            self.process.send_signal(signal.SIGTERM)
            if proxy is not None:
                time.sleep(0.3)
                proxy.drop()
            try:
                self.process.wait(timeout=grace)
                result["exit_after_sigterm"] = self.process.returncode
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait()
                result["killed_after_seconds"] = grace
            result["seconds_to_stop"] = round(time.monotonic() - started, 1)
        text = self.stderr_path.read_text(errors="replace")
        result["sanitizer"] = rewrite.sanitizer_markers(text)
        codes = [result.get("exit_before_signal"),
                 result.get("exit_after_sigterm")]
        if result["sanitizer"] or any(
                code is not None and code not in (0, 1) for code in codes):
            report = harness.BINSRV.workdir.parent / \
                f"abnormal-{MODE}-{self.label}.txt"
            report.write_text(text + "\n--- log\n" + self.log()[-8000:])
            result["report_file"] = str(report)
            result["stderr"] = text.strip()[-400:]
        return result


class Client:
    """mysqlbinlog --raw on the listener of 'pull', from 'first_binlog'."""

    def __init__(self, label: str, first_binlog: str, stop_never: bool):
        workdir = harness.BINSRV.workdir
        self.directory = workdir / f"dump-{label}"
        shutil.rmtree(self.directory, ignore_errors=True)
        self.directory.mkdir(parents=True)
        self.stderr_path = workdir / f"dump-{label}.stderr"
        with self.stderr_path.open("wb") as stderr:
            self.process = subprocess.Popen(
                [str(ARGS.mysql_basedir / "bin" / "mysqlbinlog"),
                 "--no-defaults", "--read-from-remote-server",
                 "--host=127.0.0.1", f"--port={PULL_PORT}",
                 f"--user={replication.PULL_USER}",
                 f"--password={replication.PULL_PASSWORD}",
                 "--default-auth=caching_sha2_password",
                 "--get-server-public-key", "--ssl-mode=DISABLED", "--raw",
                 "--stop-never" if stop_never else "--to-last-log",
                 "--connection-server-id=77" if stop_never else
                 "--connection-server-id=78",
                 f"--result-file={self.directory}/", first_binlog],
                stdout=subprocess.DEVNULL, stderr=stderr)

    def files(self) -> dict:
        result = {}
        for path in sorted(self.directory.rglob("*")):
            if path.is_file():
                result[str(path.relative_to(self.directory))] = \
                    path.read_bytes()
        return result

    def total(self) -> int:
        return sum(len(data) for data in self.files().values())

    def state(self) -> dict:
        code = self.process.poll()
        error = "; ".join(
            line for line in self.stderr_path.read_text(
                errors="replace").splitlines()
            if "[Warning]" not in line)[-240:]
        return {"connected": code is None, "exit": code, "error": error}

    def finish(self, seconds: float) -> dict:
        try:
            self.process.wait(timeout=seconds)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.wait()
            state = self.state()
            state["exit"] = f"no end in {seconds} s"
            return state
        return self.state()

    def stop(self):
        if self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait()


def ensure_replica():
    """A replica server that died of what it was sent is set up again."""
    if REPLICA.process is not None and REPLICA.process.poll() is None:
        return
    REPLICA.process = None
    shutil.rmtree(REPLICA.datadir, ignore_errors=True)
    REPLICA.error_log.unlink(missing_ok=True)
    REPLICA.start()


def replica_died() -> str:
    """'' while the replica server runs, else how it ended (its error log is
    kept in the work directory)."""
    if REPLICA.process is None:
        return "the replica server is not running"
    code = REPLICA.process.poll()
    if code is None:
        return ""
    log = REPLICA.error_log.read_text(errors="replace").splitlines() \
        if REPLICA.error_log.exists() else []
    kept = harness.BINSRV.workdir.parent / f"replica-died-{MODE}-{CASE}.err"
    kept.write_text("\n".join(log[-200:]))
    marks = ("got signal", "Assertion", "terminate called", "what():",
             "MY-010443", "MY-010590", "MY-013183")
    return (f"mysqld exited with {code}" +
            (f" ({signal.Signals(-code).name})" if code < 0 else "") + ": " +
            " | ".join(line[:200] for line in log
                       if any(mark in line for mark in marks))[-600:] +
            f" (log: {kept})")


def executed(server) -> dict:
    return rewrite.parse_gtid_set(
        server.sql("SELECT @@GLOBAL.gtid_executed"))


def replica_view() -> dict:
    died = replica_died()
    if died:
        return {"rows": "the replica server died", "died": died,
                "receiver": [], "applier": [], "errors": []}
    view = {"rows": rewrite.rows_digest(REPLICA)}
    try:
        receiver = REPLICA.sql(
            "SELECT SERVICE_STATE, LAST_ERROR_NUMBER, "
            "LEFT(LAST_ERROR_MESSAGE, 200) FROM "
            "performance_schema.replication_connection_status"
        ).strip().split("\t")
        applier = REPLICA.sql(
            "SELECT SERVICE_STATE, LAST_ERROR_NUMBER, "
            "LEFT(LAST_ERROR_MESSAGE, 200) FROM "
            "performance_schema.replication_applier_status_by_worker "
            "ORDER BY LAST_ERROR_NUMBER DESC LIMIT 1").strip().split("\t")
        if MODE != "position":
            view["executed"] = rewrite.format_gtid_set(executed(REPLICA))
    except RuntimeError as error:
        receiver = applier = ["?", "-1", str(error).splitlines()[0][:160]]
    view["receiver"] = receiver
    view["applier"] = applier
    view["errors"] = []
    if len(receiver) > 1 and receiver[1] not in CONNECTION_ERRORS:
        view["errors"].append("receiver: " + " ".join(receiver)[:240])
    if len(applier) > 1 and applier[1] not in ("0", ""):
        view["errors"].append("applier: " + " ".join(applier)[:240])
    return view


def replica_matches() -> bool:
    if replica_died():
        return False
    if rewrite.rows_digest(REPLICA) != rewrite.rows_digest(harness.SERVER):
        return False
    try:
        return MODE == "position" or \
            executed(REPLICA) == executed(harness.SERVER)
    except RuntimeError:
        # the replica server has just gone
        return False


def replica_sql(statements: str):
    try:
        REPLICA.sql(statements)
    except RuntimeError:
        if not replica_died():
            raise


# ---------------------------------------------------------------------------
# the storage and the dumps against the source
# ---------------------------------------------------------------------------

def snapshot() -> dict:
    for _ in range(5):
        try:
            return tamper.snapshot(harness.BINSRV)
        except OSError:
            # an object was replaced while the directory was read
            time.sleep(0.05)
    return tamper.snapshot(harness.BINSRV)


def data_files(files: dict) -> dict:
    pattern = rewrite.DATA_NAME if MODE == "gtid_rewrite" else DATA_NAME
    return {name: data for name, data in files.items()
            if pattern.fullmatch(name)}


def strangers(files: dict) -> list:
    pattern = rewrite.STORAGE_NAME if MODE == "gtid_rewrite" else \
        STORAGE_NAME
    return sorted(repr(name)[1:-1][:60] for name in files
                  if not pattern.fullmatch(name))


def source_binlogs() -> dict:
    return {name: harness.SERVER.binlog_as_sent(name)
            for name in harness.SERVER.binlog_names()}


def source_gtids() -> list:
    return [f"{sid}:{number}"
            for sid, numbers in sorted(executed(harness.SERVER).items())
            for number in sorted(numbers)]


def common_prefix(data: bytes, original: bytes) -> int:
    shorter = min(len(data), len(original))
    return next((index for index in range(shorter)
                 if data[index] != original[index]), shorter)


def missing_copy(files: dict, source: dict) -> list:
    """The source's binlogs with a transaction the copies do not hold (a
    transaction that starts after the bytes a copy shares with the
    source's binlog)."""
    result = []
    for name, original in source.items():
        shared = common_prefix(files.get(name, b""), original)
        if any(offset >= shared and
               code in harness.TRANSACTION_START_EVENTS
               for offset, code, _ in harness.parse_events(original)):
            result.append(name)
    return result


def judge_copy(files: dict, source: dict, boundaries: bool) -> list:
    """Without rewrite: every binlog in 'files' must be a prefix of the
    source's binlog of that name."""
    problems = []
    for name, data in sorted(files.items()):
        shown = repr(name)[1:-1][:40]
        if name not in source:
            problems.append(f"'{shown}' ({len(data)} bytes) is not a "
                            "binlog of the source")
            continue
        original = source[name]
        if not original.startswith(data):
            at = common_prefix(data, original)
            event = ""
            try:
                for offset, code, size in harness.parse_events(data):
                    if offset <= at < offset + size:
                        event = (f", in an event of type {code} at "
                                 f"{offset}")
            except harness.Failure:
                event = ", no binlog magic"
            problems.append(
                f"{shown} ({len(data)} bytes) is not a prefix of "
                f"the source's ({len(original)} bytes): first difference "
                f"at byte {at}{event}")
        elif boundaries and data and \
                len(data) not in harness.transaction_boundaries(original):
            problems.append(f"{shown} ends at {len(data)}, inside a "
                            "transaction of the source's binlog")
    return problems


def judge_stream(files: dict) -> tuple:
    """With rewrite: (problems, the source's GTIDs that are not there)."""
    files = {name: data for name, data in files.items() if len(data) > 4}
    examined = rewrite.examine_stream(files)
    problems = list(examined["problems"])
    expected = source_gtids()
    stored = examined["gtids"]
    twice = sorted({gtid for gtid in stored if stored.count(gtid) > 1})
    unknown = sorted(set(stored) - set(expected))
    in_order = [gtid for gtid in expected if gtid in set(stored)]
    once = [gtid for index, gtid in enumerate(stored)
            if gtid not in stored[:index] and gtid in set(expected)]
    if twice:
        problems.append("held twice: " +
                        rewrite.short(", ".join(twice), 120))
    if unknown:
        problems.append("GTIDs the source never had: " +
                        rewrite.short(", ".join(unknown), 120))
    if once != in_order:
        problems.append("the source's transactions are out of order: " +
                        rewrite.short(", ".join(
                            gtid.rsplit(":", 1)[1] for gtid in once), 120))
    missing = [gtid for gtid in expected if gtid not in set(stored)]
    return problems, missing


def without_open_transaction(files: dict) -> dict:
    """The binlogs of a dump that ended with its connection, without the
    transaction (or the event) that was on its way."""
    if not files:
        return files
    last = sorted(files)[-1]
    data = files[last]
    try:
        events = harness.parse_events(data)
    except harness.Failure:
        return files
    starts = [offset for offset, code, _ in events
              if code in harness.TRANSACTION_START_EVENTS]
    if not events or not starts:
        return files
    group = [event for event in events if event[0] > starts[-1]]
    texts = [rewrite.query_text(data, event) for event in group]
    complete = bool(group) and (
        group[-1][1] == XID_EVENT or texts[-1] == b"COMMIT" or
        len(group) == 1 and group[0][1] == rewrite.QUERY_EVENT and
        texts[0] != b"BEGIN")
    end = events[-1][0] + events[-1][2] if complete else starts[-1]
    return dict(files, **{last: data[:end]})


def judge(files: dict, boundaries: bool = False) -> tuple:
    """(what is wrong with a set of binlogs, what of the source they
    lack)."""
    if MODE == "gtid_rewrite":
        problems, missing = judge_stream(files)
        return problems, rewrite.short(", ".join(
            gtid.rsplit(":", 1)[1] for gtid in missing), 100)
    source = source_binlogs()
    return (judge_copy(files, source, boundaries),
            ", ".join(missing_copy(files, source)))


def note(found: dict, details: list, where: str):
    """Records where each thing that is wrong was seen."""
    for detail in details:
        found.setdefault(detail, []).append(where)


def crashed(stopped: dict, when: str) -> list:
    """What Pull.stop() returned, as a problem if 'pull' died of a signal
    or printed a sanitizer report."""
    codes = [code for code in (stopped.get("exit_before_signal"),
                               stopped.get("exit_after_sigterm"))
             if code is not None]
    if stopped["sanitizer"] or any(code not in (0, 1) for code in codes):
        return [f"pull crashed {when}: exit {codes}, sanitizer "
                f"{stopped['sanitizer']}, see {stopped.get('report_file')}"]
    return []


def run_list() -> dict:
    config = write_config("list", None)
    try:
        completed = subprocess.run(
            [str(harness.BINSRV.binary), "list", str(config)],
            capture_output=True, timeout=60, check=False,
            env=tamper.binsrv_environment())
    except subprocess.TimeoutExpired:
        return {"exit": "timeout", "output": "", "sanitizer": []}
    output = (completed.stdout + completed.stderr).decode(errors="replace")
    return {"exit": completed.returncode, "output": output.strip()[-200:],
            "sanitizer": rewrite.sanitizer_markers(output)}


def reconnects(proxy, since: int, pull, log_before: int,
               seconds: float) -> dict:
    """How often 'pull' came back to the proxy since connection 'since'."""
    times = proxy.connect_times[since:]
    intervals = [round(later - earlier, 2)
                 for earlier, later in zip(times, times[1:])]
    log = pull.log()
    facts = {
        "connections": len(times), "intervals_seconds": intervals[:12],
        "log_reconnect_lines": log.count("trying to reconnect"),
        "log_bytes_per_minute": round(
            (len(log.encode()) - log_before) * 60 / max(seconds, 1)),
    }
    if len(times) > 1:
        facts["reconnects_per_minute"] = round(
            (len(times) - 1) * 60 / max(times[-1] - times[0], 0.01))
    if len(intervals) >= 3:
        facts["backoff"] = intervals[-1] > 1.5 * intervals[0]
    return facts


# ---------------------------------------------------------------------------
# a case
# ---------------------------------------------------------------------------

def fetch_known(name: str) -> str:
    kind = kind_of(name)
    ticket = FETCH_KNOWN[MODE].get(name)
    if kind in ("live", "control") or \
            kind == "transaction" and MODE != "gtid_rewrite":
        return "not a fetch case"
    if name == "reannounce_midstream" and MODE == "position":
        return "pass (fetch refuses the stream)"
    return f"fail (ticket {ticket})" if ticket else "pass"


def workload_during():
    rewrite.insert_many("DURING", 2, 8)
    harness.SERVER.sql(
        "UPDATE qa.t SET payload = CONCAT('THIRD-', id, REPEAT('x', 60)) "
        "WHERE id <= 3; UPDATE qa.t SET payload = CONCAT('FOURTH-', id, "
        "REPEAT('x', 60)) WHERE id <= 3")
    rewrite.insert_many("DURING", 1, 8)


def start_attack(name: str, proxy, result: dict):
    kind = kind_of(name)
    binsrv = harness.BINSRV
    source = harness.SERVER
    connections = len(proxy.connect_times)
    if kind in ("name", "resource"):
        upcoming = int(source.binlog_names()[-1].rsplit(".", 1)[1]) + 1
        proxy.old_name = proxy.new_name = b"binlog.%06d" % upcoming
        if kind == "name":
            proxy.new_name = (
                rewrite.storage_file_names(len(binsrv.indexed_binlogs()),
                                           upcoming)[name]
                if MODE == "gtid_rewrite" else hostile.CASES[name](upcoming))
            result["hostile_name"] = repr(proxy.new_name)[:80]
        proxy.arm(drop=False)
        source.sql("FLUSH BINARY LOGS")
        workload_during()
        return
    if kind == "live" and name.endswith("mid_transaction") or \
            name == "stall_mid_event":
        proxy.arm(drop=False)
        workload_during()
        return
    if kind in ("transaction", "control"):
        source.sql("FLUSH BINARY LOGS")
    proxy.arm(drop=True)
    # 'pull' resumes through the attacker
    if not wait_for(lambda: len(proxy.connect_times) > connections and
                    proxy.dumping.is_set(), IDLE_TIME + 10, 0.05):
        result["note_reconnect"] = ("pull did not come back to the proxy "
                                    f"within {IDLE_TIME + 10} s of a closed "
                                    "connection")
    workload_during()


def vs_fetch(known: str, fetch_like: list, pull_only: list) -> str:
    if known == "not a fetch case":
        return "no fetch result to compare with"
    more = ("; only with pull: " + "; ".join(pull_only)[:400]
            if pull_only else "")
    if known.startswith("fail"):
        return ("same" if fetch_like else "better by the checks of the "
                "fetch harness") + more
    if fetch_like:
        return "worse: " + "; ".join(fetch_like)[:300] + more
    return "same by the checks of the fetch harness" + more


def run_case(name: str, proxy_port: int) -> dict:
    binsrv = harness.BINSRV
    source = harness.SERVER
    kind = kind_of(name)
    global CASE  # pylint: disable=global-statement
    CASE = name
    result = {"case": name, "mode": MODE, "fetch_known": fetch_known(name)}
    problems = []
    # what is wrong with the storage or with a dump -> where it was seen
    found = {}
    # when each step ended, in seconds from the start of the case
    case_started = time.monotonic()
    steps = result["steps_done_at"] = {}

    def done(step: str):
        steps[step] = round(time.monotonic() - case_started, 1)
    # what hostile_source_test.py / rewrite_attack_test.py check for 'fetch'
    fetch_like = []
    ensure_replica()
    REPLICA.sql("STOP REPLICA")
    source.reset()
    binsrv.reset()
    if name == "lower_sequence" and MODE != "gtid_rewrite":
        # the storage's first binlog is then not binlog.000001; the purged
        # binlogs hold nothing, so a replica can still start from the rest
        source.sql("DROP TABLE qa.t")
        source.sql("RESET BINARY LOGS AND GTIDS")
        for _ in range(hostile.PURGED_BINLOGS):
            source.sql("FLUSH BINARY LOGS")
        source.sql("PURGE BINARY LOGS TO 'binlog.%06d'" %
                   (hostile.PURGED_BINLOGS + 1))
        source.sql("CREATE TABLE qa.t (id INT UNSIGNED NOT NULL "
                   "AUTO_INCREMENT PRIMARY KEY, payload LONGBLOB) "
                   "ENGINE=InnoDB")
    rewrite.insert_many("BEFORE", 4, 8)
    hostile.cache_password_hash(ARGS.mysql_basedir, source.port)
    stalls = name == "stall_mid_event"
    read_timeout = STALL_READ_TIMEOUT if stalls else READ_TIMEOUT
    proxy = make_proxy(name, proxy_port)
    pull = client = recovered = None
    try:
        # 1. an honest start through the proxy
        pull = Pull("attack", proxy_port, read_timeout)
        if not wait_for(lambda: not judge(data_files(snapshot()))[1],
                        CATCH_UP_SECONDS):
            raise harness.Failure(
                "before the attack: the storage did not get the source's "
                f"transactions (exit {pull.exit_code()}, {pull.errors()})")
        first = binsrv.indexed_binlogs()[0]
        replication.replicate_from_pull(
            REPLICA, PULL_PORT, MODE != "position",
            "SOURCE_AUTO_POSITION=1" if MODE != "position" else
            f"SOURCE_LOG_FILE='{first}', SOURCE_LOG_POS=4")
        client = Client("client", first, True)
        if not wait_for(replica_matches, CATCH_UP_SECONDS):
            raise harness.Failure(
                "before the attack: the replica did not catch up: "
                f"{json.dumps(replica_view())[:500]}; pull exit "
                f"{pull.exit_code()}, {pull.errors()}")
        stored_size = sum(len(data) for data in
                          data_files(snapshot()).values())
        wait_for(lambda: client.total() >= stored_size, 5)
        before = snapshot()
        result["before"] = {
            "replica_rows": rewrite.rows_digest(REPLICA),
            "client_bytes": client.total(), "stored_bytes": stored_size}

        done("caught_up")

        # 2. the attack
        connections = len(proxy.connect_times)
        log_before = len(pull.log().encode())
        started = time.monotonic()
        start_attack(name, proxy, result)
        window = ARGS.observe
        if stalls:
            # two rounds of read_timeout + idle_time
            window = max(window, 2 * (STALL_READ_TIMEOUT + IDLE_TIME) + 4)
        deadline = started + window
        exited_at = None
        while time.monotonic() < deadline:
            if pull.exit_code() is not None:
                exited_at = exited_at or time.monotonic()
                if time.monotonic() - exited_at > 2.5:
                    break
            time.sleep(0.1)
        seconds = time.monotonic() - started
        attack = reconnects(proxy, connections, pull, log_before, seconds)
        attack["proxy_changes"] = proxy.changed()
        attack["pull_exit"] = pull.exit_code()
        attack["pull_seconds_until_exit"] = (
            round(exited_at - started, 1) if exited_at else None)
        attack["pull_errors"] = pull.errors()
        if isinstance(proxy, NameProxy) and name in hostile.STREAM_CASES:
            attack["injected_at"] = proxy.injections + (
                [proxy.injected_at] if proxy.injected_at is not None else [])
            attack["proxy_changes"] = len(attack["injected_at"])
        result["attack"] = attack
        if kind != "control" and attack["proxy_changes"] == 0:
            problems.append("the proxy changed nothing: the case did not "
                            "run")
        intervals = attack["intervals_seconds"]
        if len(intervals) >= 2 and min(intervals) < 0.6 * IDLE_TIME:
            problems.append(
                f"pull reconnects to the hostile source every "
                f"{min(intervals)} s ({attack.get('reconnects_per_minute')}"
                f" per minute), faster than idle_time ({IDLE_TIME} s)")
        if stalls and attack["pull_exit"] is None and \
                attack["connections"] == 0:
            problems.append(
                f"hang: the source stalled in the middle of an event for "
                f"{round(seconds)} s and connection.read_timeout "
                f"({STALL_READ_TIMEOUT} s) did not fire")

        done("attack")

        # 3. what the clients of the listener have
        time.sleep(0.5)
        view = replica_view()
        client_files = client.files()
        client_data = data_files(client_files)
        if MODE == "gtid_rewrite":
            client_data = without_open_transaction(client_data)
        note(found, judge(client_data)[0], "the mysqlbinlog client got it")
        client_strangers = strangers(client_files)
        result["during"] = {
            "replica": view, "source_rows": rewrite.rows_digest(source),
            "client": client.state(),
            "client_files": {repr(name)[1:-1][:40]: len(data)
                             for name, data in client_files.items()},
            "stored": {repr(name)[1:-1][:40]: len(data) for name, data in
                       snapshot().items()
                       if not name.endswith(".json")},
        }
        if client_strangers:
            note(found, ["files that are no binlogs of the source: " +
                         ", ".join(client_strangers[:4])],
                 "the mysqlbinlog client wrote them")
        replica_problems = []
        if MODE != "position" and not replica_died():
            foreign = rewrite.subtract(executed(REPLICA), executed(source))
            if foreign:
                replica_problems.append(
                    "the replica executed GTIDs the source never had: " +
                    rewrite.short(rewrite.format_gtid_set(foreign)))
        if pull.exit_code() is None:
            probe = Client("probe", first, False)
            state = probe.finish(20)
            note(found, judge(data_files(probe.files()))[0],
                 "served to a new client during the attack")
            result["during"]["new_client"] = {
                "exit": state["exit"], "error": state["error"],
                "bytes": probe.total()}
            if state["exit"] != 0:
                note(found, ["a new mysqlbinlog client ended with "
                             f"{state['exit']}: {state['error'][-160:]}"],
                     "while pull was running under the attack")

        # the attack stops while 'pull' is still running
        if kind == "live" and not stalls and pull.exit_code() is None:
            proxy.disarm()
            result["recovers_without_restart"] = wait_for(
                lambda: not judge(data_files(snapshot()))[1] and
                replica_matches(), 10)
        view_after = replica_view()

        done("clients")

        # 4. stop, and the storage the attack leaves
        client.stop()
        if stalls and pull.exit_code() is None:
            # SIGTERM while the source connection is stalled
            limit = STALL_READ_TIMEOUT + IDLE_TIME + 5
            stopped = pull.stop(None, limit)
            if "killed_after_seconds" in stopped:
                problems.append(f"hang: pull did not stop within {limit} s "
                                "of SIGTERM while its source stalled")
        else:
            stopped = pull.stop(proxy)
            if "killed_after_seconds" in stopped:
                problems.append("hang: pull did not stop within 10 s of "
                                "SIGTERM with its source connection closed")
        result["pull_stop"] = stopped
        problems += crashed(stopped, "under the attack")
        if stopped.get("exit_before_signal") == 0:
            problems.append("pull exited with 0 on its own")
        proxy.close()
        after = snapshot()
        storage_problems, _ = judge(data_files(after), boundaries=True)
        odd = strangers(after)
        if odd:
            storage_problems.append("entries that are no binlogs: " +
                                    ", ".join(odd[:4]))
        note(found, storage_problems, "in the storage after the attack")
        lost = sorted(object_name for object_name, content in
                      data_files(before).items()
                      if not after.get(object_name, b"").startswith(content))
        if lost:
            fetch_like.append("binlogs stored before the attack lost "
                              "bytes: " + ", ".join(lost))
        if after.get("metadata.json") != before.get("metadata.json"):
            fetch_like.append("metadata.json was overwritten")
        twice = sorted(
            object_name for object_name, content in data_files(after).items()
            if hostile.format_descriptions(content) > 1)
        if twice:
            fetch_like.append("a second FORMAT_DESCRIPTION inside " +
                              ", ".join(twice))
        if MODE == "gtid_rewrite":
            # rewrite_attack_test.py judges the stored stream as well
            fetch_like += storage_problems
        listed = run_list()
        result["list_exit"] = listed["exit"]
        if listed["sanitizer"] or listed["exit"] not in (0, 1):
            problems.append(f"'list' crashed: exit {listed['exit']}, "
                            f"sanitizer {listed['sanitizer']}: "
                            f"{listed['output']}")
        if listed["exit"] != 0:
            fetch_like.append("the storage no longer opens: 'list' exit "
                              f"{listed['exit']}: {listed['output']}")
        result["storage_entries"] = sorted(
            repr(entry)[1:-1][:50] for entry in after)[-12:]

        done("storage")

        # 5. the attacker is gone
        rewrite.insert_many("AFTER", 2, 8)
        recovery = {}
        try:
            recovered = Pull("recovery", None)
        except harness.Failure as failure:
            recovery["pull"] = harness.describe_failure(failure, 300)

        def complete() -> bool:
            return not judge(data_files(snapshot()))[1]

        if recovered is not None:
            wait_for(lambda: recovered.exit_code() is not None or
                     complete() and replica_matches(), ARGS.recover)
            recovery["pull_exit"] = recovered.exit_code()
            recovery["pull_errors"] = recovered.errors()
            if any(hostile.STORAGE_REFUSED in line
                   for line in recovery["pull_errors"]):
                fetch_like.append("the storage no longer opens for writing "
                                  f"('{hostile.STORAGE_REFUSED}')")
            if recovered.exit_code() is None and complete() and \
                    not replica_matches():
                # its threads may have stopped: start them once more
                recovery["replica_before_restart"] = replica_view()
                replica_sql("STOP REPLICA; START REPLICA")
                recovery["replica_recovers_after_start_replica"] = wait_for(
                    replica_matches, 12)
        done("restart")
        final = snapshot()
        final_problems, missing = judge(data_files(final), boundaries=True)
        note(found, final_problems, "in the storage after the clean restart")
        final_problems = []
        final_view = replica_view()
        recovery["storage_lacks"] = missing
        recovery["replica"] = final_view
        recovery["source_rows"] = rewrite.rows_digest(source)
        if MODE != "position":
            recovery["source_executed"] = rewrite.format_gtid_set(
                executed(source))
        if recovered is not None and recovered.exit_code() is None:
            last = Client("final", first, False)
            state = last.finish(20)
            served_problems, served_missing = judge(data_files(last.files()))
            note(found, served_problems,
                 "served to a new client after the clean restart")
            recovery["new_client"] = {"exit": state["exit"],
                                      "error": state["error"],
                                      "lacks": served_missing}
        if recovered is not None:
            # killed after 2 s: without events from the source it stops
            # only at connection.read_timeout (ticket 43)
            recovery["pull_stop"] = recovered.stop(None, 2)
            problems += crashed(recovery["pull_stop"],
                                "after the clean restart")
        result["recovery"] = recovery
        if missing or recovery.get("pull_exit") is not None or \
                recovered is None:
            final_problems.append(
                "a clean restart of pull without the proxy does not "
                f"recover: exit {recovery.get('pull_exit')}, the storage "
                f"lacks [{missing}], errors "
                f"{recovery.get('pull_errors') or recovery.get('pull')}")
        died = [one["died"] for one in (view, view_after, final_view)
                if one.get("died")]
        if died:
            replica_problems.append("the replica SERVER died of what pull "
                                    "sent: " + died[0])
        errors = sorted(set(view["errors"] + view_after["errors"] +
                            final_view["errors"]))
        if errors:
            replica_problems.append("the replica reported errors: " +
                                    " | ".join(errors)[:500])
        behind = []
        if not died and (
                final_view["rows"] != recovery["source_rows"] or
                MODE != "position" and
                final_view.get("executed") != recovery["source_executed"]):
            behind.append(
                "after the clean restart the replica has "
                f"{final_view['rows']} (GTIDs "
                f"{final_view.get('executed', '-')}), the source "
                f"{recovery['source_rows']} (GTIDs "
                f"{recovery.get('source_executed', '-')})" +
                ("" if errors or missing else
                 ": silently, no replica error and the storage is "
                 "complete"))
        restarted = recovery.get("replica_recovers_after_start_replica")
        if not died and restarted is not None:
            replica_problems.append(
                "the replica needed STOP/START REPLICA after the clean "
                f"restart (caught up then: {restarted})")
        pull_only = ([problem for problem in problems
                      if problem.startswith(("pull crashed", "hang", "'list'",
                                             "pull reconnects"))] +
                     replica_problems +
                     [problem for problem in behind
                      if "silently" in problem] +
                     [f"{detail} [{places[0]}]"
                      for detail, places in found.items()
                      if not any("storage" in place for place in places)])
        located = [f"{detail} [{'; '.join(places)}]"
                   for detail, places in found.items()]
        if len(located) > 8:
            located = located[:8] + [f"and {len(located) - 8} more of the "
                                     "kind"]
        problems += replica_problems + behind + located + final_problems
        problems += [problem for problem in fetch_like
                     if problem not in found]
        result["fetch_like_problems"] = fetch_like
        result["vs_fetch"] = vs_fetch(result["fetch_known"], fetch_like,
                                      pull_only)
    finally:
        if client is not None:
            client.stop()
        proxy.close()
        for process in (pull, recovered):
            if process is not None and process.exit_code() is None:
                # connection.read_timeout later at the latest (ticket 43)
                process.process.send_signal(signal.SIGTERM)
                try:
                    process.process.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    process.process.kill()
                    process.process.wait()
        try:
            REPLICA.sql("STOP REPLICA")
        except RuntimeError:
            # the replica server died; the next case sets it up again
            pass
    # the same problem is not reported twice
    problems = list(dict.fromkeys(problems))
    result["ok"] = not problems
    if problems:
        result["why"] = "; ".join(problems)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--mysql-basedir", type=Path, required=True)
    parser.add_argument("--binsrv", type=Path, required=True)
    parser.add_argument("--workdir", type=Path, required=True)
    parser.add_argument("--port", type=int, default=24300,
                        help="the source's port; the replica and 'pull' use "
                             "the two above it, the proxy the 20 after "
                             "those")
    parser.add_argument("--mode", choices=["position", "gtid"],
                        default="position")
    parser.add_argument("--rewrite", action="store_true",
                        help="with 'replication.rewrite' (needs --mode "
                             "gtid)")
    parser.add_argument("--cases", nargs="*",
                        help="the cases to run (default: a selection that "
                             "takes under ten minutes; 'all' runs every "
                             "case of the mode)")
    parser.add_argument("--observe", type=float, default=6,
                        help="seconds the attack is watched")
    parser.add_argument("--recover", type=float, default=15,
                        help="seconds the clean restart gets to catch up")
    args = parser.parse_args()
    if args.rewrite and args.mode != "gtid":
        parser.error("--rewrite needs --mode gtid")
    global ARGS, MODE, REPLICA, PULL_PORT  # pylint: disable=global-statement
    ARGS = args
    MODE = "gtid_rewrite" if args.rewrite else args.mode
    args.mysql_basedir = args.mysql_basedir.expanduser()
    args.workdir = args.workdir.expanduser()
    args.workdir.mkdir(parents=True, exist_ok=True)
    cases = args.cases or DEFAULT_CASES[MODE]
    if cases == ["all"]:
        cases = all_cases()
    unknown = [name for name in cases if name not in all_cases()]
    if unknown or len(set(cases)) != len(cases):
        parser.error(f"unknown or repeated cases {unknown}; the cases of "
                     f"this mode: {' '.join(all_cases())}")
    gtid_mode = args.mode == "gtid"
    PULL_PORT = args.port + 2
    harness.SERVER = harness.MySQLServer(args.mysql_basedir,
                                         args.workdir / "mysql", args.port,
                                         gtid_mode, server_id=1)
    REPLICA = harness.MySQLServer(args.mysql_basedir,
                                  args.workdir / "replica", args.port + 1,
                                  gtid_mode, server_id=2)
    harness.BINSRV = harness.BinlogServer(args.binsrv.expanduser(),
                                          args.workdir / "binsrv", args.port,
                                          gtid_mode)
    results = []
    started = time.monotonic()
    # a replica that died of what it was sent may die again when it starts
    shutil.rmtree(REPLICA.datadir, ignore_errors=True)
    harness.SERVER.start()
    try:
        REPLICA.start()
        for index, name in enumerate(cases):
            case_started = time.monotonic()
            try:
                result = run_case(name, args.port + 3 + index % 20)
            except (harness.Failure, RuntimeError,
                    subprocess.SubprocessError, OSError) as failure:
                result = {"case": name, "mode": MODE, "ok": False,
                          "why": harness.describe_failure(failure, 700)}
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
