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

"""A running 'pull' attacked from both sides at once.

'binlog_server pull' is two programs in one process: a collector thread
that reads binlog events from the source and writes the storage, and a
listener (an io_context with the sessions, the heartbeat and telemetry
timers and the signal handling) that serves the same storage to replicas.
The other harnesses attack one side with the other one quiet
(hostile_source_pull_test.py: the source; listener_fuzz_test.py,
listener_command_fuzz_test.py, listener_replica_dos_test.py: the clients).
Here both happen together, for --duration seconds per case, so that the
collector writes (checkpoint_size is small: the storage is flushed and its
metadata saved all the time; in rewrite mode a new binlog every 16K) while
the listener reads and the telemetry timer fires (its intervals are at
their minimums, 20 s and 10 s); it is meant for a ThreadSanitizer build of
binlog_server above all (it is then started through 'setarch -R'), and
runs on any build.

The source side (the proxies and attack definitions are imported from
hostile_source_test.py, hostile_source_pull_test.py and
rewrite_attack_test.py; one proxy here switches between them while 'pull'
is connected). Only attacks that keep 'pull' alive and writing:

  drop                    the source connection is closed
  drop_mid_transaction    ... after GTID + BEGIN
  stall_mid_event         half an event, then silence (read_timeout, 4 s)
  eof_in_preamble         an EOF packet right after the artificial ROTATE
  err_mid_transaction     an ERR packet (1236) after GTID + BEGIN: 'pull'
                          ends with 1, as 'fetch' does, while its clients
                          are served, and is started again
  reannounce_midstream    (rewrite mode, where the preamble is ignored) the
                          open binlog announced again at position 4
  and all the time a workload on the source with FLUSH BINARY LOGS in a
  loop and now and then a transaction larger than a rewritten binlog.

The case 'hostile' (--mode gtid only) adds what rewrite mode is known to
store (ticket 81, PBS-147): gtid_twice, gtid_swapped, replay_after_reconnect. Its
storage and its readers are then not compared with the source; liveness,
shutdown and sanitizer reports are checked as in the other cases.

The client side, --clients threads at once (the raw client and the command
encoders are imported from listener_fuzz_test.py and
listener_command_fuzz_test.py), each choosing over and over, with and
without TLS:

  fuzz            a login and one to five replica commands, most of them
                  mutated, in one piece or in pieces
  odd_dump        a dump of a stored binlog (mostly the one being written)
                  from a position that is usually inside an event, read for
                  a moment, cut in the middle of a frame with a reset, or
                  not read at all for a few seconds
  heartbeat_dump  a blocking dump at the end of the storage with a
                  heartbeat period of 0.1 to 1 s
  gtid_dump       COM_BINLOG_DUMP_GTID with the source's GTIDs up to a
                  random number, all of them, future ones, another UUID, a
                  random set
  wrong_login     a wrong password, an unknown user, the garbage logins of
                  listener_fuzz_test.py
  idle            a connection that sends nothing

Next to them: a MySQL replica and a 'mysqlbinlog --raw --stop-never' client
(the honest readers), a login every few seconds that must succeed, and
'list' / 'search_by_timestamp' (/ 'search_by_gtid_set') in a loop against
the same storage. At a random moment after --duration, with everything
still running, 'pull' gets SIGTERM.

A case FAILS on
  * a sanitizer report of 'pull' or of a query (ThreadSanitizer reports are
    collected with log_path in <workdir>/san/<mode>-<case>/ and grouped by
    their SUMMARY line; an AddressSanitizer report ends the process);
  * 'pull' ending by a signal, or on its own with 0; a login that takes
    longer than 10 s (times --timeout-scale); memory that keeps growing;
  * a shutdown that takes longer than read_timeout + idle_time + 10 s
    (scaled) or does not exit with 0;
  * storage that is not a prefix of the source's binlogs (rewrite mode: the
    source's GTIDs once and in order) after the SIGTERM;
  * after a clean restart without the proxy: a storage, a replica or a new
    mysqlbinlog client without the source's data, a replica error;
  * a query that crashes, or fails for another reason than ticket 29 (PBS-101).

Not in the mix, as the single-sided harnesses already report them: the
hostile binlog names (tickets 59, 69, 70), a lowered transaction_length
(82), EOF after GTID + BEGIN (83 in rewrite mode; without rewrite 'pull'
keeps the open transaction, resumes inside it and ends with "transaction
group must start from one of the gtid log events"), statements with long
runs of white space or digits (72), a 16 MB statement (74), a heartbeat
period below 50 ms (75). Dumps from a position inside an event are sent,
but what they return is not judged (ticket 39, PBS-112); 'eof' and client resets
logged as errors (42) are not counted; ticket 43 (PBS-116) is why read_timeout is
4 s here.

Cases (--cases): plain, encrypted (the storage encrypted with a keyring
key), hostile (--mode gtid). --mode position (default) or gtid; GTID mode
runs with 'replication.rewrite' (mandatory since PBS-80 / PBS-98;
--rewrite is accepted, --no-rewrite is for binaries older than that). A
JSON line per case, then a summary; result.json in the work directory;
exit 1 if any case failed. What a known ticket explains is listed under
"known" and does not fail a case. Example:

  PYTHONPATH=~/ws/qa/pylib python3 pull_two_sided_attack_test.py \\
    --mysql-basedir ~/ws/dist/mysql-8.4.8-linux-glibc2.28-x86_64-minimal \\
    --binsrv ~/ws/percona-binlog-server-build-tsan_gcc14/binlog_server \\
    --workdir ~/ws/qa/two-sided --mode gtid --seed 7
"""

import argparse
import collections
import json
import os
import platform
import random
import re
import shutil
import signal
import socket
import ssl
import struct
import subprocess
import sys
import threading
import time
import uuid as uuid_module
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import fetch_crash_test as harness  # noqa: E402
import hostile_source_pull_test as hsp  # noqa: E402
import hostile_source_test as hostile  # noqa: E402
import listener_command_fuzz_test as fuzz  # noqa: E402
import listener_fuzz_test as listener  # noqa: E402
import metadata_tamper_test as tamper  # noqa: E402
import pull_replica_test as replication  # noqa: E402
import rewrite_attack_test as rewrite  # noqa: E402

IDLE_TIME = 1
# ticket 43 (PBS-116): an idle source delays the stop by up to this
READ_TIMEOUT = 4
REWRITE_FILE_SIZE = "16K"
CATCH_UP_SECONDS = 60
LOGIN_SECONDS = 10
# the shortest heartbeat period a client here asks for (ticket 75, PBS-155)
MIN_HEARTBEAT_NS = 50_000_000
HEARTBEAT_PERIOD = re.compile(rb"heartbeat_period\s*=\s*([0-9]+)")
TSAN_DEFAULTS = "halt_on_error=0 second_deadlock_stack=1 history_size=4"
TSAN_SUPPRESSIONS = Path(__file__).resolve().parent / "nightly" / "tsan.supp"
# 'list' next to a writer (query_race_test.py)
TICKET_29 = re.compile(r"\.tmp|No such file or directory|cannot (get|open)|"
                       r"file_size|filesystem error", re.IGNORECASE)

LIVE = ["drop_mid_transaction", "stall_mid_event", "eof_in_preamble",
        "err_mid_transaction"]
# 'pull' ends with 1 on an ERR packet from the source ("cannot fetch binlog
# event: <the message of the packet>"); it is started again
ERR_TEXT = hsp.ERR_PACKET[9:].decode()
SOURCE_MIX = {
    "position": ["drop"] + LIVE,
    "gtid": ["drop"] + LIVE,
    "gtid_rewrite": ["drop"] + LIVE + ["reannounce_midstream"],
}
# ticket 81 (PBS-147): stored and served in rewrite mode
HOSTILE_MIX = ["gtid_twice", "gtid_swapped", "replay_after_reconnect"]
TRANSACTION_FAMILY = ("gtid_twice", "gtid_swapped")
STREAM_FAMILY = ("reannounce_midstream",)
# attacks that act at the start of a connection
NEEDS_DROP = ("err_in_preamble", "eof_in_preamble", "replay_after_reconnect",
              "drop") + TRANSACTION_FAMILY + STREAM_FAMILY
CASES = ["plain", "encrypted", "hostile"]
CLIENT_SCENARIOS = [("fuzz", 5), ("odd_dump", 5), ("heartbeat_dump", 2),
                    ("gtid_dump", 3), ("wrong_login", 2), ("idle", 1)]

# set by main()
ARGS = None
MODE = "position"
SCALE = 1.0
BUILD = "plain"
RUN_PREFIX: list = []
REPLICA: harness.MySQLServer = None
PULL_PORT = 0
SAN_DIR: Path = None
SOURCE_UUID = ""


def wait_for(condition, seconds: float, step: float = 0.2) -> bool:
    return hsp.wait_for(condition, seconds, step)


def build_kind(binary: Path) -> str:
    data = binary.read_bytes()
    if b"__tsan_init" in data:
        return "tsan"
    if b"__asan_init" in data:
        return "asan"
    return "plain"


def environment() -> dict:
    """ThreadSanitizer writes its reports to files in SAN_DIR (one per
    process) and goes on; AddressSanitizer ends the process."""
    result = tamper.binsrv_environment()
    options = [option for option in
               os.environ.get("TSAN_OPTIONS", TSAN_DEFAULTS).split()
               if not option.startswith("log_path=")]
    if not any(option.startswith("suppressions=") for option in options) \
            and ARGS.tsan_suppressions.exists():
        options.append(f"suppressions={ARGS.tsan_suppressions}")
    options.append(f"log_path={SAN_DIR}/tsan")
    result["TSAN_OPTIONS"] = " ".join(options)
    if "quarantine_size_mb" not in result["ASAN_OPTIONS"]:
        # by default freed memory stays resident up to 256 MB, which looks
        # like a leak in the memory check
        result["ASAN_OPTIONS"] += ":quarantine_size_mb=16"
    return result


# ---------------------------------------------------------------------------
# the source side
# ---------------------------------------------------------------------------

class MixProxy(hsp.LongRunning, rewrite.AttackProxy):
    """One man in the middle for a 'pull' that stays connected: the attack
    in force is chosen with start() and ended with stop(). The packet loops
    are those of hostile_source_pull_test.LiveProxy,
    rewrite_attack_test.AttackProxy (whole transactions) and
    hostile_source_test.RenamingProxy (reannounce_midstream); which one a
    connection gets is decided when it starts."""

    def __init__(self, listen_port: int, source_port: int, source_uuid: str):
        self.prepare()
        self.acted = []
        self.last_transaction = []
        self.injections = []
        self.family = "live"
        rewrite.AttackProxy.__init__(self, listen_port, source_port, "",
                                     source_uuid, 2)

    def new_connection(self):
        hsp.LongRunning.new_connection(self)
        if self.injected_at is not None:
            self.injections.append(self.injected_at)
        # hostile_source_test.RenamingProxy
        self.in_next_binlog = False
        self.shift = 0
        self.preamble = {}
        self.transactions = 0
        self.last_log_pos = 0
        self.injected_at = None
        self.rewind = 0
        # rewrite_attack_test.AttackProxy
        self.sequence = 0
        self.pending = []
        self.seen = 0
        self.held = None
        self.continuation = False

    def start(self, attack: str):
        self.attack = attack
        self.family = ("transaction" if attack in TRANSACTION_FAMILY else
                       "stream" if attack in STREAM_FAMILY else "live")
        self.target = 3 if attack == "gtid_swapped" else 2
        if attack == "drop":
            self.drop()
            self.acted.append(time.monotonic())
            return
        self.arm(drop=attack in NEEDS_DROP)

    def stop(self):
        if self.armed.is_set():
            self.disarm()
        self.family = "live"

    def actions(self) -> int:
        return (len(self.acted) + self.changed() + len(self.injections) +
                (self.injected_at is not None))

    def _server_to_client(self, server, client):
        family = self.family
        if family == "transaction":
            rewrite.AttackProxy._server_to_client(self, server, client)
        elif family == "stream":
            hostile.RenamingProxy._server_to_client(self, server, client)
        else:
            hsp.LiveProxy._server_to_client(self, server, client)

    def _process(self, payload: bytes, length: int) -> list:
        if not self.armed.is_set():
            return [payload]
        return super()._process(payload, length)

    def _midstream(self, payload: bytes) -> list:
        if not self.armed.is_set():
            return [payload]
        packets = super()._midstream(payload)
        if self.injected_at is not None and len(payload) > 5 and \
                payload[0] == 0 and payload[5] == hostile.ROTATE_EVENT:
            # the source goes on to its next binlog, whose positions start
            # anew: that one is announced again as well
            self.injections.append(self.injected_at)
            self.injected_at = None
            self.rewind = 0
            self.last_log_pos = 0
            self.transactions = 0
        return packets

    def _rewrite(self, payload: bytes) -> bytes:
        return payload


class SourceSide(threading.Thread):
    """The workload on the source and the attacks on its connection."""

    def __init__(self, proxy: MixProxy, mix: list, seed: int):
        super().__init__(daemon=True)
        self.proxy = proxy
        self.mix = mix
        self.rng = random.Random(seed)
        self.halt = threading.Event()
        self.attacks = collections.Counter()
        self.acted = collections.Counter()
        self.current = ""
        self.last = ("", 0.0)
        self.transactions = 0
        self.rotations = 0
        self.errors = []
        self.loader = threading.Thread(target=self._load, daemon=True)

    def _load(self):
        rng = random.Random(self.rng.random())
        count = 0
        while not self.halt.is_set():
            count += 1
            try:
                if count % 97 == 0:
                    # larger than a rewritten binlog
                    rewrite.insert_many("BIG", 1, 260)
                    self.transactions += 1
                elif count % 23 == 0:
                    harness.SERVER.sql("FLUSH BINARY LOGS")
                    self.rotations += 1
                else:
                    transactions = rng.choice([1, 1, 2, 3])
                    rewrite.insert_many(f"W{count}", transactions, 6)
                    self.transactions += transactions
            except RuntimeError as error:
                self.errors.append(str(error)[:160])
                if len(self.errors) > 20:
                    return
            self.halt.wait(ARGS.load_pause)

    def run(self):
        self.loader.start()
        while not self.halt.wait(self.rng.uniform(1.0, 3.0)):
            name = self.rng.choice(self.mix)
            before = self.proxy.actions()
            self.current = name
            self.proxy.start(name)
            hold = (READ_TIMEOUT + IDLE_TIME + 2 if name == "stall_mid_event"
                    else self.rng.uniform(2.0, 5.0))
            self.halt.wait(hold)
            self.proxy.stop()
            self.last = (name, time.monotonic())
            self.current = ""
            self.attacks[name] += 1
            self.acted[name] += self.proxy.actions() - before

    def finish(self):
        self.halt.set()
        self.join(30)
        self.loader.join(60)
        self.proxy.stop()


# ---------------------------------------------------------------------------
# 'pull'
# ---------------------------------------------------------------------------

def write_config(label: str, proxy_port: "int | None",
                 encrypted: bool) -> Path:
    """The configuration of one binlog_server run, with a log of its own
    (binlog_server starts its log file anew every time)."""
    binsrv = harness.BINSRV
    binsrv.checkpoint_size = ARGS.checkpoint_size
    binsrv.idle_time = IDLE_TIME
    binsrv.read_timeout = READ_TIMEOUT
    binsrv.write_config()
    config = json.loads(binsrv.config_path.read_text())
    config["logger"]["file"] = str(binsrv.workdir / f"{label}.log")
    config["connection"]["connect_timeout"] = 5
    if MODE == "gtid_rewrite":
        config["replication"]["rewrite"] = {"base_file_name": rewrite.BASE,
                                            "file_size": REWRITE_FILE_SIZE}
    else:
        config["replication"].pop("rewrite", None)
    # the telemetry timer fires during the attack (the minimum intervals)
    config["telemetry"].update({"grace_interval": "20s",
                                "scrape_interval": "10s",
                                "history_keep_interval": "1h"})
    if proxy_port is not None:
        # through the proxy, without TLS
        config["connection"]["port"] = proxy_port
        config["connection"]["ssl"] = {"mode": "disabled"}
    if encrypted:
        keyring = binsrv.workdir / "keyring.json"
        keyring.write_text(json.dumps(tamper.KEYRING))
        config["keyring"] = {"uri": f"file://{keyring}"}
        config["storage"]["encryption"] = dict(tamper.ENCRYPTION)
    config["replication_source"] = {
        "port": PULL_PORT, "server_id": 142,
        "server_uuid": "00000000-0000-0000-0000-00000000008e",
        "read_timeout": 20, "write_timeout": 20,
        "require_secure_transport": False,
        "auth_rsa_private_key": str(replication.AUTH_RSA_PRIVATE_KEY),
        # TLS and plain clients at once
        "ssl": listener.SSL,
        "authentication": {"user": replication.PULL_USER,
                           "password": replication.PULL_PASSWORD,
                           "plugin": "caching_sha2_password"}}
    path = binsrv.workdir / f"{label}.json"
    path.write_text(json.dumps(config, indent=2))
    return path


class Pull:
    def __init__(self, label: str, proxy_port: "int | None", encrypted: bool):
        binsrv = harness.BINSRV
        self.label = label
        self.config = write_config(label, proxy_port, encrypted)
        self.log_path = binsrv.workdir / f"{label}.log"
        self.log_path.unlink(missing_ok=True)
        self.stderr_path = binsrv.workdir / f"{label}.stderr"
        with self.stderr_path.open("wb") as stderr:
            self.process = subprocess.Popen(
                RUN_PREFIX + [str(binsrv.binary), "pull", str(self.config)],
                stdout=subprocess.DEVNULL, stderr=stderr, env=environment())
        self.peak_rss = 0
        if not wait_for(self.listening, 60 * SCALE, 0.1) or \
                self.exit_code() is not None:
            stopped = self.stop(2)
            raise harness.Failure(
                f"pull ({label}) did not listen: {stopped}, "
                f"{self.errors()}")

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

    def rss_mb(self) -> int:
        try:
            status = Path(f"/proc/{self.process.pid}/status").read_text()
        except OSError:
            return 0
        found = re.search(r"^VmRSS:\s+(\d+) kB", status, re.MULTILINE)
        value = int(found.group(1)) // 1024 if found else 0
        self.peak_rss = max(self.peak_rss, value)
        return value

    def log(self) -> str:
        try:
            return self.log_path.read_text(errors="replace")
        except OSError:
            return ""

    def errors(self, limit: int = 3) -> list:
        lines = [line.strip().split("] ", 2)[-1][:230]
                 for line in self.log().splitlines()
                 if ("[  error]" in line or "exception caught" in line) and
                 # a client that closes its connection (ticket 42, PBS-115)
                 "caught in session" not in line]
        return lines[-limit:]

    def stderr(self) -> str:
        return self.stderr_path.read_text(errors="replace")

    def stop(self, grace: float) -> dict:
        result = {"exit_before_signal": self.process.poll()}
        if result["exit_before_signal"] is None:
            started = time.monotonic()
            self.process.send_signal(signal.SIGTERM)
            try:
                self.process.wait(timeout=grace)
                result["exit_after_sigterm"] = self.process.returncode
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait()
                result["killed_after_seconds"] = round(grace, 1)
            result["seconds_to_stop"] = round(time.monotonic() - started, 1)
        text = self.stderr()
        result["sanitizer"] = rewrite.sanitizer_markers(text)
        codes = [result.get("exit_before_signal"),
                 result.get("exit_after_sigterm")]
        if result["sanitizer"] or any(
                code is not None and code not in (0, 1) for code in codes):
            report = SAN_DIR / f"abnormal-{self.label}.txt"
            report.write_text(text + "\n--- log\n" + self.log()[-8000:])
            result["report_file"] = str(report)
            result["stderr"] = text.strip()[-400:]
        return result


def crashed(stopped: dict, when: str) -> list:
    codes = [code for code in (stopped.get("exit_before_signal"),
                               stopped.get("exit_after_sigterm"))
             if code is not None]
    if stopped["sanitizer"] or any(code not in (0, 1) for code in codes):
        return [f"pull crashed {when}: exit {codes}, "
                f"{stopped['sanitizer']}, see {stopped.get('report_file')}"]
    return []


# ---------------------------------------------------------------------------
# the client side
# ---------------------------------------------------------------------------

def stored_binlogs() -> list:
    """(name, size) of the binlogs in the storage, oldest first."""
    pattern = rewrite.DATA_NAME if MODE == "gtid_rewrite" else hsp.DATA_NAME
    result = []
    try:
        for entry in os.scandir(harness.BINSRV.storage):
            if pattern.fullmatch(entry.name):
                try:
                    result.append((entry.name.encode(), entry.stat().st_size))
                except OSError:
                    pass
    except OSError:
        pass
    return sorted(result)


class Binlogs:
    """What listener_command_fuzz_test.random_command() needs of its
    Context."""

    @property
    def binlogs(self) -> list:
        return stored_binlogs() or [(b"binlog.000001", 4)]


def reset_close(client):
    """Closes with a reset instead of a FIN."""
    try:
        client.sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER,
                               struct.pack("ii", 1, 0))
    except OSError:
        pass
    client.close()


def safe_payload(payload: bytes) -> bytes:
    """Nothing that tickets 72 and 75 would answer for."""
    if fuzz.LONG_RUN.search(payload):
        return bytes([fuzz.COM_PING])
    found = HEARTBEAT_PERIOD.search(payload)
    if found and int(found.group(1)[:30]) < MIN_HEARTBEAT_NS:
        return fuzz.com_query(b"SET @master_heartbeat_period = 500000000, "
                              b"@source_heartbeat_period = 500000000")
    return payload


class Attacker(threading.Thread):
    """One hostile client after another, until halted."""

    def __init__(self, index: int, seed: int, halt: threading.Event,
                 stats: collections.Counter, pull_down: threading.Event):
        super().__init__(daemon=True)
        self.rng = random.Random(seed * 1000 + index)
        self.halt = halt
        self.stats = stats
        self.pull_down = pull_down
        self.binlogs = Binlogs()
        self.uuid = uuid_module.UUID(SOURCE_UUID).bytes

    def run(self):
        names = [name for name, _ in CLIENT_SCENARIOS]
        weights = [weight for _, weight in CLIENT_SCENARIOS]
        while not self.halt.is_set():
            if self.pull_down.is_set():
                self.halt.wait(0.2)
                continue
            name = self.rng.choices(names, weights)[0]
            self.stats[f"scenario {name}"] += 1
            try:
                getattr(self, name)()
            except (OSError, ssl.SSLError, ValueError, struct.error,
                    harness.Failure) as error:
                self.stats[f"client error {type(error).__name__}"] += 1
            self.halt.wait(self.rng.uniform(0.0, 0.05))

    # -- helpers

    def connect(self):
        client = listener.Listener("127.0.0.1", PULL_PORT,
                                   timeout=5.0 * SCALE)
        self.stats["connections"] += 1
        return client

    def login(self, capabilities=None):
        """A logged-in client or None."""
        client = self.connect()
        tls = self.rng.random() < 0.4
        self.stats["tls connections" if tls else "plain connections"] += 1
        if tls:
            logged_in = fuzz.tls_handshake_with(client, capabilities)
        elif capabilities is None:
            logged_in = client.handshake()
        else:
            logged_in = fuzz.handshake_with(client, capabilities)
        if not logged_in:
            self.stats["logins refused or lost"] += 1
            client.close()
            return None
        self.stats["logins"] += 1
        return client

    def send_command(self, client, payload: bytes, label: str):
        self.stats["commands"] += 1
        self.stats[f"command {label}"] += 1
        return fuzz.command(client, payload, timeout=5.0 * SCALE)

    def read(self, client, seconds: float, quiet=None) -> dict:
        answer = fuzz.read_stream(client, seconds, quiet=quiet, answers={})
        self.stats["events received"] += answer["events"]
        self.stats["heartbeats received"] += answer["heartbeats"]
        self.stats["bytes received"] += answer["bytes"]
        return answer

    def source_gtids(self, upper: int) -> bytes:
        return fuzz.gtid_set([(self.uuid, b"", [(1, upper)], None)])

    # -- scenarios

    def fuzz(self):
        rng = self.rng
        client = self.login(rng.choice(
            [None, None, None,
             listener.BASE_CAPABILITIES | fuzz.CLIENT_CONNECT_ATTRS,
             listener.BASE_CAPABILITIES & ~listener.CLIENT_SECURE_CONNECTION]))
        if client is None:
            return
        try:
            data = (listener.frame(0, fuzz.com_query(fuzz.CHECKSUM_ON))
                    if rng.random() < 0.7 else b"")
            for _ in range(rng.randint(1, 5)):
                label, payload = fuzz.random_command(self.binlogs, rng)
                if rng.random() < 0.6:
                    label += ", mutated"
                    payload = fuzz.mutate(rng, payload)
                payload = safe_payload(payload)
                self.stats["commands"] += 1
                self.stats[f"command {label}"] += 1
                if "dump" in label:
                    self.stats["dumps"] += 1
                data += listener.frame(
                    rng.choice([0] * 9 + [rng.randrange(256)]), payload)
            if rng.random() < 0.1:   # a header that promises more
                data += struct.pack("<I", rng.randrange(1, 1 << 24))[:3] + \
                    b"\x00" + listener.random_bytes(rng, 0, 20)
            if rng.random() < 0.5:
                client.send(data)
            else:
                cuts = sorted(rng.randrange(len(data) + 1)
                              for _ in range(rng.randint(1, 4)))
                for start, end in zip([0] + cuts, cuts + [len(data)]):
                    client.send(data[start:end])
                    time.sleep(0.002)
            self.read(client, 2.0, quiet=0.15)
        finally:
            client.close()

    def odd_dump(self):
        rng = self.rng
        client = self.login()
        if client is None:
            return
        try:
            binlogs = self.binlogs.binlogs
            name, size = binlogs[-1] if rng.random() < 0.6 else \
                rng.choice(binlogs)
            position = rng.choice([
                4, size, max(size - 1, 0), size + 1,
                rng.randrange(0, size + 64), rng.randrange(0, size + 64),
                rng.randrange(5, min(size, 400) + 6)])
            flags = rng.choice([fuzz.NON_BLOCKING, 0])
            if self.send_command(client, fuzz.com_query(fuzz.CHECKSUM_ON),
                                 "checksum") != "OK":
                return
            if MODE != "position" and rng.random() < 0.5:
                payload = fuzz.com_binlog_dump_gtid(
                    flags, rng.randrange(1000, 2000), name, position,
                    self.source_gtids(rng.randrange(1, 50)))
            else:
                payload = fuzz.com_binlog_dump(
                    position, flags, rng.randrange(1000, 2000), name)
            self.stats["commands"] += 1
            self.stats["dumps"] += 1
            self.stats["dumps from odd positions"] += 1
            client.send(listener.frame(0, payload))
            how = rng.choice(["read", "read", "cut", "cut", "stop reading"])
            if how == "read":
                self.read(client, rng.uniform(0.2, 2.0), quiet=1.0)
            elif how == "cut":
                # one frame, then the header and a part of the next
                first = fuzz.read_frame(client, 2.0 * SCALE)
                if not isinstance(first, str):
                    client.sock.settimeout(1.0)
                    try:
                        client.sock.recv(4 + rng.randrange(1, 12))
                    except OSError:
                        pass
                self.stats["dumps cut inside a frame"] += 1
                reset_close(client)
            else:
                self.stats["dumps not read"] += 1
                self.halt.wait(rng.uniform(2.0, 8.0))
        finally:
            client.close()

    def heartbeat_dump(self):
        rng = self.rng
        client = self.login()
        if client is None:
            return
        try:
            period = rng.randrange(100_000_000, 1_000_000_000)
            if self.send_command(client, fuzz.com_query(fuzz.CHECKSUM_ON),
                                 "checksum") != "OK":
                return
            self.send_command(client, fuzz.com_query(
                b"SET @master_heartbeat_period = %d, "
                b"@source_heartbeat_period = %d" % (period, period)),
                "heartbeat period")
            name, size = self.binlogs.binlogs[-1]
            if MODE == "position":
                payload = fuzz.com_binlog_dump(
                    rng.choice([4, size]), 0, rng.randrange(2000, 3000),
                    name)
            else:
                payload = fuzz.com_binlog_dump_gtid(
                    0, rng.randrange(2000, 3000), b"", 4,
                    self.source_gtids(1 << 40))
            self.stats["commands"] += 1
            self.stats["dumps"] += 1
            self.stats["blocking dumps with heartbeats"] += 1
            client.send(listener.frame(0, payload))
            self.read(client, rng.uniform(2.0, 6.0))
        finally:
            client.close()

    def gtid_dump(self):
        rng = self.rng
        client = self.login()
        if client is None:
            return
        try:
            if self.send_command(client, fuzz.com_query(fuzz.CHECKSUM_ON),
                                 "checksum") != "OK":
                return
            other = uuid_module.UUID(rewrite.OTHER_UUID).bytes
            gtids = rng.choice([
                lambda: fuzz.gtid_set([]),
                lambda: self.source_gtids(rng.randrange(1, 4000)),
                lambda: self.source_gtids(rng.randrange(1, 4000)),
                lambda: self.source_gtids(1 << 40),
                lambda: fuzz.gtid_set([(self.uuid, b"", [
                    (rng.randrange(1, 50), rng.randrange(50, 90)),
                    (rng.randrange(100, 200), rng.randrange(200, 5000))],
                    None)]),
                lambda: fuzz.gtid_set([(other, b"", [(1, 100)], None)]),
                lambda: fuzz.gtid_set([(other, b"", [(1, 100)], None),
                                       (self.uuid, b"", [(1, 20)], None)]),
                lambda: fuzz.random_gtid_set(rng)])()
            binlogs = self.binlogs.binlogs
            name = rng.choice([b"", b"", rng.choice(binlogs)[0]])
            flags = rng.choice([fuzz.NON_BLOCKING, fuzz.NON_BLOCKING, 0])
            self.stats["commands"] += 1
            self.stats["dumps"] += 1
            self.stats["gtid dumps"] += 1
            client.send(listener.frame(0, fuzz.com_binlog_dump_gtid(
                flags, rng.randrange(3000, 4000), name,
                rng.choice([4, 4, rng.randrange(0, 5000)]), gtids)))
            self.read(client, rng.uniform(0.3, 3.0), quiet=1.5)
            if rng.random() < 0.3:
                reset_close(client)
        finally:
            client.close()

    def wrong_login(self):
        rng = self.rng
        client = self.connect()
        try:
            choice = rng.randrange(4)
            if choice == 0:
                # the garbage logins of listener_fuzz_test.py
                self.stats["garbage logins"] += 1
                listener.scenario(rng, client)
                client.read_frame()
                return
            user = replication.PULL_USER if choice == 1 else \
                rng.choice(["root", "nobody", "x" * 40])
            password = "wrong-" + str(rng.randrange(1000))
            self.stats["wrong logins"] += 1
            salt = client.server_salt()
            if salt is None:
                return
            if choice == 3:
                if not client.upgrade_to_tls():
                    return
                client.send(listener.frame(2, listener.greeting_payload(
                    listener.BASE_CAPABILITIES | listener.CLIENT_SSL,
                    user.encode(), listener.scramble(password, salt),
                    b"caching_sha2_password")))
            else:
                client.send(listener.frame(1, listener.greeting_payload(
                    listener.BASE_CAPABILITIES, user.encode(),
                    listener.scramble(password, salt),
                    b"caching_sha2_password")))
            for _ in range(5):
                read = client.read_frame()
                if read is None:
                    return
                sequence, body = read
                if body[:1] == b"\x00":
                    self.stats["WRONG LOGINS ACCEPTED"] += 1
                    return
                if body[:1] == b"\xff":
                    self.stats["wrong logins refused"] += 1
                    return
                if body == b"\x01\x04" and choice == 3:
                    client.send(listener.frame(sequence + 1,
                                               password.encode() + b"\x00"))
                elif body == b"\x01\x04":
                    client.send(listener.frame(sequence + 1, b"\x02"))
                elif body[:1] == b"\x01" and b"PUBLIC KEY" in body:
                    encrypted = listener.rsa_oaep_encrypt(
                        body[1:], listener.xor_with_salt(password, salt))
                    if encrypted is None:
                        return
                    client.send(listener.frame(sequence + 1, encrypted))
        finally:
            client.close()

    def idle(self):
        client = self.connect()
        try:
            if self.rng.random() < 0.5:
                client.read_frame()
            self.stats["idle connections"] += 1
            self.halt.wait(self.rng.uniform(0.5, 4.0))
        finally:
            client.close()


def honest_login(tls: bool) -> "float | None":
    """How long a clean login takes; None if it does not succeed within
    LOGIN_SECONDS (scaled)."""
    started = time.monotonic()
    try:
        client = listener.Listener("127.0.0.1", PULL_PORT,
                                   timeout=LOGIN_SECONDS * SCALE)
    except OSError:
        return None
    try:
        logged_in = (fuzz.tls_handshake_with(client) if tls
                     else client.handshake())
        seconds = time.monotonic() - started
        if logged_in and seconds <= LOGIN_SECONDS * SCALE:
            return round(seconds, 2)
        return None
    except (OSError, ssl.SSLError):
        return None
    finally:
        client.close()


class Watch(threading.Thread):
    """An honest login every few seconds, the memory of 'pull', and 'list'
    / 'search_by_*' in a loop."""

    def __init__(self, case):
        super().__init__(daemon=True)
        self.case = case
        self.halt = threading.Event()
        self.logins = []
        self.failed_logins = []
        self.slow_logins = []
        self.rss = []
        self.queries = collections.Counter()
        self.query_errors = collections.Counter()
        self.query_crashes = []
        self.querier = threading.Thread(target=self._queries, daemon=True)

    def run(self):
        self.querier.start()
        started = time.monotonic()
        tls = False
        while not self.halt.wait(4.0):
            pull = self.case.pull
            if self.case.pull_down.is_set() or \
                    self.case.stopping.is_set() or \
                    pull.exit_code() is not None:
                continue
            self.rss.append(pull.rss_mb())
            at = time.strftime("%H:%M:%S", time.gmtime())
            seconds = honest_login(tls)
            if seconds is None:
                # not while 'pull' is ending
                self.halt.wait(1.0 * SCALE)
                if not self.case.pull_down.is_set() and \
                        not self.case.stopping.is_set() and \
                        pull.exit_code() is None:
                    self.failed_logins.append(
                        f"{'TLS' if tls else 'plain'} login at {at} UTC ("
                        f"{round(time.monotonic() - started)} s)")
            else:
                self.logins.append(seconds)
                if seconds > 2.0:
                    self.slow_logins.append(
                        f"{seconds} s, {'TLS' if tls else 'plain'}, "
                        f"started {at} UTC, pull log {pull.log_path.name}")
            tls = not tls

    def _queries(self):
        config = write_config("query", None, self.case.encrypted)
        binary = str(harness.BINSRV.binary)
        commands = [["list"], ["search_by_timestamp", time.strftime(
            "%Y-%m-%dT%H:%M:%S", time.gmtime())],
                    ["search_by_timestamp", "now"]]
        if MODE != "position":
            commands.append(["search_by_gtid_set", "gtids"])
        index = 0
        while not self.halt.is_set():
            command = list(commands[index % len(commands)])
            index += 1
            if command[-1] == "now":
                command[-1] = time.strftime("%Y-%m-%dT%H:%M:%S",
                                            time.gmtime())
            elif command[-1] == "gtids":
                command[-1] = f"{SOURCE_UUID}:1-{1 + index % 4}"
            try:
                completed = subprocess.run(
                    RUN_PREFIX + [binary, command[0], str(config)] +
                    command[1:], capture_output=True, timeout=60 * SCALE,
                    check=False, env=environment())
            except subprocess.TimeoutExpired:
                self.query_crashes.append(f"{command[0]}: no end in "
                                          f"{60 * SCALE} s")
                continue
            self.queries[command[0]] += 1
            output = completed.stdout.decode(errors="replace")
            stderr = completed.stderr.decode(errors="replace")
            try:
                answer = json.loads(output)
            except ValueError:
                answer = {}
            if completed.returncode == 0 and \
                    answer.get("status") == "success":
                self.halt.wait(0.1)
                continue
            if completed.returncode not in (0, 1) or \
                    rewrite.sanitizer_markers(stderr):
                report = SAN_DIR / f"abnormal-query-{index}.txt"
                report.write_text(stderr + "\n--- stdout\n" + output)
                self.query_crashes.append(
                    f"{command[0]}: exit {completed.returncode}, see "
                    f"{report}")
                continue
            message = str(answer.get("message") or stderr.strip()[-200:])
            self.query_errors[
                command[0] + ": " +
                re.sub(r"\d{3,}", "N", message)[:160]] += 1
            self.halt.wait(0.05)

    def finish(self):
        self.halt.set()
        self.join(LOGIN_SECONDS * SCALE + 30)
        self.querier.join(60 * SCALE + 30)


# ---------------------------------------------------------------------------
# the sanitizer reports
# ---------------------------------------------------------------------------

def sanitizer_reports() -> dict:
    """The ThreadSanitizer report files of the case (one per process that
    had a report), grouped by SUMMARY line."""
    summaries = collections.Counter()
    files = []
    reports = 0
    for path in sorted(SAN_DIR.glob("tsan.*")):
        text = path.read_text(errors="replace")
        count = text.count("WARNING: ThreadSanitizer")
        if not count:
            continue
        reports += count
        files.append(path.name)
        for line in text.splitlines():
            if line.startswith("SUMMARY: ThreadSanitizer:"):
                summaries[re.sub(r"/\S*/(src|include)/", r"\1/",
                                 line[len("SUMMARY: ThreadSanitizer: "):])
                          [:200]] += 1
    return {"reports": reports, "files": files[:20],
            "directory": str(SAN_DIR),
            "summaries": dict(summaries.most_common(40))}


# ---------------------------------------------------------------------------
# a case
# ---------------------------------------------------------------------------

def without_open_end(files: dict) -> dict:
    """The binlogs of the '--stop-never' client, whose connection ended
    with 'pull': without the transaction that was on its way and without a
    last binlog of which only a part of the preamble had arrived."""
    files = hsp.without_open_transaction(files)
    if files:
        last = sorted(files)[-1]
        try:
            events = harness.parse_events(files[last])
        except harness.Failure:
            events = []
        if len(events) < 2:
            files = {name: data for name, data in files.items()
                     if name != last}
    return files


class Case:
    def __init__(self, name: str, proxy_port: int, seed: int):
        self.name = name
        self.proxy_port = proxy_port
        self.seed = seed
        self.encrypted = name == "encrypted"
        self.hostile = name == "hostile"
        self.pull = None
        self.pull_down = threading.Event()
        self.stopping = threading.Event()
        self.pull_exits = []

    def complete(self) -> bool:
        """The storage holds what the source has (seen through the
        replica, which gets it from 'pull')."""
        return hsp.replica_matches()

    def restart_if_exited(self, source: SourceSide) -> bool:
        """'pull' refused what the source sent and ended: it is started
        again through the proxy. False if it crashed, or keeps ending."""
        code = self.pull.exit_code()
        if code is None:
            return True
        self.pull_down.set()
        during = source.current or source.last[0]
        # the next start must not meet the same packet
        source.proxy.stop()
        stopped = self.pull.stop(1)
        errors = self.pull.errors(2)
        # 'pull' reads the source's stream seconds after the proxy sent it
        # (more so in a sanitizer build), so the reason is taken from its
        # log, not from the attack in force when it ended
        refused = any(ERR_TEXT in line for line in errors)
        self.pull_exits.append({"exit": code, "during": during,
                                "refused_err_packet": refused,
                                "errors": errors,
                                "crashed": crashed(stopped, "on its own")})
        if code != 1 or stopped["sanitizer"] or \
                not (refused or self.hostile) or len(self.pull_exits) > 40:
            return False
        self.pull = Pull(f"attack{len(self.pull_exits)}", self.proxy_port,
                         self.encrypted)
        self.pull_down.clear()
        return True

    def run(self) -> dict:
        binsrv = harness.BINSRV
        source = harness.SERVER
        rng = random.Random(self.seed)
        result = {"case": self.name, "mode": MODE, "build": BUILD,
                  "seed": self.seed, "duration": ARGS.duration}
        problems = []
        known = []
        hsp.ensure_replica()
        REPLICA.sql("STOP REPLICA")
        source.reset()
        binsrv.reset()
        for path in list(binsrv.workdir.glob("attack*")) + \
                list(binsrv.workdir.glob("recovery.*")):
            path.unlink()
        rewrite.insert_many("BEFORE", 4, 8)
        hostile.cache_password_hash(ARGS.mysql_basedir, source.port)
        proxy = MixProxy(self.proxy_port, source.port, SOURCE_UUID)
        mix = SOURCE_MIX[MODE] + (HOSTILE_MIX if self.hostile else [])
        client = recovered = side = watch = None
        halt_clients = threading.Event()
        stats = collections.Counter()
        attackers = []
        try:
            # 1. an honest start through the proxy, and the honest readers
            self.pull = Pull("attack", self.proxy_port, self.encrypted)
            if not wait_for(lambda: binsrv.indexed_binlogs() and
                            stored_binlogs() and
                            stored_binlogs()[-1][1] > 400,
                            CATCH_UP_SECONDS * SCALE):
                raise harness.Failure(
                    "before the attack: nothing was stored (exit "
                    f"{self.pull.exit_code()}, {self.pull.errors()})")
            first = binsrv.indexed_binlogs()[0]
            replication.replicate_from_pull(
                REPLICA, PULL_PORT, MODE != "position",
                "SOURCE_AUTO_POSITION=1" if MODE != "position" else
                f"SOURCE_LOG_FILE='{first}', SOURCE_LOG_POS=4")
            client = hsp.Client("client", first, True)
            if not wait_for(self.complete, CATCH_UP_SECONDS * SCALE):
                raise harness.Failure(
                    "before the attack: the replica did not catch up: "
                    f"{json.dumps(hsp.replica_view())[:400]}; pull exit "
                    f"{self.pull.exit_code()}, {self.pull.errors()}")
            rss_start = self.pull.rss_mb()

            # 2. both sides at once
            started = time.monotonic()
            side = SourceSide(proxy, mix, self.seed)
            watch = Watch(self)
            attackers = [Attacker(index, self.seed, halt_clients, stats,
                                  self.pull_down)
                         for index in range(ARGS.clients)]
            side.start()
            watch.start()
            for attacker in attackers:
                attacker.start()
            # SIGTERM at a random moment after the duration
            deadline = started + ARGS.duration + rng.uniform(0.0, 4.0)
            alive = True
            while time.monotonic() < deadline and alive:
                alive = self.restart_if_exited(side)
                time.sleep(0.2)
            result["attack_seconds"] = round(time.monotonic() - started, 1)
            rss_end = self.pull.rss_mb() if alive else 0
            result["rss_mb"] = {"start": rss_start, "end": rss_end,
                                "peak": self.pull.peak_rss,
                                "samples": watch.rss[::max(
                                    1, len(watch.rss) // 12)]}

            # 3. SIGTERM under load
            limit = READ_TIMEOUT + IDLE_TIME + 10 * SCALE
            if alive:
                final_login = honest_login(False)
                result["final_login_seconds"] = final_login
                if final_login is None and self.pull.exit_code() is None:
                    problems.append(
                        "hang: no login within "
                        f"{LOGIN_SECONDS * SCALE} s at the end of the "
                        "attack")
                result["source_attack_at_sigterm"] = side.current
                # the clients and the workload go on during the shutdown
                self.stopping.set()
                stopped = self.pull.stop(limit)
                self.pull_down.set()
                result["shutdown"] = stopped
                problems += crashed(stopped, "under the attack")
                if "killed_after_seconds" in stopped:
                    problems.append(
                        f"hang: pull did not stop within {limit} s of "
                        "SIGTERM under load (source attack: "
                        f"'{side.current}')")
                elif stopped.get("exit_before_signal") is None and \
                        stopped.get("exit_after_sigterm") != 0 and \
                        not self.hostile:
                    problems.append(
                        "pull exited with "
                        f"{stopped.get('exit_after_sigterm')} after "
                        f"SIGTERM: {self.pull.errors()}")
            else:
                self.stopping.set()
                self.pull_down.set()
                last = self.pull_exits[-1]
                problems += last["crashed"] or [
                    f"pull ended on its own with {last['exit']} during "
                    f"'{last['during']}' (exit number "
                    f"{len(self.pull_exits)}): {last['errors']}"]
            halt_clients.set()
            side.finish()
            watch.finish()
            for attacker in attackers:
                attacker.join(30)
            client.stop()
            proxy.close()
            result["source"] = {
                "connections_of_pull": len(proxy.connect_times),
                "attacks": dict(side.attacks),
                "attack_actions": dict(side.acted),
                "transactions": side.transactions,
                "rotations": side.rotations,
                "workload_errors": side.errors[:3],
                "pull_exits": self.pull_exits,
                "log_reconnect_lines": self.pull.log().count(
                    "trying to reconnect")}
            result["telemetry_reports"] = len(
                list(binsrv.telemetry.glob("*.json")))
            result["clients"] = dict(sorted(stats.items()))
            result["logins"] = {
                "honest_logins": len(watch.logins),
                "slowest_seconds": max(watch.logins, default=None),
                "over_2_seconds": watch.slow_logins[:8],
                "failed": watch.failed_logins[:5]}
            result["queries"] = {"runs": dict(watch.queries),
                                 "errors": dict(watch.query_errors),
                                 "crashes": watch.query_crashes[:5]}
            if stats["WRONG LOGINS ACCEPTED"]:
                problems.append(f"{stats['WRONG LOGINS ACCEPTED']} logins "
                                "with a wrong password were accepted")
            if watch.failed_logins:
                problems.append(
                    f"{len(watch.failed_logins)} honest logins did not "
                    f"succeed within {LOGIN_SECONDS * SCALE} s: " +
                    ", ".join(watch.failed_logins[:4]))
            if watch.query_crashes:
                problems.append("a query next to pull crashed: " +
                                "; ".join(watch.query_crashes[:3]))
            other = {message: count for message, count
                     in watch.query_errors.items()
                     if not TICKET_29.search(message)}
            if len(other) != len(watch.query_errors):
                known.append("ticket 29 (PBS-101): " + str(sum(
                    watch.query_errors.values()) - sum(other.values())) +
                    " queries failed next to the writer")
            if other:
                problems.append("queries next to pull failed: " +
                                json.dumps(other)[:400])
            if side.errors:
                problems.append("the workload on the source failed: " +
                                side.errors[0])
            if not sum(side.acted.values()):
                problems.append("the proxy changed nothing: no attack ran")
            if rss_end > 4 * max(rss_start, 1) and rss_end - rss_start > 300:
                problems.append(f"memory: {rss_start} MB at the start, "
                                f"{rss_end} MB at the end of the attack")

            # 4. what the attack left
            found = {}
            client_files = hsp.data_files(client.files())
            if MODE == "gtid_rewrite":
                client_files = without_open_end(client_files)
            hsp.note(found, hsp.judge(client_files)[0],
                     "the mysqlbinlog client got it")
            if not self.encrypted:
                after = hsp.snapshot()
                storage_problems, _ = hsp.judge(hsp.data_files(after),
                                                boundaries=True)
                odd = hsp.strangers(after)
                if odd:
                    storage_problems.append("entries that are no binlogs: " +
                                            ", ".join(odd[:4]))
                hsp.note(found, storage_problems,
                         "in the storage after the SIGTERM")

            # 5. a clean restart without the proxy
            rewrite.insert_many("AFTER", 2, 8)
            recovery = {}
            recovered = Pull("recovery", None, self.encrypted)
            wait_for(lambda: recovered.exit_code() is not None or
                     self.complete(), CATCH_UP_SECONDS * SCALE)
            if recovered.exit_code() is None and not self.complete():
                # its threads may have stopped: start them once more
                recovery["replica_before_restart"] = hsp.replica_view()
                hsp.replica_sql("STOP REPLICA; START REPLICA")
                recovery["replica_recovers_after_start_replica"] = wait_for(
                    self.complete, CATCH_UP_SECONDS * SCALE)
            recovery["pull_exit"] = recovered.exit_code()
            recovery["pull_errors"] = recovered.errors()
            view = hsp.replica_view()
            recovery["replica"] = view
            recovery["source_rows"] = rewrite.rows_digest(source)
            missing = ""
            if recovered.exit_code() is None:
                probe = hsp.Client("final", first, False)
                state = probe.finish(60 * SCALE)
                served_problems, missing = hsp.judge(
                    hsp.data_files(probe.files()))
                hsp.note(found, served_problems,
                         "served to a new client after the clean restart")
                recovery["new_client"] = {
                    "exit": state["exit"], "error": state["error"],
                    "bytes": probe.total(), "lacks": missing}
                if state["exit"] != 0:
                    hsp.note(found, ["a new mysqlbinlog client ended with "
                                     f"{state['exit']}: "
                                     f"{state['error'][-160:]}"],
                             "after the clean restart")
            if not self.encrypted:
                final_problems, on_disk = hsp.judge(
                    hsp.data_files(hsp.snapshot()), boundaries=False)
                hsp.note(found, final_problems,
                         "in the storage after the clean restart")
                missing = missing or on_disk
            listed = subprocess.run(
                RUN_PREFIX + [str(binsrv.binary), "list",
                              str(write_config("list", None,
                                               self.encrypted))],
                capture_output=True, timeout=120 * SCALE, check=False,
                env=environment())
            recovery["list_exit"] = listed.returncode
            # the source is idle now: ticket 43 (PBS-116)
            recovery["pull_stop"] = recovered.stop(limit)
            result["recovery"] = recovery
            located = [f"{detail} [{'; '.join(places)}]"
                       for detail, places in found.items()]
            if len(located) > 8:
                located = located[:8] + [f"and {len(located) - 8} more"]
            end_state = []
            end_state += crashed(recovery["pull_stop"],
                                 "after the clean restart")
            if recovery["pull_exit"] is not None or missing:
                end_state.append(
                    "a clean restart of pull without the proxy does not "
                    f"recover: exit {recovery['pull_exit']}, lacks "
                    f"[{missing}], {recovery['pull_errors']}")
            if listed.returncode != 0:
                end_state.append(
                    f"'list' exits with {listed.returncode} after the "
                    "clean restart: " +
                    listed.stdout.decode(errors="replace")[-200:])
            if view.get("died"):
                end_state.append("the replica SERVER died: " + view["died"])
            if view["errors"]:
                end_state.append("the replica reported errors: " +
                                 " | ".join(view["errors"])[:400])
            if not view.get("died") and not hsp.replica_matches():
                end_state.append(
                    f"after the clean restart the replica has "
                    f"{view['rows']}, the source "
                    f"{recovery['source_rows']}")
            if "replica_recovers_after_start_replica" in recovery:
                end_state.append("the replica needed STOP/START REPLICA "
                                 "after the clean restart")
            end_state += located
            if self.hostile:
                # ticket 81 (PBS-147): what a hostile source sent twice or out of
                # order is in the storage and went to the readers
                if end_state:
                    known.append("ticket 81 (PBS-147) (not judged in this case): " +
                                 "; ".join(end_state)[:600])
                problems += [problem for problem in end_state
                             if problem.startswith("pull crashed")]
            else:
                problems += end_state
        finally:
            halt_clients.set()
            for thread in (side, watch):
                if thread is not None:
                    thread.halt.set()
            if client is not None:
                client.stop()
            proxy.close()
            for process in (self.pull, recovered):
                if process is not None and process.exit_code() is None:
                    process.process.kill()
                    process.process.wait()
            try:
                REPLICA.sql("STOP REPLICA")
            except RuntimeError:
                pass
        reports = sanitizer_reports()
        result["sanitizer"] = reports
        if reports["reports"]:
            problems.append(
                f"{reports['reports']} ThreadSanitizer reports in "
                f"{reports['directory']}: " +
                "; ".join(f"{count}x {summary}" for summary, count
                          in list(reports["summaries"].items())[:6]))
        if known:
            result["known"] = known
        problems = list(dict.fromkeys(problems))
        if problems or result.get("logins", {}).get("over_2_seconds"):
            kept = SAN_DIR / "pull-logs"
            kept.mkdir(exist_ok=True)
            for path in list(binsrv.workdir.glob("attack*.log")) + \
                    list(binsrv.workdir.glob("attack*.stderr")) + \
                    list(binsrv.workdir.glob("recovery.*")):
                shutil.copy(path, kept / path.name)
            result["pull_logs"] = str(kept)
        result["ok"] = not problems
        if problems:
            result["why"] = "; ".join(problems)
        return result


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--mysql-basedir", type=Path, required=True)
    parser.add_argument("--binsrv", type=Path, required=True)
    parser.add_argument("--workdir", type=Path, required=True)
    parser.add_argument("--port", type=int, default=24400,
                        help="the source's port; the replica and 'pull' use "
                             "the two above it, the proxy the next ones")
    parser.add_argument("--mode", choices=["position", "gtid"],
                        default="position")
    parser.add_argument("--rewrite", action="store_true",
                        help="kept for compatibility: --mode gtid uses "
                             "'replication.rewrite'")
    parser.add_argument("--no-rewrite", action="store_true",
                        help="GTID mode without 'replication.rewrite', for "
                             "binaries older than PBS-80 / PBS-98")
    parser.add_argument("--cases", nargs="*",
                        help=f"of {' '.join(CASES)} (default: plain "
                             "encrypted)")
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument("--duration", type=float, default=90,
                        help="seconds of attack per case")
    parser.add_argument("--clients", type=int, default=6,
                        help="hostile client threads")
    parser.add_argument("--checkpoint-size", default="512")
    parser.add_argument("--load-pause", type=float, default=0.05,
                        help="seconds between two statements of the "
                             "workload")
    parser.add_argument("--timeout-scale", type=float,
                        help="default: 6 for a ThreadSanitizer build, 2 "
                             "for an AddressSanitizer build, else 1")
    parser.add_argument("--tsan-suppressions", type=Path,
                        default=TSAN_SUPPRESSIONS)
    args = parser.parse_args()
    if (args.rewrite or args.no_rewrite) and args.mode != "gtid":
        parser.error("--rewrite / --no-rewrite need --mode gtid")
    global ARGS, MODE, SCALE, BUILD, RUN_PREFIX  # pylint: disable=W0603
    global REPLICA, PULL_PORT, SAN_DIR, SOURCE_UUID  # pylint: disable=W0603
    ARGS = args
    MODE = args.mode if args.mode == "position" or args.no_rewrite \
        else "gtid_rewrite"
    args.mysql_basedir = args.mysql_basedir.expanduser()
    args.workdir = args.workdir.expanduser().resolve()
    args.workdir.mkdir(parents=True, exist_ok=True)
    args.binsrv = args.binsrv.expanduser()
    cases = args.cases or ["plain", "encrypted"]
    unknown = [name for name in cases if name not in CASES]
    if unknown or len(set(cases)) != len(cases):
        parser.error(f"unknown or repeated cases {unknown}; the cases: "
                     f"{' '.join(CASES)}")
    if "hostile" in cases and MODE != "gtid_rewrite":
        parser.error("the case 'hostile' needs --mode gtid (rewrite)")
    BUILD = build_kind(args.binsrv)
    SCALE = args.timeout_scale or {"tsan": 6.0, "asan": 2.0}.get(BUILD, 1.0)
    if BUILD == "tsan":
        # ThreadSanitizer needs the address space layout it expects
        RUN_PREFIX = ["setarch", platform.machine(), "-R"]
    gtid_mode = args.mode == "gtid"
    PULL_PORT = args.port + 2
    # the modules whose helpers are used here
    hsp.ARGS = args
    hsp.MODE = MODE
    hsp.PULL_PORT = PULL_PORT
    listener.PULL_USER = replication.PULL_USER
    listener.PULL_PASSWORD = replication.PULL_PASSWORD
    harness.SERVER = harness.MySQLServer(args.mysql_basedir,
                                         args.workdir / "mysql", args.port,
                                         gtid_mode, server_id=1)
    REPLICA = hsp.REPLICA = harness.MySQLServer(
        args.mysql_basedir, args.workdir / "replica", args.port + 1,
        gtid_mode, server_id=2)
    harness.BINSRV = harness.BinlogServer(args.binsrv,
                                          args.workdir / "binsrv", args.port,
                                          gtid_mode)
    results = []
    started = time.monotonic()
    shutil.rmtree(REPLICA.datadir, ignore_errors=True)
    harness.SERVER.start()
    try:
        REPLICA.start()
        SOURCE_UUID = harness.SERVER.sql("SELECT @@server_uuid").strip()
        for index, name in enumerate(cases):
            case_started = time.monotonic()
            hsp.CASE = name
            SAN_DIR = args.workdir / "san" / f"{MODE}-{name}"
            shutil.rmtree(SAN_DIR, ignore_errors=True)
            SAN_DIR.mkdir(parents=True)
            try:
                result = Case(name, args.port + 3 + index,
                              args.seed + index).run()
            except (harness.Failure, RuntimeError,
                    subprocess.SubprocessError, OSError) as failure:
                result = {"case": name, "mode": MODE, "build": BUILD,
                          "seed": args.seed + index, "ok": False,
                          "why": harness.describe_failure(failure, 700),
                          "sanitizer": sanitizer_reports()}
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
