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

"""Memory, disk and log a hostile or broken source (and replicas) can make
'fetch' / 'pull' consume, and whether any limit exists.

'fetch' / 'pull' keep the events of a transaction in memory until the
transaction ends (binsrv/storage.cpp: only complete transactions are handed
to the storage backend, at a checkpoint or when the binlog is closed), name
and rotate their binlogs after what the source sends (position mode) and log
several lines per event at level 'info'. A source that is compromised, or an
attacker between PBS and the source on a connection without TLS
verification, decides how much of each there is.

The test runs a real MySQL source and, for the attacks, the man-in-the-middle
proxy of hostile_source_test.py between it and binlog_server. Here the proxy
('FloodProxy') lets the dump through up to the first transaction ('the
template', which the source has just executed), holds that transaction back,
and then writes the stream itself from the template's events, with the event
sizes, positions, CRC32 footers and packet framing kept valid. Every
binlog_server the test starts runs under 'nice' and a hard address-space
limit (RLIMIT_AS: --memory-cap-mib, 3072; --unfinished-cap-mib, 1536, in
the cases of A, so that their streams end sooner), and no case streams more
than --stream-cap-mib; resident memory (VmRSS, VmHWM of /proc/<pid>/status) is
sampled against the bytes the proxy has sent.

Cases (a JSON line each; 'ok' false means the consumption is not bounded by
a limit, or the process died badly, or a cost grows faster than the data):

A. a transaction that never ends
  unfinished_fetch       the template's GTID event declares a
                         transaction_length just under 4 GiB, and its
                         WRITE_ROWS event is repeated (new log_pos and CRC32)
                         without an XID ever being sent; no checkpoints
  unfinished_checkpoints the same with checkpoint_size 1M and
                         checkpoint_interval 1s: do they bound it?
  unfinished_pull        the same against 'pull', up to the memory cap
  unfinished_pull_dropped  'pull', and the source drops the connection
                         after --checkpoint-stream-mib (384): is 'pull'
                         still alive, does it recover by itself, how much
                         memory does it keep
  overrun_declared       the declared length is kept and the WRITE_ROWS is
                         repeated: must be refused at the declared length
  For each: bytes of RSS per byte streamed, the largest RSS reached, what
  the process did at the cap (exit code or signal, last error), whether the
  storage is unchanged and whether a direct 'fetch' then completes the copy.

B. honest baseline
  honest_large_transaction  one real transaction of --honest-mib (300) from
                         the source, fetched directly: peak RSS as a multiple
                         of the transaction size, without checkpoints, with
                         them, and with storage encryption

C. many small things, one big thing
  complete_no_checkpoints  --complete-mib (320) of the template (1 MiB)
                         repeated as complete transactions (GTID mode:
                         consecutive GTIDs) with no checkpoint setting:
                         everything since the binlog was opened stays in
                         memory
  complete_checkpoints   half as much with checkpoint_size 1M: must stay
                         flat
  tiny_transactions      --tiny-transactions (50000) one-row transactions:
                         memory and log per transaction
  tiny_events            one transaction of --tiny-events (200000) small
                         WRITE_ROWS events: memory per buffered event
  gtid_gaps              GTID mode: --gap-transactions (1200) one-row
                         transactions whose GTIDs leave gaps (every second
                         number), with rewrite file_size --gaps-file-size
                         (4K): the GTID set gets one interval per
                         transaction, and it is written into the
                         PREVIOUS_GTIDS_LOG of every binlog PBS starts;
                         once that event alone exceeds file_size every
                         transaction starts a binlog. Stored bytes per byte
                         streamed, memory of 'fetch', and time and memory
                         of 'list' on the storage afterwards
  huge_event             one transaction whose WRITE_ROWS event is padded to
                         each of --huge-event-mib (64 256): accepted or not,
                         peak RSS as a multiple of the event size
  huge_event_declared    an event whose header declares 3.5 GiB, of which
                         only the first 16 MiB packet is sent before the
                         source stalls: memory must follow the bytes
                         received, not the declared size

D. disk
  rotation_flood         position mode: after the template the proxy sends
                         nothing but rotations (ROTATE, artificial ROTATE,
                         FORMAT_DESCRIPTION, PREVIOUS_GTIDS_LOG), one new
                         binlog each; GTID mode (source rotations are
                         ignored there): one-row transactions with rewrite
                         file_size 1K, one binlog each. For each of
                         --rotations (300 1000): binlogs per second at the
                         start and at the end, bytes written per rotation
                         at the start and at the end (binlog.index is
                         written anew at every rotation), files, index
                         size, bytes on disk,
                         log bytes, then the time and peak RSS of 'list' and
                         of the next 'fetch' start
  sequence_exhaustion    position mode: the source announces binlog.999998,
                         binlog.999999, binlog.1000000, binlog.1000001

E. the serving side of 'pull'
  stalled_replicas       --replicas (300) logged-in replicas request a
                         blocking dump of a --serve-mib (64) storage and
                         never read: RSS and threads per replica, and
                         whether mysqlbinlog is still served
  replica_huge_event     a storage whose binlog holds one event of
                         --serve-event-mib (256; the listener cannot send
                         an event of 16 MiB or more, ticket 10, PBS-76): one
                         replica requests that binlog, then --serve-clients
                         (8) at once: peak RSS of 'pull' as a multiple of
                         the event, and whether 'pull' survives

F. log volume
  log_volume             level 'info': bytes of log per byte streamed for
                         one-row transactions and for a flood of heartbeat
                         events (which store nothing)

--mode gtid runs the cases in GTID mode (which since PBS-80 always uses
rewrite mode; --rewrite is accepted for symmetry with the other harnesses
and selects a small rewrite file_size, 64K, for the memory cases, so that
PBS rotates by itself while the attacks run). A JSON line per case, then a
summary; exit 1 if any case failed. The default run takes under ten minutes
and stays under 2 GB of disk. Example:

  PYTHONPATH=~/ws/qa/pylib python3 resource_exhaustion_test.py \\
    --mysql-basedir ~/ws/dist/mysql-8.4.8-linux-glibc2.28-x86_64-minimal \\
    --binsrv ~/ws/percona-binlog-server-build-debug_gcc14/binlog_server \\
    --workdir ~/ws/qa/resource-exhaustion --mode gtid
"""

import argparse
import hashlib
import json
import os
import re
import resource
import shutil
import signal
import socket
import struct
import subprocess
import sys
import threading
import time
import zlib
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import fetch_crash_test as harness  # noqa: E402
import hostile_source_test as hostile  # noqa: E402
import listener_fuzz_test as listener  # noqa: E402
import listener_replica_dos_test as dos  # noqa: E402
import metadata_tamper_test as tamper  # noqa: E402
import rewrite_attack_test as rw  # noqa: E402

MIB = 1 << 20
XID_EVENT = 16
HEARTBEAT_EVENT = 27
ARTIFICIAL_FLAG = 0x20
IGNORABLE_FLAG = 0x80
EOF_PACKET = b"\xfe\x00\x00\x02\x00"
MAX_PACKET = 0xFFFFFF
# the proxy hands its packets to the socket in pieces of this size
FLUSH_BYTES = 256 * 1024
# 'transaction_length' just under the 4 GiB the reader accepts (ticket 41, PBS-114)
DECLARED_LENGTH = 0xFFFFFF00
# an event header that declares 3.5 GiB
DECLARED_EVENT_SIZE = 3500 * MIB
DATA_NAME = re.compile(r"^binlog\.\d+$")
# RSS growth (MiB) a hostile stream may cause before the case counts as
# "memory follows the stream"
GROWTH_LIMIT_MIB = 256
# peak RSS of an honest transaction as a multiple of its size
HONEST_MULTIPLE_LIMIT = 3.5
# a rotation at the end of a flood may cost this many times one at its start
ROTATION_COST_RATIO_LIMIT = 3.0
# seconds 'list' / the next start may take on a flooded storage
START_SECONDS_LIMIT = 10.0
# MiB of RSS a stalled replica may hold
REPLICA_MIB_LIMIT = 8.0
# bytes of log per byte of stream at level 'info'
LOG_RATIO_LIMIT = 1.0

ARGS = None
# the address-space limit (MiB) of the binlog_server processes a case
# starts, when it is not --memory-cap-mib
CAP_MIB = None
PULL_PORT = 0
DEAD_PORT = 0
PROXY_PORT = 0


# ---------------------------------------------------------------------------
# binlog_server under a memory cap
# ---------------------------------------------------------------------------

def limited():
    """Runs in the child before exec: a lower priority and the hard
    address-space limit every binlog_server of this test gets."""
    os.nice(10)
    cap = (CAP_MIB or ARGS.memory_cap_mib) * MIB
    resource.setrlimit(resource.RLIMIT_AS, (cap, cap))


def proc_status(pid: int) -> dict:
    """VmRSS, VmHWM, VmSize (KiB) and Threads of a process; {} once gone."""
    found = {}
    try:
        for line in Path(f"/proc/{pid}/status").read_text().splitlines():
            key, _, value = line.partition(":")
            if key in ("VmRSS", "VmHWM", "VmSize", "Threads"):
                found[key] = int(value.split()[0])
    except (OSError, ValueError, IndexError):
        return {}
    return found


class Watch:
    """Samples the memory of 'pid' against the bytes 'proxy' has sent."""

    def __init__(self, pid: int, proxy=None, step: float = 0.05):
        self.pid = pid
        self.proxy = proxy
        self.step = step
        self.samples = []  # (seconds, bytes sent, rss KiB)
        self.hwm = 0
        self.threads = 0
        self.started = time.monotonic()
        self.running = True
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

    def _run(self):
        while self.running:
            status = proc_status(self.pid)
            if "VmRSS" in status:
                sent = self.proxy.sent if self.proxy is not None else 0
                self.samples.append((time.monotonic() - self.started, sent,
                                     status["VmRSS"]))
                self.hwm = max(self.hwm, status.get("VmHWM", 0))
                self.threads = max(self.threads, status.get("Threads", 0))
            time.sleep(self.step)

    def stop(self) -> dict:
        self.running = False
        self.thread.join(timeout=2)
        if not self.samples:
            return {"peak_rss_mib": 0, "vm_hwm_mib": 0}
        peak = max(self.samples, key=lambda sample: sample[2])
        # the memory before the proxy wrote anything of its own
        flood_from = (self.proxy.flood_from if self.proxy is not None and
                      self.proxy.flood_from is not None else 0)
        before = [sample[2] for sample in self.samples
                  if sample[1] <= flood_from] or [self.samples[0][2]]
        base = before[-1]
        top = max(peak[2], self.hwm)
        result = {"base_rss_mib": round(base / 1024, 1),
                  "peak_rss_mib": round(peak[2] / 1024, 1),
                  "vm_hwm_mib": round(self.hwm / 1024, 1),
                  "rss_growth_mib": round((top - base) / 1024, 1)}
        if self.proxy is None:
            return result
        flood = [(sent - flood_from, rss * 1024)
                 for _, sent, rss in self.samples if sent > flood_from]
        result["streamed_at_peak_mib"] = round(
            max(0, peak[1] - flood_from) / MIB, 1)
        if len(flood) >= 3:
            mean_x = sum(x for x, _ in flood) / len(flood)
            mean_y = sum(y for _, y in flood) / len(flood)
            spread = sum((x - mean_x) ** 2 for x, _ in flood)
            if spread:
                result["rss_bytes_per_stream_byte"] = round(
                    sum((x - mean_x) * (y - mean_y) for x, y in flood) /
                    spread, 3)
            stride = max(1, len(flood) // 6)
            result["streamed_mib_vs_rss_mib"] = [
                [round(x / MIB), round(y / MIB)]
                for x, y in flood[stride - 1::stride]][:7]
        streamed = self.proxy.sent - flood_from
        if streamed > 0:
            result["growth_per_stream_byte"] = round(
                (top - base) * 1024 / streamed, 3)
        return result


def start_binsrv(operation: str, config: Path, label: str,
                 keep_stdout: bool = False) -> subprocess.Popen:
    stderr = (ARGS.workdir / f"{label}.stderr").open("wb")
    stdout = ((ARGS.workdir / f"{label}.stdout").open("wb") if keep_stdout
              else subprocess.DEVNULL)
    try:
        return subprocess.Popen(
            [str(harness.BINSRV.binary), operation, str(config)],
            stdout=stdout, stderr=stderr, preexec_fn=limited,
            env=tamper.binsrv_environment())
    finally:
        stderr.close()
        if keep_stdout:
            stdout.close()


def stdout_tail(label: str, limit: int = 300) -> str:
    """The end of what a run started with 'keep_stdout' printed; the file
    (which can be large) is removed."""
    path = ARGS.workdir / f"{label}.stdout"
    try:
        with path.open("rb") as output:
            output.seek(max(0, path.stat().st_size - limit))
            return output.read().decode(errors="replace").strip()
    except OSError:
        return ""
    finally:
        path.unlink(missing_ok=True)


def outcome(code: "int | None") -> str:
    if code is None:
        return "still running"
    if code < 0:
        try:
            return f"killed by {signal.Signals(-code).name}"
        except ValueError:
            return f"killed by signal {-code}"
    return f"exit {code}"


def stderr_tail(label: str, limit: int = 300) -> str:
    try:
        return (ARGS.workdir / f"{label}.stderr").read_text(
            errors="replace").strip()[-limit:]
    except OSError:
        return ""


def log_path(label: str) -> Path:
    return harness.BINSRV.workdir / f"{label}.log"


def log_size(label: str) -> int:
    try:
        return log_path(label).stat().st_size
    except OSError:
        return 0


def log_errors(label: str, limit: int = 2, ignore: str = "") -> list:
    """The last error lines of a run's log (read from its tail: the log of
    a flood is large)."""
    try:
        with log_path(label).open("rb") as log:
            log.seek(max(0, log_size(label) - 200_000))
            text = log.read().decode(errors="replace")
    except OSError:
        return []
    return [line.strip().split("] ", 2)[-1][:260]
            for line in text.splitlines()
            if ("[  error]" in line or "exception caught" in line) and
            not (ignore and ignore in line)][-limit:]


def run_timed(operation: str, config: Path, label: str,
              timeout: float = 600, keep_stdout: bool = False) -> dict:
    """One binlog_server run to its end: exit, seconds, peak RSS (the
    kernel's ru_maxrss)."""
    started = time.monotonic()
    process = start_binsrv(operation, config, label, keep_stdout)
    deadline = started + timeout
    while True:
        pid, status, usage = os.wait4(process.pid, os.WNOHANG)
        if pid:
            break
        if time.monotonic() > deadline:
            process.kill()
            _, status, usage = os.wait4(process.pid, 0)
            break
        time.sleep(0.01)
    process.returncode = os.waitstatus_to_exitcode(status)
    return {"exit": process.returncode,
            "seconds": round(time.monotonic() - started, 2),
            "peak_rss_mib": round(usage.ru_maxrss / 1024, 1)}


def run_watched(operation: str, config: Path, label: str,
                timeout: float = 600) -> dict:
    """One binlog_server run to its end with its memory sampled."""
    started = time.monotonic()
    process = start_binsrv(operation, config, label)
    watch = Watch(process.pid)
    try:
        process.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait()
    memory = watch.stop()
    return {"exit": process.returncode,
            "seconds": round(time.monotonic() - started, 2), **memory}


# ---------------------------------------------------------------------------
# configuration, source, storage
# ---------------------------------------------------------------------------

def make_config(label: str, proxied: bool = False, checkpoint_size=None,
                checkpoint_interval=None, file_size: "str | None" = None,
                encrypted: bool = False, source_port: "int | None" = None,
                read_timeout: "int | None" = None) -> Path:
    """The configuration of one binlog_server run, with a log of its own."""
    binsrv = harness.BINSRV
    binsrv.checkpoint_size = checkpoint_size
    binsrv.checkpoint_interval = checkpoint_interval
    binsrv.write_config()
    config = json.loads(binsrv.config_path.read_text())
    config["logger"]["file"] = str(log_path(label))
    config["connection"]["connect_timeout"] = 5
    if binsrv.gtid_mode:
        config["replication"]["rewrite"] = {
            "base_file_name": harness.BINLOG_BASE_NAME,
            "file_size": file_size or ARGS.file_size}
    if encrypted:
        keyring = binsrv.workdir / "keyring.json"
        keyring.write_text(json.dumps(tamper.KEYRING))
        config["keyring"] = {"uri": f"file://{keyring}"}
        config["storage"]["encryption"] = dict(tamper.ENCRYPTION)
    if proxied:
        # through the proxy, without TLS
        config["connection"]["port"] = PROXY_PORT
        config["connection"]["ssl"] = {"mode": "disabled"}
    if source_port is not None:
        config["connection"]["port"] = source_port
    if read_timeout is not None:
        config["connection"]["read_timeout"] = read_timeout
    # used by 'pull' only
    config["replication_source"] = {
        "port": PULL_PORT, "server_id": 142,
        "server_uuid": "00000000-0000-0000-0000-00000000008e",
        "read_timeout": dos.TIMEOUT, "write_timeout": dos.TIMEOUT,
        "require_secure_transport": False,
        "auth_rsa_private_key": str(listener.AUTH_RSA_PRIVATE_KEY),
        "authentication": {"user": listener.PULL_USER,
                           "password": listener.PULL_PASSWORD,
                           "plugin": "caching_sha2_password"}}
    path = binsrv.workdir / f"{label}.json"
    path.write_text(json.dumps(config, indent=2))
    log_path(label).unlink(missing_ok=True)
    return path


def data_files(directory: Path) -> list:
    try:
        return sorted((path for path in directory.iterdir()
                       if DATA_NAME.match(path.name)),
                      key=lambda path: int(path.name.split(".")[1]))
    except OSError:
        return []


def storage_facts() -> dict:
    """Entries, bytes and the index size of the storage directory."""
    storage = harness.BINSRV.storage
    entries = 0
    total = 0
    binlogs = 0
    with os.scandir(storage) as found:
        for entry in found:
            entries += 1
            total += entry.stat().st_size
            binlogs += 1 if DATA_NAME.match(entry.name) else 0
    try:
        index = (storage / "binlog.index").stat().st_size
    except OSError:
        index = 0
    return {"storage_entries": entries, "storage_binlogs": binlogs,
            "storage_mib": round(total / MIB, 2), "index_bytes": index}


def storage_digest() -> str:
    """A digest of the names and contents of the storage."""
    digest = hashlib.sha256()
    for path in sorted(harness.BINSRV.storage.iterdir()):
        if path.is_file():
            digest.update(path.name.encode() + b"\0")
            with path.open("rb") as content:
                for block in iter(lambda: content.read(MIB), b""):
                    digest.update(block)
    return digest.hexdigest()[:16]


def transactions_in(directory: Path) -> int:
    """The transactions (GTID events) in the binlogs of a directory; -1 if
    a binlog cannot be walked."""
    count = 0
    for path in data_files(directory):
        try:
            count += sum(1 for _, code, _ in
                         harness.parse_events(path.read_bytes())
                         if code in harness.TRANSACTION_START_EVENTS)
        except Exception:  # pylint: disable=broad-except
            return -1
    return count


def must_fetch(label: str, **settings) -> dict:
    run = run_timed("fetch", make_config(label, **settings), label)
    if run["exit"] != 0:
        raise harness.Failure(
            f"a direct fetch ({label}) ended with {outcome(run['exit'])}: "
            f"{log_errors(label)} {stderr_tail(label)}")
    return run


def prepare(template: list, **settings) -> None:
    """A fresh source and storage; the storage holds the source's first
    binlog data, and the source has then executed 'template' (statements):
    the first transaction a resumed dump sends."""
    harness.SERVER.reset()
    harness.BINSRV.reset()
    harness.SERVER.sql("CREATE TABLE qa.first (x INT); "
                       "INSERT INTO qa.first VALUES (1);")
    must_fetch("prepare", **settings)
    for statement in template:
        harness.SERVER.sql(statement)


def rows_statement(rows: int, size: int) -> str:
    values = ", ".join(f"(REPEAT('r', {size}))" for _ in range(rows))
    return f"INSERT INTO qa.t (payload) VALUES {values};"


def recovery(label: str, **settings) -> dict:
    """After an attack that must leave nothing behind: 'list', then a
    direct fetch, then whether the storage holds every transaction of the
    source."""
    listed = run_timed("list", make_config(f"{label}-list", **settings),
                       f"{label}-list", 120)
    fetched = run_timed("fetch", make_config(f"{label}-direct", **settings),
                        f"{label}-direct", 300)
    source = transactions_in(harness.SERVER.datadir)
    stored = transactions_in(harness.BINSRV.storage)
    result = {"list_exit": listed["exit"],
              "direct_fetch": outcome(fetched["exit"]),
              "source_transactions": source, "stored_transactions": stored}
    if fetched["exit"] != 0:
        result["direct_fetch_errors"] = log_errors(f"{label}-direct")
    return result


def recovery_problems(result: dict) -> list:
    problems = []
    if result["list_exit"] != 0:
        problems.append("the storage no longer opens ('list' exit "
                        f"{result['list_exit']})")
    if result["direct_fetch"] != "exit 0":
        problems.append("a direct fetch from the source then fails ("
                        f"{result['direct_fetch']}: "
                        f"{result.get('direct_fetch_errors')})")
    elif result["source_transactions"] != result["stored_transactions"]:
        problems.append(
            f"after a direct fetch the storage holds "
            f"{result['stored_transactions']} transactions, the source "
            f"{result['source_transactions']}")
    return problems


# ---------------------------------------------------------------------------
# the man in the middle
# ---------------------------------------------------------------------------

class Stream:
    """The dump connection towards binlog_server: packet numbering, the
    position of the binlog being sent and the events seen so far."""

    def __init__(self, client: socket.socket, proxy: "FloodProxy"):
        self.client = client
        self.proxy = proxy
        self.sequence = 0
        self.buffer = bytearray()
        self.total = 0
        self.position = 4
        self.name = b""
        self.server_id = 1
        self.description = None
        self.previous = None
        self.held = None
        self.held_from = 0

    def flush(self):
        if self.buffer:
            self.client.sendall(self.buffer)
            self.proxy.sent += len(self.buffer)
            self.buffer = bytearray()

    def packet(self, payload) -> None:
        """A payload of any size as MySQL packets (16 MiB - 1 each, the
        last one shorter)."""
        view = memoryview(payload)
        offset = 0
        while True:
            piece = view[offset:offset + MAX_PACKET]
            offset += len(piece)
            self.sequence = (self.sequence + 1) & 0xFF
            header = struct.pack("<I", len(piece))[:3] + bytes([self.sequence])
            self.total += 4 + len(piece)
            if len(piece) > FLUSH_BYTES:
                self.flush()
                self.client.sendall(header)
                self.client.sendall(piece)
                self.proxy.sent += 4 + len(piece)
            else:
                self.buffer += header
                self.buffer += piece
                if len(self.buffer) >= FLUSH_BYTES:
                    self.flush()
            if len(piece) < MAX_PACKET:
                return

    def event(self, body: bytearray, artificial: bool = False,
              advance: bool = True) -> None:
        """'body' (an event without its footer) at the current position:
        its size, log_pos and CRC32 are set and it is sent."""
        size = len(body) + 4
        struct.pack_into("<I", body, 9, size)
        if artificial:
            struct.pack_into("<I", body, 13, 0)
        else:
            if advance:
                self.position += size
            struct.pack_into("<I", body, 13, self.position & 0xFFFFFFFF)
        self.proxy.flood_events += 1
        self.packet(b"\x00" + bytes(body) +
                    struct.pack("<I", zlib.crc32(body)))

    def header(self, code: int, flags: int, timestamp: int) -> bytearray:
        return bytearray(struct.pack("<IBIIIH", timestamp, code,
                                     self.server_id, 0, 0, flags))


def with_length(gtid: bytearray, value: int) -> bytearray:
    """A GTID event whose 'transaction_length' is 'value', written as a
    9-byte length-encoded integer."""
    position, width, _ = rw.length_field(gtid)
    return bytearray(gtid[:position] + b"\xfe" + struct.pack("<Q", value) +
                     gtid[position + width:])


class FloodProxy(hostile.RenamingProxy):
    """The proxy of hostile_source_test.py with a stream of its own: the
    dump passes unchanged up to the first transaction, which is held back
    until its XID; then 'plan' decides what binlog_server gets, written
    from that transaction's events. 'plan' members: 'attack', 'bytes' (the
    most the proxy writes), 'count', 'end' ('eof': the dump ends as a
    non-blocking one does, 'drop': the connection is closed, 'hold': it
    stays open and silent until the proxy is closed). Connections after the
    attacked one pass unchanged."""

    def __init__(self, listen_port: int, source_port: int, plan: dict):
        self.plan = plan
        self.sent = 0
        self.flood_from = None
        self.flood_events = 0
        self.units = 0
        self.connections = 0
        self.attacks = 0
        self.ended = ""
        self.failure = ""
        self.flood_seconds = 0.0
        self.finished = threading.Event()
        self.release = threading.Event()
        self.sockets = []
        super().__init__(listen_port, source_port, b"", b"", plan["attack"])

    def close(self):
        self.release.set()
        for sock in list(self.sockets):
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
        super().close()

    def _accept(self):
        while True:
            try:
                client, _ = self.listener.accept()
            except OSError:
                return
            try:
                server = socket.create_connection(
                    ("127.0.0.1", self.source_port))
            except OSError:
                client.close()
                continue
            self.connections += 1
            self.sockets += [client, server]
            dumping = threading.Event()
            threading.Thread(target=self._upstream,
                             args=(client, server, dumping),
                             daemon=True).start()
            threading.Thread(target=self._downstream,
                             args=(server, client, dumping),
                             daemon=True).start()

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
                        hostile.COM_BINLOG_DUMP,
                        hostile.COM_BINLOG_DUMP_GTID):
                    dumping.set()
                server.sendall(header + payload)
        except OSError:
            pass
        finally:
            try:
                server.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass

    def _downstream(self, server, client, dumping):
        stream = Stream(client, self)
        client.settimeout(self.plan.get("send_timeout", 60))
        whole = b""
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
                whole += payload
                if length == MAX_PACKET:
                    continue
                payload, whole = whole, b""
                if self._event(stream, payload):
                    break
        except OSError:
            pass
        finally:
            for sock in (client, server):
                try:
                    sock.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass

    def _event(self, stream: Stream, payload: bytes) -> bool:
        """Passes one event of the dump on; True once the attack has been
        made on this connection (which is then finished)."""
        if len(payload) < 1 + hostile.HEADER_SIZE + 4 or payload[0] != 0 or \
                self.attacks:
            stream.packet(payload)
            stream.flush()
            return False
        event = payload[1:]
        if zlib.crc32(event[:-4]) != struct.unpack("<I", event[-4:])[0]:
            self.failure = "the source sent an event without a CRC32"
            stream.packet(payload)
            stream.flush()
            return False
        body = bytearray(event[:-4])
        code = body[4]
        flags = struct.unpack_from("<H", body, 17)[0]
        log_pos = struct.unpack_from("<I", body, 13)[0]
        if code == hostile.ROTATE_EVENT and flags & ARTIFICIAL_FLAG:
            stream.name = bytes(body[hostile.HEADER_SIZE + 8:])
            stream.position = struct.unpack_from(
                "<Q", body, hostile.HEADER_SIZE)[0]
            stream.server_id = struct.unpack_from("<I", body, 5)[0]
        elif code == hostile.FORMAT_DESCRIPTION_EVENT:
            stream.description = bytes(body)
        elif code == hostile.PREVIOUS_GTIDS_LOG_EVENT:
            stream.previous = bytes(body)
        if stream.held is None and code in harness.TRANSACTION_START_EVENTS:
            stream.held = [body]
            stream.held_from = log_pos - len(event)
            return False
        if stream.held is not None:
            stream.held.append(body)
            if code != XID_EVENT:
                return False
            self.attacks += 1
            stream.position = stream.held_from
            self._attack(stream, stream.held)
            return True
        if log_pos:
            stream.position = log_pos
        stream.packet(payload)
        stream.flush()
        return False

    def _attack(self, stream: Stream, transaction: list) -> None:
        stream.flush()
        self.flood_from = self.sent
        # binlog_server has stored what was passed on: its memory now is
        # what the stream of the proxy is measured against
        time.sleep(0.5)
        started = time.monotonic()
        try:
            getattr(self, "_" + self.plan["attack"])(stream, transaction)
            stream.flush()
            self.ended = "the proxy sent all it had"
            end = self.plan.get("end", "drop")
            if end == "eof":
                stream.packet(EOF_PACKET)
                stream.flush()
        except socket.timeout:
            self.ended = "binlog_server stopped reading"
            end = "drop"
        except OSError:
            self.ended = "binlog_server closed the connection"
            end = "drop"
        self.flood_seconds = time.monotonic() - started
        self.finished.set()
        if end == "hold":
            self.release.wait()

    def _room(self, stream: Stream) -> bool:
        return stream.total < self.plan["bytes"]

    def _unfinished(self, stream: Stream, transaction: list) -> None:
        """The transaction without its XID, then its last rows event again
        and again."""
        declared = self.plan.get("declared")
        first = (with_length(transaction[0], declared)
                 if declared is not None else transaction[0])
        stream.event(first)
        for body in transaction[1:-1]:
            stream.event(body)
        rows = transaction[-2]
        while self._room(stream):
            stream.event(rows)
            self.units += 1

    def _complete(self, stream: Stream, transaction: list) -> None:
        """The transaction again and again, as complete transactions: in
        GTID mode with the GTIDs that follow its own, 'step' apart."""
        first = transaction[0]
        tagged = first[4] == harness.GTID_TAGGED_LOG_EVENT
        number = struct.unpack_from("<q", first, 36)[0]
        sequence = struct.unpack_from("<q", first, 53)[0]
        step = self.plan.get("step", 1)
        count = self.plan.get("count", 1 << 62)
        while self.units < count and self._room(stream):
            if not tagged:
                if first[4] == harness.GTID_LOG_EVENT:
                    struct.pack_into("<q", first, 36,
                                     number + self.units * step)
                struct.pack_into("<qq", first, 45, sequence + self.units - 1,
                                 sequence + self.units)
            for body in transaction:
                stream.event(body)
            self.units += 1

    def _tiny_events(self, stream: Stream, transaction: list) -> None:
        """One transaction with its last rows event 'count' times."""
        count = self.plan["count"]
        rows = transaction[-2]
        first = with_length(transaction[0], 0)
        total = len(first) + 4 + sum(len(body) + 4
                                     for body in transaction[1:]) + \
            (count - 1) * (len(rows) + 4)
        stream.event(with_length(transaction[0], total))
        for body in transaction[1:-1]:
            stream.event(body)
        for _ in range(count - 1):
            stream.event(rows)
            self.units += 1
        stream.event(transaction[-1])

    def _huge_event(self, stream: Stream, transaction: list) -> None:
        """One transaction whose last rows event is padded to 'size'
        bytes; with 'declared_size' the header claims that size instead and
        only the first packet is sent."""
        size = self.plan["size"]
        declared = self.plan.get("declared_size")
        rows = transaction[-2]
        first = with_length(transaction[0], 0)
        total = len(first) + 4 + sum(len(body) + 4
                                     for body in transaction[1:]) - \
            (len(rows) + 4) + (declared or size)
        stream.event(with_length(transaction[0], total & 0xFFFFFFFF))
        for body in transaction[1:-2]:
            stream.event(body)
        stream.flush()
        payload = bytearray(1 + size)
        payload[1:1 + len(rows)] = rows
        view = memoryview(payload)
        struct.pack_into("<I", payload, 1 + 9, declared or size)
        stream.position += declared or size
        struct.pack_into("<I", payload, 1 + 13, stream.position & 0xFFFFFFFF)
        struct.pack_into("<I", payload, size - 3, zlib.crc32(view[1:-4]))
        self.flood_events += 1
        if declared:
            # the first of the packets such an event would take
            stream.sequence = (stream.sequence + 1) & 0xFF
            stream.client.sendall(b"\xff\xff\xff" + bytes([stream.sequence]))
            stream.client.sendall(view[:MAX_PACKET])
            self.sent += 4 + MAX_PACKET
            stream.total += 4 + MAX_PACKET
            return
        stream.packet(view)
        del view, payload
        stream.event(transaction[-1])
        self.units += 1

    def _rotations(self, stream: Stream, transaction: list) -> None:
        """The transaction as it is, then 'count' rotations: every one
        closes the current binlog and starts the next, which stays empty."""
        for body in transaction:
            stream.event(body)
        if stream.description is None:
            raise harness.Failure("no FORMAT_DESCRIPTION seen in the dump")
        description = bytearray(stream.description)
        flags = struct.unpack_from("<H", description, 17)[0]
        struct.pack_into("<H", description, 17,
                         flags & ~harness.LOG_EVENT_BINLOG_IN_USE_F)
        if stream.previous is not None:
            previous = bytearray(stream.previous)
        else:
            # not sent on a resume in position mode: an empty set
            previous = stream.header(hostile.PREVIOUS_GTIDS_LOG_EVENT,
                                     IGNORABLE_FLAG, int(time.time())) + \
                struct.pack("<Q", 0)
        current = int(stream.name.rsplit(b".", 1)[1])
        number = self.plan.get("first_sequence", current + 1)
        for _ in range(self.plan["count"]):
            if not self._room(stream):
                break
            name = b"%s.%06d" % (harness.BINLOG_BASE_NAME.encode(), number)
            number += 1
            stream.event(stream.header(hostile.ROTATE_EVENT, 0,
                                       int(time.time())) +
                         struct.pack("<Q", 4) + name)
            stream.position = 4
            stream.event(stream.header(hostile.ROTATE_EVENT, ARTIFICIAL_FLAG,
                                       0) + struct.pack("<Q", 4) + name,
                         artificial=True)
            stream.event(description)
            stream.event(previous)
            self.units += 1
            if self.units % 50 == 0:
                stream.flush()

    def _heartbeats(self, stream: Stream, transaction: list) -> None:
        """The transaction as it is, then 'count' heartbeat events."""
        for body in transaction:
            stream.event(body)
        heartbeat = stream.header(HEARTBEAT_EVENT, 0, 0) + stream.name
        for _ in range(self.plan["count"]):
            if not self._room(stream):
                break
            stream.event(heartbeat, advance=False)
            self.units += 1


def attacked(operation: str, label: str, plan: dict, timeout: float = 300,
             after=None, on_start=None, **settings) -> dict:
    """binlog_server ('fetch' or 'pull') through the proxy with 'plan'.
    Waits until the process ends, or until the proxy has finished and (for
    'pull', or a held connection) 'after(process, proxy, result)' has run;
    then stops what is left. Returns the measurements."""
    plan.setdefault("bytes", ARGS.stream_cap_mib * MIB)
    plan["bytes"] = min(plan["bytes"], ARGS.stream_cap_mib * MIB)
    config = make_config(label, proxied=True, **settings)
    hostile.cache_password_hash(ARGS.mysql_basedir, ARGS.port)
    proxy = FloodProxy(PROXY_PORT, ARGS.port, plan)
    started = time.monotonic()
    process = start_binsrv(operation, config, label)
    watch = Watch(process.pid, proxy)
    if on_start is not None:
        on_start(process)
    result = {}
    try:
        while process.poll() is None and not proxy.finished.is_set() and \
                time.monotonic() - started < timeout:
            time.sleep(0.05)
        if process.poll() is None and proxy.finished.is_set() and \
                operation == "fetch" and plan.get("end") != "hold":
            # the dump has ended: fetch is expected to end by itself
            try:
                process.wait(timeout=max(
                    120, timeout - (time.monotonic() - started)))
            except subprocess.TimeoutExpired:
                result["hung_after_the_stream_ended"] = True
        if proxy.attacks:
            # the proxy notices a closed connection a moment later
            proxy.finished.wait(5)
        result["seconds"] = round(time.monotonic() - started, 1)
        result["during"] = outcome(process.poll())
        if after is not None and process.poll() is None:
            after(process, proxy, result)
    finally:
        memory = watch.stop()
        if process.poll() is None:
            process.send_signal(signal.SIGTERM)
            proxy.close()
            try:
                process.wait(timeout=20)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
                result["killed_by_the_test"] = True
        proxy.close()
    flood_from = proxy.flood_from or 0
    streamed = max(0, proxy.sent - flood_from)
    result.update(
        operation=operation, outcome=outcome(process.returncode),
        proxy_ended=proxy.ended or "the attack did not start",
        connections=proxy.connections, attacks=proxy.attacks,
        streamed_mib=round(streamed / MIB, 1),
        events_sent=proxy.flood_events, units=proxy.units,
        flood_seconds=round(proxy.flood_seconds, 1),
        log_mib=round(log_size(label) / MIB, 2),
        errors=log_errors(label), **memory)
    if streamed:
        result["log_bytes_per_stream_byte"] = round(
            log_size(label) / streamed, 3)
    if result["outcome"] not in ("exit 0", "exit 1"):
        result["stderr"] = stderr_tail(label)
    if proxy.failure:
        raise harness.Failure(proxy.failure)
    if not proxy.attacks:
        raise harness.Failure(
            f"the proxy saw no transaction to attack ({label}): "
            f"{result['outcome']}, {result['errors']} {stderr_tail(label)}")
    result["_streamed_bytes"] = streamed
    return result


def public(result: dict) -> dict:
    return {key: value for key, value in result.items()
            if not key.startswith("_")}


def finish(name: str, result: dict, problems: list, bound: str = "") -> dict:
    result = {"case": name, "ok": not problems, **public(result)}
    if problems:
        result["why"] = "; ".join(problems)
    elif bound:
        result["bound"] = bound
    return result


def died_badly(run: dict) -> list:
    """A signal, or an exit code other than 0 and 1."""
    if run["outcome"] in ("exit 0", "exit 1") or \
            run.get("killed_by_the_test") or run["outcome"] == "killed by " \
            "SIGTERM":
        return []
    return [f"{run['operation']} ended with {run['outcome']} "
            f"({run.get('stderr', '')[-160:]})"]


def follows_stream(run: dict) -> bool:
    return run["rss_growth_mib"] > GROWTH_LIMIT_MIB


# ---------------------------------------------------------------------------
# A. a transaction that never ends
# ---------------------------------------------------------------------------

def unfinished(name: str, operation: str, stream_mib: "int | None" = None,
               **settings) -> dict:
    # a lower cap than the other cases have: the stream ends sooner
    global CAP_MIB  # pylint: disable=global-statement
    CAP_MIB = min(ARGS.unfinished_cap_mib, ARGS.memory_cap_mib)
    try:
        return unfinished_capped(name, operation, stream_mib, **settings)
    finally:
        CAP_MIB = None


def unfinished_capped(name: str, operation: str, stream_mib: "int | None",
                      **settings) -> dict:
    prepare([rows_statement(1, MIB)], **settings)
    before = storage_digest()
    plan = {"attack": "unfinished", "declared": DECLARED_LENGTH,
            "end": "drop"}
    if stream_mib is not None:
        # the proxy ends the stream below the memory cap
        plan["bytes"] = stream_mib * MIB
    extra = {}

    def after_pull(process, proxy, result):
        # the attacked connection is over; later ones pass unchanged
        deadline = time.monotonic() + 40
        source = transactions_in(harness.SERVER.datadir)
        while time.monotonic() < deadline and process.poll() is None and \
                transactions_in(harness.BINSRV.storage) != source:
            time.sleep(1)
        extra["pull_alive_after"] = process.poll() is None
        extra["pull_recovered_by_itself"] = \
            transactions_in(harness.BINSRV.storage) == source
        extra["rss_after_mib"] = round(
            proc_status(process.pid).get("VmRSS", 0) / 1024, 1)

    run = attacked(operation, name, plan,
                   after=after_pull if operation == "pull" else None,
                   **settings)
    run.update(extra)
    run["declared_transaction_length"] = DECLARED_LENGTH
    run["memory_cap_mib"] = CAP_MIB
    run["storage_unchanged"] = storage_digest() == before
    problems = []
    if follows_stream(run):
        problems.append(
            "memory follows a transaction that does not end: "
            f"{run.get('rss_bytes_per_stream_byte')} bytes of RSS per byte "
            f"streamed, {run['peak_rss_mib']} MiB RSS (VmHWM "
            f"{run['vm_hwm_mib']}) after {run['streamed_mib']} MiB under a "
            f"{CAP_MIB} MiB address-space cap"
            + (f", with checkpoint_size {settings['checkpoint_size']} and "
               f"checkpoint_interval {settings['checkpoint_interval']}"
               if settings.get("checkpoint_size") else "") +
            f"; then: {run['during']}, {run['errors'][-1:]}")
    problems += died_badly(run)
    if not run["storage_unchanged"] and operation == "fetch":
        problems.append("the storage changed although no transaction ended")
    if operation == "pull" and not extra.get("pull_alive_after", False):
        problems.append(f"pull is gone ({run['outcome']}, "
                        f"{run['errors'][-1:]})")
    recovered = recovery(name, **settings)
    run.update(recovered)
    problems += recovery_problems(recovered)
    return finish(name, run, problems,
                  f"RSS grew by {run['rss_growth_mib']} MiB for "
                  f"{run['streamed_mib']} MiB streamed")


def case_unfinished_fetch(name: str) -> dict:
    return unfinished(name, "fetch")


def case_unfinished_checkpoints(name: str) -> dict:
    return unfinished(name, "fetch", ARGS.checkpoint_stream_mib,
                      checkpoint_size="1M", checkpoint_interval="1s")


def case_unfinished_pull(name: str) -> dict:
    return unfinished(name, "pull", checkpoint_size="1M",
                      checkpoint_interval="1s")


def case_unfinished_pull_dropped(name: str) -> dict:
    return unfinished(name, "pull", ARGS.checkpoint_stream_mib,
                      checkpoint_size="1M", checkpoint_interval="1s")


def case_overrun_declared(name: str) -> dict:
    prepare([rows_statement(1, MIB)])
    before = storage_digest()
    run = attacked("fetch", name, {"attack": "unfinished", "end": "drop",
                                   "bytes": 256 * MIB})
    run["storage_unchanged"] = storage_digest() == before
    problems = died_badly(run)
    if follows_stream(run):
        problems.append(
            f"RSS grew by {run['rss_growth_mib']} MiB "
            f"({run['streamed_mib']} MiB streamed) past the declared "
            "transaction length")
    if run["outcome"] == "exit 0":
        problems.append("fetch accepted events past the declared "
                        "transaction length")
    recovered = recovery(name)
    run.update(recovered)
    problems += recovery_problems(recovered)
    return finish(name, run, problems,
                  "refused at the declared transaction_length: "
                  f"{run['errors'][-1:]}")


# ---------------------------------------------------------------------------
# B. honest baseline
# ---------------------------------------------------------------------------

def case_honest_large_transaction(name: str) -> dict:
    harness.SERVER.reset()
    statements = "".join(rows_statement(10, MIB)
                         for _ in range(max(1, ARGS.honest_mib // 10)))
    started = time.monotonic()
    # the transaction stays in the session's memory, not in the tmpdir
    harness.SERVER.sql("SET GLOBAL binlog_cache_size = "
                       f"{(ARGS.honest_mib + 64) * MIB}")
    harness.SERVER.sql(f"BEGIN; {statements} COMMIT; TRUNCATE qa.t;")
    harness.SERVER.sql("SET GLOBAL binlog_cache_size = DEFAULT")
    result = {"source_seconds": round(time.monotonic() - started, 1)}
    size = sum(path.stat().st_size
               for path in data_files(harness.SERVER.datadir))
    result["transaction_mib"] = round(size / MIB, 1)
    problems = []
    variants = {
        "no_checkpoints": {},
        "checkpoints_16M_1s": {"checkpoint_size": "16M",
                               "checkpoint_interval": "1s"},
        "encrypted": {"encrypted": True},
    }
    for variant, settings in variants.items():
        harness.BINSRV.reset()
        label = f"{name}-{variant}"
        run = run_watched("fetch", make_config(label, **settings), label)
        stored = sum(path.stat().st_size
                     for path in data_files(harness.BINSRV.storage))
        multiple = round(max(run["peak_rss_mib"], run["vm_hwm_mib"]) * MIB /
                         size, 2)
        result[variant] = {
            "outcome": outcome(run["exit"]), "seconds": run["seconds"],
            "peak_rss_mib": run["peak_rss_mib"],
            "vm_hwm_mib": run["vm_hwm_mib"],
            "peak_rss_per_transaction_byte": multiple,
            "stored_mib": round(stored / MIB, 1),
            "log_mib": round(log_size(label) / MIB, 2)}
        if run["exit"] != 0:
            result[variant]["errors"] = log_errors(label)
            problems.append(f"{variant}: fetch ended with "
                            f"{outcome(run['exit'])} {log_errors(label)}")
        elif multiple > HONEST_MULTIPLE_LIMIT:
            problems.append(
                f"{variant}: peak RSS is {multiple} times the transaction "
                f"({run['vm_hwm_mib']} MiB for {result['transaction_mib']} "
                "MiB)")
        log_path(label).unlink(missing_ok=True)
    harness.BINSRV.reset()
    worst = max(result[variant]["peak_rss_per_transaction_byte"]
                for variant in variants)
    return finish(name, result, problems,
                  f"peak RSS is at most {worst}"
                  " times the transaction, which the reader limits to 4 GiB "
                  "(ticket 41, PBS-114)")


# ---------------------------------------------------------------------------
# C. many small things, one big thing
# ---------------------------------------------------------------------------

def complete(name: str, stream_mib: int, **settings) -> dict:
    prepare([rows_statement(1, MIB)], **settings)
    run = attacked("fetch", name,
                   {"attack": "complete", "end": "eof",
                    "bytes": stream_mib * MIB}, **settings)
    run.update(storage_facts())
    return run


def case_complete_no_checkpoints(name: str) -> dict:
    run = complete(name, ARGS.complete_mib)
    problems = died_badly(run)
    if run["outcome"] != "exit 0":
        problems.append(f"fetch did not store the transactions: "
                        f"{run['outcome']} {run['errors']}")
    if follows_stream(run):
        problems.append(
            "without a checkpoint setting memory follows the complete "
            "transactions of the binlog being received: "
            f"{run.get('rss_bytes_per_stream_byte')} bytes of RSS per byte "
            f"streamed, {run['peak_rss_mib']} MiB RSS (VmHWM "
            f"{run['vm_hwm_mib']}) for {run['streamed_mib']} MiB in "
            f"{run['units']} transactions")
    return finish(name, run, problems,
                  f"RSS grew by {run['rss_growth_mib']} MiB for "
                  f"{run['streamed_mib']} MiB (rewrite file_size "
                  f"{ARGS.file_size})" if harness.BINSRV.gtid_mode else
                  f"RSS grew by {run['rss_growth_mib']} MiB for "
                  f"{run['streamed_mib']} MiB")


def case_complete_checkpoints(name: str) -> dict:
    run = complete(name, ARGS.complete_mib // 2, checkpoint_size="1M")
    problems = died_badly(run)
    if run["outcome"] != "exit 0":
        problems.append(f"fetch did not store the transactions: "
                        f"{run['outcome']} {run['errors']}")
    if follows_stream(run):
        problems.append(
            f"with checkpoint_size 1M RSS still grew by "
            f"{run['rss_growth_mib']} MiB for {run['streamed_mib']} MiB of "
            "complete transactions")
    return finish(name, run, problems,
                  "checkpoint_size 1M: RSS grew by "
                  f"{run['rss_growth_mib']} MiB for {run['streamed_mib']} "
                  "MiB")


def case_tiny_transactions(name: str) -> dict:
    settings = {"checkpoint_size": "1M"}
    prepare([rows_statement(1, 16)], **settings)
    run = attacked("fetch", name,
                   {"attack": "complete", "end": "eof",
                    "count": ARGS.tiny_transactions}, timeout=900, **settings)
    run.update(storage_facts())
    count = max(1, run["units"])
    run["rss_growth_bytes_per_transaction"] = round(
        run["rss_growth_mib"] * MIB / count)
    run["microseconds_per_transaction"] = round(
        run["seconds"] * 1e6 / count)
    run["log_bytes_per_transaction"] = round(run["log_mib"] * MIB / count)
    problems = died_badly(run)
    if run["outcome"] != "exit 0":
        problems.append(f"fetch did not store the transactions: "
                        f"{run['outcome']} {run['errors']}")
    if follows_stream(run):
        problems.append(
            f"RSS grew by {run['rss_growth_mib']} MiB for {count} one-row "
            f"transactions ({run['streamed_mib']} MiB): "
            f"{run['rss_growth_bytes_per_transaction']} bytes each")
    return finish(name, run, problems,
                  f"{run['rss_growth_bytes_per_transaction']} bytes of RSS "
                  "per transaction with checkpoint_size 1M")


def case_tiny_events(name: str) -> dict:
    prepare([rows_statement(1, 16)])
    before = storage_facts()["storage_mib"]
    run = attacked("fetch", name,
                   {"attack": "tiny_events", "end": "eof",
                    "count": ARGS.tiny_events}, timeout=900)
    run.update(storage_facts())
    stored = run["storage_mib"] - before
    run["rss_growth_per_buffered_byte"] = round(
        run["rss_growth_mib"] / max(run["streamed_mib"], 0.001), 2)
    problems = died_badly(run)
    if run["outcome"] != "exit 0":
        problems.append(f"fetch did not store the transaction: "
                        f"{run['outcome']} {run['errors']}")
    if run["rss_growth_per_buffered_byte"] > HONEST_MULTIPLE_LIMIT and \
            follows_stream(run):
        problems.append(
            f"a transaction of {run['events_sent']} small events "
            f"({run['streamed_mib']} MiB) took {run['rss_growth_mib']} MiB "
            "of RSS")
    run["stored_mib"] = round(stored, 1)
    return finish(name, run, problems,
                  f"{run['rss_growth_per_buffered_byte']} bytes of RSS per "
                  "byte of the transaction: no per-event overhead")


def case_gtid_gaps(name: str) -> dict:
    if not harness.BINSRV.gtid_mode:
        return {"case": name, "ok": True,
                "skipped": "GTID mode only: position mode keeps no GTID set"}
    settings = {"checkpoint_size": "1M", "file_size": ARGS.gaps_file_size}
    prepare([rows_statement(1, 16)], **settings)
    before = storage_facts()
    run = attacked("fetch", name,
                   {"attack": "complete", "end": "eof", "step": 2,
                    "count": ARGS.gap_transactions}, timeout=900, **settings)
    run.update(storage_facts())
    stored = (run["storage_mib"] - before["storage_mib"]) * MIB
    run["rewrite_file_size"] = ARGS.gaps_file_size
    run["stored_bytes_per_stream_byte"] = round(
        stored / max(1, run["_streamed_bytes"]), 1)
    run["binlogs_per_transaction"] = round(
        (run["storage_binlogs"] - before["storage_binlogs"]) /
        max(1, run["units"]), 3)
    largest = max((path.stat().st_size for path in
                   harness.BINSRV.storage.iterdir()
                   if path.suffix == ".json"), default=0)
    run["largest_metadata_bytes"] = largest
    listed = run_timed("list", make_config(f"{name}-list", **settings),
                       f"{name}-list", 300, keep_stdout=True)
    run["list_seconds"], run["list_peak_rss_mib"] = \
        listed["seconds"], listed["peak_rss_mib"]
    run["list"] = outcome(listed["exit"])
    said = stdout_tail(f"{name}-list", 200)
    # the next start for writing (the source has none of these GTIDs, so
    # at best it ends with the source's refusal)
    again = run_timed("fetch", make_config(f"{name}-again", **settings),
                      f"{name}-again", 300)
    run["next_fetch"] = outcome(again["exit"])
    run["next_fetch_seconds"] = again["seconds"]
    run["next_fetch_peak_rss_mib"] = again["peak_rss_mib"]
    run["next_fetch_errors"] = log_errors(f"{name}-again")
    problems = died_badly(run)
    if listed["exit"] != 0:
        run["list_said"] = said
        problems.append(
            f"the storage no longer opens under the {ARGS.memory_cap_mib} "
            f"MiB cap: 'list' ends with {outcome(listed['exit'])} after "
            f"{listed['seconds']} s and {listed['peak_rss_mib']} MiB of RSS "
            f"({said[-120:]} {stderr_tail(name + '-list', 120)}); the next "
            f"fetch: {run['next_fetch']} after {again['seconds']} s and "
            f"{again['peak_rss_mib']} MiB {run['next_fetch_errors'][-1:]}")
    elif listed["peak_rss_mib"] > GROWTH_LIMIT_MIB or \
            listed["seconds"] > START_SECONDS_LIMIT:
        problems.append(
            f"'list' on that storage ({run['storage_mib']} MiB) takes "
            f"{listed['seconds']} s and {listed['peak_rss_mib']} MiB of "
            "RSS: every start loads the GTID set of every binlog")
    if run["stored_bytes_per_stream_byte"] > 10:
        problems.append(
            f"{run['units']} transactions with gaps between their GTIDs "
            f"({run['streamed_mib']} MiB streamed) made the storage grow "
            f"by {round(stored / MIB, 1)} MiB in "
            f"{run['storage_binlogs'] - before['storage_binlogs']} binlogs: "
            f"{run['stored_bytes_per_stream_byte']} bytes stored per byte "
            "streamed (the GTID set, one interval per transaction, is "
            "written at the start of every binlog)")
    if follows_stream(run):
        problems.append(
            f"the RSS of fetch grew by {run['rss_growth_mib']} MiB "
            f"({run.get('growth_per_stream_byte')} bytes per byte streamed)")
    return finish(name, run, problems,
                  f"{run['stored_bytes_per_stream_byte']} bytes stored per "
                  "byte streamed")


def case_huge_event(name: str) -> dict:
    result = {"sizes": {}}
    problems = []
    accepted = 0
    for size_mib in ARGS.huge_event_mib:
        prepare([rows_statement(1, 16)])
        label = f"{name}-{size_mib}"
        run = attacked("fetch", label,
                       {"attack": "huge_event", "end": "eof",
                        "size": size_mib * MIB,
                        "bytes": (size_mib + 1) * MIB}, timeout=600)
        multiple = round(max(run["peak_rss_mib"], run["vm_hwm_mib"]) /
                         size_mib, 2)
        result["sizes"][str(size_mib)] = {
            "outcome": run["outcome"], "during": run["during"],
            "seconds": run["seconds"], "streamed_mib": run["streamed_mib"],
            "peak_rss_mib": run["peak_rss_mib"],
            "vm_hwm_mib": run["vm_hwm_mib"],
            "peak_rss_per_event_byte": multiple,
            "errors": run["errors"], **storage_facts()}
        if run["outcome"] == "exit 0":
            accepted = max(accepted, size_mib)
        problems += [f"{size_mib} MiB: {problem}"
                     for problem in died_badly(run)]
        if multiple > HONEST_MULTIPLE_LIMIT and follows_stream(run):
            problems.append(
                f"an event of {size_mib} MiB took {run['vm_hwm_mib']} MiB "
                f"of RSS ({multiple} times its size)")
        if run["outcome"] != "exit 0":
            recovered = recovery(label)
            result["sizes"][str(size_mib)].update(recovered)
            problems += [f"{size_mib} MiB: {problem}"
                         for problem in recovery_problems(recovered)]
        harness.BINSRV.reset()
    result["largest_accepted_mib"] = accepted
    result["memory_cap_mib"] = ARGS.memory_cap_mib
    return finish(name, result, problems,
                  f"the largest event accepted here is {accepted} MiB; peak "
                  "RSS is a small multiple of the event")


def case_huge_event_declared(name: str) -> dict:
    prepare([rows_statement(1, 16)])
    before = storage_digest()
    held = {}

    def after(process, proxy, result):
        time.sleep(5)
        status = proc_status(process.pid)
        held["rss_while_stalled_mib"] = round(status.get("VmRSS", 0) / 1024)
        held["vm_size_while_stalled_mib"] = round(
            status.get("VmSize", 0) / 1024)

    run = attacked("fetch", name,
                   {"attack": "huge_event", "end": "hold",
                    "size": MAX_PACKET - 1,
                    "declared_size": DECLARED_EVENT_SIZE}, after=after)
    run.update(held)
    run["declared_event_mib"] = DECLARED_EVENT_SIZE // MIB
    run["storage_unchanged"] = storage_digest() == before
    problems = []
    if follows_stream(run):
        problems.append(
            f"an event header declaring {DECLARED_EVENT_SIZE // MIB} MiB "
            f"made RSS grow by {run['rss_growth_mib']} MiB after "
            f"{run['streamed_mib']} MiB received")
    if not run["storage_unchanged"]:
        problems.append("the storage changed")
    recovered = recovery(name)
    run.update(recovered)
    problems += recovery_problems(recovered)
    return finish(name, run, problems,
                  "memory follows the bytes received "
                  f"({run['rss_growth_mib']} MiB for {run['streamed_mib']} "
                  "MiB), not the declared event size")


# ---------------------------------------------------------------------------
# D. disk
# ---------------------------------------------------------------------------

class FileCounter:
    """Counts the binlogs in the storage, and the bytes binlog_server has
    written (wchar of /proc/<pid>/io), while a flood runs."""

    def __init__(self):
        self.timeline = []  # (seconds, binlogs, bytes written)
        self.pid = None
        self.running = True
        self.started = time.monotonic()
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

    def _run(self):
        while self.running:
            try:
                with os.scandir(harness.BINSRV.storage) as found:
                    count = sum(1 for entry in found
                                if DATA_NAME.match(entry.name))
            except OSError:
                count = 0
            written = 0
            try:
                for line in Path(f"/proc/{self.pid}/io").read_text(
                ).splitlines():
                    if line.startswith("wchar:"):
                        written = int(line.split()[1])
            except (OSError, ValueError):
                pass
            if written or self.pid is None:
                self.timeline.append((time.monotonic() - self.started,
                                      count, written))
            time.sleep(0.1)

    def attach(self, process):
        self.pid = process.pid

    def stop(self):
        self.running = False
        self.thread.join(timeout=5)

    def _between(self, low: int, high: int) -> "tuple | None":
        start = next((entry for entry in self.timeline if entry[1] >= low),
                     None)
        end = next((entry for entry in self.timeline if entry[1] >= high),
                   None)
        if start is None or end is None or end[0] <= start[0] or \
                end[1] <= start[1]:
            return None
        return start, end

    def rate(self, low: int, high: int) -> "float | None":
        """Binlogs per second while the count went from 'low' to 'high'."""
        found = self._between(low, high)
        if found is None:
            return None
        start, end = found
        return round((end[1] - start[1]) / (end[0] - start[0]), 1)

    def written(self, low: int, high: int) -> "int | None":
        """Bytes written per binlog while the count went from 'low' to
        'high'."""
        found = self._between(low, high)
        if found is None:
            return None
        start, end = found
        return round((end[2] - start[2]) / (end[1] - start[1]))

    def total_written(self) -> int:
        return self.timeline[-1][2] if self.timeline else 0


def rotation_run(name: str, count: int) -> tuple:
    gtid = harness.BINSRV.gtid_mode
    settings = {"checkpoint_size": "1M"}
    if gtid:
        # the smallest file_size accepted, and transactions larger than it
        settings["file_size"] = "1K"
        plan = {"attack": "complete", "end": "eof", "count": count}
        prepare([rows_statement(1, 1200)], **settings)
    else:
        plan = {"attack": "rotations", "end": "eof", "count": count}
        prepare([rows_statement(1, 16)], **settings)
    label = f"{name}-{count}"
    counter = FileCounter()
    try:
        run = attacked("fetch", label, plan, timeout=1800,
                       on_start=counter.attach, **settings)
    finally:
        counter.stop()
    facts = storage_facts()
    tenth = max(10, count // 10)
    early = (tenth // 2, tenth + tenth // 2)
    late = (facts["storage_binlogs"] - tenth - tenth // 2,
            facts["storage_binlogs"] - tenth // 2)
    first = counter.rate(*early)
    last = counter.rate(*late)
    listed = run_timed("list", make_config(f"{label}-list", **settings),
                       f"{label}-list", 600)
    # in position mode the source has none of these binlogs, so the next
    # fetch ends with an error once the storage is loaded and it has
    # connected: its time is that of a start
    again = run_timed("fetch", make_config(f"{label}-again", **settings),
                      f"{label}-again", 600)
    entry = {
        "outcome": run["outcome"], "seconds": run["seconds"],
        "rotations_sent": run["units"],
        "streamed_mib": run["streamed_mib"], **facts,
        "binlogs_per_second_at_start": first,
        "binlogs_per_second_at_end": last,
        "milliseconds_per_rotation_at_start":
            round(1000 / first, 2) if first else None,
        "milliseconds_per_rotation_at_end":
            round(1000 / last, 2) if last else None,
        "bytes_written_per_rotation_at_start": counter.written(*early),
        "bytes_written_per_rotation_at_end": counter.written(*late),
        "written_mib": round(counter.total_written() / MIB, 1),
        "peak_rss_mib": run["peak_rss_mib"], "log_mib": run["log_mib"],
        "log_bytes_per_binlog": round(
            run["log_mib"] * MIB / max(1, facts["storage_binlogs"])),
        "list_exit": listed["exit"], "list_seconds": listed["seconds"],
        "list_peak_rss_mib": listed["peak_rss_mib"],
        "next_fetch": outcome(again["exit"]),
        "next_fetch_seconds": again["seconds"],
        "next_fetch_peak_rss_mib": again["peak_rss_mib"],
        "errors": run["errors"]}
    for extra in (f"{label}-list", f"{label}-again", label):
        log_path(extra).unlink(missing_ok=True)
    return entry, run


def case_rotation_flood(name: str) -> dict:
    gtid = harness.BINSRV.gtid_mode
    result = {"how": "one-row transactions with rewrite file_size 1K (a "
                     "setting, not the source: source rotations are ignored "
                     "in rewrite mode)" if gtid else
                     "rotations sent by the source, an empty binlog each",
              "runs": {}}
    problems = []
    for count in ARGS.rotations:
        entry, run = rotation_run(name, count)
        result["runs"][str(count)] = entry
        problems += [f"{count}: {problem}" for problem in died_badly(run)]
        if entry["storage_binlogs"] < count:
            problems.append(
                f"{count}: only {entry['storage_binlogs']} binlogs were "
                f"stored ({entry['outcome']}, {entry['errors']})")
        first, last = (entry["binlogs_per_second_at_start"],
                       entry["binlogs_per_second_at_end"])
        if first and last and first / last > ROTATION_COST_RATIO_LIMIT:
            problems.append(
                f"{count}: a rotation costs "
                f"{entry['milliseconds_per_rotation_at_end']} ms at the "
                f"end against {entry['milliseconds_per_rotation_at_start']}"
                f" ms at the start ({round(first / last, 1)} times): the "
                "cost of a rotation grows with the number of binlogs")
        small, large = (entry["bytes_written_per_rotation_at_start"],
                        entry["bytes_written_per_rotation_at_end"])
        if small and large and large / small > ROTATION_COST_RATIO_LIMIT:
            # the growth per binlog, and what it comes to for more of them
            slope = (large - small) / (0.8 * entry["storage_binlogs"])
            problems.append(
                f"{count}: a rotation writes {large} bytes at the end "
                f"against {small} at the start: {round(slope, 1)} bytes "
                "more for every binlog there is (binlog.index is written "
                f"anew), {entry['written_mib']} MiB written for a storage "
                f"of {entry['storage_mib']} MiB; at that rate 20000 "
                f"binlogs take {round(slope * 2e8 / (1 << 30), 1)} GiB of "
                f"writes and 100000 {round(slope * 5e9 / (1 << 30))} GiB")
        if entry["list_exit"] != 0:
            problems.append(f"{count}: 'list' exit {entry['list_exit']}")
        for what in ("list_seconds", "next_fetch_seconds"):
            if entry[what] > START_SECONDS_LIMIT:
                problems.append(f"{count}: {what} is {entry[what]}")
        harness.BINSRV.reset()
    last = list(result["runs"].values())[-1]
    each = round(last["storage_mib"] * MIB / max(1, last["storage_binlogs"]))
    return finish(
        name, result, problems,
        ("a binlog per transaction is the operator's setting here" if gtid
         else "the number of binlogs is the source's choice, their rate is "
              "not") +
        f": {last['milliseconds_per_rotation_at_end']} ms per rotation at "
        f"{last['storage_binlogs']} binlogs "
        f"({last['milliseconds_per_rotation_at_start']} ms at the start), "
        f"2 files and {each} bytes per binlog; 'list' "
        f"{last['list_seconds']} s and the next start "
        f"{last['next_fetch_seconds']} s at that size")


def case_sequence_exhaustion(name: str) -> dict:
    if harness.BINSRV.gtid_mode:
        return {"case": name, "ok": True,
                "skipped": "position mode only: in rewrite mode PBS numbers "
                           "the binlogs itself and is known to refuse the "
                           "one after 999999 ('binlog sequence number is "
                           "too large')"}
    prepare([rows_statement(1, 16)])
    run = attacked("fetch", name,
                   {"attack": "rotations", "end": "eof", "count": 4,
                    "first_sequence": 999_998})
    run["stored_names"] = [path.name for path in
                           data_files(harness.BINSRV.storage)]
    run["index"] = harness.BINSRV.indexed_binlogs()
    listed = run_timed("list", make_config(f"{name}-list"), f"{name}-list")
    run["list_exit"] = listed["exit"]
    run["list_errors"] = log_errors(f"{name}-list") + \
        [stderr_tail(f"{name}-list", 200)]
    problems = died_badly(run)
    if listed["exit"] != 0:
        problems.append(
            f"after the source announced binlog.999998 to binlog.1000001 "
            f"the storage no longer opens ('list' exit {listed['exit']}: "
            f"{run['list_errors']})")
    return finish(name, run, problems,
                  f"fetch: {run['outcome']} {run['errors'][-1:]}; stored "
                  f"{run['stored_names'][-4:]}; the storage opens")


# ---------------------------------------------------------------------------
# E. the serving side of 'pull'
# ---------------------------------------------------------------------------

def start_pull(label: str, **settings):
    # a short read timeout: 'pull' notices SIGTERM only when its read from
    # the source returns (ticket 43, PBS-116)
    config = make_config(label, read_timeout=5, **settings)
    process = start_binsrv("pull", config, label)
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline and process.poll() is None:
        try:
            socket.create_connection(("127.0.0.1", PULL_PORT),
                                     timeout=1).close()
            return process
        except OSError:
            time.sleep(0.2)
    stop_pull(process)
    raise harness.Failure(f"pull ({label}) does not listen: "
                          f"{outcome(process.returncode)} "
                          f"{log_errors(label)} {stderr_tail(label)}")


def stop_pull(process) -> str:
    if process.poll() is None:
        process.send_signal(signal.SIGTERM)
        try:
            process.wait(timeout=30)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()
            return "killed by the test (did not stop on SIGTERM in 30 s)"
    return outcome(process.returncode)


def case_stalled_replicas(name: str) -> dict:
    harness.SERVER.reset()
    harness.BINSRV.reset()
    for _ in range(ARGS.serve_mib // 8):
        harness.SERVER.sql(rows_statement(8, MIB))
    settings = {"checkpoint_size": "1M"}
    must_fetch(f"{name}-fill", **settings)
    stored = storage_facts()
    pull = start_pull(name, **settings)
    sessions = []
    result = {"stored_mib": stored["storage_mib"]}

    def stall(first_id: int):
        for index in range(ARGS.replicas):
            client = dos.logged_in(PULL_PORT)
            if client is None:
                break
            # a small receive buffer, and nothing is ever read
            client.sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
            client.send(dos.dump_packet(blocking=True,
                                        server_id=first_id + index))
            sessions.append(client)
        time.sleep(5)
        return proc_status(pull.pid)

    try:
        time.sleep(2)
        before = proc_status(pull.pid)
        watch = Watch(pull.pid, step=0.2)
        started = time.monotonic()
        during = stall(30_000)
        result["seconds_to_connect"] = round(
            time.monotonic() - started - 5, 1)
        served = listener.streams_binlogs("127.0.0.1", PULL_PORT)
        stalled = len(sessions)
        for client in sessions:
            client.close()
        del sessions[:]
        time.sleep(5)
        after = proc_status(pull.pid)
        # the same again: is the memory of the first wave used again?
        second = stall(40_000)
        memory = watch.stop()
    finally:
        for client in sessions:
            client.close()
        result["pull_stop"] = stop_pull(pull)
    count = max(1, stalled)
    growth = (max(during.get("VmRSS", 0), memory["peak_rss_mib"] * 1024) -
              before.get("VmRSS", 0)) / 1024
    result.update(
        replicas_stalled=stalled,
        rss_before_mib=round(before.get("VmRSS", 0) / 1024, 1),
        rss_during_mib=round(during.get("VmRSS", 0) / 1024, 1),
        peak_rss_mib=memory["peak_rss_mib"],
        rss_after_mib=round(after.get("VmRSS", 0) / 1024, 1),
        rss_second_wave_mib=round(second.get("VmRSS", 0) / 1024, 1),
        rss_mib_per_replica=round(growth / count, 3),
        threads_before=before.get("Threads"),
        threads_during=during.get("Threads"),
        vm_size_during_mib=round(during.get("VmSize", 0) / 1024),
        served_meanwhile=served, pull_alive=bool(during),
        errors=log_errors(name))
    problems = []
    if not during:
        problems.append(f"pull died ({result['pull_stop']})")
    if stalled < ARGS.replicas:
        problems.append(f"only {stalled} of {ARGS.replicas} replicas "
                        "could log in")
    if result["rss_mib_per_replica"] > REPLICA_MIB_LIMIT:
        problems.append(
            f"a stalled replica holds {result['rss_mib_per_replica']} MiB "
            f"of RSS ({stalled} replicas: "
            f"{result['rss_during_mib']} MiB)")
    if not served and during:
        problems.append("mysqlbinlog was not served while the replicas "
                        "were stalled")
    harness.BINSRV.reset()
    return finish(name, result, problems,
                  f"{result['rss_mib_per_replica']} MiB of RSS per stalled "
                  f"replica ({stalled} replicas of a storage whose "
                  "largest event is 1 MiB); nothing limits the number of "
                  "replicas")


def case_replica_huge_event(name: str) -> dict:
    size = ARGS.serve_event_mib * MIB
    settings = {"checkpoint_size": "1M"}
    prepare([rows_statement(1, 16)], **settings)
    filled = attacked("fetch", f"{name}-fill",
                      {"attack": "huge_event", "end": "eof", "size": size,
                       "bytes": size + MIB}, timeout=600, **settings)
    result = {"event_mib": ARGS.serve_event_mib,
              "fill": filled["outcome"],
              "fill_peak_rss_mib": filled["peak_rss_mib"],
              "memory_cap_mib": ARGS.memory_cap_mib}
    if filled["outcome"] != "exit 0":
        harness.BINSRV.reset()
        return finish(name, result, [
            f"the storage could not be filled: {filled['outcome']} "
            f"{filled['errors']}"])

    def request(count: int) -> dict:
        """'count' replicas request the binlog at once and read what they
        get."""
        before = proc_status(pull.pid)
        watch = Watch(pull.pid, step=0.02)
        clients = []
        received = [0] * count

        def read(index: int, client):
            client.sock.settimeout(60)
            try:
                while received[index] < size:
                    piece = client.sock.recv(MIB)
                    if not piece:
                        break
                    received[index] += len(piece)
            except OSError:
                pass

        for index in range(count):
            client = dos.logged_in(PULL_PORT)
            if client is not None:
                clients.append(client)
        started = time.monotonic()
        threads = []
        for index, client in enumerate(clients):
            client.send(dos.dump_packet(blocking=False,
                                        server_id=31_000 + index))
            threads.append(threading.Thread(target=read,
                                            args=(index, client),
                                            daemon=True))
            threads[-1].start()
        for thread in threads:
            thread.join(timeout=90)
        seconds = round(time.monotonic() - started, 1)
        memory = watch.stop()
        for client in clients:
            client.close()
        time.sleep(2)
        after = proc_status(pull.pid)
        top = max(memory["peak_rss_mib"], memory["vm_hwm_mib"])
        return {"replicas": len(clients), "seconds": seconds,
                "received_mib": round(max(received) / MIB, 1),
                "rss_before_mib": round(before.get("VmRSS", 0) / 1024, 1),
                "peak_rss_mib": top,
                "rss_after_mib": round(after.get("VmRSS", 0) / 1024, 1),
                "peak_rss_per_event_byte": round(
                    top / ARGS.serve_event_mib, 2),
                "pull_alive": pull.poll() is None,
                "pull": outcome(pull.poll())}

    # no source: 'pull' keeps trying to connect and serves meanwhile
    pull = start_pull(name, source_port=DEAD_PORT, **settings)
    problems = []
    try:
        time.sleep(1)
        for key, count in (("one_replica", 1),
                           ("replicas_at_once", ARGS.serve_clients)):
            result[key] = request(count)
            if not result[key]["pull_alive"]:
                problems.append(
                    f"{count} replica(s) requesting a binlog with one "
                    f"{ARGS.serve_event_mib} MiB event ended pull "
                    f"({result[key]['pull']}; peak RSS "
                    f"{result[key]['peak_rss_mib']} MiB under a "
                    f"{ARGS.memory_cap_mib} MiB cap)")
                break
    finally:
        result["pull_stop"] = stop_pull(pull)
    result["errors"] = log_errors(name, 3, "unable to establish connection")
    if result["pull_stop"] not in ("exit 0", "exit 1") and not problems:
        problems.append(f"pull ended with {result['pull_stop']}: "
                        f"{stderr_tail(name)}")
    single = result["one_replica"]
    if single["peak_rss_per_event_byte"] > HONEST_MULTIPLE_LIMIT:
        problems.append(
            f"serving one event of {ARGS.serve_event_mib} MiB took "
            f"{single['peak_rss_mib']} MiB of RSS")
    many = result.get("replicas_at_once")
    if many and many["pull_alive"] and \
            many["peak_rss_mib"] > GROWTH_LIMIT_MIB + \
            HONEST_MULTIPLE_LIMIT * ARGS.serve_event_mib:
        problems.append(
            f"{many['replicas']} replicas requesting the binlog at once "
            f"made pull use {many['peak_rss_mib']} MiB of RSS for an event "
            f"of {ARGS.serve_event_mib} MiB it cannot send "
            f"({many['peak_rss_per_event_byte']} times the event; "
            f"{single['peak_rss_per_event_byte']} for one replica): "
            "memory is the event times the number of replicas")
    harness.BINSRV.reset()
    return finish(name, result, problems,
                  f"peak RSS is {single['peak_rss_per_event_byte']} times "
                  "the event, whatever the number of replicas")


# ---------------------------------------------------------------------------
# F. log volume
# ---------------------------------------------------------------------------

def case_log_volume(name: str) -> dict:
    result = {}
    problems = []
    settings = {"checkpoint_size": "1M"}
    floods = {
        "one_row_transactions": {"attack": "complete", "end": "eof",
                                 "count": ARGS.log_units},
        "heartbeats": {"attack": "heartbeats", "end": "eof",
                       "count": ARGS.log_units},
    }
    for flood, plan in floods.items():
        prepare([rows_statement(1, 16)], **settings)
        before = storage_facts()["storage_mib"]
        run = attacked("fetch", f"{name}-{flood}", plan, timeout=900,
                       **settings)
        stored = storage_facts()["storage_mib"] - before
        ratio = run.get("log_bytes_per_stream_byte", 0)
        result[flood] = {
            "outcome": run["outcome"], "units": run["units"],
            "seconds": run["seconds"],
            "streamed_mib": run["streamed_mib"], "log_mib": run["log_mib"],
            "stored_mib": round(stored, 2),
            "log_bytes_per_stream_byte": ratio,
            "log_bytes_per_unit": round(run["log_mib"] * MIB /
                                        max(1, run["units"])),
            "errors": run["errors"]}
        problems += [f"{flood}: {problem}" for problem in died_badly(run)]
        if ratio > LOG_RATIO_LIMIT:
            problems.append(
                f"{flood}: {ratio} bytes of log per byte streamed at level "
                f"'info' ({run['log_mib']} MiB of log, "
                f"{round(stored, 2)} MiB stored, for {run['streamed_mib']} "
                "MiB)")
    harness.BINSRV.reset()
    return finish(name, result, problems,
                  "the log grows more slowly than the stream")


CASES = {
    "unfinished_fetch": case_unfinished_fetch,
    "unfinished_checkpoints": case_unfinished_checkpoints,
    "unfinished_pull": case_unfinished_pull,
    "unfinished_pull_dropped": case_unfinished_pull_dropped,
    "overrun_declared": case_overrun_declared,
    "honest_large_transaction": case_honest_large_transaction,
    "complete_no_checkpoints": case_complete_no_checkpoints,
    "complete_checkpoints": case_complete_checkpoints,
    "tiny_transactions": case_tiny_transactions,
    "tiny_events": case_tiny_events,
    "gtid_gaps": case_gtid_gaps,
    "huge_event": case_huge_event,
    "huge_event_declared": case_huge_event_declared,
    "rotation_flood": case_rotation_flood,
    "sequence_exhaustion": case_sequence_exhaustion,
    "stalled_replicas": case_stalled_replicas,
    "replica_huge_event": case_replica_huge_event,
    "log_volume": case_log_volume,
}


def free_mib(path: Path) -> int:
    usage = shutil.disk_usage(path)
    return usage.free // MIB


def main():
    # pylint: disable-next=global-statement
    global ARGS, PULL_PORT, DEAD_PORT, PROXY_PORT
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--mysql-basedir", type=Path, required=True)
    parser.add_argument("--binsrv", type=Path, required=True)
    parser.add_argument("--workdir", type=Path, required=True)
    parser.add_argument("--port", type=int, default=23900)
    parser.add_argument("--mode", choices=["position", "gtid"],
                        default="position")
    parser.add_argument("--rewrite", action="store_true",
                        help="GTID mode (always rewrite mode): rewrite "
                             "file_size 64K instead of --file-size")
    parser.add_argument("--file-size", default="1G",
                        help="rewrite file_size of the memory cases")
    parser.add_argument("--cases", nargs="*", choices=list(CASES))
    parser.add_argument("--memory-cap-mib", type=int, default=3072,
                        help="address-space limit of every binlog_server")
    parser.add_argument("--unfinished-cap-mib", type=int, default=1536,
                        help="the limit in the 'unfinished' cases")
    parser.add_argument("--stream-cap-mib", type=int, default=2560,
                        help="the most the proxy writes in one case")
    parser.add_argument("--checkpoint-stream-mib", type=int, default=384)
    parser.add_argument("--honest-mib", type=int, default=300)
    parser.add_argument("--complete-mib", type=int, default=320)
    parser.add_argument("--tiny-transactions", type=int, default=50_000)
    parser.add_argument("--tiny-events", type=int, default=200_000)
    parser.add_argument("--gap-transactions", type=int, default=1200)
    parser.add_argument("--gaps-file-size", default="4K")
    parser.add_argument("--huge-event-mib", type=int, nargs="*",
                        default=[64, 256],
                        help="1100 shows what happens above 1 GiB")
    parser.add_argument("--rotations", type=int, nargs="*",
                        default=[300, 1000],
                        help="5000 or 20000 take minutes")
    parser.add_argument("--replicas", type=int, default=300)
    parser.add_argument("--serve-mib", type=int, default=64)
    parser.add_argument("--serve-event-mib", type=int, default=256)
    parser.add_argument("--serve-clients", type=int, default=8)
    parser.add_argument("--log-units", type=int, default=20_000)
    parser.add_argument("--min-free-mib", type=int, default=20 * 1024,
                        help="refuse to start with less free disk")
    args = parser.parse_args()
    ARGS = args
    args.mysql_basedir = args.mysql_basedir.expanduser()
    args.workdir = args.workdir.expanduser().resolve()
    args.workdir.mkdir(parents=True, exist_ok=True)
    if free_mib(args.workdir) < args.min_free_mib:
        parser.error(f"less than {args.min_free_mib} MiB free under "
                     f"{args.workdir}")
    gtid_mode = args.mode == "gtid"
    if args.rewrite:
        if not gtid_mode:
            parser.error("--rewrite needs --mode gtid")
        args.file_size = "64K"
    if args.cases is None:
        args.cases = list(CASES)
    PULL_PORT = args.port + 2
    DEAD_PORT = args.port + 3
    PROXY_PORT = args.port + 4
    listener.MYSQLBINLOG = args.mysql_basedir / "bin" / "mysqlbinlog"
    # one binlog on the source, whatever the size of a transaction
    harness.MAX_BINLOG_SIZE = 1 << 30
    harness.SERVER = harness.MySQLServer(args.mysql_basedir,
                                         args.workdir / "mysql", args.port,
                                         gtid_mode, server_id=1)
    harness.BINSRV = harness.BinlogServer(args.binsrv.expanduser(),
                                          args.workdir / "binsrv", args.port,
                                          gtid_mode)
    results = []
    started = time.monotonic()
    harness.SERVER.start()
    try:
        harness.SERVER.sql("SET GLOBAL sync_binlog = 0, "
                           "innodb_flush_log_at_trx_commit = 2")
        for name in args.cases:
            case_started = time.monotonic()
            try:
                result = CASES[name](name)
            except (harness.Failure, subprocess.SubprocessError,
                    OSError, RuntimeError) as failure:
                result = {"case": name, "ok": False,
                          "why": harness.describe_failure(failure, 600)}
            result["case_seconds"] = round(time.monotonic() - case_started, 1)
            result["free_disk_mib"] = free_mib(args.workdir)
            print(json.dumps(result), flush=True)
            results.append(result)
            # the large files of a case go when it ends
            try:
                harness.BINSRV.reset()
                for path in harness.BINSRV.workdir.glob("*.log"):
                    path.unlink(missing_ok=True)
            except OSError:
                pass
    finally:
        harness.SERVER.stop()
        shutil.rmtree(args.workdir / "mysql" / "data", ignore_errors=True)
        shutil.rmtree(harness.BINSRV.storage, ignore_errors=True)
    failures = [result for result in results if not result.get("ok")]
    print(json.dumps({"cases": len(results), "failures": len(failures),
                      "passed": not failures,
                      "seconds": round(time.monotonic() - started)}),
          flush=True)
    (args.workdir / "result.json").write_text(json.dumps(results, indent=2))
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
