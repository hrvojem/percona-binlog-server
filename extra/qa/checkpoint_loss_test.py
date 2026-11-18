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

"""How much does a crash of 'pull' / 'fetch' lose at each checkpoint setting?

'storage.checkpoint_size' and 'storage.checkpoint_interval' make the storage
record complete transactions before the current binlog ends. For each
setting (none, checkpoint_size 64K / 1M / 16M, checkpoint_interval 1s) and
on the file storage or S3 (--storage), this measures what is not recorded
when the process dies, and what the setting costs.

'pull/kill/<setting>': the source commits a steady stream of transactions
(--rate-mb per second, 10 to 100 KB each, all in one binlog) into an empty
storage through a running 'pull', which gets SIGKILL at a random moment
(between --min-seconds and --duration). Measured per repetition:

- committed: the source's binlog size at the kill (the last transaction
  boundary within the file size read just before the signal);
- recorded: the size in the binlog's metadata in the storage (what the next
  start keeps), and the size of the stored data;
- the loss: committed - recorded, in bytes and in seconds of workload (the
  time since the source's binlog was as large as what is recorded);
- recovery: the next 'fetch' must exit 0 and leave an exact copy;
- on S3: the bytes uploaded in PUT requests for the binlog object, counted
  by an s3_fault_proxy.py this harness starts in front of --s3-endpoint
  (every checkpoint uploads the whole binlog again, ticket 17, PBS-82).

'pull/term/<setting>' is the same with SIGTERM, for comparison (the loss is
what the source committed up to the signal and 'pull' did not record before
it exited; 'pull' must exit 0).

'fetch/kill/<setting>' and 'fetch/term/<setting>': the source holds
--fetch-mb of such transactions; a complete 'fetch' gives the time the copy
takes (and the upload volume), then 'fetch' is stopped at a random moment
of the copy. Here the source is ahead of 'fetch' by design, so the loss is
received - recorded, where received is the end of the last transaction that
'fetch' logged before the signal.

A case passes when every repetition recovers to an exact copy, the storage
holds prefixes ending at transaction boundaries, and the largest loss is
within the bound of its setting:

- checkpoint_size S: S + the largest transaction + what the source
  committed in the last second before the signal. A flush starts at the
  first transaction boundary S bytes after the previous one, so up to S
  plus one transaction can be in memory; the second covers what 'pull' had
  not received yet or was still writing ('fetch': S + two transactions, the
  second one for the log line that had not been written);
- checkpoint_interval T: what the source committed in the last T + 1
  seconds before the signal, plus the largest transaction (a flush starts
  with the first event that arrives T after the previous flush);
- none: no bound, the loss is the whole current binlog by design; it is
  recorded, not judged.

The bounds assume that the storage keeps up with the source: a flush runs
in the loop that receives the events, so when flushes take longer than the
data takes to arrive (small checkpoints on S3, where each one uploads the
whole binlog; a busy disk), 'pull' falls behind and the loss is its backlog.
A run whose stored data is shorter than the size in the metadata (which the
next start cannot repair) also records whether that size ends a
transaction and whether it stays ("data_shorter_than_recorded").

One JSON line per case with "case", "ok" and "why" on failure, then a
summary (and a table on stderr); '<workdir>/result.json'; exit 1 if any
case failed. The defaults take 4 to 7 minutes on the file storage and about
8 on S3 (--repetitions 5, --term-repetitions 1, signals 2 to 8 s into a
2 MB/s workload, an 8 MB binlog for 'fetch'); with them checkpoint_size 16M
never takes effect, as no run commits 16 MB. A thorough run (--repetitions
20 --term-repetitions 5 --duration 20 --fetch-mb 32) is estimated at 45
minutes on the file storage and a few hours on S3, where the uploads of
small checkpoints grow with the square of the binlog size. Uses the helpers
of fetch_crash_test.py. Example:

  PYTHONPATH=~/ws/qa/pylib python3 checkpoint_loss_test.py \\
    --mysql-basedir ~/ws/dist/mysql-8.4.8-linux-glibc2.28-x86_64-minimal \\
    --binsrv ~/ws/percona-binlog-server-build-debug_gcc14/binlog_server \\
    --workdir ~/ws/qa/checkpoint-loss --storage s3 \\
    --s3-endpoint 127.0.0.1:9000 --s3-bucket pbs-mtr \\
    --s3-bucket-dir ~/ws/s3/data/pbs-mtr
"""

import argparse
import json
import os
import random
import shutil
import signal
import statistics
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import fetch_crash_test as harness  # noqa: E402

MEGABYTE = 1024 * 1024
UNITS = {"K": 1024, "M": MEGABYTE, "G": 1024 * MEGABYTE}
# (checkpoint_size, checkpoint_interval)
SETTINGS = {"none": (None, None), "size_64K": ("64K", None),
            "size_1M": ("1M", None), "size_16M": ("16M", None),
            "interval_1s": (None, "1s")}
TRANSACTION_BYTES = (10_000, 100_000)
# what the bounds allow for data on its way from the source
LATENCY_SECONDS = 1.0
# 'pull' notices SIGTERM when its read from the source returns (ticket 43, PBS-116)
READ_TIMEOUT = 5
SAMPLE_SECONDS = 0.02
# the sample key of the MTR suite, for the listener of 'pull'
AUTH_RSA_PRIVATE_KEY = (Path(__file__).resolve().parents[2] / "mtr" /
                        "binlog_streaming" / "std_data" /
                        "caching_sha2_rsa_priv.pem")
BINLOG = harness.BINLOG_BASE_NAME + ".000001"


def size_bytes(text: str) -> int:
    return int(text[:-1]) * UNITS[text[-1]] if text[-1] in UNITS else \
        int(text)


class Writer:
    """Commits transactions of random sizes through one mysql client, at
    'rate' bytes per second (as fast as the server takes them without one)
    until stopped or until 'limit' bytes were sent."""

    def __init__(self, server, rng, rate: "float | None",
                 limit: "int | None" = None):
        self.rng = rng
        self.rate = rate
        self.limit = limit
        self.halt = threading.Event()
        self.client = subprocess.Popen(
            [str(server.basedir / "bin" / "mysql"), "--no-defaults",
             f"--socket={server.socket}", "-uroot", "-N", "-B",
             "--unbuffered", "--max-allowed-packet=64M"],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE)
        # the pace starts when the client is connected
        self.client.stdin.write(b"SELECT 'ready';\n")
        self.client.stdin.flush()
        if b"ready" not in self.client.stdout.readline():
            raise RuntimeError("the workload client did not connect: " +
                               self.client.stderr.read().decode(
                                   errors="replace")[-300:])
        self.thread = threading.Thread(target=self._run)
        self.thread.start()

    def _run(self):
        due = time.monotonic()
        sent = 0
        while not self.halt.is_set() and \
                (self.limit is None or sent < self.limit):
            size = self.rng.randint(*TRANSACTION_BYTES)
            try:
                self.client.stdin.write(
                    "INSERT INTO qa.t (payload) VALUES "
                    f"(REPEAT('x', {size}));\n".encode())
                self.client.stdin.flush()
            except OSError:
                return
            sent += size
            if self.rate is not None:
                # a steady rate: time lost is not made up for with a burst
                due = max(due, time.monotonic()) + size / self.rate
                ahead = due - time.monotonic()
                if ahead > 0:
                    self.halt.wait(ahead)

    def stop(self) -> None:
        """Stops sending and waits for the statements already sent."""
        self.halt.set()
        self.finish()

    def finish(self) -> None:
        self.thread.join()
        try:
            self.client.stdin.close()
        except OSError:
            pass
        errors = self.client.stderr.read().decode(errors="replace")
        if self.client.wait(timeout=300) != 0:
            raise RuntimeError(f"the workload failed: {errors[-300:]}")


class Sampler:
    """The size of the source's binlog over time."""

    def __init__(self, path: Path):
        self.path = path
        self.samples = []
        self.halt = threading.Event()
        self.thread = threading.Thread(target=self._run)
        self.thread.start()

    def _run(self):
        while not self.halt.is_set():
            try:
                self.samples.append((time.monotonic(),
                                     self.path.stat().st_size))
            except OSError:
                pass
            self.halt.wait(SAMPLE_SECONDS)

    def stop(self) -> None:
        self.halt.set()
        self.thread.join()

    def size_at(self, moment: float) -> int:
        """The size of the binlog at 'moment' (0 before the first sample)."""
        size = 0
        for sampled_at, sampled in self.samples:
            if sampled_at > moment:
                break
            size = sampled
        return size

    def when(self, size: int) -> "float | None":
        """When the binlog was first seen to hold more than 'size' bytes."""
        for moment, sampled in self.samples:
            if sampled > size:
                return moment
        return None


class Proxy:
    """An s3_fault_proxy.py of this run, for its traffic counters."""

    def __init__(self, listen: str, backend: str):
        self.control = f"http://{listen}/__faults__"
        self.process = subprocess.Popen(
            [sys.executable,
             str(Path(__file__).resolve().parent / "s3_fault_proxy.py"),
             "--listen", listen, "--backend", backend],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        deadline = time.monotonic() + 30
        while True:
            try:
                self.traffic()
                return
            except (urllib.error.URLError, OSError):
                if self.process.poll() is not None or \
                        time.monotonic() > deadline:
                    self.stop()
                    raise RuntimeError(
                        f"s3_fault_proxy.py did not start on {listen}")
                time.sleep(0.2)

    def reset(self) -> None:
        request = urllib.request.Request(
            self.control, method="PUT",
            data=json.dumps({"rules": [], "reset_traffic": True}).encode())
        with urllib.request.urlopen(request, timeout=10) as response:
            response.read()

    def traffic(self) -> dict:
        with urllib.request.urlopen(self.control, timeout=10) as response:
            return json.loads(response.read())["traffic"]

    def stop(self) -> None:
        if self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=20)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait()


class Lab:
    def __init__(self, args, proxy):
        self.args = args
        self.proxy = proxy
        self.rng = random.Random(args.seed)
        self.machine = harness.FetchCrashMachine()
        self.fetch_source = None
        self.complete_fetches = {}

    # -- binlog_server ------------------------------------------------------

    def start(self, operation: str) -> subprocess.Popen:
        binsrv = harness.BINSRV
        binsrv.write_config()
        config = json.loads(binsrv.config_path.read_text())
        config["replication_source"]["port"] = self.args.pull_port
        config["replication_source"]["auth_rsa_private_key"] = \
            str(AUTH_RSA_PRIVATE_KEY)
        # the '[end_of_transaction]' line of every received transaction is
        # how the harness counts them; since PBS-118 it is a debug message
        config["logger"]["level"] = "debug"
        binsrv.config_path.write_text(json.dumps(config, indent=2))
        return subprocess.Popen(
            [str(binsrv.binary), operation, str(binsrv.config_path)],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            env=dict(os.environ, AWS_EC2_METADATA_DISABLED="true"))

    @staticmethod
    def log_count(text: str) -> int:
        log = harness.BINSRV.log_path
        if not log.exists():
            return 0
        return sum(text in line for line in
                   log.read_text(errors="replace").splitlines())

    def wait_connected(self, process) -> None:
        deadline = time.monotonic() + 60
        while self.log_count("switched to replication") == 0:
            if process.poll() is not None or time.monotonic() > deadline:
                if process.poll() is None:
                    process.kill()
                process.wait()
                raise harness.Failure(
                    "binlog_server did not connect to the source\n" +
                    harness.BINSRV.log_tail())
            time.sleep(0.01)

    def stop(self, process, signal_number) -> "int | str":
        process.send_signal(signal_number)
        try:
            return process.wait(timeout=READ_TIMEOUT + 60)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()
            return "timeout"

    def reset_storage(self, setting: str) -> None:
        binsrv = harness.BINSRV
        binsrv.reset()
        self.machine.steps = []
        binsrv.checkpoint_size, binsrv.checkpoint_interval = SETTINGS[setting]
        binsrv.read_timeout = READ_TIMEOUT
        if self.proxy is not None:
            self.proxy.reset()

    # -- measurements -------------------------------------------------------

    @staticmethod
    def recorded() -> dict:
        binsrv = harness.BINSRV
        data = binsrv.storage / BINLOG
        metadata = binsrv.storage / (BINLOG + ".json")
        size = 0
        if metadata.exists():
            try:
                size = json.loads(metadata.read_text())["size"]
            except (ValueError, KeyError):
                size = 0
        return {"recorded": size,
                "data_bytes": data.stat().st_size if data.exists() else 0}

    def uploads(self) -> dict:
        if self.proxy is None:
            return {}
        puts = self.proxy.traffic().get("binlog_puts", {})
        return {"binlog_put_requests": puts.get("requests", 0),
                "binlog_put_bytes": puts.get("request_bytes", 0)}

    @staticmethod
    def transaction_ends(source: bytes) -> list:
        """The offset after each transaction of the binlog."""
        events = harness.parse_events(source)
        starts = [offset for offset, type_code, _ in events
                  if type_code in harness.TRANSACTION_START_EVENTS]
        return starts[1:] + [len(source)] if starts else []

    @staticmethod
    def bound(setting: str, largest: int, arrived) -> "int | None":
        """The largest acceptable loss; 'arrived(seconds)' is how many bytes
        the operation was given in the last 'seconds' before the signal."""
        checkpoint_size, checkpoint_interval = SETTINGS[setting]
        if checkpoint_size is not None:
            return int(size_bytes(checkpoint_size) + largest +
                       arrived(LATENCY_SECONDS))
        if checkpoint_interval is not None:
            seconds = int(checkpoint_interval.rstrip("s"))
            return int(arrived(seconds + LATENCY_SECONDS) + largest)
        return None

    def check_and_recover(self, run: dict, ends: list) -> None:
        """The storage must hold prefixes ending at transaction boundaries,
        and the next 'fetch' must leave an exact copy."""
        if run["data_bytes"] < run["recorded"]:
            # less data than the metadata records cannot be repaired by the
            # next start: is it an older version of the object or a cut-off
            # one, and does it stay?
            time.sleep(3)
            run["data_shorter_than_recorded"] = {
                "data_bytes_ends_a_transaction": run["data_bytes"] in ends,
                "data_bytes_3s_later": self.recorded()["data_bytes"]}
        try:
            self.machine.storage_holds_prefixes()
        except harness.Failure as failure:
            run["storage_error"] = harness.describe_failure(failure, 300)
        started = time.monotonic()
        try:
            self.machine.fetch_to_the_end()
            run["recovered"] = True
        except harness.Failure as failure:
            run["recovered"] = False
            run["recovery_error"] = harness.describe_failure(failure, 400)
        run["recovery_seconds"] = round(time.monotonic() - started, 1)

    # -- one repetition -----------------------------------------------------

    def pull_run(self, setting: str, signal_number) -> dict:
        args = self.args
        server = harness.SERVER
        server.reset()
        self.reset_storage(setting)
        process = self.start("pull")
        writer = sampler = None
        try:
            self.wait_connected(process)
            sampler = Sampler(server.binlog_path(BINLOG))
            started = time.monotonic()
            writer = Writer(server, self.rng, args.rate_mb * MEGABYTE)
            delay = self.rng.uniform(args.min_seconds, args.duration)
            time.sleep(delay)
            if process.poll() is not None:
                raise harness.Failure(
                    f"pull exited with {process.returncode} during the "
                    f"workload\n{harness.BINSRV.log_tail()}")
            source_size = server.binlog_path(BINLOG).stat().st_size
            at_signal = time.monotonic()
            code = self.stop(process, signal_number)
            stopped = time.monotonic()
        finally:
            if process.poll() is None:
                process.kill()
                process.wait()
            if sampler is not None:
                sampler.stop()
            if writer is not None:
                writer.stop()
        if self.proxy is not None:
            # a PUT that was sent in full may still be on its way
            time.sleep(0.5)
        run = {"seconds": round(at_signal - started, 2), "exit": code,
               "seconds_to_stop": round(stopped - at_signal, 2)}
        run.update(self.recorded())
        run.update(self.uploads())
        source = server.binlog_as_sent(BINLOG)
        ends = [4] + self.transaction_ends(source)
        committed = max(end for end in ends if end <= source_size)
        largest = max(after - before for before, after in zip(ends, ends[1:]))
        rate = committed / (at_signal - started)
        loss = max(0, committed - run["recorded"])
        since = sampler.when(run["recorded"])
        run.update({
            "committed": committed, "loss": loss,
            "loss_seconds": round(max(0.0, at_signal - since), 2)
            if loss and since is not None else 0.0,
            "rate_mb": round(rate / MEGABYTE, 2),
            "largest_transaction": largest,
            "bound": self.bound(
                setting, largest, lambda seconds: source_size -
                sampler.size_at(at_signal - seconds))})
        self.check_and_recover(run, ends)
        return run

    def prepare_fetch_source(self) -> None:
        """The source for the 'fetch' cases: --fetch-mb of transactions."""
        if self.fetch_source is not None:
            return
        server = harness.SERVER
        server.reset()
        Writer(server, self.rng, None,
               self.args.fetch_mb * MEGABYTE).finish()
        source = server.binlog_as_sent(BINLOG)
        ends = self.transaction_ends(source)
        self.fetch_source = {
            "bytes": len(source), "ends": ends,
            "largest": max(after - before
                           for before, after in zip([4] + ends, ends))}

    def complete_fetch(self, setting: str) -> dict:
        """A whole 'fetch': how long the copy takes and what it uploads."""
        self.reset_storage(setting)
        process = self.start("fetch")
        self.wait_connected(process)
        connected = time.monotonic()
        try:
            code = process.wait(timeout=1800)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()
            raise harness.Failure("a complete fetch took over 1800 s")
        result = {"copy_seconds": round(time.monotonic() - connected, 2),
                  "exit": code, "bytes": self.fetch_source["bytes"],
                  "transactions_logged":
                  self.log_count("[end_of_transaction]")}
        result.update(self.uploads())
        self.machine.check_complete_copy()
        if code != 0:
            raise harness.Failure(f"a complete fetch exited with {code}\n"
                                  f"{harness.BINSRV.log_tail()}")
        return result

    def fetch_run(self, setting: str, signal_number, complete: dict) -> dict:
        self.reset_storage(setting)
        source = self.fetch_source
        process = self.start("fetch")
        try:
            self.wait_connected(process)
            connected = time.monotonic()
            time.sleep(self.rng.uniform(0.05, 0.9) *
                       complete["copy_seconds"])
            finished_before = process.poll() is not None
            at_signal = time.monotonic()
            code = process.returncode if finished_before else \
                self.stop(process, signal_number)
        finally:
            if process.poll() is None:
                process.kill()
                process.wait()
        if self.proxy is not None:
            time.sleep(0.5)
        run = {"seconds": round(at_signal - connected, 2), "exit": code,
               "finished_before_the_signal": finished_before}
        run.update(self.recorded())
        run.update(self.uploads())
        logged = self.log_count("[end_of_transaction]")
        # the log names every transaction 'fetch' received
        received = source["ends"][logged - 1] if 0 < logged <= len(
            source["ends"]) and complete["transactions_logged"] == len(
            source["ends"]) else None
        rate = (received or 0) / max(at_signal - connected, 0.001)
        loss = max(0, (received or 0) - run["recorded"])
        run.update({
            "received": received, "committed": source["bytes"], "loss": loss,
            "loss_seconds": round(loss / rate, 2) if rate else 0.0,
            "rate_mb": round(rate / MEGABYTE, 2),
            "largest_transaction": source["largest"],
            # 'fetch' is not waiting for the source: the allowance for
            # data on its way is one more transaction, for the log line
            # that was not written yet
            "bound": self.bound(
                setting, source["largest"], lambda seconds:
                source["largest"] if seconds == LATENCY_SECONDS else
                rate * seconds)})
        self.check_and_recover(run, source["ends"])
        return run

    # -- one case -----------------------------------------------------------

    def case(self, operation: str, how: str, setting: str) -> dict:
        args = self.args
        signal_number = signal.SIGKILL if how == "kill" else signal.SIGTERM
        repetitions = args.repetitions if how == "kill" else \
            args.term_repetitions
        result = {"case": f"{operation}/{how}/{setting}",
                  "storage": args.storage, "runs": []}
        complete = None
        if operation == "fetch":
            self.prepare_fetch_source()
            if setting not in self.complete_fetches:
                self.complete_fetches[setting] = self.complete_fetch(setting)
            complete = self.complete_fetches[setting]
            result["complete_fetch"] = complete
        for _ in range(repetitions):
            run = self.pull_run(setting, signal_number) \
                if operation == "pull" else \
                self.fetch_run(setting, signal_number, complete)
            result["runs"].append(run)
        runs = result["runs"]
        losses = [run["loss"] for run in runs]
        seconds = [run["loss_seconds"] for run in runs]
        result.update({
            "loss_median": int(statistics.median(losses)),
            "loss_max": max(losses),
            "loss_seconds_median": round(statistics.median(seconds), 2),
            "loss_seconds_max": max(seconds),
            "committed_median": int(statistics.median(
                run["received"] or 0 if operation == "fetch" else
                run["committed"] for run in runs)),
            "bound": max((run["bound"] for run in runs
                          if run["bound"] is not None), default=None),
            "recovered": sum(run["recovered"] for run in runs),
            "repetitions": len(runs)})
        if self.proxy is not None:
            result["binlog_put_bytes_median"] = int(statistics.median(
                run["binlog_put_bytes"] for run in runs))
            result["upload_factor_median"] = round(statistics.median(
                run["binlog_put_bytes"] / max(run["recorded"], 1)
                for run in runs), 1)
        problems = []
        over = [run for run in runs
                if run["bound"] is not None and run["loss"] > run["bound"]]
        if over:
            worst = max(over, key=lambda run: run["loss"] - run["bound"])
            problems.append(
                f"{len(over)} of {len(runs)} runs lost more than the bound: "
                f"{worst['loss']} bytes ({worst['loss_seconds']} s of "
                f"workload) against {worst['bound']}")
        if result["recovered"] != len(runs):
            problems.append(
                f"{len(runs) - result['recovered']} of {len(runs)} runs did "
                "not recover to an exact copy: " + next(
                    run["recovery_error"] for run in runs
                    if not run["recovered"])[:300])
        damaged = [run["storage_error"] for run in runs
                   if "storage_error" in run]
        if damaged:
            problems.append(f"the storage after the signal: {damaged[0]}")
        if how == "term" and operation == "pull" and \
                any(run["exit"] != 0 for run in runs):
            problems.append("pull did not exit 0 after SIGTERM: " +
                            str([run["exit"] for run in runs]))
        result["ok"] = not problems
        if problems:
            result["why"] = "; ".join(problems)
        return result


def table(results: list) -> str:
    lines = [f"{'case':<28}{'loss median':>12}{'max':>10}{'s median':>9}"
             f"{'s max':>7}{'bound':>10}{'recovered':>10}{'PUT bytes':>12}"
             f"{'x':>7}  ok"]
    for result in results:
        if "runs" not in result:
            continue
        lines.append(
            f"{result['case']:<28}{result['loss_median']:>12}"
            f"{result['loss_max']:>10}{result['loss_seconds_median']:>9}"
            f"{result['loss_seconds_max']:>7}"
            f"{str(result['bound'] or '-'):>10}"
            f"{result['recovered']:>8}/{result['repetitions']}"
            f"{str(result.get('binlog_put_bytes_median', '-')):>12}"
            f"{str(result.get('upload_factor_median', '-')):>7}  "
            f"{'yes' if result['ok'] else 'NO'}")
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--mysql-basedir", type=Path, required=True)
    parser.add_argument("--binsrv", type=Path, required=True)
    parser.add_argument("--workdir", type=Path, required=True)
    parser.add_argument("--port", type=int, default=23760)
    parser.add_argument("--mode", choices=["gtid", "position"],
                        default="position")
    parser.add_argument("--pull-port", type=int,
                        help="the listener of 'pull' (default: --port + 2)")
    parser.add_argument("--storage", choices=["file", "s3"], default="file")
    parser.add_argument("--s3-endpoint", default="127.0.0.1:9000")
    parser.add_argument("--s3-bucket", default="pbs-mtr")
    parser.add_argument("--s3-bucket-dir", type=Path)
    parser.add_argument("--s3-access-key", default="pbsaccesskey")
    parser.add_argument("--s3-secret-key", default="pbs/secret+key=1")
    parser.add_argument("--s3-proxy-port", type=int,
                        help="where this run's s3_fault_proxy.py listens "
                             "(default: --port + 9)")
    parser.add_argument("--no-s3-proxy", action="store_true",
                        help="connect to --s3-endpoint directly: no upload "
                             "volumes")
    parser.add_argument("--settings", default=",".join(SETTINGS),
                        help="of " + ", ".join(SETTINGS))
    parser.add_argument("--operations", default="pull,fetch")
    parser.add_argument("--repetitions", type=int, default=5,
                        help="SIGKILL runs per setting")
    parser.add_argument("--term-repetitions", type=int, default=1,
                        help="SIGTERM runs per setting")
    parser.add_argument("--rate-mb", type=float, default=2.0,
                        help="what the source commits per second for 'pull'")
    parser.add_argument("--min-seconds", type=float, default=2.0)
    parser.add_argument("--duration", type=float, default=8.0,
                        help="the latest moment of the signal for 'pull'")
    parser.add_argument("--fetch-mb", type=int, default=8,
                        help="the size of the binlog for 'fetch'")
    parser.add_argument("--seed", type=int,
                        default=random.SystemRandom().randrange(1 << 32))
    args = parser.parse_args()
    args.workdir = args.workdir.expanduser().resolve()
    args.workdir.mkdir(parents=True, exist_ok=True)
    args.pull_port = args.pull_port or args.port + 2
    settings = [name for name in args.settings.split(",") if name]
    for name in settings:
        if name not in SETTINGS:
            parser.error(f"unknown setting {name}")
    gtid_mode = args.mode == "gtid"
    # one binlog for the whole workload
    harness.MAX_BINLOG_SIZE = 1024 * MEGABYTE
    harness.FETCH_TIMEOUT_SECONDS = 1800

    proxy = None
    s3 = None
    results = []
    try:
        if args.storage == "s3":
            if args.s3_bucket_dir is None:
                parser.error("--storage s3 needs --s3-bucket-dir")
            endpoint = args.s3_endpoint
            if not args.no_s3_proxy:
                endpoint = f"127.0.0.1:{args.s3_proxy_port or args.port + 9}"
                proxy = Proxy(endpoint, args.s3_endpoint)
            s3 = harness.S3Storage(
                endpoint, args.s3_bucket, args.s3_bucket_dir.expanduser(),
                args.s3_access_key, args.s3_secret_key,
                f"checkpoint-loss-{args.mode}-{os.getpid()}")
        harness.SERVER = harness.MySQLServer(
            args.mysql_basedir.expanduser(), args.workdir / "mysql",
            args.port, gtid_mode, server_id=1)
        harness.BINSRV = harness.BinlogServer(
            args.binsrv.expanduser(), args.workdir / "binsrv", args.port,
            gtid_mode, s3)
        print(json.dumps({"seed": args.seed, "storage": args.storage,
                          "mode": args.mode}), flush=True)
        harness.SERVER.start()
        # the source's own durability is not under test, and its fsyncs make
        # the rate of the workload depend on what else uses the disk
        harness.SERVER.sql("SET GLOBAL sync_binlog = 0; "
                           "SET GLOBAL innodb_flush_log_at_trx_commit = 2")
        lab = Lab(args, proxy)
        cases = [(operation, how, setting)
                 for operation in args.operations.split(",") if operation
                 for how, count in (("kill", args.repetitions),
                                    ("term", args.term_repetitions))
                 if count > 0
                 for setting in settings]
        for operation, how, setting in cases:
            try:
                result = lab.case(operation, how, setting)
            except (harness.Failure, RuntimeError,
                    subprocess.SubprocessError, OSError) as failure:
                result = {"case": f"{operation}/{how}/{setting}",
                          "storage": args.storage, "ok": False,
                          "why": harness.describe_failure(failure, 500)}
            print(json.dumps(result), flush=True)
            results.append(result)
    finally:
        if harness.SERVER is not None:
            harness.SERVER.stop()
        if proxy is not None:
            proxy.stop()
        if s3 is not None and harness.BINSRV is not None:
            shutil.rmtree(harness.BINSRV.storage, ignore_errors=True)
    print(table(results), file=sys.stderr, flush=True)
    failures = [result for result in results if not result.get("ok")]
    print(json.dumps({"cases": len(results), "failures": len(failures),
                      "passed": not failures}), flush=True)
    (args.workdir / "result.json").write_text(json.dumps(results, indent=2))
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
