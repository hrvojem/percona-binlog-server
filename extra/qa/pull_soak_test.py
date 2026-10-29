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

"""Runs 'binlog_server pull' for a long time under churn and watches it.

For --duration seconds a source gets a steady insert workload (now and then a
larger transaction) with a binlog rotation every ROTATE_EVERY transactions,
while 'pull' (info log level) serves:

  - LONG_LIVED 'mysqlbinlog --raw --stop-never' clients for the whole run;
  - one MySQL replica (heartbeat period 1 s), stopped and started again every
    REPLICA_RESTART_SECONDS;
  - a churn of short clients, one every CHURN_SECONDS: most copy from a
    random closed binlog to the end of the data and exit, some are killed
    (SIGKILL) a few seconds into a --stop-never dump, as a client that dies.

Every SAMPLE_SECONDS it records pull's resident memory, open file
descriptors and threads (/proc), its log size and the storage size. Cases
(one JSON line each, the samples in --workdir/samples.json):

  alive             pull runs to the end;
  memory            resident memory in the last quarter of the run stays
                    within MEMORY_GROWTH (and MEMORY_SLACK_MB) of the
                    second quarter (the first quarter is warm-up);
  file_descriptors  the most open descriptors in the last quarter exceed
                    those of the second quarter by at most FD_SLACK;
  threads           likewise, by at most THREAD_SLACK;
  log               the log grows by at most LOG_MB_PER_HOUR and holds no
                    error line other than the known ones of ticket 42
                    (PBS-115: a disconnect logged at error level, as EOF
                    when reading or as EPIPE, system:32, when a client
                    dies during a dump), which are counted; the most
                    frequent message is reported (ticket 45, PBS-118: at info level
                    every event streamed to every client is logged);
  copies            every long-lived client's copy of every closed binlog,
                    and every completed churn client's copies of the
                    binlogs closed before it started, equal the source's;
  replica           the replica ends with the source's data;
  still_serving     at the end a new client still gets every closed binlog.

Position mode. Uses the helpers of fetch_crash_test.py and
pull_replica_test.py. Example (a short trial):

  PYTHONPATH=~/ws/qa/pylib python3 pull_soak_test.py \\
    --mysql-basedir ~/ws/dist/mysql-8.4.8-linux-glibc2.28-x86_64-minimal \\
    --binsrv ~/ws/percona-binlog-server-build-debug_gcc14/binlog_server \\
    --workdir ~/ws/qa/pull-soak --duration 900
"""

import argparse
import json
import os
import random
import re
import signal
import statistics
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import fetch_crash_test as harness  # noqa: E402
import pull_replica_test as replication  # noqa: E402

AUTH_RSA_PUBLIC_KEY = (Path(__file__).resolve().parents[2] / "mtr" /
                       "binlog_streaming" / "std_data" /
                       "caching_sha2_rsa_pub.pem")
LONG_LIVED = 3
TRANSACTION = [{"rows": [2000, 500], "ddl": False}]
LARGE_TRANSACTION = [{"rows": [200_000], "ddl": False}]
LARGE_EVERY = 100
TRANSACTION_SECONDS = 0.5
ROTATE_EVERY = 60
CHURN_SECONDS = 15
ABORTED_SHARE = 0.3
REPLICA_RESTART_SECONDS = 600
SAMPLE_SECONDS = 60
CATCH_UP_SECONDS = 300
MEMORY_GROWTH = 0.20
MEMORY_SLACK_MB = 32
FD_SLACK = 8
THREAD_SLACK = 4
LOG_MB_PER_HOUR = 50
# the known error lines of ticket 42 (PBS-115): EOF (asio.misc:2) when a
# client disconnects, EPIPE (system:32) when it dies during a dump
KNOWN_ERROR = re.compile(
    r"system error caught in session .*(asio\.misc:2|system:32)")


def start_pull(binsrv, pull_port: int) -> subprocess.Popen:
    """replication.start_pull at info log level, as pull runs in
    production, so that the log growth means something."""
    binsrv.write_config()
    config = json.loads(binsrv.config_path.read_text())
    config["replication_source"] = {
        "port": pull_port, "server_id": 142,
        "server_uuid": "00000000-0000-0000-0000-00000000008e",
        "read_timeout": 60, "write_timeout": 60,
        "auth_rsa_private_key": str(replication.AUTH_RSA_PRIVATE_KEY),
        "authentication": {"user": replication.PULL_USER,
                           "password": replication.PULL_PASSWORD,
                           "plugin": "caching_sha2_password"}}
    binsrv.config_path.write_text(json.dumps(config, indent=2))
    pull = subprocess.Popen(
        [str(binsrv.binary), "pull", str(binsrv.config_path)],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        env=dict(os.environ, AWS_EC2_METADATA_DISABLED="true"))
    time.sleep(3)
    return pull


def start_client(args, port: int, first_binlog: str, directory: Path,
                 server_id: int, stop_never: bool) -> subprocess.Popen:
    directory.mkdir(parents=True, exist_ok=True)
    return subprocess.Popen(
        [str(args.mysql_basedir / "bin" / "mysqlbinlog"), "--no-defaults",
         "--read-from-remote-server", "--host=127.0.0.1", f"--port={port}",
         f"--user={replication.PULL_USER}",
         f"--password={replication.PULL_PASSWORD}",
         f"--server-public-key-path={AUTH_RSA_PUBLIC_KEY}",
         "--ssl-mode=DISABLED", "--raw",
         "--stop-never" if stop_never else "--to-last-log",
         f"--connection-server-id={server_id}",
         f"--result-file={directory}/", first_binlog],
        stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)


def mismatches(source, names: list, directory: Path) -> list:
    result = []
    for name in names:
        copy = directory / name
        if not copy.exists():
            result.append(f"{directory.name}/{name}: missing")
        elif copy.read_bytes() != source.binlog_as_sent(name):
            result.append(f"{directory.name}/{name}: {copy.stat().st_size} "
                          f"bytes, expected "
                          f"{len(source.binlog_as_sent(name))}")
    return result


def proc_sample(pid: int) -> dict:
    """Resident memory (KiB), threads and open descriptors of 'pid'."""
    status = Path(f"/proc/{pid}/status").read_text()
    fields = dict(line.split(":", 1) for line in status.splitlines()
                  if ":" in line)
    return {"rss_kib": int(fields["VmRSS"].split()[0]),
            "threads": int(fields["Threads"]),
            "fds": len(os.listdir(f"/proc/{pid}/fd"))}


def directory_size(path: Path) -> int:
    return sum(item.stat().st_size for item in path.rglob("*")
               if item.is_file())


def quarters(samples: list, key: str) -> tuple:
    """The values of 'key' in the second and in the last quarter of the
    samples."""
    count = len(samples)
    second = [sample[key] for sample in samples[count // 4: count // 2]]
    last = [sample[key] for sample in samples[3 * count // 4:]]
    return second, last


def run(args) -> list:
    work = args.workdir
    rng = random.Random(args.seed)
    source = harness.MySQLServer(args.mysql_basedir, work / "source",
                                 args.port, False, server_id=1)
    replica = harness.MySQLServer(args.mysql_basedir, work / "replica",
                                  args.port + 10, False, server_id=10)
    harness.SERVER = source
    harness.BINSRV = binsrv = harness.BinlogServer(
        args.binsrv, work / "binsrv", args.port, False)
    pull_port = args.port + 2
    cases = ("alive", "memory", "file_descriptors", "threads", "log",
             "copies", "replica", "still_serving")
    results = {case: {"case": case} for case in cases}
    pull = None
    long_lived = []
    churn = []
    samples = []
    source.start()
    replica.start()
    try:
        machine = harness.FetchCrashMachine()
        machine.start_from_scratch()
        binsrv.checkpoint_size = "64K"
        machine.run_workload(TRANSACTION)
        first_binlog = source.binlog_names()[0]
        pull = start_pull(binsrv, pull_port)
        replication.replicate_from_pull(
            replica, pull_port, False,
            f"SOURCE_LOG_FILE='{first_binlog}', SOURCE_LOG_POS=4, "
            "SOURCE_HEARTBEAT_PERIOD=1")
        for index in range(LONG_LIVED):
            long_lived.append(start_client(
                args, pull_port, first_binlog,
                work / "copies" / f"long{index}", 1000 + index, True))

        started = time.monotonic()
        next_sample = started
        next_churn = started + CHURN_SECONDS
        next_replica_restart = started + REPLICA_RESTART_SECONDS
        transactions = 0
        churn_index = 0
        died_at = None
        while time.monotonic() - started < args.duration:
            now = time.monotonic()
            if pull.poll() is not None:
                died_at = round(now - started)
                break
            if now >= next_sample:
                sample = proc_sample(pull.pid)
                sample.update(
                    seconds=round(now - started),
                    log_bytes=(binsrv.log_path.stat().st_size
                               if binsrv.log_path.exists() else 0),
                    storage_bytes=directory_size(binsrv.storage),
                    binlogs=len(source.binlog_names()))
                samples.append(sample)
                next_sample += SAMPLE_SECONDS
            if now >= next_churn:
                closed = source.binlog_names()[:-1]
                if closed:
                    start_name = rng.choice(closed)
                    aborted = rng.random() < ABORTED_SHARE
                    directory = work / "copies" / f"churn{churn_index:05d}"
                    process = start_client(args, pull_port, start_name,
                                           directory, 2000 + churn_index,
                                           aborted)
                    churn.append({
                        "process": process, "directory": directory,
                        "aborted": aborted,
                        "kill_at": now + rng.uniform(1.0, 5.0),
                        "closed": [name for name in closed
                                   if name >= start_name]})
                    churn_index += 1
                next_churn += CHURN_SECONDS
            for client in churn:
                if client["aborted"] and client["process"].poll() is None \
                        and now >= client["kill_at"]:
                    client["process"].kill()
                    client["process"].wait()
            if now >= next_replica_restart:
                replica.sql("STOP REPLICA")
                replica.sql("START REPLICA")
                next_replica_restart += REPLICA_RESTART_SECONDS
            machine.run_workload(LARGE_TRANSACTION
                                 if transactions % LARGE_EVERY ==
                                 LARGE_EVERY - 1 else TRANSACTION)
            transactions += 1
            if transactions % ROTATE_EVERY == 0:
                machine.flush_binary_logs()
            time.sleep(TRANSACTION_SECONDS)
        machine.flush_binary_logs()
        elapsed = round(time.monotonic() - started)
        (work / "samples.json").write_text(json.dumps(samples, indent=1))

        results["alive"].update(seconds=elapsed, died_at_seconds=died_at,
                                transactions=transactions,
                                binlogs=len(source.binlog_names()),
                                churn_clients=churn_index,
                                ok=died_at is None)
        if died_at is not None:
            for case in cases[1:]:
                results[case].update(ok=False, reason="pull exited early")
            return list(results.values())

        for case, key, check in (
                ("memory", "rss_kib",
                 lambda second, last: statistics.median(last) <=
                 statistics.median(second) * (1 + MEMORY_GROWTH) +
                 MEMORY_SLACK_MB * 1024),
                ("file_descriptors", "fds",
                 lambda second, last: max(last) <= max(second) + FD_SLACK),
                ("threads", "threads",
                 lambda second, last: max(last) <= max(second) +
                 THREAD_SLACK)):
            second, last = quarters(samples, key)
            if not second or not last:
                results[case].update(ok=False, reason="too few samples")
                continue
            results[case].update(
                second_quarter=[min(second), statistics.median(second),
                                max(second)],
                last_quarter=[min(last), statistics.median(last), max(last)],
                ok=check(second, last))

        log_text = binsrv.log_path.read_text(errors="replace") \
            if binsrv.log_path.exists() else ""
        error_lines = [line for line in log_text.splitlines()
                       if "[  error]" in line or "[  fatal]" in line]
        unknown_errors = [line for line in error_lines
                          if not KNOWN_ERROR.search(line)]
        log_mb_per_hour = (len(log_text.encode()) / 1e6) / (elapsed / 3600)
        messages = {}
        for line in log_text.splitlines():
            message = re.sub(r"\d+", "N", line.split("] ", 2)[-1])[:80]
            messages[message] = messages.get(message, 0) + 1
        top_message, top_count = max(messages.items(),
                                     key=lambda item: item[1],
                                     default=("", 0))
        results["log"].update(
            mb_per_hour=round(log_mb_per_hour, 1),
            most_frequent=top_message,
            most_frequent_share=round(top_count / max(
                1, len(log_text.splitlines())), 2),
            known_error_lines=len(error_lines) - len(unknown_errors),
            other_error_lines=len(unknown_errors),
            first_other_errors=[line[:200] for line in unknown_errors[:3]],
            ok=log_mb_per_hour <= LOG_MB_PER_HOUR and not unknown_errors)

        # everyone catches up with the closed binlogs
        closed = source.binlog_names()[:-1]
        expected_size = sum(len(source.binlog_as_sent(name))
                            for name in closed)
        long_dirs = [work / "copies" / f"long{index}"
                     for index in range(LONG_LIVED)]
        deadline = time.monotonic() + CATCH_UP_SECONDS
        while time.monotonic() < deadline and any(
                sum((path / name).stat().st_size for name in closed
                    if (path / name).exists()) < expected_size
                for path in long_dirs):
            time.sleep(2)
        for client in churn:
            if not client["aborted"] and client["process"].poll() is None:
                try:
                    client["process"].wait(timeout=CATCH_UP_SECONDS)
                except subprocess.TimeoutExpired:
                    client["process"].kill()
                    client["process"].wait()
        wrong = []
        for path in long_dirs:
            wrong += mismatches(source, closed, path)
        completed = [client for client in churn if not client["aborted"]]
        failed_churn = [client["directory"].name for client in completed
                        if client["process"].returncode != 0]
        for client in completed:
            wrong += mismatches(source, client["closed"], client["directory"])
        exited_long = [index for index, client in enumerate(long_lived)
                       if client.poll() is not None]
        results["copies"].update(
            closed_binlogs=len(closed), completed_churn_clients=len(completed),
            aborted_churn_clients=len(churn) - len(completed),
            churn_clients_failed=failed_churn[:5],
            long_lived_exited=exited_long, wrong=wrong[:5],
            ok=not wrong and not failed_churn and not exited_long)

        expected = replication.checksum(source)
        deadline = time.monotonic() + CATCH_UP_SECONDS
        while time.monotonic() < deadline and \
                replication.checksum(replica) != expected:
            time.sleep(2)
        state = replica.sql(
            "SELECT SERVICE_STATE, COUNT_RECEIVED_HEARTBEATS FROM "
            "performance_schema.replication_connection_status").split()
        matched = replication.checksum(replica) == expected
        results["replica"].update(io_thread_and_heartbeats=state,
                                  data_matches=matched, ok=matched)

        final = start_client(args, pull_port, closed[0],
                             work / "copies" / "final", 9999, False)
        try:
            final.wait(timeout=CATCH_UP_SECONDS)
        except subprocess.TimeoutExpired:
            final.kill()
            final.wait()
        final_wrong = mismatches(source, closed, work / "copies" / "final")
        results["still_serving"].update(exit_code=final.returncode,
                                        wrong=final_wrong[:3],
                                        ok=final.returncode == 0 and
                                        not final_wrong)
    finally:
        for client in long_lived + [item["process"] for item in churn]:
            if client.poll() is None:
                client.kill()
                client.wait()
        if pull is not None and pull.poll() is None:
            pull.kill()
            pull.wait()
        source.stop()
        replica.stop()
    for result in results.values():
        result.setdefault("ok", False)
    return list(results.values())


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--mysql-basedir", type=Path, required=True)
    parser.add_argument("--binsrv", type=Path, required=True)
    parser.add_argument("--workdir", type=Path, required=True)
    parser.add_argument("--port", type=int, default=23600)
    parser.add_argument("--mode", choices=["position"], default="position")
    parser.add_argument("--duration", type=int, default=7200,
                        help="seconds of load")
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument("--keep", action="store_true",
                        help="keep the data directories and copies")
    args = parser.parse_args()
    args.mysql_basedir = args.mysql_basedir.expanduser()
    args.binsrv = args.binsrv.expanduser()
    args.workdir.mkdir(parents=True, exist_ok=True)
    results = run(args)
    for result in results:
        print(json.dumps(result), flush=True)
    (args.workdir / "results.json").write_text(json.dumps(results, indent=2))
    if not args.keep:
        import shutil
        for name in ("source", "replica", "copies"):
            shutil.rmtree(args.workdir / name, ignore_errors=True)
    return 0 if all(result.get("ok") for result in results) else 1


if __name__ == "__main__":
    sys.exit(main())
