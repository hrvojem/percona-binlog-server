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

"""Serves many readers from 'binlog_server pull' under load, then stops it.

A source gets a steady insert workload with a binlog rotation every few
seconds while 'pull' serves --clients 'mysqlbinlog --raw --stop-never'
copies and two MySQL replicas (heartbeat period 1 s). One client is frozen
(SIGSTOP) for the middle of the run, as a reader that stops reading would.
Then 'pull' gets SIGTERM with everyone still connected. Cases (one JSON line
each):

  many_readers  every client's copy of every closed binlog equals the
                source's;
  slow_reader   while one client was frozen the others kept receiving data,
                and once resumed it caught up to an exact copy;
  replicas      both MySQL replicas end with the source's data;
  shutdown      after SIGTERM 'pull' exits 0 within SHUTDOWN_SECONDS, every
                client's connection is closed, and a 'fetch' on the same
                storage then resumes and leaves exact copies (so the
                shutdown left the storage consistent). The source is idle by
                then; 'pull' only exits when its blocking read from the
                source returns, up to connection.read_timeout later (ticket
                43), so the case also reports how long the exit took (waiting
                up to EXIT_WAIT_SECONDS).

Position mode only (pull does not serve GTID auto-position replicas yet).
Uses the helpers of fetch_crash_test.py and pull_replica_test.py. Example:

  PYTHONPATH=~/ws/qa/pylib python3 pull_load_test.py \\
    --mysql-basedir ~/ws/dist/mysql-8.4.8-linux-glibc2.28-x86_64-minimal \\
    --binsrv ~/ws/percona-binlog-server-build-debug_gcc14/binlog_server \\
    --workdir ~/ws/qa/pull-load
"""

import argparse
import json
import os
import signal
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
REPLICAS = 2
# one transaction of a few rows every TRANSACTION_SECONDS, a rotation every
# ROTATE_EVERY transactions
TRANSACTION = [{"rows": [2000, 500, 30_000], "ddl": False}]
TRANSACTION_SECONDS = 0.1
ROTATE_EVERY = 40
CATCH_UP_SECONDS = 180
SHUTDOWN_SECONDS = 10
EXIT_WAIT_SECONDS = 120


def copy_size(directory: Path) -> int:
    return sum(path.stat().st_size for path in directory.iterdir()
               if path.is_file())


def mismatches(source, closed: list, directory: Path) -> list:
    """The closed binlogs whose copy in 'directory' is missing or differs
    from what the source sends."""
    result = []
    for name in closed:
        copy = directory / name
        if not copy.exists():
            result.append(f"{name}: missing")
        elif copy.read_bytes() != source.binlog_as_sent(name):
            result.append(f"{name}: {copy.stat().st_size} bytes, expected "
                          f"{len(source.binlog_as_sent(name))}")
    return result


def start_client(args, index: int, first_binlog: str, pull_port: int,
                 directory: Path) -> subprocess.Popen:
    directory.mkdir(parents=True)
    return subprocess.Popen(
        [str(args.mysql_basedir / "bin" / "mysqlbinlog"), "--no-defaults",
         "--read-from-remote-server", "--host=127.0.0.1",
         f"--port={pull_port}", f"--user={replication.PULL_USER}",
         f"--password={replication.PULL_PASSWORD}",
         f"--server-public-key-path={AUTH_RSA_PUBLIC_KEY}",
         "--ssl-mode=DISABLED", "--raw", "--stop-never",
         f"--connection-server-id={1000 + index}",
         f"--result-file={directory}/", first_binlog],
        stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)


def run(args) -> list:
    work = args.workdir
    source = harness.MySQLServer(args.mysql_basedir, work / "source",
                                 args.port, False, server_id=1)
    replicas = [harness.MySQLServer(args.mysql_basedir,
                                    work / f"replica{index}",
                                    args.port + 10 + index, False,
                                    server_id=10 + index)
                for index in range(REPLICAS)]
    harness.SERVER = source
    harness.BINSRV = binsrv = harness.BinlogServer(
        args.binsrv, work / "binsrv", args.port, False)
    pull_port = args.port + 2
    results = {case: {"case": case} for case in
               ("many_readers", "slow_reader", "replicas", "shutdown")}
    pull = None
    clients = []
    source.start()
    for replica in replicas:
        replica.start()
    try:
        machine = harness.FetchCrashMachine()
        machine.start_from_scratch()
        binsrv.checkpoint_size = "1"
        machine.run_workload(TRANSACTION)
        first_binlog = source.binlog_names()[0]
        pull = replication.start_pull(binsrv, pull_port)
        for replica in replicas:
            replication.replicate_from_pull(
                replica, pull_port, False,
                f"SOURCE_LOG_FILE='{first_binlog}', SOURCE_LOG_POS=4, "
                "SOURCE_HEARTBEAT_PERIOD=1")
        for index in range(args.clients):
            clients.append(start_client(args, index, first_binlog, pull_port,
                                        work / "copies" / f"c{index:02d}"))
        frozen = clients[0]
        frozen_dir = work / "copies" / "c00"
        others = [work / "copies" / f"c{index:02d}"
                  for index in range(1, args.clients)]

        # the workload; client 0 frozen from a quarter to three quarters of
        # the run
        transactions = int(args.duration / TRANSACTION_SECONDS)
        freeze_at, thaw_at = transactions // 4, 3 * transactions // 4
        sizes_at_freeze = sizes_at_thaw = None
        for number in range(transactions):
            if number == freeze_at:
                frozen.send_signal(signal.SIGSTOP)
                sizes_at_freeze = ([copy_size(path) for path in others],
                                   copy_size(frozen_dir))
            if number == thaw_at:
                sizes_at_thaw = ([copy_size(path) for path in others],
                                 copy_size(frozen_dir))
                frozen.send_signal(signal.SIGCONT)
            machine.run_workload(TRANSACTION)
            if number % ROTATE_EVERY == ROTATE_EVERY - 1:
                machine.flush_binary_logs()
            time.sleep(TRANSACTION_SECONDS)
        machine.flush_binary_logs()
        closed = source.binlog_names()[:-1]

        # everyone catches up
        deadline = time.monotonic() + CATCH_UP_SECONDS
        directories = [work / "copies" / f"c{index:02d}"
                       for index in range(args.clients)]
        expected_size = sum(len(source.binlog_as_sent(name))
                            for name in closed)
        while time.monotonic() < deadline and any(
                sum((path / name).stat().st_size
                    for name in closed if (path / name).exists())
                < expected_size for path in directories):
            time.sleep(2)
        wrong = {path.name: problems for path in directories
                 if (problems := mismatches(source, closed, path))}
        exited_early = [index for index, client in enumerate(clients)
                        if client.poll() is not None]
        results["many_readers"].update(
            clients=args.clients, closed_binlogs=len(closed),
            clients_with_wrong_copies=len(wrong),
            clients_exited_early=exited_early,
            ok=not wrong and not exited_early)
        if wrong:
            results["many_readers"]["first_wrong"] = dict(
                list(wrong.items())[:3])

        grew = [after - before for before, after in
                zip(sizes_at_freeze[0], sizes_at_thaw[0])]
        results["slow_reader"].update(
            frozen_seconds=round((thaw_at - freeze_at) * TRANSACTION_SECONDS),
            others_min_growth_bytes=min(grew),
            frozen_growth_bytes=sizes_at_thaw[1] - sizes_at_freeze[1],
            caught_up=frozen_dir.name not in wrong,
            ok=min(grew) > 0 and frozen_dir.name not in wrong)

        expected = replication.checksum(source)
        deadline = time.monotonic() + CATCH_UP_SECONDS
        while time.monotonic() < deadline and any(
                replication.checksum(replica) != expected
                for replica in replicas):
            time.sleep(2)
        states = [replica.sql(
            "SELECT SERVICE_STATE, COUNT_RECEIVED_HEARTBEATS FROM "
            "performance_schema.replication_connection_status").split()
            for replica in replicas]
        matched = [replication.checksum(replica) == expected
                   for replica in replicas]
        results["replicas"].update(replicas=REPLICAS, data_matches=matched,
                                   io_thread_and_heartbeats=states,
                                   ok=all(matched))

        # SIGTERM with every client and replica connected; the clients'
        # exits show when their connections closed
        started = time.monotonic()
        pull.send_signal(signal.SIGTERM)
        clients_closed_after = None
        exit_code = None
        while time.monotonic() - started < EXIT_WAIT_SECONDS:
            if clients_closed_after is None and all(
                    client.poll() is not None for client in clients):
                clients_closed_after = round(time.monotonic() - started, 1)
            if (exit_code := pull.poll()) is not None:
                break
            time.sleep(0.2)
        shutdown_seconds = round(time.monotonic() - started, 1)
        if exit_code is None:
            pull.kill()
            pull.wait()
        pull = None
        deadline = time.monotonic() + SHUTDOWN_SECONDS
        while time.monotonic() < deadline and any(
                client.poll() is None for client in clients):
            time.sleep(0.5)
        if clients_closed_after is None and all(
                client.poll() is not None for client in clients):
            clients_closed_after = round(time.monotonic() - started, 1)
        still_connected = [index for index, client in enumerate(clients)
                           if client.poll() is None]
        for replica in replicas:
            replica.sql("STOP REPLICA")
        # the storage must be one 'fetch' can resume from
        machine.run_workload(TRANSACTION)
        machine.flush_binary_logs()
        # its own log, so that pull's log is kept
        binsrv.log_path = work / "binsrv" / "fetch-after-pull.log"
        binsrv.write_config()
        fetch = subprocess.run(
            [str(binsrv.binary), "fetch", str(binsrv.config_path)],
            capture_output=True, text=True, timeout=600,
            env=dict(os.environ, AWS_EC2_METADATA_DISABLED="true"))
        stored_wrong = mismatches(source, source.binlog_names()[:-1],
                                  binsrv.storage)
        results["shutdown"].update(
            exit_code=exit_code, exit_seconds=shutdown_seconds,
            clients_disconnected_after_seconds=clients_closed_after,
            read_timeout_seconds=binsrv.read_timeout,
            clients_still_connected=still_connected,
            fetch_after_exit=fetch.returncode,
            stored_copies_wrong=stored_wrong[:3],
            ok=(exit_code == 0 and shutdown_seconds <= SHUTDOWN_SECONDS and
                not still_connected and fetch.returncode == 0 and
                not stored_wrong))
    finally:
        for client in clients:
            if client.poll() is None:
                client.send_signal(signal.SIGCONT)
                client.kill()
                client.wait()
        if pull is not None:
            pull.kill()
            pull.wait()
        source.stop()
        for replica in replicas:
            replica.stop()
    for result in results.values():
        result.setdefault("ok", False)
    return list(results.values())


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--mysql-basedir", type=Path, required=True)
    parser.add_argument("--binsrv", type=Path, required=True)
    parser.add_argument("--workdir", type=Path, required=True)
    parser.add_argument("--port", type=int, default=23530)
    parser.add_argument("--clients", type=int, default=30)
    parser.add_argument("--duration", type=int, default=60,
                        help="seconds of workload")
    # position only: pull does not serve GTID auto-position replicas yet
    parser.add_argument("--mode", choices=["position"], default="position")
    args = parser.parse_args()
    args.mysql_basedir = args.mysql_basedir.expanduser()
    args.binsrv = args.binsrv.expanduser()
    args.workdir.mkdir(parents=True, exist_ok=True)
    results = run(args)
    for result in results:
        print(json.dumps(result), flush=True)
    (args.workdir / "results.json").write_text(json.dumps(results, indent=2))
    return 0 if all(result["ok"] for result in results) else 1


if __name__ == "__main__":
    sys.exit(main())
