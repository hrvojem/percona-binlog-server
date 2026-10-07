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

"""Network faults between the Binlog Server and its MySQL source, injected
with Toxiproxy (https://github.com/Shopify/toxiproxy) placed between them.

Part 'fetch': for each fault (the connection cut after N bytes in either
direction, a reset after T ms, a stalled connection, a refused connection,
a stream split into 1-byte pieces, added latency, limited bandwidth):
1. a workload runs on the MySQL server;
2. 'fetch' runs through the faulty proxy: it must exit with 0 or 1, and
   within read_timeout plus a margin when the connection stalls; faults that
   only slow the stream down must not make it fail;
3. the stored binlogs must be prefixes of the server's, ending at
   transaction boundaries (the checks of fetch_crash_test.py);
4. with the fault removed, 'fetch' must exit with 0 and leave exact copies.

Part 'pull': one 'binlog_server pull' runs through the proxy for the whole
test while episodes of workload + outage (reset, cut, stall, proxy
disabled) + workload follow each other. After each episode 'pull' must
still be running, the storage must reach exact copies of the server's
binlogs on its own (it reconnects after 'idle_time'), and its number of
open file descriptors must not keep growing.

Results are printed as JSON lines and collected in '<workdir>/results.json'.
Uses the helpers of fetch_crash_test.py.
"""

import argparse
import json
import os
import signal
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import fetch_crash_test as harness  # noqa: E402

PROXY_NAME = "mysql"
CONNECT_TIMEOUT = 5
READ_TIMEOUT = 8
# how long a stalled or cut fetch may take before it counts as a hang
FETCH_LIMIT_SECONDS = READ_TIMEOUT * 3 + 30
CATCH_UP_SECONDS = 120

WORKLOAD = [{"rows": [3000, 150_000], "ddl": False},
            {"rows": [500, 70_000], "ddl": True},
            {"rows": [400_000], "ddl": False}]


class Toxiproxy:
    def __init__(self, api: str):
        self.api = api.rstrip("/")

    def _call(self, method: str, path: str, body=None):
        request = urllib.request.Request(
            self.api + path, method=method,
            data=None if body is None else json.dumps(body).encode(),
            headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(request, timeout=10) as response:
            text = response.read()
            return json.loads(text) if text else None

    def create(self, listen: str, upstream: str):
        try:
            self._call("DELETE", f"/proxies/{PROXY_NAME}")
        except urllib.error.HTTPError:
            pass
        self._call("POST", "/proxies", {"name": PROXY_NAME, "listen": listen,
                                        "upstream": upstream,
                                        "enabled": True})

    def add_toxic(self, toxic_type: str, stream: str, attributes: dict):
        self._call("POST", f"/proxies/{PROXY_NAME}/toxics",
                   {"name": "fault", "type": toxic_type, "stream": stream,
                    "toxicity": 1.0, "attributes": attributes})

    def set_enabled(self, enabled: bool):
        self._call("POST", f"/proxies/{PROXY_NAME}", {"enabled": enabled})

    def clear(self):
        try:
            self._call("DELETE", f"/proxies/{PROXY_NAME}/toxics/fault")
        except urllib.error.HTTPError:
            pass
        self.set_enabled(True)


# (name, toxic type or "disabled", stream, attributes, expectation)
# expectation: "complete" - must exit with 0; "fail_or_complete" - 0 or 1;
# "fail_in_time" - must exit within FETCH_LIMIT_SECONDS
FETCH_FAULTS = (
    [(f"cut server->client after {n} bytes", "limit_data", "downstream",
      {"bytes": n}, "fail_or_complete")
     for n in (0, 4, 78, 200, 1_000, 5_000, 50_000, 160_000, 400_000)] +
    [(f"cut client->server after {n} bytes", "limit_data", "upstream",
      {"bytes": n}, "fail_or_complete")
     for n in (0, 50, 200, 400)] +
    [(f"reset after {t} ms", "reset_peer", "downstream", {"timeout": t},
      "fail_or_complete") for t in (0, 50, 300, 1_500)] +
    [("stalled connection (no data)", "timeout", "downstream",
      {"timeout": 0}, "fail_in_time"),
     ("stalled connection from the start (upstream)", "timeout", "upstream",
      {"timeout": 0}, "fail_in_time"),
     ("connection refused (proxy disabled)", "disabled", "", {},
      "fail_or_complete"),
     ("stream split into 1-byte pieces", "slicer", "downstream",
      {"average_size": 1, "size_variation": 0, "delay": 0}, "complete"),
     ("200 ms latency with jitter", "latency", "downstream",
      {"latency": 200, "jitter": 100}, "complete"),
     ("bandwidth 200 KB/s", "bandwidth", "downstream", {"rate": 200},
      "complete")])

# outages for the pull part: (name, toxic type or "disabled", stream,
# attributes, seconds the outage lasts)
PULL_OUTAGES = [
    ("reset", "reset_peer", "downstream", {"timeout": 0}, 3),
    ("cut after 100 KB", "limit_data", "downstream", {"bytes": 100_000}, 3),
    ("stall", "timeout", "downstream", {"timeout": 0}, READ_TIMEOUT + 5),
    ("proxy disabled", "disabled", "", {}, 6),
    ("cut after 1 KB", "limit_data", "downstream", {"bytes": 1_000}, 3),
    ("stall upstream", "timeout", "upstream", {"timeout": 0},
     READ_TIMEOUT + 5),
    ("reset after 200 ms", "reset_peer", "downstream", {"timeout": 200}, 3),
]


def apply(proxy: Toxiproxy, toxic_type: str, stream: str, attributes: dict):
    if toxic_type == "disabled":
        proxy.set_enabled(False)
    else:
        proxy.add_toxic(toxic_type, stream, attributes)


def run_fetch_part(proxy: Toxiproxy, results: list):
    for name, toxic_type, stream, attributes, expectation in FETCH_FAULTS:
        result = {"part": "fetch", "fault": name}
        machine = harness.FetchCrashMachine()
        try:
            proxy.clear()
            machine.start_from_scratch()
            machine.run_workload(WORKLOAD)
            machine.fetch_to_the_end()
            for statement in harness.workload_sql(WORKLOAD):
                harness.SERVER.sql(statement)
            apply(proxy, toxic_type, stream, attributes)
            started = time.monotonic()
            process = harness.BINSRV.start_fetch()
            try:
                _, stderr = process.communicate(timeout=FETCH_LIMIT_SECONDS)
            except subprocess.TimeoutExpired:
                process.kill()
                process.communicate()
                raise harness.Failure(
                    f"fetch did not finish in {FETCH_LIMIT_SECONDS} s\n"
                    f"{harness.BINSRV.log_tail()}")
            finally:
                proxy.clear()
            result["exit"] = process.returncode
            result["seconds"] = round(time.monotonic() - started, 1)
            if process.returncode not in (0, 1):
                raise harness.Failure(
                    f"fetch exited with {process.returncode}\n"
                    f"{stderr.decode(errors='replace')[-1500:]}")
            if expectation == "complete" and process.returncode != 0:
                raise harness.Failure(
                    f"fetch failed although the fault only slows the stream "
                    f"down\n{harness.BINSRV.log_tail()}")
            machine.storage_holds_prefixes()
            machine.fetch_to_the_end()
            machine.storage_holds_prefixes()
            result["recovered"] = True
        except Exception as failure:  # every case is reported
            result["failure"] = f"{type(failure).__name__}: {failure}"[:700]
        print(json.dumps(result), flush=True)
        results.append(result)


def open_descriptors(pid: int) -> int:
    return len(os.listdir(f"/proc/{pid}/fd"))


def wait_for_exact_copies(machine) -> float:
    """Waits until the storage holds exact copies of the server's binlogs;
    returns the seconds it took."""
    started = time.monotonic()
    last_failure = None
    while time.monotonic() - started < CATCH_UP_SECONDS:
        try:
            machine.check_complete_copy()
            return time.monotonic() - started
        except (harness.Failure, FileNotFoundError) as failure:
            last_failure = failure
            time.sleep(1)
    raise harness.Failure(f"pull did not catch up in {CATCH_UP_SECONDS} s: "
                          f"{last_failure}")


def run_pull_part(proxy: Toxiproxy, results: list, rounds: int):
    binsrv = harness.BINSRV
    machine = harness.FetchCrashMachine()
    proxy.clear()
    machine.start_from_scratch()
    # without checkpoints pull keeps the binlog being written in memory until
    # it rotates; flushing every transaction makes "caught up" observable
    binsrv.checkpoint_size = "1"
    machine.run_workload(WORKLOAD)
    binsrv.write_config()
    environment = dict(os.environ, AWS_EC2_METADATA_DISABLED="true")
    process = subprocess.Popen(
        [str(binsrv.binary), "pull", str(binsrv.config_path)],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, env=environment)
    try:
        wait_for_exact_copies(machine)
        baseline = open_descriptors(process.pid)
        for round_number in range(rounds):
            for name, toxic_type, stream, attributes, seconds in PULL_OUTAGES:
                result = {"part": "pull", "round": round_number + 1,
                          "fault": name}
                try:
                    machine.run_workload(WORKLOAD[:1])
                    apply(proxy, toxic_type, stream, attributes)
                    machine.run_workload(WORKLOAD[1:])
                    time.sleep(seconds)
                    proxy.clear()
                    if process.poll() is not None:
                        raise harness.Failure(
                            f"pull exited with {process.returncode}\n"
                            f"{binsrv.log_tail()}")
                    result["catch_up_seconds"] = round(
                        wait_for_exact_copies(machine), 1)
                    machine.storage_holds_prefixes()
                    descriptors = open_descriptors(process.pid)
                    result["open_descriptors"] = descriptors
                    if descriptors > baseline + 20:
                        raise harness.Failure(
                            f"pull has {descriptors} open file descriptors, "
                            f"{baseline} at the start")
                except Exception as failure:  # every case is reported
                    proxy.clear()
                    result["failure"] = \
                        f"{type(failure).__name__}: {failure}"[:700]
                print(json.dumps(result), flush=True)
                results.append(result)
                if process.poll() is not None:
                    return
    finally:
        if process.poll() is None:
            process.send_signal(signal.SIGTERM)
            try:
                process.wait(timeout=60)
            except subprocess.TimeoutExpired:
                process.kill()
                results.append({"part": "pull", "fault": "shutdown",
                                "failure": "pull did not stop within 60 s "
                                           "of SIGTERM"})


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--mysql-basedir", type=Path, required=True)
    parser.add_argument("--binsrv", type=Path, required=True)
    parser.add_argument("--workdir", type=Path, required=True)
    parser.add_argument("--mysql-port", type=int, default=23416)
    parser.add_argument("--proxy-port", type=int, default=23417)
    parser.add_argument("--toxiproxy-api", default="http://127.0.0.1:8474")
    parser.add_argument("--mode", choices=["gtid", "position"],
                        default="position")
    parser.add_argument("--parts", default="fetch,pull")
    parser.add_argument("--pull-rounds", type=int, default=2)
    args = parser.parse_args()

    gtid_mode = args.mode == "gtid"
    args.workdir.mkdir(parents=True, exist_ok=True)
    harness.SERVER = harness.MySQLServer(args.mysql_basedir.expanduser(),
                                         args.workdir / "mysql",
                                         args.mysql_port, gtid_mode)
    # binlog_server connects to the proxy
    harness.BINSRV = harness.BinlogServer(args.binsrv.expanduser(),
                                          args.workdir / "binsrv",
                                          args.proxy_port, gtid_mode)
    harness.BINSRV.connect_timeout = CONNECT_TIMEOUT
    harness.BINSRV.read_timeout = READ_TIMEOUT
    harness.BINSRV.idle_time = 2
    proxy = Toxiproxy(args.toxiproxy_api)
    proxy.create(f"127.0.0.1:{args.proxy_port}",
                 f"127.0.0.1:{args.mysql_port}")
    results = []
    harness.SERVER.start()
    try:
        if "fetch" in args.parts:
            run_fetch_part(proxy, results)
        if "pull" in args.parts:
            run_pull_part(proxy, results, args.pull_rounds)
    finally:
        proxy.clear()
        harness.SERVER.stop()
    (args.workdir / "results.json").write_text(json.dumps(results, indent=2))
    failures = [result for result in results if "failure" in result]
    print(f"{len(results)} cases, {len(failures)} failures")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
