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

"""What the 'pull' listener gives away at login, and what one statement of a
logged-in client can do to it.

Runs 'pull' on a small storage and talks to its listener with the raw client
of listener_fuzz_test.py. Cases:

- 'user_name_probe': a client greeting with 32 random bytes as its
  caching_sha2_password response, once with the configured user name and
  once with another name, before any client has logged in and again after
  one successful login. The first reply must be of the same kind for both
  names (ticket 67: after a login the configured name gets ERR 1045 at once,
  any other name 'perform full authentication', so one connection tells
  whether a name is the replication user).
- 'long_statement': a logged-in client sends COM_QUERY with 'SET' followed
  by 1,000 to 1,000,000 spaces, then 'SET @master_heartbeat_period=' with a
  long run of digits. After each, 'pull' must still run and serve a new
  login (ticket 72: the statement is matched with std::regex, which recurses
  per character; 100,000 spaces end 'pull' with a segmentation fault).

A JSON line per case, then a summary; exit 1 if any case failed. Example:

  PYTHONPATH=~/ws/qa/pylib python3 listener_login_test.py \\
    --mysql-basedir ~/ws/dist/mysql-8.4.8-linux-glibc2.28-x86_64-minimal \\
    --binsrv ~/ws/percona-binlog-server-build-debug_gcc14/binlog_server \\
    --workdir ~/ws/qa/listener-login
"""

import argparse
import json
import os
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import fetch_crash_test as harness  # noqa: E402
import listener_fuzz_test as listener  # noqa: E402

WORKLOAD = [{"rows": [200, 2000], "ddl": True}]
OTHER_USER = "somebody_else"
STATEMENTS = (
    [(f"SET and {count} spaces", b"SET" + b" " * count)
     for count in (1_000, 20_000, 100_000, 1_000_000)] +
    [(f"a heartbeat period of {count} digits",
      b"SET @master_heartbeat_period=" + b"1" * count)
     for count in (100_000, 1_000_000)])


def start_pull(binsrv, pull_port: int) -> subprocess.Popen:
    binsrv.write_config()
    config = json.loads(binsrv.config_path.read_text())
    config["replication_source"] = {
        "port": pull_port, "server_id": 142,
        "server_uuid": "00000000-0000-0000-0000-00000000008e",
        "read_timeout": 10, "write_timeout": 10,
        "require_secure_transport": False,
        "auth_rsa_private_key": str(listener.AUTH_RSA_PRIVATE_KEY),
        "authentication": {"user": listener.PULL_USER,
                           "password": listener.PULL_PASSWORD,
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
            return pull
        except OSError:
            if pull.poll() is not None:
                raise harness.Failure("pull exited before it listened\n" +
                                      binsrv.log_tail())
            time.sleep(0.2)
    pull.kill()
    raise harness.Failure("pull did not start listening in 30 s")


def stop_pull(pull: subprocess.Popen):
    if pull.poll() is None:
        pull.send_signal(signal.SIGTERM)
        try:
            pull.wait(timeout=60)
        except subprocess.TimeoutExpired:
            pull.kill()
            pull.wait()


def logs_in(pull_port: int) -> bool:
    client = listener.Listener("127.0.0.1", pull_port, timeout=10.0)
    try:
        return client.handshake()
    finally:
        client.close()


def first_reply(pull_port: int, user: str) -> str:
    """The kind of the first server packet after a client greeting that
    carries a random scramble."""
    client = listener.Listener("127.0.0.1", pull_port, timeout=10.0)
    try:
        if client.server_salt() is None:
            return "no server greeting"
        client.send(listener.frame(1, listener.greeting_payload(
            listener.BASE_CAPABILITIES, user.encode(), os.urandom(32),
            b"caching_sha2_password")))
        read = client.read_frame()
        if read is None:
            return "connection closed"
        body = read[1]
        if body[:1] == b"\xff":
            return f"ERR {int.from_bytes(body[1:3], 'little')}"
        if body == b"\x01\x04":
            return "perform full authentication"
        return "bytes " + body[:4].hex()
    finally:
        client.close()


def case_user_name_probe(pull_port: int) -> dict:
    def probes() -> dict:
        return {"configured_user": first_reply(pull_port, listener.PULL_USER),
                "other_user": first_reply(pull_port, OTHER_USER)}

    before = probes()
    logged_in = logs_in(pull_port)
    after = probes()
    problems = []
    if not logged_in:
        problems.append("the configured user could not log in")
    for when, replies in (("before any login", before),
                          ("after one login", after)):
        if replies["configured_user"] != replies["other_user"]:
            problems.append(
                f"{when}, a wrong scramble is answered with "
                f"'{replies['configured_user']}' for the configured user "
                f"name and '{replies['other_user']}' for another name")
    result = {"case": "user_name_probe", "before_any_login": before,
              "after_one_login": after, "ok": not problems}
    if problems:
        result["why"] = "; ".join(problems)
    return result


def case_long_statement(pull: subprocess.Popen, pull_port: int) -> dict:
    attempts = []
    problem = None
    for label, statement in STATEMENTS:
        client = listener.Listener("127.0.0.1", pull_port, timeout=30.0)
        attempt = {"statement": label}
        try:
            if not client.handshake():
                problem = f"could not log in before sending {label}"
                break
            client.send(listener.frame(
                0, bytes([listener.COM_QUERY]) + statement))
            read = client.read_frame()
            attempt["reply"] = (
                "none, connection closed" if read is None else
                "ERR" if read[1][:1] == b"\xff" else
                "OK" if read[1][:1] == b"\x00" else read[1][:4].hex())
        finally:
            client.close()
        time.sleep(1.0)
        attempt["pull_alive"] = pull.poll() is None
        attempts.append(attempt)
        if not attempt["pull_alive"]:
            attempt["pull_exit"] = pull.returncode
            how = (f"signal {-pull.returncode} "
                   f"({signal.Signals(-pull.returncode).name})"
                   if pull.returncode < 0 else f"exit {pull.returncode}")
            problem = f"pull ended ({how}) after a COM_QUERY of {label}"
            break
        if not logs_in(pull_port):
            problem = f"no login possible after a COM_QUERY of {label}"
            break
    result = {"case": "long_statement", "attempts": attempts,
              "ok": problem is None}
    if problem:
        result["why"] = problem
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--mysql-basedir", type=Path, required=True)
    parser.add_argument("--binsrv", type=Path, required=True)
    parser.add_argument("--workdir", type=Path, required=True)
    parser.add_argument("--port", type=int, default=23660)
    parser.add_argument("--mode", choices=["gtid", "position"],
                        default="position")
    args = parser.parse_args()
    args.workdir.mkdir(parents=True, exist_ok=True)
    gtid_mode = args.mode == "gtid"
    harness.SERVER = harness.MySQLServer(args.mysql_basedir.expanduser(),
                                         args.workdir / "mysql", args.port,
                                         gtid_mode, server_id=1)
    binsrv = harness.BINSRV = harness.BinlogServer(
        args.binsrv.expanduser(), args.workdir / "binsrv", args.port,
        gtid_mode)
    pull_port = args.port + 2
    results = []
    pull = None
    harness.SERVER.start()
    try:
        machine = harness.FetchCrashMachine()
        machine.start_from_scratch()
        machine.run_workload(WORKLOAD)
        binsrv.checkpoint_size = "1"
        machine.fetch_to_the_end()
        # the long statement goes last: it may end pull
        for name in ("user_name_probe", "long_statement"):
            try:
                if pull is None or pull.poll() is not None:
                    pull = start_pull(binsrv, pull_port)
                result = (case_user_name_probe(pull_port)
                          if name == "user_name_probe"
                          else case_long_statement(pull, pull_port))
            except (harness.Failure, subprocess.SubprocessError,
                    OSError) as failure:
                result = {"case": name, "ok": False,
                          "why": harness.describe_failure(failure, 500)}
            print(json.dumps(result), flush=True)
            results.append(result)
    finally:
        if pull is not None:
            stop_pull(pull)
        harness.SERVER.stop()
    failures = [result for result in results if not result.get("ok")]
    print(json.dumps({"cases": len(results), "failures": len(failures),
                      "passed": not failures}), flush=True)
    (args.workdir / "result.json").write_text(json.dumps(results, indent=2))
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
