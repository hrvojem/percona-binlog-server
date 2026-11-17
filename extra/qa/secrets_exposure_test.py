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

"""Secrets in what 'binlog_server' writes: logs, stdout / stderr, telemetry.

Every operation runs with a configuration full of distinctive secrets, at
log level 'trace' (the most verbose), on success and on the error paths that
tend to print a configuration value or a URI. Afterwards the log file, the
standard output and error of every run and the telemetry directory are
searched for each secret (as given, and key bytes also as hex in either
case). The secrets: the source user's password (and a wrong one the
configuration gives on purpose), the replication source listener's password,
a wrong password a client sends to the listener, the key bytes of the KEK in
the keyring (and of a malformed keyring record), and the S3 access and
secret keys embedded in the storage URI.

Runs (each a JSON line with the secrets it found, by place):
  fetch_encrypted       fetch with an encrypted storage (success)
  fetch_wrong_password  fetch with a wrong source password
  fetch_bad_keyring     fetch with a keyring whose record has key bytes of
                        the wrong length
  pull_logins           pull, then mysqlbinlog logs in with the right and
                        with a wrong password
  list_and_search       list and search_by_timestamp on the storage
  s3_unreachable        fetch to an S3 storage whose endpoint refuses
                        connections, credentials in the URI
  s3_wrong_credentials  fetch to the local S3 server (if one listens on
                        --s3-endpoint) with wrong credentials in the URI
  s3_unencoded_*        fetch with a storage URI whose secret key holds a
                        '/' that is not percent-encoded, in three shapes
                        (the secret starts with '/', only digits come before
                        the '/', the endpoint form); such a URI parses
                        without user info, and the "masked" URI in the log
                        then carries the secret (ticket 64, PBS-159)

Exit 1 if any secret was found. Example:

  PYTHONPATH=~/ws/qa/pylib python3 secrets_exposure_test.py \\
    --mysql-basedir ~/ws/dist/mysql-8.4.8-linux-glibc2.28-x86_64-minimal \\
    --binsrv ~/ws/percona-binlog-server-build-debug_gcc14/binlog_server \\
    --workdir ~/ws/qa/secrets
"""

import argparse
import json
import os
import signal
import socket
import subprocess
import sys
import time
import urllib.parse
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import fetch_crash_test as harness  # noqa: E402
import listener_fuzz_test as listener  # noqa: E402

WORKLOAD = [{"rows": [200], "ddl": True}]
SOURCE_USER = "secrets_rpl"
SECRETS = {
    "source password": "SrcPass-9F2c-Kx71",
    "wrong source password": "WrongSrc-4D8e-Qz55",
    "listener password": "LsnPass-3Ab9-Wq22",
    "wrong client password": "ClientWrong-8Hc1-Pl03",
    "S3 access key": "AKIASECRET77XQ",
    "S3 secret key": "S3Secret-6Tg2-Mn48",
    # the part after the '/' of a secret key that is not percent-encoded
    "unencoded S3 secret key": "RawS3Key-7Vd4-Hx90",
}
KEK_HEX = "5EC7E75EC7E7A11CE0B0B5EC7E75EC7E"
# 31 bytes (62 hex digits) for a 32-byte cipher: a malformed record
BAD_KEY_HEX = ("BADC0DE5EC7E7" * 5)[:62]


def secret_needles() -> dict:
    needles = dict(SECRETS)
    for what, hex_value in (("KEK bytes", KEK_HEX),
                            ("malformed key bytes", BAD_KEY_HEX)):
        needles[what + " (hex)"] = hex_value
        needles[what + " (lower-case hex)"] = hex_value.lower()
    needles["S3 secret key (URL-encoded)"] = urllib.parse.quote(
        SECRETS["S3 secret key"], safe="")
    return needles


def scan(places: dict) -> dict:
    """{place: [secret names found]} for the non-empty ones."""
    found = {}
    for place, text in places.items():
        hits = sorted(what for what, needle in secret_needles().items()
                      if needle and needle in text)
        if hits:
            found[place] = hits
    return found


class Runner:
    def __init__(self, args):
        self.args = args
        self.binsrv = harness.BINSRV
        self.counter = 0

    def config(self, *, password: str = SECRETS["source password"],
               encrypted: bool = False, keyring: "str | None" = None,
               storage: "dict | None" = None,
               listener_port: "int | None" = None) -> Path:
        self.binsrv.write_config()
        config = json.loads(self.binsrv.config_path.read_text())
        config["logger"]["level"] = "trace"
        config["connection"]["user"] = SOURCE_USER
        config["connection"]["password"] = password
        config["replication_source"]["authentication"]["password"] = \
            SECRETS["listener password"]
        if listener_port is not None:
            # pull serves what it has checkpointed: every event at once
            config["storage"]["checkpoint_size"] = "1"
            config["replication_source"].update({
                "port": listener_port,
                "auth_rsa_private_key": str(listener.AUTH_RSA_PRIVATE_KEY),
                "authentication": {"user": listener.PULL_USER,
                                   "password": SECRETS["listener password"],
                                   "plugin": "caching_sha2_password"}})
        if keyring is not None:
            keyring_path = self.binsrv.workdir / "keyring_data.json"
            keyring_path.write_text(keyring)
            config["keyring"] = {"uri": f"file://{keyring_path}"}
        if encrypted:
            config["storage"]["encryption"] = {
                "format": "generic", "kek_id": "secret_kek",
                "cipher": "AES-256-CTR"}
        if storage is not None:
            config["storage"] = storage
        self.counter += 1
        path = self.binsrv.workdir / f"secrets-{self.counter}.json"
        path.write_text(json.dumps(config, indent=2))
        return path

    def run(self, operation: list, config: Path,
            timeout: int = 120) -> "tuple[int, str]":
        try:
            completed = subprocess.run(
                [str(self.binsrv.binary), operation[0], str(config)] +
                operation[1:], capture_output=True, timeout=timeout,
                check=False,
                env=dict(os.environ, AWS_EC2_METADATA_DISABLED="true"))
            return completed.returncode, (
                completed.stdout.decode(errors="replace") +
                completed.stderr.decode(errors="replace"))
        except subprocess.TimeoutExpired as expired:
            output = (expired.stdout or b"") + (expired.stderr or b"")
            return -1, output.decode(errors="replace")

    def outputs(self, name: str, exit_code: int, output: str) -> dict:
        log = (self.binsrv.log_path.read_text(errors="replace")
               if self.binsrv.log_path.exists() else "")
        telemetry = "".join(
            path.read_text(errors="replace")
            for path in self.binsrv.telemetry.glob("*") if path.is_file()) \
            if self.binsrv.telemetry.exists() else ""
        found = scan({"log": log, "stdout/stderr": output,
                      "telemetry": telemetry})
        return {"case": name, "exit": exit_code, "found": found,
                "log_lines": log.count("\n"), "ok": not found}

    def fresh(self):
        harness.BINSRV.reset()
        if self.binsrv.log_path.exists():
            self.binsrv.log_path.unlink()


def keyring_text(bad: bool = False) -> str:
    keys = [{"id": "secret_kek", "cipher": "AES-256-CTR",
             "data_hex": KEK_HEX * 2}]
    if bad:
        # 31 bytes for a 32-byte cipher: the whole keyring is rejected
        keys.append({"id": "broken", "cipher": "AES-256-GCM",
                     "data_hex": BAD_KEY_HEX})
    return json.dumps({"version": 1, "keys": keys})


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--mysql-basedir", type=Path, required=True)
    parser.add_argument("--binsrv", type=Path, required=True)
    parser.add_argument("--workdir", type=Path, required=True)
    parser.add_argument("--port", type=int, default=23620)
    parser.add_argument("--mode", choices=["position"], default="position")
    parser.add_argument("--s3-endpoint", default="127.0.0.1:9000")
    args = parser.parse_args()
    args.mysql_basedir = args.mysql_basedir.expanduser()
    listener.MYSQLBINLOG = args.mysql_basedir / "bin" / "mysqlbinlog"
    args.workdir.mkdir(parents=True, exist_ok=True)
    harness.SERVER = harness.MySQLServer(args.mysql_basedir,
                                         args.workdir / "mysql", args.port,
                                         False, server_id=1)
    harness.BINSRV = harness.BinlogServer(args.binsrv.expanduser(),
                                          args.workdir / "binsrv", args.port,
                                          False)
    results = []
    harness.SERVER.start()
    try:
        machine = harness.FetchCrashMachine()
        machine.start_from_scratch()
        harness.SERVER.sql(
            f"CREATE USER '{SOURCE_USER}'@'%' IDENTIFIED BY "
            f"'{SECRETS['source password']}'; GRANT REPLICATION SLAVE, "
            f"REPLICATION CLIENT ON *.* TO '{SOURCE_USER}'@'%';")
        machine.run_workload(WORKLOAD)
        runner = Runner(args)

        def record(result):
            print(json.dumps(result), flush=True)
            results.append(result)

        runner.fresh()
        code, output = runner.run(["fetch"], runner.config(
            encrypted=True, keyring=keyring_text()))
        record(runner.outputs("fetch_encrypted", code, output))

        runner.fresh()
        code, output = runner.run(["fetch"], runner.config(
            password=SECRETS["wrong source password"]))
        record(runner.outputs("fetch_wrong_password", code, output))

        runner.fresh()
        code, output = runner.run(["fetch"], runner.config(
            encrypted=True, keyring=keyring_text(bad=True)))
        record(runner.outputs("fetch_bad_keyring", code, output))

        # pull on an encrypted storage, two mysqlbinlog logins
        runner.fresh()
        listener_port = args.port + 2
        pull_config = runner.config(encrypted=True, keyring=keyring_text(),
                                    listener_port=listener_port)
        pull = subprocess.Popen(
            [str(runner.binsrv.binary), "pull", str(pull_config)],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            env=dict(os.environ, AWS_EC2_METADATA_DISABLED="true"))
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            try:
                socket.create_connection(("127.0.0.1", listener_port),
                                         timeout=1).close()
                break
            except OSError:
                time.sleep(0.3)
        # pull serves once it has stored the first binlog
        listener.PULL_PASSWORD = SECRETS["listener password"]
        good_login = False
        deadline = time.monotonic() + 30
        while not good_login and time.monotonic() < deadline:
            good_login = listener.streams_binlogs("127.0.0.1", listener_port)
            if not good_login:
                time.sleep(1)
        if not good_login:
            attempt = subprocess.run(
                [str(listener.MYSQLBINLOG), "--no-defaults",
                 "--read-from-remote-server", "--host=127.0.0.1",
                 f"--port={listener_port}", f"--user={listener.PULL_USER}",
                 f"--password={SECRETS['listener password']}",
                 "--default-auth=caching_sha2_password",
                 "--get-server-public-key", "--ssl-mode=DISABLED",
                 "binlog.000001"], capture_output=True, timeout=30,
                check=False)
            client_error = (attempt.stderr.decode(errors="replace")[-300:] +
                            f" (exit {attempt.returncode}, "
                            f"{len(attempt.stdout)} bytes out)")
        listener.PULL_PASSWORD = SECRETS["wrong client password"]
        wrong_login = listener.streams_binlogs("127.0.0.1", listener_port)
        pull.send_signal(signal.SIGTERM)
        try:
            pull_output, _ = pull.communicate(timeout=60)
        except subprocess.TimeoutExpired:
            pull.kill()
            pull_output, _ = pull.communicate()
        result = runner.outputs("pull_logins", pull.returncode,
                                pull_output.decode(errors="replace"))
        result["good_login_served"] = good_login
        result["wrong_login_served"] = wrong_login
        if not good_login:
            result["client_error"] = client_error
        if not good_login and runner.binsrv.log_path.exists():
            result["pull_errors"] = [
                line[-200:] for line in runner.binsrv.log_path.read_text(
                    errors="replace").splitlines()
                if "[  error]" in line or "[warning]" in line][:5]
        result["ok"] = result["ok"] and good_login and not wrong_login
        record(result)

        # list and search on the encrypted storage pull left
        config = runner.config(encrypted=True, keyring=keyring_text())
        code_list, output_list = runner.run(["list"], config)
        code_search, output_search = runner.run(
            ["search_by_timestamp", "2030-01-01 00:00:00"], config)
        record(runner.outputs("list_and_search", code_list or code_search,
                              output_list + output_search))

        quote = lambda text: urllib.parse.quote(text, safe="")
        credentials = (f"{quote(SECRETS['S3 access key'])}:"
                       f"{quote(SECRETS['S3 secret key'])}")
        for name, endpoint in (("s3_unreachable", "127.0.0.1:1"),
                               ("s3_wrong_credentials", args.s3_endpoint)):
            host, _, port = endpoint.partition(":")
            if name == "s3_wrong_credentials":
                try:
                    socket.create_connection((host, int(port)),
                                             timeout=2).close()
                except OSError:
                    record({"case": name, "skipped": "no S3 server at "
                            f"{endpoint}", "ok": True})
                    continue
            runner.fresh()
            storage = {"backend": "s3",
                       "uri": f"http://{credentials}@{endpoint}/pbs-mtr/"
                              "secrets-test",
                       "fs_buffer_directory": str(runner.binsrv.workdir /
                                                  "s3-buffer")}
            code, output = runner.run(["fetch"], runner.config(
                storage=storage), timeout=180)
            record(runner.outputs(name, code, output))

        # a secret key with a '/' that is not percent-encoded (the README
        # asks for %2F, nothing enforces it): the URI parses as host, port
        # and path, without user info
        access = SECRETS["S3 access key"]
        raw = SECRETS["unencoded S3 secret key"]
        for name, uri in (
                ("s3_unencoded_slash_first",
                 f"s3://{access}:/{raw}@bucket.us-east-1/vault"),
                ("s3_unencoded_digits_then_slash",
                 f"s3://{access}:123/{raw}@bucket.us-east-1/vault"),
                ("s3_unencoded_endpoint",
                 f"http://{access}:/{raw}@127.0.0.1:1/pbs-mtr/"
                 "secrets-test")):
            runner.fresh()
            storage = {"backend": "s3", "uri": uri,
                       "fs_buffer_directory": str(runner.binsrv.workdir /
                                                  "s3-buffer")}
            code, output = runner.run(["fetch"], runner.config(
                storage=storage), timeout=180)
            record(runner.outputs(name, code, output))
    finally:
        harness.SERVER.stop()
    failures = [result for result in results if not result.get("ok")]
    print(json.dumps({"cases": len(results), "failures": len(failures),
                      "passed": not failures}), flush=True)
    (args.workdir / "result.json").write_text(json.dumps(results, indent=2))
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
