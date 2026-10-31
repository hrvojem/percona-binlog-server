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

"""Checks TLS on the replication source listener of 'pull' (PBS-31).

A source gets a workload with two rotations; then 'pull' is started with
one listener configuration after another (the storage is kept, so every
start resumes), and clients connect in the TLS mode each case needs. With
an 'ssl' section the listener uses the test certificate of the MTR suite
(std_data/rs_cert.pem, signed by rs_ca_cert.pem). Cases (one JSON line
each):

  no_ssl_plain_client       without an 'ssl' section, a client with
                            --ssl-mode=DISABLED copies every closed binlog
                            exactly (the baseline);
  ssl_plain_client          with an 'ssl' section, a client with
                            --ssl-mode=DISABLED still copies them exactly;
  ssl_default_client        with an 'ssl' section, a client with the default
                            --ssl-mode (PREFERRED, what most clients use)
                            copies them exactly;
  ssl_required_client       ... with --ssl-mode=REQUIRED (a TLS session);
  ssl_verify_ca_client      ... with --ssl-mode=VERIFY_CA and the test CA;
  tls_without_rsa_key       with an 'ssl' section and no auth_rsa_private_key,
                            a --ssl-mode=REQUIRED client logs in (the password
                            travels inside TLS, so full authentication needs
                            no RSA key) and copies them exactly;
  plain_refused_when_required
                            with require_secure_transport, a
                            --ssl-mode=DISABLED client is refused with an
                            error (MySQL: ER_SECURE_TRANSPORT_REQUIRED, 3159)
                            and pull keeps serving;
  tls_accepted_when_required
                            ... while a --ssl-mode=REQUIRED client copies them
                            exactly;
  replica_over_tls          a MySQL replica with SOURCE_SSL=1 replicates from
                            pull to the source's data;
  bad_cert_path, bad_key_path, mismatched_key, cert_not_pem
                            with a missing certificate, a missing key, a key
                            that is not the certificate's, or a certificate
                            file that is not PEM, pull exits with an error
                            within STARTUP_SECONDS and says which file is
                            wrong.

Position mode. Uses the helpers of fetch_crash_test.py and
pull_replica_test.py. Example:

  PYTHONPATH=~/ws/qa/pylib python3 pull_tls_test.py \\
    --mysql-basedir ~/ws/dist/mysql-8.4.8-linux-glibc2.28-x86_64-minimal \\
    --binsrv ~/ws/percona-binlog-server-build-debug_gcc14/binlog_server \\
    --workdir ~/ws/qa/pull-tls
"""

import argparse
import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import fetch_crash_test as harness  # noqa: E402
import pull_replica_test as replication  # noqa: E402

STD_DATA = (Path(__file__).resolve().parents[2] / "mtr" / "binlog_streaming" /
            "std_data")
CERT = STD_DATA / "rs_cert.pem"
KEY = STD_DATA / "rs_priv.pem"
CA = STD_DATA / "rs_ca_cert.pem"
AUTH_RSA_PUBLIC_KEY = STD_DATA / "caching_sha2_rsa_pub.pem"
WORKLOAD = [{"rows": [2000, 500], "ddl": False}]
CLIENT_SECONDS = 60
STARTUP_SECONDS = 15
CATCH_UP_SECONDS = 120
SOURCE_READ_TIMEOUT = 5


def start_pull(binsrv, pull_port: int, ssl: "dict | None",
               require_secure_transport: bool,
               rsa_key: bool) -> subprocess.Popen:
    binsrv.write_config()
    config = json.loads(binsrv.config_path.read_text())
    source = {
        "port": pull_port, "server_id": 142,
        "server_uuid": "00000000-0000-0000-0000-00000000008e",
        "read_timeout": 60, "write_timeout": 60,
        "require_secure_transport": require_secure_transport,
        "authentication": {"user": replication.PULL_USER,
                           "password": replication.PULL_PASSWORD,
                           "plugin": "caching_sha2_password"}}
    if ssl is not None:
        source["ssl"] = ssl
    if rsa_key:
        source["auth_rsa_private_key"] = str(replication.AUTH_RSA_PRIVATE_KEY)
    config["replication_source"] = source
    config["logger"]["level"] = "info"
    binsrv.config_path.write_text(json.dumps(config, indent=2))
    return subprocess.Popen(
        [str(binsrv.binary), "pull", str(binsrv.config_path)],
        stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True,
        env=dict(os.environ, AWS_EC2_METADATA_DISABLED="true"))


def wait_listening(port: int, process: subprocess.Popen) -> bool:
    """Whether 'pull' listens on 'port' within STARTUP_SECONDS (False when
    it exits first)."""
    import socket
    deadline = time.monotonic() + STARTUP_SECONDS
    while time.monotonic() < deadline:
        if process.poll() is not None:
            return False
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=1):
                return True
        except OSError:
            time.sleep(0.3)
    return False


def stop(process: subprocess.Popen) -> None:
    """SIGTERM, which pull handles by shutting down cleanly (a coverage build
    then writes its counters), and SIGKILL only if it does not exit."""
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=60)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()


def copy(args, port: int, first_binlog: str, directory: Path,
         ssl_options: list) -> subprocess.CompletedProcess:
    """mysqlbinlog --raw from 'first_binlog' to the end of the data."""
    shutil.rmtree(directory, ignore_errors=True)
    directory.mkdir(parents=True)
    try:
        return subprocess.run(
            [str(args.mysql_basedir / "bin" / "mysqlbinlog"), "--no-defaults",
             "--read-from-remote-server", "--host=127.0.0.1",
             f"--port={port}", f"--user={replication.PULL_USER}",
             f"--password={replication.PULL_PASSWORD}", "--raw",
             "--to-last-log", f"--result-file={directory}/", *ssl_options,
             first_binlog],
            capture_output=True, text=True, timeout=CLIENT_SECONDS)
    except subprocess.TimeoutExpired as error:
        return subprocess.CompletedProcess(
            error.cmd, 124, "", f"timed out after {CLIENT_SECONDS} s")


def mismatches(source, names: list, directory: Path) -> list:
    result = []
    for name in names:
        path = directory / name
        if not path.exists():
            result.append(f"{name}: missing")
        elif path.read_bytes() != source.binlog_as_sent(name):
            result.append(f"{name}: differs")
    return result


PLAIN = ["--ssl-mode=DISABLED",
         f"--server-public-key-path={AUTH_RSA_PUBLIC_KEY}"]
DEFAULT = [f"--server-public-key-path={AUTH_RSA_PUBLIC_KEY}"]
REQUIRED = ["--ssl-mode=REQUIRED"]
VERIFY_CA = ["--ssl-mode=VERIFY_CA", f"--ssl-ca={CA}"]
SSL = {"cert": str(CERT), "key": str(KEY)}


def run(args) -> list:
    work = args.workdir
    source = harness.MySQLServer(args.mysql_basedir, work / "source",
                                 args.port, False, server_id=1)
    replica = harness.MySQLServer(args.mysql_basedir, work / "replica",
                                  args.port + 10, False, server_id=10)
    harness.SERVER = source
    harness.BINSRV = binsrv = harness.BinlogServer(
        args.binsrv, work / "binsrv", args.port, False)
    # pull exits on SIGTERM only once its read from the idle source times
    # out (ticket 43, PBS-116): a short timeout keeps each stop short
    binsrv.read_timeout = SOURCE_READ_TIMEOUT
    port = args.port + 2
    results = []
    source.start()
    replica.start()
    try:
        machine = harness.FetchCrashMachine()
        machine.start_from_scratch()
        for _ in range(2):
            machine.run_workload(WORKLOAD)
            machine.flush_binary_logs()
        machine.run_workload(WORKLOAD)
        names = source.binlog_names()
        closed = names[:-1]
        first = names[0]

        def copy_case(case: str, ssl, require: bool, rsa_key: bool,
                      options: list) -> None:
            pull = start_pull(binsrv, port, ssl, require, rsa_key)
            try:
                if not wait_listening(port, pull):
                    results.append({"case": case, "ok": False,
                                    "reason": "pull did not start",
                                    "stderr": (pull.stderr.read() or "")
                                    [-300:] if pull.poll() is not None
                                    else ""})
                    return
                outcome = copy(args, port, first, work / "copies" / case,
                               options)
                wrong = mismatches(source, closed, work / "copies" / case)
                results.append({
                    "case": case, "client_exit": outcome.returncode,
                    "client_error": outcome.stderr.strip()[-300:],
                    "wrong": wrong[:3], "pull_alive": pull.poll() is None,
                    "ok": outcome.returncode == 0 and not wrong and
                    pull.poll() is None})
            finally:
                stop(pull)

        copy_case("no_ssl_plain_client", None, False, True, PLAIN)
        copy_case("ssl_plain_client", SSL, False, True, PLAIN)
        copy_case("ssl_default_client", SSL, False, True, DEFAULT)
        copy_case("ssl_required_client", SSL, False, True, REQUIRED)
        copy_case("ssl_verify_ca_client", SSL, False, True, VERIFY_CA)
        copy_case("tls_without_rsa_key", SSL, False, False, REQUIRED)
        copy_case("tls_accepted_when_required", SSL, True, True, REQUIRED)

        # a plain client under require_secure_transport
        pull = start_pull(binsrv, port, SSL, True, True)
        try:
            listening = wait_listening(port, pull)
            outcome = copy(args, port, first, work / "copies" / "refused",
                           PLAIN) if listening else None
            refused_cleanly = (outcome is not None and
                               outcome.returncode != 0 and
                               "ERROR" in outcome.stderr and
                               "Lost connection" not in outcome.stderr)
            after = copy(args, port, first, work / "copies" / "after",
                         REQUIRED) if listening else None
            results.append({
                "case": "plain_refused_when_required",
                "client_exit": outcome.returncode if outcome else None,
                "client_error": outcome.stderr.strip()[-300:]
                if outcome else "pull did not start",
                "secure_transport_error": outcome is not None and (
                    "3159" in outcome.stderr or
                    "insecure transport" in outcome.stderr),
                "tls_client_after": after.returncode if after else None,
                "ok": refused_cleanly and pull.poll() is None})
        finally:
            stop(pull)

        # a replica over TLS
        pull = start_pull(binsrv, port, SSL, False, True)
        try:
            listening = wait_listening(port, pull)
            if listening:
                replica.sql("STOP REPLICA; RESET REPLICA ALL; "
                            "DROP DATABASE IF EXISTS qa; CREATE DATABASE qa;")
                replica.sql(
                    "CHANGE REPLICATION SOURCE TO SOURCE_HOST='127.0.0.1', "
                    f"SOURCE_PORT={port}, "
                    f"SOURCE_USER='{replication.PULL_USER}', "
                    f"SOURCE_PASSWORD='{replication.PULL_PASSWORD}', "
                    f"SOURCE_SSL=1, SOURCE_SSL_CA='{CA}', "
                    "SOURCE_CONNECT_RETRY=2, SOURCE_RETRY_COUNT=3, "
                    f"SOURCE_LOG_FILE='{first}', SOURCE_LOG_POS=4; "
                    "START REPLICA;")
            expected = replication.checksum(source)
            deadline = time.monotonic() + CATCH_UP_SECONDS
            while listening and time.monotonic() < deadline and \
                    replication.checksum(replica) != expected:
                time.sleep(2)
            matched = listening and replication.checksum(replica) == expected
            error = replica.sql(
                "SELECT LAST_ERROR_NUMBER, LAST_ERROR_MESSAGE FROM "
                "performance_schema.replication_connection_status"
            ).strip()[:300] if listening else "pull did not start"
            results.append({"case": "replica_over_tls",
                            "data_matches": matched, "io_error": error,
                            "ok": matched})
            replica.sql("STOP REPLICA")
        finally:
            stop(pull)

        # misconfigured certificates: pull must refuse to start
        bad_dir = work / "bad-certs"
        bad_dir.mkdir(parents=True, exist_ok=True)
        other_key = bad_dir / "other_key.pem"
        subprocess.run(["openssl", "genpkey", "-algorithm", "RSA",
                        "-pkeyopt", "rsa_keygen_bits:2048", "-out",
                        str(other_key)], capture_output=True, check=True)
        not_pem = bad_dir / "not_pem.pem"
        not_pem.write_text("this is not a certificate\n")
        for case, ssl, words in (
                ("bad_cert_path", {"cert": str(bad_dir / "missing.pem"),
                                   "key": str(KEY)}, "certificate"),
                ("bad_key_path", {"cert": str(CERT),
                                  "key": str(bad_dir / "missing.pem")},
                 "private key"),
                ("mismatched_key", {"cert": str(CERT),
                                    "key": str(other_key)}, "private key"),
                ("cert_not_pem", {"cert": str(not_pem), "key": str(KEY)},
                 "certificate")):
            pull = start_pull(binsrv, port, ssl, False, True)
            try:
                exit_code = pull.wait(timeout=STARTUP_SECONDS)
            except subprocess.TimeoutExpired:
                exit_code = None
            stderr = (pull.stderr.read() or "") if exit_code is not None \
                else ""
            log = binsrv.log_path.read_text(errors="replace")[-3000:] \
                if binsrv.log_path.exists() else ""
            says = words in (stderr + log).lower()
            results.append({"case": case, "exit_code": exit_code,
                            "names_the_problem": says,
                            "message": (stderr.strip() or log.strip())
                            [-240:],
                            "ok": exit_code not in (None, 0) and says})
            stop(pull)
    finally:
        source.stop()
        replica.stop()
    return results


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--mysql-basedir", type=Path, required=True)
    parser.add_argument("--binsrv", type=Path, required=True)
    parser.add_argument("--workdir", type=Path, required=True)
    parser.add_argument("--port", type=int, default=23800)
    parser.add_argument("--mode", choices=["position"], default="position")
    args = parser.parse_args()
    args.mysql_basedir = args.mysql_basedir.expanduser()
    args.binsrv = args.binsrv.expanduser()
    args.workdir.mkdir(parents=True, exist_ok=True)
    results = run(args)
    for result in results:
        print(json.dumps(result), flush=True)
    (args.workdir / "results.json").write_text(json.dumps(results, indent=2))
    return 0 if all(result.get("ok") for result in results) else 1


if __name__ == "__main__":
    sys.exit(main())
