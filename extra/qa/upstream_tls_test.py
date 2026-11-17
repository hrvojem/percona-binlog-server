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

"""TLS verification of the connections 'fetch' makes: to the MySQL source
and to an HTTPS S3 endpoint, against a man in the middle.

A test CA signs the source's certificate (SAN IP 127.0.0.1). Between 'fetch'
and the source a proxy that understands the MySQL TLS upgrade (the plain
greeting, the client's SSLRequest, then TLS on the same socket) plays one of
two attacks:

- 'mitm': it terminates the client's TLS with an attacker certificate,
  either self-signed or issued by the trusted CA for another host
  ('evil.example'), opens its own TLS to the source and relays. It sees
  the decrypted stream, including the password a caching_sha2_password full
  authentication sends inside TLS (each case logs in as a new user, whose
  hash the source has not cached, so the full authentication happens).
- 'strip': it clears CLIENT_SSL in the source's greeting, so the client
  either goes on without TLS or refuses (the source's password hash is
  cached first, so that a plain login can succeed despite ticket 23, PBS-133).

Each case runs 'fetch' through the proxy with one 'connection.ssl' setting
and records whether it connected and stored data, and whether the proxy
captured the password. Expected, as with MySQL's own client: 'verify_ca'
and 'verify_identity' refuse a self-signed certificate; 'verify_identity'
also refuses the trusted CA's certificate for another host; 'required',
'verify_ca' and 'verify_identity' refuse a connection without TLS. Without
an 'ssl' section, and with 'preferred' or 'required', a MySQL client does not
verify the server, which these cases record as 'accepted' (they pass; the
summary lists them, as they let an attacker on the path act as the source,
see ticket 59, PBS-131).

's3_https_self_signed': an S3 storage at https://127.0.0.1:<port> served by a
TLS endpoint with a self-signed certificate; 'fetch' must not send an HTTP
request over it.

A JSON line per case, then a summary; exit 1 if any case failed. Example:

  PYTHONPATH=~/ws/qa/pylib python3 upstream_tls_test.py \\
    --mysql-basedir ~/ws/dist/mysql-8.4.8-linux-glibc2.28-x86_64-minimal \\
    --binsrv ~/ws/percona-binlog-server-build-debug_gcc14/binlog_server \\
    --workdir ~/ws/qa/upstream-tls
"""

import argparse
import json
import os
import select
import socket
import ssl
import struct
import subprocess
import sys
import threading
import urllib.parse
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import fetch_crash_test as harness  # noqa: E402

WORKLOAD = [{"rows": [200], "ddl": True}]
CLIENT_SSL = 0x800


def openssl(*arguments: str, cwd: Path):
    subprocess.run(["openssl", *arguments], cwd=cwd, check=True,
                   capture_output=True)


def make_certificates(directory: Path) -> dict:
    """A test CA, the source's certificate (SAN IP:127.0.0.1) and two
    attacker certificates: self-signed, and issued by the CA for
    evil.example. Returns their paths."""
    directory.mkdir(parents=True, exist_ok=True)
    openssl("req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "30",
            "-subj", "/CN=PBS QA test CA", "-keyout", "ca-key.pem",
            "-out", "ca.pem", cwd=directory)

    def issued(name: str, subject: str, san: str):
        (directory / f"{name}.ext").write_text(
            f"subjectAltName={san}\nbasicConstraints=CA:FALSE\n")
        openssl("req", "-newkey", "rsa:2048", "-nodes", "-subj", subject,
                "-keyout", f"{name}-key.pem", "-out", f"{name}.csr",
                cwd=directory)
        openssl("x509", "-req", "-in", f"{name}.csr", "-CA", "ca.pem",
                "-CAkey", "ca-key.pem", "-CAcreateserial", "-days", "30",
                "-extfile", f"{name}.ext", "-out", f"{name}.pem",
                cwd=directory)

    issued("server", "/CN=127.0.0.1", "IP:127.0.0.1")
    issued("evil", "/CN=evil.example", "DNS:evil.example")
    openssl("req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "30",
            "-subj", "/CN=127.0.0.1", "-addext", "subjectAltName=IP:127.0.0.1",
            "-keyout", "self-key.pem", "-out", "self.pem", cwd=directory)
    return {name: directory / f"{name}.pem"
            for name in ("ca", "server", "evil", "self")} | {
        name + "_key": directory / f"{name}-key.pem"
        for name in ("server", "evil", "self")}


class TlsSource(harness.MySQLServer):
    """The harness's source with the test CA's server certificate."""

    certificates: dict = {}

    def _mysqld_args(self):
        return super()._mysqld_args() + [
            f"--ssl-ca={self.certificates['ca']}",
            f"--ssl-cert={self.certificates['server']}",
            f"--ssl-key={self.certificates['server_key']}"]


def read_packet(sock) -> "bytes | None":
    header = b""
    while len(header) < 4:
        piece = sock.recv(4 - len(header))
        if not piece:
            return None
        header += piece
    length = header[0] | header[1] << 8 | header[2] << 16
    payload = b""
    while len(payload) < length:
        piece = sock.recv(length - len(payload))
        if not piece:
            return None
        payload += piece
    return header + payload


class MitmProxy:
    """One connection at a time: 'attack' is 'mitm' (with the attacker
    certificate 'cert' / 'key') or 'strip'."""

    def __init__(self, port: int, source_port: int, attack: str,
                 cert: "Path | None" = None, key: "Path | None" = None,
                 password: str = ""):
        self.source_port = source_port
        self.attack = attack
        self.cert, self.key = cert, key
        self.password = password.encode()
        self.events = []
        self.captured_password = False
        self.client_sent_ssl_request = False
        self.client_accepted_certificate = None
        # whether the source asked for a caching_sha2_password full
        # authentication (0x01 0x04), which sends the password inside TLS
        self.full_authentication_requested = False
        self.listener = socket.socket()
        self.listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.listener.bind(("127.0.0.1", port))
        self.listener.listen(4)
        self.thread = threading.Thread(target=self._serve, daemon=True)
        self.thread.start()

    def close(self):
        for action in (lambda: self.listener.shutdown(socket.SHUT_RDWR),
                       self.listener.close):
            try:
                action()
            except OSError:
                pass
        self.thread.join(timeout=5)

    def _serve(self):
        while True:
            try:
                client, _ = self.listener.accept()
            except OSError:
                return
            threading.Thread(target=self._session, args=(client,),
                             daemon=True).start()

    def _session(self, client):
        server = socket.create_connection(("127.0.0.1", self.source_port))
        try:
            greeting = read_packet(server)
            if greeting is None:
                return
            if self.attack == "strip":
                greeting = self._strip_ssl(greeting)
            client.sendall(greeting)
            first = read_packet(client)
            if first is None:
                return
            payload = first[4:]
            capabilities = (struct.unpack_from("<I", payload)[0]
                            if len(payload) >= 4 else 0)
            if len(payload) == 32 and capabilities & CLIENT_SSL:
                self.client_sent_ssl_request = True
                if self.attack != "mitm":
                    self.events.append("client sent an SSLRequest to a "
                                       "greeting without CLIENT_SSL")
                    return
                server.sendall(first)
                upstream = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
                upstream.check_hostname = False
                upstream.verify_mode = ssl.CERT_NONE
                server = upstream.wrap_socket(server)
                downstream = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
                downstream.load_cert_chain(self.cert, self.key)
                try:
                    client = downstream.wrap_socket(client, server_side=True)
                    self.client_accepted_certificate = True
                except (ssl.SSLError, OSError) as error:
                    self.client_accepted_certificate = False
                    self.events.append(f"client refused the attacker "
                                       f"certificate: {error}")
                    return
            else:
                server.sendall(first)
                self._sniff(payload)
            self._relay(client, server)
        except (OSError, ssl.SSLError) as error:
            self.events.append(f"proxy: {error}")
        finally:
            for sock in (client, server):
                try:
                    sock.close()
                except OSError:
                    pass

    @staticmethod
    def _strip_ssl(packet: bytes) -> bytes:
        payload = bytearray(packet[4:])
        end_of_version = payload.index(0, 1)
        # protocol(1) version(NUL) connid(4) salt1(8) filler(1) caps-low(2)
        offset = end_of_version + 1 + 4 + 8 + 1
        low = struct.unpack_from("<H", payload, offset)[0]
        struct.pack_into("<H", payload, offset, low & ~CLIENT_SSL)
        return packet[:4] + bytes(payload)

    def _sniff(self, data: bytes):
        if self.password and self.password in data:
            self.captured_password = True

    def _relay(self, client, server):
        pending = b""
        sockets = [client, server]
        while True:
            readable = [sock for sock in sockets
                        if isinstance(sock, ssl.SSLSocket) and sock.pending()]
            if not readable:
                readable, _, _ = select.select(sockets, [], [], 60)
                if not readable:
                    return
            for sock in readable:
                try:
                    data = sock.recv(65536)
                except ssl.SSLWantReadError:
                    continue
                if not data:
                    return
                if sock is client:
                    pending = (pending + data)[-4096:]
                    self._sniff(pending)
                    server.sendall(data)
                else:
                    if b"\x02\x00\x00" in data[:3] and data[4:6] == b"\x01\x04":
                        self.full_authentication_requested = True
                    client.sendall(data)


class TlsHttpEndpoint:
    """A TLS listener with a given certificate that records whether a client
    completed the handshake and then sent anything (an HTTP request)."""

    def __init__(self, port: int, cert: Path, key: Path):
        self.context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        self.context.load_cert_chain(cert, key)
        self.handshakes = 0
        self.requests = 0
        self.refusals = []
        self.listener = socket.socket()
        self.listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.listener.bind(("127.0.0.1", port))
        self.listener.listen(8)
        self.thread = threading.Thread(target=self._serve, daemon=True)
        self.thread.start()

    def close(self):
        for action in (lambda: self.listener.shutdown(socket.SHUT_RDWR),
                       self.listener.close):
            try:
                action()
            except OSError:
                pass
        self.thread.join(timeout=5)

    def _serve(self):
        while True:
            try:
                client, _ = self.listener.accept()
            except OSError:
                return
            try:
                client.settimeout(10)
                tls = self.context.wrap_socket(client, server_side=True)
                self.handshakes += 1
                if tls.recv(4096):
                    self.requests += 1
                    tls.sendall(b"HTTP/1.1 403 Forbidden\r\n"
                                b"Content-Length: 0\r\n\r\n")
                tls.close()
            except (ssl.SSLError, OSError) as error:
                self.refusals.append(str(error)[:120])
                client.close()


class Cases:
    def __init__(self, args, certificates: dict):
        self.args = args
        self.certificates = certificates
        self.binsrv = harness.BINSRV
        self.port = args.port + 10

    def next_port(self) -> int:
        self.port += 1
        return self.port

    def config(self, port: int, ssl_section: "dict | None",
               storage: "dict | None" = None,
               user: "tuple[str, str] | None" = None) -> Path:
        self.binsrv.write_config()
        config = json.loads(self.binsrv.config_path.read_text())
        config["connection"]["port"] = port
        if user is not None:
            config["connection"]["user"], config["connection"]["password"] = \
                user
        if ssl_section is not None:
            config["connection"]["ssl"] = ssl_section
        if storage is not None:
            config["storage"] = storage
        path = self.binsrv.workdir / "binsrv-tls.json"
        path.write_text(json.dumps(config, indent=2))
        return path

    def fetch(self, config: Path) -> "tuple[int, bool, str]":
        try:
            completed = subprocess.run(
                [str(self.binsrv.binary), "fetch", str(config)],
                capture_output=True, timeout=120, check=False,
                env=dict(os.environ, AWS_EC2_METADATA_DISABLED="true"))
            code = completed.returncode
        except subprocess.TimeoutExpired:
            code = -1
        stored = any(path.name.startswith("binlog.") and
                     path.suffix not in (".json", ".index") and
                     path.stat().st_size > 4
                     for path in self.binsrv.storage.iterdir()) \
            if self.binsrv.storage.exists() else False
        errors = [line.strip()[-200:] for line in
                  self.binsrv.log_path.read_text(errors="replace")
                  .splitlines() if "[  error]" in line][-2:] \
            if self.binsrv.log_path.exists() else []
        return code, stored, " | ".join(errors)

    def fresh(self):
        harness.BINSRV.reset()

    def ssl_section(self, mode: "str | None") -> "dict | None":
        if mode is None:
            return None
        section = {"mode": mode}
        if mode in ("verify_ca", "verify_identity"):
            section["ca"] = str(self.certificates["ca"])
        return section

    def direct(self) -> dict:
        self.fresh()
        code, stored, errors = self.fetch(self.config(
            self.args.port, self.ssl_section("verify_identity")))
        return {"case": "direct_verify_identity", "exit": code,
                "stored": stored, "errors": errors,
                "ok": code == 0 and stored,
                **({} if code == 0 and stored else
                   {"why": "fetch does not connect to the real source with "
                           "verify_identity: the test set-up is wrong"})}

    def mitm(self, certificate: str, mode: "str | None") -> dict:
        self.fresh()
        port = self.next_port()
        # a new user: the source has no cached hash for it, so the login is
        # a caching_sha2_password full authentication, which sends the
        # password inside TLS (FLUSH PRIVILEGES does not clear the cache on
        # 8.4)
        user = (f"tls{port}", f"TlsPass-{port}-Zq9")
        harness.SERVER.sql(
            f"CREATE USER '{user[0]}'@'%' IDENTIFIED BY '{user[1]}'; "
            f"GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO "
            f"'{user[0]}'@'%';")
        proxy = MitmProxy(port, self.args.port, "mitm",
                          self.certificates[certificate],
                          self.certificates[certificate + "_key"], user[1])
        try:
            code, stored, errors = self.fetch(
                self.config(port, self.ssl_section(mode), user=user))
        finally:
            proxy.close()
        accepted = code == 0 and stored
        must_refuse = (mode in ("verify_ca", "verify_identity")
                       if certificate == "self" else
                       mode == "verify_identity")
        result = {"case": f"mitm_{certificate}/{mode or 'default'}",
                  "accepted": accepted, "exit": code,
                  "tls_intercepted": bool(proxy.client_accepted_certificate),
                  "password_captured": proxy.captured_password,
                  "full_authentication_requested":
                      proxy.full_authentication_requested,
                  "errors": errors, "proxy": proxy.events[-2:],
                  "must_refuse": must_refuse,
                  "ok": not (must_refuse and (accepted or
                                              proxy.captured_password))}
        if not result["ok"]:
            result["why"] = (f"ssl mode {mode} accepted a man in the middle "
                             f"with the {certificate} certificate")
        return result

    def strip(self, mode: "str | None") -> dict:
        self.fresh()
        cache_password_hash(self.args.mysql_basedir, self.args.port)
        port = self.next_port()
        proxy = MitmProxy(port, self.args.port, "strip",
                          password=harness.REPLICATION_PASSWORD)
        try:
            code, stored, errors = self.fetch(
                self.config(port, self.ssl_section(mode)))
        finally:
            proxy.close()
        accepted = code == 0 and stored
        must_refuse = mode in ("required", "verify_ca", "verify_identity")
        result = {"case": f"strip/{mode or 'default'}", "accepted": accepted,
                  "exit": code, "password_captured": proxy.captured_password,
                  "errors": errors, "proxy": proxy.events[-2:],
                  "must_refuse": must_refuse,
                  "ok": not (must_refuse and accepted)}
        if not result["ok"]:
            result["why"] = (f"ssl mode {mode} went on without TLS when the "
                             "greeting did not offer it")
        return result

    def s3_https(self) -> dict:
        self.fresh()
        port = self.next_port()
        endpoint = TlsHttpEndpoint(port, self.certificates["self"],
                                   self.certificates["self_key"])
        quote = lambda text: urllib.parse.quote(text, safe="")
        storage = {"backend": "s3",
                   "uri": f"https://{quote('qaaccess')}:{quote('qasecret')}"
                          f"@127.0.0.1:{port}/pbs-tls/prefix",
                   "fs_buffer_directory": str(self.binsrv.workdir /
                                              "s3-buffer")}
        try:
            code, _, errors = self.fetch(self.config(
                self.args.port, None, storage))
        finally:
            endpoint.close()
        result = {"case": "s3_https_self_signed", "exit": code,
                  "tls_handshakes": endpoint.handshakes,
                  "http_requests": endpoint.requests,
                  "refusals": endpoint.refusals[:2], "errors": errors,
                  "ok": endpoint.requests == 0}
        if not result["ok"]:
            result["why"] = ("fetch sent S3 requests (with its signed "
                             "credentials) to an endpoint with a self-signed "
                             "certificate")
        return result


def cache_password_hash(basedir: Path, port: int):
    subprocess.run(
        [str(basedir / "bin" / "mysql"), "--no-defaults",
         "--ssl-mode=DISABLED", "--get-server-public-key",
         "--host=127.0.0.1", f"--port={port}",
         f"--user={harness.REPLICATION_USER}",
         f"--password={harness.REPLICATION_PASSWORD}", "-e", "SELECT 1"],
        capture_output=True, timeout=30, check=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--mysql-basedir", type=Path, required=True)
    parser.add_argument("--binsrv", type=Path, required=True)
    parser.add_argument("--workdir", type=Path, required=True)
    parser.add_argument("--port", type=int, default=23640)
    parser.add_argument("--mode", choices=["position"], default="position")
    args = parser.parse_args()
    args.mysql_basedir = args.mysql_basedir.expanduser()
    args.workdir.mkdir(parents=True, exist_ok=True)
    certificates = make_certificates(args.workdir / "certs")
    TlsSource.certificates = certificates
    harness.SERVER = TlsSource(args.mysql_basedir, args.workdir / "mysql",
                               args.port, False, server_id=1)
    harness.BINSRV = harness.BinlogServer(args.binsrv.expanduser(),
                                          args.workdir / "binsrv", args.port,
                                          False)
    results = []
    harness.SERVER.start()
    try:
        machine = harness.FetchCrashMachine()
        machine.start_from_scratch()
        machine.run_workload(WORKLOAD)
        cases = Cases(args, certificates)
        plan = [cases.direct]
        for mode in (None, "preferred", "required", "verify_ca",
                     "verify_identity"):
            plan.append(lambda mode=mode: cases.mitm("self", mode))
        for mode in ("verify_ca", "verify_identity"):
            plan.append(lambda mode=mode: cases.mitm("evil", mode))
        for mode in (None, "preferred", "required", "verify_ca",
                     "verify_identity"):
            plan.append(lambda mode=mode: cases.strip(mode))
        plan.append(cases.s3_https)
        for case in plan:
            try:
                result = case()
            except (harness.Failure, subprocess.SubprocessError,
                    OSError) as failure:
                result = {"case": getattr(case, "__name__", "?"),
                          "ok": False,
                          "why": harness.describe_failure(failure, 400)}
            print(json.dumps(result), flush=True)
            results.append(result)
    finally:
        harness.SERVER.stop()
    failures = [result for result in results if not result.get("ok")]
    unverified = [result["case"] for result in results
                  if result.get("accepted") and not result.get("must_refuse")]
    print(json.dumps({"cases": len(results), "failures": len(failures),
                      "accepted_by_design_without_verification": unverified,
                      "passed": not failures}), flush=True)
    (args.workdir / "result.json").write_text(json.dumps(results, indent=2))
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
