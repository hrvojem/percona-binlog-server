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

"""The TLS configuration of both ends: what the 'pull' listener negotiates,
and whether each 'connection.ssl' / 'connection.tls' setting of 'fetch' that
should make the connection to the source fail does.

Listener side. 'pull' runs with the MTR suite's test certificate
('replication_source.ssl'); clients do the MySQL TLS upgrade (the server
greeting, an SSLRequest, then TLS on the same socket) in three ways: a
hand-built ClientHello (so that protocol versions and cipher suites the
local OpenSSL cannot offer, SSLv3, export, RC4, are asked for too), Python's
ssl module, and 'openssl s_client -starttls mysql'. Cases:

- 'listener_protocol_versions': SSLv3, TLS 1.0, 1.1, 1.2 and 1.3, each with
  a ClientHello of that version only and, where the local OpenSSL has it,
  with a real handshake at security level 0. Fails if SSLv3, TLS 1.0 or
  TLS 1.1 is accepted or TLS 1.2 and 1.3 are both refused.
- 'listener_cipher_suites': the cipher suites the listener selects, for
  each accepted version, found by offering every code point and removing
  the selected one until the listener refuses; the TLS 1.2 list is checked
  against real handshakes, one cipher each. Lists the suites with a NULL
  cipher, no authentication, export strength, RC4, DES, 3DES, a CBC-SHA1
  MAC or no forward secrecy, and whether the listener's or the client's
  order decides. Fails if a NULL, anonymous, export or RC4 suite is
  accepted; the rest is recorded.
- 'listener_renegotiation': whether the ServerHello carries the
  renegotiation_info extension, and whether a renegotiation started by a
  TLS 1.2 client ('R' in s_client) is carried out. Recorded; fails only on
  a renegotiation without the extension.
- 'listener_compression': a ClientHello offering DEFLATE; the method the
  listener selects. Recorded.
- 'listener_certificate': the key size, signature, validity and names of
  the configured certificate, what the listener sends (chain length, a
  request for a client certificate), and the key exchange and signature of
  a default handshake. Recorded.
- 'listener_session_resumption': whether a TLS 1.2 and a TLS 1.3 session
  are resumed on a second connection, and the ticket lifetime. Recorded.
- 'listener_plain_downgrade': a client that asks for TLS must not be served
  in plain: after an SSLRequest, a plain greeting and password are not
  answered; with 'require_secure_transport' a client without TLS gets no
  authentication exchange (that it gets no error either is ticket 48), and
  a TLS login works.
- 'listener_legacy_openssl_policy': 'pull' started with OPENSSL_CONF naming
  a configuration with 'MinProtocol = TLSv1' and security level 0, as a
  host with a legacy crypto policy has; the versions and suites it accepts
  then. Recorded ('floor_set_by': 'the code' if TLS 1.0 and 1.1 are still
  refused, else 'the OpenSSL configuration of the host'): network_service
  creates its context with 'tls_server' and sets no minimum version and no
  cipher list of its own.

Source side. A MySQL source with a certificate of a test CA (the helpers of
upstream_tls_test.py); 'fetch' runs with one 'connection' configuration per
case and must either store data (exit 0) or not connect (another exit code,
nothing stored); its last error lines are recorded, and 'log_says_why'
tells whether the log has the MySQL client's reason for a refusal (it has
not: collector_context logs "unable to establish connection to mysql
server" and drops the message; recorded, the exit code 1 is what counts).
Cases ('source_<group>/<name>'):

- 'client_cert': a 'REQUIRE X509' user with the right client certificate
  (accepted), with none, one of another CA, an expired one, one with
  another key, and with TLS disabled (refused); a missing certificate file
  (refused).
- 'ca': 'verify_ca' with the right CA file or directory (accepted), with
  another CA, a missing, unreadable, empty or non-PEM file, an empty
  directory, or no CA at all (refused).
- 'crl': 'verify_ca' with a 'crl' / 'crlpath' that revokes the source's
  certificate (refused) or another one (accepted); a missing or non-PEM
  CRL file (refused).
- 'cipher': with the source limited to one TLS 1.2 cipher and the client
  to TLS 1.2: that cipher (accepted), another one or an unknown name
  (refused).
- 'tls': 'version' TLSv1.2 / TLSv1.3 (accepted); a version the source does
  not allow, in both directions, an unknown or removed version (refused);
  'ciphersuites' the source has (accepted) or lacks, or an unknown name
  (refused).
- 'identity': 'verify_identity' to an address the certificate does not
  name (refused), 'verify_ca' to the same address (accepted).

Cases whose outcome is the MySQL client's design, not a failure, are run
and listed in the summary as recorded only: a CA or CRL with 'required'
(no verification), a 'cipher' the source lacks while TLS 1.3 is allowed
(TLS 1.3 suites are 'tls.ciphersuites'), a 'tls' section with TLS disabled.

A JSON line per case, then a summary; exit 1 if any case failed. Example:

  PYTHONPATH=~/ws/qa/pylib python3 tls_config_test.py \\
    --mysql-basedir ~/ws/dist/mysql-8.4.8-linux-glibc2.28-x86_64-minimal \\
    --binsrv ~/ws/percona-binlog-server-build-debug_gcc14/binlog_server \\
    --workdir ~/ws/qa/tls-config
"""

import argparse
import contextlib
import json
import os
import re
import shutil
import socket
import ssl
import struct
import subprocess
import sys
import time
import warnings
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import fetch_crash_test as harness  # noqa: E402
import listener_fuzz_test as listener  # noqa: E402
import listener_login_test as login_test  # noqa: E402
import upstream_tls_test as upstream  # noqa: E402

WORKLOAD = [{"rows": [200], "ddl": True}]
CERTIFICATE = Path(listener.SSL["cert"])
X509_USER = ("x509only", "X509-Pass-7Qz")

SSL3, TLS10, TLS11, TLS12, TLS13 = 0x0300, 0x0301, 0x0302, 0x0303, 0x0304
VERSION_NAMES = {SSL3: "SSLv3", TLS10: "TLSv1.0", TLS11: "TLSv1.1",
                 TLS12: "TLSv1.2", TLS13: "TLSv1.3"}
ALERTS = {10: "unexpected_message", 20: "bad_record_mac",
          40: "handshake_failure", 47: "illegal_parameter",
          50: "decode_error", 70: "protocol_version",
          71: "insufficient_security", 80: "internal_error",
          86: "inappropriate_fallback", 100: "no_renegotiation",
          109: "missing_extension", 112: "unrecognized_name"}
# every cipher suite code point of TLS 1.2 and earlier that was ever
# assigned (the signalling values 0x00FF and 0x5600 left out)
OLD_SUITES = (list(range(0x0001, 0x00FF)) + list(range(0xC001, 0xC100)) +
              list(range(0xCC13, 0xCC16)) + list(range(0xCCA8, 0xCCAF)) +
              list(range(0xD001, 0xD006)))
TLS13_SUITES = list(range(0x1301, 0x1306))
# what the local 'openssl ciphers' cannot name (it has none of these)
EXPORT_SUITES = {0x0003, 0x0006, 0x0008, 0x000B, 0x000E, 0x0011, 0x0014,
                 0x0017, 0x0019, 0x0026, 0x0027, 0x0028, 0x0029, 0x002A,
                 0x002B, 0x0060, 0x0061, 0x0062, 0x0063, 0x0064, 0x0065}
RC4_SUITES = {0x0003, 0x0004, 0x0005, 0x0017, 0x0018, 0x0020, 0x0024,
              0x0028, 0x002B, 0x0060, 0x0064, 0x0065, 0x008A, 0x008E,
              0x0092, 0xC002, 0xC007, 0xC00C, 0xC011, 0xC016, 0xC033}
NULL_SUITES = {0x0001, 0x0002, 0x002C, 0x002D, 0x002E, 0x003B, 0x00B0,
               0x00B1, 0x00B4, 0x00B5, 0x00B8, 0x00B9, 0xC001, 0xC006,
               0xC00B, 0xC010, 0xC015, 0xC039, 0xC03A, 0xC03B}
ANONYMOUS_SUITES = {0x0017, 0x0018, 0x0019, 0x001A, 0x001B, 0x0034, 0x003A,
                    0x0046, 0x0089, 0x009B, 0x006C, 0x006D, 0x00A6, 0x00A7,
                    0x00BF, 0x00C5, 0xC015, 0xC016, 0xC017, 0xC018, 0xC019,
                    0xC046, 0xC047, 0xC05A, 0xC05B, 0xC084, 0xC085}
FORBIDDEN = ("NULL", "anonymous", "export", "RC4")
# all that 'fetch' logs when it cannot connect to the source
GENERIC_ERRORS = ("unable to establish connection to mysql server",
                  "fetching binlog events loop did not reach EOF")
LEGACY_OPENSSL_CONF = """\
openssl_conf = openssl_init
[openssl_init]
ssl_conf = ssl_configuration
[ssl_configuration]
system_default = tls_system_default
[tls_system_default]
MinProtocol = TLSv1
CipherString = DEFAULT:@SECLEVEL=0
"""


# ---------------------------------------------------------------------------
# the listener
# ---------------------------------------------------------------------------

def start_pull(binsrv, port: int, require_secure_transport: bool = False,
               environment: "dict | None" = None) -> subprocess.Popen:
    binsrv.write_config()
    config = json.loads(binsrv.config_path.read_text())
    config["replication_source"] = {
        "port": port, "server_id": 142,
        "server_uuid": "00000000-0000-0000-0000-00000000008e",
        "read_timeout": 10, "write_timeout": 10,
        "require_secure_transport": require_secure_transport,
        "auth_rsa_private_key": str(listener.AUTH_RSA_PRIVATE_KEY),
        "ssl": listener.SSL,
        "authentication": {"user": listener.PULL_USER,
                           "password": listener.PULL_PASSWORD,
                           "plugin": "caching_sha2_password"}}
    binsrv.config_path.write_text(json.dumps(config, indent=2))
    pull = subprocess.Popen(
        [str(binsrv.binary), "pull", str(binsrv.config_path)],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        env=dict(os.environ, AWS_EC2_METADATA_DISABLED="true",
                 **(environment or {})))
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        try:
            socket.create_connection(("127.0.0.1", port), timeout=1).close()
            return pull
        except OSError:
            if pull.poll() is not None:
                raise harness.Failure("pull exited before it listened\n" +
                                      binsrv.log_tail())
            time.sleep(0.2)
    pull.kill()
    raise harness.Failure("pull did not start listening in 30 s")


def upgrade_requested(port: int, timeout: float = 5.0) -> listener.Listener:
    """A connection on which the listener expects a ClientHello next: its
    greeting read, an SSLRequest sent."""
    client = listener.Listener("127.0.0.1", port, timeout=timeout)
    client.read_frame()
    client.send(listener.frame(1, listener.ssl_request_payload(
        listener.BASE_CAPABILITIES | listener.CLIENT_SSL)))
    return client


def extension(kind: int, data: bytes) -> bytes:
    return struct.pack(">HH", kind, len(data)) + data


def client_hello(version: int, suites: list, compressions: tuple = (0,)
                 ) -> bytes:
    """A ClientHello record for exactly 'version' (TLS 1.3: through
    supported_versions) offering 'suites'."""
    modern = version == TLS13
    session = os.urandom(32) if modern else b""
    body = struct.pack(">H", min(version, TLS12)) + os.urandom(32)
    body += bytes([len(session)]) + session
    body += struct.pack(">H", 2 * len(suites)) + b"".join(
        struct.pack(">H", suite) for suite in suites)
    body += bytes([len(compressions)]) + bytes(compressions)
    if version != SSL3:
        groups = (0x001D, 0x0017, 0x0018, 0x0019, 0x001E, 0x0100, 0x0101)
        signatures = (0x0403, 0x0503, 0x0603, 0x0807, 0x0808, 0x0804, 0x0805,
                      0x0806, 0x0809, 0x080A, 0x080B, 0x0401, 0x0501, 0x0601,
                      0x0303, 0x0301, 0x0302, 0x0402, 0x0502, 0x0602, 0x0201,
                      0x0203, 0x0202, 0x0101)
        extensions = (
            extension(0x000A, struct.pack(">H", 2 * len(groups)) + b"".join(
                struct.pack(">H", group) for group in groups)) +
            extension(0x000B, b"\x01\x00") +
            extension(0x000D, struct.pack(">H", 2 * len(signatures)) +
                      b"".join(struct.pack(">H", one)
                               for one in signatures)) +
            extension(0xFF01, b"\x00") +   # renegotiation_info
            extension(0x0017, b"") +       # extended_master_secret
            extension(0x0023, b""))        # session_ticket
        if modern:
            extensions += (
                extension(0x002B, b"\x02\x03\x04") +
                extension(0x002D, b"\x01\x01") +
                # any 32 bytes are an X25519 public key
                extension(0x0033, struct.pack(">HHH", 36, 0x001D, 32) +
                          os.urandom(32)))
        body += struct.pack(">H", len(extensions)) + extensions
    handshake = b"\x01" + len(body).to_bytes(3, "big") + body
    return b"\x16" + struct.pack(
        ">HH", SSL3 if version == SSL3 else TLS10, len(handshake)) + handshake


def server_answer(client: listener.Listener) -> dict:
    """The listener's answer to a ClientHello: the version, cipher suite,
    compression method and extensions of its ServerHello, or its alert."""
    def receive(count: int) -> "bytes | None":
        data = b""
        while len(data) < count:
            try:
                piece = client.sock.recv(count - len(data))
            except OSError:
                return None
            if not piece:
                return None
            data += piece
        return data

    header = receive(5)
    if header is None:
        return {"refused": "connection closed"}
    kind, _, length = struct.unpack(">BHH", header)
    payload = receive(length)
    if payload is None:
        return {"refused": "truncated record"}
    if kind == 0x15 and len(payload) == 2:
        return {"refused": "alert " + ALERTS.get(payload[1],
                                                  str(payload[1]))}
    if kind != 0x16 or payload[:1] != b"\x02":
        return {"refused": "record " + header.hex() + payload[:8].hex()}
    hello = payload[4:]
    version = struct.unpack_from(">H", hello)[0]
    offset = 35 + hello[34]
    suite = struct.unpack_from(">H", hello, offset)[0]
    compression = hello[offset + 2]
    offset += 3
    extensions = []
    if offset + 2 <= len(hello):
        end = offset + 2 + struct.unpack_from(">H", hello, offset)[0]
        offset += 2
        while offset + 4 <= end:
            number, size = struct.unpack_from(">HH", hello, offset)
            if number == 0x002B and size == 2:
                version = struct.unpack_from(">H", hello, offset + 4)[0]
            extensions.append(number)
            offset += 4 + size
    return {"version": version, "suite": suite, "compression": compression,
            "extensions": extensions}


def offer(port: int, version: int, suites: list,
          compressions: tuple = (0,)) -> dict:
    client = upgrade_requested(port)
    try:
        client.send(client_hello(version, suites, compressions))
        return server_answer(client)
    finally:
        client.close()


def accepted(answer: dict, version: int) -> bool:
    return answer.get("version") == version


def selected_suites(port: int, version: int) -> "tuple[list, str]":
    """Every suite the listener selects for 'version', and how it refused
    the remaining ones."""
    remaining = list(TLS13_SUITES if version == TLS13 else OLD_SUITES)
    selected = []
    while remaining:
        answer = offer(port, version, remaining)
        if not accepted(answer, version) or answer["suite"] not in remaining:
            return selected, answer.get("refused", json.dumps(answer))
        selected.append(answer["suite"])
        remaining.remove(answer["suite"])
    return selected, "nothing left to offer"


def local_cipher_table() -> dict:
    """Code point -> what 'openssl ciphers -V' says about it."""
    output = subprocess.run(
        ["openssl", "ciphers", "-V", "ALL:COMPLEMENTOFALL:@SECLEVEL=0"],
        capture_output=True, text=True, check=False).stdout
    table = {}
    for line in output.splitlines():
        match = re.match(
            r"\s*0x(\w\w),0x(\w\w) - (\S+)\s+(\S+)\s+Kx=(\S+)\s+Au=(\S+)\s+"
            r"Enc=(\S+)\s+Mac=(\S+)", line)
        if match:
            table[int(match[1] + match[2], 16)] = {
                "name": match[3], "protocol": match[4], "kx": match[5],
                "au": match[6], "enc": match[7], "mac": match[8]}
    return table


def weaknesses(code: int, table: dict) -> list:
    found = set()
    entry = table.get(code)
    if entry is not None:
        enc, mac = entry["enc"], entry["mac"]
        if enc.startswith("None"):
            found.add("NULL")
        if entry["au"] == "None":
            found.add("anonymous")
        if "RC4" in enc:
            found.add("RC4")
        if enc.startswith("3DES"):
            found.add("3DES")
        if enc.startswith("DES("):
            found.add("DES")
        if mac == "MD5":
            found.add("MD5")
        if mac == "SHA1" and not enc.startswith(("None", "RC4")):
            found.add("CBC-SHA1")
        if entry["kx"] in ("RSA", "PSK", "RSAPSK") and \
                entry["protocol"] != "TLSv1.3":
            found.add("no forward secrecy")
    elif code not in TLS13_SUITES:
        found.add("not known to the local openssl")
    for name, codes in (("export", EXPORT_SUITES), ("RC4", RC4_SUITES),
                        ("NULL", NULL_SUITES),
                        ("anonymous", ANONYMOUS_SUITES)):
        if code in codes:
            found.add(name)
    return sorted(found)


def suite_name(code: int, table: dict) -> str:
    return table[code]["name"] if code in table else f"0x{code:04X}"


def real_handshake(port: int, version: "ssl.TLSVersion | None" = None,
                   ciphers: "str | None" = None,
                   context: "ssl.SSLContext | None" = None,
                   session=None) -> "tuple[listener.Listener | None, str]":
    """A TLS handshake by Python's ssl module after the MySQL upgrade; the
    open client, or None and why."""
    try:
        if context is None:
            context = listener.tls_context()
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                if version is not None:
                    context.minimum_version = version
                    context.maximum_version = version
            if ciphers is not None:
                context.set_ciphers(ciphers)
    except (ssl.SSLError, ValueError) as error:
        return None, f"the local openssl cannot offer it: {error}"
    client = upgrade_requested(port)
    try:
        client.sock = context.wrap_socket(client.sock, session=session)
    except (OSError, ValueError) as error:
        client.close()
        return None, str(error)[:120]
    return client, ""


def s_client(port: int, options: list, stdin: "str | None" = None,
             wait: float = 0.0) -> str:
    """The output of 'openssl s_client -starttls mysql'; with 'stdin', that
    is typed 'wait' seconds after the start and the session then held for
    'wait' seconds more."""
    command = ["openssl", "s_client", "-starttls", "mysql", "-connect",
               f"127.0.0.1:{port}", *options]
    if stdin is None:
        try:
            done = subprocess.run(command, stdin=subprocess.DEVNULL,
                                  capture_output=True, text=True, timeout=20,
                                  check=False)
        except subprocess.TimeoutExpired:
            return "timed out"
        return done.stdout + done.stderr
    process = subprocess.Popen(command, stdin=subprocess.PIPE,
                               stdout=subprocess.PIPE,
                               stderr=subprocess.STDOUT, text=True)
    try:
        time.sleep(wait)
        process.stdin.write(stdin)
        process.stdin.flush()
        time.sleep(wait)
        output, _ = process.communicate(timeout=20)
    except (OSError, subprocess.TimeoutExpired):
        process.kill()
        output, _ = process.communicate()
    return output


class ListenerCases:
    def __init__(self, binsrv, port: int):
        self.binsrv, self.port, self.pull = binsrv, port, None
        self.table = local_cipher_table()
        self.versions = {}

    def restart(self, require_secure_transport: bool = False,
                environment: "dict | None" = None):
        self.stop()
        self.pull = start_pull(self.binsrv, self.port,
                               require_secure_transport, environment)

    def stop(self):
        if self.pull is not None:
            login_test.stop_pull(self.pull)
            self.pull = None

    def alive(self) -> bool:
        return self.pull is not None and self.pull.poll() is None

    def finish(self, result: dict, problems: list) -> dict:
        if not self.alive():
            problems.append(f"pull ended (exit {self.pull.returncode})")
        result["ok"] = not problems
        if problems:
            result["why"] = "; ".join(problems)
        return result

    def measure_versions(self) -> dict:
        """Per version: the answer to a ClientHello of that version, and the
        outcome of a real handshake."""
        real = {TLS10: ssl.TLSVersion.TLSv1, TLS11: ssl.TLSVersion.TLSv1_1,
                TLS12: ssl.TLSVersion.TLSv1_2, TLS13: ssl.TLSVersion.TLSv1_3}
        measured = {}
        for version, name in VERSION_NAMES.items():
            answer = offer(self.port, version,
                           TLS13_SUITES if version == TLS13 else OLD_SUITES)
            entry = {"client_hello": "accepted" if accepted(answer, version)
                     else answer.get("refused") or "answered with " +
                     VERSION_NAMES.get(answer.get("version"), "?")}
            if version in real:
                client, why = real_handshake(
                    self.port, real[version],
                    None if version == TLS13
                    else "ALL:COMPLEMENTOFALL:@SECLEVEL=0")
                if client is None:
                    entry["handshake"] = "failed: " + why
                else:
                    entry["handshake"] = (f"{client.sock.version()} "
                                          f"{client.sock.cipher()[0]}")
                    client.close()
            else:
                entry["handshake"] = "the local openssl has no SSLv3"
            entry["accepted"] = entry["client_hello"] == "accepted" or \
                not entry["handshake"].startswith(("failed", "the local"))
            measured[name] = entry
        return measured

    def protocol_versions(self) -> dict:
        self.versions = self.measure_versions()
        problems = [f"{name} is accepted" for name in
                    ("SSLv3", "TLSv1.0", "TLSv1.1")
                    if self.versions[name]["accepted"]]
        if not (self.versions["TLSv1.2"]["accepted"] or
                self.versions["TLSv1.3"]["accepted"]):
            problems.append("neither TLS 1.2 nor TLS 1.3 is accepted")
        return self.finish(
            {"case": "listener_protocol_versions", "versions": self.versions,
             "accepted": [name for name, entry in self.versions.items()
                          if entry["accepted"]]}, problems)

    def measure_suites(self, versions: dict) -> "tuple[dict, dict, list]":
        """Per accepted version: the names of the suites the listener
        selects; the weak ones by kind; the code points."""
        suites, weak, codes = {}, {}, []
        for version, name in VERSION_NAMES.items():
            if not versions.get(name, {}).get("accepted"):
                continue
            selected, refusal = selected_suites(self.port, version)
            suites[name] = [suite_name(code, self.table)
                            for code in selected]
            suites[name + "_then"] = refusal
            for code in selected:
                codes.append(code)
                for kind in weaknesses(code, self.table):
                    weak.setdefault(kind, [])
                    label = suite_name(code, self.table)
                    if label not in weak[kind]:
                        weak[kind].append(label)
        return suites, weak, codes

    def cipher_suites(self) -> dict:
        if not self.versions:
            self.versions = self.measure_versions()
        suites, weak, codes = self.measure_suites(self.versions)
        problems = [f"{kind} suites are accepted: {', '.join(weak[kind])}"
                    for kind in FORBIDDEN if kind in weak]
        result = {"case": "listener_cipher_suites", "selected": suites,
                  "weak": weak}
        # real handshakes, one TLS 1.2 cipher each
        completed, differences = [], []
        if self.versions.get("TLSv1.2", {}).get("accepted"):
            hello_names = set(suites.get("TLSv1.2", []))
            for entry in self.table.values():
                if entry["protocol"] == "TLSv1.3":
                    continue
                client, _ = real_handshake(self.port, ssl.TLSVersion.TLSv1_2,
                                           entry["name"] + ":@SECLEVEL=0")
                if client is not None:
                    completed.append(client.sock.cipher()[0])
                    client.close()
            differences = sorted(hello_names.symmetric_difference(completed))
            result["tls12_handshakes_completed"] = len(completed)
            result["tls12_differences_between_both_methods"] = differences
        # TLS 1.3 suites by s_client, which can limit them
        if self.versions.get("TLSv1.3", {}).get("accepted"):
            result["tls13_handshakes_completed"] = [
                name for name in ("TLS_AES_128_GCM_SHA256",
                                  "TLS_AES_256_GCM_SHA384",
                                  "TLS_CHACHA20_POLY1305_SHA256",
                                  "TLS_AES_128_CCM_SHA256",
                                  "TLS_AES_128_CCM_8_SHA256")
                if "CONNECTION ESTABLISHED" in s_client(
                    self.port, ["-brief", "-tls1_3", "-ciphersuites", name])]
        # whose order decides: two suites offered in both orders
        old = [code for code in codes if code not in TLS13_SUITES]
        if len(old) >= 2:
            first = offer(self.port, TLS12, [old[0], old[-1]]).get("suite")
            second = offer(self.port, TLS12, [old[-1], old[0]]).get("suite")
            result["order_decided_by"] = ("the listener" if first == second
                                          else "the client")
        for kind in FORBIDDEN:
            names = [entry["name"] for code, entry in self.table.items()
                     if kind in weaknesses(code, self.table)]
            if not names:
                continue
            client, _ = real_handshake(
                self.port, ssl.TLSVersion.TLSv1_2,
                ":".join(names) + ":@SECLEVEL=0")
            if client is not None:
                problems.append(f"a handshake offering only {kind} suites "
                                f"completed with {client.sock.cipher()[0]}")
                client.close()
        return self.finish(result, problems)

    def renegotiation(self) -> dict:
        problems = []
        answer = offer(self.port, TLS12, OLD_SUITES)
        extension_sent = 0xFF01 in answer.get("extensions", [])
        output = s_client(self.port, ["-tls1_2", "-msg"], "R\n", 2.0)
        before, marker, after = output.partition("RENEGOTIATING")
        carried_out = "ServerHello" in after
        result = {"case": "listener_renegotiation",
                  "tls12_available": accepted(answer, TLS12),
                  "renegotiation_info_in_server_hello": extension_sent,
                  "s_client_secure_renegotiation":
                      "Secure Renegotiation IS supported" in before,
                  "client_renegotiation_tried": bool(marker),
                  "client_renegotiation_carried_out": carried_out,
                  "after_the_request": " ".join(
                      line.strip() for line in after.splitlines()
                      if "error" in line or "alert" in line.lower()
                      or "ServerHello" in line)[:300]}
        if carried_out and not extension_sent:
            problems.append("a renegotiation was carried out without the "
                            "renegotiation_info extension")
        return self.finish(result, problems)

    def compression(self) -> dict:
        answers = {}
        for version in (TLS12, TLS13):
            answer = offer(self.port, version,
                           TLS13_SUITES if version == TLS13 else OLD_SUITES,
                           (1, 0) if version == TLS12 else (0,))
            answers[VERSION_NAMES[version]] = (
                {0: "none", 1: "DEFLATE"}.get(answer["compression"],
                                              str(answer["compression"]))
                if "compression" in answer else answer.get("refused"))
        client, why = real_handshake(self.port)
        real = why
        if client is not None:
            real = client.sock.compression() or "none"
            client.close()
        return self.finish(
            {"case": "listener_compression",
             "selected_when_deflate_is_offered": answers,
             "real_handshake": real}, [])

    def certificate(self) -> dict:
        text = subprocess.run(
            ["openssl", "x509", "-in", str(CERTIFICATE), "-noout", "-text"],
            capture_output=True, text=True, check=False).stdout

        def field(pattern: str) -> str:
            match = re.search(pattern, text)
            return match[1].strip() if match else ""

        expired = subprocess.run(
            ["openssl", "x509", "-in", str(CERTIFICATE), "-noout",
             "-checkend", "0"], capture_output=True, check=False
        ).returncode != 0
        shown = s_client(self.port, ["-showcerts"])
        brief = s_client(self.port, ["-brief"])
        details = {}
        for line in brief.splitlines():
            name, colon, value = line.partition(":")
            if colon and name in ("Protocol version", "Ciphersuite",
                                  "Hash used", "Signature type",
                                  "Server Temp Key", "Peer Temp Key"):
                details[name] = value.strip()
        return self.finish(
            {"case": "listener_certificate", "file": CERTIFICATE.name,
             "public_key": field(r"Public Key Algorithm: (.+)") + " " +
             field(r"Public-Key: \((.+?)\)"),
             "signature": field(r"Signature Algorithm: (.+)"),
             "subject": field(r"Subject: (.+)"),
             "issuer": field(r"Issuer: (.+)"),
             "not_after": field(r"Not After : (.+)"), "expired": expired,
             "subject_alternative_names":
                 field(r"Subject Alternative Name:.*\n\s*(.+)"),
             "is_a_ca": "CA:TRUE" in text,
             "certificates_sent": shown.count("BEGIN CERTIFICATE"),
             "client_certificate_requested":
                 "Acceptable client certificate CA names" in shown,
             "default_handshake": details}, [])

    def session_resumption(self) -> dict:
        result = {"case": "listener_session_resumption"}
        for version, name in ((ssl.TLSVersion.TLSv1_2, "TLSv1.2"),
                              (ssl.TLSVersion.TLSv1_3, "TLSv1.3")):
            context = listener.tls_context()
            context.minimum_version = context.maximum_version = version
            first, why = real_handshake(self.port, context=context)
            if first is None:
                result[name] = "no handshake: " + why
                continue
            # a TLS 1.3 ticket comes after the handshake: have the listener
            # answer a greeting, so that it is read
            first.send(listener.frame(2, listener.greeting_payload(
                listener.BASE_CAPABILITIES | listener.CLIENT_SSL,
                login_test.OTHER_USER.encode(), os.urandom(32),
                b"caching_sha2_password")))
            first.read_frame()
            session = first.sock.session
            first.close()
            entry = {"ticket": bool(session and session.has_ticket),
                     "ticket_lifetime_seconds":
                         session.ticket_lifetime_hint if session else None,
                     "session_timeout_seconds":
                         session.timeout if session else None}
            second, why = real_handshake(self.port, context=context,
                                         session=session)
            if second is None:
                entry["second_handshake"] = "failed: " + why
            else:
                entry["resumed"] = second.sock.session_reused
                second.close()
            result[name] = entry
        return self.finish(result, [])

    def plain_downgrade(self) -> dict:
        problems = []
        result = {"case": "listener_plain_downgrade"}
        scramble_path = {"fast authentication", "full authentication", "OK"}
        for require in (False, True):
            self.restart(require)
            where = ("require_secure_transport" if require
                     else "secure_transport_optional")
            entry = {}
            dialogue = login_test.Dialogue(self.port, 3.0)
            try:
                entry["greeting_offers_tls"] = bool(
                    dialogue.capabilities & listener.CLIENT_SSL)
                dialogue.client.send(listener.frame(
                    1, listener.ssl_request_payload(
                        listener.BASE_CAPABILITIES | listener.CLIENT_SSL)))
                dialogue.sequence = 2
                reply = dialogue.greet(
                    capabilities=listener.BASE_CAPABILITIES |
                    listener.CLIENT_SSL)
                if reply == "full authentication":
                    reply += ", " + dialogue.say(
                        login_test.PASSWORD + b"\x00")
                entry["ssl_request_then_plain_greeting"] = reply
                if not login_test.refused(reply):
                    problems.append(f"{where}: after an SSLRequest a plain "
                                    f"greeting was answered with '{reply}'")
            finally:
                dialogue.close()
            if not entry["greeting_offers_tls"]:
                problems.append(f"{where}: the server greeting does not "
                                "offer TLS")
            dialogue = login_test.Dialogue(self.port, 3.0)
            try:
                reply = dialogue.greet()
                entry["plain_client"] = reply
                if require and reply in scramble_path:
                    problems.append(
                        "require_secure_transport: a client without TLS got "
                        f"an authentication exchange ('{reply}')")
            finally:
                dialogue.close()
            served, _ = login_test.login(self.port, True)
            entry["tls_login"] = served
            if not served:
                problems.append(f"{where}: a login in TLS failed")
            if require:
                served, _ = login_test.login(self.port, False)
                entry["plain_login_after_a_tls_login"] = served
                if served:
                    problems.append("require_secure_transport: a login "
                                    "without TLS succeeded")
            result[where] = entry
        return self.finish(result, problems)

    def legacy_openssl_policy(self) -> dict:
        path = self.binsrv.workdir / "legacy-openssl.cnf"
        path.write_text(LEGACY_OPENSSL_CONF)
        linked = subprocess.run(["ldd", str(self.binsrv.binary)],
                                capture_output=True, text=True, check=False
                                ).stdout
        self.restart(environment={"OPENSSL_CONF": str(path)})
        versions = self.measure_versions()
        suites, weak, _ = self.measure_suites(versions)
        old = [name for name in ("SSLv3", "TLSv1.0", "TLSv1.1")
               if versions[name]["accepted"]]
        result = {
            "case": "listener_legacy_openssl_policy",
            "openssl_conf": LEGACY_OPENSSL_CONF.splitlines()[-2:],
            "libssl": sorted({line.split()[0] for line in linked.splitlines()
                              if "libssl" in line or "libcrypto" in line})
            or "not a shared library of the binary",
            "accepted": [name for name, entry in versions.items()
                         if entry["accepted"]],
            "versions": versions, "selected": suites, "weak": weak,
            "floor_set_by": "the OpenSSL configuration of the host" if old
            else "the code",
            "recorded_only": True}
        if old:
            result["note"] = (
                f"with this OpenSSL configuration the listener accepts "
                f"{', '.join(old)}: the minimum version and the cipher "
                "list are the host's OpenSSL defaults, the code sets none")
        return self.finish(result, [])


# ---------------------------------------------------------------------------
# the source
# ---------------------------------------------------------------------------

def make_more_certificates(directory: Path) -> dict:
    """Next to those of upstream_tls_test.make_certificates(): client
    certificates (good, expired, of another CA), CRLs of the test CA (one
    revoking the source's certificate, one revoking another), and the CA
    and CRL directories."""
    def openssl(*arguments: str) -> str:
        return subprocess.run(["openssl", *arguments], cwd=directory,
                              check=True, capture_output=True,
                              text=True).stdout.strip()

    openssl("req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "30",
            "-subj", "/CN=PBS QA another CA", "-keyout", "other-ca-key.pem",
            "-out", "other-ca.pem")
    for name, authority, days in (("client", "ca", "30"),
                                  ("client-expired", "ca", "-1"),
                                  ("client-other", "other-ca", "30")):
        openssl("req", "-newkey", "rsa:2048", "-nodes", "-subj",
                f"/CN=pbs-{name}", "-keyout", f"{name}-key.pem", "-out",
                f"{name}.csr")
        openssl("x509", "-req", "-in", f"{name}.csr", "-CA",
                f"{authority}.pem", "-CAkey", f"{authority}-key.pem",
                "-CAcreateserial", "-days", days, "-out", f"{name}.pem")
    paths = {}
    for name, revoked in (("crl-source-revoked", "server"),
                          ("crl-other-revoked", "evil")):
        database = directory / (name + "-db")
        shutil.rmtree(database, ignore_errors=True)
        database.mkdir()
        (database / "index.txt").write_text("")
        (database / "crlnumber").write_text("01\n")
        (database / "ca.cnf").write_text(
            "[ca]\ndefault_ca = qa\n[qa]\n"
            f"database = {database}/index.txt\n"
            f"crlnumber = {database}/crlnumber\n"
            "default_md = sha256\ndefault_crl_days = 30\n")
        for action in (["-revoke", f"{revoked}.pem"],
                       ["-gencrl", "-out", f"{name}.pem"]):
            openssl("ca", "-config", str(database / "ca.cnf"), "-cert",
                    "ca.pem", "-keyfile", "ca-key.pem", *action)
        folder = directory / (name + "-dir")
        shutil.rmtree(folder, ignore_errors=True)
        folder.mkdir()
        digest = openssl("crl", "-hash", "-noout", "-in", f"{name}.pem")
        shutil.copy(directory / f"{name}.pem", folder / f"{digest}.r0")
        paths[name] = directory / f"{name}.pem"
        paths[name + "-dir"] = folder
    folder = directory / "ca-dir"
    shutil.rmtree(folder, ignore_errors=True)
    folder.mkdir()
    shutil.copy(directory / "ca.pem", folder / (
        openssl("x509", "-hash", "-noout", "-in", "ca.pem") + ".0"))
    empty = directory / "empty-dir"
    empty.mkdir(exist_ok=True)
    unreadable = directory / "ca-unreadable.pem"
    unreadable.unlink(missing_ok=True)
    shutil.copy(directory / "ca.pem", unreadable)
    unreadable.chmod(0)
    (directory / "not-pem.pem").write_text("this is not a certificate\n")
    (directory / "empty.pem").write_text("")
    return paths | {
        "ca-dir": folder, "empty-dir": empty, "ca-unreadable": unreadable,
        "not-pem": directory / "not-pem.pem", "empty": directory / "empty.pem",
        "missing": directory / "no-such-file.pem",
        "other-ca": directory / "other-ca.pem"} | {
        name: directory / f"{name}.pem"
        for name in ("client", "client-expired", "client-other")} | {
        name + "-key": directory / f"{name}-key.pem"
        for name in ("client", "client-expired", "client-other")}


@contextlib.contextmanager
def source_tls(**settings: str):
    """The source's TLS system variables set to 'settings' for a while."""
    server = harness.SERVER
    before = {name: server.sql(f"SELECT @@global.{name}").strip()
              for name in settings}

    def apply(values: dict):
        server.sql("".join(
            f"SET GLOBAL {name} = " +
            ("DEFAULT" if value in ("NULL", "") else f"'{value}'") + "; "
            for name, value in values.items()) + "ALTER INSTANCE RELOAD TLS;")

    apply(settings)
    try:
        yield
    finally:
        apply(before)


def host_address() -> str:
    """An address of this host other than the loopback one."""
    output = subprocess.run(["hostname", "-I"], capture_output=True,
                            text=True, check=False).stdout.split()
    return next((address for address in output
                 if "." in address and not address.startswith("127.")), "")


class SourceCases:
    def __init__(self, args, certificates: dict):
        self.args = args
        self.certificates = certificates
        self.binsrv = harness.BINSRV
        self.runner = upstream.Cases(args, certificates)

    def fetch(self, name: str, expected: "str | None",
              ssl_section: "dict | None", tls_section: "dict | None" = None,
              user: "tuple | None" = None, host: str = "127.0.0.1") -> dict:
        """One 'fetch' into an empty storage; 'expected' is 'accepted',
        'refused' or None (recorded only)."""
        self.binsrv.reset()
        self.binsrv.write_config()
        config = json.loads(self.binsrv.config_path.read_text())
        connection = config["connection"]
        connection["host"] = host
        if user is not None:
            connection["user"], connection["password"] = user
        if ssl_section is not None:
            connection["ssl"] = ssl_section
        if tls_section is not None:
            connection["tls"] = tls_section
        path = self.binsrv.workdir / "binsrv-tls.json"
        path.write_text(json.dumps(config, indent=2))
        code, stored, errors = self.runner.fetch(path)
        outcome = ("accepted" if code == 0 and stored else
                   "refused" if code not in (0, -1) and not stored else
                   f"exit {code}, " + ("data" if stored else "no data") +
                   " stored")
        result = {"case": "source_" + name, "ssl": ssl_section,
                  "tls": tls_section, "user": (user or ("rpl",))[0],
                  "exit": code, "stored": stored, "errors": errors,
                  "outcome": outcome, "expected": expected,
                  "ok": expected is None or outcome == expected}
        if outcome != "accepted":
            result["log_says_why"] = self.log_says_why()
        if expected is None:
            result["recorded_only"] = True
        if not result["ok"]:
            result["why"] = (f"expected to be {expected}, but fetch was "
                             f"{outcome}" + (f" ({errors})" if errors
                                             else ""))
        return result

    def log_says_why(self) -> bool:
        """Whether the log of the last 'fetch' has more about a failed
        connection than that it failed (the MySQL client's message: 'SSL
        connection error', 'Access denied', ...)."""
        text = self.binsrv.log_path.read_text(errors="replace") \
            if self.binsrv.log_path.exists() else ""
        _, _, after = text.rpartition("mysql client version")
        return any(line.strip() and GENERIC_ERRORS[0] not in line and
                   GENERIC_ERRORS[1] not in line
                   for line in after.splitlines()[1:])

    def plan(self) -> list:
        """(name, expected, ssl, tls, user, host, source settings)."""
        files = {name: str(path) for name, path in self.certificates.items()}
        ca = {"mode": "verify_ca", "ca": files["ca"]}
        pair = {"cert": files["client"], "key": files["client-key"]}
        x509 = X509_USER
        tls12, tls13 = {"version": "TLSv1.2"}, {"version": "TLSv1.3"}
        one_cipher = {"ssl_cipher": "ECDHE-RSA-AES256-GCM-SHA384"}
        one_suite = {"tls_ciphersuites": "TLS_AES_128_GCM_SHA256"}
        cases = [
            ("client_cert/right", "accepted", ca | pair, None, x509),
            ("client_cert/right_with_required", "accepted",
             {"mode": "required"} | pair, None, x509),
            ("client_cert/none", "refused", ca, None, x509),
            ("client_cert/of_another_ca", "refused",
             ca | {"cert": files["client-other"],
                   "key": files["client-other-key"]}, None, x509),
            ("client_cert/expired", "refused",
             ca | {"cert": files["client-expired"],
                   "key": files["client-expired-key"]}, None, x509),
            ("client_cert/another_key", "refused",
             ca | {"cert": files["client"],
                   "key": files["client-other-key"]}, None, x509),
            ("client_cert/tls_disabled", "refused",
             {"mode": "disabled"} | pair, None, x509),
            ("client_cert/missing_file", "refused",
             {"mode": "required", "cert": files["missing"],
              "key": files["missing"]}, None, None),
            ("client_cert/without_key", None,
             {"mode": "required", "cert": files["client"]}, None, None),

            ("ca/right", "accepted", ca, None, None),
            ("ca/another_ca", "refused",
             {"mode": "verify_ca", "ca": files["other-ca"]}, None, None),
            ("ca/missing_file", "refused",
             {"mode": "verify_ca", "ca": files["missing"]}, None, None),
            ("ca/unreadable_file", "refused",
             {"mode": "verify_ca", "ca": files["ca-unreadable"]}, None,
             None),
            ("ca/not_pem", "refused",
             {"mode": "verify_ca", "ca": files["not-pem"]}, None, None),
            ("ca/empty_file", "refused",
             {"mode": "verify_ca", "ca": files["empty"]}, None, None),
            ("ca/none_given", "refused", {"mode": "verify_ca"}, None, None),
            ("ca/capath_right", "accepted",
             {"mode": "verify_ca", "capath": files["ca-dir"]}, None, None),
            ("ca/capath_empty_directory", "refused",
             {"mode": "verify_ca", "capath": files["empty-dir"]}, None,
             None),
            ("ca/another_ca_with_required", None,
             {"mode": "required", "ca": files["other-ca"]}, None, None),

            ("crl/source_revoked", "refused",
             ca | {"crl": files["crl-source-revoked"]}, None, None),
            ("crl/another_revoked", "accepted",
             ca | {"crl": files["crl-other-revoked"]}, None, None),
            ("crl/crlpath_source_revoked", "refused",
             ca | {"crlpath": files["crl-source-revoked-dir"]}, None, None),
            ("crl/crlpath_another_revoked", "accepted",
             ca | {"crlpath": files["crl-other-revoked-dir"]}, None, None),
            ("crl/missing_file", "refused",
             ca | {"crl": files["missing"]}, None, None),
            ("crl/not_pem", "refused",
             ca | {"crl": files["not-pem"]}, None, None),
            ("crl/source_revoked_with_required", None,
             {"mode": "required", "ca": files["ca"],
              "crl": files["crl-source-revoked"]}, None, None),

            ("cipher/the_source_has_it", "accepted",
             {"mode": "required", "cipher": "ECDHE-RSA-AES256-GCM-SHA384"},
             tls12, None, None, one_cipher),
            ("cipher/the_source_lacks_it", "refused",
             {"mode": "required", "cipher": "ECDHE-RSA-AES128-GCM-SHA256"},
             tls12, None, None, one_cipher),
            ("cipher/the_source_lacks_it_tls13_allowed", None,
             {"mode": "required", "cipher": "ECDHE-RSA-AES128-GCM-SHA256"},
             None, None, None, one_cipher),
            ("cipher/unknown_name", "refused",
             {"mode": "required", "cipher": "NO-SUCH-CIPHER"}, tls12, None),

            ("tls/version_12", "accepted", {"mode": "required"}, tls12,
             None),
            ("tls/version_13", "accepted", {"mode": "required"}, tls13,
             None),
            ("tls/version_13_source_has_12_only", "refused",
             {"mode": "required"}, tls13, None, None,
             {"tls_version": "TLSv1.2"}),
            ("tls/version_12_source_has_13_only", "refused",
             {"mode": "required"}, tls12, None, None,
             {"tls_version": "TLSv1.3"}),
            ("tls/version_13_source_has_12_only_no_ssl_section", None, None,
             tls13, None, None, {"tls_version": "TLSv1.2"}),
            ("tls/version_unknown", "refused", {"mode": "required"},
             {"version": "TLSv9"}, None),
            ("tls/version_11", "refused", {"mode": "required"},
             {"version": "TLSv1.1"}, None),
            ("tls/ciphersuites_the_source_has", "accepted",
             {"mode": "required"},
             tls13 | {"ciphersuites": "TLS_AES_128_GCM_SHA256"}, None, None,
             one_suite),
            ("tls/ciphersuites_the_source_lacks", "refused",
             {"mode": "required"},
             tls13 | {"ciphersuites": "TLS_AES_256_GCM_SHA384"}, None, None,
             one_suite),
            ("tls/ciphersuites_unknown_name", "refused",
             {"mode": "required"},
             tls13 | {"ciphersuites": "TLS_NO_SUCH_SUITE"}, None),
            ("tls/section_with_tls_disabled", None, {"mode": "disabled"},
             tls13, None),
        ]
        other = host_address()
        if other:
            cases += [
                ("identity/verify_ca_other_address", "accepted", ca, None,
                 None, other),
                ("identity/verify_identity_other_address", "refused",
                 {"mode": "verify_identity", "ca": files["ca"]}, None, None,
                 other)]
        return cases

    def run(self):
        harness.SERVER.sql(
            f"DROP USER IF EXISTS '{X509_USER[0]}'@'%'; "
            f"CREATE USER '{X509_USER[0]}'@'%' IDENTIFIED BY "
            f"'{X509_USER[1]}' REQUIRE X509; GRANT REPLICATION SLAVE, "
            f"REPLICATION CLIENT ON *.* TO '{X509_USER[0]}'@'%';")
        for case in self.plan():
            name, expected, ssl_section, tls_section, user = case[:5]
            host = case[5] if len(case) > 5 and case[5] else "127.0.0.1"
            settings = case[6] if len(case) > 6 else {}
            try:
                with source_tls(**settings):
                    yield self.fetch(name, expected, ssl_section,
                                     tls_section, user, host)
            except (harness.Failure, subprocess.SubprocessError, OSError,
                    RuntimeError) as failure:
                yield {"case": "source_" + name, "ok": False,
                       "why": harness.describe_failure(failure, 400)}


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--mysql-basedir", type=Path, required=True)
    parser.add_argument("--binsrv", type=Path, required=True)
    parser.add_argument("--workdir", type=Path, required=True)
    parser.add_argument("--port", type=int, default=23680)
    parser.add_argument("--mode", choices=["gtid", "position"],
                        default="position")
    parser.add_argument("--sides", default="listener,source",
                        help="'listener', 'source' or both (the default)")
    args = parser.parse_args()
    args.mysql_basedir = args.mysql_basedir.expanduser()
    args.workdir = args.workdir.expanduser().resolve()
    args.workdir.mkdir(parents=True, exist_ok=True)
    gtid_mode = args.mode == "gtid"
    certificates = upstream.make_certificates(args.workdir / "certs")
    certificates |= make_more_certificates(args.workdir / "certs")
    upstream.TlsSource.certificates = certificates
    harness.SERVER = upstream.TlsSource(
        args.mysql_basedir, args.workdir / "mysql", args.port, gtid_mode,
        server_id=1)
    binsrv = harness.BINSRV = harness.BinlogServer(
        args.binsrv.expanduser(), args.workdir / "binsrv", args.port,
        gtid_mode)
    results = []

    def record(result: dict):
        print(json.dumps(result), flush=True)
        results.append(result)

    listener_cases = ListenerCases(binsrv, args.port + 2)
    harness.SERVER.start()
    try:
        machine = harness.FetchCrashMachine()
        machine.start_from_scratch()
        machine.run_workload(WORKLOAD)
        if "listener" in args.sides:
            binsrv.checkpoint_size = "1"
            machine.fetch_to_the_end()
            listener_cases.restart()
            for case in (listener_cases.protocol_versions,
                         listener_cases.cipher_suites,
                         listener_cases.renegotiation,
                         listener_cases.compression,
                         listener_cases.certificate,
                         listener_cases.session_resumption,
                         listener_cases.plain_downgrade,
                         listener_cases.legacy_openssl_policy):
                try:
                    if not listener_cases.alive():
                        listener_cases.restart()
                    record(case())
                except (harness.Failure, subprocess.SubprocessError, OSError,
                        ValueError, KeyError, IndexError,
                        struct.error) as failure:
                    record({"case": "listener_" + case.__name__, "ok": False,
                            "why": harness.describe_failure(failure, 400)})
            listener_cases.stop()
            binsrv.checkpoint_size = None
        if "source" in args.sides:
            for result in SourceCases(args, certificates).run():
                record(result)
    finally:
        listener_cases.stop()
        harness.SERVER.stop()
    failures = [result for result in results if not result.get("ok")]
    print(json.dumps({
        "cases": len(results), "failures": len(failures),
        "refusals_the_log_does_not_explain": sum(
            1 for result in results if result.get("log_says_why") is False),
        "recorded_only": [result["case"] + ": " + result.get(
            "outcome", result.get("floor_set_by", ""))
            for result in results if result.get("recorded_only")],
        "passed": not failures}), flush=True)
    (args.workdir / "result.json").write_text(json.dumps(results, indent=2))
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
