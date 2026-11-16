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
- 'guess_rate': how fast wrong passwords can be tried. For about 10 s each,
  one client and then 8 clients in parallel log in as the configured user
  with a wrong password, first over the full authentication (no client has
  logged in: every guess is an RSA-OAEP encrypted password) and then, after
  one legitimate login, over the fast path (a wrong scramble). The result
  always holds the measured rates (per second, in total and per second of
  the run), the latency of a server greeting or of a legitimate login made
  every half second meanwhile, what 'pull' logged and how its memory and
  open files changed. MySQL has no lockout by default either, so the lack
  of one is recorded ('throttled': false) and does not fail the case. It
  fails if a wrong password is accepted, a legitimate login fails or takes
  longer than LEGITIMATE_SECONDS during or after the attempts, 'pull' ends,
  the refusals leave nothing in the log, or 'pull' keeps more file
  descriptors open afterwards than before.
- 'rsa_exchange': the non-TLS full authentication. Malformed answers to
  "perform full authentication" (an empty packet, ciphertexts of a wrong
  length, all zeros, all 0xff, random bytes of the key size, valid
  encryptions of a wrong password, of the right password XORed with a wrong
  nonce or with none, or without its NUL, a second and a 50-fold public
  key request, a wrong sequence number) must each be refused (ERR or a
  closed connection, never OK) with 'pull' alive. The time from the
  ciphertext to the refusal is measured TIMING_TRIES times in each of two
  rounds for padding-invalid (random) and padding-valid (wrong password,
  and unterminated) ciphertexts; the medians are recorded, and the case
  fails only if both rounds show the same clear difference (more than
  TIMING_CLEAR_SECONDS and 25 %) or the replies differ. The nonces of all
  these connections must not repeat. Then a valid login is captured and
  replayed: its scramble on the fast path of the same 'pull', and its
  ciphertext on the full path of a restarted 'pull' (the RSA key is the
  same); both must be refused, and the right login must then work.
- 'plain_password_without_tls': what else the login accepts. Without an
  'ssl' section: the password in plain (as a TLS client sends it) on the
  full path, and a client that sets CLIENT_SSL, must be refused. With an
  'ssl' section: the plain password is refused outside TLS and accepted
  inside it only when it is exactly the password and its NUL (empty, no
  NUL, a second NUL, trailing bytes, a key request are refused); user names
  with trailing bytes or another case, over-long user names, responses and
  plugin names (up to 1 MB) are refused with 'pull' alive (the size of
  the reply and of what is logged is recorded); a line feed in a user name
  must not start a log line the client chose; a client naming
  mysql_native_password, sha256_password, mysql_clear_password or no plugin
  gets an auth switch with a new nonce, after which a native-password hash
  or the clear password is refused and only the caching_sha2_password
  answer to the new nonce is accepted; an SSLRequest followed by a plain
  greeting is not served; an auth packet after the OK does not end 'pull'.
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
import base64
import collections
import hashlib
import json
import multiprocessing
import os
import signal
import socket
import statistics
import struct
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import fetch_crash_test as harness  # noqa: E402
import listener_fuzz_test as listener  # noqa: E402

WORKLOAD = [{"rows": [200, 2000], "ddl": True}]
OTHER_USER = "somebody_else"
CASES = ("user_name_probe", "guess_rate", "rsa_exchange",
         "plain_password_without_tls", "long_statement")
STATEMENTS = (
    [(f"SET and {count} spaces", b"SET" + b" " * count)
     for count in (1_000, 20_000, 100_000, 1_000_000)] +
    [(f"a heartbeat period of {count} digits",
      b"SET @master_heartbeat_period=" + b"1" * count)
     for count in (100_000, 1_000_000)])


def start_pull(binsrv, pull_port: int,
               ssl: "dict | None" = None) -> subprocess.Popen:
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
    if ssl is not None:
        config["replication_source"]["ssl"] = ssl
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


# ---------------------------------------------------------------------------
# the strength of the login: guess_rate, rsa_exchange,
# plain_password_without_tls
# ---------------------------------------------------------------------------

GUESS_SECONDS = 10.0
GUESS_CLIENTS = 8
LEGITIMATE_SECONDS = 5.0
TIMING_TRIES = 200
TIMING_CLEAR_SECONDS = 0.0002
KEY_REQUESTS = 200
CACHING_SHA2 = b"caching_sha2_password"
PASSWORD = listener.PULL_PASSWORD.encode()
AUTH_FAILED = b"client authentication failed"
# what a client puts after a line feed in its user name: if the log has it
# as a line of its own, a client that cannot log in writes the log
FORGED_LINE = (b"[2026-01-01T00:00:00.000000] [   info] net    : client "
               b"authentication succeeded for " + listener.PULL_USER.encode())


class Pull:
    """The running 'pull', and how to start it anew: a new 'pull' has an
    empty authentication cache, so the next login of the configured user is
    a full authentication."""

    def __init__(self, binsrv, port: int):
        self.binsrv, self.port, self.process = binsrv, port, None

    def alive(self) -> bool:
        return self.process is not None and self.process.poll() is None

    def restart(self, tls: "dict | None" = None):
        self.stop()
        self.process = start_pull(self.binsrv, self.port, tls)

    def stop(self):
        if self.process is not None:
            stop_pull(self.process)
            self.process = None

    def open_files(self) -> int:
        return len(os.listdir(f"/proc/{self.process.pid}/fd"))

    def resident_kb(self) -> int:
        for line in Path(f"/proc/{self.process.pid}/status").read_text(
                ).splitlines():
            if line.startswith("VmRSS:"):
                return int(line.split()[1])
        return 0


class Dialogue:
    """One connection to the listener: reads the server greeting, sends
    frames with the sequence number the listener expects next and names
    each reply."""

    def __init__(self, port: int, timeout: float = 10.0,
                 reset_on_close: bool = False):
        self.timeout = timeout
        self.client = listener.Listener("127.0.0.1", port, timeout=timeout)
        if reset_on_close:
            # no TIME_WAIT socket per attempt on this (shared) host
            self.client.sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER,
                                        struct.pack("ii", 1, 0))
        self.salt = None
        self.capabilities = 0
        self.sequence = 1
        self.secure = False
        self.body = b""
        read = self.client.read_frame()
        if read is not None:
            self._parse_greeting(read[1])

    def _parse_greeting(self, payload: bytes):
        # protocol(1) version(NUL) connid(4) salt1(8) filler(1) caps-low(2)
        # charset(1) status(2) caps-high(2) auth-data-len(1) reserved(10)
        # salt2(12) NUL plugin NUL
        try:
            offset = payload.index(b"\x00", 1) + 1 + 4
            salt1 = payload[offset:offset + 8]
            offset += 9
            low = struct.unpack_from("<H", payload, offset)[0]
            high = struct.unpack_from("<H", payload, offset + 5)[0]
            self.capabilities = low | high << 16
            offset += 2 + 1 + 2 + 2 + 1 + 10
            self.salt = salt1 + payload[offset:offset + 12]
        except (ValueError, struct.error):
            self.salt = None

    def tls(self) -> bool:
        """SSLRequest and the TLS handshake."""
        if not self.client.upgrade_to_tls(self.sequence):
            return False
        self.sequence += 1
        self.secure = True
        return True

    def greet(self, user: bytes = listener.PULL_USER.encode(),
              auth: "bytes | None" = None, plugin: bytes = CACHING_SHA2,
              capabilities: "int | None" = None,
              password: str = listener.PULL_PASSWORD) -> str:
        """The client greeting; by default with the scramble of 'password'
        for the nonce of the server greeting."""
        if auth is None:
            auth = listener.scramble(password, self.salt)
        if capabilities is None:
            capabilities = listener.BASE_CAPABILITIES | (
                listener.CLIENT_SSL if self.secure else 0)
        return self.say(listener.greeting_payload(capabilities, user, auth,
                                                  plugin))

    def say(self, payload: bytes, sequence: "int | None" = None) -> str:
        self.client.send(listener.frame(
            self.sequence if sequence is None else sequence, payload))
        return self.reply()

    def reply(self) -> str:
        started = time.monotonic()
        read = self.client.read_frame()
        if read is None:
            self.body = b""
            return (f"no reply in {self.timeout:g} s"
                    if time.monotonic() - started >= self.timeout - 0.05
                    else "closed")
        self.sequence = read[0] + 1
        self.body = body = read[1]
        if body[:1] == b"\xff":
            return f"ERR {int.from_bytes(body[1:3], 'little')}"
        if body[:1] == b"\x00":
            return "OK"
        if body == b"\x01\x04":
            return "full authentication"
        if body == b"\x01\x03":
            return "fast authentication"
        if body[:1] == b"\x01" and b"PUBLIC KEY" in body:
            return "public key"
        if body[:1] == b"\xfe":
            return "auth switch"
        return "bytes " + body[:8].hex()

    def switch(self) -> "tuple[bytes, bytes]":
        """The plugin name and the nonce of an auth switch request (0xfe,
        the name, NUL, the nonce, NUL)."""
        name, _, data = self.body[1:].partition(b"\x00")
        return name, data[:20]

    def close(self):
        self.client.close()


def _tlv(data: bytes, offset: int) -> "tuple[int, bytes, int]":
    """A DER element at 'offset': its tag, its value and the next offset."""
    tag, length = data[offset], data[offset + 1]
    offset += 2
    if length & 0x80:
        count = length & 0x7F
        length = int.from_bytes(data[offset:offset + count], "big")
        offset += count
    return tag, data[offset:offset + length], offset + length


def public_key_numbers(pem: bytes) -> "tuple[int, int]":
    """The modulus and the exponent of a PEM SubjectPublicKeyInfo RSA key."""
    der = base64.b64decode(b"".join(
        line for line in pem.replace(b"\x00", b"").split(b"\n")
        if line and not line.startswith(b"-----")))
    _, info, _ = _tlv(der, 0)
    _, _, offset = _tlv(info, 0)      # the algorithm
    _, bits, _ = _tlv(info, offset)   # BIT STRING: a byte of unused bits
    _, key, _ = _tlv(bits, 1)
    _, modulus, offset = _tlv(key, 0)
    _, exponent, _ = _tlv(key, offset)
    return int.from_bytes(modulus, "big"), int.from_bytes(exponent, "big")


def _mgf1(seed: bytes, length: int) -> bytes:
    output = b""
    counter = 0
    while len(output) < length:
        output += hashlib.sha1(seed + counter.to_bytes(4, "big")).digest()
        counter += 1
    return output[:length]


def xor(data: bytes, mask: bytes) -> bytes:
    """'data' XOR 'mask', the mask repeated."""
    return bytes(byte ^ mask[index % len(mask)]
                 for index, byte in enumerate(data))


def oaep_encrypt(key: "tuple[int, int]", message: bytes) -> bytes:
    """RSA-OAEP (SHA-1, MGF1-SHA-1, no label), as caching_sha2_password
    uses it; in Python, as a guessing client would not start a process per
    password."""
    modulus, exponent = key
    size = (modulus.bit_length() + 7) // 8
    block = (hashlib.sha1(b"").digest() +
             b"\x00" * (size - len(message) - 42) + b"\x01" + message)
    seed = os.urandom(20)
    masked_block = xor(block, _mgf1(seed, size - 21))
    masked_seed = xor(seed, _mgf1(masked_block, 20))
    return pow(int.from_bytes(masked_seed + masked_block, "big"), exponent,
               modulus).to_bytes(size, "big")


def fetch_public_key(port: int, user: bytes = OTHER_USER.encode()
                     ) -> "bytes | None":
    """The PEM public key the listener hands out on request. By default
    asked as another user, who always gets the full authentication."""
    dialogue = Dialogue(port)
    try:
        if dialogue.salt is None or \
                dialogue.greet(user=user, auth=os.urandom(32)) != \
                "full authentication" or \
                dialogue.say(b"\x02") != "public key":
            return None
        return dialogue.body[1:]
    finally:
        dialogue.close()


def login(port: int, tls: bool = False) -> "tuple[bool, float]":
    """A correct login of the configured user over whichever path the
    listener chooses; whether it reached the OK, and the seconds it took."""
    started = time.monotonic()
    try:
        dialogue = Dialogue(port)
    except OSError:
        return False, time.monotonic() - started
    try:
        reply = "no server greeting"
        if dialogue.salt is not None and (not tls or dialogue.tls()):
            reply = dialogue.greet()
            if reply == "fast authentication":
                reply = dialogue.reply()
            elif reply == "full authentication" and tls:
                reply = dialogue.say(PASSWORD + b"\x00")
            elif reply == "full authentication" and \
                    dialogue.say(b"\x02") == "public key":
                reply = dialogue.say(oaep_encrypt(
                    public_key_numbers(dialogue.body[1:]),
                    xor(PASSWORD + b"\x00", dialogue.salt)))
        return reply == "OK", time.monotonic() - started
    finally:
        dialogue.close()


def greeting_probe(port: int) -> "tuple[bool, float]":
    """Whether a new connection gets the server greeting, and how fast."""
    started = time.monotonic()
    try:
        dialogue = Dialogue(port)
    except OSError:
        return False, time.monotonic() - started
    dialogue.close()
    return dialogue.salt is not None, time.monotonic() - started


def guess_worker(port: int, path: str, key: "tuple[int, int]", queue):
    """Wrong passwords for the configured user for GUESS_SECONDS, one
    connection each: over the fast path a wrong scramble, over the full
    path also the guess, RSA-OAEP encrypted."""
    per_second = collections.Counter()
    replies = collections.Counter()
    accepted = []
    number = 0
    started = time.monotonic()
    while (elapsed := time.monotonic() - started) < GUESS_SECONDS:
        number += 1
        guess = f"guess-{os.getpid()}-{number}"
        try:
            dialogue = Dialogue(port, reset_on_close=True)
        except OSError as error:
            replies[f"connect failed: {error.strerror}"] += 1
            time.sleep(0.01)
            continue
        try:
            if dialogue.salt is None:
                reply = "no server greeting"
            else:
                reply = dialogue.greet(password=guess)
                if reply == "full authentication" and path == "full":
                    reply = "full authentication, " + dialogue.say(
                        oaep_encrypt(key, xor(guess.encode() + b"\x00",
                                              dialogue.salt)))
                if reply.endswith("OK") or reply == "fast authentication":
                    accepted.append(guess)
        finally:
            dialogue.close()
        replies[reply] += 1
        per_second[int(elapsed)] += 1
    queue.put((dict(per_second), dict(replies), accepted,
               time.monotonic() - started))


def flood(port: int, path: str, clients: int, key: "tuple[int, int]",
          probe, probe_name: str) -> dict:
    """'clients' guess_worker processes; meanwhile 'probe' every half
    second."""
    context = multiprocessing.get_context("fork")
    queue = context.Queue()
    workers = [context.Process(target=guess_worker,
                               args=(port, path, key, queue))
               for _ in range(clients)]
    started = time.monotonic()
    for worker in workers:
        worker.start()
    latencies, failed = [], 0
    while time.monotonic() - started < GUESS_SECONDS - 0.5:
        time.sleep(0.5)
        served, seconds = probe(port)
        latencies.append(seconds)
        failed += not served
    outcomes = [queue.get(timeout=GUESS_SECONDS + 120) for _ in workers]
    for worker in workers:
        worker.join()
    per_second = collections.Counter()
    replies = collections.Counter()
    accepted = []
    for seconds, kinds, guesses, _ in outcomes:
        per_second.update({int(second): count
                           for second, count in seconds.items()})
        replies.update(kinds)
        accepted += guesses
    attempts = sum(per_second.values())
    by_second = [per_second.get(second, 0)
                 for second in range(int(GUESS_SECONDS))]
    return {"clients": clients, "attempts": attempts,
            "per_second": round(attempts / max(
                outcome[3] for outcome in outcomes)),
            "per_second_of_the_run": by_second,
            "replies": dict(replies), "accepted_guesses": accepted[:5],
            "probe": probe_name, "probes": len(latencies),
            "probes_failed": failed,
            "probe_ms_median": round(statistics.median(latencies) * 1000, 1),
            "probe_ms_max": round(max(latencies) * 1000, 1),
            "slowed_down": sum(by_second[-2:]) < 0.5 * sum(by_second[:2])}


def case_guess_rate(pull: Pull) -> dict:
    pull.restart()
    port = pull.port
    log = pull.binsrv.log_path
    problems = []
    pem = fetch_public_key(port)
    if pem is None:
        return {"case": "guess_rate", "ok": False,
                "why": "the listener did not hand out its public key"}
    key = public_key_numbers(pem)
    log_start = log.stat().st_size
    files_before, memory_before = pull.open_files(), pull.resident_kb()
    phases = {}
    expected = {}
    for clients in (1, GUESS_CLIENTS):
        name = f"full_path_{clients}_client" + ("s" if clients > 1 else "")
        phases[name] = flood(port, "full", clients, key, greeting_probe,
                             "a server greeting")
        expected[name] = "full authentication, ERR 1045"
    # the first legitimate login: from here on the configured user is in
    # the authentication cache and gets the fast path
    first_login, first_seconds = login(port)
    for clients in (1, GUESS_CLIENTS):
        name = f"fast_path_{clients}_client" + ("s" if clients > 1 else "")
        phases[name] = flood(port, "fast", clients, key, login,
                             "a legitimate login")
        expected[name] = "ERR 1045"
    last_login, last_seconds = login(port)
    time.sleep(2.0)
    alive = pull.alive()
    files_after = pull.open_files() if alive else None
    memory_after = pull.resident_kb() if alive else None
    with log.open("rb") as stream:
        stream.seek(log_start)
        written = stream.read()
    attempts = sum(phase["attempts"] for phase in phases.values())
    failures_logged = written.count(AUTH_FAILED)

    if not alive:
        problems.append(f"pull ended (exit {pull.process.returncode})")
    for name, phase in phases.items():
        if phase["accepted_guesses"]:
            problems.append(f"{name}: a wrong password was accepted "
                            f"({phase['accepted_guesses'][0]})")
        if phase["probes_failed"]:
            problems.append(f"{name}: {phase['probes_failed']} of "
                            f"{phase['probes']} times {phase['probe']} "
                            "failed during the attempts")
        if phase["probe_ms_max"] > LEGITIMATE_SECONDS * 1000:
            problems.append(f"{name}: {phase['probe']} took "
                            f"{phase['probe_ms_max']} ms during the attempts")
        unclean = sum(count for reply, count in phase["replies"].items()
                      if reply != expected[name] and
                      not reply.startswith("connect failed"))
        phase["not_refused_with_err_1045"] = unclean
        if unclean > 0.01 * phase["attempts"]:
            problems.append(f"{name}: {unclean} of {phase['attempts']} "
                            f"attempts were not answered with "
                            f"'{expected[name]}': {phase['replies']}")
    if not first_login or not last_login:
        problems.append("a legitimate login failed after the attempts")
    if max(first_seconds, last_seconds) > LEGITIMATE_SECONDS:
        problems.append("a legitimate login after the attempts took "
                        f"{max(first_seconds, last_seconds):.1f} s")
    if attempts and not failures_logged:
        problems.append(f"{attempts} refused logins left no "
                        f"'{AUTH_FAILED.decode()}' line in the log")
    if alive and files_after > files_before + 8:
        problems.append(f"pull holds {files_after} open files after the "
                        f"attempts, {files_before} before")
    result = {
        "case": "guess_rate", "seconds_per_phase": GUESS_SECONDS,
        "phases": phases,
        "throttled": any(phase["slowed_down"] for phase in phases.values()),
        "locked_out": not (first_login and last_login),
        "legitimate_login_ms_after_full_path": round(first_seconds * 1000, 1),
        "legitimate_login_ms_at_the_end": round(last_seconds * 1000, 1),
        "attempts": attempts, "failures_logged": failures_logged,
        "log_bytes_per_attempt": round(len(written) / max(attempts, 1)),
        "log_megabytes": round(len(written) / 1e6, 1),
        "open_files": [files_before, files_after],
        "resident_kb": [memory_before, memory_after],
        "ok": not problems}
    if problems:
        result["why"] = "; ".join(problems)
    return result


def refused(reply: str) -> bool:
    """Not logged in: the (last) reply is an error packet, a closed
    connection or silence."""
    return reply.split(", ")[-1].removeprefix("then ").startswith(
        ("ERR", "closed", "no reply", "TLS handshake failed",
         "no server greeting"))


def case_rsa_exchange(pull: Pull) -> dict:
    pull.restart()
    port = pull.port
    problems = []
    pem = fetch_public_key(port, listener.PULL_USER.encode())
    if pem is None:
        return {"case": "rsa_exchange", "ok": False,
                "why": "the listener did not hand out its public key"}
    key = public_key_numbers(pem)
    size = (key[0].bit_length() + 7) // 8
    nonces = []

    def answer(make, key_request: bool = False, repeat: int = 1
               ) -> "tuple[str, bytes, float]":
        """A full authentication of the configured user answered with
        'make(nonce)'; the reply, its bytes and the seconds it took."""
        dialogue = Dialogue(port)
        try:
            nonces.append(dialogue.salt)
            reply = dialogue.greet(auth=os.urandom(32))
            if reply != "full authentication":
                return "instead of full authentication: " + reply, b"", 0.0
            if key_request and dialogue.say(b"\x02") != "public key":
                return "no public key", b"", 0.0
            payload = make(dialogue.salt)
            frames = b"".join(
                listener.frame(dialogue.sequence + 2 * index, payload)
                for index in range(repeat))
            started = time.perf_counter()
            dialogue.client.send(frames)
            reply = dialogue.reply()
            seconds = time.perf_counter() - started
            for _ in range(repeat):  # until the listener refuses
                if reply != "public key":
                    break
                reply = dialogue.reply()
            return reply, dialogue.body, seconds
        finally:
            dialogue.close()

    def below_modulus(_nonce: bytes) -> bytes:
        return (int.from_bytes(os.urandom(size + 8), "big") %
                key[0]).to_bytes(size, "big")

    malformed = [
        ("an empty packet", lambda nonce: b"", False, 1),
        ("one byte (0x01)", lambda nonce: b"\x01", False, 1),
        ("random bytes, one fewer than the key size",
         lambda nonce: os.urandom(size - 1), False, 1),
        ("random bytes, one more than the key size",
         lambda nonce: os.urandom(size + 1), False, 1),
        ("random bytes, twice the key size",
         lambda nonce: os.urandom(2 * size), False, 1),
        ("all zeros of the key size", lambda nonce: bytes(size), False, 1),
        ("all 0xff of the key size (above the modulus)",
         lambda nonce: b"\xff" * size, False, 1),
        ("random bytes of the key size", below_modulus, False, 1),
        ("a valid encryption of a wrong password",
         lambda nonce: oaep_encrypt(key, xor(b"wrong-password\x00", nonce)),
         False, 1),
        ("the right password XOR a wrong nonce",
         lambda nonce: oaep_encrypt(key, xor(PASSWORD + b"\x00",
                                             os.urandom(20))), False, 1),
        ("the right password, not XORed",
         lambda nonce: oaep_encrypt(key, PASSWORD + b"\x00"), False, 1),
        ("the right password XOR the nonce, without its NUL",
         lambda nonce: oaep_encrypt(key, xor(PASSWORD, nonce)), False, 1),
        ("the right password XOR the nonce, encrypted without OAEP",
         lambda nonce: pow(int.from_bytes(xor(PASSWORD + b"\x00", nonce),
                                          "big"), key[1],
                           key[0]).to_bytes(size, "big"), False, 1),
        ("a second public key request", lambda nonce: b"\x02", True, 1),
        ("50 public key requests in one write", lambda nonce: b"\x02",
         False, 50),
    ]
    attempts = []
    for label, make, key_request, repeat in malformed:
        reply, _, _ = answer(make, key_request, repeat)
        alive = pull.alive()
        attempts.append({"sent": label, "reply": reply, "pull_alive": alive})
        if not refused(reply):
            problems.append(f"{label} was answered with '{reply}'")
        if not alive:
            problems.append(f"pull ended (exit {pull.process.returncode}) "
                            f"after {label}")
            break

    # the key, asked for again and again
    keys = set()
    if pull.alive():
        for _ in range(KEY_REQUESTS):
            keys.add(fetch_public_key(port, listener.PULL_USER.encode()))
        if keys != {pem}:
            problems.append(f"{KEY_REQUESTS} public key requests were "
                            f"answered with {len(keys)} different replies")

    # padding-invalid against padding-valid ciphertexts
    classes = {
        "padding_invalid_random": below_modulus,
        "padding_valid_wrong_password":
            lambda nonce: oaep_encrypt(key, xor(b"wrong-password\x00",
                                                nonce)),
        "padding_valid_unterminated":
            lambda nonce: oaep_encrypt(key, xor(b"wrong-password", nonce)),
        "wrong_length": lambda nonce: os.urandom(size - 1)}
    rounds = []
    replies = {name: set() for name in classes}
    names = list(classes)
    for _ in range(2):
        samples = {name: [] for name in classes}
        for index in range(TIMING_TRIES if pull.alive() else 0):
            for offset in range(len(names)):
                name = names[(index + offset) % len(names)]
                reply, body, seconds = answer(classes[name])
                samples[name].append(seconds)
                replies[name].add(f"{reply} {body.hex()}")
        rounds.append({name: round(statistics.median(values) * 1e6)
                       for name, values in samples.items() if values})
    timing = {"tries_per_class_and_round": TIMING_TRIES,
              "median_microseconds": rounds}
    if len(rounds) == 2 and all(len(one) == len(names) for one in rounds):
        for name in ("padding_valid_wrong_password",
                     "padding_valid_unterminated"):
            deltas = [one[name] - one["padding_invalid_random"]
                      for one in rounds]
            clear = [abs(delta) > max(
                TIMING_CLEAR_SECONDS * 1e6,
                0.25 * min(one[name], one["padding_invalid_random"]))
                for delta, one in zip(deltas, rounds)]
            timing[f"{name}_minus_invalid_microseconds"] = deltas
            if all(clear) and deltas[0] * deltas[1] > 0:
                problems.append(
                    f"the refusal of {name} ciphertexts takes "
                    f"{deltas[0]} and {deltas[1]} microseconds longer "
                    "(median, two rounds) than that of random ones: the "
                    "time tells whether the padding was valid")
    distinct = {name: sorted(kinds) for name, kinds in replies.items()}
    timing["replies"] = {name: [kind[:60] for kind in kinds]
                         for name, kinds in distinct.items()}
    same_size = [distinct[name] for name in names[:3]]
    if any(kinds != same_size[0] or len(kinds) != 1 for kinds in same_size):
        problems.append("the replies to padding-valid and padding-invalid "
                        f"ciphertexts differ: {timing['replies']}")
    for name, kinds in distinct.items():
        if any(not refused(kind) for kind in kinds):
            problems.append(f"a {name} ciphertext was not refused")

    seen = [nonce for nonce in nonces if nonce is not None]
    nonce_report = {"connections": len(seen),
                    "distinct": len(set(seen)),
                    "all_20_bytes": all(len(nonce) == 20 for nonce in seen),
                    "distinct_byte_values": len(set(b"".join(seen)))}
    if len(set(seen)) != len(seen):
        problems.append(f"{len(seen) - len(set(seen))} of {len(seen)} "
                        "server nonces were repeats")

    # a valid login, captured ...
    replay = {}
    captured = None
    if pull.alive():
        dialogue = Dialogue(port)
        try:
            nonce = dialogue.salt
            scramble = listener.scramble(listener.PULL_PASSWORD, nonce)
            ciphertext = oaep_encrypt(key, xor(PASSWORD + b"\x00", nonce))
            reply = dialogue.greet(auth=scramble)
            if reply == "full authentication":
                reply = dialogue.say(b"\x02")
            if reply == "public key":
                reply = dialogue.say(ciphertext)
            replay["captured_login"] = reply
            if reply == "OK":
                captured = (scramble, ciphertext)
            else:
                problems.append("the login to capture was answered with "
                                f"'{reply}'")
        finally:
            dialogue.close()
    if captured is not None:
        # ... and replayed: the scramble on the fast path of the same pull
        dialogue = Dialogue(port)
        try:
            reply = dialogue.greet(auth=captured[0])
            if reply == "fast authentication":
                reply += ", " + dialogue.reply()
            elif reply == "full authentication":
                reply += ", " + dialogue.say(captured[1])
            replay["scramble_on_the_fast_path"] = reply
            if not refused(reply):
                problems.append("a replayed scramble was answered with "
                                f"'{reply}'")
        finally:
            dialogue.close()
        # the ciphertext on the full path of a new pull with the same key
        pull.restart()
        for label, key_request in (("ciphertext_on_the_full_path", False),
                                   ("ciphertext_after_a_key_request", True)):
            dialogue = Dialogue(port)
            try:
                reply = dialogue.greet(auth=captured[0])
                if reply == "full authentication" and key_request:
                    reply = dialogue.say(b"\x02")
                    if reply == "public key" and dialogue.body[1:] != pem:
                        replay["key_changed_at_restart"] = True
                if reply in ("full authentication", "public key"):
                    reply = "full authentication, " + \
                        dialogue.say(captured[1])
                replay[label] = reply
                if not refused(reply):
                    problems.append("a replayed ciphertext was answered "
                                    f"with '{reply}'")
            finally:
                dialogue.close()
        # MySQL takes the decrypted password up to its first NUL; recorded
        reply, _, _ = answer(lambda nonce: oaep_encrypt(
            key, xor(PASSWORD + b"\x00trailing\x00", nonce)))
        replay["right_password_nul_and_more_bytes"] = reply
        served, _ = login(port)
        replay["right_login_afterwards"] = served
        if not served:
            problems.append("the right login failed after the replays")
    if not pull.alive():
        problems.append(f"pull ended (exit {pull.process.returncode})")
    result = {"case": "rsa_exchange", "key_bits": key[0].bit_length(),
              "malformed": attempts, "key_requests": KEY_REQUESTS,
              "timing": timing, "nonces": nonce_report, "replay": replay,
              "ok": not problems}
    if problems:
        result["why"] = "; ".join(problems)
    return result


def native_password_hash(password: bytes, nonce: bytes) -> bytes:
    """What mysql_native_password answers: SHA1(password) XOR
    SHA1(nonce . SHA1(SHA1(password)))."""
    first = hashlib.sha1(password).digest()
    return xor(first, hashlib.sha1(
        nonce + hashlib.sha1(first).digest()).digest())


def case_plain_password_without_tls(pull: Pull) -> dict:
    port = pull.port
    user = listener.PULL_USER.encode()
    checks = []

    def check(name: str, reply: str, expected: "str | None"):
        """'expected': 'refused', 'accepted', a reply, or None for a reply
        that is only recorded."""
        entry = {"check": name, "reply": reply, "pull_alive": pull.alive()}
        if expected is not None:
            entry["expected"] = expected
            entry["ok"] = pull.alive() and (
                refused(reply) if expected == "refused" else
                reply.endswith("OK") if expected == "accepted" else
                reply == expected)
        elif not pull.alive():
            entry["ok"] = False
        checks.append(entry)

    def full(tls: bool, answer: bytes, name: bytes = user,
             timeout: float = 10.0) -> str:
        """A login whose answer to "perform full authentication" is
        'answer'."""
        dialogue = Dialogue(port, timeout)
        try:
            if dialogue.salt is None:
                return "no server greeting"
            if tls and not dialogue.tls():
                return "TLS handshake failed"
            reply = dialogue.greet(user=name)
            if reply == "full authentication":
                reply = dialogue.say(answer)
            elif reply == "fast authentication":
                reply += ", " + dialogue.reply()
            return reply
        finally:
            dialogue.close()

    def switched(tls: bool, plugin: bytes, first: bytes, make,
                 then=None) -> "tuple[str, bool]":
        """A greeting naming 'plugin' with the response 'first'; after the
        auth switch 'make(new nonce, greeting nonce)' is the answer, and
        'then(nonce, dialogue)' that to a full authentication. The final
        reply, and whether the switch came with a new nonce."""
        dialogue = Dialogue(port)
        try:
            if tls and not dialogue.tls():
                return "TLS handshake failed", False
            reply = dialogue.greet(auth=first, plugin=plugin)
            if reply != "auth switch":
                return "instead of an auth switch: " + reply, False
            name, nonce = dialogue.switch()
            if name != CACHING_SHA2:
                return "auth switch to " + name.decode(errors="replace"), \
                    False
            reply = dialogue.say(make(nonce, dialogue.salt))
            if reply == "fast authentication":
                reply += ", " + dialogue.reply()
            elif reply == "full authentication" and then is not None:
                reply = "full authentication, " + then(nonce, dialogue)
            return reply, nonce != dialogue.salt and len(nonce) == 20
        finally:
            dialogue.close()

    def rsa_answer(use_new_nonce: bool):
        def then(nonce: bytes, dialogue: Dialogue) -> str:
            if dialogue.say(b"\x02") != "public key":
                return "no public key"
            return dialogue.say(oaep_encrypt(
                public_key_numbers(dialogue.body[1:]),
                xor(PASSWORD + b"\x00",
                    nonce if use_new_nonce else dialogue.salt)))
        return then

    # --- no 'ssl' section -------------------------------------------------
    pull.restart()
    dialogue = Dialogue(port)
    dialogue.close()
    check("no ssl section: the server greeting offers CLIENT_SSL",
          str(bool(dialogue.capabilities & listener.CLIENT_SSL)), "False")
    check("no ssl section: full authentication answered with the password "
          "in plain and its NUL", full(False, PASSWORD + b"\x00"), "refused")
    check("no ssl section: ... without its NUL", full(False, PASSWORD),
          "refused")
    dialogue = Dialogue(port, 3.0)
    try:
        check("no ssl section: an SSLRequest", dialogue.say(
            listener.ssl_request_payload(listener.BASE_CAPABILITIES |
                                         listener.CLIENT_SSL)), "refused")
    finally:
        dialogue.close()
    dialogue = Dialogue(port, 3.0)
    try:
        reply = dialogue.greet(capabilities=listener.BASE_CAPABILITIES |
                               listener.CLIENT_SSL)
        if reply == "full authentication":
            reply += ", " + dialogue.say(PASSWORD + b"\x00")
        check("no ssl section: a full greeting with CLIENT_SSL, then the "
              "password in plain", reply, "refused")
    finally:
        dialogue.close()
    # the auth switch on the non-TLS full path
    for plugin, first, label, make in (
            (b"mysql_native_password", None, "a native-password hash",
             lambda nonce, old: native_password_hash(PASSWORD, nonce)),
            (b"sha256_password", b"\x01", "the password in plain",
             lambda nonce, old: PASSWORD + b"\x00"),
            (b"mysql_clear_password", PASSWORD + b"\x00",
             "the password in plain",
             lambda nonce, old: PASSWORD + b"\x00"),
            (b"", b"", "an empty response", lambda nonce, old: b""),
            (b"no_such_plugin", os.urandom(32), "32 random bytes",
             lambda nonce, old: os.urandom(32))):
        reply, new_nonce = switched(
            False, plugin, first if first is not None else os.urandom(20),
            make, lambda nonce, dialogue, make=make:
            dialogue.say(make(nonce, dialogue.salt)))
        shown = plugin.decode() or "no plugin name"
        check(f"no ssl section: {shown}, then {label} after the auth "
              "switch (and again for the full authentication)", reply,
              "refused")
        check(f"no ssl section: {shown}: the auth switch has a new nonce",
              str(new_nonce), "True")
    reply, _ = switched(
        False, b"mysql_native_password", os.urandom(20),
        lambda nonce, old: listener.scramble(listener.PULL_PASSWORD, nonce),
        rsa_answer(False))
    check("no ssl section: after an auth switch, the RSA answer made with "
          "the nonce of the server greeting (not the new one)", reply, None)
    reply, _ = switched(
        False, b"mysql_native_password", os.urandom(20),
        lambda nonce, old: listener.scramble(listener.PULL_PASSWORD, nonce),
        rsa_answer(True))
    check("no ssl section: after an auth switch, the right "
          "caching_sha2_password answers for the new nonce", reply,
          "accepted")

    # --- with an 'ssl' section --------------------------------------------
    pull.restart(listener.SSL)
    dialogue = Dialogue(port)
    dialogue.close()
    check("ssl section: the server greeting offers CLIENT_SSL",
          str(bool(dialogue.capabilities & listener.CLIENT_SSL)), "True")
    check("ssl section, no TLS: full authentication answered with the "
          "password in plain and its NUL", full(False, PASSWORD + b"\x00"),
          "refused")
    dialogue = Dialogue(port, 3.0)
    try:
        dialogue.client.send(listener.frame(1, listener.ssl_request_payload(
            listener.BASE_CAPABILITIES | listener.CLIENT_SSL)))
        dialogue.sequence = 2
        reply = dialogue.greet(capabilities=listener.BASE_CAPABILITIES |
                               listener.CLIENT_SSL)
        if reply == "full authentication":
            reply += ", " + dialogue.say(PASSWORD + b"\x00")
        check("ssl section: an SSLRequest, then no ClientHello but the "
              "greeting and the password in plain", reply, "refused")
    finally:
        dialogue.close()
    for label, answer, expected in (
            ("an empty packet", b"", "refused"),
            ("a NUL (the empty password)", b"\x00", "refused"),
            ("the password without its NUL", PASSWORD, "refused"),
            ("a wrong password and its NUL", b"wrong-password\x00",
             "refused"),
            ("a prefix of the password and its NUL", PASSWORD[:-1] + b"\x00",
             "refused"),
            ("the password, a NUL and one more byte", PASSWORD + b"\x00x",
             "refused"),
            ("the password and two NULs", PASSWORD + b"\x00\x00", None),
            ("the password, a NUL, bytes and a NUL",
             PASSWORD + b"\x00more\x00", None),
            ("a public key request (0x02)", b"\x02", "refused")):
        check(f"ssl section, in TLS: full authentication answered with "
              f"{label}", full(True, answer), expected)
    for label, name in (
            ("a trailing space", user + b" "),
            ("one more letter", user + b"x"),
            ("a leading space", b" " + user),
            ("upper case", user.upper()),
            ("a prefix of it", user[:-1]),
            ("a NUL and more bytes after it", user + b"\x00x"),
            ("a line feed and more after it", user + b"\n" + FORGED_LINE),
            ("no user name", b"")):
        check(f"ssl section, in TLS: the right password for the user name "
              f"with {label}", full(True, PASSWORD + b"\x00", name),
              "refused")
    log = pull.binsrv.log_path
    time.sleep(0.3)
    forged = [line for line in log.read_bytes().split(b"\n")
              if line == FORGED_LINE]
    check("ssl section: the log has a line of its own that the client "
          "chose (sent after a line feed in the user name)",
          str(bool(forged)), "False")
    for label, kwargs in (
            ("a user name of 300 bytes", {"user": b"A" * 300}),
            ("a user name of 70,000 bytes", {"user": b"A" * 70_000}),
            ("a user name of 1 MB", {"user": b"A" * 1_000_000}),
            ("a response of 70,000 bytes", {"auth": b"B" * 70_000}),
            ("a response of 1 MB", {"auth": b"B" * 1_000_000}),
            ("a plugin name of 70,000 bytes", {"plugin": b"C" * 70_000}),
            ("a plugin name of 1 MB", {"plugin": b"C" * 1_000_000})):
        log_before = log.stat().st_size
        dialogue = Dialogue(port)
        try:
            reply = dialogue.greet(**kwargs)
            if reply == "full authentication":
                reply = dialogue.say(b"\x01")
            elif reply == "auth switch":
                reply = dialogue.say(os.urandom(32))
                if reply == "full authentication":
                    reply = dialogue.say(b"\x01")
            reply += f" ({len(dialogue.body)} bytes)"
        finally:
            dialogue.close()
        time.sleep(0.3)
        check(f"ssl section, no TLS: {label}", reply, "refused")
        checks[-1]["log_bytes"] = log.stat().st_size - log_before
        served, _ = greeting_probe(port)
        if not served:
            check(f"a server greeting after {label}", "none", "a greeting")
    for tls in (False, True):
        where = "in TLS" if tls else "no TLS"
        reply, _ = switched(
            tls, b"mysql_native_password", os.urandom(20),
            lambda nonce, old: native_password_hash(PASSWORD, nonce),
            lambda nonce, dialogue: dialogue.say(
                native_password_hash(PASSWORD, nonce)))
        check(f"ssl section, {where}: mysql_native_password, then its hash "
              "after the auth switch (and again for the full "
              "authentication)", reply, "refused")
        reply, _ = switched(tls, b"mysql_clear_password", PASSWORD + b"\x00",
                            lambda nonce, old: PASSWORD + b"\x00")
        check(f"ssl section, {where}: mysql_clear_password, then the "
              "password in plain as the answer to the auth switch", reply,
              "full authentication")
    # the first login this pull accepts: the plain password inside TLS
    check("ssl section, in TLS: full authentication answered with the "
          "password and its NUL", full(True, PASSWORD + b"\x00"), "accepted")
    # from here on the configured user is in the authentication cache
    check("ssl section, no TLS, after that login: the right scramble (the "
          "fast path, as with MySQL)", full(False, b""), None)
    dialogue = Dialogue(port)
    try:
        reply = dialogue.greet(auth=os.urandom(32))
        if refused(reply):
            dialogue.client.send(listener.frame(dialogue.sequence,
                                                PASSWORD + b"\x00"))
            late = dialogue.reply()
            reply += ", then " + late
        check("ssl section, no TLS, after that login: a wrong scramble, "
              "then the password in plain", reply, "refused")
    finally:
        dialogue.close()
    for tls in (False, True):
        where = "in TLS" if tls else "no TLS"
        reply, _ = switched(
            tls, b"mysql_native_password", os.urandom(20),
            lambda nonce, old: listener.scramble(listener.PULL_PASSWORD,
                                                 old))
        check(f"ssl section, {where}, after that login: after an auth "
              "switch, the scramble for the nonce of the server greeting",
              reply, None)
        reply, _ = switched(
            tls, b"mysql_native_password", os.urandom(20),
            lambda nonce, old: listener.scramble(listener.PULL_PASSWORD,
                                                 nonce))
        check(f"ssl section, {where}, after that login: after an auth "
              "switch, the scramble for the new nonce", reply, "accepted")
    for tls in (False, True):
        where = "in TLS" if tls else "no TLS"
        for label, payload, sequence in (
                ("the password in plain with the next sequence number",
                 PASSWORD + b"\x00", None),
                ("a caching_sha2_password scramble as a new command",
                 None, 0),
                ("a second client greeting as a new command", b"", 0)):
            dialogue = Dialogue(port, 3.0)
            try:
                if tls and not dialogue.tls():
                    check(f"ssl section, {where}: TLS for {label}",
                          "TLS handshake failed", "accepted")
                    continue
                reply = dialogue.greet()
                if reply == "fast authentication":
                    reply = dialogue.reply()
                if reply != "OK":
                    check(f"ssl section, {where}: a login before {label}",
                          reply, "accepted")
                    continue
                if payload is None:
                    payload = listener.scramble(listener.PULL_PASSWORD,
                                                dialogue.salt)
                elif payload == b"":
                    payload = listener.greeting_payload(
                        listener.BASE_CAPABILITIES, OTHER_USER.encode(),
                        os.urandom(32), CACHING_SHA2)
                reply = dialogue.say(payload, sequence)
                time.sleep(0.2)
                check(f"ssl section, {where}: after the OK, {label}", reply,
                      None)
            finally:
                dialogue.close()
    served, _ = login(port, True)
    check("ssl section, in TLS: a login at the end", "OK" if served
          else "failed", "accepted")

    failed = [entry for entry in checks if entry.get("ok") is False]
    result = {"case": "plain_password_without_tls", "checks": checks,
              "ok": not failed}
    if failed:
        result["why"] = "; ".join(
            f"{entry['check']}: '{entry['reply']}'" +
            (f", expected {entry['expected']}" if "expected" in entry
             else "") + ("" if entry["pull_alive"] else ", pull ended")
            for entry in failed)[:3000]
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
    parser.add_argument("--cases", default=",".join(CASES),
                        help="comma-separated, run in the documented order")
    args = parser.parse_args()
    args.workdir.mkdir(parents=True, exist_ok=True)
    gtid_mode = args.mode == "gtid"
    harness.SERVER = harness.MySQLServer(args.mysql_basedir.expanduser(),
                                         args.workdir / "mysql", args.port,
                                         gtid_mode, server_id=1)
    binsrv = harness.BINSRV = harness.BinlogServer(
        args.binsrv.expanduser(), args.workdir / "binsrv", args.port,
        gtid_mode)
    pull = Pull(binsrv, args.port + 2)
    results = []
    harness.SERVER.start()
    try:
        machine = harness.FetchCrashMachine()
        machine.start_from_scratch()
        machine.run_workload(WORKLOAD)
        binsrv.checkpoint_size = "1"
        machine.fetch_to_the_end()
        chosen = args.cases.split(",")
        # the long statement goes last: it may end pull
        for name in (name for name in CASES if name in chosen):
            try:
                if name == "user_name_probe":
                    pull.restart()  # no client has logged in
                    result = case_user_name_probe(pull.port)
                elif name == "long_statement":
                    pull.restart()
                    result = case_long_statement(pull.process, pull.port)
                else:
                    result = globals()["case_" + name](pull)
            except (harness.Failure, subprocess.SubprocessError, OSError,
                    ValueError, TypeError, IndexError) as failure:
                result = {"case": name, "ok": False,
                          "why": harness.describe_failure(failure, 500)}
            print(json.dumps(result), flush=True)
            results.append(result)
    finally:
        pull.stop()
        harness.SERVER.stop()
    failures = [result for result in results if not result.get("ok")]
    print(json.dumps({"cases": len(results), "failures": len(failures),
                      "passed": not failures}), flush=True)
    (args.workdir / "result.json").write_text(json.dumps(results, indent=2))
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
