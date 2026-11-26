#!/usr/bin/env python3
"""A TCP relay that goes silent during given windows of simulated time, or
cuts a connection after an exact number of bytes.

Runs on its own simulated host under Shadow, between a client (pull, a
replica) and a server (the MySQL source, pull's listener, the S3 server).
During a pause window it forwards nothing and closes nothing: the
connections stay open, no RST and no FIN arrive, the bytes wait (in the
relay and in the TCP buffers). New connections are accepted but not
connected to the server until the window ends, so the client sees no
greeting. That is a network partition as an endpoint sees it, which Shadow
cannot model otherwise (its network graph does not change during a run).

usage: pause_relay.py <listen port> <server host> <server port> [<windows>]
  server host: a host name, or @<file>: the host named in that file, read
           again for every new connection (a failover moves it)
  windows: <start>-<end>[/<flag>...][,...] in seconds of simulated time
           since the simulation started (Shadow's clock starts at
           2000-01-01 00:00:00 UTC); flags:
             down  only server -> client is silent (up flows)
             up    only client -> server is silent
             old   only the connections opened before the window are
                   silent; new ones flow (a NAT or firewall that dropped
                   its state: the old connection is half-open, a new one
                   works)
environment:
  RELAY_CUT=<n>          close connection #RELAY_CUT_CONN (default 1) once
                         <n> bytes went from the server to the client
  RELAY_CUT_MODE=fin|rst a FIN (shutdown) or an RST (SO_LINGER 0) to both
                         ends (default fin)
  RELAY_RECORD=<file>    write what connection #1 carried from the server to
                         the client to <file> (to place the cuts)
Logs one line per connection and window to stdout.
"""
import os
import socket
import struct
import sys
import threading
import time

EPOCH = 946684800  # 2000-01-01 00:00:00 UTC, the start of Shadow's clock


def parse_windows(text):
    windows = []
    for part in filter(None, text.split(",")):
        span, *flags = part.split("/")
        start, end = span.split("-")
        windows.append((float(start), float(end), set(flags)))
    return windows


WINDOWS = []
CUT = int(os.environ.get("RELAY_CUT", "-1"))
CUT_MODE = os.environ.get("RELAY_CUT_MODE", "fin")
CUT_CONN = int(os.environ.get("RELAY_CUT_CONN", "1"))
RECORD = os.environ.get("RELAY_RECORD", "")


def now():
    return time.time() - EPOCH


def wait_open(direction, opened):
    """returns when no pause window silences this direction ('down' or
    'up', or 'connect' for a new connection) of a connection opened at
    'opened'"""
    while True:
        t = now()
        for start, end, flags in WINDOWS:
            if not start <= t < end:
                continue
            if "old" in flags and (direction == "connect" or opened >= start):
                continue
            if direction in ("down", "up") and ({"down", "up"} & flags) and direction not in flags:
                continue
            time.sleep(end - t)
            break
        else:
            return


def log(text):
    print(f"{now():10.3f} {text}", flush=True)


class Connection:
    def __init__(self, number, client, server):
        self.number, self.client, self.server = number, client, server
        self.opened = now()
        self.down_bytes = 0
        self.cut = CUT if number == CUT_CONN else -1
        self.record = open(RECORD, "wb") if RECORD and number == 1 else None

    def cut_now(self):
        log(f"#{self.number} cut after {self.down_bytes} bytes ({CUT_MODE})")
        for sock in (self.client, self.server):
            try:
                if CUT_MODE == "rst":
                    sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER,
                                    struct.pack("ii", 1, 0))
                else:
                    sock.shutdown(socket.SHUT_RDWR)
                sock.close()
            except OSError as error:
                log(f"#{self.number} cut: {error}")

    def pump(self, src, dst, direction):
        try:
            while True:
                wait_open(direction, self.opened)
                data = src.recv(65536)
                if not data:
                    break
                # bytes read just before a window opened wait for its end
                wait_open(direction, self.opened)
                if direction == "down":
                    if 0 <= self.cut < self.down_bytes + len(data):
                        data = data[:self.cut - self.down_bytes]
                        if data:
                            dst.sendall(data)
                        self.down_bytes += len(data)
                        if self.record:
                            self.record.write(data)
                        self.cut_now()
                        return
                    self.down_bytes += len(data)
                    if self.record:
                        self.record.write(data)
                        self.record.flush()
                dst.sendall(data)
        except OSError as error:
            log(f"#{self.number} {direction}: {error}")
        finally:
            for sock, how in ((dst, socket.SHUT_WR), (src, socket.SHUT_RD)):
                try:
                    sock.shutdown(how)
                except OSError:
                    pass


def serve(client, peer, host, port, number):
    # a connection accepted during a window is held until the window ends
    wait_open("connect", now())
    if host.startswith("@"):
        with open(host[1:]) as f:
            host = f.read().strip()
    try:
        server = socket.create_connection((host, port))
    except OSError as error:
        log(f"#{number} from {peer}: cannot connect to {host}:{port}: {error}")
        client.close()
        return
    log(f"#{number} from {peer} relayed to {host}:{port}")
    for sock in (client, server):
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    conn = Connection(number, client, server)
    up = threading.Thread(target=conn.pump, args=(client, server, "up"),
                          daemon=True)
    up.start()
    conn.pump(server, client, "down")
    up.join()
    for sock in (client, server):
        try:
            sock.close()
        except OSError:
            pass
    log(f"#{number} closed after {conn.down_bytes} bytes down")


def main():
    global WINDOWS
    listen_port, host, port = int(sys.argv[1]), sys.argv[2], int(sys.argv[3])
    WINDOWS = parse_windows(sys.argv[4]) if len(sys.argv) > 4 else []
    for start, end, flags in WINDOWS:
        log(f"pause window {start:g} s to {end:g} s {' '.join(sorted(flags))}")
    if CUT >= 0:
        log(f"cut of connection #{CUT_CONN} after {CUT} bytes ({CUT_MODE})")
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("0.0.0.0", listen_port))
    listener.listen(64)
    number = 0
    while True:
        client, peer = listener.accept()
        number += 1
        log(f"#{number} accepted from {peer[0]}:{peer[1]}")
        threading.Thread(target=serve, args=(client, f"{peer[0]}:{peer[1]}",
                                             host, port, number),
                         daemon=True).start()


if __name__ == "__main__":
    main()
