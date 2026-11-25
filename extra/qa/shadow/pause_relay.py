#!/usr/bin/env python3
"""A TCP relay that goes silent during given windows of simulated time.

Runs on its own simulated host under Shadow, between a client (pull, a
replica) and a server (the MySQL source, pull's listener, the S3 server).
During a pause window it forwards nothing in either direction and closes
nothing: the connections stay open, no RST and no FIN arrive, the bytes
wait (in the relay and in the TCP buffers). New connections are accepted but
not connected to the server until the window ends, so the client sees no
greeting. That is a network partition as an endpoint sees it, which Shadow
cannot model otherwise (its network graph does not change during a run).

usage: pause_relay.py <listen port> <server host> <server port> <windows>
  server host: a host name, or @<file>: the host named in that file, read
           again for every new connection (a failover moves it)
  windows: <start>-<end>[,<start>-<end>...] in seconds of simulated time
           since the simulation started (Shadow's clock starts at
           2000-01-01 00:00:00 UTC)
Logs one line per connection and window to stdout.
"""
import socket
import sys
import threading
import time

EPOCH = 946684800  # 2000-01-01 00:00:00 UTC, the start of Shadow's clock


def parse_windows(text):
    windows = []
    for part in text.split(","):
        start, end = part.split("-")
        windows.append((float(start), float(end)))
    return windows


WINDOWS = []


def now():
    return time.time() - EPOCH


def wait_open():
    """returns when no pause window covers the current time"""
    while True:
        t = now()
        for start, end in WINDOWS:
            if start <= t < end:
                time.sleep(end - t)
                break
        else:
            return


def log(text):
    print(f"{now():10.3f} {text}", flush=True)


def pump(src, dst, label):
    try:
        while True:
            wait_open()
            data = src.recv(65536)
            if not data:
                break
            # bytes read just before a window opened wait for its end
            wait_open()
            dst.sendall(data)
    except OSError as error:
        log(f"{label}: {error}")
    finally:
        for sock, how in ((dst, socket.SHUT_WR), (src, socket.SHUT_RD)):
            try:
                sock.shutdown(how)
            except OSError:
                pass


def serve(client, peer, host, port, number):
    # a connection accepted during a window is held until the window ends
    wait_open()
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
    up = threading.Thread(target=pump, args=(client, server, f"#{number} up"),
                          daemon=True)
    up.start()
    pump(server, client, f"#{number} down")
    up.join()
    client.close()
    server.close()
    log(f"#{number} closed")


def main():
    global WINDOWS
    listen_port, host, port = int(sys.argv[1]), sys.argv[2], int(sys.argv[3])
    WINDOWS = parse_windows(sys.argv[4]) if len(sys.argv) > 4 and sys.argv[4] else []
    for start, end in WINDOWS:
        log(f"pause window {start:g} s to {end:g} s")
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
