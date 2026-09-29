#!/usr/bin/env python3
"""
Reproducer: storage checkpointing (checkpoint_size / checkpoint_interval)
has no effect.

Since commit 7f8cb69 ("Rework storage layer abstractions ... (part 2)"),
binsrv::storage never copies 'storage.checkpoint_size' and
'storage.checkpoint_interval' from the configuration into its private
fields, so checkpoint-driven flushes never happen. Event data is kept in
memory and only reaches the storage backend when a binlog file is closed
(rotation) or when the utility shuts down.

What this script does
  1. Prepares a scratch schema on the source MySQL server and runs
     FLUSH BINARY LOGS so streaming starts on a fresh binlog file.
  2. Writes a binlog_server config (file backend, position mode) with
     checkpointing enabled and starts 'binlog_server pull'.
  3. Generates load on the source WITHOUT rotating binlogs, and after each
     step prints the source's binlog position next to the size of the same
     binlog file in binlog_server's storage directory.
  4. Decides whether checkpointing worked (on-disk size keeps up with the
     source, lagging by at most about one checkpoint) or is broken (on-disk
     size stays at the 4-byte magic header).
  5. Sends SIGTERM and shows that all buffered data is written only at
     shutdown.

Exit codes: 0 = checkpointing works, 1 = bug reproduced, 2 = setup error.

Requirements
  - python 3.8+, the 'mysql' command-line client on PATH
  - a built 'binlog_server' binary
  - a MySQL 8.0 / 8.4 source with binary logging enabled; the account needs
    CREATE/INSERT/DROP on the scratch schema, RELOAD (FLUSH BINARY LOGS),
    REPLICATION CLIENT and REPLICATION SLAVE

Example
  ./repro_broken_checkpointing.py --binsrv ../../build/binlog_server \\
      --host 127.0.0.1 --port 3306 --user root --password secret --mode size
"""

import argparse
import json
import os
import random
import re
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path

MAGIC_SIZE = 4  # binlog magic header written when a new file is opened
SCHEMA = "pbs_checkpoint_repro"
ROW_PAYLOAD_BYTES = 1024


class SetupError(Exception):
    pass


# --------------------------------------------------------------------------
# MySQL helpers (via the 'mysql' CLI so no Python driver is required)
# --------------------------------------------------------------------------
class MySQL:
    def __init__(self, args):
        self.base = [
            args.mysql_client, "--protocol=TCP", "-h", args.host,
            "-P", str(args.port), "-u", args.user, "-N", "-B",
        ]
        self.env = dict(os.environ, MYSQL_PWD=args.password)

    def run(self, sql, database=None):
        cmd = list(self.base)
        if database:
            cmd.append(database)
        res = subprocess.run(cmd, input=sql, capture_output=True, text=True,
                             env=self.env)
        if res.returncode != 0:
            raise SetupError(f"mysql failed: {res.stderr.strip()}\nSQL: {sql[:200]}")
        return res.stdout

    def binlog_status(self):
        """Returns (file, position) of the source's current binlog."""
        for stmt in ("SHOW BINARY LOG STATUS", "SHOW MASTER STATUS"):
            try:
                out = self.run(stmt + ";").strip()
            except SetupError:
                continue
            if out:
                fields = out.splitlines()[0].split("\t")
                return fields[0], int(fields[1])
        raise SetupError("cannot read binlog status - is binary logging enabled?")


# --------------------------------------------------------------------------
# binlog_server helpers
# --------------------------------------------------------------------------
def free_tcp_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("0.0.0.0", 0))
        return s.getsockname()[1]


def write_config(args, workdir, storage_dir):
    storage = {"backend": "file", "uri": f"file://{storage_dir}"}
    if args.mode in ("size", "both"):
        storage["checkpoint_size"] = args.checkpoint_size
    if args.mode in ("interval", "both"):
        storage["checkpoint_interval"] = args.checkpoint_interval

    config = {
        "logger": {"level": "info", "file": str(workdir / "binsrv.log")},
        "connection": {
            "host": args.host, "port": args.port, "user": args.user,
            "password": args.password, "connect_timeout": 20,
            "read_timeout": args.read_timeout, "write_timeout": 60,
        },
        "replication": {
            "server_id": random.randint(10000, 60000), "idle_time": 5,
            "verify_checksum": True, "mode": "position",
        },
        # required by the config schema even though only 'pull' uses it
        "replication_source": {
            "port": free_tcp_port(), "read_timeout": 60, "write_timeout": 60,
            "authentication": {"user": "rpl", "password": "password",
                               "plugin": "caching_sha2_password"},
        },
        "storage": storage,
    }
    path = workdir / "binsrv_config.json"
    path.write_text(json.dumps(config, indent=2))
    return path


def on_disk_size(storage_dir, binlog_name):
    p = storage_dir / binlog_name
    return p.stat().st_size if p.exists() else None


def parse_size(text):
    m = re.fullmatch(r"(\d+)([KMGT]?)", text.strip().upper())
    if not m:
        raise SetupError(f"cannot parse size '{text}'")
    mult = {"": 1, "K": 1 << 10, "M": 1 << 20, "G": 1 << 30, "T": 1 << 40}
    return int(m.group(1)) * mult[m.group(2)]


def parse_seconds(text):
    m = re.fullmatch(r"(\d+)([smhd]?)", text.strip().lower())
    if not m:
        raise SetupError(f"cannot parse interval '{text}'")
    mult = {"": 1, "s": 1, "m": 60, "h": 3600, "d": 86400}
    return int(m.group(1)) * mult[m.group(2)]


# --------------------------------------------------------------------------
# workload
# --------------------------------------------------------------------------
def txn_sql(rows):
    values = ",".join([f"(REPEAT('x', {ROW_PAYLOAD_BYTES}))"] * rows)
    return f"BEGIN; INSERT INTO t1(val) VALUES {values}; COMMIT;\n"


def print_header():
    print(f"  {'elapsed':>8}  {'source pos':>12}  {'on disk':>12}  {'not on disk':>12}")


def print_sample(t0, src_pos, disk):
    disk_str = "-" if disk is None else f"{disk:,}"
    lag = "-" if disk is None else f"{src_pos - disk:,}"
    print(f"  {time.time() - t0:7.1f}s  {src_pos:>12,}  {disk_str:>12}  {lag:>12}")


def run_size_load(db, storage_dir, binlog, args):
    """Writes ~target_mb MB in 64 KB transactions, sampling after each MB."""
    rows_per_txn = 64
    txns_per_step = (1 << 20) // (rows_per_txn * ROW_PAYLOAD_BYTES)  # ~1 MB
    t0 = time.time()
    print_header()
    for _ in range(args.target_mb):
        db.run(txn_sql(rows_per_txn) * txns_per_step, database=SCHEMA)
        time.sleep(0.3)  # let binlog_server receive the events
        _, pos = db.binlog_status()
        print_sample(t0, pos, on_disk_size(storage_dir, binlog))
    return rows_per_txn * ROW_PAYLOAD_BYTES


def run_interval_load(db, storage_dir, binlog, args, interval_s):
    """A small transaction every 0.5s, for several checkpoint intervals."""
    duration = max(4 * interval_s, 10)
    t0 = time.time()
    next_sample = t0
    print_header()
    while time.time() - t0 < duration:
        db.run(txn_sql(4), database=SCHEMA)
        time.sleep(0.5)
        if time.time() >= next_sample:
            _, pos = db.binlog_status()
            print_sample(t0, pos, on_disk_size(storage_dir, binlog))
            next_sample += 1.0
    return 4 * ROW_PAYLOAD_BYTES


# --------------------------------------------------------------------------
# main
# --------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--binsrv", required=True, help="path to binlog_server binary")
    ap.add_argument("--mysql-client", default="mysql")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=3306)
    ap.add_argument("--user", default="root")
    ap.add_argument("--password", default="")
    ap.add_argument("--mode", choices=["size", "interval", "both"], default="size",
                    help="which checkpointing trigger to exercise")
    ap.add_argument("--checkpoint-size", default="1M")
    ap.add_argument("--checkpoint-interval", default="2s")
    ap.add_argument("--target-mb", type=int, default=8,
                    help="size mode: MB of binlog data to generate")
    ap.add_argument("--read-timeout", type=int, default=15,
                    help="binlog_server connection.read_timeout (affects shutdown wait)")
    ap.add_argument("--workdir", help="where to put config/storage/logs (default: temp dir)")
    ap.add_argument("--keep-workdir", action="store_true")
    args = ap.parse_args()

    binsrv = Path(args.binsrv).resolve()
    if not binsrv.is_file():
        raise SetupError(f"binlog_server binary not found: {binsrv}")
    if shutil.which(args.mysql_client) is None:
        raise SetupError(f"'{args.mysql_client}' client not found on PATH")

    workdir = Path(args.workdir or tempfile.mkdtemp(prefix="pbs_ckpt_")).resolve()
    storage_dir = workdir / "storage"
    storage_dir.mkdir(parents=True, exist_ok=True)
    if any(storage_dir.iterdir()):
        raise SetupError(f"storage directory {storage_dir} is not empty")

    db = MySQL(args)
    proc = None
    try:
        print("== Step 1: preparing source")
        db.run(f"DROP DATABASE IF EXISTS {SCHEMA}; CREATE DATABASE {SCHEMA};"
               f"CREATE TABLE {SCHEMA}.t1(id SERIAL PRIMARY KEY, val TEXT NOT NULL)"
               f" ENGINE=InnoDB; FLUSH BINARY LOGS;")
        binlog, start_pos = db.binlog_status()
        print(f"   source is now writing '{binlog}' (position {start_pos})")

        print("== Step 2: starting 'binlog_server pull'")
        config_path = write_config(args, workdir, storage_dir)
        print(f"   config : {config_path}")
        print(f"   storage: {storage_dir}")
        print(f"   checkpointing: " + ", ".join(
            f"{k}={v}" for k, v in json.loads(config_path.read_text())["storage"].items()
            if k.startswith("checkpoint")))
        out = open(workdir / "binsrv.stdout", "w")
        proc = subprocess.Popen([str(binsrv), "pull", str(config_path)],
                                stdout=out, stderr=subprocess.STDOUT)

        deadline = time.time() + 30
        while on_disk_size(storage_dir, binlog) is None:
            if proc.poll() is not None:
                raise SetupError(f"binlog_server exited early (code {proc.returncode}),"
                                 f" see {workdir}/binsrv.log")
            if time.time() > deadline:
                raise SetupError(f"'{binlog}' did not appear in storage within 30s")
            time.sleep(0.2)
        print(f"   '{binlog}' opened in storage, size {on_disk_size(storage_dir, binlog)} bytes")

        print("== Step 3: generating load on the source (no binlog rotation)")
        if args.mode == "interval":
            txn_bytes = run_interval_load(db, storage_dir, binlog, args,
                                          parse_seconds(args.checkpoint_interval))
        else:
            txn_bytes = run_size_load(db, storage_dir, binlog, args)

        time.sleep(2)
        cur_binlog, src_pos = db.binlog_status()
        if cur_binlog != binlog:
            raise SetupError(f"source rotated to '{cur_binlog}' during the test;"
                             f" raise max_binlog_size or lower --target-mb")
        disk = on_disk_size(storage_dir, binlog) or 0
        unflushed = src_pos - disk

        print("== Step 4: verdict")
        print(f"   source position     : {src_pos:,} bytes")
        print(f"   on disk in storage  : {disk:,} bytes")
        print(f"   still only in memory: {unflushed:,} bytes")

        # with working checkpoints the unflushed tail is bounded by roughly
        # one checkpoint window plus one transaction
        slack = 64 * 1024
        if args.mode == "interval":
            allowed = src_pos // 2
        else:
            allowed = parse_size(args.checkpoint_size) + txn_bytes + slack
        reproduced = unflushed > allowed and src_pos > disk
        if reproduced:
            print(f"   RESULT: BUG REPRODUCED - more than {allowed:,} bytes were never"
                  f" checkpointed" + (" (file holds only the magic header)"
                                      if disk <= MAGIC_SIZE else ""))
        else:
            print(f"   RESULT: checkpointing works (unflushed tail <= {allowed:,} bytes)")

        print("== Step 5: SIGTERM - buffered data is written only now")
        proc.send_signal(signal.SIGTERM)
        # a tiny event wakes up the blocking binlog read so shutdown is quick
        db.run("INSERT INTO t1(val) VALUES ('wake-up');", database=SCHEMA)
        try:
            proc.wait(timeout=args.read_timeout + 30)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()
            print("   binlog_server did not stop in time, killed")
        print(f"   binlog_server exited with code {proc.returncode}")
        print(f"   on disk after shutdown: {on_disk_size(storage_dir, binlog) or 0:,} bytes")
        proc = None

        return 1 if reproduced else 0
    finally:
        if proc is not None and proc.poll() is None:
            proc.kill()
            proc.wait()
        try:
            db.run(f"DROP DATABASE IF EXISTS {SCHEMA};")
        except SetupError:
            pass
        if args.keep_workdir or args.workdir:
            print(f"   artifacts kept in {workdir}")
        else:
            shutil.rmtree(workdir, ignore_errors=True)


if __name__ == "__main__":
    try:
        sys.exit(main())
    except SetupError as e:
        print(f"SETUP ERROR: {e}", file=sys.stderr)
        sys.exit(2)
