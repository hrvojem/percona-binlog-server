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

"""Measures how much data 'binlog_server fetch' uploads to S3 storage for
one binlog of a given size, for several 'storage.checkpoint_size' values.

For each setting, the MySQL server gets the same workload (1 MB rows, one
per transaction, all in one binlog), 'fetch' copies it into an empty S3
storage through s3_fault_proxy.py, and the proxy's traffic counters give the
number of PUT requests and the bytes uploaded for the binlog object. The
stored binlog is compared with the server's afterwards.

Uses the MySQL and binlog_server helpers of fetch_crash_test.py (same
directory). Example:

  PYTHONPATH=~/ws/qa/pylib python3 s3_upload_volume.py \\
    --mysql-basedir ~/ws/dist/mysql-8.4.8-linux-glibc2.28-x86_64-minimal \\
    --binsrv ~/ws/percona-binlog-server-build-debug_gcc14/binlog_server \\
    --s3-endpoint 127.0.0.1:9200 \\
    --s3-proxy-control http://127.0.0.1:9200/__faults__ \\
    --s3-bucket-dir ~/ws/s3/data/pbs-mtr --data-mb 64 \\
    --checkpoint-sizes none,16M,4M,1M --workdir ~/ws/qa/upload-volume
"""

import argparse
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import fetch_crash_test as harness  # noqa: E402

MEGABYTE = 1024 * 1024


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--mysql-basedir", type=Path, required=True)
    parser.add_argument("--binsrv", type=Path, required=True)
    parser.add_argument("--workdir", type=Path, required=True)
    parser.add_argument("--port", type=int, default=23356)
    parser.add_argument("--s3-endpoint", required=True,
                        help="host:port of s3_fault_proxy.py")
    parser.add_argument("--s3-proxy-control", required=True)
    parser.add_argument("--s3-bucket", default="pbs-mtr")
    parser.add_argument("--s3-bucket-dir", type=Path, required=True)
    parser.add_argument("--s3-access-key", default="pbsaccesskey")
    parser.add_argument("--s3-secret-key", default="pbs/secret+key=1")
    parser.add_argument("--data-mb", type=int, default=64)
    parser.add_argument("--checkpoint-sizes", default="none,16M,4M,1M")
    args = parser.parse_args()

    # one binlog for the whole workload
    harness.MAX_BINLOG_SIZE = 1024 * MEGABYTE
    args.workdir.mkdir(parents=True, exist_ok=True)
    server = harness.MySQLServer(args.mysql_basedir.expanduser(),
                                 args.workdir / "mysql", args.port, False)
    proxy = harness.FaultProxy(args.s3_proxy_control)
    results = []
    server.start()
    try:
        for setting in args.checkpoint_sizes.split(","):
            server.reset()
            for _ in range(args.data_mb):
                server.sql("INSERT INTO qa.t (payload) VALUES "
                           f"(REPEAT('x', {MEGABYTE}))")
            names = server.binlog_names()
            binlog_size = sum(server.binlog_path(name).stat().st_size
                              for name in names)

            s3 = harness.S3Storage(args.s3_endpoint, args.s3_bucket,
                                   args.s3_bucket_dir.expanduser(),
                                   args.s3_access_key, args.s3_secret_key,
                                   f"upload-volume-{setting}")
            binsrv = harness.BinlogServer(args.binsrv.expanduser(),
                                          args.workdir / f"binsrv-{setting}",
                                          args.port, False, s3)
            binsrv.reset()
            binsrv.checkpoint_size = None if setting == "none" else setting
            harness.BINSRV = binsrv
            proxy.set_rules([])
            harness.urllib.request.urlopen(harness.urllib.request.Request(
                args.s3_proxy_control,
                data=json.dumps({"rules": [], "reset_traffic": True}).encode(),
                method="PUT"), timeout=10).read()

            started = time.monotonic()
            process = binsrv.start_fetch()
            _, stderr = process.communicate(timeout=3600)
            elapsed = time.monotonic() - started
            traffic = proxy.stats()["traffic"]
            if process.returncode != 0:
                raise RuntimeError(f"fetch exited with {process.returncode}: "
                                   f"{stderr.decode(errors='replace')[-500:]}")
            for name in names:
                if (binsrv.storage / name).read_bytes() != \
                        server.binlog_as_sent(name):
                    raise RuntimeError(f"stored {name} differs")

            binlog_puts = traffic.get("binlog_puts", {})
            put = traffic.get("PUT", {})
            get = traffic.get("GET", {})
            result = {
                "checkpoint_size": setting,
                "binlogs": len(names),
                "binlog_bytes": binlog_size,
                "binlog_put_requests": binlog_puts.get("requests", 0),
                "binlog_put_bytes": binlog_puts.get("request_bytes", 0),
                "all_put_requests": put.get("requests", 0),
                "all_put_bytes": put.get("request_bytes", 0),
                "get_requests": get.get("requests", 0),
                "get_bytes": get.get("response_bytes", 0),
                "fetch_seconds": round(elapsed, 1),
            }
            result["upload_factor"] = round(
                result["binlog_put_bytes"] / max(binlog_size, 1), 1)
            results.append(result)
            print(json.dumps(result), flush=True)
            # free the space for the next setting
            harness.shutil.rmtree(binsrv.storage, ignore_errors=True)
    finally:
        server.stop()
    (args.workdir / "results.json").write_text(json.dumps(results, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
