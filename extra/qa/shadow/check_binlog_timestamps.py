#!/usr/bin/env python3
"""Checks the timestamp range PBS records for each stored binlog against the
events in the stored file.

For every binlog.NNNNNN in a file storage, reads the event headers (4-byte
timestamp, type, event size) and compares the min_timestamp / max_timestamp
of binlog.NNNNNN.json with the timestamps of the events in the file (PBS
counts every event it stores). A recorded range that reaches outside them is
reported: ticket 6 (PBS-72) carries the timestamps of a dropped incomplete
transaction into the next binlog.

usage: check_binlog_timestamps.py <storage dir> [-v]
       check_binlog_timestamps.py --gtids <source datadir> <storage dir>
       check_binlog_timestamps.py --header-only <binlog>
exit 0 when every range is inside the file's events (or, with --gtids, the
storage holds the source's GTIDs in order), 1 otherwise.
"""
import datetime
import json
import os
import re
import struct
import sys

GTID_TYPES = {33, 34, 42}  # GTID, ANONYMOUS_GTID, GTID_TAGGED
XID = 16
QUERY = 2


def events(path):
    with open(path, "rb") as f:
        data = f.read()
    if data[:4] != b"\xfebin":
        return
    pos = 4
    while pos + 19 <= len(data):
        ts, typ, _server, size = struct.unpack_from("<IBII", data, pos)
        if size < 19 or pos + size > len(data):
            return
        body = data[pos + 19:pos + size]
        yield ts, typ, body
        pos += size


def transaction_timestamps(path):
    """timestamps of the events of complete transactions"""
    out = []
    current = None
    for ts, typ, body in events(path):
        if typ in GTID_TYPES:
            current = [ts]
            continue
        if current is None:
            continue
        current.append(ts)
        ends = typ == XID
        if typ == QUERY and (b"COMMIT" in body or b"CREATE" in body.upper()
                             or b"FLUSH" in body.upper() or b"DROP" in body.upper()):
            ends = b"BEGIN" not in body
        if ends:
            out.extend(current)
            current = None
    return out


def parse_ts(text):
    return int(datetime.datetime.strptime(text, "%Y-%m-%dT%H:%M:%S").replace(
        tzinfo=datetime.timezone.utc).timestamp())


def fmt(ts):
    return datetime.datetime.fromtimestamp(ts, datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")


def gtids(directory):
    """(uuid, gno) of every GTID event in the binlogs of a directory"""
    out = []
    for name in sorted(os.listdir(directory)):
        if re.fullmatch(r"binlog\.\d+", name):
            for _ts, typ, body in events(os.path.join(directory, name)):
                if typ in (33, 42) and len(body) >= 25:
                    out.append((body[1:17].hex(), struct.unpack_from("<q", body, 17)[0]))
    return out


def compare_gtids(source, storage):
    """rewrite mode stores the source's transactions in PBS's own files: the
    GTIDs must be the same, in the same order"""
    want, got = gtids(source), gtids(storage)
    if want == got:
        print(f"GTIDs: the storage holds the source's {len(want)} transactions, in order")
        return 0
    missing = [g for g in want if g not in got]
    extra = len(got) - len(set(got))
    print(f"GTIDs DIFFER: source {len(want)}, storage {len(got)}, missing {len(missing)}, "
          f"duplicated {extra}, first missing {missing[:3]}")
    return 1


def header_only(path):
    """0 when the binlog holds only FORMAT_DESCRIPTION / PREVIOUS_GTIDS
    events: PBS stores such a binlog as the magic alone until the first
    transaction arrives"""
    return 0 if all(typ in (15, 35) for _ts, typ, _b in events(path)) else 1


def main():
    if sys.argv[1] == "--gtids":
        return compare_gtids(sys.argv[2], sys.argv[3])
    if sys.argv[1] == "--header-only":
        return header_only(sys.argv[2])
    storage = sys.argv[1]
    verbose = "-v" in sys.argv
    bad = 0
    for name in sorted(os.listdir(storage)):
        if not re.fullmatch(r"binlog\.\d+", name):
            continue
        meta_path = os.path.join(storage, name + ".json")
        if not os.path.exists(meta_path):
            continue
        meta = json.load(open(meta_path))
        all_ts = [ts for ts, _t, _b in events(os.path.join(storage, name))]
        trx_ts = transaction_timestamps(os.path.join(storage, name))
        rec = (meta.get("min_timestamp"), meta.get("max_timestamp"))
        line = (f"{name}: recorded {rec[0]} .. {rec[1]}; events "
                f"{fmt(min(all_ts)) if all_ts else '-'} .. {fmt(max(all_ts)) if all_ts else '-'}; "
                f"transactions {fmt(min(trx_ts)) if trx_ts else '-'} .. {fmt(max(trx_ts)) if trx_ts else '-'}")
        problem = None
        if rec[0] and rec[1]:
            lo, hi = parse_ts(rec[0]), parse_ts(rec[1])
            # PBS counts every event it stores (in position mode the
            # FORMAT_DESCRIPTION and PREVIOUS_GTIDS events too), so the range
            # must lie within the file's own events
            if not all_ts:
                # PBS records the epoch for a binlog that holds only the magic
                if lo != 0 or hi != 0:
                    problem = "a timestamp range is recorded for a binlog without events"
            elif lo < min(all_ts) or hi > max(all_ts):
                problem = "the recorded range reaches outside the binlog's events"
        if problem:
            bad += 1
            print(f"BAD {line}: {problem}")
        elif verbose:
            print(f"ok  {line}")
    print(f"timestamp ranges: {'all inside their binlogs' if not bad else f'{bad} outside their binlogs'}")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
