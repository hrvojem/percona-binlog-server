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
       check_binlog_timestamps.py --rewritten <copy at the kill> <storage> [--encrypted]
       check_binlog_timestamps.py --prefix <source datadir> <storage> [--gtid]
       check_binlog_timestamps.py --failover <old source> <new source> <storage>
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


def prefix(source, storage, gtid_mode):
    """a writer that stopped early (the source purged what it needed) must
    leave the start of the source's binlogs: every stored binlog equal to the
    source's, the last one possibly cut short; in rewrite mode the first of
    the source's transactions, in order"""
    if gtid_mode:
        want, got = gtids(source), gtids(storage)
        if got == want[:len(got)]:
            print(f"GTIDs: the storage holds the first {len(got)} of the source's "
                  f"{len(want)} transactions, in order")
            return 0
        print(f"GTIDs DIFFER: the storage's {len(got)} transactions are not the start "
              f"of the source's {len(want)}")
        return 1
    stored = sorted(n for n in os.listdir(storage) if re.fullmatch(r"binlog\.\d+", n))
    names = sorted(n for n in os.listdir(source) if re.fullmatch(r"binlog\.\d+", n))
    bad = 0
    full = 0
    if stored != names[:len(stored)]:
        bad += 1
        print(f"BAD the stored binlogs {stored[:3]}.. are not the first of the source's "
              f"{names[:3]}..")
    for i, name in enumerate(stored):
        data = open(os.path.join(storage, name), "rb").read()
        src_path = os.path.join(source, name)
        if not os.path.exists(src_path):
            continue
        src = open(src_path, "rb").read()
        last = i == len(stored) - 1
        # byte 22 (index 21) is LOG_EVENT_BINLOG_IN_USE_F, cleared in PBS's copy
        same = len(data) <= len(src) and all(
            a == b for j, (a, b) in enumerate(zip(data, src)) if j != 21)
        if len(data) == 4 and header_only(src_path) == 0:
            full += 1
        elif same and len(data) == len(src):
            full += 1
        elif same and last:
            print(f"ok  {name}: the first {len(data)} of the source's {len(src)} bytes")
        else:
            bad += 1
            print(f"BAD {name}: {len(data)} bytes, not {'a prefix of ' if last else ''}"
                  f"the source's {len(src)}")
    print(f"prefix: {full} binlogs equal to the source's, {len(stored) - full} cut short "
          f"or different; {bad} problems")
    return 1 if bad else 0


def failover(old, new, storage):
    """after a switchover to a promoted replica (which holds the old
    source's transactions in its own binlogs), the storage must hold the old
    source's transactions, then the ones the new source wrote after its
    promotion: each once, in order"""
    first = gtids(old)
    seen = set(first)
    later = [g for g in gtids(new) if g not in seen]
    want, got = first + later, gtids(storage)
    if want == got:
        print(f"GTIDs: the storage holds the old source's {len(first)} transactions, then "
              f"the new source's {len(later)}, in order")
        return 0
    missing = [g for g in want if g not in got]
    extra = len(got) - len(set(got))
    foreign = [g for g in got if g not in set(want)]
    print(f"GTIDs DIFFER: expected {len(first)} + {len(later)}, storage {len(got)}, "
          f"missing {len(missing)}, duplicated {extra}, unknown {len(foreign)}, "
          f"first missing {missing[:3]}")
    return 1


def header_only(path):
    """0 when the binlog holds only FORMAT_DESCRIPTION / PREVIOUS_GTIDS
    events: PBS stores such a binlog as the magic alone until the first
    transaction arrives"""
    return 0 if all(typ in (15, 35) for _ts, typ, _b in events(path)) else 1


def rewritten(copy, storage, encrypted):
    """the bytes a killed writer left past each binlog's recorded size (in
    the copy taken before the restart) against what the storage holds at
    those offsets now. The restart truncates the unrecorded tail and writes
    again; in an encrypted storage, different bytes at the same offset under
    the same encryption record mean the AES-CTR keystream was used twice
    (ticket 32, PBS-104)"""
    bad = 0
    seen = 0
    for name in sorted(os.listdir(copy)):
        if not re.fullmatch(r"binlog\.\d+", name):
            continue
        meta_path = os.path.join(copy, name + ".json")
        meta = json.load(open(meta_path)) if os.path.exists(meta_path) else {}
        recorded = int(meta.get("size", 0))
        with open(os.path.join(copy, name), "rb") as f:
            before = f.read()
        if len(before) <= recorded:
            continue
        seen += 1
        final_path = os.path.join(storage, name)
        after = open(final_path, "rb").read() if os.path.exists(final_path) else b""
        after_meta_path = os.path.join(storage, name + ".json")
        after_meta = (json.load(open(after_meta_path))
                      if os.path.exists(after_meta_path) else {})
        tail_before = before[recorded:]
        tail_after = after[recorded:recorded + len(tail_before)]
        span = f"{name} bytes {recorded}..{len(before)} (unrecorded at the kill)"
        if tail_after == tail_before:
            print(f"same  {span}: written again with the same bytes")
            continue
        differ = sum(1 for a, b in zip(tail_before, tail_after) if a != b)
        same_record = meta.get("encryption") == after_meta.get("encryption")
        if encrypted and same_record and meta.get("encryption"):
            bad += 1
            print(f"BAD   {span}: {differ} of {len(tail_after)} bytes now differ under the "
                  f"same encryption record: the AES-CTR keystream at these offsets was used "
                  f"for two different plaintexts")
        else:
            print(f"info  {span}: {differ} of {len(tail_after)} bytes now differ"
                  f"{'' if encrypted else ' (plaintext storage: harmless)'}"
                  f"{'' if same_record else ' (a new encryption record)'}")
    print(f"unrecorded tails at the kill: {seen}; keystream reuse: {bad}")
    return 1 if bad else 0


def main():
    if sys.argv[1] == "--gtids":
        return compare_gtids(sys.argv[2], sys.argv[3])
    if sys.argv[1] == "--rewritten":
        return rewritten(sys.argv[2], sys.argv[3], "--encrypted" in sys.argv)
    if sys.argv[1] == "--failover":
        return failover(sys.argv[2], sys.argv[3], sys.argv[4])
    if sys.argv[1] == "--prefix":
        return prefix(sys.argv[2], sys.argv[3], "--gtid" in sys.argv)
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
