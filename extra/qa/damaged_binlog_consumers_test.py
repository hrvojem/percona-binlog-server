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

"""What do the consumers of the storage make of a damaged-but-accepted binlog?

Several attack tests leave binlogs in the storage that 'fetch' accepted
although they are malformed, and so far only a replica of 'pull' that starts
empty was pointed at them. An operator uses the storage in other ways as
well: 'mysqlbinlog <files> | mysql' for point-in-time recovery, 'mysqlbinlog
--read-from-remote-server' through 'pull', replicas that start at a position
or already hold some GTIDs, and the Binlog Server's own 'list' / 'search_*'
commands. This test produces each damaged state with the code of the test
that found it (imported, not copied), and gives the result to every consumer.
The outcome is compared with the SOURCE'S data: the rows of every table, and
in GTID mode GTID_EXECUTED.

States (a state that needs the other --mode is skipped with the reason):
  S0_control             an untouched storage (both modes): what every
                         consumer does with a good storage
  S0_rewrite_control     the same in rewrite mode (GTID)
  S1_reannounce          ticket 69, position mode: the next binlog's preamble
                         and events appended to the binlog just closed
                         (hostile_source_test.py 'reannounce')
  S1_midstream           ticket 69, GTID mode: a second FORMAT_DESCRIPTION +
                         PREVIOUS_GTIDS_LOG in the middle of a binlog, and
                         after them events whose end positions are lower
                         than their file offsets ('reannounce_midstream')
  S2_gtid_twice / S2_gtid_swapped / S2_gtid_other_uuid
                         ticket 81, rewrite mode: a transaction stored twice,
                         two transactions in swapped order, a transaction
                         under a foreign UUID (rewrite_attack_test.py
                         'source_gtid_*')
  S2_gtid_twice_keyless / S2_gtid_other_uuid_keyless
                         the same with the attacked transactions inserting
                         into a table without a key, where a second copy
                         of a transaction is no duplicate-key error
  S2_tail_added_emptied / S2_tail_previous_emptied /
  S2_tail_added_other_uuid / S2_tail_added_extended
                         ticket 81, encrypted rewrite storage whose last
                         binlog's metadata was edited before a 'fetch'
                         ('storage_tail_*'): transactions stored twice,
                         PREVIOUS_GTIDS_LOG events that contradict the
                         binlogs before them, a GTID range missing
  S3_length_smaller      ticket 82: a transaction stored as GTID + BEGIN only
                         ('source_length_smaller')
  S4_forged_row / S4_plaintext_swap
                         tickets 53 / 62: a row changed in the ciphertext, a
                         plaintext binlog put into an encrypted storage
                         (replica_tamper_test.py)
  S5_lower_sequence      ticket 70 (PBS-158): a binlog with a lower sequence
                         number indexed after higher ones ('lower_sequence')
  S6_gtid_intervals / S6_large_transaction
                         rewrite mode without an attacker: every file
                         starts with a PREVIOUS_GTIDS_LOG larger than
                         file_size; a 600 KB transaction with file_size 2K

Consumers (the case name is '<state>/<consumer>'):
  C1_files               mysqlbinlog --verify-binlog-checksum on the stored
                         files (as 'pull' serves them to 'mysqlbinlog --raw'
                         if the storage is encrypted): the transactions it
                         prints against those of the source's binlogs
  C1_one_binlog          the binlog with the second preamble alone, against
                         the source's binlog of that name (S0_control, S1)
  C1_start_position / C1_stop_position
                         with a position that is a transaction boundary in
                         the SOURCE's binlog (S0_control, S1)
  C1_include_gtids / C1_exclude_gtids / C1_skip_gtids   (GTID mode)
  C2_replay              mysqlbinlog <all files> | mysql into an empty server
  C2_replay_skip_gtids, C2_replay_stop_datetime, C2_replay_stop_position
                         the same with --skip-gtids / up to a second between
                         two of the source's transactions / up to the
                         position of C1_stop_position; compared with the
                         same command on the source's binlogs
  C3_raw                 mysqlbinlog --read-from-remote-server --raw
                         --to-last-log through 'pull' against the stored
                         files ('same' here: the copy is the storage)
  C3_remote              the same without --raw: the transactions printed
  C3_remote_start_second_preamble / _after_preamble / _source_position
                         with --start-position at the second
                         FORMAT_DESCRIPTION, after the second preamble, and
                         at a later transaction boundary of the source (S1)
  C3_remote_source_binlog / C4_position_replica_source_binlog
                         asking 'pull' for the source's binlog whose events
                         the storage holds under another name
                         (S1_reannounce)
  C4_position_replica    a replica of 'pull' with SOURCE_LOG_FILE / _POS at
                         the first binlog, position 4
  C4_position_replica_second_preamble / _after_preamble / _source_position
                         a replica that holds the source's data up to that
                         point and starts there (S1)
  C4_gtid_replica        SOURCE_AUTO_POSITION=1 from an empty replica with
                         replica_parallel_workers=4 (GTID mode)
  C4_gtid_replica_partway
                         a replica that already has the first half of the
                         source's transactions (GTID mode)
  C4_single_worker_replica
                         replica_parallel_workers=1
  C4_replica_<version>   --replica-basedirs: an empty replica of another
                         MySQL version, for the states of --replica-states
  C5_list / C5_search_by_gtid_set / C5_search_by_timestamp
                         the answers against what the stored binlogs hold
  C5_resume              three more transactions on the source and a 'fetch'
                         on top of the damaged storage; then a replay of the
                         whole storage against the source
  C3_pull                how the 'pull' that served the consumers ended

Every case line carries a 'verdict':
  same           the consumer ends with the source's data
  loud           it fails with an error the operator sees (a non-zero exit,
                 a stopped replication thread)
  silent_wrong   it ends without an error and with other data: the case
                 FAILS, and 'why' holds the row-level difference
  crash / hang   the consumer (or a mysqld) died or did not end: FAILS

A JSON line per case ('case', 'ok', 'verdict', 'why' with the evidence on
failure, the commands), a 'matrix' line, then a summary; result.json and
matrix.json in the work directory; exit 1 if any case failed. --cases
selects by substring of '<state>/<consumer>'. S4, S6_gtid_intervals and the
'keyless' states get one replica only, and after a replica of a state has
killed its mysqld the other replicas of that state are skipped
(--mysqld-deaths). A run takes about 10 minutes in GTID mode and 3 in
position mode. Ports: --port (the source), +1
(the scratch server / replica), +2 ('pull'), +3 (the proxy), +4 (nothing: the
unreachable source of the serving 'pull'), +5... (--replica-basedirs).
Example:

  PYTHONPATH=~/ws/qa/pylib python3 damaged_binlog_consumers_test.py \\
    --mysql-basedir ~/ws/dist/mysql-8.4.8-linux-glibc2.28-x86_64-minimal \\
    --binsrv ~/ws/percona-binlog-server-build-debug_gcc14/binlog_server \\
    --workdir ~/ws/qa/damaged --mode gtid
"""

import argparse
import collections
import hashlib
import json
import re
import shlex
import shutil
import signal
import socket
import struct
import subprocess
import sys
import time
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parent))

import fetch_crash_test as harness  # noqa: E402
import hostile_source_test as hostile  # noqa: E402
import metadata_tamper_test as tamper  # noqa: E402
import pull_replica_test as replication  # noqa: E402
import replica_tamper_test as replica_tamper  # noqa: E402
import rewrite_attack_test as rw  # noqa: E402

LIST_SECONDS = 60
REPLAY_SECONDS = 120
REPLICA_SECONDS = 30
# how long a replica's data must stand still before it is taken for final
STABLE_SECONDS = 5
PASSING = ("same", "loud", "skipped")
TABLE = ("CREATE TABLE qa.t (id INT UNSIGNED NOT NULL AUTO_INCREMENT "
         "PRIMARY KEY, payload LONGBLOB) ENGINE=InnoDB")
# more transactions than hostile_source_test.py's own workload, so that
# several of them follow the second preamble
HOSTILE_WORKLOAD = [{"rows": [200], "ddl": True}] + [
    {"rows": [300 + number], "ddl": False} for number in range(4)]

# set by main()
ARGS = None
GTID = False
SOURCE: harness.MySQLServer = None
SCRATCH: harness.MySQLServer = None
WORK: Path = None
PULL_PORT = 0
PROXY_PORT = 0
DEAD_PORT = 0
OTHER_REPLICAS = {}


# ---------------------------------------------------------------------------
# small tools
# ---------------------------------------------------------------------------

def tool(name: str) -> list:
    return [str(ARGS.mysql_basedir / "bin" / name), "--no-defaults"]


def remote() -> list:
    return ["--read-from-remote-server", "--host=127.0.0.1",
            f"--port={PULL_PORT}", f"--user={replication.PULL_USER}",
            f"--password={replication.PULL_PASSWORD}",
            "--default-auth=caching_sha2_password", "--get-server-public-key",
            "--ssl-mode=DISABLED"]


def shown(command: list) -> str:
    """A command for a result line, with the long directories shortened."""
    text = " ".join(shlex.quote(str(part)) for part in command)
    return text.replace(str(ARGS.mysql_basedir) + "/bin/", "").replace(
        str(WORK), "$W").replace("--no-defaults ", "")


def spans(numbers: list) -> str:
    """[1, 2, 3, 7] as '1-3,7'."""
    numbers = sorted(numbers)
    parts = []
    index = 0
    while index < len(numbers):
        end = index
        while end + 1 < len(numbers) and numbers[end + 1] == numbers[end] + 1:
            end += 1
        parts.append(str(numbers[index]) if end == index else
                     f"{numbers[index]}-{numbers[end]}")
        index = end + 1
    return rw.short(",".join(parts), 120)


def alive(server) -> bool:
    return server.process is not None and server.process.poll() is None


def revive(server) -> str:
    """How a dead mysqld ended; it is set up again from scratch."""
    code = server.process.wait() if server.process is not None else None
    log = server.error_log.read_text(errors="replace").splitlines() \
        if server.error_log.exists() else []
    marks = [line[:220] for line in log
             if "Assertion" in line or "got signal" in line or
             "MY-013183" in line or "terminate called" in line or
             "what():" in line or "MY-010443" in line]
    kept = WORK / f"mysqld-crash-{int(time.time())}.err"
    kept.write_text("\n".join(log[-150:]))
    died = f"mysqld exited with {code}"
    if isinstance(code, int) and code < 0:
        died += f" (signal {signal.Signals(-code).name})"
    died += ": " + " | ".join(marks)[-600:] + f" (log: {kept})"
    server.process = None
    shutil.rmtree(server.datadir, ignore_errors=True)
    server.error_log.unlink(missing_ok=True)
    server.start()
    return died


def gtid_executed(server) -> str:
    return server.sql("SELECT @@GLOBAL.gtid_executed").replace(
        "\\n", "").replace("\n", "").strip()


def rows(server) -> dict:
    """Every row of qa.t (id: CRC32, length and start of the payload) and
    the tables of the 'qa' database."""
    tables = sorted(server.sql(
        "SELECT table_name FROM information_schema.tables WHERE "
        "table_schema = 'qa'").split())
    others = {}
    for name in tables:
        if name != "t":
            # 'table, checksum' and the number of rows
            others[name] = server.sql(
                f"CHECKSUM TABLE qa.`{name}` EXTENDED").split()[-1] + \
                " (CHECKSUM TABLE), " + server.sql(
                    f"SELECT COUNT(*) FROM qa.`{name}`").strip() + " rows"
    if "t" not in tables:
        return {"t": None, "tables": tables, "others": others}
    table = {}
    for line in server.sql(
            "SELECT id, CRC32(payload), LENGTH(payload), LEFT(payload, 28) "
            "FROM qa.t ORDER BY id").splitlines():
        fields = line.split("\t")
        if len(fields) >= 4:
            table[int(fields[0])] = (fields[1], fields[2], fields[3])
    return {"t": table, "tables": tables, "others": others}


def digest(server) -> str:
    try:
        return server.sql(
            "SELECT COUNT(*), COALESCE(SUM(CRC32(CONCAT(id, ':', payload))), "
            "0) FROM qa.t").strip()
    except RuntimeError as error:
        if not alive(server):
            raise
        return "error: " + str(error).splitlines()[0][:80]


def row_difference(expected: dict, got: dict) -> "dict | None":
    if expected == got:
        return None
    source = expected["t"] or {}
    mine = got["t"] or {}
    difference = {"source_rows": len(source),
                  "rows": len(mine) if got["t"] is not None else
                  "no table qa.t"}
    missing = sorted(set(source) - set(mine))
    extra = sorted(set(mine) - set(source))
    other = sorted(key for key in set(source) & set(mine)
                   if source[key] != mine[key])
    if missing:
        difference["missing_rows"] = f"{len(missing)}: id {spans(missing)}"
        difference["first_missing"] = source[missing[0]][2]
    if extra:
        difference["rows_the_source_has_not"] = \
            f"{len(extra)}: id {spans(extra)}"
        difference["first_extra"] = mine[extra[0]][2]
    if other:
        difference["rows_with_another_value"] = [
            {"id": key, "source": source[key][2], "here": mine[key][2]}
            for key in other[:3]]
        difference["rows_with_another_value_count"] = len(other)
    if expected["tables"] != got["tables"]:
        difference["tables"] = {"source": expected["tables"],
                                "here": got["tables"]}
    for name, state in expected["others"].items():
        if name in got["others"] and got["others"][name] != state:
            difference[f"table_{name}"] = {"source": state,
                                           "here": got["others"][name]}
    return difference


# ---------------------------------------------------------------------------
# mysqlbinlog: the transactions it prints
# ---------------------------------------------------------------------------

GTID_NEXT = re.compile(r"^SET @@SESSION\.GTID_NEXT= '([^']*)'")
NOT_A_STATEMENT = ("#", "/*!", "SET ", "DELIMITER", "ROLLBACK", "use ",
                   "USE ", "BINLOG", "'")


def listing(arguments: list, split_on_begin: bool = False) -> dict:
    """'mysqlbinlog --verify-binlog-checksum -v <arguments>': its exit code,
    what it complains about, and the transactions it prints, each as (GTID,
    SHA-1 of its row changes and statements, a short description)."""
    command = tool("mysqlbinlog") + [
        "--verify-binlog-checksum", "--base64-output=DECODE-ROWS", "-v"] + \
        [str(argument) for argument in arguments]
    result = {"command": shown(command), "units": [], "messages": []}
    try:
        done = subprocess.run(command, capture_output=True,
                              timeout=LIST_SECONDS, check=False)
    except subprocess.TimeoutExpired:
        result["exit"] = "timeout"
        result["messages"] = [f"no end in {LIST_SECONDS} s"]
        return result
    result["exit"] = done.returncode
    text = done.stdout.decode(errors="replace")
    result["messages"] = [
        line.strip()[:220] for line in
        done.stderr.decode(errors="replace").splitlines()
        if line.strip() and "password on the command line" not in line]
    units = []
    current = None

    def start(gtid: str):
        unit = {"gtid": gtid, "hash": hashlib.sha1(), "brief": "",
                "committed": False, "rows": False}
        units.append(unit)
        return unit

    for line in text.splitlines():
        if line.startswith(("# Warning", "WARNING", "ERROR")):
            result["messages"].append(line[:220])
        match = GTID_NEXT.match(line)
        if match:
            current = None if match.group(1) == "AUTOMATIC" else \
                start(match.group(1))
            continue
        if line.startswith("### "):
            if current is None:
                current = start("")
            current["hash"].update(line.encode(errors="replace") + b"\n")
            current["rows"] = True
            if line.startswith("###   @") and current["brief"].count("@") < 2:
                current["brief"] += " " + line[6:60]
            elif not line.startswith("### SET") and \
                    not line.startswith("### WHERE") and not current["brief"]:
                current["brief"] = line[4:40]
            continue
        if line.startswith("BEGIN"):
            if split_on_begin:
                current = start("")
            continue
        if line.startswith("COMMIT"):
            # a transaction printed without it is rolled back by the
            # 'ROLLBACK' mysqlbinlog adds at the end
            if current is not None:
                current["hash"].update(b"COMMIT\n")
                current["committed"] = True
            continue
        if not line.strip() or line.startswith(NOT_A_STATEMENT):
            continue
        # a statement: DDL
        if current is None or split_on_begin:
            current = start("")
        current["hash"].update(line.encode(errors="replace") + b"\n")
        if not current["brief"]:
            current["brief"] = line[:60]
    result["units"] = [
        (unit["gtid"], unit["hash"].hexdigest()[:12], unit["brief"].strip() +
         (" (no COMMIT)" if unit["rows"] and not unit["committed"] else ""))
        for unit in units]
    return result


def judged_listing(reference: dict, actual: dict,
                   with_gtids: bool = True) -> dict:
    """The verdict for what mysqlbinlog printed for the storage ('actual')
    against what it prints for the source's binlogs ('reference')."""
    def keys(units):
        return [(gtid if with_gtids else "", content)
                for gtid, content, _ in units]

    def described(key, units):
        for gtid, content, brief in units:
            if (gtid if with_gtids else "", content) == key:
                return f"{gtid} {brief}".strip()
        return str(key)

    result = {"commands": [actual["command"]],
              "reference_command": reference["command"],
              "exit": actual["exit"],
              "transactions": len(actual["units"]),
              "source_transactions": len(reference["units"])}
    if actual["messages"]:
        result["messages"] = actual["messages"][:4]
    if reference["exit"] != 0:
        raise harness.Failure(
            f"mysqlbinlog fails on the source's own binlogs (exit "
            f"{reference['exit']}): {reference['messages'][:2]} "
            f"{reference['command']}")
    if actual["exit"] == "timeout":
        result["verdict"] = "hang"
        result["why"] = f"mysqlbinlog did not end in {LIST_SECONDS} s"
        return result
    wanted, got = keys(reference["units"]), keys(actual["units"])
    if wanted == got:
        result["verdict"] = "same"
        if actual["exit"] != 0:
            result["note"] = (f"mysqlbinlog exits with {actual['exit']} "
                              "after printing every transaction")
        return result
    lacking = list((collections.Counter(wanted) -
                    collections.Counter(got)).elements())
    surplus = list((collections.Counter(got) -
                    collections.Counter(wanted)).elements())
    difference = {}
    if lacking:
        difference["not_printed"] = [described(key, reference["units"])
                                     for key in lacking[:6]]
        difference["not_printed_count"] = len(lacking)
    if surplus:
        difference["printed_but_not_in_the_source"] = [
            described(key, actual["units"]) for key in surplus[:6]]
        difference["printed_but_not_in_the_source_count"] = len(surplus)
    if not lacking and not surplus:
        first = next(index for index, (left, right) in
                     enumerate(zip(wanted, got)) if left != right)
        difference["order"] = {
            "at_transaction": first + 1,
            "source": [described(key, reference["units"])
                       for key in wanted[first:first + 3]],
            "here": [described(key, actual["units"])
                     for key in got[first:first + 3]]}
    result["difference"] = difference
    if actual["exit"] != 0:
        result["verdict"] = "loud"
        return result
    result["verdict"] = "silent_wrong"
    result["why"] = (
        f"mysqlbinlog exits 0 and prints {len(got)} transaction(s) where "
        f"the same command on the source's binlogs prints {len(wanted)}: " +
        json.dumps(difference)[:700])
    return result


# ---------------------------------------------------------------------------
# the source's truth
# ---------------------------------------------------------------------------

def capture_truth() -> dict:
    """The source's rows, GTIDs and binlog files (as it sends them)."""
    directory = WORK / "truth"
    shutil.rmtree(directory, ignore_errors=True)
    directory.mkdir(parents=True)
    names = SOURCE.binlog_names()
    files = []
    stamps = []
    for name in names:
        data = SOURCE.binlog_as_sent(name)
        (directory / name).write_bytes(data)
        files.append(directory / name)
        stamps += [struct.unpack_from("<I", data, offset)[0]
                   for offset, code, _ in harness.parse_events(data)
                   if code in harness.TRANSACTION_START_EVENTS]
    truth = {"names": names, "files": files, "stamps": stamps,
             "rows": rows(SOURCE), "digest": digest(SOURCE),
             "executed": "", "purged": "",
             "uuid": SOURCE.sql("SELECT @@server_uuid").strip(),
             # the table's creation is in a binlog the source has purged
             "needs_table": names[0] != "binlog.000001"}
    if GTID:
        truth["executed"] = gtid_executed(SOURCE)
        truth["purged"] = SOURCE.sql("SELECT @@GLOBAL.gtid_purged").replace(
            "\\n", "").replace("\n", "").strip()
    return truth


def own_numbers(truth: dict) -> list:
    """The numbers of the source's own GTIDs that are in its binlogs."""
    executed = rw.parse_gtid_set(truth["executed"])
    purged = rw.parse_gtid_set(truth["purged"])
    return sorted(executed.get(truth["uuid"], set()) -
                  purged.get(truth["uuid"], set()))


def stop_second(truth: dict) -> "int | None":
    """A second with transactions of the source before it and from it on."""
    seconds = sorted(set(truth["stamps"]))
    return seconds[len(seconds) // 2] if len(seconds) > 1 else None


# ---------------------------------------------------------------------------
# the scratch server: replays and replicas
# ---------------------------------------------------------------------------

def reset_scratch(server, truth: dict) -> None:
    server.sql("STOP REPLICA; RESET REPLICA ALL; "
               "SET GLOBAL replica_parallel_workers = 4; "
               "DROP DATABASE IF EXISTS qa; CREATE DATABASE qa;")
    server.sql("RESET BINARY LOGS AND GTIDS"
               if server.version() >= (8, 2, 0) else "RESET MASTER")
    if truth["needs_table"]:
        server.sql("SET sql_log_bin = 0; " + TABLE)
    if truth["purged"]:
        server.sql(f"SET GLOBAL gtid_purged = '{truth['purged']}'")


def replay(server, truth: dict, arguments: list, reset: bool = True) -> dict:
    """mysqlbinlog <arguments> | mysql, into 'server' (emptied first)."""
    if reset:
        reset_scratch(server, truth)
    first = tool("mysqlbinlog") + [str(argument) for argument in arguments]
    second = tool("mysql") + [f"--socket={server.socket}", "-uroot",
                              "--max-allowed-packet=64M"]
    result = {"command": shown(first) + " | mysql", "errors": []}
    complaints = WORK / "mysqlbinlog.stderr"
    with complaints.open("wb") as stderr:
        producer = subprocess.Popen(first, stdout=subprocess.PIPE,
                                    stderr=stderr)
        client = subprocess.Popen(second, stdin=producer.stdout,
                                  stdout=subprocess.DEVNULL,
                                  stderr=subprocess.PIPE)
        producer.stdout.close()
        try:
            client_errors = client.communicate(timeout=REPLAY_SECONDS)[1]
            producer.wait(timeout=REPLAY_SECONDS)
        except subprocess.TimeoutExpired:
            for process in (producer, client):
                process.kill()
                process.wait()
            result.update(mysqlbinlog_exit="timeout", mysql_exit="timeout",
                          errors=[f"no end in {REPLAY_SECONDS} s"],
                          hang=True)
            return result
    result["mysql_exit"] = client.returncode
    # mysqlbinlog dies of SIGPIPE when the client has stopped at an error
    result["mysqlbinlog_exit"] = producer.returncode
    if client.returncode != 0:
        result["errors"].append("mysql: " + client_errors.decode(
            errors="replace").strip()[-300:])
    if producer.returncode not in (0, -signal.SIGPIPE) or \
            producer.returncode != 0 and client.returncode == 0:
        result["errors"].append("mysqlbinlog (exit %s): %s" % (
            producer.returncode, " ".join(
                line for line in complaints.read_text(
                    errors="replace").splitlines()
                if "password on the command line" not in line)[-300:]))
    return result


def judged_server(server, expected_rows: dict, expected_executed,
                  errors: list, result: dict) -> dict:
    """The verdict for a server that was to end with 'expected_rows' (and
    'expected_executed', unless None); 'errors' is what the operator was
    told."""
    got = rows(server)
    difference = row_difference(expected_rows, got)
    gtids = None
    if expected_executed is not None:
        wanted = rw.parse_gtid_set(expected_executed)
        mine = rw.parse_gtid_set(gtid_executed(server))
        if wanted != mine:
            gtids = {"lacks": rw.short(rw.format_gtid_set(
                         rw.subtract(wanted, mine))),
                     "has_more": rw.short(rw.format_gtid_set(
                         rw.subtract(mine, wanted)))}
    if errors:
        result["errors"] = [error[:300] for error in errors[:3]]
    if difference:
        result["rows_difference"] = difference
    if gtids:
        result["gtid_executed_difference"] = gtids
    if not difference and not gtids:
        result["verdict"] = "same"
    elif errors:
        result["verdict"] = "loud"
    else:
        result["verdict"] = "silent_wrong"
        result["why"] = ("no error anywhere, but not the source's data: " +
                         json.dumps({"rows": difference,
                                     "gtid_executed": gtids})[:800])
    return result


def replica_threads(server) -> dict:
    receiver = server.sql(
        "SELECT SERVICE_STATE, LAST_ERROR_NUMBER, LEFT(LAST_ERROR_MESSAGE, "
        "260) FROM performance_schema.replication_connection_status"
    ).strip().split("\t")
    coordinator = server.sql(
        "SELECT SERVICE_STATE, LAST_ERROR_NUMBER, LEFT(LAST_ERROR_MESSAGE, "
        "260) FROM performance_schema."
        "replication_applier_status_by_coordinator").strip().split("\t")
    worker = server.sql(
        "SELECT SERVICE_STATE, LAST_ERROR_NUMBER, LEFT(LAST_ERROR_MESSAGE, "
        "260) FROM performance_schema.replication_applier_status_by_worker "
        "ORDER BY LAST_ERROR_NUMBER DESC LIMIT 1").strip().split("\t")
    threads = {"receiver": receiver, "applier": worker,
               "coordinator": coordinator}
    threads["errors"] = [
        f"{name}: " + " ".join(fields) for name, fields in threads.items()
        if len(fields) > 1 and fields[1] not in ("0", "")]
    return threads


def replicate(state, start: str, workers: int = 4, seed: "list | None" = None,
              server=None) -> dict:
    """A replica of 'pull': 'start' are the CHANGE REPLICATION SOURCE TO
    options that say where it starts, 'seed' the mysqlbinlog arguments
    whose output it holds before it starts."""
    server = server or SCRATCH
    truth = state.truth
    result = {"start": start, "replica_parallel_workers": workers,
              "replica_version": ".".join(map(str, server.version()))}
    reset_scratch(server, truth)
    if seed is not None:
        seeded = replay(server, truth, seed, reset=False)
        result["replica_first_holds"] = seeded["command"]
        if seeded["errors"]:
            raise harness.Failure(f"seeding the replica failed: {seeded}")
        result["rows_before"] = digest(server)
    change = (f"CHANGE REPLICATION SOURCE TO SOURCE_HOST='127.0.0.1', "
              f"SOURCE_PORT={PULL_PORT}, SOURCE_USER="
              f"'{replication.PULL_USER}', SOURCE_PASSWORD="
              f"'{replication.PULL_PASSWORD}', GET_SOURCE_PUBLIC_KEY=1, "
              f"SOURCE_SSL=0, SOURCE_CONNECT_RETRY=2, {start}")
    log_mark = len(server.error_log.read_text(errors="replace").splitlines()) \
        if server.error_log.exists() else 0
    result["commands"] = [
        f"SET GLOBAL replica_parallel_workers = {workers}",
        change.replace(f"'{replication.PULL_PASSWORD}'", "'...'"),
        "START REPLICA"]
    server.sql(f"SET GLOBAL replica_parallel_workers = {workers}; {change}; "
               "START REPLICA;")
    started = time.monotonic()
    last = None
    stable_since = started
    threads = {"errors": []}
    while time.monotonic() - started < REPLICA_SECONDS:
        current = (digest(server), gtid_executed(server) if GTID else "")
        if current[0] == truth["digest"] and (
                not GTID or rw.parse_gtid_set(current[1]) ==
                rw.parse_gtid_set(truth["executed"])):
            break
        now = time.monotonic()
        threads = replica_threads(server)
        if threads["errors"] and now - started > 4:
            break
        if current != last:
            last, stable_since = current, now
        elif now - stable_since > STABLE_SECONDS and now - started > 6:
            break
        time.sleep(0.4)
    threads = replica_threads(server)
    server.sql("STOP REPLICA")
    result["seconds"] = round(time.monotonic() - started, 1)
    result["replica_threads"] = {name: fields[:2] for name, fields in
                                 threads.items() if name != "errors"}
    result = judged_server(server, truth["rows"],
                           truth["executed"] if GTID else None,
                           threads["errors"], result)
    if result["verdict"] != "same":
        # what the two error logs say (SHOW REPLICA STATUS shows neither)
        log = server.error_log.read_text(errors="replace").splitlines() \
            if server.error_log.exists() else []
        result["replica_error_log"] = [
            line.split("] ", 1)[-1][:230] for line in log[log_mark:]
            if "[ERROR]" in line or "[Warning] [MY-010897]" not in line and
            "[Warning]" in line and "[Repl]" in line][-3:]
        result["pull_log"] = [line[-200:] for line in
                              tamper.last_errors(harness.BINSRV)][-2:]
    return result


# ---------------------------------------------------------------------------
# 'pull' on the damaged storage
# ---------------------------------------------------------------------------

class Pull:
    """'binlog_server pull' that serves the storage while its own source
    cannot be reached (port +4), so that it stores nothing more."""

    def __init__(self, state):
        binsrv = harness.BINSRV
        config = json.loads(state.config.read_text())
        config["connection"]["port"] = DEAD_PORT
        config["connection"]["connect_timeout"] = 2
        config["connection"].pop("ssl", None)
        config["replication_source"] = {
            "port": PULL_PORT, "server_id": 142,
            "server_uuid": "00000000-0000-0000-0000-00000000008e",
            "read_timeout": 60, "write_timeout": 60,
            "require_secure_transport": False,
            "auth_rsa_private_key": str(replication.AUTH_RSA_PRIVATE_KEY),
            "authentication": {"user": replication.PULL_USER,
                               "password": replication.PULL_PASSWORD,
                               "plugin": "caching_sha2_password"}}
        self.config = binsrv.workdir / "binsrv-consumers.json"
        self.config.write_text(json.dumps(config, indent=2))
        self.stderr_path = binsrv.workdir / "pull-consumers.stderr"
        self.stderr = self.stderr_path.open("wb")
        self.process = subprocess.Popen(
            [str(binsrv.binary), "pull", str(self.config)],
            stdout=subprocess.DEVNULL, stderr=self.stderr,
            env=tamper.binsrv_environment())
        self.listening = False
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline and self.process.poll() is None:
            try:
                socket.create_connection(("127.0.0.1", PULL_PORT),
                                         timeout=1).close()
                self.listening = True
                break
            except OSError:
                time.sleep(0.1)

    def close(self) -> dict:
        code = self.process.poll()
        if code is None:
            self.process.send_signal(signal.SIGTERM)
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait()
        self.stderr.close()
        text = self.stderr_path.read_text(errors="replace")
        errors = [line for line in tamper.last_errors(harness.BINSRV)
                  if "establish connection" not in line and
                  "connect to" not in line.lower()]
        return {"exit": code, "sanitizer": rw.sanitizer_markers(text),
                "error": (errors or [text.strip()[-300:]])[-1][-300:]}


def serving(state) -> bool:
    """Makes sure 'pull' serves the state's storage; False if it refuses."""
    if state.pull is not None and state.pull.process.poll() is None:
        return True
    if state.pull is not None:
        state.pull_ends.append(state.pull.close())
        state.pull = None
    if state.pull_refused:
        return False
    pull = Pull(state)
    if not pull.listening:
        ended = pull.close()
        state.pull_refused = (f"'pull' does not serve the storage (exit "
                              f"{ended['exit']}): {ended['error']}")
        state.pull_ends.append(ended)
        return False
    state.pull = pull
    return True


def raw_dump(state, directory: Path) -> dict:
    """mysqlbinlog --raw --to-last-log from the first indexed binlog."""
    shutil.rmtree(directory, ignore_errors=True)
    directory.mkdir(parents=True)
    command = tool("mysqlbinlog") + remote() + [
        "--raw", "--to-last-log", f"--result-file={directory}/",
        state.indexed[0]]
    result = {"command": shown(command), "files": {}}
    try:
        done = subprocess.run(command, capture_output=True,
                              timeout=LIST_SECONDS, check=False)
        result["exit"] = done.returncode
        result["error"] = "; ".join(
            line for line in done.stderr.decode(errors="replace").splitlines()
            if "[Warning]" not in line)[-300:]
    except subprocess.TimeoutExpired:
        result["exit"] = "timeout"
        result["error"] = f"no end in {LIST_SECONDS} s"
    for path in sorted(directory.iterdir()):
        result["files"][path.name] = path.read_bytes()
    return result


def read_storage(state) -> None:
    """The stored binlogs in plaintext, in the order of binlog.index: from
    the storage directory, or for an encrypted storage as 'pull' serves
    them to mysqlbinlog --raw."""
    binsrv = harness.BINSRV
    state.indexed = binsrv.indexed_binlogs()
    state.plain = {}
    state.plain_error = ""
    directory = WORK / "plain"
    if not state.indexed:
        state.plain_error = "binlog.index names no binlog"
        return
    if state.encrypted:
        if not serving(state):
            state.plain_error = state.pull_refused
            return
        dump = raw_dump(state, directory)
        state.plain = {name: dump["files"][name] for name in state.indexed
                       if name in dump["files"]}
        if dump["exit"] != 0:
            state.plain_error = (
                f"mysqlbinlog --raw cannot read the encrypted storage "
                f"through 'pull' (exit {dump['exit']}: {dump['error']}); "
                f"{dump['command']}")
    else:
        shutil.rmtree(directory, ignore_errors=True)
        directory.mkdir(parents=True)
        for name in state.indexed:
            if (binsrv.storage / name).is_file():
                state.plain[name] = (binsrv.storage / name).read_bytes()
                (directory / name).write_bytes(state.plain[name])
    state.paths = [directory / name for name in state.indexed
                   if name in state.plain]


def stored_gtids(data: bytes) -> list:
    return rw.examine_stream({"x": data})["gtids"]


def damage(state) -> list:
    """What is wrong with the stored binlogs: the proof that the state was
    produced."""
    found = []
    if state.plain_error:
        found.append(state.plain_error)
    stream = rw.examine_stream(state.plain)
    found += stream["problems"][:4]
    if GTID:
        source = rw.subtract(rw.parse_gtid_set(state.truth["executed"]),
                             rw.parse_gtid_set(state.truth["purged"]))
        stored = rw.as_set(stream["gtids"])
        counts = collections.Counter(stream["gtids"])
        twice = [gtid for gtid, count in counts.items() if count > 1]
        if twice:
            found.append("stored twice: " + rw.short(
                rw.format_gtid_set(rw.as_set(twice))))
        order = [int(gtid.rpartition(":")[2]) for gtid in stream["gtids"]
                 if gtid.rpartition(":")[0] == state.truth["uuid"]]
        late = [f"{later} before {earlier}" for earlier, later in
                zip(order[1:], order) if later > earlier]
        if late:
            found.append("stored in another order: GTID " +
                         ", ".join(late[:3]))
        if rw.subtract(source, stored):
            found.append("not stored: " + rw.short(rw.format_gtid_set(
                rw.subtract(source, stored))))
        if rw.subtract(stored, source):
            found.append("stored but never executed by the source: " +
                         rw.short(rw.format_gtid_set(
                             rw.subtract(stored, source))))
    if any(replica_tamper.NEW_VALUE[:17] in data
           for data in state.plain.values()):
        found.append("a stored row holds the forged value")
    if state.indexed != sorted(state.indexed):
        found.append(f"binlog.index order: {state.indexed[-4:]}")
    return found


# ---------------------------------------------------------------------------
# the states
# ---------------------------------------------------------------------------

class State:
    def __init__(self, name: str, modes: tuple, produce, ticket: str = "",
                 rewrite: bool = False, encrypted: bool = False,
                 control: bool = False, light: bool = False):
        self.name = name
        self.modes = modes
        self.produce = produce
        self.ticket = ticket
        self.rewrite = rewrite
        self.encrypted = encrypted
        # run the consumers even if nothing is found wrong in the storage
        self.control = control
        # only one replica of 'pull' (the others take long and add nothing)
        self.light = light
        self.mysqld_deaths = 0
        self.config = None
        self.made = {}
        self.truth = None
        self.indexed = []
        self.plain = {}
        self.paths = []
        self.plain_error = ""
        self.pull = None
        self.pull_refused = ""
        self.pull_ends = []
        self.second_preamble_in = None
        self.layout = None


def hostile_arguments() -> SimpleNamespace:
    return SimpleNamespace(mysql_basedir=ARGS.mysql_basedir, port=ARGS.port,
                           storage="file", case_insensitive_dir=None)


def produce_control(state) -> None:
    machine = harness.FetchCrashMachine()
    machine.start_from_scratch()
    machine.run_workload(HOSTILE_WORKLOAD)
    SOURCE.sql("FLUSH BINARY LOGS")
    machine.run_workload(HOSTILE_WORKLOAD)
    machine.fetch_to_the_end()
    state.config = harness.BINSRV.config_path
    state.second_preamble_in = None


def produce_hostile(attack: str):
    def produce(state) -> None:
        made = hostile.run_case(attack, hostile_arguments(), PROXY_PORT)
        state.made = {key: made.get(key) for key in (
            "hostile_name", "events_rewritten", "proxied_fetch_exit",
            "second_preamble_appended", "format_descriptions_at",
            "injected_at_source_position", "positions_lowered_by",
            "direct_fetch_exit", "list_exit", "last_errors", "skipped",
            "why") if made.get(key) not in (None, [], "")}
        # 'fetch' with the storage written by the last write_config()
        state.config = harness.BINSRV.config_path
        doubled = made.get("second_preamble_appended") or []
        state.second_preamble_in = doubled[0] if doubled else None
    return produce


def produce_rewrite(case: str, function_name: str):
    def produce(state) -> None:
        # the base storage of the 'storage_*' cases is kept from one case
        # to the next, with the times of the first: made anew here
        rw.BASE_STATE.clear()
        made = getattr(rw, function_name)(case)
        state.made = {key: made.get(key) for key in (
            "edit", "events_changed", "attacked_fetch", "honest_fetch",
            "fetch", "refused", "note", "observation") if key in made}
        if isinstance(state.made.get("observation"), dict):
            state.made["observation"].pop("text", None)
        state.config = harness.BINSRV.workdir / "binsrv-rw.json"
    return produce


def produce_keyless(case: str):
    """A 'source_gtid_*' case of rewrite_attack_test.py in which the
    transactions sent during the attack insert into a table without any
    key, 'qa.k': a transaction applied twice is then no duplicate-key
    error."""
    inner = produce_rewrite(case, "run_source_case")

    def produce(state) -> None:
        original_reset, original_insert = SOURCE.reset, rw.insert_many

        def reset():
            original_reset()
            SOURCE.sql("CREATE TABLE qa.k (payload VARCHAR(200)) "
                       "ENGINE=InnoDB")

        def insert_many(marker: str, transactions: int, count: int) -> None:
            if marker != "DURING":
                original_insert(marker, transactions, count)
                return
            values = ",".join(f"('KEYLESS-{row:04d}-{'x' * 64}')"
                              for row in range(count))
            SOURCE.sql(f"INSERT INTO qa.k (payload) VALUES {values};" *
                       transactions)

        SOURCE.reset, rw.insert_many = reset, insert_many
        try:
            inner(state)
        finally:
            del SOURCE.reset
            rw.insert_many = original_insert
    return produce


def produce_tampered(case: str):
    def produce(state) -> None:
        arguments = SimpleNamespace(mysql_basedir=ARGS.mysql_basedir,
                                    port=ARGS.port, mode=ARGS.mode)
        if case == "forged_row":
            made = replica_tamper.case_forged_row(arguments, SOURCE, SCRATCH,
                                                  True)
        else:
            made = replica_tamper.case_plaintext_swap(arguments, SOURCE,
                                                      SCRATCH)
        state.made = {key: made.get(key) for key in (
            "binlog", "event_offset_and_size", "pull_refused_the_binlog",
            "replica_has_the_forged_value") if key in made}
        state.config = replica_tamper.configure(harness.BINSRV, True)
    return produce


BOTH = ("position", "gtid")
STATES = [
    State("S0_control", BOTH, produce_control, control=True),
    State("S0_rewrite_control", ("gtid",),
          produce_rewrite("source_control", "run_source_case"),
          rewrite=True, control=True),
    State("S1_reannounce", ("position",), produce_hostile("reannounce"),
          "69"),
    State("S1_midstream", ("gtid",), produce_hostile("reannounce_midstream"),
          "69"),
    State("S2_gtid_twice", ("gtid",),
          produce_rewrite("source_gtid_twice", "run_source_case"), "81",
          rewrite=True),
    State("S2_gtid_swapped", ("gtid",),
          produce_rewrite("source_gtid_swapped", "run_source_case"), "81",
          rewrite=True),
    State("S2_gtid_other_uuid", ("gtid",),
          produce_rewrite("source_gtid_other_uuid", "run_source_case"), "81",
          rewrite=True),
    State("S2_gtid_twice_keyless", ("gtid",),
          produce_keyless("source_gtid_twice"), "81", rewrite=True,
          light=True),
    State("S2_gtid_other_uuid_keyless", ("gtid",),
          produce_keyless("source_gtid_other_uuid"), "81", rewrite=True,
          light=True),
    State("S2_tail_added_emptied", ("gtid",),
          produce_rewrite("storage_tail_added_emptied", "run_storage_case"),
          "81", rewrite=True, encrypted=True),
    State("S2_tail_previous_emptied", ("gtid",),
          produce_rewrite("storage_tail_previous_emptied",
                          "run_storage_case"),
          "81", rewrite=True, encrypted=True),
    State("S2_tail_added_other_uuid", ("gtid",),
          produce_rewrite("storage_tail_added_other_uuid",
                          "run_storage_case"),
          "81", rewrite=True, encrypted=True),
    State("S2_tail_added_extended", ("gtid",),
          produce_rewrite("storage_tail_added_extended", "run_storage_case"),
          "81", rewrite=True, encrypted=True),
    State("S3_length_smaller", ("gtid",),
          produce_rewrite("source_length_smaller", "run_source_case"), "82",
          rewrite=True),
    State("S4_forged_row", BOTH, produce_tampered("forged_row"), "53",
          encrypted=True, light=True),
    State("S4_plaintext_swap", BOTH, produce_tampered("plaintext_swap"),
          "62", encrypted=True, light=True),
    State("S5_lower_sequence", BOTH, produce_hostile("lower_sequence"),
          "70"),
    State("S6_gtid_intervals", ("gtid",),
          produce_rewrite("rotation_gtid_intervals",
                          "run_rotation_gtid_intervals"),
          rewrite=True, control=True, light=True),
    State("S6_large_transaction", ("gtid",),
          produce_rewrite("rotation_large_transaction",
                          "run_rotation_large_transaction"),
          rewrite=True, control=True),
]


def find_layout(state) -> "dict | None":
    """The positions the position cases use. With a second preamble in
    'binlog': where it starts and ends in the stored file, the source's
    binlogs that hold what follows it, and a transaction boundary of the
    source ('source_position') that lies after it."""
    if state.rewrite or state.name.startswith(("S4", "S5")):
        return None
    truth = state.truth
    names = truth["names"]

    def starts(name: str) -> list:
        return [offset for offset, code, _ in harness.parse_events(
            (WORK / "truth" / name).read_bytes())
                if code in harness.TRANSACTION_START_EVENTS]

    def truth_from(name: str) -> list:
        return truth["files"][names.index(name):]

    def plain_from(name: str) -> list:
        return state.paths[state.indexed.index(name):]

    binlog = state.second_preamble_in
    if binlog is None:
        # the control: the source's last binlog
        binlog = names[-1]
        if binlog not in state.plain or len(starts(binlog)) < 3:
            return None
        boundaries = starts(binlog)
        return {"binlog": binlog, "second_preamble": None,
                "start_position": boundaries[1],
                "start_truth": truth_from(binlog),
                "start_plain": plain_from(binlog),
                "stop_position": boundaries[-1],
                "stop_truth": truth_from(binlog),
                "stop_plain": plain_from(binlog),
                "source_position": boundaries[1],
                "source_seed": truth["files"][:names.index(binlog) + 1]}
    if binlog not in state.plain or binlog not in names:
        return None
    events = harness.parse_events(state.plain[binlog])
    preambles = [offset for offset, code, _ in events
                 if code == harness.FORMAT_DESCRIPTION_EVENT]
    if len(preambles) < 2:
        return None
    second = preambles[1]
    after = [offset for offset, code, _ in events if offset > second and
             code not in (harness.FORMAT_DESCRIPTION_EVENT,
                          harness.PREVIOUS_GTIDS_LOG_EVENT)]
    layout = {"binlog": binlog, "second_preamble": second,
              "after_preamble": after[0] if after else None}
    upto = truth["files"][:names.index(binlog) + 1]
    if state.name == "S1_midstream":
        # the source's binlog of the same name; 'second' is the position of
        # the transaction that follows the injected preamble
        later = [offset for offset in starts(binlog) if offset > second]
        if len(later) < 3:
            return None
        layout.update(
            start_position=later[1], start_truth=truth_from(binlog),
            start_plain=plain_from(binlog),
            stop_position=later[-1], stop_truth=truth_from(binlog),
            stop_plain=plain_from(binlog),
            # what a reader that starts at the second preamble must get
            middle_truth=[f"--start-position={second}"] + truth_from(binlog),
            middle_seed=[f"--stop-position={second}"] + upto,
            source_position=later[1])
    else:
        # 'reannounce': the appended part is the source's next binlog
        following = names[names.index(binlog) + 1:]
        if not following or len(starts(binlog)) < 3 or \
                len(starts(following[-1])) < 2:
            return None
        layout.update(
            start_position=starts(binlog)[1],
            start_truth=truth_from(binlog), start_plain=plain_from(binlog),
            stop_position=starts(binlog)[-1],
            stop_truth=[truth["files"][names.index(binlog)]],
            stop_plain=[state.paths[state.indexed.index(binlog)]],
            middle_truth=truth_from(following[0]), middle_seed=upto,
            source_position=starts(binlog)[1])
        if following[0] not in state.indexed:
            # the source's binlog that the storage has under another name
            layout["source_binlog"] = following[0]
    layout["source_seed"] = upto
    return layout


# ---------------------------------------------------------------------------
# the consumers
# ---------------------------------------------------------------------------

def unreadable(state) -> "dict | None":
    if state.plain_error and not state.paths:
        return {"verdict": "loud", "error": state.plain_error}
    return None


def not_served(state) -> "dict | None":
    if not serving(state):
        return {"verdict": "loud", "error": state.pull_refused}
    return None


def middle_gtids(state) -> str:
    numbers = own_numbers(state.truth)
    low = numbers[len(numbers) // 3]
    high = numbers[2 * len(numbers) // 3]
    return f"{state.truth['uuid']}:{low}-{high}"


def c1_files(state) -> dict:
    return unreadable(state) or judged_listing(
        listing(state.truth["files"]), listing(state.paths))


def c1_one_binlog(state) -> dict:
    """The one binlog of the layout against the source's of that name."""
    name = state.layout["binlog"]
    return judged_listing(
        listing([WORK / "truth" / name]),
        listing([state.paths[state.indexed.index(name)]]))


def c1_start_position(state) -> dict:
    layout = state.layout
    option = [f"--start-position={layout['start_position']}"]
    result = judged_listing(listing(option + layout["start_truth"]),
                            listing(option + layout["start_plain"]))
    result["position"] = (f"{layout['start_position']}: where a transaction "
                          f"starts in the source's {layout['binlog']}")
    return result


def c1_stop_position(state) -> dict:
    layout = state.layout
    option = [f"--stop-position={layout['stop_position']}"]
    result = judged_listing(listing(option + layout["stop_truth"]),
                            listing(option + layout["stop_plain"]))
    result["position"] = (
        f"{layout['stop_position']}: where the last transaction starts in "
        f"the source's {Path(str(layout['stop_truth'][-1])).name}")
    return result


def c1_gtid_option(option: str):
    def consumer(state) -> dict:
        if unreadable(state):
            return unreadable(state)
        arguments = [option] if option == "--skip-gtids" else \
            [f"{option}={middle_gtids(state)}"]
        skip = option == "--skip-gtids"
        return judged_listing(
            listing(arguments + state.truth["files"], split_on_begin=skip),
            listing(arguments + state.paths, split_on_begin=skip),
            with_gtids=not skip)
    return consumer


def c2_replay(options: tuple = (), compare_gtids: bool = True):
    def consumer(state) -> dict:
        if unreadable(state):
            return unreadable(state)
        truth = state.truth
        arguments = list(options)
        files, stored = truth["files"], state.paths
        if "--stop-position" in arguments:
            layout = state.layout
            arguments = [f"--stop-position={layout['stop_position']}"]
            files, stored = layout["stop_truth"], layout["stop_plain"]
            # with what the binlogs before these hold
            earlier = truth["names"].index(Path(str(files[0])).name)
            files = truth["files"][:earlier] + files
            stored = state.paths[:state.paths.index(stored[0])] + stored
        if "--stop-datetime" in arguments:
            second = stop_second(truth)
            if second is None:
                return {"verdict": "skipped",
                        "skipped": "all of the source's transactions carry "
                                   "the same second"}
            arguments = ["--stop-datetime=" + time.strftime(
                "%Y-%m-%d %H:%M:%S", time.localtime(second))]
        # what the same command gives for the source's own binlogs
        reference = replay(SCRATCH, truth, arguments + files)
        if reference["errors"]:
            raise harness.Failure("the source's own binlogs do not replay: "
                                  f"{reference}")
        expected = rows(SCRATCH)
        expected_executed = gtid_executed(SCRATCH) \
            if GTID and compare_gtids else None
        if not arguments and (
                expected != truth["rows"] or GTID and
                rw.parse_gtid_set(expected_executed) !=
                rw.parse_gtid_set(truth["executed"])):
            raise harness.Failure(
                "a replay of the source's own binlogs does not give the "
                f"source's data: {row_difference(truth['rows'], expected)}")
        outcome = replay(SCRATCH, truth, arguments + stored)
        result = {"commands": [outcome["command"]],
                  "reference_command": reference["command"],
                  "mysqlbinlog_exit": outcome["mysqlbinlog_exit"],
                  "mysql_exit": outcome["mysql_exit"],
                  "expected_rows": len(expected["t"] or {})}
        if outcome.get("hang"):
            result.update(verdict="hang", why=outcome["errors"][0])
            return result
        if state.rewrite and "--stop-datetime" in options:
            # mysqlbinlog stops at the first event from that second on,
            # and the PREVIOUS_GTIDS_LOG and ROTATE events of a rewritten
            # binlog carry the time 'fetch' wrote them
            result["note"] = (
                "in rewrite mode the outcome depends on when 'fetch' ran: "
                "the events it generates carry its clock, not the "
                "source's (always wrong when one fetch stores everything "
                "afterwards, as in S6)")
        errors = list(outcome["errors"])
        if state.plain_error:
            errors.append(state.plain_error)
        return judged_server(SCRATCH, expected, expected_executed, errors,
                             result)
    return consumer


def c3_raw(state) -> dict:
    refused = not_served(state)
    if refused:
        return refused
    dump = raw_dump(state, WORK / "raw")
    result = {"commands": [dump["command"]], "exit": dump["exit"],
              "files": {name: len(data)
                        for name, data in list(dump["files"].items())[-4:]}}
    if dump["exit"] == "timeout":
        result.update(verdict="hang", why=dump["error"])
        return result
    binsrv = harness.BINSRV
    problems = []
    for name in state.indexed:
        stored = binsrv.storage / name
        got = dump["files"].get(name)
        if got is None:
            problems.append(f"{name} is not in the copy")
        elif not state.encrypted and stored.is_file() and \
                got != stored.read_bytes():
            problems.append(
                f"{name}: the copy ({len(got)} bytes) differs from the "
                f"stored file ({stored.stat().st_size} bytes) at byte "
                f"{tamper.first_difference(got, stored.read_bytes())}")
        elif state.encrypted and stored.is_file() and \
                len(got) != stored.stat().st_size:
            problems.append(f"{name}: the copy has {len(got)} bytes, the "
                            f"stored file {stored.stat().st_size}")
    problems += [f"{name} is copied but not indexed"
                 for name in dump["files"] if name not in state.indexed]
    # what the copy holds against the source
    copied = [WORK / "raw" / name for name in state.indexed
              if name in dump["files"]]
    against = judged_listing(listing(state.truth["files"]), listing(copied)) \
        if copied else {"verdict": "loud", "difference": "nothing copied"}
    result["transactions_in_the_copy"] = against.get("transactions")
    result["source_transactions"] = against.get("source_transactions")
    if dump["exit"] != 0:
        result.update(verdict="loud", error=dump["error"])
    elif problems:
        if against["verdict"] != "same":
            problems = problems[:3] + [
                "the transactions of the copy against those of the "
                "source's binlogs: " +
                json.dumps(against.get("difference"))[:500]]
        result.update(verdict="silent_wrong",
                      why="mysqlbinlog --raw exits 0, but: " +
                          "; ".join(problems[:4]))
    else:
        # 'same' as the storage here: C1_files has the stored files
        # against the source
        result["verdict"] = "same"
        result["note"] = "identical to the stored files" \
            if not state.encrypted else "as many bytes as the stored files"
    return result


def c3_remote(state) -> dict:
    refused = not_served(state)
    if refused:
        return refused
    return judged_listing(
        listing(state.truth["files"]),
        listing(remote() + ["--to-last-log", state.indexed[0]]))


def c3_remote_start(where: str):
    def consumer(state) -> dict:
        refused = not_served(state)
        if refused:
            return refused
        layout = state.layout
        position = layout[where]
        if where == "source_position":
            reference = listing([f"--start-position={position}"] +
                                layout["start_truth"])
        else:
            reference = listing(layout["middle_truth"])
        result = judged_listing(reference, listing(
            remote() + ["--to-last-log", f"--start-position={position}",
                        layout["binlog"]]))
        result["position"] = f"{layout['binlog']}:{position} ({where})"
        return result
    return consumer


def c3_remote_source_binlog(state) -> dict:
    refused = not_served(state)
    if refused:
        return refused
    name = state.layout["source_binlog"]
    return judged_listing(
        listing(state.truth["files"][state.truth["names"].index(name):]),
        listing(remote() + ["--to-last-log", name]))


def c4_position_replica_source_binlog(state) -> dict:
    refused = not_served(state)
    if refused:
        return refused
    name = state.layout["source_binlog"]
    return replicate(
        state, f"SOURCE_LOG_FILE='{name}', SOURCE_LOG_POS=4, "
               "SOURCE_AUTO_POSITION=0",
        seed=state.truth["files"][:state.truth["names"].index(name)])


def native_start(state) -> str:
    return "SOURCE_AUTO_POSITION=1" if GTID else \
        f"SOURCE_LOG_FILE='{state.indexed[0]}', SOURCE_LOG_POS=4"


def c4_position_replica(state) -> dict:
    return not_served(state) or replicate(
        state, f"SOURCE_LOG_FILE='{state.indexed[0]}', SOURCE_LOG_POS=4, "
               "SOURCE_AUTO_POSITION=0")


def c4_position_replica_at(where: str):
    def consumer(state) -> dict:
        refused = not_served(state)
        if refused:
            return refused
        layout = state.layout
        position = layout[where]
        seed = [f"--stop-position={position}"] + layout["source_seed"] \
            if where == "source_position" else layout["middle_seed"]
        result = replicate(
            state, f"SOURCE_LOG_FILE='{layout['binlog']}', "
                   f"SOURCE_LOG_POS={position}, SOURCE_AUTO_POSITION=0",
            seed=seed)
        result["position"] = f"{layout['binlog']}:{position} ({where})"
        return result
    return consumer


def c4_gtid_replica(state) -> dict:
    return not_served(state) or replicate(state, "SOURCE_AUTO_POSITION=1")


def c4_gtid_replica_partway(state) -> dict:
    refused = not_served(state)
    if refused:
        return refused
    truth = state.truth
    numbers = own_numbers(truth)
    cut = numbers[len(numbers) // 2 - 1]
    layout = state.layout
    if layout and layout.get("second_preamble"):
        # one transaction further than the second preamble
        before = len([gtid for gtid in stored_gtids(
            state.plain[layout["binlog"]][:layout["second_preamble"]])])
        earlier = sum(len(stored_gtids(state.plain[name]))
                      for name in state.indexed[
                          :state.indexed.index(layout["binlog"])])
        cut = numbers[min(earlier + before, len(numbers) - 2)]
    have = f"{truth['uuid']}:1-{cut}"
    result = replicate(state, "SOURCE_AUTO_POSITION=1",
                       seed=[f"--include-gtids={have}"] + truth["files"])
    result["replica_had"] = have
    return result


def c4_single_worker_replica(state) -> dict:
    return not_served(state) or replicate(state, native_start(state),
                                          workers=1)


def c4_other_version(version: str):
    def consumer(state) -> dict:
        refused = not_served(state)
        if refused:
            return refused
        server = OTHER_REPLICAS[version]
        if not alive(server):
            server.start()
        return replicate(state, native_start(state), server=server)
    return consumer


def file_timestamps(data: bytes) -> list:
    return [struct.unpack_from("<I", data, offset)[0]
            for offset, _, _ in harness.parse_events(data)]


def c5_list(state) -> dict:
    answer = rw.query(state.config, "list")
    result = {"commands": [f"binlog_server list {shown([state.config])}"],
              "exit": answer["exit"], "status": answer["status"]}
    if answer["exit"] != 0 or answer["status"] != "success":
        result.update(verdict="loud", error=answer["message"])
        if isinstance(answer["exit"], int) and answer["exit"] not in (0, 1):
            result.update(verdict="crash",
                          why=f"'list' exited with {answer['exit']}")
        return result
    entries = [entry for entry in answer["result"]
               if isinstance(entry, dict)]
    result["listed"] = [f"{entry.get('name')}:{entry.get('size')}"
                        for entry in entries[-4:]]
    problems = []
    listed = {}
    for entry in entries:
        name = str(entry.get("name"))
        data = state.plain.get(name)
        added = rw.parse_gtid_set(str(entry.get("added_gtids") or ""))
        listed = rw.union(listed, added)
        if data is None:
            problems.append(f"{name} is listed but cannot be read")
            continue
        if entry.get("size") != len(data):
            problems.append(f"{name}: listed with {entry.get('size')} "
                            f"bytes, holds {len(data)}")
        if GTID:
            held = rw.as_set(stored_gtids(data))
            if held != added:
                problems.append(
                    f"{name}: 'added_gtids' is "
                    f"'{rw.short(str(entry.get('added_gtids')), 80)}', the "
                    f"binlog holds '{rw.short(rw.format_gtid_set(held), 80)}'")
    names = [str(entry.get("name")) for entry in entries]
    if names != state.indexed:
        problems.append(f"lists {names[-4:]}, binlog.index names "
                        f"{state.indexed[-4:]}")
    if GTID:
        source = rw.subtract(rw.parse_gtid_set(state.truth["executed"]),
                             rw.parse_gtid_set(state.truth["purged"]))
        if listed != source:
            lacking = rw.format_gtid_set(rw.subtract(source, listed))
            surplus = rw.format_gtid_set(rw.subtract(listed, source))
            problems.append(
                f"the listed 'added_gtids' lack '{rw.short(lacking, 80)}' "
                f"of the source's GTIDs and have '{rw.short(surplus, 80)}' "
                "that the source has not")
    elif not state.rewrite and \
            [name for name in state.truth["names"] if name not in names]:
        problems.append(
            "the source's %s is not listed" % ", ".join(
                name for name in state.truth["names"] if name not in names))
    if problems:
        result.update(verdict="silent_wrong",
                      why="'list' answers 'success', but: " +
                          "; ".join(problems[:5]))
    else:
        result["verdict"] = "same"
    return result


def c5_search_by_gtid_set(state) -> dict:
    truth = state.truth
    numbers = own_numbers(truth)
    step = max(1, len(numbers) // 10)
    probes = [truth["executed"]] + [
        f"{truth['uuid']}:{number}"
        for number in sorted(set(numbers[::step] + numbers[-2:]))]
    holders = {name: rw.as_set(stored_gtids(data))
               for name, data in state.plain.items()}
    result = {"commands": ["binlog_server search_by_gtid_set "
                           f"{shown([state.config])} <set>"],
              "probes": len(probes)}
    wrong = []
    refusals = []
    for probe in probes:
        wanted = rw.subtract(rw.parse_gtid_set(probe),
                             rw.parse_gtid_set(truth["purged"]))
        answer = rw.query(state.config, "search_by_gtid_set", probe)
        if isinstance(answer["exit"], int) and answer["exit"] not in (0, 1) \
                or answer["exit"] == "timeout":
            result.update(verdict="crash", why=f"search_by_gtid_set "
                          f"'{rw.short(probe, 60)}' exited with "
                          f"{answer['exit']}")
            return result
        if answer["status"] != "success":
            refusals.append(f"'{rw.short(probe, 60)}': {answer['message']}")
            continue
        named = [str(entry.get("name")) for entry in answer["result"]
                 if isinstance(entry, dict)]
        covered = {}
        for name in named:
            covered = rw.union(covered, holders.get(name, {}))
        lacking = rw.subtract(wanted, covered)
        if lacking:
            elsewhere = sorted(
                name for name, held in holders.items() if name not in named
                and any(held.get(sid, set()) & gtids
                        for sid, gtids in lacking.items()))
            wrong.append(
                f"'{rw.short(probe, 60)}': answers 'success' with "
                f"{named[-3:]}, which do not hold "
                f"'{rw.short(rw.format_gtid_set(lacking), 60)}' (" +
                (f"it is in {elsewhere[:3]}" if elsewhere else
                 "no stored binlog holds it") + ")")
    if refusals:
        result["refused"] = refusals[:3]
    if wrong:
        result.update(verdict="silent_wrong", why="; ".join(wrong[:4]),
                      wrong_answers=len(wrong))
    else:
        result["verdict"] = "loud" if refusals else "same"
    return result


def c5_search_by_timestamp(state) -> dict:
    """Two moments: a second in the middle of the source's transactions and
    the second of its last one. The answer must name every binlog that
    holds a transaction up to then."""
    stamps = state.truth["stamps"] or [0]
    result = {"commands": [], "answers": []}
    wrong = []
    refusals = []
    for second in sorted(set([stop_second(state.truth) or stamps[-1],
                              max(stamps)])):
        moment = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(second))
        answer = rw.query(state.config, "search_by_timestamp", moment)
        result["commands"].append("binlog_server search_by_timestamp "
                                  f"{shown([state.config])} {moment}")
        if isinstance(answer["exit"], int) and answer["exit"] not in (0, 1) \
                or answer["exit"] == "timeout":
            result.update(verdict="crash", why=f"search_by_timestamp "
                          f"{moment} exited with {answer['exit']}")
            return result
        if answer["status"] != "success":
            refusals.append(f"{moment}: {answer['message']}")
            continue
        named = [str(entry.get("name")) for entry in answer["result"]
                 if isinstance(entry, dict)]
        wanted = [name for name in state.indexed
                  if name in state.plain and any(
                      struct.unpack_from("<I", state.plain[name],
                                         offset)[0] <= second
                      for offset, code, _ in
                      harness.parse_events(state.plain[name])
                      if code in harness.TRANSACTION_START_EVENTS)]
        result["answers"].append({"moment": moment, "named": named[-4:],
                                  "hold_a_transaction_up_to_then":
                                      wanted[-4:]})
        lacking = [name for name in wanted if name not in named]
        if lacking:
            wrong.append(f"search_by_timestamp {moment} answers 'success' "
                         f"with {named[-3:]}, without {lacking[:4]}, which "
                         "hold transactions up to that second")
    if refusals:
        result["refused"] = refusals
    if wrong:
        result.update(verdict="silent_wrong", why="; ".join(wrong))
    else:
        result["verdict"] = "loud" if refusals else "same"
    return result


def c5_resume(state) -> dict:
    """Three more transactions and a 'fetch' on top; then all of the
    storage is replayed. Changes the source and the storage: the last case
    of a state."""
    if state.pull is not None:
        state.pull_ends.append(state.pull.close())
        state.pull = None
    rw.insert_many("RESUMEMARKER", 3, 2)
    fetched = rw.brief(rw.command("fetch", state.config,
                                  state.name + "-resume"))
    result = {"commands": ["3 x INSERT on the source",
                           f"binlog_server fetch {shown([state.config])}"],
              "fetch": fetched}
    violation = tamper.command_violation("fetch", dict(fetched, peak_rss_mb=
                                         fetched.get("peak_rss_mb", 0)))
    if violation:
        result.update(verdict="crash", why=violation)
        return result
    if fetched["exit"] != 0:
        result.update(verdict="loud", error=fetched["message"])
        return result
    state.truth = capture_truth()
    state.pull_refused = ""
    read_storage(state)
    if unreadable(state):
        return dict(result, **unreadable(state))
    outcome = replay(SCRATCH, state.truth, state.paths)
    result["commands"].append(outcome["command"])
    result["storage_now"] = damage(state)[:4]
    if outcome.get("hang"):
        result.update(verdict="hang", why=outcome["errors"][0])
        return result
    errors = list(outcome["errors"])
    if state.plain_error:
        errors.append(state.plain_error)
    return judged_server(SCRATCH, state.truth["rows"],
                         state.truth["executed"] if GTID else None, errors,
                         result)


def c3_pull(state) -> dict:
    """How the 'pull' processes that served this state ended."""
    if state.pull is not None:
        state.pull_ends.append(state.pull.close())
        state.pull = None
    result = {"pull_processes": len(state.pull_ends),
              "ends": [{key: value for key, value in ended.items()
                        if value not in ([], "")}
                       for ended in state.pull_ends][:4]}
    crashed = [ended for ended in state.pull_ends if ended["sanitizer"] or
               ended["exit"] not in (None, 0, 1)]
    if crashed:
        result.update(verdict="crash", why="'pull' died while it served "
                      f"the storage: {crashed[0]}")
    elif state.pull_refused or any(ended["exit"] is not None
                                   for ended in state.pull_ends):
        result.update(verdict="loud", error=state.pull_refused or
                      state.pull_ends[0]["error"])
    else:
        result["verdict"] = "same"
        result["note"] = "served every consumer and ran until it was stopped"
    return result


def has_layout(key: str):
    return lambda state: state.layout is not None and \
        state.layout.get(key) is not None


def is_s1(key: str):
    return lambda state: state.layout is not None and \
        state.layout.get("second_preamble") is not None and \
        state.layout.get(key) is not None


def in_gtid_mode(_state) -> bool:
    return GTID


def more_replicas(state) -> bool:
    return not state.light


def position_replica(state) -> bool:
    return not (state.light and GTID)


def partway_replica(state) -> bool:
    return GTID and not state.light


def always(_state) -> bool:
    return True


# (name, applies to the state, consumer, the name's states for --cases)
CONSUMERS = [
    ("C1_files", always, c1_files),
    ("C1_one_binlog", has_layout("binlog"), c1_one_binlog),
    ("C1_start_position", has_layout("start_position"), c1_start_position),
    ("C1_stop_position", has_layout("stop_position"), c1_stop_position),
    ("C1_include_gtids", in_gtid_mode, c1_gtid_option("--include-gtids")),
    ("C1_exclude_gtids", in_gtid_mode, c1_gtid_option("--exclude-gtids")),
    ("C1_skip_gtids", in_gtid_mode, c1_gtid_option("--skip-gtids")),
    ("C2_replay", always, c2_replay()),
    ("C2_replay_skip_gtids", in_gtid_mode,
     c2_replay(("--skip-gtids",), compare_gtids=False)),
    ("C2_replay_stop_datetime", always, c2_replay(("--stop-datetime",))),
    ("C2_replay_stop_position", has_layout("stop_position"),
     c2_replay(("--stop-position",))),
    ("C3_raw", always, c3_raw),
    ("C3_remote", always, c3_remote),
    ("C3_remote_start_second_preamble", is_s1("second_preamble"),
     c3_remote_start("second_preamble")),
    ("C3_remote_start_after_preamble", is_s1("after_preamble"),
     c3_remote_start("after_preamble")),
    ("C3_remote_start_source_position", has_layout("source_position"),
     c3_remote_start("source_position")),
    ("C3_remote_source_binlog", has_layout("source_binlog"),
     c3_remote_source_binlog),
    ("C4_position_replica", position_replica, c4_position_replica),
    ("C4_position_replica_second_preamble", is_s1("second_preamble"),
     c4_position_replica_at("second_preamble")),
    ("C4_position_replica_after_preamble", is_s1("after_preamble"),
     c4_position_replica_at("after_preamble")),
    ("C4_position_replica_source_position", has_layout("source_position"),
     c4_position_replica_at("source_position")),
    ("C4_position_replica_source_binlog", has_layout("source_binlog"),
     c4_position_replica_source_binlog),
    ("C4_gtid_replica", in_gtid_mode, c4_gtid_replica),
    ("C4_gtid_replica_partway", partway_replica, c4_gtid_replica_partway),
    ("C4_single_worker_replica", more_replicas, c4_single_worker_replica),
    ("C3_pull", always, c3_pull),
    ("C5_list", always, c5_list),
    ("C5_search_by_gtid_set", in_gtid_mode, c5_search_by_gtid_set),
    ("C5_search_by_timestamp", always, c5_search_by_timestamp),
    ("C5_resume", always, c5_resume),
]


# ---------------------------------------------------------------------------

def selected(name: str) -> bool:
    return not ARGS.cases or any(part in name for part in ARGS.cases)


def run_consumer(state, name: str, consumer) -> dict:
    case = f"{state.name}/{name}"
    started = time.monotonic()
    if name.startswith("C4_") and state.mysqld_deaths >= ARGS.mysqld_deaths:
        return {"case": case, "ok": True, "verdict": "skipped",
                "skipped": f"the mysqld of {state.mysqld_deaths} replica(s) "
                           "of this state died already (each takes 20 s to "
                           "set up again; --mysqld-deaths)"}
    try:
        result = consumer(state)
    except subprocess.TimeoutExpired as expired:
        result = {"verdict": "hang",
                  "why": f"no end in {expired.timeout} s: " +
                         shown(expired.cmd if isinstance(expired.cmd, list)
                               else [expired.cmd])[:200]}
    except (harness.Failure, RuntimeError, subprocess.SubprocessError,
            OSError, KeyError, IndexError, ValueError) as failure:
        result = {"verdict": "harness_error",
                  "why": harness.describe_failure(failure, 700)}
    # a mysqld that died of what it was given
    for server in [SCRATCH] + list(OTHER_REPLICAS.values()):
        if server.process is not None and not alive(server):
            result["verdict"] = "crash"
            result["why"] = ("the mysqld of the consumer died: " +
                             revive(server))
            state.mysqld_deaths += 1
    if not alive(SOURCE):
        raise harness.Failure("the source died: " + revive(SOURCE))
    if state.pull is not None and state.pull.process.poll() is not None:
        ended = state.pull.close()
        state.pull_ends.append(ended)
        state.pull = None
        result["pull_exited"] = ended
        if ended["sanitizer"] or ended["exit"] not in (0, 1):
            result["verdict"] = "crash"
            result["why"] = f"'pull' died while it served this: {ended}"
    result = dict({"case": case, "ok": result["verdict"] in PASSING},
                  **result)
    if state.ticket:
        result["ticket"] = state.ticket
    if ARGS.mode == "gtid":
        result["mode"] = "gtid"
    result["seconds"] = round(time.monotonic() - started, 1)
    return result


def run_state(state, emit) -> None:
    other_versions = [
        (f"C4_replica_{version}", always, c4_other_version(version))
        for version in OTHER_REPLICAS
        if any(part in state.name for part in ARGS.replica_states)]
    names = [name for name, _, _ in CONSUMERS + other_versions]
    if not any(selected(f"{state.name}/{name}") for name in names + ["state"]):
        return
    case = f"{state.name}/state"
    if ARGS.mode not in state.modes:
        emit({"case": case, "ok": True, "verdict": "skipped",
              "skipped": f"needs --mode {state.modes[0]}"})
        return
    started = time.monotonic()
    for server in (SOURCE, SCRATCH):
        if not alive(server):
            revive(server)
    try:
        state.produce(state)
        state.truth = capture_truth()
        read_storage(state)
        found = damage(state)
        state.layout = find_layout(state)
    except (harness.Failure, RuntimeError, subprocess.SubprocessError,
            OSError, KeyError, IndexError, ValueError) as failure:
        emit({"case": case, "ok": False, "verdict": "harness_error",
              "why": "the state could not be produced: " +
                     harness.describe_failure(failure, 700)})
        if state.pull is not None:
            state.pull.close()
        return
    line = {"case": case, "ok": True, "verdict": "produced",
            "binlogs": state.indexed[-4:], "binlog_count": len(state.indexed),
            "source_rows": len(state.truth["rows"]["t"] or {}),
            "source_gtid_executed": rw.short(state.truth["executed"], 100),
            "damage": found, "made": state.made,
            "seconds": round(time.monotonic() - started, 1)}
    if state.layout:
        line["positions"] = {key: value for key, value in
                             state.layout.items()
                             if isinstance(value, (int, str))}
    if not found and not state.control:
        line.update(verdict="skipped",
                    skipped="the storage shows no damage: the state was "
                            "not produced (ticket fixed, or the attack was "
                            "refused)")
        emit(line)
        if state.pull is not None:
            state.pull.close()
        return
    emit(line)
    consumers = list(CONSUMERS)
    position = [name for name, _, _ in consumers].index("C3_pull")
    consumers[position:position] = other_versions
    try:
        for name, applies, consumer in consumers:
            if not selected(f"{state.name}/{name}") or not applies(state):
                continue
            emit(run_consumer(state, name, consumer))
    finally:
        if state.pull is not None:
            state.pull.close()
            state.pull = None


def spaced(original):
    """The source's sql() with a second's pause before every other
    statement that writes rows, so that the transactions of a state do not
    all carry the same second (--stop-datetime, search_by_timestamp)."""
    calls = [0]

    def sql(statements: str, *arguments, **named) -> str:
        if statements.lstrip().upper().startswith(("INSERT", "UPDATE",
                                                   "BEGIN")):
            calls[0] += 1
            if calls[0] % 2 == 0:
                time.sleep(1.05)
        return original(statements, *arguments, **named)
    return sql


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--mysql-basedir", type=Path, required=True)
    parser.add_argument("--binsrv", type=Path, required=True)
    parser.add_argument("--workdir", type=Path, required=True)
    parser.add_argument("--port", type=int, default=24100)
    parser.add_argument("--mode", choices=["position", "gtid"],
                        default="position")
    parser.add_argument("--cases", nargs="*", default=[],
                        help="substrings of '<state>/<consumer>'")
    parser.add_argument("--mysqld-deaths", type=int, default=1,
                        help="after so many replicas of a state have died, "
                             "its other replicas are skipped")
    parser.add_argument("--replica-basedirs", nargs="*", type=Path,
                        default=[],
                        help="MySQL distributions of other versions: an "
                             "empty replica of each for the states of "
                             "--replica-states")
    parser.add_argument("--replica-states", nargs="*",
                        default=["S0_control", "S1_", "S3_",
                                 "S2_gtid_swapped"],
                        help="substrings of the state names")
    args = parser.parse_args()
    args.mysql_basedir = args.mysql_basedir.expanduser()
    args.workdir = args.workdir.expanduser().resolve()
    args.workdir.mkdir(parents=True, exist_ok=True)
    # pylint: disable=global-statement
    global ARGS, GTID, SOURCE, SCRATCH, WORK
    global PULL_PORT, PROXY_PORT, DEAD_PORT
    ARGS, WORK, GTID = args, args.workdir, args.mode == "gtid"
    PULL_PORT, PROXY_PORT, DEAD_PORT = \
        args.port + 2, args.port + 3, args.port + 4
    SOURCE = harness.MySQLServer(args.mysql_basedir, WORK / "source",
                                 args.port, GTID, server_id=1)
    SCRATCH = harness.MySQLServer(args.mysql_basedir, WORK / "scratch",
                                  args.port + 1, GTID, server_id=2)
    for index, basedir in enumerate(args.replica_basedirs):
        basedir = basedir.expanduser()
        version = re.search(r"\d+\.\d+(\.\d+)?", basedir.name)
        label = version.group(0) if version else f"other{index}"
        OTHER_REPLICAS[label] = harness.MySQLServer(
            basedir, WORK / f"replica{index}", args.port + 5 + index, GTID,
            server_id=10 + index)
    harness.SERVER = SOURCE
    harness.BINSRV = harness.BinlogServer(args.binsrv.expanduser(),
                                          WORK / "binsrv", args.port, GTID)
    SOURCE.sql = spaced(SOURCE.sql)
    hostile.WORKLOAD = HOSTILE_WORKLOAD
    # the producing tests judge the storage with a replica of their own;
    # here the consumers do
    rw.REPLICA, rw.MYSQL_BASEDIR = SCRATCH, args.mysql_basedir
    rw.PULL_PORT, rw.PROXY_PORT = PULL_PORT, PROXY_PORT
    rw.judge = lambda *_arguments, **_named: {
        "facts": {"binlogs": len(harness.BINSRV.indexed_binlogs())},
        "problems": [], "missing": "", "plain": {}}
    rw.resuming_replica = lambda *_arguments: []

    results = []
    started = time.monotonic()

    def emit(result: dict) -> None:
        print(json.dumps(result), flush=True)
        results.append(result)

    # a server that died of what it was given may die again when it starts
    shutil.rmtree(SCRATCH.datadir, ignore_errors=True)
    SOURCE.start()
    try:
        SCRATCH.start()
        for state in STATES:
            try:
                run_state(state, emit)
            except (harness.Failure, RuntimeError,
                    subprocess.SubprocessError, OSError) as failure:
                emit({"case": f"{state.name}/state", "ok": False,
                      "verdict": "harness_error",
                      "why": harness.describe_failure(failure, 700)})
    finally:
        for server in [SCRATCH, SOURCE] + list(OTHER_REPLICAS.values()):
            try:
                server.stop()
            except OSError:
                pass
    matrix = {}
    for result in results:
        state_name, _, consumer = result["case"].partition("/")
        matrix.setdefault(state_name, {})[consumer] = result["verdict"]
    print(json.dumps({"matrix": matrix}), flush=True)
    failures = [result for result in results if not result.get("ok")]
    print(json.dumps({"cases": len(results), "failures": len(failures),
                      "failed": [result["case"] for result in failures],
                      "seconds": round(time.monotonic() - started),
                      "passed": not failures}), flush=True)
    (WORK / "result.json").write_text(json.dumps(results, indent=2))
    (WORK / "matrix.json").write_text(json.dumps(matrix, indent=2))
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
