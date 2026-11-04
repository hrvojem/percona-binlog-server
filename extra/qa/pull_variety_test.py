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

"""A MySQL replica of 'binlog_server pull' ends with the source's data.

event_variety_test.py checks that 'fetch' stores less common events byte
for byte; the other pull harnesses replicate plain inserts. Here a source
writes a broad workload, one database per kind, while 'pull' streams it
and a MySQL replica applies it from 'pull'. Kinds:

  types        every column type (integers at their limits, DECIMAL(65,30),
               FLOAT / DOUBLE, BIT(64), temporal types with microseconds,
               utf8mb4 text with 4-byte characters, binary strings, a 1 MB
               blob, ENUM / SET, JSON, GEOMETRY, virtual and stored
               generated columns), NULLs, then UPDATE and DELETE;
  row_images   binlog_row_image MINIMAL and NOBLOB, on a table with a
               primary key and on one without any key (duplicate rows,
               UPDATE / DELETE ... LIMIT);
  partial_json binlog_row_value_options=PARTIAL_JSON: JSON_SET,
               JSON_REPLACE and JSON_REMOVE on large documents;
  compression  binlog_transaction_compression=ON, large transactions;
  statement    binlog_format STATEMENT and MIXED (MIXED logs UUID() as
               rows);
  ddl          CREATE / ALTER (column, index) / RENAME / TRUNCATE / DROP
               TABLE, CREATE TABLE ... SELECT, a view, a function, a
               procedure and a trigger, all used;
  xa           XA transactions: committed, rolled back after PREPARE, and
               COMMIT ONE PHASE;
  large        a transaction of 20000 rows and an 8 MB row (below the 16 MB
               event limit of ticket 10);
  savepoints   SAVEPOINT / ROLLBACK TO, a rolled-back transaction, and a
               non-transactional MyISAM table;
  charsets     latin1, utf8mb4 and binary columns with their collations;
  tagged       (GTID mode, 8.3+) tagged GTIDs ('AUTOMATIC:<tag>');
  empty        (GTID mode) empty transactions given GTIDs of another UUID.

The storage is a file storage by default; --encrypted encrypts it (KEK
'alpha' of the repository's sample keyring, AES-256-CTR) and --storage s3 puts it
on an S3-compatible server (--s3-bucket-dir), so the replicas are served
through those read paths.

Two phases: 'live' (the replica is connected while the workload runs) and
'replay' (a reset replica replicates everything again from the storage).
After each the replica's databases, tables (SHOW CREATE TABLE, row count
and CHECKSUM TABLE), views, routines and triggers must equal the source's,
its threads must report no error and, in GTID mode, its GTID_EXECUTED must
equal the source's. A kind whose statements this MySQL version refuses is
reported as skipped. One JSON line per phase, then one per kind with the
kind's own differences.

Uses the helpers of fetch_crash_test.py and pull_replica_test.py. Example:

  PYTHONPATH=~/ws/qa/pylib python3 pull_variety_test.py \\
    --mysql-basedir ~/ws/dist/mysql-8.4.8-linux-glibc2.28-x86_64-minimal \\
    --binsrv ~/ws/percona-binlog-server-build-debug_gcc14/binlog_server \\
    --workdir ~/ws/qa/pull-variety --mode gtid
"""

import argparse
import json
import os
import re
import shutil
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import fetch_crash_test as harness  # noqa: E402
import pull_replica_test as replication  # noqa: E402

CATCH_UP_SECONDS = 300
OTHER_UUID = "22222222-3333-4444-8555-666666666666"
TAG = "qa"
# errors that mean this MySQL version lacks a feature (unknown system
# variable, not supported yet, a syntax it does not know): the kind is
# skipped; any other error fails it
UNSUPPORTED = re.compile(r"ERROR (1193|1235|1064) ")
PREFIX = "qv_"
# the 'alpha' record of the repository's sample keyring: its record 'beta'
# holds 31 bytes for AES-256-GCM, so every build rejects the whole file
# (ticket 52)
KEYRING_SOURCE = Path(__file__).resolve().parents[2] / "keyring_data.json"
KEYRING: Path = None  # written into the work directory by main()
ENCRYPTION = {"format": "generic", "kek_id": "alpha", "cipher": "AES-256-CTR"}


def types() -> list:
    columns = (
        "id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, "
        "c_tiny TINYINT, c_utiny TINYINT UNSIGNED, c_small SMALLINT, "
        "c_med MEDIUMINT, c_int INT, c_big BIGINT, c_ubig BIGINT UNSIGNED, "
        "c_dec DECIMAL(65,30), c_float FLOAT, c_double DOUBLE, c_bit BIT(64), "
        "c_date DATE, c_time TIME(6), c_dt DATETIME(6), "
        "c_ts TIMESTAMP(6) NULL, c_year YEAR, c_char CHAR(20), "
        "c_varchar VARCHAR(2000), c_bin BINARY(16), c_varbin VARBINARY(300), "
        "c_text MEDIUMTEXT, c_blob LONGBLOB, c_enum ENUM('a','b','c'), "
        "c_set SET('x','y','z'), c_json JSON, c_geo GEOMETRY, "
        "c_gen_v BIGINT AS (c_int * 2) VIRTUAL, "
        "c_gen_s VARCHAR(40) AS (CONCAT(c_char, '!')) STORED")
    names = ("c_tiny, c_utiny, c_small, c_med, c_int, c_big, c_ubig, c_dec, "
             "c_float, c_double, c_bit, c_date, c_time, c_dt, c_ts, c_year, "
             "c_char, c_varchar, c_bin, c_varbin, c_text, c_blob, c_enum, "
             "c_set, c_json, c_geo")
    rows = [
        # the lower limits
        "-128, 0, -32768, -8388608, -2147483648, -9223372036854775808, 0, "
        "-99999999999999999999999999999999999.999999999999999999999999999999,"
        " -3.40282e38, -1.7976931348623157e308, b'0', '1000-01-01', "
        "'-838:59:59.000000', '1000-01-01 00:00:00.000000', "
        "'1970-01-01 00:00:01.000000', 1901, '', '', "
        "UNHEX('00000000000000000000000000000000'), '', '', '', 'a', '', "
        "'[]', ST_GeomFromText('POINT(0 0)')",
        # the upper limits
        "127, 255, 32767, 8388607, 2147483647, 9223372036854775807, "
        "18446744073709551615, "
        "99999999999999999999999999999999999.999999999999999999999999999999, "
        "3.40282e38, 1.7976931348623157e308, b'1111111111111111111111111111"
        "111111111111111111111111111111111111', '9999-12-31', "
        "'838:59:59.000000', '9999-12-31 23:59:59.999999', "
        "'2038-01-19 03:14:07.999999', 2155, REPEAT('z', 20), "
        "REPEAT('é', 2000), UNHEX(REPEAT('FF', 16)), "
        "UNHEX(REPEAT('00FF', 150)), REPEAT('\U0001F600', 5000), "
        "REPEAT(UNHEX('00010203FEFF'), 175000), 'c', 'x,y,z', "
        "'{\"a\": [1, 2, {\"b\": null}], \"c\": \"\U0001F600\"}', "
        "ST_GeomFromText('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))')",
        # NULLs everywhere
        ", ".join(["NULL"] * 26),
        # ordinary values
        "1, 2, 3, 4, 5, 6, 7, 1.5, 0.25, 1e-300, b'101', '2026-11-04', "
        "'12:34:56.789012', '2026-11-04 12:34:56.123456', "
        "'2026-11-04 12:34:56.654321', 2026, 'chars', "
        "'žšćčđ 中文 \U0001F680', "
        "UNHEX('0102030405060708090A0B0C0D0E0F10'), UNHEX('DEADBEEF'), "
        "'text', UNHEX('CAFEBABE'), 'b', 'x,z', "
        "'{\"k\": 1.25e10, \"s\": \"x\"}', "
        "ST_GeomFromText('LINESTRING(0 0, 1 1, 2 0)')",
    ]
    statements = [f"CREATE DATABASE {PREFIX}types;",
                  f"CREATE TABLE {PREFIX}types.t ({columns}) ENGINE=InnoDB;"]
    for row in rows:
        statements.append(
            f"INSERT INTO {PREFIX}types.t ({names}) VALUES ({row});")
    statements += [
        f"UPDATE {PREFIX}types.t SET c_int = c_int DIV 2, c_char = 'upd', "
        f"c_json = JSON_SET(COALESCE(c_json, '{{}}'), '$.u', 1) "
        f"WHERE id IN (1, 4);",
        f"UPDATE {PREFIX}types.t SET c_blob = NULL, c_text = 'short' "
        f"WHERE id = 2;",
        f"DELETE FROM {PREFIX}types.t WHERE id = 3;",
    ]
    return statements


def row_images() -> list:
    db = PREFIX + "row_images"
    statements = [
        f"CREATE DATABASE {db};",
        f"CREATE TABLE {db}.pk (id INT PRIMARY KEY, a INT, b LONGBLOB, "
        f"c VARCHAR(100)) ENGINE=InnoDB;",
        f"CREATE TABLE {db}.nokey (a INT, b VARCHAR(100), c LONGBLOB) "
        f"ENGINE=InnoDB;",
    ]
    for image in ("MINIMAL", "NOBLOB", "FULL"):
        base = {"MINIMAL": 0, "NOBLOB": 100, "FULL": 200}[image]
        statements.append(f"SET SESSION binlog_row_image = {image};")
        for index in range(base, base + 20):
            statements.append(
                f"INSERT INTO {db}.pk VALUES ({index}, {index}, "
                f"REPEAT('b', {index * 100}), 'c{index}');")
            # duplicate rows in the keyless table
            statements.append(
                f"INSERT INTO {db}.nokey VALUES ({index % 5}, 'dup', "
                f"REPEAT('x', {index}));")
        statements += [
            f"UPDATE {db}.pk SET a = a + 1000 WHERE id BETWEEN {base} AND "
            f"{base + 9};",
            f"UPDATE {db}.pk SET b = REPEAT('n', 50) WHERE id = {base + 3};",
            f"DELETE FROM {db}.pk WHERE id = {base + 5};",
            f"UPDATE {db}.nokey SET b = 'changed' WHERE a = 1 LIMIT 2;",
            f"DELETE FROM {db}.nokey WHERE a = 2 LIMIT 1;",
        ]
    statements.append("SET SESSION binlog_row_image = FULL;")
    return statements


def partial_json() -> list:
    db = PREFIX + "partial_json"
    document = ("JSON_OBJECT('id', {i}, 'name', REPEAT('n', 2000), "
                "'list', JSON_ARRAY(1, 2, 3, REPEAT('l', 500)), "
                "'nested', JSON_OBJECT('a', JSON_OBJECT('b', {i})))")
    statements = [
        f"CREATE DATABASE {db};",
        f"CREATE TABLE {db}.j (id INT PRIMARY KEY, doc JSON) ENGINE=InnoDB;",
        "SET SESSION binlog_row_value_options = PARTIAL_JSON;",
    ]
    for index in range(30):
        statements.append(f"INSERT INTO {db}.j VALUES ({index}, "
                          f"{document.format(i=index)});")
    statements += [
        f"UPDATE {db}.j SET doc = JSON_SET(doc, '$.nested.a.b', 'set', "
        f"'$.new', 7) WHERE id < 10;",
        f"UPDATE {db}.j SET doc = JSON_REPLACE(doc, '$.name', 'short') "
        f"WHERE id BETWEEN 10 AND 19;",
        f"UPDATE {db}.j SET doc = JSON_REMOVE(doc, '$.list[1]') "
        f"WHERE id >= 20;",
        f"UPDATE {db}.j SET doc = JSON_SET(doc, '$.list[0]', 100), "
        f"doc = JSON_REMOVE(doc, '$.nested') WHERE id % 7 = 0;",
        "SET SESSION binlog_row_value_options = '';",
    ]
    return statements


def compression() -> list:
    db = PREFIX + "compression"
    statements = [
        f"CREATE DATABASE {db};",
        f"CREATE TABLE {db}.c (id INT AUTO_INCREMENT PRIMARY KEY, "
        f"v VARCHAR(200), b MEDIUMBLOB) ENGINE=InnoDB;",
        "SET SESSION binlog_transaction_compression = ON;",
    ]
    for batch in range(10):
        values = ", ".join(f"('v{batch}-{index}', REPEAT('compress', 200))"
                           for index in range(100))
        statements.append(f"BEGIN; INSERT INTO {db}.c (v, b) VALUES "
                          f"{values}; UPDATE {db}.c SET v = CONCAT(v, '+') "
                          f"WHERE id % 3 = {batch % 3}; COMMIT;")
    statements += [f"DELETE FROM {db}.c WHERE id % 10 = 0;",
                   "SET SESSION binlog_transaction_compression = OFF;"]
    return statements


def statement() -> list:
    db = PREFIX + "statement"
    return [
        f"CREATE DATABASE {db};",
        f"CREATE TABLE {db}.s (id INT PRIMARY KEY, v VARCHAR(100), "
        f"u VARCHAR(36)) ENGINE=InnoDB;",
        "SET SESSION binlog_format = 'STATEMENT';",
        f"INSERT INTO {db}.s VALUES (1, 'stmt', NULL), (2, 'stmt', NULL);",
        f"UPDATE {db}.s SET v = CONCAT(v, '-updated') WHERE id = 1;",
        "SET SESSION binlog_format = 'MIXED';",
        # UUID() is unsafe for statement logging: MIXED logs it as rows
        f"INSERT INTO {db}.s VALUES (3, 'mixed', UUID());",
        f"INSERT INTO {db}.s SELECT id + 10, v, NULL FROM {db}.s "
        f"WHERE id < 3;",
        f"DELETE FROM {db}.s WHERE id = 2;",
        "SET SESSION binlog_format = 'ROW';",
    ]


def ddl() -> list:
    db = PREFIX + "ddl"
    return [
        f"CREATE DATABASE {db};",
        f"CREATE TABLE {db}.a (id INT PRIMARY KEY, v INT) ENGINE=InnoDB;",
        f"INSERT INTO {db}.a VALUES (1, 10), (2, 20), (3, 30);",
        f"ALTER TABLE {db}.a ADD COLUMN w VARCHAR(20) DEFAULT 'w', "
        f"ADD INDEX v_index (v);",
        f"INSERT INTO {db}.a VALUES (4, 40, 'four');",
        f"ALTER TABLE {db}.a MODIFY COLUMN v BIGINT, DROP INDEX v_index;",
        f"RENAME TABLE {db}.a TO {db}.b;",
        f"CREATE TABLE {db}.c (id INT PRIMARY KEY, v BIGINT) "
        f"SELECT id, v FROM {db}.b;",
        f"CREATE TABLE {db}.gone (id INT PRIMARY KEY) ENGINE=InnoDB;",
        f"INSERT INTO {db}.gone VALUES (1);",
        f"DROP TABLE {db}.gone;",
        f"CREATE TABLE {db}.emptied (id INT PRIMARY KEY) ENGINE=InnoDB;",
        f"INSERT INTO {db}.emptied VALUES (1), (2);",
        f"TRUNCATE TABLE {db}.emptied;",
        f"CREATE VIEW {db}.view_b AS SELECT id, v * 2 AS v2 FROM {db}.b;",
        f"CREATE FUNCTION {db}.twice(x BIGINT) RETURNS BIGINT DETERMINISTIC "
        f"RETURN x * 2;",
        f"CREATE TABLE {db}.audit (id INT AUTO_INCREMENT PRIMARY KEY, "
        f"note VARCHAR(100)) ENGINE=InnoDB;",
        f"CREATE TRIGGER {db}.b_insert AFTER INSERT ON {db}.b FOR EACH ROW "
        f"INSERT INTO {db}.audit (note) VALUES (CONCAT('inserted ', "
        f"NEW.id));",
        f"CREATE PROCEDURE {db}.add_row(new_id INT) INSERT INTO {db}.b "
        f"(id, v) VALUES (new_id, {db}.twice(new_id));",
        f"CALL {db}.add_row(5);",
        f"CALL {db}.add_row(6);",
        f"INSERT INTO {db}.b (id, v) VALUES (7, {db}.twice(7));",
    ]


def xa() -> list:
    db = PREFIX + "xa"
    return [
        f"CREATE DATABASE {db};",
        f"CREATE TABLE {db}.x (id INT PRIMARY KEY, v VARCHAR(20)) "
        f"ENGINE=InnoDB;",
        f"XA START 'qa1'; INSERT INTO {db}.x VALUES (1, 'committed'); "
        f"XA END 'qa1'; XA PREPARE 'qa1'; XA COMMIT 'qa1';",
        f"XA START 'qa2'; INSERT INTO {db}.x VALUES (2, 'rolled back'); "
        f"XA END 'qa2'; XA PREPARE 'qa2'; XA ROLLBACK 'qa2';",
        f"XA START 'qa3'; INSERT INTO {db}.x VALUES (3, 'one phase'); "
        f"XA END 'qa3'; XA COMMIT 'qa3' ONE PHASE;",
        f"XA START 'qa4'; UPDATE {db}.x SET v = 'updated' WHERE id = 1; "
        f"DELETE FROM {db}.x WHERE id = 3; XA END 'qa4'; XA PREPARE 'qa4'; "
        f"XA COMMIT 'qa4';",
    ]


def large() -> list:
    db = PREFIX + "large"
    statements = [
        f"CREATE DATABASE {db};",
        f"CREATE TABLE {db}.many (id INT PRIMARY KEY, v VARCHAR(64)) "
        f"ENGINE=InnoDB;",
        f"CREATE TABLE {db}.big (id INT PRIMARY KEY, b LONGBLOB) "
        f"ENGINE=InnoDB;",
    ]
    # the rows are generated by the server (a command line holds about
    # 128 KB); MySQL 9.7 resolves the CTE name in INSERT ... WITH against
    # the default database, and its minimal build has no MD5()
    rows = ("WITH RECURSIVE seq (n) AS (SELECT {first} UNION ALL SELECT "
            "n + 1 FROM seq WHERE n < {last}) SELECT n, CONCAT('row-', n) "
            "FROM seq")
    statements.append(
        f"USE {db}; SET SESSION cte_max_recursion_depth = 20000; BEGIN; "
        f"INSERT INTO {db}.many {rows.format(first=0, last=9999)}; "
        f"INSERT INTO {db}.many {rows.format(first=10000, last=19999)}; "
        "COMMIT;")
    # 8 MB: one event well past the 64 KB binlog size, below 16 MB
    statements.append(f"INSERT INTO {db}.big VALUES (1, "
                      f"REPEAT(UNHEX('0123456789ABCDEF'), 1048576));")
    statements.append(f"UPDATE {db}.many SET v = 'updated' "
                      f"WHERE id % 1000 = 0;")
    return statements


def savepoints() -> list:
    db = PREFIX + "savepoints"
    return [
        f"CREATE DATABASE {db};",
        f"CREATE TABLE {db}.s (id INT PRIMARY KEY, v VARCHAR(20)) "
        f"ENGINE=InnoDB;",
        f"CREATE TABLE {db}.m (id INT PRIMARY KEY, v VARCHAR(20)) "
        f"ENGINE=MyISAM;",
        f"BEGIN; INSERT INTO {db}.s VALUES (1, 'kept'); SAVEPOINT one; "
        f"INSERT INTO {db}.s VALUES (2, 'undone'); ROLLBACK TO SAVEPOINT "
        f"one; INSERT INTO {db}.s VALUES (3, 'kept'); COMMIT;",
        f"BEGIN; INSERT INTO {db}.s VALUES (4, 'never'); ROLLBACK;",
        f"INSERT INTO {db}.m VALUES (1, 'myisam'), (2, 'myisam');",
        f"UPDATE {db}.m SET v = 'myisam-upd' WHERE id = 1;",
        f"DELETE FROM {db}.m WHERE id = 2;",
    ]


def charsets() -> list:
    db = PREFIX + "charsets"
    return [
        f"CREATE DATABASE {db} CHARACTER SET latin1;",
        f"CREATE TABLE {db}.c (id INT PRIMARY KEY, "
        f"l VARCHAR(50) CHARACTER SET latin1 COLLATE latin1_swedish_ci, "
        f"u VARCHAR(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_as_cs, "
        f"g VARCHAR(50) CHARACTER SET gbk, "
        f"b VARBINARY(50)) ENGINE=InnoDB;",
        f"INSERT INTO {db}.c VALUES (1, 'café åäö', "
        f"'\U0001F44D жить', '中文', "
        f"UNHEX('C3A9'));",
        f"INSERT INTO {db}.c VALUES (2, 'A', 'a', 'x', UNHEX('00'));",
        f"UPDATE {db}.c SET u = CONCAT(u, '\U0001F600') WHERE id = 1;",
    ]


def tagged() -> list:
    db = PREFIX + "tagged"
    return [
        f"CREATE DATABASE {db};",
        f"CREATE TABLE {db}.t (id INT PRIMARY KEY) ENGINE=InnoDB;",
        f"SET gtid_next = 'AUTOMATIC:{TAG}'; "
        f"INSERT INTO {db}.t VALUES (1); INSERT INTO {db}.t VALUES (2); "
        f"SET gtid_next = 'AUTOMATIC';",
        f"INSERT INTO {db}.t VALUES (3);",
    ]


def empty() -> list:
    return [f"SET gtid_next = '{OTHER_UUID}:{gno}'; BEGIN; COMMIT; "
            f"SET gtid_next = 'AUTOMATIC';" for gno in range(1, 6)]


KINDS = {"types": types, "row_images": row_images,
         "partial_json": partial_json, "compression": compression,
         "statement": statement, "ddl": ddl, "xa": xa, "large": large,
         "savepoints": savepoints, "charsets": charsets, "tagged": tagged,
         "empty": empty}
# the databases each kind compares ('empty' only adds GTIDs)
KIND_DATABASES = {name: [PREFIX + name] for name in KINDS}
KIND_DATABASES["empty"] = []


def query(server, text: str) -> list:
    return [line.split("\t") for line in server.sql(text).splitlines()
            if line]


def describe(server, database: str) -> dict:
    """What the comparison looks at in one database."""
    state = {}
    for name, kind in query(
            server, "SELECT TABLE_NAME, TABLE_TYPE FROM "
            "information_schema.TABLES WHERE TABLE_SCHEMA = "
            f"'{database}' ORDER BY TABLE_NAME"):
        if kind == "VIEW":
            definition = server.sql(
                "SELECT VIEW_DEFINITION FROM information_schema.VIEWS WHERE "
                f"TABLE_SCHEMA = '{database}' AND TABLE_NAME = '{name}'")
            state[f"view {name}"] = definition.strip()
            continue
        create = server.sql(f"SHOW CREATE TABLE {database}.{name}")
        create = re.sub(r" AUTO_INCREMENT=\d+", "", create.split("\t", 1)[1])
        count = server.sql(f"SELECT COUNT(*) FROM {database}.{name}").strip()
        checksum = server.sql(
            f"CHECKSUM TABLE {database}.{name}").split("\t")[-1].strip()
        state[f"table {name}"] = {"create": create.strip(), "rows": count,
                                  "checksum": checksum}
    for name, kind in query(
            server, "SELECT ROUTINE_NAME, ROUTINE_TYPE FROM "
            "information_schema.ROUTINES WHERE ROUTINE_SCHEMA = "
            f"'{database}' ORDER BY ROUTINE_NAME"):
        state[f"{kind.lower()} {name}"] = "present"
    for name, event in query(
            server, "SELECT TRIGGER_NAME, EVENT_MANIPULATION FROM "
            "information_schema.TRIGGERS WHERE TRIGGER_SCHEMA = "
            f"'{database}' ORDER BY TRIGGER_NAME"):
        state[f"trigger {name}"] = event
    return state


def databases(server) -> list:
    return [row[0] for row in query(
        server, "SELECT SCHEMA_NAME FROM information_schema.SCHEMATA WHERE "
        f"SCHEMA_NAME LIKE '{PREFIX}%' ORDER BY SCHEMA_NAME")]


def differences(source, replica, database: str) -> dict:
    """The keys of one database whose state differs (source, replica)."""
    try:
        expected = describe(source, database)
        got = describe(replica, database)
    except RuntimeError as error:
        return {"error": str(error).splitlines()[0][:300]}
    found = {}
    for key in sorted(set(expected) | set(got)):
        if expected.get(key) != got.get(key):
            found[key] = {"source": str(expected.get(key))[:300],
                          "replica": str(got.get(key))[:300]}
    return found


def caught_up(source, replica, gtid_mode: bool, marker: int) -> bool:
    """Waits until the replica holds the marker row (and, in GTID mode,
    the source's GTID_EXECUTED)."""
    source_executed = executed(source) if gtid_mode else ""
    deadline = time.monotonic() + CATCH_UP_SECONDS
    while time.monotonic() < deadline:
        try:
            has_marker = replica.sql(
                f"SELECT COUNT(*) FROM {PREFIX}marker.m WHERE id = "
                f"{marker}").strip() == "1"
        except RuntimeError:
            has_marker = False
        if has_marker and (not gtid_mode or same_set(
                replica, executed(replica), source_executed)):
            return True
        if thread_errors(replica):
            return False
        time.sleep(1)
    return False


def executed(server) -> str:
    return server.sql("SELECT @@GLOBAL.GTID_EXECUTED").strip().replace(
        "\\n", "").replace("\n", "")


def same_set(server, first: str, second: str) -> bool:
    return server.sql(
        f"SELECT GTID_SUBSET('{first}', '{second}') AND "
        f"GTID_SUBSET('{second}', '{first}')").strip() == "1"


def thread_errors(replica) -> list:
    errors = []
    for table in ("replication_connection_status",
                  "replication_applier_status_by_worker"):
        for line in replica.sql(
                "SELECT LAST_ERROR_NUMBER, LAST_ERROR_MESSAGE FROM "
                f"performance_schema.{table}").splitlines():
            number, _, message = line.partition("\t")
            if number not in ("", "0"):
                errors.append(f"{number}: {message[:300]}")
    return errors


def compare(source, replica, gtid_mode: bool, marker: int,
            kinds: dict) -> tuple:
    """(the phase's result, each kind's differences)."""
    reached = caught_up(source, replica, gtid_mode, marker)
    errors = thread_errors(replica)
    per_kind = {name: {database: found for database in
                       KIND_DATABASES[name]
                       if (found := differences(source, replica, database))}
                for name, status in kinds.items() if status == "written"}
    extra = sorted(set(databases(replica)) - set(databases(source)))
    phase = {"caught_up": reached, "errors": errors,
             "extra_databases": extra}
    if gtid_mode:
        phase["gtid_executed_equal"] = same_set(
            replica, executed(replica), executed(source))
    phase["ok"] = (reached and not errors and not extra and
                   phase.get("gtid_executed_equal", True) and
                   not any(per_kind.values()))
    return phase, per_kind


def run(args) -> list:
    work = args.workdir
    gtid_mode = args.mode == "gtid"
    source = harness.MySQLServer(args.mysql_basedir, work / "source",
                                 args.port, gtid_mode, server_id=1)
    replica = harness.MySQLServer(args.mysql_basedir, work / "replica",
                                  args.port + 10, gtid_mode, server_id=10)
    harness.SERVER = source
    s3 = None
    if args.storage == "s3":
        s3 = harness.S3Storage(args.s3_endpoint, args.s3_bucket,
                               args.s3_bucket_dir, args.s3_access_key,
                               args.s3_secret_key,
                               f"variety-{args.mode}-{os.getpid()}")
    binsrv = harness.BinlogServer(args.binsrv, work / "binsrv", args.port,
                                  gtid_mode, s3)
    if args.encrypted:
        # every configuration written (also start_pull's) is encrypted
        write_config = binsrv.write_config

        def write_encrypted_config():
            write_config()
            config = json.loads(binsrv.config_path.read_text())
            config["keyring"] = {"uri": f"file://{KEYRING}"}
            config["storage"]["encryption"] = dict(ENCRYPTION)
            binsrv.config_path.write_text(json.dumps(config, indent=2))

        binsrv.write_config = write_encrypted_config
    binsrv.reset()
    binsrv.checkpoint_size = "1"
    pull_port = args.port + 2
    start = ("SOURCE_AUTO_POSITION=1" if gtid_mode else
             "SOURCE_LOG_FILE='binlog.000001', SOURCE_LOG_POS=4")
    results = []
    pull = None
    source.start()
    replica.start()
    try:
        source.reset()
        for database in databases(source):
            source.sql(f"DROP DATABASE {database}")
        source.reset()
        replica.sql("STOP REPLICA; RESET REPLICA ALL;")
        for database in databases(replica):
            replica.sql(f"DROP DATABASE {database}")
        source.sql(f"CREATE DATABASE {PREFIX}marker; CREATE TABLE "
                   f"{PREFIX}marker.m (id INT PRIMARY KEY) ENGINE=InnoDB;")
        pull = replication.start_pull(binsrv, pull_port)
        replication.replicate_from_pull(replica, pull_port, gtid_mode, start)

        version = source.version()
        wanted = args.kinds.split(",") if args.kinds else list(KINDS)
        kinds = {}
        for name in wanted:
            if name in ("tagged", "empty") and not gtid_mode:
                kinds[name] = "skipped: GTID mode only"
                continue
            if name == "tagged" and version < (8, 3, 0):
                kinds[name] = "skipped: tagged GTIDs need MySQL 8.3"
                continue
            try:
                for statement in KINDS[name]():
                    source.sql(statement)
                kinds[name] = "written"
            except RuntimeError as error:
                message = str(error).splitlines()[0][:200]
                kinds[name] = ("skipped: " if UNSUPPORTED.search(message)
                               else "failed: ") + message
            source.sql("FLUSH BINARY LOGS")
        source.sql(f"INSERT INTO {PREFIX}marker.m VALUES (1)")

        live, live_kinds = compare(source, replica, gtid_mode, 1, kinds)
        results.append({"case": "live", **live})
        print(json.dumps(results[-1]), flush=True)

        # everything again, from the storage, into a reset replica
        drops = " ".join(f"DROP DATABASE IF EXISTS {database};"
                         for database in databases(replica))
        replication.replicate_from_pull(
            replica, pull_port, gtid_mode, start,
            f"SET SESSION SQL_LOG_BIN = 0; {drops} "
            "SET SESSION SQL_LOG_BIN = 1;")
        source.sql(f"INSERT INTO {PREFIX}marker.m VALUES (2)")
        replay, replay_kinds = compare(source, replica, gtid_mode, 2, kinds)
        replay["pull_alive"] = pull.poll() is None
        replay["ok"] = replay["ok"] and replay["pull_alive"]
        results.append({"case": "replay", **replay})
        print(json.dumps(results[-1]), flush=True)

        for name, status in kinds.items():
            result = {"case": name, "status": status}
            if status == "written":
                result["live"] = live_kinds[name]
                result["replay"] = replay_kinds[name]
                result["ok"] = not live_kinds[name] and \
                    not replay_kinds[name] and live["caught_up"] and \
                    replay["caught_up"]
            else:
                result["ok"] = status.startswith("skipped")
            results.append(result)
            print(json.dumps(result), flush=True)
    finally:
        if pull is not None and pull.poll() is None:
            pull.terminate()
            pull.wait(timeout=120)
        source.stop()
        replica.stop()
    if s3 is not None and all(result.get("ok") for result in results):
        # the objects live in a shared bucket; a failed run keeps them

        shutil.rmtree(binsrv.storage, ignore_errors=True)
    return results


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--mysql-basedir", type=Path, required=True)
    parser.add_argument("--binsrv", type=Path, required=True)
    parser.add_argument("--workdir", type=Path, required=True)
    parser.add_argument("--port", type=int, default=24010)
    parser.add_argument("--mode", choices=["position", "gtid"],
                        default="gtid")
    parser.add_argument("--encrypted", action="store_true",
                        help="encrypt the storage")
    parser.add_argument("--storage", choices=["file", "s3"], default="file")
    parser.add_argument("--s3-endpoint", default="127.0.0.1:9000")
    parser.add_argument("--s3-bucket", default="pbs-mtr")
    parser.add_argument("--s3-bucket-dir", type=Path,
                        help="the directory holding the bucket's objects")
    parser.add_argument("--s3-access-key", default="pbsaccesskey")
    parser.add_argument("--s3-secret-key", default="pbs/secret+key=1")
    parser.add_argument("--kinds", default="",
                        help="comma-separated kinds (default: all)")
    args = parser.parse_args()
    args.mysql_basedir = args.mysql_basedir.expanduser()
    args.binsrv = args.binsrv.expanduser()
    args.workdir = args.workdir.expanduser().resolve()
    global KEYRING
    KEYRING = args.workdir / "keyring.json"
    keyring = json.loads(KEYRING_SOURCE.read_text())
    keyring["keys"] = [key for key in keyring["keys"] if key["id"] == "alpha"]
    KEYRING.write_text(json.dumps(keyring, indent=2))
    if args.storage == "s3":
        if args.s3_bucket_dir is None:
            parser.error("--storage s3 needs --s3-bucket-dir")
        args.s3_bucket_dir = args.s3_bucket_dir.expanduser().resolve()
    args.workdir.mkdir(parents=True, exist_ok=True)
    results = run(args)
    (args.workdir / "results.json").write_text(json.dumps(results, indent=2))
    return 0 if all(result.get("ok") for result in results) else 1


if __name__ == "__main__":
    sys.exit(main())
