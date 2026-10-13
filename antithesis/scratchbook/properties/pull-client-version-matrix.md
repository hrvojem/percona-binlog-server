# pull-client-version-matrix — mysqlbinlog clients of every supported version complete a dump against pull

## What led here
- Pull's command loop accepts only an **exact-string allowlist** of queries (`network_service.cpp:487-550`): four spellings of the checksum SET, `SELECT VERSION()` (answers "9.7.0"), `select @@version_comment limit 1`, and test scaffolding. Any other **query** gets ER_PARSE_ERROR 1064 "Syntax error" (`:574-585`). Only an unsupported **command** gets ER_UNKNOWN_COM_ERROR "Unknown command" (`:607-617`). That includes `COM_BINLOG_DUMP_GTID`, which is parsed (`connection_context.cpp:446`) but not dispatched. The handshake advertises `"9.7.0-pbs"` regardless of the stored data (`connection_context.hpp:41`).
- Client preambles: the investigation (see log) found that mysqlbinlog 8.0.36, 9.6.0 and 9.7.1 send exactly the same two queries, `SELECT VERSION()` and `SET @master_binlog_checksum = 'NONE', @source_binlog_checksum = 'NONE'`, and both are in the allowlist. The earlier claim that "older releases send `@master_binlog_checksum` only" does not apply to any version in the CI matrix. Replicas send many more queries (ticket 20 confirms replicas fail entirely). CI/MTR uses the mysqlbinlog matching each MTR server version, so each one is exercised only against its own server version's storage.
- Cross-version combinations are untested: e.g. mysqlbinlog 8.0 reading a storage whose newer files carry 8.4/9.7 FDEs (43-entry tables, possibly GTID_TAGGED_LOG), or mysqlbinlog 9.7 reading an 8.0-only storage.

## Antithesis value
Low by itself (deterministic). Its use is as a *workload enabler*: if the Antithesis image ships one mysqlbinlog and the source is a different version, every pull property depends on this matrix working. Include it as a `SOMETIMES` per client version so a broken combination is visible instead of silently starving the pull properties.

## Oracle
- Workload: for each client binary available in the image, `SOMETIMES(dump session received ≥ 1 non-artificial event)`. `ALWAYS(no "Syntax error" / "Unknown command" error returned to a mysqlbinlog client in BINLOG-DUMP-NON-GTIDS mode)`, taken from the client's stderr. mysqlbinlog's `--read-from-remote-source=BINLOG-DUMP-GTIDS` sends `COM_BINLOG_DUMP_GTID`, which is unsupported today and always gets "Unknown command". Keep it out of this `ALWAYS`, or give it a separate `Sometimes` once GTID dump is implemented.
- Matrix scope: only combinations where the client version ≥ the newest server version that produced the stored data are expected to work. That is MySQL's own rule: an older client cannot decode newer event types such as `GTID_TAGGED_LOG` (8.3+). PBS does not cause those failures, and they should not count against this property.

## SUT-side instrumentation (missing)
- `REACHABLE("unknown query from client")` at the syntax-error branch `network_service.cpp:574-585` (not the unknown-command branch at `:607-617`), with the query text in details. Optionally a second `REACHABLE("unsupported client command")` at `:607-617` with the command byte. Triage then shows exactly which client preamble is unsupported.

## Open questions

- Which mysqlbinlog versions does the product claim to support as pull clients, and are cross-version combinations (client ≠ source version that produced the storage) in scope? `(needs human input)`

### Investigation Log

#### Which mysqlbinlog versions does the product claim to support as pull clients?

- Examined: README (intro; `replication_source` section; no client-version list), `.github/workflows/cmake.yml:18-20, 292-336` (CI pins MySQL 8.0.45, 8.4.8, 9.7.2), `mtr/binlog_streaming/include/v80_v84_compatibility_defines.inc` (dies with "unsupported MySQL Server version" outside 8.0/8.4/9.7), deployment-topology.md (image ships the mysqlbinlog matching `MYSQL_VERSION`).
- Found: the de facto tested matrix is mysqlbinlog 8.0.45 / 8.4.8 / 9.7.2, each against storage produced by the same server version. The handshake advertises `9.7.0-pbs` (`connection_context.hpp:41`) and `SELECT VERSION()` returns `9.7.0` whatever the stored data is. mysqlbinlog accepts any version starting with 5, 8 or 9 (`check_master_version`, 8.0.36 `client/mysqlbinlog.cc:2554-2566`).
- Not found: any statement of a supported client range or of cross-version support.
- Conclusion: `(needs human input)`. Until it is answered, the workload should run same-version combinations as the property, and record cross-version ones as observations (`Sometimes` only, never `Always`).

#### Exact preamble strings per mysqlbinlog version

- Examined: `client/mysqlbinlog.cc` in three local MySQL-derived trees: PXC 8.0.36 (`worktable_old/percona-xtradb-cluster`, `check_master_version` at `:2519-2575`, `safe_connect` at `:2345-2400`), PXC 9.6.0 (`work/percona-xtradb-cluster`, `:2531-2558`) and the MySQL 9.7.1 tree in `work/percona-xtrabackup` (`:2525-2552`). Also every `mysql_query` call in those files, and libmysqlclient `sql-common/client.cc` for queries sent by the library (`check_license`, compiled only under `CHECK_LICENSE`, i.e. commercial builds).
- Found: all three versions send exactly `SELECT VERSION()` and then `SET @master_binlog_checksum = 'NONE', @source_binlog_checksum = 'NONE'`, then `COM_BINLOG_DUMP` (or `COM_BINLOG_DUMP_GTID` with `--read-from-remote-source=BINLOG-DUMP-GTIDS`) via `mysql_binlog_open`. Both queries match allowlist entries byte for byte (`network_service.cpp:530-541, 542-544`). Connection attributes (`program_name`, `_client_role`) travel in the handshake, not as queries. No 8.4.x source tree was available locally. 8.4 lies between two versions with identical code, and CI's 8.4.8 MTR `rs_*` tests exercise it.
- Conclusion: resolved. In non-GTID dump mode the preamble works for every CI version, so a failing matrix cell would point at stream content (FDE or event types), not at the query allowlist. The GTID dump mode is unsupported regardless of version.

## Evaluation refinements (2026-10-13)

Applied by the evaluation synthesis (`../evaluation/synthesis.md`). Where this section and earlier text in this file disagree, this section wins; the catalog entry has been updated to match.

- The image ships only the `mysqlbinlog` matching `MYSQL_VERSION`, so each launch can satisfy only one V. The per-V `Sometimes` is emitted only for the shipped client; the cross-version anchors are Deferred until the image ships all three clients (`client-matrix`) (antithesis-fit F4, implementability P7).
- Whether this property stays standalone or becomes a setup check is part of bias B4.
