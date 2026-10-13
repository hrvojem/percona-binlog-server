# source-swap-never-silently-accepted

## What led to it

Focus 7 (distributed coordination) asks how binlog_server behaves when the "leader" it replicates from changes identity: a failover to another server, or a reset of the source's binlog lineage. The storage has no notion of who wrote it. Ticket 25 (open, High) reports the two outcomes; the QA repro is `extra/qa/source_change_test.py` (scenarios `reset` and `identity`, both failing on MySQL 8.4.8).

Validation of ticket 25 (competing explanation: "the harness changed something else, e.g. the config pointed at a new storage"): the discriminating detail is in code. `storage_metadata` holds only `version`, `mode`, `encryption` (`src/binsrv/storage_metadata.hpp:35-37`). Nothing in `src/` queries `@@server_uuid` (`grep -rn server_uuid src` finds nothing; the only SQL the collector issues is `SELECT @@GLOBAL.gtid_purged`, `collector_context.cpp:231`). Resume in position mode is just `(tail name, tail size)` (`collector_context.cpp:253-261`; `storage_core.hpp:313-337`). So there is no mechanism that could notice a changed source. The QA report's "exit 0 while bytes differ from offset 4" (identity) and "exit 1 forever" (reset) follow directly.

## Code paths

- `src/operations/collector_context.cpp:207-272` `open_connection_and_switch_to_replication`: position dump with stored name + size, GTID dump with stored GTID set; `log_connection_info` (`logger_helpers.cpp:253-265`) logs version / protocol / connection info only.
- `src/operations/collector_context.cpp:503-572` `process_artificial_rotate_event`: name equality + (position mode only) position check is the only continuity check against the source.
- `src/binsrv/events/reader_context.cpp:547-556` position-mode artificial ROTATE check; `:905-920` `validate_position` (next_event_position continuity).
- `src/operations/fetch_operation.cpp:107-115`: exit 0 on reaching EOF.

## Failure scenario

Position mode. Storage holds A's `binlog.000001` up to offset X. The workload repoints the source endpoint (a proxy/VIP container, or same hostname) to server B with its own `binlog.000001`. If B's file is exactly X bytes long or X happens to be an event boundary in B, B sends nothing (or valid events from X), the artificial ROTATE position matches X, and fetch exits 0. Stored `binlog.000001` = A's bytes [0,X) + possibly B's bytes [X,…): a file that neither server wrote. With RESET BINARY LOGS AND GTIDS the source's new `binlog.000001` is shorter than X → 1236-style error → exit 1 on every run (wedge).

GTID mode (non-rewrite) against an unrelated server: B's GTID set does not contain A's GTIDs; B's dump check only verifies GTIDs with B's own server_uuid, so the dump is accepted and B streams all of B's transactions; whether PBS then fails depends on name collisions (see `gtid-failover-resume-progresses`). Rewrite mode accepts any source silently by design.

## Property

After the workload swaps the source for a server with unrelated history (or resets the source's binlogs), no fetch run exits 0 and no pull run appends bytes, unless the storage was recreated. Workload-side `Always` evaluated at every fetch exit / periodic pull check, keyed on a workload-tracked "source epoch" variable.

## Key observations

- The FDE event header carries the server_id of the server that wrote the file; this is the only identity signal PBS already parses (reader_context FDE state). The server_id in transaction events is the originating server's, so it is not an identity signal on a promoted replica.
- Reset is the fail-closed half (wedge, liveness); identity is the silent half (safety). The property asserts only the safety half; the wedge is a recoverability expectation.

## SUT-side instrumentation suggestions (all missing; see existing-assertions.md)

- `collector_context.cpp` after connect: `SOMETIMES(source_identity_changed_since_storage_creation, "source identity differs from the storage's recorded source")` — needs a recorded identity first (does not exist), so realistically a workload-side check.
- `reader_context` FDE state: `REACHABLE("FDE server_id differs from the previous FDE server_id seen by this storage")` as an exploration anchor (missing).
- `fetch_operation.cpp:107-115`: `ALWAYS(storage_tail_matches_source_at_exit, ...)` is not computable in-process; keep this workload-side.

## Open questions

- Is a position-mode fetch against a swapped source expected to fail closed, or is source continuity the operator's responsibility? `(needs human input)`

## Answers and implications (this pass)

- **Harness:** settled by `deployment-topology.md`. The swap variant lives in the deferred `failover` template, with a `mysql-replica` (`log_replica_updates`) plus a VIP/proxy container that the workload repoints, six containers in total (topology line 221). The reset variant (`RESET BINARY LOGS AND GTIDS`) runs from `mysql-source`'s `parallel_driver_kill_mysqld.sh` only when `source-swap-*` is enabled (line 178). The proxy is a separate container, so Antithesis can partition the old and new sources independently of the PBS↔proxy link.
- **Error for the position case:** every refusal is `ER_SOURCE_FATAL_ERROR_READING_BINLOG` (1236). The messages are "position > file size" when X is past B's file end (`rpl_binlog_sender.cc:987-992`), or "bogus data in log event" / "binlog truncated in the middle of event" / "event read from binlog did not pass crc check" (`:1204-1219`) when X is mid-file. When X is mid-file but the bytes there happen to pass MySQL's length checks (the sender verifies CRCs only with `source_verify_checksum`), B can send misframed "events". PBS then rejects them in `event_view` (size, CRC with `verify_checksum`, unknown type) or in `validate_position`. Either way the run exits non-zero. 1236 is not 2013, so pull ends rather than reconnecting. Outcome for the property: when X is not an event boundary in B, the swap fails closed. The silent branch needs X to be an event boundary in B (or B's file to be exactly X bytes long), as the Failure scenario already says. **No property change.** The workload should log which branch occurred (exit code and message) so triage can tell "fail-closed by luck" from "silent accept".
- Also confirmed for the GTID-mode paragraph: MySQL's dump check `ER_REPLICA_HAS_MORE_GTIDS_THAN_SOURCE` compares only GTIDs of the source's own UUID (`is_subset_for_sid(..., server_tsid)`, `rpl_binlog_sender.cc:899-903` in 9.7.1; same in 8.0 and 8.4.8). The "B accepts A's GTID set" premise is verified, not assumed.

### Investigation Log

#### Is a position-mode fetch against a swapped source expected to fail closed?

- Examined: README (no "failover", "server_uuid", "promote" or "source change" text; grep), `storage_metadata.hpp:35-37`, ticket 25 (snapshot lines 1347-1399), `git log` for storage metadata (d27de1f, f62a95a).
- Found: ticket 25's "Expected behaviour" (record server_uuid and lineage, refuse on mismatch) is a QA draft, not maintainer-confirmed. The code has no identity mechanism at all. README gives no guidance on reusing a storage against a different source.
- Not found: any maintainer statement.
- Conclusion: `(needs human input)`.

#### How does the harness swap the source?

- Examined: `deployment-topology.md` lines 170-180, 215-225, 276.
- Found: VIP/proxy container plus `mysql-replica` in the deferred `failover` template; reset from `mysql-source`'s driver behind a flag. Topology still lists placement of the reset action as `[open]` (line 276), but that is a catalog-wide scheduling question, not one about this property.
- Conclusion: resolved.

#### Which error does B return when X is not an event boundary?

- Examined: MySQL `sql/rpl_binlog_sender.cc` (percona-xtrabackup f1aa0691f76, 9.7.1): `set_fatal_error` (`rpl_binlog_sender.h:434-436`), start-position checks (`.cc:970-993`), `log_read_error_msg` (`:1201-1220`); PBS `event_view.cpp:44-150`, `reader_context.cpp:905-920`.
- Found: all sender refusals are 1236 with the messages listed above. A misframed event that MySQL lets through is rejected by PBS's parser or position check.
- Not found: an execution showing which message is most common (not run).
- Conclusion: resolved. Fail-closed for every non-boundary X.

## Evaluation refinements (2026-10-13)

Applied by the evaluation synthesis (`../evaluation/synthesis.md`). Where this section and earlier text in this file disagree, this section wins; the catalog entry has been updated to match.

- **Priority P0 → P1.** Whether a swap fails closed or is silently accepted depends on whether the new server's file length or an event boundary lines up with the stored offset: a property of the data, not of the interleaving. Antithesis mostly re-confirms a known design gap with an existing QA reproduction (`source_change_test.py`). The independent-server swap needs the deferred `failover` template; the RESET variant fits `fetch_fs` only behind a flag, because it makes ticket 25 fire constantly (antithesis-fit F6, coverage-balance F2, implementability P7).
- Runs in: **Deferred** (`failover`); RESET variant in `fetch_fs` behind a flag. The RESET command differs by version (`RESET MASTER` on 8.0, `RESET BINARY LOGS AND GTIDS` on 8.2+).
- The same-UUID rewind (source restored from its own backup, reused GNOs with different content) is not covered here; it is gap G1 (wildcard W4).
