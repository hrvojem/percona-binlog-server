# source-rewind-never-silently-continued

## What led to it

Evaluation gap G1 (wildcard lens W4). The catalog's source-identity scenarios are a swap to an unrelated server or a `RESET` (`source-swap-never-silently-accepted`) and a failover to a replica with a different `server_uuid` (`gtid-failover-*`). The missing case is the one most common in Percona deployments: the source is **restored from its own earlier physical backup** (xtrabackup or a datadir snapshot). The restored server keeps its `server_uuid`, because `auto.cnf` lives in the datadir, and PBS is still pointed at it. GNOs after the backup point are then reused for transactions with different content.

This is a separate property and not an extension of `source-swap-never-silently-accepted`, for three reasons:
- **Different trigger.** A datadir restore under the mysqld restart-loop wrapper needs no proxy/VIP container and no second mysqld, so it can run in `fetch_fs`/`pull_fs` with a wrapper extension rather than the deferred `failover` template.
- **Different precondition.** The swap oracle keys on an identity epoch. A rewind does not change identity, so the epoch has to come from a workload restore marker plus a GTID-set comparison ("was PBS ahead of the restore point?").
- **Different temporal shape.** The server first refuses (ER_REPLICA_HAS_MORE_GTIDS_THAN_SOURCE) and then accepts once the rewound source has committed enough new transactions. The dangerous window opens late, after a period of correct fail-closed behaviour.

A fix for ticket 25 based on `server_uuid` would pass the swap property and still miss this one, so it needs its own guard.

## What PBS checks on resume (verified in this repo)

- No source identity is recorded. `storage_metadata` holds only `version`, `mode` and `encryption` (`src/binsrv/storage_metadata.hpp:30-37`). `grep -rn server_uuid src` finds nothing.
- GTID mode resume sends `storage_->get_gtids()` (the tail's `previous_gtids ∪ added_gtids`) as COM_BINLOG_DUMP_GTID (`src/operations/collector_context.cpp:227-251`). The only SQL issued is `SELECT @@GLOBAL.gtid_purged`, and only on an empty storage (`:229-240`).
- Position mode resume sends the tail name and size (`collector_context.cpp:253-261`).
- On the first artificial ROTATE, `process_artificial_rotate_event` checks name equality with the open binlog, plus the position in position mode only (`collector_context.cpp:493-572`). A different name closes the open binlog and opens or creates the named one. `storage_core::open_binlog` refuses to append to an existing non-tail record (`src/binsrv/storage_core.cpp:270-279`, "cannot open an existing binlog that is not the latest one for append").
- `reader_context::validate_position` requires `position + event_size == next_event_position` for each event (`src/binsrv/events/reader_context.cpp:905-920`). Heartbeats that report skipped GTID groups are dropped client-side, because PBS always sets `MYSQL_RPL_SKIP_HEARTBEAT` (`src/easymysql/connection.cpp:205`; mechanism traced in `properties/gtid-failover-resume-progresses.md`).
- Rewrite mode ignores source ROTATE/FDE/PREVIOUS_GTIDS/STOP and re-stamps positions (`collector_context.cpp:274-330`; the reader's position check is tautological there, `:422-428`). It has no name or position continuity check against the source.

## What the server checks (MySQL source, PXC tree `sql/rpl_binlog_sender.cc`)

- `:918-921`: the dump is refused with `ER_REPLICA_HAS_MORE_GTIDS_THAN_SOURCE` only when the requested set holds more GTIDs of **the server's own UUID** than it has executed or owned. The comparison is by GNO count/interval, never by content.
- `:946-951`: refused when `gtid_purged ⊄ requested`.
- `find_first_log_not_in_gtid_set` picks the newest binlog whose PREVIOUS_GTIDS ⊆ the request, and skips groups already in the request.

## Failure scenario

Setup: PBS has stored `U:1-100`. The restored datadir snapshot was taken at `U:1-80` (binlogs `binlog.000001…000008` in the snapshot). PBS's storage index runs to `binlog.000010`.

1. The source restarts from the snapshot as `U:1-80`. PBS's dump of `U:1-100` is refused with 1236/ER_REPLICA_HAS_MORE_GTIDS_THAN_SOURCE. This arrives at the first fetch, so fetch exits 1 and pull exits and the supervisor restarts it. This phase is correct fail-closed behaviour.
2. The workload commits 20+ transactions on the rewound source. They get `U:81-100` with **different content**. The subset check now passes.
3. What happens next depends on the mode:
   - **Rewrite mode:** the source streams from its chosen file, PBS ignores names and positions, and stores `U:101…`. **Silent continuation is predicted.** The storage holds `U:81-100` with content no server has, and pull serves them downstream.
   - **GTID mode, non-rewrite:** the restored source's numbering restarts from the snapshot (`000009`, `000010`, …), so it reuses names already in PBS's index:
     - a name that is a stored non-tail record → `open_binlog` refuses, which is a wedge (fail-closed);
     - a name equal to the tail → `validate_position` against the stored size, which fails unless the offsets coincide;
     - a new name whose file skips any group → the dropped heartbeat makes `log_pos` jump, and `validate_position` fails.

     **Silent continuation** happens only when the source's numbering has passed PBS's tail **and** the chosen file's PREVIOUS_GTIDS is exactly `U:1-100` (a rotation right after GNO 100). That is narrow, but reachable with a 64 KiB `max_binlog_size` and the workload's `FLUSH BINARY LOGS`.
   - **Position mode:** PBS requests (tail name, size X). The rewound source rewrites files with the same names. If X falls past the end of the source's same-named file, the server answers "position > file size" (1236), and the outcome is fail-closed forever. If X is mid-event, the server fails with a 1236 read error or PBS rejects a misframed event. If X lands on an event boundary of the source's new file, the server streams from X and PBS appends. **The result is a file neither server wrote**, the same mechanism as the swap, but with no identity change at all.

Benign case that must NOT be flagged: PBS's stored GTID set ⊆ the restored source's `gtid_executed` at restore time (PBS was behind the backup point). Continuing is then correct, because nothing PBS holds was rewritten.

## Property and oracle

The workload marks a rewind epoch when it restores a snapshot. At that moment it records:
- `ahead = stored_gtids − restored_source.gtid_executed`, computed from `list`/`search_by_gtid_set` output and SQL. The source always runs `gtid_mode=ON`, so this works in position mode too.
- The stored tail (name, size) and a hash of the stored index and objects.

Assertion while `ahead ≠ ∅` and the storage has not been re-seeded: `Always("no PBS append after a source rewind behind the stored GTIDs")`. "Append" means any stored object changed beyond the restore-time snapshot. Recovery truncation of an untruncated tail beyond `metadata.size` is tolerated, as in the swap oracle. The check runs at every fetch exit and at each periodic pull check, and the mode goes in the details.

The diagnostic attached on violation is the per-GTID payload hash of the stored `U:81-100` against the source's `U:81-100`. It shows what the operator would replay.

Vacuity and exploration markers:
- `Sometimes("rewound source refused PBS's dump with ER_REPLICA_HAS_MORE_GTIDS_THAN_SOURCE")` (log line);
- `Sometimes("rewound source committed past the stored GTID count while the storage still held reused GNOs")`. The dangerous window must actually open, or a green result means nothing.

`Always` fits because a single silent append permanently mixes two histories.

## Key observations

- PBS has a cheap signal it could check and does not: the stored tail's last transaction. On a GTID resume, the server's chosen file and the received PREVIOUS_GTIDS say nothing about content. A position-mode resume could compare the source's bytes just before X against the stored tail. No such check exists.
- MySQL itself has the same blind spot for real replicas. A replica ahead of a rewound source also gets the "more GTIDs" error and then silently continues once the GNOs are reused. So "PBS behaves like a replica" is not a defence, but it may be the maintainers' contract. That is an open question.
- A rewind run invalidates the exactly-once oracles (`stored-transactions-exactly-once-across-resumes`, `stored-binlog-byte-prefix-of-source`) for the rest of the timeline, because the source history itself changed. The rewind action must be flagged and excluded in the same way as swap/RESET runs (catalog Assumptions).
- The wedge outcomes (name collision, position past EOF) are fail-closed, but they cut exploration short (coverage-balance F-wedges). The driver should re-seed the storage after its check.

## SUT-side instrumentation suggestions (all missing; see existing-assertions.md)

- `collector_context.cpp` in `process_artificial_rotate_event`, GTID mode: `REACHABLE("GTID resume: source's first file is not the stored tail and is new to the storage")`, an exploration anchor for the silent branch.
- `collector_context.cpp:227-251`, where the GTID set is sent: no in-process check is possible without recorded lineage, so the oracle stays workload-side.
- The `storage_core.cpp:276` and `reader_context.cpp:917` refusal sites are already anchored by `recovery-paths-exercised` ("resume named an existing non-tail binlog"). Reference them; do not duplicate.

## Open questions

- **Is PBS expected to detect a rewound source, or is lineage the operator's job?** If PBS should detect it, the property is a safety bug in all three modes, and the fix needs a content or lineage check, because a `server_uuid` check is insufficient. If it is the operator's job, the property becomes a documented-limitation guard (a `Sometimes` marker only), and the README should say so. This is the same contract question as catalog Open Question 5, with a different answer space. `(needs human input)`

## Resolved during investigation (see Investigation Log)

- **xtrabackup restore vs datadir snapshot:** they are not equivalent. PXB copies only the binlog that is current at the backup point, truncated at the backup position, and rewrites the index to list only that file (`percona-xtrabackup` `backup_mysql.cc` `write_current_binlog_file`, `backup_copy.cc:2480-2513`). The restored server keeps the file number (it does not start a fresh `binlog.000001`) and rotates to the next number on startup. **Property change:** the rewind driver needs two variants, (i) datadir snapshot (all binlogs up to the snapshot) and (ii) xtrabackup-shaped (only the current binlog, truncated at the backup position, index rewritten to that single file). Predicted outcomes for (ii): position mode with PBS's tail older than the restored current file gives "Could not find first log file" (fail-closed forever); tail equal to it gives "position > file size" (fail-closed); tail newer than it behaves like the snapshot case once the source's numbering reaches the tail name. GTID mode: `gtid_purged` becomes the restored file's PREVIOUS_GTIDS, so the purged-subset check passes and the silent-continuation analysis is unchanged.
- **Rewrite-mode silent continuation:** confirmed from code. Nothing in rewrite mode compares the source's names, positions, PREVIOUS_GTIDS or GTIDs with the stored state; the only refusal on the rewrite path is the "events must have checksums" FDE check. Rewrite mode stays in the "silent continuation predicted" column (not run).

### Investigation Log

#### Does PBS check anything on resume that would notice a same-UUID rewind?

- Examined: `storage_metadata.hpp:30-37`, `collector_context.cpp:207-272, 274-330, 503-572`, `storage_core.cpp:260-290`, `reader_context.cpp:895-925`, `grep -rn server_uuid src`.
- Found: no identity, lineage or content check. GTID mode relies on the server's subset checks. Non-rewrite modes add only name/position continuity, which catches the rewind only through file-name and offset coincidences.
- Conclusion: resolved, no tag. The prediction per mode is in "Failure scenario".

#### Is PBS expected to detect a rewound source, or is lineage the operator's job?

- Examined: README (operation modes, `fetch`/`pull` sections, `grep -i "restor|server_uuid|lineage|re-creat"`), `storage_metadata.hpp:30-37`, ticket 25 text (`.tickets-snapshot.txt:1355-1395`).
- Found: the README states no lineage contract and does not mention source restores, resets or failovers. Ticket 25 (Open, QA-filed) proposes recording `server_uuid` and a lineage marker and failing on mismatch, but that is the reporter's expected behaviour, not a maintainer decision, and its proposal would not catch a same-UUID rewind anyway.
- Not found: any maintainer statement on whether PBS or the operator owns lineage.
- Conclusion: `(needs human input)`. The maintainers must say whether PBS should detect a rewind; the code has no mechanism either way.

#### Does a datadir snapshot under the restart wrapper reproduce an xtrabackup restore closely enough?

- Examined: `/Users/hrvoje/work/percona-xtrabackup` (9.7.1 tree) `storage/innobase/xtrabackup/src/backup_mysql.cc` `write_current_binlog_file` (copies `log_status.filename` up to `log_status.position` + encryption header, and writes only that file's line into the backup's `.index`), `backup_copy.cc:2480-2513` (copy-back moves that one binlog and rewrites the index to contain only it).
- Found: an xtrabackup restore leaves exactly one binlog (the one current at backup time, cut at the backup position), with its original number. Older binlogs are absent, so their GTIDs become `gtid_purged`. The earlier hypothesis "starts a fresh `binlog.000001`" is wrong; the numbering continues from the backed-up file. Name collisions and position-mode refusals therefore differ from the datadir-snapshot case (see "Resolved during investigation").
- Conclusion: resolved. Not equivalent; the driver models both variants. Property changed (driver scope), invariant unchanged.

#### Is the rewrite-mode silent continuation reachable in practice?

- Examined: `collector_context.cpp:130-170` (rewrite dispatch), `:274-428` (`rewrite_and_process_binlog_event`: skips source ROTATE/FDE/PREVIOUS_GTIDS/STOP; on reconnect `context.is_fresh()` reuses `storage_->get_current_binlog_name()` and generates its own artificial ROTATE/FDE/PREVIOUS_GTIDS from `storage_->get_gtids()`), `:503-550` (generated artificial ROTATE name always equals the open binlog, and the position check runs only in position mode, which rewrite never uses), `reader_context.cpp:721-746` (PREVIOUS_GTIDS content not compared with anything), `gtid_set.cpp:585-609` (`add` is an idempotent interval insert; no "GTID already stored" check), `storage.cpp:143-170`.
- Found: no rewrite-mode check could refuse the rewound source. The only rewrite-specific refusal is the FDE checksum requirement (`collector_context.cpp:312-322`), which a rewound source of the same configuration passes. The server skips the GNOs PBS already requested, so PBS appends `U:101…` after a stored `U:81-100` that no longer matches the source.
- Not found: a run; this is a code-level prediction.
- Conclusion: resolved, no tag. Rewrite mode is the mode where silent continuation is unconditional once the server accepts the dump.
