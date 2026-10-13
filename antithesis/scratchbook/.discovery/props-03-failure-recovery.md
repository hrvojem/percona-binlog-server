# Property discovery 03: Failure Recovery

Focus: crash recovery, restart correctness, partial-failure handling and retry semantics. All line numbers refer to commit 99de682. "Workload kill" means the workload SIGKILLs a PBS process it spawned itself, so it does **not** need Antithesis node termination. "Node termination" means the platform fault, which is disabled by default.

## Category A: Restart after interruption (storage-level recovery)

### kill-anywhere-storage-reopens — A PBS writer killed at any instant leaves a storage the next run can open

| | |
|---|---|
| **Type** | Safety |
| **Property** | After a fetch or pull process is killed at an arbitrary moment, the next streaming start on the same storage does not fail with a storage-validation error. |
| **Invariant** | Workload: `Always(restart_after_kill_opened_storage)`, evaluated on every restart that follows a kill. A non-zero exit whose log carries a validator message (`storage_core.cpp:196, 775, 780, 939, 970, 975, 994`) is a violation, while source-side errors are not. `Always` is used because a single wedge is a safety failure. SUT: one `UNREACHABLE` per validator raise site (gated on `construction_mode_ != querying_only`). Replace the assert at `storage_core.cpp:707` with `ALWAYS(open_stream_offset == flushed position)`. |
| **Antithesis Angle** | Kills land at random instants. The interesting window is rotation (`storage_core.cpp:680-703`: create/magic → `.json` → index), which is hit more often with frequent `FLUSH BINARY LOGS`, a small `max_binlog_size`, or rewrite mode with a tiny `file_size`. Antithesis explores the instruction-level timing that `fetch_crash_test.py`'s file-size polling only approximates. |
| **Why It Matters** | Ticket 15 (validated): a kill during rotation leaves an unindexed object and every mode refuses the storage until manual repair. The in-repo MTR test `storage_interrupted_binlog_creation.test` states that the next start "must recover", so the team treats this as a bug. |

Priority: P0. Confidence: high (mechanism traced, MTR test encodes the expectation). Fault requirements: workload kill of PBS (or node termination of the PBS container).

**Open Questions:**

- Is the intended fix roll-back (delete leftovers) or roll-forward (index them)? This decides how much the convergence check must add.
- Is `storage_interrupted_binlog_creation.test` currently failing with no fix in flight?

### backend-error-exit-leaves-openable-storage — Any backend error that ends fetch/pull leaves a storage the next run opens

| | |
|---|---|
| **Type** | Safety |
| **Property** | When fetch or pull exits non-zero because of a storage-backend error, the next start opens the storage. |
| **Invariant** | Workload: `Always(restart_after_error_exit_opened_storage)` after every non-kill non-zero writer exit, with a message distinct from the kill property so triage separates the two triggers. SUT: `REACHABLE("binlog rotation failed after creating the data object")` with the failing step (magic / `.json` / index) in details. |
| **Antithesis Angle** | Needs no process faults. A PBS↔S3 partition or latency spike during a rotation exhausts SDK retries on the `.json` or index PUT. An ambiguous PUT (the server applied it but the response was lost) can leave an object behind even when the client saw a failure. This uses Antithesis's default network faults. |
| **Why It Matters** | README:373 claims any error leaves "storage … in consistent state". Ticket 15's text reports the same wedge from failed index uploads alone. Under S3 throttling this is an operational outage with no crash. |

Priority: P0. Confidence: high. Fault requirements: network faults on the PBS↔S3 link (default), or `fsfault` ENOSPC/EIO for the filesystem backend.

**Open Questions:**

- Does "consistent state" in README:373 include "openable by the next run"?
- Can an aborted S3 CRT upload leave an object (ambiguous success)?

### restart-after-kill-converges-to-source — After kills and restarts, a final fetch makes the storage an exact copy of the source

| | |
|---|---|
| **Type** | Safety |
| **Property** | After any sequence of writer kills, restarts and faults, a final fault-free fetch to EOF leaves every stored binlog byte-identical to the source's binlog (IN_USE cleared; magic-only tail allowed). |
| **Invariant** | Workload, in the terminal convergence check (`eventually_` or after `ANTITHESIS_STOP_FAULTS`): `Always(check_complete_copy())`, reusing `extra/qa/fetch_crash_test.py:636-697`. `Always` because each evaluation of convergence must hold. `Sometimes(kill_count >= 3 before convergence)` is a coverage hint. SUT, GTID mode: `ALWAYS(!storage->get_gtids().contains(gtid), "received GTID is not already in storage")` at the collector's write site. |
| **Antithesis Angle** | Kills between data append and metadata PUT, during disconnect cleanup, during recovery itself and during rotation, mixed with partitions between restarts. The search space is the cross-product of kill points and resume modes (position/GTID, checkpointing on/off, rewrite, encryption). |
| **Why It Matters** | SG1 (README:711, resume continuity) is the product's core promise. A gap or duplicate is silent until a point-in-time recovery fails. Ticket 14 is a known GTID-mode exception the oracle must account for. |

Priority: P0. Confidence: high (design traced; oracle exists). Fault requirements: workload kill of PBS; network faults optional.

**Open Questions:**

- Is byte identity an official guarantee or only an MTR convention?
- Does the harness pin `sync_binlog=1` on the source?

### startup-recovery-is-restartable — Killing PBS during its own startup recovery never makes the storage worse

| | |
|---|---|
| **Type** | Safety |
| **Property** | A kill or backend fault during startup recovery (`.tmp` cleanup, tail truncation, encryption-format metadata upgrade, first `metadata.json`) leaves a storage that a later start still recovers and converges. |
| **Invariant** | Workload: `Always(post_recovery_kill_restart_opens_and_converges)` after kills timed into the first moments of startup (triggered off the recovery log lines or a short random delay). SUT: `REACHABLE("startup removed leftover .tmp objects")`, `REACHABLE("startup truncated tail binlog to metadata size")`, and `ALWAYS(metadata size >= 4, "tail truncation target is at least the binlog magic")` before `storage_core.cpp:979`. |
| **Antithesis Angle** | On S3, truncation is GET → local truncate → PUT of the whole tail object (`s3_storage_backend.cpp:765-808`), which is a wide window for kills and partitions. Crash loops (a kill shortly after every start) stress whether repeated partial recovery converges. |
| **Why It Matters** | Recovery code that assumes a clean run is classic failure-recovery debt. A recovery step that is not idempotent turns one crash into permanent damage. Each step looks idempotent today, and this property turns that reading into a checked claim. |

Priority: P1. Confidence: medium (each step reads as idempotent; the value is regression protection and S3 consistency). Fault requirements: workload kill of PBS; network faults to S3.

**Open Questions:**

- Does the S3 stand-in give read-after-write consistency for an overwrite followed by a ranged GET?

### interrupted-purge-leaves-openable-storage — A purge that is killed or whose deletes fail still leaves a storage every mode can open

| | |
|---|---|
| **Type** | Safety |
| **Property** | After `purge_binlogs` ends with any outcome (`success`, `warning`, `error`, or killed), the storage opens in every mode, the resume position is unchanged, and `success` implies no victim object remains. |
| **Invariant** | Workload: `Always(post_purge_storage_opens)`, `Always(post_purge_resume_position_unchanged)`, and `Always(status_success_implies_no_victim_objects)`. Three distinct assertions, each `Always`. SUT: `REACHABLE("purge committed the index but object cleanup failed")` at `storage_core.cpp:421`. |
| **Antithesis Angle** | A workload kill of the purge process between the index commit (`:398`) and the deletes, or a PBS↔S3 partition during the DELETE batch (yields `warning`), or an ambiguous index PUT (reported `error`, yet committed). The latter two need only the default network faults. |
| **Why It Matters** | Lead N3: the code comment admits leftovers "will trip the constructor's validators", so a routine purge with a transient S3 hiccup takes the backup offline, and purge itself cannot retry. Users are told `warning` means only that some files remain. |

Priority: P0. Confidence: high (behaviour stated in code comments and traced). Fault requirements: network faults to S3, or workload kill of purge.

**Open Questions:**

- Is "unopenable after a warning" accepted behaviour?
- Should a retried purge with the same target be required to succeed?

## Category B: Retry semantics

### failed-flush-retry-never-duplicates — The destructor's re-flush after a failed write never persists the same transactions twice

| | |
|---|---|
| **Type** | Safety |
| **Property** | No stored binlog ever contains the same transaction twice, including after a write failure followed by the destructor's implicit retry. |
| **Invariant** | Workload after convergence: `Always(no_duplicate_transaction_in_storage)`, using an event-level scan (GTID uniqueness; byte identity in position mode). SUT: `UNREACHABLE("write_event_block re-appends after a failed metadata save")`, driven by a flag set when `save_binlog_metadata` throws after `size +=` (`storage_core.cpp:310-321`). `REACHABLE("storage destructor swallowed a flush exception")` at `storage.cpp:74`. |
| **Antithesis Angle** | Needs a PBS↔S3 partition that starts after the data PUT and before the metadata PUT, and heals before `~storage` runs. A second path is an ambiguous metadata PUT (applied, response lost). These are precise timing windows that Antithesis's partition scheduling can hit and replay. |
| **Why It Matters** | Ticket 18 (validated): a non-idempotent retry turns a transient S3 error into a permanent duplicate that startup recovery cannot repair, because metadata agrees with the doubled data. |

Priority: P0. Confidence: high. Fault requirements: network faults to S3 (default); `fsfault` for the filesystem variant.

**Open Questions:**

- What are the CRT SDK default retry, backoff and timeout values (they decide how long a partition must last)?
- Which fix direction is intended (no destructor re-flush vs re-read the stored size)?

### inprocess-reconnect-resumes-without-dup-or-gap — pull's in-process reconnect continues the open binlog with no duplicated or missing transaction

| | |
|---|---|
| **Type** | Safety |
| **Property** | After any number of in-process reconnects (2013 / read-timeout path), pull's storage remains a byte prefix of the source with no transaction stored twice or skipped. |
| **Invariant** | Workload, per quiet period: `Always(storage_holds_prefixes() and GTIDs unique)`. SUT: `SOMETIMES(binlog_reused_after_reconnect, "pull resumed into its still-open binlog after reconnect")` at `collector_context.cpp:535`, and `ALWAYS(current_position == flushed_position, "after disconnect cleanup the event buffer is empty")` after `collector_context.cpp:200`. |
| **Antithesis Angle** | Partitions and resets on the PBS↔source link at arbitrary event offsets (mid-transaction, right after rotation, during a checkpoint). Correlated S3 faults at the moment of the disconnect flush push the process into the destructor-retry path. Flapping links exercise the per-connection `reader_context` preamble logic. GTID mode lacks the position check that position mode has (`collector_context.cpp:522-533`). |
| **Why It Matters** | Reconnect is pull's advertised resilience mechanism (README:378). Unlike a restart, it carries live in-memory state across the fault, so a cleanup bug becomes silent corruption rather than a failed start. |

Priority: P1. Confidence: medium (mechanism traced; no concrete bug found, value is in the unexercised GTID path and the correlated faults). Fault requirements: network faults on the PBS↔source link (default).

**Open Questions:**

- Which mid-stream errnos do Antithesis faults produce (2013 vs 2006/1053/1317)?

### pull-reconnects-after-transient-source-fault — After a transient source-side fault heals, pull eventually resumes collecting

| | |
|---|---|
| **Type** | Liveness |
| **Property** | Once faults stop, a running pull process reconnects and stores transactions committed after the quiet period began, within `idle_time + connect_timeout + slack`. |
| **Invariant** | Workload, once per quiet period (`ANTITHESIS_STOP_FAULTS`): `Always(pull alive AND storage advanced past a post-quiet-period commit within the bound)`. This is a bounded-liveness check, so every evaluation must pass. `Sometimes(pull reconnected in-process and caught up)` guards against vacuous passes. SUT: `SOMETIMES(native_error == CR_SERVER_LOST, …)` and `REACHABLE("mid-stream fetch failed with non-2013 errno")` (errno in details) at `connection.cpp:548`. `REACHABLE("pull reconnect attempt succeeded after idle")`. |
| **Antithesis Angle** | Explores which fault (partition, RST, source kill, dump-thread KILL, S3 failure during a checkpoint) maps to which errno or exception, and whether pull survives it. Fixed `idle_time` retry with no backoff means no retry storm, so the interesting outcome is the fatal path. |
| **Why It Matters** | Lead N7 / A6: only errno 2013 is retried. Any other transient fault kills collection **and** the replication source together, leaving downstream clients with no data until an external restart. |

Priority: P1. Confidence: medium-high (fatal branch traced at `connection.cpp:546-551`; errno mapping unverified). Fault requirements: network faults (default); source kill needs node termination or a custom fault.

**Open Questions:**

- Is pull expected to run under a supervisor? If so, the "alive" clause drops.
- What is the errno mapping for each Antithesis fault type?

### source-crash-restart-resume-is-faithful — After the MySQL source crashes and restarts, PBS resumes and its copy stays faithful

| | |
|---|---|
| **Type** | Safety |
| **Property** | After the source mysqld is killed and restarted while PBS streams, PBS resumes, and after convergence its storage is an exact copy of the source, including the source binlog left without a ROTATE/STOP terminator. |
| **Invariant** | Workload: `Always(check_complete_copy())` after the source is back and faults stop. `Sometimes(stored a binlog the source ended unterminated)` is a coverage hint. SUT: `REACHABLE("collector closed a binlog the source left unterminated")` at `collector_context.cpp:547`. |
| **Antithesis Angle** | Source kill mid-transaction (PBS must discard the partial transaction, `collector_context.cpp:195`), source kill right after a commit PBS already flushed, and a source rotation before PBS reconnects (ticket 14 interplay in GTID mode). |
| **Why It Matters** | A binlog server exists to survive source failures. The "unterminated binlog" branch (`collector_context.cpp:514-553`) is the recovery path for exactly this event and has no byte-level test under a real source crash. |

Priority: P1. Confidence: medium. Fault requirements: **source kill**, via node termination of the mysqld container (disabled by default) or a custom fault script plus an in-container supervisor. The source must use `sync_binlog=1` and `innodb_flush_log_at_trx_commit=1`.

**Open Questions:**

- Is node termination enabled, or will source kills come from a custom fault?
- What should PBS do when it is ahead of a recovered source (`sync_binlog != 1`)?

## Category C: Structural invariants checked after recovery

### stored-rotate-or-stop-is-last-event — No stored binlog has events after its ROTATE/STOP

| | |
|---|---|
| **Type** | Safety |
| **Property** | In every stored binlog, a ROTATE (non-artificial) or STOP event, if present, is the last event. Resume never appends to a binlog that is already terminated. |
| **Invariant** | Workload, after convergence (decrypting through pull if encrypted): `Always(every stored binlog has at most one terminator and nothing follows it)`. SUT: `ALWAYS_OR_UNREACHABLE(!tail_already_terminated, "resume opens a tail binlog that is not already terminated by ROTATE/STOP")` in `storage_core::open_binlog`'s existing-binlog branch. This needs a persisted "terminated" bit or a last-event read. |
| **Antithesis Angle** | New lead, no ticket. In **rewrite mode**, a local rotation flushes the old file's generated ROTATE and its metadata (`storage.cpp:204-215`) before creating the next file (`storage_core.cpp:680-703`). If creating the next file fails, the process exits; on S3 a failed magic PUT is enough, with no kill needed. If the process is killed in that gap (filesystem), the restart takes the `is_fresh()` branch (`collector_context.cpp:369-371`), which reopens the **terminated** old file for append (`storage_core.cpp:268-291`). The next transaction lands after the ROTATE. Antithesis finds this by combining tiny `rewrite.file_size` with S3 partitions. |
| **Why It Matters** | It produces a structurally invalid binlog that no open-time check detects (data is never parsed, `storage_core.cpp:785`). PITR tools or replicas may drop or misplace the transactions after the mid-file ROTATE. Rewrite storage is also the format least covered by crash tests. |

Priority: P1. Confidence: medium (traced through `collector_context.cpp:352-427` and `reader_context.cpp:579-582`; not reproduced). Fault requirements: network faults to S3 (default) for the S3 path; workload kill for the filesystem path.

**Open Questions:**

- Does anything downstream (mysqlbinlog, pull's sender) reject or misread such a file?
- Is there a guard I missed that makes a fresh restart rotate first?

## Category D: Coverage of recovery paths

### recovery-paths-exercised — Every recovery and fallback branch is reached under the fault mix

| | |
|---|---|
| **Type** | Reachability |
| **Property** | Each recovery or fallback branch fires at least once across the run: `.tmp` cleanup, tail truncation, magic-only reopen, reconnect reuse, closing an unterminated binlog, disconnect discard, pull retry iteration, destructor-swallowed flush, purge cleanup failure, `querying_only` metadata skip, and S3 re-upload truncation. The zero-size reopen fallback must **never** fire. |
| **Invariant** | One distinct SDK call per branch (listed with file:line in the evidence file): `REACHABLE` for path markers, `SOMETIMES(cond)` where a meaningful condition exists (for example `bytes_discarded > 0`), and `UNREACHABLE("reopened tail binlog with zero size")` at `storage_core.cpp:713-718`. |
| **Antithesis Angle** | These are exploration anchors and replay checkpoints. Without them, a green run cannot be told apart from one where the faults never reached recovery code. |
| **Why It Matters** | Every recovery property above passes vacuously if its branch never runs. These markers also give triage a timeline of which recovery step preceded a failure. |

Priority: P1. Confidence: high (branches located). Fault requirements: union of the above.

**Open Questions:**

- Will SUT-side Antithesis macros be accepted in `src/` behind a CMake option? If not, log-line reachability is the weaker fallback.

## Areas examined

- `src/binsrv/storage_core.cpp`: constructor (`:150-221`), `remove_temporary_objects` (`:597-630`), `open_binlog` / `open_new_binlog_file_internal` / `open_existing_binlog_file_internal` (`:257-297, 680-719`), `write_event_block` (`:299-322`), `purge_binlogs` (`:369-433`), index load/validate/save (`:721-799`), metadata load/validate and tail recovery (`:801-1005`).
- `src/binsrv/storage.cpp`: destructor re-flush (`:68-77`), `write_event` checkpointing (`:143-202`), `close_binlog`, `discard_incomplete_transaction_events`, `flush_event_buffer_internal` (`:204-302`).
- `src/binsrv/filesystem_storage_backend.cpp`: `put_object` tmp+fsync+rename, `resize_object`, `remove_object`, stream open/write (no fsync).
- `src/binsrv/s3_storage_backend.cpp`: temp-dir ownership and destructor cleanup (`:555-651`), `resize_object` (`:765-808`), stream open in append mode (download), whole-object re-upload per write (`:850-863, 933-938`).
- `src/binsrv/basic_storage_backend.cpp`: `remove_objects` best-effort batch (`:58-77`).
- `src/operations/collector_context.cpp`: disconnect handling (`:172-205`), connect/switch retry (`:207-272`), rewrite-mode local rotation and fresh-resume (`:274-429`), artificial ROTATE reuse/close-left-open (`:493-572`).
- `src/operations/pull_operation.cpp` (retry loop, `wait_for_interruptable`, exception → stop) and `fetch_operation.cpp` (EOF result, destructor ordering).
- `src/easymysql/connection.cpp:537-553` (2013-only retry).
- `src/binsrv/events/reader_context.cpp:516-605, 859-930` (artificial ROTATE position checks, info-only preamble on resume).
- MTR tests: `storage_interrupted_binlog_creation.test` (team expectation for ticket 15), `recover.test`, `gtid_renumbering_resume_after_partial.test`. Tickets 15, 18, 19 and 9 in `.tickets-snapshot.txt`.
- Not turned into new properties because other agents already own them (overlap noted for synthesis):
  - `fetch-exit-zero-implies-tail-flushed`: I independently confirmed that fetch's *only* final flush at EOF runs in `~storage` with errors swallowed after `result = true`.
  - `failed-flush-never-reappends-data`: overlaps my `failed-flush-retry-never-duplicates`; mine adds the ambiguous-PUT and partition-heal timing angles, so merge at synthesis.
  - `s3-local-buffer-bounded-across-crashes` (N10 temp leak per crash).
  - `startup-tmp-cleanup-spares-live-writer` and `purge-ctor-never-truncates-live-tail`: cross-process recovery races (N1/N2).
- Ticket 19 (page-cache loss) is excluded. A process or container kill keeps the page cache, so it is unreachable unless the platform drops un-fsynced data.

## Cross-property assumptions

- **PBS kills are driven by the workload**, not by Antithesis node termination: the workload spawns `fetch`/`pull`/`purge_binlogs` as subprocesses in its own container and SIGKILLs them after Antithesis-random delays. Only `source-crash-restart-resume-is-faithful` needs a cross-container kill (node termination or a custom fault script).
- The S3 stand-in (MinIO/Versity) is strongly consistent for overwrite-then-read and list. Otherwise several oracles fail for environment reasons.
- The source runs with `sync_binlog=1` and `innodb_flush_log_at_trx_commit=1`, so PBS can never be ahead of a recovered source.
- The SUT is a debug or ASan build, so the assert at `storage_core.cpp:707` and the others are live. The suggested `ALWAYS` replacements make the checks independent of the build type.
- Byte-identity oracles (`check_complete_copy`, `storage_holds_prefixes`) apply to non-rewrite, unencrypted storages. Rewrite and encrypted storages are compared through pull's served stream or event/GTID-level equality. The ticket 14 GTID-mode exception must be filtered or tracked as a known failure, or it will dominate every convergence check.
- "Openable" means constructible in `streaming` mode without a validator exception. `querying_only` is deliberately tolerant and is not the bar.
