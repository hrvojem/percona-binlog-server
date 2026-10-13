# Property discovery 09: Idempotency and Replay

Focus: re-streaming after a reconnect or restart (duplicates or gaps at the resume point, in position, GTID and rewrite modes), destructor re-flush (ticket 18), repeated fetch on a complete storage, downstream pull clients that reconnect, repeated or retried purge, and replayed S3 requests. Evidence files are in `antithesis/scratchbook/properties/`. No Antithesis instrumentation exists, so every SUT-side assertion below is **missing**.

## Category: Writer-side replay (collector ↔ source ↔ storage)

### failed-flush-never-reappends-data — Failed flush is never re-appended

| | |
|---|---|
| **Type** | Safety |
| **Property** | After `write_event_block` throws, no later flush (in particular the one in `~storage`) appends the same buffered bytes a second time. |
| **Invariant** | SUT `ALWAYS(!write_failed_)` at the entry of `storage::flush_event_buffer_internal` ("storage never re-flushes a buffer after a failed write_event_block"). `Always` fits because any re-flush after a failure is a bug. Twin `SOMETIMES("metadata save failed after data append")` in `storage_core::write_event_block` confirms the window is reached. Workload `ALWAYS`: no transaction appears twice in the stored bytes. |
| **Antithesis Angle** | A fault on the metadata PUT/rename only: an S3 5xx on `*.json` through an S3 shim, ENOSPC via `fsfault`, or the N2 race where a concurrent purge deletes `.json.tmp`. The process then exits through the destructor. In GTID mode, the source's later rotations and quiet periods decide whether the duplicate wedges the storage or survives silently with exit 0. |
| **Why It Matters** | Ticket 18 (validated: `storage.cpp:286/296`, `storage_core.cpp:310/321`, `storage.cpp:68-77`). The duplicate is permanent. Position mode then wedges with CR_1236. GTID mode can exit 0 with duplicated events, and rewrite mode never notices. pull clients replay the duplicated transactions. |

Priority: P0. Confidence: high (mechanism and ticket repro). Medium for the GTID-mode "exit 0" extension.
Fault requirements: an injected S3 5xx or filesystem error on metadata writes (`s3_fault_proxy.py`/`fsfault` or an equivalent stand-in), or a concurrent purge process (N2). No node termination needed.

**Open Questions:**

- Does the S3 CRT default retry policy ever give up under transient network faults alone, or is an S3-side 5xx shim required?
- In GTID mode with no new source transactions in the tail file, does MySQL send nothing (so "exit 0 with duplicate" is reachable), or re-send from an older file (fail-stop)?

### stored-transactions-exactly-once-across-resumes — Every source transaction stored exactly once across resumes

| | |
|---|---|
| **Type** | Safety |
| **Property** | Across any sequence of fetch runs, pull reconnects, kills and signals, the stored transaction sequence equals the source's sequence from the first stored transaction onward, with no duplicates and no gaps. |
| **Invariant** | Workload `ALWAYS` at quiescence, after a final un-faulted fetch to EOF. Stored GTID sequence (GTID and rewrite modes; decrypted through pull for encrypted storages) == source sequence, and position-mode transaction boundaries == the source's `binlog_as_sent` prefix. Separate messages per mode. `Always` because one violation means corrupted backup data. |
| **Antithesis Angle** | Kills at arbitrary points during checkpoints and rotation, source↔PBS partitions and resets (2013 reconnects), source restarts and rotations while PBS is down, and S3/disk faults. These explore every resume point: mid-transaction discard, reuse of the open binlog, tail recovery and re-fetch. |
| **Why It Matters** | SG1 (README:711) is the product's core claim. A silent duplicate or gap in a binlog backup corrupts point-in-time recovery. |

Priority: P0. Confidence: high (claimed guarantee, and reusable QA oracles exist).
Fault requirements: process kill of PBS (container restart or workload-driven SIGKILL), network faults; S3/disk fault shims optional.

**Open Questions:**

- Must runs where the source purged a binlog before PBS re-fetched it be excluded (expected non-zero exit)?
- For encrypted storages, is the pull stream the only oracle path, and must ticket-10 events (≥ 16 MiB) be excluded from the workload?

### no-gtid-written-twice — No GTID is appended twice

| | |
|---|---|
| **Type** | Safety |
| **Property** | A transaction whose GTID is already stored or buffered is never appended to the storage again. |
| **Invariant** | SUT `ALWAYS` in `storage::write_event` at a transaction boundary: the GTID is not in `core_->get_gtids()` and not in `gtids_in_event_buffer_` ("transaction GTID is not already stored or buffered"). `Always` because duplicates are never valid. It fires at the moment of the duplicate, even in rewrite mode, where the reader's position-continuity check is tautological (`collector_context.cpp:422-428`). |
| **Antithesis Angle** | Rewrite mode with kills, reconnects and metadata faults. Any path that replays transactions (ticket 18, a GTID reconnect with a non-empty buffer, overlapping source failover) shows up immediately instead of at the end-of-run oracle. |
| **Why It Matters** | `added_gtids` uses idempotent `+=`, so metadata never reveals the duplicate. In rewrite mode nothing else detects it. |

Priority: P1. Confidence: high that the guard is missing; medium on which paths reach it.
Fault requirements: process kill, network faults; metadata-write faults for the ticket-18 route.

**Open Questions:**

- Does `gtid_set` expose a cheap single-GTID membership test?
- Can anonymous (non-GTID) transactions reach GTID-mode storage? If so, the assertion skips them silently.

### gtid-reconnect-with-empty-event-buffer — GTID resume request covers all buffered transactions

| | |
|---|---|
| **Type** | Safety |
| **Property** | When the collector sends COM_BINLOG_DUMP_GTID, the facade buffer holds no complete transaction that is missing from the requested GTID set. |
| **Invariant** | SUT `ALWAYS(!has_event_data_to_flush())` right before `switch_to_gtid_replication` (`collector_context.cpp:249`). `Always` because a non-empty buffer guarantees the source re-sends buffered transactions. Plus `SOMETIMES("pull collector saw EOF from a blocking dump")` to learn whether the unflushed-return path (`collector_context.cpp:184-187`) is reachable. |
| **Antithesis Angle** | Source shutdown, KILL of the dump thread, and duplicate-server_id eviction while pull is mid-file with checkpointing off. If any of these produce an EOF packet rather than error 2013, pull reconnects with a stale GTID set. |
| **Why It Matters** | Position mode resumes at flushed + buffered, while GTID mode resumes from flushed only. The mismatch causes duplicate appends (silent in rewrite mode) or a wedge (plain GTID). |

Priority: P1. Confidence: medium (the code asymmetry is certain; reachability depends on mysqld sending EOF to a blocking dump).
Fault requirements: source process stop/kill or SQL-driven KILL from the workload; no node termination needed.

**Open Questions:**

- Does mysqld ever send an EOF packet to a blocking binlog dump (shutdown, KILL, server_id eviction)?
- Should pull's case-(3) return flush like case (2)? This decides whether the assertion is expected to hold after a fix.

### resume-continuity-guard-never-trips — Resume continuity checks never fire

| | |
|---|---|
| **Type** | Safety |
| **Property** | In runs without source reset or external storage damage, PBS's resume self-checks never detect a mismatch between stored bytes and the source. |
| **Invariant** | SUT `UNREACHABLE`, one per raise site, each with a unique message: artificial-ROTATE position mismatch (`collector_context.cpp:524-532`, `reader_context.cpp:547-567`), non-contiguous first event after resume (`reader_context.cpp:908-921`), GTID resume into a non-tail stored binlog (`storage_core.cpp:276-279`). Plus workload `ALWAYS`: fetch never reports CR_1236 "position > file size" while the source still holds that binlog. `Unreachable` because each site can only be reached through a replay inconsistency. |
| **Antithesis Angle** | Converts latent duplicates and truncations from earlier faulted runs into an immediate signal on the next resume, and gives Antithesis a precise anchor for searching the causal history. |
| **Why It Matters** | These are the only in-product replay detectors. Each firing is a wedge (LG5) that needs manual repair, and it points at an earlier SG1 violation. |

Priority: P1. Confidence: high.
Fault requirements: same as stored-transactions-exactly-once-across-resumes. The workload must not RESET or swap the source in these runs.

**Open Questions:**

- Implement these in the SUT, or classify them in the workload from log text? This depends on whether the workload also performs source-reset actions (ticket 25).

### gtid-resume-no-skipped-binlogs — GTID resume skips no source binlog

| | |
|---|---|
| **Type** | Safety |
| **Property** | After a GTID-mode resume, the stored index continues with the source binlog that directly follows the stored tail, and the previous stored binlog keeps its closing ROTATE event. |
| **Invariant** | Workload `ALWAYS` at quiescence: stored names are contiguous and every non-tail stored binlog is byte-identical to the source's. SUT `ALWAYS` in the name-mismatch branch (`collector_context.cpp:541-553`): the new name is `current.next()` and the stored tail ended with ROTATE/STOP. Expected to fail today (ticket 14). |
| **Antithesis Angle** | Source rotations (FLUSH BINARY LOGS, restarts, max_binlog_size) timed during PBS partitions, kills or idle gaps between fetch runs. |
| **Why It Matters** | Ticket 14 (validated). It happens in normal operation whenever a caught-up PBS reconnects after a rotation. pull clients never see the closing ROTATE, and transaction-free binlogs are missing from the backup. |

Priority: P1. Confidence: high.
Fault requirements: network faults or PBS process kill; source restart optional.

**Open Questions:**

- Is byte identity with the source an official guarantee? If not, this property drops to P2 and only the transaction-level check remains.

### repeat-fetch-on-caught-up-storage-is-noop — Repeated fetch on a caught-up storage changes nothing

| | |
|---|---|
| **Type** | Safety |
| **Property** | With the source quiesced and the storage caught up, any number of fetch runs (including killed ones) leave every object byte-identical, and a clean run exits 0. |
| **Invariant** | Workload `ALWAYS`: the snapshot `{name: (size, sha256)}` of data, `.json`, index and `metadata.json` is unchanged, ignoring `*.tmp` removal ("fetch on a caught-up storage changes no object"). `Always` because any change is spurious. |
| **Antithesis Angle** | Kills during the re-open path (S3 append download, constructor recovery), network faults during the preamble, and S3 latency. Checks that the resume path itself is side-effect free. |
| **Why It Matters** | Cron fetch is the main backup workflow. Spurious rewrites would turn harmless repeats into crash windows (tickets 15 and 18). |

Priority: P2. Confidence: high that it currently holds (regression guard).
Fault requirements: PBS process kill, network faults.

**Open Questions:**

- Must the "caught-up" precondition exclude a magic-only GTID tail, where the source legitimately sends FDE/PGTIDS?

### s3-stale-put-replay-never-regresses-object — A replayed stale S3 PUT never regresses an object

| | |
|---|---|
| **Type** | Safety |
| **Property** | A delayed or replayed earlier PUT attempt never overwrites a newer binlog object, `.json` or index, so an object is never shorter than its saved metadata. |
| **Invariant** | Workload `ALWAYS`, polled and at quiescence: for every indexed binlog, `HEAD(object).ContentLength >= json.size`, and every `*.json` is indexed ("S3 binlog object never shorter than its saved metadata size"). Optional SUT `SOMETIMES("S3 PutObject succeeded after an SDK retry")`. |
| **Antithesis Angle** | Per-stream packet hold or delay between PBS and the S3 stand-in, long enough for SDK retries, then a burst release. Last-writer-wins on whole-object PUTs reorders a stale body after a newer one. |
| **Why It Matters** | A regressed object wedges every start ("metadata bigger than actual"). A stale non-tail `.json` wedges the storage, and a stale index orphans a file. |

Priority: P2. Confidence: low (depends on SDK timeout/retry behaviour and on whether the S3 server commits after a client reset).
Fault requirements: network faults between PBS and S3 (packet hold or delay modes).

**Open Questions:**

- Does the S3 CRT client in SDK 1.11.774 time out and retry an in-flight PutObject by default?
- Does MinIO/Versity commit a PUT whose full body arrives after the client has reset the connection?
- Does CRT auto-multipart apply, making a replayed CompleteMultipartUpload the relevant case?

## Category: Reader-side replay (pull → downstream clients)

### pull-client-reconnect-receives-each-event-once — A reconnecting pull client receives each event once

| | |
|---|---|
| **Type** | Safety |
| **Property** | A downstream client that reconnects at its last `(file, next_event_position)` across network faults and pull restarts receives every stored event exactly once, and the union of what it received equals the stored stream. |
| **Invariant** | Workload `ALWAYS("pull client never receives the same binlog offset twice across reconnects")` plus an equality check against a final clean dump. `SOMETIMES("pull client resumed mid-file after reconnect")` guards against a vacuous pass. |
| **Antithesis Angle** | Client↔pull partitions and resets, pull kill and restart, concurrent collector flushes with small checkpoint sizes, several clients, and metadata faults (ticket 18 makes the restarted pull serve a duplicate copy at the next offsets). |
| **Why It Matters** | pull is the only restore path for encrypted storage (W5). A replayed transaction applied twice downstream is silent corruption. |

Priority: P1. Confidence: medium (mechanisms validated; client reconnect semantics are workload-defined because ticket 20 blocks MySQL replicas).
Fault requirements: network faults on the client side, PBS process kill, optional metadata-write faults.

**Open Questions:**

- What resume position does `mysqlbinlog --stop-never` use after a failure, and can it be the client instead of a custom one?
- Is a silent close for a client ahead of the storage (N12) acceptable if a later reconnect succeeds?

### served-bytes-stable-across-restart — Bytes served by pull never change after restart

| | |
|---|---|
| **Type** | Safety |
| **Property** | Any byte pull served at `(file, offset)` equals the stored byte at that offset after any later restart, tail recovery and re-fetch. |
| **Invariant** | Workload `ALWAYS("bytes served by pull never change at the same offset after restart")`. SUT `SOMETIMES("storage recovery truncated a tail binlog on start")` at `storage_core.cpp:979` as a reachability anchor. |
| **Antithesis Angle** | Kill pull between the data append and the metadata save (the sender already serves up to the in-memory size, `storage_core.cpp:310` vs `:321`), in rewrite mode with a small `file_size`, so that regenerated events with a wall-clock timestamp replace served ones. |
| **Why It Matters** | Replay safety for downstream clients depends on re-fetched bytes being identical. A divergence leaves clients holding events that the backup does not contain. |

Priority: P2. Confidence: medium for the window; low that rewrite-mode generated events fall inside it.
Fault requirements: PBS process kill (precise timing); thread pausing helps.

**Open Questions:**

- Should the sender serve only metadata-durable sizes? (Is "served but not yet durable" accepted?)
- Can tail truncation ever remove a rewrite-generated event that has already been served?

## Category: Administrative replay

### purge-retry-idempotent — Retried purge converges

| | |
|---|---|
| **Type** | Safety |
| **Property** | Re-running `purge_binlogs` with the same target after an interrupted or failed run converges to the state of one successful purge, and repeating a completed purge changes nothing. |
| **Invariant** | Two workload `ALWAYS` checks with distinct messages: "purge retried after interruption converges to an openable storage" (expected to fail today: the purging-mode constructor rejects orphans, `storage_core.cpp:779-783, 993-996`), and "repeating a completed purge changes no object". Plus SUT `SOMETIMES(cleanup_warning_message non-empty)`. |
| **Antithesis Angle** | Kill purge between the index commit and `remove_objects`, inject DELETE failures, then retry. |
| **Why It Matters** | Operators retry. The code comment admits leftovers make every mode, including purge itself, refuse to open (N3). |

Priority: P1. Confidence: high (code and comment).
Fault requirements: process kill of the purge CLI (workload-driven) or S3 DELETE / unlink faults.

**Open Questions:**

- Is an unopenable storage after an interrupted purge accepted fail-stop behaviour?
- Should a repeated purge of an already-purged target report `success` or `error`?

## Category: Reachability anchors

### resume-replay-paths-exercised — Resume shapes are exercised

| | |
|---|---|
| **Type** | Reachability |
| **Property** | Antithesis drives the collector through every distinct resume shape. |
| **Invariant** | SUT `SOMETIMES`, one per shape, each with a unique message: discarded partial transaction (>0 bytes) before reconnect; reused open binlog; source names a different binlog than the tail; tail reopened with data present; recovery truncated the tail; rewrite-mode fresh context on a non-empty storage. `Sometimes(cond)` because these are rare semantic states, not invariants. |
| **Antithesis Angle** | Exploration hints and replay anchors for the exactly-once properties above. |
| **Why It Matters** | Without them the safety properties can pass vacuously. |

Priority: P1. Confidence: high.
Fault requirements: network faults, PBS process kill.

**Open Questions:**

- None.

## Areas examined

- `storage.cpp` buffer/flush/discard/destructor; `storage.hpp` position accessors.
- `storage_core.cpp`: open_binlog, write_event_block, purge, fetch_event_block, validation and recovery.
- `collector_context.cpp`: receive loop termination cases, resume request, artificial ROTATE handling, rewrite path.
- `reader_context.cpp`: preamble info-only logic and position validation in both modes.
- `pull_operation.cpp` / `fetch_operation.cpp` retry loops.
- `easymysql/connection.cpp` fetch errno handling.
- `sender_context.cpp` and `network_service.cpp` dump handling.
- `s3_storage_backend.cpp` stream/upload path.
- `purge_binlogs_operation.cpp`.
- Tickets 14 and 18 (re-read).

Two areas were judged safe by reading:
- The timeout-reconnect path empties the buffer before resuming (discard and flush).
- Position-mode resume uses flushed + buffered consistently.

Not covered here: deterministic parsing tickets (1, 3, 5, 11, 12), already under Hegel.

## Cross-property assumptions

- The workload owns a byte-level oracle per mode: direct bytes for plain storages, the pull stream with artificial events stripped for encrypted or rewrite storages, and `binlog_as_sent`/GTID extraction from `extra/qa/fetch_crash_test.py`.
- The source is never RESET or swapped in runs that check these properties (ticket 25 runs are tagged and excluded).
- A debug/ASan build, so that assert-only invariants (for example `storage_core.cpp:707`) are live.
- Metadata-write faults need an S3 fault shim or `fsfault`. Antithesis does not inject S3 5xx or ENOSPC natively.
- Overlap noticed with slugs written concurrently by other agents: `failed-flush-retry-never-duplicates`, `enospc-write-fails-closed-without-duplicates` (ticket 18), `gtid-resume-preserves-source-rotations` (ticket 14), `sender-reads-only-flushed-bytes`. Merge these at synthesis. My versions add the GTID-mode "exit 0 with duplicate" path, the rewrite-mode blind spot, and the retry/replay framing.
