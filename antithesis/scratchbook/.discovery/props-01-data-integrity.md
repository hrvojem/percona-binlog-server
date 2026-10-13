# Property discovery 01 — Data Integrity

Focus: stored binlogs as byte-exact prefixes of the source ending on transaction boundaries, metadata/data agreement, GTID bookkeeping, encryption at rest, rewrite-mode consistency, checkpoint durability. Evidence files are in `antithesis/scratchbook/properties/{slug}.md`. All SUT-side assertions are **missing** (no Antithesis instrumentation exists). Where an `assert` already exists it is marked **partial**.

## Category: Stored bytes vs source

### stored-binlog-byte-prefix-of-source — Stored binlogs are byte prefixes of the source's binlogs

| | |
|---|---|
| **Type** | Safety |
| **Property** | For non-rewrite storages, every indexed binlog's bytes `[0, metadata.size)` equal the source's same-named binlog as sent, and `binlog.index` names a contiguous run of the source's binlogs. |
| **Invariant** | Workload `Always`, checked after every PBS exit or kill and at quiescent points. It compares stored bytes with `binlog_as_sent(source)` (QA `storage_holds_prefixes`), reading through pull for encrypted storages. `Always` fits because any divergent byte is a silent backup corruption. SUT-side: `ALWAYS(stream position == record.size before append)` in `storage_core::write_event_block`, and the `storage_core.cpp:707` assert promoted to `ALWAYS`. |
| **Antithesis Angle** | Kills at any instruction during checkpoints, reconnects and rotations. Network faults mid-transaction. S3 5xx or stall on data and `.json` PUTs. Source restarts and rotations while PBS is disconnected. The append-offset assertion fires the moment bytes land at a wrong offset, before any oracle runs. |
| **Why It Matters** | This is the core promise of a binlog backup (README:3, 711). Tickets 18 and 24 and leads N1 and 25 all produce non-prefix stores. |

**Priority:** P0 · **Confidence:** high (mechanism and oracle exist in the QA harness; violations are validated tickets) · **Fault requirements:** process kill (node termination or a workload-driven kill), network faults; S3/disk fault shims for the failed-write paths.

**Open Questions:**

- Is byte-identity to the source an official guarantee or an MTR convention?
- Is a writer running concurrently with purge, or a second writer, in contract?
- Can the workload read the source binlog files byte-exactly, or must it go through `mysqlbinlog --raw`?

### metadata-size-at-transaction-boundary — Persisted size always ends on a transaction boundary

| | |
|---|---|
| **Type** | Safety |
| **Property** | Every binlog's `.json` `size` ends exactly at an event boundary that is also a transaction boundary, the end of the preamble, the magic, or a terminal ROTATE/STOP. |
| **Invariant** | Workload `Always` after each PBS exit or kill: parse `[0, size)` and check that it ends exactly at `size` on an allowed boundary. SUT-side: `ALWAYS(flushed block ends on reader boundary)` in the facade (missing). The `storage.cpp:278` assert becomes an `ALWAYS` (partial). `SOMETIMES(discarded partial transaction on disconnect)` and `SOMETIMES(checkpoint flush left an incomplete transaction buffered)` act as exploration anchors. |
| **Antithesis Angle** | Checkpoints that fire mid-burst, disconnects in the middle of multi-event transactions, and kills between the data append and the metadata PUT. Thread pausing around `flush_event_buffer_internal`. |
| **Why It Matters** | A tail that is not on a boundary resumes mid-transaction. The next run then fails the reader FSM or silently produces a torn transaction (SG3, README:373/715/721). |

**Priority:** P0 · **Confidence:** high · **Fault requirements:** process kill, network faults; checkpoint_size/interval enabled in the config.

**Open Questions:**

- Does `reader_context` classify every transaction shape correctly (XA, compressed `TRANSACTION_PAYLOAD`, atomic DDL, tagged GTIDs)?
- Is a magic-only (size 4) tail acceptable in all positions, or only before the first transaction of a new binlog?

### source-transaction-stored-exactly-once — Every source transaction is stored exactly once across resumes

| | |
|---|---|
| **Type** | Safety |
| **Property** | Across any number of runs, reconnects, signals and injected write failures, the transactions in the stored bytes are a gap-free, duplicate-free prefix of the source's committed transactions (from the storage's starting point). |
| **Invariant** | Workload `Always`: parse GTIDs from the stored bytes (not from `.json`, whose sets hide duplicates) and assert no repeats, with the union equal to a prefix of the source's commit order. Position mode relies on the byte-prefix oracle. SUT-side: `ALWAYS(block_gtids ∩ record.added_gtids == ∅)` in `write_event_block` (missing). It catches ticket 18 at the moment of the duplicate append. |
| **Antithesis Angle** | S3 5xx or a stall on the `.json` PUT followed by SIGTERM or an exception (the ticket 18 destructor re-flush). Purge or a second instance deleting `.json.tmp` (N2). Kills and partitions around reconnects. |
| **Why It Matters** | A duplicated transaction replayed during PITR corrupts the restored database. Ticket 18 is validated and its duplicate is permanent when the metadata PUT failed. |

**Priority:** P0 · **Confidence:** high (ticket 18 mechanism traced at `storage_core.cpp:299-322`, `storage.cpp:68-77, 276-302`) · **Fault requirements:** S3 fault proxy or fsfault for metadata-write failures; process kill; network faults.

**Open Questions:**

- Will the ticket 18 fix erase the buffer on failure or roll back `size`? This decides where the assertion goes.
- Is a purge concurrent with a writer in contract (the N2 trigger)?
- Will the workload enable binlog transaction compression, which needs `TRANSACTION_PAYLOAD` parsing in the oracle?

## Category: Metadata/data agreement and crash recovery

### storage-reopens-after-writer-crash — Index, objects and metadata stay mutually consistent across a writer crash

| | |
|---|---|
| **Type** | Safety |
| **Property** | After a PBS writer dies at any point, the storage passes the non-`querying_only` startup validators: index entries, data objects and `.json` objects are 1:1, non-tail sizes match, and the tail's metadata size is ≤ its data size. |
| **Invariant** | Workload `Always`: re-implement the validator checks on the storage directory or bucket after each death, and confirm that the next writer start does not fail with a validator error. SUT-side: `UNREACHABLE` at each validator raise site with distinct messages (missing). `SOMETIMES(recovery truncated tail)` and `SOMETIMES(.tmp objects removed at startup)` serve as anchors (missing). |
| **Antithesis Angle** | Kill windows inside rotation (magic → `.json` → index), inside checkpoint, inside S3 `resize_object` and inside purge. Injected PUT failures at the same points. |
| **Why It Matters** | A storage that will not open needs manual surgery and stops the backup (LG5). Ticket 15 is a validated violation (`storage_core.cpp:680-703`, `:779-783`). |

**Priority:** P0 · **Confidence:** high · **Fault requirements:** process kill (node termination or workload-driven), S3/disk fault shims. Ticket 19 only if page-cache loss is simulated.

**Open Questions:**

- Is an unopenable storage after an interrupted rotation or purge accepted fail-stop behaviour or a bug?
- Are node-termination faults enabled, or must the workload kill PBS itself?
- Does any fault drop un-fsynced page cache (ticket 19)?

### stored-state-never-regresses — Persisted progress never goes backwards

| | |
|---|---|
| **Type** | Safety |
| **Property** | Once a binlog's `.json` has recorded `(size, added_gtids)` and the data bytes `[0, size)`, later observations never show a smaller size, fewer GTIDs or changed bytes for that binlog. The index changes only by appending at the tail or by a workload-issued purge removing a prefix. |
| **Invariant** | Workload `Always`: track the maximum observed `.json` per binlog plus hashes of `[0, size)` and the last index, and compare on each observation. SUT-side, in-process only: `ALWAYS(new saved size ≥ previous saved size)` in `save_binlog_metadata` (missing). |
| **Antithesis Angle** | Kill/restart loops, S3 last-writer-wins under stalls, purge or a second writer interleaving with rotations (ticket 9 stale index rewrite, N1 purge-time truncation, ticket 24). |
| **Why It Matters** | Data that has been checkpointed is claimed to be permanent (README:619, 626). A regression means silent loss of data that was already backed up and that the source may have purged since. |

**Priority:** P1 · **Confidence:** medium (single-writer paths look sound. Known violations need cross-process activity whose contract status is open) · **Fault requirements:** process kill; S3 latency or stalls; optional concurrent purge or second writer.

**Open Questions:**

- Is a concurrent purge or a second writer in contract?
- Is the S3 stand-in strongly consistent enough for observations taken while a writer is active, or only at quiescent points?

### gtid-metadata-matches-stored-bytes — GTID bookkeeping in metadata agrees with stored bytes

| | |
|---|---|
| **Type** | Safety |
| **Property** | In GTID mode, each binlog's `added_gtids` equals the GTIDs present in its bytes `[0, size)`. Consecutive records satisfy `prev[i+1] == prev[i] ∪ added[i]`, `added` sets are pairwise disjoint, and the stored PREVIOUS_GTIDS event equals `prev[i]`. |
| **Invariant** | Workload `Always` per quiescent observation. SUT-side: `ALWAYS` chain check in `load_and_validate_binlog_metadata_set` (missing; there is no check today) and `ALWAYS(block_gtids ∩ added_gtids == ∅)` in `write_event_block` (missing; shared with `source-transaction-stored-exactly-once`). |
| **Antithesis Angle** | Disconnects and kills at transaction boundaries and around checkpoints. Failed metadata PUTs. A source purge racing the `SELECT @@GLOBAL.gtid_purged` → dump sequence on an empty storage. |
| **Why It Matters** | GTID-mode resume and `search_by_gtid_set` read only metadata (`storage_core.hpp:313-337`, `search_helpers.cpp:22-52`), so a disagreement causes silent skips or duplicates on resume and wrong PITR plans. |

**Priority:** P1 · **Confidence:** high (no load-time validation confirmed in code) · **Fault requirements:** process kill, network faults, S3/disk shims for the metadata-failure path.

**Open Questions:**

- Is `purged_gtids_ = front().added_gtids` intended? It is out of scope while unread on non-empty storages.
- Should the chain be checked only over surviving files after a purge?
- In GTID mode with ticket 14, may a source binlog that PBS skips be absent from the index?

### metadata-timestamps-match-stored-events — Metadata timestamp range matches stored events

| | |
|---|---|
| **Type** | Safety |
| **Property** | Each binlog's `.json` `min_timestamp`/`max_timestamp` equal the min/max event timestamps over its stored bytes `[0, size)`. |
| **Invariant** | Workload `Always` per quiescent observation. SUT-side: promote `assert(incomplete_transaction_timestamps_.is_empty())` (`storage.cpp:138`) to `ALWAYS` (partial). |
| **Antithesis Angle** | Disconnects mid-transaction followed by a file switch (the ticket 6 path). Failed-flush retries. Clock faults make rewrite-generated wall-clock stamps visible. |
| **Why It Matters** | `search_by_timestamp` plans PITR from these fields only. A wrong range silently omits or adds files. |

**Priority:** P2 · **Confidence:** medium (ticket 6 mechanism validated as latent; reachability from the wire unclear) · **Fault requirements:** network faults; clock faults optional.

**Open Questions:**

- Can the mid-transaction `close_binlog` path be reached from the wire?
- How does `ctime_timestamp_range` treat events with timestamp 0?

## Category: Rewrite-mode consistency

### rewrite-no-events-after-terminal-rotate — No stored binlog has events after its terminal ROTATE

| | |
|---|---|
| **Type** | Safety |
| **Property** | In every stored binlog, a non-artificial ROTATE or STOP event, if present, is the last event. |
| **Invariant** | Workload `Always`: parse each stored binlog. This is cheap enough to run in all modes. SUT-side: `ALWAYS(append never targets a binlog already closed by a terminal event)` in the reopen path (missing; needs a persisted "closed" flag or a last-event read). `REACHABLE("rewrite: terminal ROTATE flushed, next file not yet created")` serves as the timing anchor (missing). |
| **Antithesis Angle** | In rewrite mode the local rotation is "flush N with ROTATE" followed by "create N+1", as two steps. A kill between them, or a failed or stalled S3 magic PUT for N+1 (no crash needed), leaves N as the tail. On restart `is_fresh()` reuses N (`collector_context.cpp:367-370`) and appends the next transaction after N's ROTATE. The window on the filesystem is tiny; on S3 it lasts the whole magic upload. |
| **Why It Matters** | Readers treat ROTATE as end-of-file, so a `mysqlbinlog --raw` restore or a replica fed by pull misattributes or skips the trailing transactions. That is silent data loss on restore. Nothing in PBS notices because the rewriter keeps positions and CRCs consistent. |

**Priority:** P0 · **Confidence:** medium (new lead with no ticket, traced through `collector_context.cpp:352-427`, `storage.cpp:204-215`, `storage_core.cpp:680-703`, `reader_context.cpp:579-582`, not reproduced. Independently found by another discovery agent as `stored-rotate-or-stop-is-last-event`) · **Fault requirements:** S3 fault proxy (PUT failure or stall on the new object key) or a process kill; rewrite mode with a small `file_size`.

**Open Questions:**

- Is there a guard against reopening a terminated tail that this pass missed? A reproduction with the S3 fault proxy settles it.
- Can the same window occur in non-rewrite position mode? Reasoning says no; untested.
- Will the fix persist a "closed" flag or re-derive it on restart? This decides where the assertion goes.

### rewrite-binlog-chain-valid-across-crashes — Rewritten binlogs stay internally valid across crashes

| | |
|---|---|
| **Type** | Safety |
| **Property** | In rewrite mode every stored binlog is a valid self-consistent binlog. It has magic, FDE and a PREVIOUS_GTIDS event equal to the metadata `previous_gtids`. `next_event_position` chains and CRCs are valid. GTID `sequence_number`s run 1..k contiguously with `.json` `last_sequence_number` = k, and `transaction_length` matches. |
| **Invariant** | Workload `Always` per stored file (through pull if encrypted), plus a `mysqlbinlog --verify-binlog-checksum` pass. SUT-side: `ALWAYS(rewritten sequence_number == last + 1)` in the rewriter (missing). `SOMETIMES(rewrite resumed into non-empty file after restart)` (missing). |
| **Antithesis Angle** | Kills at arbitrary points around checkpoints and disconnects inside multi-event transactions. MTR covers one hand-built disconnect (`gtid_renumbering_resume_after_partial.test`), and rewrite has never been crash-tested. |
| **Why It Matters** | Rewrite output cannot be compared with the source byte for byte, so internal validity is its only integrity check. A gap or duplicate in sequence numbers, or a bad chain, breaks parallel replication and restore tools. |

**Priority:** P1 · **Confidence:** medium · **Fault requirements:** process kill, network faults; rewrite mode, small `file_size` and checkpoints.

**Open Questions:**

- Must `last_committed` reflect source dependencies, or is any value below `sequence_number` acceptable?
- Is `transaction_length` recomputed or copied?
- Should rewrite combined with encryption be covered by this property, with the oracle reading through pull?

## Category: Purge and exit-code contracts

### purge-preserves-surviving-data-and-resume-point — Purge never alters surviving binlogs or the resume point

| | |
|---|---|
| **Type** | Safety |
| **Property** | Under any purge outcome (success, warning, error, or the purge process killed), the new index is a suffix of the old one that keeps the tail. Surviving `.json` objects and data `[0, size)` are unchanged, and the tail's resume point (name, size, GTID set) is unchanged. |
| **Invariant** | Workload `Always`: snapshot before purge and compare after. SUT-side: `ALWAYS(purge never selects the tail)` (missing; enforced today by a throw). `SOMETIMES(index committed with leftover objects)` and `REACHABLE("purge index committed")` (missing). |
| **Antithesis Angle** | Kills between the index commit and the deletes, DELETE failures, and purge started right after a writer crash (purge's own tail recovery). If in contract, purge overlapping a live writer (N1 truncation, ticket 9). |
| **Why It Matters** | Purge is the routine retention operation (W7). Over-deletion or a changed resume point loses data or forces a re-stream from the source. |

**Priority:** P1 · **Confidence:** high for the single-process paths (`storage_core.cpp:332-433`) · **Fault requirements:** process kill; S3 DELETE fault shim.

**Open Questions:**

- Is purge concurrent with a writer in contract?
- Is a `warning` purge that leaves the storage unopenable an accepted outcome?

### fetch-exit-zero-implies-complete-copy — fetch exit 0 means the storage holds everything the source had at start

| | |
|---|---|
| **Type** | Safety |
| **Property** | When `fetch` exits 0, the storage contains every source transaction committed before the fetch started, byte-identical to the source (non-rewrite), with only the magic-only tail exception. |
| **Invariant** | Workload `Always` on each fetch exit 0, using the QA `check_complete_copy` relaxed to a "source state at start" bound. SUT-side: `UNREACHABLE("final storage flush failed but fetch reports success")` (missing). The fix would be to flush before deciding `result`. |
| **Antithesis Angle** | S3 or disk faults on the final flush, which runs in `~storage` after `result` is set and swallows every exception (`fetch_operation.cpp:80-115`, `storage.cpp:68-77`). Source RESET or swap between fetches (ticket 25). |
| **Why It Matters** | cron-driven backups treat exit 0 as "backup current". A silent shortfall is discovered only at restore. |

**Priority:** P1 · **Confidence:** high for the swallowed-flush mechanism, medium for reachability under the tenant's faults · **Fault requirements:** S3/disk fault shim on the last PUT; optionally a workload source-reset action.

**Open Questions:**

- Does exit 0 mean "complete as of EOF" or only "reached EOF on the wire"?
- Should the workload perform source identity changes (ticket 25)?
- Can standard network faults make the final S3 flush fail within CRT timeouts?

## Category: Encryption at rest

### encrypted-storage-decrypts-to-source — Encrypted storage reads back as the source's bytes after crashes

| | |
|---|---|
| **Type** | Safety |
| **Property** | For an encrypted non-rewrite storage, the plaintext served by pull for each binlog (artificial events stripped) equals the source's binlog as sent, across crashes, truncation recovery and S3 retries. |
| **Invariant** | Workload `Always` comparing per-file non-blocking pull dumps with the source. SUT-side: `ALWAYS(encryption offset == backend stream position)` at append (missing). `ALWAYS(decrypted block at offset 4 starts with a valid FDE header)` in `fetch_event_block` (missing). |
| **Antithesis Angle** | Kill mid-append followed by truncation at a non-block-aligned offset and a CTR re-append. S3 failed upload then destructor retry. S3 `resize_object`. |
| **Why It Matters** | Pull is the only in-product way to restore an encrypted storage. A keystream/offset bug makes the backup unreadable while every check on the ciphertext passes. |

**Priority:** P1 · **Confidence:** medium (well tested in-process; never crash-tested) · **Fault requirements:** process kill; S3 fault shim.

**Open Questions:**

- Will SG9 serving bugs (N5) add noise to this oracle unless each file is dumped separately from position 4?
- Will the workload rotate KEKs (the old KEK must stay in the keyring)?

### encryption-envelope-on-every-new-binlog — Once a storage is marked encrypted, every new binlog is encrypted

| | |
|---|---|
| **Type** | Safety |
| **Property** | If `metadata.json` records an encryption format, every binlog created afterwards has an encryption envelope in its `.json` and ciphertext data. |
| **Invariant** | Workload `Always` (no `\xfebin` at the start of new data objects, and an envelope present). SUT-side: `ALWAYS(!encryption_format_ || envelope)` in `open_new_binlog_file_internal` (missing). |
| **Antithesis Angle** | Only reachable when the workload restarts PBS with config variants. Antithesis adds little beyond random ordering. |
| **Why It Matters** | `validate_metadata` accepts a config that drops `encryption` on an encrypted storage, and new files are then written in plaintext (N9, `storage_core.cpp:808-829`, `:1008-1010`). The data is silently exposed. |

**Priority:** P2 · **Confidence:** medium (mechanism traced; intent open) · **Fault requirements:** none (a config-variation workload action).

**Open Questions:**

- Are a plaintext tail after enabling encryption, and silent acceptance of dropping encryption, intended?
- Should the workload vary the config across restarts at all?

## Areas examined

- `src/binsrv/storage.cpp` (facade buffer, boundary tracking, checkpoint triggers, flush, close, discard, destructor).
- `src/binsrv/storage_core.cpp` (constructor validators and recovery, `write_event_block`, new and existing binlog open, purge, `fetch_event_block` decryption, metadata save/load, encryption record generation).
- `src/binsrv/s3_storage_backend.cpp` (open_stream, write/upload, resize) and `basic_storage_backend.cpp` (remove_objects).
- `src/operations/collector_context.cpp` (connect/resume derivation, rewrite path, artificial ROTATE handling, close on rotate/stop) and `fetch_operation.cpp` (exit-code decision vs destructor flush).
- `src/binsrv/events/reader_context.cpp` (artificial ROTATE checks, info-only preamble logic, position continuity).
- `extra/qa/fetch_crash_test.py` oracles; the `mtr/.../gtid_renumbering_resume_after_partial.test` scenario.
- Not examined in depth: `sender_context` serving (SG9), the rewriter internals (`events/rewriter`), the filesystem backend fsync details beyond the sut-analysis, and the search helpers.

## Cross-property assumptions

- Oracles that compare with the source need the source to retain its binlogs for the whole run (large `binlog_expire_logs_seconds`, and no source-side PURGE unless it is a deliberate workload action), plus byte access to the source files or an equivalent raw dump.
- "Quiescent observation" means no PBS writer is running on the storage. Observations taken while a writer runs are limited to atomic per-object reads and must tolerate an un-truncated tail beyond `metadata.size`.
- Byte-identity oracles apply to non-rewrite storages. Encrypted storages are read through pull, and rewrite storages are checked by internal validity plus transaction-set equality.
- PBS kills are assumed to be available, either through node termination or a workload-controlled kill of the PBS process. Several P0 properties are weak without them, although S3 PUT failure or stall injection reaches the most important windows without a kill.
- The SUT is a debug or ASan build so that the existing `assert`s at `storage_core.cpp:707`, `storage.cpp:138, 278` are live in addition to any new `ALWAYS`.
- Single writer per storage is assumed in-contract. Properties that list N1, N2, ticket 9 or ticket 24 as violations hold only if the human answer says concurrent purge or writers are in scope.
- Overlap with other agents' files (for synthesis to merge): `stored-rotate-or-stop-is-last-event` (same lead as `rewrite-no-events-after-terminal-rotate`), `stored-transactions-exactly-once-across-resumes` / `no-gtid-written-twice` / `failed-flush-*` (overlap with `source-transaction-stored-exactly-once`), `kill-anywhere-storage-reopens` (overlap with `storage-reopens-after-writer-crash`), `encrypted-binlog-decryptable-after-crash` / `encrypted-storage-no-plaintext-at-rest` / `cipher-offset-equals-physical-offset` (overlap with the encryption properties), and `fetch-exit-zero-implies-tail-flushed` (overlap with `fetch-exit-zero-implies-complete-copy`).
