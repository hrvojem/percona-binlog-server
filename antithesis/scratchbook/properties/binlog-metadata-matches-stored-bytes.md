# binlog-metadata-matches-stored-bytes — Per-binlog metadata (GTIDs, timestamps) agrees with the stored bytes

Found by: focus 1 (data integrity), from two separate leads (SG7 GTID bookkeeping, and the W6/ticket 6 timestamp ranges).
Merged from: gtid-metadata-matches-stored-bytes (focus 1), metadata-timestamps-match-stored-events (focus 1).

## Synthesis notes

- **Type and priority.** Safety, P1. The GTID part was rated P1 and the timestamp part P2. The merged property takes P1 because GTID-mode resume and `search_by_gtid_set` read only the metadata.
- **One oracle shape.** Both parts work the same way: at quiescence, parse `[0,size)` of each binlog and compare a metadata field with what the bytes contain.
- **Workload `Always` checks**, each with its own message:
  - "binlog added_gtids equals GTIDs in stored bytes"
  - "consecutive binlog previous_gtids chain is consistent"
  - "stored PREVIOUS_GTIDS event equals metadata previous_gtids"
  - "binlog min/max timestamp equals stored event range"
- **SUT-side checks:**
  - A chain check in `load_and_validate_binlog_metadata_set`. This is missing today.
  - Promote `assert(incomplete_transaction_timestamps_.is_empty())` at `storage.cpp:138` to `ALWAYS`. This is partial today.
- **Out of scope here.** The `block_gtids ∩ added_gtids == ∅` assertion belongs to `stored-transactions-exactly-once-across-resumes`. This file references it rather than duplicating it.

## What led here

### GTIDs (SG7, GTID bookkeeping)

- In GTID mode, the `.json` metadata is the single source of truth for two things:
  - **Resume.** The resume GTID set is the tail's `previous_gtids ∪ added_gtids` (`storage_core.hpp:313-337`).
  - **`search_by_gtid_set`** (`search_helpers.cpp:22-52`).
- Data bytes are never parsed on open (TODO at `storage_core.cpp:785`). A disagreement between metadata and bytes is therefore silent:
  - resume asks for the wrong set, so transactions are skipped or duplicated;
  - PITR planning returns the wrong files.

### Timestamps (W6, ticket 6)

- `search_by_timestamp` plans PITR from the per-file `min_timestamp`/`max_timestamp` in `.json` (`search_helpers.cpp:72-77`). It never reads the bytes.
- If those fields disagree with the events actually stored, PITR planning silently omits or adds files. sut-analysis rates this W6 as "Silent".
- Ticket 6 (validated, latent) is a mechanism that can leak a timestamp from a discarded partial transaction into the next file's range.

## Code paths

### GTIDs

- **New file.** `previous_gtids = get_gtids_unsafe()` and `added_gtids = {}` (`storage_core.cpp:688-699`). The first value is the tail's prev ∪ added, or `purged_gtids_` on an empty storage.
- **Checkpoint.** `added_gtids += block_gtids` happens in the same metadata PUT as `size` (`:311-316`).
  - `block_gtids` comes from `gtids_in_event_buffer_`.
  - The facade fills that buffer only at transaction boundaries (`storage.cpp:162-165`) and clears it after a successful flush (`:298-300`).
- **Load.** `load_and_validate_binlog_metadata_set` (`:927-1005`) checks that prev/added are present in GTID mode. It **never** checks the chain `prev[i+1] == prev[i] ∪ added[i]` or that the sets are disjoint.
- **Empty storage.** PBS runs `SELECT @@GLOBAL.gtid_purged`, then starts the dump (`collector_context.cpp:229-240`).
- **`purged_gtids_ = front().added_gtids` on load** (`:1001-1004`). It arguably should be `previous_gtids`. It is read only when the storage is empty.
- **Rewrite.** The body of the generated PREVIOUS_GTIDS event is `storage_->get_gtids()` (`collector_context.cpp:412-417`).

### Timestamps

- **Collection.** `storage::write_event` adds every event's timestamp to `incomplete_transaction_timestamps_`. At a transaction boundary, these move to `ready_to_flush_timestamps_` (`storage.cpp:143-170`).
- **Discard.** `discard_incomplete_transaction_events` clears `incomplete_transaction_timestamps_` (`storage.cpp:217-223`).
- **Close and open (ticket 6).** `storage::close_binlog` clears the event buffer but not `incomplete_transaction_timestamps_` (`storage.cpp:204-215`). `open_binlog` then `assert`s that it is empty (`storage.cpp:134-138`). A debug build aborts; a release build leaks silently.
- **Checkpoint.** `write_event_block` merges the block's range into `record.timestamps` in the same `.json` PUT as `size` (`storage_core.cpp:317`).
- **Rewrite.** The generated FDE, PREVIOUS_GTIDS and ROTATE events use the PBS wall clock (`event_generation_helpers.cpp:48, 117, 149`).
  - These events are written, so they enter the range. That is consistent with the bytes, but skewed relative to source time.
  - Clock faults make the skew visible.
- **Artificial ROTATE events** have timestamp 0 and are info-only; they are not written.
- **Mid-transaction acceptance of an artificial ROTATE.** `reader_context.cpp:797-822` accepts it in the "any other" state.

## Failure scenarios

### GTID part

1. **[trigger: ticket 18 duplicate re-append]** The bytes hold a duplicate, but `added_gtids` is a set. The metadata lists each GTID once while the bytes hold it twice.
2. **[trigger: failed flush then retry]** A failed flush leaves `gtids_in_event_buffer_` intact, and the retry re-adds it. That is idempotent and fine for the metadata. Combined with the duplicate bytes from scenario 1, though, the metadata and the bytes disagree on how many times each GTID appears.
3. **[trigger: mid-transaction disconnect]** A GTID could be recorded while its bytes were discarded, or the reverse.
   - `discard_incomplete_transaction_events` resets the buffer to the last boundary.
   - `gtids_in_event_buffer_` only ever contains complete transactions, so this should hold. Antithesis tests it under varied timing.
4. **[trigger: source purges between `SELECT @@GLOBAL.gtid_purged` and the dump on an empty storage]**
   - Expected outcome: failure with 1236 (fail-stop).
   - If it does not fail, the first `previous_gtids` disagrees with the first stored PREVIOUS_GTIDS event.
5. **[trigger: ticket 14 skipping transaction-less source binlogs]** The chain still holds, because the skipped files add nothing.

### Timestamp part

6. **[trigger: source crash right after a rotation, position mode; the ticket 6 mechanism]** Reachable from the wire (see the investigation log; this was "latent" in ticket 6):
   1. The source rotates to N and is SIGKILLed before any transaction is written to N, so N holds only FDE + PREVIOUS_GTIDS and no ROTATE/STOP (sender comment case 4c).
   2. PBS had created N (magic only). Its FDE/PGTIDS were buffered without a transaction boundary and were discarded on the disconnect.
   3. PBS reconnects with `(N, 4)`. The source sends ROTATE(N,4), N's FDE and PGTIDS (written to the buffer, timestamps into `incomplete_transaction_timestamps_`), then, because N is no longer active, an artificial ROTATE(N+1,4).
   4. `process_artificial_rotate_event` calls `close_binlog()`. That flush writes nothing because there is no boundary, then `event_buffer_.clear()` drops N's preamble but keeps its timestamps. `open_binlog(N+1)` follows.
   5. Debug build: the `assert` at `storage.cpp:138` aborts PBS. Every retry replays the same sequence, so the storage is wedged. Release build: the `min_timestamp` of N+1 includes N's preamble timestamps, which come from no event stored in N+1.
   6. Either way, N stays magic-only forever while the source's N holds FDE + PGTIDS. This byte-fidelity loss belongs to `source-crash-restart-resume-is-faithful`.
   - GTID mode does not hit the timestamp leak. Its request makes the source start at N+1, so N's preamble is never received; N stays magic-only, as in ticket 14.
   - A close in the middle of a real transaction is still unreachable: `finish_transaction()` raises `logic_error` first (`reader_context.cpp:1011-1015`).
7. **[trigger: failed flush then retry]** `ready_to_flush_timestamps_` is not cleared on failure and is re-added. That is idempotent for min/max.
8. **[trigger: clock jumps on the source]** Antithesis clock faults affect all containers, so source timestamps can be non-monotonic (A14).
   - This property still holds, because the range is computed over the bytes.
   - `search_by_timestamp` semantics do break. That belongs to a search property, not this one.

## Oracle / workload checks

When: after each PBS exit or kill, and periodically on a quiescent storage. Decrypt through pull if the storage is encrypted.

GTID checks apply in GTID mode, for indexed binlogs i = 0..n-1:

| Message | Check |
|---|---|
| `Always("binlog added_gtids equals GTIDs in stored bytes")` | `added[i]` == the set of GTIDs of GTID/GTID_TAGGED events in bytes `[0, size_i)`. |
| `Always("consecutive binlog previous_gtids chain is consistent")` | `prev[i+1] == prev[i] ∪ added[i]` for each consecutive pair of files **currently in the index**. Purge removes a prefix and never rewrites survivors' metadata (`storage_core.cpp:369-420`), so the chain over survivors holds, and the new head's `prev` includes the purged GTIDs. Files skipped by ticket 14 are absent from the index, but they carry no transactions, so the chain still holds across the gap. |
| `Always("stored PREVIOUS_GTIDS event equals metadata previous_gtids")` | Non-rewrite: the PREVIOUS_GTIDS event in the bytes of file i, if present, equals `prev[i]`. Rewrite: the generated PREVIOUS_GTIDS equals `prev[i]` by construction, and this check guards that. |
| `Always("binlog added_gtids sets are disjoint from each other and from previous_gtids")` | `added[i] ∩ added[j] == ∅` for i ≠ j, and `added[i] ∩ prev[i] == ∅`. This is the metadata-level counterpart of the uniqueness check owned by `stored-transactions-exactly-once-across-resumes`. If that property's workload already performs it, keep it there. |
| `Always("binlog metadata GTIDs are contained in source gtid_executed")` | `prev[0] ∪ ⋃ added` ⊆ the source's `gtid_executed`. |

Timestamp check, for each stored binlog:

| Message | Check |
|---|---|
| `Always("binlog min/max timestamp equals stored event range")` | `min_timestamp`/`max_timestamp` in `.json` equal the min/max of the `timestamp` header field over the events in `[0, size)` whose timestamp is **non-zero**. `ctime_timestamp_range::add_timestamp` ignores epoch (0) timestamps (`util/ctime_timestamp_range.cpp:36-39`). If no event has a non-zero timestamp (magic-only or empty file), both fields hold the epoch value, which is the empty range. |

## SUT-side instrumentation suggestions

The codebase has no Antithesis SDK, so every SDK assertion here is missing. Existing C `assert`s count as partial.

| Assertion | Site | Status |
|---|---|---|
| `ALWAYS(prev[i+1] == prev[i] ∪ added[i], "loaded binlog metadata previous_gtids chain is consistent")` | `load_and_validate_binlog_metadata_set` (`storage_core.cpp:927-1005`), for non-`querying_only` modes | missing; there is no check at all today |
| `ALWAYS(new record previous_gtids == previous record prev ∪ added, "new binlog previous_gtids equals predecessor prev plus added")` | `open_new_binlog_file_internal` (`storage_core.cpp:688-699`) | missing. It is trivially true by construction today, but guards a future refactor. |
| `ALWAYS(incomplete_transaction_timestamps_.is_empty(), "no stale incomplete-transaction state when opening a binlog")` | `open_binlog` (`storage.cpp:138`), promoting the existing assert | partial. The C `assert` exists and aborts only in debug builds. In the planned debug build, scenario 6 shows up as a PBS SIGABRT, not as a named property failure. Promote it, or place the `ALWAYS` in `close_binlog` (`storage.cpp:204-215`) before `event_buffer_.clear()`, as `ALWAYS(incomplete_transaction_timestamps_.is_empty(), "binlog closed with no incomplete-transaction timestamps")`. |
| `REACHABLE("collector closed a binlog holding only buffered preamble events")` | `collector_context.cpp:547` (the `close_binlog` in `process_artificial_rotate_event`), when the event buffer is non-empty with no boundary | missing; an exploration hint for scenario 6 |
| `ALWAYS(block_gtids ∩ added_gtids == ∅)` in `write_event_block` | Owned by `stored-transactions-exactly-once-across-resumes`; referenced here, not duplicated | missing |

## Key observations

- Data bytes are never parsed on open (`storage_core.cpp:785`), so only an external parse can detect metadata/bytes drift.
- Both `search_by_gtid_set` and `search_by_timestamp` trust the metadata alone. Any drift gives a silently wrong PITR plan.

## Fault requirements

- Kills, plus network faults on PBS↔source (mid-transaction disconnects) and on PBS↔storage (failed flushes).
- The source purging binlogs around a fresh start.
- Clock faults for the rewrite/timestamp skew observations.
- No node termination is needed.

## Property changes from investigation

- The timestamp part is no longer only a regression guard. Scenario 6 can be reached with a source SIGKILL right after a rotation in position mode, and both are in the planned fault set (`parallel_driver_kill_mysqld.sh` plus `FLUSH BINARY LOGS`). The merged property stays Safety, P1. The timestamp check is now a live check, not a P2 guard.
- The timestamp oracle formula is now exact: it excludes events with timestamp 0, and the empty range is the epoch value.
- The chain oracle compares consecutive files currently in the index. That covers both purge and ticket 14.
- The `purged_gtids_ = front().added_gtids` question is dropped from this property. Today nothing reads `purged_gtids_` on a non-empty storage, so it cannot affect the property. It moves to the file-level open questions (see log).

## Open questions

- None

### Investigation Log

#### Is `purged_gtids_ = front().added_gtids` (`storage_core.cpp:1001-1004`) intended, or was `previous_gtids` meant?

- Examined: `storage_core.cpp:997-1004` and the assert at `:219-220`, `storage_core.cpp:225-237` (`set_purged_gtids` refuses a non-empty storage), `storage_core.hpp:313-320` (`get_gtids_unsafe` reads `purged_gtids_` only when the storage is empty), `collector_context.cpp:229-240` (the only reader, under `is_empty()`), commit ec557f3 (PS-11054), whose message says the purged set "is stored in the 'previous_gtids' field of its [first] metadata file".
- Found: the load-time assignment and the matching assert were added in the same commit. The commit message describes purged GTIDs as the first file's `previous_gtids`, which conflicts with assigning `added_gtids`. No code path reads `purged_gtids_` once the storage is non-empty, and purge refuses to remove the tail, so the storage cannot become empty after load.
- Not found: maintainer intent.
- Conclusion: no effect on this property, so it is removed from the property's list. The intent question should move to the catalog-level open questions, tagged `(needs human input)`. The commit message suggests `previous_gtids` was meant.

#### Should the chain be required to hold after a purge?

- Examined: `storage_core::purge_binlogs` (`storage_core.cpp:369-420`), `select_purge_victim_count` (`:333-366`).
- Found: purge removes a prefix of records, rewrites `binlog.index`, and deletes the victims' data and `.json`. It never modifies a survivor's metadata. The chain over survivors therefore holds by construction, and the head's `prev` keeps including the purged GTIDs.
- Conclusion: resolved. The oracle compares consecutive pairs in the current index; `prev[head]` is not expected to equal `gtid_purged` of the source.

#### In GTID mode with ticket 14, may a source binlog that PBS skips be absent from the index?

- Examined: ticket 14 (MTR `gtid_resume_after_rotation` and the Hypothesis repro), `collector_context.cpp:493-570` (`process_artificial_rotate_event`), `reader_context.cpp:579-598`.
- Found: yes. The collector opens whichever file the first artificial ROTATE names and has no path that creates files the source did not send. The new file's `previous_gtids` is `prev ∪ added` of the previous stored file (`storage_core.cpp:688-699`). The skipped source files hold no transactions, so this equals the source's PREVIOUS_GTIDS for the new file.
- Conclusion: resolved. The chain and the stored-PGTIDS check still hold across the gap. Only an index-vs-source check (not part of this property) sees the missing files.

#### Can the mid-transaction `close_binlog` path actually be reached from the wire?

- Examined: `reader_context.cpp:749-830` (artificial ROTATE/ROTATE/STOP handling in `gtid_log_expected` and `any_other_expected`), `reader_context.cpp:1011-1020` (`finish_transaction` raises if the transaction is incomplete), `reader_context.hpp:74-78`, `collector_context.cpp:186-196` (disconnect: discard, then flush), `collector_context.cpp:493-578` (both `close_binlog` callers), `storage.cpp:134-138, 204-223`, `pull_operation.cpp:126-157` (exceptions end pull, no reconnect with stale state), `easymysql/connection.cpp:537-553`, the sender comment's case 4c (`sender_context.cpp:100-108`), MySQL `Binlog_sender` (PXC 9.6 tree `sql/rpl_binlog_sender.cc:401-416`: a fake ROTATE is sent before each next file), deployment-topology.md (mysqld SIGKILL with a restart loop).
- Found:
  - **Mid-transaction in the strict sense: unreachable.** An artificial ROTATE, ROTATE or STOP that arrives while a transaction is incomplete makes `finish_transaction()` raise `logic_error` before any `close_binlog`. A disconnect goes through `discard_incomplete_transaction_events()`, which clears the timestamps. Any other fetch error throws and ends the process.
  - **Preamble-only state: reachable.** The FDE and PGTIDS of a new file are written with `at_transaction_boundary == false`, so their timestamps sit in `incomplete_transaction_timestamps_` until the first transaction ends.
    - In position mode, a source SIGKILL right after a rotation leaves N with only FDE + PGTIDS and no terminator.
    - On resume at `(N, 4)` the source sends N's preamble, then a fake ROTATE(N+1). The reader accepts that ROTATE in `gtid_log_expected`; `finish_transaction` passes because both lengths are 0.
    - `process_artificial_rotate_event` then calls `close_binlog()` with the preamble buffered and no boundary. The flush is skipped, the buffer is cleared, and the timestamps remain. `open_binlog(N+1)` hits the assert.
- Not found: an end-to-end run. This is a code trace, not a reproduction. The MySQL behaviour (no terminator after a crash, fake ROTATE to the next file) relies on the sender comment and the PXC 9.6 source; 8.0 and 8.4 were not read.
- Conclusion: resolved, and the priority question resolves upward. Scenario 6 is reachable with in-plan faults, so the timestamp check is live. Side effect for the orchestrator: N stays magic-only forever, which breaks byte fidelity for `source-crash-restart-resume-is-faithful`. In a debug build every retry aborts the same way, a liveness wedge. A new ticket could be drafted for this; ticket 6 calls it latent.

#### How does `ctime_timestamp_range` treat events with timestamp 0?

- Examined: `util/ctime_timestamp_range.cpp:23-58`, `util/ctime_timestamp_range.hpp` (`is_empty()` is `min_timestamp_.is_epoch()`).
- Found: `add_timestamp` returns early for an epoch (0) timestamp, so such events never widen the range. An empty range has both fields at the epoch.
- Conclusion: resolved. The oracle formula is updated in the table above.
