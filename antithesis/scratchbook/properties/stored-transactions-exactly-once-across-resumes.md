# stored-transactions-exactly-once-across-resumes — Every source transaction is stored exactly once across resumes

Found by: focus 9 (idempotency/replay), 1 (data integrity), 3 (failure recovery).
Merged from: stored-transactions-exactly-once-across-resumes (focus 9), source-transaction-stored-exactly-once (focus 1), restart-after-kill-converges-to-source (focus 3), no-gtid-written-twice (focus 9).

## Synthesis notes

- **Property type and priority:** Safety, P0. Focus 1, 3 and 9 agree on P0. no-gtid-written-twice was P1 as a standalone SUT guard and is folded in here as the in-process twin.
- **Main check:** a workload `Always`, evaluated at quiescence after a final fault-free fetch to EOF. The stored transaction sequence must equal the source sequence, starting from the first stored transaction. How the stored sequence is read depends on the mode:
  - plain position mode: `check_complete_copy`;
  - GTID and rewrite modes: GTIDs parsed from the bytes, not from `.json`;
  - encrypted storages: read through pull.

  Each mode gets its own message.
- **SUT-side twins:** two `ALWAYS` checks with distinct messages: a block site in `storage_core::write_event_block` (block GTIDs disjoint from stored GTIDs; the only placement that catches ticket 18) and a receive site in `storage::write_event` (GTID not already stored or buffered). Revised by investigation; see the instrumentation table.
- **Coverage hint:** `Sometimes(kill_count >= 3 before convergence)`.
- **Known exception:** ticket 14 (GTID resume skips rotations). The oracle must filter it or track it separately.
- **Related properties, kept separate:**
  - `failed-flush-never-reappends-data`: the root-cause guard for one route to a duplicate.
  - `inprocess-reconnect-resumes-without-dup-or-gap`: the no-kill reconnect path.

## What led to it

- **SG1, the core idempotency claim.** README:711 promises "resuming streaming from the position at which the previous run finished". Every reconnect re-requests events from the source, and so does every restart. Reconnects come from pull's idle loop. Restarts come from the fetch cron or from a supervised pull after a kill. Each one is a chance to replay already-stored transactions or to skip some.
- **SG2/SG4, byte-prefix fidelity and recovery by truncation.** The recovery design truncates the tail to the metadata size. Resume reads only the tail metadata (`storage_core.hpp:313-337`):
  - position mode uses the name + `size`;
  - GTID mode uses `previous_gtids ∪ added_gtids`.

  Correctness therefore depends on metadata and data agreeing at a transaction boundary after *every* kill point.
- **Known violations.**
  - Ticket 18 (validated) is a violation of this property.
  - Ticket 14 (validated) is also a violation, but of the binlog-structure half only. It does not change the transaction sequence.
  - N2 is a cross-process trigger for ticket 18.
- **Existing QA coverage.** `extra/qa/fetch_crash_test.py` already checks this black-box with Hypothesis: it sends SIGKILL by polling the file size, then runs `check_complete_copy()` at `:636-697`. Antithesis adds:
  - arbitrary kill timing;
  - network and S3 faults between restarts;
  - kills during recovery itself.
- **Rewrite mode (focus 9).** In plain GTID mode, the reader's position-continuity check (`reader_context.cpp:908-921`) is the only thing that prevents a replayed transaction from being appended. It works only because the stored bytes carry the source's `next_event_position`. In rewrite mode, every event is re-stamped with `storage_->get_current_position()` (`collector_context.cpp:422-428`) before the reader sees it, so the check is tautological.
- **GTID mode (focus 9).** A transaction that the source sends twice, or that PBS re-requests because the buffer and the requested set disagree, would be appended silently. The `added_gtids` metadata would not reveal it, because `gtid_set +=` is idempotent (`storage_core.cpp:311-316`).

## Code paths

- **Resume request:** `collector_context.cpp:218-272` (also cited as `:207-272`).
  - Position mode calls `switch_to_position_replication(tail name, storage::get_current_position())`, where `get_current_position = flushed + event_buffer_.size()` (`storage.hpp:81-83`).
  - GTID mode calls `storage_core::get_gtids_unsafe()`. It returns the tail's `previous ∪ added`, which covers **flushed data only** (`storage_core.hpp:313-333`, `:313-337`). On an empty storage, `purged_gtids_` is used instead.
  - The reader starts at `storage_->get_current_position()` (`collector_context.cpp:139-143` / `:142-146`).
- **Timeout disconnect:** `discard_incomplete_transaction_events()`, then `flush_event_buffer()` (`collector_context.cpp:195-200`; the disconnect handling as a whole is `:189-204`). This leaves the buffer empty before the reconnect. It is what keeps a mid-transaction disconnect from duplicating the re-sent transaction.
- **Checkpoint write:** `storage::flush_event_buffer_internal` (`storage.cpp:276-302`) calls `storage_core::write_event_block` (`storage_core.cpp:299-322`). Inside, the steps are:
  1. append the data;
  2. `size +=`;
  3. update GTIDs and timestamps (`*optional_added_gtids += block_gtids`, `:311-316`);
  4. `save_binlog_metadata`.

  Encrypted appends reuse the envelope with the offset (`storage_core.cpp:308`).
- **Ticket 18 mechanism.**
  - Sites: `storage_core.cpp:299-322`, `storage.cpp:276-302`, `storage.cpp:68-77`.
  - If the metadata PUT throws, the facade does not erase the buffer, because the erase happens after the call returns.
  - `~storage` then flushes again. It appends the same n bytes at offset old+n and saves `size = old+2n`, which is a permanent duplicate.
  - On the filesystem, sticky failbits may make the retry fail instead. On S3, the retry succeeds.
- **N2:** `remove_temporary_objects` runs in any streaming or purging constructor and deletes every `*.tmp` (`storage_core.cpp:597-630`). That includes a live writer's in-flight `<name>.json.tmp`, so the writer's rename fails after its data was appended. This reaches the ticket 18 path through a concurrent purge.
- **Ticket 14:** the GTID-mode name-mismatch branch only calls `close_binlog()` (`collector_context.cpp:541-553`), and the source skips binlogs that contain no transactions.
- **Duplicate and gap guards:**
  - the artificial ROTATE position check, position mode only (`collector_context.cpp:524-532`, `reader_context.cpp:547-556`);
  - `validate_position` on the first GTID event after a resume (`reader_context.cpp:790, 908-921`).

  `validate_position` applies in **both** modes. The reason is that `position_` is seeded from the storage position, and preamble events after a same-file resume are info-only (`reader_context.cpp:579-582`).
- **Reopen:**
  - `storage_core::open_binlog` (`storage_core.cpp:257-297`) refuses an existing name that is not the tail (`:270-283`).
  - `open_existing_binlog_file_internal` (`:704-719`).
  - Tail recovery truncates to the metadata size (`storage_core.cpp:960-985`, `:960-984`, truncation at `:980`).
- **Rewrite mode:** `collector_context.cpp:274-429`.
  - Positions are recomputed, so the reader checks PBS's own positions and the guards above are ineffective.
  - After a reconnect, a fresh context regenerates the artificial ROTATE/FDE/PGTIDS for the current file (`collector_context.cpp:352-420`). These are info-only because `position_ > 4`. After that, every source transaction is accepted.
- **No membership check on write:** `storage::write_event` (`storage.cpp:126-200`) adds `transaction_gtid` to `gtids_in_event_buffer_` without checking whether `core_->get_gtids()` already contains it.

## Failure scenarios

1. **[Ticket 18, S3]** An S3 5xx on the `.json` PUT of a checkpoint (via `s3_fault_proxy.py`), followed by SIGTERM or an exception. The destructor's re-flush duplicates the block. In GTID or rewrite mode the duplicate is silent.
2. **[N2, filesystem; out of contract per README:473, excluded from scored runs]** A purge that starts while fetch or pull is mid-checkpoint on the filesystem backend deletes `.json.tmp`. The result is the same as scenario 1.
3. **[GTID reconnect with a non-empty buffer]** See `inprocess-reconnect-resumes-without-dup-or-gap` / `inprocess-reconnect-resumes-without-dup-or-gap`.
4. **[Kill between `size +=` and the metadata save]** Recovery truncates the tail, and the next run re-fetches from the metadata position. This is expected to be lossless as long as the source still has the data. A violation would show up as one of:
   - a duplicate transaction (metadata ahead of the data that survived);
   - a gap (metadata claims more GTIDs than the bytes contain);
   - a stored binlog that stops being a byte prefix of the source.
5. **[Kill or partition around reconnects, GTID mode]** If `added_gtids` is ever saved ahead of the bytes, or behind them, the source resends or skips transactions.
6. **[Mid-transaction disconnect]** The discard drops too little or too much.
7. **[Encrypted storage]** A wrong counter offset after a resume would corrupt the plaintext rather than duplicate it. The same oracle still detects it when it reads through pull.
8. **[Source re-sends a transaction]** For example, after failover to a server with an overlapping executed set (ticket 25). Only the SUT-side GTID guard catches this in rewrite mode.

**Known exception (ticket 14):** a GTID-mode resume across a source rotation drops the old file's terminal ROTATE and skips empty binlogs. This does not change the *transaction* sequence, so the transaction oracle is independent of it. The structural part is tracked by `gtid-resume-preserves-source-rotations` (formerly `gtid-resume-no-skipped-binlogs`). The byte-identity check, however, must filter it or track it separately, or it will flag constantly.

## Oracle / workload checks

**Workload loop.** Repeat a random number of times:
1. run the source workload;
2. start the writer;
3. kill it after a random delay;
4. optionally inject network or S3 faults.

**Convergence step.** Stop faults (`ANTITHESIS_STOP_FAULTS`, or an `eventually_` command). Stop source writes, then run one final fault-free `fetch` to EOF, or let pull catch up. Then read the stored stream:
- plain storages: read the bytes directly;
- encrypted or rewrite storages: read the pull stream with artificial events stripped.

**Checks.** Each is an `Always` evaluated in the terminal check, with its own message:

| Mode | Check |
|---|---|
| Plain position mode | `Always("stored position-mode binlogs equal the source binlogs")`. Uses `check_complete_copy()`: byte identity, IN_USE cleared, magic-only tail allowed. Transactions are identified by their `(file, end offset)` boundaries and compared against `MySQLServer.binlog_as_sent()` from `extra/qa/fetch_crash_test.py`. |
| GTID mode | `Always("stored GTID sequence equals source sequence")`. Parse every stored binlog over `[0, size)` and collect the GTID events in index order. No GTID may appear twice. The union must equal the source's `gtid_executed`, restricted to GTIDs committed in binlogs from the storage's first source binlog onward. For an in-progress pull, the expected set is a prefix of the source's commit order. After `fetch` exits 0, it is everything committed before the fetch began. |
| Rewrite mode | `Always("rewrite-mode stored GTID sequence equals source sequence")`. GTID sequence plus per-transaction payloads, read through the bytes or through pull. |
| Encrypted storages | `Always("encrypted storage transaction sequence via pull equals source sequence")`. |

**Position mode without GTIDs.** An alternative oracle: each workload transaction inserts a unique id, and the workload decodes the stored row events. It can also rely on `stored-binlog-byte-prefix-of-source`, which already catches duplicates as a byte mismatch.

**Parse the bytes, not the `.json`.** `added_gtids` is a set, so a duplicate in the bytes is invisible in the metadata.

**Coverage hints:**
- `Sometimes("kill_count >= 3 before convergence check")`.
- `Sometimes("a resume happened between two runs with at least one transaction in between")`. This keeps the check from being vacuous.
- The workload should log when a purge overlaps a writer. That overlap cannot be detected in-process.

## SUT-side instrumentation suggestions

The codebase has no Antithesis SDK, so every SDK assertion below is **missing**.

| Status | Assertion | Site | Origin |
|---|---|---|---|
| missing (primary in-process twin, block site) | `ALWAYS(!intersects(core gtids, block_gtids), "flushed block GTIDs are disjoint from GTIDs already stored")` | `storage_core::write_event_block` (`storage_core.cpp:298-322`), under the lock, **before** `write_data_to_stream`; "core gtids" = `get_gtids_unsafe()` (tail `previous_gtids ∪ added_gtids`, or `purged_gtids_` when empty, `storage_core.hpp:313-333`). GTID and rewrite modes only (rewrite requires GTID mode, `replication_config.cpp:32`). | **Changed by investigation (2026-10-13).** This is the only one of the merged placements that catches ticket 18: the destructor re-flush (`storage.cpp:68-77`) calls `write_event_block` again with the same `block_gtids` and never calls `storage::write_event`, while the failed first call already did `*added_gtids += block_gtids` (`storage_core.cpp:311-316`) before `save_binlog_metadata` threw. It overlaps `failed-flush-never-reappends-data`'s root-cause UNREACHABLE on that route but also covers any other re-append of a stored block. `gtid_set` provides `contains(const gtid&)` (`gtid_set.hpp:66`) and `intersects(a, b)` (`gtid_set.hpp:107,137`), so no new API is needed. The `binlog-metadata-matches-stored-bytes` reference to the `block_gtids ∩ added_gtids == ∅` form points here. |
| missing (in-process twin, receive site) | `ALWAYS(!core_->get_gtids().contains(gtid) && !gtids_in_event_buffer_.contains(gtid), "received transaction GTID is not already stored or buffered")` | `storage::write_event` (`storage.cpp:126-200`) when `at_transaction_boundary && !transaction_gtid.is_empty()` | Catches a source re-send or a reconnect replay (scenarios 3, 5, 8) at the instant the GTID enters the buffer, including in rewrite mode. It does **not** catch ticket 18 (see the block-site row), so the two sites are distinct sub-checks with distinct messages, not alternatives. Merged from no-gtid-written-twice and restart-after-kill's `collector_context` proposal (same condition). With the harness's `gtid_mode=ON` source, position-mode transactions also carry GTIDs, so the condition need not be restricted to GTID replication mode if `transaction_gtid` is populated there; in GTID replication mode anonymous transactions cannot reach this site (see Investigation Log). |
| missing | `SOMETIMES("storage destructor flush threw")` / `UNREACHABLE("storage destructor flush after a failed flush")` | `storage.cpp:68-77` | Proposed by source-transaction-stored-exactly-once. Owned by `failed-flush-never-reappends-data` (root-cause UNREACHABLE) and `recovery-paths-exercised` (`REACHABLE("storage destructor swallowed a flush exception")` at `storage.cpp:74`). Reference them, do not duplicate them. |
| missing | `REACHABLE("tail binlog truncated to metadata size on startup")` | `storage_core.cpp:980` | Proposed by restart-after-kill. Also anchored by `recovery-paths-exercised` and `kill-anywhere-storage-reopens`. Reference the generic anchor. |

## Fault requirements

- SIGKILL of fetch/pull from the workload at random times, including during recovery.
- PBS↔source partitions, resets and timeouts.
- PBS↔S3 faults: network faults, or `s3_fault_proxy.py` for 5xx on `.json` PUTs.
- No overlapping purge: concurrent purge is out of contract (README:473), so N2 is excluded here.
- Faults stop before the convergence fetch.

## Open questions

- **Byte identity.** Is byte identity an official guarantee, or only an MTR convention? (File-level open question in sut-analysis.) If it is only a convention, the oracle relaxes to "same transactions in the same order", and ticket 14 drops out as a violation. `(needs human input)`

## Answers recorded by investigation (2026-10-13)

- **Lost tail re-fetch:** in contract (README:721, "kill -9 … may result in losing some progress"; README:711 resume). If the source purged the needed data, the dump fails with a non-`CR_SERVER_LOST` error, which `connection::fetch_binlog_event` raises as `core_error` (`easymysql/connection.cpp:544-550`); fetch rethrows (`fetch_operation.cpp:107-109`), pull rethrows out of its idle loop (`pull_operation.cpp:131-160`), so the writer exits non-zero rather than storing a gap. The oracle excludes runs where the source purged past PBS's position; the planned harness sets `binlog_expire_logs_seconds=0` and the workload controls source purges (`deployment-topology.md:67`), so such runs are explicit and taggable.
- **Encrypted oracle:** reading through pull is acceptable only with events below 16,777,211 bytes; ticket 10's mechanism is confirmed in code (`minimysql/network_io_operations.cpp:144-147` rejects any frame ≥ `max_payload_size`). Workloads on encrypted storages cap event size below that, or decrypt offline with the keyring per `encrypted-storage-decrypts-to-source`.
- **Ticket 18 fix direction:** irrelevant to this property's placement. The block-site twin runs before the append and checks stored GTIDs, so it holds under either fix ("erase on failure" or the ticket's own suggestion, an all-or-nothing `write_event_block` that bumps size only after the metadata save). The fix-direction question stays with `failed-flush-never-reappends-data`.
- **Purge concurrent with a writer:** out of contract. README:473 says it "may cause data corruption and should be avoided". N2 (scenario 2) is excluded from this property's oracle; the workload must not overlap purge with writers in runs scored by this property. N2 remains tracked by its own properties (`startup-tmp-cleanup-spares-live-writer`). Scenarios 1 and 5 remain.
- **Binlog compression:** no parser change needed for the GTID-sequence oracle: the GTID event precedes and sits outside the `TRANSACTION_PAYLOAD_EVENT` (PBS's reader treats the payload as an ordinary in-transaction event, `reader_context.cpp:171,223,768-794`). Only the "decode unique row ids" alternative oracle would need to decompress the payload. QA already exercises compression (`extra/qa/event_variety_test.py:25,72-73`).
- **`sync_binlog=1`:** the planned harness sets `sync_binlog=1` and `innodb_flush_log_at_trx_commit=1` on the source (`deployment-topology.md:67`). No harness exists yet, so the workload/harness builder must keep it.
- **`gtid_set::contains`:** exists (`gtid_set.hpp:66`), as does `intersects` (`gtid_set.hpp:107,137`).
- **Anonymous transactions:** PBS's GTID replication mode refuses them: `reader_context.cpp:783-787` raises "unexpected anonymous gtid log event in gtid mode replication", and the dump is requested with `MYSQL_RPL_GTID` (`easymysql/connection.cpp:203-205`). So the SUT twins never silently skip an anonymous transaction in GTID/rewrite mode; such a source is a fail-stop. In position mode against a `GTID_MODE=OFF` source the twins are vacuous and the byte oracle (`check_complete_copy`) is the only check.

## Property changes from investigation

- Invariant, assertion type, priority unchanged (Safety, P0, workload `Always` at convergence).
- SUT-side twin split into two sites with two messages (block site in `write_event_block`, receive site in `write_event`). The block site is the one that catches ticket 18; the previously "primary" `write_event` placement does not.
- Scope narrowed: concurrent purge (N2) is out of contract and excluded from this property's scored runs; runs where the source purges past PBS's resume point are excluded (expected non-zero exit).

### Investigation Log

#### Is losing an un-checkpointed tail and re-fetching it allowed; what if the source purged it?

- Examined: README:711-721; `easymysql/connection.cpp:537-554`; `operations/collector_context.cpp:124-272`; `fetch_operation.cpp:88-123`; `pull_operation.cpp:120-175`; `deployment-topology.md:67`.
- Found: README allows losing progress on kill -9 and promises resume. A dump error other than `CR_SERVER_LOST` raises `core_error`, which propagates out of fetch and pull (non-zero exit). Harness plan disables source auto-purge.
- Not found: nothing missing for this property.
- Conclusion: resolved. Exclude runs where the workload purged the source past PBS's position.

#### Should the encrypted oracle compare only the pull output; ticket 10?

- Examined: ticket 10 text; `minimysql/network_io_operations.cpp:133-147`, `network_io_operations_fwd.hpp:27`; `connection_context.cpp` `generate_encoded_binlog_event`.
- Found: `async_write_mysql_frame` throws for payloads ≥ `max_payload_size`, confirming ticket 10's mechanism from code (not just the report).
- Conclusion: resolved. Pull-based oracle valid with events < ~16 MiB; otherwise decrypt offline (`encrypted-storage-decrypts-to-source`).

#### Ticket 18 fix direction and instrumentation placement

- Examined: `storage_core.cpp:298-322`, `storage.cpp:126-200, 270-302, 68-77`; ticket 18 "Suggested fix" (`.tickets-snapshot.txt:1006-1008`).
- Found: the re-flush path re-enters `write_event_block` only; `write_event` is never re-called for the same transaction, so a `write_event`-site GTID check cannot see ticket 18. `added_gtids` is bumped in memory before the metadata save, so a pre-append disjointness check in `write_event_block` fires on the retry. That check is independent of the fix.
- Conclusion: resolved for this property; instrumentation changed (two sites). Fix intent remains with `failed-flush-never-reappends-data`.

#### Is purge concurrent with a writer in contract?

- Examined: README:473.
- Found: "may cause data corruption and should be avoided".
- Conclusion: resolved, out of contract. N2 excluded from this property.

#### Does the oracle parser need TRANSACTION_PAYLOAD_EVENT handling?

- Examined: `reader_context.cpp:165-171, 217-223, 768-794`; `extra/qa/event_variety_test.py`.
- Found: the transaction group still starts with a GTID event; the payload event is an ordinary member event. GTID scans need no decompression.
- Conclusion: resolved. Only a row-decoding oracle would need it.

#### Is byte identity an official guarantee?

- Examined: README (no "identical"/"byte" guarantee wording found by grep; only README:711 "resuming streaming from the position at which the previous run finished"); sut-analysis.md:314; QA `check_complete_copy` (`extra/qa/fetch_crash_test.py:671-697`) enforces byte identity as test convention.
- Not found: any maintainer statement.
- Conclusion: `(needs human input)`.

#### Does the harness set `sync_binlog=1`?

- Examined: `deployment-topology.md:46,67`; `extra/qa/fetch_crash_test.py:140-151` (QA does not set it).
- Found: the planned harness `my.cnf` sets `sync_binlog=1` and `innodb_flush_log_at_trx_commit=1`.
- Conclusion: resolved (harness requirement, recorded in topology).

#### Is `gtid_set::contains(gtid)` available?

- Examined: `src/binsrv/gtids/gtid_set.hpp`.
- Found: `contains(const gtid&)` at :66; `intersects` at :107/:137.
- Conclusion: resolved.

#### Does PBS GTID mode require GTID mode on the source (anonymous transactions)?

- Examined: `reader_context.cpp:776-787, 936-950`; `easymysql/connection.cpp:203-205`.
- Found: anonymous GTID events raise a logic_error in GTID replication mode.
- Conclusion: resolved; the SUT twins cannot silently skip transactions in GTID/rewrite mode.

## Evaluation refinements (2026-10-13)

Applied by the evaluation synthesis (`../evaluation/synthesis.md`). Where this section and earlier text in this file disagree, this section wins; the catalog entry has been updated to match.

- Encrypted storages: the independent Python decryptor is the canonical encrypted reader. Pull is not used to read a storage it does not own: pull always builds a streaming storage and runs the collector (`pull_operation.cpp:108-153`), so on a `fetch_fs` or `s3` storage it would run recovery on, and append to, the storage under test. A pull readback there runs only at quiescent points on a snapshot copy with `connection.host` pointing at an unreachable endpoint (lenses: implementability P1, wildcard W2).
- Ground truth is the source's own binlog bytes and `gtid_executed`; the workload ledger is only a cross-check, because a commit whose acknowledgement was lost is ambiguous in a client ledger (wildcard W13).
- The `Always` is evaluated only when the final fetch reaches exit 0. A final fetch that never does is the fetch-convergence liveness gap G4, being filled by targeted discovery (coverage-balance F4).
- Runs in `fetch_fs`, `pull_fs`, `s3`.
