# rewrite-binlog-chain-valid-across-crashes — Rewritten binlogs stay internally valid across crashes and source upgrades

Found by: focus 1, 10 (data integrity; version compat)
Merged from: rewrite-binlog-chain-valid-across-crashes (focus 1), rewrite-file-fde-describes-events (focus 10)

## Synthesis notes

Safety, P1 (focus 1 rated P1, focus 10 rated P2). Rewrite output cannot be compared byte-for-byte with the source, so internal validity is its integrity check: magic, FDE, PREVIOUS_GTIDS equal to `.json` `previous_gtids`, the `next_event_position` chain, CRCs, `sequence_number` 1..k contiguous with `.json` `last_sequence_number = k`, matching `transaction_length`, and every event type / post-header length described by the file's own FDE.

Workload `Always` messages: "rewritten binlog has a valid position chain and CRCs", "rewritten sequence numbers are contiguous and match last_sequence_number", "every event in a rewritten file is described by that file's FDE", plus a `mysqlbinlog --verify-binlog-checksum` pass.

SUT-side: `ALWAYS(rewritten sequence_number == last + 1)`, `ALWAYS_OR_UNREACHABLE(event type code known in current file's FDE version)` before writing a non-preamble event in `rewrite_and_process_binlog_event`, and `SOMETIMES(rewrite resumed into non-empty file after restart)`.

The FDE variant needs a source upgrade: generated FDEs use the connection version from file creation (`event_generation_helpers.cpp:120-131`), and source FDEs are skipped before the reader sees them (`collector_context.cpp:290-334`). Payload equality is a separate property (rewrite-storage-transaction-sequence-matches-source).

## What led here

- In rewrite mode PBS regenerates ROTATE/FDE/PREVIOUS_GTIDS and rewrites every event's `next_event_position`, CRC, and the GTID event's `sequence_number`/`last_committed`/`transaction_length` (`collector_context.cpp:274-429`, `events/rewriter`). The byte-prefix oracle does not apply, so the only integrity check is internal: each stored file must be a valid, self-consistent binlog.
- MTR covers one disconnect (`mtr/binlog_streaming/t/gtid_renumbering_resume_after_partial.test`, contiguous seq 1,2,3) and local rotation, but never a kill at an arbitrary instruction or an S3 fault. SUT analysis notes rewrite has never been crash-tested (surface 8).
- (focus 10) The source's ROTATE/FDE/PREVIOUS_GTIDS/STOP are **skipped before** `context.process_event_view` (`collector_context.cpp:290-334`, an early `return`). The reader_context therefore never adopts the source FDE: `current_encoded_server_version_` and `post_header_lengths_` stay at their constructor values, which come from `connection.get_server_version()` (`reader_context.cpp:36-53`, `collector_context.cpp:141-146`).
- (focus 10) The generated FDE for a new rewritten file uses `context.get_current_encoded_server_version()` (`event_generation_helpers.cpp:120-131`), i.e. the version of the **server PBS was connected to when the file was created**, with the hard-coded post-header table for that version (`event_generation_helpers.cpp:87-95`).
- (focus 10) On reconnect to a non-empty storage, rewrite reuses the open rewritten file: `is_fresh()` → `binlog_name = storage_->get_current_binlog_name()`, the generated artificial ROTATE matches the stored name and position, `expect_info_only_preamble_events_` becomes true (`reader_context.cpp:579-582`), and the newly generated FDE is info-only, so it is not written.

## Code paths

- Sequence number seed on resume: `storage_->get_last_transaction_sequence_number()` (`collector_context.cpp:424-427`), persisted as `last_sequence_number` in the same `.json` PUT as `size` (`storage_core.cpp:317-321`). Reset to 0 for a newly created file (`storage.cpp:120-131`).
- Rewriter uses `storage_->get_current_position()` (buffer-inclusive) as the base for `next_event_position`.
- Preamble generation on empty storage / rotation / reconnect (`collector_context.cpp:352-420`).
- Checksums are mandatory in rewrite (`collector_context.cpp:296-322`).
- Source-event skip before the reader context: `collector_context.cpp:290-334`; reader version/post-header init: `reader_context.cpp:36-53`, `collector_context.cpp:141-146`.
- FDE generation: `event_generation_helpers.cpp:120-131` (version), `:87-95` (post-header table).
- Info-only preamble on reuse of the open file: `reader_context.cpp:579-582`.
- Rewrite config (`base_file_name`, `file_size`) is not recorded in the storage (`storage_core.cpp:274-278`, sut-analysis N11).

## Failure scenarios

- **Crash trigger** — kill between a checkpoint's data append and its `.json`: recovery truncates to the old size and old `last_sequence_number`, so re-sent transactions get the same numbers (expected to hold; Antithesis checks every window).
- **Disconnect trigger** — `discard_incomplete_transaction_events` restores `incomplete_transaction_last_sequence_number_` but the renumberer's own state is in the rewriter. The MTR test above guards one shape of this; Antithesis explores others (disconnect during a multi-event transaction exactly at a checkpoint).
- **Source-upgrade trigger (focus 10)** — a rewritten file created while connected to an 8.0 source (42-entry table, version 8.0.x) keeps that FDE when the source is upgraded in place to 8.4/9.7 and PBS reconnects. If the upgraded source then produces GTID_TAGGED_LOG (type 42, ≥ 8.3 only), that event lands in a file whose FDE does not describe type 42. mysqlbinlog and PBS's own sender pass the bytes through, but a downstream parser using the FDE table can reject the file.
- **Source-upgrade, benign side (focus 10)** — the reader_context in rewrite mode validates the post-header lengths of 8.0-era events against the 8.4 table when it resumes into old 8.0 source files after the upgrade. For shared event types these are identical, so this is benign as far as checked.
- `stored-rotate-or-stop-is-last-event` produces a file that is chain-valid in positions but has a ROTATE mid-file; this property should not double-report it (keep the ROTATE-last check there, stored-rotate-or-stop-is-last-event).
- Toggling `base_file_name` between runs: undetected (N11); deterministic config, low Antithesis value.
- 999,999 sequence limit reachable in ~17 min with tiny `file_size` (N11); exercise via small `file_size`.

## Oracle (workload, per stored binlog `[0, size)`)

1. Starts with magic, then FDE (with checksum alg), then PREVIOUS_GTIDS whose body equals `.json` `previous_gtids`.
2. For each event: `next_event_position == offset + event_size`; CRC32 valid. → `Always("rewritten binlog has a valid position chain and CRCs")`.
3. GTID events: `sequence_number` is 1, 2, 3, … contiguous within the file; `last_committed < sequence_number`; `.json` `last_sequence_number` equals the last GTID's `sequence_number` (0 if none). → `Always("rewritten sequence numbers are contiguous and match last_sequence_number")`.
4. `transaction_length` of each GTID event equals the byte length of the transaction.
5. (focus 10) Parse each stored rewritten file using only its own FDE: every event type code ≤ `number_of_events` in that FDE and the post-header length matches. → `Always("every event in a rewritten file is described by that file's FDE")`. Companion: `Always("rewritten file FDE server version covers every GTID_TAGGED_LOG it contains")` — tagged events appear only in files whose FDE version is ≥ 8.3.
6. Across files, the concatenation of transactions (GTID + body, ignoring rewritten header fields) equals the source's transactions in commit order, each exactly once (owned by rewrite-storage-transaction-sequence-matches-source / stored-transactions-exactly-once-across-resumes; referenced, not duplicated here).
7. `mysqlbinlog --verify-binlog-checksum` reads every stored file without error. → `Always("mysqlbinlog verifies every rewritten binlog")`.

## SUT-side instrumentation suggestions

- `ALWAYS(rewritten sequence_number == last_sequence_number + 1, "rewriter assigns the next contiguous sequence_number")` inside the rewriter for GTID events — **missing**.
- `ALWAYS(last_sequence_number in .json equals ready_to_flush_last_sequence_number_ at flush, "flushed last_sequence_number matches the rewriter state")` — **missing**; mostly by construction.
- `ALWAYS_OR_UNREACHABLE(code is known in the FDE version of the current storage file, "rewritten event type is known to the current file's FDE")` in `rewrite_and_process_binlog_event` before writing a non-preamble event — **missing**; requires tracking the written FDE version per open file.
- `SOMETIMES(rewrite resumed into a non-empty file after restart, "rewrite resumed into a non-empty file after restart")` — **missing**.

## Key observations

- Confidence (focus 10 part): medium. The skip-before-context path and the info-only reuse are read from code. The client-side rejection (mysqlbinlog's reaction to an event type outside its FDE table) is not verified.
- The rewriter's base for `next_event_position` is buffer-inclusive, so positions depend on correct buffer discard on reconnect.

## Fault requirements

- Process kills of PBS at arbitrary instants; network faults on PBS<->source and PBS<->S3.
- Small `file_size` to drive rotation and the sequence limit.
- For the FDE variant: an in-place source upgrade (8.0 → 8.4/9.7) during the run, plus workload traffic that emits GTID_TAGGED_LOG on the upgraded source.

## Open questions

- Does rewrite mode support an in-place source upgrade at all, or is "start a new storage after a source upgrade" the expected procedure? If the latter, the FDE variant (check 5 and its companion) becomes a documentation item rather than a property. `(needs human input)`

## Investigation results (2026-10-13)

- **`last_committed` is derived, not copied.** `rewriter::fix_sequence_number_and_last_committed` (`rewriter.cpp:149-174`) keeps the source **delta** `d = src_seq - src_lc`. With new `seq = last_local + 1`, it sets `lc = seq - d` if `seq > d`, otherwise `lc = 0` when `seq == 1` and `1` otherwise. It throws if the source has `lc >= seq`. **Check 3 is strengthened** from "any `lc < seq`" to `Always(lc == expected_lc(seq, src_seq, src_lc))`, which requires pairing each rewritten GTID with its source GTID event (the sibling oracle already parses the source). The message stays "rewritten sequence numbers are contiguous and match last_sequence_number". Add a distinct one: "rewritten last_committed preserves the source dependency distance". The rule is parallel-apply safe as far as code reading shows. Commit order is preserved, so a window of `d` transactions covers the same predecessors. Clamping happens only when the window would cross a rewritten file start. A source file start (`src_lc = 0`, `d = 1`) becomes a full serialization point.
- **`transaction_length`: GTID_LOG copies it, GTID_TAGGED_LOG recomputes it.** For GTID_LOG and ANONYMOUS_GTID_LOG the fixer rewrites only `sequence_number` and `last_committed` in the fixed-size post-header (`rewriter.cpp:176-226`), so the event size and the body's `transaction_length` are unchanged. For GTID_TAGGED_LOG the varint re-encoding can change the GTID event's own size, so `transaction_length` is recomputed as `new_event_size + (original_length - original_event_size)` in a fixed-point loop (`rewriter.cpp:228-328`). In both cases, check 4 (`transaction_length` equals the rewritten transaction's byte length) depends on every **non-GTID** event keeping its size. Mandatory checksums guarantee that: `force_add_checksum` turns into `leave_checksum_as_is` when the source event already has a footer (`rewriter.cpp:84-87`, `collector_context.cpp:296-322`). Check 4 stays as written. It is the direct oracle for the tagged recomputation path, so tagged GTIDs (source ≥ 8.4) should be in the traffic mix.
- **Rewrite with encryption: include it.** The oracle should decrypt stored files **directly with the keyring** (file key unwrapped with the KEK, then AES-CTR from offset 0, as `encryption.test:199-237` does), rather than read through pull. That keeps failures attributable to the rewrite path rather than the sender. The checks are otherwise unchanged.
- **A reconnect always reuses the open rewritten file.** A new `reader_context` is built per connection (`collector_context.cpp:142-146`). On a non-empty storage the fresh branch names the **current** file (`:367-370`), and the rotation check is bypassed for that first transaction (`:352`, `is_fresh() ||`). Every reconnect therefore appends **at least one transaction** to the open file, even if it has already reached `file_size`. A rollover can happen only from the second transaction onward. **Consequence for the FDE variant:** after an in-place upgrade, the first transaction from the upgraded source always lands in a file whose FDE was generated with the old connection version (`event_generation_helpers.cpp:120-131` uses `context.get_current_encoded_server_version()`, which the new connection resets but which applies only to *newly generated* FDEs). The mixed-FDE file is the deterministic outcome of every upgrade-reconnect that carries traffic, not an occasional one. With tagged GTID traffic on ≥ 8.3, check 5 fires deterministically. The upgrade-support question decides whether that is a bug.
- **Side observation for size-based oracles:** a rewritten file can exceed `file_size` by one transaction normally, and by two after a reconnect. Do not assert `size <= file_size`.

### Investigation Log

#### Must `last_committed` be preserved from the source, or is any value < `sequence_number` acceptable?

- Examined: `src/binsrv/events/rewriter.cpp:130-226` (`rewrite`, `fix_sequence_number_and_last_committed`, `rewrite_gtid_log_internal`) and `:228-328` (tagged).
- Found: the code keeps the source delta, with a clamp at the rewritten file start. That gives a deterministic expected value.
- Not found: the maintainers' stated intent. The code's own comments define the rule, and that is enough for the oracle.
- Conclusion: resolved. The check is strengthened to equal the source-derived value.

#### Is `transaction_length` recomputed or copied?

- Examined: `rewriter.cpp:40-125` (materialize modes), `:176-226` (GTID_LOG fixer), `:228-328` (GTID_TAGGED_LOG recompute loop); `collector_context.cpp:296-322` (checksum enforcement).
- Found: GTID_LOG copies it, and GTID_TAGGED_LOG recomputes it. Both rely on non-GTID events keeping their size, which mandatory checksums guarantee.
- Not found: nothing missing.
- Conclusion: resolved. Check 4 stands, and tagged traffic is needed to exercise the recompute path.

#### Should rewrite with encryption be in the same property?

- Examined: `encryption.test:199-237` (offline decryption procedure), `storage_core.cpp:1125-1160`.
- Found: stored files can be decrypted offline from the keyring and `.json` envelope, so pull is not needed.
- Conclusion: resolved. Include it, and decrypt offline.

#### Does rewrite mode support a source upgrade at all?

- Examined: README:592-595 (rewrite section), the full README for "upgrade" or "version", the ticket snapshot, `collector_context.cpp:142-146`, and `event_generation_helpers.cpp:84-133`.
- Found: there is no documentation of upgrade behaviour and no server-version check against the stored files. The code silently mixes versions in the reused file.
- Not found: the owners' intent.
- Conclusion: `(needs human input)`.

#### Does a reconnect after a source restart always reuse the open rewritten file, or does a `file_size` rollover usually happen first?

- Examined: `collector_context.cpp:120-170`, `:352-429`; `reader_context.hpp:42` (`is_fresh` = `cycle_number_ == 0`); `reader_context.cpp:576-600`.
- Found: the file is always reused, and the first post-reconnect transaction is always appended to it before any rollover.
- Conclusion: resolved. The mixed-FDE file is guaranteed on every upgrade-reconnect that carries traffic.
