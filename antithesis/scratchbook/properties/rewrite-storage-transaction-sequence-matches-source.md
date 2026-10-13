# rewrite-storage-transaction-sequence-matches-source

## What led to it

In rewrite mode no byte oracle applies (SG2 explicitly excludes it), and comparing the pull stream to the source does not work either, because the files are re-split and every event is re-stamped. Existing rewrite properties check **internal** validity: `rewrite-binlog-chain-valid-across-crashes` (positions, CRCs, sequence numbers), `stored-transactions-exactly-once-across-resumes` (duplicates), `rewrite-binlog-chain-valid-across-crashes`, `stored-rotate-or-stop-is-last-event`. None of them asserts the thing a backup must guarantee, **semantic equivalence with the source**: the same transactions, in the same order, with identical payloads and none missing. An internally valid rewritten storage can still have dropped transactions. Resume in rewrite mode is by GTID set only (`collector_context.cpp:227-251`); it never checks that the transactions skipped by the source are exactly the ones stored. It is also crash-untested (sut-analysis surface 8).

## Code paths

- Rewrite pipeline: `collector_context.cpp:274-429`. Source ROTATE/FDE/PREVIOUS_GTIDS/STOP are dropped before the reader sees them (`:290-334`). Preamble generation happens at fresh/boundary+size (`:352-420`), and each event is rewritten at `:422-428` (`rewriter::rewrite`, `events/rewriter.cpp:130-220`).
- Resume: the GTID set is tail `previous_gtids ∪ added_gtids` (`storage_core.hpp:313-337`), and `last_sequence_number` comes from metadata (`storage.cpp:119-131`).
- Transaction boundaries come from `transaction_length` in GTID events (`reader_context.hpp:74-78`, `reader_context.cpp:999-1019`). The rewriter must keep event sizes unchanged within a transaction, which is why checksums are enforced (`collector_context.cpp:296-322`).

## Oracle

Workload, at quiescence: parse the source's binlogs and the rewritten storage (directly, or through pull for encrypted storages) into transaction sequences. Each element is `(GTID, [event type + body bytes excluding common header, footer and the GTID event's logical-clock and length fields])`.
- `ALWAYS(rewritten sequence == source sequence restricted to GTIDs after the storage's initial purged set)`.
- `ALWAYS(rewritten logical clock is valid: for every GTID event 0 <= last_committed < sequence_number and sequence_number strictly increasing within a file)`. This partially overlaps the chain-valid sibling, so keep the message distinct.

Comparing body bytes, not just GTIDs, catches a rewriter bug that alters payloads, such as an offset slip in `rewrite_gtid_tagged_log_internal` or the re-checksum masking corruption (W6).

## Antithesis angle

Kill PBS at arbitrary points across rewritten-file creation and checkpoints. Partition the source mid-transaction. Use small `file_size` (1024-4096) so that local rotations are frequent and interleave with source rotations (`FLUSH BINARY LOGS`), and mix tagged GTIDs (≥ 8.4 source) and large transactions.

## Instrumentation suggestions

- Workload (missing): the two ALWAYS checks above.
- SUT-side (missing): in `rewrite_and_process_binlog_event`, `ALWAYS(rewritten_event.size == source_event.size, "rewrite preserves event size within a transaction")`. The transaction-length logic relies on this (comment at `:303-318`).
- SUT-side (missing): `SOMETIMES(context.is_fresh() && !storage_->is_empty() && storage_->get_current_position() > magic, "rewrite resumed into a non-empty rewritten file")` as an exploration anchor.

## Open questions

- None

## Investigation results (2026-10-13)

- **A mid-transaction resume cannot diverge silently.** A new `reader_context` is built per connection (`collector_context.cpp:142-146`). After the preamble it is in `gtid_log_expected`, and any first event other than GTID_LOG, GTID_TAGGED_LOG or ANONYMOUS_GTID_LOG raises `"transaction group must start from one of the gtid log events"` (`reader_context.cpp:748-790`). Before reconnecting, PBS discards the partial transaction from its buffer and flushes the complete transactions (`collector_context.cpp:193-200`; `storage.cpp:217-223`), and the GTID set it sends is built only from complete, flushed transactions (`collector_context.cpp:228-251`, `storage_->get_gtids()`). If a server version ever resumed mid-transaction, PBS would stop with an error (a liveness failure), not store a diverged sequence. The oracle needs no special case. A run that stops this way shows up in the liveness properties.
- **A source transaction is never split across rewritten files.** The rotation condition is `context.is_at_transaction_boundary() && storage_->get_current_position() >= file_size` (`collector_context.cpp:352-353`), and the code comment states the intent: "the storage is not empty, we are at transaction boundary, and current binlog file reached the file size" (`collector_context.cpp:344-349`). The fresh-reconnect branch reuses the current file and does not rotate (`:367-370`). The oracle asserts "never split" as an `Always`, with the message "no source transaction spans two rewritten files". The README (`:593`) does not state the boundary rule, but the code's intent is explicit, so this needs no human input.
- **Corrections to the instrumentation and the oracle (property refined):**
  - The suggested SUT assertion `ALWAYS(rewritten_event.size == source_event.size, "rewrite preserves event size within a transaction")` is **wrong for GTID_TAGGED_LOG**. `rewrite_gtid_tagged_log_internal` re-encodes `sequence_number`, `last_committed` and `transaction_length` as varints, so the GTID event's own size can change, and `transaction_length` is recomputed to match (`rewriter.cpp:228-328`). Scope the assertion to **non-GTID events**: `ALWAYS(code is not a GTID event ⇒ rewritten size == source size, "rewrite preserves the size of non-GTID events")`. That is the condition the transaction-length logic actually relies on (`collector_context.cpp:296-322`).
  - In the oracle's transaction element, a tagged GTID event cannot be compared as "body bytes excluding the logical-clock and length fields", because those fields are varint-encoded inside the body. Decode it and compare the fields (UUID, tag, GNO, commit timestamps, server versions, flags), excluding `sequence_number`, `last_committed` and `transaction_length`. For GTID_LOG the post-header fields sit at fixed offsets and a byte mask works.
  - Common-header timestamps are copied unchanged from the source (`rewriter.cpp:304-306` copies the common header; the GTID_LOG path changes only size and next position), so the oracle can include the timestamp in the comparison. That strengthens the check.
  - The logical-clock check can be tightened to the exact source-derived value (see `rewrite-binlog-chain-valid-across-crashes`, Investigation results). Keep this property's message about validity distinct, or drop it in favour of the sibling's equality check, to avoid duplicated messages.

### Investigation Log

#### Does the source re-send a partially received transaction in full after a GTID-mode reconnect, in every MySQL version tested?

- Examined: `collector_context.cpp:120-170` (a reader per connection), `:207-251` (the GTID set sent); `storage.cpp:143-223` (buffering and discard of the incomplete transaction); `reader_context.cpp:748-790` (the first transaction event must be a GTID event).
- Found: PBS only requests from complete transactions, and it rejects any stream that does not start with a GTID event after the preamble. The server-side behaviour (whole transactions in COM_BINLOG_DUMP_GTID) was not verified per version. MySQL server source was not checked, only mysqlbinlog sources are local. That check is not needed, because PBS fails closed.
- Not found: per-version server traces.
- Conclusion: resolved for the property. Divergence would surface as an error and a stall, not as a wrong stored sequence. No oracle change is needed.

#### Should the oracle allow a source transaction to be split across two rewritten files?

- Examined: `collector_context.cpp:334-370` (comment and rotation condition), README:592-595.
- Found: rotation happens only at transaction boundaries, the intent is stated in the code comment, and the reconnect path does not rotate.
- Not found: a README statement (not needed).
- Conclusion: resolved. Assert "never split" (`Always`).
