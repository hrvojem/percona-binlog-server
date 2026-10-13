# metadata-size-at-transaction-boundary

## What led here

SG3 and SG4: README:373/715/721 claim the storage is left consistent after errors and signals, and that `kill -9` may lose progress but not corrupt. The storage facade is designed to flush only up to the last transaction boundary (`storage.cpp:276-302`, `last_transaction_boundary_position_in_event_buffer_`). The QA oracle checks "metadata size is a transaction boundary of the server's file" (`fetch_crash_test.py:658-669`).

## Code paths

- `storage::write_event` records `last_transaction_boundary_position_in_event_buffer_` only when `at_transaction_boundary` (`src/binsrv/storage.cpp:143-170`).
- `storage::flush_event_buffer_internal` writes `[0, boundary)` (`storage.cpp:276-302`).
- `collector_context::receive_binlog_events` calls `discard_incomplete_transaction_events()` before the timeout flush (`collector_context.cpp:189-200`); on an exception the discard is skipped but `~storage` still flushes only to the boundary (`storage.cpp:68-77`).
- `storage_core::write_event_block` increments `size` by exactly the block length and saves it in the same `.json` PUT as GTIDs and `last_sequence_number` (`storage_core.cpp:299-322`).
- Startup: tail data longer than metadata is truncated to metadata size (`storage_core.cpp:979-983`).
- `reader_context::is_at_transaction_boundary()` decides what counts as a boundary (`reader_context.hpp:74`). Boundaries are the end of a transaction (by declared `transaction_length`) and terminal ROTATE/STOP. The end of the preamble is **not** a boundary (corrected; see Investigation outcome).

## Failure scenarios

- A boundary misclassification in `reader_context` would flush mid-transaction. Investigation shows the boundary is taken from the declared `transaction_length` and mismatches throw, so this is a regression scenario, not a known weakness. Antithesis can find the timing at which a checkpoint fires exactly on such an event.
- Ticket 6 (validated, latent): `close_binlog` mid-transaction clears the buffer but not the incomplete-transaction state; the next file's first flush might include stale state. Bytes themselves are cleared, so the size should still be at a boundary — this is a sanity anchor.
- Ticket 18's permanent duplicate keeps `size` at a boundary (two whole blocks), so it does not violate this property; it is caught by `stored-transactions-exactly-once-across-resumes`.
- Rewrite mode: the generated ROTATE/artificial ROTATE/FDE/PREVIOUS_GTIDS flow through `process_binlog_event` and must leave boundaries consistent; see `stored-rotate-or-stop-is-last-event`.

## Oracle

After every PBS exit or kill, for every indexed binlog: parse the stored bytes `[0, metadata.size)` (decrypted via pull for encrypted storages, or read directly for plaintext) as an event sequence; assert the parse ends exactly at `metadata.size`, and that the last event is one of: the magic only (size 4, **last indexed binlog only**), the event that completes a transaction's declared `transaction_length` (from its GTID/ANONYMOUS_GTID/GTID_TAGGED event), with a preamble and at least one transaction before it, or ROTATE/STOP. For non-rewrite plaintext storages the QA `transaction_boundaries(source)` helper is enough.

## SUT-side instrumentation suggestions

- `ALWAYS(last_transaction_boundary_position_in_event_buffer_ <= size(event_buffer_))` is an `assert` today (`storage.cpp:278`) — **partial** (assert exists, no Antithesis assertion).
- `ALWAYS(block passed to write_event_block ends at reader boundary, "flushed block ends on a transaction boundary")` — **missing**; requires carrying a flag from the facade.
- `SOMETIMES(discard_incomplete_transaction_events dropped > 0 bytes, "partial transaction discarded on disconnect")` — **missing**; shows Antithesis hit mid-transaction disconnects.
- `SOMETIMES(flush on checkpoint with bytes left in buffer after boundary, "checkpoint flush left an incomplete transaction buffered")` — **missing**.

## Investigation outcome (2026-10-13) — oracle tightened

- **Boundary detection is length-based, not shape-based.** `is_at_transaction_boundary()` is true only in `any_other_expected` with `current_transaction_length_ == expected_transaction_length_`, or in `rotate_artificial_expected` (after ROTATE/STOP) (`src/binsrv/events/reader_context.hpp:74-77`). The expected length is the `transaction_length` field of the opening ANONYMOUS_GTID / GTID / GTID_TAGGED event (`reader_context.cpp:928-1000`). Every following event adds its size (`:1002-1009`). The parser never looks at the type of the closing event, so XA PREPARE / XA COMMIT (each its own GTID transaction), `TRANSACTION_PAYLOAD_EVENT` (one event inside its GTID transaction), atomic DDL and GTID_TAGGED_LOG all close where the source's own `transaction_length` says. A mismatch is not flushed silently. Overrun throws "current event exceeds declared transaction length" (`:1005-1009`), and a short transaction throws "transaction did not end at the declared length" at the next GTID/ROTATE/STOP (`:1012-1016`). A transaction above 4 GiB throws (`:951-956`). All of these are exceptions, so the exit path flushes only up to the previous boundary. A shape-specific misclassification would therefore show up as an **exception exit (liveness)**, not a mid-transaction metadata size. This property is a **regression guard**. Event-shape variety in the workload is still worth having, but it is not required to reach a known weakness.
- Existing non-Antithesis coverage of the shapes: `extra/qa/event_variety_test.py` (compression, minimal row image, partial JSON, XA, tagged GTID; kill + resume + prefix-at-boundary check, `checkpoint_size 4K`, GTID mode only), and the Hegel `tests/reader_context_property_test.cpp` (`transaction_length` convergence, valid streams, broken streams rejected cleanly). Neither was executed in this investigation.
- **The end of the preamble is not a flush point.** In streaming mode, FDE and PREVIOUS_GTIDS leave the reader in `previous_gtids_expected` / `gtid_log_expected`, so `write_event` gets `at_transaction_boundary = false` (`collector_context.cpp:477-480`). `has_event_data_to_flush()` needs a recorded boundary (`storage.hpp:165-167`). The preamble therefore reaches the backend only together with the first complete transaction or a ROTATE/STOP. On a timeout disconnect before the first transaction completes, `discard_incomplete_transaction_events()` resizes the buffer to 0 and drops the preamble too (`storage.cpp:217-224`). The source resends it on resume.
- **Size 4 (magic only)** comes from opening a binlog: `open_new_binlog_file_internal` writes the magic and saves metadata with size 4 at once (`storage_core.cpp:680-702`). It persists until the first flush of that binlog. A closed binlog always ends with its flushed ROTATE/STOP (`storage.cpp:204-215`, `collector_context.cpp:485-492`), so size 4 is valid **only for the last binlog in the index**.
- **Oracle change:** the accepted terminal states are now (a) size 4 on the last indexed binlog only, (b) the end of a complete transaction that follows a preamble (preamble plus at least one transaction), or (c) a terminal ROTATE/STOP. Remove "end of the preamble" from the accepted set. A preamble-only size is unreachable by design, so observing one means boundary tracking regressed. The invariant stays `Always`. Interrupted rotation (ticket 15) is handled by its own property and should be excluded from (a).

## Open questions

- None.

### Investigation Log

#### Does `reader_context` treat every MySQL 8.0/8.4/9.x transaction shape as ending at the right event?

- Examined: `src/binsrv/events/reader_context.hpp:60-135`, `reader_context.cpp:700-1020` (state handlers, `start_transaction`, `update_transaction`, `finish_transaction`), `collector_context.cpp:440-492`, `storage.cpp:143-224`, `tests/reader_context_property_test.cpp`, `extra/qa/event_variety_test.py:17-45`.
- Found: the boundary comes from the server-declared `transaction_length`, and the type of the closing event is never consulted. Mismatches throw instead of flushing. XA, payload, DDL and tagged GTIDs need no special handling.
- Not found: an executed run of the QA variety test against current code. Position-mode (ANONYMOUS_GTID) variety is not in that test, but it uses the same length logic.
- Conclusion: resolved. The property is a regression guard, and a shape bug would surface as an exception exit.

#### Is a magic-only tail (size 4) always acceptable?

- Examined: `storage_core.cpp:680-720`, `storage.cpp:204-224`, `storage.hpp:165-172`, `collector_context.cpp:189-200`.
- Found: size 4 is the metadata state of a just-opened binlog that has not yet flushed a transaction. The end of the preamble is never a flush point. A closed binlog always ends at ROTATE/STOP.
- Conclusion: resolved. Size 4 is valid only on the last indexed binlog, and the oracle is tightened as described above.
