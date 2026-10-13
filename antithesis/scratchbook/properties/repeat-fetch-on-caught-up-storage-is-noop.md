# repeat-fetch-on-caught-up-storage-is-noop

## What led to it

Cron-driven `fetch` is the documented continuous-backup workflow (W1). When the source has nothing new, a repeated fetch should be a pure no-op: exit 0 and no change to any object. Reading the code suggests this holds. The no-op run still re-opens the tail and goes through every resume step, so it is a cheap regression oracle that also catches stray metadata rewrites and stray appends.

## Code path of a no-op run

- The constructor removes `*.tmp` and runs tail recovery (no-op if consistent) (`storage_core.cpp:597-630, 960-985`).
- The artificial ROTATE for the tail reaches `open_binlog(tail)` in append mode (`storage_core.cpp:285-292`). `open_existing_binlog_file_internal` writes nothing when the offset is ≥ 4 (`:705-718`). On S3 the append open downloads the object to a temp file (`s3_storage_backend.cpp:828-832`) and uploads nothing.
- FDE and PGTIDS are info-only because `position_ > 4` (`reader_context.cpp:579-582, 698-743`). EOF returns true without a flush (`collector_context.cpp:184-187`), and `~storage` has nothing to flush (`storage.cpp:68-77`, `has_event_data_to_flush()` is false).
- Exception, the `opened_empty` branch: if tail metadata size is 0, it writes magic and sets size=4 **without saving metadata** (`storage_core.cpp:715-718`). That is unreachable with today's write order (magic before `.json`), but it would make a "no-op" run mutate the data object.

## Oracle

With the source quiesced, snapshot `{name: (size, sha256)}` of every object (data, `.json`, `binlog.index`, `metadata.json`), run fetch N times (some of them killed at random points), then one clean fetch. Assert exit 0 on the clean run and an identical snapshot, apart from the absence of `*.tmp`. Killed runs must not change any object either. Deleting stale `*.tmp` objects is the only allowed difference.

## Instrumentation (missing)

- Workload-side `ALWAYS(snapshot unchanged, "fetch on a caught-up storage changes no object")`.
- SUT-side `REACHABLE("open_binlog reopened tail with data present and wrote nothing")` is optional.

## Precondition: "caught up" (resolved by investigation, see log)

A magic-only tail (size 4) is a normal caught-up state, not an exception, in both position and GTID mode. After any fetch that ends right after a source rotation, the new file's FDE and PREVIOUS_GTIDS stay in the event buffer and are never flushed, because neither event is a transaction boundary (`reader_context.hpp:74-78`, `storage.hpp:165-167`). On the next run the source re-sends them, they are buffered again and dropped again. A repeated fetch on a magic-only tail is therefore a no-op, as long as the source has no new transaction in that file.

The workload's "caught up" definition, computed against the quiesced source (`SHOW BINARY LOGS`):
- the stored tail's name equals the source's newest binlog name. If the source rotated after the last stored transaction, the next fetch makes progress: position mode appends the closing ROTATE and creates the new file; GTID mode creates the new file (ticket 14). Neither is a no-op.
- every stored size equals the source size, except a stored tail of size 4 whose source file holds no transaction (only FDE + PREVIOUS_GTIDS).
- Exclude it: a stored non-tail file of size 4. It cannot come from a normal rotation and points to a separate defect (ticket 14, or the source-crash path recorded in `binlog-metadata-matches-stored-bytes`).

## Open questions

- None

### Investigation Log

#### In GTID mode with a magic-only tail (size 4), the source re-sends FDE+PGTIDS, which are written as data. Should "caught up" exclude a magic-only tail?

- Examined: `reader_context.cpp:579-582` (`expect_info_only_preamble_events_` is false when `position_ == 4`), `:714-717` and `:745` (FDE/PGTIDS state transitions), `reader_context.hpp:74-78` (`is_at_transaction_boundary`), `storage.cpp:143-200` (`write_event`), `storage.cpp:68-77` (`~storage` flush), `storage.hpp:165-167` (`has_event_data_to_flush` is `last_transaction_boundary_position_in_event_buffer_ != 0`), `storage_core.cpp:705-718` (`open_existing_binlog_file_internal`), `collector_context.cpp:184-187` (EOF return), `collector_context.cpp:229-253` (GTID and position resume requests), ticket 14 text.
- Found: the premise is only half right. On a magic-only tail the FDE and PGTIDS are not info-only, so they are appended to the event buffer, but with `at_transaction_boundary == false`: after the FDE the state is `previous_gtids_expected`, after the PGTIDS it is `gtid_log_expected`, and the boundary predicate is true only in `any_other_expected` with a complete transaction, or in `rotate_artificial_expected`. With no transaction after them, `last_transaction_boundary_position_in_event_buffer_` stays 0. The checkpoint path, the EOF path and `~storage` all flush only when that value is non-zero, so nothing is written. `open_binlog` on the size-4 tail returns `opened_at_magic_payload_offset` and writes nothing. This is the same in both modes. In GTID mode the source starts from the newest binlog whose previous_gtids is a subset of the requested set (the mechanism behind ticket 14, reproduced there with MTR and Hypothesis). When the stored tail is the source's newest file, that file is the tail, so the request is answered with the tail's own preamble.
- Found (related): the same buffering is why a clean fetch that ends right after a rotation leaves a magic-only tail. That makes the magic-only tail a routine caught-up state, not a corner case.
- Not found: nothing; the remaining precondition detail (stored tail name == source newest) comes from ticket 14 and the position-mode resume path.
- Conclusion: resolved. A magic-only tail stays in scope as a no-op case. The precondition section above defines "caught up"; the property, invariant (`Always`) and priority are unchanged.
