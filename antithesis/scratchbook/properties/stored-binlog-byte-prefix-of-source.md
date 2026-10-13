# stored-binlog-byte-prefix-of-source

## What led here

SG2 in the SUT analysis (implicit guarantee). MTR asserts it with `diff_with_storage_object.inc`. The QA crash harness asserts it after every step in `storage_holds_prefixes()` (`extra/qa/fetch_crash_test.py:636-669`). README:711 says a second run "results in resuming streaming from the position at which the previous run finished", which only makes sense if what is stored is a faithful prefix. This is the core data-integrity claim of a backup tool: every stored byte equals the source's byte at the same offset of the same file.

## Code paths

- Bytes reach the backend only through `storage_core::write_event_block` (`src/binsrv/storage_core.cpp:299-322`), which appends at the backend stream's EOF. Nothing checks that the stream position equals `record.size` except a debug-only `assert` on open (`storage_core.cpp:707`).
- New file: `open_new_binlog_file_internal` writes the magic (`:680-703`).
- Append after restart: `open_existing_binlog_file_internal` (`:704-719`), stream opened `app|ate` (FS) or downloaded and appended to locally (S3 `s3_storage_backend.cpp:822-848`).
- Crash recovery truncates the tail to the metadata size (`storage_core.cpp:960-985`).
- The reader's position continuity check (`reader_context.cpp:905-928`) makes a position-gap fail closed. In GTID mode the artificial ROTATE position is not checked against storage (`reader_context.cpp:562-570`); continuity relies on `validate_position` of the first written event.
- The collector reuses an open binlog after a reconnect (`collector_context.cpp:506-549`).

## Failure scenarios (what would violate it)

- Ticket 24 (validated; whether writer+writer is in contract is open, see below): two writers on one storage; `app` mode means both append at the real EOF (duplicates / interleaving), and the shared deterministic `<name>.tmp` lets one writer publish the other's metadata.
- Ticket 18 (validated) failed-checkpoint retry: the same buffer appended twice. In position mode the duplicate bytes are still at offsets the source holds different bytes at, so the prefix check catches it.
- N1: purge truncating a live writer's tail while the writer keeps appending at the new EOF with a too-large in-memory size; subsequent bytes land at the wrong offset. **Out of contract (investigation 2026-10-13):** README:473 says running `purge_binlogs` on a storage while another instance fetches/pulls into it "may cause data corruption and should be avoided". N1 is therefore not a violation of this property; the workload must not evaluate this property on a storage that had a concurrent purge (e.g. the `contention` storage).
- Ticket 25: a source identity change (RESET BINARY LOGS / failover) with event boundaries that happen to line up passes `validate_position` and appends a different server's bytes.
- S3 failed data upload then retry (sut-analysis "Failed flush"): the local temp file holds old+2n while size = old+n; restart truncation keeps the first copy, so the stored object is a prefix again after restart, but an in-process observer between the two could see a non-prefix object. This is a transient case; the property should be evaluated on quiescent storages (after the writer has exited and, ideally, after the next start has run recovery) or should compare only `[0, metadata.size)`.

## Scope / oracle notes

- Compare `stored[0:metadata.size]` with `source_as_sent[0:metadata.size]` where `source_as_sent` = source file with the `LOG_EVENT_BINLOG_IN_USE_F` flag cleared and the FDE CRC recomputed (`MySQLServer.binlog_as_sent()` in the QA harness). Bytes beyond `metadata.size` on the tail are allowed to be garbage until recovery; the analogous file-length check belongs to `kill-anywhere-storage-reopens`.
- Also check `binlog.index` == a contiguous run of the source's binlog names (QA oracle compares to a prefix of `SHOW BINARY LOGS`; after a purge on either side, compare to a contiguous sub-sequence). In GTID mode ticket 14 makes PBS skip source binlogs that have no transactions, so the "contiguous run" part must allow gaps of transaction-less source files until ticket 14 is resolved.
- Not applicable to rewrite mode (bytes are regenerated) or to the ciphertext of encrypted storages; see `encrypted-storage-decrypts-to-source` and `rewrite-binlog-chain-valid-across-crashes`.
- The source must keep its binlogs (large `binlog_expire_logs_seconds`) for the oracle to read them. **Source bytes (resolved):** the topology does not share the source datadir, so the workload obtains them with `mysqlbinlog --read-from-remote-server --raw` from position 4 against the source. That stream goes through the same MySQL binlog sender PBS reads from, so it already carries the "as sent" transformation (IN_USE flag cleared, FDE CRC recomputed) and is directly comparable; `binlog_as_sent()`'s patching is only needed when reading the datadir file. `rs_raw_mode.test` shows `mysqlbinlog --raw` writes received events verbatim (its output equals the server's closed binlog). A raw dump of the active binlog stops at the source's current EOF, which is fine for a prefix check (dump after the PBS storage is quiescent, so source length >= stored length).
- **Oracle reference:** QA `storage_holds_prefixes()` (`extra/qa/fetch_crash_test.py:637-669`) compares the *whole stored file* (not only `[0, metadata.size)`) byte-for-byte with the prefix of `binlog_as_sent(name)`, checks `binlog.index` is a prefix of `SHOW BINARY LOGS`, and checks the metadata size is a transaction boundary of the source file. `check_complete_copy()` (`:671-697`) requires full byte equality after a complete fetch. Both are byte comparisons, so they catch a position-mode duplicate-plus-skip (ticket 18) as well as a plain duplicate.

## SUT-side instrumentation suggestions

- `ALWAYS(backend stream position == record.size before append, "binlog append offset equals recorded size")` in `storage_core::write_event_block` — **missing**. Catches ticket 24 interleaving, N1 (out of contract per README:473; a firing under a concurrent purge is expected, not a finding) and the S3 failed-upload retry in-process (that last one is a tolerated-but-diverged state; decide whether to keep the assertion strict).
- Promote the `assert(get_flushed_position_unsafe() == open_stream_offset)` at `storage_core.cpp:707` to an `ALWAYS` so it survives NDEBUG — **missing** (an `assert` exists, not an Antithesis assertion).
- `SOMETIMES(open_binlog returned opened_with_data_present, "resumed append to a non-empty stored binlog")` — **missing**; confirms resume paths are exercised.

## Open questions

- Is "byte-identical to the source" an official guarantee or an MTR convention? If it is only a convention, ticket 14's missing ROTATE is a deviation rather than a violation and the "exact copy after catch-up" part becomes a separate, softer property. The prefix part is still implied by README:711. `(needs human input)`
- Are two concurrent writers (fetch/pull) on one storage in contract? If not, ticket 24 is out of scope for this property and the workload must not run them on a storage this property checks. `(needs human input)`

### Investigation Log

#### Is "byte-identical to the source" an official guarantee or an MTR convention?

- Examined: README.md intro (lines 1-5), "Resuming previous operation" (README:709-711), grep of README for copy/verbatim/identical; MTR `binsrv.test:68-152` (diff of stored objects vs. server files, patching the IN_USE byte at offset 21), `recover.test`, `pull_mode.test`, `rs_raw_mode.test`; QA `check_complete_copy()`.
- Found: the README positions PBS as "an enhanced version of mysqlbinlog in --read-from-remote-server mode" and promises resume "from the position at which the previous run finished"; it never states byte-identity. Byte-identity is asserted by MTR (`diff_with_storage_object.inc`) and QA, and position-mode collection passes events through unchanged (`collector_context.cpp:150-170` only transforms in rewrite mode).
- Not found: any documented statement that the stored binlog is byte-identical to the source, or how transaction-less source binlogs (ticket 14) should be stored.
- Conclusion: `(needs human input)`. The prefix part stands on README:711 plus tests; whether "complete exact copy" is a contract is a maintainer decision.

#### Is concurrent writer + purge or writer + writer in contract?

- Examined: README.md (grep for instance/lock/same storage), README:473, ticket 24 snapshot (`.tickets-snapshot.txt:1292-1340`), `src/binsrv/*.cpp` and `src/util/*.cpp` for flock/lockf/lock files.
- Found: README:473 — running `purge_binlogs` on a storage "while another instance is fetching / pulling data to the same storage may cause data corruption and should be avoided". So writer+purge is explicitly out of contract; N1 is not a violation of this property. No cross-process lock exists in the code (no flock/lockf/exclusive matches), consistent with ticket 24.
- Not found: any README statement about two fetch/pull instances on one storage. Ticket 24 argues they should fail fast, but that is the reporter's expectation, not documented intent.
- Conclusion: writer+purge resolved (out of contract; property must exclude storages with a concurrent purge). Writer+writer stays open `(needs human input)`. Property change: N1 removed as a violating scenario; scope narrowed to storages with a single writer and no concurrent purge.

#### Can the workload read source binlogs byte-exactly from another container, or must it use `mysqlbinlog --raw`?

- Examined: `antithesis/scratchbook/deployment-topology.md` (lines 60, 73, 95, 115-117), `extra/qa/fetch_crash_test.py:217-234` (`binlog_as_sent`), `mtr/binlog_streaming/t/rs_raw_mode.test`, `binsrv.test:73-79`.
- Found: the topology keeps the source datadir on the mysqld container's filesystem (not shared) and plans `mysqlbinlog --read-from-remote-server --raw` from the `pbs` container. The IN_USE flag is cleared by the source's sender (that is exactly why QA patches datadir files in `binlog_as_sent`), so a raw remote dump from position 4 receives the same transformed bytes PBS receives. `rs_raw_mode.test` demonstrates `mysqlbinlog --raw` writes received events verbatim (output `diff_files`-equal to the server's closed binlog).
- Not found: none blocking. (`mysqlbinlog` itself is MySQL code, not in this repo; the verbatim-write behaviour is evidenced by the MTR test, not by reading its source.)
- Conclusion: resolved. Use remote `--raw` dumps (matching-version `mysqlbinlog`, already planned in the pbs image) as `source_as_sent`; no flag patching needed. Read only after PBS is quiescent so the dump is at least as long as the stored file.

## Evaluation refinements (2026-10-13)

Applied by the evaluation synthesis (`../evaluation/synthesis.md`). Where this section and earlier text in this file disagree, this section wins; the catalog entry has been updated to match.

- Encrypted storages: the independent Python decryptor is the canonical encrypted reader. Pull is not used to read a storage it does not own: pull always builds a streaming storage and runs the collector (`pull_operation.cpp:108-153`), so on a `fetch_fs` or `s3` storage it would run recovery on, and append to, the storage under test. A pull readback there runs only at quiescent points on a snapshot copy with `connection.host` pointing at an unreachable endpoint (lenses: implementability P1, wildcard W2).
- Runs in `fetch_fs`, `pull_fs`, `s3`. The QA oracle is reworked to take source-bytes and storage-bytes providers and to compare index suffixes after purges (implementability C8; topology change).
