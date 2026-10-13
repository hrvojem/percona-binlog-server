# recovery-paths-exercised — Every recovery, resume and lifecycle branch is reached under the fault mix

Found by: focus 3, 7, 8, 9 (failure recovery; distributed coordination; lifecycle; idempotency/replay)
Merged from: recovery-paths-exercised (focus 3), resume-replay-paths-exercised (focus 9), lifecycle-outcomes-all-reached (focus 8), collector-observes-source-binlog-switch-on-reconnect (focus 7)

## Synthesis notes

Reachability, P1 (focus 3, 9, 8 rated P1; focus 7 rated P2). One property with **one distinct SDK call per branch**: branches that several contributors proposed (.tmp cleanup, tail truncation, reuse of the open binlog, etc.) are deduplicated and keep one message each. All branches are listed in the table below with file:line and the contributor(s) that proposed them.

Assertion kinds: `REACHABLE` for path markers, `SOMETIMES(cond)` where a meaningful condition exists (e.g., `bytes_discarded > 0`), and `UNREACHABLE("reopened tail binlog with zero size")` at `storage_core.cpp:713-718` (must never fire).

Coverage includes the resume shapes (discarded partial transaction, reused open binlog, source names a different binlog than the tail in position mode vs GTID mode, resume named an existing non-tail binlog (`storage_core.cpp:276`), tail reopened with data present / magic-only, recovery truncated the tail, rewrite fresh context on a non-empty storage) and the lifecycle outcomes (fresh init, metadata-only open, encryption format stamped, gtid_purged bootstrap, rotation via ROTATE / STOP, empty-storage and magic-only dump polls, empty name resolved after waiting, silent session close, stop at EOF / by signal mid-stream / idle / read_timeout / exception, final flush failed, listener stopped vs died).

For the destructor-swallowed flush this property keeps `REACHABLE("storage destructor swallowed a flush exception")` at `storage.cpp:74`; graceful-exit-leaves-flushed-storage places its `UNREACHABLE` in the operation layer gated on reported success, so the two do not conflict. Where a site is already owned by another property's assertion, the table says so. Until the SDK is linked, the workload asserts `Sometimes` on existing distinct log lines; branches with no log line are listed separately.

## What led to it

- (focus 3) Recovery branches are rare by construction: they run only after improper shutdowns, disconnects or backend errors. A green run says nothing unless these branches actually fired. Each one is a named, distinct outcome and a good replay anchor. Focus 3 explicitly asks for instrumentation points for recovery subphases and dangerous fallbacks.
- (focus 9) The exactly-once properties are vacuous unless Antithesis drives the collector through its distinct resume shapes. Each shape takes a different code path with its own replay hazard.
- (focus 8) Most lifecycle properties are conditional ("after a graceful stop…", "when recovery runs…"). Without coverage anchors, a run where Antithesis never reached the interesting branch looks identical to a passing run. The brief asked for instrumentation of *distinct* lifecycle outcomes rather than broad "entered startup/shutdown" markers.
- (focus 7) The coordination-relevant collector branches are the ones taken when the upstream "leader" has moved on while PBS was disconnected: the first artificial ROTATE after a reconnect names a binlog other than the stored tail. This is the entry point for ticket 14 (GTID rotation gap), source failover (ticket 25) and the GTID failover cases. If it is never hit, gtid-resume-preserves-source-rotations passes vacuously.

## Why one property

These are coverage goals, not invariants. Grouping them keeps the catalog short. Each row is still its **own** assertion message (catalog rule: no reused messages), implemented as separate SDK calls. Rows that should never happen under a fixed config (e.g. name-mismatch reopen without a source change) belong as `UNREACHABLE` in the specific safety properties, not here.

## Code paths (context)

- `collector_context.cpp:503-572` `process_artificial_rotate_event`: three outcomes — reuse open binlog (`:518-539`), close the open binlog because the name differs (`:540-553`), open/create (`:555-571`).
- `reader_context.cpp:547-581`: per-mode position checks; `expect_info_only_preamble_events_` (`reader_context.cpp:579-582`, info-only preamble decision on reopen with data present).
- `storage_core.cpp:257-296` `open_binlog`: created / opened existing / refused non-tail name (`:276`).
- `storage_core::storage_core`, `storage_core.cpp:126-221`: storage-open outcomes.
- `storage_core.cpp:695-699`: new records are created with size 4 (why a zero-size reopen should be impossible).
- Rotation: `process_rotate_or_stop_event` (`collector_context.cpp:574-579`), `open_new_binlog_file_internal` (`storage_core.cpp:680-703`).
- Shutdown: `pull_operation.cpp`, `fetch_operation.cpp`, `storage.cpp:68-77`.
- Disconnect discard: `collector_context.cpp:195-200`, `storage.cpp:217-224`.

## Branch table (all SDK assertions missing — no Antithesis SDK in the repo)

Contributors: R = recovery-paths-exercised (3), RR = resume-replay-paths-exercised (9), L = lifecycle-outcomes-all-reached (8), C = collector-observes-source-binlog-switch-on-reconnect (7).

### Storage open

| Branch | Location | Assertion and message | Proposed by | Existing log line | Notes |
|---|---|---|---|---|---|
| Fresh storage initialised (metadata.json written) | `storage_core.cpp:152-157` (streaming only) | `REACHABLE("fresh storage initialised")` | L | none specific | |
| Metadata-only storage opened (previous run never created a binlog) | `storage_core.cpp:183-185` | `REACHABLE("opened metadata-only storage")` | L | none | new code needed |
| Encryption format stamped into metadata.json | `storage_core.cpp:175-178` | `REACHABLE("encryption format stamped into storage metadata")` | L | none | |
| Leftover `*.tmp` removed on start | `storage_core.cpp:625-629` | `REACHABLE("startup removed leftover .tmp objects")` | R, L | "removed {} temporary storage object(s) left after improper shutdown" | kill-anywhere-storage-reopens anchors .tmp cleanup and other-process-startup-spares-live-writer plans `SOMETIMES(startup removed a .tmp object)` with mode in details at the same site; reuse one call if messages are unified |
| Tail truncated to metadata size | `storage_core.cpp:979-983` | `SOMETIMES(truncated_bytes > 0, "startup truncated tail binlog to metadata size")` | R, RR, L | "recovered binlog file '{}' by truncating it…" | same site also carries second-writer-on-storage-fails-closed's ticket-24-specific `REACHABLE` and pull-chain-prefix-consistency's "storage recovery truncated a tail binlog that pull had already served"; kill-anywhere-storage-reopens adds `ALWAYS(metadata size >= 4)` before it. RR's hazard: re-fetch of the truncated range |
| S3 tail truncation via download and re-upload | `s3_storage_backend.cpp:765-808` | `REACHABLE("S3 tail truncation re-uploaded the object")` | R | (truncation log above) | |
| Open refused by a validator | `storage_core.cpp:162, 196, 774, 781, 938, 969, 975, 994`; mode/encryption mismatch `:812, 822` | `REACHABLE("storage open refused: <validator>")`, one message per raise site, only for refusals the workload provokes deliberately (querying_only, misconfig variants) | L | exception text | **Synthesis resolution:** dropped as SDK calls. kill-anywhere-storage-reopens owns one `UNREACHABLE` per validator raise site (gated on `construction_mode_ != querying_only`); the deliberate-misconfig refusals (mode/encryption mismatch at `:812, :822`) are observed workload-side from the exit status and log text, not with a second SDK call at the same site |
| querying_only: missing index tolerated | `storage_core.cpp:193-195` | `REACHABLE("query mode tolerated a missing index")` | L | none | |
| querying_only: record with unreadable metadata dropped | `storage_core.cpp:942-950` (`:943-949`) | referenced: query-success-never-omits-middle-binlog owns `SOMETIMES(..., "querying_only dropped a non-edge binlog record")` at this catch; R proposed `REACHABLE("query mode skipped a binlog whose metadata could not be read")` | R, L | none | silent fallback that can make `search_*` omit files during a writer's `.json` rename; storage-format-version-mismatch-handled also references this site |

### Collector first contact, reconnect and resume

| Branch | Location | Assertion and message | Proposed by | Existing log line | Notes |
|---|---|---|---|---|---|
| GTID mode, empty storage: `gtid_purged` fetched from source | `collector_context.cpp:229-240` | `REACHABLE("GTID bootstrap fetched gtid_purged from source")` | L | | |
| Position mode, empty storage: dump from the source's first binlog | `collector_context.cpp:253-255` | `REACHABLE("position mode started from the source's first binlog")` | L | | |
| Disconnect cleanup discarded a partial transaction | `collector_context.cpp:195-200`, `storage.cpp:217-224` | `SOMETIMES(bytes_discarded > 0, "collector discarded a partial transaction before reconnect")` | R, RR | "timed out waiting for events and disconnected" (no count) | needs `discard_incomplete_transaction_events` to return the count; hazard: discard too little (duplicate GTID event) or too much |
| Same-process reconnect reused the open binlog | `collector_context.cpp:518-539` (`:535-539`, `:536`) | `SOMETIMES(…, "collector reused the open binlog after reconnect")` | R, RR, L, C | | inprocess-reconnect-resumes-without-dup-or-gap owns `SOMETIMES(pull resumed into its still-open binlog after reconnect)` at `:535`; reuse that call rather than adding a second. Hazard: position mismatch, duplicate preamble |
| Name mismatch on reconnect, position mode (closed old, opened next) | `collector_context.cpp:540-553` (`:541-553`, `:545-550`) | `REACHABLE("position-mode resume continued into the next source binlog")` | C, RR, L, R | | R described this branch as "collector closed a binlog the source left unterminated"; that fact goes in details |
| Name mismatch on reconnect, GTID mode | `collector_context.cpp:540-553` | `REACHABLE("GTID-mode resume landed in a different source binlog than the stored tail")` | C, RR, L, R | | ticket 14 path. gtid-resume-preserves-source-rotations places `UNREACHABLE("GTID resume closed the stored tail without its ROTATE event")` and `ALWAYS(new binlog name == current.next())` in the same branch; that UNREACHABLE is expected to fire today |
| Resume named an existing non-tail binlog | `storage_core.cpp:276` (`open_binlog`, `:257-296`) | `REACHABLE("resume named an existing non-tail binlog")` | C | exception text | failover/reset fail-closed path |
| Fresh process reopened tail with data present | `storage_core.cpp:705-712` / `collector_context.cpp:555-570` (`opened_with_data_present`) | `REACHABLE("storage reopened a tail binlog with data present")` | RR, L | | hazard: info-only preamble decision (`reader_context.cpp:579-582`) |
| Fresh process reopened magic-only tail | `storage_core.cpp:708-710` / `collector_context.cpp:564-566` (`opened_at_magic_payload_offset`) | `REACHABLE("reopened magic-only tail binlog for append")` | R, L | | |
| **Tail reopened at offset 0 (writes magic again)** | `storage_core.cpp:713-718` | `UNREACHABLE("reopened tail binlog with zero size")` | R | none | dangerous fallback: metadata size 0 should be impossible because new records are created with size 4 (`:695-699`); if reachable, the magic is rewritten into a file whose metadata says 0 |
| Rewrite-mode fresh context on a non-empty storage | `collector_context.cpp:352-366` | `SOMETIMES(…, "rewrite mode resumed into an existing non-empty binlog")` | RR | | rewrite-binlog-chain-valid-across-crashes plans `SOMETIMES(rewrite resumed into a non-empty file after restart)` here; reuse one call. Hazard: regenerated preamble written instead of info-only |
| Pull idle-retry iteration | `pull_operation.cpp:146-152` | `REACHABLE("pull retry iteration started")` | R | | |

### Rotation

| Branch | Location | Assertion and message | Proposed by |
|---|---|---|---|
| Real ROTATE closed a file and the next artificial ROTATE created a new one | `collector_context.cpp:574-579`, `storage_core.cpp:680-703` | `REACHABLE("rotation via ROTATE event")` | L |
| STOP event closed a file (source restarted) | `collector_context.cpp:574-579` | `REACHABLE("rotation via STOP event")` | L |
| Rewrite-mode generated rotation | `collector_context.cpp:376-386` | `REACHABLE("rewrite mode generated a rotation")` | L |

### Sender / clients

| Branch | Location | Assertion and message | Proposed by |
|---|---|---|---|
| Dump on empty storage polled | `storage_core.cpp:474` | `REACHABLE("dump polled an empty storage")` | L |
| Dump on magic-only binlog polled | `storage_core.cpp:528-531` | `REACHABLE("dump polled a magic-only binlog")` | L |
| Empty name resolved after waiting | `sender_context.cpp:406-411` | `REACHABLE("dump resolved an empty binlog name after waiting")` | L |
| Session closed by PBS without error packet | `network_service.cpp:308-313` | `REACHABLE("dump session closed without an error packet")` | L |

### Shutdown and fallbacks

| Branch | Location | Assertion and message | Proposed by | Existing log line |
|---|---|---|---|---|
| fetch reached source EOF | `fetch_operation.cpp` | `REACHABLE("fetch reached source EOF")` | L | "successfully fetched everything and disconnected" |
| Stop by signal mid-stream | `pull_operation.cpp` / `fetch_operation.cpp` | `REACHABLE("stopped by signal mid-stream")` | L | "fetching binlog events loop terminated by signal" |
| Stop by signal while idle (pull) | `pull_operation.cpp:142-144` (`wait_for_interruptable` returns false) | `REACHABLE("pull stopped by signal while idle")` | L | none specific |
| read_timeout disconnect flush | collector | `REACHABLE("read_timeout disconnect flushed and reconnected")` | L | "timed out waiting for events and disconnected" |
| Exit by exception (collector) | collector / operations | `REACHABLE("collector exited by exception")` | L | `handle_std_exception` output |
| Final flush in `~storage` failed and was swallowed | `storage.cpp:74` (`:68-77`) | `REACHABLE("storage destructor swallowed a flush exception")` | R, L | **none — invisible today** (new code needed). L proposed `SOMETIMES(true, "final flush in ~storage failed")` for the same site; deduplicated. graceful-exit-leaves-flushed-storage owns the operation-layer `UNREACHABLE` gated on reported success |
| Purge committed the index but object cleanup failed | `storage_core.cpp:421-429` | `REACHABLE("purge committed the index but object cleanup failed")` | R | |
| Listener stopped vs listener died | `network_service.cpp:647-653` | `REACHABLE("listener stopped")`, `REACHABLE("listener died by exception")` | L | "listener stopped" vs exception text |

### Branches without a distinct log line today

Fresh storage init, metadata-only open, encryption stamp, missing-index tolerated, unreadable-record drop, gtid_purged bootstrap, position-mode first-binlog start, discard count (log exists but no count), zero-size reopen, idle signal stop, `~storage` swallowed flush, and the reconnect/resume branch markers (reuse, per-mode name mismatch, data-present / magic-only reopen, rewrite non-empty resume). For these the workload has no log-based fallback until the SDK (or a log line) is added.

## Workload checks until the SDK is linked

- The workload asserts `Sometimes` on the existing log lines above. The log must go to a file that survives restarts; `file_logger` truncates on open (`file_logger.cpp:30`), so the workload must copy or rotate it before each restart.
- Log-based reachability is weaker: no replay anchors, and Antithesis's exploration guidance is lost.

## Fault requirements

- Disconnect windows overlapping a source rotation: partitions / node hang on PBS or the source, plus workload `FLUSH BINARY LOGS`, small `max_binlog_size`, and source restarts.
- PBS kills at arbitrary instants (startup recovery, mid-write), S3/backend errors, signals at random moments, purge runs, query-mode runs, and blocking dump clients on empty / magic-only storages.

## Open questions

- Will the team accept SUT-side Antithesis macros in `src/` (behind a CMake option like `WITH_PROPERTY_TESTS`)? If not, the set must be approximated from log lines (see "Branches without a distinct log line"), losing replay anchors and exploration guidance. `(needs human input)`

## Answers from investigation (2026-10-13)

- **Where the SDK markers live (resolved).** `storage_core.cpp` is part of the static `lib_binsrv` (`CMakeLists.txt:666, 687`), which the Hegel tests link (`tests/CMakeLists.txt:222-236`). The operation layer is not test-free either: `sender_context.cpp` and `event_generation_helpers.cpp` are compiled straight into `storage_property_test`. So moving markers to the operation layer does not keep them out of test binaries. Put them where the branch is (mostly `storage_core` / `collector_context`), and compile them as no-ops unless an Antithesis CMake option defines the SDK (the C++ SDK header has a no-op mode for that). No storage-open row has to be surfaced through return values. **Property unchanged.**
- **Which rows are out of the planned workload's scope (resolved against `deployment-topology.md`).** Rewrite and encryption are per-timeline config dimensions and GTID/position mode is too, so the rewrite rows, both per-mode name-mismatch rows and both empty-storage bootstrap rows stay in. Rows that the current topology cannot reach without extra workload hooks:
  - "Encryption format stamped into metadata.json" (`storage_core.cpp:170-178`) needs a storage first opened *without* `storage.encryption`, then reopened *with* it. The topology draws encryption once per timeline, so a writer never flips it. Either add a "flip encryption on restart" action or drop the row. (`query-modes-never-write-storage` reaches the same site from a query process.)
  - "Resume named an existing non-tail binlog" (`storage_core.cpp:276`) needs a source reset or failover. The `source-swap` action is off by default in the topology, so this row exists only in timelines with it enabled.
  - "Rotation via STOP event" needs a clean source shutdown (the restart wrapper's SQL `SHUTDOWN`), not only `kill -9`. Keep the row only if the source driver issues `SHUTDOWN` sometimes.
  - "Listener died by exception" (`network_service.cpp:647-653`) has no planned trigger. Drop it, or keep it as a known-unreached row.
  - "Final flush swallowed" and "purge committed the index but object cleanup failed" need the `libfsfault` rules (or S3 proxy faults) on rename/remove that the topology already lists. They stay in.
  Every other row is reachable with the planned drivers. **Property scope narrowed:** the four rows above are conditional on the matching workload hook and must be omitted, not left as always-failing `REACHABLE`s, when that hook is absent.

### Investigation Log

#### Will the team accept SUT-side Antithesis macros in `src/`?

- Examined: `CMakeLists.txt` options (`WITH_PROPERTY_TESTS` :78, `WITH_ASAN` :62), `existing-assertions.md`, git log for any Antithesis or instrumentation discussion.
- Found: a precedent for an opt-in test-only dependency (`WITH_PROPERTY_TESTS` downloads hegel-cpp). No Antithesis reference anywhere in the repo.
- Not found: any statement from the maintainers about instrumentation macros in product code.
- Conclusion: `(needs human input)`. The precedent makes a `WITH_ANTITHESIS` option plausible, but only the team can accept it.

#### Should the SDK markers live in `storage_core` or only in the operation layer?

- Examined: `CMakeLists.txt:557-708` (`binsrv_source_files`, `lib_binsrv`), `tests/CMakeLists.txt:162-410`.
- Found: `storage_core.cpp` is in `lib_binsrv` (`CMakeLists.txt:666`), linked by `storage_property_test` and other Hegel tests. `storage_property_test` also compiles `src/operations/sender_context.cpp` and `event_generation_helpers.cpp` directly (`tests/CMakeLists.txt:222-228`).
- Conclusion: resolved. Both layers end up in test binaries, so the choice does not avoid the no-op requirement. Place markers at the branch site and compile them as no-ops outside an Antithesis build.

#### Which rows are deliberately out of the workload's scope?

- Examined: `deployment-topology.md` (config dimensions, `fetch_fs` / `pull_fs` templates, source driver), `storage_core.cpp:170-178, 257-296`, `network_service.cpp:647-653`.
- Found: rewrite, encryption and replication mode are drawn per timeline, so their rows are reachable. Encryption is never flipped between runs on the same storage; `source-swap` is off by default; source restarts are mostly SIGKILL; no listener-death trigger exists.
- Conclusion: resolved. Four rows (encryption stamp, non-tail resume, STOP rotation, listener died) are conditional on workload hooks, listed above.

## Evaluation refinements (2026-10-13)

Applied by the evaluation synthesis (`../evaluation/synthesis.md`). Where this section and earlier text in this file disagree, this section wins; the catalog entry has been updated to match.

- The log-line fallback covers at minimum tail truncation, magic-only reopen and purge cleanup failure (the last from purge's `warning` JSON status, since purge logs to `null_logger`), so reachability does not hinge entirely on accepting SUT macros (bias B5; coverage-balance F11). PBS logs to the console (`cout_logger`, topology change) so every restart's log survives `file_logger`'s truncate-on-open.
- Runs in all templates.
