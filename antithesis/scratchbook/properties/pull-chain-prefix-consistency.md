# pull-chain-prefix-consistency — What pull serves is a stable prefix of the storage, and the storage a prefix of the source

Found by: focus 7 (distributed coordination), focus 2 (concurrency), focus 9 (idempotency/replay)
Merged from: pull-chain-prefix-consistency (focus 7), sender-reads-only-flushed-bytes (focus 2), served-bytes-stable-across-restart (focus 9), pull-client-reconnect-receives-each-event-once (focus 9)

## Synthesis notes

Safety, **P1**. Contributor ratings: focus 7 P1, focus 2 P1, focus 9 P1 and P2.

**Invariant.** For each binlog, `client ⊑ stored ⊑ source`. This assumes one source epoch, with artificial events stripped and encrypted data decrypted. Bytes a client has received at (file, offset) are never contradicted later, whether by a pull restart, tail recovery or re-fetch. They are also never delivered twice across client reconnects.

**Workload `Always` messages** (each distinct):
- "pull client bytes are a prefix of stored bytes"
- "stored bytes are a prefix of source bytes at observation time"
- "bytes served by pull never change at the same offset after restart"
- "pull client never receives the same binlog offset twice across reconnects"

**SUT assertions:**
- `ALWAYS(size(get_object result) == read_length)` in `fetch_event_block`.
- `UNREACHABLE("session ended by storage read exception")` for single-writer runs without S3 faults.
- `SOMETIMES(storage recovery truncated a tail binlog that pull had already served)`. It is distinct from the generic truncation anchor.
- `SOMETIMES(pull client resumed mid-file after reconnect)`.

**Key windows:**
- The sender serves up to the in-memory size (`storage_core.cpp:310` vs `:321`). A crash between data append and metadata save can therefore retract served bytes. See the lock note below for which variant of this window is actually reachable.
- Ticket 18 re-flush duplicates.
- Rewrite mode regenerates events with wall-clock timestamps.

**Lock note (from the contributors' evidence).** The append, the `size +=` and the metadata save all happen under the unique lock. A reader therefore cannot observe the raised size while a write is in flight. A SIGKILL inside that window never exposes the bytes to a client.

A served-then-retracted range needs a different sequence:
1. A metadata save fails, leaving `size` incremented and the lock released.
2. The sender serves the extra bytes.
3. A kill or exit happens before a successful save.

Ticket 18's destructor re-flush sits in this same window. The workload must open it through `.json` PUT or rename failures, not through kill timing alone.

## What led here

- **Chain view (focus 7).** From the coordination lens, PBS is the middle of a replication chain: source → PBS storage → downstream clients via pull.
  - The chain-level safety property of any log-shipping hierarchy is prefix order. A downstream node never holds a log entry that its upstream does not hold at the same position.
  - Focus 7 asks for network partitions between each pair: source↔PBS, PBS↔storage (S3), and PBS↔client.
  - This is the end-to-end chain check across PBS restarts and source changes. Single-hop pieces are now merged in here. `restart-after-kill-converges-to-source` is now part of `stored-transactions-exactly-once-across-resumes`.
- **In-process reader/writer split (focus 2).** In `pull`, the writer (the collector jthread) and the readers (asio sessions on the main thread) share one `storage_core`. The safety argument is purely lock-based.
- **Served bytes vs durable bytes (focus 9).** The sender serves up to the in-memory `record.size` (`storage_core.cpp:513-538`). `write_event_block` raises that size **before** it saves the metadata (`storage_core.cpp:310` vs `:321`). On the filesystem, data is not fsynced at all (ticket 19).
  - So a pull client can be served bytes that the next start's recovery truncates away (`storage_core.cpp:974-983`).
  - After restart, PBS re-fetches that range from the source. The replay is safe only if the re-fetched bytes are **identical** to what was served.
- **Client reconnect replay (focus 9).** Downstream clients of pull resume after a disconnect by re-dumping from `(last file, last next_event_position)`. The clients are `mysqlbinlog --read-from-remote-server --stop-never` or a workload client.
  - The Hegel spec (`tests/PROPERTY_TESTS.md:213`: "every stored byte fetched exactly once") covers one session against a static storage.
  - Nothing covers reconnect sequences against a storage that keeps growing, with the collector flushing, recovering after pull restarts, or duplicating (ticket 18).
  - MySQL replicas cannot connect (ticket 20). The client must therefore be the workload's own COM_BINLOG_DUMP client, or mysqlbinlog restarted by the workload.

## Code paths

### Ingress

- Resume request: `collector_context.cpp:207-272`.
- Continuity checks: `reader_context.cpp:547-573`, `905-920`.

### Storage write (writer side)

`storage_core::write_event_block` (`storage_core.cpp:299-322`) runs entirely under `unique_lock`. The window between the data append and the metadata save is `:309-321`. The steps are:
1. Append and flush the data:
   - On the filesystem, `ofs_.write` + `flush` (`filesystem_storage_backend.cpp:217-231`).
   - On S3, the whole temp file is re-uploaded.
2. `size += n` (`:310`).
3. Update GTIDs and timestamps.
4. `save_binlog_metadata` (`:321`).

Readers cannot see a size before the metadata save completes. The one exception is after a failed save, which leaves `size` incremented.

### Storage read (sender side)

- `storage_core::fetch_event_block` (~:435-564) takes `shared_lock`.
- It clamps reads to the in-memory `record.size - offset` (`:513-541`; `:513-538`).
- A position past the stored size returns false (`:513-515`).
- It then reads via `backend_->get_object`, using a fresh `ifstream` on the filesystem or a ranged GET on S3.
- The read must return exactly `read_length` bytes. Otherwise it throws:
  - Filesystem: `read_file_content` throws `out_of_range` (`util/file_operations_helpers.cpp:60-71`).
  - S3: `get_object_into_byte_buffer` throws on a length mismatch (`s3_storage_backend.cpp` ~:285-292).
- The `storage` wrapper's buffer and checkpoint state (`storage.hpp`) are unsynchronized. Only the collector touches them. Sessions call only `storage::fetch_event_block` → core (`sender_context.cpp` `storage_->fetch_event_block`).

### Egress sessions

- `minimysql/network_service.cpp:279-334` builds a per-session `sender_context` from the request's (name, position).
  - It sends 1 MiB blocks and polls every 500 ms in blocking mode.
  - A false return from `get_event` closes the session with `co_return` and no error packet (`:308-313`, N12). One cause is a requested offset beyond the flushed size.
- Exceptions in a session are caught per session (`network_service.cpp` ~:621 `handle_exception`). A violation therefore shows up as a dropped client plus an error log line, not as a crash.
- FSM start states are in `sender_context.cpp:52-73`.
  - `start_from_offset` sends an artificial ROTATE(pos) and an artificial FDE.
  - Then `populate_event_block` runs (`:458-532`), with the partial tail carried in RAM.
- The position is never checked to be on an event boundary (A13). A client that resumes at a mid-event offset reaches ticket 7.

### Restart recovery

- Tail truncation is at `storage_core.cpp:974-983`, with the `resize_object` call at `:979`.

### Rewrite-mode generated events

- `event_generation_helpers.cpp:48, 117, 149` (N11).
- Generated ROTATE, FDE and PREVIOUS_GTIDS events carry PBS's **wall clock**.

## Ways the chain order can break (failure scenarios)

1. **Retraction** (focus 7, focus 9). Bytes served to a client are later truncated and replaced by different bytes.
   - Truncation happens only in the constructor and only when data > metadata. That state arises after a failed metadata save (the ticket 18 window) followed by a kill, or after a SIGKILL between `size +=` and the `.json` rename. The second case is restricted by the lock note in the synthesis.
   - The replacement bytes must differ. That requires a changed source (ticket 25), or rewrite mode regenerating ROTATE/FDE/PREVIOUS_GTIDS with a new wall clock.
   - Plain modes take the bytes verbatim from the source (after IN_USE clearing and CRC recompute), so the replay is expected to be identical.
   - In rewrite mode, a served-then-truncated generated non-artificial ROTATE, or a new file's preamble, is regenerated later with a different timestamp and CRC. A client that already applied the first version then holds bytes that disagree with storage at the same offsets. Its resume offset points into different content.
2. **Duplicate delivery** (focus 7, focus 9). Ticket 18's destructor re-flush stores an event E twice.
   - A blocking client that read E at `[x, x+n)` later reads E again at `[x+n, x+2n)`, either in the same session or after the restarted pull serves it.
   - A position-mode downstream replica or client applies it twice.
3. **Missing ROTATE at a file switch** (focus 7, focus 9; ticket 14, N5).
   - Clients see bytes of two files without a switch marker.
   - At a magic-only next file, the sender misses the artificial ROTATE. The client then computes its resume position against the wrong file name.
   - `sender-file-switch-emits-artificial-rotate` covers the ROTATE itself. This property covers the replay consequence.
4. **Source divergence** (focus 7). After an identity change, storage ⋢ current source while clients keep receiving the old lineage.
5. **Reader/writer lock regression** (focus 2). Data is written before `size +=` under the exclusive lock, so a reader should never see a size beyond the readable bytes. It could break in these ways:
   - (a) A future change moves `size +=` or adds a sender-side getter on the unsynchronized wrapper.
   - (b) A failed metadata save leaves `size` incremented while the bytes are present. That is safe for reading, but the next retry appends duplicates (ticket 18, scenario 2).
   - (c) The S3 server lacks read-after-write consistency for overwrites (A10).
   - (d) External truncation happens, from the purge ctor (N1) or a second writer. These are covered by `other-process-startup-spares-live-writer` and `second-writer-on-storage-fails-closed`.

   Under a single-writer topology this is a regression guard. The interesting interleavings are thread pauses and CPU modulation around rotation and checkpoint.
6. **Client ahead of a recovered storage** (focus 9). The storage tail was truncated by recovery while the client holds a later position.
   - The client gets a silent close until the data is re-fetched.
   - That is a liveness issue, not duplication.

## Oracle / workload checks

### Clients

- Use `mysqlbinlog --read-from-remote-server --raw --stop-never`, or a Python COM_BINLOG_DUMP client, whose received bytes the workload stores.
- The workload client records every non-artificial event as `(file, start offset, bytes)` across all its sessions, deduplicating nothing.

### Always checks at any observation

These hold while faults are active, so this is a safety property under faults. Scope: non-rewrite, unencrypted (or decrypted) storage, single source epoch.

- `Always("pull client bytes are a prefix of stored bytes")`. Each blocking client's output, with artificial events stripped, is a byte prefix of the stored (or decrypted) binlogs.
- `Always("stored bytes are a prefix of source bytes at observation time")`.
- GTID-set chain: every client's received GTID set ⊆ the storage's GTID set ⊆ the source's `gtid_executed`. Use the message `Always("pull client GTIDs are a subset of stored GTIDs, which are a subset of source gtid_executed")`.

### Always checks at quiescence

These run after a final clean dump.

- `Always("bytes served by pull never change at the same offset after restart")`. Every stored byte at a received offset equals the received byte. No `(file, offset)` was received twice with *different* bytes.
- `Always("pull client never receives the same binlog offset twice across reconnects")`. The one exception is the event at the exact resume point, if the client deliberately re-requested it.
- `Always("union of pull client receipts equals the stored stream")`. Decrypt via a final clean dump.

### Log checks

- Pull's log never contains "requested range is beyond the end" or "does not match the length" while no other process touches the storage.
- Use the message `Always("pull log has no short-read errors in single-writer runs")`.

### Coverage

- `Sometimes("pull client resumed mid-file after reconnect")`, to avoid a vacuous pass.
- `Sometimes("pull client ahead of a recovered storage resumed successfully after re-fetch")`, the liveness twin for N12 (added on investigation).

## SUT-side instrumentation

The codebase has no Antithesis SDK, so all of these are **missing**.

| Assertion | Site | Proposed by | Status |
|---|---|---|---|
| `ALWAYS(size(result_buffer) == read_length, "sender read returns exactly the flushed length")` | `fetch_event_block` after `get_object` | focus 2 | missing |
| `UNREACHABLE("session ended by storage read exception")` | session catch block, `network_service.cpp` ~:621 | focus 2 | missing; gated to runs without S3 or storage faults (resolved: a fail-closed session drop is not a prefix violation) |
| `SOMETIMES("storage recovery truncated a tail binlog that pull had already served")` | `storage_core.cpp:979` | focus 9 | missing; recovery cannot know this directly, so the weaker proxy is `SOMETIMES(recovery truncated > 0 bytes)`, already anchored by recovery-paths-exercised (reference it) |
| `REACHABLE("metadata save failed after data append; in-memory size is ahead of persisted size")` | exception path of `write_event_block`, `storage_core.cpp:309-321` | focus 7 (replaces the awkward `ALWAYS(metadata_saved \|\| !readers_can_see_size)`) | missing; overlaps failed-flush-never-reappends-data's `SOMETIMES(metadata save failed after a successful data append)` at the same site, so reference that one rather than adding a second |
| `SOMETIMES(requested_offset > flushed_size, "dump requested past the flushed size")` | `network_service.cpp:308-313` | focus 7 | missing; reached when a client is ahead of a recovered PBS |
| `SOMETIMES("reader and writer contend: shared_lock waited while unique held")` | `fetch_event_block`, try_lock probe | focus 2 | missing; exploration hint |

## Antithesis angle / fault requirements

- Partitions on each pair independently: source↔PBS, PBS↔S3, PBS↔client. Also network resets between client and pull.
- PBS kills mid-checkpoint, aimed between the data append and the metadata save. That window is short, and Antithesis kill timing is good at hitting it. A thread-pause fault makes the window wider.
- S3 5xx on `.json` PUTs (a custom fault or proxy), plus S3/disk faults on the collector, to open the ticket 18 window.
- Rewrite mode with a small `file_size`, so generated ROTATEs are frequent.
- Several concurrent clients that disconnect and resume at their last received (file, pos).
- A small `checkpoint_size`, so the sender reads a growing tail.
- Thread pauses and CPU modulation around rotation and checkpoint.

## Investigation findings (2026-10-13)

These answers come from the investigation log at the bottom of this file.

- **Duplicate delivery (scenario 2) counts as a violation.** The duplicate is already a storage defect that the team filed (ticket 18, High, open). Pull serving it is a consequence of that defect, not a client-tolerance question. The duplicate copy sits at *different* offsets `[x+n, x+2n)`, so `Always("pull client never receives the same binlog offset twice across reconnects")` does **not** catch it. `Always("stored bytes are a prefix of source bytes at observation time")` catches it, and so does the client ⊑ source chain. Triage it as a known failure keyed to ticket 18.
- **No real MySQL replica downstream.** This is confirmed in code. `known_queries` (`network_service.cpp:503-554`) answers only the mysqlbinlog query strings; every other query gets a syntax error (`:575-584`). `COM_BINLOG_DUMP_GTID` is parsed (`connection_context.cpp:446`) but falls through to "unknown command" (`network_service.cpp:607-616`). The property uses mysqlbinlog or a custom COM_BINLOG_DUMP client until ticket 20 is fixed.
- **Rewrite-mode retraction is reachable, and only rewrite mode makes it contradictory.**
  - The generated non-artificial ROTATE is written and then flushed on its own by `close_binlog` (`collector_context.cpp:377-386, 484-488`; `storage.cpp:202-213`).
  - The new file's FDE and PREVIOUS_GTIDS are buffered and flushed at the next checkpoint. That can be on their own, under interval checkpointing or the timeout flush (`collector_context.cpp:200`).
  - All three carry `ctime_timestamp::now()` (`event_generation_helpers.cpp:47-48, 117, 149`).
  - When a metadata save fails on such a flush (size raised, lock released, bytes servable) and the process then dies without a successful save, recovery truncates the range (`storage_core.cpp:974-983`).
  - On reconnect in rewrite mode the first transaction goes straight into the current file, because the `is_fresh()` branch generates no non-artificial ROTATE (`collector_context.cpp:353-386`). A served ROTATE at `[x, x+r)` can therefore be replaced by **transaction bytes** at the same offset. A regenerated preamble differs in timestamp and CRC.
  - In plain (position and GTID) modes, re-fetched bytes are taken verbatim from the same source, so retraction followed by re-fetch is byte-identical and no contradiction arises.
  - Whether rewrite mode is *allowed* to do this is still open (see below). The quiescence check "bytes served by pull never change at the same offset after restart" stays scoped to non-rewrite storage.
- **Served vs durable sizes.** Under the lock, a reader sees a raised `size` only after `save_binlog_metadata` has returned, or after it has thrown: `write_event_block` has no rollback (`storage_core.cpp:299-322`), and `fetch_event_block` takes `shared_lock` and clamps to `record.size` (`:465, 513-541`). So "served but not metadata-durable" exists only after a failed metadata save. The README has no statement about pull serving semantics. README:721 covers only `kill -9` losing progress.
- **Transient S3 GET error in a session.** The exception propagates out of `handle_binlog_dump_command` to the session catch (`network_service.cpp:620-623`), which logs the error and closes the socket. The client receives no wrong bytes, so this is fail-closed and not a prefix violation. Decision: the `UNREACHABLE("session ended by storage read exception")` is gated to runs without S3 or storage faults. An `ALWAYS` on the bytes stays global.
- **mysqlbinlog resume.** mysqlbinlog has no reconnect loop. After a disconnect it exits (MySQL client behaviour; the team's own tickets 21 and 26 describe the client as "restarted with --start-position"). The resume position is therefore whatever the workload passes. With `--raw`, that is the size of the last raw file written. Either mysqlbinlog restarted by the workload or a custom client can serve as the oracle client. The workload must retry connections refused during PBS startup (ticket 26 / PBS-96).
- **Client ahead of the storage.** For a non-empty storage, an offset past `record.size` makes `fetch_event_block` return false (`storage_core.cpp:513-515`). The session then `co_return`s with an error log line and no error packet (`network_service.cpp:308-313`). Once the data is re-fetched, a reconnect at the same offset succeeds. In plain modes that offset is still a boundary, because the bytes are identical.

**Property changes.**
- The invariant and the assertion types are unchanged.
- Duplicate delivery is attributed to the prefix-of-source check, not to the "same offset twice" check.
- Add the liveness twin `Sometimes("pull client ahead of a recovered storage resumed successfully after re-fetch")`. It turns N12 into an explicit exploration target. The safety verdict on silent close stays open.
- The `UNREACHABLE` on session read exceptions is gated to fault-free storage runs.
- The rewrite-mode retraction is a confirmed mechanism. It is excluded from the main Always pending a maintainer decision, and a rewrite-only variant `Always("rewrite-mode bytes served by pull never change at the same offset after restart")` can be enabled once that decision is made.

## Open questions

- Is regenerating different bytes (a ROTATE replaced by transaction bytes, or a preamble with a new timestamp) at an already served offset acceptable in rewrite mode? `(needs human input)`
- Should the sender serve only metadata-durable sizes, which would close the failed-save window? `(needs human input)`
- Is TSan worth adding as an Antithesis image variant? No TSan option exists today, only `WITH_ASAN`. `(needs human input)`
- Is a silent close without an error packet (N12) acceptable for a client ahead of a recovered storage, given that reconnect later succeeds? `(needs human input)`

### Investigation Log

#### Is a position-mode client expected to tolerate duplicate delivery after a PBS write failure?

- Examined: `storage.cpp:276-302`, `storage.cpp:68-77`, `storage_core.cpp:299-322`; ticket 18 in `.tickets-snapshot.txt:56`.
- Found: the duplicate is created in storage (no rollback after `size +=`, buffer erased only on success, destructor re-flushes). The team filed this as ticket 18 (High, open), so the stored duplicate itself is a recognised defect, independent of what a client tolerates. The duplicate occupies new offsets, so a client never sees the *same* offset twice. It sees the same transaction at two offsets.
- Not found: nothing suggests the served duplicate is intended.
- Conclusion: resolved. This counts as a violation, caught by the prefix-of-source check, and is a known failure under ticket 18.

#### Can the harness run a real MySQL replica downstream?

- Examined: `network_service.cpp:503-616`, `connection_context.cpp:440-446`; ticket 20 (`.tickets-snapshot.txt:1084-1139`).
- Found: only exact mysqlbinlog query strings are answered, and anything else gets a syntax error. `COM_BINLOG_DUMP_GTID` is parsed but answered "unknown command". No heartbeats are sent. These are the discriminating details for ticket 20's claim that a replica stops at its second query.
- Conclusion: resolved. No real replica is possible today; use mysqlbinlog or a custom client.

#### Is regenerating different ROTATE/FDE bytes for an already served range acceptable in rewrite mode?

- Examined: `collector_context.cpp:274-428, 430-490`, `event_generation_helpers.cpp:38-150`, `reader_context.cpp:570-600`, `storage.cpp:142-213`, `storage_core.cpp:299-322, 960-983`, README rewrite section (:592-595).
- Found: the mechanism is real (see the Investigation findings). The generated ROTATE, FDE and PREVIOUS_GTIDS use `now()`. A failed metadata save plus a death before a successful save lets recovery truncate a served generated range. After reconnect the `is_fresh()` branch writes the next transaction where the ROTATE was.
- Not found: any README or code statement on whether pull clients may observe rewritten history.
- Conclusion: `(needs human input)`. Maintainer intent decides whether the rewrite variant is a safety property.

#### Should the sender serve only metadata-durable sizes?

- Examined: `storage_core.cpp:299-322, 436-551`, README:721 and the README checkpoint section (:619-670).
- Found: the lock makes the normal path safe. Only a thrown `save_binlog_metadata` exposes a non-durable size. The README is silent on pull semantics.
- Conclusion: `(needs human input)`. This is a design decision. In plain modes the retraction is benign (identical re-fetch); in rewrite mode it is the previous question.

#### Does recovery ever truncate a range that contains a rewrite-mode generated event?

- Examined: as for the rewrite question above, plus `collector_context.cpp:197-200` (timeout flush) and `storage.cpp:180-199` (checkpoint conditions).
- Found: yes. The non-artificial ROTATE is always flushed alone by `close_binlog`. FDE and PREVIOUS_GTIDS can be flushed alone (interval checkpoint, timeout flush) or together with the first transaction (size checkpoint). Any of these flushes can hit a failed metadata save. A new file starts with metadata size 4 (`storage_core.cpp:680-702`), so truncation back to 4 regenerates the whole preamble.
- Conclusion: resolved. The impact is not limited to timestamp and CRC: a truncated ROTATE is replaced by transaction bytes.

#### Should a session dropped by a transient S3 GET error count as a failure?

- Examined: `network_service.cpp:233-277, 279-334, 620-623`, `storage_core.cpp:543-546`; `s3_storage_backend.cpp` has no custom retry configuration, so the SDK defaults apply.
- Found: the exception closes the session with only a log line. No partial or wrong bytes are sent for that block.
- Conclusion: resolved as a harness decision. Fail-closed is not a prefix violation, so the `UNREACHABLE` is gated to runs without S3 or storage faults.

#### Is TSan worth building as an Antithesis image variant?

- Examined: `CMakeLists.txt:62-72`, `CMakePresets.json`, `.github/workflows/cmake.yml`.
- Found: only `WITH_ASAN` exists. There is no TSan option or preset, so adding one is new build work.
- Conclusion: `(needs human input)` on cost versus benefit.

#### What resume position does real `mysqlbinlog --stop-never` use after a failure?

- Examined: `mtr/binlog_streaming/t/rs_blocking_dump*.test`, `rs_start_position.test`; tickets 21 and 26 (`.tickets-snapshot.txt:1171, 1429`).
- Found: mysqlbinlog does not reconnect. The repo's tests and tickets always restart it, with an explicit `--start-position` when resuming. Ticket 26: resuming at a position other than 4 before the first ROTATE is stored gets disconnected.
- Not found: in-repo proof of mysqlbinlog's internal behaviour. This relies on known MySQL client behaviour, and the tickets corroborate it.
- Conclusion: resolved. The workload controls the resume position, and either client works as the oracle, with retry around PBS startup.

#### Is a silent close (N12) acceptable for a client ahead of the storage, as long as a later reconnect succeeds?

- Examined: `storage_core.cpp:513-515`, `network_service.cpp:308-313`; tickets 21 and 26 "Expected behaviour".
- Found: the session closes without an error packet. A reconnect after re-fetch succeeds in plain modes. Ticket 21 expects an error (as a MySQL source returns ER_SOURCE_FATAL_ERROR_READING_BINLOG) for unstored positions, and ticket 26 expects waiting for an empty storage. Neither ticket covers a non-empty storage that is behind the client.
- Conclusion: `(needs human input)` for the safety verdict. Regardless of the answer, the liveness twin `Sometimes("pull client ahead of a recovered storage resumed successfully after re-fetch")` is added.

## Evaluation refinements (2026-10-13)

Applied by the evaluation synthesis (`../evaluation/synthesis.md`). Where this section and earlier text in this file disagree, this section wins; the catalog entry has been updated to match.

- The `UNREACHABLE("session ended by storage read exception")` that was valid "only in runs without S3 or storage faults" becomes `ALWAYS(!ended_by_storage_exception || storage_faults_flag)`, with the flag read from the harness timeline-tag file that the proxy and fsfault drivers set. Antithesis tallies an assertion by its message across every timeline of a run, so an SUT message cannot be scoped to a template (wildcard W1).
- "Client ⊑ stored" needs stored bytes from `pbs` while client captures live in `workload`; the shared read-only storage helper in `pbs` is a topology change (implementability C2).
- Runs in `pull_fs`, `s3` (pull-like).
