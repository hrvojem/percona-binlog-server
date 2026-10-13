# graceful-exit-leaves-flushed-storage — fetch exit 0 and signal-initiated stops leave a fully flushed, consistent storage

Found by: focus 1, 2, 5, 8 (data integrity, concurrency, resource, lifecycle).
Merged from: fetch-exit-zero-implies-tail-flushed (focus 5), fetch-exit-zero-implies-complete-copy (focus 1), graceful-stop-leaves-recovery-free-storage (focus 8), sigterm-mid-write-leaves-boundary-storage (focus 2).

## Synthesis notes

- **Type:** Safety. **Priority:** P0.
- **Shared mechanism.** The final flush runs in `~storage` (`storage.cpp:68-77`), and every exception it raises is swallowed. By the time it runs, the operation has already decided it succeeded:
  - `fetch_operation.cpp:80-115` sets the result first.
  - `collector_context.cpp:184-187` returns at EOF without flushing.
  - `pull_operation.cpp:168-171` logs "successfully shut down" first.
- **Workload checks.** Three workload `Always` checks, each with its own message:
  - **(a) fetch exit 0.** The storage holds every source transaction committed before the fetch started. This is QA's `check_complete_copy`, relaxed to a source snapshot taken at fetch start, with the magic-only tail exception.
  - **(b) pull exit 0 after exactly one SIGTERM/SIGINT.** There are no `*.tmp` objects, and every binlog object's size equals its `.json` size.
  - **(c) any single signal at an arbitrary instant, to fetch or pull, whatever the exit code.** The storage opens, every binlog is a source prefix, and the tail sits on a transaction boundary (README:715).
- **SUT-side checks.**
  - `UNREACHABLE("final storage flush failed after the operation reported success")`. It goes in the operation layer, after the storage is destroyed, and is gated on `result == true`. This requires surfacing the swallowed failure.
  - Do **not** put an unconditional UNREACHABLE in the `storage.cpp:72-75` catch. `recovery-paths-exercised` keeps a REACHABLE anchor there for the legitimate after-exception case.
  - `SOMETIMES(signal arrived while collector inside write_event_block)`.
- **Why P0.** Contributors rated it differently:
  - focus 5: P0;
  - focus 1: P1 (reachability under tenant faults is medium);
  - focus 8 and focus 2: P1.

  It is resolved to P0 for two reasons. Cron-driven fetch exit 0 is the primary signal that a backup succeeded. The trigger is an ordinary network or disk fault at the end of a run, with no process faults needed.

## What led here

- **Lead N8**, raised by focus 3, 5 and 8: exit 0 after a failed final flush. Focus 5 reached it from the disk-full analysis of the fetch exit path.
- **The implicit exit-code contract** (sut-analysis "Claimed safety guarantees"): "fetch exit 0 ⇒ the storage is an exact copy of the source at that time".
  - `fetch` exits 0 only after reaching EOF (`fetch_operation.cpp:107-115`).
  - Operators, for example through cron, read exit 0 as "the storage now holds everything the source had".
  - The QA harness asserts `check_complete_copy()` after a successful fetch (`extra/qa/fetch_crash_test.py:671-697`).
- **Ticket 25:** the QA reproduction reports exit 0 with bytes differing from offset 4.
- **README:713-721 (lifecycle focus):** SIGINT/SIGTERM terminate fetch and pull "leaving storage in consistent state". Only `kill -9` "may result in losing some progress". README:373 makes the same claim for errors ("immediate termination").
  - Startup recovery exists only for the improper-shutdown case. The constructor logs "found temporary storage object … left after improper shutdown" (`storage_core.cpp:607-609`) and "recovered binlog file … by truncating it" (`:980-983`).
  - So the system has an explicit, observable distinction between a clean and an unclean previous stop. After a graceful stop, neither recovery branch should ever fire.
- **Concurrency focus (signal handling vs in-flight writes):** the signal is handled on the main thread while the write runs on the collector jthread. The handoff between the two is asynchronous.

## Code paths (verified at 99de682)

### End of stream (EOF)

- **`collector_context.cpp:172-187`.** At the source EOF (non-blocking dump), case (3) returns `true` (`:183-186`). It does this **without** flushing the event buffer and without closing the binlog. Only the timeout case (2) flushes (`:195-200`): it calls both `discard_incomplete_transaction_events()` and `flush_event_buffer()`. At EOF the tail binlog is still open, and the buffer may hold unflushed complete transactions (there is no checkpointing by default).
- **`fetch_operation.cpp:80-115`.** It runs `operation_result = collector_ctx.receive_binlog_events(io_ctx)` and sets `result = true` ("successfully fetched everything") at `:112-115`. This happens **before** `collector_ctx`, which owns the `storage` shared_ptr (`:84-86`), leaves scope at the end of the `try` block.
- **`storage.cpp:68-77` `~storage`.** It calls `flush_event_buffer()` inside `catch (...) {}`. Any ENOSPC, S3 5xx, partition error, or `.json.tmp` deleted by a concurrent purge during this last write is swallowed, and the process returns exit 0 (N8).
- **How much is at stake.** With the default config (no `checkpoint_size` or `checkpoint_interval`), the final flush carries the whole active binlog since the last rotation. With checkpointing, it carries the transactions since the last checkpoint.

### Signals

- **Handlers.**
  - `fetch_operation.cpp:69-75` (~:74) and `pull_operation.cpp:98-104` (~:100) install an asio `signal_set` whose handler only calls `io_ctx.stop()`.
  - The collector checks `io_ctx.stopped()` only at the top of the event loop (`collector_context.cpp:151-153`). A `write_event`, its checkpoint flush and the backend write therefore all run to completion first. There is no cancellation inside storage, which is good.
  - On the signal path, the collector returns `false` at `:178-183` **without** calling `discard_incomplete_transaction_events()` or `flush_event_buffer()`.
- **Where `~storage` runs.**
  - **fetch:** `storage` is owned by `collector_ctx` (`fetch_operation.cpp:84-86`). It is destroyed at the end of the `try` block, on the main thread after the jthread join, and after the result has been decided.
  - **pull:** the owners are the local `storage` (`pull_operation.cpp:109`), `collector_ctx`, `network_service`, and any suspended session coroutine frame (`sender_context`). The last owner may be destroyed inside `~io_context` (declaration order `:98-118`). That happens after `signal_set` is destroyed, so a second SIGTERM kills the flush with the default action.
  - In both cases pull logs "successfully shut down after receiving a termination signal" and sets `result = true` (`:168-171`) **before** the final flush runs.
- **Exit codes.** Fetch returns non-zero on a signal ("did not reach EOF"). A signal racing EOF also returns non-zero, because the loop ends with `fetch_result` true but `stopped()` is checked first. That is benign.

### Write path invariants

- `flush_event_buffer_internal` (`storage.cpp:276-302`) writes only up to `last_transaction_boundary_position_in_event_buffer_`. An interrupted transaction should therefore never land.
- `write_event_block` (`storage_core.cpp:299-322`) appends the data, bumps `size`, then saves `<name>.json`.
- **A graceful stop never interrupts a rotation midway.** Rotation (`close_binlog` → `open_binlog` → `open_new_binlog_file_internal`, `storage_core.cpp:680-703`) runs inside one `process_binlog_event` call, and the stop flag is checked only between events. The sequence magic → .json → index cannot be interrupted by the first signal. A *second* signal or a SIGKILL can, and that is the crash domain (ticket 15, kill-anywhere-storage-reopens).

### Ticket 25 (source identity)

- `storage_metadata` has no source identity (`storage_metadata.hpp:35-37`).
- After RESET BINARY LOGS or a failover, a position-mode resume passes the artificial ROTATE position check whenever the new file is long enough and the first event boundary lines up (`reader_context.cpp:547-556`, `:905-928`).

## Failure scenarios

1. **[trigger: fetch EOF + backend fault]** The storage volume is almost full, or S3 is partitioned, returns 5xx, or is slow when fetch reaches EOF.
   - Fetch logs "successfully fetched everything and disconnected" and exits 0, but the tail transactions never reach the backend.
   - If the metadata save fails after the data append, data is appended but the metadata is stale, and the next start truncates.
   - A cron wrapper believes the backup is current. The next run re-fetches the tail only if the source still has it. Source binlog purging is exactly what this backup tool exists to protect against.
2. **[trigger: graceful signal + backend fault]** The final flush fails (S3 partition or 5xx at the moment of shutdown, or ENOSPC).
   - Data may have been appended without the metadata being saved, so data size > metadata size. The next start truncates (recovery fires) although the previous run exited 0 (pull) or "by signal" (fetch).
   - On the filesystem backend, the stale `.json.tmp` is also left behind.
   - The exit code told the operator nothing was wrong.
3. **[trigger: ticket 18 inside shutdown]** A checkpoint flush failed just before the signal. `~storage` re-flushes and re-appends, and this time the metadata save succeeds.
   - Recovery does not fire, but the storage contains a duplicate transaction.
   - This case is covered by `failed-flush-never-reappends-data`. This property catches only the variant where the final metadata save also fails.
4. **[trigger: second SIGTERM during the pull final flush]** The flush runs in `~io_context` after `signal_set` is destroyed, so the default disposition kills the process mid-flush.
   - This turns a graceful stop into a crash, and recovery fires on the next start.
   - `sigterm-not-starved-by-storage-lock` (now merged into `sigterm-honoured-within-bound`) notes the same window.
   - Supervisors make this realistic: docker stop sends SIGTERM, then SIGKILL after 10 s.
5. **[trigger: signal at an arbitrary instant vs the writer thread]** The signal-vs-writer-thread handoff, plus the `~storage`-on-the-wrong-thread or late path. After any single SIGTERM/SIGINT, the storage must still:
   - open in streaming mode;
   - hold every binlog as a byte prefix of the source;
   - have the tail's metadata size equal to its file size, on a transaction boundary.

   If pull exits 0, the storage must also contain every transaction the collector had received up to its last complete transaction.
6. **[trigger: source identity change, ticket 25]** The source ran RESET BINARY LOGS or was switched to a different server between fetches. Fetch exits 0 with foreign bytes.
7. **[trigger: source writes during fetch]** A fetch exit 0 must cover at least everything committed before the fetch started.

## Oracle / workload checks

**Before each fetch**, record the source's `SHOW BINARY LOG STATUS` position and `gtid_executed`. Then either stop source writes for that fetch, or record the position after fetch returns and require that it contains the before-snapshot.

**(a) `Always("fetch exit 0 implies storage holds every source transaction committed before fetch start")`.** Use `check_complete_copy()` restricted to the recorded snapshot:
- The stored index ⊇ the source binlogs (from the storage's first file) as of fetch start.
- For each file that existed at fetch start, stored bytes `[0, size)` equal source bytes `[0, size_at_start)` at least (a prefix of length ≥ size_at_start), with the magic-only tail exception of `check_complete_copy`.
- GTID mode: stored GTIDs ⊇ the source's `gtid_executed` at fetch start (minus `gtid_purged` at the first fetch).

**(b) `Always("pull exit 0 after one graceful signal leaves no tmp objects and object sizes equal metadata sizes")`.**
- Record that the previous stop was graceful: the workload sent exactly one SIGTERM/SIGINT, the process exited on its own, and the log contains "terminated by signal" or "successfully shut down after receiving a termination signal".
- Before restarting, list the storage directly (filesystem or S3 ListObjects) and check that there is no `*.tmp` object and that every binlog's object size == `<name>.json.size`.
- Gating on "graceful" keeps kill-initiated stops out.
- Optional check on restart: `Always("startup after a graceful stop logs no improper-shutdown recovery")`, meaning no "left after improper shutdown" and no "recovered binlog file".

**(c) `Always("storage consistent after a single graceful signal at an arbitrary instant")`.**
- Send SIGTERM/SIGINT at random, Antithesis-chosen times to fetch or pull.
- Regardless of the exit code, check `storage_holds_prefixes() && tail_on_boundary` and that the storage opens in streaming mode.
- Optionally send a second SIGTERM shortly after, to mimic supervisors. Runs with a second signal are excluded from (b) and the exit-0 clause (see the open questions).

**Window anchors:** `Sometimes(final flush in ~storage threw)` (via the SUT-side marker below), so we know the window was hit.

**Overlap.** Other foci cover errors and kills with their own properties:
- kill-anywhere-storage-reopens;
- failed-flush-never-reappends-data;
- stored-transactions-exactly-once-across-resumes.

This property is specifically about the success-reporting path and the signal handoff.

## SUT-side instrumentation suggestions

The codebase has no Antithesis SDK, so all SDK assertions below are missing.

| Assertion | Site | Status |
|---|---|---|
| `UNREACHABLE("final storage flush failed after the operation reported success")` | Operation layer (`fetch_operation.cpp` after the `try` block that destroys `collector_ctx`; `pull_operation.cpp` after the last storage owner is gone). Gated on `result == true`. Needs `~storage` to record the swallowed failure in a flag the operation can read. | missing |
| `ALWAYS("event buffer is empty when fetch returns success")` | `fetch_operation.cpp` before `result = true` (`:112-115`). This documents the fix: move the final flush before `result` is decided. | missing |
| `SOMETIMES("final flush in ~storage threw")` | `storage.cpp:72-75` catch. This is an exploration anchor only. `recovery-paths-exercised` owns `REACHABLE("storage destructor swallowed a flush exception")` at `storage.cpp:74`; implement one call there and reference that one. | missing |
| `SOMETIMES("signal arrived while collector inside write_event_block")` | A flag set inside the unique-lock section of `write_event_block` (`storage_core.cpp:299-322`) and read from the signal handler. | missing |
| `REACHABLE("startup removed tmp objects")` / `REACHABLE("startup truncated tail")` | `storage_core.cpp:625` / `:979`. These are owned by `recovery-paths-exercised` (they came from `recovery-paths-exercised`), so reference them rather than duplicating. | missing |

These contributor proposals were folded into the rows above:
- `UNREACHABLE("~storage swallowed a flush failure")` inside the catch at `storage.cpp:74`, from focus 5. It was rejected as an unconditional check at that site per the synthesis notes, and moved to the operation layer.
- Focus 5's alternative, an `ALWAYS(false)`-style marker only in streaming mode with a non-empty buffer, is subsumed by the gated UNREACHABLE.
- `UNREACHABLE("final storage flush failed but fetch reports success")` with a flag, from focus 1.
- `ALWAYS(!final_flush_failed || !result, "graceful stop reports success only after a successful final flush")` in pull/fetch before returning, from focus 8. It is the same condition as the operation-layer UNREACHABLE, so it is kept once.

## Key observations

- No cancellation exists inside storage, so a single graceful signal never splits a write or a rotation. The risk lies entirely in the late, exception-swallowing destructor flush and in the result being decided before it.
- The pull destruction order (`pull_operation.cpp:98-118`) decides which thread and which point in time the final flush runs at, and whether a second signal can kill it. This is not a deliberate design.

## Fault requirements

- A full volume: a filler task or `fsfault` (see `failed-flush-never-reappends-data`, which absorbed `enospc-write-fails-closed-without-duplicates`).
- A network partition or latency between PBS and MinIO, timed around EOF or around the SIGTERM. Antithesis network faults on the PBS↔MinIO link are enough.
- S3 5xx via `s3_fault_proxy.py`.
- Pause faults on PBS increase the chance that the signal lands mid-flush.
- Workload-sent SIGTERM/SIGINT at random times, optionally followed by a second one.
- No node termination is needed.

## Open questions

- **Is "fetch exit 0 ⇒ complete copy at EOF" an intended contract, or does exit 0 only mean "reached EOF on the wire"?** `(needs human input)`
  - If the contract is not intended, scenario 1 is a documentation issue. The property weakens to "exit 0 ⇒ storage consistent and openable", or to "exit 0 ⇒ the *next* fetch adds nothing that was committed before the first fetch started" (weaker, still checkable).
  - This decides whether check (a) and the operation-layer UNREACHABLE stay.
- **Is exit 0 after a failed final flush (fetch at EOF, pull after a signal) an intended best-effort flush, or a bug (N8)?** `(needs human input)`
  - If exit 0 is intended, the property weakens to "the storage still opens and resumes", and the oracle drops the exit-code gate (check (b) and the exit-0 clause of (c) are dropped). If it is a bug, the SUT-side check is the right one.
- **Should a second SIGTERM during the pull final flush be ignored, or should it escalate?** `(partial: confirmed the flush runs after signal_set is gone, so a second signal kills it, but only when a COM_BINLOG_DUMP session is live at shutdown; intent is a maintainer decision)`
- **Should the workload perform a source identity change (ticket 25)?** `(needs human input)` It needs a workload action (RESET BINARY LOGS AND GTIDS, or swapping the source container). If the team considers it out-of-contract misuse, drop scenario 6.

## Investigation results (2026-10-13)

- **Reachability under the planned faults is settled.** The final flush can fail in the test without any new shim:
  - The topology puts `s3_fault_proxy.py` in front of the S3 gateway, so test commands can fail six consecutive PUTs on demand.
  - QA has shown that six failed PUTs outlast the SDK retries and that a single failure is retried transparently (ticket 18; `extra/qa/s3_fault_sweep.py:21-23`).
  - A pure Antithesis partition on PBS↔S3 also reaches it, provided it outlasts the CRT retry/timeout budget. `s3_storage_backend.cpp` sets no retry or timeout options, so the budget is the SDK default. Its exact value was not verified, which only affects how often plain partitions hit the window.
- **The pull destruction-order claim is refined.**
  - The listener coroutine holds the storage only by reference (`network_service.cpp:629-637`), and so does `session` (`:344-350`). Only `sender_context` keeps a `storage_ptr` copy (`sender_context.hpp:61`), and only a live COM_BINLOG_DUMP session creates one (`network_service.cpp:301-303`).
  - With no dump session live, the last owner is the local `storage` (`pull_operation.cpp:109`). It is destroyed before `signals` (`:99`), so the flush runs while the handler is still installed and a second signal is queued harmlessly.
  - With a dump session live, the last owner is that session's frame, destroyed inside `~io_context`. That is after `~signal_set`, which restores `SIG_DFL` (Boost.Asio `signal_set_service::destroy` → `clear`, `signal_set_service.ipp:305-311, 500-523`). A second SIGTERM then kills the flush.
  - Fetch is not affected: `collector_ctx` (the only owner) is declared after `signals` (`fetch_operation.cpp:70, 84`), so the flush runs with the handler still installed.
  - **Implication.** The second-signal arm of check (c) only needs live pull readers (the workload already runs them) to reach the kill window. Runs with a second signal stay excluded from (b).
- **Maintainer precedent (context for the open intent questions).** Commit 18e365d (PS-10320, maintainer) added an explicit flush on pull's read-timeout disconnect, which is case (2) at `collector_context.cpp:195-200`, and called it "the third kind of checkpointing". So the maintainers treated "buffered data not uploaded at disconnect" as a bug. The EOF case (3) and the signal case (1) were left to the exception-swallowing `~storage`, whose `catch (...)` dates back to the first S3 backend (07b8637). No comment says that swallowing is intended.
- **Property unchanged.** The type, assertions and priority stay as they are, pending the intent questions above.

### Investigation Log

#### Is "fetch exit 0 ⇒ complete copy at EOF" an intended contract?

- Examined: README:372-373 ('fetch' mode), README:711-721 (graceful termination), `fetch_operation.cpp:80-123`, `collector_context.cpp:178-204`, `storage.cpp:68-77`, `git log -L68,77:src/binsrv/storage.cpp`, commit 18e365d.
- Found:
  - README:372 says fetch reads "all available binary logs" and exits after the last event. README:373 says any error (including out of space) "results in immediate termination". Neither mentions exit codes.
  - The code sets `result = true` (`fetch_operation.cpp:112-115`) before `collector_ctx`, and with it the storage, is destroyed. The EOF branch returns without flushing (`collector_context.cpp:185-187`).
  - The empty `catch (...)` in `~storage` has existed since 07b8637. 18e365d added a flush only for the timeout branch.
- Not found: any doc, comment or test that states what exit 0 promises about stored data.
- Conclusion: `(needs human input)`. Only the maintainers can say whether exit 0 is meant as a data guarantee.

#### Is exit 0 after a swallowed final-flush failure acceptable (N8)?

- Examined: `pull_operation.cpp:160-175`, `storage.cpp:68-77`, README:715-721, ticket 18.
- Found: pull logs "successfully shut down" and sets `result = true` before any storage owner is destroyed. The flush failure is invisible to the exit code. README promises a "consistent state" on SIGINT/SIGTERM and allows lost progress only for `kill -9`.
- Not found: any statement on whether a failed final flush should change the exit code.
- Conclusion: `(needs human input)`.

#### Should a second SIGTERM during the final flush be in scope, ignored, or escalate?

- Examined: `pull_operation.cpp:98-118` (declaration order), `network_service.cpp:629-686` (listener/session parameters), `sender_context.hpp:44-61`, Boost.Asio `signal_set_service.ipp` (1.87 copy; same logic in 1.90), `fetch_operation.cpp:69-86`.
- Found:
  - `~signal_set` restores `SIG_DFL`.
  - In pull, the final flush runs after `~signal_set` only when a dump session's `sender_context` holds the last `storage_ptr`. Otherwise it runs before.
  - In fetch, it always runs before.
- Not found: the maintainers' intent for repeated signals.
- Conclusion: `(partial: ...)`. The mechanism is confirmed and narrowed to "pull with a live dump session". The policy remains a maintainer decision.

#### Should the workload perform a source identity change (ticket 25)?

- Examined: README "Resuming previous operation" (README:709-711), `storage_metadata.hpp`, ticket 25.
- Found: the storage records no source identity, and the README says nothing about changing sources.
- Not found: any statement of whether pointing an existing storage at another source is supported.
- Conclusion: `(needs human input)`.

#### Do Antithesis' standard faults ever make the final flush fail?

- Examined: `s3_storage_backend.cpp:180-260` (client construction; no retry or timeout options), `:850-863`, deployment-topology.md (s3 component), ticket 18, `extra/qa/s3_fault_sweep.py:17-50`.
- Found:
  - Every flush is a whole-object PUT plus a `.json` PUT.
  - QA observed that single failures are retried transparently, and that six consecutive failures surface as an exception.
  - The topology includes the fault proxy, so the failure can be injected deterministically. Long partitions also reach it.
- Not found: the SDK 1.11.774 default retry count and timeouts (the SDK source is not in the repo).
- Conclusion: resolved. The window is reachable with the planned harness, and the exact CRT budget only tunes partition length.
