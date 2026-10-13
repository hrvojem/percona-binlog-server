# second-writer-on-storage-fails-closed — Overlapping writers never yield a silently corrupt storage (ticket 24)

Found by: focus 2 (concurrency) and focus 7 (distributed coordination: zombie-leader / split-brain).
Merged from: second-writer-on-storage-fails-closed (focus 2), zombie-writer-takeover-fails-closed (focus 7).

## Synthesis notes

- **Type and priority.** Safety, P0. Both contributors agree.
- **Main workload check.** `Always(no process exits 0 unless the storage afterwards opens and every binlog is a source byte prefix ending on a transaction boundary)`.
  - It is evaluated after both writers stop and one clean fetch has run.
  - Stricter optional variant: `Always(second writer refused)`, for the case where the owners rule overlap out of contract.
- **Proof of overlap.** `Sometimes(both writers simultaneously past storage construction)`.
- **SUT-side checks:**
  - Validator site `storage_core.cpp:974-977` ("metadata bigger than data"): owned by `kill-anywhere-storage-reopens` (`UNREACHABLE("streaming/purging open: tail metadata size exceeds data size")`). This property references that assertion and adds no second SDK call at the site (synthesis decision: one owner per site).
  - `UNREACHABLE("data file grew beyond in-memory size")`.
  - A ticket-24-specific `REACHABLE` on the startup tail truncation at `:979`, with its own message.
- **Why the truncation anchor gets its own message.** `recovery-paths-exercised` also anchors the `:979` site, with a generic message. A message such as "...while another writer was live" would be ideal, but that condition cannot be observed in-process.
- **Triggers.**
  - Overlapping fetch/fetch or fetch/pull runs launched by the workload, as from a cron overrun or a supervisor double start.
  - Zombie takeover: pull #1 is hung or partitioned, the workload starts #2, then #1 resumes.
- **S3 and server_id behaviour.** On S3, the data object and the `.json` follow last-writer-wins independently. Instances with the same `server_id` also evict each other's dump threads.
- **No node termination is needed.**

## What led here

- **Concurrency focus: several OS processes on one storage.** Ticket 24 (open, High) reports that two fetch instances corrupt a storage. The QA harness `extra/qa/concurrent_storage_test.py` reproduces it on file storage.
- **Validation in code.** The sut-analysis bug-history table records ticket 24 as validated in code. Both contributors re-traced it at 99de682:
  - There is no fencing primitive anywhere in `src/`: no flock, lock file, O_EXCL or pid file.
  - The temp name is deterministic, `<name>.tmp` (fixed suffix, `basic_storage_backend_fwd.hpp:56-60`).
  - The constructor truncates the tail (`storage_core.cpp:974-983`).
- **A competing explanation was ruled out.** It was that "the harness reused one storage path by mistake and the reported corruption is a harness artefact". The reported corruption is "stored file 44 bytes longer than the source's, diverging at the source's end". That is exactly what two appenders produce when they resume from the same metadata size with `app`-mode streams. Validated.
- **The zombie-leader angle (focus 7).** This is the split-brain analogue inside PBS's own tier. A supervisor or cron starts a new writer on a storage while the previous one is still alive but unreachable or hung.
- **Realistic triggers:**
  - a cron `fetch` overrunning into the next run;
  - a supervisor double-starting `pull`;
  - an operator running `fetch` beside a live `pull`;
  - a health-check-driven failover to a new pull while the old one is hung.

## Code paths (re-traced at 99de682)

1. **Constructor.** The `storage_core` ctor (`storage_core.cpp` ~:120-220) does three things:
   - runs `list_objects`;
   - runs `remove_temporary_objects` (`storage_core.cpp:597-630`), which deletes every `*.tmp` unless the mode is `querying_only`. That includes a live writer's in-flight `.json.tmp`;
   - runs tail recovery, `resize_object(tail, metadata.size)`, when the actual size exceeds the metadata size (`storage_core.cpp:973-984`; the truncation is at `:974-983` and `:979`, and the "metadata bigger than data" raise is at `:974-977`). This can cut a live writer's data.
2. **`do_put_object`** (`filesystem_storage_backend.cpp:127-160`) writes `<name>.tmp`, fsyncs it, then renames it. Both processes use the same `.tmp` path for `binlog.index` and `<tail>.json`.
3. **`do_open_stream` with `app|ate`** (`filesystem_storage_backend.cpp` ~:196-213; data stream at `:229-230`, no fsync). Both processes append at the real EOF, but each keeps its own in-memory `size`. The metadata each one writes therefore describes a different file length.
4. **Index rewrite.** The index is loaded once in the ctor (`storage_core.cpp:214`). `save_binlog_index` writes it from the in-memory `binlog_records_` at every rotation (`:695-701`, `:789-799`), so the later writer's stale list overwrites the other's (last writer wins).
5. **S3:**
   - Temp files are local and UUID-named, so there is no tmp collision.
   - Every write re-uploads the whole local copy (`s3_storage_backend.cpp:850-863, 933-938`).
   - Opening for append downloads the object first (`:828-832`).
   - Last-writer-wins applies to the data object and to `.json` independently, each written from stale per-process state.
6. **Upstream side.**
   - Both instances usually share `replication.server_id`, because they use the same config.
   - When a new dump thread registers with the same server_id, MySQL evicts the existing one. PBS sends no replica UUID.
   - The evicted PBS sees a mid-stream error:
     - If the error is 2013, it reconnects after `idle_time` (`connection.cpp:546-551`, `pull_operation.cpp:131-150`) and evicts the other instance. The two then ping-pong.
     - If it is another errno, that instance exits, which accidentally fences it.
   - Which errno it receives is unverified.
7. **Pull ordering.** In `pull_operation.cpp`, storage is constructed *before* the acceptor. The `.tmp` deletion and recovery truncation therefore happen even when the port bind then fails.

## Failure scenarios

1. **[trigger: overlapping fetch/fetch, fetch/pull or pull/pull, from a cron overrun, a supervisor double start, or an operator]**
   - **Fail-closed but misleading (usual case).** One instance throws "cannot rename underlying tmp object file: No such file or directory" and exits 1. This is from ticket 24's QA reproduction.
   - **Silent corruption (sometimes).** Both instances append the same events. The binlog becomes longer than the source's and diverges at the source's end. The winner exits 0, and the storage keeps the oversized binlog.
2. **[trigger: zombie takeover]**
   - Pull #1 runs. Antithesis hangs pull #1's container (node hang) or partitions it from the workload's health check, and the workload "fails over" by starting pull #2 on the same storage.
   - Pull #2's constructor deletes #1's `.json.tmp` and truncates the tail to the metadata size.
   - Then either #1 resumes and appends at the real EOF from a stale in-memory size, or the two interleave rotations and the index rewrite drops the other's new file.
   - Possible outcomes:
     - an oversized or diverged binlog (silent and permanent);
     - metadata > data (the next open is fatal);
     - an index/object mismatch (wedge).
3. **[trigger: zombie takeover or overlap on S3]**
   - A's data upload and B's metadata upload interleave. This leaves metadata size > object size (wedge) or a missing index entry.
   - If the S3 server does not provide atomic whole-object overwrite, torn objects are a further outcome.
4. **[trigger: same-server_id instances]** The two instances evict each other's dump threads. With errno 2013 they ping-pong, both continuing to write. With another errno, one exits.

## Property

Two writer instances (fetch/fetch, fetch/pull or pull/pull) may be alive on one storage at overlapping times. The outcome must never be "a process exits 0 while the storage is not a byte prefix of the source, or does not reopen".

Either the overlap is rejected or detected (non-zero exit), or the storage remains a valid prefix that a later single clean fetch completes. "Valid prefix" means the storage opens, and every stored binlog is a byte prefix of the source's binlog ending on a transaction boundary.

## Oracle / workload checks

The workload launches a second writer on the same storage at a random time during a running fetch or pull, or starts one after hanging or partitioning the first (zombie takeover). It records the exit codes. Use distinct `server_id`s in some runs and shared ones in others.

| Message | Check |
|---|---|
| `Always("overlapping writers never yield exit 0 with corrupt storage")` | After both writers stop and one clean fetch runs: `not(any exit 0) or (storage opens and storage_holds_prefixes() and every binlog ends on a transaction boundary)`. |
| `Always("second writer refused to start while first holds the storage")` | Optional strict variant. This is ticket 24's expected behaviour. Enable it only if the owners rule overlap out of contract or a lock is added. |
| `Sometimes("both writers simultaneously past storage construction")` | Proves the race was actually exercised. |

`check_complete_copy` after one clean fetch is a separate recoverability check; see the LG5 properties and `kill-anywhere-storage-reopens`.

## SUT-side instrumentation suggestions

The codebase has no Antithesis SDK, so every SDK assertion here is missing.

| Assertion | Site | Status |
|---|---|---|
| (referenced) `UNREACHABLE("streaming/purging open: tail metadata size exceeds data size")`, owned by kill-anywhere-storage-reopens | `storage_core.cpp:974-977`. A process kill on a single writer does not produce this, because metadata is saved after data. It indicates a concurrent writer, a lost data write, or page-cache loss. | missing |
| `UNREACHABLE("data file grew beyond in-memory size")` | `write_event_block`, against the stream's `tellp` (filesystem). Fires as soon as another process has appended. | missing |
| `REACHABLE("startup recovery truncated the tail binlog (ticket-24 overlap anchor)")` | `storage_core.cpp:979`, with sizes in details. This is the ticket-24-specific anchor showing that a crash or concurrent writer happened. `recovery-paths-exercised` anchors the same site with its own generic message. | missing |
| `SOMETIMES(removed_tmp_count > 0, "startup removed leftover temporary objects (ticket-24 overlap anchor)")` | `storage_core.cpp:597-630`. The .tmp-cleanup site is also anchored by `recovery-paths-exercised` / `kill-anywhere-storage-reopens` and by `other-process-startup-spares-live-writer`. Keep at most one SDK call per site, and reference the others if they already cover it. | missing |

## Key observations

- **There is no fencing primitive.** Every streaming or purging constructor freely deletes another process's in-flight `.tmp` files and truncates its tail.
- **The usual outcome is fail-closed, by accident.** It comes from the rename ENOENT and is misleading. The dangerous outcome is the rarer one: an exit 0 with an oversized or diverged binlog.
- **The ping-pong between same-server_id instances** can keep both writers alive indefinitely if the eviction error is 2013.

## Fault requirements

- Overlapping writers launched by the workload.
- Node hang or network partition of the first writer, for the zombie takeover.
- Optional SIGKILLs.
- No node termination is needed.

## Open questions

- **Is a second writer on a live storage in contract at all (fetch+fetch, fetch+pull, pull+pull)?** README:473 forbids or warns only about purge alongside a writer. If overlap is out of contract, assert only the weaker "fails closed and detectable" form; if it is in contract (ticket 24's expected behaviour) or a lock is planned, the property tightens to "the second writer always refuses to start". `(needs human input)`

## Answers from investigation (2026-10-13)

- **pull+pull ordering (resolved).** Pull constructs the storage at `pull_operation.cpp:109-110` and only then builds `network_service` (`:117-118`), whose member initialiser constructs the acceptor, which opens, binds and listens (`network_service.cpp:672-681`). So a second pull on the same port runs `.tmp` removal and tail truncation, then fails with "address already in use" and exits non-zero. A pull+pull overlap on the same port is therefore *not* like fetch+fetch: B never streams, but it can still damage A. That case is owned by `other-process-startup-spares-live-writer` (bind variant). A pull+pull overlap behaves like fetch+fetch only when B uses a different `replication_source.port`.
- **Eviction error for a same-server_id instance (resolved).** PBS sends no replica UUID (no `replica_uuid`/`slave_uuid` anywhere in `src/easymysql` or `src/operations`). In MySQL 8.4 `kill_zombie_dump_threads` (`sql/rpl_source.cc`) sets `duplicate_slave_id = true` on the older dump thread and wakes it with `THD::KILL_QUERY`. `Binlog_sender::run` (`sql/rpl_binlog_sender.cc`) then calls `set_fatal_error("A replica with the same server_uuid/server_id as this replica has connected to the source")`, which sets `ER_SOURCE_FATAL_ERROR_READING_BINLOG` (1236, `rpl_binlog_sender.h`), and `cleanup()` sends it as an error packet. In PBS, `connection::fetch_binlog_event` treats only `CR_SERVER_LOST` (2013) as a disconnect; any other errno raises (`easymysql/connection.cpp:544-552`). The exception leaves `receive_binlog_events`, so fetch and pull both exit non-zero (pull: `pull_operation.cpp:154-166`, no reconnect). **So there is no in-process ping-pong:** the older instance exits as soon as the newer one starts its dump. Ping-pong can still happen *across restarts* when a supervisor restarts the evicted pull (the `pull_fs` supervisor does this). The 8.0 and 9.x branches were not read; the mechanism dates from 5.6, so the same behaviour is expected.
  - **Implications:** failure scenario 4 and the "ping-pong" key observation are corrected below. With a shared `server_id` the overlap window during which both instances *stream* is short (from B's dump start until A's next read), but both still run their constructors and A can still flush buffered data on its way out (`~storage`). Ticket 24's QA reproduction hit corruption with a shared server_id, which fits. The shared-server_id arm still needs to be tested, and the evicted instance's non-zero exit is expected, not a failure.
- **S3 atomic overwrite in the harness (resolved for Versity).** The harness uses Versity S3 Gateway 1.8.0 with the posix backend (`deployment-topology.md`). In versitygw v1.8.0 `backend/posix/posix.go`, `PutObjectWithPostFunc` streams the body into a temp file from `openTmpFile`, and publishes it with `f.link()` only after the copy and metadata steps succeed. A copy error returns before `link()`. Whole-object overwrite is therefore atomic, and torn objects are not an outcome on Versity. MinIO (the fallback) was not checked; re-check if the harness switches.

### Corrections to the sections above

- Failure scenario 4 should read: same-server_id instances evict each other's dump threads; the evicted one receives error 1236 and exits non-zero. Under a restart-on-exit supervisor the two can alternate indefinitely, each restart running the destructive constructor again.
- Key observation 3 ("ping-pong ... if the eviction error is 2013") is wrong: the error is 1236, so the ping-pong needs a supervisor restart.
- Code path 6 ("Which errno it receives is unverified") is now verified: 1236.

### Investigation Log

#### Is a second writer on a live storage in contract at all?

- Examined: README :473 and the "Resuming previous operation" / "Graceful termination" sections, ticket 24 in `.tickets-snapshot.txt` (:1292-1345), `deployment-topology.md` Open Questions.
- Found: README warns only about `purge_binlogs` alongside a writer. Ticket 24 is a QA-filed bug whose "Expected behaviour" is a refusal with "storage <uri> is in use by another process", but it is open and carries no maintainer response. The topology lists the same question as needs-human.
- Not found: any maintainer statement about fetch+fetch, fetch+pull or pull+pull.
- Conclusion: `(needs human input)`.

#### Does a pull+pull overlap behave like fetch+fetch?

- Examined: `pull_operation.cpp:98-160`, `network_service.cpp:672-686`.
- Found: storage construction (`:109-110`) precedes `network_service` construction (`:117-118`); the acceptor is created in the member initialiser and binds immediately.
- Conclusion: resolved. With the same port, B damages A then exits on bind failure (owned by `other-process-startup-spares-live-writer`). With different ports, B is a full second writer and this property applies.

#### What error does the evicted dump connection receive when MySQL kills a same-server_id zombie dump thread?

- Examined: `src/easymysql/connection.cpp:535-554`, `src/operations/pull_operation.cpp:124-166`, grep for replica/slave UUID in `src/`; upstream mysql-server 8.4 `sql/rpl_source.cc` (`kill_zombie_dump_threads`), `sql/rpl_binlog_sender.cc` (`run`, `cleanup`), `sql/rpl_binlog_sender.h` (`set_fatal_error`).
- Found: the zombie is woken with `KILL_QUERY` and `duplicate_slave_id`; it sends `ER_SOURCE_FATAL_ERROR_READING_BINLOG` (1236) with the "same server_uuid/server_id" message. PBS raises on any errno except 2013, so the evicted instance exits non-zero.
- Not found: the 8.0 and 9.x sources were not read; the body of `Find_zombie_dump_thread` was not quoted (matching by server_id when no UUID is sent is inferred from the early return on `replica_uuid.length() == 0 && server_id == 0`).
- Conclusion: resolved. No in-process ping-pong; only supervisor-driven alternation.

#### For S3, does the harness's S3 server provide atomic whole-object overwrite?

- Examined: `deployment-topology.md` (Versity 1.8.0 posix); versitygw v1.8.0 `backend/posix/posix.go` (`PutObjectWithPostFunc`, `CompleteMultipartUploadWithCopy`).
- Found: data is staged in a temp file (`openTmpFile`, O_TMPFILE when available) and published with `f.link()` after the copy; errors return before `link()`.
- Not found: MinIO behaviour (fallback only).
- Conclusion: resolved for Versity: no torn objects.

## Evaluation refinements (2026-10-13)

Applied by the evaluation synthesis (`../evaluation/synthesis.md`). Where this section and earlier text in this file disagree, this section wins; the catalog entry has been updated to match.

- Same-config writers share `server_id`, so the source evicts one dump thread and the two rarely append concurrently. The main arm gives overlapping writers distinct `server_id`s; a smaller same-id arm keeps the eviction path. Added `Sometimes(both writers appended after the other's construction)` (implementability P5).
- Runs only in `contention`, so its SUT messages carry the harness timeline tag. Antithesis tallies an assertion by its message across every timeline of a run, so an SUT message cannot be scoped to a template (wildcard W1).
- Whether this whole template stays is bias B2.
