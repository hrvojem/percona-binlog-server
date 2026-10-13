# Property discovery 02: Concurrency

Focus: races, atomicity, ordering, shared mutable state. Covers:
- the collector jthread vs the asio sender sessions on one `storage_core`;
- multiple OS processes on one storage (fetch+fetch, purge+writer, purge+purge, list/search+writer);
- signal handling vs in-flight writes.

Evidence files are in `antithesis/scratchbook/properties/<slug>.md`. Line numbers refer to commit 99de682. Every SUT-side assertion suggested here is **missing** (there is no Antithesis SDK in the repo).

## In-process: collector vs replication-source sessions (pull)

### sender-file-switch-emits-artificial-rotate — Every binlog switch served by pull carries an artificial ROTATE

| | |
|---|---|
| **Type** | Safety |
| **Property** | Every time a pull dump stream moves from binlog N to N+1, the client receives an artificial ROTATE naming N+1 before any event of N+1, even when the sender polled while N+1 was still magic-only. |
| **Invariant** | Workload `Always`: in each blocking client's captured stream, every file change is followed by an artificial ROTATE (artificial flag, timestamp 0) naming the new file before its first real event. With `mysqlbinlog --raw`, each output file equals the stored file. SUT-side `Always` in `sender_context::populate_event_block`: the name never changes on an empty fetch. Plus `Sometimes(sender at EOF saw a magic-only next binlog)` as an exploration anchor. `Always` because it must hold at every switch. |
| **Antithesis Angle** | A race between the collector's rotation steps (flush N → create N+1 with 4 bytes → first flush of N+1) and the sender's 500 ms EOF poll. Thread pausing and CPU modulation move the poll into the window. Varying `checkpoint_interval`/size changes the window width; without checkpointing, the window spans the whole life of N+1. |
| **Why It Matters** | Lead N5. `fetch_event_block` advances to N+1 with a zero-length read (`storage_core.cpp:523-541`), and `populate_event_block` returns EOF before `handle_binlog_file_switch` (`sender_context.cpp:487-491`), so the switch is lost. Clients can merge two binlogs into one file or miss the FDE refresh. The existing concurrent-reader test strips artificial ROTATEs (`storage_property_test.cpp:1356`), so it cannot see this. |

Priority: P0. Confidence: high (mechanism traced end to end); medium (client-visible impact). Fault requirements: none beyond thread pausing/CPU modulation. Works on the default fault set.

**Open Questions:**
- Does `mysqlbinlog --raw` switch output files only on the artificial ROTATE?
- Is the intended fix "don't advance on an empty next file" or "emit ROTATE whenever the name changes"?

### sender-reads-only-flushed-bytes — Pull never reads past flushed data, and serves exactly the stored bytes

| | |
|---|---|
| **Type** | Safety |
| **Property** | With a single writer process, every sender read returns exactly the bytes the collector flushed, and each client's stream (artificial events stripped) is a byte prefix of the stored (decrypted) binlogs. |
| **Invariant** | SUT-side `Always(size(get_object result) == read_length)` in `fetch_event_block`. `Unreachable("session ended by storage read exception")` in single-writer runs without S3 faults. Workload `Always`: client streams are stored-byte prefixes. `Always` and `Unreachable` because the lock protocol is supposed to make violations impossible. |
| **Antithesis Angle** | Thread pauses between the collector's data append and `size +=`, and between the sender's shared-lock acquisition and its read. Rotations and checkpoints under load with several concurrent clients. Encryption on/off (decryption is offset-based). |
| **Why It Matters** | This is the core reader/writer contract (SG9). It relies on data-before-size under `unique_lock` (`storage_core.cpp:299-322`) and on sessions touching only `storage_core` (the wrapper is unsynchronized). It is a regression guard for the least-settled code (`sender_context` was rewritten recently). |

Priority: P1. Confidence: high that it holds today; the value is regression detection. Fault requirements: none (thread pausing helps).

**Open Questions:**
- Should a session dropped by a transient S3 GET error count as a violation?
- Is a TSan build variant worth running in Antithesis?

### sigterm-not-starved-by-storage-lock — Pull handles SIGTERM within the documented bound even under storage-lock contention

| | |
|---|---|
| **Type** | Liveness |
| **Property** | After SIGTERM, pull exits within `read_timeout + 1` s (README:719) plus a stated slack, even while dump sessions are blocked on the storage lock held by a long collector write. |
| **Invariant** | Workload `Always(exit_latency <= bound)` per SIGTERM. SUT-side `Sometimes(SIGTERM arrived while a session was blocked in fetch_event_block)` as an anchor. `Always` because the bound is a documented per-event claim. |
| **Antithesis Angle** | Network latency/partition between PBS and S3 stretches the collector's `unique_lock` hold (whole-object PUT, ticket 17). Blocking clients polling at 500 ms then park the single io thread on `shared_lock`, so the signal handler cannot run. Node throttling has the same effect on the file backend. |
| **Why It Matters** | The signal handler shares the io thread with sessions (`pull_operation.cpp` ~:98-104). Supervisors escalate to SIGKILL, which turns a graceful stop into a crash. The final flush may also run in `~io_context` after `signal_set` is gone. |

Priority: P1. Confidence: medium (mechanism clear; whether the bound is meant to include storage latency is unclear). Fault requirements: network latency to S3, or node throttling. Needs a SIGTERM-sending workload command (no node-termination fault needed).

**Open Questions:**
- Is the README:719 bound meant to cover pull with a slow storage backend?
- What are the S3 CRT default request timeouts? Can a blackholed endpoint hang a PUT indefinitely?
- Should SIGKILL escalation after SIGTERM be part of the workload?

### blocking-dump-catches-up-after-io-stall — Blocking dump clients catch up once storage/network faults stop

| | |
|---|---|
| **Type** | Liveness |
| **Property** | After faults stop, every connected blocking dump client eventually receives every flushed byte, and the collector eventually catches up to the source, regardless of earlier io-thread stalls caused by lock contention. |
| **Invariant** | `Sometimes(all clients' received bytes == stored bytes)` after `ANTITHESIS_STOP_FAULTS` plus settling. An `eventually_` check that the stored size equals the source size and each client equals the store. Optionally `Unreachable("healthy client dropped by write timer after io stall")`. `Sometimes` because this is a progress condition checked after a quiet period. |
| **Antithesis Angle** | S3 latency, network congestion and throttling while K clients do synchronous 1 MiB reads on the single io thread. This explores the hypothesised degradation spiral (collector stall → source `net_write_timeout` → reconnect → bigger re-upload). |
| **Why It Matters** | Pull is the only restore path for encrypted storage (W5). A stalled single thread affects every replica and the collector at once (sut-analysis A7, W3). |

Priority: P2. Confidence: medium. Fault requirements: network latency/partitions to S3 and to clients, node throttling. Uses a quiet period.

**Open Questions:**
- Is a replica disconnect on write timeout during a storage stall acceptable?
- Does the degradation spiral actually occur?
- Should the workload set `checkpoint_interval`, or force a rotation before the eventual check (LG3)?

### sigterm-mid-write-leaves-boundary-storage — A graceful signal at any instant leaves a consistent, boundary-aligned storage

| | |
|---|---|
| **Type** | Safety |
| **Property** | After SIGTERM/SIGINT delivered at an arbitrary moment to fetch or pull, the storage opens, every binlog is a source prefix, and the tail metadata size equals the file size on a transaction boundary. |
| **Invariant** | Workload `Always(storage_holds_prefixes() && tail_on_boundary)` after each signalled exit. SUT-side `Sometimes(signal arrived while collector inside write_event_block)`. `Always` because README:715 claims it unconditionally. |
| **Antithesis Angle** | The signal (main thread) races the collector's mid-checkpoint, mid-rotation or mid-metadata-PUT state. The signal path skips the explicit flush and relies on `~storage` (`collector_context.cpp:178-183`, `storage.cpp:68-77`), which can run late (pull: in `~io_context`). An optional second SIGTERM hits the post-`signal_set` window. |
| **Why It Matters** | Graceful stop is the documented safe way to stop PBS (W3). It overlaps SG3 from other foci, but this property targets the thread handoff specifically. |

Priority: P1. Confidence: medium-high. Fault requirements: none (a workload sends signals). Combining with storage faults widens coverage.

**Open Questions:**
- Should a second SIGTERM during the final flush be in scope?
- Is exit 0 after a swallowed final-flush failure acceptable (N8)?

## Cross-process: writers, purges and queries on one storage

### second-writer-on-storage-fails-closed — Overlapping writer processes never yield exit 0 with a corrupt storage

| | |
|---|---|
| **Type** | Safety |
| **Property** | When two writer processes (fetch/fetch, fetch/pull) overlap on one storage, no process exits 0 unless the storage afterwards opens and every binlog is a byte prefix of the source. |
| **Invariant** | Workload `Always(not any_exit_0 or (opens and storage_holds_prefixes()))`. `Sometimes(both writers simultaneously past storage construction)` to prove the overlap happened. Optional SUT-side `Unreachable("data file grew beyond in-memory size")`. |
| **Antithesis Angle** | Process start timing relative to the other writer's put/rename, recovery truncation and append. Both processes share the deterministic `.tmp` names and `app` streams. Pausing either process at the right point decides between fail-closed and silent duplication. |
| **Why It Matters** | Ticket 24 (open, validated: no lock primitive, fixed `.tmp` suffix, recovery truncation). Realistic: a cron fetch overrun, or a supervisor double-start. The QA repro shows exit 0 with an oversized, diverged binlog. |

Priority: P0. Confidence: high (mechanism + QA repro). Fault requirements: none (the workload launches the second writer); thread pausing helps.

**Open Questions:**
- Is fetch+fetch overlap in contract, or should the property require only fail-closed?
- Does a second pull construct storage (deleting `.tmp`s and truncating) before it fails to bind the port?

### startup-tmp-cleanup-spares-live-writer — Startup temp cleanup by another process never breaks a live writer

| | |
|---|---|
| **Type** | Safety |
| **Property** | A fetch/pull/purge starting on a storage that a live writer is using never deletes the live writer's in-flight `.tmp` objects in a way that leaves duplicated or unindexed data. |
| **Invariant** | Workload `Always(storage_holds_prefixes())` for the live writer's storage after it exits. SUT-side `Unreachable("tmp object vanished before rename")` on the ENOENT branch of `do_put_object`. `Sometimes(removed a fresh .tmp)` as an anchor. |
| **Antithesis Angle** | A second process's ctor `remove_temporary_objects` (`storage_core.cpp:597-630`) races the writer's write→fsync→rename (`filesystem_storage_backend.cpp:127-160`). If it lands after `size +=`, `~storage`'s re-flush then duplicates data (ticket 18 path). If it lands during rotation, the result is ticket 15's unindexed binlog. |
| **Why It Matters** | Lead N2. Purge is a routine operation, and its ctor is not `querying_only`. It is silent (purge uses `null_logger`) and permanent (duplicates pass the size check on restart). |

Priority: P1. Confidence: high (mechanism); medium (frequency). Fault requirements: none. Filesystem backend only.

**Open Questions:**
- Is purge-while-writing out of contract (README:473)?
- Should the property be filesystem-only (S3 temps are UUID-named)?

### purge-ctor-never-truncates-live-tail — purge_binlogs never truncates a live writer's tail

| | |
|---|---|
| **Type** | Safety |
| **Property** | A `purge_binlogs` run overlapping a live writer never changes the bytes of the tail binlog. After the writer stops, the tail is a source prefix and its metadata size ≤ its file size. |
| **Invariant** | Workload `Always(tail is source prefix && storage reopens)` after purge+writer overlaps. SUT-side `Sometimes(purging-mode ctor performed tail truncation)` as an anchor, because purge logs nothing (`null_logger`). |
| **Antithesis Angle** | Purge's ctor listing (t0) and metadata read land between the writer's data append and its metadata PUT (`storage_core.cpp:299-322`). `resize_object` (`:979`) then cuts live bytes. The writer keeps appending at the new EOF with an inflated in-memory size. Small checkpoints and a heavy workload widen the window. |
| **Why It Matters** | Lead N1. It wedges the storage ("metadata bigger than actual") and drops pull sessions (reads past EOF). It is invisible in purge's JSON output. |

Priority: P0. Confidence: high (traced). Fault requirements: none. Process/thread pausing helps.

**Open Questions:**
- Should purge perform tail recovery at all? Bug vs documented misuse (README:473).

### purge-during-writer-keeps-storage-openable — Storage stays openable after purge overlaps a writer

| | |
|---|---|
| **Type** | Safety |
| **Property** | After any interleaving of `purge_binlogs` with a running fetch/pull, followed by stopping the writer, a fresh fetch opens the storage and the index references only existing objects. |
| **Invariant** | Workload `Always(storage opens in streaming mode)` and `Always(every index entry exists)`. `Sometimes(writer rotated after a successful concurrent purge)`. |
| **Antithesis Angle** | The ordering between purge's index commit and the writer's next rotation, which rewrites the index from its stale in-memory list (`storage_core.cpp:701, 789-799`). Also pull serving purged names from memory. |
| **Why It Matters** | Ticket 9 (open, validated). The storage becomes unopenable in every mode, and the purge itself cannot repair it. |

Priority: P1 (P0 if purge+writer is ruled in-contract). Confidence: high. Fault requirements: none.

**Open Questions:**
- Is purge alongside a writer in contract?
- Should a purged-then-requested binlog in pull return an error packet?

### concurrent-purges-keep-index-consistent — Concurrent purges never resurrect deleted binlogs in the index

| | |
|---|---|
| **Type** | Safety |
| **Property** | Any set of concurrent `purge_binlogs` runs on a storage without a writer leaves a storage that opens, whose index is a suffix of the original and references only existing objects. |
| **Invariant** | Workload `Always(storage opens && index ⊆ objects && index is suffix)`. `Sometimes(two purges both reported success or warning)` proves real overlap. |
| **Antithesis Angle** | Both purge ctors load the index before either commits. The later committer writes its stale suffix, which resurrects binlogs the other purge deleted (`storage_core.cpp:368-433`). Process pausing between ctor and commit is what makes this reachable. |
| **Why It Matters** | A new lead with no ticket. The README does not forbid concurrent purges (cron/automation overlap, retries). The outcome is the same wedge as ticket 9, with no writer involved. |

Priority: P1. Confidence: medium (traced in code, not reproduced). Fault requirements: none.

**Open Questions:**
- Are concurrent purges in contract?
- Does the losing purge always report `warning` (detectable), or can both report `success`?

### query-concurrent-with-writer-never-errors — list/search never error or crash while a writer is active

| | |
|---|---|
| **Type** | Safety |
| **Property** | `list`, `search_by_timestamp` and `search_by_gtid_set` running against an actively written storage always return well-formed JSON, never crash, and never report `error` for a concurrency-only reason. |
| **Invariant** | Workload `Always(exit 0 or a legitimate unsatisfiable-search message)` and `Always(not killed by signal)`. SUT-side `Always(!binlog_records_.empty())` before the `front()` access at `storage_core.cpp:1001` (and the ctor assert at :219). `Sometimes(querying ctor dropped a record)`. |
| **Antithesis Angle** | A writer's `.json.tmp`/`binlog.index.tmp` renamed between `readdir` and the per-entry `stat` in `do_list_objects` (`filesystem_storage_backend.cpp:103-117`, N4) throws. A rotation plus purge between the query's listing (t0) and index read (t1) drops every record and dereferences `front()` on an empty vector. |
| **Why It Matters** | Querying beside a writer is an explicit design intent (code comments at `storage_core.cpp:189-195`). A failing or crashing PITR search during an incident is user-visible (W6, W8). |

Priority: P1. Confidence: high for N4 (relies on libstdc++ re-stat); medium for the crash path (needs purge+writer, or a filesystem `readdir` quirk). Fault requirements: none.

**Open Questions:**
- Does libstdc++ `directory_entry::file_size()` re-stat on gcc 14?
- Is a query racing a purge in contract?

### query-sees-binlogs-flushed-before-start — Queries never silently drop data committed before they started

| | |
|---|---|
| **Type** | Safety |
| **Property** | Every binlog whose data, metadata and index entry were flushed before a query started appears in that query's result, with size ≥ the size at query start. A GTID search for fully stored GTIDs never returns "cannot be covered". |
| **Invariant** | Workload `Always(snapshot ⊆ result, sizes non-decreasing)` comparing a pre-query snapshot (direct index + `.json` read) with the concurrent query's output. `Sometimes(query result strictly newer than snapshot)`. SUT-side `Sometimes(querying ctor dropped the tail record)`. |
| **Antithesis Angle** | Queries race checkpoint renames of the tail `.json`. `querying_only` silently drops any record whose `.json` was missing from its t0 listing (`storage_core.cpp:942-950`). Filesystem `readdir` behaviour under rename-over-existing, and S3 listing consistency, decide whether a pre-existing tail can be dropped. |
| **Why It Matters** | A silently stale PITR answer (W6) is worse than an error. Dropping a record is never reported. |

Priority: P2. Confidence: low-medium (depends on filesystem semantics; not reproduced). Fault requirements: none.

**Open Questions:**
- Which filesystem backs the Antithesis storage volume (ext4/xfs vs tmpfs/overlay)?
- Is dropping the whole tail binlog acceptable, or only lagging by one checkpoint?

## Areas examined

- `src/operations/pull_operation.cpp`, `fetch_operation.cpp`: thread layout, signal_set, jthread join, destruction order (signal_set destroyed before `io_context`; storage last owner may be a session frame).
- `src/operations/collector_context.cpp:125-205`: stop polling, signal vs timeout paths.
- `src/binsrv/storage.cpp`: the unsynchronized wrapper, checkpoint evaluation, `~storage` swallow, buffer erase after success.
- `src/binsrv/storage_core.cpp`: every `shared_lock`/`unique_lock` site (231, 253, 261, 305, 327, 372, 461); `fetch_event_block` (435-564); `write_event_block` (299-322); `purge_binlogs` (368-433); ctor reconciliation, `.tmp` cleanup (597-630), index load/save, metadata load/recovery (927-1005), querying_only tolerance.
- `src/binsrv/filesystem_storage_backend.cpp`: list/stat, put (tmp+rename), resize, `app` stream. `s3_storage_backend.cpp`: empty-range GET, length check, per-instance `tmp_fstream_` (touched only under `unique_lock`).
- `src/operations/sender_context.cpp`: populate/switch/EOF logic, start_from_offset FDE fetch (separate lock acquisition; safe because storage is append-only).
- `src/minimysql/network_service.cpp`, `network_io_operations.cpp`: session exception containment, 500 ms poll, `||` timer races.
- `src/operations/purge_binlogs_operation.cpp`, `list_operation.cpp`, `search_by_gtid_set_operation.cpp`: all use `null_logger`.
- `tests/storage_property_test.cpp:1254-1360` (concurrent reader; strips artificial ROTATEs); `tests/PROPERTY_TESTS.md`; `extra/qa/concurrent_storage_test.py`; tickets 9, 24 (snapshot).
- Examined, but no property: lock ordering (only storage lock → logger mutex, no inversion found); S3 `tmp_fstream_`/`current_name_` mutation only under `unique_lock`, while `is_binlog_open` reads it only under `shared_lock` (no concurrent writer); `fetch_fde_from_storage` reading the FDE under a separate lock acquisition (safe, because FDE bytes never change after the first flush); asio `||` timer-vs-write tie-breaking after a long io-thread stall (unclear benefit; folded into blocking-dump-catches-up-after-io-stall).

## Cross-property assumptions

- One storage per test topology. A second process is launched by the workload (or a custom fault), not by Antithesis node faults. None of these properties needs node termination.
- The workload can stop/inspect the storage directly (shared volume or S3 client) to run `storage_holds_prefixes()`/`check_complete_copy()` from `extra/qa/fetch_crash_test.py`.
- The filesystem backend is the primary target for cross-process `.tmp` hazards. On S3, the cross-process hazard is last-writer-wins of whole objects (still relevant for second-writer, purge-during-writer and concurrent-purges).
- The build is debug or ASan, so the `front()`/assert paths abort instead of being UB.
- Whether cross-process overlaps are "in contract" is undecided (sut-analysis open question). The cross-process properties are written so that they hold even if the decision is "fail closed": they forbid only *silent* corruption with exit 0, or an unopenable result. If the answer is "fully in contract", they tighten.
- Line numbers refer to 99de682. A `~` prefix means the line is within a few of the cited one.
