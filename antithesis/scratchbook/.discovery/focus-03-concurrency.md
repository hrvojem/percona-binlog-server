# Focus 3: Concurrency model (binlog_server @ 99de682)

## Findings

**F1. In-process model is small: at most two app threads plus AWS CRT threads.**
- `fetch`: main thread runs `io_ctx.run()`, which handles only the SIGINT/SIGTERM `signal_set` (fetch_operation.cpp:69-75). The collector runs on a `std::jthread` (fetch_operation.cpp:91-104), which is joined at the end of the scope (line 107).
- `pull`: the same, plus `minimysql::network_service` coroutines on the same `io_ctx` (pull_operation.cpp:98-160). Every replica session and every `sender_context::get_event` therefore runs on the main thread. The collector thread is the only writer.
- Shared state between the two threads: `storage` (via `shared_ptr`), the logger (`do_log_mutex_`, basic_logger.cpp:57; atomic `min_level_`), and `io_ctx.stopped()`/`stop()`, which are thread-safe in asio.
- Other modes (list, search_*, purge_binlogs) are single-threaded.
- The S3 backend creates one `S3CrtClient`, which has its own event-loop threads. `InitAPI`/`ShutdownAPI` run once per backend instance (s3_storage_backend.cpp:78-83).

**F2. Reader/writer split in `pull`: the writer's half (`storage` wrapper) is unsynchronized, and the reader must only touch `storage_core`.**
- `binsrv::storage` holds `event_buffer_`, checkpoint state, and `get_current_position()` (storage.hpp:81-82, 140-152) without a lock. This is safe only because the collector thread is the only caller.
- `sender_context` calls only `storage_->fetch_event_block` (sender_context.cpp:226, 484), which takes `shared_lock` (storage_core.cpp:461).
- The writer takes `unique_lock` in `open_binlog`/`write_event_block`/`close_binlog`/`set_purged_gtids`/`purge_binlogs` (storage_core.cpp:231, 261, 305, 327, 372).
- In `write_event_block`, bytes are written and flushed to the backend (filesystem: `ofs_.flush()`, filesystem_storage_backend.cpp:225; S3: full re-upload, s3_storage_backend.cpp:850-863) *before* `size +=` (storage_core.cpp:307-310), all under the exclusive lock. A reader can therefore never see a size larger than the data that is readable.
- Only whole-transaction prefixes reach the core (storage.cpp:276-297). The reader never sees a partial transaction or the in-memory buffer.
- Candidate property: **every byte served by `pull` is a prefix of what `fetch` would store, and never passes the core's flushed size.** Any future getter that the sender calls on the `storage` wrapper (for example `get_current_position`) would be a data race. A TSan build is worth having in Antithesis.
- Unit coverage of this exists only at the library level: storage_property_test.cpp:1336 runs a concurrent reader jthread under nightly TSan (PROPERTY_TESTS.md:82-84).

**F3. Blocking I/O under the storage lock stalls the whole `pull` event loop (liveness, timing-sensitive).**
- `fetch_event_block` performs the backend read (`get_object`: synchronous S3 GetObject, or a file read) plus decryption while holding `shared_lock`, on the asio thread (storage_core.cpp:461-560).
- The collector holds `unique_lock` across S3 PutObject of the *whole* temporary binlog file plus the metadata PUT (storage_core.cpp:305-321; ticket 17 confirms in the code that the upload grows with binlog size). `open_binlog` in append mode first downloads the whole object (s3_storage_backend.cpp:828-832) under the lock.
- While the writer holds the lock, the main thread blocks on `shared_lock`. All replica sessions, accept, `steady_timer` read/write timeouts (network_io_operations.cpp:63, 151), the 500 ms poll timer (network_service.cpp:295) and the **signal handler** are frozen.
- Conversely, N replicas serialize synchronous 1 MiB reads (network_service.cpp:294) on one thread and delay the collector.
- Antithesis angle: slow disk or S3 latency plus several blocking replicas. Expect spurious replica timeouts, delayed SIGTERM handling, and a collector that falls behind the source's `read_timeout`. I found no deadlock: the only nesting is storage lock then logger mutex, never the reverse.

**F4. Graceful termination: there is a claimed bound, and the mechanism relies on a destructor.**
- README:713-721 claims that SIGINT/SIGTERM leave the storage consistent. The worst-case delay is `connection.read_timeout + 1` s.
- Mechanism: the signal handler only calls `io_ctx.stop()`. The collector polls `io_ctx.stopped()` between events (collector_context.cpp:151), so a blocking `fetch_binlog_event` (connection.cpp:537-554, no heartbeat configured) holds the jthread join (pull_operation.cpp:161) for up to `read_timeout`. The idle loop polls once per second (pull_operation.cpp:70-75).
- On the signal path (collector_context.cpp:178-183), neither `discard_incomplete_transaction_events` nor `flush_event_buffer` is called. The final flush happens in `storage::~storage()` (storage.cpp:68-76), which **swallows all exceptions**. A failed final flush therefore still yields "successfully shut down" and exit 0 (pull_operation.cpp:168-171).
- In `pull`, the last `storage_ptr` owner can be a suspended session coroutine frame. That flush then runs during `~io_context`, after `network_service` is destroyed. Session frames hold `const storage_ptr&` into the destroyed `network_service::storage_` (network_service.cpp:283, 633, 663). Frame destruction does not dereference it, and `sender_context` owns a copy, so this is fragile but I found no current UAF.
- Candidate properties:
  - (a) After SIGTERM, the process exits within `read_timeout + idle granularity + ε`.
  - (b) After SIGTERM, the storage reopens, and every binlog is a source prefix ending at a transaction boundary.
  - (c) An exit code of 0 implies (b).

**F5. No cross-process exclusion. Ticket 24 is confirmed in the code.**
- There is no flock, lock file or O_EXCL anywhere in src/. The only guard is the in-process `shared_mutex` (storage_core.hpp:275).
- Mechanisms I verified:
  - (i) The temp name is deterministic: `<name>.tmp` (basic_storage_backend_fwd.hpp:56-60, filesystem_storage_backend.cpp:126-160). Two writers share one temp file: write, then fsync, then rename. One writer's rename can publish the other's half-written content (a torn `binlog.index`/metadata), or fail with ENOENT.
  - (ii) A second `streaming`/`purging` instance **deletes every `*.tmp` at construction** (storage_core.cpp:596-628; skipped only for `querying_only`). This includes the live instance's in-flight temp files.
  - (iii) The binlog stream is opened with `ios::app` (filesystem_storage_backend.cpp:201-204), so two writers append duplicate events to the real EOF. Each one's in-memory `size` then disagrees with the file.
  - (iv) Constructor recovery truncates the tail binlog to the metadata size (storage_core.cpp:960-983). A second process that starts inside the first one's window between write and `save_binlog_metadata` (storage_core.cpp:307-321) truncates live data out from under it.
- Ticket 24's QA reproduction (extra/qa/concurrent_storage_test.py) is consistent with (iii).
- On S3, the temp files are local and uuid-named (s3_storage_backend.cpp:929-931). The S3 hazard is last-writer-wins PUT of whole objects (binlog body, index, metadata) from stale in-process state.
- Note: the README forbids only purge plus fetch on one storage (README:473), not fetch plus fetch.

**F6. purge_binlogs during fetch/pull. Ticket 9 is confirmed in the code. This is a documented-unsupported hazard.**
- The streaming instance loads the index only in its constructor. `open_new_binlog_file_internal` appends to the in-memory `binlog_records_` and calls `save_binlog_index()`, which rewrites the index from that stale list (storage_core.cpp:695-701, 789-799). This resurrects purged entries whose files are gone. The next start fails with "binlog index contains a reference to a non-existing object" (storage_core.cpp:772-776).
- In `pull`, the sender also keeps serving purged records from memory. `get_object` on a deleted file throws inside the session.
- README:473 says this "may cause data corruption and should be avoided". A property should therefore treat this as either *documented misuse* or as "fails closed and detectable". It must not demand success. This is a decision for a human.

**F7. Query-only modes run concurrently with a writer, by design.**
- `list`/`search_*` (`querying_only`) explicitly tolerate a concurrent writer:
  - a missing index is accepted (storage_core.cpp:187-196);
  - index/object mismatches are ignored (764-770);
  - unreadable metadata drops the binlog silently (938-947);
  - no size check is done (950-951);
  - temp files are not removed (619-623).
- Every object write is a rename-atomic put on the filesystem and a PutObject on S3. A query that overlaps rotation can therefore omit the newest binlog. It can also report a size or GTID set slightly behind the writer's in-memory state.
- Candidate property: **a query that runs concurrently with fetch/pull never fails, and returns a prefix (by name, with sizes ≤ the eventual final sizes) of the binlog list at quiescence.** This is a good Antithesis target, because the timing of `list_objects` (t0) versus index/metadata reads (t1) is the whole game.

## Areas examined
- fetch_operation.cpp, pull_operation.cpp (threads, signals, shutdown order); collector_context.cpp:125-205.
- storage.hpp/.cpp (unsynchronized wrapper, checkpointing, destructor flush); storage_core.hpp/.cpp (lock coverage, purge, fetch_event_block, constructor validation/recovery, temp cleanup).
- filesystem_storage_backend.cpp, s3_storage_backend.cpp (stream semantics, atomic put, temp naming); basic_storage_backend.
- network_service.cpp, network_io_operations.cpp (coroutines, timers, blocking poll); sender_context.cpp (storage access surface).
- basic_logger; connection_context atomic id.
- README termination/purge sections; tickets 1, 9, 17, 18, 24 (snapshot); PROPERTY_TESTS.md; extra/qa/README.md.

## Assumptions
- S3 CRT client sync calls from two threads are thread-safe (AWS documents this). I did not verify it in the SDK source.
- Kernel page cache makes `ofs_.flush()`'d bytes visible to a separate read fd immediately (POSIX). Durability is a separate issue (no fsync on the stream: filesystem_storage_backend.cpp:229-230).
- I did not run anything. The ticket 24 and ticket 9 reproductions are QA's. I validated only the code mechanisms above.

## Open questions
- [needs-human] Should Antithesis assert anything for fetch+fetch or purge+fetch overlap (README says "avoid"), or only "fails closed / exit≠0 and storage still openable or detectably broken"? Cron overlap (ticket 24) is a realistic operator accident.
- [needs-human] Should `exit 0 after SIGTERM` imply the final flush succeeded? Today, `~storage` swallows flush errors (storage.cpp:72-75).
- [open] Is the SIGTERM latency bound in README:719 meant to hold in `pull` while the main thread is blocked behind a long S3 PUT under `unique_lock`? The signal handler cannot run until the lock is released. That adds unbounded time beyond `read_timeout + 1`.
- [open] Replica `read_timeout`/`write_timeout` behaviour when the io thread was blocked longer than the timeout: does a resumed `||` race favour the timer and drop healthy replicas?
