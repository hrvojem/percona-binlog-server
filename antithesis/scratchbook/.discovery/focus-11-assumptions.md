# Focus 11: Unproven Assumptions (binlog_server @ 99de682)

## Findings

Ticket references are QA drafts. The tag "confirmed in code" means I traced the mechanism myself. It does not mean I reproduced the bug.

**A1. Single writer per storage. Nothing enforces it.** (ticket 24, confirmed in code)
- The only guard is the in-process `std::shared_mutex` in `storage_core`. There is no flock, lock object or O_EXCL.
- `filesystem_storage_backend.cpp:140-158` writes to a deterministic `<name>.tmp` and then renames it.
- On startup every non-querying constructor deletes all `.tmp` objects (`storage_core.cpp:150`, `597-630`). The comment calls them leftovers "after improper shutdown".
- New nuance: `purge_binlogs` constructs in `purging` mode, and only `querying_only` skips the deletion (`:621`). So a purge run against a live fetch/pull can delete the writer's in-flight `binlog.NNN.json.tmp`. The writer's rename then fails at `:155-157` with `runtime_error`, and fetch/pull exits. Ticket 9 does not describe this race; it is distinct from ticket 9's stale-index problem.
- `open_existing_binlog_file_internal` checks "file size == flushed position" with an assert only (`storage_core.cpp:707`). That is exactly the invariant a second writer breaks.

**A2. The storage assumes the source never resets and never changes identity.** (ticket 25, confirmed in code)
- `grep server_uuid src/` finds nothing. The only query to the source is `SELECT @@GLOBAL.gtid_purged`, at `collector_context.cpp:231`.
- `storage_core::save_metadata` (`:828-834`) stores only mode and encryption.
- On reconnect in GTID mode, `process_artificial_rotate_event` compares only the binlog name (`collector_context.cpp:518-533`). The position check exists only in position mode.
- So a different server with the same binlog names is accepted silently.

**A3. Data is assumed durable before metadata. It is not.** (ticket 19, confirmed in code)
- `do_write_data_to_stream` does `ofstream::flush()` but no fsync. The code says so itself: `// TODO: ... use fsync()` at `filesystem_storage_backend.cpp:229-230`.
- `do_put_object` fsyncs both the tmp file and the directory (`:147,159`).
- Recovery at `storage_core.cpp:960-984` assumes that `actual_size >= metadata.size`, and that a mismatch only ever affects the *last* binlog. After a power loss either assumption can fail, and both failures are fatal logic_errors (`:969`, `:975`).
- The previous binlog was never fsynced either, so a torn earlier binlog wedges the storage.

**A4. Creating a binlog is assumed atomic. It is three separate writes.** (ticket 15, confirmed in code)
- `open_new_binlog_file_internal` (`storage_core.cpp:680-703`) does three things in order: create and write the data file (magic bytes), `save_binlog_metadata`, `save_binlog_index`.
- A crash between any two of them leaves an object that the strict validators reject on the next start (`:779-783`, `:993-996`). Recovery handles only the size mismatch. It does not handle orphan objects.
- Purge has the same shape: the index is committed (`:397`), then the file removals are swallowed (`:419-430`). The comment admits the leftovers "will trip the constructor's validators on next startup". The code never reconciles them.

**A5. A failed write is assumed not to have happened.** (ticket 18, confirmed in code)
- `write_event_block` advances `size`, the added GTIDs and the timestamps in memory *before* `save_binlog_metadata` (`storage_core.cpp:307-321`).
- `flush_event_buffer_internal` erases the buffer only after a successful write (`storage.cpp:286-296`).
- `storage::~storage` flushes again and swallows every exception (`storage.cpp:68-77`). So if the metadata write fails transiently, the destructor appends the same block a second time with a consistent `size`, and the duplicate is permanent.
- The filesystem stream's failbit is never cleared after a write error (`filesystem_storage_backend.cpp:220-228`), so every later write on that stream fails too.

**A6. Only `CR_SERVER_LOST` is treated as transient. Every other mid-stream error kills pull, including its replication source.** (new)
- `connection::fetch_binlog_event` throws `core_error` for any errno other than `CR_SERVER_LOST` (`easymysql/connection.cpp:546-550`).
- `receive_binlog_events` catches `core_error` only around connect and the replication switch (`collector_context.cpp:212,263`). It does not catch it in the event loop (`:151-171`).
- Pull's retry loop (`pull_operation.cpp:133-156`) turns any exception into process exit. So any error during streaming, other than a lost connection, takes down the replication source too: storage I/O, S3 503, a server error packet (e.g. 1236), a parse failure.
- Under-documented: the code never states which network faults map to errno 2013 and which to other errnos.

**A7. Storage I/O is assumed fast enough to run on the network event loop.** (new)
- All replica sessions, the acceptor and the SIGINT/SIGTERM handler run on one `io_ctx.run()` thread (`pull_operation.cpp:99-104,160`).
- `sender_context::get_event` calls `storage::fetch_event_block` synchronously from that coroutine (`network_service.cpp:308`). That call holds the `shared_lock` across the backend GET (`storage_core.cpp:461-542`).
- The collector holds the `unique_lock` across the whole data write and metadata PUT (`storage_core.cpp:305-321`). On S3 that is a full-object upload (ticket 17, `s3_storage_backend.cpp:850-863,933-938`).
- Result: one slow S3 PUT or GET stalls every replica, every new accept and signal-driven shutdown. Shutdown of the collector itself is bounded only by the MySQL `read_timeout`, because the blocking `fetch_binlog_event` does not check `io_ctx.stopped()`.

**A8. `accept()` is assumed never to fail transiently.** (new)
- In `listener` (`network_service.cpp:640-669`), any accept error other than `operation_aborted` or `bad_descriptor` is rethrown. Examples: EMFILE, ENFILE, ENOBUFS.
- The catch at `:666` logs it and the coroutine *ends*. The pull process stays alive, still collecting, but never accepts another replica.
- Classic log-and-swallow. Nothing checks liveness afterwards.

**A9. A directory listing is assumed to be a quiescent snapshot.** (new, filesystem)
- `do_list_objects` calls `dir_entry.file_size()` for each entry (`filesystem_storage_backend.cpp:106-115`). That is a fresh stat.
- If a concurrent writer renames a `.tmp` away between readdir and stat, the stat throws `filesystem_error`. So `list` and `search_by_*` (querying_only) can fail spuriously while fetch/pull is running, even though querying_only mode is explicitly meant to tolerate concurrent writers (`storage_core.cpp:189-195`).
- Any non-regular entry (lost+found, a subdirectory) is fatal (`:108-112`).

**A10. S3 is assumed to give strong read-after-write consistency and atomic overwrites on every endpoint.**
- `s3_storage_backend.cpp:815-819` states it and makes fsync a no-op. Custom endpoints are allowed: MinIO, Versity and other gateways.
- `get_object_into_byte_buffer` treats a short range response as a hard `out_of_range` (`:296-300`). A replica read that hits a briefly stale object therefore disconnects the replica.
- The code configures no retry, timeout or clock-skew handling. It relies on the SDK defaults (`:120-280`).

**A11. Several invariants are guarded only by `assert`, which disappears in Release builds.**
- `storage_core.cpp:219` and `:707`; `s3_storage_backend.cpp:315,365`; `network_io_operations.cpp:91-92,128-129,175`.
- No NDEBUG override exists in the CMake files, so a release build of the SUT runs without these checks.

**A12. Binlog offsets are assumed to fit in 32 bits.**
- The `reader_context` position is a `uint32_t` (`reader_context.cpp:49-50`). The collector casts the 64-bit storage size: `static_cast<std::uint32_t>(storage_->get_current_position())` at `collector_context.cpp:146,378`.
- Resuming a binlog larger than 4 GiB in position mode compares a 64-bit rotate position with a truncated one (`reader_context.cpp:552`), which raises a fatal logic_error. Hard to reach under Antithesis; low priority.

**A13. Event sizes are assumed to fit in one packet and in memory.**
- `generate_encoded_binlog_event` builds one frame per event. `async_write_mysql_frame` throws at 16 MiB or more (`network_io_operations.cpp:144`; ticket 10, confirmed).
- The sender accepts events up to 1 GiB (`protocol_traits_fwd.hpp:61`), but backend reads are capped at 256 MiB (`basic_storage_backend.hpp:35`). Events between those sizes cannot be served.
- Requested dump positions are never checked to fall on an event boundary. The garbage header that results is filtered only by `max_event_size_bytes` (`sender_context.cpp:360`). Ticket 7's below-19-byte loop is reachable this way.

**A14. Clock assumptions.**
- Checkpoint timing uses `steady_clock` (`storage.cpp:181,268`), so that part is safe.
- `search_by_timestamp` assumes binlog `min_timestamp`s don't decrease along the binlog sequence. It stops at the first binlog with `min > t` (`search_helpers.cpp:72-77`), so a source clock that steps backward hides later binlogs.
- In rewrite mode the generated ROTATE, FDE and PREVIOUS_GTIDS events carry PBS's *wall clock* (`event_generation_helpers.cpp:48,92,117,149`). Those timestamps feed the per-binlog timestamp ranges (`storage.cpp:152`), mixing the PBS clock with the source clock.

## Areas examined
- `storage_core.cpp` (all), `storage.cpp` (all), `filesystem_storage_backend.cpp`, `basic_storage_backend.cpp`
- `s3_storage_backend.cpp`: get, put, resize, stream and fsync paths
- `collector_context.cpp`, `pull_operation.cpp`, `fetch_operation.cpp`, `easymysql/connection.cpp` (fetch path and flags)
- `reader_context.cpp`: rotate, position and transaction checks
- `sender_context.cpp`, `minimysql/network_service.cpp`: dump loop, session, listener, exception handler
- `network_io_operations.cpp`: size limits
- `search_helpers.cpp`, `ctime_timestamp_range.cpp`
- Tickets 9, 15, 18, 19, 24, 25, 26 as leads

## Assumptions
- libstdc++'s `directory_entry::file_size()` does a fresh stat and is not cached from readdir (A9). I did not test this on the target toolchain.
- I believe the SUT is built as Release (NDEBUG) for Antithesis. If it is built as Debug, the A11 asserts become crashes instead of silent corruption.
- Pull's single io thread is inferred from `io_ctx.run()` being called once, on the main thread. I found no thread pool.

## Open questions
- [open] Which faults surface as `CR_SERVER_LOST` (2013), and which as other errnos that kill pull (A6)? Candidates: graceful source shutdown, partition plus read_timeout, RST, `KILL` of the dump thread. Needs observation under Antithesis.
- [open] With the default S3Crt SDK configuration, are 503/SlowDown and RequestTimeTooSkewed retried, and is clock skew corrected (A10)?
- [open] Do the maintainers intend errors in `purge_binlogs` and in mid-stream reads to be fatal? Or is pull supposed to survive them?
- [open] Is running purge concurrently with fetch/pull a supported deployment? The README allows it, per ticket 9. If so, the `.tmp` deletion race in A1 is a real bug. If not, nothing enforces the restriction.
