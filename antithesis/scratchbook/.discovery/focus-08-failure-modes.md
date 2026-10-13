# Focus 8: Failure and Degradation Modes

Commit 99de682. All file:line refs are from the current tree. "Confirmed in code" means I traced the mechanism myself; ticket numbers are given only as cross-references.

## Findings

### F1. Pull mode retries only on "connection lost / timeout". Every other error ends the process (confirmed in code)
- `pull_operation.cpp:131-158`: the collector thread runs `receive_binlog_events` in a loop. A `false` return leads to a fixed `idle_time` sleep (`wait_for_interruptable`, `:56-75`) and a reconnect. The retry has no backoff and no limit. Any exception goes to `catch(...)`, which calls `io_ctx.stop()` and ends the whole process. The replication source stops with it.
- `connection.cpp:544-551`: `fetch_binlog_event` returns false only when `mysql_errno == CR_SERVER_LOST`. Any other errno raises `core_error`. `collector_context.cpp:125-205` does not catch it, so it ends pull.
  - The source can purge the requested binlog, which gives ER 1236 from the server. That error is not CR_SERVER_LOST, so pull exits instead of retrying, and every restart fails the same way, so it crash-loops. Failing closed may be the intended behaviour; a property should check that the process exits and the storage can still be opened.
  - Only connect and switch failures are softened (`collector_context.cpp:209-216, 227-267`, `core_error` only).
- Any parse, checksum or logic error while processing an event (`collector_context.cpp:154-156`, `:530`, the `reader_context` throws) also ends pull. Ticket 1 is one instance.
- **Messy state:** pull is "down" for collection *and* for serving at the same moment. A degraded mode where it keeps serving stored binlogs while the source is gone does not exist.

### F2. Mid-event disconnect is handled at transaction granularity (confirmed in code)
- On a `false` fetch: `discard_incomplete_transaction_events()` then `flush_event_buffer()` (`collector_context.cpp:195-200`; `storage.cpp:217-224`).
- On an exception: the explicit discard is skipped, but `~storage` flushes (`storage.cpp:68-77`, errors swallowed). `flush_event_buffer_internal` writes only up to `last_transaction_boundary_position_in_event_buffer_` (`storage.cpp:281-288`), so partial transactions never reach the backend.
- **Property candidate:** after any disconnect or kill, every stored binlog ends on a transaction boundary, and `size` in the metadata equals the file size or recovery can fix it.

### F3. A failed flush is retried by the destructor, which duplicates data (confirmed in code; ticket 18)
- `storage_core::write_event_block` (`storage_core.cpp:299-322`) runs `write_data_to_stream`, then `size += n`, then `save_binlog_metadata`. The event buffer is erased only after it returns (`storage.cpp:290-296`).
- If the metadata `put_object` throws (ENOSPC, EIO, S3 5xx), the data has already been appended and `size` has already grown. When the exception unwinds, `~storage` calls the flush again (`storage.cpp:73`). It appends the same bytes again and saves `size = old + 2n`.
  - If the second metadata save succeeds, the duplicate events are permanent. The next start accepts them because the metadata size equals the file size.
  - If the second save fails, the metadata still says `old` and the file holds `old + 2n`, so startup recovery truncates the file (`storage_core.cpp:965-990`). The data is lost but the storage opens again.
- If the *data* write fails:
  - Filesystem: `std::ofstream` failbits are sticky (`filesystem_storage_backend.cpp:217-230`), so the destructor retry also fails. Recovery truncates the partial write.
  - S3: the local tmp file is appended before the PUT (`s3_storage_backend.cpp:850-863`), so the retry writes `old + D + D`. The metadata says `old + n`, and the truncation happens to keep the first copy, so the result is correct.
- **Antithesis fit:** high. The trigger is an S3 or disk fault that hits *only* the `.json` PUT, followed by a recovery window.

### F4. Binlog creation is not crash-atomic (confirmed in code; ticket 15)
- `open_new_binlog_file_internal` (`storage_core.cpp:680-702`) writes the magic, then `binlog.NNN.json`, then `binlog.index`.
- `validate_binlog_index` (`:764-787`) rejects any object that the index does not list. Only `*.tmp` leftovers are cleaned up (`remove_temporary_objects`, `:597-630`).
- A kill or crash between those steps, or a failed index PUT or rename, leaves a storage that *no mode* can open. Query modes tolerate it because they skip validation (`:190-195`, `:768-770`).
- This is the highest-value crash-consistency target. It happens at every rotation, so FLUSH BINARY LOGS on the source widens the window.

### F5. Purge is crash-unsafe by design (confirmed in code; tickets 9 and 24)
- `purge_binlogs` (`storage_core.cpp:368-433`) commits the new index first, then removes the files on a best-effort basis. The code comment itself says that leftovers "will trip the constructor's validators on next startup" (`:391-397`, `:403-405`).
- A kill or I/O error between the index save and the removals therefore leaves the storage unopenable.
- No cross-process lock exists anywhere: a grep for flock, lockf, O_EXCL or a pid file found none. A concurrent pull keeps its stale in-memory `binlog_records_` and rewrites the purged names back into the index at the next rotation (`save_binlog_index`, `:789-799`). After that, the index references objects that no longer exist.

### F6. Filesystem data is never fsynced while metadata is (confirmed in code; ticket 19)
- `filesystem_storage_backend.cpp:229`: a TODO says no fsync is done for stream data.
- Metadata uses the full write-temp, fsync, rename, fsync-dir sequence (`:118-147`).
- After a power loss, the metadata size can be larger than the file size. Recovery refuses that case: "cannot perform recovery – size … bigger" (`storage_core.cpp:980-984`).
- **Antithesis fit: low.** A process kill keeps the page cache. This needs an OS-level crash or page-cache-loss fault.

### F7. In the replication source, synchronous storage I/O runs on the single io_context thread (confirmed in code)
- `io_ctx.run()` runs only on the main thread (`pull_operation.cpp:162`). The listener, every session, *and the SIGINT/SIGTERM handler* live on it (`:98-102`).
- `sender_context::get_event` calls `storage_->fetch_event_block` synchronously (`sender_context.cpp:~395`). That call takes a `shared_lock` around `backend_->get_object` (`storage_core.cpp:466, 532-533`). On S3 this is a network GET.
- The collector's `write_event_block` holds a `unique_lock` across `write_data_to_stream` (`storage_core.cpp:305-310`). On S3, that write re-uploads the *whole* binlog on every flush (`s3_storage_backend.cpp:862, 937-941`; ticket 17).
- **Result:** a slow S3 PUT by the collector blocks every replica session and also delays shutdown on SIGTERM. A slow GET by one replica blocks the collector's next checkpoint.
- **Hypothesis (not run):** long uploads stop the collector from reading the MySQL socket. The source's `net_write_timeout` would then drop the dump, leading to CR_SERVER_LOST, a reconnect, and a larger binlog, so each upload takes longer. This would be a degradation spiral.

### F8. Silent failures and resource leaks in the replication source (confirmed in code)
- `get_event` returning false leads to `co_return` with **no MySQL error packet** (`network_service.cpp:308-313`, `:597-601`). Causes include a requested binlog that is not in storage, an offset past the end, a corrupt tail, or ticket 26.
  - Real MySQL sends ER 1236 in this case. A replica that only sees the socket close will most likely keep retrying as if it were a network error. The client-side behaviour is an open question.
- A blocking dump never reads from the socket. It only polls storage every 500 ms (`network_service.cpp:296, 315-320`). A client that disconnects while caught up is therefore noticed only on the next event write. Until then its coroutine and fd stay alive.
- The listener ends for good on any accept error other than aborted or bad_descriptor, for example EMFILE or ENFILE (`network_service.cpp:647-652`, caught at `:665-666`). Collection then goes on while the replication source stays dead, and nothing reports it beyond one error log line.
  - Together with the fd leak above, this is a plausible "pull looks up but serves nothing" state.
- Exceptions inside a session are isolated per session (`:621-623`), which is good. EOF and RST are logged at info level.

### F9. A corrupted stored event hangs the whole process (confirmed in code; ticket 7)
- `indexed_event_block.cpp:31-50` advances by `event_size`. In release builds the size is not checked against the minimum (`assert` only), so `event_size == 0` loops forever and keeps growing `index_`.
- The loop runs on the io thread, so per F7 it also blocks the signal handling. The only bounds check is the `max_event_size_bytes` upper limit (`sender_context.cpp:compute_fetch_size_requirements`).
- Reaching it needs on-disk corruption, which Antithesis does not inject by default, so this is a lower priority.

### F10. S3 temp files leak after a kill (confirmed in code)
- The tmp dir is `temp_directory_path()/<new uuid>` for each process (`s3_storage_backend.cpp:563-571`). It is removed only in the destructor (`:640-651`).
- Opening in append mode downloads the *whole* current binlog into it (`:828-832`).
- Each kill -9 or OOM kill therefore leaks a full binlog copy, and nothing ever removes it. Repeated crash and restart cycles on S3 fill the local disk. Local ENOSPC then turns into F3 and F4 on the next flush or rotation.
- An `fs_buffer_directory` the user configures is not owned by the process, so it leaks the same way.

### F11. Shutdown latency
- In blocking mode the collector checks `io_ctx.stopped()` only between events (`collector_context.cpp:151`). On SIGTERM the `jthread` join (`pull_operation.cpp:161`) can wait up to the MySQL `read_timeout`, or for one S3 upload.

## Areas examined
- `operations/pull_operation.cpp`, `fetch_operation.cpp`, `collector_context.cpp`, `sender_context.cpp`
- `binsrv/storage.cpp`, `storage_core.cpp` (open, create, recovery, purge, fetch_event_block, validate), `filesystem_storage_backend.cpp`, `s3_storage_backend.cpp` (client config, put, stream, tmp dir), `basic_storage_backend.cpp`, `exception_handling_helpers.cpp`, `indexed_event_block.cpp`
- `easymysql/connection.cpp` (fetch error classification), `minimysql/network_service.cpp` (listener, session, dump loop, handle_exception), `network_io_operations.cpp` (timeouts)
- `util/file_operations_helpers.cpp`, `native_file_operations_helpers_posix.cpp` (fsync)
- Tickets 7, 9, 15, 17, 18, 19, 24 and 26 were used as leads. Each mechanism was re-derived from the code above. I did not run any repro.

## Assumptions
- libmysqlclient reports a read timeout or a peer RST as CR_SERVER_LOST (2013). If it reports CR_SERVER_GONE_ERROR (2006) in some paths, those disconnects also end pull (F1).
- The S3Crt client uses SDK default retry and timeout settings. No explicit `retryStrategy` or timeout is set in `aws_context` (`s3_storage_backend.cpp:186-236`).
- glibc `std::shared_mutex` behaviour is not relied on, because the readers are serialized on one io thread anyway.

## Open questions
- [open] What errno does the source cause when it shuts down, or when its dump thread is KILLed, mid-stream (1053, 1317, 2013)? If it is not 2013, a routine source restart ends pull (F1).
- [open] After a failed `PutObject` that read `tmp_fstream_` as its body, are eof or fail bits left set on the shared fstream, so that every later `write_data_to_stream` fails? No `clear()` or `seekp` call is visible (`s3_storage_backend.cpp:369-386, 850-863`).
- [open] Does a MySQL replica or `mysqlbinlog` treat the silent close in F8 as retryable (endless reconnects) or as fatal?
- [open] The 5 GB single-PUT limit (TODO at `s3_storage_backend.cpp:557`): can a rewrite-mode `file_size` setting or a huge transaction reach it?
- [open] Is the "fail closed" behaviour on a purged source binlog (F1) intended? The property should assert "exits non-zero with storage still openable", not "retries".
