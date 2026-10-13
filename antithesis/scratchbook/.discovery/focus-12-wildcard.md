# Focus 12 — Wildcard findings (binlog_server @ 99de682)

None of the findings below matched a ticket in `.tickets-snapshot.txt` when I grepped for them, except where a ticket number is given. Each finding was checked in the code. Where a finding still needs a runtime check, it says so.

## Findings

**W1. Downstream readers only see flushed data. With the default config, that means closed binlogs.** `storage.cpp:30-40` leaves checkpointing disabled unless `checkpoint_size` or `checkpoint_interval` is set. Without them, `write_event` only buffers in RAM, and data reaches the backend only in `close_binlog` (on rotation) or at a disconnect (`collector_context.cpp:195-200`). `fetch_event_block` serves up to `record.size` (`storage_core.cpp:513-538`), which is the flushed size. So with the default config, `pull`'s replicas see nothing of the active binlog until the source rotates, which can take hours. The event buffer also grows to the full binlog size.
- Even with `checkpoint_interval` set, the check runs only inside `write_event` (`storage.cpp:187`). It is not a timer.
- Upstream heartbeats are dropped (`MYSQL_RPL_SKIP_HEARTBEAT`, `easymysql/connection.cpp:205`).
- Result: the last transaction of a burst becomes visible only at the next event, or at the `read_timeout` disconnect (60 s in the README).
- **False confidence:** `rs_blocking_dump.test:17-26` sets `checkpoint_interval=1s` and `read_timeout=3`, which hides this.
- Property idea: the delay before a downstream reader sees a committed source transaction is bounded.

**W2. The `pull` listener can stop accepting forever while the process looks healthy.** `listener()` re-throws any `async_accept` error other than aborted or bad_descriptor (`network_service.cpp:647-650`). The catch at `:667` only logs it, and the coroutine ends.
- The acceptor socket stays open. The kernel still completes TCP handshakes into the backlog, so a TCP-connect health check passes, but no client is ever served again.
- `EMFILE` is easy to reach: the number of sessions is unlimited, and a pre-auth session holds an fd for `read_timeout`.
- `fetch` keeps working, so the process does not exit.

**W3. One I/O thread, synchronous storage reads, and one storage-wide lock.**
- Every replica session runs on the single `io_ctx.run()` thread (`pull_operation.cpp`).
- `sender_context` calls `storage_->fetch_event_block()` synchronously inside the coroutine. That call takes a `shared_lock` and does backend I/O (an S3 `GetObject` on S3) under it (`storage_core.cpp:461,541`).
- The writer holds a `unique_lock` during the data write, two `fsync`s and the metadata `put_object` (`storage_core.cpp:305-321`). On S3 it also re-uploads the whole file (ticket 17).
- So one slow S3 call stops every session and the accept loop, and `std::shared_mutex` has no fairness guarantee.
- Possible effect (not checked): if many polling replicas starve the writer, the collector stops reading, which risks the source's `net_write_timeout` dropping the upstream connection. Downstream load would then reach the upstream link.

**W4. Binlog sequence numbers stop at 999,999.** `number_of_sequence_number_characters{6}` (`composite_binlog_name.hpp:30`): the constructor rejects values of 1,000,000 or more, and `parse()` expects the separator at length−7.
- Rewrite mode: `next()` throws at 999,999. `rewrite.file_size` can be as small as 1024 (`rewrite_config.cpp`), so at 1 MB/s the limit is about 17 minutes away.
- Source names: to check — MySQL may go on to `binlog.1000000`, which PBS would fail to parse at the ROTATE.
- No test reaches this (`thousand_binlogs.test` is the largest).

**W5. Rewrite mode stamps generated events with PBS's own clock, not the source's.** The generated FORMAT_DESCRIPTION, PREVIOUS_GTIDS_LOG and closing ROTATE use `ctime_timestamp::now()` (`event_generation_helpers.cpp:48,117,149`). They are written, so their timestamps feed each binlog's `max_timestamp` (and, with a slow PBS clock, its `min_timestamp`).
- When PBS catches up on history (the normal backup case), each rewritten file starts with a PREVIOUS_GTIDS_LOG event timestamped later than every transaction in the file.
- Not yet checked against mysqlbinlog: `--stop-datetime` exempts only FORMAT_DESCRIPTION, so it would stop at that PREVIOUS_GTIDS_LOG and output no transactions for that file. This would make point-in-time recovery over rewritten storage silently empty.
- Clock skew between PBS and the source changes what `list`/`search` report.

**W6. Rewrite mode also hides corruption.** Every rewritten event gets a freshly computed CRC (`force_add_checksum`, `rewriter.cpp` `rewrite()`). With `verify_checksum=false`, corruption in transit then becomes undetectable downstream, while non-rewrite mode keeps the source CRC. Storage encryption is CTR without a MAC (no GCM for data, `storage_core.cpp:1160` TODO). The opening checks compare sizes only (`storage_core.cpp:787` TODO).

**W7. The storage does not record whether rewrite mode was used.** `save_metadata` stores only `mode` and `encryption` (`storage_core.cpp:830`). Turning `rewrite` on or off, or changing `base_file_name`, against an existing storage is not detected:
- A changed `base_file_name` is silently ignored, because the name continues from `get_current_binlog_name()` (`collector_context.cpp:369-377`).
- Turning rewrite off mixes source-named files into the same index. If a source name matches an older rewritten entry, `open_binlog` throws "not the latest one" (`storage_core.cpp:274-278`), which wedges the storage.
- Related to ticket 25 (a storage has no source identity), but this is about config identity.

**W8. Filesystem storage requires a directory nothing else writes to, and does not say so.** `do_list_objects` throws on any entry that is not a regular file (`filesystem_storage_backend.cpp:108`), in every mode including `list`. A dedicated ext4 volume has `lost+found`, so pointing `storage.uri` at a mount root (Docker or Kubernetes volume) fails at start-up. Any stray file (a log, a keyring, `.DS_Store`) gives "not referenced in the binlog index". **This matters for the Antithesis compose volumes.**

**W9. S3 listing includes nested objects.** `ListObjectsV2` has no delimiter (`s3_storage_backend.cpp:419-487`), so nested keys are returned. A storage at `bucket/a` therefore sees the objects of `bucket/a/b` and refuses to open. Storages cannot be nested, and a storage at the bucket root cannot share its bucket.

**W10. A crash leaves temporary data that is never cleaned up.**
- **S3:** `fs_buffer_directory` holds a full local copy of the active binlog. Only the owned UUID directory is removed, and only in the destructor (`s3_storage_backend.cpp:640-650`). After SIGKILL or OOM, each crash leaves a copy of up to one binlog (1 GB) behind, and start-up never cleans it.
- **Filesystem:** `remove_temporary_objects` also runs in *purging* mode (`storage_core.cpp:150,621`). `purge_binlogs` therefore deletes the `.tmp` file of a `fetch` that is running at the same time, and that fetch's rename fails. This is the same mechanism as ticket 24, triggered by a different command.

**W11. The logger works against diagnosing a crash.**
- `file_logger` opens its file with `std::ofstream` and the default truncate mode (`file_logger.cpp:30`). A supervisor restart loop therefore erases the log of the crash that caused the restart.
- Write errors are ignored. After one ENOSPC the stream stays bad, so logging stops silently even once space is freed.
- There is no reopen for logrotate.
- Each idle blocking replica logs "sender : fetched EOF" at info level every 500 ms (`sender_context.cpp:384`, `network_service.cpp:294`), so log volume grows with the number of idle replicas. Every line takes the logger mutex, which the collector's per-event logging also uses.

**W12. Test scaffolding in the production listener.** It answers `select * from tbl` with Alice/Bob rows and `SELECT VERSION()` with "9.7.0" (`network_service.cpp:504,530`). It binds `0.0.0.0` with no TLS (`:680`). Storage encrypted at rest is served decrypted, in cleartext, to anyone who knows the single shared password.

**W13. Release builds remove 163 `assert`s.** The `release` preset is RelWithDebInfo, which defines NDEBUG. Some checks exist only as asserts: the flushed position matching the open offset (`storage_core.cpp:707`) and convergence of the GTID_TAGGED rewrite loop (`rewriter.cpp`). **Antithesis should use the debug or asan build, or these never fire.**

**W14. Oddities that do no harm today:**
- `purged_gtids_` is loaded from the *added* GTIDs of the front binlog, not its *previous* GTIDs (`storage_core.cpp:1003`). The assert at `:219` is a tautology. It is harmless only because purge refuses to empty the storage.
- The rewrite logical-clock translation (`fix_sequence_number_and_last_committed`) is sound: I checked across source rotations and local rotations.

## Areas examined
`storage_core.cpp/.hpp`, `storage.cpp/.hpp`, the filesystem and S3 backends, `collector_context.cpp`, `sender_context.cpp`, `pull_operation.cpp`, `network_service.cpp`, `rewriter.cpp`, `reader_context.cpp` (artificial ROTATE, ROTATE/STOP), `composite_binlog_name`, `event_generation_helpers.cpp`, logger, config validation, `rs_blocking_dump.test`, README, and the ticket snapshot.

## Assumptions
- MySQL binlog extensions go past 999999 (not checked here).
- mysqlbinlog's `--stop-datetime` exempts only FORMAT_DESCRIPTION (from memory, not checked).
- `boost::asio` `detached` swallows coroutine exceptions, so `io_ctx.run()` does not throw.

## Open questions
- Does W3 starvation actually drop the upstream connection under S3 latency together with N polling replicas?
- Is W1 the intended product behaviour, so that the README should require checkpointing for `pull` serving?
- Does W5 reproduce with `mysqlbinlog --stop-datetime` on rewritten storage?
- Which errors does `async_accept` really produce under fd exhaustion on the Antithesis kernel?
