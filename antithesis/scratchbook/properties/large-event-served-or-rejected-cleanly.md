# large-event-served-or-rejected-cleanly — large binlog events are stored and served, or refused with an explicit error, never a silent loop

## What led here

Resource focus: "16 MB event frames". Ticket 10 is validated in sut-analysis: `connection_context.cpp:348-350` builds a single frame of `size+1`, and `network_io_operations.cpp:144` rejects any frame `>= max_payload_size`. There is also a second, larger boundary: backend reads are capped at `max_memory_object_size` = 256 MiB (`basic_storage_backend.hpp:35`), while the sender accepts events up to 1 GiB (`protocol_traits_fwd.hpp:61`, `sender_context.cpp:360`).

## Code paths

- Egress: `network_service.cpp:322-325` → `generate_encoded_binlog_event` → `async_write_mysql_frame` throws `message_size` for events of about 16 MiB or more. The session's `catch (...)` (`:621-623`) logs and closes the socket. **No MySQL error packet** is sent. A reconnecting client asks for the same position again and hits the same event every time, so the client is stuck forever.
- Sender fetch sizing: `sender_context.cpp:371-378` tops the buffer up to `first_event_size`. That makes N concurrent sessions at a large event hold N copies (RSS ≈ N × event size).
- Events above 256 MiB: `get_object` throws `out_of_range` on S3 (`s3_storage_backend.cpp:281-285`). On the filesystem, `read_file_content(..., max_memory_object_size, …)` applies the same cap. That turns into a session exception and a silent close.
- Ingress: PBS sets no `MYSQL_OPT_MAX_ALLOWED_PACKET` (only the SSL and timeout options in `easymysql/connection.cpp:225-310`). Whether libmysqlclient's default limit accepts large replication packets is unverified. If it does not, the error is not 2013, so pull/fetch exits (`connection.cpp:546-551`) and stays in a crash loop at the same event.
- The collector buffers the whole transaction in `storage::event_buffer_` (`storage.cpp:150-151`) until its boundary.

## Oracle

- The workload sometimes inserts rows with BLOBs of 8, 15.9, 16, 17 and 64 MiB (source `max_allowed_packet` ≥ 128 MiB, `binlog_row_image=full`), alongside normal traffic, with 1 to 4 dump clients.
- `Always`: when a dump client connected at a position before a large event ends its session, it either received the event intact or received an explicit MySQL error packet. Detect this with `mysqlbinlog --read-from-remote-server` exit status and stderr, or with a raw-protocol client.
- `Always`: fetch/pull ingests every event the source produced up to the source's `max_allowed_packet` (storage prefix check). Otherwise PBS exits non-zero with a logged reason, and never exits 0 short of the source.
- `Sometimes(a ≥ 16 MiB event was delivered to a dump client)`: the liveness half. It fails today (ticket 10), so mark it as a known failure.

## Antithesis angle

Mostly deterministic, but faults matter for the memory side. Several clients that reconnect after network resets all re-fetch the same big event. Node throttling stretches the time those buffers are held. Put a container memory limit on PBS so that RSS growth shows up as an OOM kill.

## Instrumentation suggestions (missing)

- `UNREACHABLE("binlog event exceeds frame limit")` at `network_io_operations.cpp:144` before the throw. Today it would fire on ticket 10 and act as a regression marker once that is fixed.
- `SOMETIMES(first_event_size > block_size_)` at `sender_context.cpp:371`, to confirm the workload creates multi-block events.

## Corrections after investigation (2026-10-13)

- **Exact egress threshold.** The frame buffer is a 4-byte header + 1-byte OK marker + the event. `async_write_mysql_frame` rejects buffers `>= 16 MiB` (`network_io_operations.cpp:144`, `max_payload_size` = 16,777,216 in `network_io_operations_fwd.hpp:27-28`). The first event size that fails is therefore **16,777,211 bytes**, not "about 16 MiB". Use event sizes that straddle this point: 16,777,210, 16,777,211, 16,777,214, 16,777,215 and 16,777,216. Today the length wrap-around cases from ticket 10 (16,777,214 and up) are hidden behind the write rejection. They become reachable as soon as the size check is relaxed, which makes them the regression cases for a partial fix.
- **Ingress is not the bottleneck** up to 1 GiB (see log). The ingress `Always` ("fetch/pull ingests every event up to the source's `max_allowed_packet`") stays. Its failure mode is no longer "client packet limit" but storage-side limits, such as the 256 MiB backend read cap that affects later serving.
- **Existing coverage.** `mtr/binlog_streaming/t/rs_huge_event.test` (20 MB row, commit 7a52316, on this branch only, not upstream) shows the defect end to end. `fetch` stores the event, and `mysqlbinlog` then fails with "Lost connection" while PBS logs "frame payload size too large to send". `rs_large_event.test` (1.5 MB) is the passing control for the top-up path, with the log line `sender : block too small for first event`.
- **Liveness target confirmed.** The `Sometimes(a ≥ 16 MiB event was delivered)` is the target behaviour, not an optional extra (see log). `Always(served intact or explicit ERR)` stays as the safety half and fails today too, because the session ends with a silent close (`network_service.cpp:621-623`).

## Open questions

- None.

### Investigation Log

#### What is libmysqlclient's effective max_allowed_packet for mysql_binlog_fetch when the option is unset?

- Examined: `src/easymysql/connection.cpp` (all `mysql_options` calls; no `MYSQL_OPT_MAX_ALLOWED_PACKET`); libmysqlclient sources in the MySQL 9.7.1 tree (`percona-xtrabackup/sql-common/client.cc:229-230, 7002-7003`; `libmysql/libmysql.cc:972-987`) and the 8.0.36 tree (`worktable_old/percona-xtradb-cluster/sql-common/client.cc:220, 6966-6967`); commit 7a52316 and ticket 10's end-to-end result.
- Found: `my_net_local_init` sets `net->max_packet_size = max(net_buffer_length, max_allowed_packet)`, where the client-global default `g_max_allowed_packet` is `1024L * 1024L * 1024L` (1 GiB). Per-connection `options.max_allowed_packet` overrides it only when it is non-zero, and PBS never sets it. The ingress limit is therefore 1 GiB, which equals the server's maximum `max_allowed_packet` and PBS's `max_event_size_bytes` (`protocol_traits_fwd.hpp:61`). Empirical support: in rs_huge_event, `fetch` stores the 20 MB event and the failure appears only on egress ("frame payload size too large to send" in the session log, ticket 10 "End-to-end result").
- Not found: the 1 GiB ingress path was not verified end to end (only 20 MB was). The libmysqlclient21 shipped in the runtime image is 8.0.x. Its default matches the 8.0.36 source read here.
- Conclusion: resolved. Ingress does not fail before egress for events up to 1 GiB. For events above 256 MiB, the backend read cap (`basic_storage_backend.hpp:35`) is the next limit, on the serving side.

#### Is an explicit refusal (an error packet) acceptable instead of supporting multi-frame events?

- Examined: ticket 10 (`.tickets-snapshot.txt:534-596`, "Expected behaviour"), `rs_huge_event.test` header comment, `generate_encoded_binlog_event` (`connection_context.cpp:342-358`), MySQL protocol packet splitting (also described in ticket 10).
- Found: ticket 10 expects events "of any size up to the MySQL limit [to be] sent as the MySQL server sends them: split into packets of at most 0xFFFFFF bytes". rs_huge_event is written to pass after that fix. A MySQL client cannot receive an event ≥ 0xFFFFFF payload bytes unless it is split, so refusing it would leave PBS unable to replace a source for any workload with large rows.
- Conclusion: resolved. Multi-packet delivery is the target, so the `Sometimes` is the liveness goal and a known failure today. "Explicit ERR" is acceptable only as the safety floor in the `Always`. No change to the assertion types.
