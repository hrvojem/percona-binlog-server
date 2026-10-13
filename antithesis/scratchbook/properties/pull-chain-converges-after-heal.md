# pull-chain-converges-after-heal — After faults stop, pull reconnects, catches up with the source, and every client catches up with the storage

Found by: focus 7 (distributed coordination), focus 2 (concurrency), focus 5 (resource), focus 3 (failure recovery).
Merged from: pull-chain-converges-after-heal (focus 7), blocking-dump-catches-up-after-io-stall (focus 2), s3-pull-converges-despite-reupload-growth (focus 5), pull-reconnects-after-transient-source-fault (focus 3).

## Synthesis notes

> **Updated 2026-10-13:** T does not need checkpointing, the N7 clause is rescoped to "no exit from a network-only fault" and observed through the supervisor log, and there is a new EOF-mid-transaction lead. See "Property changes from investigation".

Liveness, P1 (focus 7 P1, focus 5 P1, focus 3 P1, focus 2 P2).

Main assertion (workload): `Sometimes("converged within T after a heal: stored tail == source position and every live blocking client holds every stored byte")`. T = read_timeout + idle_time + checkpoint_interval (+ connect_timeout) + margin. It is checked after ANTITHESIS_STOP_FAULTS mid-run, so faults can resume afterwards.

The N7 sub-check from focus 3 keeps its own message: workload `Always("pull still running after each healed transient source fault")`. Only errno 2013 is retried (connection.cpp:546-551). Any other transient error kills collection and also the replication source. Whether pull is expected to run under a supervisor is an open question, and the answer decides whether this clause stays.

SUT anchors:
- `SOMETIMES(native_error == CR_SERVER_LOST)`
- `REACHABLE("mid-stream fetch failed with non-2013 errno")`, with the errno in details
- `REACHABLE("pull reconnect attempt succeeded after idle")`
- `SOMETIMES(single checkpoint upload >= 64 MiB)`
- `SOMETIMES(source dropped the dump during an upload)`
- optional `UNREACHABLE("healthy client dropped by write timer after io stall")`

Mechanisms:
- whole-object S3 re-upload on every checkpoint, under the exclusive lock (ticket 17, README:671);
- a single io thread that does synchronous storage reads;
- a hypothesised degradation spiral: collector stall → source net_write_timeout → reconnect → bigger re-upload.

## What led here

- **Chain liveness (focus 7).** After partitions heal and the source stops writing, every hop should converge: the storage to the source, and every connected downstream client to the storage. This is focus 7's "replicas converging to the source's data through pull". The existing QA `source_change_test.py` "restart" scenario passes: pull reconnects to a restarted source and catches up. `network_fault_test.py` covers only PBS↔source, without downstream clients.
- **io-thread stall (focus 2).** Pull's single io thread multiplexes N replica sessions, the 500 ms poll timers, the read/write `steady_timer`s and the acceptor. It shares a `std::shared_mutex` with the collector thread. The collector holds the exclusive side across synchronous backend I/O, so long exclusive holds freeze every session. N sessions doing 1 MiB synchronous reads in turn delay the collector.
- **Re-upload growth on S3 (focus 5).** Ticket 17 is validated in sut-analysis: each write does a whole-file PutObject (`s3_storage_backend.cpp:850-863, 933-938`). README:671 documents the write amplification ("256M + 512M + 768M + 1024M = 2560M"), so the amplification itself is **intended**. The resource question is what it does to progress. Each checkpoint PUT costs O(current file size). It is made while `storage_core::mutex_` is held exclusively (`storage_core.cpp:305`), and the single io thread blocks behind it (SUT-analysis "Blocking I/O on the event loop", hypothesis W3, and the focus 8/12 spiral).
- **Transient-fault retry (focus 3).** LG1, README:378 claims that pull reconnects after network errors. The mechanism covers only two cases: connect/switch failures (`collector_context.cpp:209-216, 263-267`, which return false) and a mid-stream `CR_SERVER_LOST` (`connection.cpp:546-551`). Every other mid-stream error raises `core_error` and ends the whole process, including the replication source (`pull_operation.cpp:154-166`). This is lead N7 / assumption A6 ("only 2013 is transient"). The retry is a fixed `idle_time` sleep with no backoff (`pull_operation.cpp:56-76, 137-153`). That rules out a retry storm toward the source, but it also means no jitter and no cap.

## Code paths

**Reconnect / retry**
- Reconnect loop: `pull_operation.cpp:120-175` (fixed `idle_time`, no backoff; `:56-76`, `:133-158`, `:136-153`, `:137-153`). On a caught exception, `catch (...)` stores it and calls `io_ctx.stop()` (`:133-158`, `:154-166`). The process then rethrows and exits non-zero.
- `connection::fetch_binlog_event` (`connection.cpp:537-553`; check at `:546-551`, `:548`): `if (native_error != CR_SERVER_LOST) raise_core_error_from_connection(...)`.
- `collector_context::receive_binlog_events` (`:125-205`) has no try/catch, so a `core_error` from `fetch_binlog_event` propagates. Connect/switch failures return false (`collector_context.cpp:209-216, 263-267`). The timeout/disconnect path (`:189-200`, `:189-204`, `:195-200`) discards the incomplete transaction and flushes.
- Also fatal mid-stream: a parse/validation `logic_error` from `reader_context` (for example a truncated packet surfacing as a bad event), and a storage exception during a checkpoint (S3 partition).

**Freshness**
- An interval checkpoint fires only when the next event arrives (`storage.cpp:175-200`, triggered by size or interval). With no checkpoint config, pull clients see nothing of the active binlog until rotation, disconnect or shutdown (LG3).
- A read_timeout disconnect flushes (`collector_context.cpp:195-200`). With no new events, the tail becomes visible after `read_timeout`.

**Egress / sessions**
- Blocking dump: `network_service.cpp` `handle_binlog_dump_command` (~:279-334) loops `get_event` → `async_write_mysql_frame(..., write_timeout)`. At EOF it waits on `idle_timer` for 500 ms (`network_service.cpp:293, 314-321`). It sends no heartbeats. A request beyond the flushed size is closed silently (`:308-313`). A dead peer is noticed only on the next write.
- `network_io_operations.cpp` (~:140-170): the write races `write_timer` with `||`. The timer is constructed, and starts counting, right before the write. A thread stall during the write can let the timer win when both handlers become ready at once. Focus 2 did not establish which handler asio dispatches first in that case.

**Locking / storage I/O**
- `storage_core::fetch_event_block` holds `shared_lock` across `get_object` and decryption (lock wait at `storage_core.cpp:461`). `write_event_block` holds `unique_lock` across the data write and the metadata PUT (`storage_core.cpp:305`).
- glibc `pthread_rwlock` backs `std::shared_mutex` and prefers readers by default. Here all readers come from one thread, so they cannot overlap, and the gaps between them let the writer in. Total starvation therefore looks impossible, but the delay grows with N × GET latency.
- `storage_core.cpp:299-322` → `s3_storage_backend.cpp:850-863`: append to the local temp file, then `upload_tmp_stream_internal()` re-uploads the **whole** file (`:933-938`), then the metadata PUT. All of it happens under the unique lock.
- S3 timeouts and retries are left at the CRT defaults; they are not configured (`s3_storage_backend.cpp:193-196, 239-256`).

## Failure scenarios

**A. Asymmetric partitions, heal, converge (focus 7 trigger).**
- source↔PBS: the dump thread is stuck in a write while PBS times out.
- PBS↔client: the client never notices, and PBS's session waits on a write timeout.
- PBS↔S3: the collector blocks under the storage lock, which freezes every session.

Convergence after the heal exercises reconnect, eviction of the zombie dump thread on the source (same server_id), and client resume at (file, pos).

**B. io stall drops healthy clients or delays catch-up (focus 2 trigger: S3/network latency, thread pausing).** K blocking clients and the collector contend on the shared mutex. A stall during a frame write lets `write_timer` win, so a healthy client is disconnected with "frame write timeout". Hypothesis (sut-analysis W3, focus 8/12): the collector stalls → the source hits `net_write_timeout` → reconnect → a larger append-mode download/upload on S3 → longer stalls. That would be a degradation spiral.

**C. S3 re-upload growth (focus 5 trigger: MinIO under node throttling or network latency).** Settings: `max_binlog_size` 256 MiB on the source, `checkpoint_size` 1 MiB. Near the end of a binlog, each checkpoint uploads about 256 MiB. While the PUT runs, the collector does not read from the source socket. The source's dump thread blocks on `net_write_timeout` (MySQL default 60 s) and may drop the connection. PBS then sees `CR_SERVER_LOST`, `receive_binlog_events` returns, discards the incomplete transaction, flushes (another whole-object PUT), sleeps `idle_time` and reconnects (`pull_operation.cpp:136-153`, `collector_context.cpp:189-204`). During the PUT, the io thread's `fetch_event_block` waits for the shared lock (`storage_core.cpp:461`), so sessions, the acceptor and the SIGTERM handler all stall. The data stays correct, but progress may stall: each resume adds one more full upload and `idle_time` of sleep. The hypothesis is that under sustained load the lag grows without bound. If a non-2013 error appears (for example a TLS read error mid-stream), pull exits (N7).

**D. Transient source-side fault ends pull (focus 3 trigger: N7).**
- A source link partition long enough to hit `read_timeout` → 2013 → reconnect. Expected to recover.
- A TCP reset, or the source being killed → libmysqlclient may report 2013 or 2006 (CR_SERVER_GONE_ERROR). 2006 is fatal.
- A graceful source shutdown → ER_SERVER_SHUTDOWN (1053) or a STOP event. A STOP event is processed as a file close, then EOF/2013.
- The DBA kills the dump thread (`KILL <id>`) → 1317 or 2013?
- An S3 partition during a checkpoint → a storage exception → fatal. This is not source-side, but the outcome is the same: a transient fault ends pull.

## Oracle / workload checks

Setup: pull runs continuously, ideally under a supervisor that restarts it after a fatal exit. The source keeps committing, then freezes writes. K blocking `mysqlbinlog --stop-never --raw` clients read from pull. Faults include S3/network latency and partitions on source↔PBS, PBS↔client and PBS↔S3. Each check is evaluated per quiet period (`ANTITHESIS_STOP_FAULTS`), mid-run, so faults can resume afterwards. Before checking, wait for a settle period. Optionally run one `FLUSH BINARY LOGS` on the source to force a flush, or set `checkpoint_interval` (see LG3 in the open questions).

| Workload assertion | Type | Notes |
|---|---|---|
| "converged within T after a heal: stored tail == source position and every live blocking client holds every stored byte" | `Sometimes` (per heal cycle) | Main assertion. (a) The storage holds every transaction in the source's binlogs: byte-equal up to a magic-only tail, and allowing ticket 14's missing ROTATE in GTID mode. (b) Every (re)connected blocking downstream client holds every stored byte. This also covers focus 2's "blocking dumps caught up after stall" and focus 5's "stored tail size equals the source position and all live dump clients have received it". If this never fires across the run, the chain did not converge. |
| "stored size reaches the source binlog size and every client equals the store within T after quiescence" | `Always` (eventually-style command) | Bounded form from focus 2, run in an `eventually_` command. Use it only if the owners accept a bound (see open questions). |
| "pull still running after each healed transient source fault" | `Always` (once per quiet period) | N7 sub-check from focus 3. Poll for up to `idle_time + connect_timeout + slack`. Pull must still be running, and the storage's tail GTID set/position must include a transaction committed after faults stopped. Bounded liveness, so a failure is a real finding: pull died or wedged. Stays only if pull is not expected to run under a supervisor. |
| "workload: pull reconnected in-process and caught up after a heal" | `Sometimes` | Coverage hint paired with the clause above, so a vacuous pass (no fault ever hit pull) is visible. |
| "pull never hangs silently: alive or exited with a logged error, and SIGTERM at the end of the quiet period completes within read_timeout + longest S3 PUT + delta" | `Always` | From focus 5, LG2-style. |
| "healthy client dropped by write timer after io stall" | `Unreachable` (optional) | Counts clients disconnected with "frame write timeout" while the client container was never faulted. Becomes a safety check only if owners say such disconnects are unacceptable. |

## SUT-side instrumentation suggestions

The codebase has no Antithesis SDK, so every entry below is missing.

| Assertion | Site | Status | Proposed by |
|---|---|---|---|
| `SOMETIMES(native_error == CR_SERVER_LOST, "mid-stream fetch failed with CR_SERVER_LOST (retried)")` | `connection.cpp:546-551` | missing | focus 3 |
| `REACHABLE("mid-stream fetch failed with non-2013 errno")`, errno in details | `connection.cpp:548` | missing | focus 3 |
| `REACHABLE("pull reconnect attempt succeeded after idle")` | `pull_operation.cpp`, when `receive_binlog_events` gets past `open_connection_and_switch_to_replication` on iteration ≥ 1 | missing | focus 3 |
| `SOMETIMES(iteration_number > 1 && caught_up, "pull caught up with the source after a reconnect")` | `pull_operation.cpp` reconnect loop | missing | focus 7 |
| `REACHABLE("collector flushed on read_timeout disconnect")` | `collector_context.cpp:189-200` | missing | focus 7 |
| `SOMETIMES(sent_bytes_after_idle_poll > 0, "blocking dump delivered data after idling at EOF")` | `network_service.cpp` blocking dump loop | missing | focus 7 |
| `SOMETIMES(single checkpoint upload >= 64 MiB)` | `s3_storage_backend.cpp` `upload_tmp_stream_internal` (`:933-938`) | missing | focus 5 |
| `SOMETIMES(upload bytes / newly appended bytes > 64, "checkpoint upload amplification exceeded 64x")` | same; record bytes per call and the ratio to newly appended bytes | missing | focus 5 |
| `SOMETIMES(source dropped the dump during an upload)` | collector, `CR_SERVER_LOST` observed while a checkpoint upload was in flight | missing | focus 5 |
| `SOMETIMES(unique-lock hold in write_event_block > 5 s)` | `storage_core::write_event_block` (`storage_core.cpp:299-322`) | missing | focus 5 |

## Key observations

- The retry is narrow. Only errno 2013 mid-stream and connect/switch failures are retried. Everything else, including a storage exception during a checkpoint, ends pull and its replication source.
- There is no backoff (fixed `idle_time`), so there is no retry storm, but also no jitter or cap.
- Freshness depends on configuration. Without checkpointing, the active binlog is invisible to clients until rotation, disconnect or shutdown (LG3).
- Every session, the acceptor and the SIGTERM handler share one io thread, which blocks on the storage lock while the collector does whole-object S3 PUTs.
- Reader preference in `pthread_rwlock`, combined with single-thread readers, makes total starvation unlikely. Delay still scales with N × GET latency.

## Fault requirements

- Network partitions, including asymmetric ones, and latency on source↔PBS, PBS↔client and PBS↔S3.
- TCP resets and source kill/restart, source graceful shutdown, and dump-thread `KILL` (workload-driven).
- Node throttling / CPU modulation / thread pausing on PBS and MinIO, to widen lock holds and upload times.
- ANTITHESIS_STOP_FAULTS (quiet periods) mid-run for the convergence checks.
- Workload config: a large `max_binlog_size` with a small `checkpoint_size` on S3 for the re-upload variant. Optionally `checkpoint_interval` set (see LG3).

## Open questions

- Is an S3 network error during a checkpoint meant to terminate pull? README:378 says network-related errors are retried and do not terminate. The S3 backend raises on a failed PUT, and `collector_context` has no catch for it, so pull exits. `(needs human input)`
- Is a replica disconnect on write timeout during a storage stall acceptable, given that the replica reconnects? If yes, only the eventual catch-up is asserted. If no, the dropped-healthy-client check becomes a safety property. `(needs human input)`
- Does the degradation spiral actually happen, or does the collector always recover once faults stop? This decides whether the property is liveness-only, or also needs a bounded-lag `ALWAYS`. `(partial: CRT fails a stalled transfer through its throughput monitor rather than hanging, and the failure then ends pull; whether the lag grows without bound needs a run)`
- What request timeout and retry policy does the S3 CRT client actually use in SDK 1.11.774? `(partial: no overall request timeout; connect 1000 ms; throughput monitor at lowSpeedLimit=1 B/s over max(3 s, requestTimeoutMs/1000); the default retry strategy is delegated to aws-c-s3 and its count is unverified)`
- Is any upper bound on catch-up time acceptable to the product owners? Until they answer, the bounded `Always` (adopted by the evaluation pass) uses a generous parametric T and reports the measured catch-up time in its details; their answer tightens T. `(needs human input)`
- **New lead (code-traced, not reproduced).** If the source sends an EOF packet in the middle of a transaction, does pull corrupt the stored binlog (GTID mode) or crash on reconnect (position mode)? The source does this after `KILL QUERY` on the dump thread, or during a graceful shutdown that stops a dump between events. See the Investigation Log.

## Property changes from investigation (2026-10-13)

- **T does not require checkpointing.** The convergence check runs after the source freezes writes. A frozen source makes the collector hit `connection.read_timeout`, and that path flushes (`collector_context.cpp:189-203`). So T = `connection.read_timeout` + `idle_time` (when a reconnect is needed) + 500 ms dump poll + write time + margin. `checkpoint_interval` is optional. LG3 (the active binlog is invisible while the source keeps writing without checkpointing) is a separate freshness concern and is not part of this property.
- **The N7 clause stays, with a narrower scope and a different observation.** The package ships no supervisor: `packaging/debian/rules` installs only the binary, and there is no systemd unit. README:378 claims that network errors are retried. The harness supervisor (`deployment-topology.md`) restarts pull, which would mask an exit. So the workload must detect exits from the supervisor's outcome log ("exit code + last error line"), not from "process running". The message becomes `Always("pull did not exit because of a network-only fault")`, where "network-only" means source↔PBS and PBS↔S3 network faults.
  - Transport faults on source↔PBS are always 2013 (see the log), so that leg is expected to hold.
  - The PBS↔S3 leg is expected to fail: a checkpoint PUT that exhausts the CRT retries raises out of `receive_binlog_events`. Whether that failure is a bug depends on the first open question.
- **The SUT anchor at `connection.cpp:548` changes meaning.** Non-2013 values there come only from server error packets (for example 1236) or 2020 (packet too large), never from network faults. Keep the anchor as a `REACHABLE` diagnostic.
- **New SUT suggestion:** `UNREACHABLE("blocking pull got EOF from the source with an incomplete transaction buffered", details: last_transaction_boundary vs buffer size)` at the case (3) return in `collector_context.cpp:184-187`. Missing.
- **New workload action:** `KILL QUERY <dump thread id>` on the source while a large transaction is streaming. Also run a graceful `SHUTDOWN` during a large transaction.

### Investigation Log

#### What is the intended freshness bound, and is the default no-checkpoint config expected to serve the active binlog? (LG3)

- Examined: README:377, :619-635 and :670-671 (checkpointing is documented as optional, and "if not set ... disabled"); `storage.cpp:143-200` (checkpoints fire only inside `write_event`); `collector_context.cpp:170-204` (case (2): read timeout → discard incomplete → `flush_event_buffer`); ticket 2 in `.tickets-snapshot.txt` ("checkpointing settings are ignored", fixed upstream).
- Found:
  - Without checkpointing, buffered data reaches the backend only at three points: rotation (`close_binlog`), a read-timeout disconnect, or shutdown.
  - The property's quiet period freezes source writes, so the collector always reaches the read-timeout flush within `connection.read_timeout`.
- Not found: any doc promising that the active binlog is visible while the source keeps writing without checkpointing.
- Conclusion: resolved for this property. T is based on `connection.read_timeout`, and `checkpoint_interval` is optional (see "Property changes"). LG3 is out of scope here.

#### Is pull expected to run under a supervisor (systemd `Restart=`)?

- Examined: `packaging/debian/rules:110-111` and `packaging/rpm/binlog-server.spec` (only `/usr/bin/binlog_server` is installed; no unit file); README:378; `pull_operation.cpp:120-175`; `collector_context.cpp:125-205` (catches only `easymysql::core_error` from connect/switch at :212 and :263); `s3_storage_backend.cpp:383` (`raise_s3_error_from_outcome` on a failed PUT); `deployment-topology.md` (the harness supervisor restarts pull).
- Found:
  - The product ships no supervisor.
  - README:378 claims that "any network-related error ... does not result in immediate termination". For source↔PBS transport faults this holds (see the next question). An S3 network failure during a checkpoint propagates uncaught and ends pull.
- Not found: maintainer intent about storage-side network errors.
- Conclusion: the supervisor question is resolved, and the property changes as described in "Property changes". A new, narrower question about S3 errors is tagged `(needs human input)`.

#### Which errno does each Antithesis fault type produce mid-stream (2013 vs 2006 / 1053 / 1317 / others)?

- Examined: libmysqlclient `sql-common/client.cc:1199-1221` and `:7578-7610` (`mysql_binlog_fetch`), and server `sql/rpl_binlog_sender.cc:366-383, 400, 465-495, 575-586`, `sql/sql_class.cc:1630-1665` (`THD::awake`), `sql/sql_class.cc:2264-2273` (`send_kill_message`), `sql/mysqld.cc:2310-2370` (`Set_kill_conn`), `sql/rpl_source.cc:1170-1171`. All of these are in a local MySQL 9.7.1-based tree (`~/work/percona-xtrabackup`). The code at these sites is long-standing, but 8.0/8.4 were not checked separately.
- Found:
  - **Transport failures always give 2013.** Any failed or zero-length read becomes `CR_SERVER_LOST`; `ER_NET_PACKET_TOO_LARGE` becomes 2020. This covers a partition hitting `MYSQL_OPT_READ_TIMEOUT`, an RST, a TLS read error and a killed source. So no network fault produces 2006, 1053 or 1317 mid-stream.
  - **Server error packets carry the server errno and are fatal in PBS.** An example is 1236 for purged binlogs or other sender fatal errors. That is fail-closed by design.
  - **`KILL <id>` (KILL CONNECTION)** → `THD::awake` calls `shutdown_active_vio()` → 2013 → reconnect.
  - **`KILL QUERY <id>` on the dump thread, and graceful shutdown.** In the shutdown case, `Set_kill_conn` sets `killed` without closing the socket. The sender loop then exits between events (`rpl_binlog_sender.cc:585`), and `cleanup()` sends **EOF** (`my_eof`). `send_kill_message` skips because the DA is already set. PBS receives an EOF packet: `fetch_binlog_event` returns true with an empty portion. That is "case (3)" in `collector_context.cpp:184-187`, whose comment claims it "can only happen in 'fetch', non-blocking" mode (:177). Case (3) returns **without** `discard_incomplete_transaction_events()` or a flush. Pull then sleeps and reconnects.
  - **Duplicate server_id.** The source kills the old (zombie) dump thread with KILL_QUERY + 1236 on the *old* socket, which PBS has already abandoned. No effect on the new connection.
- Consequence, code-traced and not reproduced: if case (3) arrives mid-transaction, `event_buffer_` keeps the partial events, and `get_current_position()` includes them (`storage.hpp:81-83`). The outcome depends on the replication mode:
  - Position mode: the reconnect requests the mid-transaction offset. The artificial ROTATE position matches (`collector_context.cpp:522-531`), the stream resumes mid-transaction, and `reader_context` rejects a non-GTID first event (`reader_context.cpp:776`). Pull exits (fail-closed).
  - GTID mode: no position check applies, and the source resends the whole transaction. The buffer then holds partial-prefix + full transaction, and the next boundary flushes both. The stored binlog is **silently corrupt** (a duplicated, incomplete transaction group).
- Conclusion: resolved. Network faults → 2013 → reconnect. KILL QUERY or a graceful source shutdown → EOF → case (3), which opens a new lead, recorded as an open question with its own SUT suggestion. The SUT anchors at `connection.cpp:548` remain useful runtime confirmation.

#### Is a replica disconnect on write timeout during a storage stall acceptable?

- Examined: `network_io_operations.cpp:140-180` (write raced against `write_timer`); README:601 ("wait to write data to a connected replica before treating the connection as timed out").
- Found: the README documents write_timeout as the replica-liveness mechanism. It says nothing about stalls caused by PBS itself.
- Conclusion: `(needs human input)`, a product decision.

#### Does the degradation spiral actually happen?

- Examined: the CRT configuration (below); `collector_context.cpp` (no catch around storage writes).
- Found: a stalled S3 PUT does not stall forever. Under the CRT throughput monitor it fails after a few seconds × retries, and the resulting `s3_error` ends pull rather than feeding a spiral. A spiral can then only come from slow, not stalled, uploads.
- Conclusion: `(partial: ...)`. Needs a run.

#### What are the CRT default request timeout and retry policy in AWS SDK 1.11.774?

- Examined: `s3_storage_backend.cpp:185-256` (a default-constructed `Aws::S3Crt::ClientConfiguration`; only region, scheme, virtual addressing and endpoint are set). Upstream aws-sdk-cpp tag 1.11.774, fetched from GitHub: `S3CrtClientConfiguration.h` (inherits `GenericClientConfiguration`; `crtRetryStrategyType = NOT_SET`, `maxRetries = 0` → "fall back to the default set by aws-c-io"), `S3CrtClient.cpp` init (`connect_timeout_ms = config.connectTimeoutMs`; when `lowSpeedLimit` is set, a throughput monitor with an interval of `max(3 s, requestTimeoutMs/1000)`; `requestTimeoutMs` is used *only* there), `S3CrtClientConfiguration.cpp` (an unrecognised or "default" retry strategy name → `nullptr`, so aws-c-s3 picks its own), `ClientConfiguration.h` (`connectTimeoutMs = 1000`, `lowSpeedLimit = 1`, `enableTcpKeepAlive = true`, doc says `requestTimeoutMs` defaults to 3000).
- Found:
  - CRT has no overall request timeout.
  - Stalled transfers are failed by the throughput monitor (<1 B/s for ≥ 3 s).
  - The retry count comes from aws-c-s3's internal default.
- Not found: the aws-c-s3 default retry count, and whether the monitor applies while a request waits on a blackholed connect or TLS handshake.
- Conclusion: `(partial: ...)`. During a blackhole, pull does not hang forever; it exits once retries are exhausted. This strengthens the N7 S3 leg.

#### Is any upper bound on catch-up time acceptable to the product owners?

- Examined: README (no latency or lag SLO anywhere).
- Conclusion: `(needs human input)`.

## Evaluation refinements (2026-10-13)

Applied by the evaluation synthesis (`../evaluation/synthesis.md`). Where this section and earlier text in this file disagree, this section wins; the catalog entry has been updated to match.

- Main assertion is now `Always("converged within T after a heal")`, evaluated once per quiet window that lasts longer than T, with T parametric and reported in details. `Sometimes("quiet period reached with ≥ 1 live blocking client and converged within T")` is kept as the vacuity guard: a `Sometimes` alone passes once any timeline converges, so the spiral and a never-catching-up client would go unreported (antithesis-fit F3). Quiet window = an `ANTITHESIS_STOP_FAULTS` window in which a harness quiet flag also pauses the harness's own fault drivers (kill drivers, proxy rules, fsfault rules, SIGSTOP), with the measured action started after the window begins and the window longer than the bound. Latency inside the window comes only from harness-bounded stimuli (an `s3_fault_proxy` stall of known seconds, SIGSTOP/SIGCONT of known duration) whose duration is added to the bound, and the bound is calibrated against a measured baseline of the coverage-instrumented debug build (lenses: antithesis-fit F2, implementability C3/C4, wildcard W10).
- N7 cause attribution: the supervisor outcome record carries the last error line of pull's console log and the custom-fault state at exit (quiet flag, proxy `GET /__faults__` snapshot, fsfault rule), so exits caused by same-`server_id` eviction, fsfault, a proxy rule or a known-defect wedge are excluded (implementability P9).
- The trailing "SIGTERM at the end of the quiet period completes within read_timeout + longest S3 PUT + delta" check is owned by `sigterm-honoured-within-bound` under the same quiet-window rule.
- Runs in `pull_fs`, `s3` (pull-like).
