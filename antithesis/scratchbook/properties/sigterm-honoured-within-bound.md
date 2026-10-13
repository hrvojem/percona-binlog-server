# sigterm-honoured-within-bound — SIGTERM stops fetch/pull within a bound, during startup and under storage-lock contention

Found by: focus 2, 8 (concurrency; lifecycle)
Merged from: sigterm-not-starved-by-storage-lock (focus 2), sigterm-during-startup-is-honoured (focus 8)

## Synthesis notes

Liveness, P1 (focus 2 rated P1, focus 8 rated P2). Bounded liveness is checked with `Always`, because an unbounded "eventually exits" cannot fail in a finite run.

Distinct workload `Always` messages:
- "pull exits within read_timeout + 1 s + slack after SIGTERM in steady state" (README:719)
- "fetch/pull exits within the startup bound after a SIGTERM sent before the first streamed event" (`B_startup` = storage-open bound + `connect_timeout` + `read_timeout` + slack)
- "next start after a startup SIGTERM opens the storage"

SUT-side: `SOMETIMES(SIGTERM arrived while a session was blocked in fetch_event_block)`, `SOMETIMES(SIGTERM delivered before io_ctx.run())`.

Mechanisms: the signal handler shares the single io thread with dump sessions (`pull_operation.cpp` ~:98-104), which park on `shared_lock` while the collector holds `unique_lock` across a whole-object S3 PUT (ticket 17). The handler runs only once `io_ctx.run()` starts (`:160`), after S3 client construction, listing, .tmp deletion, tail truncation and bind. The first `open_connection_and_switch_to_replication` (`collector_context.cpp:132`) does not check `io_ctx.stopped()`. Supervisors escalate to SIGKILL, turning a graceful stop into a crash mid-recovery.

## What led here

- (focus 2, steady state) The SIGINT/SIGTERM handler shares the single `io_ctx.run()` thread with every replica session (`pull_operation.cpp:98-104, 157`). Sessions call `storage_core::fetch_event_block`, which blocks on `shared_lock` while the collector holds `unique_lock` across backend writes. README:713-721 claims a graceful-stop worst case of `connection.read_timeout + 1` s.
- (focus 8, startup) The signal handler is installed before the storage is constructed, but nothing runs it until `io_ctx.run()` starts, which happens only after storage construction, recovery and (for pull) listener setup. README:719's bound silently assumes the process is already streaming or idle.

## Code paths (verified at 99de682)

### Steady state

- The signal handler is `signals.async_wait([&io_ctx](auto, auto) { io_ctx.stop(); })`, just an asio completion on the io thread (`pull_operation.cpp` ~:100). It cannot run while that thread is inside a synchronous call.
- Synchronous calls on the io thread: `storage_core::fetch_event_block` → `shared_lock` (waits for the collector's `unique_lock`), then `backend_->get_object` (an S3 GetObject with SDK-default timeouts) and decryption (`storage_core.cpp` ~:461-558).
- Collector-held `unique_lock` sections:
  - `write_event_block` (whole-object PutObject on S3, `s3_storage_backend.cpp:850-863, 933-938`, which grows with binlog size, ticket 17);
  - the metadata PUT;
  - `open_binlog` in append mode (downloads the object first, `s3_storage_backend.cpp:828-832`);
  - `open_new_binlog_file_internal` (magic upload + .json + index PUTs).
- After `io_ctx.stop()`, the collector notices only between events (`collector_context.cpp` loop `while (!io_ctx.stopped() && fetch_binlog_event...)`, `:151`), or within 1 s in the idle loop (`pull_operation.cpp` `wait_for_interruptable`). The jthread join waits for it.
- Final flush: `~storage` runs when the last `storage_ptr` owner dies. In pull this can be a suspended session frame destroyed in `~io_context`, after `signal_set` is destroyed (declaration order in `pull_operation.cpp`). A second SIGTERM at that point gets the default disposition and kills the process mid-flush.

### Startup (`pull_operation.cpp`)

- `:90-93` console logger, config parse, logger re-init (a `file_logger` truncates its file on open, `file_logger.cpp:30`). A signal here gets the default disposition (process dies, nothing touched; if PBS is PID 1 in a container, SIGTERM is **ignored**).
- `:98-104` `signal_set` registered. From now on SIGINT/SIGTERM are queued by asio.
- `:109-110` `std::make_shared<binsrv::storage>(…streaming)`: keyring load, S3 client construction (ticket 16: about 6 s IMDS wait when `AWS_EC2_METADATA_DISABLED` is not set), `list_objects`, `remove_temporary_objects`, metadata load/validate, possible `save_metadata`, index load, per-binlog metadata GETs, tail `resize_object` (on S3: download the whole object, truncate locally, re-upload, `s3_storage_backend.cpp:765-808`). All synchronous, no signal check.
- `:117-118` `network_service` constructor binds the port.
- `:125-158` collector jthread starts; `:160` `io_ctx.run()` — only now does the queued signal's handler run and call `io_ctx.stop()`.
- The collector's first `receive_binlog_events` (`collector_context.cpp:125-134`) calls `open_connection_and_switch_to_replication` (`:132`) **without** checking `io_ctx.stopped()`: connect (bounded by `connect_timeout`), in GTID mode on an empty storage `SELECT @@GLOBAL.gtid_purged`, then COM_BINLOG_DUMP(_GTID). Only then does the loop at `:151` see the stop.
- `fetch_operation.cpp:69-107` has the same order.
- Once the handler has fired, the `signal_set` has no pending wait, so further SIGINT/SIGTERM are queued and ignored until the `signal_set` is destroyed. An operator's second Ctrl-C does not force an exit; only SIGKILL does.

## Failure scenarios

- **Steady-state lock starvation (focus 2)** — S3 latency is injected (or network congestion between PBS and MinIO). The collector is mid-PUT of a 100 MB tail with the lock held. A blocking replica's 500 ms poll wakes and blocks on `shared_lock`. SIGTERM arrives. Stop latency becomes PUT time + read_timeout + flush time, which is unbounded by config. A supervisor's SIGKILL escalation (docker stop: 10 s) then turns a graceful stop into a crash.
- **Second SIGTERM during final flush (focus 2)** — after `signal_set` is destroyed, a second SIGTERM gets the default disposition and kills the process mid-flush in `~storage`.
- **Startup recovery transfer (focus 8)** — large S3 storage with a long tail: startup recovery downloads and re-uploads the tail. SIGTERM arrives; the process keeps working for the whole transfer plus one connection attempt. A supervisor with a short grace period (docker stop = 10 s) escalates to SIGKILL in the middle of the recovery re-upload. The re-upload is a single PutObject so the object should survive, but a local temp copy leaks (s3-crash-leftovers-bounded).
- **Startup with unreachable source (focus 8)** — the connection attempt blocks for `connect_timeout` after the signal; with default libmysqlclient settings this may be much longer than `read_timeout + 1`.
- **Container PID 1 (focus 8)** — a SIGTERM delivered during config parse is ignored entirely (no handler yet, PID 1 ignores default-terminate signals), so `docker stop` always escalates to SIGKILL if timed that way.

## Oracle / workload checks

- Steady state: send SIGTERM at random moments to pull while 1..N blocking dump clients are attached; measure until process exit. `Always(exit_latency <= read_timeout + 1 + slack, "pull exits within read_timeout + 1 s + slack after SIGTERM in steady state")`, where slack is a stated margin for S3 latency. Because the claimed bound clearly excludes S3 PUT time, the property may be split: (a) on the file backend the bound must hold; (b) on S3, a bounded-with-larger-slack form after a quiet period.
- Startup: start fetch/pull, send SIGTERM after a random delay in `[0, startup_duration]` (Antithesis chooses the delay), measure time to exit. `Always(exit_latency <= B_startup, "fetch/pull exits within the startup bound after a SIGTERM sent before the first streamed event")`, `B_startup = storage_open_time_upper_bound + connect_timeout + read_timeout + slack`. The bound is a parameter until the team states one.
- `Always("next start after a startup SIGTERM opens the storage")` after any such stop (reuses the stored-transactions-exactly-once-across-resumes / restart-convergence oracles).
- `Always(pull exit code == 0 || an error was logged, "signal-only stop of pull does not report failure")` — a fetch stopped by signal exits 1 by design ("did not reach EOF").
- Exploration: `Sometimes("SIGTERM sent before the first streamed event")` (workload side) must be observed true, otherwise the startup check is vacuous.

## SUT-side instrumentation suggestions

- `SOMETIMES(sigterm arrived while a dump client was blocked in a storage read, "SIGTERM arrived while a session was blocked in fetch_event_block")` — **missing**; reachability anchor for the steady-state window.
- `SOMETIMES(SIGTERM delivered before io_ctx.run())` — **missing**; hard to see from inside asio, so implement as `SOMETIMES(io_ctx.stopped(), "collector saw stop immediately after first connect")` right after `open_connection_and_switch_to_replication` returns in the first `receive_binlog_events` call.
- Optional `SOMETIMES(true, "storage construction finished")` at `pull_operation.cpp:111` to correlate phases — **missing**. Phase markers via log lines are enough for the workload.

## Fault requirements

- Process pause on PBS during startup; network latency/congestion PBS<->MinIO (stretches storage open and PUTs); source container stopped (stretches connect). No node termination needed.
- Workload-driven SIGTERM at Antithesis-chosen delays, with 1..N blocking dump clients attached for the steady-state variant.

## Open questions

- Is the README:719 bound meant to cover pull with a slow storage backend? If yes, any S3 latency fault is a violation. If no, assert the bound only on the file backend and use a looser form on S3. `(needs human input)`
- What stop latency does the team commit to during startup? README:719 states only the streaming-phase bound. If no startup bound is intended, the `Always` becomes "eventually exits" (liveness only) and only hangs are caught. `(needs human input)`
- Does a blackholed or packet-held S3 endpoint hang a PUT indefinitely? `(partial: SDK 1.11.774 source shows CRT throughput monitoring, 1 B/s over at least 3 s, plus 1 s connect timeout and default CRT retries, so attempts should abort within seconds; not runtime-verified that a stalled response wait trips the monitor)`
- Should the collector check `io_ctx.stopped()` before connecting? If yes, a signal during startup would never open a replication connection; if no, the extra connection is accepted and the bound must include `connect_timeout`. `(needs human input)`
- Is PBS expected to run as PID 1 in containers? If yes, the ignored-early-SIGTERM case is reachable in production and worth a harness variant without a supervisor. `(needs human input)`

## Answers from investigation (2026-10-13)

- **README:719 wording.** The README attributes the delay only to "the synchronous nature of the binlog API from the MySQL client library" and to the 1 s idle granularity. It never mentions storage writes. So the documented bound was reasoned from the MySQL read path alone. This supports splitting the check (strict bound on the file backend, looser on S3) until the owners answer.
- **S3 request timeouts (partial).** `s3_storage_backend.cpp:189-255` builds `Aws::S3Crt::ClientConfiguration` with defaults only (region, scheme, endpoint, virtual addressing). In SDK 1.11.774: `connectTimeoutMs = 1000`, `lowSpeedLimit = 1` byte/s, `requestTimeoutMs = 0` (core `ClientConfiguration.h`). `S3CrtClient::init` turns `lowSpeedLimit` into CRT `monitoring_options` with a failure interval of `requestTimeoutMs/1000` floored at 3 s, and uses the default CRT retry strategy (`maxRetries = 0` means the aws-c-io default, back-off up to 20 s). So a stalled S3 request is expected to fail and be retried within seconds rather than hang. The total time is bounded by retries times (3 s + back-off), which can still be tens of seconds. **Implication:** the S3 form of the check does not need `ANTITHESIS_STOP_FAULTS` to be finite, but its slack must cover CRT retries (order of a minute) unless faults are stopped first.
- **SIGKILL escalation (resolved).** The topology already composes it: `parallel_driver_kill_binlog_server.sh` sends SIGKILL / SIGTERM / SIGSTOP independently of `parallel_driver_ensure_pull.py`'s SIGTERM, so a SIGKILL or a second SIGTERM after a first SIGTERM occurs naturally. The window after `signal_set` destruction is real: in both `pull_operation.cpp:98-118` and `fetch_operation.cpp:69-85` the locals are `io_ctx`, `signals`, then storage, so `signals` is destroyed before `io_ctx`, and `~storage` can run in `~io_context` when a suspended session frame holds the last `storage_ptr`. Measure exit latency only for timelines where the SIGTERM was not followed by a SIGKILL, and judge storage outcomes after a SIGKILL with the crash properties.
- **PID 1 (partial).** `packaging/` ships only deb/rpm specs (no Dockerfile, systemd unit or entrypoint) and the README says nothing about containers. The harness avoids the case (`helper_supervisor.sh` is PID 1). So the production answer is unknown.

### Investigation Log

#### Is the README:719 bound meant to cover pull with a slow storage backend?

- Examined: README "Graceful termination" (:711-721), README `connection.read_timeout` (:567), `pull_operation.cpp:98-160`.
- Found: the README explains the delay only through the MySQL client library's synchronous read plus the idle sleep granularity. Storage, S3 and dump sessions are not mentioned.
- Not found: any statement of intent about storage-backend latency.
- Conclusion: `(needs human input)`. The wording suggests the bound was not reasoned for slow storage.

#### What stop latency does the team commit to during startup?

- Examined: README :711-721, `pull_operation.cpp:84-160`, `fetch_operation.cpp:60-107`, git log for signal-handling commits.
- Found: no startup bound is stated anywhere. The handler can run only from `io_ctx.run()` (`pull_operation.cpp:160`), after storage construction (`:109`) and bind (`:117`).
- Conclusion: `(needs human input)`.

#### What are the S3 CRT default request timeouts in SDK 1.11.774?

- Examined: `CMakeLists.txt:85` (`AWSSDK 1.11.774 EXACT`, component `s3-crt`), `s3_storage_backend.cpp:189-255, 371-385`; upstream aws-sdk-cpp tag 1.11.774: `S3CrtClientConfiguration.h`, core `ClientConfiguration.h`, `S3CrtClient.cpp` init.
- Found: PBS sets no timeouts or retry strategy. Defaults: `connectTimeoutMs = 1000`, `lowSpeedLimit = 1`, `requestTimeoutMs = 0`, `crtRetryStrategyType = NOT_SET` (matches the SDK default), `maxRetries = 0` (aws-c-io default), `maxBackoffSecs = 20`. `S3CrtClient::init` enables throughput monitoring when `lowSpeedLimit` is non-zero, with an interval floored at 3 s.
- Not found: a runtime check that a stalled response wait (body sent, response held) trips CRT's throughput monitor; aws-c-http's monitor semantics were not read.
- Conclusion: `(partial: ...)`. Indefinite hangs are not expected; the bound for S3 must include CRT retries.

#### Should a SIGKILL that follows the SIGTERM be part of the workload?

- Examined: `deployment-topology.md` (`parallel_driver_kill_binlog_server.sh`, `parallel_driver_ensure_pull.py`), `pull_operation.cpp:98-118`, `fetch_operation.cpp:69-85`.
- Found: the independent kill driver already produces SIGTERM-then-SIGKILL and double-SIGTERM sequences. Declaration order confirms `signal_set` is destroyed before `io_context`.
- Conclusion: resolved. Keep escalation in the workload; exclude escalated timelines from the latency `Always`.

#### Should the collector check `io_ctx.stopped()` before connecting?

- Examined: `collector_context.cpp:125-153`, `pull_operation.cpp:133-151`, git log of `collector_context.cpp`.
- Found: `receive_binlog_events` calls `open_connection_and_switch_to_replication` (`:132`) with no stop check; the first check is the loop condition at `:151`. No comment explains the order.
- Conclusion: `(needs human input)` (maintainer intent). Until answered, `B_startup` includes `connect_timeout`.

#### Is PBS expected to run as PID 1 in containers?

- Examined: `packaging/` (debian, rpm, builder script), README, `extra/` for Dockerfiles or entrypoints.
- Found: no Dockerfile, systemd unit or entrypoint ships with the product. The harness design runs `binlog_server` under a bash supervisor, never as PID 1.
- Conclusion: `(needs human input)`. The harness does not reach the PID-1 case; a no-supervisor variant is needed only if the owners say PID 1 is a supported deployment.

## Evaluation refinements (2026-10-13)

Applied by the evaluation synthesis (`../evaluation/synthesis.md`). Where this section and earlier text in this file disagree, this section wins; the catalog entry has been updated to match.

- The latency `Always`s are evaluated only inside quiet windows, with the SIGTERM sent after the window starts. Quiet window = an `ANTITHESIS_STOP_FAULTS` window in which a harness quiet flag also pauses the harness's own fault drivers (kill drivers, proxy rules, fsfault rules, SIGSTOP), with the measured action started after the window begins and the window longer than the bound. Latency inside the window comes only from harness-bounded stimuli (an `s3_fault_proxy` stall of known seconds, SIGSTOP/SIGCONT of known duration) whose duration is added to the bound, and the bound is calibrated against a measured baseline of the coverage-instrumented debug build (lenses: antithesis-fit F2, implementability C3/C4, wildcard W10).
- Native S3 latency, node throttling and pause stretch the latency without limit, so outside quiet windows the latency is recorded as `Sometimes("pull exit latency after SIGTERM exceeded the bound")` with the in-flight storage operation's duration in details: data for the README:719 question, not a failure (wildcard W10).
- Runs in `pull_fs`, `s3`, and `fetch_fs` for the startup half.
