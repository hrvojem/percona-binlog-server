# stalled-dump-client-isolated — a stalled downstream client delays neither other clients nor the collector beyond write_timeout

## What led here

Resource focus: backpressure and "many replica sessions on one thread". All sessions and the signal handler share the single `io_ctx.run()` thread (`pull_operation.cpp:98-161`).

## Code paths

- `network_service.cpp:322-325`: each event is written with `async_write_mysql_frame(..., write_timeout)`. The write is asynchronous, so a full TCP send buffer suspends only that coroutine. After `write_timeout` the timer branch throws `timed_out` (`network_io_operations.cpp:151-165`) and the session ends.
- `sender_context::get_event` → `storage_core::fetch_event_block` is **synchronous** on the io thread and takes the shared lock (`storage_core.cpp:461`). It performs a backend GET of up to 1 MiB (a filesystem read or an S3 GetObject) plus decryption. These calls never block on a slow client, but they serialise all sessions.
- The collector takes the unique lock in `write_event_block` (`storage_core.cpp:305`) and on rotation. Investigation (see log) ruled out writer *starvation*. Every shared-lock holder on the serving side runs on the single io thread, so read holds never overlap, and the planned `debug_clang20` build uses libc++'s writer-preferring `shared_mutex`. What remains is a **bounded delay**: the shared lock is held across the synchronous backend GET and decryption (`storage_core.cpp:461` → `:545-559`), so the collector waits at most one in-flight fetch. That fetch can be slow when S3 is slowed by faults, or large (an exact-size top-up of a big event, up to 256 MiB).
- The session keeps its `event_block_` (the event data) in memory while suspended in a write. A slow client therefore pins up to one block (or one large event) per session until `write_timeout`.

## Failure scenario to look for

A client container is hung (Antithesis node hang) or its link is congested, so its TCP window closes. Expected: only its session stalls, and it ends after `write_timeout`. Possible violations: (a) some synchronous path in the session (logging under the logger mutex, a storage fetch) blocks the io thread so other sessions also stall; (b) many catching-up sessions starve the collector's unique lock, which backs up the source dump until the source's `net_write_timeout` drops PBS (upstream backpressure: the W3 hypothesis in the SUT analysis).

## Oracle

- Workload: M ≥ 4 healthy dump clients plus one deliberately non-reading client. Node hang or network faults on the client side also produce non-readers. The source writes steadily with checkpointing on (`checkpoint_interval=1s`).
- `Always`: each healthy client receives every transaction that the storage shows flushed (from the `.json` size) within `write_timeout + checkpoint_interval + δ`, while PBS↔storage and PBS↔source are fault-free. The bound must be generous, and the evaluation is gated on the absence of storage-side faults.
- `Always`: the non-reading client's session ends within `write_timeout + δ` **after PBS has more unsent data for it than the TCP send and receive buffers hold** (it is gone from the PBS connection table). This is gated (property change, see "Findings"). A stalled client that is caught up is never detected today.
- `Sometimes(collector reconnected because the source dropped it while ≥ 4 sessions were catching up)`: reports the upstream spiral if it happens. After the investigation, the plausible trigger is a slow S3 GET holding the shared lock (and the io thread), not reader starvation. Pair it with S3 latency faults from the fault proxy.

## Instrumentation suggestions (missing)

- `SOMETIMES(session ended by write timeout)` at the timer branch in `async_write_mysql_frame`.
- Time spent waiting for the unique lock in `write_event_block`, as a `SOMETIMES(wait > 1 s)` hint.

## Findings after investigation (2026-10-13)

- **Writer starvation hypothesis (W3 variant b) does not hold.** See the log. The collector's wait on `write_event_block` is bounded by one in-flight fetch.
- **A stalled client that is caught up is never reclaimed.** The dump loop (`network_service.cpp:302-327`) only writes when there is an event and never reads from the socket. At EOF a blocking session sleeps 500 ms and polls again (`:314-320`), and PBS sends no heartbeats (ticket 20). `write_timeout` applies only to a pending write (`network_io_operations.cpp:141-165`). `read_timeout` applies only to the command loop. So a client that stops reading, or whose node hangs, while caught up keeps its session (socket, `sender_context`, carry buffer) until PBS has enough new data to fill the TCP buffers and then waits a further `write_timeout`. If the source is idle, that never happens. This feeds `stale-sessions-reclaimed-within-bound`.
- **The io thread itself does synchronous backend I/O.** `fetch_event_block` runs the backend GET on the io thread. A slow S3 GET (fault proxy delay) therefore stalls **all** sessions, the listener and the SIGTERM handler for its duration, independent of any client. That is a more likely violation of the healthy-client `Always` than a slow client. The evaluation gate ("PBS↔storage fault-free") must stay, and a separate `Sometimes(io thread blocked on storage fetch > write_timeout)` hint is worth adding.

**Property change:** the second `Always` is now gated on "PBS had more unsent data for the stalled client than the socket buffers hold". The upstream-spiral `Sometimes` is re-aimed at storage latency. The first `Always` (healthy clients served within the bound when storage and source are fault-free) is unchanged.

## Open questions

- What latency bound is acceptable for healthy clients? None is documented, so the property is parametric in δ. `(needs human input)`

### Investigation Log

#### Does std::shared_mutex on the target libc prefer readers?

- Examined: deployment-topology.md (SUT build `debug_clang20`); `CMakePresets.json:56-63` (`clang20_hidden` sets `WITH_STDLIB_LIBCXX=ON`) and `CMakeLists.txt:45-49` (`-stdlib=libc++`); libc++ `<shared_mutex>` (LLVM 21 headers, `__shared_mutex_base` with `__gate1_`/`__gate2_` and the `__write_entered_` bit, lines 156-252); `storage_core.hpp:275` (`mutable std::shared_mutex mutex_`); every lock site in `storage_core.cpp` (`:231, 253, 261, 305, 327, 372, 461`); `pull_operation.cpp:98-161` (one `io_ctx.run()` thread plus one collector `jthread`).
- Found: the planned build links libc++, whose `shared_mutex` uses Hinnant's two-gate algorithm. Once a writer sets `__write_entered_`, new readers block at gate 1, so writers are not starved. The gcc14 presets use libstdc++, which wraps glibc `pthread_rwlock` with the default reader-preferring kind. Even there, starvation needs overlapping read holds. All serving-side shared locks are taken on the single io thread inside the synchronous `fetch_event_block`, and the collector's own shared reads happen on the collector thread, which is never waiting for its own unique lock at the same time. Read holds therefore never overlap, and the writer gets the lock at the next release in either library. The shared lock is held across `backend_->get_object` and decryption (`storage_core.cpp:461, 545-559`).
- Not found: no measurement of how long the hold lasts under S3 faults. The bound depends on the backend timeouts.
- Conclusion: resolved. Writer starvation is not plausible. The collector's delay is bounded by one fetch (backend GET + decrypt), which matters mainly when S3 is slow. Hypothesis (b) of the failure scenario is re-aimed accordingly (see Findings).

#### What latency bound is acceptable for healthy clients?

- Examined: README `replication_source` section (only `read_timeout`/`write_timeout` documented), `handle_binlog_dump_command` poll interval (500 ms, `network_service.cpp:293`), checkpoint docs.
- Found: the only documented timing knobs are `write_timeout`, the checkpoint settings and the hard-coded 500 ms poll. No serving SLA is documented.
- Conclusion: `(needs human input)`. The workload keeps δ as a parameter (suggested starting point: `write_timeout + checkpoint_interval + 500 ms poll + 2 s`).

## Evaluation refinements (2026-10-13)

Applied by the evaluation synthesis (`../evaluation/synthesis.md`). Where this section and earlier text in this file disagree, this section wins; the catalog entry has been updated to match.

- Both `Always`s are evaluated only inside quiet windows that also have no node throttling. Quiet window = an `ANTITHESIS_STOP_FAULTS` window in which a harness quiet flag also pauses the harness's own fault drivers (kill drivers, proxy rules, fsfault rules, SIGSTOP), with the measured action started after the window begins and the window longer than the bound. Latency inside the window comes only from harness-bounded stimuli (an `s3_fault_proxy` stall of known seconds, SIGSTOP/SIGCONT of known duration) whose duration is added to the bound, and the bound is calibrated against a measured baseline of the coverage-instrumented debug build (lenses: antithesis-fit F2, implementability C3/C4, wildcard W10).
- The stalled client is a harness reader in `workload` that stops calling `recv()`: healthy and stalled readers share the `workload` container, so a node hang or congestion there would hit both (implementability P4). Native client hang and congestion drive only the exploration `Sometimes` hints.
- Runs in `pull_fs`.
