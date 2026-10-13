# stale-sessions-reclaimed-within-bound — Pre-auth and idle dump sessions are reclaimed within a bound

Found by: focus 5, 6 (resource; security)
Merged from: rs-preauth-session-lifetime-bounded (focus 6), idle-dump-session-reclaimed (focus 5)

## Synthesis notes

Liveness (bounded, checked with `Always`), P2 (both contributors P2). Two session states, both of which hold an fd and feed fd exhaustion (listener-survives-fd-exhaustion):
- unauthenticated connections must be closed within `B = 2·(2·read_timeout) + 3·write_timeout` (+ slack) of last server activity;
- a blocking dump session whose client vanished must be released within `write_timeout` after the next flushed data.

Distinct workload `Always` messages: "unauthenticated connection closed within the pre-auth bound", "PBS established connections on the listener port do not exceed live clients after flush + write_timeout". Workload `Sometimes("attacker closed by server timeout")`, `Sometimes("stale dump sessions > 0 before the flush")`. SUT `REACHABLE("pre-auth session ended by read timeout")`, `ALWAYS(pre-auth duration <= bound + slack)`.

At EOF a blocking dump never touches the socket (`network_service.cpp:314-320`), there are no heartbeats/keepalive, and timers share the io thread with blocking storage I/O.

## What led here

- (focus 6) The replication source accepts an unbounded number of connections (`network_service.cpp:643-665`) with no per-IP or global cap. Every connection costs an fd, a coroutine frame, and up to one 16 MiB-1 receive buffer (`network_io_operations.cpp` rejects only payloads ≥ 0xFFFFFF; the dynamic buffer grows as bytes arrive). The only thing that bounds what an **unauthenticated** peer can hold is the per-frame read/write timeout on the single io thread's `steady_timer`s. If pre-auth sessions outlive their bound, unauthenticated peers can exhaust fds and kill the listener (listener-survives-fd-exhaustion, N6) or memory. This property states the bound itself, which is the precondition for the listener property.
- (focus 5) Resource focus: "many replica sessions on one thread" and fd/memory leaks. Liveness lead LG4 (focus 5, 8): "a dead client is noticed only on the next write".

## Code paths

### Pre-auth sessions

- `src/minimysql/network_io_operations.cpp:60-130` `async_read_mysql_frame`: header read raced against a timer of `read_timeout`, then payload read raced against a re-armed timer of `read_timeout`. So one frame ≤ 2 × read_timeout even for a byte-at-a-time sender.
- `network_io_operations.cpp:150-180` `async_write_mysql_frame`: write raced against `write_timeout` (focus 5 cites the same function as `:136-176`).
- `src/minimysql/network_service.cpp:379-455` pre-auth sequence: write greeting, read greeting, optional write switch + read response, write ERR or proceed. Bound: `B = 2·(2·read_timeout) + 3·write_timeout` plus scheduling slack.
- Timers live on the same io thread that runs `fetch_event_block` (S3 GET + decrypt under the shared lock) and the collector-side lock waits (sut-analysis "Blocking I/O on the event loop"). A stalled storage call delays **every** timer, so the bound is really "B + longest io-thread stall".
- `steady_clock` is used, so clock jumps should not matter.

### Idle dump sessions

- `src/minimysql/network_service.cpp:307-321` `handle_binlog_dump_command` (EOF branch `:314-320`): at EOF a blocking dump sleeps 500 ms on a `steady_timer` and polls `sender_ctx.get_event` again. It **never reads** from the socket and **never writes** while there is no new data. PBS sends no heartbeats (the client sets `MYSQL_RPL_SKIP_HEARTBEAT`, and the listener has no heartbeat support).
- No TCP keepalive is set anywhere in `src/minimysql` or `src/operations` (grep for `keep_alive`/`SO_KEEPALIVE` finds nothing).
- A peer that closes (FIN) or vanishes (container killed, partitioned) is detected only when the next `async_write_mysql_frame` fails or hits `write_timeout` (`network_io_operations.cpp:136-176`). That happens only once new data has been flushed for that binlog.
- Each session holds a socket fd, a coroutine frame, a `sender_context` (carry buffer plus an `indexed_event_block`, up to 1 MiB, or more for big events) and a `storage_ptr`. It also logs "sender : fetched EOF" at info level every 500 ms (`sender_context.cpp:384`, under the logger mutex).
- The session count is unbounded (listener, `network_service.cpp:662-664`).
- EOF poll cost: `storage_core.cpp:523-532` returns before `get_object` at EOF (see open question).
- With the default config (no checkpointing), no new data becomes visible until the source rotates (LG3). That can take hours, and during that time no write is attempted.

## Failure scenarios

- **Slow-loris pre-auth (focus 6)** — the workload opens many raw TCP connections that send the 4-byte header of a 16 MiB frame and then trickle 1 byte every `read_timeout − ε`. Each read completes before its timer, but the payload timer is armed once for the whole payload, so the session should still close at about 2 × read_timeout. Concurrently, S3 latency stalls the io thread inside `fetch_event_block` for a blocking replica, so timers fire late and pre-auth sessions pile up past the fd limit.
- **Vanished dump client (focus 5)** — a downstream client reconnects on its own timeout (a MySQL replica with `replica_net_timeout`, or a supervisor-wrapped `mysqlbinlog --stop-never`) or is killed and restarted. Each reconnect adds a session, and the old one sits at EOF forever while the storage is idle. Sessions, fds and log volume grow linearly in time, and in the end this exhausts fds (feeding listener-survives-fd-exhaustion) or the disk used by the log.

## Oracle / workload checks

- Pre-auth: for each attacker connection, record open time and server-close time (EOF/RST observed). `Always(close_time − last_server_activity ≤ B + slack, "unauthenticated connection closed within the pre-auth bound")`, where slack is generous (for example 10 s) and is reported separately when S3 latency faults are active.
- `Sometimes("attacker closed by server timeout")` to confirm the timeout path ran.
- Idle dump: K "flaky" dump clients repeatedly connect, request a blocking dump at the current end, then close the socket or get killed (node hang or kill of the client container). Keep the source idle for a while, then write one transaction and wait for it to be flushed. After the flush plus `write_timeout` plus slack: `Always(PBS established connections on port 3307 ≤ live workload clients, "PBS established connections on the listener port do not exceed live clients after flush + write_timeout")`. Measure with a helper in the PBS container (`/proc/<pid>/fd` or `ss`; `extra/qa/network_fault_test.py:198` already counts fds).
- `Sometimes("stale dump sessions > 0 before the flush")` documents the idle-leak window, which is expected by the current design.
- Optional stricter variant (design question below): `Always("stale dump sessions reclaimed within T without new data")`.

## SUT-side instrumentation suggestions

- `REACHABLE("pre-auth session ended by read timeout")` in `handle_exception` when the session never authenticated — **missing**.
- `ALWAYS(pre-auth session duration ≤ B + slack, "pre-auth session duration within bound")` measured in `session` — **missing**.
- Active-session gauge: `SOMETIMES(active_sessions >= 8, "at least 8 concurrent listener sessions")` as an exploration hint — **missing**.
- `REACHABLE("dump session ended by write error at EOF wake-up")` on the session-end path caused by a write error at EOF wake-up — **missing**.

## Antithesis angle / fault requirements

- Node throttling or CPU modulation of PBS, S3 latency or stall injected while blocking replicas are dumping, and partitions that hold packets so that half-open sockets look alive. All of these stretch the io thread's response time, which is the only enforcer of the bound.
- Kills/hangs of downstream client containers (or workload-driven client kills) for the idle-dump variant; an idle source period followed by one transaction.

## Open questions

- What slack is acceptable for the pre-auth bound? The bound is only as good as the io thread's responsiveness. If the team accepts unbounded stalls under storage faults, assert this only in fault phases without storage latency. `(needs human input)`
- Should a cap on concurrent pre-auth sessions exist? There is none. If one is added, the property gains "connections above the cap are refused promptly". `(needs human input)`
- Should PBS detect dead idle dump clients without new data (heartbeats, keepalive, or reading the socket for FIN)? If yes, the stricter variant applies. If no, only the "reclaimed after next flush" form holds. `(partial: no heartbeat, keepalive or socket read at EOF in code; QA draft ticket 20 asks for heartbeats, maintainer position unknown)`

## Findings from investigation (2026-10-13)

- **The EOF poll does no backend I/O.** `storage_core::fetch_event_block` takes the shared lock (`storage_core.cpp:461`) and returns EOF before `get_object` (`:523-532`). An idle dump session costs only a shared-lock acquisition every 500 ms plus an info log line (`sender_context.cpp:384`), on top of its fd and memory. That shared-lock acquisition still queues behind the collector's unique lock, so idle sessions add io-thread work but no S3 requests.
- **The pre-auth bound is confirmed** against `network_service.cpp:379-480`:
  - The failed-auth path is greeting write, response read, optional switch write and switch read, then the ERR write: 2 reads × (2·read_timeout) + 3 writes = B. The successful path writes the fast-auth and OK frames and then leaves pre-auth.
  - **Authenticated non-dump sessions** are also bounded: the command loop reads with `read_timeout` (`network_service.cpp:~556`).
  - Only **blocking dump sessions at EOF** are unbounded. This scope is already in the property, so no change.

### Investigation Log

#### What slack is acceptable for the pre-auth bound?

- Examined: `network_io_operations.cpp:50-180` (each frame read uses a header timer plus a re-armed payload timer; each write is raced against `write_timeout`); `network_service.cpp:379-480` (pre-auth sequence); README:600-601.
- Found: B = 2·(2·read_timeout) + 3·write_timeout is confirmed. All timers run on the single io thread, which also does storage reads under the shared lock. Nothing in the docs states a tolerance for io-thread stalls.
- Conclusion: `(needs human input)`, an acceptable-slack policy decision.

#### Should a cap on concurrent pre-auth sessions exist?

- Examined: `network_service.cpp:629-669` (the listener `co_spawn`s every accepted socket); README replication_source section (no session or connection limit option).
- Found: no cap and no config knob.
- Conclusion: `(needs human input)`, a design decision. The property is unchanged until a cap exists.

#### Should PBS detect dead idle clients without new data?

- Examined: `network_service.cpp:279-334` (EOF branch: 500 ms `idle_timer`, no read or write); grep for `heartbeat|keep_alive|keepalive` in `src/minimysql`, `src/operations` and `src/easymysql` (only the client-side `MYSQL_RPL_SKIP_HEARTBEAT`, `connection.cpp:185-205`); `network_service.cpp:~560-585` (unknown queries such as `SET @source_heartbeat_period` get a syntax error); `.tickets-snapshot.txt` ticket 20 (QA draft, "Send heartbeat events while idle").
- Found: no liveness mechanism at EOF. The only signal that the client is gone is the next write after new data. The heartbeat request is a QA draft, not a maintainer decision.
- Conclusion: `(partial: ...)`. The maintainers must decide whether the stricter variant applies.

#### Does the sender's EOF poll do any backend I/O on S3?

- Examined: `storage_core.cpp:455-545`; `sender_context.cpp:382-390`.
- Found: on EOF, the function returns with an empty buffer before `backend_->get_object` (`:523-532`). On an empty storage it returns EOF before that point too (`:463-470`). It does take `std::shared_lock` at `:461`.
- Conclusion: resolved. The leak is fds, memory, log volume and shared-lock traffic, not S3 requests.

## Evaluation refinements (2026-10-13)

Applied by the evaluation synthesis (`../evaluation/synthesis.md`). Where this section and earlier text in this file disagree, this section wins; the catalog entry has been updated to match.

- The bounded `Always`s are evaluated only inside quiet windows, with the attacker's connect and the waking flush started after the window begins. Quiet window = an `ANTITHESIS_STOP_FAULTS` window in which a harness quiet flag also pauses the harness's own fault drivers (kill drivers, proxy rules, fsfault rules, SIGSTOP), with the measured action started after the window begins and the window longer than the bound. Latency inside the window comes only from harness-bounded stimuli (an `s3_fault_proxy` stall of known seconds, SIGSTOP/SIGCONT of known duration) whose duration is added to the bound, and the bound is calibrated against a measured baseline of the coverage-instrumented debug build (lenses: antithesis-fit F2, implementability C3/C4, wildcard W10).
- Close time is measured on the `pbs` side (`ss` or `/proc/<pid>/fd` from a `pbs` helper), not by when the client sees EOF or RST, which network faults on pbs↔workload can delay or swallow (antithesis-fit F2). The silent attacker and the vanished dump client are harness-driven sockets, not a node hang.
- The SUT `ALWAYS("pre-auth session duration within bound plus slack")` becomes a logged measurement (not an Antithesis assertion — a `Sometimes` on a failure state would be satisfied by the very bug it describes; the bounded workload `Always` evaluated in quiet windows carries the check): under a `pbs` node hang wall time advances while the container is frozen, and the binary cannot tell that from starvation.
- Runs in `pull_fs`.
