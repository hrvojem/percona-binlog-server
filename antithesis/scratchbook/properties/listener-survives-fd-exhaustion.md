# listener-survives-fd-exhaustion — the replication-source listener outlives a transient accept() error

## What led here

Lead N6 and assumption A8 (focus 8, 9, 11, 12). Resource focus: fd exhaustion is the standard way to make `accept()` fail.

## Code paths

- `src/minimysql/network_service.cpp:629-669` `listener`: on any `listener_ec` other than `operation_aborted` or `bad_descriptor`, it **throws** (`:648-651`). The catch at `:666-668` logs and the coroutine returns. Nothing respawns the listener.
- `:673-686`: the acceptor is owned by `network_service` and stays open. The kernel keeps completing TCP handshakes into the backlog, so TCP health checks pass while no greeting is ever sent.
- `src/operations/pull_operation.cpp:98-161`: `io_ctx.run()` keeps running because of the `signal_set` and the collector thread. Pull stays "up" and keeps collecting, but it never serves again until restarted.
- What consumes fds: each session (unbounded count, a pre-auth session lives up to `replication_source.read_timeout`, 60 s in the sample config, `network_service.cpp:358, 397`), the logger file, S3 temp files and CRT sockets, filesystem `put_object` `.tmp` files (`filesystem_storage_backend.cpp:144`), and the `ofs_` stream.
- Under EMFILE the collector can fail too: a filesystem `put_object` cannot open its `.tmp`, the exception ends pull (`pull_operation.cpp:154-157`), and pull exits non-zero. Which component hits EMFILE first is timing-dependent. That race is the Antithesis angle.

## Failure scenario

Start PBS with a low fd limit (`ulimit -n 128`). The workload opens about 150 TCP connections to port 3307 and holds them pre-auth. `accept` gets EMFILE and the listener ends. The workload closes its connections. Pull keeps collecting, but every later client sits in the backlog forever: the replication source has failed silently.

## Oracle

- Workload probe after the flood ends and fds are released: connect and expect the server greeting within T (for example 2 × read_timeout).
- `Always(!(pbs_process_alive && probe_timed_out_waiting_for_greeting))`. "Pull exited non-zero" is an acceptable fail-closed outcome. "Alive but not serving" is not.
- `Sometimes(flood_caused_accept_error)`, confirmed through the SUT-side marker below, so we know the boundary was reached.
- SUT-side: `UNREACHABLE("listener exited on non-shutdown accept error")` at `network_service.cpp:650`. Missing.

## Antithesis angle

Faults are not needed. Node throttling or hang of the flood clients stretches pre-auth lifetimes. The interesting part is the ordering race between the collector's next `.tmp` open and the next `accept()`.

## Instrumentation suggestions

- `UNREACHABLE` at the throw site `network_service.cpp:650`. Missing.
- `SOMETIMES(accept returned EMFILE/ENFILE)`, to show the workload reaches the boundary. Missing.

## Open questions

- Should the number of concurrent sessions be capped? With no cap, the boundary is the process fd limit. A cap would not change the oracle. `(needs human input)`

## Findings from investigation (2026-10-13)

- **Supervisor question removed.** The package ships no supervisor (`packaging/debian/rules` installs only the binary). The harness supervisor cannot help, because the process stays alive. Both candidate fixes satisfy the oracle: retrying accept after a backoff, or exiting the process. "Alive but not serving" is the only failing outcome, so the fix choice does not affect the property.
- **The fd limit is set by the harness, not by compose.** The PBS supervisor entrypoint lowers the soft limit with `ulimit -n 256` (or `prlimit`) before it execs `binlog_server pull`. Lowering the soft `RLIMIT_NOFILE` needs no privilege, so the property does not depend on Antithesis honouring compose `ulimits:`. Run it only in a variant timeline, because the default limit would make EMFILE unreachable.
- **Which accept errors kill the listener.** Asio's `async_accept` retries `ECONNABORTED` (and `EPROTO`) internally when `enable_connection_aborted` is off, which is the default. Every other error completes the operation, and the listener throws at `network_service.cpp:648-651`: EMFILE, ENFILE, ENOBUFS and ENOMEM. The `REACHABLE`/`SOMETIMES` for accept errors should therefore record the errno, not only EMFILE.

### Investigation Log

#### Is pull expected to run under a supervisor that restarts it?

- Examined: `packaging/debian/rules:110-111` and `packaging/rpm/binlog-server.spec` (no unit file); `network_service.cpp:629-669` (the listener coroutine ends after `handle_exception`; nothing respawns it); `pull_operation.cpp:98-161` (`io_ctx.run()` stays alive because of the `signal_set` and the collector thread).
- Found: no shipped supervisor. Even with one, the process would not exit.
- Conclusion: resolved. The oracle accepts either fix, so the question does not affect the property.

#### What fd limit will the Antithesis container have?

- Examined: `deployment-topology.md` (pbs entrypoint is a bash supervisor; a compose `ulimits` variant is listed as `[needs-check]`).
- Found: the exact Docker default nofile limit on the Antithesis hosts is unknown. It is also unnecessary: the supervisor script can lower its own soft limit (`ulimit -Sn 256`) before `exec`, and the limit is inherited by `binlog_server`. No privilege or compose support is needed.
- Conclusion: resolved by harness design. The property needs a variant entrypoint flag.

#### Should the number of concurrent sessions be capped?

- Examined: `network_service.cpp:629-669`; README `replication_source` section (no limit option).
- Found: no cap and no knob.
- Conclusion: `(needs human input)`. It does not change the property.
