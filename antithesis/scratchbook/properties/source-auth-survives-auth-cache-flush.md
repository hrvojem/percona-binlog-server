# source-auth-survives-auth-cache-flush — PBS re-authenticates after the source's auth cache is emptied (ticket 23)

Found by: focus 6, 10 (security; version compat)
Merged from: source-auth-survives-auth-cache-flush (focus 6), source-restart-reauth-liveness (focus 10)

## Synthesis notes

Liveness, P1 (focus 6 rated P1, focus 10 rated P2). Resolved to P1: a source restart is routine, the stall is silent (pull retries forever, fetch fails), ticket 23 is validated in code (no `MYSQL_OPT_GET_SERVER_PUBLIC_KEY` in `src/`, `connection.cpp:225-291`), and on 9.x there is no `mysql_native_password` fallback. Whether non-TLS caching_sha2 is in contract is an open question; it decides whether the `ssl.mode=disabled` variant is a regression test or a known-failure tracker.

Workload checks:
- `Sometimes("PBS stored a new transaction after a source restart or FLUSH PRIVILEGES")`, per TLS mode.
- `Always("no live pull without a Binlog Dump thread on the source after the quiet period")`.
- TLS configs only: `Always("transaction committed T s after the source is back appears in storage")`.

SUT-side: `SOMETIMES(reconnect succeeded after a failed connect)` and an errno-tagged `REACHABLE("connect failed with auth error")` at `collector_context.cpp:209-216`.

## What led here

- Ticket 23 ("fetch and pull cannot authenticate without SSL as a caching_sha2_password user"), validated in code. The ticket's reproduction on 8.4.8 and 9.7.2 shows `ssl.mode=disabled` failing full auth and `preferred` succeeding. The ticket's own repro note says SSL-disabled connections work only "until the next server restart or FLUSH PRIVILEGES, after the user has logged in once over SSL". The outcome therefore depends on *timing*: whether the source's in-memory caching_sha2 hash cache holds PBS's user at the moment PBS reconnects. Antithesis explores that interleaving well (source restarts, PBS reconnect loops, other clients logging in).
- Validation (competing explanation: reporter's config/environment). Discriminating detail: `src/easymysql/connection.cpp:225-291` sets only `MYSQL_OPT_SSL_*`, `MYSQL_OPT_TLS_*` and the three timeouts. There is no `MYSQL_OPT_GET_SERVER_PUBLIC_KEY` and no `MYSQL_SERVER_PUBLIC_KEY` anywhere in `src/` (grep for `PUBLIC_KEY` returns no hits), and `connection_config.hpp:43-44` has no option for them. Without either, libmysqlclient cannot do caching_sha2 full authentication over a non-TLS connection, so the defect is in PBS, not the reporter's setup.
- The version- and timing-specific part, which the ticket does not cover (focus 10): `caching_sha2_password` succeeds **without** RSA/TLS whenever the server has the account's hash in its in-memory cache, which any earlier full-auth login of that account fills. A **server restart**, `FLUSH PRIVILEGES`, or `ALTER USER` empties it. So a non-TLS PBS can run fine for hours (because some other client warmed the cache) and then fail permanently after the source restarts.
- Source-version matrix (focus 10):
  - 8.0: the account may be `mysql_native_password` (works without TLS);
  - 8.4: `mysql_native_password` is disabled by default;
  - 9.x: the plugin is removed, so `caching_sha2_password` is the only path and TLS (or a warm cache) is mandatory.
  The same harness config therefore behaves differently per source version.

## Code paths

- `src/operations/collector_context.cpp:207-216` `open_connection_and_switch_to_replication`: creates a fresh `easymysql::connection` from config each attempt. A connect/auth failure is logged ("unable to establish connection to mysql server") and returns false.
- `collector_context.cpp:125-205` `receive_binlog_events`: a false return means "sleep `idle_time`, retry" in pull (LG1). Pull therefore does not exit on a permanent auth failure: it retries forever, silently stale.
- fetch: the same failure exits non-zero.
- `src/easymysql/connection.cpp:225-291`: only SSL/TLS and timeout options; `connection.cpp:291-321`: TLS options applied only when `connection.ssl` / `connection.tls` sections exist. Without them libmysqlclient defaults to `SSL_MODE_PREFERRED`.
- `src/easymysql/connection_config.hpp:43-44`: no public-key options.

## Failure scenarios

- **Cache flush by source restart / FLUSH PRIVILEGES / ALTER USER (focus 6, 10)** — config `connection.ssl.mode = disabled`, PBS user created `IDENTIFIED WITH caching_sha2_password`. Some earlier TLS login (for example the workload's own admin connection using the same user, or PBS once with `preferred`) has filled the cache, so PBS connects and streams. Antithesis restarts the source container (or the workload runs `FLUSH PRIVILEGES` / `ALTER USER`). The cache is cleared. Every PBS reconnect now needs full auth, which fails with CR 2061 "Authentication requires secure connection". Pull loops forever, reports nothing, and serves stale data; fetch exits non-zero.
- **TLS configs (expected to hold)** — with `preferred`/`required` over a TLS-enabled source, full auth runs over TLS and should succeed.
- **Version-dependent outcome (focus 10)** — on 8.0 with a `mysql_native_password` account the non-TLS config keeps working; on 8.4/9.x it wedges as above.

## Oracle / workload checks

- After each source restart or cache-flush action, and after faults heal, wait up to T (for example `idle_time + connect_timeout + 30 s`). Then check that PBS storage has reached the source's binlog position (or that `performance_schema.threads` shows a Binlog Dump thread for the PBS user).
- `Sometimes("PBS stored a new transaction after a source restart or FLUSH PRIVILEGES")` as the liveness signal, per run and per TLS mode.
- `Always("no live pull without a Binlog Dump thread on the source after the quiet period")` at the end of each quiet period: `!(pbs_alive && quiet_period_elapsed && no_dump_thread_for_pbs_user)`.
- TLS-enabled configs only: `Always("transaction committed T s after the source is back appears in storage")`. For non-TLS configs the property *expects* a failure and documents the ticket.

## SUT-side instrumentation suggestions

- `SOMETIMES(reconnect succeeded after a failed attempt, "reconnect to source succeeded after a failed connect")` in `open_connection_and_switch_to_replication` (`collector_context.cpp:209-216`) — **missing**.
- `REACHABLE("connect failed with auth error")` on the connect-failure branch, with the libmysqlclient errno (2061 `CR_AUTH_PLUGIN_ERR` for the cache-miss wedge) in details, so triage separates the auth wedge from a network failure — **missing**. It must sit inside the `catch (const easymysql::core_error &e)` at `collector_context.cpp:212`. That catch currently drops the code; read it with `e.code().value()`.
- No Antithesis SDK in the repo (see existing-assertions.md).
- Existing (non-Antithesis): MTR `ssl_connection` test, which per the ticket runs step 1.0 with `ssl.mode = required`, so it does not catch this.

## Antithesis angle / fault requirements

- Node termination (or a workload-driven `mysqld` restart / `RESTART` SQL) of the source, optional `FLUSH PRIVILEGES` / `ALTER USER` from the workload, and network partitions on PBS<->source that force reconnects. Antithesis explores whether the cache is warm at PBS's reconnect instant (another client logged in before or after the restart).
- Harness variants: `ssl.mode=preferred` with a TLS-enabled source (expected to hold; regression guard), and `ssl.mode=disabled` (expected to fail; reproduces ticket 23 only when a cache flush lands between reconnects). Run across source versions 8.0 / 8.4 / 9.x.

## Investigation outcome (2026-10-13)

- **Errno resolved:** on a cache miss without TLS and without a public key, libmysqlclient fails with **CR_AUTH_PLUGIN_ERR (2061)** and the text "Authentication requires secure connection." (`sql-common/client_authentication.cc:775-778` and `:983-986` in the 9.6 source; `include/errmsg.h:124`). It is not 1045. PBS **discards the errno**: `open_connection_and_switch_to_replication` catches `easymysql::core_error` and logs only "unable to establish connection to mysql server" (`src/operations/collector_context.cpp:212-215`). The errno is available as `e.code().value()`, because `core_error` is a `std::system_error` built from `mysql_errno` (`src/easymysql/core_error_helpers_private.cpp:56-59`). The SUT-side `REACHABLE("connect failed with auth error")` must therefore read it inside that catch. The errno-tagged assertion has to be **added in the catch block**; the existing log line cannot carry it. Log-based triage cannot tell 2061 from a network failure today.
- **Fault availability resolved for this property:** the harness plan does not need node termination of the source. `mysql-source` runs `mysqld` under a restart-loop wrapper, and `parallel_driver_kill_mysqld.sh` SIGKILLs it (`deployment-topology.md:69`, `:178`, `:242`). The workload can also run `FLUSH PRIVILEGES` / `ALTER USER`. Both empty the caching_sha2 cache, which is what this property needs. Whether the tenant has node termination enabled remains a catalog-wide question (deployment-topology Open Questions). It no longer blocks this property.
- **Client/server compatibility resolved for the harness build:** the harness builds PBS against Ubuntu 24.04 `libmysqlclient-dev` (8.0.x), the same as CI (`.github/workflows/cmake.yml:25,143`). CI runs MTR, including `ssl_connection` with `preferred`/`required`/`verify_ca`/`verify_identity`, against 8.0.45, 8.4.8 and 9.7.2 (`cmake.yml:18-20,289-340`; `mtr/binlog_streaming/t/ssl_connection.test:75-110`). The TLS variants of this property therefore use a client/server pairing that CI already exercises. Packaged builds link Percona Server 8.4 LTS client libraries instead (`packaging/scripts/binlog-server_builder.sh:244,252`; `packaging/debian/control:10`). That pairing is outside the harness and not exercised by CI.
- The harness baseline is TLS-on with `ssl.mode: required` (`deployment-topology.md:70`). The baseline property is therefore "reconnect + re-auth liveness after a source restart / cache flush over TLS". The `ssl.mode=disabled` variant is opt-in, pending the remaining question.
- Property unchanged in invariant and assertion types.

## Open questions

- Is a non-TLS `caching_sha2_password` configuration supported / should the `ssl.mode=disabled` variant be in the workload at all? (needs human input) If the owners treat it as unsupported (no fix planned for ticket 23; documentation says "use TLS"), the property runs only with TLS configs, becomes plain reconnect liveness after a source restart (overlapping LG1 properties), and the disabled variant is a known-failure tracker. If a fix (adding `get_server_public_key`) is planned, the disabled variant becomes the regression test.

### Investigation Log

#### Is a non-TLS caching_sha2_password configuration supported?

- Examined: README connection/ssl section (README:567-584), `.tickets-snapshot.txt` ticket 23 (Open, Medium, a suggested fix adds `connection.get_server_public_key` / `server_public_key_path`), `src/easymysql/connection_config.hpp:43-44`, `deployment-topology.md:70`.
- Found: README documents `ssl.mode = disabled` as a valid value and says nothing that excludes caching_sha2 users. Ticket 23 is open with a proposed fix but no recorded decision. The harness plan already excludes the variant from the baseline.
- Not found: maintainer intent (fix vs. document "use TLS").
- Conclusion: `(needs human input)`.

#### Does the Antithesis tenant allow source container termination?

- Examined: `deployment-topology.md` (mysql-source design :66-74, test commands :178/:195, fault table :241-243, Open Questions :265).
- Found: the harness gets source restarts from a test-command SIGKILL under a restart-loop wrapper, independent of node termination. A cache flush can also be driven by SQL.
- Conclusion: resolved for this property. The tenant-level question stays catalog-wide.

#### Which errno does PBS see (2061 vs 1045)?

- Examined: `sql-common/client_authentication.cc` (9.6.0 source, `~/work/percona-xtradb-cluster`) `:585` `is_secure_transport`, `:690-700` and `:920-950` (`get_server_public_key` branches), `:775-778` and `:983-986`; `include/errmsg.h:124`; PBS `collector_context.cpp:207-216`, `core_error_helpers_private.cpp:40-60`.
- Found: 2061 `CR_AUTH_PLUGIN_ERR`, "Authentication requires secure connection." PBS swallows the code.
- Conclusion: resolved. The instrumentation note is updated: the assertion goes in the catch block.

#### Does libmysqlclient 8.0.x negotiate auth correctly with a 9.7 server in every TLS mode?

- Examined: `.github/workflows/cmake.yml`, `mtr/binlog_streaming/t/ssl_connection.test`, `packaging/scripts/binlog-server_builder.sh`, `deployment-topology.md:88,257`.
- Found: CI builds against Ubuntu 24.04 libmysqlclient 8.0 and runs `ssl_connection` (all TLS modes) against 9.7.2. The harness uses the same build.
- Not found: CI coverage for the packaged PS 8.4 client build. That build is not part of the harness.
- Conclusion: resolved for the harness. The packaged-build pairing is out of scope.
