# source-session-tls-when-configured — every PBS connection to the source is TLS when TLS is configured

## What led here

Security focus: TLS to the source. PBS opens a brand-new libmysqlclient connection on every reconnect (`src/operations/collector_context.cpp:209-211`), and pull reconnects often under faults. Each reconnect must re-apply the TLS settings. With `ssl.mode = preferred` (libmysqlclient's default when no `ssl` section is given), a reconnect *silently* falls back to cleartext if the server stops offering TLS. This is not a code defect in PBS, but it is the scenario where the source's credentials and binlog data cross the network in the clear.

## Code paths

- `src/easymysql/connection.cpp:291-321` `process_connection_config`: applies `process_ssl_config` (`:225-275`) and `process_tls_config` (`:277-291`) only when the optional sections exist.
- `connection.cpp` constructor (`:313` on): config is processed per connection object. `collector_context.cpp:209-211` builds a new one per attempt, so config is not lost across reconnects (verified by reading; not executed).
- README:572-584 documents `ssl.mode` values mirroring `--ssl-mode`.

## Failure scenario

Two variants:
1. Regression: some refactor reuses a `MYSQL*` handle across reconnects without re-applying options, or applies `tls` without `ssl`. The reconnect after a partition is cleartext.
2. Configuration-dependent: `ssl.mode` absent or `preferred`, and the source restarts with TLS unavailable (for example missing certificate files in a restarted container). Reconnect succeeds in cleartext with no warning. This is libmysqlclient-documented behaviour, so the oracle must treat `preferred` as "may be cleartext" and assert only for `required` and above.

## Oracle

- Workload polls the source: `SELECT CONNECTION_TYPE FROM performance_schema.threads WHERE PROCESSLIST_USER = '<pbs_user>'`.
- With `ssl.mode ∈ {required, verify_ca, verify_identity}`: `Always(connection_type == 'SSL/TLS')` for every observed PBS thread, on the single per-attempt connection, both while it runs `SELECT @@GLOBAL.gtid_purged` (`collector_context.cpp:229-233`) and after it becomes the Binlog Dump thread. It is the same connection; see Investigation outcome.
- `Sometimes(observed_pbs_thread_after_reconnect)` so the check is known to have run after at least one reconnect.
- SUT-side (missing): after `mysql_real_connect` succeeds, `ALWAYS(mysql_get_ssl_cipher(conn) != nullptr)` when the configured mode is `required` or above.

## Antithesis angle

Partitions and source restarts drive many reconnects. Polling races the short-lived setup connection, so `Sometimes` coverage matters.

## Instrumentation

- missing: SUT-side ALWAYS above. No existing check.

## Investigation outcome (2026-10-13)

- **performance_schema resolved:** the harness source `my.cnf` sets `performance_schema=ON` (`deployment-topology.md:67`). `performance_schema.threads` has `PROCESSLIST_USER` and `CONNECTION_TYPE` (values `SSL/TLS`, `TCP/IP`, `Socket`), so the oracle works as written. The MTR test `ssl_connection` already uses `performance_schema.session_status` `Ssl_version` for the mysql client (`mtr/binlog_streaming/t/ssl_connection.test:56`).
- **Correction to the oracle:** there is **no separate setup-query connection**. `collector_context.cpp:211` is the only `create_connection` call in `src/`. `SELECT @@GLOBAL.gtid_purged` (`:229-233`) runs on the same connection that is then switched to replication (`:249-260`). Each attempt therefore has exactly one PBS thread on the source. It shows the query first and then `Binlog Dump [GTID]`. The polling race is between the probe and that one thread's short pre-dump phase plus reconnect gaps, not a second connection.
- Verified by reading: the ssl-mode enum maps one-to-one onto `SSL_MODE_*` (`src/easymysql/connection.cpp:49-67`, `set_generic_mysql_option_helper` `:81-86`). `mode` is mandatory once an `ssl` section exists (`ssl_config.hpp:33`). Options are applied to each fresh `MYSQL*` in the constructor before `mysql_real_connect` (`connection.cpp:313-360`). PBS does not log the negotiated TLS state (`logger_helpers.cpp:253-265`), so only the source-side probe or a SUT-side assertion can observe it.
- Property unchanged.

## Open questions

- Does the team want `preferred` + cleartext fallback flagged (as a `Sometimes` warning signal) or ignored? It is documented libmysqlclient behaviour, but a backup tool silently downgrading may be considered a product issue. `(needs human input)`

### Investigation Log

#### Is `performance_schema` enabled on the harness source?

- Examined: `deployment-topology.md:66-70`, `mtr/binlog_streaming/t/ssl_connection.test`.
- Found: `performance_schema=ON` is part of the planned `my.cnf`. It is also the MySQL default.
- Conclusion: resolved. The oracle also needed a correction: one connection per attempt, no separate setup connection (`collector_context.cpp:207-270`).

#### Should `preferred` + cleartext fallback be flagged or ignored?

- Examined: README:567-584 (documents `ssl.mode` as mirroring `--ssl-mode`, no warning about fallback), `logger_helpers.cpp:253-265` (no TLS state logged), `.tickets-snapshot.txt` (no ticket about source-side TLS downgrade).
- Found: PBS adds no policy beyond libmysqlclient's. Without an `ssl` section the default is `PREFERRED`, and nothing in PBS logs whether TLS was negotiated.
- Not found: product intent.
- Conclusion: `(needs human input)`.
