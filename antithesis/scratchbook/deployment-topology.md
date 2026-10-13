---
sut_path: /Users/hrvoje/work/percona-binlog-server
commit: 99de68207c84f83e0d477ebe63d2f531facbb954
updated: 2026-10-13
external_references:
  - path: https://claude.ai/artifact/9om6fiPd64QzVXbEWEtDT1
    why: QA bug-ticket list for binlog_server (local snapshot antithesis/scratchbook/.tickets-snapshot.txt); open tickets used as leads, each validated against the code before use
---

# Deployment Topology: Percona Binlog Server on Antithesis

This version is regenerated from the final catalog (`property-catalog.md`, 67 properties) and applies the topology changes T1-T25 from `evaluation/synthesis.md`. Every backticked property slug in this file is a heading in the catalog. The section "Traceability: T1-T25" at the end shows where each change landed.

## Summary

The minimal topology keeps **four containers**, each with one replica:

| # | Container | Role | Image source | Runs |
|---|---|---|---|---|
| 1 | `mysql-source` | dependency | **new thin Dockerfile** `antithesis/docker/mysql/Dockerfile`, `FROM mysql:${MYSQL_VERSION}` | `mysqld` under a restart-loop wrapper (GTID, binlog, TLS), with datadir snapshot/restore hooks for the rewind action |
| 2 | `s3` | dependency + harness control plane | **new Dockerfile** `antithesis/docker/s3/Dockerfile` (Ubuntu 24.04 + Versity S3 Gateway 1.8.0 + a harness copy of `extra/qa/s3_fault_proxy.py` extended with a `/__harness__` control path) | `versitygw posix` on :9000; fault proxy and harness control on :9100 |
| 3 | `pbs` | service (SUT) | **new multi-stage Dockerfile** `antithesis/docker/pbs/Dockerfile`, target `pbs` | PID-1 supervisor that keeps `binlog_server pull` alive when enabled, plus a read-only storage helper on :8081. Test commands launch `fetch`, `purge_binlogs`, `list`, `search_*`, a scratch `mysqld` for replay, and extra writers as subprocesses. |
| 4 | `workload` | client | same Dockerfile, target `workload` | emits `setup_complete`, then sleeps. Holds the source load and admin drivers, pull readers, chained fetch, listener abuse and the quiet-window liveness driver. |

**No fifth container is added.** Every catalog change fits inside these four: the storage helper (T2) and the scratch replay `mysqld` (G2) run in `pbs`; the quiet flag and timeline flags (T3, T11) live in the `s3` proxy's control path; the stalled client (R20) is a harness reader in `workload`; the sibling-storage writer (T19) is a second process in `pbs`. Extra containers appear only in the Deferred templates.

The container boundaries follow fault boundaries. Antithesis faults whole containers, so the four containers give three SUT links that can be faulted independently:

- **source ↔ PBS** (ingress, `collector_context`);
- **PBS ↔ S3** (storage backend);
- **PBS ↔ readers** (the pull replication source, `minimysql`).

Node hang and throttling of `pbs` hit only the SUT, never the source, the object store or the oracles in `workload`. **No property needs node termination** (see Fault requirements). Process crashes come from harness SIGKILL drivers.

```text
                     +---------------------------------------------+
                     | workload (client)                           |
                     |  python3 + antithesis SDK + mysqlbinlog     |
                     |  + binlog_server copy (chained fetch,       |
                     |  S3 zombie writer); emits setup_complete    |
                     |  /data: chain storage, reader captures      |
                     +--+--------------+----------------+---------+
       SQL (TLS) :3306  |              | dump :3307      | HTTP :8081 (read-only storage helper)
       load + admin     |              v                 v
+-----------------------+---+  dump  +------------------------------------------+
| mysql-source (dependency) |<-------| pbs (service / SUT)                      |
|  mysqld 8.0 | 8.4 | 9.7   | :3306  |  PID1 helper_supervisor.sh               |
|  server_id=1, GTID on     |  TLS   |   -> binlog_server pull (when enabled)   |
|  restart-loop wrapper     |------->|   -> helper_storage_server.py :8081 (RO) |
|  snapshot/restore hooks   |SQL +   |  drivers spawn fetch / purge / list /    |
|  (rewind action)          |oracle  |   search / scratch mysqld (replay)       |
+-------------+-------------+mysqlbinlog /data: storages, run/, logs/, oracle/  |
              |                      +--------------------+---------------------+
              | HTTP :9100 /__harness__ (quiet + flags)   | S3 HTTP :9100 (fault proxy), :9000 direct
              v                                           v
            +-----------------------------------------------------------+
            | s3 (dependency + harness control plane)                   |
            |  fault_proxy.py :9100 -> :9000  (/__faults__, /__harness__)|
            |  versitygw posix /s3data                                  |
            +-----------------------------------------------------------+
```

`workload` also reaches `s3` directly (S3 oracles through boto3 on :9000, fault-proxy and harness control on :9100). Every container reads `/__harness__` on `s3:9100`.

## Components

### 1. `mysql-source` (dependency)

- **Image:** a thin Dockerfile, `FROM mysql:${MYSQL_VERSION}` (official Oracle image; Percona Server is the alternative, see Open Questions). It adds:
  - `my.cnf`: `server_id=1`, `log_bin`, `gtid_mode=ON`, `enforce_gtid_consistency=ON`, `binlog_format=ROW`, `sync_binlog=1`, `innodb_flush_log_at_trx_commit=1`, `binlog_expire_logs_seconds=0` (no automatic purge; source purges are deliberate driver actions), `performance_schema=ON` (needed for `performance_schema.threads` and `performance_schema.error_log`), `max_allowed_packet=1G` (large transactions and events). `max_binlog_size` is **not** fixed here: `first_setup_timeline.py` sets it per timeline with `SET GLOBAL` (see "Timeline configuration").
  - init SQL under `/docker-entrypoint-initdb.d/`: the replication user (`caching_sha2_password`, `REPLICATION SLAVE, REPLICATION CLIENT`), a workload user with `SYSTEM_VARIABLES_ADMIN`, `CONNECTION_ADMIN`, `RELOAD`, `BINLOG_ADMIN`, `SHUTDOWN`, `XA_RECOVER_ADMIN`, and on ≥ 8.3 `TRANSACTION_GTID_TAG` (tagged GTIDs, `tagged-gtid-resume-exact`). A marker table that `RESET` does not remove identifies the source epoch.
  - **`helper_mysqld_wrapper.sh` (entrypoint).** A restart loop around `mysqld`. After every exit it restarts `mysqld`, so a SIGKILL or SQL `SHUTDOWN` acts as a source crash or graceful restart without node termination. Between exit and restart it applies **marker files** written by the rewind driver: `snapshot` copies the stopped datadir to `/var/lib/mysql-snapshots/<n>`, and `restore` replaces the datadir with a snapshot (keeping `auto.cnf`, so `server_uuid` is unchanged). This is the source rewind of `source-rewind-never-silently-continued`.
- **TLS on.** The official image auto-generates certificates; the PBS config uses `ssl.mode: required`. That avoids ticket 23 (no `GET_SERVER_PUBLIC_KEY`) on first connect, and `source-session-tls-when-configured` checks it.
- **Test commands here** (each runs in the container that defines it; the image needs only bash, the `mysql` client and `curl` for `/__harness__`):
  - `parallel_driver_kill_mysqld.sh`: SIGKILL of `mysqld` or SQL `SHUTDOWN` (choice per the `source_stop_mode` knob), optionally aimed just after a rotation (the lead (b) knob). Acts only when the timeline flag `source_kills_possible` is acknowledged by `pbs` and the quiet flag is off.
  - `parallel_driver_rewind_source.sh` (flagged timelines): requests a snapshot on a `SHUTDOWN` cycle, lets the source run, then requests a restore on a later cycle.
  - `parallel_driver_reset_source.sh` (flagged timelines): `RESET MASTER` on 8.0, `RESET BINARY LOGS AND GTIDS` on ≥ 8.2 (branch as in `fetch_crash_test.py:205-208`).
- **Network:** accepts :3306 from `pbs` (dump, oracle SQL, oracle `mysqlbinlog`) and `workload` (load, admin, health checks).
- **Volumes:** datadir on the container filesystem, snapshots under `/var/lib/mysql-snapshots`. A process kill keeps the binlogs. Node termination is not used.
- **Version:** one `MYSQL_VERSION` per launch: 8.0.45, 8.4.8 or 9.7.2 (the CI and QA pins). Tagged-GTID actions are enabled only on 8.4 and 9.7.

### 2. `s3` (dependency + harness control plane)

- **Image:** Ubuntu 24.04, the pinned `versitygw_v1.8.0_Linux_x86_64.tar.gz` (same artifact as `extra/qa/setup_test_environment.sh:117`), `python3`, and `antithesis/docker/s3/fault_proxy.py`. That file is a harness copy of `extra/qa/s3_fault_proxy.py` with two additions (the QA original stays unchanged):
  - **`/__harness__` control path** (T3, T11). `PUT /__harness__/quiet {"seconds": N}` sets a quiet window that expires on the proxy's monotonic clock (immune to clock jitter); `GET /__harness__` returns `{quiet, quiet_remaining, flags, acks}`. `flags` holds the timeline configuration published by `first_setup_timeline.py`; `acks` records that `pbs` has written its local timeline-tag file (see "Timeline configuration").
  - **Quiet-aware rules.** While quiet, random fault rules set by `parallel_driver_s3_faults.py` are suspended. Rules marked `"stimulus": true` keep applying, so a liveness check can install a harness-bounded stall of known duration (R1).
- **Entrypoint:** `mkdir -p /s3data/pbs-antithesis`, start `versitygw --port :9000 posix /s3data`, then `fault_proxy.py --listen :9100 --backend 127.0.0.1:9000`.
- **Why Versity, not MinIO:** QA uses Versity, so behaviour matches `s3_fault_sweep.py` and `s3_upload_volume.py`. Its posix backend gives the strong overwrite-then-read behaviour PBS assumes (A10 is an explicit exclusion in `s3-whole-object-put-publishes-intended-bytes`). MinIO is the fallback.
- **Why the proxy lives here:** PBS talks to `s3:9100`, so native network faults on `pbs ↔ s3` still surface to PBS as real timeouts and resets, and the proxy adds the HTTP-level faults Antithesis cannot inject (5xx, `reset_after`, stall, `truncate_body`). The proxy buffers the whole request before forwarding, so it cannot publish a partial body; mid-body aborts come only from native faults (`s3-whole-object-put-publishes-intended-bytes`).
- **Why the control plane lives here:** it is the one service every container already reaches, it is never the SUT, and it holds no SUT state. If `/__harness__` is unreachable, a fault driver **skips its action** for that iteration (fail-safe: no harness fault without a confirmed non-quiet, acknowledged state).
- **Network:** :9100 and :9000 from `pbs` and `workload`; :9100 from `mysql-source`.
- **Volumes:** `/s3data`.

### 3. `pbs` (service, the SUT)

- **Image:** a new multi-stage Dockerfile (the repo has none; `packaging/` is not reusable for an instrumented build).
  1. `deps`: `ubuntu:24.04` with `clang-20 lld-20 libc++-20-dev libc++abi-20-dev cmake ninja-build git libmysqlclient-dev libcurl4-openssl-dev libssl-dev zlib1g-dev libaio-dev` (from `.github/workflows/cmake.yml:142-167`); Boost 1.90.0 and AWS SDK C++ 1.11.774 built with `extra/cmake_presets/{boost,aws-sdk-cpp}/CMakePresets.json` for **`debug_clang20`**, against the same libc++.
  2. `build`: `cmake --preset debug_clang20` with `-fsanitize-coverage=trace-pc-guard,pc-table -g` and `-Wl,--build-id`, one TU including `antithesis_instrumentation.h` (a `WITH_ANTITHESIS` CMake option, or the legacy `libvoidstar.so` link with no source change), and later the header-only `antithesis-sdk-cpp` for SUT assertions (B5). Also builds `extra/qa/fsfault/fsfault.c` into `libfsfault.so`.
  3. `runtime-base`: `ubuntu:24.04` with runtime libraries (`libmysqlclient21`, libc++, libcurl, openssl, `libaio1t64`, `libnuma1`), `python3`, `pip install antithesis pymysql boto3 cryptography`, and from the MySQL generic tarball for the same `MYSQL_VERSION` (`linux-glibc2.28`, as in `setup_test_environment.sh`): `bin/mysqlbinlog`, plus `bin/mysqld` and `bin/mysql` for the replay-restore oracle (G2). It adds `binlog_server`, `libfsfault.so` and `helper_lib/` (below).
  4. Target `pbs`: `runtime-base` plus `/symbols/binlog_server` (unstripped), the `pbs`-side templates, the supervisor and the storage helper.
- **Build preset:** `debug_clang20`. Release removes 163 asserts (sut-analysis W13, A11); `*_gcc14` cannot take Antithesis coverage instrumentation (Clang ≥ 13). `asan_clang20` is a second launch variant (`ASAN_OPTIONS=abort_on_error=1`); whether ASan composes with `trace-pc-guard` under Antithesis is an open check. A release build appears only as the optional ticket-7 hang launch (`hostile_dump`).
- **Environment:** `AWS_EC2_METADATA_DISABLED=true` (ticket 16); `TMPDIR` / `fs_buffer_directory` = `/data/s3buf` (N10 leaks land on the measured volume); `PBS_ANTITHESIS_TIMELINE_TAG_FILE=/data/antithesis/timeline_tag` (read by the SUT assertion wrapper, T11); `TZ=UTC`.
- **Logging (T7):** every PBS config the harness writes leaves `logger.file` empty, so `logger_factory.cpp:28-29` selects `cout_logger`. Launchers pipe stdout through `tee -a /data/logs/<role>-<pid>.log`, so Antithesis captures every incarnation and the log survives a restart loop (`file_logger` truncates on open). The wedge classifier, the N7 attribution and the log-line fallbacks read `/data/logs/`.
- **PID 1: `helper_supervisor.sh`.**
  - While `/data/pull/pull.enabled` exists, it runs `binlog_server pull /data/pull/config.json`, restarting it `idle` seconds after any exit (`systemd Restart=always` style). `binlog_server` is its child, never PID 1.
  - Per restart it may set `LD_PRELOAD=libfsfault.so` with a rule drawn from the timeline's fsfault set (T16), and in the fd-limit variant it runs pull under `ulimit -n 256` (T21).
  - Before each restart it snapshots the bytes beyond each tail `.json` size into `/data/oracle/ctr/` (T23, `ctr-keystream-not-reused-across-truncation`).
  - It appends `{start_ts, exit_code, signal, last_error_line, fsfault_rule, proxy_rules_snapshot, quiet, kill_log_ref}` to `/data/pull/outcomes.jsonl` (R21).
  - **Quiet watcher (T3).** It polls `/__harness__` once a second. When a quiet window starts it SIGCONTs every stopped `binlog_server`, kills the process groups of orphaned writers (pidfile owner dead) except its own pull, removes stale writer locks, and posts `acks.quiet_fenced` so liveness checks know the harness is quiet.
  - It mirrors the timeline flags into `/data/antithesis/timeline_tag` (T11).
- **`helper_storage_server.py` (T2).** A read-only HTTP server on :8081 over `/data`: `GET /files/<path>[?range=]`, `GET /ls/<dir>`, `GET /snapshot/<storage>` (a tar of index, metadata and data files copied in index order, without running any PBS constructor), `GET /logs/...`, `GET /outcomes`, `GET /sessions` (`ss -tnp` for :3307, used by `stale-sessions-reclaimed-within-bound`). It never writes. `workload` oracles that need stored bytes, `list`-equivalent state labels or the outcome log use it with retries. `pbs ↔ workload` faults hit it too, so oracles skip (never fail) when it is unreachable.
- **Process hygiene (T15, T3).** Every launcher runs `binlog_server` in its own process group and writes `/data/run/<role>/<pid>.pid` with role ∈ {`writer`, `pull`, `query`, `purge`, `readback`, `replay`}. `parallel_driver_kill_binlog_server.sh` targets only `writer` and `pull` pids and logs every signal to `/data/run/kills.jsonl`.
- **Storage location (W8, W9):** every storage is a subdirectory of `/data`, never the mount root (`filesystem_storage_backend.cpp:108-112` throws on `lost+found`): `file:///data/<template>/storage[-<n>]`. S3 storages use sibling prefixes `s3://…@s3:9100/pbs-antithesis/<template>/<timeline-id>/<n>/`, never nested. A wedge reset (T12) moves to the next `<n>`.
- **Volumes:** a named volume at `/data`, not shared with other containers.
- **Network:** out to `mysql-source:3306` (dump, oracle SQL and `mysqlbinlog`); out to `s3:9100` (or `:9000` for the no-proxy arm); in on `:3307` (pull listener) and `:8081` (storage helper) from `workload`.
- **Resource variants:** fd limit through the supervisor (T21, needs no compose support); memory limit through a variant compose file with `mem_limit` on `pbs` (if honoured), otherwise VmRSS sampling (R27).

### 4. `workload` (client)

- **Image:** the same Dockerfile, target `workload` (`runtime-base` plus the client-side templates). It carries `mysqlbinlog` (shipped version only), the Python SDK, `helper_lib/`, and its own `binlog_server` for chained fetch and the S3 zombie writer.
- **Entrypoint:** `helper_wait_ready.py`: waits for a TLS login to `mysql-source` with `@@gtid_mode = ON`, for `s3:9000` `HeadBucket`, for `s3:9100/__harness__`, and for `pbs:8081/ls/`; then emits `setup_complete` (`antithesis.lifecycle.setup_complete`) and runs `sleep infinity`. Readiness never depends on a `first_` command.
- **Concurrency:** several pull readers (`mysqlbinlog --read-from-remote-server --raw` and a raw Python COM_BINLOG_DUMP client built on `extra/qa/listener_fuzz_test.py:90-214`, sending the exact checksum `SET` strings PBS recognises) run as concurrent `parallel_driver_` instances: N replicas without N containers.
- **Volumes:** `/data` for the chained-fetch storage, reader captures and the optional ledger cross-check.
- **Network:** to `mysql-source:3306`, `pbs:3307`, `pbs:8081`, `s3:9000`, `s3:9100`.

### Why multi-process storage races run inside `pbs`

The races of `second-writer-on-storage-fails-closed`, `other-process-startup-spares-live-writer`, `stale-index-commit-never-wedges-storage` and `distinct-storages-shared-server-id-stay-consistent` are races on one local filesystem (`.tmp` names, rename vs stat, truncation, appends). They run as subprocesses of `pbs`-side commands on one directory: no shared volume is needed, the instrumented binary covers every process, and thread pausing and CPU modulation choose the interleaving. The only cross-container writer is `workload/parallel_driver_s3_second_writer.py`, which shares an S3 prefix with a `pbs` writer and can be partitioned from it (the zombie arm of `second-writer-on-storage-fails-closed`).

## Shared harness library (`helper_lib/`, in every template directory)

| Module | Purpose | Change |
|---|---|---|
| `harness.py` | `/__harness__` client: read flags and acks, request quiet (`ANTITHESIS_STOP_FAULTS` plus `PUT quiet`), wait for `acks.quiet_fenced`, `may_act()` = reachable ∧ acknowledged ∧ not quiet | T3, T11 |
| `ensure_state.py` | idempotent per-container initialisation from the published timeline config; in `pbs` writes `/data/<template>/config.json`, the timeline-tag file and the ack | T4, T11 |
| `server_ids.py` | the `server_id` table below | T5, T6 |
| `oracles.py` | provider-based oracles (source bytes via `mysqlbinlog --read-from-remote-server --raw` without `--stop-never`; storage bytes via local path, storage helper or boto3); index-suffix-aware comparison; incremental verified-prefix cache in `/data/oracle/prefix-cache.json`; vendored `parse_events`, `transaction_boundaries` and constants from `extra/qa/fetch_crash_test.py` (no Hypothesis dependency) | T8, T22 |
| `decrypt.py` | independent decryptor: reads the keyring file and each `<binlog>.json` `file_key_envelope` (`kek_id`, `data_hex`, `iv_hex`), decrypts with the configured AES-CTR cipher via `cryptography`. The canonical encrypted reader; pull is never used to read a storage it does not own | R5 |
| `launch.py` | runs `binlog_server` in its own process group with a role pidfile, cout logging via `tee`, optional `LD_PRELOAD=libfsfault.so` + `FSFAULT_RULES` | T7, T15, T16 |
| `readback.py` | pull readback of a non-pull storage on a **snapshot copy** (`cp -a` to `/data/scratch/<n>`), with `connection.host` set to an unreachable endpoint (`127.0.0.1:1`), only at quiescent points (`finally_`) | T14 |
| `wedge.py` | wedge detect-and-reset (below) | T12 |
| `knobs.py` | per-defect known-trigger knobs (below) | T13 |
| `source_state.py` | polls `performance_schema.threads` / `SHOW PROCESSLIST` for the PBS user's `Binlog Dump` thread (connected, caught up, mid-transaction) | T20 |

### `server_id` allocation (T5, T6)

| Role | `server_id` | Notes |
|---|---|---|
| `mysql-source` | 1 | |
| `pull` (supervisor) | 101 | |
| `fetch` writer (`fetch_fs`, `s3`, reconfigure runs, purge-past driver) | 102 | one writer at a time under the writer lock |
| `contention` overlapping writers | 111-118 by instance slot | same-id arm (low probability): all use 102, for the eviction path (R17) |
| `contention` sibling-storage writer | 102 (shared on purpose) | different storage directory; `distinct-storages-shared-server-id-stay-consistent` |
| `workload` S3 zombie writer | 121 | same-id arm: 102 |
| `workload` chained fetch (dumps from `pbs:3307`, not the source) | 201 | |
| snapshot-copy pull readback | 131 | source unreachable by construction |
| pull readers on `pbs:3307` (`--stop-never`) | `--connection-server-id` 1000 + slot (`mysqlbinlog`), 2000 + slot (Python) | unique per invocation |
| oracle `mysqlbinlog` against the source | 0 (no `--stop-never`) | never evicts (`rpl_source.cc:1148`); a blocking oracle would take 3000 + slot |
| scratch replay `mysqld` | 900 | never connects to the source |

`workload/anytime_source_health.py` asserts `Always("no unexpected dump-thread eviction")` from `performance_schema.error_log` (zombie dump thread kills of ids that are not deliberately shared) and `Sometimes("deliberate shared-id eviction observed")` in the shared-id timelines.

### Wedge detect-and-reset (T12) and known-trigger knobs (T13)

**Waits for B3** (human decision on known-defect containment). The mechanism is planned either way; B3 decides the knob defaults.

- **Knobs** (`knobs.py`, drawn once per timeline in `first_setup_timeline.py`; each is `avoid` or `trigger`):

  | Knob | `trigger` means | Defect | Properties it feeds |
  |---|---|---|---|
  | `kill_during_rotation` | the kill driver aims SIGKILL at rotation (watching new data objects of 4 bytes) | ticket 15 | `kill-anywhere-storage-reopens`, `fetch-converges-after-faults-stop` |
  | `json_put_failures` | ≥ 6 consecutive `.json` PUT failures in position mode | ticket 18 (CR_1236 wedge) | `failed-flush-never-reappends-data`, `fetch-converges-after-faults-stop` |
  | `post_rotation_source_kill` | `kill_mysqld` fires right after a source rotation | lead (b) | `source-crash-restart-resume-is-faithful`, `binlog-metadata-matches-stored-bytes` |
  | `source_stop_mode` | `SHUTDOWN` / `KILL QUERY` allowed (else SIGKILL only) | lead (a) | `inprocess-reconnect-resumes-without-dup-or-gap`, `resume-continuity-guard-never-trips`, `pull-chain-converges-after-heal` |
  | `purge_warning` | fsfault on `remove` during purge | N3 | `interrupted-purge-leaves-openable-storage` |
  | `ahead_of_flushed_requests` | readers request positions past the flushed size | ticket 26 / PBS-96, N12 | `blocking-dump-ahead-of-flushed-position-eventually-served`, `dump-session-ends-with-eof-or-err-packet` |
  | `large_event` | events ≥ 16,777,211 bytes | ticket 10 | `large-event-served-or-rejected-cleanly` |
  | `misaligned_dump` | direct misaligned requests (only in `hostile_dump`) | ticket 7 | `misaligned-dump-position-rejected-cleanly` |
  | `rewrite_toggle` | the reconfigure toggle arm changes rewrite / mode / `base_file_name` | rewrite-toggle wedge | `rewrite-toggle-between-runs-keeps-storage-resumable` |
  | `checksum_none_in_rewrite` | `binlog_checksum=NONE` while rewrite is on | checksum-none crash loop | `rewrite-checksum-none-fail-stop` |
  | `query_adds_encryption` | queries run with an encryption-added config on an unencrypted storage | lead (e) | `query-modes-never-write-storage`, `other-process-startup-spares-live-writer` |

  Proposed default until B3 is decided: in `fetch_fs`, `pull_fs` and `s3` each knob is `trigger` in about 15 % of timelines, so every expected-to-fail property still reports, and `known_defects` sets them all to `trigger`.
- **`wedge.py`** is called by every writer launcher after a non-zero exit and by every terminal command before it checks. It recognises each known signature (ticket 15: an unindexed 4-byte data object; lead (b): abort at `storage.cpp:138` in the log; N3: purge leftovers after a `warning`; ticket 18: `CR_1236` after failed `.json` PUTs; rewrite toggle; checksum-none crash loop; ticket 7 abort; ticket 10 reconnect loop). For each one it asserts once under its own ticket-named message, moves the storage to `/data/<template>/wedged/<n>` (or a fresh S3 prefix), re-seeds a fresh storage, flags the timeline `storage_reset` for the oracles (which restart their baselines), and records `Sometimes("storage reset after known wedge")`. Unknown failures are never reset.

## Timeline configuration (T4, T8, T9, T11, T24)

`workload/first_setup_timeline.py` (the single `first_` of every template) draws, from Antithesis randomness, and publishes to `/__harness__`:

- **SUT config:** mode (gtid or position); `checkpoint_size` / `checkpoint_interval` (including off; weighted toward on in `pull_fs` for R12); encryption (off, or KEK/cipher); rewrite (on/off) with `file_size` ≥ 16 KiB so the 999,999 sequence limit cannot be reached (T24); `idle_time`, `read_timeout`, `connect_timeout`; and whether the timeline has a reconfigure arm (T4).
- **Source sizing (T8):** `max_binlog_size` from a bimodal draw: small (4-64 KiB) for rotation-heavy timelines, or large (32-128 MiB) for the multipart (> 8 MiB), ≥ 64 MiB upload and memory checks. The large draw also enables 64-100 MiB transactions in the load generator.
- **Event-kind weights (T9)** for the load generator.
- **Knobs** (T13) and **flags** (T11): `template`, `source_kills_possible`, `source_reset`, `source_rewind`, `source_purge_past`, `storage_faults_active` (fsfault or proxy rules possible), `misuse` (`contention`), `shared_server_id`, `fd_limit`, `mem_limit`.
- **Resource variants:** fd limit (`pull_fs`, ~10 %), memory limit (only in the `mem_limit` compose variant).

It then creates the schema, sets `max_binlog_size`, records `server_uuid`, and seeds a few transactions. `pbs` `ensure_state.py` reads the config, writes `/data/<template>/config.json` and `/data/antithesis/timeline_tag`, and posts `acks.pbs`. Drivers in other containers act only after that ack, so the tag file is always in place **before** a source kill, RESET, rewind or storage fault can happen. Flags are declared per timeline up front, never per event, which keeps the SUT's read of the tag file ordered with respect to the faults it qualifies.

**How SUT assertions use it (R6).** The SUT assertion wrapper (B5) reads the tag file once at process start. Sites reachable by misuse, source SIGKILL/RESET/rewind or storage faults either put the template and flags into the message (for example `"[contention] storage validator: unindexed data object"`), use `ALWAYS(cond || flag)`, or use one message per leftover shape. Until SUT macros exist, the same split is applied to the log-line fallbacks.

**Config mutation (T4).** `pbs/parallel_driver_reconfigure.py` runs only in timelines with the reconfigure arm. Under the writer lock (`fetch_fs`) or with pull stopped (`pull_fs`: remove `pull.enabled`, SIGTERM, wait, change, re-enable) it changes one dimension: encryption on/off, KEK and/or cipher (all KEKs stay in the keyring, catalog OQ 12), source `binlog_checksum` (`SET GLOBAL` over SQL: CRC32 ↔ NONE), rewrite on/off and `base_file_name`/`file_size` and mode (only with the `rewrite_toggle` knob), or a query config that adds encryption (`query_adds_encryption`). Every change is appended to `/data/<template>/config_history.jsonl` with the binlog name in force, so oracles key on "config when binlog X was created". After the next writer start it records `Sometimes("writer restarted after <dimension> changed")` (R16).

**Event variety (T9).** `workload/parallel_driver_source_load.py` draws the event kind per transaction: plain row DML; `binlog_transaction_compression=ON` (per session); XA PREPARE / XA COMMIT (sometimes leaving an XA prepared across a source kill); `binlog_row_value_options=PARTIAL_JSON` with `JSON_SET` updates; `binlog_row_image=MINIMAL`; DDL; tagged GTIDs (`SET GTID_NEXT='AUTOMATIC:<tag>'`, 8.4/9.7 only); `SET TIMESTAMP` skew; large transactions (large-binlog timelines); and events ≥ 16,777,211 bytes only under the `large_event` knob (otherwise capped below). Each kind has a `Sometimes("source committed a <kind> transaction")`. It also issues `FLUSH BINARY LOGS` and idle gaps, and keeps an optional ledger in `workload:/data/ledger.jsonl` that is only a cross-check (R25).

**Source admin actions (T1, T20).** `workload/parallel_driver_source_admin.py`, acting only when `may_act()`:

- `KILL QUERY` / `KILL CONNECTION` of PBS's dump thread, preferably while it is mid-transaction (found through `source_state.py`);
- graceful `SHUTDOWN` while a large transaction streams (the wrapper restarts mysqld; needs `source_stop_mode=trigger`);
- `FLUSH BINARY LOGS` 0-3 times while PBS is disconnected and caught up;
- `FLUSH PRIVILEGES` and `ALTER USER … IDENTIFIED BY` (same password) to empty the auth cache (ticket 23);
- `SET GLOBAL binlog_checksum` toggles in `pull_fs` timelines without a reconfigure arm (the `sender-file-switch-emits-artificial-rotate` format variant).

The process- and datadir-level source actions (SIGKILL, rewind, RESET) live in `mysql-source` (above), and the source purge past PBS's resume point lives in `pbs/serial_driver_source_purge_past_pbs.py`, because it has to stop the PBS writer first.

## Quiet windows and orphan cleanup (T3, R1)

`ANTITHESIS_STOP_FAULTS` stops only Antithesis faults. Every time-bounded `Always` is therefore measured like this:

1. The checking command calls `${ANTITHESIS_STOP_FAULTS} N` and `PUT /__harness__/quiet {"seconds": N}`.
2. Every harness fault driver (`parallel_driver_kill_binlog_server.sh`, `parallel_driver_kill_mysqld.sh`, `parallel_driver_rewind_source.sh`, `parallel_driver_reset_source.sh`, `parallel_driver_s3_faults.py`, `parallel_driver_source_admin.py`, fsfault draws in launchers) checks `may_act()` before each action and does nothing while quiet. The proxy suspends non-stimulus rules.
3. The `pbs` supervisor fences: SIGCONT of stopped processes, kill of orphaned writer process groups, release of stale locks, then `acks.quiet_fenced`.
4. The measured action starts only after the ack. Any latency in the bound is a harness-bounded stimulus (`"stimulus": true` proxy stall of known seconds, SIGSTOP of known duration, a reader that stops calling `recv()`). Bounds are calibrated against the coverage-instrumented debug build.
5. Native latency, hang and throttling feed only exploration `Sometimes` measurements.

`eventually_` and `finally_` commands start with the same steps 1-3 (they kill other commands, but not necessarily the processes those commands spawned), then check.

## Test templates

Each timeline uses exactly one template. A template directory of the same name exists in several containers, and each command runs in its defining container. There is one `first_` per template, `workload/first_setup_timeline.py`. `pbs` and `mysql-source` commands initialise lazily through `ensure_state.py`. Terminal commands (`eventually_` / `finally_`) are alternatives: each timeline branch ends in one of them, and Antithesis branching gives each its share of branches.

Container prefixes in the tables: **m** = `mysql-source`, **p** = `pbs`, **w** = `workload`.

| Template | In minimal topology | Purpose |
|---|---|---|
| `fetch_fs` | yes | repeated `fetch` on a filesystem storage, purge, queries, reconfigure, rewrite oracles, flagged source-history actions |
| `pull_fs` | yes | long-lived supervised `pull` with downstream readers, chained fetch, retention cycle, quiet-window liveness |
| `s3` | yes | `fetch` or `pull` (or both phases, T17) on S3 with proxy faults and packet-hold partitions on `pbs ↔ s3` |
| `contention` | yes (separate launch recommended, B2) | out-of-contract multi-process races, asserted as fail-closed and detectable |
| `hostile_dump` | yes (T18) | direct misaligned / hostile dump requests, kept away from reader-convergence timelines; optional release-build launch for ticket 7's hang |
| `known_defects` | yes, **waits for B3** (T13) | every known-trigger knob set to `trigger`, with the wedge helper; a known-bug confirmation run separate from regression hunting |

### Template `fetch_fs`

| C | Command | Does / checks |
|---|---|---|
| w | `first_setup_timeline.py` | timeline config, schema, `max_binlog_size`, seed |
| w | `parallel_driver_source_load.py` | event-variety load (T9) |
| w | `parallel_driver_source_admin.py` | SQL source actions (T1, T20) |
| w | `anytime_source_health.py` | TLS of PBS sessions, auth after flush, eviction pair (T5) |
| p | `parallel_driver_fetch.py` | writer lock; `fetch` (id 102) with optional fsfault; random SIGTERM/SIGKILL/none; CTR snapshot before releasing the lock (T23); exit 0 ⇒ complete copy; any exit ⇒ next `list` succeeds; `wedge.py` on failure |
| p | `parallel_driver_reconfigure.py` | config mutation (T4), reconfigure-arm timelines only |
| p | `parallel_driver_query.py` | `list` / `search_by_gtid_set` / `search_by_timestamp` loops beside the writer; JSON envelope; no-write hashing on quiescent storage |
| p | `parallel_driver_kill_binlog_server.sh` | SIGKILL / SIGTERM / SIGSTOP+SIGCONT of writer pids only (T15) |
| p | `serial_driver_purge.py` | writer lock; purge with fsfault on `remove`/`rename` and optional self-SIGKILL |
| p | `serial_driver_damaged_copy_query.py` | copies the storage, empties `binlog.index` in the copy (ticket 13), runs `list`/`search_*`/`purge` on the copy |
| p | `serial_driver_source_purge_past_pbs.py` | flag `source_purge_past`: stop writer, `FLUSH` + `PURGE BINARY LOGS TO` past the resume point, k fetch attempts, checks, re-seed |
| m | `parallel_driver_kill_mysqld.sh` | source SIGKILL / `SHUTDOWN` |
| m | `parallel_driver_reset_source.sh` | flag `source_reset` |
| m | `parallel_driver_rewind_source.sh` | flag `source_rewind` |
| p | `anytime_storage_prefix.py` | incremental prefix, transaction-boundary, metadata, GTID bookkeeping, no-regression and decryptor checks |
| p | `anytime_rewrite_oracles.py` | rewrite timelines: chain validity, last-event, transaction sequence, PITR by datetime (`TZ=UTC`) |
| p | `anytime_source_history.py` | flagged timelines: no append after RESET / rewind, per-GTID payload hashes |
| p | `anytime_outcomes.py` | log-line fallbacks (SUT-only checks, recovery anchors) over `/data/logs` |
| p | `eventually_converge.py` | fence; `fetch` until exit 0 within N attempts; byte identity; exactly-once; one extra no-op `fetch` |
| p | `finally_storage_reopens.py` | every mode opens the storage; no `.tmp`; nothing outside the index; snapshot-copy pull readback (T14) |
| p | `finally_replay_restore.py` | scratch `mysqld` replay of decrypted stored binlogs; `CHECKSUM TABLE` vs source |

### Template `pull_fs`

| C | Command | Does / checks |
|---|---|---|
| w | `first_setup_timeline.py`, `parallel_driver_source_load.py`, `parallel_driver_source_admin.py`, `anytime_source_health.py` | as in `fetch_fs` |
| w | `parallel_driver_pull_reader.py` | random start file/offset (incl. empty storage, magic-only, purged, ahead-of-flushed per knob), blocking or not, `--raw` or Python client, unique `--connection-server-id`; labels each attempt from the storage helper; checks preamble, ROTATE+FDE at each switch, log_pos/CRC chain, EOF/ERR ending, client ⊑ stored ⊑ source |
| w | `parallel_driver_chained_fetch.py` | position-mode `fetch` from `pbs:3307` into `workload:/data/chain` (id 201) |
| w | `parallel_driver_listener_abuse.py` | idle and pre-auth connections, malformed frames (from `listener_fuzz_test.py`); connection flood in the fd-limit variant |
| w | `parallel_driver_quiet_window.py` | mid-run quiet window (T3): pull catch-up bound per window, a stalled reader that stops calling `recv()` beside healthy readers, silent/vanishing clients reclaimed (measured through `GET /sessions`) |
| p | `parallel_driver_ensure_pull.py` | writes the pull config (id 101), creates `pull.enabled`; in a self-requested quiet window SIGTERMs pull with a known stimulus and checks the exit bound |
| p | `parallel_driver_reconfigure.py` | pull variant: stop, change encryption/KEK/cipher or checksum, restart |
| p | `parallel_driver_checkpoint_rpo.py` | quiet window; marker transaction; stored by `t + checkpoint_interval + read_timeout + δ`; optional kill |
| p | `parallel_driver_query.py`, `parallel_driver_kill_binlog_server.sh` | as in `fetch_fs`, against the pull storage |
| p | `serial_driver_retention_cycle.py` | T10: capture served stream, remove `pull.enabled`, SIGTERM, purge (fsfault/kill options), re-enable |
| p | `serial_driver_source_purge_past_pbs.py` | flag `source_purge_past`, pull variant |
| p | `anytime_memory_sample.py` | VmRSS of pull and fetch vs the checkpoint bound; kill-log attribution of SIGKILLs |
| m | `parallel_driver_kill_mysqld.sh`, `parallel_driver_rewind_source.sh` | as in `fetch_fs` |
| p | `anytime_storage_prefix.py`, `anytime_rewrite_oracles.py`, `anytime_source_history.py`, `anytime_outcomes.py` | as in `fetch_fs`; `anytime_outcomes.py` also checks the supervisor outcome log (N7 with cause attribution) |
| p | `eventually_pull_converges.py` | fence; pull up; storage = source; full dump from `127.0.0.1:3307` with artificial events stripped = decrypted stored bytes |
| w | `eventually_readers_converge.py` | fence (via `/__harness__`); a blocking reader started now receives every source transaction; chained storage = upstream (via storage helper) |
| p | `finally_storage_reopens.py`, `finally_replay_restore.py` | after stopping pull gracefully |

### Template `s3`

Same commands as `fetch_fs` and `pull_fs`, with S3 URIs; `ensure_state.py` picks the fetch-like arm, the pull-like arm, or both phases in sequence (T17). Additions:

| C | Command | Does / checks |
|---|---|---|
| p | `parallel_driver_s3_faults.py` | random proxy rules: 5xx / `reset_after` / stall / `truncate_body` on `.json`, binlog or index PUTs and GETs; `.json` 5xx at low probability (R11); ≥ 6 consecutive binlog PUT failures; per-path GET rule on one `.json`; suspended while quiet |
| p | `anytime_s3_local_buffer.py` | `/data/s3buf` size bound across crashes (N10) |
| p | `anytime_s3_tail_recovery.py` | after a startup-recovery log line, HEAD the tail on `s3:9000`: size = metadata size |
| w | `anytime_s3_objects_consistent.py` | boto3 on `s3:9000`: every indexed object exists, metadata size ≤ object size, no orphaned multipart uploads, no regression of object prefixes |

Tenant fault config for this template should include packet-hold partitions on `pbs ↔ s3` (T17, R14).

### Template `contention`

Kept separate (and ideally a separate launch) so expected out-of-contract failures never hide in-contract regressions (B2). Assertions are "fails closed and detectable" until contract question 1 is answered.

| C | Command | Does / checks |
|---|---|---|
| w | `first_setup_timeline.py`, `parallel_driver_source_load.py` | as in `fetch_fs`; flag `misuse` |
| p | `parallel_driver_fetch.py` | **no** writer lock; distinct ids 111-118, same-id arm 102 |
| p | `parallel_driver_purge.py` | purge racing writers (N1, N2, ticket 9) |
| p | `parallel_driver_purge_pair.py` | two concurrent purges with no writer (T19) |
| p | `parallel_driver_sibling_writer.py` | second PBS writer on a sibling storage directory, shared `server_id` 102 (T19) |
| p | `parallel_driver_query.py` | queries beside writers, encryption-added config arm |
| w | `parallel_driver_s3_second_writer.py` | zombie writer from `workload` on the S3 prefix a `pbs` writer uses (id 121 / 102) |
| p | `parallel_driver_kill_binlog_server.sh`, `anytime_storage_prefix.py`, `eventually_converge.py`, `finally_storage_reopens.py` | as in `fetch_fs`, with misuse-tagged messages |

### Template `hostile_dump` (T18)

| C | Command | Does / checks |
|---|---|---|
| w | `first_setup_timeline.py`, `parallel_driver_source_load.py` | as in `pull_fs` |
| p | `parallel_driver_ensure_pull.py`, `parallel_driver_kill_binlog_server.sh` | as in `pull_fs` |
| w | `parallel_driver_hostile_dump.py` | misaligned positions, offsets inside events, huge/zero lengths; asserts pull neither dies nor hangs and a fresh client is served |
| w | `parallel_driver_listener_abuse.py` | as in `pull_fs` |
| p | `anytime_outcomes.py` | pull exit and abort signatures |

### Template `known_defects` (T13, waits for B3)

`fetch_fs` + `pull_fs` commands combined at reduced concurrency, every knob `trigger`, `wedge.py` active after every failure. Purpose: one confirmation per known signature per run, without spending regression-hunting budget. If B3 chooses "fix first" or "variant builds", this template is dropped.

## Property → template → command → container map (all 67)

Command names refer to the template tables above; **m**/**p**/**w** give the container. "+SUT" means a SUT-side assertion (B5; log-line fallback in `p/anytime_outcomes.py` where the catalog names one). "Knob" names the T13 knob that gates a known-defect trigger.

### Storage integrity

| Property | P | Templates | Commands (container) | Workload actions / knobs |
|---|---|---|---|---|
| `stored-binlog-byte-prefix-of-source` | P0 | `fetch_fs`, `pull_fs`, `s3` | p `anytime_storage_prefix.py`, p `eventually_converge.py` / `eventually_pull_converges.py`, w `anytime_s3_objects_consistent.py` (s3); +SUT append offset | load; writer kills; fsfault, proxy rules |
| `metadata-size-at-transaction-boundary` | P0 | `fetch_fs`, `pull_fs`, `s3` | p `anytime_storage_prefix.py` | event variety (XA, compression, DDL) |
| `failed-flush-never-reappends-data` | P0 | `s3`, `fetch_fs` | p `parallel_driver_s3_faults.py` (`.json` 5xx), p `parallel_driver_fetch.py` (fsfault `nth`/`count` on `.json.tmp`), p `anytime_storage_prefix.py`; +SUT | knob `json_put_failures` |
| `kill-anywhere-storage-reopens` | P0 | `fetch_fs`, `pull_fs`, `s3` | p `parallel_driver_kill_binlog_server.sh`, p `parallel_driver_fetch.py`, supervisor restarts, p `finally_storage_reopens.py`; +SUT per-shape validator messages | knob `kill_during_rotation` (ticket 15) |
| `graceful-exit-leaves-flushed-storage` | P0 | `fetch_fs`, `pull_fs`, `s3` | p `parallel_driver_fetch.py` (exit 0, SIGTERM), p `parallel_driver_ensure_pull.py` (SIGTERM), p `parallel_driver_s3_faults.py` (6+ PUT failures); +SUT | fsfault ENOSPC |
| `stored-state-never-regresses` | P1 | `fetch_fs`, `pull_fs`, `s3` | p `anytime_storage_prefix.py` (high-water marks), w `anytime_s3_objects_consistent.py` | packet hold `pbs ↔ s3` (stale-PUT variant) |
| `binlog-metadata-matches-stored-bytes` | P1 | `fetch_fs`, `pull_fs`, `s3` | p `anytime_storage_prefix.py` | m `parallel_driver_kill_mysqld.sh`; knob `post_rotation_source_kill`; clock optional |
| `checkpoint-bounds-unpersisted-data` | P1 | `pull_fs`, `s3` (pull-like); SUT check everywhere | p `parallel_driver_checkpoint_rpo.py` (quiet window); +SUT backlog | checkpointing drawn on |
| `s3-whole-object-put-publishes-intended-bytes` | P2 | `s3` | p `parallel_driver_s3_faults.py`, p `anytime_s3_tail_recovery.py`, p `parallel_driver_kill_binlog_server.sh`; +SUT | large `max_binlog_size` + small checkpoint; native mid-body partitions |
| `stored-binlogs-replay-to-source-state` | P1 | `fetch_fs`, `pull_fs`, `s3` | p `finally_replay_restore.py` (scratch `mysqld`, `decrypt.py`) | event variety |

### Multiple PBS processes

| Property | P | Templates | Commands (container) | Workload actions / knobs |
|---|---|---|---|---|
| `second-writer-on-storage-fails-closed` | P0 | `contention` | p `parallel_driver_fetch.py` (no lock), w `parallel_driver_s3_second_writer.py`, p `finally_storage_reopens.py` | distinct ids + same-id arm; node hang / partition for the zombie |
| `other-process-startup-spares-live-writer` | P1 | `contention` | p `parallel_driver_purge.py`, p `parallel_driver_fetch.py` (startup cleanup), p `parallel_driver_query.py`; +SUT | knob `query_adds_encryption` |
| `stale-index-commit-never-wedges-storage` | P1 | `contention` | p `parallel_driver_purge_pair.py` (T19), p `parallel_driver_purge.py` | — |
| `distinct-storages-shared-server-id-stay-consistent` | P2 | `contention` | p `parallel_driver_sibling_writer.py` (T19), w `anytime_source_health.py` (eviction `Sometimes`) | shared id 102; the catalog's "Deferred until added" lifts once this driver exists |

### Resume and replay

| Property | P | Templates | Commands (container) | Workload actions / knobs |
|---|---|---|---|---|
| `stored-transactions-exactly-once-across-resumes` | P0 | `fetch_fs`, `pull_fs`, `s3` | p `eventually_converge.py`, p `eventually_pull_converges.py`, p `anytime_storage_prefix.py`; +SUT | source bytes + `gtid_executed` are ground truth; flagged timelines excluded |
| `inprocess-reconnect-resumes-without-dup-or-gap` | P1 | `pull_fs`, `s3` (pull-like) | w `parallel_driver_source_admin.py` (`KILL QUERY`/`KILL CONNECTION`, `SHUTDOWN` mid-transaction), m `parallel_driver_kill_mysqld.sh`, p `anytime_storage_prefix.py`; +SUT | knob `source_stop_mode` (lead (a)) |
| `gtid-resume-preserves-source-rotations` | P1 | `fetch_fs`, `pull_fs`, `s3` | w `parallel_driver_source_admin.py` (`FLUSH BINARY LOGS` while PBS is disconnected), p `anytime_storage_prefix.py`; +SUT as `ALWAYS(cond ‖ source_kills_possible)` | ticket 14 known failure |
| `resume-continuity-guard-never-trips` | P1 | `fetch_fs`, `pull_fs`, `s3` | +SUT (tag-gated on `source_reset`/`source_rewind`), p `anytime_outcomes.py`, p `anytime_storage_prefix.py` | knob `source_stop_mode` |
| `repeat-fetch-on-caught-up-storage-is-noop` | P2 | `fetch_fs` | p `eventually_converge.py` (extra no-op fetch) | — |

### Pull replication source serving

| Property | P | Templates | Commands (container) | Workload actions / knobs |
|---|---|---|---|---|
| `sender-file-switch-emits-artificial-rotate` | P0 | `pull_fs` | w `parallel_driver_pull_reader.py` (capture vs stored via storage helper), w `parallel_driver_source_admin.py` / p `parallel_driver_reconfigure.py` (checksum toggle); +SUT | checkpointing weighted on; default-config case in MTR (R12) |
| `pull-served-stream-self-consistent` | P0 | `pull_fs`, `s3` (pull-like) | w `parallel_driver_pull_reader.py` | — |
| `pull-chain-prefix-consistency` | P1 | `pull_fs`, `s3` (pull-like) | w `parallel_driver_pull_reader.py`, w `parallel_driver_chained_fetch.py`; +SUT tag-gated on `storage_faults_active` | — |
| `chained-fetch-from-pull-reproduces-upstream` | P1 | `pull_fs` | w `parallel_driver_chained_fetch.py`, w `eventually_readers_converge.py` | position mode only |
| `dump-session-ends-with-eof-or-err-packet` | P1 | `pull_fs` | w `parallel_driver_pull_reader.py`, p `serial_driver_retention_cycle.py`, p `anytime_outcomes.py` (`"net    : failed to fetch next event block"`); +SUT | knob `ahead_of_flushed_requests` |
| `blocking-dump-before-first-flush-eventually-served` | P1 | `pull_fs` | w `parallel_driver_pull_reader.py` (empty/magic-only labels), p `parallel_driver_ensure_pull.py`, w `parallel_driver_source_load.py` | wedge reset re-seeds an empty storage |
| `blocking-dump-ahead-of-flushed-position-eventually-served` | P1 | `pull_fs` | w `parallel_driver_pull_reader.py` | knob `ahead_of_flushed_requests` |
| `misaligned-dump-position-rejected-cleanly` | P1 | `pull_fs` (natural), `hostile_dump` (direct) | w `parallel_driver_pull_reader.py`, w `parallel_driver_hostile_dump.py`; +SUT | knob `misaligned_dump`; optional release-build launch |
| `dump-preamble-matches-request` | P2 | `pull_fs` | w `parallel_driver_pull_reader.py`, p `serial_driver_retention_cycle.py` (after-purge anchor) | — |
| `large-event-served-or-rejected-cleanly` | P2 | `pull_fs` | w `parallel_driver_source_load.py`, w `parallel_driver_pull_reader.py`; +SUT | knob `large_event` (ticket 10) |
| `rs-unauthenticated-session-never-served` | P2 | `pull_fs`, `hostile_dump` | w `parallel_driver_listener_abuse.py` | — |
| `pull-retention-cycle-keeps-serving-survivors` | P1 | `pull_fs` | p `serial_driver_retention_cycle.py`, w `parallel_driver_pull_reader.py` | — |

### Query and purge

| Property | P | Templates | Commands (container) | Workload actions / knobs |
|---|---|---|---|---|
| `interrupted-purge-leaves-openable-storage` | P0 | `fetch_fs`, `s3` | p `serial_driver_purge.py`, p `finally_storage_reopens.py` | knob `purge_warning` (N3) |
| `query-success-never-omits-middle-binlog` | P1 | `fetch_fs`, `pull_fs`, `s3` | p `parallel_driver_query.py`, p `parallel_driver_s3_faults.py` (per-path GET rule); +SUT | — |
| `cli-json-envelope-and-exit-code-agree` | P1 | `fetch_fs`, `pull_fs`, `s3` | p `parallel_driver_query.py`, p `serial_driver_purge.py`, p `serial_driver_damaged_copy_query.py`; +SUT | kill driver never hits query pids (T15) |
| `query-modes-never-write-storage` | P2 | `fetch_fs` | p `parallel_driver_query.py`, p `parallel_driver_reconfigure.py` (vacuity); +SUT | knob `query_adds_encryption` (lead (e)) |
| `search-by-timestamp-includes-all-early-files` | P2 | `fetch_fs` | p `parallel_driver_query.py`, w `parallel_driver_source_load.py` (`SET TIMESTAMP`) | clock optional |

### Encryption at rest

| Property | P | Templates | Commands (container) | Workload actions / knobs |
|---|---|---|---|---|
| `encrypted-storage-decrypts-to-source` | P1 | `fetch_fs`, `pull_fs`, `s3` | p `anytime_storage_prefix.py` (`decrypt.py`), p `finally_storage_reopens.py` (snapshot-copy readback, T14); +SUT | encryption drawn on |
| `encrypted-storage-no-plaintext-at-rest` | P1 | `fetch_fs`, `pull_fs` | p `parallel_driver_reconfigure.py`, p `anytime_storage_prefix.py`; +SUT | reconfigure arm |
| `mixed-encryption-storage-served-correctly` | P1 | `pull_fs` | p `parallel_driver_reconfigure.py`, p `eventually_pull_converges.py`, w `parallel_driver_pull_reader.py` | all KEKs kept |
| `ctr-keystream-not-reused-across-truncation` | P2 | `fetch_fs`, `s3` | p `parallel_driver_fetch.py` + supervisor snapshot hooks (T23), p `anytime_storage_prefix.py` | rewrite + encryption, small checkpoint |

### Rewrite mode

| Property | P | Templates | Commands (container) | Workload actions / knobs |
|---|---|---|---|---|
| `stored-rotate-or-stop-is-last-event` | P0 | `fetch_fs`, `pull_fs`, `s3` (rewrite) | p `anytime_rewrite_oracles.py`; +SUT | — |
| `rewrite-binlog-chain-valid-across-crashes` | P1 | `fetch_fs`, `pull_fs`, `s3`; upgrade variant **Deferred** (`upgrade`) | p `anytime_rewrite_oracles.py` | — |
| `rewrite-storage-transaction-sequence-matches-source` | P1 | `fetch_fs`, `pull_fs`, `s3` | p `anytime_rewrite_oracles.py`, p `finally_replay_restore.py`; +SUT | — |
| `rewrite-pitr-by-datetime-matches-source` | P1 | `fetch_fs` | p `anytime_rewrite_oracles.py` (`TZ=UTC`) | clock optional |
| `rewrite-toggle-between-runs-keeps-storage-resumable` | P2 | `fetch_fs` | p `parallel_driver_reconfigure.py`; +SUT | knob `rewrite_toggle` |
| `rewrite-checksum-none-fail-stop` | P2 | `fetch_fs` | p `parallel_driver_reconfigure.py` (checksum NONE) | knob `checksum_none_in_rewrite` |

### Source-side changes and version compatibility

| Property | P | Templates | Commands (container) | Workload actions / knobs |
|---|---|---|---|---|
| `source-swap-never-silently-accepted` | P1 | `fetch_fs` (RESET variant, flag); swap variant **Deferred** (`failover`) | m `parallel_driver_reset_source.sh`, p `anytime_source_history.py` | flag `source_reset` |
| `source-rewind-never-silently-continued` | P1 | `fetch_fs`, `pull_fs` (flag) | m `parallel_driver_rewind_source.sh`, p `anytime_source_history.py` | flag `source_rewind` |
| `source-purge-past-resume-point-fails-closed` | P2 | `fetch_fs`, `pull_fs` (flag) | p `serial_driver_source_purge_past_pbs.py`; +SUT | flag `source_purge_past` |
| `source-crash-restart-resume-is-faithful` | P0 | `fetch_fs`, `pull_fs`, `s3` | m `parallel_driver_kill_mysqld.sh`, p `anytime_storage_prefix.py`, p `eventually_converge.py` / `eventually_pull_converges.py` | knob `post_rotation_source_kill` (lead (b)) |
| `gtid-failover-no-lost-transaction-merge` | P1 | **Deferred** (`failover`) | — | — |
| `gtid-failover-resume-progresses` | P2 | **Deferred** (`failover`) | — | — |
| `source-upgrade-resume-continuity` | P1 | **Deferred** (`upgrade`) | — | — |
| `tagged-gtid-resume-exact` | P1 | `fetch_fs`, `pull_fs` (8.4 / 9.7 launches) | w `parallel_driver_source_load.py` (tagged GTIDs), p `anytime_storage_prefix.py` | `TRANSACTION_GTID_TAG` grant |
| `source-auth-survives-auth-cache-flush` | P1 | `fetch_fs`, `pull_fs` | w `parallel_driver_source_admin.py` (`FLUSH PRIVILEGES`, `ALTER USER`), m `parallel_driver_kill_mysqld.sh`, w `anytime_source_health.py` | — |
| `source-session-tls-when-configured` | P2 | `fetch_fs`, `pull_fs` | w `anytime_source_health.py` (`performance_schema.threads.CONNECTION_TYPE`) | — |
| `storage-format-version-mismatch-handled` | P2 | **Deferred** (`old-binary`) | — | — |
| `pull-client-version-matrix` | P2 | `pull_fs` (shipped client only); cross-version **Deferred** (`client-matrix`) | w `parallel_driver_pull_reader.py` | — |

### Liveness and resource boundaries

| Property | P | Templates | Commands (container) | Workload actions / knobs |
|---|---|---|---|---|
| `pull-chain-converges-after-heal` | P1 | `pull_fs`, `s3` (pull-like) | w `parallel_driver_quiet_window.py` (bounded per window), w `eventually_readers_converge.py`, p `anytime_outcomes.py` (N7 with attribution); +SUT | w `parallel_driver_source_admin.py` (`SHUTDOWN`, `KILL QUERY`) |
| `fetch-converges-after-faults-stop` | P1 | `fetch_fs`, `s3` (fetch-like) | p `eventually_converge.py` | — |
| `sigterm-honoured-within-bound` | P1 | `pull_fs`, `s3`, `fetch_fs` (startup half) | p `parallel_driver_ensure_pull.py`, p `parallel_driver_fetch.py` (self-requested quiet windows, stimulus stall / SIGSTOP) | — |
| `stalled-dump-client-isolated` | P1 | `pull_fs` | w `parallel_driver_quiet_window.py` (reader that stops calling `recv()`) | — |
| `s3-crash-leftovers-bounded` | P1 | `s3` | p `anytime_s3_local_buffer.py`, w `anytime_s3_objects_consistent.py` | large-binlog draw (> 8 MiB, ideally > 16 MiB) |
| `listener-survives-fd-exhaustion` | P1 | `pull_fs` (fd-limit variant) | supervisor `ulimit -n 256`, w `parallel_driver_listener_abuse.py` (flood); +SUT | variant `fd_limit` |
| `collector-memory-bounded-by-checkpoint` | P2 | `fetch_fs`, `pull_fs` | p `anytime_memory_sample.py`, w `parallel_driver_source_load.py` (64-100 MiB transactions); +SUT | large-binlog draw; `mem_limit` compose variant if honoured |
| `stale-sessions-reclaimed-within-bound` | P2 | `pull_fs` | w `parallel_driver_quiet_window.py` (silent / vanishing clients), storage helper `GET /sessions` | — |

### Reachability

| Property | P | Templates | Commands (container) | Workload actions / knobs |
|---|---|---|---|---|
| `recovery-paths-exercised` | P1 | all | +SUT `REACHABLE`/`SOMETIMES`; p `anytime_outcomes.py` log-line anchors (tail truncation, magic-only reopen, purge cleanup `warning`) | union of the above |

**Count:** 63 properties mapped to a minimal-topology template (3 of them with an additional Deferred variant: `source-swap-never-silently-accepted`, `rewrite-binlog-chain-valid-across-crashes`, `pull-client-version-matrix`); **4 fully Deferred** (`gtid-failover-no-lost-transaction-merge`, `gtid-failover-resume-progresses`, `source-upgrade-resume-continuity`, `storage-format-version-mismatch-handled`). `distinct-storages-shared-server-id-stay-consistent` and the purge-pair anchor of `stale-index-commit-never-wedges-storage` become runnable through the new `contention` steps (T19).

## Deferred templates and containers (T25)

None of these is in the minimal topology. Each is a separate launch configuration.

| Template | Extra containers / images | What it adds | Unlocks |
|---|---|---|---|
| `failover` | `mysql-replica` (replica of `mysql-source`, `log_replica_updates`, same image) and `source-vip` (a TCP proxy, e.g. HAProxy, in front of both, switched by a driver): six containers | promotion and VIP swap driven by a driver; partitions isolating the old primary | `gtid-failover-no-lost-transaction-merge`, `gtid-failover-resume-progresses`, swap variant of `source-swap-never-silently-accepted` |
| `upgrade` | variant `mysql-source` image carrying two MySQL tarballs (e.g. 8.0 → 8.4, 8.4 → 9.7) that the wrapper swaps over one datadir on a restart; no extra container | in-place source upgrade under a live PBS | `source-upgrade-resume-continuity`, upgrade variant of `rewrite-binlog-chain-valid-across-crashes` |
| `old-binary` | variant `pbs` image with pbs-0.2.3 / 0.3.1 binaries and legacy fixture storages (whether old tags build with the current presets is unverified) | binary chosen per restart over one storage | `storage-format-version-mismatch-handled` |
| `client-matrix` | variant `workload` image with 8.0 / 8.4 / 9.7 `mysqlbinlog` binaries and their libraries | reader picks a client version | cross-version half of `pull-client-version-matrix` |

Real MySQL replicas as pull clients stay out entirely (ticket 20).

## SDK selection

| Where | SDK / instrumentation | Purpose |
|---|---|---|
| `binlog_server` (C++23, Clang 20, libc++) | coverage instrumentation (`trace-pc-guard,pc-table`, `--build-id`, `antithesis_instrumentation.h` or legacy `libvoidstar`, unstripped binary in `/symbols`); later the C++ SDK (header-only, C++20, Clang ≥ 16) behind a CMake option, with a small wrapper that reads the timeline-tag file | guided exploration, thread pausing; SUT `ALWAYS`/`SOMETIMES`/`REACHABLE`/`UNREACHABLE` (B5) |
| workload and oracles (Python 3) | Antithesis Python SDK: `lifecycle.setup_complete`, `assertions.*`, `random` for every choice | all workload assertions |
| shell drivers | Antithesis randomness through a tiny Python shim (never `$RANDOM`) | deterministic exploration of kill points |

First launch: coverage instrumentation only, with workload assertions and the log-line fallbacks. SUT macros follow once B5 is answered and the Clang 20 workaround is verified.

## Fault requirements

No property needs node termination: every crash is a harness SIGKILL of `binlog_server` or `mysqld` (with the wrapper restart and the pull supervisor). Clock faults are optional.

| Fault | Needed? | Used by (examples) | Notes |
|---|---|---|---|
| Network faults (latency, congestion, partitions, bad node) on source ↔ pbs, pbs ↔ s3, pbs ↔ workload | **required** | `inprocess-reconnect-resumes-without-dup-or-gap`, `gtid-resume-preserves-source-rotations`, `pull-chain-prefix-consistency`, `s3-whole-object-put-publishes-intended-bytes`, `query-success-never-omits-middle-binlog`, `source-purge-past-resume-point-fails-closed` | default on; ask for **packet-hold** partitions on `pbs ↔ s3` (`stored-state-never-regresses` stale-PUT replay, T17) |
| Node hang / throttling | **required** | `second-writer-on-storage-fails-closed` (zombie), exploration halves of `sigterm-honoured-within-bound`, `stalled-dump-client-isolated`, `pull-chain-converges-after-heal` | bounded checks use harness stimuli in quiet windows instead (R1) |
| CPU modulation | **required** | collector-vs-sender races: `sender-file-switch-emits-artificial-rotate`, `other-process-startup-spares-live-writer`, `stale-index-commit-never-wedges-storage` | needs no instrumentation |
| Thread pausing | wanted | same races, finer-grained | needs coverage instrumentation (planned) |
| Harness faults (test commands) | **required** | writer SIGKILL/SIGTERM/SIGSTOP (`p/parallel_driver_kill_binlog_server.sh`), source SIGKILL/`SHUTDOWN`/RESET/rewind (`m/…`), SQL `KILL QUERY`/`FLUSH PRIVILEGES`/purge (`w/parallel_driver_source_admin.py`, `p/serial_driver_source_purge_past_pbs.py`), S3 HTTP faults (`p/parallel_driver_s3_faults.py`), ENOSPC/EIO (`libfsfault.so` via `LD_PRELOAD` in fetch, purge and **pull** launches, T16), fd limit | all quiet-aware (T3); implemented as test commands, so no tenant custom-fault setup is needed. They could later move to tenant custom faults. |
| Clock jitter | optional | `search-by-timestamp-includes-all-early-files`, `rewrite-pitr-by-datetime-matches-source`, `binlog-metadata-matches-stored-bytes` | `SET TIMESTAMP` covers the non-monotonic half without it; the quiet flag uses the proxy's monotonic clock |
| Node termination | **not needed** (optional) | adds only "every process in `pbs` dies at once" | if enabled, `/data` must survive it and the supervisor must be re-run; never on `s3` (it also hosts the control plane) |
| Page-cache loss | not available | ticket 19 | excluded from the catalog |

## Assumptions

- Antithesis runs this docker-compose topology. Template directories with the same name merge into one template, each command runs in its defining container, and `helper_` files are ignored.
- Terminal commands are alternatives: each branch runs one `eventually_` or `finally_`, and `finally_` runs only when no started command was killed.
- A killed test command's children may survive. The quiet watcher in `pbs` handles orphans (T3).
- Process SIGKILL keeps the page cache, so ticket 19 is unreachable.
- `libmysqlclient` 8.0 from Ubuntu 24.04 streams from 8.0.45, 8.4.8 and 9.7.2 sources over TLS (CI does this).
- `mysqlbinlog --read-from-remote-server --raw` without `--stop-never` returns the source bytes as sent and uses server_id 0, which never evicts PBS.
- The generic-tarball `mysqld` starts in `runtime-base` with `libaio1t64` and `libnuma1` (cost of G2 unverified).
- Versity 1.8.0's posix backend is strongly consistent for overwrite-then-read and supports `ListMultipartUploads`.
- `debug_clang20` builds cleanly with the coverage flags; the Clang 17-20 SDK compile bug affects only SDK assertion macros.
- Each driver is bounded (about 1-2 minutes of simulated time) and exits 0 unless it found a bug; the prefix oracle is incremental so `anytime_` cost does not grow with history.
- The workload ledger is never ground truth; source bytes and `gtid_executed` are.

## Open Questions

- [needs-human] **Tenant fault configuration.** Are network faults with packet-hold partitions, node hang/throttle and CPU modulation enabled for the Percona tenant and webhook? Clock jitter is optional; node termination is not needed. Should harness faults later move to tenant custom faults?
- [needs-human] **B3: known-defect containment.** Accept the proposed knobs + wedge helper with ~15 % trigger rate in the main templates plus a `known_defects` template, or fix first, or use variant builds? This decides T12/T13 defaults.
- [needs-human] **B2: `contention`.** Keep as a fail-closed tracker in a separate launch, drop it, or promote parts into the main templates (contract question 1)?
- [needs-human] **B5: SUT macros and product-code changes.** Are SDK macros behind a CMake option acceptable, and which of the ~10 logic changes? Until then the first launch is coverage-only with log-line fallbacks.
- [needs-human] Official `mysql:` or `percona/percona-server:` source image? Which MySQL version first (8.4 LTS suggested)?
- [needs-human] Are flagged source-history actions (RESET, rewind, purge past resume) worth their budget in `fetch_fs`/`pull_fs`, or should they go to their own launch?
- [needs-check] Does Antithesis honour compose `ulimits`, `mem_limit` and `tmpfs: size=`? The fd limit no longer needs it (supervisor `ulimit`); `collector-memory-bounded-by-checkpoint` falls back to VmRSS sampling without `mem_limit`.
- [needs-check] Is the `workload` container subject to node hang and network faults? R20 and R22 removed the dependency for `stalled-dump-client-isolated` and `stale-sessions-reclaimed-within-bound`; it still matters for reader-link realism and for oracles reached over `pbs:8081`.
- [needs-check] Does `asan_clang20` combine with `trace-pc-guard` under Antithesis? If not, run ASan as a separate uninstrumented launch or locally.
- [needs-check] Clang 20 SDK workaround: is `-flto=thin` plus link-time instrumentation workable with the static Boost and AWS SDK builds?
- [needs-check] Does a compose `restart:` policy or the entrypoint re-run after a container restart (relevant only if node termination is enabled)?
- [needs-check] Do several `eventually_`/`finally_` commands in one template get branch budget each, as assumed?
- [needs-check] Cost of the scratch `mysqld` replay per terminal check, and of large-binlog timelines under the instrumented build; sample if too slow.
- [open] `WITH_ANTITHESIS` CMake option or legacy `libvoidstar.so` for the first launch?

## Traceability: T1-T25

| ID | Where applied |
|---|---|
| T1 | Property map (all 67 slugs, no stale slugs); source admin actions; node termination optional |
| T2 | `helper_storage_server.py` in `pbs` |
| T3 | `/__harness__` quiet flag in `s3`; `may_act()`; quiet watcher and orphan fencing in the supervisor; process groups |
| T4 | `parallel_driver_reconfigure.py`; `config_history.jsonl`; vacuity `Sometimes` |
| T5 | `server_id` table; eviction pair in `anytime_source_health.py` |
| T6 | oracle `mysqlbinlog` without `--stop-never`; unique `--connection-server-id` for readers |
| T7 | `logger.file` empty → `cout_logger`; `tee -a /data/logs/` |
| T8 | bimodal `max_binlog_size`; incremental prefix cache |
| T9 | event-variety load generator |
| T10 | `serial_driver_retention_cycle.py` in `pull_fs` |
| T11 | timeline-tag file, flags declared up front, `acks.pbs` ordering |
| T12 | `wedge.py` (waits for B3) |
| T13 | knobs table; `known_defects` template (waits for B3) |
| T14 | `readback.py`, used only in `finally_storage_reopens.py` |
| T15 | role pidfiles; kill driver targets writers only; `kills.jsonl` |
| T16 | supervisor `LD_PRELOAD=libfsfault.so` per restart |
| T17 | `s3` template both-phase arm; packet-hold partitions requested |
| T18 | `hostile_dump` template; optional release-build launch |
| T19 | `parallel_driver_purge_pair.py`, `parallel_driver_sibling_writer.py` in `contention` |
| T20 | `source_state.py` polling `performance_schema.threads` |
| T21 | fd limit as a timeline variant through the supervisor |
| T22 | `helper_lib/oracles.py` |
| T23 | CTR snapshot hooks in the fetch driver and the supervisor |
| T24 | rewrite `file_size` ≥ 16 KiB floor |
| T25 | Deferred templates and containers table |
