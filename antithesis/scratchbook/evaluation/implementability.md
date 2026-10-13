---
sut_path: /Users/hrvoje/work/percona-binlog-server
commit: 99de68207c84f83e0d477ebe63d2f531facbb954
updated: 2026-10-13
external_references:
  - path: https://claude.ai/artifact/9om6fiPd64QzVXbEWEtDT1
    why: QA bug-ticket list for binlog_server (local snapshot antithesis/scratchbook/.tickets-snapshot.txt); open tickets used as leads, each validated against the code before use
---

# Evaluation lens 3: Implementability

Question asked for every property: can it actually be checked with the planned four-container topology (`mysql-source`, `s3`, `pbs`, `workload`; no shared volume), the planned test templates (`fetch_fs`, `pull_fs`, `s3`, `contention`) and the codebase as it is (no Antithesis SDK, Clang 20 + libc++)?

Inputs read: `property-catalog.md` (all 60 entries), `deployment-topology.md`, `existing-assertions.md`, the relevant parts of `sut-analysis.md`, evidence files where depth was needed, and the code/harness files cited below. Antithesis command semantics come from the antithesis-workload `test-commands.md` reference and the research `faults.md` reference.

Findings are ordered catalog-wide first, then property-specific. Categorisation (Gap/Bias/Refinement) is left to synthesis.

---

## Catalog-wide findings

### C1. The topology and the catalog have drifted apart. 40 of 60 properties have no template or command, and the topology still uses about 25 pre-merge slugs

**Evidence.** Cross-checking every backticked slug in `deployment-topology.md` against `properties/*.md`:

- The topology names slugs that no longer exist (merged away or renamed): `storage-reopens-after-writer-crash`, `restart-after-kill-converges-to-source`, `zombie-writer-takeover-fails-closed`, `purge-ctor-never-truncates-live-tail`, `startup-tmp-cleanup-spares-live-writer`, `purge-during-writer-keeps-storage-openable`, `query-concurrent-with-writer-never-errors`, `query-read-error-not-reported-as-success`, `listed-binlog-size-is-stable-readable-prefix`, `sigterm-not-starved-by-storage-lock`, `pull-reconnects-after-transient-source-fault`, `source-restart-reauth-liveness`, `blocking-dump-catches-up-after-io-stall`, `s3-pull-converges-despite-reupload-growth`, `enospc-write-fails-closed-without-duplicates`, `failed-flush-retry-never-duplicates`, `s3-stale-put-replay-never-regresses-object`, `s3-no-orphaned-multipart-uploads`, `s3-local-buffer-bounded-across-crashes`, `encrypted-binlog-decryptable-after-crash`, `metadata-timestamps-match-stored-events`, `sender-reads-only-flushed-bytes`, `lifecycle-outcomes-all-reached`, `resume-replay-paths-exercised`, `pull-fde-switch-across-format-change`.
- 40 catalog slugs never appear in the topology. Examples: all six rewrite properties, all four encryption properties, `tagged-gtid-resume-exact`, `source-auth-survives-auth-cache-flush`, `source-session-tls-when-configured`, `large-event-served-or-rejected-cleanly`, `misaligned-dump-position-rejected-cleanly`, `rs-unauthenticated-session-never-served`, `pull-client-version-matrix`, `query-modes-never-write-storage`, `stored-rotate-or-stop-is-last-event`, `pull-served-stream-self-consistent`, `dump-preamble-matches-request`.
- Several of these need workload **actions** that no listed driver performs: `SET GLOBAL binlog_checksum` toggles (`sender-file-switch-emits-artificial-rotate` format variant, `rewrite-checksum-none-fail-stop`), `FLUSH PRIVILEGES`/`ALTER USER` (`source-auth-survives-auth-cache-flush`), `KILL QUERY`/`KILL CONNECTION` of the dump thread (`inprocess-reconnect-resumes-without-dup-or-gap`, `pull-chain-converges-after-heal`), tagged-GTID transactions (`tagged-gtid-resume-exact`, which also needs the `TRANSACTION_GTID_TAG` privilege for the workload user, absent from the planned init SQL), hostile or misaligned dump requests (`misaligned-dump-position-rejected-cleanly`), `SET TIMESTAMP` skew (`search-by-timestamp-includes-all-early-files`), and the encryption-added query config (`query-modes-never-write-storage`).

**Why it matters for implementability.** The workload skill will implement from the topology's command tables. Properties that are not mapped will be dropped silently, or bolted onto templates whose other properties they break (see C3, C5).

**Suggested action.** Regenerate the topology's per-template command tables from the final catalog slugs. Add a "property → template → command(s) → container" column, and list the workload actions each property needs.

### C2. No shared volume, but many oracles need data from another container

The topology deliberately has no shared volume. Filesystem storages live only in `pbs:/data`, the source datadir only in `mysql-source`, and the workload ledger and reader captures only in `workload`. The following checks compare data that sits in two different containers:

| Property | Needs in `workload` | Lives in |
|---|---|---|
| `pull-chain-prefix-consistency` | "pull client bytes are a prefix of **stored** bytes", "served bytes never change at the same offset" | client capture in `workload`, storage in `pbs` |
| `chained-fetch-from-pull-reproduces-upstream` | "downstream file equals **upstream** file prefix" | downstream in `workload:/data/chain`, upstream in `pbs` |
| `sender-file-switch-emits-artificial-rotate` | "`--raw` output file equals the **stored** file byte for byte" | same split |
| `blocking-dump-ahead-of-flushed-position-eventually-served`, `blocking-dump-before-first-flush-eventually-served`, `dump-preamble-matches-request` | label each attempt by storage state (empty / magic-only / collector reconnecting / oldest indexed file) "from `list`" | `list` needs the storage, which is in `pbs`; pull's listener answers only an exact-string allowlist (`src/minimysql/network_service.cpp:502-550`), with no `SHOW BINARY LOGS` |
| `pull-chain-converges-after-heal` / `eventually_readers_converge.py` | "every live blocking client holds every stored byte" | same split |
| `stored-transactions-exactly-once-across-resumes` (topology: `eventually_converge.py` in `pbs` checks "against the ledger") | ledger | `workload`-local `/data/ledger.jsonl` (topology line 173) |
| `source-swap-never-silently-accepted` | the workload "tracks the source epoch", but fetch exits are observed in `pbs` | split |

For S3 storages the split disappears (the workload reads the bucket directly through boto3 on `s3:9000`). For filesystem storages it does not.

**Suggested action.** Pick one mechanism and state it in the topology:

- (a) A read-only helper in `pbs` (for example a `helper_` Python HTTP server over `/data`, or a small "describe storage" endpoint) that the workload queries with retries. Caveat: `pbs↔workload` faults hit the oracle too.
- (b) Run the pull-reader oracles that need stored bytes in the `s3` template only.
- (c) Weaken the workload-side checks to "client ⊑ source" and move "client ⊑ stored" to a `pbs`-side reader on `127.0.0.1:3307`. This loses link faults on that path.

The ledger is unnecessary: the source's own binlogs (via `mysqlbinlog`) and `gtid_executed` are the reference, and they can be read from `pbs`. The source epoch can be detected from `pbs` by SQL (`@@server_uuid`, plus a marker row that RESET does not remove).

### C3. `ANTITHESIS_STOP_FAULTS` does not stop the harness's own fault drivers, and quiet-period liveness checks need a cross-container "quiet" signal

`STOP_FAULTS` pauses only Antithesis-injected faults. The catalog moves most fault injection into harness drivers: `parallel_driver_kill_binlog_server.sh` (in `pbs`), `parallel_driver_kill_mysqld.sh` (in `mysql-source`), `parallel_driver_s3_faults.py` rules in the proxy, `fsfault` rules, and SIGSTOP/SIGCONT. These drivers keep running during a mid-run quiet period. Antithesis also gives no way to ask whether a fault is active.

Affected properties are those that say "after faults stop", "during a quiet period" or "only while links are fault-free": `pull-chain-converges-after-heal`, `stalled-dump-client-isolated` ("evaluated only while storage and source links are fault-free"), `sigterm-honoured-within-bound`, `stale-sessions-reclaimed-within-bound`, `source-auth-survives-auth-cache-flush` (TLS `Always` with T s), `gtid-failover-resume-progresses`, `blocking-dump-*`.

`eventually_` does kill all other commands, but:

1. Only one `eventually_` runs per timeline (already an open question in the topology).
2. Killing a driver command does not necessarily kill the `binlog_server` subprocess it spawned. An orphaned `fetch` keeps writing, so `eventually_converge.py` then runs a **second writer** on the same storage, which is exactly the ticket-24 scenario. The orphan may also still hold the inherited `flock` on `/data/fetch/writer.lock`.
3. Proxy rules set by `parallel_driver_s3_faults.py` persist in the `s3` process after the driver is killed.

**Suggested action.**

- Add a harness "quiet" flag that every custom-fault driver checks before acting. All containers can reach it, for example an object in the S3 bucket via `s3:9000` (bypassing the proxy) or a row on `mysql-source`.
- `eventually_`/`finally_` and any mid-run liveness command must: (i) `PUT /__faults__ {"rules": []}`; (ii) kill every `binlog_server` except the supervisor's `pull`, or acquire the writer `flock` before acting; (iii) set the quiet flag.
- Drivers should run `binlog_server` in its own process group, so that killing a driver can take the child with it.

### C4. Bounded-latency `Always` checks cannot be evaluated while the faults that motivate them are active

Four properties use bounded liveness as `Always`: `sigterm-honoured-within-bound`, `stalled-dump-client-isolated`, `stale-sessions-reclaimed-within-bound`, and `pull-chain-converges-after-heal` (which is `Sometimes`, but within a bound T). Their stated angles are node hang, throttling, S3 latency or packet hold, and process pause. Each of those can stretch wall-clock latency without limit (a node hang of `pbs` is unbounded from the workload's view). The bound then either fires on correct behaviour or needs a slack large enough to make the check vacuous. Measuring only under `STOP_FAULTS` removes the precondition, because the stall is the fault.

**Suggested action.** Create the precondition with **harness-controlled, bounded** stimuli, and measure under `STOP_FAULTS` plus the C3 quiet flag:

- an S3 `stall` proxy rule with a known number of `seconds` (so the bound is `read_timeout + 1 + stall + slack`);
- a reader that stops calling `recv()` (instead of a node hang of the client container);
- SIGSTOP/SIGCONT with a known duration.

Keep Antithesis-native latency for exploration-only `Sometimes` hints.

### C5. Config dimensions are drawn once per timeline, but several properties need configuration changes between runs within a timeline

Topology: "Each template draws its config dimensions once per timeline … stores them in `/data/<template>/config.json`." The following properties need restarts with a **different** config on the same storage:

- `rewrite-toggle-between-runs-keeps-storage-resumable` (rewrite on/off, `base_file_name`, `file_size`, mode);
- `encrypted-storage-no-plaintext-at-rest` (encryption toggled between runs);
- `mixed-encryption-storage-served-correctly` (none → KEK A/cipher X → KEK B/cipher Y);
- `query-modes-never-write-storage` (query with an encryption-added config against an unencrypted storage);
- `storage-format-version-mismatch-handled` (binary choice per restart);
- `rewrite-checksum-none-fail-stop` and the `sender-file-switch-emits-artificial-rotate` format variant (source-side `binlog_checksum` toggles mid-timeline).

The topology also lists `mode` (gtid/position) as a per-timeline draw. A mode switch mid-storage is a refusal case, so it must not happen accidentally either.

**Suggested action.** Add an explicit "config-mutation" driver (or a dedicated template). Its draws are logged so that oracles key on "config in force when binlog X was created". Keep the immutable dimensions (mode, rewrite in the non-toggle templates) fixed per timeline.

### C6. Expected-to-fail properties wedge the shared storage and starve the rest of the timeline

These properties fire today, and when they fire they leave the storage or the process permanently stuck:

| Trigger | Effect | Properties starved |
|---|---|---|
| Ticket 15: kill during rotation | The storage is refused in every mode until manual repair | `kill-anywhere-storage-reopens`, and then every `fetch_fs`/`pull_fs` property |
| Lead (b): source SIGKILL right after a rotation, position mode | Debug-build abort at `storage.cpp:138` on every retry | Every property in that timeline (the topology runs `kill_mysqld` in `fetch_fs`/`pull_fs`/`s3`) |
| N3: purge `warning` | Storage unopenable | Everything after a `serial_driver_purge.py` warning |
| Ticket 7: misaligned dump | Debug abort of all of pull, collector included | Every `pull_fs` property each time `misaligned-dump-position-rejected-cleanly` runs |
| Ticket 10: ≥ 16,777,211-byte event | Every reader reconnects forever at that event | Every `pull_fs` reader property once the topology's "occasional large events" exceed the limit |

Once wedged, every later check in that timeline is either noise (the same failure re-reported) or vacuous (no progress).

**Suggested action.** For each known wedge, add one of the following:

- a workload-side detector that records the wedge once and then repairs the storage or resets it to a fresh one (a "known-failure reset"), gated so the triggering property still fires;
- or isolate the trigger in its own template or timeline variant. Examples: cap events below 16,777,211 bytes outside a `large_event` variant; keep misaligned/hostile dumps out of the reader-convergence timelines; gate the post-rotation source kill.

### C7. SUT-side assertions: about 25 properties depend on an unbuilt SDK path, and several "assertions" are really product code changes

`existing-assertions.md`: no SDK and no instrumentation. The topology records that the C++ SDK on Clang 17–20 has a compile bug whose workaround is `-flto=thin` plus link-time instrumentation, unverified with the static Boost and AWS SDK builds (topology open question). Catalog open question 7 (will macros in `src/` be accepted) is unresolved. Beyond inserting macros, the following planned assertions need **new state or behaviour** in the SUT:

| Property | Planned SUT change |
|---|---|
| `failed-flush-never-reappends-data` | A new flag set when `save_binlog_metadata` throws after `size +=`, checked in `flush_event_buffer_internal` |
| `graceful-exit-leaves-flushed-storage` | "Requires surfacing the swallowed failure" from `~storage` to the operation layer. This changes error propagation, not just observation |
| `stored-rotate-or-stop-is-last-event` | `ALWAYS_OR_UNREACHABLE` "needs a persisted closed flag or a last-event read". A storage-format change or a new read at open |
| `encrypted-storage-decrypts-to-source` | A backend stream-size accessor plus per-stream "a write has failed" tracking |
| `rs-unauthenticated-session-never-served` | A new `authenticated` flag set only at `network_service.cpp:457` |
| `dump-session-ends-with-eof-or-err-packet` | A new `catch` around `get_event` |
| `stale-sessions-reclaimed-within-bound` | An active-session gauge |
| `cli-json-envelope-and-exit-code-agree` | A new top-level `catch (...)`. This changes crash behaviour, which is what the property observes |
| `binlog-metadata-matches-stored-bytes` | A "chain check in `load_and_validate_binlog_metadata_set`". That is a new validator, not an assertion |
| `stored-transactions-exactly-once-across-resumes` | Receive-side "GTID not already stored or buffered" check. Needs the union of all stored `added_gtids` at each transaction boundary, which the writer does not keep in a queryable form today |

Properties whose *primary or only* safety detector is SUT-side:

- `failed-flush-never-reappends-data` (has workload fallbacks);
- `resume-continuity-guard-never-trips` (mostly SUT; has one workload `Always`);
- `dump-session-ends-with-eof-or-err-packet`. Its only workload assertion is a `Sometimes`, so the safety half is SUT-only;
- `listener-survives-fd-exhaustion` (has workload `Always`);
- `collector-memory-bounded-by-checkpoint` (the SUT `ALWAYS` is primary);
- `query-modes-never-write-storage` (has workload `Always`).

**Suggested action.**

- Tag each SUT assertion as "macro-only" or "needs SUT logic change".
- Give every SUT-only safety check a workload or log-line fallback for the first runs. Example for `dump-session-ends-*`: `network_service.cpp:308-312` already logs `"net    : failed to fetch next event block for {}"` at error level before the silent `co_return`, so a log scan can stand in for the `UNREACHABLE`.
- Sequence the first launch with coverage instrumentation only (no SDK macros), as the topology already hints.

### C8. QA oracles are not reusable "unmodified"

Topology: install `hypothesis` "so the oracle helpers can be imported unmodified". In `extra/qa/fetch_crash_test.py`:

- `storage_holds_prefixes` (`:637`) and `check_complete_copy` (`:671`) are methods of `FetchCrashMachine(RuleBasedStateMachine)` (`:515`). They use module globals `SERVER`/`BINSRV` (`:420-421`).
- They read the storage as a local directory (`BINSRV.storage / name`) and the source as a local datadir (`MySQLServer.binlog_path`, `:214`).
- They assume the index starts at the source's first binlog (`indexed != server_names[:len(indexed)]`, `:640`). That breaks after any purge (`interrupted-purge-*`, `stale-index-*`) or with a seeded source that already rotated.

Only `parse_events` (`:241`), `transaction_boundaries` (`:257`) and the constants are reusable as-is. `binlog_as_sent` has to be replaced by `mysqlbinlog --read-from-remote-server --raw` (the topology already says so). S3 storages need a download path.

**Suggested action.** Plan a small oracle library (`helper_lib/oracles.py`) with these features:

- inputs are source-bytes and storage-bytes providers;
- index-suffix-aware comparison;
- a decrypting reader for encrypted storages, needed by `metadata-size-at-transaction-boundary`, `binlog-metadata-matches-stored-bytes` and `stored-rotate-or-stop-is-last-event`. The catalog's "decrypt through pull" is not available in `fetch_fs`, see P1.

### C9. Oracle `mysqlbinlog` dump connections can evict PBS's dump thread

The byte-prefix oracles call `mysqlbinlog --read-from-remote-server` against the source repeatedly, from `anytime_` commands that can run concurrently. `distinct-storages-shared-server-id-stay-consistent` itself documents that the source evicts a dump thread that shares a `server_id`, and that PBS registers no replica UUID. If the oracle's connection server id equals PBS's `replication.server_id`, or several oracle instances share one, the oracle evicts PBS (ER 1236, a non-zero exit). The result is spurious exits and spurious "pull exited" outcomes in `pull-chain-converges-after-heal` (N7 sub-check).

**Suggested action.** Use an explicit, per-invocation-unique `--connection-server-id` for every oracle and reader, disjoint from all PBS server ids. Verify what `mysqlbinlog` does by default for the chosen version (see Uncertainties).

### C10. Workload sizing conflicts: a small `max_binlog_size` versus properties that need large binlogs or transactions

The topology sets `max_binlog_size` to about 64 KiB to drive rotation. These properties need much larger objects:

- `s3-crash-leftovers-bounded`: multipart only above 8 MiB (its evidence file, "the workload must grow at least one binlog past 8 MiB, preferably past 16 MiB");
- `pull-chain-converges-after-heal`: `SOMETIMES(single checkpoint upload ≥ 64 MiB)`;
- `collector-memory-bounded-by-checkpoint`: transactions of 64–100 MiB;
- `large-event-served-or-rejected-cleanly`: 16 MiB events.

With 64 KiB binlogs the upload-size and multipart anchors can never fire. Drivers are also budgeted at about 1–2 min each, and the `anytime_` prefix oracle re-reads every source binlog through `mysqlbinlog` on each run; 100 MiB transactions multiply that cost.

**Suggested action.** Draw `max_binlog_size` per timeline from a bimodal range (small, for rotation-heavy timelines; ≥ 32 MiB, for the S3, ticket-17 and memory timelines). Make the prefix oracle incremental (cache verified prefixes per file in `pbs:/data`).

---

## Property-specific findings

### P1. Encrypted filesystem storages in `fetch_fs`: "read through pull" means starting a second writer

Affected: `stored-binlog-byte-prefix-of-source`, `stored-transactions-exactly-once-across-resumes`, `stored-rotate-or-stop-is-last-event`, `encrypted-storage-decrypts-to-source`.

These properties say to read encrypted storages through pull. Pull is not a read-only reader. Its storage constructor runs `.tmp` removal and tail truncation (`storage_core.cpp:597-630, 974-983`), and its collector connects to the source and appends. In `fetch_fs`, starting pull just to read means a second writer or recovery pass on the storage under test. It changes the post-kill state the oracle is supposed to judge, and it is the out-of-contract concurrency the catalog excludes elsewhere.

Suggested action: use an independent Python decryptor (which `encrypted-storage-decrypts-to-source` already requires) as the primary encrypted reader. Restrict pull-based reads to `pull_fs` (pull is already the writer) or to quiescent points with fetch stopped and a pull config pointing at an unreachable source (it still runs recovery, so do this only in `eventually_`/`finally_`).

### P2. `gtid-resume-preserves-source-rotations`: the SUT `UNREACHABLE` "enabled only in timelines without source SIGKILL" cannot be gated

The SUT has no knowledge of what the `mysql-source` container did. The topology runs `parallel_driver_kill_mysqld.sh` in `fetch_fs`, `pull_fs` and `s3`, which are all the templates where GTID resume happens. So the `UNREACHABLE` fires on correct post-crash behaviour (the catalog says so).

Suggested action: drop the SUT `UNREACHABLE` in favour of the workload byte check. Alternatively, add a template variant without `kill_mysqld` and pass an env flag to `pbs` at start. The same per-template gating applies to `resume-continuity-guard-never-trips` if source RESET/swap ever shares a template (catalog open question 5).

### P3. `cli-json-envelope-and-exit-code-agree` and the `parallel_driver_query.py` "never errors" checks conflict with the kill driver

`parallel_driver_kill_binlog_server.sh` "picks a random live `binlog_server` pid", which includes `list`/`search_*`/`purge` invocations. The workload `Unreachable("cli: query or purge died by a signal")` and the query driver's "never error" assertions then fire on harness kills.

Suggested action: the kill driver targets only writer pids, identified through a pidfile or process tag written by the fetch/pull launchers, or logs every pid it signals so the query oracle can exclude them. `interrupted-purge-*` kills its own purge (self-SIGKILL), which is fine.

### P4. `stalled-dump-client-isolated`: healthy and stalled clients share the `workload` container

Node hang or congestion "of a client container" hits every reader in `workload`, healthy ones included. So "healthy clients receive every transaction beside a stalled client" cannot be set up with native faults in this topology. The topology already flags a possible fifth `reader` container.

Suggested action: implement the stalled client as a harness reader that stops calling `recv()` (no fifth container needed), measured under C3/C4 quiet conditions. Use a fifth container only if native-fault realism is wanted.

### P5. `second-writer-on-storage-fails-closed`: identical `server_id` makes the source evict one writer, which shrinks the race window

The `contention` template runs several `parallel_driver_fetch.py` with the same config. Same-config writers share `server_id`, so the source evicts one dump thread (ER 1236 exit; the property's own Antithesis Angle). The two writers therefore rarely append concurrently, and the ticket-24 interleaving (both past construction **and** appending) becomes rare. Only the `.tmp`/truncation race at construction stays reachable.

Suggested action: give overlapping writers distinct `server_id`s in the main arm, keep a same-`server_id` arm for the eviction path, and add `Sometimes(both writers appended after the other's construction)`.

### P6. `ctr-keystream-not-reused-across-truncation`: the observation channel races a restart, and the S3 variant depends on a bug

**Filesystem variant.** The "pre-restart snapshot taken by the killing driver" must capture bytes beyond the `.json` size after the kill and **before** the next opener truncates them. In `pull_fs` the supervisor restarts pull `idle` seconds later. In `fetch_fs` the next `parallel_driver_fetch` can take the writer lock first. Suggested action: snapshot inside the writer's own driver right after its child exits, before releasing the `flock` (fetch). For pull, have the supervisor snapshot before restarting.

**S3 variant.** Old ciphertext is observed only through leaked `/data/s3buf` copies, which is the N10 leak (`s3-crash-leftovers-bounded`). Once N10 is fixed the property goes blind on S3. State this dependency, or capture the object with a workload GET before the restart.

### P7. Properties that need containers or binaries outside the minimal topology

- `gtid-failover-no-lost-transaction-merge`, `gtid-failover-resume-progresses`: a replica plus VIP/proxy (the deferred `failover` template, six containers). They are not implementable in the first topology.
- `source-swap-never-silently-accepted` (**P0**): the VIP swap variant needs the same proxy container. Only the RESET variant fits `fetch_fs`, and the topology flags that it swamps signal. Its RESET command differs by version: `RESET MASTER` on 8.0 and `RESET BINARY LOGS AND GTIDS` on 8.2 and later; `fetch_crash_test.py:205-208` already branches. A P0 property whose main variant is deferred should say so.
- `source-upgrade-resume-continuity`, the `rewrite-binlog-chain-valid-across-crashes` upgrade variant: the deferred `upgrade` template.
- `storage-format-version-mismatch-handled`: pbs-0.2.3 and 0.3.1 binaries built in the image. Older tags may not build with the current pinned Boost 1.90 and AWS SDK 1.11.774 presets (not verified). It also needs legacy fixtures produced by those binaries. The property is "mostly deterministic" by its own text, so an MTR or fixture test is the practical home.
- `pull-client-version-matrix`: needs 8.0, 8.4 and 9.7 `mysqlbinlog` in `workload`. The topology ships one `mysqlbinlog` that matches `MYSQL_VERSION`. Easy to fix: add three tarballs (about 100 MB each), or keep just the three `mysqlbinlog` binaries plus their libs.
- `distinct-storages-shared-server-id-stay-consistent`: the catalog says "two PBS containers". It can be implemented inside `pbs` as a second process on a sibling storage launched by a test command. The topology does not mention this.

### P8. `listener-survives-fd-exhaustion` and `collector-memory-bounded-by-checkpoint`: resource limits

- **fd limit.** `ulimit -n 256` set by the PID-1 supervisor works without compose `ulimits`. But it applies to every `pull` in that timeline, and N6/N10 paths (S3 temp files, sessions) make pull exit often, which affects other `pull_fs` properties. Gate it as a timeline variant.
- **Memory.** If compose `mem_limit` is not honoured, the SUT `ALWAYS(event_buffer_.size() ≤ …)` together with workload sampling of `/proc/<pid>/status` VmRSS checks the safety half without any memory limit. Only the "no OOM crash loop" half needs the limit. Distinguishing an OOM SIGKILL from the kill driver's SIGKILL needs the kill-driver log from P3.

### P9. `pull-chain-converges-after-heal`: the N7 sub-check needs cause attribution

`Always("pull did not exit because of a network-only fault")` is read from the supervisor outcome log `{start_ts, exit_code, signal}`. That record cannot tell a network-caused exit from one caused by eviction (C9), fsfault, a proxy rule, or a C6 wedge. The outcome log must also capture the last error line of pull's log and the active custom-fault state (the C3 quiet flag, the proxy `GET /__faults__` snapshot) at exit time.

### P10. `gtid-resume-preserves-source-rotations` and `inprocess-reconnect-*`: preconditions are opportunistic

"`FLUSH BINARY LOGS` 0–3 times while PBS is disconnected and caught up" and "`KILL QUERY` of the dump thread during a large transaction" both need the workload to know PBS's connection state. That is observable from the source: `performance_schema.threads` or `SHOW PROCESSLIST` for the PBS user's `Binlog Dump` thread. The topology does not list it. Implementable; it just has to be written down so that the driver polls the source rather than guessing.

### P11. `rewrite-pitr-by-datetime-matches-source`: time zone

`mysqlbinlog --start-datetime`/`--stop-datetime` interpret T in the client's local time zone. The oracle must pin `TZ=UTC` (or the same TZ) for both the sampled-T computation and the `mysqlbinlog` call. Otherwise the check is off by the TZ offset. Minor.

### P12. `failed-flush-never-reappends-data`: trigger feasibility (positive)

`fsfault` reads static `FSFAULT_RULES` at process start (`extra/qa/fsfault/fsfault.c:20-31`), but `nth`/`count` express "fail the first k `.json.tmp` writes, then succeed", which is the needed "ENOSPC then space freed before the destructor retry". No ballast task is needed. `s3_fault_proxy.py` supports per-path, per-method `error500`/`error503`/`reset_before`/`reset_after`/`stall`/`truncate_body` with `nth`/`count` (`:28-44`). That covers "≥ 6 consecutive `.json` PUT failures" and "applied, response lost". Implementable as planned.

---

## Passes (checked and implementable as described)

- **Chained fetch handshake.** Pull's listener always sends caching_sha2 *fast-auth success* (`network_service.cpp:461-470`, `connection_context.cpp:249-252`), so a downstream PBS `fetch` or `mysqlbinlog` over cleartext authenticates without the RSA key that ticket 23 lacks. Position-mode PBS `fetch` sends only the `SET @source_binlog_checksum …` query (`easymysql/connection.cpp:214-223`), which is on the allowlist (`network_service.cpp:536-550`). GTID mode would send `SELECT @@GLOBAL.gtid_purged` (`collector_context.cpp:230-234`), which is not on the allowlist, so the catalog's restriction of `chained-fetch-from-pull-reproduces-upstream` to position mode is necessary and correct.
- **S3 fault proxy.** It provides every HTTP-level fault the catalog names. It sits in `s3`, so native `pbs↔s3` network faults still surface as real timeouts. It buffers the full request body before forwarding, so a PUT whose client gave up after sending the body can still land late (the `stored-state-never-regresses` stale-PUT replay remains reachable).
- **Process-kill-based crash properties** (`kill-anywhere-storage-reopens`, `stored-transactions-exactly-once-across-resumes`, `metadata-size-at-transaction-boundary`, `interrupted-purge-leaves-openable-storage`, `repeat-fetch-on-caught-up-storage-is-noop`, `s3-crash-leftovers-bounded` local half) need no node termination. The test-command SIGKILL plus the supervisor restart is enough, consistent with the catalog's assumption.
- **Multi-process races on one filesystem storage** run as subprocesses in `pbs`. No shared volume is needed (`other-process-startup-spares-live-writer`, `stale-index-commit-never-wedges-storage`, `second-writer-*` filesystem arm).
- **Pull-stream format oracles** (`pull-served-stream-self-consistent`, `dump-preamble-matches-request` minus the "oldest indexed file" label, `rs-unauthenticated-session-never-served`, `misaligned-dump-position-rejected-cleanly`) are fully observable from `workload` with a raw Python client, provided it sends the exact checksum `SET` strings.
- **`source-session-tls-when-configured`** is observable from SQL (`performance_schema.threads.CONNECTION_TYPE`). **`source-auth-survives-auth-cache-flush`** (TLS arm) needs only SQL and the kill driver. **`tagged-gtid-resume-exact`** is implementable on 8.4/9.7 timelines once the privilege is granted (C1).
- **`recovery-paths-exercised`** has a workable log-line fallback until the SDK lands, and it already omits rows whose workload hooks are absent.
- **`query-success-never-omits-middle-binlog`** can use the proxy's per-path GET rule (`match` on a single `.json`) instead of relying on an exact-length partition.

## Uncertainties

- The default `--connection-server-id` of `mysqlbinlog` 8.0/8.4/9.7 for non-blocking and `--stop-never` dumps, and whether the source evicts on server id 0, were not checked in MySQL source. C9 is therefore stated as "set it explicitly" rather than as a confirmed eviction.
- Whether an Antithesis-killed test command's child processes are killed with it (process group or cgroup) is not documented in the references read. C3(2) assumes they may survive.
- Whether older PBS tags (0.2.3, 0.3.1) build with the current dependency presets was not attempted.
- Whether Antithesis honours compose `ulimits`, `mem_limit` and `tmpfs: size=` remains the topology's open question. P8 gives fallbacks that do not need them.
- The CPU and simulated-time cost of re-reading the full source history via `mysqlbinlog` on every `anytime_` run under Antithesis is not measurable without a run (C10).
