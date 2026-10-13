---
sut_path: /Users/hrvoje/work/percona-binlog-server
commit: 99de68207c84f83e0d477ebe63d2f531facbb954
updated: 2026-10-13
external_references:
  - path: https://claude.ai/artifact/9om6fiPd64QzVXbEWEtDT1
    why: QA bug-ticket list for binlog_server (local snapshot antithesis/scratchbook/.tickets-snapshot.txt); open tickets used as leads, each validated against the code before a property was built on it
---

# Property Catalog: Percona Binlog Server (binlog_server)

## Summary

This catalog synthesises eleven property-discovery passes (`.discovery/props-01` … `props-11`) into 60 properties, and the evaluation gap-fill pass (`.discovery/gap-source-retention.md`, `.discovery/gap-restore-durability-s3.md`) added 7 more, for **67 properties**: 56 safety, 9 liveness and 2 reachability. By priority there are 12 P0, 37 P1 and 18 P2. The 109 draft properties were deduplicated into 29 merge groups, and 31 single-focus finds were kept as they were. None of the 7 gap-fill properties was merged into an existing one: each overlapping pair differs in invariant or trigger, so they are cross-referenced instead (see each entry's "Rationale / conflicts resolved"). Each property has one evidence file at `properties/<slug>.md`. "Found by" records which discovery focuses surfaced it; a property found by several focuses is a confidence signal.

The highest-value surface is the crash- and fault-consistency of multi-object storage updates. That covers rotation, checkpoints, purge, startup recovery and the final flush that runs in `~storage` after success has already been decided. Validated tickets 15, 18 and 24 and the code-admitted purge wedge (N3) are all violations here. Second is pull's replication source. It runs on a single io thread with synchronous storage reads, and it skips the file-switch ROTATE when the next file is still magic-only (N5). Third is source-side change: PBS records no source identity (ticket 25), GTID resume drops rotations (ticket 14), and auth fails after a cache flush (ticket 23).

No Antithesis instrumentation exists in the codebase (`existing-assertions.md`), so every SUT-side assertion named below is new. Existing C `assert`s that would be promoted are marked "partial".

The "Investigate Open Questions" pass has been run (`.discovery/investigation/batch-01..10`) and its results are synced here. Each evidence file's Investigation Log backs the tags. After the pass, 10 properties have no open questions, 35 have only `(needs human input)` questions, and 15 still carry at least one `(partial: …)` question (one of them, `pull-chain-converges-after-heal`, also has one new untagged question). No property was fully invalidated. The lock-starvation half of `stalled-dump-client-isolated` was invalidated. No priority letter changed: `stale-index-commit-never-wedges-storage` stays P1 but now rests on the concurrent-purges variant, because the ticket 9 purge-beside-writer variant is out of contract (README:473). `search-by-timestamp-includes-all-early-files` stays at P2, the lowest level, as the investigation recommended. Several properties are now **expected to fail today**: `kill-anywhere-storage-reopens` (ticket 15), `gtid-resume-preserves-source-rotations` (ticket 14), `inprocess-reconnect-resumes-without-dup-or-gap` and `resume-continuity-guard-never-trips` (EOF path), `sender-file-switch-emits-artificial-rotate` (N5), `blocking-dump-ahead-of-flushed-position-eventually-served` (cases 1 and 2), `rewrite-pitr-by-datetime-matches-source`, `ctr-keystream-not-reused-across-truncation` (rewrite mode), `source-crash-restart-resume-is-faithful` (post-rotation kill), `stored-rotate-or-stop-is-last-event` (once the window is hit), the warning/retry checks of `interrupted-purge-leaves-openable-storage`, the large-event liveness half (ticket 10), and, added by the evaluation pass, `dump-session-ends-with-eof-or-err-packet` (every request past the flushed size closes silently, `network_service.cpp:308-313`). The recurring contract questions are consolidated under the file-level Open Questions.

The evaluation pass (`evaluation/synthesis.md`, four lenses) applied its refinements here: a **Runs in** column with deferred templates marked; quiet-window evaluation with harness-bounded stimuli for every time-bounded `Always`; a bounded `Always` per quiet period for `pull-chain-converges-after-heal`; the independent Python decryptor as the canonical encrypted-storage reader; SUT assertion messages that carry a timeline tag or a per-shape cause, because Antithesis tallies a message across the whole run; "Runs in" requirements for `Sometimes` anchors that no template reaches yet; and two priority swaps (`source-crash-restart-resume-is-faithful` P1 → P0, `source-swap-never-silently-accepted` P0 → P1). The seven gaps (G1-G7) were filled by targeted discovery: `source-rewind-never-silently-continued` (G1), `stored-binlogs-replay-to-source-state` (G2), `checkpoint-bounds-unpersisted-data` (G3, split out of `collector-memory-bounded-by-checkpoint` with refinement R27 applied), `fetch-converges-after-faults-stop` (G4), `source-purge-past-resume-point-fails-closed` (G5), `s3-whole-object-put-publishes-intended-bytes` (G6) and `pull-retention-cycle-keeps-serving-survivors` (G7). `fetch-converges-after-faults-stop` is expected to fail today wherever ticket 15, ticket 18 (position mode) or lead (b) is hit. Five biases (B1-B5) wait for a human decision. Gaps and biases are both listed in `evaluation/synthesis.md`. Tickets and leads that deliberately have no property are listed under "Uncataloged tickets and leads".

## New defect leads from investigation (not reproduced)

These came out of the open-question pass. Each is a code trace, not a reproduction, and each is ready to draft as a ticket. The lettered references (lead (a) … (g)) are used in the property entries.

- **(a) Graceful source shutdown or `KILL QUERY` sends EOF mid-transaction, and pull keeps the partial buffer** (batches 4, 7). mysqld's dump thread sends EOF on `SHUTDOWN`/SIGTERM or `KILL QUERY` and checks `killed` before every event (`rpl_binlog_sender.cc:366-384`, `mysqld.cc:2323-2368`). Pull's case (3) at `collector_context.cpp:184-187` returns without discarding the incomplete transaction or flushing, then reconnects with the buffer intact. Outcomes: plain GTID mode re-receives the buffered transactions as duplicates and fail-stops on the position check (recovers on restart); rewrite mode stores them twice silently; position mode with a partial transaction buffered fail-stops ("transaction group must start"). The mysqld restart wrapper already issues `SHUTDOWN`, so this is in the planned fault set. Slugs: `inprocess-reconnect-resumes-without-dup-or-gap`, `pull-chain-converges-after-heal`, `resume-continuity-guard-never-trips`.
- **(b) Source SIGKILL right after a rotation reaches the ticket 6 path** (batch 5). In position mode N holds only FDE + PREVIOUS_GTIDS. On resume at `(N, 4)` the source re-sends them, then a fake ROTATE(N+1); `close_binlog` drops the buffered preamble but keeps its timestamps. A debug build aborts at `storage.cpp:138` on every retry (a wedge); a release build gives N+1 a wrong timestamp range. N stays magic-only forever. Slugs: `source-crash-restart-resume-is-faithful`, `binlog-metadata-matches-stored-bytes`.
- **(c) Rewrite-mode wall-clock stamps break `mysqlbinlog --start/--stop-datetime` PITR under ≥ 1 s lag** (batch 8). Generated FDE, PREVIOUS_GTIDS and closing ROTATE carry PBS `now()`; `--stop-datetime` exempts no event type (8.0/8.4/9.6/9.7.1), so it stops early and drops transactions, and `--start-datetime` returns extra, older ones. Slugs: `rewrite-pitr-by-datetime-matches-source` (also `search-by-timestamp-includes-all-early-files`).
- **(d) Rewrite-mode CTR keystream reuse after crash truncation** (batch 8). After a kill between the data append and the metadata save, the first transaction after reconnect is encrypted at the truncation point in the reused file under the same key and IV: about 50 bytes (the ROTATE's length) of GTID-event metadata overlap with small checkpoints. Old ciphertext is observable in leaked local S3 buffer copies. Slug: `ctr-keystream-not-reused-across-truncation`.
- **(e) Query modes rewrite `metadata.json` to stamp encryption** (batch 3). `storage_core.cpp:174-178` calls `save_metadata()` in every construction mode when the config adds encryption to an unencrypted storage. Commit 3e812f2 gated the neighbouring fresh-storage write on `streaming` but not this one, which looks like an oversight. Slugs: `query-modes-never-write-storage` (also `other-process-startup-spares-live-writer`).
- **(f) A caught-up dump client that stops reading is never reclaimed** (batch 6). At EOF a blocking session only polls storage every 500 ms and never touches the socket; there are no heartbeats or keepalive, and `write_timeout` applies only to a pending write. The session holds its fd and buffers until enough new data fills the TCP buffers. Slugs: `stale-sessions-reclaimed-within-bound`, `stalled-dump-client-isolated`.
- **(g) The pbs-0.3.1 binary rewrites the tail `<binlog>.json` without the encryption envelope** (batch 7, downgrade only). Running the old binary on an encrypted storage destroys the tail file's key. Relevant only if downgrade is supported. Slug: `storage-format-version-mismatch-handled`.

**P0 properties:** `stored-binlog-byte-prefix-of-source`, `metadata-size-at-transaction-boundary`, `failed-flush-never-reappends-data`, `kill-anywhere-storage-reopens`, `graceful-exit-leaves-flushed-storage`, `second-writer-on-storage-fails-closed`, `stored-transactions-exactly-once-across-resumes`, `sender-file-switch-emits-artificial-rotate`, `pull-served-stream-self-consistent`, `interrupted-purge-leaves-openable-storage`, `stored-rotate-or-stop-is-last-event`, `source-crash-restart-resume-is-faithful` (promoted by the evaluation pass; `source-swap-never-silently-accepted` moved to P1).

## Overview

Fault abbreviations: **kill** = workload-driven SIGKILL of a PBS subprocess (no node termination needed); **net** = Antithesis network faults; **node-term** = Antithesis node termination (disabled by default); **clock** = clock faults; **custom** = harness-provided faults or fixtures that Antithesis does not inject natively (`s3_fault_proxy`, `fsfault`, size-limited volume, fd/memory limits, source swap/promotion/upgrade actions, legacy fixtures).

**Runs in** names the test template(s) from `deployment-topology.md` that exercise the property: `fetch_fs`, `pull_fs`, `s3`, `contention`. **Deferred** marks a property, or a variant, that needs a template outside the minimal topology: `failover` (replica + VIP), `upgrade` (binary swap over one datadir), `old-binary` (legacy fixtures and previous-release binaries) or `client-matrix` (several `mysqlbinlog` versions in `workload`). Deferred properties stay in the catalog but are not expected to report in the first launches. "Quiet windows" means the bounded check is evaluated only inside an `ANTITHESIS_STOP_FAULTS` window that also pauses the harness's own fault drivers (see Assumptions). Notes such as "config-mutation driver" or "retention-cycle driver" name a workload step the topology regeneration adds.

| Slug | Category | Type | Assertion | Priority | Runs in | Fault requirements | Open Qs | Related tickets / leads |
|---|---|---|---|---|---|---|---|---|
| `stored-binlog-byte-prefix-of-source` | Storage integrity | Safety | Always (workload) + ALWAYS (SUT append offset) | P0 | `fetch_fs`, `pull_fs`, `s3` | kill, net, custom S3/fs shim | 2 | tickets 18, 24, 25 |
| `metadata-size-at-transaction-boundary` | Storage integrity | Safety | Always | P0 | `fetch_fs`, `pull_fs`, `s3` | kill, net | 0 | SG3 |
| `failed-flush-never-reappends-data` | Storage integrity | Safety | Unreachable (SUT) + Always | P0 | `s3` (proxy `.json` rule at low probability), `fetch_fs` (fsfault) | net (6+ consecutive failures) or custom S3/ENOSPC shim | 3 | ticket 18 |
| `kill-anywhere-storage-reopens` | Storage integrity | Safety | Always (3 triggers) + Unreachable per validator | P0 | `fetch_fs`, `pull_fs`, `s3` | kill, net, custom fsfault | 4 | tickets 15, 19; fails today (ticket 15 MTR) |
| `graceful-exit-leaves-flushed-storage` | Storage integrity | Safety | Always (3 checks) + Unreachable (op layer) | P0 | `fetch_fs`, `pull_fs`, `s3` | net/disk (6+ PUT failures) or custom S3/disk shim, signals | 4 | N8 |
| `stored-state-never-regresses` | Storage integrity | Safety | Always + SOMETIMES (SDK-retry guard) | P1 | `fetch_fs`, `pull_fs`, `s3` (stale-PUT variant: `s3` with packet hold on pbs↔s3) | kill, S3 latency/packet hold | 4 | tickets 9, 24; N1 (out of contract) |
| `binlog-metadata-matches-stored-bytes` | Storage integrity | Safety | Always | P1 | `fetch_fs`, `pull_fs`, `s3` | kill, net, source SIGKILL (wrapper); clock optional | 0 | ticket 6; lead (b) |
| `checkpoint-bounds-unpersisted-data` | Storage integrity | Safety | ALWAYS (SUT backlog) + SOMETIMES (anchor) + Always (bounded, quiet windows) | P1 | `pull_fs`, `s3` (pull-like), quiet windows; SUT `ALWAYS` in all templates (checkpoint-RPO driver) | kill, net; checkpointing drawn on | 2 | SG8; ticket 2 (fixed) |
| `s3-whole-object-put-publishes-intended-bytes` | Storage integrity | Safety | ALWAYS (SUT, 2) + REACHABLE + SOMETIMES (SUT) + Always + Sometimes | P2 | `s3` (both arms; S3 tail-recovery driver) | net mid-body on pbs↔s3, kill; custom S3 proxy (5xx, `truncate_body`) | 2 | ticket 18 (S3 data-upload variant); A10 excluded |
| `stored-binlogs-replay-to-source-state` | Storage integrity | Safety | Always (per storage kind + strict variant) + Sometimes (vacuity) | P1 | `fetch_fs`, `pull_fs`, `s3` (replay-restore `finally_` driver; scratch `mysqld` in `runtime-base`) | kill, net; custom S3 proxy optional | 2 | ticket 14; lead (a) |
| `second-writer-on-storage-fails-closed` | Multi-process | Safety | Always | P0 | `contention` | none (workload); hang/partition for zombie | 1 | ticket 24 |
| `other-process-startup-spares-live-writer` | Multi-process | Safety | Always + Unreachable/Sometimes (SUT) | P1 | `contention` | none (workload); pause | 3 | N1, N2 (.tmp variant filesystem-only) |
| `stale-index-commit-never-wedges-storage` | Multi-process | Safety | Always | P1 (rests on concurrent purges) | `contention` (purge-pair-without-writer step needed; anchor deferred until added) | none (workload); pause | 4 | ticket 9 (out of contract) |
| `distinct-storages-shared-server-id-stay-consistent` | Multi-process | Safety | Always + Sometimes (eviction) | P2 | `contention` (sibling-storage writer step needed; **Deferred** until added) | none (topology); supervisor restart | 1 | tickets 14, 18 |
| `stored-transactions-exactly-once-across-resumes` | Resume & replay | Safety | Always (per mode) + ALWAYS (SUT receive + block dup GTID) | P0 | `fetch_fs`, `pull_fs`, `s3` | kill, net | 1 | SG1; ticket 14 exception |
| `inprocess-reconnect-resumes-without-dup-or-gap` | Resume & replay | Safety | ALWAYS (SUT, 3) + REACHABLE + Always | P1 | `pull_fs`, `s3` (pull-like) | net, source SHUTDOWN / KILL QUERY / KILL CONNECTION | 0 | lead (a); fails today (EOF path) |
| `gtid-resume-preserves-source-rotations` | Resume & replay | Safety | Always + ALWAYS/UNREACHABLE (SUT, gated on no source kill) | P1 | `fetch_fs`, `pull_fs`, `s3` | net or kill | 2 | ticket 14; fails today |
| `resume-continuity-guard-never-trips` | Resume & replay | Safety | Unreachable | P1 | `fetch_fs`, `pull_fs`, `s3` | kill, net | 1 | lead (a); fires today after graceful SHUTDOWN |
| `repeat-fetch-on-caught-up-storage-is-noop` | Resume & replay | Safety | Always | P2 | `fetch_fs` | kill, net | 0 | — |
| `sender-file-switch-emits-artificial-rotate` | Pull serving | Safety | Always (workload + SUT) | P0 | `pull_fs` (checkpointed configs weighted) | none (pause/CPU); source restart/kill for A2 | 2 | N5; fails today |
| `pull-served-stream-self-consistent` | Pull serving | Safety | Always | P0 | `pull_fs`, `s3` (pull-like) | kill, net, custom shim | 1 | tickets 18, 24; N1 |
| `pull-chain-prefix-consistency` | Pull serving | Safety | Always + Sometimes (N12 resume) | P1 | `pull_fs`, `s3` (pull-like) | net (3 links), kill, custom metadata-PUT fault | 4 | ticket 18; N12 |
| `chained-fetch-from-pull-reproduces-upstream` | Pull serving | Safety | Always | P1 | `pull_fs` | kill/pause, net; downstream pull for N5 | 1 | N5; tickets 21, 22, 26 |
| `dump-session-ends-with-eof-or-err-packet` | Pull serving | Safety | Unreachable (SUT; log-line fallback) + Sometimes (any ERR) | P1 | `pull_fs` (purge case needs the retention-cycle driver) | kill, net | 1 | N12; ticket 26 moved to liveness; fails today (any request past the flushed size) |
| `blocking-dump-before-first-flush-eventually-served` | Pull serving | Liveness | Sometimes (per state) | P1 | `pull_fs` | source flap, kill, pause; workload flush trigger | 0 | PBS-91 |
| `blocking-dump-ahead-of-flushed-position-eventually-served` | Pull serving | Liveness | Sometimes (per case 1/2; 3 conditional) | P1 | `pull_fs` | none; restarts help | 2 | ticket 26 / PBS-96; N12; fails today (cases 1/2) |
| `misaligned-dump-position-rejected-cleanly` | Pull serving | Safety | Always + ALWAYS (SUT event_size) | P1 | `pull_fs` (natural variant); direct hostile driver in its own template | none (workload) | 2 | ticket 7 |
| `dump-preamble-matches-request` | Pull serving | Safety | Always | P2 | `pull_fs` (after-purge anchor needs the retention-cycle driver) | none; kill and stop/purge/restart help | 1 | tickets 21, 22, 26 |
| `large-event-served-or-rejected-cleanly` | Pull serving | Safety | Always + Unreachable (SUT) | P2 | `pull_fs` (low-probability large-event knob) | none; memory limit | 0 | ticket 10; fails today (≥ 16,777,211 B) |
| `rs-unauthenticated-session-never-served` | Pull serving | Safety | Always | P2 | `pull_fs` (listener-abuse driver) | net (resets) | 1 | ticket 20 |
| `pull-retention-cycle-keeps-serving-survivors` | Pull serving | Safety | Always (2) + Sometimes (liveness, empty-name) | P1 | `pull_fs` (retention-cycle driver) | SIGTERM + purge (workload); net, kill, fsfault optional | 1 | — |
| `interrupted-purge-leaves-openable-storage` | Query & purge | Safety | Always (11 checks) | P0 | `fetch_fs`, `s3` | net, kill of purge, custom fsfault | 5 | N3; warning/retry fail today |
| `query-success-never-omits-middle-binlog` | Query & purge | Safety | Always + Unreachable (SUT) | P1 | `fetch_fs`, `pull_fs`, `s3` | net to S3, custom GET shim | 4 | W6 |
| `cli-json-envelope-and-exit-code-agree` | Query & purge | Safety | Always + Unreachable | P1 | `fetch_fs`, `pull_fs`, `s3` | workload concurrency, kill, net; index-damage step | 1 | N4; ticket 13 |
| `query-modes-never-write-storage` | Query & purge | Safety | ALWAYS (SUT) + Always + Sometimes (vacuity) | P2 | `fetch_fs` (config-mutation driver) | none (encryption-added query config) | 1 | lead (e) |
| `search-by-timestamp-includes-all-early-files` | Query & purge | Safety | Always | P2 | `fetch_fs` (clock faults) | clock (realistic variant) | 2 | N11 |
| `encrypted-storage-decrypts-to-source` | Encryption | Safety | Always + ALWAYS (SUT offset, gated) + SOMETIMES twin | P1 | `fetch_fs`, `pull_fs`, `s3` | kill, custom shim; misuse phase separate | 1 | ticket 18 |
| `encrypted-storage-no-plaintext-at-rest` | Encryption | Safety | Always + ALWAYS (SUT new-file envelope) | P1 | `fetch_fs`, `pull_fs` (config-mutation driver) | kill/restart + config variation | 1 | N9 |
| `mixed-encryption-storage-served-correctly` | Encryption | Safety | Always | P1 | `pull_fs` (config-mutation driver) | kill + config variation, custom S3 fault | 1 | N9 |
| `ctr-keystream-not-reused-across-truncation` | Encryption | Safety | Always | P2 | `fetch_fs`, `s3` (rewrite + encryption) | kill; rewrite small checkpoint; source RESET | 1 | ticket 25; N10; lead (d); fails today (rewrite) |
| `stored-rotate-or-stop-is-last-event` | Rewrite | Safety | Always + AlwaysOrUnreachable (SUT) | P0 | `fetch_fs`, `pull_fs`, `s3` (rewrite) | net to S3 (no kill) or kill | 3 | new lead; fails today once window hit |
| `rewrite-binlog-chain-valid-across-crashes` | Rewrite | Safety | Always | P1 | `fetch_fs`, `pull_fs`, `s3`; upgrade variant **Deferred** (`upgrade`) | kill, net; custom source upgrade | 1 | — |
| `rewrite-storage-transaction-sequence-matches-source` | Rewrite | Safety | Always + ALWAYS (SUT non-GTID size) | P1 | `fetch_fs`, `pull_fs`, `s3` | kill, partition | 0 | — |
| `rewrite-pitr-by-datetime-matches-source` | Rewrite | Safety | Always (stop + start direction) | P1 | `fetch_fs` | partition or kill; clock optional | 1 | N11; lead (c); fails today |
| `rewrite-toggle-between-runs-keeps-storage-resumable` | Rewrite | Safety | Always + Unreachable (SUT) | P2 | `fetch_fs` (config-mutation driver) | none (config variation) | 2 | N11 |
| `rewrite-checksum-none-fail-stop` | Rewrite | Safety | Always | P2 | `fetch_fs` (checksum-toggle action) | kill; checksum toggle | 0 | — |
| `source-swap-never-silently-accepted` | Source & version | Safety | Always | P1 | **Deferred** (`failover`); RESET variant in `fetch_fs` behind a flag | net; custom swap action + proxy | 1 | ticket 25 |
| `source-rewind-never-silently-continued` | Source & version | Safety | Always + Sometimes (vacuity, 2) | P1 | `fetch_fs`, `pull_fs` behind a flag (rewind-source driver) | custom datadir snapshot/restore in the mysqld wrapper; kill, net optional | 1 | ticket 25 (not fixed by a `server_uuid` check) |
| `source-purge-past-resume-point-fails-closed` | Source & version | Safety | Always (3) + Sometimes (per mode) + ALWAYS_OR_UNREACHABLE (SUT) | P2 | `fetch_fs`, `pull_fs` (source-purge driver, flagged) | workload FLUSH/PURGE on source, kill/SIGTERM, net | 2 | ticket 18 (CR_1236 output) |
| `source-crash-restart-resume-is-faithful` | Source & version | Safety | Always | P0 | `fetch_fs`, `pull_fs`, `s3` | source SIGKILL via wrapper (no node-term) | 0 | ticket 6; lead (b); fails today (post-rotation kill) |
| `gtid-failover-no-lost-transaction-merge` | Source & version | Safety | Always (both modes) | P1 | **Deferred** (`failover`) | partition; custom promotion + proxy | 2 | — |
| `gtid-failover-resume-progresses` | Source & version | Liveness | Always (rewrite, bounded per quiet period) + Sometimes (per mode, vacuity) + Always (non-rewrite) | P2 | **Deferred** (`failover`) | partition, hang; node-term optional; custom promotion | 1 | MYSQL_RPL_SKIP_HEARTBEAT loss |
| `source-upgrade-resume-continuity` | Source & version | Safety | Always + AlwaysOrUnreachable (SUT) | P1 | **Deferred** (`upgrade`) | kill, net; custom binary swap | 1 | ticket 14 |
| `tagged-gtid-resume-exact` | Source & version | Safety | Always | P1 | `fetch_fs`, `pull_fs` (8.4/9.7 launches) | kill, partition | 0 | — |
| `source-auth-survives-auth-cache-flush` | Source & version | Liveness | Sometimes + Always | P1 | `fetch_fs`, `pull_fs` | source restart (kill_mysqld driver) or FLUSH PRIVILEGES | 1 | ticket 23 |
| `source-session-tls-when-configured` | Source & version | Safety | Always | P2 | `fetch_fs`, `pull_fs` | partition, source restart (wrapper) | 1 | — |
| `storage-format-version-mismatch-handled` | Source & version | Safety | Always + Unreachable (SUT) | P2 | **Deferred** (`old-binary`: legacy fixtures + previous-release binaries) | kill; custom fixture/old binary/S3 proxy | 3 | lead (g) |
| `pull-client-version-matrix` | Source & version | Reachability | Sometimes (per client version) | P2 | `pull_fs` (shipped client only); cross-version **Deferred** (`client-matrix`) | none | 1 | ticket 20 |
| `pull-chain-converges-after-heal` | Liveness & resource | Liveness | Always (bounded, per quiet period) + Sometimes (vacuity) + Always (N7) + Unreachable (SUT EOF) | P1 | `pull_fs`, `s3` (pull-like), quiet windows | net, throttling (exploration), quiet period; source SHUTDOWN / KILL QUERY | 6 | N7; ticket 17; lead (a) |
| `fetch-converges-after-faults-stop` | Liveness & resource | Liveness | Always (bounded attempts) + Sometimes (vacuity) + Unreachable (harness guard) | P1 | `fetch_fs`, `s3` (fetch-like), via `eventually_converge.py` | none during check; preceding phase: kill, net, source SIGKILL, custom S3 proxy / fsfault | 3 | tickets 15, 18; lead (b); fails today where those fire |
| `sigterm-honoured-within-bound` | Liveness & resource | Liveness | Always (bounded, quiet windows) + Sometimes (over-bound measurement) | P1 | `pull_fs`, `s3`, `fetch_fs` (startup half), quiet windows | harness-bounded S3 stall / SIGSTOP in quiet windows; native latency/throttling for exploration only; signals | 5 | ticket 17 |
| `stalled-dump-client-isolated` | Liveness & resource | Liveness | Always (bounded, quiet windows) | P1 | `pull_fs`, quiet windows | harness reader that stops calling recv(); harness-bounded S3 stall; native faults for exploration only | 1 | lead (f); lock-starvation part invalidated |
| `s3-crash-leftovers-bounded` | Liveness & resource | Safety | Always | P1 | `s3` | kill, partition to S3 | 2 | N10 |
| `listener-survives-fd-exhaustion` | Liveness & resource | Safety | Unreachable (SUT) + Always | P1 | `pull_fs` (fd-limit timeline variant) | custom fd limit (ulimit -n 256) + connection flood | 1 | N6 |
| `collector-memory-bounded-by-checkpoint` | Liveness & resource | Safety | ALWAYS (SUT) + Always | P2 | `fetch_fs`, `pull_fs` (memory-limit variant) | custom memory limit | 2 | ticket 2 (fixed) |
| `stale-sessions-reclaimed-within-bound` | Liveness & resource | Liveness | Always (bounded, quiet windows, measured on the `pbs` side) | P2 | `pull_fs`, quiet windows | harness-bounded silent/vanishing clients; native client faults for exploration only | 3 | N6; lead (f) |
| `recovery-paths-exercised` | Reachability | Reachability | Reachable/Sometimes per branch + one Unreachable | P1 | all | union of the above | 1 | — |

## Storage integrity under crashes and storage faults

A single writer (fetch or pull) updating a multi-object storage: data appends, `.json` metadata, `binlog.index`, startup recovery and the final flush in `~storage`. These properties check that whatever a kill, network fault or disk fault interrupts, the storage stays a source prefix on transaction boundaries, never contains a re-appended block, never regresses, and opens on the next run.

### stored-binlog-byte-prefix-of-source — Stored binlogs are byte prefixes of the source's binlogs

| | |
|---|---|
| **Type** | Safety |
| **Property** | For non-rewrite storages, every indexed binlog's bytes `[0, metadata.size)` equal the source's same-named binlog as sent, and `binlog.index` names a contiguous run of the source's binlogs. |
| **Invariant** | Workload `Always("stored binlog bytes are a prefix of the source binlog")`, checked after every PBS exit or kill and at quiescent points. It compares stored bytes with `binlog_as_sent(source)` (QA `storage_holds_prefixes`, made index-suffix-aware). Encrypted storages are decrypted with the independent Python decryptor, the canonical encrypted reader. A pull readback is not used here: pull always builds a streaming storage and runs the collector (`pull_operation.cpp:108-153`), so it would run recovery on, and append to, the storage under test. `Always` fits because any divergent byte is a silent backup corruption. SUT-side (missing): `ALWAYS(stream position == record.size before append)` in `storage_core::write_event_block`, and the `storage_core.cpp:707` assert promoted to `ALWAYS` (partial today). |
| **Antithesis Angle** | Kills at any instruction during checkpoints, reconnects and rotations. Network faults mid-transaction. S3 5xx or stall on data and `.json` PUTs. Source restarts and rotations while PBS is disconnected. The append-offset assertion fires the moment bytes land at a wrong offset, before any oracle runs. |
| **Why It Matters** | This is the core promise of a binlog backup (README:3, 711). Tickets 18, 24 and 25 all produce non-prefix stores. N1 (purge-time tail truncation) is no longer counted as a violation here: purge beside a writer is out of contract (README:473), so storages that saw a concurrent purge are skipped by the oracle. It is the always-on, any-time counterpart of the convergence check in `stored-transactions-exactly-once-across-resumes`. |

**Priority:** P0 · **Confidence:** high (oracle exists in the QA harness; violations are validated tickets) · **Found by:** focus 1 · **Fault requirements:** process kill (workload SIGKILL; node termination not needed), network faults; **custom:** S3 fault proxy / `fsfault` for the failed-write paths.

**Evidence:** `properties/stored-binlog-byte-prefix-of-source.md`

**Open Questions:**

- Is byte-identity to the source an official guarantee or an MTR convention? `(needs human input)`
- Are two concurrent writers on one storage in contract? `(needs human input)`

### metadata-size-at-transaction-boundary — Persisted size always ends on a transaction boundary

| | |
|---|---|
| **Type** | Safety |
| **Property** | Every binlog's `.json` `size` ends exactly at an event boundary that is the end of a complete transaction (after a preamble), a terminal ROTATE/STOP, or the magic (last indexed binlog only). |
| **Invariant** | Workload `Always("persisted binlog size ends on a transaction boundary")` after each PBS exit or kill: parse `[0, size)` and check that it ends exactly at `size` on an allowed boundary. The oracle is tight: a preamble-only size (magic + FDE + PREVIOUS_GTIDS, no transaction) is not an allowed end, and size 4 (magic only) is allowed only on the last binlog. SUT-side (missing): `ALWAYS(flushed block ends on reader boundary)` in the facade; the `storage.cpp:278` assert becomes an `ALWAYS` (partial). `SOMETIMES(discarded partial transaction on disconnect)` and `SOMETIMES(checkpoint flush left an incomplete transaction buffered)` are exploration anchors. `Always` because a single persisted size off a transaction boundary is a violation whenever the check runs; the `SOMETIMES` anchors are exploration targets, not guarantees. |
| **Antithesis Angle** | Checkpoints that fire mid-burst, disconnects in the middle of multi-event transactions, and kills between the data append and the metadata PUT. Thread pausing around `flush_event_buffer_internal`. |
| **Why It Matters** | A tail that is not on a boundary resumes mid-transaction. The next run then fails the reader FSM or silently produces a torn transaction (README:373/715/721). Investigation found boundary tracking length-based and sound today, so this is a regression guard: a transaction-shape misclassification would surface as an exception exit, not a torn size. |

**Priority:** P0 · **Confidence:** high (holds today; regression guard) · **Found by:** focus 1 · **Fault requirements:** process kill (workload), network faults; `checkpoint_size`/`checkpoint_interval` enabled in the config.

**Evidence:** `properties/metadata-size-at-transaction-boundary.md`

**Open Questions:**

- None

### failed-flush-never-reappends-data — A failed flush is never re-appended (ticket 18)

| | |
|---|---|
| **Type** | Safety |
| **Property** | After `write_event_block` fails at any point after its data append succeeded, no later flush (in particular the one in `~storage`) appends the same buffered bytes again, and no stored binlog contains a transaction twice. |
| **Invariant** | Primary, SUT-side (missing): `UNREACHABLE("write_event_block re-appends a block whose data append already succeeded")`. It is driven by a flag set when `save_binlog_metadata` throws after `size +=` (`storage_core.cpp:307-321`) and checked on entry to `storage::flush_event_buffer_internal`. Twin `SOMETIMES(metadata save failed after a successful data append)` marks the window. A cheap second detector runs at open: `ALWAYS(tail's last event header position == metadata size)` (data is never parsed today, `:785`). Workload `Always("no transaction appears twice in stored bytes")` (GTID scan) and `Always("stored binlog header walk is contiguous from offset 4 to metadata size")` (source-free). `Unreachable` fits because a re-append after a failure is never valid. |
| **Antithesis Angle** | A fault on the metadata PUT/rename only, after the data append succeeded. Triggers: a PBS↔S3 partition that starts after the data PUT and heals before `~storage`; an ambiguous metadata PUT (applied, response lost); an S3 5xx on `*.json`; ENOSPC on `.json.tmp` followed by space freed (by a workload ballast task) before the destructor retry. N2 (a concurrent purge deleting `.json.tmp`) and a concurrent purge freeing the space are out of contract (README:473) and excluded. Mixing fixed-width and variable-size transactions decides which outcome follows. |
| **Why It Matters** | Ticket 18 is validated (`storage.cpp:68-77, 286/296`, `storage_core.cpp:310/321`). The duplicate is permanent because metadata agrees with the doubled data. Downstream outcomes: a wedge (position mode, CR_1236); a silent duplicate, possibly with exit 0 (GTID mode when no new transaction lands in N, rewrite mode); and a silent duplicate plus skipped source bytes (position mode with uniform-size transactions). Pull clients replay the duplicate. |

**Priority:** P0 · **Confidence:** high (mechanism and ticket repro); medium for the position-mode silent-skip and GTID exit-0 outcomes · **Found by:** focus 3, 5, 9, 11 (focus 1 via `source-transaction-stored-exactly-once`) · **Fault requirements:** network faults on PBS↔S3 (native, if CRT retries exhaust); **custom:** `s3_fault_proxy` 5xx on `.json` (likely needed: about 6 consecutive failures exhaust CRT retries), `fsfault` or a size-limited volume for ENOSPC. Node termination is not needed.

**Rationale / conflicts resolved:** Focus 9 assumed that in position mode the duplicate always wedges with CR_1236. The wildcard pass (focus 11) showed that with uniform-size transactions the offset S+2n can be a valid source boundary, so resume silently skips source bytes `[S+n, S+2n)` while every continuity check passes. The catalog therefore does not count on a detectable wedge, and makes the root-cause `UNREACHABLE` the primary assertion rather than an end-of-run oracle. The wildcard's separate property (`position-resume-after-duplicate-flush-never-skips-source`, P1) is subsumed here: its header-walk oracle and fixed-width workload requirement are kept. ENOSPC (focus 5) uses the same code path and is kept as a trigger variant. Focus 11 also notes that in plain GTID mode a new transaction in N fails loudly ("unexpected next event position"). Merged priority is P0 (focus 3, 5 and 9 rated P0; focus 11 rated P1).

**Evaluation refinements (2026-10-13):** With the `s3_fault_proxy` 5xx rule on `*.json` active, ticket 18 reproduces in almost every `s3` timeline that runs it, which `s3_fault_sweep.py` already shows deterministically. Until ticket 18 is fixed, the harness applies that rule at low probability per timeline. The natively triggered and combined variants then get the exploration budget: a partition that starts after the data PUT, an ambiguous metadata PUT, ENOSPC freed before the destructor retry, and the fixed-width position-mode silent skip. The rest of the `s3` template is also not masked by a constant ticket-18 firing.

**Evidence:** `properties/failed-flush-never-reappends-data.md`

**Open Questions:**

- What are the S3 CRT retry count, backoff and timeouts? `(partial: default ClientConfiguration, no overrides; QA shows 1 failure retried, 6 consecutive exhaust; exact values not in repo)`
- Which ticket 18 fix direction is intended (no destructor re-flush, or re-read the stored size)? `(needs human input)`
- Are size-limited volumes honoured in the Antithesis PBS container (fallback `fsfault.c`)? `(needs human input)`

### kill-anywhere-storage-reopens — A writer killed or failed at any point leaves a storage the next run opens

| | |
|---|---|
| **Type** | Safety |
| **Property** | After a fetch or pull process is killed at an arbitrary moment, including during its own startup recovery, or exits because of a storage-backend error, the next streaming start on the same storage does not fail with a storage-validation error and goes on to converge. |
| **Invariant** | Three workload `Always` checks with distinct messages, so triage separates the triggers: "restart after writer kill opened storage", "restart after backend-error exit opened storage", and "restart after kill during startup recovery opened and converged". Each check counts a non-zero exit with a validator message as a violation, while source-side errors are not. SUT-side (all missing): one `UNREACHABLE` per validator raise site, gated on `construction_mode_ != querying_only` (`storage_core.cpp:196, 775, 780, 939, 970, 975, 994`), with this property owning those sites. Antithesis tallies an assertion by its message across every timeline of a run, so the validator assertions use one message per leftover shape, not one per site: "unindexed data object of size 4 (interrupted rotation)", "metadata or data object without an index entry (interrupted purge)" and "tail data larger or smaller than its metadata size (writer overlap or truncation)". Ticket 15 and N3 then keep only their own messages red, and a new route into a validator stays visible. Sites that `contention` timelines can reach (tickets 9 and 24, N1) carry the harness timeline tag in the message. The assert at `:707` is promoted to `ALWAYS(open_stream_offset == flushed position)` (partial today). `ALWAYS(metadata size ≥ 4)` sits before the truncation at `:979`. `REACHABLE("binlog rotation failed after creating the data object")` carries the failing step in details. `Always` because every restart must open the storage, and `UNREACHABLE` because reaching a validator raise site in a writing mode is itself the defect. |
| **Antithesis Angle** | Kill windows inside rotation (`storage_core.cpp:680-703`: create/magic → `.json` → index), inside checkpoints and S3 `resize_object` (GET → truncate → PUT of the whole tail, `s3_storage_backend.cpp:765-808`), and crash loops that kill shortly after every start. Without any kill, a PBS↔S3 partition or an ambiguous PUT during rotation exhausts retries on the `.json` or index PUT; a lost-response upload of the magic object alone leaves an unindexed object and wedges the storage. Small `max_binlog_size`, frequent `FLUSH BINARY LOGS`, or rewrite mode with a tiny `file_size` make rotations frequent. |
| **Why It Matters** | Ticket 15 (validated): an interrupted rotation leaves an unindexed object, and every mode refuses the storage until manual repair. The QA-authored MTR test `storage_interrupted_binlog_creation.test` (commit 73cbe43) says the next start "must recover"; it fails today and no fix is in flight, so the kill-in-rotation checks are **expected to fail today**. README:373 claims errors leave the storage "in consistent state". A recovery step that is not idempotent turns one crash into permanent damage. |

**Priority:** P0 · **Confidence:** high · **Found by:** focus 1, 3 (four drafts merged) · **Fault requirements:** workload SIGKILL of PBS (node termination not needed), default network faults on PBS↔S3; **custom:** `fsfault` ENOSPC/EIO for the filesystem error variant. Ticket 19 is in scope only if a fault drops un-fsynced page cache.

**Rationale / conflicts resolved:** `storage-reopens-after-writer-crash` (focus 1) and `kill-anywhere-storage-reopens` (focus 3) are the same property. `backend-error-exit-leaves-openable-storage` and `startup-recovery-is-restartable` (focus 3) check the same invariant after different triggers, so they are kept as separately named workload assertions inside one property. Focus 3 rated startup recovery P1. The merged property is P0, because ticket 15 is a validated wedge. This property owns the validator raise sites. `second-writer-on-storage-fails-closed` and `recovery-paths-exercised` reference them instead of adding SDK calls there.

**Evidence:** `properties/kill-anywhere-storage-reopens.md`

**Open Questions:**

- Is the intended ticket 15 fix roll-back (delete leftovers) or roll-forward (index them)? `(needs human input)`
- Is an unopenable storage accepted fail-stop, and does "consistent state" (README:373) mean "openable by the next run"? `(partial: maintainer code treats improper shutdown as recoverable; purge comment accepts manual attention; rotation intent unstated)`
- Does any fault drop un-fsynced page cache (ticket 19)? `(needs human input)`
- Does the S3 stand-in give read-after-write consistency for an overwrite followed by a ranged GET? `(partial: Versity posix backend expected consistent, unverified)`

### graceful-exit-leaves-flushed-storage — fetch exit 0 and signal-initiated stops leave a fully flushed, consistent storage

| | |
|---|---|
| **Type** | Safety |
| **Property** | When fetch exits 0, the storage holds every source transaction committed before the fetch started. When pull exits 0 after a single SIGTERM/SIGINT, the storage needs no startup recovery. After any single signal at any instant, the storage opens with every binlog a source prefix ending on a transaction boundary. |
| **Invariant** | Three workload `Always` checks: (a) "fetch exit 0 implies complete copy up to the source state at start" (QA `check_complete_copy` relaxed to a pre-run source snapshot; magic-only tail exception). (b) "pull exit 0 after one signal leaves no tmp objects and object sizes equal metadata sizes". (c) "storage opens with prefix binlogs on a transaction boundary after any single signal". SUT-side (missing): `UNREACHABLE("final storage flush failed after the operation reported success")` in the operation layer, after the storage is destroyed and gated on `result == true`. This requires surfacing the swallowed failure. It is deliberately not placed unconditionally in the `storage.cpp:72-75` catch, where `recovery-paths-exercised` keeps a REACHABLE for the legitimate after-exception swallow. Also `SOMETIMES(signal arrived while collector inside write_event_block)`. `Always` because each graceful exit is a promise that must hold every time; `UNREACHABLE` because a swallowed final-flush failure after reported success is itself the defect. |
| **Antithesis Angle** | The final flush runs in `~storage` (`storage.cpp:68-77`, every exception swallowed) after the result is already decided. In fetch, `collector_context.cpp:184-187` returns at EOF without flushing and `fetch_operation.cpp:112-115` sets success. In pull, "successfully shut down" is logged first (`pull_operation.cpp:168-171`), and the last `storage_ptr` owner may be destroyed inside `~io_context` after the `signal_set` is gone. S3 partitions, latency or ENOSPC timed to the end of the run, and signals landing mid-checkpoint, mid-rotation or mid-metadata-PUT, hit this. |
| **Why It Matters** | cron-driven backups treat fetch exit 0 as "backup current". With the default config, the whole active binlog can be missing behind a success status (N8). README:713-721 promises that signals leave a consistent storage. |

**Priority:** P0 · **Confidence:** high for the swallowed-flush mechanism; medium for reachability under native faults · **Found by:** focus 1, 2, 5, 8 (four drafts merged) · **Fault requirements:** network partition or latency on PBS↔S3 long enough for 6+ consecutive PUT failures (CRT retries absorb fewer), or **custom** `s3_fault_proxy` / a full storage volume; workload-sent signals; process pause helps. Node termination is not needed.

**Rationale / conflicts resolved:** Focus 5 rated the fetch half P0. Focus 1 rated it P1 because reachability under the tenant's faults is medium. Focus 2 and 8 rated the signal halves P1. The merge resolves to P0. Exit 0 is the primary backup success signal, and the trigger is an ordinary network or disk fault at the end of a run, with no process faults needed. Focus 5's unconditional `UNREACHABLE` in the destructor catch conflicted with focus 3's `REACHABLE` at the same line. The merge moves the `UNREACHABLE` to the operation layer, gated on reported success.

**Evidence:** `properties/graceful-exit-leaves-flushed-storage.md`

**Open Questions:**

- Is "fetch exit 0 ⇒ complete copy at EOF" an intended contract, or does exit 0 only mean "reached EOF on the wire"? `(needs human input)`
- Is exit 0 after a failed final flush intended (N8)? `(needs human input)`
- Should a second SIGTERM during the final flush be ignored or escalate? `(partial: kill window confirmed only with a live COM_BINLOG_DUMP session; intent is a maintainer decision)`
- Should the workload perform source identity changes (ticket 25) in this property's scenarios? `(needs human input)`

### stored-state-never-regresses — Persisted progress and listed prefixes never regress

| | |
|---|---|
| **Type** | Safety |
| **Property** | Once a binlog's `(size, added_gtids)` and bytes `[0, size)` have been observed, directly or through `list`, later observations never show a smaller size, fewer GTIDs, changed bytes in `[0, size)`, or a data object shorter than its `.json` size. The index changes only by appending at the tail or by a workload-issued purge removing a prefix. |
| **Invariant** | Workload `Always` messages: "binlog metadata size never decreases", "persisted binlog prefix bytes never change", "data object never shorter than its saved metadata size" (polled and at quiescence; HEAD ContentLength on S3), "list: object at uri holds at least the listed size", "list: listed size falls on an event boundary", "binlog index changes only by tail append or a workload-issued purge prefix removal", "binlog index lists every metadata object present". SUT-side (missing): `ALWAYS("in-process: saved binlog metadata size never decreases")` in `save_binlog_metadata`; optional `ALWAYS(object size ≥ metadata size)` in `querying_only` construction; `SOMETIMES(S3 PutObject succeeded after an SDK retry)` as the vacuity guard for the stale-PUT variant. Observations may race the writer provided each reads the `.json` before the data object. `Always` because a single regression is a violation whenever it is observed; the `SOMETIMES` is a vacuity guard for the retry path. |
| **Antithesis Angle** | Kill/restart loops. S3 last-writer-wins on whole-object PUTs, with delayed or replayed earlier PUT attempts (packet hold, then burst release). Purge or a second writer interleaving with rotations (ticket 9 stale index rewrite, N1 purge-time truncation, ticket 24). Concurrent `list` calls at random moments. |
| **Why It Matters** | Checkpointed data is claimed to be permanent (README:619, 626). A regression silently loses data that was already backed up and that the source may since have purged. PITR tooling downloads `uri` and replays `size` bytes. A regressed object wedges every start ("metadata bigger than actual"). |

**Priority:** P1 · **Confidence:** medium (single-writer paths look sound; known violations need cross-process activity or S3 retry behaviour whose status is open) · **Found by:** focus 1, 4, 9 · **Fault requirements:** workload kill; S3 latency, stalls and packet hold; optional concurrent purge or second writer.

**Rationale / conflicts resolved:** `listed-binlog-size-is-stable-readable-prefix` (focus 4, P1) and `s3-stale-put-replay-never-regresses-object` (focus 9, P2) check the same "never contradicted later" condition. One does it through `list` output; the other does it through a specific S3 replay trigger. The merged property is P1; the stale-PUT replay variant on its own stays P2 and is reported under its SDK-retry `SOMETIMES` guard.

**Evaluation refinements (2026-10-13):** The stale-PUT replay variant relies on Antithesis holding packets on the pbs↔s3 link and releasing them late. Neither `s3_fault_proxy` nor any QA harness can produce that shape, so this variant is unique to Antithesis and should not be the first one cut. The `s3` template must enable packet-hold partitions on pbs↔s3. The proxy buffers the whole request body before forwarding, so a PUT whose client already gave up can still land late.

**Evidence:** `properties/stored-state-never-regresses.md`

**Open Questions:**

- Are a concurrent purge and a second writer in contract (README:473)? `(needs human input)`
- Does CRT retry a held (not lost) PutObject so a stale attempt can land later? `(partial: defaults abort after ≥3 s below 1 B/s and retry; not runtime-verified)`
- Does Versity commit a PUT whose body arrives after the client gave up? `(partial: temp file + link() after body copy; FIN/RST-after-full-body behaviour unknown)`
- Does CRT auto-multipart apply, and can a replayed CompleteMultipartUpload replace the object? `(partial: threshold 8 MiB if content length known; PBS never sets ContentLength; replay unverified)`

### binlog-metadata-matches-stored-bytes — Per-binlog metadata (GTIDs, timestamps) agrees with the stored bytes

| | |
|---|---|
| **Type** | Safety |
| **Property** | For each binlog, the `.json` `added_gtids` equal the GTIDs present in bytes `[0, size)`, consecutive records satisfy `prev[i+1] == prev[i] ∪ added[i]` with disjoint `added` sets, the stored PREVIOUS_GTIDS event equals `prev[i]`, and `min_timestamp`/`max_timestamp` equal the range of non-zero event timestamps over `[0, size)` (epoch when empty). The chain is checked over consecutive files currently in the index, so it holds across purges and ticket-14 gaps. |
| **Invariant** | Workload `Always` per quiescent observation: "binlog added_gtids equals GTIDs in stored bytes", "consecutive binlog previous_gtids chain is consistent", "stored PREVIOUS_GTIDS event equals metadata previous_gtids", "binlog min/max timestamp equals stored event range", plus "binlog added_gtids sets are disjoint" and "metadata GTIDs are a subset of source gtid_executed". SUT-side: chain check in `load_and_validate_binlog_metadata_set` (missing); `ALWAYS("new binlog previous_gtids equals predecessor's previous plus added")` in `open_new_binlog_file_internal` (missing); `assert(incomplete_transaction_timestamps_.is_empty())` at `storage.cpp:138` promoted to `ALWAYS` (partial). The append-time duplicate check is owned by `stored-transactions-exactly-once-across-resumes`. `Always` because any metadata/byte disagreement at any quiescent point corrupts PITR and search results. |
| **Antithesis Angle** | Disconnects and kills at transaction boundaries and around checkpoints. Failed metadata PUTs. A source SIGKILL right after a rotation in position mode reaches the ticket 6 path: resume re-sends N's preamble and a fake ROTATE, `close_binlog` drops the buffered preamble but keeps its timestamps. A source purge racing the `SELECT @@GLOBAL.gtid_purged` → dump sequence on an empty storage. Clock faults make rewrite wall-clock stamps visible. |
| **Why It Matters** | GTID-mode resume, `search_by_gtid_set` and `search_by_timestamp` read only metadata (`storage_core.hpp:313-337`, `search_helpers.cpp:22-85`). A disagreement causes silent skips or duplicates on resume, and wrong PITR plans. The timestamp check is a live check, not a regression guard: the ticket 6 path is reachable with in-plan faults (lead (b) in "New defect leads"; code trace only), giving N+1 a wrong timestamp range in release builds and a debug abort at `storage.cpp:138`. |

**Priority:** P1 · **Confidence:** high for the GTID part (no load-time validation); medium for the timestamp part (reachable by code trace, not reproduced) · **Found by:** focus 1 (two drafts merged) · **Fault requirements:** workload kill, network faults, source SIGKILL via the restart wrapper (`parallel_driver_kill_mysqld.sh`, no node termination) plus `FLUSH BINARY LOGS`, S3/disk shims for the metadata-failure path; **clock** faults optional.

**Rationale / conflicts resolved:** `gtid-metadata-matches-stored-bytes` (P1) and `metadata-timestamps-match-stored-events` (P2) use the same oracle shape. Each parses `[0, size)` and compares a metadata field with what the bytes contain. The merged property takes P1 because resume depends on the GTID fields.

**Evidence:** `properties/binlog-metadata-matches-stored-bytes.md`

**Open Questions:**

- None

### checkpoint-bounds-unpersisted-data — With checkpointing on, received data is persisted within a bounded size and time

| | |
|---|---|
| **Type** | Safety |
| **Property** | With `checkpoint_size` set, the complete-transaction bytes PBS has received but not persisted stay below `checkpoint_size` after every received event. With checkpointing on and PBS connected and caught up on healthy links, a transaction is persisted, and so survives a SIGKILL, within the configured bound. The bound is: `read_timeout + δ` if the source goes quiet; `checkpoint_interval + inter-event gap + δ` under steady traffic; or once `checkpoint_size` more bytes have followed it. δ covers transfer and the backend write, including S3's whole-tail re-upload. |
| **Invariant** | SUT-side (missing): `ALWAYS("unflushed complete-transaction bytes stay below checkpoint_size")` at the end of `storage::write_event` when size checkpointing is on (`get_ready_to_flush_position() − get_flushed_position() < checkpoint_size_bytes_`, `storage.cpp:143-202`). This property owns that message; `collector-memory-bounded-by-checkpoint` keeps its distinct buffer-size `ALWAYS`. Anchor (missing): `SOMETIMES("a flush wrote complete transactions older than checkpoint_interval")` in `flush_event_buffer_internal`, which marks the interval-gap state as reached. Workload: a bounded `Always("with checkpointing on, a marker transaction is persisted within the configured bound")`, evaluated in an `ANTITHESIS_STOP_FAULTS` window. The driver waits for catch-up, commits a marker, picks a follow-up pattern (quiet / steady filler / ≥ `checkpoint_size` filler), optionally SIGKILLs PBS at `t + B + ε`, then reads the persisted `.json` (`size` / `added_gtids`). `Always` fits because the bound is a durability promise that must hold on every evaluation. Liveness phrasing would hide a single late flush. |
| **Antithesis Angle** | The checkpoint baselines are reset in several places: open/rotation (`storage.cpp:118-131, 204-215`), reconnect reusing an open binlog, rewrite's local rotation at `file_size`, and the read-timeout flush that does not reset the baseline (`collector_context.cpp:189-203`). Kills and partitions drive every one of those transitions. Exploration of event timing finds the gap where the interval trigger fires only on the next event (`storage.cpp:179-201`). The S3 re-upload time grows with the tail size (ticket 17), so the flush itself can outlast the interval under latency. |
| **Why It Matters** | SG8 (README:619, 626) is the recovery-point promise: how much received data a crash loses. It is asserted nowhere today, and its best check sat in a P2 blocked on `mem_limit` (W6). The interval half has a real expectation gap. The actual bound is about `max(checkpoint_interval + gap, read_timeout)`, not `checkpoint_interval`, so with the README sample (`read_timeout` 60, `checkpoint_interval` 30s) a quiet period after a burst leaves up to 60 s unpersisted. Ticket 2 (checkpointing silently ignored, fixed in 3804c25) shows this logic can regress unnoticed. Its Hegel test covers the size bound only at the model level, without reconnects or kills. |

**Priority:** P1 · **Confidence:** high for the size `ALWAYS` (mechanism traced, Hegel precedent); medium for the time bound (contract wording and the effective read timeout are open) · **Found by:** evaluation gap G3 (wildcard W6, coverage-balance F11) · **Fault requirements:** workload SIGKILL of PBS and network faults to drive the baseline transitions; `ANTITHESIS_STOP_FAULTS` for the quiet-window check; `checkpoint_size` / `checkpoint_interval` drawn on (not off) in the timelines that evaluate it. No custom faults or memory limit are needed. · **Runs in:** `pull_fs` and the pull-like arm of `s3` (SUT `ALWAYS` in every template, including `fetch_fs`). Needs new driver: `pbs/parallel_driver_checkpoint_rpo.py` (quiet window, marker, follow-up pattern, optional kill, metadata read; reuses the shape of `extra/qa/repro_broken_checkpointing.py`).

**Rationale / conflicts resolved:** Split out of `collector-memory-bounded-by-checkpoint` (gap G3), with refinement R27 applied here. Not merged, because the invariants differ: this property is about durability (bytes persisted within a size and time bound), and that one is about RAM. This property now owns the checkpoint/durability SUT check (the backlog `ALWAYS`) and the ticket 2 regression role. `collector-memory-bounded-by-checkpoint` keeps only the memory checks: its buffer-size `ALWAYS`, the capacity `SOMETIMES`, the OOM check, and (R27) workload VmRSS sampling when `mem_limit` is not honoured.

**Evidence:** `properties/checkpoint-bounds-unpersisted-data.md`

**Open Questions:**

- Is the interval contract "flushed within `checkpoint_interval`" (README:626) or "flushed on the first event after it" (ticket 2)? `(needs human input)`
- Should PBS flush on a local timer (or request heartbeats) instead of only on event arrival? `(needs human input)`

### s3-whole-object-put-publishes-intended-bytes — S3 startup truncation and checkpoint re-uploads publish exactly the intended bytes, including after a failed transfer

| | |
|---|---|
| **Type** | Safety |
| **Property** | On S3: (1) startup recovery's `resize_object` (GET whole tail, then local `resize_file`, then whole-object PUT) leaves the tail object exactly `metadata.size` long, with bytes equal to its previous prefix, and never zero-extended. (2) After a `PutObject` of `tmp_fstream_` fails, any later successful upload in the same process publishes the whole local buffer from offset 0, never a suffix or a partial body. |
| **Invariant** | SUT-side (all missing): `ALWAYS("S3 resize download covers the requested size")` before `resize_file` in `do_resize_object` (`s3_storage_backend.cpp:782`); `REACHABLE("S3 tail truncated by resize_object during startup recovery")` after its PUT (a backend-local site, distinct from `recovery-paths-exercised`'s marker at `storage_core.cpp:979`); `SOMETIMES("S3 upload succeeded on a stream whose previous upload failed")` in `upload_tmp_stream_internal`; optional tripwire `ALWAYS("S3 checkpoint upload published the whole local buffer")` after a successful upload, using a HEAD only in the Antithesis build (`ContentLength == local temp size`); investigation found a mid-body abort cannot publish a suffix (the SDK rewinds the body and recomputes its length per request), so the property's main value is the resize zero-extension and ambiguous-PUT scenarios. Workload (`s3` template): `Always("S3 tail object equals its metadata size after startup recovery")`, evaluated when the recovery log line appeared and no append followed yet; `Sometimes("S3 startup truncation observed in a writer log")` as a log-line fallback for the `REACHABLE`. Content after appends is judged by the existing `stored-state-never-regresses` prefix hash and `stored-binlog-byte-prefix-of-source`, which this property feeds triggers to. `Always` fits because a wrong published body is silent corruption that later size checks cannot detect. |
| **Antithesis Angle** | Partitions and stalls on pbs↔s3 **mid-body** during the upload of a large tail are the only route to the dangerous outcome (a suffix published as the whole object). The `s3_fault_proxy` cannot produce it, because it reads the whole request before acting, and CRT's throughput monitor may abort a stalled transfer. Kills between the data PUT and the `.json` PUT, and ticket 18's data-upload variant (the destructor retry uploads old+2n, the next start must truncate), drive the resize path. `truncate_body` on startup GETs targets the download half. |
| **Why It Matters** | `s3_storage_backend.cpp` is a 16-commit churn hotspot, and its mechanics differ from the filesystem backend's, yet it had only one S3-specific property (F7). Ticket 18's S3 data-upload variant is called "self-healing" only because this truncation round trip is assumed exact. A zero-padded or suffix tail of the right length passes every size check, and on encrypted storages it decrypts to garbage that only content oracles see. |

**Priority:** P2 · **Confidence:** medium (resize mechanism traced; the sticky-failbit hypothesis refuted for HTTP-error failures by ticket 18's repro numbers; mid-body abort behaviour unverified) · **Found by:** evaluation gap G6 (coverage-balance F7; focus 8 lead) · **Fault requirements:** network partitions, latency and stalls on pbs↔s3 (native, timed inside uploads), workload SIGKILL of the writer; **custom:** `s3_fault_proxy` 5xx on six or more consecutive binlog PUTs and `truncate_body` on GETs. Node termination is not needed. · **Runs in:** `s3` (both arms), by extending `pbs/parallel_driver_s3_faults.py` with the triggers above. Needs new driver: `pbs/anytime_s3_tail_recovery.py` (reads the writer log and HEADs the tail through `s3:9000`). Config draw: a large `max_binlog_size` with a small `checkpoint_size` in some timelines, so bodies are large.

**Rationale / conflicts resolved:** Kept separate from `failed-flush-never-reappends-data` and `stored-state-never-regresses`, not merged. The first owns the ticket 18 duplicate append after a failed metadata save (storage layer); the second owns the prefix hash and stale-PUT ordering across writes. This property asserts the body of one whole-object PUT and the resize round trip at backend-local sites, and feeds triggers to both. A10 (a stale short-range read on a non-Versity store) is an explicit exclusion, because the harness uses Versity.

**Evidence:** `properties/s3-whole-object-put-publishes-intended-bytes.md`

**Open Questions:**

- Should `do_resize_object` verify the downloaded length itself instead of trusting `resize_file`? `(needs human input)`
- Is a failed start after a successful resize PUT (temp-file removal fails) acceptable? `(needs human input)`

### stored-binlogs-replay-to-source-state — Replaying the stored binlogs reproduces the source's tables

| | |
|---|---|
| **Type** | Safety |
| **Property** | At quiescence, `mysqlbinlog --verify-binlog-checksum <stored binlogs in index order> \| mysql` into a freshly initialised scratch `mysqld` (same version as the source) exits 0. It yields the same per-table checksums and pending XA set as the reference. The reference is the source itself when the storage has converged, or an identical replay of the source's own binlogs cut at the stored GTID set / tail position when it has not. This holds for plain, encrypted (decrypted offline) and rewrite-mode storages, across all event kinds the load generator produces. |
| **Invariant** | Workload `Always`, one message per storage kind: "replay of stored binlogs matches the source state (position)", "(gtid)", "(rewrite)", "(encrypted)". The replay's exit status is part of the check. `Always` fits because any divergence means a restore from this backup produces wrong data. A secondary strict variant, `--skip-gtids`, gets its own message, "strict replay of stored binlogs matches the source state". It catches duplicates that GTID auto-skip hides in the realistic variant. Vacuity: `Sometimes("replay restore covered a storage containing <kind> transactions")` for each of compressed payload, XA prepare/commit, PARTIAL_JSON, MINIMAL row image, DDL, and tagged GTID (8.4 or later), plus `Sometimes("replay restore ran on a storage that saw at least one writer kill")`. No SUT instrumentation is required. Optional anchor (missing): `SOMETIMES("rewrite split a binlog inside an XA prepare/commit pair")`. |
| **Antithesis Angle** | Kills, rotations, checkpoints and rewrite `file_size` splits land next to boundary-sensitive event kinds: an XA PREPARE without its COMMIT at a cut, a compressed `TRANSACTION_PAYLOAD`, DDL with no BEGIN/XID. Network and S3 faults force resumes at those points. The oracle judges the result through the consumer the backup exists for. That is where the byte oracles are weakest: in rewrite mode (CRCs, preamble, `transaction_length` regenerated), on encrypted storages, and for events after a terminal ROTATE/STOP. |
| **Why It Matters** | PITR via `mysqlbinlog \| mysql` is the product's purpose (README:3, 711). Today no oracle applies a backup. The check also separates cosmetic file differences (ticket 14's dropped ROTATE) from restore-breaking ones across the many expected-to-fail properties. It answers `stored-rotate-or-stop-is-last-event`'s "what does a downstream tool do" question empirically. Prior art: `mtr/binlog_streaming/t/rs_tail_reuse.test:70-110` replays a pull dump with a row-signature compare, in one fault-free scenario. |

**Priority:** P1 · **Confidence:** medium (the oracle is standard and the MTR precedent exists; the reference construction for non-converged storages and the replay cost are unverified) · **Found by:** evaluation gap G2 (wildcard W5, W8) · **Fault requirements:** workload SIGKILL of PBS, network faults (native); **custom:** `s3_fault_proxy` for S3 write failures (optional). Node termination is not needed. · **Runs in:** needs new driver: `pbs/finally_replay_restore.py` for `fetch_fs`, `pull_fs` (after pull stops gracefully) and `s3`. It requires `bin/mysqld` and `bin/mysql` from the same generic tarball in `runtime-base` (today only `mysqlbinlog` is planned), the offline decryptor from W2, and an event-kind dimension in `workload/parallel_driver_source_load.py` reusing the `extra/qa/event_variety_test.py` generators.

**Rationale / conflicts resolved:** New, and a new kind of oracle (apply-based restorability). It does not replace the byte oracles: complete-copy and exactly-once stay with `graceful-exit-leaves-flushed-storage` and `stored-transactions-exactly-once-across-resumes`, byte prefixes with `stored-binlog-byte-prefix-of-source`, and rewrite payload equality with `rewrite-storage-transaction-sequence-matches-source`. It judges their failures by restore impact. `evaluation/synthesis.md` recommends a short second evaluation pass for it.

**Evidence:** `properties/stored-binlogs-replay-to-source-state.md`

**Open Questions:**

- Is the per-timeline replay cost (scratch `mysqld` init plus replay) affordable, or must the check be sampled? `(partial: cost model known; needs a local timing run)`
- Is offline decryption a supported restore path, or only reading through pull? `(needs human input)`

## Multiple PBS processes on one storage or one source

There is no fencing: no lock, deterministic `.tmp` names, constructor-time `.tmp` deletion and tail truncation in every non-`querying_only` mode, and index rewrites from stale in-memory lists. These properties cover second writers, purges and failed-bind pulls overlapping a live writer, concurrent purges, and independent instances that share a `server_id`. Whether concurrent openers are in contract (README:473) is the main open question.

### second-writer-on-storage-fails-closed — Overlapping writers never yield a silently corrupt storage (ticket 24)

| | |
|---|---|
| **Type** | Safety |
| **Property** | Two writer processes (fetch/fetch, fetch/pull, pull/pull on different ports, or a zombie writer plus a supervisor-started takeover) may be alive on one storage at overlapping times. Neither exits 0 unless, after both stop and one clean fetch runs, the storage opens and every stored binlog is a source byte prefix ending on a transaction boundary. |
| **Invariant** | Workload `Always("no overlapping writer exits 0 unless the storage opens with prefix binlogs on boundaries")`. An optional stricter variant is `Always("second writer on a live storage is refused")`, used if the owners rule overlap out of contract. `Sometimes(both writers simultaneously past storage construction)` proves the overlap happened, and `Sometimes(both writers appended after the other's construction)` proves the ticket-24 interleaving itself was reached. This property runs only in `contention`, so its SUT messages carry the harness timeline tag. SUT-side (missing): `UNREACHABLE("data file grew beyond in-memory size")`, and a ticket-24-specific `.tmp`-removal anchor with its own message. The "metadata bigger than data" validator site (`storage_core.cpp:974-977`) is owned by `kill-anywhere-storage-reopens`, and this property references it. `Always` because one silently corrupt outcome of an overlap is a violation; the `Sometimes` pair proves the overlap was actually reached, and `UNREACHABLE` because data growing past the in-memory size is itself the defect. |
| **Antithesis Angle** | Process start timing relative to the other writer's put/rename, recovery truncation and append. Both share the deterministic `.tmp` names and `app` streams. Zombie takeover: a node hang or partition of pull #1, then the workload starts pull #2, then #1 resumes. On S3, data and `.json` objects are last-writer-wins independently (Versity overwrites are atomic, so no torn objects). Same-config instances also evict each other's dump threads on the source (same server_id): the evicted instance gets ER 1236 and exits non-zero, with no in-process ping-pong. That shrinks the window in which both append, so the main arm gives overlapping writers distinct `server_id`s and a smaller same-`server_id` arm keeps the eviction path. A second pull on the same port fails to bind after constructing the storage; that case belongs to `other-process-startup-spares-live-writer`. |
| **Why It Matters** | Ticket 24 (open, validated): there is no lock primitive, `.tmp` suffixes are fixed, and the constructor truncates. A QA repro shows exit 0 with an oversized, diverged binlog. Overlapping cron runs and supervisor takeovers are routine. |

**Priority:** P0 · **Confidence:** high (mechanism plus QA repro on the file backend); medium for S3 · **Found by:** focus 2, 7 · **Fault requirements:** none beyond the workload launching the second writer; node hang or partition for the zombie variant; thread pausing helps. Node termination is not needed.

**Evidence:** `properties/second-writer-on-storage-fails-closed.md`

**Open Questions:**

- Is a second writer on a live storage in contract (fetch+fetch, fetch+pull, pull+pull)? `(needs human input)`

### other-process-startup-spares-live-writer — Another process opening the storage never damages a live writer (N1, N2)

| | |
|---|---|
| **Type** | Safety |
| **Property** | A `purge_binlogs`, a second fetch, or a pull that will later fail to bind its port may construct the storage while a writer is live. Its startup recovery never deletes the live writer's in-flight `.tmp` objects and never truncates bytes from the live tail. After the writer stops, the tail is a source prefix and the storage reopens. |
| **Invariant** | Workload `Always` messages: "live writer's tail stays a source prefix after a concurrent storage open", and "live writer never fails with rename/ENOENT after a concurrent storage open". After the writer's graceful stop, the storage is recovery-free and reopens. SUT-side anchors (missing), which are the only visibility because purge uses `null_logger`: `SOMETIMES(purging-mode ctor performed tail truncation)`, `SOMETIMES(startup removed a .tmp object)` with the mode in details, and `UNREACHABLE("tmp object vanished before rename")` on the ENOENT branch of `do_put_object`. Only `contention` timelines open a storage beside a live writer, so these SUT messages carry the harness timeline tag; an in-contract firing then shows under its own message. `Always` because a single damaged live writer is a violation; the `SOMETIMES` anchors show the dangerous interleavings were reached, and `UNREACHABLE` because the vanished-tmp branch is itself the defect. |
| **Antithesis Angle** | Any non-`querying_only` constructor runs `remove_temporary_objects` (`storage_core.cpp:597-630`), which is N2, and tail truncation to the metadata size (`:974-983`), which is N1, with no fencing. Pull constructs storage before binding (`pull_operation.cpp:109` vs `:117`, `network_service.cpp:679-681`), so a pull that fails with "address in use" has already done both. Pausing the writer between its data append and metadata save opens the window. The `.tmp` variant (N2) is filesystem-only, because S3 temporary names are UUIDs. The N1 truncation variant runs on both backends; on S3 it goes through the resize download/re-upload race. |
| **Why It Matters** | Purge is a routine operation, and supervisor restarts overlap old instances. Damage is silent and permanent. A deleted `.json.tmp` triggers the ticket-18 duplicate path. A cut tail wedges the storage ("metadata bigger than actual") and makes pull sessions read past EOF. |

**Priority:** P1 · **Confidence:** high (mechanism); medium (frequency) · **Found by:** focus 2, 8 (three drafts merged) · **Fault requirements:** none beyond workload-launched concurrent processes; process pause helps. Node termination is not needed.

**Rationale / conflicts resolved:** Focus 2 rated the purge-truncation variant P0 and the `.tmp` variant P1. Focus 8 rated the bind-failure variant P2. The merge resolves to P1. README:473 already warns against running purge alongside fetch, and the owners have not said whether any concurrent opener is in contract. The property becomes P0 if they rule concurrent purge in contract.

**Evidence:** `properties/other-process-startup-spares-live-writer.md`

**Open Questions:**

- Is any concurrent opener in contract (purge+writer, second pull/fetch)? `(needs human input)`
- Should purge perform recovery at all, or open the tail like `querying_only`? `(needs human input)`
- Is the `metadata.json` encryption stamp (`storage_core.cpp:170-178`) meant to run before index/records validation? `(needs human input)`

### stale-index-commit-never-wedges-storage — A stale in-memory index is never committed over a newer one (ticket 9)

| | |
|---|---|
| **Type** | Safety |
| **Property** | After any interleaving of `purge_binlogs` with a running writer, or of several concurrent purges, the storage opens in streaming mode, its index references only existing objects, and after concurrent purges it is a suffix of the original. |
| **Invariant** | Workload `Always` messages: "storage opens in streaming mode after purge overlapped a writer", "storage opens after concurrent purges", "every index entry references an existing object", "index after concurrent purges is a suffix of the original". `Sometimes(writer rotated after a successful concurrent purge)` and `Sometimes(two overlapping purges both reported success or warning)` prove the overlap happened. These are workload-only checks: no contributor proposed an SUT assertion. The purge-pair anchor needs a `contention` step that runs two purges with no writer, because `fetch_fs` serialises purge under the writer lock and `contention` races purges against writers. Until the topology adds that step the anchor is deferred, so it does not report as an unmet `Sometimes`. `Always` because one wedged or inconsistent index is a violation whenever it is checked; the `Sometimes` anchors guard against vacuity by proving the overlap happened. |
| **Antithesis Angle** | A process loads `binlog.index` at construction and later rewrites it from its stale in-memory list. This happens with the writer at its next rotation (`storage_core.cpp:701, 789-799`) after a concurrent purge, and with the later of two concurrent purges (`:368-433`). Process pausing between construction and commit makes it reachable. Pull also serves purged names from memory. |
| **Why It Matters** | A new concurrent-purge lead, plus ticket 9 (open, validated). Deleted binlogs are resurrected in the index or objects are orphaned. The storage becomes unopenable in every mode, and purge itself cannot repair it. The ticket 9 variant (purge beside a writer) is out of contract today: README:473, which predates the ticket, says to avoid it. It runs only in a separate misuse phase, so the property's in-contract value rests on concurrent purges. |

**Priority:** P1 (rests on the concurrent-purges variant; P0 only if purge alongside a writer is ruled in contract) · **Confidence:** high for ticket 9; medium for purge+purge (traced, not reproduced) · **Found by:** focus 2 (two drafts merged) · **Fault requirements:** none beyond workload concurrency; process pause helps.

**Evidence:** `properties/stale-index-commit-never-wedges-storage.md`

**Open Questions:**

- Will maintainers support purge beside a writer, or make it refuse? `(needs human input)`
- Are concurrent purges in contract (README is silent)? `(needs human input)`
- Does any purge report `success` in interleaving A, or always `warning`? `(partial: filesystem reports warning; S3 likely success, Versity unverified)`
- What should pull return for a purged-then-requested binlog? `(needs human input)`

### distinct-storages-shared-server-id-stay-consistent — Copy-pasted server_id across independent PBS instances never corrupts either storage

| | |
|---|---|
| **Type** | Safety |
| **Property** | Two PBS writers on **different** storages that share `replication.server_id` (every README example uses 42) each keep a storage that is a byte prefix of the source and that opens on the next run. |
| **Invariant** | Workload `Always("each storage of a shared-server_id pair is a source byte prefix")`. Workload `Sometimes(same-server_id eviction observed)` (an instance exits with ER 1236) proves the collision happened, and `Sometimes(both instances advanced in the same window)` covers progress. Progress needs a supervisor restart of the evicted instance. Both instances run inside `pbs`: the second is a PBS process on a sibling storage, launched by a `contention` test command. The property is deferred until that step exists. Every other template allocates `server_id`s per role, so ordinary timelines never see this eviction. `Always` because either storage diverging from the source even once is corruption; the `Sometimes` checks prove the collision and progress actually occurred. |
| **Antithesis Angle** | PBS sets no replica UUID, so MySQL evicts the other dump thread with the same server_id (`rpl_source.cc:1145-1173`). The evicted instance gets ER 1236 and fail-stops (non-zero exit), with no in-process 2013 reconnect ping-pong, so every eviction exercises the exit-flush path and a supervisor-driven restart and resume (tickets 14 and 18 surfaces). CPU modulation and pauses decide who evicts whom. Scope: clients that register no UUID, which is PBS today. |
| **Why It Matters** | A realistic misconfiguration (HA pair, or cron fetch plus pull from one config template) silently becomes either a crash loop or a resume-path torture test. `second-writer-on-storage-fails-closed` covers only the same-storage case. |

**Priority:** P2 · **Confidence:** medium (eviction by server_id traced to `rpl_source.cc`; ER 1236 fail-stop confirmed by code) · **Found by:** focus 11 · **Fault requirements:** none beyond a second PBS process on a sibling storage in `pbs` (no second container needed), one source, and a supervisor restart loop; timing faults help.

**Evidence:** `properties/distinct-storages-shared-server-id-stay-consistent.md`

**Open Questions:**

- Should PBS detect or avoid a server_id collision (warn, document, or set a per-instance `@replica_uuid`)? `(needs human input)`

## Resume and replay correctness

How fetch and pull continue after a reconnect, restart or kill: position-mode and GTID-mode resume requests, the reader's continuity checks, the in-process reconnect path, and repeated fetches. The central oracle is exactly-once storage of every source transaction.

### stored-transactions-exactly-once-across-resumes — Every source transaction is stored exactly once across resumes

| | |
|---|---|
| **Type** | Safety |
| **Property** | Across any sequence of fetch runs, pull reconnects, kills, signals and storage faults, the stored transaction sequence equals the source's sequence from the first stored transaction onward, with no duplicates and no gaps. After a final fault-free fetch to EOF the storage is a complete copy. |
| **Invariant** | Workload `Always` at quiescence after a final fault-free fetch to EOF, with one message per mode. "position mode: storage is a complete byte copy of the source" uses QA `check_complete_copy`, IN_USE cleared, magic-only tail allowed. "GTID mode: stored GTID sequence from bytes equals source sequence" parses bytes, not `.json`, whose sets hide duplicates. "rewrite mode: stored GTID sequence equals source sequence". Encrypted storages are decrypted with the independent Python decryptor. The ground truth is the source's own binlog bytes (`mysqlbinlog --raw --read-from-remote-server`) and `gtid_executed`; a workload ledger is only a cross-check, because a commit whose acknowledgement was lost to a network fault is ambiguous in a client ledger. The check is evaluated only when the final fetch reaches exit 0; a final fetch that never does is the fetch-convergence liveness gap (G4 in `evaluation/synthesis.md`). SUT-side (missing), two twins with distinct messages: a receive check `ALWAYS("received transaction GTID is not already stored or buffered")` at the transaction boundary in `storage::write_event`, and a block check `ALWAYS("flushed block GTIDs are disjoint from GTIDs already stored")` in `storage_core::write_event_block`, which catches the ticket 18 re-append in GTID and rewrite modes. In rewrite mode they are the only in-process duplicate detectors, because position re-stamping makes the reader check tautological (`collector_context.cpp:422-428`). `Sometimes(kill_count ≥ 3 before convergence)` is a coverage hint. `Always` because a single duplicated or missing transaction is a violation at every quiescent check. |
| **Antithesis Angle** | Kills at arbitrary points during checkpoints, rotation, disconnect cleanup and recovery itself. Source↔PBS partitions and resets (2013 reconnects). Source restarts and rotations while PBS is down. S3/disk faults. These explore the cross-product of kill points and resume modes (position/GTID, checkpointing on/off, rewrite, encryption). |
| **Why It Matters** | README:711 resume continuity is the product's core claim. A silent duplicate or gap in a binlog backup corrupts point-in-time recovery. Ticket 14 is a known GTID-mode exception that the oracle must filter or track separately. Scope excludes runs with a concurrent purge (N2, out of contract per README:473) and runs where the source purged an un-checkpointed tail before it was re-fetched. |

**Priority:** P0 · **Confidence:** high (claimed guarantee; reusable QA oracles exist) · **Found by:** focus 1, 3, 9 (four drafts merged) · **Fault requirements:** workload SIGKILL of PBS (node termination not needed), network faults; S3/disk fault shims optional.

**Rationale / conflicts resolved:** Four drafts were merged here: `source-transaction-stored-exactly-once` (focus 1), `restart-after-kill-converges-to-source` (focus 3), `no-gtid-written-twice` (focus 9) and this one. They share one invariant and one convergence oracle. Three of them proposed the same SUT duplicate-GTID check at different sites (`write_event`, `write_event_block`, `process_binlog_event`). The merge keeps one message. `no-gtid-written-twice` was P1 as a standalone guard; it becomes this property's in-process twin, and the merged priority is P0.

**Evidence:** `properties/stored-transactions-exactly-once-across-resumes.md`

**Open Questions:**

- Is byte identity an official guarantee or only an MTR convention? `(needs human input)`

### inprocess-reconnect-resumes-without-dup-or-gap — pull's in-process reconnect resumes with an empty buffer and no duplicate or gap

| | |
|---|---|
| **Type** | Safety |
| **Property** | After any number of in-process reconnects (2013/read-timeout path, or an EOF from a blocking dump), pull's event buffer is empty when the new dump request is sent, and the storage remains a source byte prefix with no transaction stored twice or skipped. |
| **Invariant** | SUT-side (missing): `ALWAYS("after disconnect cleanup the event buffer is empty")` (`current_position == flushed_position`) after `collector_context.cpp:200` (2013 path); `ALWAYS("pull EOF from a blocking dump leaves the event buffer empty before reconnect")` at `:184-187` (EOF path, distinct message); and `ALWAYS("GTID dump request is sent with no buffered transaction")` (`!has_event_data_to_flush()`) right before `switch_to_gtid_replication` (`:249`). Anchors: `SOMETIMES(pull resumed into its still-open binlog after reconnect)` at `:535` and `REACHABLE("pull collector saw EOF from a blocking dump")` at `:184-187`. Workload `Always("pull storage stays a source prefix with unique GTIDs across in-process reconnects")` per quiet period. `ALWAYS`/`Always` because a non-empty buffer at any reconnect, or one duplicate or gap, is a violation on every occurrence; `SOMETIMES`/`REACHABLE` are exploration anchors for the reconnect paths. |
| **Antithesis Angle** | Two routes. Network faults, source SIGKILL and `KILL CONNECTION` of the dump thread all reach PBS as 2013 and take the discard-flush-reconnect path (case 2). A graceful source `SHUTDOWN` (the restart wrapper already issues one) or `KILL QUERY` of the dump thread makes mysqld send EOF, possibly mid-transaction (`rpl_binlog_sender.cc:366-384`, `mysqld.cc:2323-2368`); pull's case (3) returns without discarding or flushing and reconnects with the buffer intact. Position mode resumes at flushed + buffered, GTID mode from flushed only. Same-server_id eviction is not an EOF trigger: it sends ER 1236 and pull exits. |
| **Why It Matters** | Reconnect is pull's advertised resilience mechanism (README:378). **Expected to fail today** on the EOF path (lead (a) in "New defect leads"): plain GTID mode re-receives the buffered transactions, fail-stops on the position check and recovers on restart; rewrite mode stores them twice silently; position mode with a partial transaction buffered fail-stops. The GTID-request `ALWAYS` and the EOF-path `ALWAYS` are expected to fire after a graceful source shutdown. Validated defect candidate (new ticket, draft only). |

**Priority:** P1 · **Confidence:** high for the EOF path (traced in MySQL 9.7.1 server and client source; 8.0/8.4 assumed identical) · **Found by:** focus 3, 9 · **Fault requirements:** network faults on PBS↔source (native); source `SHUTDOWN` via the restart wrapper and `KILL QUERY` / `KILL CONNECTION` of the dump thread from the workload. Node termination is not needed.

**Evidence:** `properties/inprocess-reconnect-resumes-without-dup-or-gap.md`

**Open Questions:**

- None

### gtid-resume-preserves-source-rotations — GTID resume keeps every source rotation (ticket 14)

| | |
|---|---|
| **Type** | Safety |
| **Property** | After every reconnect or restart in GTID mode (non-rewrite), the stored binlog names have no gaps from the first stored name to the tail, and every non-tail stored binlog is byte-identical to the source's closed binlog, including its terminal ROTATE or STOP (a graceful source restart writes STOP; a source crash writes neither). |
| **Invariant** | Workload `Always("GTID resume: stored names contiguous and non-tail binlogs byte-identical to source")` after a quiet period, compared with `SHOW BINARY LOGS` and the source files. The byte half lives in the workload, which compares against the source's file (it carries a terminator exactly when one was written). SUT-side (missing), in the name-mismatch branch (`collector_context.cpp:540-553`): `ALWAYS("GTID resume continues with the binlog right after the stored tail")` (`new name == current.next()`), and `ALWAYS(terminator_written || source_sigkilled, "GTID resume closed the stored tail without its ROTATE/STOP event (no source SIGKILL in this timeline)")`. After a source crash the SUT sees the same name mismatch with no terminator on correct behaviour, and the binary cannot know what `mysql-source` did, so `source_sigkilled` is read from the harness timeline-tag file; the source-kill driver sets it before its first kill. Every template runs that driver, so the SUT check is informative only in timelines where the kill knob is off. The workload byte check is the primary detector. The REACHABLE anchor for "resume landed in a different source binlog" at the same site belongs to `recovery-paths-exercised`. `Always` because any lost rotation or byte mismatch is a violation on every resume, and the SUT `ALWAYS` checks must hold on every pass through the mismatch branch. |
| **Antithesis Angle** | Partitions, source restarts, PBS read_timeout disconnects and PBS kills, combined with workload `FLUSH BINARY LOGS` repeated 0 to 3 times while PBS is disconnected and caught up. |
| **Why It Matters** | Ticket 14 (validated): the name-mismatch branch only calls `close_binlog()`, with no ROTATE and no gap check. This happens in normal operation whenever a caught-up PBS reconnects after a rotation. Stored copies are not exact. `mysqlbinlog --raw` and replica file tracking never see the switch, and transaction-free binlogs are missing. Expected to fail today. |

**Priority:** P1 · **Confidence:** high (known failing) · **Found by:** focus 7, 9 · **Fault requirements:** network partitions or workload kill; source restart optional (graceful `SHUTDOWN` writes STOP; via the mysqld restart wrapper, no node termination).

**Rationale / conflicts resolved:** Both foci rated it P1. Focus 9 notes that if byte identity is not an official guarantee, the property drops to P2, because `stored-transactions-exactly-once-across-resumes` already covers the transaction gap.

**Evidence:** `properties/gtid-resume-preserves-source-rotations.md`

**Open Questions:**

- Is byte identity with the source an official guarantee for GTID mode, or only an MTR convention? `(needs human input)`
- Should transaction-less intermediate binlogs be materialised in the storage? `(needs human input)`

### resume-continuity-guard-never-trips — Resume continuity checks never fire

| | |
|---|---|
| **Type** | Safety |
| **Property** | In runs without source reset or external storage damage, PBS's resume self-checks never detect a mismatch between the storage, or the in-memory resume state (event buffer), and the source. |
| **Invariant** | SUT `UNREACHABLE`, one per raise site, each with a unique message (missing): artificial-ROTATE position mismatch (`collector_context.cpp:524-532`, `reader_context.cpp:547-567`), non-contiguous first event after resume (`reader_context.cpp:908-921`), GTID resume into a non-tail stored binlog (`storage_core.cpp:276-279`), and first event after resume is not a transaction start ("transaction group must start", `reader_context.cpp:775-778`, message "resume: first event after resume is not a transaction start"). Plus workload `Always("fetch never reports CR_1236 position beyond file size while the source still holds the binlog")`. `Unreachable` because each site can only be reached through a replay inconsistency. Source RESET runs only behind the source-swap flag; in timelines where it ran, the messages carry the harness timeline tag, so a RESET-induced firing does not mark the in-contract property red. |
| **Antithesis Angle** | Converts latent duplicates and truncations from earlier faulted runs into an immediate signal on the next resume, and gives Antithesis a precise anchor for searching the causal history. |
| **Why It Matters** | These are the only in-product replay detectors. Each firing is a wedge or fail-stop, and it points at an earlier exactly-once violation or a stale in-memory buffer. **Expected to fire today** in pull after a graceful source `SHUTDOWN` or `KILL QUERY` (lead (a) in "New defect leads"): GTID mode trips the position check, position mode with a partial transaction trips the transaction-start check. With `sync_binlog=1` an ahead-of-source CR_1236 after a source crash is ruled out. |

**Priority:** P1 · **Confidence:** high · **Found by:** focus 9 · **Fault requirements:** same as `stored-transactions-exactly-once-across-resumes` (workload kill, network faults). The workload must not RESET or swap the source in these runs.

**Evidence:** `properties/resume-continuity-guard-never-trips.md`

**Open Questions:**

- Should the guards be SUT `UNREACHABLE` or classified workload-side from logs? `(partial: SUT UNREACHABLE is sound in timelines without source RESET, since sync_binlog=1 rules out ahead-of-source 1236; hinges on whether source-swap/RESET actions share a template)`

### repeat-fetch-on-caught-up-storage-is-noop — Repeated fetch on a caught-up storage changes nothing

| | |
|---|---|
| **Type** | Safety |
| **Property** | With the source quiesced and the storage caught up (stored tail name equals the source's newest binlog; a magic-only tail of size 4 only when that source file holds no transaction), any number of fetch runs (including killed ones) leave every object byte-identical, and a clean run exits 0. A magic-only tail is a no-op in both position and GTID mode. |
| **Invariant** | Workload `Always("fetch on a caught-up storage changes no object")`: the snapshot `{name: (size, sha256)}` of data, `.json`, index and `metadata.json` is unchanged, ignoring `*.tmp` removal. `Always` because any change is spurious. |
| **Antithesis Angle** | Kills during the re-open path (S3 append download, constructor recovery), network faults during the preamble, and S3 latency. Checks that the resume path itself is side-effect free. |
| **Why It Matters** | Cron fetch is the main backup workflow. Spurious rewrites would turn harmless repeats into crash windows (tickets 15 and 18). |

**Priority:** P2 · **Confidence:** high that it currently holds (regression guard) · **Found by:** focus 9 · **Fault requirements:** workload kill of PBS, network faults.

**Evidence:** `properties/repeat-fetch-on-caught-up-storage-is-noop.md`

**Open Questions:**

- None

## Pull replication source serving

pull's home-grown MySQL replication source: dump preamble, file switches, position chains and CRCs, error packets, clients that arrive early or ahead of the flushed data, hostile or misaligned requests, and authentication. This is the only in-product way to read an encrypted storage back as plaintext.

### sender-file-switch-emits-artificial-rotate — Every binlog switch served by pull carries an artificial ROTATE and the new file's FDE (N5)

| | |
|---|---|
| **Type** | Safety |
| **Property** | Every time a pull dump stream moves from binlog N to N+1, the client receives an artificial ROTATE naming N+1 before any non-artificial event of N+1, and every generated artificial ROTATE uses the checksum setting of the binlog being left. This holds even when the sender polled while N+1 was still magic-only, and when N and N+1 differ in checksum algorithm or FDE version. (Under N5 the client still receives N+1's FDE as an ordinary event; what is lost is the artificial ROTATE and the sender's FDE-state refresh.) |
| **Invariant** | Workload `Always` messages: "every served file change is preceded by an artificial ROTATE naming the new file" (primary; the FDE-first check cannot detect N5), "every served event parses under the most recent FDE received" (catches the stale-checksum artificial ROTATE at the following switch), and, for `mysqlbinlog --raw` clients, each output file equals the stored file byte for byte. `Sometimes(dump client at EOF of a STOP-terminated binlog while the next binlog is magic-only)` anchors scenario A2. SUT-side (missing): `ALWAYS("sender never changes binlog name on an empty fetch")` in `populate_event_block`; `ALWAYS("sender artificial ROTATE checksum follows the previous file's FDE")` in `handle_start_states` (replaces the earlier FDE-first SUT assertion, which N5 does not violate); `SOMETIMES(sender at EOF saw a magic-only next binlog)`; `SOMETIMES(sender crossed a checksum-format boundary)` in `handle_binlog_file_switch`. `Always` because each missing or stale ROTATE breaks the client at that switch; the `Sometimes`/`SOMETIMES` anchors make the rare A2 and checksum-boundary states exploration targets. |
| **Antithesis Angle** | A race between the collector's rotation steps (flush N, then create N+1 with 4 bytes, then the first flush of N+1) and the sender's 500 ms EOF poll. Thread pausing and CPU modulation move the poll into the window. Without checkpointing, the window spans the whole life of N+1. Source `SET GLOBAL binlog_checksum` toggles or upgrades make consecutive files differ in format. Source clean restarts and kills (STOP or no terminator at the end of N) make the skip client-visible (scenario A2). |
| **Why It Matters** | `fetch_event_block` advances to N+1 on a zero-length read (`storage_core.cpp:523-541`), and `populate_event_block` returns EOF before `handle_binlog_file_switch` (`sender_context.cpp:487-491`), so the switch is lost. **Expected to fail today.** After a normal rotation (real ROTATE ends N) `mysqlbinlog --raw` output is still correct; after STOP or no terminator, the raw client silently overwrites its copy of N with N+1's bytes (scenario A2). With a checksum change, the next artificial ROTATE carries the wrong footer and the client mis-names its next file or fails. The existing concurrent-reader test strips artificial ROTATEs (`storage_property_test.cpp:1356`), so it cannot see this. |

**Priority:** P0 · **Confidence:** high (mechanism traced end to end); medium (client-visible impact) · **Found by:** focus 2, 10 (independently confirmed by focus 4; also caught by focus 11's chained-fetch oracle) · **Fault requirements:** none beyond thread pausing/CPU modulation (default fault set); workload checksum toggles for the format-change variant; source clean restart or SIGKILL via the restart wrapper for scenario A2 (no node termination).

**Rationale / conflicts resolved:** Focus 2 rated it P0 and focus 10 rated the format-change variant P1. The merge resolves to P0. The mechanism needs only default faults, and with a format change the same skip produces misparsed events, not just a cosmetic gap.

**Evaluation refinements (2026-10-13):** In the default config (no checkpointing) N5 is effectively deterministic for any caught-up blocking client at a rotation, so that case belongs in an MTR reproduction. Antithesis weights the `checkpoint_size`/`checkpoint_interval`-on configurations, where the window between N+1's creation and its first flush is short and depends on the 500 ms poll and thread pausing, plus the checksum-change and A2 variants. Each assertion reports the checkpoint config in its details. A vacuity marker `Sometimes("sender crossed a rewrite-generated file switch")` covers rewrite-mode local rotations, which are the most frequent source of magic-only next files.

**Evidence:** `properties/sender-file-switch-emits-artificial-rotate.md`

**Open Questions:**

- Does mysqlbinlog 8.0/8.4 raw mode behave as 9.6 does (output name from any ROTATE, truncating reopen at each FDE)? `(partial: confirmed in PXC 9.6 client/mysqlbinlog.cc; 8.0/8.4 not read)`
- Where should the fix go ("don't advance on an empty next file" or "emit ROTATE whenever the name changes")? `(needs human input)`

### pull-served-stream-self-consistent — Served events carry a log_pos chain and CRC consistent with where they were served

| | |
|---|---|
| **Type** | Safety |
| **Property** | Every non-artificial event pull serves has `next_event_position == its offset in the file + event_size`, and verifies its CRC32 when the governing FDE says CRC32. |
| **Invariant** | Workload `Always` with two messages ("pull: served event next_event_position equals file offset plus size", "pull: served event CRC32 verifies under the governing FDE"), checked per event by a raw dump client that tracks the offset from the artificial ROTATE and the checksum mode from the last FDE. SUT-side `ALWAYS` twins in `handle_fetch_event_from_storage_state` (missing). `Sometimes(carry splice used)` and `Sometimes(encrypted file served)` against vacuity. `Always` because every served event must be consistent; the `Sometimes` checks prove the splice and encrypted paths were exercised. |
| **Antithesis Angle** | Egress never re-validates what it serves (`indexed_event_block.cpp:29-55` TODO). Duplicates (ticket 18), truncate-then-append (N1), two writers (ticket 24), wrong carry splices (`sender_context.cpp:438-524`) and wrong CTR offsets all show up as a broken chain or a CRC failure. The check needs no source access and works for encrypted and rewrite storages. |
| **Why It Matters** | Clients derive resume coordinates from log_pos and detect corruption by CRC. A broken chain means wrong replica coordinates, double-applied transactions or corrupt `--raw` copies, and pull is the only in-product way to read an encrypted storage. |

**Priority:** P0 · **Confidence:** high (the oracle is standard MySQL format) · **Found by:** focus 4 (the wildcard recommends the same source-free header walk as a shared workload helper) · **Fault requirements:** pull process kill (workload), network faults on pull↔S3, CPU throttling; **custom:** `s3_fault_proxy`/`fsfault` for `.json` write failures.

**Evidence:** `properties/pull-served-stream-self-consistent.md`

**Open Questions:**

- Should the per-event CRC check run SUT-side in production builds or only behind an Antithesis/CMake option? `(needs human input)`

### pull-chain-prefix-consistency — What pull serves is a stable prefix of the storage, and the storage a prefix of the source

| | |
|---|---|
| **Type** | Safety |
| **Property** | For each binlog (one source epoch, artificial events stripped, decrypted), the bytes every pull client receives are a prefix of the stored bytes, which are a prefix of the source's. Bytes a client received at `(file, offset)` are never contradicted later, whether by a pull restart, tail recovery or re-fetch, and are never delivered twice across client reconnects. |
| **Invariant** | Workload `Always` messages: "pull client bytes are a prefix of stored bytes", "stored bytes are a prefix of source bytes at observation time", "bytes served by pull never change at the same offset after restart", "pull client never receives the same binlog offset twice across reconnects". These are checked at random times while faults are active. Duplicate delivery after a ticket 18 re-append lands at different offsets, so it is owned by the stored-prefix-of-source check, not by the "same offset twice" check. The "never change at the same offset" check is scoped to non-rewrite storages. Workload `Sometimes("pull client ahead of a recovered storage resumed successfully after re-fetch")` makes N12 an exploration target. SUT-side (missing): `ALWAYS(size(get_object result) == read_length)` in `fetch_event_block`; `ALWAYS(!ended_by_storage_exception || storage_faults_flag, "session ended by storage read exception without storage faults")`, where `storage_faults_flag` comes from the harness timeline-tag file that the proxy and fsfault drivers set (a GET error closes the session fail-closed, and an SUT message cannot be scoped to a template); `SOMETIMES(storage recovery truncated a tail binlog that pull had already served)`; `SOMETIMES(pull client resumed mid-file after reconnect)`. `Always` because one non-prefix or changed byte is a violation whenever observed; the `Sometimes`/`SOMETIMES` checks make the recovered-storage and mid-file resume states exploration targets. |
| **Antithesis Angle** | Independent partitions on source↔PBS, PBS↔S3 and PBS↔client. PBS kills. Clients resuming at `(file, pos)`. Several concurrent clients with small checkpoints. The append, `size +=` and the metadata save all run under the unique lock, so a plain kill cannot expose bytes. A served-then-retracted range needs a failed metadata save (size left incremented, lock released) followed by a kill or exit, or the ticket 18 re-flush. Rewrite-mode regenerated events with wall-clock timestamps can also replace served bytes. |
| **Why It Matters** | Downstream replicas that apply bytes PBS later retracts or duplicates diverge from the source silently. Pull is the only restore path for encrypted storage. This is the core reader/writer contract that `sender_context` (recently rewritten) relies on. |

**Priority:** P1 · **Confidence:** medium (each break route is code-confirmed; the end-to-end chain has not been reproduced) · **Found by:** focus 2, 7, 9 (four drafts merged) · **Fault requirements:** network partitions on all three links, workload kill of pull; **custom:** a metadata-PUT fault (`s3_fault_proxy`) for the retraction route.

**Rationale / conflicts resolved:** `sender-reads-only-flushed-bytes` (focus 2, P1), `served-bytes-stable-across-restart` (focus 9, P2) and `pull-client-reconnect-receives-each-event-once` (focus 9, P1) are all facets of client ⊑ stored ⊑ source over time. The merged property is P1. The merge also corrects focus 9's claim that a SIGKILL between `size +=` and the rename alone can retract served bytes. It cannot, because of the lock.

**Evidence:** `properties/pull-chain-prefix-consistency.md`

**Open Questions:**

- Is regenerating different bytes at an already-served offset acceptable in rewrite mode? `(needs human input)`
- Should the sender serve only metadata-durable sizes? `(needs human input)`
- Is TSan worth adding as an Antithesis image variant? `(needs human input)`
- Is a silent close without an error packet (N12) acceptable for a client ahead of a recovered storage? `(needs human input)`

### chained-fetch-from-pull-reproduces-upstream — A PBS fetch pointed at a PBS pull reproduces the upstream storage byte for byte

| | |
|---|---|
| **Type** | Safety |
| **Property** | A downstream position-mode PBS `fetch` against an upstream PBS `pull` listener exits 0 with each downstream binlog equal to the upstream's flushed (decrypted) bytes, and never rejects the served stream. The downstream may lack a trailing magic-only upstream file, but never skips a magic-only non-tail upstream file. |
| **Invariant** | Workload `Always("downstream file equals upstream file prefix at fetch end, only the tail may be shorter")`. Workload `Always("downstream fetch never exits with a reader_context logic_error")`. The file-equality check excuses only a trailing magic-only upstream file; a magic-only non-tail upstream file (ticket 14 or the source-crash path) that the downstream skips is flagged. Workload `Sometimes(downstream resumed at an upstream tail offset while the upstream next binlog was magic-only)`. Catching N5 needs a downstream PBS `pull` (blocking dump), with `Sometimes("chained downstream pull was at EOF when the upstream created a magic-only next binlog")`: a downstream `fetch` ends at the first EOF and never waits across the switch. `Always` because a single byte mismatch or logic_error in the chain is a violation; the `Sometimes` checks prove the magic-only and EOF-across-switch states were reached. |
| **Antithesis Angle** | It reuses PBS's strict `reader_context` (FSM, position continuity, artificial-ROTATE position, FDE checks) as an oracle on `sender_context`, the least-settled code. Downstream kills exercise start-from-offset at arbitrary offsets. Upstream kills, pauses and S3 faults overlap serving with rotation, checkpoints and recovery. It catches N5, ticket 26, regressions of 21/22, and decryption faults. |
| **Why It Matters** | Pull is the only restore path for encrypted storage. Lenient clients (`mysqlbinlog`) hide framing and file-switch errors that a strict reader exposes. It also tests cascaded backup as a topology. |

**Priority:** P1 · **Confidence:** medium (handshake compatibility traced in code, not executed) · **Found by:** focus 11 · **Fault requirements:** process kill and pause (workload), network faults between containers; a downstream `pull` variant for N5.

**Evidence:** `properties/chained-fetch-from-pull-reproduces-upstream.md`

**Open Questions:**

- Is PBS → PBS chaining a supported topology or only a test device? `(needs human input)`

### dump-session-ends-with-eof-or-err-packet — Unservable dump requests get an ERR packet, not a silent close

| | |
|---|---|
| **Type** | Safety |
| **Property** | When pull's replication source cannot serve a COM_BINLOG_DUMP request (unknown/purged binlog, a name older than the oldest stored file, a binlog not yet created, position past the flushed size, unreadable FDE, storage exception), it sends a MySQL ERR packet before closing. A request pull will serve once the collector catches up on an empty storage (ticket 26 / PBS-96) is a liveness case, owned by `blocking-dump-ahead-of-flushed-position-eventually-served`. |
| **Invariant** | SUT-side (missing) `UNREACHABLE("replication source: dump session closed without ERR or EOF packet")` at `network_service.cpp:312`, plus `UNREACHABLE("replication source: storage exception ended a dump session")` in a new catch around `get_event`. SUT-side placement avoids confusing them with injected network drops. Workload `Sometimes(dump session ended with an ERR packet)` as the positive goal; it accepts any ERR and records the code (1236 expected, not required). Until the SDK is linked, a log-line fallback stands in for the first `UNREACHABLE`: `network_service.cpp:308-312` logs `"net    : failed to fetch next event block for {}"` at error level just before the silent `co_return`, so a workload scan of pull's console log asserts `Unreachable` on that line. `Unreachable` because a silent close or a storage exception ending a session is itself the defect; `Sometimes` because seeing at least one ERR packet proves the error path was exercised. |
| **Antithesis Angle** | The unservable states are timing-produced: a client ahead of a recovery-truncated tail after a pull kill, a request for a binlog not yet created, a request for a name the source purged before PBS's first start, S3 GET faults or short reads. Purge beside a running pull (ticket 9) is out of contract (README:473) and runs only in a misuse phase. |
| **Why It Matters** | A silent close looks like a network blip, so replicas reconnect forever with no visible error and `mysqlbinlog` reports "Lost connection" instead of the real cause. It generalises the error-packet clause of `large-event-served-or-rejected-cleanly`. **Expected to fail today:** every `sender_ctx.get_event()` false return reaches the silent `co_return` at `network_service.cpp:308-313` without an ERR packet, including the "position past the flushed size" branch (`storage_core.cpp:513-515`, N12), which `parallel_driver_pull_reader.py` requests on purpose. Requests ahead of the flushed position are therefore a known-defect trigger. |

**Priority:** P1 · **Confidence:** high (code path verified: `network_service.cpp:305-313`, session `catch (...)` at `:621-623`) · **Found by:** focus 4 (focus 7 and 8 note the same silent close) · **Fault requirements:** pull kill/restart (workload), network faults on pull↔S3; the purged-name case needs the in-contract `pull_fs` retention-cycle driver (stop pull → purge → restart), which the topology regeneration adds.

**Evidence:** `properties/dump-session-ends-with-eof-or-err-packet.md`

**Open Questions:**

- Should a blocking dump wait instead of failing when the client is past a tail truncated by a pull restart, or asks for a binlog not yet created? `(needs human input)`

### blocking-dump-before-first-flush-eventually-served — A blocking client that connects before pull has stored anything is kept open and eventually served (PBS-91 regression)

| | |
|---|---|
| **Type** | Liveness |
| **Property** | A blocking COM_BINLOG_DUMP from `("",4)` or `(<first stored name>,4)` that arrives while the storage is empty, holds only a magic-only first binlog, or while the collector is reconnecting, is not disconnected by PBS and receives the first FDE and transactions once they are flushed. Requests for names older than the source's oldest retained binlog are out of scope (owned by `dump-session-ends-with-eof-or-err-packet`). Latency is unbounded: without checkpointing the first flush happens only on rotation, the `read_timeout` disconnect flush, or shutdown. |
| **Invariant** | Workload `Sometimes(client connected in state S and later received FDE + ≥1 transaction)` for S ∈ {empty, magic-only, collector-reconnecting}, one message per state. Workload `Always("no PBS-initiated close of an early blocking dump session while PBS runs")`. `Sometimes` fits because this is eventual delivery and each state must be shown reachable and served. |
| **Antithesis Angle** | The fix (`storage_core.cpp:463-478`) is tested in one interleaving (`rs_blocking_dump_empty_storage.test`) and by a Hegel model with no real collector. Antithesis varies the client's arrival time against the collector's states (no records; record of size 4 with `binlog_name` left empty, `:523-531`; first flush) and adds source flaps and PBS restarts. |
| **Why It Matters** | A replica or `mysqlbinlog --stop-never` started together with PBS (compose, k8s, after a host reboot) must not depend on start order. |

**Priority:** P1 · **Confidence:** high that the mechanism should work; regression guard · **Found by:** focus 8 · **Fault requirements:** source stop/start, partition PBS↔source, PBS kill (workload), process pause. The workload must trigger a flush in every timeline (`FLUSH BINARY LOGS`, a quiet period longer than `read_timeout`, or checkpointing in some timelines).

**Evidence:** `properties/blocking-dump-before-first-flush-eventually-served.md`

**Open Questions:**

- None

### blocking-dump-ahead-of-flushed-position-eventually-served — A blocking client asking for a position PBS will store later waits instead of being silently disconnected

| | |
|---|---|
| **Type** | Liveness |
| **Property** | A blocking dump from (name, position) that the source has but PBS has not yet flushed is eventually served from that position in the same session. Case 1: empty storage with position > 4. Case 2: the requested binlog is magic-only. Case 3 (position > the flushed size of a binlog that has an FDE) is included only if the owners choose "wait". A name not yet stored (former case 4) moves to `dump-session-ends-with-eof-or-err-packet`. |
| **Invariant** | Workload `Sometimes`, one message per case (the workload labels each attempt from `list` and the source position): "case 1 (empty storage): client later received the event at its requested position in the same session", "case 2 (magic-only binlog): …", and case 3 only if the owners choose wait. Supporting workload `Always("PBS never closes an ahead-of-flushed dump session without events or an ERR packet")`. SUT anchor (missing) `SOMETIMES(dump requested offset beyond flushed size)` at `storage_core.cpp:513-515` (a meaningful condition, not `Sometimes(true)`). `Sometimes` because eventual delivery of an ahead-of-flushed request is a liveness goal that must be observed at least once per case; paired with the `Always` that forbids a silent close on every session. Because a `Sometimes` cannot catch a wedge, a bounded workload `Always("ahead-of-flushed dump session received the event within T after the flush covering its position, in a quiet window")` (cases 1 and 2) turns an indefinite wait into a failure; `Always` because every such session in a quiet window must be served within the bound. |
| **Antithesis Angle** | Ticket 26/PBS-96 (`sender_context.cpp:231-233`, validated) and N12 (`storage_core.cpp:513-515` → `network_service.cpp:308-313`, a silent `co_return`). The outcome depends on when the client arrives relative to PBS's first record, first flush and rotations. PBS restarts and source lag widen the gap. |
| **Why It Matters** | Switchover (pointing a client that read from the source at PBS) and clients reconnecting with saved positions after a PBS restart both hit this. Clients see a dropped connection with no error and retry forever or exit. **Expected to fail today** for cases 1 and 2. PBS-96's suggested fix, read literally, keeps case 2 failing, so the case-2 `Sometimes` doubles as a check on an incomplete fix. |

**Priority:** P1 · **Confidence:** high (mechanism verified, ticket 26 validated) · **Found by:** focus 8 (focus 4 and 7 note the silent close) · **Fault requirements:** none for the base case; PBS restarts and source flaps add timing variety.

**Evidence:** `properties/blocking-dump-ahead-of-flushed-position-eventually-served.md`

**Open Questions:**

- Case 3 (position beyond the flushed size of an existing binlog with an FDE): wait, or ERR 1236 like MySQL? `(needs human input)`
- Is the switchover workflow a supported use case? `(needs human input)`

### misaligned-dump-position-rejected-cleanly — A dump request cannot kill or hang pull (ticket 7)

| | |
|---|---|
| **Type** | Safety |
| **Property** | A COM_BINLOG_DUMP at a position that is not an event boundary, or any other request from any client, ends at most the offending session with an error. It never aborts pull, hangs its io thread or streams garbage, and other sessions and the collector keep running. |
| **Invariant** | Workload `Always` messages: "misaligned dump request rejected without affecting other sessions" (the session receives no non-artificial event, concurrent tailing sessions keep advancing, pull stays alive), and "legit replica not stalled while pull is alive and the source has new events". SUT-side: `ALWAYS(event_size ≥ 19)` replacing the assert at `indexed_event_block.cpp:43` (partial today: aborts in debug, nothing in release); `UNREACHABLE("dump position not on event boundary served")` (missing). `Sometimes(misaligned request while other sessions tail)`, `Sometimes(legit replica progresses after hostile burst)`. `Always` because one hostile request killing pull or stalling replicas is a violation; `UNREACHABLE` because serving a misaligned position is itself the defect; the `Sometimes` checks prove the hostile scenarios ran. |
| **Antithesis Angle** | Hostile authenticated dumps at random positions (never boundary-checked: `sender_context.cpp:52-76`, `storage_core.cpp:436-562`), concurrent with rotation and checkpoints, which shift what "mid-event" means. Misaligned positions also arise naturally from earlier faults: duplicates, truncation, source swap, rewrite re-splitting. |
| **Why It Matters** | In debug/ASan builds an `event_size < 19` header aborts the whole pull, collector included; that is a defect (ticket 7), not accepted fail-stop, so the cross-session `Always` applies to the debug build too. In release, `event_size == 0` loops forever on the single io thread (ticket 7), blocking all sessions and SIGTERM. Any client holding the shared password can trigger it. |

**Priority:** P1 · **Confidence:** high for the mechanism (ticket 7 validated); medium for how often real clients send such positions · **Found by:** focus 4, 6 · **Fault requirements:** none for the direct case; kills and storage faults for the diverged-storage variants.

**Evaluation refinements (2026-10-13):** The direct case (a hostile request at a random offset) is input fuzzing that `listener_fuzz_test.py` already approximates, and in a debug build each firing aborts the whole pull through the `indexed_event_block.cpp:43` assert, which kills the collector and every other session and starves every other pull property in that timeline. The hostile-request driver therefore moves out of `pull_fs` into its own template (or a release-build launch for ticket 7's hang). `pull_fs` keeps the naturally arising variant: misaligned positions produced by earlier duplicates, truncation and rewrite re-splitting.

**Rationale / conflicts resolved:** Focus 4 rated it P1 and focus 6 rated it P2. The merge resolves to P1, because a single authenticated client can take down collection and every replica. Both foci proposed the same `event_size` assertion; it is kept once.

**Evidence:** `properties/misaligned-dump-position-rejected-cleanly.md`

**Open Questions:**

- Should a non-boundary dump position be rejected with ERR 1236, or are authenticated replicas trusted (reliability-only)? `(needs human input)`
- Validate the start position against an event index, or only sanity-check the first header? `(needs human input)`

### dump-preamble-matches-request — Every dump starts with an artificial ROTATE for the requested file and position, then that file's FDE

| | |
|---|---|
| **Type** | Safety |
| **Property** | For every dump session, the first event is an artificial ROTATE (timestamp 0, next_event_position 0) whose binlog and position equal the request (or the oldest indexed file for an empty name), and the second is that file's FDE (artificial with zeroed next_event_position/create_timestamp iff position ≠ 4, valid CRC). |
| **Invariant** | Workload `Always` with three messages: "dump: first event is artificial ROTATE", "dump: artificial ROTATE matches request", "dump: FDE follows ROTATE and matches position mode". `Sometimes` markers for requests on an empty storage, on a magic-only file, and for an empty-name request within one second of a pull restart that followed a purge (pull stopped during the purge). The after-purge marker runs only once `pull_fs` has the retention-cycle driver (stop pull → purge → restart); until then it is deferred, so it does not report as an unmet `Sometimes`. SUT-side `ALWAYS` in `handle_start_states` (missing). `Always` because every dump must start with the correct preamble; the `Sometimes` markers make the empty-storage, magic-only and after-purge cases exploration targets. |
| **Antithesis Angle** | Regression guard for fixed tickets 21 and 22 and open ticket 26, against a storage that is changing: first flush, file creation, rotation, the in-contract stop/purge/restart sequence, pull restart. A purge concurrent with pull (ticket 9 stale index) is out of contract (README:473) and removed from scope. The Hegel spec covers only static storages. |
| **Why It Matters** | `mysqlbinlog --raw` names output files from this ROTATE, and clients reset their parsing from this FDE. A wrong or missing preamble produces misnamed or corrupt local copies. |

**Priority:** P2 · **Confidence:** medium (regression plus race leads, some scenarios unverified) · **Found by:** focus 4 · **Fault requirements:** none beyond workload concurrency; pull kills and a purge task widen the windows.

**Evidence:** `properties/dump-preamble-matches-request.md`

**Open Questions:**

- A magic-only requested file with a later file present (only via ticket 14 or the source-crash path): serve ROTATE(requested), skip, or ERR? `(needs human input)`

### large-event-served-or-rejected-cleanly — Large binlog events are stored and served, or refused with an explicit error, never a silent loop

| | |
|---|---|
| **Type** | Safety |
| **Property** | A large binlog event (≥ 16 MiB, up to the source's `max_allowed_packet`) is ingested and either delivered intact to dump clients or refused with an explicit MySQL error packet. A session is never closed silently in a way that traps a reconnecting client forever. |
| **Invariant** | The workload uses event sizes straddling the exact egress limit (16,777,210, 16,777,211, 16,777,214, 16,777,215, 16,777,216 bytes; 16,777,211 is the first that fails). Workload `Always("dump session spanning a large event ends with the event or an error packet")` and `Always("fetch/pull never exits 0 short of the source after a large event")`. SUT-side (missing) `UNREACHABLE` at the frame-limit throw (`network_io_operations.cpp:144`). Liveness half: `Sometimes(≥ 16 MiB event delivered to a dump client)`, a known failure today (ticket 10). `Always` because a silent loop or false success after any large event is a violation; `UNREACHABLE` because hitting the frame-limit throw is itself the defect; `Sometimes` because delivering a large event is a liveness goal that must be seen at least once. |
| **Antithesis Angle** | Network resets make several clients re-fetch the same big event concurrently. Each session tops up its buffer to the event size (`sender_context.cpp:371-378`), so RSS ≈ N × event. With a PBS memory limit and throttling, OOM interacts with the reconnect loop. |
| **Why It Matters** | Big BLOB rows and bulk statements are routine. Today a replica stops permanently at the first ≥ 16 MiB event with no error, and events above 256 MiB cannot be read back at all (`basic_storage_backend.hpp:35`). |

**Priority:** P2 · **Confidence:** high for egress (ticket 10 validated); medium-high for ingress (libmysqlclient default `max_allowed_packet` is 1 GiB, verified in source, end-to-end only to 20 MB) · **Found by:** focus 5 · **Fault requirements:** none essential; network resets on client links and a container memory limit add the resource angle.

**Evidence:** `properties/large-event-served-or-rejected-cleanly.md`

**Open Questions:**

- None

### rs-unauthenticated-session-never-served — No data or OK to unauthenticated clients

| | |
|---|---|
| **Type** | Safety |
| **Property** | A client that did not present the correct caching_sha2 scramble for the configured user never receives an OK, a fast-auth success, a resultset or a binlog event. |
| **Invariant** | Workload `Always("unauthenticated session receives only AuthMethodSwitch, ERR 1045 or a silent close")`: attacker sessions (wrong password/user, stale-salt scramble, no `plugin_auth`, `mysql_native_password`, truncated greeting, reset mid-switch, dump frame as greeting). SUT-side (missing) `ALWAYS(context.authenticated)` at the top of the command loop and in `handle_binlog_dump_command` (`network_service.cpp:553, 279`), two messages, with a new flag set only at `:457`. `Sometimes(attacker hit auth-switch path)`. `Always` because a single data or OK response to an unauthenticated session is a security violation; `Sometimes` proves the auth-switch attack path was reached. |
| **Antithesis Angle** | Interleaved sessions on the single io thread; drops, reordering and resets mid-handshake. |
| **Why It Matters** | The listener serves decrypted backups over cleartext TCP behind one shared password. It is the only access control on encrypted data. |

**Priority:** P2 · **Confidence:** high that it holds (linear handshake, per-session context, `connection_context.cpp:111-117`) · **Found by:** focus 6 · **Fault requirements:** network faults (drops, resets); none strictly required.

**Evidence:** `properties/rs-unauthenticated-session-never-served.md`

**Open Questions:**

- Is the shared-password cleartext listener the intended production model? `(needs human input)`

### pull-retention-cycle-keeps-serving-survivors — After stop → purge → restart, pull serves surviving binlogs unchanged, serves nothing purged, and resumes collecting

| | |
|---|---|
| **Type** | Safety |
| **Property** | After the in-contract retention cycle (SIGTERM pull, a `purge_binlogs` that reports success, restart pull), pull serves every surviving binlog exactly as it served it before the cycle, serves no event from a purged binlog, and resumes storing new source transactions. |
| **Invariant** | Workload, evaluated only after a purge that reported `success`: `Always("pull restarted after a retention purge serves every surviving binlog identical to what it served before the purge")` (the post-cycle served bytes per surviving file start with the pre-cycle served bytes, artificial events stripped; this covers encrypted storages without an offline decryptor); `Always("no event from a purged binlog is served after a retention cycle")`; `Sometimes("pull stored a new source transaction after a retention cycle")` (liveness, after faults stop); `Sometimes("empty-name dump after a retention purge started at the oldest surviving binlog")`, with mode and encryption in the details. ERR-vs-close and the preamble contents are scored by `dump-session-ends-with-eof-or-err-packet` and `dump-preamble-matches-request`. The purge outcome itself is scored by `interrupted-purge-leaves-openable-storage`. `Always` because each retention cycle must preserve survivors and hide purged data; `Sometimes` because resumed collection and the empty-name start are liveness and state goals that must be observed at least once. |
| **Antithesis Angle** | It exercises pull's streaming constructor and recovery on a freshly purged storage, the sender's empty-name resolution and file switches over a shortened index, and encrypted survivors whose neighbours' `.json` envelopes were just deleted. Antithesis varies the purge target, readers reconnecting at positions inside purged files, source load during the downtime (so the resume has catch-up work), and kills or partitions around the restart. |
| **Why It Matters** | For a 24/7 pull this is the only retention procedure README:473 leaves in contract, and no template runs it (`pull_fs` has no purge driver). A wrong served byte after routine retention breaks PITR from the binlog server. A pull that does not restart takes both backup and serving offline. |

**Priority:** P1 · **Confidence:** medium (paths read in code; the combination has never been run; predicted to pass except on encrypted or N3 leftovers) · **Found by:** evaluation gap G7 (wildcard W7; coverage-balance F9 for the encryption angle) · **Fault requirements:** workload SIGTERM of pull and purge; network faults and workload SIGKILL optional; `fsfault` on `remove`/`rename` optional (those outcomes are scored by the purge property). **Runs in:** needs new driver: `pbs/serial_driver_retention_cycle.py` in `pull_fs`, which captures the pre-cycle served stream, removes `pull.enabled`, SIGTERMs pull, purges, and re-enables pull. It purges only below every downstream client's current file unless the timeline marks chained readers as "purged past". The fetch half is already covered by `fetch_fs/serial_driver_purge.py`.

**Rationale / conflicts resolved:** New. It adds the in-contract pull retention cycle and reuses existing owners rather than duplicating them: the purge outcome stays with `interrupted-purge-leaves-openable-storage`, and ERR-vs-close and the preamble stay with `dump-session-ends-with-eof-or-err-packet` and `dump-preamble-matches-request`. Its driver is the retention-cycle driver (topology change T10), which also feeds the purge anchors of those two properties (R4, R7).

**Evidence:** `properties/pull-retention-cycle-keeps-serving-survivors.md`

**Open Questions:**

- What retention policy toward downstream consumers (lagging chained PBS / `mysqlbinlog`) are operators expected to follow? `(needs human input)`

## Query and purge operations

The administrative CLI modes (`list`, `search_*`, `purge_binlogs`) whose JSON outputs drive PITR and retention automation. They cover tolerant `querying_only` construction, purge's commit-then-delete sequence, and status/exit-code honesty.

### interrupted-purge-leaves-openable-storage — Every purge outcome leaves an openable storage, intact survivors, an honest status, and a retry that converges

| | |
|---|---|
| **Type** | Safety |
| **Property** | After `purge_binlogs` ends with any outcome (`success`, `warning`, `error`, or killed), on a storage with no writer: the storage opens in every mode; the new index is a suffix of the old one that keeps the tail; surviving binlogs and the tail resume point are unchanged; the status is honest; and a retry converges. |
| **Invariant** | Workload `Always` messages: the reopen check is split by outcome into "storage opens in every mode after a purge that reported success or error", "storage opens in every mode after a killed purge" and "storage opens in every mode after a purge that reported warning" (expected to fail by design today: N3); then "purge leaves the tail resume point unchanged", "purge never alters surviving binlog data or metadata", "new index is a suffix of the old index that keeps the tail", "purge error status implies index unchanged", "purge success status implies no victim objects remain", "purge warning status implies index committed", "purge retried after interruption converges to an openable storage" (expected to fail today: the purging-mode constructor rejects orphans), "repeating a completed purge changes no object". Purge runs only on a quiescent storage; purge beside a writer (former scenario 9: N1, ticket 9) is out of contract (README:473) and excluded. SUT-side (missing): `REACHABLE("purge committed the index but object cleanup failed")` at `storage_core.cpp:421`, `REACHABLE("purge index committed")` after `:398`, `REACHABLE("purge: index put_object threw")`, `ALWAYS("purge never selects the tail")`, `SOMETIMES(cleanup_warning_message non-empty)`. Workload vacuity marker `Sometimes("purge interrupted on an encrypted storage")`: purge removes `<name>.json`, which holds the per-file envelope, before the payload, and startup validates every indexed file's KEK. The reopen-after-warning check fails through `kill-anywhere-storage-reopens`'s "interrupted purge" validator message. `Always` because every purge outcome must leave a consistent, openable storage; `REACHABLE`/`SOMETIMES` anchor the rare commit and cleanup-failure branches, and the `Sometimes` marker guards encrypted-purge vacuity. |
| **Antithesis Angle** | A kill of the purge process between the index commit (`:398`) and the deletes. A PBS↔S3 partition during the DELETE batch, which yields `warning`. An ambiguous index PUT (applied, response lost), which is reported as `error` but is committed. On the filesystem, the rename-then-directory-fsync window. Purge started right after a writer crash, which runs purge's own tail recovery. |
| **Why It Matters** | Lead N3: a code comment admits that leftovers "will trip the constructor's validators" (`:391-397`). A routine purge with a transient S3 hiccup takes the backup offline, purge itself cannot repair it (`:779-783, 993-996`), and users are told `warning` only means some files remain. The warning-reopen and retry-converges checks are **expected to fail today**; maintainer comments make "warning ⇒ manual attention" the design, so whether they are bugs or accepted fail-stop is open. Over-deletion or a changed resume point loses data. |

**Priority:** P0 · **Confidence:** high (behaviour stated in code comments and traced) · **Found by:** focus 1, 3, 4, 9 (four drafts merged) · **Fault requirements:** default network faults to S3, or workload kill of purge; **custom:** `fsfault` directory-fsync EIO for the filesystem "error but committed" variant.

**Rationale / conflicts resolved:** Focus 3 rated it P0. Focus 1 (preserve survivors), focus 4 (status honesty) and focus 9 (retry) rated their facets P1. The merge resolves to P0. The wedge is admitted in code, is reachable with default network faults alone, and cannot be repaired by purge.

**Evidence:** `properties/interrupted-purge-leaves-openable-storage.md`

**Open Questions:**

- Is an unopenable storage after purge accepted? `(partial: maintainer comments make warning ⇒ manual attention the design; kill and error cases unaddressed)`
- Should a retry of a killed purge converge? `(needs human input)`
- Does `error` mean nothing changed? `(partial: README lists only pre-commit errors; post-commit failures also report error)`
- Should a filesystem directory-fsync failure after the rename be success-with-warning? `(needs human input)`
- What are the CRT retry/timeout defaults? `(partial: 2-6 attempts empirically; timeouts unknown)`

### query-success-never-omits-middle-binlog — A successful query never silently omits a binlog

| | |
|---|---|
| **Type** | Safety |
| **Property** | A `list`/`search_by_timestamp`/`search_by_gtid_set` response with `status: success` covers every binlog indexed and flushed before the query started. Its files are a contiguous run of that index, so a failed metadata read or a concurrent rename never becomes a silently missing file. |
| **Invariant** | Workload `Always` messages: "list: success output is a contiguous run of the pre-query index", "search_by_timestamp: success output is contiguous", "search_by_gtid_set: success output is contiguous", "search_by_timestamp: success result equals reference selection on a quiescent storage", "query result includes every binlog flushed before it started with size ≥ its pre-query size". SUT-side (missing): `UNREACHABLE("querying_only dropped a binlog record that is not the leading one")` at `storage_core.cpp:946-949`. It is gated on "neither first nor last", or on ".json present in the t0 listing", because a tail `.json` created after t0 is a legitimate drop. `validate_binlog_metadata` runs outside the per-record `try` (`storage_core.cpp:953`), so a validation failure errors out rather than dropping. Also `SOMETIMES(querying ctor dropped the tail record)` and `Sometimes(status error during an S3 fault)`. `Always` because one silently omitted binlog is a violation; `UNREACHABLE` because dropping a non-leading record is itself the defect; the `SOMETIMES`/`Sometimes` anchors show the legitimate drop and S3-fault paths were reached. |
| **Antithesis Angle** | `querying_only` erases any record whose `.json` load throws, or whose `.json` was missing from its t0 listing (`storage_core.cpp:933-950`). It does so silently (`null_logger`). A partition on the query↔S3 link that outlasts CRT GET retries for one key and heals before the next produces a hole without any shim. Purge's delete order (`:400-406`) and writer checkpoint renames racing `readdir` produce other holes. |
| **Why It Matters** | `search_helpers.cpp:64-85` has no contiguity check, so `search_by_timestamp` returns the files around the hole as `success`, and a PITR replay silently skips a whole binlog. `search_by_gtid_set` reports a false "cannot be covered". |

**Priority:** P1 · **Confidence:** high (drop and absence of warning confirmed in code); medium for reachability · **Found by:** focus 2, 4, 11 (three drafts merged) · **Fault requirements:** network faults between the query container and S3 (native); **custom:** per-key GET 5xx shim or `fsfault` EIO on `.json` reads; optional concurrent writer or purge.

**Rationale / conflicts resolved:** Focus 4 rated the read-error variant P0, focus 11 rated the middle-hole variant P1, and focus 2 rated the writer-race variant P2. The merge resolves to P1. The answer is silently wrong, but the stored data is intact, so a re-run answers correctly. Reachability also depends on unverified CRT retry behaviour or filesystem `readdir` semantics. The wildcard's `Sometimes(dropped record is neither first nor last)` describes the same condition as the `UNREACHABLE`. The merge keeps the `UNREACHABLE`, because a success response with a hole is never acceptable.

**Evidence:** `properties/query-success-never-omits-middle-binlog.md`

**Open Questions:**

- Should a hole report `warning` or `error`? `(needs human input)`
- How long must an S3 fault last to get past CRT retries? `(partial: throughput monitor fails a stalled GET after ≥3 s below 1 B/s; retry count unverified)`
- Which filesystem backs the storage volume? `(partial: named volume at /data; host filesystem under Antithesis unknown)`
- Is dropping an already-committed tail acceptable? `(partial: PBS-27 cc49ea5 only motivates a not-yet-committed tail)`

### cli-json-envelope-and-exit-code-agree — list/search/purge answer with one JSON envelope matching the exit code, never crash, and never error spuriously beside a writer

| | |
|---|---|
| **Type** | Safety |
| **Property** | Under any storage state, backend fault or concurrent writer, each `list`, `search_*` and `purge_binlogs` run prints exactly one JSON object with `status ∈ {success, warning, error}` (with a non-empty `message` on error). It exits 0 iff the status is not `error`, never dies by a signal, and never reports `error` for a reason that is only due to concurrency with a writer. |
| **Invariant** | Workload `Always` messages: "cli: stdout is exactly one JSON response", "cli: exit code agrees with JSON status", "cli: error status carries a non-empty message", "query beside an active writer never reports error for a concurrency-only reason". Workload `Unreachable("cli: query or purge died by a signal")`. SUT-side: `ALWAYS("storage ctor: binlog records non-empty before front()")` before `storage_core.cpp:1001` (missing; the ctor assert at `:219` is partial), a top-level `catch (...)` with `UNREACHABLE` (missing), and `SOMETIMES(querying ctor dropped a record)`. `Always` because every CLI invocation must honour the contract; `Unreachable`/`UNREACHABLE` because a signal death or uncaught exception is itself the defect; `SOMETIMES` anchors the record-drop path. |
| **Antithesis Angle** | N4: a writer's `.json.tmp`/`binlog.index.tmp` is renamed between `readdir` and the per-entry stat in `do_list_objects` (`filesystem_storage_backend.cpp:103-117`), which throws; confirmed on the deployed libc++ toolchain (`debug_clang20`), whose `directory_entry::file_size()` re-stats. The crash triggers are every metadata load failing under S3 faults, and an empty `binlog.index` (ticket 13), which needs a workload damage step. The former trigger (a), rotation plus purge between the query's listing and index read, needs purge beside a writer and is dropped as out of contract (README:473). `list` on an empty or index-less storage prints `success` with `[]` (README silent); `search_*` return `error`. |
| **Why It Matters** | Querying beside a writer is an explicit design intent (`storage_core.cpp:189-195`). Retention and PITR scripts parse stdout and branch on the exit code. A crash, no JSON, or a status/exit mismatch breaks automation, and a failed PITR search during an incident is user-visible. |

**Priority:** P1 · **Confidence:** high for N4 (libc++ re-stat confirmed); medium for the crash paths · **Found by:** focus 2, 4 · **Fault requirements:** workload concurrency (writer, purge), writer kills, S3 network faults; **custom:** optional `fsfault`.

**Rationale / conflicts resolved:** Focus 2 rated it P1 and focus 4 rated it P2. The merge resolves to P1, because querying beside a writer is a design intent. This property owns the empty-records site at `:1001`. `storage-format-version-mismatch-handled` references that site instead of adding a second assertion.

**Evidence:** `properties/cli-json-envelope-and-exit-code-agree.md`

**Open Questions:**

- Should `warning` exit 0? `(needs human input)`

### query-modes-never-write-storage — Query modes never modify the storage

| | |
|---|---|
| **Type** | Safety |
| **Property** | `list`, `search_by_timestamp` and `search_by_gtid_set` never write any storage object, regardless of the encryption section in their config. |
| **Invariant** | SUT `ALWAYS(construction_mode_ == streaming)` in `storage_core::save_metadata` (missing). Workload `Always("metadata.json unchanged across a query-mode invocation")`. `Always` because a single write from a query mode is a violation every time it happens. |
| **Antithesis Angle** | The path is reachable only with an "encryption-added" query config (encryption section plus a keyring holding the `kek_id`) against a storage initialised without encryption, so the workload must sometimes run `list`/`search_*` with that variant; `Sometimes("query ran with encryption config against an unencrypted storage")` guards against vacuity. Query invocations race a live writer and each other. On the filesystem they collide on the deterministic `metadata.json.tmp`; on S3 it is last-writer-wins. |
| **Why It Matters** | Code-confirmed: `storage_core.cpp:174-178` calls `save_metadata()` in every construction mode when the config enables encryption on an unencrypted storage. A shared config file makes this the normal path for operators enabling encryption. Evidence suggests an oversight: commit 3e812f2 gated the neighbouring fresh-storage write on `streaming` but not this one (lead (e) in "New defect leads"). |

**Priority:** P2 · **Confidence:** high (mechanism); impact low to medium · **Found by:** focus 10 (independently by focus 8) · **Fault requirements:** none (concurrency only).

**Evidence:** `properties/query-modes-never-write-storage.md`

**Open Questions:**

- Is the `metadata.json` encryption upgrade meant for any mode, or only the writer? `(needs human input)`

### search-by-timestamp-includes-all-early-files — search_by_timestamp returns every file with an event at or before t

| | |
|---|---|
| **Type** | Safety |
| **Property** | `search_by_timestamp t` returns, in index order, every stored binlog whose `min_timestamp ≤ t`, as README documents, even when timestamps are not monotonic across files. |
| **Invariant** | Workload `Always("search_by_timestamp: every file with an event at or before t is returned")` against a `list` snapshot of a quiescent storage. `Sometimes(list shows a record with min_timestamp lower than its predecessor's)` so the case is reached. SUT-side `SOMETIMES` marker for that condition in `select_records_by_timestamp` (missing). `Always` because one missing early file is a violation; `Sometimes` because the non-monotonic-timestamp precondition is rare and must be shown reached so the check is not vacuous. |
| **Antithesis Angle** | The implementation stops at the first file with `min_timestamp > t` (`search_helpers.cpp:75-77`). Non-monotonic timestamps come from source clock steps, `SET TIMESTAMP`, rewrite-mode wall-clock stamps (N11) and source swaps. Clock faults produce realistic skews. |
| **Why It Matters** | A PITR plan that omits a file containing events before t restores less than requested, and does so silently. The implemented leading run is a superset of what a `mysqlbinlog --stop-datetime=t` replay reads (it stops the whole multi-file replay at the first event ≥ t), so under that restore procedure the implementation suffices and the README over-promises; the intended contract decides which is right. |

**Priority:** P2 (lowest; the investigation recommends low because the violation is deterministic via `SET TIMESTAMP` and an MTR test shows it more cheaply) · **Confidence:** low-medium (code and README disagree; intended semantics unclear) · **Found by:** focus 4 (focus 11 recorded a dissenting negative result) · **Fault requirements:** none for the `SET TIMESTAMP` variant; **clock** faults on source/PBS for the realistic variant.

**Evidence:** `properties/search-by-timestamp-includes-all-early-files.md`

**Open Questions:**

- Which semantics is the contract: README's "every file with an event ≤ t", or the implemented leading run? `(needs human input)`
- Should rewrite mode stamp generated events with source time? `(needs human input)`

## Encryption at rest

AES-CTR encryption of binlog data with per-file envelopes. These properties cover decryptability after crashes, keystream/offset alignment, plaintext never reaching rest once a storage is encrypted, and storages whose encryption settings change across restarts.

### encrypted-storage-decrypts-to-source — Encrypted storage decrypts to the source's bytes after any crash

| | |
|---|---|
| **Type** | Safety |
| **Property** | For an encrypted non-rewrite storage, after any kill, storage fault or truncation recovery, every indexed binlog decrypts with its persisted envelope and the keyring to the source's binlog as sent: magic, a valid event chain and valid CRCs up to its metadata size. Every encrypted write is encrypted for exactly the physical offset at which it lands. |
| **Invariant** | Workload `Always` messages: "independent decryption yields magic plus a valid event chain and CRCs up to metadata size" and "independently decrypted binlog equals the source binlog as sent". The independent Python decryptor (envelope + AES-CTR, about 30 lines, AES KEKs only) is the canonical encrypted reader for every storage-integrity oracle in the catalog. Workload `Always("pull plaintext of each encrypted binlog equals the source binlog as sent")` (per-file non-blocking dumps from position 4, artificial events stripped) is kept as the SG9 serving check. It runs in `pull_fs`, where pull is already the writer. On `fetch_fs` and `s3` storages it runs only at quiescent points on a snapshot copy, with `connection.host` pointing at an unreachable endpoint, because pull always runs storage recovery and the collector (`pull_operation.cpp:108-153`). SUT-side (missing): `ALWAYS("encryption offset equals backend physical stream size before write")` in `storage_core::write_data_to_stream` (`:1125-1160`; needs a stream-size accessor), evaluated only while no write has failed on that stream, with a twin `SOMETIMES` anchoring the exempted after-failure path (the self-healing ticket-18 S3 retry), and carrying the harness timeline tag in its message because `contention` timelines (N1, ticket 24) reach it; optional `UNREACHABLE("encrypted binlog reopened with an empty data object")` at `:713` as a regression guard; `ALWAYS("decrypted block at offset 0 starts with binlog magic")` and `ALWAYS("decrypted block at offset 4 starts with a valid FDE header")` in `fetch_event_block` (`:544-557`). `Always` because any decryption mismatch at any check is silent corruption; the `SOMETIMES` twin anchors the exempted after-failure path, and `UNREACHABLE` because reopening with an empty data object is itself the defect. |
| **Antithesis Angle** | Kill mid-append, then truncation at a non-block-aligned offset and a CTR re-append. Kill inside rotation: the envelope exists only in memory until `<name>.json` is PUT (`:680-703`). An S3 failed upload followed by the destructor retry (ticket 18). S3 `resize_object`. The reopen-at-offset-0 magic rewrite (`:715`) is unreachable today and is no longer a trigger. A concurrent purge truncating a live tail (N1) and two writers (ticket 24) run only in a separate misuse phase, read as fail-closed. Pull comparison dumps each file separately from position 4, which keeps N5 out; the workload keeps old KEKs in the keyring and also decrypts independently. |
| **Why It Matters** | Encryption has never been crash-tested. Pull is the only in-product way to restore an encrypted storage. A keystream/offset bug makes the backup unreadable while every check on the ciphertext passes, and it may feed ticket 7's `event_size` hang on the sender. |

**Priority:** P1 · **Confidence:** medium (holds by code reading on in-contract paths; never crash-tested) · **Found by:** focus 1, 6 (three drafts merged) · **Fault requirements:** workload kill; **custom:** `s3_fault_proxy`/`fsfault` on `.json` PUTs; concurrent purge/fetch processes for the misuse phase only.

**Rationale / conflicts resolved:** `cipher-offset-equals-physical-offset` (focus 6, P2) proposed the same offset assertion as focus 1, and is kept once. `encrypted-binlog-decryptable-after-crash` (focus 6, P1) is the independent-decryption half of the same invariant. The merged property is P1. The investigation resolved the offset `ALWAYS` placement: it exempts writes after a failed write on the same stream, because the ticket-18 S3 retry is a transient, self-healing mismatch that leaves no readable byte wrong.

**Evidence:** `properties/encrypted-storage-decrypts-to-source.md`

**Open Questions:**

- Are two concurrent fetch/pull writers on one storage (ticket 24) in contract? `(needs human input)`

### encrypted-storage-no-plaintext-at-rest — Once a storage is encrypted, no new binlog content is written in plaintext

| | |
|---|---|
| **Type** | Safety |
| **Property** | Every binlog created while a KEK is configured has an encryption envelope and ciphertext data, and content committed while encryption is configured never appears in plaintext in any at-rest location (storage objects, the S3 local buffer, PBS logs) except, pending the open question, in a pre-existing plaintext tail. Disabling encryption afterwards is accepted by design (`encryption.test:100-110`) and is out of scope. |
| **Invariant** | Workload `Always` messages, keyed on the config in force when each binlog was created: "no per-transaction marker committed while encryption was configured appears in plaintext at rest", and "every binlog created while a KEK was configured has an envelope and ciphertext data". Workload vacuity marker `Sometimes("writer restarted after encryption was enabled on an existing storage")`, because the trigger is a config transition that only the config-mutation driver produces. SUT-side (missing): `ALWAYS(!has_active_kek() || encryption_record, "new binlog created while a KEK is configured has an envelope")` in `open_new_binlog_file_internal` (`storage_core.cpp:679-702`). It replaces the earlier `write_data_to_stream` assertion, which fired on the maintainers' own tested disable flow. `Always` because one plaintext leak is a violation whenever it is checked; the `Sometimes` marker guards vacuity by proving the encryption-enable transition happened. |
| **Antithesis Angle** | Restarts and kills at arbitrary points (mid-transaction, around rotation, inside the constructor's `save_metadata` at `:177`), with encryption toggled between runs. The plaintext window depends on when the source rotates relative to the toggle. S3 local-buffer leaks on SIGKILL multiply the at-rest locations. |
| **Why It Matters** | README:673 claims data files are encrypted before they are written. N9 (code-confirmed): enabling encryption keeps appending plaintext to the existing tail (`:172-178`). The per-file envelope design and MTR `encryption.test` imply per-file semantics, while README:674 says all files are encrypted. Dropping the `encryption` section, which writes new files in plaintext, is by design and no longer counted. |

**Priority:** P1 · **Confidence:** high (mechanism); whether it is a bug depends on intent · **Found by:** focus 1, 6 · **Fault requirements:** workload kill/restart with config variation; no network faults needed.

**Rationale / conflicts resolved:** Focus 6 rated it P1 and focus 1 rated the envelope half P2. The merge resolves to P1, because the exposure is silent and contradicts README:673. `mixed-encryption-storage-served-correctly` checks the readability of the same transitions. Scope is now the forward direction (new files after enablement). If the owners say the plaintext tail is intended, the marker check narrows to "after the first rotation after enablement".

**Evidence:** `properties/encrypted-storage-no-plaintext-at-rest.md`

**Open Questions:**

- Is the plaintext tail after enabling encryption intended? `(partial: per-file envelope design + MTR encryption.test imply per-file semantics; README:674 says all files encrypted; live-tail position unknown)`

### mixed-encryption-storage-served-correctly — Pull serves the exact source stream across restarts that change encryption settings

| | |
|---|---|
| **Type** | Safety |
| **Property** | A storage may be written across restarts whose configs switch between no encryption, KEK A with cipher X, and KEK B with cipher Y, with all KEKs kept, interleaved with kills. Pull serves it as exactly the source's events for every file, and each file's on-disk form matches its own `.json` envelope (plaintext iff no envelope). |
| **Invariant** | Workload `Always` messages: "pull stream minus artificial events equals source bytes across encryption config changes", "file starts with plaintext magic iff its .json has no encryption envelope". `Sometimes(one session crossed an encrypted↔plaintext file boundary)`, `Sometimes(an encrypted tail was appended to under a different active KEK)`, and the vacuity marker `Sometimes("writer restarted after the encryption config changed")` (the config-mutation driver produces the transitions). The property runs in `pull_fs`, where pull is the writer, so reading through pull adds no second writer. SUT-side (missing): `SOMETIMES(per-file envelope presence differs from metadata.json encryption presence)` and `SOMETIMES(tail encryption differs from current active KEK)` in `open_binlog`. The decrypted-magic/FDE checks belong to `encrypted-storage-decrypts-to-source`. `Always` because a single wrong served byte across a config change is a violation; the `Sometimes`/`SOMETIMES` checks prove the mixed-encryption transitions were reached. |
| **Antithesis Angle** | An append reuses the tail's own envelope (`storage_core.cpp:307-309`), new files take the current config's envelope, and decryption is per record (`:544-557`). Every indexed file's KEK is validated at open (`storage_core.cpp:899-903, 927-953`), so the workload keeps all KEKs in the keyring. `encryption.test` changes the config only after `FLUSH BINARY LOGS`, never kills PBS and never reads through pull. Antithesis adds kills inside `open_new_binlog_file_internal`, truncation recovery on an encrypted tail, metadata-PUT failures, and concurrent clients across encrypted/plaintext and 1 MiB block boundaries. |
| **Why It Matters** | Pull is the only in-product plaintext read-back path. An encryption config change is an operator action that no test combines with an existing storage under faults. |

**Priority:** P1 · **Confidence:** medium (paths verified; no concrete defect found) · **Found by:** focus 8, 10 · **Fault requirements:** workload kill or pause with config variation; **custom:** S3 faults on the metadata PUT.

**Rationale / conflicts resolved:** Focus 8 rated it P1 and focus 10 rated it P2. The merge resolves to P1, because no existing test reads a mixed storage back through pull under faults.

**Evidence:** `properties/mixed-encryption-storage-served-correctly.md`

**Open Questions:**

- Is per-file mixing intended (N9), given that `encryption.test` appends plaintext to a plaintext tail while encryption is enabled but README:674 says all files are encrypted? `(needs human input)`

### ctr-keystream-not-reused-across-truncation — CTR keystream never reused for different plaintext

| | |
|---|---|
| **Type** | Safety |
| **Property** | For a given binlog envelope (file key + IV), the ciphertext byte at any offset never changes value; a change means two different plaintexts were encrypted under the same keystream. |
| **Invariant** | Workload `Always` with two messages so triage separates the shapes: "keystream reused for different plaintext (rewrite rotate overlap)" and "keystream reused for different plaintext (transaction body overlap)". Across snapshots of each encrypted binlog keyed by `(name, iv_hex)`, overlapping byte ranges are equal. Old ciphertext is observed through leaked `/data/s3buf` copies (S3) and a pre-restart snapshot (filesystem); Versity has no versioning. The filesystem snapshot must capture the bytes beyond the `.json` size before the next opener truncates them: in `fetch_fs` the writer's own driver takes it right after its child exits and before it releases the `flock`; in `pull_fs` the supervisor takes it before restarting pull. The S3 observation depends on the N10 leak (`s3-crash-leftovers-bounded`); once N10 is fixed, a workload GET of the object before the restart replaces it. SUT-side (missing) `SOMETIMES(recovery truncated an encrypted tail)` at `storage_core.cpp:979` confirms the precondition. `Always` because one keystream reuse is a confidentiality break whenever detected; `SOMETIMES` confirms the truncation precondition was reached. |
| **Antithesis Angle** | Kill between the data append and the metadata save in `write_event_block`; restart truncates the tail and re-encrypts new bytes at the same offsets with the reused envelope (`:307`, `:715`). In rewrite mode the first transaction after any reconnect lands at the truncation point in the reused file: with `checkpoint_size` ≤ one transaction, the next GTID event is encrypted where ROTATE(T1) was (about 50 bytes of overlap, shape A); with larger or no checkpointing, only the ROTATE's timestamp and CRC differ (shape B). A source RESET or failover with the same binlog names (ticket 25) overlaps whole transaction bodies. |
| **Why It Matters** | AES-CTR nonce reuse leaks P1 ⊕ P2. **Expected to fail today in rewrite mode** (lead (d) in "New defect leads"): shape A recovers GTID-event metadata (source UUID, GNO, commit timestamps), not row data; the source-swap shape leaks row data. Old ciphertext persists in PBS's own leaked local-buffer copies (N10) and in backups. Whether a firing is a bug depends on the threat model. |

**Priority:** P2 · **Confidence:** high for rewrite-mode reuse (code reading); not executed · **Found by:** focus 6 · **Fault requirements:** process kill (workload); rewrite mode with small `checkpoint_size` (shape A) and larger or absent checkpointing (shape B); workload-driven source RESET/failover for the non-rewrite variant. No bucket versioning needed.

**Evidence:** `properties/ctr-keystream-not-reused-across-truncation.md`

**Open Questions:**

- Is "old ciphertext is unavailable to an attacker" part of the threat model? `(needs human input)`

## Rewrite mode

GTID-mode re-splitting into PBS-named files with regenerated ROTATE/FDE/PREVIOUS_GTIDS events and recomputed positions and CRCs. There is no byte oracle against the source, local rotation takes two steps, generated events carry wall-clock timestamps, and the settings are not persisted.

### stored-rotate-or-stop-is-last-event — No stored binlog has events after its terminal ROTATE/STOP

| | |
|---|---|
| **Type** | Safety |
| **Property** | In every stored binlog, a non-artificial ROTATE or STOP event, if present, is the last event. Resume never appends to a binlog that is already terminated. |
| **Invariant** | Workload `Always("every stored binlog has at most one terminator and nothing follows it")`, checked after each PBS exit and at convergence. It is cheap, runs in all modes, and decrypts with the independent Python decryptor if the storage is encrypted (not through pull, which would run recovery and append on the storage under test). SUT-side (missing): `ALWAYS_OR_UNREACHABLE(!tail_already_terminated, "resume opens a tail binlog that is not already terminated by ROTATE/STOP")` in `open_binlog`'s existing-binlog branch, which needs a persisted closed flag or a last-event read. Anchors: `REACHABLE("rewrite: terminal ROTATE flushed, next file not yet created")` and `REACHABLE("rewrite: resumed into the current tail after restart")` at `collector_context.cpp:370`. `Always` because one event after a terminator is a violation; `ALWAYS_OR_UNREACHABLE` because the resume-into-existing-tail branch is workload-dependent but must hold when taken; the `REACHABLE` markers are pure exploration anchors. |
| **Antithesis Angle** | In rewrite mode, a local rotation is two steps. First N is flushed with its generated ROTATE (`storage.cpp:204-215`), then N+1 is created (`storage_core.cpp:680-703`). The process may be killed in between, or creating N+1 may fail; on S3 a failed or stalled magic PUT is enough, with no kill needed. The rotation entry condition is `is_fresh() || (boundary && size >= file_size)` (`collector_context.cpp:352-353`); on restart the `is_fresh()` branch (`:367-370`) takes the current tail name without checking its size or last event, and reopens the terminated N for append (`storage_core.cpp:268-291`). No restart guard exists. Nothing at open parses data (`:785`). A tiny `rewrite.file_size` with S3 partitions finds it. |
| **Why It Matters** | Readers treat ROTATE as end of file, so a `mysqlbinlog --raw` restore or a replica fed by pull misattributes or skips the trailing transactions. That is silent loss on restore, and PBS never notices because the rewriter keeps positions and CRCs consistent. Expected to fail today once the window is hit (no guard, confirmed by code trace). |

**Priority:** P0 · **Confidence:** medium-high (no restart guard, confirmed by code trace through `collector_context.cpp:352-427`, `reader_context.cpp:579-582`; not reproduced) · **Found by:** focus 1, 3 (independently) · **Fault requirements:** default network faults to S3 (S3 path, no kill needed); workload kill for the filesystem path; rewrite mode with a small `file_size`.

**Rationale / conflicts resolved:** Focus 1 rated it P0 and focus 3 rated it P1. The merge resolves to P0 for three reasons. Two foci found it independently. The outcome is silent loss on restore. On S3, only a failed PUT under default network faults is needed. The investigation confirmed by code trace that no restart guard exists; an S3-proxy reproduction is still pending.

**Evidence:** `properties/stored-rotate-or-stop-is-last-event.md`

**Open Questions:**

- Does anything downstream reject the file? `(partial: pull serves it unchecked; mysqlbinlog behaviour outside repo)`
- Can non-rewrite modes reach it? `(partial: PBS appends nothing itself; source dump semantics outside repo)`
- Which fix is intended (rotate on restart when tail size ≥ `file_size`, or persist a closed flag)? `(needs human input)`

### rewrite-binlog-chain-valid-across-crashes — Rewritten binlogs stay internally valid across crashes and source upgrades

| | |
|---|---|
| **Type** | Safety |
| **Property** | In rewrite mode every stored binlog is valid and self-consistent: magic, FDE, and a PREVIOUS_GTIDS event equal to metadata `previous_gtids`; a valid `next_event_position` chain and CRCs; GTID `sequence_number`s 1..k contiguous with `.json` `last_sequence_number` = k; matching `transaction_length`; and every event type and post-header length described by that file's own FDE. |
| **Invariant** | Workload `Always` messages: "rewritten binlog has a valid position chain and CRCs", "rewritten sequence numbers are contiguous and match last_sequence_number", "rewritten last_committed preserves the source dependency distance" (exact `expected_lc(seq, src_seq, src_lc)`, pairing each rewritten GTID with its source GTID event), "every event in a rewritten file is described by that file's FDE", "rewritten file FDE server version covers every GTID_TAGGED_LOG it contains", "mysqlbinlog --verify-binlog-checksum accepts every rewritten file". The oracle must not assert `size <= file_size` (a file legitimately exceeds it by one transaction, two after a reconnect). Encrypted rewrite storages are included, decrypted offline with the keyring. SUT-side (missing): `ALWAYS("rewritten sequence_number equals last plus one")`, and `ALWAYS_OR_UNREACHABLE("rewritten event type known in the current file's FDE version")` before writing a non-preamble event in `rewrite_and_process_binlog_event`. The `SOMETIMES(rewrite resumed into non-empty file after restart)` anchor is shared with `recovery-paths-exercised`. `Always` because one invalid rewritten event breaks replay whenever it exists; `ALWAYS_OR_UNREACHABLE` because the non-preamble event check may go unexercised but must hold when it runs. |
| **Antithesis Angle** | Kills at arbitrary points around checkpoints, and disconnects inside multi-event transactions. MTR covers one hand-built disconnect, and rewrite has never been crash-tested. The upgrade variant: generated FDEs use the connection version from when the file was created (`event_generation_helpers.cpp:120-131`), and source FDEs are skipped before the reader sees them (`collector_context.cpp:290-334`). An 8.0-era file reused after reconnecting to an upgraded source can therefore receive GTID_TAGGED_LOG events it does not describe. |
| **Why It Matters** | Rewrite output cannot be compared with the source byte for byte, so internal validity is its integrity check. A gap or duplicate in sequence numbers, a bad chain, or undescribed events break parallel replication and restore tools. |

**Priority:** P1 · **Confidence:** medium (upgrade variant medium-low) · **Found by:** focus 1, 10 · **Fault requirements:** workload kill, network faults; rewrite mode with a small `file_size` and checkpoints; **custom:** workload mysqld binary swap for the upgrade variant.

**Rationale / conflicts resolved:** Focus 1 rated it P1 and focus 10 rated the FDE variant P2. The merged property is P1. Payload equality is a separate property, `rewrite-storage-transaction-sequence-matches-source`.

**Evidence:** `properties/rewrite-binlog-chain-valid-across-crashes.md`

**Open Questions:**

- Does rewrite mode support an in-place source upgrade? `(needs human input)`

### rewrite-storage-transaction-sequence-matches-source — A rewritten storage holds exactly the source's transactions, in order, with identical payloads

| | |
|---|---|
| **Type** | Safety |
| **Property** | After any sequence of kills, partitions and source rotations, the transaction sequence (GTID plus event bodies, excluding headers, footers and the logical-clock and length fields) parsed from a rewrite-mode storage equals the source's sequence after the storage's initial purged set. |
| **Invariant** | Workload `Always("rewritten transaction sequence equals source sequence")` (the oracle decodes tagged-GTID varint fields and compares common-header timestamps, which are copied unchanged), `Always("no source transaction spans two rewritten files")`, and `Always("rewritten logical clock valid: last_committed < sequence_number, strictly increasing per file")`. SUT-side (missing) `ALWAYS("rewrite preserves the size of non-GTID events")` in `rewrite_and_process_binlog_event`, which the `transaction_length` logic depends on; it is scoped to non-GTID events because GTID_TAGGED_LOG re-encodes varints and legitimately changes size. A mid-transaction resume fails closed (the reader requires a GTID event first), so it needs no oracle special case. `Always` because any sequence or payload divergence is a violation at every check. |
| **Antithesis Angle** | Rewrite has no byte oracle and has never been crash-tested. GTID-only resume does not verify that what the source skipped equals what was stored. Small `file_size`, frequent source `FLUSH BINARY LOGS`, kills across preamble generation and checkpoints, tagged GTIDs, large transactions. |
| **Why It Matters** | `rewrite-binlog-chain-valid-across-crashes` checks internal validity only. A storage can be internally valid and still be missing or altering transactions, and the recomputed CRCs hide payload corruption. |

**Priority:** P1 · **Confidence:** medium · **Found by:** focus 11 · **Fault requirements:** process kill (workload), network partition.

**Evidence:** `properties/rewrite-storage-transaction-sequence-matches-source.md`

**Open Questions:**

- None

### rewrite-pitr-by-datetime-matches-source — PITR by datetime over a rewritten storage selects the same transactions as over the source

| | |
|---|---|
| **Type** | Safety |
| **Property** | For any T, `mysqlbinlog --stop-datetime=T` (and `--start-datetime`) over a rewrite-mode storage outputs the same GTID set as over the source's binlogs. |
| **Invariant** | Workload `Always`, one message per direction: "rewrite-storage PITR stop-datetime GTID set equals source" and "rewrite-storage PITR start-datetime GTID set equals source", evaluated at quiescence for sampled whole-second T values inside source timestamp gaps, including T inside lag windows of ≥ 2 s. The oracle runs `mysqlbinlog` in non-raw mode (`--stop-datetime` is ignored with `--raw`) and extracts GTIDs from `SET @@SESSION.GTID_NEXT`; non-rewrite storages are the control. `mysqlbinlog` reads T in the client's local time zone, so the oracle pins `TZ=UTC` for both the computation of T and the `mysqlbinlog` call. SUT-side (missing) `Sometimes(generated preamble timestamp > first source event timestamp in the file)` in `rewrite_and_process_binlog_event`. `Always` because one wrong PITR selection is a restore error; `Sometimes` because the preamble-timestamp-ahead state is rare and must be shown reached so the check is not vacuous. |
| **Antithesis Angle** | Generated FDE, PREVIOUS_GTIDS and closing ROTATE carry PBS `now()` (`event_generation_helpers.cpp:48, 92, 117, 149`) and are stored. Any lag (partition, kill/restart, S3 stall) makes every file created during catch-up start with "future" events. `mysqlbinlog --stop-datetime` exempts no event type (same in 8.0/8.4/9.6/9.7.1), so it stops at them; `--start-datetime` switches printing on at the first PREVIOUS_GTIDS(now) ≥ T and returns extra, older transactions. Lag alone is enough; forward clock jumps make it worse. |
| **Why It Matters** | Datetime PITR is the main restore workflow, and it silently loses transactions committed before T (or replays extra ones). This is N11 made concrete. **Expected to fail today** on any catch-up lag ≥ 1 s (lead (c) in "New defect leads"); confirmed by code reading on both sides, not executed. |

**Priority:** P1 · **Confidence:** high (PBS generation and mysqlbinlog's stop rule both read in source; not executed) · **Found by:** focus 11 · **Fault requirements:** network partition or process kill; **clock** jitter optional.

**Evidence:** `properties/rewrite-pitr-by-datetime-matches-source.md`

**Open Questions:**

- Is stamping generated events with PBS's wall clock intentional? `(needs human input)`

### rewrite-toggle-between-runs-keeps-storage-resumable — Changing replication mode, rewrite or base_file_name between runs is either refused at start-up or leaves a resumable storage

| | |
|---|---|
| **Type** | Safety |
| **Property** | After any change to `replication.mode`, the presence of `rewrite`, `rewrite.base_file_name` or `rewrite.file_size` between runs, a start either exits non-zero before modifying any non-`.tmp` object, or it streams and every later run (with the new or the original config) keeps converging. |
| **Invariant** | Workload `Always("mode mismatch start exits non-zero with binlog objects unchanged")`, `Always("every start after a config toggle refuses with a config error or makes progress")` and `Always("no stored ROTATE event names a binlog that sorts at or before the file that contains it")`. The failure signature is no progress across repeated restarts (N consecutive restarts with unchanged config, each non-zero with no change to `binlog.index` or the tail size), not an error-string match. Workload vacuity marker `Sometimes("writer restarted after rewrite, mode or base_file_name changed")`, because the trigger exists only when the config-mutation driver changes a dimension between runs. SUT-side (missing) `UNREACHABLE("writer asked to append to a non-tail binlog")` at `storage_core.cpp:275-278`. `Always` because each toggled start must refuse or progress; the `Sometimes` marker guards vacuity of the config transition, and `UNREACHABLE` because appending to a non-tail binlog is itself the defect. |
| **Antithesis Angle** | `rewrite` and `base_file_name` are not stored. Turning rewrite on → off in GTID mode makes the source resume at an older source file name that the rewriter already created, so `open_binlog` throws on every restart (`collector_context.cpp:352-374`, `:555-557`, `storage_core.cpp:268-279`). The wedge comes from the existence check in `storage_core::open_binlog`, independent of ticket 14 and of any ticket 14 fix. Changing `base_file_name` on a non-empty storage is silently ignored. Antithesis combines toggles with kills just after a rewritten file is created. |
| **Why It Matters** | A routine config edit wedges the backup writer permanently, with no hint that the config is at fault (lead N11). Mixed base names also stop purge (`storage_core.cpp:339-344`). |

**Priority:** P2 · **Confidence:** high for the wedge mechanism (traced, not reproduced); low for whether toggling is supported · **Found by:** focus 8 · **Fault requirements:** none for the base case (config-variation workload action); process kill enriches combinations.

**Evidence:** `properties/rewrite-toggle-between-runs-keeps-storage-resumable.md`

**Open Questions:**

- Is toggling `rewrite` on an existing storage supported? `(needs human input)`
- Should a `base_file_name` change on a non-empty storage be ignored or rejected? `(needs human input)`

### rewrite-checksum-none-fail-stop — Rewrite mode refuses checksum-less files without corrupting storage

| | |
|---|---|
| **Type** | Safety |
| **Property** | When the source switches to `binlog_checksum=NONE` mid-stream, rewrite-mode fetch/pull stops with an error, stores no event from the NONE file, and leaves a storage that opens, has a CRC on every stored event, and is unchanged except for whole transactions. |
| **Invariant** | Workload `Always("storage opens after rewrite checksum fail-stop")`, `Always("all stored rewrite events carry a valid CRC32")`, `Always("stored GTIDs are a subset of CRC32-era source GTIDs")`. SUT-side (missing) `Reachable("rewrite rejected checksum-less FDE mid-stream")` at `collector_context.cpp:318`. `Always` because the storage must stay valid after every fail-stop; `Reachable` because hitting the rejection branch is a pure exploration anchor. |
| **Antithesis Angle** | Toggle timing relative to buffered and unflushed events, checkpoints and PBS kills. In pull, the failure also ends the replication source. Antithesis explores the destructor flush after the exception. |
| **Why It Matters** | MTR checks only the deterministic pre-fetch toggle. A mid-stream toggle forces a permanent fail-stop loop, which is the documented contract (README:593 plus MTR), and the storage must survive every restart attempt intact. |

**Priority:** P2 · **Confidence:** medium · **Found by:** focus 10 · **Fault requirements:** process kill (workload); workload-driven `SET GLOBAL binlog_checksum`.

**Evidence:** `properties/rewrite-checksum-none-fail-stop.md`

**Open Questions:**

- None

## Source-side changes and version compatibility

Changes on the upstream side and across builds: source crash, swap, reset or failover; upgrades and tagged GTIDs; source authentication and TLS; storages written by other PBS builds; and client versions against pull.

### source-swap-never-silently-accepted — A swapped or reset source is never silently continued

| | |
|---|---|
| **Type** | Safety |
| **Property** | After the source is replaced by a server with unrelated history, or the source's binlogs are reset, fetch never exits 0 and pull never appends bytes to the existing storage. |
| **Invariant** | Workload `Always("no fetch exit 0 or pull append after a source epoch change")`, evaluated at every fetch exit and at periodic pull checks. The workload tracks the source epoch, because PBS records no source identity. `Always` fits because it must hold for every run after a swap. |
| **Antithesis Angle** | The workload repoints a VIP/proxy container from source A to an independent server B, or runs RESET BINARY LOGS AND GTIDS. Antithesis varies how far B's binlog has grown relative to the stored offset, and partitions or hangs around the switch. The exit-0 interleaving is the one where B's file length or an event boundary lines up with the stored offset; a non-boundary offset fails closed (ER 1236 or a misframe). The workload logs which branch occurred. |
| **Why It Matters** | Ticket 25. The backup silently holds another server's lineage (a file that neither server wrote), and in the reset case the storage is wedged permanently. Validated: `storage_metadata.hpp:35-37` has no identity, and nothing in `src/` queries server_uuid. |

**Priority:** P1 (P0 before the evaluation pass) · **Confidence:** high (mechanism confirmed in code; the QA repro reports exit 0 with diverged bytes) · **Found by:** focus 7 · **Fault requirements:** network faults; **custom:** a workload "source swap" action and a proxy/VIP container.

**Rationale / conflicts resolved:** The evaluation pass moved this property from P0 to P1. Whether a swap fails closed or is silently accepted depends on whether the new server's file length or an event boundary lines up with the stored offset, which is a property of the data rather than of the interleaving, so Antithesis mostly re-confirms a known design gap that already has a QA reproduction (`source_change_test.py`). The independent-server swap needs the deferred `failover` template. The RESET variant fits `fetch_fs`, but only behind a flag, because it makes ticket 25 fire constantly. The property stays as a known-gap tracker and the ticket-25 regression check once a fix lands. The same-UUID rewind case (source restored from its own backup, GNOs reused with different content) is not covered here; it is `source-rewind-never-silently-continued` (gap G1).

**Evidence:** `properties/source-swap-never-silently-accepted.md`

**Open Questions:**

- Is fail-closed expected, or is source continuity the operator's job? `(needs human input)`

### source-rewind-never-silently-continued — A source restored from its own backup is never silently continued past reused GTIDs

| | |
|---|---|
| **Type** | Safety |
| **Property** | After the source is restored from an earlier backup of itself (same `server_uuid`) behind what PBS has stored, PBS never appends further data to that storage. Fetch never exits 0 and pull never stores new bytes, including after the rewound source has re-used the stored GNOs for different transactions. |
| **Invariant** | Workload `Always("no PBS append after a source rewind behind the stored GTIDs")`. It is evaluated at every fetch exit and at periodic pull checks while `ahead = stored_gtids − restored.gtid_executed ≠ ∅` and the storage has not been re-seeded. The mode goes in the details. "Append" means a storage object changed beyond the restore-time snapshot, with recovery truncation tolerated. On violation the details carry per-GTID payload hashes of the reused GNOs, stored versus source. Vacuity markers: `Sometimes("rewound source refused PBS's dump with ER_REPLICA_HAS_MORE_GTIDS_THAN_SOURCE")` and `Sometimes("rewound source committed past the stored GTID count while the storage still held reused GNOs")`. `Always` fits because one silent append permanently mixes two histories. The benign case (stored ⊆ restored set) is excluded by the precondition. |
| **Antithesis Angle** | Antithesis varies when the snapshot is taken relative to PBS's progress, how long the restored source runs (and rotates) before PBS reconnects, and where rotations fall. In rewrite mode silent continuation is predicted as soon as the GNO-count check passes. In GTID non-rewrite mode it needs the source's numbering to pass PBS's tail and a file whose PREVIOUS_GTIDS equals the stored set exactly; otherwise there is a name-collision wedge (`storage_core.cpp:276`) or a `validate_position` failure (`reader_context.cpp:917`). In position mode it needs X to land on an event boundary of the rewritten same-named file. Kills and partitions around the restart move PBS's tail. |
| **Why It Matters** | Restoring a source from xtrabackup/snapshot is the most common moment a binlog server is needed for PITR. The server checks only the GNO count for its own UUID (`rpl_binlog_sender.cc:918-921`), and PBS records no identity or lineage (`storage_metadata.hpp:30-37`). The storage then holds GTIDs whose content no server has, and pull serves them downstream. A `server_uuid`-based fix for ticket 25 would not catch this, which is why it is separate from `source-swap-never-silently-accepted` (different trigger, precondition and timing; no proxy needed). |

**Priority:** P1 · **Confidence:** medium (PBS side confirmed in code; server acceptance confirmed in MySQL source; the rewrite-mode silent branch is predicted, not run) · **Found by:** evaluation gap G1 (wildcard W4) · **Fault requirements:** **custom:** a mysqld restart-wrapper extension that snapshots the datadir while mysqld is stopped and restores it before a later restart, driven by a marker file; workload SIGKILL of PBS and network faults optional. **Runs in:** needs new driver: `mysql-source/parallel_driver_rewind_source.sh` (snapshot/restore via the restart-loop wrapper) in `fetch_fs` and `pull_fs`, behind a flag like the `source-swap-*` RESET action. It invalidates the exactly-once oracles for the timeline, so it must be excluded from them or re-seed afterwards.

**Rationale / conflicts resolved:** Kept separate from `source-swap-never-silently-accepted`, not merged. Both say "no append after a source-history change", but the trigger, precondition and timing differ: here the source keeps its `server_uuid`, the GNOs are reused with different content, no proxy or deferred template is needed, and a `server_uuid`-based ticket 25 fix passes this property while fixing that one. The assertion message is distinct. Like the RESET variant there, it invalidates the exactly-once oracles for its timeline.

**Evidence:** `properties/source-rewind-never-silently-continued.md`

**Open Questions:**

- Is PBS expected to detect a rewound source, or is lineage the operator's job (same contract as catalog OQ 5, but a `server_uuid` check cannot answer it)? `(needs human input)`

### source-purge-past-resume-point-fails-closed — PBS that fell behind source retention fails closed, cleanly and repeatably

| | |
|---|---|
| **Type** | Safety |
| **Property** | When the source has purged binlogs or GTIDs that PBS's resume point still needs, every fetch exits non-zero and pull exits non-zero instead of collecting. No bytes are appended and no gap is stored. Repeated restarts leave the storage byte-identical and openable. |
| **Invariant** | Workload, with distinct messages: `Always("writer never exits 0 or appends after the source purged a binlog it still needed")`; `Always("repeated failed resumes against a purged source leave the storage byte-identical")` (hashes equal across attempts 2..k, and `list` succeeds); `Always("pull exits after its resume hits a purged source binlog")` (a supervisor outcome row with non-zero exit within `idle_time + connect_timeout + δ`); `Sometimes("resume hit a purged source binlog")`, split by mode (position: "Could not find first log file"; GTID: purged required GTIDs). The oracle classifies the position-mode "complete tail purged" case separately. SUT-side (missing): `ALWAYS_OR_UNREACHABLE(storage is empty, "gtid_purged bootstrap only on an empty storage")` at `collector_context.cpp:229-240` guards the silent-skip regression. `Always` because a single silent append or storage change after a source purge is a violation; `Sometimes` proves the purged-source scenario was reached; `ALWAYS_OR_UNREACHABLE` because the bootstrap path is optional but must hold when taken. |
| **Antithesis Angle** | A purge bites only at the next reconnect, because MySQL keeps files in use by a dump thread. So the interesting interleavings are a partition or kill that forces pull's 2013 reconnect, or a pull restart, landing after the workload's `PURGE BINARY LOGS TO`. Kills during the crash loop's startup recovery test that the loop is idempotent. Partitions between PBS and the source decide whether the refusal or a 2013 is seen first. |
| **Why It Matters** | PBS down for longer than `binlog_expire_logs_seconds` is routine. Code today fails closed: the refusal arrives at the first `mysql_binlog_fetch` (`mysql_binlog_open` reads no reply), and non-2013 errors are raised (`connection.cpp:546-550`) and end fetch/pull. That path is confirmed by the ticket 18 QA output "cannot fetch binlog event: … CR_1236". The property guards against a silent "resume from first available" regression, for example in a ticket 25 fix, and checks that the supervisor crash loop never drifts the storage. It also replaces the catalog's blanket "source retains its binlogs" assumption with a tested outcome. |

**Priority:** P2 · **Confidence:** high (fail-closed path traced in PBS and libmysqlclient and seen in QA output; the drift and pull-exit clauses are untested) · **Found by:** evaluation gap G5 (coverage-balance F5, wildcard W13) · **Fault requirements:** workload SQL `FLUSH BINARY LOGS` + `PURGE BINARY LOGS TO` on the source; workload SIGTERM/SIGKILL of PBS; network faults (native) to force reconnects. **Runs in:** needs new driver: `pbs/serial_driver_source_purge_past_pbs.py` in `fetch_fs` (under the writer lock) and `pull_fs` (removes `pull.enabled` and SIGTERMs pull, then re-enables it). The driver re-seeds the storage after its checks so the permanent wedge does not end the timeline, and flags the timeline for the exactly-once oracles.

**Rationale / conflicts resolved:** New. No existing property drives a source-side purge: `interrupted-purge-leaves-openable-storage` covers PBS's own `purge_binlogs`, and `dump-session-ends-with-eof-or-err-packet` covers pull's serving side. For the flagged timelines this property replaces the Assumption "the source retains its binlogs" with a tested outcome.

**Evidence:** `properties/source-purge-past-resume-point-fails-closed.md`

**Open Questions:**

- Is a permanent fail-closed (crash loop) the intended outcome, or should PBS offer an explicit re-seed that records the gap? `(needs human input)`
- Should position mode resume when only a complete (ROTATE-terminated) tail was purged, instead of failing? `(needs human input)`

### source-crash-restart-resume-is-faithful — After the MySQL source crashes and restarts, PBS resumes and its copy stays faithful

| | |
|---|---|
| **Type** | Safety |
| **Property** | After the source mysqld is killed and restarted while PBS streams, PBS resumes, and after convergence its storage is an exact copy of the source, including the source binlog left without a ROTATE/STOP terminator. |
| **Invariant** | Workload `Always("storage is a complete copy after a source crash and restart")` (`check_complete_copy()` after the source is back and faults stop; IN_USE is masked only on the source's active file, because crash recovery clears it on the crashed file). `Sometimes(stored a binlog the source ended unterminated)` coverage hint. SUT-side (missing) `REACHABLE("collector closed a binlog the source left unterminated")` at `collector_context.cpp:547`. `Always` because the copy must be complete after every source crash and restart; `Sometimes`/`REACHABLE` anchor the unterminated-binlog path. |
| **Antithesis Angle** | Source SIGKILL (graceful shutdown is covered by `inprocess-reconnect-resumes-without-dup-or-gap`) mid-transaction (PBS must discard the partial transaction, `collector_context.cpp:195`), right after a commit PBS already flushed, right after a rotation (the ticket 6 path), and a source rotation before PBS reconnects (ticket 14 interplay in GTID mode). PBS ahead of the source is out of scope: with `sync_binlog=1` the dump thread never sends non-durable bytes. |
| **Why It Matters** | A binlog server exists to survive source failures. The "unterminated binlog" branch (`collector_context.cpp:514-553`) is the recovery path for exactly this event and has no byte-level test under a real source crash. **Expected to fail today** when the kill lands right after a rotation in position mode (lead (b) in "New defect leads"): the debug build aborts at `storage.cpp:138` on every retry, and N stays magic-only forever. |

**Priority:** P0 (P1 before the evaluation pass) · **Confidence:** medium · **Found by:** focus 3 · **Fault requirements:** source SIGKILL via the mysqld restart-loop wrapper and `parallel_driver_kill_mysqld.sh`; node termination not needed. Source must run `sync_binlog=1` and `innodb_flush_log_at_trx_commit=1`.

**Rationale / conflicts resolved:** The evaluation pass promoted this property to P0. A source SIGKILL at a random instant is a natively random, high-fit fault that needs no custom shim and no deferred template. With planned faults alone it reaches the unterminated-binlog branch (`collector_context.cpp:514-553`), which has no byte-level test, and lead (b) (the ticket-6 path, a debug abort at `storage.cpp:138`). It replaces `source-swap-never-silently-accepted` in the P0 tier. Lead (b) wedges the timeline in a debug build, so the post-rotation kill is a known-defect trigger until it is fixed.

**Evidence:** `properties/source-crash-restart-resume-is-faithful.md`

**Open Questions:**

- None

### gtid-failover-no-lost-transaction-merge — Lost-on-failover transactions are never merged with the new primary's history

| | |
|---|---|
| **Type** | Safety |
| **Property** | After PBS appends any transaction received from source S, every GTID in the storage is in S's `gtid_executed`. Otherwise PBS must have failed closed. |
| **Invariant** | Workload `Always("no transaction appended from a new primary while storage holds GTIDs it lacks")`, in both modes (mode in the details), where `lost = storage_gtids − new_source.gtid_executed` is computed at failover from `list` output and SQL. `Always` fits because a single merge permanently corrupts the history that PITR relies on. |
| **Antithesis Angle** | Asymmetric partitions of A↔B while PBS still receives from A create "PBS ahead of the promoted replica". The workload then kills or isolates A and promotes B behind the VIP. The server accepts a GTID dump whose set contains GTIDs it lacks, so it never makes PBS fail closed. Rewrite mode accepts any source and completes the merge silently; non-rewrite mode fails closed only through file-name and position coincidences, and completes the merge silently too when B's name and position line up. |
| **Why It Matters** | The classic split-brain artefact. PBS's metadata (`storage_core.cpp:691`; in rewrite mode also the generated PREVIOUS_GTIDS) encodes a history no server has. Pull serves the deposed primary's lost transactions downstream, and PITR replays a branch that conflicts with B. |

**Priority:** P1 · **Confidence:** medium-high (PBS side confirmed in code; server acceptance of the dump confirmed in MySQL source) · **Found by:** focus 7 · **Fault requirements:** network partitions; **custom:** an async replica B with `log_replica_updates`, a promotion action and a VIP/proxy container.

**Evidence:** `properties/gtid-failover-no-lost-transaction-merge.md`

**Open Questions:**

- Is a merged history acceptable, or must PBS refuse? `(needs human input)`
- Is failover in scope at all? `(needs human input)`

### gtid-failover-resume-progresses — GTID mode follows a clean failover

| | |
|---|---|
| **Type** | Liveness |
| **Property** | After a clean GTID failover (the promoted replica's executed set includes every stored GTID) and a quiet period, PBS eventually stores transactions committed on the new source after the promotion. |
| **Invariant** | Rewrite mode is expected to progress after a clean failover, so its check is bounded: workload `Always("rewrite mode stores a post-promotion transaction within T after the quiet period starts")`, evaluated once per quiet period (`ANTITHESIS_STOP_FAULTS` plus the harness quiet flag). Workload `Sometimes(stored GTIDs intersect new-source post-promotion GTIDs)`, split by mode with separate messages for rewrite and non-rewrite, stays as the vacuity guard and as the only progress check for non-rewrite mode, where progress is not guaranteed. Non-rewrite companion: `Always("non-rewrite failover run either progresses or exits non-zero with no bytes appended to an existing stored file")`. A `Sometimes` alone would pass as soon as one timeline progressed, so it cannot catch a wedge on its own; hence the bounded rewrite-mode `Always`. |
| **Antithesis Angle** | Partitions and hangs decide how far B lagged, whether B rotated at promotion, and where the first missing GTID sits in B's files. Non-rewrite mode is predicted to fail closed when B's file name collides with a stored non-tail name (`storage_core.cpp:272-279`), and when B skips already-stored transactions, which breaks `next_event_position` continuity (`reader_context.cpp:905-920`): B starts from its newest binlog whose PREVIOUS_GTIDS ⊆ the request and sends a heartbeat for skipped groups, but PBS drops it via `MYSQL_RPL_SKIP_HEARTBEAT` (a fixable PBS-side loss). Non-rewrite progress needs B to rotate at promotion with PBS fully caught up, so the workload sometimes runs `FLUSH BINARY LOGS` on B at promotion. |
| **Why It Matters** | GTID auto-position exists so that replicas can follow failovers. A backup tier that wedges on every routine promotion needs manual re-seeding and leaves a backup gap. |

**Priority:** P2 · **Confidence:** medium · **Found by:** focus 7 · **Fault requirements:** network partitions, node hang or kill of A (**node-termination** optional); **custom:** promotion action and VIP/proxy.

**Evidence:** `properties/gtid-failover-resume-progresses.md`

**Open Questions:**

- Is following a failover in non-rewrite mode supported? `(needs human input)`

### source-upgrade-resume-continuity — Resume across an in-place source upgrade

| | |
|---|---|
| **Type** | Safety |
| **Property** | A storage being filled from a source that is then upgraded in place (8.0→8.4, 8.4→9.7) resumes after PBS kills and reconnects with no lost or duplicated transactions, and each stored file keeps its source file's own FDE version. |
| **Invariant** | Workload `Always("stored GTIDs equal source gtid_executed without duplicates across a source upgrade")`, `Always("stored file is a byte prefix of the source file across a source upgrade")` (position mode), `Always("stored file FDE server version equals the source file's")`. SUT-side (missing) `AlwaysOrUnreachable` in `reader_context` FDE handling: when the FDE version is below the connection version, the adopted post-header table equals the known table for that version; `Sometimes(fde_version < connection_version)`. `Always` because any duplicate or byte mismatch across the upgrade is a violation; `AlwaysOrUnreachable` because the older-FDE path runs only when an upgrade occurs; `Sometimes` proves that path was reached. |
| **Antithesis Angle** | Supported paths are 8.0→8.4 and 8.4→9.7; the FDE layout switch happens only on 8.0→8.4, and position mode is the clean way to exercise it. Kill PBS before, during and after the source's shutdown, restart and first post-upgrade rotation. Partitions force 2013 reconnects that land on the other side of the upgrade. Explores resume from the middle of a pre-upgrade file and GTID-mode resume across the upgrade-induced rotation (ticket 14 path). |
| **Why It Matters** | `event_view.cpp:72-96` claims mixed 8.0/8.4 FDE support, but the only test fetches once into an empty storage after the upgrade and never resumes across it. A source upgrade is routine for a continuous backup. |

**Priority:** P1 · **Confidence:** medium (never run across an upgrade) · **Found by:** focus 10 · **Fault requirements:** process kill (workload), network faults; **custom:** workload-driven mysqld binary swap over a shared datadir (feasible via the restart-loop wrapper and a marker file; scheduled in the deferred `upgrade` template).

**Evidence:** `properties/source-upgrade-resume-continuity.md`

**Open Questions:**

- Is a GTID-mode resume across the upgrade byte-identical (same question as ticket 14)? `(needs human input)`

### tagged-gtid-resume-exact — Tagged-GTID resume (8.4/9.7) is exact

| | |
|---|---|
| **Type** | Safety |
| **Property** | With a ≥ 8.4 source and a workload mixing tagged and untagged GTIDs, GTID-mode resume after any kill or reconnect sends a dump set the server interprets correctly: stored transactions are neither duplicated nor missing. |
| **Invariant** | Workload `Always("no tagged or untagged GTID stored twice")`, `Always("stored GTIDs are a subset of source gtid_executed and equal after catch-up")`, `Always("per-file added_gtids equals GTIDs parsed from bytes under tagged GTIDs")`. SUT-side (missing) `Sometimes(stored set contains tags at reconnect)` in `open_connection_and_switch_to_replication`; `AlwaysOrUnreachable(decode(encode(gtids)) == gtids)` after `encode_to`. `Always` because a single duplicated or extra GTID is a violation; `Sometimes` proves tagged reconnects happened; `AlwaysOrUnreachable` because the round-trip check runs only on GTID-mode reconnects. |
| **Antithesis Angle** | Kills and partitions at random points after the first tagged transaction is stored, so each reconnect uses the tagged COM_BINLOG_DUMP_GTID encoding (`gtid_set.cpp:474-565`) with varied set shapes (several tags per UUID, gaps). |
| **Why It Matters** | The tagged encoding is reached only on ≥ 8.3 sources and only on reconnect. Caveat: `gtid_set_parser::parse` ignores trailing input (not tag-specific; servers never emit it). It is covered offline by Hegel and locally by `event_variety_test.py` (not CI). A wrong encoding silently duplicates or skips transactions. |

**Priority:** P1 · **Confidence:** medium (version-specific path confirmed; no defect found by reading) · **Found by:** focus 10 · **Fault requirements:** process kill (workload), network partition/reset.

**Evidence:** `properties/tagged-gtid-resume-exact.md`

**Open Questions:**

- None

### source-auth-survives-auth-cache-flush — PBS re-authenticates after the source's auth cache is emptied (ticket 23)

| | |
|---|---|
| **Type** | Liveness |
| **Property** | After a source restart, `FLUSH PRIVILEGES` or `ALTER USER`, each of which empties the caching_sha2_password cache, a running pull (or the next fetch) eventually re-authenticates and stores new transactions, in every supported auth/TLS configuration. |
| **Invariant** | Workload `Sometimes(PBS stored a new transaction after a source restart or FLUSH PRIVILEGES)`, one message per TLS mode. Workload `Always("no live pull without a Binlog Dump thread on the source after the quiet period")`. For TLS configs only, workload `Always("transaction committed T s after the source is back appears in storage")`. SUT-side (missing): `SOMETIMES(reconnect succeeded after a failed connect)`, and an errno-tagged `REACHABLE("connect failed with auth error")` in the catch at `collector_context.cpp:212`, where errno 2061 (`CR_AUTH_PLUGIN_ERR`) is dropped today. `Sometimes` because re-authentication is a liveness goal that must be observed at least once per TLS mode, paired with the bounded `Always` checks after the quiet period; `SOMETIMES`/`REACHABLE` are exploration anchors. |
| **Antithesis Angle** | Whether full authentication is needed depends on the interleaving of source restarts, cache flushes, and other TLS logins of the same user that warm the cache. With `ssl.mode=disabled`, PBS cannot do full auth: there is no `MYSQL_OPT_GET_SERVER_PUBLIC_KEY` (`connection.cpp:225-291`). Pull then retries forever, silently stale. |
| **Why It Matters** | Ticket 23 is validated in code: there is no public-key option anywhere in `src/`. On MySQL 9.x there is no `mysql_native_password` fallback. A backup that silently stops after a routine source restart is a core failure. |

**Priority:** P1 · **Confidence:** high (mechanism validated); in-contract status of non-TLS unknown · **Found by:** focus 6, 10 · **Fault requirements:** source restart via the `kill_mysqld` driver (node termination not needed), or workload `FLUSH PRIVILEGES`/`ALTER USER`; partitions to force reconnects.

**Rationale / conflicts resolved:** Focus 6 rated it P1 and focus 10 rated it P2 because its contract status is low. The merge resolves to P1. Source restarts are routine and the stall is silent. The contract question decides only whether the `ssl.mode=disabled` variant is a regression test or a tracker for a known failure.

**Evidence:** `properties/source-auth-survives-auth-cache-flush.md`

**Open Questions:**

- Is non-TLS caching_sha2_password a supported config (should the `ssl.mode=disabled` variant run)? `(needs human input)`

### source-session-tls-when-configured — Every source connection is TLS when TLS is required

| | |
|---|---|
| **Type** | Safety |
| **Property** | When `connection.ssl.mode` is `required` or stricter, every connection PBS opens to the source, including reconnects, is TLS-encrypted. PBS opens one connection per attempt (setup queries and the dump share it), each rebuilt from config. |
| **Invariant** | Workload `Always("every PBS source thread uses SSL/TLS when TLS is required")` over `performance_schema.threads` rows of the PBS user. `Sometimes(observed PBS thread after a reconnect)`. SUT-side (missing) `ALWAYS(mysql_get_ssl_cipher(conn) != nullptr)` after connect when the mode is `required` or above. `Always` because one plaintext connection when TLS is required is a security violation; `Sometimes` proves reconnect threads were observed. |
| **Antithesis Angle** | Partitions and source restarts force many reconnects, each rebuilding the `MYSQL*` from config (`collector_context.cpp:209-211`, `connection.cpp:291-321`). Polling races short-lived connections. |
| **Why It Matters** | Credentials and binlog data cross the network. Guards against a regression that drops TLS options on reconnect. |

**Priority:** P2 · **Confidence:** high that it holds today (regression guard) · **Found by:** focus 6 · **Fault requirements:** network partitions; source restart via the mysqld restart wrapper (node termination not needed).

**Evidence:** `properties/source-session-tls-when-configured.md`

**Open Questions:**

- Should `preferred` mode's cleartext fallback be flagged or ignored? `(needs human input)`

### storage-format-version-mismatch-handled — Storages written by other builds are opened or refused cleanly

| | |
|---|---|
| **Type** | Safety |
| **Property** | A storage written by an older build (binlog `.json` without fields added later, such as `last_sequence_number`), or one carrying features this build lacks (encryption, when run by a pre-0.4.0 binary), makes writers refuse cleanly before changing any object. Query modes never crash and never report `success` with fewer binlogs than the index lists. |
| **Invariant** | Workload `Always` messages: "writer on a legacy-format storage refuses cleanly without changing objects", "list success on a legacy storage lists every indexed binlog", "no query-mode process dies by signal on a legacy storage". The downgrade messages ("older binary exits non-zero without changing any object when metadata.json records encryption", "current-binary pull stream equals source after an older binary ran") would fail deterministically today against released binaries (pbs-0.3.1 never refuses), so they are kept only as regression guards for a future version bump, and only if downgrade is declared in scope. SUT-side (missing): `UNREACHABLE("binlog metadata records encryption but storage metadata does not")`. The empty-records site at `storage_core.cpp:1001` is owned by `cli-json-envelope-and-exit-code-agree`, and the record-drop site at `:942-950` by `query-success-never-omits-middle-binlog`; this property references both. `Always` because each refusal or listing must be clean every time; `UNREACHABLE` because the inconsistent-encryption-record state is itself the defect. |
| **Antithesis Angle** | Mostly deterministic, using a legacy fixture as the initial state and a random choice of binary per restart in a kill/restart loop. The timing-dependent parts are S3 GetObject faults during `list`/`search_*`, and the old binary's recovery truncation running on files it cannot interpret. |
| **Why It Matters** | `last_sequence_number` was added in pbs-0.2.4 with no version bump and no default, so upgrading from 0.2.3 bricks the storage for writers and makes query modes hit UB. `nv_tuple_from_json` ignores unknown keys, and metadata versions stayed at 1 when `encryption` was added. A downgrade therefore silently mixes plaintext into encrypted files; pbs-0.3.1 also rewrites the tail `<binlog>.json` without the encryption envelope, destroying the file key (lead (g) in "New defect leads", not reproduced). No released tag contains the replication-source listener, so an older `pull` is collector-only. |

**Priority:** P2 · **Confidence:** high (code and git history); in-contract status unknown · **Found by:** focus 10 (two drafts merged) · **Fault requirements:** workload kill; **custom:** a legacy-storage fixture, a previous-release binary in the image, and an S3 fault proxy for the transient-read variant.

**Evidence:** `properties/storage-format-version-mismatch-handled.md`

**Open Questions:**

- Is opening a 0.2.1–0.2.3 storage with ≥ 0.2.4 supported? `(needs human input)`
- Should `list` report `warning` when it skipped records? `(needs human input)`
- Is binary downgrade supported? `(needs human input)`

### pull-client-version-matrix — Every supported mysqlbinlog version completes a dump against pull

| | |
|---|---|
| **Type** | Reachability |
| **Property** | Each mysqlbinlog version shipped in the harness (8.0/8.4/9.7) completes the handshake and preamble against pull in non-GTID dump mode and receives events, including from storages holding FDEs of other (not newer) versions. A client older than the stored data is out of scope. |
| **Invariant** | Workload `Sometimes(dump session for client version V received ≥ 1 non-artificial event)`, one message per V, emitted only for the client versions shipped in that launch. The image ships only the `mysqlbinlog` matching `MYSQL_VERSION`, so each launch satisfies one V; the cross-version anchors are deferred until the image ships all three clients (`client-matrix`), and are not emitted before then. Workload `Always("no Syntax error or Unknown command returned to a mysqlbinlog client in BINLOG-DUMP-NON-GTIDS mode")` (unknown queries get ER 1064, unsupported commands "Unknown command"); GTID dump mode (`COM_BINLOG_DUMP_GTID`, always "Unknown command" today) is excluded. SUT-side (missing) `Reachable("unknown query from client")` at `network_service.cpp:574-585` with the query text in details. `Sometimes` because a working dump per client version is a goal that must be observed at least once; `Always` because any protocol error returned to a client is a violation; `Reachable` is a pure exploration anchor. |
| **Antithesis Angle** | Low by itself. It guards every pull property against silent starvation caused by a client/storage version mismatch in the image. |
| **Why It Matters** | Pull answers only an exact-string allowlist (`network_service.cpp:487-550`) and always advertises "9.7.0-pbs". Ticket 20 shows that a client sending other queries fails completely. |

**Priority:** P2 · **Confidence:** medium (non-GTID preamble verified to work for every CI client version) · **Found by:** focus 10 · **Fault requirements:** none.

**Evidence:** `properties/pull-client-version-matrix.md`

**Open Questions:**

- Which mysqlbinlog versions are claimed as pull clients, and are cross-version combinations in scope? `(needs human input)`

## Liveness and resource boundaries

Progress after faults heal, shutdown latency, and bounded resource use: the single io thread, whole-object S3 re-uploads, the local S3 buffer, file descriptors, session lifetimes and collector memory.

### pull-chain-converges-after-heal — After faults stop, pull reconnects and catches up, and every client catches up with the storage

| | |
|---|---|
| **Type** | Liveness |
| **Property** | After faults stop and the source stops writing, within T = `connection.read_timeout` + `idle_time` (when a reconnect is needed) + 500 ms dump poll + write time + margin, pull is still collecting (a frozen source triggers the `read_timeout` flush, `collector_context.cpp:189-203`, so checkpointing is optional), the storage holds every source transaction, and every reconnected blocking client holds every stored byte. This holds despite earlier io-thread stalls and whole-object S3 re-uploads. |
| **Invariant** | Main assertion: workload `Always("converged within T after a heal")`, evaluated once per quiet window (an `ANTITHESIS_STOP_FAULTS` window in which the harness quiet flag also pauses the harness's own fault drivers, proxy rules are cleared and no fsfault rule is armed) that lasts longer than T. Here "converged" means the stored tail equals the source position and every live blocking client holds every stored byte. T is parametric while the bound is an open question; the value used and the measured catch-up time go in the details. Vacuity guard: `Sometimes("quiet period reached with ≥ 1 live blocking client and converged within T")`; a `Sometimes` alone would pass as soon as one timeline converged and could not catch a client that never catches up. The N7 sub-check is workload `Always("pull did not exit because of a network-only fault")` (source↔PBS and PBS↔S3 network faults), read from the harness supervisor's outcome log rather than "process running", because the supervisor restarts pull. The outcome record carries the last error line of pull's console log and the custom-fault state at exit (quiet flag, the proxy's `GET /__faults__` snapshot, any fsfault rule), so exits caused by same-`server_id` eviction, fsfault, a proxy rule or a known-defect wedge are attributed and excluded. The source↔PBS leg is expected to hold (transport faults are always 2013); the PBS↔S3 leg is expected to fail when a checkpoint PUT exhausts CRT retries. SUT anchors (missing): `UNREACHABLE("blocking pull got EOF from the source with an incomplete transaction buffered")` at the case (3) return (`collector_context.cpp:184-187`); `SOMETIMES(native_error == CR_SERVER_LOST)`, `REACHABLE("mid-stream fetch failed with non-2013 errno")` (errno in details; only server error packets or 2020 land here), `REACHABLE("pull reconnect attempt succeeded after idle")`, `SOMETIMES(single checkpoint upload ≥ 64 MiB)`, `SOMETIMES(source dropped the dump during an upload)`. Workload `Unreachable("healthy client dropped by write timer after io stall")`, where the client container is never faulted. |
| **Antithesis Angle** | Asymmetric partitions: a source dump thread stuck in write, a PBS session waiting on a write timeout, a collector blocked on S3 under the storage lock. Then a heal. S3 latency or throttling stretches whole-object uploads under the exclusive lock (`s3_storage_backend.cpp:850-863, 933-938`) past the source's `net_write_timeout`. This explores the hypothesised spiral (stall → drop → bigger re-upload). The workload also runs `KILL QUERY` on the dump thread and a graceful source `SHUTDOWN` during a large transaction, which delivers an EOF mid-transaction (lead (a) in "New defect leads"). |
| **Why It Matters** | Only errno 2013 is retried (`connection.cpp:546-551`); README:378 says network errors are retried, yet an S3 network error during a checkpoint ends pull (N7). The case (3) EOF `UNREACHABLE` is expected to fire today after a graceful source shutdown mid-transaction. A relay tier that does not catch up after a blip falls behind silently. Ticket 17 and README:671 document the re-upload amplification but no catch-up bound. |

**Priority:** P1 · **Confidence:** medium (mechanisms and errno mapping verified in code; spiral unverified) · **Found by:** focus 2, 3, 5, 7 (four drafts merged) · **Fault requirements:** network partitions and latency on all links, node throttling of the S3 container, a quiet period; the harness supervisor restarts pull; workload `KILL QUERY` of the dump thread and source `SHUTDOWN`/SIGKILL via the restart wrapper (node termination not needed).

**Rationale / conflicts resolved:** Focus 3 framed N7 as a bounded `Always`, and the other three framed convergence as `Sometimes`. Both assertions are kept, with distinct messages. The evaluation pass turned the convergence check into a bounded `Always` per quiet period, with the `Sometimes` kept as the vacuity guard: a `Sometimes` passes once any timeline converges, so the stall-drop-re-upload spiral and a client that never catches up would go unreported. "Pull survives a transient fault" is an invariant, so it is `Always`; the investigation kept it, narrowed to network-only faults and observed through the supervisor's outcome log. Focus 2 rated its stall variant P2; the merged property is P1.

**Evidence:** `properties/pull-chain-converges-after-heal.md`

**Open Questions:**

- Is an S3 network error during a checkpoint meant to terminate pull (README:378 says network errors are retried)? `(needs human input)`
- Is a replica disconnect on write timeout during a storage stall acceptable? `(needs human input)`
- Does the degradation spiral happen? `(partial: CRT fails a stalled transfer via its throughput monitor, then pull ends; unbounded lag needs a run)`
- What are the CRT defaults? `(partial: no overall request timeout; connect 1000 ms; lowSpeedLimit 1 B/s over max(3 s, requestTimeoutMs/1000); retry count unverified)`
- Is any upper bound on catch-up time acceptable? `(needs human input)`
- Does an EOF mid-transaction corrupt the stored binlog (GTID mode) or crash pull (position mode)?

### fetch-converges-after-faults-stop — After faults stop, repeated fetch runs reach exit 0 within N attempts

| | |
|---|---|
| **Type** | Liveness |
| **Property** | After faults stop, the harness has cleared custom fault rules and stragglers, the source is reachable and the source load has stopped, at most N (= 3) consecutive `fetch` runs, each within a timeout `T_a`, include one that exits 0. That success then lets the existing complete-copy and exactly-once checks run. |
| **Invariant** | Workload `Always("fetch reaches exit 0 within N attempts after faults stop")` in `eventually_converge.py`. The details carry mode, encryption, rewrite, backend, and each attempt's exit code and last error line, so triage buckets ticket 15, ticket 18/`CR_1236`, lead (b) aborts and hangs without new assertions. This is a bounded `Always`, not `Sometimes`: a `Sometimes` passes when any one timeline converges and would hide a wedge in another, which is the silence this property removes. Vacuity: `Sometimes("fetch converged after a timeline with a writer kill during rotation")`, `Sometimes("fetch converged after an exhausted S3 PUT failure")`, `Sometimes("fetch converged on an encrypted storage")`, `Sometimes("fetch converged on a rewrite storage")`. Harness guard: `Unreachable("fetch convergence preconditions not met")`. No new complete-copy assertion; those stay with `graceful-exit-leaves-flushed-storage` and `stored-transactions-exactly-once-across-resumes`. Optional SUT anchor (missing): `REACHABLE("fetch reached EOF and is about to report success")` at `fetch_operation.cpp:112`. |
| **Antithesis Angle** | Any wedge left by the fault phase surfaces here as a red result instead of an unevaluated `Always`. That covers ticket 15 rotation kills, ticket 18 metadata-PUT failures in position mode (`CR_1236`, not a validator error, so outside `kill-anywhere-storage-reopens`), lead (b) source kills just after rotation, interrupted purges, and S3 hangs with no overall request timeout. It also catches wedges nobody has found yet. Each fault timing that leads to a wedge is a distinct, replayable timeline. |
| **Why It Matters** | Fetch is the cron backup workflow (sut-analysis W1/W2/W4: "silent, catastrophic"). Without this property, a timeline that wedges reports green on the P0 exactly-once property because the property never runs. Pull has `pull-chain-converges-after-heal`; fetch had nothing. **Expected to fail today** wherever ticket 15, ticket 18 (position mode) or lead (b) is hit, with the same root causes their owning properties report. |

**Priority:** P1 · **Confidence:** high (the gap is structural; known wedge routes are catalogued and code-traced by their owners) · **Found by:** evaluation gap G4 (coverage-balance F4) · **Fault requirements:** none during the check (faults stopped). The preceding phase needs the union of the wedge routes: workload SIGKILL of fetch during rotation, network faults on pbs↔s3 and source↔pbs, source SIGKILL via the restart wrapper; **custom:** `s3_fault_proxy` / `fsfault` for the ticket 18 route. · **Runs in:** `fetch_fs` and the fetch-like arm of `s3`, by extending the existing `pbs/eventually_converge.py` (adds a precondition step, an attempt bound and the assertion).

**Rationale / conflicts resolved:** Kept separate from `pull-chain-converges-after-heal`, not merged. The quiet-window precondition is shared, but the operation, trigger and observation differ: a one-shot fetch reaching exit 0 within N runs, against a long-running pull and its clients catching up within T. It is the liveness complement that keeps the P0 exactly-once and complete-copy `Always`s from passing vacuously (R25 notes the same gap).

**Evidence:** `properties/fetch-converges-after-faults-stop.md`

**Open Questions:**

- What attempt bound N is the contract (N = 1 "next run succeeds" or a few runs)? `(needs human input)`
- Is fetch expected to progress on a storage left inconsistent by an earlier run (are manual-repair fail-stops accepted)? `(partial: README:373 claims any fetch error leaves the storage "in consistent state"; acceptance of the known wedges unconfirmed)`
- How should `T_a` separate a hang from slow S3 progress? `(partial: PBS sets no S3 request timeout, so CRT defaults apply; T_a must be calibrated from the measured upload rate)`

### sigterm-honoured-within-bound — SIGTERM stops fetch/pull within a bound, during startup and under storage-lock contention

| | |
|---|---|
| **Type** | Liveness |
| **Property** | After SIGTERM, pull exits within `read_timeout + 1` s plus a stated slack (README:719), even while dump sessions are blocked on the storage lock behind a long collector write. A SIGTERM delivered between process start and the first streamed event makes fetch/pull exit within a startup bound, and the next start opens the storage. |
| **Invariant** | Bounded liveness checked with `Always`, because an unbounded "eventually exits" cannot fail in a finite run. Workload `Always` messages: "pull exits within read_timeout + 1 s + slack after SIGTERM in steady state", "fetch/pull exits within the startup bound after a SIGTERM sent before the first streamed event" (B_startup = storage-open bound + connect_timeout + read_timeout + slack), "next start after a startup SIGTERM opens the storage", "signal-only stop of pull does not report failure". Timelines where the kill driver escalated SIGTERM to SIGKILL are excluded from the latency checks. The latency `Always`s are evaluated only inside a quiet window (an `ANTITHESIS_STOP_FAULTS` window in which the harness quiet flag also pauses the harness's own fault drivers, proxy rules are cleared and no fsfault rule is armed): the SIGTERM is sent after the window starts, the window outlasts the bound, and any storage latency in play is a harness-bounded stimulus (an `s3_fault_proxy` stall rule of a known number of seconds, or SIGSTOP/SIGCONT of a known duration) whose duration is added to the slack. The slack is calibrated against a measured baseline of the coverage-instrumented debug build; for S3 storages it also covers CRT retries (about 1 min). Outside quiet windows the latency is recorded as `Sometimes("pull exit latency after SIGTERM exceeded the bound")` with the in-flight storage operation's duration in details: exploration data for the README:719 question, not a failure. SUT-side (missing): `SOMETIMES(SIGTERM arrived while a session was blocked in fetch_event_block)`, `SOMETIMES(io_ctx.stopped(), "collector saw stop immediately after first connect")`; workload `Sometimes(SIGTERM delivered before io_ctx.run())`. |
| **Antithesis Angle** | The signal handler shares the single io thread with dump sessions (`pull_operation.cpp` ~:98-104). Those sessions park on `shared_lock` while the collector holds `unique_lock` across a whole-object S3 PUT (ticket 17). Native network latency and node throttling stretch that wait without limit, so they drive the exploration-only measurement; the bounded checks use the harness-bounded stall instead. At startup, the handler runs only once `io_ctx.run()` starts (`:160`). That is after S3 client construction, listing, `.tmp` deletion, tail truncation and bind. The first `open_connection_and_switch_to_replication` (`collector_context.cpp:132`) does not check `io_ctx.stopped()`. |
| **Why It Matters** | Supervisors escalate to SIGKILL after a grace period, which turns a graceful stop into a crash, possibly in the middle of recovery. The final flush may also run in `~io_context` after the `signal_set` is gone. |

**Priority:** P1 · **Confidence:** medium (ordering verified; whether the bound covers storage latency or startup is unstated) · **Found by:** focus 2, 8 · **Fault requirements:** in quiet windows, a harness-bounded `s3_fault_proxy` stall or SIGSTOP/SIGCONT of known duration and a workload-sent SIGTERM; outside them, native S3 latency, node throttling and process pause for the exploration-only measurement; source container stop. Node termination is not needed.

**Rationale / conflicts resolved:** Focus 2 rated the steady-state variant P1 and focus 8 rated the startup variant P2. The merged property is P1. Focus 8's `SOMETIMES(SIGTERM delivered before io_ctx.run())` cannot be observed inside asio. The SUT marker becomes the `io_ctx.stopped()` condition after the first connect, plus a workload-side `Sometimes`.

**Evidence:** `properties/sigterm-honoured-within-bound.md`

**Open Questions:**

- Is the README:719 bound meant to cover pull with a slow storage backend? `(needs human input)`
- What stop latency is acceptable during startup? `(needs human input)`
- Does a blackholed or packet-held S3 endpoint hang a PUT indefinitely? `(partial: SDK 1.11.774 CRT throughput monitor 1 B/s over ≥3 s + 1 s connect timeout + default retries; not runtime-verified)`
- Should the collector check `io_ctx.stopped()` before connecting? `(needs human input)`
- Is PBS expected to run as PID 1? `(needs human input)`

### stalled-dump-client-isolated — A stalled downstream client delays neither other clients nor the collector beyond write_timeout

| | |
|---|---|
| **Type** | Liveness |
| **Property** | A downstream client that stops reading delays no other client or the collector beyond `write_timeout`, and, once PBS has more unsent data for it than the socket buffers hold, its own session is dropped within `write_timeout`. (A caught-up client that stops reading is never reclaimed today; that belongs to `stale-sessions-reclaimed-within-bound`, lead (f) in "New defect leads".) |
| **Invariant** | Workload `Always("healthy clients receive every flushed transaction within write_timeout + checkpoint_interval + δ beside a stalled client")` and `Always("stalled dump session disappears within write_timeout + δ")` (the latter only after unsent data exceeds the socket buffers), both evaluated only inside a quiet window (an `ANTITHESIS_STOP_FAULTS` window in which the harness quiet flag also pauses the harness's own fault drivers, proxy rules are cleared and no fsfault rule is armed) that also has no node throttling, with the bound calibrated against the instrumented debug build (bounded liveness, so `Always`). The stalled client is a harness reader in `workload` that stops calling `recv()`, because healthy and stalled readers share the `workload` container and a node hang or congestion there would hit both. Any storage latency inside the window is a harness-bounded `s3_fault_proxy` stall whose duration is added to δ. `Sometimes(source dropped PBS while ≥ 4 sessions were catching up)` reports the upstream spiral, now aimed at storage latency (a slow S3 GET holding the io thread) rather than lock starvation, paired with S3 latency faults; `Sometimes(io thread blocked on storage fetch > write_timeout)` as a hint. |
| **Antithesis Angle** | A reader that stops calling `recv()` fills its TCP window; node hang or congestion of the client container does the same natively and drives the exploration-only `Sometimes` hints. All sessions, the listener and the SIGTERM handler share one io thread, and `fetch_event_block` runs the backend GET synchronously on it, so a slow S3 read blocks every session. The collector's wait for the unique lock is bounded by one in-flight fetch. |
| **Why It Matters** | One bad replica must not degrade backup ingestion or other replicas (downstream load propagating upstream). |

**Priority:** P1 · **Confidence:** medium (async write timeout looks correct) · **Found by:** focus 5 · **Fault requirements:** a harness reader that stops calling `recv()` and an optional harness-bounded S3 stall, in quiet windows; node hang or throttling on client containers, network congestion on PBS↔client links and S3 latency faults for the exploration-only `Sometimes` hints.

**INVALIDATED (part): lock starvation.** The original "stalled clients starve the collector on the storage lock" half is dropped. libc++ `std::shared_mutex` is writer-preferring, and all dump sessions run on a single io thread, so at most one in-flight fetch can delay the collector's unique lock. The rest of the property (healthy-client latency, stalled-session reclaim) stands.

**Evidence:** `properties/stalled-dump-client-isolated.md`

**Open Questions:**

- What latency bound for healthy clients is acceptable (the property is parametric in δ)? `(needs human input)`

### s3-crash-leftovers-bounded — Crashes and failed uploads do not accumulate S3 leftovers locally or in the bucket

| | |
|---|---|
| **Type** | Safety |
| **Property** | Across any number of PBS crashes, restarts and failed uploads on an S3 storage, the local buffer directory holds no more than a small constant multiple of the largest binlog size, incomplete multipart uploads under the prefix do not accumulate, and an interrupted upload leaves only the old or the new full object content. |
| **Invariant** | Workload `Always` messages: "S3 local buffer stays within C × largest binlog after kill/restart cycles" (C counts live writer processes only; the CRT client itself buffers nothing on disk), "incomplete multipart uploads older than T stay within k", "an interrupted upload leaves the old or the new full object content". `Sometimes(kill happened while a temp file existed)`, `Sometimes(an incomplete multipart upload observed)`. The multipart sub-check needs binlogs larger than 8 MiB (CRT uploads 8 MiB parts, per ticket 17's PUT counts). `Always` because leftovers beyond the bound or a torn object are violations whenever observed; the `Sometimes` checks prove the kill-with-temp-file and multipart states were reached. |
| **Antithesis Angle** | Workload SIGKILLs at random instants: during the append-download in `do_open_stream`, mid-upload, during startup `do_resize_object`. Each kill skips `~s3_storage_backend` (`s3_storage_backend.cpp:640-652`) and leaves a full binlog copy. Partial-download and resize failures leak without any kill (`:828-832`, `:765-808`). PBS↔MinIO partitions during whole-object re-uploads (`:369-386, 933-938`) may leave multipart parts. |
| **Why It Matters** | The README sample config sets an explicit `fs_buffer_directory`, which is never cleaned. Leaks fill the local disk until every run fails at startup or at the first checkpoint, feeding the ENOSPC trigger of the ticket-18 path (N10). Orphaned multipart uploads cause hidden bucket growth, and leaked copies are an extra at-rest location for plaintext or old ciphertext. |

**Priority:** P1 · **Confidence:** high for the local leak; medium for multipart (CRT 8 MiB parts inferred from ticket 17 measurements) · **Found by:** focus 5 (two drafts merged) · **Fault requirements:** workload SIGKILL of PBS, network partitions on PBS↔S3, an S3 stand-in supporting `ListMultipartUploads`. Node termination is not needed.

**Rationale / conflicts resolved:** The local-buffer variant was rated P1 and the multipart variant P2. The merged property is P1. The local leak is verified in code and leads to a full wedge.

**Evidence:** `properties/s3-crash-leftovers-bounded.md`

**Open Questions:**

- Is leaking a binlog copy per crash accepted? `(needs human input)`
- Are incomplete multipart uploads acceptable, with an AbortIncompleteMultipartUpload lifecycle rule expected? `(needs human input)`

### listener-survives-fd-exhaustion — The replication-source listener outlives a transient accept() error

| | |
|---|---|
| **Type** | Safety |
| **Property** | After a burst of connections that exhausts file descriptors has drained, pull either serves new clients again or has exited. It never stays alive and collecting with a dead listener. |
| **Invariant** | SUT-side (missing) `UNREACHABLE("listener died on a non-shutdown accept error")` at the throw at `network_service.cpp:648-651`, logging the errno. Workload `Always("pull is not alive with a listener that stopped greeting after an fd flood")`. `Sometimes(accept hit EMFILE/ENFILE)` confirms the boundary was reached. `UNREACHABLE` because a listener dying on an accept error is itself the defect; `Always` because pull alive but not greeting is a violation whenever observed. |
| **Antithesis Angle** | A low fd limit (the entrypoint can set `ulimit -n 256`) plus workload-held pre-auth connections. asio retries only ECONNABORTED/EPROTO, so EMFILE, ENFILE, ENOBUFS and ENOMEM all kill the listener. The race is whether the collector's next `.tmp`/S3 temp open (which exits pull) or the next `accept()` (which kills only the listener) hits EMFILE first. Node hang or throttling of flood clients lengthens how long pre-auth sessions live. |
| **Why It Matters** | The replication source fails silently: the process stays up while no client is ever served. Lead N6. |

**Priority:** P1 · **Confidence:** high (code path verified; reaching it requires a lowered fd limit) · **Found by:** focus 5 · **Fault requirements:** none from Antithesis; **custom:** a workload connection flood and a lowered fd limit (`ulimit -n 256` in the entrypoint).

**Evidence:** `properties/listener-survives-fd-exhaustion.md`

**Open Questions:**

- Should concurrent sessions be capped? `(needs human input)`

### collector-memory-bounded-by-checkpoint — Collector buffer memory is bounded by the checkpoint size plus the open transaction

| | |
|---|---|
| **Type** | Safety |
| **Property** | With size checkpointing enabled, the collector's in-memory event buffer never exceeds `checkpoint_size` plus the open transaction plus one event, and PBS within its memory limit is never OOM-crash-looped by transactions below a documented size. |
| **Invariant** | SUT-side (missing) `ALWAYS("collector event buffer stays within checkpoint_size plus the open transaction plus one event")`, i.e. `event_buffer_.size() ≤ checkpoint_size_bytes_ + open-transaction bytes + one event`, after each `storage::write_event` (`storage.cpp:143-202`). This is the memory bound (it counts the open transaction); the durability backlog check at the same site is a distinct message owned by `checkpoint-bounds-unpersisted-data`. Workload `Always("no OOM kill of PBS when the largest source transaction is below the documented size")`, gated on the memory-limit variant. When the platform does not honour `mem_limit`, the safety half is checked by the SUT `ALWAYS` plus workload VmRSS sampling of the PBS process against the same ≈ 2 × (checkpoint_size + largest transaction) bound; telling an OOM SIGKILL apart from a harness SIGKILL uses the kill driver's log of every pid it signalled. `Sometimes(EOF reached after a ≥ 64 MiB transaction)`. Capacity hint: `SOMETIMES(event_buffer_.capacity() > 2 * checkpoint_size_bytes_ after a flush)` shows the retained-capacity state (memory kept until rotation). The memory limit is a harness parameter of about 2 × (checkpoint_size + largest transaction). `ALWAYS`/`Always` because the memory bound must hold after every write and a single OOM kill is a violation; `Sometimes`/`SOMETIMES` make the large-transaction and retained-capacity states exploration targets. |
| **Antithesis Angle** | A memory-limited PBS container, varied transaction sizes, and kills or restarts that cause re-delivery of the same big transaction. The interval checkpoint fires only on event arrival, so source pauses keep data buffered. |
| **Why It Matters** | Without checkpointing (the default), a whole binlog (up to 1 GiB) sits in RAM. A transaction larger than memory becomes an OOM crash loop with no progress. How much received data a kill loses (SG8) and the ticket 2 regression guard now belong to `checkpoint-bounds-unpersisted-data`. |

**Priority:** P2 · **Confidence:** medium · **Found by:** focus 5 · **Fault requirements:** **custom:** a container memory limit (or VmRSS sampling where it is not honoured); process kills and node hang on the source optional.

**Rationale / conflicts resolved:** Gap G3 split the durability half out into `checkpoint-bounds-unpersisted-data`, which owns the checkpoint/durability SUT check (unflushed backlog below `checkpoint_size`) and the recovery-point bound. Refinement R27 was applied with that split: this property is now only about memory, and its safety half no longer depends on the platform honouring `mem_limit` (SUT `ALWAYS` plus VmRSS sampling, with OOM kills told apart from harness kills by the kill-driver log).

**Evidence:** `properties/collector-memory-bounded-by-checkpoint.md`

**Open Questions:**

- Is whole-binlog RAM buffering under the default config intended? `(needs human input)`
- What memory limit will the PBS container have? `(partial: harness parameter ≈ 2 × (checkpoint_size + largest transaction); blocked on the catalog-wide container memory-limit question)`

### stale-sessions-reclaimed-within-bound — Pre-auth and idle dump sessions are reclaimed within a bound

| | |
|---|---|
| **Type** | Liveness |
| **Property** | An unauthenticated connection is closed within B = 2·(2·read_timeout) + 3·write_timeout (+ slack) of the server's last activity on it. A blocking dump session whose client vanished is released within `write_timeout` after the next flushed data. |
| **Invariant** | Bounded liveness checked with `Always`, evaluated only inside a quiet window (an `ANTITHESIS_STOP_FAULTS` window in which the harness quiet flag also pauses the harness's own fault drivers, proxy rules are cleared and no fsfault rule is armed), with the attacker's connect and the flush that should wake stale sessions started after the window begins and the bound calibrated against the instrumented debug build. Close time is measured on the `pbs` side (`ss` or `/proc/<pid>/fd` from a `pbs` helper), not by when the client sees EOF or RST, which network faults on pbs↔workload can delay or swallow. The silent attacker and the vanished dump client are harness-driven (a socket the harness stops servicing or closes without FIN), not a node hang. Workload `Always` messages: "unauthenticated connection closed within the pre-auth bound", "PBS established connections on the listener port do not exceed live clients after flush plus write_timeout". `Sometimes(attacker closed by server timeout)`, `Sometimes(stale dump sessions > 0 before the flush)`. SUT-side (missing): `REACHABLE("pre-auth session ended by read timeout")`, a logged pre-auth session duration (a measurement, not an assertion: a `Sometimes` on this failure state would be satisfied by the bug itself, and the binary cannot tell an injected `pbs` hang from starvation, so the bound is checked by the workload), an active-session gauge, `REACHABLE("dump session ended by write error at EOF wake-up")`. Bounded `Always` because an unbounded "eventually closed" cannot fail in a finite run, while one stale session surviving the bound in a quiet window is a violation; the `Sometimes`/`REACHABLE` markers prove the timeout and stale-session paths were reached. |
| **Antithesis Angle** | Slow-loris and silent attacker connections. Client container kills, hangs and partitions while the session sits at EOF: a blocking dump at EOF never touches the socket (`network_service.cpp:314-320`), and there are no heartbeats or keepalive, so a caught-up client that stops reading is never reclaimed while no new data arrives (lead (f) in "New defect leads"). Long idle source periods stretch the window. Timers share the io thread with blocking storage I/O, so S3 latency and throttling stretch the bound; native client faults, S3 latency and throttling therefore drive exploration only. |
| **Why It Matters** | There is no connection cap (`network_service.cpp:643-665`). Each stale session holds an fd and buffers, and idle sessions log at info every 500 ms. Accumulated sessions feed `listener-survives-fd-exhaustion` (N6). |

**Priority:** P2 · **Confidence:** medium · **Found by:** focus 5, 6 · **Fault requirements:** harness-driven silent and vanishing clients in quiet windows; kill, hang or partition of client containers, S3 latency, node throttling and network faults on PBS↔client for exploration only.

**Rationale / conflicts resolved:** Focus 6 typed its half Safety and focus 5 typed its half Liveness. Both are time-bounded "eventually closed" checks, so the merged property is Liveness, implemented with bounded `Always`. Both rated it P2.

**Evidence:** `properties/stale-sessions-reclaimed-within-bound.md`

**Open Questions:**

- What slack is acceptable for the pre-auth bound? `(needs human input)`
- Should concurrent pre-auth sessions be capped? `(needs human input)`
- Should PBS detect dead idle dump clients without new data? `(partial: no heartbeat, keepalive or read at EOF; ticket 20 asks for heartbeats, maintainer position unknown)`

## Reachability anchors

Markers that prove the fault-path properties above were exercised rather than passing vacuously.

### recovery-paths-exercised — Every recovery, resume and lifecycle branch is reached under the fault mix

| | |
|---|---|
| **Type** | Reachability |
| **Property** | Every recovery, fallback, resume-shape and lifecycle branch the workload targets fires at least once across the run, and the zero-size reopen fallback never fires. |
| **Invariant** | One distinct SDK call per branch, listed with file:line in the evidence table. `REACHABLE` is used for path markers and `SOMETIMES(cond)` where a meaningful condition exists (e.g. `bytes_discarded > 0`). One `UNREACHABLE("reopened tail binlog with zero size")` sits at `storage_core.cpp:713-718`. Branches covered: `.tmp` cleanup, tail truncation, magic-only reopen, reconnect reuse of the open binlog, closing an unterminated binlog, disconnect discard, pull retry iteration, destructor-swallowed flush (`REACHABLE("storage destructor swallowed a flush exception")` at `storage.cpp:74`), purge cleanup failure, `querying_only` record skip, S3 re-upload truncation. Resume shapes: discarded partial transaction; source names a different binlog than the tail, per mode; resume named an existing non-tail binlog (`:276`); rewrite fresh context on a non-empty storage. Lifecycle outcomes: fresh init, metadata-only open, encryption format stamped, `gtid_purged` bootstrap, rotation via ROTATE/STOP, empty-storage and magic-only dump polls, empty name resolved after waiting, silent session close, stop at EOF / by signal mid-stream / idle / read_timeout / exception, final flush failed, listener stopped vs died. Markers sit at the branch site and compile to no-ops outside the Antithesis build. Four rows are conditional on workload hooks and are omitted, not left always-failing, when the hook is absent: encryption format stamped, resume into a non-tail binlog, rotation via STOP, listener died. Until the SDK is linked, the workload asserts `Sometimes` on existing distinct log lines (PBS logs to the console via `cout_logger`, so every restart's log survives). At minimum the fallback covers tail truncation, magic-only reopen and purge cleanup failure (the last from purge's `warning` JSON status, because purge logs to `null_logger`), so that reachability coverage does not hinge entirely on accepting SUT macros. `REACHABLE` because each branch is a pure exploration anchor, `SOMETIMES(cond)` where a meaningful state must be seen at least once, and `UNREACHABLE` because a zero-size tail reopen is itself the defect. |
| **Antithesis Angle** | These are exploration anchors and replay checkpoints for rare transitions (a kill during recovery, a signal before `io_ctx.run()`, a source switch during a disconnect). They also give triage a timeline of which recovery step preceded a failure. |
| **Why It Matters** | Every fault-path safety property passes vacuously if its branch never runs. Without these markers, a green run cannot be told apart from one where faults never reached recovery code. |

**Priority:** P1 · **Confidence:** high (branches located) · **Found by:** focus 3, 7, 8, 9 (four drafts merged) · **Fault requirements:** the union of the fault-path properties' requirements.

**Rationale / conflicts resolved:** `resume-replay-paths-exercised` (focus 9), `lifecycle-outcomes-all-reached` (focus 8) and `collector-observes-source-binlog-switch-on-reconnect` (focus 7, P2) were merged here, with shared branches deduplicated to one message each. The merged property is P1. Focus 8 proposed `REACHABLE`s at the validator raise sites. They conflicted with the per-site `UNREACHABLE` owned by `kill-anywhere-storage-reopens`, so they were dropped as SDK calls. Deliberate-misconfig refusals are observed workload-side instead. Sites shared with other properties are owned there and referenced here: open-binlog reuse with `inprocess-reconnect-resumes-without-dup-or-gap`, rewrite non-empty resume with `rewrite-binlog-chain-valid-across-crashes`, and query record drop with `query-success-never-omits-middle-binlog`.

**Evidence:** `properties/recovery-paths-exercised.md`

**Open Questions:**

- Will the team accept SUT-side Antithesis macros in `src/` behind a CMake option? `(needs human input)`

## Candidates not cataloged (better tested elsewhere)

Deterministic findings from the investigation pass. Antithesis adds little to them, so they are recorded here for an MTR or unit test instead of becoming catalog properties.

- **`rewrite-without-checksum-verification-progresses`** (batch 9, from `rewrite-checksum-none-fail-stop`). Rewrite mode with `verify_checksum=false` against a CRC32 source likely fail-stops on the first transaction of every connection: the first GTID event is viewed without a footer, `rewriter` appends a second CRC, and body parsing fails (`event_view.cpp:60-63`, `rewriter.cpp:80-101`, `gtid_log_body.cpp:126-128`). It is a liveness gap, not corruption. It would be `Sometimes(rewrite fetch with verify_checksum=false stores a transaction)`, but it is deterministic, so an MTR combination with `$binsrv_verify_checksum = FALSE` in `resume_streaming` / `gtid_renumbering` is the cheaper confirmation.
- **Downgrade refusal checks** (batch 7, from `storage-format-version-mismatch-handled`). "Older binary refuses an encrypted storage" and "pull stream equals source after an older binary ran" fail deterministically against released binaries (pbs-0.3.1 never refuses, and it destroys the tail envelope, lead (g)). They stay in that property only as regression guards for a future version bump, if downgrade is declared supported; otherwise they belong in an MTR upgrade/downgrade test.
- **The `SET TIMESTAMP` variant of `search-by-timestamp-includes-all-early-files`** (batch 9). Under README semantics it fails from code alone (`search_helpers.cpp:75-77`), so an MTR test shows it more cheaply. The catalog keeps the property at P2 for the clock-fault and rewrite-stamp variants.

## Uncataloged tickets and leads

Tickets from the QA list and code-confirmed leads that deliberately have no property, with the reason. Recorded so that "considered and dropped" can be told apart from "missed".

- **Ticket 1** (an unparsable FDE server version terminates the parser). Deterministic parsing; on fetch it needs a malformed source FDE, and through pull it needs a corrupted stored FDE, which no planned fault produces. Unit or Hegel territory.
- **Ticket 3** (`gtid_set` has no GNO upper-bound check). Input validation of a parser; no timing component. Unit test.
- **Ticket 4** (Boost.ICL miscompiles with Apple clang). Build-environment issue; the harness builds with Clang 20 on Linux.
- **Ticket 5** (an oversized GTID tag is reported as `bad_alloc`). Deterministic parsing of a malformed tag that a real source never sends. Unit test.
- **Ticket 8** (the CTR counter does not carry into the IV's high half). With a random IV a carry needs the low 64 bits to wrap within one file (at most about 2^26 blocks for a 1 GiB binlog, out of 2^64), so a run practically never reaches it. Unit test of `cipher_context`.
- **Ticket 11** (`search_by_timestamp` accepts out-of-range time). CLI argument validation; deterministic. Unit or MTR test.
- **Ticket 12** (`%00` in a `file://` URI). Config parsing; deterministic and outside the fault model.
- **Ticket 16** (about 6 s of IMDS probing outside EC2). Environmental; the harness sets `AWS_EC2_METADATA_DISABLED=true`. It would only stretch startup bounds in `sigterm-honoured-within-bound`.
- **N13** (32-bit position cast when resuming a binlog over 4 GiB in position mode). Needs a binlog over 4 GiB, which the simulation's storage and time budget cannot reach. MTR or a targeted test with a large `max_binlog_size`.
- **The rewrite 999,999 sequence limit** (part of N11; `composite_binlog_name.hpp:30`). Reachable in about 17 minutes at 1 MB/s with `file_size=1024`. The topology regeneration pins the rewrite `file_size` floor high enough that a run cannot reach the limit (a topology change listed in `evaluation/synthesis.md`); an MTR test with a seeded sequence number covers the refusal.
- **A10** (a stale short-range read on S3 raises `out_of_range`). Excluded on purpose: the harness's S3 stand-in is Versity, which is strongly consistent. Recorded in `s3-whole-object-put-publishes-intended-bytes` (gap G6).
- **Pull without fsfault.** Not a ticket but a coverage note: `libfsfault.so` is applied only to test-command-launched `fetch`, and the PID-1 supervisor starts `pull` without `LD_PRELOAD`. Pull × filesystem × ENOSPC/EIO (N8 exit 0 after a failed final flush, ticket 18 through pull's destructor) is therefore reached only through the `s3` template's proxy until the supervisor can start pull under `LD_PRELOAD` with a per-restart rule (topology change).

## Assumptions

- **Kills are workload-driven.** The workload spawns `fetch`/`pull`/`purge_binlogs` as subprocesses and SIGKILLs or signals them after random delays. Source crashes and graceful restarts come from the mysqld restart-loop wrapper (`parallel_driver_kill_mysqld.sh`, SQL `SHUTDOWN`), and dump-thread kills from SQL `KILL QUERY`/`KILL CONNECTION`. The investigation pass confirmed that no property needs Antithesis node termination.
- **Native faults are network, process pause/throttle, and possibly clock.** Antithesis does not inject S3 5xx, ENOSPC or EIO natively. The repo's `extra/qa/s3_fault_proxy.py` and `fsfault.c`, a fault-injecting S3 stand-in, or size-limited volumes provide those, and every property that needs one marks it **custom**.
- **The build is debug or ASan**, so existing assert-only checks (`storage_core.cpp:707`, `storage.cpp:138, 278`, `indexed_event_block.cpp:43`) are live. The C++ Antithesis SDK would be added behind a CMake option, the way `WITH_PROPERTY_TESTS` gates Hegel.
- **Oracle scope.** Byte-identity oracles (`storage_holds_prefixes`, `check_complete_copy` from `extra/qa/fetch_crash_test.py`, reworked to take source-bytes and storage-bytes providers and to compare index suffixes after purges) apply to non-rewrite storages. Encrypted storages are decrypted by an independent Python decryptor, the canonical encrypted reader. Pull is never used as a reader of a storage it does not already own: it always builds a streaming storage and runs the collector (`pull_operation.cpp:108-153`), so a readback on a `fetch_fs` or `s3` storage runs only at quiescent points on a snapshot copy, with an unreachable source endpoint. Rewrite storages are checked by internal validity plus transaction-sequence equality. The source's own binlog bytes and `gtid_executed` are the ground truth; a workload ledger is only a cross-check.
- **The source retains its binlogs** for the whole run (large `binlog_expire_logs_seconds`, no PURGE unless it is a deliberate action; the deliberate purge-past-resume-point action belongs to `source-purge-past-resume-point-fails-closed` (gap G5), whose timelines are flagged and re-seeded) and runs with `sync_binlog=1` and `innodb_flush_log_at_trx_commit=1`. Source swaps, RESETs and rewinds (`source-rewind-never-silently-continued`) happen only in flagged runs, and the exactly-once oracles exclude those runs.
- **Ticket 14 is a known failure.** GTID-mode convergence checks must filter or track it, or it dominates every GTID-mode run.
- **The S3 stand-in (MinIO or Versity) is strongly consistent** for overwrite-then-read and list, and it supports `ListMultipartUploads`.
- **Downstream pull clients** are `mysqlbinlog`, a raw Python COM_BINLOG_DUMP client, or a second PBS `fetch`. Real MySQL replicas cannot be used (ticket 20). The raw client must send the exact checksum `SET` strings PBS recognises (`network_service.cpp:539-550`).
- **Quiescent observations** mean that no writer or purge is in flight on the storage. Observations taken while a writer runs are limited to atomic per-object reads and tolerate an untruncated tail beyond `metadata.size`.
- **One owner per assertion site.** Where several properties want an SDK call at the same line, one property owns it and the others reference it, so every planned assertion message is unique. The validator sites belong to `kill-anywhere-storage-reopens`, the `querying_only` record drop to `query-success-never-omits-middle-binlog`, the empty-records `front()` to `cli-json-envelope-and-exit-code-agree`, and the destructor catch to `recovery-paths-exercised` (REACHABLE), with the exit-0 UNREACHABLE in the operation layer.
- **SUT assertion messages are tallied per run, not per template.** Antithesis aggregates an assertion by its message across every timeline of a run, whatever template it ran in, and the binary cannot know which template or faults its timeline has seen. SUT sites that misuse (`contention`), source RESET, source SIGKILL or injected storage faults can reach therefore either carry a harness timeline tag in the message (read from a timeline-tag file the harness writes: template name plus flags such as `source_sigkilled`, `source_reset`, `storage_faults_active`), use the flag in the condition (`ALWAYS(cond || flag)`), or use one message per leftover shape or known defect, so a known failure never hides a new route into the same site.
- **Quiet windows.** `ANTITHESIS_STOP_FAULTS` pauses only Antithesis-injected faults. Every time-bounded `Always` is evaluated in a window where a harness quiet flag also pauses the harness's own fault drivers (kill drivers, proxy rules, fsfault rules, SIGSTOP), the measured action starts after the window begins, and any latency in play is a harness-bounded stimulus whose duration is part of the bound. Bounds are calibrated against a measured baseline of the coverage-instrumented debug build. Native latency, hang and throttling drive exploration-only `Sometimes` measurements.
- **Known defects.** About 16 properties are expected to fail on today's code, and several of their triggers wedge the storage for the rest of the timeline. How they are contained (per-defect avoid-trigger knobs, a wedge detect-and-reset helper, variant builds with fixes) is bias B3 in `evaluation/synthesis.md`, awaiting a human decision.
- **`server_id` allocation.** Every PBS process role gets a distinct `replication.server_id`, and oracle `mysqlbinlog` dumps run without `--stop-never` (server_id 0, never evicting) or with a unique `--connection-server-id`. A shared id appears only where `distinct-storages-shared-server-id-stay-consistent` or the same-id arm of `second-writer-on-storage-fails-closed` wants it.

## Open Questions

The investigation pass resolved most code-level questions. What remains is mostly contract questions for the maintainers. Each recurs across several properties and is listed once here, with the properties it blocks. All are `(needs human input)` unless tagged otherwise.

1. **Are concurrent writers, or purge beside a writer, in contract?** README:473 says only "avoid" purge alongside fetch/pull and is silent on two writers and on concurrent purges. Today these run only in a separate misuse phase. Blocks: `second-writer-on-storage-fails-closed`, `other-process-startup-spares-live-writer`, `stale-index-commit-never-wedges-storage`, `stored-binlog-byte-prefix-of-source`, `stored-state-never-regresses`, `encrypted-storage-decrypts-to-source`.
2. **Is byte identity with the source an official guarantee, or an MTR convention?** Maintainer MTR checks bytes only in position mode. Blocks: `stored-binlog-byte-prefix-of-source`, `stored-transactions-exactly-once-across-resumes`, `gtid-resume-preserves-source-rotations`, `source-upgrade-resume-continuity`.
3. **Fail-stop or recovery: is an unopenable storage after an interrupted rotation or purge accepted?** Maintainer code treats improper shutdown as recoverable, and a purge comment accepts "manual attention". Blocks: `kill-anywhere-storage-reopens` (and its ticket 15 fix direction), `interrupted-purge-leaves-openable-storage`, `stored-rotate-or-stop-is-last-event` (fix choice). `(partial: rotation intent unstated)`
4. **What does exit 0 mean?** Is fetch exit 0 a complete copy at EOF, is exit 0 after a failed final flush intended (N8), and should a purge `warning` exit 0? Blocks: `graceful-exit-leaves-flushed-storage`, `cli-json-envelope-and-exit-code-agree`, `interrupted-purge-leaves-openable-storage`.
5. **Are source swap, RESET and failover in scope?** Should PBS fail closed or is source continuity the operator's job, and is a merged history acceptable? Whether source-swap/RESET actions share a template with the exactly-once runs also decides how the resume guards are implemented. Blocks: `source-swap-never-silently-accepted`, `source-rewind-never-silently-continued` (where a `server_uuid` check is not enough), `gtid-failover-no-lost-transaction-merge`, `gtid-failover-resume-progresses`, `graceful-exit-leaves-flushed-storage` (ticket 25 scenarios), `resume-continuity-guard-never-trips`.
6. **Are rewrite toggling, source upgrade and binary upgrade/downgrade supported?** Blocks: `rewrite-toggle-between-runs-keeps-storage-resumable`, `rewrite-binlog-chain-valid-across-crashes`, `source-upgrade-resume-continuity`, `storage-format-version-mismatch-handled`.
7. **Will SUT-side Antithesis macros in `src/` (behind a CMake option, no-ops outside the Antithesis build) be accepted?** If not, the SUT assertions fall back to weaker log-line checks. Blocks: `recovery-paths-exercised`, `pull-served-stream-self-consistent` (CRC check), and every SUT-side assertion in the catalog.
8. **Tenant faults: are node termination and clock faults enabled?** No property needs node termination any more (all kills are workload-driven). Clock faults are optional for `binlog-metadata-matches-stored-bytes`, `rewrite-pitr-by-datetime-matches-source` and `search-by-timestamp-includes-all-early-files`.
9. **Does any fault drop un-fsynced page cache?** If none does, ticket 19 stays unreachable. Blocks: `kill-anywhere-storage-reopens`.
10. **What memory limit will the PBS container have?** Blocks: `collector-memory-bounded-by-checkpoint`, the resource angle of `large-event-served-or-rejected-cleanly`. `(partial: harness parameter ≈ 2 × (checkpoint_size + largest transaction))`
11. **Is `purged_gtids_ = front().added_gtids` (`storage_core.cpp:1001-1004`) intended, or was `previous_gtids` meant?** Nothing reads it on a non-empty storage today, so no property depends on it; it matters only if purge plus an emptied storage is ever exercised. Moved here from `binlog-metadata-matches-stored-bytes`.
12. **Is removing a KEK from the keyring a supported fail-stop?** Every indexed file's KEK is validated at open (`storage_core.cpp:899-903, 927-953`), so the workload keeps all KEKs. Related: is per-file encryption mixing (N9) intended? Blocks: `mixed-encryption-storage-served-correctly`, `encrypted-storage-no-plaintext-at-rest`.
13. **Service model of pull:** is it expected to run as PID 1 or under a supervisor, should an S3 network error during a checkpoint end pull (README:378 says network errors are retried), and should concurrent sessions be capped? Blocks: `pull-chain-converges-after-heal`, `sigterm-honoured-within-bound`, `listener-survives-fd-exhaustion`, `stale-sessions-reclaimed-within-bound`.
14. **What are the S3 CRT retry count and timeouts in SDK 1.11.774?** `(partial: default ClientConfiguration with no overrides; connect 1000 ms; throughput monitor fails a transfer below 1 B/s over ≥ 3 s; no overall request timeout; QA shows 1 failure retried and 6 consecutive failures exhausting; not runtime-verified)`. Blocks reachability estimates in `failed-flush-never-reappends-data`, `stored-state-never-regresses`, `sigterm-honoured-within-bound`, `pull-chain-converges-after-heal`, `query-success-never-omits-middle-binlog`, `interrupted-purge-leaves-openable-storage`.
15. Should the workload reuse the Python/Hypothesis oracles from `extra/qa` (`storage_holds_prefixes`, `check_complete_copy`), or re-implement them in a lighter client? This is a workload-planning choice, not a contract question. `(partial: the evaluation pass found that only `parse_events`, `transaction_boundaries` and the constants are reusable as-is; `storage_holds_prefixes` and `check_complete_copy` are state-machine methods that read local paths and assume the index starts at the source's first binlog, so the topology plans a small provider-based oracle library)`
16. **Evaluation biases B1-B5** (`evaluation/synthesis.md`) need human decisions: whether the unreleased pull listener is the release target (B1), whether out-of-contract concurrency stays (B2), how expected-to-fail wedges are contained (B3), where deterministic properties live (B4), and whether SUT-side macros and about ten small product-code changes are acceptable (B5, which extends question 7).
