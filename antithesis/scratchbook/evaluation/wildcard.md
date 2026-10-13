---
sut_path: /Users/hrvoje/work/percona-binlog-server
commit: 99de68207c84f83e0d477ebe63d2f531facbb954
updated: 2026-10-13
external_references:
  - path: https://claude.ai/artifact/9om6fiPd64QzVXbEWEtDT1
    why: QA bug-ticket list for binlog_server (local snapshot antithesis/scratchbook/.tickets-snapshot.txt); open tickets used as leads, each validated against the code before use
---

# Evaluation lens 4: Wildcard

Inputs read: `property-catalog.md` (all 60 entries), `sut-analysis.md`, `deployment-topology.md`, `existing-assertions.md`, `property-relationships.md`, and selected evidence files. Code checked: `src/operations/pull_operation.cpp`, `src/binsrv/` logger files, `extra/qa/fetch_crash_test.py`, `extra/qa/event_variety_test.py`. MySQL server and client source were checked in the local PXC tree (`~/work/percona-xtradb-cluster`: `sql/rpl_source.cc`, `sql/rpl_binlog_sender.cc`, `client/mysqlbinlog.cc`).

Findings are ordered catalog-wide first, then property-specific.

---

## W1. SUT-side assertions cannot be scoped to a template, so "misuse phase" and "gated" assertions leak across the whole run (catalog-wide)

**Properties:** `kill-anywhere-storage-reopens` (owns the validator-site UNREACHABLEs), `interrupted-purge-leaves-openable-storage`, `second-writer-on-storage-fails-closed`, `stale-index-commit-never-wedges-storage`, `other-process-startup-spares-live-writer`, `encrypted-storage-decrypts-to-source`, `gtid-resume-preserves-source-rotations`, `resume-continuity-guard-never-trips`, `pull-chain-prefix-consistency`.

**Concern.** The catalog and topology rely on two isolation devices:
- The `contention` template / "separate misuse phase" is "kept apart so that expected failures here do not hide regressions in the in-contract templates" (topology §Template contention).
- SUT assertions are "enabled only in timelines without source SIGKILL" (`gtid-resume-preserves-source-rotations`), "only in runs without S3 or storage faults" (`pull-chain-prefix-consistency`), or are sound only "in timelines without source RESET" (`resume-continuity-guard-never-trips`).

Antithesis aggregates an assertion by its message across every timeline of a run, whatever the template. A SUT `UNREACHABLE` that fires in a `contention` timeline therefore marks the property failed for the whole run. The binary also has no way to know which template, or which faults, its timeline has seen unless the harness tells it.

The single-owner rule makes this worse. The validator-site UNREACHABLEs owned by `kill-anywhere-storage-reopens` (`storage_core.cpp:779-783, 974-977, 993-996`) are tripped by at least four different root causes:
- ticket 15 (interrupted rotation, expected to fail today);
- N3 (purge `warning` leftovers, expected to fail by design);
- ticket 9 and ticket 24 (contention template, by design);
- N1 truncation (misuse phase).

Ticket 15 and N3 alone keep the message red in every run, so a new route into the same validators is invisible.

**Suggested action.**
- (a) Have the harness write a timeline-tag file (template name, plus flags such as `source_sigkilled` and `s3_faults_active`) that the SDK wrapper reads. Put the tag into the assertion *message*, not into `details`, for SUT sites that misuse or known bugs can reach. The alternative is to run `contention` and source-swap as separate launches.
- (b) Give the validator sites one message per leftover shape: an unindexed data object of size 4 (rotation), metadata without an index entry (purge), and a tail larger or smaller than its metadata (writer overlap). The cause then becomes visible in the property name.
- (c) Implement the "gated" UNREACHABLEs as `ALWAYS(cond || harness_flag)` with the flag read from that file, or move them to the workload side.

## W2. "Read encrypted storages through pull" makes the oracle a writer (catalog-wide, oracle design)

**Properties:**
- `stored-binlog-byte-prefix-of-source`, `stored-transactions-exactly-once-across-resumes` ("encrypted storages: read through pull");
- `stored-rotate-or-stop-is-last-event` ("decrypted through pull first");
- `encrypted-storage-decrypts-to-source`, `mixed-encryption-storage-served-correctly`.

**Concern.** Pull always builds storage in `streaming` mode and always runs the collector (`pull_operation.cpp:108-115, 125-153`; checked). On a `fetch_fs` or `s3` storage, an oracle pull therefore:
- runs constructor recovery (deleting `.tmp` objects and truncating the tail: N1 and N2 by the oracle itself);
- connects to the source and **appends new data** to the storage it is meant to check;
- if it overlaps a running fetch, is itself the ticket 24 second writer.

In `pull_fs` this is not an issue, because the pull under test is the reader. There, though, the encrypted-storage verdicts inherit pull's own known failures (N5 and ticket 10, which the catalog already works around with per-file dumps and an event-size cap). Either way, the storage-integrity verdict for encrypted configs depends on the least-settled component.

**Suggested action.**
- Make the independent Python decryptor (already planned in `encrypted-storage-decrypts-to-source`, about 30 lines, AES KEKs only) the canonical reader that every storage-integrity oracle uses.
- Keep the through-pull comparison only as the SG9 serving check.
- If a pull readback is wanted on fetch storages, run it on a **snapshot copy**, with `connection.host` set to an unreachable endpoint. Connect failures are retried in the idle loop, so the listener still serves the copy and nothing is appended.

## W3. Known-failure containment is missing, so expected-to-fail properties will dominate runs (catalog-wide)

**Properties:** the 12 properties the catalog marks "expected to fail today". Wedge-type failures are the critical ones:
- `kill-anywhere-storage-reopens` (ticket 15): permanent wedge;
- `source-crash-restart-resume-is-faithful` and `binlog-metadata-matches-stored-bytes` (lead (b)): the debug build aborts at `storage.cpp:138` "on every retry";
- `interrupted-purge-leaves-openable-storage` (N3): permanent wedge;
- `rewrite-toggle-between-runs-keeps-storage-resumable`: wedge.

**Concern.**
- Once a timeline wedges, every later check in it either fails for the same reason or passes vacuously:
  - the convergence checks (`eventually_converge`, `eventually_pull_converges`);
  - `pull-chain-converges-after-heal`;
  - every pull serving property;
  - the supervisor outcome-log checks.
- The supervisor turns lead (b) into an infinite abort loop.
- The default fault and workload mix makes these triggers frequent. `fetch_fs`, `pull_fs` and `s3` all include `parallel_driver_kill_mysqld.sh`, the wrapper issues `SHUTDOWN`, rotations are frequent, and kills are random.

The catalog says how to *observe* each known bug. It says nothing about how to *keep exploring* after one.

**Suggested action.** Add a workload-side "known-wedge classifier and repair" helper:
- recognise each known signature (unindexed size-4 object; abort at `storage.cpp:138`; purge leftovers);
- assert it once under its own ticket-named message;
- apply the documented manual repair (drop the orphan, or re-initialise into a fresh storage subdirectory) so the timeline continues.

Also add per-launch switches that suppress individual known triggers, for example SIGKILL-only versus `SHUTDOWN` in the mysqld wrapper, and no kill during rotation. This allows "regression-hunting" runs alongside "known-bug confirmation" runs.

## W4. Missing failure scenario: source rewind (restored from its own backup), with the same server_uuid and reused GNOs (catalog-wide gap)

**Concern.** The source-identity scenarios in the catalog are:
- swap to an unrelated server or `RESET` (`source-swap-never-silently-accepted`);
- failover to a replica with a different UUID (`gtid-failover-*`).

The case that is most typical in Percona's world is missing: the source is **restored from an earlier physical backup** (xtrabackup or a datadir snapshot) and keeps its `server_uuid` (`auto.cnf` is in the datadir), while PBS keeps pulling. This is exactly the moment a binlog server is used for PITR.

Checked in `rpl_binlog_sender.cc:918-921`: the server rejects a GTID dump only while the requested set holds more of the **server's own UUID** than it has executed (`ER_REPLICA_HAS_MORE_GTIDS_THAN_SOURCE`). The sequence is:
1. PBS has stored `uuid:1-100`, and the source is rewound to `uuid:1-80`.
2. PBS fails closed until the rewound source commits 20 new transactions, which reuse GNOs 81-100 with *different content*.
3. The subset check then passes, and PBS silently continues from 101.

The storage now holds GTIDs whose content no server has. Both a UUID-based ticket 25 fix and the source-swap oracle (epoch tracked by identity) would miss this.

**Feasibility.** No new container is needed. The mysqld restart wrapper can copy the datadir while mysqld is stopped (on a `SHUTDOWN` cycle), and later restore that copy before a restart, driven by a marker file.

**Suggested action.** Add a property along the lines of `source-rewind-never-silently-merged`: `Always("no stored GTID whose content differs from the source's transaction with the same GTID")`, with the oracle comparing per-GTID payload hashes. Alternatively, add a rewind action and per-GTID content check to `source-swap-never-silently-accepted`, and record that a server_uuid-only fix does not close it.

## W5. Missing perspective: restorability. No oracle applies the backup (catalog-wide)

**Concern.** Every oracle compares bytes, GTID sequences or served streams. The user's actual workflow is `mysqlbinlog <stored files> | mysql` into a restored server. This matters most where the byte oracles are weak:
- rewrite mode, where CRCs are recomputed and `last_committed`, `transaction_length` and the preamble events are regenerated;
- `stored-rotate-or-stop-is-last-event` (does a restore actually skip the trailing transactions?);
- XA transactions and compressed `TRANSACTION_PAYLOAD` transactions (see W8).

An apply-based oracle checks semantics in the target the user cares about. It also answers several "does anything downstream reject this?" open questions empirically (`stored-rotate-or-stop-is-last-event` OQ1, `rewrite-binlog-chain-valid-across-crashes`).

**Feasibility.** The image already carries the MySQL generic tarball for `mysqlbinlog`, and that tarball contains `mysqld`. A `finally_` or `eventually_` command can start a scratch mysqld, replay the stored binlogs (decrypted offline per W2) from a fresh, empty initial state, and compare `CHECKSUM TABLE` for the workload schema with the source.

**Suggested action.** Add one P1 end-to-end property, `stored-binlogs-replay-to-source-state`, run per mode (position, GTID, rewrite, encrypted).

## W6. Feasible reformulation: recovery-point (checkpoint durability, SG8) is never asserted, and its best assertion is buried in a blocked P2 (cross-lens)

**Properties:** `collector-memory-bounded-by-checkpoint` (P2, blocked on the container memory-limit question).

**Concern.** SG8 (README:619, 626, "written permanently" at size or interval boundaries) is the RPO guarantee: how much received data a kill can lose. No property asserts it (grep for SG8, RPO and freshness finds nothing in the catalog). `collector-memory-bounded-by-checkpoint` already proposes the right SUT check, `ALWAYS(event_buffer_.size() ≤ checkpoint_size + open transaction + one event)`. That check *is* the size half of the RPO bound and needs no memory limit. Because it sits in a P2 that depends on `mem_limit` support, it will likely never be built.

The interval half is a genuine expectation gap. The interval checkpoint fires only when an event arrives (LG3), so the real bound is roughly `max(checkpoint_interval, read_timeout)`, not `checkpoint_interval`.

**Suggested action.** Split out a P1 `checkpoint-bounds-unpersisted-data` property:
- the SUT `ALWAYS` above, with no memory limit needed;
- workload `Always("a transaction committed at t is in the storage by t + checkpoint_interval + read_timeout + δ while PBS is connected")`, evaluated under `ANTITHESIS_STOP_FAULTS`.

Leave only the OOM and crash-loop half in the P2.

## W7. Usage pattern the workload cannot represent: retention with a continuously running pull (catalog-wide, framing)

**Concern.** README:473 says purge alongside fetch or pull should be avoided. For a 24/7 `pull`, the only in-contract retention workflow is therefore: stop pull → purge → restart pull, with downstream clients reconnecting. `pull_fs` has no purge driver at all (topology §Template pull_fs), and purge with pull appears only in the misuse phase.

That leaves several things untested:
- the in-contract retention cycle;
- clients asking for just-purged files after the restart (`dump-session-ends-with-eof-or-err-packet`, `dump-preamble-matches-request`'s "empty name after purge" marker);
- pull's startup recovery on a freshly purged storage.

Framing point for the humans: if retention requires downtime of the replication source, operators will run purge beside pull anyway. That makes contract question #1 more urgent than "avoid" suggests.

**Suggested action.** Add a `serial_driver_retention_cycle` to `pull_fs`. It removes `pull.enabled`, SIGTERMs pull, waits for exit, runs the purge (with fsfault and kill options), and re-enables pull. Flag in the catalog that the answer to question #1 for *pull* largely decides whether the misuse phase is really misuse.

## W8. Event-type variety is absent from the catalog's workload model (catalog-wide)

**Concern.** The load generator is described as "varied sizes, occasional large events, `FLUSH BINARY LOGS`, idle gaps", plus tagged GTIDs and checksum toggles. The repo's own `extra/qa/event_variety_test.py` exists precisely because "the crash test only writes plain row-based transactions" and the collector "has to place [other kinds] into transactions and checkpoint at the right boundaries". It covers:
- `binlog_transaction_compression` (`TRANSACTION_PAYLOAD` wraps a whole transaction);
- XA PREPARE / XA COMMIT (two transaction groups);
- PARTIAL_JSON;
- MINIMAL row image.

DDL (GTID + Query, with no BEGIN/XID) is not mentioned either. Boundary detection is length-based (`reader_context.cpp:949-1019`). The rewriter regenerates `transaction_length` and the logical clock. An XA PREPARE without its COMMIT at a kill, rotation or file-size boundary is exactly the kind of combination no single property constructs.

**Suggested action.** Make event kind a per-transaction random dimension in `parallel_driver_source_load.py` (compression per session, XA, DDL, JSON partial updates, tagged GTIDs on ≥ 8.4). Add `Sometimes` markers per kind, so every boundary-sensitive property (`metadata-size-at-transaction-boundary`, exactly-once, the rewrite properties) is evaluated over them. This costs nothing in new properties.

## W9. Harness hygiene: server_id allocation is unspecified, and same-id clients evict each other (catalog-wide)

**Concern.** `kill_zombie_dump_threads` (`rpl_source.cc:1145-1172`; checked) evicts an existing dump thread with the same server_id whenever the new client sets no replica UUID and its server_id ≠ 0. The topology has several clients that dump from the source, and none sets a UUID:
- pull;
- fetch;
- `workload`'s own `binlog_server` (contention S3 writer);
- the `distinct-storages` second instance;
- the oracles' `mysqlbinlog --read-from-remote-server`.

`mysqlbinlog` uses server_id 0 (not evicting) unless `--stop-never` is passed (then it defaults to 1) or `--connection-server-id` is set (`mysqlbinlog.cc:2639-2672`). Two PBS processes generated from one config template will evict each other in *every* template, not only in the one property that wants it (`distinct-storages-shared-server-id-stay-consistent`). This injects ER 1236 exits that look like product failures.

**Suggested action.** Have the harness allocate server_ids deliberately: a distinct id per PBS process role, oracle `mysqlbinlog` without `--stop-never` (id 0), and a shared id only in the dedicated property's timelines. Add a workload `Sometimes`/`Always` pair on "unexpected dump-thread eviction" (from the source's error log or `performance_schema`).

## W10. Debug + coverage build combined with default throttling makes wall-clock-bounded liveness `Always`s noisy (cross-lens)

**Properties:** `sigterm-honoured-within-bound`, `stalled-dump-client-isolated`, `stale-sessions-reclaimed-within-bound`, `pull-chain-converges-after-heal` (T bound).

**Concern.**
- These are bounded `Always` checks with fixed bounds (`read_timeout + 1 s`, `write_timeout + δ`, `2·(2·read_timeout) + 3·write_timeout`). They run against an unoptimised, coverage-instrumented debug binary.
- `sigterm-honoured-within-bound` lists node throttling as a *desired* fault, and node throttling is on by default.
- CPU throttling of the `pbs` container stretches any wall-clock bound without any product defect. The "fault-free links" gates in `stalled-dump-client-isolated` exclude network faults but not throttling.
- The result is a property that fires in most runs for environmental reasons, and is then ignored, including when it fires for a real reason.

**Suggested action.** Evaluate the fixed-bound `Always`s only inside `ANTITHESIS_STOP_FAULTS` windows, with the bound calibrated against a measured baseline of the instrumented build. Keep the "stretched by storage latency" angle as an attributed measurement: `Sometimes(exit latency > bound)` with the in-flight storage operation duration in details. That turns the README:719 contract question into data rather than a red property.

## W11. Topology and catalog disagree; the topology references 25 slugs that no longer exist (catalog-wide, odd)

**Concern.** The topology still uses pre-merge draft slugs. The workload author will build from it. Slugs with no `properties/<slug>.md` (checked):

- `blocking-dump-catches-up-after-io-stall`
- `encrypted-binlog-decryptable-after-crash`
- `enospc-write-fails-closed-without-duplicates`
- `failed-flush-retry-never-duplicates`
- `lifecycle-outcomes-all-reached`
- `listed-binlog-size-is-stable-readable-prefix`
- `metadata-timestamps-match-stored-events`
- `pull-fde-switch-across-format-change`
- `pull-reconnects-after-transient-source-fault`
- `purge-ctor-never-truncates-live-tail`
- `purge-during-writer-keeps-storage-openable`
- `query-concurrent-with-writer-never-errors`
- `query-read-error-not-reported-as-success`
- `restart-after-kill-converges-to-source`
- `resume-replay-paths-exercised`
- `s3-local-buffer-bounded-across-crashes`
- `s3-no-orphaned-multipart-uploads`
- `s3-pull-converges-despite-reupload-growth`
- `s3-stale-put-replay-never-regresses-object`
- `sender-reads-only-flushed-bytes`
- `sigterm-not-starved-by-storage-lock`
- `source-restart-reauth-liveness`
- `startup-tmp-cleanup-spares-live-writer`
- `storage-reopens-after-writer-crash`
- `zombie-writer-takeover-fails-closed`

The documents also disagree on node termination. The topology's fault table calls node termination of `pbs` "the primary crash fault" and lists properties that need it. The catalog's Assumptions say "no property needs node termination".

**Suggested action.** Re-sync the topology to the merged catalog slugs, using the merge map in each catalog Rationale. Make the node-termination stance consistent: optional, adding only the "all processes die together" case.

## W12. The P0 tier rests partly on an unreleased feature whose contract is still open (framing, odd)

**Properties:** `sender-file-switch-emits-artificial-rotate` (P0), `pull-served-stream-self-consistent` (P0), and 9 more serving properties.

**Concern.** The catalog itself notes that "no released tag contains the replication-source listener" (`storage-format-version-mismatch-handled`). The main client, MySQL replicas, cannot use it (ticket 20). Several of its behaviours are still owner decisions:
- wait or ERR for ahead-of-flushed requests;
- heartbeats;
- the GTID dump;
- the fix direction for N5.

About 18% of the catalog targets this surface. That is defensible, because it is the least-settled code and it is better to catch bugs before release. The cost is that these properties will churn as the contract is decided, and two of them hold P0 slots that otherwise go to released-path integrity.

**Suggested action.** Not a change for this lens to make. Ask the owners whether pull serving is the release target for this Antithesis effort. If it is not, demote the two serving P0s to P1 until the wait/ERR/heartbeat contract is fixed.

## W13. Smaller property-specific notes

- **`stored-transactions-exactly-once-across-resumes`, a source-retention interaction worth a regression guard.** The oracle excludes "runs where the source purged past PBS's position". The evidence file traces fail-closed behaviour: a non-2013 `core_error` leads to a non-zero exit. That is a real production path (PBS down longer than retention), so it deserves a cheap `Always("writer never exits 0 or stores a gap after the source purged binlogs it still needed")` under a deliberate `PURGE BINARY LOGS` action, rather than only an exclusion. A crash loop under the supervisor is the expected outcome, and the property should say so explicitly.
- **Triage evidence and logging.** `file_logger` truncates on open (W11 in sut-analysis), and the supervisor restarts pull. Under a crash loop each restart erases the previous crash's log. `cout_logger.cpp` exists, so the harness config should log to the console so Antithesis captures every incarnation. This is not a property, but it decides whether lead (b) or ticket 15 failures can be triaged.
- **Ledger location.** The ledger lives in `workload:/data/ledger.jsonl` (topology `fetch_fs`), but `eventually_converge.py` runs in `pbs` "against the ledger", and there is no shared volume. Commits whose acknowledgement was lost to a network fault are also ambiguous in a client ledger. Source binlog bytes should be the ground truth for exactly-once, with the ledger used only as a cross-check.

---

## Passes

- The catalog counts are consistent: 60 properties, 12 P0 / 32 P1 / 16 P2, and 50 safety / 8 liveness / 2 reachability, matching the overview table.
- `binlog_as_sent()` correctly models IN_USE clearing and FDE CRC recomputation (`fetch_crash_test.py:217-234`), so `mysqlbinlog --raw` as the source-bytes oracle is consistent with it.
- Oracle `mysqlbinlog --read-from-remote-server` without `--stop-never` uses server_id 0, which `kill_zombie_dump_threads` skips (`rpl_source.cc:1148`), so the default oracle does not evict PBS.
- Clock faults hit all containers equally. No property depends on inter-container clock skew: the rewrite-stamp issues come from lag, and search-by-timestamp from backward jumps. The catalog's clock usage is compatible.
- The `ticket 18` cluster dedup, with a root-cause UNREACHABLE plus downstream observers, is a good shape. The "uniform-size silent skip" refinement from the earlier wildcard pass is preserved.

## Uncertainties

- Whether Antithesis groups SUT-side assertions per launch or allows any per-template scoping. W1 assumes per-message aggregation across all timelines of a run, as documented for the SDK.
- Whether the rewound-source scenario (W4) is in scope for the owners. It is the same contract question as ticket 25 (open question 5), but a server_uuid-based fix would not cover it.
- Whether a scratch `mysqld` from the generic tarball starts inside the `workload` image without additional libraries (libaio and others are only in the `pbs` deps stage). This affects the cost of W5.
- Whether the Clang 17–20 SDK assertion workaround (`-flto=thin`) changes debug-build timing further. This affects the calibration in W10.
