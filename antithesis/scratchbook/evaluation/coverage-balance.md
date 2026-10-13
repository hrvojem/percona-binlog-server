---
sut_path: /Users/hrvoje/work/percona-binlog-server
commit: 99de68207c84f83e0d477ebe63d2f531facbb954
updated: 2026-10-13
external_references:
  - path: https://claude.ai/artifact/9om6fiPd64QzVXbEWEtDT1
    why: QA bug-ticket list for binlog_server (local snapshot antithesis/scratchbook/.tickets-snapshot.txt); open tickets used as leads, each validated against the code before use
---

# Evaluation lens 2: Coverage Balance

Inputs read: `property-catalog.md` (overview table, all section headers, summary, candidates-not-cataloged, assumptions, open questions, and selected property bodies), `sut-analysis.md` and `deployment-topology.md` in full, `property-relationships.md`, and the evidence files of `query-success-never-omits-middle-binlog`, `pull-client-version-matrix`, `recovery-paths-exercised`, `stored-transactions-exactly-once-across-resumes`, `kill-anywhere-storage-reopens`, `interrupted-purge-leaves-openable-storage`, the failover/swap/upgrade group, and the ticket snapshot. Method: map every SUT-analysis risk item (SG1-9, LG1-6, N1-N13, A1-A14, W*, the 13 ranked attack surfaces, the 26 tickets) to catalog properties by grep plus reading, then compare the property portfolio against the topology's templates and config model.

## Portfolio shape (counts from the overview table)

| Category | Props | P0 | Notes |
|---|---|---|---|
| Storage integrity | 7 | 5 | Matches attack surfaces 1, 2 (top of sut-analysis ranking) |
| Multi-process | 4 | 1 | 3 of 4 hinge on contract Q1; run only in `contention` |
| Resume & replay | 5 | 1 | Good spread across resume routes |
| Pull serving | 11 | 2 | Largest group; sender is "least settled code" — justified |
| Query & purge | 5 | 1 | |
| Encryption | 4 | 0 | Bug history says "quiet, well tested"; crash-untested |
| Rewrite | 6 | 1 | Bug history says "quiet"; 6 props is heavy relative to that |
| Source & version | 10 | 1 | 5 of 10 need deferred templates or fixtures |
| Liveness & resource | 7 | 0 | |
| Reachability | 1 | 0 | Aggregate anchor with ~40 markers |

Type mix: 50 safety / 8 liveness / 2 reachability. Assertion mix in the overview's Assertion column: 13 rows carry a Sometimes/SOMETIMES, 2 REACHABLE, 16 Unreachable, 2 AlwaysOrUnreachable, 3 "Always (bounded)". 18 of 59 rows need a **custom** fault; 3 use clock; none needs node termination.

Overall the portfolio is weighted the right way: the top three surfaces in sut-analysis (multi-object storage consistency, pull's single io thread / sender, source-side change) carry the most and the highest-priority properties. The findings below are about the edges.

## Findings

### F1. Config is drawn once per timeline, but ~6 properties need config to change between runs (catalog-wide)

- **Properties:** `encrypted-storage-no-plaintext-at-rest`, `mixed-encryption-storage-served-correctly`, `rewrite-toggle-between-runs-keeps-storage-resumable`, `query-modes-never-write-storage` (needs a query config that differs from the writer's), `storage-format-version-mismatch-handled` (old binary/fixture), `rewrite-checksum-none-fail-stop` (checksum toggle).
- **Concern:** The fault-requirement column for these says "config variation" / "kill + config variation", and their triggers (N9, N11, lead (e)) are *transitions* between configs. `deployment-topology.md:156` says each template "draws its config dimensions once per timeline ... and stores them in `/data/<template>/config.json`". No template command mutates config between runs. As planned, these properties never see their trigger state and pass vacuously. This is the "lifecycle transition" gap pattern: the catalog names the transition, the topology only models steady state.
- **Evidence:** topology lines 156-164 (dimension list, drawn once); catalog overview rows for the six slugs; SG6/N9 in sut-analysis line 100 describe toggling as the weak spot.
- **Suggested action:** Either add a `parallel_driver_reconfigure.py` (between writer runs, under the writer lock, flip encryption / rewrite / base_file_name / mode with Antithesis randomness) to `fetch_fs` and `pull_fs`, or add a per-run override in `ensure_state.py`. Give each transition a `Sometimes("writer restarted after <dimension> changed")` so vacuity shows in triage.

### F2. Five Source & version properties (one P0) depend on deferred templates or fixtures (catalog-wide)

- **Properties:** `source-swap-never-silently-accepted` (P0), `gtid-failover-no-lost-transaction-merge`, `gtid-failover-resume-progresses`, `source-upgrade-resume-continuity`, `storage-format-version-mismatch-handled`; partly `distinct-storages-shared-server-id-stay-consistent` (its fault column says "two PBS containers"; the topology has one `pbs` container, though two pulls inside it would do).
- **Concern:** The P0 list advertises 12 P0s, but one of them cannot run in the minimal topology (`deployment-topology.md:221` defers `failover`; the RESET variant's placement is still an `[open]` at line 276). Source & version is the second-largest category (10) yet about half of it is unrunnable in the first launches, so the effective portfolio for the first runs is ~54 properties and the "source-side change" surface (ranked third in the catalog summary) is mostly represented by `source-crash-restart-resume-is-faithful`, `source-auth-survives-auth-cache-flush` and `tagged-gtid-resume-exact`.
- **Evidence:** catalog overview rows; evidence files of the swap/failover/upgrade properties cite the deferred templates (e.g. `source-swap-never-silently-accepted.md:43,59`; `source-upgrade-resume-continuity.md:57`).
- **Suggested action:** Mark each property with the template that runs it (a "Runs in" column), and either demote `source-swap-never-silently-accepted` to "P0, deferred" or schedule the RESET variant in `fetch_fs` behind a flag so the P0 has a home. Make `distinct-storages-shared-server-id-stay-consistent` explicit about running as two pulls in `pbs`.

### F3. Known wedge bugs will truncate exploration in the main templates (catalog-wide)

- **Properties:** `kill-anywhere-storage-reopens` (ticket 15), `stored-rotate-or-stop-is-last-event`, `interrupted-purge-leaves-openable-storage` (N3), `failed-flush-never-reappends-data` (ticket 18 permanent duplicate → CR_1236 wedge in position mode), `binlog-metadata-matches-stored-bytes` (lead (b): debug-build abort at `storage.cpp:138` "on every retry").
- **Concern:** About 13 properties are expected to fail today, 4 of them P0. The balance problem is not the red signal itself but that several of these failures are *wedges*: once a timeline hits ticket 15 or lead (b), every later fetch fails to open or aborts, so every downstream property in that timeline (exactly-once, convergence, purge, query, pull serving) stops exploring. Ticket 15 is ranked "high (top)" fit precisely because rotation is frequent (small `max_binlog_size`, `FLUSH BINARY LOGS`), so it is likely to cut most `fetch_fs` timelines short.
- **Evidence:** catalog summary list of expected-to-fail properties; sut-analysis ticket table rows 15, 18; lead (b) text.
- **Suggested action:** Plan a "known-wedge recovery" step in the workload (detect the validated wedge signature, assert/record it once, then reset the storage to a fresh subdirectory and continue), or run a variant build with the minimal ticket-15 fix. Otherwise the coverage of everything downstream of rotation is overstated.

### F4. No fetch-side convergence liveness: the end-to-end oracle is gated on success (catalog-wide)

- **Properties:** `stored-transactions-exactly-once-across-resumes`, `kill-anywhere-storage-reopens`, `repeat-fetch-on-caught-up-storage-is-noop`.
- **Concern:** The P0 exactly-once oracle is an `Always` evaluated "at quiescence after a final fault-free fetch to EOF". If the final fetch never reaches exit 0 (wedge, crash loop, CR_1236 after ticket 18), the `Always` is never evaluated. `kill-anywhere-storage-reopens` checks only that the next start *opens*. Pull has a liveness counterpart (`pull-chain-converges-after-heal`), fetch does not. Of the 8 liveness properties none is about fetch, which is the cron/backup workflow W1/W2/W4 that sut-analysis rates "silent, catastrophic". LG5 (restart recoverability) has no direct liveness property.
- **Evidence:** catalog lines 342-352; topology `eventually_converge.py` "retries fetch until exit 0 within a bounded number of attempts" with no stated assertion for the failure branch.
- **Suggested action:** Add `Always("after faults stop, fetch reaches exit 0 within N attempts")` (or `Sometimes` per mode × encryption × rewrite) in `eventually_converge.py`, so a wedge registers as a liveness failure rather than as silence.

### F5. Source-side binlog retention is assumed away; no property covers PBS falling behind the source (property gap)

- **Concern:** The catalog assumes "the source retains its binlogs for the whole run" (Assumptions). sut-analysis "Failure & degradation modes" says a purged source binlog (ER 1236) produces a crash loop and that "a property should assert 'exits non-zero and storage still opens'". No property does. This is a routine operational state (`binlog_expire_logs_seconds`, PBS down for maintenance), and it intersects the un-checkpointed-tail loss that `stored-transactions-exactly-once-across-resumes` explicitly excludes from scope. There is also a silent variant to rule out: in position mode with a rotated-away tail, does PBS ever skip to a later file and continue (gap) instead of 1236?
- **Evidence:** sut-analysis line 117; `binlog-metadata-matches-stored-bytes.md:71-72` mentions only the empty-storage race; exactly-once scope note in catalog line 349.
- **Suggested action:** Add a P2 property `source-purge-past-resume-point-fails-closed`: workload occasionally `PURGE BINARY LOGS TO` beyond PBS's tail while PBS is stopped; `Always(fetch/pull exit non-zero, no new bytes stored, storage still opens)` and `Sometimes(resume hit a purged source binlog)`. Keep it in a flagged action so it does not swamp the exactly-once runs.

### F6. Over-investment in deterministic, low-Antithesis-fit properties (catalog-wide, mild)

- **Properties:** `pull-client-version-matrix` (evidence file itself says "Low by itself (deterministic)"), `rewrite-checksum-none-fail-stop`, `query-modes-never-write-storage` (lead (e) fires deterministically on any encryption-added query config), `large-event-served-or-rejected-cleanly` (ticket 10, rated "low (deterministic)" in sut-analysis), `rs-unauthenticated-session-never-served`, `misaligned-dump-position-rejected-cleanly`, `rewrite-toggle-between-runs-keeps-storage-resumable`.
- **Concern:** About 7 properties (~12%) are input/config-deterministic; the catalog already moved three similar findings to "Candidates not cataloged (better tested elsewhere)" but did not apply the same rule to these. They cost workload actions (large events, crafted positions, version matrix, fixtures) that compete for Antithesis time with the high-fit surfaces. Rewrite in particular has 6 properties while sut-analysis bug history calls the rewriter "quiet because well tested"; its crash-only angle is legitimate (`rewrite-binlog-chain-valid-across-crashes`, `rewrite-storage-transaction-sequence-matches-source`), the toggle/checksum ones are not.
- **Suggested action:** Apply the "candidates not cataloged" rule consistently: keep each as a cheap `Sometimes`/`Always` in an existing driver where it costs nothing (e.g. large events in the load generator), but move the deterministic primary check to MTR and drop these to "regression guard" in the catalog. `pull-client-version-matrix` is better framed as a vacuity guard folded into `pull-served-stream-self-consistent` than as a standalone reachability property.

### F7. S3 backend is under-represented as its own subsystem (catalog-wide)

- **Concern:** `s3_storage_backend.cpp` is a churn hotspot (16 commits) with distinctive mechanics: whole-object re-upload per write, `resize_object` = download/truncate/re-upload during startup recovery, local temp copies, CRT default retries/timeouts, the `tmp_fstream_` eof/fail-bit singleton (focus 8), and the short-range `out_of_range` on a stale read (A10). Only `s3-crash-leftovers-bounded` is S3-specific. The rest treat S3 as a backend variant of generic properties. The topology's `s3` template picks fetch-like *or* pull-like once per timeline, halving the S3 exploration for each. Specific uncovered items:
  - kill during S3 `resize_object` in startup recovery (download → truncate → re-upload; a kill between leaves the object untruncated, which is fine, but a partial/failed re-upload after a CRT retry is not covered by any S3-specific check);
  - `tmp_fstream_` stuck-failbit after one failed PutObject making every later write fail: would show as a liveness failure of pull, but no property names it and `failed-flush-never-reappends-data` notes sticky failbits only for the filesystem;
  - A10 is excluded by choosing Versity, which is fine, but should be recorded as an explicit exclusion.
- **Evidence:** sut-analysis lines 47, 122, 197, 261, 277; catalog overview (only one S3-only row).
- **Suggested action:** Add `Sometimes("S3 write succeeded after an earlier PutObject failure on the same stream")` (and the matching `Always` that the next checkpoint advances) to `failed-flush-never-reappends-data` or `pull-chain-converges-after-heal`; add an S3 row to `recovery-paths-exercised` for "startup truncation via resize_object on S3". Consider letting the `s3` template run both fetch and pull in different phases.

### F8. Pull on the filesystem never gets disk faults (topology/catalog mismatch)

- **Properties:** `failed-flush-never-reappends-data`, `graceful-exit-leaves-flushed-storage`, `pull-served-stream-self-consistent`, `pull-chain-prefix-consistency` (list "custom shim" faults).
- **Concern:** `libfsfault.so` is applied only to test-command-launched `fetch` (topology line 174). `pull` is launched by the PID-1 supervisor without `LD_PRELOAD`. So pull × filesystem × ENOSPC/EIO (N8 exit 0 after failed final flush, ticket 18 via the destructor in pull) is reachable only via the `s3` template's proxy. N2 (`.json.tmp` deleted under a live writer → ticket 18 path) is filesystem-only and lives in the out-of-contract `contention` template.
- **Suggested action:** Let the supervisor optionally start pull under `LD_PRELOAD=libfsfault.so` with a rule chosen per restart from Antithesis randomness.

### F9. Cross-cutting combinations with no property: purge × encryption/rewrite, pull × rewrite file switch (property-specific)

- **Properties:** `interrupted-purge-leaves-openable-storage` (0 mentions of encryption), `sender-file-switch-emits-artificial-rotate` (1 mention of rewrite).
- **Concern:** Purge removes `<name>.json` (which holds the per-file encryption envelope) before the payload. An interrupted purge on an encrypted storage can leave a payload whose key is gone, and startup validates every indexed file's KEK (`storage_core.cpp:899-903`). Rewrite rotates locally at `file_size`, which is the most frequent source of magic-only next files, i.e. the N5 window; N5 is analysed mainly for source rotations. Neither combination is called out, so the config randomisation may or may not reach them.
- **Suggested action:** Add per-config `Sometimes` vacuity markers: `Sometimes("purge interrupted on an encrypted storage")`, `Sometimes("sender crossed a rewrite-generated file switch")`.

### F10. Ticket and lead traceability has silent omissions (catalog-wide, documentation)

- **Concern:** Tickets 1, 3, 4, 5, 8, 11, 12 and 16 appear nowhere in the catalog, and N13 (32-bit position cast on > 4 GiB binlogs) and the rewrite 999,999 sequence limit (N11 part 3) are not mentioned. Most are rightly low-fit (deterministic parsing, build env, harness env var), but ticket 1 is "High" in the ticket list and reachable through pull reading a stored FDE (sut-analysis row 1, "med via pull"), and the sequence limit is reachable by a long rewrite timeline with the topology's "small `file_size`" (sut-analysis line 154: ~17 min at 1 MB/s with `file_size=1024`). Without a recorded exclusion, synthesis cannot tell "considered and dropped" from "missed".
- **Suggested action:** Add a short "Tickets and leads not cataloged" table with one-line rationale each. For the sequence limit, either add `Always("rewrite reaching the sequence limit exits non-zero with storage openable")` or pin `file_size` high enough that the limit is unreachable and say so.

### F11. Liveness/reachability balance is thin outside pull and storage recovery (catalog-wide, mild)

- **Concern:** 8 liveness properties, of which 3 are "Always (bounded)" timing checks and 1 (`gtid-failover-resume-progresses`) is deferred. The real `Sometimes`-style progress properties are `blocking-dump-*` (2), `source-auth-survives-auth-cache-flush`, `pull-chain-converges-after-heal`. There is no freshness property for LG3 (interval checkpoint fires only on the next event; default config serves nothing of the active binlog), only an indirect bound inside `stalled-dump-client-isolated`. `recovery-paths-exercised` carries reachability almost alone; it is broad (~40 markers) but it is one property, so if the SUT-macro question (Open Q 7) is answered "no", the catalog loses essentially all reachability coverage at once.
- **Suggested action:** Add a workload-side fallback for the most important anchors (log-line `Sometimes` for tail truncation, magic-only reopen, purge cleanup failure) so reachability does not hinge entirely on Q7. Consider a small LG3 property: with `checkpoint_interval = t`, a transaction followed by one more event is visible to a pull client within `t + δ` when links are healthy.

## Passes

- Top-ranked attack surfaces 1-6 and 8-10 in sut-analysis each map to at least one P0/P1 property; surface 1 (ticket 15) and surface 2 (ticket 18) are covered from several angles with explicit root-cause owners.
- Every high-fit ticket (9, 14, 15, 18, 24, 25, 26) and every code-confirmed lead N1-N12 has a property; leads (a)-(g) from the investigation are all traced to slugs.
- Pull serving gets proportionally the most properties, which matches the "least settled code" bug-history note and the single-io-thread risk.
- Safety vs liveness for the same state is deliberately paired (`dump-session-ends-with-eof-or-err-packet` vs the `blocking-dump-*` pair; `gtid-failover-no-lost-transaction-merge` vs `gtid-failover-resume-progresses`).
- Multi-process hazards are isolated in a `contention` template so expected out-of-contract failures do not hide in-contract regressions.
- Ticket 19 is excluded with an explicit, correct rationale (no page-cache-loss fault).
- Query modes: `search_by_gtid_set` correctness is covered inside `query-success-never-omits-middle-binlog` (coverage ⊇ target, no false "cannot be covered"), not just contiguity.

## Uncertainties

- Whether the config-transition properties (F1) are meant to be driven by a later workload step that is simply not written into the topology yet; if so F1 is a topology fix rather than a catalog gap.
- How often the ticket-15 wedge will actually fire per timeline (F3) depends on `max_binlog_size` and kill frequency; if rare, the truncation effect is small.
- Whether Antithesis applies network/hang faults to the `workload` container (topology open question) affects how much pull-client-side liveness is actually explored.
- Ticket 1's reachability through a stored FDE needs storage corruption, which no planned fault produces; it may be correctly out of scope.
