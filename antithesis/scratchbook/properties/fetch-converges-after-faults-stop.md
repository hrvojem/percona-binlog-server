# fetch-converges-after-faults-stop — After faults stop, repeated fetch runs reach exit 0 and a complete copy within N attempts

## What led here

Evaluation gap G4 (coverage-balance F4). The P0 exactly-once oracle (`stored-transactions-exactly-once-across-resumes`) and the complete-copy check of `graceful-exit-leaves-flushed-storage` are evaluated only **after** a final fetch reaches exit 0. If no fetch ever does (a wedge, a crash loop, a hang), those `Always`s never run, and the timeline is silent instead of red. `kill-anywhere-storage-reopens` checks only that the next start does not fail with a **validator** error, and it explicitly excludes source-side errors. Pull has a liveness counterpart (`pull-chain-converges-after-heal`). Fetch, the cron backup workflow (sut-analysis W1/W2/W4, rated "silent, catastrophic"), has none. The topology's `eventually_converge.py` "retries fetch until exit 0 within a bounded number of attempts" but names no assertion for the failure branch (deployment-topology §fetch_fs).

## Known routes to a wedge (from the catalog; not re-validated here)

Every route below leaves a storage on which every later fetch exits non-zero after faults stop. Each is owned by another property. This property is the end-to-end liveness net that turns any of them, or an unknown one, into a red result.

- **Ticket 15** (validated; the MTR test `storage_interrupted_binlog_creation.test` fails today): an interrupted rotation leaves an unindexed object. Every mode refuses the storage. Owner: `kill-anywhere-storage-reopens`.
- **Ticket 18, position mode**: a metadata PUT fails after the data append, and the destructor re-appends. Every later fetch fails with `CR_1236` ("position > file size"). This is **not** a validator error, so `kill-anywhere-storage-reopens` does not count it. Owner: `failed-flush-never-reappends-data`.
- **Lead (b)**: a source SIGKILL right after a rotation in position mode. A debug build aborts at `storage.cpp:138` on every retry. Owners: `source-crash-restart-resume-is-faithful`, `binlog-metadata-matches-stored-bytes`.
- **N3 / interrupted purge**: a purge wedge. In `fetch_fs`, purge runs as a `serial_driver_` under the writer lock. Owner: `interrupted-purge-leaves-openable-storage`.
- **Hangs**: a fetch that never exits after faults stop, for example blocked on S3 with no overall request timeout (CRT defaults, catalog Open Q 14), or on a source that accepts the connection but never answers. Owner for bounded shutdown: `sigterm-honoured-within-bound`. Nothing owns "fetch completes".

Exclusions: rewrite/replication-mode/`base_file_name` toggles are drawn once per timeline (topology §Test templates), so `rewrite-toggle-between-runs-keeps-storage-resumable` cannot cause a wedge here. Source swap/RESET runs (ticket 25) are excluded, as for the exactly-once oracle.

## Code paths

- `src/operations/fetch_operation.cpp:108-119`: `operation_result` true means "successfully fetched everything and disconnected" and gives exit 0. Otherwise "fetching binlog events loop did not reach EOF" gives non-zero. An exception from storage construction (validator) or from the collector also gives non-zero.
- `src/operations/collector_context.cpp:171-187`: fetch is non-blocking. Case (3), EOF, returns true. A read timeout or connection error is case (2) and returns false, so a fetch that cannot reach the source exits non-zero, which is correct while faults are on.
- `src/binsrv/storage_core.cpp:930-985`: the startup validator and the tail truncation. They run on every attempt, so a deterministic storage defect fails every attempt identically.

## Failure scenario

A timeline SIGKILLs fetch during a rotation (ticket 15), or hits a metadata PUT failure (ticket 18). Faults stop and `eventually_converge.py` retries fetch. Each attempt exits 1 with the same message. Today the command gives up after N attempts and nothing asserts, so the exactly-once `Always` is never evaluated, and the timeline reports green on the P0 properties that matter most.

## Oracle

Run in `eventually_converge.py` (pbs, `fetch_fs`, and the fetch-like `s3` arm):

1. **Preconditions** (each failure is a harness problem, not a PBS failure; log it and `Unreachable("fetch convergence preconditions not met")` only if they never become true within a generous timeout):
   - every leftover `binlog_server` process is gone (SIGKILL stragglers; a surviving child of a killed driver would be a ticket 24 second writer);
   - every `s3_fault_proxy` rule is cleared, and no `LD_PRELOAD` is used;
   - `mysql-source` accepts a TLS login as the replication user (the restart wrapper may be mid-restart);
   - the source load is stopped, so the target is fixed. Snapshot `SHOW BINARY LOGS` and `gtid_executed` as the target.
2. Run `fetch` up to **N attempts** (N = 3), each with a timeout `T_a = connect_timeout + read_timeout + (source bytes not yet stored) / throughput + S3 whole-object upload time + margin`. A timeout counts as a failed attempt (SIGKILL the process).
3. **Main assertion:** `Always("fetch reaches exit 0 within N attempts after faults stop")`. The details carry the mode, encryption, rewrite flag, backend, each attempt's exit code and last error line, and whether a validator message appeared. That lets triage bucket ticket 15, ticket 18/CR_1236, lead (b) aborts and hangs without new assertions.
4. **Completion:** once exit 0 is reached, the existing complete-copy and exactly-once `Always`s run (owned by `graceful-exit-leaves-flushed-storage` (a) and `stored-transactions-exactly-once-across-resumes`). This property adds no second complete-copy assertion. It guarantees that they are evaluated, or that their non-evaluation is red.
5. **Vacuity guards:** `Sometimes("fetch converged after a timeline with a writer kill during rotation")`, `Sometimes("fetch converged after an exhausted S3 PUT failure")`, `Sometimes("fetch converged on an encrypted storage")`, `Sometimes("fetch converged on a rewrite storage")`. Each is a meaningful state that should become true at least once if the wedges are fixed. Until then, the first two show how often recovery succeeds despite the known wedges.

Why `Always`, not `Sometimes`: a `Sometimes(converged)` passes as soon as one timeline converges. A wedge in another timeline would then stay hidden, which is exactly the silence this property exists to remove. After faults stop, a deterministic recovery path must succeed every time, so a bounded `Always` is the right commitment.

## Expected status

**Expected to fail today** in any timeline that hits ticket 15 (frequent with a small `max_binlog_size`), ticket 18 in position mode, or lead (b) on a debug build. This is the same root cause the owning properties report. The assertion details make the bucket explicit, so these failures are not new findings in triage.

## SUT instrumentation suggestions (missing)

- None required. The check is workload-side by design, so it survives a "no SUT macros" answer to catalog Open Q 7.
- Optional: `REACHABLE("fetch reached EOF and is about to report success")` at `fetch_operation.cpp:112`, so a run where no fetch ever succeeds is visible even outside `eventually_`.

## Open questions

- **What N is acceptable?** After faults stop, a healthy storage should converge on the first attempt. N > 1 absorbs residual effects (a source still restarting, a stale TCP state). If maintainers expect "the next run after any crash succeeds", N = 1 is the contract and N = 3 is lenient. If they accept "a few runs", N must be stated. The choice decides whether a one-off recoverable failure (for example the S3 data-upload variant of ticket 18, which self-heals on the second start) is red. `(needs human input)`
- **Is fetch expected to make progress at all on a storage that a previous run left inconsistent (ticket 15, ticket 18 metadata variant)?** If the maintainers accept a manual-repair fail-stop (catalog Open Q 3), this property should exclude those buckets by detail instead of failing on them. If not, it stays as written. `(partial: README:373 claims any fetch error leaves the storage "in consistent state", which supports "the next fetch must progress"; no maintainer statement on the known wedges)`
- **How should a hang be told apart from slow progress?** `T_a` depends on S3 throughput under whole-object re-uploads (ticket 17: 32.5× upload volume at `checkpoint_size=1M`). Too small a `T_a` reports slow S3 as a wedge. `(partial: PBS sets no S3 request timeout, so CRT defaults apply; T_a must be calibrated from a measured fault-free upload rate, not derived from config)`

## Resolved during investigation (see Investigation Log)

- **Child processes of killed drivers:** the Antithesis test-composer reference says only that an `eventually_` command "kills all other running commands"; it does not say whether their child processes or process groups are killed. The property no longer depends on the answer: the precondition step SIGKILLs every leftover `binlog_server` process (and waits for the writer lock / process table to be clear) unconditionally before the first attempt. **Property change:** that kill step is mandatory, not conditional.

### Investigation Log

#### What N is acceptable?

- Examined: README `fetch` section (`:371-373`), ticket 18 S3 variant (`.tickets-snapshot.txt:978-992`, self-heals on the next start via tail truncation), ticket 15.
- Found: the README promises a consistent storage after any error but says nothing about how many runs a recovery may take. The code's recovery (tail truncation, `.tmp` cleanup) runs entirely at the next start, so a healthy storage converges on attempt 1 unless the source or S3 is still settling.
- Not found: a maintainer statement on N.
- Conclusion: `(needs human input)`. Keep N = 3 and put the attempt index of the first exit 0 in the details, so N = 1 can be applied in triage without rerunning.

#### Is fetch expected to make progress at all on a storage a previous run left inconsistent?

- Examined: README:373 ("Any error ... results in immediate termination of the program making sure that storage is left in consistent state"), README:715-719 (graceful termination "leaving storage in consistent state"), ticket 15 and 18 status (Open, High).
- Found: the README's claimed guarantee is that a failed fetch leaves a storage the next run can use. Tickets 15 and 18 are open defects against that claim, not documented exceptions.
- Not found: whether maintainers would accept a manual-repair fail-stop for those cases (catalog Open Q 3).
- Conclusion: `(partial: README claims consistency after any error; acceptance of the known wedges unconfirmed)`. The property stays as written, with buckets in details.

#### How should a hang be told apart from slow progress?

- Examined: `s3_storage_backend.cpp:205-258` (client construction sets only `region`, `scheme`, `useVirtualAddressing`, `endpointOverride`; no request/connect timeout), `grep -i timeout s3_storage_backend.cpp` (none), README:670-671 and ticket 17 (upload volume).
- Found: S3 timeouts are the SDK/CRT defaults, so there is no config-derived upper bound on one PUT. The only PBS-side bounds are `connect_timeout` and `read_timeout` on the MySQL side.
- Not found: the CRT default request timeout and low-throughput thresholds for SDK 1.11.774 (catalog Open Q 14).
- Conclusion: `(partial: no PBS S3 timeout; calibrate T_a per timeline from the observed fault-free upload rate of the previous successful checkpoint, plus margin)`.

#### Do drivers' child processes outlive the driver when the `eventually_` command starts?

- Examined: Antithesis docs `product/writing_tests/test_templates/test_composer_reference.md` (fetched), `antithesis-workload/references/test-commands.md`.
- Found: "When an `eventually` command starts, Antithesis kills all other running commands and stops all fault injection." Nothing about child processes, process groups or daemons.
- Conclusion: resolved for the property's purposes: the precondition kill of stragglers is made unconditional, so the answer does not change the oracle. (Whether children survive is still worth confirming for other `eventually_` commands; that is a harness question, not this property's.)
