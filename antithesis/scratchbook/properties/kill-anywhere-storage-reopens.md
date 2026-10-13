# kill-anywhere-storage-reopens — A writer killed or failed at any point leaves a storage the next run opens

Found by: focus 3 (failure recovery), focus 1 (data integrity).
Merged from: kill-anywhere-storage-reopens (focus 3), storage-reopens-after-writer-crash (focus 1), startup-recovery-is-restartable (focus 3), backend-error-exit-leaves-openable-storage (focus 3).

## Synthesis notes

**Type and priority.** Safety, P0. All contributors rated it P0 except startup-recovery-is-restartable, which rated it P1.

**Invariant.** The next streaming start does not fail with a storage-validation error. Opening in `querying_only` mode is not the bar.

**Workload checks.** The invariant is checked after three triggers. Each trigger has its own workload `Always` message, so triage can tell them apart:
- after a kill;
- after a backend-error exit that was not a kill;
- after a kill timed into startup recovery itself. In this case the storage must also converge.

**SUT-side assertions.**
- One `UNREACHABLE` per validator raise site, gated on `construction_mode_ != querying_only`.
- The assert at `storage_core.cpp:707`, promoted to `ALWAYS(open_stream_offset == flushed position)`.
- `ALWAYS(metadata size >= 4)` before the truncation at `:979`.
- Anchors for `.tmp` cleanup, tail truncation, and "rotation failed after creating the data object". The rotation anchor records the failing step in its details.

**Evidence of a violation.** Ticket 15 is the validated violation. The MTR test `storage_interrupted_binlog_creation.test` encodes the team's expectation.

**Faults.** The workload sends SIGKILL itself, so node termination is not needed. The backend-error variant needs only default network faults on PBS<->S3, or fsfault.

## What led to it

- **Focus 3 lens: failure recovery.** The storage is a set of objects updated in several non-atomic steps. Startup recovery handles only two kinds of damage:
  - a tail binlog that is *longer* than its metadata (`storage_core.cpp:960-984`, `:960-985`);
  - leftover `*.tmp` objects (`:597-630`).

  A validator rejects every other partial state.
- **Focus 1 lens: data integrity.** Metadata and data must agree. Index, objects and `.json` must stay mutually consistent across every multi-object update. `put_object` is claimed to be atomic (`basic_storage_backend.hpp:49-51`).
- **Focus 3 lens: "recovery procedures that assume clean state".** Startup recovery itself changes the storage, and a kill or backend fault can interrupt any of these steps:
  - `.tmp` deletion (`storage_core.cpp:597-630`);
  - tail truncation (`:979`);
  - the encryption-format metadata upgrade (`:175-178`);
  - the first `metadata.json` on an empty storage (`:152-157`).

  Each step looks idempotent on its own, but nobody has exercised it under a fault. On S3, truncation is a three-step download, local truncate and re-upload (`s3_storage_backend.cpp:765-808`). The TODO there and ticket 17 note that this transfers the whole object, so the window is large for big tails.
- **Claimed guarantees.**
  - SG3, README:373: "Any error … results in immediate termination … making sure that storage is left in consistent state".
  - SG4, code comment at `storage_core.cpp:960-967`: "in case when Binlog Server process was not properly shut down … we can perform automatic recovery".
  - README:715 and README:721: `kill -9` may "lose some progress", not corrupt the storage.
  - LG5.
- **Ticket 15** (validated in the sut-analysis "Bug history"). The order magic → `.json` → index at `storage_core.cpp:680-703` is not atomic, and unindexed objects are rejected at `:779-783`. The ticket also says a failed `binlog.index` upload on S3 reaches the wedge "without any crash", and that "Every later start fails with the same error".
- **QA's expected behaviour, not yet the maintainers'.** The MTR test `mtr/binlog_streaming/t/storage_interrupted_binlog_creation.test` (commit 73cbe43) hand-crafts the "magic only, not in index" leftover. It says: "The next start must recover from that … instead of refusing to open the storage".
  - The commit is QA-authored and exists only on the `pbt-init` / `antithesis-research` branches, not on upstream `main`. It records QA's expectation (the same as ticket 15's "Expected behaviour"), not a maintainer decision.
  - The maintainer-side evidence for "crash ⇒ next start recovers" is indirect: the recovery code and comments for improper shutdown (`storage_core.cpp:597-630`, `:960-967`) and README:721.

## Code paths

- **New binlog creation.** `storage_core::open_new_binlog_file_internal`, `storage_core.cpp:680-703`, runs these steps in order:
  1. `open_stream(create)`. On the filesystem, this creates and truncates the file immediately (`filesystem_storage_backend.cpp` `do_open_stream`).
  2. `write_data_to_stream(magic)`. On S3, this is the first PutObject of the object (`s3_storage_backend.cpp:850-863`).
  3. `binlog_records_.emplace_back`.
  4. `save_binlog_metadata` (`put_object(<name>.json)`).
  5. `save_binlog_index` (`put_object(binlog.index)`).

  A failure at any step throws out of `open_binlog`, then out of `process_artificial_rotate_event` (`collector_context.cpp:555-571`), and the process ends.
- **Other write orderings.**
  - Checkpoint: `:299-322`.
  - Purge: `:369-433`. The comment at `:391-397` acknowledges orphan leftovers.
  - `metadata.json` update when encryption is newly configured: `:172-178` / `:175-178`. This is a `save_metadata()`, an atomic `put_object`. After a kill, the next run sees the old metadata and repeats the update.
  - First `metadata.json` on an empty storage: `:152-157`.
- **Constructor validation** (`storage_core.cpp:150-221`):
  - `:160-164` "storage is not empty but does not contain metadata"
  - `:187-198` (raise at `:196`) "storage is not empty but does not contain binlog index". This fires when the first-ever binlog was interrupted before the first index write.
  - `:772-783` (`:775`) "binlog index contains a reference to a non-existing object"
  - `:779-783` (`:780`) "storage contains an object that is not referenced in the binlog index"
  - `:937-940` (`:939`) "missing metadata for a binlog listed in the binlog index"
  - `:968-972` (`:970`) non-tail size mismatch
  - `:974-977` (`:975`) "cannot perform recovery - size from the binlog metadata is bigger than the actual binlog file size"
  - `:992-996` / `:993-996` (`:994`) "found metadata for a non-existing binlog"
- **Ordering inside the constructor.** `.tmp` cleanup runs **before** validation. Truncation runs **during** validation, one binlog at a time (`:927-988`). If a later validation step fails after an earlier truncation, the truncation stays applied. That is fine, because truncation only removes bytes beyond the committed metadata.
- **Recovery that does exist.**
  - `remove_temporary_objects` (`:597-630`; the `.tmp` found branch is at `:604-611`, removal at `:626`). It calls `basic_storage_backend::remove_objects`, which is best effort, then fsyncs, then rethrows the first error. A partial removal is retried on the next start. A rethrown first error fails the constructor, which is fine because the next start retries.
  - Tail truncation via `backend_->resize_object` (`:979-983`; anchor at `:980`). On the filesystem, `do_resize_object` uses `resize_file` + fsync (`filesystem_storage_backend.cpp`). On S3, `do_resize_object` (`s3_storage_backend.cpp:765-808`) GETs the object into a UUID temp file, runs `resize_file`, then PUTs it back. If the PUT fails, the local temp file is left behind, because the `remove` at `:801-806` is skipped on exception.
- **Append reopen.** `storage_core.cpp:707` asserts that the open-stream offset equals the flushed position. Release builds compile the assert out, so a mismatch silently appends at the wrong offset.
- **Filesystem backend.**
  - `put_object` writes a tmp file, fsyncs, renames, then fsyncs the directory (`filesystem_storage_backend.cpp:127-160`).
  - The data stream is never fsynced (`:229-230`).
- **Process exit on error.** In pull, every error other than 2013 ends the process (`pull_operation.cpp:154-166`). That makes the backend-error path common under faults.
- **Other error exits.** A checkpoint failure, error 1236 or a parse error leaves one of two outcomes:
  - a tail longer than its metadata, which is recovered;
  - ticket 18's duplicate (see `failed-flush-never-reappends-data`).

## Failure scenarios

### Trigger A — kill (workload SIGKILL)

| Kill point | Objects left | Next streaming start |
|---|---|---|
| after first-ever `open_stream(create)` or magic, before the first index | `metadata.json`, `binlog.000001` | fails at `:196` (no index) |
| after the magic of N+1, before `.json` (ticket 15) | unindexed `binlog.N+1` | fails at `:780` |
| after `.json`, before the index (ticket 15) | unindexed `binlog.N+1` + `.json` | fails at `:780` |
| between the data append and the metadata PUT (checkpoint) | tail longer than metadata | recovered by truncation (expected OK) |
| inside `put_object` before rename (filesystem) | `<name>.tmp` | removed (expected OK) |
| during `resize_object` on S3 (download + re-upload) | object either old or new (single PUT) | expected to recover |
| between purge's index commit and its deletes (N3) | orphan objects | rejected |

The first three rows and the N3 row are wedges. They affect every mode: purge and the queries too, except that `querying_only` tolerates a missing index.

### Trigger B — backend-error exit (no kill)

The source rotates while PBS<->MinIO is partitioned. `put_object(binlog.index)` exhausts the SDK retries, and fetch or pull exits 1. The partition heals. From then on, every start (fetch, pull, purge) fails with "storage contains an object that is not referenced in the binlog index".

Depending on which rotation step failed:
- A failed magic upload with no server-side effect leaves nothing (OK).
- An ambiguous magic upload leaves an unindexed object (wedge).
- A failed `.json` PUT leaves an unindexed data object (wedge).
- A failed index PUT leaves an unindexed object plus its `.json` (wedge).

The same damage can come from an S3 5xx or from ENOSPC (via `fsfault`) at the kill points in trigger A. Antithesis injects network partitions by default.

### Trigger C — kill during startup recovery

- **Kill during the S3 truncation PUT.** The object is either fully old or fully new, so this is expected to be safe. A stale read after the PUT could make the next run see the untruncated object and redo the work, which is harmless. This path cannot produce a *shorter* object.
- **Kill or partition during the S3 GET.** The exception ends the constructor and the next run retries. Expected to be safe.
- **Crash loop.** Even if PBS is killed shortly after every start, the storage must converge once the kills stop.
- **Interrupted `.tmp` removal or encryption metadata upgrade.** The next start repeats it.

This trigger mostly turns an "expected safe" design into a checked one. It is cheap to add to the kill workload.

### Out of reach unless page cache is dropped

Page-cache loss (ticket 19) leaves metadata larger than the data, and the storage is rejected. This only happens if the platform drops un-fsynced data. A container kill does not.

## Key observations

- **No node termination is needed.** `fetch` is a one-shot CLI and `pull` a long-running process. The workload can spawn either as a subprocess and SIGKILL it at a random moment, which is the pattern `extra/qa/fetch_crash_test.py` uses. Container-level node termination also works, but it is disabled by default.
- **Make rotations frequent.** The rotation window is small, but it opens at every rotation. Drive `FLUSH BINARY LOGS` often and set a small `max_binlog_size` on the source. Rewrite mode with a tiny `file_size` multiplies local rotations.
- **The kill and backend-error triggers reach the same wedge.** The SUT cannot tell how the damage arose, so distinct workload-side messages per trigger keep them apart in triage.

## Oracle / workload checks

- **Classifying a failed start.** A non-zero exit whose log contains one of the validator messages above counts as a violation. A source-side error, such as 1236, does not.
- **Re-implemented validator.** The workload can also re-implement the validator checks directly on the storage directory or bucket:
  - index entries, data objects and `.json` objects match 1:1;
  - non-tail sizes are equal;
  - tail metadata ≤ actual size.

  This lets the assertion fire without starting PBS. `list` cannot serve as this check, because it is querying-only.
- **Distinct workload `Always` messages, one per trigger:**
  - `Always("next streaming start after a kill does not fail with a storage-validation error")`
  - `Always("next streaming start after a backend-error exit does not fail with a storage-validation error")`. After every non-zero writer exit, the workload stops faults and restarts the writer.
  - `Always("next streaming start after a kill during startup recovery opens the storage and converges")`. To set this up, the workload sometimes kills the writer within the first N ms after start. The timing comes from the log lines "found temporary storage object" and "recovered binlog file … by truncating", or from a random delay. After the kills stop, the final convergence check (`stored-transactions-exactly-once-across-resumes`) must pass.
- **Coverage hint:** `Sometimes("a kill landed while startup recovery was in progress")`, detected from log ordering.

## SUT-side instrumentation suggestions

The codebase has no Antithesis SDK, so every SDK assertion below is **missing**. The existing C `assert` at `:707` counts as **partial**.

| Status | Assertion | Site | Origin |
|---|---|---|---|
| missing | `UNREACHABLE("streaming/purging open: storage has objects but no metadata.json")` | `storage_core.cpp:160-164` | focus 1 |
| missing | `UNREACHABLE("streaming/purging open: storage has objects but no binlog index")` | `:196` | 3, 1 |
| missing | `UNREACHABLE("streaming/purging open: binlog index references a non-existing object")` | `:775` | 3 |
| missing | `UNREACHABLE("streaming/purging open found an object not referenced in binlog.index")` | `:780` | 3, 1 |
| missing | `UNREACHABLE("streaming/purging open: missing metadata for an indexed binlog")` | `:939` | 3 |
| missing | `UNREACHABLE("streaming/purging open: non-tail binlog size differs from metadata")` | `:970` | 3, 1 |
| missing | `UNREACHABLE("streaming/purging open: tail metadata size exceeds data size")` | `:975` | 3, 1. Synthesis: this property owns the site; `second-writer-on-storage-fails-closed` references it (ticket 24 is a main trigger). |
| missing | `UNREACHABLE("streaming/purging open: metadata for a binlog not in the index")` | `:994` | 3, 1 |
| partial (C `assert`) → missing SDK | `ALWAYS(open_stream_offset == get_flushed_position_unsafe(), "append-open offset equals metadata size")` | replaces the assert at `:707` | 3 |
| missing | `ALWAYS(loaded_binlog_metadata.size >= magic_binlog_offset, "tail truncation target is at least the binlog magic")` | before `:979` | 3 (startup-recovery). Guards against truncating to 0 from a corrupted `.json`. |
| missing | `SOMETIMES(bytes_removed > 0, "tail binlog truncated to metadata size on startup")`, details {backend, bytes_removed} | `:979-983` / `:980` | 1 and 3 proposed the same anchor (one as SOMETIMES, one as REACHABLE). Kept once. It is shared with `recovery-paths-exercised` (owner of the generic anchor) and referenced by `stored-transactions-exactly-once-across-resumes`. |
| missing | `SOMETIMES(found_tmp, "startup removed leftover .tmp objects")` | `:604-611` / `:626` | 1 and 3, kept once. Shared with `recovery-paths-exercised`. |
| missing | `REACHABLE("binlog rotation failed after creating the data object")`, details {failed_step: magic / .json / index} | catch-and-rethrow wrapper around `open_new_binlog_file_internal` (`:680-703`) | 3 (backend-error) |

The `UNREACHABLE`s name a wedge on the SUT side even when the workload never restarts.

## Fault requirements

- Workload-driven SIGKILL of fetch and pull at random times, including within the first ms after start. No node termination is needed.
- The source needs frequent rotations: `FLUSH BINARY LOGS`, a small `max_binlog_size`, and rewrite mode with a tiny `file_size`.
- Default network faults on PBS<->S3, or `fsfault` ENOSPC/EIO on the filesystem backend, for trigger B.
- Optional: a purge interrupted between its index commit and its deletes (N3).
- Page-cache drop, only if the platform supports it (ticket 19).

## Open questions

- **Roll back or roll forward?** Is the intended fix to delete unindexed leftovers, or to index them? `(needs human input)`
  - If the fix rolls forward, a magic-only N+1 becomes the tail. The property then also needs the convergence check (`stored-transactions-exactly-once-across-resumes`) to be sure nothing is skipped.
  - If the fix rolls back, the next run re-creates N+1, and the oracle only needs to check that the storage opens.
- **Accepted fail-stop or a bug? What does "consistent state" (README:373) mean?** Is a storage that cannot be opened after an interrupted rotation, or after a backend-error exit, accepted fail-stop behaviour? `(partial: maintainer code treats improper shutdown as recoverable (storage_core.cpp:597-630, :960-967), and the maintainer purge comment accepts manual attention after a purge cleanup failure (purge_binlogs_operation.cpp:62-69); nothing states intent for the rotation window)`
  - If accepted, this becomes a detectability property ("PBS refuses to start with a clear error rather than misbehaving"), and the `UNREACHABLE`s for the rotation and purge windows (ticket 15 windows, N3) become `REACHABLE`s.
  - If "consistent" means only "no corrupted bytes", the backend-error part becomes a recoverability expectation (Liveness-like) rather than a safety claim.
- **Does any fault drop un-fsynced page cache?** `(needs human input)` This is the catalog-wide question in sut-analysis. If no fault does, the page-cache-loss scenario (ticket 19) is unreachable and stays excluded.
- **Read-after-write consistency on S3.** Does the S3 server give read-after-write consistency for an overwrite followed by a ranged GET? `(partial: the topology picks Versity S3 Gateway posix, a single-node gateway over a local filesystem, so overwrite-then-read is expected to be consistent; not verified in Versity's code or by a test)` If it is not consistent, a restart right after a truncation PUT could append to a stale, longer object, and the property would fail for an environment reason.

## Investigation results (2026-10-13)

- **Ticket 15 is confirmed at 99de682, and no fix exists.**
  - `remove_temporary_objects` removes only names with the `.tmp` suffix (`storage_core.cpp:597-614`).
  - `validate_binlog_index` still raises on any unindexed object (`:779-783`).
  - No local or upstream branch changes this. `git log --all -- src/binsrv/storage_core.cpp` shows no such commit.
  - Ticket 15 reports `storage_interrupted_binlog_creation` failing on MySQL 8.0.45 and 8.4.8 today.
  - The test is QA-authored and not upstream, so it is a regression target for a future fix, not a maintainer-endorsed spec (see "What led to it").
  - **Implication:** every `Always("next streaming start after a kill …")` is expected to fail today once a kill lands in the rotation window.
- **Node termination is not needed.** deployment-topology.md runs `binlog_server` as a child of the `pbs` supervisor. `fetch`, `purge_binlogs` and the queries are subprocesses of `pbs`-side test commands, and `parallel_driver_kill_binlog_server.sh` SIGKILLs them in the same container. The S3 network faults stay on the separate `pbs ↔ s3` link.
- **An ambiguous S3 upload can leave an object, so a failed magic upload alone can wedge the storage.**
  - S3 `write_data_to_stream` is a whole-object PutObject (`s3_storage_backend.cpp:850-863, 933-938`). A PUT that the server applied but whose response was lost becomes an exception after the retries are exhausted. `open_binlog` then throws with `<N+1>` already in the bucket and absent from the index (`storage_core.cpp:686-701`).
  - QA's proxy action `reset_after` ("forward, then close the connection without answering", `s3_fault_proxy.py:41, 244`) produces exactly this. Ticket 15 records the index case: "When only the response is lost, the index is written".
  - The magic-object case follows by the same mechanism. Nothing QA recorded isolates it.
  - **Implication:** Trigger B row "an ambiguous magic upload leaves an unindexed object (wedge)" stands.
- **Filesystem detail for the first kill row.** `do_open_stream(create)` creates a 0-byte file before the magic is written (`filesystem_storage_backend.cpp:196-214`). A kill right after it leaves a 0-byte unindexed object, which is the same wedge.
- **Property unchanged.** The invariant, assertion types and P0 stay. The tags above record what is still undecided.

### Investigation Log

#### Roll back or roll forward?

- Examined: ticket 15 "Suggested fix", `storage_core.cpp:597-630, 680-703, 764-787`, MTR test 73cbe43.
- Found: ticket 15 offers both options. The MTR test accepts either, because it checks only that the next fetch succeeds and that the first binlog matches.
- Not found: any maintainer statement or upstream change.
- Conclusion: `(needs human input)`.

#### Is `storage_interrupted_binlog_creation.test` expected to fail today?

- Examined: `git branch -a --contains 73cbe43`, `git log --all -- src/binsrv/storage_core.cpp`, `storage_core.cpp:597-630, 772-783`, ticket 15 "Tests".
- Found:
  - The test exists only on QA branches.
  - The code has no unindexed-object cleanup.
  - Ticket 15 records the test failing with exit 1 on 8.0.45 and 8.4.8, and says it "has not been run against a fixed build yet".
- Conclusion: resolved. The test fails today, and no fix is in flight in the repo. The property is a regression target for a future fix.

#### Accepted fail-stop or a bug? What does "consistent state" in README:373 mean?

- Examined: README:373, README:713-721, `storage_core.cpp:388-404, 597-630, 960-984`, `purge_binlogs_operation.cpp:62-69` (git blame: maintainer, bf0c238 / 7f8cb69), sut-analysis catalog-wide questions.
- Found:
  - The maintainers built automatic recovery for "not properly shut down" (`:960-967`) and for `.tmp` leftovers "after improper shutdown" (`:607-610`).
  - For purge, they explicitly accept that a cleanup failure leaves a storage "that the constructor's existing validators will refuse to open" (`:391-396`). They report it as `warning` so "the operator knows the storage will need attention" (`purge_binlogs_operation.cpp:67-69`).
- Not found: any maintainer statement about the rotation window, or about what "consistent" includes.
- Conclusion: `(partial: …)`. The precedent cuts both ways (crash recovery intended, purge fail-stop accepted), so a maintainer must decide for rotation.

#### Are node-termination faults enabled?

- Examined: deployment-topology.md (pbs component, supervisor, test-command placement).
- Found: the harness kills `binlog_server` with workload-driven SIGKILL inside the `pbs` container. Node termination is optional.
- Conclusion: resolved. The property does not depend on node termination.

#### Does any fault drop un-fsynced page cache?

- Examined: sut-analysis "Open questions" (`[needs-human]`, line ~307), deployment-topology.md.
- Found: nothing in the repo or the scratchbook settles Antithesis' page-cache semantics. The Antithesis docs CLI (`snouty`) is not installed here.
- Conclusion: `(needs human input)`. This is catalog-wide.

#### Does an aborted S3 CRT upload ever leave an object (ambiguous success)?

- Examined: `s3_storage_backend.cpp:369-386, 850-863, 933-938`, `s3_fault_proxy.py:35-61, 244`, ticket 15 and ticket 18 "Tests".
- Found:
  - A PUT is applied server-side before its response is sent.
  - The proxy's `reset_after` drops only the response, and QA observed the index written in that case.
  - A whole-object PUT of the magic behaves the same way.
- Not found: a recorded run that isolates the magic-object case.
- Conclusion: resolved by mechanism. A failed magic upload can wedge the storage.

#### Read-after-write consistency on S3

- Examined: deployment-topology.md (s3 component: Versity 1.8.0 posix, assumption A10), sut-analysis line ~131, `s3_storage_backend.cpp:815-819` (`do_fsync` no-op relies on it).
- Found: the harness uses a single-node posix gateway, and QA ran its S3 sweeps on the same gateway.
- Not found: a Versity code reference or a test that checks overwrite-then-ranged-GET consistency.
- Conclusion: `(partial: …)`.

## Evaluation refinements (2026-10-13)

Applied by the evaluation synthesis (`../evaluation/synthesis.md`). Where this section and earlier text in this file disagree, this section wins; the catalog entry has been updated to match.

- Validator-site messages are per leftover shape, not per site: "unindexed data object of size 4 (interrupted rotation)", "metadata or data object without an index entry (interrupted purge)", "tail data larger or smaller than its metadata size (writer overlap or truncation)". Antithesis tallies an assertion by its message across every timeline of a run, so an SUT message cannot be scoped to a template (wildcard W1). Ticket 15 and N3 then keep only their own messages red, and a new route into a validator stays visible. Sites reachable from `contention` (tickets 9 and 24, N1) carry the harness timeline tag in the message.
- Ticket 15 is a wedge; how expected-to-fail wedges are contained (avoid-trigger knobs, a detect-and-reset helper, a variant build with the fix) is bias B3.
- Runs in `fetch_fs`, `pull_fs`, `s3`.
