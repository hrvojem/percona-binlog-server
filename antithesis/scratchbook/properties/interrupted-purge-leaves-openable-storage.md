# interrupted-purge-leaves-openable-storage — Every purge outcome leaves an openable storage, intact survivors and resume point, an honest status, and a retry that converges

Found by: focus 3, 9, 4, 1 (failure recovery, idempotency/replay, protocol, data integrity)
Merged from: interrupted-purge-leaves-openable-storage (focus 3), purge-retry-idempotent (focus 9), purge-error-status-means-index-unchanged (focus 4), purge-preserves-surviving-data-and-resume-point (focus 1)

## Synthesis notes

Safety, **P0**. Focus 3 rated it P0; focus 9, 4 and 1 rated it P1. We chose P0 for three reasons:
- A code comment admits the N3 wedge: leftovers "will trip the constructor's validators".
- Default network faults to S3 during the DELETE batch are enough to reach it.
- The wedge takes the backup offline, and purge itself cannot repair it.

Distinct workload `Always` messages:
- "storage opens in every mode after any purge outcome"
- "purge leaves the tail resume point unchanged"
- "purge never alters surviving binlog data or metadata"
- "new index is a suffix of the old index that keeps the tail"
- "purge error status implies index unchanged"
- "purge success status implies no victim objects remain"
- "purge warning status implies index committed"
- "purge retried after interruption converges to an openable storage". Expected to fail today, because the purging-mode constructor rejects orphans (`storage_core.cpp:779-783`, `:993-996`).
- "repeating a completed purge changes no object"

SUT-side assertions:
- `REACHABLE("purge committed the index but object cleanup failed")` at `:421`
- `REACHABLE("purge: index put_object threw")`
- `ALWAYS(purge never selects the tail)`
- `SOMETIMES(cleanup_warning_message non-empty)`

Triggers:
- Kill between the index commit (`:398`) and the deletes.
- PBS↔S3 partition during the DELETE batch.
- Ambiguous index PUT: the PUT is applied but the response is lost, so purge reports error while the index is committed.
- On the filesystem backend, the window between the rename and the directory fsync.
- Purge started right after a writer crash, so purge runs its own tail recovery.

## What led to it

- **Lead N3 (sut-analysis).** The code comments at `storage_core.cpp:387-397` and `:401-405` say leftovers "will trip the constructor's validators on next startup". The code itself states that a partial purge wedges the storage.
- **SG5.** README:374-376, 420-424 and 441, plus `storage_core.cpp:331-366`, present purge as a routine, safe operation. They say it removes a strict prefix and never the tail: `select_purge_victim_count` refuses the tail. The JSON `warning` status (`purge_binlogs_operation.cpp:66-86`) presents a cleanup failure as non-fatal. So the user-facing contract (warning = purge succeeded, some files remain) contradicts the code comment (warning = storage now unopenable).
- **Focus 3 lens (failure recovery).** Purge is a commit-then-cleanup protocol with no recovery for the cleanup phase. The next open cannot roll forward, because only the old index knew the purged names and it is gone. The validators reject orphans.
- **Focus 9 lens (replay).** Operators retry failed or interrupted commands. The purging-mode constructor runs the same validation as streaming mode, so a retried purge fails before it can clean anything up. The retry does not converge to the state of a single successful purge.
- **Focus 4 lens (protocol contract).** Retention scripts consume the JSON status of `purge_binlogs` as an API. README and `purge_binlogs_operation.cpp:48-87` define three outcomes:
  - `success`: the prefix up to the target is removed.
  - `warning`: the index was committed, but some payload or metadata objects could not be removed (`cleanup_warning_message`, `:62-75`).
  - `error`: the purge failed.

  An operator or script that reads `error` concludes nothing changed. That conclusion holds only if no call that later throws has already succeeded at the commit point (`save_binlog_index()`, `storage_core.cpp:398`). The code shows such a call can.
- **Focus 1 lens (data integrity).** Whatever happens during a purge (success, warning, kill, DELETE failure), the binlogs that survive must be untouched and the resume point unchanged.

## Code paths

- `operations/purge_binlogs_operation.cpp:48-87`: constructs a purging-mode storage, calls `purge_binlogs`, then prints a JSON status. The storage is constructed at `:57`. `cleanup_warning_message` is handled at `:62-75`, and the warning status at `:66-86`.
- The purging-mode constructor also runs startup cleanup:
  - `remove_temporary_objects`, the constructor's `.tmp` deletion (`storage_core.cpp:597-630`);
  - tail recovery truncation (`storage_core.cpp:960-985`, truncation at `:974-983`, N1).
- `select_purge_victim_count` (`storage_core.cpp:331-366` / `:332-366`) checks three things:
  - the base name matches;
  - the target is present ("target binlog name is not present in the storage", `:346-349`);
  - the target is not the tail.
- `storage_core::purge_binlogs` (`storage_core.cpp:368-433` / `:369-433`) runs in three steps:
  1. `:377-386` (`:369-385` per focus 9): extract the victim prefix in memory.
  2. `:398`: `save_binlog_index()` calls `backend_->put_object("binlog.index", …)` (`:789-799`). This is the commit point.
  3. `:409-430` (`:387-430` per focus 9): `backend_->remove_objects([<v>.json, <v>, …])`, `.json` then data for each victim, best-effort. Errors are caught at `:421` and swallowed into `cleanup_warning_message` (`:425-430`).
- Purge does not rewrite any surviving `.json`.
- `basic_storage_backend::remove_objects` (`basic_storage_backend.cpp:58-77` / `:58-78`) tries every name, remembers the first error, runs one `do_fsync`, then rethrows.
- Filesystem `do_remove_object` (`filesystem_storage_backend.cpp`, `do_remove_object`) throws "file does not exist" on ENOENT. On S3, DeleteObject is idempotent.
- Filesystem `do_put_object` (`filesystem_storage_backend.cpp:127-160`): write `.tmp`, `fsync(tmp)`, `rename`, then `fsync(parent dir)`. Suppose the directory fsync throws (EIO, or an injected fault via `extra/qa/fsfault.c`). The rename has already happened, so the new index is live, yet the exception propagates out of `purge_binlogs` before step 3. Purge prints `status: error` and exits 1, with the index committed and every victim object still present.
- S3 `do_put_object` (`s3_storage_backend.cpp:759-763`) is one PutObject through the CRT client. Retries and timeouts are SDK defaults (sut-analysis: "Timeouts and retries are left at SDK/CRT defaults"). One attempt can be applied by the server while its response is lost (connection reset or partition after the server commits). If the remaining attempts then fail, the call throws: purge reports `status: error` with the index committed.
- The next open (`storage_core.cpp:772-783`, `:992-996`) rejects any orphan `<v>` or `<v>.json` in every mode except `querying_only`, **including `purging`**:
  - an object not in the index raises at `:779-783`;
  - orphan metadata raises at `:993-996`.

  So the storage is wedged (N3), and a retried purge also fails in its constructor.

## Failure scenarios

1. **Kill after the index commit.** Trigger: workload SIGKILL of the `purge_binlogs` process after `save_binlog_index` and before all deletes finish. Any number of orphans is left, the index is a suffix, and every later start fails (N3). The surviving data and metadata are unchanged, so the data-preservation clauses should still hold.
2. **DELETEs fail after the commit.** Trigger: PBS↔S3 partition during step 3, or DELETE 5xx on some victims. The DELETEs fail after SDK retries, purge prints `status: warning` and exits, and every later start fails. Antithesis can produce this with default faults, without a kill.
3. **Ambiguous index PUT (step 2), S3.** Trigger: network fault during the index PUT. The PUT is applied server-side but the response is lost, so purge reports `error`. Nothing was deleted, but the index no longer lists the victims, so the outcome matches scenario 1. Concrete form: a nightly script runs `purge_binlogs` on an S3 storage. A network blip resets the connection right after MinIO stores the new index, and the SDK gives up. The script logs `"status": "error"` and plans to retry tomorrow. Meanwhile the next `pull` restart fails with "storage contains an object that is not referenced in the binlog index", and the backup stops.
4. **Ambiguous index PUT (step 2), filesystem.** Trigger: the directory fsync fails after the rename. Purge reports `error` with the index committed and all victims present, which again wedges the storage.
5. **Index PUT fails cleanly.** Trigger: the exception arrives before the commit. Purge reports `error`, and nothing should change.
6. **Retry after interruption.** Trigger: scenario 1, 2, 3 or 4 followed by an operator retry of purge(T). The purging-mode constructor rejects the orphans, so the retry fails, and so does every later retry.
7. **Repeat of a completed purge.** Trigger: purge(T) twice with no faults. The second run hits "target binlog name is not present in the storage" (`:346-349`) and exits with `status: error` and no storage changes. That is idempotent in effect, but the outcome is reported as an error. The workload must accept either outcome without treating it as corruption.
8. **Purge after a writer crash.** Purge's own constructor performs tail recovery (truncation). This changes tail data only beyond the metadata size, so `[0, size)` is unchanged. This is allowed.
9. **Purge concurrent with a writer (N1, ticket 9). Excluded:** README:473 puts it out of the documented contract (see "Investigation results"). Kept for reference. The purging-mode constructor truncates the tail while a writer is mid-checkpoint, which changes the tail's bytes and violates "tail unchanged".

## Oracle / workload checks

**Setup.** Run purge only against a quiescent storage: no writer, per README:473. This keeps the purge in contract and makes the before/after state well defined. Run `purge_binlogs <target>` while faults are active. Optionally SIGKILL it after a random delay, or inject DELETE failures.

**Before purge.** Read and record:
- index `I0` / `L`, read directly from the backend;
- object list `O0`;
- for each binlog, its `.json` content and the hash of `data[0:size]`.

**After purge.** Whatever the exit, status or kill, read index `I1` / `L'` and objects `O1`. Let `P = L[k+1:]` be the expected suffix, where `L[k]` is the target. Then check:
- `Always("storage opens in every mode after any purge outcome")`: after success, warning, error or kill, the next fetch/pull start opens the storage and `list` returns success.
- `Always("purge leaves the tail resume point unchanged")`: the tail's name, `size`, and `previous_gtids ∪ added_gtids` (the GTID set) are unchanged.
- `Always("purge never alters surviving binlog data or metadata")`: for every name in `L'`, the `.json` is byte-identical to before and `hash(data[0:size])` is unchanged.
- `Always("new index is a suffix of the old index that keeps the tail")`: `L'` is a suffix of `L` and contains `L[-1]`.
- `Always("purge error status implies index unchanged")`: `status == "error" ⇒ I1 == I0`.
- `Always("purge success status implies no victim objects remain")`: `status == "success" ⇒ I1 == P`, and no victim object or victim `.json` remains.
- `Always("purge warning status implies index committed")`: `status == "warning" ⇒ I1 == P`, and the index never references a deleted object.
- GTID mode: `search_by_gtid_set` for GTIDs that are only in surviving files still returns the surviving files.
- `Sometimes(status == "error" and purge reached the index write)` shows that the fault window is explored.
- `Always(stdout parses as one JSON object with status in {success, warning, error})` is covered by `cli-json-envelope-and-exit-code-agree`.

**Retry / replay:**
1. Snapshot `I0`.
2. Run purge(T) and interrupt it, by kill or by DELETE failures.
3. Retry purge(T) up to k times without faults.
4. `Always("purge retried after interruption converges to an openable storage")`: the final storage opens in every mode (`list` returns success and `fetch` resumes), and the index equals `I0` with the prefix up to and including T removed. Expected to fail today.

Separately, run purge(T) twice without faults. `Always("repeating a completed purge changes no object")`: the object snapshot after the second run is byte-identical to the snapshot after the first. The second run may report `success` or `error`.

## SUT-side instrumentation suggestions

The codebase has no Antithesis SDK, so every SDK assertion below is missing.

- `REACHABLE("purge committed the index but object cleanup failed")` in the catch at `storage_core.cpp:421`. **Missing.** This is an exploration anchor for the wedge state. Focus 1 proposed `SOMETIMES(remove_objects threw after index commit, "purge committed with leftover objects")` at the same site for the same condition, so it is folded in here.
- `SOMETIMES(cleanup_warning_message non-empty)` at `storage_core.cpp:425-430`. **Missing.** Shows that the partial-delete path is reached.
- `REACHABLE("purge index committed")` right after `save_binlog_index()` (`:398`). **Missing.** Anchor for kill timing.
- `REACHABLE("purge: index put_object threw")`, in a catch around step 2 that rethrows. **Missing.** Guides exploration.
- `UNREACHABLE("purge: index committed but error reported")`, to add after a fix (for example, re-reading the index to disambiguate, or treating post-rename failures as warnings). **Missing.**
- `ALWAYS(victim_count < size(binlog_records_) before erase, "purge never selects the tail")`. **Missing.** A throw enforces this today; the assertion documents it.
- The validator `UNREACHABLE` markers from `kill-anywhere-storage-reopens` cover the next open.

## Key observations

- **Two windows.** The S3 ambiguous window is one round trip. The filesystem window is the few microseconds between `rename` and the directory `fsync`. Antithesis chooses the timing of network faults on the purge-container↔MinIO link during the PUT, so it hits these windows far more often than scripted tests do. The filesystem variant needs an injected fsync error.
- **No roll-forward.** After the index commit, nothing in the system remembers the victim names.
- **Startup work in purge mode.** The purging-mode constructor also deletes `.tmp` objects and truncates the tail (N1). The tail truncation is harmless only when no writer is live.

## Fault requirements

- Workload-driven SIGKILL of the purge process. No node termination needed.
- Network faults between the purge task's container and the S3 server: partition, reset, and latency high enough to exceed the SDK timeout.
- For the filesystem variant, the repo's `fsfault.c` LD_PRELOAD shim with an `fsync` EIO on the directory.
- No clock faults needed.

## Open questions

- **Is a storage left unopenable after an interrupted or `warning` purge accepted behaviour, or a bug?** `(partial: maintainer comments (purge_binlogs_operation.cpp:62-69, storage_core.cpp:388-404) make "warning ⇒ storage will need attention before the next fetch/pull" the designed outcome; no repair path is documented, and the kill and error-status wedges are not addressed)`
  - If it is accepted: the `Always` on reopen and the retry-convergence check become recoverability expectations. The property keeps the data-preservation clauses, the status-honesty clauses and the no-op repeat check.
  - If it is not accepted: the property stays P0 as written.
- **Should an operator retry a killed purge, and should the retry converge?** `(needs human input)` Today the retry fails in its constructor (confirmed: `validate_binlog_index` exempts only `querying_only`, `storage_core.cpp:766-770`). The answer decides whether the oracle requires "a second purge with the same target succeeds".
- **Is `error` intended to mean "nothing changed"?** `(partial: README lists only pre-commit validation failures as error reasons; the code reports error for post-commit failures inside save_binlog_index; no statement of intent)` If maintainers say "error means unknown state, check `list`", the status check weakens to "error ⇒ storage still openable or index unchanged".
- **Should the filesystem backend treat a failed directory fsync after a successful rename as success-with-warning?** `(needs human input)` This decides whether the filesystem variant is a bug or accepted behaviour.
- **What are the CRT client's default retry count and request timeout in SDK 1.11.774?** `(partial: PBS sets none (s3_storage_backend.cpp:217-253); QA observed single failures retried transparently and six consecutive failures surfaced, so the budget is between 2 and 6 attempts; timeouts unknown)` This affects how much fault time the workload must inject, not whether the property is valid.

## Investigation results (2026-10-13)

- **Maintainers acknowledge the `warning` wedge.**
  - `purge_binlogs_operation.cpp:62-69` (Yura Sorokin, bf0c238, upstream `main`) says the `warning` status exists "so the operator knows the storage will need attention before the next 'fetch' / 'pull' run".
  - `storage_core.cpp:391-396, 399-404` (7f8cb69, maintainer) says leftovers "will trip the constructor's validators on next startup".
  - README:441 presents `warning` only as "partial success" and gives no repair procedure.
  - **Implication for the oracle.** Split the reopen check by outcome, so that triage separates the designed case from the undesigned ones:
    - `Always("storage opens in every mode after a purge that reported success or error")`
    - `Always("storage opens in every mode after a killed purge")`
    - `Always("storage opens in every mode after a purge that reported warning")`. This one is expected to fail by design today, and it becomes a recoverability expectation if maintainers confirm the design.

    The status-honesty, data-preservation and suffix checks are unaffected.
- **A repeated purge returns `error`, as documented.** README (`purge_binlogs` error reasons) lists `cannot purge: target binlog name is not present in the storage`, which `select_purge_victim_count` raises (`storage_core.cpp:345-350`). The workload accepts `error` with exactly this message for the second run of a completed purge.
- **Purge alongside a writer is out of the documented contract.** README:473 (maintainer, 91c4f80, before the ticket snapshot base ee82afa) says it "may cause data corruption and should be avoided". The workload runs purge only on a quiescent storage, and scenario 9 (N1, ticket 9) is excluded from this property. Ticket 9 claims the README "only forbids purging the binlog currently being written", which is incorrect.
- **Property scope narrowed:** scenario 9 is out. The oracle's reopen check is split as above. Type and priority are unchanged.

### Investigation Log

#### Is a storage left unopenable after an interrupted or `warning` purge accepted behaviour, or a bug?

- Examined: `purge_binlogs_operation.cpp:45-87` and git blame, `storage_core.cpp:368-433` and git blame, README:374-473, sut-analysis N3.
- Found:
  - The maintainer comments explicitly expect leftovers to make the next start fail, and expect `warning` to tell the operator to intervene.
  - The README does not say how to intervene.
  - The kill-after-commit case gives no status at all.
  - The ambiguous-commit case gives `error`.
- Not found: any statement on whether the wedge is acceptable for kill and `error` outcomes, or whether auto-repair is planned.
- Conclusion: `(partial: …)`.

#### Should an operator retry a killed purge?

- Examined: `storage_core.cpp:126-221, 764-787, 927-997`, README purge section.
- Found: the purging-mode constructor runs the same index and metadata validation as streaming. Only `querying_only` is exempt. A retry after orphans therefore fails before doing anything.
- Not found: any documented retry guidance.
- Conclusion: `(needs human input)`.

#### Should a repeated purge of an already-purged target return `success` or `error`?

- Examined: README purge error reasons, `storage_core.cpp:332-366`.
- Found: the documented error reason "cannot purge: target binlog name is not present in the storage".
- Conclusion: resolved. `error` is the documented behaviour, and the oracle accepts it.

#### Is "error" intended to mean "nothing changed"?

- Examined: README purge section, `storage_core.cpp:388-397`, `filesystem_storage_backend.cpp:127-160`, `s3_storage_backend.cpp:369-386`.
- Found:
  - Every documented error reason is a pre-commit validation failure.
  - The code has two post-commit failure paths: a directory-fsync failure after rename, and an S3 PUT applied server-side but reported failed. Both are reported as `error`.
- Not found: an intent statement.
- Conclusion: `(partial: …)`.

#### Should a failed directory fsync after rename be success-with-warning?

- Examined: `filesystem_storage_backend.cpp:127-160` comment.
- Found: the maintainers describe both fsyncs as "required for full crash durability", so a failure is treated as a failed put. Nothing classifies it as committed.
- Conclusion: `(needs human input)`.

#### CRT default retry count and timeout

- Examined: `s3_storage_backend.cpp:180-260` (only region, scheme, endpoint and virtual addressing are set), `extra/qa/s3_fault_sweep.py:17-50`, ticket 18 "Tests".
- Found: QA's results bound the retry budget between 2 and 6 attempts.
- Not found: the SDK defaults (the SDK source is not in the repo).
- Conclusion: `(partial: …)`.

#### Is purge concurrent with a writer in contract?

- Examined: README:473 and its git blame (91c4f80, maintainer, 2026-06-05), ticket 9 "Impact".
- Found: the README explicitly discourages it, and that text predates the tickets' code base.
- Conclusion: resolved. It is out of the documented contract and excluded here. Ticket 9's README claim is wrong.

## Evaluation refinements (2026-10-13)

Applied by the evaluation synthesis (`../evaluation/synthesis.md`). Where this section and earlier text in this file disagree, this section wins; the catalog entry has been updated to match.

- Added vacuity marker `Sometimes("purge interrupted on an encrypted storage")`: purge removes `<name>.json` (which holds the per-file envelope) before the payload, and startup validates every indexed file's KEK (coverage-balance F9).
- The warning-reopen failure (N3) shows under `kill-anywhere-storage-reopens`'s "interrupted purge" validator message, separate from ticket 15's message. N3 wedges the storage; containment is bias B3.
- Runs in `fetch_fs`, `s3`.
