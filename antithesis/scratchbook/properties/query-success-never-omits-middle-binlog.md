# query-success-never-omits-middle-binlog — A successful query never silently omits a binlog

Found by: focus 11 (wildcard), focus 4 (protocol), focus 2 (concurrency).
Merged from: query-success-never-omits-middle-binlog (focus 11), query-read-error-not-reported-as-success (focus 4), query-sees-binlogs-flushed-before-start (focus 2).

## Synthesis notes

Safety, P1.

Shared mechanism: `querying_only` construction erases any binlog record whose `.json` load throws, or whose `.json` was missing from its t0 listing (storage_core.cpp:933-950). It does this silently (null_logger) and still reports success. search_helpers.cpp:64-85 has no contiguity check.

Workload `Always` messages:
- "list: success output is a contiguous run of the pre-query index"
- "search_by_timestamp: success output is contiguous"
- "search_by_gtid_set: success output is contiguous"
- "search_by_timestamp: success result equals reference selection on a quiescent storage"
- "query result includes every binlog flushed before it started with size >= its pre-query size"

SUT-side:
- `UNREACHABLE("querying_only dropped a binlog record that is not the leading one")` at storage_core.cpp:946-949. This is the single SUT check at that site. The wildcard proposed `Sometimes(dropped record is neither first nor last)` for the same condition; the UNREACHABLE is used instead because a success response with a hole is never acceptable.
- `SOMETIMES(querying ctor dropped the tail record)`.
- `Sometimes(status error during an S3 fault)`.

Priority conflict: focus 4 rated it P0, focus 11 P1, focus 2 P2. Resolved to P1. The answer is silently wrong (PITR skips a whole binlog), but the stored data is intact, so a re-run answers correctly. Reachability depends on one of two things: an S3 GET failure that outlasts the CRT retries (unverified), or filesystem readdir/rename semantics.

The empty-records `front()` crash belongs to cli-json-envelope-and-exit-code-agree and is only referenced here.

**Gating the UNREACHABLE.** Focus 2 documents a legitimate tail drop: the tail's `.json` was created after t0 but is already in the index at t1. A pure `record_it != begin()` condition would fire on that drop. The UNREACHABLE should therefore be gated on "neither first nor last" (the wildcard's condition), or on "its `.json` was present in the t0 listing".

## What led here

- **Wildcard (focus 11).** The question was what PITR tooling sees when it consumes `search_by_*` output while the storage is changing or the S3 link flaps. `querying_only` construction catches **any** exception from loading a binlog's `.json` and silently erases that record (`src/binsrv/storage_core.cpp:937-950`). Query modes run with a `null_logger` (`src/operations/search_by_timestamp_operation.cpp:55-57`, `list_operation.cpp:48-50`), so nothing is logged and no warning reaches the JSON response, which still says `status: success`. The record loop runs over the index in order. A transient load failure (a partition that heals mid-loop, or an S3 GET 5xx/timeout on one key) can leave a **hole**: records 1..k-1 and m+1..N are present and k..m are missing.
- **Protocol contract, "response codes that mask errors" (focus 4).** The code comment says the drop is for forensics on an otherwise unusable storage. README promises that `list` prints "the complete list of binlog files". On a **quiescent** storage, a transient backend read error on one `<name>.json` is not a race: the metadata is committed and readable a moment later. Examples are an S3 5xx after SDK retries, a partition between the query container and S3, a timeout, or EIO on the filesystem.
- **Concurrency, list/search beside a writer (focus 2).** `querying_only` tolerates a writer by silently **dropping** records it cannot validate (`storage_core.cpp` ~:942-950). A dropped record is not an error. The risk is a query that quietly answers from less data than was already durably stored before it started. For `search_by_gtid_set` that means a false "cannot be covered" or a wrong file set for PITR (W6, silent).

## Code paths

- `storage_core::load_and_validate_binlog_metadata_set` (`storage_core.cpp:928-1004`; record loop `:930-952`). The ctor runs in this order: `list_objects` (t0) → `load_metadata` → `load_binlog_index` (t1) → for each record, a membership check of `<name>.json` in the **t0** listing (`:937-940`), then `load_binlog_metadata` (t2; call at `:941`, defined at `:843`, issues a backend GET). Inside `try`, the record is erased if its name is missing from t0 or the load throws. In `querying_only`, the `catch (const std::exception &)` erases the record (`:943`, `:943-949`, `:946-949`; `erase` at `:948`) and continues. This covers `s3_error`, which is a `std::system_error`. The "debugging/forensics" comment is at `:944-947`.
- The erase can hit any record, not only the front, so the result can have a **gap in the middle**.
- `search_helpers.cpp:64-85` (`:64-84`) `select_records_by_timestamp` returns indices 0..j, where j is the last record before the first one with `min_timestamp > t`. A missing middle record is simply absent from the result. There is no contiguity check, and the operation prints `success`.
- `search_helpers.cpp:30-61` (`:30-62`) `select_records_by_gtid_set` is greedy over `added_gtids`. A missing middle record makes its GTIDs uncoverable, so the operation fails closed with a false "cannot be covered" error, unless the target does not intersect them.
- `list_operation.cpp:42-65` (`:52-55`) lists whatever survived, with `success`.
- If every record is erased, `binlog_records_.front()` at `:1001` (and the debug assert at `:219`) runs on an empty vector. That is UB, typically a crash with no JSON output. It is owned by cli-json-envelope-and-exit-code-agree.
- Purge deletes per victim in the order metadata, payload, metadata, payload (`storage_core.cpp:400-406`).

## Hole / drop triggers

1. **S3 GET failure on one `.json`** (focus 11, focus 4). A partition, reset, latency or congestion between the query container and S3 outlasts the CRT client's default retries for key k, then heals before key m+1. No shim is needed. `s3_fault_proxy.py` with a per-key GET 5xx is a deterministic version.
2. **Concurrent purge interleaving** (focus 11). A query that loaded `json1` before the purge and reaches `json2` after its deletion keeps 1 (stale), drops 2 and keeps 3+. The result `{1,3,...}` has a hole, and file 1 no longer exists either. (focus 2 (iii): a purge deleting a victim's `.json` between t0 and t2 is legitimate for a prefix drop.)
3. **Filesystem EIO on one `.json` read** (`fsfault` shim, `fsfault.c` EIO on `open/read`).
4. **`readdir` skipping an entry during a concurrent rename-over-existing** (focus 2 (i)). Every checkpoint renames `<tail>.json.tmp` → `<tail>.json`. POSIX leaves unspecified whether entries added or removed during iteration are returned. ext4 and xfs probably replace in place and are safe. On tmpfs, `d_move` relocates the dentry in the parent's child list and could make the cursor skip it. Not verified. Only the tail's `.json` is rewritten after rotation, so this drops the newest binlog: the result is still a *prefix*, but a stale one. If that binlog is the only one, the ctor reaches the empty-records crash.
5. **S3 listing not strongly consistent** (focus 2 (ii), A10).
6. **Legitimate, not a violation** (focus 2). A record whose `.json` was created after t0 but is in the index at t1 is dropped. That is acceptable: it was not committed at query start, because the index is written after the `.json`.

## Failure scenarios

**A. PITR planning across an S3 hiccup (focus 11, trigger 1).** The operator runs `search_by_timestamp T` to plan PITR. A short S3 hiccup drops `binlog.000007` from the middle. The JSON says `success` and lists 1..6 and 8..12. The restore tool replays them in order and silently skips every transaction in file 7. W6 in sut-analysis is "silent" by nature, and this is a concrete single-fault path to it.

**B. Restore script on a flaky MinIO link (focus 4, trigger 1, quiescent storage).** A restore script runs `search_by_timestamp config.json 2026-10-13T09:00:00` against an S3 storage while the network to MinIO is flaky. The GET of `binlog.000042.json` fails after retries. The script gets `success` with files 000040, 000041, 000043, 000044 and replays them. The restored database silently lacks every transaction from 000042. **Silent data loss in restore.**

**C. Stale result beside a writer (focus 2, trigger 4).** A query runs while the writer checkpoints. readdir skips the tail's renamed-over `.json`, and the tail binlog, already flushed before the query started, is missing from the result. `search_by_gtid_set` for GTIDs stored before the query returns a false "cannot be covered".

## Oracle / workload checks

Before each query, the workload snapshots the index (`binlog.index` content read directly from the backend) and, for the concurrency variant, the `.json` sizes, or uses a previous `list`. It snapshots again after the query. Variants:
- **Quiescent** (no writer, no purge; focus 4): the authoritative index is known. Query commands run repeatedly while Antithesis injects faults on the query ↔ S3 path.
- **Concurrent with a writer / purge** (focus 2, focus 11).

| Workload assertion | Type | Notes |
|---|---|---|
| "list: success output is a contiguous run of the pre-query index" | `Always` | A prefix-dropped or tail-dropped run is acceptable when concurrent purge/writer is running. A gap is not. On a quiescent storage, the run must be the whole index. |
| "search_by_timestamp: success output is contiguous" | `Always` | A contiguous run starting at the first indexed binlog that existed throughout the query. |
| "search_by_gtid_set: success output is contiguous" | `Always` | The union of returned `added_gtids` ⊇ target (already enforced). **Also**, no file between two returned files may be missing from the index snapshot while covering part of the target. |
| "search_by_timestamp: success result equals reference selection on a quiescent storage" | `Always` | Reference = the leading run computed from the authoritative records. |
| "query result includes every binlog flushed before it started with size >= its pre-query size" | `Always` | Snapshot names ⊆ result names, sizes non-decreasing (focus 2). |
| "search_by_gtid_set succeeds for a GTID set fully stored before the query started" | `Always` | Never a false "cannot be covered" (focus 2). |
| "query returned status error during an S3 fault" | `Sometimes` | Shows the fault path is reached. A fail-closed answer is acceptable. |
| "query result strictly newer than its pre-query snapshot" | `Sometimes` | Shows that query/writer overlap is exercised (focus 2). |

## SUT-side instrumentation

The codebase has no Antithesis SDK, so every entry below is missing.

| Assertion | Site | Status | Notes |
|---|---|---|---|
| `UNREACHABLE("querying_only dropped a binlog record that is not the leading one")` | `querying_only` erase branch, `storage_core.cpp:946-949` (catch at `:943`) | missing | Single SUT check at this site. It replaces focus 11's `SOMETIMES(record_it != begin && std::next(record_it) != end, "querying_only dropped a non-edge binlog record")`. Gate it on "neither first nor last" (or "its `.json` was in the t0 listing"), so the legitimate tail drop and the leading drop under concurrent purge do not fire it. |
| `SOMETIMES(querying ctor dropped the tail record, "query dropped tail binlog under concurrency")` | same branch, plus the t0-membership branch `:937-940` | missing | focus 2. |
| `REACHABLE("querying_only: dropped a binlog record")` (generic exploration) | same branch | missing | focus 4. The same condition as cli-json-envelope-and-exit-code-agree's `SOMETIMES(querying ctor dropped a record)`; reference that one instead of adding a second. |
| `ALWAYS(!binlog_records_.empty())` before `front()` | `storage_core.cpp:1001` | missing (debug assert at `:219` is partial) | Owned by cli-json-envelope-and-exit-code-agree; referenced only. |

A product fix could surface a `warning` status instead of `success` (see the first open question).

## Key observations / Antithesis angle

- The failure needs a backend read error on one specific metadata GET while the others succeed: a partition or reset with the right timing during a multi-GET loop. Antithesis' network faults on the query-container ↔ MinIO link produce this naturally. Deterministic tests do not.
- Sibling views are partial: tail/prefix drops under concurrency, and errors or the empty-records crash (cli-json-envelope-and-exit-code-agree). A middle hole is a distinct, silent outcome.
- `search_by_gtid_set` mostly fails closed (a false error). `search_by_timestamp` and `list` fail open (success with a hole).

## Fault requirements

- Network faults (partition, reset, latency beyond the SDK timeout) between the query task's container and the S3 server.
- Filesystem backend: `fsfault.c` EIO on `open/read` of a `.json`.
- Concurrency variant: a workload-driven writer (checkpointing) and/or purge running beside queries.
- No clock faults or node termination are needed.

## Open questions

- Should a hole downgrade the status to `warning`/`error`, or is the catch-all drop meant to cover transient I/O errors on committed metadata? `(needs human input)`
  - The commit that introduced the drop (cc49ea5, PBS-27, released in 0.3.1) gives a narrower purpose: tolerating a concurrent writer.
  - Nothing documents dropping committed middle records on I/O errors.
- How long must an S3 fault last to surface past the CRT retries? This affects reachability only, not the property. `(partial: CRT throughput monitor fails a stalled GET after ≥3 s at <1 B/s; the aws-c-s3 default retry count is unverified)`
- Which filesystem will back the Antithesis storage volume, and can its `readdir` skip a renamed-over entry? `(partial: topology uses a named volume at /data (not tmpfs); the host filesystem under Antithesis is unknown)`
- Is dropping a tail binlog whose `.json` was already committed before the query started acceptable? `(partial: PBS-27 only motivates tolerating a not-yet-committed tail; no stated intent covers an already-committed tail)`

## Findings from investigation (2026-10-13)

- **Intent evidence.** The `querying_only` catch was added in cc49ea5 (PBS-27, "list command fails with ERROR if executed while fetch/pull is in progress"). Its commit message describes only concurrent-writer states, where the writer has updated the data file but not its metadata. A middle hole on committed metadata falls outside that intent. **No property change:** `success` with a hole stays a violation.
- **The catch is narrower than its comment.** `validate_binlog_metadata` runs **outside** the `try` (`storage_core.cpp:953`). The skip therefore covers only three cases: a `.json` missing from the t0 listing, a backend GET failure, and a JSON or `nv_tuple` parse failure. Semantically invalid metadata, such as a GTID-mode mismatch, still throws in `querying_only`. This contradicts the comment's "skip invalid metadata" and makes I/O errors one of the main things the skip actually catches.
- **Write ordering confirmed.** `open_new_binlog_file_internal` saves `<name>.json` before `binlog.index` (`storage_core.cpp:700-701`). So the legitimate "json after t0 but in index at t1" tail drop is exactly the not-yet-committed tail.

### Investigation Log

#### Is the silent drop intended for transient I/O errors, or only for semantically invalid metadata?

- Examined: `storage_core.cpp:927-1004` (the catch at :942-950 and `validate_binlog_metadata` at :953, outside the try); `git log -S` for the comment → cc49ea5 (PBS-27) and 7f8cb69 (move into `storage_core`); the cc49ea5 commit message; the debian changelog entry for 0.3.1 (PBS-27); README `list` section.
- Found:
  - The commit message says the relaxation exists so that queries running concurrently with fetch/pull do not error on transient writer states.
  - The code comment adds "debugging / forensics" on an otherwise unusable storage.
  - Neither mentions backend I/O failures on committed records, and neither mentions reporting a drop.
  - Semantic validation is not covered by the skip.
- Not found: a maintainer statement on whether a drop should change the status.
- Conclusion: `(needs human input)`. The property is unchanged, and the documented intent supports it.

#### What are the S3 CRT client's default GET retry count and timeout in SDK 1.11.774?

- Examined: the same sources as pull-chain-converges-after-heal (aws-sdk-cpp 1.11.774 `S3CrtClientConfiguration.h/.cpp`, `S3CrtClient.cpp`, `ClientConfiguration.h`; `s3_storage_backend.cpp:185-256`, which uses a default-constructed config).
- Found:
  - No overall request timeout.
  - Connect timeout 1000 ms.
  - The throughput monitor fails a transfer below 1 B/s for ≥ max(3 s, requestTimeoutMs/1000).
  - The retry strategy is left to aws-c-s3's default.
- Not found: the aws-c-s3 default `max_retries`.
- Conclusion: `(partial: ...)`. A partition of several seconds probably surfaces as a GET failure. Reachability only.

#### Which filesystem will back the Antithesis storage volume?

- Examined: `deployment-topology.md` ("named volume at `/data`", storage subdirectories, optional `tmpfs` variant only for ENOSPC).
- Not found: the backing filesystem of Antithesis Docker named volumes.
- Conclusion: `(partial: ...)`. If the ENOSPC `tmpfs` variant is used, the readdir trigger may become reachable there.

#### Is dropping the whole tail binlog acceptable?

- Examined: cc49ea5 commit message; `storage_core.cpp:679-702` (`.json` saved before the index); `storage_core.cpp:937-950`.
- Found: the only motivated tolerance is a writer state that is not yet committed. A tail whose `.json` was in the t0 listing and whose index entry predates the query is committed.
- Conclusion: `(partial: ...)`. The workload oracle stays names plus sizes for records committed before the query started; a lag of one checkpoint in size is allowed via `.json` size ≥ pre-query size.
