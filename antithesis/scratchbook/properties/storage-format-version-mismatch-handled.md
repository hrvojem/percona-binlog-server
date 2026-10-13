# storage-format-version-mismatch-handled — Storages written by other builds are opened or refused cleanly

Found by: focus 10 (version compat; two independent findings)
Merged from: legacy-binlog-metadata-handled (focus 10), older-build-refuses-newer-storage (focus 10)

## Synthesis notes

> **Updated 2026-10-13:** the downgrade invariants as written would fail deterministically on any downgrade run, and no released binary has the listener. See "Corrections and findings from investigation".

Safety, P2 (both contributors P2). Both findings are storage-format version compatibility with no version bump:
- `last_sequence_number` was added in pbs-0.2.4 without a default, so 0.2.1–0.2.3 storages brick writers and make query modes hit UB;
- encryption was added in 0.4.0 while both metadata versions stayed 1 and `nv_tuple_from_json` ignores unknown keys, so an older binary silently appends plaintext to an encrypted tail.

Workload `Always` messages:
- "writer on a legacy-format storage refuses cleanly without changing objects"
- "older binary exits non-zero without changing any object when metadata.json records encryption"
- "current-binary pull stream equals source after an older binary ran"
- "list success on a legacy storage lists every indexed binlog"
- "no query-mode process dies by signal on a legacy storage"

SUT-side: the `UNREACHABLE("querying_only erased every binlog record")` proposed before `storage_core.cpp:1001` is the same site as the `ALWAYS(!binlog_records_.empty())` already planned by cli-json-envelope-and-exit-code-agree; this property **references** that assertion instead of adding a second one. Likewise `SOMETIMES(query mode skipped unreadable binlog metadata)` would sit in the same `querying_only` catch (`storage_core.cpp:942-950`) where query-success-never-omits-middle-binlog already plans `SOMETIMES(..., "querying_only dropped a non-edge binlog record")`; it is referenced, not duplicated. Needs a legacy fixture and a previous-release binary in the image.

## What led here (code- and git-confirmed)

### Legacy metadata (pbs-0.2.1–0.2.3 storages opened by ≥ 0.2.4)

- `binlog_file_metadata.hpp:46-54`: the per-binlog `.json` schema is an `nv_tuple`, and every field is extracted by `nv_tuple_from_json` (`util/nv_tuple_from_json.hpp:66-79`). A **missing** key is replaced with JSON `null` and passed to `boost::json::value_to<T>`. For optional types (`optional_gtid_set`, `optional_binlog_file_encryption_record`) null → nullopt. For a plain `std::uint64_t` it throws.
- `git log -S last_sequence_number`: the field `last_sequence_number` (`events::seq_no_t` = `std::uint64_t`, `events/common_types.hpp:23`) was added in cf07e1d (PS-11080 part 2), first released in **pbs-0.2.4**. Field lists per tag (checked with `git show <tag>:src/binsrv/binlog_file_metadata.hpp`):
  - pbs-0.2.1, 0.2.3: version, size, previous_gtids, added_gtids, min_timestamp, max_timestamp
  - pbs-0.2.4…0.3.1: + last_sequence_number
  - pbs-0.4.0+: + encryption (optional)
- `expected_binlog_file_metadata_version` stayed **1** in every tag (`binlog_file_metadata_fwd.hpp:25`), and there is no migration or defaulting code. A 0.2.1–0.2.3 storage therefore fails with `unable to extract "last_sequence_number"` in the current build. The storage-level `metadata.json` is also still version 1 (`storage_metadata_fwd.hpp:25`) and gives no hint.

### Older binary on a newer (encrypted) storage

- `util/nv_tuple_from_json.hpp:66-71` (same at pbs-0.3.1, checked with `git show pbs-0.3.1:...`) looks up only the keys it knows and **ignores unknown keys**.
- Encryption arrived in pbs-0.4.0. It added `encryption` to `metadata.json` (`storage_metadata.hpp:35-37`) and to every `<binlog>.json` (`binlog_file_metadata.hpp:53`). **Neither version number was bumped**: both are still 1 in every release tag.
- The same pattern applies to any future additive field: the format has no "required features" or minimum-reader-version field.

## Code paths

- `binlog_file_metadata.hpp:46-54`, `:53` (encryption field); `binlog_file_metadata_fwd.hpp:25` (version 1).
- `util/nv_tuple_from_json.hpp:66-79` (missing key → null → throw for non-optional), `:66-71` (unknown keys ignored).
- `events/common_types.hpp:23` (`seq_no_t`).
- `storage_metadata.hpp:35-37` (encryption in `metadata.json`); `storage_metadata_fwd.hpp:25` (version 1).
- `storage_core.cpp:927-1005` per-construction-mode metadata load; `:942-950` `querying_only` catch that erases unreadable records; `:1001` `binlog_records_.front()`.
- Older-binary recovery path: `resize_object` truncation on files whose bytes it cannot interpret.

## Failure scenarios

- **Legacy storage, streaming / purging mode** — the exception propagates, so fetch/pull/purge refuse the storage. Loud but permanent: there is no upgrade path, and the backup chain is bricked for the new binary.
- **Legacy storage, querying_only mode** — the catch at `storage_core.cpp:942-950` **erases** every record whose metadata cannot be read and continues. On a legacy storage that is every record, so `binlog_records_` becomes empty, and `:1001` then dereferences `binlog_records_.front()` on an empty vector (UB; compare ticket 13 for the empty-index variant). If only some records fail (e.g. a mix), `list` returns `status: success` with an incomplete list, and `search_by_gtid_set` / `search_by_timestamp` can return wrong answers.
- **Runtime trigger of the same erase path** — whenever a `.json` read fails transiently (S3 5xx/timeout on GetObject during `list`), a query **silently omits** a binlog and still reports success. Timing-dependent under S3 faults (overlaps query-success-never-omits-middle-binlog).
- **Downgrade trigger** — a pbs-0.3.x binary started on a 0.4.x encrypted storage:
  - passes `storage_metadata::validate` (version 1) and ignores `encryption`;
  - reads `<binlog>.json` without the envelope, so it treats ciphertext as plaintext;
  - on resume, opens the tail in `app` mode and appends **plaintext** events after AES-CTR ciphertext. The size checks pass, because sizes are byte counts;
  - in pull, serves ciphertext bytes as events to clients.
  The 0.4 binary started again later finds a tail whose envelope says "encrypted" but whose suffix is plaintext. Decryption then produces garbage for the suffix, silently.

## Antithesis angle

Mostly deterministic. The Antithesis value is:
- making the workload *start* from a legacy fixture storage as one of its initial states, so all modes are exercised against it under faults;
- the querying-only erase path, reachable at runtime under S3 faults (the stronger reason to keep the legacy part);
- a rolling upgrade/downgrade scenario: the workload restarts PBS with a randomly chosen binary from {current, previous release} across kills, against encrypted and unencrypted storages. Crash timing matters because the old binary's recovery path (`resize_object` truncation) runs on files whose bytes it cannot interpret.

## Oracle / workload checks

- Legacy fixtures: `Always("writer on a legacy-format storage refuses cleanly without changing objects")` — streaming modes exit non-zero with a metadata error and no object changes.
- Legacy fixtures: `Always("no query-mode process dies by signal on a legacy storage")` — `exit_code in {0,1}`, no signal.
- `Always("list success on a legacy storage lists every indexed binlog")` — for a `list` with `status == success`, the listed names equal the index lines (read the index object directly).
- Downgrade: after every run of the older binary on a storage with `metadata.json.encryption` set, `Always("older binary exits non-zero without changing any object when metadata.json records encryption")` (equivalently, no object mtime/etag changed by the older binary).
- End-to-end: with the current binary, pull-serve the storage and compare against the source (SG9): `Always("current-binary pull stream equals source after an older binary ran")`.

## SUT-side instrumentation suggestions

- Empty-records guard before `storage_core.cpp:1001`: **referenced** — owned by cli-json-envelope-and-exit-code-agree (`ALWAYS(!binlog_records_.empty())` before `:1001` and `:219`); this contributor proposed it as `UNREACHABLE("querying_only erased every binlog record")` / `ALWAYS_OR_UNREACHABLE(!binlog_records_.empty())`. Missing.
- Erase-path anchor in the `querying_only` catch (`storage_core.cpp:942-950`): **referenced** — query-success-never-omits-middle-binlog plans `SOMETIMES(..., "querying_only dropped a non-edge binlog record")` at the same catch; this contributor's `SOMETIMES(record erased in querying_only, "query mode skipped unreadable binlog metadata")` would be a second assertion at that site. Add it only if a site-wide (any-record) anchor is wanted in addition to the non-edge one. Missing.
- In the current binary: `UNREACHABLE("binlog metadata records encryption but storage metadata does not")` when `<binlog>.json` has `encryption` but `metadata.json.encryption` is null — would detect a storage touched by a downgraded writer only partially. Missing.
- None useful in the old binary (it cannot be changed).

## Fault requirements

- A legacy (0.2.1–0.2.3) fixture storage, and a previous-release binary (e.g. pbs-0.3.1) in the image.
- PBS kills across binary switches; S3 GetObject errors/timeouts during query modes.

## Open questions

- Is opening a 0.2.1–0.2.3 storage with ≥ 0.2.4 a supported upgrade? If yes, this is a real compat bug (missing default or version bump). If no, the expected behaviour is a clear "unsupported storage version" refusal, and the legacy part reduces to "fail cleanly, never UB". `(needs human input)`
- Should `list` report `warning` instead of `success` when it skipped records? Shared with query-success-never-omits-middle-binlog. If `success` is intended, this property only requires "no crash" and "listed ⊆ index". `(needs human input)`
- Is binary downgrade supported, or explicitly out of scope? If out of scope, the downgrade part becomes a recommendation (bump `expected_*_version` on schema change) rather than a property to run. `(needs human input)`

## Corrections and findings from investigation (2026-10-13)

- **No released binary has the replication-source listener.** `src/minimysql` was merged on 2026-06-27/07-01 (84bee8e, 9362b55), but it is not an ancestor of any tag (`git merge-base --is-ancestor 9362b55 pbs-0.4.1` → false). The downgrade bullet "in pull, serves ciphertext bytes as events to clients" therefore cannot happen with an older binary. pbs-0.3.1 `pull` is collector-only.
- **The older binary destroys the tail's file key** (code-traced at tag pbs-0.3.1; stronger than "appends plaintext"). pbs-0.3.1 `storage::save_binlog_metadata` (`git show pbs-0.3.1:src/binsrv/storage.cpp:655-664`) rewrites `<binlog>.json` with only the fields it knows, so the `encryption` envelope is dropped on the first checkpoint or close.
  - The wrapped file key of the tail binlog is then gone, and its encrypted prefix becomes **permanently undecryptable**.
  - The current binary later sees that tail as plaintext (no envelope) and serves or decrypts nothing for it.
  - pbs-0.3.1 does not rewrite `metadata.json` on a non-empty storage (it saves only when the storage is empty, `:71-77`), so `metadata.json` keeps `encryption`.
- **The older binary has no truncation recovery.** `resize_object` recovery arrived in pbs-0.4.1 (PBS-21). pbs-0.3.1 *refuses* a data/metadata size mismatch (`"size from the binlog metadata does not match"`, `storage.cpp:~700-705` at that tag). The "older-binary recovery path: `resize_object` truncation" bullet is wrong for 0.3.1. Crash timing still matters: a crash between data and metadata writes makes 0.3.1 refuse the storage.
- **The previous-release binary is obtainable.** The repo has tags pbs-0.2.1 to pbs-0.4.1. pbs-0.3.x already uses Boost 1.90 and AWS SDK 1.11.774 (0.3.0 changelog), so a pbs-0.3.1 build fits the same builder stage. pbs-0.2.3 predates that dependency bump. The simpler legacy fixture is a current storage with `last_sequence_number` removed from each `<binlog>.json`: the format is plain JSON, and the field lists per tag are known. The harness question is resolved: build pbs-0.3.1 from the tag in the multi-stage Dockerfile.
- **The downgrade invariant changes.** `Always("older binary exits non-zero without changing any object when metadata.json records encryption")` describes a fix that does not exist. pbs-0.3.1 cannot be changed, and today it never refuses. As written, the assertion fails deterministically on the first downgrade run. Keep it only as a **regression guard for a future version bump**, and only if the maintainers declare downgrade in scope. Otherwise move it to recommendations. The end-to-end `Always("current-binary pull stream equals source after an older binary ran")` would also fail deterministically, because of the destroyed envelope.
- **Legacy query path confirmed.** A missing `last_sequence_number` throws inside `load_binlog_metadata` (`storage_core.cpp:843-861`), which is inside the `try`. Every record is erased, and then `front()` runs on an empty vector (`:1001`). This matches the original analysis.

### Investigation Log

#### Is opening a 0.2.1–0.2.3 storage with ≥ 0.2.4 a supported upgrade?

- Examined: `packaging/debian/changelog` (0.2.4 entry: PS-11137, PS-11136, PS-11080; no upgrade or migration note); README (no upgrade section); `binlog_file_metadata_fwd.hpp:25` and `storage_metadata_fwd.hpp:25` (both versions 1); `util/nv_tuple_from_json.hpp:66-79`.
- Found: no migration code, no version bump and no documentation either way.
- Conclusion: `(needs human input)`.

#### Should `list` report `warning` when it skipped records?

- Examined: cc49ea5 (PBS-27) commit message; `storage_core.cpp:937-953`.
- Found: the skip was added to tolerate concurrent writers. It is silent by construction (`null_logger`), and no `warning` status exists in the JSON envelope.
- Conclusion: `(needs human input)`, shared with query-success-never-omits-middle-binlog.

#### Is binary downgrade supported or explicitly out of scope?

- Examined: README, changelog, tags; `git show pbs-0.3.1:src/binsrv/storage.cpp` (`save_metadata`, `save_binlog_metadata`, the size check); `git show pbs-0.3.1:src/binsrv/storage_metadata.hpp` (version + mode only).
- Found: no statement either way. The mechanics are worse than first described: the older binary drops the per-file envelope (see "Corrections").
- Conclusion: `(needs human input)`. The downgrade invariants as written would fail deterministically.

#### Does the harness have access to a previous-release binary?

- Examined: `git tag` (pbs-0.2.1 … pbs-0.4.1); changelog 0.3.0 (Boost 1.90, AWS SDK 1.11.774); `deployment-topology.md` (multi-stage builder).
- Found: buildable from tags with the same toolchain.
- Conclusion: resolved. Build pbs-0.3.1 in the builder stage. Synthesize the 0.2.1–0.2.3 fixture by stripping `last_sequence_number`, since pbs-0.2.x needs older Boost/SDK.
