# stored-state-never-regresses — Persisted progress and listed prefixes never regress

Found by: focus 1, 4, 9 (data integrity, protocol, idempotency/replay).
Merged from: stored-state-never-regresses (focus 1), listed-binlog-size-is-stable-readable-prefix (focus 4), s3-stale-put-replay-never-regresses-object (focus 9).

## Synthesis notes

- **Type and priority:** Safety, P1. Focus 1 and focus 4 rated it P1, and focus 9 rated it P2.
- **The invariant.** Once a binlog's `(size, added_gtids)` and its bytes `[0,size)` have been observed, either directly or through `list`, later observations never show:
  - a smaller size;
  - fewer GTIDs;
  - changed bytes in `[0,size)`;
  - a data object shorter than its `.json` size.
- **Index rule.** The index changes only in two ways: the tail grows by appending, or a purge issued by the workload removes a prefix.
- **Workload `Always` checks**, each with its own message:
  - "binlog metadata size never decreases"
  - "persisted binlog prefix bytes never change"
  - "data object never shorter than its saved metadata size". This one is polled and also checked at quiescence, including HEAD ContentLength on S3.
  - "list: object at uri holds at least the listed size"
  - "list: listed size falls on an event boundary"
- **SUT-side checks:**
  - In-process `ALWAYS(new saved size >= previous saved size)` in `save_binlog_metadata`.
  - Optional `ALWAYS(object size >= metadata size)` in `querying_only` construction.
  - Optional `SOMETIMES(S3 PutObject succeeded after an SDK retry)`.
- **Triggers, kept as variants:**
  - kill/restart loops;
  - S3 last-writer-wins, and delayed or replayed PUTs (packet hold);
  - purge or second-writer interleavings (ticket 9, N1, ticket 24).

## What led here

- **Focus 1: data integrity.** A backup must not lose what it has already persisted.
  - README:619/626 (SG8) say checkpointed data is written "permanently".
  - The `.json` metadata is the commit record of a checkpoint. It is written after the data and, on the filesystem, it is fsynced.
  - So once a `.json` with `size = s` and `added_gtids = A` has been observed, no later state of the storage should hold less for that binlog. The one exception is purge removing the whole binlog from the front.
  - This catches lost-update hazards that the byte-prefix property misses: a storage that shrank is still a prefix.
- **Focus 4: protocol contract.** The JSON output of `list`/`search_*` is the interface for PITR tooling. The README "list operation mode" section describes it as "name, current size in bytes, timestamps, URI".
  - A restore script downloads `uri` and replays the first `size` bytes. The tail object may legitimately be longer, because data reaches the backend before the metadata.
  - For that to be safe, three things must hold:
    1. The object at `uri` contains at least `size` bytes.
    2. Bytes `[0, size)` reported by any listing are identical in every later read. Binlog content is append-only by contract.
    3. For unencrypted, non-rewrite storages, `[0, size)` ends on an event (and transaction) boundary.
- **Focus 9: idempotency/replay.** On S3, every data checkpoint re-uploads the **whole** binlog object (`s3_storage_backend.cpp:850-863, 933-938`, ticket 17), and every metadata save is a whole-object PUT.
  - These PUTs are idempotent only if they arrive in order.
  - The SDK's own retries are left at defaults. `ClientConfiguration` is created with no timeouts and no retry strategy (`s3_storage_backend.cpp:189-255`).
  - Antithesis has a partition mode that "holds packets until the fault expires then delivers them at once". Suppose the client times out a request attempt whose packets are only *held*. The retry succeeds, and PBS moves on (`size +=`, then the next checkpoint PUT with more bytes).
  - The stale attempt can still complete server-side afterwards. Last writer wins, so the object regresses to an older, shorter body.

## Code paths

- **Write order.** Data append, then `size +=`, then the metadata PUT via `save_binlog_metadata` (`storage_core.cpp:299-322`, `:907-925`). With a single writer, the object is always at least the metadata size.
- **Recovery truncation** uses the metadata size as the floor (`:979-983`), so it should never cut below an observed metadata size.
- **Open-time validation:**
  - "cannot perform recovery - size from the binlog metadata is bigger than the actual binlog file size" (`:974-977`);
  - "size from the binlog metadata does not match" for non-tail files (`:968-972`);
  - an unindexed `.json` is rejected (`:779-783`).
- **Index rewrite.** The index is rewritten at every rotation from the in-memory list, which is loaded once at construction (`:214`, `:695-701`, `:789-799`).
- **Purge** rewrites the index first, then deletes (`:369-433`).
- **`list`** builds its response from `querying_only` metadata (`list_operation.cpp:42-65`, `model_helpers.cpp:28-43`).
  - `size` comes from each `<name>.json` (`storage_core.cpp:907-925`).
  - There is no check against the object, because `querying_only` skips the size check (`storage_core.cpp:952-953`).
  - The listing sizes are already collected at `storage_core.cpp:180-216`.
- **S3:**
  - Every write re-uploads the whole object from a local temp file (`s3_storage_backend.cpp:850-863`, `:933-938`), and the last writer wins on whole objects.
  - A short range read raises a hard `out_of_range` (`s3_storage_backend.cpp:296-300`).
  - The client is built with default retries and timeouts (`s3_storage_backend.cpp:189-255`).

## Failure scenarios

1. **[trigger: purge concurrent with a writer, ticket 9 (validated)].**
   - A purge runs during fetch/pull.
   - Later, the writer rewrites `binlog.index` from its stale list and resurrects the purged names, so the index grows at the front.
   - This undoes the purge, and the storage then fails to open.
2. **[trigger: two writers on S3, ticket 24 / A1].**
   - Writer B uploads its local copy, downloaded earlier, over writer A's newer object.
   - B's `.json` with a smaller `size` overwrites A's. This is observable as the metadata size decreasing.
   - On the filesystem, the two writers share deterministic `.tmp` names and append to the same file.
3. **[trigger: purge-mode constructor tail recovery on a live writer, N1].**
   - A purge in `purging` mode runs tail recovery and truncates the live writer's tail to the on-disk metadata size, while the writer has already appended more (`storage_core.cpp:974-983`).
   - The writer then saves metadata with the larger in-memory size while the object is shorter, and its next append lands at the truncated EOF.
   - A listing taken in between reports `size` > object length. A later listing shows different bytes in `[old, new)`.
   - The next start fails, and the bytes in between are lost.
   - Concrete case: a purge cron job runs while pull is writing (discouraged by README:473, but routine). A PITR script runs `list` at that moment and gets `size = S+n` for the tail, but the object is only `S` bytes long.
     - If it downloads then, the download fails.
     - If it downloads later, it replays bytes at `[S, S+n)` that belong to a different transaction than the one the earlier listing described.
4. **[trigger: S3 stale PUT replay via packet hold and SDK retry].** Several outcomes are possible:
   - **A data object shorter than its metadata.** Every streaming or purging start then fails with the "bigger than the actual binlog file size" error (`storage_core.cpp:974-977`), and the sender gets a short range (`out_of_range`, `s3_storage_backend.cpp:296-300`).
   - **A stale `<name>.json` (smaller size) on the tail.** Recovery truncates data that was already flushed, and that data is re-fetched. This is benign if the source still has it.
   - **A stale `<name>.json` on a non-tail file** (stale arrival after rotation). The "size from the binlog metadata does not match" error wedges the storage (`:968-972`).
   - **A stale `binlog.index` from before a rotation.** The new file is left unindexed and the storage is rejected (`:779-783`).
5. **[trigger: failed metadata save then destructor re-append, ticket 18].**
   - Listed prefixes stay identical, but the next listing's bytes beyond the old size duplicate a transaction.
   - The boundary and duplicate parts are covered by `failed-flush-never-reappends-data`.
6. **[trigger: S3 endpoint without read-after-write consistency for overwrites, A10].** After the metadata PUT, the endpoint can serve an older, shorter object.
7. **[trigger: page-cache loss, ticket 19].** The data becomes shorter than the fsynced metadata. This happens only with that fault.
8. **[allowed: restart after kill].**
   - Recovery truncates the tail to the last saved metadata size.
   - The observed size never decreases, because the observation was the metadata itself.

## Oracle / workload checks

### Per-binlog history

- The workload keeps, per binlog name:
  - the maximum `(size, added_gtids)` it has ever read from that binlog's `.json`;
  - the last observed index;
  - a history of `(name, size, sha256 of object[0:size])`.
- It observes:
  - after every PBS exit or kill;
  - periodically while writers run, reading the `.json` objects that exist;
  - by running `list` periodically on its own schedule, concurrently with writers and other tasks.
- **Read order.** Read the `.json` before the data object.
  - On the filesystem, reading a `.json` that is mid-rename is atomic.
  - After a listing, read the object directly from the backend (filesystem path or S3 GET) *after* the listing. Because data is written before metadata, a correct system never violates the length check.

### Checks

| Message | Check |
|---|---|
| `Always("binlog metadata size never decreases")` | If the binlog is still indexed: `size ≥ max_size` and `added_gtids ⊇ max_added`. |
| `Always("binlog index changes only by tail append or a workload-issued purge prefix removal")` | The new index equals `old_index[k:] + new_tail_names` for some k ≥ 0. A prefix may be removed only by a purge that the workload issued and that reported success or warning. Names are appended only at the end. |
| `Always("persisted binlog prefix bytes never change")` | For every earlier observation of the same name with size s0 ≤ current size, from a direct `.json` read or a `list` record, `sha256(object[0:s0])` equals the recorded hash. Encrypted storages compare ciphertext hashes: CTR keeps lengths and offsets, so the ciphertext is also stable. Rewrite storages: the prefix-stability check still applies. |
| `Always("data object never shorter than its saved metadata size")` | For every indexed binlog, object length ≥ `json.size`. On S3 this uses `HEAD(object).ContentLength`. Polled continuously and checked at quiescence. |
| `Always("list: object at uri holds at least the listed size")` | Object length ≥ the listed `size`, with the object read after the listing. |
| `Always("list: listed size falls on an event boundary")` | Plain storages only: `object[0:size]` parses into whole events. |
| `Always("binlog index lists every metadata object present")` | `binlog.index` lists every `*.json` present. |
| `Always("storage opens after a final clean run following regression faults")` | After a final clean pull/fetch, the storage opens. |
| `Sometimes("list taken while a writer was mid-checkpoint")` | Exploration. |

## SUT-side instrumentation suggestions

The codebase has no Antithesis SDK, so every SDK assertion here is missing.

| Assertion | Site | Status |
|---|---|---|
| `ALWAYS(new size >= previous saved size for the same binlog, "in-process: saved binlog metadata size never decreases")` | Inside `save_binlog_metadata` (`storage_core.cpp:907-925`). It needs the previous value. It is in-process only; cross-process regressions are visible only to the workload. | missing |
| `ALWAYS(actual object size >= metadata size, "querying_only: object not shorter than its metadata size")` | Optional. In `storage_core` `querying_only` construction, using the listing sizes already collected at `storage_core.cpp:180-216`. It would flag N1 from inside every `list` call. | missing |
| `SOMETIMES("S3 PutObject succeeded after at least one SDK retry")` | Optional. In the S3 backend PUT path. It needs an SDK retry hook or log scraping. Without it, nobody knows whether the replay window was ever reached. | missing |

Rejected: `ALWAYS(index written == in-memory index read-back prefix-compatible)` is not useful in-process. Cross-process index checks are left to the workload.

## Key observations

- **The property is mostly workload-side by nature.** Cross-process regressions (purge, a second writer, stale S3 PUTs) are visible only from outside.
- **Antithesis angle:**
  - It needs interleavings between a writer's append/metadata-save and another process's constructor (a purge or a second writer), plus concurrent listings. Those are exactly the cross-process timing windows Antithesis explores.
  - For S3, it needs the packet-hold partition mode on the PBS↔S3 link.
- **Consumer read length.** An object longer than `size` is acceptable by design, because data is written before metadata.
  - The README does not tell PITR tooling to read only `size` bytes. If tooling reads the whole object, a partial trailing transaction could be replayed.
  - That is a documentation gap, not something this property checks.

## Fault requirements

- **Workload-level concurrency:** a writer, plus purge or second-writer tasks, plus list tasks.
- **Process kills** widen the windows.
- **S3 variant:** network partitions with packet hold on PBS↔S3, so that an attempt is held, the SDK retries, and the held packets are delivered late.
- **Ticket 19 variant:** a page-cache loss fault.
- **Not needed:** clock faults or node termination.

## Open questions

- **Are a concurrent purge and a second writer in contract?** README:473 says purge alongside a writer "may cause data corruption and should be avoided". If out of contract, ticket 9, ticket 24 and N1 are workload-gated to a "misuse" mode and the property guards single-writer crash/fault sequences plus queries; if purge alongside a writer is in contract, the property fails today. `(needs human input)`
- **Does the CRT client retry a PutObject whose first attempt is held rather than lost, so that a stale attempt can land later?** `(partial: SDK 1.11.774 defaults abort an attempt after at least 3 s below 1 B/s and retry with the default CRT strategy, so a second attempt is expected; not runtime-verified for a held response)`
- **Does Versity commit a PUT whose body arrives in full after the client has given up on that connection?** If not, a held-then-flushed stream cannot complete, and only multipart Complete requests remain a risk. `(partial: Versity 1.8.0 posix publishes via temp file + link() only after the body copy succeeds; behaviour when the client's FIN/RST follows a complete held body not determined)`
- **Does CRT auto-multipart apply to PBS's whole-object re-uploads, and can a replayed `CompleteMultipartUpload` of an abandoned upload replace the object?** `(partial: SDK defaults give multipartUploadThreshold = max(partSize 8 MiB, 5 MiB), so bodies over 8 MiB go multipart if CRT knows the content length; PBS never sets ContentLength, and the replay question is unverified)`

## Answers from investigation (2026-10-13)

- **Racing observations (resolved).** Filesystem objects are replaced by `rename` of a fsynced `<name>.tmp` (`filesystem_storage_backend.cpp:141-157`), so a reader sees whole old or whole new content. The harness S3 stand-in is Versity 1.8.0 posix, whose `PutObjectWithPostFunc` stages the body in a temp file and publishes it with `link()` (versitygw `backend/posix/posix.go`), so a GET after a completed PUT returns the new whole object. Mid-run observations are safe on both backends under the harness, provided the workload reads the `.json` before the data object (data is written first, `storage_core.cpp:299-322`). No restriction to quiescent points is needed. If the harness switches to MinIO, re-check.
- **S3 timeouts and retries (partial).** `s3_storage_backend.cpp:189-255` sets only region, scheme, endpoint and virtual addressing on `Aws::S3Crt::ClientConfiguration`. SDK 1.11.774 defaults: `connectTimeoutMs = 1000`, `lowSpeedLimit = 1`, `requestTimeoutMs = 0`; `S3CrtClient::init` enables throughput monitoring with an interval floored at 3 s; CRT retry strategy defaults to the SDK's (`crtRetryStrategyType = NOT_SET`, `maxRetries = 0` meaning the aws-c-io default, back-off up to 20 s). So PBS does not hang forever on a held attempt; the attempt is abandoned and retried. **The stale-PUT variant stays in scope at P2 within this property**; its reachability depends on the Versity answer above, so keep `SOMETIMES("S3 PutObject succeeded after at least one SDK retry")` as the vacuity guard.
- **Multipart (partial).** `S3CrtClientConfiguration.h`: `partSize = 8 MiB`, `multipartUploadThreshold = 0` meaning max(`partSize`, 5 MiB). Binlog objects grow past 8 MiB in normal runs (a `max_binlog_size` above that), so whole-object re-uploads would use multipart if CRT can see the body length. PBS passes a stream body with no `SetContentLength` (`s3_storage_backend.cpp:369-385`). How the SDK sizes that stream for CRT was not traced.
- **Object longer than `size` (resolved, not a property question).** By design the data object can be longer than the `.json` size (data first, metadata second). The README `list` section documents `size` but does not tell PITR tooling to read only `size` bytes. That is a documentation gap for the owners, not something this property checks. Removed from the open list.

### Investigation Log

#### Are a concurrent purge and a second writer in contract?

- Examined: README :473, ticket 24 and ticket 9 in `.tickets-snapshot.txt`, `deployment-topology.md` Open Questions.
- Found: only purge + writer is mentioned, and as something to avoid. No maintainer statement on second writers.
- Conclusion: `(needs human input)`.

#### Should observations be allowed to race the writer?

- Examined: `filesystem_storage_backend.cpp:127-160`, `storage_core.cpp:299-322, 907-925`, `deployment-topology.md` (Versity 1.8.0 posix), versitygw v1.8.0 `backend/posix/posix.go` (`PutObjectWithPostFunc`).
- Found: filesystem replacement is tmp + fsync + rename; Versity publishes via temp file + `link()` after the copy.
- Conclusion: resolved: mid-run observations are allowed on both harness backends, reading `.json` before data.

#### Does AWS SDK C++ 1.11.774 S3 CRT apply a request timeout and retry an in-flight PutObject?

- Examined: `CMakeLists.txt:85`, `s3_storage_backend.cpp:189-255, 369-385`; upstream aws-sdk-cpp 1.11.774 `S3CrtClientConfiguration.h`, core `ClientConfiguration.h`, `S3CrtClient.cpp` (init).
- Found: defaults listed above; throughput monitoring is on by default (`lowSpeedLimit = 1`), interval at least 3 s; default CRT retries.
- Not found: runtime confirmation that a held response (body fully sent) trips the monitor; aws-c-http monitor semantics were not read.
- Conclusion: `(partial: ...)`. The variant is not unreachable on the timeout side.

#### Does the S3 server commit a PUT whose body arrives in full after the client has reset the connection?

- Examined: versitygw v1.8.0 `backend/posix/posix.go` (`PutObjectWithPostFunc`).
- Found: `io.CopyBuffer` into the temp file, then post-processing, then `link()`. A copy error returns before `link()`. A short body that ends with a clean EOF is not rejected in this function.
- Not found: how the HTTP layer (fiber) reports a body whose connection is reset after the last byte, or whether it reads the full body before invoking the handler.
- Conclusion: `(partial: ...)`. Needs a local experiment with `s3_fault_proxy.py` holding then flushing a PUT.

#### Does CRT auto-multipart apply above its part size (8 MiB)?

- Examined: `S3CrtClientConfiguration.h` (1.11.774), `S3CrtClient.cpp` init (`part_size`, `multipart_upload_threshold` copied into the CRT config), `s3_storage_backend.cpp:369-385`.
- Found: threshold defaults to max(8 MiB, 5 MiB); `PutObject` is issued as `AWS_S3_META_REQUEST_TYPE_PUT_OBJECT`.
- Not found: whether the SDK supplies a content length for PBS's un-sized stream body; replay behaviour of `CompleteMultipartUpload`.
- Conclusion: `(partial: ...)`.

#### Is an object longer than `size` acceptable for consumers?

- Examined: README list/search output sections, `storage_core.cpp:299-322`.
- Found: by design the object can exceed `size`; the README does not tell consumers to read only `size` bytes.
- Conclusion: resolved as a documentation gap outside this property.

## Evaluation refinements (2026-10-13)

Applied by the evaluation synthesis (`../evaluation/synthesis.md`). Where this section and earlier text in this file disagree, this section wins; the catalog entry has been updated to match.

- The stale-PUT replay variant relies on Antithesis holding packets on pbs↔s3 and releasing them late. Neither `s3_fault_proxy` nor any QA harness produces that shape, so it is unique to Antithesis and should not be the first variant cut. The `s3` template must enable packet-hold partitions on pbs↔s3; the proxy buffers whole request bodies, so a late PUT can still be forwarded (antithesis-fit F8, implementability passes).
- Runs in `fetch_fs`, `pull_fs`, `s3`.
