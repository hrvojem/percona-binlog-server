# stale-index-commit-never-wedges-storage — A stale in-memory index is never committed over a newer one (ticket 9)

Found by: focus 2 (concurrency; purge+writer and purge+purge leads)
Merged from: purge-during-writer-keeps-storage-openable (focus 2), concurrent-purges-keep-index-consistent (focus 2)

## Synthesis notes

Safety, **P1**. Both contributors rated it P1. The ticket 9 variant (purge + writer) is out of the documented contract (README:473; see "Investigation results"). It becomes P0 only if the maintainers bring it into contract. Until then the P1 rests on the concurrent-purges variant.

Shared mechanism: a process loads `binlog.index` at construction and later rewrites it from its stale in-memory list. That happens in two places:

- the writer at its next rotation (`storage_core.cpp:701`, `789-799`) after a concurrent purge;
- the later of two concurrent purges (`:368-433`).

Either way, the rewrite resurrects deleted entries or orphans objects.

Workload `Always`, with distinct messages:

- "storage opens in streaming mode after purge overlapped a writer"
- "storage opens after concurrent purges"
- "every index entry references an existing object"
- "index after concurrent purges is a suffix of the original"

Coverage anchors:

- `Sometimes(writer rotated after a successful concurrent purge)`
- `Sometimes(two overlapping purges both reported success or warning)`

## What led here

- **Ticket 9: purge + writer** (from purge-during-writer-keeps-storage-openable). Concurrency focus, stale shared state across processes. Ticket 9 is open and was validated in sut-analysis by its discriminating detail: the index is loaded only in the ctor (`storage_core.cpp:214`) and rewritten from the stale in-memory list. Re-traced at 99de682.
- **Purge + purge** (from concurrent-purges-keep-index-consistent). No ticket. This is a new lead from reading the purge commit protocol with two processes in mind. README:473 warns only about purge alongside fetch/pull. It says nothing about two purges: two cron jobs, an operator plus automation, or a purge retried after a client-side timeout.

## Code paths

### Index lifecycle

- Each process constructs storage and runs the following steps (`storage_core.cpp` ctor ~:120-220, ~:597-630, ~:721-787, ~:927-1005):
  1. `list_objects` (t0)
  2. `remove_temporary_objects`
  3. `load_binlog_index`
  4. `validate_binlog_index`
  5. metadata load and recovery
- Purge processes construct it in `purging` mode.
- The writer loads `binlog_records_` once in its ctor (`load_binlog_index`, `storage_core.cpp` ~:721-762; the index load is at `:214`). It never re-reads it.
- `purge_binlogs` (~:368-433) works in three steps:
  1. Drop the prefix from the **in-memory** list.
  2. `save_binlog_index()`, a whole-object rewrite from memory.
  3. Best-effort `remove_objects` on the victims. Failures are swallowed into a `warning` message.
- `validate_binlog_index` (~:772-776) rejects an index that references a missing object with "binlog index contains a reference to a non-existing object".

### Variant 1: writer commits a stale index after a purge (ticket 9)

1. Purge (another process) rewrites `binlog.index` to the surviving suffix, then deletes the victim `.json` and data objects.
2. At its next rotation, the writer appends to its stale list and calls `save_binlog_index()` (~:789-799, called from `open_new_binlog_file_internal` ~:701). That resurrects the purged names, whose files are gone.
3. Every later construction except `querying_only` fails `validate_binlog_index`. Purge itself cannot repair it either.
4. In pull, `fetch_event_block` still resolves purged names from memory and `get_object` throws. A client asking for the oldest binlog (empty name) gets a dropped session.

Some interleavings fail closed instead. Purge's ctor running mid-rotation sees an index/object mismatch and errors out. Purge's ctor can also hit the N1 and N2 hazards (see other-process-startup-spares-live-writer).

### Variant 2: concurrent purges

- **Interleaving A** (purge P1 targets 000003, purge P2 targets 000001, storage [000001..000005]):
  1. Both ctors complete before either writes the index.
  2. P1 writes index [000004, 000005] and deletes 000001..000003.
  3. P2 writes index [000002..000005] from its stale list, which resurrects 000002 and 000003. Its delete of 000001 fails, so P2 reports `warning`.
  4. Result: the index references deleted objects, and every non-query mode fails to open.

  This was traced in code but not reproduced.
- **Interleaving B**: P2's ctor deletes P1's `binlog.index.tmp` (filesystem). P1's rename fails and P1 exits `error` before any delete. That is fail-closed and fine.
- **Interleaving C**: P2's ctor lists while P1 is deleting. P2's `remove_objects` or `validate_binlog_index` sees a mismatch and P2 reports `error`. Also fine.
- Purge is short, so interleaving A needs both ctors to finish before the first index write. Antithesis thread/process pausing makes that reachable.

## Property

The storage must stay consistent after either of these, once all processes have stopped:

- any interleaving of `purge_binlogs` with a running fetch/pull on the same storage;
- any set of concurrent `purge_binlogs` runs.

"Consistent" means three things:

- a fresh `fetch` can open the storage (concurrent purges: it opens in every mode);
- `binlog.index` references only objects whose data and `.json` both exist;
- for purge-only interleavings, the index is a suffix of the pre-purge index.

## Oracle / workload checks

- Workload, variant 1: run purge (target = a random non-tail binlog) at random times against a live fetch/pull, plus source `FLUSH BINARY LOGS` so that rotations follow purges. After the writer stops:
  - `Always("storage opens in streaming mode after purge overlapped a writer")`. The contributor's message was "storage openable after purge concurrent with writer".
  - `Always("every index entry references an existing object")`. This is a cheap direct check on the directory or bucket, and it applies to both variants.
- Workload, variant 2: issue 2-3 purge commands concurrently with random targets on a quiescent storage, or alongside only `list`. Afterwards:
  - `Always("storage opens after concurrent purges")`, in both purging and streaming mode. The contributor's message was "concurrent purges leave storage openable".
  - `Always("index after concurrent purges is a suffix of the original")`, alongside the entries-exist check.
- Coverage:
  - `Sometimes("writer rotated after a successful concurrent purge")` ensures the ticket 9 trigger was hit.
  - `Sometimes("two overlapping purges both reported success or warning")` shows that a real overlap happened.

## SUT-side instrumentation

No contributor proposed an SUT-side assertion for this property. The codebase has no Antithesis SDK, so none exists. The checks above are workload-side.

## Fault requirements

- Thread/process pausing, to let both purge ctors complete before the first index write and to stretch the writer's time between purge and rotation.
- Source `FLUSH BINARY LOGS` driven by the workload.
- No node termination needed.

## Open questions

- **Will the maintainers bring purge alongside a writer into contract, or make it refuse (ticket 9 fix)?** `(needs human input)` Today it is out of the documented contract (README:473). If they bring it in, variant 1 becomes a P0 safety property. If they add a refusal, it becomes "purge refuses while a writer is live". This is the same decision as for `second-writer-on-storage-fails-closed`.
- **Are concurrent purges in contract?** `(needs human input)` README:473 covers only purge alongside fetch/pull and is silent on two purges. If they are in contract, variant 2 is a P1 safety bug candidate. If not, the README needs to say so, and the property becomes "at most one purge succeeds, or the result is still consistent".
- **Does interleaving A report `warning` or `success`?** `(partial: filesystem reports warning, because do_remove_object throws on ENOENT; on S3 it reports success if the gateway answers DeleteObject on a missing key with 204, as AWS does; not verified for Versity)`
- **What should pull return for a purged binlog?** `(needs human input)` Today the session is closed without an error packet. This is a separate user-visible behaviour (LG4/N12) and does not affect this property's oracle.

## Investigation results (2026-10-13)

- **Variant 1 (purge + writer) is out of the documented contract.**
  - README:473 (maintainer, 91c4f80, 2026-06-05, before the ticket snapshot base ee82afa) says purge "while another instance is fetching / pulling data to the same storage may cause data corruption and should be avoided".
  - Ticket 9's statement that the README "only forbids purging the binlog currently being written" is wrong. The mechanism of ticket 9 is still real: the index is loaded once at `storage_core.cpp:214` and rewritten from memory at `:701` → `:789-799`.
  - **Property change:** variant 1 is no longer an in-contract safety claim.
    - The workload should either skip it, or keep it as a ticket-9 regression target with its own message that is expected to fail.
    - The P0 upgrade path applies only if the maintainers bring it into contract.
    - The P1 priority now rests on variant 2.
- **Interleaving A outcome by backend.**
  - On the filesystem, P2's delete of the already-removed `000001.json` and `000001` throws "file does not exist" (`filesystem_storage_backend.cpp:174-189`). `remove_objects` rethrows it (`basic_storage_backend.cpp:58-77`), and P2 reports `warning`. That is detectable, but the storage is still wedged.
  - On S3, `delete_object` raises only on an unsuccessful outcome (`s3_storage_backend.cpp:399-412`). AWS semantics return success for a missing key, so P2 would report `success` with a wedged storage, which is worse.
  - **Implication:** the `Sometimes("two overlapping purges both reported success or warning")` anchor is reachable on both backends. On S3, the silent `success` makes the post-check `Always("every index entry references an existing object")` the only detector.
- **What pull does for a purged binlog.**
  - `fetch_event_block` resolves the stale name from memory, then `backend_->get_object` throws (`storage_core.cpp:541-542`).
  - The exception leaves `sender_context::get_event` and `handle_binlog_dump_command` (`network_service.cpp:279-334`).
  - The session coroutine catches it (`network_service.cpp:621-623`) and closes the socket with no error packet. When `get_event` returns false instead, `handle_binlog_dump_command` just `co_return`s (`:307-312`).

### Investigation Log

#### Is purge alongside a writer in contract?

- Examined: README:473 plus git blame and `git show ee82afa:README.md`, ticket 9 "Impact".
- Found: the maintainer README text explicitly discourages it, and it was already in the code base that the ticket was written against.
- Not found: any plan to support it.
- Conclusion: resolved as out of the documented contract. Whether that will change is tagged `(needs human input)` above.

#### Are concurrent purges in contract?

- Examined: README purge section and README:473, `purge_binlogs_operation.cpp`, `storage_core.cpp:368-433`.
- Found: no lock, and no statement either way.
- Conclusion: `(needs human input)`.

#### Does any purge report `success` in interleaving A, or always `warning`?

- Examined: `basic_storage_backend.cpp:58-77`, `filesystem_storage_backend.cpp:174-189`, `s3_storage_backend.cpp:399-412`, `storage_core.cpp:419-430`.
- Found: the filesystem path gives `warning`. The S3 path gives `success` if the server returns 2xx for a missing key.
- Not found: Versity's DeleteObject behaviour for a missing key (not in the repo).
- Conclusion: `(partial: …)`.

#### What should pull return for a purged binlog?

- Examined: `network_service.cpp:279-334`, `sender_context.cpp:484`, `storage_core.cpp:436-564`.
- Found: there is no error packet on either the throw path or the false path.
- Not found: the intended client-visible behaviour.
- Conclusion: `(needs human input)`. It is out of scope for this property's oracle.

## Evaluation refinements (2026-10-13)

Applied by the evaluation synthesis (`../evaluation/synthesis.md`). Where this section and earlier text in this file disagree, this section wins; the catalog entry has been updated to match.

- `Sometimes(two overlapping purges both reported success or warning)` has no running scenario today: `fetch_fs` serialises purge under the writer lock and `contention` races purges against writers. The anchor needs a `contention` step running two purges with no writer; until the topology adds it the anchor is deferred, so it does not report as an unmet `Sometimes` (antithesis-fit F4).
- Runs in `contention`. Whether purge beside a writer is in contract is bias B2.
