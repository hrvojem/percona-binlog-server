# other-process-startup-spares-live-writer — Another process opening the storage never damages a live writer (N1, N2)

Found by: focus 2 (concurrency), focus 8 (lifecycle)
Merged from: startup-tmp-cleanup-spares-live-writer (focus 2), purge-ctor-never-truncates-live-tail (focus 2), bind-failure-leaves-storage-untouched (focus 8)

## Synthesis notes

Safety, **P1**.

Shared mechanism: any storage constructor not in `querying_only` mode runs `remove_temporary_objects` (`storage_core.cpp:597-630`) and tail truncation to the metadata size (`:974-983`) without any fencing. That includes `purge_binlogs`, a second fetch, and a pull that will later fail to bind its port, because the bind happens after storage construction (`pull_operation.cpp:109` vs `:117`, `network_service.cpp:679-681`). `remove_temporary_objects` deletes the live writer's `.json.tmp` (N2). Tail truncation cuts bytes that were appended but are not yet in the metadata (N1).

Workload `Always`, with distinct messages:
- "live writer's tail stays a source prefix after a concurrent storage open"
- "live writer never fails with rename/ENOENT after a concurrent storage open"
- After the writer's graceful stop, the storage is recovery-free and reopens.

SUT anchors. Purge uses `null_logger`, so these anchors are the only visibility:
- `SOMETIMES(purging-mode ctor performed tail truncation)`
- `SOMETIMES(startup removed a .tmp object)`, with the construction mode in details
- `UNREACHABLE("tmp object vanished before rename")` on the ENOENT branch of `do_put_object`

Priority conflict: focus 2 rated the purge-truncation variant P0 and the `.tmp` variant P1. Focus 8 rated the bind-failure variant P2. Resolved to P1. README:473 already warns against running purge alongside fetch, and the owners have not said whether any concurrent opener is in contract. The property becomes P0 if they rule concurrent purge in contract. The filesystem backend is primary for the `.tmp` variant, because S3 temp files are UUID-named.

## What led here

- **N2 (`.tmp` cleanup; focus 2, from startup-tmp-cleanup-spares-live-writer).** This is the concurrency focus, cross-process. Lead N2 in sut-analysis, re-verified here. Every construction that is not a query deletes all `*.tmp` objects on the assumption that they are crash leftovers. On the filesystem backend, a live writer's in-flight `put_object` temp file has exactly that shape.
- **N1 (purge tail truncation; focus 2, from purge-ctor-never-truncates-live-tail).** This is the concurrency focus, purge+writer. Lead N1 came from sut-analysis and was verified by the synthesis, then re-traced here. The purge-mode constructor performs the same tail recovery as a writer's startup, and that recovery mutates the live tail.
- **Bind-failure ordering (focus 8, from bind-failure-leaves-storage-untouched).** This is the lifecycle focus (initialization ordering, restart overlap). There is no lock file, pid file or `flock` anywhere in `src/` (sut-analysis "Cross-process"). For pull, the only de-facto mutual exclusion between two instances on the same host and config is the replication-source TCP port, which the second instance cannot bind. But pull constructs the storage before it binds the port, and construction deletes every `*.tmp` and truncates the tail to its metadata size. So the one check that would detect a duplicate instance runs after the destructive steps. This is the ordering version of the cross-process hazards in `second-writer-on-storage-fails-closed` and the two N1/N2 variants above.

## Code paths

### Shared startup recovery (all non-querying_only constructors)

- `storage_core::storage_core` (`storage_core.cpp:126-221`):
  - `:149-150` `list_objects` + `remove_temporary_objects` → `backend_->remove_objects` (`:625`) for every `*.tmp`. This includes a live instance's in-flight `<name>.json.tmp` and `binlog.index.tmp`.
  - `:175-178` may rewrite `metadata.json` (encryption format upgrade).
- `storage_core::remove_temporary_objects` (`storage_core.cpp` ~:597-630) extracts every name with extension `.tmp`. It returns early only for `storage_construction_mode_type::querying_only`. Otherwise it calls `backend_->remove_objects`. So it runs for `streaming` (fetch, pull) **and `purging`** (purge_binlogs).
- `remove_objects` can fail on a vanished name, when the other process renamed it first. It then throws "file does not exist" in the second process's ctor. That outcome is fail-closed for the second process.
- `storage_core::load_and_validate_binlog_metadata_set` (`storage_core.cpp` ~:927-1005): for every mode except `querying_only`, it compares the metadata size with `object_names.at(file)`. That size was captured by `list_objects` (a stat at t0). On the tail, if metadata < actual, it calls `backend_->resize_object(tail, metadata.size)` (truncate + fsync) at `:979`. Metadata bigger than actual is fatal (~:974-977). The recovery warning "recovered binlog file ... by truncating" is at ~:980-983.

### Live writer side

- `write_event_block` (`storage_core.cpp` ~:299-322; data append to metadata save at `:307-321`) appends and flushes the data and does `size += n`, then PUTs the metadata. Between those two steps, the file is longer than its metadata.
- Filesystem `do_put_object` (`filesystem_storage_backend.cpp:127-160`) runs `write_file_content(tmp)`, `fsync(tmp)`, `rename(tmp, name)`.
  - If another process unlinks `tmp` between the write and the rename, `rename` fails with ENOENT → `runtime_error("cannot rename underlying tmp object file")`.
  - If the unlink happens while the write is in progress, the writer writes into an unlinked inode and the rename fails the same way.
- The exception propagates, and the buffer is not erased: `storage.cpp` `flush_event_buffer_internal` erases it only after success. `~storage` then flushes again (`storage.cpp:68-77`), appends the same bytes again, and saves size = old + 2n. Ticket 18's permanent-duplicate path is thus triggered purely by a concurrent opener.
- In `open_new_binlog_file_internal` (~:680-703), the rename failure can hit `<new>.json` or `binlog.index`. That leaves an unindexed binlog (ticket 15's wedge).

### Purge (N1)

- `purge_binlogs_operation.cpp:57` constructs storage in `purging` mode. `purge_binlogs_operation.cpp:56-57` passes a `null_logger`.
- Two interleavings truncate live data:
  - (a) Purge lists while the writer is between append and metadata PUT. Purge reads the old metadata and truncates.
  - (b) Purge lists after the writer's append and reads metadata before the PUT lands. Same result.
- On S3, `resize_object` downloads, truncates and re-uploads (`s3_storage_backend.cpp:765-808`). That races the writer's next whole-object PUT (last-writer-wins).
- After truncation on the filesystem, the writer's `ofstream` (`app`) keeps appending at the new EOF, but its in-memory `size` (old + n) still counts the cut bytes. Metadata says old + n + m while the file has old + m.
  - The next startup fails with "metadata bigger than actual" (fatal, ~:974-977). That is a wedge.
  - Meanwhile pull readers clamp to the in-memory size and read past EOF → `out_of_range` → the session drops (`util/file_operations_helpers.cpp:66-71`).

### Pull bind ordering (verified at 99de682)

- `pull_operation.cpp:109-110` constructs the storage (streaming mode), running all of the recovery above.
- `pull_operation.cpp:117-118` constructs `minimysql::network_service`. In `network_service.cpp:679-681`, `acceptor_type(context, endpoint{v4, port})` opens the socket, sets `SO_REUSEADDR`, binds and listens. If another process holds the port in LISTEN, bind fails with `EADDRINUSE` and throws. The exception reaches `handle_std_exception`, and the process exits non-zero after `~storage` runs (nothing is buffered, so it writes nothing).

## Failure scenarios

1. **N2: purge deletes the live `.json.tmp` (filesystem).** A pull with `checkpoint_size=1M` saves `binlog.000007.json` many times per second. An operator runs `purge_binlogs binlog.000003`, which is routine. Purge's ctor lists `binlog.000007.json.tmp` and deletes it. Pull's rename fails, and pull exits with an error. `~storage` re-flushes and duplicates the last checkpoint's transactions in 000007. On restart, the metadata (old + 2n) equals the actual file size, so recovery does not notice. The storage is now permanently not a byte prefix of the source.
2. **N1: purge truncates the live tail.** Purge's ctor sees data > metadata on the tail (interleaving a or b) and truncates. The writer keeps appending past the cut. Metadata now exceeds the file size, so the next startup wedges with "metadata bigger than actual" and pull readers get `out_of_range`. On S3, the resize races the writer's whole-object PUT instead.
3. **Bind failure after destructive recovery (second pull, same host and port).**
   1. Pull A is running and streaming. Alternatively, A is in its final flush after SIGTERM: a supervisor restart did not wait for exit, or A stalled and the operator believes it dead.
   2. Pull B starts with the same config. B's constructor deletes A's `<tail>.json.tmp` (A's next rename fails → the ticket 18 duplicate path, scenario 1). Or it truncates A's tail between A's append and metadata save (the N1 path, scenario 2).
   3. B then fails with "address already in use" and exits. The operator sees a harmless startup failure, but A continues on a damaged storage.

   The window for step 2 is small per event but recurs on every checkpoint. A restart loop (B crash-looping under a supervisor) hits it repeatedly.

## Oracle / workload checks

- Workload triggers:
  - Run `purge_binlogs` at random times against a live fetch/pull with a small `checkpoint_size`, plus a heavy write workload so the append/metadata window is often open.
  - Run `list` the same way as a negative control: it must never delete or truncate.
  - Repeatedly launch pull B with the identical config (same port) while pull A runs with frequent checkpoints. B is expected to exit non-zero.
- `Always("live writer's tail stays a source prefix after a concurrent storage open")`. Evaluate it after the writer exits with any code. It subsumes the contributors' "writer storage is a source prefix after concurrent purge" and "tail is a source prefix after purge". Also check that the tail's metadata size is ≤ its file size.
- `Always("live writer never fails with rename/ENOENT after a concurrent storage open")`. This covers the bind-failure clause "A's subsequent behaviour is unaffected".
- After the writer is stopped gracefully: the storage is recovery-free (`graceful-stop-leaves-recovery-free-storage`, now merged into graceful-exit-leaves-flushed-storage), is a byte prefix of the source (SG2), and reopens.
- The workload knows whether a writer was live when it launched purge or B. Use that flag together with the byte oracle, because the condition cannot be checked in-process.
- A simpler direct check for the filesystem backend (bind variant): snapshot the storage directory listing (names + sizes + mtimes, excluding A's ongoing appends) right before B starts and right after B exits. Then assert `Always("no pre-existing object removed or shrunk by a process that failed startup")`. Attribution is tricky while A is live, so the end-to-end oracle above is the robust one.

## SUT-side instrumentation

The codebase has no Antithesis SDK, so every SDK assertion below is **missing**.

| Assertion | Site | Proposed by | Status |
|---|---|---|---|
| `SOMETIMES(construction_mode == purging && tail truncated, "purging-mode ctor performed tail truncation")` | recovery branch, `storage_core.cpp` ~:979 | N1 | missing |
| `SOMETIMES(true, "startup removed a .tmp object")`, mode in details | `remove_temporary_objects` ~:597-630 | N2, bind | missing |
| `SOMETIMES(true, "startup recovery modified storage")`, mode in details | before `resize_object` (~:979), concurrent with a writer | bind | missing |
| `SOMETIMES(removed a .tmp whose mtime is younger than N s, "startup removed a recently written .tmp object")` | `remove_temporary_objects` | N2 | missing |
| `UNREACHABLE("tmp object vanished before rename")` | ENOENT branch of `do_put_object`, `filesystem_storage_backend.cpp:127-160` | N2 | missing |
| `UNREACHABLE("startup recovery ran in a process that later failed to bind")` | `pull_operation` after network_service construction fails | bind | missing; checkable only after a fix (bind first, or a lock object), or with a flag passed from the storage ctor to `pull_operation` |

`ALWAYS(construction_mode != purging || no other writer is live, ...)` cannot be checked from inside the process, so it is replaced by the workload-side flag plus the byte oracle. Purge constructs storage with a `null_logger` (`purge_binlogs_operation.cpp:56-57`). The "recovered binlog file ... by truncating" warning (`storage_core.cpp` ~:980-983) and the `.tmp`-removal warnings are therefore **silently dropped**, and the truncation is invisible in purge's JSON output. That makes SUT-side anchors more valuable than usual.

## Key observations

- The S3 backend is not affected by N2 (local UUID temp files). It is affected by N1 through the `resize_object` re-upload race.
- If B's config differs (it adds encryption), B also rewrites `metadata.json` (`:175-178`) before failing.

## Fault requirements

- Process pause of the writer, to stretch its append→metadata window.
- A second PBS container, or an extra process in the writer's container, sharing the storage volume.
- No node termination needed.

## Open questions

- **Is any concurrent opener in contract?** README:473 says purge-while-writing "should be avoided" and says nothing about a second pull or fetch. Out of contract: require only fail-closed (writer exits non-zero, no duplicated bytes; today that fails too, because of the dtor re-flush), and for the bind variant "B refuses before touching storage". In contract: `.tmp` cleanup needs ownership (unique names or a lock). Decides P1 vs P0. `(needs human input)`
- **Should purge perform recovery at all?** Purge could open the tail like `querying_only`. The answer decides whether N1 is a bug or documented misuse (README:473). `(needs human input)`
- **Encryption upgrade order:** is the `metadata.json` encryption stamp at `storage_core.cpp:170-178` meant to run before the binlog index and records are validated? If B's config adds encryption, B rewrites `metadata.json` before it fails. `(needs human input)`

## Answers from investigation (2026-10-13)

- **`.tmp` variant is filesystem-only (resolved).** Only `filesystem_storage_backend::do_put_object` creates `<name>.tmp` objects (`filesystem_storage_backend.cpp:141-157`, suffix from `basic_storage_backend_fwd.hpp:60`). The S3 backend never creates `.tmp` objects in the bucket; its local temp files are named `tmp_file_directory_ / <uuid>` (`s3_storage_backend.cpp:929-931`), and the default buffer directory is itself a per-process UUID subdirectory (`:564-571`). So N2 (and `UNREACHABLE("tmp object vanished before rename")`) apply to the filesystem backend only. N1 (tail truncation) applies to both backends. **Workload backend matrix:** run the `.tmp` variant on `file` only; run the purge-truncation variant on both.
- **Topology (resolved for the harness).** `deployment-topology.md` runs all multi-process storage properties as subprocesses in the `pbs` container on one filesystem storage. A second pull launched with the same config therefore shares the host and port, so the bind variant applies. A variant that gives B a different `replication_source.port` turns B into a full second writer, which is `second-writer-on-storage-fails-closed`'s case. Whether production deployments ever share a host and port is not something the harness needs.
- **Purge recovery intent (partial).** The commit that added purge (`0ad42fc`, "PS-11001: Implement logic for purge_binlogs mode") says nothing about tail recovery; the recovery comment at `storage_core.cpp:961-966` assumes the mismatch comes from a process that "was not properly shut down", i.e. no live writer. Recovery runs for purge only because it is gated on `construction_mode_ != querying_only` (`:956`). No code or doc states that purge needs recovery.
- **Encryption upgrade order (partial).** The stamp was added in `3e812f2` (PBS-39 part 8). Its commit message says it targets the case "we start PBS with encryption enabled" on a non-empty storage. It does not discuss ordering or non-streaming modes. The fresh-storage `save_metadata()` in the same constructor *is* gated on `streaming` (`storage_core.cpp:154-156`, already gated in `3e812f2`), while the upgrade is not. That suggests an oversight rather than intent, but only the owners can say.

### Investigation Log

#### Does purge_binlogs log its recovery actions somewhere the workload can read?

- Examined: `src/operations/purge_binlogs_operation.cpp:45-60`, `list_operation.cpp:50`, `search_by_gtid_set_operation.cpp:56`.
- Found: all three construct storage with `std::make_shared<binsrv::null_logger>()`. Purge's tail truncation and `.tmp` deletion produce no log output and are not reflected in the JSON response.
- Conclusion: resolved. Detection must use the byte oracle or a new SUT-side assertion.

#### Is any concurrent opener in contract?

- Examined: README :473, ticket 24 (`.tickets-snapshot.txt:1292-1345`), `deployment-topology.md` Open Questions.
- Found: only purge + writer is mentioned, as something that "may cause data corruption and should be avoided". Ticket 24's expected behaviour is a QA proposal, not a maintainer statement.
- Conclusion: `(needs human input)`.

#### Should purge perform recovery at all?

- Examined: `purge_binlogs_operation.cpp:45-60`, `storage_core.cpp:927-1005` (recovery gated on `!= querying_only`), commit `0ad42fc` message, commit `93baca0`.
- Found: no comment or commit text says purge needs tail recovery. The recovery comment assumes an improperly shut-down process.
- Not found: maintainer intent.
- Conclusion: `(needs human input)`.

#### Should the `.tmp` variant be filesystem-only?

- Examined: `filesystem_storage_backend.cpp:127-160`, `s3_storage_backend.cpp:556-590, 826-832, 929-938`, `basic_storage_backend_fwd.hpp:56-60`.
- Found: only the filesystem backend writes `<name>.tmp`; S3 temp files are local, UUID-named, in a per-process directory by default.
- Conclusion: resolved: yes, filesystem-only. (Two S3 processes sharing an explicit `fs_buffer_directory` still get distinct UUID file names.)

#### Topology: can two pulls share a host and port, or only a storage?

- Examined: `deployment-topology.md` ("Why the multi-process storage properties run inside `pbs`"), `pull_operation.cpp:109-118`, `network_service.cpp:672-681`.
- Found: the harness launches the extra processes in the same `pbs` container with the same config, so they share the port.
- Conclusion: resolved for the harness. The bind variant applies; a different-port variant belongs to `second-writer-on-storage-fails-closed`.

#### Encryption upgrade order: is the metadata.json stamp meant to run before the rest of the storage is validated?

- Examined: `storage_core.cpp:126-221`; `git log -S` for the stamp (commits `3e812f2`, `7f8cb69`) and their messages.
- Found: the stamp predates the `storage_core` split and was introduced for a writer start with encryption enabled. The comparable fresh-storage write is mode-gated; this one is not, and it runs before `load_binlog_index` / `load_and_validate_binlog_metadata_set`.
- Not found: any statement about ordering or about non-writer modes.
- Conclusion: `(needs human input)`.

## Evaluation refinements (2026-10-13)

Applied by the evaluation synthesis (`../evaluation/synthesis.md`). Where this section and earlier text in this file disagree, this section wins; the catalog entry has been updated to match.

- Runs only in `contention`; its SUT anchors and `UNREACHABLE` carry the harness timeline tag in the message. Antithesis tallies an assertion by its message across every timeline of a run, so an SUT message cannot be scoped to a template (wildcard W1).
- Whether concurrent openers are in contract is bias B2.
