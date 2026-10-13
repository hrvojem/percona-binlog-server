# cli-json-envelope-and-exit-code-agree — list/search/purge always answer with one JSON envelope whose status matches the exit code, never crash, and never error spuriously beside a writer

Found by: focus 4 (protocol), focus 2 (concurrency).
Merged from: cli-json-envelope-and-exit-code-agree (focus 4), query-concurrent-with-writer-never-errors (focus 2).

## Synthesis notes

Safety, P1. Focus 2 rated it P1 and focus 4 rated it P2; it lands at P1 because querying beside a writer is an explicit design intent (storage_core.cpp:189-195).

Workload messages:
- `Always`: "cli: stdout is exactly one JSON response"
- `Always`: "cli: exit code agrees with JSON status"
- `Always`: "query beside an active writer never reports error for a concurrency-only reason"
- `Unreachable`: "cli: query or purge died by a signal"

SUT-side:
- `ALWAYS(!binlog_records_.empty())` before `front()` at storage_core.cpp:1001. The ctor assert at :219 already covers this partially.
- A top-level `catch (...)` with an `UNREACHABLE`.
- `SOMETIMES(querying ctor dropped a record)`.

Triggers:
- N4: `.json.tmp` or `binlog.index.tmp` renamed between readdir and the per-entry stat in do_list_objects (filesystem_storage_backend.cpp:103-117).
- A rotation plus a purge between the listing (t0) and the index read (t1), which drops every record.
- An empty index after an interrupted rotation (ticket 13).
- Every metadata load failing under S3 faults.

## What led here

- **Protocol contract (focus 4).** This is about the machine-readable output of the non-streaming modes. README documents a response envelope: `{"status": "success"|"error"|"warning", "result": [...]}` or `{"status": "error", "message": "<reason>"}`. Every operation implements it the same way. It builds the storage and the result inside `try`, catches `std::exception` into `error_response`, prints one line, and returns `operation_successful` (`list_operation.cpp:42-65`, `search_by_timestamp_operation.cpp:46-79`, `search_by_gtid_set_operation.cpp:46-76`, `purge_binlogs_operation.cpp:46-87`). `app.cpp:50` then maps the result to `EXIT_SUCCESS`/`EXIT_FAILURE`. Consumers such as scripts and PITR tooling depend on four things:
  - exactly one parseable JSON object on stdout;
  - `status` in the documented set;
  - exit code 0 iff `success` or `warning`;
  - `message` present on `error`.
- **Concurrency, list/search beside a writer (focus 2).** `querying_only` construction is explicitly designed to tolerate a concurrent writer:
  - the code comment at `storage_core.cpp` ~:189-195 says "the concurrent query-only operation may intrude exactly between these two steps";
  - it skips index/object reconciliation (~:766-770);
  - it drops records whose metadata is unreadable (~:942-950);
  - it skips size checks and `.tmp` deletion.

  Lead N4 (sut-analysis) identifies a race that this tolerance logic does not cover. A second race, which can crash, was found while re-reading the code.

## Code paths and ways the envelope can break

1. **Listing stat race (N4).** `filesystem_storage_backend::do_list_objects` (`filesystem_storage_backend.cpp:103-117`) iterates the directory and calls `dir_entry.is_regular_file()` and `dir_entry.file_size()` per entry. With libstdc++, `file_size()` does a fresh `stat`; only the file type is cached from `d_type`. A writer's `<x>.json.tmp` or `binlog.index.tmp` can exist at `readdir` but be renamed away before `stat`. `file_size()` then throws `filesystem_error` (ENOENT), and `list`/`search_*` print `status: error` and exit non-zero. The writer renames these temp files on every checkpoint and every rotation. The envelope stays well-formed, but the error is spurious.
2. **Empty-records crash, UB that bypasses the catch.** `binlog_records_.front()` can run on an empty vector in the constructor: the `storage_core.cpp:219` assert in debug builds (the `assert(!binlog_records_.front()...)` after metadata load), and the `:1001` read of `front().added_gtids`. In `querying_only` mode, `load_and_validate_binlog_metadata_set` erases every record whose `.json` was not in the t0 listing or failed to load (~:933-950). The result is a debug assert, or a garbage read/segfault in release. This is ticket 13's shape (empty index → segfault, validated), reached here through concurrency instead of damage. Ways to reach it:
   - (a) The index read at t1 lists only binlogs whose `.json` did not exist at the t0 listing. This needs a writer rotation plus a concurrent purge of the older binlogs between t0 and t1.
   - (b) `readdir` misses the tail `.json` while it is being replaced by rename, on a single-binlog storage. Filesystem-dependent; see query-success-never-omits-middle-binlog.
   - (c) Every metadata load fails under S3 faults.
   - (d) Purging or querying mode on a storage whose index is empty (ticket 13). Corrected 2026-10-13: only **external damage** produces this. An interrupted rotation cannot:
     - the first index is written after a record is added (`storage_core.cpp:695-701`);
     - `put_object` is atomic on both backends;
     - purge never empties the index (tail refusal, `:356-361`).

     An interrupted *first* rotation leaves no index at all, and that case is handled (`:187-198`).
3. **Other UB and abort paths:**
   - debug asserts on other storage-open paths;
   - `std::terminate` from an exception escaping a `noexcept` function (the ticket 1 pattern, `format_description_post_header_impl.cpp:130,166-169`; not on the query path today).
4. **Non-`std::exception` throws** are not caught, because only `catch (const std::exception &)` is in place. Any escape with no `catch (...)` terminates without printing JSON.
5. **Same ctor, should not fail.** `load_metadata` and `load_binlog_index` are whole-object reads of rename-atomic objects. They should not fail on the filesystem or on strongly consistent S3.
6. **No break.** The S3 client constructor's IMDS wait (ticket 16) only delays the response.
7. **Exit code vs status.** `warning` returns `true`, so the exit code is 0 (`purge_binlogs_operation.cpp:79-80`). Whether 0 is intended for `warning` is a contract question.
8. **Index-less storage.** The `querying_only` "no index yet" path returns an empty storage (`storage_core.cpp:189-195`), so the envelope holds there.

## Failure scenarios

- **N4 spurious error (focus 2).** `list` runs in a tight loop beside pull with a small `checkpoint_size`. A `.json.tmp` is renamed between readdir and stat. `list` prints `status: error` and exits 1, though nothing is wrong with the storage.
- **Empty-records crash under rotation+purge (focus 2, trigger a).** A query lists objects at t0. The writer rotates, and a purge removes every older binlog. The index at t1 names only binlogs whose `.json` was absent at t0. Every record is erased, `front()` is UB, and the process dies by signal with no JSON.
- **Crash on a damaged storage (focus 4, trigger d / c).** Either the workload damages the storage by emptying `binlog.index` (ticket 13; kills alone cannot produce it), or every metadata GET fails under S3 faults. `list`/`search`/`purge` crash instead of printing an `error` envelope.

## Oracle / workload checks

The workload runs each CLI mode (list, search_by_timestamp, search_by_gtid_set, purge) many times, with valid arguments, against storages in every state Antithesis produces: writer live, writer killed mid-rotation, purge interrupted, S3 faults. It also runs tight loops of `list`/`search_*` against pull/fetch with a small `checkpoint_size`, which causes many `.json.tmp` renames.

| Workload assertion | Type | Notes |
|---|---|---|
| "cli: stdout is exactly one JSON response" | `Always` | One parseable JSON object. |
| "cli: exit code agrees with JSON status" | `Always` | `status ∈ {success, warning, error}` and `(exit==0) == (status != error)`. |
| "cli: error status carries a non-empty message" | `Always` | `status == error ⇒ message` present and non-empty. |
| "query beside an active writer never reports error for a concurrency-only reason" | `Always` | Exit 0, or a legitimate "cannot be covered" / "no binlogs" message. |
| "cli: query or purge died by a signal" | `Unreachable` | SIGABRT/SIGSEGV for list/search/purge. Merges focus 4's "cli: query or purge crashed" and focus 2's "query never crashes concurrent with writer/purge". |

## SUT-side instrumentation

| Assertion | Site | Status |
|---|---|---|
| `ALWAYS(!binlog_records_.empty(), "storage ctor: binlog records non-empty before front()")` | before `storage_core.cpp:1001` | missing |
| same check at the ctor | `storage_core.cpp:219` | partial (an existing C `assert`, debug only) |
| top-level `catch (...)` in each `execute()` with `UNREACHABLE("cli: non-std exception escaped operation")`, so the workload sees a signal instead of silence | `list_operation.cpp`, `search_by_timestamp_operation.cpp`, `search_by_gtid_set_operation.cpp`, `purge_binlogs_operation.cpp` | missing |
| `SOMETIMES(querying ctor dropped a record, "querying ctor dropped a record due to missing or unreadable metadata")` | `querying_only` erase branch, `storage_core.cpp` ~:933-950 | missing |

query-success-never-omits-middle-binlog places an `UNREACHABLE` for non-edge drops and a `SOMETIMES` for tail drops at the same erase branch. The `SOMETIMES` above is the generic anchor; do not add a second generic one there.

Focus 2 proposed the message "querying ctor never ends with empty record list when index was non-empty". The message above is broader on purpose: it also covers the empty-index purging path from ticket 13.

## Key observations / Antithesis angle

- The crash paths need unusual storage states: all metadata unreadable, or an empty index after an interrupted rotation or external damage. Kills and backend faults at specific moments produce them.
- The envelope check is a cheap, always-on oracle over every CLI call the workload makes.
- N4 is a frequent, benign-looking race: every checkpoint and every rotation renames a temp file.

## Fault requirements

- Process kills of writers, to create intermediate storage states.
- Network faults on the S3 path.
- Optionally `fsfault.c`.
- Workload-driven writer (small `checkpoint_size`) and purge running beside queries.
- No clock faults or node termination are needed.

## Open questions

- **Should `warning` exit 0?** `(needs human input)` The code does: `operation_successful = true` after a warning (`purge_binlogs_operation.cpp:71-80`, `app.cpp:50`). The maintainer comment (`:62-69`) treats `warning` as "storage will need attention". README calls it "partial success" and documents no exit codes. A script that checks only the exit code will miss leftovers that wedge the storage (see `interrupted-purge-leaves-openable-storage`). If maintainers want a distinct exit code, the oracle's exit-code rule changes.

## Investigation results (2026-10-13)

- **`list` on an empty or index-less storage prints `success` with an empty result.**
  - The querying constructor returns early on an empty storage, a metadata-only storage, or a missing index (`storage_core.cpp:152-158, 183-185, 193-195`).
  - `list` then emits a default `search_response`, whose status is `success` (`list_operation.cpp:50-56`, `models/search_response.cpp:33-35`).
  - The README does not describe this case for `list`. The earlier "README says yes" was wrong.
  - Unlike `list`, `search_*` return `error` "Binlog storage is empty".
  - The envelope holds either way. The content belongs to `query-success-never-omits-middle-binlog`.
- **The N4 stat race exists on the target toolchain.**
  - The topology builds with `debug_clang20` and libc++.
  - libc++'s POSIX `directory_iterator` creates entries via `directory_entry::__create_iter_result(file_type)`, which caches only the type. `directory_entry::file_size()` → `__get_size()` re-stats for the `_Empty`, `_IterNonSymlink` and `_IterSymlink` cache states (`__filesystem/directory_entry.h:238-250, 385-392`, Xcode libc++ copy).
  - So a `.tmp` renamed between `readdir` and `file_size()` throws ENOENT out of `do_list_objects` (`filesystem_storage_backend.cpp:103-117`).
  - libstdc++ (gcc14 presets) was not re-checked here. It is not the deployed toolchain.
- **Trigger (a) is out of contract.** It needs a purge alongside a writer, which README:473 tells operators to avoid. The concurrency crash path therefore rests on (b). The other crash triggers are (c), S3 faults on every metadata GET, and (d), workload-applied external damage only (see the correction under "Code paths").
  - **Property scope change:** drop trigger (a) from the workload. A workload step that empties `binlog.index` is needed if (d) is to be exercised at all.
- **Priority stays P1.** It rests on the N4 spurious-error race, which is in contract (query beside a writer is an explicit design intent, `storage_core.cpp:189-195`), and on the envelope/exit-code check.

### Investigation Log

#### Should `warning` exit 0?

- Examined: `purge_binlogs_operation.cpp:45-87` and git blame (maintainer), `app.cpp:25-51`, README purge section.
- Found: the code deliberately returns success for `warning`. The README does not mention exit codes for any mode except `version` (README:178).
- Not found: the intent for scripts that read only the exit code.
- Conclusion: `(needs human input)`.

#### Is `list` on an empty or index-less storage expected to print `success` with `[]`?

- Examined: `list_operation.cpp:42-65`, `storage_core.cpp:126-221`, `models/search_response.cpp`, README `list` section (README:188-250).
- Found: the code prints `success` with no records. README shows only populated output.
- Conclusion: resolved from code. The envelope holds.

#### Does `directory_entry::file_size()` re-stat on the target toolchain?

- Examined: deployment-topology.md (build preset `debug_clang20`, libc++), the libc++ `__filesystem/directory_entry.h` shipped with Xcode.
- Found: it re-stats for the cache states that POSIX directory iteration produces.
- Not found: a libstdc++ re-check (not the deployed toolchain).
- Conclusion: resolved. The N4 race is real for the harness build.

#### Is a query racing a purge in contract?

- Examined: README:473 (maintainer, 91c4f80), `storage_core.cpp:187-198, 927-997`.
- Found: README discourages only purge alongside fetch/pull. Trigger (a) needs that combination. A query racing a lone purge on a quiescent storage is not addressed, and it is not a crash trigger by itself.
- Conclusion: resolved. Trigger (a) is excluded.
