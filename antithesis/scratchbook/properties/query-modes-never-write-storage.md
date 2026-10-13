# query-modes-never-write-storage — list / search_by_* never modify the storage

## What led here (code-confirmed)
- `storage_core.cpp:166-178`: after `load_metadata()` and `validate_metadata()`, the constructor runs
  `if (!encryption_format_.has_value() && encryption_format.has_value()) { encryption_format_ = encryption_format; save_metadata(); }`
  with **no construction-mode guard**. `save_metadata()` (`:828-834`) does a `put_object(metadata.json)`.
- `initialize_storage_encryption` (`:632-660`) runs for every mode when the config has `storage.encryption` (and a keyring). Operators normally share one config file between `pull`/`fetch` and `list`/`search_*`. Turning on encryption in that file and running `list` is therefore enough to rewrite `metadata.json` of a storage that a running (older-config) writer is still filling in plaintext. The same happens with `purge_binlogs`.
- This is a storage-format "upgrade" (a metadata.json schema value flipped from absent to `generic`) performed by a read-only operation, while a writer may be live. Querying alongside a writer is explicitly a design intent (`storage_core.cpp:189-195`, `:766-770`, `:942-950`). On the filesystem backend, `put_object` uses the deterministic `metadata.json.tmp` (`basic_storage_backend_fwd.hpp:56-60`), so two concurrent `list` runs can collide on the temp name (sut-analysis hazard (i)). On S3 it is last-writer-wins.
- Effects found so far are benign for data. Per-file envelopes decide encryption (`generate_binlog_encryption_record`, `:1008+`, depends only on the *process's* active KEK), so a `metadata.json` that says `generic` over plaintext files does not break reading. But it breaks the invariant "metadata.json describes the storage", and it makes the next writer start without encryption config pass validation (`:810-818` only rejects when both are set and differ). There is no detectable record that encryption was enabled by a query.

## Oracle
- Workload: snapshot object names + sizes/ETags (or mtimes) before each `list`/`search_*`, and compare after, excluding objects a concurrent writer is allowed to touch (data files, `.json`, index). `ALWAYS(metadata.json unchanged by a query-mode process)`.
- Simpler SUT-side check (below).

## SUT-side instrumentation (missing)
- In `storage_core::save_metadata`: `ALWAYS(construction_mode_ == streaming)` with the message "only streaming mode writes metadata.json". Currently `save_metadata` is reachable from querying_only/purging via `:174-178`, so this would fire as soon as the workload runs a query with an encryption-enabled config against an unencrypted storage.

## Open questions
- Is the `metadata.json` "encryption upgrade" meant to be performed by any mode, or only by the writer? If any mode is fine, drop the `ALWAYS` and keep only the temp-name collision concern. `(needs human input)`

## Answers from investigation (2026-10-13)
- **Code re-verified at 99de682.** The stamp is at `storage_core.cpp:172-178` (comment `:172-174`, `save_metadata()` at `:177`), with no mode check. `save_metadata` (`:828-834`) is an unconditional `put_object(metadata.json)`. `list_operation.cpp:47-51` passes the whole `main_config` into a `querying_only` storage, so a config with `storage.encryption` reaches the stamp. `search_by_*` and `purge_binlogs` do the same.
- **Intent (partial).** The stamp came from `3e812f2` (PBS-39 part 8). Its message frames it as "we start PBS with encryption enabled" on a non-empty storage. Construction modes (`querying_only`, `streaming`) already existed then, and the fresh-storage `save_metadata()` a few lines earlier was already gated on `streaming` (`storage_core.cpp:154-156` today). The upgrade write was not gated. That looks like an oversight, which favours keeping the `ALWAYS`, but maintainers must confirm.
- **Workload must vary the query config (resolved).** The path is reachable only when (a) the storage's `metadata.json` has no `encryption` key, i.e. the storage was initialised by a writer without `storage.encryption`, and (b) the query process's config has `storage.encryption` *and* a `keyring` holding the `kek_id`; otherwise `initialize_storage_encryption` throws first (`storage_core.cpp:632-645`). `deployment-topology.md` draws encryption once per timeline and shares the config across processes, so with the current plan this path is **never** reached. The query driver must sometimes run `list` / `search_*` with an "encryption-added" config variant against an unencrypted storage. Without that variant, the property is vacuous: add `Sometimes("query ran with encryption config against an unencrypted storage")` on the workload side.
- **Scope note.** `purge_binlogs` (purging mode) reaches the same write; the SUT-side `ALWAYS(construction_mode_ == streaming)` covers it too.

### Investigation Log

#### Is the metadata.json encryption upgrade meant to be performed by any mode, or only by the writer?
- Examined: `storage_core.cpp:126-221, 828-834`; `list_operation.cpp:40-65`; `git log -S"did not have encryption"` (commits `3e812f2`, `7f8cb69`) and the `3e812f2` message and diff; `3e812f2:src/binsrv/storage.cpp` for construction-mode gating at the time.
- Found: introduced for writer start with encryption; the neighbouring fresh-storage write was mode-gated in the same version, the upgrade write was not.
- Not found: any comment, doc or test stating that query or purge modes should stamp the metadata.
- Conclusion: `(needs human input)`, with the evidence leaning to "oversight".

#### Should the workload randomise whether query invocations carry the encryption section?
- Examined: `storage_core.cpp:140-178, 632-660`; `deployment-topology.md` (config dimensions).
- Found: the stamp needs an unencrypted storage plus a query config that has encryption and a valid keyring. The topology's per-timeline shared config never produces that combination.
- Conclusion: resolved: yes. Add an "encryption-added" query config variant, otherwise the property is unreachable.
