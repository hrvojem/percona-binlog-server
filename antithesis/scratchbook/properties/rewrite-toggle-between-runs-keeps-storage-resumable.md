# rewrite-toggle-between-runs-keeps-storage-resumable — changing `replication.mode`, `rewrite` or `base_file_name` between runs is either refused up front or leaves a resumable storage

## What led here

Lifecycle focus (config changes between runs). `metadata.json` records only format version, replication mode and encryption format (`storage_metadata.hpp:35-37`, `storage_core.cpp:828-834`). Replication mode is validated at start-up (`validate_metadata`, `:808-815`): a mismatch is refused before any binlog object is touched. `rewrite` and `rewrite.base_file_name` are not recorded anywhere (sut-analysis N11: "toggling them is undetected and can wedge the storage (`storage_core.cpp:274-278`)"). This pass traced the wedge end to end.

## Code paths (verified at 99de682)

- Rewrite naming (`collector_context.cpp:352-374`): on an empty storage the first file is `<base_file_name>.000001`; after a reconnect or restart (`context.is_fresh()` on a non-empty storage) it reuses `storage_->get_current_binlog_name()`; at the size limit it uses `get_current_binlog_name().next()`. So once a storage exists, `base_file_name` is silently ignored and the sequence number advances independently of the source's file numbers (usually faster, because `rewrite.file_size` is small).
- Rewrite **on → off** (GTID mode): the reader is built with the storage tail name (e.g. `binlog.000010`, `collector_context.cpp:142-146`). The source resumes from the file holding the first missing GTID, e.g. `binlog.000003`, and sends artificial ROTATE(`binlog.000003`, 4). The reader accepts it (GTID mode only checks position == 4, `reader_context.cpp:566-573`). `process_artificial_rotate_event` → no binlog open in a fresh process → `storage_->open_binlog("binlog.000003")` → `storage_core::open_binlog` (`storage_core.cpp:268-279`): the name **exists** in `binlog_records_` (the rewriter created it earlier) but is not the latest → `logic_error("cannot open an existing binlog that is not the latest one for append")`. This repeats on every restart: the storage is wedged for writers (query modes still work).
- If `base_file_name` differed from the source's base name, the same toggle instead creates `binlog.000003` after `rw.000010` in the index. The storage keeps working, but the index now mixes base names, and `select_purge_victim_count` (`storage_core.cpp:339-344`) refuses any target whose base name differs from the front record's, so purge can no longer reach the newer files.
- Rewrite **off → on**: continues under the existing tail name; no wedge, but the resulting files are rewritten (regenerated FDE/PREVIOUS_GTIDS, recomputed positions) while earlier files are byte copies, so SG2 byte-identity holds only for a prefix of the index.
- Mode change: `validate_metadata` throws before `load_binlog_index`. Only `remove_temporary_objects` (`:150`) has run, so the failed start is non-destructive apart from `.tmp` deletion.

## Oracle

- Workload: between restarts of fetch/pull on one storage, randomly flip `replication.mode`, `rewrite` presence, `rewrite.base_file_name` and `rewrite.file_size` (rewrite needs GTID mode and checksums). The source keeps writing and rotating.
- `Always(a start-up with changed config either exits non-zero before modifying any non-.tmp object, or — if it starts streaming — the storage remains resumable: every later run with the same config converges, and a run with the original config also converges)`.
- Concrete sub-checks:
  - `Always(mode mismatch ⇒ exit != 0 && binlog objects unchanged)` (expected to pass).
  - `Always(after any sequence of rewrite toggles, a fetch/pull either refuses at start-up with a config-mismatch message or makes progress)`; a repeated `cannot open an existing binlog that is not the latest one for append` across restarts is the failure signature.
- Mostly deterministic; Antithesis adds value by combining toggles with kills mid-run (e.g. a kill right after the first rewritten file is created, then rewrite turned off).

## SUT-side instrumentation (missing)

- `storage_core::open_binlog` at `:275-278`: `UNREACHABLE("writer asked to append to a non-tail binlog")`. Under a fixed config this should be unreachable except via ticket 14/25-style source changes, so firing it in a toggle run is a direct signal.
- Recording `rewrite`/`base_file_name` in `metadata.json` would turn this into a validated start-up check; then `ALWAYS(config rewrite settings == stored rewrite settings)` after `validate_metadata`.

## Open questions

- Is toggling `rewrite` on an existing storage supported? If not, the expected behaviour is "refuse at start-up" and the property checks for that (today it fails because nothing is recorded). If it is supported, the wedge is a bug and the property is a recoverability check. `(needs human input)`
- Is changing `base_file_name` on a non-empty storage meant to be ignored (current behaviour) or rejected? This decides whether "ignored silently" is itself a violation. `(needs human input)`

Resolved (see Investigation Log): the ticket 14 interaction. The wedge is raised by the existence check in `storage_core::open_binlog`, downstream of the code a ticket 14 fix would touch, so a fix may change the error text or what gets written to the tail before it fires, but not the wedge itself. **Property change:** the oracle must not key on the error string `cannot open an existing binlog that is not the latest one for append`. The failure signature is "N consecutive restarts with an unchanged config, each exiting non-zero with no change to `binlog.index` or the tail size". Add the sub-check `Always(no stored ROTATE event names a binlog that sorts at or before the file that contains it)`, which catches a ticket-14-style fix writing a closing ROTATE into the tail before `open_binlog` throws.

Invariant, assertion type and priority are unchanged.

### Investigation Log

#### Is toggling `rewrite` on an existing storage supported?

- Examined: README `<replication.rewrite>` section (lines 592-595), `storage_metadata.hpp:35-37`, `storage_core.cpp:800-834` (`load_metadata`/`validate_metadata`/`save_metadata`), `git log` of `storage_metadata.hpp` (d27de1f PS-10244, f62a95a PBS-39), all MTR tests that set `$binsrv_rewrite_file_size`.
- Found: README describes rewrite only for a single configuration and says nothing about changing it on an existing storage. Metadata records `mode` and `encryption` only. The commit history shows the designers did handle one config change on a non-empty storage on purpose: PBS-39 turns on encryption for a previously unencrypted storage by updating `metadata.json`. Nothing similar exists for rewrite. No MTR test changes `rewrite` between runs.
- Not found: any statement of intent (doc, comment, commit message) about rewrite toggles.
- Conclusion: `(needs human input)`. The code shows the wedge mechanism (see "Code paths"). Whether "refuse" or "recover" is the expected outcome is a maintainer decision.

#### Is changing `base_file_name` on a non-empty storage meant to be ignored or rejected?

- Examined: `collector_context.cpp:352-374` (name selection), README line 594, `storage_core.cpp:335-344` (purge base-name check).
- Found: the code ignores `base_file_name` silently on a non-empty storage. On reconnect it uses the tail name, and at the size limit it uses `tail.next()`. The README describes the generated sequence only from the base name. Purge refuses a target whose base name differs from the front record's, so the code assumes one base name per storage, but nothing enforces it on the write path.
- Not found: documented intent.
- Conclusion: `(needs human input)`.

#### How does this interact with ticket 14 (GTID resume after rotation)?

- Examined: ticket 14 text (snapshot lines 742-794, including its suggested fix), `collector_context.cpp:503-572` (`process_artificial_rotate_event`), `storage_core.cpp:260-280` (`open_binlog`), `reader_context.cpp:566-600`.
- Found: both cases enter the name-mismatch path of `process_artificial_rotate_event`. Ticket 14 is the sub-case where the new name is **not** in `binlog_records_`: the storage closes the tail and creates the new file. The toggle wedge is the sub-case where the name **is** in `binlog_records_` but is not the tail, and `storage_core::open_binlog` throws (`:272-279`). Ticket 14's suggested fix ("close the current stored binlog with a ROTATE naming the new binlog before opening it", or "request the skipped binlogs by name") sits before `open_binlog`. With the first variant, the toggle case would write ROTATE(`binlog.000003`) into the tail (`binlog.000010`) and then still throw. The wedge would remain and would also leave a backwards ROTATE in the tail, if that event reaches a flush.
- Conclusion: resolved. The wedge survives a ticket 14 fix, but its error text and side effects may change. The property's oracle is now message-agnostic and gains the backwards-ROTATE sub-check (see above).

## Evaluation refinements (2026-10-13)

Applied by the evaluation synthesis (`../evaluation/synthesis.md`). Where this section and earlier text in this file disagree, this section wins; the catalog entry has been updated to match.

- Added vacuity marker `Sometimes("writer restarted after rewrite, mode or base_file_name changed")`; the trigger needs the config-mutation driver (topology change; coverage-balance F1, implementability C5).
- The base-case wedge is deterministic (MTR territory); whether to move it is part of bias B4.
- Runs in `fetch_fs`.
