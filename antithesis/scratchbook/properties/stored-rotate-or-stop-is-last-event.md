# stored-rotate-or-stop-is-last-event — No stored binlog has events after its terminal ROTATE/STOP

Found by: focus 3, 1 (failure recovery, data integrity). Both foci found it independently.
Merged from: stored-rotate-or-stop-is-last-event (focus 3), rewrite-no-events-after-terminal-rotate (focus 1)

## Synthesis notes

Safety, **P0**. The foci disagreed on priority: focus 1 rated it P0 and focus 3 rated it P1. We resolved it to P0 for three reasons:

- Two foci found it independently.
- The outcome is silent loss on restore, because readers treat ROTATE as end of file.
- On S3 the window needs no kill. A failed or stalled magic PUT for N+1 under default network faults is enough.

Confidence stays medium, because the bug has not been reproduced.

**Mechanism.** In rewrite mode, the local rotation takes two steps:

1. Flush N with its generated ROTATE (`storage.cpp:204-215`).
2. Create N+1 (`storage_core.cpp:680-703`).

If N+1 creation fails, or the process is killed between the two steps, restart takes the `is_fresh()` branch (`collector_context.cpp:367-371`). That branch reopens the terminated N for append (`storage_core.cpp:268-291`), and the next transaction lands after the ROTATE. Nothing at open time parses data (`:785`).

**Assertions.**

- Workload: `Always(every stored binlog has at most one terminator and nothing follows it)`. The check is cheap and runs in all modes. Encrypted binlogs are decrypted through pull first.
- SUT: `ALWAYS_OR_UNREACHABLE(!tail_already_terminated, "resume opens a tail binlog that is not already terminated by ROTATE/STOP")` in the existing-binlog branch of `open_binlog`. This needs either a persisted closed flag or a read of the last event.
- SUT: `REACHABLE("rewrite: terminal ROTATE flushed, next file not yet created")`.

## What led to it

This is a new lead from the failure-recovery and data-integrity passes, with no ticket. It was traced in code at commit 99de682 but not reproduced.

In rewrite mode, PBS itself decides where one stored binlog ends. At a transaction boundary, once the current file reaches `rewrite.file_size`, PBS does three things:

1. Generates a non-artificial ROTATE into the current file.
2. Closes the file.
3. Creates the next file (`collector_context.cpp:352-420`).

"Close N with its terminal ROTATE" and "create N+1" are separate storage operations, with a backend round-trip between them. On restart, the rewrite path picks the file to append to purely from the storage tail. It assumes that tail is still open for writing and never checks whether the tail already ends with a terminal ROTATE.

## Code paths (commit 99de682)

**Step 1: local rotation.** This happens in `collector_context::rewrite_and_process_binlog_event` (`collector_context.cpp:352-427`, rotation at `:352-420`).
- The entry condition is `context.is_fresh() || (context.is_at_transaction_boundary() && storage_->get_current_position() >= file_size)` (`:352-353`).
- `!context.is_fresh()` only guards generating the terminal non-artificial ROTATE (`:376`).

(Corrected 2026-10-13. Earlier text folded `!is_fresh()` into the entry condition.)

- It sets `binlog_name = current.next()` (`:352-373`).
- It generates a non-artificial `ROTATE(<next>:4)`. The call chain is `process_binlog_event` → `storage_->write_event` → `process_rotate_or_stop_event` → `storage::close_binlog()` (`collector_context.cpp:376-384`, `:463-477`, `:574-579`; `storage.cpp:204-215`).
- The flush writes the ROTATE and **saves the old file's metadata (`.json` PUT), including the ROTATE bytes**.

**Step 2: create N+1.** PBS generates an artificial `ROTATE(<next>:4)`. The call chain is `process_artificial_rotate_event` → `storage_->open_binlog(next)` → `open_new_binlog_file_internal` (`storage_core.cpp:680-703`), which:

- writes the magic;
- PUTs the `.json`;
- PUTs the index.

On S3, `open_stream(create)` is local only (`s3_storage_backend.cpp:822-848`), so the first remote action is the magic PutObject. On the filesystem, `ofs_.open` with trunc creates the file immediately.

**Step 3: restart after a gap between steps 1 and 2.**

- The `reader_context` is constructed with the storage name N and position = size(N) (`collector_context.cpp:142-146`).
- In `rewrite_and_process_binlog_event` (`collector_context.cpp:352-372`), the storage is non-empty and `context.is_fresh()` is true. The `else if (context.is_fresh())` branch (`:367-371`, `:370`) sets `binlog_name = storage_->get_current_binlog_name()`, which is the **old, already terminated file N**.
- No non-artificial ROTATE is generated, because of the `!context.is_fresh()` guard.
- The generated artificial `ROTATE(N:4)` makes `process_artificial_rotate_event` call `open_binlog(N)`. N is an existing tail, so it is opened for append (`storage_core.cpp:268-291`, `opened_with_data_present`).

**Step 4: append after the terminator.**

- Because position > 4, `expect_info_only_preamble_events_` is true (`reader_context.cpp:579-582`). The generated FDE and PREVIOUS_GTIDS are therefore not written.
- The next GTID transaction is rewritten with `storage_->get_current_position()` and **appended after N's ROTATE**.
- At the following transaction boundary, `position >= file_size` triggers a second local rotation.
- The stored N ends up as `[…, ROTATE→N+1, GTID trx …, XID, ROTATE→N+1]`.
- Byte positions and CRCs are internally consistent, because the rewriter recomputes them. Nothing in PBS notices.

**Detection.** Nothing at open time parses data (`storage_core.cpp:785`, a TODO), so this state is never detected.

**Non-rewrite modes.** These do not appear affected.

- In position mode, the source resumes at (N, end), then sends its own artificial ROTATE(N+1) and moves on.
- In GTID mode, the source starts at the file that holds the first missing GTID, which is N+1.
- Both modes close the old file through the name-mismatch branch (`collector_context.cpp:541-553`).

The invariant is still worth checking in every mode as a generic structural check.

## Failure scenarios / when the window opens

1. **S3, no crash needed.** Trigger: the magic upload of `<next>` fails, from a PBS↔S3 partition longer than the SDK retries or an S3 5xx/stall that becomes a CRT error.
   - `open_binlog` throws and the process exits 1.
   - The bucket holds the old file with its ROTATE and metadata, but no `<next>` object.
   - On restart, the bug triggers.
   - The whole magic PUT duration is the window, and an injected PUT failure on the N+1 key lands exactly in it.
   - If the magic upload was ambiguous (the object was created), ticket 15's wedge happens instead.
2. **Filesystem, kill needed.** Trigger: SIGKILL between `close_binlog()` returning and `open_stream(create)` for `<next>`.
   - The window is narrow (logging plus event generation), but Antithesis can explore it.
   - An `open_stream` failure lands in the same window.
   - A kill after file creation leads to ticket 15's wedge (an unindexed object) instead.
3. **Non-rewrite modes, when the source itself is mid-switch.** Believed not to append to N, because the source's next artificial ROTATE always names N+1. This was reasoned, not tested. The structural check covers it anyway.

## Impact

Downstream readers treat a ROTATE as end of file.

- `mysqlbinlog --read-from-remote-server --raw` through pull starts a new output file at the ROTATE.
- A replica switches to N+1.
- The transactions after the mid-file ROTATE are either attributed to the wrong file or skipped. That is silent loss on restore.
- `sender_context` serves bytes without inspecting event types, so pull serves such a file without complaint.

## Oracle / workload checks

After convergence, and optionally after every run, parse events `[0, size)` of every stored binlog. If the storage is encrypted, decrypt through pull first. Then check:

- `Always("every stored binlog has at most one terminator and nothing follows it")`. A non-artificial ROTATE or STOP event, if present, is the last event. The tail binlog may have no terminator yet.

The check targets rewrite mode, but it is cheap enough to run in all modes.

## SUT-side instrumentation suggestions

The codebase has no Antithesis SDK, so every SDK assertion below is missing.

- `ALWAYS_OR_UNREACHABLE(!tail_already_terminated, "resume opens a tail binlog that is not already terminated by ROTATE/STOP")`. **Missing.**
  - Site: the existing-binlog branch of `storage_core::open_binlog` / `open_existing_binlog_file_internal`, or `collector_context::process_artificial_rotate_event` before it reuses or opens a binlog for append.
  - It needs a cheap way to know the tail's last event type. Options are a "closed" flag persisted in `<name>.json`, which would also be the natural fix, or reading the last event header. Today nothing records it, which is why the bug is silent.
  - Focus 1 proposed the same check at the same site as `ALWAYS(!current binlog ends with ROTATE/STOP, "append never targets a binlog closed by a terminal event")`. It is kept once, under the message above.
- `REACHABLE("rewrite: terminal ROTATE flushed, next file not yet created")`, between step 1 (`close_binlog` returned) and step 2 (`open_binlog(next)`). **Missing.** It gives Antithesis an anchor for the window.
- `REACHABLE("rewrite: resumed into the current tail after restart")` at `collector_context.cpp:370`. **Missing.** It anchors the resume branch that reopens N.

## Fault requirements

- **S3 variant.** Default network faults on the PBS↔S3 link, such as a partition or stall longer than the SDK retries during the N+1 magic PUT. The `s3_fault_proxy` can also fail the first PUT of `<base>.000002`. No kill is needed.
- **Filesystem variant.** A workload-driven SIGKILL timed between `close_binlog` and N+1 creation.
- **Workload configuration.** Rewrite mode with a tiny `rewrite.file_size`, so that local rotations are frequent.

## Open questions

- **Does anything downstream reject such a file?** `(partial: pull's sender_context inspects only FORMAT_DESCRIPTION events and serves a mid-file ROTATE as-is; mysqlbinlog and replica handling live in MySQL source outside this repo)` The answer decides the severity (silent PITR corruption versus a visible failure on restore), not the property itself.
- **Can non-rewrite position or GTID mode open the same window when the source itself is mid-switch?** `(partial: PBS side traced, so a resume at (N, size(N)) reopens N, writes nothing, and closes it on the next artificial ROTATE(N+1) (collector_context.cpp:541-553); the claim that the source always continues at N+1 rests on MySQL dump semantics not in the repo)`
- **Which fix?** `(needs human input)` One option: on a fresh restart in rewrite mode, if the tail size ≥ `file_size` or the tail ends with a ROTATE, create the next file. The other: persist a "closed" flag. The choice affects where the SUT-side assertion goes.

## Investigation results (2026-10-13)

- **No guard exists, and the mechanism is confirmed by code trace (still not reproduced).**
  - On a fresh connection with a non-empty storage, the branch at `collector_context.cpp:367-370` takes `storage_->get_current_binlog_name()` without consulting `get_current_position() >= file_size` or the tail's last event. The size check at `:353` applies only to the non-fresh disjunct.
  - The rewriter's persisted `last_sequence_number` (`:426`) feeds only event renumbering.
  - The generated artificial `ROTATE(N:4)` passes the GTID-mode check (`reader_context.cpp:566-572`). `expect_info_only_preamble_events_` becomes true because the name matches and `position_ = size(N) > 4` (`:579-582`).
  - `process_artificial_rotate_event` finds no open binlog (it is a new process), so it calls `open_binlog(N)`. That takes the existing-tail append path (`storage_core.cpp:268-291`), which checks only that N is the last record and has a non-zero size.
  - Nothing parses stored data at open (`:785`, a TODO).
  - The S3 no-kill window is also confirmed. `open_stream(create)` is local-only on S3 (`s3_storage_backend.cpp:822-848`), so a failed magic PUT for N+1 leaves the bucket with the terminated N as tail and no `<N+1>` object.
  - **Implication:** the property stays a bug-finding property. It is expected to fail today once the window is hit.
- **A natural guard exists for the fix.** A terminated rewrite file always has `size ≥ file_size`: the terminal ROTATE is generated only once `get_current_position() >= file_size` (`:352-353`). So "on `is_fresh()`, rotate if the tail size ≥ `file_size`" is a sufficient fix for rewrite mode. Whether the maintainers pick it is the open fix question.
- **The SUT-side assertion site stays valid** for either fix: the existing-binlog branch of `open_binlog`, or `collector_context.cpp:367-370`.
- **Property unchanged:** type, assertion and P0 stay. The text was corrected for the entry condition.

### Investigation Log

#### Is there a guard that makes the rewrite path rotate first on restart?

- Examined: `collector_context.cpp:274-429, 431-579`, `reader_context.cpp:500-610`, `storage.cpp:119-141, 204-215`, `storage_core.cpp:257-297, 680-719`.
- Found: there is no size check or last-event check on the fresh branch. The append-reopen of N is accepted at every layer.
- Not found: any persisted "closed" marker in `binlog_file_metadata`. Its fields are size, GTIDs, timestamps, last_sequence_number and encryption (`storage_core.cpp:907-925`).
- Conclusion: resolved. No guard exists, and the mechanism is confirmed in code.

#### Does anything downstream reject such a file?

- Examined: `sender_context.cpp` (event-type handling: only `format_description` at `:202-324`; the artificial ROTATE is generated at `:557-573`), `network_service.cpp:279-334`.
- Found: pull serves the stored bytes without checking ROTATE or STOP placement.
- Not found: `mysqlbinlog` and replica behaviour (MySQL source, not in the repo).
- Conclusion: `(partial: …)`.

#### Can non-rewrite position mode open the same window?

- Examined: `collector_context.cpp:493-572`, `reader_context.cpp:546-565`.
- Found:
  - A resume request at (N, size(N)) yields the artificial ROTATE(N, size(N)). It passes the position check and opens N for append. The FDE is info-only, so nothing is written.
  - The next artificial ROTATE(N+1) closes N through the name-mismatch branch and creates N+1.
  - A pure-PBS trace therefore never appends to a terminated N.
- Not found: the MySQL server's dump behaviour when asked to start at the end of a rotated file (outside the repo).
- Conclusion: `(partial: …)`.

#### Which fix?

- Examined: ticket list (no ticket for this lead), git log (no related commit).
- Conclusion: `(needs human input)`.

## Evaluation refinements (2026-10-13)

Applied by the evaluation synthesis (`../evaluation/synthesis.md`). Where this section and earlier text in this file disagree, this section wins; the catalog entry has been updated to match.

- Encrypted storages: the independent Python decryptor is the canonical encrypted reader. Pull is not used to read a storage it does not own: pull always builds a streaming storage and runs the collector (`pull_operation.cpp:108-153`), so on a `fetch_fs` or `s3` storage it would run recovery on, and append to, the storage under test. A pull readback there runs only at quiescent points on a snapshot copy with `connection.host` pointing at an unreachable endpoint (lenses: implementability P1, wildcard W2).
- Runs in `fetch_fs`, `pull_fs`, `s3` (rewrite configs).
