# gtid-resume-preserves-source-rotations — GTID resume keeps every source rotation (ticket 14)

Found by: focus 7 (distributed coordination), focus 9 (idempotency/replay)
Merged from: gtid-resume-preserves-source-rotations (focus 7), gtid-resume-no-skipped-binlogs (focus 9)

## Synthesis notes

Safety, **P1** (both contributors rated it P1). It drops to P2 if byte identity with the source is not an official guarantee.

**Workload check.** After a quiet period, assert `Always(stored names contiguous && every non-tail stored binlog byte-identical to the source's closed binlog incl. its ROTATE)`.

**SUT check.** The name-mismatch branch (`collector_context.cpp:540-553`) gets two assertions with separate messages:
- `UNREACHABLE("GTID resume closed the stored tail without its ROTATE/STOP event")`
- `ALWAYS(new binlog name == current.next())`

Both contributors proposed a gap/ROTATE check at this site. Focus 9's combined `ALWAYS(next name && last event is ROTATE/STOP)` is split into these two.

**Expected outcome today.** This is expected to fail, because ticket 14 is validated.

**Triggers.** All of these happen while PBS is disconnected:
- 0-3 `FLUSH BINARY LOGS` statements
- source restarts
- `max_binlog_size` rotation

PBS can be disconnected by partitions, `read_timeout`, kills, or idle gaps between fetch runs.

## What led here

GTID auto-position resume is how PBS rejoins the source as a replica after any disconnect: a partition, a `read_timeout`, a source restart, or a PBS restart. GTID resume picks the *file* by GTID coverage. Focus 9 treats this as the "gap at the resume point" case.

Ticket 14 (open, Medium) reports the bug. Suppose the source rotates while PBS is disconnected or caught up (or, focus 9 adds, stopped). Then the stored previous binlog loses its closing ROTATE event, and any transaction-free binlogs in between are skipped.

**Validation (focus 7).** The competing explanation was "MTR/QA compare logic is wrong; PBS stores what it received". The discriminating code is `collector_context.cpp:541-553`:
- After a reconnect, the first artificial ROTATE may name a binlog other than the stored tail. In that case the collector just calls `storage_->close_binlog()` and logs "closed binlog file left open".
- No ROTATE is synthesized, and no gap check exists.
- In GTID mode, `reader_context.cpp:566-573` only checks that the artificial ROTATE position is 4. Nothing checks that the new name is `tail.next()`.

Position mode does not hit this. It re-requests the tail name at the stored size, so it receives the real ROTATE. The QA harness reports concrete sizes: stored 422 bytes vs source 466 bytes, a 44-byte ROTATE. The ticket is validated. sut-analysis validated it as well.

## Code paths

- `src/operations/collector_context.cpp:207-251`: GTID dump request with `prev ∪ added` of the tail record.
- `src/operations/collector_context.cpp:503-572`: the artificial-ROTATE handling. Its name-mismatch branch (`:540-553` / `:541-553`) closes the current file and falls through to `open_binlog(new)` without a ROTATE.
- `src/binsrv/storage_core.cpp:680-703`: creates the new binlog record and appends it to the index. Two consequences:
  - `previous_gtids = get_gtids_unsafe()` (`:691`), so the metadata stays GTID-consistent while the byte stream does not.
  - Nothing checks that the new file's sequence number is `current.next()`.
- `src/binsrv/events/reader_context.cpp:566-573`: GTID-mode artificial ROTATE check, position == 4 only.
- `src/binsrv/events/reader_context.cpp:579-581`: info-only preamble, only when the names match.
- `collector_context.cpp:195-200`: a disconnect flushes the buffer. With pull's default no-checkpoint config, that makes the "caught up at disconnect" precondition easy to reach.
- Contrasts:
  - Rewrite mode generates its own ROTATE (`collector_context.cpp:367-376`).
  - Position mode resumes by name, so it receives the real ROTATE.
- Pull egress: `sender_context` does not synthesize a closing ROTATE at a file switch. Related lead N5 is covered by `sender-file-switch-emits-artificial-rotate`.

## Failure scenario

Setup: GTID mode, pull or periodic fetch. PBS is caught up at the end of source `binlog.000007`.

1. PBS loses its connection to the source. Any of these will do:
   - Antithesis partitions PBS↔source.
   - The workload runs `FLUSH BINARY LOGS` while PBS's `read_timeout` disconnect is in progress.
   - The workload restarts the source.
   - PBS is killed, or sits between fetch runs.
2. The source rotates once or several times before the next transaction.
3. On reconnect, the source starts in `binlog.000008` (or `.000010`).

Results:
- The stored `binlog.000007` never gets its ROTATE.
- `.000008` and `.000009` are never created.
- Downstream clients reading through pull never see a ROTATE for that switch.

A GTID pull that hits a 2013 reconnect at the wrong moment reproduces this without any kill.

## Property

This applies after every reconnect or restart in GTID mode (non-rewrite).
- **No gaps.** The stored binlog names have no gaps from the first stored name to the tail. Each next name is `prev.next()`, unless the source's own index has the same gap.
- **Byte identity.** Every non-tail stored binlog is byte-identical to the source's closed binlog, including its closing ROTATE.

## Oracle / workload checks

At quiescence, after a quiet period, compare the stored index with the source's `SHOW BINARY LOGS` from the first stored file onward. Check both of these:
- `Always("GTID resume: stored binlog names are contiguous")`, meaning `seq[i+1] == seq[i]+1`.
- `Always("GTID resume: every non-tail stored binlog is byte-identical to the source's closed binlog")`. Use `check_complete_copy` in `extra/qa/fetch_crash_test.py`.

## Antithesis angle

The interleaving that matters is "the source rotates while PBS is disconnected and caught up". Natural disconnect windows come from:
- partitions and source restarts (each restart rotates)
- kills
- idle gaps between fetch runs

The workload adds rotations of its own:
- `FLUSH BINARY LOGS` with random multiplicity (0-3) at random times
- `max_binlog_size` rotation

## SUT-side instrumentation

The codebase has no Antithesis SDK, so all of these are **missing**.

| Assertion | Site | Proposed by | Status |
|---|---|---|---|
| `UNREACHABLE("GTID resume closed the stored tail without its ROTATE/STOP event")` (STOP covers graceful source restarts) | name-mismatch branch, `collector_context.cpp:541-553` | focus 7 (focus 9's "last stored event is ROTATE/STOP" clause) | missing; would fire today |
| `ALWAYS(new_name == current_name.next(), "GTID resume landed on the binlog right after the stored tail")` | same branch | focus 7, focus 9 | missing; expected to fail today. Keep it as a known failure until the fix lands, after which it becomes a regression guard |
| `REACHABLE("reconnect resumed in a different source binlog than the stored tail")` | same branch | focus 7, focus 9 (`SOMETIMES(name mismatch on resume)`) | missing; exploration anchor shared with `collector-observes-source-binlog-switch-on-reconnect` (merged into recovery-paths-exercised), so reference that site's message rather than adding a second one |

## Status note

**Known failing today (ticket 14).** Keep this property in the catalog as a regression target for the eventual fix. This note used to be listed as an open question.

## Investigation findings (2026-10-13)

**The lost terminator can be STOP as well as ROTATE.** A graceful source restart (SQL `SHUTDOWN`, an upgrade restart) writes STOP to the old binlog, and a GTID resume misses it in the same way. The SUT message should therefore read `UNREACHABLE("GTID resume closed the stored tail without its ROTATE/STOP event")`. When the source crashes (SIGKILL), its own old file has no terminator either, so that case is not a violation (see `source-crash-restart-resume-is-faithful`).

The SUT cannot tell these two cases apart at `collector_context.cpp:541-553`, because both arrive as a name mismatch with no terminator received. Under source SIGKILL faults, the SUT-side `UNREACHABLE` would fire on correct behavior. Two options:
- make the byte check workload-side only (compare against the source's file, which has the terminator exactly when a terminator was written), and keep the SUT site as the `REACHABLE` anchor;
- enable the SUT `UNREACHABLE` only in timelines without source kills.

This changes the instrumentation plan, not the invariant.

**Interplay with the in-process EOF path.** A graceful source shutdown while pull is streaming delivers an EOF packet to PBS, which is case (3) with the buffer kept (see `inprocess-reconnect-resumes-without-dup-or-gap`). Ticket 14's precondition ("caught up at disconnect") applies on that path only when the buffer is empty at EOF. Otherwise the GTID resume goes down the duplicate/fail-stop path instead.

**Byte identity: who tests it.**
- Upstream maintainer MTR tests check byte identity (`diff_with_storage_object.inc`) only in position mode: `binsrv.test:45`, `pull_mode.test:22`, `recover.test:29`.
- The GTID combinations of `resume_streaming.test` compare no bytes.
- The only GTID-mode byte comparison is the user's own `gtid_resume_after_rotation.test` (c658df2, aec5c01), which fails today.
- The README positions PBS as "an enhanced version of mysqlbinlog --read-from-remote-server" that can "resume operation from the point when it was previously stopped". It makes no byte-identity statement.

The Invariant stays `Always`, but its ROTATE/byte half rests on a guarantee that is not yet confirmed. Treat it parametrically until a maintainer answers.

## Open questions

- Is byte identity with the source (including closing ROTATE/STOP) an official guarantee in GTID mode, or only an MTR/QA convention? If it is not, the property shrinks to "no skipped binlog names", or to P2 if that is dropped as well. `(needs human input)`
- Should transaction-less intermediate binlogs be materialized in GTID mode? It only matters if byte or file-name fidelity is a guarantee. `(needs human input)`

### Investigation Log

#### Is byte identity with the source an official guarantee for GTID mode, or only an MTR convention?

- Examined: README.md (line 3, the "Pull" section around line 377, the configuration reference), the MTR tests that use `diff_files` / `diff_with_storage_object.inc` and their authorship (`git log`), `resume_streaming.test` together with its GTID combinations, and ticket 14 in `.tickets-snapshot.txt` (lines 742-786).
- Found: upstream byte comparisons exist in position mode only. In GTID mode, upstream checks continuity through the reader's position validation and log output, not file bytes. There is no README statement either way. The design does accept non-identical bytes in GTID-based rewrite mode (renumbered files), which shows that GTID-mode fidelity is not assumed universally.
- Not found: any maintainer statement on GTID-mode byte identity.
- Conclusion: `(needs human input)`. This is the same catalog-wide question as in sut-analysis.

#### Should empty (transaction-less) intermediate binlogs be materialized?

- Examined: `collector_context.cpp:541-553`, `storage_core.cpp:680-703` (no next-name check), and ticket 14's "Expected behaviour" and "Suggested fix" sections, which are the user's drafts, not a maintainer decision.
- Found: no code or documentation states an intent. They matter for `mysqlbinlog --raw` file naming and for replicas tracking source file names through pull. They do not matter for GTID-set completeness.
- Conclusion: `(needs human input)`. The answer depends on the previous question.

## Evaluation refinements (2026-10-13)

Applied by the evaluation synthesis (`../evaluation/synthesis.md`). Where this section and earlier text in this file disagree, this section wins; the catalog entry has been updated to match.

- The SUT `UNREACHABLE` "enabled only in timelines without source SIGKILL" cannot be gated inside the binary, and every template runs `parallel_driver_kill_mysqld.sh` (implementability P2, wildcard W1). It becomes `ALWAYS(terminator_written || source_sigkilled, "GTID resume closed the stored tail without its ROTATE/STOP event (no source SIGKILL in this timeline)")`, with `source_sigkilled` read from the harness timeline-tag file that the source-kill driver sets before its first kill. The workload byte check is the primary detector.
- Ticket 14 fires in ordinary GTID timelines; containment is bias B3.
- Runs in `fetch_fs`, `pull_fs`, `s3`.
