# source-crash-restart-resume-is-faithful — After the MySQL source crashes and restarts, PBS resumes and its copy stays faithful

## What led to it

Focus 3 lens, applied to the *other* side of the link. A crashed source leaves its active binlog without a ROTATE/STOP terminator and starts a new binlog on restart. PBS has a dedicated recovery branch for exactly this, with a comment: "in case when the server was not shut down properly, it won't have ROTATE or STOP event as the last one in the binlog, so here we handle this case by closing the old binlog and opening a new one" (`collector_context.cpp:514-516`). No existing harness kills the source while PBS streams: `kill_and_restart.test` kills mysqld but not mid-stream with byte checks; `source_change_test.py` uses graceful restarts (sut-analysis "Existing test coverage").

## Code paths

- `process_artificial_rotate_event` `collector_context.cpp:493-572`:
  - in-process (pull): the binlog is still open, and the new name differs → `storage_->close_binlog()` ("closed binlog file left open", `:541-553`) → `open_binlog(new)`;
  - cross-process (fetch, or pull restarted): the binlog is not open → `open_binlog(new)`. Position mode first replays (old, end) → the source sends artificial ROTATE(old, end), then switches to the new file.
- `reader_context.cpp:547-566` (position mode): artificial ROTATE position must equal the stored position for the same file, or be 4 for a new file.
- `connection.cpp:546-551`: what the source's death looks like mid-stream (2013 → retry, or another errno → exit).

## Failure scenarios to explore

- The source is SIGKILLed mid-transaction. PBS holds the partial transaction in its buffer, gets 2013, discards it (`collector_context.cpp:195`) and flushes. After the source restarts, its crash recovery truncates the partial transaction from its binlog. PBS must match.
- The source is SIGKILLed after a commit that PBS already received and flushed, but (with `sync_binlog != 1`) the commit was not durable on the source. After the restart the source lacks it, and PBS is **ahead** of the source. Position mode then gets 1236 (position beyond end of file). GTID mode gets a 1236 "slave has more GTIDs than the master". Both are crash loops (fail-stop pull). This is out of scope: the harness pins `sync_binlog=1` and `innodb_flush_log_at_trx_commit=1`, which makes it unreachable (see Investigation findings).
- The source restarts and is immediately rotated again (FLUSH) before PBS reconnects. GTID mode then hits ticket 14 (the unterminated old file is closed without its ROTATE, and empty binlogs are skipped). The unterminated case is fine here, because the source's own copy also lacks the terminator.

## Oracle

- Requires killing the **source mysqld** with SIGKILL. The harness does this with the restart-loop wrapper plus `parallel_driver_kill_mysqld.sh` (deployment-topology.md:69-71), so node termination is not needed.
- After faults stop and the source is up: `Always(storage converges to an exact copy of the source's binlogs)`, using `check_complete_copy()`. Mask IN_USE only for the source's active file (`binlog_as_sent`). Recovery clears IN_USE on the crashed file, and the sender clears it on the wire (see Investigation findings). Then `Sometimes(PBS stored a binlog that the source ended without ROTATE/STOP)` as a coverage hint.

## SUT-side instrumentation suggestions (missing)

- `REACHABLE("collector closed a binlog the source left unterminated")` at `collector_context.cpp:547`. The branch itself is the event of interest, so `REACHABLE` fits better than `SOMETIMES(true)`.

## Investigation findings (2026-10-13) and property changes

**Fault injection.** The deployment already provides it. `mysqld` runs under a restart-loop wrapper in `mysql-source`, and `parallel_driver_kill_mysqld.sh`, a test command in that container, SIGKILLs it (deployment-topology.md:69-71, 178, 195, 242). Node termination of `mysql-source` is not needed. Whether node termination is enabled for the tenant remains a file-level question, and it does not block this property.

**Use SIGKILL, not SQL `SHUTDOWN`, for this property.** The two take different PBS paths:
- SIGKILL closes the socket, and PBS gets `CR_SERVER_LOST` (2013). That is case (2): discard plus flush, then the in-process reconnect.
- A graceful `SHUTDOWN` sends an EOF packet. That is case (3): the buffer is kept. It is covered by `inprocess-reconnect-resumes-without-dup-or-gap`.

**PBS ahead of the source is out of scope.** The harness pins `sync_binlog=1` and `innodb_flush_log_at_trx_commit=1` (deployment-topology.md:67). With `sync_period == 1`, MySQL advances `binlog_end_pos` only after the fsync (MySQL 9.7.1 `sql/binlog.cc:7967, 7984, 8035-8048`). The dump thread therefore never sends non-durable bytes, and PBS cannot end up ahead after a process kill.
- What PBS *should* do when it is ahead (it crash-loops on 1236 today) belongs to `source-swap-never-silently-accepted` / ticket 25. It is not part of this property.

**Oracle correction.** The earlier text said "the source's crashed binlog has IN_USE set forever". That is wrong:
- Crash recovery truncates the crashed binlog to the last valid transaction and **clears** `LOG_EVENT_BINLOG_IN_USE_F` (`binlog.cc:6025-6043, 6983-6990`).
- The dump thread also clears that flag on the FDE it sends (`rpl_binlog_sender.cc:1149`).

So a recovered, closed source binlog should be byte-identical to PBS's copy with no masking. Only the source's *currently active* file needs `binlog_as_sent()` masking (`extra/qa/fetch_crash_test.py:217-235`). The recovered file may also hold complete, synced transactions beyond the last `binlog_end_pos` PBS saw, from a crash between the fsync and the end-pos update. PBS must pick those up on resume, and the byte-identity check covers that case.

## Open questions

- None.

### Investigation Log

#### Are node-termination faults enabled for the tenant, or will source kills be driven by a custom fault script?

- Examined: deployment-topology.md (lines 18, 66-73, 178, 195, 242, 265).
- Found: the harness design uses a restart-loop wrapper plus the `parallel_driver_kill_mysqld.sh` test command, which works without node termination.
- Conclusion: resolved for this property. The tenant question stays file-level only.

#### What should PBS do when it is ahead of a recovered source (`sync_binlog != 1`)?

- Examined: MySQL 9.7.1 `sql/binlog.cc:7940-8050` (`update_binlog_end_pos_after_sync`) and deployment-topology.md:67.
- Found: with the pinned `sync_binlog=1`, the "ahead" state cannot arise from a process kill. PBS's intended behavior when ahead is a source-identity concern (ticket 25).
- Conclusion: resolved by scoping it out. The question moves to `source-swap-never-silently-accepted` if anyone wants it answered.

#### (Side check) Does the crashed source binlog keep IN_USE?

- Examined: `binlog.cc:6025-6043, 6960-6993` and `rpl_binlog_sender.cc:1149`.
- Found: recovery clears the flag, and the sender always clears it on the wire.
- Conclusion: the oracle needs no IN_USE masking for recovered files.

## Evaluation refinements (2026-10-13)

Applied by the evaluation synthesis (`../evaluation/synthesis.md`). Where this section and earlier text in this file disagree, this section wins; the catalog entry has been updated to match.

- **Priority P1 → P0.** A source SIGKILL at a random instant is natively random and high-fit, needs no custom shim and no deferred template, and reaches both the unterminated-binlog branch (`collector_context.cpp:514-553`) and lead (b) (debug abort at `storage.cpp:138`) with planned faults alone (antithesis-fit F7). It takes the P0 slot vacated by `source-swap-never-silently-accepted`.
- Lead (b) wedges a debug-build timeline (abort on every retry); containment is bias B3.
- Runs in `fetch_fs`, `pull_fs`, `s3`.
