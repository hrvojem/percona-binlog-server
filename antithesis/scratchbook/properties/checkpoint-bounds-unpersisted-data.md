# checkpoint-bounds-unpersisted-data — With checkpointing on, received data is persisted within a bounded size and time

## What led here

Evaluation gap G3 (wildcard W6, coverage-balance F11). SG8 is the recovery-point guarantee: README:619 and :626 say the storage writes received data "permanently" once `checkpoint_size` bytes have arrived or `checkpoint_interval` has elapsed. No property asserts it. Its best check, the SUT `ALWAYS` on the event buffer, sits in `collector-memory-bounded-by-checkpoint`, which is P2 and blocked on whether Antithesis honours `mem_limit`. This property splits the durability half out. It needs no memory limit.

Ticket 2 (fixed upstream in 3804c25) is the closed-bug anchor: the checkpoint settings were ignored, and the ticket's expected-behaviour text says "With checkpoint_size = S, committed data not yet on the backend never exceeds about S plus one transaction. With checkpoint_interval = T, buffered complete transactions are flushed on the first event after T has passed" (`.tickets-snapshot.txt:179`). Its regression tests are the Hegel `StorageRespectsCheckpointSize{,OnS3}` (`tests/storage_property_test.cpp:1986, 2020`), which drive the storage model without a network, kills or reconnects, and `extra/qa/repro_broken_checkpointing.py`, which compares the source position with the on-disk size (not run in CI).

## Code paths

- `src/binsrv/storage.cpp:143-202` `write_event`: appends to `event_buffer_`. At a transaction boundary it advances `last_transaction_boundary_position_in_event_buffer_`. Then, if `has_event_data_to_flush()`, it flushes when `ready_to_flush_position >= last_checkpoint_position_ + checkpoint_size_bytes_` **or** `now >= last_checkpoint_timestamp_ + checkpoint_interval_seconds_` (`:179-201`). Both are evaluated **only inside `write_event`**, that is, only when an event arrives.
- `:204-215` `close_binlog`: flush at rotation, then `update_last_checkpoint_info()`.
- `:118-138` `open_binlog`: `update_last_checkpoint_info()` resets the size and time baselines for the new file (`:263-270`). So `last_checkpoint_position_` never exceeds the flushed position, and the size bound is per file.
- `src/operations/collector_context.cpp:189-203` case (2): when `fetch_binlog_event` returns false (a read timeout or a connection error), the collector discards the incomplete transaction and calls `flush_event_buffer()`. This is the "third kind of checkpointing" and the only time-driven flush that needs no new event. It does **not** call `update_last_checkpoint_info`, so the next size checkpoint fires earlier, which is conservative.
- `collector_context.cpp:180-187` case (3): fetch (non-blocking) at EOF returns true **without** flushing. The data reaches the backend only in `~storage` (`storage.cpp:68-77`), the N8 window owned by `graceful-exit-leaves-flushed-storage`.
- `src/easymysql/connection.cpp:185-205`: the dump uses `MYSQL_RPL_SKIP_HEARTBEAT`. PBS never sets `@master_heartbeat_period`, so the source sends no heartbeats, and `MYSQL_OPT_READ_TIMEOUT` (`:301-303`) does expire on a quiet source.
- S3: each flush re-uploads the whole tail object (`s3_storage_backend.cpp:850-863, 933-938`, README:670-671, ticket 17). The time a flush takes therefore grows with the binlog size. That term belongs in any wall-clock bound.

## The bound, as derived from code

For a transaction X whose last event PBS receives at time `a`, with PBS connected and the backend healthy:

- **Size only** (`checkpoint_size = S`): X is persisted no later than when at most `S` further complete-transaction bytes have been received after X's boundary. More precisely, after every `write_event` returns, the unflushed complete-transaction backlog is less than `S` (`ready_to_flush_position − flushed_position < S`). X itself may exceed `S`. There is no time bound: with no further events, X waits for the read-timeout flush.
- **Interval** (`checkpoint_interval = i`): the flush happens at the first event arriving at or after `last_checkpoint + i`. If the source keeps sending with inter-event gaps below `read_timeout = r`, X is persisted by about `a + i + gap`. If the source goes quiet, X is persisted at `last_event + r` (the case (2) flush), regardless of `i`. So the real bound is about `a + max(i + gap_max, r)` plus the backend write time, not `a + i` as README:626 reads.
- **Neither set (the default):** there is no bound until rotation, a timeout disconnect or shutdown (LG3). This is outside the property and documented by `collector-memory-bounded-by-checkpoint`'s `Sometimes`.

A kill loses only what is not yet persisted. Metadata is saved after the data (`storage_core.cpp:299-322`), and kills do not drop page cache (ticket 19 is excluded). So "persisted by `a + B`" is the same as "survives a SIGKILL at any time after `a + B`".

## Failure scenarios

1. **Regression of the checkpoint logic** (ticket 2 class): a refactor drops the config read again, or resets `last_checkpoint_position_` wrongly at rotation or reconnect. For example, if `update_last_checkpoint_info` ran before `open_binlog` set the new file's position, `last_checkpoint_position_` would hold the old file's size and the first size checkpoint of the new file would be postponed by that amount. Today the order is correct (`:118-131`). The SUT `ALWAYS` catches any such slip on the paths Antithesis reaches: reconnect reusing an open binlog, rotation during catch-up, rewrite-mode local rotation at `file_size`.
2. **Interval expectation gap:** an operator sets `checkpoint_interval=30s` and a quiet period follows a burst. The last transactions stay in RAM for up to `read_timeout` (60 s in the README sample, `:490, 519`), not 30 s. A kill in that window loses them. Whether this is a bug or a documentation gap is a contract question.
3. **S3 slow flush:** with whole-object re-uploads, a flush of a large tail under S3 latency can take longer than `i`. The bound then stretches by the upload time, and a kill during the upload loses the whole batch. The property's δ must account for this. The `Always` is evaluated only in quiet windows with faults stopped, so latency faults do not count against PBS.

## Oracle

- **SUT-side (missing):** `ALWAYS("unflushed complete-transaction bytes stay below checkpoint_size")` at the end of `storage::write_event` when `size_checkpointing_enabled()`: `get_ready_to_flush_position() - get_flushed_position() < checkpoint_size_bytes_`. This is the backlog form, not the buffer-size form. `collector-memory-bounded-by-checkpoint` keeps its own `event_buffer_.size()` `ALWAYS` (the memory bound, which includes the open transaction). The two messages are distinct, and this property owns the backlog one.
- **SUT-side anchor (missing):** `SOMETIMES("a flush wrote complete transactions older than checkpoint_interval")` in `flush_event_buffer_internal`, evaluated when interval checkpointing is on, using a timestamp recorded when each batch became ready. This marks the LG3 gap state as reached. It is an anchor, not a violation.
- **Workload, bounded `Always`:** "with checkpointing on, a marker transaction is persisted within the configured bound". It is evaluated by a new driver in a quiet window:
  1. Call `ANTITHESIS_STOP_FAULTS` for a window long enough for the check. Clear any `s3_fault_proxy` rules.
  2. Wait until PBS has caught up (stored tail position equals the source's, observed through `<binlog>.json` on the filesystem or through boto3 on S3). If it does not catch up within the window, skip; convergence is `pull-chain-converges-after-heal`'s job.
  3. Commit a marker transaction at `t` (a GTID recorded, or the source position after it). Then pick a follow-up pattern from Antithesis randomness: (A) quiet; (B) a steady filler stream with gaps `g < read_timeout`; (C) for size-only configs, filler totalling at least `checkpoint_size` bytes.
  4. Optionally SIGKILL PBS at `t + B + ε`. Then read the persisted metadata (`.json` `size`/`added_gtids`) and assert that the marker is included. `B` = (A) `read_timeout + δ`; (B) `checkpoint_interval + g + δ`; (C) the time the filler took to commit, plus δ. δ covers the source→PBS transfer and the backend write (on S3, the whole-tail PUT at the measured throughput).
- This is a fetch-free check. It applies to `pull` (pull_fs and the pull-like `s3` arm). For `fetch`, which reaches EOF quickly and then flushes in `~storage`, only the SUT `ALWAYS` applies.

## SUT instrumentation suggestions

- `ALWAYS` backlog check above — **missing**.
- `SOMETIMES` stale-batch anchor above — **missing**.
- `REACHABLE("read-timeout disconnect flushed buffered complete transactions")` at `collector_context.cpp:200` when the flush wrote more than 0 bytes. **Missing**. It may overlap `recovery-paths-exercised`'s "stop by read_timeout" marker. If that marker already sits at this line, reference it instead of adding a second one.

## Open questions

- **Is the interval contract "flushed within `checkpoint_interval`" or "flushed on the first event after `checkpoint_interval`"?** README:626 reads as the former. Ticket 2's expected-behaviour text (QA-authored) states the latter. If the former is intended, scenario 2 is a bug and `B` for pattern (A) should be `i + δ`, which fails today whenever `i < read_timeout`. If the latter, `B = max(i + g, r) + δ` and the property is a regression guard. `(needs human input)`
- **Should PBS set a heartbeat period, or flush on a local timer instead of only on event arrival?** This is a design question. If maintainers add a timer flush, the bound tightens to `i + δ` and the property's pattern (A) bound should follow. `(needs human input)`

## Resolved during investigation (see Investigation Log)

- **The "+ one transaction" slack is per transaction, and it does not change the oracle.** `write_event` flushes in the same call in which a transaction boundary pushes the complete backlog to `≥ S`, so after every `write_event` returns the complete-transaction backlog is `< S`; the "+ one transaction" in ticket 2's text is the open (incomplete) transaction, which is never flushed early. The backlog `ALWAYS` and pattern (C) use complete transactions only, so no extra slack is needed there. A single transaction larger than `S` is flushed at its own boundary. Only the memory bound (`collector-memory-bounded-by-checkpoint`) must add the largest transaction.
- **The effective read timeout is 1× `connection.read_timeout`** in both the 9.7.1 and the 8.0.35 client sources. `B` for pattern (A) is `read_timeout + δ`. README:719 (graceful-termination delay "`<connection.read_timeout>` seconds + 1") agrees with 1×.

### Investigation Log

#### Does the effective read timeout equal `connection.read_timeout`, or three times it?

- Examined: `src/easymysql/connection.cpp:301-303` (sets `MYSQL_OPT_READ_TIMEOUT` from the config); MySQL 9.7.1 client source at `/Users/hrvoje/work/percona-xtrabackup/sql-common/net_serv.cc:315-333` (`net_should_retry`) and `vio/viosocket.cc:445` (`vio_should_retry` is `errno == EINTR` only).
- Found: in the client library (`#ifndef MYSQL_SERVER`), only interrupted I/O (EINTR) is retried. A timed-out read is not retried, so one `read_timeout` expiry ends `mysql_binlog_fetch`. The "three times" wording in the C API docs reflects older behaviour.
- Not found: the same check against the `libmysqlclient21` (8.0.x) the PBS image links. That code path has been stable, but it was not read for 8.0.
- Conclusion: `(partial: 1× read_timeout in the 9.7.1 client source; 8.0 libmysqlclient21 not checked)`. Until it is checked, the workload should use `B = read_timeout + δ` and record the observed flush delay in assertion details, so a 3× library would show up as a clear, explainable failure.

(Second pass, this investigation.)
- Examined: `percona-xtrabackup` `origin/8.0` (MYSQL_VERSION 8.0.35) `sql-common/net_serv.cc:307-326` (`net_should_retry`) and `vio/viosocket.cc:423, 435` (`vio_should_retry` is `SOCKET_EINTR` only; `vio_was_timeout` is `SOCKET_ETIMEDOUT`); README:719.
- Found: identical to 9.7.1: in the client library (`#ifndef MYSQL_SERVER`) only EINTR is retried, so a read that times out ends `mysql_binlog_fetch` after one `read_timeout`. The README's own shutdown-latency statement assumes 1×. The PBS image links Ubuntu 24.04's `libmysqlclient-dev` (8.0.x, deployment-topology §pbs); this retry logic is the same across 8.0.
- Conclusion: resolved. 1× `read_timeout`.

#### Is the "+ one transaction" slack in the size bound per transaction or per event?

- Examined: `storage.cpp:143-202` (`write_event`), `:263-270` (`update_last_checkpoint_info`), `:272-300` (`flush_event_buffer_internal` writes only up to `last_transaction_boundary_position_in_event_buffer_`), ticket 2 expected behaviour (`.tickets-snapshot.txt:179`).
- Found: the size check uses `ready_to_flush_position` (end of the last complete transaction) and, when it fires, sets `last_checkpoint_position_ = ready_to_flush_position`, which equals the flushed position. The read-timeout flush (`collector_context.cpp:189-203`) does not move `last_checkpoint_position_` forward, which only makes the next check fire earlier. So after every `write_event`, complete backlog `< S`. The incomplete transaction is buffered separately and never flushed by a checkpoint. "About S plus one transaction" therefore means "complete backlog < S, plus the transaction in flight".
- Conclusion: resolved. Per transaction; oracle unchanged (complete-transaction backlog only).

#### Is the interval contract "flushed within `checkpoint_interval`" or "flushed on the first event after `checkpoint_interval`"?

- Examined: README:619, :626 ("after achieving which backend storage should flush ... permanently"), ticket 2 text (`.tickets-snapshot.txt:179`, QA-authored, fixed in 3804c25), `storage.cpp:179-201` (interval checked only inside `write_event`), `collector_context.cpp:189-203` (read-timeout flush).
- Found: the code implements "first event after the interval" plus a read-timeout flush. The README wording is ambiguous and can be read as a wall-clock bound. The fix commit restored config reading only; it did not add a timer.
- Not found: a maintainer statement of which reading is intended.
- Conclusion: `(needs human input)`. The workload uses `B = max(i + g, r) + δ` (the code's behaviour) and records the observed delay, so either answer can be applied by changing `B`.

#### Should PBS set a heartbeat period, or flush on a local timer instead of only on event arrival?

- Examined: `connection.cpp:185-205` (`MYSQL_RPL_SKIP_HEARTBEAT`; no `@master_heartbeat_period`/`@source_heartbeat_period` SET issued by the collector), ticket 20 text (`.tickets-snapshot.txt:1101-1133`, about the pull listener (serving side) not honouring heartbeat SETs or sending heartbeats).
- Found: no timer flush and no heartbeat request in the collector; no ticket asks for one on the collector side.
- Conclusion: `(needs human input)` — a design choice for maintainers.
