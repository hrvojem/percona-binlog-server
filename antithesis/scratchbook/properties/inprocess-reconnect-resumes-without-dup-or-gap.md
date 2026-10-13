# inprocess-reconnect-resumes-without-dup-or-gap — pull's in-process reconnect resumes with an empty buffer and no duplicate or gap

Found by: focus 3 (failure recovery), focus 9 (idempotency/replay)
Merged from: inprocess-reconnect-resumes-without-dup-or-gap (focus 3), gtid-reconnect-with-empty-event-buffer (focus 9)

## Synthesis notes

Safety, **P1** (both contributors rated it P1).

Both contributors proposed the same SUT check at different points. Keep both, as two distinct messages:

- `ALWAYS(current_position == flushed_position, "after disconnect cleanup the event buffer is empty")`, placed after `collector_context.cpp:200`.
- `ALWAYS(!has_event_data_to_flush(), "GTID dump request is sent with no buffered transaction")`, placed right before `switch_to_gtid_replication` (`collector_context.cpp:249`).

Two anchors go with them:

- `SOMETIMES(pull resumed into its still-open binlog after reconnect)` at `:535`.
- `REACHABLE("pull collector saw EOF from a blocking dump")` at `:184-187`. Server source confirms that this path is reachable (see Investigation findings).

Workload `Always(storage_holds_prefixes and GTIDs unique)` is checked per quiet period.

Triggers:

- partitions, resets, source SIGKILL and `KILL CONNECTION` of the dump thread on PBS<->source (the 2013 path, case 2)
- graceful source shutdown (SQL `SHUTDOWN`) and `KILL QUERY` of the dump thread while pull is mid-file with checkpointing off (EOF, case 3). Same-server_id eviction is *not* an EOF trigger: it yields error 1236 and pull exits (see Investigation findings).

GTID mode resumes from flushed bytes only, while position mode resumes from flushed + buffered. That asymmetry is the risk.

## What led here

### Focus 3: in-process reconnect

- LG1 (README:378: "another reconnection attempt is made") plus SG1. Unlike a process restart, an in-process reconnect **keeps the storage binlog open and the facade state alive**: the buffer, the checkpoint counters, `last_sequence_number`, and in rewrite mode the renumberer.
- Resume correctness therefore depends on the cleanup in `collector_context.cpp:189-204`. That cleanup must leave the facade exactly at a transaction boundary that matches the persisted metadata.
- This path runs only in `pull` (blocking mode). It also runs only for a mid-stream `CR_SERVER_LOST` (2013, `connection.cpp:546-551`) or a `read_timeout`; any other error ends the process (N7).
- Antithesis partitions, resets and source restarts exercise exactly this path. Existing harnesses touch it only through Toxiproxy (`network_fault_test.py`), with no byte-level checks.

### Focus 9: GTID resume from flushed state only

The two modes derive the resume point from different state:

- Position mode resumes at `flushed + buffered` (`storage.hpp:81-83`).
- GTID mode requests `previous ∪ added` from the core. That covers **flushed bytes only** (`storage_core.hpp:313-333`).

A GTID-mode reconnect can happen while `event_buffer_` still holds complete transactions. The request then asks the source for transactions that are already buffered, and the source re-sends them. They are appended to the buffer a second time, so the collector would write `T T`: the first copy from the buffer, the second from the replay.

The reader checks the first replayed GTID event by comparing the source's `next_event_position` with `position_ = flushed + buffered`. That check *should* trip in plain GTID mode. It does not trip in rewrite mode.

## Code paths

### Disconnect handling

- `receive_binlog_events` case (2) (`collector_context.cpp:189-204`, discard then flush at `:195-200`) calls `storage_->discard_incomplete_transaction_events()` (`storage.cpp:217-224`), then `storage_->flush_event_buffer()`.
- Case (3) covers `fetch_result == true` with an empty portion (EOF packet). `receive_binlog_events` returns true **without flushing** (`collector_context.cpp:184-187`). libmysqlclient maps the EOF packet to `rpl.size == 0` (`easymysql/connection.cpp:172-179`).

### Retry loop

- `pull_operation.cpp:133-153` (loop body `:136-152`) sleeps a fixed `idle_time` (`wait_for_interruptable`, `:56-76`), then calls `receive_binlog_events` again.
- There is no backoff and no retry limit.

### Resume request

- The request is built in `collector_context.cpp:207-272`.
- **Position mode** sends `(get_current_binlog_name(), get_current_position())`. `get_current_position()` = flushed + `size(event_buffer_)` (`storage.hpp:81-82`), so it equals flushed only if the buffer is empty after the flush.
- **GTID mode** sends `get_gtids()`, which covers flushed GTIDs only (`storage_core.hpp:313-333`). The GTID dump request is sent at `switch_to_gtid_replication` (`collector_context.cpp:249`).
- `has_event_data_to_flush()` is at `storage.hpp:165`. It is not public.

### Reusing the open binlog

- `process_artificial_rotate_event` is at `collector_context.cpp:505-540`.
- Same name: "reused already open binlog file" (`:535`). In position mode the rotate position is checked against `get_current_position()` (`:522-533`). **GTID mode has no equivalent check.**
- Different name: "closed binlog file left open" (`:541-553`), then a new open.

### Other relevant code

- `open_binlog` refuses older names: "cannot open an existing binlog that is not the latest one" (`storage_core.cpp:275-279`).
- Each reconnect cycle builds a fresh `reader_context` (`collector_context.cpp:142-146`).
- Reader preamble handling: when resuming into a non-empty file, the FDE and PREVIOUS_GTIDS events are info-only (`reader_context.cpp:579-582`).

## When can the buffer be non-empty at reconnect?

- **Case (2), timeout or CR_SERVER_LOST.** Discard then flush leaves the buffer empty. This is safe unless the flush throws, in which case the process exits.
- **Case (3), EOF packet.** In fetch, the process then exits and `~storage` flushes.
  - In **pull**, the retry loop idles and calls `receive_binlog_events` again with the buffer intact.
  - With checkpointing off (the default), that buffer can hold the whole current file.
- **Verified (see Investigation findings):** a blocking (pull) dump receives an EOF packet on a graceful source shutdown and on `KILL QUERY` of the dump thread, possibly mid-transaction. `KILL CONNECTION` gives 2013, and duplicate-`server_id` eviction gives error 1236.

## Failure scenarios to explore

1. **Flush throws during disconnect cleanup** (focus 3; correlated fault). The flush in case (2) throws because the S3 link is partitioned at the same moment. The exception ends pull, `~storage` re-flushes, and ticket 18's duplicate follows (see `failed-flush-never-reappends-data`, which absorbed `failed-flush-retry-never-duplicates`). Correlated network faults on the source link and the S3 link make this realistic.
2. **Complete transactions after an incomplete one at disconnect** (focus 3). This cannot happen, because transaction boundaries are monotonic. The discard relies on `last_transaction_boundary_position_in_event_buffer_` being the last boundary, which is worth an SUT `ALWAYS`.
3. **GTID resume starts at an older file** (focus 3). The first missing GTID is earlier than the tail, so the source starts from an older file.
   - The "closed binlog file left open" path closes the tail and tries to open the *older* name.
   - `open_binlog` rejects it ("cannot open an existing binlog that is not the latest one", `storage_core.cpp:275-279`), and pull dies.
   - Whether this is reachable depends on GTID gaps.
4. **Flapping network** (focus 3). Many fast reconnects with a small `idle_time`. This checks two things:
   - each cycle's `reader_context` starts at the right position;
   - preamble handling that depends on `cycle_number_` is per connection.
5. **EOF from a blocking dump with a non-empty buffer** (focus 9; case 3).
   - Triggers: graceful source shutdown or `KILL QUERY` of the dump thread while pull is mid-file with checkpointing off (confirmed in server source; see Investigation findings).
   - In GTID mode, the resume request omits the buffered transactions and the source replays them.
   - In plain GTID mode, the reader's position check trips and pull fail-stops; `~storage` flushes the buffered complete transactions, so a restart recovers (not a permanent wedge).
   - In rewrite mode, the duplicate is silent.

## Oracle / workload checks

- Run the pull workload under source-link partitions and resets (2013 path), and under graceful source shutdown and `KILL QUERY` of the dump thread (EOF path). Same-server_id eviction (1236, pull exits) and `KILL CONNECTION` (2013) are useful negative/other-path triggers.
- After each quiet period, check `Always("stored binlogs are source byte prefixes with no transaction stored twice after in-process reconnects")`.
  - It uses `storage_holds_prefixes()` plus a GTID-uniqueness scan.
  - The byte-prefix part applies to non-rewrite, unencrypted storage.
- Check `Sometimes("pull performed an in-process reconnect and then stored a newer transaction")`. The evidence is the log line "awoke after sleeping and trying to reconnect", followed by storage growth.

## SUT-side instrumentation

The codebase has no Antithesis SDK, so every assertion below is **missing**.

| Assertion | Site | Proposed by | Status |
|---|---|---|---|
| `ALWAYS(storage_->get_current_position() == storage_->get_flushed_position(), "after disconnect cleanup the event buffer is empty")` | right after the flush at `collector_context.cpp:200` | focus 3 | missing |
| `ALWAYS(!has_event_data_to_flush(), "GTID dump request is sent with no buffered transaction")` | `open_connection_and_switch_to_replication` just before `switch_to_gtid_replication` (`collector_context.cpp:249`) | focus 9 | missing; needs a public accessor for `has_event_data_to_flush()` (`storage.hpp:165`) |
| `SOMETIMES(binlog_reused_after_reconnect, "pull resumed into its still-open binlog after reconnect")` | `collector_context.cpp:535` | focus 3 | missing |
| `REACHABLE("pull collector saw EOF from a blocking dump")` (was `SOMETIMES(cond)`) | `collector_context.cpp:184-187`, blocking mode only | focus 9 | missing |
| `ALWAYS(storage_->get_current_position() == storage_->get_flushed_position(), "pull EOF from a blocking dump leaves the event buffer empty before reconnect")` | `collector_context.cpp:184-187`, blocking mode | investigation | missing; expected to fail today |
| `ALWAYS(!storage_->get_gtids().contains(gtid), "pull reconnect: received GTID is not already in storage")` | GTID mode, on receive | focus 3 | missing |

A non-empty buffer after cleanup means the position-mode resume position includes unflushed bytes. The received-GTID check overlaps the transaction-boundary `ALWAYS` owned by `stored-transactions-exactly-once-across-resumes` (absorbed `restart-after-kill-converges-to-source`). If both are added, keep the distinct message above, or reference that one instead.

## Fault requirements

- Network partitions and resets on PBS<->source.
- Source shutdown or restart.
- Workload-driven dump-thread `KILL QUERY` (EOF) and `KILL CONNECTION` (2013); graceful source `SHUTDOWN` via the restart-loop wrapper (EOF). Same-server_id eviction gives 1236 and ends pull.
- Correlated PBS<->S3 faults for scenario 1.
- No node termination needed.

## Investigation findings (2026-10-13) and property changes

The open questions below were resolved against MySQL 9.7.1 server and client source (`/Users/hrvoje/work/percona-xtrabackup`, MYSQL_VERSION 9.7.1). 8.0/8.4 are assumed to behave the same, since the code is long-standing and MySQL's own replica handles the same case. The `REACHABLE("pull collector saw EOF from a blocking dump")` anchor confirms this dynamically on each version.

**Errnos under faults.** Every transport-level read failure reaches PBS as `CR_SERVER_LOST` (2013), so it takes case (2) and the in-process reconnect. That covers a partition plus `read_timeout`, a TCP RST, a source SIGKILL, and `KILL CONNECTION` of the dump thread. Only error packets the server sends differ: 1236 for duplicate-`server_id` eviction, purged binlogs or a position past the end of the file. Those end pull. The path is therefore well exercised by Antithesis network faults, and no supervisor-only fallback is needed.

**EOF to a blocking dump is real and common.** The dump thread ends in `Binlog_sender::cleanup()`, which calls `my_eof()` whenever no fatal error was set, and that EOF packet is sent. Two common actions trigger it:
- a graceful source shutdown (SQL `SHUTDOWN`, SIGTERM, an upgrade restart);
- `KILL QUERY` of the dump thread.

The deployment's mysqld restart-loop wrapper already performs SQL `SHUTDOWN`. The sender checks `killed` before every event (`rpl_binlog_sender.cc` `send_events`), so the EOF can arrive **mid-transaction**.

In pull, case (3) returns `true` without discard or flush (`collector_context.cpp:184-187`). `pull_operation.cpp:136-152` then idles and reconnects with the buffer intact. That violates the comment at `collector_context.cpp:174-176` ("can only happen in fetch"). The consequences by mode:

- **Position mode, buffer at a transaction boundary:** the resume is correct, because it requests flushed + buffered.
- **Position mode, partial transaction in the buffer:** the resume starts mid-transaction. The reader raises "transaction group must start from one of the gtid log events" (`reader_context.cpp:775-778`) and pull fail-stops. `~storage` flushes only up to the boundary, so the data is safe.
- **Plain GTID mode, buffered complete transactions:** the request omits them, and the source re-sends them. The first re-sent GTID trips "unexpected next event position in the event common header" (`reader_context.cpp:908-921`), and pull fail-stops. `~storage` flushes the buffered complete transactions, so a supervisor restart recovers. This is a crash-and-recover, **not a permanent wedge** as the earlier text said.
- **Rewrite mode:** the reader positions come from the storage, so nothing trips. The re-sent transactions are appended after the buffered ones, which gives silent duplicates. If the buffer held a partial transaction, its bytes stay ahead of the replay and are flushed with the next boundary. This is code-derived and not yet observed.

**Changes to this property:**
- It stays **P1**, and the GTID buffered-transaction part is no longer latent-only.
- The triggers change: graceful source shutdown and `KILL QUERY` of the dump thread lead to EOF and case (3). Partitions, RST, source SIGKILL and `KILL CONNECTION` lead to 2013 and case (2).
- Duplicate-`server_id` eviction is **not** an EOF trigger. It gives a 1236 error packet and pull exits.
- `ALWAYS("GTID dump request is sent with no buffered transaction")` is **expected to fail today** in pull after a graceful source shutdown. It becomes a regression guard after the fix.
- Add `ALWAYS(storage_->get_current_position() == storage_->get_flushed_position(), "pull EOF from a blocking dump leaves the event buffer empty before reconnect")` at `collector_context.cpp:184-187`, as a distinct message from the case (2) one.
- `REACHABLE("pull collector saw EOF from a blocking dump")` replaces the `SOMETIMES(cond)` anchor, since the branch itself is the event. It is missing, as is everything in existing-assertions.md.

This is a validated defect candidate for a new ticket (draft only).

## Open questions

- None.

### Investigation Log

#### Which errnos do Antithesis faults actually produce mid-stream?

- Examined: the PBS side, `easymysql/connection.cpp:536-551` (only `CR_SERVER_LOST` is retried) and `collector_context.cpp:189-204`. The MySQL 9.7.1 client, `sql-common/client.cc:1199-1222` (`cli_safe_read_with_ok_complete`) and `:7578-7612` (`mysql_binlog_fetch`). The server: `sql/sql_class.cc:1636-1666` (`THD::awake`), `sql/rpl_source.cc:1094-1170` (zombie dump thread eviction), `sql/rpl_binlog_sender.cc:366-384, 464-494`, and `sql/mysqld.cc:2311-2370, 2427-2500` (shutdown).
- Found: any `len == packet_error || len == 0` read maps to `CR_SERVER_LOST` unless the error is `ER_NET_PACKET_TOO_LARGE`. The `ER_NET_READ_INTERRUPTED` early return is `#ifdef MYSQL_SERVER` only, so it is not in libmysqlclient, and a `read_timeout` therefore gives 2013. `KILL CONNECTION` calls `shutdown_active_vio()` before the dump thread can send anything, so the client sees a closed socket, which is 2013. Duplicate-`server_id` eviction sets `duplicate_slave_id`, then `awake(KILL_QUERY)` and `set_fatal_error`. The client receives a 1236 error packet, so pull exits. PBS sets no replica UUID, so eviction matches on `server_id` (`Find_zombie_dump_thread`).
- Not found: dynamic confirmation on 8.0/8.4.
- Conclusion: resolved. Network faults and source SIGKILL take the 2013 reconnect path.

#### Does mysqld send an EOF packet to a blocking COM_BINLOG_DUMP[_GTID] client on shutdown or KILL?

- Examined: `rpl_binlog_sender.cc:366-384` (`cleanup` ends with `my_eof(thd)` when `!has_error()`), `:400-419` (a kill breaks the loop with no fatal error), and the per-event `killed` check in `send_events`. Also `sql_parse.cc:2462-2463`: `send_kill_message` is skipped because the DA is already set, and `send_statement_status` sends the EOF. Also `mysqld.cc:2323-2368`: on shutdown, `Set_kill_conn` sets `KILL_CONNECTION` and broadcasts the condition without shutting down the vio, so the EOF is written to a live socket. Finally `rpl_replica.cc:4319-4324`: MySQL's own replica handles `rpl->size == 0` and logs ER_RPL_REPLICA_DUMP_THREAD_KILLED_BY_SOURCE, "Dump threads are killed for example during source shutdown, explicitly by a user, or ... duplicate server UUID".
- Found: yes, for a graceful shutdown and for `KILL QUERY`. It does not happen for `KILL CONNECTION` (2013) or for duplicate eviction (1236). The EOF can follow a partial transaction.
- Conclusion: resolved. The GTID buffered-transaction risk is reachable and P1.

#### Should case (3) in pull also call `flush_event_buffer()`, as case (2) does?

- Examined: the comments at `collector_context.cpp:170-176` and `:189-200`.
- Found: case (3) is commented as fetch-only, an assumption the server code above refutes. The authors added the discard+flush in case (2) precisely so that "the persisted stream offset matches a transaction boundary" on reconnect, and the same reasoning applies to the EOF case in pull. Either fix keeps the proposed assertions valid: discard+flush on EOF, or treating EOF as fatal in pull.
- Conclusion: resolved. The GTID-request assertion fails today and should pass after any fix. The maintainers choose the fix, but that choice does not change the property.
