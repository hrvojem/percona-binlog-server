# dump-session-ends-with-eof-or-err-packet — A dump request pull cannot serve is answered with a MySQL ERR packet, never a silent close

## What led to it

Protocol-contract focus: "response codes that mask errors". A MySQL source that cannot serve a COM_BINLOG_DUMP request (unknown binlog, position past the end of the file, corrupt data, I/O error) sends an ERR packet, typically 1236 `ER_SOURCE_FATAL_ERROR_READING_BINLOG`, with a reason. Clients treat the two outcomes very differently:
- ERR 1236: a MySQL replica's IO thread stops with a visible error; `mysqlbinlog` prints the server's reason and exits non-zero.
- A dropped TCP connection: the replica's IO thread treats it as a transient network error (2013) and reconnects forever, hiding the cause; `mysqlbinlog` prints "Lost connection".

Pull's replication source closes silently on every unservable request. Lead N12 in sut-analysis names one case; reading the code shows that every failure on the serving path ends the same way.

## Code paths (commit 99de682)

- `minimysql/network_service.cpp:305-313`: `if (!sender_ctx.get_event(event_data)) { log error; co_return; }`. No `generate_encoded_*error*` call precedes it. The caller (`:597-601`) sets `terminated = true`, and the session coroutine ends, which closes the socket.
- `get_event` returns false for:
  - a binlog name not in the in-memory record list (`storage_core.cpp:489-496`), including a purged file and a file pull has not yet created;
  - an offset past the flushed size (`storage_core.cpp:510-515`), which happens to a client that is "ahead" after a pull restart truncated the tail (see `pull-chain-prefix-consistency`), or a client that switched to PBS from the real source while PBS lags;
  - a non-4 position with an empty name (`sender_context.cpp:533-541`);
  - an unreadable or too-short FDE for `start_from_offset`, including ticket 26 / PBS-96 (empty storage, `sender_context.cpp:210-233`, listed in `extra/qa/nightly/known_failures.txt`);
  - a partial event at a file switch (`sender_context.cpp:413-429`) and a nonsensical event size (`:360-363`).
- Exceptions thrown on the serving path (S3 GetObject failure, short read `out_of_range` (A10), a payload deleted by an external purge while pull's index is stale (ticket 9), a frame ≥ 16 MiB (ticket 10, covered by `large-event-served-or-rejected-cleanly`)) propagate to the session's `catch (...)` at `network_service.cpp:621-623`, which only logs.
- By contrast, the non-blocking end of stream is correct: `generate_encoded_eof()` at `:331-336`.

## Failure scenario

Pull is killed after serving bytes `[S, S+n)` of the tail but before the metadata save. Recovery truncates to `S`. A blocking client reconnects at `S+n` and is closed silently, again and again, until the collector re-fetches the range. If the source has meanwhile purged it, or the collector is stuck (N7), the client loops forever without any error ever surfacing. With a real replica this is an IO thread stuck in "Connecting" with no `Last_IO_Error` explaining why.

## Oracle

- SUT-side (preferred, because Antithesis network faults make a client-side "closed without ERR" check noisy): `UNREACHABLE` at `network_service.cpp:312` with message "replication source: dump session closed without ERR or EOF packet". Add a second `UNREACHABLE` for the storage-exception path: wrap `get_event` in `handle_binlog_dump_command` in a try/catch that separates storage/logic errors from socket errors, and mark the storage-error branch.
- Workload-side (raw Python COM_BINLOG_DUMP client): when a session ends while the pull process is alive and no network fault was injected on that connection (the workload can only approximate this; use it as a `Sometimes`, not an `Always`), record whether the last packet was EOF (0xFE), ERR (0xFF) or nothing. `Sometimes(dump session ended with an ERR packet)` as the positive reachability goal once fixed (record the code; 1236 expected, not required).

## SUT-side instrumentation (missing)

- `UNREACHABLE("replication source: dump session closed without ERR or EOF packet")` at `network_service.cpp:312`.
- `UNREACHABLE("replication source: storage exception ended a dump session")` in a new catch around `sender_ctx.get_event`.
- `REACHABLE("replication source: dump request past flushed size")` at `storage_core.cpp:510-515` when called from a session, as an exploration hint for the restart-truncation interleaving.

## Antithesis angle

The unservable-request states are timing-produced: pull restart with recovery truncation after bytes were served, a client connecting before the first flush (ticket 26), an external purge while pull's index is stale (ticket 9), S3 faults during a ranged GET. Antithesis reaches these interleavings; the SUT-side assertion fires at the exact moment.

## Fault requirements

Process kill/restart of pull, network faults between pull and S3, a purge task in the workload. No clock or node-termination faults needed.

## Property changes from investigation

- **Split by request class.**
  - A request that pull will be able to serve once the collector catches up, at a position other than 4 on an empty storage (ticket 26 / PBS-96), is a **liveness** case: "eventually served, never closed". The maintainers already chose waiting for the position-4 counterpart (PBS-91, 0f56bd6), and ticket 26's expected behaviour is waiting. It is excluded from the ERR requirement and belongs with the PBS-91 polling properties.
  - Every other unservable request needs an ERR packet: an unknown binlog on a non-empty storage, an offset past the flushed size, a non-4 position with an empty name, an unreadable FDE, a partial event at a switch, a nonsensical event size, or a storage exception.
- **Error-code agnostic.** The `UNREACHABLE` assertions and the workload check accept any ERR packet. Only the positive `Sometimes` records the code. `Sometimes(dump session ended with an ERR packet)` replaces `Sometimes(dump session ended with ERR 1236)`. 1236 is logged as the expected MySQL-compatible value but is not required.
- A purge concurrent with pull (ticket 9 stale index) is out of contract (README:473). A file purged **before** pull started is still in scope as an "unknown binlog" request.
- Invariant (`Unreachable` at `network_service.cpp:312`) and priority are unchanged.

## Open questions

- For a **blocking** dump on a non-empty storage, should a request past the flushed size (a client ahead of a tail that a pull restart truncated) or for a binlog not yet created wait instead of failing? If it should wait, those cases join the liveness split above. `(needs human input)`

### Investigation Log

#### For a blocking dump, should a request past the flushed size (or for a binlog not yet created) wait, as for an empty storage (PBS-91), instead of failing?

- Examined: `storage_core.cpp:462-475` (empty-storage EOF comment), `:482-496` (unknown name, `return false`), `:507-516` (offset past size, `return false`), commit 0f56bd6 (PBS-91, Kamil Holubicki), PXC 9.6 `sql/rpl_binlog_sender.cc:980-1011` (MySQL `check_start_file`), `sql/rpl_source.cc:129`.
- Found:
  - The documented storage contract fails both cases. The PBS-91 comment says an empty storage is polled so the next fetch can "resolve an empty name once records exist or fail with 'not found' if the specific binlog never shows up". In practice the failure is immediate as soon as any record exists, even if the binlog would be created later.
  - MySQL answers both cases with a fatal error: "Could not find first log file name in binary log index file" and "Client requested source to start replication from position > file size". Both are sent as ER_SOURCE_FATAL_ERROR_READING_BINLOG (1236).
  - MySQL never meets the PBS-specific variant: a client ahead of a recovery-truncated tail, or a not-yet-created binlog while PBS lags its source.
- Not found: a maintainer decision for those PBS-specific variants.
- Conclusion: partially resolved. Today's intent is "fail", which needs an ERR. Whether the PBS-specific lag cases should wait instead is `(needs human input)`, narrowed in the list above.

#### Which error code and message should PBS use?

- Examined: PXC 9.6 `sql/rpl_source.cc:129` (every Binlog_sender fatal error goes out as `ER_SOURCE_FATAL_ERROR_READING_BINLOG`), `src/minimysql/connection_context.cpp:296-339` (`generate_encoded_error(code, message, sqlstate)` already exists; it is used for access denied, unknown command and syntax error), `network_service.cpp:296-336`.
- Found: MySQL uses 1236 with a reason text for every unservable dump. PBS already has a generic ERR encoder, so no protocol work is missing, only the call.
- Conclusion: resolved by reformulation. The property and its assertions do not depend on the code. The workload accepts any ERR and records the code; 1236 is the MySQL-compatible expectation, reported but not required.

#### Does the known_failures entry for ticket 26 mean the maintainers already consider a silent close a bug?

- Examined: `extra/qa/nightly/known_failures.txt:22`, `git log` of that file (commits 99de682, 8057f88 and de2b50a, all by the user, not by a maintainer), ticket 26 / PBS-96 text in `.tickets-snapshot.txt` ("Expected behaviour": wait, as PBS-91 does for position 4), commit 0f56bd6.
- Found: the entry was added on this branch by the ticket's reporter, not by a maintainer. It marks the disconnect as a bug, but the expected fix is waiting, not an ERR. The maintainer signal is PBS-91: they chose waiting over failing for the empty-storage case.
- Conclusion: resolved. This is no evidence about silent closes in general. Ticket 26 moves to the liveness split, and the other cases stay under the ERR requirement.

## Evaluation refinements (2026-10-13)

Applied by the evaluation synthesis (`../evaluation/synthesis.md`). Where this section and earlier text in this file disagree, this section wins; the catalog entry has been updated to match.

- **Expected to fail today.** Every `sender_ctx.get_event()` false return reaches the silent `co_return` at `network_service.cpp:308-313` without an ERR packet, including the "position past the flushed size" branch (`storage_core.cpp:513-515`, N12), which `parallel_driver_pull_reader.py` requests on purpose (antithesis-fit F1). Requests ahead of the flushed position are therefore a known-defect trigger (bias B3).
- Log-line fallback until the SDK is linked: `network_service.cpp:308-312` logs `"net    : failed to fetch next event block for {}"` at error level just before the silent `co_return`; a workload scan of pull's console log asserts `Unreachable` on that line (implementability C7).
- "A purge task" is replaced by the in-contract `pull_fs` retention-cycle driver (stop pull, purge, restart), a topology change; gap G7 covers the cycle itself.
- Runs in `pull_fs`.
