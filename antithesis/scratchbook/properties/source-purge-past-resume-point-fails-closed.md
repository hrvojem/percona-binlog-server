# source-purge-past-resume-point-fails-closed

## What led to it

Evaluation gap G5 (coverage-balance F5, wildcard W13). The catalog assumes "the source retains its binlogs for the whole run", and `stored-transactions-exactly-once-across-resumes` explicitly excludes runs where the source purged past PBS's position. sut-analysis "Failure & degradation modes" says a purged source binlog produces a crash loop, and that a property should assert "exits non-zero and storage still opens". No property does.

The scenario is routine in production. PBS is down or lagging (maintenance, a long partition, a crash loop from another bug) for longer than `binlog_expire_logs_seconds`, or an operator runs `PURGE BINARY LOGS` on the source without checking the binlog server.

The QA script `extra/qa/source_change_test.py` scenario `purge` (`:163-196`) covers only the empty-storage case: the source purges before PBS ever ran, and the expected outcome there is "start from the first remaining binlog" (ticket 25 text, snapshot line 1373). The non-empty, behind-retention case has no test.

## Code paths (this repo)

- Resume request: GTID set `previous_gtids ∪ added_gtids` of the tail (`src/operations/collector_context.cpp:227-251`), or (tail name, tail size) in position mode (`:253-261`). On an **empty** GTID storage PBS reads `@@GLOBAL.gtid_purged` and starts after it (`:229-240`). That is by design: "start from what is available" applies only to an empty storage.
- `connection::switch_to_*_replication` → `mysql_binlog_open` (`src/easymysql/connection.cpp:135-138, 159-162`). libmysqlclient sends COM_BINLOG_DUMP[_GTID] with `simple_command(..., skip_check=1)` (PXC tree `sql-common/client.cc:~7649-7756`), so it does not read the server's reply. The server's refusal therefore surfaces at the **first** `mysql_binlog_fetch`, not in the switch's `catch` that returns `false` and retries (`collector_context.cpp:263-267`).
- `connection::fetch_binlog_event` raises `core_error` for any errno other than `CR_SERVER_LOST` (`connection.cpp:536-551`). 1236 is not 2013, so the exception propagates out of `receive_binlog_events`.
  - fetch: caught in the operation thread, rethrown, `handle_std_exception` logs `"std::exception caught: cannot fetch binlog event: <server message>"`, and the run exits non-zero (`src/operations/fetch_operation.cpp:84-120`, `src/binsrv/exception_handling_helpers.cpp:52-71`).
  - pull: the same exception ends the operation thread, `io_ctx.stop()` takes down the replication source too, and the process exits non-zero (`src/operations/pull_operation.cpp:125-165`). The supervisor restarts it `idle` seconds later. That is the crash loop.
- Empirical confirmation of the error path (different trigger, same mechanism): the ticket 18 run in the snapshot (line 992) shows "Every later fetch exits with 1: cannot fetch binlog event: Client requested source to start replication from position > file size: CR_1236".

Server side (PXC tree `sql/rpl_binlog_sender.cc`):
- Position mode, tail binlog purged: `find_log_pos` fails → "Could not find first log file name in binary log index file" (`:986-990`).
- GTID mode, `gtid_purged ⊄ requested` → `report_missing_purged_gtids` (`:946-951`).
- Both are set with `set_fatal_error` in `init()`, before any event, so PBS receives nothing before the error.
- A PURGE never removes a file in use by a live dump thread. A purge past PBS's position therefore bites only at the **next reconnect**: pull's in-process reconnect after a 2013, a pull restart, or the next fetch.

## Failure scenario (what could go wrong)

The expected code-level outcome is fail-closed: no gap, no bytes appended. The property is a regression guard plus a check that the fail-closed state is clean:

1. **Silent skip (regression):** a future change that resumes from "the first available binlog" when the requested one is gone. Examples: a convenience fallback, a ticket 25 fix that re-reads `gtid_purged` on non-empty storages, or reusing the empty-storage path after a tail truncation that empties the index. The storage then has a gap that every byte-prefix oracle misses, because they compare only against binlogs the source still has.
2. **Storage drift during the crash loop:** each restart runs the streaming constructor, which does tail recovery truncation and `.tmp` cleanup (`storage_core.cpp:597-630, 960-985`). On S3 the restart re-downloads the tail. Repeated restarts must leave the storage byte-identical after the first one. A kill landing during recovery is covered by `kill-anywhere-storage-reopens`; here the loop must be idempotent.
3. **Spurious refusal (position mode only, liveness):** PBS stored binlog N completely, including its ROTATE, and stopped before the next file's fake ROTATE arrived (`collector_context.cpp:574-579` closes N, and the next record is created only on the next artificial ROTATE). If the source then purges N, the position resume (N, end) is refused although PBS lacks nothing. GTID mode does not have this problem. The property accepts fail-closed here too, but the oracle should classify the case ("complete tail purged") so triage does not read it as data loss.
4. **Error clarity:** the operator-facing signal is the log line. With `file_logger`, every restart truncates the log on open (`src/binsrv/file_logger.cpp:30`, sut-analysis W11), so a crash loop keeps only the latest attempt's log. The harness should log to console.

Chained variant (observation, not this property's oracle): a downstream PBS `fetch`/`pull` from an upstream PBS `pull` whose storage purged the downstream's tail gets a **silent close**, not an ERR (`network_service.cpp:305-313`; owned by `dump-session-ends-with-eof-or-err-packet`). Predicted: the downstream libmysqlclient reports 2013, the downstream fetch exits 1 with "did not reach EOF", and a downstream pull retries forever without ever failing closed with a clear error. Not run.

## Property and oracle

Driver (new): with the writer stopped (fetch_fs: writer lock; pull_fs: `pull.enabled` removed and pull SIGTERMed), the workload:
- reads PBS's tail (name/size, GTID set);
- runs `FLUSH BINARY LOGS` until the source is past it, then `PURGE BINARY LOGS TO` a file after PBS's tail;
- records `needed_purged = true` and a hash of every storage object;
- runs fetch k times (or re-enables pull and lets the supervisor loop for a random time).

Assertions:
- `Always("writer never exits 0 or appends after the source purged a binlog it still needed")`: each attempt exits non-zero, and no indexed object grows beyond the post-first-attempt snapshot.
- `Always("repeated failed resumes against a purged source leave the storage byte-identical")`: hashes are equal across attempts 2..k, and every mode opens (`list` succeeds).
- `Always("pull exits after its resume hits a purged source binlog")`: pull does not sit connected with the replication source up while never collecting. A supervisor outcome row with non-zero exit appears within `idle_time + connect_timeout + δ`. This checks that the refusal is not swallowed into the switch-retry path.
- `Sometimes("resume hit a purged source binlog")`, split by mode: position "Could not find first log file", GTID "purged required GTIDs". This is the vacuity marker.

Afterwards the driver **re-seeds** the storage (deletes it and lets the next fetch start from empty), so the permanent wedge does not end the timeline's exploration. The timeline is flagged so that the exactly-once oracles skip the pre-re-seed storage.

## SUT-side instrumentation suggestions (all missing; see existing-assertions.md)

- `connection.cpp:546-550` (non-2013 fetch error): `REACHABLE("fetch error other than CR_SERVER_LOST ends the collector")`, with the errno in details. This is a branch anchor shared with any 1236 cause. Check with `recovery-paths-exercised`, which lists "stop by exception" as a lifecycle outcome, and reference it there if that is the owner.
- `collector_context.cpp:229-240` (empty-storage `gtid_purged` bootstrap): `ALWAYS_OR_UNREACHABLE(storage_->is_empty(), "gtid_purged bootstrap only on an empty storage")`. This guards against the silent-skip regression (1).

## Open questions

- **Is a permanent fail-closed (crash loop) the intended behaviour once the source has purged past PBS, or should PBS offer an explicit "re-seed from the first available binlog" mode that records the gap?** If a re-seed mode is intended, the property needs an `Always("a re-seed after a source purge records the gap in metadata/output")`. If it is not, the README should document that the storage must be re-created, and the property stays as written. `(needs human input)`
- **Should the position-mode "complete tail purged" case (scenario 3) resume instead of failing?** If yes, it becomes a liveness bug with its own `Sometimes(resumed after complete tail was purged)`. If no, the oracle just classifies it. `(needs human input)`

### Investigation Log

#### Does the server's refusal surface at switch time (retry forever) or at the first fetch (process exits)?

- Examined: `easymysql/connection.cpp:130-180, 536-551`; PXC `sql-common/client.cc` `mysql_binlog_open` (`simple_command(..., 1)` at ~7754); `rpl_binlog_sender.cc` `init()` (`:900-1000`); `collector_context.cpp:207-272`; `pull_operation.cpp:125-165`; ticket 18 snapshot line 992.
- Found: `mysql_binlog_open` does not read the reply. The refusal is set in `Binlog_sender::init()` before any event, so it arrives at the first `mysql_binlog_fetch` and is raised as `core_error`. The ticket 18 QA output shows exactly this message path ("cannot fetch binlog event: … CR_1236") with exit 1 on every retry.
- Conclusion: resolved. fetch exits non-zero, and pull exits non-zero (supervisor crash loop). It does not retry silently in-process.

#### Is a permanent fail-closed (crash loop) the intended behaviour once the source has purged past PBS, or should there be an explicit re-seed mode?

- Examined: README operation modes (`:150-180`, the seven modes; no re-seed or "start from first available" option), README `fetch`/`pull` sections (`:371-378`: fetch terminates on any error leaving a consistent storage; pull retries network errors and terminates on "more serious errors"), config reference (no option that changes resume behaviour), `collector_context.cpp:227-261` (the `gtid_purged` bootstrap is gated on `storage_->is_empty()`), ticket 25 text (`.tickets-snapshot.txt:1373`: only the empty-storage purge case is described, as passing).
- Found: no re-seed mode exists and none is documented. The README neither says that a purged-past storage must be re-created nor classifies a server 1236 as "network-related" or "serious" for pull. Code behaviour is fail-closed (see the first log entry).
- Not found: a maintainer statement of intent.
- Conclusion: `(needs human input)`. The property stays as written (fail-closed regression guard) until maintainers say otherwise.

#### Should the position-mode "complete tail purged" case (scenario 3) resume instead of failing?

- Examined: `collector_context.cpp:253-261` (position resume always sends the stored tail name and size), `:574-579` (the next record is created only on the next artificial ROTATE), README (no statement about this case).
- Found: the code has no "tail complete, ask for the next file" path; a resume on a purged, complete tail is refused by the server like any other purged tail. Whether that is acceptable is a contract question.
- Conclusion: `(needs human input)`. The oracle classifies the case ("complete tail purged") without failing on it.
