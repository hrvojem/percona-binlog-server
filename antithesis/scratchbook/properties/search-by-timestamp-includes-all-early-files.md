# search-by-timestamp-includes-all-early-files — search_by_timestamp returns every binlog that holds an event at or before the requested time

## What led to it

Protocol-contract focus: the documented JSON contract of `search_by_timestamp` (README "search_by_timestamp operation mode"): "the list of binlog files … that have at least one event whose timestamp is less or equal to the provided `<iso_timestamp>`". The implementation (`search_helpers.cpp:64-84`) does something narrower: it walks records in index order and **stops at the first record whose `min_timestamp` is greater than t** (`:75-77`). The two agree only if `min_timestamp` is non-decreasing along the index. sut-analysis A14 records this as an unproven assumption.

Ways `min_timestamp` becomes non-monotonic in practice:
- source clock steps backwards (NTP correction, VM migration, Antithesis clock fault on the source container);
- statements executed under `SET TIMESTAMP = <past>` on the source: MySQL stamps binlog events with the session's statement time, so a later binlog can contain events older than an earlier one, without any clock fault;
- rewrite mode: generated FDE/PREVIOUS_GTIDS/ROTATE events carry PBS's wall clock (`event_generation_helpers.cpp:48, 117, 149`, N11) and every written event feeds the record's timestamp range (`storage.cpp:146-167`), so a PBS clock skew relative to the source changes `min_timestamp`;
- source swap or failover to a server with a different clock (ticket 25).

The Hegel test `search_helpers_property_test` specifies the implemented semantics ("the leading run up to the first record past the timestamp", `tests/PROPERTY_TESTS.md:211`), not the README semantics. So the tests and the documentation disagree.

## Code paths

- `operations/search_helpers.cpp:64-84` `select_records_by_timestamp`.
- `binsrv/storage.cpp:146-167`: per-event timestamp accumulation into `ready_to_flush_timestamps_`, saved in `<name>.json` `min_timestamp`/`max_timestamp`.
- A magic-only tail has an epoch `min_timestamp` (`util/ctime_timestamp_range.hpp:29-46`), so it is always included even though it has no events. That is harmless (a superset), but it is also not what README says.

## Failure scenario

The source's clock jumps back by an hour at 10:00. Binlog 000051 (written 09:30-10:00) has `min_timestamp = 09:30`; binlog 000052 (written after the jump) has `min_timestamp = 09:00`. A PITR request for 09:15 stops at 000051 (its min 09:30 > 09:15) and does not return 000052, which contains events stamped 09:00-09:15. Whether that is wrong depends on how the restore is driven (see Open questions).

## Oracle

From a `list` snapshot taken on a quiescent storage right before the search:
- `Always(every record with min_timestamp ≤ t appears in the search result)` and `Always(result is in index order)`: message "search_by_timestamp: every file with an event at or before t is returned".
- `Sometimes(list shows a record whose min_timestamp is lower than its predecessor's)`, so the non-monotonic case is actually exercised.

## SUT-side instrumentation (missing)

- In `select_records_by_timestamp`, after the `break`: `SOMETIMES(any later record has min_timestamp <= t, "search_by_timestamp: later file with early event exists")` as an exploration marker, and, if the README semantics are confirmed, `UNREACHABLE` for the same condition.

## Antithesis angle

Weak-to-medium: the workload can create non-monotonic timestamps directly with `SET TIMESTAMP`. Antithesis adds clock faults on the source and PBS containers (and rewrite mode), which produce realistic skews at arbitrary moments.

## Fault requirements

None for the `SET TIMESTAMP` variant. Clock faults on the source container (and on PBS for rewrite mode) for the realistic variant; flag: clock faults may be disabled by default on the tenant.

## Open questions

- Which semantics is the contract: README/commit "every file with an event ≤ t", or the implemented leading run (which matches what a `mysqlbinlog --stop-datetime=t` replay reads)? `(needs human input)`
- Should rewrite mode stamp generated events with the source's event time instead of PBS's wall clock? `(needs human input)`

## Answers and implications (this pass)

- Both the README (line 257) and the feature commit 1b79789 (PS-10280: "list of all binlog files that have at least one event generated not later than the provided timestamp") state the "every file" semantics. The `break` was in that same commit ("break when we find a binlog file with min timestamp greater"), so the implementation deliberately assumed monotonic `min_timestamp` from day one. The Hegel test pins the implemented behaviour.
- `mysqlbinlog --stop-datetime` (`client/mysqlbinlog.cc:1433-1437`, percona-xtrabackup f1aa0691f76) returns `OK_STOP` at the first event with `when >= stop_datetime`, and the per-file loop (`:2466-2474`) stops the **whole** multi-file replay on `OK_STOP`. So a replay of the full index stops no later than the first file whose earliest event is ≥ t. The implemented leading run is therefore a superset of what such a replay reads, and the "missed" later files would never be applied by `mysqlbinlog --stop-datetime` anyway. Under that restore procedure the implementation is sufficient and the README over-promises.
- Implication for the property: under either answer, the violation is deterministic. The `SET TIMESTAMP` variant fails the current Always from code alone (`search_helpers.cpp:75-77`), without clock faults. If README semantics wins, the property is a confirmed bug best shown by an MTR test, and Antithesis adds little. If implemented semantics wins, the property is replaced by the Hegel-covered leading-run check plus a README fix. **Recommend lowering priority to low** in either case. Invariant and assertion type are unchanged while the contract is open.

### Investigation Log

#### Which semantics is the contract?

- Examined: README lines 255-300; commit 1b79789 message and diff (`search_by_timestamp` introduction); `git log` of `search_helpers.cpp`; `tests/PROPERTY_TESTS.md:211`; `mtr/binlog_streaming/t/search_by_timestamp.test` (monotonic timestamps only); `client/mysqlbinlog.cc` option text (`:1988-1991`), stop check (`:1433`) and per-file loop (`:2466-2474`) in percona-xtrabackup f1aa0691f76.
- Found: the documentation and the original commit message both say "every file with an event ≤ t". The implementation has had the early `break` since that commit. `mysqlbinlog --stop-datetime` ends the whole replay at the first event ≥ t, so the implemented prefix suffices for that restore path.
- Not found: any maintainer statement on non-monotonic timestamps, or any PITR tooling in this repo that consumes the search result (other than README examples).
- Conclusion: `(needs human input)`. Code and docs disagree, and only the maintainers can say which one is the contract.

#### Should rewrite mode stamp generated events with the source event time?

- Examined: `operations/event_generation_helpers.cpp:40-55` (ROTATE: `now()` if `current_timestamp`, zero for artificial), `:110-120` (FDE: `now()`), `:140-152` (PREVIOUS_GTIDS: `now()`); `collector_context.cpp:376-417` (which generated events go through `process_binlog_event`); `storage.cpp:143-167` and `util/ctime_timestamp_range.cpp` (`add_timestamp` ignores epoch values only).
- Found: generated FDE, PREVIOUS_GTIDS and non-artificial ROTATE carry PBS's wall clock, and they enter the record's min/max range. The artificial ROTATE (zero timestamp) is ignored. A PBS clock behind the source therefore lowers a rewritten file's `min_timestamp`.
- Not found: documented intent for generated-event timestamps.
- Conclusion: `(needs human input)`. The mechanism is confirmed in code; whether it is a bug is a maintainer decision.
