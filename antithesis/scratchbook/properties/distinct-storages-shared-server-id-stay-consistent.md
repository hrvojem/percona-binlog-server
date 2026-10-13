# distinct-storages-shared-server-id-stay-consistent

## What led to it

Every README config example uses `"server_id": 42` (`README.md:508`), and `server_id` is a required, unvalidated field (`binsrv/replication_config.hpp:30-36`). PBS registers no replica UUID and sets no `@replica_uuid` (`easymysql/connection.cpp`, `MYSQL_RPL_SKIP_HEARTBEAT` only). MySQL's zombie-dump-thread eviction therefore matches on `server_id`, and a new dump connection with the same server_id kills the existing one with ER 1236 (verified in MySQL source; see Investigation outcome).

`second-writer-on-storage-fails-closed` already covers same-server_id instances on the **same** storage. This property covers the realistic HA or migration deployment where operators copy the config: two PBS instances writing **different** storages (two regions, or a cron `fetch` to a local dir plus a `pull` to S3), ~~or PBS plus a real replica that reuses id 42~~ (out of scope: a real replica sets a UUID and does not collide; see Investigation outcome). The storages are independent, but the upstream eviction ping-pong becomes a continuous, natural mid-stream disconnect injector against both.

## Code paths

- Mid-stream disconnect handling: `easymysql/connection.cpp:546-551`. Only `CR_SERVER_LOST` (2013) returns "retry". Anything else throws, which ends pull and its replication source (`pull_operation.cpp:133-157`).
- On a false fetch the collector discards the incomplete transaction and flushes (`collector_context.cpp:195-200`). Exceptions skip the discard.
- Resume: `collector_context.cpp:207-272`, with a fixed `idle_time` and no backoff (`pull_operation.cpp:135-151`).
- `fetch` exits non-zero on the evicted connection if the error is not 2013 (`fetch_operation.cpp:98-115`).

## Failure scenarios

1. ~~The eviction surfaces as 2013~~ (ruled out: eviction is ER 1236). If it were 2013, so both instances reconnect after `idle_time` and evict each other. Each storage receives a stream chopped at arbitrary points every `idle_time` seconds. Every chop runs the discard/flush/resume path, which is ideal coverage for resume bugs (ticket 14 in GTID mode across rotations, ticket 18 if a flush fails during the chop). Progress is still possible, because each instance gets about `idle_time` of streaming per cycle.
2. **Confirmed:** the eviction surfaces as server error 1236, so the evicted pull **dies**, taking its replication source down (N7). Cron fetch dies with a non-zero exit. Under a supervisor, both crash-loop alternately.
3. (With the supervisor) liveness degenerates when both `idle_time`s are equal and connections land in lockstep, so each gets evicted before it receives anything.

## Oracle

- Safety: each storage independently remains a byte prefix of the source (QA `storage_holds_prefixes`) and opens on the next run.
- Liveness: `SOMETIMES(both storages advanced since the last check)`. After the workload quiesces (stops one instance), the other converges to the source.

## Antithesis angle

Configure two writers with the same `server_id` on different storages for part of the runs. Antithesis supplies the timing variation (CPU modulation, thread pauses) that decides who evicts whom, plus ordinary partitions that add more disconnect points.

## Instrumentation suggestions

- Workload (missing): `SOMETIMES(both same-server_id instances made progress in the same window, "shared server_id instances both progress")`.
- Workload (missing): `ALWAYS(each storage is a byte prefix of the source, "storage consistent under server_id eviction")`.
- SUT-side (missing): in `connection.cpp` around `:546`, `SOMETIMES(native_error != CR_SERVER_LOST, "mid-stream fetch error other than 2013")`, labelled with the errno (expected 1236 for eviction).
- Workload (missing): `SOMETIMES(evicted instance exited with the same-server_id message, "shared server_id eviction observed")`.

## Investigation outcome (2026-10-13) — property changed

The eviction error is resolved: it is **scenario 2 (fail-stop)**. Scenario 1 (2013 ping-pong) does not happen. See the Investigation Log.

- Mechanism (MySQL 8.0.36, 9.6.0 and 9.7.1 sources, identical code): at `COM_BINLOG_DUMP[_GTID]`, `kill_zombie_dump_threads()` (`sql/rpl_source.cc:1145-1173`) looks for another dump thread. When the new client set no replica UUID, the match is "same `server_id` and the other thread also has no UUID" (`:1110-1114`). The old thread gets `duplicate_slave_id = true` and `KILL_QUERY`. Its `Binlog_sender` then calls `set_fatal_error("A replica with the same server_uuid/server_id as this replica has connected to the source")` (`sql/rpl_binlog_sender.cc:473-477`), which is `ER_SOURCE_FATAL_ERROR_READING_BINLOG` (**1236**, `rpl_binlog_sender.h:434-436`). `cleanup()` sends it with `my_message` (`:381-382`). The server comment says this is so that "it will not try to reconnect anymore".
- PBS side: `connection::fetch_binlog_event` throws on any errno other than 2013 (`src/easymysql/connection.cpp:546-551`), so the **older** instance exits. In pull, the exception leaves `receive_binlog_events`, the operation thread catches it and stops `io_ctx`, which also stops the replication source listener. `pull` rethrows and exits non-zero (`pull_operation.cpp:133-166`). `fetch` exits non-zero the same way. `~storage` still flushes up to the last boundary (`storage.cpp:68-77`). The log carries the server message through `mysql_error`, from `core_error_helpers_private.cpp:56-59`.
- **Scope correction:** PBS plus a *real MySQL replica* that reuses id 42 does **not** collide. A replica sets its UUID, so the PBS dump (no UUID) matches only UUID-less threads, and the replica's reconnect matches only its own UUID (`rpl_source.cc:1104-1114`). The collision happens only between UUID-less clients: two PBS instances, or PBS plus another UUID-less dump client with the same `server_id`. With `server_id = 0` there is no eviction at all (`:1148`).
- **Property changes:**
  - The safety half is unchanged: each storage stays a byte prefix ending at a transaction boundary, and it opens.
  - Liveness is now **conditional on an external restarter**. Every connect by one instance kills the other's pull process. Under the harness supervisor (`deployment-topology.md`: restart `idle` seconds after exit) this is a crash/restart ping-pong. It is not an in-process reconnect loop, and it also drops downstream replica sessions on each eviction. Without a supervisor, the evicted instance stays down. So `SOMETIMES(both advanced)` only makes sense with the supervisor on. The property now mainly drives the **exception exit path** (`~storage` flush on exception, ticket 18 if that flush fails) rather than the timeout/discard path.
  - Add a workload `Sometimes(evicted instance exited with "same server_uuid/server_id", "shared server_id eviction observed")` so the path is known to be reached.
- Design note for the owners: setting a unique `@replica_uuid` per instance would make same-`server_id` PBS instances **coexist** (UUID matching only). Fail-stop would then disappear, not become fail-closed.

## Open questions

- Should PBS detect or avoid a server_id collision (warn, document "server_id must be unique per PBS instance", or set a per-instance `@replica_uuid`)? `(needs human input)` If the owners choose a UUID, the collision vanishes and this property reduces to plain two-instance independence. If they choose "document only", today's fail-stop is the intended behaviour, and the property asserts safety plus supervisor-driven progress.

### Investigation Log

#### Which client error does `mysql_binlog_fetch` receive when the source kills a same-server_id zombie dump thread?

- Examined: the MySQL server source for 9.6.0 (`~/work/percona-xtradb-cluster`), 9.7.1 (`~/work/percona-xtrabackup`) and 8.0.36 (`~/worktable_old/percona-xtradb-cluster`): `sql/rpl_source.cc` (`com_binlog_dump` :957, `com_binlog_dump_gtid` :1027, `Find_zombie_dump_thread` :1094-1122, `kill_zombie_dump_threads` :1145-1173), `sql/rpl_binlog_sender.cc:466-492` and `cleanup()` `:367-385`, `rpl_binlog_sender.h:434-440`, `sql/sql_parse.cc:2981` / `sql_class.cc:2692-2699` (`send_kill_message` does not overwrite an already-set diagnostics area). PBS: `connection.cpp:121-160` (no `@replica_uuid` / `@slave_uuid` query, no `COM_REGISTER_SLAVE`), `connection.cpp:537-553`, `collector_context.cpp:125-205`, `pull_operation.cpp:120-175`.
- Found: the old dump thread ends with ER 1236 and the message "A replica with the same server_uuid/server_id as this replica has connected to the source; the first event ...". The error reaches the client as an error packet. 1236 != `CR_SERVER_LOST`, so PBS throws and the process exits non-zero. Matching is on `server_id` only when neither side has a UUID. A real replica sets a UUID and is therefore unaffected. The logic is the same in 8.0.36, 9.6.0 and 9.7.1.
- Not found: an executed reproduction. This conclusion comes from reading the code. MySQL 8.4 source was not on disk, but 8.0.36 and 9.6/9.7 match, so 8.4 almost certainly does too.
- Conclusion: resolved, scenario 2. The property is changed as described above.

#### Should PBS refuse or warn on a server_id collision?

- Examined: `src/binsrv/replication_config.hpp` / `.cpp` (no uniqueness or UUID option), README config section, `.tickets-snapshot.txt`. Ticket 26 (concurrent storage) reproduces with `--shared-server-id`, but no ticket covers distinct storages.
- Found: PBS cannot detect a collision before connecting. The server reports one only to the *evicted* side, after the fact. The available design levers are documentation, a per-instance UUID, or a friendlier error on 1236.
- Not found: any statement of product intent.
- Conclusion: `(needs human input)`.

## Evaluation refinements (2026-10-13)

Applied by the evaluation synthesis (`../evaluation/synthesis.md`). Where this section and earlier text in this file disagree, this section wins; the catalog entry has been updated to match.

- No template runs two writers on different storages against one source. The property needs a second PBS process in `pbs` on a sibling storage, launched by a `contention` test command (no second container). It is Deferred until that step exists (antithesis-fit F4, implementability P7).
- Every other template allocates `server_id`s per role, and oracle `mysqlbinlog` dumps use server_id 0 or a unique `--connection-server-id`, so ordinary timelines never see this eviction (implementability C9, wildcard W9; topology change).
