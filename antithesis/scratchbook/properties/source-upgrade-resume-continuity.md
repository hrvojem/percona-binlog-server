# source-upgrade-resume-continuity — Resume across an in-place source upgrade (8.0→8.4, 8.4→9.7)

## What led here
- `src/binsrv/events/event_view.cpp:72-96` contains an explicit design claim: "in a scenario when 8.0 MySQL Server was upgraded to 8.4, it is possible for us to receive both 8.0 FDEs (coming from binlog files created before the upgrade) and 8.4 FDEs". The FDE post-header size is derived from the event size instead of the context's version table, specifically to support this.
- The only test of it, `mtr/binlog_streaming/t/data_directory_8_0_to_8_4_upgrade.test`, upgrades the source **first** and then runs a single `fetch` into an empty storage (`--exec $BINSRV fetch ... > /dev/null`, lines ~84-86). It never has a storage that was being filled from the 8.0 server and then *resumes* against the upgraded 8.4 server, never runs pull across the upgrade, never kills PBS near the boundary, and never compares the stored bytes.
- The 8.4→9.7 boundary has no test at all. Nothing in CI changes the source version during a run.

## Code paths
- `collector_context.cpp:141-146`: `reader_context` is built with `connection.get_server_version()` (the *new* server's version after the upgrade), and with the stored tail name/position.
- `reader_context.cpp:644-650`: FDE version > connection version is fatal ("unexpected server version in format description event"). On an upgrade the old FDE is older, so this passes. On any *downgrade* or a failover to an older server, it is fatal (a fail-stop, which is fine).
- `reader_context.cpp:651-660`: post-header lengths are validated per FDE version and replaced (`post_header_lengths_ = ...`), so each source file is parsed with its own table.
- `protocol_traits_fwd.hpp:30-37`: only two layouts are known, `< 80300` → 42 events and `>= 80300` → 43 events. `format_description_post_header_impl.cpp:~116-121` rejects an FDE post header larger than the 8.3 layout. A future 9.x with a 44th event type would be fatal (deterministic, which CI catches).
- Position-mode resume: the tail name + `size` (`storage_core.hpp:313-337`) → artificial ROTATE + pseudo-FDE from the *old* 8.0 file (`next_event_position == 0` check, `reader_context.cpp:~690-696`) → remaining events → STOP (written at the 8.0 shutdown) → ROTATE into the first 8.4 file.
- GTID-mode resume: the source restart always rotates, so resume crosses a file boundary. This is exactly ticket 14's path (`collector_context.cpp:541-553`: on a name mismatch it just calls `close_binlog()` with no gap check). An upgrade therefore guarantees ticket-14 conditions in GTID mode.

## Failure scenario
1. The workload writes to an 8.0 source. PBS pull or a fetch loop stores binlog.000001-3.
2. Mid-transaction, the source is stopped and restarted on 8.4 binaries over the same datadir (the 8.0 file gets STOP; the server opens binlog.000004 with an 8.4 FDE).
3. PBS is killed before, during or after the switch, and restarted.
4. Expected (position mode; GTID mode pending the byte-identity question): the stored binlog.000003 ends with the same STOP as the source, binlog.000004 starts with the 8.4 FDE, and no transaction is lost or duplicated. Risks: a GTID-mode resume skips the tail of .000003 (ticket 14); the pseudo-FDE from the old file is mis-sized; checksum or version table confusion appears at the boundary.

## Oracles
- Position mode: `storage_holds_prefixes()` / `check_complete_copy()` from `extra/qa/fetch_crash_test.py:636-697` per file, after catch-up.
- Both modes: the union of stored `added_gtids` equals the source's `gtid_executed` once caught up; no GTID appears in two files.
- Per stored file: the FDE server-version string equals the source file's FDE server version.

## SUT-side instrumentation (all missing; none exists per existing-assertions.md)
- `ALWAYS_OR_UNREACHABLE` in `reader_context::process_event_in_format_description_expected_state` when the FDE version differs from the connection version: the post-header table that is adopted equals `get_known_post_header_lengths(fde_version)`. Message: "mixed-version FDE adopted with matching layout".
- `SOMETIMES(fde_version < connection_version)` at the same place, to prove the workload actually reaches a cross-version resume.

## Investigation findings (2026-10-13) and property changes

**Environment feasibility.** No tenant feature is needed. The deployment already runs `mysqld` under a restart-loop wrapper in the `mysql-source` container (deployment-topology.md:69). A variant image that carries both the old and the new generic tarball can choose the binary from a marker file that a test command flips before an SQL `SHUTDOWN`. deployment-topology.md:74 and :222 record this as the deferred `upgrade` template.

**Only 8.0 to 8.4 changes the event layout.** In MySQL 9.7.1, `GTID_TAGGED_LOG_EVENT = 42` is still the last event type (`libs/mysql/binlog/event/binlog_event.h:367-372`), so 8.4 and 9.7 share the 43-entry post-header table (`protocol_traits_fwd.hpp:30-40`). Across 8.4 to 9.7 only the FDE version string changes. As a result:
- the `ALWAYS_OR_UNREACHABLE("mixed-version FDE adopted with matching layout")` is meaningful mainly on 8.0 to 8.4;
- `SOMETIMES(fde_version < connection_version)` is reachable on both paths.

**The upgrade restart is graceful.** A graceful restart has two consequences:
1. The source writes STOP to the old file, and a GTID-mode resume across it hits ticket 14 with STOP as the lost terminator.
2. A pull that is streaming receives an EOF packet from the dump thread and keeps its buffer (case (3), see `inprocess-reconnect-resumes-without-dup-or-gap`). In GTID mode with buffered transactions, pull then fail-stops at the position check instead of reaching the cross-version path.

A position-mode pull is the clean way to exercise the FDE switch.

**Supported paths.** By MySQL's upgrade policy, an in-place upgrade is supported from one LTS series to the next (8.0 to 8.4, 8.4 to 9.7), but an LTS may not be skipped (8.0 to 9.7). The workload is therefore restricted to 8.0 to 8.4 and 8.4 to 9.7. This is MySQL policy from the reference manual's upgrade-paths section, not something PBS defines.

## Open questions

- Is a GTID-mode resume across the upgrade expected to be byte-identical (the old file's STOP is stored)? This is the same question as ticket 14's byte-identity guarantee. If it is not, the GTID-mode oracle is GTID-set equality only. `(needs human input)`

### Investigation Log

#### Can the Antithesis environment perform an in-place binary swap of mysqld over a shared datadir?

- Examined: deployment-topology.md (lines 18, 66-74, 222, 242).
- Found: the restart-loop wrapper and the test commands in the `mysql-source` container let the workload stop mysqld and restart it on a different binary over the same container datadir. It needs only a variant image with both tarballs, and no node termination or tenant configuration.
- Conclusion: resolved, feasible. Scheduling is still deferred to the `upgrade` template.

#### Is a GTID-mode resume across the upgrade expected to be byte-identical?

- Examined: the same evidence as `gtid-resume-preserves-source-rotations`: upstream MTR byte checks are position-mode only, and the README makes no byte-identity statement.
- Conclusion: `(needs human input)`. This is the same catalog-wide question.

#### Which upgrade paths are supported?

- Examined: MySQL's LTS upgrade policy (reference manual, upgrade paths); `binlog_event.h:367-372` in MySQL 9.7.1; PBS `protocol_traits_fwd.hpp:30-40`.
- Found: 8.0 to 8.4 and 8.4 to 9.7 are supported, and 8.0 to 9.7 is not. The 8.4 and 9.7 event tables are identical.
- Conclusion: resolved. Restrict the workload to the two adjacent-LTS hops, with 8.0 to 8.4 the higher-value one.
