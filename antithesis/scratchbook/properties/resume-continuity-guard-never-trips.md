# resume-continuity-guard-never-trips

## What led to it

PBS has built-in replay/gap detectors that turn a mismatch between stored bytes and the source into a `logic_error` or a CR_1236 error:
- `collector_context.cpp:524-532` "unexpected binlog position in artificial rotate event" (position mode);
- `reader_context.cpp:547-567` "unexpected position found in an artificial rotate event";
- `reader_context.cpp:908-921` "unexpected next event position in the event common header", the first event after a same-file resume in both GTID and position modes;
- `storage_core.cpp:276-279` "cannot open an existing binlog that is not the latest one for append", where the GTID source starts in an older stored file, which means the storage is missing a transaction there;
- server-side CR_1236 "start replication from position > file size", which the client sees as a `core_error` at `connection.cpp:548-549`.

In a run with no source identity change and no external storage damage, each of these fires **only** if the storage already holds a duplicate, a gap or a truncation inconsistent with its metadata. Asserting them unreachable makes Antithesis report the *first* moment a replay inconsistency becomes visible, which is often a later run than the one that caused it.

## Instrumentation (missing)

- One `UNREACHABLE` per raise site, each with a unique message (for example "resume: artificial ROTATE position differs from stored size", "resume: first event after resume not contiguous with stored bytes", "resume: GTID source restarted in a non-tail stored binlog", "resume: first event after resume is not a transaction start" for `reader_context.cpp:775-778`).
- Workload-side: `ALWAYS` that fetch's stderr/exit never reports CR_1236 "position > file size" while the source still holds the binlog.

## Caveats

- Ticket 25 (no source identity) and source `RESET BINARY LOGS` legitimately trip these. The workload must not perform those actions, or must tag such runs and exclude them.
- If the source purges a binlog the storage needs (1236 "purged"), that is a different message and is not part of this property.
- These checks fire in an *operation* that might already have succeeded once. Report them as a resume-consistency violation, not as a bug at the raise site.

## Investigation findings (2026-10-13) and property changes

**The premise is too narrow.** The text above says each guard fires "only if the storage already holds a duplicate, a gap or a truncation". It can also fire when the **in-memory event buffer** disagrees with the resume request, with the storage itself intact. Pull's EOF path does exactly that. A graceful source shutdown or `KILL QUERY` of the dump thread sends EOF (MySQL 9.7.1 `rpl_binlog_sender.cc:366-384`, `mysqld.cc:2323-2368`). PBS then returns from case (3) without discard or flush (`collector_context.cpp:184-187`) and reconnects with the buffer intact. Two outcomes follow:
- in GTID mode, with buffered complete transactions, it trips "unexpected next event position in the event common header" (`reader_context.cpp:908-921`);
- in position mode, with a partial transaction buffered, it trips "transaction group must start from one of the gtid log events" (`reader_context.cpp:775-778`), a guard missing from the list above.

Both are still real PBS defects (see `inprocess-reconnect-resumes-without-dup-or-gap`), so `UNREACHABLE` stays the right assertion type. The property is now stated as "the storage **or the in-memory resume state** is inconsistent with the resume request", and the guard list gains the `reader_context.cpp:775-778` site, with its own message: "resume: first event after resume is not a transaction start". Expect these guards to fire today in pull after a graceful source shutdown.

**Ruling out the legitimate trips.**
- **Source crash ahead-of-source (CR_1236 "position > file size", "more GTIDs than the source").** This cannot happen with the harness's `sync_binlog=1` (deployment-topology.md:67). With `sync_period == 1`, MySQL publishes `binlog_end_pos` only after the fsync (`binlog.cc:7967, 7984, 8035-8048`), so the dump thread never sends bytes that are not durable.
- **Source `RESET BINARY LOGS AND GTIDS` / identity change (ticket 25).** The default `parallel_driver_kill_mysqld.sh` does not reset. It resets only when the `source-swap-*` properties are enabled (deployment-topology.md:178), and whether that action lives in `fetch_fs` or only in the deferred `failover` template is still a file-level open question (deployment-topology.md:276).

## Open questions

- Should the guards be SUT `UNREACHABLE` or be classified workload-side from log text? `(partial: SUT UNREACHABLE is sound in timelines without source RESET, since sync_binlog=1 rules out ahead-of-source 1236; it hinges on the file-level decision whether source-swap/RESET actions share a template with this property)`

### Investigation Log

#### Should the guards be UNREACHABLE in the SUT or classified on the workload side?

- Examined: the guard sites (`collector_context.cpp:519-532`, `reader_context.cpp:547-573, 775-778, 908-921`, `storage_core.cpp:273-283`); case (3) in `collector_context.cpp:170-187` together with `pull_operation.cpp:133-153`; the MySQL 9.7.1 sources `rpl_binlog_sender.cc`, `mysqld.cc` and `binlog.cc`; and deployment-topology.md lines 67, 178 and 276.
- Found: with `sync_binlog=1`, PBS is never ahead of a recovered source. Without a source RESET or identity change, the only non-storage way to trip a guard is pull's EOF path, which is itself a defect. The harness plans to run source RESET only under the `source-swap-*` properties.
- Not found: the final template assignment for the source-swap action, which is a file-level open question.
- Conclusion: `(partial: ...)`. If source-swap stays out of this property's templates, use SUT `UNREACHABLE`. Otherwise the workload must tag RESET timelines, and the guards must be classified from logs.

## Evaluation refinements (2026-10-13)

Applied by the evaluation synthesis (`../evaluation/synthesis.md`). Where this section and earlier text in this file disagree, this section wins; the catalog entry has been updated to match.

- Source RESET runs only behind the source-swap flag. In timelines where it ran, the `UNREACHABLE` messages carry the harness timeline tag, so a RESET-induced firing does not mark the in-contract property red. Antithesis tallies an assertion by its message across every timeline of a run, so an SUT message cannot be scoped to a template (wildcard W1).
- Lead (a) makes these fire after every graceful source `SHUTDOWN`; containment is bias B3.
- Runs in `fetch_fs`, `pull_fs`, `s3`.
