# misaligned-dump-position-rejected-cleanly — A dump request cannot kill or hang pull (ticket 7)

Found by: focus 4 (protocol), focus 6 (security).
Merged from: misaligned-dump-position-rejected-cleanly (focus 4), rs-client-request-cannot-kill-or-hang-pull (focus 6).

## Synthesis notes

Safety, P1 (focus 4 rated P1, focus 6 rated P2). Resolved to P1: any authenticated client can take down collection and every other replica, because all replicas share one password. Positions are never validated (sender_context.cpp:52-76, :61-64; storage_core.cpp:436-562).

Both contributors proposed `ALWAYS(event_size >= 19)` to replace the assert at indexed_event_block.cpp:43. It is kept once here, together with `UNREACHABLE("dump position not on event boundary served")`.

Workload `Always` messages:
- "misaligned dump request rejected without affecting other sessions": the session gets no non-artificial event, concurrent tailing sessions keep advancing, and pull stays alive.
- "legit replica not stalled while pull alive and source has new events".

Workload anchors: `Sometimes(misaligned request while other sessions tail)` and `Sometimes(legit replica progresses after hostile burst)`.

Build matters. A debug or ASan build aborts pull, collector included. A release build loops forever on event_size == 0 on the io thread, which blocks all sessions and SIGTERM.

Misaligned positions also arise from earlier faults: duplicates, truncation, source swap, and rewrite re-splitting.

## What led here

- **Protocol contract (focus 4).** COM_BINLOG_DUMP carries an arbitrary 32-bit position. sut-analysis A13 records that dump positions are never checked to fall on an event boundary. Ticket 7 shows what an unchecked header does: `indexed_event_block.cpp:43-51` only asserts, then runs `offset += event_size` (validated). MySQL refuses such requests with ERR 1236 ("bogus data in log event" / "event read from binlog did not pass crc check").
- **Security / blast radius (focus 6).** Pull is one process. Its single io thread hosts every replica session, the listener and the SIGTERM handler, and the collector runs alongside. A failure confined to one session is acceptable. A failure that escapes the session either takes down backup collection and every other replica, or freezes them.
- Realistic ways a client ends up asking for a misaligned position:
  - Rewrite mode. PBS re-splits binlogs and recomputes positions, so coordinates a client got from the real source do not match PBS's files.
  - A client resumes after the storage diverged at the same offsets: the ticket 18 duplicate, N1 truncation-then-append, ticket 24 two writers, the ticket 25 source swap.
  - A buggy or hostile authenticated client. All replicas use one shared password (`network_service.cpp`).

## Code paths

- `sender_context` constructor (`src/operations/sender_context.cpp:52-76`, `:61-64`): any position ≠ 4 sets `fsm_state_ = start_from_offset` and `range_ = {position, block_size}`. The position is not validated.
- `storage_core::fetch_event_block` (`storage_core.cpp:436-562`) checks only `offset ≥ 4` and `offset ≤ size`.
- `compute_fetch_size_requirements` (`sender_context.cpp:347-380`; size check at `:358-370`) reads `event_size` from whatever 19 bytes sit at the position. It rejects only sizes > `max_event_size_bytes`:
  - `> 1 GiB` → `event_too_large` → `get_event` returns false → silent close (see `dump-session-ends-with-eof-or-err-packet`).
  - Between the buffer length and 1 GiB → keeps topping up. At EOF the partial buffer is kept as carry, and a blocking session polls forever (a stuck session). If a next file exists, `handle_binlog_file_switch` fails on the non-empty carry (`:413-429`) → silent close.
  - `≤` buffer length → an `indexed_event_block` is built.
- `indexed_event_block` constructor (`src/binsrv/indexed_event_block.cpp:29-55`; loop at `:31-50`; assert at `:43`):
  - `event_size < 19`: debug builds abort the **whole pull process** on the assert. Release builds (NDEBUG) continue.
  - `event_size == 0`: release builds loop forever, pushing to `index_` until memory runs out. This runs on the single io thread, so it blocks every other session, the acceptor and SIGTERM handling (LG6).
  - Any other value: slices of garbage are served as "events".
- Assert-only invariants in framing (`src/minimysql/network_io_operations.cpp:91-92, 128-129, 175`). asio guarantees these sizes, so they look unreachable from peer input. Treat them as a lead only.
- Unauthenticated paths. Every pre-auth parse failure throws `system_error`, which is caught per session (`network_service.cpp:621-623`). The binlog name in a dump request is looked up only among indexed records, so path traversal (`../`) is not possible. `composite_binlog_name::parse` throws on bad names inside the session's try block.

## Failure scenarios

**A. Random mid-file position (focus 4; workload input, debug/ASan build).** One client requests `(binlog.000003, 4711)`. In a debug or ASan build (the recommended Antithesis SUT build), this aborts pull: the collector stops and every other replica is disconnected. In a release build, the same request can hang pull's io thread forever. If the garbage header happens to look valid, the session streams nonsense that the client may apply.

**B. Position inside the FDE (focus 6; hostile authenticated replica).** The workload's "hostile replica" authenticates and sends `COM_BINLOG_DUMP binlog.000001 @ 5`. The sender sends an artificial ROTATE+FDE, then fetches from offset 5. `indexed_event_block` reads bytes 5..23 as a header, so `event_size` is arbitrary. In a debug build the assert may abort pull, which ends collection and drops every replica. In a release build, a zero value hangs the io thread, and pull ignores SIGTERM until SIGKILL.

**C. Position misaligned because of earlier faults (focus 4 and 6).** A position that was valid for the source, or for an earlier version of the stored file, becomes mid-event because of rewrite re-splitting, a ticket 18 duplicate, N1 truncation-then-append, ticket 24 two writers, or a ticket 25 source swap. The collector's concurrent rotations and checkpoints can also move a file under a client, but a valid position should become mid-event that way only through a bug.

## Oracle / workload checks

- Hostile / misaligned client. A raw dump client sometimes requests a random position inside a stored file (offsets chosen from `list` sizes, avoiding known boundaries) and other times a known boundary. Hostile sessions also send random names, garbage commands, oversize frames and resets mid-dump. All of this runs concurrently with legitimate dumps and the collector.
- `Always("misaligned dump request rejected without affecting other sessions")`. The misaligned session ends with ERR, or at least receives no non-artificial event whose log_pos chain fails (see `pull-served-stream-self-consistent`). Other concurrent sessions keep receiving events within a bound after the bad request. The pull process stays alive, unless it exited for a collector-side cause unrelated to the hostile client.
- `Always("legit replica not stalled while pull alive and source has new events")`, i.e. `!(pull_alive && legit_replica_stalled_for > T && source_has_new_events)`.
- `Sometimes("misaligned request while other sessions tail")`, so the cross-session part is exercised.
- `Sometimes("legit replica progresses after hostile burst")`: after each hostile burst, a legit replica receives new events within T.

## SUT-side instrumentation

The repo has no Antithesis SDK, so every SDK assertion below is missing.

| Assertion | Site | Status |
|---|---|---|
| `ALWAYS(event_size >= 19, "indexed_event_block: event size at least common header length")`, replacing the assert, plus graceful session termination | `indexed_event_block.cpp:43` | partial (C `assert` exists; aborts in debug, no-op in release) |
| `ALWAYS(first event size >= 19, "sender: first fetched event size at least common header length")` | `compute_fetch_size_requirements` (`sender_context.cpp:347-380`) | missing |
| `UNREACHABLE("sender: dump position not on event boundary served")` | sender, first event after `start_from_offset` | missing |
| `UNREACHABLE("sender: first event after start_from_offset fails header sanity")`, once a validation exists (type code known, size sane, CRC verifies when CRC32) | sender, `start_from_offset` path | missing |

Existing non-Antithesis coverage: `extra/qa/listener_fuzz_test.py` sends sequential malformed traffic, but not authenticated random-position dumps. The Hegel sender_context property tests already cover the ticket-7 parser bug in isolation (`known_failures.txt`).

## Key observations / Antithesis angle

- For a given input the parsing failure is deterministic, and Hegel covers it in-process. Antithesis adds the system-level consequence: one session's bad input aborting or hanging a process that serves other replicas and runs the collector.
- Antithesis also composes the positions that become misaligned *because of* earlier faults (duplicates, truncation, source swap), and runs all of it concurrently with collector rotations and checkpoints.
- Depending on the build, the outcome is an abort (debug: pull dies, detectable) or a hang (release: SIGTERM ignored, which is worse).

## Fault requirements

None for the direct case, which needs only workload input. The "diverged storage" variants need process kills and storage faults.

## Findings after investigation (2026-10-13)

- **Build preset is `debug_clang20`** (deployment-topology.md, "Build preset"). The `assert` at `indexed_event_block.cpp:43` is active there, so in Antithesis an event size below 19 at a misaligned position **aborts pull**. The release-build infinite loop (event_size == 0, io thread blocked, SIGTERM ignored) is not reachable in the planned SUT. Only a `release_*` timeline would reach it, so it stays documented but untested unless a release image is added.
- **The abort is not acceptable fail-stop.** Ticket 7's expected behaviour is that an event size below the header size "is treated as invalid data: the constructor raises `std::invalid_argument` (or stops indexing and lets the caller report corruption)". That makes it a session-level error, which the session's `catch (...)` (`network_service.cpp:621-623`) would contain. The cross-session `Always("misaligned dump request rejected without affecting other sessions")`, including "pull stays alive", therefore applies to the debug build. In Antithesis it fails through a process abort (ticket 7, known failure).
- **MySQL precedent.** MySQL's sender checks every event it reads. A length below the minimal header gives `BOGUS` ("corrupted data in log event"), a too-large event gives `EVENT_TOO_LARGE`, and with CRC32 a mismatch gives `CHECKSUM_FAILURE` (`sql/binlog_istream.cc:34-60`). Each is sent as a fatal ERR 1236. MySQL also does not validate the start position against an event index. Its protection is the per-event header and CRC check, so with `binlog_checksum=NONE` a lucky misaligned header can be served by MySQL too.
- **No event index in PBS storage.** `binlog_record` holds only name, size, GTID sets, timestamps, sequence number and encryption parameters (`storage_core.hpp:72-88`). Validating against a full event index would need a new persisted structure.

**Property change:** none to the invariants. The tightened version ("the misaligned session always receives ERR") still depends on the owners' decision. Until then, the misaligned session's own `Always` stays "no non-artificial event fails the header/log_pos sanity check". The cross-session `Always` and "pull stays alive" apply to every build.

## Open questions

- Should a dump position that is not on an event boundary be rejected with an ERR packet (MySQL rejects it through per-event header/CRC checks, ERR 1236)? Or are authenticated replicas trusted, which would make this reliability-only? `(needs human input)`
- Should pull validate the start position against an event index (none exists today), or only sanity-check the first header (size, type, CRC)? `(needs human input)`

### Investigation Log

#### Should a dump position that is not on an event boundary be rejected with an error packet, as MySQL does?

- Examined: `indexed_event_block` constructor (`src/binsrv/indexed_event_block.cpp:29-55`); `compute_fetch_size_requirements` (`src/operations/sender_context.cpp:347-380`); `sender_context` constructor (`:52-72`); ticket 7 (`.tickets-snapshot.txt:386-430`); MySQL `Binlog_read_error::get_str` (`percona-xtrabackup/sql/binlog_istream.cc:34-70`) and `Binlog_sender::check_start_file` (`sql/rpl_binlog_sender.cc:967-993`).
- Found: PBS checks only `offset ≥ 4`, `offset ≤ size` and `event_size ≤ 1 GiB`. Ticket 7 asks for size < 19 to be rejected as invalid data, but says nothing about boundary validation in general or about ERR packets. MySQL checks the position only against the file size, then rejects garbage through per-event checks and fatal ERR 1236.
- Not found: any owner statement on whether authenticated replicas are trusted, or on whether a misaligned request must produce ERR.
- Conclusion: `(needs human input)`. The cross-session isolation `Always` does not depend on the answer. Only the "misaligned session gets ERR" tightening does.

#### Should pull validate positions against an event index, or only validate the first header?

- Examined: `binlog_record` (`src/binsrv/storage_core.hpp:72-88`), `fetch_event_block` (`storage_core.cpp:436-562`), MySQL sender checks (above).
- Found: no per-event index is persisted. Only per-file size and metadata are. MySQL itself relies on per-event header/CRC checks, not an index.
- Not found: any design note on intended validation.
- Conclusion: `(needs human input)`. If "first-header sanity" is chosen, the `UNREACHABLE("sender: first event after start_from_offset fails header sanity")` goes in `handle_start_states` after the first fetch. If an index is chosen, it goes in `fetch_event_block`.

#### Which build preset will Antithesis run, and is a debug-build abort acceptable as fail-stop?

- Examined: `antithesis/scratchbook/deployment-topology.md` (Build preset: `debug_clang20`), `CMakePresets.json:56-63, 94-120`, ticket 7 Expected behaviour, `network_service.cpp:621-623`.
- Found: the planned SUT image uses `debug_clang20`, where asserts are active. Ticket 7 wants the condition reported as invalid data (an exception), which the per-session catch would contain. That means aborting the whole process (collector plus every replica) is the defect, not an accepted fail-stop.
- Conclusion: resolved. The cross-session `Always` applies to the debug build. In Antithesis it fails through a pull abort (exit by SIGABRT, which the workload sees as pull dying while the hostile session is active). The release hang is out of reach unless a release image is added.

## Evaluation refinements (2026-10-13)

Applied by the evaluation synthesis (`../evaluation/synthesis.md`). Where this section and earlier text in this file disagree, this section wins; the catalog entry has been updated to match.

- The direct case is input fuzzing, and in a debug build each firing aborts the whole pull through the `indexed_event_block.cpp:43` assert, starving every other pull property in that timeline. The hostile-request driver moves out of `pull_fs` into its own template (or a release-build launch for ticket 7's hang); `pull_fs` keeps the naturally arising variant (antithesis-fit F5, implementability C6).
- The direct case is also part of bias B4 (deterministic territory).
