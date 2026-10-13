# blocking-dump-ahead-of-flushed-position-eventually-served — a blocking client asking for a position PBS will store later waits instead of being silently disconnected

## What led here

Lifecycle focus (replica connecting before the collector has caught up). Ticket 26 / PBS-96 (open, validated in sut-analysis: empty storage returns true + empty buffer, `storage_core.cpp:474-478`; `fetch_fde_from_storage` fails on `size < 19`, `sender_context.cpp:231-233`) and sut-analysis lead N12 (a dump request ahead of the flushed position closes silently with no error packet, `network_service.cpp:308-313`). The Hegel sender test documents the from-position case as failing (PROPERTY_TESTS.md:213: "the from-position case fails, PBS-96").

The realistic lifecycle trigger is a **switchover**: a replica or `mysqlbinlog --stop-never` client that was reading from the real source is pointed at PBS. Its position comes from the source, while PBS lags (no checkpointing = nothing of the active binlog is stored until the source rotates, LG3). A second trigger is PBS starting on an empty storage while clients reconnect with saved positions.

## Code paths (verified at 99de682)

`sender_context` construction (`sender_context.cpp:52-72`): `position != 4` → `start_from_offset`.

1. **Empty storage, position > 4** (ticket 26): `handle_start_states` → `fetch_fde_from_storage` (`:542`) → `fetch_event_block` returns true with an empty buffer (empty-storage branch) → `size < 19` → false (`:231-233`) → `get_event` false → session `co_return`s after logging "failed to fetch next event block" (`network_service.cpp:308-313`). No error packet.
2. **Binlog exists, magic only, position > 4**: FDE fetch returns an empty buffer → same as 1.
3. **Binlog exists with FDE, position > flushed size**: FDE fetch succeeds; artificial ROTATE and artificial FDE are sent; then `populate_event_block` → `fetch_event_block` → `read_offset > record_it->size` → false (`storage_core.cpp:513-515`) → session closed silently after the client already received ROTATE+FDE.
4. **Name not stored yet** (client asks `binlog.000007`, PBS tail is `binlog.000006`): `std::ranges::find` fails → false (`:489-495`) → silent close. (With `position == 4` and empty storage, PBS-91 makes it poll; as soon as any record exists it fails instead — the behaviour depends on *when* the client arrives.)

In all cases the client sees a dropped connection rather than either data or a MySQL error, and a replica retries in a loop (each retry is a new session; harmless but never progresses until PBS catches up, after which it works). `mysqlbinlog --stop-never` exits.

## Oracle

- Workload: keep a source writing; run pull with random checkpoint config. Start blocking clients at positions taken from the **source's** current `SHOW BINARY LOG STATUS` (i.e. ahead of PBS), and also at positions captured from earlier PBS sessions after PBS restarts.
- `Sometimes(a client that requested a position beyond PBS's flushed size at connect time later received the event at that position from the same session)` — liveness; today expected to be false for cases 1–4.
- Supporting safety check: `Always(PBS never closes a dump session for a position ≤ the source's position without sending either events or an error packet)`; the client-side observation is "connection closed, no ERR packet, PBS still running".
- Keep a control: a client at a position ≤ PBS's flushed size must always be served (`pull-chain-prefix-consistency`).

## SUT-side instrumentation (missing)

- `storage_core::fetch_event_block` `:513-515`: `SOMETIMES(true, "dump requested offset beyond flushed size")`.
- `network_service.cpp:308-313`: `UNREACHABLE("blocking dump session closed by PBS without error packet")` once the team decides the intended behaviour (wait or ERR). Until then, a `SOMETIMES` anchor with details (`name`, `position`, `blocking`, storage tail name/size).

## Faults

None required for the base case; Antithesis adds the timing of client connect relative to PBS start, rotation and checkpoint flushes, plus PBS restarts (kill/SIGTERM) and source flaps that stretch PBS's lag.

## Scope after investigation (2026-10-13)

The investigation split the four cases by documented intent (see the Investigation Log):

| Case | Intended behaviour | Source | In this property? |
|---|---|---|---|
| 1. Empty storage, position > 4 | wait (poll), then serve | PBS-91 rationale (0f56bd6: empty storage is "a transient PBS-startup condition"); ticket 26 / PBS-96 "Expected behaviour" | **yes**, known failure today (PBS-96) |
| 2. Requested binlog exists but is magic-only (size 4), position > 4 | wait | Same transient state, one step later. Ticket 26's *suggested fix* says "keep returning false for a binlog that exists but is shorter than a FORMAT_DESCRIPTION event", which, read literally, keeps this case failing. `fetch_fde_from_storage` cannot tell the two apart: both return an empty buffer (`storage_core.cpp:474-478` vs `:520-531`). | **yes**. A fix for PBS-96 that only checks for an empty storage is incomplete. Add a `Sometimes` anchor that separates case 2 from case 1. |
| 3. Binlog has an FDE, position > flushed size | undecided. MySQL's own precedent is a fatal ERR 1236 "Client requested source to start replication from position > file size" (`rpl_binlog_sender.cc:988-992`). | none | **conditional**: the `Sometimes` covers case 3 only if the owners pick "wait" |
| 4. Requested name not stored, storage non-empty | fail | PBS-91 commit: that path "still correctly returns 'false'" | **no**. Moved to `dump-session-ends-with-eof-or-err-packet` (ERR vs silent close) |

**Property change:** the liveness `Sometimes` now has a fixed scope: cases 1 and 2. Case 3 is included only if the owners choose "wait". Case 4 is removed. The workload should label each client attempt with its case (it knows the PBS tail from `list` and the source position) so that each case gets its own `Sometimes` message. The supporting `Always` (no silent close for a position ≤ source position) is unchanged, but for case 4 it is now asserted by `dump-session-ends-with-eof-or-err-packet`, not here.

## Open questions

- Case 3 (position beyond the flushed size of an existing binlog that has an FDE): should PBS wait or send ERR 1236, as MySQL does? `(needs human input)`
- Is the switchover workflow (pointing an existing replica or a `mysqlbinlog --stop-never --start-position` client at PBS) a supported use case? `(needs human input)`

### Investigation Log

#### Intended behaviour for a position ahead of PBS: wait, or reply with error 1236?

- Examined: PBS-91 commit 0f56bd6 message (upstream/main, maintainer-authored); the comment at `src/binsrv/storage_core.cpp:463-473`; ticket 26 / PBS-96 in `.tickets-snapshot.txt:1399-1450` (Expected behaviour, Suggested fix); `sender_context::fetch_fde_from_storage` (`src/operations/sender_context.cpp:220-245`) and `handle_start_states` (`:530-545`); `fetch_event_block` (`storage_core.cpp:436-562`); MySQL `Binlog_sender::check_start_file` (`percona-xtrabackup/sql/rpl_binlog_sender.cc:967-993`).
- Found: the maintainer's rationale covers only the empty storage. Ticket 26 (open, filed by the team, verified with a property test) asks that "a request at any position behaves like one at position 4 while the storage is empty". That settles cases 1 and 2 as "wait". For a magic-only tail, `fetch_event_block(name,{4,len})` takes the `read_offset == size` and no-next-record branch and returns true with an empty buffer (`:520-531`), so `fetch_fde_from_storage` fails at `size < 19` (`sender_context.cpp:231-233`) exactly as in case 1. MySQL rejects a position beyond the file size with a fatal error (`rpl_binlog_sender.cc:988-992`).
- Not found: any PBS statement about case 3 (an FDE is present but the position is past the flushed size).
- Conclusion: partially resolved. Cases 1 and 2 are "wait". Case 3 needs a decision from the owners and is tagged `(needs human input)`. MySQL's own behaviour suggests ERR 1236.

#### For case 4 (name not yet stored), how should PBS distinguish "will exist" from "never existed"?

- Examined: `fetch_event_block` name lookup (`storage_core.cpp:489-495`), the PBS-91 commit message, MySQL `check_start_file` (`rpl_binlog_sender.cc:967-971`).
- Found: PBS does not try to tell them apart. Once any record exists, an unknown name returns false, and the maintainer calls that correct ("the specific binlog_name + non-empty storage case is unchanged - that path still correctly returns 'false'"). MySQL answers "Could not find first log file name in binary log index file" (ERR 1236) immediately.
- Conclusion: resolved. Case 4 is out of scope for this property. Whether the failure must carry an ERR packet is tested by `dump-session-ends-with-eof-or-err-packet`.

#### Is the switchover workflow a supported use case?

- Examined: README `replication_source` section ("so that downstream replicas can dump binary log events from it"), README intro, ticket 20 (`.tickets-snapshot.txt:1072-1130`), `network_service.cpp` known-query allowlist (`:500-551`).
- Found: the README promises replica support in general terms. Ticket 20 shows real MySQL replicas stop at `SELECT @@GLOBAL.SERVER_ID` (syntax error from the allowlist), so only mysqlbinlog works today. `mysqlbinlog --read-from-remote-server --stop-never --start-position=N <name>` reaches the case 1–3 paths today.
- Not found: any document that names "switch a running replica over to PBS" as a supported workflow, or its expected semantics while PBS lags.
- Conclusion: `(needs human input)`. The workload can still drive the cases with mysqlbinlog or a raw COM_BINLOG_DUMP client, so the question affects the priority of the property, not whether it can be implemented.


## Refinement (self-review, 2026-10-13)

The per-case `Sometimes` assertions only show that delivery happened at least once; they cannot catch a session that waits forever. Added a bounded workload `Always("ahead-of-flushed dump session received the event within T after the flush covering its position, in a quiet window")` for cases 1 and 2, evaluated only inside an `ANTITHESIS_STOP_FAULTS` quiet window with the harness quiet flag set, where T is calibrated from a baseline run (checkpoint interval + read_timeout + slack). Case 3 joins only if the owners choose "wait".
