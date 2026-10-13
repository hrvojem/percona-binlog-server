# dump-preamble-matches-request — Every dump session starts with an artificial ROTATE naming the requested file and position, followed by that file's FDE, whatever the storage is doing at that moment

## What led to it

Protocol-contract focus: the start-of-stream sequence that MySQL clients depend on. The contract is written out in `sender_context.cpp:81-140`:
- request `(name, 4)` or `("", 4)`: artificial ROTATE (`artificial` flag, timestamp 0, next_event_position 0, position 4, binlog = requested name, or the oldest available file for an empty name; checksum per the session's `@source_binlog_checksum`), then the file's real FDE as stored;
- request `(name, p ≠ 4)`: artificial ROTATE with position `p`, then an **artificial FDE** (the file's FDE with next_event_position and create_timestamp zeroed and the CRC recomputed), then events from `p`. Both must be sent even when `p` equals the file size.

`mysqlbinlog --raw` names its output files from that ROTATE (`mtr/binlog_streaming/t/rs_raw_mode.test` header). Two of the three fixed tickets are regressions here: ticket 21 (source ignored the requested binlog/position, fix at `sender_context.cpp:61-72`) and ticket 22 (`--raw` failed without the artificial ROTATE/FDE). Ticket 26 / PBS-96 (open, in `known_failures.txt`) is a third case: a from-position request on an empty storage disconnects instead of waiting.

The Hegel spec (`tests/sender_context_property_test.cpp:16-35`) checks the preamble against a **static** storage. What it cannot cover is the preamble when the request races storage changes: the first ever flush, the collector creating the requested file, a rotation, an external purge while pull's in-memory index is stale (ticket 9), a pull restart.

## Code paths

- `sender_context` constructor (`sender_context.cpp:52-76`): FSM start state from the position; empty name kept empty for resolution by `fetch_event_block`.
- `handle_start_states` (`sender_context.cpp:525-575`): `start_from_offset` reads the FDE directly (`fetch_fde_from_storage`, `:210-256`); `start_from_beginning` takes it from the first fetched block (`extract_fde_from_event_block`, `:258-273`). The ROTATE is generated from `binlog_name_`, which for an empty request is whatever `fetch_event_block` resolved (`storage_core.cpp:482-488`: the first record in pull's *in-memory* list).
- Empty storage → EOF and polling (`storage_core.cpp:474-478`), except `start_from_offset`, which needs the FDE and fails (ticket 26).
- Requested file magic-only (just created): `start_from_beginning` sees EOF and keeps polling with the requested name, then emits the preamble when data arrives. If the next file already exists at that point, the fetch jumps to it and the ROTATE names that next file instead of the requested one (`storage_core.cpp:523-534` and `handle_binlog_file_switch`).

## Failure scenarios

- **Out of contract, workload-gated.** A client asks for `("", 4)` right after an external purge that ran while pull was running. Pull's stale in-memory list resolves to a purged file. Either the FDE read fails and the session closes silently, or, on a filesystem race, the preamble names a file that `list` no longer reports. README:473 says a purge on a storage that another instance is fetching or pulling into "may cause data corruption and should be avoided". The workload must not purge under a running pull. In the in-contract variant (stop pull, purge, restart pull), pull reloads the index at start, so the list is fresh.
- A client asks for `(binlog.000007, 4)` while 000007 is magic-only and 000008 already exists. The first ROTATE names 000008, so a `--raw` client writes the stream under a name it did not ask for. A normal rotation cannot produce this: `close_binlog` flushes 000007's FDE, PGTIDS and ROTATE before 000008 is created, so a poller sees 000007 grow first. The state needs a magic-only **non-tail** file, which only PBS defects create:
  - ticket 14 (GTID mode);
  - in position mode, a source SIGKILL right after a rotation (traced in `binlog-metadata-matches-stored-bytes`, scenario 6). 000007's preamble is dropped at the close, and 000007 stays magic-only forever.
- A client asks for `(binlog.000007, p)` before the first flush (ticket 26): disconnect.

## Oracle

Workload raw dump client (or `mysqlbinlog --raw` plus file inspection), for every session:
- `Always(first event is an artificial ROTATE with timestamp 0 and next_event_position 0)`: message "dump: first event is artificial ROTATE".
- `Always(ROTATE.position == requested position)` and `Always(ROTATE.binlog == requested name, or == the first entry of the index at request time for an empty name)`: message "dump: artificial ROTATE matches request".
- `Always(second event is an FDE; artificial with zeroed next_event_position and create_timestamp iff position ≠ 4; its CRC verifies when the FDE says CRC32)`: message "dump: FDE follows ROTATE and matches position mode".
- `Sometimes(request issued while the requested file was magic-only)`, `Sometimes(request issued on an empty storage)`, `Sometimes(empty-name request issued within one second of a pull restart that followed a purge)`. The last one replaces "within one second of a purge": a purge concurrent with pull is out of contract (README:473).

## SUT-side instrumentation (missing)

- In `handle_start_states`: `ALWAYS(first_preamble implies binlog_name_ == requested name when one was given, "sender: preamble names the requested binlog")`.
- `SOMETIMES(start_from_offset with position == file size)` to make sure the boundary case runs.

## Antithesis angle

All interesting cases are timing races between a client's request and the collector (first flush, rotation, file creation) or an external purge. Antithesis drives requests at varied moments relative to those events.

## Fault requirements

None for the races themselves (workload concurrency). Pull kills/restarts and a purge task widen them. No clock or node-termination faults needed.

## Property changes from investigation

- The purge-race case is removed from scope (out of contract per README:473), and the matching `Sometimes` is redefined around the in-contract stop/purge/restart sequence.
- The "requested magic-only file, later file appears first" case is reachable only through a magic-only non-tail file, which is itself a defect. When the requested-name `Always` fires, triage should check the storage for such a file before blaming the sender.
- Invariant, assertion types and priority are unchanged.

## Open questions

- When the requested file is magic-only and a later file exists (reachable only through a magic-only non-tail file, from ticket 14 or a source crash after rotation), should the server send ROTATE(requested), skip to the next file as it does now, or refuse with an ERR? `(needs human input)`

### Investigation Log

#### When the requested file is magic-only and a later file appears first, should the server send ROTATE(requested) + its (absent) FDE, or skip to the next file?

- Examined: `storage_core.cpp:436-565` (`fetch_event_block`: read_offset == size moves to the next record), `sender_context.cpp:466-523` (`handle_binlog_file_switch` with a non-empty `saved_binlog_name`), `storage.cpp:204-215` (`close_binlog` flushes before closing), `collector_context.cpp:493-578` (close N before open N+1), the scenario 6 trace in `binlog-metadata-matches-stored-bytes`, ticket 14.
- Found: the sender's request for `(N, 4)` on a magic-only N reads 0 bytes from N, moves to N+1, and the switch handling sets `start_from_beginning`, so the ROTATE names N+1. In normal operation N cannot stay magic-only once N+1 exists: the collector writes N's FDE, PGTIDS and closing ROTATE together at `close_binlog`, before `open_binlog(N+1)`. Only two defects leave N magic-only behind a later file: ticket 14 (GTID) and a source crash right after rotation (position mode).
- Not found: maintainer intent for how the sender should serve a file it cannot serve faithfully.
- Conclusion: `(needs human input)`, with the partial finding that the case is a downstream symptom of a storage defect.

#### For an empty-name request, is "oldest in pull's in-memory list" acceptable when that list is stale after a purge (ticket 9)?

- Examined: README.md:473, `storage_core.cpp:369-420` (`purge_binlogs` runs in a separate process with `purging` construction mode), deployment-topology.md (the serial purge driver holds the writer lock).
- Found: README:473 says running `purge_binlogs` on a storage while another instance fetches or pulls into it "may cause data corruption and should be avoided". Pull reads the index only when it starts.
- Conclusion: resolved. This is out of contract, and the case is workload-gated. Pull must be stopped during a purge, and the `Sometimes` targets the empty-name request right after the pull restart.

## Evaluation refinements (2026-10-13)

Applied by the evaluation synthesis (`../evaluation/synthesis.md`). Where this section and earlier text in this file disagree, this section wins; the catalog entry has been updated to match.

- The empty-name-after-purge `Sometimes` needs the `pull_fs` retention-cycle driver (stop pull, purge, restart; topology change, gap G7). Until it exists the marker is deferred, so it does not report as an unmet `Sometimes` (antithesis-fit F4).
- Runs in `pull_fs`.
