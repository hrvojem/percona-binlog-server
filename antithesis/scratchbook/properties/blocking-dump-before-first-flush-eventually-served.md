# blocking-dump-before-first-flush-eventually-served — a blocking client that connects from the beginning before pull has stored anything is kept and eventually served (PBS-91 regression)

## What led here

Lifecycle focus (replica connects before the collector has data). PBS-91 (commit 0f56bd6) fixed a hard disconnect when a blocking COM_BINLOG_DUMP arrived while `binlog_records_` was empty. The fix is in `storage_core::fetch_event_block` (`storage_core.cpp:463-478`, "an empty storage is a transient condition during PBS startup"). It is guarded by `rs_blocking_dump_empty_storage.test` (one fixed interleaving: source down → PBS up → client → source up) and by the `sender_context` Hegel property (PROPERTY_TESTS.md:213: "An empty storage is polled … until the binlog arrives"). Neither explores the intermediate collector states between "empty" and "first transaction flushed", nor collector restarts and source flaps during the wait.

## Lifecycle states a client can land in (traced at 99de682)

The collector's start-up goes through these storage states. In each, a blocking client requesting `("", 4)` or `(<first name>, 4)` should poll, not disconnect.

1. **No storage objects / metadata only.** `fetch_event_block` returns true with an empty buffer (`:474-478`) → `finalize_fetch_eof` → 500 ms poll (`network_service.cpp:314-320`).
2. **First record exists, magic only** (after `open_new_binlog_file_internal`, `storage_core.cpp:680-703`, before the first flush). For `("",4)`: the name resolves to the first record, `read_offset 4 == size 4`, there is no next record → returns true, range length 0, and `binlog_name` is **left empty** (`:523-531`). The sender polls. For `(name,4)`: same EOF path. Without checkpointing this state lasts until the source rotates or PBS's `read_timeout` disconnect flush (LG3), which can be a long time.
3. **First flush landed.** The next fetch resolves the name, returns the block. `handle_binlog_file_switch(saved = "")` returns true (`sender_context.cpp:406-411`). `start_from_beginning` extracts the FDE from the block and emits artificial ROTATE + FDE.
4. **Collector reconnecting** (source down/flapping) while the client waits: storage stays in state 1 or 2; sessions are independent of the collector.

## Failure scenarios to explore

- Client arrives in state 2 and the first flushed block begins with a partial FDE because the client's previous empty fetch left a carry (should not happen: carry is empty in state 2 — worth a `Sometimes` anchor).
- Client requests `(binlog.000001, 4)` but the source's oldest retained binlog is `binlog.000005` (source purged before PBS's first start). Once records exist, `fetch_event_block` cannot find `binlog.000001` and returns false → session `co_return`s with no error packet (`network_service.cpp:308-313`). A real MySQL source sends error 1236 here. This is a different contract question (see Open questions), so the workload should request `("",4)` or the name it knows PBS will create.
- pull restarts (kill) while the client waits: the client is disconnected (acceptable) and must be served after reconnecting.

## Oracle

- Workload: start pull on an **empty** storage with random `checkpoint_size`/`checkpoint_interval` (including none) and the source randomly up/down. Attach 1..N `mysqlbinlog --read-from-remote-server --stop-never --raw` (or a light Python COM_BINLOG_DUMP client) at random moments relative to PBS start; then write transactions on the source and at some point `FLUSH BINARY LOGS`.
- `Sometimes(client connected while storage was empty or magic-only && later received the first FDE + at least one transaction)` per state. This is a liveness property: some timeline must show the client being served.
- `Always(a client connected before the first flush is not disconnected by PBS while PBS is running)` — tracked by the client seeing EOF/connection close without PBS having exited. Pair with the stream oracle from `pull-chain-prefix-consistency` / SG9 to check correctness once served.

## SUT-side instrumentation (missing)

- `storage_core::fetch_event_block` at `:474`: `SOMETIMES(true, "dump fetch on empty storage")`.
- At `:528` with `record_it` being the first record and `size == 4`: `SOMETIMES(true, "dump fetch on magic-only first binlog")`.
- `sender_context::handle_binlog_file_switch` empty-name branch (`:406-411`) after at least one prior EOF: `SOMETIMES(true, "empty-name client resolved after waiting")` — this is the PBS-91 success marker.

## Faults

Source container stop/start, network partition PBS↔source, PBS kill/restart, pause of the collector thread (via process pause) to stretch state 2. No node termination needed.

## Open questions

- None. Both earlier questions were resolved by scoping (see the Investigation Log). The "ERR 1236 vs silent close" question for a name that never appears belongs to `dump-session-ends-with-eof-or-err-packet`, and the freshness question belongs to LG3 / a freshness property, not to this one.

## Scope after investigation (2026-10-13)

- **In scope:** blocking requests for `("", 4)` and for `(<name PBS will create first>, 4)`. The workload learns that name from the source's `SHOW BINARY LOGS` (oldest retained file) before PBS starts on an empty storage.
- **Out of scope:** a request for a name that is not among the records once any record exists. This includes the "source purged before PBS's first start" case. The PBS-91 maintainer commit says this case must fail (see log), so it is not a wait case. Whether it fails with an ERR packet or a silent close is tested by `dump-session-ends-with-eof-or-err-packet`.
- **Latency:** the property stays an unbounded liveness property, `Sometimes`. With no checkpointing, state 2 ends only when the collector flushes. That happens when the source rotates, on the `connection.read_timeout` disconnect flush (`collector_context.cpp:191-200`) or at shutdown. So the workload must make sure one of these happens within the run. Options: `FLUSH BINARY LOGS`, a quiet period longer than `read_timeout`, or `checkpoint_interval`/`checkpoint_size` set in some timelines. Bounded freshness belongs to the LG3 discussion and is not part of this property.

No change to the invariant or the assertion types. The workload constraints above are new.

### Investigation Log

#### Should a blocking request for a non-existent binlog name older than the oldest stored file get ERR 1236 instead of a silent close? Does the case belong here or in the "ahead" property?

- Examined: `storage_core::fetch_event_block` (`src/binsrv/storage_core.cpp:436-562`, empty-storage branch `:463-478`, name lookup `:489-495`); `handle_binlog_dump_command` (`src/minimysql/network_service.cpp:278-337`); the PBS-91 fix commit 0f56bd6 (author Kamil Holubicki, on upstream/main); MySQL `Binlog_sender::check_start_file` (`percona-xtrabackup/sql/rpl_binlog_sender.cc:967-971`, MySQL 9.7.1 tree).
- Found: the PBS-91 commit message states the intent: "'binlog_name' is left as the caller passed it, so once records exist the next fetch still either resolves an empty name to the first binlog or fails with \"not found\" if the specifically requested binlog really isn't there. The specific binlog_name + non-empty storage case is unchanged - that path still correctly returns 'false'". The maintainer therefore treats only the empty storage as transient. A missing name with non-empty storage is a deliberate failure. On failure, `get_event` returns false and the session `co_return`s with no packet (`network_service.cpp:308-313`). MySQL answers the same request with the fatal error "Could not find first log file name in binary log index file" (ER 1236 to the client). PBS has a generic `generate_encoded_error` (`connection_context.cpp:296`), but nothing on the dump path calls it.
- Not found: any statement on whether the failure should be an ERR packet or a close.
- Conclusion: resolved for this property. The case is not "eventually served" in either reading, so it is out of scope for this property and for `blocking-dump-ahead-of-flushed-position-eventually-served`. The ERR-vs-close question moves to `dump-session-ends-with-eof-or-err-packet`. The workload must not request names older than the source's oldest retained binlog here.

#### With default config (no checkpointing), state 2 may last until the source rotates. Is that acceptable latency for PBS-91's intent?

- Examined: `storage::flush_event_buffer` call sites (`src/binsrv/storage.cpp:68-77, 175-214, 226-231`); `collector_context.cpp:189-203` (read-timeout disconnect); README "pull operation mode" and the `checkpoint_size` / `checkpoint_interval` docs; `fetch_event_block` magic-only path (`storage_core.cpp:520-531`).
- Found: without checkpointing, buffered data is flushed only at a file boundary (`storage.cpp:207-209`), on the read-timeout disconnect ("connection termination is a good place to flush ... the third kind of checkpointing", `collector_context.cpp:197-200`) and in the destructor at shutdown. The README presents checkpointing as the user's knob for how often data is persisted. PBS-91 only promises that the client "polls until the collector catches up" (commit message, comment at `storage_core.cpp:463-473`). In state 2, a `("",4)` client keeps `binlog_name_` empty because the resolved name is assigned only on a non-EOF return (`:520-531`), and it polls every 500 ms.
- Not found: a documented freshness bound for pull serving.
- Conclusion: resolved by scoping. PBS-91's contract is "poll instead of disconnect", not a latency bound. The property stays an unbounded `Sometimes` liveness property. The workload must trigger a flush (rotation, quiet period > `read_timeout`, or checkpointing in some timelines) so that `Sometimes` can become true. A latency bound is a separate freshness question (LG3).
