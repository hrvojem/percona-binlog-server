# chained-fetch-from-pull-reproduces-upstream

## What led to it

The replication source (`sender_context`) is the least settled code (rewritten in one week, PBS-64/65/91). Its existing oracles are `mysqlbinlog` round trips (`mtr/binlog_streaming/include/rs_roundtrip_body.inc:42-114`) and the C++ sender properties. Both are lenient readers. PBS has its own **strict** binlog stream validator: `reader_context`, with an FSM (artificial ROTATE → FDE → PREVIOUS_GTIDS? → GTID… → ROTATE|STOP), position continuity (`reader_context.cpp:905-927`), the position-mode artificial-ROTATE position check (`:547-565`), FDE validation (`:630-665`), and 32 `logic_error` raise sites. Pointing a second PBS **`fetch`** (position mode) at the first PBS's **`pull` listener** turns all of those checks into an oracle on the serving path. It also gives a byte-level end-to-end equality check: downstream storage == upstream storage (decrypted).

Nobody else proposed PBS-as-client-of-PBS. `pull-chain-*` properties cover source → PBS → generic clients.

## Why it should work today (validated in code)

- The handshake reports server version `9.7.0-pbs` (`minimysql/connection_context.hpp:41`), which is ≥ any stored FDE version, so the FDE version check at `reader_context.cpp:647-652` passes.
- Auth uses a caching_sha2 fast-path scramble check (`connection_context.cpp:111-117`). libmysqlclient-based `mysqlbinlog` already authenticates in MTR.
- PBS `fetch` sends `SET @source_binlog_checksum = 'CRC32', @master_binlog_checksum = 'CRC32'` (or `'NONE'`) (`easymysql/connection.cpp:214-223`), which matches a `known_queries` entry exactly (`network_service.cpp:539-550`).
- Position mode on an empty downstream storage sends COM_BINLOG_DUMP with an empty name (`collector_context.cpp:253-255`), which the sender resolves to the oldest binlog (`sender_context.cpp:65-72`).
- `fetch` uses the NON_BLOCK flag (`connection.cpp:197-208`). The listener sends EOF at the end (`network_service.cpp:315-334`), so `fetch` reaches EOF and exits 0.
- GTID-mode downstream cannot work (ticket 20: COM_BINLOG_DUMP_GTID is "unknown command"). Downstream must be position mode, though the upstream can be either mode.

## What it catches

- **N5** (no artificial ROTATE when the next binlog is still magic-only), **only with a blocking downstream (`pull`)**. With a downstream `fetch` (NON_BLOCK), the session ends at the first EOF (`network_service.cpp:314-317`). The N5 empty fetch is itself an EOF, so the stream never continues into N+1 within that session, and the next session resumes at `(N, size)`, which handles the switch correctly. With a downstream `pull`, the reader gets N+1's stored FDE without an artificial ROTATE before it:
  - after a real ROTATE/STOP, in `rotate_artificial_expected` it raises "an artificial rotate event must be the very first event in the rotate_artificial_expected state" (`reader_context.cpp:526-530`);
  - after a file with no terminator, in `any_other_expected` it raises "encountered second format description event within the same binlog".
  - The downstream pull's collector thread rethrows and pull exits non-zero (`pull_operation.cpp:153-165`).
- Ticket 26 / PBS-96 (dump from position on an empty storage), and regressions of tickets 21 and 22.
- Wrong artificial-ROTATE position on start-from-offset, a wrong artificial FDE (`next_event_position`/`create_timestamp`/CRC), and gaps or duplicates at file switches (continuity check).
- Decryption errors on the serving path: the downstream stores plaintext, which must equal the upstream decrypted bytes.
- Ticket 10 (≥ 16 MiB event) shows up as a downstream failure. The workload must expect it while that ticket is open.

## Oracle

At quiescence (upstream pull caught up, downstream fetch exited 0):
- For every upstream binlog N, the downstream has an N with bytes equal to `upstream[N][0:upstream_size_at_fetch_end]`. The downstream must not be longer than the upstream flushed size, and only the downstream tail may be shorter (data flushed upstream after the fetch's last read).
- Two allowed exceptions. The downstream may lack a **trailing** upstream file that is magic-only: the sender serves nothing from it, and N5 means no ROTATE names it. A magic-only **non-tail** upstream file must not be excused. It cannot come from normal operation (ticket 14 in GTID mode, or the source-crash-after-rotation path in `binlog-metadata-matches-stored-bytes`), and the sender skips straight to the next file, so the downstream index lacks it. That divergence should fire.
- Downstream fetch exit 0 ⇒ the above holds.
- A repeated downstream fetch (resume at the downstream tail) is a no-op or appends only new upstream data. This exercises the sender's start-from-offset path at every downstream restart.

## Antithesis angle

- Kill or partition the **downstream** fetch at random to exercise the sender's start-from-offset path at arbitrary stored offsets, including offsets that equal the upstream tail size, the N5 window, and offsets mid-way through a rotation.
- Kill or pause the **upstream** pull mid-serve and partition upstream S3, so that serving overlaps rotation, checkpoints and recovery truncation on the upstream side.
- Run both the encrypted and the unencrypted upstream.

## Instrumentation suggestions

- Workload (missing): `ALWAYS(downstream storage is a byte prefix of upstream storage, per file, "chained PBS fetch reproduces upstream storage")`.
- Workload (missing): `ALWAYS(downstream fetch never exits with a reader_context logic_error, "chained PBS fetch never rejects the stream served by pull")`, which classifies the exit message from stderr/log.
- Workload (missing): `SOMETIMES(downstream fetch resumed at an offset equal to the upstream tail size while the upstream had a magic-only next binlog)`. This needs the workload to observe the upstream index (via `list`) at fetch start.
- SUT-side: none needed. The point is to reuse PBS's existing validators.

## Property changes from investigation

- The scope splits by downstream mode. A downstream `fetch` exercises the start-from-offset path, file switches between sessions, and the byte-prefix oracle, but it **cannot** catch N5. Catching N5 needs a downstream `pull` (blocking) as well, with its own listener port and storage. Add `Sometimes("chained downstream pull was at EOF when the upstream created a magic-only next binlog")`.
- The oracle gains the trailing-magic-only exception described above.
- Assertion types and priority are unchanged.

## Open questions

- Is chaining PBS → PBS a supported topology, or only a test device? If it is only a test device, failures that come purely from the downstream fetch's assumptions about a real MySQL (none known) should be triaged as harness issues. `(needs human input)`

### Investigation Log

#### Is chaining PBS → PBS a supported topology, or only a test device?

- Examined: README.md (introduction, `replication_source` section :597-607), all `mtr/binlog_streaming/t/rs_*.test`, PROPERTY_TESTS.md, ticket snapshot.
- Found: the README describes the listener as serving "replicas" and "downstream replica" connections, and every MTR `rs_*` test uses `mysqlbinlog` as the client. Nothing mentions PBS as a client of PBS. The handshake facts in "Why it should work today" still hold.
- Not found: any statement of support or non-support.
- Conclusion: `(needs human input)`. The property is valuable as a test device either way.

#### Does the downstream PBS reader treat the upstream's served STOP / real ROTATE / no-terminator variants (sender comment 4a-c) identically to a real MySQL stream?

- Examined: `reader_context.cpp:749-830` (artificial ROTATE / ROTATE / STOP in `gtid_log_expected` and `any_other_expected`), `:859-905` (`rotate_or_stop_expected`), `:547-565` (position checks on the artificial ROTATE), `collector_context.cpp:493-578`, `sender_context.cpp:466-523`, `storage_core.cpp:520-541`, PXC 9.6 `sql/rpl_binlog_sender.cc:401-416` (MySQL also sends a fake ROTATE before every next file, including after case 4c).
- Found:
  - 4a, real ROTATE: it is validated (post-header position 4, next_event_position continuity), the reader moves to `rotate_artificial_expected`, and the served artificial ROTATE(N+1,4) passes the "new file, position 4" check. This is the same sequence a MySQL source sends.
  - 4b, STOP: handled the same way.
  - 4c, no terminator after a complete transaction: the artificial ROTATE(N+1) arrives in `any_other_expected` or `gtid_log_expected`, and `finish_transaction()` passes. The downstream collector closes N on the name change (`collector_context.cpp:543-551`). The downstream N then ends exactly where the upstream N does, as with MySQL.
  - Divergence found: the upstream may hold a magic-only **non-tail** file, which a MySQL source never has (ticket 14; the source-crash path). The sender reads 0 bytes from it and goes straight to the next file, and its ROTATE names that next file. The downstream accepts this without error (a real ROTATE naming N followed by an artificial ROTATE naming N+1 is not cross-checked), but it never creates N.
- Conclusion: resolved. The strict reader accepts all three variants as it would from MySQL. The magic-only non-tail divergence is an upstream defect that the oracle should report; the oracle text above says so.

#### (Found while tracing) Can a downstream `fetch` observe N5 at all?

- Examined: `minimysql/network_service.cpp:296-336` (non-blocking: an empty event leads to `break`, then EOF), `sender_context.cpp:484-491`, `easymysql/connection.cpp:197-208` (fetch uses NON_BLOCK).
- Found: no. In a non-blocking session the N5 empty fetch is reported as EOF and the session ends. The next session starts with `start_from_offset` at the downstream tail, and the switch is then detected normally (`saved_binlog_name` N differs from N+1).
- Conclusion: the property claim about catching N5 is restricted to a blocking downstream (`pull`). Recorded under "Property changes".
