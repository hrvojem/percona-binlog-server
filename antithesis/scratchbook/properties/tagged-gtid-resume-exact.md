# tagged-gtid-resume-exact — GTID-mode resume with tagged GTIDs (8.4/9.7 only) neither duplicates nor skips transactions

## What led here
- Tagged GTIDs (`uuid:tag:gno`) exist only on sources ≥ 8.3 (`protocol_traits_fwd.hpp:30-37`, `innovation83_with_tagged_gtids_protocol_server_version`). They change three version-specific code paths:
  1. the event type GTID_TAGGED_LOG (code 42) is parsed (`reader_context.cpp:777, 809, 978`);
  2. the stored `added_gtids` / `previous_gtids` JSON strings contain tags;
  3. the COM_BINLOG_DUMP_GTID payload switches to the **tagged encoding** (format byte 1, a different header layout and per-TSID tag field) whenever the set `contains_tags()` (`gtid_set.cpp:474-565`).
- Resume in GTID mode sends `previous_gtids ∪ added_gtids` of the tail (`collector_context.cpp:242-251`). If the tagged encoding is mis-sized or mis-ordered, the server either rejects the dump (fatal, loud) or interprets a different set (silent duplicates or skips).
- The 8.0 source can never produce tags, so on 8.0 the untagged path is used. The tagged encoding path is exercised only once the stored set contains a tag, which happens only after a tagged transaction was stored and PBS **reconnects**. `extra/qa/event_variety_test.py:112-123, 173` has a `tagged_gtid` variety with SIGKILL (skipped on < 8.4). It runs locally, not in CI, and kills by polling the file size.
- Hegel tests cover encode/decode round-trips in-process. They cannot check that the *server* interprets the encoding as intended.

## Failure scenario
The workload mixes untagged and tagged transactions (`SET GTID_NEXT='AUTOMATIC:tagA'`) on 8.4/9.7. PBS is killed at random, and network partitions force 2013 reconnects. On each reconnect the stored set (now tagged) is encoded with format 1. A bug in the tag encoding (e.g. tag-length field, TSID count in the tagged header, ordering of tags within a UUID) makes the server resume from the wrong place.

## Oracle
- After catch-up: the multiset of GTIDs in the stored bytes (parse the GTID / GTID_TAGGED events) equals the source's `gtid_executed`, with no duplicates. Assert `ALWAYS(no_duplicate_gtid_in_storage)` per check and `ALWAYS(stored_gtids ⊆ source_executed)`.
- `ALWAYS(stored added_gtids JSON == GTIDs parsed from the bytes)` per file (SG7).
- `SOMETIMES(reconnect happened while stored set contains tags)` (workload-side, from PBS's log line or by construction).

## SUT-side instrumentation (missing)
- In `collector_context::open_connection_and_switch_to_replication`: `SOMETIMES(gtids.contains_tags())` with the message "GTID dump sent with tagged encoding".
- `ALWAYS_OR_UNREACHABLE` right after `encode_to`: decoding the buffer yields a set equal to `gtids` (a cheap self-check; the Hegel test proves this offline, but under the real build it guards against build-specific miscompiles; compare ticket 4 Boost.ICL with Apple clang).

## Investigation findings (2026-10-13)

The PBS encoder was checked statically against the MySQL 9.7.1 server decoder (`/Users/hrvoje/work/percona-xtrabackup`). The two match on four points:

- **Header.** PBS `gtid_set.cpp` `header_encoder` produces `1<<56 | n<<8 | 1`. That is identical to the server's `encode_nsids_format` (`sql/rpl_gtid_set.cc:1351-1361`). The server decoder reads the format from the top byte and `n` from bits 8-55 (`:1364-1378`).
- **Mixed sets.** The server's own encoder (`Gtid_set::encode`, `:1388-1437`) uses the tagged format for the whole set when any TSID is tagged (`analyze_encoding_format`, `:1521-1535`). It writes an empty tag (one varlen length byte 0) for each untagged TSID, which is exactly the PBS layout. The decoder (`add_gtid_encoding`, `:1439-1519`, `Tsid::decode_tsid`, `libs/mysql/gtid/tsid.cpp:105-120`, `Tag::decode_tag`, `libs/mysql/gtid/tag.cpp:76-91`) accepts empty tags. It rejects only a missing tag field.
- **Tag length.** PBS writes MySQL's serialization varlen (`util/byte_span_inserters.hpp:221-268`, ported from `variable_length_integers.h`). Tags are at most 32 characters, so the length is always a single byte (`len<<1`).
- **Intervals.** The server requires strictly increasing, non-adjacent intervals (`start > previous end`, `rpl_gtid_set.cc:1492-1496`). PBS uses a joining `boost::icl::interval_set` (`gtid_set.hpp:111`), which never emits adjacent intervals. TSID order is not checked by the server.

The `@@GLOBAL.gtid_purged` text parse also matches. The PBS grammar (`gtid_set.cpp:69-273`) accepts `uuid:1-3:tag:1-2` and `,<whitespace>` separators, which is the server's text format. Unit tests cover tagged strings (`tests/gtid_set_test.cpp:311-327`).

One caveat: `gtid_set_parser::parse` (`:61-65`) does not check that the whole input was consumed. Trailing text would be ignored silently. This is not tag-specific, and the server never emits trailing text.

**Implication.** No static encoding mismatch exists, so the property keeps its scope and priority as a dynamic check. The remaining risk is server-side interpretation on a specific build, and the property catches that. The SUT `ALWAYS_OR_UNREACHABLE` decode-equals-input self-check is still worthwhile. No MTR test resumes against a real server with a tagged stored set: the tagged MTR tests only cover oversized-tag parsing.

## Open questions

- None.

### Investigation Log

#### Does the MySQL server accept the tagged encoding when the set also contains untagged intervals (mixed)?

- Examined: PBS `src/binsrv/gtids/gtid_set.cpp:474-565`, `tag.cpp:75-87` and `util/byte_span_inserters.hpp:221-268`. MySQL 9.7.1 `sql/rpl_gtid_set.cc:1351-1378, 1388-1437, 1439-1519, 1521-1555`, `sql/rpl_source.cc:1018` (the dump GTID decode call), `libs/mysql/gtid/tsid.cpp:98-120` and `libs/mysql/gtid/tag.cpp:66-91`.
- Found: the server emits and accepts exactly the mixed layout PBS produces (format 1 with an empty tag for untagged TSIDs), and the headers match bit for bit.
- Not found: 8.4 source locally. Tagged GTIDs date from 8.3, and 9.7 is the newer superset. There is no dynamic test against a server.
- Conclusion: resolved statically. Dynamic confirmation is what this property itself provides.

#### Is a tag-containing `gtid_purged` (read on an empty storage) parsed correctly?

- Examined: `collector_context.cpp:229-240`, the parser grammar at `gtid_set.cpp:61-273`, and `tests/gtid_set_test.cpp:311-327`.
- Found: the grammar covers `uuid(:tag)?:intervals` groups and multi-UUID sets with whitespace. Tags are accepted per the MySQL tag syntax `[a-zA-Z_][a-zA-Z0-9_]{0,31}`.
- Conclusion: resolved. This path is reached only when the source has purged binlogs before the first PBS run.
