# Property discovery 10: Version Compatibility

Focus: MySQL source 8.0 / 8.4 / 9.7 differences, checksum on/off, tagged GTIDs, storage metadata format versions (opening storages from older or newer builds), encryption format transitions, and client versions against pull.

## Source-version transitions (ingress)

### source-upgrade-resume-continuity — Resume across an in-place source upgrade

| | |
|---|---|
| **Type** | Safety |
| **Property** | A storage that was being filled from a source that is then upgraded in place (8.0→8.4, 8.4→9.7) resumes after PBS kills and reconnects with no lost or duplicated transactions. Each stored file keeps its source file's own FDE version. |
| **Invariant** | Workload `Always`: after catch-up, stored GTIDs equal the source `gtid_executed` with no duplicates; in position mode each stored file is a byte prefix (exact after catch-up) of the source file; each stored file's FDE server version equals the source file's. SUT `AlwaysOrUnreachable` in `reader_context` FDE handling: when the FDE version is below the connection version, the adopted post-header table equals the known table for that version. SUT `Sometimes(fde_version < connection_version)` to prove the cross-version path is reached. These are invariants on every check, so `Always`. The cross-version branch is optional per run, so `AlwaysOrUnreachable`. |
| **Antithesis Angle** | Kill PBS before, during and after the source's shutdown, restart and first post-upgrade rotation. Network partitions force 2013 reconnects that land on the other side of the upgrade. Explores resume from the middle of a pre-upgrade file (pseudo-FDE of the old version) and GTID-mode resume across the upgrade-induced rotation (ticket 14 path). |
| **Why It Matters** | `event_view.cpp:72-96` claims mixed 8.0/8.4 FDE support, but the only test (`data_directory_8_0_to_8_4_upgrade.test`) fetches once into an empty storage after the upgrade and never resumes across it. A source upgrade is a routine operation for a continuous backup (W1/W2). |

Priority: P1. Confidence: medium (mechanism read from code; a cross-upgrade resume has never been run). Fault requirements: process kill of PBS, network faults; workload-driven mysqld binary swap over a shared datadir (no node termination needed if the workload restarts mysqld).

**Open Questions:**

- Can the harness swap mysqld binaries over one datadir (both versions in one image, or two containers sharing a volume)?
- Is GTID-mode byte identity across the upgrade rotation expected (same question as ticket 14)? `(needs human input)`
- Which upgrade paths are in scope (8.0→8.4, 8.4→9.7 only)?

### tagged-gtid-resume-exact — Tagged-GTID resume (8.4/9.7) is exact

| | |
|---|---|
| **Type** | Safety |
| **Property** | With a ≥ 8.4 source and a workload mixing tagged and untagged GTIDs, GTID-mode resume after any kill or reconnect sends a dump set the server interprets correctly: stored transactions are neither duplicated nor missing. |
| **Invariant** | Workload `Always`: no GTID appears twice in the stored bytes; stored GTIDs ⊆ source `gtid_executed`, and equal after catch-up; per file, `added_gtids` JSON equals the GTIDs parsed from the bytes. SUT `Sometimes(stored set contains tags at reconnect)` in `open_connection_and_switch_to_replication`, to prove the tagged encoding path is exercised. SUT `AlwaysOrUnreachable(decode(encode(gtids)) == gtids)` after `encode_to`. |
| **Antithesis Angle** | Kills and partitions at random points after the first tagged transaction is stored, so each reconnect uses the tagged COM_BINLOG_DUMP_GTID encoding (`gtid_set.cpp:474-565`) with varied set shapes (several tags per UUID, gaps). |
| **Why It Matters** | The tagged encoding is reached only on ≥ 8.3 sources *and* only on reconnect. It is covered offline by Hegel, and by `event_variety_test.py` locally (not CI). A wrong encoding silently duplicates or skips transactions in a backup. |

Priority: P1. Confidence: medium (version-specific path confirmed; no defect found by reading). Fault requirements: process kill, network partition/reset.

**Open Questions:**

- Does MySQL accept the all-tagged-format encoding for a set that mixes tagged and untagged intervals?
- Is a tagged `gtid_purged` read on an empty storage handled (only if the workload purges the source first)?

### source-restart-reauth-liveness — Re-authentication after a source restart

| | |
|---|---|
| **Type** | Liveness |
| **Property** | After the source restarts (which empties the `caching_sha2_password` cache), PBS eventually re-authenticates and stores new transactions, for every supported auth/TLS configuration and source version. |
| **Invariant** | Workload `Sometimes(new transaction stored after a source restart)`. For TLS configs, workload `Always(transaction committed T s after source is back appears in storage)`. SUT `Sometimes(reconnect succeeded after a failed connect)` and `Reachable("connect failed with auth error")` for triage. `Sometimes` because recovery has to happen at least once per run. |
| **Antithesis Angle** | Node termination or a workload restart of mysqld, plus optional `FLUSH PRIVILEGES`. Antithesis varies whether another client warms the auth cache before PBS's reconnect. |
| **Why It Matters** | Ticket 23 (validated: no `GET_SERVER_PUBLIC_KEY`). Non-TLS PBS works only while the cache is warm, so it fails permanently after a routine source restart. On 9.x there is no `mysql_native_password` fallback. |

Priority: P2. Confidence: high for the mechanism, low for in-contract status. Fault requirements: source restart (node termination or workload-driven).

**Open Questions:**

- Is non-TLS `caching_sha2_password` a supported config? If not, this reduces to TLS reconnect liveness. `(needs human input)`
- Does the packaged libmysqlclient 8.0.x behave identically against 9.7 in all TLS modes?

## Format changes between stored files (egress)

### pull-fde-switch-across-format-change — Pull clients get each file's own FDE across checksum or version changes

| | |
|---|---|
| **Type** | Safety |
| **Property** | When consecutive stored binlogs differ in checksum algorithm (source `SET GLOBAL binlog_checksum`) or in FDE version (upgrade), every pull client receives file N+1's FDE before any event of N+1, and every event parses with the footer rule of the most recent FDE received. |
| **Invariant** | Workload `Always(parse_ok)`: re-parse each client stream, applying the latest received FDE's checksum setting and verifying CRCs. Workload `Always(first non-artificial event of each file is its FDE)`. SUT `Sometimes(sender crossed a checksum-format boundary)` in `handle_binlog_file_switch`. SUT `Always("no event of a binlog is sent before its FDE")`. |
| **Antithesis Angle** | Timing of the toggle versus the collector's rotation versus a blocking client at EOF. This hits the N5 window (next file magic-only when the reader reaches EOF), where the sender skips ROTATE+FDE. With a format change, that skip turns into misparsed events instead of a cosmetic gap. |
| **Why It Matters** | Pull is the only restore path for encrypted storages (W5). A client using the wrong checksum assumption either fails or, with `--raw`, silently writes garbage files. No test serves a mixed-checksum storage. |

Priority: P1. Confidence: medium (contract from code comments; the N5 skip is verified in sut-analysis; the client reaction is unverified). Fault requirements: none beyond timing; process pause/CPU throttling of PBS widens windows.

**Open Questions:**

- Does real MySQL send N+1's artificial ROTATE with N's checksum alg, as the sender comment claims?
- Do mysqlbinlog versions fail loudly or silently on a wrong footer assumption?

### rewrite-file-fde-describes-events — Rewritten files' FDE describes all their events

| | |
|---|---|
| **Type** | Safety |
| **Property** | Every event stored in a rewrite-mode binlog is of a type, and has a post-header length, described by that file's own FDE, including when PBS reconnects to an upgraded source and reuses the open rewritten file. |
| **Invariant** | Workload `Always`: parse each rewritten file with only its own FDE; every type code is within that FDE's table and the post-header lengths match. SUT `AlwaysOrUnreachable(code known in current file's FDE version)` before writing a non-preamble event in `rewrite_and_process_binlog_event`. |
| **Antithesis Angle** | Kill and reconnect timing around a source upgrade, with tagged GTIDs produced only after the upgrade. Reuse of the open rewritten file (`is_fresh` → info-only FDE) versus `file_size` rollover decides whether a mixed file arises. |
| **Why It Matters** | In rewrite mode the source FDEs are skipped *before* the reader context sees them (`collector_context.cpp:290-334`). Generated FDEs use the connection version from when the file was created (`event_generation_helpers.cpp:120-131`), so an 8.0-era file can receive GTID_TAGGED_LOG events it doesn't describe. |

Priority: P2. Confidence: medium-low (path read from code; downstream rejection not verified). Fault requirements: process kill; source upgrade as in source-upgrade-resume-continuity.

**Open Questions:**

- Is a source upgrade supported for rewrite-mode storages at all? `(needs human input)`
- How often does a reconnect reuse the open rewritten file rather than roll it over?

### rewrite-checksum-none-fail-stop — Rewrite mode refuses checksum-less files without corrupting storage

| | |
|---|---|
| **Type** | Safety |
| **Property** | When the source switches to `binlog_checksum=NONE` mid-stream, rewrite-mode fetch/pull stops with an error, stores no event from the NONE file, and leaves a storage that opens, has a CRC on every stored event, and is unchanged except for whole transactions. |
| **Invariant** | Workload `Always(storage opens after failure)`, `Always(all stored events carry a valid CRC32)`, `Always(stored GTIDs ⊆ GTIDs of CRC32-era source files)`. SUT `Reachable("rewrite rejected checksum-less FDE mid-stream")` at `collector_context.cpp:318`. |
| **Antithesis Angle** | Toggle timing relative to buffered and unflushed events, checkpoints and PBS kills. In pull, the failure also ends the replication source. Antithesis explores the destructor flush after the exception. |
| **Why It Matters** | MTR checks only the deterministic pre-fetch toggle. A mid-stream toggle forces a permanent fail-stop loop, and the storage must survive every restart attempt intact. |

Priority: P2. Confidence: medium. Fault requirements: process kill.

**Open Questions:**

- Is the permanent wedge (until the source drops NONE files) the intended behaviour, versus adding or stripping CRCs (TODO `:311-313`)? `(needs human input)`
- In rewrite mode `footer_expected_` never follows the source FDE. Can `verify_checksum=false` plus a CRC32 source mis-size events?

### mixed-encryption-storage-served-correctly — Storages with mixed encryption settings read back exactly

| | |
|---|---|
| **Type** | Safety |
| **Property** | A storage written across restarts whose configs randomly enable, disable or rotate encryption (KEK A/B) is served by pull as exactly the source's events for every file, and each file's on-disk form matches its own `.json` envelope (plaintext iff no envelope). |
| **Invariant** | Workload `Always(pull stream minus artificial events == source binlog bytes)`. Workload `Always(file starts with plaintext magic ⇔ .json.encryption is null)`. SUT `Sometimes(per-file envelope presence != metadata.json encryption presence)` in `open_binlog`. SUT `AlwaysOrUnreachable(decrypted first 4 bytes == magic)` in the sender decrypt path. |
| **Antithesis Angle** | Kills inside `open_new_binlog_file_internal` (envelope generated before the `.json` exists), config changes between restarts, and concurrent pull clients reading across encrypted/plaintext boundaries and 1 MiB block boundaries. |
| **Why It Matters** | SG6 / N9. Pull is the only plaintext read-back path, and an encryption config change is an operator action that no test combines with an existing storage. |

Priority: P2. Confidence: medium. Fault requirements: process kill.

**Open Questions:**

- Is a plaintext tail continuing after encryption is enabled intended (N9)? `(needs human input)`
- Is removing an old KEK while its tail is active a supported fail-stop or a wedge?

## Storage format versions

### legacy-binlog-metadata-handled — Old-format storages are opened or refused cleanly; queries never crash or silently drop records

| | |
|---|---|
| **Type** | Safety |
| **Property** | A storage whose binlog `.json` lacks fields added later (pbs-0.2.1–0.2.3 lack `last_sequence_number`), or whose `.json` reads fail transiently, makes writers refuse cleanly. Query modes never crash, and never report `success` with fewer binlogs than the index lists. |
| **Invariant** | SUT `Unreachable("querying_only erased every binlog record")` before `storage_core.cpp:1001` (`front()` on empty vector). SUT `Sometimes(query mode skipped unreadable binlog metadata)`. Workload `Always(list status==success ⇒ listed names == index lines)`, and `Always(no query-mode process dies by signal)`. |
| **Antithesis Angle** | Mostly deterministic with a legacy fixture as an initial state. The timing-dependent part is S3 GetObject faults during `list`/`search_*`, which drive the erase-and-continue path (`:942-950`) and give a silent partial answer. |
| **Why It Matters** | Git-confirmed: `last_sequence_number` was added in pbs-0.2.4 without a version bump or default, so upgrading from 0.2.3 bricks the storage for writers and makes query modes hit UB. Partial `search_*` answers mislead PITR (W6). |

Priority: P2. Confidence: high (code and git history). Fault requirements: S3 fault proxy for the transient-read variant; none for the fixture variant.

**Open Questions:**

- Is upgrading a 0.2.1–0.2.3 storage supported? `(needs human input)`
- Should `list` report `warning` when it skipped records?

### older-build-refuses-newer-storage — A downgraded binary refuses storages with features it lacks

| | |
|---|---|
| **Type** | Safety |
| **Property** | Running a previous-release binary (e.g. pbs-0.3.x) on a storage written with features it does not understand (encryption, added in 0.4.0) fails before writing anything. It never appends plaintext to an encrypted tail or serves ciphertext as events. |
| **Invariant** | Workload `Always(older binary exited non-zero without changing any object)` when `metadata.json.encryption` is set. Workload `Always(current-binary pull stream == source)` afterwards. |
| **Antithesis Angle** | Random choice of binary per restart in a kill/restart loop. The old binary's recovery truncation runs on files it cannot interpret. |
| **Why It Matters** | `nv_tuple_from_json` ignores unknown keys, and both metadata versions stayed 1 when `encryption` was added, so nothing stops a downgrade from silently mixing plaintext into encrypted files. |

Priority: P2. Confidence: high (mechanism); in-contract status unknown. Fault requirements: process kill; needs a previous-release binary in the image.

**Open Questions:**

- Is binary downgrade in scope? `(needs human input)`
- Can the harness image carry a previous-release binary?

### query-modes-never-write-storage — Query modes never modify the storage

| | |
|---|---|
| **Type** | Safety |
| **Property** | `list`, `search_by_timestamp` and `search_by_gtid_set` never write any storage object, regardless of the encryption section in their config. |
| **Invariant** | SUT `Always(construction_mode_ == streaming)` in `storage_core::save_metadata`. Workload `Always(metadata.json unchanged across a query-mode invocation)`. |
| **Antithesis Angle** | Query invocations (with randomised encryption config) racing a live writer and each other. On the filesystem they collide on the deterministic `metadata.json.tmp`; on S3 it is last-writer-wins. |
| **Why It Matters** | Code-confirmed: `storage_core.cpp:174-178` calls `save_metadata()` in every construction mode when the config enables encryption on an unencrypted storage. A shared config file makes this the normal path for operators enabling encryption. |

Priority: P2. Confidence: high (mechanism); impact low to medium. Fault requirements: none (concurrency only).

**Open Questions:**

- Is the metadata.json encryption "upgrade" meant to be done by any mode? `(needs human input)`
- Will the workload randomise the encryption section for query invocations (it is needed to reach this path)?

## Client versions against pull

### pull-client-version-matrix — Every supported mysqlbinlog version completes a dump against pull

| | |
|---|---|
| **Type** | Reachability |
| **Property** | Each mysqlbinlog version shipped in the harness (8.0/8.4/9.7) completes the handshake and preamble against pull and receives events, including from storages holding FDEs of other versions. |
| **Invariant** | Workload `Sometimes(dump session for client version V received ≥ 1 non-artificial event)` per V. Workload `Always(no "unknown command" returned to a mysqlbinlog client)`. SUT `Reachable("unknown query from client")` with the query text. |
| **Antithesis Angle** | Low by itself. It guards every pull property against silent starvation caused by a client/storage version mismatch in the image. |
| **Why It Matters** | Pull answers only an exact-string allowlist (`network_service.cpp:487-550`) and always advertises "9.7.0-pbs". Ticket 20 shows that a client sending other queries fails completely. |

Priority: P2. Confidence: low (client preamble strings per version not verified). Fault requirements: none.

**Open Questions:**

- Which client versions are supported as pull clients? `(needs human input)`
- Which exact preamble strings does each mysqlbinlog version send?

## Areas examined

- `src/binsrv/events/protocol_traits_fwd.hpp`, `protocol_traits.cpp`: the two known layouts (8.0: 42 events; ≥ 8.3: 43). An FDE post header larger than the 8.3 layout is rejected, so a future MySQL adding an event type is fatal (deterministic, which CI would catch).
- `reader_context.cpp:36-53, 579-582, 607-700`: FDE checks (binlog version 4, header 19, FDE version ≤ connection version, post-header validation per FDE version), `footer_expected_` taken from FDE.
- `event_view.cpp:50-145`: FDE post-header size derived from the event size (explicit 8.0→8.4 upgrade support), FDE always has a footer, CRC verification rules.
- `easymysql/connection.cpp:115-222`: the dump flags (`MYSQL_RPL_SKIP_HEARTBEAT`, GTID, non-block) and the checksum SET (both `@source_` and `@master_` names). There is no `GET_SERVER_PUBLIC_KEY`.
- `collector_context.cpp:125-430`: reader context built from the connection version; rewrite skips source preamble events before the context sees them; generated FDE version; checksum enforcement in rewrite.
- `sender_context.cpp:81-140, 225-330, 552-567`: per-file FDE, checksum handling and the fake version-0 context, which is safe for FDEs.
- `minimysql/network_service.cpp:480-616`, `connection_context.hpp:41`: client preamble allowlist, advertised version.
- `binlog_file_metadata.{hpp,cpp}`, `storage_metadata.{hpp,cpp}`, `*_fwd.hpp`, `util/nv_tuple_from_json.hpp`: schema and versions per release tag (git), handling of missing and unknown keys.
- `storage_core.cpp:125-215, 632-720, 800-835, 927-1050`: metadata load/validate/save across modes, per-file encryption decisions, querying-only record erasure.
- `gtids/gtid_set.cpp:300-565`: tagged and untagged binary encoding.
- MTR: `data_directory_8_0_to_8_4_upgrade.test`, `gtid_rewrite_enforce_checksums.test`, `fde_invalid_server_version.test`, encryption tests. QA: `event_variety_test.py` (tagged GTIDs, compression, XA under SIGKILL). CI matrix: 8.0.45 / 8.4.8 / 9.7.2, file backend only.
- Not property-worthy here: ticket 1 (FDE version parsing, deterministic, covered by Hegel); ticket 20 (replicas, a feature gap); TRANSACTION_PAYLOAD (same layout in all versions; covered by `event_variety_test.py`); source downgrade (unsupported by MySQL; PBS fails stop on FDE version > connection version, which is the correct behaviour).

## Cross-property assumptions

- A source upgrade in Antithesis requires the workload to restart mysqld with a different binary over the same datadir. The properties source-upgrade-resume-continuity and rewrite-file-fde-describes-events depend on that harness capability.
- Byte-identity oracles apply to non-rewrite, unencrypted storages. Encrypted and rewrite storages are compared through the pull-served event stream or GTID sets (as in sut-analysis).
- "Supported" source versions are those in the CI matrix (8.0.45, 8.4.8, 9.7.2). Tagged-GTID properties apply only to ≥ 8.4.
- The harness should run a debug or ASan build so that `front()`-on-empty and the other assert-guarded paths are observable rather than silent UB.
- The previous-release-binary property needs a second PBS binary in the image. Without it, that property is documentation only.
