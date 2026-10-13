# Property discovery 06: Security boundaries

Focus: authentication and authorization on pull's replication source, encryption at rest (plaintext never reaching storage, keyring and envelope handling, CTR keystream use under crash recovery), TLS to the MySQL source, and malformed or hostile replica traffic. Only properties where faults or timing matter are prioritised. Pure parsing (tickets 1, 3, 5, 8, 11, 12) is left to Hegel.

## Encryption at rest

### encrypted-storage-no-plaintext-at-rest — No plaintext binlog content at rest in an encrypted storage

| | |
|---|---|
| **Type** | Safety |
| **Property** | Once a storage is configured as encrypted, no binlog content committed after that point appears in plaintext in any at-rest location: storage objects, the S3 local buffer (`fs_buffer_directory` or `/tmp/<uuid>`), or PBS logs. |
| **Invariant** | Workload `Always`: per-transaction random markers committed while encryption was configured never appear in a byte scan of those locations. Workload `Always`: if `metadata.json` says encrypted, every `<name>.json` created after enablement has a `file_key_envelope`. SUT-side `ALWAYS(!encryption_format_ \|\| encryption_record)` in `storage_core::write_data_to_stream` (`storage_core.cpp:1125-1132`). `Always` because a single plaintext leak is a violation. |
| **Antithesis Angle** | Restarts and kills at arbitrary points (mid-transaction, around rotation, inside the constructor's `save_metadata` at `:177`), optionally with encryption toggled between runs. The plaintext window depends on when the source rotates relative to the toggle. S3 local-buffer leaks on SIGKILL multiply the at-rest locations. |
| **Why It Matters** | README:673 claims data files are encrypted before they are written. Code-confirmed N9: enabling encryption on an existing storage keeps appending plaintext to the tail (`:172-178`, record `encryption = nullopt`). Disabling it creates plaintext binlogs under an "encrypted" `metadata.json` (`validate_metadata` `:808-826`; `generate_binlog_encryption_record` `:1008-1012`). |

Priority: P1. Confidence: high (mechanism verified in code); whether it is a bug depends on intent.
Fault requirements: process kill/restart (PBS container, or workload-driven restart); no network faults needed. S3 variant needs MinIO/Versity.

**Open Questions:**

- Is the plaintext tail after enabling encryption intended (README:610 tolerates mixed storages)?
- Should disabling encryption on an encrypted storage be rejected?
- Should the workload toggle encryption at all, or only run encryption-always-on (in which case the property is a crash-path regression guard)?

### encrypted-binlog-decryptable-after-crash — Encrypted binlogs decrypt to valid events after any crash

| | |
|---|---|
| **Type** | Safety |
| **Property** | After any kill or storage fault, every indexed binlog of an encrypted storage decrypts, with its persisted envelope and the keyring, to a magic-prefixed, CRC-valid event chain up to its metadata size. |
| **Invariant** | Workload `Always` after each successful reopen: independent decryption (unwrap the file key with the KEK, AES-CTR from offset 0) gives magic + a valid `next_event_position` chain + valid CRC32 for every event. The cross-check that pull's served stream equals the workload's decryption is shared with SG9. SUT-side `ALWAYS(decrypted first 4 bytes == magic)` at `fetch_event_block` offset 0 (`storage_core.cpp:544-557`). |
| **Antithesis Angle** | Kills inside rotation (envelope exists only in memory until `<name>.json` is PUT, `:680-703`), checkpoint (`:299-322`), recovery truncation (`:960-985`), the reopen-at-offset-0 magic rewrite (`:715`), and the destructor re-flush (ticket 18). |
| **Why It Matters** | Encryption has never been crash-tested (sut-analysis surface 8). Pull is the only in-product decryption path, so an undecryptable file is silent data loss that shows up only at restore. |

Priority: P1. Confidence: medium (holds by code reading on the in-contract paths; value is finding unknown crash windows).
Fault requirements: process kill; optional S3 5xx/ENOSPC shims (`s3_fault_proxy.py`, `fsfault`) on `.json` PUTs.

**Open Questions:**

- Should the workload decrypt independently (needs a Python implementation of the envelope format) or rely on pull only?
- Is a binlog with a `.json` but zero-length data reachable after a crash (the `open_existing` offset-0 path)?

### ctr-keystream-not-reused-across-truncation — CTR keystream never reused for different plaintext

| | |
|---|---|
| **Type** | Safety |
| **Property** | For a given binlog envelope (file key + IV), the ciphertext byte at any offset never changes value; a change means two different plaintexts were encrypted under the same keystream. |
| **Invariant** | Workload `Always`: across snapshots of each encrypted binlog keyed by `(name, iv_hex)`, including leaked S3 local-buffer copies and old S3 object versions, the overlapping byte ranges are equal. SUT-side `SOMETIMES("recovery truncated an encrypted tail")` at `storage_core.cpp:979` confirms the precondition. `Always` because one reuse is a confidentiality break. |
| **Antithesis Angle** | Kill between the data append and the metadata save in `write_event_block`; restart truncates the tail and re-encrypts new bytes at the same offsets with the reused envelope (`:307`, `:715`). Different plaintext arises in rewrite mode (the generated ROTATE carries `now()`, `collector_context.cpp:376-385`, and resume reuses the tail name, `:367-370`), and after a source RESET or failover with the same binlog names (ticket 25). |
| **Why It Matters** | AES-CTR nonce reuse leaks P1 ⊕ P2. Old ciphertext persists in PBS's own leaked `/tmp/<uuid>` copies (N10), in S3 versions and in backups. |

Priority: P2. Confidence: medium (mechanism read in code; the exact rewrite-mode resume byte layout was not executed).
Fault requirements: process kill; workload-driven source RESET/failover for the non-rewrite variant; MinIO versioning optional.

**Open Questions:**

- Does rewrite-mode resume write different bytes at exactly the truncated offset? (partial: resume name reuse confirmed in `collector_context.cpp:367-370`)
- Is the availability of old ciphertext (versioning, backups, local disk) inside the threat model?
- Does the harness S3 server have versioning enabled?

### cipher-offset-equals-physical-offset — Keystream offset matches physical write offset

| | |
|---|---|
| **Type** | Safety |
| **Property** | Every encrypted write is encrypted for exactly the physical offset at which it lands in the binlog object. |
| **Invariant** | SUT-side `ALWAYS(offset == backend physical stream size before write)` in `storage_core::write_data_to_stream` (`:1125-1160`). This needs a stream-size accessor on the backend. The black-box fallback is the CRC-valid decryption check of `encrypted-binlog-decryptable-after-crash`. `Always` because any divergence makes all later bytes of that file undecryptable. |
| **Antithesis Angle** | The logical offset (`record.size`) and the append-mode physical EOF diverge under a concurrent purge truncating a live tail (N1, `:974-983`), under two writers (ticket 24), and transiently under an S3 data-upload failure followed by the destructor re-flush (ticket 18). Thread pausing and kills between processes explore these. |
| **Why It Matters** | On an encrypted storage the divergence is invisible to byte-length checks and is found only when decryption produces garbage, possibly feeding ticket 7's `event_size` hang on the sender. |

Priority: P2. Confidence: medium (divergence paths are code-confirmed; whether concurrency misuse is in contract is open).
Fault requirements: process kill; concurrent purge/fetch processes; S3 5xx shim for the ticket-18 path.

**Open Questions:**

- Is concurrent purge or a second writer in contract (README:473)? This decides "never happens" vs "fails closed".
- Should the self-healing S3 ticket-18 transient count as a violation?

## TLS and authentication to the source

### source-auth-survives-auth-cache-flush — PBS re-authenticates after the source's auth cache is cleared

| | |
|---|---|
| **Type** | Liveness |
| **Property** | After a source restart, `FLUSH PRIVILEGES` or `ALTER USER`, which empty the caching_sha2_password cache, a running pull (or the next fetch) eventually re-authenticates and resumes streaming. |
| **Invariant** | Workload `Sometimes(pbs resumed after auth cache flush)`, plus `Always(!(pbs_alive && quiet_period_elapsed && no Binlog Dump thread for pbs user on source))`. SUT-side `SOMETIMES(reconnect succeeded after failed attempt)` and an errno-tagged `REACHABLE` on the connect-failure branch (`collector_context.cpp:209-216`). |
| **Antithesis Angle** | Whether full authentication is needed depends on the interleaving of source restarts, cache flushes and other TLS logins of the same user. With `ssl.mode=disabled`, PBS cannot do full auth (no `MYSQL_OPT_GET_SERVER_PUBLIC_KEY`, `connection.cpp:225-291`). Pull then retries forever, silently stale. |
| **Why It Matters** | Ticket 23 (validated in code: no public-key option anywhere in `src/`). On MySQL 9.x non-TLS has no workaround. A backup that silently stops after a routine source restart is a W1 failure. |

Priority: P1. Confidence: high (mechanism validated; the TLS variant is expected to hold, and the disabled variant is expected to fail).
Fault requirements: source restart (node termination, or a workload-driven `RESTART`/`FLUSH PRIVILEGES`); partitions to force reconnects.

**Open Questions:**

- Is non-TLS caching_sha2 meant to be supported (i.e. is ticket 23 going to be fixed)? This decides whether the disabled variant is a regression test or a known-failure tracker.
- Is source container termination enabled in the tenant?
- Which errno does PBS see on the failure (2061 vs 1045)?

### source-session-tls-when-configured — Every source connection is TLS when TLS is required

| | |
|---|---|
| **Type** | Safety |
| **Property** | When `connection.ssl.mode` is `required` or stricter, every connection PBS opens to the source, including reconnects and the setup-query connection, is TLS-encrypted. |
| **Invariant** | Workload `Always(CONNECTION_TYPE == 'SSL/TLS')` for every `performance_schema.threads` row of the PBS user. `Sometimes(observed pbs thread after a reconnect)`. SUT-side `ALWAYS(mysql_get_ssl_cipher(conn) != nullptr)` after connect when the mode is `required` or above. |
| **Antithesis Angle** | Partitions and source restarts force many reconnects, and each one rebuilds the `MYSQL*` from config (`collector_context.cpp:209-211`, `connection.cpp:291-321`). Polling races short-lived connections. |
| **Why It Matters** | Credentials and binlog data cross the network. Guards against a regression that drops TLS options on reconnect. `preferred`'s documented cleartext fallback is noted but not asserted. |

Priority: P2. Confidence: high that it holds today (regression guard).
Fault requirements: network partitions; source restart.

**Open Questions:**

- Is `performance_schema` enabled on the harness source?
- Should a `preferred` cleartext fallback be flagged?

## Pull replication source (minimysql) boundary

### rs-unauthenticated-session-never-served — No data or OK to unauthenticated clients

| | |
|---|---|
| **Type** | Safety |
| **Property** | A client that did not present the correct caching_sha2 scramble for the configured user never receives an OK, a fast-auth success, a resultset or a binlog event. |
| **Invariant** | Workload `Always`: attacker sessions (wrong password/user, stale-salt scramble, no `plugin_auth`, `mysql_native_password`, truncated greeting, reset mid-switch, dump frame as greeting) receive only AuthMethodSwitch or ERR 1045. SUT-side `ALWAYS(context.authenticated)` at the top of the command loop and in `handle_binlog_dump_command` (`network_service.cpp:553, 279`), with a new flag set only at `:457`. `Sometimes(attacker hit auth-switch path)`. |
| **Antithesis Angle** | Interleaved sessions on the single io thread; drops, reordering and resets mid-handshake. |
| **Why It Matters** | The listener serves decrypted backups over cleartext TCP behind one shared password. It is the only access control on encrypted data. |

Priority: P2. Confidence: high that it holds (linear handshake, per-session context, `connection_context.cpp:111-117`).
Fault requirements: network faults (drops, resets); none strictly required.

**Open Questions:**

- Is the shared-password cleartext listener the intended production model, or will TLS be added (ticket 20 proposes it)?

### rs-preauth-session-lifetime-bounded — Unauthenticated connections close within a bound

| | |
|---|---|
| **Type** | Safety |
| **Property** | Every connection that has not authenticated is closed by the server within B = 2·(2·read_timeout) + 3·write_timeout (+ slack) of its last server activity. |
| **Invariant** | Workload `Always(close_time − last_server_activity ≤ B + slack)` for slow-loris and silent attacker connections. `Sometimes(attacker closed by server timeout)`. SUT-side `REACHABLE("pre-auth session ended by read timeout")` and `ALWAYS(pre-auth duration ≤ B + slack)` in `session`. |
| **Antithesis Angle** | Timers run on the io thread that also does blocking storage I/O (`fetch_event_block` S3 GET + decrypt). S3 latency, node throttling and held packets stretch the bound. |
| **Why It Matters** | There is no connection cap (`network_service.cpp:643-665`), and each pre-auth session holds an fd and up to a 16 MiB buffer. The timeout is the only thing stopping unauthenticated peers from exhausting fds and killing the listener (`listener-survives-fd-exhaustion`, N6). |

Priority: P2. Confidence: medium (bound derived from `network_io_operations.cpp:60-180`; io-thread stall effect is inferred).
Fault requirements: S3 latency/stall, node throttling, partitions that hold packets.

**Open Questions:**

- What slack is acceptable under storage stalls?
- Should a pre-auth connection cap exist?

### rs-client-request-cannot-kill-or-hang-pull — No client request kills or hangs pull

| | |
|---|---|
| **Type** | Safety |
| **Property** | No request from any replica client, authenticated or not, terminates the pull process or stalls its io thread; failures stay confined to the offending session. |
| **Invariant** | Workload `Always(!(pull_alive && legit_replica_stalled > T && source_has_new_events))` and no pull exit attributable to a hostile session. `Sometimes(legit replica progresses after hostile burst)`. SUT-side `ALWAYS(event_size >= 19)` replacing the assert at `indexed_event_block.cpp:43`; `UNREACHABLE("dump position not on event boundary served")`. |
| **Antithesis Angle** | Hostile authenticated dumps at random positions (never boundary-checked; `sender_context.cpp:61-64` → `start_from_offset`), running concurrently with collector rotation and checkpoints, which shift what "mid-event" means. Debug build: abort kills pull. Release: an `event_size == 0` loop hangs the io thread and SIGTERM. |
| **Why It Matters** | One replica holding the shared password can take down collection and every other replica (ticket 7 reached through A13). |

Priority: P2. Confidence: high for the ticket-7 reachability via crafted position (code path read); low for other escapes.
Fault requirements: none beyond the workload; benefits from concurrent load.

**Open Questions:**

- Should non-boundary dump positions be rejected with ERR 1236?
- Which build preset (debug abort vs release hang) will run?

## Areas examined

- `src/minimysql/network_service.cpp` (full): handshake ordering, command dispatch, the test-scaffolding queries (`select * from tbl`, `SELECT VERSION()` → canned rows; harmless, no data exposure), listener exit paths.
- `src/minimysql/connection_context.cpp` (full): scramble generation (CSPRNG, per connection, refreshed on switch), `check_client_authentication`, sequence-number validation, command parsing (COM_BINLOG_DUMP_GTID parsed but answered "unknown command", ticket 20). The non-constant-time compare was judged not exploitable (random per-connection salt); no property.
- `src/minimysql/caching_sha2_password_authenticator.cpp`, `network_io_operations.cpp` (frame size cap, per-frame timeouts, assert-only checks).
- `src/binsrv/authentication_config.cpp`, `replication_source_config.cpp`: non-empty user/password, plugin pinned, non-zero timeouts. No empty-password bypass.
- `src/easymysql/connection.cpp`: SSL/TLS option plumbing; no `GET_SERVER_PUBLIC_KEY` (ticket 23).
- `src/binsrv/storage_core.cpp`: envelope generation (`:1008`), file-key unwrap (`:1091`), `write_data_to_stream` (`:1125`), encryption init (`:632-668`), metadata/encryption validation (`:172-178`, `:808-826`, `:884-904` keyring presence fail-closed), decryption on fetch (`:544-557`).
- `src/binsrv/file_keyring.cpp`: keyring loaded once at construction from a local file; never written by PBS; no rotation handling. Removing an old KEK fails closed at open (`:899`). No timing angle, so no property.
- `src/binsrv/s3_storage_backend.cpp`: local buffer directory created with default permissions and leaked on SIGKILL (relevant at-rest location; ciphertext iff the record is encrypted).
- `operations/logger_helpers.cpp`, `binsrv/storage_config.cpp`: S3 URI credentials masked in logs; source and listener passwords are not logged; event bodies are not printed (`unknown_body` prints nothing), so debug logs do not leak row data.
- Ticket snapshot: tickets 8 (CTR carry, deterministic, Hegel), 20 (listener has no TLS), 23 (validated, used above).

## Cross-property assumptions

- The workload holds the keyring/KEK and can independently decrypt storage objects (needed by the decryptability and keystream-reuse oracles), or else relies on pull as the decryptor, which is weaker.
- The harness source runs with TLS enabled and `performance_schema` on. The PBS replication user is `caching_sha2_password`.
- The SUT is a debug/ASan build, so assert-only paths abort instead of hanging (affects `rs-client-request-cannot-kill-or-hang-pull`).
- "At rest" includes PBS's local S3 buffer directory and log files, not only the configured storage.
- Concurrent purge/second-writer scenarios are a separate "misuse" workload phase until owners decide README:473's scope.
- The Antithesis C++ SDK is added behind a CMake option; every SUT-side assertion here is missing today (existing-assertions.md).
