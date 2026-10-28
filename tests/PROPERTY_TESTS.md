# Property-based tests

This directory holds the project's unit tests. Alongside the ordinary
example-based [Boost.Test](https://www.boost.org/doc/libs/release/libs/test/)
cases, a subset of them are **property-based**: instead of a handful of
hand-picked inputs, each test describes an *invariant* and lets the
[Hegel](https://hegel.dev) engine search for an input that breaks it, shrinking
any counter-example down to a minimal reproducer.

This document explains how those tests are built, how to run and replay them,
how the harness fits together, and what each property test covers. For the
out-of-tree QA harnesses (the Python crash/fault tests, the MTR suite, the
nightly runner), see [`../extra/qa/README.md`](../extra/qa/README.md).

## What a property test is

A traditional unit test asserts on fixed inputs:

```cpp
BOOST_AUTO_TEST_CASE(EmptyGtidSetIsEmpty) {
  BOOST_TEST(gtid_set{}.str().empty());
}
```

A property test instead states something that must hold for *every* input, and
Hegel generates the inputs:

```cpp
BOOST_AUTO_TEST_CASE(GtidSetOperationsMatchModel) {
  run_property([](hegel::TestCase &tc) {
    const auto ops{tc.draw("ops", gs::vectors(operations()))};
    const auto built{build_from_operations(ops)};
    require_matches_model(built.gtids, built.model);
  });
}
```

Here Hegel draws a random sequence of operations, applies them both to the real
`gtid_set` and to a trivially correct model, and the property fails (throws) if
they ever disagree. When a failure is found, Hegel *shrinks* the operation
sequence to the smallest one that still fails and records it in an example
database so the same case is replayed first next time.

The two building blocks live in
[`property_test_helpers.hpp`](property_test_helpers.hpp):

- **`run_property(body [, settings])`** runs a Hegel property inside the current
  Boost.Test case. The Boost test-case name is used both in the failure report
  and as the Hegel example-database key, so a failure found once is replayed
  first on the next run. The optional `settings` argument overrides individual
  Hegel settings (fields left at their defaults still come from the active
  profile).
- **`require(condition, message)`** is the property assertion: it throws
  `std::runtime_error{message}` when the condition is false. A property is
  violated by throwing, which is what lets Hegel shrink the input — so inside a
  property use `require`, not `BOOST_TEST`.

Inputs come from Hegel generators (`namespace gs = hegel::generators;`) drawn
through `tc.draw("label", generator)`. The label names the value in the failure
report. Common generators used across these tests are `gs::integers<T>({.min_value=…, .max_value=…})`,
`gs::vectors(elem)`, `gs::strings(pattern)` and `gs::element_of(...)`; several
tests also build a small custom generator for a domain type (an event, a
transaction, a config string) out of these.

## Building

Property tests are gated behind a CMake option and are **off by default**
because they pull in the Hegel dependency (fetched from GitHub at configure
time, pinned to `v0.13.1`). Enable them with:

```sh
cmake -S . -B build --preset debug_gcc14 -DWITH_PROPERTY_TESTS=ON
cmake --build build
```

Notes:

- `WITH_PROPERTY_TESTS=ON` adds the `*_property_test` targets and their
  `add_test` entries; without it only the example-based tests are built.
- When the build enables AddressSanitizer (`WITH_ASAN`), Hegel is built with
  ASan too (`HEGEL_SANITIZE=address`) so generated inputs are checked for
  memory errors as well as logical ones. For the concurrent `storage` property
  the nightly builds a ThreadSanitizer variant (`-DHEGEL_SANITIZE=thread` plus
  `-fsanitize=thread`); see the setup and nightly scripts under `extra/qa`.
- Hegel's reflection-based generator derivation is disabled
  (`HEGEL_REFLECTION OFF`) — these tests write their generators explicitly — so
  no extra dependency is fetched for it.
- Each property executable links only the component it tests plus `hegel`; a
  few compile a source file directly into the test (for example
  `storage_property_test` pulls in `operations/sender_context.cpp`,
  `minimysql_property_test` pulls in `connection_context.cpp` and the
  `caching_sha2_password` authenticator, `replication_source_property_test`
  pulls in the whole listener (`minimysql/*.cpp`) and `sender_context`, and `event_generation_property_test`
  pulls in `operations/event_generation_helpers.cpp`). The wiring is in
  [`CMakeLists.txt`](CMakeLists.txt).

## Running

All tests, property and example-based, run through CTest:

```sh
ctest --test-dir build --output-on-failure
```

To run one property executable directly (each is a Boost.Test program):

```sh
./build/tests/gtid_set_property_test                        # all cases
./build/tests/gtid_set_property_test --list_content          # list the cases
./build/tests/gtid_set_property_test --run_test=GtidSetTextRoundTrip
```

### Settings profiles (`hegel.toml`)

How hard Hegel searches is controlled by a settings profile. Hegel reads a
`hegel.toml` from the working directory or any ancestor, and the
`HEGEL_DEFAULT_PROFILE` environment variable selects which `[profiles.<name>]`
table applies. A profile sets, among other things:

- `test_cases` — how many inputs to try per property;
- `database` — the directory holding the shrunk failing examples to replay
  first. The nightly gives each build its own, and before each build's run
  copies in the inputs the other builds have saved, so a failure one build
  found is replayed in all of them. Do not point several builds at one
  database: Hegel drops a saved input that no longer fails, so a build that
  cannot hit a failure (an `assert` compiled out of release) would delete it
  for the build that found it;
- `suppress_health_check` — Hegel health checks to silence (the nightly
  suppresses `too_slow` under the sanitizers, which are expected to be slow).

The nightly profiles are the reference example
([`../extra/qa/nightly/hegel.toml`](../extra/qa/nightly/hegel.toml)): a big
`nightly_debug` run (2000 cases), and smaller `nightly_asan` / `nightly_tsan`
/ `nightly_release` runs (500 / 300 / 500 cases), each with its own failure
database. Run a property under a profile like this:

```sh
cd some/dir/with/hegel.toml
HEGEL_DEFAULT_PROFILE=nightly_debug ./path/to/storage_property_test \
    --run_test=StorageOperationsMatchModel
```

With no `hegel.toml` and no profile selected (the plain `ctest` case above),
Hegel uses its built-in defaults — enough for a quick developer check.

### Reproducing a failure

A property failure prints the drawn values (by their `tc.draw` labels) and the
Boost test case. To reproduce it, run that single case; because the shrunk
example is stored in the profile's `database`, running the same case under the
same profile replays the minimal counter-example first:

```sh
cd <work dir used for the run>      # so the same hegel.toml / database apply
HEGEL_DEFAULT_PROFILE=<profile> <test binary> --run_test=<test case>
```

(This is exactly the hint `run_nightly.sh` prints for any failure it records.)

### Tests that need external services

Two property tests reach an optional S3-compatible server when four environment
variables are set, and skip those variants otherwise:

- `PBS_TEST_S3_ENDPOINT` (`host:port`)
- `PBS_TEST_S3_ACCESS_KEY`
- `PBS_TEST_S3_SECRET_KEY`
- `PBS_TEST_S3_BUCKET`

`storage_property_test` then reruns every property with a `…OnS3` variant
against the S3 backend, and `config_value_property_test` additionally
authenticates generated credentials against the server. Starting a local
S3-compatible server is described in `extra/qa/README.md`. The storage `…OnS3`
variants cap themselves at a small number of cases (overridable with
`PBS_TEST_S3_TEST_CASES`, 20 by default) because each round-trip hits the
network, so they do not scale with the profile's `test_cases`; the nightly sets
`PBS_TEST_S3_TEST_CASES=100`.

The nightly run sets these four variables automatically: `run_nightly.sh`
brings up the local S3 server and exports them (and the endpoint/bucket shown
above) before the property phase, so the S3 variants run every night. If the
server cannot be reached it leaves them unset and the variants skip rather than
fail.

## The design, in one place

- **Model-based** properties (`gtid_set`, `storage`) run the generated
  operations against both the real component and a deliberately simple,
  obviously correct model, and assert the two always agree. The model is the
  specification; the real code is the thing under test.
- **Round-trip** properties (events, config values, generators) assert that
  encode-then-parse, or parse-then-print, returns the original value over the
  whole input domain — not just a few examples.
- **Robustness** properties feed arbitrary or deliberately corrupted bytes in
  and require the parser to either accept them or reject them with one of the
  exceptions it is documented to use (`std::logic_error` and its descendants),
  never to crash, over-read or fail in any other way. Running these under
  AddressSanitizer turns an out-of-bounds read into a test failure.
- **Small input pools** are a recurring trick: drawing UUIDs, tags, GNOs, event
  types, etc. from small fixed pools makes adjacent, overlapping and repeated
  values common, which is where boundary bugs hide.

## The property tests

| Test | Component under test | What it asserts |
| --- | --- | --- |
| `gtid_set_property_test` | `binsrv::gtids::gtid_set` | Set operations, union/difference/intersection and containment match a `std::set` model; text, lexical-cast and binary forms round-trip; out-of-range input is rejected; results are insertion-order independent. |
| `event_property_test` | `binsrv::events::event` / `event_view` | Every supported event type round-trips through encode/parse; arbitrary or corrupted network bytes are parsed or cleanly rejected, never crash; flipping any single bit of a CRC32-footered event is detected; a HEARTBEAT_LOG_EVENT_V2 body (PBS-30), generated from its fields (file name, position with a right or wrong declared length, unknown fields, optional end marker, packed integers in their shortest or a wider form) and sometimes corrupted, decodes as MySQL's `Heartbeat::decode` does: rejected with `std::invalid_argument` exactly when a model of it rejects, otherwise with the same file name and position (`HeartbeatLogV2BodyDecodesLikeMysql`). |
| `reader_context_property_test` | `binsrv::events::reader_context` | The per-binlog stream grammar is accepted for valid streams (GTID or position mode, with/without checksums, 8.0/8.4), transaction boundaries and `next_event_position` are reported exactly, structurally broken streams are rejected, and resume-from-position is accepted; a valid stream with one targeted defect (artificial ROTATE with a timestamp / next position / wrong position, artificial FDE / PREVIOUS_GTIDS / ordinary event / STOP, wrong binlog version or common header length, FDE newer than the connection's server, an event before the FDE, a second FDE / PREVIOUS_GTIDS, closing ROTATE position other than 4, anonymous GTID in GTID mode, GTID event without a GTID, zero or wrong next position) is rejected exactly at that event with a `std::logic_error` naming the defect. |
| `rewriter_property_test` | `binsrv::events::rewriter` | In `replication.rewrite` mode, copied events materialize unchanged with correct size/position/footer; renumbered streams keep valid logical clocks and preserved dependencies across local files; positions, lengths and GTIDs stay consistent. |
| `event_generation_property_test` | `operations/event_generation_helpers.cpp` | The artificial ROTATE, FORMAT_DESCRIPTION and PREVIOUS_GTIDS generators parse back with the right type, server id, flags, checksum and body, and a generated FORMAT_DESCRIPTION makes a fresh reader expect checksummed events. |
| `storage_property_test` | `binsrv::storage` (streaming, filesystem + optional S3) | A stateful sequence of writes/flushes/rotations/restarts/"crashes" matches a model of on-disk binlogs (purges run only as the supported stop → `purge_binlogs` → restart; purging beside a running writer is unsupported, PBS-75); read-back through `sender_context` is exact; while the writer writes, checkpoints and rotates, a reader thread from the beginning and a second one that connects after a generated operation and resumes at the end of what is on disk (a reconnecting replica) each receive exactly what is on disk, every binlog announced by one artificial ROTATE (the first with the session's checksum setting, later ones with the previous binlog's FORMAT_DESCRIPTION setting, as MySQL's `Binlog_sender` does) plus an artificial FORMAT_DESCRIPTION when resuming, and every heartbeat they take at idle points reports where they had got to (`StorageConcurrentReadBackMatchesDisk`, also under TSan); encrypted storage differs on disk but decrypts correctly; damaged files never crash and fail only as designed, and when the damage is confined to one binlog's files (or adds an object) opening for queries still works and returns every other binlog (losing the index entry of a magic-only last binlog, or the first of several entries, which an interrupted purge leaves, may be accepted); opening an encrypted storage for queries with a keyring that lacks its KEK, or without a keyring, fails naming the keyring / KEK (`StorageQueryingReportsKeyringErrors`, fails today: ticket 35); opening the storage for queries never changes any object, also with a configuration that adds encryption; opening it for queries while it is being written always succeeds and lists the binlogs without a gap; closing a binlog with an incomplete transaction buffered drops that transaction completely; a writer killed, or hitting a storage error, at any backend call (the backend is wrapped through a test-only seam in `storage_backend_factory`, compiled in with `WITH_PROPERTY_TESTS`) leaves a storage that opens again, holds every binlog but the last completely and the last up to a transaction boundary, never holds data twice, and resumes right after its data. A fault injected only during the supported stop → `purge_binlogs` → restart leaves a storage that opens and holds either the pre-purge or the post-purge binlogs (ticket 33, fixed upstream by PBS-59). The start after such a fault never drops a binlog that `binlog.index` lists nor data its metadata records — the recorded state is read from the objects themselves, not through `storage_core` — although it may remove leftovers of the interrupted operation and truncate data the metadata does not cover; and a second fault during that start (while it removes leftovers, truncates or resumes; the first fault may also hit the creation of a new storage and its first binlog) still leaves a storage that the start after it opens with everything recorded before the first fault (`StorageRestartKeepsRecordedData`, `StorageRestartSurvivesAFault`; both catch a mutant that loses the last index entry, ticket 36's mechanism. The second fails today: a failed delete during the recovery removes the data file but keeps its metadata, ticket 37). A failure that a storage destructor swallows (the final flush of a restart or a purge) is reported as an injected failure, so that it is judged by what the next start finds. With an encrypted storage, a writer killed right after appending data its metadata does not cover yet, recovered (the tail is truncated) and then writing different events never encrypts an offset twice with the same AES-CTR keystream (fails today: the recovered binlog keeps its key and IV, ticket 32). |
| `indexed_event_block_property_test` | `binsrv::indexed_event_block` | Splitting raw binlog bytes into complete events either rejects the bytes cleanly or yields an index consistent with them, for truncated or corrupted input. |
| `cipher_context_property_test` | `opensslpp::cipher_context` (AES-CTR) | Encrypting from any offset matches an independent CTR reference; encrypting in arbitrary pieces matches encrypting in one call; decrypting from arbitrary offsets recovers the data. |
| `minimysql_property_test` | `minimysql::connection_context` | The `pull`-mode source listener completes `caching_sha2_password` only with the right credentials, parses client commands faithfully, rejects corrupted packets (run under ASan), and delivers binlog events that a MySQL client reassembles exactly, including multi-packet payloads. |
| `replication_source_property_test` | `minimysql::network_service` (the `pull`-mode replication source handshake, PBS-90), started in-process on 127.0.0.1 and driven over TCP by a minimal MySQL client | `SELECT @source_binlog_checksum` / `@master_binlog_checksum` return the value of the session's last checksum `SET` (NULL before any); `COM_BINLOG_DUMP` is refused with ERR 1236 exactly when no checksum `SET` came first and otherwise streams the stored binlog up to EOF; statements are matched exactly or by the documented patterns only (a known statement gets OK or a resultset, a near-miss gets an error: fails today, ticket 31; since PBS-30's regular expressions only a bare `SET @slave_uuid` / `SET @replica_uuid` / `SET NAMES`, or one followed by a non-word character, is still accepted); `SELECT @@GLOBAL.SERVER_ID` / `SERVER_UUID` return the configured values and `GTID_MODE` follows the storage's mode. A `COM_BINLOG_DUMP` from any binlog name and position (inside an event, past the end) ends that session only: a new connection still logs in and gets OK to `COM_PING` (`ReplicationSourceSurvivesAnyDumpPosition`; fails today: the listener process terminates, ticket 39, PBS-112). Any sequence of logins with the right or a wrong user name and password follows a model of the `caching_sha2_password` cache (PBS-10 part 3): the user is cached after its first successful full authentication; a cached user gets only the fast path, which succeeds exactly with the right password and is otherwise refused at once; everyone else gets the full authentication, which succeeds exactly with the right user and password (`ReplicationSourceAuthenticationCacheFollowsModel`; catches a cache that is never filled and one that ignores the user name). After a heartbeat period statement in any spelling (letter case, spacing, leading zeros, one or both variables; 0, 60-150 ms, a value too large for 64 bits, or no value, which is refused), a blocking dump gets about one HEARTBEAT_LOG_EVENT_V2 per period, each reporting the client's coordinates, and a non-blocking dump or a zero period gets none (`ReplicationSourceHeartbeatsFollowTheRequestedPeriod`, PBS-30; wall-clock based, so it checks the count and the average spacing within loose bounds and runs a fixed 25 cases whatever the profile). On a GTID-mode storage of real GTID_LOG / XID transactions after purged GTIDs, `COM_BINLOG_DUMP_GTID` answers a replica that lacks a purged GTID with ERR 1236 and streams any other exactly the stored transactions it lacks, in order, whatever else its set holds (`ReplicationSourceDumpGtidSendsTheMissingTransactions`, written ahead of PBS-29 part 2: fails today with ERR 1047 "unknown command", ticket 20; its storage was checked with a plain `COM_BINLOG_DUMP`). |
| `config_value_property_test` | user/server value parsers | Size/time units, binlog names, timestamps, semantic versions, filesystem and S3 storage URIs, keyring files and config files accept exactly the valid inputs (against an independent model) and round-trip; damaged keyring/config files are rejected with a regular exception; a keyring URI is accepted exactly when it is a `file://` URI without host, port, userinfo, query or fragment naming a regular file holding a valid keyring. |
| `search_helpers_property_test` | `operations/search_helpers.cpp` (the `search_by_gtid_set` / `search_by_timestamp` record selection) | GTID search succeeds exactly when the target is covered by the union of the records' GTID sets, selecting a minimal increasing subsequence that each intersects the target; timestamp search returns the leading run up to the first record past the timestamp; empty-storage, non-GTID-mode and too-old inputs are rejected. |
| `purge_selection_property_test` | `binsrv::select_purge_victim_count` (the `purge_binlogs` victim selection) | Purging up to a target drops exactly the prefix ending with it, always leaving at least one record; an empty storage, a different base name, an absent target and the current tail are each rejected. |
| `sender_context_property_test` | `operations::sender_context` (the COM_BINLOG_DUMP streamer) | From the beginning, the stream is an artificial ROTATE (artificial flag, zero timestamp/next_event_position, magic-offset position, checksum per the session setting) then the stored events byte for byte (FDE and PREVIOUS_GTIDS_LOG first: every seeded binlog starts with both, as the sender requires since PBS-29; the PREVIOUS_GTIDS_LOG holds a generated GTID set, sometimes of hundreds of UUIDs, so that it outgrows the sender's first preamble read, also in the next binlog of the rotation property); from a position, an artificial ROTATE carrying that position, an artificial FDE with zero next_event_position, then the stored events from that position. Blocks are at least `sender_single_file_context::min_allowed_block_size` (PBS-29), with some events larger than a block. The event loop reads every stored byte from the starting position on exactly once (PBS-65: a partial trailing event is carried over in memory, not re-read), counted at the storage backend through the test seam; only the preamble (one read, plus one for the rest of a large PREVIOUS_GTIDS_LOG) and a resume inside or right after it may read bytes twice (a mutant that carries nothing over is caught). A binlog whose last event is incomplete fails the stream after its complete events (since PBS-29 the collector is taken never to store a partial event; before, the reader waited for the rest), as does a binlog ending in a partial event followed by the next binlog. An empty storage is polled (EOF, not an error) until the binlog arrives, then streamed from the beginning or from the requested position (PBS-91; fails since PBS-29 when the dump names the binlog, ticket 38, and from a position, PBS-96). The same wait holds for a named binlog that holds only the magic yet; moving into the next binlog always emits an artificial ROTATE naming it, also when that binlog held only the magic when the reader reached it; a dump position inside a stored event never yields events cut out of the stored data; after the oldest binlogs are purged and the storage is reopened, the surviving binlogs are streamed byte for byte and a purged one is refused. While a dump is idle, `generate_heartbeat()` reports the replica's coordinates, as a MySQL source does: the binlog of the last artificial ROTATE and the end of the last event sent, with a zero timestamp, the binlog's server_id, the position in the header and a valid CRC32, also when resuming from a position and after a switch to the next binlog (`IdleHeartbeatReportsTheReplicaCoordinates`, PBS-30; fails today while the next binlog holds only the magic: no heartbeat at all, ticket 40, PBS-113). |

Each test file opens with a comment block describing its properties in detail;
start there when changing or extending one.

## How these run in CI and nightly

- **CI / local**: built with `-DWITH_PROPERTY_TESTS=ON` and run via `ctest`
  with Hegel's default settings — a fast smoke level.
- **Nightly** (on the QA host): `extra/qa/nightly/run_nightly.sh` builds the
  `debug`, `asan`, `tsan` and `release` configurations, runs every property
  test case in its own process under the matching `nightly_*` profile
  (hundreds to thousands of cases each; TSan runs `storage_property_test`
  only), shares the saved failing inputs between the builds, compares
  failures against `known_failures.txt`, and feeds the results into the
  nightly report. The nightly profiles also set
  `report_multiple_failures = true`, so a case surfaces every distinct failure
  in one run rather than stopping at the first, and the run enables the S3
  backend variants (see above). See
  [`../extra/qa/README.md`](../extra/qa/README.md) for that layer.

## Adding a property test

1. Create `tests/<name>_property_test.cpp`. Include `property_test_helpers.hpp`
   and the component's headers; define `BOOST_TEST_MODULE` and include
   `<boost/test/unit_test.hpp>` for `main()`.
2. Write each invariant as a `BOOST_AUTO_TEST_CASE` whose body calls
   `run_property`, drawing inputs with `tc.draw(...)` and checking them with
   `require(...)`. Prefer a simple model or an independent reference over
   re-deriving the result the way the code does.
3. Add an `add_executable` / `target_link_libraries` / `set_target_properties`
   block in [`CMakeLists.txt`](CMakeLists.txt) (inside the
   `if(WITH_PROPERTY_TESTS)` section) linking `hegel` and only the component
   under test, plus a matching `add_test`. Compile a component source directly
   into the test when it is part of the executable rather than a library.
4. Put checks that several programs need in a shared header
   (`property_test_helpers.hpp`, `heartbeat_test_helpers.hpp`). A property
   that depends on wall-clock time should pass its own `hegel::Settings` with
   a fixed `test_cases`, so the nightly's large profiles do not multiply its
   run time, and should check loose bounds (TCP may deliver small packets late
   and then together).
5. Sanity-check that the property actually catches regressions by planting a
   deliberate bug and confirming the property fails and shrinks to a small
   reproducer, then remove the bug.
