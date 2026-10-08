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
  `caching_sha2_password` authenticator, and `event_generation_property_test`
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
  first (give each build its own so a failure found under one sanitizer is
  replayed under the same one);
- `suppress_health_check` — Hegel health checks to silence (the nightly
  suppresses `too_slow` under the sanitizers, which are expected to be slow).

The nightly profiles are the reference example
([`../extra/qa/nightly/hegel.toml`](../extra/qa/nightly/hegel.toml)): a big
`nightly_debug` run (2000 cases), and smaller `nightly_asan` / `nightly_tsan`
runs (500 / 300 cases), each with its own failure database. Run a property
under a profile like this:

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
`PBS_TEST_S3_TEST_CASES`) because each round-trip hits the network, so they do
not scale with the profile's `test_cases`.

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
| `event_property_test` | `binsrv::events::event` / `event_view` | Every supported event type round-trips through encode/parse; arbitrary or corrupted network bytes are parsed or cleanly rejected, never crash; flipping any single bit of a CRC32-footered event is detected. |
| `reader_context_property_test` | `binsrv::events::reader_context` | The per-binlog stream grammar is accepted for valid streams (GTID or position mode, with/without checksums, 8.0/8.4), transaction boundaries and `next_event_position` are reported exactly, structurally broken streams are rejected, and resume-from-position is accepted. |
| `rewriter_property_test` | `binsrv::events::rewriter` | In `replication.rewrite` mode, copied events materialize unchanged with correct size/position/footer; renumbered streams keep valid logical clocks and preserved dependencies across local files; positions, lengths and GTIDs stay consistent. |
| `event_generation_property_test` | `operations/event_generation_helpers.cpp` | The artificial ROTATE, FORMAT_DESCRIPTION and PREVIOUS_GTIDS generators parse back with the right type, server id, flags, checksum and body, and a generated FORMAT_DESCRIPTION makes a fresh reader expect checksummed events. |
| `storage_property_test` | `binsrv::storage` (streaming, filesystem + optional S3) | A stateful sequence of writes/flushes/rotations/restarts/"crashes"/purges matches a model of on-disk binlogs; read-back through `sender_context` is exact (including a concurrent reader thread); encrypted storage differs on disk but decrypts correctly; damaged files never crash and fail only as designed. |
| `indexed_event_block_property_test` | `binsrv::indexed_event_block` | Splitting raw binlog bytes into complete events either rejects the bytes cleanly or yields an index consistent with them, for truncated or corrupted input. |
| `cipher_context_property_test` | `opensslpp::cipher_context` (AES-CTR) | Encrypting from any offset matches an independent CTR reference; encrypting in arbitrary pieces matches encrypting in one call; decrypting from arbitrary offsets recovers the data. |
| `minimysql_property_test` | `minimysql::connection_context` | The `pull`-mode source listener completes `caching_sha2_password` only with the right credentials, parses client commands faithfully, rejects corrupted packets (run under ASan), and delivers binlog events that a MySQL client reassembles exactly, including multi-packet payloads. |
| `config_value_property_test` | user/server value parsers | Size/time units, binlog names, timestamps, semantic versions, filesystem and S3 storage URIs, keyring files and config files accept exactly the valid inputs (against an independent model) and round-trip; damaged keyring/config files are rejected with a regular exception. |
| `search_helpers_property_test` | `operations/search_helpers.cpp` (the `search_by_gtid_set` / `search_by_timestamp` record selection) | GTID search succeeds exactly when the target is covered by the union of the records' GTID sets, selecting a minimal increasing subsequence that each intersects the target; timestamp search returns the leading run up to the first record past the timestamp; empty-storage, non-GTID-mode and too-old inputs are rejected. |

Each test file opens with a comment block describing its properties in detail;
start there when changing or extending one.

## How these run in CI and nightly

- **CI / local**: built with `-DWITH_PROPERTY_TESTS=ON` and run via `ctest`
  with Hegel's default settings — a fast smoke level.
- **Nightly** (on the QA host): `extra/qa/nightly/run_nightly.sh` builds the
  `debug`, `asan` and `tsan` configurations, runs every property test case
  under the matching `nightly_*` profile (hundreds to thousands of cases each),
  compares failures against `known_failures.txt`, and feeds the results into
  the nightly report. The nightly profiles also set
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
4. Sanity-check that the property actually catches regressions by planting a
   deliberate bug and confirming the property fails and shrinks to a small
   reproducer, then remove the bug.
