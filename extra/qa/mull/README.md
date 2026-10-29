# Mutation testing with Mull

[Mull](https://github.com/mull-project/mull) compiles every mutant of the
code (`>` turned into `>=`, `+` into `-`, ...) into one instrumented test
binary, then runs the tests once per mutant. A mutant that no test notices
(a *survivor*) shows code whose behaviour the tests do not pin down: a weak
property, a missing boundary case, or an equivalent mutant (a change with no
observable effect).

`mull.yml` names the code that is mutated: the GTID, util and event
libraries (`reader_context` and the rewriter included), the configuration and
keyring validators and the size / time units. `run_mull.sh` lists the test
binaries that run against the mutants: the unit tests and the property tests
of that code, `sender_context_property_test` included (about 50 s per run,
the slowest), but not the storage, listener or replication-source tests
(minutes per run, and the S3 cases need the S3 server).

The nightly runs it weekly (stage `mull`, on Sundays by default,
`MULL_DAY`): each mutant no test kills, unless it is listed in
`mull_known_survivors.txt`, is a new failure of the build `mull`.

## Setup (test-infra)

Mull 0.35.0 for LLVM 20.1.2 (the Ubuntu 24.04 clang-20), unpacked without
installing:

```bash
mkdir -p ~/ws/mull/pkg && cd ~/ws/mull
curl -sfLO "https://dl.cloudsmith.io/public/mull-project/mull-stable/deb/ubuntu/pool/noble/main/M/Mu/mull-20_0.35.0/Mull-20-0.35.0-LLVM-20.1.2-ubuntu-amd64-24.04.deb"
dpkg-deb -x Mull-20-0.35.0-LLVM-20.1.2-ubuntu-amd64-24.04.deb pkg
```

A Clang build with the Mull plugin and Clang coverage (Mull skips mutants on
lines no test executes), linked against the GCC-built dependencies of
`debug_gcc14` as the fuzz build is:

```bash
cd ~/ws
export MULL_CONFIG=$PWD/percona-binlog-server/extra/qa/mull/mull.yml
F="-fpass-plugin=$HOME/ws/mull/pkg/usr/lib/mull-ir-frontend-20 -g -grecord-command-line -fprofile-instr-generate -fcoverage-mapping -O0"
cmake -S percona-binlog-server -B percona-binlog-server-build-mull_clang20 \
  -DCMAKE_BUILD_TYPE=Debug -DCMAKE_C_COMPILER=clang-20 \
  -DCMAKE_CXX_COMPILER=clang++-20 -DWITH_STDLIB_LIBCXX=OFF -DCPP_STANDARD=23 \
  -DBUILD_TESTING=ON -DWITH_PROPERTY_TESTS=ON -DS2N_USE_CRYPTO_SHARED_LIBS=ON \
  "-DCMAKE_PREFIX_PATH=$HOME/ws/aws-sdk-cpp-install-debug_gcc14;$HOME/ws/boost-install-debug_gcc14" \
  "-DCMAKE_C_FLAGS=$F" "-DCMAKE_CXX_FLAGS=$F" \
  -DCMAKE_EXE_LINKER_FLAGS=-fprofile-instr-generate
cmake --build percona-binlog-server-build-mull_clang20 -j12 \
  $(printf -- '--target %s ' $(percona-binlog-server/extra/qa/mull/run_mull.sh --targets))
```

The nightly's `mull` stage configures this build itself the first time it
runs; only the unpacked Mull is needed.

## Running

```bash
cd ~/ws/mull
MULL_CONFIG=~/ws/percona-binlog-server/extra/qa/mull/mull.yml \
  ~/ws/percona-binlog-server/extra/qa/mull/run_mull.sh \
  ~/ws/mull/pkg/usr/bin/mull-runner-20 \
  ~/ws/percona-binlog-server-build-mull_clang20 ~/ws/mull/reports \
  ~/ws/nightly/known_failures.txt
```

`run_mull.sh --targets` prints the test binaries to build. After changing
`includePaths` in `mull.yml`, rebuild them with `--clean-first`: the plugin
reads the file at compile time.

The script sets `CI=1`, which makes Hegel derandomize its inputs and skip its
failure database, so every mutant meets the same test cases. A test binary
must pass without mutants, so the test cases listed in `known_failures.txt`
are left out (all of `indexed_event_block_property_test`'s, for now). A slow
binary gets its own `timeout=<ms>` in the list: Mull's default timeout also
applies to the unmutated warm-up run.

mull-runner judges each test binary on its own; `combine_mull_reports.py`
counts a mutant as surviving only if no test kills it, sets aside the
triaged ones in `mull_known_survivors.txt` and reports listed ones that no
longer survive as stale. The per-binary reports are `<binary>.txt` (IDE
format: file, line, column and the change) and `<binary>.sqlite`.

## Results

### 2026-10-28: the GTID and util libraries

Six test binaries, about 15 s in all (12 workers): 118 mutants on covered
lines, 96 killed (81.4%), 22 surviving every test. 15 of the survivors are
harmless (11 equivalent, 4 unreachable boundaries) and 7 were test gaps,
all closed the next day by tests in `gtid_set_test` and
`byte_span_encoding_test` (103 of 118 killed, 87.3%, afterwards):

| Mutant | Gap | Closed by |
| --- | --- | --- |
| `binsrv/gtids/gtid_set.cpp:133` `>=` → `>` | an upper-case `A` in the hex of a UUID was never parsed (it would be read as a digit) | `GtidSetParsesUpperCaseHex` |
| `binsrv/gtids/gtid_set.cpp:768` `<=` → `<` | a binary GTID set with an empty interval (upper == lower) was never decoded | `GtidSetBinaryRejectsEmptyInterval` |
| `util/byte_span_extractors.hpp:116`, `:138` `>` → `>=` | the checked byte span / byte array extractors never got exactly as many bytes as they need | `CheckedByteExtractionAtExactSize` |
| `util/byte_span_inserters.hpp:165`, `:173`, `:181` `<` → `<=` | the checked packed-integer inserter never got a buffer of exactly the encoded size | `PackedIntEncoding` (exact and one-byte-short buffers) |

### 2026-10-29: the event library, validators and units added

15 test binaries (`run_mull.sh`), about 25 min, most of it
`sender_context_property_test`: 414 mutants on covered lines, 356 killed
(86.0%), 58 surviving every test. Per binary, from 35% (`gtid_test`) to
100% (`tag_test`); `reader_context_property_test` 80%,
`event_property_test` 74%, `config_value_property_test` 90%,
`sender_context_property_test` 71%. `indexed_event_block_property_test`
does not run: its only test case is a known failure (tickets 7 / 39).

All 58 survivors are triaged in `mull_known_survivors.txt`: 23 harmless
(equivalent, or a boundary no input can reach) and 35 open test gaps,
listed so that the weekly run reports only new survivors:

- **Text output no test compares (10):** the names of checksum
  algorithms and common header flags, `flag_set` printing, the
  post-header length table.
- **Boundaries no test reaches (15):** a binlog sequence number of exactly
  1,000,000, an FDE checksum byte equal to the first invalid value, a
  repeated field id in a tagged GTID body, the largest representable size
  and time, a server version exactly at the protocol table switch,
  `last_committed` equal to `sequence_number` and a transaction length
  equal to the event size in the rewriter, encoding into exact-size buffers,
  a post-header length lookup past a version's table.
- **Behaviour no test in the run checks (5):** `next_event_position` of an
  artificial ROTATE built through `common_header`, resuming at position 4
  of the same binlog, an artificial ROTATE in the middle of a stream,
  `event_size` after a rewrite that changes the size, `is_epoch()`.
- **Storage reads (5):** `byte_range` text and `file_operations_helpers`
  boundaries, covered by `storage_property_test`, which is not in the run
  (minutes per run, and its S3 cases need the S3 server).
