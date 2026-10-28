# libFuzzer targets

Coverage-guided fuzz targets ([libFuzzer](https://llvm.org/docs/LibFuzzer.html))
for the parsers that read untrusted bytes. They complement the Hegel property
tests under [`../tests`](../tests), which generate structured inputs without
coverage feedback, and `extra/qa/listener_fuzz_test.py`, which throws random
malformed traffic at a running `pull`. libFuzzer keeps every input that reaches
new code and mutates it further, so over time it digs into parsing paths that
random generation rarely reaches. What it finds is the class of bug these
parsers have had before: a crash, a `std::terminate` or a sanitizer report on
malformed input (tickets 1, 3, 5, 7 and 39 / PBS-112).

| Target | Input | Code under test |
| --- | --- | --- |
| `event_stream_fuzzer` | a byte selecting the connection (replication mode, checksum verification, server version 8.0 / 8.4 / 9.7), then a binlog event stream as a source sends it, split into events by their `event_size` field | per event, what `collector_context` does: `event_view` against the `reader_context`, printed as at debug log level, `reader_context::process_event_view()`, the materialized `event`, printed |
| `listener_packet_fuzzer` | the packets a client sends to the `pull` replication source listener, split into MySQL frames by their length header | `minimysql::connection_context` in the order `network_service` drives it: client greeting, auth method switch, `caching_sha2_password` fast or full authentication (the "encrypted password" is decrypted with a real RSA key; a public key request is answered), then the command loop, with each query matched against the handshake regular expressions and a heartbeat period parsed |
| `stored_binlog_fuzzer` | two selector bytes (a dump from the beginning or from an event boundary, the session checksum, the block size, a switch to the next binlog), then the content of a stored binlog (a corrupted or foreign binlog in the storage) | the input stored as `binlog.000001` through a real filesystem storage (a fresh one per input, on a RAM disk under `$TMPDIR`, default `/dev/shm`), then streamed by `operations::sender_context` as for a COM_BINLOG_DUMP |
| `decoders_fuzzer` | a selector byte, then the input of one of eight decoders | the GTID set text and binary decoders, binlog names, server versions, timestamps, size and time units, and HEARTBEAT_LOG_EVENT_V2 bodies; text forms the code writes back (GTID set, binlog name, server version) must also print and parse back to the same value |

A rejection is a `std::exception`, which production code turns into stopping
the stream or closing the session; anything else (a crash, `std::terminate`,
an ASan or UBSan report, an out-of-memory) is a finding.

The listener target runs the command loop whatever the authentication decided:
a fuzzer cannot produce a valid scramble or encrypted password, and the point is
to reach every parser.

## Building

The targets need Clang (libFuzzer ships with it) and the whole build
instrumented, so they get their own build directory. With GCC's standard
library, the GCC-built Boost, MySQL and AWS SDK installs can be reused:

```sh
F="-fsanitize=fuzzer-no-link,address,undefined -fno-sanitize-recover=undefined -fsanitize-ignorelist=$PWD/percona-binlog-server/fuzz/ubsan_ignorelist.txt -fno-omit-frame-pointer -O1 -g"
cmake -S percona-binlog-server -B percona-binlog-server-build-fuzz_clang20 \
  -DCMAKE_BUILD_TYPE=Debug -DCMAKE_C_COMPILER=clang-20 -DCMAKE_CXX_COMPILER=clang++-20 \
  -DWITH_STDLIB_LIBCXX=OFF -DCPP_STANDARD=23 -DBUILD_TESTING=OFF -DWITH_FUZZERS=ON \
  -DS2N_USE_CRYPTO_SHARED_LIBS=ON \
  "-DCMAKE_PREFIX_PATH=$HOME/ws/aws-sdk-cpp-install-debug_gcc14;$HOME/ws/boost-install-debug_gcc14" \
  "-DCMAKE_C_FLAGS=$F" "-DCMAKE_CXX_FLAGS=$F" \
  "-DCMAKE_EXE_LINKER_FLAGS=-fsanitize=address,undefined"
cmake --build percona-binlog-server-build-fuzz_clang20 --target event_stream_fuzzer \
  listener_packet_fuzzer stored_binlog_fuzzer decoders_fuzzer
```

`clang-20` and `libclang-rt-20-dev` come from Ubuntu 24.04
(`extra/qa/setup_test_environment.sh` installs them).

## Seeds

`make_seeds.py` writes a starting corpus for each target: real binlogs from a
throwaway MySQL server (both replication modes, a varied workload with
rotations), each turned into the stream a source sends, and client sessions
in the MySQL wire format (greeting, public key request, encrypted password,
the statements and commands a replica sends):

```sh
PYTHONPATH=~/ws/qa/pylib python3 fuzz/make_seeds.py \
  --mysql-basedir ~/ws/dist/mysql-8.4.8-linux-glibc2.28-x86_64-minimal \
  --workdir /tmp/fuzz-seeds-mysql --out ~/ws/fuzz/seeds
```

The seeds take the event stream target from 12 to 962 covered edges before any
fuzzing, and the listener target from 501 to 1,948.

## Running

Keep a corpus directory per target between runs (start it from the seeds);
`-fork` runs several workers, and `-ignore_crashes` keeps going after a
finding so one known bug does not end the run:

```sh
cd ~/ws/fuzz
event_stream_fuzzer -fork=6 -ignore_crashes=1 -ignore_ooms=1 -ignore_timeouts=1 \
  -max_len=65536 -timeout=20 -rss_limit_mb=4096 -max_total_time=1800 \
  -artifact_prefix=artifacts/event_stream/ corpus/event_stream
```

Each finding is written to the artifact directory (`crash-*`, `oom-*`,
`timeout-*`). To reproduce one, run the target on that file alone:
`event_stream_fuzzer artifacts/event_stream/crash-<hash>`.

## In the nightly

`extra/qa/nightly/run_nightly.sh` has a `fuzz` stage, the last one: it
configures this build in `$WS/percona-binlog-server-build-fuzz_clang20` the first
time, builds the targets (`FUZZ_TARGETS`) and runs each for `FUZZ_SECONDS`
(600) with `FUZZ_JOBS` (12) workers against its corpus in
`$WS/fuzz/corpus/<target>`, which grows from night to night (a new machine
starts it from `$WS/fuzz/seeds`). Every finding is reproduced and grouped by
its signature (the exception message, sanitizer summary or UBSan error); each
group is a case `<target>/<signature>` of the build `fuzz`, compared with
`extra/qa/nightly/fuzz_known_failures.txt`, with up to three of its inputs kept
in the run directory (`fuzz-<target>/`). A passing case `<target>/run` records
the run's statistics.

## Known bug filters and ignored reports

A known crash that almost any input reaches ends nearly every run and hides
everything else, so the targets treat the inputs that reach it as the clean
rejection the fix should make them (`known_bug_filters.hpp`); build with
`-DFUZZ_KEEP_<ticket>` to keep the crash, and remove a filter once its bug is
fixed:

- **PBS-68** (ticket 1): a FORMAT_DESCRIPTION whose server version is not
  `major.minor.patch` ends the process. In the first 30-minute run it
  accounted for all 26,205 crashes of `event_stream_fuzzer` (seven variants:
  each version component missing or not numeric, or more than three).
  Filtered in `event_stream_fuzzer` and `stored_binlog_fuzzer`.
- **PBS-112** (ticket 39): an event header with an invalid type code where the
  `pull` sender expects one (a dump position inside an event, corrupted
  stored data) ends the process. Filtered in `stored_binlog_fuzzer`, which
  only dumps from positions on the chain of events.

`ubsan_ignorelist.txt` (`-fsanitize-ignorelist`) silences a report in
third-party code: Boost 1.90's `lexical_cast<unsigned short>` computes a digit
in `int` and overflows on a long run of digits (undefined behaviour, harmless
in practice: the result is rejected by the overflow check that follows).
`decoders_fuzzer` reached it through `util::ctime_timestamp` within seconds,
1,180 times in 90 s.

## First results (2026-10-28, 20-30 minutes per target, 6-12 workers)

| Target | Inputs | Coverage (edges) | Findings |
| --- | --- | --- | --- |
| `event_stream_fuzzer`, before the filter | 3.4 million | 962 seeds → 1,439 | PBS-68 only (26,205 inputs) |
| `event_stream_fuzzer`, with the filter | 75.6 million | 1,439 → 1,491 | none |
| `listener_packet_fuzzer` | 6.0 million | 1,948 seeds → 3,208 | none |
| `stored_binlog_fuzzer` | 7.4 million | 1,978 seeds → 2,309 | none |
| `decoders_fuzzer`, before the ignore list | 5.3 million | 1,656 | the Boost `lexical_cast` overflow only (2,022 inputs) |
| `decoders_fuzzer`, with the ignore list | 50.9 million | 1,646 seeds → 1,658 | none |

With PBS-68 filtered, the event stream target runs about 20 times as many
inputs in the same time, and finds nothing else so far.
