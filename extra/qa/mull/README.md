# Mutation testing with Mull (trial)

[Mull](https://github.com/mull-project/mull) compiles every mutant of the
code (`>` turned into `>=`, `+` into `-`, ...) into one instrumented test
binary, then runs the tests once per mutant. A mutant that no test notices
(a *survivor*) shows code whose behaviour the tests do not pin down: a weak
property, a missing boundary case, or an equivalent mutant (a change with no
observable effect).

The trial mutates the GTID and util libraries (`mull.yml`) and runs the
deterministic tests that exercise them.

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
cmake --build percona-binlog-server-build-mull_clang20 -j12 --target \
  gtid_test gtid_set_test gtid_set_property_test byte_span_encoding_test \
  uuid_test tag_test
```

## Running

`CI=1` makes Hegel derandomize its inputs and skip its failure database,
so every mutant meets the same test cases. A test binary must pass without
mutants, so the property tests run without their known failures
(`known_failures.txt`):

```bash
cd ~/ws/mull
B=~/ws/percona-binlog-server-build-mull_clang20/tests
export MULL_CONFIG=$HOME/ws/percona-binlog-server/extra/qa/mull/mull.yml CI=1
known=$(awk '!/^#/ && NF {print $1}' ~/ws/nightly/known_failures.txt | sed 's/^[a-z_0-9]*://' | sort -u)
for t in gtid_test gtid_set_test gtid_set_property_test byte_span_encoding_test uuid_test tag_test; do
  args=()
  if [[ $t == *_property_test ]]; then
    args=("--run_test=$($B/$t --list_content 2>&1 | sed -n 's/^ *\([A-Za-z0-9_]*\)\*$/\1/p' | grep -vxF "$known" | paste -sd:)")
  fi
  pkg/usr/bin/mull-runner-20 --allow-surviving --workers 12 \
    --reporters IDE --reporters SQLite --report-dir reports --report-name $t \
    $B/$t "${args[@]}" > $t.out 2>&1
done
python3 ~/ws/percona-binlog-server/extra/qa/mull/combine_mull_reports.py \
  --known ~/ws/percona-binlog-server/extra/qa/mull/mull_known_survivors.txt \
  reports/*.sqlite
```

mull-runner judges each test binary on its own; `combine_mull_reports.py`
counts a mutant as surviving only if no test kills it, and sets aside the
triaged harmless ones in `mull_known_survivors.txt`.

## First results (2026-10-28)

Six test binaries, about 15 s in all (12 workers): 118 mutants on covered
lines, 96 killed (81.4%), 22 surviving every test. Per binary:
`gtid_set_test` 72% (81 covered), `gtid_set_property_test` 76% (85),
`gtid_test` 35% (14), `byte_span_encoding_test` 89% (46), `tag_test` 100%,
`uuid_test` covers none.

Of the 22 survivors, 15 are harmless (`mull_known_survivors.txt`: 11
equivalent, 4 unreachable boundaries) and 7 are test gaps:

| Mutant | Gap |
| --- | --- |
| `binsrv/gtids/gtid_set.cpp:133` `>=` → `>` | an upper-case `A` in the hex of a UUID is never parsed (it would be read as a digit) |
| `binsrv/gtids/gtid_set.cpp:768` `<=` → `<` | a binary GTID set with an empty interval (upper == lower) is never decoded; the check that rejects it goes untested (`GtidSetDecodedFromBinaryIsPrintable`, a known failure of ticket 3 left out of the run, may reach it) |
| `util/byte_span_extractors.hpp:116`, `:138` `>` → `>=` | the checked byte span / byte array extractors never get exactly as many bytes as they need |
| `util/byte_span_inserters.hpp:165`, `:173`, `:181` `<` → `<=` | the checked packed-integer inserter never gets a buffer of exactly the encoded size (1, 3 or 4 bytes) |
