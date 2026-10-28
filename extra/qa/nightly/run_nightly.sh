#!/bin/bash
# Nightly run of the percona-binlog-server tests on pbs-infra.
#
# Builds the tree in ~/ws/percona-binlog-server as it is (kept in sync from a
# workstation) in the existing debug_gcc14, asan_gcc14 and tsan_gcc14 build
# directories (see build_project.sh), runs the unit tests, then runs every
# Hegel property test case separately with many more cases than the default
# 100 (profiles in hegel.toml), several cases at a time (JOBS / TSAN_JOBS).
# The local S3-compatible server is brought up first so the storage and config
# property tests exercise the S3 backend (PBS_TEST_S3_*) instead of skipping
# it. Failures are compared with known_failures.txt.
#
# After the C++ stages, the whole binlog_streaming MTR suite runs with the
# debug build on MySQL 8.0, 8.4 and 9.7 (MTR_DISTS), and black-box QA
# harnesses from extra/qa run against MySQL 8.4: every night
# pull_replica_test.py, query_race_test.py, pull_heartbeat_test.py and a
# short fetch_crash_test.py run per mode on file and on S3 storage; and one
# of the heavier harnesses by day of the week (QA_ROTATION: the S3 and disk
# fault sweeps, the network fault test, the listener fuzzer, the source
# change test, the over-4-GiB binlog test and the many-readers / shutdown
# test). Their
# failures are compared with mtr_known_failures.txt and qa_known_failures.txt
# (see nightly_stages.py, which runs those stages). QA_ROTATION_DAY (1-7 for
# Monday-Sunday, a comma list, 'all' or 'none'; default today's weekday)
# picks the rotation's set, e.g. to try Tuesday's set now:
#   QA_ROTATION_DAY=2 STAGES=qa OUT_DIR=~/ws/qa/nightly-try \
#     ~/ws/nightly/run_nightly.sh
#
# The release stage builds release_gcc14 (the project's RelWithDebInfo preset:
# -O2, NDEBUG, against release builds of Boost and the AWS SDK) in its own
# build directory, configuring it the first time, and runs its unit tests and
# every property test case like the C++ stage (profile nightly_release), but
# each case under a short wall-clock limit (RELEASE_CASE_TIMEOUT): without the
# assertions some known bugs loop forever instead of aborting (ticket 7), and
# a hang must come out as a TIMEOUT failure, not a stuck nightly. Then a short
# fetch_crash_test run uses the release binary. Release-only behaviour of a
# known failure is listed in known_failures.txt as 'release_gcc14:<case>'.
#
# STAGES selects what runs (default "cpp mtr qa release"); without "cpp" the
# debug / ASan / TSan trees are not built and the existing binaries are used.
# OUT_DIR runs into another directory instead of runs/<date> and then leaves
# history.tsv, the latest symlink and old runs alone, to try a stage out
# without touching the nightly's own results, e.g.
#   STAGES=mtr OUT_DIR=~/ws/qa/nightly-try ~/ws/nightly/run_nightly.sh
# (for the cpp / release stages keep OUT_DIR under $NIGHTLY: the property
# tests find hegel.toml in an ancestor of their working directory, e.g.
#   STAGES=release OUT_DIR=~/ws/nightly/try-release ...)
#
# Results: ~/ws/nightly/runs/<date>/summary.txt (also ~/ws/nightly/latest),
# one log per test case next to it, results.jsonl (one JSON line per test
# case: build, program, test case, status, ticket, reason, seconds) and
# run.json (start, revision, counts), and a line per night in
# ~/ws/nightly/history.tsv. Exits with 1 when there are new failures or a
# build fails.
set -uo pipefail
export PATH=/usr/local/bin:/usr/bin:/bin
# this host is outside EC2: without this, every S3 backend the tests create
# waits about 1.4 s on instance metadata lookups (ticket 16)
export AWS_EC2_METADATA_DISABLED=true

WS=${WS:-$HOME/ws}
SRC=$WS/percona-binlog-server
NIGHTLY=${NIGHTLY:-$WS/nightly}
KEEP_DAYS=30
CASE_TIMEOUT=3600
# the release stage's limit per property test case: a release case of 500
# examples takes seconds to a few minutes, so anything near this is a hang
RELEASE_CASE_TIMEOUT=${RELEASE_CASE_TIMEOUT:-600}
# and its address-space limit per case (ulimit -v, MB): with the assertions
# compiled out, ticket 7 makes indexed_event_block grow its index until
# std::bad_alloc, 8+ GB on this 16 GB host shared with other jobs
RELEASE_CASE_MEMORY_MB=${RELEASE_CASE_MEMORY_MB:-4096}
# property test cases run several at a time; each one is a separate process
# with its own Hegel database subtree, so they do not interfere. ThreadSanitizer
# cases are memory-heavy, so they use a smaller pool.
JOBS=${JOBS:-4}
TSAN_JOBS=${TSAN_JOBS:-2}
STAGES=" ${STAGES:-cpp mtr qa release fuzz} "
# MTR stage: <series>:<distribution under $WS/dist>; its own build thread
# (ports) and var directories so it does not collide with manual MTR runs.
# A worker of build thread N uses ports 10000+10*N.. (30 per worker) and the
# rs_* tests make 'pull' listen 1000 above the server's port, so build thread
# 500 (ports 15000-15119 and 16000-16119 with 4 workers) stays clear of the
# automatic build threads (300-399: 13000-14999 including the +1000), e.g.
# a manual run with --build-thread=300.
MYSQL_SUFFIX=linux-glibc2.28-x86_64
MTR_DISTS=${MTR_DISTS:-"8.0:mysql-8.0.45-$MYSQL_SUFFIX 8.4:mysql-8.4.8-$MYSQL_SUFFIX-minimal 9.7:mysql-9.7.2-$MYSQL_SUFFIX-minimal"}
MTR_BUILD_THREAD=${MTR_BUILD_THREAD:-500}
MTR_PARALLEL=${MTR_PARALLEL:-4}
# fuzzing stage: the libFuzzer targets of fuzz/, each run for FUZZ_SECONDS
# with FUZZ_JOBS workers against its corpus in $WS/fuzz/corpus (kept between
# nights, started from $WS/fuzz/seeds, written by fuzz/make_seeds.py)
FUZZ_TARGETS=${FUZZ_TARGETS:-"event_stream_fuzzer listener_packet_fuzzer"}
FUZZ_SECONDS=${FUZZ_SECONDS:-900}
FUZZ_JOBS=${FUZZ_JOBS:-12}
# QA-harness stage: one MySQL version, the Python libraries, a port range
QA_DIST=${QA_DIST:-8.4:mysql-8.4.8-$MYSQL_SUFFIX-minimal}
QA_PORT=${QA_PORT:-24470}
QA_RUNS=${QA_RUNS:-500}
QA_EXAMPLES=${QA_EXAMPLES:-10}
QA_STEPS=${QA_STEPS:-8}
# the S3 variant of fetch_crash_test, every night (the local S3 server)
QA_S3_EXAMPLES=${QA_S3_EXAMPLES:-5}
S3_BUCKET_DIR=${S3_BUCKET_DIR:-$WS/s3/data/pbs-mtr}
# the rotation of the heavier QA harnesses: '<weekday>:<harness>/<mode>'
# (weekday 1-7 is Monday-Sunday, date +%u); 4-15 min a night (README.md)
QA_ROTATION=${QA_ROTATION:-"1:s3_fault_sweep/position 2:disk_fault_sweep/position 3:network_fault_test/position 3:large_binlog_test/position 4:listener_fuzz_test/position 4:listener_fuzz_test/gtid 4:source_change_test/position 4:source_change_test/gtid 5:s3_fault_sweep/gtid 6:disk_fault_sweep/gtid 6:pull_load_test/position 7:network_fault_test/gtid"}
QA_ROTATION_DAY=${QA_ROTATION_DAY:-$(date -u +%u)}
# each fuzz connection takes about 1 s since the listener always does full
# caching_sha2_password authentication (PBS-10), 3000 would take 50 min
QA_FUZZ_CONNECTIONS=${QA_FUZZ_CONNECTIONS:-600}
# release stage: one short fetch_crash_test run with the release binary
RELEASE_QA_EXAMPLES=${RELEASE_QA_EXAMPLES:-5}
export PYTHONPATH=${PYTHONPATH:-$WS/qa/pylib}
want() { [[ "$STAGES" == *" $1 "* ]]; }

exec 9> "$NIGHTLY/.lock"
flock -n 9 || { echo "another nightly run is in progress"; exit 0; }

day=$(date -u +%F)
out=${OUT_DIR:-$NIGHTLY/runs/$day}
rm -rf "$out"
mkdir -p "$out" "$NIGHTLY/hegel-db"
summary=$out/summary.txt
results_file=$out/results.jsonl
: > "$results_file"
started=$(date +%s)
started_utc=$(date -u "+%F %T")
# one JSON line per result: config, program, test case, status, ticket,
# reason, seconds
# prints one JSON result line to stdout (callers redirect it to the results
# file or a per-case file)
record() {
  python3 -c 'import json,sys; print(json.dumps(dict(zip(["config","program","test_case","status","ticket","reason","seconds"], sys.argv[1:7]+[int(sys.argv[7])]))))' "$@"
}
log() { echo "[$(date -u "+%F %T")] $*" | tee -a "$out/nightly.log"; }

# runs one property test case (meant to be backgrounded) and writes its
# outcome to "$status_dir/<test case>" as tab-separated fields the main loop
# aggregates afterwards: status, program, test case, ticket, reason, seconds,
# and the human-readable summary line. Reads $config / $work / $profile /
# $run_prefix / $status_dir / $case_timeout / $case_memory_kb / known[] from
# the enclosing config iteration. A known_failures.txt line
# '<config>:<test case>' takes precedence over a plain '<test case>' line for
# that build.
run_case() {
  local program=$1 binary=$2 test_case=$3
  local case_log=$out/$config-$test_case.log
  local case_started
  case_started=$(date +%s)
  local s3_cases=${PBS_TEST_S3_TEST_CASES:-}
  [ "$config" = debug_gcc14 ] && [ -n "${S3_DEBUG_TEST_CASES:-}" ] &&
    s3_cases=$S3_DEBUG_TEST_CASES
  (cd "$work" && { [ -z "$case_memory_kb" ] || ulimit -v "$case_memory_kb"; } &&
    env ${s3_cases:+PBS_TEST_S3_TEST_CASES=$s3_cases} \
    HEGEL_DEFAULT_PROFILE=$profile \
    timeout -k 30 "$case_timeout" "${run_prefix[@]}" "$binary" \
    --run_test="$test_case" > "$case_log" 2>&1)
  local rc=$?
  local case_seconds=$(( $(date +%s) - case_started ))
  local tsan_reports
  tsan_reports=$(grep -c "WARNING: ThreadSanitizer" "$case_log")
  local ticket=${known[$config:$test_case]:-${known[$test_case]:-}}
  local status reason="" line=""
  if [ $rc -eq 0 ] && [ "$tsan_reports" -eq 0 ]; then
    rm -f "$case_log"
    if [ -n "$ticket" ]; then
      status=known-passed
      line="KNOWN PASSED  $config  $test_case  ($ticket; fixed, or not hit tonight?)"
    else
      status=passed
    fi
  else
    reason=$(grep -m1 -oE "Exception: .{0,150}" "$case_log")
    # 124: timeout's TERM worked, 137: the case ignored it and was killed
    local timed_out=""
    if [ $rc -eq 124 ] || [ $rc -eq 137 ]; then
      timed_out=" [TIMEOUT]"
      reason="TIMEOUT: no result after ${case_timeout}s (hang?)"
    fi
    [ "$tsan_reports" -gt 0 ] && reason="$tsan_reports ThreadSanitizer reports"
    grep -q "ERROR: AddressSanitizer" "$case_log" &&
      reason=$(grep -m1 -oE "ERROR: AddressSanitizer: .{0,120}" "$case_log")
    if [ -n "$ticket" ]; then
      status=known
      line="known         $config  $test_case  ($ticket)$timed_out"
    else
      status=new
      line="NEW FAILURE   $config  $test_case  $reason  ($(basename "$case_log"))"
    fi
  fi
  # separate files, so empty fields (ticket / reason / line) survive
  printf '%s' "$status" > "$status_dir/$test_case.status"
  printf '%s' "$line" > "$status_dir/$test_case.line"
  record "$config" "$program" "$test_case" "$status" "$ticket" "$reason" \
    "$case_seconds" > "$status_dir/$test_case.json"
}

# appends the results of one nightly_stages.py call (written to
# $stage_results / $stage_lines) to the run's results and counters; if the
# call failed (exit code $1) without recording anything, that counts as
# broken for build $2 / program $3
stage_results=$out/.stage-results.jsonl
stage_lines=$out/.stage-lines.txt
tally_stage() {
  local rc=$1 config=$2 program=$3 status
  if [ "$rc" -ne 0 ] && [ ! -s "$stage_results" ]; then
    record "$config" "$program" "" "no-test-cases" "" \
      "nightly_stages.py failed with exit code $rc, see nightly.log" 0 \
      > "$stage_results"
    echo "NO TEST CASES $config  $program  (nightly_stages.py failed with exit code $rc, see nightly.log)" \
      > "$stage_lines"
  fi
  while read -r status; do
    case $status in
      passed) passed=$((passed + 1)) ;;
      known-passed) unexpected_pass=$((unexpected_pass + 1)); passed=$((passed + 1)) ;;
      known) known_failed=$((known_failed + 1)) ;;
      new) new=$((new + 1)) ;;
      no-test-cases) broken=$((broken + 1)) ;;
      skipped) skipped=$((skipped + 1)) ;;
    esac
  done < <(python3 -c 'import json,sys
for line in open(sys.argv[1]):
    if line.strip(): print(json.loads(line)["status"])' "$stage_results")
  cat "$stage_results" >> "$results_file"
  local line
  while IFS= read -r line; do
    [ -n "$line" ] && results+=("$line")
  done < "$stage_lines"
  : > "$stage_results"
  : > "$stage_lines"
}

declare -A known
while read -r name ticket; do
  [[ -z "$name" || "$name" == \#* ]] && continue
  known[$name]=$ticket
done < "$NIGHTLY/known_failures.txt"

revision=$(git -C "$SRC" log -1 --format="%h %s" 2>/dev/null)
changed=$(git -C "$SRC" status --short -- src tests CMakeLists.txt 2>/dev/null | wc -l)
{
  echo "percona-binlog-server nightly, $day"
  echo "tree: $SRC at $revision ($changed paths differ from that commit)"
  echo
} > "$summary"

# Bring up the local S3-compatible server (idempotent) so the storage and
# config property tests run their S3 backend variants. The values match
# start_s3.sh / setup_test_environment.sh; these are local-only test
# credentials. If the server cannot be reached, leave PBS_TEST_S3_* unset so
# those variants skip rather than fail against a dead endpoint.
if [ -x "$WS/s3/start_s3.sh" ]; then
  # fd 9 (the run lock) is closed for it: the S3 server it may start keeps
  # running after the nightly, and would otherwise hold the lock and make
  # every later run exit with "another nightly run is in progress"
  "$WS/s3/start_s3.sh" 9>&- > "$out/s3-start.log" 2>&1 || true
fi
if curl -s -o /dev/null --max-time 5 http://127.0.0.1:9000/; then
  export PBS_TEST_S3_ENDPOINT=127.0.0.1:9000
  export PBS_TEST_S3_ACCESS_KEY=pbsaccesskey
  export PBS_TEST_S3_SECRET_KEY='pbs/secret+key=1'
  export PBS_TEST_S3_BUCKET=pbs-mtr
  log "S3 backend property variants enabled (127.0.0.1:9000, bucket pbs-mtr)"
  # nightly only: the storage '...OnS3' properties run 20 cases by default
  # (PBS_TEST_S3_TEST_CASES); the debug build runs more of them (they run
  # serially, and 100 cases in every build would add 1-3 hours a night)
  S3_DEBUG_TEST_CASES=${S3_DEBUG_TEST_CASES:-100}
  log "S3 storage property variants run $S3_DEBUG_TEST_CASES cases each in debug_gcc14, 20 elsewhere"
else
  log "S3 server not reachable; S3 property variants will be skipped"
fi

new=0; known_failed=0; passed=0; unexpected_pass=0; broken=0; skipped=0
results=()
: > "$stage_results"
: > "$stage_lines"
# builds that failed tonight (the later stages skip what needs them)
declare -A build_failed

cpp_configs=()
want cpp && cpp_configs+=(debug_gcc14 asan_gcc14 tsan_gcc14)
want release && cpp_configs+=(release_gcc14)
for config in "${cpp_configs[@]}"; do
  build_dir=$WS/percona-binlog-server-build-$config
  config_started=$(date +%s)
  targets=()
  run_prefix=()
  property_tests=()
  case_timeout=$CASE_TIMEOUT
  case_memory_kb=""
  case $config in
    release_gcc14)
      profile=nightly_release
      case_timeout=$RELEASE_CASE_TIMEOUT
      case_memory_kb=$(( RELEASE_CASE_MEMORY_MB * 1024 ))
      # the only tree the nightly configures itself (the others come from
      # setup_test_environment.sh / build_project.sh)
      if [ ! -f "$build_dir/CMakeCache.txt" ]; then
        log "configuring $config"
        cmake -S "$SRC" -B "$build_dir" --preset release_gcc14 \
          -DWITH_PROPERTY_TESTS=ON > "$out/configure-$config.log" 2>&1 ||
          log "CONFIGURE FAILED: $config (configure-$config.log)"
      fi
      ;;
    debug_gcc14) profile=nightly_debug ;;
    asan_gcc14)  profile=nightly_asan ;;
    tsan_gcc14)
      profile=nightly_tsan
      targets=(--target storage_property_test)
      run_prefix=(setarch "$(uname -m)" -R)
      # suppress ThreadSanitizer's false races inside the AWS SDK's own
      # threads (see tsan.supp); our code is still checked
      export TSAN_OPTIONS="halt_on_error=0 suppressions=$NIGHTLY/tsan.supp"
      ;;
  esac

  log "building $config"
  if ! cmake --build "$build_dir" --parallel 4 "${targets[@]}" \
      > "$out/build-$config.log" 2>&1; then
    log "BUILD FAILED: $config"
    results+=("BUILD FAILED  $config  (build-$config.log)")
    record "$config" "" "build" "build-failed" "" "see build-$config.log" 0 \
      >> "$results_file"
    broken=$((broken + 1))
    build_failed[$config]=1
    continue
  fi

  if [ "$config" != tsan_gcc14 ]; then
    log "unit tests ($config)"
    unit_started=$(date +%s)
    if (cd "$build_dir" && ctest -E "_property_test$" --output-on-failure \
        --timeout 600 > "$out/unit-$config.log" 2>&1); then
      passed=$((passed + 1))
      record "$config" "ctest" "unit tests" "passed" "" "" \
        $(( $(date +%s) - unit_started )) >> "$results_file"
    else
      results+=("NEW FAILURE   $config  unit tests  (unit-$config.log)")
      new=$((new + 1))
      record "$config" "ctest" "unit tests" "new" "" "see unit-$config.log" \
        $(( $(date +%s) - unit_started )) >> "$results_file"
    fi
    mapfile -t property_tests < <(find "$build_dir/tests" -maxdepth 1 \
      -name "*_property_test" -type f -perm -u+x | sort)
  else
    property_tests=("$build_dir/tests/storage_property_test")
  fi

  # hegel.toml is found in the working directory or an ancestor
  work=$out/work-$config
  mkdir -p "$work"
  status_dir=$out/status-$config
  rm -rf "$status_dir"
  mkdir -p "$status_dir"
  jobs=$JOBS
  [ "$config" = tsan_gcc14 ] && jobs=$TSAN_JOBS

  # share failing inputs between the builds: before its run, this build's
  # Hegel database (hegel.toml) gets every input another build has saved,
  # copied without overwriting, so a bug one build found is replayed first in
  # all of them. One shared database would not do: Hegel drops a saved input
  # that no longer fails, so a build that cannot hit it (an assertion compiled
  # out of release) would delete it for the build that found it
  database=$NIGHTLY/hegel-db/$config
  mkdir -p "$database"
  saved_before=$(find "$database" -type f | wc -l)
  for other in "$NIGHTLY"/hegel-db/*/; do
    [ "${other%/}" = "$database" ] && continue
    cp -r --update=none "$other". "$database"/
  done
  log "hegel database of $config: $saved_before saved input(s)," \
    "$(( $(find "$database" -type f | wc -l) - saved_before )) taken from" \
    "the other builds"

  # collect every (program, binary, test case) task for this config
  case_tasks=()
  for binary in "${property_tests[@]}"; do
    program=$(basename "$binary")
    mapfile -t test_cases < <("${run_prefix[@]}" "$binary" --list_content 2>&1 |
      sed -n 's/^ *\([A-Za-z0-9_]*\)\*$/\1/p')
    if [ ${#test_cases[@]} -eq 0 ]; then
      log "NO TEST CASES: $config $program"
      results+=("NO TEST CASES $config  $program  (the program lists no test cases)")
      record "$config" "$program" "" "no-test-cases" "" \
        "the program lists no test cases" 0 >> "$results_file"
      broken=$((broken + 1))
      continue
    fi
    for test_case in "${test_cases[@]}"; do
      case_tasks+=("$program|$binary|$test_case")
    done
  done

  # split out the cases that touch the S3 backend: the AWS SDK is unreliable
  # (heap-use-after-free, races) when several of its clients hit the same
  # server at once, so those run one at a time while the rest run in parallel
  parallel_tasks=()
  serial_tasks=()
  for task in "${case_tasks[@]}"; do
    IFS='|' read -r program binary test_case <<< "$task"
    if [[ "$test_case" == *OnS3 || "$test_case" == S3Storage* ]]; then
      serial_tasks+=("$task")
    else
      parallel_tasks+=("$task")
    fi
  done

  # non-S3 cases, up to 'jobs' at a time (each writes its own status files)
  running=0
  for task in "${parallel_tasks[@]}"; do
    IFS='|' read -r program binary test_case <<< "$task"
    log "$config $program/$test_case"
    run_case "$program" "$binary" "$test_case" &
    running=$((running + 1))
    if [ "$running" -ge "$jobs" ]; then
      wait -n
      running=$((running - 1))
    fi
  done
  wait

  # S3 cases, one at a time
  for task in "${serial_tasks[@]}"; do
    IFS='|' read -r program binary test_case <<< "$task"
    log "$config $program/$test_case (serial S3)"
    run_case "$program" "$binary" "$test_case"
  done

  # aggregate the per-case outcomes for this config
  for status_file in "$status_dir"/*.status; do
    [ -e "$status_file" ] || continue
    base=${status_file%.status}
    status=$(cat "$status_file")
    cat "$base.json" >> "$results_file"
    case $status in
      passed) passed=$((passed + 1)) ;;
      known-passed)
        unexpected_pass=$((unexpected_pass + 1))
        passed=$((passed + 1))
        results+=("$(cat "$base.line")")
        ;;
      known)
        known_failed=$((known_failed + 1))
        results+=("$(cat "$base.line")")
        ;;
      new)
        new=$((new + 1))
        results+=("$(cat "$base.line")")
        ;;
    esac
  done
  log "$config done in $(( ($(date +%s) - config_started) / 60 )) min"
done

debug_binsrv=$WS/percona-binlog-server-build-debug_gcc14/binlog_server
asan_binsrv=$WS/percona-binlog-server-build-asan_gcc14/binlog_server
release_binsrv=$WS/percona-binlog-server-build-release_gcc14/binlog_server

# the binlog_streaming MTR suite with the debug build, one MySQL version at a
# time (each run is already parallel)
if want mtr; then
  if [ -n "${build_failed[debug_gcc14]:-}" ]; then
    log "MTR stage skipped: the debug build failed"
  else
    for entry in $MTR_DISTS; do
      series=${entry%%:*}
      stage_started=$(date +%s)
      log "MTR suite on MySQL $series"
      python3 "$NIGHTLY/nightly_stages.py" mtr --dist "$WS/dist/${entry#*:}" \
        --series "$series" --binsrv "$debug_binsrv" \
        --known "$NIGHTLY/mtr_known_failures.txt" --results "$stage_results" \
        --lines "$stage_lines" --out "$out" \
        --vardir "$WS/mtr-var-nightly-$series" \
        --build-thread "$MTR_BUILD_THREAD" --parallel "$MTR_PARALLEL" \
        >> "$out/nightly.log" 2>&1
      tally_stage $? "mtr-$series" binlog_streaming
      log "MTR suite on MySQL $series done in $(( ($(date +%s) - stage_started) / 60 )) min"
    done
  fi
fi

rotation=()
# black-box QA harnesses from extra/qa (one MySQL version); workdirs are kept
# short because the mysqld socket path is limited to 107 characters
qa_series=${QA_DIST%%:*}
qa_args=(--dist "$WS/dist/${QA_DIST#*:}" --series "$qa_series"
  --known "$NIGHTLY/qa_known_failures.txt" --results "$stage_results"
  --lines "$stage_lines" --out "$out" --qa-dir "$SRC/extra/qa")
# qa_run <harness> <name> [--config <build label>] <harness options>...
qa_run() {
  local harness=$1 name=$2 label="qa-$qa_series"; shift 2
  [ "${1:-}" = --config ] && label=$2
  log "QA harness $name"
  python3 "$NIGHTLY/nightly_stages.py" qa "${qa_args[@]}" \
    --harness "$harness" "$@" >> "$out/nightly.log" 2>&1
  tally_stage $? "$label" "$harness"
}
if want qa; then
  if [ -n "${build_failed[debug_gcc14]:-}" ]; then
    log "QA harnesses on the debug build skipped: the debug build failed"
  else
    qa_run pull_replica_test pull_replica_test \
      --binsrv "$debug_binsrv" --workdir "$out/qa/pr" --port "$QA_PORT" \
      --timeout 900
    qa_run query_race_test query_race_test \
      --binsrv "$debug_binsrv" --workdir "$out/qa/qr" \
      --port $((QA_PORT + 30)) --runs "$QA_RUNS" --timeout 1200
    qa_run pull_heartbeat_test pull_heartbeat_test \
      --binsrv "$debug_binsrv" --workdir "$out/qa/hb" \
      --port $((QA_PORT + 120)) --timeout 900
  fi
  # the crash/resume test runs against the ASan build to catch memory errors
  if [ -n "${build_failed[asan_gcc14]:-}" ]; then
    log "fetch_crash_test skipped: the ASan build failed"
  else
    for mode in gtid position; do
      qa_run fetch_crash_test "fetch_crash_test ($mode)" \
        --binsrv "$asan_binsrv" --workdir "$out/qa/fc-$mode" \
        --port $((QA_PORT + 10)) --mode "$mode" --examples "$QA_EXAMPLES" \
        --steps "$QA_STEPS" --timeout 1800
    done
    # the same on S3 storage (cases <mode>/s3): reaches the S3 backend's
    # crash recovery, which nothing else exercises end to end
    for mode in gtid position; do
      qa_run fetch_crash_test "fetch_crash_test ($mode, S3 storage)" \
        --binsrv "$asan_binsrv" --workdir "$out/qa/fs-$mode" \
        --port $((QA_PORT + 60)) --mode "$mode" --examples "$QA_S3_EXAMPLES" \
        --steps "$QA_STEPS" --storage s3 --s3-bucket-dir "$S3_BUCKET_DIR" \
        --timeout 1200
    done
  fi
  # tonight's share of the heavier harnesses
  for entry in $QA_ROTATION; do
    entry_day=${entry%%:*}
    case ",$QA_ROTATION_DAY," in
      *,all,*|*",$entry_day,"*) rotation+=("${entry#*:}") ;;
    esac
  done
  log "QA rotation for day $QA_ROTATION_DAY: ${rotation[*]:-nothing}"
  for entry in "${rotation[@]}"; do
    harness=${entry%%/*}
    mode=${entry#*/}
    # the build each one uses: ASan to catch memory errors, but the debug
    # build under the fsfault LD_PRELOAD shim (ASan must come first in the
    # library list)
    build=asan_gcc14
    binsrv=$asan_binsrv
    if [ "$harness" = disk_fault_sweep ]; then
      build=debug_gcc14
      binsrv=$debug_binsrv
    fi
    if [ -n "${build_failed[$build]:-}" ]; then
      log "$harness ($mode) skipped: the $build build failed"
      continue
    fi
    stage_started=$(date +%s)
    common=(--binsrv "$binsrv" --mode "$mode")
    case $harness in
      s3_fault_sweep)
        qa_run s3_fault_sweep "s3_fault_sweep ($mode)" "${common[@]}" \
          --workdir "$out/qa/s3s-$mode" --port $((QA_PORT + 70)) \
          --s3-proxy-port $((QA_PORT + 75)) --s3-bucket-dir "$S3_BUCKET_DIR" \
          --timeout 3000 ;;
      disk_fault_sweep)
        qa_run disk_fault_sweep "disk_fault_sweep ($mode)" "${common[@]}" \
          --workdir "$out/qa/ds-$mode" --port $((QA_PORT + 80)) \
          --fsfault-lib "$WS/qa/fsfault/libfsfault.so" --timeout 3000 ;;
      network_fault_test)
        # Toxiproxy outlives the nightly: it must not hold the run lock
        [ -x "$WS/qa/start_toxiproxy.sh" ] &&
          "$WS/qa/start_toxiproxy.sh" 9>&- >> "$out/nightly.log" 2>&1
        qa_run network_fault_test "network_fault_test ($mode)" \
          "${common[@]}" --workdir "$out/qa/net-$mode" \
          --port $((QA_PORT + 90)) --timeout 3000 ;;
      listener_fuzz_test)
        # a new seed every day of the year; the command in a failure's log
        # replays it
        qa_run listener_fuzz_test "listener_fuzz_test ($mode)" \
          "${common[@]}" --workdir "$out/qa/lf-$mode" \
          --port $((QA_PORT + 100)) --connections "$QA_FUZZ_CONNECTIONS" \
          --seed "$((10#$(date -u +%j)))" --timeout 1500 ;;
      source_change_test)
        qa_run source_change_test "source_change_test ($mode)" \
          "${common[@]}" --workdir "$out/qa/sc-$mode" \
          --port $((QA_PORT + 110)) --timeout 1500 ;;
      large_binlog_test)
        # binlogs and transactions past 4 GiB; about 20 GB of disk while it
        # runs, removed at the end
        qa_run large_binlog_test "large_binlog_test ($mode)" \
          "${common[@]}" --workdir "$out/qa/lb-$mode" \
          --port $((QA_PORT + 140)) --timeout 5400 ;;
      pull_load_test)
        # 30 mysqlbinlog clients and 2 replicas of pull under load, one
        # reader frozen, then SIGTERM with everyone connected
        qa_run pull_load_test "pull_load_test ($mode)" \
          "${common[@]}" --workdir "$out/qa/pl-$mode" \
          --port $((QA_PORT + 150)) --timeout 1800 ;;
      *) log "unknown QA_ROTATION harness $harness" ;;
    esac
    log "$harness ($mode) done in $(( ($(date +%s) - stage_started) / 60 )) min"
  done
fi
# the release stage's crash/resume run: release-only misbehaviour of the
# resume path (wrong state where debug would assert) under the same oracle
if want release; then
  if [ -n "${build_failed[release_gcc14]:-}" ]; then
    log "release fetch_crash_test skipped: the release build failed"
  else
    stage_started=$(date +%s)
    qa_run fetch_crash_test "fetch_crash_test (release, gtid)" \
      --config release_gcc14 --binsrv "$release_binsrv" \
      --workdir "$out/qa/fc-release" --port $((QA_PORT + 50)) --mode gtid \
      --examples "$RELEASE_QA_EXAMPLES" --steps "$QA_STEPS" --timeout 900
    log "release fetch_crash_test done in $(( ($(date +%s) - stage_started) / 60 )) min"
  fi
fi
# the fuzzing stage, last so that it has the machine to itself: the libFuzzer
# targets in their own Clang build (instrumented for libFuzzer, ASan and
# UBSan, with GCC's standard library so that the GCC-built dependencies of
# debug_gcc14 link), configured by the nightly the first time. Each distinct
# finding is a case of the build 'fuzz', compared with fuzz_known_failures.txt
if want fuzz; then
  fuzz_build=$WS/percona-binlog-server-build-fuzz_clang20
  stage_started=$(date +%s)
  if [ ! -f "$fuzz_build/CMakeCache.txt" ]; then
    log "configuring fuzz_clang20"
    fuzz_flags="-fsanitize=fuzzer-no-link,address,undefined -fno-sanitize-recover=undefined -fno-omit-frame-pointer -O1 -g"
    cmake -S "$SRC" -B "$fuzz_build" -DCMAKE_BUILD_TYPE=Debug \
      -DCMAKE_C_COMPILER=clang-20 -DCMAKE_CXX_COMPILER=clang++-20 \
      -DWITH_STDLIB_LIBCXX=OFF -DCPP_STANDARD=23 -DBUILD_TESTING=OFF \
      -DWITH_FUZZERS=ON -DS2N_USE_CRYPTO_SHARED_LIBS=ON \
      "-DCMAKE_PREFIX_PATH=$WS/aws-sdk-cpp-install-debug_gcc14;$WS/boost-install-debug_gcc14" \
      "-DCMAKE_C_FLAGS=$fuzz_flags" "-DCMAKE_CXX_FLAGS=$fuzz_flags" \
      "-DCMAKE_EXE_LINKER_FLAGS=-fsanitize=address,undefined" \
      > "$out/configure-fuzz_clang20.log" 2>&1 ||
      log "CONFIGURE FAILED: fuzz_clang20 (configure-fuzz_clang20.log)"
  fi
  log "building fuzz_clang20"
  # shellcheck disable=SC2086 # one --target per word
  if ! cmake --build "$fuzz_build" --parallel 4 \
      $(printf -- '--target %s ' $FUZZ_TARGETS) \
      > "$out/build-fuzz_clang20.log" 2>&1; then
    log "BUILD FAILED: fuzz_clang20"
    results+=("BUILD FAILED  fuzz_clang20  (build-fuzz_clang20.log)")
    record fuzz_clang20 "" build build-failed "" \
      "see build-fuzz_clang20.log" 0 >> "$results_file"
    broken=$((broken + 1))
  else
    for target in $FUZZ_TARGETS; do
      corpus=$WS/fuzz/corpus/${target%_fuzzer}
      seeds=$WS/fuzz/seeds/${target%_fuzzer}
      mkdir -p "$corpus"
      if [ -z "$(ls -A "$corpus")" ] && [ -d "$seeds" ]; then
        cp -r "$seeds/." "$corpus/"
      fi
      log "fuzzing $target for ${FUZZ_SECONDS}s"
      python3 "$NIGHTLY/nightly_stages.py" fuzz --target "$target" \
        --binary "$fuzz_build/fuzz/$target" --corpus "$corpus" \
        --seconds "$FUZZ_SECONDS" --jobs "$FUZZ_JOBS" \
        --known "$NIGHTLY/fuzz_known_failures.txt" \
        --results "$stage_results" --lines "$stage_lines" --out "$out" \
        >> "$out/nightly.log" 2>&1
      tally_stage $? fuzz "$target"
    done
  fi
  log "fuzzing done in $(( ($(date +%s) - stage_started) / 60 )) min"
fi
rm -f "$stage_results" "$stage_lines"

duration=$(( ($(date +%s) - started) / 60 ))
{
  echo "new failures: $new, build failures: $broken, known failures: $known_failed, known failures that passed: $unexpected_pass, passed: $passed, skipped (MTR): $skipped"
  echo "duration: $duration min"
  echo
  printf '%s\n' "${results[@]}" | sort
  echo
  echo "Replay a property failure: cd $out/work-<build> && HEGEL_DEFAULT_PROFILE=<profile> <test binary> --run_test=<test case> (profile nightly_<debug|asan|tsan|release>; release cases under 'timeout $RELEASE_CASE_TIMEOUT')"
  echo "Replay an MTR failure: cd $WS/dist/<distribution>/mysql-test && BINSRV=$debug_binsrv ./mtr --build-thread=$MTR_BUILD_THREAD binlog_streaming.<test> (log: mtr-<series>-<test>.log)"
  echo "Replay a QA failure: see the command at the top of qa-<series>-<case>.log"
  echo "Replay a fuzzing finding: $WS/percona-binlog-server-build-fuzz_clang20/fuzz/<target> <input> (inputs listed in fuzz-<target>-<finding>.log)"
} >> "$summary"
python3 -c 'import json,sys; print(json.dumps(dict(zip(["day","started","revision","qa_rotation","new","broken","known","known_passed","passed","skipped","minutes"], sys.argv[1:5]+[int(v) for v in sys.argv[5:]])), indent=1))' \
  "$day" "$started_utc" "$revision" "${rotation[*]:-}" "$new" "$broken" "$known_failed" \
  "$unexpected_pass" "$passed" "$skipped" "$duration" > "$out/run.json"
if [ -n "${OUT_DIR:-}" ]; then
  log "done (OUT_DIR run, history not updated): $new new, $broken build failures, $known_failed known"
  [ $new -eq 0 ] && [ $broken -eq 0 ]
  exit
fi
printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$day" "$revision" "$new" "$broken" \
  "$known_failed" "$passed" "$duration" >> "$NIGHTLY/history.tsv"
ln -sfn "runs/$day" "$NIGHTLY/latest"
find "$NIGHTLY/runs" -mindepth 1 -maxdepth 1 -type d -mtime +"$KEEP_DAYS" \
  -exec rm -rf {} +
log "done: $new new, $broken build failures, $known_failed known"
[ $new -eq 0 ] && [ $broken -eq 0 ]
