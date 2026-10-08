#!/bin/bash
# Nightly run of the percona-binlog-server tests on pbs-infra.
#
# Builds the tree in ~/ws/percona-binlog-server as it is (kept in sync from a
# workstation) in the existing debug_gcc14, asan_gcc14 and tsan_gcc14 build
# directories (see build_project.sh), runs the unit tests, then runs every
# Hegel property test case separately with many more cases than the default
# 100 (profiles in hegel.toml). Failures are compared with
# known_failures.txt.
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
NIGHTLY=$WS/nightly
KEEP_DAYS=30
CASE_TIMEOUT=3600

exec 9> "$NIGHTLY/.lock"
flock -n 9 || { echo "another nightly run is in progress"; exit 0; }

day=$(date -u +%F)
out=$NIGHTLY/runs/$day
rm -rf "$out"
mkdir -p "$out" "$NIGHTLY/hegel-db"
summary=$out/summary.txt
results_file=$out/results.jsonl
: > "$results_file"
started=$(date +%s)
started_utc=$(date -u "+%F %T")
# one JSON line per result: config, program, test case, status, ticket,
# reason, seconds
record() {
  python3 -c 'import json,sys; print(json.dumps(dict(zip(["config","program","test_case","status","ticket","reason","seconds"], sys.argv[1:7]+[int(sys.argv[7])]))))' "$@" >> "$results_file"
}
log() { echo "[$(date -u "+%F %T")] $*" | tee -a "$out/nightly.log"; }

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

new=0; known_failed=0; passed=0; unexpected_pass=0; broken=0
results=()

for config in debug_gcc14 asan_gcc14 tsan_gcc14; do
  build_dir=$WS/percona-binlog-server-build-$config
  targets=()
  run_prefix=()
  property_tests=()
  case $config in
    debug_gcc14) profile=nightly_debug ;;
    asan_gcc14)  profile=nightly_asan ;;
    tsan_gcc14)
      profile=nightly_tsan
      targets=(--target storage_property_test)
      run_prefix=(setarch "$(uname -m)" -R)
      export TSAN_OPTIONS="halt_on_error=0"
      ;;
  esac

  log "building $config"
  if ! cmake --build "$build_dir" --parallel 4 "${targets[@]}" \
      > "$out/build-$config.log" 2>&1; then
    log "BUILD FAILED: $config"
    results+=("BUILD FAILED  $config  (build-$config.log)")
    record "$config" "" "build" "build-failed" "" "see build-$config.log" 0
    broken=$((broken + 1))
    continue
  fi

  if [ "$config" != tsan_gcc14 ]; then
    log "unit tests ($config)"
    unit_started=$(date +%s)
    if (cd "$build_dir" && ctest -E "_property_test$" --output-on-failure \
        --timeout 600 > "$out/unit-$config.log" 2>&1); then
      passed=$((passed + 1))
      record "$config" "ctest" "unit tests" "passed" "" "" $(( $(date +%s) - unit_started ))
    else
      results+=("NEW FAILURE   $config  unit tests  (unit-$config.log)")
      new=$((new + 1))
      record "$config" "ctest" "unit tests" "new" "" "see unit-$config.log" $(( $(date +%s) - unit_started ))
    fi
    mapfile -t property_tests < <(find "$build_dir/tests" -maxdepth 1 \
      -name "*_property_test" -type f -perm -u+x | sort)
  else
    property_tests=("$build_dir/tests/storage_property_test")
  fi

  # hegel.toml is found in the working directory or an ancestor
  work=$out/work-$config
  mkdir -p "$work"
  for binary in "${property_tests[@]}"; do
    program=$(basename "$binary")
    mapfile -t test_cases < <("${run_prefix[@]}" "$binary" --list_content 2>&1 |
      sed -n 's/^ *\([A-Za-z0-9_]*\)\*$/\1/p')
    if [ ${#test_cases[@]} -eq 0 ]; then
      log "NO TEST CASES: $config $program"
      results+=("NO TEST CASES $config  $program  (the program lists no test cases)")
      record "$config" "$program" "" "no-test-cases" "" "the program lists no test cases" 0
      broken=$((broken + 1))
      continue
    fi
    for test_case in "${test_cases[@]}"; do
      case_log=$out/$config-$test_case.log
      log "$config $program/$test_case"
      case_started=$(date +%s)
      (cd "$work" && HEGEL_DEFAULT_PROFILE=$profile \
        timeout "$CASE_TIMEOUT" "${run_prefix[@]}" "$binary" \
        --run_test="$test_case" > "$case_log" 2>&1)
      rc=$?
      case_seconds=$(( $(date +%s) - case_started ))
      tsan_reports=$(grep -c "WARNING: ThreadSanitizer" "$case_log")
      if [ $rc -eq 0 ] && [ "$tsan_reports" -eq 0 ]; then
        if [ -n "${known[$test_case]:-}" ]; then
          results+=("KNOWN PASSED  $config  $test_case  (${known[$test_case]}; fixed, or not hit tonight?)")
          unexpected_pass=$((unexpected_pass + 1))
          record "$config" "$program" "$test_case" "known-passed" "${known[$test_case]}" "" $case_seconds
        else
          record "$config" "$program" "$test_case" "passed" "" "" $case_seconds
        fi
        passed=$((passed + 1))
        rm -f "$case_log"
        continue
      fi
      reason=$(grep -m1 -oE "Exception: .{0,150}" "$case_log")
      [ $rc -eq 124 ] && reason="timed out after ${CASE_TIMEOUT}s"
      [ "$tsan_reports" -gt 0 ] && reason="$tsan_reports ThreadSanitizer reports"
      grep -q "ERROR: AddressSanitizer" "$case_log" &&
        reason=$(grep -m1 -oE "ERROR: AddressSanitizer: .{0,120}" "$case_log")
      if [ -n "${known[$test_case]:-}" ]; then
        results+=("known         $config  $test_case  (${known[$test_case]})")
        known_failed=$((known_failed + 1))
        record "$config" "$program" "$test_case" "known" "${known[$test_case]}" "$reason" $case_seconds
      else
        results+=("NEW FAILURE   $config  $test_case  $reason  ($(basename "$case_log"))")
        new=$((new + 1))
        record "$config" "$program" "$test_case" "new" "" "$reason" $case_seconds
      fi
    done
  done
done

duration=$(( ($(date +%s) - started) / 60 ))
{
  echo "new failures: $new, build failures: $broken, known failures: $known_failed, known failures that passed: $unexpected_pass, passed: $passed"
  echo "duration: $duration min"
  echo
  printf '%s\n' "${results[@]}" | sort
  echo
  echo "Replay a failure: cd $out/work-<build> && HEGEL_DEFAULT_PROFILE=<profile> <test binary> --run_test=<test case>"
} >> "$summary"
printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$day" "$revision" "$new" "$broken" \
  "$known_failed" "$passed" "$duration" >> "$NIGHTLY/history.tsv"
python3 -c 'import json,sys; print(json.dumps(dict(zip(["day","started","revision","new","broken","known","known_passed","passed","minutes"], sys.argv[1:4]+[int(v) for v in sys.argv[4:]])), indent=1))' \
  "$day" "$started_utc" "$revision" "$new" "$broken" "$known_failed" \
  "$unexpected_pass" "$passed" "$duration" > "$out/run.json"
ln -sfn "runs/$day" "$NIGHTLY/latest"
find "$NIGHTLY/runs" -mindepth 1 -maxdepth 1 -type d -mtime +"$KEEP_DAYS" \
  -exec rm -rf {} +
log "done: $new new, $broken build failures, $known_failed known"
[ $new -eq 0 ] && [ $broken -eq 0 ]
