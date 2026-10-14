#!/bin/bash
# Runs the Hegel property tests of a build at Hegel's default (fast)
# settings, one process per test case, skipping the cases known_failures.txt
# lists (plain '<test case>  ticket ...' lines; '<build>:<case>' overrides are
# for the nightly), so that a known, unfixed bug does not fail CI while any
# other failure still does.
#
# Usage: ci_property_tests.sh <build dir> [<known_failures.txt>] [--parallel N]
set -euo pipefail
# without this, every S3 backend a config test creates waits on EC2 instance
# metadata lookups (ticket 16): config_value_property_test takes ~9 min
# instead of ~30 s
export AWS_EC2_METADATA_DISABLED=true
build_dir=$(cd "${1:?usage: $0 <build dir> [<known_failures.txt>] [--parallel N]}" && pwd)
shift
known_file=$(dirname "$0")/known_failures.txt
if [ $# -gt 0 ] && [ -f "$1" ]; then
  known_file=$1
  shift
fi

mapfile -t known < <(sed -n 's/^\([A-Za-z0-9_][A-Za-z0-9_]*\)  .*/\1/p' "$known_file")
shopt -s nullglob
programs=("$build_dir"/tests/*_property_test)
[ ${#programs[@]} -gt 0 ] || { echo "no property test programs in $build_dir/tests (configure with -DWITH_PROPERTY_TESTS=ON)"; exit 1; }

# every case runs in its own process, as in the nightly: a case that
# crashes the process (a caught SIGSEGV / SIGABRT) would otherwise make the
# program's later cases return without running
jobs=1
while [ $# -gt 0 ]; do
  case $1 in
    --parallel|-j) jobs=$2; shift 2 ;;
    *) shift ;;
  esac
done
list=$(mktemp)
trap 'rm -f "$list"' EXIT
for binary in "${programs[@]}"; do
  program=$(basename "$binary")
  mapfile -t cases < <("$binary" --list_content 2>&1 |
    sed -n 's/^ *\([A-Za-z0-9_]*\)\*$/\1/p')
  skipped=0
  for test_case in "${cases[@]}"; do
    if [[ " ${known[*]} " == *" $test_case "* ]]; then
      skipped=$((skipped + 1))
    else
      printf '%s %s\n' "$binary" "$test_case" >> "$list"
    fi
  done
  echo "$program: ${#cases[@]} cases, $skipped known failures skipped"
done

cd "$build_dir"
failed=0
xargs -P "$jobs" -L 1 sh -c '
  log=$(mktemp)
  if "$0" --run_test="$1" > "$log" 2>&1; then
    echo "PASS $(basename "$0")/$1"
  else
    echo "FAIL $(basename "$0")/$1"; cat "$log"
    rm -f "$log"; exit 1
  fi
  rm -f "$log"' < "$list" || failed=1
exit $failed
