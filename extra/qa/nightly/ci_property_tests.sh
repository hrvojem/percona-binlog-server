#!/bin/bash
# Runs the Hegel property tests of a build through ctest at Hegel's default
# (fast) settings, skipping the test cases known_failures.txt lists, so that
# a known, unfixed bug does not fail CI while any other failure still does.
#
# The skipped cases are passed to every test program as a Boost.Test run
# filter (BOOST_TEST_RUN_FILTERS="!CaseA:!CaseB:..."; a name a program does not
# have is ignored). A program whose every case is a known failure would then
# have nothing to run, which Boost.Test reports as an error, so such a program
# is left out of the ctest run altogether.
#
# Usage: ci_property_tests.sh <build dir> [<known_failures.txt>] [<ctest options>...]
set -euo pipefail
# without this, every S3 backend a config test creates waits on EC2 instance
# metadata lookups (ticket 16): config_value_property_test takes ~9 min
# instead of ~30 s
export AWS_EC2_METADATA_DISABLED=true
build_dir=$(cd "${1:?usage: $0 <build dir> [<known_failures.txt>] [<ctest options>...]}" && pwd)
shift
known_file=$(dirname "$0")/known_failures.txt
if [ $# -gt 0 ] && [ -f "$1" ]; then
  known_file=$1
  shift
fi

mapfile -t known < <(sed -n 's/^\([A-Za-z0-9_][A-Za-z0-9_]*\)  .*/\1/p' "$known_file")
filter=""
[ ${#known[@]} -gt 0 ] && filter=$(printf '!%s:' "${known[@]}") && filter=${filter%:}

excluded=()
shopt -s nullglob
programs=("$build_dir"/tests/*_property_test)
[ ${#programs[@]} -gt 0 ] || { echo "no property test programs in $build_dir/tests (configure with -DWITH_PROPERTY_TESTS=ON)"; exit 1; }
for binary in "${programs[@]}"; do
  program=$(basename "$binary")
  mapfile -t cases < <("$binary" --list_content 2>&1 |
    sed -n 's/^ *\([A-Za-z0-9_]*\)\*$/\1/p')
  skipped=()
  for test_case in "${cases[@]}"; do
    [[ " ${known[*]} " == *" $test_case "* ]] && skipped+=("$test_case")
  done
  echo "$program: ${#cases[@]} cases, ${#skipped[@]} known failures skipped${skipped[*]:+: ${skipped[*]}}"
  if [ ${#cases[@]} -gt 0 ] && [ ${#skipped[@]} -eq ${#cases[@]} ]; then
    excluded+=("$program")
  fi
done

ctest_args=(-R '_property_test$')
if [ ${#excluded[@]} -gt 0 ]; then
  echo "left out (every case is a known failure): ${excluded[*]}"
  ctest_args+=(-E "^($(IFS='|'; echo "${excluded[*]}"))\$")
fi
cd "$build_dir"
BOOST_TEST_RUN_FILTERS=$filter ctest "${ctest_args[@]}" --output-on-failure "$@"
