#!/bin/bash
# Runs mull-runner over the test binaries of a Mull build (see README.md) and
# combines the reports: prints the mutants no test kills, apart from the
# triaged ones in mull_known_survivors.txt (MULL_KNOWN_SURVIVORS), and exits 1
# if there are any.
#
#   run_mull.sh <mull-runner> <build dir> <report dir> [<known failures>]
#   run_mull.sh --targets      (prints the test binaries, for the build)
#
# <known failures>: known_failures.txt; the test cases it lists are left out
# (a test binary must pass without mutants). MULL_CONFIG must point to
# mull.yml; CI=1 is set so that Hegel derandomizes its inputs.
set -u
runner=$1 build=${2:-} reports=${3:-} known_file=${4:-}
here=$(cd "$(dirname "$0")" && pwd)
export CI=1
export MULL_CONFIG=${MULL_CONFIG:-$here/mull.yml}
WORKERS=${MULL_WORKERS:-12}

# <binary> [timeout=<ms>] [test cases to leave out, besides the known
# failures]; the timeout (default: Mull's) covers the unmutated warm-up run,
# so a slow binary needs one
TESTS=(
  "gtid_test"
  "gtid_set_test"
  "gtid_set_property_test"
  "byte_span_encoding_test"
  "util_test"
  "tag_test"
  "event_test"
  "heartbeat_log_v2_event_test"
  "event_property_test"
  "event_generation_property_test"
  "reader_context_property_test"
  "indexed_event_block_property_test"
  "search_helpers_property_test"
  "rewriter_property_test"
  # about 50 s per run: the slowest binary of the set; its GTID-mode
  # property is left out (it mostly exercises indexed_event_block, which is
  # not mutated, and would make the run 20 minutes longer)
  "sender_context_property_test timeout=300000 GtidModeStreamsExactlyTheMissingTransactions"
  # the S3 URI cases start the AWS SDK for each case (minutes per run)
  "config_value_property_test S3StorageUriMatchesModel S3StorageCredentialsAreDecoded"
)

if [ "$runner" = --targets ]; then
  for entry in "${TESTS[@]}"; do
    echo "${entry%% *}"
  done
  exit 0
fi

known=""
if [ -n "$known_file" ] && [ -f "$known_file" ]; then
  known=$(awk '!/^#/ && NF {print $1}' "$known_file" | sed 's/^[a-z_0-9]*://' | sort -u)
fi
rm -rf "$reports"
mkdir -p "$reports"
for entry in "${TESTS[@]}"; do
  read -r binary rest <<< "$entry"
  timeout_args=()
  skip=""
  for word in $rest; do
    case $word in
      timeout=*) timeout_args=(--timeout "${word#timeout=}") ;;
      *) skip+=" $word" ;;
    esac
  done
  path=$build/tests/$binary
  if [ ! -x "$path" ]; then
    echo "missing test binary $path" >&2
    exit 2
  fi
  args=()
  leave_out=$(printf '%s\n%s\n' "$known" "${skip// /$'\n'}" | sed '/^$/d')
  all=$("$path" --list_content 2>&1 | sed -n 's/^ *\([A-Za-z0-9_]*\)\*$/\1/p')
  kept=$(grep -vxF -- "$leave_out" <<< "$all" | paste -sd:)
  if [ -z "$kept" ]; then
    echo "$binary: every test case is left out" >&2
    continue
  fi
  if [ "$kept" != "$(paste -sd: <<< "$all")" ]; then
    args=("--run_test=$kept")
  fi
  started=$(date +%s)
  "$runner" --allow-surviving --workers "$WORKERS" "${timeout_args[@]}" \
    --reporters IDE --reporters SQLite --report-dir "$reports" \
    --report-name "$binary" "$path" "${args[@]}" > "$reports/$binary.out" 2>&1
  status=$?
  score=$(cat "$reports/$binary.txt" "$reports/$binary.out" 2>/dev/null |
    grep -o "Mutation score: [0-9]*%\|All mutations have been killed\|No mutants found" | head -1)
  echo "$binary: ${score:-no score (exit $status, see $reports/$binary.out)} in $(( $(date +%s) - started )) s"
done
python3 "$here/combine_mull_reports.py" \
  --known "${MULL_KNOWN_SURVIVORS:-$here/mull_known_survivors.txt}" \
  --json "$reports/summary.json" "$reports"/*.sqlite
python3 -c 'import json,sys; sys.exit(1 if json.load(open(sys.argv[1]))["surviving"] else 0)' \
  "$reports/summary.json"
