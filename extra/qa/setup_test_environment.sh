#!/bin/bash
# Sets up the percona-binlog-server QA testing environment on a fresh
# Ubuntu 24.04 (x86_64) machine, the way it is set up on the shared test host.
#
# It provisions everything the tests under extra/qa and the binlog_streaming
# MTR suite need:
#   * the GCC 14 toolchain and build dependencies (system packages);
#   * Boost and the AWS SDK for C++, built with the project's CMake presets;
#   * the binlog_server builds (debug, ASan, TSan, coverage);
#   * the MySQL server + test tarballs for 8.0, 8.4 and 9.7;
#   * the Python libraries the harnesses use (Hypothesis, gcovr), in a
#     target directory added to PYTHONPATH (no virtualenv needed);
#   * a local S3-compatible server (Versity S3 Gateway) and the AWS CLI;
#   * Toxiproxy, for the network fault tests;
#   * the fsfault LD_PRELOAD library, for the disk fault tests;
#   * small start_*.sh helpers for the S3 server and Toxiproxy.
#
# It is idempotent: every step is skipped when its result is already there, so
# it is safe to re-run after a partial setup. It needs passwordless sudo for
# the system-package step only.
#
# Usage:
#   extra/qa/setup_test_environment.sh [step ...]
#
# With no arguments it runs every step. Named steps run only those, e.g.
#   setup_test_environment.sh deps project            # just rebuild the code
#   setup_test_environment.sh mtr s3 toxiproxy        # just the services
# Steps: packages repo deps project mtr pylib s3 toxiproxy fsfault helpers nightly
#
# The 'nightly' step installs the nightly test runner under $WS/nightly and a
# crontab entry that runs it daily (schedule from NIGHTLY_CRON).
#
# Environment overrides:
#   WS           workspace directory            (default: $HOME/ws)
#   REPO_URL     git URL to clone if the repo is not already under WS
#                (default: https://github.com/Percona-Lab/percona-binlog-server.git)
#   REPO_BRANCH  branch to clone                (default: main)
#   PROJECT_PRESETS  project configs to build   (default: "debug_gcc14 asan_gcc14 tsan_gcc14 coverage_gcc14")
#   JOBS         parallel build jobs            (default: nproc)
#   NIGHTLY_CRON cron schedule for the nightly   (default: "30 1 * * *";
#                run, five fields; empty string   empty installs the runner
#                installs the runner but no cron)  but no crontab entry

set -uo pipefail

# --------------------------------------------------------------------------
# configuration
# --------------------------------------------------------------------------
WS="${WS:-$HOME/ws}"
REPO_URL="${REPO_URL:-https://github.com/Percona-Lab/percona-binlog-server.git}"
REPO_BRANCH="${REPO_BRANCH:-main}"
PROJECT_PRESETS="${PROJECT_PRESETS:-debug_gcc14 asan_gcc14 tsan_gcc14 coverage_gcc14}"
JOBS="${JOBS:-$(nproc)}"
SRC="$WS/percona-binlog-server"

# pinned versions, matching the shared test host
BOOST_TAG="boost-1.90.0"
AWS_SDK_TAG="1.11.774"
MYSQL_SUFFIX="linux-glibc2.28-x86_64"
MYSQL_80="8.0.45"
MYSQL_84="8.4.8"
MYSQL_97="9.7.2"
HYPOTHESIS_VERSION="6.168.5"
GCOVR_VERSION="8.6"
VERSITYGW_VERSION="1.8.0"
AWSCLI_VERSION="2.37.10"
TOXIPROXY_VERSION="2.12.0"

# nightly cron schedule (crontab five-field form); an explicit empty value
# (NIGHTLY_CRON="") installs the runner but no cron line. The '-' (not ':-')
# keeps an empty string empty instead of falling back to the default.
NIGHTLY_CRON="${NIGHTLY_CRON-30 1 * * *}"

# S3 test credentials and bucket (what the extra/qa tools default to)
S3_ACCESS_KEY="pbsaccesskey"
S3_SECRET_KEY="pbs/secret+key=1"
S3_BUCKET="pbs-mtr"

log()  { echo "[$(date '+%F %T')] $*"; }
fail() { echo "[$(date '+%F %T')] ERROR: $*" >&2; exit 1; }

# downloads $1 to $2 unless $2 already exists
download() {
  local url="$1" out="$2"
  [ -s "$out" ] && return 0
  log "downloading $(basename "$out")"
  curl -sSfL -o "$out" "$url" || fail "could not download $url"
}

# --------------------------------------------------------------------------
# steps
# --------------------------------------------------------------------------
step_packages() {
  log "installing system packages (sudo)"
  sudo -n apt-get update -qq || fail "sudo apt-get update failed (passwordless sudo required)"
  sudo -n apt-get install -y -qq \
    build-essential gcc-14 g++-14 cmake ninja-build git curl ca-certificates \
    pkg-config unzip xz-utils python3 python3-pip python3-venv \
    libssl-dev zlib1g-dev libaio-dev libnuma1 libncurses6 \
    || fail "apt-get install failed"
  # the MySQL minimal tarballs link against libaio.so.1
  if [ ! -e /lib/x86_64-linux-gnu/libaio.so.1 ] && \
     [ -e /lib/x86_64-linux-gnu/libaio.so ]; then
    sudo -n ln -s libaio.so /lib/x86_64-linux-gnu/libaio.so.1
  fi
}

step_repo() {
  mkdir -p "$WS" "$WS/logs"
  if [ -d "$SRC/.git" ]; then
    log "repository already present at $SRC"
    return 0
  fi
  # if this script is being run from inside a checkout, use that checkout
  local here
  here="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
  if [ -d "$here/.git" ] && [ -f "$here/extra/qa/fetch_crash_test.py" ]; then
    log "linking the current checkout into $SRC"
    ln -sfn "$here" "$SRC"
  else
    log "cloning $REPO_URL ($REPO_BRANCH)"
    git clone -q --branch "$REPO_BRANCH" "$REPO_URL" "$SRC" \
      || fail "git clone failed"
  fi
}

step_deps() {
  [ -d "$SRC" ] || fail "repository missing; run the 'repo' step first"
  cd "$WS"
  if [ ! -d boost ]; then
    log "cloning Boost $BOOST_TAG"
    git clone -q --recurse-submodules -b "$BOOST_TAG" --jobs="$JOBS" \
      https://github.com/boostorg/boost.git || fail "boost clone failed"
  fi
  cp "$SRC/extra/cmake_presets/boost/CMakePresets.json" "$WS/boost/"
  if [ ! -d aws-sdk-cpp ]; then
    log "cloning aws-sdk-cpp $AWS_SDK_TAG"
    git clone -q --recurse-submodules -b "$AWS_SDK_TAG" --jobs="$JOBS" \
      https://github.com/aws/aws-sdk-cpp.git || fail "aws-sdk-cpp clone failed"
  fi
  cp "$SRC/extra/cmake_presets/aws-sdk-cpp/CMakePresets.json" "$WS/aws-sdk-cpp/"

  # the dependencies are needed for every non-coverage preset; coverage and
  # tsan reuse the debug_gcc14 dependency install
  local dep_presets
  dep_presets="$(echo "$PROJECT_PRESETS" | tr ' ' '\n' \
    | sed -e 's/tsan_gcc14/debug_gcc14/' -e 's/coverage_gcc14/debug_gcc14/' \
    | sort -u | tr '\n' ' ')"
  for preset in $dep_presets; do
    for dep in boost aws-sdk-cpp; do
      if [ -f "$WS/$dep-build-$preset/.installed" ]; then
        log "$dep ($preset) already installed"
        continue
      fi
      log "configuring $dep ($preset)"
      cmake "$WS/$dep" --preset "$preset" \
        > "$WS/logs/$dep-$preset-configure.log" 2>&1 \
        || fail "$dep ($preset) configure failed, see logs/$dep-$preset-configure.log"
      log "building $dep ($preset)"
      cmake --build "$WS/$dep-build-$preset" --parallel "$JOBS" \
        > "$WS/logs/$dep-$preset-build.log" 2>&1 \
        || fail "$dep ($preset) build failed"
      log "installing $dep ($preset)"
      cmake --install "$WS/$dep-build-$preset" \
        > "$WS/logs/$dep-$preset-install.log" 2>&1 \
        || fail "$dep ($preset) install failed"
      touch "$WS/$dep-build-$preset/.installed"
    done
  done
}

step_project() {
  [ -d "$SRC" ] || fail "repository missing; run the 'repo' step first"
  cd "$WS"
  for config in $PROJECT_PRESETS; do
    local build_dir="$WS/percona-binlog-server-build-$config"
    local configure=(cmake -S "$SRC" -B "$build_dir" -DWITH_PROPERTY_TESTS=ON)
    case "$config" in
      tsan_gcc14)
        # the debug preset with ThreadSanitizer, for the concurrent storage
        # property test
        local tsan="-fsanitize=thread"
        configure+=(--preset debug_gcc14 -DHEGEL_SANITIZE=thread
                    "-DCMAKE_C_FLAGS=$tsan" "-DCMAKE_CXX_FLAGS=$tsan"
                    "-DCMAKE_EXE_LINKER_FLAGS=$tsan"
                    "-DCMAKE_SHARED_LINKER_FLAGS=$tsan")
        ;;
      coverage_gcc14)
        # the debug preset with gcov instrumentation, for the coverage report
        local cov="--coverage -fprofile-update=atomic -fprofile-abs-path"
        configure+=(--preset debug_gcc14
                    "-DCMAKE_C_FLAGS=$cov" "-DCMAKE_CXX_FLAGS=$cov"
                    "-DCMAKE_EXE_LINKER_FLAGS=--coverage")
        ;;
      *)
        configure+=(--preset "$config")
        ;;
    esac
    log "configuring project ($config)"
    "${configure[@]}" > "$WS/logs/project-$config-configure.log" 2>&1 \
      || fail "project ($config) configure failed, see logs/project-$config-configure.log"
    log "building project ($config)"
    if [ "$config" = tsan_gcc14 ]; then
      # TSan build is only used for the storage property test
      cmake --build "$build_dir" --parallel "$JOBS" \
        --target storage_property_test \
        > "$WS/logs/project-$config-build.log" 2>&1 \
        || fail "project ($config) build failed"
    else
      cmake --build "$build_dir" --parallel "$JOBS" \
        > "$WS/logs/project-$config-build.log" 2>&1 \
        || fail "project ($config) build failed"
    fi
    log "project ($config) built: $build_dir/binlog_server"
  done
}

step_mtr() {
  mkdir -p "$WS/dist"
  cd "$WS/dist"
  local url="https://dev.mysql.com/get/Downloads"
  fetch_dist() {
    local series="$1" file="$2"
    if [ ! -f ".$file.done" ]; then
      download "$url/$series/$file" "$file"
      log "unpacking $file"
      tar xf "$file" && rm -f "$file" && touch ".$file.done"
    fi
  }
  fetch_dist MySQL-8.0 "mysql-$MYSQL_80-$MYSQL_SUFFIX.tar.xz"
  fetch_dist MySQL-8.0 "mysql-test-$MYSQL_80-$MYSQL_SUFFIX.tar.xz"
  fetch_dist MySQL-8.4 "mysql-$MYSQL_84-$MYSQL_SUFFIX-minimal.tar.xz"
  fetch_dist MySQL-8.4 "mysql-test-$MYSQL_84-$MYSQL_SUFFIX-minimal.tar.xz"
  fetch_dist MySQL-9.7 "mysql-$MYSQL_97-$MYSQL_SUFFIX-minimal.tar.xz"
  fetch_dist MySQL-9.7 "mysql-test-$MYSQL_97-$MYSQL_SUFFIX-minimal.tar.xz"
  # link the binlog_streaming suite into each distribution's test suite
  for dist in "mysql-$MYSQL_80-$MYSQL_SUFFIX" \
              "mysql-$MYSQL_84-$MYSQL_SUFFIX-minimal" \
              "mysql-$MYSQL_97-$MYSQL_SUFFIX-minimal"; do
    [ -d "$WS/dist/$dist" ] && ln -sfn "$SRC/mtr/binlog_streaming" \
      "$WS/dist/$dist/mysql-test/suite/binlog_streaming"
  done
  perl -MCompress::Zlib -e 1 2>/dev/null \
    || log "WARNING: perl Compress::Zlib missing (some MTR tests need it)"
}

step_pylib() {
  local target="$WS/qa/pylib"
  mkdir -p "$target"
  if [ -d "$target/hypothesis" ] && [ -d "$target/gcovr" ]; then
    log "python libraries already present in $target"
    return 0
  fi
  log "installing Hypothesis $HYPOTHESIS_VERSION and gcovr $GCOVR_VERSION into $target"
  python3 -m pip install --quiet --target "$target" \
    "hypothesis==$HYPOTHESIS_VERSION" "gcovr==$GCOVR_VERSION" \
    || fail "pip install into $target failed (use PYTHONPATH=$target to import them)"
}

step_s3() {
  local s3="$WS/s3"
  mkdir -p "$s3/bin" "$s3/dl" "$s3/data"
  # Versity S3 Gateway (POSIX backend)
  if [ ! -x "$s3/bin/versitygw" ]; then
    download \
      "https://github.com/versity/versitygw/releases/download/v$VERSITYGW_VERSION/versitygw_v${VERSITYGW_VERSION}_Linux_x86_64.tar.gz" \
      "$s3/dl/versitygw.tar.gz"
    tar xf "$s3/dl/versitygw.tar.gz" -C "$s3/dl"
    install -m 0755 "$s3/dl/versitygw" "$s3/bin/versitygw" 2>/dev/null \
      || install -m 0755 "$(find "$s3/dl" -name versitygw -type f | head -1)" "$s3/bin/versitygw"
  fi
  # AWS CLI v2
  if [ ! -x "$s3/bin/aws" ]; then
    download "https://awscli.amazonaws.com/awscli-exe-linux-x86_64-$AWSCLI_VERSION.zip" \
      "$s3/dl/awscliv2.zip"
    rm -rf "$s3/dl/aws"
    unzip -q "$s3/dl/awscliv2.zip" -d "$s3/dl"
    "$s3/dl/aws/install" --install-dir "$s3/aws-cli" --bin-dir "$s3/bin" \
      --update > "$s3/dl/install.log" 2>&1 || fail "aws cli install failed"
  fi
  log "versitygw $("$s3/bin/versitygw" --version 2>&1 | awk '/Version/{print $3; exit}'), aws $("$s3/bin/aws" --version 2>&1 | awk '{print $1}')"
}

step_toxiproxy() {
  local bin="$WS/qa/bin"
  mkdir -p "$bin"
  local base="https://github.com/Shopify/toxiproxy/releases/download/v$TOXIPROXY_VERSION"
  if [ ! -x "$bin/toxiproxy-server" ]; then
    download "$base/toxiproxy-server-linux-amd64" "$bin/toxiproxy-server"
    chmod +x "$bin/toxiproxy-server"
  fi
  if [ ! -x "$bin/toxiproxy-cli" ]; then
    download "$base/toxiproxy-cli-linux-amd64" "$bin/toxiproxy-cli"
    chmod +x "$bin/toxiproxy-cli"
  fi
  log "toxiproxy $("$bin/toxiproxy-server" --version 2>&1 | awk '{print $NF; exit}')"
}

step_fsfault() {
  local dir="$WS/qa/fsfault"
  mkdir -p "$dir"
  [ -f "$SRC/extra/qa/fsfault/fsfault.c" ] \
    || fail "fsfault source missing at $SRC/extra/qa/fsfault/fsfault.c"
  log "building libfsfault.so"
  gcc-14 -shared -fPIC -O2 -o "$dir/libfsfault.so" \
    "$SRC/extra/qa/fsfault/fsfault.c" -ldl \
    || fail "fsfault build failed"
}

# small run-time helpers the extra/qa tools expect, written into the workspace
step_helpers() {
  mkdir -p "$WS/qa" "$WS/s3"
  cat > "$WS/s3/start_s3.sh" <<EOF
#!/bin/bash
# Starts the local S3-compatible server on 127.0.0.1:9000 and creates the
# test bucket. Written by setup_test_environment.sh.
set -u
S3="$WS/s3"
export AWS_ACCESS_KEY_ID="$S3_ACCESS_KEY"
export AWS_SECRET_ACCESS_KEY="$S3_SECRET_KEY"
mkdir -p "\$S3/data"
if ! curl -s -o /dev/null http://127.0.0.1:9000/; then
  nohup "\$S3/bin/versitygw" --access "\$AWS_ACCESS_KEY_ID" --secret "\$AWS_SECRET_ACCESS_KEY" \\
    --port 127.0.0.1:9000 posix "\$S3/data" > "\$S3/versitygw.log" 2>&1 < /dev/null &
  for i in \$(seq 1 30); do curl -s -o /dev/null http://127.0.0.1:9000/ && break; sleep 0.5; done
fi
"\$S3/bin/aws" --endpoint-url http://127.0.0.1:9000 --region us-east-1 s3 mb "s3://$S3_BUCKET" 2>&1 \\
  | grep -v BucketAlreadyOwnedByYou
EOF
  cat > "$WS/qa/start_toxiproxy.sh" <<EOF
#!/bin/bash
# Starts toxiproxy-server on 127.0.0.1:8474 unless it is already running.
# Written by setup_test_environment.sh.
curl -s -o /dev/null http://127.0.0.1:8474/version && exit 0
nohup "$WS/qa/bin/toxiproxy-server" -host 127.0.0.1 -port 8474 \\
  > "$WS/qa/toxiproxy.log" 2>&1 < /dev/null &
for i in \$(seq 1 20); do
  curl -s -o /dev/null http://127.0.0.1:8474/version && exit 0
  sleep 0.25
done
echo "toxiproxy did not start"; exit 1
EOF
  chmod +x "$WS/s3/start_s3.sh" "$WS/qa/start_toxiproxy.sh"
  log "wrote $WS/s3/start_s3.sh and $WS/qa/start_toxiproxy.sh"
}

step_nightly() {
  [ -d "$SRC" ] || fail "repository missing; run the 'repo' step first"
  local src_dir="$SRC/extra/qa/nightly"
  local dst_dir="$WS/nightly"
  [ -d "$src_dir" ] || fail "nightly files missing at $src_dir"
  mkdir -p "$dst_dir/hegel-db" "$dst_dir/runs"
  # install the nightly runner and its data files
  install -m 0755 "$src_dir/run_nightly.sh" "$dst_dir/run_nightly.sh"
  install -m 0644 "$src_dir/known_failures.txt" "$dst_dir/known_failures.txt"
  install -m 0644 "$src_dir/build_report_data.py" "$dst_dir/build_report_data.py"
  # hegel.toml needs absolute database paths, filled in from this workspace
  sed "s#@NIGHTLY_DIR@#$dst_dir#g" "$src_dir/hegel.toml" > "$dst_dir/hegel.toml"
  log "installed the nightly runner in $dst_dir"

  if [ -z "$NIGHTLY_CRON" ]; then
    log "NIGHTLY_CRON is empty; not installing a cron entry"
    return 0
  fi
  # install (or refresh) the crontab line, leaving every other entry intact
  local line="$NIGHTLY_CRON $dst_dir/run_nightly.sh > $dst_dir/cron.out 2>&1"
  local current
  current="$(crontab -l 2>/dev/null)"
  if printf '%s\n' "$current" | grep -qF "$line"; then
    log "nightly cron entry already installed"
  else
    { printf '%s\n' "$current" | grep -vF "nightly/run_nightly.sh"
      printf '%s\n' "$line"
    } | crontab - || fail "could not install the crontab entry"
    log "installed nightly cron entry: $line"
  fi
}

summary() {
  cat <<EOF

[$(date '+%F %T')] setup complete.

  workspace            $WS
  source               $SRC
  builds               $(for c in $PROJECT_PRESETS; do printf '%s ' "$WS/percona-binlog-server-build-$c/binlog_server"; done)
  MySQL distributions  $WS/dist
  python libraries     $WS/qa/pylib   (export PYTHONPATH=$WS/qa/pylib)
  S3 server            $WS/s3/start_s3.sh        (127.0.0.1:9000, bucket $S3_BUCKET)
  Toxiproxy            $WS/qa/start_toxiproxy.sh (127.0.0.1:8474)
  fsfault library      $WS/qa/fsfault/libfsfault.so
  nightly runner       $WS/nightly/run_nightly.sh  (cron: ${NIGHTLY_CRON:-<disabled>})

Examples:
  # a crash test against MySQL 8.4
  PYTHONPATH=$WS/qa/pylib python3 $SRC/extra/qa/fetch_crash_test.py \\
    --mysql-basedir $WS/dist/mysql-$MYSQL_84-$MYSQL_SUFFIX-minimal \\
    --binsrv $WS/percona-binlog-server-build-asan_gcc14/binlog_server \\
    --mode gtid --workdir $WS/qa/run-crash

  # the MTR suite on MySQL 9.7
  (cd $WS/dist/mysql-$MYSQL_97-$MYSQL_SUFFIX-minimal/mysql-test && \\
    BINSRV=$WS/percona-binlog-server-build-debug_gcc14/binlog_server \\
    ./mtr --suite=binlog_streaming --force --big-test --parallel=\$(nproc))
EOF
}

# --------------------------------------------------------------------------
# driver
# --------------------------------------------------------------------------
ALL_STEPS=(packages repo deps project mtr pylib s3 toxiproxy fsfault helpers nightly)
STEPS=("$@")
[ ${#STEPS[@]} -eq 0 ] && STEPS=("${ALL_STEPS[@]}")

log "workspace: $WS   presets: $PROJECT_PRESETS   jobs: $JOBS"
for step in "${STEPS[@]}"; do
  case "$step" in
    packages|repo|deps|project|mtr|pylib|s3|toxiproxy|fsfault|helpers|nightly)
      log "=== step: $step"
      "step_$step"
      ;;
    *)
      fail "unknown step '$step' (valid: ${ALL_STEPS[*]})"
      ;;
  esac
done

# a full run prints the summary at the end
if [ ${#STEPS[@]} -eq ${#ALL_STEPS[@]} ]; then
  summary
fi
log "ALL DONE"
