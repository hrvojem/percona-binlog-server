#!/bin/bash
# Runs a MySQL 8.4 source, Percona Binlog Server (pull) and a workload client
# under Shadow, each on its own simulated host, then checks PBS's storage
# against the source's binlogs.
#
# usage: shadow_pbs.sh <name> [scenario]
#   scenario: basic (default) - a workload with binlog rotations
#             restart         - the same, with the source shut down (SQL
#                               SHUTDOWN) at 40 s and started again at 55 s
#             idle            - a short workload, then an hour of idle time
#             crash           - ticket 6 (PBS-72): a short workload ending in
#                               FLUSH BINARY LOGS, the source killed with
#                               SIGKILL at KILL_AT s (default 40) before it
#                               writes to the new binlog, started again 5 s
#                               later, then a second workload
#             quiet           - basic without the closing FLUSH BINARY LOGS
#                               the other scenarios end with: the last
#                               transaction is followed by silence (ticket
#                               94, PBS-167: with checkpoint_interval it is
#                               not written until another event arrives)
#             lossy           - basic, with LOSS (default 0.02) packet loss
#                               and LATENCY (default 20 ms) between the
#                               source and the other hosts
# env: SEED (default 1), MODE (position|gtid, default position),
#      STOP (simulated seconds, default by scenario), WALL (default 1500),
#      PBS (binary), LOSS / LATENCY (any scenario), KILL_AT (crash),
#      SHADOW (the patched shadow binary, see README.md), EXP_DIR (where
#      each run gets its directory <name>), MYSQL_BASEDIR
# exit: 0 when Shadow ran, every process ended as expected, PBS's storage
#       matches the source and every recorded timestamp range is inside its
#       binlog; 1 otherwise
set -uo pipefail
NAME=$1 SCENARIO=${2:-basic}
S=${SHADOW_WS:-$HOME/ws/shadow}
SHADOW=${SHADOW:-$S/install/bin/shadow}
EXP=${EXP_DIR:-$S/exp}
B=${MYSQL_BASEDIR:-$HOME/ws/dist/mysql-8.4.8-linux-glibc2.28-x86_64-minimal}
PBS=${PBS:-$HOME/ws/percona-binlog-server-build-release_gcc14/binlog_server}
HERE=$(cd "$(dirname "$0")" && pwd)
MODE=${MODE:-position}
SEED=${SEED:-1}
LOSS=${LOSS:-0}
LATENCY=${LATENCY:-1 ms}
KILL_AT=${KILL_AT:-40}
case $SCENARIO in
  # past the read timeout (60 s) after the last transaction: in GTID mode
  # the closing ROTATE does not make PBS write it (ticket 94), the reconnect
  # does
  basic) STOP=${STOP:-180} ;;
  quiet) STOP=${STOP:-120} ;;
  restart) STOP=${STOP:-180} ;;
  idle) STOP=${STOP:-3700} ;;
  crash) STOP=${STOP:-$((KILL_AT + 60))} ;;
  lossy) STOP=${STOP:-180}; [ "$LOSS" = 0 ] && LOSS=0.02; [ "$LATENCY" = "1 ms" ] && LATENCY="20 ms" ;;
  *) echo "unknown scenario $SCENARIO"; exit 2 ;;
esac
E=$EXP/$NAME
rm -rf "$E"; mkdir -p "$E/storage" "$E/local_settings" "$E/telemetry"
cat > "$E/init.sql" <<'SQL'
CREATE USER 'rpl'@'%' IDENTIFIED BY '';
GRANT ALL ON *.* TO 'rpl'@'%';
SQL
# The data directory is initialized once, natively, and copied for every run,
# so the server UUID (and with it the GTIDs) is the same in every run.
T=$EXP/template-$(basename "$B")
if [ ! -d "$T" ]; then
  "$B/bin/mysqld" --no-defaults --basedir="$B" --datadir="$T" \
    --initialize-insecure --init-file="$E/init.sql" \
    --log-error="$E/init.err" || { echo "init failed"; tail -5 "$E/init.err"; rm -rf "$T"; exit 1; }
fi
cp -a "$T" "$E/data"

# PBS configuration
if [ "$MODE" = gtid ]; then
  REWRITE='"rewrite": {"base_file_name": "binlog", "file_size": "64K"},'
else
  REWRITE=''
fi
cat > "$E/binsrv.json" <<JSON
{
  "instance": {"local_settings_path": "$E/local_settings"},
  "logger": {"level": "debug", "file": "$E/binsrv.log"},
  "connection": {"host": "db", "port": 3306, "user": "rpl", "password": "",
                 "connect_timeout": 20, "read_timeout": 60, "write_timeout": 60},
  "replication": {"server_id": 4242, "idle_time": 1, "verify_checksum": true,
                  $REWRITE
                  "mode": "$MODE"},
  "replication_source": {"port": 65535, "server_id": 142,
                 "server_uuid": "00000000-0000-0000-0000-00000000008e",
                 "read_timeout": 60, "write_timeout": 60,
                 "require_secure_transport": false,
                 "authentication": {"user": "unused", "password": "unused",
                                    "plugin": "caching_sha2_password"}},
  "storage": {"backend": "file", "uri": "file://$E/storage", "checkpoint_interval": "1s"},
  "telemetry": {"path": "$E/telemetry"}
}
JSON

# workload, run by bash on the client host
M="$B/bin/mysql --no-defaults --host=db --port=3306 --user=rpl --ssl-mode=DISABLED"
# the event after the last transaction makes PBS write it (ticket 94)
FINAL_FLUSH="\$M -e 'FLUSH BINARY LOGS'"
case $SCENARIO in
  basic|restart|lossy) ROUNDS=30 ;;
  quiet) ROUNDS=30; FINAL_FLUSH="" ;;
  idle) ROUNDS=5 ;;
  # ends with a rotation and then writes nothing until the source is killed
  crash) ROUNDS=6; FINAL_FLUSH="\$M -e 'FLUSH BINARY LOGS'" ;;
esac
cat > "$E/workload.sh" <<SH
#!/bin/bash
set -e
M="$M"
\$M -e "CREATE DATABASE w; CREATE TABLE w.t (id INT AUTO_INCREMENT PRIMARY KEY, v VARCHAR(200))"
i=0
while [ \$i -lt $ROUNDS ]; do
  i=\$((i + 1))
  # retry while the source is down (restart scenario)
  until \$M -e "INSERT INTO w.t (v) VALUES (REPEAT('a', 150)), (REPEAT('b', 150)), (REPEAT('c', 150))"; do sleep 1; done
  if [ \$((i % 7)) -eq 0 ]; then \$M -e "FLUSH BINARY LOGS" || true; fi
  sleep 2
done
$FINAL_FLUSH
echo "workload done: \$i rounds"
\$M -e "SELECT COUNT(*) FROM w.t; SHOW BINARY LOGS"
SH
chmod +x "$E/workload.sh"
# crash: the writes after the restart
cat > "$E/workload2.sh" <<SH
#!/bin/bash
set -e
M="$M"
for i in 1 2 3; do
  until \$M -e "INSERT INTO w.t (v) VALUES (REPEAT('d', 150))"; do sleep 1; done
  sleep 2
done
\$M -e "FLUSH BINARY LOGS"
echo "workload2 done"
\$M -e "SELECT COUNT(*) FROM w.t; SHOW BINARY LOGS"
SH
chmod +x "$E/workload2.sh"

MYSQLD_ARGS=$(cat <<ARGS
      - "--no-defaults"
      - "--basedir=$B"
      - "--datadir=$E/data"
      - "--port=3306"
      - "--bind-address=0.0.0.0"
      - "--socket="
      - "--mysqlx=OFF"
      - "--log-error=$E/db.err"
      - "--innodb-use-native-aio=OFF"
      - "--server-id=1"
      - "--log-bin=binlog"
      - "--binlog-format=ROW"
      - "--gtid-mode=ON"
      - "--enforce-gtid-consistency=ON"
      - "--max-binlog-size=64K"
ARGS
)
for a in ${MYSQLD_EXTRA:-}; do MYSQLD_ARGS+=$'\n'"      - \"$a\""; done
if [ "$SCENARIO" = restart ]; then
  DB_PROCESSES=$(cat <<YAML
    - path: $B/bin/mysqld
      args:
$MYSQLD_ARGS
      start_time: 1s
      expected_final_state: {exited: 0}
    - path: $B/bin/mysqld
      args:
$MYSQLD_ARGS
      start_time: 55s
      expected_final_state: running
YAML
)
  SHUTDOWN_PROCESS=$(cat <<YAML
    - path: $B/bin/mysql
      args: ["--no-defaults", "--host=db", "--port=3306", "--user=rpl", "--ssl-mode=DISABLED", "-e", "SHUTDOWN"]
      start_time: 40s
      expected_final_state: {exited: 0}
YAML
)
elif [ "$SCENARIO" = crash ]; then
  # SIGKILL ends the process at once, so the second server can start soon;
  # Shadow does not enforce InnoDB's file locks, so it must not overlap
  DB_PROCESSES=$(cat <<YAML
    - path: $B/bin/mysqld
      args:
$MYSQLD_ARGS
      start_time: 1s
      shutdown_time: ${KILL_AT}s
      shutdown_signal: SIGKILL
      expected_final_state: {signaled: SIGKILL}
    - path: $B/bin/mysqld
      args:
$MYSQLD_ARGS
      start_time: $((KILL_AT + 5))s
      expected_final_state: running
YAML
)
  SHUTDOWN_PROCESS=$(cat <<YAML
    - path: /bin/bash
      args: ["$E/workload2.sh"]
      start_time: $((KILL_AT + 10))s
      expected_final_state: {exited: 0}
YAML
)
else
  DB_PROCESSES=$(cat <<YAML
    - path: $B/bin/mysqld
      args:
$MYSQLD_ARGS
      start_time: 1s
      expected_final_state: running
YAML
)
  SHUTDOWN_PROCESS=""
fi

cat > "$E/shadow.yaml" <<YAML
general:
  stop_time: ${STOP}s
  seed: $SEED
  model_unblocked_syscall_latency: ${SYSCALL_LATENCY:-true}
experimental:
  native_preemption_enabled: ${PREEMPT:-true}
  strace_logging_mode: ${STRACE:-off}
network:
  graph:
    type: gml
    inline: |
      graph [
        directed 0
        node [
          id 0
          host_bandwidth_down "1 Gbit"
          host_bandwidth_up "1 Gbit"
        ]
        node [
          id 1
          host_bandwidth_down "1 Gbit"
          host_bandwidth_up "1 Gbit"
        ]
        edge [
          source 0
          target 0
          latency "1 ms"
          packet_loss 0.0
        ]
        edge [
          source 1
          target 1
          latency "1 ms"
          packet_loss 0.0
        ]
        edge [
          source 0
          target 1
          latency "$LATENCY"
          packet_loss $(LC_ALL=C printf %.4f "$LOSS")
        ]
      ]
hosts:
  db:
    network_node_id: 1
    processes:
$DB_PROCESSES
  pbs:
    network_node_id: 0
    processes:
    - path: $PBS
      args: ["pull", "$E/binsrv.json"]
      environment: { AWS_EC2_METADATA_DISABLED: "true" }
      start_time: 10s
      expected_final_state: running
  client:
    network_node_id: 0
    processes:
    - path: /bin/bash
      args: ["$E/workload.sh"]
      start_time: 15s
      expected_final_state: {exited: 0}
$SHUTDOWN_PROCESS
YAML

cd "$E"
started=$(date +%s)
timeout "${WALL:-1500}" "$SHADOW" --data-directory "$E/shadow.data" "$E/shadow.yaml" > "$E/shadow.log" 2>&1
code=$?
echo "== $NAME ($SCENARIO, $MODE, seed $SEED): shadow exit $code after $(( $(date +%s) - started )) s wall time for $STOP s simulated"
echo "-- workload stdout:"; cat "$E"/shadow.data/hosts/client/bash.*.stdout 2>/dev/null | grep -E "done|COUNT|^[0-9]" | tail -6
echo "-- workload stderr (last 5):"; cat "$E"/shadow.data/hosts/client/bash.*.stderr 2>/dev/null | grep -v "password on the command line" | tail -5
echo "-- mysqld errors:"; grep -E "\[ERROR\]" "$E/db.err" | grep -v sigwait | head -5 | cut -c1-200
echo "-- PBS log errors/warnings (first 10):"; grep -iE "\[(error|warning)\]" "$E/binsrv.log" 2>/dev/null | head -10 | cut -c1-220
echo "-- PBS log lines: $(wc -l < "$E/binsrv.log" 2>/dev/null)"
echo "-- shadow: unsupported syscalls / process states:"
grep -E "unsupported syscall|final state|panic" "$E/shadow.log" | sed -E 's/.*\] //' | sort -u | head -15 | cut -c1-200
ok=0; bad=0
if [ "$MODE" = gtid ]; then
  # GTID mode is rewrite mode: PBS's binlogs are its own files, so only the
  # transactions can be compared
  echo "-- storage vs source transactions:"
  python3 "$HERE/check_binlog_timestamps.py" --gtids "$E/data" "$E/storage" | sed 's/^/  /'
  [ "${PIPESTATUS[0]}" = 0 ] || bad=1
else
echo "-- storage vs source binlogs:"
pending=0
for f in "$E"/data/binlog.[0-9]*; do
  n=$(basename "$f")
  if [ ! -e "$E/storage/$n" ]; then echo "  missing in storage: $n"; bad=$((bad + 1)); continue; fi
  # a source binlog without transactions yet (only FORMAT_DESCRIPTION and
  # PREVIOUS_GTIDS): PBS keeps those events back until the first transaction
  # and stores the magic alone, by design (ticket 40 describes it)
  if [ "$(stat -c %s "$E/storage/$n")" = 4 ] &&
      python3 "$HERE/check_binlog_timestamps.py" --header-only "$f"; then
    pending=$((pending + 1)); continue
  fi
  # byte 22 is the format description event's LOG_EVENT_BINLOG_IN_USE_F
  # flag, set in the source's open file and cleared in PBS's copy
  if cmp -s "$f" "$E/storage/$n" || [ "$(cmp -l "$f" "$E/storage/$n" 2>/dev/null | awk '$1 != 22' | wc -l)" = 0 -a \
       "$(stat -c %s "$f")" = "$(stat -c %s "$E/storage/$n")" ]; then ok=$((ok + 1)); else
    echo "  differs: $n (source $(stat -c %s "$f"), storage $(stat -c %s "$E/storage/$n"))"; bad=$((bad + 1)); fi
done
echo "  identical: $ok, without transactions yet (magic only in storage): $pending, missing or different: $bad"
fi
ls "$E/storage" | head -30 | tr '\n' ' '; echo
echo "-- recorded timestamp ranges (ticket 6, PBS-72):"
python3 "$HERE/check_binlog_timestamps.py" "$E/storage" | sed 's/^/  /'
ts_code=${PIPESTATUS[0]}
verdict=PASS
[ "$code" = 0 ] || verdict=FAIL
[ "$bad" = 0 ] || verdict=FAIL
[ "$ts_code" = 0 ] || verdict=FAIL
echo "-- verdict: $verdict (shadow exit $code, storage mismatches $bad, timestamp check $ts_code)"
# what must repeat with the same seed: native preemption makes simulated
# times jitter by microseconds between runs (README.md), so the bytes and
# log times may differ, but not the binlogs, their sizes, the transactions
# PBS received, how each process ended or the verdict
outcome=$( (cd "$E/storage" 2>/dev/null && for f in $(ls | grep -E '^binlog\.[0-9]+$'); do echo "$f $(stat -c %s "$f")"; done)
  echo "transactions $(grep -c end_of_transaction "$E/binsrv.log" 2>/dev/null)"
  grep -E "expected end state" "$E/shadow.log" | sed -E 's/.*Error: //' | sort -u
  echo "$verdict $code $bad $ts_code")
echo "-- outcome hash: $(echo "$outcome" | sha256sum | cut -c1-16)"
echo "-- storage hash:$(cd "$E/storage" && cat $(ls | grep -E '^binlog\.[0-9]+$') 2>/dev/null | sha256sum | cut -c1-16)"
echo "-- PBS log hash, paths removed (determinism check): $(sed "s|$E||g" "$E/binsrv.log" | sha256sum | cut -c1-16)"
echo "-- mysqld error log hash, paths removed: $(sed "s|$E||g" "$E/db.err" | sha256sum | cut -c1-16)"
[ "$verdict" = PASS ]
