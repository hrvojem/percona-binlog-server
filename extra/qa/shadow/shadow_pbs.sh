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
#             pbskill         - basic, with pull killed (SIGKILL) at
#                               PBS_KILL_US (microseconds of simulated time)
#                               and started again 1 s later: the storage must
#                               still end up equal to the source
#             replica         - basic, with a MySQL replica of pull's
#                               listener on a fourth host, started at
#                               REPLICA_AT (default 20 s): it must end with
#                               the source's rows and no replication error
#             replica_early   - replica, started at REPLICA_AT (default 12
#                               s), after pull started but before it stored
#                               any transaction (tickets 26 and 38, PBS-96
#                               and PBS-111); position mode names the binlog
#             heartbeat       - replica with SOURCE_HEARTBEAT_PERIOD 1 s:
#                               during 20 quiet seconds after the closing
#                               FLUSH BINARY LOGS it must receive heartbeats
#                               (ticket 40, PBS-113)
#             stop            - basic, with SIGTERM to pull at PBS_TERM_AT
#                               (default 100 s) while the source is idle: it
#                               must exit 0 within 5 s (ticket 43, PBS-116)
# env: SEED (default 1), MODE (position|gtid, default position),
#      STOP (simulated seconds, default by scenario), WALL (default 1500),
#      PBS (binary), LOSS / LATENCY (any scenario), KILL_AT (crash),
#      PBS_KILL_US (pbskill), REPLICA_AT (replica scenarios), PBS_TERM_AT
#      (stop),
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
PBS_TERM_AT=${PBS_TERM_AT:-100}
REPLICA=""
case $SCENARIO in
  replica|replica_early|heartbeat) REPLICA=$SCENARIO ;;
esac
DB_START=1 WORKLOAD_START=15
case $SCENARIO in
  # the source starts late, so pull (started at 10 s) has nothing, not even a
  # binlog file, when the replica connects
  replica_early) REPLICA_AT=${REPLICA_AT:-12}; DB_START=14; WORKLOAD_START=20 ;;
  *) REPLICA_AT=${REPLICA_AT:-20} ;;
esac
case $SCENARIO in
  pbskill) STOP=${STOP:-180} ;;
  replica|replica_early|heartbeat) STOP=${STOP:-190} ;;
  stop) STOP=${STOP:-$((PBS_TERM_AT + 70))} ;;
  # past the read timeout (60 s) after the last transaction: in GTID mode
  # the closing ROTATE does not make PBS write it (ticket 94), the reconnect
  # does
  basic) STOP=${STOP:-180} ;;
  quiet) STOP=${STOP:-120} ;;
  restart) STOP=${STOP:-180} ;;
  idle) STOP=${STOP:-3700} ;;
  crash) STOP=${STOP:-$((KILL_AT + 60))} ;;
  lossy) STOP=${STOP:-180}; [ "$LOSS" = 0 ] && LOSS=0.02; [ "$LATENCY" = "1 ms" ] && LATENCY="20 ms" ;;
  pbskill|replica|replica_early|heartbeat|stop) ;;
  *) echo "unknown scenario $SCENARIO"; exit 2 ;;
esac
if [ "$SCENARIO" = pbskill ] && [ -z "${PBS_KILL_US:-}" ]; then
  echo "pbskill needs PBS_KILL_US"; exit 2
fi
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

# PBS configuration; the listener does caching_sha2_password full
# authentication, which without TLS needs its RSA key (the replica asks for
# it with GET_SOURCE_PUBLIC_KEY)
RSA_KEY=${RSA_KEY:-$(cd "$HERE/../../.." && pwd)/mtr/binlog_streaming/std_data/caching_sha2_rsa_priv.pem}
if [ "$MODE" = gtid ]; then
  REWRITE='"rewrite": {"base_file_name": "binlog", "file_size": "8K"},'
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
  "replication_source": {"port": 3307, "server_id": 142,
                 "server_uuid": "00000000-0000-0000-0000-00000000008e",
                 "read_timeout": 60, "write_timeout": 60,
                 "require_secure_transport": false,
                 "auth_rsa_private_key": "$RSA_KEY",
                 "authentication": {"user": "repl", "password": "replpass",
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
  basic|restart|lossy|pbskill|replica|replica_early|heartbeat|stop) ROUNDS=30 ;;
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
# the replica: configured at REPLICA_AT, checked from CHECK_AT on
RM="$B/bin/mysql --no-defaults --host=replica --port=3306 --user=rpl --ssl-mode=DISABLED"
if [ "$MODE" = gtid ]; then
  START_OPTIONS="SOURCE_AUTO_POSITION=1"
else
  START_OPTIONS="SOURCE_LOG_FILE='binlog.000001', SOURCE_LOG_POS=4"
fi
# after pull's reconnect at the read timeout (60 s after the last event): in
# GTID mode only that writes the last transaction (ticket 94)
CHECK_AT=${CHECK_AT:-150}
cat > "$E/replica_setup.sh" <<SH
#!/bin/bash
RM="$RM"
# the replica's mysqld may still be starting
until \$RM -e "SELECT 1" >/dev/null 2>&1; do sleep 0.2; done
\$RM -e "SET GLOBAL replica_net_timeout = 10;
  CHANGE REPLICATION SOURCE TO SOURCE_HOST='pbs', SOURCE_PORT=3307,
    SOURCE_USER='repl', SOURCE_PASSWORD='replpass', GET_SOURCE_PUBLIC_KEY=1,
    SOURCE_SSL=0, SOURCE_CONNECT_RETRY=1, SOURCE_RETRY_COUNT=1000,
    SOURCE_HEARTBEAT_PERIOD=1, $START_OPTIONS;
  START REPLICA;"
echo "replica started"
SH
cat > "$E/replica_check.sh" <<SH
#!/bin/bash
RM="$RM"
M="$M"
out=$E/replica_check.txt
hb() { \$RM -N -e "SELECT COUNT_RECEIVED_HEARTBEATS FROM performance_schema.replication_connection_status"; }
h1=\$(hb); sleep 20; h2=\$(hb)
echo "heartbeats \$h1 \$h2" > \$out
echo "source_rows \$(\$M -N -e 'SELECT COUNT(*) FROM w.t')" >> \$out
echo "replica_rows \$(\$RM -N -e 'SELECT COUNT(*) FROM w.t' 2>/dev/null || echo none)" >> \$out
\$RM -e "SHOW REPLICA STATUS\\G" | grep -E " (Replica_IO_Running|Replica_SQL_Running|Last_IO_Errno|Last_IO_Error|Last_SQL_Errno|Last_SQL_Error):" | sed 's/^ *//' >> \$out
SH
chmod +x "$E/replica_setup.sh" "$E/replica_check.sh"

# a position-mode PBS of a GTID source cannot serve a MySQL replica (ticket
# 30, PBS-102), so the replica scenarios in position mode run without GTIDs
SOURCE_GTID=ON
[ -n "$REPLICA" ] && [ "$MODE" = position ] && SOURCE_GTID=OFF
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
      - "--gtid-mode=$SOURCE_GTID"
      - "--enforce-gtid-consistency=$SOURCE_GTID"
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
      start_time: ${DB_START}s
      expected_final_state: running
YAML
)
  SHUTDOWN_PROCESS=""
fi

PBS_PROCESS="    - path: $PBS
      args: [\"pull\", \"$E/binsrv.json\"]
      environment: { AWS_EC2_METADATA_DISABLED: \"true\" }"
if [ "$SCENARIO" = pbskill ]; then
  PBS_PROCESSES="$PBS_PROCESS
      start_time: 10s
      shutdown_time: $PBS_KILL_US us
      shutdown_signal: SIGKILL
      expected_final_state: {signaled: SIGKILL}
$PBS_PROCESS
      start_time: $((PBS_KILL_US + 1000000)) us
      expected_final_state: running
    - path: /bin/bash
      args: [\"$E/kill_snapshot.sh\"]
      start_time: $((PBS_KILL_US + 500000)) us
      expected_final_state: {exited: 0}"
  # what the killed pull left in the storage, before the restart repairs it
  cat > "$E/kill_snapshot.sh" <<SH
#!/bin/bash
cd "$E/storage" || exit 0
for f in \$(ls | sort); do echo "\$f \$(stat -c %s "\$f")"; done > "$E/kill_snapshot.txt"
echo "index: \$(tr '\\n' ' ' < binlog.index 2>/dev/null)" >> "$E/kill_snapshot.txt"
SH
elif [ "$SCENARIO" = stop ]; then
  PBS_PROCESSES="$PBS_PROCESS
      start_time: 10s
      shutdown_time: ${PBS_TERM_AT}s
      shutdown_signal: SIGTERM
      expected_final_state: {exited: 0}"
else
  PBS_PROCESSES="$PBS_PROCESS
      start_time: 10s
      expected_final_state: running"
fi
REPLICA_HOST=""
REPLICA_CLIENTS=""
if [ -n "$REPLICA" ]; then
  cp -a "$T" "$E/rdata"
  rm -f "$E/rdata/auto.cnf"   # a server UUID of its own
  REPLICA_HOST="  replica:
    network_node_id: 0
    processes:
    - path: $B/bin/mysqld
      args: [\"--no-defaults\", \"--basedir=$B\", \"--datadir=$E/rdata\",
             \"--port=3306\", \"--bind-address=0.0.0.0\", \"--socket=\",
             \"--mysqlx=OFF\", \"--log-error=$E/replica.err\",
             \"--innodb-use-native-aio=OFF\", \"--server-id=2\",
             \"--skip-log-bin\", \"--gtid-mode=$SOURCE_GTID\",
             \"--enforce-gtid-consistency=$SOURCE_GTID\", \"--skip-replica-start\"]
      start_time: 1s
      expected_final_state: running"
  REPLICA_CLIENTS="    - path: /bin/bash
      args: [\"$E/replica_setup.sh\"]
      start_time: ${REPLICA_AT}s
      expected_final_state: {exited: 0}
    - path: /bin/bash
      args: [\"$E/replica_check.sh\"]
      start_time: ${CHECK_AT}s
      expected_final_state: {exited: 0}"
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
$PBS_PROCESSES
$REPLICA_HOST
  client:
    network_node_id: 0
    processes:
    - path: /bin/bash
      args: ["$E/workload.sh"]
      start_time: ${WORKLOAD_START}s
      expected_final_state: {exited: 0}
$SHUTDOWN_PROCESS
$REPLICA_CLIENTS
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
extra=""
if [ -n "$REPLICA" ]; then
  echo "-- replica:"
  sed 's/^/  /' "$E/replica_check.txt" 2>/dev/null || echo "  no replica_check.txt"
  r() { awk -v k="$1" '$1 == k {print $2}' "$E/replica_check.txt" 2>/dev/null; }
  status() { grep -E "^$1:" "$E/replica_check.txt" 2>/dev/null | sed -E "s/^$1: ?//"; }
  if [ "$REPLICA" = heartbeat ]; then
    # 20 s at a heartbeat period of 1 s; the count restarts at a reconnect
    read -r h1 h2 < <(awk '$1 == "heartbeats" {print $2, $3}' "$E/replica_check.txt" 2>/dev/null)
    hb=$(( ${h2:-0} - ${h1:-0} ))
    replica_ok=$([ "$hb" -ge 10 ] && echo ok || echo FAIL)
    extra=", heartbeats in 20 quiet seconds $hb: $replica_ok"
  else
    replica_ok=ok
    [ -n "$(r source_rows)" ] && [ "$(r source_rows)" = "$(r replica_rows)" ] || replica_ok=FAIL
    [ "$(status Replica_IO_Running)" = Yes ] && [ "$(status Replica_SQL_Running)" = Yes ] || replica_ok=FAIL
    [ "$(status Last_IO_Errno)" = 0 ] && [ "$(status Last_SQL_Errno)" = 0 ] || replica_ok=FAIL
    extra=", replica rows $(r replica_rows) of $(r source_rows): $replica_ok"
  fi
  [ "$replica_ok" = ok ] || verdict=FAIL
fi
if [ "$SCENARIO" = pbskill ]; then
  echo "-- storage left by the kill at $PBS_KILL_US us:"
  sed 's/^/  /' "$E/kill_snapshot.txt" 2>/dev/null
  echo "-- kill snapshot hash: $(sha256sum < "$E/kill_snapshot.txt" 2>/dev/null | cut -c1-16)"
fi
if [ "$SCENARIO" = stop ]; then
  # the last line pull logs, against the SIGTERM at PBS_TERM_AT
  last=$(tail -1 "$E/binsrv.log" | sed -E 's/^\[2000-01-01T([0-9:.]+)\].*/\1/')
  delay=$(awk -v t="$last" -v s="$PBS_TERM_AT" 'BEGIN {split(t, a, ":"); printf "%.3f", a[1] * 3600 + a[2] * 60 + a[3] - s}')
  echo "-- stop: SIGTERM at ${PBS_TERM_AT} s, last log line at $last: $(tail -1 "$E/binsrv.log" | cut -c30-150)"
  stop_ok=$(awk -v d="$delay" 'BEGIN {print (d >= 0 && d <= 5) ? "ok" : "FAIL"}')
  extra=", stop delay ${delay} s: $stop_ok"
  [ "$stop_ok" = ok ] || verdict=FAIL
fi
echo "-- verdict: $verdict (shadow exit $code, storage mismatches $bad, timestamp check $ts_code$extra)"
# what must repeat with the same seed: native preemption makes simulated
# times jitter by microseconds between runs (README.md), so the bytes and
# log times may differ, but not the binlogs, their sizes, the transactions
# PBS received, how each process ended or the verdict
outcome=$( (cd "$E/storage" 2>/dev/null && for f in $(ls | grep -E '^binlog\.[0-9]+$'); do echo "$f $(stat -c %s "$f")"; done)
  echo "transactions $(grep -c end_of_transaction "$E/binsrv.log" 2>/dev/null)"
  grep -E "expected end state" "$E/shadow.log" | sed -E 's/.*Error: //' | sort -u
  echo "$verdict $code $bad $ts_code$extra")
echo "-- outcome hash: $(echo "$outcome" | sha256sum | cut -c1-16)"
echo "-- storage hash: $(cd "$E/storage" && cat $(ls | grep -E '^binlog\.[0-9]+$') 2>/dev/null | sha256sum | cut -c1-16)"
echo "-- PBS log hash, paths removed (determinism check): $(sed "s|$E||g" "$E/binsrv.log" | sha256sum | cut -c1-16)"
echo "-- mysqld error log hash, paths removed: $(sed "s|$E||g" "$E/db.err" | sha256sum | cut -c1-16)"
[ "$verdict" = PASS ]
