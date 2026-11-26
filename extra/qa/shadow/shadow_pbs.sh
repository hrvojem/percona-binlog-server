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
#             slow            - 3 rounds and one 20 MB transaction over a
#                               link of BANDWIDTH (default 2 Mbit) to the
#                               source
#             soak            - SOAK_HOURS (default 6) simulated hours of
#                               inserts every 10 s, with rotations; a native
#                               sampler records pull's memory, descriptors
#                               and threads (samples.txt), which must not
#                               keep growing
#             cron            - 'fetch' every 5 minutes instead of 'pull',
#                               for CRON_HOURS (default 3) of inserts every 5
#                               minutes, the source restarted every hour: a
#                               fetch may fail only while the source is down
# switches for any scenario:
#   WITH_REPLICA=1  a replica of pull (as in 'replica'), judged by its rows
#   REPLICAS=N      N replicas (hosts replica, replica2, ...)
#   REPLICA_KILL_AT=<s>  the first replica killed (SIGKILL) then and started
#                   again 5 s later (it resumes with relay_log_recovery)
#   REPLICA_LOSS / REPLICA_LATENCY  the link between pull and the replicas
#   ENCRYPT=1       an encrypted storage (AES-256-CTR); it cannot be compared
#                   with the source, so a replica checks the data instead;
#                   with pbskill the bytes the kill left unrecorded are
#                   compared with what is written there after the restart
#                   (ticket 32, PBS-104)
#   KILL_BASE=restart  pbskill on the restart scenario's source
#   PBS_KILL_US with any scenario: the kill of pbskill in that scenario
#   PAUSE=<start>-<end>[/down|/up][/old][,...]  pull reaches the source
#                   through a relay (pause_relay.py, host 'relay') that goes
#                   silent during these windows of simulated seconds: the
#                   connection stays open, nothing arrives, no RST; /down or
#                   /up silences one direction only, /old only the
#                   connections opened before the window (half-open: a new
#                   connection works). During the first window the source's
#                   Binlog Dump threads are counted (DUMP_CHECK_AT, default
#                   5 s before its end): more than one is a stale one
#   CUT=<n>         the relay closes pull's first connection to the source
#                   once n bytes went to pull, with CUT_MODE fin (default)
#                   or rst; pull must reconnect and end equal to the source
#   RECORD_STREAM=1 the relay writes what that connection carried to pull
#                   to stream.bin (shadow_test.py places the cuts with it)
#   SOURCE_SSL_MODE connection.ssl.mode of pull (default: PBS's, preferred,
#                   which negotiates TLS with the source)
#   REPLICA_PAUSE=<start>-<end>[,...]  the same between the replicas and
#                   pull's listener (host 'rrelay')
#   PURGE=safe|lost  with PAUSE: during the first window the source runs
#                   FLUSH BINARY LOGS and PURGE BINARY LOGS TO the binlog it
#                   wrote 2 s before the window (safe: pull has everything
#                   before it) or to the newest one (lost: what pull still
#                   needs is gone; it must exit with an error and leave a
#                   valid prefix of the source's binlogs)
#   S3=1            an S3 storage: versitygw (VERSITYGW, posix backend) on
#                   host 's3', its bucket directory is what gets compared
#   S3_LOSS / S3_LATENCY / S3_BANDWIDTH  the link between pull and the S3
#                   server
#   SAMPLE=1        sample pull's native process as in 'soak' (any
#                   scenario); MAX_RSS_MB fails the run above that RSS
#   BIG_ROW_MB=<n>  slow: one row of n MiB (one row event) instead of 20
#                   rows of 1 MiB
#   S3_PAUSE=<start>-<end>[,...]  a relay between pull and the S3 server
#                   (host 's3relay') silent during these windows
#   FAILOVER=<s>    a second source, db2, replicates from db with GTIDs;
#                   at that second a planned switchover: db read-only, db2
#                   caught up and promoted, the relay in front of pull
#                   pointed at db2, db shut down, the workload follows; the
#                   storage must hold db's transactions, then db2's (GTID
#                   mode), or stay a prefix of db's binlogs (position mode)
#   DB2_FLUSHES=N   db2 rotates its binlog N times before replicating, so
#                   it has binlogs with the names of db's (position mode:
#                   pull then asks it for an offset in another file)
#   S3_KILL_AT=<s>  the S3 server killed (SIGKILL) then, started again
#                   S3_DOWN (default 20) seconds later
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
REPLICAS=${REPLICAS:-1}
REPLICA_LOSS=${REPLICA_LOSS:-0}
REPLICA_LATENCY=${REPLICA_LATENCY:-1 ms}
REPLICA_KILL_AT=${REPLICA_KILL_AT:-}
ENCRYPT=${ENCRYPT:-}
KILL_BASE=${KILL_BASE:-basic}
CRON_HOURS=${CRON_HOURS:-3}
BANDWIDTH=${BANDWIDTH:-1 Gbit}
PAUSE=${PAUSE:-}
REPLICA_PAUSE=${REPLICA_PAUSE:-}
PURGE=${PURGE:-}
S3=${S3:-}
S3_LOSS=${S3_LOSS:-0}
S3_LATENCY=${S3_LATENCY:-1 ms}
S3_PAUSE=${S3_PAUSE:-}
S3_KILL_AT=${S3_KILL_AT:-}
S3_DOWN=${S3_DOWN:-20}
S3_BANDWIDTH=${S3_BANDWIDTH:-1 Gbit}
CUT=${CUT:-}
CUT_MODE=${CUT_MODE:-fin}
RECORD_STREAM=${RECORD_STREAM:-}
SAMPLE=${SAMPLE:-}
[ "$SCENARIO" = soak ] && SAMPLE=1
# dynamically linked: Shadow runs no static binaries (README.md)
VERSITYGW=${VERSITYGW:-$(dirname "$SHADOW")/versitygw}
FAILOVER=${FAILOVER:-}
DB2_FLUSHES=${DB2_FLUSHES:-0}
SOAK_HOURS=${SOAK_HOURS:-6}
if [ -n "$PURGE" ] && [ -z "$PAUSE" ]; then echo "PURGE needs PAUSE"; exit 2; fi
if [ -n "$S3" ] && [ ! -x "$VERSITYGW" ]; then
  echo "S3 needs a dynamically linked versitygw at $VERSITYGW (build_shadow.sh)"; exit 2
fi
# the end of the last window of a list of windows
windows_end() { tr ',' '\n' <<< "$1" | awk -F- '$2 + 0 > m {m = $2 + 0} END {printf "%d\n", m + 0.999}'; }
REPLICA=""
case $SCENARIO in
  replica|replica_early|heartbeat) REPLICA=$SCENARIO ;;
esac
# an encrypted storage is checked through a replica
[ -z "$REPLICA" ] && [ -n "${WITH_REPLICA:-}$ENCRYPT" ] && REPLICA=replica
if [ "$SCENARIO" = slow ]; then
  [ "$BANDWIDTH" = "1 Gbit" ] && BANDWIDTH="2 Mbit"
  # the binlog stream fills the link for minutes: the workload's own logins
  # need longer than the default 10 s to finish
  MYSQLD_EXTRA="${MYSQLD_EXTRA:-} --connect-timeout=300"
fi
DB_START=1 WORKLOAD_START=15
case $SCENARIO in
  # the source starts late, so pull (started at 10 s) has nothing, not even a
  # binlog file, when the replica connects
  replica_early) REPLICA_AT=${REPLICA_AT:-12}; DB_START=14; WORKLOAD_START=20 ;;
  *) REPLICA_AT=${REPLICA_AT:-20} ;;
esac
case $SCENARIO in
  pbskill) STOP=${STOP:-190} ;;
  slow) STOP=${STOP:-420} ;;
  cron) STOP=${STOP:-$((CRON_HOURS * 3600 + 600))} ;;
  soak) STOP=${STOP:-$((SOAK_HOURS * 3600 + 300))} ;;
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
  pbskill|replica|replica_early|heartbeat|stop|slow|cron|soak) ;;
  *) echo "unknown scenario $SCENARIO"; exit 2 ;;
esac
# a pause (or the S3 server's absence) delays everything by its length; pull
# then needs up to a read timeout (60 s) and a reconnect to catch up
last_window=$(windows_end "$PAUSE,$REPLICA_PAUSE,$S3_PAUSE,0-${S3_KILL_AT:+$((S3_KILL_AT + S3_DOWN))}")
[ "$last_window" -gt 0 ] && [ "$STOP" -lt $((last_window + 100)) ] && STOP=$((last_window + 100))
if [ "$SCENARIO" = pbskill ] && [ -z "${PBS_KILL_US:-}" ]; then
  echo "pbskill needs PBS_KILL_US"; exit 2
fi
E=$EXP/$NAME
rm -rf "$E"; mkdir -p "$E/storage" "$E/local_settings" "$E/telemetry" "$E/tmp"
cat > "$E/init.sql" <<'SQL'
CREATE USER 'rpl'@'%' IDENTIFIED BY '';
GRANT ALL ON *.* TO 'rpl'@'%';
SQL
# The data directory is initialized once, natively, and copied for every run,
# so the server UUID (and with it the GTIDs) is the same in every run.
T=$EXP/template-$(basename "$B")
# runs side by side share the template: one makes it, under a lock, and
# renames it into place when it is complete
exec 8> "$EXP/.template.lock"
flock 8
if [ ! -d "$T" ]; then
  rm -rf "$T.tmp"
  "$B/bin/mysqld" --no-defaults --basedir="$B" --datadir="$T.tmp" \
    --initialize-insecure --init-file="$E/init.sql" \
    --log-error="$E/init.err" || { echo "init failed"; tail -5 "$E/init.err"; rm -rf "$T.tmp"; exit 1; }
  mv "$T.tmp" "$T"
fi
flock -u 8
exec 8>&-
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
KEYRING_JSON="" ENCRYPTION_JSON=""
if [ -n "$ENCRYPT" ]; then
  # a fixed key: the runs stay comparable
  cat > "$E/keyring.json" <<KEYS
{"version": 1, "keys": [{"id": "shadow", "cipher": "AES-256-CTR",
  "data_hex": "6B1F8A3C5D2E4F60718293A4B5C6D7E8F90A1B2C3D4E5F60718293A4B5C6D7E8"}]}
KEYS
  KEYRING_JSON="\"keyring\": {\"uri\": \"file://$E/keyring.json\"},"
  ENCRYPTION_JSON=", \"encryption\": {\"format\": \"generic\", \"kek_id\": \"shadow\", \"cipher\": \"AES-256-CTR\"}"
fi
# pull reaches the source, and the S3 server, directly or through a relay
SOURCE_HOST=db
[ -n "$PAUSE$FAILOVER$CUT$RECORD_STREAM" ] && SOURCE_HOST=relay
# FAILOVER: the current primary, read by the relay for every new connection
# and by the workload for every statement
echo db > "$E/primary"
# STORE: the directory the checks read, the file storage or the S3 bucket's
# directory on the S3 server's posix backend
if [ -n "$S3" ]; then
  mkdir -p "$E/s3data/pbs" "$E/s3meta"
  S3_HOST=s3
  [ -n "$S3_PAUSE" ] && S3_HOST=s3relay
  STORAGE_URI="http://shadowkey:shadowsecret@$S3_HOST:9000/pbs/vault"
  STORE=$E/s3data/pbs/vault
  # its own buffer directory: runs side by side share the real /tmp
  S3_JSON=", \"fs_buffer_directory\": \"$E/s3buf\""
else
  STORAGE_URI="file://$E/storage"
  STORE=$E/storage
  S3_JSON=""
fi
# pull's connection to the source: TLS by default (preferred: the source
# has auto-generated certificates and nothing checks their dates, which lie
# after Shadow's clock); SOURCE_SSL_MODE=disabled makes the stream readable
# (the cut sets place their cuts in MySQL packets and events)
SSL_JSON=""
[ -n "${SOURCE_SSL_MODE:-}" ] && SSL_JSON=", \"ssl\": {\"mode\": \"$SOURCE_SSL_MODE\"}"
cat > "$E/binsrv.json" <<JSON
{
  "instance": {"local_settings_path": "$E/local_settings"},
  "logger": {"level": "debug", "file": "$E/binsrv.log"},
  "connection": {"host": "$SOURCE_HOST", "port": 3306, "user": "rpl", "password": "",
                 "connect_timeout": 20, "read_timeout": 60, "write_timeout": 60$SSL_JSON},
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
  $KEYRING_JSON
  "storage": {"backend": "$([ -n "$S3" ] && echo s3 || echo file)", "uri": "$STORAGE_URI", "checkpoint_interval": "1s"$S3_JSON$ENCRYPTION_JSON},
  "telemetry": {"path": "$E/telemetry"}
}
JSON

# workload, run by bash on the client host
M="$B/bin/mysql --no-defaults --host=db --port=3306 --user=rpl --ssl-mode=DISABLED"
# the workload's client: with FAILOVER a function that asks the current
# primary
WM=$M PRELUDE=""
if [ -n "$FAILOVER" ]; then
  PRELUDE="mysqlw() { $B/bin/mysql --no-defaults --host=\$(cat $E/primary) --port=3306 --user=rpl --ssl-mode=DISABLED \"\$@\"; }"
  WM=mysqlw
fi
# the event after the last transaction makes PBS write it (ticket 94)
FINAL_FLUSH="\$M -e 'FLUSH BINARY LOGS'"
SLEEP=2 BIG=""
case $SCENARIO in
  # 20 rows of 1 MiB in one transaction, generated by the server, so only
  # the binlog stream crosses the slow link
  slow) ROUNDS=3; BIG='$M -e "CREATE TABLE w.big (id INT AUTO_INCREMENT PRIMARY KEY, v LONGBLOB); INSERT INTO w.big (v) WITH RECURSIVE s(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM s WHERE n < 20) SELECT REPEAT(CHAR(120), 1048576) FROM s"'
    # one row, one row event of BIG_ROW_MB MiB (REPEAT is limited by the
    # source's max_allowed_packet, 64 MiB)
    [ -n "${BIG_ROW_MB:-}" ] && BIG='$M -e "CREATE TABLE w.big (id INT AUTO_INCREMENT PRIMARY KEY, v LONGBLOB); INSERT INTO w.big (v) SELECT REPEAT(CHAR(120), '"$BIG_ROW_MB"' * 1048576)"' ;;
  cron) ROUNDS=$((CRON_HOURS * 12)); SLEEP=300 ;;
  soak) ROUNDS=$((SOAK_HOURS * 360)); SLEEP=10 ;;
  basic|restart|lossy|pbskill|replica|replica_early|heartbeat|stop) ROUNDS=30 ;;
  quiet) ROUNDS=30; FINAL_FLUSH="" ;;
  idle) ROUNDS=5 ;;
  # ends with a rotation and then writes nothing until the source is killed
  crash) ROUNDS=6; FINAL_FLUSH="\$M -e 'FLUSH BINARY LOGS'" ;;
esac
cat > "$E/workload.sh" <<SH
#!/bin/bash
set -e
$PRELUDE
M="$WM"
\$M -e "CREATE DATABASE w; CREATE TABLE w.t (id INT AUTO_INCREMENT PRIMARY KEY, v VARCHAR(200))"
i=0
while [ \$i -lt $ROUNDS ]; do
  i=\$((i + 1))
  # retry while the source is down (restart scenario)
  until \$M -e "INSERT INTO w.t (v) VALUES (REPEAT('a', 150)), (REPEAT('b', 150)), (REPEAT('c', 150))"; do sleep 1; done
  if [ \$((i % 7)) -eq 0 ]; then \$M -e "FLUSH BINARY LOGS" || true; fi
  sleep $SLEEP
done
$BIG
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
# PURGE: started 2 s before the first pause window, purges in its middle.
# The purged binlogs are copied first (the hosts share the real file
# system), so the checks can compare the storage with them.
PAUSE_START=$(cut -d, -f1 <<< "$PAUSE" | cut -d- -f1)
PAUSE_END=$(cut -d, -f1 <<< "$PAUSE" | cut -d- -f2)
if [ -n "$PURGE" ]; then
  cat > "$E/purge.sh" <<SH
#!/bin/bash
set -e
M="$M"
# the binlog the source writes now: pull has everything before it
current=\$(\$M -N -e "SHOW BINARY LOGS" | tail -1 | cut -f1)
sleep $(awk -v s="$PAUSE_START" -v e="$PAUSE_END" 'BEGIN {printf "%d", 2 + (e - s) / 2}')
\$M -e "FLUSH BINARY LOGS"
newest=\$(\$M -N -e "SHOW BINARY LOGS" | tail -1 | cut -f1)
target=\$([ "$PURGE" = safe ] && echo "\$current" || echo "\$newest")
mkdir -p "$E/purged"
for f in \$(\$M -N -e "SHOW BINARY LOGS" | cut -f1); do
  [ "\$f" = "\$target" ] && break
  cp "$E/data/\$f" "$E/purged/"
done
\$M -e "PURGE BINARY LOGS TO '\$target'"
echo "purged to \$target (the source wrote \$current 2 s before the pause): \$(ls "$E/purged" | tr '\\n' ' ')" | tee "$E/purge.txt"
SH
  chmod +x "$E/purge.sh"
  PURGE_PROCESS="    - path: /bin/bash
      args: [\"$E/purge.sh\"]
      start_time: $(awk -v s="$PAUSE_START" 'BEGIN {printf "%d", s - 2}')s
      expected_final_state: {exited: 0}"
else
  PURGE_PROCESS=""
fi
DUMP_PROCESS=""
if [ -n "$PAUSE" ] && [ -z "$FAILOVER" ]; then
  DUMP_CHECK_AT=${DUMP_CHECK_AT:-$(awk -v e="$PAUSE_END" 'BEGIN {printf "%d", e - 5}')}
  cat > "$E/dump_check.sh" <<SH
#!/bin/bash
$M -N -e "SELECT ID, TIME, STATE FROM information_schema.PROCESSLIST WHERE COMMAND LIKE 'Binlog Dump%'" > "$E/dump_threads.txt"
SH
  chmod +x "$E/dump_check.sh"
  DUMP_PROCESS="    - path: /bin/bash
      args: [\"$E/dump_check.sh\"]
      start_time: ${DUMP_CHECK_AT}s
      expected_final_state: {exited: 0}"
fi
# the replicas: configured at REPLICA_AT, checked from CHECK_AT on
RHOSTS="replica"
for i in $(seq 2 "$REPLICAS"); do RHOSTS="$RHOSTS replica$i"; done
RMC="$B/bin/mysql --no-defaults --port=3306 --user=rpl --ssl-mode=DISABLED"
RM="$RMC --host=replica"
if [ "$MODE" = gtid ]; then
  START_OPTIONS="SOURCE_AUTO_POSITION=1"
else
  START_OPTIONS="SOURCE_LOG_FILE='binlog.000001', SOURCE_LOG_POS=4"
fi
# after pull's reconnect at the read timeout (60 s after the last event): in
# GTID mode only that writes the last transaction (ticket 94)
CHECK_AT=${CHECK_AT:-150}
# the replicas reach pull's listener directly or through a relay
LISTENER_HOST=pbs
[ -n "$REPLICA_PAUSE" ] && LISTENER_HOST=rrelay
cat > "$E/replica_setup.sh" <<SH
#!/bin/bash
for h in $RHOSTS; do
  RM="$RMC --host=\$h"
  # the replica's mysqld may still be starting
  until \$RM -e "SELECT 1" >/dev/null 2>&1; do sleep 0.2; done
  \$RM -e "SET PERSIST replica_net_timeout = 10;
    CHANGE REPLICATION SOURCE TO SOURCE_HOST='$LISTENER_HOST', SOURCE_PORT=3307,
      SOURCE_USER='repl', SOURCE_PASSWORD='replpass', GET_SOURCE_PUBLIC_KEY=1,
      SOURCE_SSL=0, SOURCE_CONNECT_RETRY=1, SOURCE_RETRY_COUNT=1000,
      SOURCE_HEARTBEAT_PERIOD=1, $START_OPTIONS;
    START REPLICA;"
  echo "replica \$h started"
done
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
for h in $RHOSTS; do
  RM="$RMC --host=\$h"
  echo "replica_rows \$h \$(\$RM -N -e 'SELECT COUNT(*) FROM w.t' 2>/dev/null || echo none)" >> \$out
  \$RM -e "SHOW REPLICA STATUS\\G" | grep -E " (Replica_IO_Running|Replica_SQL_Running|Last_IO_Errno|Last_IO_Error|Last_SQL_Errno|Last_SQL_Error):" | sed "s/^ */\$h /" >> \$out
done
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
      - "--tmpdir=$E/tmp"
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
if [ "$SCENARIO" = restart ] || { [ "$SCENARIO" = pbskill ] && [ "$KILL_BASE" = restart ]; }; then
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
elif [ "$SCENARIO" = cron ]; then
  # a clean restart at every full hour
  DB_PROCESSES="" SHUTDOWN_PROCESS=""
  start=1
  for h in $(seq 1 $((CRON_HOURS - 1))); do
    DB_PROCESSES+="    - path: $B/bin/mysqld
      args:
$MYSQLD_ARGS
      start_time: ${start}s
      expected_final_state: {exited: 0}
"
    SHUTDOWN_PROCESS+="    - path: $B/bin/mysql
      args: [\"--no-defaults\", \"--host=db\", \"--port=3306\", \"--user=rpl\", \"--ssl-mode=DISABLED\", \"-e\", \"SHUTDOWN\"]
      start_time: $((h * 3600))s
      expected_final_state: {exited: 0}
"
    start=$((h * 3600 + 15))
  done
  DB_PROCESSES+="    - path: $B/bin/mysqld
      args:
$MYSQLD_ARGS
      start_time: ${start}s
      expected_final_state: running"
else
  DB_PROCESSES=$(cat <<YAML
    - path: $B/bin/mysqld
      args:
$MYSQLD_ARGS
      start_time: ${DB_START}s
      expected_final_state: $([ -n "$FAILOVER" ] && echo "{exited: 0}" || echo running)
YAML
)
  SHUTDOWN_PROCESS=""
fi

PBS_PROCESS="    - path: $PBS
      args: [\"pull\", \"$E/binsrv.json\"]
      environment: { AWS_EC2_METADATA_DISABLED: \"true\" }"
if [ "$PURGE" = lost ] || { [ -n "$FAILOVER" ] && [ "$MODE" = position ]; }; then
  # pull must (PURGE) or may (a failover in position mode) exit: a wrapper
  # records how and when; after a failover it stays, so either way ends in
  # the same final state
  cat > "$E/pull_wrap.sh" <<SH
#!/bin/bash
export AWS_EC2_METADATA_DISABLED=true
"$PBS" pull "$E/binsrv.json"
rc=\$?
echo "pull exit \$rc at \$(( \$(date +%s) - 946684800 )) s" > "$E/pull_exit.txt"
$([ -n "$FAILOVER" ] && echo "exec sleep 1000000")
SH
  PBS_PROCESSES="    - path: /bin/bash
      args: [\"$E/pull_wrap.sh\"]
      start_time: 10s
      expected_final_state: $([ -n "$FAILOVER" ] && echo running || echo "{exited: 0}")"
elif [ -n "${PBS_KILL_US:-}" ]; then
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
cd "$STORE" || exit 0
cp -a "$STORE" "$E/kill_copy"
for f in \$(ls | sort); do echo "\$f \$(stat -c %s "\$f")"; done > "$E/kill_snapshot.txt"
echo "index: \$(tr '\\n' ' ' < binlog.index 2>/dev/null)" >> "$E/kill_snapshot.txt"
SH
elif [ "$SCENARIO" = cron ]; then
  # simulated time starts at 2000-01-01 00:00:00 UTC (946684800)
  cat > "$E/cron_fetch.sh" <<SH
#!/bin/bash
export AWS_EC2_METADATA_DISABLED=true
i=0
while :; do
  i=\$((i + 1))
  t=\$(( \$(date +%s) - 946684800 ))
  "$PBS" fetch "$E/binsrv.json" > /dev/null 2>&1
  echo "fetch \$i at \$t s exit \$?" >> "$E/cron.txt"
  [ \$t -ge $((CRON_HOURS * 3600 + 120)) ] && break
  sleep 300
done
SH
  PBS_PROCESSES="    - path: /bin/bash
      args: [\"$E/cron_fetch.sh\"]
      start_time: 10s
      expected_final_state: {exited: 0}"
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
  n=0
  for h in $RHOSTS; do
    n=$((n + 1))
    suffix=$([ "$n" = 1 ] && echo "" || echo "$n")
    cp -a "$T" "$E/rdata$suffix"
    # a server UUID of its own, fixed: one generated inside the simulation
    # comes out the same in every run (same simulated clock and seed), and
    # names the replica's temporary files
    printf '[auto]\nserver-uuid=00000000-0000-0000-0000-0000000000%02d\n' "$((n + 1))" \
      > "$E/rdata$suffix/auto.cnf"
    mkdir -p "$E/rtmp$suffix"
    # relay_log_recovery: a replica killed with SIGKILL resumes safely; its
    # own tmpdir: runs side by side share the real /tmp
    rargs="\"--no-defaults\", \"--basedir=$B\", \"--datadir=$E/rdata$suffix\",
             \"--tmpdir=$E/rtmp$suffix\",
             \"--port=3306\", \"--bind-address=0.0.0.0\", \"--socket=\",
             \"--mysqlx=OFF\", \"--log-error=$E/replica$suffix.err\",
             \"--innodb-use-native-aio=OFF\", \"--server-id=$((n + 1))\",
             \"--skip-log-bin\", \"--gtid-mode=$SOURCE_GTID\",
             \"--enforce-gtid-consistency=$SOURCE_GTID\", \"--relay-log-recovery=ON\""
    if [ "$n" = 1 ] && [ -n "$REPLICA_KILL_AT" ]; then
      procs="    - path: $B/bin/mysqld
      args: [$rargs, \"--skip-replica-start\"]
      start_time: 1s
      shutdown_time: ${REPLICA_KILL_AT}s
      shutdown_signal: SIGKILL
      expected_final_state: {signaled: SIGKILL}
    - path: $B/bin/mysqld
      args: [$rargs]
      start_time: $((REPLICA_KILL_AT + 5))s
      expected_final_state: running"
    else
      procs="    - path: $B/bin/mysqld
      args: [$rargs, \"--skip-replica-start\"]
      start_time: 1s
      expected_final_state: running"
    fi
    REPLICA_HOST+="  $h:
    network_node_id: 2
    processes:
$procs
"
  done
  REPLICA_CLIENTS="    - path: /bin/bash
      args: [\"$E/replica_setup.sh\"]
      start_time: ${REPLICA_AT}s
      expected_final_state: {exited: 0}
    - path: /bin/bash
      args: [\"$E/replica_check.sh\"]
      start_time: ${CHECK_AT}s
      expected_final_state: {exited: 0}"
fi
# relays (pause_relay.py) and the S3 server, each on a host of its own
EXTRA_HOSTS=""
FAILOVER_PROCESSES=""
if [ -n "$FAILOVER" ]; then
  # db2: a copy of the template with a server UUID of its own, a GTID
  # replica of db that writes its own binlog (log_replica_updates)
  cp -a "$T" "$E/rdata_db2"
  printf '[auto]\nserver-uuid=00000000-0000-0000-0000-0000000000d2\n' > "$E/rdata_db2/auto.cnf"
  mkdir -p "$E/rtmp_db2"
  DB2_ARGS=$(sed -e "s|--datadir=$E/data|--datadir=$E/rdata_db2|" -e "s|--log-error=$E/db.err|--log-error=$E/db2.err|" \
    -e "s|--tmpdir=$E/tmp|--tmpdir=$E/rtmp_db2|" -e "s|--server-id=1\"|--server-id=11\"|" \
    -e "s|--gtid-mode=OFF|--gtid-mode=ON|" -e "s|--enforce-gtid-consistency=OFF|--enforce-gtid-consistency=ON|" <<< "$MYSQLD_ARGS")
  EXTRA_HOSTS+="  db2:
    network_node_id: 1
    processes:
    - path: $B/bin/mysqld
      args:
$DB2_ARGS
      start_time: 1s
      expected_final_state: running
"
  MC="$B/bin/mysql --no-defaults --port=3306 --user=rpl --ssl-mode=DISABLED"
  cat > "$E/db2_setup.sh" <<SH
#!/bin/bash
until $MC --host=db2 -e "SELECT 1" >/dev/null 2>&1; do sleep 0.2; done
for i in \$(seq 1 $DB2_FLUSHES); do $MC --host=db2 -e "FLUSH BINARY LOGS"; done
$MC --host=db2 -e "CHANGE REPLICATION SOURCE TO SOURCE_HOST='db', SOURCE_PORT=3306,
    SOURCE_USER='rpl', SOURCE_PASSWORD='', SOURCE_SSL=0, GET_SOURCE_PUBLIC_KEY=1,
    SOURCE_AUTO_POSITION=1, SOURCE_CONNECT_RETRY=1; START REPLICA"
echo "db2 replicating from db"
SH
  # a planned switchover, as an operator or an orchestrator does it
  cat > "$E/failover.sh" <<SH
#!/bin/bash
set -e
t() { echo \$(( \$(date +%s) - 946684800 )); }
$MC --host=db -e "SET GLOBAL super_read_only = ON"
executed=\$($MC --host=db -N -e "SELECT REPLACE(@@GLOBAL.gtid_executed, '\\n', '')")
caught=\$($MC --host=db2 -N -e "SELECT WAIT_FOR_EXECUTED_GTID_SET('\$executed', 60)")
$MC --host=db2 -e "STOP REPLICA; RESET REPLICA ALL"
echo db2 > "$E/primary"
$MC --host=db -e "SHUTDOWN"
echo "switchover at \$(t) s: db read-only at $FAILOVER s, db2 caught up (\$caught) with \$executed" | tee "$E/failover.txt"
SH
  chmod +x "$E/db2_setup.sh" "$E/failover.sh"
  FAILOVER_PROCESSES="    - path: /bin/bash
      args: [\"$E/db2_setup.sh\"]
      start_time: 5s
      expected_final_state: {exited: 0}
    - path: /bin/bash
      args: [\"$E/failover.sh\"]
      start_time: ${FAILOVER}s
      expected_final_state: {exited: 0}"
fi
relay_host() {  # <host> <network node> <listen port> <server> <port> <windows> [<environment>]
  EXTRA_HOSTS+="  $1:
    network_node_id: $2
    processes:
    - path: /usr/bin/python3
      args: [\"$HERE/pause_relay.py\", \"$3\", \"$4\", \"$5\", \"$6\"]
      environment: { ${7:-} }
      start_time: 1s
      expected_final_state: running
"
}
relay_env=""
[ -n "$CUT" ] && relay_env="RELAY_CUT: \"$CUT\", RELAY_CUT_MODE: \"$CUT_MODE\""
[ -n "$RECORD_STREAM" ] && relay_env="${relay_env:+$relay_env, }RELAY_RECORD: \"$E/stream.bin\""
[ -n "$PAUSE$FAILOVER$CUT$RECORD_STREAM" ] && relay_host relay 0 3306 "$([ -n "$FAILOVER" ] && echo "@$E/primary" || echo db)" 3306 "$PAUSE" "$relay_env"
[ -n "$REPLICA_PAUSE" ] && relay_host rrelay 2 3307 pbs 3307 "$REPLICA_PAUSE"
[ -n "$S3_PAUSE" ] && relay_host s3relay 0 9000 s3 9000 "$S3_PAUSE"
if [ -n "$S3" ]; then
  # --disableotmp: an object is published with linkat() through
  # /proc/self/fd otherwise, which under Shadow names the native process's
  # descriptors, not the simulated ones (ENOENT, retried forever); the
  # sidecar directory keeps the metadata out of extended attributes
  s3_process="    - path: $VERSITYGW
      args: [\"--access\", \"shadowkey\", \"--secret\", \"shadowsecret\",
             \"--port\", \"0.0.0.0:9000\", \"posix\", \"--disableotmp\",
             \"--sidecar\", \"$E/s3meta\", \"$E/s3data\"]"
  if [ -n "$S3_KILL_AT" ]; then
    S3_PROCESSES="$s3_process
      start_time: 1s
      shutdown_time: ${S3_KILL_AT}s
      shutdown_signal: SIGKILL
      expected_final_state: {signaled: SIGKILL}
$s3_process
      start_time: $((S3_KILL_AT + S3_DOWN))s
      expected_final_state: running"
  else
    S3_PROCESSES="$s3_process
      start_time: 1s
      expected_final_state: running"
  fi
  EXTRA_HOSTS+="  s3:
    network_node_id: 3
    processes:
$S3_PROCESSES
"
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
          host_bandwidth_down "$BANDWIDTH"
          host_bandwidth_up "$BANDWIDTH"
        ]
        node [
          id 2
          host_bandwidth_down "1 Gbit"
          host_bandwidth_up "1 Gbit"
        ]
        edge [
          source 2
          target 2
          latency "1 ms"
          packet_loss 0.0
        ]
        edge [
          source 0
          target 2
          latency "$REPLICA_LATENCY"
          packet_loss $(LC_ALL=C printf %.4f "$REPLICA_LOSS")
        ]
        edge [
          source 1
          target 2
          latency "1 ms"
          packet_loss 0.0
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
        node [
          id 3
          host_bandwidth_down "$S3_BANDWIDTH"
          host_bandwidth_up "$S3_BANDWIDTH"
        ]
        edge [
          source 3
          target 3
          latency "1 ms"
          packet_loss 0.0
        ]
        edge [
          source 0
          target 3
          latency "$S3_LATENCY"
          packet_loss $(LC_ALL=C printf %.4f "$S3_LOSS")
        ]
        edge [
          source 1
          target 3
          latency "1 ms"
          packet_loss 0.0
        ]
        edge [
          source 2
          target 3
          latency "1 ms"
          packet_loss 0.0
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
$EXTRA_HOSTS
  client:
    network_node_id: 0
    processes:
    - path: /bin/bash
      args: ["$E/workload.sh"]
      start_time: ${WORKLOAD_START}s
      expected_final_state: {exited: 0}
$SHUTDOWN_PROCESS
$REPLICA_CLIENTS
$PURGE_PROCESS
$FAILOVER_PROCESSES
$DUMP_PROCESS
YAML

cd "$E"
started=$(date +%s)
if [ -n "$SAMPLE" ]; then
  # Shadow's /proc shows native processes, not simulated ones, so pull is
  # sampled from outside: the RSS and threads of its native process, with the
  # simulated time of its last log line. Shadow opens regular files on its
  # behalf and emulates sockets, so the files pull holds open are counted
  # among the shadow process's descriptors (those in the storage, the S3
  # buffer directory and the local settings)
  (
    while sleep "${SAMPLE_EVERY:-5}"; do
      pid=$(pgrep -f "pull $E/binsrv.json" | head -1)
      # the shadow process itself, not the 'timeout' in front of it
      spid=$(pgrep -f "^$SHADOW --data-directory $E/shadow.data" | head -1)
      [ -n "$pid" ] && [ -r "/proc/$pid/status" ] || continue
      stamp=$(tail -1 "$E/binsrv.log" 2>/dev/null | sed -nE 's/^\[2000-01-(0[1-9])T([0-9:]{8}).*/\1 \2/p')
      [ -n "$stamp" ] || continue
      files=$(ls -l "/proc/$spid/fd" 2>/dev/null | grep -cE "$E/(storage|s3buf|local_settings)")
      echo "$stamp $(awk '/^VmRSS/ {r = $2} /^Threads/ {t = $2} END {print r, t}' "/proc/$pid/status") $files"
    done > "$E/samples.txt"
  ) &
  SAMPLER=$!
fi
timeout "${WALL:-1500}" "$SHADOW" --data-directory "$E/shadow.data" "$E/shadow.yaml" > "$E/shadow.log" 2>&1
code=$?
[ -n "${SAMPLER:-}" ] && kill "$SAMPLER" 2>/dev/null
echo "== $NAME ($SCENARIO, $MODE, seed $SEED): shadow exit $code after $(( $(date +%s) - started )) s wall time for $STOP s simulated"
echo "-- workload stdout:"; cat "$E"/shadow.data/hosts/client/bash.*.stdout 2>/dev/null | grep -E "done|COUNT|^[0-9]" | tail -6
echo "-- workload stderr (last 5):"; cat "$E"/shadow.data/hosts/client/bash.*.stderr 2>/dev/null | grep -v "password on the command line" | tail -5
echo "-- mysqld errors:"; grep -E "\[ERROR\]" "$E/db.err" | grep -v sigwait | head -5 | cut -c1-200
echo "-- PBS log errors/warnings (first 10):"; grep -iE "\[(error|warning)\]" "$E/binsrv.log" 2>/dev/null | head -10 | cut -c1-220
echo "-- PBS log lines: $(wc -l < "$E/binsrv.log" 2>/dev/null)"
echo "-- shadow: unsupported syscalls / process states:"
grep -E "unsupported syscall|final state|panic" "$E/shadow.log" | sed -E 's/.*\] //' | sort -u | head -15 | cut -c1-200
ok=0; bad=0
# the purged binlogs, copied before the purge, count as the source's
if [ -n "$PURGE" ]; then
  echo "-- purge: $(cat "$E/purge.txt" 2>/dev/null || echo "no purge.txt")"
  cp "$E"/purged/binlog.[0-9]* "$E/data/" 2>/dev/null
fi
if [ -n "$FAILOVER" ]; then
  echo "-- failover: $(cat "$E/failover.txt" 2>/dev/null || echo "no failover.txt")"
  if [ "$MODE" = gtid ]; then
    # db's transactions, then the ones db2 wrote after its promotion
    echo "-- storage vs db, then db2 (transactions):"
    python3 "$HERE/check_binlog_timestamps.py" --failover "$E/data" "$E/rdata_db2" "$STORE" | sed 's/^/  /'
  else
    # db2's binlogs are other files under the same names: whatever pull did
    # after the switch, what it stored must still be a prefix of db's
    echo "-- storage vs db, a prefix of it (db2 writes other binlogs under the same names):"
    python3 "$HERE/check_binlog_timestamps.py" --prefix "$E/data" "$STORE" | sed 's/^/  /'
  fi
  [ "${PIPESTATUS[0]}" = 0 ] || bad=1
  [ -n "$FAILOVER" ] && [ "$MODE" = position ] &&
    echo "-- pull: $(cat "$E/pull_exit.txt" 2>/dev/null || echo "still running at the end")"
  echo "-- pull after the switchover: $(grep -E 'established connection|error\]' "$E/binsrv.log" | tail -3 | cut -c1-200 | tr '\n' '|')"
elif [ -n "$ENCRYPT" ]; then
  echo "-- storage vs source: encrypted, checked through the replica instead"
elif [ "$PURGE" = lost ] || { [ "$SCENARIO" = stop ] && [ -n "$PAUSE" ]; }; then
  # pull stopped where the purge left it, or was stopped during the pause:
  # what it stored must be the start of the source's binlogs, the last one
  # possibly cut short
  echo "-- storage vs source, a prefix of it:"
  python3 "$HERE/check_binlog_timestamps.py" --prefix "$E/data" "$STORE" \
    $([ "$MODE" = gtid ] && echo --gtid) | sed 's/^/  /'
  [ "${PIPESTATUS[0]}" = 0 ] || bad=1
elif [ "$MODE" = gtid ]; then
  # GTID mode is rewrite mode: PBS's binlogs are its own files, so only the
  # transactions can be compared
  echo "-- storage vs source transactions:"
  python3 "$HERE/check_binlog_timestamps.py" --gtids "$E/data" "$STORE" | sed 's/^/  /'
  [ "${PIPESTATUS[0]}" = 0 ] || bad=1
else
echo "-- storage vs source binlogs:"
pending=0
for f in "$E"/data/binlog.[0-9]*; do
  n=$(basename "$f")
  if [ ! -e "$STORE/$n" ]; then echo "  missing in storage: $n"; bad=$((bad + 1)); continue; fi
  # a source binlog without transactions yet (only FORMAT_DESCRIPTION and
  # PREVIOUS_GTIDS): PBS keeps those events back until the first transaction
  # and stores the magic alone, by design (ticket 40 describes it)
  if [ "$(stat -c %s "$STORE/$n")" = 4 ] &&
      python3 "$HERE/check_binlog_timestamps.py" --header-only "$f"; then
    pending=$((pending + 1)); continue
  fi
  # byte 22 is the format description event's LOG_EVENT_BINLOG_IN_USE_F
  # flag, set in the source's open file and cleared in PBS's copy
  if cmp -s "$f" "$STORE/$n" || [ "$(cmp -l "$f" "$STORE/$n" 2>/dev/null | awk '$1 != 22' | wc -l)" = 0 -a \
       "$(stat -c %s "$f")" = "$(stat -c %s "$STORE/$n")" ]; then ok=$((ok + 1)); else
    echo "  differs: $n (source $(stat -c %s "$f"), storage $(stat -c %s "$STORE/$n"))"; bad=$((bad + 1)); fi
done
echo "  identical: $ok, without transactions yet (magic only in storage): $pending, missing or different: $bad"
fi
ls "$STORE" | head -30 | tr '\n' ' '; echo
ts_code=0
if [ -z "$ENCRYPT" ]; then
  echo "-- recorded timestamp ranges (ticket 6, PBS-72):"
  python3 "$HERE/check_binlog_timestamps.py" "$STORE" | sed 's/^/  /'
  ts_code=${PIPESTATUS[0]}
fi
verdict=PASS
[ "$code" = 0 ] || verdict=FAIL
[ "$bad" = 0 ] || verdict=FAIL
[ "$ts_code" = 0 ] || verdict=FAIL
extra=""
if [ -n "$REPLICA" ]; then
  echo "-- replica:"
  sed 's/^/  /' "$E/replica_check.txt" 2>/dev/null || echo "  no replica_check.txt"
  r() { awk -v k="$1" '$1 == k {print $2}' "$E/replica_check.txt" 2>/dev/null; }
  # replica_rows <host> <rows>; <host> <field>: <value>
  rows() { awk -v h="$1" '$1 == "replica_rows" && $2 == h {print $3}' "$E/replica_check.txt" 2>/dev/null; }
  status() { grep -E "^$1 $2:" "$E/replica_check.txt" 2>/dev/null | sed -E "s/^$1 $2: ?//"; }
  if [ "$REPLICA" = heartbeat ]; then
    # 20 s at a heartbeat period of 1 s; the count restarts at a reconnect
    read -r h1 h2 < <(awk '$1 == "heartbeats" {print $2, $3}' "$E/replica_check.txt" 2>/dev/null)
    hb=$(( ${h2:-0} - ${h1:-0} ))
    replica_ok=$([ "$hb" -ge 10 ] && echo ok || echo FAIL)
    extra=", heartbeats in 20 quiet seconds $hb: $replica_ok"
  else
    replica_ok=ok
    all_rows=""
    for h in $RHOSTS; do
      [ -n "$(r source_rows)" ] && [ "$(r source_rows)" = "$(rows "$h")" ] || replica_ok=FAIL
      [ "$(status "$h" Replica_IO_Running)" = Yes ] && [ "$(status "$h" Replica_SQL_Running)" = Yes ] || replica_ok=FAIL
      [ "$(status "$h" Last_IO_Errno)" = 0 ] && [ "$(status "$h" Last_SQL_Errno)" = 0 ] || replica_ok=FAIL
      all_rows="$all_rows${all_rows:+/}$(rows "$h")"
    done
    extra=", replica rows $all_rows of $(r source_rows): $replica_ok"
  fi
  [ "$replica_ok" = ok ] || verdict=FAIL
fi
if [ -n "${PBS_KILL_US:-}" ]; then
  echo "-- storage left by the kill at $PBS_KILL_US us:"
  sed 's/^/  /' "$E/kill_snapshot.txt" 2>/dev/null
  echo "-- kill snapshot hash: $(sha256sum < "$E/kill_snapshot.txt" 2>/dev/null | cut -c1-16)"
  echo "-- bytes left unrecorded by the kill, written again (ticket 32, PBS-104):"
  python3 "$HERE/check_binlog_timestamps.py" --rewritten "$E/kill_copy" "$STORE" \
    ${ENCRYPT:+--encrypted} | sed 's/^/  /'
  if [ "${PIPESTATUS[0]}" != 0 ]; then
    verdict=FAIL; extra="$extra, keystream reused: FAIL"
  fi
fi
if [ "$SCENARIO" = cron ]; then
  # a fetch may fail only while the source restarts (within 60 s of a full
  # hour); the last one must succeed
  echo "-- fetch runs:"
  failed_ok=0 failed_bad=0
  while read -r _ i _ t _ _ rc; do
    [ "$rc" = 0 ] && continue
    if [ $((t % 3600)) -lt 60 ] && [ "$t" -ge 3600 ]; then failed_ok=$((failed_ok + 1)); else
      failed_bad=$((failed_bad + 1)); echo "  fetch $i at $t s exit $rc, not during a restart"; fi
  done < "$E/cron.txt"
  runs=$(wc -l < "$E/cron.txt" 2>/dev/null)
  last_rc=$(tail -1 "$E/cron.txt" 2>/dev/null | awk '{print $NF}')
  echo "  $runs runs, $failed_ok failed during a restart, $failed_bad failed otherwise, the last exit ${last_rc:-none}"
  cron_ok=$([ "$failed_bad" = 0 ] && [ "$last_rc" = 0 ] && echo ok || echo FAIL)
  extra=", fetch runs $runs, failed outside restarts $failed_bad, last exit ${last_rc:-none}: $cron_ok"
  [ "$cron_ok" = ok ] || verdict=FAIL
fi
if [ "$PURGE" = lost ]; then
  # pull must exit with an error once it finds the source purged what it
  # needs (ER 1236), not retry it forever
  pull_exit=$(cat "$E/pull_exit.txt" 2>/dev/null)
  echo "-- pull after the purge: ${pull_exit:-still running at the end}"
  echo "   its last error: $(grep -iE '\[ *(error|fatal)\]' "$E/binsrv.log" | tail -1 | cut -c1-260)"
  rc=$(awk '{print $3}' <<< "$pull_exit")
  purge_ok=$([ -n "$rc" ] && [ "$rc" != 0 ] && echo ok || echo FAIL)
  extra="$extra, pull exit ${rc:-none} after the purge: $purge_ok"
  [ "$purge_ok" = ok ] || verdict=FAIL
fi
if [ -n "$PAUSE$REPLICA_PAUSE$S3_PAUSE" ]; then
  echo "-- relays:"
  for h in relay rrelay s3relay; do
    [ -d "$E/shadow.data/hosts/$h" ] || continue
    echo "  $h: $(cat "$E"/shadow.data/hosts/$h/*.stdout | grep -c accepted) connections"
    grep -E "pause window|cannot" "$E"/shadow.data/hosts/$h/*.stdout | head -4 | sed 's/^/    /'
  done
  echo "-- pull connections to the source: $(grep -c 'established connection to mysql' "$E/binsrv.log" 2>/dev/null)"
  grep -E " cut after " "$E"/shadow.data/hosts/relay/*.stdout 2>/dev/null | sed 's/^/  /'
fi
if [ -n "$CUT$RECORD_STREAM" ] && [ -z "$PAUSE" ]; then
  echo "-- relay: $(grep -hE "cut after|closed after" "$E"/shadow.data/hosts/relay/*.stdout 2>/dev/null | head -3 | tr '\n' '|')"
  echo "-- pull connections to the source: $(grep -c 'established connection to mysql' "$E/binsrv.log" 2>/dev/null)"
fi
if [ -n "$DUMP_PROCESS" ]; then
  # a connection pull gave up on must not keep a dump thread on the source
  # once pull has a new one (MySQL ends the old dump thread of the same
  # server_id when a new one starts)
  dumps=$(grep -c . "$E/dump_threads.txt" 2>/dev/null)
  echo "-- Binlog Dump threads on the source at ${DUMP_CHECK_AT} s: ${dumps:-no dump_threads.txt}"
  sed 's/^/  /' "$E/dump_threads.txt" 2>/dev/null
  dump_ok=$([ -n "$dumps" ] && [ "$dumps" -le 1 ] && echo ok || echo FAIL)
  extra="$extra, dump threads ${dumps:-none}: $dump_ok"
  [ "$dump_ok" = ok ] || verdict=FAIL
fi
if [ -n "$S3" ]; then
  echo "-- S3 server stderr (last 3):"
  cat "$E"/shadow.data/hosts/s3/*.stderr 2>/dev/null | tail -3 | cut -c1-200 | sed 's/^/  /'
fi
if [ -n "$SAMPLE" ] && [ "$SCENARIO" != soak ]; then
  max_rss=$(awk '$3 > m {m = $3} END {print m + 0}' "$E/samples.txt" 2>/dev/null)
  echo "-- pull's native process: $(grep -c . "$E/samples.txt" 2>/dev/null) samples, RSS at most $max_rss KiB, threads at most $(awk '$4 > m {m = $4} END {print m + 0}' "$E/samples.txt" 2>/dev/null)"
  [ -n "$S3" ] && echo "   S3 buffer directory at the end: $(du -sk "$E/s3buf" 2>/dev/null | cut -f1) KiB; uploads: $(grep -c ' PUT ' "$E"/shadow.data/hosts/s3/*.stdout 2>/dev/null)"
  if [ -n "${MAX_RSS_MB:-}" ]; then
    rss_ok=$([ "$max_rss" -le $((MAX_RSS_MB * 1024)) ] && echo ok || echo FAIL)
    extra="$extra, RSS at most $((max_rss / 1024)) MiB (limit $MAX_RSS_MB): $rss_ok"
    [ "$rss_ok" = ok ] || verdict=FAIL
  fi
fi
if [ "$SCENARIO" = soak ]; then
  # growth from the second simulated hour (after start-up and the first
  # rotations) to the last one
  echo "-- pull's native process, sampled (simulated time, RSS KiB, threads, files open in the storage):"
  soak=$(awk -v hours="$SOAK_HOURS" '
    { split($2, c, ":"); t = ($1 - 1) * 86400 + c[1] * 3600 + c[2] * 60 + c[3]
      n++; if (t >= 3600 && t < 7200) { if ($3 > r0) r0 = $3; if ($5 > f0) f0 = $5; if ($4 > h0) h0 = $4 }
      if (t >= (hours - 1) * 3600) { if ($3 > r1) r1 = $3; if ($5 > f1) f1 = $5; if ($4 > h1) h1 = $4 } }
    END { printf "%d %d %d %d %d %d %d\n", n, r0, r1, f0, f1, h0, h1 }' "$E/samples.txt" 2>/dev/null)
  read -r n r0 r1 f0 f1 h0 h1 <<< "$soak"
  awk 'NR % 20 == 1' "$E/samples.txt" 2>/dev/null | head -40 | sed 's/^/  /'
  echo "  $n samples; hour 2 vs the last hour: RSS $r0 -> $r1 KiB, open storage files $f0 -> $f1, threads $h0 -> $h1"
  [ -n "$S3" ] && echo "  S3 buffer directory at the end: $(du -sk "$E/s3buf" 2>/dev/null | cut -f1) KiB"
  # a leak shows as steady growth; allowances for allocator noise
  soak_ok=$([ "${n:-0}" -ge 10 ] && [ "${r1:-0}" -le $(( ${r0:-0} + ${r0:-0} / 4 + 20480 )) ] &&
    [ "${f1:-0}" -le $(( ${f0:-0} + 5 )) ] && [ "${h1:-0}" -le $(( ${h0:-0} + 2 )) ] && echo ok || echo FAIL)
  extra="$extra, RSS ${r0}->${r1} KiB files ${f0}->${f1} threads ${h0}->${h1}: $soak_ok"
  [ "$soak_ok" = ok ] || verdict=FAIL
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
outcome=$( (cd "$STORE" 2>/dev/null && for f in $(ls | grep -E '^binlog\.[0-9]+$'); do echo "$f $(stat -c %s "$f")"; done)
  echo "transactions $(grep -c end_of_transaction "$E/binsrv.log" 2>/dev/null)"
  grep -E "expected end state" "$E/shadow.log" | sed -E 's/.*Error: //' | sort -u
  echo "$verdict $code $bad $ts_code$extra")
echo "-- outcome hash: $(echo "$outcome" | sha256sum | cut -c1-16)"
echo "-- storage hash: $(cd "$STORE" && cat $(ls | grep -E '^binlog\.[0-9]+$') 2>/dev/null | sha256sum | cut -c1-16)"
echo "-- PBS log hash, paths removed (determinism check): $(sed "s|$E||g" "$E/binsrv.log" | sha256sum | cut -c1-16)"
echo "-- mysqld error log hash, paths removed: $(sed "s|$E||g" "$E/db.err" | sha256sum | cut -c1-16)"
# the data directories of the source and the replicas are most of a run's
# size (about 200 MB each) and no check needs them any more; the storage,
# the logs and Shadow's per-process output stay
[ -n "${KEEP_DATA:-}" ] || rm -rf "$E/data" "$E"/rdata* "$E/tmp" "$E"/rtmp*
echo "-- run directory: $(du -sh "$E" 2>/dev/null | cut -f1)"
[ "$verdict" = PASS ]
