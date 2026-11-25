# PBS under the Shadow simulator

[Shadow](https://shadow.github.io) runs real Linux programs on simulated
hosts, with simulated time and a simulated network. `shadow_pbs.sh` runs a
MySQL 8.4 source, `binlog_server pull` and a workload client (bash and the
`mysql` client) on three such hosts, then checks PBS's storage against the
source. `../shadow_test.py` runs its scenarios as a QA harness (nightly
rotation, see `../README.md`).

What it gives over the harnesses on real hosts:

* time is simulated: an hour of idle time with 60 read timeouts takes about
  two minutes;
* a kill, a restart or a packet drop happens at a simulated instant, the
  same in every run with the same seed;
* packet loss, latency and bandwidth between hosts come from the network
  graph, no proxy needed; a link that goes silent comes from a relay host
  (`pause_relay.py`).

## Files

* `build_shadow.sh` builds Shadow 3.3.0 with `shadow-mysql.patch` into
  `~/ws/shadow/install` (no root needed).
* `shadow-mysql.patch`: what Shadow 3.3.0 lacks for `mysqld`:
  * `times()` (MySQL's timer calibration loops forever without it);
  * `umask()` passed to the kernel;
  * `rt_sigtimedwait()` (`mysqld`'s signal thread); it polls every 10 ms of
    simulated time, so a signal with a finite timeout is seen only when the
    timeout ends;
  * `shutdown()` of a listening TCP socket wakes the threads polling it, as
    on Linux (Shadow returned an error);
  * `poll()` / `select()` wake when another thread closes a polled file.
  Without the last three, SQL `SHUTDOWN` leaves `mysqld` waiting forever.
* `shadow_pbs.sh <name> <scenario>`: one simulation; prints a report and a
  verdict, exits 0 on PASS. Scenarios: `basic`, `restart`, `crash`, `lossy`,
  `idle`, `quiet`, `pbskill`, `replica`, `replica_early`, `heartbeat`,
  `stop`, `slow`, `cron` (see the script's header), and switches for any
  scenario: `WITH_REPLICA=1`, `REPLICAS=N`, `REPLICA_KILL_AT`,
  `REPLICA_LOSS` / `REPLICA_LATENCY`, `ENCRYPT=1` (an encrypted storage,
  checked through a replica), `KILL_BASE=restart`, `BANDWIDTH` (the
  source's link), `CRON_HOURS`, `KEEP_DATA=1` (keep the MySQL data
  directories, which each run otherwise deletes at the end: they were
  about 200 MB a run). Environment: `SEED`, `MODE`
  (`position` / `gtid`), `PBS`, `SHADOW`, `EXP_DIR`, `MYSQL_BASEDIR`,
  `KILL_AT`, `PBS_KILL_US`, `REPLICA_AT`, `PBS_TERM_AT`, `LOSS`, `LATENCY`,
  `STOP`, `WALL`, `RSA_KEY` (the listener's key, default from
  `mtr/binlog_streaming/std_data`), `STRACE=standard` (Shadow's syscall log
  per process: it shows each file operation of a rotation with its
  simulated time, which is how the kill points were placed).
  Faults between hosts: `PAUSE`, `REPLICA_PAUSE`, `S3_PAUSE` (windows of
  simulated seconds such as `40-160`), `PURGE=safe|lost` (with `PAUSE`),
  `S3=1` with `S3_LOSS` / `S3_LATENCY` and `S3_KILL_AT` / `S3_DOWN`,
  `FAILOVER=<s>` with `DB2_FLUSHES`; the scenario `soak` with
  `SOAK_HOURS`.
* `FAILOVER`: a second source `db2` (its own server UUID, a GTID replica of
  `db` that writes its own binlogs) on the source's network node. At that
  second a client on the `client` host makes a planned switchover: `db`
  `super_read_only`, `WAIT_FOR_EXECUTED_GTID_SET` on `db2`, `RESET REPLICA
  ALL`, then the file `primary` names `db2` and `db` is shut down. The relay
  in front of `pull` (`pause_relay.py @<file>`) and the workload read
  `primary` for every new connection. Checks: `--failover` in GTID mode,
  `--prefix` against `db` in position mode, where `pull` runs under a
  wrapper that records its exit and stays.
* `soak`: inserts every 10 s for `SOAK_HOURS` (about 50 rotations an
  hour). A native loop samples `pull` while Shadow runs, because Shadow's
  `/proc` shows native processes: the RSS and threads of `pull`'s native
  process, and the files of the storage that the `shadow` process holds open
  for it (Shadow opens regular files on a managed process's behalf and
  emulates sockets, so the native process's own descriptors say nothing).
  Two simulated hours take about 7.5 minutes.
* `pause_relay.py`: a TCP relay on a host of its own (`relay` between `pull`
  and the source, `rrelay` between the replicas and the listener, `s3relay`
  between `pull` and the S3 server) that forwards nothing during its pause
  windows and closes nothing: no RST, no FIN, new connections accepted but
  held. Shadow's network graph cannot change during a run, so this is how a
  partition is made.
* `PURGE`: a client on the `client` host notes the source's current binlog
  2 s before the first pause window, then in the middle of it runs `FLUSH
  BINARY LOGS` and `PURGE BINARY LOGS TO` that binlog (`safe`) or to the
  newest one (`lost`), copying the purged files to `purged/` first so the
  checks can compare with them. With `lost`, `pull` runs under a wrapper that
  records its exit code (`pull_exit.txt`), and the storage must be a prefix
  of the source (`check_binlog_timestamps.py --prefix`).
* `S3=1`: versitygw 1.8.0 (posix backend) on the host `s3`. Its bucket
  directory holds the objects as plain files, so the same checks read it.
  Shadow runs only dynamically linked binaries, and the release builds of
  Go programs are static, so `build_shadow.sh` builds versitygw with
  `-linkmode=external` into `install/bin` (`VERSITYGW` overrides it). It
  runs with `--disableotmp`: otherwise it publishes an object with
  `linkat()` through `/proc/self/fd`, which under Shadow names the native
  process's descriptors, and retries the ENOENT forever (the AWS client then
  gives up: "failure to meet throughput minimum").
* A replica is a fourth host running a second `mysqld` (its own server
  UUID); bash clients configure it (`CHANGE REPLICATION SOURCE TO` the
  `pbs` host, `GET_SOURCE_PUBLIC_KEY=1`) and query it at 150 s.
* Shadow kills a process at a syscall boundary, and `pull` makes one about
  every 1–2 us of simulated time, so `PBS_KILL_US` places a kill between
  two file operations; `pbskill` records what the killed process left in
  the storage (`kill_snapshot.txt`) before the restart.
* `check_binlog_timestamps.py`: the timestamp range PBS records for each
  binlog must lie within the binlog's own events (ticket 6); with
  `--gtids`, the GTIDs of the storage must be the source's, in order (GTID
  mode is rewrite mode, whose binlogs are PBS's own files); with
  `--rewritten <copy> <storage> --encrypted`, the bytes a killed `pull`
  left past each recorded size must not be written again differently
  under the same encryption record (ticket 32, AES-CTR keystream reuse);
  with `--prefix <source> <storage> [--gtid]`, the storage must be the start
  of the source's binlogs (or transactions), the last one possibly cut
  short; with `--failover <old> <new> <storage>`, the old source's
  transactions, then the ones the new source wrote after its promotion.
* The Shadow changes are also on the fork `hrvojem/shadow`: branch
  `mysql-support-v3.3.0` (this patch on v3.3.0, one commit) and branch
  `mysql-support` (the same changes on shadow's main as five commits,
  built and checked with MySQL 8.4 on 2026-11-24).
* Simulated hosts share the real file system: every run has its own
  directory, the replicas a fixed server UUID and their own `tmpdir` (a
  UUID generated inside the simulation is the same in every run, and
  concurrent replicas collided on `/tmp/SQL_LOAD-<uuid>`), an S3 storage
  its own `fs_buffer_directory`.

## How `mysqld` runs under Shadow

* `experimental.native_preemption_enabled: true`: InnoDB threads spin
  without making syscalls, which stalls a simulation (Shadow switches threads
  only at syscalls). Turning InnoDB's spin settings off does not remove the
  need (`innodb_sync_spin_loops=0`, `innodb_spin_wait_delay=0`,
  `innodb_log_wait_for_flush_spin_hwm=0`, `innodb_log_writer_threads=OFF`:
  the simulation still stalls).
* `--socket=` (empty): Shadow has no Unix sockets bound to file system
  paths.
* `--innodb-use-native-aio=OFF`: no `io_setup` in Shadow.
* The data directory is initialized once, natively, and copied for every
  run, so the server UUID is the same in every run.
* Shadow does not enforce `fcntl` locks: a restarted `mysqld` must start
  only after the old one has exited, or both use the same data directory.
* The simulated clock starts at 2000-01-01, so certificates are "not yet
  valid": the scenarios run without TLS.

## Determinism

Native preemption is driven by real CPU time, and Shadow charges simulated
time for each syscall and vDSO call. A spinning InnoDB thread therefore makes
a slightly different number of clock calls in each run, and two runs with
the same seed can differ by microseconds of simulated time (seen from
simulated second 21 in a crash run). The outcome of a scenario (which
transactions are stored, which process aborts) was the same in every repeat
so far; byte-identical logs are not guaranteed. Neither of these made runs
exact (2026-11-24): preemption off with InnoDB's spinning turned off
(`mysqld` stalls), and preemption on with
`model_unblocked_syscall_latency: false` (spinning clock calls no longer
advance simulated time, and the run crawls). So `shadow_test.py`'s
`determinism` case compares the outcome hash `shadow_pbs.sh` prints, not
the bytes.
