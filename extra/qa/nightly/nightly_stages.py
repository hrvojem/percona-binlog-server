#!/usr/bin/env python3
"""The MTR, QA-harness, fuzzing and mutation-testing stages of the nightly
run.

run_nightly.sh calls this once per stage step; each call appends one JSON
result line per test case to --results (the same fields the property-test
cases use: config, program, test_case, status, ticket, reason, seconds) and
one human-readable line per case that is not a plain pass to --lines, and
writes a log per failing case into --out.

  mtr  runs the whole binlog_streaming MTR suite on one MySQL distribution and
       records every test (and combination) as a case of the build
       'mtr-<series>' (program 'binlog_streaming', test case '<test>' or
       '<test>/<combination>'). Known failures: mtr_known_failures.txt, lines
       '<series> <test>  ticket N: ...' where <series> is 8.0 / 8.4 / 9.7 or
       '*' for every version, and <test> is a test name (every combination)
       or '<test>/<combination>'.

  qa   runs one black-box harness from extra/qa and records each of its cases
       as a case of the build 'qa-<series>' (program: the harness name).
       Known failures: qa_known_failures.txt, lines
       '<harness> <case>  ticket N: ...' (or '<harness> *' for every case).
       --config records the cases under another build label instead of
       'qa-<series>' (the release stage's fetch_crash_test run uses
       'release_gcc14').
       Harnesses with several cases name them '<mode>/...', e.g.
       s3_fault_sweep 'position/PUT-binlog.index/error500/x6' or
       source_change_test 'gtid/reset'; fetch_crash_test with --storage s3
       records '<mode>/s3'. A known-failure name may be a shell-style
       pattern ('position/PUT-*/reset_after/*'), and as for MTR a name
       without '/' also covers every '<name>/...' case.

  mull runs extra/qa/mull/run_mull.sh (mutation testing of the Mull build's
       test binaries) and records a passing case 'mull/run' of the build
       'mull' with the score, plus one new case
       'mull/<file>:<line>:<column>-<mutator>' per mutant that no test kills
       and that is not in mull_known_survivors.txt. Listed survivors that no
       longer survive are named in the reason of 'mull/run'.

Statuses: passed, known (a listed failure), new (an unlisted failure),
known-passed (a listed case that passed), skipped (MTR skipped the test), and
no-test-cases (the stage produced no results at all, counted as broken).
"""

import argparse
import fnmatch
import json
import os
import re
import shutil
import signal
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path

MTR_LINE = re.compile(
    r"^\[\s*\d+%\]\s+binlog_streaming\.(?P<test>\S+)"
    r"(?:\s+'(?P<combination>[^']*)')?"
    r"(?:\s+w\d+)?\s+\[ (?P<result>pass|fail|skipped|disabled) \]"
    r"\s*(?P<rest>.*)$")
PROGRESS_LINE = re.compile(r"^\[\s*\d+%\]\s")
SERVER_LOG_LINE = re.compile(r"^\s*\d{4}-\d\d-\d\dT\d")
LOGFILE_MENTION = re.compile(r"logfile can be found in '([^']+)'")


def read_known(path: Path) -> list:
    """(scope, name, ticket) of every entry."""
    entries = []
    if not path.exists():
        return entries
    for line in path.read_text().splitlines():
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        head, _, ticket = line.partition("  ")
        fields = head.split()
        if len(fields) != 2:
            continue
        entries.append((fields[0], fields[1], ticket.strip()))
    return entries


def match_known(entries: list, scope: str, test_case: str) -> str:
    base = test_case.split("/", 1)[0]
    for entry_scope, name, ticket in entries:
        if entry_scope not in ("*", scope):
            continue
        if name in ("*", test_case, base) or \
                fnmatch.fnmatchcase(test_case, name):
            return ticket or "known"
    return ""


class Recorder:
    def __init__(self, args, config: str, program: str, prefix: str = ""):
        self.results = Path(args.results)
        # prepended to the test case in summary lines and log names (the
        # harness name, so that cases of different harnesses stay apart)
        self.prefix = prefix
        self.lines = Path(args.lines)
        self.out = Path(args.out)
        self.config = config
        self.program = program

    def record(self, test_case: str, failed: bool, ticket: str,
               reason: str, seconds: float, log_text: "str | None" = None,
               status: "str | None" = None):
        shown = self.prefix + test_case
        log_name = f"{self.config}-{shown.replace('/', '-')}.log"
        if status is None:
            if failed:
                status = "known" if ticket else "new"
            else:
                status = "known-passed" if ticket else "passed"
        line = ""
        if status == "known":
            line = f"known         {self.config}  {shown}  ({ticket})"
        elif status == "new":
            line = (f"NEW FAILURE   {self.config}  {shown}  {reason}  "
                    f"({log_name})")
        elif status == "known-passed":
            line = (f"KNOWN PASSED  {self.config}  {shown}  "
                    f"({ticket}; fixed, or not hit tonight?)")
        elif status == "no-test-cases":
            line = f"NO TEST CASES {self.config}  {self.program}  ({reason})"
        if failed and log_text is not None:
            (self.out / log_name).write_text(log_text)
        with self.results.open("a") as file:
            file.write(json.dumps({
                "config": self.config, "program": self.program,
                "test_case": test_case, "status": status, "ticket": ticket,
                "reason": reason, "seconds": int(seconds)}) + "\n")
        if line:
            with self.lines.open("a") as file:
                file.write(line + "\n")


# ---------------------------------------------------------------------------
# MTR
# ---------------------------------------------------------------------------

def run_mtr(args) -> int:
    recorder = Recorder(args, f"mtr-{args.series}", "binlog_streaming")
    known = read_known(Path(args.known))
    mtr_dir = Path(args.dist) / "mysql-test"
    full_log = Path(args.out) / f"mtr-{args.series}.log"
    shutil.rmtree(args.vardir, ignore_errors=True)
    command = [
        "./mtr", f"--vardir={args.vardir}",
        f"--build-thread={args.build_thread}", "--force",
        "--max-test-fail=0", "--retry=0", "--nounit-tests", "--big-test",
        f"--parallel={args.parallel}", "--testcase-timeout=30",
        f"--suite-timeout={args.suite_timeout}", "--shutdown-timeout=600",
        "--suite=binlog_streaming"]
    environment = dict(os.environ, BINSRV=str(args.binsrv))
    with full_log.open("w") as log:
        process = subprocess.run(command, cwd=mtr_dir, stdout=log,
                                 stderr=subprocess.STDOUT, env=environment)
    lines = full_log.read_text(errors="replace").splitlines()

    cases = []          # (test case, result, rest, block of lines)
    current = None
    for line in lines:
        match = MTR_LINE.match(line)
        if match:
            test_case = match["test"]
            if match["combination"]:
                test_case += "/" + match["combination"]
            current = [test_case, match["result"], match["rest"].strip(), []]
            cases.append(current)
        elif PROGRESS_LINE.match(line) or line.startswith("Completed:") or \
                line.startswith("------"):
            current = None
        elif current is not None:
            current[3].append(line)

    if not cases:
        recorder.record("", True, "", f"MTR produced no test results "
                        f"(exit {process.returncode}, see {full_log.name})",
                        0, status="no-test-cases")
        shutil.rmtree(args.vardir, ignore_errors=True)
        return 0

    for test_case, result, rest, block in cases:
        if result in ("skipped", "disabled"):
            recorder.record(test_case, False, "", rest[:150], 0,
                            status="skipped")
            continue
        ticket = match_known(known, args.series, test_case)
        failed = result == "fail"
        seconds = 0.0
        found = re.match(r"(\d+)", rest)
        if found:
            seconds = int(found[1]) / 1000
        reason = ""
        log_text = None
        if failed:
            # the mysqltest verdict, else the first error-like line that is
            # not from a server log (those start with a timestamp)
            own = [text.strip() for text in block
                   if not SERVER_LOG_LINE.match(text)]
            reason = rest or next(
                (text[:150] for text in own
                 if text.startswith("mysqltest: ")), None) or next(
                (text[:150] for text in own
                 if re.search(r"error|failed|mismatch|timeout", text, re.I)),
                "failed")
            log_text = "\n".join(block) + "\n"
            for text in block:
                mention = LOGFILE_MENTION.search(text)
                if mention and Path(mention[1]).is_file():
                    log_text += (f"\n===== {mention[1]} =====\n" +
                                 Path(mention[1]).read_text(errors="replace"))
        recorder.record(test_case, failed, ticket, reason[:200], seconds,
                        log_text)
    # a var directory grows to several GB; the logs of the failing tests are
    # copied into the run directory above
    shutil.rmtree(args.vardir, ignore_errors=True)
    return 0


# ---------------------------------------------------------------------------
# QA harnesses
# ---------------------------------------------------------------------------

# an ASan error in binlog_server must not look like its clean failure exit
# code 1, which most harnesses accept (exit 0 or 1 within the time limit)
ASAN_OPTIONS = "exitcode=134"


def json_lines(lines: list) -> list:
    """(object, arrival time) of every JSON line of the output, 'lines'
    being (arrival time, text) pairs."""
    result = []
    for moment, line in lines:
        line = line.strip()
        if line.startswith("{"):
            try:
                result.append((json.loads(line), moment))
            except ValueError:
                pass
    return result


def slug(text: str) -> str:
    return re.sub(r"[^A-Za-z0-9.]+", "-", str(text)).strip("-")


def summarize(failure: str, limit: int = 240) -> str:
    """The first line of a harness failure text, plus the last error-like
    line of the binlog_server log tail it carries (the one that ended the
    run)."""
    lines = [line.strip() for line in str(failure).splitlines()
             if line.strip()]
    if not lines:
        return ""
    head = lines[0]
    detail = next((line for line in reversed(lines[1:])
                   if re.search(r"\]\s*\[\s*(error|fatal)\]|Sanitizer|"
                                r"exception|Traceback", line, re.I)), "")
    detail = re.sub(r"^\[[0-9T:.-]+\]\s*", "", detail)
    return (head + (" | " + detail if detail else ""))[:limit]


def ending_error(output: str) -> str:
    """The exception that ended a harness, else its last lines."""
    texts = [line.strip() for line in output.splitlines() if line.strip()]
    error = next((line for line in reversed(texts) if re.match(
        r"[\w.]*(Error|Exception|Failure)\b", line)), "")
    return error or " | ".join(texts[-3:])


def harness_command(args, workdir: Path) -> list:
    qa = Path(args.qa_dir)
    script = [sys.executable, str(qa / f"{args.harness}.py")]
    base = ["--mysql-basedir", str(args.dist), "--binsrv", str(args.binsrv),
            "--workdir", str(workdir)]
    common = base + ["--port", str(args.port)]
    mode = ["--mode", args.mode] if args.mode else []
    if args.harness in ("pull_replica_test", "pull_heartbeat_test",
                        "pull_tls_test"):
        return script + common
    if args.harness == "pull_gtid_test":
        return script + common + mode
    if args.harness == "fetch_crash_test":
        command = script + common + [
            "--mode", args.mode, "--examples", str(args.examples),
            "--steps", str(args.steps)]
        if args.storage == "s3":
            command += ["--storage", "s3", "--s3-endpoint", args.s3_endpoint,
                        "--s3-bucket-dir", str(args.s3_bucket_dir)]
        return command
    if args.harness == "query_race_test":
        return script + common + ["--runs", str(args.runs)]
    if args.harness == "s3_fault_sweep":
        proxy = f"127.0.0.1:{args.s3_proxy_port}"
        return script + common + mode + [
            "--s3-endpoint", proxy,
            "--s3-fault-control", f"http://{proxy}/__faults__",
            "--s3-bucket-dir", str(args.s3_bucket_dir)]
    if args.harness == "disk_fault_sweep":
        return script + common + mode + [
            "--fsfault-lib", str(args.fsfault_lib)]
    if args.harness == "network_fault_test":
        return script + base + mode + [
            "--mysql-port", str(args.port), "--proxy-port",
            str(args.port + 1), "--toxiproxy-api", args.toxiproxy_api]
    if args.harness == "listener_fuzz_test":
        return script + common + mode + [
            "--connections", str(args.connections), "--seed", str(args.seed)]
    if args.harness == "large_binlog_test" and args.storage == "s3":
        return script + common + mode + [
            "--storage", "s3", "--s3-endpoint", args.s3_endpoint,
            "--s3-proxy-port", str(args.s3_proxy_port),
            "--s3-bucket-dir", str(args.s3_bucket_dir)]
    if args.harness == "pull_soak_test":
        return script + common + mode + ["--duration", str(args.duration)]
    if args.harness in ("source_change_test", "large_binlog_test",
                        "pull_load_test"):
        return script + common + mode
    raise SystemExit(f"unknown harness {args.harness}")


def harness_cases(args, returncode: int, output: str, lines: list,
                  started: float) -> list:
    """(test case, failed, reason, seconds or None, detail or None, window or
    None) of every case the run reports: detail is the case's own part of
    the output (None: the whole output is its log), window the (start, end)
    wall-clock times of a case that has its own JSON line."""
    objects = json_lines(lines)
    mode = args.mode or "position"
    if args.harness in ("pull_load_test", "pull_soak_test", "pull_tls_test",
                        "pull_gtid_test"):
        cases = []
        for result, _ in objects:
            if "case" not in result:
                continue
            reason = ""
            if not result.get("ok"):
                details = {key: value for key, value in result.items()
                           if key not in ("case", "ok")}
                reason = json.dumps(details)
            cases.append((f"{mode}/{result['case']}", not result.get("ok"),
                          reason[:240], None, None, None))
        return cases
    if args.harness == "large_binlog_test":
        cases = []
        for result, _ in objects:
            if "case" not in result:
                continue
            reason = result.get("reason", "")
            if not result.get("ok") and not reason:
                failed_checks = [check for check in (
                    "fetch_copies_match", "refetch", "list_size",
                    "pull_raw_copy", "s3_put_size")
                    if check in result and not result[check]]
                reason = ("failed: " + ", ".join(failed_checks) +
                          f" (fetch exit {result.get('fetch_exit')})")
            # cases on S3 storage: '<mode>/<case>/s3'
            suffix = "/s3" if result.get("storage") == "s3" else ""
            cases.append((f"{mode}/{result['case']}{suffix}",
                          not result.get("ok"), reason[:240], None, None,
                          None))
        return cases
    if args.harness == "pull_heartbeat_test":
        cases = []
        for result, _ in objects:
            if "case" not in result:
                continue
            reason = ""
            if not result.get("ok"):
                reason = (f"{result.get('heartbeats_during_quiet')} "
                          f"heartbeat(s), "
                          f"{result.get('replica_connections_during_quiet')} "
                          f"replica reconnection(s) in "
                          f"{result.get('quiet_seconds')} s of quiet")
                if not result.get("caught_up"):
                    reason = "replica did not catch up; " + reason
            cases.append((result["case"], not result.get("ok"), reason,
                          None, None, None))
        return cases
    if args.harness == "pull_replica_test":
        cases = []
        for result, _ in objects:
            if "mode" not in result:
                continue
            reason = ""
            if not result.get("replicated"):
                io_thread = result.get("replica_io_thread") or []
                reason = ("replica did not catch up: " +
                          " ".join(str(part) for part in io_thread))[:200]
            cases.append((result["mode"], not result.get("replicated"),
                          reason, None, None, None))
        return cases
    if args.harness == "fetch_crash_test":
        failed = returncode != 0
        reason = ""
        if failed:
            sanitizer = re.search(r"ERROR: (Address|Leak)Sanitizer: .{0,120}",
                                  output)
            errors = [line for line in output.splitlines()
                      if re.search(r"(Failure|Error|Exception)\b", line)]
            reason = (sanitizer[0] if sanitizer else errors[-1].strip()
                      if errors else f"exit code {returncode}")[:200]
        case = args.mode + ("/s3" if args.storage == "s3" else "")
        return [(case, failed, reason, None, None, None)]
    if args.harness == "query_race_test":
        for result, _ in objects:
            if result.get("test") == "query_race":
                reason = ""
                if not result.get("passed"):
                    top = result.get("errors") or [{}]
                    reason = (f"{result.get('failed')}/{result.get('runs')} "
                              f"list runs failed: "
                              f"{top[0].get('message', '')}")[:200]
                    if not result.get("pull_alive", True):
                        reason = "pull exited; " + reason
                return [("list_next_to_pull", not result.get("passed"),
                         reason, None, None, None)]
        return []

    # the harnesses with one JSON line per case
    cases = []
    for result, moment in objects:
        detail = json.dumps(result, indent=1)
        if args.harness == "s3_fault_sweep" and "fault" in result:
            fault = result["fault"]
            name = (f"{mode}/{fault['method']}-{slug(fault['match'])}/"
                    f"{fault['action']}/x{fault['count']}")
            if "case" in result:
                # a named case (the pull read case)
                name = f"{mode}/{result['case']}"
            if "recovery_fault" in result:
                second = result["recovery_fault"]
                name += (f"/then/{second['method']}-{slug(second['match'])}/"
                         f"{second['action']}/x{second['count']}")
            cases.append((name, "failure" in result,
                          summarize(result.get("failure", "")), moment,
                          detail))
        elif args.harness == "disk_fault_sweep" and "fault" in result:
            fault = result["fault"]
            name = (f"{mode}/{fault['op']}/{slug(fault['target'])}/"
                    f"{fault['errno']}/x{fault['count']}")
            if "recovery_fault" in result:
                second = result["recovery_fault"]
                name += (f"/then/{second['op']}/{slug(second['target'])}/"
                         f"{second['errno']}")
            cases.append((name, "failure" in result,
                          summarize(result.get("failure", "")), moment,
                          detail))
        elif args.harness == "network_fault_test" and "part" in result:
            name = f"{mode}/{result['part']}/"
            if result["part"] == "pull":
                name += f"r{result.get('round', 0)}-"
            name += slug(result.get("fault", ""))
            cases.append((name, "failure" in result,
                          summarize(result.get("failure", "")), moment,
                          detail))
        elif args.harness == "listener_fuzz_test" and "crashed" in result:
            failed = result["crashed"] or not result.get("still_serves")
            reason = ""
            if result["crashed"]:
                stderr = result.get("stderr_tail", "")
                sanitizer = re.search(r"ERROR: \w+Sanitizer: .{0,100}",
                                      stderr)
                reason = (f"pull exited with {result.get('exit')} at "
                          f"connection {result.get('crashed_at')} "
                          f"({result.get('last_scenario')})" +
                          (f": {sanitizer[0]}" if sanitizer else ""))
            elif failed:
                reason = "pull no longer streams binlogs after the fuzzing"
            cases.append((mode, failed, reason[:240], moment, detail))
        elif args.harness == "source_change_test" and "scenario" in result:
            reason = ""
            if not result.get("ok"):
                parts = [result.get("exception", "")]
                for key in ("fetch_after_reset", "fetch_after_identity_change",
                            "fetch_after_purge"):
                    if key in result:
                        parts.append(f"{key}: exit "
                                     f"{result[key].get('exit')}")
                for key in ("mismatch", "copy_error"):
                    if key in result:
                        parts.append(summarize(result[key], 160))
                if result.get("running_after_restart") is False:
                    parts.append("pull exited")
                reason = "; ".join(part for part in parts if part)
            cases.append((f"{mode}/{result['scenario']}",
                          not result.get("ok"), reason[:240], moment,
                          detail))
    # each case took the time since the previous one's line
    timed = []
    previous = started
    for name, failed, reason, moment, detail in cases:
        timed.append((name, failed, reason, moment - previous, detail,
                      (previous, moment)))
        previous = moment
    cases = timed
    # a run that stopped early, or failed without a failing case
    if returncode == 124:
        cases.append((f"{mode}/timeout", True,
                      f"timed out after {args.timeout} s with {len(cases)} "
                      f"cases done", 0, None, None))
    elif cases and returncode != 0 and not any(case[1] for case in cases):
        cases.append((f"{mode}/run", True,
                      f"exit {returncode}: {ending_error(output)}"[:240], 0,
                      None, None))
    return cases


def run_harness(command: list, timeout: int, environment: dict) -> tuple:
    """Runs a harness in its own process group; returns (exit code, 124 on a
    timeout; output; (arrival time, line) pairs). Whatever it started (MySQL
    servers, binlog_server) goes with it on a timeout."""
    process = subprocess.Popen(command, stdout=subprocess.PIPE,
                               stderr=subprocess.STDOUT, text=True,
                               errors="replace", env=environment,
                               start_new_session=True)
    lines = []

    def read():
        for line in process.stdout:
            lines.append((time.time(), line))

    reader = threading.Thread(target=read, daemon=True)
    reader.start()
    try:
        returncode = process.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        returncode = 124
        try:
            os.killpg(process.pid, signal.SIGTERM)
            process.wait(timeout=30)
        except (ProcessLookupError, subprocess.TimeoutExpired):
            pass
    # anything of the group still running (killpg also works once the
    # leader is gone)
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except (ProcessLookupError, PermissionError):
        pass
    process.wait()
    reader.join(timeout=10)
    output = "".join(line for _, line in lines)
    if returncode == 124:
        output += f"\ntimed out after {timeout} s\n"
    return returncode, output, lines


def start_s3_proxy(args, environment: dict, log_path: Path):
    """Starts s3_fault_proxy.py for s3_fault_sweep; returns the process, or
    raises RuntimeError when it does not come up."""
    log = log_path.open("w")
    proxy = subprocess.Popen(
        [sys.executable, str(Path(args.qa_dir) / "s3_fault_proxy.py"),
         "--listen", f"127.0.0.1:{args.s3_proxy_port}",
         "--backend", args.s3_endpoint],
        stdout=log, stderr=subprocess.STDOUT, env=environment,
        start_new_session=True)
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(
                    f"http://127.0.0.1:{args.s3_proxy_port}/__faults__",
                    timeout=2) as response:
                response.read()
            return proxy
        except OSError:
            if proxy.poll() is not None:
                break
            time.sleep(0.3)
    proxy.kill()
    proxy.wait()
    raise RuntimeError(f"s3_fault_proxy.py did not come up on port "
                       f"{args.s3_proxy_port} (see {log_path})")


def precondition_failure(args) -> str:
    """Why the harness cannot run at all, or ''."""
    def reachable(url: str) -> bool:
        try:
            with urllib.request.urlopen(url, timeout=5) as response:
                response.read()
            return True
        except urllib.error.HTTPError:
            return True
        except OSError:
            return False
    uses_s3 = args.harness == "s3_fault_sweep" or args.storage == "s3"
    if uses_s3 and not reachable(f"http://{args.s3_endpoint}/"):
        return f"the S3 server at {args.s3_endpoint} is not reachable"
    if uses_s3 and not (args.s3_bucket_dir and
                        Path(args.s3_bucket_dir).is_dir()):
        return f"the bucket directory {args.s3_bucket_dir} does not exist"
    if args.harness == "network_fault_test" and \
            not reachable(f"{args.toxiproxy_api}/version"):
        return f"Toxiproxy is not reachable at {args.toxiproxy_api}"
    if args.harness == "disk_fault_sweep" and not (
            args.fsfault_lib and Path(args.fsfault_lib).is_file()):
        return f"the fsfault library {args.fsfault_lib} does not exist"
    return ""


def sanitizer_reports(directory: Path) -> list:
    """(modification time, summary line, text) of every sanitizer report
    written under 'directory' (ASAN_OPTIONS log_path)."""
    reports = []
    for path in sorted(directory.glob("asan.*")):
        text = path.read_text(errors="replace")
        found = re.search(r"SUMMARY: (\w+Sanitizer: .*)", text) or \
            re.search(r"ERROR: (\w+Sanitizer: .*)", text)
        summary = found[1] if found else text.strip().split("\n")[0]
        reports.append((path.stat().st_mtime, summary[:160],
                        f"===== {path.name} =====\n{text}"))
    return reports


def run_qa(args) -> int:
    recorder = Recorder(args, args.config or f"qa-{args.series}",
                        args.harness, args.harness + "/")
    known = read_known(Path(args.known))
    workdir = Path(args.workdir)
    shutil.rmtree(workdir, ignore_errors=True)
    workdir.mkdir(parents=True)
    problem = precondition_failure(args)
    if problem:
        recorder.record("", True, "", problem, 0, status="no-test-cases")
        return 0
    environment = dict(os.environ)
    # sanitizer reports go to files (a harness keeps only the end of
    # binlog_server's stderr, which cuts a report's head off), one per
    # process, and are attached to the case that was running
    reports_dir = workdir / "sanitizer"
    reports_dir.mkdir()
    environment.setdefault(
        "ASAN_OPTIONS", f"{ASAN_OPTIONS}:log_path={reports_dir}/asan")
    environment.setdefault("AWS_EC2_METADATA_DISABLED", "true")
    command = harness_command(args, workdir)
    # the S3 objects the harness creates (fetch_crash_test uses a prefix per
    # process): removed afterwards, or moved into the workdir of a failure
    bucket = Path(args.s3_bucket_dir) if args.s3_bucket_dir else None
    before = set(os.listdir(bucket)) if bucket and bucket.is_dir() else None
    proxy = None
    if args.harness == "s3_fault_sweep":
        try:
            proxy = start_s3_proxy(args, environment,
                                   workdir / "s3_fault_proxy.log")
        except RuntimeError as error:
            recorder.record("", True, "", str(error), 0,
                            status="no-test-cases")
            return 0
    started = time.time()
    try:
        returncode, output, lines = run_harness(command, args.timeout,
                                                environment)
    finally:
        if proxy is not None:
            proxy.terminate()
            try:
                proxy.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proxy.kill()
                proxy.wait()
    seconds = time.time() - started
    reports = sanitizer_reports(reports_dir)
    log_text = " ".join(command) + "\n\n" + output + "".join(
        "\n" + text for _, _, text in reports)
    cases = harness_cases(args, returncode, output, lines, started)
    if not cases:
        cases = [(args.mode or "run", True,
                  f"timed out after {args.timeout} s" if returncode == 124
                  else f"harness reported nothing (exit {returncode}): "
                  f"{ending_error(output)}"[:240],
                  None, None, None)]
    any_failed = any(case[1] for case in cases) or bool(reports)
    # cases without their own time share what the others did not use
    timed = sum(case[3] for case in cases if case[3] is not None)
    untimed = [case for case in cases if case[3] is None]
    share = max(seconds - timed, 0) / len(untimed) if untimed else 0
    full_log = None
    if any_failed and any(case[4] is not None for case in cases):
        # one log of the whole run; each failing case's log holds its own
        # JSON line and points at it
        full_log = (f"{recorder.config}-{args.harness}-"
                    f"{args.mode or 'run'}-full.log")
        (Path(args.out) / full_log).write_text(log_text)
    for test_case, failed, reason, case_seconds, detail, window in cases:
        # the sanitizer reports written while the case ran (before its JSON
        # line; a case without its own window gets them all); a report
        # fails the case even if the harness let the exit code pass
        mine = [report for report in reports if window is None or
                window[0] < report[0] <= window[1]]
        if mine:
            failed = True
            summaries = sorted({summary for _, summary, _ in mine})
            reason = (f"{reason} | " if reason else "") + "; ".join(summaries)
        ticket = match_known(known, args.harness, test_case)
        # a sanitizer report is its own bug: a name pattern does not cover
        # it unless the ticket says so
        if mine and "Sanitizer" not in ticket:
            ticket = ""
        text = log_text
        if detail is not None:
            text = (" ".join(command) + "\n\n" + detail + "".join(
                "\n\n" + report for _, _, report in mine) +
                f"\n\nthe whole run's output: {full_log}\n")
        recorder.record(test_case, failed, ticket, reason[:300],
                        share if case_seconds is None else case_seconds,
                        text)
    if bucket is not None and before is not None and bucket.is_dir():
        for name in set(os.listdir(bucket)) - before:
            if any_failed:
                shutil.move(str(bucket / name), str(workdir / f"s3-{name}"))
            else:
                shutil.rmtree(bucket / name, ignore_errors=True)
    if not any_failed:
        shutil.rmtree(workdir, ignore_errors=True)
    else:
        # keep the logs and the storage of a failing run, not the MySQL data
        # directories (hundreds of MB, and a run directory is kept 30 days)
        for data in workdir.rglob("data"):
            if data.is_dir() and (data / "mysql").exists():
                shutil.rmtree(data, ignore_errors=True)
    return 0


# ---------------------------------------------------------------------------
# libFuzzer targets
# ---------------------------------------------------------------------------

FUZZ_STATS = re.compile(r"^#(\d+):?\s+.*?cov: (\d+) ft: (\d+) corp: (\d+)")
FUZZ_SIGNATURE = re.compile(
    r"what\(\):\s+(?P<what>.{0,120})|"
    r"SUMMARY: (?P<sanitizer>[A-Za-z]+Sanitizer: \S+(?: \S+)?)|"
    r"runtime error: (?P<ub>.{0,120})|"
    r"(?P<oom>out-of-memory)|(?P<timeout>ALARM: working on the last Unit for)")


def fuzz_signature(output: str, artifact: str) -> str:
    """What a finding is, independent of the input: the exception message,
    the sanitizer summary, the UBSan error or out-of-memory / timeout, with
    addresses and long numbers blanked."""
    match = FUZZ_SIGNATURE.search(output)
    if match is None:
        text = artifact.split("-", 1)[0] + " without a message"
    elif match.group("what"):
        text = "terminate: " + match.group("what")
    elif match.group("sanitizer"):
        text = match.group("sanitizer")
    elif match.group("ub"):
        text = "undefined behaviour: " + match.group("ub")
    elif match.group("oom"):
        text = "out of memory"
    else:
        text = "timeout"
    text = re.sub(r"0x[0-9a-f]+", "ADDR", text)
    return re.sub(r"\d{3,}", "N", text).strip()


def run_fuzz(args) -> int:
    """Runs one libFuzzer target for --seconds against its persistent corpus
    (--corpus), reproduces every finding and records one case per distinct
    signature ('<target>/<signature>', known ones from --known), plus a
    passing '<target>/run' case with the run's statistics."""
    recorder = Recorder(args, "fuzz", args.target)
    out = Path(args.out)
    artifacts = out / f"fuzz-{args.target}"
    shutil.rmtree(artifacts, ignore_errors=True)
    artifacts.mkdir(parents=True)
    Path(args.corpus).mkdir(parents=True, exist_ok=True)
    known = read_known(Path(args.known))
    command = [str(args.binary), f"-fork={args.jobs}", "-ignore_crashes=1",
               "-ignore_ooms=1", "-ignore_timeouts=1", "-max_len=65536",
               "-timeout=20", "-rss_limit_mb=4096",
               f"-max_total_time={args.seconds}",
               f"-artifact_prefix={artifacts}/", str(args.corpus)]
    started = time.monotonic()
    try:
        run = subprocess.run(command, capture_output=True, text=True,
                             errors="replace", timeout=args.seconds + 600)
        log_text = run.stdout + run.stderr
    except subprocess.TimeoutExpired as error:
        log_text = (error.stdout or b"").decode(errors="replace") + \
            "\n(the fuzzer did not stop in time)"
    seconds = time.monotonic() - started
    stats = [match for line in log_text.splitlines()
             if (match := FUZZ_STATS.match(line))]
    summary = ("no statistics" if not stats else
               f"{stats[-1].group(1)} inputs, cov {stats[-1].group(2)}, "
               f"ft {stats[-1].group(3)}, corpus {stats[-1].group(4)}")

    groups = {}
    for artifact in sorted(artifacts.iterdir()):
        try:
            replay = subprocess.run([str(args.binary), str(artifact)],
                                    capture_output=True, text=True,
                                    errors="replace", timeout=120)
            output = replay.stdout + replay.stderr
        except subprocess.TimeoutExpired:
            output = "ALARM: working on the last Unit for (replay timeout)"
        groups.setdefault(fuzz_signature(output, artifact.name),
                          []).append((artifact, output))
    for signature, findings in sorted(groups.items()):
        # keep a few inputs to reproduce each finding, not all of them
        for artifact, _ in findings[3:]:
            artifact.unlink()
        test_case = f"{args.target}/{slug(signature)[:100]}"
        ticket = match_known(known, args.target, test_case)
        kept = ", ".join(str(artifact) for artifact, _ in findings[:3])
        recorder.record(
            test_case, True, ticket,
            f"{len(findings)} input(s): {signature}"[:300], 0,
            f"{signature}\n\n{len(findings)} input(s); kept: {kept}\n"
            f"reproduce: {args.binary} <input>\n\n{findings[0][1]}")
    recorder.record(f"{args.target}/run", False, "", summary, seconds)
    log_path = out / f"fuzz-{args.target}.log"
    log_path.write_text(" ".join(command) + "\n\n" + log_text[-200_000:])
    if not groups:
        shutil.rmtree(artifacts, ignore_errors=True)
    return 0


def run_mull(args) -> int:
    """Runs run_mull.sh over the Mull build and records 'mull/run' plus a new
    case per unlisted surviving mutant."""
    recorder = Recorder(args, "mull", "mull")
    reports = Path(args.out) / "mull"
    started = time.monotonic()
    environment = dict(os.environ, MULL_KNOWN_SURVIVORS=str(args.known))
    try:
        run = subprocess.run(
            [str(args.mull_script), str(args.runner), str(args.build),
             str(reports), str(args.known_failures)],
            capture_output=True, text=True, errors="replace",
            timeout=args.timeout, env=environment)
        output = run.stdout + run.stderr
    except subprocess.TimeoutExpired as error:
        output = (error.stdout or b"").decode(errors="replace") + \
            f"\n(timed out after {args.timeout} s)"
    seconds = time.monotonic() - started
    (Path(args.out) / "mull.log").write_text(output)
    summary_path = reports / "summary.json"
    if not summary_path.exists():
        recorder.record("", True, "", "run_mull.sh wrote no summary, see "
                        "mull.log", seconds, status="no-test-cases")
        return 0
    summary = json.loads(summary_path.read_text())
    for key in summary["surviving"]:
        location, mutator = key.split(" ", 1)
        recorder.record(
            f"mull/{location}-{mutator}", True, "",
            f"no test kills the mutant {mutator} at {location}", 0,
            f"{key}\n\nNo test binary of the run fails with this mutant. "
            f"Add a test that notices it, or, if it is equivalent or "
            f"unreachable, list it in mull_known_survivors.txt.\n\n"
            f"Per binary: {reports}/<binary>.txt\n\n{output}")
    reason = (f"score {summary['score']}%: {summary['killed']} of "
              f"{summary['covered']} covered mutants killed, "
              f"{len(summary['known'])} known survivors")
    if summary["stale"]:
        reason += (f"; {len(summary['stale'])} listed survivors no longer "
                   f"survive, remove them: " + ", ".join(summary["stale"]))
    recorder.record("mull/run", False, "", reason[:600], seconds)
    return 0


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("stage", choices=["mtr", "qa", "fuzz", "mull"])
    parser.add_argument("--dist", type=Path, help="the MySQL distribution")
    parser.add_argument("--series", help="8.0, 8.4, 9.7")
    parser.add_argument("--binsrv", type=Path)
    parser.add_argument("--known", required=True,
                        help="the known-failures file of the stage")
    parser.add_argument("--results", required=True,
                        help="results.jsonl to append to")
    parser.add_argument("--lines", required=True,
                        help="the summary lines file to append to")
    parser.add_argument("--out", required=True,
                        help="the run directory for logs")
    # mtr
    parser.add_argument("--vardir")
    parser.add_argument("--build-thread", type=int, default=500)
    parser.add_argument("--parallel", type=int, default=4)
    parser.add_argument("--suite-timeout", type=int, default=120,
                        help="minutes")
    # qa
    parser.add_argument("--harness")
    parser.add_argument("--config", default="",
                        help="build label of the cases (default qa-<series>)")
    parser.add_argument("--qa-dir", help="extra/qa of the source tree")
    parser.add_argument("--workdir")
    parser.add_argument("--port", type=int, default=24500)
    parser.add_argument("--mode", default="")
    parser.add_argument("--examples", type=int, default=10)
    parser.add_argument("--steps", type=int, default=8)
    parser.add_argument("--runs", type=int, default=500)
    parser.add_argument("--timeout", type=int, default=1800, help="seconds")
    parser.add_argument("--storage", choices=["file", "s3"], default="file",
                        help="fetch_crash_test's and large_binlog_test's "
                             "storage")
    parser.add_argument("--s3-endpoint", default="127.0.0.1:9000",
                        help="the S3 server (fetch_crash_test --storage s3, "
                             "and s3_fault_sweep's proxy backend)")
    parser.add_argument("--s3-bucket-dir", type=Path,
                        help="the directory of the S3 server's bucket")
    parser.add_argument("--s3-proxy-port", type=int, default=24590,
                        help="where s3_fault_sweep's fault proxy (or "
                             "large_binlog_test's counting one) listens")
    parser.add_argument("--fsfault-lib", type=Path,
                        help="disk_fault_sweep's LD_PRELOAD library")
    parser.add_argument("--toxiproxy-api", default="http://127.0.0.1:8474")
    parser.add_argument("--connections", type=int, default=3000,
                        help="listener_fuzz_test connections")
    parser.add_argument("--seed", type=int, default=1,
                        help="listener_fuzz_test seed")
    parser.add_argument("--duration", type=int, default=7200,
                        help="pull_soak_test seconds of load")
    # fuzz
    parser.add_argument("--target", help="the libFuzzer target's name")
    parser.add_argument("--binary", type=Path, help="the libFuzzer target")
    parser.add_argument("--corpus", type=Path,
                        help="the target's corpus, kept between runs")
    parser.add_argument("--seconds", type=int, default=900)
    parser.add_argument("--jobs", type=int, default=8)
    # mull
    parser.add_argument("--mull-script", type=Path,
                        help="extra/qa/mull/run_mull.sh")
    parser.add_argument("--runner", type=Path, help="mull-runner")
    parser.add_argument("--build", type=Path, help="the Mull build directory")
    parser.add_argument("--known-failures", type=Path,
                        help="known_failures.txt (test cases left out)")
    args = parser.parse_args()
    if args.stage == "mull":
        if not (args.mull_script and args.runner and args.build):
            parser.error("mull needs --mull-script, --runner and --build")
        return run_mull(args)
    if args.stage == "fuzz":
        if not (args.target and args.binary and args.corpus):
            parser.error("fuzz needs --target, --binary and --corpus")
        return run_fuzz(args)
    if not (args.dist and args.series and args.binsrv):
        parser.error(f"{args.stage} needs --dist, --series and --binsrv")
    if args.stage == "mtr":
        if not args.vardir:
            parser.error("mtr needs --vardir")
        return run_mtr(args)
    if not (args.harness and args.qa_dir and args.workdir):
        parser.error("qa needs --harness, --qa-dir and --workdir")
    return run_qa(args)


if __name__ == "__main__":
    sys.exit(main())
