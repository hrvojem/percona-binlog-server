#!/usr/bin/env python3
"""The MTR and QA-harness stages of the nightly run.

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

Statuses: passed, known (a listed failure), new (an unlisted failure),
known-passed (a listed case that passed), skipped (MTR skipped the test), and
no-test-cases (the stage produced no results at all, counted as broken).
"""

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import time
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
        if name in ("*", test_case, base):
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

def json_lines(text: str) -> list:
    result = []
    for line in text.splitlines():
        line = line.strip()
        if line.startswith("{"):
            try:
                result.append(json.loads(line))
            except ValueError:
                pass
    return result


def harness_command(args, workdir: Path) -> list:
    qa = Path(args.qa_dir)
    common = ["--mysql-basedir", str(args.dist), "--binsrv", str(args.binsrv),
              "--workdir", str(workdir), "--port", str(args.port)]
    if args.harness == "pull_replica_test":
        return [sys.executable, str(qa / "pull_replica_test.py")] + common
    if args.harness == "fetch_crash_test":
        return [sys.executable, str(qa / "fetch_crash_test.py")] + common + [
            "--mode", args.mode, "--examples", str(args.examples),
            "--steps", str(args.steps)]
    if args.harness == "query_race_test":
        return [sys.executable, str(qa / "query_race_test.py")] + common + [
            "--runs", str(args.runs)]
    raise SystemExit(f"unknown harness {args.harness}")


def harness_cases(args, returncode: int, output: str) -> list:
    """(test case, failed, reason) of every case the run reports."""
    lines = json_lines(output)
    if args.harness == "pull_replica_test":
        cases = []
        for result in lines:
            if "mode" not in result:
                continue
            reason = ""
            if not result.get("replicated"):
                io_thread = result.get("replica_io_thread") or []
                reason = ("replica did not catch up: " +
                          " ".join(str(part) for part in io_thread))[:200]
            cases.append((result["mode"], not result.get("replicated"),
                          reason))
        return cases
    if args.harness == "fetch_crash_test":
        failed = returncode != 0
        reason = ""
        if failed:
            errors = [line for line in output.splitlines()
                      if re.search(r"(Failure|Error|Exception)\b", line)]
            reason = (errors[-1].strip() if errors else
                      f"exit code {returncode}")[:200]
        return [(args.mode, failed, reason)]
    if args.harness == "query_race_test":
        for result in lines:
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
                         reason)]
        return []
    return []


def run_qa(args) -> int:
    recorder = Recorder(args, f"qa-{args.series}", args.harness,
                        args.harness + "/")
    known = read_known(Path(args.known))
    workdir = Path(args.workdir)
    shutil.rmtree(workdir, ignore_errors=True)
    workdir.mkdir(parents=True)
    command = harness_command(args, workdir)
    started = time.monotonic()
    try:
        run = subprocess.run(command, capture_output=True, text=True,
                             timeout=args.timeout)
        returncode, output = run.returncode, run.stdout + run.stderr
    except subprocess.TimeoutExpired as expired:
        returncode = 124
        output = ((expired.stdout or b"").decode(errors="replace") +
                  (expired.stderr or b"").decode(errors="replace") +
                  f"\ntimed out after {args.timeout} s\n")
    seconds = time.monotonic() - started
    log_text = " ".join(command) + "\n\n" + output
    cases = harness_cases(args, returncode, output)
    if not cases:
        cases = [(args.mode or "run", True,
                  f"timed out after {args.timeout} s" if returncode == 124
                  else f"harness reported nothing (exit {returncode})")]
    for test_case, failed, reason in cases:
        ticket = match_known(known, args.harness, test_case)
        recorder.record(test_case, failed, ticket, reason,
                        seconds / len(cases), log_text)
    if not any(failed for _, failed, _ in cases):
        shutil.rmtree(workdir, ignore_errors=True)
    else:
        # keep the logs and the storage of a failing run, not the MySQL data
        # directories (hundreds of MB, and a run directory is kept 30 days)
        for data in workdir.rglob("data"):
            if data.is_dir() and (data / "mysql").exists():
                shutil.rmtree(data, ignore_errors=True)
    return 0


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("stage", choices=["mtr", "qa"])
    parser.add_argument("--dist", type=Path, required=True,
                        help="the MySQL distribution")
    parser.add_argument("--series", required=True, help="8.0, 8.4, 9.7")
    parser.add_argument("--binsrv", type=Path, required=True)
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
    parser.add_argument("--qa-dir", help="extra/qa of the source tree")
    parser.add_argument("--workdir")
    parser.add_argument("--port", type=int, default=24500)
    parser.add_argument("--mode", default="")
    parser.add_argument("--examples", type=int, default=10)
    parser.add_argument("--steps", type=int, default=8)
    parser.add_argument("--runs", type=int, default=500)
    parser.add_argument("--timeout", type=int, default=1800, help="seconds")
    args = parser.parse_args()
    if args.stage == "mtr":
        if not args.vardir:
            parser.error("mtr needs --vardir")
        return run_mtr(args)
    if not (args.harness and args.qa_dir and args.workdir):
        parser.error("qa needs --harness, --qa-dir and --workdir")
    return run_qa(args)


if __name__ == "__main__":
    sys.exit(main())
