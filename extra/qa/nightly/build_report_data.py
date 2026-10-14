#!/usr/bin/env python3
"""Collects the nightly runs into one JSON document for the report page.

Reads ~/ws/nightly/history.tsv (one line per run), the per-test results of
every run directory still kept (results.jsonl, or for runs from before it
existed, a reconstruction from nightly.log and summary.txt) and the
known-failure lists, and writes ~/ws/nightly/report.json.

Besides the property-test builds (debug_gcc14, asan_gcc14, tsan_gcc14), a
run's results hold the MTR stage as builds 'mtr-<series>' (program
'binlog_streaming', one case per test or test/combination) and the QA-harness
stage as builds 'qa-<series>' (program: the harness, one case per mode or
harness case). Their statuses are the same, plus 'skipped' for MTR tests the
suite skipped. Every known failure carries the 'kind' of list it comes from
(property, mtr, qa) and its 'scope' (the MySQL series or '*' for MTR, the
harness for QA, empty for property cases).
"""

import json
import re
from datetime import datetime, timezone
from pathlib import Path

NIGHTLY = Path.home() / "ws" / "nightly"
LINE = re.compile(r"^\[(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d)\] (.*)$")


def history():
    runs = []
    for line in (NIGHTLY / "history.tsv").read_text().splitlines():
        fields = line.split("\t")
        if len(fields) < 7:
            continue
        day, revision, new, broken, known, passed, minutes = fields[:7]
        runs.append({"day": day, "revision": revision, "new": int(new),
                     "broken": int(broken), "known": int(known),
                     "passed": int(passed), "minutes": int(minutes)})
    return runs


def reconstruct(run_dir: Path):
    """Results of a run from before results.jsonl: every test case from
    nightly.log, the failures from summary.txt."""
    entries = []
    for line in (run_dir / "nightly.log").read_text().splitlines():
        match = LINE.match(line)
        if match:
            entries.append((datetime.strptime(match[1], "%Y-%m-%d %H:%M:%S"),
                            match[2]))
    started = entries[0][0] if entries else None
    outcomes = {}
    for line in (run_dir / "summary.txt").read_text().splitlines():
        for label, status in (("NEW FAILURE", "new"), ("known", "known"),
                              ("KNOWN PASSED", "known-passed")):
            if line.startswith(label):
                rest = line[len(label):].split()
                if len(rest) >= 2:
                    config, test_case = rest[0], rest[1]
                    detail = line[line.find(test_case) + len(test_case):]
                    ticket = ""
                    found = re.search(r"\((ticket [^;)]*)", detail)
                    if found:
                        ticket = found[1]
                    outcomes[(config, test_case)] = (status, ticket,
                                                     detail.strip())
    results = []
    for index, (moment, text) in enumerate(entries):
        following = entries[index + 1][0] if index + 1 < len(entries) else moment
        seconds = int((following - moment).total_seconds())
        unit = re.match(r"unit tests \((\S+)\)", text)
        case = re.match(r"(\S+) (\S+)/(\S+)$", text)
        if unit:
            config = unit[1]
            status = "new" if any(c == config and t == "unit"
                                  for (c, t) in outcomes) else "passed"
            results.append({"config": config, "program": "ctest",
                            "test_case": "unit tests", "status": status,
                            "ticket": "", "reason": "", "seconds": seconds})
        elif case:
            config, program, test_case = case[1], case[2], case[3]
            status, ticket, reason = outcomes.get((config, test_case),
                                                  ("passed", "", ""))
            results.append({"config": config, "program": program,
                            "test_case": test_case, "status": status,
                            "ticket": ticket,
                            "reason": "" if status == "passed" else reason,
                            "seconds": seconds})
    return started, results


def run_details():
    runs = []
    for run_dir in sorted((NIGHTLY / "runs").iterdir()):
        if not run_dir.is_dir():
            continue
        results_file = run_dir / "results.jsonl"
        if results_file.exists():
            results = [json.loads(line) for line in
                       results_file.read_text().splitlines() if line]
            meta = json.loads((run_dir / "run.json").read_text()) \
                if (run_dir / "run.json").exists() else {}
            started = meta.get("started")
        else:
            moment, results = reconstruct(run_dir)
            started = moment.strftime("%Y-%m-%d %H:%M:%S") if moment else None
        runs.append({"day": run_dir.name, "started": started,
                     "results": results})
    return runs


def known_failures():
    known = []
    for kind, file_name, scoped in (("property", "known_failures.txt", False),
                                    ("mtr", "mtr_known_failures.txt", True),
                                    ("qa", "qa_known_failures.txt", True)):
        path = NIGHTLY / file_name
        if not path.exists():
            continue
        for line in path.read_text().splitlines():
            if not line.strip() or line.lstrip().startswith("#"):
                continue
            name, _, ticket = line.partition("  ")
            scope = ""
            if scoped:
                scope, _, name = name.strip().partition(" ")
            known.append({"kind": kind, "scope": scope,
                          "test_case": name.strip(),
                          "ticket": ticket.strip()})
    return known


def builds(runs):
    """A description of every build label that occurs in the kept runs."""
    described = {"debug_gcc14": "debug, 2000 cases per property",
                 "asan_gcc14": "AddressSanitizer, 500 cases",
                 "tsan_gcc14": "ThreadSanitizer, storage test only, "
                               "300 cases"}
    for run in runs:
        for result in run["results"]:
            label = result.get("config", "")
            if label in described:
                continue
            if label.startswith("mtr-"):
                described[label] = (f"binlog_streaming MTR suite, debug build, "
                                    f"MySQL {label[4:]}")
            elif label.startswith("qa-"):
                described[label] = (f"black-box QA harnesses (extra/qa), "
                                    f"MySQL {label[3:]}")
    return described


def main():
    runs = run_details()
    report = {"generated": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
              "history": history(), "runs": runs,
              "known_failures": known_failures(),
              "builds": builds(runs)}
    (NIGHTLY / "report.json").write_text(json.dumps(report, indent=1))
    print(f"{len(report['history'])} runs in history, "
          f"{len(report['runs'])} with details")


if __name__ == "__main__":
    main()
