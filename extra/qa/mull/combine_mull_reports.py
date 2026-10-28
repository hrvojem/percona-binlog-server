#!/usr/bin/env python3
# Copyright (c) 2026 Percona and/or its affiliates.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License, version 2.0,
# as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License, version 2.0, for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

"""Combines the SQLite reports of several mull-runner runs (one per test
executable) into the mutants that no test kills.

mull-runner judges each test executable on its own, but a mutant only shows
a weak test suite if it survives every test that covers it. A mutant counts
as killed when any run reports it failed, timed out, crashed or exited
abnormally, and as surviving when at least one run executed it and none
killed it. Mutants listed in --known (one '<file>:<line>:<column>
<mutator>' per line, '#' comments allowed, the file relative to src/) are
reported separately, as the triaged equivalent or unreachable ones.

Prints a summary and the surviving mutants; with --json, writes them as
JSON too. Example:

  python3 combine_mull_reports.py --known mull_known_survivors.txt \\
    ~/ws/mull/reports/*.sqlite
"""

import argparse
import json
import sqlite3
import sys
from pathlib import Path

# Mull's ExecutionStatus values that mean a test noticed the mutant
KILLED_STATUSES = {1, 3, 4, 5}  # Failed, Timedout, Crashed, AbnormalExit
PASSED_STATUS = 2


def mutant_key(mutant_id: str) -> str:
    """'<mutator>:<path>:<line>:<column>:...' as '<file>:<line>:<column>
    <mutator>', the file relative to src/."""
    mutator, path, line, column = mutant_id.split(":")[:4]
    relative = path.split("/src/", 1)[-1]
    return f"{relative}:{line}:{column} {mutator}"


def read_known(path: "Path | None") -> set:
    if path is None:
        return set()
    known = set()
    for line in path.read_text().splitlines():
        line = line.split("#", 1)[0].strip()
        if line:
            known.add(" ".join(line.split()[:2]))
    return known


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("reports", nargs="+", type=Path)
    parser.add_argument("--known", type=Path)
    parser.add_argument("--json", type=Path)
    args = parser.parse_args()

    statuses = {}
    for report in args.reports:
        with sqlite3.connect(report) as connection:
            for mutant_id, status in connection.execute(
                    "SELECT mutant_id, execution_status FROM mutant"):
                statuses.setdefault(mutant_key(mutant_id), []).append(status)
    killed = {key for key, values in statuses.items()
              if KILLED_STATUSES.intersection(values)}
    surviving = sorted(key for key, values in statuses.items()
                       if key not in killed and PASSED_STATUS in values)
    known = read_known(args.known)
    new = [key for key in surviving if key not in known]
    triaged = [key for key in surviving if key in known]
    score = 100.0 * len(killed) / max(len(statuses), 1)
    print(f"{len(args.reports)} reports, {len(statuses)} covered mutants, "
          f"{len(killed)} killed, {len(surviving)} surviving every test "
          f"(score {score:.1f}%), {len(triaged)} of them known")
    for key in new:
        print(f"  SURVIVED {key}")
    if args.json:
        args.json.write_text(json.dumps({
            "covered": len(statuses), "killed": len(killed),
            "score": round(score, 1), "surviving": new, "known": triaged},
            indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
