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

"""Stopping what an earlier QA run left running in a work directory (see
stop_leftover_processes). Used by the harness module (fetch_crash_test.py)
and by nightly/nightly_stages.py; standard library only."""

import os
import signal
import sys
import time
from pathlib import Path



def _descends_from(pid: int, ancestor: int) -> bool:
    while pid > 1:
        if pid == ancestor:
            return True
        try:
            stat = Path(f"/proc/{pid}/stat").read_text()
        except OSError:
            return False
        # the parent pid follows the parenthesized command name
        pid = int(stat.rsplit(")", 1)[1].split()[1])
    return False


def stop_leftover_processes(directory: Path) -> list:
    """Stops the processes an earlier run left in 'directory' (a mysqld or
    binlog_server whose command line names a path in it) and returns their
    command lines. A harness killed by a timeout leaves its servers running;
    deleting the directory under them frees nothing, and a mysqld whose data
    directory is gone logs InnoDB errors in a loop (on 2026-11-06 one wrote
    a 99 GB error log in three days and filled the disk). Processes of this
    run (descendants of this process) and its ancestors (a wrapper such as
    timeout, or the nightly runner) are left alone. Linux only (/proc)."""
    proc = Path("/proc")
    if not proc.is_dir():
        return []
    prefixes = {str(directory).rstrip("/") + "/",
                str(directory.resolve()).rstrip("/") + "/"}
    found = {}
    for entry in proc.iterdir():
        if not entry.name.isdigit():
            continue
        pid = int(entry.name)
        try:
            arguments = (entry / "cmdline").read_bytes().split(b"\0")
        except OSError:
            continue
        line = " ".join(argument.decode(errors="replace")
                        for argument in arguments if argument)
        if (any(prefix in line for prefix in prefixes) and
                not _descends_from(pid, os.getpid()) and
                not _descends_from(os.getpid(), pid)):
            found[pid] = line
    for pid in found:
        try:
            os.kill(pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
    deadline = time.monotonic() + 30
    for pid in found:
        while time.monotonic() < deadline and Path(f"/proc/{pid}").exists():
            time.sleep(0.2)
        try:
            os.kill(pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
    for line in found.values():
        print(f"stopped a leftover process of an earlier run: {line[:300]}",
              file=sys.stderr)
    return list(found.values())
