"""Refuse writes of an earlier session's peer RS or AB checklist.

A run for date R may rewrite ``data/peers/R_peer_rs.csv`` and
``data/ab_checklist/R_*``. It must not replace (or create) those files
for a date d < R. That is how an older Finviz tape got saved as a
finished prior day.
"""
from __future__ import annotations

import re
from pathlib import Path

_DATED = re.compile(r"^(\d{4}-\d{2}-\d{2})_")


def _file_date(path: Path) -> str | None:
    m = _DATED.match(path.name)
    return m.group(1) if m else None


def _guarded(path: Path) -> bool:
    """Peer RS csv and any dated file under data/ab_checklist."""
    d = _file_date(path)
    if not d:
        return False
    parts = set(path.parts)
    if "peers" in parts and path.name.endswith("_peer_rs.csv"):
        return True
    if "ab_checklist" in parts:
        return True
    return False


def refuse_past_overwrite(path: Path | str, run_date: str | None) -> bool:
    """True when the write must not happen.

    Logs one line and leaves the file untouched. Same-day writes are
    allowed. Paths outside the two trees are ignored.
    """
    path = Path(path)
    run = str(run_date or "")[:10]
    d = _file_date(path)
    if not run or not d or not _guarded(path) or d >= run:
        return False
    print(
        f"[past-write] REFUSE {path.name} — file date {d} < run date {run} "
        f"(not overwritten)",
        flush=True,
    )
    return True
