"""Page list loaded by ``src.staleness_canary`` (PR #743 is on main).

``sources_for_canary`` returns daily pages that are not already in the
canary base list. Manual/frozen pages, and jev-train (already listed,
lag 1, weekday cron from #743), are omitted so they are not appended
twice and do not fail the canary. ``updown_days`` reads ``days[-1][0]``.
"""
from __future__ import annotations

import json
from pathlib import Path

COVERAGE_PATH = Path(__file__).resolve().parent / "staleness_coverage.json"


def load_coverage(path: Path | None = None) -> dict:
    dest = Path(path) if path is not None else COVERAGE_PATH
    data = json.loads(dest.read_text(encoding="utf-8"))
    if not isinstance(data, dict) or not isinstance(data.get("pages"), list):
        raise ValueError("staleness coverage file has no pages list")
    return data


def pages(path: Path | None = None) -> list[dict]:
    return list(load_coverage(path)["pages"])


def sources_for_canary(path: Path | None = None) -> tuple[tuple[str, str, int], ...]:
    """(path, kind, lag_limit) for pages a daily writer owns.

    Manual/frozen pages are omitted. A null lag is not a number the
    canary can compare.
    """
    rows = []
    for page in pages(path):
        if page.get("manual") or page.get("frozen") or page.get("already_in_canary"):
            continue
        lag = page.get("lag_limit")
        if not isinstance(lag, int):
            continue
        rows.append((str(page["path"]), str(page["kind"]), lag))
    return tuple(rows)


def newest_updown_day(text: str) -> str:
    """Newest session in updown_o2c_v2 index.json (``days[-1][0]``)."""
    if not text:
        return ""
    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        return ""
    days = data.get("days") if isinstance(data, dict) else None
    if not isinstance(days, list) or not days:
        return ""
    last = days[-1]
    if isinstance(last, list) and last:
        day = str(last[0])[:10]
        if len(day) == 10 and day[4] == "-" and day[7] == "-":
            return day
    return ""
