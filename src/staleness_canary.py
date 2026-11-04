"""Compare published dates with the latest completed NYSE session.

Each source is read on main (this checkout) and on the live Pages site.
Allowed lag is 0 sessions except where a source says otherwise. A missing
date is over the limit. The report is ``data/health/staleness.json``.

CLI: python -m src.staleness_canary
Exit 0 when every source is inside its lag. Exit 1 otherwise.
"""
from __future__ import annotations

import argparse
import csv
import io
import json
import urllib.error
import urllib.request
from datetime import date, datetime, timedelta
from pathlib import Path

from src.pages_publish_gate import data_asof_from_html, latest_completed_session
from src.skip_if_good import _session_date

ROOT = Path(__file__).resolve().parent.parent
PAGES = "https://sroyaltyy.github.io/fullscan"
REPORT = Path("data/health/staleness.json")
COVERAGE = Path(__file__).resolve().parent / "staleness_coverage.json"
_MISSING_LAG = 10_000

# path, kind, allowed session lag
SOURCES = (
    ("dashboard/index.html", "html", 0),
    ("data/webull_sim/days.jsonl", "jsonl", 1),
    ("dashboard/h1/closes.json", "closes", 0),
    ("data/book_paper/equity_curve.csv", "csv", 0),
    ("data/paper/equity_curve.csv", "csv", 0),
    ("dashboard/factor-mine/preopen.json", "json", 0),
    ("data/day_board/latest.json", "json", 0),
    ("dashboard/jev-train/days.json", "jev_days", 1),
    ("dashboard/factor-mine/strategy_tickets.json", "json", 0),
)


def sessions_behind(asof: str, session: str) -> int:
    """NYSE sessions ``session`` is ahead of ``asof``. 0 when asof covers it."""
    if not asof:
        return _MISSING_LAG
    try:
        start = date.fromisoformat(str(asof)[:10])
        end = date.fromisoformat(str(session)[:10])
    except ValueError:
        return _MISSING_LAG
    if start >= end:
        return 0
    count = 0
    cursor = start
    while cursor < end:
        cursor += timedelta(days=1)
        if _session_date(cursor):
            count += 1
    return count


def _day(value: object) -> str:
    text = str(value or "")[:10]
    if len(text) == 10 and text[4] == "-" and text[7] == "-":
        return text
    return ""


def _csv_max_date(text: str) -> str:
    try:
        rows = list(csv.DictReader(io.StringIO(text)))
    except csv.Error:
        return ""
    days = [_day(row.get("date")) for row in rows]
    days = [day for day in days if day]
    return max(days) if days else ""


def _jsonl_max_date(text: str) -> str:
    days: list[str] = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(row, dict):
            day = _day(row.get("date"))
            if day:
                days.append(day)
    return max(days) if days else ""


def _walk_dates(node: object, found: list[str]) -> None:
    if isinstance(node, dict):
        for key, value in node.items():
            if key == "date":
                day = _day(value)
                if day:
                    found.append(day)
            else:
                _walk_dates(value, found)
    elif isinstance(node, list):
        for item in node:
            _walk_dates(item, found)


def _json_max_date(text: str) -> str:
    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        return ""
    found: list[str] = []
    _walk_dates(data, found)
    return max(found) if found else ""


def _closes_max_date(text: str) -> str:
    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        return ""
    if not isinstance(data, dict):
        return ""
    days = [_day(key) for key in data]
    days = [day for day in days if day]
    return max(days) if days else ""


def _jev_days_max(text: str) -> str:
    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        return ""
    rows = data.get("days") if isinstance(data, dict) else None
    if not isinstance(rows, list):
        return ""
    days = [_day(row.get("date")) for row in rows if isinstance(row, dict)]
    days = [day for day in days if day]
    return max(days) if days else ""


def newest_date(text: str, kind: str) -> str:
    if not text:
        return ""
    if kind == "html":
        return data_asof_from_html(text)
    if kind == "csv":
        return _csv_max_date(text)
    if kind == "jsonl":
        return _jsonl_max_date(text)
    if kind == "closes":
        return _closes_max_date(text)
    if kind == "jev_days":
        return _jev_days_max(text)
    if kind == "json":
        return _json_max_date(text)
    return ""


def _read_text(path: Path) -> str:
    if not path.is_file():
        return ""
    return path.read_text(encoding="utf-8", errors="replace")


def fetch_url(url: str, timeout: float = 30) -> str:
    request = urllib.request.Request(
        url, headers={"User-Agent": "fullscan-staleness-canary"},
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return response.read().decode("utf-8", errors="replace")


def manual_sources() -> list[dict]:
    """Pages with no weekday cron. They stay in the report and do not fail it."""
    if not COVERAGE.is_file():
        return []
    try:
        doc = json.loads(COVERAGE.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return []
    rows = doc.get("sources") if isinstance(doc, dict) else None
    if not isinstance(rows, list):
        return []
    return [row for row in rows if isinstance(row, dict) and row.get("mode") == "manual"]


def evaluate(*, root: Path | None = None, now: datetime | None = None,
             fetch=None, pages_base: str = PAGES) -> dict:
    root = Path(root) if root is not None else ROOT
    fetch = fetch or fetch_url
    session = latest_completed_session(now)
    rows = []
    for path, kind, limit in SOURCES:
        main_text = _read_text(root / path)
        live_text = ""
        live_error = ""
        url = pages_base.rstrip("/") + "/" + path
        try:
            live_text = fetch(url)
        except (OSError, urllib.error.URLError, TimeoutError, ValueError) as exc:
            live_error = str(exc)
        main_date = newest_date(main_text, kind)
        live_date = newest_date(live_text, kind)
        main_lag = sessions_behind(main_date, session)
        live_lag = sessions_behind(live_date, session)
        ok = main_lag <= limit and live_lag <= limit
        rows.append({
            "path": path,
            "lag_limit": limit,
            "main_date": main_date,
            "live_date": live_date,
            "main_lag": main_lag,
            "live_lag": live_lag,
            "live_error": live_error,
            "ok": ok,
        })
    for spec in manual_sources():
        rows.append({
            "path": spec.get("path") or spec.get("page") or "",
            "page": spec.get("page") or "",
            "lag_limit": None,
            "mode": "manual",
            "main_date": str(spec.get("last_run") or "")[:10],
            "live_date": "",
            "main_lag": 0,
            "live_lag": 0,
            "live_error": "",
            "ok": True,
            "note": spec.get("reason") or "updated by hand",
        })
    report = {
        "generated_at": datetime.now().astimezone().isoformat(),
        "session": session,
        "ok": all(row["ok"] for row in rows),
        "sources": rows,
    }
    dest = root / REPORT
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    return report


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", default="")
    args = parser.parse_args()
    root = Path(args.root) if args.root else ROOT
    report = evaluate(root=root)
    print(f"[staleness] session {report['session']} ok={report['ok']}")
    for row in report["sources"]:
        if row.get("mode") == "manual":
            print(
                f"[staleness] MANUAL {row['path']} "
                f"last_run={row['main_date'] or 'missing'} updated by hand"
            )
            continue
        flag = "OK" if row["ok"] else "STALE"
        print(
            f"[staleness] {flag} {row['path']} "
            f"main={row['main_date'] or 'missing'} lag={row['main_lag']} "
            f"live={row['live_date'] or 'missing'} lag={row['live_lag']} "
            f"limit={row['lag_limit']}"
        )
    if not report["ok"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
