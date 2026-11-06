"""Chart end dates against the source each line is allowed to use.

A later session is appended only when that source already has a mark.
A missed morning with holdings is not treated as a flat day. A source
that does not exist is named, not filled in.
"""
from __future__ import annotations

import csv
import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def _read(path: Path) -> str:
    if not path.is_file():
        return ""
    return path.read_text(encoding="utf-8", errors="replace")


def _csv_last(path: Path) -> str:
    last = ""
    if not path.is_file():
        return last
    with path.open(encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            day = (row.get("date") or "")[:10]
            if len(day) == 10 and day > last:
                last = day
    return last


def _jsonl_last(path: Path) -> str:
    last = ""
    if not path.is_file():
        return last
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        day = str(row.get("date") or "")[:10]
        if len(day) == 10 and day > last:
            last = day
    return last


def _board_series() -> dict[str, str]:
    html = _read(ROOT / "dashboard/strategy-board/index.html")
    match = re.search(r"const D = (\{.*?\});\s*\n", html)
    if not match:
        return {}
    try:
        payload = json.loads(match.group(1))
    except json.JSONDecodeError:
        return {}
    out = {}
    for series in payload.get("series") or []:
        pts = series.get("pts") or []
        out[str(series.get("id") or "")] = str(pts[-1].get("date") or "") if pts else ""
    return out


def _preopen_last() -> dict[str, str]:
    path = ROOT / "dashboard/factor-mine/preopen.json"
    if not path.is_file():
        return {}
    try:
        doc = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    out = {}
    for name, sleeve in (doc.get("sleeves") or {}).items():
        days = sleeve.get("days") or []
        out[name] = str(days[-1].get("date") or "") if days else ""
    return out


def _log_equity_days(path: Path) -> dict[str, bool]:
    """True when that session has an equity mark."""
    if not path.is_file():
        return {}
    try:
        rows = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    out: dict[str, bool] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        day = str(row.get("date") or "")[:10]
        if len(day) != 10:
            continue
        marked = row.get("equity_primary") is not None and row.get("kind") in (
            "mark", "close", "session", "fill",
        )
        out[day] = out.get(day, False) or marked
    return out


def _updown_last() -> str:
    path = ROOT / "dashboard/updown/data/index.json"
    if not path.is_file():
        return ""
    try:
        doc = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return ""
    days = doc.get("days") or []
    if not days or not isinstance(days[-1], list) or not days[-1]:
        return ""
    return str(days[-1][0])[:10]


def _forward_files() -> list[str]:
    folder = ROOT / "research/updown_o2c_v2/forward"
    if not folder.is_dir():
        return []
    return sorted(path.stem for path in folder.glob("*.json") if len(path.stem) == 10)


def coverage() -> list[dict]:
    """One row per published line. ``blocker`` is empty when the chart matches its source."""
    board = _board_series()
    paper = _csv_last(ROOT / "data/paper/equity_curve.csv")
    mover = _csv_last(ROOT / "data/mover_paper/equity_curve.csv")
    book = _csv_last(ROOT / "data/book_paper/equity_curve.csv")
    pre = _preopen_last()
    hold = _log_equity_days(ROOT / "dashboard/holdup/log.json")
    h1 = _log_equity_days(ROOT / "dashboard/h1/log.json")
    webull = _jsonl_last(ROOT / "data/webull_sim/days.jsonl")
    updown = _updown_last()
    forwards = _forward_files()
    forward_last = forwards[-1] if forwards else ""
    rows = [
        {
            "strategy": "strategy-board io_2w_size",
            "chart": board.get("io_2w_size", ""),
            "source": paper,
            "blocker": "" if board.get("io_2w_size") == paper else "chart behind paper equity_curve.csv",
        },
        {
            "strategy": "strategy-board mover_paper_live",
            "chart": board.get("mover_paper_live", ""),
            "source": mover,
            "blocker": "" if board.get("mover_paper_live") == mover else "chart behind mover equity_curve.csv",
        },
        {
            "strategy": "strategy-board book_paper_1w",
            "chart": board.get("book_paper_1w", ""),
            "source": book,
            "blocker": "" if board.get("book_paper_1w") == book else "chart behind book equity_curve.csv",
        },
        {
            "strategy": "factor-mine preopen holdup",
            "chart": pre.get("union_hot_n4_holdup_preopen", ""),
            "source": pre.get("union_hot_n4_holdup_preopen", ""),
            "blocker": (
                "2026-11-05 pre-open book is not sealed; that morning stays missing"
                if pre.get("union_hot_n4_holdup_preopen", "") < "2026-11-05" else ""
            ),
        },
        {
            "strategy": "factor-mine preopen h1",
            "chart": pre.get("union_hot_n4_h1_preopen", ""),
            "source": pre.get("union_hot_n4_h1_preopen", ""),
            "blocker": (
                "2026-11-05 pre-open book is not sealed; that morning stays missing"
                if pre.get("union_hot_n4_h1_preopen", "") < "2026-11-05" else ""
            ),
        },
        {
            "strategy": "holdup",
            "chart": max(hold) if hold else "",
            "source": max(hold) if hold else "",
            "blocker": "",
        },
        {
            "strategy": "h1",
            "chart": max((day for day, marked in h1.items() if marked), default=""),
            "source": max(h1) if h1 else "",
            "blocker": "",
        },
        {
            "strategy": "webull-sim",
            "chart": webull,
            "source": webull,
            "blocker": (
                "no final sim row after 2026-10-28; a missed earlier day stays missing "
                "and is not backfilled"
                if webull <= "2026-10-28" else ""
            ),
        },
        {
            "strategy": "updown o2c",
            "chart": updown,
            "source": forward_last or updown,
            "blocker": (
                "no research/updown_o2c_v2/forward file after "
                f"{forward_last or updown}; 2026-10-07 is not scored"
                if not any(day > updown for day in forwards) else ""
            ),
        },
    ]
    # Plan-only mornings are part of the h1 report, not a fake equity.
    for day in ("2026-11-02", "2026-11-04"):
        rows.append({
            "strategy": f"h1 {day}",
            "chart": "plan-only" if h1.get(day) is False else ("marked" if h1.get(day) else "absent"),
            "source": "plan",
            "blocker": "" if h1.get(day) is False else "expected a plan with no equity mark",
        })
    return rows
