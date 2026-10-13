#!/usr/bin/env python3
"""Add-only: show the newest Factor Mine session on the calendar even when the
embedded mine data stops a day earlier.

The large calendar draws from the page's embedded data (``window.__FM_D``),
which only moves when the post-close mine publishes. When
``data/day_board/today_strategies.json`` names a later session, this inserts a
display-only "pending" day cell next to the calendar, with the pending-ruling
note from ``PENDING_NOTES``. Nothing in the embedded data, sealed rows,
fingerprints or engine files is changed. Idempotent via MARK.
"""
from __future__ import annotations

import base64
import gzip
import html as H
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MARK = "<!-- fm-pending-day v1 -->"
TODAY = ROOT / "data" / "day_board" / "today_strategies.json"
DEFAULT_NOTE = "pending: not yet mined after the close"
PENDING_NOTES = {
    "2026-10-13": "pending ruling: GLM-written predict, not a decision yet",
}


def embedded_last_date(page: str) -> str:
    m = re.search(r'const B64 = "([^"]+)"', page)
    if not m:
        return ""
    data = json.loads(gzip.decompress(base64.b64decode(m.group(1))))
    dates = data.get("dates") or []
    return max(dates) if dates else ""


def block(day: str, note: str, n_ok, mined: bool = False) -> str:
    extra = f" · {n_ok} strategies OK in today_strategies.json" if n_ok is not None else ""
    return (
        f'{MARK}<div id="fmPendingDay" class="note" data-date="{H.escape(day)}" '
        'style="margin:6px 0;padding:6px 10px;border:1px dashed #c90;border-radius:6px;'
        'background:#fff8e6;color:#704c00">'
        f'<b>Newest day {H.escape(day)}</b> — {H.escape(note)}{H.escape(extra)}. '
        'Display note only; no record changed. '
        + ('The calendar cells for this day are shown below with this label.'
           if mined else 'The calendar below fills in once the post-close mine publishes.')
        + '</div>'
    )


def patch(page: str, today: dict) -> str:
    if MARK in page:
        return page
    day = str(today.get("date") or "")
    last = embedded_last_date(page)
    if not day:
        return page
    mined = bool(last) and day <= last
    if mined and day not in PENDING_NOTES:
        return page
    snippet = block(day, PENDING_NOTES.get(day, DEFAULT_NOTE), today.get("n_ok"), mined)
    anchor = '<div id="ovCalBox"'
    i = page.find(anchor)
    if i < 0:
        i = page.find("<body")
        i = page.find(">", i) + 1 if i >= 0 else 0
    return page[:i] + snippet + page[i:]


def main(argv: list[str]) -> int:
    path = Path(argv[1])
    today_path = Path(argv[2]) if len(argv) > 2 else TODAY
    if not today_path.is_file():
        print("no today_strategies.json; page unchanged")
        return 0
    page = path.read_text(encoding="utf-8")
    out = patch(page, json.loads(today_path.read_text(encoding="utf-8")))
    if out != page:
        path.write_text(out, encoding="utf-8")
        print(f"pending day added to {path}")
    else:
        print("page unchanged")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
