"""Last line of defense for the event scanner: the day must NEVER end
with an empty events file.

Order of defense (this module is step 3):
  1. run_events          — primary scan (+ JSON repair pass)
  2. run_events_catcher  — gap hunt, or full REPLACEMENT if primary empty
  3. events_fallback     — if both produced nothing (API outage, double
                           parse failure), carry forward the most recent
                           previous day's still-live events, marked
                           status=carried, so downstream consumers
                           (weather, stock book sector tilt) keep working
                           on slightly stale but real data instead of
                           nothing.

Carried days are visibly marked in the JSON (`carried_from`) and the MD,
and the orchestrator's green-empty check still re-dispatches a real scan
later in the morning — this fallback just removes the zero-events window.

CLI: python -m src.events_fallback [--date YYYY-MM-DD]
"""
from __future__ import annotations

import argparse
import json
import os
from datetime import datetime
from zoneinfo import ZoneInfo

from . import config
from .event_context import EVENTS_DIR

MAX_CARRY_DAYS = 5


def _load(path: str) -> dict | None:
    try:
        with open(path, encoding="utf-8") as fh:
            return json.load(fh)
    except (OSError, ValueError):
        return None


def ensure(date_str: str) -> bool:
    """Returns True if the dated events file has events after this call."""
    json_path = os.path.join(EVENTS_DIR, f"{date_str}_events.json")
    cur = _load(json_path) or {}
    if cur.get("events"):
        print(f"[events-fallback] {date_str}: {len(cur['events'])} events present — nothing to do")
        return True

    # find the most recent previous dated file that has events
    try:
        names = sorted(os.listdir(EVENTS_DIR))
    except OSError:
        names = []
    candidates = [
        n[:10] for n in names
        if n.endswith("_events.json") and n[:10] < date_str
    ]
    src_date, src = None, None
    for d in reversed(candidates):
        delta = (datetime.fromisoformat(date_str) - datetime.fromisoformat(d)).days
        if delta > MAX_CARRY_DAYS:
            break
        data = _load(os.path.join(EVENTS_DIR, f"{d}_events.json"))
        if data and data.get("events"):
            src_date, src = d, data
            break
    if not src:
        print(f"[events-fallback] {date_str}: no prior events within "
              f"{MAX_CARRY_DAYS}d to carry — leaving empty")
        return False

    events = []
    for e in src["events"]:
        e = dict(e)
        e["status"] = "carried"
        events.append(e)
    payload = {
        "scan_date": date_str,
        "events": events,
        "carried_from": src_date,
        "summary": f"CARRIED FORWARD from {src_date}: both scan passes failed "
                   f"for {date_str}; these are the previous still-live events.",
    }
    os.makedirs(EVENTS_DIR, exist_ok=True)
    with open(json_path, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2, ensure_ascii=False)
    with open(os.path.join(EVENTS_DIR, "latest.json"), "w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2, ensure_ascii=False)

    md = [
        f"# Event Scan — {date_str} (CARRIED FORWARD from {src_date})",
        "",
        f"⚠️ Both scan passes failed today; the {len(events)} events below are",
        f"carried from {src_date}. The orchestrator will re-dispatch a real scan.",
        "",
    ]
    for e in events:
        md.append(f"- **{e.get('title', '?')}** ({e.get('date_or_window', '?')}) — "
                  f"{e.get('category', '?')}; {', '.join(e.get('sectors') or [])}")
    md_text = "\n".join(md) + "\n"
    with open(os.path.join(EVENTS_DIR, f"{date_str}_events.md"), "w",
              encoding="utf-8") as fh:
        fh.write(md_text)
    with open(os.path.join(EVENTS_DIR, "latest.md"), "w", encoding="utf-8") as fh:
        fh.write(md_text)
    print(f"[events-fallback] {date_str}: carried {len(events)} events from {src_date}")
    return True


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=None)
    args = ap.parse_args()
    date_str = args.date or datetime.now(ZoneInfo(config.TZ)).date().isoformat()
    ensure(date_str)


if __name__ == "__main__":
    main()
