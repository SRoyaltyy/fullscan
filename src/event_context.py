"""Shared helper: inject the latest event scan into any workflow prompt.

The event scanner (run_events.py) writes 01_daily/events/latest.md every
day. Any stage that wants market-moving-event background context calls
event_context.block() and splices the returned text into its user message.
Returns an empty string when no scan exists yet, so callers behave
exactly as before until the first scan lands.
"""
from __future__ import annotations

import os

EVENTS_DIR = os.path.join("01_daily", "events")


def block(max_chars: int = 6000) -> str:
    path = os.path.join(EVENTS_DIR, "latest.md")
    if not os.path.exists(path):
        return ""
    try:
        with open(path, encoding="utf-8") as fh:
            text = fh.read()
    except OSError:
        return ""
    if len(text) > max_chars:
        text = text[:max_chars] + "\n...(truncated; full scan in 01_daily/events/)"
    return ("=== EVENT SCANNER (market-moving events: past 2 weeks / today / "
            "next 2 weeks — background context, not a price prediction) ===\n"
            + text + "\n=== END EVENT SCANNER ===\n")
