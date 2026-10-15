"""Side file for the morning flatten card (keeps today.json frozen).

2026-10-14 (~09:20 ET) and 2026-10-15 (09:25 ET, commit 9bd8ea4c, run
38740869857) the Sleeve merge live card rewrote
``data/sleeve_merge/today.json`` inside the 03:00-09:40 ET input freeze.
That path is a frozen input (research/input_freeze/tool/freeze_inputs.py
``sleeve_merge`` row), so h1 and Factor Mine were struck both days.

Rule (flatten_robust itself is unchanged; same card, same clock):
- Every card write goes to ``data/sleeve_merge/today_live.json``.
- ``today.json`` is written too, EXCEPT when the card is for today's ET
  date and the clock is 03:00 ET inclusive .. 09:40 ET exclusive. Then
  today.json is left byte-identical.
- Flatten consumers (strategy_tickets, broker execs, dashboard panel)
  read :func:`flatten_card_path`: the live file if it exists, else
  today.json. Outside the freeze both files are written with the same
  bytes, so post-open readers see exactly what they saw before.
- The freeze manifest, decision freeze check, h1 and Factor Mine keep
  reading only ``today.json``; nothing here touches them.
"""
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")
ROOT = Path(__file__).resolve().parent.parent
SLEEVE_DIR = ROOT / "data" / "sleeve_merge"
TODAY_JSON = SLEEVE_DIR / "today.json"
LIVE_JSON = SLEEVE_DIR / "today_live.json"
FREEZE_START = (3, 0)
FREEZE_END = (9, 40)


def _now() -> datetime:
    return datetime.now(ET)


def in_freeze(now: datetime) -> bool:
    et = now.astimezone(ET) if now.tzinfo else now.replace(tzinfo=ET)
    return FREEZE_START <= (et.hour, et.minute) < FREEZE_END


def today_json_frozen(card_date: str, now: datetime | None = None) -> bool:
    now = now or _now()
    et = now.astimezone(ET) if now.tzinfo else now.replace(tzinfo=ET)
    return str(card_date) == et.strftime("%Y-%m-%d") and in_freeze(et)


def write_today(card: dict, now: datetime | None = None,
                today_path: Path | None = None,
                live_path: Path | None = None) -> dict[str, Path]:
    """Write the card. Returns {"live": p} or {"live": p, "today": p}."""
    today_path = today_path or TODAY_JSON
    live_path = live_path or LIVE_JSON
    text = json.dumps(card, indent=2)
    live_path.parent.mkdir(parents=True, exist_ok=True)
    live_path.write_text(text, encoding="utf-8")
    out = {"live": live_path}
    if today_json_frozen(card.get("date", ""), now):
        print(f"[sleeve-live-card] 03:00-09:40 ET freeze: {today_path.name} "
              f"left unchanged; card -> {live_path.name}", flush=True)
        return out
    today_path.write_text(text, encoding="utf-8")
    out["today"] = today_path
    return out


def flatten_card_path(today_path: Path | None = None,
                      live_path: Path | None = None) -> Path:
    today_path = today_path or TODAY_JSON
    live_path = live_path or LIVE_JSON
    return live_path if live_path.is_file() else today_path


def load_flatten_card(today_path: Path | None = None,
                      live_path: Path | None = None) -> dict | None:
    p = flatten_card_path(today_path, live_path)
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
