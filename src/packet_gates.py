"""Hard gates so conflict stubs cannot look like a finished packet.

2026-09-09: land printed ok=True (2B) because QCResult.size was len('ok'),
then rebase kept a stub JSON. Weather 10B was len('sectors=11').
JSON under MIN_JSON_BYTES is always FAIL, even if the .md sibling is fat.
"""
from __future__ import annotations

import json
from pathlib import Path

MIN_JSON_BYTES = 80
# Offline/thin weather with 5 unknown sectors is ~200–400B. A morning
# rewrite that actually stamped VIX/yields is kilobytes (09-09 heal: 9KB).
MIN_WEATHER_BYTES = 800


def file_bytes(path: str | Path) -> int:
    try:
        return int(Path(path).stat().st_size)
    except OSError:
        return 0


def json_too_small(path: str | Path) -> str | None:
    n = file_bytes(path)
    if n < MIN_JSON_BYTES:
        return f"too_small({n}B)"
    return None


def load_json(path: str | Path):
    try:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError, json.JSONDecodeError):
        return None


def weather_ok(path: str | Path, min_bytes: int = MIN_JSON_BYTES) -> tuple[bool, str]:
    n = file_bytes(path)
    if n < min_bytes:
        return False, f"too_small({n}B)"
    data = load_json(path)
    if not isinstance(data, dict):
        return False, "unparseable_json"
    secs = ((data.get("signals") or {}).get("sectors") or {})
    if not isinstance(secs, dict) or len(secs) < 5:
        return False, "thin_or_unreadable"
    return True, f"sectors={len(secs)} bytes={n}"
