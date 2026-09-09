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


_UNKNOWN = frozenset({"", "unknown", "n/a", "na", "none", "null"})


def _macro_known(val) -> bool:
    if val is None:
        return False
    if isinstance(val, bool):
        return False
    if isinstance(val, (int, float)):
        return True
    if isinstance(val, str):
        return val.strip().lower() not in _UNKNOWN
    return False


def weather_ok(path: str | Path, min_bytes: int = MIN_WEATHER_BYTES) -> tuple[bool, str]:
    """Reject 2–10B stubs and fat files that never stamped VIX/yields."""
    n = file_bytes(path)
    if n < min_bytes:
        return False, f"too_small({n}B)"
    data = load_json(path)
    if not isinstance(data, dict):
        return False, "unparseable_json"
    sig = data.get("signals") or {}
    if not isinstance(sig, dict):
        sig = {}
    secs = sig.get("sectors") or {}
    if not isinstance(secs, dict) or len(secs) < 5:
        nsec = len(secs) if isinstance(secs, dict) else 0
        return False, f"too_few_sectors({nsec})"
    vix_ok = _macro_known(sig.get("vix")) or _macro_known(sig.get("vix_spot"))
    yld_ok = _macro_known(sig.get("yields")) or _macro_known(sig.get("dgs10_current"))
    if not vix_ok:
        return False, "vix_unknown"
    if not yld_ok:
        return False, "yields_unknown"
    return True, f"sectors={len(secs)} bytes={n} vix={sig.get('vix')} yields={sig.get('yields')}"
