"""Read-only theme-radar raw Finviz exports for a rebuilt morning.

Morning D uses the previous session's ``YYYY-MM-DD.raw.csv``. The cleaned
``.csv`` has no Earnings Date and no Change. Nothing here writes to
theme-radar.
"""
from __future__ import annotations

import hashlib
import io
from datetime import datetime
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

import pandas as pd

ET = ZoneInfo("America/New_York")
RAW_URL = (
    "https://raw.githubusercontent.com/SRoyaltyy/theme-radar/"
    "main/data/snapshots/{day}.raw.csv"
)
MANIFEST_URL = (
    "https://raw.githubusercontent.com/SRoyaltyy/theme-radar/"
    "main/data/snapshots/manifest.json"
)
# Frozen fullscan copies, including the 2026-11-04 morning. These mornings
# are rebuilt from theme-radar's previous-session raw file.
FROZEN_FROM = "2026-10-13"
FROZEN_TO = "2026-11-04"
SOURCE = "theme_radar_snapshot"
REQUIRED = (
    "Ticker",
    "Change",
    "Volume",
    "Average Volume",
    "Market Cap",
    "Industry",
    "Earnings Date",
    "News Title",
)
ALIASES = {
    "Ticker": ("Ticker", "Symbol"),
    "Change": ("Change", "Change %"),
    "Volume": ("Volume",),
    "Average Volume": ("Average Volume", "Avg Volume"),
    "Market Cap": ("Market Cap",),
    "Industry": ("Industry",),
    "Earnings Date": ("Earnings Date", "Earnings"),
    "News Title": ("News Title", "News"),
}


def raw_url(day: str) -> str:
    return RAW_URL.format(day=str(day)[:10])


def previous_session(morning: str) -> str:
    from .finviz_export_guard import previous_session as prev

    return prev(str(morning)[:10])


def _fetch(url: str) -> bytes:
    request = Request(url, headers={"User-Agent": "fullscan-rebuild-readonly"})
    with urlopen(request, timeout=180) as response:
        return response.read()


def _manifest(fetch) -> dict:
    import json
    raw = fetch(MANIFEST_URL)
    doc = json.loads(raw.decode("utf-8"))
    return doc if isinstance(doc, dict) else {}


def scrape_ts_utc(prior: str, manifest: dict) -> str:
    found = ""
    for run in manifest.get("runs") or []:
        if not isinstance(run, dict):
            continue
        if str(run.get("date") or "")[:10] != str(prior)[:10]:
            continue
        stamp = str(run.get("scrape_ts_utc") or "").strip()
        if stamp:
            found = stamp
    return found


def before_morning_open(stamp: str, morning: str) -> bool:
    """True when ``stamp`` is strictly before 09:30 ET on ``morning``."""
    text = str(stamp or "").strip()
    if not text:
        return False
    try:
        when = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return False
    if when.tzinfo is None:
        when = when.replace(tzinfo=ET)
    local = when.astimezone(ET)
    year, month, day = (int(part) for part in str(morning)[:10].split("-"))
    bell = datetime(year, month, day, 9, 30, tzinfo=ET)
    return local < bell


def _rename(frame: pd.DataFrame) -> pd.DataFrame:
    rename = {}
    have = set(frame.columns)
    for canonical, names in ALIASES.items():
        if canonical in have:
            continue
        for name in names:
            if name in have:
                rename[name] = canonical
                break
    if rename:
        frame = frame.rename(columns=rename)
    missing = [name for name in REQUIRED if name not in frame.columns]
    if missing:
        raise ValueError("theme-radar raw missing columns: " + ", ".join(missing))
    return frame


def load_prior_raw(morning: str, *, fetch=None, manifest: dict | None = None) -> dict:
    """Bytes, sha256, and frame of D-1.raw.csv for morning D.

    Refuses D's own file and a scrape that is not before 09:30 ET on D.
    """
    day = str(morning or "")[:10]
    prior = previous_session(day)
    if prior >= day:
        raise ValueError(f"prior session {prior} is not before {day}")
    url = raw_url(prior)
    if url.endswith(f"/{day}.raw.csv"):
        raise ValueError(f"refusing morning {day}'s own raw file")
    getter = fetch or _fetch
    if manifest is None:
        manifest = _manifest(getter)
    stamp = scrape_ts_utc(prior, manifest)
    if not before_morning_open(stamp, day):
        raise ValueError(
            f"theme-radar {prior}.raw.csv scrape_ts_utc={stamp or 'missing'} "
            f"is not before 09:30 ET on {day}"
        )
    try:
        raw = getter(url)
    except Exception as exc:
        raise ValueError(
            f"theme-radar {prior}.raw.csv is missing ({type(exc).__name__})"
        ) from exc
    if not raw:
        raise ValueError(f"theme-radar {prior}.raw.csv is missing (empty)")
    frame = _rename(pd.read_csv(io.BytesIO(raw), low_memory=False))
    return {
        "source": SOURCE,
        "source_file": f"data/snapshots/{prior}.raw.csv",
        "source_url": url,
        "source_session": prior,
        "source_sha256": hashlib.sha256(raw).hexdigest(),
        "source_scrape_ts_utc": stamp,
        "n_rows": int(len(frame)),
        "frame": frame,
    }


def require_prior_raw(morning: str, *, fetch=None,
                      manifest: dict | None = None) -> dict:
    """Exact ``D-1.raw.csv`` or a loud stop.

    A missing file, a late scrape, or a bad header does not search an
    older session. The only snapshot URL this builds is that one file.
    """
    day = str(morning or "")[:10]
    prior = previous_session(day)
    exact = f"data/snapshots/{prior}.raw.csv"
    try:
        loaded = load_prior_raw(day, fetch=fetch, manifest=manifest)
    except Exception as exc:
        message = (
            f"[finviz] FAIL {day} theme-radar {exact} is not usable "
            f"({exc}). Stopping. Will not use an older snapshot."
        )
        print(message, flush=True)
        raise SystemExit(message) from exc
    if loaded.get("source_file") != exact or loaded.get("source_session") != prior:
        message = (
            f"[finviz] FAIL {day} refused a snapshot other than {exact}. "
            f"Stopping. Will not use an older snapshot."
        )
        print(message, flush=True)
        raise SystemExit(message)
    return loaded


def applies(morning: str) -> bool:
    day = str(morning or "")[:10]
    return FROZEN_FROM <= day <= FROZEN_TO
