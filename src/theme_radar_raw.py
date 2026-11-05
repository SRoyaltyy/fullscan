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
# First raw snapshot. Every trading day from this date through 2026-11-04
# has a .raw.csv except 2026-08-27. Morning D reads that exact D-1 file.
EARLIEST_RAW = "2026-08-06"
MISSING_RAW = frozenset({"2026-08-27"})
NO_PRIOR_BEFORE = "no D-1 snapshot before 08-06"
NO_PRIOR_0827 = "no D-1 snapshot (2026-08-27.raw.csv is missing)"
# Still-broken mornings start here. 2026-08-07 is the first morning with a D-1 file.
REBUILD_FROM = "2026-08-13"
# Local exports from this morning onward were byte copies. 2026-11-04
# is a separate rebuilt day and stays outside this window.
FROZEN_FROM = "2026-10-13"
FROZEN_TO = "2026-11-03"
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


def _commit_times(day: str) -> list[str]:
    """Commit times that touched this exact raw file. Read-only."""
    import json
    url = (
        "https://api.github.com/repos/SRoyaltyy/theme-radar/commits"
        f"?path=data/snapshots/{day}.raw.csv&sha=main&per_page=20"
    )
    try:
        raw = _fetch(url)
    except Exception:
        return []
    try:
        rows = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return []
    times = []
    if not isinstance(rows, list):
        return times
    for row in rows:
        if not isinstance(row, dict):
            continue
        commit = row.get("commit") or {}
        when = str(((commit.get("committer") or {}).get("date")) or "").strip()
        if when:
            times.append(when)
    return times


def load_prior_raw(morning: str, *, fetch=None, manifest: dict | None = None,
                   commit_times: list[str] | None = None) -> dict:
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
    if not stamp:
        times = list(commit_times) if commit_times is not None else _commit_times(prior)
        late = [item for item in times if not before_morning_open(item, day)]
        if late:
            raise ValueError(
                f"theme-radar {prior}.raw.csv commit {late[0]} "
                f"is not before 09:30 ET on {day}"
            )
        if times:
            stamp = min(times)
    if stamp and not before_morning_open(stamp, day):
        raise ValueError(
            f"theme-radar {prior}.raw.csv scrape_ts_utc={stamp} "
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
    if not stamp:
        raise ValueError(
            f"theme-radar {prior}.raw.csv has no scrape time and no commit"
        )
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


def prior_gap(morning: str) -> str | None:
    """Why morning D has no exact prior raw file, or None when D-1 exists.

    2026-08-28 is the session after the missing 2026-08-27 file. Mornings
    before 2026-08-07 have no earlier snapshot. Neither case borrows
    another day.
    """
    prior = previous_session(str(morning or "")[:10])
    if prior < EARLIEST_RAW:
        return NO_PRIOR_BEFORE
    if prior in MISSING_RAW:
        return NO_PRIOR_0827
    return None


def applies(morning: str) -> bool:
    """True when morning D should read its exact prior raw snapshot.

    2026-11-04 stays outside this window. A missing D-1 file does not
    fall through to an older snapshot.
    """
    day = str(morning or "")[:10]
    if day > FROZEN_TO:
        return False
    return prior_gap(day) is None
