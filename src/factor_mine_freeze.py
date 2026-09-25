"""Append-only point-in-time inputs and decisions for Factor Mine.

A landed session is frozen once:

* ``data/factor_mine/snapshots/{D}.json`` — every row input used to
  decide D, including the heat vintage and the 09:30 open
* ``data/factor_mine/prices/{D}.json`` — prior bars and the session
  open that those inputs were scored from
* ``data/factor_mine/ledgers/{D}.json.gz`` — buy/sell decisions for every
  recipe and every start-date book, plus the end-of-day state the next
  session resumes from. Gzip of the canonical JSON so a full book stays
  under the publish size budget. The manifest hash is the gzip bytes.
* ``data/factor_mine/lineups/{D}.json`` — recipes shown on the dashboard
  for D, each with its creation date. Write-once.
* ``data/factor_mine/candidates/{D}.json`` — morning candidate list,
  count, and the source plus rank that put each name on the list.
  Built from D's locked morning files. Prior-night holdings are not
  an input. Write-once, and the same list is embedded in the snapshot.
* ``data/factor_mine/recipe_created_on.json`` — creation date of every
  recipe. New names can be appended. An existing date cannot change.
* ``data/factor_mine/freeze_manifest.json`` — sha256 of each file

Every snapshot and ledger carries ``code_sha``, the git commit that
built that day. A later rule change is a new recipe version or a
logged ``--restate``. It does not rewrite the old file in place.

Later runs read those files and append the new day. They do not rebuild
earlier dates. ``--restate D`` is the logged correction path.
"""
from __future__ import annotations

import gzip
import hashlib
import json
import math
import os
import subprocess
from datetime import datetime, timedelta
from pathlib import Path

from . import ticker_lookback as tl

ROOT = Path(__file__).resolve().parent.parent
SNAP_DIR = ROOT / "data" / "factor_mine" / "snapshots"
LEDGER_DIR = ROOT / "data" / "factor_mine" / "ledgers"
PRICE_DIR = ROOT / "data" / "factor_mine" / "prices"
MANIFEST_PATH = ROOT / "data" / "factor_mine" / "freeze_manifest.json"
LINEUP_DIR = ROOT / "data" / "factor_mine" / "lineups"
CANDIDATE_DIR = ROOT / "data" / "factor_mine" / "candidates"
CREATED_PATH = ROOT / "data" / "factor_mine" / "recipe_created_on.json"
GUARD_SLOTS = ("snapshots", "ledgers", "lineups", "candidates")
# One table so the external daily checker uses these exact numbers.
# Close (Finviz post-close Price, else theme-radar Price): max(0.5%, $0.02).
# Open and Webull paper fills: max(1%, $0.02).
# Open source: theme-radar ``{D}.raw.csv`` Finviz Open when the latest
# git commit of that file is before the next session's 09:30 ET. The
# lower bound is the after-close workflow, not a second clock check.
# From 2026-09-24 a present scrape_ts must also be before that next
# open. That stamp is the slim ``{D}.csv`` column, or ``scrape_ts_utc``
# in theme-radar ``manifest.json``. It is not read from raw.csv.
# From SNAPSHOT_OPEN_FROM the slim ``{D}.csv`` Open column is next.
# A missing export, a late commit, or a late scrape uses Stooq.
# ``current.csv`` is never a source.
PRICE_CHECK = {
    "close": {"pct": 0.005, "abs": 0.02},
    "open": {"pct": 0.01, "abs": 0.02},
}
# A candidate missing the hot-score lookback (ohlc.INDICATOR_LOOKBACK
# prior closes, or the session print) is unrankable and is not scored.
# The day is held only when that share is strictly above this line.
UNRANKABLE_MAX_SHARE = 0.10
SNAPSHOT_OPEN_FROM = "2026-09-25"
# scrape_ts exists on the slim snapshot and in manifest.json from this day.
SCRAPE_TS_FROM = "2026-09-24"
OPEN_SOURCE_LOG = ROOT / "data" / "factor_mine" / "open_source_log.csv"
LAST_OPEN_SOURCE: dict[str, dict] = {}
THEME_RADAR_SNAPSHOT_URL = (
    "https://raw.githubusercontent.com/SRoyaltyy/theme-radar/"
    "main/data/snapshots/{date}.csv"
)
THEME_RADAR_RAW_URL = (
    "https://raw.githubusercontent.com/SRoyaltyy/theme-radar/"
    "main/data/snapshots/{date}.raw.csv"
)
THEME_RADAR_HASHES_URL = (
    "https://raw.githubusercontent.com/SRoyaltyy/theme-radar/"
    "main/data/snapshots/HASHES.json"
)
THEME_RADAR_MANIFEST_URL = (
    "https://raw.githubusercontent.com/SRoyaltyy/theme-radar/"
    "main/data/snapshots/manifest.json"
)
THEME_RADAR_COMMITS_URL = (
    "https://api.github.com/repos/SRoyaltyy/theme-radar/commits"
)
OPEN_LOG_FIELDS = (
    "date", "source", "commit_sha", "commit_time", "scrape_ts", "note",
)
STOOQ_DAILY_URL = "https://stooq.com/q/d/l/?s={ticker}.us&i=d"
PAPER_OPEN_DIR = ROOT / "data" / "paper_open"
# Tests point this at a temp dir. None uses the vendor / slim / remote lookup.
THEME_RADAR_SNAP_DIR: Path | None = None
_COMMIT_CACHE: dict[str, dict | None] = {}
_HASHES_DOC: dict | None = None
_RADAR_MANIFEST: dict | None = None
# Morning files only. Flatten books and mover buys are prior-night
# outputs and are not a reason a name is on this list.
MORNING_SOURCES = (
    "liquid_tape",
    "yday_gainer",
    "yday_mover",
    "earn_react",
    "overnight",
    "overnight_mega",
    "oppset",
)

_FILL_KEYS = (
    "date", "ticker", "side", "shares", "price", "fees", "pnl",
    "reason", "cash_after", "equity_after",
)


class HoldDay(Exception):
    """Do not land D. A hole would have been written as hot_score 0."""

    def __init__(self, date: str, missing: list[str] | None, reason: str, *,
                 status: str = "held_incomplete", gaps: list | None = None):
        self.date = str(date)
        self.missing = [str(t) for t in (missing or [])][:80]
        self.reason = reason
        self.status = status or "held_incomplete"
        self.gaps = list(gaps or [])
        super().__init__(
            f"hold {self.date}: {self.status}: {reason}; "
            f"n={len(missing or [])} sample={self.missing[:12]}"
        )


class FrozenHistory(Exception):
    """A frozen snapshot or ledger already exists and was not restated."""


def canonical_bytes(obj) -> bytes:
    return json.dumps(
        obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
        default=_json_default,
    ).encode("utf-8")


def encode_frozen(slot: str, obj) -> bytes:
    """Bytes written for a frozen slot. Ledgers are gzip (mtime 0)."""
    raw = canonical_bytes(obj)
    if slot != "ledgers":
        return raw
    return gzip.compress(raw, compresslevel=6, mtime=0)


def code_sha() -> str:
    """Git commit that is building this frozen day."""
    try:
        out = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=ROOT, capture_output=True, text=True, check=False,
        )
    except OSError:
        return "unknown"
    sha = (out.stdout or "").strip()
    if out.returncode != 0 or len(sha) < 7:
        return "unknown"
    return sha


def _json_default(obj):
    item = getattr(obj, "item", None)
    if callable(item):
        try:
            value = item()
        except Exception:
            value = None
        else:
            if isinstance(value, float) and not math.isfinite(value):
                return None
            return value
    if isinstance(obj, float):
        return None if not math.isfinite(obj) else obj
    return str(obj)


def sha256_bytes(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _now() -> str:
    return datetime.now(tl.ET).isoformat()


def load_manifest() -> dict:
    if not MANIFEST_PATH.is_file():
        return {
            "version": 1,
            "first_frozen": None,
            "snapshots": {},
            "ledgers": {},
            "prices": {},
            "restatements": [],
        }
    try:
        doc = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        doc = {}
    doc.setdefault("version", 1)
    doc.setdefault("first_frozen", None)
    doc.setdefault("snapshots", {})
    doc.setdefault("ledgers", {})
    doc.setdefault("prices", {})
    doc.setdefault("restatements", [])
    return doc


def save_manifest(doc: dict) -> None:
    snaps = doc.get("snapshots") or {}
    doc["first_frozen"] = min(snaps) if snaps else None
    MANIFEST_PATH.parent.mkdir(parents=True, exist_ok=True)
    MANIFEST_PATH.write_text(
        json.dumps(doc, indent=2, sort_keys=True), encoding="utf-8")


def snapshot_path(date: str) -> Path:
    return SNAP_DIR / f"{date}.json"


def ledger_path(date: str) -> Path:
    return LEDGER_DIR / f"{date}.json.gz"


def price_path(date: str) -> Path:
    return PRICE_DIR / f"{date}.json"


def lineup_path(date: str) -> Path:
    return LINEUP_DIR / f"{date}.json"


def candidate_path(date: str) -> Path:
    return CANDIDATE_DIR / f"{date}.json"


def slot_path(slot: str, date: str) -> Path:
    if slot == "ledgers":
        return ledger_path(date)
    if slot == "lineups":
        return lineup_path(date)
    if slot == "prices":
        return price_path(date)
    if slot == "candidates":
        return candidate_path(date)
    return snapshot_path(date)


def reset_price_memory() -> None:
    """Drop OHLC caches so a fetch just landed is visible to features()."""
    from . import candle_factor as cf
    from . import factor_mine as fm

    tl.reset_price_caches()
    cf._TICKER_BARS = None
    try:
        cf._bars_before.cache_clear()
    except Exception:
        pass
    fm._OHLC_CACHE.clear()
    fm._CANDLE_CACHE.clear()
    fm._SCAN_CACHE.clear()


def ranking_universe(date: str, cal: list[str], plan: dict,
                     movers: dict) -> list[str]:
    """Names whose bars must exist before D is ranked or written.

    Includes the liquid tape ``liquid_hot`` / ``continuation`` score,
    not only the names that survive a ranking done with empty bars.
    """
    from . import factor_mine as fm
    from . import gainer_asof as ga
    from . import gainer_capture as gc
    from . import oppset_clock_b as opp

    look = gc.lookback_calendar(cal)
    prior = gc.knowable_export_date(look, date)
    nxt = gc.next_session(cal, date)
    names: set[str] = set()
    for t in (plan or {}).get("tickers") or []:
        tick = fm._tick(t)
        if tick:
            names.add(tick)
    if prior:
        df = ga.load_finviz(prior)
        for raw in ga._liquid_tape(
            df, top_n=0, min_change=0.0, liquid=True,
            min_mcap_m=None, side="up", skip_change=True,
        ):
            tick = fm._tick((raw or {}).get("ticker"))
            if tick:
                names.add(tick)
        for raw in ga.liquid_gainers(df, top_n=60, min_change=0.0, liquid=True):
            tick = fm._tick((raw or {}).get("ticker"))
            if tick:
                names.add(tick)
        for t in gc.yesterday_gainers(prior, top_n=25):
            names.add(fm._tick(t))
        for t in gc.yesterday_movers(prior, top_n=20):
            names.add(fm._tick(t))
        for t in gc.earnings_reaction(prior, date):
            names.add(fm._tick(t))
    for t in gc.overnight_scheduled(prior, date, nxt):
        names.add(fm._tick(t))
    for t in gc.overnight_scheduled(
        prior, date, nxt, min_mcap_m=gc.OVERNIGHT_MEGA_MCAP_M,
    ):
        names.add(fm._tick(t))
    for t in (movers or {}).get(date) or []:
        names.add(fm._tick(t))
    if opp.union_enabled():
        for t in opp.flagged_tickers(date, top_n=30):
            names.add(fm._tick(t))
    names.discard("")
    return sorted(names)


def _raw_bars(ticker: str) -> list[dict]:
    """Stored prints for one ticker, oldest first. Not split-adjusted."""
    from . import candle_factor as cf

    return list(cf._ticker_bars().get(str(ticker or "").strip().upper()) or [])


def completeness_gaps(ticker: str, date: str) -> list[str]:
    """Holes that would lock D with a missing open, close, or indicator.

    The 09:30 open has to be the stored raw print. A Finviz fallback does
    not fill it. Indicator history is the MACD window: every one of those
    prior bars needs a close.
    """
    from . import ohlc_ripper as ohlc
    from . import price_store as ps

    if ps.AUTO_ADJUST:
        return ["adjusted bars"]
    d = str(date or "")[:10]
    t = str(ticker or "").strip().upper()
    gaps: list[str] = []
    if tl._official_ohlc(t, d).get("open") is None:
        gaps.append("missing 09:30 open")
    priors = [b for b in _raw_bars(t) if str(b.get("date") or "") < d]
    need = int(ohlc.MIN_INDICATOR_BARS)
    if not priors or priors[-1].get("close") is None:
        gaps.append("missing prior close")
    if len(priors) < need:
        gaps.append(f"indicator bars {len(priors)}/{need}")
    else:
        window = priors[-need:]
        if any(b.get("close") is None for b in window):
            gaps.append("missing prior close")
    return gaps


def ensure_candidate_bars(date: str, tickers: list[str]) -> None:
    """Fetch raw bars for every candidate. Any hole holds D."""
    from . import price_store as ps

    names = sorted({str(t).strip().upper() for t in tickers if t})
    if ps.AUTO_ADJUST:
        raise HoldDay(
            date, names, "refusing to lock adjusted bars",
            status="held_incomplete",
            gaps=[{"ticker": t, "missing": ["adjusted bars"]} for t in names],
        )
    if not names:
        raise HoldDay(
            date, [], "no candidates — refusing to freeze an empty day",
            status="held_incomplete",
        )
    try:
        ps.ensure_through(date, tickers=names, strict=True)
    except (Exception, SystemExit) as e:
        raise HoldDay(
            date, names, f"price fetch failed: {e}",
            status="held_incomplete",
        ) from e
    reset_price_memory()
    gaps = []
    for t in names:
        missing = completeness_gaps(t, date)
        if missing:
            gaps.append({"ticker": t, "missing": missing})
    if gaps:
        raise HoldDay(
            date, [g["ticker"] for g in gaps],
            "held_incomplete — refusing to write hot_score 0",
            status="held_incomplete",
            gaps=gaps,
        )


def row_price_problem(ticker: str, date: str) -> str | None:
    """None when this row can be frozen. Otherwise it would be hot=0 or no open."""
    gaps = completeness_gaps(ticker, date)
    if gaps:
        return "; ".join(gaps)
    return None


def prices_agree(ours, ref, field: str = "close") -> bool:
    """True when ``ours`` is inside the tolerance for ``field``.

    ``field`` is ``close`` or ``open`` (fills use the open tolerance).
    The limit is the wider of the percent band and the dollar floor in
    ``PRICE_CHECK``. A binary remainder on a round cent still agrees.
    """
    if ours is None or ref is None:
        return False
    try:
        a, b = float(ours), float(ref)
    except (TypeError, ValueError):
        return False
    if not math.isfinite(a) or not math.isfinite(b):
        return False
    spec = PRICE_CHECK["open" if field == "open" else "close"]
    limit = max(float(spec["abs"]), float(spec["pct"]) * abs(b))
    return abs(a - b) <= limit + 1e-8


def _parse_et(stamp: str) -> datetime | None:
    text = str(stamp or "").strip()
    if not text:
        return None
    try:
        when = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if when.tzinfo is None:
        when = when.replace(tzinfo=tl.ET)
    return when.astimezone(tl.ET)


def export_is_postclose(session: str) -> bool:
    """True when ``finviz_{session}.csv`` was saved at or after 16:00 ET.

    The window ends at the next calendar 09:30. A morning scrape, or a
    missing ``.scraped_at``, is not that session's close. Price in a
    morning file is a last trade, not the 16:00 print.
    """
    from . import finviz_digest as fd

    day = str(session or "")[:10]
    try:
        start = datetime.strptime(day, "%Y-%m-%d")
    except ValueError:
        return False
    when = _parse_et(fd.read_export_scraped_at(
        tl.EXPORT_DIR / f"finviz_{day}.csv"))
    if when is None:
        return False
    close = start.replace(hour=16, minute=0, tzinfo=tl.ET)
    nxt_open = (start + timedelta(days=1)).replace(
        hour=9, minute=30, tzinfo=tl.ET)
    return close <= when < nxt_open


def finviz_export_has_open(date: str) -> bool:
    """True when the dated Elite export's header includes the Open column.

    The custom view (``v=151``) already asks for Open, High, Low, and
    Prev Close. A post-close file's Open is that session's 09:30 print.
    """
    day = str(date or "")[:10]
    path = tl.EXPORT_DIR / f"finviz_{day}.csv"
    try:
        header = path.read_text(encoding="utf-8", errors="replace").splitlines()[0]
    except (OSError, IndexError):
        return False
    cols = [c.strip().strip('"') for c in header.split(",")]
    return "Open" in cols


def _dated_snapshot_name(date: str) -> str | None:
    day = str(date or "")[:10]
    if len(day) != 10 or not day[0].isdigit():
        return None
    name = f"{day}.csv"
    if name == "current.csv":
        return None
    return name


def _theme_radar_roots() -> list[Path]:
    roots = []
    env = os.environ.get("THEME_RADAR_ROOT", "").strip()
    if env:
        base = Path(env)
        snap = base / "data" / "snapshots"
        roots.append(snap if snap.is_dir() else base)
    roots.append(ROOT / "vendor" / "theme-radar" / "data" / "snapshots")
    roots.append(ROOT / "data" / "theme_radar_snapshots")
    return roots


def _theme_radar_bytes(date: str) -> bytes | None:
    """Dated snapshot bytes. ``current.csv`` is never a close source."""
    name = _dated_snapshot_name(date)
    if not name:
        return None
    roots = [Path(THEME_RADAR_SNAP_DIR)] if THEME_RADAR_SNAP_DIR is not None else _theme_radar_roots()
    for root in roots:
        path = root / name
        if path.is_file() and path.name != "current.csv":
            try:
                return path.read_bytes()
            except OSError:
                return None
    if THEME_RADAR_SNAP_DIR is not None:
        return None
    return _fetch_url(THEME_RADAR_SNAPSHOT_URL.format(date=str(date)[:10]))


def _hashes_doc() -> dict | None:
    """HASHES.json, fetched once per process. A snap dir skips the network."""
    global _HASHES_DOC
    if THEME_RADAR_SNAP_DIR is not None:
        return None
    if _HASHES_DOC is not None:
        return _HASHES_DOC
    raw = _fetch_url(THEME_RADAR_HASHES_URL)
    if not raw:
        return None
    try:
        doc = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None
    if not isinstance(doc, dict):
        return None
    _HASHES_DOC = doc
    return doc


def _theme_radar_expected_hash(date: str) -> str | None:
    """sha256 from HASHES.json for the slim dated csv. Empty if unknown."""
    doc = _hashes_doc()
    if not doc:
        return None
    files = doc.get("files") if isinstance(doc, dict) else None
    if not isinstance(files, dict):
        return None
    entry = files.get(f"data/snapshots/{str(date)[:10]}.csv") or {}
    sha = str(entry.get("sha256") or "")
    return sha or None


def _as_float(raw):
    """Finite float, or None. A blank cell is missing, not zero."""
    if isinstance(raw, bool) or raw is None:
        return None
    if isinstance(raw, (int, float)):
        val = float(raw)
        return val if math.isfinite(val) else None
    text = str(raw).replace(",", "").strip()
    if not text:
        return None
    try:
        val = float(text)
    except (TypeError, ValueError):
        return None
    return val if math.isfinite(val) else None


def radar_quote(entry) -> tuple[float | None, float | None]:
    """``(close, open)`` from one snapshot row.

    A bare float is the close proxy and no open. That keeps a caller
    which still hands back ``{ticker: price}`` working.
    """
    if isinstance(entry, dict):
        return _as_float(entry.get("close")), _as_float(entry.get("open"))
    return _as_float(entry), None


def parse_theme_radar_prices(text: str) -> dict[str, dict]:
    """Ticker → ``{close, open}`` from a theme-radar snapshot.

    ``Price`` is the close proxy. ``Open`` is the Finviz open appended
    on dated snapshots from 2026-09-25. The column may sit at the end.
    A missing or blank Open is None.
    """
    import csv
    import io

    out: dict[str, dict] = {}
    reader = csv.DictReader(io.StringIO(text or ""))
    if not reader.fieldnames or "Ticker" not in reader.fieldnames or "Price" not in reader.fieldnames:
        return out
    fields = set(reader.fieldnames)
    has_open = "Open" in fields
    for row in reader:
        tick = str(row.get("Ticker") or "").strip().upper()
        if not tick:
            continue
        px = _as_float(row.get("Price"))
        if px is None:
            continue
        opened = _as_float(row.get("Open")) if has_open else None
        item = {"close": px, "open": opened}
        for col, key in (
            ("High", "high"),
            ("Low", "low"),
            ("Prev Close", "prev_close"),
            ("Volume", "volume"),
        ):
            if col not in fields:
                continue
            val = _as_float(row.get(col))
            if val is not None:
                item[key] = val
        out[tick] = item
    return out


def theme_radar_prices(date: str) -> dict[str, dict]:
    """Dated snapshot quotes. A bad hash is not used. Never ``current.csv``."""
    raw = _theme_radar_bytes(date)
    if not raw:
        return {}
    expect = _theme_radar_expected_hash(date)
    if expect and sha256_bytes(raw) != expect:
        return {}
    return parse_theme_radar_prices(raw.decode("utf-8", errors="replace"))


def _dated_raw_name(date: str) -> str | None:
    """``{D}.raw.csv`` only. ``current.csv`` is not an open source."""
    day = str(date or "")[:10]
    if len(day) != 10 or not day[:4].isdigit() or day[4] != "-":
        return None
    return f"{day}.raw.csv"


def _theme_radar_raw_bytes(date: str) -> bytes | None:
    name = _dated_raw_name(date)
    if not name:
        return None
    roots = [Path(THEME_RADAR_SNAP_DIR)] if THEME_RADAR_SNAP_DIR is not None else _theme_radar_roots()
    for root in roots:
        path = root / name
        if path.is_file() and "current" not in path.name:
            try:
                return path.read_bytes()
            except OSError:
                return None
    if THEME_RADAR_SNAP_DIR is not None:
        return None
    return _fetch_url(THEME_RADAR_RAW_URL.format(date=str(date)[:10]))


def _theme_radar_raw_expected_hash(date: str) -> str | None:
    """sha256 of ``data/snapshots/{D}.raw.csv``. Empty when unknown."""
    doc = _hashes_doc()
    if not doc:
        return None
    files = doc.get("files") if isinstance(doc, dict) else None
    if not isinstance(files, dict):
        return None
    entry = files.get(f"data/snapshots/{str(date)[:10]}.raw.csv") or {}
    sha = str(entry.get("sha256") or "")
    return sha or None


def parse_finviz_opens(text: str) -> dict[str, float]:
    """Ticker → Finviz Open. The column may sit anywhere, including last."""
    import csv
    import io

    out: dict[str, float] = {}
    reader = csv.DictReader(io.StringIO(text or ""))
    if not reader.fieldnames or "Ticker" not in reader.fieldnames or "Open" not in reader.fieldnames:
        return out
    for row in reader:
        tick = str(row.get("Ticker") or "").strip().upper()
        if not tick:
            continue
        opened = _as_float(row.get("Open"))
        if opened is not None:
            out[tick] = opened
    return out


def first_scrape_ts(text: str) -> str | None:
    """First non-empty ``scrape_ts`` cell on a slim snapshot.

    Absent before 2026-09-24. The raw export is not a scrape_ts source.
    """
    import csv
    import io

    reader = csv.DictReader(io.StringIO(text or ""))
    if not reader.fieldnames or "scrape_ts" not in reader.fieldnames:
        return None
    for row in reader:
        stamp = str(row.get("scrape_ts") or "").strip()
        if stamp:
            return stamp
    return None


def theme_radar_manifest() -> dict | None:
    """theme-radar ``data/snapshots/manifest.json``.

    A test snap dir reads a local ``manifest.json`` and does not use the
    network cache. ``current`` is not a manifest.
    """
    global _RADAR_MANIFEST
    if THEME_RADAR_SNAP_DIR is not None:
        path = Path(THEME_RADAR_SNAP_DIR) / "manifest.json"
        if not path.is_file():
            return None
        try:
            doc = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return None
        return doc if isinstance(doc, dict) else None
    if _RADAR_MANIFEST is not None:
        return _RADAR_MANIFEST
    raw = _fetch_url(THEME_RADAR_MANIFEST_URL)
    if not raw:
        return None
    try:
        doc = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None
    if not isinstance(doc, dict):
        return None
    _RADAR_MANIFEST = doc
    return doc


def manifest_scrape_ts(date: str) -> str | None:
    """Latest ``scrape_ts_utc`` for D in theme-radar ``manifest.json`` runs."""
    doc = theme_radar_manifest()
    if not doc:
        return None
    day = str(date or "")[:10]
    found = None
    for run in doc.get("runs") or []:
        if not isinstance(run, dict):
            continue
        if str(run.get("date") or "")[:10] != day:
            continue
        stamp = str(run.get("scrape_ts_utc") or "").strip()
        if stamp:
            found = stamp
    return found


def _next_session_date(date: str) -> str:
    from .skip_if_good import _next_weekday
    return _next_weekday(str(date)[:10])


def before_next_open(stamp: str | None, session: str) -> bool:
    """True when ``stamp`` is strictly before the next session's 09:30 ET.

    The snapshot workflow starts after the close, so there is no lower
    bound against D 09:30. A missing or unreadable stamp does not pass.
    Equality with the next open is outside the window.
    """
    when = _parse_et(stamp or "")
    if when is None:
        return False
    day = str(session or "")[:10]
    nxt = datetime.fromisoformat(
        f"{_next_session_date(day)}T09:30:00"
    ).replace(tzinfo=tl.ET)
    return when < nxt


def theme_radar_latest_commit(path: str) -> dict | None:
    """Latest commit of a theme-radar path, or None when it was never committed.

    ``path`` is ``data/snapshots/{D}.raw.csv`` (or the slim ``{D}.csv``).
    The time is the committer timestamp from
    ``GET /repos/SRoyaltyy/theme-radar/commits?path=``. ``per_page=1`` is
    the newest commit. A lookup failure returns ``{"error": ...}`` so the
    day does not treat a blip as a missing export. ``current.csv`` is
    refused. A test snap dir does not call the network.
    """
    name = Path(str(path or "")).name
    if not name or "current" in name:
        return None
    key = str(path)
    if THEME_RADAR_SNAP_DIR is None and key in _COMMIT_CACHE:
        return _COMMIT_CACHE[key]
    if THEME_RADAR_SNAP_DIR is not None:
        return None
    from urllib.parse import urlencode

    url = THEME_RADAR_COMMITS_URL + "?" + urlencode({"path": key, "per_page": "1"})
    raw = _fetch_github(url)
    if raw is None:
        return {"error": "commit lookup failed"}
    try:
        doc = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return {"error": "commit lookup failed"}
    if isinstance(doc, dict):
        return {"error": str(doc.get("message") or "commit lookup failed")}
    if not isinstance(doc, list):
        return {"error": "commit lookup failed"}
    if not doc:
        _COMMIT_CACHE[key] = None
        return None
    item = doc[0] if isinstance(doc[0], dict) else {}
    commit = item.get("commit") if isinstance(item.get("commit"), dict) else {}
    committer = commit.get("committer") if isinstance(commit.get("committer"), dict) else {}
    sha = str(item.get("sha") or "")
    committed = str(committer.get("date") or "")
    if not sha or not committed:
        return {"error": "commit lookup failed"}
    found = {"sha": sha, "committed_at": committed}
    _COMMIT_CACHE[key] = found
    return found


def _slim_text(date: str) -> str:
    raw = _theme_radar_bytes(date)
    if not raw:
        return ""
    expect = _theme_radar_expected_hash(date)
    if expect and sha256_bytes(raw) != expect:
        return ""
    return raw.decode("utf-8", errors="replace")


def theme_radar_raw_opens(date: str) -> dict[str, float]:
    """Finviz Open from ``{D}.raw.csv``. A bad hash is an empty map."""
    raw = _theme_radar_raw_bytes(date)
    if not raw:
        return {}
    expect = _theme_radar_raw_expected_hash(date)
    if expect and sha256_bytes(raw) != expect:
        return {}
    return parse_finviz_opens(raw.decode("utf-8", errors="replace"))


def _open_decision(
    source: str,
    *,
    commit_sha: str = "",
    commit_time: str = "",
    scrape_ts: str | None = None,
    opens: dict | None = None,
    note: str = "",
) -> dict:
    return {
        "source": source,
        "commit_sha": commit_sha,
        "commit_time": commit_time,
        "scrape_ts": scrape_ts,
        "opens": opens or {},
        "note": note,
    }


def _session_scrape_ts(day: str, slim: str) -> str | None:
    """scrape_ts for D, from 2026-09-24 on.

    The slim ``{D}.csv`` column wins. Otherwise ``scrape_ts_utc`` on that
    date's manifest run. raw.csv is not a scrape_ts source.
    """
    stamp = first_scrape_ts(slim) if slim else None
    if stamp:
        return stamp
    if str(day or "")[:10] < SCRAPE_TS_FROM:
        return None
    return manifest_scrape_ts(day)


def _slim_open_or_stooq(
    day: str, slim: str, stamp: str | None, note: str,
    *, commit_sha: str = "", commit_time: str = "",
) -> dict:
    """Slim Open from 2026-09-25, otherwise the Stooq note already chosen."""
    if day < SNAPSHOT_OPEN_FROM or not slim:
        return _open_decision(
            "stooq", commit_sha=commit_sha, commit_time=commit_time,
            scrape_ts=stamp, note=note,
        )
    if stamp and not before_next_open(stamp, day):
        return _open_decision(
            "stooq", commit_sha=commit_sha, commit_time=commit_time,
            scrape_ts=stamp,
            note="scrape_ts at or after the next session 09:30 ET",
        )
    slim_commit = theme_radar_latest_commit(f"data/snapshots/{day}.csv") or {}
    if slim_commit.get("error"):
        return _open_decision(
            "stooq", scrape_ts=stamp, note="commit lookup failed",
        )
    slim_sha = str(slim_commit.get("sha") or "")
    slim_time = str(slim_commit.get("committed_at") or "")
    if not slim_sha or not before_next_open(slim_time, day):
        return _open_decision(
            "stooq",
            commit_sha=slim_sha or commit_sha,
            commit_time=slim_time or commit_time,
            scrape_ts=stamp,
            note=note if not slim_sha else (
                "snapshot commit at or after the next session 09:30 ET"
            ),
        )
    slim_opens = parse_finviz_opens(slim)
    if not slim_opens:
        return _open_decision(
            "stooq", commit_sha=slim_sha, commit_time=slim_time,
            scrape_ts=stamp, note="no Open on the accepted snapshot",
        )
    extra = ""
    if stamp:
        extra = "; scrape_ts before the next session 09:30 ET"
    return _open_decision(
        "finviz_snapshot", commit_sha=slim_sha, commit_time=slim_time,
        scrape_ts=stamp, opens=slim_opens,
        note="latest snapshot commit before the next session 09:30 ET" + extra,
    )


def day_open_tape(date: str) -> dict:
    """Which open file is allowed for D.

    ``{D}.raw.csv`` Finviz Open is primary when the latest git commit of
    that path is before the next session's 09:30 ET. From 2026-09-24 a
    scrape_ts on the slim snapshot, or ``scrape_ts_utc`` in manifest.json,
    must clear the same upper bound. It is not read from raw.csv. From
    2026-09-25 the slim snapshot Open is next. 2026-08-27 has no raw
    export, so that day is Stooq, as is any late commit or late scrape.
    """
    day = str(date or "")[:10]
    raw_commit = theme_radar_latest_commit(f"data/snapshots/{day}.raw.csv") or {}
    if raw_commit.get("error"):
        return _open_decision("stooq", note="commit lookup failed")
    sha = str(raw_commit.get("sha") or "")
    committed = str(raw_commit.get("committed_at") or "")
    slim = _slim_text(day)
    stamp = _session_scrape_ts(day, slim)
    if not sha:
        return _slim_open_or_stooq(day, slim, stamp, "no raw export")
    if not before_next_open(committed, day):
        return _open_decision(
            "stooq", commit_sha=sha, commit_time=committed, scrape_ts=stamp,
            note="raw.csv commit at or after the next session 09:30 ET",
        )
    if stamp and not before_next_open(stamp, day):
        return _open_decision(
            "stooq", commit_sha=sha, commit_time=committed, scrape_ts=stamp,
            note="scrape_ts at or after the next session 09:30 ET",
        )
    raw = _theme_radar_raw_bytes(day)
    expect = _theme_radar_raw_expected_hash(day)
    if raw and expect and sha256_bytes(raw) != expect:
        return _open_decision(
            "stooq", commit_sha=sha, commit_time=committed, scrape_ts=stamp,
            note="raw.csv hash does not match HASHES.json",
        )
    opens = parse_finviz_opens(raw.decode("utf-8", errors="replace")) if raw else {}
    if opens:
        extra = ""
        if stamp:
            extra = "; scrape_ts before the next session 09:30 ET"
        return _open_decision(
            "finviz_raw", commit_sha=sha, commit_time=committed,
            scrape_ts=stamp, opens=opens,
            note="latest raw.csv commit before the next session 09:30 ET" + extra,
        )
    return _slim_open_or_stooq(
        day, slim, stamp, "no Open on the accepted raw export",
        commit_sha=sha, commit_time=committed,
    )


def log_open_sources(dates: list[str], *, path: Path | None = None) -> list[dict]:
    """Record the open file the guard accepts for each session."""
    rows = []
    for date in dates:
        tape = day_open_tape(date)
        write_open_source_row(date, tape, path=path)
        row = {
            "date": str(date)[:10],
            "source": tape.get("source"),
            "commit_sha": tape.get("commit_sha") or "",
            "commit_time": tape.get("commit_time") or "",
            "scrape_ts": tape.get("scrape_ts") or "",
            "note": tape.get("note") or "",
        }
        rows.append(row)
        print(f"[factor-mine] open source {row['date']} {row['source']} {row['note']}",
              flush=True)
    return rows


def write_open_source_row(date: str, row: dict, *, path: Path | None = None) -> None:
    """One row per session. A later check replaces that date."""
    import csv

    dest = Path(path or OPEN_SOURCE_LOG)
    dest.parent.mkdir(parents=True, exist_ok=True)
    existing: list[dict] = []
    if dest.is_file():
        try:
            with dest.open(newline="", encoding="utf-8") as handle:
                existing = list(csv.DictReader(handle))
        except OSError:
            existing = []
    day = str(date or "")[:10]
    kept = [item for item in existing if str(item.get("date") or "") != day]
    kept.append({
        "date": day,
        "source": str(row.get("source") or ""),
        "commit_sha": str(row.get("commit_sha") or ""),
        "commit_time": str(row.get("commit_time") or ""),
        "scrape_ts": str(row.get("scrape_ts") or ""),
        "note": str(row.get("note") or ""),
    })
    kept.sort(key=lambda item: item.get("date") or "")
    with dest.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(OPEN_LOG_FIELDS))
        writer.writeheader()
        writer.writerows(kept)


def parse_stooq_bar(text: str, date: str) -> dict:
    """One daily row from a Stooq CSV. Dates may be YYYY-MM-DD or YYYYMMDD."""
    import csv
    import io

    empty = {"open": None, "high": None, "low": None, "close": None}
    want = str(date or "")[:10]
    compact = want.replace("-", "")
    reader = csv.DictReader(io.StringIO(text or ""))
    if not reader.fieldnames:
        return empty
    fields = {name.strip().lower(): name for name in reader.fieldnames if name}
    for row in reader:
        stamp = str(row.get(fields.get("date") or "Date") or "").strip()
        if stamp != want and stamp != compact:
            continue
        bar = {}
        for key in ("open", "high", "low", "close"):
            raw = row.get(fields.get(key) or key.title())
            try:
                val = float(str(raw).replace(",", ""))
            except (TypeError, ValueError):
                val = None
            bar[key] = val if val is not None and math.isfinite(val) else None
        return bar
    return empty


def _fetch_url(url: str) -> bytes | None:
    import urllib.request

    try:
        req = urllib.request.Request(url, headers={"User-Agent": "fullscan-factor-mine"})
        with urllib.request.urlopen(req, timeout=20) as resp:
            return resp.read()
    except Exception:
        return None


def _fetch_github(url: str) -> bytes | None:
    """GitHub JSON. Sends a token when the environment has one."""
    import urllib.request

    headers = {
        "User-Agent": "fullscan-factor-mine",
        "Accept": "application/vnd.github+json",
    }
    token = os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN") or ""
    if token:
        headers["Authorization"] = f"Bearer {token}"
    try:
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=20) as resp:
            return resp.read()
    except Exception:
        return None


def _fetch_stooq(ticker: str) -> str:
    tick = "".join(ch for ch in str(ticker or "").upper() if ch.isalpha())
    if not tick:
        return ""
    raw = _fetch_url(STOOQ_DAILY_URL.format(ticker=tick.lower()))
    if not raw:
        return ""
    return raw.decode("utf-8", errors="replace")


def stooq_bar(ticker: str, date: str) -> dict:
    """Free daily OHLC. Used when the Elite export has no post-close Open."""
    return parse_stooq_bar(_fetch_stooq(ticker), date)


def paper_fills(date: str) -> dict[str, list[float]]:
    """Filled Webull paper prints for D. Read-only. Plan prices are not fills."""
    day = str(date or "")[:10]
    path = PAPER_OPEN_DIR / f"{day}_status.json"
    if not path.is_file():
        alt = PAPER_OPEN_DIR / f"{day}_submit.json"
        path = alt if alt.is_file() else path
    if not path.is_file():
        return {}
    try:
        doc = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    out: dict[str, list[float]] = {}
    for row in doc.get("sent") or []:
        if not isinstance(row, dict):
            continue
        status = str(row.get("status") or "").lower()
        broker = str(row.get("broker_status") or "").upper()
        if status != "filled" and not broker.startswith("FILLED"):
            continue
        px = row.get("avg_fill_px")
        if px is None or px == "":
            continue
        tick = str(row.get("ticker") or "").strip().upper()
        try:
            val = float(px)
        except (TypeError, ValueError):
            continue
        if not tick or not math.isfinite(val):
            continue
        out.setdefault(tick, []).append(val)
    return out


def _session_print(ticker: str, date: str) -> dict:
    bar = tl._official_ohlc(ticker, date)
    return {"open": bar.get("open"), "close": bar.get("close")}


def _price_gap(ticker: str, field: str, ours, ref, source: str) -> dict:
    return {
        "ticker": ticker,
        "field": field,
        "ours": ours,
        "ref": ref,
        "source": source,
    }


def session_cross_check(date: str, tickers: list[str], *,
                         fetch_stooq: bool = True) -> list[dict]:
    """Open and close versus the external sources, then paper fills.

    Close order: post-close ``finviz_{D}.csv`` Price, else the dated
    theme-radar snapshot Price (never ``current.csv``). Open order:
    ``finviz_raw`` when the latest ``{D}.raw.csv`` commit is before the
    next session's 09:30 ET, else from 2026-09-25 ``finviz_snapshot``,
    else Stooq. A filled Webull paper order is a third check against
    our 09:30 open.
    """
    day = str(date or "")[:10]
    post = export_is_postclose(day)
    radar: dict | None = None
    stooq: dict[str, dict] = {}
    fills = paper_fills(day)
    tape = day_open_tape(day)
    tape_opens = tape.get("opens") or {}
    used_stooq = False

    def radar_map() -> dict:
        nonlocal radar
        if radar is None:
            radar = theme_radar_prices(day) or {}
        return radar

    gaps = []
    names = sorted({str(t).strip().upper() for t in tickers if t})
    for t in names:
        ours = _session_print(t, day)
        close_ref = None
        close_src = None
        if post:
            bar = tl._finviz_bar(t, day)
            if bar.get("close") is not None:
                close_ref = bar.get("close")
                close_src = f"finviz_{day}.csv Price"
        if close_ref is None:
            close_ref = radar_quote(radar_map().get(t))[0]
            if close_ref is not None:
                close_src = f"theme-radar snapshots/{day}.csv Price"
        if close_ref is None:
            gaps.append({
                "ticker": t,
                "missing": ["missing session close"],
                "source": close_src or f"theme-radar snapshots/{day}.csv Price",
            })
        elif not prices_agree(ours.get("close"), close_ref, "close"):
            gaps.append(_price_gap(t, "close", ours.get("close"), close_ref, close_src))

        open_ref = None
        open_src = None
        opened = tape_opens.get(t)
        if opened is not None:
            open_ref = opened
            open_src = tape.get("source")
        if open_ref is None and fetch_stooq:
            used_stooq = True
            if t not in stooq:
                stooq[t] = stooq_bar(t, day)
            if stooq[t].get("open") is not None:
                open_ref = stooq[t]["open"]
                open_src = f"stooq {t}.us Open"
        if open_ref is None:
            gaps.append({
                "ticker": t,
                "missing": ["missing 09:30 open reference"],
                "source": open_src or f"stooq {t}.us Open",
            })
        elif not prices_agree(ours.get("open"), open_ref, "open"):
            gaps.append(_price_gap(t, "open", ours.get("open"), open_ref, open_src))

        for px in fills.get(t) or []:
            if not prices_agree(ours.get("open"), px, "open"):
                gaps.append(_price_gap(
                    t, "fill", ours.get("open"), px,
                    f"webull paper {day}_status.json avg_fill_px",
                ))
    decision = dict(tape)
    if tape.get("source") != "stooq" and used_stooq:
        decision["note"] = (
            str(tape.get("note") or "") + "; Stooq filled names missing from the file"
        ).strip("; ")
    elif tape.get("source") == "stooq" or used_stooq:
        decision["source"] = "stooq"
    LAST_OPEN_SOURCE[day] = decision
    return gaps


def _ranked_names(rows: list, *, score=None) -> list[str]:
    """Source order, with equal scores broken by ticker."""
    from . import factor_mine as fm

    items = []
    seen: set[str] = set()
    for i, raw in enumerate(rows or []):
        if isinstance(raw, dict):
            tick = fm._tick(raw.get("ticker"))
            value = raw.get("change_pct") if score is None else score(raw)
        else:
            tick = fm._tick(raw)
            value = None
        if not tick or tick in seen:
            continue
        seen.add(tick)
        try:
            number = None if value is None else float(value)
        except (TypeError, ValueError):
            number = None
        items.append((number, tick, i))
    if score is not None or any(item[0] is not None for item in items):
        items.sort(key=lambda item: (
            -(item[0] if item[0] is not None else -1e18),
            item[1],
            item[2],
        ))
    return [tick for _, tick, _ in items]


def candidate_provenance(date: str, cal: list[str] | None = None) -> dict:
    """Who was a candidate on D, and why, from morning files only.

    Flatten plan tickers and mover-buy holdings are prior-night outputs.
    They are not a source on this list.
    """
    from . import gainer_asof as ga
    from . import gainer_capture as gc
    from . import oppset_clock_b as opp

    day = str(date or "")[:10]
    look = gc.lookback_calendar(list(cal or []))
    prior = gc.knowable_export_date(look, day)
    nxt = gc.next_session(look, day)
    buckets: dict[str, list[str]] = {name: [] for name in MORNING_SOURCES}
    if prior:
        frame = ga.load_finviz(prior)
        buckets["liquid_tape"] = _ranked_names(ga._liquid_tape(
            frame, top_n=0, min_change=0.0, liquid=True,
            min_mcap_m=None, side="up", skip_change=True,
        ))
        buckets["yday_gainer"] = _ranked_names(
            gc.yesterday_gainers(prior, top_n=25))
        buckets["yday_mover"] = _ranked_names(
            gc.yesterday_movers(prior, top_n=20))
        buckets["earn_react"] = _ranked_names(gc.earnings_reaction(prior, day))
    buckets["overnight"] = _ranked_names(
        gc.overnight_scheduled(prior, day, nxt))
    buckets["overnight_mega"] = _ranked_names(gc.overnight_scheduled(
        prior, day, nxt, min_mcap_m=gc.OVERNIGHT_MEGA_MCAP_M,
    ))
    if opp.union_enabled():
        buckets["oppset"] = _ranked_names(opp.flagged_tickers(day, top_n=30))
    by_ticker: dict[str, list[dict]] = {}
    for source in MORNING_SOURCES:
        for rank, tick in enumerate(buckets.get(source) or [], start=1):
            by_ticker.setdefault(tick, []).append(
                {"source": source, "rank": rank})
    names = [
        {"ticker": tick, "sources": by_ticker[tick]}
        for tick in sorted(by_ticker)
    ]
    return {
        "date": day,
        "code_sha": code_sha(),
        "prior_export": prior,
        "n": len(names),
        "excluded": ["flatten", "mover_buy"],
        "names": names,
    }


def _review_hold(date: str, gaps: list[dict]) -> None:
    tickers = sorted({str(g.get("ticker") or "") for g in gaps if g.get("ticker")})
    bits = []
    for gap in gaps[:12]:
        if gap.get("missing"):
            bits.append(f"{gap.get('ticker')}: {', '.join(gap['missing'])}")
        else:
            bits.append(
                f"{gap.get('ticker')} {gap.get('field')} "
                f"ours={gap.get('ours')} ref={gap.get('ref')} "
                f"({gap.get('source')})"
            )
    raise HoldDay(
        date, tickers,
        "price cross-check: " + "; ".join(bits),
        status="held_review",
        gaps=gaps,
    )


def prepare_lock(date: str) -> dict:
    """Candidate log plus the open/close cross-check.

    The open-source row is the only file this writes. A mismatch still
    holds the day before any snapshot, pin, or candidate file.
    """
    doc = candidate_provenance(date)
    names = [row["ticker"] for row in doc.get("names") or []]
    traded = list(paper_fills(date))
    gaps = session_cross_check(date, list(names) + traded)
    decision = LAST_OPEN_SOURCE.get(str(date)[:10])
    if decision:
        write_open_source_row(date, decision)
    if gaps:
        _review_hold(date, gaps)
    return doc


def write_candidates(date: str, doc: dict, *, restate: bool = False) -> str:
    return _write_frozen(
        candidate_path(date), doc, "candidates", date, restate=restate)


def pin_morning_if_overlay(date: str) -> Path | None:
    """Copy a still-intact morning overlay aside so postclose cannot replace it."""
    from . import map_heat as mh

    dest = mh.OUT_DIR / f"{date}_map_heat_morning.json"
    if dest.is_file():
        return dest
    main = mh.OUT_DIR / f"{date}_map_heat.json"
    if not main.is_file():
        return None
    try:
        data = json.loads(main.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if str(data.get("phase") or "") != "morning_overlay":
        return None
    if not (data.get("tape") or []):
        return None
    dest.write_bytes(main.read_bytes())
    print(f"[factor-mine] pinned morning map_heat {dest.name}", flush=True)
    return dest


def heat_record(date: str, prior: str | None) -> dict:
    """Heat vintage knowable for D, preferring the immutable morning file."""
    pin_morning_if_overlay(date)
    board, vintage = tl._map_heat_board(date, prior)
    from . import map_heat as mh

    morning = mh.OUT_DIR / f"{date}_map_heat_morning.json"
    main = mh.OUT_DIR / f"{date}_map_heat.json"
    src = morning if morning.is_file() else main
    digest = None
    if src.is_file():
        try:
            digest = sha256_bytes(src.read_bytes())
        except OSError:
            digest = None
    return {
        "vintage": vintage,
        "phase": (board or {}).get("phase"),
        "board_date": vintage,
        "source": str(src.relative_to(ROOT)) if src.is_file() else None,
        "sha256": digest,
    }


def pin_prices(date: str, tickers: list[str]) -> dict:
    from . import ohlc_ripper as ohlc

    names = {}
    for t in sorted({str(x) for x in tickers if x}):
        sess = tl.session_bar(t, date) or {}
        names[t] = {
            "prior": ohlc.prior_bars(t, date, n=60),
            "open": sess.get("open"),
            "high": sess.get("high"),
            "low": sess.get("low"),
            "close": sess.get("close"),
        }
    return {
        "date": date,
        "tape": "raw",
        "auto_adjust": False,
        "names": names,
    }


def bars_from_panel(panel: dict) -> dict:
    bars = {}
    for row in panel.get("rows") or []:
        t, d = row.get("ticker"), row.get("date")
        if not t or not d:
            continue
        bar = {}
        if row.get("open") is not None:
            bar["open"] = row.get("open")
        if row.get("close") is not None:
            bar["close"] = row.get("close")
        if bar:
            bars[(t, d)] = bar
    return bars


def bars_for_decisions(panel: dict, date: str, pinned: dict | None) -> dict:
    bars = bars_from_panel(panel)
    for t, info in ((pinned or {}).get("names") or {}).items():
        bars[(t, date)] = {
            "open": info.get("open"),
            "high": info.get("high"),
            "low": info.get("low"),
            "close": info.get("close"),
        }
    return bars


def _write_frozen(path: Path, obj: dict, slot: str, date: str, *,
                  restate: bool) -> str:
    raw = encode_frozen(slot, obj)
    digest = sha256_bytes(raw)
    man = load_manifest()
    prev = (man.get(slot) or {}).get(date) or {}
    if path.is_file() and not restate:
        have = sha256_bytes(path.read_bytes())
        raise FrozenHistory(
            f"{path.name} already frozen sha={have[:12]} "
            f"(pass --restate {date} to correct it)"
        )
    if path.is_file() and restate:
        have = sha256_bytes(path.read_bytes())
        man.setdefault("restatements", []).append({
            "date": date,
            "slot": slot,
            "at": _now(),
            "prev_sha256": prev.get("sha256") or have,
            "sha256": digest,
        })
        print(
            f"[factor-mine] RESTATE {slot} {date} "
            f"prev={ (prev.get('sha256') or have)[:12] } -> {digest[:12]}",
            flush=True,
        )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(raw)
    entry = {
        "sha256": digest,
        "written_at": _now(),
        "bytes": len(raw),
    }
    if slot == "ledgers":
        entry["encoding"] = "gzip"
    man.setdefault(slot, {})[date] = entry
    if slot == "snapshots":
        man[slot][date]["n_rows"] = len(obj.get("rows") or [])
        man[slot][date]["heat_vintage"] = (obj.get("heat") or {}).get("vintage")
    save_manifest(man)
    print(f"[factor-mine] froze {slot} {date} sha={digest[:12]}", flush=True)
    return digest


def write_snapshot(date: str, snap: dict, *, restate: bool = False) -> str:
    return _write_frozen(snapshot_path(date), snap, "snapshots", date, restate=restate)


def write_ledger(date: str, ledger: dict, *, restate: bool = False) -> str:
    plain = LEDGER_DIR / f"{date}.json"
    if plain.is_file() and not restate:
        raise FrozenHistory(
            f"{plain.name} already frozen "
            f"(pass --restate {date} to correct it)"
        )
    if plain.is_file() and restate:
        plain.unlink()
    return _write_frozen(ledger_path(date), ledger, "ledgers", date, restate=restate)


def write_price_pin(date: str, doc: dict, *, restate: bool = False) -> str:
    return _write_frozen(price_path(date), doc, "prices", date, restate=restate)


def read_json(path: Path) -> dict | None:
    if not path.is_file():
        return None
    try:
        blob = path.read_bytes()
        if blob[:2] == b"\x1f\x8b":
            blob = gzip.decompress(blob)
        return json.loads(blob)
    except (OSError, json.JSONDecodeError, gzip.BadGzipFile):
        return None


def apply_frozen_snapshots(panel: dict) -> dict:
    """Replace landed dates with their frozen rows. Other dates stay."""
    from . import factor_mine as fm

    panel = fm.rehydrate_panel(panel)
    if not SNAP_DIR.is_dir():
        return panel
    by_date = dict(panel.get("by_date") or {})
    replaced = False
    for path in sorted(SNAP_DIR.glob("*.json")):
        date = path.stem
        snap = read_json(path) or {}
        rows = list(snap.get("rows") or [])
        if not rows:
            continue
        by_date[date] = rows
        replaced = True
    if not replaced:
        return panel
    rows = []
    for date in sorted(by_date):
        rows.extend(by_date[date] or [])
    out = dict(panel)
    dates = list(panel.get("session_dates") or [])
    for date in by_date:
        if date not in dates:
            dates.append(date)
    dates = sorted({d for d in dates if d})
    out.update({
        "rows": rows,
        "by_date": by_date,
        "session_dates": dates,
        "n_rows": len(rows),
        "n_sessions": len(dates),
        "to_date": dates[-1] if dates else panel.get("to_date"),
    })
    return out


def make_snapshot(date: str, rows: list[dict], prior: str | None,
                  prices_sha: str | None,
                  candidates: dict | None = None) -> dict:
    from . import price_store as ps

    if ps.AUTO_ADJUST:
        raise HoldDay(
            date, [], "refusing to lock adjusted bars",
            status="held_incomplete",
        )
    heat = heat_record(date, prior)
    frozen_rows = []
    for row in rows:
        item = dict(row)
        item["heat_vintage"] = heat.get("vintage")
        item["open_0930"] = row.get("open")
        frozen_rows.append(item)
    frozen_rows.sort(key=lambda r: (
        r.get("date") or "", int(r.get("src_rank") or 0), r.get("ticker") or "",
    ))
    snap = {
        "date": date,
        "asof": "09:30_et",
        "open_clock": "09:30 ET",
        "code_sha": code_sha(),
        "tape": "raw",
        "auto_adjust": False,
        "heat": heat,
        "prices_sha256": prices_sha,
        "n_rows": len(frozen_rows),
        "rows": frozen_rows,
    }
    if candidates is not None:
        snap["candidates"] = {
            "n": candidates.get("n"),
            "prior_export": candidates.get("prior_export"),
            "excluded": list(candidates.get("excluded") or []),
            "names": list(candidates.get("names") or []),
        }
    return snap


def freeze_meta() -> dict:
    man = load_manifest()
    first = man.get("first_frozen")
    return {
        "first_frozen": first,
        "reconstructed_before": first,
        "n_snapshots": len(man.get("snapshots") or {}),
        "n_ledgers": len(man.get("ledgers") or {}),
    }


def label_payload(payload: dict) -> dict:
    """Mark sessions before the first frozen day as reconstructed."""
    meta = freeze_meta()
    payload = dict(payload)
    payload["freeze"] = meta
    first = meta.get("first_frozen")
    dates = list(payload.get("dates") or [])
    payload["reconstructed_dates"] = [
        d for d in dates if first and str(d) < str(first)
    ]
    labels = {}
    for date, meta in (load_manifest().get("snapshots") or {}).items():
        lab = str((meta or {}).get("label") or "")
        if lab:
            labels[str(date)] = lab
    if labels:
        payload["retro"] = labels
        payload["pit_rebuilt_dates"] = sorted(
            d for d, lab in labels.items() if lab == "pit_rebuilt")
        payload["incomplete_pit_dates"] = sorted(
            d for d, lab in labels.items() if lab == "incomplete_pit")
    return payload


def committed_manifest() -> dict:
    rel = MANIFEST_PATH.relative_to(ROOT).as_posix()
    try:
        out = subprocess.run(
            ["git", "show", f"HEAD:{rel}"],
            cwd=ROOT, capture_output=True, text=True, check=False,
        )
    except OSError:
        return {}
    if out.returncode != 0 or not (out.stdout or "").strip():
        return {}
    try:
        return json.loads(out.stdout)
    except json.JSONDecodeError:
        return {}


def guard_manifest(old: dict | None, new: dict | None,
                   restate: list[str] | None = None) -> None:
    """Fail if any earlier snapshot, ledger, lineup, or candidate hash changed."""
    old = old or {}
    new = new or {}
    allow = {str(d)[:10] for d in (restate or []) if d}
    for slot in GUARD_SLOTS:
        for date, meta in (old.get(slot) or {}).items():
            if date in allow:
                print(f"[factor-mine] restate allowed {slot} {date}", flush=True)
                continue
            now = (new.get(slot) or {}).get(date) or {}
            prev = (meta or {}).get("sha256")
            got = now.get("sha256")
            if prev and got != prev:
                raise SystemExit(
                    f"frozen {slot} {date} hash changed {prev} -> {got}. "
                    f"Pass --restate {date} to log a correction."
                )
    for slot in GUARD_SLOTS:
        for date, meta in (new.get(slot) or {}).items():
            path = slot_path(slot, date)
            if not path.is_file():
                raise SystemExit(f"freeze manifest lists {slot} {date} but {path} is missing")
            have = sha256_bytes(path.read_bytes())
            if have != (meta or {}).get("sha256"):
                raise SystemExit(
                    f"freeze manifest {slot} {date} sha does not match {path.name}"
                )


def read_lineup(date: str) -> dict | None:
    return read_json(lineup_path(date))


def lineup_document(date: str, recipes: list[dict],
                    shown: list[str] | None = None) -> dict:
    """Recipes displayed on D. Creation date must be on or before D."""
    from . import factor_mine as fm

    by: dict[str, str] = {}
    for rec in recipes:
        name = str(rec.get("name") or "")
        if not name:
            continue
        by[name] = fm.recipe_created_on(name, rec)
    if shown is None:
        names = [name for name, created in by.items() if created <= date]
    else:
        names = []
        for name in shown:
            created = by.get(name)
            if created is None:
                created = fm.recipe_created_on(name, {})
                by[name] = created
            if created <= date:
                names.append(name)
    entries = [
        {"created_on": by[name], "name": name}
        for name in sorted(set(names))
    ]
    return {"code_sha": code_sha(), "date": date, "recipes": entries}


def record_lineup(date: str, recipes: list[dict],
                  shown: list[str] | None = None) -> dict | None:
    """Write D's dashboard lineup once. A second call keeps the first file."""
    day = str(date or "")[:10]
    if len(day) != 10 or not recipes:
        return None
    existing = read_lineup(day)
    if existing:
        return existing
    doc = lineup_document(day, recipes, shown)
    _write_frozen(lineup_path(day), doc, "lineups", day, restate=False)
    return doc


def load_recipe_catalog(path: Path | None = None) -> dict[str, str]:
    src = Path(path or CREATED_PATH)
    if not src.is_file():
        return {}
    try:
        doc = json.loads(src.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    if not isinstance(doc, dict):
        return {}
    return {str(k): str(v)[:10] for k, v in doc.items() if k and v}


def record_recipe_dates(recipes: list[dict], path: Path | None = None) -> dict[str, str]:
    """Append creation dates. An existing name cannot move to another date."""
    from . import factor_mine as fm

    dest = Path(path or CREATED_PATH)
    catalog = load_recipe_catalog(dest)
    for rec in recipes:
        name = str(rec.get("name") or "")
        if not name:
            continue
        created = fm.recipe_created_on(name, rec)
        prev = catalog.get(name)
        if prev and prev != created:
            raise FrozenHistory(
                f"recipe {name} created_on is {prev}; refusing {created}"
            )
        catalog[name] = created
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(
        json.dumps(dict(sorted(catalog.items())), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return catalog


def guard_recipe_catalog(old: dict | None, new: dict | None) -> None:
    """New recipe names may appear. An existing creation date may not change."""
    old = old or {}
    new = new or {}
    for name, created in old.items():
        got = new.get(name)
        if got != created:
            raise SystemExit(
                f"recipe {name} created_on changed {created} -> {got}. "
                "Add a new recipe version instead of moving the old one."
            )


def committed_recipe_catalog() -> dict:
    rel = CREATED_PATH.relative_to(ROOT).as_posix()
    try:
        out = subprocess.run(
            ["git", "show", f"HEAD:{rel}"],
            cwd=ROOT, capture_output=True, text=True, check=False,
        )
    except OSError:
        return {}
    if out.returncode != 0 or not (out.stdout or "").strip():
        return {}
    try:
        doc = json.loads(out.stdout)
    except json.JSONDecodeError:
        return {}
    if not isinstance(doc, dict):
        return {}
    return {str(k): str(v)[:10] for k, v in doc.items()}


def assert_history_unchanged(restate: list[str] | str | None = None) -> None:
    if isinstance(restate, str):
        restate = [d for d in restate.split(",") if d.strip()]
    env = os.environ.get("FM_RESTATE") or os.environ.get("RESTATE") or ""
    extra = [d.strip() for d in env.split(",") if d.strip()]
    guard_manifest(committed_manifest(), load_manifest(), list(restate or []) + extra)
    guard_recipe_catalog(committed_recipe_catalog(), load_recipe_catalog())


def _prior(cal: list[str], date: str) -> str | None:
    prev = None
    for d in cal:
        if d == date:
            return prev
        if d < date:
            prev = d
    return prev


def _ledger_dates() -> list[str]:
    if not LEDGER_DIR.is_dir():
        return []
    dates = []
    for path in LEDGER_DIR.iterdir():
        name = path.name
        if name.endswith(".json.gz"):
            dates.append(name[: -len(".json.gz")])
        elif name.endswith(".json"):
            dates.append(name[: -len(".json")])
    return sorted(set(dates))


def _ledger_on_disk(date: str) -> Path | None:
    gz = ledger_path(date)
    plain = LEDGER_DIR / f"{date}.json"
    if gz.is_file():
        return gz
    if plain.is_file():
        return plain
    return None


def read_ledger(date: str) -> dict | None:
    path = _ledger_on_disk(date)
    if path is None:
        return None
    return read_json(path)


def latest_ledger_before(date: str) -> dict | None:
    prev = [d for d in _ledger_dates() if d < date]
    if not prev:
        return None
    return read_ledger(prev[-1])


def _saved(ledger: dict | None, recipe: str, key: str) -> dict | None:
    if not ledger:
        return None
    rec = (ledger.get("recipes") or {}).get(recipe) or {}
    if key == "primary":
        return rec.get("primary")
    return (rec.get("starts") or {}).get(key)


def bridge_published(payload: dict, name: str, prior: str | None,
                     hold: int) -> dict | None:
    """Resume cursor from the published primary book. Does not resimulate it."""
    if not prior:
        return None
    daily = list((payload.get("daily") or {}).get(name) or [])
    row = next((d for d in daily if d.get("date") == prior), None)
    if row is None:
        earlier = [d for d in daily if str(d.get("date") or "") <= prior]
        row = earlier[-1] if earlier else None
    if not row:
        return None
    pos = {}
    for lot in row.get("lots") or []:
        t = str(lot.get("ticker") or "")
        if not t:
            continue
        try:
            px = float(lot.get("entry_px") or 0)
            shares = int(lot.get("shares") or 0)
        except (TypeError, ValueError):
            continue
        pos[t] = {
            "ticker": t,
            "shares": shares,
            "entry_px": px,
            "entry_date": lot.get("entry_date") or prior,
            "cost": shares * px,
            "fee_in": 0.0,
            "notional": shares * px,
            "last_px": px,
            "peak_px": px,
            "close_px": px,
            "min_hold": int(hold or 1),
        }
    return {
        "cash": row.get("cash"),
        "yday_equity": row.get("equity"),
        "pos": pos,
        "after": row.get("date"),
    }


def decision_from_book(book: dict, date: str) -> dict:
    trades = [t for t in (book.get("trades") or []) if t.get("date") == date]
    daily = next((d for d in (book.get("daily") or []) if d.get("date") == date), None)
    skips = [s for s in (book.get("skips") or []) if s.get("date") == date]

    def fill(t: dict) -> dict:
        return {k: t.get(k) for k in _FILL_KEYS if t.get(k) is not None}

    state = {
        "cash": book.get("cash"),
        "yday_equity": None if not daily else daily.get("equity"),
        "pos": book.get("pos") or {},
        "after": date,
    }
    if book.get("member_states") is not None:
        state["members"] = book.get("member_states")
    return {
        "buys": [fill(t) for t in trades if t.get("side") in ("BUY", "SHORT")],
        "sells": [fill(t) for t in trades if t.get("side") in ("SELL", "COVER")],
        "skips": [
            {k: s.get(k) for k in ("ticker", "kind", "reason") if s.get(k) is not None}
            for s in skips
        ],
        "daily": daily,
        "trades": trades,
        "state": state,
    }


def _simulate_single(panel, rec, *, start, bars, fees, regime, saved):
    from . import factor_mine_book as fmb

    resume = None
    if saved and saved.get("state"):
        resume = dict(saved["state"])
    if start:
        return fmb.simulate_book(
            panel, rec, start=start, bars=bars, fees=fees,
            regime=regime, resume=resume,
        )
    return fmb.simulate_book(
        panel, rec, bars=bars, fees=fees, regime=regime, resume=resume,
    )


def _simulate_combo(panel, spec, members, *, start, bars, fees, regime, saved):
    from . import factor_mine_combo as fmc

    resume = dict(saved["state"]) if saved and saved.get("state") else None
    pool = spec.get("pool") or "shared"
    weights = list(spec.get("weights") or [1] * len(members))
    if pool == "split":
        return fmc.simulate_split(
            panel, members, weights, start=start, bars=bars, fees=fees,
            regime=regime, name=spec.get("name") or "combo", resume=resume,
        )
    return fmc.simulate_shared(
        panel, members, weights, start=start, bars=bars, fees=fees,
        regime=regime, net=spec.get("net") or "priority",
        name=spec.get("name") or "combo", resume=resume,
    )


def build_ledger(panel: dict, payload: dict, recipes: list[dict],
                 date: str, bars: dict, *, fees=None, regime=None) -> dict:
    """Decisions for D only. Earlier days stay in the published payload."""
    from . import factor_mine as fm
    from . import factor_mine_book as fmb

    cal = [d for d in (panel.get("session_dates") or []) if d <= date]
    prior = _prior(cal, date)
    prev_ledger = latest_ledger_before(date)
    fees = fees if fees is not None else fm.pt_fees()
    regime = regime if regime is not None else fmb.load_regime()
    by_name = {r.get("name"): r for r in recipes if r.get("name")}
    out_recipes: dict[str, dict] = {}
    failed: list[str] = []

    singles = [
        r for r in recipes
        if r.get("name") and r.get("universe") != "combo" and not r.get("members")
    ]
    for rec in singles:
        name = rec["name"]
        try:
            saved = _saved(prev_ledger, name, "primary")
            if not saved:
                bridged = bridge_published(
                    payload, name, prior, int(rec.get("hold") or 1))
                if bridged:
                    saved = {"state": bridged}
                    print(f"[factor-mine] bridge {name} from published book @ {prior}",
                          flush=True)
            book = _simulate_single(
                panel, rec, start=None, bars=bars, fees=fees,
                regime=regime, saved=saved,
            )
            primary = decision_from_book(book, date)
            starts = {}
            origin = cal[0] if cal else date
            if origin:
                starts[origin] = primary
            for start in cal:
                if start == origin:
                    continue
                if start > date:
                    continue
                saved_s = _saved(prev_ledger, name, start)
                if start == date:
                    saved_s = None
                book_s = _simulate_single(
                    panel, rec, start=start, bars=bars, fees=fees,
                    regime=regime, saved=saved_s,
                )
                starts[start] = decision_from_book(book_s, date)
            out_recipes[name] = {"primary": primary, "starts": starts}
            print(f"[factor-mine] ledger {name} {date} "
                  f"buys={len(primary.get('buys') or [])} "
                  f"starts={len(starts)}", flush=True)
        except Exception as e:  # noqa: BLE001
            failed.append(name)
            print(f"[factor-mine] ledger failed {name}: {e}", flush=True)

    for rec in recipes:
        if rec.get("universe") != "combo" and not rec.get("members"):
            continue
        name = rec.get("name")
        if not name:
            continue
        members = []
        missing = False
        for member in rec.get("members") or []:
            hit = by_name.get(member)
            if not hit:
                missing = True
                break
            members.append(hit)
        if missing or not members:
            failed.append(name)
            print(f"[factor-mine] ledger failed combo {name}: missing members",
                  flush=True)
            continue
        spec = {
            "name": name,
            "members": list(rec.get("members") or []),
            "weights": list(rec.get("weights") or []),
            "net": rec.get("net") or "priority",
            "pool": rec.get("pool") or "shared",
        }
        try:
            saved = _saved(prev_ledger, name, "primary")
            book = _simulate_combo(
                panel, spec, members, start=None, bars=bars, fees=fees,
                regime=regime, saved=saved,
            )
            primary = decision_from_book(book, date)
            starts = {}
            origin = cal[0] if cal else None
            if origin:
                starts[origin] = primary
            for start in cal:
                if start == origin or start > date:
                    continue
                saved_s = _saved(prev_ledger, name, start)
                if start == date:
                    saved_s = None
                book_s = _simulate_combo(
                    panel, spec, members, start=start, bars=bars, fees=fees,
                    regime=regime, saved=saved_s,
                )
                starts[start] = decision_from_book(book_s, date)
            out_recipes[name] = {"primary": primary, "starts": starts}
        except Exception as e:  # noqa: BLE001
            failed.append(name)
            print(f"[factor-mine] ledger failed combo {name}: {e}", flush=True)

    if failed:
        raise HoldDay(
            date, failed,
            "ledger incomplete — refusing to freeze a partial decision set",
        )
    return {
        "date": date,
        "origin": "frozen",
        "code_sha": code_sha(),
        "recipes": out_recipes,
    }


def _growth(decision: dict, prior_equity: float | None) -> float | None:
    row = decision.get("daily") or {}
    equity = row.get("equity")
    prev = row.get("yday_equity")
    if equity is None:
        return None
    base = prev if prev not in (None, 0) else prior_equity
    if not base:
        return None
    try:
        return float(equity) / float(base)
    except (TypeError, ValueError, ZeroDivisionError):
        return None


def splice_payload(payload: dict, date: str, ledger: dict, *,
                   replace: bool = False) -> dict:
    """Append D's frozen decisions. Earlier daily rows stay as they are."""
    from . import factor_mine as fm

    payload = dict(payload)
    dates = list(payload.get("dates") or [])
    if date not in dates:
        dates.append(date)
        dates.sort()
    payload["dates"] = dates
    payload["to_date"] = dates[-1] if dates else date
    payload["n_sessions"] = len(dates)
    daily_all = dict(payload.get("daily") or {})
    books = dict(payload.get("books") or {})
    series = dict(payload.get("series") or {})
    starts = dict(payload.get("starts") or {})
    capital = float(payload.get("capital") or fm.CAPITAL)

    for name, block in (ledger.get("recipes") or {}).items():
        primary = block.get("primary") or {}
        row = primary.get("daily")
        days = list(daily_all.get(name) or [])
        if replace:
            days = [d for d in days if d.get("date") != date]
        if row and not any(d.get("date") == date for d in days):
            days.append(fm._slim_dash_daily([row])[0])
        daily_all[name] = days
        book = dict(books.get(name) or {})
        trades = list(book.get("trades") or [])
        if replace:
            trades = [t for t in trades if t.get("date") != date]
        have = {(t.get("date"), t.get("side"), t.get("ticker")) for t in trades}
        for t in primary.get("trades") or []:
            key = (t.get("date"), t.get("side"), t.get("ticker"))
            if key not in have:
                trades.append(t)
                have.add(key)
        book["trades"] = trades
        state = primary.get("state") or {}
        if state.get("cash") is not None:
            book["cash"] = state.get("cash")
        book["n_trades"] = len([
            t for t in trades if t.get("side") not in ("OPEN", "CLOSE")
        ])
        books[name] = book
        eq = None if not row else row.get("equity")
        curve = list(series.get(name) or [])
        if eq is not None and (not curve or len(curve) < len(dates)):
            curve.append(eq)
        series[name] = curve
        if eq is not None:
            for stat in payload.get("stats") or []:
                if stat.get("name") == name:
                    stat["final_equity"] = eq
                    stat["total_ret_pct"] = round(100.0 * (float(eq) / capital - 1.0), 3)

        paths = list(starts.get(name) or [])
        by_start = {p.get("start"): p for p in paths}
        for start, decision in (block.get("starts") or {}).items():
            path = dict(by_start.get(start) or {"start": start, "days": []})
            sdays = list(path.get("days") or [])
            if replace:
                sdays = [d for d in sdays if d.get("date") != date]
            srow = decision.get("daily") or {}
            if srow and not any(d.get("date") == date for d in sdays):
                prev_eq = path.get("final_equity")
                if sdays and sdays[-1].get("equity") is not None:
                    prev_eq = sdays[-1].get("equity")
                growth = _growth(decision, prev_eq)
                new_eq = srow.get("equity")
                if prev_eq is not None and growth is not None and start != date:
                    new_eq = round(float(prev_eq) * growth, 2)
                sdays.append({
                    "date": date,
                    "s": srow.get("s"),
                    "hard_red": srow.get("hard_red"),
                    "bought": list(srow.get("bought") or []),
                    "sold": list(srow.get("sold") or []),
                    "cash": srow.get("cash"),
                    "equity": new_eq,
                    "open_cash": srow.get("open_cash"),
                    "made_money": bool(
                        growth is not None and growth > 1.0
                    ) if start != date else bool(srow.get("made_money")),
                })
            path["days"] = sdays
            path["n_sessions"] = len(sdays)
            if sdays and sdays[-1].get("equity") is not None:
                path["final_equity"] = sdays[-1]["equity"]
                try:
                    path["return_pct"] = round(
                        100.0 * (float(path["final_equity"]) / capital - 1.0), 3)
                except (TypeError, ValueError, ZeroDivisionError):
                    pass
                path["made_money"] = bool((path.get("return_pct") or 0) > 0)
            if start == date:
                path["bought"] = [b.get("ticker") for b in (decision.get("buys") or [])]
                path["buys"] = list(decision.get("buys") or [])
                path["pending"] = False
            path["start"] = start
            by_start[start] = path
        starts[name] = list(by_start.values())

    payload["daily"] = daily_all
    payload["books"] = books
    payload["series"] = series
    payload["starts"] = starts
    mornings = dict(payload.get("mornings") or {})
    if date not in mornings:
        try:
            from . import factor_mine_probe as fmp
            built = fmp.build_mornings()
            if isinstance(built, dict) and built:
                mornings = built
        except Exception as e:  # noqa: BLE001
            print(f"[factor-mine] mornings label skipped: {e}", flush=True)
    mornings.setdefault(date, {"s": None, "freeze": "appended"})
    payload["mornings"] = mornings
    return label_payload(payload)


def write_panel_file(panel: dict, path: Path | None = None) -> None:
    """Persist rows. Dates already on disk are kept if this panel omits them."""
    from . import factor_mine as fm

    path = Path(path or fm.PANEL_PATH)
    existing: dict = {}
    if path.is_file():
        try:
            existing = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            existing = {}
    panel = fm.rehydrate_panel(panel)
    have = set(panel.get("session_dates") or [])
    old_rows = [
        r for r in (existing.get("rows") or [])
        if r.get("date") and r.get("date") not in have
    ]
    rows = list(old_rows) + list(panel.get("rows") or [])
    dates = sorted({
        d for d in list(existing.get("session_dates") or []) + list(have) + [
            r.get("date") for r in rows
        ] if d
    })
    slim = {k: v for k, v in panel.items() if k != "by_date"}
    slim.update({
        "from_date": existing.get("from_date") or panel.get("from_date"),
        "to_date": dates[-1] if dates else panel.get("to_date"),
        "session_dates": dates,
        "n_sessions": len(dates),
        "n_rows": len(rows),
        "rows": rows,
        "by_date": None,
    })
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(slim, indent=2), encoding="utf-8")


def landed_dates(panel: dict) -> list[str]:
    return list(panel.get("session_dates") or [])


def append_land(from_date: str, target: str, *, write: bool = False,
                restate: list[str] | None = None,
                recipes: list[dict] | None = None,
                payload: dict | None = None,
                panel: dict | None = None) -> dict:
    """Land ``target`` from frozen history. Hold the day if bars are missing."""
    from . import factor_mine as fm

    restate_set = {str(d)[:10] for d in (restate or []) if d}
    if payload is None:
        payload = fm.load_scoreboard() if fm.OUT_JSON.is_file() else {}
    payload = dict(payload or {})
    if panel is None:
        if fm.PANEL_PATH.is_file():
            try:
                panel = json.loads(fm.PANEL_PATH.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                panel = {}
        else:
            panel = {}
    panel = apply_frozen_snapshots(fm.rehydrate_panel(panel or {}))
    have = set(landed_dates(panel))
    # Closed sessions strictly after the published board, plus restatements.
    published_dates = set(payload.get("dates") or [])
    want = fm.panel_lookback_calendar(from_date, target)
    new_dates = [
        d for d in want
        if d >= from_date and d <= target and (d not in have or d in restate_set)
        and (d not in published_dates or d in restate_set)
    ]
    # A date on the panel but not yet frozen still gets a snapshot when it
    # is the new session. Dates already published stay reconstructed.
    if target not in published_dates and target not in new_dates and target >= from_date:
        if target <= (want[-1] if want else target):
            new_dates.append(target)
    new_dates = sorted(set(new_dates))
    if not new_dates and target in published_dates and target not in restate_set:
        print(f"[factor-mine] freeze: {target} already published — no rewrite",
              flush=True)
        return label_payload(payload)

    frozen_dates = []
    for date in new_dates:
        if not fm.session_has_closed(date) and date not in restate_set:
            print(f"[factor-mine] freeze: {date} not closed — hold", flush=True)
            continue
        try:
            extra = fm.build_panel(date, date, fail_closed=True)
        except HoldDay as e:
            print(f"[factor-mine] {e}", flush=True)
            break
        try:
            candidates = prepare_lock(date)
        except HoldDay as e:
            print(f"[factor-mine] {e}", flush=True)
            break
        panel = fm.merge_panel_days(panel, extra)
        rows = [
            r for r in (panel.get("rows") or []) if r.get("date") == date
        ]
        prior = _prior(list(panel.get("session_dates") or []), date)
        pinned = pin_prices(date, [r.get("ticker") for r in rows])
        try:
            write_candidates(date, candidates, restate=date in restate_set)
        except FrozenHistory as e:
            print(f"[factor-mine] {e}", flush=True)
            candidates = read_json(candidate_path(date)) or candidates
        try:
            prices_sha = write_price_pin(date, pinned, restate=date in restate_set)
        except FrozenHistory as e:
            print(f"[factor-mine] {e}", flush=True)
            prices_sha = (load_manifest().get("prices") or {}).get(date, {}).get("sha256")
            pinned = read_json(price_path(date)) or pinned
        snap = make_snapshot(date, rows, prior, prices_sha, candidates)
        try:
            write_snapshot(date, snap, restate=date in restate_set)
        except FrozenHistory as e:
            print(f"[factor-mine] {e}", flush=True)
        panel = apply_frozen_snapshots(panel)
        bars = bars_for_decisions(panel, date, pinned)
        recs = list(recipes or payload.get("recipes") or [])
        existing = read_ledger(date)
        if existing and date not in restate_set:
            ledger = existing
            print(f"[factor-mine] ledger {date} already frozen — splicing",
                  flush=True)
        else:
            ledger = build_ledger(panel, payload, recs, date, bars)
            try:
                write_ledger(date, ledger, restate=date in restate_set)
            except FrozenHistory as e:
                print(f"[factor-mine] {e}", flush=True)
                ledger = read_ledger(date) or ledger
        payload = splice_payload(
            payload, date, ledger, replace=date in restate_set)
        payload["n_rows"] = panel.get("n_rows")
        frozen_dates.append(date)
        print(f"[factor-mine] appended frozen session {date}", flush=True)

    payload = label_payload(payload)
    if write and frozen_dates:
        write_panel_file(panel)
        fm.write_outputs(payload, stats=payload.get("stats") or [], books=payload.get("books"))
    elif write:
        print("[factor-mine] freeze: nothing new written", flush=True)
    return payload
