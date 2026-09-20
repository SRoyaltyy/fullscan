"""Stage 2 — leak-safe Grok-news standalone catalyst book + overlay.

What this answers:
  1. Every Grok-pipeline article knowable before each trading day
  2. Which listed stocks that article names EXACTLY
  3. Standalone: long those policy/gov names (they are not on hot4)
  4. Overlay: same signals on ``union_hot_n4_h1`` / ``flatten_h5``

Reads ONLY:
  * frozen ``data/grok_automations/*.json``
  * Grok pipeline: ``01_daily/news/{D}_{parsed,actions,judge}.json``
    (never ``*_close*``), ``01_daily/events/{D}_events.json``
  * PRIOR-session Company / Sector / Industry (D-1 Finviz) to resolve
    a named company → ticker. Never same-day tape. Never “Finviz
    attached this headline to ticker X”. Never sector-basket expansion.

No live web, no live automation API, no same-day close digest, no
post-close research_baseline, no OOS peek while mapping IS.

Fill = next official 09:30 STRICTLY AFTER known_at. A 09:30:00 ET print
is too late for that open.

Overlays (long-only, hard-red S≤−3 sit):
  * veto_bear     — drop exact-bearish names from the strategy list
  * require_bull  — keep only strategy names the article named bullish
  * add_named     — add exact-bullish names the strategy missed
  * full          — veto_bear + add_named + exit held names on bearish

Not live. Does not touch factor-mine / flatten / Webull.
"""
from __future__ import annotations

import argparse
import csv
import json
import re
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Iterable
from zoneinfo import ZoneInfo

from . import factor_mine as fm
from . import factor_mine_book as fmb
from . import grok_automation_harvest as harvest
from . import news_parse as np
from . import ticker_lookback as tl

ROOT = Path(__file__).resolve().parent.parent
AUTO_DIR = ROOT / "data" / "grok_automations"
EXPORT_DIR = ROOT / "data" / "exports"
NEWS_DIR = ROOT / "01_daily" / "news"
EVENTS_DIR = ROOT / "01_daily" / "events"
OUT_MD = ROOT / "03_scoreboard" / "GROK_NEWS_TICKER_BT.md"
OUT_JSON = ROOT / "03_scoreboard" / "grok_news_ticker_bt.json"
DASH_DIR = ROOT / "dashboard" / "factor-mine-grok-news"
WINDOW_START = "2026-08-13"
IS_END = "2026-09-09"
OOS_START = "2026-09-10"
ET = ZoneInfo("America/New_York")
CLOSE_NAME = re.compile(r"close|research_baseline", re.I)
STALE_MAX_DAYS = 2
ADD_CAP = 4
BASE_NAMES = ("union_hot_n4_h1", "flatten_h5")
OVERLAY_MODES = ("base", "veto_bear", "require_bull", "add_named", "full")

# ── drop / keep ──────────────────────────────────────────────────────
DROP_TITLE = re.compile(
    r"(?i)("
    r"files? with the sec|"
    r"form\s+(8-k|10-q|10-k|s-3|s-4|13d|13g|4)\b|"
    r"registration statement|"
    r"q[1-4]\b.{0,24}(earnings|eps|revenue)|"
    r"\bearnings\b.{0,20}\b(beat|miss|call|release|preview)\b|"
    r"(beat|miss)(es|ed)? (on )?(eps|earnings|revenue)|"
    r"schedules?.{0,40}earnings|"
    r"jim cramer|cramer (believes|reveals|shares|says|didn)|"
    r"stock of the day|top stocks to|should you buy|"
    r"is it too late to buy|here'?s what to know|"
    r"declares? (a )?(quarterly )?cash dividend|"
    r"announces date of .{0,40}(results|conference call)"
    r")"
)
HORMUZ = re.compile(r"(?i)\b(hormuz|strait of hormuz|iran.{0,20}tanker)\b")
HORMUZ_SHOCK = re.compile(
    r"(?i)\b(struck|strikes?|detained|seized|attack(?:ed|s)?|mined|"
    r"blockade|closed the strait|new (attack|shock|strike))\b"
)
POLICY_KEEP = re.compile(
    r"(?i)("
    r"\b(sec|fda|doj|epa|ftc|fcc|cftc|cms|occ|fdic|nlrb|osha|cpsc|itc|bis)\b|"
    r"federal reserve|fed chair|fomc|fed funds|dot plot|chair powell|"
    r"kevin warsh|\bwarsh\b|"
    r"white house|federal register|executive order|"
    r"\btariff|\bban(?:s|ned|ning)?\b|\bexemption|"
    r"tokeniz|regulation crypto|safe harbor|\batkins\b|"
    r"antitrust|chips act|\bghg\b|greenhouse gas|"
    r"export control|section 301|de minimis|"
    r"supreme court|court of (appeals|international trade)|"
    r"\bbill\b.{0,30}(congress|senate|house)|"
    r"(congress|senate|house).{0,30}\bbill\b|"
    r"rate (cut|hike|hold)|interest[- ]rate|"
    r"\bthaad\b|\bpentagon\b|defense contract"
    r")"
)
SINGLE_FDA = re.compile(
    r"(?i)\b(fda (approval|clearance|nod|crl|fast track)|receives? fda|"
    r"fda (approves?|clears?|grants?|designation)|fast track designation)\b"
)
YAHOO_TABLOID = re.compile(r"(?i)(yahoo|seeking alpha|motley fool|benzinga)")

# Theme packs inform polarity of a NAMED ticker only. Never expand a
# sector basket (EPA ≠ every coal plant; Hormuz ≠ COP/EOG book).
THEME_PACKS: list[dict] = [
    {
        "id": "crypto_sec",
        "rx": re.compile(
            r"(?i)(\batkins\b|tokeniz|regulation crypto|tokenized|"
            r"sec.{0,48}(exemption|safe harbor|greenlight)|"
            r"clarity act|crypto (exemption|framework|rule))"
        ),
        "polarity": "bullish",
    },
    {
        "id": "epa_ghg",
        "rx": re.compile(
            r"(?i)(epa.{0,40}(ghg|greenhouse|carbon|repeal)|"
            r"greenhouse gas.{0,24}(repeal|rule|standard))"
        ),
        "polarity": "bullish",
    },
    {
        "id": "fed_path",
        "rx": re.compile(
            r"(?i)(federal reserve|fed chair|fomc|fed (rate|hike|cut)|"
            r"warsh|chair powell|rate hike|rate cut|fed funds)"
        ),
        "polarity": None,
    },
    {
        "id": "tariff",
        "rx": re.compile(
            r"(?i)(tariff|section 301|section 232|de minimis|"
            r"import ban|export control|chips act)"
        ),
        "polarity": "bearish",
    },
    {
        "id": "hormuz_new",
        "rx": HORMUZ,
        "polarity": None,
    },
    {
        "id": "oil_inventory",
        "rx": re.compile(
            r"(?i)(oil inventor|eia crude|crude inventor|opec|"
            r"wti|brent.{0,20}inventor)"
        ),
        "polarity": None,
    },
    {
        "id": "antitrust",
        "rx": re.compile(r"(?i)(antitrust|doj.{0,30}(suit|probe|case)|no breakup)"),
        "polarity": None,
    },
    {
        "id": "fda_policy",
        "rx": re.compile(
            r"(?i)(fda (guidance|ban|policy|class|labeling rule)|"
            r"cms (rate|rule)|ira drug|drug pricing)"
        ),
        "polarity": None,
    },
    {
        "id": "chips_export",
        "rx": re.compile(
            r"(?i)(chips act|export control|bis\b|huawei ban|"
            r"advanced (node|chip) restrict)"
        ),
        "polarity": None,
    },
    {
        "id": "sre_rin",
        "rx": re.compile(
            r"(?i)(small[- ]refinery exemption|\bsre\b|rin (market|waiver))"
        ),
        "polarity": "bullish",
    },
]

BULL = re.compile(
    r"(?i)\b(exemption|greenlight|approves?|clarity|repeal|"
    r"rate cut|easing|dovish|safe harbor|wins?|clears?|lifts?|"
    r"debuts?|surge|rall(?:y|ies))\b"
)
BEAR = re.compile(
    r"(?i)\b(ban|hike|hawkish|tariff|probe|suit|block|"
    r"inventory build|tightening|enforcement|fine|slide|"
    r"drop|drops|fell|falls|slump)\b"
)

# Agency / English tokens that look like tickers.
_TICKER_DENY = frozenset({
    "A", "I", "IT", "THE", "FOR", "AND", "OR", "ON", "TO", "OF", "IN",
    "AT", "BY", "AN", "BE", "SO", "IF", "AS", "NO", "YES", "ALL", "NEW",
    "US", "USA", "UK", "EU", "UN", "CEO", "CFO", "IPO", "ETF", "ETN",
    "ADR", "ADS", "AI", "GDP", "CPI", "PPI", "PCE", "FOMC", "NYSE",
    "SEC", "FDA", "DOJ", "EPA", "FTC", "FCC", "CMS", "OCC", "FDIC",
    "NLRB", "OSHA", "CPSC", "ITC", "BIS", "CFTC", "IRA", "USD", "WTI",
    "EIA", "OPEC", "ECB", "BOE", "IMF", "NATO", "IRS", "LLC", "INC",
    "CORP", "LTD", "PLC", "ETF", "SPX", "NDX", "DXY", "VIX", "Q",
    "AM", "PM", "ET", "PT", "CT", "MT", "BMO", "AMC",
    "GHG", "BTC", "ARM", "TACO", "NMS", "NDAQ",
})
_TICKER_IN_TEXT = re.compile(r"(?:[$()]|\b)([A-Z]{1,5})(?:\b|[)$])")
_NAME_STOP = {
    "class", "shares", "holdings", "company", "corp", "inc", "ltd", "plc",
    "group", "the", "and", "fund", "trust", "etf", "index", "global",
    "share", "first", "income", "equity", "growth", "value", "world",
    "united", "states", "international", "capital", "markets", "financial",
    "partners", "advisors", "limited", "ordinary", "common", "stock",
    "american", "national", "nation", "services", "resources", "industries",
    "solutions", "systems", "technologies", "energy", "mining", "bank",
    "gold", "silver", "holdings", "entertainment", "software",
    "reserve", "federal", "yields", "yield", "change", "agents", "live",
    "invest", "investor", "investors", "trader", "traders", "currency",
    "opportunistic", "green", "acquisition", "income", "strategy",
    "decline", "retail", "store", "thermal", "cooling", "power",
    "management", "conductor", "signal", "jackson", "arrow", "crown",
    "daily", "you", "with", "fed", "index", "etn", "shares",
    "interest", "dividend", "premium", "treasury", "policy", "outlook",
    "protection", "regional", "florida", "solutions", "support", "action",
    "associated", "public", "holding", "controls", "practice", "advisor",
    "assets", "broadcast", "matters", "exemption", "innovation", "nasdaq",
    "greenhouse", "security", "physical",
}
WRAP_TITLE = re.compile(
    r"(?i)("
    r"stock market today|"
    r"\bin focus\b|"
    r"live coverage|"
    r"futures (rise|advance|sink|mixed|drop|end)|"
    r"^(nasdaq|dow|s&p 500).{0,40}(dow|nasdaq|s&p)"
    r")"
)
_ETF_OR_SHELL = re.compile(
    r"(?i)(exchange traded|etn\b|\betf\b|shell compan|closed-end fund)"
)
_SOURCE_TAIL = re.compile(r"\s+[-–—]\s+[^-–—]{2,48}$")
_PAREN_TICKER = re.compile(r"\$([A-Z]{1,5})\b|\(([A-Z]{1,5})\)")
_WORD_TICKER = re.compile(r"\b([A-Z]{3,5})\b")


def _norm(title: str) -> str:
    t = re.sub(r"[^a-z0-9]+", " ", (title or "").lower())
    return re.sub(r"\s+", " ", t).strip()[:160]


def _parse_et(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value
    else:
        text = str(value).strip()
        if not text or text in ("-", "nan", "None"):
            return None
        text = text.replace("Z", "+00:00")
        try:
            dt = datetime.fromisoformat(text)
        except ValueError:
            for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
                try:
                    dt = datetime.strptime(text[:19], fmt)
                    break
                except ValueError:
                    dt = None
            if dt is None:
                return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=ET)
    return dt.astimezone(ET)


def session_calendar(start: str = WINDOW_START, end: str | None = None) -> list[str]:
    end = end or fm.last_closed_session(start) or start
    out: list[str] = []
    d = datetime.strptime(start, "%Y-%m-%d")
    last = datetime.strptime(end, "%Y-%m-%d")
    while d <= last:
        day = d.strftime("%Y-%m-%d")
        if tl.is_trading_date(day):
            out.append(day)
        d += timedelta(days=1)
    return out


def next_open_after(known_at: datetime, cal: list[str]) -> str | None:
    """Next official 09:30 strictly after known_at. 09:30:00 is too late."""
    if known_at.tzinfo is None:
        known_at = known_at.replace(tzinfo=ET)
    known_at = known_at.astimezone(ET)
    day = known_at.strftime("%Y-%m-%d")
    too_late = (known_at.hour, known_at.minute, known_at.second) >= (9, 30, 0)
    if day in cal and not too_late:
        return day
    for s in cal:
        if s > day:
            return s
    return None


def prior_session(cal: list[str], date: str) -> str | None:
    if date not in cal:
        earlier = [d for d in cal if d < date]
        return earlier[-1] if earlier else None
    i = cal.index(date)
    return cal[i - 1] if i else None


def _file_date(path: Path) -> str | None:
    m = re.search(r"(20\d{2}-\d{2}-\d{2})", path.name)
    return m.group(1) if m else None


def is_blocked_news_path(path: Path) -> bool:
    return bool(CLOSE_NAME.search(path.name))


# ── article keep / drop ──────────────────────────────────────────────
def is_hormuz_carry(title: str, *, status: str = "") -> bool:
    if not HORMUZ.search(title or ""):
        return False
    if str(status).lower() == "carried":
        return True
    return not HORMUZ_SHOCK.search(title or "")


def is_wrap(title: str) -> bool:
    """Market-wrap / 'stocks in focus' laundry list — not a catalyst map."""
    return bool(WRAP_TITLE.search(title or ""))


def is_catalyst(title: str, *, source: str = "", status: str = "",
                category: str = "") -> bool:
    """Policy / gov / regulator article that is not a wrap."""
    if is_wrap(title):
        return False
    if not POLICY_KEEP.search(title or ""):
        return False
    return keep_article(title, source=source, status=status, category=category)


def keep_article(title: str, *, source: str = "", status: str = "",
                 category: str = "") -> bool:
    t = title or ""
    if not t or len(t) < 12:
        return False
    if is_hormuz_carry(t, status=status):
        return False
    if DROP_TITLE.search(t):
        return False
    if SINGLE_FDA.search(t) and "guidance" not in t.lower():
        return False
    if YAHOO_TABLOID.search(source or "") and not POLICY_KEEP.search(t):
        return False
    cat = (category or "").lower()
    if cat in ("earnings", "ipo") and not POLICY_KEEP.search(t):
        return False
    if POLICY_KEEP.search(t):
        return True
    if cat in ("government", "judicial"):
        return True
    if cat == "geopolitical" and HORMUZ_SHOCK.search(t):
        return True
    kind = np.classify(t, source or "")
    return kind in ("macro_relevant", "sector_relevant") and POLICY_KEEP.search(t)


def article_polarity(title: str, theme: dict | None = None) -> str:
    blob = title or ""
    forced = (theme or {}).get("polarity")
    b, e = len(BULL.findall(blob)), len(BEAR.findall(blob))
    if "fed_path" == (theme or {}).get("id"):
        if re.search(r"(?i)rate hike|hawkish|higher for longer", blob):
            return "bearish"
        if re.search(r"(?i)rate cut|dovish|easing", blob):
            return "bullish"
    if "oil_inventory" == (theme or {}).get("id"):
        if re.search(r"(?i)build|surplus", blob):
            return "bearish"
        if re.search(r"(?i)draw|deficit", blob):
            return "bullish"
    if "hormuz_new" == (theme or {}).get("id"):
        return "mixed"
    if forced in ("bullish", "bearish"):
        return forced
    if b > e:
        return "bullish"
    if e > b:
        return "bearish"
    return {"+": "bullish", "-": "bearish"}.get(np._polarity(blob), "neutral")


def match_themes(title: str) -> list[dict]:
    return [p for p in THEME_PACKS if p["rx"].search(title or "")]


def usable_known_at(stamp: datetime, file_date: str,
                    cal: list[str]) -> tuple[datetime | None, str | None]:
    """Stamp is usable only if its file is dated ≤ the fill session.

    A stale News Time on a later export cannot buy the earlier open.
    A stamp dated after the file is dropped (future peek).
    """
    if stamp.strftime("%Y-%m-%d") > file_date:
        return None, None
    fill = next_open_after(stamp, cal)
    if fill and file_date <= fill:
        return stamp, fill
    file_dt = datetime.strptime(file_date, "%Y-%m-%d").replace(
        hour=0, minute=0, second=1, tzinfo=ET)
    known = max(stamp, file_dt)
    fill2 = next_open_after(known, cal)
    if fill2 and file_date <= fill2:
        return known, fill2
    return None, None


def norm_polarity(value: Any) -> str:
    s = str(value or "").strip().lower()
    if s in ("+", "bullish", "buy", "long", "positive"):
        return "bullish"
    if s in ("-", "bearish", "sell", "short", "negative"):
        return "bearish"
    if s in ("mixed",):
        return "mixed"
    return "neutral"


# ── loaders (dated Grok-pipeline files only) ─────────────────────────
def load_coverage() -> dict:
    path = AUTO_DIR / "_coverage.json"
    if path.is_file():
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            pass
    return {"n_days": 0, "n_results": 0, "automation_get_results": False,
            "reason": "no harvest coverage file"}


def _walk_titles(obj: Any) -> Iterable[str]:
    if isinstance(obj, str):
        s = obj.strip()
        if 16 <= len(s) <= 320 and " " in s:
            yield s
        return
    if isinstance(obj, dict):
        for k, v in obj.items():
            if str(k).lower() in ("title", "headline", "news_title"):
                if isinstance(v, str) and v.strip():
                    yield v.strip()
            else:
                yield from _walk_titles(v)
        return
    if isinstance(obj, list):
        for x in obj[:400]:
            yield from _walk_titles(x)


def _stamp_article(title: str, stamp: datetime | None, file_date: str,
                   cal: list[str], **extra) -> dict | None:
    if stamp is None:
        stamp = datetime.strptime(file_date, "%Y-%m-%d").replace(
            hour=0, minute=0, second=1, tzinfo=ET)
    known, fill = usable_known_at(stamp, file_date, cal)
    if not known or not fill:
        return None
    row = {
        "title": title,
        "file_date": file_date,
        "known_at": known.isoformat(),
        "fill": fill,
        "url": extra.pop("url", "") or "",
        "digest": extra.pop("digest", "") or "",
        "task": extra.pop("task", "") or "",
        "conversationId": extra.pop("conversationId", "") or "",
    }
    row.update(extra)
    return row


def load_frozen_articles(cal: list[str]) -> list[dict]:
    rows: list[dict] = []
    if not AUTO_DIR.is_dir():
        return rows
    for path in sorted(AUTO_DIR.glob("*.json")):
        if path.name.startswith("_"):
            continue
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        file_date = payload.get("date") or _file_date(path)
        if not file_date or file_date < WINDOW_START:
            continue
        results = payload.get("results") if isinstance(payload, dict) else None
        if not isinstance(results, list):
            results = [payload]
        for res in results:
            if not isinstance(res, dict):
                continue
            created = _parse_et(res.get("createTime"))
            raw = res.get("raw")
            titles = list(_walk_titles(raw))
            if not titles and isinstance(raw, str):
                titles = [ln.strip() for ln in raw.splitlines() if len(ln.strip()) > 16]
            for title in titles:
                if not keep_article(title, source="automation"):
                    continue
                art = _stamp_article(
                    title, created, file_date, cal,
                    source="automation",
                    task=res.get("task") or payload.get("task"),
                    conversationId=res.get("conversationId") or "",
                )
                if art:
                    rows.append(art)
    return rows


def load_parsed_articles(cal: list[str]) -> list[dict]:
    """Grok news-parsing automation output (pre-open ``generated_at``)."""
    rows: list[dict] = []
    for path in sorted(NEWS_DIR.glob("*_parsed.json")):
        if is_blocked_news_path(path):
            continue
        file_date = _file_date(path)
        if not file_date or file_date < WINDOW_START:
            continue
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        gen = _parse_et(data.get("generated_at"))
        seen: set[str] = set()
        items = list(data.get("usable_top") or []) + list(data.get("all_items") or [])
        for it in items:
            title = (it.get("title") or "").strip()
            key = _norm(title)
            if not title or key in seen:
                continue
            seen.add(key)
            if it.get("class") == "noise" and not keep_article(
                title, source=it.get("source") or "parsed",
            ):
                continue
            if it.get("class") == "single_name" and not keep_article(
                title, source=it.get("source") or "parsed",
            ):
                continue
            if not it.get("usable") and not keep_article(
                title, source=it.get("source") or "parsed",
            ):
                continue
            stamp = _parse_et(it.get("published_at")) or gen
            art = _stamp_article(
                title, stamp, file_date, cal,
                source="parsed",
                url=it.get("url") or "",
                grok_polarity=norm_polarity(it.get("polarity")),
                grok_class=it.get("class") or "",
            )
            if art:
                rows.append(art)
    return rows


def load_actions_articles(cal: list[str]) -> list[dict]:
    """Grok reasoned-event evidence titles (pre-open ``generated_at``).

    ``ticker_actions`` baskets are ignored unless the ticker is named in
    that event's evidence titles.
    """
    rows: list[dict] = []
    for path in sorted(NEWS_DIR.glob("*_actions.json")):
        if is_blocked_news_path(path):
            continue
        file_date = _file_date(path)
        if not file_date or file_date < WINDOW_START:
            continue
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        gen = _parse_et(data.get("generated_at"))
        for ev in data.get("reasoned_events") or []:
            fw = ev.get("framework") or {}
            if str(fw.get("keep") or "").lower() == "drop":
                continue
            ev_pol = norm_polarity(fw.get("polarity"))
            event = ev.get("event") or ""
            for evd in ev.get("evidence") or []:
                title = (evd.get("title") or "").strip()
                if not keep_article(title, source=evd.get("source") or "actions"):
                    continue
                stamp = _parse_et(evd.get("published_at")) or gen
                art = _stamp_article(
                    title, stamp, file_date, cal,
                    source="actions",
                    url=evd.get("url") or "",
                    grok_polarity=ev_pol,
                    event=event,
                )
                if art:
                    rows.append(art)
    return rows


def load_event_articles(cal: list[str]) -> list[dict]:
    rows: list[dict] = []
    for path in sorted(EVENTS_DIR.glob("*_events.json")):
        if path.name in ("latest.json",):
            continue
        file_date = _file_date(path)
        if not file_date or file_date < WINDOW_START:
            continue
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        for ev in data.get("events") or []:
            title = (ev.get("title") or "").strip()
            if not keep_article(
                title, source="events",
                status=ev.get("status") or "",
                category=ev.get("category") or "",
            ):
                continue
            stamp = _parse_et(ev.get("date_or_window") or ev.get("scan_date"))
            art = _stamp_article(
                title, stamp, file_date, cal,
                source="events",
                digest=(ev.get("why_it_matters") or "")[:400],
                category=ev.get("category"),
                status=ev.get("status"),
            )
            if art:
                rows.append(art)
    return rows


def load_judge_sides(file_date: str) -> dict[str, str]:
    """Signed judge tickers — applied only when the name is already exact."""
    path = NEWS_DIR / f"{file_date}_judge.json"
    if not path.is_file() or is_blocked_news_path(path):
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    out: dict[str, str] = {}
    for t, v in (data.get("tickers") or {}).items():
        tick = fm._tick(t)
        if not tick or tick in _TICKER_DENY:
            continue
        try:
            score = float(v)
        except (TypeError, ValueError):
            continue
        if score > 0:
            out[tick] = "bullish"
        elif score < 0:
            out[tick] = "bearish"
    return out


def dedupe_articles(rows: list[dict]) -> list[dict]:
    best: dict[str, dict] = {}
    rank = {"automation": 0, "actions": 1, "parsed": 2, "events": 3}
    for r in rows:
        key = _norm(r.get("title") or "")
        if not key:
            continue
        prev = best.get(key)
        if prev is None or rank.get(r.get("source"), 9) < rank.get(prev.get("source"), 9):
            best[key] = r
            continue
        if r.get("known_at") and (
            not prev.get("known_at") or r["known_at"] < prev["known_at"]
        ) and rank.get(r.get("source"), 9) == rank.get(prev.get("source"), 9):
            best[key] = r
    out = list(best.values())
    out.sort(key=lambda r: (r.get("known_at") or "", r.get("title") or ""))
    return out


# ── D-1 company text (resolve a named company → ticker) ──────────────
_PROFILE_CACHE: dict[str, dict[str, dict]] = {}
_NAME_INDEX: dict[str, dict[str, list[str]]] = {}
_EXPORT_DATES: list[str] | None = None


def _export_dates() -> list[str]:
    global _EXPORT_DATES
    if _EXPORT_DATES is None:
        _EXPORT_DATES = sorted(
            d for p in EXPORT_DIR.glob("finviz_20*.csv")
            if (d := _file_date(p))
        )
    return _EXPORT_DATES


def prior_export_date(fill: str, cal: list[str]) -> str | None:
    """Finviz file dated strictly before the fill session."""
    prior = prior_session(cal, fill)
    dates = [d for d in _export_dates() if d < fill]
    if prior and prior in dates:
        return prior
    return dates[-1] if dates else None


def _company_keys(company: str) -> list[str]:
    c = re.sub(
        r"\b(inc|corp|ltd|plc|llc|sa|ag|co|the|holdings|holding|"
        r"class [a-z]|ordinary shares|ads|adr|limited)\b",
        " ",
        company or "",
        flags=re.I,
    )
    c = re.sub(r"[^a-z0-9&. -]+", " ", c.lower())
    c = re.sub(r"\s+", " ", c).strip()
    keys: list[str] = []
    if len(c) >= 6:
        keys.append(c)
    for w in c.split():
        if len(w) >= 6 and w not in _NAME_STOP:
            keys.append(w)
    # two-word names ("bank of america", "duke energy")
    parts = [w for w in c.split() if w not in _NAME_STOP]
    if len(parts) >= 2:
        pair = " ".join(parts[:2])
        if len(pair) >= 8:
            keys.append(pair)
    return keys


def load_prior_profiles(fill: str, cal: list[str]) -> dict[str, dict]:
    """Company / Sector / Industry from D-1 Finviz only.

    Never Change%, Gap, RelVol, never same-day attachment ticker.
    """
    exp = prior_export_date(fill, cal)
    if not exp:
        return {}
    if exp in _PROFILE_CACHE:
        return _PROFILE_CACHE[exp]
    path = EXPORT_DIR / f"finviz_{exp}.csv"
    out: dict[str, dict] = {}
    if not path.is_file():
        _PROFILE_CACHE[exp] = out
        return out
    keep = ("Ticker", "Company", "Sector", "Industry", "Tags",
            "Sector/Theme", "ETF Type")
    try:
        with path.open(newline="", encoding="utf-8", errors="replace") as fh:
            for rec in csv.DictReader(fh):
                t = fm._tick(rec.get("Ticker"))
                if not t:
                    continue
                bits = [str(rec.get(c) or "") for c in keep if c != "Ticker"]
                out[t] = {
                    "ticker": t,
                    "company": str(rec.get("Company") or ""),
                    "sector": str(rec.get("Sector") or ""),
                    "industry": str(rec.get("Industry") or ""),
                    "text": " ".join(bits).lower(),
                    "export": exp,
                }
    except OSError:
        pass
    _PROFILE_CACHE[exp] = out
    _index_profiles(exp, out)
    return out


def _is_etf_or_shell(prof: dict) -> bool:
    blob = f"{prof.get('industry') or ''} {prof.get('company') or ''}"
    return bool(_ETF_OR_SHELL.search(blob))


def _index_profiles(exp: str, profiles: dict[str, dict]) -> None:
    names: dict[str, list[str]] = defaultdict(list)
    for t, p in profiles.items():
        if _is_etf_or_shell(p):
            continue
        for key in _company_keys(p.get("company") or ""):
            names[key].append(t)
    _NAME_INDEX[exp] = names


def _ensure_index(profiles: dict[str, dict]) -> str | None:
    if not profiles:
        return None
    exp = next(iter(profiles.values()), {}).get("export")
    if not exp:
        return None
    if _PROFILE_CACHE.get(exp) is profiles and exp in _NAME_INDEX:
        return exp
    _index_profiles(exp, profiles)
    return exp


def exact_named_tickers(blob: str, profiles: dict[str, dict]) -> set[str]:
    """Tickers the article names — ticker token or unique company name.

    Does not expand a theme to a sector book. Skips ETF / shell-company
    name matches (``Reserve`` ≠ ARCM; ``Fed`` ≠ IFED). One- and two-
    letter tokens only count in ``$TICK`` / ``(TICK)``.
    """
    found: set[str] = set()
    if not blob or not profiles:
        return found
    cleaned = _SOURCE_TAIL.sub("", blob)
    cleaned = re.sub(r"\bU\.S\.?\b", " ", cleaned)
    cleaned = re.sub(r"\bS\s*&\s*P\b", " ", cleaned, flags=re.I)
    for a, b in _PAREN_TICKER.findall(cleaned):
        t = a or b
        if t and t not in _TICKER_DENY and t in profiles:
            found.add(t)
    for t in _WORD_TICKER.findall(cleaned):
        if t in _TICKER_DENY or t not in profiles:
            continue
        if len(t) < 4 and not re.search(
            rf"(?:\(|\$){t}\)|{t}\s*(?:drop|drops|surge|rall|shares|stock|%|jump|fell)",
            cleaned,
        ):
            continue  # bare GHG / NMS / ARM stay out; "DVN drop" / (B) stay in
        found.add(t)
    exp = _ensure_index(profiles)
    nidx = _NAME_INDEX.get(exp) or {}
    blob_l = cleaned.lower()
    for key, tickers in nidx.items():
        if not re.search(rf"\b{re.escape(key)}\b", blob_l):
            continue
        uniq = list(dict.fromkeys(tickers))
        if " " not in key and len(uniq) != 1:
            continue
        if len(uniq) > 2:
            continue
        found.update(uniq)
    return found


def map_tickers(article: dict, profiles: dict[str, dict]) -> list[dict]:
    """Exact named overlap only. No sector-wide dump, no Finviz row ticker."""
    title = article.get("title") or ""
    digest = article.get("digest") or ""
    blob = f"{title} {digest}".strip()
    _ensure_index(profiles)
    named = exact_named_tickers(blob, profiles)
    for t in article.get("grok_tickers") or []:
        tick = fm._tick(t)
        if tick and tick in profiles and (tick in named or tick in blob):
            named.add(tick)
    if not named:
        return []
    themes = match_themes(title) or match_themes(digest)
    theme = themes[0] if themes else {"id": "named", "polarity": None}
    grok_pol = norm_polarity(article.get("grok_polarity"))
    text_pol = article_polarity(blob, theme)
    default_pol = grok_pol if grok_pol in ("bullish", "bearish") else text_pol
    sides = article.get("ticker_sides") or {}
    hits: list[dict] = []
    for t in sorted(named):
        prof = profiles.get(t) or {}
        side = norm_polarity(sides.get(t)) if sides.get(t) else default_pol
        if theme.get("id") == "hormuz_new" and side not in ("bullish", "bearish"):
            text = prof.get("text") or ""
            side = ("bearish" if re.search(r"(?i)(airline|air freight)", text)
                    else "mixed")
        hits.append({
            "ticker": t,
            "side": side if side in ("bullish", "bearish", "mixed") else "neutral",
            "signed": (3 if side == "bullish" else -3 if side == "bearish" else 0),
            "theme": theme.get("id") or "named",
            "why": f"named in article ∩ D-1 {(prof.get('company') or t)}",
            "profile_export": prof.get("export") or "",
            "exact": True,
        })
    hits.sort(key=lambda h: (-abs(h["signed"]), h["ticker"]))
    return hits


def official_bar(ticker: str, date: str) -> dict:
    return tl.session_bar(ticker, date) or {}


def catalyst_daily_scores(articles: list[dict], maps: dict[str, list[dict]],
                          cal: list[str]) -> dict[str, dict[str, int]]:
    """Long-only scores from policy/gov articles that name a ticker.

    Wraps and unsigned / bearish names stay out — no short locate.
    Cap 3 names per article so a comma list cannot become the book.
    """
    scores: dict[str, dict[str, int]] = {d: {} for d in cal}
    for art in articles:
        title = art.get("title") or ""
        if not is_catalyst(title, source=art.get("source") or ""):
            continue
        fill = art.get("fill")
        if not fill or fill not in scores:
            continue
        hits = [
            h for h in (maps.get(_norm(title)) or [])
            if int(h.get("signed") or 0) > 0
        ]
        for h in hits[:3]:
            t = h["ticker"]
            scores[fill][t] = scores[fill].get(t, 0) + int(h["signed"])
    return scores


def panel_from_scores(scores: dict[str, dict[str, int]], cal: list[str],
                      *, long_only: bool = True) -> dict:
    rows = []
    by_date: dict[str, list] = {}
    for date in cal:
        day = []
        items = scores.get(date) or {}
        ranked = sorted(items.items(), key=lambda kv: (-abs(kv[1]), kv[0]))
        src = 0
        for t, sc in ranked:
            if long_only and sc <= 0:
                continue
            src += 1
            row = {
                "date": date, "ticker": t, "sources": ["union"],
                "src_rank": src, "news_score": sc, "boxes": {},
            }
            rows.append(row)
            day.append(row)
        by_date[date] = day
    return {
        "session_dates": list(cal),
        "rows": rows,
        "by_date": by_date,
        "from_date": cal[0] if cal else "",
        "to_date": cal[-1] if cal else "",
        "n_sessions": len(cal),
        "n_rows": len(rows),
        "_ohlc_filled": True,
        "_tape_filled": True,
        "_clock_b": True,
        "_oppset": True,
    }


def run_standalone_books(scores: dict[str, dict[str, int]], cal: list[str],
                         start: str) -> tuple[dict, dict]:
    """$10k leftover book of exact named policy longs. Not an overlay."""
    panel = panel_from_scores(scores, cal)
    regime = fmb.load_regime()
    fees = fm.pt_fees()
    books, stats = {}, {}
    for top_n, hold in ((4, 1), (4, 2), (8, 1), (8, 2)):
        name = f"grok_n{top_n}_h{hold}"
        print(f"[grok-news-bt] standalone {name}", flush=True)
        rec = fm.make_recipe(
            name=name, universe="union", hold=hold, top_n=top_n,
            rank="list", side="long", day_cap=0.35,
            note="standalone exact policy-catalyst longs; no short locate; "
                 "35% day-cap so one name cannot dump the book",
        )
        book = fmb.simulate_book(
            panel, rec, fees=fees, regime=regime, start=start,
            rules={**fmb.BOOK_RULES, "hard_red_no_new": False},
        )
        books[name] = slim_book(book)
        stats[name] = book_stats(book)
    return books, stats


# ── overlay on existing strategy daily lists ─────────────────────────
def apply_overlay(base_names: list[str], news: dict[str, int], mode: str,
                  *, top_n: int, add_cap: int = ADD_CAP) -> list[str]:
    """Rewrite a strategy day's names. ``news`` is ticker → signed score."""
    bull = [t for t, s in sorted(news.items(), key=lambda kv: (-kv[1], kv[0]))
            if s > 0]
    bear = {t for t, s in news.items() if s < 0}
    names = list(base_names)
    if mode in ("base", "", None):
        return names
    if mode == "veto_bear":
        return [t for t in names if t not in bear]
    if mode == "require_bull":
        return [t for t in names if t in set(bull)]
    extra = [t for t in bull if t not in names]
    if mode == "add_named":
        room = max(0, top_n - len(names)) + add_cap
        return names + extra[:room]
    if mode == "full":
        kept = [t for t in names if t not in bear]
        room = max(0, top_n - len(kept)) + add_cap
        return kept + extra[:room]
    return names


def daily_news_scores(articles: list[dict], maps: dict[str, list[dict]],
                      cal: list[str]) -> dict[str, dict[str, int]]:
    scores: dict[str, dict[str, int]] = {d: {} for d in cal}
    for art in articles:
        fill = art.get("fill")
        if not fill or fill not in scores:
            continue
        for h in maps.get(_norm(art["title"])) or []:
            if h.get("signed") == 0:
                continue
            t = h["ticker"]
            scores[fill][t] = scores[fill].get(t, 0) + int(h["signed"])
    return scores


def apply_judge_sides(scores: dict[str, dict[str, int]],
                      articles: list[dict]) -> None:
    """Judge signed tickers only when that ticker is already exact-named."""
    by_file: dict[str, set[str]] = defaultdict(set)
    for art in articles:
        fd = art.get("file_date")
        fill = art.get("fill")
        if fd:
            by_file[fd].add(fill or "")
    for fd, fills in by_file.items():
        sides = load_judge_sides(fd)
        if not sides:
            continue
        for fill in fills:
            day = scores.get(fill)
            if not day:
                continue
            for t, side in sides.items():
                if t not in day:
                    continue
                day[t] += 2 if side == "bullish" else -2


def recipe_by_name(name: str) -> dict:
    for rec in fm.build_recipes():
        if rec.get("name") == name:
            return rec
    raise KeyError(name)


def slim_book(book: dict) -> dict:
    daily = []
    for d in book.get("daily") or []:
        daily.append({
            "date": d.get("date"),
            "s": d.get("s"),
            "hard_red": d.get("hard_red"),
            "equity": d.get("equity"),
            "mean": d.get("mean"),
            "made_money": d.get("made_money"),
            "bought": d.get("bought") or [],
            "sold": d.get("sold") or [],
            "held": d.get("held") or [],
            "open_cash": d.get("open_cash"),
        })
    return {
        "name": book.get("name"),
        "total_ret_pct": book.get("total_ret_pct"),
        "final_equity": book.get("final_equity"),
        "win_rate": book.get("win_rate"),
        "n_trades": book.get("n_trades"),
        "n_skips": book.get("n_skips"),
        "equity": book.get("equity"),
        "daily": daily,
    }


def book_stats(book: dict, starts: list[dict] | None = None) -> dict:
    days = [d for d in book.get("daily") or [] if d.get("mean") is not None]
    dollar = None if not days else round(
        sum(1 for d in days if d.get("made_money")) / len(days), 4)
    n_green = sum(1 for s in (starts or []) if s.get("made_money"))
    return {
        "name": book.get("name"),
        "book_pct": book.get("total_ret_pct"),
        "final_equity": book.get("final_equity"),
        "win_rate": book.get("win_rate"),
        "dollar_days": dollar,
        "starts_yes": None if not starts else f"{n_green}/{len(starts)}",
        "start_n": 0 if not starts else len(starts),
        "start_green": n_green,
        "n_trades": book.get("n_trades"),
        "n_skips": book.get("n_skips"),
    }


def running_book_pct(book: dict, start: str, end: str,
                     capital: float = 10000.0) -> float | None:
    daily = [d for d in (book.get("daily") or []) if d.get("equity") is not None]
    window = [d for d in daily if start <= (d.get("date") or "") <= end]
    if not window:
        return None
    prior = [d for d in daily if (d.get("date") or "") < start]
    prev = float(prior[-1]["equity"]) if prior else capital
    if not prev:
        return None
    return round(100.0 * (float(window[-1]["equity"]) / prev - 1.0), 3)


def load_baseline_panel() -> dict | None:
    if not fm.PANEL_PATH.is_file():
        return None
    try:
        raw = json.loads(fm.PANEL_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    panel = fm.rehydrate_panel(raw)
    dummy = fm.make_recipe("overlay_read", universe="union", hold=1, top_n=4)
    return fm.ensure_sim_fields(panel, dummy)


def published_baselines(start: str, end: str) -> dict:
    out = {}
    try:
        raw = json.loads(
            (ROOT / "03_scoreboard" / "factor_mine.json").read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        raw = {}
    published = {s["name"]: s for s in (raw.get("stats") or [])}
    dates = list(raw.get("dates") or [])
    series = raw.get("series") or {}
    for name in BASE_NAMES:
        st = published.get(name) or {}
        eq = list(series.get(name) or [])
        book_pct = st.get("total_ret_pct")
        running = False
        if dates and eq and start in dates and end in dates:
            i0, i1 = dates.index(start), dates.index(end)
            if 0 <= i0 <= i1 < len(eq) and not (
                start == dates[0] and end == dates[-1]
            ):
                prev = 10000.0 if i0 == 0 else eq[i0 - 1]
                if prev:
                    book_pct = round(100.0 * (eq[i1] / prev - 1.0), 3)
                    running = True
        out[name] = {
            "name": name,
            "book_pct": book_pct,
            "from_published": True,
            "window_from_running_equity": running,
        }
    return out


def _clone_row(row: dict, **extra) -> dict:
    out = dict(row)
    boxes = dict(row.get("boxes") or {})
    if extra.pop("news_bad", False):
        boxes["news"] = "bad"
    out["boxes"] = boxes
    out.update(extra)
    return out


def overlay_panel(base_panel: dict, picks_by_date: dict[str, list[str]],
                  news: dict[str, dict[str, int]], mode: str) -> dict:
    row_index = {(r["date"], r["ticker"]): r for r in (base_panel.get("rows") or [])}
    rows: list[dict] = []
    by_date: dict[str, list] = {}
    extra: list[dict] = []
    for date, names in picks_by_date.items():
        day = []
        bear = {t for t, s in (news.get(date) or {}).items() if s < 0}
        for i, t in enumerate(names, start=1):
            src = row_index.get((date, t))
            news_bad = mode in ("veto_bear", "full") and t in bear
            if src:
                row = _clone_row(
                    src, sources=["union"], src_rank=i, news_bad=news_bad,
                )
            else:
                row = {
                    "date": date, "ticker": t, "sources": ["union"],
                    "src_rank": i, "boxes": {"news": "bad"} if news_bad else {},
                }
                extra.append(row)
            day.append(row)
            rows.append(row)
        by_date[date] = day
    return {
        **{k: v for k, v in base_panel.items()
           if k not in ("rows", "by_date")},
        "rows": rows,
        "by_date": by_date,
        "_ohlc_filled": True,
        "_tape_filled": True,
        "_clock_b": True,
        "_oppset": True,
    }


def build_daily_picks(panel: dict, rec: dict, news: dict[str, dict[str, int]],
                      mode: str, cal: list[str]) -> dict[str, list[str]]:
    by_date = panel.get("by_date") or {}
    top_n = int(rec.get("top_n") or fm.TOP_N_DEFAULT)
    out: dict[str, list[str]] = {}
    for date in cal:
        chosen = fm.pick_day(by_date.get(date) or [], rec)
        names = [r["ticker"] for r in chosen]
        out[date] = apply_overlay(names, news.get(date) or {}, mode, top_n=top_n)
    return out


def overlay_touch_stats(base_picks: dict[str, list[str]],
                        overlay_picks: dict[str, list[str]],
                        news: dict[str, dict[str, int]]) -> dict:
    days_changed = 0
    vetoed = added = confirmed = 0
    overlap_days = 0
    for date, base in base_picks.items():
        ov = overlay_picks.get(date) or []
        day_news = news.get(date) or {}
        if any(t in day_news for t in base):
            overlap_days += 1
        if ov != base:
            days_changed += 1
        vetoed += sum(1 for t in base if t not in ov)
        added += sum(1 for t in ov if t not in base)
        confirmed += sum(1 for t in base if (day_news.get(t) or 0) > 0)
    return {
        "days_changed": days_changed,
        "n_days": len(base_picks),
        "overlap_days": overlap_days,
        "n_vetoed": vetoed,
        "n_added": added,
        "n_confirmed": confirmed,
    }


def run_overlay_books(panel: dict, news: dict[str, dict[str, int]],
                      cal: list[str], start: str) -> tuple[dict, dict, dict]:
    """Replay each base × overlay. Base uses the live recipe; overlays use the rewritten list."""
    regime = fmb.load_regime()
    fees = fm.pt_fees()
    books, stats, meta = {}, {}, {}
    recs = {n: recipe_by_name(n) for n in BASE_NAMES}
    for base_name, rec in recs.items():
        base_picks = build_daily_picks(panel, rec, news, "base", cal)
        for mode in OVERLAY_MODES:
            name = base_name if mode == "base" else f"{base_name}+{mode}"
            print(f"[grok-news-bt] book {name}", flush=True)
            if mode == "base":
                book = fmb.simulate_book(
                    panel, rec, fees=fees, regime=regime, start=start)
                picks = base_picks
                touch = overlay_touch_stats(base_picks, picks, news)
            else:
                picks = build_daily_picks(panel, rec, news, mode, cal)
                ov_panel = overlay_panel(panel, picks, news, mode)
                ov_rec = fm.make_recipe(
                    name=name, universe="union",
                    hold=int(rec.get("hold") or 1),
                    top_n=32, rank="list", side="long",
                    exit_when={"news": "bad"} if mode in ("veto_bear", "full") else {},
                    note=f"news overlay {mode} on {base_name}",
                )
                book = fmb.simulate_book(
                    ov_panel, ov_rec, fees=fees, regime=regime, start=start)
                touch = overlay_touch_stats(base_picks, picks, news)
            books[name] = slim_book(book)
            stats[name] = book_stats(book)
            meta[name] = {
                "base": base_name, "mode": mode, "touch": touch,
                "picks": {d: picks[d] for d in cal if picks.get(d)},
            }
    return books, stats, meta


def name_grades(articles: list[dict], maps: dict[str, list[dict]],
                cal: list[str]) -> list[dict]:
    graded = []
    for art in articles:
        fill = art.get("fill")
        if not fill or fill not in cal:
            continue
        i = cal.index(fill)
        nxt = cal[i + 1] if i + 1 < len(cal) else None
        for h in maps.get(_norm(art["title"])) or []:
            if h.get("side") not in ("bullish", "bearish"):
                graded.append({
                    "title": (art["title"] or "")[:180],
                    "ticker": h["ticker"],
                    "fill": fill,
                    "side": h["side"],
                    "theme": h.get("theme"),
                    "source": art.get("source"),
                    "named_only": True,
                })
                continue
            bar = official_bar(h["ticker"], fill)
            o, c = bar.get("open"), bar.get("close")
            if o is None:
                graded.append({
                    "title": art["title"], "ticker": h["ticker"],
                    "fill": fill, "side": h["side"],
                    "no_fill": True, "why": "missing 09:30",
                })
                continue
            same = None if c is None else round(100.0 * (float(c) / float(o) - 1.0), 3)
            nxt_c = None
            if nxt:
                b2 = official_bar(h["ticker"], nxt)
                if b2.get("close") is not None:
                    nxt_c = round(100.0 * (float(b2["close"]) / float(o) - 1.0), 3)
            pred = 1 if h["side"] == "bullish" else -1
            same_hit = None if same is None else (pred * same > 0)
            nxt_hit = None if nxt_c is None else (pred * nxt_c > 0)
            graded.append({
                "title": art["title"][:180],
                "ticker": h["ticker"],
                "fill": fill,
                "side": h["side"],
                "theme": h.get("theme"),
                "open": o,
                "close": c,
                "same_day_pct": same,
                "next_close_pct": nxt_c,
                "same_day_hit": same_hit,
                "next_close_hit": nxt_hit,
                "source": art.get("source"),
            })
    return graded


def pick_examples(graded: list[dict]) -> tuple[list[dict], list[dict]]:
    hits = [g for g in graded if g.get("same_day_hit") or g.get("next_close_hit")]
    misses = [g for g in graded if g.get("same_day_hit") is False
              and g.get("next_close_hit") is False]
    hits.sort(key=lambda g: -abs(g.get("same_day_pct") or g.get("next_close_pct") or 0))
    misses.sort(key=lambda g: -abs(g.get("same_day_pct") or g.get("next_close_pct") or 0))
    chosen_hits, seen = [], set()
    for g in hits:
        if g["ticker"] in seen:
            continue
        seen.add(g["ticker"])
        chosen_hits.append(g)
        if len(chosen_hits) == 3:
            break
    chosen_miss, seen = [], set()
    for g in misses:
        k = (g["ticker"], g.get("title"))
        if k in seen:
            continue
        seen.add(k)
        chosen_miss.append(g)
        if len(chosen_miss) == 3:
            break
    return chosen_hits, chosen_miss


def _pct(v) -> str:
    if v is None:
        return "—"
    return f"{float(v):+.2f}"


def _delta(ov, base) -> str:
    if ov is None or base is None:
        return "—"
    return f"{float(ov) - float(base):+.2f}"


def _hit_rate(graded: list[dict], field: str) -> str:
    xs = [g for g in graded if g.get(field) is not None]
    if not xs:
        return "—"
    n = sum(1 for g in xs if g.get(field))
    return f"{n}/{len(xs)} ({n / len(xs):.0%})"


def write_md(payload: dict) -> str:
    cov = payload["coverage"]
    w = payload["windows"]
    lines = [
        "# Grok news standalone catalyst book + overlay (research)",
        "",
        f"Window **{payload['from_date']} → {payload['to_date']}** · "
        f"last closed session **{payload['to_date']}**. "
        "Every pre-open Grok-pipeline article → exact named tickers. "
        "Standalone long-only book of policy/gov names (they are not on "
        "hot4). Overlay on `union_hot_n4_h1` / `flatten_h5` is secondary. "
        "Not live. Does not touch factor-mine / flatten / Webull.",
        "",
        "## Coverage",
        "",
        f"- Stage 1 automation days dumped: **{cov.get('automation_days', 0)}** "
        f"({cov.get('n_results', 0)} results).",
        f"- Grok-pipeline sessions with a pre-open file: "
        f"**{cov.get('pipeline_sessions', 0)}** of {cov.get('n_sessions', 0)}.",
        f"- Sessions filled from repo files only: **{cov.get('repo_only_sessions', 0)}** "
        f"of {cov.get('n_sessions', 0)}.",
        f"- `automation_get_results`: **{cov.get('automation_get_results')}**. "
        f"{cov.get('reason') or ''}",
        f"- Articles kept: **{cov.get('articles_kept', 0)}** "
        f"(frozen={cov.get('frozen_articles', 0)}, "
        f"actions={cov.get('actions_articles', 0)}, "
        f"parsed={cov.get('parsed_articles', 0)}, "
        f"events={cov.get('event_articles', 0)}).",
        f"- Articles that name ≥1 listed ticker: **{cov.get('articles_with_name', 0)}**.",
        f"- Exact ticker-days: **{cov.get('n_ticker_days', 0)}** "
        f"(bull/bear used by overlay: {cov.get('n_signed_ticker_days', 0)}).",
        f"- Catalyst articles (policy/gov, not a market wrap): "
        f"**{cov.get('catalyst_articles', 0)}** → "
        f"**{cov.get('catalyst_ticker_days', 0)}** standalone longs.",
        "- Raw Finviz CSV is **not** the article list. Sector baskets "
        "(Hormuz → COP/EOG, EPA → every generator) are **not** a map. "
        "Stock-market-today / in-focus laundry lists are not a catalyst.",
        "- GH Actions must not harvest. Replay reads frozen dumps + dated Grok pipeline.",
        "",
        "## Leak rules",
        "",
        "- `known_at` = automation createTime, or the pipeline file's "
        "`generated_at` / printed stamp. Earlier stamp only if that stamp "
        "sits on a file dated ≤ the fill session.",
        "- Fill = next official 09:30 **strictly after** known_at. "
        "09:30:00 ET is too late for that open. RTH → next open. "
        "Missing official open → no fill.",
        "- A ticker is affected only if the article **names** it (ticker "
        "token or unique D-1 company name). Grok `ticker_actions` baskets "
        "and theme packs do not expand the set.",
        "- No close digest, no post-close research_baseline, no OOS files "
        "while mapping an IS fill, no same-day Change%/Gap/RelVol.",
        "- Long-only (no short locate) + hard-red S≤−3 sit. $10k leftover "
        "split, Futubull fees, whole shares. Standalone holds 1 and 2 — "
        "policy prints often land on the next session, not 09:30→close.",
        "",
        "## Standalone policy-catalyst book",
        "",
        "These names are **not** on `union_hot_n4_h1` (0/26 overlap). "
        "If we do not run a standalone book, the catalysts are unused. "
        "Long-only the exact bullish names on a policy/gov article. "
        "No shorts. No wraps. Cap 3 names per article. "
        "Does **not** inherit hard-red sit (that weather gate is why "
        "Disney/FCC never traded). Day-cap 35% so one name cannot dump $10k.",
        "",
        "| Window | `grok_n4_h1` | `grok_n4_h2` | `grok_n8_h1` | `grok_n8_h2` | published hot4 | published flatten |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ]
    for key, label in (
        ("full", f"{payload['from_date']}→{payload['to_date']}"),
        ("is", f"{WINDOW_START}→{IS_END}"),
        ("oos", f"{OOS_START}→{payload['to_date']}"),
    ):
        row = w[key]
        st = row.get("standalone") or {}
        pub = row.get("published") or {}
        lines.append(
            f"| {label} | {_pct((st.get('grok_n4_h1') or {}).get('book_pct'))} | "
            f"{_pct((st.get('grok_n4_h2') or {}).get('book_pct'))} | "
            f"{_pct((st.get('grok_n8_h1') or {}).get('book_pct'))} | "
            f"{_pct((st.get('grok_n8_h2') or {}).get('book_pct'))} | "
            f"{_pct((pub.get('union_hot_n4_h1') or {}).get('book_pct'))} | "
            f"{_pct((pub.get('flatten_h5') or {}).get('book_pct'))} |"
        )
    lines += [
        "",
        f"Catalyst same-day hit rate: {w['full'].get('catalyst_same_day_hit')}. "
        f"Next-close (the hold-2 print): {w['full'].get('catalyst_next_close_hit')}.",
        "",
        "## Overlay vs same-panel base",
        "",
        "Base books are resimulated on the research panel with the live "
        "recipe (`pick_day`). Overlay books rewrite that day's list, then "
        "use the same cash book. IS / OOS are running-book splits of the "
        "full-window path. Published factor-mine numbers are a footnote "
        "only — overlay delta is vs the same-panel base.",
        "",
        "| Window | `union_hot_n4_h1` | +veto_bear | +require_bull | +add_named | +full |",
        "|---|---:|---:|---:|---:|---:|",
    ]
    for key, label in (
        ("full", f"{payload['from_date']}→{payload['to_date']}"),
        ("is", f"{WINDOW_START}→{IS_END}"),
        ("oos", f"{OOS_START}→{payload['to_date']}"),
    ):
        row = w[key]
        ov = row["overlay"]["union_hot_n4_h1"]
        lines.append(
            f"| {label} | {_pct(ov['base']['book_pct'])} | "
            f"{_pct(ov['veto_bear']['book_pct'])} "
            f"({_delta(ov['veto_bear']['book_pct'], ov['base']['book_pct'])}) | "
            f"{_pct(ov['require_bull']['book_pct'])} "
            f"({_delta(ov['require_bull']['book_pct'], ov['base']['book_pct'])}) | "
            f"{_pct(ov['add_named']['book_pct'])} "
            f"({_delta(ov['add_named']['book_pct'], ov['base']['book_pct'])}) | "
            f"{_pct(ov['full']['book_pct'])} "
            f"({_delta(ov['full']['book_pct'], ov['base']['book_pct'])}) |"
        )
    lines += [
        "",
        "| Window | `flatten_h5` | +veto_bear | +require_bull | +add_named | +full |",
        "|---|---:|---:|---:|---:|---:|",
    ]
    for key, label in (
        ("full", f"{payload['from_date']}→{payload['to_date']}"),
        ("is", f"{WINDOW_START}→{IS_END}"),
        ("oos", f"{OOS_START}→{payload['to_date']}"),
    ):
        row = w[key]
        ov = row["overlay"]["flatten_h5"]
        lines.append(
            f"| {label} | {_pct(ov['base']['book_pct'])} | "
            f"{_pct(ov['veto_bear']['book_pct'])} "
            f"({_delta(ov['veto_bear']['book_pct'], ov['base']['book_pct'])}) | "
            f"{_pct(ov['require_bull']['book_pct'])} "
            f"({_delta(ov['require_bull']['book_pct'], ov['base']['book_pct'])}) | "
            f"{_pct(ov['add_named']['book_pct'])} "
            f"({_delta(ov['add_named']['book_pct'], ov['base']['book_pct'])}) | "
            f"{_pct(ov['full']['book_pct'])} "
            f"({_delta(ov['full']['book_pct'], ov['base']['book_pct'])}) |"
        )
    touch = payload.get("touch") or {}
    lines += [
        "",
        "### How often news actually touched the list (full window)",
        "",
        "| Base | overlay | days changed | list ∩ named | vetoed names | added names | confirmed bulls |",
        "|---|---|---:|---:|---:|---:|---:|",
    ]
    for base in BASE_NAMES:
        for mode in ("veto_bear", "require_bull", "add_named", "full"):
            t = ((touch.get(base) or {}).get(mode) or {})
            lines.append(
                f"| `{base}` | `{mode}` | {t.get('days_changed', 0)}/"
                f"{t.get('n_days', 0)} | {t.get('overlap_days', 0)} | "
                f"{t.get('n_vetoed', 0)} | {t.get('n_added', 0)} | "
                f"{t.get('n_confirmed', 0)} |"
            )
    pub = (w["full"].get("published") or {})
    lines += [
        "",
        "### Published factor-mine footnote (not the overlay delta)",
        "",
        f"- Published `union_hot_n4_h1`: {_pct((pub.get('union_hot_n4_h1') or {}).get('book_pct'))}. "
        f"Published `flatten_h5`: {_pct((pub.get('flatten_h5') or {}).get('book_pct'))}.",
        "- Same-panel resim can differ from the published phone book "
        "(start-on / lookback). Overlay minus base uses the resim.",
        "",
        "## 3 hits (exact named, directional)",
        "",
    ]
    for g in payload["examples"]["hits"]:
        lines.append(
            f"- **{g['ticker']}** `{g['side']}` fill {g['fill']} "
            f"same-day {_pct(g.get('same_day_pct'))} / next-close "
            f"{_pct(g.get('next_close_pct'))} — {g.get('title')}"
        )
    if not payload["examples"]["hits"]:
        lines.append("- —")
    lines += ["", "## 3 misses", ""]
    for g in payload["examples"]["misses"]:
        lines.append(
            f"- **{g['ticker']}** `{g['side']}` fill {g['fill']} "
            f"same-day {_pct(g.get('same_day_pct'))} / next-close "
            f"{_pct(g.get('next_close_pct'))} — {g.get('title')}"
        )
    if not payload["examples"]["misses"]:
        lines.append("- —")
    lines += [
        "",
        f"Exact named same-day hit rate: {w['full'].get('same_day_hit')}. "
        f"Next-close: {w['full'].get('next_close_hit')}.",
        "",
        "## Method",
        "",
        "1. Stage 1 harvest (Cursor / Grok Bot, not GH Action) dumps each "
        "automation result since 2026-08-13 into "
        "`data/grok_automations/{date}_{task}.json`. This environment has "
        "no `automation_get_results`, so Stage 2 reads the dated Grok "
        "pipeline the news-parsing automation already wrote "
        "(`*_parsed.json`, `*_actions.json` evidence, events).",
        "2. Keep policy / regulator / exemption / ban / tariff / Fed / "
        "court / bill rows. Drop earnings PR, SEC filings, Yahoo/Cramer "
        "tabloid, single-name FDA approvals, carried Hormuz.",
        "3. A stock is affected only when the article names it. Company "
        "names resolve through D-1 Finviz Company text. Theme packs set "
        "polarity of a named name; they do not add unnamed names. "
        "3-letter tokens only in `$TICK` / `(TICK)`. Wraps are not maps.",
        "4. Standalone book: long the bullish named names on a policy/gov "
        "article (`grok_n4/n8` × hold 1/2). These names are not on hot4.",
        "5. Overlay: `pick_day` the live recipe, then rewrite "
        "(`veto_bear` / `require_bull` / `add_named` / `full`). "
        "`full` marks held names news🔴 when the article names them bearish.",
        "",
    ]
    return "\n".join(lines) + "\n"


def write_dash(payload: dict) -> None:
    DASH_DIR.mkdir(parents=True, exist_ok=True)
    slim = {
        "from_date": payload["from_date"],
        "to_date": payload["to_date"],
        "coverage": payload["coverage"],
        "windows": payload["windows"],
        "touch": payload.get("touch"),
        "examples": payload["examples"],
    }
    (DASH_DIR / "payload.json").write_text(
        json.dumps(slim, indent=2), encoding="utf-8")
    html = """<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Grok news overlay — research</title>
<style>
:root{--bg:#0f1420;--card:#171e2e;--line:#262f45;--fg:#dfe6f2;--mut:#8b96ab;--pos:#4ade80;--neg:#f87171;--gold:#fbbf24}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--fg);font:13px/1.45 -apple-system,Segoe UI,sans-serif}
.wrap{max-width:1040px;margin:0 auto;padding:16px}h1{font-size:18px;margin:0 0 6px}h2{font-size:14px;margin:0 0 8px}
.sub{color:var(--mut);margin:0 0 12px}table{width:100%;border-collapse:collapse;margin:8px 0 16px}
th,td{padding:5px 6px;border-bottom:1px solid var(--line);text-align:right}th:first-child,td:first-child{text-align:left}
.pos{color:var(--pos)}.neg{color:var(--neg)}.note{color:var(--mut);font-size:12px}
.card{background:var(--card);border:1px solid var(--line);border-radius:10px;padding:10px 12px;margin:0 0 12px}
</style></head><body><div class="wrap">
<h1>Grok news standalone + overlay</h1>
<p class="sub">Research only. Exact named policy/gov catalysts as their own book, then the same signals overlaid on union_hot_n4_h1 and flatten_h5. Live factor-mine / flatten / Webull untouched.</p>
<div class="card" id="cov"></div>
<div class="card"><h2>Standalone policy-catalyst book</h2><table id="solo"></table></div>
<div class="card"><h2>union_hot_n4_h1 overlay</h2><table id="hot"></table></div>
<div class="card"><h2>flatten_h5 overlay</h2><table id="flat"></table></div>
<div class="card"><h2>List touch</h2><table id="touch"></table></div>
<div class="card"><h2>Hits / misses (exact named)</h2><div id="ex"></div></div>
<p class="note">Fill = next 09:30 strictly after known_at. Named ticker or unique D-1 company only. Long-only + hard-red sit.</p>
</div>
<script>
fetch('payload.json').then(r=>r.json()).then(d=>{
  const cov=d.coverage||{};
  document.getElementById('cov').innerHTML =
    '<b>Coverage</b><div>automation days dumped: '+(cov.automation_days||0)+
    ' · pipeline sessions: '+(cov.pipeline_sessions||0)+'/'+(cov.n_sessions||0)+
    ' · articles: '+(cov.articles_kept||0)+
    ' · named: '+(cov.articles_with_name||0)+
    ' · ticker-days: '+(cov.n_ticker_days||0)+
    ' · catalyst longs: '+(cov.catalyst_ticker_days||0)+
    '</div><div class="note">'+(cov.reason||'')+'</div>';
  const pct=v=>v==null?'—':((v>=0?'+':'')+Number(v).toFixed(2));
  const cls=v=>v==null?'':(v>=0?'pos':'neg');
  const cell=(v,base)=>{
    if(v==null) return '—';
    const d = base==null? null : (Number(v)-Number(base));
    const extra = d==null||base==null?'': ' <span class="'+cls(d)+'">('+pct(d)+')</span>';
    return '<span class="'+cls(v)+'">'+pct(v)+'</span>'+extra;
  };
  const rows=(key)=>{
    let h='<tr><th>Window</th><th>base</th><th>veto_bear</th><th>require_bull</th><th>add_named</th><th>full</th></tr>';
    for (const [k,label] of [['full','full'],['is','08-13→09-09'],['oos','09-10→end']]){
      const w=d.windows[k]; if(!w) continue;
      const ov=w.overlay[key];
      const b=ov.base.book_pct;
      h+='<tr><td>'+label+'</td><td>'+cell(b,null)+
        '</td><td>'+cell(ov.veto_bear.book_pct,b)+
        '</td><td>'+cell(ov.require_bull.book_pct,b)+
        '</td><td>'+cell(ov.add_named.book_pct,b)+
        '</td><td>'+cell(ov.full.book_pct,b)+'</td></tr>';
    }
    return h;
  };
  let sh='<tr><th>Window</th><th>n4 h1</th><th>n4 h2</th><th>n8 h1</th><th>n8 h2</th></tr>';
  for (const [k,label] of [['full','full'],['is','08-13→09-09'],['oos','09-10→end']]){
    const w=d.windows[k]; if(!w||!w.standalone) continue;
    const s=w.standalone;
    sh+='<tr><td>'+label+'</td><td class="'+cls(s.grok_n4_h1&&s.grok_n4_h1.book_pct)+'">'+pct(s.grok_n4_h1&&s.grok_n4_h1.book_pct)+
      '</td><td class="'+cls(s.grok_n4_h2&&s.grok_n4_h2.book_pct)+'">'+pct(s.grok_n4_h2&&s.grok_n4_h2.book_pct)+
      '</td><td class="'+cls(s.grok_n8_h1&&s.grok_n8_h1.book_pct)+'">'+pct(s.grok_n8_h1&&s.grok_n8_h1.book_pct)+
      '</td><td class="'+cls(s.grok_n8_h2&&s.grok_n8_h2.book_pct)+'">'+pct(s.grok_n8_h2&&s.grok_n8_h2.book_pct)+'</td></tr>';
  }
  document.getElementById('solo').innerHTML=sh;
  document.getElementById('hot').innerHTML=rows('union_hot_n4_h1');
  document.getElementById('flat').innerHTML=rows('flatten_h5');
  let th='<tr><th>Base</th><th>overlay</th><th>days changed</th><th>list ∩ named</th><th>vetoed</th><th>added</th></tr>';
  const touch=d.touch||{};
  for (const base of ['union_hot_n4_h1','flatten_h5']){
    for (const mode of ['veto_bear','require_bull','add_named','full']){
      const t=(touch[base]||{})[mode]||{};
      th+='<tr><td>'+base+'</td><td>'+mode+'</td><td>'+(t.days_changed||0)+'/'+(t.n_days||0)+
        '</td><td>'+(t.overlap_days||0)+'</td><td>'+(t.n_vetoed||0)+'</td><td>'+(t.n_added||0)+'</td></tr>';
    }
  }
  document.getElementById('touch').innerHTML=th;
  const ex=d.examples||{};
  const li=(arr,tag)=>'<p><b>'+tag+'</b></p><ul>'+(arr||[]).map(g=>
    '<li><code>'+g.ticker+'</code> '+g.side+' '+g.fill+' same-day '+pct(g.same_day_pct)+' — '+(g.title||'')+'</li>').join('')+'</ul>';
  document.getElementById('ex').innerHTML=li(ex.hits,'Hits')+li(ex.misses,'Misses');
});
</script></body></html>
"""
    (DASH_DIR / "index.html").write_text(html, encoding="utf-8")


def _window_overlay(stats: dict, books: dict, start: str, end: str,
                    full_start: str, full_end: str) -> dict:
    out: dict[str, dict] = {}
    for base in BASE_NAMES:
        block = {}
        for mode in OVERLAY_MODES:
            name = base if mode == "base" else f"{base}+{mode}"
            st = dict(stats.get(name) or {})
            if start != full_start or end != full_end:
                st["book_pct"] = running_book_pct(books.get(name) or {}, start, end)
                st["window_from_running_equity"] = True
            block[mode] = st
        out[base] = block
    return out


def run(write: bool = True) -> dict:
    cal = session_calendar(WINDOW_START)
    if not cal:
        raise SystemExit("no session calendar")
    last = cal[-1]
    cov_src = load_coverage()
    harvest_report = cov_src
    if not (AUTO_DIR / "_coverage.json").is_file():
        harvest_report = harvest.run(since=WINDOW_START, dest=AUTO_DIR)

    print("[grok-news-bt] loading Grok-pipeline articles (no Finviz dump, no close)",
          flush=True)
    frozen = load_frozen_articles(cal)
    parsed = load_parsed_articles(cal)
    actions = load_actions_articles(cal)
    events = load_event_articles(cal)
    articles = dedupe_articles(frozen + parsed + actions + events)
    articles = [a for a in articles if a.get("fill") and a["fill"] <= last]
    print(f"[grok-news-bt] kept {len(articles)} "
          f"(frozen={len(frozen)} actions={len(actions)} "
          f"parsed={len(parsed)} events={len(events)})", flush=True)

    maps: dict[str, list[dict]] = {}
    for i, art in enumerate(articles):
        fill = art["fill"]
        profiles = load_prior_profiles(fill, cal)
        maps[_norm(art["title"])] = map_tickers(art, profiles)
        if i and i % 50 == 0:
            print(f"[grok-news-bt] mapped {i}/{len(articles)}", flush=True)

    news = daily_news_scores(articles, maps, cal)
    apply_judge_sides(news, articles)
    cat_scores = catalyst_daily_scores(articles, maps, cal)
    cat_arts = [a for a in articles if is_catalyst(a.get("title") or "", source=a.get("source") or "")]
    n_cat_td = sum(len(v) for v in cat_scores.values())

    auto_days = {a.get("file_date") for a in frozen if a.get("file_date")}
    pipeline_days = sorted({
        a.get("file_date") for a in (parsed + actions)
        if a.get("file_date") and a["file_date"] in set(cal)
    })
    repo_only = [d for d in cal if d not in auto_days]

    graded = name_grades(articles, maps, cal)
    cat_keys = {_norm(a["title"]) for a in cat_arts}
    cat_graded = [
        g for g in graded
        if _norm(g.get("title") or "") in cat_keys and g.get("side") == "bullish"
    ]
    hits, misses = pick_examples(cat_graded or graded)

    print("[grok-news-bt] loading research panel (read-only)", flush=True)
    panel = load_baseline_panel()
    if panel is None:
        raise SystemExit("factor-mine panel missing — cannot overlay existing lists")
    panel_cal = [d for d in (panel.get("session_dates") or []) if d in set(cal)]
    if not panel_cal:
        raise SystemExit("panel has no sessions in the overlay window")

    solo_books, solo_stats = run_standalone_books(
        cat_scores, panel_cal, WINDOW_START)
    books, stats, meta = run_overlay_books(panel, news, panel_cal, WINDOW_START)
    books.update(solo_books)
    stats.update(solo_stats)

    n_named = sum(1 for a in articles if maps.get(_norm(a["title"])))
    n_td = sum(len(maps.get(_norm(a["title"])) or []) for a in articles)
    n_signed = sum(
        1 for a in articles for h in (maps.get(_norm(a["title"])) or [])
        if h.get("signed")
    )

    windows = {}
    for key, start, end in (
        ("full", WINDOW_START, last),
        ("is", WINDOW_START, IS_END),
        ("oos", OOS_START, last),
    ):
        arts = [a for a in articles if start <= (a.get("fill") or "") <= end]
        g_graded = [g for g in graded if start <= (g.get("fill") or "") <= end]
        windows[key] = {
            "start": start,
            "end": end,
            "n_articles": len(arts),
            "n_ticker_days": sum(
                len(maps.get(_norm(a["title"])) or []) for a in arts),
            "same_day_hit": _hit_rate(g_graded, "same_day_hit"),
            "next_close_hit": _hit_rate(g_graded, "next_close_hit"),
            "overlay": _window_overlay(
                stats, books, start, end, WINDOW_START, last),
            "standalone": {
                n: (
                    dict(solo_stats[n], book_pct=running_book_pct(
                        solo_books[n], start, end),
                        window_from_running_equity=True)
                    if start != WINDOW_START or end != last
                    else solo_stats[n]
                )
                for n in ("grok_n4_h1", "grok_n4_h2", "grok_n8_h1", "grok_n8_h2")
                if n in solo_stats
            },
            "catalyst_same_day_hit": _hit_rate(
                [g for g in cat_graded if start <= (g.get("fill") or "") <= end],
                "same_day_hit"),
            "catalyst_next_close_hit": _hit_rate(
                [g for g in cat_graded if start <= (g.get("fill") or "") <= end],
                "next_close_hit"),
            "published": published_baselines(start, end),
        }

    touch: dict[str, dict] = {}
    for base in BASE_NAMES:
        touch[base] = {}
        for mode in ("veto_bear", "require_bull", "add_named", "full"):
            touch[base][mode] = (meta.get(f"{base}+{mode}") or {}).get("touch") or {}

    coverage = {
        "automation_get_results": bool(harvest_report.get("automation_get_results")),
        "reason": harvest_report.get("reason") or "",
        "automation_days": int(harvest_report.get("n_days") or 0),
        "n_results": int(harvest_report.get("n_results") or 0),
        "n_sessions": len(cal),
        "pipeline_sessions": len(pipeline_days),
        "repo_only_sessions": len(repo_only),
        "frozen_articles": len(frozen),
        "parsed_articles": len(parsed),
        "actions_articles": len(actions),
        "event_articles": len(events),
        "articles_kept": len(articles),
        "articles_with_name": n_named,
        "n_ticker_days": n_td,
        "n_signed_ticker_days": n_signed,
        "catalyst_articles": len(cat_arts),
        "catalyst_ticker_days": n_cat_td,
        "note": (
            "0 automation days dumped in this environment. Stage 1 must be "
            "re-run by Grok Bot / Cursor with Automations connector. Stage 2 "
            "filled from dated Grok pipeline (parsed/actions/events) only. "
            "Raw Finviz is not the article list."
            if not harvest_report.get("n_days") else
            "Frozen automation dumps present; Grok pipeline used for gaps."
        ),
    }

    slim_articles = []
    for a in articles:
        slim_articles.append({
            "title": a["title"][:220],
            "source": a.get("source"),
            "task": a.get("task") or "",
            "event": a.get("event") or "",
            "file_date": a.get("file_date"),
            "known_at": a.get("known_at"),
            "fill": a.get("fill"),
            "tickers": [
                {"ticker": h["ticker"], "side": h["side"], "theme": h.get("theme"),
                 "why": h.get("why")}
                for h in (maps.get(_norm(a["title"])) or [])
            ],
        })

    payload = {
        "generated_at": datetime.now(ET).isoformat(),
        "stage": 2,
        "research_only": True,
        "product": "standalone+overlay",
        "live_untouched": [
            "dashboard/factor-mine/", "03_scoreboard/factor_mine.json",
            "flatten", "webull", "paper_open",
        ],
        "from_date": WINDOW_START,
        "to_date": last,
        "coverage": coverage,
        "windows": windows,
        "books_full": stats,
        "daily_books": books,
        "touch": touch,
        "articles": slim_articles,
        "examples": {"hits": hits, "misses": misses},
        "leak": {
            "fill": "next official 09:30 strictly after known_at; 09:30:00 too late",
            "mapper": "exact named ticker or unique D-1 company only",
            "forbidden": [
                "same-day Change%/Gap/RelVol",
                "Finviz row ticker as proof",
                "sector-basket expansion",
                "raw Finviz as the article list",
                "close digest",
                "post-close research_baseline",
                "OOS files while mapping IS",
                "live automation API",
                "live web",
            ],
            "short_locate": False,
            "long_only": True,
            "hard_red": -3,
        },
    }
    if write:
        OUT_MD.write_text(write_md(payload), encoding="utf-8")
        OUT_JSON.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        write_dash(payload)
    return payload


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--write", action="store_true", default=True)
    ap.add_argument("--no-write", action="store_true")
    args = ap.parse_args(argv)
    payload = run(write=not args.no_write)
    hot = ((payload["windows"]["full"]["overlay"].get("union_hot_n4_h1") or {}))
    print(
        f"[grok-news-bt] articles={payload['coverage']['articles_kept']} "
        f"named={payload['coverage']['articles_with_name']} "
        f"auto_days={payload['coverage']['automation_days']} "
        f"hot4={hot.get('base', {}).get('book_pct')} "
        f"hot4+full={hot.get('full', {}).get('book_pct')} "
        f"solo_n4h2={((payload['windows']['full'].get('standalone') or {}).get('grok_n4_h2') or {}).get('book_pct')}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
