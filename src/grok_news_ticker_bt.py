"""Stage 2 — leak-safe Grok-news → ticker cash-book replay.

Reads ONLY:
  * frozen ``data/grok_automations/*.json``
  * dated repo news: ``data/exports/finviz_{D}.csv``,
    ``01_daily/news/{D}_*.json`` (never ``*_close*``),
    ``01_daily/events/{D}_events.json``
  * PRIOR-session Company / Sector / Industry (D-1 Finviz)

No live web, no live automation API, no same-day close digest, no
post-close research_baseline, no future files, no same-day Change%/Gap
as mapping proof, no “Finviz attached this headline to ticker X”.

Fill = next official 09:30 STRICTLY AFTER known_at. A 09:30:00 ET print
is too late for that open.
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
    r"federal reserve|fed chair|fomc|fed funds|dot plot|powell|warsh|"
    r"white house|federal register|executive order|"
    r"\btariff|\bban(?:s|ned|ning)?\b|\bexemption|"
    r"tokeniz|regulation crypto|safe harbor|atkins|"
    r"antitrust|chips act|\bghg\b|greenhouse gas|"
    r"export control|section 301|de minimis|"
    r"supreme court|court of (appeals|international trade)|"
    r"\bbill\b.{0,30}(congress|senate|house)|"
    r"(congress|senate|house).{0,30}\bbill\b|"
    r"rate (cut|hike|hold)|interest[- ]rate"
    r")"
)
SINGLE_FDA = re.compile(
    r"(?i)\b(fda (approval|clearance|nod|crl)|receives? fda|"
    r"fda (approves?|clears?|grants?))\b"
)
YAHOO_TABLOID = re.compile(r"(?i)(yahoo|seeking alpha|motley fool|benzinga)")

# Theme packs: sector-wide only when the article is actually sector-wide.
THEME_PACKS: list[dict] = [
    {
        "id": "crypto_sec",
        "rx": re.compile(
            r"(?i)(atkins|tokeniz|regulation crypto|tokenized|"
            r"sec.{0,48}(exemption|safe harbor|greenlight)|"
            r"clarity act|crypto (exemption|framework|rule))"
        ),
        "sector_wide": False,
        "need": re.compile(
            r"(?i)(crypto|bitcoin|ether|blockchain|token|digital asset|"
            r"coinbase|robinhood|securitize|broker|exchange|capital markets|"
            r"fintech)"
        ),
        "polarity": "bullish",
    },
    {
        "id": "epa_ghg",
        "rx": re.compile(
            r"(?i)(epa.{0,40}(ghg|greenhouse|carbon|repeal)|"
            r"greenhouse gas.{0,24}(repeal|rule|standard))"
        ),
        "sector_wide": True,
        "need": re.compile(
            r"(?i)(coal|natural gas|gas-fired|thermal|independent power|"
            r"regulated electric|lignite|generation|power producer|"
            r"oil.?gas|utility)"
        ),
        "polarity": "bullish",
    },
    {
        "id": "fed_path",
        "rx": re.compile(
            r"(?i)(federal reserve|fed chair|fomc|fed (rate|hike|cut)|"
            r"warsh|powell|rate hike|rate cut|fed funds)"
        ),
        "sector_wide": True,
        "need": re.compile(
            r"(?i)(gold|silver|reit|real estate|treasury|regional bank|"
            r"mortgage|homebuilder|precious)"
        ),
        "polarity": None,  # from text
    },
    {
        "id": "tariff",
        "rx": re.compile(
            r"(?i)(tariff|section 301|section 232|de minimis|"
            r"import ban|export control|chips act)"
        ),
        "sector_wide": True,
        "need": re.compile(
            r"(?i)(copper|aluminum|steel|solar|semiconductor|chip|foundry|"
            r"auto|dairy|e-?commerce|china|drone|rare earth|mining|"
            r"refined copper)"
        ),
        "polarity": "bearish",
    },
    {
        "id": "hormuz_new",
        "rx": HORMUZ,
        "sector_wide": True,
        "need": re.compile(
            r"(?i)(oil|gas|e&p|exploration|petroleum|airline|air freight|"
            r"crude|refiner|tanker)"
        ),
        "polarity": None,
    },
    {
        "id": "oil_inventory",
        "rx": re.compile(
            r"(?i)(oil inventor|eia crude|crude inventor|opec|"
            r"wti|brent.{0,20}inventor)"
        ),
        "sector_wide": True,
        "need": re.compile(
            r"(?i)(e&p|exploration|petroleum|oil.?gas|crude|refiner|"
            r"integrated oil)"
        ),
        "polarity": None,
    },
    {
        "id": "antitrust",
        "rx": re.compile(r"(?i)(antitrust|doj.{0,30}(suit|probe|case)|no breakup)"),
        "sector_wide": False,
        "need": re.compile(
            r"(?i)(advertis|search|ad.tech|asset management|index fund|"
            r"blackrock|state street|vanguard|google|alphabet|meta)"
        ),
        "polarity": None,
    },
    {
        "id": "fda_policy",
        "rx": re.compile(
            r"(?i)(fda (guidance|ban|policy|class|labeling rule)|"
            r"cms (rate|rule)|ira drug|drug pricing)"
        ),
        "sector_wide": True,
        "need": re.compile(
            r"(?i)(drug|pharma|biotech|medicare|managed care|pharmacy)"
        ),
        "polarity": None,
    },
    {
        "id": "chips_export",
        "rx": re.compile(
            r"(?i)(chips act|export control|bis\b|huawei ban|"
            r"advanced (node|chip) restrict)"
        ),
        "sector_wide": True,
        "need": re.compile(
            r"(?i)(semiconductor|chip|foundry|gpu|asic|fab|wafer|eda)"
        ),
        "polarity": None,
    },
    {
        "id": "sre_rin",
        "rx": re.compile(
            r"(?i)(small[- ]refinery exemption|\bsre\b|rin (market|waiver))"
        ),
        "sector_wide": True,
        "need": re.compile(r"(?i)(refiner|refining|rin|biofuel|ethanol)"),
        "polarity": "bullish",
    },
]

BULL = re.compile(
    r"(?i)\b(exemption|greenlight|approves?|clarity|repeal|"
    r"rate cut|easing|dovish|safe harbor|wins?|clears?)\b"
)
BEAR = re.compile(
    r"(?i)\b(ban|hike|hawkish|tariff|probe|suit|block|"
    r"inventory build|tightening|enforcement|fine)\b"
)


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
    pol, _ = np._polarity(blob), None
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


# ── loaders (dated files only) ───────────────────────────────────────
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
                if created is None:
                    continue
                known, fill = usable_known_at(created, file_date, cal)
                if not known or not fill:
                    continue
                rows.append({
                    "title": title,
                    "source": "automation",
                    "task": res.get("task") or payload.get("task"),
                    "conversationId": res.get("conversationId") or "",
                    "file_date": file_date,
                    "known_at": known.isoformat(),
                    "fill": fill,
                    "url": "",
                    "digest": "",
                })
    return rows


def load_parsed_articles(cal: list[str]) -> list[dict]:
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
        items = list(data.get("all_items") or []) + list(data.get("usable_top") or [])
        for it in items:
            title = (it.get("title") or "").strip()
            if not keep_article(title, source=it.get("source") or "parsed"):
                continue
            stamp = _parse_et(it.get("published_at")) or gen
            if stamp is None:
                stamp = datetime.strptime(file_date, "%Y-%m-%d").replace(
                    hour=0, minute=0, second=1, tzinfo=ET)
            known, fill = usable_known_at(stamp, file_date, cal)
            if not known or not fill:
                continue
            rows.append({
                "title": title,
                "source": "parsed",
                "task": "",
                "conversationId": "",
                "file_date": file_date,
                "known_at": known.isoformat(),
                "fill": fill,
                "url": it.get("url") or "",
                "digest": "",
                "class": it.get("class"),
            })
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
            if stamp is None:
                stamp = datetime.strptime(file_date, "%Y-%m-%d").replace(
                    hour=0, minute=0, second=1, tzinfo=ET)
            known, fill = usable_known_at(stamp, file_date, cal)
            if not known or not fill:
                continue
            rows.append({
                "title": title,
                "source": "events",
                "task": "",
                "conversationId": "",
                "file_date": file_date,
                "known_at": known.isoformat(),
                "fill": fill,
                "url": "",
                "digest": (ev.get("why_it_matters") or "")[:400],
                "category": ev.get("category"),
                "status": ev.get("status"),
                "event_sectors": ev.get("sectors") or [],
            })
    return rows


def _finviz_news_time_ok(news_dt: datetime, file_date: str) -> bool:
    nd = news_dt.strftime("%Y-%m-%d")
    if nd > file_date:
        return False
    file_d = datetime.strptime(file_date, "%Y-%m-%d")
    if news_dt.replace(tzinfo=None) < file_d - timedelta(days=STALE_MAX_DAYS):
        return False
    return True


def load_finviz_articles(cal: list[str]) -> list[dict]:
    rows: list[dict] = []
    for path in sorted(EXPORT_DIR.glob("finviz_20*.csv")):
        file_date = _file_date(path)
        if not file_date or file_date < WINDOW_START:
            continue
        try:
            with path.open(newline="", encoding="utf-8", errors="replace") as fh:
                reader = csv.DictReader(fh)
                for rec in reader:
                    title = (rec.get("News Title") or "").strip()
                    if not title:
                        continue
                    if not keep_article(title, source=rec.get("News URL") or ""):
                        continue
                    news_dt = _parse_et(rec.get("News Time"))
                    if news_dt is None or not _finviz_news_time_ok(news_dt, file_date):
                        continue
                    known, fill = usable_known_at(news_dt, file_date, cal)
                    if not known or not fill:
                        continue
                    rows.append({
                        "title": title,
                        "source": "finviz",
                        "task": "",
                        "conversationId": "",
                        "file_date": file_date,
                        "known_at": known.isoformat(),
                        "fill": fill,
                        "url": rec.get("News URL") or "",
                        "digest": (rec.get("Daily Digest") or "")[:400],
                    })
        except OSError:
            continue
    return rows


def dedupe_articles(rows: list[dict]) -> list[dict]:
    best: dict[str, dict] = {}
    rank = {"automation": 0, "events": 1, "parsed": 2, "finviz": 3}
    for r in rows:
        key = _norm(r.get("title") or "")
        if not key:
            continue
        prev = best.get(key)
        if prev is None:
            best[key] = r
            continue
        if rank.get(r["source"], 9) < rank.get(prev["source"], 9):
            r = dict(r)
            if prev.get("known_at") and (
                not r.get("known_at") or r["known_at"] > prev["known_at"]
            ):
                # keep earlier usable stamp from the worse source if it
                # is still on a file dated ≤ fill — already enforced.
                pass
            best[key] = r
            continue
        if r.get("known_at") and (
            not prev.get("known_at") or r["known_at"] < prev["known_at"]
        ):
            best[key] = r
    out = list(best.values())
    out.sort(key=lambda r: (r.get("known_at") or "", r.get("title") or ""))
    return out


# ── D-1 company text (never same-day tape) ───────────────────────────
_PROFILE_CACHE: dict[str, dict[str, dict]] = {}
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
    return out


def _article_names(title: str, digest: str) -> str:
    return f"{title or ''} {digest or ''}"


def map_tickers(article: dict, profiles: dict[str, dict]) -> list[dict]:
    """Overlap of article themes vs D-1 company text. No row-ticker proof."""
    title = article.get("title") or ""
    digest = article.get("digest") or ""
    blob = _article_names(title, digest)
    blob_l = blob.lower()
    themes = match_themes(title) or match_themes(digest)
    if not themes:
        # Policy keep without a pack: require explicit company-name hit.
        themes = [{
            "id": "named",
            "sector_wide": False,
            "need": None,
            "polarity": None,
        }]
    hits: dict[str, dict] = {}
    for theme in themes:
        pol = article_polarity(blob, theme)
        if pol not in ("bullish", "bearish"):
            if theme.get("id") == "hormuz_new":
                # upstream + / airlines − handled per industry below
                pass
            else:
                continue
        need = theme.get("need")
        sector_wide = bool(theme.get("sector_wide"))
        for t, prof in profiles.items():
            text = prof["text"]
            company = (prof["company"] or "").strip()
            named = False
            if len(company) >= 5 and company.lower() in blob_l:
                named = True
            if re.search(rf"\b{re.escape(t)}\b", blob):
                named = True
            both = bool(need and need.search(blob_l) and need.search(text))
            if theme.get("id") == "named":
                overlap = named
            elif not sector_wide:
                overlap = named or both
            else:
                overlap = bool(need and need.search(text) and (
                    need.search(blob_l) or theme["rx"].search(blob)
                ))
            if not overlap:
                continue
            side = pol
            if theme.get("id") == "hormuz_new":
                if re.search(r"(?i)(airline|air freight)", text):
                    side = "bearish"
                else:
                    side = "bullish"
            if theme.get("id") == "fed_path" and side == "bearish":
                if re.search(r"(?i)(gold|silver|precious)", text):
                    side = "bearish"
                elif re.search(r"(?i)(reit|real estate|homebuilder|mortgage)", text):
                    side = "bearish"
                elif re.search(r"(?i)regional bank", text):
                    side = "bullish"
            score = 2
            if named:
                score += 2
            prev = hits.get(t)
            signed = score if side == "bullish" else -score
            if prev:
                signed = prev["signed"] + signed
            hits[t] = {
                "ticker": t,
                "side": "bullish" if signed > 0 else "bearish",
                "signed": signed,
                "theme": theme["id"],
                "why": f"{theme['id']} ∩ D-1 {prof['industry'] or prof['sector']}",
                "profile_export": prof["export"],
            }
    out = [h for h in hits.values() if h["signed"] != 0]
    out.sort(key=lambda h: (-abs(h["signed"]), h["ticker"]))
    return out[:40]


def official_bar(ticker: str, date: str) -> dict:
    return tl.session_bar(ticker, date) or {}


# ── books ────────────────────────────────────────────────────────────
def daily_scores(articles: list[dict], maps: dict[str, list[dict]],
                 cal: list[str], start: str, end: str) -> dict[str, dict[str, int]]:
    scores: dict[str, dict[str, int]] = {d: {} for d in cal if start <= d <= end}
    for art in articles:
        fill = art.get("fill")
        if not fill or fill not in scores:
            continue
        for h in maps.get(_norm(art["title"])) or []:
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
                "date": date,
                "ticker": t,
                "sources": ["union"],
                "src_rank": src,
                "news_score": sc,
                "boxes": {},
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


def run_news_books(panel: dict, *, top_ns=(4, 8), holds=(1, 2),
                   starts_for: set[str] | None = None) -> tuple[dict, dict]:
    regime = fmb.load_regime()
    fees = fm.pt_fees()
    books, stats = {}, {}
    starts_for = starts_for or {"grok_n4_h1"}
    for top_n in top_ns:
        for hold in holds:
            name = f"grok_n{top_n}_h{hold}"
            rec = fm.make_recipe(
                name=name, universe="union", hold=hold, top_n=top_n,
                rank="list", side="long",
                note="Grok news ticker research; long-only (no short locate)",
            )
            book = fmb.simulate_book(panel, rec, fees=fees, regime=regime)
            starts = []
            if name in starts_for:
                starts = fmb.replay_starts(
                    panel, rec, fees=fees, regime=regime)
            books[name] = slim_book(book)
            stats[name] = book_stats(book, starts)
    return books, stats


def load_baseline_panel() -> dict | None:
    if not fm.PANEL_PATH.is_file():
        return None
    try:
        raw = json.loads(fm.PANEL_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return fm.rehydrate_panel(raw)


def run_baselines(panel: dict | None, start: str, end: str) -> dict:
    out = {}
    published = {}
    try:
        raw = json.loads(
            (ROOT / "03_scoreboard" / "factor_mine.json").read_text(encoding="utf-8"))
        published = {s["name"]: s for s in (raw.get("stats") or [])}
        dates = list(raw.get("dates") or [])
        series = raw.get("series") or {}
    except (OSError, json.JSONDecodeError):
        dates, series = [], {}

    if panel is not None:
        sl = fm.slice_panel(panel, start, end)
        regime = fmb.load_regime()
        fees = fm.pt_fees()
        for name, kwargs in (
            ("union_hot_n4_h1", dict(universe="union", hold=1, top_n=4,
                                     rank="hot_score",
                                     forbid={"alarm": True})),
            ("flatten_h5", dict(universe="flatten", hold=5)),
        ):
            rec = fm.make_recipe(name=name, **kwargs)
            book = fmb.simulate_book(sl, rec, fees=fees, regime=regime)
            starts = fmb.replay_starts(sl, rec, fees=fees, regime=regime)
            out[name] = book_stats(book, starts)
        return out

    for name in ("union_hot_n4_h1", "flatten_h5"):
        st = published.get(name) or {}
        eq = list(series.get(name) or [])
        book_pct = st.get("total_ret_pct")
        if dates and eq and start in dates and end in dates:
            i0, i1 = dates.index(start), dates.index(end)
            if 0 <= i0 <= i1 < len(eq):
                # Running-book window (not a fresh $10k) — labeled as such.
                prev = 10000.0 if i0 == 0 else eq[i0 - 1]
                if prev:
                    book_pct = round(100.0 * (eq[i1] / prev - 1.0), 3)
        out[name] = {
            "name": name,
            "book_pct": book_pct,
            "final_equity": st.get("final_equity"),
            "dollar_days": st.get("profitable_day_rate"),
            "starts_yes": (
                f"{st.get('start_green')}/{st.get('start_n')}"
                if st.get("start_n") else None
            ),
            "from_published": True,
            "window_from_running_equity": True,
        }
    return out


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
    prefer = []
    for g in hits:
        if g["ticker"] in ("SECZ", "HOOD", "COIN") or "token" in (g.get("title") or "").lower():
            prefer.append(g)
    seen = set()
    chosen_hits = []
    for g in prefer + hits:
        k = (g["ticker"], g.get("title"))
        if k in seen:
            continue
        seen.add(k)
        chosen_hits.append(g)
        if len(chosen_hits) == 3:
            break
    chosen_miss = []
    seen = set()
    for g in misses:
        k = (g["ticker"], g.get("title"))
        if k in seen:
            continue
        seen.add(k)
        chosen_miss.append(g)
        if len(chosen_miss) == 3:
            break
    return chosen_hits, chosen_miss


def window_slice(articles, maps, cal, start, end):
    fills = [d for d in cal if start <= d <= end]
    arts = [a for a in articles if a.get("fill") in set(fills)]
    n_td = 0
    for a in arts:
        n_td += len(maps.get(_norm(a["title"])) or [])
    return arts, n_td


def _pct(v) -> str:
    if v is None:
        return "—"
    return f"{float(v):+.2f}"


def write_md(payload: dict) -> str:
    cov = payload["coverage"]
    w = payload["windows"]
    lines = [
        "# Grok news → ticker cash-book (research)",
        "",
        f"Window **{payload['from_date']} → {payload['to_date']}** · "
        f"last closed session **{payload['to_date']}**. "
        "Two-stage: harvest automations to frozen files, then replay. "
        "Not live. Does not touch factor-mine / flatten / Webull.",
        "",
        "## Coverage",
        "",
        f"- Stage 1 automation days dumped: **{cov.get('automation_days', 0)}** "
        f"({cov.get('n_results', 0)} results).",
        f"- Sessions filled from repo files only: **{cov.get('repo_only_sessions', 0)}** "
        f"of {cov.get('n_sessions', 0)}.",
        f"- `automation_get_results`: **{cov.get('automation_get_results')}**. "
        f"{cov.get('reason') or ''}",
        "- GH Actions must not harvest. Replay reads frozen dumps + dated repo news.",
        "",
        "## Leak rules",
        "",
        "- `known_at` = automation createTime, or Finviz News Time if the title "
        "is on that day’s export. Earlier stamp only if that stamp sits on a "
        "file dated ≤ the fill session.",
        "- Fill = next official 09:30 **strictly after** known_at. "
        "09:30:00 ET is too late for that open. RTH → next open. "
        "Missing official open → no fill.",
        "- Mapper uses D-1 Company / Sector / Industry only. "
        "Never same-day Change%/Gap/RelVol. Never “Finviz attached this "
        "headline to ticker X”.",
        "- No close digest, no post-close research_baseline, no OOS files "
        "while mapping an IS fill.",
        "- Hormuz carry with no new shock is not an article.",
        "- Long-only (no short locate) + hard-red S≤−3 sit. "
        "$10k leftover split, Futubull fees, whole shares.",
        "",
        "## Headline books vs controls",
        "",
        "| Window | n articles | n ticker-days | same-day name hit | "
        "`grok_n4_h1` book% | starts YES | $ days | "
        "`union_hot_n4_h1` | `flatten_h5` |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for key, label in (
        ("full", f"{payload['from_date']}→{payload['to_date']}"),
        ("is", f"{WINDOW_START}→{IS_END}"),
        ("oos", f"{OOS_START}→{payload['to_date']}"),
    ):
        row = w[key]
        g = row["grok_n4_h1"]
        lines.append(
            f"| {label} | {row['n_articles']} | {row['n_ticker_days']} | "
            f"{row['same_day_hit']} | {_pct(g.get('book_pct'))} | "
            f"{g.get('starts_yes') or '—'} | {_pct((g.get('dollar_days') or 0)*100)}% | "
            f"{_pct(row['baselines']['union_hot_n4_h1'].get('book_pct'))} | "
            f"{_pct(row['baselines']['flatten_h5'].get('book_pct'))} |"
        )
    lines += [
        "",
        "### Variants (full window)",
        "",
        "| Recipe | book% | starts YES | $ days | trades |",
        "|---|---:|---:|---:|---:|",
    ]
    for name, st in payload["books_full"].items():
        lines.append(
            f"| `{name}` | {_pct(st.get('book_pct'))} | "
            f"{st.get('starts_yes') or '—'} | "
            f"{_pct((st.get('dollar_days') or 0)*100)}% | {st.get('n_trades')} |"
        )
    lines += ["", "## 3 hits", ""]
    for g in payload["examples"]["hits"]:
        lines.append(
            f"- **{g['ticker']}** `{g.get('side')}` fill {g.get('fill')} "
            f"same-day { _pct(g.get('same_day_pct')) } / next-close "
            f"{_pct(g.get('next_close_pct'))} — {g.get('title')}"
        )
    if not payload["examples"]["hits"]:
        lines.append("- *(none graded)*")
    lines += ["", "## 3 misses", ""]
    for g in payload["examples"]["misses"]:
        lines.append(
            f"- **{g['ticker']}** `{g.get('side')}` fill {g.get('fill')} "
            f"same-day { _pct(g.get('same_day_pct')) } / next-close "
            f"{_pct(g.get('next_close_pct'))} — {g.get('title')}"
        )
    if not payload["examples"]["misses"]:
        lines.append("- *(none graded)*")
    lines += [
        "",
        "## Method",
        "",
        "1. Stage 1 harvest (Cursor / Grok Bot, not GH Action) dumps each "
        "automation result since 2026-08-13 into "
        "`data/grok_automations/{date}_{task}.json`.",
        "2. Stage 2 keeps policy / regulator / exemption / ban / tariff / "
        "Fed / court / bill rows. Drops earnings PR, SEC filings, Yahoo/"
        "Cramer tabloid, single-name FDA approvals, carried Hormuz.",
        "3. Tickers must overlap D-1 company text. Sector-wide only when "
        "the article is sector-wide (EPA GHG → coal/gas generators; random "
        "software no).",
        "4. Cash book is the factor-mine family: $10k, leftover split, "
        "Futubull fees, whole shares, sell first, hard-red sit.",
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
        "books_full": payload["books_full"],
        "examples": payload["examples"],
    }
    (DASH_DIR / "payload.json").write_text(
        json.dumps(slim, indent=2), encoding="utf-8")
    html = """<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Grok news ticker book — research</title>
<style>
:root{--bg:#0f1420;--card:#171e2e;--line:#262f45;--fg:#dfe6f2;--mut:#8b96ab;--pos:#4ade80;--neg:#f87171;--gold:#fbbf24}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--fg);font:13px/1.45 -apple-system,Segoe UI,sans-serif}
.wrap{max-width:960px;margin:0 auto;padding:16px}h1{font-size:18px;margin:0 0 6px}
.sub{color:var(--mut);margin:0 0 12px}table{width:100%;border-collapse:collapse;margin:8px 0 16px}
th,td{padding:5px 6px;border-bottom:1px solid var(--line);text-align:right}th:first-child,td:first-child{text-align:left}
.pos{color:var(--pos)}.neg{color:var(--neg)}.note{color:var(--mut);font-size:12px}
.card{background:var(--card);border:1px solid var(--line);border-radius:10px;padding:10px 12px;margin:0 0 12px}
</style></head><body><div class="wrap">
<h1>Grok news → ticker cash-book</h1>
<p class="sub">Research only. Frozen automations + dated repo news. Live factor-mine / flatten / Webull untouched.</p>
<div class="card" id="cov"></div>
<div class="card"><h2>Windows</h2><table id="win"></table></div>
<div class="card"><h2>Hits / misses</h2><div id="ex"></div></div>
<p class="note">Fill = next 09:30 strictly after known_at. Mapper is D-1 company text. Long-only + hard-red sit.</p>
</div>
<script>
fetch('payload.json').then(r=>r.json()).then(d=>{
  const cov=d.coverage||{};
  document.getElementById('cov').innerHTML =
    '<b>Coverage</b><div>automation days dumped: '+(cov.automation_days||0)+
    ' · repo-only sessions: '+(cov.repo_only_sessions||0)+'/'+(cov.n_sessions||0)+
    '</div><div class="note">'+(cov.reason||'')+'</div>';
  const pct=v=>v==null?'—':((v>=0?'+':'')+Number(v).toFixed(2));
  let h='<tr><th>Window</th><th>n art</th><th>ticker-days</th><th>same-day hit</th><th>n4 h1</th><th>hot4</th><th>flatten_h5</th></tr>';
  for (const [k,label] of [['full','full'],['is','08-13→09-09'],['oos','09-10→end']]){
    const w=d.windows[k]; if(!w) continue;
    h+='<tr><td>'+label+'</td><td>'+w.n_articles+'</td><td>'+w.n_ticker_days+
      '</td><td>'+w.same_day_hit+'</td><td>'+pct(w.grok_n4_h1.book_pct)+
      '</td><td>'+pct(w.baselines.union_hot_n4_h1.book_pct)+
      '</td><td>'+pct(w.baselines.flatten_h5.book_pct)+'</td></tr>';
  }
  document.getElementById('win').innerHTML=h;
  const ex=d.examples||{};
  const li=(arr,tag)=>'<p><b>'+tag+'</b></p><ul>'+(arr||[]).map(g=>
    '<li><code>'+g.ticker+'</code> '+g.side+' '+g.fill+' same-day '+pct(g.same_day_pct)+' — '+(g.title||'')+'</li>').join('')+'</ul>';
  document.getElementById('ex').innerHTML=li(ex.hits,'Hits')+li(ex.misses,'Misses');
});
</script></body></html>
"""
    (DASH_DIR / "index.html").write_text(html, encoding="utf-8")


def _hit_rate(graded: list[dict], field: str) -> str:
    xs = [g for g in graded if g.get(field) is not None]
    if not xs:
        return "—"
    n = sum(1 for g in xs if g.get(field))
    return f"{n}/{len(xs)} ({n/len(xs):.0%})"


def run(write: bool = True) -> dict:
    cal = session_calendar(WINDOW_START)
    if not cal:
        raise SystemExit("no session calendar")
    last = cal[-1]
    cov_src = load_coverage()
    harvest_report = cov_src
    if not (AUTO_DIR / "_coverage.json").is_file():
        harvest_report = harvest.run(since=WINDOW_START, dest=AUTO_DIR)

    frozen = load_frozen_articles(cal)
    parsed = load_parsed_articles(cal)
    events = load_event_articles(cal)
    finviz = load_finviz_articles(cal)
    articles = dedupe_articles(frozen + parsed + events + finviz)
    articles = [a for a in articles if a.get("fill") and a["fill"] <= last]

    maps: dict[str, list[dict]] = {}
    for art in articles:
        fill = art["fill"]
        # IS mapping must not peek OOS profiles; D-1 of fill is enough.
        profiles = load_prior_profiles(fill, cal)
        maps[_norm(art["title"])] = map_tickers(art, profiles)

    auto_days = set()
    for a in frozen:
        auto_days.add(a.get("file_date"))
    repo_only = [d for d in cal if d not in auto_days]

    graded = name_grades(articles, maps, cal)
    hits, misses = pick_examples(graded)

    windows = {}
    baseline_panel = load_baseline_panel()
    books_full_stats = {}
    books_full_slim = {}
    for key, start, end in (
        ("full", WINDOW_START, last),
        ("is", WINDOW_START, IS_END),
        ("oos", OOS_START, last),
    ):
        arts, n_td = window_slice(articles, maps, cal, start, end)
        sub_cal = [d for d in cal if start <= d <= end]
        scores = daily_scores(arts, maps, sub_cal, start, end)
        panel = panel_from_scores(scores, sub_cal)
        starts_for = {"grok_n4_h1"} if key != "full" else {
            "grok_n4_h1", "grok_n4_h2", "grok_n8_h1", "grok_n8_h2"
        }
        books, stats = run_news_books(panel, starts_for=starts_for)
        g_graded = [g for g in graded if start <= (g.get("fill") or "") <= end]
        base = run_baselines(baseline_panel, start, end)
        windows[key] = {
            "start": start,
            "end": end,
            "n_articles": len(arts),
            "n_ticker_days": n_td,
            "same_day_hit": _hit_rate(g_graded, "same_day_hit"),
            "next_close_hit": _hit_rate(g_graded, "next_close_hit"),
            "grok_n4_h1": stats["grok_n4_h1"],
            "variants": stats,
            "baselines": base,
        }
        if key == "full":
            books_full_stats = stats
            books_full_slim = books

    coverage = {
        "automation_get_results": bool(harvest_report.get("automation_get_results")),
        "reason": harvest_report.get("reason") or "",
        "automation_days": int(harvest_report.get("n_days") or 0),
        "n_results": int(harvest_report.get("n_results") or 0),
        "n_sessions": len(cal),
        "repo_only_sessions": len(repo_only),
        "frozen_articles": len(frozen),
        "parsed_articles": len(parsed),
        "event_articles": len(events),
        "finviz_articles": len(finviz),
        "articles_kept": len(articles),
        "note": (
            "0 automation days dumped in this environment. Stage 1 must be "
            "re-run by Grok Bot / Cursor with Automations connector. Stage 2 "
            "filled from dated repo news only."
            if not harvest_report.get("n_days") else
            "Frozen automation dumps present; repo news used for gaps."
        ),
    }

    slim_articles = []
    for a in articles:
        slim_articles.append({
            "title": a["title"][:220],
            "source": a.get("source"),
            "task": a.get("task") or "",
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
        "live_untouched": ["dashboard/factor-mine/", "03_scoreboard/factor_mine.json",
                           "flatten", "webull", "paper_open"],
        "from_date": WINDOW_START,
        "to_date": last,
        "coverage": coverage,
        "windows": windows,
        "books_full": books_full_stats,
        "daily_books": books_full_slim,
        "articles": slim_articles,
        "examples": {"hits": hits, "misses": misses},
        "leak": {
            "fill": "next official 09:30 strictly after known_at; 09:30:00 too late",
            "mapper": "D-1 Company/Sector/Industry only",
            "forbidden": [
                "same-day Change%/Gap/RelVol",
                "Finviz row ticker as proof",
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
    print(
        f"[grok-news-bt] articles={payload['coverage']['articles_kept']} "
        f"auto_days={payload['coverage']['automation_days']} "
        f"n4h1={payload['books_full']['grok_n4_h1'].get('book_pct')}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
