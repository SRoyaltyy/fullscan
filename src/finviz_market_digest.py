"""Finviz homepage market-day prose digest (not the quote-page ticker file).

The live Finviz homepage embeds a `$MARKET` / `market_summary` widget
(`script#why-stock-moving-init-data`) — the narrative like "The S&P 500
rose 0.86%… Brent retreated… hotter CPI… DELL/HPE/HPQ… Monday calendar
light…". That is a different artifact from
`01_daily/news/YYYY-MM-DD_finviz_digest.md` (quote-page headlines + ~400
ticker blurbs).

Outputs:
  01_daily/news/<date>_finviz_market_digest.md
  01_daily/news/<date>_finviz_market_digest.json
  01_daily/news/latest_finviz_market_digest.md

Clock (Theme Radar / War room):
  Generated is the scrape time in America/New_York ISO.
  Live: write only when Generated is before 09:30 ET on that date;
        otherwise leave the file missing (no afternoon live backfill).
  Wayback / archive.ph: stamp the real capture time + source=wayback.
        Capture before 09:30 ET that day → legal for that morning's
        Pre-Open (`clock_legal_for` = that date, `clock_use` =
        same_morning). Midday/afternoon capture → legal for the NEXT
        NYSE session open only, as a prior-day close recap
        (`clock_legal_for` = next session, `clock_use` = next_open) —
        NOT the same morning. Gaps stay gaps.
  Not wired into tape_anchor / predict / #210.

CLI:
  python -m src.finviz_market_digest [--date YYYY-MM-DD] [--html PATH]
  python -m src.finviz_market_digest --backfill --from 2026-08-20 --to 2026-09-12
"""
from __future__ import annotations

import argparse
import gzip
import json
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import requests

from . import config, finviz_session

ROOT = Path(__file__).resolve().parent.parent
NEWS_DIR = ROOT / "01_daily" / "news"
ET = ZoneInfo(config.TZ)
UTC = ZoneInfo("UTC")
OPEN_HM = 930

HOME_PATHS = ("/", "/index.ashx")
CDX_URL = "https://web.archive.org/cdx/search/cdx"
WAYBACK_ID = "https://web.archive.org/web/{ts}id_/{orig}"
ARCHIVE_PH_HOSTS = (
    "https://archive.ph",
    "https://archive.today",
    "https://archive.md",
)
CDX_TARGETS = (
    "https://finviz.com/",
    "https://finviz.com",
    "https://www.finviz.com/",
)

UA = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    )
}

INDEX_ALIASES = {
    "s&p 500": "SPX",
    "s&p": "SPX",
    "spx": "SPX",
    "spy": "SPY",
    "nasdaq": "COMP",
    "nasdaq-100": "NDX",
    "nasdaq composite": "COMP",
    "comp": "COMP",
    "dow": "DJI",
    "dow jones": "DJI",
    "dow jones industrial average": "DJI",
    "dji": "DJI",
    "russell 2000": "RUT",
    "rut": "RUT",
    "iwm": "IWM",
}

MD_LINK_RE = re.compile(r"\[([^\]]+)\]\((https?://[^)]+)\)")
TICKER_LINK_RE = re.compile(
    r"finviz\.com/(?:quote|quote\.ashx)\?t=([A-Za-z][A-Za-z0-9.-]{0,7})",
    re.I,
)
INDEX_LINK_RE = re.compile(
    r"finviz\.com/index/([A-Za-z0-9]+)",
    re.I,
)
PAREN_TICKER_RE = re.compile(r"\(([A-Z]{1,5})\)")
BOLD_RE = re.compile(r"\*\*([^*]+)\*\*")
INDEX_MOVE_RE = re.compile(
    r"(?i)\b(S&P\s*500|Nasdaq(?:-100| composite)?|Dow(?:\s+Jones(?:\s+Industrial\s+Average)?)?|"
    r"Russell\s*2000)\b"
    r".{0,80}?\b(rose|gained|added|climbed|advanced|jumped|rallied|"
    r"fell|dropped|slid|retreated|lost|slipped|declined)\b"
    r".{0,40}?\b([+-]?\d+(?:\.\d+)?)\s*%"
)
OIL_RE = re.compile(
    r"(?i)\b(Brent(?:\s+crude)?|WTI(?:\s+crude)?|crude(?:\s+oil)?)\b"
    r".{0,100}?\$(\d+(?:\.\d+)?)"
)
FED_ODDS_RE = re.compile(
    r"(?:roughly|about|near|around|implied)?\s*"
    r"(\d+(?:\.\d+)?)\s*%[^\n.]{0,80}?\b(hike|cut|hold|pause)\b"
    r"|"
    r"\b(hike|cut|hold|pause)\b[^\n.]{0,80}?"
    r"(\d+(?:\.\d+)?)\s*%",
    re.I,
)
CPI_RE = re.compile(
    r"(?i)\b((?:hotter|cooler|softer|hot)-than-expected\s+)?"
    r"(?:[A-Za-z]+\s+)?CPI\b[^\n.]{0,140}"
)
CAL_HINT_RE = re.compile(
    r"(?i)\b(monday|tuesday|wednesday|thursday|friday|tomorrow|today|"
    r"next week|calendar|earnings slate|auction|retail sales|fomc|"
    r"housing data|ppi|cpi)\b"
)
HOUSING_RE = re.compile(
    r"(?i)\b(housing|home sales|building permits|housing starts)\b"
)
RETAIL_RE = re.compile(r"(?i)\bretail sales\b")
FED_CAL_RE = re.compile(
    r"(?i)\b(fomc|federal reserve|the fed|fed(?:eral)?(?:\s+rate)?|"
    r"rate hike|rate cut)\b"
)
NEXT_SESSION_RE = re.compile(
    r"(?i)\b(monday|tuesday|wednesday|thursday|tomorrow|next week|"
    r"calendar|housing|retail sales|fomc|federal reserve|auction)\b"
)
EARNINGS_SLATE_RE = re.compile(
    r"(?i)\bearnings slate\b|"
    r"\b(?:monday|tuesday|wednesday|thursday|friday|today|tomorrow)'s earnings\b|"
    r"\bearnings (?:from|include|due|before|after|this)\b"
)
GEO_RE = re.compile(
    r"(?i)\b(middle east|iran|hormuz|geopolit|israel|gaza|ukraine|"
    r"houthi|tanker|strait of)\b"
)
GRAIN_RE = re.compile(r"(?i)\b(grain|wheat|corn export|russian grain)\b")
OIL_DOWN = frozenset({
    "retreated", "fell", "dropped", "slid", "lost", "slipped", "declined",
})
OIL_UP = frozenset({
    "rose", "gained", "jumped", "climbed", "advanced", "rallied", "added",
})
THEME_RADAR_KEYS = (
    "prior_close",
    "oil",
    "cpi_fed",
    "named_leaders",
    "next_session_calendar",
    "earnings_slate",
    "geo_grain",
)
WHY_SCRIPT_RE = re.compile(
    r'<script[^>]+id=["\']why-stock-moving-init-data["\'][^>]*>\s*(.*?)\s*</script>',
    re.I | re.S,
)
def decode_html_bytes(raw: bytes | str | None) -> str:
    """Decode homepage HTML. Wayback id_ snapshots are often gzip bodies."""
    if raw is None:
        return ""
    if isinstance(raw, str):
        return raw
    if raw[:2] == b"\x1f\x8b":
        try:
            raw = gzip.decompress(raw)
        except OSError:
            pass
    return raw.decode("utf-8", "replace")


def decode_response(resp: requests.Response) -> str:
    raw = resp.content if resp is not None else b""
    text = decode_html_bytes(raw)
    if text:
        return text
    return resp.text or ""


def et_now() -> datetime:
    return datetime.now(ET)


def parse_et_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=ET)
    return dt.astimezone(ET)


def wayback_ts_to_dt(ts: str) -> datetime | None:
    ts = re.sub(r"\D", "", ts or "")
    if len(ts) < 14:
        return None
    try:
        return datetime.strptime(ts[:14], "%Y%m%d%H%M%S").replace(tzinfo=UTC)
    except ValueError:
        return None


def session_open(date_str: str) -> datetime:
    return datetime.fromisoformat(f"{date_str}T09:30:00").replace(tzinfo=ET)


def clock_legal_at(dt: datetime | None, session_date: str) -> bool:
    """True when `dt` is strictly before that session's 09:30 ET open."""
    if dt is None:
        return False
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=ET)
    return dt.astimezone(ET) < session_open(session_date)


def next_session_date(date_str: str) -> str:
    """Next NYSE session after `date_str` (weekends + full-day holidays)."""
    from .skip_if_good import _next_weekday
    return _next_weekday(date_str)


def classify_clock(generated: datetime | None, session_date: str,
                   source: str) -> dict[str, Any]:
    """War-room clock: live same-morning only; Wayback afternoon → next open."""
    archive = source in ("wayback", "archive.ph")
    same_morning = clock_legal_at(generated, session_date)
    if same_morning:
        return {
            "clock_legal": True,
            "clock_same_morning": True,
            "clock_use": "same_morning",
            "clock_legal_for": session_date,
            "clock_rule": (
                "Live: Generated must be before 09:30 ET on this date or "
                "leave missing. Wayback: capture before 09:30 ET that day "
                "is legal for that morning's Pre-Open. Afternoon Wayback is "
                "legal for the NEXT session open only (prior-day close "
                "recap), marked by clock_legal_for. Gaps stay gaps. "
                "Capture only — not tape_anchor / predict / #210."
            ),
        }
    if archive and generated is not None:
        nxt = next_session_date(session_date)
        return {
            "clock_legal": True,
            "clock_same_morning": False,
            "clock_use": "next_open",
            "clock_legal_for": nxt,
            "clock_rule": (
                "Wayback midday/afternoon capture. Legal for the NEXT "
                f"session open only ({nxt}) as a prior-day close recap — "
                "NOT the same morning. Generated is the real archive "
                "capture time. Gaps stay gaps."
            ),
        }
    return {
        "clock_legal": False,
        "clock_same_morning": False,
        "clock_use": None,
        "clock_legal_for": None,
        "clock_rule": (
            "Live scrape after 09:30 ET — leave missing. Do not write an "
            "afternoon live file as if it were pre-open."
        ),
    }


def apply_clock(report: dict) -> dict:
    """Stamp clock_legal / clock_legal_for / clock_use from Generated + source."""
    generated = parse_et_iso(str(report.get("generated_at") or ""))
    date_str = str(report.get("date") or "")
    source = str(report.get("source") or "live")
    report.update(classify_clock(generated, date_str, source))
    return report


def strip_markup(text: str) -> str:
    if not text:
        return ""
    out = MD_LINK_RE.sub(r"\1", text)
    out = BOLD_RE.sub(r"\1", out)
    out = re.sub(r"<[^>]+>", " ", out)
    out = out.replace("\u00a0", " ")
    out = re.sub(r"[ \t]+", " ", out)
    out = re.sub(r"\n{3,}", "\n\n", out)
    return out.strip()


def _index_code(name: str) -> str:
    return INDEX_ALIASES.get(re.sub(r"\s+", " ", name.lower().strip()), name.upper())


def parse_index_moves(text: str) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for m in INDEX_MOVE_RE.finditer(text or ""):
        name = re.sub(r"\s+", " ", m.group(1)).strip()
        verb = m.group(2).lower()
        pct = float(m.group(3))
        down = verb in {
            "fell", "dropped", "slid", "retreated", "lost", "slipped", "declined",
        }
        if down and pct > 0:
            pct = -pct
        code = _index_code(name)
        if code in seen:
            continue
        seen.add(code)
        out.append({
            "name": name, "code": code, "change_pct": round(pct, 3), "verb": verb,
        })
    return out


def parse_oil(text: str) -> dict[str, Any] | None:
    m = OIL_RE.search(text or "")
    if not m:
        return None
    name = m.group(1)
    price = float(m.group(2))
    window = (text or "")[max(0, m.start() - 40): m.end() + 40]
    verb = None
    for word in ("retreated", "fell", "dropped", "slid", "rose", "gained", "jumped"):
        if re.search(rf"(?i)\b{word}\b", window):
            verb = word
            break
    if verb in OIL_DOWN:
        direction = "down"
    elif verb in OIL_UP:
        direction = "up"
    else:
        direction = None
    return {"name": name, "price": price, "verb": verb, "direction": direction}


def parse_cpi(text: str) -> dict[str, Any] | None:
    m = CPI_RE.search(text or "")
    if not m:
        return None
    snippet = re.sub(r"\s+", " ", m.group(0)).strip()
    hotter = bool(re.search(r"(?i)hotter|\bhot\b", snippet))
    return {"text": snippet, "hotter": hotter}


def parse_fed_odds(text: str) -> dict[str, Any] | None:
    m = FED_ODDS_RE.search(text or "")
    if not m:
        return None
    pct_s = m.group(1) or m.group(4)
    action = (m.group(2) or m.group(3) or "").lower()
    try:
        pct = float(pct_s)
    except (TypeError, ValueError):
        return None
    return {"pct": pct, "action": action, "text": m.group(0).strip()}


def parse_named_tickers(text: str) -> list[str]:
    found: list[str] = []
    skip = {
        "SPX", "SPY", "DJI", "DIA", "COMP", "NDX", "QQQ", "RUT", "IWM",
        "WTI", "CPI", "PPI", "FOMC", "ETF", "USD", "US", "AI", "ET",
        "AM", "PM", "CEO", "EPS", "GDP", "FED",
    }
    for rx in (TICKER_LINK_RE, PAREN_TICKER_RE):
        for m in rx.finditer(text or ""):
            t = m.group(1).upper()
            if t in skip or t in found:
                continue
            if not re.fullmatch(r"[A-Z]{1,5}", t):
                continue
            found.append(t)
    return found


def _iter_lines(text: str) -> list[str]:
    out: list[str] = []
    for raw in re.split(r"\n+|•", text or ""):
        line = strip_markup(raw).lstrip("-").strip()
        if line:
            out.append(line)
    return out


def parse_calendar_bullets(text: str) -> list[str]:
    bullets: list[str] = []
    for line in _iter_lines(text):
        if len(line) < 20:
            continue
        if CAL_HINT_RE.search(line):
            bullets.append(line)
    return bullets


def parse_next_session_calendar(text: str) -> dict[str, Any]:
    """Housing / retail / Fed mentions on the next-session calendar."""
    bullets: list[str] = []
    blob = strip_markup(text or "")
    for line in _iter_lines(text):
        if EARNINGS_SLATE_RE.search(line) and not (
            HOUSING_RE.search(line) or RETAIL_RE.search(line) or FED_CAL_RE.search(line)
        ):
            continue
        if NEXT_SESSION_RE.search(line) or HOUSING_RE.search(line) or RETAIL_RE.search(line):
            if len(line) >= 16:
                bullets.append(line)
    return {
        "housing": bool(HOUSING_RE.search(blob)),
        "retail": bool(RETAIL_RE.search(blob)),
        "fed": bool(FED_CAL_RE.search(blob)),
        "bullets": bullets,
        "text": " ".join(bullets).strip(),
    }


def parse_earnings_slate(text: str) -> dict[str, Any]:
    lines = [ln for ln in _iter_lines(text) if EARNINGS_SLATE_RE.search(ln)]
    tickers: list[str] = []
    for ln in lines:
        for t in parse_named_tickers(ln):
            if t not in tickers:
                tickers.append(t)
    return {
        "tickers": tickers,
        "text": " ".join(lines).strip(),
    }


def parse_geo_grain(text: str) -> dict[str, Any]:
    blob = strip_markup(text or "")
    flags: list[str] = []
    for rx, label in ((GEO_RE, "geo"), (GRAIN_RE, "grain")):
        if rx.search(blob) and label not in flags:
            flags.append(label)
    lines = [
        ln for ln in _iter_lines(text)
        if GEO_RE.search(ln) or GRAIN_RE.search(ln)
    ]
    return {
        "geo": "geo" in flags,
        "grain": "grain" in flags,
        "flags": flags,
        "text": " ".join(lines).strip(),
    }


def prior_close_from_moves(moves: list[dict] | None) -> dict[str, float | None]:
    return {
        "spx": _move_pct(moves or [], "SPX", "SPY"),
        "nasdaq": _move_pct(moves or [], "COMP", "NDX", "QQQ"),
        "dow": _move_pct(moves or [], "DJI", "DIA"),
    }


def theme_radar_fields(parsed: dict) -> dict[str, Any]:
    """First-class Theme Radar contract keys."""
    moves = parsed.get("index_moves") or []
    oil = parsed.get("oil")
    if isinstance(oil, dict) and "direction" not in oil:
        verb = str(oil.get("verb") or "")
        if verb in OIL_DOWN:
            oil = {**oil, "direction": "down"}
        elif verb in OIL_UP:
            oil = {**oil, "direction": "up"}
    leaders = list(parsed.get("named_leaders") or parsed.get("named_tickers") or [])
    raw = str(parsed.get("raw_text") or parsed.get("raw_summary_md") or "")
    nxt = parsed.get("next_session_calendar")
    if not isinstance(nxt, dict):
        nxt = parse_next_session_calendar(raw)
    earn = parsed.get("earnings_slate")
    if not isinstance(earn, dict):
        earn = parse_earnings_slate(raw)
    geo = parsed.get("geo_grain")
    if not isinstance(geo, dict):
        geo = parse_geo_grain(raw)
    return {
        "prior_close": parsed.get("prior_close") or prior_close_from_moves(moves),
        "oil": oil,
        "cpi_fed": {
            "cpi": parsed.get("cpi"),
            "fed_odds": parsed.get("fed_odds"),
        },
        "named_leaders": leaders,
        "next_session_calendar": nxt,
        "earnings_slate": earn,
        "geo_grain": geo,
    }


def _why_moving_from_obj(data: Any) -> dict | None:
    if not isinstance(data, dict):
        return None
    wm = data.get("whyMoving")
    if not isinstance(wm, dict):
        wm = data if "headline" in data and "summary" in data else None
    if not isinstance(wm, dict):
        return None
    ticker = str(wm.get("ticker") or "")
    source = str(wm.get("source") or "")
    headline = str(wm.get("headline") or "").strip()
    summary = str(wm.get("summary") or "").strip()
    if not (headline or summary):
        return None
    if source == "market_summary" or ticker in ("$MARKET", "MARKET", ""):
        return wm
    # Homepage widget is $MARKET. Ignore single-name "why is this moving".
    if ticker.startswith("$") or "S&P" in headline or "stocks" in headline.lower():
        return wm
    return None


def extract_why_moving(html: str) -> dict | None:
    if not html:
        return None
    m = WHY_SCRIPT_RE.search(html)
    if m:
        try:
            parsed = _why_moving_from_obj(json.loads(m.group(1)))
            if parsed:
                return parsed
        except (json.JSONDecodeError, TypeError, ValueError):
            pass
    # Some archives wrap / escape the script. Search a JSON island.
    m2 = re.search(
        r'\{"whyMoving"\s*:\s*\{.*?"source"\s*:\s*"market_summary".*?\}\s*,\s*"whyMovingRatings"',
        html, re.S,
    )
    blob = m2.group(0) + "}" if m2 else None
    if blob:
        try:
            parsed = _why_moving_from_obj(json.loads(blob))
            if parsed:
                return parsed
        except (json.JSONDecodeError, TypeError, ValueError):
            pass
    return None


def structured_from_text(text: str) -> dict[str, Any]:
    plain = strip_markup(text or "")
    index_moves = parse_index_moves(plain)
    oil = parse_oil(plain)
    cpi = parse_cpi(plain)
    fed_odds = parse_fed_odds(plain)
    named = parse_named_tickers(text or "")
    bits = {
        "index_moves": index_moves,
        "oil": oil,
        "cpi": cpi,
        "fed_odds": fed_odds,
        "named_tickers": named,
        "named_leaders": list(named),
        "calendar": parse_calendar_bullets(plain),
        "raw_text": plain,
        "prior_close": prior_close_from_moves(index_moves),
        "cpi_fed": {"cpi": cpi, "fed_odds": fed_odds},
        "next_session_calendar": parse_next_session_calendar(plain),
        "earnings_slate": parse_earnings_slate(text or ""),
        "geo_grain": parse_geo_grain(plain),
    }
    bits.update(theme_radar_fields(bits))
    return bits


def parse_market_digest_text(text: str, headline: str = "") -> dict[str, Any]:
    """Parse Cyrus-style pasted prose or cleaned summary bullets."""
    raw = (text or "").strip()
    bits = structured_from_text(raw)
    return {
        "headline": (headline or "").strip(),
        "raw_text": strip_markup(raw),
        "raw_summary_md": raw if "**" in raw or "](" in raw else "",
        **bits,
    }


def parse_homepage_html(html: str) -> dict | None:
    """Extract the homepage market-day narrative. None = not present (gap)."""
    html = decode_html_bytes(html)
    if finviz_session.looks_like_login_html(html):
        return None
    wm = extract_why_moving(html)
    if wm:
        headline = str(wm.get("headline") or "").strip()
        summary = str(wm.get("summary") or "").strip()
        parsed = parse_market_digest_text(summary, headline=headline)
        parsed.update({
            "finviz_id": wm.get("id"),
            "finviz_ticker": wm.get("ticker"),
            "finviz_published_at": wm.get("dateTime"),
            "finviz_source": wm.get("source"),
            "finviz_sentiment": wm.get("sentiment"),
            "raw_summary_md": summary,
        })
        if parsed.get("raw_text") or headline:
            return parsed
    return None


def fetch_homepage_html(sess: requests.Session | None = None
                        ) -> tuple[str | None, str, str | None]:
    """Return (html, source_url, error). Elite first, public homepage fallback."""
    sess = sess or finviz_session.session()
    r = finviz_session.get(sess, list(HOME_PATHS), timeout=40)
    if r is not None:
        return decode_response(r), r.url, None
    if not finviz_session.live_html_allowed():
        return None, "", "live_html_skipped"
    try:
        r = sess.get("https://finviz.com/", headers=UA, timeout=40)
    except requests.RequestException as e:
        return None, "", f"public homepage: {e}"
    if r.status_code == 200 and not finviz_session.looks_like_login_html(r.text):
        return decode_response(r), r.url, None
    return None, "", f"homepage empty/{r.status_code}"


def build_report(
    asof: str | None = None,
    html: str | None = None,
    source: str = "live",
    source_url: str = "",
    archive_ts: str | None = None,
    archive_url: str | None = None,
    error: str | None = None,
) -> dict:
    asof = asof or et_now().date().isoformat()
    archive_dt = wayback_ts_to_dt(archive_ts) if archive_ts else None
    # Generated = scrape time. Wayback/archive.ph use the snapshot clock,
    # not the reconstruction clock, so Theme Radar can trust < 09:30 ET.
    if source in ("wayback", "archive.ph") and archive_dt is not None:
        generated = archive_dt.astimezone(ET)
    else:
        generated = et_now()
    clock = classify_clock(generated, asof, source)
    parsed = parse_homepage_html(html) if html else None
    report = {
        "date": asof,
        "generated_at": generated.isoformat(),
        "timezone": "America/New_York",
        "source": source,
        "source_url": source_url,
        "archive_snapshot_ts": archive_ts,
        "archive_snapshot_at": (
            archive_dt.astimezone(ET).isoformat() if archive_dt else None
        ),
        "archive_url": archive_url,
        **clock,
        "kind": "finviz_homepage_market_digest",
        "represents": (
            "Finviz homepage market-day prose. Same-morning Pre-Open when "
            "Generated is before 09:30 ET; afternoon Wayback is a prior-day "
            "close recap legal for the next session only. "
            "Not quote-page ticker blurbs."
        ),
        "error": error,
        "headline": None,
        "raw_text": None,
        "raw_summary_md": None,
        "index_moves": [],
        "oil": None,
        "cpi": None,
        "fed_odds": None,
        "named_tickers": [],
        "named_leaders": [],
        "calendar": [],
        "prior_close": {"spx": None, "nasdaq": None, "dow": None},
        "cpi_fed": {"cpi": None, "fed_odds": None},
        "next_session_calendar": {
            "housing": False, "retail": False, "fed": False,
            "bullets": [], "text": "",
        },
        "earnings_slate": {"tickers": [], "text": ""},
        "geo_grain": {"geo": False, "grain": False, "flags": [], "text": ""},
    }
    if parsed:
        for k in (
            "headline", "raw_text", "raw_summary_md", "index_moves", "oil",
            "cpi", "fed_odds", "named_tickers", "named_leaders", "calendar",
            "prior_close", "cpi_fed", "next_session_calendar",
            "earnings_slate", "geo_grain", "finviz_id", "finviz_ticker",
            "finviz_published_at", "finviz_source", "finviz_sentiment",
        ):
            if k in parsed:
                report[k] = parsed[k]
    attach_theme_radar(report)
    return report


def attach_theme_radar(report: dict) -> dict:
    """Stamp first-class Theme Radar keys from narrative text. Clock unchanged."""
    raw = str(report.get("raw_summary_md") or report.get("raw_text") or "")
    if raw:
        bits = structured_from_text(raw)
        for k in (
            "index_moves", "oil", "cpi", "fed_odds", "named_tickers",
            "calendar", "prior_close", "cpi_fed", "named_leaders",
            "next_session_calendar", "earnings_slate", "geo_grain",
        ):
            if k in bits:
                report[k] = bits[k]
    else:
        report.update(theme_radar_fields(report))
    return report


def _move_pct(moves: list, *codes: str) -> float | None:
    by = {str(m.get("code") or ""): m for m in (moves or []) if isinstance(m, dict)}
    for code in codes:
        if code in by and by[code].get("change_pct") is not None:
            return float(by[code]["change_pct"])
    return None


def _fmt_pct(pct: float | None) -> str:
    if pct is None:
        return "—"
    return f"{pct:+.2f}%"


def _flag(ok: bool) -> str:
    return "yes" if ok else "no"


def _oil_stamp(oil: dict | None) -> str:
    if not oil or oil.get("price") is None:
        return "—"
    direction = oil.get("direction") or ""
    verb = oil.get("verb") or ""
    extra = " ".join(x for x in (direction, f"({verb})" if verb else "") if x)
    extra = f" {extra}" if extra else ""
    return f"{oil.get('name')} ${oil.get('price')}{extra}"


def _cpi_fed_stamp(report: dict) -> str:
    block = report.get("cpi_fed") if isinstance(report.get("cpi_fed"), dict) else {}
    cpi = (block or {}).get("cpi") or report.get("cpi") or {}
    fed = (block or {}).get("fed_odds") or report.get("fed_odds") or {}
    bits = []
    if isinstance(cpi, dict) and cpi.get("text"):
        bits.append(str(cpi["text"]))
    if isinstance(fed, dict) and fed.get("pct") is not None:
        bits.append(
            f"{fed.get('pct')}% {fed.get('action') or ''} "
            f"({fed.get('text') or ''})".strip()
        )
    return "; ".join(x for x in bits if x) or "—"


def to_markdown(report: dict) -> str:
    src = report.get("source") or "live"
    gen = report.get("generated_at") or ""
    headline = str(report.get("headline") or "").strip()
    prior = report.get("prior_close") if isinstance(report.get("prior_close"), dict) else {}
    if not prior:
        moves = report.get("index_moves") or []
        prior = prior_close_from_moves(moves)
    spx = _fmt_pct(prior.get("spx"))
    nasdaq = _fmt_pct(prior.get("nasdaq"))
    dow = _fmt_pct(prior.get("dow"))
    oil_s = _oil_stamp(report.get("oil") if isinstance(report.get("oil"), dict) else None)
    cpi_fed_s = _cpi_fed_stamp(report)
    leaders = ", ".join(
        report.get("named_leaders") or report.get("named_tickers") or []
    ) or "—"
    nxt = report.get("next_session_calendar") if isinstance(
        report.get("next_session_calendar"), dict) else {}
    earn = report.get("earnings_slate") if isinstance(
        report.get("earnings_slate"), dict) else {}
    geo = report.get("geo_grain") if isinstance(report.get("geo_grain"), dict) else {}
    earn_s = ", ".join(earn.get("tickers") or []) or (earn.get("text") or "—")
    geo_flags = ", ".join(geo.get("flags") or []) or "—"
    nxt_s = (
        f"housing {_flag(bool(nxt.get('housing')))} · "
        f"retail {_flag(bool(nxt.get('retail')))} · "
        f"Fed {_flag(bool(nxt.get('fed')))}"
    )
    lines = [
        f"# Finviz homepage market digest — {report.get('date')}",
        "",
        f"**Generated:** {gen} (America/New_York)",
        f"**Source:** `{src}`",
        f"**Banner:** {headline or '—'}",
        f"**Prior close:** SPX {spx}  Nasdaq {nasdaq}  Dow {dow}",
        f"**SPX:** {spx}  **Nasdaq:** {nasdaq}  **Dow:** {dow}",
        f"**Oil:** {oil_s}",
        f"**CPI/Fed:** {cpi_fed_s}",
        f"**Leaders:** {leaders}",
        f"**Next session:** {nxt_s}",
        f"**Earnings slate:** {earn_s}",
        f"**Geo/grain:** {geo_flags}",
    ]
    if report.get("archive_snapshot_ts") or report.get("archive_snapshot_at"):
        lines.append(
            f"**Archive snapshot:** `{report.get('archive_snapshot_ts')}` "
            f"({report.get('archive_snapshot_at')})"
        )
        if report.get("archive_url"):
            lines.append(f"**Archive URL:** {report['archive_url']}")
    use = report.get("clock_use") or ""
    legal_for = report.get("clock_legal_for") or "—"
    lines.append(f"**Clock legal for:** {legal_for}")
    lines.append(f"**Clock use:** `{use or '—'}`")
    if use == "next_open":
        clock_body = (
            "Wayback midday/afternoon capture. **Legal for the NEXT session "
            f"open only** (`clock_legal_for` = {legal_for}) as a prior-day "
            "close recap — **NOT** the same morning. Generated is the real "
            "archive capture time."
        )
    elif use == "same_morning":
        clock_body = (
            "Capture is **before 09:30 ET on this date**, so it is legal "
            f"for that morning's Pre-Open (`clock_legal_for` = {legal_for})."
        )
    else:
        clock_body = (
            "Live scrape after 09:30 ET — leave missing. Do not write an "
            "afternoon live file as if it were pre-open."
        )
    lines += [
        "",
        "## Clock",
        "",
        clock_body,
        "",
        "Homepage `$MARKET` prose — **not** `YYYY-MM-DD_finviz_digest.md` "
        "(quote-page headlines + ticker blurbs). Capture only. Not wired "
        "into tape_anchor / predict / #210.",
        "",
        "## Theme Radar",
        "",
        f"- **Prior close:** SPX {spx} · Nasdaq {nasdaq} · Dow {dow}",
        f"- **Oil:** {oil_s}",
        f"- **CPI / Fed-odds:** {cpi_fed_s}",
        f"- **Named leaders:** {leaders}",
        f"- **Next-session calendar:** {nxt_s}",
    ]
    for bullet in nxt.get("bullets") or []:
        lines.append(f"  - {bullet}")
    lines += [
        f"- **Earnings slate:** {earn_s}",
        f"- **Geo / grain:** {geo_flags}",
        "",
        "## Narrative",
        "",
    ]
    if headline:
        lines.append(f"**{headline}**")
        lines.append("")
    raw = report.get("raw_text") or ""
    if raw:
        lines.append(raw)
        lines.append("")
    else:
        err = report.get("error") or "no homepage market narrative in this snapshot"
        lines.append(f"_unavailable: {err}_")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def save_report(report: dict) -> tuple[Path, Path] | None:
    """Write same-morning files, or Wayback afternoon as next-open only.

    Live after 09:30 ET → leave missing. Paths are always
    `*_finviz_market_digest.*`. Never overwrite or rename the quote-page
    `*_finviz_digest.md` / `.json`.
    """
    apply_clock(report)
    date_str = report["date"]
    if report.get("clock_use") not in ("same_morning", "next_open"):
        print(f"[market_digest] {date_str}: live after 09:30 ET — leave missing")
        return None
    attach_theme_radar(report)
    NEWS_DIR.mkdir(parents=True, exist_ok=True)
    jp = NEWS_DIR / f"{date_str}_finviz_market_digest.json"
    mp = NEWS_DIR / f"{date_str}_finviz_market_digest.md"
    if jp.name.endswith("_finviz_digest.json") or mp.name.endswith("_finviz_digest.md"):
        raise RuntimeError("refusing to write quote-page finviz_digest path")
    jp.write_text(json.dumps(report, indent=2, ensure_ascii=False, default=str),
                  encoding="utf-8")
    mp.write_text(to_markdown(report), encoding="utf-8")
    latest = NEWS_DIR / "latest_finviz_market_digest.md"
    latest.write_text(mp.read_text(encoding="utf-8"), encoding="utf-8")
    return jp, mp


def report_has_narrative(report: dict | None) -> bool:
    if not isinstance(report, dict):
        return False
    raw = strip_markup(str(report.get("raw_text") or ""))
    headline = str(report.get("headline") or "").strip()
    return len(raw) >= 40 or len(headline) >= 20


def existing_morning_ok(date_str: str, force: bool = False) -> bool:
    if force:
        return False
    jp = NEWS_DIR / f"{date_str}_finviz_market_digest.json"
    if not jp.exists():
        return False
    try:
        payload = json.loads(jp.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return False
    if not report_has_narrative(payload):
        return False
    if payload.get("clock_use") != "same_morning":
        return False
    dt = parse_et_iso(str(payload.get("generated_at") or ""))
    return clock_legal_at(dt, date_str)


def existing_next_open_ok(date_str: str, force: bool = False) -> bool:
    if force:
        return False
    jp = NEWS_DIR / f"{date_str}_finviz_market_digest.json"
    if not jp.exists():
        return False
    try:
        payload = json.loads(jp.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return False
    if not report_has_narrative(payload):
        return False
    return payload.get("clock_use") == "next_open" and bool(payload.get("clock_legal_for"))


# ---------------------------------------------------------------------------
# Wayback / archive.ph backfill
# ---------------------------------------------------------------------------

def cdx_search(url: str, start: str, end: str,
               sess: requests.Session | None = None) -> list[dict]:
    """CDX rows for one URL. start/end are YYYY-MM-DD (inclusive)."""
    sess = sess or requests.Session()
    params = {
        "url": url,
        "matchType": "exact",
        "from": start.replace("-", ""),
        "to": end.replace("-", ""),
        "output": "json",
        "filter": "statuscode:200",
        "fl": "timestamp,original,statuscode,mimetype,length",
    }
    try:
        r = sess.get(CDX_URL, params=params, headers=UA, timeout=45)
    except requests.RequestException as e:
        print(f"[market_digest] CDX fail {url}: {e}")
        return []
    if r.status_code != 200:
        print(f"[market_digest] CDX {r.status_code} {url}")
        return []
    try:
        rows = r.json()
    except json.JSONDecodeError:
        return []
    if not isinstance(rows, list) or len(rows) < 2:
        return []
    out = []
    for row in rows[1:]:
        if not row or len(row) < 2:
            continue
        ts, orig = str(row[0]), str(row[1])
        dt = wayback_ts_to_dt(ts)
        if dt is None:
            continue
        et = dt.astimezone(ET)
        out.append({
            "timestamp": ts,
            "original": orig,
            "utc": dt.isoformat(),
            "et": et.isoformat(),
            "et_date": et.date().isoformat(),
            "length": row[4] if len(row) > 4 else None,
        })
    return out


def collect_cdx(start: str, end: str,
                sess: requests.Session | None = None) -> list[dict]:
    sess = sess or requests.Session()
    seen: set[str] = set()
    all_rows: list[dict] = []
    # Pad CDX from/to by one UTC day so evening ET captures are not dropped.
    cdx_from = (
        datetime.fromisoformat(start) - timedelta(days=1)
    ).date().isoformat()
    cdx_to = (
        datetime.fromisoformat(end) + timedelta(days=1)
    ).date().isoformat()
    for url in CDX_TARGETS:
        for row in cdx_search(url, cdx_from, cdx_to, sess=sess):
            key = row["timestamp"] + " " + row["original"]
            if key in seen:
                continue
            seen.add(key)
            all_rows.append(row)
    all_rows.sort(key=lambda r: r["timestamp"])
    return all_rows


def pick_capture_for_date(rows: list[dict], date_str: str) -> dict | None:
    """Best homepage snapshot on ET date `date_str`.

    Prefer the latest capture before 09:30 ET (same-morning Pre-Open).
    If none, take the latest midday/afternoon capture (next-open recap).
    """
    day = [r for r in rows if r.get("et_date") == date_str]
    if not day:
        return None
    pre = [r for r in day if clock_legal_at(parse_et_iso(r.get("et")), date_str)]
    if pre:
        return pre[-1]
    return day[-1]


def fetch_wayback_html(ts: str, original: str,
                       sess: requests.Session | None = None
                       ) -> tuple[str | None, str]:
    sess = sess or requests.Session()
    orig = original if original.startswith("http") else f"https://{original}"
    url = WAYBACK_ID.format(ts=ts, orig=orig)
    try:
        r = sess.get(url, headers=UA, timeout=50, allow_redirects=True)
    except requests.RequestException as e:
        print(f"[market_digest] wayback {ts}: {e}")
        return None, url
    if r.status_code != 200:
        print(f"[market_digest] wayback {ts}: HTTP {r.status_code}")
        return None, url
    return decode_response(r), r.url


def archive_ph_timemap(url: str, sess: requests.Session | None = None
                       ) -> list[dict]:
    """Best-effort archive.ph / archive.today TimeMap. Empty on 429/404."""
    sess = sess or requests.Session()
    out: list[dict] = []
    for host in ARCHIVE_PH_HOSTS:
        for path in (f"/timemap/json/{url}", f"/timemap/link/{url}"):
            try:
                r = sess.get(host + path, headers=UA, timeout=25,
                             allow_redirects=True)
            except requests.RequestException as e:
                print(f"[market_digest] archive.ph {host}: {e}")
                continue
            if r.status_code != 200:
                print(f"[market_digest] archive.ph {r.status_code} {host}{path}")
                continue
            text = r.text or ""
            # JSON TimeMap
            try:
                data = r.json()
            except json.JSONDecodeError:
                data = None
            if isinstance(data, list):
                for row in data:
                    if isinstance(row, dict) and row.get("datetime"):
                        out.append(row)
                    elif isinstance(row, list) and row:
                        out.append({"timestamp": row[0], "original": url})
            # Link-format Memento
            for m in re.finditer(
                r'<([^>]+)>;\s*rel="[^"]*memento[^"]*"[^;]*;\s*datetime="([^"]+)"',
                text, re.I,
            ):
                out.append({"url": m.group(1), "datetime": m.group(2)})
            if out:
                return out
    return out


def fetch_archive_ph_newest(url: str, sess: requests.Session | None = None
                            ) -> tuple[str | None, str | None, str | None]:
    """Return (html, snapshot_url, snapshot_ts_guess)."""
    sess = sess or requests.Session()
    for host in ARCHIVE_PH_HOSTS:
        target = f"{host}/newest/{url}"
        try:
            r = sess.get(target, headers=UA, timeout=40, allow_redirects=True)
        except requests.RequestException as e:
            print(f"[market_digest] archive.ph newest {host}: {e}")
            continue
        if r.status_code != 200:
            print(f"[market_digest] archive.ph newest {r.status_code} {host}")
            continue
        html = decode_response(r)
        ts = None
        m = re.search(r"/(\d{8,14})/", r.url)
        if m:
            ts = m.group(1)
        if "why-stock-moving" in html or "whyMoving" in html:
            return html, r.url, ts
    return None, None, None


def backfill_range(start: str, end: str, force: bool = False,
                   try_archive_ph: bool = True) -> dict:
    """Best-effort historical homepage narratives. Gaps stay gaps."""
    sess = requests.Session()
    sess.headers.update(UA)
    start_d = datetime.fromisoformat(start).date()
    end_d = datetime.fromisoformat(end).date()
    rows = collect_cdx(start, end, sess=sess)
    print(f"[market_digest] CDX captures={len(rows)} "
          f"et_days={len({r['et_date'] for r in rows})}")
    wrote = []
    skipped = []
    gaps = []
    day = start_d
    while day <= end_d:
        date_str = day.isoformat()
        day += timedelta(days=1)
        if existing_morning_ok(date_str, force=force) and not force:
            skipped.append(date_str)
            print(f"[market_digest] {date_str}: keep existing same-morning")
            continue
        cap = pick_capture_for_date(rows, date_str)
        cap_is_morning = bool(
            cap and clock_legal_at(parse_et_iso(cap.get("et")), date_str)
        )
        if (existing_next_open_ok(date_str, force=force) and not force
                and not cap_is_morning):
            skipped.append(date_str)
            print(f"[market_digest] {date_str}: keep existing next-open")
            continue
        html = None
        source = None
        source_url = ""
        archive_ts = None
        archive_url = None
        if cap:
            html, archive_url = fetch_wayback_html(
                cap["timestamp"], cap["original"], sess=sess)
            source = "wayback"
            source_url = archive_url
            archive_ts = cap["timestamp"]
        if (not html or parse_homepage_html(html) is None) and try_archive_ph:
            ph_html, ph_url, ph_ts = fetch_archive_ph_newest(
                "https://finviz.com/", sess=sess)
            parsed_ph = parse_homepage_html(ph_html) if ph_html else None
            # archive.ph /newest is "latest", not dated. Only accept if the
            # widget stamp or snapshot lands on this session date.
            if parsed_ph:
                pub = str(parsed_ph.get("finviz_published_at") or "")
                snap_et = wayback_ts_to_dt(ph_ts).astimezone(ET) if ph_ts and wayback_ts_to_dt(ph_ts) else None
                on_day = pub.startswith(date_str) or (
                    snap_et is not None and snap_et.date().isoformat() == date_str
                )
                if on_day:
                    html, source, source_url = ph_html, "archive.ph", ph_url or ""
                    archive_ts, archive_url = ph_ts, ph_url
                else:
                    print(f"[market_digest] {date_str}: archive.ph newest "
                          f"is other day ({pub[:10]}) — not reused")
        parsed = parse_homepage_html(html) if html else None
        if not parsed:
            gaps.append(date_str)
            print(f"[market_digest] {date_str}: gap (no archive narrative)")
            continue
        report = build_report(
            asof=date_str, html=html, source=source or "wayback",
            source_url=source_url, archive_ts=archive_ts,
            archive_url=archive_url,
        )
        if not report_has_narrative(report):
            gaps.append(date_str)
            print(f"[market_digest] {date_str}: gap (parsed empty)")
            continue
        if report.get("clock_use") not in ("same_morning", "next_open"):
            gaps.append(date_str)
            print(f"[market_digest] {date_str}: live after 09:30 ET — leave missing")
            continue
        saved = save_report(report)
        if not saved:
            gaps.append(date_str)
            continue
        jp, mp = saved
        wrote.append(date_str)
        print(
            f"[market_digest] {date_str}: {report['source']} "
            f"use={report.get('clock_use')} "
            f"legal_for={report.get('clock_legal_for')} "
            f"Generated={report['generated_at']} "
            f"snap={report.get('archive_snapshot_ts')} → {mp.name}"
        )
    refresh_latest()
    return {
        "from": start, "to": end,
        "wrote": wrote, "skipped": skipped, "gaps": gaps,
        "cdx_n": len(rows),
        "inventory": window_inventory(start, end),
    }


def refresh_latest() -> Path | None:
    files = sorted(
        p for p in NEWS_DIR.glob("*_finviz_market_digest.md")
        if p.name != "latest_finviz_market_digest.md"
    )
    if not files:
        return None
    latest = NEWS_DIR / "latest_finviz_market_digest.md"
    latest.write_text(files[-1].read_text(encoding="utf-8"), encoding="utf-8")
    return latest


def window_inventory(start: str, end: str) -> dict[str, Any]:
    """Classify each calendar day in [start, end] for the War-room report."""
    start_d = datetime.fromisoformat(start).date()
    end_d = datetime.fromisoformat(end).date()
    same_morning: list[str] = []
    next_open: list[str] = []
    gaps: list[str] = []
    day = start_d
    while day <= end_d:
        date_str = day.isoformat()
        day += timedelta(days=1)
        jp = NEWS_DIR / f"{date_str}_finviz_market_digest.json"
        if not jp.exists():
            gaps.append(date_str)
            continue
        try:
            payload = json.loads(jp.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            gaps.append(date_str)
            continue
        if not report_has_narrative(payload):
            gaps.append(date_str)
            continue
        use = payload.get("clock_use")
        src = str(payload.get("source") or "")
        if use == "same_morning" and src in ("wayback", "archive.ph"):
            same_morning.append(date_str)
        elif use == "same_morning":
            same_morning.append(date_str)
        elif use == "next_open":
            next_open.append(date_str)
        else:
            gaps.append(date_str)
    return {
        "from": start,
        "to": end,
        "path": "01_daily/news/{date}_finviz_market_digest.md",
        "same_morning_wayback": [
            d for d in same_morning
            if _file_source(d) in ("wayback", "archive.ph")
        ],
        "same_morning": same_morning,
        "next_open_only": next_open,
        "gaps": gaps,
        "n_same_morning_wayback": sum(
            1 for d in same_morning if _file_source(d) in ("wayback", "archive.ph")
        ),
        "n_next_open_only": len(next_open),
        "n_gaps": len(gaps),
    }


def _file_source(date_str: str) -> str:
    jp = NEWS_DIR / f"{date_str}_finviz_market_digest.json"
    try:
        return str(json.loads(jp.read_text(encoding="utf-8")).get("source") or "")
    except (OSError, json.JSONDecodeError):
        return ""


def _land(date_str: str) -> None:
    try:
        from . import land_file
        land_file.land(
            date_str, "finviz_market_digest",
            title="Finviz homepage market digest",
        )
    except Exception as e:  # noqa: BLE001
        print(f"[market_digest] WARN: land failed: {e}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=None)
    ap.add_argument("--html", default=None,
                    help="Parse this HTML file instead of live fetch")
    ap.add_argument("--text", default=None,
                    help="Parse this prose/markdown file (no scrape)")
    ap.add_argument("--force", action="store_true")
    ap.add_argument("--backfill", action="store_true")
    ap.add_argument("--from", dest="date_from", default="2026-08-20")
    ap.add_argument("--to", dest="date_to", default=None)
    ap.add_argument("--no-archive-ph", action="store_true")
    args = ap.parse_args()

    if args.backfill:
        end = args.date_to or et_now().date().isoformat()
        summary = backfill_range(
            args.date_from, end, force=args.force,
            try_archive_ph=not args.no_archive_ph,
        )
        print(json.dumps(summary, indent=2))
        for d in summary.get("wrote") or []:
            _land(d)
        return

    date_str = args.date or et_now().date().isoformat()
    if existing_morning_ok(date_str, force=args.force):
        print(f"[market_digest] {date_str}: skip, quality-ok already on disk")
        return
    html = None
    source = "live"
    source_url = "https://finviz.com/"
    err = None
    if args.text:
        raw = Path(args.text).read_text(encoding="utf-8")
        parsed = parse_market_digest_text(raw)
        report = build_report(asof=date_str, html=None, source="text")
        report.update(parsed)
        report["error"] = None
        apply_clock(report)
        saved = save_report(report)
        if not saved:
            return
        print(f"[market_digest] {date_str}: text → {saved[1]}")
        _land(date_str)
        return
    if args.html:
        html = decode_html_bytes(Path(args.html).read_bytes())
        source = "html_file"
        source_url = str(Path(args.html))
    else:
        html, source_url, err = fetch_homepage_html()
        source = "live"
    report = build_report(
        asof=date_str, html=html, source=source,
        source_url=source_url, error=err,
    )
    if not report_has_narrative(report):
        print(f"[market_digest] {date_str}: no narrative ({err or 'empty'})")
        # Do not write a stub that Theme Radar could mistake for a digest.
        raise SystemExit("finviz market digest: no homepage narrative")
    saved = save_report(report)
    if not saved:
        return
    jp, mp = saved
    print(
        f"[market_digest] {report['date']}: source={report['source']} "
        f"Generated={report['generated_at']} "
        f"tickers={report.get('named_tickers')} "
        f"moves={len(report.get('index_moves') or [])}"
    )
    print(f"[market_digest] {jp}")
    print(f"[market_digest] {mp}")
    _land(date_str)


if __name__ == "__main__":
    main()
