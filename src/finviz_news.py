"""Company-level Finviz news for the RYG checklist.

The Elite export already has News Title + Daily Digest + News Time on
every name. The daily digest JSON kept only a ranked sample, and
news_actions only booked RSS/macro buckets, so the lookback ``news``
box stayed missing / ungradable.

This module is the shared reader: parse Finviz timestamps, score a
headline, and return every ticker that actually has company news.
"""
from __future__ import annotations

import functools
import re
from datetime import date, datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
EXPORT_DIR = ROOT / "data" / "exports"

# Month names are always English on Finviz — do not use locale %b.
_FINVIZ_MONTHS = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}
_FINVIZ_RELATIVE = re.compile(
    r"(?i)^\s*(today|yesterday|(\d+)\s*(min|mins|minutes|hour|hours|hr|hrs))\b"
)
_FINVIZ_TIME_ONLY = re.compile(r"(?i)^\s*\d{1,2}:\d{2}\s*(AM|PM)\s*$")
_FINVIZ_DATE_TOKEN = re.compile(
    r"(?i)\b([A-Za-z]{3}-\d{1,2}-\d{2,4}|[A-Za-z]{3}-\d{1,2})\b"
)
DIVIDEND_RE = re.compile(
    r"(?i)((declares?|announces?|declared).{0,60}dividend"
    r"|quarterly (cash )?dividend"
    r"|dividend of \$?[\d.]+ per share)"
)
POSITIVE_RE = re.compile(
    r"\b(beat|beats|beating|raises?|raised|upgrade[sd]?|approval|approved|"
    r"record|surges?|wins?|won|contract|buyback|reaffirm[sd]?|"
    r"reduces? risk|cut(?:s)? (?:all-cause )?mortality|positive phase)\b",
    re.I,
)
NEGATIVE_RE = re.compile(
    r"\b(miss(?:es|ed)?|weak|downgrade[sd]?|lowers?|lowered|"
    r"cuts? guidance|cuts?.{0,40}price target|price target cut|"
    r"bankrupt(?:cy)?|offering|dilution|fraud|"
    r"investigat(?:e|es|ion)|warning|plunges?|selloff|recall|"
    r"lawsuit|litigation|sued|class action|"
    r"profit.taking|insider sale|insider sold|ceo .* sold|sold .* stock|"
    r"sells? \$?[\d,.]+ (?:million|billion).*(?:stock|shares))\b",
    re.I,
)
HIGH_MATERIALITY_RE = re.compile(
    r"\b(earnings|revenue|eps|guidance|phase [123]|mortality|fda|"
    r"approval|clinical|merger|acqui(?:re|res|sition)|contract|"
    r"buyback|bankrupt(?:cy)?|offering|fraud|trial|primary endpoint|"
    r"raises? (?:fy|full.year|outlook))\b",
    re.I,
)
ETF_TICKERS = {
    "SPY", "QQQ", "DIA", "IWM", "XLE", "XLY", "XLK", "XLF", "XLV",
    "XLI", "XLB", "XLU", "XLRE", "XLC", "GDX", "SMH", "SOXX",
}


def _date_from_finviz_token(token: str, today_d: date) -> date | None:
    parts = (token or "").split("-")
    if len(parts) < 2:
        return None
    mon = _FINVIZ_MONTHS.get(parts[0][:3].lower())
    if not mon:
        return None
    try:
        day = int(parts[1])
        if len(parts) >= 3:
            year = int(parts[2])
            if year < 100:
                year += 2000
            return date(year, mon, day)
        parsed = date(today_d.year, mon, day)
        if parsed > today_d:
            parsed = date(today_d.year - 1, mon, day)
        return parsed
    except ValueError:
        return None


def parse_finviz_news_date(raw, *, last_date=None, today=None) -> str | None:
    """Normalize a Finviz news timestamp to YYYY-MM-DD."""
    today_s = today or date.today().isoformat()
    try:
        today_d = datetime.strptime(str(today_s)[:10], "%Y-%m-%d").date()
    except ValueError:
        today_d = date.today()

    if raw is None:
        return last_date
    if hasattr(raw, "date") and callable(getattr(raw, "date")) and not isinstance(raw, str):
        try:
            return raw.date().isoformat()
        except Exception:
            pass

    s = " ".join(str(raw).split())
    if not s or s.lower() in ("nan", "nat", "none", "nat+", ""):
        return last_date

    if re.match(r"^\d{4}-\d{2}-\d{2}", s):
        return s[:10]

    rel = _FINVIZ_RELATIVE.match(s)
    if rel:
        word = (rel.group(1) or "").lower()
        if word == "yesterday":
            return (today_d - timedelta(days=1)).isoformat()
        return today_d.isoformat()

    if _FINVIZ_TIME_ONLY.match(s):
        return last_date or today_d.isoformat()

    token_m = _FINVIZ_DATE_TOKEN.search(s)
    if token_m:
        parsed = _date_from_finviz_token(token_m.group(1), today_d)
        if parsed:
            return parsed.isoformat()

    for fmt in ("%I:%M %p %m/%d/%Y", "%m/%d/%Y", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(s, fmt).date().isoformat()
        except ValueError:
            continue
    return last_date


def headline_tone(text: str) -> str:
    """good / bad / neutral. Neutral still means the headline is gradable."""
    value = str(text or "").strip()
    if not value:
        return "missing"
    if re.search(
        r"\b(insider|ceo|cfo|director)\b.*\b(sold|sells|sale)\b",
        value, re.I,
    ):
        return "bad"
    pos = len(POSITIVE_RE.findall(value))
    neg = len(NEGATIVE_RE.findall(value))
    if pos > neg:
        return "good"
    if neg > pos:
        return "bad"
    return "neutral"


def headline_net(text: str) -> float:
    tone = headline_tone(text)
    if tone == "good":
        return 1.6
    if tone == "bad":
        return -1.6
    return 0.0


def _clean(value) -> str:
    s = str(value or "").strip()
    if not s or s.lower() in ("nan", "none", "-", "nat"):
        return ""
    return re.sub(r"\s+", " ", s)


def _latest_export(asof: str | None = None) -> Path | None:
    files = sorted(EXPORT_DIR.glob("finviz_????-??-??.csv"))
    if not files:
        return None
    if asof:
        exact = EXPORT_DIR / f"finviz_{asof}.csv"
        if exact.exists():
            return exact
        prior = [f for f in files if f.stem.replace("finviz_", "") <= asof]
        # Never fall forward to a later export — that would leak future
        # headlines onto an older lookback session.
        return prior[-1] if prior else None
    return files[-1]


@functools.lru_cache(maxsize=16)
def load_company_news(asof: str | None = None, *, path: Path | None = None,
                      today: str | None = None) -> dict[str, dict]:
    """Every ticker with a News Title and/or Daily Digest on the Elite export.

    Does not use the truncated digest JSON. Neutral headlines are kept so
    the checklist news box can be yellow instead of blank.
    """
    export = path or _latest_export(asof)
    if export is None or not export.exists():
        return {}
    try:
        import pandas as pd
    except ImportError:
        return {}
    df = pd.read_csv(export, low_memory=False)
    tcol = "Ticker" if "Ticker" in df.columns else df.columns[0]
    today_s = today or asof or date.today().isoformat()
    out: dict[str, dict] = {}
    for _, row in df.iterrows():
        ticker = str(row.get(tcol) or "").strip().upper()
        if not ticker or ticker in ETF_TICKERS:
            continue
        digest = _clean(row.get("Daily Digest"))
        title = _clean(row.get("News Title"))
        if not digest and not title:
            continue
        text = digest or title
        # Finviz often stamps a quarterly dividend into Daily Digest while
        # News Title is the real story. Skip only when the headline itself
        # is the dividend announcement.
        is_div = bool(DIVIDEND_RE.search(title or digest))
        title_tone = headline_tone(title) if title else "missing"
        digest_tone = headline_tone(digest) if digest else "missing"
        if title_tone in ("good", "bad"):
            news_tone = title_tone
        elif digest_tone in ("good", "bad"):
            news_tone = digest_tone
        else:
            news_tone = title_tone if title_tone != "missing" else digest_tone
        tone = digest_tone if digest_tone != "missing" else title_tone
        event_date = parse_finviz_news_date(
            row.get("News Time"), today=today_s,
        )
        out[ticker] = {
            "ticker": ticker,
            "digest": digest,
            "news_title": title,
            "text": text,
            "news_time_raw": _clean(row.get("News Time")),
            "event_date": event_date,
            "tone": tone,
            "news_tone": news_tone,
            "digest_tone": digest_tone,
            "net": headline_net(text),
            "is_dividend": is_div,
            "materiality": "high" if HIGH_MATERIALITY_RE.search(text) else "normal",
            "sector": _clean(row.get("Sector")),
            "industry": _clean(row.get("Industry")),
            "source": "finviz_export",
        }
    return out


def actions_from_company_news(news: dict[str, dict]) -> dict[str, dict]:
    """Signed news-book rows for names with a directional company headline."""
    out: dict[str, dict] = {}
    for ticker, rec in (news or {}).items():
        if rec.get("is_dividend"):
            continue
        net = float(rec.get("net") or 0)
        if not net:
            continue
        out[ticker] = {
            "ticker": ticker,
            "net": net,
            "side": "buy" if net > 0 else "sell",
            "events": [{
                "event": "finviz_company",
                "side": "buy" if net > 0 else "sell",
                "weight": abs(net),
                "bucket": "company",
                "digest": str(rec.get("text") or "")[:160],
                "event_date": rec.get("event_date"),
            }],
            "source": "finviz_company",
        }
    return out
