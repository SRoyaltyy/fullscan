"""Fail-closed freshness for the Pre-Open news window.

2026-09-24 Pre-Open stamped hours=48 on 400 headlines whose published_at
clustered on 2026-08-26..08-29 (median 2026-08-28). A same-session packet
cannot treat that as overnight news.

Thresholds (session date S, as-of 09:30 ET on S):

* Median published calendar date must be on or after S minus 2 trading
  days (weekends skipped, no holiday calendar). Thursday 2026-09-24's
  floor is Tuesday 2026-09-22. Two sessions is the outer edge of a 48h
  query; anything older is a carry-forward.
* At least half of *dated* items must fall in the 36h before that as-of.
  36h covers the entire prior regular session plus the overnight. A real
  48h pull is dominated by that span. The 09-24 file was 0%.
* Fewer than 8 dated items cannot prove a window (undated Finviz titles
  do not count as fresh).

Either failing test is stale. ``news_mode`` is ``"on"`` or ``"none_stale"``.
"""
from __future__ import annotations

import json
import re
from datetime import date, datetime, time as dtime, timedelta, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from statistics import median
from zoneinfo import ZoneInfo

from . import config

ET = ZoneInfo(config.TZ)
ROOT = Path(__file__).resolve().parent.parent
NEWS_MODE_ON = "on"
NEWS_MODE_STALE = "none_stale"
MIN_DATED = 8
MIN_RECENT_SHARE = 0.50
RECENT_HOURS = 36
MEDIAN_MAX_TRADING_DAYS = 2
# grok_ok false is a hard stop only when the fail is the news window,
# not an unrelated tape disagreement (map-heat futures on 09-24).
_STALE_REASON = re.compile(
    r"stale|carry-forward|carry forward|prior-date|prior date|"
    r"not a same-day|not the \d{4}-\d{2}-\d{2} session|month-old|"
    r"old news|news window",
    re.I,
)
_NEWS_PATH = re.compile(r"parsed|actions|news", re.I)


def parse_published(raw: object) -> datetime | None:
    """Parse RSS / ISO published_at. None when the stamp is missing or junk."""
    text = str(raw or "").strip()
    if not text:
        return None
    dt: datetime | None = None
    if text[0].isdigit():
        try:
            dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            dt = None
    if dt is None:
        try:
            dt = parsedate_to_datetime(text)
        except (TypeError, ValueError, IndexError, OverflowError):
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def trading_days_before(session: date, n: int) -> date:
    """Calendar date n weekdays before ``session`` (session itself not counted)."""
    d = session
    left = max(0, int(n))
    while left > 0:
        d -= timedelta(days=1)
        if d.weekday() < 5:
            left -= 1
    return d


def assess(items: list[dict] | None, session: str,
           *, asof: datetime | None = None) -> dict:
    """Freshness of a headline batch for session ``YYYY-MM-DD``.

    Undated rows are ignored for the median and do not count as recent.
    """
    session_d = date.fromisoformat(str(session)[:10])
    asof_dt = asof or datetime.combine(session_d, dtime(9, 30), tzinfo=ET)
    if asof_dt.tzinfo is None:
        asof_dt = asof_dt.replace(tzinfo=ET)
    recent_cut = asof_dt.astimezone(timezone.utc) - timedelta(hours=RECENT_HOURS)
    floor = trading_days_before(session_d, MEDIAN_MAX_TRADING_DAYS)
    parsed: list[datetime] = []
    n_items = 0
    for it in items or []:
        if not isinstance(it, dict):
            continue
        n_items += 1
        dt = parse_published(it.get("published_at"))
        if dt is not None:
            parsed.append(dt)
    n_dated = len(parsed)
    n_recent = sum(1 for dt in parsed if dt >= recent_cut)
    share = (n_recent / n_dated) if n_dated else 0.0
    med_dt = datetime.fromtimestamp(median(dt.timestamp() for dt in parsed),
                                    tz=timezone.utc) if parsed else None
    med_date = med_dt.astimezone(ET).date() if med_dt is not None else None
    reasons: list[str] = []
    if n_dated < MIN_DATED:
        reasons.append(
            f"only {n_dated} dated headlines (need >={MIN_DATED} to prove "
            f"the window; undated titles do not count)")
    if n_dated and share < MIN_RECENT_SHARE:
        reasons.append(
            f"{share:.0%} of dated headlines are inside {RECENT_HOURS}h "
            f"of {session} 09:30 ET (need >={MIN_RECENT_SHARE:.0%})")
    if med_date is not None and med_date < floor:
        reasons.append(
            f"median published {med_date.isoformat()} is older than "
            f"{MEDIAN_MAX_TRADING_DAYS} trading days before {session} "
            f"(floor {floor.isoformat()})")
    ok = not reasons
    return {
        "ok": ok,
        "news_mode": NEWS_MODE_ON if ok else NEWS_MODE_STALE,
        "reason": "; ".join(reasons),
        "session": session_d.isoformat(),
        "asof": asof_dt.isoformat(),
        "n_items": n_items,
        "n_dated": n_dated,
        "n_recent": n_recent,
        "recent_hours": RECENT_HOURS,
        "min_recent_share": MIN_RECENT_SHARE,
        "recent_share": round(share, 4),
        "median_published": med_date.isoformat() if med_date else "",
        "median_floor": floor.isoformat(),
        "median_max_trading_days": MEDIAN_MAX_TRADING_DAYS,
        "min_dated": MIN_DATED,
    }


def grok_stale_reason(session: str, root: Path | None = None) -> str:
    """Stale-news sentence from grok_ok false. Empty when the fail is not news.

    Map-heat tape disagreements are not a news-window stop.
    """
    root = root or ROOT
    path = root / "01_daily" / f"{session}_grok_review.json"
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, json.JSONDecodeError):
        return ""
    if not isinstance(data, dict):
        return ""
    if data.get("ok") and not (data.get("fails") or []):
        return ""
    hits: list[str] = []
    for fail in data.get("fails") or []:
        if not isinstance(fail, dict):
            continue
        rel = str(fail.get("path") or "")
        reason = str(fail.get("reason") or "").strip()
        if not reason or not _NEWS_PATH.search(rel):
            continue
        if not _STALE_REASON.search(reason):
            continue
        hits.append(reason)
    return hits[0] if hits else ""


def _read_parsed(session: str, root: Path) -> dict:
    path = root / "01_daily" / "news" / f"{session}_parsed.json"
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, json.JSONDecodeError):
        return {}
    return data if isinstance(data, dict) else {}


def decision(session: str, root: Path | None = None) -> dict:
    """Whether news-dependent Pre-Open parts may run for this session.

    Order: explicit ``news_mode: none_stale`` on the parse, then a live
    assess of dated items (so today's unmarked Aug 26-29 file still
    stops), then grok_ok false with a stale-news reason. A legacy parse
    with no dates and no stale stamp is left alone.
    """
    root = root or ROOT
    data = _read_parsed(session, root)
    stamped = str(data.get("news_mode") or "")
    freshness = data.get("freshness") if isinstance(data.get("freshness"), dict) else {}
    if stamped == NEWS_MODE_STALE or freshness.get("ok") is False:
        reason = str(freshness.get("reason") or data.get("abstain_reason")
                     or "news window failed the freshness gate")
        grok = grok_stale_reason(session, root)
        return {
            "ok": False,
            "news_mode": NEWS_MODE_STALE,
            "reason": grok or reason,
            "via": "grok" if grok else "parse_stamp",
        }
    items = [it for it in (data.get("all_items") or []) if isinstance(it, dict)]
    if any(str(it.get("published_at") or "").strip() for it in items):
        verdict = assess(items, session)
        if not verdict["ok"]:
            return {
                "ok": False,
                "news_mode": NEWS_MODE_STALE,
                "reason": verdict["reason"],
                "via": "assess",
                "freshness": verdict,
            }
    grok = grok_stale_reason(session, root)
    if grok:
        return {
            "ok": False,
            "news_mode": NEWS_MODE_STALE,
            "reason": grok,
            "via": "grok",
        }
    return {"ok": True, "news_mode": NEWS_MODE_ON, "reason": "", "via": "fresh"}


def banner(reason: str) -> str:
    why = (reason or "news window failed the freshness gate").strip()
    return (
        f"NEWS_MODE: {NEWS_MODE_STALE}\n"
        "News-dependent inputs are off for this session "
        f"({why}). Do not use news actions, catalyst headlines, "
        "the news judge, or Grok news picks. Score from tape, weather, "
        "and factor inputs only.\n"
    )
