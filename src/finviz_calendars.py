"""Finviz calendar + futures tape parsers.

The redesigned Calendar embeds JSON in <script id=route-init-data>.
Economic pages are week buckets (dateFrom=Monday). The earnings table is
paginated; season-preview carries ~30 days of reports in one payload.
Futures tiles on /futures expose ~49 roots (Brent=QA, Bitcoin=BTC).
"""
from __future__ import annotations

import json
import re
from datetime import datetime, timedelta
from typing import Any

import requests

from . import finviz_session

CALENDAR_LOOKAHEAD_DAYS = 16

FUTURES_KEEP = [
    ("ES", "S&P 500"), ("NQ", "Nasdaq 100"), ("ER2", "Russell 2000"),
    ("YM", "DJIA"), ("VX", "VIX futures"),
    ("CL", "Crude WTI"), ("QA", "Brent"), ("NG", "Nat gas"),
    ("HO", "Heating oil"), ("RB", "RBOB"),
    ("GC", "Gold"), ("SI", "Silver"), ("HG", "Copper"),
    ("PL", "Platinum"), ("PA", "Palladium"),
    ("DX", "USD"), ("6E", "EUR"), ("6J", "JPY"), ("6B", "GBP"),
    ("ZN", "10Y note"), ("ZF", "5Y note"), ("ZT", "2Y note"), ("ZB", "30Y bond"),
    ("NKD", "Nikkei"), ("DY", "DAX"), ("EX", "Euro Stoxx"),
    ("ZC", "Corn"), ("ZS", "Soybeans"), ("ZW", "Wheat"),
    ("ZL", "Soy oil"), ("ZM", "Soy meal"),
    ("KC", "Coffee"), ("SB", "Sugar"), ("CT", "Cotton"), ("CC", "Cocoa"),
    ("LC", "Live cattle"), ("LH", "Lean hogs"),
    ("BTC", "Bitcoin"),
]
FUTURES_ALIAS = {
    "BZ": "QA", "LCO": "QA", "BT": "BTC", "VI": "VX", "VIX": "VX",
    "LE": "LC", "HE": "LH",
}


def _pct(val: Any) -> float | None:
    if val is None:
        return None
    s = str(val).strip().replace(",", "").replace("%", "")
    if s.lower() in ("", "-", "nan", "none", "null"):
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _numeric_surprise(actual: Any, forecast: Any) -> float | None:
    a, f = _pct(actual), _pct(forecast)
    if a is None or f is None:
        return None
    return round(a - f, 4)


def _monday_on_or_before(day: datetime) -> datetime:
    return day - timedelta(days=day.weekday())


def _calendar_week_starts(asof: str, lookahead: int = CALENDAR_LOOKAHEAD_DAYS) -> list[str]:
    start = datetime.fromisoformat(asof[:10])
    first = _monday_on_or_before(start)
    last = start + timedelta(days=lookahead)
    out = []
    cur = first
    while cur <= last:
        out.append(cur.date().isoformat())
        cur += timedelta(days=7)
    return out


def _route_init_data(html: str) -> dict:
    m = re.search(
        r'<script id="route-init-data" type="application/json">(.*?)</script>',
        html or "",
        re.S,
    )
    if not m:
        return {}
    try:
        blob = json.loads(m.group(1))
    except json.JSONDecodeError:
        return {}
    return blob.get("data") or blob


def _walk_dicts(obj: Any):
    if isinstance(obj, dict):
        yield obj
        for v in obj.values():
            yield from _walk_dicts(v)
    elif isinstance(obj, list):
        for v in obj:
            yield from _walk_dicts(v)


def _event_day(row: dict) -> str:
    return str(row.get("datetime") or row.get("date") or "")[:10]


def _in_window(day: str, asof: str, lookahead: int = CALENDAR_LOOKAHEAD_DAYS) -> bool:
    if not day:
        return False
    try:
        d = datetime.fromisoformat(day).date()
        start = datetime.fromisoformat(asof[:10]).date()
    except ValueError:
        return day >= asof
    return start <= d <= start + timedelta(days=lookahead)


def _clean_cal_val(val: Any) -> Any:
    if val is None:
        return None
    if isinstance(val, str):
        s = val.strip()
        if s.lower() in ("", "-", "null", "none", "nan"):
            return None
        return s
    return val


def parse_econ_html(html: str, asof: str) -> list[dict]:
    rows: list[dict] = []
    data = _route_init_data(html)
    entries = data.get("entries") if isinstance(data, dict) else None
    pool: list[dict] = []
    if isinstance(entries, list):
        pool = [e for e in entries if isinstance(e, dict)]
    elif isinstance(entries, dict):
        pool = [e for e in _walk_dicts(entries) if "event" in e and "date" in e]
    if not pool:
        for m in re.finditer(
            r'\{"calendarId":(\d+),.*?"event":"(.*?)".*?"date":"(.*?)".*?'
            r'"actual":(.*?),.*?"previous":(.*?),.*?"forecast":(.*?),.*?'
            r'"importance":(\d+)',
            html or "",
        ):
            pool.append({
                "event": m.group(2).encode("utf-8").decode("unicode_escape"),
                "date": m.group(3),
                "actual": None if m.group(4) == "null" else m.group(4).strip('"'),
                "previous": None if m.group(5) == "null" else m.group(5).strip('"'),
                "forecast": None if m.group(6) == "null" else m.group(6).strip('"'),
                "importance": int(m.group(7)),
                "category": "",
            })
    for raw in pool:
        when = str(raw.get("date") or "")
        day = when[:10]
        if not _in_window(day, asof):
            continue
        actual = _clean_cal_val(raw.get("actual"))
        fc = _clean_cal_val(raw.get("forecast") or raw.get("teforecast"))
        prev = _clean_cal_val(raw.get("previous"))
        try:
            importance = int(raw.get("importance") or 0)
        except (TypeError, ValueError):
            importance = 0
        rows.append({
            "event": raw.get("event") or "",
            "datetime": when,
            "actual": actual,
            "previous": prev,
            "forecast": fc,
            "surprise": _numeric_surprise(actual, fc),
            "importance": importance,
            "category": raw.get("category") or "",
        })
    return rows


def fetch_econ(sess: requests.Session, date: str) -> list[dict]:
    rows: list[dict] = []
    seen_weeks = set()
    for week in _calendar_week_starts(date):
        if week in seen_weeks:
            continue
        seen_weeks.add(week)
        r = finviz_session.get(sess, [
            f"/calendar/economic?dateFrom={week}",
            f"/calendar.ashx?d={week}",
        ])
        if r is None:
            print(f"[map_heat] econ calendar week {week} failed: Elite session empty/403")
            continue
        got = parse_econ_html(r.text, date)
        print(f"[map_heat] econ week {week}: {len(got)}")
        rows.extend(got)
    seen = set()
    uniq = []
    for row in rows:
        k = (row["event"], row["datetime"])
        if k in seen:
            continue
        seen.add(k)
        uniq.append(row)
    uniq.sort(key=lambda x: (x.get("datetime") or "", -(x.get("importance") or 0)))
    print(f"[map_heat] econ {date} window: {len(uniq)}")
    return uniq


def _earnings_session(when: str) -> str:
    if "T16" in when or "T20" in when or "T21" in when or "T22" in when:
        return "AMC"
    return "BMO"


def parse_earnings_html(html: str, asof: str) -> list[dict]:
    rows: list[dict] = []
    data = _route_init_data(html)
    pool: list[dict] = []
    if isinstance(data, dict):
        for obj in _walk_dicts(data):
            if obj.get("ticker") and (obj.get("earningsDate") or obj.get("date")):
                pool.append(obj)
    if not pool:
        for m in re.finditer(
            r'\{"earningsDate":"(.*?)".*?"ticker":"(.*?)".*?"company":"(.*?)".*?'
            r'"marketCap":([0-9.]+).*?"epsEstimate":(.*?),',
            html or "",
        ):
            pool.append({
                "earningsDate": m.group(1),
                "ticker": m.group(2),
                "company": m.group(3),
                "marketCap": float(m.group(4)),
                "epsEstimate": None if m.group(5) == "null" else m.group(5),
            })
    seen = set()
    for raw in pool:
        when = str(raw.get("earningsDate") or raw.get("date") or "")
        day = when[:10]
        ticker = str(raw.get("ticker") or "").upper()
        if not ticker or not _in_window(day, asof):
            continue
        key = (ticker, day)
        if key in seen:
            continue
        seen.add(key)
        est = _clean_cal_val(raw.get("epsEstimate") or raw.get("eps_est"))
        if est is not None:
            try:
                est = float(est)
            except (TypeError, ValueError):
                pass
        mcap = raw.get("marketCap")
        try:
            mcap_f = float(mcap) if mcap is not None else 0.0
        except (TypeError, ValueError):
            mcap_f = 0.0
        rows.append({
            "ticker": ticker,
            "company": raw.get("company") or ticker,
            "datetime": when if "T" in when else f"{day}T00:00:00",
            "session": _earnings_session(when),
            "mcap": mcap_f,
            "eps_est": est,
        })
    rows.sort(key=lambda x: (-(x.get("mcap") or 0), x.get("datetime") or ""))
    return rows


def fetch_earnings(sess: requests.Session, date: str) -> list[dict]:
    rows: list[dict] = []
    r = finviz_session.get(sess, [
        "/calendar/earnings/season-preview",
        "/calendar/earnings",
    ])
    if r is not None:
        rows = parse_earnings_html(r.text, date)
        print(f"[map_heat] earnings preview: {len(rows)}")
    else:
        print("[map_heat] earnings season-preview failed: Elite session empty/403")
    if len(rows) < 8:
        for week in _calendar_week_starts(date):
            wr = finviz_session.get(sess, [f"/calendar/earnings?dateFrom={week}"])
            if wr is None:
                continue
            extra = parse_earnings_html(wr.text, date)
            print(f"[map_heat] earnings week {week}: {len(extra)}")
            rows.extend(extra)
        seen = set()
        uniq = []
        for row in rows:
            k = (row["ticker"], _event_day(row))
            if k in seen:
                continue
            seen.add(k)
            uniq.append(row)
        rows = uniq
        rows.sort(key=lambda x: (-(x.get("mcap") or 0), x.get("datetime") or ""))
    print(f"[map_heat] earnings {date} window: {len(rows)}")
    return rows


def tape_from_futures(futures: dict) -> list[dict]:
    tape = []
    used = set()
    index = {str(k).upper(): row for k, row in futures.items()}
    for alias, root in FUTURES_ALIAS.items():
        if alias in index and root not in index:
            index[root] = index[alias]

    def _add(ticker: str, label: str | None = None) -> None:
        key = ticker.upper()
        row = index.get(key)
        if not row or key in used:
            return
        used.add(key)
        tape.append({
            "ticker": key,
            "label": row.get("label") or label or key,
            "last": row.get("last"),
            "change": row.get("change"),
        })

    for ticker, label in FUTURES_KEEP:
        _add(ticker, label)
    leftovers = []
    for key, row in futures.items():
        k = str(key).upper()
        if k in used or not isinstance(row, dict):
            continue
        leftovers.append((k, row.get("label") or k, row.get("last"), row.get("change")))
    leftovers.sort(key=lambda x: x[1])
    for ticker, label, last, change in leftovers:
        tape.append({"ticker": ticker, "label": label, "last": last, "change": change})
    return tape


def calendar_fields(econ: list[dict], earns: list[dict], asof: str | None = None) -> dict:
    mega_earn = [e for e in earns if (e.get("mcap") or 0) >= 50_000][:20]
    high_econ = [e for e in econ if e.get("importance", 0) >= 2][:24]
    if asof:
        today = asof[:10]
        today_high = [e for e in high_econ if _event_day(e) == today]
        mega_today = [e for e in mega_earn if _event_day(e) == today]
    else:
        today_high = high_econ
        mega_today = mega_earn
    macro_gate = bool(today_high)
    tickers = [
        str(e.get("ticker") or "").upper()
        for e in mega_today if e.get("ticker")
    ]
    return {
        "econ": high_econ,
        "earnings": mega_earn,
        "econ_today": today_high,
        "earnings_today": mega_today,
        "macro_gate": macro_gate,
        "earnings_gate": bool(mega_today),
        "size_gate": macro_gate,
        "calendar_entry_scale": 0.5 if macro_gate else 1.0,
        "earnings_entry_tickers": tickers,
        "calendar_window_days": CALENDAR_LOOKAHEAD_DAYS,
    }
