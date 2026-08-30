"""Finviz calendar + futures tape parsers.

Verified against live Elite HTML (2026-08-30 fixtures):

Economic /calendar/economic?dateFrom=<Monday>
  React page. No <table>. Payload is
    <script id="route-init-data">{"data":{"initialDateFrom","entries":[...]}}
  entries[] keys: calendarId, ticker, event, category, date, actual,
  previous, forecast, teforecast, importance (1-3).
  Default URL (no dateFrom) on Sat/Sun is LAST week's completed prints.
  "Next week" is the following Monday.

Earnings /calendar/earnings?dateFrom=<DAY>
  dateFrom is a SINGLE DAY, not a week. Monday-only fetches miss Tue-Fri.
  route-init-data.data.entries = {items, page, pageSize, totalItemsCount,
  totalPages}. pageSize 50.

Earnings /calendar/earnings/season-preview
  entries[] ~209 names, totalCount ~241. Busy days are truncated
  (25-row cap). totalsPerDay says how many the day page still has.
  Always walk weekday dateFrom pages for truncated / missing days.

Futures /futures.ashx
  `var tiles = {...};` — 49 roots. Nested `{}` inside each tile, so a
  non-greedy brace regex is the wrong tool; brace-match the object.
  Brent is QA (not BZ). Bitcoin is BTC (not BT).
"""
from __future__ import annotations

import json
import re
from datetime import datetime, timedelta
from typing import Any

import requests

from . import finviz_session

CALENDAR_LOOKAHEAD_DAYS = 16
EARNINGS_MCAP_LIST = 10_000.0
EARNINGS_MCAP_MEGA = 50_000.0
EARNINGS_LIST_CAP = 40
ECON_LIST_CAP = 40

FUTURES_KEEP = [
    ("ES", "S&P 500"), ("NQ", "Nasdaq 100"), ("ER2", "Russell 2000"),
    ("YM", "DJIA"), ("VX", "VIX futures"),
    ("CL", "Crude WTI"), ("QA", "Brent"), ("NG", "Nat gas"),
    ("HO", "Heating oil"), ("RB", "RBOB"),
    ("GC", "Gold"), ("SI", "Silver"), ("HG", "Copper"),
    ("PL", "Platinum"), ("PA", "Palladium"),
    ("DX", "USD"), ("6E", "EUR"), ("6J", "JPY"), ("6B", "GBP"),
    ("6A", "AUD"), ("6C", "CAD"), ("6S", "CHF"), ("6N", "NZD"),
    ("ZN", "10Y note"), ("ZF", "5Y note"), ("ZT", "2Y note"), ("ZB", "30Y bond"),
    ("NKD", "Nikkei"), ("DY", "DAX"), ("EX", "Euro Stoxx"),
    ("ZC", "Corn"), ("ZS", "Soybeans"), ("ZW", "Wheat"),
    ("ZL", "Soy oil"), ("ZM", "Soy meal"), ("ZO", "Oats"), ("ZR", "Rice"),
    ("KC", "Coffee"), ("SB", "Sugar"), ("CT", "Cotton"), ("CC", "Cocoa"),
    ("JO", "Orange juice"), ("LB", "Lumber"),
    ("LC", "Live cattle"), ("LH", "Lean hogs"), ("FC", "Feeder cattle"),
    ("BTC", "Bitcoin"),
]
FUTURES_ALIAS = {
    "BZ": "QA", "LCO": "QA", "BT": "BTC", "VI": "VX", "VIX": "VX",
    "LE": "LC", "HE": "LH",
}


def _pct(val):
    if val is None:
        return None
    s = str(val).strip().replace(",", "").replace("%", "")
    if s.lower() in ("", "-", "nan", "none", "null"):
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _numeric_surprise(actual, forecast):
    a, f = _pct(actual), _pct(forecast)
    if a is None or f is None:
        return None
    return round(a - f, 4)


def _monday_on_or_before(day):
    return day - timedelta(days=day.weekday())


def _calendar_week_starts(asof, lookahead=CALENDAR_LOOKAHEAD_DAYS):
    start = datetime.fromisoformat(asof[:10])
    first = _monday_on_or_before(start)
    last = start + timedelta(days=lookahead)
    out = []
    cur = first
    while cur <= last:
        out.append(cur.date().isoformat())
        cur += timedelta(days=7)
    return out


def _calendar_days(asof, lookahead=CALENDAR_LOOKAHEAD_DAYS):
    start = datetime.fromisoformat(asof[:10]).date()
    return [(start + timedelta(days=i)).isoformat() for i in range(lookahead + 1)]


def _route_init_data(html):
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
    data = blob.get("data")
    return data if isinstance(data, dict) else blob if isinstance(blob, dict) else {}


def _walk_dicts(obj):
    if isinstance(obj, dict):
        yield obj
        for v in obj.values():
            yield from _walk_dicts(v)
    elif isinstance(obj, list):
        for v in obj:
            yield from _walk_dicts(v)


def _event_day(row):
    return str(row.get("datetime") or row.get("date") or row.get("earningsDate") or "")[:10]


def _in_window(day, asof, lookahead=CALENDAR_LOOKAHEAD_DAYS):
    if not day:
        return False
    try:
        d = datetime.fromisoformat(day).date()
        start = datetime.fromisoformat(asof[:10]).date()
    except ValueError:
        return day >= asof
    return start <= d <= start + timedelta(days=lookahead)


def _clean_cal_val(val):
    if val is None:
        return None
    if isinstance(val, str):
        s = val.strip()
        if s.lower() in ("", "-", "null", "none", "nan"):
            return None
        return s
    return val


def _econ_pool(data, html):
    pool = []
    entries = data.get("entries") if isinstance(data, dict) else None
    if isinstance(entries, list):
        pool = [e for e in entries if isinstance(e, dict) and e.get("event")]
    elif isinstance(entries, dict):
        items = entries.get("items")
        if isinstance(items, list):
            pool = [e for e in items if isinstance(e, dict) and e.get("event")]
        else:
            pool = [e for e in _walk_dicts(entries) if e.get("event") and e.get("date")]
    if pool:
        return pool
    for m in re.finditer(
        r'{"calendarId":(\d+),.*?"event":"(.*?)".*?"date":"(.*?)".*?'
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
    return pool


def parse_econ_html(html, asof):
    rows = []
    data = _route_init_data(html)
    for raw in _econ_pool(data, html):
        when = str(raw.get("date") or raw.get("datetime") or "")
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
            "ticker": raw.get("ticker") or "",
        })
    return rows


def fetch_econ(sess, date):
    rows = []
    paths = ["/calendar/economic", "/calendar.ashx"]
    for week in _calendar_week_starts(date):
        paths.append(f"/calendar/economic?dateFrom={week}")
        paths.append(f"/calendar.ashx?d={week}")
    tried = set()
    for path in paths:
        if path in tried:
            continue
        tried.add(path)
        r = finviz_session.get(sess, [path])
        if r is None:
            print(f"[map_heat] econ {path} failed: Elite session empty/403")
            continue
        got = parse_econ_html(r.text, date)
        print(f"[map_heat] econ {path}: {len(got)}")
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


def _earnings_session(when):
    if "T16" in when or "T20" in when or "T21" in when or "T22" in when:
        return "AMC"
    return "BMO"


def _earnings_pool(data, html):
    pool = []
    entries = data.get("entries") if isinstance(data, dict) else None
    if isinstance(entries, list):
        pool = [e for e in entries if isinstance(e, dict)]
    elif isinstance(entries, dict) and isinstance(entries.get("items"), list):
        pool = [e for e in entries["items"] if isinstance(e, dict)]
    if not pool and isinstance(data, dict):
        for obj in _walk_dicts(data):
            if obj.get("ticker") and obj.get("earningsDate"):
                pool.append(obj)
    if pool:
        return pool
    for m in re.finditer(
        r'{"earningsDate":"(.*?)".*?"ticker":"(.*?)".*?"company":"(.*?)".*?'
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
    return pool


def parse_earnings_html(html, asof):
    rows = []
    data = _route_init_data(html)
    seen = set()
    for raw in _earnings_pool(data, html):
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


def earnings_preview_day_counts(html):
    data = _route_init_data(html)
    totals = data.get("totalsPerDay") if isinstance(data, dict) else None
    if isinstance(totals, dict):
        out = {}
        for k, v in totals.items():
            try:
                out[str(k)[:10]] = int(v)
            except (TypeError, ValueError):
                continue
        return out
    return {}


def _earnings_total_pages(html):
    data = _route_init_data(html)
    entries = data.get("entries") if isinstance(data, dict) else None
    if isinstance(entries, dict):
        try:
            return max(1, int(entries.get("totalPages") or 1))
        except (TypeError, ValueError):
            return 1
    return 1


def fetch_earnings(sess, date):
    rows = []
    have_by_day = {}
    totals_by_day = {}

    r = finviz_session.get(sess, [
        "/calendar/earnings/season-preview",
        "/calendar/earnings",
    ])
    if r is not None:
        rows = parse_earnings_html(r.text, date)
        totals_by_day = earnings_preview_day_counts(r.text)
        print(f"[map_heat] earnings preview: {len(rows)} (site days={len(totals_by_day)})")
    else:
        print("[map_heat] earnings season-preview failed: Elite session empty/403")

    for row in rows:
        d = _event_day(row)
        have_by_day[d] = have_by_day.get(d, 0) + 1

    for day in _calendar_days(date):
        try:
            wd = datetime.fromisoformat(day).weekday()
        except ValueError:
            wd = 0
        if wd >= 5:
            continue
        have = have_by_day.get(day, 0)
        need = totals_by_day.get(day)
        if need is not None and have >= need and have > 0:
            continue
        if need is None and have >= 8:
            continue
        wr = finviz_session.get(sess, [f"/calendar/earnings?dateFrom={day}"])
        if wr is None:
            continue
        extra = parse_earnings_html(wr.text, date)
        pages = _earnings_total_pages(wr.text)
        for page in range(2, pages + 1):
            pr = finviz_session.get(sess, [f"/calendar/earnings?dateFrom={day}&page={page}"])
            if pr is None:
                break
            extra.extend(parse_earnings_html(pr.text, date))
        print(f"[map_heat] earnings day {day}: +{len(extra)} pages={pages}")
        rows.extend(extra)
        have_by_day[day] = have_by_day.get(day, 0) + len(extra)

    seen = set()
    uniq = []
    for row in rows:
        k = (row["ticker"], _event_day(row))
        if k in seen:
            continue
        seen.add(k)
        uniq.append(row)
    uniq.sort(key=lambda x: (-(x.get("mcap") or 0), x.get("datetime") or ""))
    print(f"[map_heat] earnings {date} window: {len(uniq)}")
    return uniq


def _extract_js_object(html, needle):
    i = (html or "").find(needle)
    if i < 0:
        return ""
    i = html.find("{", i)
    if i < 0:
        return ""
    depth = 0
    in_str = False
    esc = False
    for j, ch in enumerate(html[i:], i):
        if in_str:
            if esc:
                esc = False
            elif ch == "\\":
                esc = True
            elif ch == '"':
                in_str = False
            continue
        if ch == '"':
            in_str = True
        elif ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                return html[i:j + 1]
    return ""


def parse_futures_html(html):
    blob = _extract_js_object(html or "", "var tiles")
    if not blob:
        m = re.search(r"var tiles = (\{.*\})", html or "", re.S)
        blob = m.group(1) if m else ""
    if not blob:
        return {}
    try:
        tiles = json.loads(blob)
    except json.JSONDecodeError:
        try:
            tiles = json.loads(re.sub(r",\s*}", "}", blob))
        except json.JSONDecodeError:
            return {}
    if not isinstance(tiles, dict):
        return {}
    out = {}
    for key, row in tiles.items():
        if not isinstance(row, dict):
            continue
        ticker = str(row.get("ticker") or key).upper()
        out[ticker] = {
            "label": row.get("label") or ticker,
            "last": row.get("last"),
            "change": row.get("change"),
            "prev_close": row.get("prevClose"),
        }
    return out


def tape_from_futures(futures):
    tape = []
    used = set()
    index = {str(k).upper(): row for k, row in futures.items()}
    for alias, root in FUTURES_ALIAS.items():
        if alias in index and root not in index:
            index[root] = index[alias]

    def _add(ticker, label=None):
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


def calendar_fields(econ, earns, asof=None):
    high_econ = [e for e in econ if e.get("importance", 0) >= 2][:ECON_LIST_CAP]
    listed_earn = [
        e for e in earns if (e.get("mcap") or 0) >= EARNINGS_MCAP_LIST
    ][:EARNINGS_LIST_CAP]
    if not listed_earn:
        listed_earn = list(earns[:EARNINGS_LIST_CAP])
    mega_earn = [e for e in earns if (e.get("mcap") or 0) >= EARNINGS_MCAP_MEGA][:20]
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
        "earnings": listed_earn,
        "econ_today": today_high,
        "earnings_today": mega_today,
        "macro_gate": macro_gate,
        "earnings_gate": bool(mega_today),
        "size_gate": macro_gate,
        "calendar_entry_scale": 0.5 if macro_gate else 1.0,
        "earnings_entry_tickers": tickers,
        "calendar_window_days": CALENDAR_LOOKAHEAD_DAYS,
        "econ_window_count": len(econ),
        "earnings_window_count": len(earns),
    }
