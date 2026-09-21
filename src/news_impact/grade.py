"""As-of tape grades for news-impact entities (ticker / theme ETF / market).

Entry = open of the first regular session at/after the article's published
time (retrieved time if published is missing). Published at/after 09:30 ET
enters the *next* session — same rule as news_actions grades.

Does not touch flatten / Webull / factor-mine.
"""
from __future__ import annotations

import socket
from datetime import datetime, time as dtime, timedelta, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")
MARKET_OPEN = dtime(9, 30)
STORE = Path("data/prices/ohlc.parquet")

# Theme / sector → listed expression when the router named a theme, not a ticker.
THEME_TO_ETF = {
    "fed_path": "SPY",
    "inflation": "TIP",
    "rates": "TLT",
    "energy": "XLE",
    "oil": "XLE",
    "ai": "QQQ",
    "technology": "XLK",
    "tech": "XLK",
    "financials": "XLF",
    "financial": "XLF",
    "healthcare": "XLV",
    "health": "XLV",
    "consumer": "XLY",
    "consumer cyclical": "XLY",
    "consumer defensive": "XLP",
    "staples": "XLP",
    "industrials": "XLI",
    "materials": "XLB",
    "basic materials": "XLB",
    "utilities": "XLU",
    "real estate": "XLRE",
    "communications": "XLC",
    "communication services": "XLC",
    "gold": "GLD",
    "crypto": "IBIT",
    "bitcoin": "IBIT",
    "market": "SPY",
}

SECTOR_TO_ETF = {
    "Energy": "XLE",
    "Technology": "XLK",
    "Financial": "XLF",
    "Financials": "XLF",
    "Healthcare": "XLV",
    "Basic Materials": "XLB",
    "Consumer Cyclical": "XLY",
    "Consumer Defensive": "XLP",
    "Industrials": "XLI",
    "Utilities": "XLU",
    "Real Estate": "XLRE",
    "Communication Services": "XLC",
}

HORIZON_BARS = {
    "0-1d": 1,
    "1-4w": 20,
    "1-6m": 63,
    "6m+": 126,
}


def parse_when(raw: str | None) -> datetime | None:
    """ISO / RFC 2822 / YYYY-MM-DD → aware ET datetime, or None."""
    s = str(raw or "").strip()
    if not s:
        return None
    try:
        dt = parsedate_to_datetime(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(ET)
    except (TypeError, ValueError, OverflowError):
        pass
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=ET)
        return dt.astimezone(ET)
    except (TypeError, ValueError):
        pass
    if len(s) >= 10 and s[4:5] == "-" and s[7:8] == "-":
        try:
            return datetime.strptime(s[:10], "%Y-%m-%d").replace(tzinfo=ET)
        except ValueError:
            return None
    return None


def signal_dt(row: dict) -> datetime | None:
    return parse_when(row.get("published_at")) or parse_when(row.get("retrieved_at")) or parse_when(row.get("known_at"))


def entry_calendar_date(dt: datetime) -> str:
    """First calendar day whose regular session is still tradable after the signal."""
    local = dt.astimezone(ET)
    day = local.date()
    if local.timetz().replace(tzinfo=None) >= MARKET_OPEN:
        day = day + timedelta(days=1)
    return day.isoformat()


def _agree(direction: str, ret_pct: float | None) -> bool | None:
    if ret_pct is None or direction not in {"up", "down"}:
        return None
    if direction == "up":
        return ret_pct > 0
    return ret_pct < 0


def evaluation_targets(row: dict) -> list[dict]:
    """What to pull from the tape: named tickers, else theme/sector ETFs."""
    out: list[dict] = []
    seen: set[str] = set()
    for e in row.get("entities") or []:
        if not isinstance(e, dict):
            continue
        tick = str(e.get("ticker") or "").strip().upper()
        if not tick or tick in seen:
            continue
        seen.add(tick)
        out.append({
            "kind": "ticker",
            "ticker": tick,
            "name": e.get("name") or tick,
            "role": e.get("role") or "named",
            "direction": e.get("direction") or "not_determined",
            "horizon": e.get("horizon") or "0-1d",
        })
    if out:
        return out
    for theme in row.get("macro_themes") or []:
        etf = THEME_TO_ETF.get(str(theme).strip().lower())
        if etf and etf not in seen:
            seen.add(etf)
            out.append({
                "kind": "theme_etf",
                "ticker": etf,
                "name": str(theme),
                "role": "theme",
                "direction": "not_determined",
                "horizon": "0-1d",
            })
    for sec in row.get("sectors") or []:
        etf = SECTOR_TO_ETF.get(str(sec).strip()) or THEME_TO_ETF.get(str(sec).strip().lower())
        if etf and etf not in seen:
            seen.add(etf)
            out.append({
                "kind": "sector_etf",
                "ticker": etf,
                "name": str(sec),
                "role": "theme",
                "direction": "not_determined",
                "horizon": "0-1d",
            })
    return out


def _bars_from_parquet(tickers: list[str]) -> dict[str, list[dict]]:
    want = {t.upper() for t in tickers}
    if not want or not STORE.exists():
        return {}
    try:
        import pandas as pd
    except ImportError:
        return {}
    try:
        df = pd.read_parquet(STORE)
    except Exception:
        return {}
    if df is None or df.empty:
        return {}
    df["ticker"] = df["ticker"].astype(str).str.upper()
    df = df[df["ticker"].isin(want)]
    if df.empty:
        return {}
    df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
    out: dict[str, list[dict]] = {t: [] for t in want}
    for rec in df.itertuples(index=False):
        try:
            t = str(rec.ticker)
            o, h, l, c = float(rec.open), float(rec.high), float(rec.low), float(rec.close)
        except (TypeError, ValueError, AttributeError):
            continue
        if any(v != v for v in (o, h, l, c)):
            continue
        out.setdefault(t, []).append({
            "date": str(rec.date), "open": o, "high": h, "low": l, "close": c,
        })
    for t in list(out):
        out[t].sort(key=lambda b: b["date"])
    return {t: rows for t, rows in out.items() if rows}


def _yf_download(tickers: list[str], start: str) -> dict[str, list[dict]]:
    out: dict[str, list[dict]] = {t: [] for t in tickers}
    if not tickers:
        return out
    try:
        import yfinance as yf
    except ImportError:
        print("[news_impact.grade] yfinance missing — tape grades use parquet only")
        return out
    prev = socket.getdefaulttimeout()
    socket.setdefaulttimeout(30)
    try:
        chunk = 40
        for i in range(0, len(tickers), chunk):
            batch = tickers[i:i + chunk]
            try:
                data = yf.download(
                    batch, start=start, interval="1d",
                    group_by="ticker", auto_adjust=False,
                    progress=False, threads=False,
                )
            except Exception as exc:  # noqa: BLE001
                print(f"[news_impact.grade] yfinance batch failed: {exc}")
                continue
            if data is None or getattr(data, "empty", True):
                continue
            single = len(batch) == 1
            for t in batch:
                try:
                    df = data if single else data[t]
                except Exception:
                    continue
                rows = []
                for idx, r in df.iterrows():
                    try:
                        o, h, l, c = float(r["Open"]), float(r["High"]), float(r["Low"]), float(r["Close"])
                    except (TypeError, ValueError, KeyError):
                        continue
                    if any(v != v for v in (o, h, l, c)):
                        continue
                    day = idx.date().isoformat() if hasattr(idx, "date") else str(idx)[:10]
                    rows.append({"date": day, "open": o, "high": h, "low": l, "close": c})
                if rows:
                    out[t] = rows
    finally:
        socket.setdefaulttimeout(prev)
    return out


def _merge_bars(base: dict[str, list[dict]], extra: dict[str, list[dict]]) -> dict[str, list[dict]]:
    out = {t: list(rows) for t, rows in base.items()}
    for t, rows in extra.items():
        bag = {b["date"]: b for b in out.get(t) or []}
        for b in rows:
            bag[b["date"]] = b
        out[t] = [bag[d] for d in sorted(bag)]
    return out


def load_book(
    tickers: list[str],
    start: str,
    fetch: bool = True,
) -> dict[str, list[dict]]:
    names = sorted({str(t).upper() for t in tickers if t})
    book = _bars_from_parquet(names)
    if not fetch:
        return book
    need = []
    for t in names:
        rows = book.get(t) or []
        if not rows:
            need.append(t)
            continue
        last = rows[-1]["date"]
        # Parquet in this repo currently ends 2026-09-11; fill the gap.
        if last < datetime.now(ET).date().isoformat():
            need.append(t)
    if need:
        print(f"[news_impact.grade] yfinance fill {len(need)} tickers from {start}")
        book = _merge_bars(book, _yf_download(need, start))
    return book


def grade_one(
    target: dict,
    bars: list[dict],
    when: datetime | None,
) -> dict[str, Any]:
    """Grade one ticker/theme against daily bars. Safe with empty tape."""
    tick = str(target.get("ticker") or "")
    direction = str(target.get("direction") or "not_determined")
    horizon = str(target.get("horizon") or "0-1d")
    base = {
        "kind": target.get("kind") or "ticker",
        "ticker": tick,
        "name": target.get("name") or tick,
        "role": target.get("role") or "named",
        "direction": direction,
        "horizon": horizon,
        "entry_date": None,
        "entry_open": None,
        "through": None,
        "ret_1d": None,
        "ret_5d": None,
        "ret_20d": None,
        "ret_horizon": None,
        "agree_1d": None,
        "agree_20d": None,
        "agree_horizon": None,
        "note": "",
    }
    if not tick:
        base["note"] = "no listed expression"
        return base
    if when is None:
        base["note"] = "no published/retrieved time"
        return base
    if not bars:
        base["note"] = "no tape"
        return base
    want = entry_calendar_date(when)
    entry_idx = None
    for i, b in enumerate(bars):
        if b["date"] >= want:
            entry_idx = i
            break
    if entry_idx is None:
        base["note"] = f"no session on/after {want} (tape through {bars[-1]['date']})"
        return base
    entry = bars[entry_idx]
    px = float(entry["open"])
    if px <= 0:
        base["note"] = "bad entry open"
        return base
    base["entry_date"] = entry["date"]
    base["entry_open"] = round(px, 4)
    base["through"] = bars[-1]["date"]

    def ret_at(offset: int) -> float | None:
        j = entry_idx + offset
        if j >= len(bars):
            return None
        close = float(bars[j]["close"])
        if close <= 0:
            return None
        return round((close / px - 1.0) * 100.0, 2)

    # 0-1d = same-session close vs entry open (intraday reaction).
    same = bars[entry_idx]
    base["ret_1d"] = round((float(same["close"]) / px - 1.0) * 100.0, 2)
    base["ret_5d"] = ret_at(5)
    base["ret_20d"] = ret_at(20)
    h_off = HORIZON_BARS.get(horizon, 1)
    if horizon == "0-1d":
        base["ret_horizon"] = base["ret_1d"]
    else:
        base["ret_horizon"] = ret_at(h_off)
    base["agree_1d"] = _agree(direction, base["ret_1d"])
    base["agree_20d"] = _agree(direction, base["ret_20d"])
    base["agree_horizon"] = _agree(direction, base["ret_horizon"])
    missing = []
    if base["ret_1d"] is None:
        missing.append("0-1d")
    if horizon != "0-1d" and base["ret_horizon"] is None:
        missing.append(horizon)
    if missing:
        base["note"] = f"window open ({', '.join(missing)} not elapsed; through {base['through']})"
    return base


def grade_row(row: dict, book: dict[str, list[dict]]) -> list[dict]:
    when = signal_dt(row)
    return [
        grade_one(t, book.get(t["ticker"]) or [], when)
        for t in evaluation_targets(row)
    ]


def grade_results(results: list[dict], fetch: bool = True) -> list[dict]:
    tickers: list[str] = []
    when_min: datetime | None = None
    for r in results:
        for t in evaluation_targets(r):
            tickers.append(t["ticker"])
        dt = signal_dt(r)
        if dt and (when_min is None or dt < when_min):
            when_min = dt
    start = (when_min - timedelta(days=7)).date().isoformat() if when_min else (
        datetime.now(ET) - timedelta(days=80)
    ).date().isoformat()
    book = load_book(tickers, start, fetch=fetch)
    out = []
    for r in results:
        row = dict(r)
        row["performance"] = grade_row(row, book)
        out.append(row)
    return out


def performance_rollup(results: list[dict]) -> dict[str, Any]:
    n1 = hit1 = n20 = hit20 = 0
    missing = 0
    graded = 0
    by_tick: dict[str, list[float]] = {}
    for r in results:
        if not r.get("usable"):
            continue
        rows = r.get("performance") or []
        if not rows:
            missing += 1
            continue
        for g in rows:
            if g.get("direction") not in {"up", "down"}:
                continue
            graded += 1
            if g.get("ret_1d") is None:
                missing += 1
            else:
                n1 += 1
                hit1 += int(g.get("agree_1d") is True)
                by_tick.setdefault(g["ticker"], []).append(float(g["ret_1d"]))
            if g.get("ret_20d") is not None:
                n20 += 1
                hit20 += int(g.get("agree_20d") is True)
    def rate(h, n):
        return round(h / n, 4) if n else None
    avg = {
        t: round(sum(v) / len(v), 2)
        for t, v in sorted(by_tick.items(), key=lambda kv: -len(kv[1]))[:20]
    }
    return {
        "directional_calls": graded,
        "n_1d": n1,
        "hit_1d": hit1,
        "hit_rate_1d": rate(hit1, n1),
        "n_20d": n20,
        "hit_20d": hit20,
        "hit_rate_20d": rate(hit20, n20),
        "missing_tape": missing,
        "avg_ret_1d_by_ticker": avg,
    }


def format_performance(rows: list[dict] | None) -> str:
    if not rows:
        return "n/a — nothing to grade"
    bits = []
    for g in rows:
        tick = g.get("ticker") or g.get("name") or "?"
        d = g.get("direction") or "?"
        if g.get("note") and g.get("ret_1d") is None:
            bits.append(f"{tick} ({d}): {g['note']}")
            continue
        def mark(agree):
            if agree is True:
                return "agree"
            if agree is False:
                return "disagree"
            return "n/a"
        r1 = g.get("ret_1d")
        r20 = g.get("ret_20d")
        r1s = f"{r1:+.2f}%" if r1 is not None else "n/a"
        r20s = f"{r20:+.2f}%" if r20 is not None else "n/a"
        extra = f" · {g['note']}" if g.get("note") else ""
        bits.append(
            f"{tick} {d} · 0-1d {r1s} ({mark(g.get('agree_1d'))}) · "
            f"1-4w {r20s} ({mark(g.get('agree_20d'))}) "
            f"entry {g.get('entry_date') or '?'}{extra}"
        )
    return "<br>".join(bits)
