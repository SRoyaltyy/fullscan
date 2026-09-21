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

from .hygiene import (
    collapse_macro_stories,
    entry_clock_of,
    horizon_note,
    skips_01d_horizon,
)

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

# Scoreboard grade cut: 0-1d / 1-4w hit rates count a row only when
# q5=impulse, direction in {up, down}, and the listed ticker/ETF *is*
# the expression (tradeable_expression=direct). factor_impulse and
# theme/sector index proxies (Fed→QQQ/SPY) stay in the markdown as
# context and are tagged ungraded. mixed / not_determined stay ungraded.
# No new score types.
UNGRADED_EVENT_CLASSES = frozenset({"factor_impulse"})
INDEX_FACTOR_KINDS = frozenset({"theme_etf", "sector_etf"})
BLAST_EVENT_CLASSES = frozenset({
    "blast_legal", "blast_ops", "blast_cyber",
    "product_harm", "labor_stop", "cat_weather", "labor_organize",
})
SLICE_LABELS = (
    "market_structure",
    "blast",
    "guidance",
    "print_vs_priced",
    "capacity",
    "demand",
    "input_cost",
    "factor_impulse",
)


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
    """Published when the source provided it; else retrieved. Never invent Published."""
    pub = parse_when(row.get("published_at"))
    if pub is not None:
        return pub
    return parse_when(row.get("retrieved_at")) or parse_when(row.get("known_at"))


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


def row_q5(row: dict | None) -> str:
    if not row:
        return ""
    cls = row.get("classification") or {}
    if cls.get("q5"):
        return str(cls["q5"])
    q5 = row.get("q5")
    if isinstance(q5, dict):
        return str(q5.get("status") or "")
    return str(q5 or "")


def row_event_class(row: dict | None) -> str:
    if not row:
        return ""
    return str((row.get("classification") or {}).get("event_class") or "")


def ungraded_reason(target: dict, row: dict | None = None) -> str | None:
    """Why this tape row is context-only, or None if it is gradeable.

    Grade 0-1d / 1-4w only when q5=impulse, direction in {up, down},
    and tradeable_expression=direct. factor_impulse / index-factor
    proxies and mixed / not_determined never enter the hit rate.
    """
    ev = str(target.get("event_class") or row_event_class(row) or "")
    q5 = str(target.get("q5") or row_q5(row) or "")
    direction = str(target.get("direction") or "")
    expr = str(target.get("tradeable_expression") or "direct")
    kind = str(target.get("kind") or "ticker")
    if ev in UNGRADED_EVENT_CLASSES:
        return "ungraded · factor_impulse"
    if kind in INDEX_FACTOR_KINDS:
        return "ungraded · index-factor"
    if q5 != "impulse":
        return f"ungraded · q5={q5 or '?'}"
    if direction not in {"up", "down"}:
        return f"ungraded · {direction or 'no-direction'}"
    if expr != "direct":
        return f"ungraded · tradeable_expression={expr}"
    return None


def is_gradeable(target: dict, row: dict | None = None) -> bool:
    return ungraded_reason(target, row) is None


def stamp_grade_flags(results: list[dict]) -> list[dict]:
    """Attach graded / ungraded_reason on existing performance rows."""
    for r in results:
        clock = r.get("entry_clock") or entry_clock_of(r)
        r["entry_clock"] = clock
        for g in r.get("performance") or []:
            if not isinstance(g, dict):
                continue
            reason = ungraded_reason(g, r)
            g["graded"] = reason is None
            g["ungraded_reason"] = reason or ""
            g["skip_01d"] = skips_01d_horizon(g, r)
            g["horizon_note"] = horizon_note(g, r)
            g["entry_clock"] = clock
    return results


def slice_label(row: dict | None, target: dict | None = None) -> str | None:
    ev = str((target or {}).get("event_class") or row_event_class(row) or "")
    if ev == "factor_impulse":
        return "factor_impulse"
    if ev in BLAST_EVENT_CLASSES or ev == "blast":
        return "blast"
    if ev in SLICE_LABELS:
        return ev
    return None


def evaluation_targets(row: dict) -> list[dict]:
    """What to pull from the tape: named tickers, else theme/sector ETFs."""
    out: list[dict] = []
    seen: set[str] = set()
    ev = row_event_class(row)
    q5 = row_q5(row)
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
            "tradeable_expression": e.get("tradeable_expression") or "direct",
            "inferred": bool(e.get("inferred")),
            "event_class": ev,
            "q5": q5,
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
                "tradeable_expression": "proxy",
                "inferred": True,
                "event_class": ev,
                "q5": q5,
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
                "tradeable_expression": "proxy",
                "inferred": True,
                "event_class": ev,
                "q5": q5,
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
    reason = ungraded_reason(target)
    base = {
        "kind": target.get("kind") or "ticker",
        "ticker": tick,
        "name": target.get("name") or tick,
        "role": target.get("role") or "named",
        "direction": direction,
        "horizon": horizon,
        "tradeable_expression": target.get("tradeable_expression") or "direct",
        "inferred": bool(target.get("inferred")),
        "event_class": target.get("event_class") or "",
        "q5": target.get("q5") or "",
        "graded": reason is None,
        "ungraded_reason": reason or "",
        "skip_01d": skips_01d_horizon(target),
        "horizon_note": horizon_note(target),
        "entry_clock": "",
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
    clock = entry_clock_of(row)
    out = []
    for t in evaluation_targets(row):
        g = grade_one(t, book.get(t["ticker"]) or [], when)
        g["entry_clock"] = clock
        g["skip_01d"] = skips_01d_horizon(t, row)
        g["horizon_note"] = horizon_note(t, row)
        out.append(g)
    return out


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


def _empty_slice(graded: bool) -> dict[str, Any]:
    return {
        "n_1d": 0, "hit_1d": 0, "hit_rate_1d": None,
        "n_20d": 0, "hit_20d": 0, "hit_rate_20d": None,
        "graded": graded,
    }


def _rate(h: int, n: int) -> float | None:
    return round(h / n, 4) if n else None


def _add_hits(bag: dict[str, Any], g: dict, row: dict | None = None) -> None:
    skip_01d = g.get("skip_01d")
    if skip_01d is None:
        skip_01d = skips_01d_horizon(g, row)
    if g.get("ret_1d") is not None and not skip_01d:
        bag["n_1d"] += 1
        bag["hit_1d"] += int(g.get("agree_1d") is True)
    if g.get("ret_20d") is not None:
        bag["n_20d"] += 1
        bag["hit_20d"] += int(g.get("agree_20d") is True)


def performance_rollup(results: list[dict]) -> dict[str, Any]:
    """Hit rates for gradeable rows only. Slice table keeps factor_impulse as context."""
    stamp_grade_flags(results)
    n1 = hit1 = n20 = hit20 = 0
    missing = 0
    graded = 0
    ungraded = 0
    by_tick: dict[str, list[float]] = {}
    slices = {
        label: _empty_slice(graded=(label != "factor_impulse"))
        for label in SLICE_LABELS
    }
    for r in results:
        if not r.get("usable"):
            continue
        for g in r.get("performance") or []:
            if not isinstance(g, dict):
                continue
            reason = g.get("ungraded_reason") or ungraded_reason(g, r)
            gradeable = reason is None
            sl = slice_label(r, g)
            # Slice comparison: graded slices use the grade cut;
            # factor_impulse shows directional-as-if hits, tagged ungraded.
            if sl == "factor_impulse":
                if g.get("direction") in {"up", "down"}:
                    _add_hits(slices[sl], g, r)
            elif sl and gradeable:
                _add_hits(slices[sl], g, r)
            if not gradeable:
                ungraded += 1
                continue
            graded += 1
            skip_01d = g.get("skip_01d")
            if skip_01d is None:
                skip_01d = skips_01d_horizon(g, r)
            if g.get("ret_1d") is None and g.get("ret_20d") is None:
                missing += 1
            if g.get("ret_1d") is not None and not skip_01d:
                n1 += 1
                hit1 += int(g.get("agree_1d") is True)
                by_tick.setdefault(g.get("ticker") or "?", []).append(float(g["ret_1d"]))
            if g.get("ret_20d") is not None:
                n20 += 1
                hit20 += int(g.get("agree_20d") is True)
    for bag in slices.values():
        bag["hit_rate_1d"] = _rate(bag["hit_1d"], bag["n_1d"])
        bag["hit_rate_20d"] = _rate(bag["hit_20d"], bag["n_20d"])
    macro = collapse_macro_stories(results)
    # Collapsed unique legs only — reprints do not inflate the ungraded slice.
    slices["factor_impulse"] = {
        "n_1d": macro["leg_n_1d"],
        "hit_1d": macro["leg_hit_1d"],
        "hit_rate_1d": macro["leg_hit_rate_1d"],
        "n_20d": macro["leg_n_20d"],
        "hit_20d": macro["leg_hit_20d"],
        "hit_rate_20d": macro["leg_hit_rate_20d"],
        "graded": False,
    }
    avg = {
        t: round(sum(v) / len(v), 2)
        for t, v in sorted(by_tick.items(), key=lambda kv: -len(kv[1]))[:20]
    }
    return {
        "directional_calls": graded,
        "ungraded_context": ungraded,
        "n_1d": n1,
        "hit_1d": hit1,
        "hit_rate_1d": _rate(hit1, n1),
        "n_20d": n20,
        "hit_20d": hit20,
        "hit_rate_20d": _rate(hit20, n20),
        "missing_tape": missing,
        "avg_ret_1d_by_ticker": avg,
        "slices": slices,
        "grade_rule": (
            "0-1d / 1-4w graded only when q5=impulse, direction in "
            "{up, down}, tradeable_expression=direct. factor_impulse "
            "and mixed/not_determined are ungraded context. "
            "gate / capacity / blast_cyber / CHIPS awards skip 0-1d. "
            "macro headline basket is a separate column (not in these rates)."
        ),
        "macro_headline": macro,
    }


def format_slice_table(tape: dict | None) -> list[str]:
    """Markdown slice table: named classes vs ungraded factor_impulse."""
    slices = (tape or {}).get("slices") or {}
    lines = [
        "| slice | 0-1d hits | 0-1d n | 0-1d rate | 1-4w hits | 1-4w n | 1-4w rate | graded |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |",
    ]
    for label in SLICE_LABELS:
        bag = slices.get(label) or _empty_slice(graded=(label != "factor_impulse"))
        graded = bag.get("graded")
        tag = "yes" if graded else "**ungraded**"
        def cell(key, fallback="—"):
            v = bag.get(key)
            return fallback if v is None else v
        lines.append(
            f"| {label} | {cell('hit_1d', 0)} | {cell('n_1d', 0)} | "
            f"{cell('hit_rate_1d')} | {cell('hit_20d', 0)} | "
            f"{cell('n_20d', 0)} | {cell('hit_rate_20d')} | {tag} |"
        )
    return lines


def format_performance(rows: list[dict] | None, row: dict | None = None) -> str:
    if not rows:
        return "n/a — nothing to grade"
    bits = []
    for g in rows:
        tick = g.get("ticker") or g.get("name") or "?"
        d = g.get("direction") or "?"
        reason = (
            g.get("ungraded_reason")
            if "graded" in g
            else ungraded_reason(g, row)
        )
        if g.get("note") and g.get("ret_1d") is None:
            tag = f" · **{reason}**" if reason else ""
            bits.append(f"{tick} ({d}): {g['note']}{tag}")
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
        hz = g.get("horizon_note") or horizon_note(g, row)
        if hz:
            extra = f"{extra} · {hz}"
        clock = g.get("entry_clock") or (row or {}).get("entry_clock") or ""
        if clock:
            extra = f"{extra} · entry_clock={clock}"
        skip_01d = g.get("skip_01d")
        if skip_01d is None:
            skip_01d = skips_01d_horizon(g, row)
        if reason:
            # Tape stays as context; do not invent a directional score.
            bits.append(
                f"{tick} {d} · 0-1d {r1s} · 1-4w {r20s} "
                f"entry {g.get('entry_date') or '?'} · **{reason}**{extra}"
            )
            continue
        if skip_01d:
            bits.append(
                f"{tick} {d} · 0-1d {r1s} (skipped) · "
                f"1-4w {r20s} ({mark(g.get('agree_20d'))}) "
                f"entry {g.get('entry_date') or '?'}{extra}"
            )
            continue
        bits.append(
            f"{tick} {d} · 0-1d {r1s} ({mark(g.get('agree_1d'))}) · "
            f"1-4w {r20s} ({mark(g.get('agree_20d'))}) "
            f"entry {g.get('entry_date') or '?'}{extra}"
        )
    return "<br>".join(bits)
