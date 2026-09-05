"""Leak-free morning watchlist for would-be top gainers.

Flatten's 3d size book almost never holds the liquid rip (≈0.5% of
top-25 gainers). This module builds a **capture** list from information
knowable before 09:30:

  * prior session's liquid top gainers / top |movers|
  * earnings reaction: yesterday AMC or today BMO, from the **prior**
    Finviz earnings calendar (not today's tape)
  * this morning's priced mover BUY calls
  * flatten_robust would-buy / wish-list
  * specialized R:G + candlestick flags on those names (prior bars)

Same-day Change% is never an input. The list is a watchlist — it does
not change live flatten_robust fills.
"""
from __future__ import annotations

from . import candle_factor as cf
from . import gainer_asof as ga

TOP_YDAY_GAINERS = 50
TOP_YDAY_MOVERS = 40


def _tick(v) -> str:
    return str(v or "").strip().upper()


def prior_session(cal: list[str], date: str) -> str | None:
    if date in cal:
        i = cal.index(date)
        return cal[i - 1] if i else None
    earlier = [d for d in cal if d < date]
    return earlier[-1] if earlier else None


def parse_earnings(raw) -> tuple[str | None, int | None]:
    """Finviz Earnings Date → (YYYY-MM-DD, HHMM). Same stamps as the chart E."""
    from . import finviz_events as fe
    return fe.parse_finviz_datetime(raw)


def earnings_reaction(prior_date: str | None, session_date: str,
                      df=None) -> list[str]:
    """Names whose last-known earnings print is yday AMC or today BMO."""
    if not prior_date:
        return []
    frame = ga.load_finviz(prior_date) if df is None else df
    if frame is None or getattr(frame, "empty", True) or "Ticker" not in frame.columns:
        return []
    col = "Earnings Date" if "Earnings Date" in frame.columns else None
    if not col:
        return []
    liquid = {
        _tick(r.get("ticker"))
        for r in ga._liquid_tape(
            frame, top_n=0, min_change=0.0, liquid=True,
            min_mcap_m=None, side="up", skip_change=True,
        )
        if _tick(r.get("ticker"))
    }
    out: list[str] = []
    seen: set[str] = set()
    for rec in frame.to_dict("records"):
        t = _tick(rec.get("Ticker"))
        if not t or t in seen or (liquid and t not in liquid):
            continue
        ed, hm = parse_earnings(rec.get(col))
        react = False
        if ed == session_date and (hm is None or hm <= 930):
            react = True
        if ed == prior_date and (hm is None or hm >= 1600):
            react = True
        if not react:
            continue
        seen.add(t)
        out.append(t)
    return out


def yesterday_gainers(prior_date: str | None, top_n: int = TOP_YDAY_GAINERS) -> list[str]:
    if not prior_date:
        return []
    rows = ga.liquid_gainers(
        ga.load_finviz(prior_date), top_n=top_n, min_change=0.0, liquid=True,
    )
    return [_tick(r.get("ticker")) for r in rows if _tick(r.get("ticker"))]


def yesterday_movers(prior_date: str | None, top_n: int = TOP_YDAY_MOVERS) -> list[str]:
    if not prior_date:
        return []
    rows = ga.liquid_movers(ga.load_finviz(prior_date), top_n=top_n, liquid=True)
    return [_tick(r.get("ticker")) for r in rows if _tick(r.get("ticker"))]


def watchlist(date: str, *,
              cal: list[str] | None = None,
              flatten_picks: list[str] | None = None,
              mover_buys: list[str] | None = None,
              top_gainers: int = TOP_YDAY_GAINERS,
              top_movers: int = TOP_YDAY_MOVERS) -> dict:
    """Ordered unique capture names + per-name reasons. Leak-free."""
    session = str(date or "")[:10]
    calendar = list(cal or [])
    prior = prior_session(calendar, session) if calendar else None
    buckets = {
        "flatten": [_tick(t) for t in (flatten_picks or []) if _tick(t)],
        "mover_buys": [_tick(t) for t in (mover_buys or []) if _tick(t)],
        "earn_react": earnings_reaction(prior, session),
        "yday_gainers": yesterday_gainers(prior, top_n=top_gainers),
        "yday_movers": yesterday_movers(prior, top_n=top_movers),
    }
    reasons: dict[str, list[str]] = {}
    order: list[str] = []
    for key in ("flatten", "mover_buys", "earn_react", "yday_gainers", "yday_movers"):
        for t in buckets[key]:
            reasons.setdefault(t, [])
            if key not in reasons[t]:
                reasons[t].append(key)
            if t not in order:
                order.append(t)
    rows = []
    for t in order:
        feat = cf.features(t, session)
        rows.append({
            "ticker": t,
            "date": session,
            "prior_date": prior,
            "reasons": reasons[t],
            "candle_capture": bool(cf.capture(feat)),
            "candle_score": feat.get("score"),
            "candle_pattern": feat.get("last_green") and "green" or feat.get("last_red") and "red" or "—",
            "candle_body_rg": feat.get("body_rg"),
            "candle_vol_rg": feat.get("vol_rg"),
        })
    return {
        "date": session,
        "prior_date": prior,
        "tickers": order,
        "reasons": reasons,
        "rows": rows,
        "n": len(order),
        "n_flatten": len(buckets["flatten"]),
        "n_mover_buys": len(buckets["mover_buys"]),
        "n_earn_react": len(buckets["earn_react"]),
        "n_yday_gainers": len(buckets["yday_gainers"]),
        "n_yday_movers": len(buckets["yday_movers"]),
        "n_candle": sum(1 for r in rows if r["candle_capture"]),
    }


def collect_capture(from_date: str, to_date: str | None,
                    session_dates: list[str],
                    flatten_by_date: dict,
                    mover_by_date: dict,
                    top_gainers: int = TOP_YDAY_GAINERS,
                    top_movers: int = TOP_YDAY_MOVERS) -> dict:
    dates = [d for d in session_dates
             if d >= from_date and (not to_date or d <= to_date)]
    by_date: dict[str, dict] = {}
    keys: set[tuple[str, str]] = set()
    names: set[str] = set()
    for date in dates:
        plan = flatten_by_date.get(date) or {}
        wl = watchlist(
            date, cal=session_dates,
            flatten_picks=plan.get("tickers") or [],
            mover_buys=mover_by_date.get(date) or [],
            top_gainers=top_gainers, top_movers=top_movers,
        )
        by_date[date] = wl
        for t in wl["tickers"]:
            names.add(t)
            keys.add((date, t))
    return {
        "from_date": from_date,
        "to_date": to_date or (dates[-1] if dates else from_date),
        "n_sessions": len(dates),
        "n_capture_days": len(keys),
        "n_tickers": len(names),
        "by_date": by_date,
        "keys": keys,
        "tickers": sorted(names),
        "session_dates": dates,
    }
