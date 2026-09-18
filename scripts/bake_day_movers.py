#!/usr/bin/env python3
"""Bake dashboard/day-movers/days.json from investigator histories.

Optional official-bar overlay: dashboard/day-movers/eod_overlay.json
  { "TICKER": { "YYYY-MM-DD": {"o":..,"h":..,"l":..,"c":..} } }
When a date/ticker is present, intra/inter/OHLC are taken from the overlay
so a mid-session investigator print cannot rank the board.
"""
from __future__ import annotations

import json
import math
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "dashboard" / "hard-red-exceptions"
OUT_DIR = ROOT / "dashboard" / "day-movers"
OVERLAY = OUT_DIR / "eod_overlay.json"
TOP_N = 25


def _f(v):
    try:
        if v is None:
            return None
        x = float(v)
        return x if math.isfinite(x) else None
    except (TypeError, ValueError):
        return None


def top_n(rows, key, gainers=True, n=TOP_N):
    pool = [x for x in rows if isinstance(x.get(key), (int, float)) and math.isfinite(x[key])]
    pool.sort(key=lambda x: x[key], reverse=gainers)
    return pool[:n]


def slim(r, clock):
    intra, inter = r.get("intra"), r.get("inter")
    return {
        "t": r["t"],
        "pct": intra if clock == "intraday" else inter,
        "o": r.get("o"),
        "x": r.get("c") if clock == "intraday" else r.get("pc"),
        "c": r.get("c"),
        "pc": r.get("pc"),
        "h": r.get("h"),
        "l": r.get("l"),
        "s": r.get("s"),
        "hr": bool(r.get("hr")),
        "np": r.get("np"),
        "nn": r.get("nn"),
        "idio": r.get("idio"),
        "src": r.get("src") or [],
        "h1": r.get("h1"),
        "h3": r.get("h3"),
        "h5": r.get("h5"),
        "hot4": bool(r.get("hot4")),
        "fl": bool(r.get("fl")),
        "su": r.get("su"),
        "sq": r.get("sq"),
        "k": "oc" if clock == "intraday" else "gap",
        "oc": intra,
        "gap": inter,
        "px_src": r.get("px_src") or "investigator",
    }


def main() -> None:
    overlay = {}
    if OVERLAY.exists():
        overlay = json.loads(OVERLAY.read_text())

    by_date: dict[str, list] = defaultdict(list)
    n_files = 0
    for fp in sorted((SRC / "t").glob("*.json")):
        n_files += 1
        d = json.loads(fp.read_text())
        ticker = d.get("ticker") or fp.stem
        prev_close = None
        for r in d.get("rows") or []:
            date = r.get("date")
            if not date:
                continue
            ov = ((overlay.get(ticker) or {}).get(date) or {})
            opn = _f(ov.get("o") if ov else r.get("open"))
            high = _f(ov.get("h") if ov else r.get("high") or r.get("h"))
            low = _f(ov.get("l") if ov else r.get("low") or r.get("l"))
            close = _f(ov.get("c") if ov else r.get("close"))
            px_src = "yahoo_eod" if ov else "investigator"
            if high is None and opn is not None and close is not None:
                high = max(opn, close)
            if low is None and opn is not None and close is not None:
                low = min(opn, close)
            intra = ((close - opn) / opn * 100.0) if opn and close and opn > 0 else None
            inter = ((opn - prev_close) / prev_close * 100.0) if prev_close and opn and prev_close > 0 else None
            s = r.get("s")
            by_date[date].append({
                "t": ticker,
                "o": None if opn is None else round(opn, 4),
                "h": None if high is None else round(high, 4),
                "l": None if low is None else round(low, 4),
                "c": None if close is None else round(close, 4),
                "pc": None if prev_close is None else round(prev_close, 4),
                "intra": None if intra is None else round(intra, 2),
                "inter": None if inter is None else round(inter, 2),
                "s": s,
                "hr": bool(r.get("hard_red")),
                "np": r.get("n_pos"),
                "nn": r.get("n_neg"),
                "idio": r.get("idio"),
                "src": r.get("sources") or [],
                "h1": r.get("h1"),
                "h3": r.get("h3"),
                "h5": r.get("h5"),
                "hot4": bool(r.get("hot4")),
                "fl": bool(r.get("fl")),
                "su": r.get("su") or r.get("setup"),
                "sq": r.get("sq") or r.get("setup_q"),
                "px_src": px_src,
            })
            if close and close > 0:
                prev_close = close

    dates = sorted(by_date)
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    days = []
    for date in dates:
        rows = by_date[date]
        s = next((x["s"] for x in rows if x.get("s") is not None), None)
        days.append({
            "date": date,
            "s": s,
            "hard_red": bool(s is not None and s <= -3),
            "n_open": len(rows),
            "intraday": {
                "n": len(rows),
                "gainers": [slim(x, "intraday") for x in top_n(rows, "intra", True)],
                "losers": [slim(x, "intraday") for x in top_n(rows, "intra", False)],
            },
            "interday": {
                "n": len(rows),
                "gainers": [slim(x, "interday") for x in top_n(rows, "inter", True)],
                "losers": [slim(x, "interday") for x in top_n(rows, "inter", False)],
            },
        })

    payload = {
        "ok": True,
        "from_date": dates[0] if dates else None,
        "to_date": dates[-1] if dates else None,
        "n_days": len(dates),
        "top_n": TOP_N,
        "dates": dates,
        "days": days,
        "intraday_note": "same-session close — not knowable at 09:30.",
        "clocks": {
            "interday": "prior close → today's open (gap %). Fallback prior close → close if open missing. Legal at 09:30. Never ranked by same-day Change%.",
            "intraday": "same-session close — not knowable at 09:30.",
        },
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "hre_from": dates[0] if dates else None,
        "hre_to": dates[-1] if dates else None,
        "n_tickers": n_files,
        "lag": None,
        "live_sit_untouched": True,
        "generated_from": "dashboard/hard-red-exceptions/t",
        "overlay": str(OVERLAY.relative_to(ROOT)) if overlay else None,
    }
    (OUT_DIR / "days.json").write_text(json.dumps(payload, separators=(",", ":")))
    print(f"wrote days.json {payload['from_date']}→{payload['to_date']} n={n_files}")


if __name__ == "__main__":
    main()
