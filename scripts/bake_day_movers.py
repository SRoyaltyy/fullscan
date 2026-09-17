#!/usr/bin/env python3
"""Bake dashboard/day-movers/days.json from investigator ticker histories."""
from __future__ import annotations

import json
import math
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "dashboard" / "hard-red-exceptions"
OUT_DIR = ROOT / "dashboard" / "day-movers"
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


def main() -> None:
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
            opn = _f(r.get("open"))
            close = _f(r.get("close"))
            intra = ((close - opn) / opn * 100.0) if opn and close and opn > 0 else None
            inter = ((opn - prev_close) / prev_close * 100.0) if prev_close and opn and prev_close > 0 else None
            s = r.get("s")
            by_date[date].append({
                "t": ticker,
                "o": None if opn is None else round(opn, 4),
                "c": None if close is None else round(close, 4),
                "pc": None if prev_close is None else round(prev_close, 4),
                "intra": None if intra is None else round(intra, 2),
                "inter": None if inter is None else round(inter, 2),
                "s": s,
                "hr": bool(r.get("hard_red")),
                "sit": bool(r.get("hard_red")) or (isinstance(s, (int, float)) and s <= -3),
                "np": r.get("n_pos"),
                "nn": r.get("n_neg"),
                "cams": r.get("cams"),
                "idio": r.get("idio"),
                "h1": r.get("h1"),
                "h3": r.get("h3"),
                "h5": r.get("h5"),
                "on": bool(r.get("on_list")),
                "recon": bool(r.get("reconstructed")),
            })
            if close and close > 0:
                prev_close = close
    dates = sorted(by_date)
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    (OUT_DIR / "d").mkdir(exist_ok=True)
    for date in dates:
        rows = by_date[date]
        day = {
            "n": len(rows),
            "s": next((x["s"] for x in rows if x.get("s") is not None), None),
            "gainers": {
                "intraday": top_n(rows, "intra", True),
                "interday": top_n(rows, "inter", True),
            },
            "losers": {
                "intraday": top_n(rows, "intra", False),
                "interday": top_n(rows, "inter", False),
            },
        }
        (OUT_DIR / "d" / f"{date}.json").write_text(json.dumps(day, separators=(",", ":")))
    payload = {
        "generated_from": "dashboard/hard-red-exceptions/t",
        "from_date": dates[0] if dates else None,
        "to_date": dates[-1] if dates else None,
        "n_days": len(dates),
        "n_tickers_scanned": n_files,
        "top_n": TOP_N,
        "clocks": {
            "interday": "prior close → today’s 09:30 open (legal at the bell)",
            "intraday": "today’s 09:30 open → 16:00 close (autopsy; not knowable at 09:30)",
        },
        "dates": dates,
    }
    (OUT_DIR / "index.json").write_text(json.dumps(payload))
    print(f"wrote {len(dates)} day files {payload['from_date']}→{payload['to_date']}")


if __name__ == "__main__":
    main()
