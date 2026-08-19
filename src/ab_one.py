"""Fast single-ticker AB checklist (+ optional colors / form4 / merge).

Usage:
  python -m src.ab_one AAPL
  python -m src.ab_one AAPL --date 2026-08-18
  python -m src.ab_one AAPL --with-colors --with-form4 --months 18
"""
from __future__ import annotations

import argparse
import sys

import pandas as pd

from . import ab_checklist as ab


def main() -> None:
    ap = argparse.ArgumentParser(description="Score one ticker through AB checklist")
    ap.add_argument("ticker", help="e.g. AAPL")
    ap.add_argument("--date", default=None)
    ap.add_argument("--top", type=int, default=5)
    ap.add_argument("--with-colors", action="store_true")
    ap.add_argument("--with-form4", action="store_true")
    ap.add_argument("--months", type=int, default=18)
    ap.add_argument("--merge", action="store_true", help="Run ab_merge_extras after")
    args = ap.parse_args()
    t = args.ticker.strip().upper()

    # Prefer native --ticker if this ab_checklist build supports it
    try:
        out = ab.run(date=args.date, top=args.top, ticker=t)
    except TypeError:
        # Older signature: monkeypatch liquid filter to one name
        orig_filter = ab._filter_liquid

        def _one(df: pd.DataFrame) -> pd.DataFrame:
            liquid = orig_filter(df)
            hit = liquid[liquid["Ticker"] == t]
            if hit.empty:
                raw = df.copy()
                tcol = "Ticker" if "Ticker" in raw.columns else raw.columns[0]
                raw["Ticker"] = raw[tcol].astype(str).str.strip().str.upper()
                hit = raw[raw["Ticker"] == t].copy()
                if hit.empty:
                    raise SystemExit(f"[ab_one] {t} not in Finviz export")
                print(f"[ab_one] {t} outside liquid gate — scoring anyway")
                if "_mcap" not in hit.columns:
                    hit["_mcap"] = hit["Market Cap"].map(ab._num) * 1e6 if "Market Cap" in hit.columns else 0.0
                    hit["_adv"] = hit["Average Volume"].map(ab._num) * 1e3 if "Average Volume" in hit.columns else 0.0
            else:
                print(f"[ab_one] SINGLE TICKER {t}")
            return hit

        ab._filter_liquid = _one  # type: ignore
        out = ab.run(date=args.date, top=args.top)
        ab._filter_liquid = orig_filter  # type: ignore

    print("\n=== row ===")
    cols = [c for c in ("Ticker", "score", "pair_day_a", "pair_day_b", "n_good", "n_bad") if c in out.columns]
    print(out[cols].to_string(index=False) if cols else out.head(1).to_string(index=False))

    if args.with_colors:
        from . import quote_colors as qc
        qc.run(tickers=[t], asof=args.date)

    if args.with_form4:
        from . import insider_history as ih
        ih.run(tickers=[t], months=args.months, resume=True)

    if args.merge or args.with_colors or args.with_form4:
        from . import ab_merge_extras as mx
        # asof from checklist output
        asof = str(out["asof_date"].iloc[0]) if "asof_date" in out.columns else args.date
        mx.run(date=asof)


if __name__ == "__main__":
    main()
