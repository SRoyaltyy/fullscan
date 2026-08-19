"""Fast single-ticker AB checklist (bypasses mcap/ADV liquid gate).

Usage:
  python -m src.ab_one BB
  python -m src.ab_one BB --date 2026-08-18
  python -m src.ab_one --ticker BB --date 2026-08-18
  python -m src.ab_one BB --with-colors --with-form4 --months 18
"""
from __future__ import annotations

import argparse

import pandas as pd

from . import ab_checklist as ab


def _score_one(ticker: str, date: str | None, top: int) -> pd.DataFrame:
    t = ticker.strip().upper()
    orig_filter = ab._filter_liquid

    def _one(df: pd.DataFrame) -> pd.DataFrame:
        raw = df.copy()
        tcol = "Ticker" if "Ticker" in raw.columns else raw.columns[0]
        raw["Ticker"] = raw[tcol].astype(str).str.strip().str.upper()
        hit = raw[raw["Ticker"] == t].copy()
        if hit.empty:
            raise SystemExit(f"[ab_one] {t} not in Finviz export for this as-of")

        # unit fields used downstream
        if "_mcap" not in hit.columns:
            hit["_mcap"] = (
                hit["Market Cap"].map(ab._num) * 1e6 if "Market Cap" in hit.columns else 0.0
            )
        if "_adv" not in hit.columns:
            hit["_adv"] = (
                hit["Average Volume"].map(ab._num) * 1e3
                if "Average Volume" in hit.columns
                else 0.0
            )

        mcap = float(hit["_mcap"].iloc[0]) if "_mcap" in hit.columns else float("nan")
        adv = float(hit["_adv"].iloc[0]) if "_adv" in hit.columns else float("nan")
        gate = (mcap > ab.MCAP_MIN) and (adv > ab.ADV_MIN)
        print(
            f"[ab_one] {t}: mcap={mcap:,.0f} adv={adv:,.0f} "
            f"liquid_gate={'PASS' if gate else 'BYPASS (single-ticker)'}"
        )
        return hit

    ab._filter_liquid = _one  # type: ignore
    try:
        # Prefer native ticker kw if present
        try:
            out = ab.run(date=date, top=top, ticker=t)
        except TypeError:
            out = ab.run(date=date, top=top)
    finally:
        ab._filter_liquid = orig_filter  # type: ignore
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description="Score one ticker through AB checklist (no liquid gate)")
    ap.add_argument("ticker_pos", nargs="?", default=None, help="e.g. BB")
    ap.add_argument("--ticker", default=None, help="same as positional")
    ap.add_argument("--date", default=None)
    ap.add_argument("--top", type=int, default=5)
    ap.add_argument("--with-colors", action="store_true")
    ap.add_argument("--with-form4", action="store_true")
    ap.add_argument("--months", type=int, default=18)
    ap.add_argument("--merge", action="store_true")
    args = ap.parse_args()

    t = (args.ticker or args.ticker_pos or "").strip().upper()
    if not t:
        raise SystemExit("[ab_one] pass ticker: python -m src.ab_one BB   or  --ticker BB")

    out = _score_one(t, args.date, args.top)

    print("\n=== row ===")
    cols = [
        c
        for c in ("Ticker", "score", "pair_day_a", "pair_day_b", "n_good", "n_bad", "asof_date")
        if c in out.columns
    ]
    print(out[cols].to_string(index=False) if cols else out.head(1).to_string(index=False))

    if args.with_colors:
        from . import quote_colors as qc

        qc.run(tickers=[t], asof=args.date)

    if args.with_form4:
        from . import insider_history as ih

        ih.run(tickers=[t], months=args.months, resume=True)

    if args.merge or args.with_colors or args.with_form4:
        from . import ab_merge_extras as mx

        asof = str(out["asof_date"].iloc[0]) if "asof_date" in out.columns else args.date
        mx.run(date=asof)


if __name__ == "__main__":
    main()
