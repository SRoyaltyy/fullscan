"""Point-in-time daily AB checklist backfill (no look-ahead).

For each trading day D in [start, end]:
  * OHLC features use bars with date <= D only
  * Finviz B1 fields come from the latest export with export_date <= D
  * Form4 monthly nets use months fully completed before D
  * Live quote colors / analyst scrape are NOT used historically (would leak)

CLI:
  python -m src.ab_backfill --months 6
  python -m src.ab_backfill --months 6 --ticker AAPL
  python -m src.ab_backfill --start 2026-02-19 --end 2026-08-18 --ticker AMLX
"""
from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

from . import config
from . import price_store as ps
from . import ab_checklist as ab

ROOT = Path(__file__).resolve().parent.parent
EXPORT_DIR = ROOT / "data" / "exports"
OUT_DIR = ROOT / "data" / "ab_backfill"
INS_PANEL = ROOT / "data" / "insider" / "history" / "monthly_panel.parquet"
INS_PANEL_CSV = ROOT / "data" / "insider" / "history" / "monthly_panel.csv"
ET = ZoneInfo(config.TZ)


def _load_exports() -> list[tuple[str, pd.DataFrame]]:
    files = sorted(EXPORT_DIR.glob("finviz_????-??-??.csv"))
    out = []
    for f in files:
        d = f.stem.replace("finviz_", "")
        df = pd.read_csv(f, low_memory=False)
        tcol = "Ticker" if "Ticker" in df.columns else df.columns[0]
        df["Ticker"] = df[tcol].astype(str).str.strip().str.upper()
        df = df.drop_duplicates("Ticker", keep="first")
        out.append((d, df))
    return out


def _export_asof(exports: list[tuple[str, pd.DataFrame]], asof: str):
    """Latest export with date <= asof."""
    cand = [(d, df) for d, df in exports if d <= asof]
    if not cand:
        return None, None
    return cand[-1]


def _prior_export_row(exports, asof: str, ticker: str):
    cand = [(d, df) for d, df in exports if d < asof]
    if not cand:
        return None, None
    d, df = cand[-1]
    hit = df[df["Ticker"] == ticker]
    if hit.empty:
        return None, d
    return hit.iloc[0], d


def _form4_asof(panel: pd.DataFrame, ticker: str, asof: str) -> dict:
    if panel is None or panel.empty:
        return {"flag": 0, "val": "no_panel"}
    month = (pd.Timestamp(asof).to_period("M") - 1).strftime("%Y-%m")
    p = panel[panel["ticker"].astype(str).str.upper() == ticker.upper()]
    p = p[p["month"] <= month]
    if p.empty:
        return {"flag": 0, "val": f"no_form4_month<={month}"}
    row = p.sort_values("month").iloc[-1]
    net = float(row.get("net_value", np.nan))
    delta = float(row.get("net_delta", np.nan)) if "net_delta" in row.index else np.nan
    flag = 0
    if np.isfinite(delta):
        flag = 1 if delta > 0 else (-1 if delta < 0 else 0)
    elif np.isfinite(net):
        flag = 1 if net > 0 else (-1 if net < 0 else 0)
    return {
        "flag": flag,
        "val": f"month={row['month']} net={net} delta={delta}",
        "net": net,
        "delta": delta,
    }


def _setup_flag(a: dict, b: dict, pa: dict) -> tuple[int, str]:
    """Profitable + RSI<30 + near 3m lows + positive 2-day body ratio → GOOD setup."""
    if not a.get("ok"):
        return 0, "no_ohlc"
    prof = bool(b.get("profitable"))
    rsi = a.get("rsi")
    sec = a.get("sections") or {}
    near_floor = bool(sec.get("ok") and (sec.get("rising_lows") or sec.get("flatish")))
    # also near if last price close to the lowest of the three section lows
    near_low_px = False
    if sec.get("ok") and sec.get("lows"):
        floor = min(sec["lows"])
        px = a.get("price")
        if np.isfinite(px) and floor > 0 and (px / floor - 1.0) <= 0.08:
            near_low_px = True
    pair = a.get("pair") or {}
    body_ok = np.isfinite(pair.get("body_rg", np.nan)) and pair["body_rg"] >= 1.0
    vol_ok = (not np.isfinite(pair.get("vol_rg", np.nan))) or pair["vol_rg"] >= 1.0
    oversold = np.isfinite(rsi) and rsi < 30

    parts = {
        "profitable": prof,
        "rsi": rsi,
        "oversold": oversold,
        "near_floor_structure": near_floor,
        "near_low_px_8pct": near_low_px,
        "body_rg_2day_ge1": body_ok,
        "vol_rg_ok": vol_ok,
    }
    if prof and oversold and (near_floor or near_low_px) and body_ok and vol_ok:
        return 1, f"SETUP_GOOD {parts}"
    if prof and oversold and (near_floor or near_low_px):
        return 0, f"SETUP_PARTIAL {parts}"  # missing ratio confirmation
    return 0, f"SETUP_NO {parts}"


def _score_one(
    ticker: str,
    asof: str,
    ohlc: pd.DataFrame | None,
    row: pd.Series,
    prior_row,
    prior_date: str | None,
    form4_panel: pd.DataFrame | None,
) -> dict:
    a = ab._part_a(ohlc)
    b = ab._part_b1(row, prior_row, prior_date)
    pa = ab._pass_a(a)
    pb = ab._pass_b1(b)
    # strengthen A01 when profitable oversold (already +1 if RSI<=30; keep)
    setup_flag, setup_val = _setup_flag(a, b, pa)
    flags = {**pa, **pb, "A14_profitable_oversold_setup": setup_flag}
    f4 = _form4_asof(form4_panel, ticker, asof)
    flags["B15_form4_insider"] = f4["flag"]
    score = int(sum(int(v) for v in flags.values()))
    pair = (a.get("pair") or {}) if a.get("ok") else {}
    return {
        "Ticker": ticker,
        "asof_date": asof,
        "score": score,
        "n_good": sum(1 for v in flags.values() if v > 0),
        "n_bad": sum(1 for v in flags.values() if v < 0),
        "pair_day_a": pair.get("d_a"),
        "pair_day_b": pair.get("d_b"),
        "rsi": a.get("rsi") if a.get("ok") else np.nan,
        "price": a.get("price") if a.get("ok") else ab._num(row.get("Price")),
        "profitable": b.get("profitable"),
        "body_rg_2day": pair.get("body_rg"),
        "vol_rg_2day": pair.get("vol_rg"),
        "setup_flag": setup_flag,
        "setup_val": setup_val,
        "form4_val": f4["val"],
        "form4_flag": f4["flag"],
        "export_prior": prior_date,
        **{f"flag_{k}": flags[k] for k in flags},
    }


def run(
    start: str | None = None,
    end: str | None = None,
    months: int = 6,
    ticker: str | None = None,
) -> pd.DataFrame:
    exports = _load_exports()
    if not exports:
        raise SystemExit("[backfill] no finviz exports")

    store = ps._load_store()
    if not len(store):
        raise SystemExit("[backfill] empty price store — bootstrap first")

    end = end or exports[-1][0]
    if start is None:
        start = (pd.Timestamp(end) - pd.DateOffset(months=months)).date().isoformat()

    # trading days from price store
    days = sorted(
        d.date().isoformat()
        for d in pd.to_datetime(store["date"]).unique()
        if start <= d.date().isoformat() <= end
    )
    if not days:
        raise SystemExit(f"[backfill] no price days in {start}..{end}")

    form4 = None
    if INS_PANEL.exists():
        form4 = pd.read_parquet(INS_PANEL)
    elif INS_PANEL_CSV.exists():
        form4 = pd.read_csv(INS_PANEL_CSV)

    print(f"[backfill] {start} → {end}  days={len(days)}  exports={len(exports)}  ticker={ticker or 'LIQUID'}")

    # pre-group OHLC
    store = store.copy()
    store["date"] = pd.to_datetime(store["date"])
    store["ticker"] = store["ticker"].astype(str).str.upper()
    groups = {t: g.set_index("date").sort_index() for t, g in store.groupby("ticker")}

    rows = []
    for i, asof in enumerate(days, 1):
        exp_date, exp_df = _export_asof(exports, asof)
        if exp_df is None:
            print(f"[backfill] skip {asof}: no export")
            continue
        liquid = ab._filter_liquid(exp_df)
        if ticker:
            t = ticker.upper()
            hit = liquid[liquid["Ticker"] == t]
            if hit.empty:
                hit = exp_df[exp_df["Ticker"] == t].copy()
                if hit.empty:
                    continue
                if "_mcap" not in hit.columns:
                    hit["_mcap"] = hit["Market Cap"].map(ab._num) * 1e6 if "Market Cap" in hit.columns else 0.0
                    hit["_adv"] = hit["Average Volume"].map(ab._num) * 1e3 if "Average Volume" in hit.columns else 0.0
            liquid = hit

        for _, row in liquid.iterrows():
            t = row["Ticker"]
            g = groups.get(t)
            if g is None:
                ohlc = None
            else:
                ohlc = g[g.index <= pd.Timestamp(asof)]
            prior_row, prior_d = _prior_export_row(exports, exp_date or asof, t)
            rec = _score_one(t, asof, ohlc, row, prior_row, prior_d, form4)
            rec["export_used"] = exp_date
            rows.append(rec)

        if i % 5 == 0 or i == len(days):
            print(f"[backfill] {asof} ({i}/{len(days)}) rows_so_far={len(rows):,}")

    out = pd.DataFrame(rows)
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    tag = ticker.upper() if ticker else "universe"
    stem = f"{start}_{end}_{tag}"
    parquet = OUT_DIR / f"{stem}.parquet"
    csv = OUT_DIR / f"{stem}.csv"
    out.to_parquet(parquet, index=False)
    # CSV can be huge for universe — write always for single ticker; sample for universe
    if ticker or len(out) < 500_000:
        out.to_csv(csv, index=False)

    # compact MD summary
    lines = [
        f"# AB PIT backfill — {start} → {end} — {tag}",
        "",
        f"- Rows: **{len(out):,}** · days={out['asof_date'].nunique() if len(out) else 0} · tickers={out['Ticker'].nunique() if len(out) else 0}",
        "- Point-in-time: OHLC≤D, export≤D, Form4 month≤prior month",
        "- **A14 setup**: profitable + RSI<30 + near 3m lows + 2-day body/vol ratios ≥1 → GOOD",
        "",
    ]
    if len(out):
        by = out.groupby("asof_date").agg(
            mean_score=("score", "mean"),
            n=("Ticker", "count"),
            n_setup=("setup_flag", lambda s: int((s > 0).sum())),
        )
        lines += ["## Daily mean score (last 15)", "", "| date | mean_score | n | setups |", "|------|----------:|--:|-------:|"]
        for d, r in by.tail(15).iterrows():
            lines.append(f"| {d} | {r['mean_score']:.2f} | {int(r['n'])} | {int(r['n_setup'])} |")
        if ticker:
            lines += ["", f"## {ticker} path", "", "| date | score | RSI | setup | pair |", "|------|------:|----:|:-----:|------|"]
            sub = out.sort_values("asof_date")
            for _, r in sub.iterrows():
                lines.append(
                    f"| {r['asof_date']} | {int(r['score']):+d} | {r['rsi']:.1f} | "
                    f"{int(r['setup_flag'])} | {r.get('pair_day_a')}→{r.get('pair_day_b')} |"
                )
    lines += ["", f"- parquet: `{parquet.relative_to(ROOT)}`"]
    md = OUT_DIR / f"{stem}.md"
    md.write_text("\n".join(lines), encoding="utf-8")
    (OUT_DIR / f"{stem}.json").write_text(
        json.dumps({
            "start": start,
            "end": end,
            "ticker": ticker,
            "n_rows": int(len(out)),
            "generated": datetime.now(ET).isoformat(),
        }, indent=2),
        encoding="utf-8",
    )
    print(f"[backfill] wrote {parquet} rows={len(out):,}")
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default=None)
    ap.add_argument("--end", default=None)
    ap.add_argument("--months", type=int, default=6)
    ap.add_argument("--ticker", default=None)
    args = ap.parse_args()
    run(start=args.start, end=args.end, months=args.months, ticker=args.ticker)


if __name__ == "__main__":
    main()
