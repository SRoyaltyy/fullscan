"""Point-in-time daily AB checklist backfill.

Window: --months N or --start/--end (no hard cap).

Finviz exports in this repo may only cover a short span. For dates *before* the
earliest export we still score **Part A (OHLC)** every session; B1 fundamental
flags are 0 / n/a (not inventing snapshot data). That is why a 24m run used to
look "stuck" at ~6m — it was skipping every day with no export <= asof.

CLI:
  python -m src.ab_backfill --months 24 --ticker AAPL
  python -m src.ab_backfill --start 2024-08-01 --end 2026-08-18 --ticker AAPL
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

FORWARD_BARS = 42


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
    return {"flag": flag, "val": f"month={row['month']} net={net} delta={delta}"}


def _setup_flag(a: dict, b: dict) -> tuple[int, str]:
    if not a.get("ok"):
        return 0, "no_ohlc"
    prof = bool(b.get("profitable"))
    rsi = a.get("rsi")
    sec = a.get("sections") or {}
    near_floor = bool(sec.get("ok") and (sec.get("rising_lows") or sec.get("flatish")))
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
    if prof and oversold and (near_floor or near_low_px) and body_ok and vol_ok:
        return 1, "SETUP_GOOD"
    if prof and oversold and (near_floor or near_low_px):
        return 0, "SETUP_PARTIAL"
    return 0, "SETUP_NO"


def _empty_b1() -> dict:
    return {
        "eps_surprise": np.nan,
        "rev_surprise": np.nan,
        "eps_surprise_prev": np.nan,
        "rev_surprise_prev": np.nan,
        "sales": np.nan,
        "income": np.nan,
        "profit_margin": np.nan,
        "profitable": False,
        "target_price": np.nan,
        "target_delta": np.nan,
        "target_prev": np.nan,
        "analyst_recom": np.nan,
        "insider_tx": np.nan,
        "insider_delta": np.nan,
        "insider_prev": np.nan,
        "inst_tx": np.nan,
        "short_float": np.nan,
        "earnings_date": "",
        "prior_export_date": None,
    }


def _forward_extrema(ohlc, asof: str, entry: float, max_bars: int = FORWARD_BARS) -> dict:
    empty = {
        "max_profit_2m": np.nan,
        "max_loss_2m": np.nan,
        "fwd_bars": 0,
        "fwd_last_date": None,
        "fwd_window": "none",
        "entry_px": entry,
    }
    if ohlc is None or ohlc.empty or not np.isfinite(entry) or entry <= 0:
        return empty
    df = ohlc.copy()
    if not isinstance(df.index, pd.DatetimeIndex):
        df.index = pd.to_datetime(df.index)
    df.columns = [c.lower() for c in df.columns]
    fut = df[df.index > pd.Timestamp(asof)].head(max_bars)
    if fut.empty:
        return {**empty, "fwd_window": "no_future_bars"}
    high = fut["high"].astype(float) if "high" in fut.columns else fut["close"].astype(float)
    low = fut["low"].astype(float) if "low" in fut.columns else fut["close"].astype(float)
    n = len(fut)
    note = f"{fut.index[0].date().isoformat()}→{fut.index[-1].date().isoformat()} ({n} sessions)"
    if n < max_bars:
        note += f" truncated_vs_{max_bars}"
    return {
        "max_profit_2m": float(high.max() / entry - 1.0),
        "max_loss_2m": float(low.min() / entry - 1.0),
        "fwd_bars": n,
        "fwd_last_date": fut.index[-1].date().isoformat(),
        "fwd_window": note,
        "entry_px": entry,
    }


def _score_one(
    ticker,
    asof,
    ohlc_to_asof,
    ohlc_full,
    row,  # may be None when no export
    prior_row,
    prior_date,
    form4_panel,
    export_used: str | None,
) -> dict:
    a = ab._part_a(ohlc_to_asof)
    if row is not None:
        b = ab._part_b1(row, prior_row, prior_date)
        pb = ab._pass_b1(b)
        b1_mode = "export"
    else:
        b = _empty_b1()
        pb = {k: 0 for k in ab.FEATURE_ORDER if k.startswith("B")}
        b1_mode = "ohlc_only_no_export"

    pa = ab._pass_a(a)
    setup_flag, setup_val = _setup_flag(a, b)
    flags = {**pa, **pb, "A14_profitable_oversold_setup": setup_flag}
    f4 = _form4_asof(form4_panel, ticker, asof)
    flags["B15_form4_insider"] = f4["flag"]
    score = int(sum(int(v) for v in flags.values()))
    pair = (a.get("pair") or {}) if a.get("ok") else {}
    entry = a.get("price") if a.get("ok") else (
        ab._num(row.get("Price")) if row is not None else np.nan
    )
    fwd = _forward_extrema(ohlc_full, asof, float(entry) if np.isfinite(entry) else np.nan)
    return {
        "Ticker": ticker,
        "asof_date": asof,
        "score": score,
        "score_mode": b1_mode,
        "n_good": sum(1 for v in flags.values() if v > 0),
        "n_bad": sum(1 for v in flags.values() if v < 0),
        "pair_day_a": pair.get("d_a"),
        "pair_day_b": pair.get("d_b"),
        "rsi": a.get("rsi") if a.get("ok") else np.nan,
        "price": entry,
        "profitable": b.get("profitable"),
        "body_rg_2day": pair.get("body_rg"),
        "vol_rg_2day": pair.get("vol_rg"),
        "setup_flag": setup_flag,
        "setup_val": setup_val,
        "form4_val": f4["val"],
        "form4_flag": f4["flag"],
        "export_prior": prior_date,
        "export_used": export_used or "none",
        "max_profit_2m": fwd["max_profit_2m"],
        "max_loss_2m": fwd["max_loss_2m"],
        "fwd_bars": fwd["fwd_bars"],
        "fwd_last_date": fwd["fwd_last_date"],
        "fwd_window": fwd["fwd_window"],
        **{f"flag_{k}": flags[k] for k in flags},
    }


def _ensure_price_coverage(start: str, end: str, ticker: str | None) -> None:
    store = ps._load_store()
    need_days = (pd.Timestamp(end) - pd.Timestamp(start)).days + 150
    need_days = max(int(need_days), 400)
    if len(store):
        dmin = pd.to_datetime(store["date"]).min().date().isoformat()
        if dmin <= start:
            print(f"[backfill] price store from {dmin} covers start {start}")
            return
        print(f"[backfill] price store from {dmin} > start {start} — bootstrap days={need_days}")
    else:
        print(f"[backfill] empty price store — bootstrap days={need_days}")
    ps.bootstrap(days=need_days, tickers=[ticker.upper()] if ticker else None, resume=True)


def run(
    start: str | None = None,
    end: str | None = None,
    months: int | None = None,
    ticker: str | None = None,
) -> pd.DataFrame:
    exports = _load_exports()
    exp_dates = [d for d, _ in exports]
    first_exp = exp_dates[0] if exp_dates else None
    last_exp = exp_dates[-1] if exp_dates else None

    end = end or (last_exp or datetime.now(ET).date().isoformat())
    if start is None:
        m = 6 if months is None else int(months)
        if m < 1:
            raise SystemExit("[backfill] months must be >= 1")
        start = (pd.Timestamp(end) - pd.DateOffset(months=m)).date().isoformat()

    print(f"[backfill] requested {start} → {end} (months_arg={months})")
    print(
        f"[backfill] finviz exports on disk: n={len(exports)} "
        f"span={first_exp or 'none'}→{last_exp or 'none'}"
    )
    if first_exp and start < first_exp:
        print(
            f"[backfill] NOTE: days before {first_exp} score OHLC-only "
            f"(no Finviz snapshot yet — B1 flags neutral). "
            f"This is expected; add older finviz_YYYY-MM-DD.csv to deepen B1."
        )

    _ensure_price_coverage(start, end, ticker)
    store = ps._load_store()
    if not len(store):
        raise SystemExit("[backfill] empty price store after bootstrap")

    all_days = sorted(pd.to_datetime(store["date"]).unique())
    days = [
        d.date().isoformat()
        for d in all_days
        if start <= d.date().isoformat() <= end
    ]
    if not days:
        d0 = pd.to_datetime(store["date"]).min().date().isoformat()
        d1 = pd.to_datetime(store["date"]).max().date().isoformat()
        raise SystemExit(
            f"[backfill] no price days in {start}..{end}. Store covers {d0}..{d1}."
        )

    form4 = None
    if INS_PANEL.exists():
        form4 = pd.read_parquet(INS_PANEL)
    elif INS_PANEL_CSV.exists():
        form4 = pd.read_csv(INS_PANEL_CSV)

    store = store.copy()
    store["date"] = pd.to_datetime(store["date"])
    store["ticker"] = store["ticker"].astype(str).str.upper()
    groups = {t: g.set_index("date").sort_index() for t, g in store.groupby("ticker")}

    # Membership universe: latest export liquid list (or single ticker)
    latest_df = exports[-1][1] if exports else None
    if ticker:
        tickers = [ticker.upper()]
    elif latest_df is not None:
        liquid = ab._filter_liquid(latest_df)
        tickers = liquid["Ticker"].astype(str).str.upper().tolist()
    else:
        tickers = sorted(groups.keys())

    print(
        f"[backfill] scoring sessions={len(days)} ({days[0]}→{days[-1]}) "
        f"names={len(tickers)} ticker={ticker or 'LIQUID'}"
    )

    rows = []
    n_ohlc_only = 0
    n_with_export = 0
    for i, asof in enumerate(days, 1):
        exp_date, exp_df = _export_asof(exports, asof)
        exp_idx = None
        if exp_df is not None:
            exp_idx = exp_df.set_index("Ticker")

        for t in tickers:
            g = groups.get(t)
            if g is None:
                continue
            ohlc_to = g[g.index <= pd.Timestamp(asof)]
            if ohlc_to.empty:
                continue
            # need enough bars for indicators
            if len(ohlc_to) < 25:
                continue

            row = None
            prior_row = None
            prior_d = None
            if exp_idx is not None and t in exp_idx.index:
                row = exp_idx.loc[t]
                if isinstance(row, pd.DataFrame):
                    row = row.iloc[0]
                prior_row, prior_d = _prior_export_row(exports, exp_date or asof, t)
                n_with_export += 1
            else:
                n_ohlc_only += 1

            rec = _score_one(
                t, asof, ohlc_to, g, row, prior_row, prior_d, form4, exp_date
            )
            rows.append(rec)

        if i % 20 == 0 or i == len(days):
            print(
                f"[backfill] {asof} ({i}/{len(days)}) rows={len(rows):,} "
                f"export_days={n_with_export:,} ohlc_only={n_ohlc_only:,}"
            )

    out = pd.DataFrame(rows)
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    tag = ticker.upper() if ticker else "universe"
    stem = f"{start}_{end}_{tag}"
    parquet = OUT_DIR / f"{stem}.parquet"
    out.to_parquet(parquet, index=False)
    if ticker or len(out) < 500_000:
        out.to_csv(OUT_DIR / f"{stem}.csv", index=False)

    def pct(x):
        if x is None or (isinstance(x, float) and not np.isfinite(x)):
            return "n/a"
        return f"{100.0 * float(x):+.2f}%"

    lines = [
        f"# AB PIT backfill — {start} → {end} — {tag}",
        "",
        f"- Rows: **{len(out):,}**",
        f"- Requested: `{start}` → `{end}`",
        f"- Actual score span: "
        + (f"`{out['asof_date'].min()}` → `{out['asof_date'].max()}`" if len(out) else "empty"),
        f"- Finviz exports on disk: `{first_exp}` → `{last_exp}` (n={len(exports)})",
        f"- Rows with export B1: **{n_with_export:,}** · OHLC-only (no export yet): **{n_ohlc_only:,}**",
        "- Days before first export still get Part A scores (RSI, 2d ratios, 5d tape, sections).",
        "- To deepen B1 history: add more `data/exports/finviz_YYYY-MM-DD.csv` files.",
        "",
    ]
    if len(out):
        if ticker:
            lines += [
                f"## {ticker} — every asof day",
                "",
                "| date | score | mode | RSI | setup | max_profit_2m | max_loss_2m | export |",
                "|------|------:|------|----:|:-----:|--------------:|------------:|--------|",
            ]
            for _, r in out.sort_values("asof_date").iterrows():
                lines.append(
                    f"| {r['asof_date']} | {int(r['score']):+d} | {r.get('score_mode')} | "
                    f"{r['rsi'] if np.isfinite(r.get('rsi', np.nan)) else float('nan'):.1f} | "
                    f"{int(r['setup_flag'])} | {pct(r['max_profit_2m'])} | {pct(r['max_loss_2m'])} | "
                    f"{r.get('export_used')} |"
                )

    lines += ["", f"- parquet: `{parquet.relative_to(ROOT)}`"]
    md = OUT_DIR / f"{stem}.md"
    md.write_text("\n".join(lines), encoding="utf-8")
    (OUT_DIR / f"{stem}.json").write_text(
        json.dumps(
            {
                "start": start,
                "end": end,
                "months_arg": months,
                "ticker": ticker,
                "n_rows": int(len(out)),
                "n_with_export": n_with_export,
                "n_ohlc_only": n_ohlc_only,
                "export_first": first_exp,
                "export_last": last_exp,
                "actual_first": out["asof_date"].min() if len(out) else None,
                "actual_last": out["asof_date"].max() if len(out) else None,
                "generated": datetime.now(ET).isoformat(),
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    print(f"[backfill] wrote {parquet} rows={len(out):,}")
    print(f"[backfill] wrote {md}")
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default=None)
    ap.add_argument("--end", default=None)
    ap.add_argument("--months", type=int, default=None)
    ap.add_argument("--ticker", default=None)
    args = ap.parse_args()
    run(start=args.start, end=args.end, months=args.months, ticker=args.ticker)


if __name__ == "__main__":
    main()
