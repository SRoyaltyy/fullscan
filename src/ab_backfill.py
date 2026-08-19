"""Point-in-time daily AB checklist backfill (no look-ahead on features).

Window: any length via --months or --start/--end (no 6-month cap).
Forward labels: max_profit_2m / max_loss_2m over next ~42 sessions (or until data ends).

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

FORWARD_BARS = 42  # ~2 months of trading sessions


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
    return {
        "flag": flag,
        "val": f"month={row['month']} net={net} delta={delta}",
        "net": net,
        "delta": delta,
    }


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
    parts = {
        "profitable": prof,
        "rsi": rsi,
        "oversold": oversold,
        "near_floor": near_floor,
        "near_low_px": near_low_px,
        "body_ok": body_ok,
        "vol_ok": vol_ok,
    }
    if prof and oversold and (near_floor or near_low_px) and body_ok and vol_ok:
        return 1, f"SETUP_GOOD {parts}"
    if prof and oversold and (near_floor or near_low_px):
        return 0, f"SETUP_PARTIAL {parts}"
    return 0, f"SETUP_NO {parts}"


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
    mx = float(high.max() / entry - 1.0)
    mn = float(low.min() / entry - 1.0)
    last_d = fut.index[-1].date().isoformat()
    first_d = fut.index[0].date().isoformat()
    n = len(fut)
    note = f"{first_d}→{last_d} ({n} sessions)"
    if n < max_bars:
        note += f" truncated_vs_{max_bars}"
    return {
        "max_profit_2m": mx,
        "max_loss_2m": mn,
        "fwd_bars": n,
        "fwd_last_date": last_d,
        "fwd_window": note,
        "entry_px": entry,
    }


def _score_one(
    ticker, asof, ohlc_to_asof, ohlc_full, row, prior_row, prior_date, form4_panel
) -> dict:
    a = ab._part_a(ohlc_to_asof)
    b = ab._part_b1(row, prior_row, prior_date)
    pa = ab._pass_a(a)
    pb = ab._pass_b1(b)
    setup_flag, setup_val = _setup_flag(a, b)
    flags = {**pa, **pb, "A14_profitable_oversold_setup": setup_flag}
    f4 = _form4_asof(form4_panel, ticker, asof)
    flags["B15_form4_insider"] = f4["flag"]
    score = int(sum(int(v) for v in flags.values()))
    pair = (a.get("pair") or {}) if a.get("ok") else {}
    entry = a.get("price") if a.get("ok") else ab._num(row.get("Price"))
    fwd = _forward_extrema(ohlc_full, asof, float(entry) if np.isfinite(entry) else np.nan)
    return {
        "Ticker": ticker,
        "asof_date": asof,
        "score": score,
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
        "max_profit_2m": fwd["max_profit_2m"],
        "max_loss_2m": fwd["max_loss_2m"],
        "fwd_bars": fwd["fwd_bars"],
        "fwd_last_date": fwd["fwd_last_date"],
        "fwd_window": fwd["fwd_window"],
        **{f"flag_{k}": flags[k] for k in flags},
    }


def _ensure_price_coverage(start: str, end: str, ticker: str | None) -> None:
    """If store does not reach start, bootstrap more history (calendar days ≈ 1.5× span)."""
    store = ps._load_store()
    need_days = (pd.Timestamp(end) - pd.Timestamp(start)).days + 120  # buffer for indicators
    need_days = max(need_days, 400)
    if len(store):
        dmin = pd.to_datetime(store["date"]).min()
        if dmin.date().isoformat() <= start:
            print(f"[backfill] price store covers from {dmin.date()} — ok")
            return
        print(f"[backfill] price store starts {dmin.date()} > requested {start} — extending bootstrap")
    else:
        print("[backfill] empty price store — bootstrap")
    tickers = [ticker.upper()] if ticker else None
    ps.bootstrap(days=int(need_days), tickers=tickers, resume=True)


def run(
    start: str | None = None,
    end: str | None = None,
    months: int | None = None,
    ticker: str | None = None,
) -> pd.DataFrame:
    exports = _load_exports()
    if not exports:
        raise SystemExit("[backfill] no finviz exports")

    end = end or exports[-1][0]
    if start is None:
        m = 6 if months is None else int(months)
        if m < 1:
            raise SystemExit("[backfill] months must be >= 1")
        start = (pd.Timestamp(end) - pd.DateOffset(months=m)).date().isoformat()

    print(f"[backfill] requested window {start} → {end} (months_arg={months})")
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
            f"[backfill] no price days in {start}..{end}. Store covers {d0}..{d1}. "
            f"Re-run with bootstrap --days covering the window."
        )

    if days[0] > start:
        print(
            f"[backfill] WARN: first scored day {days[0]} > requested start {start} "
            f"(limited by price store / exports)"
        )

    form4 = None
    if INS_PANEL.exists():
        form4 = pd.read_parquet(INS_PANEL)
    elif INS_PANEL_CSV.exists():
        form4 = pd.read_csv(INS_PANEL_CSV)

    print(
        f"[backfill] scoring days={len(days)} ({days[0]}→{days[-1]}) exports={len(exports)} "
        f"ticker={ticker or 'LIQUID'} fwd_bars={FORWARD_BARS}"
    )

    store = store.copy()
    store["date"] = pd.to_datetime(store["date"])
    store["ticker"] = store["ticker"].astype(str).str.upper()
    groups = {t: g.set_index("date").sort_index() for t, g in store.groupby("ticker")}

    rows = []
    for i, asof in enumerate(days, 1):
        exp_date, exp_df = _export_asof(exports, asof)
        if exp_df is None:
            print(f"[backfill] skip {asof}: no export <= date")
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
                    hit["_mcap"] = (
                        hit["Market Cap"].map(ab._num) * 1e6 if "Market Cap" in hit.columns else 0.0
                    )
                    hit["_adv"] = (
                        hit["Average Volume"].map(ab._num) * 1e3
                        if "Average Volume" in hit.columns
                        else 0.0
                    )
            liquid = hit

        for _, row in liquid.iterrows():
            t = row["Ticker"]
            g = groups.get(t)
            if g is None:
                ohlc_to = None
                ohlc_full = None
            else:
                ohlc_to = g[g.index <= pd.Timestamp(asof)]
                ohlc_full = g
            prior_row, prior_d = _prior_export_row(exports, exp_date or asof, t)
            rec = _score_one(t, asof, ohlc_to, ohlc_full, row, prior_row, prior_d, form4)
            rec["export_used"] = exp_date
            rows.append(rec)

        if i % 10 == 0 or i == len(days):
            print(f"[backfill] {asof} ({i}/{len(days)}) rows_so_far={len(rows):,}")

    out = pd.DataFrame(rows)
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    tag = ticker.upper() if ticker else "universe"
    stem = f"{start}_{end}_{tag}"
    parquet = OUT_DIR / f"{stem}.parquet"
    csv = OUT_DIR / f"{stem}.csv"
    out.to_parquet(parquet, index=False)
    if ticker or len(out) < 500_000:
        out.to_csv(csv, index=False)

    def pct(x):
        if x is None or (isinstance(x, float) and not np.isfinite(x)):
            return "n/a"
        return f"{100.0 * float(x):+.2f}%"

    lines = [
        f"# AB PIT backfill — {start} → {end} — {tag}",
        "",
        f"- Rows: **{len(out):,}** · days={out['asof_date'].nunique() if len(out) else 0} · "
        f"tickers={out['Ticker'].nunique() if len(out) else 0}",
        f"- Actual score span: "
        + (
            f"{out['asof_date'].min()} → {out['asof_date'].max()}"
            if len(out)
            else "empty"
        ),
        "- Features: OHLC≤D, export≤D, Form4 month≤prior (no look-ahead)",
        f"- Forward check: next {FORWARD_BARS} sessions (~2m) or until last bar",
        "- A14: profitable + RSI<30 + near 3m lows + 2d ratios",
        "- A15: body_rg>1.4 + red_wick>green_wick + maxG>maxR (5d)",
        "",
    ]
    if len(out):
        by = out.groupby("asof_date").agg(
            mean_score=("score", "mean"),
            n=("Ticker", "count"),
            n_setup=("setup_flag", lambda s: int((s > 0).sum())),
            mean_max_profit=("max_profit_2m", "mean"),
            mean_max_loss=("max_loss_2m", "mean"),
        )
        lines += [
            "## Daily aggregates (last 15)",
            "",
            "| date | mean_score | n | setups | mean max_profit | mean max_loss |",
            "|------|----------:|--:|-------:|----------------:|--------------:|",
        ]
        for d, r in by.tail(15).iterrows():
            lines.append(
                f"| {d} | {r['mean_score']:.2f} | {int(r['n'])} | {int(r['n_setup'])} | "
                f"{pct(r['mean_max_profit'])} | {pct(r['mean_max_loss'])} |"
            )

        if ticker:
            lines += [
                "",
                f"## {ticker} — every asof day",
                "",
                "| date | score | RSI | setup | max_profit_2m | max_loss_2m | fwd_window | pair |",
                "|------|------:|----:|:-----:|--------------:|------------:|-----------|------|",
            ]
            sub = out.sort_values("asof_date")
            for _, r in sub.iterrows():
                lines.append(
                    f"| {r['asof_date']} | {int(r['score']):+d} | "
                    f"{r['rsi'] if np.isfinite(r.get('rsi', np.nan)) else float('nan'):.1f} | "
                    f"{int(r['setup_flag'])} | {pct(r['max_profit_2m'])} | {pct(r['max_loss_2m'])} | "
                    f"{r.get('fwd_window')} | {r.get('pair_day_a')}→{r.get('pair_day_b')} |"
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
                "actual_first": out["asof_date"].min() if len(out) else None,
                "actual_last": out["asof_date"].max() if len(out) else None,
                "forward_bars": FORWARD_BARS,
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
    ap.add_argument("--start", default=None, help="YYYY-MM-DD (overrides months)")
    ap.add_argument("--end", default=None, help="YYYY-MM-DD")
    ap.add_argument(
        "--months",
        type=int,
        default=None,
        help="Lookback months from end (any integer, e.g. 24). Default 6 if start omitted.",
    )
    ap.add_argument("--ticker", default=None)
    args = ap.parse_args()
    run(start=args.start, end=args.end, months=args.months, ticker=args.ticker)


if __name__ == "__main__":
    main()
