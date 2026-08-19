"""Point-in-time daily AB checklist backfill.

Window: --months N or --start/--end (no hard cap).

Forward labels (per asof close entry, long-biased):
  1d  = next 1 session
  3d  = next 3 sessions
  1w  = next 5 sessions
  2m  = next 42 sessions (~2 months) or until data ends

MD shows 🟢/🔴 per horizon (green = max upside excursion > |max downside|).
Also writes a pattern-correlation audit: consecutive scores, spikes, SMA50, R:G.

CLI:
  python -m src.ab_backfill --months 24 --ticker AAPL
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

# horizon name -> number of future sessions
HORIZONS = {
    "1d": 1,
    "3d": 3,
    "1w": 5,
    "2m": 42,
}


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
        "target_price": np.nan,
        "target_delta": np.nan,
        "analyst_recom": np.nan,
        "insider_tx": np.nan,
        "insider_delta": np.nan,
        "inst_tx": np.nan,
        "short_float": np.nan,
        "profitable": False,
        "prior_export_date": None,
    }


def _forward_multi(ohlc, asof: str, entry: float) -> dict:
    """For each horizon: max_profit, max_loss, end_ret, fav (max_up > |max_down|)."""
    out: dict = {}
    for name, bars in HORIZONS.items():
        out[f"mp_{name}"] = np.nan
        out[f"ml_{name}"] = np.nan
        out[f"end_{name}"] = np.nan
        out[f"fav_{name}"] = np.nan  # 1 favorable asymmetry, 0 not, nan n/a
        out[f"up_{name}"] = np.nan  # 1 if end close > entry
        out[f"bars_{name}"] = 0

    if ohlc is None or ohlc.empty or not np.isfinite(entry) or entry <= 0:
        return out

    df = ohlc.copy()
    if not isinstance(df.index, pd.DatetimeIndex):
        df.index = pd.to_datetime(df.index)
    df.columns = [c.lower() for c in df.columns]
    fut_all = df[df.index > pd.Timestamp(asof)]
    if fut_all.empty:
        return out

    high_col = "high" if "high" in fut_all.columns else "close"
    low_col = "low" if "low" in fut_all.columns else "close"
    close_col = "close"

    for name, bars in HORIZONS.items():
        fut = fut_all.head(bars)
        n = len(fut)
        if n == 0:
            continue
        hi = float(fut[high_col].astype(float).max())
        lo = float(fut[low_col].astype(float).min())
        end = float(fut[close_col].astype(float).iloc[-1])
        mp = hi / entry - 1.0
        ml = lo / entry - 1.0
        er = end / entry - 1.0
        out[f"mp_{name}"] = mp
        out[f"ml_{name}"] = ml
        out[f"end_{name}"] = er
        out[f"bars_{name}"] = n
        # favorable asymmetry: upside peak larger than downside trough magnitude
        if np.isfinite(mp) and np.isfinite(ml):
            out[f"fav_{name}"] = 1.0 if mp > abs(ml) else 0.0
        out[f"up_{name}"] = 1.0 if er > 0 else 0.0
    return out


def _score_one(
    ticker,
    asof,
    ohlc_to_asof,
    ohlc_full,
    row,
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
    fwd = _forward_multi(ohlc_full, asof, float(entry) if np.isfinite(entry) else np.nan)

    # trend context from Part A if present
    above50 = a.get("above_sma50") if a.get("ok") else None
    if above50 is None and a.get("ok"):
        # try common keys
        for k in ("above_sma50", "vs_sma50", "sma50_above"):
            if k in a:
                above50 = a[k]
                break

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
        "above_sma50": above50,
        "setup_flag": setup_flag,
        "setup_val": setup_val,
        "form4_val": f4["val"],
        "form4_flag": f4["flag"],
        "export_prior": prior_date,
        "export_used": export_used or "none",
        **fwd,
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


def _dot(fav) -> str:
    if fav is None or (isinstance(fav, float) and not np.isfinite(fav)):
        return "⚪"
    return "🟢" if float(fav) >= 0.5 else "🔴"


def _streaks(series: pd.Series) -> pd.Series:
    """Length of current same-sign streak ending at each row (score>0 vs <=0)."""
    sign = (series > 0).astype(int)
    out = []
    run = 0
    prev = None
    for s in sign:
        if prev is None or s != prev:
            run = 1
        else:
            run += 1
        out.append(run if s == 1 else -run)
        prev = s
    return pd.Series(out, index=series.index)


def _pattern_audit(out: pd.DataFrame) -> list[str]:
    """Score-pattern vs forward favorability — markdown lines."""
    lines = [
        "## Pattern correlation audit",
        "",
        "🟢 = max upside excursion > |max downside| over that horizon (long from asof close).",
        "Rates below = share of rows with 🟢 among graded rows.",
        "",
    ]
    if out is None or out.empty:
        lines.append("_no rows_")
        return lines

    df = out.copy().sort_values(["Ticker", "asof_date"]).reset_index(drop=True)

    # score delta / streaks per ticker
    parts = []
    for t, g in df.groupby("Ticker", sort=False):
        g = g.copy()
        g["score_delta"] = g["score"].diff()
        g["streak"] = _streaks(g["score"])
        parts.append(g)
    df = pd.concat(parts, ignore_index=True)

    horizons = ["1d", "3d", "1w", "2m"]

    def rate(mask, h):
        col = f"fav_{h}"
        sub = df.loc[mask, col].dropna()
        if len(sub) < 5:
            return None, len(sub)
        return float((sub >= 0.5).mean()), len(sub)

    # --- score level buckets ---
    lines += ["### A. Score level vs horizon favorability", ""]
    lines.append("| score bucket | n | 1d 🟢% | 3d 🟢% | 1w 🟢% | 2m 🟢% |")
    lines.append("|--------------|--:|-------:|-------:|-------:|-------:|")
    bins = [(-99, -1), (0, 2), (3, 5), (6, 99)]
    labels = ["≤ -1", "0–2", "3–5", "≥ 6"]
    for (lo, hi), lab in zip(bins, labels):
        m = (df["score"] >= lo) & (df["score"] <= hi)
        cells = []
        n0 = int(m.sum())
        for h in horizons:
            r, n = rate(m, h)
            cells.append(f"{100*r:.0f}%" if r is not None else "—")
        lines.append(f"| {lab} | {n0} | " + " | ".join(cells) + " |")

    # --- consecutive positive / negative ---
    lines += ["", "### B. Consecutive same-sign scores (streak ending today)", ""]
    lines.append("| streak | n | 1d 🟢% | 3d 🟢% | 1w 🟢% | 2m 🟢% |")
    lines.append("|--------|--:|-------:|-------:|-------:|-------:|")
    streak_groups = [
        ("pos ≥3", df["streak"] >= 3),
        ("pos 1–2", (df["streak"] >= 1) & (df["streak"] <= 2)),
        ("neg 1–2", (df["streak"] <= -1) & (df["streak"] >= -2)),
        ("neg ≤ -3", df["streak"] <= -3),
    ]
    for lab, m in streak_groups:
        cells = []
        n0 = int(m.sum())
        for h in horizons:
            r, _ = rate(m, h)
            cells.append(f"{100*r:.0f}%" if r is not None else "—")
        lines.append(f"| {lab} | {n0} | " + " | ".join(cells) + " |")

    # --- spikes / drops ---
    lines += ["", "### C. Score spikes & drops (day-over-day Δ score)", ""]
    lines.append("| Δ score | n | 1d 🟢% | 3d 🟢% | 1w 🟢% | 2m 🟢% |")
    lines.append("|---------|--:|-------:|-------:|-------:|-------:|")
    delta_groups = [
        ("spike ≥+3", df["score_delta"] >= 3),
        ("up +1..+2", (df["score_delta"] >= 1) & (df["score_delta"] <= 2)),
        ("flat 0", df["score_delta"] == 0),
        ("down −1..−2", (df["score_delta"] <= -1) & (df["score_delta"] >= -2)),
        ("drop ≤−3", df["score_delta"] <= -3),
    ]
    for lab, m in delta_groups:
        m = m & df["score_delta"].notna()
        cells = []
        n0 = int(m.sum())
        for h in horizons:
            r, _ = rate(m, h)
            cells.append(f"{100*r:.0f}%" if r is not None else "—")
        lines.append(f"| {lab} | {n0} | " + " | ".join(cells) + " |")

    # --- SMA50 regime ---
    lines += ["", "### D. Trend regime (above vs below SMA50)", ""]
    if "above_sma50" in df.columns and df["above_sma50"].notna().any():
        lines.append("| regime | n | 1d 🟢% | 3d 🟢% | 1w 🟢% | 2m 🟢% |")
        lines.append("|--------|--:|-------:|-------:|-------:|-------:|")
        for lab, val in (("above SMA50", True), ("below SMA50", False)):
            m = df["above_sma50"] == val
            if not m.any():
                # try 1/0
                m = df["above_sma50"].astype(str).str.lower().isin(
                    ["true", "1", "yes"] if val else ["false", "0", "no"]
                )
            cells = []
            n0 = int(m.sum())
            for h in horizons:
                r, _ = rate(m, h)
                cells.append(f"{100*r:.0f}%" if r is not None else "—")
            lines.append(f"| {lab} | {n0} | " + " | ".join(cells) + " |")
        # interaction: high score + above50
        lines += ["", "| interaction | n | 1w 🟢% | 2m 🟢% |", "|-------------|--:|-------:|-------:|"]
        for lab, m in (
            ("score≥3 & above50", (df["score"] >= 3) & (df["above_sma50"] == True)),
            ("score≥3 & below50", (df["score"] >= 3) & (df["above_sma50"] == False)),
            ("score≤0 & above50", (df["score"] <= 0) & (df["above_sma50"] == True)),
            ("score≤0 & below50", (df["score"] <= 0) & (df["above_sma50"] == False)),
        ):
            r1, _ = rate(m, "1w")
            r2, _ = rate(m, "2m")
            lines.append(
                f"| {lab} | {int(m.sum())} | "
                f"{100*r1:.0f}%" if r1 is not None else f"| {lab} | {int(m.sum())} | —"
            )
            # fix formatting
    else:
        lines.append("_above_sma50 not available on this run_")

    # --- body R:G ---
    lines += ["", "### E. 2-day body red:green ratio", ""]
    if "body_rg_2day" in df.columns and df["body_rg_2day"].notna().any():
        lines.append("| body R:G | n | 1d 🟢% | 3d 🟢% | 1w 🟢% | 2m 🟢% |")
        lines.append("|----------|--:|-------:|-------:|-------:|-------:|")
        for lab, m in (
            ("R:G ≥ 1.4", df["body_rg_2day"] >= 1.4),
            ("R:G 1.0–1.4", (df["body_rg_2day"] >= 1.0) & (df["body_rg_2day"] < 1.4)),
            ("R:G < 1.0", df["body_rg_2day"] < 1.0),
        ):
            cells = []
            n0 = int(m.sum())
            for h in horizons:
                r, _ = rate(m, h)
                cells.append(f"{100*r:.0f}%" if r is not None else "—")
            lines.append(f"| {lab} | {n0} | " + " | ".join(cells) + " |")
    else:
        lines.append("_body_rg_2day not available_")

    # --- overall base rates ---
    lines += ["", "### F. Base rates (all rows)", ""]
    cells = []
    for h in horizons:
        r, n = rate(pd.Series(True, index=df.index), h)
        cells.append(f"{h}: {100*r:.0f}% (n={n})" if r is not None else f"{h}: —")
    lines.append("- " + " · ".join(cells))
    lines.append("")
    lines.append(
        "_Rule of thumb: compare bucket 🟢% to base rate. "
        "Gaps < ~5pp with small n are noise._"
    )
    return lines


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
            f"(B1 neutral). Add older finviz_YYYY-MM-DD.csv to deepen B1."
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
        exp_idx = exp_df.set_index("Ticker") if exp_df is not None else None

        for t in tickers:
            g = groups.get(t)
            if g is None:
                continue
            ohlc_to = g[g.index <= pd.Timestamp(asof)]
            if ohlc_to.empty or len(ohlc_to) < 25:
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
                f"export={n_with_export:,} ohlc_only={n_ohlc_only:,}"
            )

    out = pd.DataFrame(rows)
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    tag = ticker.upper() if ticker else "universe"
    stem = f"{start}_{end}_{tag}"
    parquet = OUT_DIR / f"{stem}.parquet"
    out.to_parquet(parquet, index=False)
    if ticker or len(out) < 500_000:
        out.to_csv(OUT_DIR / f"{stem}.csv", index=False)

    lines = [
        f"# AB PIT backfill — {start} → {end} — {tag}",
        "",
        f"- Rows: **{len(out):,}**",
        f"- Requested: `{start}` → `{end}`",
        f"- Actual span: "
        + (f"`{out['asof_date'].min()}` → `{out['asof_date'].max()}`" if len(out) else "empty"),
        f"- Finviz exports: `{first_exp}` → `{last_exp}` (n={len(exports)})",
        f"- With export B1: **{n_with_export:,}** · OHLC-only: **{n_ohlc_only:,}**",
        "",
        "## Horizon legend",
        "",
        "| col | meaning |",
        "|-----|---------|",
        "| 1d | next **1** session |",
        "| 3d | next **3** sessions |",
        "| 1w | next **5** sessions |",
        "| 2m | next **42** sessions (~2 months, or until last bar) |",
        "| 🟢 | max upside > \|max downside\| over that window (favorable for longs) |",
        "| 🔴 | max downside magnitude ≥ max upside |",
        "| ⚪ | not enough future bars |",
        "",
    ]

    if len(out) and ticker:
        lines += [
            f"## {ticker} — daily trail",
            "",
            "| date | score | RSI | 1d | 3d | 1w | 2m | setup | mode |",
            "|------|------:|----:|:--:|:--:|:--:|:--:|:----:|------|",
        ]
        for _, r in out.sort_values("asof_date").iterrows():
            rsi = r["rsi"] if np.isfinite(r.get("rsi", np.nan)) else float("nan")
            lines.append(
                f"| {r['asof_date']} | {int(r['score']):+d} | {rsi:.1f} | "
                f"{_dot(r.get('fav_1d'))} | {_dot(r.get('fav_3d'))} | "
                f"{_dot(r.get('fav_1w'))} | {_dot(r.get('fav_2m'))} | "
                f"{int(r['setup_flag'])} | {r.get('score_mode')} |"
            )

    # pattern audit always when we have rows
    if len(out):
        lines.append("")
        lines.extend(_pattern_audit(out))

    lines += ["", f"- parquet: `{parquet.relative_to(ROOT)}`"]
    md = OUT_DIR / f"{stem}.md"
    md.write_text("\n".join(lines), encoding="utf-8")

    meta = {
        "start": start,
        "end": end,
        "months_arg": months,
        "ticker": ticker,
        "n_rows": int(len(out)),
        "n_with_export": n_with_export,
        "n_ohlc_only": n_ohlc_only,
        "export_first": first_exp,
        "export_last": last_exp,
        "horizons": HORIZONS,
        "actual_first": out["asof_date"].min() if len(out) else None,
        "actual_last": out["asof_date"].max() if len(out) else None,
        "generated": datetime.now(ET).isoformat(),
    }
    (OUT_DIR / f"{stem}.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")
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
