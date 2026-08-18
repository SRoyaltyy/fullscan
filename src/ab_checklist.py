"""Part A (OHLC chart) + Part B1 (Finviz export snapshot/deltas) daily checklist.

Universe gate (your rules):
  Market Cap > 80e6
  Average Volume > 500_000

CLI:
  python -m src.ab_checklist
  python -m src.ab_checklist --date 2026-08-18
  python -m src.ab_checklist --date 2026-08-18 --top 30
"""
from __future__ import annotations

import argparse
import json
import re
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

from . import config
from . import price_store as ps

ROOT = Path(__file__).resolve().parent.parent
EXPORT_DIR = ROOT / "data" / "exports"
OUT_DIR = ROOT / "data" / "ab_checklist"
ET = ZoneInfo(config.TZ)

MCAP_MIN = 80_000_000.0
ADV_MIN = 500_000.0
LOOKBACK_BODY = 10
LOOKBACK_DD = 42  # ~2 months trading sessions


def _num(x) -> float:
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return np.nan
    if isinstance(x, (int, float, np.integer, np.floating)):
        return float(x)
    s = str(x).strip().replace(",", "").replace("$", "").replace("%", "")
    if not s or s in {"-", "—", "N/A", "NA", "None", "nan"}:
        return np.nan
    m = re.match(r"^([+-]?\d*\.?\d+)\s*([KMBTkmbt])?$", s)
    if not m:
        try:
            return float(s)
        except ValueError:
            return np.nan
    v = float(m.group(1))
    suf = (m.group(2) or "").upper()
    mult = {"": 1.0, "K": 1e3, "M": 1e6, "B": 1e9, "T": 1e12}[suf]
    return v * mult


def _load_export(date: str | None) -> tuple[pd.DataFrame, str, Path]:
    files = sorted(EXPORT_DIR.glob("finviz_????-??-??.csv"))
    if not files:
        raise SystemExit("[ab] no finviz exports under data/exports/")
    if date:
        exact = EXPORT_DIR / f"finviz_{date}.csv"
        if exact.exists():
            path = exact
        else:
            ok = [f for f in files if f.stem.replace("finviz_", "") <= date]
            path = ok[-1] if ok else files[-1]
    else:
        path = files[-1]
    asof = path.stem.replace("finviz_", "")
    df = pd.read_csv(path, low_memory=False)
    tcol = "Ticker" if "Ticker" in df.columns else df.columns[0]
    df["Ticker"] = df[tcol].astype(str).str.strip().str.upper()
    df = df.drop_duplicates("Ticker", keep="first")
    return df, asof, path


def _prior_export(asof: str) -> pd.DataFrame | None:
    files = sorted(EXPORT_DIR.glob("finviz_????-??-??.csv"))
    prev = [f for f in files if f.stem.replace("finviz_", "") < asof]
    if not prev:
        return None
    df = pd.read_csv(prev[-1], low_memory=False)
    tcol = "Ticker" if "Ticker" in df.columns else df.columns[0]
    df["Ticker"] = df[tcol].astype(str).str.strip().str.upper()
    return df.drop_duplicates("Ticker", keep="first").set_index("Ticker")


def _filter_liquid(df: pd.DataFrame) -> pd.DataFrame:
    mcap_col = "Market Cap" if "Market Cap" in df.columns else None
    vol_col = None
    for c in ("Average Volume", "Avg Volume", "Volume"):
        if c in df.columns:
            vol_col = c
            break
    if not mcap_col or not vol_col:
        raise SystemExit(f"[ab] need Market Cap + Average Volume columns; have {list(df.columns)[:20]}")
    out = df.copy()
    out["_mcap"] = out[mcap_col].map(_num)
    out["_adv"] = out[vol_col].map(_num)
    out = out[(out["_mcap"] > MCAP_MIN) & (out["_adv"] > ADV_MIN)].copy()
    return out


def _rsi(close: pd.Series, n: int = 14) -> pd.Series:
    delta = close.diff()
    gain = delta.clip(lower=0.0)
    loss = -delta.clip(upper=0.0)
    avg_gain = gain.ewm(alpha=1 / n, min_periods=n, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / n, min_periods=n, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))


def _sma(s: pd.Series, n: int) -> pd.Series:
    return s.rolling(n, min_periods=n).mean()


def _part_a(ohlc: pd.DataFrame) -> dict:
    """Chart features from OHLC. Index = date."""
    empty = {"ok": False}
    if ohlc is None or ohlc.empty:
        return empty
    df = ohlc.copy()
    df.columns = [c.lower() for c in df.columns]
    need = {"open", "high", "low", "close"}
    if not need.issubset(df.columns):
        return empty
    if not isinstance(df.index, pd.DatetimeIndex):
        df.index = pd.to_datetime(df.index)
    df = df.sort_index().dropna(subset=["close"])
    if len(df) < 25:
        return {"ok": False, "n_bars": len(df)}

    c = df["close"].astype(float)
    o = df["open"].astype(float)
    h = df["high"].astype(float)
    l = df["low"].astype(float)
    v = df["volume"].astype(float) if "volume" in df.columns else pd.Series(np.nan, index=df.index)

    rsi = _rsi(c, 14)
    rsi_now = float(rsi.iloc[-1]) if len(rsi) else np.nan
    rsi_prev = float(rsi.iloc[-2]) if len(rsi) > 1 else np.nan

    def _cross(level: float) -> str:
        if not (np.isfinite(rsi_now) and np.isfinite(rsi_prev)):
            return "none"
        if rsi_prev < level <= rsi_now:
            return "up"
        if rsi_prev > level >= rsi_now:
            return "down"
        return "none"

    tail = df.tail(LOOKBACK_BODY)
    body = tail["close"] - tail["open"]
    body_g = float(body[body > 0].sum()) if (body > 0).any() else 0.0
    body_r = float((-body[body < 0]).sum()) if (body < 0).any() else 0.0
    body_rg = body_g / body_r if body_r > 1e-12 else (10.0 if body_g > 0 else 1.0)

    up_mask = tail["close"] >= tail["open"]
    vol_g = float(tail.loc[up_mask, "volume"].sum()) if "volume" in tail.columns else np.nan
    vol_r = float(tail.loc[~up_mask, "volume"].sum()) if "volume" in tail.columns else np.nan
    vol_rg = (vol_g / vol_r) if (np.isfinite(vol_r) and vol_r > 0) else np.nan

    avg_vol20 = float(v.tail(20).mean()) if v.notna().sum() >= 5 else np.nan
    rvol = float(v.iloc[-1] / avg_vol20) if (np.isfinite(avg_vol20) and avg_vol20 > 0) else np.nan

    sma20 = _sma(c, 20)
    mid = sma20
    std = c.rolling(20, min_periods=20).std()
    upper = mid + 2 * std
    lower = mid - 2 * std
    c_now = float(c.iloc[-1])
    mid_n, up_n, lo_n = float(mid.iloc[-1]), float(upper.iloc[-1]), float(lower.iloc[-1])
    if np.isfinite(up_n) and np.isfinite(lo_n) and (up_n - lo_n) > 0:
        bb_pos = (c_now - mid_n) / ((up_n - lo_n) / 2)  # 0 at mid, ~±1 at bands
    else:
        bb_pos = np.nan

    sma50 = _sma(c, 50)
    sma80 = _sma(c, 80)
    s20, s50, s80 = float(sma20.iloc[-1]), float(sma50.iloc[-1]), float(sma80.iloc[-1])
    above_sma50 = bool(c_now >= s50) if np.isfinite(s50) else None
    dist_sma50 = (c_now / s50 - 1.0) if (np.isfinite(s50) and s50 > 0) else np.nan

    if all(np.isfinite(x) for x in (s20, s50, s80)):
        if s20 > s50 > s80:
            sma_stack = "bull_aligned"
        elif s20 < s50 < s80:
            sma_stack = "bear_aligned"
        else:
            sma_stack = "mixed"
    else:
        sma_stack = "unknown"

    # max downside past ~2 months: peak-to-trough within window ending today
    win = c.tail(LOOKBACK_DD)
    if len(win) >= 5:
        peak = float(win.max())
        peak_i = int(win.values.argmax())
        after = win.iloc[peak_i:]
        trough = float(after.min())
        max_dd = trough / peak - 1.0 if peak > 0 else np.nan
        dd_peak_date = win.index[peak_i].date().isoformat()
        dd_trough_date = after.index[int(after.values.argmin())].date().isoformat()
    else:
        max_dd = np.nan
        dd_peak_date = dd_trough_date = None

    # body vs wick fractions (lookback)
    rng = (tail["high"] - tail["low"]).replace(0, np.nan)
    body_abs = (tail["close"] - tail["open"]).abs()
    upper_w = tail["high"] - tail[["open", "close"]].max(axis=1)
    lower_w = tail[["open", "close"]].min(axis=1) - tail["low"]
    g = tail["close"] > tail["open"]
    r = tail["close"] < tail["open"]

    def _frac(mask, num, den):
        if mask.sum() == 0:
            return np.nan
        n = float(num[mask].sum())
        d = float(den[mask].sum())
        return n / d if d > 0 else np.nan

    g_body_frac = _frac(g, body_abs, rng)
    r_body_frac = _frac(r, body_abs, rng)
    g_wick_frac = _frac(g, upper_w + lower_w, rng)
    r_wick_frac = _frac(r, upper_w + lower_w, rng)

    return {
        "ok": True,
        "n_bars": int(len(df)),
        "price": c_now,
        "rsi": rsi_now,
        "rsi_prev": rsi_prev,
        "rsi_x30": _cross(30),
        "rsi_x50": _cross(50),
        "rsi_x70": _cross(70),
        "body_green": body_g,
        "body_red": body_r,
        "body_rg": body_rg,
        "vol_green": vol_g,
        "vol_red": vol_r,
        "vol_rg": vol_rg,
        "rvol": rvol,
        "bb_pos": bb_pos,
        "above_sma50": above_sma50,
        "dist_sma50": dist_sma50,
        "sma20": s20,
        "sma50": s50,
        "sma80": s80,
        "sma_stack": sma_stack,
        "max_dd_2m": max_dd,
        "dd_peak_date": dd_peak_date,
        "dd_trough_date": dd_trough_date,
        "g_body_frac": g_body_frac,
        "r_body_frac": r_body_frac,
        "g_wick_frac": g_wick_frac,
        "r_wick_frac": r_wick_frac,
    }


def _pass_a(a: dict) -> dict:
    """Pass (+1) / fail (-1) / neutral (0) for Part A — long-biased prototype."""
    if not a.get("ok"):
        return {k: 0 for k in (
            "A1_rsi_zone", "A2_rsi_cross", "A3_body_rg", "A4_vol_rg", "A5_rvol",
            "A6_bb", "A7_sma50", "A8_sma_stack", "A9_max_dd", "A10_body_wick",
        )}

    rsi = a.get("rsi")
    # A1: not extended (>70 bad for new long; <30 oversold = opportunity flag +)
    if not np.isfinite(rsi):
        a1 = 0
    elif rsi >= 70:
        a1 = -1
    elif rsi <= 30:
        a1 = 1
    else:
        a1 = 0

    # A2: cross up 30/50 good; cross down 70 good (cooling); cross down 30 bad
    xs = (a.get("rsi_x30"), a.get("rsi_x50"), a.get("rsi_x70"))
    if a.get("rsi_x30") == "up" or a.get("rsi_x50") == "up":
        a2 = 1
    elif a.get("rsi_x70") == "down":
        a2 = 1
    elif a.get("rsi_x30") == "down" or a.get("rsi_x70") == "up":
        a2 = -1
    else:
        a2 = 0

    body_rg = a.get("body_rg")
    a3 = 1 if (np.isfinite(body_rg) and body_rg > 1.0) else (-1 if (np.isfinite(body_rg) and body_rg < 0.8) else 0)

    vol_rg = a.get("vol_rg")
    a4 = 1 if (np.isfinite(vol_rg) and vol_rg > 1.0) else (-1 if (np.isfinite(vol_rg) and vol_rg < 0.8) else 0)

    rvol = a.get("rvol")
    a5 = 1 if (np.isfinite(rvol) and rvol >= 1.5) else (0 if (np.isfinite(rvol) and rvol >= 0.7) else (-1 if np.isfinite(rvol) else 0))

    bb = a.get("bb_pos")
    if not np.isfinite(bb):
        a6 = 0
    elif bb <= -0.8:
        a6 = 1  # near/below lower band
    elif bb >= 0.8:
        a6 = -1  # near/above upper
    else:
        a6 = 0

    a7 = 1 if a.get("above_sma50") is True else (-1 if a.get("above_sma50") is False else 0)

    stack = a.get("sma_stack")
    a8 = 1 if stack == "bull_aligned" else (-1 if stack == "bear_aligned" else 0)

    dd = a.get("max_dd_2m")
    # moderate washout without total collapse
    if not np.isfinite(dd):
        a9 = 0
    elif -0.25 <= dd <= -0.08:
        a9 = 1
    elif dd < -0.40:
        a9 = -1
    else:
        a9 = 0

    gbf, rbf = a.get("g_body_frac"), a.get("r_body_frac")
    if np.isfinite(gbf) and np.isfinite(rbf):
        a10 = 1 if gbf > rbf else (-1 if rbf > gbf + 0.05 else 0)
    else:
        a10 = 0

    return {
        "A1_rsi_zone": a1,
        "A2_rsi_cross": a2,
        "A3_body_rg": a3,
        "A4_vol_rg": a4,
        "A5_rvol": a5,
        "A6_bb": a6,
        "A7_sma50": a7,
        "A8_sma_stack": a8,
        "A9_max_dd": a9,
        "A10_body_wick": a10,
    }


def _part_b1(row: pd.Series, prior: pd.Series | None) -> dict:
    def g(col):
        return _num(row[col]) if col in row.index else np.nan

    def gp(col):
        if prior is None or col not in prior.index:
            return np.nan
        return _num(prior[col])

    rsi_f = g("Relative Strength Index (14)")
    rvol_f = g("Relative Volume")
    sma50_f = g("50-Day Simple Moving Average")  # finviz is often % distance
    short_float = g("Short Float")
    short_ratio = g("Short Ratio")
    target = g("Target Price")
    recom = g("Analyst Recom")
    eps_surp = g("EPS Surprise")
    rev_surp = g("Revenue Surprise")
    sales = g("Sales")
    income = g("Income")
    insider_tx = g("Insider Transactions")
    inst_tx = g("Institutional Transactions")
    profit_m = g("Profit Margin")
    eps_ttm = g("EPS (ttm)")

    target_prev = gp("Target Price")
    insider_prev = gp("Insider Transactions")
    recom_prev = gp("Analyst Recom")

    target_delta = target - target_prev if np.isfinite(target) and np.isfinite(target_prev) else np.nan
    insider_delta = insider_tx - insider_prev if np.isfinite(insider_tx) and np.isfinite(insider_prev) else np.nan
    recom_delta = recom - recom_prev if np.isfinite(recom) and np.isfinite(recom_prev) else np.nan

    # earnings date proximity
    ed = row.get("Earnings Date", "")
    earn_soon = False
    if isinstance(ed, str) and ed and ed not in {"-", "nan"}:
        earn_soon = True  # presence flag; exact day parse varies

    profitable = bool(np.isfinite(income) and income > 0) or bool(np.isfinite(profit_m) and profit_m > 0)

    return {
        "fv_rsi": rsi_f,
        "fv_rvol": rvol_f,
        "fv_sma50_pct": sma50_f,
        "short_float": short_float,
        "short_ratio": short_ratio,
        "target_price": target,
        "target_delta": target_delta,
        "analyst_recom": recom,
        "recom_delta": recom_delta,
        "eps_surprise": eps_surp,
        "rev_surprise": rev_surp,
        "sales": sales,
        "income": income,
        "profit_margin": profit_m,
        "eps_ttm": eps_ttm,
        "insider_tx": insider_tx,
        "insider_delta": insider_delta,
        "inst_tx": inst_tx,
        "profitable": profitable,
        "earnings_date_raw": str(ed) if ed is not None else "",
        "earn_date_present": earn_soon,
    }


def _pass_b1(b: dict) -> dict:
    # B1: export snapshot polarity (long-biased prototype)
    def _surp(x):
        if not np.isfinite(x):
            return 0
        return 1 if x > 0 else (-1 if x < 0 else 0)

    tdelta = b.get("target_delta")
    b_target = 1 if (np.isfinite(tdelta) and tdelta > 0) else (-1 if (np.isfinite(tdelta) and tdelta < 0) else 0)

    recom = b.get("analyst_recom")
    # Finviz: 1=strong buy … 5=sell
    if not np.isfinite(recom):
        b_recom = 0
    elif recom <= 2.5:
        b_recom = 1
    elif recom >= 3.5:
        b_recom = -1
    else:
        b_recom = 0

    idelta = b.get("insider_delta")
    b_ins = 1 if (np.isfinite(idelta) and idelta > 0) else (-1 if (np.isfinite(idelta) and idelta < 0) else 0)

    inst = b.get("inst_tx")
    b_inst = 1 if (np.isfinite(inst) and inst > 0) else (-1 if (np.isfinite(inst) and inst < 0) else 0)

    b_prof = 1 if b.get("profitable") else -1

    sf = b.get("short_float")
    b_short = 0
    if np.isfinite(sf):
        if sf >= 20:
            b_short = 1  # fuel (squeeze potential) — soft +
        elif sf >= 10:
            b_short = 0
        else:
            b_short = 0

    return {
        "B1_eps_surprise": _surp(b.get("eps_surprise")),
        "B1_rev_surprise": _surp(b.get("rev_surprise")),
        "B1_target_delta": b_target,
        "B1_analyst_recom": b_recom,
        "B1_insider_delta": b_ins,
        "B1_inst_tx": b_inst,
        "B1_profitable": b_prof,
        "B1_short_fuel": b_short,
    }


def run(date: str | None = None, top: int = 40) -> pd.DataFrame:
    finviz, asof, path = _load_export(date)
    liquid = _filter_liquid(finviz)
    print(f"[ab] export={path.name} asof={asof} liquid={len(liquid):,} (mcap>{MCAP_MIN:.0f} adv>{ADV_MIN:.0f})")

    prior_df = _prior_export(asof)
    print(f"[ab] prior export for deltas: {'yes' if prior_df is not None else 'no'}")

    store = ps._load_store()
    if not len(store):
        raise SystemExit("[ab] price store empty — run: python -m src.price_store bootstrap")
    store = store[store["date"] <= pd.Timestamp(asof)]
    tickers = set(liquid["Ticker"])
    store = store[store["ticker"].isin(tickers)]
    groups = {t: g.set_index("date").sort_index() for t, g in store.groupby("ticker")}

    rows = []
    for _, row in liquid.iterrows():
        t = row["Ticker"]
        a = _part_a(groups.get(t))
        prior = prior_df.loc[t] if prior_df is not None and t in prior_df.index else None
        b = _part_b1(row, prior)
        pa = _pass_a(a)
        pb = _pass_b1(b)
        score = int(sum(pa.values()) + sum(pb.values()))
        n_pos = sum(1 for x in list(pa.values()) + list(pb.values()) if x > 0)
        n_neg = sum(1 for x in list(pa.values()) + list(pb.values()) if x < 0)

        rec = {
            "Ticker": t,
            "asof_date": asof,
            "Sector": row.get("Sector", ""),
            "Industry": row.get("Industry", ""),
            "mcap": row.get("_mcap"),
            "adv": row.get("_adv"),
            "price": a.get("price", _num(row.get("Price"))),
            "score": score,
            "n_pos": n_pos,
            "n_neg": n_neg,
            # A values
            "rsi": a.get("rsi"),
            "rsi_x30": a.get("rsi_x30"),
            "rsi_x50": a.get("rsi_x50"),
            "rsi_x70": a.get("rsi_x70"),
            "body_rg": a.get("body_rg"),
            "vol_rg": a.get("vol_rg"),
            "rvol": a.get("rvol"),
            "bb_pos": a.get("bb_pos"),
            "above_sma50": a.get("above_sma50"),
            "dist_sma50": a.get("dist_sma50"),
            "sma_stack": a.get("sma_stack"),
            "max_dd_2m": a.get("max_dd_2m"),
            "g_body_frac": a.get("g_body_frac"),
            "r_body_frac": a.get("r_body_frac"),
            # B values
            "eps_surprise": b.get("eps_surprise"),
            "rev_surprise": b.get("rev_surprise"),
            "target_price": b.get("target_price"),
            "target_delta": b.get("target_delta"),
            "analyst_recom": b.get("analyst_recom"),
            "insider_tx": b.get("insider_tx"),
            "insider_delta": b.get("insider_delta"),
            "inst_tx": b.get("inst_tx"),
            "profitable": b.get("profitable"),
            "short_float": b.get("short_float"),
            "earnings_date_raw": b.get("earnings_date_raw"),
        }
        rec.update({f"pass_{k}": v for k, v in pa.items()})
        rec.update({f"pass_{k}": v for k, v in pb.items()})
        rows.append(rec)

    out = pd.DataFrame(rows).sort_values("score", ascending=False)
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    csv_path = OUT_DIR / f"{asof}_ab_checklist.csv"
    out.to_csv(csv_path, index=False)

    # human summary
    lines = [
        f"# A+B1 Checklist — {asof}",
        "",
        f"- Gate: Market Cap > ${MCAP_MIN/1e6:.0f}M · ADV > {ADV_MIN:,.0f}",
        f"- Liquid names scored: **{len(out):,}**",
        f"- Export: `{path.name}` · prior deltas: {'yes' if prior_df is not None else 'no'}",
        "",
        "## Score legend",
        "",
        "Each item: **+1** pass / **0** neutral / **-1** fail (long-biased prototype).",
        "`score` = sum of all Part A + B1 item scores.",
        "",
        "### Part A (OHLC)",
        "A1 RSI zone · A2 RSI cross · A3 body R:G · A4 vol R:G · A5 RVOL ·",
        "A6 Bollinger pos · A7 vs SMA50 · A8 SMA20>50>80 · A9 max DD 2m · A10 body/wick",
        "",
        "### Part B1 (Finviz export)",
        "EPS/Rev surprise · target Δ · analyst recom · insider Δ · inst tx · profitable · short fuel",
        "",
        f"## Top {top} by score",
        "",
        "| Ticker | Score | +/− | RSI | stack | above50 | bodyRG | RVOL | prof | tgtΔ | insΔ | Industry |",
        "|--------|------:|----:|----:|-------|---------|-------:|-----:|:----:|-----:|-----:|----------|",
    ]
    for _, r in out.head(top).iterrows():
        lines.append(
            f"| {r['Ticker']} | {r['score']:+d} | {int(r['n_pos'])}/{int(r['n_neg'])} | "
            f"{r['rsi'] if pd.notna(r['rsi']) else float('nan'):.1f} | {r['sma_stack']} | "
            f"{r['above_sma50']} | {r['body_rg'] if pd.notna(r['body_rg']) else float('nan'):.2f} | "
            f"{r['rvol'] if pd.notna(r['rvol']) else float('nan'):.2f} | "
            f"{r['profitable']} | "
            f"{r['target_delta'] if pd.notna(r['target_delta']) else float('nan'):.2f} | "
            f"{r['insider_delta'] if pd.notna(r['insider_delta']) else float('nan'):.2f} | "
            f"{str(r['Industry'])[:28]} |"
        )
    lines += [
        "",
        f"## Bottom {min(15, len(out))} by score",
        "",
        "| Ticker | Score | RSI | stack | prof | Industry |",
        "|--------|------:|----:|-------|:----:|----------|",
    ]
    for _, r in out.tail(min(15, len(out))).iloc[::-1].iterrows():
        lines.append(
            f"| {r['Ticker']} | {r['score']:+d} | "
            f"{r['rsi'] if pd.notna(r['rsi']) else float('nan'):.1f} | {r['sma_stack']} | "
            f"{r['profitable']} | {str(r['Industry'])[:28]} |"
        )
    lines += ["", f"Full CSV: `data/ab_checklist/{asof}_ab_checklist.csv`", ""]
    md_path = OUT_DIR / f"{asof}_ab_checklist.md"
    md_path.write_text("\n".join(lines), encoding="utf-8")

    meta = {
        "asof": asof,
        "n_liquid": int(len(out)),
        "mcap_min": MCAP_MIN,
        "adv_min": ADV_MIN,
        "generated": datetime.now(ET).isoformat(),
        "csv": str(csv_path.relative_to(ROOT)),
        "top5": out.head(5)[["Ticker", "score", "Industry"]].to_dict("records"),
    }
    (OUT_DIR / f"{asof}_ab_checklist.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")

    print("\n".join(lines[:30]))
    print(f"[ab] wrote {csv_path}")
    print(f"[ab] wrote {md_path}")
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description="Part A + B1 liquid checklist")
    ap.add_argument("--date", default=None, help="YYYY-MM-DD (default: latest finviz export)")
    ap.add_argument("--top", type=int, default=40)
    args = ap.parse_args()
    run(date=args.date, top=args.top)


if __name__ == "__main__":
    main()
