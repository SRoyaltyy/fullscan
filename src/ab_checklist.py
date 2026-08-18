"""Part A (OHLC) + Part B1 (Finviz export) — feature checklist 1:1 with the agreed list.

Gate: Market Cap > $80M, Average Volume > 500k shares
Finviz units: Market Cap = millions USD; Average Volume = thousands of shares

CLI:
  python -m src.ab_checklist
  python -m src.ab_checklist --date 2026-08-18 --top 20
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
LOOKBACK_DD = 42

# Exact checklist labels (order = output order)
FEATURE_ORDER = [
    # Part A — OHLC
    "A01_rsi_value",
    "A02_rsi_cross_30",
    "A03_rsi_cross_50",
    "A04_rsi_cross_70",
    "A05_body_red_green_ratio",
    "A06_volume_red_green_ratio",
    "A07_rvol",
    "A08_bollinger_position",
    "A09_above_sma50",
    "A10_sma20_50_80_stack",
    "A11_max_downside_2m",
    "A12_green_body_vs_wick",
    "A13_red_body_vs_wick",
    # Part B1 — Finviz export (no web)
    "B01_eps_surprise",
    "B02_revenue_surprise",
    "B03_sales",
    "B04_income",
    "B05_profit_margin",
    "B06_profitable",
    "B07_target_price",
    "B08_target_price_delta",
    "B09_analyst_recom",
    "B10_insider_transactions",
    "B11_insider_tx_delta",
    "B12_institutional_transactions",
    "B13_short_float",
    "B14_earnings_date",
]


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
    return v * {"": 1.0, "K": 1e3, "M": 1e6, "B": 1e9, "T": 1e12}[suf]


def _flag(v: int) -> str:
    if v > 0:
        return "GOOD"
    if v < 0:
        return "BAD"
    return "NEUTRAL"


def _load_export(date: str | None):
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
    return df.drop_duplicates("Ticker", keep="first"), asof, path


def _prior_export(asof: str):
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
    vol_col = next((c for c in ("Average Volume", "Avg Volume") if c in df.columns), None)
    if not mcap_col or not vol_col:
        raise SystemExit(f"[ab] missing Market Cap / Average Volume; cols={list(df.columns)[:25]}")
    out = df.copy()
    raw_m = out[mcap_col].map(_num)
    raw_a = out[vol_col].map(_num)
    out["_mcap"] = raw_m * 1_000_000.0
    out["_adv"] = raw_a * 1_000.0
    filtered = out[(out["_mcap"] > MCAP_MIN) & (out["_adv"] > ADV_MIN)].copy()
    print(f"[ab] liquid gate: {len(out):,} → {len(filtered):,} (mcap>$80M adv>500k shares)")
    return filtered


def _rsi(close: pd.Series, n: int = 14) -> pd.Series:
    delta = close.diff()
    gain = delta.clip(lower=0.0)
    loss = -delta.clip(upper=0.0)
    avg_g = gain.ewm(alpha=1 / n, min_periods=n, adjust=False).mean()
    avg_l = loss.ewm(alpha=1 / n, min_periods=n, adjust=False).mean()
    rs = avg_g / avg_l.replace(0, np.nan)
    return 100 - (100 / (1 + rs))


def _sma(s: pd.Series, n: int) -> pd.Series:
    return s.rolling(n, min_periods=n).mean()


def _cross(prev: float, now: float, level: float) -> str:
    if not (np.isfinite(prev) and np.isfinite(now)):
        return "none"
    if prev < level <= now:
        return "cross_up"
    if prev > level >= now:
        return "cross_down"
    if now > level:
        return "above"
    if now < level:
        return "below"
    return "at"


def _part_a(ohlc: pd.DataFrame) -> dict:
    """Return raw feature values for Part A."""
    empty = {"ok": False}
    if ohlc is None or ohlc.empty:
        return empty
    df = ohlc.copy()
    df.columns = [c.lower() for c in df.columns]
    if not {"open", "high", "low", "close"}.issubset(df.columns):
        return empty
    if not isinstance(df.index, pd.DatetimeIndex):
        df.index = pd.to_datetime(df.index)
    df = df.sort_index().dropna(subset=["close"])
    if len(df) < 25:
        return {"ok": False, "n_bars": len(df)}

    c = df["close"].astype(float)
    v = df["volume"].astype(float) if "volume" in df.columns else pd.Series(np.nan, index=df.index)
    rsi = _rsi(c, 14)
    rsi_now = float(rsi.iloc[-1])
    rsi_prev = float(rsi.iloc[-2]) if len(rsi) > 1 else np.nan

    tail = df.tail(LOOKBACK_BODY)
    body = tail["close"] - tail["open"]
    body_g = float(body[body > 0].sum()) if (body > 0).any() else 0.0
    body_r = float((-body[body < 0]).sum()) if (body < 0).any() else 0.0
    body_rg = body_g / body_r if body_r > 1e-12 else (99.0 if body_g > 0 else 1.0)

    up = tail["close"] >= tail["open"]
    vol_g = float(tail.loc[up, "volume"].sum()) if "volume" in tail.columns else np.nan
    vol_r = float(tail.loc[~up, "volume"].sum()) if "volume" in tail.columns else np.nan
    vol_rg = vol_g / vol_r if (np.isfinite(vol_r) and vol_r > 0) else np.nan

    avg20 = float(v.tail(20).mean()) if v.notna().sum() >= 5 else np.nan
    rvol = float(v.iloc[-1] / avg20) if (np.isfinite(avg20) and avg20 > 0) else np.nan

    sma20 = _sma(c, 20)
    mid = sma20
    std = c.rolling(20, min_periods=20).std()
    upper, lower = mid + 2 * std, mid - 2 * std
    c_now = float(c.iloc[-1])
    mid_n, up_n, lo_n = float(mid.iloc[-1]), float(upper.iloc[-1]), float(lower.iloc[-1])
    bb_pos = (c_now - mid_n) / ((up_n - lo_n) / 2) if (np.isfinite(up_n) and up_n != lo_n) else np.nan

    s20 = float(sma20.iloc[-1])
    s50 = float(_sma(c, 50).iloc[-1])
    s80 = float(_sma(c, 80).iloc[-1])
    above50 = bool(c_now >= s50) if np.isfinite(s50) else None
    dist50 = (c_now / s50 - 1.0) if (np.isfinite(s50) and s50 > 0) else np.nan

    if all(np.isfinite(x) for x in (s20, s50, s80)):
        if s20 > s50 > s80:
            stack = "bull_aligned_20>50>80"
        elif s20 < s50 < s80:
            stack = "bear_aligned_20<50<80"
        else:
            stack = f"mixed_20={s20:.2f}_50={s50:.2f}_80={s80:.2f}"
    else:
        stack = "unknown"

    win = c.tail(LOOKBACK_DD)
    if len(win) >= 5:
        peak_i = int(win.values.argmax())
        peak = float(win.iloc[peak_i])
        after = win.iloc[peak_i:]
        trough_i = int(after.values.argmin())
        trough = float(after.iloc[trough_i])
        max_dd = trough / peak - 1.0 if peak > 0 else np.nan
        dd_peak = win.index[peak_i].date().isoformat()
        dd_trough = after.index[trough_i].date().isoformat()
    else:
        max_dd = np.nan
        dd_peak = dd_trough = None

    rng = (tail["high"] - tail["low"]).replace(0, np.nan)
    body_abs = (tail["close"] - tail["open"]).abs()
    upper_w = tail["high"] - tail[["open", "close"]].max(axis=1)
    lower_w = tail[["open", "close"]].min(axis=1) - tail["low"]
    gmask = tail["close"] > tail["open"]
    rmask = tail["close"] < tail["open"]

    def frac(mask, num, den):
        if mask.sum() == 0:
            return np.nan
        n, d = float(num[mask].sum()), float(den[mask].sum())
        return n / d if d > 0 else np.nan

    g_body = frac(gmask, body_abs, rng)
    r_body = frac(rmask, body_abs, rng)
    g_wick = frac(gmask, upper_w + lower_w, rng)
    r_wick = frac(rmask, upper_w + lower_w, rng)

    return {
        "ok": True,
        "n_bars": int(len(df)),
        "price": c_now,
        "rsi": rsi_now,
        "rsi_prev": rsi_prev,
        "cross_30": _cross(rsi_prev, rsi_now, 30),
        "cross_50": _cross(rsi_prev, rsi_now, 50),
        "cross_70": _cross(rsi_prev, rsi_now, 70),
        "body_rg": body_rg,
        "body_g": body_g,
        "body_r": body_r,
        "vol_rg": vol_rg,
        "rvol": rvol,
        "bb_pos": bb_pos,
        "above_sma50": above50,
        "dist_sma50": dist50,
        "sma20": s20,
        "sma50": s50,
        "sma80": s80,
        "sma_stack": stack,
        "max_dd_2m": max_dd,
        "dd_peak_date": dd_peak,
        "dd_trough_date": dd_trough,
        "g_body_frac": g_body,
        "g_wick_frac": g_wick,
        "r_body_frac": r_body,
        "r_wick_frac": r_wick,
        "window_start": tail.index[0].date().isoformat(),
        "window_end": tail.index[-1].date().isoformat(),
    }


def _pass_a(a: dict) -> dict:
    """GOOD=+1 BAD=-1 NEUTRAL=0 for each Part A feature."""
    z = {k: 0 for k in FEATURE_ORDER if k.startswith("A")}
    if not a.get("ok"):
        return z

    rsi = a["rsi"]
    # A01 absolute RSI: oversold opportunity +, overbought extension -
    if np.isfinite(rsi):
        if rsi <= 30:
            z["A01_rsi_value"] = 1
        elif rsi >= 70:
            z["A01_rsi_value"] = -1

    def cross_score(state: str, level: int) -> int:
        # cross_up into constructive zone = good; cross_down through support = bad
        if state == "cross_up":
            return 1
        if state == "cross_down":
            return -1 if level in (30, 50) else 1  # cross down 70 = cooling = mild good
        return 0

    z["A02_rsi_cross_30"] = cross_score(a["cross_30"], 30)
    z["A03_rsi_cross_50"] = cross_score(a["cross_50"], 50)
    z["A04_rsi_cross_70"] = cross_score(a["cross_70"], 70)

    br = a["body_rg"]
    if np.isfinite(br):
        z["A05_body_red_green_ratio"] = 1 if br > 1.0 else (-1 if br < 0.8 else 0)

    vr = a["vol_rg"]
    if np.isfinite(vr):
        z["A06_volume_red_green_ratio"] = 1 if vr > 1.0 else (-1 if vr < 0.8 else 0)

    rv = a["rvol"]
    if np.isfinite(rv):
        z["A07_rvol"] = 1 if rv >= 1.5 else (-1 if rv < 0.5 else 0)

    bb = a["bb_pos"]
    if np.isfinite(bb):
        z["A08_bollinger_position"] = 1 if bb <= -0.8 else (-1 if bb >= 0.8 else 0)

    if a["above_sma50"] is True:
        z["A09_above_sma50"] = 1
    elif a["above_sma50"] is False:
        z["A09_above_sma50"] = -1

    st = a["sma_stack"] or ""
    if st.startswith("bull_aligned"):
        z["A10_sma20_50_80_stack"] = 1
    elif st.startswith("bear_aligned"):
        z["A10_sma20_50_80_stack"] = -1

    dd = a["max_dd_2m"]
    if np.isfinite(dd):
        # moderate wash = opportunity; collapse = bad structure
        if -0.25 <= dd <= -0.08:
            z["A11_max_downside_2m"] = 1
        elif dd < -0.40:
            z["A11_max_downside_2m"] = -1

    gb, gw = a["g_body_frac"], a["g_wick_frac"]
    if np.isfinite(gb) and np.isfinite(gw):
        # strong green bodies (not just wicks) preferred
        z["A12_green_body_vs_wick"] = 1 if gb >= gw else -1

    rb, rw = a["r_body_frac"], a["r_wick_frac"]
    if np.isfinite(rb) and np.isfinite(rw):
        # large red bodies = distribution; small body / long wick = less bad
        z["A13_red_body_vs_wick"] = -1 if rb >= rw else 1

    return z


def _part_b1(row: pd.Series, prior) -> dict:
    def g(col):
        return _num(row[col]) if col in row.index else np.nan

    def gp(col):
        if prior is None or col not in prior.index:
            return np.nan
        return _num(prior[col])

    target = g("Target Price")
    target_prev = gp("Target Price")
    insider = g("Insider Transactions")
    insider_prev = gp("Insider Transactions")
    income = g("Income")
    profit_m = g("Profit Margin")
    ed = row.get("Earnings Date", "")

    return {
        "eps_surprise": g("EPS Surprise"),
        "rev_surprise": g("Revenue Surprise"),
        "sales": g("Sales"),
        "income": income,
        "profit_margin": profit_m,
        "profitable": bool((np.isfinite(income) and income > 0) or (np.isfinite(profit_m) and profit_m > 0)),
        "target_price": target,
        "target_delta": (target - target_prev) if np.isfinite(target) and np.isfinite(target_prev) else np.nan,
        "analyst_recom": g("Analyst Recom"),
        "insider_tx": insider,
        "insider_delta": (insider - insider_prev) if np.isfinite(insider) and np.isfinite(insider_prev) else np.nan,
        "inst_tx": g("Institutional Transactions"),
        "short_float": g("Short Float"),
        "earnings_date": str(ed) if ed is not None and str(ed) not in {"nan", "None"} else "",
    }


def _pass_b1(b: dict) -> dict:
    z = {k: 0 for k in FEATURE_ORDER if k.startswith("B")}

    def signed(x):
        if not np.isfinite(x):
            return 0
        return 1 if x > 0 else (-1 if x < 0 else 0)

    z["B01_eps_surprise"] = signed(b["eps_surprise"])
    z["B02_revenue_surprise"] = signed(b["rev_surprise"])
    # B03/B04 sales/income are levels — polarity via profitable / margin
    z["B03_sales"] = 0
    z["B04_income"] = 1 if (np.isfinite(b["income"]) and b["income"] > 0) else (-1 if np.isfinite(b["income"]) else 0)
    pm = b["profit_margin"]
    z["B05_profit_margin"] = 1 if (np.isfinite(pm) and pm > 0) else (-1 if (np.isfinite(pm) and pm < 0) else 0)
    z["B06_profitable"] = 1 if b["profitable"] else -1
    z["B07_target_price"] = 0  # absolute level alone not directional without price
    z["B08_target_price_delta"] = signed(b["target_delta"])
    ar = b["analyst_recom"]
    if np.isfinite(ar):
        z["B09_analyst_recom"] = 1 if ar <= 2.5 else (-1 if ar >= 3.5 else 0)
    z["B10_insider_transactions"] = signed(b["insider_tx"])
    z["B11_insider_tx_delta"] = signed(b["insider_delta"])
    z["B12_institutional_transactions"] = signed(b["inst_tx"])
    sf = b["short_float"]
    if np.isfinite(sf) and sf >= 20:
        z["B13_short_float"] = 1  # squeeze fuel
    z["B14_earnings_date"] = 0  # informational only in B1
    return z


def _value_map(a: dict, b: dict) -> dict:
    """Human-readable value string per feature."""
    if not a.get("ok"):
        a_vals = {k: "no_ohlc" for k in FEATURE_ORDER if k.startswith("A")}
    else:
        a_vals = {
            "A01_rsi_value": f"{a['rsi']:.2f} (prev {a['rsi_prev']:.2f})",
            "A02_rsi_cross_30": a["cross_30"],
            "A03_rsi_cross_50": a["cross_50"],
            "A04_rsi_cross_70": a["cross_70"],
            "A05_body_red_green_ratio": (
                f"{a['body_rg']:.3f} (G={a['body_g']:.4f} R={a['body_r']:.4f} "
                f"win {a['window_start']}→{a['window_end']})"
            ),
            "A06_volume_red_green_ratio": f"{a['vol_rg']:.3f}" if np.isfinite(a["vol_rg"]) else "n/a",
            "A07_rvol": f"{a['rvol']:.3f}" if np.isfinite(a["rvol"]) else "n/a",
            "A08_bollinger_position": f"{a['bb_pos']:.3f} (0=mid, ±1≈band)" if np.isfinite(a["bb_pos"]) else "n/a",
            "A09_above_sma50": f"{a['above_sma50']} dist={a['dist_sma50']:+.2%}" if a["above_sma50"] is not None else "n/a",
            "A10_sma20_50_80_stack": a["sma_stack"],
            "A11_max_downside_2m": (
                f"{a['max_dd_2m']:+.2%} peak={a['dd_peak_date']} trough={a['dd_trough_date']}"
                if np.isfinite(a["max_dd_2m"]) else "n/a"
            ),
            "A12_green_body_vs_wick": (
                f"body={a['g_body_frac']:.3f} wick={a['g_wick_frac']:.3f}"
                if np.isfinite(a.get("g_body_frac", np.nan)) else "n/a"
            ),
            "A13_red_body_vs_wick": (
                f"body={a['r_body_frac']:.3f} wick={a['r_wick_frac']:.3f}"
                if np.isfinite(a.get("r_body_frac", np.nan)) else "n/a"
            ),
        }
    b_vals = {
        "B01_eps_surprise": b["eps_surprise"],
        "B02_revenue_surprise": b["rev_surprise"],
        "B03_sales": b["sales"],
        "B04_income": b["income"],
        "B05_profit_margin": b["profit_margin"],
        "B06_profitable": b["profitable"],
        "B07_target_price": b["target_price"],
        "B08_target_price_delta": b["target_delta"],
        "B09_analyst_recom": b["analyst_recom"],
        "B10_insider_transactions": b["insider_tx"],
        "B11_insider_tx_delta": b["insider_delta"],
        "B12_institutional_transactions": b["inst_tx"],
        "B13_short_float": b["short_float"],
        "B14_earnings_date": b["earnings_date"] or "none",
    }
    return {**a_vals, **b_vals}


def run(date: str | None = None, top: int = 15) -> pd.DataFrame:
    finviz, asof, path = _load_export(date)
    liquid = _filter_liquid(finviz)
    prior_df = _prior_export(asof)
    print(f"[ab] export={path.name} asof={asof} prior_delta={'yes' if prior_df is not None else 'no'}")

    if liquid.empty:
        raise SystemExit("[ab] liquid universe empty")

    store = ps._load_store()
    if not len(store):
        raise SystemExit("[ab] price store empty — bootstrap first")
    store = store[store["date"] <= pd.Timestamp(asof)]
    groups = {t: g.set_index("date").sort_index() for t, g in store[store["ticker"].isin(set(liquid["Ticker"]))].groupby("ticker")}

    rows = []
    detail_blocks = []  # for MD: per-ticker feature tables (top only)

    for _, row in liquid.iterrows():
        t = row["Ticker"]
        a = _part_a(groups.get(t))
        prior = prior_df.loc[t] if prior_df is not None and t in prior_df.index else None
        b = _part_b1(row, prior)
        pa = _pass_a(a)
        pb = _pass_b1(b)
        flags = {**pa, **pb}
        vals = _value_map(a, b)
        score = int(sum(flags.values()))
        n_good = sum(1 for x in flags.values() if x > 0)
        n_bad = sum(1 for x in flags.values() if x < 0)

        rec = {
            "Ticker": t,
            "asof_date": asof,
            "Sector": row.get("Sector", ""),
            "Industry": row.get("Industry", ""),
            "mcap_usd": row.get("_mcap"),
            "adv_shares": row.get("_adv"),
            "price": a.get("price", _num(row.get("Price"))),
            "score": score,
            "n_good": n_good,
            "n_bad": n_bad,
        }
        for k in FEATURE_ORDER:
            rec[f"val_{k}"] = vals.get(k)
            rec[f"flag_{k}"] = flags.get(k, 0)
            rec[f"status_{k}"] = _flag(flags.get(k, 0))
        rows.append(rec)

        # keep raw for top detail later
        rec["_vals"] = vals
        rec["_flags"] = flags

    out = pd.DataFrame([{k: v for k, v in r.items() if not k.startswith("_")} for r in rows])
    out = out.sort_values("score", ascending=False).reset_index(drop=True)

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    csv_path = OUT_DIR / f"{asof}_ab_checklist.csv"
    out.to_csv(csv_path, index=False)

    # ---- MD: feature dictionary + ranked list + full checklist for top N ----
    lines = [
        f"# A+B1 Feature Checklist — {asof}",
        "",
        f"- Gate: Market Cap > $80M · ADV > 500,000 shares → **{len(out):,}** names",
        f"- Export: `{path.name}` · prior export deltas: {'yes' if prior_df is not None else 'no'}",
        f"- score = sum of flags (GOOD=+1, BAD=-1, NEUTRAL=0) over **{len(FEATURE_ORDER)}** features",
        "",
        "## Feature list (exact)",
        "",
        "### Part A — from OHLC price store",
        "| ID | Feature | Source |",
        "|----|---------|--------|",
        "| A01 | Absolute RSI(14) | OHLC |",
        "| A02 | RSI cross vs 30 (cross_up / cross_down / above / below) | OHLC |",
        "| A03 | RSI cross vs 50 | OHLC |",
        "| A04 | RSI cross vs 70 | OHLC |",
        "| A05 | Candle **body** red:green ratio (last 10 sessions) | OHLC |",
        "| A06 | Candle **volume** red:green ratio (last 10 sessions) | OHLC |",
        "| A07 | Relative volume (today vs 20d avg) | OHLC |",
        "| A08 | Bollinger position (0=mid, ±1≈band) | OHLC |",
        "| A09 | Above / below SMA50 (+ distance %) | OHLC |",
        "| A10 | SMA20 vs SMA50 vs SMA80 stack | OHLC |",
        "| A11 | Max downside past ~2 months (peak→trough in window) | OHLC |",
        "| A12 | Green candles: body fraction vs wick fraction | OHLC |",
        "| A13 | Red candles: body fraction vs wick fraction | OHLC |",
        "",
        "### Part B1 — from Finviz export (no web)",
        "| ID | Feature | Source |",
        "|----|---------|--------|",
        "| B01 | EPS Surprise | export |",
        "| B02 | Revenue Surprise | export |",
        "| B03 | Sales | export |",
        "| B04 | Income | export |",
        "| B05 | Profit Margin | export |",
        "| B06 | Profitable (income>0 or margin>0) | export |",
        "| B07 | Analyst Target Price | export |",
        "| B08 | Target Price Δ vs prior export | export pair |",
        "| B09 | Analyst Recom (1=SB … 5=sell) | export |",
        "| B10 | Insider Transactions | export |",
        "| B11 | Insider Tx Δ vs prior export | export pair |",
        "| B12 | Institutional Transactions | export |",
        "| B13 | Short Float | export |",
        "| B14 | Earnings Date (raw string) | export |",
        "",
        "**Not in this file (needs Elite page scrape = Part B2):** full analyst history with dates, multi-quarter income/balance tables, monthly insider $ bars.",
        "",
        f"## Ranked scores (top {top} / bottom 10) — full feature tables below for top {top}",
        "",
        "| Rank | Ticker | score | n_good | n_bad | Industry |",
        "|-----:|--------|------:|-------:|------:|----------|",
    ]
    for i, r in out.head(top).iterrows():
        lines.append(
            f"| {i+1} | {r['Ticker']} | {int(r['score']):+d} | {int(r['n_good'])} | {int(r['n_bad'])} | {str(r['Industry'])[:40]} |"
        )
    lines.append("")
    for i, r in out.tail(10).iloc[::-1].iterrows():
        lines.append(
            f"| — | {r['Ticker']} | {int(r['score']):+d} | {int(r['n_good'])} | {int(r['n_bad'])} | {str(r['Industry'])[:40]} |"
        )

    # rebuild detail from sorted out + original rows map
    by_t = {r["Ticker"]: r for r in rows}
    lines += ["", f"## Full checklist — top {top} names", ""]
    for _, r in out.head(top).iterrows():
        t = r["Ticker"]
        src = by_t[t]
        lines += [
            f"### {t}  ·  score **{int(r['score']):+d}**  ·  {r['Industry']}",
            f"price={r['price']}  mcap=${r['mcap_usd']/1e9:.2f}B  ADV={r['adv_shares']:,.0f}",
            "",
            "| Feature | Value | Status |",
            "|---------|-------|:------:|",
        ]
        for k in FEATURE_ORDER:
            lines.append(
                f"| `{k}` | {src['_vals'].get(k)} | **{_flag(src['_flags'].get(k, 0))}** |"
            )
        lines.append("")

    lines += [
        "",
        f"Full universe CSV (all {len(out):,} rows × every feature value + flag):",
        f"`data/ab_checklist/{asof}_ab_checklist.csv`",
        "",
        "CSV columns: `val_<feature>` = raw value, `flag_<feature>` = +1/0/-1, `status_<feature>` = GOOD/BAD/NEUTRAL",
    ]
    md_path = OUT_DIR / f"{asof}_ab_checklist.md"
    md_path.write_text("\n".join(lines), encoding="utf-8")

    meta = {
        "asof": asof,
        "n_liquid": int(len(out)),
        "features": FEATURE_ORDER,
        "generated": datetime.now(ET).isoformat(),
        "csv": str(csv_path.relative_to(ROOT)),
        "top": out.head(10)[["Ticker", "score", "n_good", "n_bad", "Industry"]].to_dict("records"),
    }
    (OUT_DIR / f"{asof}_ab_checklist.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")

    print(f"[ab] features={len(FEATURE_ORDER)} liquid={len(out):,}")
    print(f"[ab] wrote {csv_path}")
    print(f"[ab] wrote {md_path}")
    print("Top 5:", out.head(5)[["Ticker", "score", "n_good", "n_bad"]].to_string(index=False))
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=None)
    ap.add_argument("--top", type=int, default=15)
    args = ap.parse_args()
    run(date=args.date, top=args.top)


if __name__ == "__main__":
    main()
