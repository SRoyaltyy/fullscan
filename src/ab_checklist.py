"""Part A (OHLC) + Part B1 (Finviz export) — feature checklist with explicit session dates.

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
LOOKBACK_RVOL = 20

FEATURE_ORDER = [
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
        return None, None
    path = prev[-1]
    df = pd.read_csv(path, low_memory=False)
    tcol = "Ticker" if "Ticker" in df.columns else df.columns[0]
    df["Ticker"] = df[tcol].astype(str).str.strip().str.upper()
    prior_date = path.stem.replace("finviz_", "")
    return df.drop_duplicates("Ticker", keep="first").set_index("Ticker"), prior_date


def _filter_liquid(df: pd.DataFrame) -> pd.DataFrame:
    mcap_col = "Market Cap" if "Market Cap" in df.columns else None
    vol_col = next((c for c in ("Average Volume", "Avg Volume") if c in df.columns), None)
    if not mcap_col or not vol_col:
        raise SystemExit(f"[ab] missing Market Cap / Average Volume; cols={list(df.columns)[:25]}")
    out = df.copy()
    out["_mcap"] = out[mcap_col].map(_num) * 1_000_000.0
    out["_adv"] = out[vol_col].map(_num) * 1_000.0
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
    d_now = df.index[-1].date().isoformat()
    d_prev = df.index[-2].date().isoformat() if len(df) > 1 else None

    # ---- last N sessions: per-candle audit trail ----
    tail = df.tail(LOOKBACK_BODY)
    body_sessions = []   # dated list for A05
    vol_sessions = []    # dated list for A06
    wick_sessions = []   # dated list for A12/A13
    body_g = body_r = 0.0
    vol_g = vol_r = 0.0
    green_dates = []
    red_dates = []
    doji_dates = []

    for dt, row in tail.iterrows():
        d = dt.date().isoformat()
        o, h, l, cl = float(row["open"]), float(row["high"]), float(row["low"]), float(row["close"])
        vol = float(row["volume"]) if "volume" in tail.columns and pd.notna(row.get("volume")) else np.nan
        body = cl - o
        rng = h - l
        body_abs = abs(body)
        upper_w = h - max(o, cl)
        lower_w = min(o, cl) - l
        if body > 0:
            color = "GREEN"
            body_g += body
            green_dates.append(d)
            if np.isfinite(vol):
                vol_g += vol
        elif body < 0:
            color = "RED"
            body_r += -body
            red_dates.append(d)
            if np.isfinite(vol):
                vol_r += vol
        else:
            color = "DOJI"
            doji_dates.append(d)
            if np.isfinite(vol):
                vol_g += vol * 0.5
                vol_r += vol * 0.5

        body_sessions.append(
            f"{d}:{color}:O={o:.4f},C={cl:.4f},body={body:+.4f}"
        )
        vol_sessions.append(
            f"{d}:{color}:vol={vol:.0f}" if np.isfinite(vol) else f"{d}:{color}:vol=n/a"
        )
        wick_sessions.append(
            f"{d}:{color}:body={body_abs:.4f},upperW={upper_w:.4f},lowerW={lower_w:.4f},range={rng:.4f}"
        )

    body_rg = body_g / body_r if body_r > 1e-12 else (99.0 if body_g > 0 else 1.0)
    vol_rg = vol_g / vol_r if vol_r > 1e-12 else (99.0 if vol_g > 0 else np.nan)

    # RVOL: today vs prior 20 sessions average volume (exclude today from avg)
    if v.notna().sum() >= LOOKBACK_RVOL + 1:
        today_vol = float(v.iloc[-1])
        avg20 = float(v.iloc[-(LOOKBACK_RVOL + 1):-1].mean())
        rvol = today_vol / avg20 if avg20 > 0 else np.nan
        rvol_window_start = v.index[-(LOOKBACK_RVOL + 1)].date().isoformat()
        rvol_window_end = v.index[-2].date().isoformat()
    else:
        today_vol = float(v.iloc[-1]) if len(v) else np.nan
        avg20 = np.nan
        rvol = np.nan
        rvol_window_start = rvol_window_end = None

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

    # max DD window: last LOOKBACK_DD sessions
    win = c.tail(LOOKBACK_DD)
    dd_window_start = win.index[0].date().isoformat()
    dd_window_end = win.index[-1].date().isoformat()
    if len(win) >= 5:
        peak_i = int(win.values.argmax())
        peak = float(win.iloc[peak_i])
        after = win.iloc[peak_i:]
        trough_i = int(after.values.argmin())
        trough = float(after.iloc[trough_i])
        max_dd = trough / peak - 1.0 if peak > 0 else np.nan
        dd_peak = win.index[peak_i].date().isoformat()
        dd_trough = after.index[trough_i].date().isoformat()
        dd_peak_px = peak
        dd_trough_px = trough
    else:
        max_dd = np.nan
        dd_peak = dd_trough = None
        dd_peak_px = dd_trough_px = np.nan

    # body/wick fractions over same LOOKBACK_BODY window
    rng_s = (tail["high"] - tail["low"]).replace(0, np.nan)
    body_abs_s = (tail["close"] - tail["open"]).abs()
    upper_w_s = tail["high"] - tail[["open", "close"]].max(axis=1)
    lower_w_s = tail[["open", "close"]].min(axis=1) - tail["low"]
    gmask = tail["close"] > tail["open"]
    rmask = tail["close"] < tail["open"]

    def frac(mask, num, den):
        if mask.sum() == 0:
            return np.nan
        n, d = float(num[mask].sum()), float(den[mask].sum())
        return n / d if d > 0 else np.nan

    g_body = frac(gmask, body_abs_s, rng_s)
    r_body = frac(rmask, body_abs_s, rng_s)
    g_wick = frac(gmask, upper_w_s + lower_w_s, rng_s)
    r_wick = frac(rmask, upper_w_s + lower_w_s, rng_s)

    return {
        "ok": True,
        "n_bars": int(len(df)),
        "asof_session": d_now,
        "prev_session": d_prev,
        "price": c_now,
        "rsi": rsi_now,
        "rsi_prev": rsi_prev,
        "cross_30": _cross(rsi_prev, rsi_now, 30),
        "cross_50": _cross(rsi_prev, rsi_now, 50),
        "cross_70": _cross(rsi_prev, rsi_now, 70),
        "body_rg": body_rg,
        "body_g": body_g,
        "body_r": body_r,
        "body_sessions": body_sessions,          # list[str] dated
        "green_dates": green_dates,
        "red_dates": red_dates,
        "doji_dates": doji_dates,
        "vol_rg": vol_rg,
        "vol_g": vol_g,
        "vol_r": vol_r,
        "vol_sessions": vol_sessions,
        "wick_sessions": wick_sessions,
        "window_start": tail.index[0].date().isoformat(),
        "window_end": tail.index[-1].date().isoformat(),
        "rvol": rvol,
        "rvol_today_vol": today_vol,
        "rvol_avg20": avg20,
        "rvol_window_start": rvol_window_start,
        "rvol_window_end": rvol_window_end,
        "bb_pos": bb_pos,
        "bb_mid": mid_n,
        "bb_upper": up_n,
        "bb_lower": lo_n,
        "above_sma50": above50,
        "dist_sma50": dist50,
        "sma20": s20,
        "sma50": s50,
        "sma80": s80,
        "sma_stack": stack,
        "max_dd_2m": max_dd,
        "dd_window_start": dd_window_start,
        "dd_window_end": dd_window_end,
        "dd_peak_date": dd_peak,
        "dd_trough_date": dd_trough,
        "dd_peak_px": dd_peak_px,
        "dd_trough_px": dd_trough_px,
        "g_body_frac": g_body,
        "g_wick_frac": g_wick,
        "r_body_frac": r_body,
        "r_wick_frac": r_wick,
    }


def _pass_a(a: dict) -> dict:
    z = {k: 0 for k in FEATURE_ORDER if k.startswith("A")}
    if not a.get("ok"):
        return z

    rsi = a["rsi"]
    if np.isfinite(rsi):
        if rsi <= 30:
            z["A01_rsi_value"] = 1
        elif rsi >= 70:
            z["A01_rsi_value"] = -1

    def cross_score(state: str, level: int) -> int:
        if state == "cross_up":
            return 1
        if state == "cross_down":
            return -1 if level in (30, 50) else 1
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
        if -0.25 <= dd <= -0.08:
            z["A11_max_downside_2m"] = 1
        elif dd < -0.40:
            z["A11_max_downside_2m"] = -1

    gb, gw = a["g_body_frac"], a["g_wick_frac"]
    if np.isfinite(gb) and np.isfinite(gw):
        z["A12_green_body_vs_wick"] = 1 if gb >= gw else -1

    rb, rw = a["r_body_frac"], a["r_wick_frac"]
    if np.isfinite(rb) and np.isfinite(rw):
        z["A13_red_body_vs_wick"] = -1 if rb >= rw else 1

    return z


def _part_b1(row: pd.Series, prior, prior_date: str | None) -> dict:
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
        "target_prev": target_prev,
        "analyst_recom": g("Analyst Recom"),
        "insider_tx": insider,
        "insider_delta": (insider - insider_prev) if np.isfinite(insider) and np.isfinite(insider_prev) else np.nan,
        "insider_prev": insider_prev,
        "inst_tx": g("Institutional Transactions"),
        "short_float": g("Short Float"),
        "earnings_date": str(ed) if ed is not None and str(ed) not in {"nan", "None"} else "",
        "prior_export_date": prior_date,
    }


def _pass_b1(b: dict) -> dict:
    z = {k: 0 for k in FEATURE_ORDER if k.startswith("B")}

    def signed(x):
        if not np.isfinite(x):
            return 0
        return 1 if x > 0 else (-1 if x < 0 else 0)

    z["B01_eps_surprise"] = signed(b["eps_surprise"])
    z["B02_revenue_surprise"] = signed(b["rev_surprise"])
    z["B03_sales"] = 0
    z["B04_income"] = 1 if (np.isfinite(b["income"]) and b["income"] > 0) else (-1 if np.isfinite(b["income"]) else 0)
    pm = b["profit_margin"]
    z["B05_profit_margin"] = 1 if (np.isfinite(pm) and pm > 0) else (-1 if (np.isfinite(pm) and pm < 0) else 0)
    z["B06_profitable"] = 1 if b["profitable"] else -1
    z["B07_target_price"] = 0
    z["B08_target_price_delta"] = signed(b["target_delta"])
    ar = b["analyst_recom"]
    if np.isfinite(ar):
        z["B09_analyst_recom"] = 1 if ar <= 2.5 else (-1 if ar >= 3.5 else 0)
    z["B10_insider_transactions"] = signed(b["insider_tx"])
    z["B11_insider_tx_delta"] = signed(b["insider_delta"])
    z["B12_institutional_transactions"] = signed(b["inst_tx"])
    sf = b["short_float"]
    if np.isfinite(sf) and sf >= 20:
        z["B13_short_float"] = 1
    z["B14_earnings_date"] = 0
    return z


def _value_map(a: dict, b: dict) -> dict:
    if not a.get("ok"):
        a_vals = {k: "no_ohlc" for k in FEATURE_ORDER if k.startswith("A")}
    else:
        green = ",".join(a["green_dates"]) or "none"
        red = ",".join(a["red_dates"]) or "none"
        doji = ",".join(a["doji_dates"]) or "none"
        body_trail = " | ".join(a["body_sessions"])
        vol_trail = " | ".join(a["vol_sessions"])
        wick_trail = " | ".join(a["wick_sessions"])

        a_vals = {
            "A01_rsi_value": (
                f"RSI={a['rsi']:.2f} on {a['asof_session']}; "
                f"prev RSI={a['rsi_prev']:.2f} on {a['prev_session']}"
            ),
            "A02_rsi_cross_30": (
                f"{a['cross_30']} | RSI {a['rsi_prev']:.2f}@{a['prev_session']} → "
                f"{a['rsi']:.2f}@{a['asof_session']} vs level 30"
            ),
            "A03_rsi_cross_50": (
                f"{a['cross_50']} | RSI {a['rsi_prev']:.2f}@{a['prev_session']} → "
                f"{a['rsi']:.2f}@{a['asof_session']} vs level 50"
            ),
            "A04_rsi_cross_70": (
                f"{a['cross_70']} | RSI {a['rsi_prev']:.2f}@{a['prev_session']} → "
                f"{a['rsi']:.2f}@{a['asof_session']} vs level 70"
            ),
            "A05_body_red_green_ratio": (
                f"ratio={a['body_rg']:.3f} (= sum GREEN bodies / sum RED bodies); "
                f"GREEN_sum={a['body_g']:.4f} dates=[{green}]; "
                f"RED_sum={a['body_r']:.4f} dates=[{red}]; "
                f"DOJI=[{doji}]; window={a['window_start']}→{a['window_end']} ({LOOKBACK_BODY} sessions); "
                f"sessions: {body_trail}"
            ),
            "A06_volume_red_green_ratio": (
                f"ratio={a['vol_rg']:.3f} if finite else n/a; "
                f"GREEN_vol_sum={a['vol_g']:.0f} dates=[{green}]; "
                f"RED_vol_sum={a['vol_r']:.0f} dates=[{red}]; "
                f"window={a['window_start']}→{a['window_end']}; sessions: {vol_trail}"
                if np.isfinite(a["vol_rg"]) else
                f"n/a; window={a['window_start']}→{a['window_end']}; sessions: {vol_trail}"
            ),
            "A07_rvol": (
                f"RVOL={a['rvol']:.3f} on {a['asof_session']}: "
                f"today_vol={a['rvol_today_vol']:.0f} / avg20={a['rvol_avg20']:.0f} "
                f"(avg window {a['rvol_window_start']}→{a['rvol_window_end']}, excludes today)"
                if np.isfinite(a["rvol"]) else "n/a"
            ),
            "A08_bollinger_position": (
                f"pos={a['bb_pos']:.3f} on {a['asof_session']} "
                f"(price={a['price']:.4f}, mid={a['bb_mid']:.4f}, "
                f"upper={a['bb_upper']:.4f}, lower={a['bb_lower']:.4f}; 20d BB)"
                if np.isfinite(a["bb_pos"]) else "n/a"
            ),
            "A09_above_sma50": (
                f"above={a['above_sma50']} on {a['asof_session']}: "
                f"price={a['price']:.4f} vs SMA50={a['sma50']:.4f} dist={a['dist_sma50']:+.2%}"
                if a["above_sma50"] is not None else "n/a"
            ),
            "A10_sma20_50_80_stack": (
                f"{a['sma_stack']} on {a['asof_session']}: "
                f"SMA20={a['sma20']:.4f} SMA50={a['sma50']:.4f} SMA80={a['sma80']:.4f}"
            ),
            "A11_max_downside_2m": (
                f"maxDD={a['max_dd_2m']:+.2%} inside window {a['dd_window_start']}→{a['dd_window_end']} "
                f"({LOOKBACK_DD} sessions): peak {a['dd_peak_date']} @ {a['dd_peak_px']:.4f} → "
                f"trough {a['dd_trough_date']} @ {a['dd_trough_px']:.4f}"
                if np.isfinite(a["max_dd_2m"]) else "n/a"
            ),
            "A12_green_body_vs_wick": (
                f"GREEN dates=[{green}] body_frac={a['g_body_frac']:.3f} wick_frac={a['g_wick_frac']:.3f}; "
                f"window={a['window_start']}→{a['window_end']}; sessions: {wick_trail}"
                if np.isfinite(a.get("g_body_frac", np.nan)) else
                f"no GREEN candles in window {a['window_start']}→{a['window_end']}"
            ),
            "A13_red_body_vs_wick": (
                f"RED dates=[{red}] body_frac={a['r_body_frac']:.3f} wick_frac={a['r_wick_frac']:.3f}; "
                f"window={a['window_start']}→{a['window_end']}; sessions: {wick_trail}"
                if np.isfinite(a.get("r_body_frac", np.nan)) else
                f"no RED candles in window {a['window_start']}→{a['window_end']}"
            ),
        }

    prior_d = b.get("prior_export_date") or "none"
    b_vals = {
        "B01_eps_surprise": b["eps_surprise"],
        "B02_revenue_surprise": b["rev_surprise"],
        "B03_sales": b["sales"],
        "B04_income": b["income"],
        "B05_profit_margin": b["profit_margin"],
        "B06_profitable": b["profitable"],
        "B07_target_price": b["target_price"],
        "B08_target_price_delta": (
            f"delta={b['target_delta']} (now={b['target_price']} vs prior_export={b['target_prev']} "
            f"on finviz_{prior_d})"
            if np.isfinite(b.get("target_delta", np.nan)) else
            f"n/a (now={b['target_price']}, prior_export_date={prior_d})"
        ),
        "B09_analyst_recom": b["analyst_recom"],
        "B10_insider_transactions": b["insider_tx"],
        "B11_insider_tx_delta": (
            f"delta={b['insider_delta']} (now={b['insider_tx']} vs prior={b['insider_prev']} "
            f"on finviz_{prior_d})"
            if np.isfinite(b.get("insider_delta", np.nan)) else
            f"n/a (now={b['insider_tx']}, prior_export_date={prior_d})"
        ),
        "B12_institutional_transactions": b["inst_tx"],
        "B13_short_float": b["short_float"],
        "B14_earnings_date": b["earnings_date"] or "none",
    }
    return {**a_vals, **b_vals}


def run(date: str | None = None, top: int = 15) -> pd.DataFrame:
    finviz, asof, path = _load_export(date)
    liquid = _filter_liquid(finviz)
    prior_df, prior_date = _prior_export(asof)
    print(f"[ab] export={path.name} asof={asof} prior={prior_date or 'none'}")

    if liquid.empty:
        raise SystemExit("[ab] liquid universe empty")

    store = ps._load_store()
    if not len(store):
        raise SystemExit("[ab] price store empty — bootstrap first")
    store = store[store["date"] <= pd.Timestamp(asof)]
    groups = {
        t: g.set_index("date").sort_index()
        for t, g in store[store["ticker"].isin(set(liquid["Ticker"]))].groupby("ticker")
    }

    rows = []
    for _, row in liquid.iterrows():
        t = row["Ticker"]
        a = _part_a(groups.get(t))
        prior = prior_df.loc[t] if prior_df is not None and t in prior_df.index else None
        b = _part_b1(row, prior, prior_date)
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
            # compact helpers (still dated in val_*)
            "green_dates": ",".join(a.get("green_dates") or []),
            "red_dates": ",".join(a.get("red_dates") or []),
            "body_window": f"{a.get('window_start')}→{a.get('window_end')}" if a.get("ok") else "",
        }
        for k in FEATURE_ORDER:
            rec[f"val_{k}"] = vals.get(k)
            rec[f"flag_{k}"] = flags.get(k, 0)
            rec[f"status_{k}"] = _flag(flags.get(k, 0))
        rows.append(rec)
        rec["_vals"] = vals
        rec["_flags"] = flags

    out = pd.DataFrame([{k: v for k, v in r.items() if not k.startswith("_")} for r in rows])
    out = out.sort_values("score", ascending=False).reset_index(drop=True)

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    csv_path = OUT_DIR / f"{asof}_ab_checklist.csv"
    out.to_csv(csv_path, index=False)

    lines = [
        f"# A+B1 Feature Checklist — {asof}",
        "",
        f"- Gate: Market Cap > $80M · ADV > 500,000 shares → **{len(out):,}** names",
        f"- Export: `{path.name}` · prior export for Δ: `{prior_date or 'none'}`",
        f"- score = sum of flags over **{len(FEATURE_ORDER)}** features",
        "",
        "## How dates work",
        "",
        f"- **A05/A06/A12/A13** use the last **{LOOKBACK_BODY} trading sessions** ending on the latest OHLC bar on/before `{asof}`.",
        "- Each session is listed as `YYYY-MM-DD:COLOR:O=…,C=…,body=±…` (body) or with volume/wicks.",
        "- **GREEN dates** / **RED dates** are the exact session dates that entered the ratio numerator/denominator.",
        f"- **A07 RVOL** = volume on as-of session / mean volume of the prior **{LOOKBACK_RVOL}** sessions (excludes today).",
        f"- **A11 max DD** scans the last **{LOOKBACK_DD}** sessions and reports peak date+price and trough date+price.",
        "- **B08/B11 deltas** compare current Finviz export vs the previous dated export file.",
        "",
        f"## Ranked (top {top})",
        "",
        "| Rank | Ticker | score | good | bad | Industry |",
        "|-----:|--------|------:|-----:|----:|----------|",
    ]
    for i, r in out.head(top).iterrows():
        lines.append(
            f"| {i+1} | {r['Ticker']} | {int(r['score']):+d} | {int(r['n_good'])} | {int(r['n_bad'])} | {str(r['Industry'])[:40]} |"
        )

    by_t = {r["Ticker"]: r for r in rows}
    lines += ["", f"## Full checklist with dates — top {top}", ""]
    for _, r in out.head(top).iterrows():
        t = r["Ticker"]
        src = by_t[t]
        lines += [
            f"### {t}  ·  score **{int(r['score']):+d}**  ·  {r['Industry']}",
            f"price={r['price']}  mcap=${r['mcap_usd']/1e9:.2f}B  ADV={r['adv_shares']:,.0f}",
            f"body window: `{r['body_window']}`  GREEN=[{r['green_dates']}]  RED=[{r['red_dates']}]",
            "",
            "| Feature | Value (with dates) | Status |",
            "|---------|--------------------|:------:|",
        ]
        for k in FEATURE_ORDER:
            lines.append(
                f"| `{k}` | {src['_vals'].get(k)} | **{_flag(src['_flags'].get(k, 0))}** |"
            )
        lines.append("")

    lines += [
        f"CSV: `data/ab_checklist/{asof}_ab_checklist.csv`",
        "Columns: `val_*` (full dated string), `flag_*` (+1/0/-1), `status_*`, plus `green_dates` / `red_dates` / `body_window`.",
    ]
    md_path = OUT_DIR / f"{asof}_ab_checklist.md"
    md_path.write_text("\n".join(lines), encoding="utf-8")

    meta = {
        "asof": asof,
        "prior_export": prior_date,
        "n_liquid": int(len(out)),
        "features": FEATURE_ORDER,
        "lookback_body": LOOKBACK_BODY,
        "lookback_dd": LOOKBACK_DD,
        "generated": datetime.now(ET).isoformat(),
        "csv": str(csv_path.relative_to(ROOT)),
    }
    (OUT_DIR / f"{asof}_ab_checklist.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")

    print(f"[ab] wrote {csv_path}")
    print(f"[ab] wrote {md_path}")
    print("Top 5:", out.head(5)[["Ticker", "score", "green_dates", "red_dates"]].to_string(index=False))
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=None)
    ap.add_argument("--top", type=int, default=15)
    args = ap.parse_args()
    run(date=args.date, top=args.top)


if __name__ == "__main__":
    main()
