"""Continuous + gated H/I mine on Yahoo A–F (Excel STOCKHISTORY seed).

Labels are Excel H and I (fractions). Same-row H/I are never features.
Open-entry features are morning-knowable. Close-entry may use today's
H/I/G to forecast later sessions.

  python engine/mine_hi_corr.py
  python engine/mine_hi_corr.py --limit-tickers 200
"""
from __future__ import annotations

import argparse
import json
import math
import os
import sys
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
ROWS = ROOT / "data" / "rows"
SPLIT = HERE / "holdout_split.json"
FINVIZ = ROOT / "data" / "finviz_with_descriptions.csv"
OUT_MD = ROOT / "research" / "HI_CORR.md"
OUT_JSON = ROOT / "research" / "hi_corr.json"
OUT_PLAN_NOTE = ROOT / "research" / "HI_CORR_PLAN.md"

COST_HI, COST_LO = 0.001, 0.003
MIN_N = 400
MIN_TICKERS = 50
MIN_EDGE = 0.002  # 20 bp vs everyone after costs
TOP5_CAP = 0.30
DAY_CAP = 0.18
Q1, Q3 = date(2026, 4, 1), date(2026, 7, 1)
HORIZONS = (("1d", 1), ("2d", 2), ("3d", 3), ("1w", 5), ("2w", 10))


def tstat(vals):
    a = np.asarray(vals, dtype=float)
    a = a[np.isfinite(a)]
    n = a.size
    if n < 3:
        return float("nan")
    m = float(a.mean())
    v = float(a.var(ddof=1))
    if v <= 0:
        return float("nan")
    return m / math.sqrt(v / n)


def spearman(x, y):
    x = np.asarray(x, dtype=float)
    y = np.asarray(y, dtype=float)
    m = np.isfinite(x) & np.isfinite(y)
    x, y = x[m], y[m]
    n = x.size
    if n < 30:
        return float("nan"), n
    rx = pd.Series(x).rank().to_numpy()
    ry = pd.Series(y).rank().to_numpy()
    rx = (rx - rx.mean()) / (rx.std() or 1)
    ry = (ry - ry.mean()) / (ry.std() or 1)
    return float(np.mean(rx * ry)), n


def load_split():
    raw = json.loads(SPLIT.read_text())
    return set(raw["discovery"]), set(raw["holdout"])


def load_mcap():
    out = {}
    if not FINVIZ.exists():
        return out
    df = pd.read_csv(FINVIZ, usecols=["Ticker", "Market Cap"], low_memory=False)
    for t, m in zip(df["Ticker"], df["Market Cap"]):
        try:
            out[str(t).upper()] = float(m)
        except (TypeError, ValueError):
            pass
    return out


def cost_of(ticker, mcap):
    m = mcap.get(ticker)
    if m is None:
        return 0.002
    return COST_HI if m >= 300 else COST_LO


def load_panel(limit=0, extra_tickers=()):
    files = sorted(ROWS.glob("*.json"))
    if extra_tickers:
        want = {t.upper() for t in extra_tickers}
        files = [p for p in files if p.stem.upper() in want] or files
    if limit:
        files = files[:limit]
    frames = []
    for p in files:
        raw = json.loads(p.read_text())
        if not raw:
            continue
        recs = []
        for r in raw:
            try:
                d = r["date"]
                if isinstance(d, str):
                    d = date.fromisoformat(d[:10])
                recs.append({
                    "ticker": p.stem.upper(),
                    "date": d,
                    "open": float(r["open"]),
                    "high": float(r["high"]),
                    "low": float(r["low"]),
                    "close": float(r["close"]),
                    "volume": float(r.get("volume") or 0),
                })
            except (TypeError, ValueError, KeyError):
                continue
        if len(recs) < 30:
            continue
        frames.append(pd.DataFrame(recs))
    if not frames:
        raise SystemExit(f"no rows under {ROWS}")
    df = pd.concat(frames, ignore_index=True)
    df = df.drop_duplicates(["ticker", "date"]).sort_values(["ticker", "date"])
    return df.reset_index(drop=True)


def add_excel_features(df: pd.DataFrame) -> pd.DataFrame:
    """Reproduce A–F-rooted Excel columns + leak-free lags."""
    g = df.groupby("ticker", sort=False)
    o, h, l, c, v = (df[k].to_numpy() for k in
                     ("open", "high", "low", "close", "volume"))
    pc = g["close"].shift(1).to_numpy()
    po = g["open"].shift(1).to_numpy()
    pv = g["volume"].shift(1).to_numpy()

    H = (c - o) / o
    I = (c - pc) / pc
    J = (o - po) / po
    gap = (o - pc) / pc
    G = v / np.where(pv == 0, np.nan, pv)
    K = (h - o) / (o + 0.001)
    M = -(l - o) / o

    df = df.copy()
    df["H"] = H
    df["I"] = I
    df["J"] = J
    df["gap"] = gap
    df["G"] = G
    df["K"] = K
    df["M"] = M
    df["rvol"] = v / g["volume"].transform(lambda s: s.rolling(20, min_periods=8).median())
    g = df.groupby("ticker", sort=False)

    # lags (prior rows — fair at the next open)
    for col in ("H", "I", "J", "gap", "G", "K", "M", "rvol", "close", "volume"):
        df[f"{col}_l1"] = g[col].shift(1)
        df[f"{col}_l2"] = g[col].shift(2)
        df[f"{col}_l3"] = g[col].shift(3)
        df[f"{col}_l5"] = g[col].shift(5)

    # AH: COUNTIF(prior 6 H, "<=-0.05") — Excel AH10 = H4:H9
    df["AH"] = g["H"].transform(
        lambda s: s.shift(1).rolling(6, min_periods=3).apply(
            lambda x: float(np.sum(x <= -0.05)), raw=True))
    # JC-like: no H < -3% in prior 8
    df["JC"] = g["H"].transform(
        lambda s: (s.shift(1).rolling(8, min_periods=5).apply(
            lambda x: float(np.sum(x < -0.03) == 0), raw=True)))
    # JB-like: wild |H|>3% on ≥7 of prior 8
    df["JB"] = g["H"].transform(
        lambda s: s.shift(1).rolling(8, min_periods=5).apply(
            lambda x: float(np.sum(np.abs(x) > 0.03) >= 7), raw=True))
    # heat = prior 5-day mean I (visual green/red region proxy)
    df["heat5"] = g["I"].transform(
        lambda s: s.shift(1).rolling(5, min_periods=3).mean())
    df["heat10"] = g["I"].transform(
        lambda s: s.shift(1).rolling(10, min_periods=5).mean())
    df["vol20"] = g["I"].transform(
        lambda s: s.shift(1).rolling(20, min_periods=10).std())
    # consecutive signed I streak ending yesterday
    sign = np.sign(df["I"].fillna(0).to_numpy())
    df["_signI"] = sign
    streak = []
    last_t = None
    run = 0
    last_s = 0
    for t, s in zip(df["ticker"], sign):
        if t != last_t:
            run, last_s, last_t = 0, 0, t
        if s == 0 or not np.isfinite(s):
            run = 0
            last_s = 0
        elif s == last_s:
            run += int(s)
        else:
            run = int(s)
            last_s = s
        streak.append(run)
    df["I_streak"] = streak
    df["I_streak_l1"] = df.groupby("ticker")["I_streak"].shift(1)

    # ER/ES: carried sign of a big prior H or I
    er = np.where(df["H_l1"] < -0.03, -1,
                  np.where(df["I_l1"] < -0.03, -1,
                           np.where(df["H_l1"] > 0.05, 1,
                                    np.where(df["I_l1"] > 0.05, 1, 0))))
    df["ER"] = er
    es = []
    last_t = None
    cur = 0
    for t, e in zip(df["ticker"], er):
        if t != last_t:
            cur, last_t = 0, t
        if e != 0:
            cur = int(e)
        es.append(cur)
    df["ES"] = es

    # FR-like: prior median vol > 1M and/or prior G >= 3
    med5 = g["volume"].transform(lambda s: s.shift(1).rolling(5, min_periods=3).median())
    df["FR"] = ((med5 >= 1_000_000) | (df["G_l1"] >= 3)).astype(float)

    # dollar volume / liquidity (yesterday — open-knowable)
    df["dvol_l1"] = df["close_l1"] * df["volume_l1"]
    df["px_l1"] = df["close_l1"]
    return df


def add_labels(df: pd.DataFrame) -> pd.DataFrame:
    g = df.groupby("ticker", sort=False)
    df = df.copy()
    # same-day labels (H/I of this row)
    df["y_h1"] = df["H"]
    df["y_i1"] = df["I"]
    # later-session I and H (close-entry / next-day)
    for name, k in HORIZONS:
        df[f"y_i_lead_{name}"] = g["I"].shift(-k)
        df[f"y_h_lead_{name}"] = g["H"].shift(-k)
        # stacked I from *next* session through +k (does not include today)
        # close_{t+k} / close_t - 1
        df[f"y_i_stack_{name}"] = g["close"].shift(-k) / df["close"] - 1
        # from-open hold k: close_{t+k-1} / open_t - 1  (k=1 → today's H)
        if k == 1:
            df[f"y_from_open_{name}"] = df["H"]
        else:
            df[f"y_from_open_{name}"] = g["close"].shift(-(k - 1)) / df["open"] - 1
    return df


def attach_spy(df: pd.DataFrame) -> pd.DataFrame:
    spy_path = ROWS / "SPY.json"
    if not spy_path.exists():
        df["spy_I"] = np.nan
        df["spy_I_l1"] = np.nan
        return df
    spy = pd.DataFrame(json.loads(spy_path.read_text()))
    spy["date"] = pd.to_datetime(spy["date"]).dt.date
    spy = spy.sort_values("date")
    spy["spy_I"] = spy["close"].pct_change()
    spy["spy_I_l1"] = spy["spy_I"].shift(1)
    m = spy[["date", "spy_I", "spy_I_l1"]]
    out = df.merge(m, on="date", how="left")
    return out


def liquid_mask(df):
    return (
        (df["px_l1"] >= 2.0)
        & (df["dvol_l1"] >= 1_000_000)
        & np.isfinite(df["H"])
        & np.isfinite(df["I"])
        & np.isfinite(df["gap"])
    )


def split_masks(df, discovery, holdout):
    disc = df["ticker"].isin(discovery)
    hold = df["ticker"].isin(holdout)
    # leftover names (in rows but not in split) ride with discovery
    other = ~(disc | hold)
    disc = disc | other
    early = df["date"] < Q3
    late = df["date"] >= Q3
    q1 = df["date"] < Q1
    spy_up = df["spy_I_l1"] > 0.0015
    spy_dn = df["spy_I_l1"] < -0.0015
    return {
        "disc": disc, "hold": hold, "early": early, "late": late, "q1": q1,
        "spy_up": spy_up, "spy_dn": spy_dn,
        "spy_up_today": df["spy_I"] > 0.0015,
        "spy_dn_today": df["spy_I"] < -0.0015,
    }


def open_gates(df):
    """Morning-knowable boolean series. No same-row H/I/G/K/M/D/E/F."""
    return {
        "gap_le_m3": df["gap"] <= -0.03,
        "gap_le_m2": df["gap"] <= -0.02,
        "gap_le_m1": df["gap"] <= -0.01,
        "gap_ge_p2": df["gap"] >= 0.02,
        "gap_ge_p3": df["gap"] >= 0.03,
        "gap_neg": df["gap"] < 0,
        "gap_pos": df["gap"] > 0,
        "J_neg": df["J"] < 0,
        "J_pos": df["J"] > 0,
        "I_l1_le_m3": df["I_l1"] <= -0.03,
        "I_l1_le_m5": df["I_l1"] <= -0.05,
        "I_l1_ge_p3": df["I_l1"] >= 0.03,
        "H_l1_le_m3": df["H_l1"] <= -0.03,
        "H_l1_ge_p3": df["H_l1"] >= 0.03,
        "AH_ge1": df["AH"] >= 1,
        "AH_ge2": df["AH"] >= 2,
        "JC": df["JC"] >= 1,
        "JB": df["JB"] >= 1,
        "heat_hot": df["heat5"] >= 0.008,
        "heat_cold": df["heat5"] <= -0.008,
        "streak_up3": df["I_streak_l1"] >= 3,
        "streak_dn3": df["I_streak_l1"] <= -3,
        "ES_pos": df["ES"] >= 1,
        "ES_neg": df["ES"] <= -1,
        "FR": df["FR"] >= 1,
        "rvol_l1_ge2": df["G_l1"] >= 2,
        "G_l1_ge3": df["G_l1"] >= 3,
        # motivated pairs
        "washout": (df["gap"] <= -0.02) & (df["I_l1"] <= -0.03),
        "dip_in_heat": (df["gap"] <= -0.015) & (df["heat5"] >= 0.005),
        "AH_and_heat": (df["AH"] >= 1) & (df["heat5"] >= 0.005),
        "AH_and_FR": (df["AH"] >= 1) & (df["FR"] >= 1),
        "gap_fade_up": (df["gap"] <= -0.02) & (df["JC"] >= 1),
        "cont_gap": (df["gap"] >= 0.02) & (df["I_l1"] >= 0.02),
        "vol_wash": (df["gap"] <= -0.02) & (df["G_l1"] >= 2),
        "calm_heat": (df["JC"] >= 1) & (df["heat5"] >= 0.005),
        "O_l2_lt0_AH": (df["I_l2"] < 0) & (df["AH"] >= 1),  # Cyrus-style lag pair
    }


def close_gates(df):
    """Close-knowable today → later H/I. Same-row H/I OK as features."""
    return {
        "H_ge_p2": df["H"] >= 0.02,
        "H_le_m2": df["H"] <= -0.02,
        "I_ge_p3": df["I"] >= 0.03,
        "I_le_m3": df["I"] <= -0.03,
        "G_ge2": df["G"] >= 2,
        "rvol_ge15": df["rvol"] >= 1.5,
        "up_close": (df["H"] > 0) & (df["I"] > 0),
        "dn_close": (df["H"] < 0) & (df["I"] < 0),
        "Fgreen_proxy": (df["rvol"] >= 1.5) & (df["G"] >= 1.2),
        "big_red_H": df["H"] <= -0.04,
        "big_green_H": df["H"] >= 0.04,
        "I_red_G2": (df["I"] <= -0.03) & (df["G"] >= 2),
        "H_green_G2": (df["H"] >= 0.02) & (df["G"] >= 2),
    }


PLAIN = {
    "gap_le_m3": "the stock opened at least 3% below yesterday's close",
    "gap_le_m2": "the stock opened at least 2% below yesterday's close",
    "gap_le_m1": "the stock opened at least 1% below yesterday's close",
    "gap_ge_p2": "the stock opened at least 2% above yesterday's close",
    "gap_ge_p3": "the stock opened at least 3% above yesterday's close",
    "gap_neg": "the stock opened below yesterday's close",
    "gap_pos": "the stock opened above yesterday's close",
    "J_neg": "today's open is below yesterday's open",
    "J_pos": "today's open is above yesterday's open",
    "I_l1_le_m3": "yesterday's close-to-close (column I) was −3% or worse",
    "I_l1_le_m5": "yesterday's close-to-close (column I) was −5% or worse",
    "I_l1_ge_p3": "yesterday's close-to-close (column I) was +3% or more",
    "H_l1_le_m3": "yesterday's open-to-close (column H) was −3% or worse",
    "H_l1_ge_p3": "yesterday's open-to-close (column H) was +3% or more",
    "AH_ge1": "at least one of the last six completed sessions had H ≤ −5% (Excel AH)",
    "AH_ge2": "at least two of the last six completed sessions had H ≤ −5%",
    "JC": "none of the last eight completed sessions had H < −3% (Excel JC-like)",
    "JB": "at least seven of the last eight sessions had |H| > 3% (Excel JB-like)",
    "heat_hot": "the last five completed daily I prints averaged at least +0.8%",
    "heat_cold": "the last five completed daily I prints averaged −0.8% or worse",
    "streak_up3": "the last three completed sessions were all up on I",
    "streak_dn3": "the last three completed sessions were all down on I",
    "ES_pos": "the carried 'big up-day' flag (Excel ES) is on",
    "ES_neg": "the carried 'big down-day' flag (Excel ES) is on",
    "FR": "recent volume was over ~1M and/or yesterday's relative volume ≥ 3 (Excel FR-like)",
    "rvol_l1_ge2": "yesterday's volume was at least 2× the day before",
    "G_l1_ge3": "yesterday's volume was at least 3× the day before",
    "washout": "opened ≥2% down after a −3% or worse yesterday",
    "dip_in_heat": "opened ≥1.5% down while the prior five-day I average was still green",
    "AH_and_heat": "recent −5% H days (AH) while the prior five-day I average was green",
    "AH_and_FR": "AH ≥ 1 and the FR-like volume flag is on",
    "gap_fade_up": "opened ≥2% down on a name that had no −3% H in the prior eight days",
    "cont_gap": "opened ≥2% up after a +2% yesterday",
    "vol_wash": "opened ≥2% down after a 2× volume day",
    "calm_heat": "no recent −3% H and the prior five-day I average was green",
    "O_l2_lt0_AH": "I two days ago was negative and AH ≥ 1",
    "H_ge_p2": "today's H (open→close) was at least +2%",
    "H_le_m2": "today's H was −2% or worse",
    "I_ge_p3": "today's I (close vs yesterday) was at least +3%",
    "I_le_m3": "today's I was −3% or worse",
    "G_ge2": "today's volume was at least 2× yesterday",
    "rvol_ge15": "today's volume was at least 1.5× its 20-day median",
    "up_close": "today finished green on both H and I",
    "dn_close": "today finished red on both H and I",
    "Fgreen_proxy": "today's volume was hot (rvol ≥ 1.5 and G ≥ 1.2) — F-green proxy",
    "big_red_H": "today's H was −4% or worse",
    "big_green_H": "today's H was +4% or more",
    "I_red_G2": "today's I was −3% or worse on a 2× volume day",
    "H_green_G2": "today's H was +2% or more on a 2× volume day",
}

LABEL_PLAIN = {
    "y_h1": "same-day column H (open→close)",
    "y_i1": "same-day column I (close vs yesterday)",
    "y_from_open_1d": "same-day H (buy open, sell that close)",
    "y_from_open_2d": "buy open, sell next close (2 sessions)",
    "y_from_open_3d": "buy open, sell close 3 sessions later",
    "y_from_open_1w": "buy open, sell close ~1 week later",
    "y_from_open_2w": "buy open, sell close ~2 weeks later",
    "y_i_stack_1d": "next session's I (tomorrow's daily %)",
    "y_i_stack_2d": "compound close-to-close over the next 2 sessions",
    "y_i_stack_3d": "compound close-to-close over the next 3 sessions",
    "y_i_stack_1w": "compound close-to-close over the next week",
    "y_i_stack_2w": "compound close-to-close over the next 2 weeks",
    "y_i_lead_1d": "column I one session later",
    "y_h_lead_1d": "column H one session later",
}


def _subset_stats(y, tick, dt, mask):
    yy = y[mask]
    if yy.size < 20:
        return None
    return {
        "n": int(yy.size),
        "mean": float(yy.mean()),
        "t": tstat(yy),
        "tickers": int(np.unique(tick[mask]).size),
        "win": float((yy > 0).mean()),
    }


def lottery(y, tick, dt, mask):
    yy = y[mask]
    tt = tick[mask]
    dd = dt[mask]
    if yy.size < 20:
        return 1.0, 1.0
    # winning-day concentration
    by_day = defaultdict(float)
    by_t = defaultdict(float)
    for val, t, d in zip(yy, tt, dd):
        if val > 0:
            by_day[d] += val
            by_t[t] += val
    pos = sum(by_day.values())
    if pos <= 0:
        return 0.0, 0.0
    day_share = max(by_day.values()) / pos if by_day else 0
    top5 = sorted(by_t.values(), reverse=True)[:5]
    t_share = sum(top5) / pos
    return float(day_share), float(t_share)


def harden(df, mask, ycol, cost, splits, clock):
    y_raw = df[ycol].to_numpy()
    y = y_raw - cost
    tick = df["ticker"].to_numpy()
    dt = df["date"].to_numpy()
    base = mask & np.isfinite(y)
    reasons = []
    parts = {}
    for name, sm in (
        ("all", np.ones(len(df), dtype=bool)),
        ("disc", splits["disc"]),
        ("hold", splits["hold"]),
        ("early", splits["early"]),
        ("late", splits["late"]),
        ("q1", splits["q1"]),
        ("spy_up", splits["spy_up"] if clock == "open" else splits["spy_up_today"]),
        ("spy_dn", splits["spy_dn"] if clock == "open" else splits["spy_dn_today"]),
    ):
        st = _subset_stats(y, tick, dt, base & sm)
        parts[name] = st
        if st is None and name in ("disc", "hold", "spy_up", "spy_dn"):
            reasons.append(f"{name}_thin")

    uncond = _subset_stats(y, tick, dt, np.isfinite(y) & splits["hold"])
    hold = parts.get("hold")
    disc = parts.get("disc")
    if hold is None or disc is None:
        return "THIN", reasons, parts, 1.0, 1.0
    if hold["tickers"] < MIN_TICKERS or hold["n"] < MIN_N:
        reasons.append("thin")
        return "THIN", reasons, parts, 1.0, 1.0
    if not (np.isfinite(disc["t"]) and disc["t"] >= 2):
        reasons.append("disc_t")
    if not (np.isfinite(hold["t"]) and hold["t"] >= 2):
        reasons.append("hold_t")
    if disc["mean"] <= 0:
        reasons.append("disc_sign")
    if hold["mean"] <= 0:
        reasons.append("hold_sign")
    su, sd = parts.get("spy_up"), parts.get("spy_dn")
    if su is None or sd is None or su["mean"] <= 0 or sd["mean"] <= 0:
        reasons.append("tape")
    if parts.get("early") and parts["early"]["mean"] <= 0:
        reasons.append("early")
    if parts.get("late") and parts["late"]["mean"] <= 0:
        reasons.append("late")
    if parts.get("q1") and parts["q1"]["mean"] <= 0:
        reasons.append("q1")
    if uncond and hold["mean"] - uncond["mean"] < MIN_EDGE:
        reasons.append("no_edge")
    day_s, top5 = lottery(y, tick, dt, base & splits["hold"])
    if day_s > DAY_CAP:
        reasons.append("lottery_day")
    if top5 > TOP5_CAP:
        reasons.append("ticker_ghost")
    verdict = "KEEP" if not reasons else ("THIN" if "thin" in reasons else "KILL")
    return verdict, reasons, parts, day_s, top5


def scan_gates(df, gates, labels, clock, splits, mcap):
    rows = []
    cost = np.array([cost_of(t, mcap) for t in df["ticker"]])
    for gname, gmask in gates.items():
        gmask = gmask.fillna(False).to_numpy()
        for ycol in labels:
            if ycol not in df.columns:
                continue
            v, why, parts, day_s, top5 = harden(df, gmask, ycol, cost, splits, clock)
            hold = parts.get("hold") or {}
            uncond = _subset_stats(
                df[ycol].to_numpy() - cost,
                df["ticker"].to_numpy(),
                df["date"].to_numpy(),
                np.isfinite(df[ycol].to_numpy()) & splits["hold"],
            )
            rows.append({
                "clock": clock,
                "gate": gname,
                "plain": PLAIN.get(gname, gname),
                "label": ycol,
                "label_plain": LABEL_PLAIN.get(ycol, ycol),
                "verdict": v,
                "why": why,
                "hold_mean": hold.get("mean"),
                "hold_n": hold.get("n"),
                "hold_t": hold.get("t"),
                "hold_tickers": hold.get("tickers"),
                "hold_win": hold.get("win"),
                "disc_mean": (parts.get("disc") or {}).get("mean"),
                "spy_up": (parts.get("spy_up") or {}).get("mean"),
                "spy_dn": (parts.get("spy_dn") or {}).get("mean"),
                "early": (parts.get("early") or {}).get("mean"),
                "late": (parts.get("late") or {}).get("mean"),
                "q1": (parts.get("q1") or {}).get("mean"),
                "uncond": (uncond or {}).get("mean"),
                "day_share": day_s,
                "top5_share": top5,
            })
    return rows


def scan_spearman(df, feat_cols, labels, splits):
    out = []
    hold = splits["hold"]
    disc = splits["disc"]
    for feat in feat_cols:
        x = df[feat].to_numpy()
        for ycol in labels:
            y = df[ycol].to_numpy()
            rho_h, n_h = spearman(x[hold], y[hold])
            rho_d, n_d = spearman(x[disc], y[disc])
            if not np.isfinite(rho_h) or n_h < MIN_N:
                continue
            out.append({
                "feat": feat, "label": ycol,
                "rho_hold": rho_h, "n_hold": n_h,
                "rho_disc": rho_d, "n_disc": n_d,
                "agree": (rho_h * rho_d) > 0 and abs(rho_h) >= 0.02 and abs(rho_d) >= 0.02,
            })
    out.sort(key=lambda r: -abs(r["rho_hold"]))
    return out


def quintiles(df, feat, ycol, splits, q=5):
    hold = df[splits["hold"]].copy()
    hold = hold[np.isfinite(hold[feat]) & np.isfinite(hold[ycol])]
    if len(hold) < MIN_N:
        return []
    try:
        hold["bin"] = pd.qcut(hold[feat], q, labels=False, duplicates="drop")
    except ValueError:
        return []
    rows = []
    for b, sub in hold.groupby("bin"):
        rows.append({
            "feat": feat, "label": ycol, "bin": int(b),
            "n": int(len(sub)),
            "mean": float(sub[ycol].mean()),
            "t": tstat(sub[ycol].to_numpy()),
        })
    return rows


def fmt_pct(x):
    if x is None or not np.isfinite(x):
        return "—"
    return f"{x*100:+.2f}%"


def write_report(meta, gates, spears, quints):
    keeps = [r for r in gates if r["verdict"] == "KEEP"]
    kills = [r for r in gates if r["verdict"] == "KILL"]
    thins = [r for r in gates if r["verdict"] == "THIN"]
    lines = []
    lines.append("# H/I correlation mine (A–F Yahoo seed)")
    lines.append("")
    lines.append(f"_Generated {date.today().isoformat()}. Research only. "
                 "Live `flatten_robust` frozen. Labels are Excel H and I._")
    lines.append("")
    lines.append("## Plain English")
    lines.append("")
    lines.append(
        "H is the same-day open-to-close move. I is the close versus yesterday. "
        "Both are computed from columns A–F the way the spreadsheet does "
        "`(close−open)/open` and `(close−prior close)/prior close`. "
        "Features never peek at the same row's H or I. Open rules use only "
        "what you can know at 09:30 (overnight gap, yesterday's H/I, AH, "
        "prior volume, five-day heat). Close rules may use today's H/I to "
        "forecast later sessions."
    )
    lines.append("")
    lines.append(
        f"Universe: **{meta['n_tickers']}** names, **{meta['n_rows']}** "
        f"liquid name-days, {meta['first']} → {meta['last']}. "
        f"Everyone-else holdout same-day H {fmt_pct(meta['uncond_h1'])} "
        f"(n={meta['uncond_h1_n']}); same-day I {fmt_pct(meta['uncond_i1'])}."
    )
    lines.append("")
    lines.append(
        f"Board: **KEEP {len(keeps)} · KILL {len(kills)} · THIN {len(thins)}** "
        f"across {len(gates)} gate×label cells."
    )
    lines.append("")
    if keeps:
        lines.append("### What held")
        lines.append("")
        for r in keeps:
            lines.append(
                f"- When **{r['plain']}**, **{r['label_plain']}** averaged "
                f"**{fmt_pct(r['hold_mean'])}** after fees "
                f"(n={r['hold_n']}, {r['hold_tickers']} names, "
                f"t={r['hold_t']:.2f}) vs everyone {fmt_pct(r['uncond'])}. "
                f"SPY-up {fmt_pct(r['spy_up'])} / SPY-down {fmt_pct(r['spy_dn'])}. "
                f"Clock: {r['clock']}."
            )
        lines.append("")
    else:
        lines.append("### What held")
        lines.append("")
        lines.append(
            "No new leak-free gate cleared the ship bar on this tape "
            "(ticker holdout, both halves, both SPY tapes, n + effect, "
            "no lottery / name ghost, +20 bp vs everyone after fees)."
        )
        lines.append("")

    lines.append("### Standing #144 check")
    lines.append("")
    lines.append(
        "This pass does **not** rebuild fill colors. The morning five-cell "
        "light + green O family stays the fill-based research keep from #144. "
        "What we tested here is whether **numbers** from A–F (gap, lagged H/I, "
        "AH, heat, FR-like volume) predict H/I without those highlights."
    )
    lines.append("")

    lines.append("### Strongest holdout Spearman (continuous)")
    lines.append("")
    lines.append("| feature | predicts | ρ holdout | ρ discovery | n | agree? |")
    lines.append("|---|---|---:|---:|---:|---|")
    shown = 0
    for r in spears:
        if shown >= 18:
            break
        # skip near-tautologies we already understand
        if r["feat"] in {"H", "I", "gap"} and r["label"] in {"y_h1", "y_i1"}:
            continue
        lines.append(
            f"| `{r['feat']}` | `{r['label']}` | {r['rho_hold']:+.3f} | "
            f"{r['rho_disc']:+.3f} | {r['n_hold']} | "
            f"{'yes' if r['agree'] else 'no'} |"
        )
        shown += 1
    lines.append("")
    lines.append(
        "Same-row `gap` vs `y_i1` is omitted on purpose — I contains the "
        "overnight gap by algebra. Same-row H vs I is the leftover of that "
        "identity, not a forecast."
    )
    lines.append("")

    lines.append("### Soft regimes (green / red heat)")
    lines.append("")
    lines.append(
        "Visual green/red regions are proxied by the prior five-day mean of I "
        "(knowable at the next open). We do not force a perfect coder. "
        "Quintile 0 = cold tape, quintile 4 = hot tape."
    )
    lines.append("")
    lines.append("| heat quintile | same-day H | n | t |")
    lines.append("|---:|---:|---:|---:|")
    for r in quints:
        if r["feat"] == "heat5" and r["label"] == "y_h1":
            lines.append(
                f"| {r['bin']} | {fmt_pct(r['mean'])} | {r['n']} | {r['t']:.2f} |"
            )
    lines.append("")

    # near-miss table
    near = [r for r in gates if r["verdict"] == "KILL"
            and r.get("hold_mean") and r["hold_mean"] > 0.004
            and r.get("hold_t") and r["hold_t"] >= 2]
    near.sort(key=lambda r: -(r["hold_mean"] or 0))
    lines.append("### Near-misses (holdout looked good — killed on harden)")
    lines.append("")
    if not near:
        lines.append("None with holdout mean > 40 bp and t≥2.")
        lines.append("")
    else:
        lines.append("| when | predicts | holdout | why killed |")
        lines.append("|---|---|---:|---|")
        for r in near[:20]:
            lines.append(
                f"| {r['plain']} | {r['label_plain']} | "
                f"{fmt_pct(r['hold_mean'])} (n={r['hold_n']}) | "
                f"{', '.join(r['why'][:6])} |"
            )
        lines.append("")

    lines.append("## Method")
    lines.append("")
    lines.append(
        f"A–F seed: `data/rows` (Yahoo / excel-state). Liquid filter: "
        f"prior close ≥ $2 and prior dollar volume ≥ $1M. Costs: Futubull "
        f"10 bp if Finviz mcap ≥ $300M else 30 bp (20 bp if unknown). "
        f"Discovery/holdout tickers from `holdout_split.json`. "
        f"Q3 cut {Q3.isoformat()}; Q1 cut {Q1.isoformat()}."
    )
    lines.append("")
    lines.append("Research only. No cards. No live wire.")
    OUT_MD.parent.mkdir(parents=True, exist_ok=True)
    OUT_MD.write_text("\n".join(lines) + "\n")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit-tickers", type=int, default=0)
    ap.add_argument("--min-n", type=int, default=0)
    args = ap.parse_args()
    min_n = args.min_n or MIN_N
    if min_n != MIN_N:
        globals()["MIN_N"] = min_n

    discovery, holdout = load_split()
    mcap = load_mcap()
    print("loading rows…", flush=True)
    df = load_panel(limit=args.limit_tickers)
    print(f"  raw {df['ticker'].nunique()} tickers / {len(df)} rows "
          f"{df['date'].min()} → {df['date'].max()}", flush=True)
    df = add_excel_features(df)
    df = add_labels(df)
    df = attach_spy(df)
    liq = liquid_mask(df)
    df = df.loc[liq].reset_index(drop=True)
    print(f"  liquid {df['ticker'].nunique()} / {len(df)}", flush=True)
    splits = split_masks(df, discovery, holdout)

    open_labels = [
        "y_h1", "y_i1",
        "y_from_open_1d", "y_from_open_2d", "y_from_open_3d",
        "y_from_open_1w", "y_from_open_2w",
    ]
    close_labels = [
        "y_i_lead_1d", "y_h_lead_1d",
        "y_i_stack_1d", "y_i_stack_2d", "y_i_stack_3d",
        "y_i_stack_1w", "y_i_stack_2w",
    ]

    print("scanning gates…", flush=True)
    og = {k: v for k, v in open_gates(df).items()}
    cg = {k: v for k, v in close_gates(df).items()}
    gate_rows = scan_gates(df, og, open_labels, "open", splits, mcap)
    gate_rows += scan_gates(df, cg, close_labels, "close", splits, mcap)

    feat_open = [
        "gap", "J", "H_l1", "I_l1", "I_l2", "I_l3", "H_l2",
        "AH", "heat5", "heat10", "vol20", "I_streak_l1", "ES",
        "G_l1", "rvol_l1", "FR", "JC", "JB",
    ]
    print("spearman + heat quintiles…", flush=True)
    spears = scan_spearman(df, feat_open, ["y_h1", "y_i1", "y_from_open_2d",
                                           "y_i_stack_1d", "y_i_stack_1w"], splits)
    # also close features vs next I (not same-row)
    spears += scan_spearman(df, ["H", "I", "G", "rvol", "K", "M"],
                            ["y_i_lead_1d", "y_h_lead_1d", "y_i_stack_1d",
                             "y_i_stack_1w"], splits)
    spears.sort(key=lambda r: -abs(r["rho_hold"]))
    quints = quintiles(df, "heat5", "y_h1", splits)
    quints += quintiles(df, "gap", "y_h1", splits)

    cost = np.array([cost_of(t, mcap) for t in df["ticker"]])
    hold = splits["hold"]
    yh = df["y_h1"].to_numpy() - cost
    yi = df["y_i1"].to_numpy() - cost
    meta = {
        "n_tickers": int(df["ticker"].nunique()),
        "n_rows": int(len(df)),
        "first": str(df["date"].min()),
        "last": str(df["date"].max()),
        "uncond_h1": float(np.nanmean(yh[hold])),
        "uncond_h1_n": int(np.isfinite(yh[hold]).sum()),
        "uncond_i1": float(np.nanmean(yi[hold])),
        "seed": "yahoo_rows",
        "from_cache": False,
    }
    write_report(meta, gate_rows, spears, quints)
    payload = {
        "meta": meta,
        "keeps": [r for r in gate_rows if r["verdict"] == "KEEP"],
        "kills": [r for r in gate_rows if r["verdict"] == "KILL"],
        "thin": [r for r in gate_rows if r["verdict"] == "THIN"],
        "spearman": spears[:80],
        "quintiles": quints,
    }
    OUT_JSON.write_text(json.dumps(payload, indent=2, default=str))
    nkeep = sum(1 for r in gate_rows if r["verdict"] == "KEEP")
    nkill = sum(1 for r in gate_rows if r["verdict"] == "KILL")
    print(f"KEEP {nkeep}  KILL {nkill}  → {OUT_MD}")


if __name__ == "__main__":
    main()
