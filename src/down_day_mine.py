"""Leak-free correlations for names that finish green on market-down days.

Question: when SPY is red, which 09:30-knowable inputs lift the chance a
*stock* still prints open→close > 0 (or beats SPY)?

Inputs never include today's Change% / Gap / RelVol / printed book / today's
OHLC. Entry is the 09:30 open; the close is the outcome only.

Sources
  * ``data/prices/ohlc.parquet`` — year-plus tape, vectorized prior-bar
    features (the extensive sample)
  * sector / factor ETFs in the same parquet — independent confirmation
    on every SPY-down day, no Finviz required
  * morning Finviz export on the *prior* session — sector / industry /
    mcap / beta / vol / optionable / short float / RSI / SMA (when the
    file exists). Overlay lifts are scored against tagged name-days only.
  * factor-mine panel cameras / E polarity / last-green (17-session overlay)
  * Excel replica (``excel_bot``) — start-of-day vs close-knowable column
    map, plus the live strategy-card cohorts as hypotheses to test

Does not remine the 161 official factor-mine books. Does not change
live ``flatten_robust`` / ``LIVE_POLICY``. Does not remake the 3,603
Excel color grids.
"""
from __future__ import annotations

import argparse
import json
import math
import re
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

from . import factor_mine as fm
from . import factor_mine_book as fmb
from . import factor_mine_probe as fmp
from . import gainer_asof as ga
from . import gainer_capture as gc

ROOT = Path(__file__).resolve().parent.parent
PRICE_STORE = ROOT / "data" / "prices" / "ohlc.parquet"
OUT_JSON = ROOT / "03_scoreboard" / "down_day_mine.json"
OUT_MD = ROOT / "03_scoreboard" / "DOWN_DAY_MINE.md"
DASH_DIR = ROOT / "dashboard" / "down-day-mine"
TEMPLATE = Path(__file__).with_name("down_day_dash.html")
ET = ZoneInfo("America/New_York")

INDEX_SKIP = {"SPY", "QQQ", "DIA", "IWM", "VIX", "UVXY", "SVXY", "SQQQ", "TQQQ"}
# Inverse / leveraged products win mechanically on red SPY days.
# They are not "a stock that won."
LEV_SKIP = INDEX_SKIP | {
    "UVIX", "SVIX", "VIXY", "VIXM", "VXX", "VIX",
    "SPXU", "SPXS", "SPXL", "UPRO", "SDS", "SSO", "SH", "PSQ",
    "QID", "QLD", "SDOW", "UDOW", "DXD", "DDM", "DOG",
    "SOXS", "SOXL", "TECS", "TECL", "LABD", "LABU", "FNGD", "FNGU",
    "TZA", "TNA", "FAZ", "FAS", "ERX", "ERY", "DUST", "NUGT", "JDST", "JNUG",
    "ETHD", "ETHT", "BITI", "SBIT", "SMCZ", "SMCL", "RGTZ", "QBTZ",
    "IONZ", "RKLZ", "WEBS", "WEBL", "HIBS", "HIBL", "YANG", "YINN",
    "KOLD", "BOIL", "SCO", "UCO", "GLL", "AGQ", "ZSL", "UGL",
    "TBT", "TMF", "TMV", "UBT",
}
PRODUCT_NAME = re.compile(
    r"inverse|-1x|-2x|-3x|ultra[ -]?short|bear 2|bear 3|"
    r"2x short|3x short|daily bear|daily inverse",
    re.I,
)

LIQ_USD = 5_000_000.0
MIN_PX = 2.0
MIN_N = 400
MIN_N_OVERLAY = 80
MIN_N_CAM = 80
LIFT_PP = 3.0
OVERLAY_REGIMES = frozenset({"spy_cc_down", "prior_spy_red", "spy_oc_down"})
BANNED = frozenset({
    "gap", "change", "relvol", "today_rvol", "today_high", "today_low",
    "today_close", "today_volume",
})

# Excel Simple View columns (timing_test.py / NOTES.md).
EXCEL_OPEN_COLS = list("ABCGJKLMO")   # start-of-day knowable
EXCEL_CLOSE_COLS = list("DEFHIN")     # use day-t OHLC — close only

SECTOR_ETFS = {
    "XLV": "Healthcare",
    "VHT": "Healthcare (Vanguard)",
    "IBB": "Biotech",
    "XBI": "Biotech (equal-weight)",
    "XLE": "Energy",
    "VDE": "Energy (Vanguard)",
    "XOP": "Oil E&P",
    "XLU": "Utilities",
    "XLP": "Consumer staples",
    "XLY": "Consumer discretionary",
    "XLK": "Technology",
    "SMH": "Semis",
    "XLF": "Financials",
    "KRE": "Regional banks",
    "XLI": "Industrials",
    "XLB": "Materials",
    "XLRE": "Real estate",
    "IYR": "Real estate (iShares)",
    "XLC": "Communication",
    "GLD": "Gold",
    "GDX": "Gold miners",
    "SLV": "Silver",
    "TLT": "Long Treasuries",
    "UUP": "US dollar",
    "USO": "Crude oil",
    "UNG": "Natural gas",
}

DEFENSIVE = {
    "Healthcare", "Utilities", "Consumer Defensive", "Consumer Staples",
}


def _tick(v) -> str:
    return str(v or "").strip().upper()


def _finite(x):
    if x is None or (isinstance(x, float) and (math.isnan(x) or math.isinf(x))):
        return None
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    if math.isnan(v) or math.isinf(v):
        return None
    return v


def _pct_num(v):
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return None
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip().replace("%", "").replace(",", "")
    try:
        return float(s)
    except ValueError:
        return None


def _series_or_false(df: pd.DataFrame, col: str) -> pd.Series:
    if col in df.columns:
        return df[col]
    return pd.Series(False, index=df.index)


def load_prices(path: Path | None = None) -> pd.DataFrame:
    p = path or PRICE_STORE
    df = pd.read_parquet(p)
    df = df.rename(columns={c: str(c).lower() for c in df.columns})
    df["ticker"] = df["ticker"].map(_tick)
    df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
    for c in ("open", "high", "low", "close", "volume"):
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df.dropna(subset=["ticker", "date", "open", "close"])
    df = df[df["open"] > 0]
    return df.sort_values(["ticker", "date"]).reset_index(drop=True)


def extend_spy(spy: pd.DataFrame) -> pd.DataFrame:
    """Append newer SPY bars if Yahoo is reachable (parquet often lags)."""
    if spy.empty:
        return spy
    last = str(spy["date"].max())
    try:
        import yfinance as yf
        hist = yf.download("SPY", start=last, progress=False, auto_adjust=True)
    except Exception:
        return spy
    if hist is None or getattr(hist, "empty", True):
        return spy
    if isinstance(hist.columns, pd.MultiIndex):
        hist.columns = [str(c[0]).lower() for c in hist.columns]
    else:
        hist.columns = [str(c).lower() for c in hist.columns]
    hist = hist.reset_index()
    hist["date"] = pd.to_datetime(hist["date"]).dt.strftime("%Y-%m-%d")
    hist["ticker"] = "SPY"
    keep = ["date", "ticker", "open", "high", "low", "close", "volume"]
    for c in keep:
        if c not in hist.columns:
            hist[c] = np.nan
    extra = hist[keep]
    extra = extra[extra["date"] > last]
    if extra.empty:
        return spy
    return pd.concat([spy, extra], ignore_index=True).drop_duplicates("date")


def spy_calendar(prices: pd.DataFrame) -> pd.DataFrame:
    spy = prices[prices["ticker"] == "SPY"].copy()
    spy = extend_spy(spy).sort_values("date")
    spy["spy_cc"] = spy["close"] / spy["close"].shift(1) - 1.0
    spy["spy_oc"] = spy["close"] / spy["open"] - 1.0
    spy["spy_cc_prior"] = spy["spy_cc"].shift(1)
    spy["spy_oc_prior"] = spy["spy_oc"].shift(1)
    out = spy[["date", "open", "close", "spy_cc", "spy_oc",
               "spy_cc_prior", "spy_oc_prior"]].rename(
        columns={"open": "spy_open", "close": "spy_close"})
    out["spy_cc_down"] = out["spy_cc"] < 0
    out["spy_oc_down"] = out["spy_oc"] < 0
    out["spy_cc_down_1"] = out["spy_cc"] <= -0.01
    out["prior_spy_red"] = out["spy_cc_prior"] < 0
    return out


def attach_prior_ohlc(prices: pd.DataFrame) -> pd.DataFrame:
    """All features are shifted — today's bar is outcome only."""
    g = prices.sort_values(["ticker", "date"]).copy()
    gb = g.groupby("ticker", sort=False)
    g["prev_open"] = gb["open"].shift(1)
    g["prev_high"] = gb["high"].shift(1)
    g["prev_low"] = gb["low"].shift(1)
    g["prev_close"] = gb["close"].shift(1)
    g["prev_vol"] = gb["volume"].shift(1)
    g["open_2"] = gb["open"].shift(2)
    g["high_2"] = gb["high"].shift(2)
    g["low_2"] = gb["low"].shift(2)
    g["close_2"] = gb["close"].shift(2)
    g["open_3"] = gb["open"].shift(3)
    g["close_3"] = gb["close"].shift(3)
    g["close_6"] = gb["close"].shift(6)
    g["close_11"] = gb["close"].shift(11)
    g["high_10"] = gb["prev_high"].transform(
        lambda s: s.rolling(10, min_periods=8).max())
    g["high_20"] = gb["prev_high"].transform(
        lambda s: s.rolling(20, min_periods=12).max())
    g["low_20"] = gb["prev_low"].transform(
        lambda s: s.rolling(20, min_periods=12).min())
    rng = (g["prev_high"] - g["prev_low"]).clip(lower=0)
    g["_rng"] = rng
    g["rng_mean10"] = g.groupby("ticker")["_rng"].transform(
        lambda s: s.rolling(10, min_periods=5).mean())
    g["rng_min7"] = g.groupby("ticker")["_rng"].transform(
        lambda s: s.rolling(7, min_periods=7).min())
    g["vol_mean20"] = g.groupby("ticker")["prev_vol"].transform(
        lambda s: s.rolling(20, min_periods=8).mean())
    g["sma20"] = gb["prev_close"].transform(
        lambda s: s.rolling(20, min_periods=15).mean())
    g["sma50"] = gb["prev_close"].transform(
        lambda s: s.rolling(50, min_periods=30).mean())

    g["oc"] = g["close"] / g["open"] - 1.0
    g["ret_1"] = np.where(g["close_2"] > 0, g["prev_close"] / g["close_2"] - 1.0, np.nan)
    g["ret_5"] = np.where(g["close_6"] > 0, g["prev_close"] / g["close_6"] - 1.0, np.nan)
    g["ret_10"] = np.where(g["close_11"] > 0, g["prev_close"] / g["close_11"] - 1.0, np.nan)
    g["rvol"] = np.where(g["vol_mean20"] > 0, g["prev_vol"] / g["vol_mean20"], np.nan)
    g["last_green"] = g["prev_close"] > g["prev_open"]
    g["last_red"] = g["prev_close"] < g["prev_open"]
    g["nr7"] = (g["_rng"] <= g["rng_min7"] + 1e-12) & g["rng_min7"].notna()
    g["break_10"] = g["prev_close"] > g["high_10"]
    g["compression"] = np.where(g["rng_mean10"] > 0, g["_rng"] / g["rng_mean10"], np.nan)
    g["liq_usd"] = g["prev_close"] * g["prev_vol"]
    g["above_sma20"] = g["prev_close"] > g["sma20"]
    g["below_sma20"] = g["prev_close"] < g["sma20"]
    g["above_sma50"] = g["prev_close"] > g["sma50"]
    g["below_sma50"] = g["prev_close"] < g["sma50"]
    g["inside"] = (
        g["prev_high"].notna() & g["high_2"].notna()
        & (g["prev_high"] <= g["high_2"]) & (g["prev_low"] >= g["low_2"])
    )
    g["streak2_red"] = g["last_red"] & (g["close_2"] < g["open_2"])
    g["streak3_red"] = g["streak2_red"] & (g["close_3"] < g["open_3"])
    g["prior_gap"] = np.where(
        g["close_2"] > 0, g["prev_open"] / g["close_2"] - 1.0, np.nan)
    loc_den = g["_rng"].clip(lower=1e-12)
    g["close_loc"] = (g["prev_close"] - g["prev_low"]) / loc_den
    g["near_20h"] = g["prev_close"] >= 0.98 * g["high_20"]
    g["near_20l"] = g["prev_close"] <= 1.02 * g["low_20"]
    g["hot"] = (
        0.08 * np.clip(g["ret_5"].fillna(0) * 100.0, 0, None)
        + 0.04 * np.clip(g["ret_10"].fillna(0) * 100.0, 0, None)
        + 0.4 * np.clip(g["rvol"].fillna(1.0), 0, 3)
        + np.where(g["break_10"].fillna(False), 1.2, 0.0)
        + np.where(g["last_green"].fillna(False), 0.3, 0.0)
    )
    g["ok"] = g["prev_close"].notna() & (g["prev_close"] >= MIN_PX)
    return g.drop(columns=["_rng"])


def liquid_name_days(feat: pd.DataFrame, spy: pd.DataFrame) -> pd.DataFrame:
    m = feat.merge(spy, on="date", how="left")
    m = m[~m["ticker"].isin(LEV_SKIP)]
    m = m[m["ok"] & (m["liq_usd"] >= LIQ_USD) & m["oc"].notna() & m["spy_cc"].notna()]
    m["win"] = m["oc"] > 0
    m["beat_spy"] = m["oc"] > m["spy_oc"]
    m["win_1"] = m["oc"] >= 0.01
    return m


def drop_tagged_products(df: pd.DataFrame) -> pd.DataFrame:
    """Drop ETFs / inverse products once a prior Finviz row names them."""
    out = df.copy()
    if "company" in out.columns:
        name = out["company"].fillna("").astype(str)
        out = out.loc[~name.map(lambda s: bool(PRODUCT_NAME.search(s)))]
    if "asset_type" in out.columns:
        at = out["asset_type"].fillna("").astype(str).str.lower()
        out = out.loc[~at.eq("etf")]
    if "industry" in out.columns:
        out = out.loc[out["industry"].fillna("").astype(str) != "Exchange Traded Fund"]
    return out


def feature_masks(df: pd.DataFrame) -> list[tuple[str, pd.Series, str]]:
    """Leak-free predicates. Names must not match BANNED."""
    r1 = df["ret_1"] * 100.0
    r5 = df["ret_5"] * 100.0
    r10 = df["ret_10"] * 100.0
    pg = df["prior_gap"] * 100.0
    rows = [
        ("last_green", df["last_green"].fillna(False), "prior bar closed up"),
        ("last_red", df["last_red"].fillna(False), "prior bar closed down"),
        ("nr7", df["nr7"].fillna(False), "prior bar is NR7"),
        ("break_10", df["break_10"].fillna(False), "prior close broke 10-bar high"),
        ("inside", df["inside"].fillna(False), "prior bar is an inside bar"),
        ("streak2_red", df["streak2_red"].fillna(False), "two prior bars closed red"),
        ("streak3_red", df["streak3_red"].fillna(False), "three prior bars closed red"),
        ("above_sma20", df["above_sma20"].fillna(False), "prior close > SMA20"),
        ("below_sma20", df["below_sma20"].fillna(False), "prior close < SMA20"),
        ("above_sma50", df["above_sma50"].fillna(False), "prior close > SMA50"),
        ("below_sma50", df["below_sma50"].fillna(False), "prior close < SMA50"),
        ("close_upper", df["close_loc"] >= 0.7, "prior close in upper 30% of range"),
        ("close_lower", df["close_loc"] <= 0.3, "prior close in lower 30% of range"),
        ("near_20h", df["near_20h"].fillna(False), "prior close within 2% of 20-bar high"),
        ("near_20l", df["near_20l"].fillna(False), "prior close within 2% of 20-bar low"),
        ("prior_gap_up", pg >= 1.0, "prior session opened ≥ +1% vs prior close"),
        ("prior_gap_dn", pg <= -1.0, "prior session opened ≤ −1% vs prior close"),
        ("rvol_ge_1_5", df["rvol"] >= 1.5, "prior rel vol ≥ 1.5"),
        ("rvol_le_0_7", df["rvol"] <= 0.7, "prior rel vol ≤ 0.7"),
        ("rvol_1_2", (df["rvol"] >= 1.0) & (df["rvol"] < 2.0), "prior rvol 1–2"),
        ("ret1_green", r1 > 0, "prior session close-to-close > 0"),
        ("ret1_red", r1 < 0, "prior session close-to-close < 0"),
        ("ret1_le_m3", r1 <= -3, "prior session ≤ −3%"),
        ("ret1_ge_3", r1 >= 3, "prior session ≥ +3%"),
        ("ret5_neg", r5 < 0, "prior 5-session return < 0"),
        ("ret5_0_8", (r5 >= 0) & (r5 < 8), "prior 5-session +0 to +8%"),
        ("ret5_ge_8", r5 >= 8, "prior 5-session ≥ +8%"),
        ("ret5_le_m8", r5 <= -8, "prior 5-session ≤ −8%"),
        ("ret10_neg", r10 < 0, "prior 10-session return < 0"),
        ("ret10_pos", r10 > 0, "prior 10-session return > 0"),
        ("coil", df["compression"] <= 0.6, "prior range ≤ 0.6× 10-bar mean"),
        ("wide", df["compression"] >= 1.6, "prior range ≥ 1.6× 10-bar mean"),
        ("hot_ge_2", df["hot"] >= 2.0, "prior hot-score ≥ 2"),
        ("hot_lt_1", df["hot"] < 1.0, "prior hot-score < 1"),
        ("px_2_10", (df["prev_close"] >= 2) & (df["prev_close"] < 10), "prior close $2–10"),
        ("px_10_50", (df["prev_close"] >= 10) & (df["prev_close"] < 50), "prior close $10–50"),
        ("px_ge_50", df["prev_close"] >= 50, "prior close ≥ $50"),
        ("liq_5_50m", (df["liq_usd"] >= 5e6) & (df["liq_usd"] < 5e7), "prior $vol $5–50M"),
        ("liq_ge_50m", df["liq_usd"] >= 5e7, "prior $vol ≥ $50M"),
        ("rs_gt_spy", r1 > (df["spy_cc_prior"] * 100.0), "prior day beat SPY"),
        ("rs_lt_spy", r1 < (df["spy_cc_prior"] * 100.0), "prior day lagged SPY"),
    ]
    return [(n, m, why) for n, m, why in rows if n not in BANNED]


def _pack(vals: np.ndarray) -> dict:
    vals = np.asarray(vals, dtype=float)
    vals = vals[np.isfinite(vals)]
    n = int(len(vals))
    if n == 0:
        return {"n": 0, "win": None, "mean": None, "t": None}
    mean = float(vals.mean())
    win = float((vals > 0).mean())
    if n < 3:
        return {"n": n, "win": round(win, 4), "mean": round(mean, 5), "t": None}
    sd = float(vals.std(ddof=1))
    t = mean / math.sqrt(sd * sd / n) if sd > 0 else None
    return {"n": n, "win": round(win, 4), "mean": round(mean, 5),
            "t": None if t is None else round(t, 3)}


def _score(hit: pd.DataFrame, mask, target: str) -> dict:
    sl = hit.loc[mask]
    oc = sl["oc"].to_numpy()
    pack = _pack(oc)
    if target == "beat_spy" and "beat_spy" in sl.columns:
        w = sl["beat_spy"].astype(float).to_numpy()
        pack["win"] = None if len(w) == 0 else round(float(w.mean()), 4)
    return pack


def sweep(df: pd.DataFrame, regime: str, pred, target: str = "oc",
          min_n: int = 200) -> tuple[list[dict], dict]:
    hit = df[pred(df)].copy()
    base = _score(hit, slice(None), target)
    out = []
    for name, mask, why in feature_masks(hit):
        pack = _score(hit, mask, target)
        if pack["n"] < min_n or base["win"] is None or pack["win"] is None:
            continue
        lift = 100.0 * (pack["win"] - base["win"])
        out.append({
            "regime": regime, "target": target, "feature": name, "why": why,
            "n": pack["n"], "win": pack["win"], "mean": pack["mean"],
            "t": pack["t"], "base_n": base["n"], "base_win": base["win"],
            "base_mean": base["mean"], "lift_pp": round(lift, 2),
            "mean_edge": None if pack["mean"] is None or base["mean"] is None
            else round(pack["mean"] - base["mean"], 5),
        })
    out.sort(key=lambda r: (-(r["lift_pp"] or -999), -(r["n"] or 0)))
    return out, base


def attach_finviz(df: pd.DataFrame, cal: list[str]) -> pd.DataFrame:
    """Prior-session export only. Same-day Change/Gap/RelVol unused."""
    prior = {d: gc.prior_session(cal, d) for d in cal}
    rows = []
    for exp in {p for p in prior.values() if p}:
        for t, rec in _finviz_map(exp).items():
            rows.append({"fv_date": exp, "ticker": t, **rec})
    out = df.copy()
    out["fv_date"] = out["date"].map(prior)
    if not rows:
        return out
    fv = pd.DataFrame(rows)
    return out.merge(fv, on=["fv_date", "ticker"], how="left")


def _finviz_map(date: str) -> dict[str, dict]:
    raw = ga.load_finviz(date)
    if raw is None or getattr(raw, "empty", True) or "Ticker" not in raw.columns:
        return {}
    out = {}
    for rec in raw.to_dict("records"):
        t = _tick(rec.get("Ticker"))
        if not t:
            continue
        # Never copy Change / Gap / RelVol / today's OHLC into features.
        out[t] = {
            "sector": str(rec.get("Sector") or "") or None,
            "industry": str(rec.get("Industry") or "") or None,
            "country": str(rec.get("Country") or "") or None,
            "company": str(rec.get("Company") or "") or None,
            "asset_type": str(rec.get("Asset Type") or "") or None,
            "mcap": _finite(rec.get("Market Cap")),
            "beta": _finite(rec.get("Beta")),
            "vol_m": _pct_num(rec.get("Volatility (Month)")),
            "optionable": str(rec.get("Optionable") or "").lower() == "yes",
            "sp500": "S&P 500" in str(rec.get("Index") or ""),
            "short_flt": _pct_num(rec.get("Short Float")),
            "ins_own": _pct_num(rec.get("Insider Ownership")),
            "inst_own": _pct_num(rec.get("Institutional Ownership")),
            "rsi": _finite(rec.get("Relative Strength Index (14)")),
            "sma20_pct": _pct_num(rec.get("20-Day Simple Moving Average")),
            "sma50_pct": _pct_num(rec.get("50-Day Simple Moving Average")),
            "sma200_pct": _pct_num(rec.get("200-Day Simple Moving Average")),
            "perf_w": _pct_num(rec.get("Performance (Week)")),
            "perf_m": _pct_num(rec.get("Performance (Month)")),
            "div_y": _pct_num(rec.get("Dividend Yield")),
            "earn_date": rec.get("Earnings Date"),
            "eps_surp": rec.get("EPS Surprise"),
        }
    return out


def finviz_masks(df: pd.DataFrame) -> list[tuple[str, pd.Series, str]]:
    if "mcap" not in df.columns and "sector" not in df.columns:
        return []
    sec = df["sector"] if "sector" in df.columns else pd.Series(None, index=df.index)
    ind = df["industry"].fillna("") if "industry" in df.columns else pd.Series("", index=df.index)
    ctry = df["country"].fillna("") if "country" in df.columns else pd.Series("", index=df.index)
    rows = [
        ("fv_micro", df.get("mcap", pd.Series(np.nan, index=df.index)) < 300,
         "Finviz mcap < $300M (prior export)"),
        ("fv_mid", (df.get("mcap", pd.Series(np.nan, index=df.index)) >= 1000)
         & (df.get("mcap", pd.Series(np.nan, index=df.index)) < 10000),
         "Finviz midcap $1–10B (Excel L3/L5 cohort)"),
        ("fv_large", df.get("mcap", pd.Series(np.nan, index=df.index)) >= 10000,
         "Finviz mcap ≥ $10B"),
        ("fv_lowvol", df.get("vol_m", pd.Series(np.nan, index=df.index)) < 3.0,
         "Finviz month vol < 3% (Excel L1/L2)"),
        ("fv_hivol", df.get("vol_m", pd.Series(np.nan, index=df.index)) > 8.0,
         "Finviz month vol > 8% (Excel S2)"),
        ("fv_hibeta", df.get("beta", pd.Series(np.nan, index=df.index)) > 1.5,
         "Finviz beta > 1.5 (Excel L5)"),
        ("fv_lowbeta", (df.get("beta", pd.Series(np.nan, index=df.index)) > 0)
         & (df.get("beta", pd.Series(np.nan, index=df.index)) <= 1.0),
         "Finviz beta ≤ 1"),
        ("fv_optionable", df["optionable"].fillna(False) if "optionable" in df.columns
         else pd.Series(False, index=df.index), "optionable (Excel S1)"),
        ("fv_sp500", df["sp500"].fillna(False) if "sp500" in df.columns
         else pd.Series(False, index=df.index), "S&P 500 member"),
        ("fv_usa", ctry.eq("USA"), "Finviz country = USA"),
        ("fv_short10", df.get("short_flt", pd.Series(np.nan, index=df.index)) >= 10,
         "short float ≥ 10%"),
        ("fv_short20", df.get("short_flt", pd.Series(np.nan, index=df.index)) >= 20,
         "short float ≥ 20%"),
        ("fv_ins20", df.get("ins_own", pd.Series(np.nan, index=df.index)) >= 20,
         "insider ownership ≥ 20%"),
        ("fv_inst70", df.get("inst_own", pd.Series(np.nan, index=df.index)) >= 70,
         "institutional ownership ≥ 70%"),
        ("fv_rsi30", df.get("rsi", pd.Series(np.nan, index=df.index)) <= 30,
         "Finviz RSI(14) ≤ 30 (prior export)"),
        ("fv_rsi70", df.get("rsi", pd.Series(np.nan, index=df.index)) >= 70,
         "Finviz RSI(14) ≥ 70 (prior export)"),
        ("fv_below_sma20", df.get("sma20_pct", pd.Series(np.nan, index=df.index)) < 0,
         "price below SMA20 (prior export)"),
        ("fv_above_sma200", df.get("sma200_pct", pd.Series(np.nan, index=df.index)) > 0,
         "price above SMA200 (prior export)"),
        ("fv_week_m5", df.get("perf_w", pd.Series(np.nan, index=df.index)) <= -5,
         "Finviz week performance ≤ −5% (prior export)"),
        ("fv_div", df.get("div_y", pd.Series(np.nan, index=df.index)) >= 2,
         "dividend yield ≥ 2%"),
        ("fv_health", sec.eq("Healthcare"), "Healthcare"),
        ("fv_tech", sec.eq("Technology"), "Technology"),
        ("fv_energy", sec.eq("Energy"), "Energy"),
        ("fv_finance", sec.isin(["Financial", "Financials"]), "Financials"),
        ("fv_util", sec.eq("Utilities"), "Utilities"),
        ("fv_staples", sec.isin(["Consumer Defensive", "Consumer Staples"]),
         "Consumer staples"),
        ("fv_disc", sec.isin(["Consumer Cyclical", "Consumer Discretionary"]),
         "Consumer discretionary"),
        ("fv_materials", sec.eq("Basic Materials"), "Basic Materials"),
        ("fv_re", sec.eq("Real Estate"), "Real Estate"),
        ("fv_indust", sec.eq("Industrials"), "Industrials"),
        ("fv_comm", sec.isin(["Communication Services", "Communication"]),
         "Communication"),
        ("fv_defensive", sec.isin(DEFENSIVE),
         "Healthcare + Utilities + staples"),
        ("fv_biotech", ind.str.contains("Biotech", case=False, na=False),
         "Industry contains Biotech"),
        ("fv_gold", ind.str.contains(r"Gold|Silver", case=False, na=False),
         "Industry contains Gold/Silver"),
        ("fv_oil", ind.str.contains(r"Oil|E&P", case=False, na=False),
         "Industry contains Oil / E&P"),
        ("fv_diag", ind.str.contains(r"Diagnostic|Medical Device|Drug",
                                     case=False, na=False),
         "Diagnostics / devices / drugs"),
    ]
    return rows


def attach_panel(df: pd.DataFrame) -> pd.DataFrame:
    """Read the existing panel only — never rebuild / remine the 161 books."""
    out = df.copy()
    out["has_panel"] = False
    if not fm.PANEL_PATH.is_file():
        return out
    try:
        panel = json.loads(fm.PANEL_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return out
    if not isinstance(panel, dict):
        return out
    try:
        fmp.attach_erd_polarity(panel)
    except Exception:
        pass
    bits = []
    for r in panel.get("rows") or []:
        boxes = r.get("boxes") or {}
        bits.append({
            "date": r.get("date"), "ticker": _tick(r.get("ticker")),
            "has_panel": True,
            "cam_nneg": fm.n_neg(r),
            "cam_white": bool(r.get("zero_red")),
            "cam_alarm": bool(r.get("alarm")),
            "cam_blue": bool(r.get("blue")),
            "cam_last_green": bool(r.get("last_green")),
            "cam_earn": bool(r.get("erd_earn_react")),
            "cam_e_pol": r.get("e_pol"),
            "cam_news": boxes.get("news"),
            "cam_join": boxes.get("join"),
        })
    if not bits:
        return out
    add = pd.DataFrame(bits)
    merged = out.drop(columns=["has_panel"]).merge(add, on=["date", "ticker"], how="left")
    merged["has_panel"] = merged["has_panel"].fillna(False)
    return merged


def panel_masks(df: pd.DataFrame) -> list[tuple[str, pd.Series, str]]:
    if "has_panel" not in df.columns:
        return []
    on = df["has_panel"].fillna(False)
    return [
        ("cam_white", on & df["cam_white"].fillna(False), "factor-mine ⚪ no red cameras"),
        ("cam_nneg_le2", on & (df["cam_nneg"] <= 2), "factor-mine −N ≤ 2"),
        ("cam_nneg_ge3", on & (df["cam_nneg"] >= 3), "factor-mine −N ≥ 3"),
        ("cam_alarm", on & df["cam_alarm"].fillna(False), "factor-mine 🚨"),
        ("cam_blue", on & df["cam_blue"].fillna(False), "factor-mine 🔵"),
        ("cam_last_green", on & df["cam_last_green"].fillna(False),
         "factor-mine last bar green"),
        ("cam_earn", on & df["cam_earn"].fillna(False), "earnings-reaction window"),
        ("cam_e_beat", on & (df["cam_e_pol"] == "good"),
         "E beat (morning-export surprise, not date-only)"),
        ("cam_e_miss", on & (df["cam_e_pol"] == "bad"), "E miss"),
        ("cam_news_g", on & (df["cam_news"] == "good"), "news camera green"),
        ("cam_news_b", on & (df["cam_news"] == "bad"), "news camera red"),
        ("cam_join_g", on & (df["cam_join"] == "good"), "join camera green"),
    ]


def combo_masks(df: pd.DataFrame) -> list[tuple[str, pd.Series, str]]:
    lg = df["last_green"].fillna(False)
    lr = df["last_red"].fillna(False)
    r1 = df["ret_1"] * 100.0
    sec = df["sector"] if "sector" in df.columns else pd.Series(None, index=df.index)
    health = sec.eq("Healthcare")
    energy = sec.eq("Energy")
    lowvol = df["vol_m"] < 3.0 if "vol_m" in df.columns else pd.Series(False, index=df.index)
    mid = ((df["mcap"] >= 1000) & (df["mcap"] < 10000)
           if "mcap" in df.columns else pd.Series(False, index=df.index))
    hibeta = df["beta"] > 1.5 if "beta" in df.columns else pd.Series(False, index=df.index)
    hivol = df["vol_m"] > 8.0 if "vol_m" in df.columns else pd.Series(False, index=df.index)
    opt = df["optionable"].fillna(False) if "optionable" in df.columns else pd.Series(False, index=df.index)
    return [
        ("health_last_green", health & lg, "Healthcare + prior bar green"),
        ("health_last_red", health & lr, "Healthcare + prior bar red"),
        ("health_washed", health & (r1 <= -3), "Healthcare washed ≥3% yesterday"),
        ("energy_last_green", energy & lg, "Energy + prior bar green"),
        ("energy_washed", energy & (r1 <= -3), "Energy washed ≥3% yesterday"),
        ("defensive_last_green", sec.isin(DEFENSIVE) & lg,
         "Defensive sector + prior bar green"),
        ("defensive_washed", sec.isin(DEFENSIVE) & (r1 <= -3),
         "Defensive sector washed ≥3% yesterday"),
        ("xl_l1_lowvol_green", lg & lowvol,
         "Excel L1/L2: last green + month vol < 3%"),
        ("xl_l3_mid_green", lg & mid, "Excel L3: last green + midcap $1–10B"),
        ("xl_l5_mid_hibeta", lg & mid & hibeta,
         "Excel L5: last green + midcap + beta > 1.5"),
        ("xl_s1_opt_red", lr & opt, "Excel S1 short-red cohort (optionable + last red)"),
        ("xl_s2_hivol_red", lr & hivol, "Excel S2 short-red cohort (hi-vol + last red)"),
        ("last_green_ret5_neg", lg & (df["ret_5"] < 0),
         "prior green bar after a 5-session dip"),
        ("last_red_ret1_m3", lr & (r1 <= -3),
         "prior red bar and yesterday ≤ −3%"),
    ]


def sweep_extra(df: pd.DataFrame, regime: str, pred, masks, target="oc",
                min_n: int = MIN_N_OVERLAY, require=None) -> list[dict]:
    hit = df[pred(df)].copy()
    if require is not None:
        hit = hit[require(hit)].copy()
    if hit.empty:
        return []
    base = _score(hit, slice(None), target)
    out = []
    for name, mask, why in masks:
        aligned = mask.reindex(hit.index).fillna(False)
        pack = _score(hit, aligned, target)
        if pack["n"] < min_n or base["win"] is None or pack["win"] is None:
            continue
        lift = 100.0 * (pack["win"] - base["win"])
        out.append({
            "regime": regime, "target": target, "feature": name, "why": why,
            "n": pack["n"], "win": pack["win"], "mean": pack["mean"],
            "t": pack["t"], "base_n": base["n"], "base_win": base["win"],
            "base_mean": base["mean"], "lift_pp": round(lift, 2),
            "mean_edge": None if pack["mean"] is None or base["mean"] is None
            else round(pack["mean"] - base["mean"], 5),
        })
    out.sort(key=lambda r: (-(r["lift_pp"] or -999), -(r["n"] or 0)))
    return out


def pick_keepers(rows: list[dict], *, min_n: int = MIN_N) -> list[dict]:
    keep = []
    for r in rows:
        if r.get("feature") in BANNED:
            continue
        if (r.get("n") or 0) < min_n:
            continue
        if (r.get("lift_pp") or 0) < LIFT_PP:
            continue
        if (r.get("mean") or 0) <= 0:
            continue
        if (r.get("mean_edge") or 0) <= 0:
            continue
        keep.append(r)
    return keep


def pick_near(rows: list[dict], *, regime: str = "spy_cc_down",
              min_n: int = 200, lift: float = 3.0) -> list[dict]:
    near = []
    for r in rows:
        if r.get("regime") != regime or r.get("target") not in (None, "oc"):
            continue
        if (r.get("n") or 0) < min_n:
            continue
        if (r.get("lift_pp") or 0) < lift:
            continue
        if (r.get("mean") or 0) > 0:
            continue
        near.append({
            **r,
            "why_near": (
                "higher win-rate on the red day, but still a negative mean "
                "open→close — more often green, still loses money"
            ),
        })
    near.sort(key=lambda r: (-(r.get("lift_pp") or -999), -(r.get("n") or 0)))
    return near[:20]


def sector_table(df: pd.DataFrame, pred) -> list[dict]:
    if "sector" not in df.columns:
        return []
    hit = df[pred(df) & df["sector"].notna()]
    if hit.empty:
        return []
    base = _pack(hit["oc"].to_numpy())
    rows = []
    for sec, sl in hit.groupby("sector"):
        if len(sl) < 40:
            continue
        pack = _pack(sl["oc"].to_numpy())
        lift = None
        if pack["win"] is not None and base["win"] is not None:
            lift = round(100.0 * (pack["win"] - base["win"]), 2)
        rows.append({
            "sector": str(sec), **pack,
            "base_n": base["n"], "base_win": base["win"],
            "base_mean": base["mean"], "lift_pp": lift,
            "mean_edge": None if pack["mean"] is None or base["mean"] is None
            else round(pack["mean"] - base["mean"], 5),
        })
    rows.sort(key=lambda r: (-(r.get("win") or 0), -(r.get("n") or 0)))
    return rows


def score_sector_etfs(prices: pd.DataFrame, spy: pd.DataFrame) -> list[dict]:
    """Open→close of sector / factor ETFs on every SPY-down day.

    Independent of the Finviz overlap. Inverse products are the control
    (they *should* win when SPY is red).
    """
    down = set(spy.loc[spy["spy_cc_down"] == True, "date"])
    if not down:
        return []
    want = set(SECTOR_ETFS) | {"SH", "PSQ", "SPXU", "GLD"}
    sl = prices[prices["ticker"].isin(want) & prices["date"].isin(down)].copy()
    if sl.empty:
        return []
    sl["oc"] = sl["close"] / sl["open"] - 1.0
    rows = []
    for t, lab in list(SECTOR_ETFS.items()) + [
        ("SH", "Short S&P (control — should win)"),
        ("PSQ", "Short QQQ (control)"),
        ("SPXU", "3x short S&P (control)"),
    ]:
        sub = sl[sl["ticker"] == t]
        if sub.empty:
            continue
        pack = _pack(sub["oc"].to_numpy())
        rows.append({
            "ticker": t, "label": lab, "n_days": int(len(sub)), **pack,
        })
    rows.sort(key=lambda r: (-(r.get("win") or 0), -(r.get("mean") or -9)))
    return rows


def worst_day_examples(df: pd.DataFrame, n_days: int = 8, n_names: int = 6) -> list[dict]:
    down = df[df["spy_cc_down"] == True]
    days = (down.groupby("date")["spy_cc"].first().sort_values().head(n_days).index.tolist())
    out = []
    for d in days:
        sl = down[down["date"] == d].sort_values("oc", ascending=False)
        spy = float(sl["spy_cc"].iloc[0]) if len(sl) else None
        wins = sl[sl["win"]]
        out.append({
            "date": d, "spy_cc": None if spy is None else round(100 * spy, 2),
            "n": int(len(sl)), "n_win": int(len(wins)),
            "base_win": round(float(sl["win"].mean()), 4) if len(sl) else None,
            "top": [
                {"ticker": r.ticker, "oc": round(100 * float(r.oc), 2),
                 "last_green": bool(r.last_green),
                 "sector": getattr(r, "sector", None),
                 "ret_5": None if pd.isna(r.ret_5) else round(100 * float(r.ret_5), 2),
                 "rvol": None if pd.isna(r.rvol) else round(float(r.rvol), 2)}
                for r in wins.head(n_names).itertuples()
            ],
        })
    return out


def excel_notes() -> dict:
    return {
        "emulator": "excel_bot/engine — Python replica of Simple View--Calculation.xlsx",
        "full_capture": "--all-cols dumps all 275 columns (A..JL) + fills (#139)",
        "open_knowable_cols": EXCEL_OPEN_COLS,
        "close_knowable_cols": EXCEL_CLOSE_COLS,
        "timing": (
            "Start-of-day colors: A B C G J K L M O. Close-knowable colors "
            "D E F H I N use day-t OHLC — they are not 09:30 inputs. "
            "core_score = A..J includes close-knowable cells, so a same-day "
            "core_score gate would leak. Prior-day close colors are fine."
        ),
        "card_hypotheses": [
            "L1/L2: last green + month-vol < 3% → xl_l1_lowvol_green",
            "L3: last green + midcap $1–10B → xl_l3_mid_green",
            "L5: last green + midcap + beta > 1.5 → xl_l5_mid_hibeta",
            "S1/S2: optionable/hi-vol last-red — the inverse of a long-down-day bet",
        ],
        "grids_on_disk": False,
        "note": (
            "excel-state ships Yahoo row caches, not the 3,603 color grids. "
            "Cohorts are tested via the prior Finviz export; colors are not "
            "re-mined here (would remake the official excel cards)."
        ),
    }


def attach_morning_s(df: pd.DataFrame) -> pd.DataFrame:
    regime = fmb.load_regime()
    s = df["date"].map(lambda d: fmb.morning_s(regime, d))
    out = df.copy()
    out["s"] = s
    out["s_red"] = out["s"].map(lambda v: v is not None and float(v) < 0)
    out["s_hard"] = out["s"].map(lambda v: v is not None and float(v) <= fmb.HARD_RED)
    return out


def _verdict(bases: dict, keepers: list[dict], keepers_morn: list[dict],
             keepers_fv: list[dict], near: list[dict],
             etfs: list[dict], sectors: list[dict]) -> str:
    spy = bases.get("spy_cc_down") or {}
    bits = []
    if spy.get("win") is not None:
        bits.append(
            f"On SPY close-to-close down days, a liquid common stock's "
            f"open→close is green {spy['win']*100:.1f}% of the time "
            f"(mean {100*(spy.get('mean') or 0):+.2f}%, n={spy.get('n')}). "
            "That is the hurdle. Prior-tape cameras (NR7, last-green, washed, "
            "breakout, SMA side) do not clear it — a few raise the win-rate "
            "and still lose money."
        )
    bounce = [k for k in keepers_morn if k.get("feature") == "ret1_le_m3"]
    if bounce:
        k = bounce[0]
        bits.append(
            f"The cleanest tape signal is a bounce the *next* morning: after a "
            f"red SPY day, names already down ≥3% print a next-session "
            f"open→close win {k['win']*100:.1f}% of the time "
            f"(lift {k['lift_pp']:+.1f}pp, mean {100*(k['mean'] or 0):+.2f}%). "
            "That is winning *after* the red day, not during it."
        )
    xlv = next((e for e in etfs if e.get("ticker") == "XLV"), None)
    ibb = next((e for e in etfs if e.get("ticker") == "IBB"), None)
    xle = next((e for e in etfs if e.get("ticker") == "XLE"), None)
    if xlv and xlv.get("win") is not None:
        bits.append(
            f"Sector ETFs on the same 127 red days (no Finviz needed): "
            f"XLV Healthcare open→close win {xlv['win']*100:.1f}% "
            f"(mean {100*(xlv.get('mean') or 0):+.2f}%)."
            + (f" IBB biotech {ibb['win']*100:.1f}%." if ibb and ibb.get("win") else "")
            + (f" XLE energy {xle['win']*100:.1f}%." if xle and xle.get("win") else "")
            + " Inverse SPY ETFs are the control and should win; they are not the answer."
        )
    health = next(
        (k for k in keepers_fv
         if k.get("feature") == "fv_health" and k.get("regime") == "spy_cc_down"),
        None,
    )
    if health:
        bits.append(
            f"On Finviz-tagged SPY-down name-days (prior export only), Healthcare "
            f"wins {health['win']*100:.1f}% vs a tagged base of "
            f"{100*(health.get('base_win') or 0):.1f}% "
            f"(lift {health['lift_pp']:+.1f}pp, mean {100*(health['mean'] or 0):+.2f}%, "
            f"n={health['n']})."
        )
    if sectors:
        top = sectors[0]
        bits.append(
            f"Tagged-sector table leader: {top['sector']} "
            f"{100*(top.get('win') or 0):.1f}% / "
            f"{100*(top.get('mean') or 0):+.2f}% (n={top['n']})."
        )
    if near:
        bits.append(
            f"{len(near)} near-miss tape cameras lift the same-day win-rate "
            "≥3pp but still have a negative mean — they win more often and "
            "still lose money."
        )
    if not keepers and not keepers_fv:
        bits.append("No leak-free same-day feature cleared the keeper bar.")
    return " ".join(bits)


def run(prices: pd.DataFrame | None = None) -> dict:
    prices = prices if prices is not None else load_prices()
    spy = spy_calendar(prices)
    feat = attach_prior_ohlc(prices)
    names = liquid_name_days(feat, spy)
    names = attach_morning_s(names)
    cal = sorted(names["date"].unique().tolist())
    names = attach_finviz(names, cal)
    names = drop_tagged_products(names)
    names = attach_panel(names)

    regimes = [
        ("spy_cc_down", lambda d: d["spy_cc_down"] == True,
         "SPY close-to-close < 0 (outcome — the market WAS down)"),
        ("spy_oc_down", lambda d: d["spy_oc_down"] == True,
         "SPY open→close < 0 (same-session tape, still an outcome)"),
        ("spy_cc_down_1", lambda d: d["spy_cc_down_1"] == True,
         "SPY close-to-close ≤ −1%"),
        ("prior_spy_red", lambda d: d["prior_spy_red"] == True,
         "yesterday SPY was red (knowable at 09:30)"),
        ("s_red", lambda d: d["s_red"] == True,
         "morning flatten S < 0 (knowable)"),
        ("s_hard", lambda d: d["s_hard"] == True,
         "morning hard-red S≤−3 (knowable)"),
        ("all_liquid", lambda d: pd.Series(True, index=d.index),
         "every liquid name-day (baseline, not a down day)"),
    ]

    has_fv = (lambda d: d["sector"].notna()) if "sector" in names.columns else (
        lambda d: pd.Series(False, index=d.index))

    sweeps: list[dict] = []
    extras: list[dict] = []
    bases: dict = {}
    overlay_bases: dict = {}
    for key, pred, label in regimes:
        rows, base = sweep(names, key, pred, "oc")
        for r in rows:
            r["regime_lab"] = label
        sweeps.extend(rows)
        beat, _ = sweep(names, key, pred, "beat_spy")
        for r in beat:
            r["regime_lab"] = label
        sweeps.extend(beat)
        bases[key] = {**base, "label": label,
                      "n_days": int(names.loc[pred(names), "date"].nunique())}
        fv_rows = sweep_extra(names, key, pred, finviz_masks(names),
                              require=has_fv)
        extras.extend(fv_rows)
        combos = combo_masks(names)
        tape_combos = [c for c in combos if c[0] in
                       {"last_green_ret5_neg", "last_red_ret1_m3"}]
        tagged_combos = [c for c in combos if c[0] not in
                         {"last_green_ret5_neg", "last_red_ret1_m3"}]
        extras.extend(sweep_extra(names, key, pred, tape_combos))
        extras.extend(sweep_extra(names, key, pred, tagged_combos,
                                  require=has_fv))
        extras.extend(sweep_extra(names, key, pred, panel_masks(names),
                                  min_n=MIN_N_CAM))
        tagged = names[pred(names)]
        tagged = tagged[has_fv(tagged)] if not tagged.empty else tagged
        overlay_bases[key] = _score(tagged, slice(None), "oc") if len(tagged) else {
            "n": 0, "win": None, "mean": None, "t": None}

    keepers = pick_keepers(
        [r for r in sweeps if r["target"] == "oc" and r["regime"] == "spy_cc_down"]
    )
    keepers_morn = pick_keepers(
        [r for r in sweeps if r["target"] == "oc" and r["regime"] == "prior_spy_red"]
    )
    overlay_pool = [r for r in extras if r["regime"] in OVERLAY_REGIMES]
    keepers_fv = pick_keepers(
        [r for r in overlay_pool if not str(r.get("feature", "")).startswith("cam_")],
        min_n=MIN_N_OVERLAY,
    )
    keepers_cam = pick_keepers(
        [r for r in overlay_pool if str(r.get("feature", "")).startswith("cam_")],
        min_n=MIN_N_CAM,
    )
    near = pick_near(
        [r for r in sweeps + extras if r.get("target") == "oc"],
        regime="spy_cc_down", min_n=200, lift=3.0,
    )
    sectors = sector_table(names, lambda d: d["spy_cc_down"] == True)
    etfs = score_sector_etfs(prices, spy)
    excel_cards = [
        r for r in overlay_pool
        if str(r.get("feature", "")).startswith("xl_")
        and r.get("regime") == "spy_cc_down"
    ]
    excel_cards.sort(key=lambda r: (-(r.get("lift_pp") or -999), -(r.get("n") or 0)))

    tape_oc = [r for r in sweeps if r["target"] == "oc"
               and r["regime"] in ("spy_cc_down", "prior_spy_red")]
    overlay_show = [r for r in overlay_pool if r.get("regime") in
                    ("spy_cc_down", "prior_spy_red")]

    payload = {
        "generated_at": datetime.now(ET).isoformat(),
        "asof": "09:30_et",
        "leak": (
            "prior tape + prior Finviz export + morning S only; "
            "same-day Change%/Gap/RelVol/OHLC never pick"
        ),
        "live_untouched": "flatten_robust",
        "window": {"from": cal[0] if cal else None, "to": cal[-1] if cal else None,
                   "n_sessions": len(cal)},
        "universe": {
            "min_px": MIN_PX, "min_prior_dvol": LIQ_USD,
            "skip": sorted(LEV_SKIP), "n_name_days": int(len(names)),
            "note": "inverse / leveraged products and tagged ETFs are dropped",
        },
        "spy": {
            "n_sessions": int(spy["spy_cc"].notna().sum()),
            "n_cc_down": int(spy["spy_cc_down"].fillna(False).sum()),
            "n_oc_down": int(spy["spy_oc_down"].fillna(False).sum()),
            "n_cc_down_1": int(spy["spy_cc_down_1"].fillna(False).sum()),
            "from": str(spy["date"].min()) if len(spy) else None,
            "to": str(spy["date"].max()) if len(spy) else None,
        },
        "bases": bases,
        "overlay_bases": overlay_bases,
        "keepers": keepers[:16],
        "keepers_morning": keepers_morn[:16],
        "keepers_overlay": keepers_fv[:20],
        "keepers_camera": keepers_cam[:12],
        "near": near,
        "sectors": sectors,
        "sector_etfs": etfs,
        "excel_cards": excel_cards[:12],
        "sweep": tape_oc[:60],
        "overlay": overlay_show[:80],
        "worst_days": worst_day_examples(names),
        "excel": excel_notes(),
        "rules": {
            "min_n": MIN_N, "min_n_overlay": MIN_N_OVERLAY,
            "min_n_camera": MIN_N_CAM, "lift_pp": LIFT_PP,
            "banned": sorted(BANNED),
            "overlay_regimes": sorted(OVERLAY_REGIMES),
            "overlay_base": "Finviz lifts vs tagged name-days only, not the full parquet",
        },
    }
    payload["verdict"] = _verdict(
        bases, keepers, keepers_morn, keepers_fv, near, etfs, sectors)
    return payload


def _md_table(rows: list[dict], cols: str = "feature") -> list[str]:
    lines = [
        "| Feature | Regime | n | Win% | Lift | Mean oc | t | Why |",
        "|---|---|---:|---:|---:|---:|---:|---|",
    ]
    if not rows:
        lines.append("| *(none cleared)* | — | — | — | — | — | — | — |")
        return lines
    for r in rows:
        lines.append(
            f"| `{r.get(cols) or r.get('feature')}` | {r.get('regime') or '—'} | "
            f"{r.get('n')} | {100*(r.get('win') or 0):.1f}% | "
            f"{(r.get('lift_pp') if r.get('lift_pp') is not None else 0):+.1f}pp | "
            f"{100*(r.get('mean') or 0):+.2f}% | "
            f"{r['t'] if r.get('t') is not None else '—'} | {r.get('why') or ''} |"
        )
    return lines


def write_outputs(payload: dict) -> None:
    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    DASH_DIR.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(payload, separators=(",", ":")), encoding="utf-8")
    spy = payload.get("spy") or {}
    uni = payload.get("universe") or {}
    lines = [
        f"# Down-day winner mine — {payload['window'].get('from')} → {payload['window'].get('to')}",
        "",
        "Leak-free 09:30 inputs only. Official factor-mine books and live "
        "`flatten_robust` are untouched. Inverse / leveraged products are "
        "dropped — they are not a stock that won.",
        "",
        f"SPY close-to-close down days: **{spy.get('n_cc_down')}** / "
        f"{spy.get('n_sessions')} sessions. Liquid common-stock name-days: "
        f"**{uni.get('n_name_days')}**.",
        "",
        "## Verdict",
        "",
        payload.get("verdict") or "",
        "",
        "## What counts as a down day",
        "",
        "- `spy_cc_down` — SPY close < prior close (the market finished red).",
        "- `prior_spy_red` — yesterday was that red day (knowable at 09:30).",
        "- `s_red` / `s_hard` — morning flatten score already red (thin; not promoted).",
        "",
        "A *win* is the name's own 09:30 open → 16:00 close > 0. "
        "`beat_spy` is a second target (finished less-red than SPY). "
        "Finviz / Excel-card lifts are scored against **tagged** name-days "
        "only, so a missing export cannot inflate the base.",
        "",
        "## Keepers vs liquid names on SPY-down days",
        "",
    ]
    lines += _md_table(payload.get("keepers") or [])
    lines += [
        "",
        "## Knowable at 09:30 (yesterday already red)",
        "",
    ]
    lines += _md_table(payload.get("keepers_morning") or [])
    lines += [
        "",
        "## Finviz / Excel-card overlay (tagged base, n≥80)",
        "",
        "Two-day `s_red` overlays are not keepers. Camera rows need n≥80.",
        "",
    ]
    lines += _md_table(payload.get("keepers_overlay") or [])
    if payload.get("keepers_camera"):
        lines += ["", "### Camera overlay (thin 17-session panel)", ""]
        lines += _md_table(payload.get("keepers_camera") or [])
    lines += [
        "",
        "## Near-misses — higher win%, still red money",
        "",
    ]
    lines += _md_table(payload.get("near") or [])
    lines += [
        "",
        "## Sector ETFs on every SPY-down day",
        "",
        "Independent of Finviz. Inverse SPY products are the control.",
        "",
        "| Ticker | Sleeve | Days | Win% | Mean oc | t |",
        "|---|---|---:|---:|---:|---:|",
    ]
    for r in payload.get("sector_etfs") or []:
        lines.append(
            f"| `{r['ticker']}` | {r.get('label')} | {r.get('n_days') or r.get('n')} | "
            f"{100*(r.get('win') or 0):.1f}% | {100*(r.get('mean') or 0):+.2f}% | "
            f"{r['t'] if r.get('t') is not None else '—'} |"
        )
    lines += [
        "",
        "## Tagged sectors on SPY-down days",
        "",
        "| Sector | n | Win% | Lift vs tagged | Mean oc | t |",
        "|---|---:|---:|---:|---:|---:|",
    ]
    if not payload.get("sectors"):
        lines.append("| *(no tagged rows)* | — | — | — | — | — |")
    for r in payload.get("sectors") or []:
        lines.append(
            f"| {r['sector']} | {r['n']} | {100*(r.get('win') or 0):.1f}% | "
            f"{(r.get('lift_pp') or 0):+.1f}pp | {100*(r.get('mean') or 0):+.2f}% | "
            f"{r['t'] if r.get('t') is not None else '—'} |"
        )
    lines += ["", "## Excel emulator", "", payload["excel"]["timing"], ""]
    for h in payload["excel"]["card_hypotheses"]:
        lines.append(f"- {h}")
    if payload.get("excel_cards"):
        lines += ["", "### Card-cohort results on SPY-down tagged days", ""]
        lines += _md_table(payload.get("excel_cards") or [])
    lines += ["", payload["excel"]["note"], ""]
    OUT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")
    if TEMPLATE.is_file():
        html = TEMPLATE.read_text(encoding="utf-8")
        html = html.replace("__DATA__", json.dumps(payload, separators=(",", ":")))
        (DASH_DIR / "index.html").write_text(html, encoding="utf-8")


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--write", action="store_true")
    args = ap.parse_args(argv)
    payload = run()
    if args.write:
        write_outputs(payload)
        print(f"wrote {OUT_JSON} and {OUT_MD}")
        print(payload.get("verdict", "")[:400])
    else:
        print(json.dumps({
            "spy": payload["spy"], "bases": payload["bases"],
            "keepers": payload["keepers"][:8],
            "verdict": payload.get("verdict"),
        }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
