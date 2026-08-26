"""Closed-loop weight learner for the stock book.

The one metric that matters is the paper dashboard, which compounds the
forward returns of the daily buy books. This module closes the loop that
the backtest only reported on:

  realized forward returns  →  walk-forward weight evaluation
     →  00_grounding/book_policy.json  →  tomorrow's stock_book run

Method (deliberately conservative — it can only nudge, never break):
  1. Rebuild each historical signal frame (per-ticker component scores)
     from data/stock_book/{date}_stock_book.csv; reconstruct any component
     columns that predate the full snapshot from that date's committed
     inputs (AB enriched, peer RS, membership, Finviz export).
  2. Compute fully-realized forward returns per horizon from the local
     price store (data/prices/ohlc.parquet) — no lookahead: a date only
     enters the evaluation once its exit session has traded.
  3. For each horizon, re-score the frame under the incumbent weights and
     a neighborhood of candidates (weight transfers of ±0.03 between
     signal families, bounded to ±0.12 of the code defaults), select the
     top-10 buy book under the same gates the ranker uses, and measure
     mean forward return in excess of the liquid-universe median.
  4. Adopt a candidate only if there are ≥ MIN_DATES evaluation dates,
     it beats the incumbent by ≥ EPS_IMPROVE on mean excess return AND on
     ≥ WIN_FRAC of individual dates. Even then, move only HALF_STEP of
     the way. Dates whose input health was not learn-grade are excluded.
  5. The sell-book construction flag (core score vs full score) is
     evaluated the same way on the sell side.

Everything is written to 00_grounding/book_policy.json (with bounded
validation on the consumer side) and a human ledger in
03_scoreboard/BOOK_LEARN.md.

CLI: python -m src.book_learn [--date YYYY-MM-DD] [--lookback 40]
     [--top 10] [--update-prices]
"""
from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

from . import config, input_health
from .stock_book import (
    HORIZONS,
    MAX_LARGE_MEGA,
    MAX_PER_INDUSTRY,
    MAX_PER_SECTOR,
    MAX_POLICY_DRIFT,
    MAX_POLICY_WEIGHT,
    MAX_OPP_MCAP_M,
    MIN_AVG_VOL_K,
    MIN_MARKET_CAP_M,
    MIN_OPP_MCAP_M,
    PERSIST_PENALTY,
    RANGE_OPP,
    REBOUND_BOOST,
    SIGNAL_FAMILIES,
    SIZE_OPP,
    WEIGHTS,
    _load_ab_enriched,
    _load_finviz_liquidity,
    _load_peer_rs,
    _prev_book_buys,
    load_policy,
)

ROOT = Path(__file__).resolve().parent.parent
BOOK_DIR = ROOT / "data" / "stock_book"
POLICY_PATH = ROOT / "00_grounding" / "book_policy.json"
LEDGER_MD = ROOT / "03_scoreboard" / "BOOK_LEARN.md"
PRICES = ROOT / "data" / "prices" / "ohlc.parquet"

HORIZON_DAYS = {"1d": 1, "3d": 3, "1w": 5, "2w": 10, "1m": 21}

MIN_DATES = 5          # never move weights on thinner evidence
EPS_IMPROVE = 0.0005   # ≥5bps mean excess-return improvement required
WIN_FRAC = 0.6         # candidate must beat incumbent on ≥60% of dates
HALF_STEP = 0.5        # adopt only half the distance to the winner
TRANSFER = 0.03        # unit of weight moved between two families
SELL_MIN_DATES = 5
SELL_TOP = 25

COMPONENTS = ("s_join", "s_sector", "s_general", "s_news", "s_ab", "s_peer")


# ---------------------------------------------------------------- frames

def _reconstruct_opp(frame: pd.DataFrame, date: str) -> pd.DataFrame:
    """Rebuild s_opp / size / industry from the dated membership file."""
    memb_p = ROOT / "data" / "universe" / f"{date}_membership.csv"
    if not memb_p.exists():
        return frame
    memb = pd.read_csv(memb_p, low_memory=False)
    if "Ticker" not in memb.columns:
        return frame
    memb["Ticker"] = memb["Ticker"].astype(str).str.strip().str.upper()
    memb = memb.drop_duplicates("Ticker", keep="first")
    cols = [c for c in ("size", "industry", "range", "ext", "earnsurp") if c in memb.columns]
    frame = frame.merge(memb[["Ticker"] + cols], on="Ticker", how="left",
                        suffixes=("", "_memb"))
    for c in cols:
        if f"{c}_memb" in frame.columns:
            frame[c] = frame[c].fillna(frame[f"{c}_memb"])
            frame = frame.drop(columns=[f"{c}_memb"])
    if "s_opp" not in frame.columns:
        sz = frame.get("size", pd.Series("", index=frame.index)).astype(str).str.lower()
        rng = frame.get("range", pd.Series("", index=frame.index)).astype(str).str.lower()
        ext = frame.get("ext", pd.Series("", index=frame.index)).astype(str).str.lower()
        surp = frame.get("earnsurp", pd.Series("", index=frame.index)).astype(str).str.lower()
        frame["s_opp"] = sz.map(SIZE_OPP).fillna(0.0) + rng.map(RANGE_OPP).fillna(0.0)
        midish = sz.isin(["small", "mid"])
        frame.loc[midish & ext.isin(["washed", "neutral", ""]), "s_opp"] += 0.08
        frame.loc[midish & surp.isin(["beat", "big_beat"]), "s_opp"] += 0.12
    return frame


def load_frame(date: str) -> pd.DataFrame | None:
    """Per-ticker component frame for one signal date (reconstructing
    columns that predate the full snapshot format)."""
    p = BOOK_DIR / f"{date}_stock_book.csv"
    if not p.exists():
        return None
    df = pd.read_csv(p, low_memory=False)
    if "Ticker" not in df.columns or "s_join" not in df.columns:
        return None
    df["Ticker"] = df["Ticker"].astype(str).str.strip().str.upper()
    df = df.drop_duplicates("Ticker", keep="first")

    if "s_ab" not in df.columns:
        ab = _load_ab_enriched(date)
        if len(ab):
            df = df.merge(ab[["Ticker", "ab_raw"]], on="Ticker", how="left")
            df["s_ab"] = np.tanh(pd.to_numeric(df["ab_raw"], errors="coerce").fillna(0.0) / 8.0)
        else:
            df["s_ab"] = 0.0
    if "s_peer" not in df.columns:
        peer = _load_peer_rs(date)
        if len(peer) and "rs_week" in peer.columns:
            df = df.merge(peer[["Ticker", "rs_week"]], on="Ticker", how="left")
            df["s_peer"] = np.tanh(pd.to_numeric(df["rs_week"], errors="coerce").fillna(0.0) / 8.0)
        else:
            df["s_peer"] = 0.0
    if "market_cap_m" not in df.columns or "avg_vol_k" not in df.columns:
        liq = _load_finviz_liquidity(date)
        if len(liq):
            df = df.merge(liq, on="Ticker", how="left")
        else:
            df["market_cap_m"] = np.nan
            df["avg_vol_k"] = np.nan
    df = _reconstruct_opp(df, date)
    if "s_opp" not in df.columns:
        df["s_opp"] = 0.0
    if "rebound" not in df.columns:
        df["rebound"] = False
    if "s_heat_raw" not in df.columns:
        # Old snapshots either have already-scaled s_heat or no heat at all.
        df["s_heat_raw"] = df.get("s_heat", 0.0)
    for c in COMPONENTS:
        if c not in df.columns:
            df[c] = 0.0
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)
    df["s_opp"] = pd.to_numeric(df["s_opp"], errors="coerce").fillna(0.0)
    df["s_heat_raw"] = pd.to_numeric(
        df["s_heat_raw"], errors="coerce").fillna(0.0)
    df["rebound"] = df["rebound"].astype(str).str.lower().isin(["true", "1"])

    # enforce the same liquidity gate as the live ranker (old frames were
    # written before the gate)
    mcap = pd.to_numeric(df["market_cap_m"], errors="coerce").fillna(0)
    adv = pd.to_numeric(df["avg_vol_k"], errors="coerce").fillna(0)
    df = df.loc[(mcap >= MIN_MARKET_CAP_M) & (adv >= MIN_AVG_VOL_K)].copy()
    return df if len(df) else None


def _components_for(df: pd.DataFrame, h: str) -> np.ndarray:
    """(n, 6) matrix; per-horizon sector/general columns when present."""
    sec_col = f"s_sector_{h}" if f"s_sector_{h}" in df.columns else "s_sector"
    gen_col = f"s_general_{h}" if f"s_general_{h}" in df.columns else "s_general"
    cols = ["s_join", sec_col, gen_col, "s_news", "s_ab", "s_peer"]
    return df[cols].to_numpy(dtype=float)


# ---------------------------------------------------------------- prices

def _load_panel() -> pd.DataFrame | None:
    if not PRICES.exists():
        return None
    df = pd.read_parquet(PRICES)
    df["date"] = pd.to_datetime(df["date"]).dt.normalize()
    df["ticker"] = df["ticker"].astype(str).str.upper()
    df = df.drop_duplicates(subset=["date", "ticker"], keep="last")
    return df.pivot(index="date", columns="ticker", values="close").sort_index()


def _fwd_returns(panel: pd.DataFrame, date: str, n_td: int) -> pd.Series | None:
    """Vector of realized forward returns for every ticker in the panel.
    None when the exit session has not traded yet (no lookahead)."""
    ts = pd.Timestamp(date)
    idx = panel.index.searchsorted(ts)
    if idx >= len(panel.index):
        return None
    # entry must be within 4 sessions of the signal (guards huge gaps)
    if (panel.index[idx] - ts).days > 6:
        return None
    exit_idx = idx + n_td
    if exit_idx >= len(panel.index):
        return None
    entry = panel.iloc[idx]
    exitp = panel.iloc[exit_idx]
    ret = exitp / entry - 1.0
    return ret.replace([np.inf, -np.inf], np.nan).dropna()


# ---------------------------------------------------------------- selection

def _select_buys(df: pd.DataFrame, score: np.ndarray, top_n: int) -> list[int]:
    """Replicates the live _book_side buy gates, on positional indexes."""
    order = np.argsort(-score)
    sizes = df["size"].astype(str).str.lower().to_numpy() if "size" in df.columns \
        else np.array([""] * len(df))
    sectors = df["sector"].astype(str).to_numpy() if "sector" in df.columns \
        else np.array([""] * len(df))
    inds = df["industry"].astype(str).to_numpy() if "industry" in df.columns \
        else np.array([""] * len(df))
    mcaps = pd.to_numeric(df["market_cap_m"], errors="coerce").fillna(0).to_numpy()
    picks: list[int] = []
    sec_n: dict[str, int] = {}
    ind_n: dict[str, int] = {}
    large_n = 0
    for i in order:
        size, mcap = sizes[i], float(mcaps[i])
        if size == "micro" or mcap < MIN_OPP_MCAP_M:
            continue
        is_large = size in ("large", "mega") or mcap > MAX_OPP_MCAP_M
        if is_large and large_n >= MAX_LARGE_MEGA:
            continue
        sec, ind = sectors[i], inds[i]
        if sec and sec_n.get(sec, 0) >= MAX_PER_SECTOR:
            continue
        if ind and ind not in ("", "nan", "None") and ind_n.get(ind, 0) >= MAX_PER_INDUSTRY:
            continue
        picks.append(int(i))
        if is_large:
            large_n += 1
        if sec:
            sec_n[sec] = sec_n.get(sec, 0) + 1
        if ind and ind not in ("", "nan", "None"):
            ind_n[ind] = ind_n.get(ind, 0) + 1
        if len(picks) >= top_n:
            break
    return picks


def _score(df: pd.DataFrame, comp: np.ndarray, w: tuple[float, ...],
           prev_held: set[str], heat_scale: float = 1.0
           ) -> tuple[np.ndarray, np.ndarray]:
    """Returns (full score with add-ons, core score) under weights w."""
    heat = df.get("s_heat_raw", pd.Series(0.0, index=df.index)).to_numpy(dtype=float)
    core = comp @ np.asarray(w, dtype=float) + heat_scale * heat
    full = core + df["s_opp"].to_numpy(dtype=float)
    full = full + np.where(df["rebound"].to_numpy(dtype=bool), REBOUND_BOOST, 0.0)
    if prev_held:
        fresh = (
            (df["s_news"].abs() > 0.15)
            | (df["s_ab"] > 0.20)
            | (df["s_peer"] > 0.20)
        ).to_numpy(dtype=bool)
        held = df["Ticker"].isin(prev_held).to_numpy(dtype=bool)
        full = full - np.where(held & ~fresh, PERSIST_PENALTY, 0.0)
    return full, core


# ---------------------------------------------------------------- candidates

def _bounded(w: tuple[float, ...], h: str) -> bool:
    base = WEIGHTS[h]
    return all(
        0.0 <= x <= MAX_POLICY_WEIGHT and abs(x - b) <= MAX_POLICY_DRIFT + 1e-9
        for x, b in zip(w, base)
    ) and 0.85 * sum(base) <= sum(w) <= 1.15 * sum(base)


def _candidates(incumbent: tuple[float, ...], h: str) -> list[tuple[float, ...]]:
    cands: list[tuple[float, ...]] = []
    seen = {tuple(round(x, 4) for x in incumbent)}
    default = WEIGHTS[h]
    if tuple(default) not in seen and _bounded(default, h):
        cands.append(tuple(default))
        seen.add(tuple(round(x, 4) for x in default))
    n = len(incumbent)
    for i in range(n):
        for j in range(n):
            if i == j or incumbent[i] < TRANSFER:
                continue
            w = list(incumbent)
            w[i] = round(w[i] - TRANSFER, 4)
            w[j] = round(w[j] + TRANSFER, 4)
            wt = tuple(w)
            key = tuple(round(x, 4) for x in wt)
            if key in seen or not _bounded(wt, h):
                continue
            cands.append(wt)
            seen.add(key)
    return cands


# ---------------------------------------------------------------- evaluation

def _eval_dates(lookback: int, asof: str) -> list[str]:
    dates = sorted({p.name[:10] for p in BOOK_DIR.glob("????-??-??_stock_book.csv")})
    dates = [d for d in dates if d <= asof][-lookback:]
    out = []
    for d in dates:
        health = input_health.load(d)
        if health is not None and not health.get("learn_grade", True):
            print(f"[book-learn] {d}: excluded (input health not learn-grade)")
            continue
        out.append(d)
    return out


def _evaluate_horizon(
    h: str,
    frames: dict[str, pd.DataFrame],
    panel: pd.DataFrame,
    incumbent: tuple[float, ...],
    top_n: int,
    heat_scale: float = 1.0,
) -> dict:
    """Walk the candidate grid on all realized dates for one horizon."""
    n_td = HORIZON_DAYS[h]
    per_date: list[dict] = []
    for d, df in frames.items():
        rets = _fwd_returns(panel, d, n_td)
        if rets is None or len(rets) < 200:
            continue
        bench = float(rets.reindex(df["Ticker"]).median())
        prev_held = _prev_book_buys(d).get(h, set())
        comp = _components_for(df, h)
        per_date.append({
            "date": d, "df": df, "comp": comp, "rets": rets,
            "bench": bench, "prev_held": prev_held,
        })
    result = {"horizon": h, "n_dates": len(per_date),
              "dates": [x["date"] for x in per_date]}
    if len(per_date) < MIN_DATES:
        result["decision"] = f"observe — only {len(per_date)} realized dates (< {MIN_DATES})"
        result["adopted"] = list(incumbent)
        return result

    def objective(w: tuple[float, ...]) -> tuple[float, list[float]]:
        vals = []
        for x in per_date:
            full, _ = _score(
                x["df"], x["comp"], w, x["prev_held"], heat_scale=heat_scale)
            picks = _select_buys(x["df"], full, top_n)
            if not picks:
                vals.append(0.0)
                continue
            tickers = x["df"]["Ticker"].iloc[picks]
            fr = x["rets"].reindex(tickers).dropna()
            vals.append(float(fr.mean() - x["bench"]) if len(fr) else 0.0)
        return (float(np.mean(vals)) if vals else 0.0), vals

    inc_mean, inc_vals = objective(incumbent)
    best_w, best_mean, best_vals = incumbent, inc_mean, inc_vals
    for cand in _candidates(incumbent, h):
        m, vals = objective(cand)
        if m > best_mean:
            best_w, best_mean, best_vals = cand, m, vals

    result["incumbent"] = list(incumbent)
    result["incumbent_excess"] = round(inc_mean * 100, 4)
    result["best_candidate"] = list(best_w)
    result["best_excess"] = round(best_mean * 100, 4)
    wins = sum(1 for a, b in zip(best_vals, inc_vals) if a > b)
    win_frac = wins / len(inc_vals) if inc_vals else 0.0
    result["win_frac_vs_incumbent"] = round(win_frac, 3)

    if best_w == incumbent:
        result["decision"] = "hold — incumbent is the local optimum"
        result["adopted"] = list(incumbent)
    elif best_mean - inc_mean < EPS_IMPROVE:
        result["decision"] = (
            f"hold — improvement {100*(best_mean-inc_mean):.3f}pp < {100*EPS_IMPROVE:.2f}pp"
        )
        result["adopted"] = list(incumbent)
    elif win_frac < WIN_FRAC:
        result["decision"] = f"hold — wins only {win_frac:.0%} of dates (< {WIN_FRAC:.0%})"
        result["adopted"] = list(incumbent)
    else:
        stepped = tuple(
            round(a + HALF_STEP * (b - a), 4) for a, b in zip(incumbent, best_w)
        )
        base = WEIGHTS[h]
        stepped = tuple(
            round(min(max(x, max(0.0, b - MAX_POLICY_DRIFT)), b + MAX_POLICY_DRIFT), 4)
            for x, b in zip(stepped, base)
        )
        result["decision"] = (
            f"MOVE — half-step toward candidate "
            f"(+{100*(best_mean-inc_mean):.3f}pp mean excess, wins {win_frac:.0%})"
        )
        result["adopted"] = list(stepped)
    return result


HEAT_SCALES = (0.0, 0.25, 0.5, 0.75, 1.0, 1.25)
HEAT_INCUBATE = 0.25
HEAT_WARM = 0.50
HEAT_WARM_DATES = 15


def _evaluate_heat_scale(
    frames: dict[str, pd.DataFrame], panel: pd.DataFrame,
    weights: dict[str, tuple[float, ...]], current: float, top_n: int,
) -> dict:
    """Learn whether map/captain heat adds realized 1d excess return."""
    h, n_td = "1d", HORIZON_DAYS["1d"]
    rows = []
    for d, df in frames.items():
        if not df.get("s_heat_raw", pd.Series(0.0, index=df.index)).abs().gt(0).any():
            continue
        rets = _fwd_returns(panel, d, n_td)
        if rets is None or len(rets) < 200:
            continue
        rows.append({
            "date": d, "df": df, "rets": rets,
            "bench": float(rets.reindex(df["Ticker"]).median()),
            "comp": _components_for(df, h),
            "held": _prev_book_buys(d).get(h, set()),
        })
    out = {"n_dates": len(rows), "current": current}
    if len(rows) < MIN_DATES:
        out["adopted"] = HEAT_INCUBATE
        out["decision"] = (
            f"incubate {HEAT_INCUBATE:.2f} — only {len(rows)} realized "
            f"heat dates (< {MIN_DATES})"
        )
        return out
    cap = HEAT_WARM if len(rows) < HEAT_WARM_DATES else 1.5

    def objective(scale: float) -> tuple[float, list[float]]:
        vals = []
        for x in rows:
            full, _ = _score(
                x["df"], x["comp"], weights[h], x["held"], heat_scale=scale)
            picks = _select_buys(x["df"], full, top_n)
            fr = x["rets"].reindex(x["df"]["Ticker"].iloc[picks]).dropna()
            vals.append(float(fr.mean() - x["bench"]) if len(fr) else 0.0)
        return float(np.mean(vals)), vals

    cur_mean, cur_vals = objective(current)
    candidates = sorted(set(HEAT_SCALES + (round(current, 2),)))
    scored = [(s, *objective(s)) for s in candidates]
    best, best_mean, best_vals = max(scored, key=lambda x: x[1])
    wins = sum(1 for a, b in zip(best_vals, cur_vals) if a > b)
    win_frac = wins / len(cur_vals)
    out.update({
        "current_excess": round(cur_mean * 100, 4),
        "best_candidate": best,
        "best_excess": round(best_mean * 100, 4),
        "win_frac": round(win_frac, 3),
    })
    if (best == current or best_mean - cur_mean < EPS_IMPROVE
            or win_frac < WIN_FRAC):
        adopted = current
        decision = (f"hold {current:.2f} — best {best:.2f}, "
                    f"improvement {(best_mean-cur_mean)*100:.3f}pp, "
                    f"wins {win_frac:.0%}")
    else:
        adopted = round(current + HALF_STEP * (best - current), 3)
        decision = (f"MOVE {current:.2f}→{adopted:.3f} toward {best:.2f}; "
                    f"+{(best_mean-cur_mean)*100:.3f}pp, wins {win_frac:.0%}")
    if adopted > cap:
        decision = f"{decision}; cap {cap:.2f} until {HEAT_WARM_DATES} heat dates"
        adopted = cap
    out["adopted"] = adopted
    out["decision"] = decision
    return out


def _book_risk(date: str) -> str:
    """weather_risk recorded in that date's book meta ('' if unknown)."""
    p = BOOK_DIR / f"{date}_stock_book.json"
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
        return str((data.get("meta") or {}).get("weather_risk") or "")
    except (OSError, json.JSONDecodeError):
        return ""


RISK_MIN_DATES = 4
RISK_DEFAULT_SCALE = 0.5
RISK_EFFECTIVE_DEFAULT = "2026-08-25"


def _evaluate_risk_scale(
    frames: dict[str, pd.DataFrame], panel: pd.DataFrame,
    weights: dict[str, tuple[float, ...]], prev: dict, top_n: int,
) -> dict:
    """Should new entries be scaled down on risk-off days?

    Compares the book's realized next-day return on risk-off days against
    cash (0). If the book loses money on risk-off days, holding back cash
    is right → keep the reduced scale; if it reliably makes money even
    then, the scale costs returns → set back to 1.0.
    """
    cur_scale = float(prev.get("risk_off_entry_scale", RISK_DEFAULT_SCALE))
    effective = str(prev.get("risk_scaling_effective") or RISK_EFFECTIVE_DEFAULT)
    h, n_td = "1d", HORIZON_DAYS["1d"]
    vals = []
    for d, df in frames.items():
        if _book_risk(d) != "off":
            continue
        rets = _fwd_returns(panel, d, n_td)
        if rets is None or len(rets) < 200:
            continue
        comp = _components_for(df, h)
        full, _ = _score(
            df, comp, weights[h], _prev_book_buys(d).get(h, set()),
            heat_scale=float(prev.get("heat_scale", 1.0)))
        picks = _select_buys(df, full, top_n)
        if not picks:
            continue
        fr = rets.reindex(df["Ticker"].iloc[picks]).dropna()
        if len(fr):
            vals.append(float(fr.mean()))
    out = {"n_risk_off_dates": len(vals), "current_scale": cur_scale,
           "effective": effective}
    if len(vals) < RISK_MIN_DATES:
        out["adopted_scale"] = cur_scale
        out["decision"] = (f"hold scale {cur_scale} — only {len(vals)} realized "
                           f"risk-off dates (< {RISK_MIN_DATES})")
        return out
    mean_abs = float(np.mean(vals))
    out["mean_abs_return_pct"] = round(mean_abs * 100, 3)
    if mean_abs < -EPS_IMPROVE:
        out["adopted_scale"] = RISK_DEFAULT_SCALE
        out["decision"] = (f"book loses {100*mean_abs:.2f}% on risk-off days → "
                           f"keep entry scale {RISK_DEFAULT_SCALE}")
    elif mean_abs > EPS_IMPROVE:
        out["adopted_scale"] = 1.0
        out["decision"] = (f"book still makes {100*mean_abs:+.2f}% on risk-off "
                           f"days → scale back to 1.0 (cash drag not justified)")
    else:
        out["adopted_scale"] = cur_scale
        out["decision"] = f"inconclusive ({100*mean_abs:+.2f}%) — hold {cur_scale}"
    return out


def _evaluate_sell_flag(
    frames: dict[str, pd.DataFrame], panel: pd.DataFrame,
    weights: dict[str, tuple[float, ...]], current: bool,
    heat_scale: float = 1.0,
) -> dict:
    """Does ranking the sell book on core score (no buy-side add-ons)
    actually produce better shorts? Evaluated on 1w (the mid horizon)."""
    h, n_td = "1w", HORIZON_DAYS["1w"]
    core_v, full_v = [], []
    for d, df in frames.items():
        rets = _fwd_returns(panel, d, n_td)
        if rets is None or len(rets) < 200:
            continue
        bench = float(rets.reindex(df["Ticker"]).median())
        comp = _components_for(df, h)
        full, core = _score(
            df, comp, weights[h], set(), heat_scale=heat_scale)

        def sell_pnl(score: np.ndarray) -> float | None:
            order = np.argsort(score)[:SELL_TOP]
            tickers = df["Ticker"].iloc[order]
            fr = rets.reindex(tickers).dropna()
            return float(-(fr.mean() - bench)) if len(fr) else None

        c, f = sell_pnl(core), sell_pnl(full)
        if c is not None and f is not None:
            core_v.append(c)
            full_v.append(f)
    out = {"n_dates": len(core_v), "current": current}
    if len(core_v) < SELL_MIN_DATES:
        out["decision"] = f"hold {current} — only {len(core_v)} dates"
        out["adopted"] = current
        return out
    core_m, full_m = float(np.mean(core_v)), float(np.mean(full_v))
    out["core_excess"] = round(core_m * 100, 4)
    out["full_excess"] = round(full_m * 100, 4)
    wins_core = sum(1 for a, b in zip(core_v, full_v) if a > b) / len(core_v)
    better = core_m > full_m + EPS_IMPROVE and wins_core >= WIN_FRAC
    worse = full_m > core_m + EPS_IMPROVE and (1 - wins_core) >= WIN_FRAC
    if better:
        out["adopted"] = True
    elif worse:
        out["adopted"] = False
    else:
        out["adopted"] = current
    out["decision"] = f"core={core_m*100:.3f}pp full={full_m*100:.3f}pp → sell_excludes_addons={out['adopted']}"
    return out


# ---------------------------------------------------------------- persistence

def _write_policy(adopted: dict[str, list[float]], sell_flag: bool,
                  results: list[dict], sell_result: dict, asof: str,
                  risk_result: dict | None = None,
                  heat_result: dict | None = None) -> dict:
    prev = {}
    if POLICY_PATH.exists():
        try:
            prev = json.loads(POLICY_PATH.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            prev = {}
    version = int(prev.get("version") or 0) + 1
    history = list(prev.get("history") or [])[-30:]
    history.append({
        "version": version,
        "asof": asof,
        "decisions": {r["horizon"]: r["decision"] for r in results},
        "sell": sell_result.get("decision"),
        "risk": (risk_result or {}).get("decision"),
        "heat": (heat_result or {}).get("decision"),
    })
    risk_result = risk_result or {}
    policy = {
        "version": version,
        "updated": datetime.now(ZoneInfo(config.TZ)).isoformat(),
        "asof": asof,
        "weights": adopted,
        "sell_excludes_addons": sell_flag,
        "heat_scale": (heat_result or {}).get(
            "adopted", prev.get("heat_scale", 1.0)),
        "risk_off_entry_scale": risk_result.get(
            "adopted_scale", prev.get("risk_off_entry_scale", RISK_DEFAULT_SCALE)),
        "risk_scaling_effective": prev.get(
            "risk_scaling_effective", RISK_EFFECTIVE_DEFAULT),
        "objective": "mean top-10 buy fwd return in excess of liquid-universe median",
        "guardrails": {
            "min_dates": MIN_DATES, "eps_improve": EPS_IMPROVE,
            "win_frac": WIN_FRAC, "half_step": HALF_STEP,
            "max_drift_vs_defaults": MAX_POLICY_DRIFT,
        },
        "history": history,
    }
    POLICY_PATH.write_text(json.dumps(policy, indent=2), encoding="utf-8")
    return policy


def _write_ledger(policy: dict, results: list[dict], sell_result: dict,
                  risk_result: dict | None = None,
                  heat_result: dict | None = None) -> None:
    L = [
        f"# Book learn — weight tuner ledger (v{policy['version']})",
        "",
        f"Updated: **{policy['updated']}** · evaluation as of **{policy['asof']}**",
        "",
        "Objective: mean forward return of the top-10 buy book **in excess of the",
        "liquid-universe median**, walk-forward on fully-realized dates only.",
        f"Guardrails: ≥{MIN_DATES} dates, ≥{100*EPS_IMPROVE:.2f}pp improvement, "
        f"wins on ≥{WIN_FRAC:.0%} of dates, half-step adoption, "
        f"±{MAX_POLICY_DRIFT} drift cap vs code defaults.",
        "",
        "| Horizon | dates | incumbent excess | best excess | decision |",
        "|---------|-------|------------------|-------------|----------|",
    ]
    for r in results:
        L.append(
            f"| {r['horizon']} | {r['n_dates']} | "
            f"{r.get('incumbent_excess', '—')} | {r.get('best_excess', '—')} | "
            f"{r['decision']} |"
        )
    L += [
        "",
        "## Adopted weights (join / sector / general / news / AB / peer)",
        "",
        "| Horizon | adopted | code default |",
        "|---------|---------|--------------|",
    ]
    for r in results:
        h = r["horizon"]
        L.append(f"| {h} | {r['adopted']} | {list(WEIGHTS[h])} |")
    L += [
        "",
        "## Sell-book construction",
        "",
        f"- {sell_result.get('decision', 'n/a')} (n={sell_result.get('n_dates', 0)})",
        "",
        "## Risk-off entry scaling (LLM weather call → sizing action)",
        "",
        f"- scale: **{policy.get('risk_off_entry_scale')}** "
        f"(effective {policy.get('risk_scaling_effective')}) — "
        f"{(risk_result or {}).get('decision', 'not evaluated')}",
        "",
        "## Map/captain heat scale (realized 1d excess return)",
        "",
        f"- scale: **{policy.get('heat_scale', 1.0)}** — "
        f"{(heat_result or {}).get('decision', 'not evaluated')}",
        "",
        "## History",
        "",
    ]
    for hrec in (policy.get("history") or [])[-10:][::-1]:
        L.append(f"- v{hrec['version']} @ {hrec['asof']}: "
                 + "; ".join(f"{k}: {v}" for k, v in (hrec.get('decisions') or {}).items()))
    LEDGER_MD.parent.mkdir(parents=True, exist_ok=True)
    LEDGER_MD.write_text("\n".join(L), encoding="utf-8")
    print(f"[book-learn] wrote {LEDGER_MD}")


# ---------------------------------------------------------------- main

def run(date: str | None = None, lookback: int = 40, top_n: int = 10,
        update_prices: bool = False) -> None:
    asof = date or datetime.now(ZoneInfo(config.TZ)).date().isoformat()

    if update_prices:
        try:
            from . import price_store
            price_store.update()
        except BaseException as e:  # noqa: BLE001 — price refresh is best-effort
            print(f"[book-learn] price update skipped: {e}")

    panel = _load_panel()
    if panel is None or panel.empty:
        print("[book-learn] no price store — cannot learn (run price_store bootstrap)")
        return
    print(f"[book-learn] price panel {panel.index.min().date()} → "
          f"{panel.index.max().date()} ({panel.shape[1]} tickers)")

    dates = _eval_dates(lookback, asof)
    frames: dict[str, pd.DataFrame] = {}
    for d in dates:
        f = load_frame(d)
        if f is not None and len(f) >= 200:
            frames[d] = f
    print(f"[book-learn] {len(frames)} signal frames loaded: {sorted(frames)}")
    if not frames:
        print("[book-learn] nothing to learn from yet")
        return

    incumbent_w, pol_meta = load_policy()
    current_sell = bool(pol_meta.get("sell_excludes_addons", True))
    current_heat = float(pol_meta.get("heat_scale", 1.0))

    results = []
    adopted: dict[str, list[float]] = {}
    for h in HORIZONS:
        r = _evaluate_horizon(
            h, frames, panel, tuple(incumbent_w[h]), top_n,
            heat_scale=current_heat)
        print(f"[book-learn] {h}: {r['decision']}")
        results.append(r)
        adopted[h] = r["adopted"]

    sell_result = _evaluate_sell_flag(
        frames, panel, incumbent_w, current_sell, heat_scale=current_heat)
    print(f"[book-learn] sell flag: {sell_result['decision']}")

    prev_pol = {}
    if POLICY_PATH.exists():
        try:
            prev_pol = json.loads(POLICY_PATH.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            prev_pol = {}
    risk_result = _evaluate_risk_scale(frames, panel, incumbent_w, prev_pol, top_n)
    print(f"[book-learn] risk scale: {risk_result['decision']}")
    heat_result = _evaluate_heat_scale(
        frames, panel, incumbent_w,
        float(prev_pol.get("heat_scale", current_heat)),
        top_n,
    )
    print(f"[book-learn] heat scale: {heat_result['decision']}")

    policy = _write_policy(adopted, bool(sell_result["adopted"]), results,
                           sell_result, asof, risk_result, heat_result)
    _write_ledger(policy, results, sell_result, risk_result, heat_result)
    print(f"[book-learn] policy v{policy['version']} → {POLICY_PATH}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=None)
    ap.add_argument("--lookback", type=int, default=40)
    ap.add_argument("--top", type=int, default=10)
    ap.add_argument("--update-prices", action="store_true")
    args = ap.parse_args()
    run(date=args.date, lookback=args.lookback, top_n=args.top,
        update_prices=args.update_prices)


if __name__ == "__main__":
    main()
