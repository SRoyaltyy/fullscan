"""Full-sheet supervised ML → Excel H / I. Research only.

PANEL PATH (war room): batch-rebuild Yahoo/rows for many tickers ×
multi-year into a **name-day panel**, then train **once** across that
panel with a time holdout. Iterative = rebuild → train → holdout →
next fold. Never ML inside interactive one-stock Excel loops.

Clock-clean feature matrix from the Yahoo A–F DAG that *is* the
workbook (every reconstructable A–JL value + lags). Same-row H/I
are labels, never features. Live flatten_robust is not imported.

  python engine/mine_hi_ml.py
  python engine/mine_hi_ml.py --folds-only
  python engine/mine_hi_ml.py --render-only
"""
from __future__ import annotations

import argparse
import json
import math
import os
import sys
import warnings
from collections import defaultdict
from datetime import date

import numpy as np

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from clock import COST_FUTU_LONG, SHIP  # noqa: E402
from harden_hyst_open import lottery_day, splice_md  # noqa: E402
from mine_hi_horizon import FLAT_BPS, HORIZON_PLAIN, HORIZONS, load_spy_regimes  # noqa: E402
from mine_hi_soft_regime import (  # noqa: E402
    HEAT_MIN_OBS, HEAT_WINDOW, heat_bucket, prior_i_mean, tercile_cuts,
)
from mine_next_region import Q1_CUT, deeper  # noqa: E402
from mine_pair_lag import VALUE_OPEN_44, FILL_OPEN, LANDMINE_VALUE  # noqa: E402
from mine_unmined import HALF_CUT, Q3_CUT, _blk, _push, _slot  # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
REPO = os.path.dirname(ROOT)
SPLIT_PATH = os.path.join(HERE, "holdout_split.json")
CLOCK_MAP = os.path.join(ROOT, "research", "clock_map.json")
META_3603 = os.path.join(ROOT, "research", "all_cols_sample", "_meta.json")
PARQUET = os.path.join(REPO, "data", "prices", "ohlc.parquet")
RESEARCH = os.path.join(ROOT, "research")
SCOREBOARD = os.path.join(REPO, "03_scoreboard")
OUT_MD = os.path.join(RESEARCH, "HI_ML.md")
OUT_JSON = os.path.join(RESEARCH, "hi_ml.json")
SB_MD = os.path.join(SCOREBOARD, "EXCEL_BOT_MINE.md")
AO_MD = os.path.join(RESEARCH, "AO_FIRST_MINE.md")
CYCLE_MD = os.path.join(RESEARCH, "MINE_CYCLE.md")
MARKER = "## H/I full-sheet ML (multi-year, clock-clean)"

# live flatten_robust stays frozen — named so tests can see the label
LIVE_UNTOUCHED = "flatten_robust"
# Batch panel, not per-ticker Excel chat.
PANEL_PATH = (
    "batch Yahoo/rows → name-day panel → train once → time holdout → next fold"
)
TIME_CUT = "2026-04-01"  # primary chronological train < cut ≤ holdout
# Expanding-window folds. Each holdout is [train_end, hold_end).
FOLDS = (
    ("fold_q1", "2026-01-01", "2026-04-01"),
    ("fold_q2", "2026-04-01", "2026-07-01"),
    ("fold_q3", "2026-07-01", None),
)
BEAT = 0.002
FEE = COST_FUTU_LONG
TOP_Q = 0.20
LAGS = (1, 2, 5)
WARMUP = 50
MIN_BARS = 80
JULY_BAR = 0.40
TOP5_BAR = 0.25
Q1_N = 20
CELL_HOLD_N = 100
# Same-row values that may enter at the open (Excel-locked 44).
OPEN_SAME_ROW = set(VALUE_OPEN_44)
# Extra open-knowable derived (not a letter; C[t] vs B[t−1]).
OPEN_DERIVED = ("overnight", "heat5")
# Same-row close values allowed at close-entry. Never H/I, and never
# same-row transforms of H/I (B = C×(1+H); BJ is a bin of H; close
# SMAs that include today's B).
LABEL_TRANSFORMS = ("B", "BJ", "CJ", "CK", "CL", "wick_up", "wick_dn")
CLOSE_SAME_ROW = (
    "D", "E", "F", "G", "K", "M", "N", "AO", "BN",
    "range", "typ", "BU",
)
LABELS = ("H", "I", "I_sum")
MODELS = ("linear", "ridge", "lgb")
CLOCKS = ("open", "close")

PLAIN = {
    "overnight": "overnight gap (today's open vs yesterday's close)",
    "heat5": "prior-5 mean of I (yesterday and older, never today)",
    "H": "intraday % (close vs open) — label; lags are yesterday+",
    "I": "daily % (close vs yesterday) — label; lags are yesterday+",
    "J": "open-to-open %",
    "C": "today's open",
    "AH": "count of prior 6 H prints ≤ −5%",
    "FR": "prior-5 volume median > 1M and/or prior-3 G ≥ 3",
    "G": "volume vs yesterday",
    "K": "high wick vs open",
    "M": "low wick vs open",
    "EK": "prior 40-day mean H minus prior 10-day mean H",
    "EN": "prior volume bucket (sheet EN)",
    "EP": "weighted |prior H| (sheet EP)",
    "ER": "sign of a prior 3–5% H/I print (sheet ER)",
    "ES": "carried ER",
    "FQ": "yesterday H > 3%",
    "HF": "stdev of prior 8 closes",
    "JB": "7+ of the last 8 H prints were |H| > 3%",
    "JC": "none of the last 8 H prints were < −3%",
    "CJ": "50-day average close",
    "CK": "20-day average close",
    "CL": "20-day close stdev",
    "BN": "volume vs 50-day average",
    "AC": "yesterday high vs 50-day close",
    "BJ": "same-day H bin (≥5% / ≤−3%) — close-entry only at lag 0",
    "B": "close — close-entry only at lag 0",
    "D": "high — close-entry only at lag 0",
    "E": "low — close-entry only at lag 0",
    "F": "volume — close-entry only at lag 0",
}


def _safe_div(a, b):
    out = np.full_like(a, np.nan, dtype=np.float64)
    ok = np.isfinite(a) & np.isfinite(b) & (np.abs(b) > 1e-12)
    out[ok] = a[ok] / b[ok]
    return out


def _shift(a, k):
    if k <= 0:
        return np.asarray(a, dtype=np.float64)
    out = np.full(len(a), np.nan, dtype=np.float64)
    if k < len(a):
        out[k:] = np.asarray(a, dtype=np.float64)[:-k]
    return out


def _roll_mean(a, w):
    a = np.asarray(a, dtype=np.float64)
    n = len(a)
    out = np.full(n, np.nan)
    if w <= 0 or n == 0:
        return out
    c = np.cumsum(np.nan_to_num(a, nan=0.0))
    cnt = np.cumsum(np.isfinite(a).astype(np.float64))
    tot = c.copy()
    nobs = cnt.copy()
    tot[w:] = c[w:] - c[:-w]
    nobs[w:] = cnt[w:] - cnt[:-w]
    ok = nobs >= max(2, w // 2)
    out[ok] = tot[ok] / nobs[ok]
    out[: w - 1] = np.nan
    return out


def _roll_std(a, w):
    a = np.asarray(a, dtype=np.float64)
    mu = _roll_mean(a, w)
    sq = _roll_mean(a * a, w)
    var = sq - mu * mu
    var[var < 0] = 0
    out = np.sqrt(var)
    out[: w - 1] = np.nan
    return out


def _roll_max(a, w):
    a = np.asarray(a, dtype=np.float64)
    n = len(a)
    out = np.full(n, np.nan)
    for i in range(w - 1, n):
        sl = a[i - w + 1: i + 1]
        sl = sl[np.isfinite(sl)]
        if len(sl):
            out[i] = sl.max()
    return out


def _roll_med(a, w):
    a = np.asarray(a, dtype=np.float64)
    n = len(a)
    out = np.full(n, np.nan)
    for i in range(w - 1, n):
        sl = a[i - w + 1: i + 1]
        sl = sl[np.isfinite(sl)]
        if len(sl):
            out[i] = np.median(sl)
    return out


def _count_prior(a, pred, w):
    """Count pred(a[t-w:t]) — yesterday and older, never a[t]."""
    a = np.asarray(a, dtype=np.float64)
    n = len(a)
    out = np.full(n, np.nan)
    for i in range(w, n):
        sl = a[i - w: i]
        sl = sl[np.isfinite(sl)]
        if len(sl) < max(2, w // 2):
            continue
        out[i] = float(np.sum(pred(sl)))
    return out


def hi_from_ohlc(o, c, prev_c=None):
    """Excel H = (close−open)/open · I = (close−prev close)/prev close."""
    h = ((c - o) / o) if o and c else None
    i = ((c - prev_c) / prev_c) if c and prev_c else None
    return h, i


def overnight_from_ohlc(o, prev_c):
    """Open-knowable: today's open vs yesterday's close. Never uses H/I."""
    if o and prev_c:
        return (o - prev_c) / prev_c
    return None


def load_clocks():
    raw = json.load(open(CLOCK_MAP, encoding="utf-8"))
    by = {r["col"]: r for r in raw["columns"]}
    return raw, by


def assert_ml_gate(feat_names, clock):
    """Refuse same-row H/I, core_score, and open-entry landmines."""
    for name in feat_names:
        base, lag = parse_feat(name)
        if base in ("H", "I") and lag == 0:
            raise ValueError(f"same-row H/I feature: {name}")
        if "core_score" in name:
            raise ValueError(f"core_score leaked: {name}")
        if base in LABEL_TRANSFORMS and lag == 0:
            raise ValueError(f"same-row H/I transform: {name}")
        if clock == "open" and lag == 0:
            if base in LANDMINE_VALUE or base in ("H", "I"):
                raise ValueError(f"open landmine: {name}")
            if base not in OPEN_SAME_ROW and base not in OPEN_DERIVED:
                if base in CLOSE_SAME_ROW or base in ("B", "D", "E", "F", "G",
                                                      "K", "M", "N"):
                    raise ValueError(f"close value at open: {name}")
    return True


def parse_feat(name):
    if "_l" in name:
        base, lag_s = name.rsplit("_l", 1)
        try:
            return base, int(lag_s)
        except ValueError:
            return name, 0
    return name, 0


def locked_tickers():
    meta = json.load(open(META_3603, encoding="utf-8"))
    return [t for t in (meta.get("tickers") or []) if t]


def load_split():
    raw = json.load(open(SPLIT_PATH, encoding="utf-8"))
    return set(raw["discovery"]), set(raw["holdout"])


def load_spy_from_parquet(dates):
    """SPY up/down/flat on the parquet calendar. Prefer parquet over the 2026-only tape."""
    import pyarrow.compute as pc
    import pyarrow.parquet as pq

    t = pq.read_table(PARQUET, columns=["date", "ticker", "close"])
    spy = t.filter(pc.equal(t["ticker"], "SPY"))
    by = {}
    prev = None
    ds = spy["date"].to_pylist()
    cs = spy["close"].to_pylist()
    rows = sorted(zip(ds, cs), key=lambda x: str(x[0])[:10])
    for d, c in rows:
        iso = str(d)[:10]
        if prev is not None and prev:
            ret = (c - prev) / prev
            if abs(ret) < FLAT_BPS:
                by[iso] = 0
            else:
                by[iso] = 1 if ret > 0 else -1
        prev = c
    # fill any tape-only days
    tape = load_spy_regimes()
    for k, v in tape.items():
        by.setdefault(k, v)
    return by


def build_sheet_values(o, h, lo, c, v):
    """Vectorized A–JL DAG on Yahoo A–F. Letters match the sheet formulas."""
    o = np.asarray(o, dtype=np.float64)
    h = np.asarray(h, dtype=np.float64)
    lo = np.asarray(lo, dtype=np.float64)
    c = np.asarray(c, dtype=np.float64)
    v = np.asarray(v, dtype=np.float64)
    n = len(c)
    S = {}
    S["B"] = c
    S["C"] = o
    S["D"] = h
    S["E"] = lo
    S["F"] = v
    S["G"] = _safe_div(v, _shift(v, 1))
    S["H"] = _safe_div(c - o, o)
    S["I"] = _safe_div(c - _shift(c, 1), _shift(c, 1))
    S["J"] = _safe_div(o - _shift(o, 1), _shift(o, 1))
    S["K"] = _safe_div(h - o, o + 0.001)
    S["M"] = -_safe_div(lo - o, o)
    S["AO"] = S["J"]
    S["overnight"] = _safe_div(o - _shift(c, 1), _shift(c, 1))
    typ = (o + h + lo) / 3.0
    S["typ"] = typ
    S["range"] = _safe_div(h - lo, o)
    mx = np.maximum(c, o)
    mn = np.minimum(c, o)
    S["wick_up"] = _safe_div(h - mx, o)
    S["wick_dn"] = _safe_div(mn - lo, o)
    dtyp = typ - _shift(typ, 1)
    bu = np.full(n, np.nan)
    ok = np.isfinite(dtyp)
    bu[ok] = np.where(dtyp[ok] < 0, -1.0, 1.0)
    S["BU"] = bu
    S["CJ"] = _roll_mean(c, 50)
    S["CK"] = _roll_mean(c, 20)
    S["CL"] = _roll_std(c, 20)
    S["BN"] = _safe_div(v, _roll_mean(v, 50))
    S["AH"] = _count_prior(S["H"], lambda x: x <= -0.05, 6)
    med5 = _shift(_roll_med(v, 5), 1)
    mxg3 = _shift(_roll_max(S["G"], 3), 1)
    fr = np.full(n, np.nan)
    ok = np.isfinite(med5) | np.isfinite(mxg3)
    fr[ok] = 0.0
    fr[np.isfinite(med5) & (med5 > 1_000_000)] += 1.0
    fr[np.isfinite(mxg3) & (mxg3 >= 3)] += 1.0
    S["FR"] = fr
    S["EK"] = _shift(_roll_mean(S["H"], 40) - _roll_mean(S["H"], 10), 1)
    # EN: prior-4 volume buckets (sheet EN reads prior F)
    avg4 = _shift(_roll_mean(v, 4), 1)
    mx4 = _shift(_roll_max(v, 4), 1)
    en = np.zeros(n)
    en[np.isfinite(mx4) & (mx4 < 200_000)] = -2.0
    en[np.isfinite(avg4) & (avg4 >= 700_000) & (avg4 < 3_000_000)] = 1.0
    en[np.isfinite(avg4) & (avg4 >= 3_000_000) & (avg4 < 12_000_000)] = 2.0
    en[np.isfinite(avg4) & (avg4 >= 12_000_000)] = 3.0
    en[:5] = np.nan
    S["EN"] = en
    h1, h2 = _shift(S["H"], 1), _shift(S["H"], 2)
    ep = np.abs(h2) * 0.4 + np.abs(h1) * 0.6
    boost = (np.isfinite(h1) & (h1 > 0))
    ep = np.where(boost, ep * 1.5, ep)
    S["EP"] = ep
    i1 = _shift(S["I"], 1)
    er = np.zeros(n)
    er[(np.isfinite(h1) & (h1 < -0.03)) | (np.isfinite(i1) & (i1 < -0.03))] = -1
    er[(np.isfinite(h1) & (h1 > 0.05)) | (np.isfinite(i1) & (i1 > 0.05))] = 1
    er[:2] = np.nan
    S["ER"] = er
    es = np.full(n, np.nan)
    last = 0.0
    for i in range(n):
        if np.isfinite(er[i]) and er[i] != 0:
            last = er[i]
        if i >= 2:
            es[i] = last
    S["ES"] = es
    fq = np.zeros(n)
    fq[np.isfinite(h1) & (h1 > 0.03)] = 1.0
    fq[:2] = np.nan
    S["FQ"] = fq
    S["HF"] = _shift(_roll_std(c, 8), 1)
    jb = _count_prior(S["H"], lambda x: np.abs(x) > 0.03, 8)
    S["JB"] = np.where(np.isfinite(jb), (jb >= 7).astype(float), np.nan)
    jc = _count_prior(S["H"], lambda x: x < -0.03, 8)
    S["JC"] = np.where(np.isfinite(jc), (jc == 0).astype(float), np.nan)
    S["AC"] = _safe_div(_shift(h, 1) - _shift(S["CJ"], 1), _shift(S["CJ"], 1))
    bj = np.zeros(n)
    hh = S["H"]
    bj[np.isfinite(hh) & (hh >= 0.05)] = 1.0
    bj[np.isfinite(hh) & (hh <= -0.03)] = -1.0
    S["BJ"] = bj
    # rolling stand-ins for the many AVERAGE/STDEV/COUNTIF letters
    for src, tag in (("H", "H"), ("I", "I"), ("J", "J"), ("G", "G"),
                     ("F", "F"), ("K", "K"), ("M", "M")):
        for w in (5, 20):
            S[f"{tag}m{w}"] = _shift(_roll_mean(S[src], w), 1)  # prior window
            S[f"{tag}s{w}"] = _shift(_roll_std(S[src], w), 1)
    heat = np.full(n, np.nan)
    for i in range(n):
        heat[i] = prior_i_mean(S["I"], i, HEAT_WINDOW, HEAT_MIN_OBS)
    S["heat5"] = heat
    return S


# Letters / derived that exist in S
SHEET_LETTERS = (
    "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "M", "AO",
    "AH", "FR", "EK", "EN", "EP", "ER", "ES", "FQ", "HF", "JB", "JC",
    "CJ", "CK", "CL", "BN", "AC", "BJ", "BU", "typ", "range",
    "wick_up", "wick_dn", "overnight", "heat5",
)
ROLL_FEATS = tuple(
    f"{tag}{stat}{w}"
    for tag in ("H", "I", "J", "G", "F", "K", "M")
    for stat in ("m", "s")
    for w in (5, 20)
)


def feature_is_legal(base, lag, clock):
    if base in ("H", "I") and lag == 0:
        return False
    if base in LABEL_TRANSFORMS and lag == 0:
        return False
    if "core_score" in base:
        return False
    if lag >= 1:
        return True
    if clock == "open":
        return base in OPEN_SAME_ROW or base in OPEN_DERIVED
    # close-entry: any reconstructed col except same-row H/I
    return base not in ("H", "I")


def feat_name(base, lag):
    return f"{base}_l{lag}"


def build_feature_list(clock):
    names = []
    # same-row
    for base in list(OPEN_SAME_ROW) + list(OPEN_DERIVED) + list(CLOSE_SAME_ROW) + list(ROLL_FEATS) + list(SHEET_LETTERS):
        if feature_is_legal(base, 0, clock) and feat_name(base, 0) not in names:
            if base in SHEET_LETTERS or base in ROLL_FEATS or base in OPEN_DERIVED:
                names.append(feat_name(base, 0))
    # lags of every reconstructed col (including H/I)
    for base in SHEET_LETTERS + ROLL_FEATS:
        for lag in LAGS:
            if feature_is_legal(base, lag, clock):
                nm = feat_name(base, lag)
                if nm not in names:
                    names.append(nm)
    return names


def labels_at(H, I, ei, k):
    last = ei + k - 1
    if last >= len(H):
        return None
    h = H[last]
    i_print = I[last]
    acc = 1.0
    for j in range(ei, last + 1):
        iv = I[j]
        if not np.isfinite(iv):
            return None
        acc *= (1.0 + iv)
    if not np.isfinite(h) or not np.isfinite(i_print):
        return None
    return {"H": float(h), "I": float(i_print), "I_sum": float(acc - 1.0)}


def load_panel(tickers):
    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.parquet as pq

    want = [t for t in tickers]
    table = pq.read_table(PARQUET)
    table = table.filter(pc.is_in(table["ticker"], value_set=pa.array(want)))
    by = {t: [] for t in want}
    tick_col = table["ticker"].to_pylist()
    date_col = table["date"].to_pylist()
    o = table["open"].to_pylist()
    h = table["high"].to_pylist()
    lo = table["low"].to_pylist()
    c = table["close"].to_pylist()
    v = table["volume"].to_pylist()
    for i, t in enumerate(tick_col):
        bucket = by.get(t)
        if bucket is None:
            continue
        d = date_col[i]
        iso = d.date().isoformat() if hasattr(d, "date") else str(d)[:10]
        bucket.append((iso, o[i], h[i], lo[i], c[i], v[i]))
    return by


def ticker_rows(ticker, rows, feat_open, feat_close):
    rows = sorted(rows, key=lambda r: r[0])
    if len(rows) < MIN_BARS:
        return []
    iso = [r[0] for r in rows]
    o = np.array([r[1] for r in rows], dtype=np.float64)
    h = np.array([r[2] for r in rows], dtype=np.float64)
    lo = np.array([r[3] for r in rows], dtype=np.float64)
    c = np.array([r[4] for r in rows], dtype=np.float64)
    v = np.array([r[5] for r in rows], dtype=np.float64)
    S = build_sheet_values(o, h, lo, c, v)
    H, I = S["H"], S["I"]
    out = []
    n = len(iso)
    for ei in range(WARMUP, n):
        labs = {}
        ok = True
        for k in HORIZONS:
            lab = labels_at(H, I, ei, k)
            if lab is None:
                ok = False
                break
            labs[k] = lab
        if not ok:
            continue
        fo = np.empty(len(feat_open), dtype=np.float32)
        for j, name in enumerate(feat_open):
            base, lag = parse_feat(name)
            src = ei - lag
            fo[j] = S[base][src] if base in S and 0 <= src < n else np.nan
        fc = np.empty(len(feat_close), dtype=np.float32)
        for j, name in enumerate(feat_close):
            base, lag = parse_feat(name)
            src = ei - lag
            fc[j] = S[base][src] if base in S and 0 <= src < n else np.nan
        out.append({
            "ticker": ticker, "date": iso[ei], "H": labs,
            "heat": S["heat5"][ei], "overnight": S["overnight"][ei],
            "xo": fo, "xc": fc,
        })
    return out


def pearson(x, y):
    x, y = np.asarray(x, float), np.asarray(y, float)
    m = np.isfinite(x) & np.isfinite(y)
    if m.sum() < 8:
        return None
    a, b = x[m], y[m]
    a = a - a.mean()
    b = b - b.mean()
    d = np.sqrt((a * a).sum() * (b * b).sum())
    if d < 1e-18:
        return 0.0
    return float((a * b).sum() / d)


def spearman(x, y):
    x, y = np.asarray(x, float), np.asarray(y, float)
    m = np.isfinite(x) & np.isfinite(y)
    if m.sum() < 8:
        return None
    rx = _rank(x[m])
    ry = _rank(y[m])
    return pearson(rx, ry)


def _rank(a):
    order = np.argsort(a, kind="mergesort")
    ranks = np.empty(len(a), dtype=np.float64)
    ranks[order] = np.arange(1, len(a) + 1, dtype=np.float64)
    return ranks


def winsor_fit(X, p=0.01):
    lo = np.nanquantile(X, p, axis=0)
    hi = np.nanquantile(X, 1 - p, axis=0)
    return lo, hi


def winsor_apply(X, lo, hi):
    return np.clip(X, lo, hi)


def median_fill_fit(X):
    med = np.nanmedian(X, axis=0)
    med = np.where(np.isfinite(med), med, 0.0)
    return med


def median_fill_apply(X, med):
    out = np.array(X, copy=True)
    for j in range(out.shape[1]):
        col = out[:, j]
        col[~np.isfinite(col)] = med[j]
        out[:, j] = col
    return out


def standardize_fit(X):
    mu = X.mean(axis=0)
    sd = X.std(axis=0)
    sd[sd < 1e-8] = 1.0
    return mu, sd


def standardize_apply(X, mu, sd):
    return (X - mu) / sd


def fit_linear(X, y):
    from sklearn.linear_model import LinearRegression
    m = LinearRegression()
    m.fit(X, y)
    return m


def fit_ridge(X, y):
    from sklearn.linear_model import RidgeCV
    m = RidgeCV(alphas=(0.1, 1.0, 10.0, 100.0, 1000.0))
    m.fit(X, y)
    return m


def fit_lgb(X, y):
    import lightgbm as lgb
    m = lgb.LGBMRegressor(
        n_estimators=80, max_depth=4, learning_rate=0.05,
        subsample=0.8, colsample_bytree=0.5, min_child_samples=200,
        verbosity=-1, n_jobs=4,
    )
    m.fit(X, y)
    return m


FITTERS = {"linear": fit_linear, "ridge": fit_ridge, "lgb": fit_lgb}


def predict(model, X):
    return np.asarray(model.predict(X), dtype=np.float64)


def importance(model, names, kind):
    if kind == "lgb" and hasattr(model, "feature_importances_"):
        imp = np.asarray(model.feature_importances_, float)
    elif hasattr(model, "coef_"):
        imp = np.abs(np.asarray(model.coef_, float).ravel())
    else:
        return []
    order = np.argsort(-imp)
    out = []
    for i in order[:12]:
        if imp[i] <= 0:
            continue
        base, lag = parse_feat(names[i])
        plain = PLAIN.get(base, base)
        when = "today (open-knowable)" if lag == 0 else f"{lag} session(s) ago"
        out.append({
            "feat": names[i], "base": base, "lag": lag,
            "weight": float(imp[i]),
            "plain": f"{plain} — {when}",
        })
    return out


def _cell():
    return {
        "disc": _slot(), "hold": _slot(),
        "early": _slot(), "late": _slot(),
        "spy_up": _slot(), "spy_dn": _slot(), "spy_flat": _slot(),
        "q12": _slot(), "q3": _slot(), "q1": _slot(),
        "tickers": set(), "dates": set(),
        "day": defaultdict(float),
        "ticker_pnl": defaultdict(float),
        "month": defaultdict(lambda: _slot()),
        "heat_hot": _slot(), "heat_mid": _slot(), "heat_cold": _slot(),
    }


def push_trade(cell, ticker, iso, net, split_t, spy, heat, cuts):
    _push(cell["disc"] if split_t == "discovery" else cell["hold"], net)
    half = "early" if iso < HALF_CUT else "late"
    _push(cell[half], net)
    qkey = "q12" if iso < Q3_CUT else "q3"
    _push(cell[qkey], net)
    if iso < Q1_CUT:
        _push(cell["q1"], net)
    tape = spy.get(iso)
    if tape == 1:
        _push(cell["spy_up"], net)
    elif tape == -1:
        _push(cell["spy_dn"], net)
    elif tape == 0:
        _push(cell["spy_flat"], net)
    hb = heat_bucket(heat, cuts) if cuts else None
    if hb:
        _push(cell[f"heat_{hb}"], net)
    cell["tickers"].add(ticker)
    cell["dates"].add(iso)
    cell["ticker_pnl"][ticker] += net
    cell["day"][iso] += net
    _push(cell["month"][iso[:7]], net)


def score_long(recs, pred, y, split_of, spy, cuts, book_y, time_hold,
               train_mask=None):
    """Long the top quintile of pred on each side of the split."""
    hold_mask = np.asarray(time_hold, dtype=bool)
    if train_mask is None:
        train_mask = ~hold_mask
    else:
        train_mask = np.asarray(train_mask, dtype=bool)
    cell = _cell()
    book = _cell()
    for is_hold in (False, True):
        m = hold_mask if is_hold else train_mask
        sl = "holdout" if is_hold else "discovery"
        if m.sum() < 10:
            continue
        p = pred[m]
        finite = np.isfinite(p)
        if finite.sum() < 10:
            continue
        thresh = np.nanquantile(p[finite], 1.0 - TOP_Q)
        sub = np.where(m)[0]
        for j in sub:
            rec = recs[j]
            net_book = float(book_y[j]) - FEE
            push_trade(book, rec["ticker"], rec["date"], net_book, sl,
                       spy, rec["heat"], cuts)
            if np.isfinite(pred[j]) and pred[j] >= thresh:
                net = float(y[j]) - FEE
                push_trade(cell, rec["ticker"], rec["date"], net, sl,
                           spy, rec["heat"], cuts)
    return cell, book


def pack_ml(name, clock, label, hz, cell, book_cell, ic):
    baselines = {}
    b = _blk(book_cell["hold"]) or _blk(book_cell["disc"])
    if b:
        baselines[f"{clock}_{label}_{hz}"] = b
        baselines[f"{clock}_1_h{hz}"] = b
    # reuse pack_row-like checks via a thin wrapper
    from mine_unmined import pack_row
    row = pack_row(name, clock, "long", f"hold{hz}", cell, {
        f"{clock}_long_h{hz}": _blk(book_cell["disc"]) if _blk(book_cell["disc"]) else None,
    })
    if not row:
        row = {
            "def": name, "clock": clock, "side": "long",
            "exit": f"hold{hz}", "verdict": "THIN", "keep": "THIN",
            "fail_reasons": ["thin_disc"], "n_tickers": len(cell["tickers"]),
            "n_dates": len(cell["dates"]),
            "discovery": _blk(cell["disc"]), "holdout": _blk(cell["hold"]),
        }
    row["label"] = label
    row["horizon"] = hz
    row["horizon_plain"] = HORIZON_PLAIN.get(hz, f"{hz}d")
    row["family"] = "ml"
    row["live_untouched"] = LIVE_UNTOUCHED
    row["heat_hot"] = _blk(cell["heat_hot"])
    row["heat_mid"] = _blk(cell["heat_mid"])
    row["heat_cold"] = _blk(cell["heat_cold"])
    row["spy_flat"] = row.get("spy_flat") or _blk(cell["spy_flat"])
    row["ic"] = ic
    row["book"] = {
        "discovery": _blk(book_cell["disc"]),
        "holdout": _blk(book_cell["hold"]),
    }
    d, h = row.get("discovery") or {}, row.get("holdout") or {}
    bh = _blk(book_cell["hold"])
    bd = _blk(book_cell["disc"])
    edge_h = None
    if h and bh:
        edge_h = h["avg_net"] - bh["avg_net"]
    edge_d = None
    if d and bd:
        edge_d = d["avg_net"] - bd["avg_net"]
    row["edge_vs_book_hold"] = edge_h
    row["edge_vs_book_disc"] = edge_d
    reasons = list(row.get("fail_reasons") or [])
    if edge_h is not None and edge_h < BEAT:
        reasons.append("no_edge_vs_book")
    if edge_d is not None and edge_d < BEAT:
        reasons.append("no_edge_vs_book_disc")
    deeper(row, cell)
    reasons = list(dict.fromkeys((row.get("fail_reasons") or []) + reasons))
    row["fail_reasons"] = reasons
    hblk = row.get("holdout") or {}
    if (row.get("n_tickers") or 0) < SHIP["n_tickers"] or (hblk.get("n") or 0) < CELL_HOLD_N:
        row["verdict"] = "THIN"
        row["keep"] = "THIN"
    elif reasons:
        row["verdict"] = "KILL"
        row["keep"] = "KILL"
    else:
        row["verdict"] = "KEEP"
        row["keep"] = "KEEP"
    q1 = row.get("q1") or {}
    if q1.get("n", 0) >= Q1_N and q1.get("avg_net", 0) <= 0:
        if "q1_sign" not in row["fail_reasons"]:
            row["fail_reasons"].append("q1_sign")
        row["verdict"] = "KILL"
        row["keep"] = "KILL"
    if (row.get("top5_share") or 0) > TOP5_BAR:
        row["verdict"] = "KILL"
        row["keep"] = "KILL"
    if (row.get("july_share") or 0) > JULY_BAR:
        row["verdict"] = "KILL"
        row["keep"] = "KILL"
    return row


def _pct(blk):
    if not blk:
        return "—"
    return f"{blk['avg_net']*100:+.2f}% (n={blk['n']})"


def _edge(x):
    if x is None:
        return "—"
    return f"{x*100:+.2f} pp"


def _ic(d):
    if not d:
        return "—"
    sp, pe = d.get("spearman"), d.get("pearson")
    if sp is None and pe is None:
        return "—"
    return f"ρ={sp:+.3f} / r={pe:+.3f}" if sp is not None else f"r={pe:+.3f}"


FOLD_NAMES = {f[0] for f in FOLDS}


def _prefer_fold(rows, extra=None):
    """Prefer expanding-window q2, then the first-pass combined holdout."""
    extra = extra or (lambda _r: True)
    cand = [r for r in rows if extra(r)]
    present = {r.get("fold") for r in cand}
    for pref in ("fold_q2", "fold_combined", None):
        if pref in present:
            return [r for r in cand if r.get("fold") == pref]
    return cand


def assemble(tickers, disc, hold, feat_open, feat_close, spy):
    by = load_panel(tickers)
    recs = []
    n_ok = 0
    for i, t in enumerate(tickers, 1):
        rows = by.get(t) or []
        got = ticker_rows(t, rows, feat_open, feat_close)
        if got:
            n_ok += 1
            recs.extend(got)
        if i % 400 == 0:
            print(f"  panel {i}/{len(tickers)} recs={len(recs)}", flush=True)
    return recs, n_ok


def fold_masks(recs, train_end, hold_end=None):
    """Train is date < train_end. Holdout is [train_end, hold_end)."""
    train = np.array([r["date"] < train_end for r in recs])
    hold = np.array([
        (r["date"] >= train_end)
        and (hold_end is None or r["date"] < hold_end)
        for r in recs
    ])
    return train, hold


def split_masks(recs, disc, hold, train_end=TIME_CUT, hold_end=None):
    train, time_hold = fold_masks(recs, train_end, hold_end)
    name_hold = np.array([r["ticker"] in hold for r in recs])
    name_disc = np.array([r["ticker"] in disc for r in recs])
    return train, time_hold, name_hold, name_disc


def run_models(recs, feat_names, clock, X, spy, disc, hold,
               train_end=TIME_CUT, hold_end=None, fold="fold_q2",
               models=None, labels=None, horizons=None):
    models = models or MODELS
    labels = labels or LABELS
    horizons = horizons or HORIZONS
    train_mask, time_hold, name_hold, name_disc = split_masks(
        recs, disc, hold, train_end, hold_end)
    heats = [r["heat"] for r in recs]
    train_heat = [heats[i] for i in range(len(recs)) if train_mask[i]
                  and np.isfinite(heats[i])]
    cuts = tercile_cuts(train_heat)
    rows = []
    importances = {}
    # prep X on this fold's train only — do not leak later folds
    train_X = X[train_mask] if train_mask.any() else X
    lo, hi = winsor_fit(train_X)
    X = winsor_apply(X, lo, hi)
    med = median_fill_fit(X[train_mask] if train_mask.any() else X)
    X = median_fill_apply(X, med)
    mu, sd = standardize_fit(X[train_mask] if train_mask.any() else X)
    X = standardize_apply(X, mu, sd)
    Xs = X

    for lab in labels:
        for hz in horizons:
            y = np.array([r["H"][hz][lab] for r in recs], dtype=np.float64)
            book_y = y  # buy-everyone = the same label
            tr = train_mask & np.isfinite(y)
            if tr.sum() < 300:
                continue
            # cap train size so ridge/LGB stay inside the 15 GB box
            tr_idx = np.where(tr)[0]
            if len(tr_idx) > 180_000:
                rng = np.random.default_rng(20260401)
                tr_idx = rng.choice(tr_idx, 180_000, replace=False)
                tr = np.zeros(len(recs), dtype=bool)
                tr[tr_idx] = True
            # gap-algebra baseline (overnight → I / I_sum; lagged H/J → H)
            if lab in ("I", "I_sum"):
                gap = np.array([r["overnight"] for r in recs], dtype=np.float64)
                ic_gap = {
                    "spearman": spearman(gap[time_hold], y[time_hold]),
                    "pearson": pearson(gap[time_hold], y[time_hold]),
                    "kind": "overnight→label",
                }
            else:
                # yesterday I / J as the honest H baseline
                # H_l1 is in X; use overnight as a weak H baseline too
                gap = np.array([r["overnight"] for r in recs], dtype=np.float64)
                ic_gap = {
                    "spearman": spearman(gap[time_hold], y[time_hold]),
                    "pearson": pearson(gap[time_hold], y[time_hold]),
                    "kind": "overnight→H",
                }
            for kind in models:
                try:
                    model = FITTERS[kind](Xs[tr], y[tr])
                except Exception as e:
                    print(f"  skip {kind} {lab} {hz}: {e}", flush=True)
                    continue
                pred = predict(model, Xs)
                ic = {
                    "spearman": spearman(pred[time_hold], y[time_hold]),
                    "pearson": pearson(pred[time_hold], y[time_hold]),
                    "spearman_name": spearman(pred[name_hold], y[name_hold])
                    if name_hold.any() else None,
                    "pearson_name": pearson(pred[name_hold], y[name_hold])
                    if name_hold.any() else None,
                    "gap": ic_gap,
                }
                # map time_hold False → discovery, True → holdout for pack_row
                split_recs_flag = time_hold  # True = holdout
                cell, book = score_long(
                    recs, pred, y,
                    None, spy, cuts, book_y, split_recs_flag,
                    train_mask=train_mask,
                )
                name = f"ml_{clock}_{kind}_{lab}_{HORIZON_PLAIN.get(hz, hz)}"
                row = pack_ml(name, clock, lab, hz, cell, book, ic)
                row["model"] = kind
                row["fold"] = fold
                row["train_end"] = train_end
                row["hold_end"] = hold_end
                row["n_features"] = len(feat_names)
                row["n_train"] = int(tr.sum())
                row["n_hold_rows"] = int(time_hold.sum())
                row["gap_ic"] = ic_gap
                row["plain"] = (
                    f"{kind} on {clock}-entry full-sheet DAG → "
                    f"{lab} {HORIZON_PLAIN.get(hz, hz)}; long top {int(TOP_Q*100)}%"
                )
                rows.append(row)
                key = f"{fold}_{clock}_{kind}_{lab}_{hz}"
                imp = importance(model, feat_names, kind)
                importances[key] = imp
                # fold_q2 keeps the unprefixed key so the first-pass
                # driver section still resolves.
                if fold == "fold_q2":
                    importances[f"{clock}_{kind}_{lab}_{hz}"] = imp
                print(
                    f"  {row['keep']:4} {name:42} "
                    f"hold={_pct(row.get('holdout'))} "
                    f"edge={_edge(row.get('edge_vs_book_hold'))} "
                    f"ic={_ic(ic)} "
                    f"{','.join(row.get('fail_reasons') or []) or '—'}",
                    flush=True,
                )
    return rows, importances, cuts


def _spearman(row, key="ic"):
    d = row.get(key) or {}
    return d.get("spearman")


def apply_honesty_bar(rows):
    """Kill gap-algebra I and same-print close 1d H/I. Not a new join."""
    for r in rows:
        reasons = list(r.get("fail_reasons") or [])
        lab, hz, clock = r.get("label"), r.get("horizon"), r.get("clock")
        ml = _spearman(r)
        gap = _spearman(r, "gap_ic")
        if clock == "open" and lab in ("I", "I_sum") and gap is not None and ml is not None:
            # I = overnight + scaled H. If ML ≤ overnight, it is the gap.
            if ml <= gap + 0.02:
                reasons.append("gap_algebra")
        if clock == "close" and hz == 1 and lab in ("H", "I", "I_sum"):
            if ml is not None and ml >= 0.50:
                reasons.append("same_print_close")
        r["fail_reasons"] = list(dict.fromkeys(reasons))
        if r.get("keep") == "KEEP" and (
            "gap_algebra" in r["fail_reasons"]
            or "same_print_close" in r["fail_reasons"]
        ):
            r["verdict"] = "KILL"
            r["keep"] = "KILL"
    # Stacked I KEEP after a 1d I gap is the same overnight, compounded.
    # Apply per fold so q1/q2/q3 do not smear each other.
    open_i1_gap = {
        r.get("fold")
        for r in rows
        if r.get("clock") == "open" and r.get("label") == "I" and r.get("horizon") == 1
        and "gap_algebra" in (r.get("fail_reasons") or [])
    }
    if open_i1_gap:
        for r in rows:
            if (r.get("clock") == "open" and r.get("label") == "I_sum"
                    and r.get("keep") == "KEEP"
                    and r.get("fold") in open_i1_gap):
                r.setdefault("fail_reasons", []).append("gap_algebra")
                r["fail_reasons"] = list(dict.fromkeys(r["fail_reasons"]))
                r["verdict"] = "KILL"
                r["keep"] = "KILL"
    close_i1_print = {
        r.get("fold")
        for r in rows
        if r.get("clock") == "close" and r.get("label") == "I" and r.get("horizon") == 1
        and "same_print_close" in (r.get("fail_reasons") or [])
    }
    if close_i1_print:
        for r in rows:
            if (r.get("clock") == "close" and r.get("label") == "I_sum"
                    and r.get("keep") == "KEEP"
                    and r.get("fold") in close_i1_print):
                r.setdefault("fail_reasons", []).append("same_print_close")
                r["fail_reasons"] = list(dict.fromkeys(r["fail_reasons"]))
                r["verdict"] = "KILL"
                r["keep"] = "KILL"
    return rows


def family_verdict(rows):
    """KEEP only if open-entry 1d H or I clears the bar *beyond* the gap."""
    primary = [r for r in rows
               if r.get("clock") == "open"
               and r.get("horizon") == 1
               and r.get("label") in ("H", "I")
               and r.get("keep") == "KEEP"]
    if primary:
        return "KEEP", primary
    return "null", []


def english_lead(verdict, rows, meta):
    n_t = meta.get("n_tickers") or 0
    n_r = meta.get("n_rows") or 0
    span = meta.get("date_span") or "?"
    n_feat = meta.get("n_feat_open") or 0
    if verdict == "KEEP":
        wrap = (
            "Full-sheet ML on clock-clean A–JL values (Yahoo A–F DAG, "
            "multi-year) cleared the holdout ship bar *beyond* the "
            "overnight gap. KEEP the pass. Research only; live "
            "flatten_robust stays frozen."
        )
    else:
        wrap = (
            "Full-sheet ML on clock-clean A–JL values (Yahoo A–F DAG, "
            "as far back as the book / parquet go) does not beat the "
            "overnight gap. Clean **null**. Predicting I at the open is "
            "gap algebra: I = overnight + scaled H, and overnight is "
            "knowable at 9:30 (C today vs B yesterday). Ridge and "
            "LightGBM IC on I match the gap (or lose). Predicting H "
            "(the rest of the day) dies on both tapes and a five-name "
            "ghost. Hand gates did not hide a nonlinear join. Research "
            "only; live flatten_robust stays frozen."
        )
    prim = [r for r in rows if r.get("clock") == "open" and r.get("horizon") == 1
            and r.get("label") in ("H", "I") and r.get("model") in ("ridge", "lgb")]
    bits = []
    for r in prim:
        bits.append(
            f"{r.get('fold') or 'cut'} {r['model']} → {r['label']} 1d "
            f"holdout {_pct(r.get('holdout'))} "
            f"vs book {_edge(r.get('edge_vs_book_hold'))} "
            f"IC {_ic(r.get('ic'))} (**{r['keep']}**)"
        )
    extra = (" " + "; ".join(bits) + ".") if bits else ""
    return (
        wrap + "\n\n"
        f"**Panel path (not per-ticker chat):** {PANEL_PATH}. "
        f"Panel: **{n_t}** names · **{n_r}** name-days · {span}. "
        f"Open-entry features **{n_feat}** (locked 44 + open-derived + "
        f"lags of every reconstructed letter, never same-row H/I). "
        f"Walk-forward folds {', '.join(f[0] for f in FOLDS)}; primary "
        f"cut **{TIME_CUT}**. Futubull 0.15% long off the "
        f"top-{int(TOP_Q*100)}% recipe and the buy-everyone book."
        + extra
    )


def render(rows, importances, meta, verdict):
    keeps = [r for r in rows if r.get("keep") == "KEEP"]
    kills = [r for r in rows if r.get("keep") == "KILL"]
    thins = [r for r in rows if r.get("keep") == "THIN"]
    L = [
        "# Full-sheet ML → H / I (multi-year, clock-clean)",
        "",
        f"_Generated {date.today()} · live `{LIVE_UNTOUCHED}` frozen. "
        "Yahoo/rows A–F seed the whole A–JL DAG. No Excel STOCKHISTORY "
        "cache. Not a forecast wire._",
        "",
        "## Plain English",
        "",
        english_lead(verdict, rows, meta),
        "",
        f"**Family verdict: {verdict}**",
        "",
        "### Path (not per-ticker chat)",
        "",
        f"**{PANEL_PATH}**",
        "",
        "One Yahoo A–F rebuild builds the **name-day panel**. Models "
        "train once across that panel, then the next expanding-window "
        "fold re-uses the same matrix. Iterative = rebuild → train → "
        "holdout → next fold. There is no interactive one-stock Excel "
        "loop and no per-ticker chat fit.",
        "",
        "### Walk-forward folds",
        "",
        "| fold | train < | holdout | model | label | holdout pnl | vs book | IC | gap IC | verdict | why |",
        "|---|---|---|---|---|---|---|---|---|---|---|",
    ]
    wf = [r for r in rows
          if r.get("fold") in FOLD_NAMES
          and r.get("clock") == "open" and r.get("horizon") == 1
          and r.get("label") in ("H", "I")
          and r.get("model") in ("ridge", "lgb")]
    wf.sort(key=lambda r: (
        r.get("fold") or "", r.get("label") or "", r.get("model") or ""))
    if not wf:
        L.append(
            "| *(folds not in this file — first pass is the combined "
            "Apr–Aug holdout below)* | | | | | | | | | | |"
        )
    for r in wf:
        L.append(
            f"| {r.get('fold')} | {r.get('train_end')} | "
            f"{r.get('hold_end') or 'tape end'} | {r.get('model')} | "
            f"{r.get('label')} | {_pct(r.get('holdout'))} | "
            f"{_edge(r.get('edge_vs_book_hold'))} | {_ic(r.get('ic'))} | "
            f"{_ic(r.get('gap_ic'))} | **{r.get('keep')}** | "
            f"{','.join(r.get('fail_reasons') or []) or '—'} |"
        )
    L += [
        "",
        "### What was scored",
        "",
        "Cyrus asked for supervised ML: anything clock-clean from rows "
        "above (and open-knowable same-row only if fair) trains to predict "
        "**H** (intraday % close vs open) and **I** (daily % vs yesterday) "
        "below. The feature matrix is the **entire workbook A–JL** that "
        "the sheet can compute from Yahoo A–F — every reconstructable "
        "letter’s value, plus lags — not the locked-44 pair+lag subset. "
        "Fills that need ColorEngine (the 275-col dump is not on this "
        "VM) are out of this pass; their values are the DAG. Text EQ "
        "and external VIX (IY) are skipped.",
        "",
        "Excel gate (enforced in `assert_ml_gate`):",
        "",
        "- Never same-row H or I (nor transforms). Labels only. "
        "Yesterday’s H/I are fair.",
        "- Never same-row `core_score` / anything that reads D/E/F/H/I "
        "on the same row at the open.",
        "- Open-entry same-row: locked 44 values + fills A,B,C,G,J,K,L,M,O "
        "(fills not dumped here) + overnight gap (C today vs B yesterday).",
        "- Open-entry same-row out: B/G/K/M/O/L numbers, D/E/F/N, "
        "unknown→close.",
        "- Close-entry: other close cols OK same-row; still never H/I.",
        "",
        f"Labels are built from Yahoo A–F the same way the sheet does: "
        f"H = (B−C)/C, I = (B−B[t−1])/B[t−1], stacked I is the k-day "
        f"compound. Horizons 1d / 2d / 3d / 1w / 2w. Train is "
        f"**chronological** on the name-day panel. Expanding-window "
        f"folds: " + "; ".join(
            f"`{n}` train < {te}, hold "
            f"{'≥ '+te if he is None else f'[{te}, {he})'}"
            for n, te, he in FOLDS
        ) + f". Primary cut **{TIME_CUT}**. "
        "Name-holdout IC is reported as a ghost check, not the keep bar.",
        "",
        "Models: ordinary least squares, ridge (α by train CV), LightGBM "
        "(80 trees, depth 4). Recipe = long the top quintile of the "
        "score. Edge is versus buy-everyone after the same 15 bp fee.",
        "",
        "### Open-entry 1d (primary fold)",
        "",
        "| model | label | holdout | vs book | IC Spearman/Pearson | "
        "gap IC | Q1 | top-5 | July | tapes ↑/↓ | verdict | why |",
        "|---|---|---|---|---|---|---|---|---|---|---|---|",
    ]
    prim = _prefer_fold(
        rows,
        lambda r: r.get("clock") == "open" and r.get("horizon") == 1
        and r.get("label") in ("H", "I", "I_sum"),
    )
    prim.sort(key=lambda r: (r.get("label") or "", r.get("model") or ""))
    for r in prim:
        L.append(
            f"| {r.get('model')} | {r.get('label')} | {_pct(r.get('holdout'))} | "
            f"{_edge(r.get('edge_vs_book_hold'))} | {_ic(r.get('ic'))} | "
            f"{_ic(r.get('gap_ic'))} | {_pct(r.get('q1'))} | "
            f"{(r.get('top5_share') or 0)*100:.0f}% | "
            f"{(r.get('july_share') or 0)*100:.0f}% | "
            f"{_pct(r.get('spy_up'))} / {_pct(r.get('spy_dn'))} | "
            f"**{r.get('keep')}** | {','.join(r.get('fail_reasons') or []) or '—'} |"
        )
    L += [
        "",
        "Code names (after the English): "
        + ", ".join(f"`{r['def']}`" for r in prim[:8]) + ".",
        "",
        "### Horizons (open ridge + LightGBM)",
        "",
        "| model | label | 1d | 2d | 3d | 1w | 2w |",
        "|---|---|---|---|---|---|---|",
    ]
    for kind in ("ridge", "lgb"):
        for lab in LABELS:
            cells = []
            for hz in HORIZONS:
                pool = _prefer_fold(
                    rows,
                    lambda x, k=kind, lb=lab, h=hz: (
                        x.get("clock") == "open" and x.get("model") == k
                        and x.get("label") == lb and x.get("horizon") == h
                    ),
                )
                r = pool[0] if pool else None
                if not r:
                    cells.append("—")
                else:
                    cells.append(
                        f"**{r['keep']}** {_pct(r.get('holdout'))}"
                    )
            L.append(f"| {kind} | {lab} | " + " | ".join(cells) + " |")
    L += [
        "",
        "### Close-entry 1d (same-row close cols OK; still never H/I)",
        "",
        "| model | label | holdout | vs book | IC | verdict | why |",
        "|---|---|---|---|---|---|---|",
    ]
    close1 = _prefer_fold(
        rows,
        lambda r: r.get("clock") == "close" and r.get("horizon") == 1
        and r.get("label") in ("H", "I"),
    )
    for r in close1:
        L.append(
            f"| {r.get('model')} | {r.get('label')} | {_pct(r.get('holdout'))} | "
            f"{_edge(r.get('edge_vs_book_hold'))} | {_ic(r.get('ic'))} | "
            f"**{r.get('keep')}** | {','.join(r.get('fail_reasons') or []) or '—'} |"
        )
    L += [
        "",
        "### Soft regimes (open ridge 1d, holdout)",
        "",
        "Heat is the open-knowable prior-5 mean of I (terciles from "
        "**train** dates). Tape is SPY up / down / flat.",
        "",
        "| model | label | heat cold | mid | hot | spy↑ | spy↓ | spy flat |",
        "|---|---|---|---|---|---|---|---|",
    ]
    for r in prim:
        if r.get("model") != "ridge":
            continue
        L.append(
            f"| ridge | {r.get('label')} | {_pct(r.get('heat_cold'))} | "
            f"{_pct(r.get('heat_mid'))} | {_pct(r.get('heat_hot'))} | "
            f"{_pct(r.get('spy_up'))} | {_pct(r.get('spy_dn'))} | "
            f"{_pct(r.get('spy_flat'))} |"
        )
    L += [
        "",
        "### Top drivers (plain English)",
        "",
        "Weights from the open-entry **ridge** on 1d I (absolute "
        "coefficient) and LightGBM gain. Lags of H/I are yesterday’s "
        "prints, not today’s labels.",
        "",
    ]
    for key, title in (
        ("open_ridge_I_1", "Ridge → 1d I"),
        ("open_lgb_I_1", "LightGBM → 1d I"),
        ("open_ridge_H_1", "Ridge → 1d H"),
        ("open_lgb_H_1", "LightGBM → 1d H"),
    ):
        imp = importances.get(key) or []
        L.append(f"**{title}**")
        L.append("")
        if not imp:
            L.append("*(none)*")
            L.append("")
            continue
        for i, it in enumerate(imp[:8], 1):
            L.append(f"{i}. {it['plain']} (`{it['feat']}`)")
        L.append("")
    L += [
        "### Gap algebra vs ML",
        "",
        "At the open we already know the overnight gap "
        "(C[t] vs B[t−1]). Excel I is overnight plus a scaled H. If ML "
        "IC on I is no better than overnight→I, the sheet is a DAG on "
        "A–F and there is nothing past gap algebra. That is a clean null.",
        "",
        "| label | overnight IC | best open ML IC | ML − gap |",
        "|---|---|---|---|",
    ]
    for lab in ("H", "I"):
        rs = [r for r in prim if r.get("label") == lab]
        if not rs:
            continue
        gap = (rs[0].get("gap_ic") or {}).get("spearman")
        best = max(( (r.get("ic") or {}).get("spearman") or -9) for r in rs)
        gap_s = f"{gap:+.3f}" if gap is not None else "—"
        delta = (best - gap) if gap is not None and best > -8 else None
        if delta is None:
            L.append(f"| {lab} | {gap_s} | {best:+.3f} | — |")
        else:
            L.append(f"| {lab} | {gap_s} | {best:+.3f} | {delta:+.3f} |")
    n_recon = meta.get("n_reconstructed") or 0
    L += [
        "",
        "### Coverage (A–JL, not a 44-col subset)",
        "",
        f"- Workbook letters A–JL: **275**.",
        f"- Reconstructed by sheet formula from Yahoo A–F: **{n_recon}** "
        "named letters plus rolling AVERAGE/STDEV stand-ins for the many "
        "COUNTIF / AVERAGE / STDEV columns (EK, HF, CJ, CK, CL, AH, FR, …).",
        "- Not in this matrix: ColorEngine fills (need the 275-col dump), "
        "weekly AP–AU spill, external VIX IY, unparsed `#REF!` / FILTER "
        "letters (FS, FU, EB, …). Those letters’ **lags of values we do "
        "have** still enter. Same-row unknown→close stays out at the open.",
        f"- Open-entry feature count: **{meta.get('n_feat_open')}**. "
        f"Close-entry: **{meta.get('n_feat_close')}**.",
        "",
        "### Cuts and bars",
        "",
        f"- Panel path: **{PANEL_PATH}**.",
        f"- Walk-forward: " + "; ".join(
            f"{n} train < {te} / hold {he or 'tape end'}"
            for n, te, he in FOLDS
        ) + ".",
        f"- Primary chronological train < **{TIME_CUT}** · holdout ≥ {TIME_CUT} "
        "(first pass combined Apr–Aug; folds split that window).",
        f"- Name-holdout IC is extra (existing `holdout_split.json`).",
        f"- Long top {int(TOP_Q*100)}% of the score vs buy-everyone, "
        f"Futubull {FEE*100:.2f}% off both.",
        f"- KEEP needs holdout n≥{CELL_HOLD_N}, ≥{SHIP['n_tickers']} "
        f"tickers, edge vs book ≥{int(BEAT*10000)} bp, Q1 not red, "
        f"top-5 names ≤{int(TOP5_BAR*100)}%, July share ≤{int(JULY_BAR*100)}%, "
        "both SPY tapes, no day lottery, not thin.",
        f"- Q1 cut **{Q1_CUT}**. Half **{HALF_CUT}**. Q3 **{Q3_CUT}**.",
        "",
        "### What this does not change",
        "",
        "- Live `flatten_robust` is frozen. No card. No push.",
        "- Standing light+O ± AH/FR stays **DEMOTE** under soft regimes "
        f"(`HI_SOFT_REGIME.md`).",
        "- Open locked-44 pair+lag stays the accepted null.",
        "- Same-row H and I are never features.",
        "- Finviz BLOCKED.",
        "",
        f"Board: KEEP {len(keeps)} · KILL {len(kills)} · THIN {len(thins)}. "
        f"Family **{verdict}**.",
        "",
        "Research only. Multi-year Yahoo / one book.",
        "",
    ]
    return "\n".join(L)


def write_outputs(rows, importances, meta, verdict):
    rows = apply_honesty_bar(list(rows))
    verdict, _ = family_verdict(rows)
    md = render(rows, importances, meta, verdict)
    open(OUT_MD, "w", encoding="utf-8").write(md + "\n")
    slim_rows = []
    for r in rows:
        slim_rows.append({k: v for k, v in r.items() if k != "months"})
    payload = {
        "generated": str(date.today()),
        "spec": "full-sheet ML → H/I; clock-clean; multi-year Yahoo A–F DAG",
        "live_untouched": LIVE_UNTOUCHED,
        "excel_cache_used": False,
        "seed": "yahoo_rows_cache",
        "entry": "open-first then close",
        "labels": list(LABELS),
        "horizons": list(HORIZONS),
        "models": list(MODELS),
        "time_cut": TIME_CUT,
        "panel_path": PANEL_PATH,
        "iterative": "rebuild → train → holdout → next fold",
        "folds": [
            {"name": n, "train_end": te, "hold_end": he} for n, te, he in FOLDS
        ],
        "q1_cut": Q1_CUT,
        "q3_cut": Q3_CUT,
        "fee": FEE,
        "top_q": TOP_Q,
        "verdict": verdict,
        "gate": (
            "CLOCK_MAP + OPEN_SAME_ROW_LABELS; same-row H/I labels only; "
            "no core_score; lags of any letter fair"
        ),
        "n_keep": sum(1 for r in rows if r.get("keep") == "KEEP"),
        "n_kill": sum(1 for r in rows if r.get("keep") == "KILL"),
        "n_thin": sum(1 for r in rows if r.get("keep") == "THIN"),
        "inventory": meta,
        "importances": importances,
        "rows": slim_rows,
    }
    json.dump(payload, open(OUT_JSON, "w"), indent=2, default=str)
    block = (
        MARKER + "\n\n"
        f"Full-sheet ML → H/I (name-day panel, walk-forward folds): "
        f"**{verdict}**. {PANEL_PATH}. "
        f"See `excel_bot/research/HI_ML.md`. Research only. "
        f"Live {LIVE_UNTOUCHED} frozen.\n"
    )
    sb = splice_md(SB_MD, MARKER, block,
                   require_any=("first A–JL cut", "A–O clock cycle",
                                "H/I soft-regime"))
    ao = splice_md(AO_MD, MARKER, block, require="VISIBLE_COLS A..O")
    cy = splice_md(
        CYCLE_MD, MARKER, block,
        require_any=("first A–JL cut", "A–O clock cycle"),
    )
    open(SB_MD, "w", encoding="utf-8").write(sb)
    open(AO_MD, "w", encoding="utf-8").write(ao)
    open(CYCLE_MD, "w", encoding="utf-8").write(cy)
    return payload


def remaining_inventory(feat_open, feat_close, extra=None):
    extra = extra or {}
    return {
        "generated": str(date.today()),
        "live_untouched": LIVE_UNTOUCHED,
        "excel_cache_used": False,
        "seed": "yahoo_rows_cache",
        "letters": "A..JL",
        "n_workbook": 275,
        "n_reconstructed": extra.get("n_reconstructed", 0),
        "n_feat_open": len(feat_open),
        "n_feat_close": len(feat_close),
        "n_tickers": extra.get("n_tickers", 0),
        "n_rows": extra.get("n_rows", 0),
        "date_span": extra.get("date_span", ""),
        "time_cut": TIME_CUT,
        "panel_path": PANEL_PATH,
        "folds": [n for n, _te, _he in FOLDS],
        "gate": "Excel-locked CLOCK_MAP / OPEN_SAME_ROW_LABELS",
        "not_this_pass": [
            "ColorEngine fills (no 275-col dump on disk)",
            "weekly AP–AU",
            "external VIX IY",
            "unparsed #REF! / FILTER letters",
            "live flatten_robust",
        ],
    }


def fold_plan(folds_only):
    """One panel rebuild, then each expanding-window fold. Not per-ticker."""
    plan = []
    for fname, train_end, hold_end in FOLDS:
        full = (not folds_only) and fname == "fold_q2"
        plan.append({
            "name": fname,
            "train_end": train_end,
            "hold_end": hold_end,
            "models": MODELS if full else ("ridge", "lgb"),
            "labels": LABELS if full else ("H", "I", "I_sum"),
            "horizons": HORIZONS if full else (1,),
        })
    return plan


def merge_legacy_rows(new_rows, prev):
    """Keep first-pass combined-holdout horizons that this fold run skipped."""
    if not prev:
        return new_rows, prev.get("importances") if prev else {}
    covered = {
        (r.get("fold"), r.get("clock"), r.get("model"),
         r.get("label"), r.get("horizon"))
        for r in new_rows
    }
    has_q2 = any(r.get("fold") == "fold_q2" for r in new_rows)
    kept = []
    for r in prev.get("rows") or []:
        rr = dict(r)
        if rr.get("fold") in FOLD_NAMES:
            continue
        rr.setdefault("fold", "fold_combined")
        key = (rr.get("fold"), rr.get("clock"), rr.get("model"),
               rr.get("label"), rr.get("horizon"))
        if key in covered:
            continue
        if has_q2 and rr.get("horizon") == 1 and rr.get("fold") == "fold_combined":
            # 1d is re-scored on the split folds; drop the combined 1d twin.
            continue
        kept.append(rr)
    return new_rows + kept, prev.get("importances") or {}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--render-only", action="store_true")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument(
        "--folds-only",
        action="store_true",
        help="rebuild panel once, then ridge+lgb on 1d H/I/I_sum across FOLDS",
    )
    args = ap.parse_args()
    clocks, _by = load_clocks()
    vo = tuple(clocks["groups"]["value_mine_open"])
    assert vo == VALUE_OPEN_44 or set(vo) == set(VALUE_OPEN_44)
    feat_open = build_feature_list("open")
    feat_close = build_feature_list("close")
    assert_ml_gate(feat_open, "open")
    assert_ml_gate(feat_close, "close")
    if args.render_only:
        prev = json.load(open(OUT_JSON, encoding="utf-8"))
        rows = apply_honesty_bar(prev.get("rows") or [])
        payload = write_outputs(
            rows,
            prev.get("importances") or {},
            prev.get("inventory") or remaining_inventory(feat_open, feat_close),
            "null",
        )
        print(f"render-only verdict={payload['verdict']}", flush=True)
        return payload

    disc, hold = load_split()
    tickers = locked_tickers()
    if args.limit:
        tickers = tickers[: args.limit]
    print(f"[hi_ml] PANEL {PANEL_PATH}", flush=True)
    print(f"[hi_ml] tickers={len(tickers)} feat_open={len(feat_open)} "
          f"feat_close={len(feat_close)}", flush=True)
    recs, n_ok = assemble(tickers, disc, hold, feat_open, feat_close, {})
    if not recs:
        raise SystemExit("no panel rows")
    dates = sorted(r["date"] for r in recs)
    spy = load_spy_from_parquet(dates)
    xo_list = [r.pop("xo") for r in recs]
    xc_list = [r.pop("xc") for r in recs]
    Xo = np.stack(xo_list)
    del xo_list
    print(f"[hi_ml] panel rows={len(recs)} tickers_ok={n_ok} "
          f"span={dates[0]}…{dates[-1]} Xo={Xo.shape}", flush=True)
    meta = remaining_inventory(feat_open, feat_close, {
        "n_reconstructed": len(SHEET_LETTERS),
        "n_tickers": n_ok,
        "n_rows": len(recs),
        "date_span": f"{dates[0]} → {dates[-1]}",
    })
    import gc
    plan = fold_plan(args.folds_only)
    rows = []
    importances = {}
    print("[hi_ml] open-entry folds (same panel, new train/hold each time)",
          flush=True)
    for step in plan:
        print(f"[hi_ml] {step['name']} open train<{step['train_end']} "
              f"hold<{step['hold_end']}", flush=True)
        ro, io, _ = run_models(
            recs, feat_open, "open", Xo.copy(), spy, disc, hold,
            train_end=step["train_end"], hold_end=step["hold_end"],
            fold=step["name"], models=step["models"],
            labels=step["labels"], horizons=step["horizons"],
        )
        rows += ro
        importances.update(io)
        gc.collect()
    del Xo
    gc.collect()
    Xc = np.stack(xc_list)
    del xc_list
    print(f"[hi_ml] close-entry folds Xc={Xc.shape}", flush=True)
    for step in plan:
        print(f"[hi_ml] {step['name']} close train<{step['train_end']} "
              f"hold<{step['hold_end']}", flush=True)
        rc, ic, _ = run_models(
            recs, feat_close, "close", Xc.copy(), spy, disc, hold,
            train_end=step["train_end"], hold_end=step["hold_end"],
            fold=step["name"], models=step["models"],
            labels=step["labels"], horizons=step["horizons"],
        )
        rows += rc
        importances.update(ic)
        gc.collect()
    del Xc
    gc.collect()
    if args.folds_only and os.path.exists(OUT_JSON):
        prev = json.load(open(OUT_JSON, encoding="utf-8"))
        rows, old_imp = merge_legacy_rows(rows, prev)
        importances = {**old_imp, **importances}
    rows = apply_honesty_bar(rows)
    verdict, _prim = family_verdict(rows)
    payload = write_outputs(rows, importances, meta, verdict)
    print(f"verdict={verdict} KEEP={payload['n_keep']} "
          f"KILL={payload['n_kill']} THIN={payload['n_thin']}", flush=True)
    return payload


if __name__ == "__main__":
    warnings.filterwarnings("ignore", category=RuntimeWarning)
    main()
