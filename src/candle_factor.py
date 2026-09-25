"""Specialized red:green size/volume + candlestick patterns.

AB already embeds tape geometry (A15: 2-day body R:G > 1.4, red-wick
avg, max green body). This module lifts those ratios — plus volume
R:G and named patterns — as a standalone leak-free factor.

Every bar is a **completed** session strictly before ``asof``. At
09:30 on D the factor never sees D's open/high/low/close/volume.
Same-day Change% is not an input.

Sweep on 2026-08-13 → latest (flatten_robust would-buy):
  * baseline same-day lose rate is already 25% (the target)
  * R:G / pattern *vetoes* on those picks raised the lose rate
  * prior-session candles flag ~33% of next-day liquid gainers
    and ~37% of next-day liquid losers — a tape label, not a picker
Live flatten_robust sizing is unchanged so start-date P&L holds.
"""
from __future__ import annotations

from functools import lru_cache

from . import ticker_lookback as tl

LOOKBACK = 8
_TICKER_BARS: dict[str, list[dict]] | None = None
RULES = (
    "none",
    "drop_bear",
    "need_bull",
    "rg_gt_1",
    "rg_gt_14",
    "vol_rg_gt_1",
    "last_green",
    "last_green_vol",
    "a15",
    "low_s_drop_bear",
    "low_s_need_rg",
    "low_s_need_bull",
    "combo",
)

# Vetoing flatten picks with R:G / patterns raised the same-day lose
# rate. Default is label-only; ``capture()`` flags breakout tape.
DEFAULT_RULE = "none"


def _ticker_bars() -> dict[str, list[dict]]:
    """One pass over the OHLC store → ticker → chronological bars."""
    global _TICKER_BARS
    if _TICKER_BARS is not None:
        return _TICKER_BARS
    store = tl._ohlc_bars()
    out: dict[str, list[dict]] = {}
    if store is None or getattr(store, "empty", True):
        _TICKER_BARS = out
        return out
    reset = store.reset_index()
    cols = {str(c).lower(): c for c in reset.columns}
    need = ("date", "ticker", "open", "high", "low", "close")
    if any(k not in cols for k in need):
        _TICKER_BARS = out
        return out
    vol_col = cols.get("volume")
    recs = reset.sort_values([cols["ticker"], cols["date"]]).itertuples(index=False)
    names = list(reset.columns)
    idx = {n: i for i, n in enumerate(names)}
    di, ti = idx[cols["date"]], idx[cols["ticker"]]
    oi, hi, li, ci = (idx[cols[k]] for k in ("open", "high", "low", "close"))
    vi = idx[vol_col] if vol_col is not None else None
    for rec in recs:
        t = str(rec[ti] or "").strip().upper()
        ds = str(rec[di])[:10]
        if not t or len(ds) != 10:
            continue
        try:
            o, h, low, c = float(rec[oi]), float(rec[hi]), float(rec[li]), float(rec[ci])
        except (TypeError, ValueError):
            continue
        if not (o == o and h == h and low == low and c == c):
            continue
        vol = 0.0
        if vi is not None:
            try:
                vol = float(rec[vi])
                if vol != vol:
                    vol = 0.0
            except (TypeError, ValueError):
                vol = 0.0
        out.setdefault(t, []).append({
            "date": ds, "open": o, "high": h, "low": low, "close": c,
            "volume": vol,
        })
    _TICKER_BARS = out
    return out


def prior_bars(ticker: str, asof: str, n: int = LOOKBACK) -> list[dict]:
    """Completed OHLC+volume sessions with date < asof, oldest first."""
    t = str(ticker or "").strip().upper()
    d = str(asof or "")[:10]
    if not t or not d:
        return []
    return _as_dicts(_bars_before(t, d, int(n)))


@lru_cache(maxsize=16384)
def _bars_before(ticker: str, asof: str, n: int) -> tuple:
    bars = _ticker_bars().get(ticker) or []
    kept = [b for b in bars if b["date"] < asof]
    cut = kept[-n:] if n else kept
    return tuple(tuple(b.items()) for b in cut)


def _as_dicts(packed: tuple) -> list[dict]:
    return [dict(item) for item in packed]


def _empty() -> dict:
    return {
        "ok": False, "n": 0, "asof": None,
        "body_rg": None, "body_rg_2": None,
        "vol_rg": None, "vol_rg_2": None,
        "max_g_gt_r": False, "red_wick_gt_green": False,
        "last_green": False, "last_red": False, "last_vol_up": False,
        "engulf_bull": False, "engulf_bear": False,
        "hammer": False, "shooting_star": False,
        "morning_star": False, "three_green": False, "three_red": False,
        "a15": False, "bull_any": False, "bear_any": False,
        "score": 0.0,
    }


def features(ticker: str, asof: str, n: int = LOOKBACK) -> dict:
    """Leak-free tape factor for ticker as of 09:30 on ``asof``."""
    bars = prior_bars(ticker, asof, n=n)
    feat = _from_bars(bars)
    feat["ticker"] = str(ticker or "").strip().upper()
    feat["asof"] = str(asof or "")[:10]
    return feat


def _from_bars(bars: list[dict]) -> dict:
    z = _empty()
    z["n"] = len(bars)
    if len(bars) < 2:
        return z
    z["ok"] = True
    bodies_g = bodies_r = vol_g = vol_r = 0.0
    max_g = max_r = 0.0
    wick_g = wick_r = 0.0
    n_g = n_r = 0
    run_g = run_r = 0
    for b in bars:
        o, h, low, c = b["open"], b["high"], b["low"], b["close"]
        v = float(b.get("volume") or 0)
        body = c - o
        abs_body = abs(body)
        upper = h - max(o, c)
        lower = min(o, c) - low
        wick = upper + lower
        if body > 0:
            bodies_g += abs_body
            vol_g += v
            max_g = max(max_g, abs_body)
            wick_g += wick
            n_g += 1
            run_g += 1
            run_r = 0
        elif body < 0:
            bodies_r += abs_body
            vol_r += v
            max_r = max(max_r, abs_body)
            wick_r += wick
            n_r += 1
            run_r += 1
            run_g = 0
        else:
            run_g = 0
            run_r = 0
    z["body_rg"] = (bodies_g / bodies_r) if bodies_r > 1e-12 else (
        99.0 if bodies_g > 0 else 1.0)
    z["vol_rg"] = (vol_g / vol_r) if vol_r > 1e-12 else (
        99.0 if vol_g > 0 else 1.0)
    z["max_g_gt_r"] = max_g > max_r
    avg_w_g = wick_g / n_g if n_g else None
    avg_w_r = wick_r / n_r if n_r else None
    z["red_wick_gt_green"] = (
        avg_w_r is not None and avg_w_g is not None
        and avg_w_r > avg_w_g * 1.15
    )

    last2 = bars[-2:]
    bg2 = sum(max(b["close"] - b["open"], 0.0) for b in last2)
    br2 = sum(max(b["open"] - b["close"], 0.0) for b in last2)
    vg2 = sum(float(b.get("volume") or 0) for b in last2 if b["close"] > b["open"])
    vr2 = sum(float(b.get("volume") or 0) for b in last2 if b["close"] < b["open"])
    z["body_rg_2"] = (bg2 / br2) if br2 > 1e-12 else (99.0 if bg2 > 0 else 1.0)
    z["vol_rg_2"] = (vg2 / vr2) if vr2 > 1e-12 else (99.0 if vg2 > 0 else 1.0)

    last = bars[-1]
    prev = bars[-2]
    z["last_green"] = last["close"] > last["open"]
    z["last_red"] = last["close"] < last["open"]
    z["last_vol_up"] = (
        float(last.get("volume") or 0) > float(prev.get("volume") or 0) > 0
    )
    z["engulf_bull"] = _engulf(prev, last, side="bull")
    z["engulf_bear"] = _engulf(prev, last, side="bear")
    z["hammer"] = _hammer(last)
    z["shooting_star"] = _shooting_star(last)
    z["morning_star"] = _morning_star(bars[-3:]) if len(bars) >= 3 else False
    z["three_green"] = run_g >= 3
    z["three_red"] = run_r >= 3
    z["a15"] = (
        z["body_rg_2"] is not None and z["body_rg_2"] > 1.4
        and z["red_wick_gt_green"] and z["max_g_gt_r"]
    )
    z["bull_any"] = bool(
        z["engulf_bull"] or z["hammer"] or z["morning_star"]
        or z["three_green"] or z["last_green"]
    )
    z["bear_any"] = bool(
        z["engulf_bear"] or z["shooting_star"] or z["three_red"]
    )
    z["score"] = _score(z)
    return z


def _engulf(prev: dict, last: dict, side: str) -> bool:
    po, pc = prev["open"], prev["close"]
    lo, lc = last["open"], last["close"]
    if side == "bull":
        return pc < po and lc > lo and lc >= po and lo <= pc
    return pc > po and lc < lo and lc <= po and lo >= pc


def _hammer(bar: dict) -> bool:
    o, h, low, c = bar["open"], bar["high"], bar["low"], bar["close"]
    rng = h - low
    body = abs(c - o)
    if rng <= 0 or body <= 0:
        return False
    lower = min(o, c) - low
    upper = h - max(o, c)
    return lower >= 2 * body and upper <= body


def _shooting_star(bar: dict) -> bool:
    o, h, low, c = bar["open"], bar["high"], bar["low"], bar["close"]
    rng = h - low
    body = abs(c - o)
    if rng <= 0 or body <= 0:
        return False
    lower = min(o, c) - low
    upper = h - max(o, c)
    return upper >= 2 * body and lower <= body


def _morning_star(bars: list[dict]) -> bool:
    if len(bars) < 3:
        return False
    a, b, c = bars[-3], bars[-2], bars[-1]
    a_body = a["close"] - a["open"]
    b_body = abs(b["close"] - b["open"])
    c_body = c["close"] - c["open"]
    if a_body >= 0 or c_body <= 0:
        return False
    mid = (a["open"] + a["close"]) / 2.0
    small = b_body <= abs(a_body) * 0.5
    return small and c["close"] >= mid


def _score(z: dict) -> float:
    """Signed specialized factor. Higher = more green-body / bullish tape."""
    if not z.get("ok"):
        return 0.0
    s = 0.0
    rg = float(z.get("body_rg") or 1.0)
    vrg = float(z.get("vol_rg") or 1.0)
    s += max(min(rg, 4.0), 0.0) - 1.0
    s += 0.5 * (max(min(vrg, 4.0), 0.0) - 1.0)
    if z.get("max_g_gt_r"):
        s += 0.25
    if z.get("last_green"):
        s += 0.35
    if z.get("last_green") and z.get("last_vol_up"):
        s += 0.25
    if z.get("engulf_bull"):
        s += 0.6
    if z.get("hammer"):
        s += 0.4
    if z.get("morning_star"):
        s += 0.5
    if z.get("three_green"):
        s += 0.35
    if z.get("a15"):
        s += 0.5
    if z.get("engulf_bear"):
        s -= 0.6
    if z.get("shooting_star"):
        s -= 0.5
    if z.get("three_red"):
        s -= 0.45
    if z.get("last_red"):
        s -= 0.25
    return round(s, 4)


def keep(feat: dict, morning_s: float | None = None,
         rule: str = DEFAULT_RULE) -> bool:
    """True = keep the name. Missing bars never veto (thin tape)."""
    name = (rule or "none").lower().strip()
    if name in ("", "none"):
        return True
    if not feat or not feat.get("ok"):
        return True
    s = 0.0 if morning_s is None else float(morning_s)
    low = s < 1.0
    bear = bool(feat.get("bear_any"))
    bull = bool(feat.get("bull_any"))
    rg = float(feat.get("body_rg") or 1.0)
    vrg = float(feat.get("vol_rg") or 1.0)
    if name == "drop_bear":
        return not bear
    if name == "need_bull":
        return bull
    if name == "rg_gt_1":
        return rg >= 1.0
    if name == "rg_gt_14":
        return rg >= 1.4
    if name == "vol_rg_gt_1":
        return vrg >= 1.0
    if name == "last_green":
        return bool(feat.get("last_green"))
    if name == "last_green_vol":
        return bool(feat.get("last_green") and feat.get("last_vol_up"))
    if name == "a15":
        return bool(feat.get("a15"))
    if name == "low_s_drop_bear":
        return True if not low else not bear
    if name == "low_s_need_rg":
        return True if not low else rg >= 1.0
    if name == "low_s_need_bull":
        return True if not low else bull
    if name == "combo":
        if bear:
            return False
        if low:
            return rg >= 1.0 and not (
                feat.get("last_red") and feat.get("last_vol_up")
            )
        return True
    return True


def passes(ticker: str, asof: str, morning_s: float | None = None,
           rule: str = DEFAULT_RULE) -> bool:
    return keep(features(ticker, asof), morning_s, rule)


def capture(feat: dict) -> bool:
    """Watch-list flag: prior tape looks like a green-body breakout.

    Used to *capture* would-be top gainers without forcing a buy.
    Need a completed prior session that is green on rising volume, or
    a named bullish pattern / A15 recovery. Bear tape never captures.
    """
    if not feat or not feat.get("ok") or feat.get("bear_any"):
        return False
    if feat.get("a15") or feat.get("engulf_bull") or feat.get("morning_star"):
        return True
    if feat.get("last_green") and feat.get("last_vol_up") and (
            float(feat.get("body_rg") or 0) >= 1.0
            or float(feat.get("vol_rg") or 0) >= 1.0):
        return True
    if feat.get("three_green") and float(feat.get("body_rg") or 0) >= 1.2:
        return True
    return False


def filter_tickers(tickers: list[str], asof: str,
                   morning_s: float | None = None,
                   rule: str = DEFAULT_RULE) -> list[str]:
    out = []
    seen: set[str] = set()
    for raw in tickers:
        t = str(raw or "").strip().upper()
        if not t or t in seen:
            continue
        if passes(t, asof, morning_s, rule):
            seen.add(t)
            out.append(t)
    return out
