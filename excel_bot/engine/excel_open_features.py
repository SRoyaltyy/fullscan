"""Open-knowable Excel atoms beyond J — candles + tallies.

Research only. Live flatten_robust is not imported.

Clock (excel_clock_gate / CLOCK_MAP / OPEN_SAME_ROW_LABELS):
- Same-row fills: A B C G J K L M O IR IS IT
- Same-row numbers/text: the 44 value_mine_open. Never same-row H/I,
  M number, core_score, H paint, close landmines.
- DF / DG / DH / BB and BQ / BU are **close same-row**. Open-legal only
  as lag t−1+. Same-row DF/BB/BQ at 9:30 is a **leak** — abort.
- Open same-row substitutes (already in the 44): AH, JB, JC, FQ, FR,
  ER, EP, EN (prior-print tallies).

DF/DG/DH first-IFS labels are Excel-faithful. Bullish/Bearish Doji share
the same test — Excel always prints Bullish Doji. DH Three-Line Strike
reads the next row (future) and is skipped.
"""
from __future__ import annotations

from excel_clock_gate import (
    LAG_ONLY_CLOSE, OPEN_44_TALLY_SUBS, SAME_ROW_LEAK_ABORT,
    assert_feature_legal,
)


def _rng(h, l):
    if h is None or l is None:
        return None
    r = h - l
    return r if r > 0 else None


def _body(o, c):
    if o is None or c is None:
        return None
    return abs(c - o)


def _green(o, c):
    return o is not None and c is not None and c > o


def _red(o, c):
    return o is not None and c is not None and c < o


def df_pattern(bar):
    """Excel DF: 1-bar candle text. Close-knowable on that bar."""
    o, h, l, c = bar.get("o"), bar.get("h"), bar.get("l"), bar.get("c")
    r = _rng(h, l)
    b = _body(o, c)
    if r is None or b is None:
        return "None"
    br = b / r
    lo_w = (min(c, o) - l) / r
    up_w = (h - max(c, o)) / r
    # Excel IFS: first true wins. Doji tests are identical → Bullish Doji.
    if br < 0.1:
        return "Bullish Doji"
    if _green(o, c) and lo_w > 0.6 and br < 0.3:
        return "Bullish Hammer"
    if _red(o, c) and lo_w > 0.6 and br < 0.3:
        return "Bearish Hanging Man"
    if _green(o, c) and up_w > 0.6 and br < 0.3:
        return "Bullish Inverted Hammer"
    if _red(o, c) and up_w > 0.6 and br < 0.3:
        return "Bearish Shooting Star"
    if _green(o, c) and br < 0.4 and up_w > 0.3 and lo_w > 0.3:
        return "Bullish Spinning Top"
    if _red(o, c) and br < 0.4 and up_w > 0.3 and lo_w > 0.3:
        return "Bearish Spinning Top"
    if _green(o, c) and (h - c) < r * 0.05 and (o - l) < r * 0.05:
        return "Bullish Marubozu"
    if _red(o, c) and (h - o) < r * 0.05 and (c - l) < r * 0.05:
        return "Bearish Marubozu"
    if br < 0.05 and up_w < 0.1:
        return "Bullish Dragonfly Doji"
    if br < 0.05 and lo_w < 0.1:
        return "Bearish Gravestone Doji"
    if _green(o, c) and (o - l) < r * 0.05:
        return "Bullish Belt Hold"
    if _red(o, c) and (h - o) < r * 0.05:
        return "Bearish Belt Hold"
    if _green(o, c) and br < 0.15 and up_w > 0.4:
        return "Bullish High Wave Candle"
    if _red(o, c) and br < 0.15 and lo_w > 0.4:
        return "Bearish High Wave Candle"
    if _green(o, c) and lo_w > 0.5:
        return "Bullish Paper Umbrella"
    if _red(o, c) and lo_w > 0.5:
        return "Bearish Paper Umbrella"
    if _green(o, c) and (h - c) < r * 0.02:
        return "Bullish Closing Marubozu"
    if _red(o, c) and (c - l) < r * 0.02:
        return "Bearish Closing Marubozu"
    if _green(o, c) and (o - l) < r * 0.02:
        return "Bullish Opening Marubozu"
    if _red(o, c) and (h - o) < r * 0.02:
        return "Bearish Opening Marubozu"
    if _green(o, c) and (c - o) / r > 0.7:
        return "Bullish Big White Candle"
    if _red(o, c) and (o - c) / r > 0.7:
        return "Bearish Big Black Candle"
    return "None"


def dg_pattern(prev, bar):
    """Excel DG: 2-bar candle text. Close-knowable on `bar`."""
    if not prev or not bar:
        return "None"
    o0, c0 = prev.get("o"), prev.get("c")
    h0, l0 = prev.get("h"), prev.get("l")
    o, h, l, c = bar.get("o"), bar.get("h"), bar.get("l"), bar.get("c")
    r = _rng(h, l)
    b = _body(o, c)
    if None in (o0, c0, o, c, h, l):
        return "None"
    mid0 = (o0 + c0) / 2
    br = None if r is None or b is None else b / r
    if _red(o0, c0) and _green(o, c) and (c - o) > (o0 - c0):
        return "Bullish Engulfing"
    if _green(o0, c0) and _red(o, c) and (o - c) > (c0 - o0):
        return "Bearish Engulfing"
    if _red(o0, c0) and _green(o, c) and l0 is not None and c < l0 and o > mid0:
        return "Bullish Piercing Pattern"
    if _green(o0, c0) and _red(o, c) and h0 is not None and c > h0 and o < mid0:
        return "Bearish Dark Cloud Cover"
    if _red(o0, c0) and _green(o, c) and l0 is not None and l == l0:
        return "Bullish Tweezer Bottom"
    if _green(o0, c0) and _red(o, c) and h0 is not None and h == h0:
        return "Bearish Tweezer Top"
    if _red(o0, c0) and _green(o, c) and o < c0 and c > o0:
        return "Bullish Harami"
    if _green(o0, c0) and _red(o, c) and o > c0 and c < o0:
        return "Bearish Harami"
    if br is not None and br < 0.05 and _red(o0, c0) and o < c0 and c > o0:
        return "Bullish Harami Cross"
    if br is not None and br < 0.05 and _green(o0, c0) and o > c0 and c < o0:
        return "Bearish Harami Cross"
    if _red(o0, c0) and _green(o, c) and c > o0:
        return "Bullish Kicker"
    if _green(o0, c0) and _red(o, c) and c < o0:
        return "Bearish Kicker"
    return "None"


def dh_pattern(p2, prev, bar):
    """Excel DH: 3-bar candle text. Skips Three-Line Strike (reads next close)."""
    if not p2 or not prev or not bar:
        return "None"
    o2, c2 = p2.get("o"), p2.get("c")
    o1, c1 = prev.get("o"), prev.get("c")
    h1, l1 = prev.get("h"), prev.get("l")
    o, h, l, c = bar.get("o"), bar.get("h"), bar.get("l"), bar.get("c")
    r1 = _rng(h1, l1)
    b1 = _body(o1, c1)
    if None in (o2, c2, o1, c1, o, c):
        return "None"
    if (_red(o2, c2) and _red(o1, c1) and _green(o, c)
            and (c - o) > abs(o2 - c2) / 2):
        return "Bullish Morning Star"
    if (_green(o2, c2) and _green(o1, c1) and _red(o, c)
            and (o - c) > abs(o2 - c2) / 2):
        return "Bearish Evening Star"
    if _green(o2, c2) and _green(o1, c1) and _green(o, c) and c1 > c2 and c > c1:
        return "Bullish Three White Soldiers"
    if _red(o2, c2) and _red(o1, c1) and _red(o, c) and c1 < c2 and c < c1:
        return "Bearish Three Black Crows"
    if (r1 and b1 is not None and b1 / r1 < 0.05 and _red(o2, c2)
            and _green(o, c) and p2.get("h") is not None and c > p2["h"]):
        return "Bullish Abandoned Baby"
    if (r1 and b1 is not None and b1 / r1 < 0.05 and _green(o2, c2)
            and _red(o, c) and p2.get("l") is not None and c < p2["l"]):
        return "Bearish Abandoned Baby"
    return "None"


def _pstdev(xs):
    if not xs:
        return 0.0
    m = sum(xs) / len(xs)
    return (sum((x - m) ** 2 for x in xs) / len(xs)) ** 0.5


def cp_from_bands(high, low, closes20):
    """Excel CP on a *completed* bar. Same-row CP is close; this is lag."""
    if high is None or low is None or len(closes20) < 10:
        return None
    sma = sum(closes20) / len(closes20)
    sd = _pstdev(closes20)
    upper = sma + 2 * sd
    lower = sma - 2 * sd
    mid = (upper + lower) / 2
    if high >= upper:
        return 2
    if low <= lower:
        return -2
    if high <= upper and high >= mid and low <= mid:
        return 0
    if high <= mid and low >= lower:
        return -1
    if high <= upper and low >= mid:
        return 1
    return 0


def el_candles(df, dg, dh):
    """Open-fair slice of EL: prior DF:DH Bullish−Bearish. Not same-row EL."""
    texts = [x for x in (df, dg, dh) if x and x != "None"]
    bull = sum(1 for t in texts if "Bullish" in t)
    bear = sum(1 for t in texts if "Bearish" in t)
    return bull - bear


def _h(bar):
    o, c = bar.get("o"), bar.get("c")
    if not o or c is None:
        return None
    return (c - o) / o


def typical_ode(bar):
    """Excel (C+D+E)/3 = (open+high+low)/3. Close-knowable on that bar."""
    o, h, l = bar.get("o"), bar.get("h"), bar.get("l")
    if None in (o, h, l):
        return None
    return (o + h + l) / 3.0


def bu_of(bar, prev):
    """Excel BU: sign of typical-price change. Close same-row (uses D/E)."""
    t0, t1 = typical_ode(bar), typical_ode(prev) if prev else None
    if t0 is None or t1 is None:
        return None
    return -1 if (t0 - t1) < 0 else 1


def bq_of(bar, prev):
    """Excel BQ = typical × volume × BU. Close same-row (uses D/E/F)."""
    typ, bu, v = typical_ode(bar), bu_of(bar, prev), bar.get("v")
    if typ is None or bu is None or v is None:
        return None
    return typ * v * bu


def assert_lag_atoms():
    """Abort if this cut ever treats DF/BB/BQ as same-row open."""
    for col in SAME_ROW_LEAK_ABORT + LAG_ONLY_CLOSE:
        try:
            assert_feature_legal("value", col, 0)
        except ValueError:
            pass
        else:
            raise ValueError(f"same-row {col} must abort (leak)")
        assert_feature_legal("value", col, 1)
    for col in OPEN_44_TALLY_SUBS:
        assert_feature_legal("value", col, 0)


def open_features(prior, today_open):
    """Features knowable at today's 9:30 from completed prior bars + today's open.

    `prior` oldest→newest dicts: o,h,l,c,v. Never reads today's H/L/C.
    Candles DF/DG/DH/BB and tallies BQ/BU are computed on prior bars only.
    """
    out = {
        "J": None, "J_fresh": False,
        "df": "None", "dg": "None", "dh": "None", "BB_l1": 0,
        "el_candles": 0, "EQ": None, "FS": None, "CP_l1": None,
        "EN": None, "EP": None, "FR": None, "AH": None, "ER": None,
        "FQ": None, "JB": None, "JC": None,
        "BQ_l1": None, "BU_l1": None,
        "H_l1": None, "n_prior": len(prior),
        "same_row_df": False, "same_row_bb": False, "same_row_bq": False,
    }
    if today_open and prior and prior[-1].get("o"):
        po = prior[-1]["o"]
        if po:
            out["J"] = (today_open - po) / po
            out["J_fresh"] = True
    if not prior:
        return out
    last = prior[-1]
    prev = prior[-2] if len(prior) >= 2 else None
    p2 = prior[-3] if len(prior) >= 3 else None
    out["df"] = df_pattern(last)
    out["dg"] = dg_pattern(prev, last)
    out["dh"] = dh_pattern(p2, prev, last)
    out["BB_l1"] = 1 if "doji" in (out["df"] or "").lower() else 0
    out["el_candles"] = el_candles(out["df"], out["dg"], out["dh"])
    out["BU_l1"] = bu_of(last, prev)
    out["BQ_l1"] = bq_of(last, prev)
    hs, vs, closes, cps = [], [], [], []
    for b in prior:
        hv = _h(b)
        hs.append(hv)
        vs.append(b.get("v") or 0)
        if b.get("c") is not None:
            closes.append(b["c"])
        win = closes[-20:]
        cps.append(cp_from_bands(b.get("h"), b.get("l"), win))
    out["H_l1"] = hs[-1]
    out["CP_l1"] = cps[-1]
    if cps[-1] is not None:
        out["EQ"] = "L" if cps[-1] == 2 else "S"
        last4 = [x for x in cps[-4:] if x is not None]
        if last4:
            if last4[-1] < 0 and min(last4) < 0:
                out["FS"] = -2
            elif min(last4) <= -2:
                out["FS"] = -2
            elif last4[-1] == 2:
                out["FS"] = 2
            elif min(last4) >= 0 and max(last4) > 0:
                out["FS"] = 1
            else:
                out["FS"] = 0
    last6 = [x for x in hs[-6:] if x is not None]
    last8 = [x for x in hs[-8:] if x is not None]
    if last6:
        out["AH"] = sum(1 for x in last6 if x <= -0.05)
    if last8:
        out["JB"] = 1 if sum(1 for x in last8 if abs(x) > 0.03) >= 7 else 0
        out["JC"] = 1 if all(x >= -0.03 for x in last8) else 0
    h1 = hs[-1]
    out["FQ"] = 1 if h1 is not None and h1 > 0.03 else 0
    er = 0
    if h1 is not None and h1 >= 0.05:
        er = 1
    elif h1 is not None and h1 <= -0.03:
        er = -1
    out["ER"] = er
    if len(vs) >= 4:
        chunk = vs[-4:]
        mx, avg = max(chunk), sum(chunk) / 4
        en = -2 if mx < 200000 else 0
        if avg < 700000:
            pass
        elif avg < 3_000_000:
            en += 1
        elif avg < 12_000_000:
            en += 2
        else:
            en += 3
        med_h = sorted(x for x in hs[-6:] if x is not None)
        if med_h:
            mid = med_h[len(med_h) // 2]
            en += -2 if abs(mid) < 0.03 else 1
        cpw = [x for x in cps[-3:] if x is not None]
        if cpw:
            ac = sum(cpw) / len(cpw)
            en += 2 if ac > 1.5 else 1 if ac > 0.7 else 0
        out["EN"] = en
    if len(vs) >= 5:
        med = sorted(vs[-5:])[2]
        gmax = 0
        for i in range(max(1, len(vs) - 3), len(vs)):
            if vs[i - 1]:
                gmax = max(gmax, vs[i] / vs[i - 1])
        out["FR"] = (1 if med > 1_000_000 else 0) + (1 if gmax >= 3 else 0)
    # EP[t] (value-open): 0.4|H[t−2]| + 0.6|H[t−1]| × 1.5 if H[t−1]>0 and N[t−1]≥2.
    # N[t−1] stand-in = candle-net on DF/DH of t−2 × (|H[t−1]|≥3%). DD[t−1] omitted.
    if len(hs) >= 2 and hs[-1] is not None and hs[-2] is not None:
        df2 = df_pattern(prior[-2]) if len(prior) >= 2 else "None"
        dg2 = dg_pattern(prior[-3] if len(prior) >= 3 else None, prior[-2])
        dh2 = dh_pattern(
            prior[-4] if len(prior) >= 4 else None,
            prior[-3] if len(prior) >= 3 else None,
            prior[-2],
        )
        n_lag = el_candles(df2, dg2, dh2) * (1 if abs(hs[-1]) >= 0.03 else 0)
        base = abs(hs[-2]) * 0.4 + abs(hs[-1]) * 0.6
        out["EP"] = base * 1.5 if (hs[-1] > 0 and n_lag >= 2) else base
    return out


def feature_flags(xl):
    """Boolean gates. Keys are the recipe avoid/elev flags."""
    df, dg, dh = xl.get("df") or "None", xl.get("dg") or "None", xl.get("dh") or "None"
    texts = f"{df} {dg} {dh}"
    j = xl.get("J")
    return {
        "J_ge0": j is not None and j >= 0,
        "J_lt0": j is not None and j < 0,
        "J_le-1": j is not None and j <= -0.01,
        "prior_bearish": "Bearish" in texts,
        "prior_bullish": "Bullish" in texts,
        "prior_doji": "Doji" in texts,
        "prior_hammer": "Hammer" in texts and "Inverted" not in df,
        "prior_hanging": "Hanging" in texts,
        "prior_shooting": "Shooting" in texts,
        "prior_bull_engulf": dg == "Bullish Engulfing",
        "prior_bear_engulf": dg == "Bearish Engulfing",
        "prior_morning": dh == "Bullish Morning Star",
        "prior_evening": dh == "Bearish Evening Star",
        "EQ_S": xl.get("EQ") == "S",
        "EQ_L": xl.get("EQ") == "L",
        "FS_neg": (xl.get("FS") or 0) < 0,
        "FS_eq2": xl.get("FS") == 2,
        "EN_le0": xl.get("EN") is not None and xl["EN"] <= 0,
        "EN_ge2": (xl.get("EN") or 0) >= 2,
        "FR_eq0": xl.get("FR") == 0,
        "FR_ge1": (xl.get("FR") or 0) >= 1,
        "AH_ge1": (xl.get("AH") or 0) >= 1,
        "ER_p1": xl.get("ER") == 1,
        "ER_m1": xl.get("ER") == -1,
        "FQ": xl.get("FQ") == 1,
        "JB": xl.get("JB") == 1,
        "JC": xl.get("JC") == 1,
        "BB_l1": xl.get("BB_l1") == 1,
        "BU_l1_neg": xl.get("BU_l1") == -1,
        "BU_l1_pos": xl.get("BU_l1") == 1,
        "BQ_l1_neg": xl.get("BQ_l1") is not None and xl["BQ_l1"] < 0,
        "BQ_l1_pos": xl.get("BQ_l1") is not None and xl["BQ_l1"] > 0,
        "EP_ge03": (xl.get("EP") or 0) >= 0.03,
        "el_neg": xl.get("el_candles", 0) < 0,
        "el_pos": xl.get("el_candles", 0) > 0,
    }


# name, kind, mode, avoid_key, elev_key, keep_key, atoms
# atoms: (col, lag) — same-row DF/BB/BQ must never appear.
RECIPES = (
    ("avoid_J_ge0", "avoid", "avoid_refill", None, None, "J_lt0",
     (("J", 0),)),
    ("elev_cap2_J_le-1", "elevate", "elev_cap", "J_ge0", "J_le-1", None,
     (("J", 0),)),
    ("avoid_lag_bearish", "avoid", "avoid_refill", "prior_bearish", None, None,
     (("DF", 1), ("DG", 1), ("DH", 1))),
    ("elev_cap2_lag_bullish", "elevate", "elev_cap", "prior_bearish", "prior_bullish", None,
     (("DF", 1), ("DG", 1), ("DH", 1))),
    ("presence_lag_bullish", "presence", "intersect", None, "prior_bullish", None,
     (("DF", 1), ("DG", 1), ("DH", 1))),
    ("avoid_lag_doji", "avoid", "avoid_refill", "prior_doji", None, None,
     (("DF", 1), ("BB", 1))),
    ("elev_cap2_lag_doji", "elevate", "elev_cap", None, "prior_doji", None,
     (("DF", 1), ("BB", 1))),
    ("presence_lag_doji", "presence", "intersect", None, "BB_l1", None,
     (("BB", 1), ("DF", 1))),
    ("avoid_lag_bear_engulf", "avoid", "avoid_refill", "prior_bear_engulf", None, None,
     (("DG", 1),)),
    ("elev_cap2_lag_bull_engulf", "elevate", "elev_cap", None, "prior_bull_engulf", None,
     (("DG", 1),)),
    ("elev_cap2_lag_hammer", "elevate", "elev_cap", None, "prior_hammer", None,
     (("DF", 1),)),
    ("avoid_lag_evening", "avoid", "avoid_refill", "prior_evening", None, None,
     (("DH", 1),)),
    ("elev_cap2_lag_morning", "elevate", "elev_cap", None, "prior_morning", None,
     (("DH", 1),)),
    ("avoid_BU_l1_neg", "avoid", "avoid_refill", "BU_l1_neg", None, None,
     (("BU", 1),)),
    ("elev_cap2_BU_l1_pos", "elevate", "elev_cap", "BU_l1_neg", "BU_l1_pos", None,
     (("BU", 1),)),
    ("avoid_BQ_l1_neg", "avoid", "avoid_refill", "BQ_l1_neg", None, None,
     (("BQ", 1),)),
    ("elev_cap2_BQ_l1_pos", "elevate", "elev_cap", "BQ_l1_neg", "BQ_l1_pos", None,
     (("BQ", 1),)),
    ("avoid_AH_ge1", "avoid", "avoid_refill", "AH_ge1", None, None,
     (("AH", 0),)),
    ("elev_cap2_JC", "elevate", "elev_cap", None, "JC", None,
     (("JC", 0),)),
    ("avoid_FQ", "avoid", "avoid_refill", "FQ", None, None,
     (("FQ", 0),)),
    ("avoid_FR_eq0", "avoid", "avoid_refill", "FR_eq0", None, None,
     (("FR", 0),)),
    ("elev_cap2_FR_ge1", "elevate", "elev_cap", "FR_eq0", "FR_ge1", None,
     (("FR", 0),)),
    ("avoid_ER_p1", "avoid", "avoid_refill", "ER_p1", None, None,
     (("ER", 0),)),
    ("elev_cap2_ER_m1", "elevate", "elev_cap", "ER_p1", "ER_m1", None,
     (("ER", 0),)),
    ("avoid_EN_le0", "avoid", "avoid_refill", "EN_le0", None, None,
     (("EN", 0),)),
    ("elev_cap2_EN_ge2", "elevate", "elev_cap", "EN_le0", "EN_ge2", None,
     (("EN", 0),)),
    ("avoid_EP_ge03", "avoid", "avoid_refill", "EP_ge03", None, None,
     (("EP", 0),)),
    ("elev_cap2_JB", "elevate", "elev_cap", None, "JB", None,
     (("JB", 0),)),
)


def assert_recipes_legal():
    """Refuse any recipe that peeks same-row DF/BB/BQ (or other close atoms)."""
    for rec in RECIPES:
        atoms = rec[6]
        for col, lag in atoms:
            assert_feature_legal("value", col, lag)
            if col in SAME_ROW_LEAK_ABORT and lag < 1:
                raise ValueError(f"recipe {rec[0]} leaks same-row {col}")


INVENTORY = (
    # col, kind, same_row, what, gate_note
    ("A", "value+fill", "open", "date / STOCKHISTORY alias", "skip — not a threshold"),
    ("C", "value+fill", "open", "open price / IT", "skip — raw price"),
    ("J", "value+fill", "open", "open-to-open return", "scored (#153 control)"),
    ("Q", "value", "open*", "avg of prior P (warmup rows read G)", "later — warmup leak on first rows"),
    ("Z", "value", "open", "prior-row EM only", "later — EM chain"),
    ("AC", "value", "open", "prior D vs CJ", "later — needs CJ"),
    ("AH", "value", "open", "count of prior H ≤ −5% (6d)", "scored (open-44 tally sub)"),
    ("BT", "value", "open", "prior BR (RSI-like)", "later — BR chain"),
    ("BV", "value", "open", "same-row Q+BT", "later — Q/BT"),
    ("CG", "value", "open", "prior CG carry", "later"),
    ("CH", "value", "open", "prior CD/CE", "later"),
    ("DC", "value", "open", "prior CV/CX/CY wick flags", "later"),
    ("DE", "value", "open", "prior DB + vol + H", "later"),
    ("EB", "value", "open", "last nonblank EB", "later"),
    ("EK", "value", "open", "prior H average gap", "later"),
    ("EN", "value", "open", "prior vol buckets + |H| median + CP", "scored (open-44 tally sub)"),
    ("EP", "value", "open", "weighted |H[t−2]|/|H[t−1]| × N[t−1] boost", "scored (open-44 tally sub)"),
    ("EQ", "value", "open", "S/L from prior CP (text)",
     "open-44; reconstructed; not scored this cut"),
    ("ER", "value", "open", "prior H/I signed 5%/−3%", "scored (open-44 tally sub)"),
    ("ES", "value", "open", "carried ER", "later — carry"),
    ("ET", "value", "open", "prior W/X flags", "later"),
    ("EU", "value", "open", "carried ET", "later"),
    ("EV", "value", "open", "Z band hits", "later"),
    ("FQ", "value", "open", "prior H > 3%", "scored (open-44 tally sub)"),
    ("FR", "value", "open", "prior vol median >1M and/or G≥3", "scored (open-44 tally sub)"),
    ("FS", "value", "open", "prior CP run tally",
     "open-44; reconstructed; not scored this cut"),
    ("FU", "value", "open", "prior B/F quality veto", "later — #REF in older rows"),
    ("GD", "value", "open", "DN-gated prior H", "later — DN"),
    ("GE", "value", "open", "DN-gated prior vol median", "later"),
    ("GF", "value", "open", "DN-gated prior close", "later"),
    ("HF", "value", "open", "stdev of prior close", "later"),
    ("HG", "value", "open", "quiet-range flag from HF/GU/H", "later"),
    ("HW", "value", "open", "two red GU + prior CP=2", "later"),
    ("II", "value", "open", "prior IH", "later"),
    ("IR", "value+fill", "open", "STOCKHISTORY date", "skip — date"),
    ("IT", "value+fill", "open", "STOCKHISTORY open", "skip — raw open"),
    ("IY", "value", "open", "external VIX cache", "later — not on Yahoo tape"),
    ("IZ", "value", "open", "prior H vs IY", "later"),
    ("JB", "value", "open", "7/8 prior |H|>3%", "scored (open-44 tally sub)"),
    ("JC", "value", "open", "no prior H < −3% in 8d", "scored (open-44 tally sub)"),
    ("JD", "value", "open", "prior HO/GQ regime", "later — HO killed-ghost family"),
    ("JE", "value", "open", "JD flip", "later"),
    ("JF", "value", "open", "JE + prior H/U", "later"),
    ("JL", "value", "open", "prior H>0 wick combo", "later"),
    ("B", "fill only", "fill open / value close", "close price fill", "fill gate only; number OUT"),
    ("G", "fill only", "fill open / value close", "rel vol fill", "G[t−1] number is fair; same-row number OUT"),
    ("K", "fill only", "fill open / value close", "upper wick / open", "same-row number OUT"),
    ("L", "fill only", "fill open / value close", "tally fill", "L number unknown→close; L[t−1] fair"),
    ("M", "fill only", "fill open / value close", "lower wick fill", "M number OUT always same-row"),
    ("O", "fill only", "fill open / value close", "green O fill; O number uses same-row DD",
     "O green is standing keep. O number same-row OUT. Not scored this cut (BQ/BU instead)"),
    ("IS", "fill only", "fill open / value close", "STOCKHISTORY close alias", "fill only"),
    ("DF", "text", "close same-row — LEAK if used at 9:30", "1-bar patterns (Doji, Hammer, …)",
     "lag t−1+ only; same-row abort"),
    ("DG", "text", "close same-row / lag fair", "2-bar (Engulfing, Harami, …)", "lag t−1+ only"),
    ("DH", "text", "close same-row / lag fair", "3-bar (Morning/Evening Star, …)", "lag t−1+ only"),
    ("BB", "num", "close same-row — LEAK if used at 9:30", "SEARCH(doji, DF)",
     "lag t−1+ only (BB_l1). Same-row abort. Not a highlight reopen"),
    ("BQ", "num", "close same-row — LEAK if used at 9:30",
     "(open+high+low)/3 × vol × BU", "lag t−1+ only; same-row abort"),
    ("BU", "num", "close same-row / lag fair", "sign of typical-price change", "lag t−1+ only"),
    ("EL", "num", "close same-row / lag fair", "Bullish−Bearish + same-row DD",
     "EL[t] OUT (DD). el_candles = DF:DH[t−1] count — lag"),
    ("CP", "num", "close same-row / lag fair", "high/low vs 20d close bands",
     "CP[t] OUT. CP[t−1] feeds EQ/FS — not primary this cut"),
    ("N", "num", "close", "EL × |H|≥3%", "N[t] OUT. N[t−1] stand-in inside EP only"),
    ("H", "label", "close", "intraday %", "never same-row feature"),
    ("I", "label", "close", "daily %", "never same-row feature"),
    ("core_score", "landmine", "close", "A..J includes D,E,F,H,I", "never"),
)
