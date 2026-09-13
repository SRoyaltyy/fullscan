"""Pale / mid / deep (and red twins) as first-class pixel flags.

classify_fill already splits green into 1.0 / 1.5 / 2.0 and red into
-1.0 / -1.5 / -2.0. The main factory only emitted mixed `_g` / `_deepg`.
This overlay adds the missing shades without rewriting excel_pixel_desc.

Shade requires a hex fill. Family-without-hex stays `_g`/`_r` only.

Clock: same gate as the factory (lag 0 fill only if letter in FILL_OPEN).
H/I same-row never emitted here — overlay starts at lag 1 for H and I.

Research only. flatten_robust untouched.
"""
from __future__ import annotations

from excel_clock_gate import FILL_OPEN
from excel_pixel_desc import ALL_LETTERS, LAGS, _legal_lag, _letter_state
from signals import classify_fill

SHADE_GREEN = ("pale", "mid", "deepg")
SHADE_RED = ("pink", "midr", "deepr")
SHADE_STREAK_GATES = (2, 3, 5)
SHADE_COUNT_GATES = (2, 3, 6, 8, 12)
SHADE_WINDOWS = (10, 20, 40)
CAMERA = tuple("ABCDEFGHIJKLMNO")


def shade_of(fam, score):
    try:
        sc = float(score or 0.0)
    except (TypeError, ValueError):
        sc = 0.0
    if fam == "green":
        if sc >= 1.75:
            return "deepg"
        if sc >= 1.25:
            return "mid"
        if sc >= 0.5:
            return "pale"
        return ""
    if fam == "red":
        if sc <= -1.75:
            return "deepr"
        if sc <= -1.25:
            return "midr"
        if sc <= -0.5:
            return "pink"
        return ""
    return ""


def shade_from_hex(hexv):
    fam, sc = classify_fill(str(hexv or "").lstrip("#"))
    return fam, sc, shade_of(fam, sc)


def _st(days, t, let, lag):
    return _letter_state(days, t, let, lag)


def _hex_score(days, t, let, lag):
    """Prefer hex intensity; unknown intensity if no hex."""
    i = t - lag
    if i < 0 or i >= len(days):
        return None
    d = days[i]
    fills = d.get("fills") or {}
    if isinstance(fills, list):
        letters = list("ABCDEFGHIJKLMNO")
        fills = {letters[k]: fills[k] for k in range(min(15, len(fills)))}
    hexv = fills.get(let) if isinstance(fills, dict) else None
    st = _st(days, t, let, lag)
    if not st:
        return None
    if hexv:
        fam, sc = classify_fill(str(hexv).lstrip("#"))
        if st["fam"] in ("green", "red"):
            fam = st["fam"]
        return {"fam": fam or st["fam"], "score": sc, "shade": shade_of(fam or st["fam"], sc)}
    return {"fam": st["fam"], "score": 0.0, "shade": ""}


def _streak_shade(days, t, let, kind, lag, cap=16):
    n = 0
    for k in range(0, cap):
        rec = _hex_score(days, t, let, lag + k)
        if not rec or rec["shade"] != kind:
            break
        n += 1
    return n


def _window_shades(days, t, let, w, lag):
    bag = {k: 0 for k in SHADE_GREEN + SHADE_RED}
    for k in range(lag, lag + w):
        rec = _hex_score(days, t, let, k)
        if rec and rec["shade"]:
            bag[rec["shade"]] += 1
    return bag


def shade_overlay(days, t):
    """Boolean map of shade flags for session T. Clock-gated."""
    d = {}
    for let in ALL_LETTERS:
        for lag in LAGS:
            if not _legal_lag("fill", let, lag):
                continue
            if let in ("H", "I") and lag < 1:
                continue
            rec = _hex_score(days, t, let, lag)
            if not rec:
                continue
            tag = f"{let}@L{lag}"
            sh = rec["shade"]
            d[f"{tag}_pale"] = sh == "pale"
            d[f"{tag}_mid"] = sh == "mid"
            d[f"{tag}_deepg"] = sh == "deepg"
            d[f"{tag}_g_not_deep"] = rec["fam"] == "green" and sh in ("pale", "mid")
            d[f"{tag}_pink"] = sh == "pink"
            d[f"{tag}_midr"] = sh == "midr"
            d[f"{tag}_deepr"] = sh == "deepr"
            d[f"{tag}_r_not_deep"] = rec["fam"] == "red" and sh in ("pink", "midr")
            if lag >= 1 or let in FILL_OPEN:
                for kind in SHADE_GREEN + SHADE_RED:
                    ss = _streak_shade(days, t, let, kind, lag)
                    d[f"{tag}_{kind}streak==1"] = ss == 1
                    for g in SHADE_STREAK_GATES:
                        d[f"{tag}_{kind}streak>={g}"] = ss >= g
            wlag = max(lag, 1)
            if lag in (0, 1):
                for w in SHADE_WINDOWS:
                    bag = _window_shades(days, t, let, w, wlag)
                    for kind in SHADE_GREEN + SHADE_RED:
                        for g in SHADE_COUNT_GATES:
                            if g > w:
                                continue
                            d[f"{let}_w{w}_{kind}>={g}"] = bag[kind] >= g

    for lag in (1, 2, 3, 5):
        for let in ("H", "I"):
            rec = _hex_score(days, t, let, lag)
            if not rec:
                continue
            sh = rec["shade"]
            d[f"{let}@L{lag}_pale"] = sh == "pale"
            d[f"{let}@L{lag}_mid"] = sh == "mid"
            d[f"{let}@L{lag}_deepg"] = sh == "deepg"
            d[f"{let}@L{lag}_pink"] = sh == "pink"
            d[f"{let}@L{lag}_midr"] = sh == "midr"
            d[f"{let}@L{lag}_deepr"] = sh == "deepr"
        h = _hex_score(days, t, "H", lag)
        i = _hex_score(days, t, "I", lag)
        if h and i:
            d[f"HI@L{lag}_both_pale"] = h["shade"] == "pale" and i["shade"] == "pale"
            d[f"HI@L{lag}_both_mid"] = h["shade"] == "mid" and i["shade"] == "mid"
            d[f"HI@L{lag}_deepg"] = h["shade"] == "deepg" and i["shade"] == "deepg"

    for let, prefix in (("I", "Iregion"), ("H", "Hregion")):
        pale = _streak_shade(days, t, let, "pale", 1)
        mid = _streak_shade(days, t, let, "mid", 1)
        deep = _streak_shade(days, t, let, "deepg", 1)
        for g in SHADE_STREAK_GATES:
            d[f"{prefix}_pale>={g}"] = pale >= g
            d[f"{prefix}_mid>={g}"] = mid >= g
            d[f"{prefix}_deepg>={g}"] = deep >= g
        d[f"{prefix}_pale_only"] = pale >= 3 and deep == 0
        d[f"{prefix}_mid_only"] = mid >= 3 and deep == 0

    for lag in (1, 2):
        bag = {k: 0 for k in SHADE_GREEN + SHADE_RED}
        for let in CAMERA:
            rec = _hex_score(days, t, let, lag)
            if rec and rec["shade"]:
                bag[rec["shade"]] += 1
        d[f"camera@L{lag}_pale>=3"] = bag["pale"] >= 3
        d[f"camera@L{lag}_mid>=3"] = bag["mid"] >= 3
        d[f"camera@L{lag}_deepg>=3"] = bag["deepg"] >= 3
        d[f"camera@L{lag}_pink>=3"] = bag["pink"] >= 3
        d[f"camera@L{lag}_midr>=3"] = bag["midr"] >= 3
        d[f"camera@L{lag}_deepr>=3"] = bag["deepr"] >= 3

    return {k: True for k, v in d.items() if v}


def features_with_shade(days, t, quant):
    from excel_pixel_desc import pixel_features
    d = pixel_features(days, t, quant)
    d.update(shade_overlay(days, t))
    return d
