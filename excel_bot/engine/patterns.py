"""Cluster / color / formula-state patterns beyond the L1–L5 / S1–S2 cards.

Existing card defs live in sweep.definition_matrix() (A / row_score /
core_score). This module adds:

  * open_score / open_core  — fill sums that never read D,E,F,H,I,N
  * color combos            — specific open- or close-knowable columns
  * lag combos              — yesterday's close-knowable fill + today's A
  * value gates             — J / gap (open) and H / I (close) formula states

Every pattern carries `clock` and `feature_cols` so mine_clock can refuse
an illegal open entry.
"""
from __future__ import annotations

from clock import (
    OPEN_CORE_IDX,
    OPEN_IDX,
    assert_clock_legal,
    col_idx,
    feature_clock,
)
from signals import _runs
from sweep import definition_matrix, detect_def


def _with_clock(defn, feature_cols, clock=None, name=None):
    d = dict(defn)
    d["feature_cols"] = tuple(feature_cols)
    d["clock"] = clock or feature_clock(feature_cols)
    if name:
        d["name"] = name
    assert_clock_legal(d["feature_cols"], d["clock"])
    return d


def existing_card_defs():
    """Original 33 defs, clock-labeled. A-keyed also emitted at close
    (the live cards enter at close even when A is open-knowable)."""
    out = []
    for d in definition_matrix():
        key = d.get("key", "a")
        if key == "a":
            out.append(_with_clock(d, [0], "open"))
            close = _with_clock(d, [0], "close", name=d["name"] + "_closeclk")
            out.append(close)
        elif key == "core_score":
            out.append(_with_clock(d, list(range(10)), "close"))
        else:
            out.append(_with_clock(d, list(range(15)), "close"))
    return out


def new_score_defs():
    out = []
    for key, cols in (("open_score", OPEN_IDX), ("open_core", OPEN_CORE_IDX)):
        for ml in (1, 2, 3):
            out.append(_with_clock(
                {"name": f"strict_{key}_ml{ml}", "kind": "strict",
                 "key": key, "thresh": 0.5, "min_len": ml},
                cols, "open"))
        for tol in (2, 3):
            for ml in (2, 3):
                out.append(_with_clock(
                    {"name": f"tol{tol}_{key}_ml{ml}", "kind": "tolerant",
                     "key": key, "thresh": 0.5, "tol": tol, "min_len": ml},
                    cols, "open"))
        for enter, ex in ((3, 0), (5, 0), (5, 2)):
            out.append(_with_clock(
                {"name": f"hyst_{key}_e{enter}_x{ex}", "kind": "hyst",
                 "key": key, "enter": enter, "exit": ex, "min_len": 2},
                cols, "open"))
    return out


def _combo(name, letters, family, min_count=None, min_len=1, side=None):
    cols = tuple(col_idx(c) for c in letters)
    clock = feature_clock(cols)
    if side is None:
        side = 1 if family == "green" else -1
    return {
        "name": name, "kind": "combo", "cols": cols, "family": family,
        "min_count": min_count if min_count is not None else len(cols),
        "min_len": min_len, "side_hint": side,
        "feature_cols": cols, "clock": clock,
    }


def new_combo_defs():
    out = []
    # Open-knowable color pairs / majority (A-keyed pairs still open).
    for letters, fam, ml in (
        ("AG", "green", 1), ("AJ", "green", 1), ("AL", "green", 1),
        ("AO", "green", 1), ("GJ", "green", 1),
        ("AG", "red", 1), ("AJ", "red", 1),
        ("A", "green", 1), ("A", "red", 1),
        ("G", "green", 2), ("J", "green", 2), ("L", "green", 2),
        ("O", "green", 2),
        ("G", "red", 2), ("J", "red", 2),
    ):
        out.append(_combo(f"combo_{'_'.join(letters)}_{fam}_ml{ml}",
                          letters, fam, min_len=ml))
    # Majority of the 9 open-knowable fills.
    out.append(_combo("open_maj5_green", OPEN_LETTERS_STR(), "green",
                      min_count=5, min_len=1))
    out.append(_combo("open_maj7_green", OPEN_LETTERS_STR(), "green",
                      min_count=7, min_len=1))
    out.append(_combo("open_maj5_red", OPEN_LETTERS_STR(), "red",
                      min_count=5, min_len=1))
    # Close-knowable combos — close clock forced by feature_cols.
    for letters, fam in (("HI", "green"), ("HI", "red"),
                         ("N", "green"), ("N", "red"),
                         ("DEF", "green"), ("DEF", "red")):
        out.append(_combo(f"combo_{letters}_{fam}", letters, fam, min_len=1))
    # Divergence: A green + H red (uses H → close).
    out.append({
        "name": "div_A_green_H_red", "kind": "combo_mix",
        "want": ((0, "green"), (7, "red")),
        "min_len": 1, "side_hint": 1,
        "feature_cols": (0, 7), "clock": "close",
    })
    out.append({
        "name": "div_A_red_H_green", "kind": "combo_mix",
        "want": ((0, "red"), (7, "green")),
        "min_len": 1, "side_hint": -1,
        "feature_cols": (0, 7), "clock": "close",
    })
    return out


def OPEN_LETTERS_STR():
    return "ABCGJKLMO"


def new_lag_defs():
    """Yesterday's close-knowable fill + today's A. Knowable at today's open."""
    out = []
    specs = [
        ("lag_Hred_Agreen", ((7, "red"),), ((0, "green"),), 1),
        ("lag_Ired_Agreen", ((8, "red"),), ((0, "green"),), 1),
        ("lag_Nred_Agreen", ((13, "red"),), ((0, "green"),), 1),
        ("lag_Hgreen_Ared", ((7, "green"),), ((0, "red"),), -1),
        ("lag_Igreen_Ared", ((8, "green"),), ((0, "red"),), -1),
        ("lag_Hred_Ared", ((7, "red"),), ((0, "red"),), -1),
        ("lag_Hgreen_Agreen", ((7, "green"),), ((0, "green"),), 1),
    ]
    for name, prior, today, side in specs:
        # feature_cols listed are TODAY's fills only for the clock check;
        # prior fills are already in the past at the open.
        today_cols = tuple(c for c, _ in today)
        out.append({
            "name": name, "kind": "lag_combo",
            "prior": prior, "today": today, "side_hint": side,
            "min_len": 1, "feature_cols": today_cols, "clock": "open",
        })
    return out


def new_value_defs():
    """Formula-state gates computable from stored OHLCV (no --all-cols)."""
    return [
        {"name": "gap_up_1", "kind": "value_gate", "value_key": "gap",
         "op": ">=", "thresh": 0.01, "min_len": 1, "side_hint": 1,
         "feature_cols": (), "clock": "open"},
        {"name": "gap_up_3", "kind": "value_gate", "value_key": "gap",
         "op": ">=", "thresh": 0.03, "min_len": 1, "side_hint": 1,
         "feature_cols": (), "clock": "open"},
        {"name": "gap_dn_1", "kind": "value_gate", "value_key": "gap",
         "op": "<=", "thresh": -0.01, "min_len": 1, "side_hint": -1,
         "feature_cols": (), "clock": "open"},
        {"name": "gap_dn_3", "kind": "value_gate", "value_key": "gap",
         "op": "<=", "thresh": -0.03, "min_len": 1, "side_hint": -1,
         "feature_cols": (), "clock": "open"},
        {"name": "j_up_1", "kind": "value_gate", "value_key": "j_ret",
         "op": ">=", "thresh": 0.01, "min_len": 1, "side_hint": 1,
         "feature_cols": (col_idx("J"),), "clock": "open"},
        {"name": "j_dn_1", "kind": "value_gate", "value_key": "j_ret",
         "op": "<=", "thresh": -0.01, "min_len": 1, "side_hint": -1,
         "feature_cols": (col_idx("J"),), "clock": "open"},
        {"name": "h_up_3", "kind": "value_gate", "value_key": "h_ret",
         "op": ">=", "thresh": 0.03, "min_len": 1, "side_hint": 1,
         "feature_cols": (col_idx("H"),), "clock": "close"},
        {"name": "h_dn_3", "kind": "value_gate", "value_key": "h_ret",
         "op": "<=", "thresh": -0.03, "min_len": 1, "side_hint": -1,
         "feature_cols": (col_idx("H"),), "clock": "close"},
    ]


def pattern_matrix(include_existing=True):
    out = []
    if include_existing:
        out.extend(existing_card_defs())
    out.extend(new_score_defs())
    out.extend(new_combo_defs())
    out.extend(new_lag_defs())
    out.extend(new_value_defs())
    return out


def detect_pattern(days, pat):
    kind = pat["kind"]
    if kind in ("strict", "tolerant", "hyst"):
        return detect_def(days, pat)
    if kind == "combo":
        return _detect_combo(days, pat)
    if kind == "combo_mix":
        return _detect_mix(days, pat)
    if kind == "lag_combo":
        return _detect_lag(days, pat)
    if kind == "value_gate":
        return _detect_value(days, pat)
    raise ValueError(kind)


def _detect_combo(days, pat):
    cols = pat["cols"]
    fam = pat["family"]
    need = pat.get("min_count", len(cols))
    side = pat.get("side_hint") or (1 if fam == "green" else -1)
    regimes = []
    for d in days:
        n = sum(1 for i in cols if i < len(d["fams"]) and d["fams"][i] == fam)
        regimes.append(side if n >= need else 0)
    return _runs(regimes, pat.get("min_len", 1))


def _detect_mix(days, pat):
    side = pat.get("side_hint", 1)
    regimes = []
    for d in days:
        ok = all(i < len(d["fams"]) and d["fams"][i] == fam
                 for i, fam in pat["want"])
        regimes.append(side if ok else 0)
    return _runs(regimes, pat.get("min_len", 1))


def _detect_lag(days, pat):
    side = pat.get("side_hint", 1)
    regimes = [0]
    for i in range(1, len(days)):
        ok_p = all(c < len(days[i - 1]["fams"]) and
                   days[i - 1]["fams"][c] == fam
                   for c, fam in pat["prior"])
        ok_t = all(c < len(days[i]["fams"]) and days[i]["fams"][c] == fam
                   for c, fam in pat["today"])
        regimes.append(side if (ok_p and ok_t) else 0)
    return _runs(regimes, pat.get("min_len", 1))


def _detect_value(days, pat):
    key, op, thresh = pat["value_key"], pat["op"], pat["thresh"]
    side = pat.get("side_hint", 1)
    regimes = []
    for d in days:
        v = d.get(key)
        if v is None:
            regimes.append(0)
            continue
        hit = (v >= thresh) if op == ">=" else (v <= thresh)
        regimes.append(side if hit else 0)
    return _runs(regimes, pat.get("min_len", 1))
