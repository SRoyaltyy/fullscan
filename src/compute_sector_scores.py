"""Deterministic scoring for per-sector environment predictions.
Mirrors compute_scores.py — LLM emits components; this module owns totals.
"""
from __future__ import annotations

import re

WEIGHTS = {
    "S0_SHARED_MACRO": 2.0,
    "S1_SECTOR_FACTORS": 3.0,
    "S2_BREADTH": 2.0,
    "S3_FLOWS_POSITIONING": 1.5,
    "S4_ETF_TAPE": 0.5,
}

BOUNDS = {
    "S0_SHARED_MACRO": (-2.0, 2.0),
    "S1_SECTOR_FACTORS": (-3.0, 3.0),
    "S2_BREADTH": (-2.0, 2.0),
    "S3_FLOWS_POSITIONING": (-2.0, 2.0),
    "S4_ETF_TAPE": (-1.0, 1.0),
}

MULT_MIN, MULT_MAX = 0.5, 2.0
DIVERGENCE_LEADING_THRESHOLD = -6.0
DIRECTION_EPS = 1.0
MAGNITUDE_BANDS = [(12.0, "severe"), (7.0, "notable"), (3.0, "mild"), (0.0, "flat")]
ACTUAL_BANDS = [(2.0, "severe"), (1.0, "notable"), (0.3, "mild"), (0.0, "flat")]


def parse_scores(text: str) -> dict:
    m = re.search(r"SECTOR_SCORES_BEGIN(.*?)SECTOR_SCORES_END", text, re.S)
    block = m.group(1) if m else text
    out = {}
    for line in block.splitlines():
        m2 = re.match(r"\s*([A-Z0-9_]+):\s*(-?\d+(?:\.\d+)?)\s*$", line)
        if m2:
            out[m2.group(1)] = float(m2.group(2))
            continue
        m3 = re.match(r"\s*([A-Z0-9_]+):\s*(.+?)\s*$", line)
        if m3:
            out[m3.group(1)] = m3.group(2).strip()
    return out


def compute(scores: dict) -> dict:
    comps = {}
    for k, (lo, hi) in BOUNDS.items():
        v = scores.get(k, 0.0)
        try:
            v = float(v)
        except (TypeError, ValueError):
            v = 0.0
        comps[k] = max(lo, min(hi, v))

    try:
        mult = float(scores.get("MULTIPLIER", 1.0))
    except (TypeError, ValueError):
        mult = 1.0
    mult = max(MULT_MIN, min(MULT_MAX, mult))

    leading = (comps["S1_SECTOR_FACTORS"] * WEIGHTS["S1_SECTOR_FACTORS"]
               + comps["S0_SHARED_MACRO"] * WEIGHTS["S0_SHARED_MACRO"]
               + comps["S2_BREADTH"] * WEIGHTS["S2_BREADTH"])
    divergence = (leading <= DIVERGENCE_LEADING_THRESHOLD
                  and comps["S4_ETF_TAPE"] >= 0) or (
        leading >= -DIVERGENCE_LEADING_THRESHOLD
        and comps["S4_ETF_TAPE"] <= 0 and abs(leading) >= 6)

    total = sum(comps[k] * w for k, w in WEIGHTS.items())
    if divergence and comps["S4_ETF_TAPE"] * leading < 0:
        total -= comps["S4_ETF_TAPE"] * WEIGHTS["S4_ETF_TAPE"]
    total *= mult

    direction = "up" if total > DIRECTION_EPS else (
        "down" if total < -DIRECTION_EPS else "flat")
    magnitude = "flat"
    if direction != "flat":
        for thresh, band in MAGNITUDE_BANDS:
            if abs(total) >= thresh:
                magnitude = band
                break

    try:
        conf = float(scores.get("CONFIDENCE", 0.5))
    except (TypeError, ValueError):
        conf = 0.5

    return {
        "components": comps,
        "multiplier": mult,
        "leading_sum": leading,
        "divergence_flagged": bool(divergence),
        "total_score": round(total, 3),
        "predicted_direction": direction,
        "predicted_magnitude_band": magnitude,
        "confidence_score": max(0.0, min(1.0, conf)),
        "regime": scores.get("REGIME", "mixed"),
    }


def actual_band(pct_change: float) -> tuple[str, str]:
    direction = "up" if pct_change > 0.1 else (
        "down" if pct_change < -0.1 else "flat")
    for thresh, band in ACTUAL_BANDS:
        if abs(pct_change) >= thresh:
            return direction, band
    return direction, "flat"


def grade(predicted_direction: str, predicted_band: str,
          actual_pct: float) -> dict:
    ad, ab = actual_band(actual_pct)
    return {
        "actual_direction": ad,
        "actual_magnitude_band": ab,
        "direction_hit": predicted_direction == ad
        or (predicted_direction == "flat" and ad == "flat"),
        "magnitude_hit": predicted_band == ab,
    }
