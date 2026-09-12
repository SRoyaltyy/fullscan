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


def _clamp_components(scores: dict) -> tuple[dict, float]:
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
    return comps, max(MULT_MIN, min(MULT_MAX, mult))


def _llm_confidence(scores: dict) -> float:
    try:
        conf = float(scores.get("CONFIDENCE", 0.5))
    except (TypeError, ValueError):
        conf = 0.5
    return max(0.0, min(1.0, conf))


# ---- v2 rule (2026-09-12); see compute_scores.py for the reasoning --------
FLAT_EPS = 0.25
OVERLAY_CAP = 6.0
# Every sector ETF carries index beta, and the general v2 call (tape anchor
# + the LLM's news read) is the best single input the pipeline has. Adding a
# quarter of the general total to each sector lifted replayed direction hits
# from 55.7% to ~61% and was flat across 0.10–0.35, so the coefficient is
# not a knife-edge fit. The general predict runs before the sector predicts,
# so this is available at 05:55.
INDEX_CARRY = 0.25


def compute(scores: dict, sector: str | None = None, etf: str | None = None,
            ch1: dict | None = None, policy: dict | None = None,
            general_total: float | None = None) -> dict:
    """Deterministic sector scoring, v2: sector tape anchor (oil for Energy,
    NQ for Tech, note futures for bond proxies, own-ETF pre-market when
    Channel 1 has it) + INDEX_CARRY × today's general total + the
    skill-weighted LLM overlay clipped to ±OVERLAY_CAP. Falls back to
    overlay-only when no anchor is available."""
    from . import engine_policy, tape_anchor
    from .compute_scores import _band_v2

    comps, mult = _clamp_components(scores)
    anchor = tape_anchor.sector_anchor(sector or "", etf, ch1)
    mults = engine_policy.sector_multipliers(sector or "", policy)

    overlay_parts = {}
    for k, w in WEIGHTS.items():
        if anchor["available"] and k in engine_policy.SECTOR_TAPE_DUPLICATES:
            continue
        overlay_parts[k] = comps[k] * w * mults.get(k, 1.0)
    overlay_raw = sum(overlay_parts.values()) * mult
    overlay = max(-OVERLAY_CAP, min(OVERLAY_CAP, overlay_raw)) if anchor["available"] else overlay_raw

    try:
        gt = float(general_total) if general_total is not None else None
    except (TypeError, ValueError):
        gt = None
    carry = INDEX_CARRY * gt if gt is not None else 0.0
    total = anchor["score"] + overlay + carry

    leading = (comps["S1_SECTOR_FACTORS"] * WEIGHTS["S1_SECTOR_FACTORS"]
               + comps["S0_SHARED_MACRO"] * WEIGHTS["S0_SHARED_MACRO"]
               + comps["S2_BREADTH"] * WEIGHTS["S2_BREADTH"])
    disagree = anchor["available"] and overlay != 0.0 and (
        (anchor["score"] > 0) != (overlay > 0))

    if abs(total) < FLAT_EPS and gt is not None and abs(gt) >= FLAT_EPS:
        # nothing sector-specific to say: inherit the index call
        direction = "up" if gt > 0 else "down"
        magnitude = "mild"
    elif abs(total) < FLAT_EPS:
        direction, magnitude = "flat", "flat"
    else:
        direction = "up" if total > 0 else "down"
        magnitude = (_band_v2(anchor["score"] + carry, anchored=True) if anchor["available"]
                     else _band_v2(total, anchored=False))

    conf = 0.5 + min(0.35, abs(total) / 25.0)
    if disagree:
        conf -= 0.1
    conf = max(0.35, min(0.9, conf))

    return {
        "components": comps,
        "multiplier": mult,
        "leading_sum": leading,
        "divergence_flagged": bool(disagree),
        "total_score": round(total, 3),
        "predicted_direction": direction,
        "predicted_magnitude_band": magnitude,
        "confidence_score": round(conf, 3),
        "regime": scores.get("REGIME", "mixed"),
        "engine": "v2",
        "anchor": anchor,
        "overlay_score": round(overlay, 3),
        "overlay_raw": round(overlay_raw, 3),
        "index_carry": round(carry, 3),
        "general_total": gt,
        "skill_multipliers": mults,
        "llm_confidence": _llm_confidence(scores),
    }


def compute_legacy(scores: dict) -> dict:
    """Pre-2026-09-12 rule, kept for the replay harness."""
    comps, mult = _clamp_components(scores)

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

    return {
        "components": comps,
        "multiplier": mult,
        "leading_sum": leading,
        "divergence_flagged": bool(divergence),
        "total_score": round(total, 3),
        "predicted_direction": direction,
        "predicted_magnitude_band": magnitude,
        "confidence_score": _llm_confidence(scores),
        "regime": scores.get("REGIME", "mixed"),
        "engine": "legacy",
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
