"""Pure-Python scoring for the pipeline. NO LLM arithmetic lives here —
the LLM emits component scores; this module owns every weighted total,
multiplier clamp, divergence rule, and band classification.

Thresholds below are REFLECTION-CALIBRATABLE: the weekly/monthly review may
propose changes, but changes land only by editing these constants (auditable
in git history).
"""
from __future__ import annotations

import re

WEIGHTS = {
    "B0_ASIA": 2.0, "B0_EUROPE": 2.0, "B1_CATALYSTS": 3.0, "B2_BONDS": 2.0,
    "B3_FEDPATH": 2.0, "B4_VIX": 1.5, "B5_SENTIMENT": 1.0, "B6_FUTURES": 0.5,
    "B7_OIL_DOLLAR": 1.0,
}

BOUNDS = {  # sanity clamp per component
    "B0_ASIA": (-2.0, 0.5), "B0_EUROPE": (-2.0, 0.5),
    "B1_CATALYSTS": (-3.0, 3.0), "B2_BONDS": (-2.0, 2.0),
    "B3_FEDPATH": (-2.0, 2.0), "B4_VIX": (-2.0, 0.5),
    "B5_SENTIMENT": (-1.0, 1.0), "B6_FUTURES": (-0.5, 0.5),
    "B7_OIL_DOLLAR": (-1.0, 1.0),
}

MULT_MIN, MULT_MAX = 0.5, 2.0
DIVERGENCE_LEADING_THRESHOLD = -8.0

# total-score -> prediction bands (max possible |total| ~30 at multiplier 1.0;
# calibrated so an ordinary all-slightly-green day is NOT "severe")
DIRECTION_EPS = 1.0          # |total| below this -> flat
MAGNITUDE_BANDS = [(12.0, "severe"), (7.0, "notable"), (3.0, "mild"),
                   (0.0, "flat")]

# actual % move -> magnitude band
ACTUAL_BANDS = [(2.0, "severe"), (1.0, "notable"), (0.3, "mild"),
                (0.0, "flat")]


def parse_scores(text: str) -> dict:
    """Extract the SCORES_BEGIN..SCORES_END block from LLM output."""
    m = re.search(r"SCORES_BEGIN(.*?)SCORES_END", text, re.S)
    block = m.group(1) if m else text
    out = {}
    for line in block.splitlines():
        m2 = re.match(r"\s*([A-Z0-9_]+):\s*(-?\d+(?:\.\d+)?)\s*$", line)
        if m2:
            out[m2.group(1)] = float(m2.group(2))
        else:
            m3 = re.match(r"\s*([A-Z0-9_]+):\s*(.+?)\s*$", line)
            if m3:
                out[m3.group(1)] = m3.group(2)
    return out


HORIZON_KEYS = {"HORIZON_3D": 3, "HORIZON_1W": 5, "HORIZON_2W": 10,
                "HORIZON_1M": 21}

_HORIZON_RE = re.compile(
    r"^\s*(up|down|flat)\s*[:|/]\s*(flat|mild|notable|severe)\s*[:|/]\s*"
    r"(\d(?:\.\d+)?|0?\.\d+|1\.0)\s*$", re.I)


def parse_horizon_calls(scores: dict) -> dict:
    """Read multi-timeframe calls from a parsed SCORES block.

    Expected lines inside the block:
        HORIZON_3D: up:mild:0.55
        HORIZON_1W: down:notable:0.60
        HORIZON_2W: flat:flat:0.50
        HORIZON_1M: up:notable:0.55
    Returns {key: {"trading_days": n, "direction": ..., "magnitude_band": ...,
    "confidence": float}}; missing/malformed lines are skipped (never block
    the daily decision — horizons are recorded, not graded today)."""
    out = {}
    for key, days in HORIZON_KEYS.items():
        raw = scores.get(key)
        if not isinstance(raw, str):
            continue
        m = _HORIZON_RE.match(raw.strip())
        if not m:
            continue
        try:
            conf = max(0.0, min(1.0, float(m.group(3))))
        except ValueError:
            conf = None
        out[key] = {"trading_days": days,
                    "direction": m.group(1).lower(),
                    "magnitude_band": m.group(2).lower(),
                    "confidence": conf}
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


def compute_legacy(scores: dict) -> dict:
    """The pre-2026-09-12 rule: LLM components only, futures weight 0.5,
    divergence cap, ±1.0 flat zone, 3/7/12 bands. Kept for the replay
    harness and for re-grading old runs; the live path uses compute()."""
    comps, mult = _clamp_components(scores)

    leading = (comps["B1_CATALYSTS"] * WEIGHTS["B1_CATALYSTS"]
               + comps["B2_BONDS"] * WEIGHTS["B2_BONDS"]
               + (comps["B0_ASIA"] + comps["B0_EUROPE"]) * WEIGHTS["B0_ASIA"])
    divergence = leading <= DIVERGENCE_LEADING_THRESHOLD and comps["B6_FUTURES"] >= 0

    total = sum(comps[k] * w for k, w in WEIGHTS.items())
    if divergence:
        total -= comps["B6_FUTURES"] * WEIGHTS["B6_FUTURES"]  # cap B6 to zero
    total *= mult

    direction = "up" if total > DIRECTION_EPS else (
        "down" if total < -DIRECTION_EPS else "flat")
    magnitude = "flat"
    if direction != "flat":
        for thresh, band in MAGNITUDE_BANDS:
            if abs(total) >= thresh:
                magnitude = band
                break

    return {"components": comps, "multiplier": mult, "leading_sum": leading,
            "divergence_flagged": divergence, "total_score": round(total, 3),
            "predicted_direction": direction,
            "predicted_magnitude_band": magnitude,
            "confidence_score": _llm_confidence(scores),
            "engine": "legacy"}


# ---- v2 rule (2026-09-12) -------------------------------------------------
# Diagnosis behind it lives in docs/ENGINE_DIAGNOSIS.md and is reproducible
# with `python -m src.replay_harness`. Short version: the call is graded on
# close-vs-prior-close, so the overnight gap that futures/Europe already show
# is part of the answer; the old rule weighted it 0.5 and let LLM components
# with 33–48% sign accuracy (bonds, sentiment, Asia) dominate, and predicted
# "flat" (a 4%-of-days outcome) 15% of the time.
FLAT_EPS = 0.25              # |total| below this -> flat (was 1.0)
OVERLAY_CAP = 6.0            # the LLM's net say, in score units (1% of tape)
# Magnitude is read off the *tape anchor* when there is one: over 290
# sessions the actual band is "mild" in every |gap| bucket until the gap
# itself reaches ~1% (score 6), where "notable" becomes the mode. Reading it
# off the LLM-inflated total predicted "notable" far too often (39% hit vs
# 64% for always-mild). Without an anchor the total is used with wider bands.
V2_MAGNITUDE_BANDS = [(12.0, "severe"), (6.0, "notable"), (0.0, "mild")]
V2_MAGNITUDE_BANDS_NO_ANCHOR = [(15.0, "severe"), (9.0, "notable"), (0.0, "mild")]


def _band_v2(score: float, anchored: bool = True) -> str:
    bands = V2_MAGNITUDE_BANDS if anchored else V2_MAGNITUDE_BANDS_NO_ANCHOR
    for thresh, band in bands:
        if abs(score) >= thresh:
            return band
    return "mild"


def compute(scores: dict, ch1: dict | None = None,
            policy: dict | None = None) -> dict:
    """Deterministic final scoring, v2.

    total = tape anchor (Python, from Channel 1) + LLM overlay, where the
    overlay is Σ skill_mult × weight × component × multiplier, clipped to
    ±OVERLAY_CAP. When the anchor is available the components that merely
    restate the tape (Asia, Europe, futures) are dropped from the overlay.
    Without Channel 1 (or with an empty tape) the overlay stands alone.
    """
    from . import engine_policy, tape_anchor

    comps, mult = _clamp_components(scores)
    anchor = tape_anchor.general_anchor(ch1)
    mults = engine_policy.general_multipliers(policy)

    overlay_parts = {}
    for k, w in WEIGHTS.items():
        if anchor["available"] and k in engine_policy.GENERAL_TAPE_DUPLICATES:
            continue
        overlay_parts[k] = comps[k] * w * mults.get(k, 1.0)
    overlay_raw = sum(overlay_parts.values()) * mult
    overlay = max(-OVERLAY_CAP, min(OVERLAY_CAP, overlay_raw)) if anchor["available"] else overlay_raw

    total = anchor["score"] + overlay

    leading = (comps["B1_CATALYSTS"] * WEIGHTS["B1_CATALYSTS"]
               + comps["B2_BONDS"] * WEIGHTS["B2_BONDS"]
               + (comps["B0_ASIA"] + comps["B0_EUROPE"]) * WEIGHTS["B0_ASIA"])
    # informational only in v2: the tape is now the anchor, not the suspect
    disagree = anchor["available"] and overlay != 0.0 and (
        (anchor["score"] > 0) != (overlay > 0))

    if abs(total) < FLAT_EPS:
        direction, magnitude = "flat", "flat"
    else:
        direction = "up" if total > 0 else "down"
        magnitude = (_band_v2(anchor["score"], anchored=True) if anchor["available"]
                     else _band_v2(total, anchored=False))

    conf = 0.5 + min(0.35, abs(total) / 25.0)
    if disagree:
        conf -= 0.1
    conf = max(0.35, min(0.9, conf))

    return {"components": comps, "multiplier": mult, "leading_sum": leading,
            "divergence_flagged": bool(disagree),
            "total_score": round(total, 3),
            "predicted_direction": direction,
            "predicted_magnitude_band": magnitude,
            "confidence_score": round(conf, 3),
            "engine": "v2",
            "anchor": anchor,
            "overlay_score": round(overlay, 3),
            "overlay_raw": round(overlay_raw, 3),
            "skill_multipliers": mults,
            "llm_confidence": _llm_confidence(scores)}


def actual_band(pct_change: float) -> tuple[str, str]:
    """Actual daily % move -> (direction, magnitude_band)."""
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


def per_factor_breakdown(components: dict, actual_pct: float) -> list[dict]:
    """Factor 'hit' = its sign agreed with the actual close direction."""
    sign = 1 if actual_pct > 0.1 else (-1 if actual_pct < -0.1 else 0)
    out = []
    for k, v in components.items():
        if v == 0 or sign == 0:
            status = "neutral"
        else:
            status = "hit" if (v > 0) == (sign > 0) else "miss"
        out.append({"factor": k, "score": v, "status": status})
    return out


def accuracy_summary(runs: list[dict], n: int) -> dict:
    """Rolling hit rates over the last n graded runs."""
    graded = [r for r in runs if r.get("actual_pct_change") is not None][-n:]
    if not graded:
        return {"n": 0, "direction_acc": None, "magnitude_acc": None}
    d = sum(1 for r in graded if r.get("direction_hit"))
    m = sum(1 for r in graded if r.get("magnitude_hit"))
    return {"n": len(graded), "direction_acc": round(d / len(graded), 3),
            "magnitude_acc": round(m / len(graded), 3)}
