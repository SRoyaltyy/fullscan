"""Numeric learning for the general and sector predictors.

The lesson files are prose the LLM may or may not obey (LESSON_EFFICACY.md:
of 47 judged lessons, 3 helped and 42 were followed by worse accuracy). This
module is the part of the learning engine that cannot be ignored: it reads
the scoreboard, measures how often each LLM component's *sign* matched the
day's actual direction, and turns that into a weight multiplier that
`compute_scores` / `compute_sector_scores` apply deterministically.

Guardrails (same spirit as book_learn):
- a factor keeps its design weight until it has MIN_N non-zero graded runs;
- only proven-bad factors are muted (hit < MUTE_BELOW -> 0), mediocre ones
  are halved, good ones keep 1.0, strong ones get a small bonus;
- everything is computed walk-forward (only runs strictly before the date
  being scored), so the replay harness and the live path see the same rule.

Policy lives in 00_grounding/engine_policy.json with a history ledger.
"""
from __future__ import annotations

import json
import os
from datetime import datetime
from zoneinfo import ZoneInfo

from . import config

POLICY_PATH = os.path.join(config.GROUNDING, "engine_policy.json")

MIN_N = 8               # graded non-zero observations before we judge a factor
MUTE_BELOW = 0.45       # sign-hit below this -> weight 0
HALF_BELOW = 0.55       # sign-hit below this -> weight x0.5
BONUS_ABOVE = 0.65      # sign-hit at/above this -> weight x1.25
BONUS_MULT = 1.25
ACTUAL_EPS = 0.1        # % move that counts as a direction (matches grading)

GENERAL_KEYS = ("B0_ASIA", "B0_EUROPE", "B1_CATALYSTS", "B2_BONDS",
                "B3_FEDPATH", "B4_VIX", "B5_SENTIMENT", "B6_FUTURES",
                "B7_OIL_DOLLAR")
SECTOR_KEYS = ("S0_SHARED_MACRO", "S1_SECTOR_FACTORS", "S2_BREADTH",
               "S3_FLOWS_POSITIONING", "S4_ETF_TAPE")

# Components that duplicate the deterministic tape anchor. When the anchor is
# available they are dropped from the LLM overlay so the same overnight move
# is not counted twice.
GENERAL_TAPE_DUPLICATES = ("B0_ASIA", "B0_EUROPE", "B6_FUTURES")
SECTOR_TAPE_DUPLICATES = ("S4_ETF_TAPE",)


def _actual_dir(pct) -> str | None:
    try:
        p = float(pct)
    except (TypeError, ValueError):
        return None
    return "up" if p > ACTUAL_EPS else ("down" if p < -ACTUAL_EPS else "flat")


def multiplier_for(n: int, hit: float | None) -> float:
    if n < MIN_N or hit is None:
        return 1.0
    if hit < MUTE_BELOW:
        return 0.0
    if hit < HALF_BELOW:
        return 0.5
    if hit >= BONUS_ABOVE:
        return BONUS_MULT
    return 1.0


def factor_skill(runs: list[dict], keys, before_date: str | None = None) -> dict:
    """{key: {"n": int, "hit": float|None, "mult": float}} from graded runs
    with components. `before_date` makes it walk-forward."""
    stats = {k: [0, 0] for k in keys}
    for r in runs:
        if before_date and r.get("date", "") >= before_date:
            continue
        if r.get("ops_fail"):
            continue
        ad = _actual_dir(r.get("actual_pct_change"))
        if ad is None:
            continue
        comps = r.get("components") or {}
        for k in keys:
            try:
                v = float(comps.get(k, 0) or 0)
            except (TypeError, ValueError):
                continue
            if v == 0.0:
                continue
            stats[k][1] += 1
            if (v > 0 and ad == "up") or (v < 0 and ad == "down"):
                stats[k][0] += 1
    out = {}
    for k, (h, n) in stats.items():
        hit = (h / n) if n else None
        out[k] = {"n": n, "hit": round(hit, 3) if hit is not None else None,
                  "mult": multiplier_for(n, hit)}
    return out


def general_runs(board: dict) -> list[dict]:
    return [r for r in board.get("runs", [])
            if r.get("topic", "general") == "general" and not r.get("sector")]


def sector_runs(board: dict, sector: str | None = None) -> list[dict]:
    rs = [r for r in board.get("runs", []) if r.get("sector")]
    if sector:
        rs = [r for r in rs if r.get("sector") == sector]
    return rs


def build_policy(board: dict, before_date: str | None = None) -> dict:
    """Full policy from the scoreboard: general skill + per-sector skill +
    a pooled all-sector skill used when one sector has too little data."""
    gen = factor_skill(general_runs(board), GENERAL_KEYS, before_date)
    pooled = factor_skill(sector_runs(board), SECTOR_KEYS, before_date)
    per_sector = {}
    for s in sorted({r["sector"] for r in sector_runs(board)}):
        own = factor_skill(sector_runs(board, s), SECTOR_KEYS, before_date)
        merged = {}
        for k in SECTOR_KEYS:
            # own history first; fall back to the pooled read when thin
            merged[k] = own[k] if own[k]["n"] >= MIN_N else dict(pooled[k], pooled=True)
        per_sector[s] = merged
    return {"general": gen, "sector_pooled": pooled, "sectors": per_sector}


def load_policy() -> dict:
    if not os.path.exists(POLICY_PATH):
        return {}
    try:
        with open(POLICY_PATH, encoding="utf-8") as fh:
            return json.load(fh)
    except (OSError, json.JSONDecodeError):
        return {}


def general_multipliers(policy: dict | None = None) -> dict[str, float]:
    pol = policy if policy is not None else load_policy()
    gen = (pol.get("general") or {}) if pol else {}
    return {k: float((gen.get(k) or {}).get("mult", 1.0)) for k in GENERAL_KEYS}


def sector_multipliers(sector: str, policy: dict | None = None) -> dict[str, float]:
    pol = policy if policy is not None else load_policy()
    sec = ((pol.get("sectors") or {}).get(sector)
           or pol.get("sector_pooled") or {}) if pol else {}
    return {k: float((sec.get(k) or {}).get("mult", 1.0)) for k in SECTOR_KEYS}


def update_policy_file(board: dict) -> dict:
    """Nightly: recompute from the whole scoreboard and append a ledger row
    describing what changed. Returns the saved policy."""
    prev = load_policy()
    pol = build_policy(board)
    now = datetime.now(ZoneInfo(config.TZ)).isoformat(timespec="seconds")
    changes = []
    for k in GENERAL_KEYS:
        old = float(((prev.get("general") or {}).get(k) or {}).get("mult", 1.0))
        new = pol["general"][k]["mult"]
        if old != new:
            changes.append(f"general.{k}: {old} -> {new} "
                           f"(n={pol['general'][k]['n']}, hit={pol['general'][k]['hit']})")
    for s, sk in pol["sectors"].items():
        for k in SECTOR_KEYS:
            old = float((((prev.get("sectors") or {}).get(s) or {}).get(k) or {}).get("mult", 1.0))
            new = sk[k]["mult"]
            if old != new:
                changes.append(f"{s}.{k}: {old} -> {new} (n={sk[k]['n']}, hit={sk[k]['hit']})")
    history = list(prev.get("history") or [])
    history.append({"at": now, "changes": changes or ["hold"]})
    pol.update({
        "version": int(prev.get("version", 0)) + 1,
        "updated": now,
        "rules": {"min_n": MIN_N, "mute_below": MUTE_BELOW,
                  "half_below": HALF_BELOW, "bonus_above": BONUS_ABOVE,
                  "bonus_mult": BONUS_MULT},
        "history": history[-60:],
    })
    os.makedirs(os.path.dirname(POLICY_PATH), exist_ok=True)
    tmp = POLICY_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(pol, fh, indent=2, ensure_ascii=False)
    os.replace(tmp, POLICY_PATH)
    return pol
