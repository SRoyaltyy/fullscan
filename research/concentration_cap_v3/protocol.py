"""Locked rules for concentration_cap_v3. No score is imported here."""
from __future__ import annotations

import hashlib
from pathlib import Path

from research.concentration_cap_v1.protocol import TRIES as V1_TRIES
from research.concentration_cap_v2.protocol import (
    BASE_IDS,
    CAPS,
    CAP_CASH,
    TRIES as V2_TRIES,
    WIDTHS,
    candidates as v2_candidates,
)
from research.factor_mine_recipe_search_v4.protocol import (
    CAPITAL,
    CLEAN_SHA256,
    FEES_SHA256,
    FORWARD,
    INPUTS,
    MIN_TRADES,
    RANDOM_DRAWS,
    RANDOM_N,
    RANDOM_SEED,
    REJECT_JOINT,
    SESSIONS,
    SPLIT_SHA256,
    STARTS,
    TUNE,
)

ROOT = Path(__file__).resolve().parents[2]
STUDY = "concentration_cap_v3"
HERE = ROOT / "research" / STUDY
PREREG = HERE / "PREREG.md"
FREEZE = HERE / "freeze" / "FREEZE.json"
RETURNS = HERE / "returns"
MARKER = "<!-- BEGIN COVERED -->\n"

PRIOR = 21_796
DEPENDENCE_MAX = 0.20
KEEP_FRAC = 0.8
CREATION = "2026-09-26"
FORWARD_FIRST = "2026-09-28"


def candidates() -> tuple[dict, ...]:
    return v2_candidates()


N_CANDIDATES = len(BASE_IDS) * len(WIDTHS) * len(CAPS)
TRIES = N_CANDIDATES
LUCK_N = TRIES + V2_TRIES + V1_TRIES + PRIOR


def dependence(total: float | None, dropped: float | None) -> float | None:
    """1 - R_-k/R. Blank when R is missing or zero."""
    if total is None or dropped is None or float(total) == 0.0:
        return None
    return 1.0 - (float(dropped) / float(total))


def share_line_passes(total: float | None, dropped: float | None) -> bool:
    """R > 0 and R_-1 >= 0.8 * R. R <= 0 fails."""
    if total is None or dropped is None:
        return False
    r = float(total)
    return r > 0.0 and float(dropped) >= KEEP_FRAC * r


def top3_passes(dropped3: float | None) -> bool:
    return dropped3 is not None and float(dropped3) > 0.0


def covered_bytes(text: str) -> bytes:
    idx = text.find(MARKER)
    if idx < 0:
        raise RuntimeError("covered marker missing")
    return text[idx + len(MARKER):].encode("utf-8")


def fingerprint_sha256(text: str) -> str:
    return hashlib.sha256(covered_bytes(text)).hexdigest()


def file_sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()
