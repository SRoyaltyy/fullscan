"""Locked rules for concentration_cap_v2. No score is imported here."""
from __future__ import annotations

import hashlib
from pathlib import Path

from research.concentration_cap_v1.protocol import TRIES as V1_TRIES
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
    candidates as v4_candidates,
)

ROOT = Path(__file__).resolve().parents[2]
STUDY = "concentration_cap_v2"
HERE = ROOT / "research" / STUDY
PREREG = HERE / "PREREG.md"
FREEZE = HERE / "freeze" / "FREEZE.json"
RETURNS = HERE / "returns"
MARKER = "<!-- BEGIN COVERED -->\n"

PRIOR = 21_796
SHARE_MAX = 0.20
CREATION = "2026-09-26"
CAP_CASH = "sit"
FORWARD_FIRST = "2026-09-28"

BASE_IDS = (
    "union_hot_n4_h1__w0",
    "union_hot_n4_holdup__w0",
    "union_hot_n4_h1_nonews__w0",
    "union_hot_score_h3__w1",
    "union_hot_score_h3__w0",
    "union_ret_5_h3__w0",
    "union_ret_5_h3__w1",
)
WIDTHS = (4, 6, 8, 10)
CAPS = (0.20, 0.25, None)


def cap_label(cap: float | None) -> str:
    if cap is None:
        return "none"
    return str(int(round(float(cap) * 100)))


def candidate_id(base_id: str, width: int, cap: float | None) -> str:
    return f"{base_id}__n{width}__c{cap_label(cap)}"


def candidates() -> tuple[dict, ...]:
    bases = {row["id"]: row for row in v4_candidates()}
    out = []
    for base_id in BASE_IDS:
        base = dict(bases[base_id])
        for width in WIDTHS:
            for cap in CAPS:
                row = dict(base)
                row["base_id"] = base_id
                row["cap_cash"] = CAP_CASH
                row["exit_when"] = dict(base.get("exit_when") or {})
                row["forbid"] = dict(base.get("forbid") or {})
                row["require"] = dict(base.get("require") or {})
                row["source_id"] = base_id
                row["source_name"] = base["name"]
                row["top_n"] = int(width)
                row["weight_cap"] = cap
                row["id"] = candidate_id(base_id, width, cap)
                row["name"] = row["id"]
                out.append(row)
    return tuple(out)


N_CANDIDATES = len(BASE_IDS) * len(WIDTHS) * len(CAPS)
TRIES = N_CANDIDATES
LUCK_N = TRIES + V1_TRIES + PRIOR


def covered_bytes(text: str) -> bytes:
    idx = text.find(MARKER)
    if idx < 0:
        raise RuntimeError("covered marker missing")
    return text[idx + len(MARKER):].encode("utf-8")


def fingerprint_sha256(text: str) -> str:
    return hashlib.sha256(covered_bytes(text)).hexdigest()


def file_sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()
