"""Locked rules for concentration_cap_v1. No score is imported here."""
from __future__ import annotations

import hashlib
from pathlib import Path

from research.factor_mine_recipe_search_v4.protocol import (
    CAPITAL,
    CLEAN_BLOB,
    CLEAN_COMMIT,
    CLEAN_PATH,
    CLEAN_SHA256,
    FEES_SHA256,
    FORWARD,
    HOLDUP_S,
    HOLDUP_SESS,
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
STUDY = "concentration_cap_v1"
HERE = ROOT / "research" / STUDY
PREREG = HERE / "PREREG.md"
FREEZE = HERE / "freeze" / "FREEZE.json"
RETURNS = HERE / "returns"
MARKER = "<!-- BEGIN COVERED -->\n"

PRIOR_V4 = 21_536
PRIOR_SCREEN = 260
CREATION = "2026-09-26"
CAP_CASH = "sit"
FORWARD_FIRST = "2026-09-28"

BASE_IDS = (
    "union_hot_n4_h1__w0",
    "union_hot_n4_holdup__w0",
    "union_hot_n4_h1_nonews__w0",
    "union_hot_score_h3__w1",
    "union_hot_score_h3__w0",
)
WIDTHS = (4, 6, 8)
CAPS = (0.20, 0.25, None)

_GATE = {
    "union_hot_n4_h1__w0": {
        "forbid": {"alarm": True}, "hold": 1, "s_boost": "none", "weather": False,
    },
    "union_hot_n4_holdup__w0": {
        "forbid": {"alarm": True}, "hold": 1, "s_boost": "holdup", "weather": False,
    },
    "union_hot_n4_h1_nonews__w0": {
        "forbid": {"alarm": True, "news": "bad"}, "hold": 1, "s_boost": "none", "weather": False,
    },
    "union_hot_score_h3__w1": {
        "forbid": {"alarm": True}, "hold": 3, "s_boost": "none", "weather": True,
    },
    "union_hot_score_h3__w0": {
        "forbid": {"alarm": True}, "hold": 3, "s_boost": "none", "weather": False,
    },
}


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
LUCK_N = TRIES + PRIOR_V4 + PRIOR_SCREEN


def covered_bytes(text: str) -> bytes:
    idx = text.find(MARKER)
    if idx < 0:
        raise RuntimeError("covered marker missing")
    return text[idx + len(MARKER):].encode("utf-8")


def fingerprint_sha256(text: str) -> str:
    return hashlib.sha256(covered_bytes(text)).hexdigest()


def file_sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()
