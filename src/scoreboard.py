"""Scoreboard persistence. scoreboard.json = {"runs": [entry, ...]} keyed by
(date, topic). All writes go through this module."""
from __future__ import annotations

import json
import os

from . import config


def load() -> dict:
    if not os.path.exists(config.SCOREBOARD_JSON):
        return {"runs": []}
    with open(config.SCOREBOARD_JSON, encoding="utf-8") as fh:
        return json.load(fh)


def save(board: dict) -> None:
    os.makedirs(os.path.dirname(config.SCOREBOARD_JSON), exist_ok=True)
    tmp = config.SCOREBOARD_JSON + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(board, fh, indent=2, ensure_ascii=False)
    os.replace(tmp, config.SCOREBOARD_JSON)


def get_or_create(board: dict, date_str: str, topic: str) -> dict:
    for r in board["runs"]:
        if r["date"] == date_str and r["topic"] == topic:
            return r
    entry = {
        "date": date_str, "topic": topic,
        "predicted_direction": None, "predicted_magnitude_band": None,
        "confidence_score": None, "total_score": None, "multiplier": None,
        "components": {}, "leading_sum": None,
        "actual_open": None, "actual_close": None, "actual_pct_change": None,
        "actual_direction": None, "actual_magnitude_band": None,
        "direction_hit": None, "magnitude_hit": None,
        "divergence_flagged": False, "divergence_verdict": None,
        "per_factor_breakdown": [], "reflection_lesson_ref": None,
        "sources_used": [],
    }
    board["runs"].append(entry)
    return entry
