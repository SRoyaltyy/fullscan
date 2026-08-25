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


def _richness(entry: dict) -> int:
    """How complete is this run? Outcome fields beat predict-only."""
    keys = (
        "predicted_direction", "actual_pct_change", "actual_close",
        "components", "horizon_calls", "reflection_lesson_ref",
        "per_factor_breakdown", "divergence_verdict",
    )
    n = 0
    for k in keys:
        v = entry.get(k)
        if v not in (None, {}, [], "", False):
            n += 1
    return n


def merge_boards(primary: dict, extra: dict) -> dict:
    """Union runs by (date, topic).

    Used when two jobs (daily_pipeline + sector_daily) both rewrote
    scoreboard.json from a stale base and git cannot auto-merge the JSON.
    Field-wise: non-empty values from `extra` overlay `primary`; empty
    extra values do not clobber a filled primary field (so a predict-only
    write cannot erase an already-graded outcome).
    """
    def key(r: dict) -> tuple:
        return (r.get("date"), r.get("topic"))

    idx: dict[tuple, dict] = {}
    for r in (primary.get("runs") or []):
        idx[key(r)] = dict(r)
    for r in (extra.get("runs") or []):
        k = key(r)
        if k not in idx:
            idx[k] = dict(r)
            continue
        old = idx[k]
        merged = dict(old)
        for field, val in r.items():
            if val not in (None, {}, [], ""):
                merged[field] = val
        # If extra is strictly poorer (e.g. a late 0/flat stub), keep old.
        if _richness(merged) < _richness(old):
            merged = old
        idx[k] = merged
    out = dict(primary)
    out["runs"] = list(idx.values())
    return out


def merge_ours_file(ours_path: str) -> None:
    """Load current scoreboard (typically origin/main's copy) and union
    the entries from `ours_path` (the job that just ran)."""
    with open(ours_path, encoding="utf-8") as fh:
        extra = json.load(fh)
    board = load()
    save(merge_boards(board, extra))
    print(f"[scoreboard] merged {ours_path} -> {config.SCOREBOARD_JSON} "
          f"({len(load().get('runs') or [])} runs)")


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--merge-ours", default=None,
                    help="Path to OUR scoreboard.json to union into the "
                         "copy currently on disk (theirs/main)")
    args = ap.parse_args()
    if args.merge_ours:
        merge_ours_file(args.merge_ours)
    else:
        board = load()
        print(f"{config.SCOREBOARD_JSON}: {len(board.get('runs') or [])} runs")
