"""Memory-tier context assembly (Section IV of the spec).

Every prediction run reads EXACTLY:
  00_grounding/master_rubric.md          (handled by caller)
  04_consolidated_memory.md              (full)
  02_lessons/active/*                    (full — standing rules)
  03_scoreboard/scoreboard.json          (summarized: last 10 runs + rolling acc)
  last 10 trading days of 01_daily/general predict + reflect files
"""
from __future__ import annotations

import glob
import os
import re

from . import compute_scores, config, lesson_schema, scoreboard


def _read(path: str) -> str:
    try:
        with open(path, encoding="utf-8") as fh:
            return fh.read()
    except OSError:
        return ""


def consolidated_memory() -> str:
    return _read(config.CONSOLIDATED_MEMORY).strip() or "(empty — first month)"


def active_lessons() -> str:
    """Full text of every active standing rule (for inject + reflect)."""
    parts = []
    for p in sorted(glob.glob(os.path.join(config.LESSONS_ACTIVE, "*.md"))):
        if os.path.basename(p).startswith("."):
            continue
        text = _read(p).strip()
        if text:
            parts.append(f"### {os.path.basename(p)}\n{text}")
    return lesson_schema.standing_rules_block(parts)


def active_lesson_count() -> int:
    n = 0
    for p in glob.glob(os.path.join(config.LESSONS_ACTIVE, "*.md")):
        if os.path.basename(p).startswith("."):
            continue
        if _read(p).strip():
            n += 1
    return n


def scoreboard_summary() -> str:
    board = scoreboard.load()
    runs = board.get("runs", [])
    # Prefer graded runs that had a real prediction
    graded = [
        r for r in runs
        if r.get("topic", "general") == "general"
        and r.get("predicted_direction") is not None
        and r.get("direction_hit") is not None
        and not r.get("ops_fail")
    ]
    acc10 = compute_scores.accuracy_summary(graded, 10)
    acc30 = compute_scores.accuracy_summary(graded, 30)
    lines = [
        f"Rolling accuracy last 10 graded runs (ex-OPS): {acc10['direction_acc']} direction / "
        f"{acc10['magnitude_acc']} magnitude (n={acc10['n']})",
        f"Rolling accuracy last 30 graded runs (ex-OPS): {acc30['direction_acc']} direction / "
        f"{acc30['magnitude_acc']} magnitude (n={acc30['n']})",
        f"Active standing lessons injected: {active_lesson_count()}",
        "Last 10 general runs:",
    ]
    general = [r for r in runs if r.get("topic", "general") == "general"][-10:]
    for r in general:
        pct = r.get("actual_pct_change")
        if r.get("ops_fail") or r.get("predicted_direction") is None:
            status = "OPS/ungraded"
        elif r.get("direction_hit") is True:
            status = "dir HIT"
        elif r.get("direction_hit") is False:
            status = "dir MISS"
        else:
            status = "ungraded"
        lines.append(
            f"- {r['date']}: predicted {r.get('predicted_direction')}/"
            f"{r.get('predicted_magnitude_band')}, actual "
            f"{pct if pct is not None else 'pending'}% ({status})"
        )
    return "\n".join(lines)


def recent_daily_logs() -> str:
    preds = sorted(glob.glob(os.path.join(config.DAILY_GENERAL,
                                          "*_predict.md")))
    dates = [re.sub(r"_predict\.md$", "", os.path.basename(p)) for p in preds]
    dates = dates[-config.MEMORY_WINDOW_DAYS:]
    parts = []
    for d in dates:
        pp = os.path.join(config.DAILY_GENERAL, f"{d}_predict.md")
        rp = os.path.join(config.DAILY_GENERAL, f"{d}_outcome.md")
        # prefer outcome; reflect file naming varies
        body_p = _read(pp)[:2500]
        body_o = _read(rp)[:1500]
        parts.append(f"### {d} predict (excerpt)\n{body_p}\n")
        if body_o:
            parts.append(f"### {d} outcome (excerpt)\n{body_o}\n")
    return "\n".join(parts) or "(no recent daily logs)"


def build_memory_block() -> str:
    """Full memory block injected into the premarket prompt."""
    return (
        f"[CONSOLIDATED MEMORY]\n{consolidated_memory()}\n\n"
        f"{active_lessons()}\n\n"
        f"[SCOREBOARD SUMMARY]\n{scoreboard_summary()}\n\n"
        f"[RECENT DAILY LOGS]\n{recent_daily_logs()}\n"
    )
