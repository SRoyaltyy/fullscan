"""Memory-tier context assembly.

Predict injects:
  - scoreboard summary
  - consolidated memory
  - standing rules from 02_lessons/active/*
  - mutable_policy.md (learn_cycle output — the living prompt policy)
  - recent daily logs
"""
from __future__ import annotations

import glob
import os
from pathlib import Path

from . import compute_scores, config, lesson_schema, scoreboard

MUTABLE_POLICY = Path(config.GROUNDING) / "mutable_policy.md"


def _read(path: str) -> str:
    try:
        with open(path, encoding="utf-8") as fh:
            return fh.read()
    except OSError:
        return ""


def consolidated_memory() -> str:
    return _read(config.CONSOLIDATED_MEMORY).strip() or "(empty — first month)"


def active_lesson_count() -> int:
    n = 0
    for p in glob.glob(os.path.join(config.LESSONS_ACTIVE, "*.md")):
        if os.path.basename(p).startswith("."):
            continue
        if _read(p).strip():
            n += 1
    return n


def active_lessons() -> str:
    parts = []
    for p in sorted(glob.glob(os.path.join(config.LESSONS_ACTIVE, "*.md"))):
        if os.path.basename(p).startswith("."):
            continue
        text = _read(p).strip()
        if text:
            parts.append(f"### {os.path.basename(p)}\n{text}")
    return lesson_schema.standing_rules_block(parts)


def mutable_policy() -> str:
    """Living policy rewritten by learn_cycle — safe prompt edits only."""
    text = _read(str(MUTABLE_POLICY)).strip()
    if not text:
        return (
            "=== MUTABLE POLICY ===\n"
            "(empty — run python -m src.learn_cycle after outcomes)\n"
        )
    return f"=== MUTABLE POLICY (follow these adjustments; core SCORES format unchanged) ===\n{text}\n"


def recent_misses(n: int = 8) -> str:
    """Explicit 'you were wrong' list so the LLM cannot ignore its own tape."""
    board = scoreboard.load()
    rows = []
    for r in reversed(board.get("runs", [])):
        if r.get("topic", "general") != "general":
            continue
        if r.get("direction_hit") is None or r.get("ops_fail"):
            continue
        rows.append(r)
        if len(rows) >= n:
            break
    if not rows:
        return "(no graded general runs yet)"
    lines = ["HARD GRADE TAPE — treat these as constraints, not flavour:"]
    for r in rows:
        hit = "HIT" if r.get("direction_hit") else "MISS"
        lines.append(
            f"- {r.get('date')} predicted {r.get('predicted_direction')} "
            f"(score {r.get('total_score')}) vs actual {r.get('actual_direction') or r.get('actual_move')} "
            f"→ {hit}"
        )
    misses = [r for r in rows if not r.get("direction_hit")]
    if misses:
        last = misses[0]
        lines.append(
            "CONSTRAINT: the most recent miss was "
            f"{last.get('date')} {last.get('predicted_direction')} vs "
            f"{last.get('actual_direction')}. Do NOT let a 1-day FRED tick "
            "override a 1-week yield/regime signal the way that miss did."
        )
    return "\n".join(lines)


def scoreboard_summary() -> str:
    board = scoreboard.load()
    runs = board.get("runs", [])
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
    ]
    return "\n".join(lines)


def recent_daily_logs() -> str:
    files = sorted(glob.glob(os.path.join(config.DAILY_GENERAL, "*_predict.md")))
    dates = []
    for f in files:
        base = os.path.basename(f)
        if base.endswith("_predict.md"):
            dates.append(base.replace("_predict.md", ""))
    dates = dates[-config.MEMORY_WINDOW_DAYS:]
    parts = []
    for d in dates:
        pp = os.path.join(config.DAILY_GENERAL, f"{d}_predict.md")
        rp = os.path.join(config.DAILY_GENERAL, f"{d}_outcome.md")
        parts.append(f"===== {d} PREDICT =====\n{_read(pp)[:4000]}")
        if os.path.exists(rp):
            parts.append(f"===== {d} OUTCOME =====\n{_read(rp)[:2000]}")
    return "\n\n".join(parts) or "(no prior daily logs — this is the first run)"


def prediction_context() -> str:
    """Full memory block injected into the premarket prompt."""
    return (
        "=== MEMORY CONTEXT ===\n\n"
        f"[SCOREBOARD]\n{scoreboard_summary()}\n\n"
        f"[RECENT GRADED MISSES — OBEY THESE]\n{recent_misses()}\n\n"
        f"[CONSOLIDATED MEMORY]\n{consolidated_memory()}\n\n"
        f"{active_lessons()}\n\n"
        f"{mutable_policy()}\n\n"
        f"[LAST {config.MEMORY_WINDOW_DAYS} TRADING DAYS]\n{recent_daily_logs()}\n"
    )
