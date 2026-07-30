"""Memory-tier context assembly (Section IV of the spec).

Every prediction run reads EXACTLY:
  00_grounding/master_rubric.md          (handled by caller)
  04_consolidated_memory.md              (full)
  02_lessons/active/*                    (full)
  03_scoreboard/scoreboard.json          (summarized: last 10 runs + rolling acc)
  last 10 trading days of 01_daily/general predict + reflect files
Older logs are never read directly; their insight persists via
04_consolidated_memory.md only.
"""
from __future__ import annotations

import glob
import os
import re

from . import compute_scores, config, scoreboard


def _read(path: str) -> str:
    try:
        with open(path, encoding="utf-8") as fh:
            return fh.read()
    except OSError:
        return ""


def consolidated_memory() -> str:
    return _read(config.CONSOLIDATED_MEMORY).strip() or "(empty — first month)"


def active_lessons() -> str:
    parts = []
    for p in sorted(glob.glob(os.path.join(config.LESSONS_ACTIVE, "*.md"))):
        parts.append(f"### {os.path.basename(p)}\n{_read(p).strip()}")
    return "\n\n".join(parts) or "(no standing lessons yet)"


def scoreboard_summary() -> str:
    board = scoreboard.load()
    runs = board.get("runs", [])
    acc10 = compute_scores.accuracy_summary(runs, 10)
    acc30 = compute_scores.accuracy_summary(runs, 30)
    lines = [
        f"Rolling accuracy last 10 graded runs: {acc10['direction_acc']} direction / "
        f"{acc10['magnitude_acc']} magnitude (n={acc10['n']})",
        f"Rolling accuracy last 30 graded runs: {acc30['direction_acc']} direction / "
        f"{acc30['magnitude_acc']} magnitude (n={acc30['n']})",
        "Last 10 runs:",
    ]
    for r in runs[-10:]:
        pct = r.get("actual_pct_change")
        lines.append(
            f"- {r['date']}: predicted {r.get('predicted_direction')}/"
            f"{r.get('predicted_magnitude_band')}, actual "
            f"{pct if pct is not None else 'pending'}% "
            f"({'dir HIT' if r.get('direction_hit') else 'dir MISS' if r.get('direction_hit') is False else 'ungraded'})")
    return "\n".join(lines)


def recent_daily_logs() -> str:
    preds = sorted(glob.glob(os.path.join(config.DAILY_GENERAL,
                                          "*_predict.md")))
    dates = [re.sub(r"_predict\.md$", "", os.path.basename(p)) for p in preds]
    dates = dates[-config.MEMORY_WINDOW_DAYS:]
    parts = []
    for d in dates:
        pp = os.path.join(config.DAILY_GENERAL, f"{d}_predict.md")
        rp = os.path.join(config.DAILY_GENERAL, f"{d}_reflect.md")
        parts.append(f"===== {d} PREDICT =====\n{_read(pp)}")
        if os.path.exists(rp):
            parts.append(f"===== {d} REFLECT =====\n{_read(rp)}")
    return "\n\n".join(parts) or "(no prior daily logs — this is the first run)"


def prediction_context() -> str:
    """Full memory block injected into the premarket prompt."""
    return (
        "=== MEMORY CONTEXT ===\n\n"
        f"[SCOREBOARD]\n{scoreboard_summary()}\n\n"
        f"[CONSOLIDATED MEMORY]\n{consolidated_memory()}\n\n"
        f"[STANDING ACTIVE LESSONS]\n{active_lessons()}\n\n"
        f"[LAST {config.MEMORY_WINDOW_DAYS} TRADING DAYS]\n{recent_daily_logs()}"
    )
