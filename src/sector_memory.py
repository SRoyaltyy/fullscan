"""Memory context scoped to one sector topic (parallel to memory.py)."""
from __future__ import annotations

import glob
import os
import re

from . import compute_sector_scores, config, scoreboard
from .sector_taxonomy import SECTOR_ETFS, amp_damp_table, taxonomy_list


def topic_for(sector: str) -> str:
    return f"sector:{sector}"


def _read(path: str) -> str:
    try:
        with open(path, encoding="utf-8") as fh:
            return fh.read()
    except OSError:
        return ""


def scoreboard_summary(sector: str) -> str:
    board = scoreboard.load()
    topic = topic_for(sector)
    runs = [r for r in board.get("runs", []) if r.get("topic") == topic]
    acc10 = compute_sector_scores.accuracy_summary(runs, 10) if hasattr(
        compute_sector_scores, "accuracy_summary") else _acc(runs, 10)
    acc30 = _acc(runs, 30)
    lines = [
        f"Sector topic: {topic}",
        f"Rolling accuracy last 10 graded: dir={acc10['direction_acc']} "
        f"mag={acc10['magnitude_acc']} (n={acc10['n']})",
        f"Rolling accuracy last 30 graded: dir={acc30['direction_acc']} "
        f"mag={acc30['magnitude_acc']} (n={acc30['n']})",
        "Last runs:",
    ]
    for r in runs[-10:]:
        pct = r.get("actual_pct_change")
        lines.append(
            f"- {r['date']}: predicted {r.get('predicted_direction')}/"
            f"{r.get('predicted_magnitude_band')}, actual "
            f"{pct if pct is not None else 'pending'}% "
            f"({'dir HIT' if r.get('direction_hit') else 'dir MISS' if r.get('direction_hit') is False else 'ungraded'})")
    if len(runs) == 0:
        lines.append("(no prior runs — establishing baseline)")
    return "\n".join(lines)


def _acc(runs: list, n: int) -> dict:
    graded = [r for r in runs if r.get("actual_pct_change") is not None][-n:]
    if not graded:
        return {"n": 0, "direction_acc": None, "magnitude_acc": None}
    d = sum(1 for r in graded if r.get("direction_hit"))
    m = sum(1 for r in graded if r.get("magnitude_hit"))
    return {"n": len(graded), "direction_acc": round(d / len(graded), 3),
            "magnitude_acc": round(m / len(graded), 3)}


def recent_sector_logs(sector: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "_", sector.lower()).strip("_")
    root = config.DAILY_SECTORS
    dates = sorted(
        d for d in os.listdir(root)
        if os.path.isdir(os.path.join(root, d)) and re.match(r"\d{4}-\d{2}-\d{2}$", d)
    ) if os.path.isdir(root) else []
    dates = dates[-config.MEMORY_WINDOW_DAYS:]
    parts = []
    for d in dates:
        pp = os.path.join(root, d, f"{slug}_predict.md")
        rp = os.path.join(root, d, f"{slug}_reflect.md")
        if os.path.exists(pp):
            parts.append(f"===== {d} PREDICT =====\n{_read(pp)[:6000]}")
        if os.path.exists(rp):
            parts.append(f"===== {d} REFLECT =====\n{_read(rp)[:4000]}")
    return "\n\n".join(parts) or "(no prior sector logs — first run for this sector)"


def active_lessons_block() -> str:
    parts = []
    for p in sorted(glob.glob(os.path.join(config.LESSONS_ACTIVE, "*.md"))):
        parts.append(f"### {os.path.basename(p)}\n{_read(p).strip()}")
    return "\n\n".join(parts) or "(no standing lessons yet)"


def prediction_context(sector: str) -> str:
    labs = taxonomy_list(sector)
    checklist = "\n".join(f"- {x}" for x in labs)
    return (
        "=== MEMORY CONTEXT (THIS SECTOR ONLY) ===\n\n"
        f"[SCOREBOARD]\n{scoreboard_summary(sector)}\n\n"
        f"[STANDING ACTIVE LESSONS]\n{active_lessons_block()}\n\n"
        f"[LAST {config.MEMORY_WINDOW_DAYS} SECTOR LOGS]\n{recent_sector_logs(sector)}\n\n"
        f"=== SECTOR FACTOR TAXONOMY (exact labels) ===\n{checklist}\n\n"
        f"=== AMP/DAMP ===\n{amp_damp_table(sector)}\n\n"
        f"ETF proxy: {SECTOR_ETFS.get(sector)}\n"
    )
