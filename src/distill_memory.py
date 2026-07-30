"""Monthly distillation (1st of month): rewrite 04_consolidated_memory.md
(<1,500 words) from scoreboard + active lessons + last month's reflections;
archive the previous memory; refresh monthly_summary.md; archive stale
candidates (>60 days, unpromoted).

CLI: python -m src.distill_memory
"""
from __future__ import annotations

import glob
import json
import os
import re
import shutil
from datetime import datetime
from zoneinfo import ZoneInfo

from . import compute_scores, config, deepseek_client, memory, scoreboard

STALE_DAYS = 60


def _read(path: str) -> str:
    try:
        with open(path, encoding="utf-8") as fh:
            return fh.read()
    except OSError:
        return ""


def main() -> None:
    today = datetime.now(ZoneInfo(config.TZ)).date().isoformat()
    board = scoreboard.load()
    runs = board.get("runs", [])

    # last month's reflection narratives
    reflects = sorted(glob.glob(os.path.join(config.DAILY_GENERAL,
                                             "*_reflect.md")))[-22:]
    refl_text = "\n\n".join(f"===== {os.path.basename(p)} =====\n{_read(p)}"
                            for p in reflects)

    with open(os.path.join(config.GROUNDING, "distill_prompt.md"),
              encoding="utf-8") as fh:
        prompt = fh.read()

    board_json = json.dumps(runs, ensure_ascii=False)
    user_msg = (
        f"TODAY: {today}\n\n"
        f"=== SCOREBOARD ({len(runs)} runs) ===\n{board_json}\n\n"
        f"=== ACCURACY ===\nlast10: {compute_scores.accuracy_summary(runs, 10)}"
        f"\nlast30: {compute_scores.accuracy_summary(runs, 30)}\n\n"
        f"=== ACTIVE LESSONS ===\n{memory.active_lessons()}\n\n"
        f"=== LAST MONTH REFLECTIONS ===\n{refl_text}\n\n"
        f"=== PREVIOUS CONSOLIDATED MEMORY ===\n{memory.consolidated_memory()}\n\n"
        "Rewrite the consolidated memory now (under 1,500 words).")

    new_memory = deepseek_client.chat(
        [{"role": "system", "content": prompt},
         {"role": "user", "content": user_msg}],
        model=config.MODEL_DISTILL, tools=False, max_tokens=4000)

    # archive previous memory
    if os.path.exists(config.CONSOLIDATED_MEMORY):
        os.makedirs(config.MEMORY_ARCHIVE, exist_ok=True)
        shutil.copy(config.CONSOLIDATED_MEMORY,
                    os.path.join(config.MEMORY_ARCHIVE,
                                 f"consolidated_{today}.md"))
    with open(config.CONSOLIDATED_MEMORY, "w", encoding="utf-8") as fh:
        fh.write(f"# Consolidated Memory (rewritten {today})\n\n{new_memory}\n")

    # monthly summary rollup
    acc10 = compute_scores.accuracy_summary(runs, 10)
    acc30 = compute_scores.accuracy_summary(runs, 30)
    with open(config.MONTHLY_SUMMARY, "w", encoding="utf-8") as fh:
        fh.write(f"# Monthly Summary — {today}\n\n")
        fh.write(f"- total runs: {len(runs)}\n")
        fh.write(f"- last-10 accuracy: {acc10}\n- last-30 accuracy: {acc30}\n")
        fh.write(f"- active lessons: "
                 f"{len(glob.glob(os.path.join(config.LESSONS_ACTIVE, '*.md')))}\n")

    # archive stale, unpromoted candidates
    os.makedirs(config.LESSONS_ARCHIVE, exist_ok=True)
    archived = 0
    for p in glob.glob(os.path.join(config.LESSONS_CANDIDATE, "*.md")):
        m = re.match(r"(\d{4}-\d{2}-\d{2})_lesson\.md", os.path.basename(p))
        if m and (datetime.now(ZoneInfo(config.TZ)).date() -
                  datetime.strptime(m.group(1), "%Y-%m-%d").date()).days > STALE_DAYS:
            shutil.move(p, os.path.join(config.LESSONS_ARCHIVE,
                                        os.path.basename(p)))
            archived += 1
    print(f"[distill] memory rewritten; {archived} stale candidates archived")


if __name__ == "__main__":
    main()
