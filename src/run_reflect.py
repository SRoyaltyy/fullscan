"""Stage REFLECT (after outcome): diagnostic engine. Reads predict + outcome +
scoreboard + active lessons; writes a Candidate Lesson to
02_lessons/candidate/<date>_lesson.md and links it in the scoreboard.

CLI: python -m src.run_reflect [--date YYYY-MM-DD]
"""
from __future__ import annotations

import argparse
import glob
import os
import re
from datetime import datetime
from zoneinfo import ZoneInfo

from . import config, deepseek_client, memory, scoreboard, snapshot


def _read(path: str) -> str:
    try:
        with open(path, encoding="utf-8") as fh:
            return fh.read()
    except OSError:
        return "(missing)"


def _parse_lesson_block(text: str) -> dict:
    m = re.search(r"LESSON_BEGIN(.*?)LESSON_END", text, re.S)
    block = m.group(1) if m else ""
    out = {}
    for line in block.splitlines():
        if ":" in line:
            k, v = line.split(":", 1)
            out[k.strip()] = v.strip()
    return out


def _candidate_lesson_triggers(limit: int = 12) -> str:
    """Trigger patterns + categories of recent candidate lessons — injected so
    CHECK 1 (lesson match) and CHECK 2 (backward test) have real material."""
    files = sorted(glob.glob(os.path.join(config.LESSONS_CANDIDATE, "*.md")))
    rows = []
    for p in files[-limit:]:
        head = _read(p)[:800]
        trig = re.search(r'trigger_pattern:\s*"(.*?)"', head)
        cat = re.search(r'error_category:\s*"(.*?)"', head)
        date = re.search(r'date:\s*"(.*?)"', head)
        rows.append(f"- {date.group(1) if date else os.path.basename(p)} "
                    f"[{cat.group(1) if cat else '?'}]: "
                    f"{trig.group(1) if trig else '(no trigger recorded)'}")
    return "\n".join(rows) or "(no candidate lessons yet)"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=None)
    args = ap.parse_args()
    date_str = args.date or datetime.now(ZoneInfo(config.TZ)).date().isoformat()

    if not config.DEEPSEEK_API_KEY:
        raise SystemExit("DEEPSEEK_API_KEY not set")

    board = scoreboard.load()
    entry = scoreboard.get_or_create(board, date_str, config.TOPIC)
    if entry.get("actual_pct_change") is None:
        raise SystemExit(f"[reflect] {date_str}: no graded outcome yet — run "
                         "outcome first")

    predict_md = _read(os.path.join(config.DAILY_GENERAL,
                                    f"{date_str}_predict.md"))
    outcome_md = _read(os.path.join(config.DAILY_GENERAL,
                                    f"{date_str}_outcome.md"))
    with open(os.path.join(config.GROUNDING, "reflect_prompt.md"),
              encoding="utf-8") as fh:
        prompt = fh.read()

    user_msg = (
        f"TODAY: {date_str}\n\n"
        f"=== PREMARKET PREDICTION ===\n{predict_md}\n\n"
        f"=== POST-MARKET OUTCOME ===\n{outcome_md}\n\n"
        f"=== SCOREBOARD ENTRY (pipeline-graded) ===\n"
        f"direction_hit: {entry['direction_hit']} | magnitude_hit: "
        f"{entry['magnitude_hit']} | predicted {entry['predicted_direction']}/"
        f"{entry['predicted_magnitude_band']} vs actual "
        f"{entry['actual_pct_change']}% ({entry['actual_direction']}/"
        f"{entry['actual_magnitude_band']}) | divergence_flagged: "
        f"{entry['divergence_flagged']}\n\n"
        f"=== RECENT SCOREBOARD HISTORY (for CHECK 2 backward test) ===\n"
        f"{memory.scoreboard_summary()}\n\n"
        f"=== RECENT CANDIDATE LESSON TRIGGERS (for CHECK 1 lesson match) ===\n"
        f"{_candidate_lesson_triggers()}\n\n"
        f"=== STANDING ACTIVE LESSONS ===\n{memory.active_lessons()}\n\n"
        "Execute the diagnostic now. Answer all five mandatory checks "
        "explicitly, in order.")

    text = deepseek_client.chat(
        [{"role": "system", "content": prompt},
         {"role": "user", "content": user_msg}],
        model=config.MODEL_REFLECT, tools=False, max_tokens=12000,
        transcript_path=os.path.join("01_daily/_transcripts",
                                     f"{date_str}_reflect.json"),
        trace_path=os.path.join(config.DAILY_GENERAL,
                                f"{date_str}_reflect_trace.md"),
        stage_label=f"REFLECT {date_str}")

    lb = _parse_lesson_block(text)

    # candidate lesson file (yaml frontmatter + human snapshot per spec)
    os.makedirs(config.LESSONS_CANDIDATE, exist_ok=True)
    lesson_path = os.path.join(config.LESSONS_CANDIDATE,
                               f"{date_str}_lesson.md")
    with open(lesson_path, "w", encoding="utf-8") as fh:
        fh.write("---\n")
        fh.write(f"trigger_pattern: \"{lb.get('TRIGGER_PATTERN', '')}\"\n")
        fh.write(f"current_behavior: \"{lb.get('CURRENT_BEHAVIOR', '')}\"\n")
        fh.write(f"corrected_behavior: \"{lb.get('CORRECTED_BEHAVIOR', '')}\"\n")
        fh.write(f"evidence_cited: \"{lb.get('EVIDENCE', '')}\"\n")
        fh.write(f"error_category: \"{lb.get('ERROR_CATEGORY', 'NONE')}\"\n")
        fh.write(f"falsifier: \"{lb.get('FALSIFIER', '')}\"\n")
        fh.write(f"backward_check: \"{lb.get('BACKWARD_CHECK', '')}\"\n")
        fh.write(f"conflict_check: \"{lb.get('CONFLICT_CHECK', '')}\"\n")
        fh.write(f"lesson_match_check: \"{lb.get('LESSON_MATCH_CHECK', '')}\"\n")
        fh.write(f"date: \"{date_str}\"\n")
        fh.write("status: \"candidate\"\n---\n\n")
        fh.write(f"# Reflection — {date_str}\n\n")
        fh.write(snapshot.reflect_snapshot(lb, entry))
        fh.write(text + "\n")

    entry["reflection_lesson_ref"] = lesson_path
    dv = lb.get("DIVERGENCE_VERDICT")
    if dv and dv != "none_flagged":
        entry["divergence_verdict"] = dv
    scoreboard.save(board)
    print(f"[reflect] {date_str}: category={lb.get('ERROR_CATEGORY')} -> "
          f"{lesson_path}")


if __name__ == "__main__":
    main()
