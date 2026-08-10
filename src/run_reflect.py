"""Stage REFLECT: diagnostic engine → schema-gated candidate lesson.

CLI: python -m src.run_reflect [--date YYYY-MM-DD]
"""
from __future__ import annotations

import argparse
import glob
import os
import re
from datetime import datetime
from zoneinfo import ZoneInfo

from . import config, deepseek_client, lesson_schema, memory, scoreboard, snapshot


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
    files = sorted(glob.glob(os.path.join(config.LESSONS_CANDIDATE, "*.md")))
    rows = []
    for p in files[-limit:]:
        head = _read(p)[:900]
        trig = re.search(r'trigger_pattern:\s*"(.*?)"', head)
        cat = re.search(r'error_category:\s*"(.*?)"', head)
        date = re.search(r'date:\s*"(.*?)"', head)
        ok = re.search(r'schema_ok:\s*"(.*?)"', head)
        rows.append(
            f"- {date.group(1) if date else os.path.basename(p)} "
            f"[{cat.group(1) if cat else '?'}] schema_ok={ok.group(1) if ok else '?'} — "
            f"{trig.group(1) if trig else '(no trigger)'}"
        )
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
    if entry.get("actual_pct_change") is None and not entry.get("ops_fail"):
        # allow reflect on ops_fail rows that still have actuals from a bad grade
        if entry.get("direction_hit") is None:
            raise SystemExit(
                f"[reflect] {date_str}: no graded outcome yet — run outcome first"
            )

    predict_md = _read(os.path.join(config.DAILY_GENERAL, f"{date_str}_predict.md"))
    outcome_md = _read(os.path.join(config.DAILY_GENERAL, f"{date_str}_outcome.md"))
    with open(os.path.join(config.GROUNDING, "reflect_prompt.md"),
              encoding="utf-8") as fh:
        prompt = fh.read()

    schema_instructions = (
        "\n\n## CANDIDATE LESSON SCHEMA (MANDATORY)\n"
        "Inside LESSON_BEGIN...LESSON_END you MUST emit:\n"
        "ERROR_CATEGORY: A|B|C|D|E|NONE\n"
        "TRIGGER_PATTERN: <when — observable before the open, max 2 sentences>\n"
        "CURRENT_BEHAVIOR: <what the system did wrong>\n"
        "CORRECTED_BEHAVIOR: <do_instead — must name B0-B7, direction, futures, "
        "weight cap, gate, or ops step>\n"
        "FALSIFIER: <wrong_if — one sentence; when is this lesson wrong?>\n"
        "EVIDENCE: <date + predicted vs actual>\n"
        "If ERROR_CATEGORY is NONE, still close the block but leave patterns empty.\n"
        "Incomplete lessons (missing TRIGGER/CORRECTED/FALSIFIER when category "
        "is not NONE) are rejected by the pipeline.\n"
    )

    user_msg = (
        f"TODAY: {date_str}\n\n"
        f"=== PREMARKET PREDICTION ===\n{predict_md}\n\n"
        f"=== POST-MARKET OUTCOME ===\n{outcome_md}\n\n"
        f"=== SCOREBOARD ENTRY (pipeline-graded) ===\n"
        f"direction_hit: {entry.get('direction_hit')} | magnitude_hit: "
        f"{entry.get('magnitude_hit')} | ops_fail: {entry.get('ops_fail')} | "
        f"predicted {entry.get('predicted_direction')}/"
        f"{entry.get('predicted_magnitude_band')} vs actual "
        f"{entry.get('actual_pct_change')}% ({entry.get('actual_direction')}/"
        f"{entry.get('actual_magnitude_band')}) | divergence_flagged: "
        f"{entry.get('divergence_flagged')}\n\n"
        f"=== RECENT SCOREBOARD HISTORY ===\n{memory.scoreboard_summary()}\n\n"
        f"=== RECENT CANDIDATE LESSON TRIGGERS ===\n"
        f"{_candidate_lesson_triggers()}\n\n"
        f"=== STANDING ACTIVE LESSONS ===\n{memory.active_lessons()}\n\n"
        f"{schema_instructions}\n"
        "Execute the diagnostic now. Answer all mandatory checks explicitly."
    )

    text = deepseek_client.chat(
        [{"role": "system", "content": prompt},
         {"role": "user", "content": user_msg}],
        model=config.MODEL_REFLECT, tools=False, max_tokens=12000,
        transcript_path=os.path.join("01_daily/_transcripts",
                                     f"{date_str}_reflect.json"),
        trace_path=os.path.join(config.DAILY_GENERAL,
                                f"{date_str}_reflect_trace.md"),
        stage_label=f"REFLECT {date_str}",
    )

    lb = _parse_lesson_block(text)
    norm = lesson_schema.normalize(lb, date_str)
    errs = lesson_schema.validation_errors(norm)
    complete = lesson_schema.is_complete(norm) and not errs
    if not complete and norm.get("error_category") not in ("NONE", ""):
        print(f"[reflect] SCHEMA WARNING {date_str}: {errs}")
        norm["status"] = "candidate_incomplete"
    else:
        norm["status"] = "candidate"

    os.makedirs(config.LESSONS_CANDIDATE, exist_ok=True)
    lesson_path = os.path.join(config.LESSONS_CANDIDATE, f"{date_str}_lesson.md")
    with open(lesson_path, "w", encoding="utf-8") as fh:
        fh.write(lesson_schema.frontmatter(norm, extra={
            "backward_check": lb.get("BACKWARD_CHECK", ""),
            "conflict_check": lb.get("CONFLICT_CHECK", ""),
            "lesson_match_check": lb.get("LESSON_MATCH_CHECK", ""),
            "validation_errors": "; ".join(errs) if errs else "",
        }))
        fh.write(f"\n# Reflection — {date_str}\n\n")
        try:
            fh.write(snapshot.reflect_snapshot(lb, entry))
        except Exception:
            pass
        fh.write(text + "\n")

    entry["reflection_lesson_ref"] = lesson_path
    entry["lesson_schema_ok"] = complete
    dv = lb.get("DIVERGENCE_VERDICT")
    if dv and dv != "none_flagged":
        entry["divergence_verdict"] = dv
    scoreboard.save(board)
    print(
        f"[reflect] {date_str}: category={norm.get('error_category')} "
        f"schema_ok={complete} -> {lesson_path}"
    )


if __name__ == "__main__":
    main()
