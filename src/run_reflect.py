"""Stage REFLECT → schema-gated candidate lesson.

CLI: python -m src.run_reflect [--date YYYY-MM-DD]
"""
from __future__ import annotations

import argparse
import glob
import json
import os
import re
from datetime import datetime
from zoneinfo import ZoneInfo

from . import config, deepseek_client, lesson_schema, memory, scoreboard, snapshot
from .skip_if_good import is_tool_dump


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


def last_assistant(path: str) -> str:
    """Reuse a landed transcript so a missing *_reflect.md does not re-call Grok."""
    try:
        with open(path, encoding="utf-8") as fh:
            data = json.load(fh)
    except (OSError, ValueError):
        return ""
    for msg in reversed(data.get("messages") or []):
        if not isinstance(msg, dict):
            continue
        if msg.get("role") != "assistant":
            continue
        text = str(msg.get("content") or "").strip()
        # A leaked DSML tool-call in content is 600–900 B — size alone
        # would reuse it as the reflect and freeze the dump on disk.
        if text and not is_tool_dump(text):
            return text
    return ""


def _write_reflect(date_str: str, text: str, entry: dict, board: dict) -> None:
    """Gate file + candidate lesson. The night pack looks at *_reflect.md."""
    os.makedirs(config.DAILY_GENERAL, exist_ok=True)
    reflect_md = os.path.join(config.DAILY_GENERAL, f"{date_str}_reflect.md")
    with open(reflect_md, "w", encoding="utf-8") as fh:
        fh.write(f"# Reflect — {date_str}\n\n")
        fh.write(text)
        if not text.endswith("\n"):
            fh.write("\n")

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
    print(f"[reflect] {date_str}: wrote {reflect_md} "
          f"category={norm.get('error_category')} schema_ok={complete} "
          f"-> {lesson_path}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=None)
    args = ap.parse_args()
    date_str = args.date or datetime.now(ZoneInfo(config.TZ)).date().isoformat()

    board = scoreboard.load()
    entry = scoreboard.get_or_create(board, date_str, config.TOPIC)
    if entry.get("actual_pct_change") is None and entry.get("direction_hit") is None and not entry.get("ops_fail"):
        raise SystemExit(f"[reflect] {date_str}: no graded outcome yet — run outcome first")

    reflect_md = os.path.join(config.DAILY_GENERAL, f"{date_str}_reflect.md")
    if os.path.isfile(reflect_md) and os.path.getsize(reflect_md) >= 200:
        try:
            with open(reflect_md, encoding="utf-8") as fh:
                on_disk = fh.read()
        except OSError:
            on_disk = ""
        if not is_tool_dump(on_disk):
            print(f"[reflect] {date_str}: reflect already on disk — skip")
            return
        print(f"[reflect] {date_str}: disk file is a tool-dump "
              f"({len(on_disk)} chars) — rewriting", flush=True)

    transcript_path = os.path.join(
        "01_daily/_transcripts", f"{date_str}_reflect.json")
    reused = last_assistant(transcript_path)
    if len(reused) >= 200:
        print(f"[reflect] {date_str}: reuse transcript ({len(reused)} chars) "
              "— no LLM")
        _write_reflect(date_str, reused, entry, board)
        return

    config.require_llm()

    predict_md = _read(os.path.join(config.DAILY_GENERAL, f"{date_str}_predict.md"))
    outcome_md = _read(os.path.join(config.DAILY_GENERAL, f"{date_str}_outcome.md"))
    with open(os.path.join(config.GROUNDING, "reflect_prompt.md"), encoding="utf-8") as fh:
        prompt = fh.read()

    schema_instructions = (
        "\n## CANDIDATE LESSON SCHEMA (MANDATORY)\n"
        "Inside LESSON_BEGIN...LESSON_END emit:\n"
        "ERROR_CATEGORY: A|B|C|D|E|NONE\n"
        "TRIGGER_PATTERN: <when — before the open, max 2 sentences>\n"
        "CURRENT_BEHAVIOR: <what went wrong>\n"
        "CORRECTED_BEHAVIOR: <do_instead — must name B0-B7, direction, futures, weight, gate, or ops step>\n"
        "FALSIFIER: <wrong_if — one sentence>\n"
        "EVIDENCE: <date + predicted vs actual>\n"
        "Incomplete lessons (missing TRIGGER/CORRECTED/FALSIFIER when category != NONE) are rejected.\n"
    )

    user_msg = (
        f"TODAY: {date_str}\n\n"
        f"=== PREMARKET PREDICTION ===\n{predict_md}\n\n"
        f"=== POST-MARKET OUTCOME ===\n{outcome_md}\n\n"
        f"=== SCOREBOARD ENTRY ===\n"
        f"direction_hit: {entry.get('direction_hit')} | magnitude_hit: {entry.get('magnitude_hit')} | "
        f"ops_fail: {entry.get('ops_fail')} | predicted {entry.get('predicted_direction')}/"
        f"{entry.get('predicted_magnitude_band')} vs actual {entry.get('actual_pct_change')}% "
        f"({entry.get('actual_direction')}/{entry.get('actual_magnitude_band')})\n\n"
        f"=== SCOREBOARD HISTORY ===\n{memory.scoreboard_summary()}\n\n"
        f"=== CANDIDATE TRIGGERS ===\n{_candidate_lesson_triggers()}\n\n"
        f"=== STANDING ACTIVE LESSONS ===\n{memory.active_lessons()}\n\n"
        f"{schema_instructions}\n"
        "Execute the diagnostic. Answer mandatory checks explicitly."
    )

    text = deepseek_client.chat_nonempty(
        [{"role": "system", "content": prompt},
         {"role": "user", "content": user_msg}],
        ladder=[(config.MODEL_REFLECT, 12000),
                (config.MODEL_REFLECT, 16000),
                (config.MODEL_PREDICT, 8000)],
        tools=False,
        transcript_path=transcript_path,
        trace_path=os.path.join(config.DAILY_GENERAL, f"{date_str}_reflect_trace.md"),
        stage_label=f"REFLECT {date_str}",
    )

    if not text.strip():
        # Model returned empty on every rung — abort WITHOUT writing a
        # lesson file (an empty lesson would pollute the candidate pool).
        raise SystemExit(f"[reflect] {date_str}: model returned EMPTY after "
                         f"all retries — no reflect/lesson written")

    _write_reflect(date_str, text, entry, board)


if __name__ == "__main__":
    main()
