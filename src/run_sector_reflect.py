"""Sector REFLECT — one sector diagnostic → candidate lesson.

CLI:
  python -m src.run_sector_reflect [--date YYYY-MM-DD] [--sectors Technology]
"""
from __future__ import annotations

import argparse
import glob
import os
import re
import subprocess
import sys
from datetime import datetime
from zoneinfo import ZoneInfo

from . import config, deepseek_client, scoreboard
from .run_reflect import last_assistant
from .skip_if_good import is_tool_dump
from .sector_memory import scoreboard_summary, topic_for
from .sector_taxonomy import FINVIZ_SECTORS


def _slug(sector: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", sector.lower()).strip("_")


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


def _candidate_triggers(limit: int = 12) -> str:
    files = sorted(glob.glob(os.path.join(config.LESSONS_CANDIDATE, "*.md")))
    rows = []
    for p in files[-limit:]:
        head = _read(p)[:800]
        trig = re.search(r'trigger_pattern:\s*"(.*?)"', head)
        cat = re.search(r'error_category:\s*"(.*?)"', head)
        date = re.search(r'date:\s*"(.*?)"', head)
        rows.append(
            f"- {date.group(1) if date else os.path.basename(p)} "
            f"[{cat.group(1) if cat else '?'}]: "
            f"{trig.group(1) if trig else '(no trigger)'}")
    return "\n".join(rows) or "(no candidate lessons yet)"


def _persist(date_str: str) -> None:
    """Land this sector reflect before the next Grok call or a kill."""
    try:
        from .run_postclose_all import _push_pack
        _push_pack(date_str)
    except Exception as e:  # noqa: BLE001
        print(f"[sector-reflect] persist warn: {e}")


def _write_reflect(sector: str, date_str: str, slug: str, out_dir: str,
                   text: str, entry: dict, board: dict) -> None:
    lb = _parse_lesson_block(text)
    os.makedirs(config.LESSONS_CANDIDATE, exist_ok=True)
    lesson_path = os.path.join(
        config.LESSONS_CANDIDATE, f"{date_str}_sector_{slug}_lesson.md")
    with open(lesson_path, "w", encoding="utf-8") as fh:
        fh.write("---\n")
        fh.write(f"trigger_pattern: \"{lb.get('TRIGGER_PATTERN', '')}\"\n")
        fh.write(f"current_behavior: \"{lb.get('CURRENT_BEHAVIOR', '')}\"\n")
        fh.write(f"corrected_behavior: \"{lb.get('CORRECTED_BEHAVIOR', '')}\"\n")
        fh.write(f"evidence_cited: \"{lb.get('EVIDENCE', '')}\"\n")
        fh.write(f"error_category: \"{lb.get('ERROR_CATEGORY', 'NONE')}\"\n")
        fh.write(f"falsifier: \"{lb.get('FALSIFIER', '')}\"\n")
        fh.write(f"sector: \"{sector}\"\n")
        fh.write(f"date: \"{date_str}\"\n")
        fh.write("status: \"candidate\"\n---\n\n")
        fh.write(f"# Sector Reflection — {sector} — {date_str}\n\n")
        fh.write(text + "\n")

    os.makedirs(out_dir, exist_ok=True)
    reflect_md = os.path.join(out_dir, f"{slug}_reflect.md")
    with open(reflect_md, "w", encoding="utf-8") as fh:
        fh.write(f"# Sector Reflect — {sector} — {date_str}\n\n")
        fh.write(text + "\n")

    entry["reflection_lesson_ref"] = lesson_path
    scoreboard.save(board)
    print(f"[sector-reflect] {sector}: {lb.get('ERROR_CATEGORY')} -> {lesson_path}")


def _pct_from_outcome_md(text: str):
    """Read Actuals: {...} written by run_sector_outcome if scoreboard lagged."""
    import ast
    m = re.search(r"Actuals:\s*(\{.*\})", text)
    if not m:
        return None
    try:
        payload = ast.literal_eval(m.group(1))
    except (ValueError, SyntaxError):
        return None
    if not isinstance(payload, dict) or payload.get("pct") is None:
        return None
    try:
        return float(payload["pct"])
    except (TypeError, ValueError):
        return None


def run_one(sector: str, date_str: str) -> None:
    topic = topic_for(sector)
    board = scoreboard.load()
    entry = scoreboard.get_or_create(board, date_str, topic)
    slug = _slug(sector)
    out_dir = os.path.join(config.DAILY_SECTORS, date_str)
    if entry.get("actual_pct_change") is None:
        outcome_md = _read(os.path.join(out_dir, f"{slug}_outcome.md"))
        pct = _pct_from_outcome_md(outcome_md)
        if pct is None:
            print(f"[sector-reflect] skip {sector}: no graded outcome")
            return
        entry["actual_pct_change"] = pct
        print(f"[sector-reflect] {sector}: actuals from outcome.md pct={pct}")
    existing = os.path.join(out_dir, f"{slug}_reflect.md")
    if os.path.isfile(existing) and os.path.getsize(existing) >= 200:
        try:
            with open(existing, encoding="utf-8") as fh:
                on_disk = fh.read()
        except OSError:
            on_disk = ""
        if not is_tool_dump(on_disk):
            print(f"[sector-reflect] skip {sector}: reflect already on disk")
            return
        print(f"[sector-reflect] {sector}: disk file is a tool-dump "
              f"({len(on_disk)} chars) — rewriting", flush=True)

    transcript_path = os.path.join(
        "01_daily/_transcripts", f"{date_str}_sector_{slug}_reflect.json")
    reused = last_assistant(transcript_path)
    if len(reused) >= 200 and not is_tool_dump(reused):
        print(f"[sector-reflect] {sector}: reuse transcript "
              f"({len(reused)} chars) — no LLM")
        _write_reflect(sector, date_str, slug, out_dir, reused, entry, board)
        _persist(date_str)
        return

    config.require_llm()
    predict_md = _read(os.path.join(out_dir, f"{slug}_predict.md"))
    outcome_md = _read(os.path.join(out_dir, f"{slug}_outcome.md"))

    with open(os.path.join(config.GROUNDING, "sector_reflect_prompt.md"),
              encoding="utf-8") as fh:
        prompt = fh.read()

    user_msg = (
        f"DATE: {date_str}\nSECTOR: {sector}\n\n"
        f"=== PREDICT ===\n{predict_md}\n\n"
        f"=== OUTCOME ===\n{outcome_md}\n\n"
        f"=== SCOREBOARD ENTRY ===\n"
        f"direction_hit: {entry.get('direction_hit')} | magnitude_hit: "
        f"{entry.get('magnitude_hit')} | predicted "
        f"{entry.get('predicted_direction')}/{entry.get('predicted_magnitude_band')} "
        f"vs actual {entry.get('actual_pct_change')}%\n\n"
        f"=== SECTOR SCOREBOARD HISTORY ===\n{scoreboard_summary(sector)}\n\n"
        f"=== RECENT CANDIDATE TRIGGERS ===\n{_candidate_triggers()}\n\n"
        "Execute the diagnostic. Answer all five checks."
    )

    text = deepseek_client.chat_nonempty(
        [{"role": "system", "content": prompt},
         {"role": "user", "content": user_msg}],
        ladder=[(config.MODEL_REFLECT, 12000),
                (config.MODEL_REFLECT, 16000),
                (config.MODEL_PREDICT, 8000)],
        tools=False,
        transcript_path=transcript_path,
        trace_path=os.path.join(out_dir, f"{slug}_reflect_trace.md"),
        stage_label=f"SECTOR REFLECT {sector} {date_str}",
    )

    if not text.strip():
        # deepseek-reasoner exhausted its budget on every rung — writing the
        # empty text would produce a blank reflect md AND a junk empty
        # candidate lesson. Skip both; the scoreboard entry keeps no ref.
        print(f"[sector-reflect] {sector}: EMPTY after all retries — "
              f"not writing reflect md or lesson")
        return

    _write_reflect(sector, date_str, slug, out_dir, text, entry, board)
    _persist(date_str)


def _one_timeout_s(default: int = 600) -> int:
    raw = os.environ.get("SECTOR_ONE_TIMEOUT", str(default))
    try:
        return max(120, int(raw))
    except ValueError:
        return default


def _run_one_bounded(sector: str, date_str: str) -> None:
    """Kill one hung reflect so the remaining sectors still write."""
    if os.environ.get("SECTOR_GRADE_CHILD") == "1":
        run_one(sector, date_str)
        return
    env = {**os.environ, "SECTOR_GRADE_CHILD": "1"}
    timeout_s = _one_timeout_s()
    cmd = [sys.executable, "-m", "src.run_sector_reflect",
           "--date", date_str, "--sectors", sector]
    try:
        r = subprocess.run(cmd, timeout=timeout_s, env=env)
    except subprocess.TimeoutExpired:
        print(f"[sector-reflect] WARN {sector}: killed after {timeout_s}s "
              "— continue so ≥8 files can still land", flush=True)
        return
    if r.returncode:
        print(f"[sector-reflect] WARN {sector}: exit {r.returncode}",
              flush=True)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=None)
    ap.add_argument("--sectors", default=None)
    args = ap.parse_args()
    date_str = args.date or datetime.now(ZoneInfo(config.TZ)).date().isoformat()
    sectors = ([s.strip() for s in args.sectors.split(",") if s.strip()]
               if args.sectors else list(FINVIZ_SECTORS))
    for sector in sectors:
        if sector not in FINVIZ_SECTORS:
            raise SystemExit(f"unknown sector {sector}")
        print(f"\n======== SECTOR REFLECT: {sector} ========\n")
        try:
            _run_one_bounded(sector, date_str)
        except Exception as e:  # noqa: BLE001
            print(f"[sector-reflect] WARN {sector}: {e}")


if __name__ == "__main__":
    main()
