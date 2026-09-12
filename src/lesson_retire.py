"""Efficacy-gated retirement of active lessons.

lesson_efficacy already computes, for every active lesson, the topic's
direction hit rate before vs after the lesson went live. Until now that
verdict was only journaled ("retirement candidate for the monthly
distill") and the monthly distill has been disabled since 2026-08-29, so
nothing was ever retired: 42 of 47 judged lessons were WORSE and all 42
kept being injected into every prompt.

This module closes that loop nightly:
  * a lesson judged WORSE (delta <= RETIRE_DELTA with >= MIN_SIDE graded
    runs on both sides) is moved to 02_lessons/retired/ with
    status "retired" plus the numbers that retired it;
  * at most MAX_PER_CYCLE lessons retire per run (worst first) so a single
    bad week cannot wipe the rule book;
  * every retirement is appended to 03_scoreboard/LESSON_RETIREMENTS.md
    and lesson_retirements.json.

Nothing is deleted; a retired rule can be re-learned if its pattern recurs
(the reflect -> candidate -> cluster path is unchanged).

CLI: python -m src.lesson_retire [--dry-run] [--max N]
"""
from __future__ import annotations

import argparse
import json
import re
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from . import config, lesson_efficacy

ROOT = Path(__file__).resolve().parent.parent
ACTIVE_DIR = ROOT / "02_lessons" / "active"
RETIRED_DIR = ROOT / "02_lessons" / "retired"
LEDGER_MD = ROOT / "03_scoreboard" / "LESSON_RETIREMENTS.md"
LEDGER_JSON = ROOT / "03_scoreboard" / "lesson_retirements.json"

RETIRE_DELTA = -0.05     # same cut lesson_efficacy uses for the WORSE verdict
MAX_PER_CYCLE = 10


def _load_ledger() -> list[dict]:
    try:
        return json.loads(LEDGER_JSON.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []


def _mark_retired(text: str, row: dict, today: str) -> str:
    reason = (f"topic {row['topic']} direction hit "
              f"{row['before']['hit']:.0%} (n={row['before']['n']}) before -> "
              f"{row['after']['hit']:.0%} (n={row['after']['n']}) after activation "
              f"(delta {row['delta']:+.3f})")
    extra = (f'status: "retired"\nretired_on: "{today}"\n'
             f'retire_reason: "{reason}"')
    if re.search(r'^status:\s*"?active"?\s*$', text, re.M):
        return re.sub(r'^status:\s*"?active"?\s*$', extra, text, count=1, flags=re.M)
    if text.startswith("---\n"):
        return "---\n" + extra + "\n" + text[4:]
    return f"---\n{extra}\n---\n\n{text}"


def candidates(efficacy: dict | None = None) -> list[dict]:
    eff = efficacy if efficacy is not None else lesson_efficacy.evaluate()
    rows = [r for r in eff.get("lessons", [])
            if r.get("verdict") == "WORSE" and r.get("delta") is not None
            and r["delta"] <= RETIRE_DELTA]
    rows.sort(key=lambda r: r["delta"])
    return rows


def retire(efficacy: dict | None = None, dry_run: bool = False,
           max_per_cycle: int = MAX_PER_CYCLE) -> list[dict]:
    """Move the worst WORSE-verdict lessons out of active/. Returns ledger rows."""
    today = datetime.now(ZoneInfo(config.TZ)).date().isoformat()
    now = datetime.now(ZoneInfo(config.TZ)).isoformat(timespec="seconds")
    done = []
    for row in candidates(efficacy):
        if len(done) >= max_per_cycle:
            break
        src = ACTIVE_DIR / row["lesson"]
        if not src.is_file():
            continue
        entry = {
            "at": now, "lesson": row["lesson"], "topic": row["topic"],
            "active_since": row.get("active_since"),
            "before": row["before"], "after": row["after"], "delta": row["delta"],
            "dry_run": dry_run,
        }
        if not dry_run:
            RETIRED_DIR.mkdir(parents=True, exist_ok=True)
            text = src.read_text(encoding="utf-8")
            (RETIRED_DIR / row["lesson"]).write_text(
                _mark_retired(text, row, today), encoding="utf-8")
            src.unlink()
        done.append(entry)
        print(f"[retire]{' (dry)' if dry_run else ''} {row['lesson']} "
              f"{row['topic']} {row['before']['hit']:.0%} -> {row['after']['hit']:.0%}")
    if done and not dry_run:
        ledger = _load_ledger() + done
        LEDGER_JSON.parent.mkdir(parents=True, exist_ok=True)
        LEDGER_JSON.write_text(json.dumps(ledger, indent=2), encoding="utf-8")
        _write_md(ledger)
    return done


def _write_md(ledger: list[dict]) -> None:
    L = ["# Lesson retirements (efficacy-gated, automatic)", "",
         "A lesson retires when its topic's direction hit rate fell by more than "
         f"{abs(RETIRE_DELTA):.0%} in the {lesson_efficacy.MIN_SIDE}+ graded runs after it went "
         f"active vs before (lesson_efficacy verdict WORSE). Max {MAX_PER_CYCLE} per night, "
         "worst first. Files move to `02_lessons/retired/`; nothing is deleted.",
         "", f"Total retired: **{len(ledger)}**", "",
         "| Retired at | Lesson | Topic | Active since | Before | After | Δ |",
         "|---|---|---|---|---:|---:|---:|"]
    for e in reversed(ledger[-200:]):
        L.append(f"| {e['at'][:10]} | `{e['lesson'][:56]}` | {e['topic']} | {e.get('active_since') or '—'} | "
                 f"{e['before']['hit']:.0%} (n={e['before']['n']}) | "
                 f"{e['after']['hit']:.0%} (n={e['after']['n']}) | {e['delta']:+.0%} |")
    LEDGER_MD.write_text("\n".join(L) + "\n", encoding="utf-8")
    print(f"[retire] wrote {LEDGER_MD}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--max", type=int, default=MAX_PER_CYCLE)
    args = ap.parse_args()
    rows = retire(dry_run=args.dry_run, max_per_cycle=args.max)
    print(f"[retire] {len(rows)} lesson(s){' would be' if args.dry_run else ''} retired")


if __name__ == "__main__":
    main()
