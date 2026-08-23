"""DEEPTHINK — the sauna session.

Daily reflection is reactive: it explains yesterday's miss. This module is
the opposite — a slow, scheduled (weekly) session where the strongest
reasoning model gets the ENTIRE system's evidence at once, with no daily
incident to explain, and must come out with a fresh perspective on how to
do the whole job better.

Three rounds, one conversation (marinate, then rinse):

  1. DIAGNOSE  — from the full dossier, name the 3 biggest STRUCTURAL
                 limitations (not individual missed days).
  2. IDEATE    — at least 6 distinct improvement ideas, at least 2 of them
                 unconventional. No self-censoring; bad ideas cost nothing
                 here.
  3. RED-TEAM  — attack every idea, discard the weak, and emerge with the
                 top 3: concretely what to change, expected effect on the
                 dashboard, the cheapest test that would validate it, a
                 falsifier, and the main risk.

Previous sessions are included so it must build on (or explicitly retire)
its own earlier proposals instead of rediscovering them.

Outputs
  02_lessons/deepthink/{date}_deepthink.md   full three-round transcript
  03_scoreboard/DEEPTHINK.md                 latest final proposals
  02_lessons/hypotheses/deepthink_latest.md  stub the learn-cycle digest surfaces

CLI: python -m src.deepthink [--date YYYY-MM-DD] [--dry-run]
"""
from __future__ import annotations

import argparse
import os
import re
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from . import config

ROOT = Path(__file__).resolve().parent.parent
OUT_DIR = ROOT / "02_lessons" / "deepthink"
BOARD_MD = ROOT / "03_scoreboard" / "DEEPTHINK.md"
HYPO_STUB = ROOT / "02_lessons" / "hypotheses" / "deepthink_latest.md"

MAX_TOKENS = 6000


def _read(rel: str, limit: int) -> str:
    p = ROOT / rel
    try:
        return p.read_text(encoding="utf-8")[:limit]
    except OSError:
        return "(missing)"


def _active_lesson_titles(limit: int = 70) -> str:
    names = sorted(p.name for p in (ROOT / "02_lessons" / "active").glob("*.md"))
    return "\n".join(f"- {n}" for n in names[:limit]) or "(none)"


def _previous_sessions(limit: int = 2) -> str:
    files = sorted(OUT_DIR.glob("????-??-??_deepthink.md"))[-limit:]
    if not files:
        return "(this is the first deepthink session)"
    out = []
    for p in files:
        text = p.read_text(encoding="utf-8")
        m = re.search(r"## ROUND 3.*", text, re.S)
        out.append(f"--- session {p.name[:10]} (final round) ---\n"
                   f"{(m.group(0) if m else text)[:4000]}")
    return "\n\n".join(out)


def build_dossier() -> str:
    return "\n\n".join([
        "=== SYSTEM (one paragraph) ===",
        "Daily pipeline: collectors → labels×weather join → news judge/actions "
        "→ LLM general+sector predicts → AB checklist(+peer/industry enrich) → "
        "peer RS → stock_book ranker (learned weights, book_policy.json) → "
        "top-10 paper sleeves with min-holds and Futubull fees → dashboard. "
        "Learning: predict/outcome/reflect lessons → mutable policy (prompt "
        "injection); book_learn tunes ranker weights on realized returns; "
        "book_reflect scans missed movers; lesson_efficacy checks whether "
        "promoted lessons changed outcomes. THE ONLY METRIC THAT MATTERS is "
        "the paper dashboard's compounding return.",
        "=== REPORT CARD (arithmetic health) ===",
        _read("03_scoreboard/report_card.md", 4500),
        "=== HIT BOARD (LLM predicts) ===",
        _read("03_scoreboard/HIT_BOARD.md", 3500),
        "=== STOCK BOOK BACKTEST ===",
        _read("03_scoreboard/STOCK_BOOK_BACKTEST.md", 2500),
        "=== PAPER TRADING (the metric) ===",
        _read("03_scoreboard/PAPER_TRADING.md", 2500),
        "=== WEIGHT TUNER LEDGER ===",
        _read("03_scoreboard/BOOK_LEARN.md", 3000),
        "=== GAP SCAN (missed movers / blind spots) ===",
        _read("03_scoreboard/BOOK_GAPS.md", 3500),
        "=== MISSING-INPUT HYPOTHESES ===",
        _read("02_lessons/hypotheses/book_missing_inputs.md", 3000),
        "=== LESSON EFFICACY ===",
        _read("03_scoreboard/LESSON_EFFICACY.md", 3000),
        "=== LEARNINGS DIGEST ===",
        _read("03_scoreboard/LEARNINGS.md", 3000),
        "=== ACTIVE LESSON TITLES ===",
        _active_lesson_titles(),
        "=== YOUR PREVIOUS DEEPTHINK SESSIONS ===",
        _previous_sessions(),
    ])


SYSTEM = """You are the deepthink engine for an autonomous stock-picking
system. This is NOT daily reflection. You are in the sauna: no incident to
explain, no format to grade, the whole system's evidence in front of you,
and one question — how does the paper dashboard's compounding return get
structurally better?

Rules of the room:
- Ground every claim in the dossier. Cite the section you are reasoning from.
- Structural means: inputs, information flow, incentives, evaluation — not
  'the model should have scored S1 lower on Aug 14'.
- You will be asked for unconventional ideas. Give real ones, not safe ones
  dressed up. An idea that might be wrong but is cheap to test beats a
  platitude.
- Respect what is already built (weight tuner, gap scan, lesson efficacy,
  risk sizing). Build on or explicitly retire your previous sessions'
  proposals — never restate them as new.
- Numeric weight values are tuned by a deterministic process; propose
  mechanisms, not weight numbers."""

ROUND1 = """ROUND 1 — DIAGNOSE.
From the dossier, name the THREE biggest structural limitations holding
back the dashboard number. For each: the evidence (cite sections), why it
is structural rather than incidental, and what it costs (roughly, in
return or risk). Do not propose fixes yet."""

ROUND2 = """ROUND 2 — IDEATE.
Propose at least SIX distinct improvements, at least TWO unconventional.
One line each: mechanism + which diagnosed limitation it attacks. Include
at least one idea about information the system does not currently ingest,
and at least one about how the system evaluates itself. Do not evaluate
feasibility yet — that is round 3's job."""

ROUND3 = """ROUND 3 — RED-TEAM AND SELECT.
Attack each of your round-2 ideas: what breaks it, what it costs, what
evidence says it may not work. Discard the weak. Emerge with the TOP 3,
formatted exactly:

## PROPOSAL <n>: <name>
- CHANGE: <what to modify, concretely — module/file/process level>
- WHY: <which limitation it fixes, citing dossier evidence>
- EXPECTED EFFECT: <on the dashboard number, honestly sized>
- CHEAP TEST: <smallest experiment that validates or kills it>
- FALSIFIER: <what result proves it wrong>
- RISK: <main way it backfires>

Then one final section: '## RETIRED FROM PREVIOUS SESSIONS' listing any of
your earlier proposals that today's evidence has invalidated (or 'none')."""


def run(date_str: str, dry_run: bool = False) -> None:
    dossier = build_dossier()
    if dry_run:
        print(dossier[:2000])
        print(f"\n[deepthink] dry run — dossier is {len(dossier):,} chars")
        return
    if not config.has_llm():
        print("[deepthink] no OPENCLAW_GATEWAY_URL or DEEPSEEK_API_KEY — skipping")
        return
    from . import deepseek_client

    messages = [
        {"role": "system", "content": SYSTEM},
        {"role": "user", "content": f"DATE: {date_str}\n\n{dossier}\n\n{ROUND1}"},
    ]
    rounds: list[str] = []
    for i, nxt in enumerate((ROUND2, ROUND3, None), start=1):
        print(f"[deepthink] round {i}…")
        text = deepseek_client.chat(
            messages, model=config.MODEL_DISTILL, tools=False,
            max_tokens=MAX_TOKENS)
        if not text:
            print(f"[deepthink] round {i} came back empty — aborting session")
            return
        rounds.append(text)
        messages.append({"role": "assistant", "content": text})
        if nxt:
            messages.append({"role": "user", "content": nxt})

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    full = [f"# Deepthink session — {date_str}", ""]
    for i, r in enumerate(rounds, 1):
        full += [f"## ROUND {i}", "", r, ""]
    session_path = OUT_DIR / f"{date_str}_deepthink.md"
    session_path.write_text("\n".join(full), encoding="utf-8")

    BOARD_MD.write_text(
        f"# Deepthink — latest proposals ({date_str})\n\n"
        f"Full session: `02_lessons/deepthink/{date_str}_deepthink.md`\n\n"
        + rounds[-1] + "\n",
        encoding="utf-8",
    )
    HYPO_STUB.parent.mkdir(parents=True, exist_ok=True)
    HYPO_STUB.write_text(
        "---\n"
        'scope: "deepthink"\n'
        f'updated: "{date_str}"\n'
        "---\n\n"
        f"# Deepthink proposals ({date_str})\n\n"
        "See `03_scoreboard/DEEPTHINK.md` for the current top-3 structural "
        "proposals and their cheap tests.\n",
        encoding="utf-8",
    )
    print(f"[deepthink] session → {session_path}")
    print(f"[deepthink] board → {BOARD_MD}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=None)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    date_str = args.date or datetime.now(ZoneInfo(config.TZ)).date().isoformat()
    run(date_str, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
