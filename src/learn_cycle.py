"""Closed learning cycle: outcomes → lessons → policy that predict actually reads.

Problems this solves:
  - Candidate MD files accumulate but never become behavior.
  - Reflect only fires hard on losses; wins are not mined for "could have been better".
  - Active lessons are rare; weather_rules / prompt policy almost never change.

What this does (deterministic + optional LLM merge):
  1. Read recent graded general runs (wins AND losses).
  2. Write structured HYPOTHESIS files for both.
  3. Promote complete candidate lessons into 02_lessons/active (looser gate).
  4. Rewrite 00_grounding/mutable_policy.md (injected every predict).
  5. Optionally propose weather_rules threshold patches into
     00_grounding/weather_rules_proposals.json (human/bot can accept later).

Core outputs UNCHANGED: SCORES_BEGIN format, B0–B7 names, pipeline arithmetic.

CLI:
  python -m src.learn_cycle [--lookback 15]
"""
from __future__ import annotations

import argparse
import glob
import json
import os
import re
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from . import config, lesson_schema, scoreboard

ROOT = Path(__file__).resolve().parent.parent
MUTABLE = ROOT / "00_grounding" / "mutable_policy.md"
PROPOSALS = ROOT / "00_grounding" / "weather_rules_proposals.json"
HYPO_DIR = ROOT / "02_lessons" / "hypotheses"
CAND_DIR = Path(config.LESSONS_CANDIDATE)
ACTIVE_DIR = Path(config.LESSONS_ACTIVE)


def _read(p: Path | str) -> str:
    try:
        return Path(p).read_text(encoding="utf-8")
    except OSError:
        return ""


def _graded_general(lookback: int = 20) -> list[dict]:
    board = scoreboard.load()
    runs = [
        r for r in board.get("runs", [])
        if r.get("topic", "general") == "general"
        and r.get("predicted_direction")
        and r.get("direction_hit") is not None
        and not r.get("ops_fail")
    ]
    runs = sorted(runs, key=lambda x: x.get("date", ""))
    return runs[-lookback:]


def _hypotheses_from_runs(runs: list[dict]) -> list[dict]:
    """Rule-based hypotheses so we always learn something without LLM."""
    hypos = []
    for r in runs:
        d = r.get("date")
        hit = r.get("direction_hit") is True
        pred = r.get("predicted_direction")
        act = r.get("actual_direction")
        pct = r.get("actual_pct_change")
        score = r.get("total_score")
        mag_hit = r.get("magnitude_hit")

        if hit:
            # WIN — still ask: could weighting have been better?
            hypos.append({
                "date": d,
                "kind": "win",
                "title": f"win_{d}_{pred}",
                "when": f"Predicted {pred} and market went {act} (pct={pct}, score={score}).",
                "ask": (
                    "Could magnitude band have been tighter/looser? "
                    "Was any factor double-counted? "
                    "Would a different B6 vs leading-indicator weight have improved mag hit?"
                ),
                "experiment": (
                    "On next similar setup, log factor contribution order and test "
                    "whether capping lagging futures (B6) when leading sum is strong "
                    "improves magnitude_hit without hurting direction_hit."
                ),
                "do_instead": (
                    "Keep direction rule; for wins with magnitude MISS, shrink magnitude "
                    "confidence when |total_score| is modest (|score|<4)."
                ),
                "wrong_if": (
                    "Wrong if magnitude_hit stays low even after shrinking confidence "
                    "on modest scores across 5+ wins."
                ),
                "mag_hit": mag_hit,
            })
        else:
            hypos.append({
                "date": d,
                "kind": "loss",
                "title": f"loss_{d}_{pred}_vs_{act}",
                "when": f"Predicted {pred} but market went {act} (pct={pct}, score={score}).",
                "ask": (
                    "Which factor family drove the score? "
                    "Was this a missing Channel-2 source, misweighted macro print, "
                    "or regime misread (risk-on/off)?"
                ),
                "experiment": (
                    "Next time score sign agrees with this failed day, require one "
                    "extra confirming source in the dominant bucket before full weight."
                ),
                "do_instead": (
                    f"When total_score sign points {pred} but breadth/futures disagree, "
                    "cut conviction and prefer flat/mild over strong direction."
                ),
                "wrong_if": (
                    "Wrong if applying this hedge reduces direction accuracy over the "
                    "next 10 graded runs."
                ),
                "mag_hit": mag_hit,
            })
    return hypos


def _write_hypotheses(hypos: list[dict]) -> list[Path]:
    HYPO_DIR.mkdir(parents=True, exist_ok=True)
    paths = []
    for h in hypos:
        p = HYPO_DIR / f"{h['title']}.md"
        body = (
            f"---\n"
            f"kind: {h['kind']}\n"
            f"date: {h['date']}\n"
            f"status: open\n"
            f"---\n\n"
            f"# Hypothesis — {h['kind'].upper()} {h['date']}\n\n"
            f"## WHEN\n{h['when']}\n\n"
            f"## ASK (counterfactual)\n{h['ask']}\n\n"
            f"## EXPERIMENT\n{h['experiment']}\n\n"
            f"## DO INSTEAD (policy candidate)\n{h['do_instead']}\n\n"
            f"## WRONG IF (falsifier)\n{h['wrong_if']}\n"
        )
        p.write_text(body, encoding="utf-8")
        paths.append(p)
    return paths


def _promote_complete_candidates(min_market: int = 1) -> list[str]:
    """Promote complete candidates with a lower bar so policy can move.

    Market lessons: 1 complete candidate is enough to become active *draft*
    (was 2). Ops stays 1. Incomplete never promote.
    """
    from .promote_lessons import _parse_candidate, _write_active, _cluster

    paths = sorted(glob.glob(str(CAND_DIR / "*.md")))
    cands = [_parse_candidate(p) for p in paths]
    cands = [
        c for c in cands
        if c.get("status", "candidate") in ("candidate", "candidate_incomplete")
        and c.get("_complete")
    ]
    promoted = []
    for cl in _cluster(cands):
        complete = [c for c in cl if c["_complete"]]
        if not complete:
            continue
        cat = complete[0]["_norm"].get("error_category", "E")
        need = 1 if cat == "D" else min_market
        if len(complete) < need:
            continue
        # skip if an active file already covers same slug-ish when
        apath = _write_active(complete, merged_body="(learn_cycle promote)")
        promoted.append(apath)
    return promoted


def _rebuild_mutable_policy(runs: list[dict], hypos: list[dict], promoted: list[str]) -> None:
    today = datetime.now(ZoneInfo(config.TZ)).date().isoformat()
    active_parts = []
    for p in sorted(ACTIVE_DIR.glob("*.md")):
        if p.name.startswith("."):
            continue
        text = _read(p).strip()
        if not text:
            continue
        # compress to WHEN/DO/WRONG IF if possible
        when = re.search(r"WHEN[^\n]*\n(.*?)(?:\n##|\n---|
)", text, re.S | re.I)
        do = re.search(r"(?:RULE|DO INSTEAD|corrected)[^\n]*\n(.*?)(?:\n##|\n---|
)", text, re.S | re.I)
        active_parts.append(f"### {p.name}\n{text[:1200]}")

    wins = [h for h in hypos if h["kind"] == "win"]
    losses = [h for h in hypos if h["kind"] == "loss"]
    open_exp = []
    for h in hypos[-8:]:
        open_exp.append(
            f"- **{h['kind']} {h['date']}:** {h['experiment']}"
        )

    graded = [r for r in runs if r.get("direction_hit") is not None]
    n = len(graded)
    hits = sum(1 for r in graded if r.get("direction_hit") is True)
    acc = f"{100 * hits / n:.0f}%" if n else "n/a"

    body = f"""---
status: living_policy
updated: {today}
source: src/learn_cycle.py
note: Injected into PREDICT via memory. Core SCORES format unchanged.
---

# Mutable policy (standing adjustments)

Last learn_cycle: **{today}**. Graded window direction accuracy: **{acc}** (n={n}).
Promoted this cycle: {len(promoted)} lesson file(s).

## Active adjustments (from promoted lessons)

{chr(10).join(active_parts) if active_parts else "_(no active lessons)_"}

## Open experiments (test on next sessions)

{chr(10).join(open_exp) if open_exp else "_(none)_"}

## Win mining (do not only learn from losses)

Wins in window: **{len(wins)}**. For each win with magnitude miss, prefer milder bands when |score|<4.
Losses in window: **{len(losses)}**. Prefer confirmation hedge when score sign conflicts with breadth/futures.

## Methodology checklist (bot must answer in MEMORY_CONFIRM)

1. Did any open experiment apply today?
2. Are we missing a factor that would have flipped a recent loss?
3. Are we overweighting one bucket (double-count risk)?
4. Should weather stance for beta/short/size change after the last 5 days?

## Retired / falsified

_(append here when a falsifier triggers; learn_cycle does not auto-delete active files yet)_
"""
    MUTABLE.write_text(body, encoding="utf-8")
    print(f"[learn] wrote {MUTABLE}")


def _weather_proposals(runs: list[dict]) -> None:
    """Propose threshold nudges from recent accuracy — stored as proposals only."""
    graded = [r for r in runs if r.get("direction_hit") is not None]
    if len(graded) < 5:
        return
    hits = sum(1 for r in graded if r.get("direction_hit") is True)
    acc = hits / len(graded)
    proposals = {
        "updated": datetime.now(ZoneInfo(config.TZ)).isoformat(),
        "window_n": len(graded),
        "direction_acc": acc,
        "notes": [],
        "threshold_deltas": {},
    }
    if acc < 0.5:
        proposals["notes"].append(
            "Accuracy <50%: propose wider neutral band — raise risk_on_score and "
            "lower risk_off_score magnitude so fewer strong risk stances."
        )
        proposals["threshold_deltas"] = {
            "risk_on_score": +1.0,
            "risk_off_score": -1.0,
        }
    elif acc >= 0.65:
        proposals["notes"].append(
            "Accuracy healthy: no forced threshold change; keep experiments running."
        )
    PROPOSALS.write_text(json.dumps(proposals, indent=2), encoding="utf-8")
    print(f"[learn] weather proposals -> {PROPOSALS}")


def run(lookback: int = 15) -> None:
    runs = _graded_general(lookback=lookback)
    print(f"[learn] graded general runs: {len(runs)}")
    hypos = _hypotheses_from_runs(runs)
    paths = _write_hypotheses(hypos)
    print(f"[learn] hypotheses written: {len(paths)}")
    promoted = _promote_complete_candidates(min_market=1)
    print(f"[learn] promoted: {len(promoted)}")
    for p in promoted:
        print(f"  -> {p}")
    _rebuild_mutable_policy(runs, hypos, promoted)
    _weather_proposals(runs)
    print("[learn] done — next PREDICT will load mutable_policy.md via memory")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--lookback", type=int, default=15)
    args = ap.parse_args()
    run(lookback=args.lookback)


if __name__ == "__main__":
    main()
