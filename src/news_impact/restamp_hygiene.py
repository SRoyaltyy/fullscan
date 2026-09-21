"""Recompute news-impact tape rates after harvest hygiene.

Re-runs the deterministic router on the parsed corpus, overlays the
already-graded tape from the committed all-dates JSON, then writes a
lean hygiene artifact (no 5657-row dump).
"""
from __future__ import annotations

import json
from pathlib import Path

from .backtest import overlay_existing_tape, run_backtest
from .grade import performance_rollup

NEWS_DIR = Path("01_daily/news")
SCOREBOARD = Path("03_scoreboard/NEWS_IMPACT_HARVEST_HYGIENE.md")
RATES_JSON = Path("01_daily/news/all_news_impact_hygiene.json")
ARTIFACT = Path("01_daily/news/all_news_impact_backtest.json")


def _pct(h, n) -> str:
    if not n:
        return "n/a"
    return f"{h}/{n} = {h / n:.1%}"


def run(date: str = "all") -> dict:
    report = run_backtest(date, persist=False, keep_results=True)
    results = overlay_existing_tape(report.get("results") or [], ARTIFACT)
    report["results"] = results
    tape = performance_rollup(results)
    report["tape"] = tape
    mh = tape.get("macro_headline") or {}
    guide = (tape.get("slices") or {}).get("guidance") or {}
    rates = {
        "date": date,
        "harvested": report.get("harvested"),
        "new": report.get("new"),
        "rescued_n": report.get("rescued_n"),
        "killed_n": report.get("killed_n"),
        "hit_1d": _pct(tape.get("hit_1d") or 0, tape.get("n_1d") or 0),
        "hit_20d": _pct(tape.get("hit_20d") or 0, tape.get("n_20d") or 0),
        "guidance_1d": _pct(guide.get("hit_1d") or 0, guide.get("n_1d") or 0),
        "guidance_20d": _pct(guide.get("hit_20d") or 0, guide.get("n_20d") or 0),
        "macro_headline_1d": _pct(
            mh.get("headline_hit_1d") or 0, mh.get("headline_n_1d") or 0,
        ),
        "macro_headline_20d": _pct(
            mh.get("headline_hit_20d") or 0, mh.get("headline_n_20d") or 0,
        ),
        "macro_legs_1d": _pct(mh.get("leg_hit_1d") or 0, mh.get("leg_n_1d") or 0),
        "macro_stories": mh.get("n_stories"),
        "macro_reprints_collapsed": mh.get("reprints_collapsed"),
        "tape": tape,
        "grade_rule": tape.get("grade_rule"),
        "macro_rule": mh.get("grade_rule"),
    }
    RATES_JSON.write_text(json.dumps(rates, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    lines = [
        "# News-impact harvest + label hygiene",
        "",
        "Research-only restamp after the five hygiene filters. "
        "Not a new taxonomy. Deterministic router preserved. "
        "0-1d / 1-4w still use the #305 grade cut "
        "(`q5=impulse`, direction `up`/`down`, `tradeable_expression=direct`).",
        "",
        f"articles={report.get('harvested')}  usable={((report.get('new') or {}).get('usable'))}  "
        f"tradable={((report.get('new') or {}).get('tradable'))}",
        "",
        "## Graded rates after hygiene",
        "",
        f"- **0-1d** hit rate: {rates['hit_1d']}",
        f"- **1-4w** hit rate: {rates['hit_20d']}",
        f"- **guidance slice** 0-1d (after sign hygiene): {rates['guidance_1d']}",
        f"- **guidance slice** 1-4w: {rates['guidance_20d']}",
        f"- **macro headline-level basket** 0-1d (not legs): {rates['macro_headline_1d']}",
        f"- **macro headline-level basket** 1-4w: {rates['macro_headline_20d']}",
        f"- macro stories={rates['macro_stories']}  reprints_collapsed="
        f"{rates['macro_reprints_collapsed']}  "
        f"legs (transparency only) 0-1d {rates['macro_legs_1d']}",
        "",
        "## What the columns mean",
        "",
        "- **0-1d / 1-4w** = graded directional calls only. "
        "`factor_impulse` legs, `mixed` / `not_determined`, reaction titles, "
        "and long-horizon 0-1d skips are out of these denominators.",
        "- **guidance slice** = same grade cut, after reaffirm→None and "
        "raise+miss→mixed. Reaffirm rows are ungraded context.",
        "- **macro headline basket** = one row per `(factor, session, sign)`. "
        "Hit = majority of QQQ/TLT/UUP/HYG/SPY agreeing with the implied sign. "
        "This is **not** added into the graded 0-1d / 1-4w columns. "
        "Leg counts are transparency only.",
        "",
        "## Filters",
        "",
        "1. Reaction titles (plunge / surge N% / rebound / falls N% / "
        "drives N% drop) → `event_class=discard`, `q5=regime`, `entities=[]`.",
        "2. Guidance: reaffirm / maintains guidance → `sign=None`, "
        "`direction=not_determined`. Raise + miss EPS → mixed / split, never a single UP.",
        "3. `entry_clock=published` when the source Published timestamp parses; "
        "else `retrieved_only`. Published is never invented.",
        "4. Macro reprints collapse to `(factor, session, sign)`.",
        "5. gate / capacity / blast_cyber / CHIPS-style awards skip 0-1d; "
        "they may still count in 1-4w.",
        "",
    ]
    SCOREBOARD.write_text("\n".join(lines) + "\n", encoding="utf-8")
    # Keep the long human table in sync on the two dated books + scoreboard header.
    # Full 5657-row rewrite of all_news_impact_backtest.md is left to the
    # specialised action; the hygiene rates above are the restamp.
    print("hygiene rates", json.dumps({
        k: rates[k] for k in (
            "hit_1d", "hit_20d", "guidance_1d", "macro_headline_1d",
            "macro_stories", "macro_reprints_collapsed",
        )
    }, indent=2))
    print("→", SCOREBOARD, RATES_JSON)
    return rates


if __name__ == "__main__":
    run()
