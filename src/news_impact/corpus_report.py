"""B+C+D restamp: all-sources funnel, mix books, horizon grid.

Research-only. No Lane. No live wire. Does not rewrite the 5657-row table.
"""
from __future__ import annotations

import json
from pathlib import Path

from .backtest import overlay_existing_tape
from .corpus import (
    THEME_RADAR,
    dedupe_titles,
    funnel_from_results,
    inventory,
    load_all_sources,
)
from .grade import grade_results, performance_rollup
from .horizons import format_window_grid, window_grid
from .hygiene import collapse_macro_stories
from .mix import mix_book, score_mix_books
from .pipeline import analyze_article

SCOREBOARD = Path("03_scoreboard/NEWS_IMPACT_CORPUS_MIX.md")
RATES = Path("01_daily/news/all_news_impact_corpus_mix.json")


def _pct(h, n) -> str:
    if not n:
        return "n/a"
    return f"{h}/{n} = {h / n:.1%}"


def run(date: str = "all", fetch: bool = False) -> dict:
    inv = inventory()
    raw, raw_meta = load_all_sources(date)
    unique = dedupe_titles(raw)
    results = [
        analyze_article(a, use_lane=False, use_search=False, persist=False)
        for a in unique
    ]
    results = overlay_existing_tape(results)
    # Grade leftovers from parquet only — no yfinance, no live wire.
    need = [r for r in results if not r.get("performance")]
    if need:
        graded = grade_results(need, fetch=fetch)
        by_title = {str(r.get("title") or ""): r.get("performance") for r in graded}
        for r in results:
            if not r.get("performance"):
                r["performance"] = by_title.get(str(r.get("title") or "")) or []
    tape = performance_rollup(results)
    funnel = funnel_from_results(raw_meta["n_raw"], unique, results)
    mix = mix_book(results)
    mix_scores = score_mix_books(results, mix)
    grid = window_grid(results)
    mh = tape.get("macro_headline") or collapse_macro_stories(results)
    guide = (tape.get("slices") or {}).get("guidance") or {}
    payload = {
        "date": date,
        "inventory": inv,
        "raw_meta": raw_meta,
        "funnel": funnel,
        "tape": {
            "n_1d": tape.get("n_1d"),
            "hit_1d": tape.get("hit_1d"),
            "hit_rate_1d": tape.get("hit_rate_1d"),
            "n_20d": tape.get("n_20d"),
            "hit_20d": tape.get("hit_20d"),
            "hit_rate_20d": tape.get("hit_rate_20d"),
            "slices": tape.get("slices"),
        },
        "guidance_slice": {
            "n_1d": guide.get("n_1d"),
            "hit_1d": guide.get("hit_1d"),
            "hit_rate_1d": guide.get("hit_rate_1d"),
            "n_20d": guide.get("n_20d"),
            "hit_20d": guide.get("hit_20d"),
            "hit_rate_20d": guide.get("hit_rate_20d"),
        },
        "macro_headline": {
            "n_1d": mh.get("headline_n_1d"),
            "hit_1d": mh.get("headline_hit_1d"),
            "hit_rate_1d": mh.get("headline_hit_rate_1d"),
            "reprints_collapsed": mh.get("reprints_collapsed"),
            "n_stories": mh.get("n_stories"),
        },
        "mix": {
            "converge_n": mix["converge_n"],
            "conflict_n": mix["conflict_n"],
            "singleton_n": mix["singleton_n"],
            "high_mass_n": mix["high_mass_n"],
            "scores": mix_scores,
        },
        "window_grid": grid,
        "theme_radar_ask": THEME_RADAR["export_ask"],
    }
    RATES.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    lines = [
        "# News-impact corpus + mix + horizons",
        "",
        "Research-only B+C+D on top of the #306 hygiene merge. "
        "Not a new taxonomy. Deterministic router. No Lane on 5657. "
        "theme-radar is read-only — no PR there.",
        "",
        "## Window",
        "",
        f"- earliest on disk: {inv['window']['earliest_on_disk']} "
        f"(Finviz export). earliest parse: {inv['window']['earliest_parse']}. "
        f"latest: {inv['window']['latest']}.",
        f"- June 2026 parse present: {inv['window']['june_2026_parse']}. "
        f"{inv['window']['note']}",
        "",
        "## Honest funnel",
        "",
        f"- raw headlines (all used sources, before title dedupe): **{funnel['raw_headlines']}**",
        f"- unique after title dedupe: **{funnel['unique_after_dedupe']}**",
        f"- non-reaction / non-weather: **{funnel['non_reaction_non_weather']}**",
        f"- impulse + up/down + listed expression: **{funnel['impulse_updown_listed']}**",
        f"- has tape (graded 0-1d row): **{funnel['has_tape_graded']}**",
        "",
        funnel["note"],
        "",
        "## Graded rates (same #305 + #306 cut, horizon-aware)",
        "",
        f"- 0-1d: {_pct(tape.get('hit_1d') or 0, tape.get('n_1d') or 0)}",
        f"- 1-4w: {_pct(tape.get('hit_20d') or 0, tape.get('n_20d') or 0)}",
        f"- guidance slice 0-1d (after reaffirm + 1-4w natural window): "
        f"{_pct(guide.get('hit_1d') or 0, guide.get('n_1d') or 0)}",
        f"- guidance slice 1-4w: {_pct(guide.get('hit_20d') or 0, guide.get('n_20d') or 0)}",
        f"- macro headline-level basket 0-1d (not legs): "
        f"{_pct(mh.get('headline_hit_1d') or 0, mh.get('headline_n_1d') or 0)} "
        f"(stories={mh.get('n_stories')}, reprints_collapsed={mh.get('reprints_collapsed')})",
        "",
        "## Convergence vs singleton 0-1d",
        "",
        f"- converge groups: {mix['converge_n']}  "
        f"0-1d {_pct((mix_scores['converge'] or {}).get('hit_1d') or 0, (mix_scores['converge'] or {}).get('n_1d') or 0)}",
        f"- singleton groups: {mix['singleton_n']}  "
        f"0-1d {_pct((mix_scores['singleton'] or {}).get('hit_1d') or 0, (mix_scores['singleton'] or {}).get('n_1d') or 0)}",
        f"- conflict groups (ungraded): {mix['conflict_n']}",
        f"- high_mass groups (binary flag, no multiplier): {mix['high_mass_n']}",
        "",
        "## Sources scanned",
        "",
    ]
    for s in inv["sources"]:
        lines.append(
            f"- **{s['name']}** `{s['path']}` — files={s.get('n_files')} "
            f"{s.get('earliest') or ''}..{s.get('latest') or ''} status={s['status']}"
        )
        if s.get("note"):
            lines.append(f"  - {s['note']}")
    lines += [
        "",
        "## Theme Radar ask (do not edit that repo)",
        "",
        THEME_RADAR["export_ask"],
        "",
        "## Horizon window grid (graded directional only)",
        "",
    ]
    lines += format_window_grid(grid)
    lines.append("")
    SCOREBOARD.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print("corpus mix", json.dumps({
        "raw": funnel["raw_headlines"],
        "unique": funnel["unique_after_dedupe"],
        "impulse": funnel["impulse_updown_listed"],
        "has_tape": funnel["has_tape_graded"],
        "hit_1d": _pct(tape.get("hit_1d") or 0, tape.get("n_1d") or 0),
        "guidance_20d": _pct(guide.get("hit_20d") or 0, guide.get("n_20d") or 0),
        "macro_headline": _pct(mh.get("headline_hit_1d") or 0, mh.get("headline_n_1d") or 0),
        "converge": mix["converge_n"],
        "singleton": mix["singleton_n"],
    }, indent=2))
    print("→", SCOREBOARD, RATES)
    return payload


if __name__ == "__main__":
    run()
