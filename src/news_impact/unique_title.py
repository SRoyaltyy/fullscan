"""Unique-title book: run the deterministic router on every unique title.

Not a date slice. Not the old 94 taped rows only. Missing tape = ungraded
and listed as "no tape" — never invented. Research-only. No Lane.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .backtest import overlay_existing_tape
from .corpus import (
    THEME_RADAR,
    dedupe_titles,
    funnel_from_results,
    inventory,
    load_all_sources,
)
from .grade import grade_results, performance_rollup
from .grok_automations import counts as grok_counts
from .hygiene import collapse_macro_stories
from .mix import mix_book, score_mix_books
from .pipeline import analyze_article
from .schema import Classification, Entity, is_tradable, is_usable

SCOREBOARD = Path("03_scoreboard/NEWS_IMPACT_UNIQUE_TITLE.md")
RATES = Path("01_daily/news/all_news_impact_unique_title.json")
CORPUS_MD = Path("03_scoreboard/NEWS_IMPACT_CORPUS_MIX.md")


def _pct(h, n) -> str:
    if not n:
        return "n/a"
    return f"{h}/{n} = {h / n:.1%}"


def _cls_ents(row: dict) -> tuple[Classification, list[Entity]]:
    cls = row.get("classification") or {}
    cobj = Classification(
        event_class=cls.get("event_class") or "",
        sign=cls.get("sign"),
        q5=cls.get("q5") or "regime",
        constraint=cls.get("constraint") or "",
    )
    eobjs = []
    for e in row.get("entities") or []:
        if not isinstance(e, dict):
            continue
        eobjs.append(Entity(
            name=e.get("name") or "x",
            ticker=e.get("ticker"),
            role=e.get("role") or "named",
            direction=e.get("direction") or "not_determined",
            tradeable_expression=e.get("tradeable_expression") or "direct",
        ))
    return cobj, eobjs


def is_signed_listed(row: dict) -> bool:
    cls, ents = _cls_ents(row)
    if (cls.q5 or "") != "impulse":
        return False
    if not is_usable(cls, ents) or not is_tradable(cls, ents):
        return False
    for e in ents:
        if e.direction in {"up", "down"} and (e.tradeable_expression or "direct") == "direct":
            if e.ticker:
                return True
    return False


def has_tape(row: dict) -> bool:
    for g in row.get("performance") or []:
        if not isinstance(g, dict):
            continue
        if g.get("ret_1d") is not None or g.get("ret_20d") is not None:
            return True
    return False


def no_tape_rows(results: list[dict]) -> list[dict]:
    """Signed listed expressions with no OHLC — ungraded, listed as no tape."""
    out = []
    for r in results:
        if not is_signed_listed(r):
            continue
        if has_tape(r):
            continue
        ticks = []
        for e in r.get("entities") or []:
            if not isinstance(e, dict):
                continue
            if e.get("direction") in {"up", "down"} and e.get("ticker"):
                ticks.append(f"{e.get('ticker')}:{e.get('direction')}")
        out.append({
            "title": str(r.get("title") or "")[:180],
            "source": str(r.get("harvest_source") or r.get("source") or ""),
            "published_at": str(r.get("published_at") or ""),
            "retrieved_at": str(r.get("retrieved_at") or ""),
            "event_class": (r.get("classification") or {}).get("event_class"),
            "tickers": ticks,
            "note": "no tape",
        })
    return out


def _fill_missing_tape(results: list[dict], fetch: bool) -> list[dict]:
    """Grade leftovers that are signed+listed. Do not invent tape."""
    need = [
        r for r in results
        if is_signed_listed(r) and not has_tape(r)
    ]
    if not need:
        return results
    graded = grade_results(need, fetch=fetch)
    by_title = {str(r.get("title") or ""): r.get("performance") for r in graded}
    for r in results:
        if not r.get("performance"):
            taped = by_title.get(str(r.get("title") or ""))
            if taped:
                r["performance"] = taped
    return results


def run(date: str = "all", fetch: bool = True) -> dict:
    inv = inventory()
    grok = grok_counts()
    raw, raw_meta = load_all_sources(date)
    unique = dedupe_titles(raw)
    results = []
    n_u = len(unique)
    for i, a in enumerate(unique, 1):
        results.append(analyze_article(a, use_lane=False, use_search=False, persist=False))
        if i % 5000 == 0 or i == n_u:
            print(f"[unique_title] routed {i}/{n_u}", flush=True)
    results = overlay_existing_tape(results)
    # Parquet first, then yfinance only for signed listed still missing tape.
    results = _fill_missing_tape(results, fetch=False)
    if fetch:
        results = _fill_missing_tape(results, fetch=True)
    tape = performance_rollup(results)
    funnel = funnel_from_results(raw_meta["n_raw"], unique, results)
    mix = mix_book(results)
    mix_scores = score_mix_books(results, mix)
    mh = tape.get("macro_headline") or collapse_macro_stories(results)
    guide = (tape.get("slices") or {}).get("guidance") or {}
    missing = no_tape_rows(results)
    signed_n = sum(1 for r in results if is_signed_listed(r))
    graded_n = funnel.get("has_tape_graded") or 0
    payload = {
        "date": date,
        "book": "unique_title_all",
        "lane": "deterministic",
        "inventory": inv,
        "grok_automations": grok,
        "raw_meta": raw_meta,
        "funnel": funnel,
        "signed_listed_n": signed_n,
        "graded_n": graded_n,
        "no_tape_n": len(missing),
        "no_tape": missing,
        "tape": {
            "n_1d": tape.get("n_1d"),
            "hit_1d": tape.get("hit_1d"),
            "hit_rate_1d": tape.get("hit_rate_1d"),
            "n_20d": tape.get("n_20d"),
            "hit_20d": tape.get("hit_20d"),
            "hit_rate_20d": tape.get("hit_rate_20d"),
            "missing_tape": tape.get("missing_tape"),
            "directional_calls": tape.get("directional_calls"),
        },
        "guidance_slice": {
            "n_1d": guide.get("n_1d"),
            "hit_1d": guide.get("hit_1d"),
            "n_20d": guide.get("n_20d"),
            "hit_20d": guide.get("hit_20d"),
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
        "theme_radar_ask": THEME_RADAR["export_ask"],
        "note": (
            "Graded 0-1d / 1-4w only when hygiene + horizon + listed "
            "expression + tape exist. Missing tape is ungraded (no tape). "
            "No invented tape. No fake five-digit graded n."
        ),
    }
    RATES.parent.mkdir(parents=True, exist_ok=True)
    RATES.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    lines = _markdown(payload)
    SCOREBOARD.write_text("\n".join(lines) + "\n", encoding="utf-8")
    _refresh_corpus_pointer(payload)
    print("unique title book", json.dumps({
        "raw": funnel["raw_headlines"],
        "unique": funnel["unique_after_dedupe"],
        "impulse": funnel["impulse_updown_listed"],
        "has_tape": funnel["has_tape_graded"],
        "no_tape": len(missing),
        "grok_files": grok["n_files"],
        "grok_items": grok["n_items"],
        "hit_1d": _pct(tape.get("hit_1d") or 0, tape.get("n_1d") or 0),
        "hit_20d": _pct(tape.get("hit_20d") or 0, tape.get("n_20d") or 0),
        "converge": mix["converge_n"],
        "singleton": mix["singleton_n"],
    }, indent=2))
    print("→", SCOREBOARD, RATES)
    return payload


def _markdown(payload: dict[str, Any]) -> list[str]:
    funnel = payload["funnel"]
    tape = payload["tape"]
    grok = payload["grok_automations"]
    mix = payload["mix"]
    scores = mix.get("scores") or {}
    mh = payload.get("macro_headline") or {}
    guide = payload.get("guidance_slice") or {}
    inv = payload.get("inventory") or {}
    window = (inv.get("window") or {})
    missing = payload.get("no_tape") or []
    lines = [
        "# News-impact unique-title book",
        "",
        "Deterministic router on **every unique title** (all sources, all dates). "
        "Not the 09-17 slice. Not only the old 94 taped rows. "
        "No Lane. Research-only.",
        "",
        "## Scoreboard",
        "",
        f"- **grok_automations**: files={grok.get('n_files')} "
        f"items={grok.get('n_items')} status={grok.get('status')} "
        f"(0 before the harvest wire; N after, including committed fixtures)",
        f"- **unique n**: {funnel.get('unique_after_dedupe')}",
        f"- **graded n** (has tape, 0-1d graded row): {funnel.get('has_tape_graded')}",
        f"- **signed listed**: {payload.get('signed_listed_n')}  "
        f"**no tape**: {payload.get('no_tape_n')}",
        f"- **converge** groups: {mix.get('converge_n')}  "
        f"0-1d {_pct((scores.get('converge') or {}).get('hit_1d') or 0, (scores.get('converge') or {}).get('n_1d') or 0)}",
        f"- **singleton** groups: {mix.get('singleton_n')}  "
        f"0-1d {_pct((scores.get('singleton') or {}).get('hit_1d') or 0, (scores.get('singleton') or {}).get('n_1d') or 0)}",
        "",
        "No fake five-digit graded n. Graded n is tape that exists.",
        "",
        "## Window",
        "",
        f"- earliest on disk: {window.get('earliest_on_disk')}. "
        f"earliest parse: {window.get('earliest_parse')}. "
        f"latest: {window.get('latest')}.",
        f"- June 2026 parse present: {window.get('june_2026_parse')}.",
        "",
        "## Honest funnel (after grok_automations wire)",
        "",
        f"- raw headlines (before title dedupe): **{funnel.get('raw_headlines')}**",
        f"- unique after title dedupe: **{funnel.get('unique_after_dedupe')}**",
        f"- non-reaction / non-weather: **{funnel.get('non_reaction_non_weather')}**",
        f"- impulse + up/down + listed expression: **{funnel.get('impulse_updown_listed')}**",
        f"- has tape (graded 0-1d row): **{funnel.get('has_tape_graded')}**",
        "",
        funnel.get("note") or "",
        "",
        "## Graded rates (hygiene + horizon + listed + tape)",
        "",
        f"- 0-1d: {_pct(tape.get('hit_1d') or 0, tape.get('n_1d') or 0)}",
        f"- 1-4w: {_pct(tape.get('hit_20d') or 0, tape.get('n_20d') or 0)}",
        f"- guidance slice 0-1d: {_pct(guide.get('hit_1d') or 0, guide.get('n_1d') or 0)}",
        f"- guidance slice 1-4w: {_pct(guide.get('hit_20d') or 0, guide.get('n_20d') or 0)}",
        f"- macro headline-level basket 0-1d (not legs): "
        f"{_pct(mh.get('hit_1d') or 0, mh.get('n_1d') or 0)} "
        f"(stories={mh.get('n_stories')}, reprints_collapsed={mh.get('reprints_collapsed')})",
        "",
        "Reaction kill, FOMC/macro collapse, and class-horizon skip are the "
        "#306 / #307 rules. Missing tape is **not** a miss.",
        "",
        "## Sources",
        "",
    ]
    for s in (inv.get("sources") or []):
        extra = ""
        if s.get("name") == "grok_automations":
            extra = f" items={s.get('n_items')}"
        lines.append(
            f"- **{s['name']}** `{s.get('path')}` — files={s.get('n_files')}{extra} "
            f"{s.get('earliest') or ''}..{s.get('latest') or ''} status={s['status']}"
        )
        if s.get("note"):
            lines.append(f"  - {s['note']}")
    lines += [
        "",
        "## Daily refresh (Grok Automations)",
        "",
        "GH Actions tokens cannot call the Automations API. Ingest via bot/Cursor "
        "(Gmail `noreply@x.ai` Automation mails, or `automation_get_results` when "
        "a connector exists), then `python3 scripts/ingest_grok_automations.py` "
        "and commit `data/grok_automations/{date}_{slug}.json`. "
        "See `docs/GROK_AUTOMATIONS_HARVEST.md`.",
        "",
        f"## No tape ({len(missing)} signed listed rows)",
        "",
        "These would be gradeable (impulse + up/down + listed expression) but "
        "have no OHLC. Listed, not graded. Tape was not invented.",
        "",
        "| # | Title | Tickers | Source | Note |",
        "| ---: | --- | --- | --- | --- |",
    ]
    for i, row in enumerate(missing, 1):
        title = str(row.get("title") or "").replace("|", "/")
        ticks = ", ".join(row.get("tickers") or []) or "—"
        src = str(row.get("source") or "")
        lines.append(f"| {i} | {title} | {ticks} | `{src}` | no tape |")
    if not missing:
        lines.append("| — | (none) | — | — | — |")
    lines += [
        "",
        "## Theme Radar ask (do not edit that repo)",
        "",
        THEME_RADAR["export_ask"],
        "",
    ]
    return lines


def _refresh_corpus_pointer(payload: dict) -> None:
    """Keep the #307 mix scoreboard from lying about grok_automations=empty."""
    if not CORPUS_MD.is_file():
        return
    text = CORPUS_MD.read_text(encoding="utf-8")
    grok = payload.get("grok_automations") or {}
    needle = (
        f"- **grok_automations** `data/grok_automations/*.json` — files=0 "
        f".. status=empty"
    )
    repl = (
        f"- **grok_automations** `data/grok_automations/{{date}}_{{slug}}.json` — "
        f"files={grok.get('n_files')} {grok.get('earliest') or ''}.."
        f"{grok.get('latest') or ''} status={grok.get('status')} "
        f"items={grok.get('n_items')}"
    )
    if needle in text:
        CORPUS_MD.write_text(text.replace(needle, repl, 1), encoding="utf-8")


if __name__ == "__main__":
    run()
