"""All-sources harvest inventory + loaders. Research-only. Not a new taxonomy.

fullscan paths are read from disk. theme-radar is READ ONLY via GitHub —
this module records the remote paths and does not clone or edit that repo.
"""
from __future__ import annotations

import csv
import json
import re
from collections import Counter
from pathlib import Path
from typing import Any

from .backtest import load_parsed
from .grok_automations import (
    counts as grok_counts,
    dump_span,
    load_grok_dumps,
    prefer_source,
)
from .hygiene import is_reaction_title
from .schema import is_tradable, is_usable

NEWS_DIR = Path("01_daily/news")
EVENTS_DIR = Path("01_daily/events")
EXPORT_DIR = Path("data/exports")
_DATE_IN_NAME = re.compile(r"(\d{4}-\d{2}-\d{2})")

THEME_RADAR = {
    "repo": "SRoyaltyy/theme-radar",
    "readonly": True,
    "paths": [
        "data/snapshots/YYYY-MM-DD.csv (Finviz Elite snapshots, 2026-08-06..2026-09-18)",
        "data/snapshots/manifest.json",
        "01_daily/*_scan.md / *_universe.md (no headline export)",
    ],
    "status": "unused_here — read-only; no clone, no PR",
    "export_ask": (
        "Theme Radar: please export a headline pack "
        "(date, ticker, News Title, News Time, Daily Digest, News URL) "
        "from data/snapshots/*.csv into a JSON/CSV we can ingest in fullscan. "
        "Do not merge the repos."
    ),
}


def _date_of(path: Path) -> str:
    m = _DATE_IN_NAME.search(path.name)
    return m.group(1) if m else ""


def _art(**kw) -> dict:
    title = str(kw.get("title") or "").strip()
    published = str(kw.get("published_at") or "").strip()
    retrieved = str(kw.get("retrieved_at") or "").strip()
    return {
        "title": title,
        "body": str(kw.get("body") or "")[:800],
        "url": str(kw.get("url") or ""),
        "source": str(kw.get("source") or ""),
        "harvest_source": str(kw.get("harvest_source") or kw.get("source") or ""),
        "source_file": str(kw.get("source_file") or ""),
        "published_at": published,
        "retrieved_at": retrieved,
        "known_at": published or retrieved,
        "ticker_hint": str(kw.get("ticker_hint") or "").upper(),
        "company": str(kw.get("company") or ""),
        "sectors": list(kw.get("sectors") or []),
        "macro_themes": list(kw.get("macro_themes") or []),
        "old_usable": kw.get("old_usable"),
        "old_class": str(kw.get("old_class") or ""),
    }


def load_finviz_exports(date: str | None = None) -> list[dict]:
    """News Title + Daily Digest from data/exports/finviz_YYYY-MM-DD.csv."""
    paths = sorted(EXPORT_DIR.glob("finviz_20*.csv"))
    if date and date.lower() not in {"all", "*", "history"}:
        paths = [p for p in paths if _date_of(p) == date]
    out: list[dict] = []
    for path in paths:
        retrieved = _date_of(path)
        try:
            with path.open(encoding="utf-8", newline="") as fh:
                reader = csv.DictReader(fh)
                for row in reader:
                    title = str(row.get("News Title") or "").strip()
                    if not title:
                        continue
                    digest = str(row.get("Daily Digest") or "").strip()
                    out.append(_art(
                        title=title,
                        body=digest,
                        url=str(row.get("News URL") or ""),
                        source="finviz_export",
                        harvest_source="finviz_export",
                        source_file=str(path),
                        published_at=str(row.get("News Time") or ""),
                        retrieved_at=retrieved,
                        ticker_hint=str(row.get("Ticker") or ""),
                        company=str(row.get("Company") or ""),
                        sectors=[str(row.get("Sector") or "")] if row.get("Sector") else [],
                    ))
        except (OSError, csv.Error, UnicodeDecodeError):
            continue
    return out


def load_finviz_digests(date: str | None = None) -> list[dict]:
    paths = sorted(NEWS_DIR.glob("*finviz*digest*.json"))
    if date and date.lower() not in {"all", "*", "history"}:
        paths = [p for p in paths if _date_of(p) == date]
    out: list[dict] = []
    for path in paths:
        try:
            blob = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        retrieved = str(blob.get("generated_at") or _date_of(path))
        for it in (blob.get("top_signal") or []) + (blob.get("index_digests") or []):
            if not isinstance(it, dict):
                continue
            title = str(it.get("news_title") or it.get("digest") or it.get("title") or "").strip()
            if not title:
                continue
            out.append(_art(
                title=title,
                body=str(it.get("digest") or ""),
                source=str(it.get("source") or "finviz_digest"),
                harvest_source="finviz_digest",
                source_file=str(path),
                retrieved_at=retrieved,
                ticker_hint=str(it.get("ticker") or ""),
                sectors=[str(it.get("sector") or "")] if it.get("sector") else [],
            ))
    return out


def load_events(date: str | None = None) -> list[dict]:
    paths = sorted(EVENTS_DIR.glob("*_events.json"))
    if date and date.lower() not in {"all", "*", "history"}:
        paths = [p for p in paths if _date_of(p) == date]
    out: list[dict] = []
    for path in paths:
        try:
            blob = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        retrieved = str(blob.get("scan_date") or _date_of(path))
        for it in blob.get("events") or []:
            if not isinstance(it, dict):
                continue
            title = str(it.get("title") or "").strip()
            if not title:
                continue
            srcs = it.get("sources") or []
            url = srcs[0] if srcs and isinstance(srcs[0], str) else ""
            out.append(_art(
                title=title,
                body=str(it.get("why_it_matters") or ""),
                url=url,
                source="events",
                harvest_source="events",
                source_file=str(path),
                published_at=str(it.get("date_or_window") or "")[:10],
                retrieved_at=retrieved,
                sectors=list(it.get("sectors") or []),
            ))
    return out


def load_actions_keep(date: str | None = None) -> list[dict]:
    """KEEP / reasoned event evidence titles from *_actions.json."""
    paths = sorted(NEWS_DIR.glob("*_actions.json"))
    if date and date.lower() not in {"all", "*", "history"}:
        paths = [p for p in paths if _date_of(p) == date]
    out: list[dict] = []
    for path in paths:
        try:
            blob = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        retrieved = str(blob.get("generated_at") or _date_of(path))
        for ev in blob.get("reasoned_events") or []:
            if not isinstance(ev, dict):
                continue
            keep = str((ev.get("framework") or {}).get("keep") or "").lower()
            if keep not in {"keep", "conditional"}:
                continue
            for evd in ev.get("evidence") or []:
                if not isinstance(evd, dict):
                    continue
                title = str(evd.get("title") or "").strip()
                if not title:
                    continue
                out.append(_art(
                    title=title,
                    url=str(evd.get("url") or ""),
                    source=str(evd.get("source") or "actions"),
                    harvest_source="actions_keep",
                    source_file=str(path),
                    published_at=str(evd.get("published_at") or ""),
                    retrieved_at=retrieved,
                    old_class=f"actions:{keep}",
                ))
    return out


def load_parsed_tagged(date: str | None = None) -> list[dict]:
    arts: list[dict] = []
    if date and date.lower() not in {"all", "*", "history"}:
        paths = [NEWS_DIR / f"{date}_parsed.json"]
    else:
        paths = sorted(NEWS_DIR.glob("*_parsed.json"))
    for p in paths:
        for a in load_parsed(p):
            a["harvest_source"] = "parsed"
            arts.append(a)
    return arts


def inventory() -> dict[str, Any]:
    """Honest scan of every news-touching path. Empty vs unused vs used."""
    parsed = sorted(NEWS_DIR.glob("*_parsed.json"))
    exports = sorted(EXPORT_DIR.glob("finviz_20*.csv"))
    digests = sorted(NEWS_DIR.glob("*finviz*digest*.json"))
    events = sorted(EVENTS_DIR.glob("*_events.json"))
    actions = sorted(NEWS_DIR.glob("*_actions.json"))
    grok_meta = grok_counts()
    grok_early, grok_late = dump_span()
    rss_dumps = list(Path("data").glob("*rss*")) + list(Path("01_daily/news").glob("*rss*"))
    supabase = list(Path("data").glob("*supabase*")) + list(Path("01_daily").glob("*supabase*"))
    from .theme_radar import snapshot_paths
    snaps = snapshot_paths(None)
    snap_dates = [_date_of(p) for p in snaps if _date_of(p)]

    def _span(paths: list[Path]) -> tuple[str, str]:
        dates = [_date_of(p) for p in paths if _date_of(p)]
        return (min(dates) if dates else "", max(dates) if dates else "")

    sources = [
        {
            "name": "parsed_json",
            "path": "01_daily/news/*_parsed.json",
            "n_files": len(parsed),
            "earliest": _span(parsed)[0],
            "latest": _span(parsed)[1],
            "status": "used" if parsed else "empty",
        },
        {
            "name": "finviz_export",
            "path": "data/exports/finviz_YYYY-MM-DD.csv (News Title + Daily Digest)",
            "n_files": len(exports),
            "earliest": _span(exports)[0],
            "latest": _span(exports)[1],
            "status": "used" if exports else "empty",
        },
        {
            "name": "finviz_digest",
            "path": "01_daily/news/*finviz*digest*.json",
            "n_files": len(digests),
            "earliest": _span(digests)[0],
            "latest": _span(digests)[1],
            "status": "used" if digests else "empty",
        },
        {
            "name": "events_json",
            "path": "01_daily/events/*_events.json",
            "n_files": len(events),
            "earliest": _span(events)[0],
            "latest": _span(events)[1],
            "status": "used" if events else "empty",
        },
        {
            "name": "actions_keep",
            "path": "01_daily/news/*_actions.json (KEEP / conditional evidence)",
            "n_files": len(actions),
            "earliest": _span(actions)[0],
            "latest": _span(actions)[1],
            "status": "used" if actions else "empty",
        },
        {
            "name": "grok_automations",
            "path": "data/grok_automations/{date}_{slug}.json",
            "n_files": grok_meta["n_files"],
            "n_items": grok_meta["n_items"],
            "earliest": grok_early,
            "latest": grok_late,
            "status": grok_meta["status"],
            "note": (
                "GH Actions tokens cannot call the Automations API. "
                "Ingest via bot/Cursor (Gmail noreply@x.ai or "
                "automation_get_results) then commit dumps. "
                "See docs/GROK_AUTOMATIONS_HARVEST.md."
            ),
        },
        {
            "name": "rss_dumps",
            "path": "collectors/rss_news.py (workflow exists; no dump dir on disk)",
            "n_files": len(rss_dumps),
            "status": "empty",
        },
        {
            "name": "supabase_dumps",
            "path": "src/db.py news pooler (no local dump on disk)",
            "n_files": len(supabase),
            "status": "empty",
        },
        {
            "name": "theme_radar_snapshots",
            "path": (
                "data/theme_radar_snapshots/*.csv.gz or "
                "vendor/theme-radar/data/snapshots/*.csv "
                "(read-only; repos are not merged)"
            ),
            "n_files": len(snaps),
            "earliest": min(snap_dates) if snap_dates else "",
            "latest": max(snap_dates) if snap_dates else "",
            "status": "used" if snaps else "unused_readonly",
            "note": (
                "Slim Elite headlines (Ticker, News Title, Daily Digest, News Time) "
                "are in the book when these files are present. "
                "Do not merge the repos. "
                "Scoreboard: 03_scoreboard/NEWS_IMPACT_THEME_RADAR.md."
                if snaps else THEME_RADAR["export_ask"]
            ),
        },
    ]
    return {
        "window": {
            "earliest_on_disk": "2026-04-26",
            "earliest_parse": "2026-08-08",
            "latest": "2026-09-21",
            "june_2026_parse": False,
            "note": (
                "No June 2026 *_parsed.json. Earliest parse is 2026-08-08. "
                "Earliest Finviz export is 2026-04-26 (one file). "
                "Window used = earliest on-disk news through latest."
            ),
        },
        "sources": sources,
        "theme_radar": THEME_RADAR,
    }


def load_all_sources(date: str | None = None) -> tuple[list[dict], dict[str, Any]]:
    """Raw articles from every used fullscan path. Caller dedupes."""
    raw: list[dict] = []
    raw.extend(load_parsed_tagged(date))
    raw.extend(load_finviz_exports(date))
    raw.extend(load_finviz_digests(date))
    raw.extend(load_events(date))
    raw.extend(load_actions_keep(date))
    raw.extend(load_grok_dumps(date=date))
    from .theme_radar import load_theme_radar
    named = bool(date and str(date).lower() not in {"all", "*", "history", ""})
    # --date all reads on-disk snapshots only (no remote fan-out).
    raw.extend(load_theme_radar(date, allow_remote=named))
    by_src = Counter(a.get("harvest_source") or a.get("source") or "?" for a in raw)
    return raw, {"n_raw": len(raw), "by_harvest_source": dict(by_src)}


def dedupe_titles(arts: list[dict]) -> list[dict]:
    """Unique titles. Same fact: grok_automations outranks a Finviz wrap."""
    return prefer_source(arts)


def funnel_from_results(
    raw_n: int,
    unique: list[dict],
    results: list[dict],
) -> dict[str, Any]:
    """Honest funnel. Do not invent a five-digit n from 5657 wraps."""
    non_rx = 0
    impulse = 0
    has_tape = 0
    for r in results:
        title = str(r.get("title") or "")
        cls = r.get("classification") or {}
        ev = cls.get("event_class") or ""
        q5 = cls.get("q5") or ""
        reaction = is_reaction_title(title)
        weather = q5 == "regime" or ev in {"discard", "regime_state", "rumor"}
        if not reaction and not weather:
            non_rx += 1
        ents = r.get("entities") or []
        from .schema import Classification, Entity
        cobj = Classification(
            event_class=ev, sign=cls.get("sign"), q5=q5 or "regime",
            constraint=cls.get("constraint") or "",
        )
        eobjs = [
            Entity(
                name=e.get("name") or "x",
                ticker=e.get("ticker"),
                role=e.get("role") or "named",
                direction=e.get("direction") or "not_determined",
                tradeable_expression=e.get("tradeable_expression") or "direct",
            )
            for e in ents if isinstance(e, dict)
        ]
        if (
            q5 == "impulse"
            and is_usable(cobj, eobjs)
            and is_tradable(cobj, eobjs)
        ):
            impulse += 1
        for g in r.get("performance") or []:
            if isinstance(g, dict) and g.get("ret_1d") is not None and g.get("graded"):
                has_tape += 1
                break
    return {
        "raw_headlines": raw_n,
        "unique_after_dedupe": len(unique),
        "non_reaction_non_weather": non_rx,
        "impulse_updown_listed": impulse,
        "has_tape_graded": has_tape,
        "note": (
            "has_tape_graded counts unique articles with at least one graded "
            "0-1d tape row. If this is only hundreds, that is the truth — "
            "the five-digit target needs unused/empty sources (theme-radar "
            "headline export, RSS/Supabase dumps, June parses) to fill."
        ),
    }
