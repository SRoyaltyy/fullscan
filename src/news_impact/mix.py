"""Multi-article mix: converge / conflict / singleton + high_mass.

Per ticker per session, stack surviving articles that name that ticker
(or carry a finviz ticker_hint). Not a new taxonomy.

- converge: ≥2 independent sources, same direction, no hard conflict
- conflict: up and down both present → mixed / not_determined, do not grade
- singleton: one article only

Weight: first-party government / regulator / court / 8-K / FOMC print
outranks a Finviz wrap of the same fact. high_mass is binary.
"""
from __future__ import annotations

import re
from collections import defaultdict
from typing import Any

from .grade import entry_calendar_date, signal_dt
from .grok_automations import is_grok_automations
from .hygiene import is_reaction_title

_FIRST_PARTY = re.compile(
    r"(?i)(\bsec\b|\b8-k\b|\bfomc\b|federal reserve|\bfed holds|\bfed (hikes|cuts)|"
    r"\bfda\b|chips act|\bdoj\b|\bcourt\b|class[- ]action|antitrust|"
    r"white house|treasury |exemptive relief)"
)
_FINVIZ_WRAP = re.compile(r"(?i)(finviz|elite_news|digest)")

_HIGH_MASS = (
    re.compile(r"(?i)(tokeniz|tsv\b|exemptive relief|innovation exemption)"),
    re.compile(r"(?i)(cyberattack|ransomware|data breach).{0,40}(guidance|outlook).{0,20}(withdraw|cut|pull)"),
    re.compile(r"(?i)(fda (approval|approves|label)|chips act award|wins? \$\d)"),
    re.compile(r"(?i)(class[- ]action|antitrust lawsuit).{0,40}(google|alphabet|microsoft|amazon|meta)"),
)

_MAJORS = frozenset({"GOOGL", "MSFT", "AMZN", "META", "NVDA", "AAPL"})


def is_first_party(row: dict) -> bool:
    src = str(row.get("source") or row.get("harvest_source") or "")
    if _FINVIZ_WRAP.search(src):
        return False  # a Finviz wrap of an SEC fact is still a wrap
    title = str(row.get("title") or "")
    if _FIRST_PARTY.search(title):
        return True
    if re.search(r"(?i)(sec\.gov|federalreserve|fda\.gov|justice\.gov|court)", src):
        return True
    return False


def is_high_mass(row: dict) -> bool:
    title = str(row.get("title") or "")
    ev = str((row.get("classification") or {}).get("event_class") or "")
    if ev in {"gate", "blast_cyber", "market_structure", "blast_legal"}:
        if any(p.search(title) for p in _HIGH_MASS):
            return True
    if ev == "blast_legal":
        ticks = {
            str(e.get("ticker") or "").upper()
            for e in (row.get("entities") or [])
            if isinstance(e, dict)
        }
        if ticks & _MAJORS:
            return True
    return any(p.search(title) for p in _HIGH_MASS)


def _independent_key(row: dict) -> str:
    hs = str(row.get("harvest_source") or row.get("source") or "unknown")
    if _FINVIZ_WRAP.search(hs):
        return "finviz_wrap"
    if is_grok_automations(row):
        return "grok_automations"
    if is_first_party(row):
        return "first_party"
    return hs.split(":")[0][:40]


def _survives(row: dict) -> bool:
    if is_reaction_title(str(row.get("title") or "")):
        return False
    cls = row.get("classification") or {}
    if cls.get("q5") == "regime" or cls.get("event_class") in {
        "discard", "regime_state", "rumor",
    }:
        return False
    return bool(row.get("usable"))


def _named_ticks(row: dict) -> list[tuple[str, str]]:
    """(ticker, direction) from entities plus finviz ticker_hint."""
    out: list[tuple[str, str]] = []
    seen: set[str] = set()
    for e in row.get("entities") or []:
        if not isinstance(e, dict):
            continue
        tick = str(e.get("ticker") or "").strip().upper()
        if not tick or tick in seen:
            continue
        seen.add(tick)
        out.append((tick, str(e.get("direction") or "not_determined")))
    hint = str(row.get("ticker_hint") or "").strip().upper()
    if hint and hint not in seen:
        # Map via finviz description/sector row — direction from conclusion if any.
        conc = row.get("conclusion") or {}
        direction = "not_determined"
        if any(hint in str(x) for x in (conc.get("up") or [])):
            direction = "up"
        elif any(hint in str(x) for x in (conc.get("down") or [])):
            direction = "down"
        out.append((hint, direction))
    return out


def _session_of(row: dict) -> str:
    for g in row.get("performance") or []:
        if isinstance(g, dict) and g.get("entry_date"):
            return str(g["entry_date"])
    when = signal_dt(row)
    if when:
        return entry_calendar_date(when)
    return "unknown"


def mix_book(results: list[dict]) -> dict[str, Any]:
    """Stack surviving articles per (ticker, session)."""
    groups: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for r in results:
        if not _survives(r):
            continue
        session = _session_of(r)
        for tick, direction in _named_ticks(r):
            groups[(tick, session)].append({
                "row": r,
                "direction": direction,
                "first_party": is_first_party(r),
                "high_mass": is_high_mass(r),
                "indep": _independent_key(r),
                "title": str(r.get("title") or "")[:180],
            })

    converge: list[dict] = []
    conflict: list[dict] = []
    singleton: list[dict] = []
    high_mass_n = 0

    for (tick, session), bag in sorted(groups.items()):
        dirs = {b["direction"] for b in bag if b["direction"] in {"up", "down"}}
        indep = {b["indep"] for b in bag}
        mass = any(b["high_mass"] for b in bag)
        if mass:
            high_mass_n += 1
        # First-party / grok_automations outrank a Finviz wrap of the same fact.
        fp = [b for b in bag if b["first_party"] and b["direction"] in {"up", "down"}]
        auto = [b for b in bag if b["indep"] == "grok_automations"
                and b["direction"] in {"up", "down"}]
        wraps = [b for b in bag if b["indep"] == "finviz_wrap"]
        preferred = fp or auto
        if preferred and wraps and {b["direction"] for b in preferred} != {
            b["direction"] for b in wraps if b["direction"] in {"up", "down"}
        }:
            bag = [
                b for b in bag
                if b["first_party"] or b["indep"] == "grok_automations"
                or b["indep"] != "finviz_wrap"
            ]
            dirs = {b["direction"] for b in bag if b["direction"] in {"up", "down"}}
            indep = {b["indep"] for b in bag}

        rec = {
            "ticker": tick,
            "session": session,
            "n_articles": len(bag),
            "n_independent": len(indep),
            "directions": sorted(dirs),
            "high_mass": mass,
            "mass": "high_mass" if mass else "normal",
            "titles": [b["title"] for b in bag[:6]],
            "sources": sorted(indep),
        }
        if "up" in dirs and "down" in dirs:
            rec["mix"] = "conflict"
            rec["direction"] = "mixed"
            rec["grade"] = False
            conflict.append(rec)
        elif len(bag) == 1:
            rec["mix"] = "singleton"
            rec["direction"] = next(iter(dirs), "not_determined")
            rec["grade"] = rec["direction"] in {"up", "down"}
            singleton.append(rec)
        elif len(indep) >= 2 and len(dirs) == 1:
            rec["mix"] = "converge"
            rec["direction"] = next(iter(dirs))
            rec["grade"] = True
            converge.append(rec)
        else:
            rec["mix"] = "singleton"
            rec["direction"] = next(iter(dirs), "not_determined")
            rec["grade"] = rec["direction"] in {"up", "down"}
            singleton.append(rec)

    return {
        "n_groups": len(groups),
        "converge_n": len(converge),
        "conflict_n": len(conflict),
        "singleton_n": len(singleton),
        "high_mass_n": high_mass_n,
        "converge": converge,
        "conflict": conflict,
        "singleton": singleton,
    }


def score_mix_books(
    results: list[dict],
    mix: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """0-1d hit rates for converge vs singleton. Conflict never grades as a trade."""
    mix = mix or mix_book(results)
    by_key: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for r in results:
        session = _session_of(r)
        for g in r.get("performance") or []:
            if not isinstance(g, dict):
                continue
            tick = str(g.get("ticker") or "").upper()
            if tick:
                by_key[(tick, session)].append(g)

    def _book(rows: list[dict]) -> dict[str, Any]:
        n = hit = 0
        for rec in rows:
            if not rec.get("grade"):
                continue
            legs = by_key.get((rec["ticker"], rec["session"])) or []
            # One vote per (ticker, session): majority of agree_1d on signed legs.
            votes = [
                g.get("agree_1d") for g in legs
                if g.get("agree_1d") is True or g.get("agree_1d") is False
            ]
            if not votes:
                continue
            n += 1
            hit += int(sum(1 for v in votes if v) > len(votes) / 2.0)
        return {
            "n_1d": n,
            "hit_1d": hit,
            "hit_rate_1d": round(hit / n, 4) if n else None,
        }

    return {
        "converge": _book(mix.get("converge") or []),
        "singleton": _book(mix.get("singleton") or []),
        "conflict_n": mix.get("conflict_n") or 0,
        "high_mass_n": mix.get("high_mass_n") or 0,
        "note": (
            "converge = ≥2 independent sources, same direction, no hard conflict. "
            "conflict stays mixed / ungraded. high_mass is binary (no multiplier)."
        ),
    }
