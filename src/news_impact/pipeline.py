"""One article → classification → optional search/Lane → entities + watermark."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from . import scratch
from .classify import classify_article, extract_named, merge_lane_class, rank_articles
from .families import analyze, axioms_for
from .hygiene import entry_clock_of
from .schema import (
    PIPELINE_VERSION,
    Classification,
    Entity,
    is_tradable,
    is_usable,
)
from .search_pack import pack_for_article

DETERMINISTIC_MARK = "deterministic"
DETERMINISTIC_MODEL = PIPELINE_VERSION

FAMILY_TEST = {
    "blast": "named is hurt; substitutes / unscathed rivals / arms dealers only if an axiom applies",
    "structure": "who is newly allowed or newly blocked; incumbents vs new venues",
    "quantity": "who pays the unit vs who sells it; capacity add hurts incumbents",
    "permission": "gate open = named up; shut = named down",
    "print": "gap vs priced; ticker-less macro trades the factor basket (QQQ/TLT/UUP/XLE), not a name in the lede",
    "firm": "issuer-level action; no sector fishing",
    "flow": "listing / index / forced flow on the named name",
    "time": "regime_break flips the constraint; regime/discard emit nothing",
}


def _watermark(lane: str, model: str) -> dict[str, str]:
    return {
        "lane": lane,
        "model": model,
        "inference_source": lane,
    }


def _lane_excerpt(parsed: dict | None) -> str:
    if not isinstance(parsed, dict):
        return ""
    bits: list[str] = []
    if parsed.get("event_class") or parsed.get("q5"):
        bits.append(
            f"class={parsed.get('event_class')} q5={parsed.get('q5')} "
            f"sign={parsed.get('sign')}"
        )
    if parsed.get("why"):
        bits.append(str(parsed["why"])[:160])
    if parsed.get("constraint"):
        bits.append(f"constraint={str(parsed['constraint'])[:120]}")
    ents = parsed.get("entities")
    if isinstance(ents, list) and ents:
        bits.append("ents=" + ",".join(
            f"{(e or {}).get('ticker') or (e or {}).get('name')}:"
            f"{(e or {}).get('direction')}"
            for e in ents[:8] if isinstance(e, dict)
        ))
    return " | ".join(bits)[:400]


def _lane_hop(tmpl: str, art: dict, family: str = "", cls: Classification | None = None,
              pack: dict | None = None) -> tuple[dict | None, str, str, list[dict]]:
    """Optional Lane hop. Returns (parsed, lane, model, hop_log). Fail soft."""
    try:
        from src import lane_route as lane
    except Exception:
        return None, "", "", [{
            "role": tmpl, "lane": "", "model": "", "ok": False, "skip": "import",
        }]
    keys, ollama_url, gh_direct = lane.load_keys()
    if not keys and not ollama_url and not gh_direct:
        return None, "", "", [{
            "role": tmpl, "lane": "", "model": "", "ok": False, "skip": "no_keys",
        }]
    ctx = {"keys": keys, "ollama_url": ollama_url, "gh_direct": gh_direct}
    from .prompts import analyst_prompt, classifier_prompt
    if tmpl == "news_classify":
        prompt = classifier_prompt(
            str(art.get("title") or ""),
            str(art.get("body") or ""),
            str(art.get("known_at") or art.get("published_at") or ""),
        )
        system = lane.system_for("news_classify")
        budget = lane.token_budget("news_classify")
    else:
        prompt = analyst_prompt(
            str(art.get("title") or ""),
            str(art.get("body") or ""),
            family,
            cls.event_class if cls else "",
            cls.sign if cls else None,
            cls.q5 if cls else "impulse",
            cls.constraint if cls else "",
            (pack or {}).get("facts") or [],
        )
        system = lane.system_for("news_impact")
        budget = lane.token_budget("news_impact")
    hops: list[dict] = []
    for hop in lane.lanes_for(tmpl):
        parsed, model = lane.ask_lane(
            hop, prompt, ctx, max_tokens=budget, system=system, tmpl=tmpl,
        )
        if parsed is None:
            hops.append({
                "role": tmpl, "lane": hop, "model": str(model or ""),
                "ok": False, "skip": "empty",
            })
            continue
        if lane.is_banned_primary(model):
            hops.append({
                "role": tmpl, "lane": hop, "model": str(model or ""),
                "ok": False, "skip": "banned_primary",
            })
            continue
        hops.append({
            "role": tmpl, "lane": hop, "model": str(model or hop),
            "ok": True, "excerpt": _lane_excerpt(parsed),
        })
        return parsed, hop, str(model or hop), hops
    return None, "", "", hops


def _entities_from_lane(parsed: dict) -> list[Entity] | None:
    raw = parsed.get("entities") if isinstance(parsed, dict) else None
    if not isinstance(raw, list) or not raw:
        return None
    out: list[Entity] = []
    for row in raw:
        if not isinstance(row, dict):
            continue
        direction = str(row.get("direction") or "not_determined")
        if direction not in ("up", "down", "mixed", "not_determined"):
            direction = "not_determined"
        tick = row.get("ticker")
        if tick in ("", "null", "none"):
            tick = None
        out.append(Entity(
            name=str(row.get("name") or tick or "unknown"),
            ticker=str(tick).upper() if tick else None,
            role=str(row.get("role") or "named"),
            direction=direction,
            horizon=str(row.get("horizon") or "0-1d"),
            named_unit=row.get("named_unit"),
            unit_vs_parent=str(row.get("unit_vs_parent") or "whole"),
            tradeable_expression=str(row.get("tradeable_expression") or "direct"),
            relation=str(row.get("relation") or "primary"),
            if_unknown=row.get("if_unknown"),
            axiom_id=row.get("axiom_id"),
            inferred=bool(row.get("inferred")),
        ))
    return out or None


def _conclusion(entities: list[Entity]) -> dict[str, list[str]]:
    bags: dict[str, list[str]] = {
        "up": [], "down": [], "mixed": [], "not_determined": [],
    }
    for e in entities:
        label = e.ticker or e.name
        extra = f" ({e.role})" if e.role and e.role != "named" else ""
        bags.setdefault(e.direction, []).append(f"{label}{extra}")
    return bags


def _models_from_hops(hop_chain: list[dict], mark: dict) -> list[str]:
    seen: list[str] = []
    for h in hop_chain:
        if not h.get("ok"):
            continue
        token = f"{h.get('lane') or '?'}::{h.get('model') or '?'}"
        if token not in seen:
            seen.append(token)
    if not seen:
        seen.append(f"{mark.get('lane')}::{mark.get('model')}")
    return seen


def _reasoning(
    art: dict,
    cls: Classification,
    entities: list[Entity],
    pack: dict,
    hop_chain: list[dict],
    axioms: list[dict],
) -> list[str]:
    named = extract_named(str(art.get("title") or ""), str(art.get("body") or ""))
    named_s = ", ".join(f"{t}" for t, _ in named[:8]) or "none"
    axiom_s = "; ".join(
        f"{a.get('id')}: {a.get('text')}" for a in (axioms or [])[:6]
    ) or "none retrieved"
    hops = []
    for h in hop_chain:
        if h.get("role") == "router":
            hops.append(
                f"router {h.get('lane')}::{h.get('model')} — {h.get('excerpt') or 'deterministic'}"
            )
            continue
        if h.get("ok"):
            hops.append(
                f"{h.get('role')} {h.get('lane')}::{h.get('model')} — {h.get('excerpt') or 'ok'}"
            )
        else:
            hops.append(
                f"{h.get('role') or 'lane'} {h.get('lane') or 'hopper'} "
                f"skip={h.get('skip') or 'empty'}"
            )
    facts = pack.get("facts") or []
    fact_s = "; ".join(str(f)[:120] for f in facts[:4]) if facts else "off / none"
    conc = _conclusion(entities)
    return [
        f"Q5 {cls.q5} — constraint: {cls.constraint or '(none)'}",
        f"class {cls.event_class} / family {cls.family} / sign {cls.sign} — {cls.why}",
        f"named in title: {named_s}",
        f"family test ({cls.family}): {FAMILY_TEST.get(cls.family, cls.family)}",
        f"axioms: {axiom_s}",
        f"search ({pack.get('backend') or 'off'}): {fact_s}",
        "hops: " + " → ".join(hops),
        (
            f"conclusion UP {', '.join(conc['up']) or '—'} · "
            f"DOWN {', '.join(conc['down']) or '—'} · "
            f"MIXED {', '.join(conc['mixed']) or '—'} · "
            f"ND {', '.join(conc['not_determined']) or '—'}"
        ),
    ]


def analyze_article(
    art: dict,
    use_lane: bool = False,
    use_search: bool = False,
    persist: bool = True,
) -> dict[str, Any]:
    title = str(art.get("title") or "")
    body = str(art.get("body") or "")
    # Honesty: never copy retrieved into published_at.
    published = str(art.get("published_at") or "")
    retrieved = str(art.get("retrieved_at") or "")
    known = str(art.get("known_at") or published or retrieved or "")
    entry_clock = entry_clock_of(art, published_at=published)
    aid = scratch.article_id(title, known)
    cls = classify_article(art)
    mark = _watermark(DETERMINISTIC_MARK, DETERMINISTIC_MODEL)
    hop_chain: list[dict] = [{
        "role": "router",
        "lane": DETERMINISTIC_MARK,
        "model": DETERMINISTIC_MODEL,
        "ok": True,
        "excerpt": f"q5={cls.q5} class={cls.event_class} sign={cls.sign} — {cls.why}",
    }]
    if use_lane:
        lane_cls_parsed, hop, model, hops = _lane_hop("news_classify", art)
        hop_chain.extend(hops)
        if lane_cls_parsed:
            cls = merge_lane_class(cls, lane_cls_parsed)
            mark = _watermark(hop, model)

    pack = pack_for_article(title, body, enabled=use_search)
    entities = analyze(art, cls, pack)
    if use_lane and cls.q5 != "regime" and cls.event_class not in {
        "discard", "regime_state", "rumor",
    }:
        lane_an, hop, model, hops = _lane_hop(
            "news_impact", art, family=cls.family, cls=cls, pack=pack,
        )
        hop_chain.extend(hops)
        lane_ents = _entities_from_lane(lane_an or {})
        if lane_ents:
            entities = lane_ents
            mark = _watermark(hop, model)

    usable = is_usable(cls, entities)
    tradable = is_tradable(cls, entities)
    axioms = axioms_for(entities)
    reasoning = _reasoning(art, cls, entities, pack, hop_chain, axioms)
    models = _models_from_hops(hop_chain, mark)
    row = {
        "article_id": aid,
        "title": title[:300],
        "body": body[:800],
        "source": art.get("source") or "",
        "url": art.get("url") or "",
        "source_file": art.get("source_file") or art.get("source") or "",
        "published_at": published,
        "retrieved_at": retrieved,
        "known_at": known,
        "entry_clock": entry_clock,
        "sectors": art.get("sectors") or [],
        "macro_themes": art.get("macro_themes") or [],
        "pipeline_version": PIPELINE_VERSION,
        "analyzed_at": datetime.now(timezone.utc).isoformat(),
        "classification": cls.to_dict(),
        "q5": {
            "constraint": cls.constraint,
            "status": cls.q5,
            "what_would_count_as_new": (
                "verified change in the named constraint, not a reprint"
            ),
        },
        "search": {
            "backend": pack.get("backend"),
            "n_facts": len(pack.get("facts") or []),
            "errors": pack.get("errors") or [],
        },
        "axioms_used": axioms,
        "entities": [e.to_dict() for e in entities],
        "conclusion": _conclusion(entities),
        "hop_chain": hop_chain,
        "models": models,
        "reasoning": reasoning,
        "usable": usable,
        "tradable": tradable,
        "macro_factor": cls.factor or "",
        "old_usable": art.get("old_usable"),
        "old_class": art.get("old_class"),
        **mark,
    }
    if persist:
        scratch.save({
            "article_id": aid,
            "title": title[:300],
            "q5": cls.q5,
            "event_class": cls.event_class,
            "sign": cls.sign,
            "pack_complete": not any(e.if_unknown for e in entities),
            "entities": row["entities"],
            "usable": usable,
            **mark,
        })
    return row


def analyze_many(
    arts: list[dict],
    limit: int = 0,
    use_lane: bool = False,
    use_search: bool = False,
    persist: bool = True,
    ranked: bool = True,
) -> list[dict]:
    rows = rank_articles(arts) if ranked else list(arts)
    if limit and limit > 0:
        rows = rows[:limit]
    return [
        analyze_article(a, use_lane=use_lane, use_search=use_search, persist=persist)
        for a in rows
    ]


def rollup(results: list[dict]) -> dict:
    from collections import Counter
    bull, bear = Counter(), Counter()
    hops = Counter()
    classes = Counter()
    usable = 0
    tradable = 0
    for r in results:
        if r.get("usable"):
            usable += 1
        if r.get("tradable"):
            tradable += 1
        ev = ((r.get("classification") or {}).get("event_class")) or ""
        if ev:
            classes[ev] += 1
        hops[f"{r.get('lane')}::{r.get('model')}"] += 1
        for e in r.get("entities") or []:
            tick = e.get("ticker") or e.get("name")
            if e.get("direction") == "up":
                bull[tick] += 1
            elif e.get("direction") == "down":
                bear[tick] += 1
    return {
        "n": len(results),
        "usable": usable,
        "tradable": tradable,
        "discarded": len(results) - usable,
        "usable_ratio": round(usable / len(results), 4) if results else 0.0,
        "tradable_ratio": round(tradable / len(results), 4) if results else 0.0,
        "event_classes": dict(classes.most_common()),
        "hopper_watermark": dict(hops.most_common()),
        "winners": dict(bull.most_common(20)),
        "losers": dict(bear.most_common(20)),
    }
