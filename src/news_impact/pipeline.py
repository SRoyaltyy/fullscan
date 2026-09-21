"""One article → classification → optional search/Lane → entities + watermark."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from . import scratch
from .classify import classify_article, merge_lane_class, rank_articles
from .families import analyze, axioms_for
from .schema import (
    PIPELINE_VERSION,
    Classification,
    Entity,
    is_usable,
)
from .search_pack import pack_for_article

DETERMINISTIC_MARK = "deterministic"
DETERMINISTIC_MODEL = PIPELINE_VERSION


def _watermark(lane: str, model: str) -> dict[str, str]:
    return {
        "lane": lane,
        "model": model,
        "inference_source": lane,
    }


def _lane_hop(tmpl: str, art: dict, family: str = "", cls: Classification | None = None,
              pack: dict | None = None) -> tuple[dict | None, str, str]:
    """Optional Lane hop. Returns (parsed, lane, model). Fail soft."""
    try:
        from src import lane_route as lane
    except Exception:
        return None, "", ""
    keys, ollama_url, gh_direct = lane.load_keys()
    if not keys and not ollama_url and not gh_direct:
        return None, "", ""
    ctx = {"keys": keys, "ollama_url": ollama_url, "gh_direct": gh_direct}
    from .prompts import analyst_prompt, classifier_prompt
    if tmpl == "news_classify":
        prompt = classifier_prompt(
            str(art.get("title") or ""),
            str(art.get("body") or ""),
            str(art.get("known_at") or ""),
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
    for hop in lane.lanes_for(tmpl):
        parsed, model = lane.ask_lane(
            hop, prompt, ctx, max_tokens=budget, system=system, tmpl=tmpl,
        )
        if parsed is not None:
            if lane.is_banned_primary(model):
                continue
            return parsed, hop, str(model or hop)
    return None, "", ""


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


def analyze_article(
    art: dict,
    use_lane: bool = False,
    use_search: bool = False,
    persist: bool = True,
) -> dict[str, Any]:
    title = str(art.get("title") or "")
    body = str(art.get("body") or "")
    known = str(art.get("known_at") or art.get("published_at") or "")
    aid = scratch.article_id(title, known)
    cls = classify_article(art)
    mark = _watermark(DETERMINISTIC_MARK, DETERMINISTIC_MODEL)
    lane_cls_parsed = None
    if use_lane:
        lane_cls_parsed, hop, model = _lane_hop("news_classify", art)
        if lane_cls_parsed:
            cls = merge_lane_class(cls, lane_cls_parsed)
            mark = _watermark(hop, model)

    pack = pack_for_article(title, body, enabled=use_search)
    entities = analyze(art, cls, pack)
    if use_lane and cls.q5 != "regime" and cls.event_class not in {
        "discard", "regime_state", "rumor",
    }:
        lane_an, hop, model = _lane_hop(
            "news_impact", art, family=cls.family, cls=cls, pack=pack,
        )
        lane_ents = _entities_from_lane(lane_an or {})
        if lane_ents:
            entities = lane_ents
            mark = _watermark(hop, model)

    usable = is_usable(cls, entities)
    row = {
        "article_id": aid,
        "title": title[:300],
        "body": body[:800],
        "source_file": art.get("source_file") or art.get("source") or "",
        "known_at": known,
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
        "axioms_used": axioms_for(entities),
        "entities": [e.to_dict() for e in entities],
        "usable": usable,
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
    for r in results:
        if r.get("usable"):
            usable += 1
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
        "discarded": len(results) - usable,
        "usable_ratio": round(usable / len(results), 4) if results else 0.0,
        "event_classes": dict(classes.most_common()),
        "hopper_watermark": dict(hops.most_common()),
        "winners": dict(bull.most_common(20)),
        "losers": dict(bear.most_common(20)),
    }
