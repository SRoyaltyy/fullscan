"""Finish a Lane row when the Finviz linker has no company to confirm.

The one-shot stack stops at no_instrument_candidates. That stop is correct:
an empty hit list must not be filtered or declared harmless. The missing
piece is the question Deepseek still has to answer: which vertical is
hurt or helped, and which second-order vertical moves the other way.
Tickers are attached here from the Finviz industry book after that answer.
The model does not emit tickers.
"""
from __future__ import annotations

import json
import re

from src.news_impact.material_gate import gate, lane_article
from src.news_impact.schema import EVENT_CLASSES
from src.news_impact.scratch import article_id
from src.news_impact.ticker_verticals import (
    CHAINS,
    link_title,
    load_book,
    named_companies,
    roles_for_text,
    tickers_for_roles,
)

_DIRECTIONS = frozenset({"up", "down", "mixed", "not_determined"})
_Q5 = frozenset({"impulse", "regime", "regime_break"})


def vertical_packet(title: str, body: str, event_class: str, q5: str,
                    sign, constraint: str) -> dict:
    """The job left when no listed company is in the title."""
    return {
        "title": title[:300],
        "body": (body or "")[:800],
        "event_class": event_class,
        "q5": q5,
        "sign": sign,
        "constraint": constraint,
        "stated_roles": [
            {"chain": chain, "role": role}
            for chain, role in roles_for_text(title)
        ],
        "chains": CHAINS,
        "ask": (
            "Name the primary vertical and any second-order vertical. "
            "Do not emit tickers. Empty verticals if the title states none."
        ),
    }


def job_for(row: dict, book: list[dict] | None = None) -> dict:
    """One official row, gated, ready for a batch answer."""
    book = book if book is not None else load_book()
    decision = gate(row)
    article = lane_article(row)
    title = article["title"]
    return {
        "id": article_id(title, article.get("published_at") or ""),
        "keep": decision["keep"],
        "gate": decision["reason"],
        "title": title,
        "body": article["body"],
        "url": article["url"],
        "published_at": article["published_at"],
        "source": article["source"],
        "named_in_title": named_companies(title, book),
        "stated_roles": [
            {"chain": chain, "role": role}
            for chain, role in roles_for_text(title)
        ],
    }


def render_batch(jobs: list[dict]) -> str:
    """One prompt. Ontology and the vertical menu once, then the kept rows."""
    kept = [job for job in jobs if job.get("keep")]
    menu = []
    for chain, roles in CHAINS.items():
        for role, industries in roles.items():
            menu.append(f"- {chain}.{role}: {', '.join(industries)}")
    classes = ", ".join(EVENT_CLASSES)
    blocks = []
    for job in kept:
        locked = job.get("stated_roles") or []
        named = [row["ticker"] for row in job.get("named_in_title") or []]
        blocks.append(
            f"ID {job['id']}\n"
            f"TITLE: {job['title']}\n"
            f"BODY: {job.get('body') or ''}\n"
            f"LOCKED_ROLES: {json.dumps(locked)}\n"
            f"NAMED_IN_TITLE: {json.dumps(named)}\n"
        )
    return (
        "You are completing Lane. One JSON object per ID, in a JSON array.\n"
        "No markdown. No tickers. Tickers are attached after you answer.\n\n"
        "A row is an impulse only when a public decision with an economic "
        "mechanism is reported. A proposal, a speech, a referral, and a "
        "statistic that only restates the weather are regime or discard.\n"
        "Pick the narrowest class. Proposals stay statement_public.\n"
        f"event_class must be one of: {classes}\n"
        "q5 must be impulse, regime, or regime_break.\n"
        "sign must be add, destroy, up, down, open, shut, tighten, lift, "
        "cut, raise, or null.\n"
        "direction must be up, down, mixed, or not_determined.\n\n"
        "Verticals you may name:\n"
        + "\n".join(menu)
        + "\n\n"
        "If LOCKED_ROLES is not empty, those roles are the primary. "
        "You still set their direction and you may add second_order roles.\n"
        "If NAMED_IN_TITLE is not empty, set direction on that company and "
        "do not replace it with a sector.\n"
        "If both are empty, name primary only when the title supports a "
        "vertical. Otherwise primary is []. Do not guess a famous stock.\n"
        "second_order is the other side of the same mechanism: the buyer "
        "when the seller is hurt, the substitute, the supplier. Omit it "
        "when you cannot name that side from the title.\n\n"
        "Each object:\n"
        '{"id":"","event_class":"","q5":"impulse","sign":null,'
        '"primary":[{"chain":"","role":"","direction":"down"}],'
        '"second_order":[{"chain":"","role":"","direction":"up","why":""}]}\n\n'
        "ITEMS\n\n"
        + "\n".join(blocks)
    )


def parse_reply(text: str) -> list[dict]:
    """Pull a JSON array, or one object, out of a chat reply."""
    raw = text or ""
    fenced = re.search(r"```(?:json)?\s*(\[.*\]|\{.*\})\s*```", raw, re.S)
    blob = fenced.group(1) if fenced else raw
    start = min(
        (i for i in (blob.find("["), blob.find("{")) if i >= 0),
        default=-1,
    )
    if start < 0:
        return []
    blob = blob[start:]
    try:
        parsed = json.loads(blob)
    except json.JSONDecodeError:
        end = max(blob.rfind("]"), blob.rfind("}"))
        if end < 0:
            return []
        try:
            parsed = json.loads(blob[: end + 1])
        except json.JSONDecodeError:
            return []
    if isinstance(parsed, dict):
        return [parsed]
    if isinstance(parsed, list):
        return [row for row in parsed if isinstance(row, dict)]
    return []


def _legs(answer: dict, key: str) -> list[dict]:
    rows = answer.get(key) or []
    if not isinstance(rows, list):
        return []
    out = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        chain = str(row.get("chain") or "")
        role = str(row.get("role") or "")
        direction = str(row.get("direction") or "not_determined")
        if chain not in CHAINS or role not in CHAINS[chain]:
            continue
        if direction not in _DIRECTIONS:
            direction = "not_determined"
        out.append({
            "chain": chain,
            "role": role,
            "direction": direction,
            "why": str(row.get("why") or "")[:180],
        })
    return out


def apply_answer(job: dict, answer: dict, book: list[dict] | None = None) -> dict:
    """Attach book tickers to one model answer. Invented tickers are dropped."""
    book = book if book is not None else load_book()
    errors = []
    event_class = str((answer or {}).get("event_class") or "")
    q5 = str((answer or {}).get("q5") or "")
    if event_class not in EVENT_CLASSES:
        errors.append("bad_event_class")
    if q5 not in _Q5:
        errors.append("bad_q5")
    sign = (answer or {}).get("sign")
    if sign in ("", "null", "none"):
        sign = None
    named = list(job.get("named_in_title") or [])
    if not named:
        named = named_companies(job.get("title") or "", book)
    stated = [
        (row["chain"], row["role"])
        for row in (job.get("stated_roles") or [])
        if row.get("chain") in CHAINS and row.get("role") in CHAINS[row["chain"]]
    ]
    primary = _legs(answer or {}, "primary")
    second = _legs(answer or {}, "second_order")
    named_direction = "not_determined"
    for row in primary:
        if row["direction"] in {"up", "down", "mixed"}:
            named_direction = row["direction"]
            break
    if stated:
        allowed = set(stated)
        # Title-locked roles stay. The model only sets direction.
        by_role = {(row["chain"], row["role"]): row["direction"] for row in primary}
        primary = [
            {
                "chain": chain,
                "role": role,
                "direction": by_role.get((chain, role)) or named_direction,
                "why": "",
            }
            for chain, role in stated
        ]
        second = [row for row in second if (row["chain"], row["role"]) not in allowed]
    elif named:
        primary = []
    entities = []
    if q5 == "impulse":
        for row in named:
            entities.append({
                **row,
                "direction": named_direction,
                "order": "named",
            })
        for row in primary:
            for tick in tickers_for_roles(book, [(row["chain"], row["role"])], n=4):
                entities.append({**tick, "direction": row["direction"], "order": "primary"})
        for row in second:
            for tick in tickers_for_roles(book, [(row["chain"], row["role"])], n=3):
                entities.append({**tick, "direction": row["direction"], "order": "second_order", "why": row["why"]})
    if q5 == "impulse" and not entities and not errors:
        errors.append("no_vertical")
    action = _action(entities, job.get("title") or "")
    return {
        "id": job.get("id"),
        "title": job.get("title"),
        "event_class": event_class,
        "q5": q5,
        "sign": sign,
        "entities": entities,
        "action": action,
        "errors": errors,
        "complete": not errors and (q5 != "impulse" or bool(entities)),
    }


def _action(entities: list[dict], title: str) -> str:
    because = " ".join((title or "").split())[:160]
    bits = []
    for verb, direction in (("BUY", "up"), ("SELL", "down")):
        group = [row for row in entities if row.get("direction") == direction and row.get("ticker")]
        if not group:
            continue
        tickers = "/".join(row["ticker"] for row in group[:6])
        bits.append(f"{verb} {tickers}, 1-4w, because {because}")
    return "; ".join(bits)


def apply_reply(jobs: list[dict], reply: str, book: list[dict] | None = None) -> list[dict]:
    book = book if book is not None else load_book()
    by_id = {job["id"]: job for job in jobs}
    out = []
    for answer in parse_reply(reply):
        job = by_id.get(str(answer.get("id") or ""))
        if job is None:
            continue
        out.append(apply_answer(job, answer, book))
    return out


def preview_link(title: str, book: list[dict] | None = None) -> dict:
    """What the book can resolve before any model answers."""
    return link_title(title, book)
