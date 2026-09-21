"""Lane prompts: classifier sees the enum; analyst sees ONE family."""
from __future__ import annotations

from .schema import EVENT_CLASSES

CLASSIFIER_SYSTEM = (
    "You classify one news article for a trading desk. "
    "Return ONE JSON object. No markdown. No tickers. No winners."
)

ANALYST_SYSTEM = (
    "You analyse one news article using ONLY the supplied family test. "
    "Return ONE JSON object. No markdown. No scores. "
    "Direction is up|down|mixed|not_determined."
)

ENUM = " | ".join(EVENT_CLASSES)
FAMILIES = (
    "blast | structure | quantity | permission | print | firm | flow | time"
)


def classifier_prompt(title: str, body: str = "", known_at: str = "") -> str:
    return (
        "Classify this article. Pick exactly one event_class from the enum. "
        "If two facts, set split=true and list them.\n\n"
        "Q5 first: impulse | regime | regime_break. "
        "regime = weather reprint (constraint already in the tape). "
        "regime_break = verified change in the physical/legal constraint.\n\n"
        f"event_class enum:\n{ENUM}\n\n"
        "Rules:\n"
        "- No tickers. No winners. No essays.\n"
        "- Themes (AI, geopolitics, ESG) are not classes.\n"
        "- If no constraint, event_class=discard.\n\n"
        f"known_at: {known_at or ''}\n"
        f"Title: {title or '(untitled)'}\n"
        f"Body: {body or ''}\n\n"
        "STRICT JSON:\n"
        '{"event_class":"","sign":null,"q5":"impulse|regime|regime_break",'
        '"constraint":"","split":false,"split_facts":[],"why":""}'
    )


def analyst_prompt(
    title: str,
    body: str,
    family: str,
    event_class: str,
    sign: str | None,
    q5: str,
    constraint: str,
    pack_facts: list[dict] | None = None,
) -> str:
    tests = FAMILY_TESTS.get(family, FAMILY_TESTS["time"])
    facts = pack_facts or []
    fact_lines = "\n".join(
        f"- {f.get('text', '')} ({f.get('url', '')})" for f in facts[:8]
    ) or "(none — use the article only)"
    return (
        f"You are analysing a {event_class} / {sign} article (family={family}).\n"
        f"q5={q5}. Constraint: {constraint}\n\n"
        "1. Name the constraint in one sentence.\n"
        "2. Fill losers[] from the test below. If none, say why.\n"
        "3. Fill winners[] from the test below. If none, say why.\n"
        "4. Attach roles only if the test produces them: "
        "substitute, unscathed_rival, arms_dealer.\n"
        "5. Direction is up|down|mixed|not_determined. "
        "If a flip-question is unanswered, not_determined.\n"
        "6. Do not mention any other event_class.\n"
        "7. unit_vs_parent is whole|slice. tradeable_expression is direct|proxy|none.\n"
        "8. A sector cannot be both bullish and bearish; use mixed.\n\n"
        f"WINNER/LOSER TEST:\n{tests}\n\n"
        f"PACK FACTS (quote or unknown — do not invent):\n{fact_lines}\n\n"
        f"Title: {title or '(untitled)'}\nBody: {body or ''}\n\n"
        "STRICT JSON:\n"
        '{"entities":[{"name":"","ticker":null,"role":"named",'
        '"direction":"up|down|mixed|not_determined","horizon":"0-1d",'
        '"unit_vs_parent":"whole","tradeable_expression":"direct",'
        '"if_unknown":null,"axiom_id":null}],'
        '"missing_context":[{"id":"","question":"","blocks":[]}]}'
    )


FAMILY_TESTS = {
    "blast": (
        "Losers = named harm set (defendants / who cannot operate / breached name).\n"
        "Winners = unscathed rivals (compete and not in the set) and "
        "arms dealers (sell the input both sides still buy) — only if the test "
        "produces them. Q8: if the buyer still wants the end-use and the "
        "primary channel is blocked, name the listed substitute (TSA → CAR)."
    ),
    "structure": (
        "Losers = old rent (exchanges, brokers) unless incumbent_response is "
        "already_in_motion (then mixed).\n"
        "Winners = new rails + settlement arms dealer. Durability matters: "
        "a 5-year exemption is not a permanent law. "
        "tradeable_expression=none for private names."
    ),
    "quantity": (
        "capacity add: incumbents at the node lose, new supplier and buyers win.\n"
        "capacity destroy: who needs the missing output loses; substitutes win.\n"
        "input_cost up: payers down, sellers up (jet fuel: airlines down, energy up).\n"
        "inventory_print: who was positioned the wrong way vs the print loses."
    ),
    "permission": (
        "gate open: who just became legal to sell/build wins; "
        "who already had the scarce permit may lose scarcity rent.\n"
        "trial_readout is NOT approval — mixed until the next gate."
    ),
    "print": (
        "print_vs_priced / guidance: the named name vs its number.\n"
        "factor_impulse: a macro number reprices a factor, then a book — "
        "do not merge with an inventory print; split.\n"
        "peer_spill: cousin only with a cited shared-factor axiom."
    ),
    "firm": (
        "M&A: target at deal price; acquirer mixed if cash/dilutive.\n"
        "capital_return: routine dividend is not_determined, not a sector trade.\n"
        "integrity: named issuer's sector — never Technology because Nasdaq hosted the notice.\n"
        "insider_flow: not_determined."
    ),
    "flow": (
        "listing / index add: forced buyers win the name day 0.\n"
        "lockup expiry: the name into the expiry loses."
    ),
    "time": (
        "regime_state: entities [].\n"
        "regime_break: flip the signs of the old constraint's book.\n"
        "statement_public / discard: no ticket unless a run-rate changes."
    ),
}
