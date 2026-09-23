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

# Family flip questions. The planner copies ONE family's list into
# scratch.questions[]. It does not invent a second family.
FLIP_QUESTIONS = {
    "blast": [
        "Who is in the harm set (cannot operate, sued, or breached)?",
        "If the buyer still wants the end-use and the primary channel is blocked, which listed substitute is on the Finviz hit list?",
        "Which listed rival competes and is outside the harm set (unscathed_rival)?",
        "Who sells the input both sides still buy (arms_dealer)? If unanswered, direction is not_determined.",
    ],
    "structure": [
        "Who collects the old venue rent (exchange or broker)?",
        "Which new venue just received permission, and is that name on the Finviz hit list?",
        "Is the incumbent already building the same rail (then mixed, not a clean down)?",
        "Does any hit sit in Energy even though the title never names that firm? Drop it.",
    ],
    "quantity": [
        "Who pays the input, and who sells it?",
        "Is this capacity added or capacity destroyed?",
        "Which listed substitute wins if the primary supply is missing?",
    ],
    "permission": [
        "Who just became legal to sell or launch the product?",
        "Is this a gate (approval) or only a trial readout (not approval)?",
        "What clock does the print land on: same session, next open, or Monday?",
    ],
    "print": [
        "What number changed versus what was already priced?",
        "Is this one named issuer, or a factor that should not be pinned to a camera ticker?",
        "If guidance was only reaffirmed, why is an up call illegal?",
    ],
    "firm": [
        "Is this the issuer's own cash, paper, or control — not a sector story?",
        "If it is a routine dividend, why is there no 0-1d sector trade?",
        "Who is the named issuer on the Finviz hit list?",
    ],
    "flow": [
        "Who is forced to buy or sell the named line, and on which session?",
        "Is the flow the issuer itself or an index vehicle?",
    ],
    "time": [
        "Did the physical or legal constraint change, or is this a reprint?",
        "If it is a regime break, which prior book flips sign?",
        "If it is weather, why is a 0-1d entity illegal?",
    ],
}

NAION_CLOCK_Q = (
    "NAION/GLP-1 is a class wrap: which listed sponsors are on the "
    "Finviz hit list, and why is the clock not 0-1d?"
)
LANREOTIDE_CLOCK_Q = (
    "The lanreotide approval hit at 16:01. Which session can actually "
    "trade it, and why is that not the same-day 0-1d tape?"
)

# History slot. The planner copies both questions onto every article.
# Salience is not a trade. transmission=none means no ACTION from Y alone.
Y_S_QUESTION = (
    "Y-S salience: What standing axiom about {place|order} just became false? "
    "When was the last analog in the same consciousness class (year)? "
    "Audience: street / city / world tape?"
)
Y_T_QUESTION = (
    "Y-T transmission: Which listed pipe, book, or forced flow changed "
    "BECAUSE that axiom died? If the same deaths or arrest happened with no "
    "pipe, which ticker still moves? If none, transmission=none and there is "
    "no ACTION from salience alone."
)
HISTORY_SLOT = (
    "History slot. Answer Y-S and Y-T.\n"
    "history_state is first_print, analog, reprint, or n/a.\n"
    "A Ukraine-class break fires both: the axiom died (Y-S) and a listed "
    "node moved (Y-T, transmission=node).\n"
    "A tragedy with no listed contractor or insurer is salience only "
    "(transmission=none). Do not dump an index.\n"
    "Maduro captured on 3 Jan 2026 is first_print. A September 2026 wrap of "
    "that capture is reprint.\n"
    "A diesel speech is not the first_print of the 2015 export-lift axiom "
    "until an executive order exists.\n"
    "If transmission is none, emit no trade. Salience is not a trade.\n"
)

# Meta-question hop. Same quality floor as classify. No tickers.
# 8B does not decide what to ask or whether the pack is complete.
META_SYSTEM = (
    "You decide which facts block a direction on one article. "
    "Return ONE JSON object. No markdown. No tickers. No winners. No themes."
)
PACK_COMPLETE_SYSTEM = (
    "You test whether the pack answers every direction-blocking question. "
    "Return ONE JSON object. No markdown. No new tickers. "
    "A blocked question is not_determined, not a guess."
)
_SLOT_GRAMMAR = (
    "C constraint, T time, H harm, E expression, S substitute, R rival, "
    "A ammo, D durability, I invert, P priced, Y-S salience, Y-T transmission"
)


def meta_prompt(
    title: str,
    body: str,
    family: str,
    event_class: str,
    constraint: str,
) -> str:
    """M1–M3. Runs after class is locked and before any lookup."""
    return (
        "The event_class is locked. Do not change it. Do not name a ticker.\n"
        f"family={family}\n"
        f"event_class={event_class}\n"
        f"constraint={constraint}\n\n"
        "M1 need_context: If you believe only this page, can you name the "
        "constraint, the harm set, and a listed expression? Answer yes or no.\n"
        "M2 blocking_facts: list the slots that must be answered before a "
        "direction is legal. Use ONLY these slots: "
        f"{_SLOT_GRAMMAR}.\n"
        "Each question binds one noun that already appears in the article. "
        "No theme fishing. No 'what is the AI angle'. No 'who should I buy'.\n"
        "M3: a question is kept only when it can change one of "
        "direction, class, clock, unit_vs_parent, tradeable_expression. "
        "Put the rejects in m3_dropped with why set to one of "
        "ai_angle, who_should_i_buy, already_in_article, undated_weather, "
        "theme_fishing, no_direction_change.\n\n"
        f"Title: {title}\n"
        f"Body: {body or ''}\n\n"
        "STRICT JSON:\n"
        '{"m1":{"need_context":"yes|no"},'
        '"m2":[{"slot":"H","noun":"","question":"",'
        '"changes":"direction|class|clock|unit_vs_parent|tradeable_expression",'
        '"blocks":["direction"]}],'
        '"m3_dropped":[{"question":"","why":""}]}'
    )


def pack_complete_prompt(
    title: str,
    questions: list[dict],
    facts: list[dict],
) -> str:
    """M4 opposite-headline test. Floor model. Pack quotes only."""
    q_lines = []
    for q in questions:
        blocks = ",".join(q.get("blocks") or [])
        q_lines.append(
            f"- slot={q.get('slot')} blocks={blocks} question={q.get('question')}"
        )
    block = "\n".join(q_lines) or "(none)"
    fact_lines = []
    for fact in (facts or [])[:12]:
        src = fact.get("source") or ""
        url = fact.get("url") or ""
        text = fact.get("text") or ""
        status = fact.get("status") or ("quote" if text else "unknown")
        fact_lines.append(f"- source={src} status={status} url={url} quote={text}")
    facts_block = "\n".join(fact_lines) or "(empty pack)"
    return (
        "Opposite headline test. event_class is locked. Do not name a new ticker.\n"
        "For every question that blocks direction, status is answered or blocked. "
        "blocked means the direction is not_determined. Do not guess.\n"
        "invert is one sentence: the headline that would flip the direction.\n\n"
        f"QUESTIONS:\n{block}\n\n"
        f"PACK:\n{facts_block}\n\n"
        f"Title: {title}\n\n"
        "STRICT JSON:\n"
        '{"invert":"","slots":[{"question":"","status":"answered|blocked"}]}'
    )
