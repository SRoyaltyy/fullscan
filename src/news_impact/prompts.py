"""Lane prompts: classifier sees the enum; analyst sees ONE family.

PROMPT_VERSION bumps when the contract text changes. one_shot_stack
logs fingerprint() so a gold row can prove Grok saw this file, not a stub.
"""
from __future__ import annotations

import hashlib

from .schema import EVENT_CLASSES

PROMPT_VERSION = "2026-09-23-full-v1"

# Last prompt built in this process. one_shot_stack may copy this onto the row.
LAST: dict = {}

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

SLOT_GRAMMAR = (
    "C constraint, T time, H harm, E expression, S substitute, R rival, "
    "A ammo, D durability, I invert, P priced, Y-S salience, Y-T transmission"
)


def fingerprint(text: str, hop: str) -> dict:
    raw = (text or "").encode("utf-8")
    lines = (text or "").splitlines()
    meta = {
        "hop": hop,
        "version": PROMPT_VERSION,
        "sha256_16": hashlib.sha256(raw).hexdigest()[:16],
        "bytes": len(raw),
        "lines": len(lines),
        "head": "\n".join(lines[:8]),
        "tail": "\n".join(lines[-8:]),
    }
    LAST[hop] = meta
    LAST["last_hop"] = hop
    return meta


def classifier_prompt(title: str, body: str = "", known_at: str = "") -> str:
    text = (
        "Classify this article. Pick exactly one event_class from the enum. "
        "If two facts, set split=true and list them.\n\n"
        "Q5 first: impulse | regime | regime_break.\n"
        "impulse = new dated fact that changes a constraint today.\n"
        "regime = weather reprint (constraint already in the tape). "
        "Hormuz-today, gold-on-Fed, class-action wrap of an old MDL = regime.\n"
        "regime_break = verified change in the physical or legal constraint "
        "(ceasefire + hulls move; EO actually signed; FDA approval that opens a gate).\n\n"
        f"event_class enum:\n{ENUM}\n\n"
        "Rules:\n"
        "- No tickers. No winners. No essays.\n"
        "- Themes (AI, geopolitics, ESG, China, semis) are not classes.\n"
        "- Second order is a role, not a class.\n"
        "- Analyst PT is discard unless it smuggles a print or guidance number.\n"
        "- If no constraint, event_class=discard.\n"
        "- Do not dump Buist / AMRX / a listicle into regime_break.\n\n"
        f"known_at: {known_at or ''}\n"
        f"Title: {title or '(untitled)'}\n"
        f"Body: {body or ''}\n\n"
        "STRICT JSON:\n"
        '{"event_class":"","sign":null,"q5":"impulse|regime|regime_break",'
        '"constraint":"","split":false,"split_facts":[],"why":""}'
    )
    fingerprint(text, "classify")
    return text


GOLD_RULES = (
    "GOLD FIXTURE RULES (apply when the article matches):\n"
    "- TSA / unpaid officers / airport chaos: family blast. Airlines down. "
    "Listed substitute if people still travel = CAR (Avis Budget) up. Do not omit CAR.\n"
    "- Buist / four-AI antitrust: family blast_legal. Harm set = named defendants. "
    "GOOGL is a slice not a 0-1d dump of Google. META stays_out / unscathed_rival. "
    "Do not classify as regime_break.\n"
    "- SEC TSV / tokenized securities venues 09-17: family structure. "
    "Old rent = exchanges/brokers. New rail may be unlisted (tradeable_expression=none). "
    "Never attach Energy because of a tangent word.\n"
    "- Amneal lanreotide FDA 16:01: family gate. Named issuer Amneal/AMRX. "
    "Clock = next session / Monday if Friday after-close. Not same-day 0-1d.\n"
    "- NAION / GLP-1 class wrap: product_harm + often Q5=regime for TODAY's wrap. "
    "NVO and LLY via Finviz. Clock not 0-1d. AVOID_ADD not a same-session dump.\n"
    "- Hormuz reprint: regime. entities [] for 0-1d.\n"
    "- 'shares outperform competitors' / stock-of-the-day: discard.\n"
    "- Diesel export SPEECH: statement_public + proposed access_control. "
    "VLO/MPC/PSX AVOID_ADD until an EO exists. Not XLE dump.\n"
)

HISTORY_SLOT = (
    "HISTORY (required on every kept impulse row):\n"
    "Y-S salience: What standing axiom about {place|order} just became false? "
    "When was the last analog in the same consciousness class (year)? "
    "Audience: street / city / world tape?\n"
    "Y-T transmission: Which listed pipe, book, or forced flow changed BECAUSE "
    "that axiom died? If the same deaths or arrest happened with no pipe, which "
    "ticker still moves? If none, transmission=none and there is no ACTION from "
    "salience alone.\n"
    "history_state: first_print | analog | reprint | n/a.\n"
    "Ukraine 2022 = both Y-S and Y-T (nodes). Wang Fuk fire = Y-S high, Y-T none "
    "unless a listed contractor/insurer exists. Maduro 3 Jan 2026 = first_print; "
    "Sep 2026 trial wrap = reprint.\n"
    "Salience is not a trade.\n"
)

CLOCK_RULES = (
    "CLOCK: entry = next RTH after known_at. 09:30 ET + 30m cutoff. "
    "After-close Friday = Monday. Speech vs statute: talk is not an EO.\n"
)

ROLES = (
    "ROLES (only if the family test produces them): "
    "named | harm_set | substitute | unscathed_rival | arms_dealer | "
    "peer | old_rent | new_rail.\n"
    "stays_out and wins_either_outcome are flags on unscathed_rival "
    "(Buist → META). unit_vs_parent = whole|slice|unknown. "
    "tradeable_expression = direct|proxy|none.\n"
    "Do not emit a ticker that is not in PACK / instruments.\n"
)

FAMILY_TESTS = {
    "blast": (
        "BLAST family (blast_legal, blast_ops, blast_cyber, product_harm, "
        "labor_stop, labor_organize, cat_weather).\n"
        "Losers = harm set: defendants, who cannot operate, breached name.\n"
        "Winners = (1) substitute if end-use demand remains and the channel is "
        "blocked — listed name on the Finviz hit list only (TSA → CAR). "
        "(2) unscathed_rival: competes and is outside the harm set (Buist → META). "
        "(3) arms_dealer: sells the input both sides still buy (model war → NVDA) "
        "only if PACK cites the input.\n"
        "FLIP: If the opposite headline printed, does the book flip? "
        "If you cannot say, direction=not_determined.\n"
        "Do not run intermediary-rent logic on a four-plaintiff complaint.\n"
    ),
    "structure": (
        "STRUCTURE family (market_structure, access_control, sanction_lift, "
        "standard_mandate, ip_ruling, breakup_remedy, tax_fiscal, price_cap, subsidy).\n"
        "Losers = who collects old venue / broker / listing rent.\n"
        "Winners = new rail only if that name is listed; else tradeable_expression=none.\n"
        "already_in_motion incumbent copying the rail → mixed, not a clean down.\n"
        "Durability: 5-year exemptive order is not a statute.\n"
        "Drop any hit that sits in the wrong sector because of a tangent word.\n"
        "Do not run blast-radius on a rule change.\n"
    ),
    "quantity": (
        "QUANTITY family (capacity add|destroy, demand up|down, input_cost up|down, "
        "inventory_print, channel_stock).\n"
        "Name the node: supplier / buyer / substitute.\n"
        "input_cost up: payers down, sellers up (jet fuel: airlines down, energy up — "
        "not 'Energy bear').\n"
        "inventory: who was positioned the wrong way vs the print.\n"
    ),
    "permission": (
        "PERMISSION family (gate open|shut, trial_readout).\n"
        "gate = became legal to sell/build. trial_readout is NOT approval → mixed.\n"
        "Scarce-permit incumbent may lose rent when a gate opens.\n"
        "After-close FDA: which session (AMRX 16:01 Friday → Monday).\n"
    ),
    "print": (
        "PRINT family (print_vs_priced, guidance cut|raise, preannounce, peer_spill, "
        "factor_impulse).\n"
        "What number changed vs what was already priced?\n"
        "guidance reaffirmed is not an up call.\n"
        "factor_impulse: locked ETF basket or ungraded — never 'Fed in focus → QQQ'.\n"
        "Do not pin a camera ticker to a factor headline.\n"
    ),
    "firm": (
        "FIRM family (mna, spinoff, dilution, capital_return, listing_flow, lockup, "
        "credit_funding, distress, integrity, key_person, insider_flow, activist, "
        "strategic_review, deal_review).\n"
        "Issuer's own cash, paper, or control — not a sector story.\n"
        "Routine dividend → not_determined, not a 0-1d sector trade.\n"
        "integrity: issuer's real sector, never Technology because Nasdaq hosted the notice.\n"
        "Insider tax-withholding sale is not a short.\n"
    ),
    "flow": (
        "FLOW family (flow_index, flow_mechanical, flow_forced_liq).\n"
        "Who is forced to buy or sell the named line, and on which session?\n"
        "Lockup: name into the expiry.\n"
    ),
    "time": (
        "TIME family (regime_state, regime_break, statement_public, rumor, "
        "fx_translation, policy_personnel, sovereign_credit, discard).\n"
        "regime_state → entities [].\n"
        "regime_break → flip signs of the OLD constraint's book.\n"
        "statement_public / rumor: no 0-1d ticket unless a run-rate changes.\n"
    ),
}

FLIP_QUESTIONS = {
    "blast": [
        "Who is in the harm set (cannot operate, sued, or breached)?",
        "If the buyer still wants the end-use and the primary channel is blocked, which listed substitute is on the Finviz hit list?",
        "Which listed rival competes and is outside the harm set (unscathed_rival)?",
        "Who sells the input both sides still buy (arms_dealer)? If unanswered, direction is not_determined.",
        "Did winning or losing the case still leave a headache for the named set (wins_either_outcome on the stay-out name)?",
    ],
    "structure": [
        "Who collects the old venue rent (exchange or broker)?",
        "Which new venue just received permission, and is that name on the Finviz hit list?",
        "Is the incumbent already building the same rail (then mixed, not a clean down)?",
        "How durable is the rule (hours / weeks / exemption / statute)?",
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

NAION_CLOCK_Q = (
    "NAION/GLP-1 is a class wrap: which listed sponsors are on the "
    "Finviz hit list, and why is the clock not 0-1d?"
)
LANREOTIDE_CLOCK_Q = (
    "The lanreotide approval hit at 16:01. Which session can actually "
    "trade it, and why is that not the same-day 0-1d tape?"
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
    flips = "\n".join(f"- {q}" for q in FLIP_QUESTIONS.get(family, FLIP_QUESTIONS["time"]))
    facts = pack_facts or []
    fact_lines = "\n".join(
        f"- {f.get('text', '')} ({f.get('source', '')} {f.get('url', '')})"
        for f in facts[:12]
    ) or "(none — use the article only; if family requires a pack, missing_context must list the blocking slots)"
    text = (
        f"PROMPT_VERSION={PROMPT_VERSION}\n"
        f"You are analysing a {event_class} / {sign} article (family={family}).\n"
        f"q5={q5}. Constraint: {constraint}\n"
        "event_class is LOCKED. Do not change it. Do not mention any other event_class.\n\n"
        "No scores. No multipliers. Direction is up|down|mixed|not_determined.\n"
        "Empty entities is legal only for q5=regime / weather / true junk.\n\n"
        f"{CLOCK_RULES}\n"
        f"{ROLES}\n"
        f"{HISTORY_SLOT}\n"
        f"{GOLD_RULES}\n"
        f"WINNER/LOSER TEST FOR THIS FAMILY ONLY:\n{tests}\n\n"
        f"FLIP QUESTIONS (answer or mark blocked; blocked → not_determined):\n{flips}\n\n"
        f"Y-S: {Y_S_QUESTION}\n"
        f"Y-T: {Y_T_QUESTION}\n\n"
        f"PACK FACTS (quote or unknown — do not invent, do not smuggle a ticker):\n{fact_lines}\n\n"
        f"Title: {title or '(untitled)'}\nBody: {body or ''}\n\n"
        "Fill losers[] or losers_reason. Fill winners[] with roles.\n"
        "If a required flip-Q is unanswered, direction=not_determined.\n\n"
        "STRICT JSON:\n"
        '{"history_state":"first_print|analog|reprint|n/a",'
        '"axiom_broken":"","transmission":"node|book|forced_flow|premium|none",'
        '"entities":[{"name":"","ticker":null,"role":"named",'
        '"direction":"up|down|mixed|not_determined","horizon":"0-1d",'
        '"unit_vs_parent":"whole","tradeable_expression":"direct",'
        '"stays_out":false,"wins_either_outcome":false,'
        '"if_unknown":null,"axiom_id":null}],'
        '"missing_context":[{"id":"","question":"","blocks":[]}]}'
    )
    fingerprint(text, "analyst")
    return text


META_SYSTEM = (
    "You decide which facts block a direction on one article. "
    "Return ONE JSON object. No markdown. No tickers. No winners. No themes."
)
PACK_COMPLETE_SYSTEM = (
    "You test whether the pack answers every direction-blocking question. "
    "Return ONE JSON object. No markdown. No new tickers. "
    "A blocked question is not_determined, not a guess."
)


def meta_prompt(
    title: str,
    body: str,
    family: str,
    event_class: str,
    constraint: str,
) -> str:
    """M1–M3. Runs after class is locked and before any lookup."""
    text = (
        f"PROMPT_VERSION={PROMPT_VERSION}\n"
        "The event_class is locked. Do not change it. Do not name a ticker.\n"
        f"family={family}\n"
        f"event_class={event_class}\n"
        f"constraint={constraint}\n\n"
        "These five meta-gates are mandatory.\n\n"
        "M1 need_context: If you believe ONLY the sentences on the page, can you "
        "name the constraint, the harm set, and a listed expression? "
        "yes = page is enough. no = pack required. "
        "blast / structure / quantity / permission ALWAYS require a pack.\n\n"
        "M2 blocking_facts: list the slots that must be answered before a "
        "direction is legal. Use ONLY these slots: "
        f"{SLOT_GRAMMAR}.\n"
        "A question is only legal if an unanswered it FORBIDS up/down.\n"
        "Each question binds one noun that already appears in the article. "
        "Cap 6 questions. If you need more than 6 blocking facts, class is wrong "
        "or Q5 is regime.\n"
        "Do not ask 'what is the AI angle' or 'who should I buy'.\n\n"
        "M3 bullshit_filter: keep a question only when it can change one of "
        "direction, class, clock, unit_vs_parent, tradeable_expression. "
        "Drop: theme fishing, already-in-article, undated weather, asking for ACTION.\n"
        "Put rejects in m3_dropped with why in "
        "ai_angle | who_should_i_buy | already_in_article | undated_weather | "
        "theme_fishing | no_direction_change | bad_slot.\n\n"
        "M4 is a later hop (invert). Do not invent pack facts here.\n"
        "M5 is code: every ACTION ticker must sit in instruments[].\n\n"
        f"Title: {title}\n"
        f"Body: {body or ''}\n\n"
        "STRICT JSON:\n"
        '{"m1":{"need_context":"yes|no"},'
        '"m2":[{"slot":"H","noun":"","question":"",'
        '"changes":"direction|class|clock|unit_vs_parent|tradeable_expression",'
        '"blocks":["direction"]}],'
        '"m3_dropped":[{"question":"","why":""}]}'
    )
    fingerprint(text, "meta")
    return text


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
        text_f = fact.get("text") or ""
        status = fact.get("status") or ("quote" if text_f else "unknown")
        fact_lines.append(f"- source={src} status={status} url={url} quote={text_f}")
    facts_block = "\n".join(fact_lines) or "(empty pack)"
    text = (
        f"PROMPT_VERSION={PROMPT_VERSION}\n"
        "M4 pack_complete / opposite headline test.\n"
        "event_class is locked. Do not name a new ticker.\n"
        "For every question that blocks direction, status is answered or blocked. "
        "blocked means the direction is not_determined. Do not guess.\n"
        "invert is one sentence: the headline that would flip the direction.\n"
        "If the pack is empty and M1 required a pack, pack_complete is false.\n\n"
        f"QUESTIONS:\n{block}\n\n"
        f"PACK:\n{facts_block}\n\n"
        f"Title: {title}\n\n"
        "STRICT JSON:\n"
        '{"invert":"","slots":[{"question":"","status":"answered|blocked"}]}'
    )
    fingerprint(text, "pack_complete")
    return text
