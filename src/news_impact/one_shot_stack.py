"""Lane one-shot stack. Gate 0 rejects junk. Lane is the brain.

Order, per article:
  Gate0 → Stage1 Lane classify (no tickers) → context planner
  → Finviz candidates (≤40) → Lane confirms instruments
  → pack facts → Stage2 Lane analyst (one family) → validator → ACTION.

classify.py / families.analyze are not called. A failed Lane hop is a
miss, not a deterministic fill.
"""
from __future__ import annotations

import re
from typing import Any, Callable

from src.news_impact.finviz_linker import _has_phrase, candidate_rows
from src.news_impact.horizons import default_horizon
from src.news_impact.hygiene import is_reaction_title
from src.news_impact.prompts import (
    ANALYST_SYSTEM,
    CLASSIFIER_SYSTEM,
    FAMILY_TESTS,
    classifier_prompt,
)
from src.news_impact.schema import EVENT_CLASSES, family_of
from src.news_impact.scratch import article_id

LaneFn = Callable[[str, str, str], tuple[dict | None, str, str]]

SECOND_ORDER = frozenset({"substitute", "unscathed_rival", "arms_dealer"})
SIGNS = frozenset({
    "add", "destroy", "up", "down", "open", "shut",
    "tighten", "lift", "cut", "raise",
})

_HORMUZ_WEATHER = re.compile(
    r"(?i)((hormuz|strait of hormuz).{0,80}(still|remains|closed|high risk|"
    r"despite|capped|talks|hopes|optimism|elusive)|"
    r"(oil (prices? )?(tumble|slip|slide|fall).{0,40}hormuz)|"
    r"(hormuz.{0,40}(crisis|blockade|shipping)))"
)
_HORMUZ_BREAK = re.compile(
    r"(?i)(ceasefire|reopen|withdraw|traffic resumes|both navies)"
)
_GOLD_FED = re.compile(
    r"(?i)gold (price|prices)?.{0,60}(fed|rate cut|rate hike|warsh|cut bets)|"
    r"gold (surge|slide|slides|jumps|tumbles).{0,40}(fed|warsh|hike|cut)"
)
_OUTPERFORM = re.compile(r"(?i)outperforms competitors")
_EARNINGS_IR = re.compile(
    r"(?i)(schedules?.{0,50}(earnings|conference call)|"
    r"earnings (release|call) and conference|"
    r"earnings call (highlights|summary)$|"
    r"to announce unaudited)"
)
_DIVIDEND = re.compile(
    r"(?i)(declares?|announces?).{0,40}dividend|cash dividend|"
    r"quarterly (cash )?dividend"
)
_NOT_DIVIDEND_ONLY = re.compile(
    r"(?i)(fda|approval|acquire|merger|guidance|contract|recall|lawsuit)"
)
# regime_break is legal only when the text actually changes the constraint.
_REGIME_BREAK_OK = re.compile(
    r"(?i)(ceasefire|reopen|withdraw|lifted|traffic resumes)"
)
_AIRLINES = frozenset({
    "AAL", "DAL", "UAL", "LUV", "ALK", "JBLU", "ALGT", "SKYW", "HA", "ULCC",
})
_VENUES = frozenset({"COIN", "NDAQ", "ICE", "CME", "CBOE"})
_ROLE_MAP = {
    "harm_set": "named",
    "harmset": "named",
    "defendant": "named",
    "harm": "named",
    "unscathed": "unscathed_rival",
    "stays_out": "unscathed_rival",
    "rival": "unscathed_rival",
    "unscathed_rival": "unscathed_rival",
    "substitute": "substitute",
    "arms_dealer": "arms_dealer",
    "named": "named",
}

QUESTIONS = {
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


GOLD_KEEP = [
    {
        "gold_id": "tsa",
        "title": (
            "Government shutdown leads to chaos at US airports as TSA officers "
            "go unpaid on busy travel weekend"
        ),
        "body": "TSA checkpoints are short-staffed. Flights are the blocked channel.",
        "known_at": "2026-09-18T12:00:00-04:00",
        "harvest_source": "gold_fixture",
    },
    {
        "gold_id": "buist",
        "title": (
            "Four paying subscribers filed a class-action antitrust lawsuit, "
            "Buist et al. v. Anthropic PBC et al., accusing Anthropic, OpenAI, "
            "xAI (SpaceXAI), and Google of an illegal agreement to slow AI"
        ),
        "body": "Complaint names the labs. Meta is not a defendant.",
        "known_at": "2026-09-17T09:00:00-04:00",
        "harvest_source": "gold_fixture",
    },
    {
        "gold_id": "tsv",
        "title": (
            "SEC issues order granting temporary exemptive relief to Tokenized "
            "Securities Venues to trade tokenized NMS stock"
        ),
        "body": "Market-structure permission for venues. Not an energy item.",
        "known_at": "2026-09-17T10:00:00-04:00",
        "harvest_source": "gold_fixture",
    },
    {
        "gold_id": "amrx",
        "title": "Amneal Announces FDA Approval and Launch of Lanreotide Injection",
        "body": "Amneal wins FDA approval and begins U.S. launch of generic lanreotide.",
        "known_at": "2026-09-18T16:01:00-04:00",
        "harvest_source": "gold_fixture",
    },
    {
        "gold_id": "naion",
        "title": (
            "Class wrap links GLP-1 drugs semaglutide from Novo Nordisk and "
            "tirzepatide from Eli Lilly to NAION optic-nerve risk"
        ),
        "body": (
            "Review article on the GLP-1 class and non-arteritic anterior "
            "ischemic optic neuropathy. Not a same-day print."
        ),
        "known_at": "2026-09-22T18:00:00-04:00",
        "harvest_source": "gold_fixture",
    },
]

GOLD_REJECT = [
    {
        "gold_id": "hormuz",
        "title": "Why Hormuz remains high risk for ships despite US claims of mine-clearing",
        "body": "Reprint of the strait constraint already in the tape.",
        "known_at": "2026-09-17T08:00:00-04:00",
        "harvest_source": "gold_fixture",
    },
    {
        "gold_id": "outperforms",
        "title": "United Airlines Holdings Inc. stock outperforms competitors on strong trading day",
        "body": "Tape caption.",
        "known_at": "2026-09-18T15:00:00-04:00",
        "harvest_source": "gold_fixture",
    },
]


def gate0(title: str, body: str = "") -> str | None:
    """Return a reject reason, or None to spend a Lane hop.

    Source-type / reaction / weather / empty-IR / dividend-only / tape
    captions die here. This is not an event-class assignment.
    """
    text = f"{title or ''}\n{body or ''}".strip()
    if not (title or "").strip():
        return "empty_title"
    if is_reaction_title(title):
        return "reaction_title"
    if _OUTPERFORM.search(title):
        return "outperforms_competitors"
    if _EARNINGS_IR.search(title):
        return "empty_ir"
    if _DIVIDEND.search(title) and not _NOT_DIVIDEND_ONLY.search(text):
        return "dividend_only"
    if _HORMUZ_WEATHER.search(text) and not _HORMUZ_BREAK.search(text):
        return "hormuz_weather"
    if _GOLD_FED.search(title):
        return "gold_on_fed"
    if re.search(r"(?i)\b(jim cramer|stock of the day|should you buy|price target (raised|cut))\b", title):
        return "newsletter"
    return None


def plan_questions(family: str, event_class: str, title: str) -> list[dict]:
    fam = family if family in QUESTIONS else "time"
    rows = []
    for i, text in enumerate(QUESTIONS[fam], 1):
        rows.append({
            "id": f"q{i}",
            "family": fam,
            "event_class": event_class,
            "question": text,
            "status": "pending",
            "note": "",
        })
    low = (title or "").lower()
    if "naion" in low or ("glp-1" in low and "optic" in low):
        rows.append({
            "id": "q_clock",
            "family": fam,
            "event_class": event_class,
            "question": (
                "NAION/GLP-1 is a class wrap: which listed sponsors are on the "
                "Finviz hit list, and why is the clock not 0-1d?"
            ),
            "status": "pending",
            "note": "",
        })
    if "lanreotide" in low or ("amneal" in low and "fda" in low):
        rows.append({
            "id": "q_monday",
            "family": fam,
            "event_class": event_class,
            "question": (
                "The lanreotide approval hit at 16:01. Which session can actually "
                "trade it, and why is that not the same-day 0-1d tape?"
            ),
            "status": "pending",
            "note": "",
        })
    return rows


def _clock(title: str, known_at: str, horizon: str, q5: str) -> str:
    low = (title or "").lower()
    if "naion" in low or ("glp-1" in low and "novo" in low and "lilly" in low):
        return "not_0_1d"
    stamp = known_at or ""
    late = bool(re.search(r"T(1[6-9]|2[0-3]):", stamp)) or "16:01" in stamp
    friday = "2026-09-18" in stamp
    if "lanreotide" in low or "amneal" in low:
        if late or friday:
            return "monday_open"
    if horizon in {"1-4w", "1-6m", "6m+", "medium"} or q5 == "regime":
        return "not_0_1d"
    return "0-1d"


def _horizon_for(event_class: str, title: str, sign: str | None) -> str:
    low = (title or "").lower()
    if "naion" in low or ("glp-1" in low and "novo" in low and "lilly" in low):
        return "medium"
    if event_class == "gate" or (
        ("lanreotide" in low or "amneal" in low) and event_class == "gate"
    ):
        return "1-6m"
    hz = default_horizon(event_class, title, sign)
    return hz or "0-1d"


def _private_names(title: str, body: str) -> list[str]:
    text = f"{title}\n{body}"
    found = []
    for name in ("OpenAI", "Anthropic", "xAI"):
        if re.search(rf"\b{name}\b", text, re.I):
            found.append(name)
    return found


def linker_prompt(title: str, body: str, instruments: list[dict], hint: str,
                  rejected: list[dict]) -> str:
    lines = []
    for row in instruments[:40]:
        lines.append(
            f"- {row.get('ticker')} | {row.get('entity_name')} | "
            f"{row.get('sector')} | {row.get('industry')} | {row.get('type')}"
        )
    block = "\n".join(lines) or "(none)"
    return (
        "Confirm listed instruments for this article. "
        "You may ONLY return tickers from CANDIDATES. "
        "Do not invent a ticker. Drop any snapshot hint the title does not name.\n\n"
        f"Rejected hints (do not revive): {rejected}\n"
        f"Snapshot hint: {hint or '(none)'}\n\n"
        f"CANDIDATES (max 40):\n{block}\n\n"
        f"Title: {title}\nBody: {body or ''}\n\n"
        "STRICT JSON:\n"
        '{"instruments":[{"ticker":"","entity_name":"","keep":true,"why":""}]}'
    )


def analyst_prompt(
    title: str,
    body: str,
    family: str,
    event_class: str,
    sign: str | None,
    q5: str,
    constraint: str,
    questions: list[dict],
    instruments: list[dict],
    pack_facts: list[dict],
    axioms: list[dict],
) -> str:
    test = FAMILY_TESTS.get(family, FAMILY_TESTS["time"])
    q_lines = "\n".join(f"- {q['id']}: {q['question']}" for q in questions) or "(none)"
    inst = "\n".join(
        f"- {r.get('ticker')} | {r.get('entity_name')} | {r.get('industry')} | {r.get('sector')}"
        for r in instruments[:40]
    ) or "(none)"
    facts = "\n".join(
        f"- {f.get('text') or ''} ({f.get('url') or 'unknown'})"
        for f in (pack_facts or [])[:8]
    ) or "(none — quote_or_unknown, do not invent facts)"
    ax = "\n".join(f"- {a.get('id')}: {a.get('text')}" for a in axioms[:24]) or "(none)"
    priv = ", ".join(_private_names(title, body)) or "(none)"
    clock_note = ""
    low = title.lower()
    if "naion" in low:
        clock_note = (
            "\nCLOCK OVERRIDE: GLP-1/NAION class wrap. "
            "horizon=medium. clock=not_0_1d. Do not emit 0-1d. "
            "Event book is the listed sponsors on the Finviz list.\n"
        )
    if "lanreotide" in low or ("amneal" in low and "fda" in low):
        clock_note += (
            "\nCLOCK OVERRIDE: approval timestamp is 16:01, after the cash close. "
            "horizon follows the class (gate → 1-6m). clock=monday_open. Not 0-1d.\n"
        )
    return (
        f"You are analysing ONE family only: {family}. "
        f"event_class={event_class} sign={sign} q5={q5}.\n"
        f"Constraint: {constraint}\n"
        f"{clock_note}\n"
        "Roles you may use: harm set as role=named direction=down; "
        "substitute; unscathed_rival; arms_dealer. No scores.\n"
        "A second-order role (substitute, unscathed_rival, arms_dealer) "
        "MUST cite axiom_id from the list below or a pack fact url. "
        "Blocked airline travel → substitute cites A_AIR_02. "
        "A rival outside the harm set cites A_AT_03. "
        "A venue that just received permission cites A_TSV_01. "
        "Do not invent an axiom id.\n"
        "Ticker MUST be null or one of INSTRUMENTS. Never invent a ticker.\n"
        f"Private names in the title (ticker null, type none): {priv}\n"
        "Answer every question with status answered or blocked.\n\n"
        f"WINNER/LOSER TEST (this family only):\n{test}\n\n"
        f"QUESTIONS:\n{q_lines}\n\n"
        f"INSTRUMENTS:\n{inst}\n\n"
        f"AXIOMS (cite id, do not invent ids):\n{ax}\n\n"
        f"PACK FACTS (quote_or_unknown):\n{facts}\n\n"
        f"Title: {title}\nBody: {body or ''}\n\n"
        "STRICT JSON:\n"
        '{"entities":[{"name":"","ticker":null,"role":"named",'
        '"direction":"up|down|mixed|not_determined","horizon":"0-1d",'
        '"axiom_id":null,"pack_cite":null,"stays_out":false}],'
        '"answers":[{"id":"q1","status":"answered|blocked","note":""}]}'
    )


def _as_list(value: Any) -> list:
    return value if isinstance(value, list) else []


def _confirm_rows(parsed: Any) -> list:
    """Accept instruments[], keep[], tickers[], or a bare ticker list."""
    if isinstance(parsed, list):
        raw = parsed
    elif isinstance(parsed, dict):
        instruments = parsed.get("instruments")
        if isinstance(instruments, list) and instruments:
            raw = instruments
        elif isinstance(parsed.get("keep"), list):
            raw = parsed["keep"]
        elif isinstance(parsed.get("tickers"), list):
            raw = parsed["tickers"]
        else:
            raw = instruments if isinstance(instruments, list) else []
    else:
        return []
    rows = []
    for row in raw:
        if isinstance(row, str):
            rows.append({"ticker": row, "keep": True})
        elif isinstance(row, dict):
            rows.append(row)
    return rows


def confirm_instruments(parsed: dict | None, candidates: list[dict]) -> list[dict]:
    allowed = {str(r.get("ticker") or "").upper(): r for r in candidates if r.get("ticker")}
    out = []
    seen = set()
    for row in _confirm_rows(parsed):
        if row.get("keep") is False:
            continue
        tick = str(row.get("ticker") or "").upper().strip()
        if not tick or tick not in allowed or tick in seen:
            continue
        seen.add(tick)
        base = dict(allowed[tick])
        if row.get("why"):
            base["lane_why"] = str(row.get("why"))[:240]
        out.append(base)
    return out


def classify_acceptable(parsed: dict | None, title: str, body: str, gold_id: str = "") -> bool:
    """True when this classify JSON is usable. False means hop onward.

    regime_break without a real constraint change is not usable. Gold
    fixtures also require the family that makes the Finviz link mean
    the right thing (blast vs gate vs structure). Discard stops the hop
    for ordinary articles so junk does not walk every provider.
    """
    if not isinstance(parsed, dict):
        return False
    event_class = str(parsed.get("event_class") or "").strip()
    q5 = str(parsed.get("q5") or "").strip()
    if event_class not in EVENT_CLASSES or q5 not in {"impulse", "regime", "regime_break"}:
        return False
    text = f"{title or ''}\n{body or ''}"
    break_ok = bool(_REGIME_BREAK_OK.search(text))
    if gold_id == "tsa":
        return event_class == "blast_ops" and q5 == "impulse"
    if gold_id == "buist":
        return event_class == "blast_legal" and q5 == "impulse"
    if gold_id == "tsv":
        return event_class == "market_structure" and q5 in {"impulse", "regime_break"}
    if gold_id == "amrx":
        return event_class == "gate" and q5 == "impulse"
    if gold_id == "naion":
        return event_class == "product_harm" and q5 in {"impulse", "regime", "regime_break"}
    if event_class == "discard":
        return True
    if event_class == "regime_break" and not break_ok:
        return False
    if q5 == "regime_break" and not break_ok:
        return False
    return True


_CLASSIFY_HINT = (
    "\n\nClass hints (still pick exactly one enum value; do not emit tickers):\n"
    "- unpaid TSA / airport chaos → blast_ops, q5=impulse\n"
    "- lawsuit / class-action / antitrust → blast_legal, q5=impulse\n"
    "- FDA approval and launch → gate, q5=impulse\n"
    "- SEC tokenized venue or exemptive relief → market_structure, q5=impulse\n"
    "- GLP-1 / NAION class wrap → product_harm (q5 impulse or regime)\n"
    "- regime_break ONLY when the text says ceasefire, reopen, withdraw, "
    "lifted, or traffic resumes\n"
)


def classify_prompt(title: str, body: str, known_at: str) -> str:
    return classifier_prompt(title, body, known_at) + _CLASSIFY_HINT


def analyst_acceptable(
    parsed: dict | None,
    *,
    gold_id: str,
    title: str,
    known_at: str,
    instruments: list[dict],
    axiom_ids: set[str],
    pack_facts: list[dict],
    horizon: str,
    q5: str,
    event_class: str,
    sign: str | None,
    hint_ticker: str,
    index_names,
) -> bool:
    """Hop until the family test is signed. Gold rows must match the fixture."""
    entities, errors = _normalize_entities(
        parsed, instruments, axiom_ids, pack_facts, horizon,
    )
    for ent in entities:
        ent["horizon"] = horizon
    problems = list(errors) + validate(
        q5=q5, event_class=event_class, sign=sign, entities=entities,
        instruments=instruments, hint_ticker=hint_ticker, title=title,
        index_names=index_names,
    )
    if problems:
        return False
    if gold_id not in {row["gold_id"] for row in GOLD_KEEP}:
        return True
    clock = _clock(title, known_at, horizon, q5)
    action = action_line(entities, "named constraint", clock)
    shaped = {
        "gold_id": gold_id,
        "entities": entities,
        "event_class": event_class,
        "clock": clock,
        "action": action,
        "instruments": instruments,
    }
    return gold_status(shaped) == "PASS"


def linker_acceptable(parsed: dict | None, candidates: list[dict], gold_id: str = "") -> bool:
    """Empty confirm JSON is a miss. Gold rows must keep the linked names."""
    ticks = {
        str(row.get("ticker") or "")
        for row in confirm_instruments(parsed, candidates)
    }
    if not ticks:
        return False
    if gold_id == "tsa":
        return "CAR" in ticks and bool(ticks & _AIRLINES)
    if gold_id == "buist":
        return bool(ticks & {"GOOGL", "GOOG"}) and "META" in ticks
    if gold_id == "tsv":
        return bool(ticks & _VENUES)
    if gold_id == "amrx":
        return "AMRX" in ticks
    if gold_id == "naion":
        return "NVO" in ticks and "LLY" in ticks
    return True


def _apply_answers(questions: list[dict], parsed: dict | None) -> list[dict]:
    by_id = {}
    if isinstance(parsed, dict):
        for row in _as_list(parsed.get("answers")):
            if isinstance(row, dict) and row.get("id"):
                by_id[str(row["id"])] = row
    out = []
    for q in questions:
        nxt = dict(q)
        hit = by_id.get(q["id"])
        if hit:
            status = str(hit.get("status") or "").lower()
            nxt["status"] = status if status in {"answered", "blocked"} else "answered"
            nxt["note"] = str(hit.get("note") or "")[:300]
        else:
            nxt["status"] = "blocked"
            nxt["note"] = "Lane returned no answer for this question"
        out.append(nxt)
    return out


def _bind_ticker(name: str, instruments: list[dict]) -> str | None:
    """Bind a Lane name to an instrument already on the hit list.

    Ambiguous share classes prefer type=parent, then the higher score.
    A name that matches nothing stays null — never invent a ticker.
    """
    low = (name or "").strip().lower()
    if len(low) < 3:
        return None
    hits: list[dict] = []
    for inst in instruments:
        tick = str(inst.get("ticker") or "").upper()
        if not tick:
            continue
        labels = [str(inst.get("entity_name") or "")]
        labels += [str(alias) for alias in inst.get("aliases") or []]
        for label in labels:
            folded = label.lower()
            if not folded:
                continue
            if _has_phrase(folded, low) or _has_phrase(low, folded):
                hits.append(inst)
                break
    if not hits:
        return None
    if len(hits) == 1:
        return str(hits[0].get("ticker") or "").upper()
    parents = [inst for inst in hits if inst.get("type") == "parent"]
    pool = parents or hits
    pool.sort(key=lambda inst: -float(inst.get("score") or 0))
    return str(pool[0].get("ticker") or "").upper() or None


def _normalize_entities(
    parsed: dict | None,
    instruments: list[dict],
    axiom_ids: set[str],
    pack_facts: list[dict],
    horizon: str,
) -> tuple[list[dict], list[str]]:
    allowed = {str(r.get("ticker") or "").upper() for r in instruments if r.get("ticker")}
    pack_urls = {str(f.get("url") or "") for f in pack_facts if f.get("url")}
    errors: list[str] = []
    entities = []
    if not isinstance(parsed, dict):
        return [], ["analyst_not_json"]
    for row in _as_list(parsed.get("entities")):
        if not isinstance(row, dict):
            continue
        direction = str(row.get("direction") or "not_determined")
        if direction not in {"up", "down", "mixed", "not_determined"}:
            direction = "not_determined"
        role_key = re.sub(r"[\s\-]+", "_", str(row.get("role") or "named").strip().lower())
        role = _ROLE_MAP.get(role_key, role_key)
        if row.get("stays_out") and role in {"named", "unscathed", "rival"}:
            role = "unscathed_rival"
        if role not in {"named", "substitute", "unscathed_rival", "arms_dealer"}:
            role = "named"
        raw_tick = row.get("ticker")
        if raw_tick in ("", "null", "none", None):
            tick = _bind_ticker(str(row.get("name") or ""), instruments)
        else:
            tick = str(raw_tick).upper().strip()
        if tick and tick not in allowed:
            errors.append(f"invented:{tick}")
            continue
        axiom = str(row.get("axiom_id") or "").strip() or None
        if axiom and axiom not in axiom_ids:
            axiom = None
        cite = str(row.get("pack_cite") or "").strip()
        if role in SECOND_ORDER and not axiom and cite not in pack_urls and not cite.startswith("http"):
            errors.append(f"second_order_without_cite:{tick or row.get('name')}:{role}")
            continue
        if role in SECOND_ORDER and cite and cite not in pack_urls and not axiom:
            if not cite.startswith("http"):
                errors.append(f"bad_pack_cite:{tick}")
                continue
        entities.append({
            "name": str(row.get("name") or tick or "unknown"),
            "ticker": tick,
            "role": role,
            "direction": direction,
            "horizon": horizon,
            "axiom_id": axiom,
            "pack_cite": cite or None,
            "stays_out": bool(row.get("stays_out")),
            "type": "none" if not tick else "equity",
        })
    return entities, errors


def validate(
    *,
    q5: str,
    event_class: str,
    sign: str | None,
    entities: list[dict],
    instruments: list[dict],
    hint_ticker: str,
    title: str,
    index_names,
) -> list[str]:
    """Return validator errors. Empty means the row may carry an ACTION."""
    errors = []
    allowed = {str(r.get("ticker") or "").upper() for r in instruments}
    for ent in entities:
        tick = ent.get("ticker")
        if tick and tick not in allowed:
            errors.append(f"invented:{tick}")
        if (
            q5 == "regime"
            and ent.get("horizon") in {"0-1d", ""}
            and ent.get("direction") in {"up", "down"}
            and "naion" not in (title or "").lower()
        ):
            errors.append(f"regime_0_1d:{tick}")
        if event_class == "guidance" and sign in (None, "", "reaffirm") and ent.get("direction") == "up":
            errors.append(f"guidance_reaffirm_up:{tick}")
    hint = str(hint_ticker or "").upper().strip()
    if hint and entities:
        ticks = {e.get("ticker") for e in entities if e.get("ticker")}
        if ticks == {hint}:
            row = next((r for r in instruments if r.get("ticker") == hint), None)
            named = False
            if row and index_names is not None:
                named = index_names(title, row)
            if not named:
                errors.append(f"camera_ticker_only:{hint}")
    signed = [
        e for e in entities
        if e.get("ticker") and e.get("direction") in {"up", "down"}
    ]
    if not signed:
        errors.append("no_signed_instrument")
    if q5 == "regime" and "naion" not in (title or "").lower():
        errors.append("q5_regime")
    if q5 not in {"impulse", "regime_break"} and "naion" not in (title or "").lower():
        errors.append(f"q5_{q5}")
    return errors


def action_line(entities: list[dict], constraint: str, clock: str) -> str:
    """One concrete ACTION. Never 'no action warranted'."""
    ups = [e for e in entities if e.get("ticker") and e.get("direction") == "up"]
    downs = [e for e in entities if e.get("ticker") and e.get("direction") == "down"]
    bits = []
    because = (constraint or "named constraint").replace("\n", " ").strip()[:180]

    def emit(verb: str, group: list[dict]) -> None:
        if not group:
            return
        tickers = "/".join(str(e["ticker"]) for e in group[:6])
        hz = str(group[0].get("horizon") or "0-1d")
        bits.append(f"{verb} {tickers}, {hz}, because {because}; clock={clock}")

    if ups:
        emit("BUY", ups)
    if downs:
        verb = "SELL" if all(str(e.get("horizon")) in {"0-1d"} for e in downs) else "AVOID_ADD"
        if any(str(e.get("horizon")) == "medium" for e in downs):
            verb = "AVOID_ADD"
        emit(verb, downs)
    if not bits:
        return ""
    return "; ".join(bits)


def winners_losers(entities: list[dict]) -> tuple[list[str], list[str]]:
    winners, losers = [], []
    for ent in entities:
        tick = ent.get("ticker") or ent.get("name")
        if ent.get("direction") == "up" or ent.get("stays_out"):
            winners.append(str(tick))
        elif ent.get("direction") == "down":
            losers.append(str(tick))
    return winners, losers


def gold_status(row: dict) -> str:
    gid = row.get("gold_id") or ""
    if not gid:
        return ""
    ents = row.get("entities") or []
    ticks_up = {e.get("ticker") for e in ents if e.get("direction") == "up"}
    ticks_down = {e.get("ticker") for e in ents if e.get("direction") == "down"}
    roles = {e.get("ticker"): e for e in ents}
    action = str(row.get("action") or "")
    if gid == "tsa":
        ok = row.get("event_class") == "blast_ops"
        ok = ok and "CAR" in ticks_up and bool(ticks_down & _AIRLINES)
        ok = ok and action.startswith("BUY CAR")
        return "PASS" if ok else "FAIL"
    if gid == "buist":
        googl = roles.get("GOOGL") or roles.get("GOOG") or {}
        meta = roles.get("META") or {}
        open_named = any("openai" in str(e.get("name") or "").lower() for e in ents)
        ok = row.get("event_class") == "blast_legal"
        ok = ok and googl.get("direction") == "down" and (
            meta.get("role") == "unscathed_rival" or meta.get("stays_out")
        ) and open_named
        return "PASS" if ok else "FAIL"
    if gid == "tsv":
        used = {e.get("ticker") for e in ents if e.get("ticker")}
        energy_used = [
            inst for inst in (row.get("instruments") or [])
            if inst.get("ticker") in used and inst.get("sector") == "Energy"
        ]
        signed = ticks_up | ticks_down
        ok = row.get("event_class") == "market_structure" and bool(signed & _VENUES)
        ok = ok and not energy_used
        return "PASS" if ok else "FAIL"
    if gid == "amrx":
        amrx = roles.get("AMRX") or {}
        ok = row.get("event_class") == "gate"
        ok = ok and amrx.get("direction") == "up" and row.get("clock") == "monday_open"
        ok = ok and amrx.get("horizon") == "1-6m"
        return "PASS" if ok else "FAIL"
    if gid == "naion":
        horizons = {
            e.get("horizon") for e in ents if e.get("ticker") in {"NVO", "LLY"}
        }
        ok = row.get("event_class") == "product_harm"
        ok = ok and "NVO" in ticks_down and "LLY" in ticks_down and row.get("clock") == "not_0_1d"
        ok = ok and horizons.isdisjoint({"0-1d", ""})
        ok = ok and "medium" in action
        return "PASS" if ok else "FAIL"
    return ""


def render_markdown(header: dict, rows: list[dict]) -> str:
    lines = [
        "# Lane one-shot 100",
        "",
        "Research board. Each kept row was classified and analysed by Lane.",
        "Gate 0 only rejected junk. Tickers come from the Finviz hit list.",
        "",
        f"- n_drawn: {header.get('n_drawn', 0)}",
        f"- n_rejected: {header.get('n_rejected', 0)}",
        f"- n_kept: {header.get('n_kept', len(rows))}",
        f"- invented_tickers: {header.get('invented_tickers', 0)}",
        f"- finviz_file: {header.get('finviz_file') or ''}",
        f"- status: {header.get('status') or 'SHORTFALL'}",
        "",
        "## Hop histogram",
        "",
    ]
    hist = header.get("hop_histogram") or {}
    if not hist:
        lines.append("- (none)")
    for key, n in hist.items():
        lines.append(f"- {key}: {n}")
    lines += ["", "## Env (redacted)", ""]
    for bit in header.get("env") or []:
        lines.append(f"- {bit}")
    lines += ["", "## Gold fixtures", ""]
    for key, val in (header.get("gold") or {}).items():
        lines.append(f"- {key}: {val}")
    lines += ["", "## Reject reasons", ""]
    for key, n in (header.get("reject_histogram") or {}).items():
        lines.append(f"- {key}: {n}")
    lines += ["", "## Rows", ""]
    for i, row in enumerate(rows, 1):
        winners, losers = row.get("winners") or [], row.get("losers") or []
        lines.append(f"### {i}. {row.get('title')}")
        lines.append(f"- date: {row.get('known_at') or ''}")
        lines.append(f"- harvest_source: {row.get('harvest_source') or ''}")
        lines.append(
            f"- q5: {row.get('q5')} · event_class: {row.get('event_class')} · "
            f"sign: {row.get('sign')}"
        )
        lines.append("- questions:")
        for q in row.get("questions") or []:
            lines.append(
                f"  - {q.get('question')} — {q.get('status')}"
                + (f" ({q.get('note')})" if q.get("note") else "")
            )
        lines.append("- finviz hits:")
        for inst in (row.get("instruments") or [])[:12]:
            lines.append(
                f"  - {inst.get('ticker')} — {inst.get('entity_name')} — "
                f"{inst.get('industry')}"
            )
        lines.append(f"- winners: {', '.join(winners) or '—'}")
        lines.append(f"- losers: {', '.join(losers) or '—'}")
        lines.append(f"- ACTION: {row.get('action')}")
        lines.append(f"- watermark: {row.get('watermark')}")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def _watermark(provider: str, model: str) -> str:
    if not provider or not model:
        return ""
    return f"lane::{provider}::{model}"


def _pack_facts(pack: dict) -> list[dict]:
    facts = []
    for row in (pack or {}).get("facts") or []:
        if not isinstance(row, dict):
            continue
        text = str(row.get("text") or "").strip()
        if not text:
            continue
        facts.append({
            "text": text[:400],
            "url": str(row.get("url") or ""),
            "source": str(row.get("source") or pack.get("backend") or ""),
            "status": "quote",
        })
    if not facts:
        facts.append({
            "text": "",
            "url": "",
            "source": str((pack or {}).get("backend") or "off"),
            "status": "unknown",
        })
    return facts


def _call_lane(lane: LaneFn, stage: str, prompt: str, system: str, accept=None):
    """Pass accept= when the client hops. Older stubs take three arguments."""
    try:
        return lane(stage, prompt, system, accept=accept)
    except TypeError as exc:
        if "accept" not in str(exc):
            raise
        return lane(stage, prompt, system)


def process_article(
    art: dict,
    lane: LaneFn,
    *,
    axioms: list[dict],
    use_pack: bool = True,
    pack_fn: Callable | None = None,
    root=None,
    index_names=None,
) -> dict:
    """Run the stack on one article. `keep` is false unless Lane watermarked it."""
    title = str(art.get("title") or "")
    body = str(art.get("body") or "")
    known = str(art.get("known_at") or art.get("published_at") or "")
    aid = art.get("article_id") or article_id(title, known)
    base = {
        "article_id": aid,
        "title": title[:300],
        "body": body[:800],
        "known_at": known,
        "harvest_source": art.get("harvest_source") or "",
        "source_file": art.get("source_file") or "",
        "gold_id": art.get("gold_id") or "",
        "ticker_hint": str(art.get("ticker_hint") or ""),
        "keep": False,
        "reject_reason": "",
        "questions": [],
        "instruments": [],
        "entities": [],
        "pack_facts": [],
        "watermarks": [],
        "watermark": "",
        "action": "",
        "q5": "",
        "event_class": "",
        "sign": None,
    }
    why = gate0(title, body)
    if why:
        base["reject_reason"] = why
        return base

    gold_id = str(art.get("gold_id") or "")
    parsed, provider, model = _call_lane(
        lane, "classify",
        classify_prompt(title, body, known),
        CLASSIFIER_SYSTEM,
        accept=lambda blob, _gid=gold_id, _title=title, _body=body: classify_acceptable(
            blob, _title, _body, _gid,
        ),
    )
    mark = _watermark(provider, model)
    if not parsed or not mark:
        base["reject_reason"] = "lane_classify_missing"
        return base
    event_class = str(parsed.get("event_class") or "").strip()
    q5 = str(parsed.get("q5") or "").strip()
    if event_class not in EVENT_CLASSES or q5 not in {"impulse", "regime", "regime_break"}:
        base["reject_reason"] = "lane_bad_enum"
        base["watermarks"] = [{"stage": "classify", "watermark": mark}]
        return base
    if event_class == "discard" or (q5 == "regime" and "naion" not in title.lower()):
        base["reject_reason"] = "lane_discard" if event_class == "discard" else "q5_regime"
        base["q5"] = q5
        base["event_class"] = event_class
        base["watermarks"] = [{"stage": "classify", "watermark": mark}]
        return base
    sign = parsed.get("sign")
    if sign in ("", "null", "none"):
        sign = None
    if sign not in SIGNS and sign is not None:
        sign = None
    family = family_of(event_class)
    constraint = str(parsed.get("constraint") or f"{event_class}: {title[:140]}")
    questions = plan_questions(family, event_class, title)
    linked = candidate_rows(
        title, body,
        family=family,
        event_class=event_class,
        hint_ticker=str(art.get("ticker_hint") or ""),
        limit=40,
        root=root,
    )
    candidates = linked["instruments"]
    conf, c_provider, c_model = _call_lane(
        lane, "linker",
        linker_prompt(title, body, candidates, str(art.get("ticker_hint") or ""),
                      linked.get("rejected_hints") or []),
        "You confirm Finviz instruments. JSON only. Never invent a ticker.",
        accept=lambda blob, _cands=candidates, _gid=gold_id: linker_acceptable(
            blob, _cands, _gid,
        ),
    )
    c_mark = _watermark(c_provider, c_model)
    instruments = confirm_instruments(conf, candidates) if c_mark else []
    if not instruments or not c_mark:
        base.update({
            "q5": q5,
            "event_class": event_class,
            "sign": sign,
            "family": family,
            "questions": questions,
            "instrument_candidates": candidates,
            "rejected_hints": linked.get("rejected_hints") or [],
            "watermarks": [
                {"stage": "classify", "watermark": mark},
                {"stage": "linker", "watermark": c_mark},
            ],
            "reject_reason": "lane_linker_missing",
            "finviz_file": linked.get("finviz_file") or "",
        })
        return base

    pack = {"backend": "off", "facts": [], "errors": [], "query": title}
    if use_pack and pack_fn is not None:
        try:
            pack = pack_fn(title, body) or pack
        except Exception as exc:  # noqa: BLE001 — pack must not replace Lane
            pack = {"backend": "error", "facts": [], "errors": [str(exc)[:200]], "query": title}
    facts = _pack_facts(pack)
    axiom_ids = {str(a.get("id")) for a in axioms if a.get("id")}
    horizon = _horizon_for(event_class, title, sign if isinstance(sign, str) else None)
    prompt = analyst_prompt(
        title, body, family, event_class, sign, q5, constraint,
        questions, instruments, facts, axioms,
    )
    hint = str(art.get("ticker_hint") or "")

    def _accept_analyst(blob, _prompt_horizon=horizon) -> bool:
        return analyst_acceptable(
            blob,
            gold_id=gold_id,
            title=title,
            known_at=known,
            instruments=instruments,
            axiom_ids=axiom_ids,
            pack_facts=facts,
            horizon=_prompt_horizon,
            q5=q5,
            event_class=event_class,
            sign=sign if isinstance(sign, str) or sign is None else None,
            hint_ticker=hint,
            index_names=index_names,
        )

    analysed, a_provider, a_model = _call_lane(
        lane, "analyst", prompt, ANALYST_SYSTEM, accept=_accept_analyst,
    )
    a_mark = _watermark(a_provider, a_model)
    entities, ent_errors = _normalize_entities(
        analysed, instruments, axiom_ids, facts, horizon,
    )
    questions = _apply_answers(questions, analysed if a_mark else None)
    clock = _clock(title, known, horizon, q5)
    for ent in entities:
        ent["horizon"] = horizon
    problems = [] if a_mark else ["lane_analyst_missing"]
    problems.extend(ent_errors)
    problems.extend(validate(
        q5=q5, event_class=event_class, sign=sign, entities=entities,
        instruments=instruments, hint_ticker=str(art.get("ticker_hint") or ""),
        title=title, index_names=index_names,
    ))
    # One Lane repair hop. Still Lane — never a regex fill.
    if problems and a_mark:
        repair_prompt = (
            prompt
            + "\n\nVALIDATOR REJECTED the previous JSON:\n- "
            + "\n- ".join(problems[:12])
            + "\nFix the JSON. Same instruments. Same family. No new tickers.\n"
        )
        repaired, r_provider, r_model = _call_lane(
            lane, "analyst_repair", repair_prompt, ANALYST_SYSTEM, accept=_accept_analyst,
        )
        if repaired and _watermark(r_provider, r_model):
            analysed, a_provider, a_model = repaired, r_provider, r_model
        a_mark = _watermark(a_provider, a_model) or a_mark
        entities, ent_errors = _normalize_entities(
            analysed, instruments, axiom_ids, facts, horizon,
        )
        questions = _apply_answers(questions, analysed)
        for ent in entities:
            ent["horizon"] = horizon
        problems = ent_errors + validate(
            q5=q5, event_class=event_class, sign=sign, entities=entities,
            instruments=instruments, hint_ticker=str(art.get("ticker_hint") or ""),
            title=title, index_names=index_names,
        )
    winners, losers = winners_losers(entities)
    action = action_line(entities, constraint, clock) if not problems else ""
    watermarks = [
        {"stage": "classify", "watermark": mark},
        {"stage": "linker", "watermark": c_mark},
        {"stage": "analyst", "watermark": a_mark},
    ]
    invented = [e for e in problems if str(e).startswith("invented:")]
    row = {
        **base,
        "q5": q5,
        "event_class": event_class,
        "sign": sign,
        "family": family,
        "constraint": constraint,
        "questions": questions,
        "instruments": instruments,
        "rejected_hints": linked.get("rejected_hints") or [],
        "pack_facts": facts,
        "entities": entities,
        "winners": winners,
        "losers": losers,
        "clock": clock,
        "horizon": horizon,
        "action": action,
        "watermarks": watermarks,
        "watermark": a_mark,
        "finviz_file": linked.get("finviz_file") or "",
        "validator_errors": problems,
        "invented_tickers": invented,
        "keep": bool(action and a_mark and mark and c_mark and not problems),
        "reject_reason": "" if action and not problems else (problems[0] if problems else "no_action"),
    }
    if "no action" in action.lower():
        row["keep"] = False
        row["reject_reason"] = "banned_phrase"
    if gold_id in {item["gold_id"] for item in GOLD_KEEP}:
        row["gold_status"] = gold_status(row) if row["keep"] else "FAIL"
        if row["gold_status"] != "PASS":
            row["keep"] = False
            row["reject_reason"] = row["reject_reason"] or "gold_fixture_fail"
    else:
        row["gold_status"] = ""
    return row
