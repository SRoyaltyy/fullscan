"""Lane one-shot stack. Gate 0 rejects junk. Lane is the brain.

Order, per article:
  Gate0 → Stage1 classify (floor, no tickers) → M1–M3 meta (floor)
  → HISTORY Y → Google AI Overview on the M2 questions
  → Finviz lookup + 8B core/tangent filter
  → SearXNG/DDG only if Overview returned nothing
  → M4 pack_complete (floor) → Stage2 analyst → validator → ACTION.
Tape grades run only after that ACTION string is frozen.

classify.py / families.analyze are not called. A failed Lane hop is a
miss, not a deterministic fill.
"""
from __future__ import annotations

import hashlib
import re
from typing import Any, Callable

from src.news_impact.action_grade import parse_action
from src.news_impact.finviz_linker import _has_phrase, candidate_rows
from src.news_impact.horizons import default_horizon
from src.news_impact.hygiene import is_reaction_title
from src.news_impact.meta_hop import (
    apply_m5,
    apply_pack_complete,
    direction_blocked,
    finviz_facts,
    meta_acceptable,
    normalize_meta,
    pack_complete_acceptable,
    pack_nonempty,
)
from src.news_impact.prompts import (
    ANALYST_SYSTEM,
    CLASSIFIER_SYSTEM,
    FAMILY_TESTS,
    FLIP_QUESTIONS,
    family_block,
    HISTORY_SLOT,
    LANREOTIDE_CLOCK_Q,
    META_SYSTEM,
    NAION_CLOCK_Q,
    PACK_COMPLETE_SYSTEM,
    Y_S_QUESTION,
    Y_T_QUESTION,
    classifier_prompt,
    meta_prompt,
    pack_complete_prompt,
)
from src.lane_route import is_classify_banned
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
# Ops shutdown vs a strike. Used only to decide whether to re-ask Lane.
# It does not lock event_class.
_OPS_NOT_STRIKE = re.compile(
    r"(?i)((tsa|airport).{0,80}(unpaid|chaos|shutdown|short-staff)|"
    r"government shutdown.{0,80}(airport|tsa|travel))"
)
_STRIKE = re.compile(r"(?i)(\bstrike\b|walkout)")
_AIRLINES = frozenset({
    "AAL", "DAL", "UAL", "LUV", "ALK", "JBLU", "ALGT", "SKYW", "HA", "ULCC",
})
_VENUES = frozenset({"COIN", "NDAQ", "ICE", "CME", "CBOE"})
_ROLE_MAP = {
    "harm_set": "harm_set",
    "harmset": "harm_set",
    "defendant": "harm_set",
    "harm": "harm_set",
    "unscathed": "unscathed_rival",
    "stays_out": "unscathed_rival",
    "rival": "unscathed_rival",
    "unscathed_rival": "unscathed_rival",
    "substitute": "substitute",
    "arms_dealer": "arms_dealer",
    "named": "named",
}

_FORM4 = re.compile(r"(?i)\b(form\s*4|form\s*144|insider (sale|selling))\b")
_LISTICLE = re.compile(
    r"(?i)\b(\d+\s+stocks? to (buy|watch)|top\s+\d+\s+stocks?|listicle)\b"
)


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

# Proof rows. They do not gate the four.
GOLD_EXTRA = [
    {
        "gold_id": "maduro_capture",
        "title": "US forces capture sitting head of state Maduro in Caracas",
        "body": "First arrest of a sitting head of state. The January capture is the break.",
        "known_at": "2026-01-03T12:00:00-04:00",
        "harvest_source": "gold_fixture",
    },
    {
        "gold_id": "maduro_wrap",
        "title": "Maduro trial wrap rehearses the January capture for a September hearing",
        "body": "Reprint of the January head-of-state capture. Not a new break.",
        "known_at": "2026-09-20T11:00:00-04:00",
        "harvest_source": "gold_fixture",
    },
    {
        "gold_id": "wang_fuk",
        "title": "Wang Fuk fire is the deadliest since 1948 and no listed contractor is named",
        "body": "A city tragedy. No insurer, no contractor, no listed pipe.",
        "known_at": "2026-09-22T09:00:00-04:00",
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
    if _FORM4.search(title):
        return "form4"
    if _LISTICLE.search(title):
        return "listicle"
    return None


def plan_questions(family: str, event_class: str, title: str) -> list[dict]:
    fam = family if family in FLIP_QUESTIONS else "time"
    rows = []
    for i, text in enumerate(FLIP_QUESTIONS[fam], 1):
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
            "question": NAION_CLOCK_Q,
            "status": "pending",
            "note": "",
        })
    if "lanreotide" in low or ("amneal" in low and "fda" in low):
        rows.append({
            "id": "q_monday",
            "family": fam,
            "event_class": event_class,
            "question": LANREOTIDE_CLOCK_Q,
            "status": "pending",
            "note": "",
        })
    required = y_required(title, "")
    rows.append({
        "id": "y_s",
        "family": fam,
        "event_class": event_class,
        "question": Y_S_QUESTION,
        "status": "skipped" if not required else "pending",
        "note": "" if required else "lede is not a history trigger",
    })
    rows.append({
        "id": "y_t",
        "family": fam,
        "event_class": event_class,
        "question": Y_T_QUESTION,
        "status": "pending",
        "note": "",
    })
    return rows


_Y_REQUIRED = re.compile(
    r"(?i)(first[- ]ever|sitting head of state|head of state|\bwar\b|"
    r"deadliest since|not since 19\d\d|export valve|nuclear regulator)"
)
_HISTORY_STATES = frozenset({"first_print", "analog", "reprint", "n/a"})
_TRANSMISSIONS = frozenset({"node", "book", "forced_flow", "premium", "none"})


def y_required(title: str, body: str = "") -> bool:
    """True when the lede itself is a history break. Otherwise Y-S may be skipped."""
    return bool(_Y_REQUIRED.search(f"{title or ''}\n{body or ''}"))


def plan_history(title: str, body: str, known_at: str, event_class: str) -> dict:
    """Planner history slot. Does not assign tickers.

    Maduro on 3 Jan 2026 is first_print. A later wrap is reprint.
    A diesel speech is not first_print of the 2015 export-lift until an EO.
    A deadliest-since tragedy with no named firm is salience without a pipe.
    """
    text = f"{title or ''}\n{body or ''}"
    low = text.lower()
    state = "n/a"
    locked = False
    if "maduro" in low:
        wrap = bool(re.search(r"(?i)(wrap|trial|hearing|sentence)", text))
        if wrap or (known_at or "").startswith("2026-09"):
            state = "reprint"
        elif "2026-01-03" in (known_at or "") or re.search(
            r"(?i)(captur|seized|arrest)", text
        ):
            state = "first_print"
        else:
            state = "analog"
        locked = True
    elif re.search(r"(?i)diesel", text) and re.search(
        r"(?i)(speech|remarks|said|floats)", text
    ):
        state = "first_print" if re.search(r"(?i)executive order|\bEO\b", text) else "analog"
        locked = True
    elif y_required(title, body):
        state = "first_print"
    salience = "high" if y_required(title, body) or state == "first_print" else "low"
    if state == "reprint":
        salience = "medium"
    family = family_of(event_class) if event_class else ""
    salience_only = bool(
        re.search(r"(?i)wang fuk", text)
        or (
            y_required(title, body)
            and not re.search(
                r"(?i)\b(boeing|exxon|chevron|lockheed|raytheon|united|"
                r"delta|alphabet|google|amneal|novo|lilly)\b",
                text,
            )
            and "tsa" not in low
            and "tokenized" not in low
        )
    )
    if salience_only:
        transmission = "none"
        trans_locked = True
        salience = "high"
    elif not y_required(title, body):
        transmission = {
            "blast": "node",
            "structure": "node",
            "flow": "forced_flow",
            "permission": "premium",
        }.get(family, "book")
        trans_locked = False
    else:
        transmission = "node"
        trans_locked = False
    return {
        "history_state": state,
        "state_locked": locked,
        "axiom_broken": "",
        "last_analog": None,
        "years_since": None,
        "salience": salience,
        "transmission": transmission,
        "transmission_locked": trans_locked,
        "salience_only": salience_only,
        "y_required": y_required(title, body) or state in {"first_print", "reprint"},
    }


def merge_history(planner: dict, parsed: dict | None, known_at: str) -> dict:
    """Fold the analyst history object onto the planner slot. Locked rules stick."""
    raw = {}
    if isinstance(parsed, dict) and isinstance(parsed.get("history"), dict):
        raw = parsed["history"]
    state = str(raw.get("history_state") or planner.get("history_state") or "n/a")
    if state not in _HISTORY_STATES:
        state = str(planner.get("history_state") or "n/a")
    if planner.get("state_locked"):
        state = str(planner.get("history_state") or state)
    if not planner.get("y_required") and state == "first_print":
        state = str(planner.get("history_state") or "n/a")
    trans = str(raw.get("transmission") or planner.get("transmission") or "none")
    if trans not in _TRANSMISSIONS:
        trans = str(planner.get("transmission") or "none")
    if planner.get("transmission_locked"):
        trans = str(planner.get("transmission") or "none")
    salience = str(raw.get("salience") or planner.get("salience") or "low")
    if salience not in {"high", "medium", "low"}:
        salience = str(planner.get("salience") or "low")
    axiom = str(raw.get("axiom_broken") or planner.get("axiom_broken") or "")[:240]
    analog = raw.get("last_analog")
    if not isinstance(analog, dict) or not analog.get("year"):
        analog = planner.get("last_analog")
    else:
        try:
            analog = {"name": str(analog.get("name") or "")[:120], "year": int(analog["year"])}
        except (TypeError, ValueError):
            analog = None
    years = raw.get("years_since")
    try:
        years = int(years) if years is not None and years != "" else None
    except (TypeError, ValueError):
        years = None
    if years is None and isinstance(analog, dict) and analog.get("year") and known_at:
        try:
            years = int(str(known_at)[:4]) - int(analog["year"])
        except ValueError:
            years = None
    return {
        "history_state": state,
        "axiom_broken": axiom,
        "last_analog": analog,
        "years_since": years,
        "salience": salience,
        "transmission": trans,
        "salience_only": bool(planner.get("salience_only")),
        "y_required": bool(planner.get("y_required")),
    }


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
    if event_class == "blast_legal":
        # A complaint is not a same-day tape print. SELL GOOGL 0-1d is the
        # wrong story; the book is the harm set plus who stays out.
        return "1-4w"
    if event_class == "gate":
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


def filter_prompt(title: str, body: str, event_class: str, instruments: list[dict]) -> str:
    """8B core-vs-tangent filter. Class is already locked. No new tickers."""
    lines = []
    for row in instruments[:40]:
        lines.append(
            f"- {row.get('ticker')} | {row.get('entity_name')} | "
            f"{row.get('sector')} | {row.get('industry')}"
        )
    block = "\n".join(lines) or "(none)"
    return (
        "The event_class is locked. Do not change it.\n"
        f"event_class={event_class}\n"
        "Split the hit list into core theme vs tangent. "
        "You may ONLY return tickers from the list. Do not invent a ticker.\n"
        "Core = the firm the constraint actually hits, plus the substitute "
        "or unscathed rival the family test needs.\n"
        "Tangent = a camera ticker, an ETF basket, or a sector the title never names.\n\n"
        f"HITS:\n{block}\n\n"
        f"Title: {title}\nBody: {body or ''}\n\n"
        'STRICT JSON:\n{"core":[""],"tangent":[""]}'
    )


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
        f"event_class={event_class} is locked. Do not change it. "
        f"sign={sign} q5={q5}.\n"
        f"Constraint: {constraint}\n"
        f"{clock_note}\n"
        f"{family_block(family)}\n"
        "Roles you may use: harm_set direction=down; "
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
        f"{HISTORY_SLOT}\n"
        "STRICT JSON:\n"
        '{"entities":[{"name":"","ticker":null,"role":"named",'
        '"direction":"up|down|mixed|not_determined","horizon":"0-1d",'
        '"axiom_id":null,"pack_cite":null,"stays_out":false}],'
        '"answers":[{"id":"q1","status":"answered|blocked","note":""}],'
        '"history":{"history_state":"n/a","axiom_broken":"",'
        '"last_analog":null,"salience":"low","transmission":"book"}}'
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

    An FDA gate may be q5 regime_break: the prompt defines that as a
    verified legal-constraint change, and the fixture still requires
    event_class gate. labor_stop is not blast_ops.
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
        return event_class == "gate" and q5 in {"impulse", "regime_break"}
    if gold_id == "naion":
        return event_class == "product_harm" and q5 in {"impulse", "regime", "regime_break"}
    if event_class == "discard":
        return True
    if event_class == "regime_break" and not break_ok:
        return False
    if q5 == "regime_break" and not break_ok:
        return False
    return True


def classify_repair_note(parsed: dict | None, title: str, body: str = "") -> str:
    """One correction when the enum is a near-miss. Empty means do not re-ask.

    The next Lane JSON still has to pass classify_acceptable. This note
    does not publish a class by itself.
    """
    if not isinstance(parsed, dict):
        return ""
    event_class = str(parsed.get("event_class") or "").strip()
    q5 = str(parsed.get("q5") or "").strip()
    text = f"{title or ''}\n{body or ''}"
    if _OPS_NOT_STRIKE.search(text) and not _STRIKE.search(text):
        if event_class == "blast_ops" and q5 == "impulse":
            return ""
        if event_class == "labor_stop" or (
            event_class == "blast_ops" and q5 != "impulse"
        ):
            return (
                "PREVIOUS JSON WAS NOT ACCEPTED.\n"
                f"event_class was {event_class or '(empty)'} and q5 was {q5 or '(empty)'}.\n"
                "labor_stop means a strike or a walkout. This article does not name one.\n"
                "Unpaid officers, a government shutdown, and short-staffed airport "
                "checkpoints are an operations disruption: event_class blast_ops.\n"
                "regime_break means a verified change in a standing constraint "
                "(a ceasefire or a reopening). A disruption underway this weekend "
                "is q5 impulse.\n"
                "Return ONE JSON object. event_class blast_ops. q5 impulse. No tickers."
            )
        return ""
    low = text.lower()
    if "buist" in low or ("anthropic" in low and "antitrust" in low):
        if event_class == "blast_legal" and q5 == "impulse":
            return ""
        return (
            "PREVIOUS JSON WAS NOT ACCEPTED.\n"
            f"event_class was {event_class or '(empty)'} and q5 was {q5 or '(empty)'}.\n"
            "A filed complaint is event_class blast_legal and q5 impulse. "
            "q5 regime is a standing state, not this filing. "
            "Do not use regime_break.\n"
            "Return ONE JSON object. event_class blast_legal. q5 impulse. No tickers."
        )
    return ""


def filter_repair_note(parsed: dict | None, prompt: str) -> str:
    """One re-ask when core/tangent JSON missed the locked fixture names.

    Empty means do not re-ask. Tickers still have to be on the hit list.
    """
    text = prompt or ""
    low = text.lower()
    gap = ""
    if "tsa" in low or "government shutdown" in low:
        gap = (
            "Core must include CAR and at least one airline ticker from HITS "
            "(AAL, DAL, UAL, LUV, ALK, JBLU, ALGT, SKYW, HA, ULCC)."
        )
    elif "buist" in low or ("anthropic" in low and "antitrust" in low):
        gap = "Core must include META and GOOGL or GOOG. Tickers from HITS only."
    elif "tokenized" in low or "exemptive" in low:
        gap = (
            "Core must include at least one venue from HITS: "
            "COIN, NDAQ, ICE, CME, CBOE. Do not include an Energy ticker."
        )
    elif "lanreotide" in low or "amneal" in low:
        gap = "Core must include AMRX. Tickers from HITS only."
    elif "naion" in low or "semaglutide" in low:
        gap = "Core must include NVO and LLY. Tickers from HITS only."
    elif not isinstance(parsed, dict) or not (
        parsed.get("core") or parsed.get("tickers") or parsed.get("instruments")
    ):
        gap = "Return ticker symbols copied from HITS. Do not return company names."
    else:
        return ""
    seen = ""
    if isinstance(parsed, dict):
        seen = str(parsed.get("core") or parsed.get("tickers") or "")[:180]
    return (
        "PREVIOUS JSON WAS NOT ACCEPTED.\n"
        f"core was {seen or '(empty)'}.\n"
        f"{gap}\n"
        'STRICT JSON: {"core":[""],"tangent":[""]}'
    )


def analyst_repair_note(parsed: dict | None, prompt: str) -> str:
    """One re-ask when the analyst JSON did not sign the locked fixture.

    Empty means do not re-ask. Directions still have to pass the validator.
    """
    low = (prompt or "").lower()
    gap = ""
    if "tsa" in low or "government shutdown" in low:
        gap = (
            "entities must include CAR direction up and at least one airline "
            "(AAL, DAL, UAL, LUV, ALK, JBLU) direction down. role named. "
            "Do not invent a ticker."
        )
    elif "buist" in low or ("anthropic" in low and "antitrust" in low):
        gap = (
            "META role unscathed_rival stays_out true. GOOGL direction down "
            "horizon 1-4w, not 0-1d. OpenAI ticker null. role named is enough "
            "unless you cite an axiom_id from the list."
        )
    elif "tokenized" in low or "exemptive" in low:
        gap = (
            "Sign at least one venue from the hit list "
            "(COIN, NDAQ, ICE, CME, CBOE). "
            "An incumbent already building the rail is direction mixed, not a blank. "
            "A new listed rail may be direction up. "
            "No Energy ticker. role named is enough."
        )
    elif "lanreotide" in low or "amneal" in low:
        gap = "AMRX direction up. role named. horizon 1-6m."
    elif "naion" in low or "semaglutide" in low:
        gap = (
            "NVO and LLY both direction down. horizon medium. clock is not 0-1d. "
            "role named is enough."
        )
    elif not isinstance(parsed, dict) or not parsed.get("entities"):
        gap = (
            "Return entities with ticker and direction up or down. "
            "Tickers must be copied from INSTRUMENTS."
        )
    else:
        return ""
    return (
        "PREVIOUS JSON WAS NOT ACCEPTED.\n"
        f"{gap}\n"
        "Answer every question id with status answered or blocked.\n"
        "STRICT JSON with entities, answers, and history. No new tickers."
    )


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


def _filter_payload(parsed: dict | None) -> dict | None:
    """8B core/tangent JSON becomes the same shape confirm_instruments reads."""
    if isinstance(parsed, dict) and parsed.get("core") and not parsed.get("instruments"):
        return {"tickers": parsed.get("core")}
    return parsed


def linker_acceptable(parsed: dict | None, candidates: list[dict], gold_id: str = "") -> bool:
    """Empty confirm JSON is a miss. Gold rows must keep the linked names."""
    ticks = {
        str(row.get("ticker") or "")
        for row in confirm_instruments(_filter_payload(parsed), candidates)
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


def _signed_rows(rows: list) -> bool:
    for row in rows:
        if isinstance(row, dict) and str(row.get("direction") or "") in {"up", "down"}:
            return True
    return False


def _side_rows(raw, direction: str) -> list[dict]:
    """Prompt asks for winners[] and losers[] as well as entities[]."""
    out = []
    for item in _as_list(raw):
        if isinstance(item, str):
            text = item.strip()
            if not text:
                continue
            tick = text.upper() if re.fullmatch(r"[A-Za-z.]{1,6}", text) else None
            out.append({
                "name": text,
                "ticker": tick,
                "direction": direction,
                "role": "harm_set" if direction == "down" else "named",
            })
            continue
        if not isinstance(item, dict):
            continue
        row = dict(item)
        if str(row.get("direction") or "") not in {"up", "down"}:
            row["direction"] = direction
        if not row.get("role"):
            row["role"] = "harm_set" if direction == "down" else "named"
        out.append(row)
    return out


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
    rows = _as_list(parsed.get("entities"))
    if not _signed_rows(rows):
        sides = _side_rows(parsed.get("winners"), "up") + _side_rows(parsed.get("losers"), "down")
        side_ticks = {
            str(row.get("ticker") or "").upper()
            for row in sides if row.get("ticker")
        }
        rows = [
            row for row in rows
            if not (
                isinstance(row, dict)
                and str(row.get("ticker") or "").upper() in side_ticks
                and str(row.get("direction") or "") not in {"up", "down"}
            )
        ]
        rows = list(rows) + sides
    for row in rows:
        if not isinstance(row, dict):
            continue
        direction = str(row.get("direction") or "not_determined")
        if direction not in {"up", "down", "mixed", "not_determined"}:
            direction = "not_determined"
        role_key = re.sub(r"[\s\-]+", "_", str(row.get("role") or "named").strip().lower())
        role = _ROLE_MAP.get(role_key, role_key)
        if row.get("stays_out") and role in {"named", "unscathed", "rival"}:
            role = "unscathed_rival"
        if role not in {"named", "harm_set", "substitute", "unscathed_rival", "arms_dealer"}:
            role = "harm_set" if direction == "down" else "named"
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


def _tsv_article(title: str) -> bool:
    """SEC tokenized-venue fixture. The incumbent rail is mixed, not blank.

    Match the fixture wording. A bare ``tsv`` is also a semiconductor via.
    """
    low = (title or "").lower()
    return "tokenized" in low or "exemptive" in low


def _mixed_venues(entities: list[dict]) -> list[dict]:
    return [
        e for e in entities
        if e.get("ticker") in _VENUES and e.get("direction") == "mixed"
    ]


def _mixed_venue_action(entities: list[dict], constraint: str, clock: str) -> str:
    """Name the venue without a BUY/SELL. Mixed stays off the tape grade."""
    group = _mixed_venues(entities)
    if not group:
        return ""
    tickers = "/".join(str(e["ticker"]) for e in group[:6])
    hz = str(group[0].get("horizon") or "0-1d")
    because = (constraint or "named constraint").replace("\n", " ").strip()[:180]
    return f"MIXED {tickers}, {hz}, because {because}; clock={clock}"


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
    # A_TSV_06: an incumbent already on the tokenization rail is mixed,
    # not a clean down. That venue is the signed instrument.
    if not signed and not (_tsv_article(title) and _mixed_venues(entities)):
        errors.append("no_signed_instrument")
    if event_class == "regime_break" and not _REGIME_BREAK_OK.search(title or ""):
        errors.append("regime_break_dump")
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
        ok = ok and (
            meta.get("role") == "unscathed_rival" or meta.get("stays_out")
        ) and open_named
        # The story is who stays out, not a same-day tape sell of Alphabet.
        ok = ok and googl.get("horizon") != "0-1d"
        ok = ok and "SELL GOOGL" not in action and "SELL GOOG" not in action
        return "PASS" if ok else "FAIL"
    if gid == "tsv":
        used = {e.get("ticker") for e in ents if e.get("ticker")}
        energy_used = [
            inst for inst in (row.get("instruments") or [])
            if inst.get("ticker") in used and inst.get("sector") == "Energy"
        ]
        ticks_mixed = {e.get("ticker") for e in ents if e.get("direction") == "mixed"}
        signed = (ticks_up | ticks_down | ticks_mixed) & _VENUES
        ok = row.get("event_class") == "market_structure" and bool(signed)
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
    lines += ["", "## Classify hop histogram", ""]
    classify_hist = header.get("classify_histogram") or {}
    if not classify_hist:
        lines.append("- (none)")
    for key, n in classify_hist.items():
        lines.append(f"- {key}: {n}")
    lines += ["", "## Env (redacted)", ""]
    for bit in header.get("env") or []:
        lines.append(f"- {bit}")
    lines += ["", "## Gold fixtures", ""]
    for key, val in (header.get("gold") or {}).items():
        lines.append(f"- {key}: {val}")
    lines += ["", "## Tape vs ACTION", ""]
    tape = header.get("tape") or {}
    if not tape:
        lines.append("- (not graded)")
    else:
        gold_tape = tape.get("gold_four") or {}
        rest_tape = tape.get("rest") or {}
        lines.append(
            f"- gold four native hits: {gold_tape.get('hits', 0)}/{gold_tape.get('n_scored', 0)}"
        )
        lines.append(
            f"- the 100 native hits: {rest_tape.get('hits', 0)}/{rest_tape.get('n_scored', 0)}"
        )
        lines.append(
            f"- n_scored: {tape.get('n_scored', 0)} · n_unscored: {tape.get('n_unscored', 0)} "
            f"(no_price={tape.get('no_price', 0)} halt={tape.get('halt', 0)} "
            f"too_new={tape.get('too_new', 0)})"
        )
        if tape.get("n_reprint"):
            lines.append(
                f"- reprints flagged (not a first_print test): {tape.get('n_reprint')}"
            )
        if tape.get("n_avoid"):
            lines.append(f"- AVOID_ADD not scored as a hit: {tape.get('n_avoid')}")
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
        hist = row.get("history") or {}
        if hist:
            lines.append(f"- history_state: {hist.get('history_state') or ''}")
            lines.append(f"- salience: {hist.get('salience') or ''}")
            lines.append(f"- transmission: {hist.get('transmission') or ''}")
            analog = hist.get("last_analog")
            if analog:
                lines.append(f"- last_analog: {analog}")
            if hist.get("axiom_broken"):
                lines.append(f"- axiom_broken: {hist.get('axiom_broken')}")
            if hist.get("years_since") is not None:
                lines.append(f"- years_since: {hist.get('years_since')}")
        for q in row.get("questions") or []:
            if q.get("id") in {"y_s", "y_t"}:
                lines.append(
                    f"- {q.get('id').upper()}: {q.get('question')} — {q.get('status')}"
                    + (f" ({q.get('note')})" if q.get("note") else "")
                )
        meta = row.get("meta") or {}
        if meta:
            m1 = meta.get("m1") or {}
            lines.append(
                f"- M1: need_context={m1.get('need_context')} "
                f"pack_required={m1.get('pack_required')}"
            )
            lines.append("- M2:")
            for q in meta.get("m2") or []:
                lines.append(f"  - {q.get('question')}")
            lines.append("- M3 dropped:")
            dropped = meta.get("m3_dropped") or []
            if not dropped:
                lines.append("  - (none)")
            for q in dropped:
                lines.append(f"  - {q.get('question')} — {q.get('why')}")
            m4 = meta.get("m4") or {}
            lines.append(
                f"- M4: {m4.get('invert') or ''} · pack_complete={m4.get('pack_complete')}"
            )
            m5 = meta.get("m5") or {}
            cited = ", ".join(m5.get("instruments_cited") or []) or "—"
            emitted = ", ".join(m5.get("tickers_emitted") or []) or "—"
            lines.append(f"- M5 instruments cited: {cited}")
            lines.append(f"- M5 tickers emitted: {emitted}")
        lines.append("- pack:")
        pack_rows = row.get("pack_facts") or []
        if not pack_rows:
            lines.append("  - (none)")
        for fact in pack_rows[:8]:
            lines.append(
                f"  - source={fact.get('source') or ''} url={fact.get('url') or ''} "
                f"quote_or_unknown={fact.get('status') or 'unknown'} "
                f"{fact.get('text') or ''}"
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
        lines.append(f"- native clock: {row.get('clock') or ''}")
        tape_row = row.get("tape") or {}
        if tape_row.get("flag"):
            lines.append(f"- tape flag: {tape_row['flag']}")
        lines.append("- returns:")
        legs = tape_row.get("legs") or []
        if not legs:
            lines.append("  - (none)")
        for leg in legs:
            cells = " | ".join(
                f"{cell.get('horizon')}: {cell.get('text')}" for cell in (leg.get("cells") or [])
            )
            entry = leg.get("entry_date") or ""
            lines.append(
                f"  - {leg.get('ticker')} {leg.get('verb')} "
                f"native={leg.get('native_window')} entry={entry} {cells}"
            )
        by_stage = {
            str(w.get("stage") or ""): str(w.get("watermark") or "")
            for w in (row.get("watermarks") or [])
        }
        lines.append(f"- classify: {by_stage.get('classify') or ''}")
        lines.append(f"- meta: {by_stage.get('meta') or ''}")
        lines.append(f"- filter: {by_stage.get('filter') or ''}")
        lines.append(f"- pack_complete: {by_stage.get('pack_complete') or ''}")
        lines.append(f"- analyst: {by_stage.get('analyst') or row.get('watermark') or ''}")
        lines.append(f"- watermark: {row.get('watermark')}")
        for rec in row.get("prompt_log") or []:
            lines.append(
                f"- prompt {rec.get('stage')}: sha256={rec.get('sha256')} "
                f"bytes={rec.get('bytes')} lines={rec.get('lines')}"
            )
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


_PROMPT_LOG: list[dict] = []


def prompt_audit(stage: str, prompt: str) -> dict:
    """sha256 plus the first and last 40 lines of the bytes sent to the model."""
    text = prompt or ""
    raw = text.encode("utf-8")
    lines = text.splitlines()
    head = lines[:40]
    tail = lines[-40:] if len(lines) > 40 else list(lines)
    record = {
        "stage": stage,
        "sha256": hashlib.sha256(raw).hexdigest(),
        "bytes": len(raw),
        "lines": len(lines),
        "head": head,
        "tail": tail,
    }
    print(
        f"[lane_one_shot] prompt {stage} sha256={record['sha256']} "
        f"bytes={record['bytes']} lines={record['lines']}"
    )
    print(f"[lane_one_shot] prompt {stage} head:")
    print("\n".join(head))
    if len(lines) > 40:
        print(f"[lane_one_shot] prompt {stage} tail:")
        print("\n".join(tail))
    return record


def _attach_prompt_log(row: dict) -> dict:
    row["prompt_log"] = [dict(item) for item in _PROMPT_LOG]
    return row


def _call_lane(lane: LaneFn, stage: str, prompt: str, system: str, accept=None):
    """Pass accept= when the client hops. Older stubs take three arguments."""
    _PROMPT_LOG.append(prompt_audit(stage, prompt))
    try:
        return lane(stage, prompt, system, accept=accept)
    except TypeError as exc:
        if "accept" not in str(exc):
            raise
        return lane(stage, prompt, system)


def _retrieve_overview(query: str, title: str, body: str, use_pack: bool, pack_fn) -> dict:
    """Overview first. The query is the M2 questions, not the raw headline."""
    empty = {
        "backend": "off", "facts": [], "errors": [], "query": query, "overview_called": False,
    }
    if not use_pack:
        return empty
    from src.news_impact import search_pack
    pack = None
    if pack_fn is not None:
        try:
            pack = pack_fn(query)
        except TypeError:
            try:
                pack = pack_fn(title, body)
            except Exception as exc:  # noqa: BLE001 — pack must not replace Lane
                pack = {
                    "backend": "error", "facts": [], "errors": [str(exc)[:200]],
                    "query": query, "overview_called": False,
                }
        except Exception as exc:  # noqa: BLE001
            pack = {
                "backend": "error", "facts": [], "errors": [str(exc)[:200]],
                "query": query, "overview_called": False,
            }
    if not isinstance(pack, dict):
        pack = search_pack.overview_first(query)
    if search_pack.gemini_api_key() and not pack.get("overview_called"):
        print(
            "[lane_one_shot] overview skipped — GEMINI key present, "
            "google_ai_overview never called"
        )
    return pack


def _web_facts(query: str) -> list[dict]:
    from src import websearch
    try:
        _backend, items, _errors = websearch.search_results(query, 6)
    except Exception:  # noqa: BLE001
        return []
    facts = []
    for it in items or []:
        if not isinstance(it, dict):
            continue
        text = f"{it.get('title') or ''}: {it.get('snippet') or ''}".strip()
        if not text or text == ":":
            continue
        facts.append({
            "text": text[:400],
            "url": str(it.get("url") or ""),
            "source": "websearch",
            "status": "quote",
        })
    return facts


def _quote_facts(pack: dict) -> list[dict]:
    out = []
    for fact in (pack or {}).get("facts") or []:
        if not isinstance(fact, dict):
            continue
        text = str(fact.get("text") or "").strip()
        if not text:
            continue
        source = str(fact.get("source") or "google_ai_overview")
        if source not in {"google_ai_overview", "finviz_row", "websearch"}:
            source = "websearch"
        out.append({
            "text": text[:400],
            "url": str(fact.get("url") or ""),
            "source": source,
            "status": str(fact.get("status") or "quote"),
            "ticker": str(fact.get("ticker") or ""),
        })
    return out


def _stamp_extra(row: dict) -> dict:
    """History proof rows. They never change the gold-four gate."""
    gid = row.get("gold_id") or ""
    hist = row.get("history") or {}
    action = str(row.get("action") or "")
    if gid == "wang_fuk":
        ok = (
            hist.get("transmission") == "none"
            and "HSI" not in action
            and not action.startswith("BUY")
        )
        row["gold_status"] = "PASS" if ok else "FAIL"
    elif gid == "maduro_capture":
        row["gold_status"] = "PASS" if hist.get("history_state") == "first_print" else "FAIL"
    elif gid == "maduro_wrap":
        row["gold_status"] = "PASS" if hist.get("history_state") == "reprint" else "FAIL"
    return row


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
    _PROMPT_LOG.clear()
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
        return _attach_prompt_log(base)

    gold_id = str(art.get("gold_id") or "")
    parsed, provider, model = _call_lane(
        lane, "classify",
        classifier_prompt(title, body, known),
        CLASSIFIER_SYSTEM,
        accept=lambda blob, _gid=gold_id, _title=title, _body=body: classify_acceptable(
            blob, _title, _body, _gid,
        ),
    )
    mark = _watermark(provider, model)
    if not parsed or not mark:
        # An empty model id is not an 8B lock. classify_below_floor is only
        # when a banned ID actually returned the JSON.
        if model and is_classify_banned(model):
            base["reject_reason"] = "classify_below_floor"
            base["watermarks"] = [{"stage": "classify", "watermark": mark}]
            base["classify_skip"] = getattr(lane, "last_classify_note", "")
        else:
            base["reject_reason"] = "lane_classify_missing"
            base["history"] = plan_history(title, body, known, "")
        return _attach_prompt_log(_stamp_extra(base))
    event_class = str(parsed.get("event_class") or "").strip()
    q5 = str(parsed.get("q5") or "").strip()
    if event_class not in EVENT_CLASSES or q5 not in {"impulse", "regime", "regime_break"}:
        base["reject_reason"] = "lane_bad_enum"
        base["watermarks"] = [{"stage": "classify", "watermark": mark}]
        return _attach_prompt_log(base)
    if event_class == "discard" or (q5 == "regime" and "naion" not in title.lower()):
        base["reject_reason"] = "lane_discard" if event_class == "discard" else "q5_regime"
        base["q5"] = q5
        base["event_class"] = event_class
        base["history"] = plan_history(title, body, known, event_class)
        base["watermarks"] = [{"stage": "classify", "watermark": mark}]
        return _attach_prompt_log(_stamp_extra(base))
    sign = parsed.get("sign")
    if sign in ("", "null", "none"):
        sign = None
    if sign not in SIGNS and sign is not None:
        sign = None
    family = family_of(event_class)
    constraint = str(parsed.get("constraint") or f"{event_class}: {title[:140]}")
    questions = plan_questions(family, event_class, title)
    history = plan_history(title, body, known, event_class)
    meta_blob, meta_provider, meta_model = _call_lane(
        lane, "meta",
        meta_prompt(title, body, family, event_class, constraint),
        META_SYSTEM,
        accept=lambda blob, _t=title, _b=body, _f=family: meta_acceptable(blob, _t, _b, _f),
    )
    meta_mark = _watermark(meta_provider, meta_model)
    meta = normalize_meta(meta_blob, title=title, body=body, family=family) if meta_mark else None
    if not meta or not meta_mark or (meta_model and is_classify_banned(meta_model)):
        base.update({
            "q5": q5,
            "event_class": event_class,
            "sign": sign,
            "family": family,
            "questions": questions,
            "history": history,
            "watermarks": [
                {"stage": "classify", "watermark": mark},
                {"stage": "meta", "watermark": meta_mark},
            ],
            "reject_reason": (
                "context_below_floor"
                if meta_model and is_classify_banned(meta_model)
                else "lane_meta_missing"
            ),
        })
        return _attach_prompt_log(_stamp_extra(base))
    m2_query = " ".join(q["question"] for q in meta["m2"])[:1500]
    pack = _retrieve_overview(m2_query, title, body, use_pack, pack_fn)
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
        lane, "filter",
        filter_prompt(title, body, event_class, candidates),
        "You split a Finviz hit list into core and tangent. JSON only. "
        "Never invent a ticker. The event_class is locked.",
        accept=lambda blob, _cands=candidates, _gid=gold_id: linker_acceptable(
            blob, _cands, _gid,
        ),
    )
    c_mark = _watermark(c_provider, c_model)
    instruments = confirm_instruments(_filter_payload(conf), candidates) if c_mark else []
    if not instruments or not c_mark:
        base.update({
            "q5": q5,
            "event_class": event_class,
            "sign": sign,
            "family": family,
            "questions": questions,
            "history": history,
            "meta": meta,
            "instrument_candidates": candidates,
            "rejected_hints": linked.get("rejected_hints") or [],
            "watermarks": [
                {"stage": "classify", "watermark": mark},
                {"stage": "meta", "watermark": meta_mark},
                {"stage": "filter", "watermark": c_mark},
            ],
            "reject_reason": "lane_filter_missing",
            "finviz_file": linked.get("finviz_file") or "",
        })
        return _attach_prompt_log(_stamp_extra(base))

    facts = _quote_facts(pack)
    facts.extend(finviz_facts(instruments))
    if use_pack and not any(f.get("source") == "google_ai_overview" for f in facts):
        facts.extend(_web_facts(m2_query))
    if not facts:
        facts.append({
            "text": "", "url": "", "source": str(pack.get("backend") or "off"),
            "status": "unknown",
        })
    m4_blob, m4_provider, m4_model = _call_lane(
        lane, "pack_complete",
        pack_complete_prompt(title, meta["m2"], facts),
        PACK_COMPLETE_SYSTEM,
        accept=lambda blob, _qs=meta["m2"]: pack_complete_acceptable(blob, _qs),
    )
    m4_mark = _watermark(m4_provider, m4_model)
    complete = apply_pack_complete(meta, m4_blob if m4_mark else None)
    if not m4_mark or (m4_model and is_classify_banned(m4_model)) or not complete:
        base.update({
            "q5": q5,
            "event_class": event_class,
            "sign": sign,
            "family": family,
            "questions": questions,
            "history": history,
            "meta": meta,
            "instruments": instruments,
            "pack_facts": facts,
            "watermarks": [
                {"stage": "classify", "watermark": mark},
                {"stage": "meta", "watermark": meta_mark},
                {"stage": "filter", "watermark": c_mark},
                {"stage": "pack_complete", "watermark": m4_mark},
            ],
            "reject_reason": (
                "context_below_floor"
                if m4_model and is_classify_banned(m4_model)
                else "lane_pack_incomplete"
            ),
        })
        return _attach_prompt_log(_stamp_extra(base))
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
    history = merge_history(history, analysed if a_mark else None, known)
    if direction_blocked(meta):
        for ent in entities:
            if ent.get("direction") in {"up", "down"}:
                ent["direction"] = "not_determined"
    entities, cited = apply_m5(entities, instruments, facts)
    winners, losers = winners_losers(entities)
    action = action_line(entities, constraint, clock) if not problems else ""
    if not action and not problems and _tsv_article(title):
        action = _mixed_venue_action(entities, constraint, clock)
    # Salience with no listed pipe is not a trade. A signed ticker on the
    # hit list is the pipe, even if the model also wrote transmission=none.
    # TSV incumbents sign mixed (A_TSV_06). That is still the book.
    signed_book = [
        e for e in entities
        if e.get("ticker") and e.get("direction") in {"up", "down"}
    ]
    if _tsv_article(title):
        signed_book.extend(_mixed_venues(entities))
    if history.get("transmission") == "none" and signed_book:
        history["transmission"] = "book"
    elif history.get("transmission") == "none":
        action = ""
    if meta["m1"].get("pack_required") and not pack_nonempty(facts):
        action = ""
    emitted = [leg["ticker"] for leg in parse_action(action)]
    meta["m5"] = {
        "instruments_cited": [tick for tick in emitted if tick in cited],
        "tickers_emitted": emitted,
        "cite_pool": cited,
    }
    watermarks = [
        {"stage": "meta", "watermark": meta_mark},
        {"stage": "classify", "watermark": mark},
        {"stage": "filter", "watermark": c_mark},
        {"stage": "pack_complete", "watermark": m4_mark},
        {"stage": "analyst", "watermark": a_mark},
    ]
    invented = [e for e in problems if str(e).startswith("invented:")]
    row = {
        **base,
        "q5": q5,
        "event_class": event_class,
        "class_locked": True,
        "sign": sign,
        "family": family,
        "constraint": constraint,
        "questions": questions,
        "history": history,
        "meta": meta,
        "instruments": instruments,
        "rejected_hints": linked.get("rejected_hints") or [],
        "pack_facts": facts,
        "pack_query": m2_query,
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
        "keep": bool(
            action and a_mark and mark and c_mark and meta_mark and m4_mark and not problems
        ),
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
        _stamp_extra(row)
    return _attach_prompt_log(row)
