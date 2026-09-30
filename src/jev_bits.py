"""Hop-0 bits matching the 2026-09-30 Finviz sheet labels.

Order: source / tip / week / tape / ask / fluff veto.
Then keep deal, earn, rating, print, policy.
Same-day near-dup of a keep is dropped.
"""
from __future__ import annotations

import re

TRASH_NOUL = 0.70
KEEP_NOUL = 0.60
DUP_JACCARD = 0.72

SOURCE_DENY = re.compile(
    r"(?i)(seeking.?alpha|benzinga|motley|fool\.com|the.?fool|"
    r"tipranks|zacks|thestreet|simplywall|investorplace|"
    r"marketbeat|insidermonkey)"
)

DONE_RE = re.compile(
    r"(?i)\b(?:holds?|held|agrees?|agreed|rejects?|rejected|"
    r"ended|recalled|identifies|filed|files?|acquires?|acquisition|"
    r"priced|launched|verdict|cut tariffs|tariff cuts?|"
    r"completes?|completed|approved|wins?|won|divests?|divested|"
    r"stockholders approve|positive opinion|now live|rollout|"
    r"prime lending rate|trial data|settles?|settled|settlement|"
    r"closes? (?:the )?(?:acquisition|deal|purchase|merger)|"
    r"closed (?:the )?(?:acquisition|deal|purchase|merger)|"
    r"close us\$|merger plan|merger benefits|merger package|"
    r"credit facility|term loan|revolving credit|"
    r"sues?|sued|lawsuit|sells? \$\d)\b"
)

EARN_RE = re.compile(
    r"(?i)\beps\b|beats estimates|misses estimates|non-gaap|"
    r"quarterly results|q[1-4] 20\d{2}|raises? .{0,32}guidance|"
    r"reaffirms? .{0,32}guidance|issues? .{0,32}guidance|"
    r"posts record|record (?:q[1-4]|revenue|eps)|boosts 20\d{2} buyback"
)

RATING_RE = re.compile(
    r"(?i)price targets?|raises? (?:its )?price target|"
    r"cuts? (?:its )?price target|upgrades? to|downgrades? to|"
    r"\bupgraded\b|\bdowngrade\b|\bdowngraded\b|"
    r"initiates .{0,40}with (?:buy|sell|hold|overweight|underweight|neutral)|"
    r"reiterates (?:buy|sell|hold|outperform)|keeps buy"
)

DOLLAR_RE = re.compile(
    r"\$[\d,.]+|\b\d+(?:\.\d+)?\s*(?:billion|million)\b"
)

VETO_RES: list[tuple[str, re.Pattern | None]] = [
    (
        "v_tipsheet",
        re.compile(
            r"(?i)should you buy|better stock to buy|is it a buy|"
            r"where will .{0,48} be|where it can go|prediction:|"
            r"i'd still buy|top dividend stocks|stocks to buy and hold|"
            r"could it set you up|top (?:research|analyst|stock) reports|"
            r"\d+ stocks put traders|moonshot|plus \d+ more|"
            r"these stocks stand to gain|stocks could break out|"
            r"3 things investors"
        ),
    ),
    ("v_quote", re.compile(r"(?i)stock price,\s*news,\s*quote")),
    (
        "v_week",
        re.compile(
            r"(?i)week ahead|all eyes on|what to watch|"
            r"markets brace|what to expect in markets|"
            r"to report earnings today|before market open"
        ),
    ),
    (
        "v_odds",
        re.compile(
            r"(?i)\bodds\b|polymarket|forecast:|sees rising|"
            r"could slide|could dethrone|rising odds|could reset"
        ),
    ),
    (
        "v_tape",
        re.compile(
            r"(?i)\d+(?:\.\d+)?\s*%|"
            r"gold price (?:surge|slide)|prices slide|"
            r"stocks? rise as|gold ends week|impacts your wallet|"
            r"sinking even though|on track for weekly|"
            r"stock outperforms competitors|jumps to close at record|"
            r"in focus|stock market today|"
            r"fuel\w* .{0,40}gain|spark\w* .{0,40}drop|"
            r"pressures .{0,24}shares|"
            r"stock (?:drops|dropped|climbs|fell|slips|lost) on|"
            r"outperforms competitors|support\w* .{0,40}peers including|"
            r"reportedly"
        ),
    ),
    ("v_ask", None),
    (
        "v_fluff",
        re.compile(
            r"(?i)puppy|patio stuff|el charro|football star shot|"
            r"richest people in america|polar bear cub|"
            r"heartbreaking update|dividend analysis|"
            r"appoints .{0,40} as |disclosed in an sec filing that it reaffirmed"
        ),
    ),
]

KEEP_RES: list[tuple[str, re.Pattern]] = [
    ("k_done", DONE_RE),
    ("k_earn", EARN_RE),
    ("k_rating", RATING_RE),
    (
        "k_print",
        re.compile(
            r"(?i)fed holds|federal reserve holds|\bfomc\b|"
            r"\bcpi\b|\bpce\b|\bnfp\b|\beia\b|\bfda\b|\bsec\b|"
            r"\bema\b|\bchmp\b|\bcafe\b|budget boost|chips act|"
            r"mis-selling"
        ),
    ),
    ("k_dollar", DOLLAR_RE),
    (
        "k_choke",
        re.compile(
            r"(?i)(?:rejects?|accepts?|seizes?|strikes?).{0,40}"
            r"(?:iran|hormuz|strait)|"
            r"(?:iran|hormuz|strait).{0,40}"
            r"(?:rejects?|accepts?|seizes?|strikes?)"
        ),
    ),
    (
        "k_policy",
        re.compile(
            r"(?i)(?:trump|china|u\.?s\.?|united states|hhs).{0,56}"
            r"(?:chips?|tariffs?|ev rule|fuel economy|export ban|"
            r"weighs allowing|drug prices)|"
            r"(?:chips?|tariffs?|ev rule|fuel economy|export ban|"
            r"weighs allowing|drug prices).{0,56}(?:trump|china|u\.?s\.?|hhs)"
        ),
    ),
]

BIT_QUESTIONS: dict = {
    "v_tipsheet": {
        "type": "noul",
        "instructions": "Tip sheet pick?",
        "criteria": {
            "true": "Should You Buy Palantir. Top research reports for AMD.",
            "false": "AMETEK completes $5.0 billion acquisition.",
        },
    },
    "v_quote": {
        "type": "noul",
        "instructions": "Quote page?",
        "criteria": {
            "true": "Sony stock price, news, quote and history.",
            "false": "Boeing identifies 737 MAX software glitch.",
        },
    },
    "v_week": {
        "type": "noul",
        "instructions": "Calendar preview?",
        "criteria": {
            "true": "Cintas to report earnings today before market open.",
            "false": "Fastenal beats Q2 estimates with EPS $0.33.",
        },
    },
    "v_odds": {
        "type": "noul",
        "instructions": "Odds or forecast only?",
        "criteria": {
            "true": "Trump-Xi Summit Could Reset Trade Tensions.",
            "false": "JPMorgan upgrades America Movil to Overweight.",
        },
    },
    "v_tape": {
        "type": "noul",
        "instructions": "Already-moved tape? Percent move in the title?",
        "criteria": {
            "true": "Surprise inventory build drive 5.63% DVN drop. NFLX drops on HSBC cut.",
            "false": "GE Vernova settles over $300 million Vineyard Wind dispute.",
        },
    },
    "v_ask": {
        "type": "noul",
        "instructions": "Question column?",
        "criteria": {
            "true": "How far can BSX stock swing?",
            "false": "Cenovus Q2 EPS $1.08 misses estimates.",
        },
    },
    "v_fluff": {
        "type": "noul",
        "instructions": "Not a market act?",
        "criteria": {
            "true": "South32 Ltd's Dividend Analysis. Aon Appoints a CCO.",
            "false": "Arthur J. Gallagher acquires Innovise.",
        },
    },
    "k_done": {
        "type": "noul",
        "instructions": "Deal, settlement, lawsuit, or finished act?",
        "criteria": {
            "true": "acquires, closes $4B acquisition, settles $300m, hospitals sue.",
            "false": "odds, could, should you buy.",
        },
    },
    "k_earn": {
        "type": "noul",
        "instructions": "Company earnings print or guidance?",
        "criteria": {
            "true": "Cenovus Q2 EPS $1.08 misses estimates. CSX raises 2026 outlook.",
            "false": "Cintas to report earnings today.",
        },
    },
    "k_rating": {
        "type": "noul",
        "instructions": "Named analyst upgrade, downgrade, or price target?",
        "criteria": {
            "true": "JPMorgan upgrades America Movil. HSBC downgraded Netflix to Hold.",
            "false": "Top analyst reports for AMD, Linde and Amgen.",
        },
    },
    "k_print": {
        "type": "noul",
        "instructions": "Official print or rule?",
        "criteria": {
            "true": "FDA approval. CHIPS Act award. EMA CHMP.",
            "false": "Barr says more hikes likely needed.",
        },
    },
    "k_dollar": {
        "type": "noul",
        "instructions": "Money amount AND a finished act?",
        "criteria": {
            "true": "AMETEK completes $5.0 billion acquisition.",
            "false": "US landlords face a $1.8 trillion debt wall.",
        },
    },
    "k_choke": {
        "type": "noul",
        "instructions": "New strait verb?",
        "criteria": {
            "true": "Trump rejects Iran proposal to reopen Hormuz.",
            "false": "Tensions persist as tankers transit Hormuz.",
        },
    },
    "k_policy": {
        "type": "noul",
        "instructions": "State plus named policy object?",
        "criteria": {
            "true": "Pfizer share revenue with HHS on overseas drug prices.",
            "false": "How Trump became mob boss of the United States.",
        },
    },
}


def _noul(answers: dict | None, key: str) -> float:
    if not answers:
        return 0.0
    try:
        return float(answers.get(key) or 0.0)
    except (TypeError, ValueError):
        return 0.0


def code_veto(title: str) -> str:
    t = title or ""
    if SOURCE_DENY.search(t):
        return "source"
    if t.rstrip().endswith("?"):
        return "v_ask"
    for name, rx in VETO_RES:
        if name == "v_ask":
            continue
        if rx is not None and rx.search(t):
            return name
    return ""


def code_keep(title: str) -> str:
    t = title or ""
    if DONE_RE.search(t):
        return "k_done"
    if EARN_RE.search(t):
        return "k_earn"
    if RATING_RE.search(t):
        return "k_rating"
    for name, rx in KEEP_RES:
        if name in {"k_done", "k_earn", "k_rating"}:
            continue
        if name == "k_dollar":
            if rx.search(t) and DONE_RE.search(t):
                return "k_dollar"
            continue
        if rx.search(t):
            return name
    return ""


def decide(row: dict, answers: dict | None = None) -> dict:
    title = row.get("title") or ""
    source = row.get("source") or ""
    blob = f"{title} {source}"

    def pack(decision: str, reason: str) -> dict:
        return {
            "title": title,
            "source": source,
            "published_at": row.get("published_at") or "",
            "url": row.get("url") or "",
            "id": row.get("id") or "",
            "decision": decision,
            "reason": reason,
            "geo": "",
            "actor_power": "",
            "action_material": 0.0,
            "new_instrument": 1.0 if decision == "keep" else 0.0,
            "reprint_weather": 0.0,
            "place": "",
            "has_new_verb": False,
        }

    veto = code_veto(blob)
    if veto:
        return pack("drop", veto)

    if answers:
        for name, _rx in VETO_RES:
            if _noul(answers, name) >= TRASH_NOUL:
                return pack("drop", name)

    keep = code_keep(title)
    if answers:
        for name, _rx in KEEP_RES:
            if name == "k_dollar" and not (
                _noul(answers, "k_done") >= KEEP_NOUL or DONE_RE.search(title)
            ):
                continue
            if _noul(answers, name) >= KEEP_NOUL:
                keep = keep or name

    if keep:
        return pack("keep", keep)
    return pack("drop", "no_keep_bit")


def collapse_dupes(items: list[dict], threshold: float = DUP_JACCARD) -> list[dict]:
    """Second keep that overlaps an earlier keep is a rewrite. Drop it."""
    try:
        from .jev_gate import jaccard, tokens
    except Exception:
        return items
    kept_toks: list[frozenset[str]] = []
    out: list[dict] = []
    for raw in items:
        item = dict(raw)
        title = item.get("title") or ""
        toks = tokens(title)
        if item.get("decision") == "keep":
            if any(jaccard(toks, prev) >= threshold for prev in kept_toks):
                item["decision"] = "drop"
                item["reason"] = "dup"
            else:
                kept_toks.append(toks)
        out.append(item)
    return out
