"""Short veto/keep bits for Jev hop-0.

Jev only sees one line per question plus a true/false example.
Vetoes win. A keep bit cannot override a veto.
"""
from __future__ import annotations

import re

TRASH_NOUL = 0.70
KEEP_NOUL = 0.60

SOURCE_DENY = re.compile(
    r"(?i)(seeking.?alpha|benzinga|motley|fool\.com|the.?fool|"
    r"tipranks|zacks|thestreet|simplywall|investorplace|"
    r"marketbeat|insidermonkey)"
)

VETO_RES: list[tuple[str, re.Pattern | None]] = [
    (
        "v_tipsheet",
        re.compile(
            r"(?i)should you buy|better stock to buy|is it a buy|"
            r"where will .{0,48} be|prediction:|i'd still buy|"
            r"top dividend stocks|stocks to buy and hold|"
            r"could it set you up"
        ),
    ),
    ("v_quote", re.compile(r"(?i)stock price,\s*news,\s*quote")),
    (
        "v_week",
        re.compile(
            r"(?i)week ahead|all eyes on|what to watch|"
            r"markets brace|what to expect in markets this week|"
            r"what to expect in markets"
        ),
    ),
    (
        "v_odds",
        re.compile(
            r"(?i)\bodds\b|polymarket|forecast:|sees rising|"
            r"could slide|could dethrone|rising odds"
        ),
    ),
    (
        "v_tape",
        re.compile(
            r"(?i)stocks? rise as|gold ends week|impacts your wallet|"
            r"sinking even though|on track for weekly|how the latest .{0,24} impacts"
        ),
    ),
    ("v_ask", None),
    (
        "v_fluff",
        re.compile(
            r"(?i)puppy|patio stuff|el charro|football star shot|"
            r"richest people in america|polar bear cub|"
            r"heartbreaking update"
        ),
    ),
]

KEEP_RES: list[tuple[str, re.Pattern]] = [
    (
        "k_done",
        re.compile(
            r"(?i)\b(?:holds?|held|agrees?|agreed|rejects?|rejected|"
            r"ended|recalled|identifies|filed|acquires?|priced|"
            r"launched|verdict|cut tariffs|tariff cuts?)\b"
        ),
    ),
    (
        "k_print",
        re.compile(
            r"(?i)fed holds|federal reserve holds|\bfomc\b|"
            r"\bcpi\b|\bpce\b|\bnfp\b|\beia\b|\bfda\b|\bsec\b|"
            r"\bcafe\b|budget boost"
        ),
    ),
    (
        "k_dollar",
        re.compile(r"\$[\d,.]+|\b\d+(?:\.\d+)?\s*(?:billion|million)\b"),
    ),
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
            r"(?i)(?:trump|china|u\.?s\.?|united states).{0,56}"
            r"(?:chips?|tariffs?|ev rule|fuel economy|export ban|weighs allowing)|"
            r"(?:chips?|tariffs?|ev rule|fuel economy|export ban|weighs allowing)"
            r".{0,56}(?:trump|china|u\.?s\.?)"
        ),
    ),
]

BIT_QUESTIONS: dict = {
    "v_tipsheet": {
        "type": "noul",
        "instructions": "Tip sheet pick?",
        "criteria": {
            "true": "Should You Buy Palantir. Better stock to buy. Prediction: crash.",
            "false": "Apple hit with $5.7 billion verdict.",
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
            "true": "The Week Ahead: PCE and payrolls. All eyes on jobs data.",
            "false": "Fed holds interest rates steady.",
        },
    },
    "v_odds": {
        "type": "noul",
        "instructions": "Odds or forecast only?",
        "criteria": {
            "true": "Fed October Rate Hike Odds Exceed 60%. Polymarket 64%.",
            "false": "Fed holds interest rates steady.",
        },
    },
    "v_tape": {
        "type": "noul",
        "instructions": "Already-moved tape or wallet recap?",
        "criteria": {
            "true": "U.S. stocks rise as Fed signals hikes. How the hike impacts your wallet.",
            "false": "Trump rejects Iran proposal to reopen Hormuz.",
        },
    },
    "v_ask": {
        "type": "noul",
        "instructions": "Question column?",
        "criteria": {
            "true": "Is AI really fueling inflation?",
            "false": "China, U.S. agree to tariff cuts on $30 billion in goods.",
        },
    },
    "v_fluff": {
        "type": "noul",
        "instructions": "Not a market act?",
        "criteria": {
            "true": "Golden retriever puppy looks like a polar bear. Patio storage.",
            "false": "Buckhead tuna recalled across 5 states.",
        },
    },
    "k_done": {
        "type": "noul",
        "instructions": "Finished act verb?",
        "criteria": {
            "true": "holds, agreed, rejects, ended, recalled, identifies, verdict, acquires.",
            "false": "odds, could, may, should, watch, forecast.",
        },
    },
    "k_print": {
        "type": "noul",
        "instructions": "Official print or rule?",
        "criteria": {
            "true": "Fed holds rates. FDA outbreak. CAFE rule. $6b budget.",
            "false": "Barr says more hikes likely needed.",
        },
    },
    "k_dollar": {
        "type": "noul",
        "instructions": "Named name plus money?",
        "criteria": {
            "true": "Apple $5.7 billion verdict. Akamai $11.6B deal.",
            "false": "Gold ends week lower at $4,285.",
        },
    },
    "k_choke": {
        "type": "noul",
        "instructions": "New strait verb?",
        "criteria": {
            "true": "Trump rejects Iran proposal to reopen Hormuz.",
            "false": "Iran war live: world holds breath for new Hormuz resolution.",
        },
    },
    "k_policy": {
        "type": "noul",
        "instructions": "State plus named policy object?",
        "criteria": {
            "true": "China weighs allowing Nvidia chips. Trump ended EV rule.",
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
    for name, rx in KEEP_RES:
        if rx.search(t):
            return name
    return ""


def decide(row: dict, answers: dict | None = None) -> dict:
    """Veto first. Keep only if a keep bit fires and no veto did."""
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
            if _noul(answers, name) >= KEEP_NOUL:
                keep = keep or name

    if keep:
        return pack("keep", keep)
    return pack("drop", "no_keep_bit")
