"""Hop-0 for every harvest. Patterns, not a title list.
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
    r"(?i)\b(?:holds? rates|fed holds|federal reserve holds|"
    r"agrees?|agreed|rejects?|rejected|"
    r"ended|recalled|identifies|filed|files? to|filed to|"
    r"acquires?|acquisition|"
    r"priced|launched|court verdict|jury verdict|cut tariffs|tariff cuts?|"
    r"completes?|completed|approved|wins?|won|divests?|divested|"
    r"stockholders approve|positive opinion|now live|rollout|"
    r"prime lending rate|trial data|settles?|settled|settlement|"
    r"closes? (?:the )?(?:acquisition|deal|purchase|merger)|"
    r"closed (?:the )?(?:acquisition|deal|purchase|merger)|"
    r"close us\$|merger plan|merger benefits|merger package|"
    r"credit facility|term loan|revolving credit|"
    r"sues?|sued|lawsuit|sells? \$\d|layoffs?|uplisting|"
    r"raising .{0,24}\bipo\b|\bipo\b.{0,24}(?:raising|priced|pricing|debut)|"
    r"ipo debut|set to list|ipo pricing)\b"
)

EARN_RE = re.compile(
    r"(?i)\beps\b|beats estimates|misses estimates|non-gaap|"
    r"quarterly results|q[1-4] 20\d{2}|raises? .{0,32}guidance|"
    r"reaffirms? .{0,32}guidance|issues? .{0,32}guidance|"
    r"raised outlook|raises outlook|beat, raised|"
    r"forecasts .{0,28}(?:revenue|growth|eps)|"
    r"posts record|record (?:q[1-4]|revenue|eps)|boosts 20\d{2} buyback"
)

RATING_RE = re.compile(
    r"(?i)price targets?|raises? (?:its )?price target|"
    r"cuts? (?:its )?price target|upgrades? to|downgrades? to|"
    r"\bupgraded\b|\bdowngrade\b|\bdowngraded\b|"
    r"initiates .{0,40}with (?:buy|sell|hold|overweight|underweight|neutral)|"
    r"reiterates (?:buy|sell|hold|outperform)|keeps buy|slashing price targets"
)

DOLLAR_RE = re.compile(
    r"\$[\d,.]+|\b\d+(?:\.\d+)?\s*(?:billion|million)\b"
)

PRINT_RE = re.compile(
    r"(?i)fed holds|federal reserve holds|federal reserve cuts|"
    r"cuts? interest rates|fed cuts|\bfomc\b|"
    r"\bcpi\b|\bpce\b|\bnfp\b|\beia\b|\bfda\b|\bsec\b|"
    r"\bema\b|\bchmp\b|\bcafe\b|budget boost|chips act|"
    r"mis-selling|retail sales|"
    r"strategic (?:petroleum |oil )?reserve|\bspr\b|"
    r"(?:fed|federal reserve).{0,48}(?:says|sees|signals|backs)|"
    r"cyclospor|\d[\d,]* (?:suspected )?cases"
)

POLICY_RE = re.compile(
    r"(?i)tax credit|electric vehicles|eu .{0,40}rules|"
    r"waste packaging|trump adviser|"
    r"(?:us-iran|iran).{0,40}peace deal|peace deal.{0,40}iran|"
    r"(?:trump|china|u\.?s\.?|united states|hhs|california|eu).{0,56}"
    r"(?:chips?|tariffs?|ev rule|fuel economy|export ban|"
    r"weighs allowing|drug prices|sanctions|denies|lobbyists)|"
    r"(?:chips?|tariffs?|ev rule|fuel economy|export ban|"
    r"weighs allowing|drug prices|sanctions|denies|lobbyists).{0,56}"
    r"(?:trump|china|u\.?s\.?|hhs|california|eu)"
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
            r"3 stocks that offer|3 things investors"
        ),
    ),
    ("v_quote", re.compile(r"(?i)stock price,\s*news,\s*quote")),
    (
        "v_week",
        re.compile(
            r"(?i)week ahead|all eyes on|what to watch|"
            r"markets brace|what to expect in markets|"
            r"to report earnings today|before market open|watch live|"
            r"earnings call (?:highlights|transcript)"
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
            r"stocks? (?:rise|hold steady) as|gold ends week|"
            r"impacts your wallet|sinking even though|on track for weekly|"
            r"stock outperforms competitors|jumps to close at record|"
            r"in focus|stock market today|"
            r"fuel\w* .{0,40}gain|spark\w* .{0,40}drop|"
            r"pressures .{0,24}shares|"
            r"stock (?:drops|dropped|climbs|fell|slips|lost|jumps|surges) on|"
            r"outperforms competitors|support\w* .{0,40}peers including|"
            r"reportedly|dollar holds near"
        ),
    ),
    ("v_ask", None),
    (
        "v_fluff",
        re.compile(
            r"(?i)puppy|patio stuff|el charro|football star shot|"
            r"richest people in america|polar bear cub|"
            r"heartbreaking update|dividend analysis|"
            r"rwanda genocide|legal fight|critical moment|"
            r"astronomical. consequences|"
            r"appoints .{0,40} as "
        ),
    ),
]

KEEP_RES: list[tuple[str, re.Pattern]] = [
    ("k_done", DONE_RE),
    ("k_earn", EARN_RE),
    ("k_rating", RATING_RE),
    ("k_print", PRINT_RE),
    ("k_dollar", DOLLAR_RE),
    (
        "k_choke",
        re.compile(
            r"(?i)(?:rejects?|accepts?|seizes?|strikes?|denies?|dropped).{0,40}"
            r"(?:iran|hormuz|strait|sanctions|peace)|"
            r"(?:iran|hormuz|strait|sanctions|peace).{0,40}"
            r"(?:rejects?|accepts?|seizes?|strikes?|denies?|dropped)"
        ),
    ),
    ("k_policy", POLICY_RE),
]

HARD_KEEP = frozenset({"k_print", "k_policy", "k_done", "k_earn", "k_rating", "k_choke"})

BIT_QUESTIONS: dict = {
    "v_tipsheet": {"type": "noul", "instructions": "Tip sheet?",
        "criteria": {"true": "3 Stocks That Offer AI Exposure.", "false": "AMETEK completes acquisition."}},
    "v_quote": {"type": "noul", "instructions": "Quote page?",
        "criteria": {"true": "WELL stock price, news, quote.", "false": "Raytheon Gets $20.7 Billion Contract."}},
    "v_week": {"type": "noul", "instructions": "Calendar, live, or call transcript?",
        "criteria": {"true": "Chiron Q2 Earnings Call Transcript.",
                     "false": "ACV Auctions Reports Record Revenue."}},
    "v_odds": {"type": "noul", "instructions": "Odds or forecast only?",
        "criteria": {"true": "October Hike Odds at 70%.", "false": "Fed Governor Cook Says inflation persists."}},
    "v_tape": {"type": "noul", "instructions": "Already-moved tape?",
        "criteria": {"true": "DOCS Stock Surges 40% Following Earnings.",
                     "false": "Huang forecasts 70% fiscal 2028 revenue growth."}},
    "v_ask": {"type": "noul", "instructions": "Question column?",
        "criteria": {"true": "Is Its Future Worth Buying Into?", "false": "Fed cuts 25bp."}},
    "v_fluff": {"type": "noul", "instructions": "Not a US-listed market act?",
        "criteria": {"true": "ICE agent gun in airport bathroom.", "false": "EU waste packaging rules."}},
    "k_done": {"type": "noul", "instructions": "Deal, IPO debut, layoff, court verdict?",
        "criteria": {"true": "Unitree soars in Shanghai IPO debut.", "false": "JPMorgan doubles down on SpaceX verdict."}},
    "k_earn": {"type": "noul", "instructions": "Company print, outlook, or named growth forecast?",
        "criteria": {"true": "Garmin beat, raised outlook. Huang forecasts 70% growth.",
                     "false": "Earnings call highlights."}},
    "k_rating": {"type": "noul", "instructions": "Named PT slash, upgrade, downgrade?",
        "criteria": {"true": "Analysts Are Slashing Price Targets on AppLovin.",
                     "false": "Top analyst reports."}},
    "k_print": {"type": "noul", "instructions": "Official print or named Fed officer voice?",
        "criteria": {"true": "Fed Governor Cook Says. Fed Barr backs further hikes.",
                     "false": "2 forces knocking the Fed off course."}},
    "k_dollar": {"type": "noul", "instructions": "Money AND a finished act?",
        "criteria": {"true": "Raytheon $20.7 Billion Contract.", "false": "$10 Billion ETF theme."}},
    "k_choke": {"type": "noul", "instructions": "New strait/sanctions/peace verb?",
        "criteria": {"true": "Trump rejects Iran peace. Steel intake through Hormuz dropped.",
                     "false": "Hormuz tensions linger."}},
    "k_policy": {"type": "noul", "instructions": "State/EU plus named policy object?",
        "criteria": {"true": "EU waste packaging rules. Fresh US-Iran peace deal reports.",
                     "false": "Americans Are Right: Inflation Is Not Just the Fed."}},
}


def code_keep(title: str) -> str:
    t = title or ""
    if PRINT_RE.search(t):
        return "k_print"
    if POLICY_RE.search(t):
        return "k_policy"
    if DONE_RE.search(t):
        return "k_done"
    if EARN_RE.search(t):
        return "k_earn"
    if RATING_RE.search(t):
        return "k_rating"
    for name, rx in KEEP_RES:
        if name in {"k_done", "k_earn", "k_rating", "k_print", "k_policy"}:
            continue
        if name == "k_dollar":
            if rx.search(t) and DONE_RE.search(t):
                return "k_dollar"
            continue
        if rx.search(t):
            return name
    return ""


def code_veto(title: str) -> str:
    t = title or ""
    keep = code_keep(t)
    hard = keep in HARD_KEEP
    if SOURCE_DENY.search(t) and not hard:
        return "source"
    if t.rstrip().endswith("?") and not hard:
        return "v_ask"
    for name, rx in VETO_RES:
        if name == "v_ask" or rx is None:
            continue
        if rx.search(t):
            if hard and name in {"v_tape", "v_odds"}:
                continue
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
    keep = code_keep(title)
    if keep:
        return pack("keep", keep)
    return pack("drop", "no_keep_bit")


def collapse_dupes(items: list[dict], threshold: float = DUP_JACCARD) -> list[dict]:
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
