"""Hop-0 for every harvest, every day. Not a one-draw patch.

Veto tape/tips/live-coverage/geo-fluff.
Keep prints, policy, deals, IPO pricing, earnings, ratings, Fed officers.
Source-deny cannot kill a hard keep.
Live Jev answers do not override this pack.
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
    r"priced|launched|verdict|cut tariffs|tariff cuts?|"
    r"completes?|completed|approved|wins?|won|divests?|divested|"
    r"stockholders approve|positive opinion|now live|rollout|"
    r"prime lending rate|trial data|settles?|settled|settlement|"
    r"closes? (?:the )?(?:acquisition|deal|purchase|merger)|"
    r"closed (?:the )?(?:acquisition|deal|purchase|merger)|"
    r"close us\$|merger plan|merger benefits|merger package|"
    r"credit facility|term loan|revolving credit|"
    r"sues?|sued|lawsuit|sells? \$\d|layoffs?|uplisting|"
    r"raising .{0,24}\bipo\b|\bipo\b.{0,24}(?:raising|priced|pricing)|"
    r"set to list|ipo pricing)\b"
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

PRINT_RE = re.compile(
    r"(?i)fed holds|federal reserve holds|federal reserve cuts|"
    r"cuts? interest rates|fed cuts|\bfomc\b|"
    r"\bcpi\b|\bpce\b|\bnfp\b|\beia\b|\bfda\b|\bsec\b|"
    r"\bema\b|\bchmp\b|\bcafe\b|budget boost|chips act|"
    r"mis-selling|retail sales|"
    r"strategic (?:petroleum |oil )?reserve|\bspr\b|"
    r"fed'?s? \w+ (?:says|sees|signals)|"
    r"federal reserve'?s? \w+ (?:says|sees|signals)|"
    r"fed officials (?:see|says?|signal)"
)

POLICY_RE = re.compile(
    r"(?i)tax credit|electric vehicles|"
    r"(?:trump|china|u\.?s\.?|united states|hhs|california).{0,56}"
    r"(?:chips?|tariffs?|ev rule|fuel economy|export ban|"
    r"weighs allowing|drug prices|sanctions|denies)|"
    r"(?:chips?|tariffs?|ev rule|fuel economy|export ban|"
    r"weighs allowing|drug prices|sanctions|denies).{0,56}"
    r"(?:trump|china|u\.?s\.?|hhs|california)"
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
            r"to report earnings today|before market open|watch live"
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
            r"stock (?:drops|dropped|climbs|fell|slips|lost) on|"
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
            r"(?i)(?:rejects?|accepts?|seizes?|strikes?|denies?).{0,40}"
            r"(?:iran|hormuz|strait|sanctions)|"
            r"(?:iran|hormuz|strait|sanctions).{0,40}"
            r"(?:rejects?|accepts?|seizes?|strikes?|denies?)"
        ),
    ),
    ("k_policy", POLICY_RE),
]

HARD_KEEP = frozenset({"k_print", "k_policy", "k_done", "k_earn", "k_rating", "k_choke"})

BIT_QUESTIONS: dict = {
    "v_tipsheet": {"type": "noul", "instructions": "Tip sheet pick?",
        "criteria": {"true": "Should You Buy Palantir.", "false": "AMETEK completes acquisition."}},
    "v_quote": {"type": "noul", "instructions": "Quote page?",
        "criteria": {"true": "Sony stock price, news, quote.", "false": "Boeing identifies 737 MAX glitch."}},
    "v_week": {"type": "noul", "instructions": "Calendar or live coverage?",
        "criteria": {"true": "WATCH LIVE: Fed chair holds news conference.",
                     "false": "Federal Reserve cuts interest rates by 0.25 percentage points."}},
    "v_odds": {"type": "noul", "instructions": "Odds or forecast only?",
        "criteria": {"true": "Next price spike could roil economies.", "false": "JPMorgan upgrades AMX."}},
    "v_tape": {"type": "noul", "instructions": "Already-moved tape?",
        "criteria": {"true": "US stocks hold steady after Fed speech.",
                     "false": "Federal Reserve cuts interest rates by 0.25 percentage points."}},
    "v_ask": {"type": "noul", "instructions": "Question column?",
        "criteria": {"true": "How far can BSX stock swing?", "false": "Cenovus Q2 EPS $1.08 misses."}},
    "v_fluff": {"type": "noul", "instructions": "Not a US-listed market act?",
        "criteria": {"true": "Rwanda genocide sentence. Meta legal-fight color.",
                     "false": "Arthur J. Gallagher acquires Innovise."}},
    "k_done": {"type": "noul", "instructions": "Deal, IPO pricing, layoff, settlement?",
        "criteria": {"true": "Raising $540 million in IPO. Oracle planning layoffs.",
                     "false": "odds, could, should you buy."}},
    "k_earn": {"type": "noul", "instructions": "Company earnings print or guidance?",
        "criteria": {"true": "Marvell Q2 earnings: record revenue, raised guidance.",
                     "false": "Earnings call highlights."}},
    "k_rating": {"type": "noul", "instructions": "Named analyst upgrade, downgrade, or PT?",
        "criteria": {"true": "HSBC downgraded Netflix to Hold.", "false": "Top analyst reports for AMD."}},
    "k_print": {"type": "noul", "instructions": "Official print, FOMC, or named Fed officer?",
        "criteria": {"true": "Fed cuts 25bp. Retail sales fall. Barr signals further hikes.",
                     "false": "Markets figure out the Fed next move."}},
    "k_dollar": {"type": "noul", "instructions": "Money amount AND a finished act?",
        "criteria": {"true": "Raytheon Gets $20.7 Billion Contract.",
                     "false": "US landlords face a $1.8 trillion debt wall."}},
    "k_choke": {"type": "noul", "instructions": "New strait or sanctions verb?",
        "criteria": {"true": "Trump denies Iran sanctions easing reports.",
                     "false": "Tensions persist as tankers transit Hormuz."}},
    "k_policy": {"type": "noul", "instructions": "State plus named policy object?",
        "criteria": {"true": "California offers new tax credit for electric vehicles.",
                     "false": "Trump slams Canada as trade war persists."}},
}


def _noul(answers: dict | None, key: str) -> float:
    if not answers:
        return 0.0
    try:
        return float(answers.get(key) or 0.0)
    except (TypeError, ValueError):
        return 0.0


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
