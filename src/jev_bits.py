"""Hop-0 for every harvest.

Live path: cheap structural veto, then Jev BIT_QUESTIONS decide keep/drop.
Code-only / no-answers path: the regex pack (also the lock fallback).
"""
from __future__ import annotations

import re

TRASH_NOUL = 0.70
KEEP_NOUL = 0.50
DUP_JACCARD = 0.72

SOURCE_DENY = re.compile(
    r"(?i)(seeking.?alpha|benzinga|motley|fool\.com|the.?fool|"
    r"tipranks|zacks|thestreet|simplywall|investorplace|"
    r"marketbeat|insidermonkey)"
)

DONE_RE = re.compile(
    r"(?i)\b(?:holds? rates|fed holds|federal reserve holds|"
    r"agrees?|agreed|rejects?|rejected|launches?|"
    r"ended|recalled|identifies|filed|files? to|filed to|"
    r"acquires?|acquisition|"
    r"priced|launched|court verdict|jury verdict|cut tariffs|tariff cuts?|"
    r"completes?|completed|approved|wins?(?!['’]t)|won(?!['’]t)|divests?|divested|"
    r"stockholders approve|positive opinion|now live|rollout|"
    r"prime lending rate|trial data|settles?|settled|settlement|"
    r"closes? (?:the )?(?:acquisition|deal|purchase|merger)|"
    r"closed (?:the )?(?:acquisition|deal|purchase|merger)|"
    r"close us\$|merger plan|merger benefits|merger package|"
    r"credit facility|term loan|revolving credit|"
    r"sues?|sued|lawsuit|sells? \$\d|layoffs?|uplisting|"
    r"gets? \$\d|billion contract|wins? .{0,32}contract|"
    r"raising .{0,24}\bipo\b|\bipo\b.{0,24}(?:raising|priced|pricing|debut)|"
    r"ipo debut|set to list|ipo pricing|pulls .{0,40}listing|withdraws .{0,40}(?:ipo|listing)|"
    r"aiming for .{0,40}\bipo\b|targets? .{0,40}\bipo\b|\$[\d,.]+ trillion ipo)\b"
)

EARN_RE = re.compile(
    r"(?i)\beps\b|beats estimates|beats expectations|misses estimates|misses expectations|non-gaap|"
    r"quarterly results|q[1-4] 20\d{2}|raises? .{0,32}guidance|"
    r"reaffirms? .{0,32}guidance|issues? .{0,32}guidance|"
    r"raised outlook|raises outlook|beat, raised|"
    r"forecasts .{0,28}(?:revenue|growth|eps)|"
    r"posts record|record (?:q[1-4]|second quarter |third quarter |first quarter )?(?:revenue|eps|earnings)|per diluted share|boosts 20\d{2} buyback|"
    r"strong earnings|earnings (?:spark|beat|miss)\b|quarterly profit"
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
    r"mis-selling|retail sales|inflation gauge posts|"
    r"strategic (?:petroleum |oil )?reserve|\bspr\b|"
    r"(?:fed|federal reserve).{0,48}(?:says|see[s]?|signals|backs|warns?)|"
    r"(?:says|see[s]?|signals|backs|warns?).{0,48}(?:fed|federal reserve)|"
    r"raised (?:its )?benchmark|federal reserve raised|fed raised|"
    r"(?:treasury|yield|30-year|10-year|cpi|pce|nfp|payrolls).{0,48}(?:highest|lowest).{0,24}since|"
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
            r"what it means|why this matters|"
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
            r"rwanda genocide|legal fight|critical moment|hush-money|"
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
            r"(?:rejects?|accepts?|seizes?|strikes?|denies?|dropped)|"
            r"pipeline.{0,40}(?:shut|closed|halted|struck)|"
            r"(?:drone strikes?|strikes).{0,32}pipeline|"
            r"(?:freight|transportation) costs?|cost of transportation|"
            r"steel (?:trade )?flows|flow of steel"
        ),
    ),
    ("k_policy", POLICY_RE),
]

HARD_KEEP = frozenset({"k_print", "k_policy", "k_done", "k_earn", "k_rating", "k_choke"})

BIT_QUESTIONS: dict = {
    "v_tipsheet": {
        "type": "noul",
        "instructions": (
            "TRUE if this is a buy/sell list, 'N funds/stocks to consider', "
            "better-buy matchup, or 'what investors should do'. "
            "FALSE if a named company closed a deal or printed results."
        ),
        "criteria": {
            "true": "3 Financial Mutual Funds to Consider as Fed Signals More Rate Hikes. 5 Best Plastics Stocks.",
            "false": "AMETEK completes acquisition.",
        },
    },
    "v_quote": {"type": "noul", "instructions": "Quote/history landing page?",
        "criteria": {"true": "WELL stock price, news, quote.", "false": "Raytheon Gets $20.7 Billion Contract."}},
    "v_week": {
        "type": "noul",
        "instructions": (
            "TRUE if the title is a calendar, preview, 'ahead of' a print, "
            "'will be released', 'what to expect', live stream, or earnings "
            "call highlights/transcript. FALSE if the official print or "
            "company results already happened in this headline."
        ),
        "criteria": {
            "true": "Asia stocks gain ahead of U.S. PCE. The Fed's main inflation measure will be released Wednesday. Chiron Q2 Earnings Call Transcript.",
            "false": "US July PPI Below Expectations. ACV Auctions Reports Record Revenue.",
        },
    },
    "v_odds": {"type": "noul", "instructions": "Odds, 'traders think', or forecast-only?",
        "criteria": {"true": "October Hike Odds at 70%. Here's how much traders think Nvidia will move off earnings.",
                     "false": "Fed Governor Cook Says inflation persists."}},
    "v_tape": {"type": "noul", "instructions": "Already-moved price tape or 'stock market today' recap?",
        "criteria": {"true": "DOCS Stock Surges 40% Following Earnings.",
                     "false": "Huang forecasts 70% fiscal 2028 revenue growth."}},
    "v_ask": {"type": "noul", "instructions": "Rhetorical question column?",
        "criteria": {"true": "Is Its Future Worth Buying Into?", "false": "Fed cuts 25bp."}},
    "v_fluff": {"type": "noul", "instructions": "Not a US-listed market act (crime, lifestyle, protest)?",
        "criteria": {"true": "ICE agent gun in airport bathroom.", "false": "EU waste packaging rules."}},
    "k_done": {
        "type": "noul",
        "instructions": (
            "TRUE if a named firm finished or announced a real act: buy/acquire, "
            "IPO raise/aim/debut/underwriting, court order to pay, launch of a "
            "dollar facility, layoff, settlement. FALSE if someone merely "
            "'touts wins', 'is now worth', or recaps an old verdict."
        ),
        "criteria": {
            "true": "Nasdaq to Buy Dark Pool Stock Venue LeveL. Guardant ordered to pay $245m. Dangote secures $1 billion underwriting ahead of IPO. Anthropic aiming for $2 trillion IPO.",
            "false": "Capita Touts Contract Wins. $10,000 invested at SpaceX IPO is now worth.",
        },
    },
    "k_earn": {
        "type": "noul",
        "instructions": (
            "TRUE if this IS the company print: beat, miss, revenue miss, "
            "quarterly profit, EPS, guidance raise/cut. FALSE if highlights, "
            "transcript, 'may show', 'hard to call on earnings today', or "
            "a column about earnings."
        ),
        "criteria": {
            "true": "Bitdeer Q2 earnings and revenue miss. Nvidia $10.2 Billion Quarterly Profit Increase Topped Its Entire 2022 Operating Profit. CrowdStrike strong earnings spark a rally.",
            "false": "Earnings call highlights. Here's how much traders think Nvidia will move off earnings. Micron: Earnings May Show Why $100 Billion Won't Save The Rally.",
        },
    },
    "k_rating": {"type": "noul", "instructions": "Named upgrade, downgrade, or PT change?",
        "criteria": {"true": "Apple downgraded, HPE upgraded. Analysts Are Slashing Price Targets on AppLovin.",
                     "false": "Top analyst reports. Bank of America Sees ASML Stock's Next Big AI Trigger."}},
    "k_print": {
        "type": "noul",
        "instructions": (
            "TRUE if this title IS the official print or a named Fed official "
            "speaking. July PPI below expectations, consumer confidence at a "
            "12-year low, Beige Book, FOMC hold/hike/cut that already happened, "
            "and Williams/Barr/Cook/Hammack/Fed officials say, see, urge, or "
            "warn (colon titles count) are TRUE. FALSE if the print is only "
            "upcoming, expected, 'ahead of', 'what to expect', or a tip sheet "
            "that mentions the Fed."
        ),
        "criteria": {
            "true": "US July PPI Below Expectations. Fed's Williams: No Rush on Rate Hikes. Beth Hammack urges Fed rate hike. three Fed officials issue inflation warnings. Consumer confidence sags to 12-year low.",
            "false": "Asia stocks gain ahead of U.S. PCE. US core PCE inflation expected to increase. 3 Financial Mutual Funds to Consider as Fed Signals More Rate Hikes.",
        },
    },
    "k_dollar": {"type": "noul", "instructions": "Money AND a finished act?",
        "criteria": {"true": "Raytheon $20.7 Billion Contract.", "false": "$10 Billion ETF theme. Harvard discloses $2.2 billion SpaceX stake."}},
    "k_choke": {"type": "noul", "instructions": "New strait/sanctions/peace verb, shut pipeline, or steel/freight shock?",
        "criteria": {"true": "Trump denies Iran sanctions relief. Pipeline still shut after drone strikes.",
                     "false": "Hormuz tensions linger. Crude oil price today Brent $103."}},
    "k_policy": {
        "type": "noul",
        "instructions": (
            "TRUE if a state/EU act landed (tax credit, tariff rule, sanctions "
            "deny, packaging rules, peace deal). FALSE if someone only 'flags "
            "concerns', or an opinion column about China/chips/tariffs."
        ),
        "criteria": {
            "true": "California offers new tax credit for electric vehicles. Trump denies Iran sanctions easing.",
            "false": "India flags concerns over 100% US tariffs. China Won't Move Chip Stocks Anymore.",
        },
    },
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


CHEAP_VETO_RES: list[tuple[str, re.Pattern]] = [
    ("v_quote", re.compile(r"(?i)stock price,\s*news,\s*quote")),
    (
        "v_week",
        re.compile(
            r"(?i)watch live|earnings call (?:highlights|transcript)"
        ),
    ),
]


def cheap_veto(title: str) -> str:
    """Always-drop shapes. Skip the Jev HTTP; regex fallback also drops them."""
    t = title or ""
    for name, rx in CHEAP_VETO_RES:
        if rx.search(t):
            return name
    return ""


AHEAD_RE = re.compile(
    r"(?i)\bahead of\b|\bexpected to\b|\bwill be released\b|"
    r"\bwhat to expect\b|\bweek ahead\b"
)
OFFICIAL_PRINT_RE = re.compile(
    r"(?i)\b(?:ppi|cpi|nfp|beige book|consumer confidence)\b"
)


def cheap_keep(title: str) -> str:
    """Official prints that already landed. Skip the Jev HTTP."""
    t = title or ""
    if not t or AHEAD_RE.search(t):
        return ""
    if OFFICIAL_PRINT_RE.search(t):
        return "k_print"
    return ""


def _noul(answers: dict | None, key: str) -> float:
    if not answers:
        return 0.0
    raw = answers.get(key)
    if isinstance(raw, dict):
        raw = raw.get("noul", 0)
    try:
        return float(raw or 0.0)
    except (TypeError, ValueError):
        return 0.0


def answers_keep(answers: dict | None) -> str:
    best = ""
    score = 0.0
    for name in (
        "k_print", "k_policy", "k_done", "k_earn",
        "k_rating", "k_choke", "k_dollar",
    ):
        got = _noul(answers, name)
        if got >= KEEP_NOUL and got > score:
            best = name
            score = got
    return best


def answers_veto(answers: dict | None, hard: bool) -> str:
    for name in (
        "v_tipsheet", "v_quote", "v_week", "v_odds",
        "v_tape", "v_ask", "v_fluff",
    ):
        if _noul(answers, name) < TRASH_NOUL:
            continue
        if hard and name in {"v_tape", "v_odds"}:
            continue
        return name
    return ""


def has_bit_answers(answers: dict | None) -> bool:
    if not answers:
        return False
    return any(key in answers for key in BIT_QUESTIONS)


def decide(row: dict, answers: dict | None = None) -> dict:
    title = row.get("title") or ""
    source = row.get("source") or ""
    blob = f"{title} {source}"

    def pack(decision: str, reason: str) -> dict:
        out = {
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
        if has_bit_answers(answers):
            out["noul"] = {
                key: round(_noul(answers, key), 3) for key in BIT_QUESTIONS
            }
        return out

    cheap = cheap_veto(blob)
    if cheap:
        return pack("drop", cheap)
    landed = cheap_keep(title)
    if landed:
        return pack("keep", landed)

    if has_bit_answers(answers):
        keep = answers_keep(answers)
        hard = keep in HARD_KEEP
        if SOURCE_DENY.search(blob) and not hard:
            return pack("drop", "source")
        if title.rstrip().endswith("?") and not hard:
            return pack("drop", "v_ask")
        veto = answers_veto(answers, hard)
        if veto:
            return pack("drop", veto)
        if keep:
            return pack("keep", keep)
        code = code_keep(title)
        if code in {"k_print", "k_earn"}:
            return pack("keep", code)
        return pack("drop", "no_keep_bit")

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
