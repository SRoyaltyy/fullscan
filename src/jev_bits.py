"""Hop-0 for every harvest.

Live path: six frozen bits. Keep iff (done OR print) AND NOT tape
AND NOT soft AND NOT tip. Criteria are 2 true + 2 false.
Code-only / no-answers path: the regex pack (also the lock fallback).
"""
from __future__ import annotations

import re

BIT_NOUL = 0.60
TRASH_NOUL = 0.70
KEEP_NOUL = BIT_NOUL
DUP_JACCARD = 0.72
SIX_BITS = ("tape", "soft", "tip", "done", "print", "listed")

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
    "tape": {
        "type": "noul",
        "instructions": (
            "TRUE if the price move is already in the title: a percent, "
            "'surges', 'stock today', or a close recap. FALSE if the title "
            "is the act or the official print itself."
        ),
        "criteria": {
            "true": (
                "FuelCell Energy Inc. stock falls Wednesday, underperforms market. "
                "DOCS Stock Surges 40% Following Earnings."
            ),
            "false": (
                "Nasdaq to Buy Dark Pool Stock Venue LeveL. "
                "US July PPI Below Expectations."
            ),
        },
    },
    "soft": {
        "type": "noul",
        "instructions": (
            "TRUE if this is a preview: 'may', 'could', 'what to watch', "
            "'ahead of', or earnings call highlights/transcript. FALSE if "
            "the official print or the finished act already happened here."
        ),
        "criteria": {
            "true": (
                "Asia stocks gain ahead of U.S. PCE. "
                "Kadant Q2 Earnings Call Highlights."
            ),
            "false": (
                "US July PPI Below Expectations. "
                "Fed's Williams: No Rush on Rate Hikes."
            ),
        },
    },
    "tip": {
        "type": "noul",
        "instructions": (
            "TRUE if this is a buy/sell list, 'which is better', or a "
            "column. FALSE if a named firm closed a deal or an official "
            "print landed."
        ),
        "criteria": {
            "true": (
                "3 Financial Mutual Funds to Consider as Fed Signals More Rate Hikes. "
                "Archer Aviation vs. AST SpaceMobile: Which Industrials Stock Is a Better Buy in 2026?"
            ),
            "false": (
                "Nasdaq to Buy Dark Pool Stock Venue LeveL. "
                "AMETEK completes acquisition."
            ),
        },
    },
    "done": {
        "type": "noul",
        "instructions": (
            "TRUE if the act already happened: deal closed, listing pulled, "
            "rate was raised, company printed results, court ordered a pay. "
            "FALSE if someone touts wins or recaps an old IPO price."
        ),
        "criteria": {
            "true": (
                "Apple ordered to pay $5.7bn in haptic tech patent case. "
                "U.S. Taps Strategic Oil Reserve Again as Diesel Tops $6."
            ),
            "false": (
                "Capita Flags CSPS Costs but Touts Contract Wins. "
                "$10,000 invested at SpaceX stock IPO is now worth."
            ),
        },
    },
    "print": {
        "type": "noul",
        "instructions": (
            "TRUE if this sentence is a numbered official release or a "
            "named Fed officer speaking. FALSE if the print is only "
            "upcoming, expected, or a tip sheet that mentions the Fed."
        ),
        "criteria": {
            "true": (
                "US July PPI Below Expectations. "
                "Barr Says Further Fed Rate Hikes Likely Needed As Inflation Remains Too High."
            ),
            "false": (
                "Asia stocks gain ahead of U.S. PCE. "
                "3 Financial Mutual Funds to Consider as Fed Signals More Rate Hikes."
            ),
        },
    },
    "listed": {
        "type": "noul",
        "instructions": (
            "TRUE if a US-tradable name or a policy lever that hits one "
            "is in the title. FALSE if this is local color or a protest."
        ),
        "criteria": {
            "true": (
                "Nasdaq to Buy Dark Pool Stock Venue LeveL. "
                "Nvidia’s $10.2 Billion Quarterly Profit Increase."
            ),
            "false": (
                "Bakery buys comedian's former sites after closure. "
                "RSS chief’s US visit faces protests over Hindu nationalism."
            ),
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


def bit_on(answers: dict | None, name: str) -> bool:
    return _noul(answers, name) >= BIT_NOUL


def formula_reason(answers: dict | None) -> tuple[str, str]:
    """Keep iff (done OR print) AND NOT tape AND NOT soft AND NOT tip."""
    if bit_on(answers, "tape"):
        return "drop", "tape"
    if bit_on(answers, "soft"):
        return "drop", "soft"
    if bit_on(answers, "tip"):
        return "drop", "tip"
    if bit_on(answers, "print"):
        return "keep", "print"
    if bit_on(answers, "done"):
        return "keep", "done"
    return "drop", "no_keep_bit"


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

    if has_bit_answers(answers):
        done = bit_on(answers, "done")
        printed = bit_on(answers, "print")
        hard = (
            (done or printed)
            and not bit_on(answers, "tape")
            and not bit_on(answers, "soft")
            and not bit_on(answers, "tip")
        )
        if SOURCE_DENY.search(blob) and not hard:
            return pack("drop", "source")
        decision, reason = formula_reason(answers)
        return pack(decision, reason)

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
