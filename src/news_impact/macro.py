"""Ticker-less macro: trade the factor, never a name in the lede.

A Fed-path speech, "in focus", or hike-odds reprint is weather (Q5 regime).
A print or a decision with a signed surprise is impulse: emit the locked
ETF basket for that factor. Themes are not classes — this stays
factor_impulse.
"""
from __future__ import annotations

import re
from typing import Any

from .schema import Classification, Entity, family_of

# Specific before the Fed catch-all.
_FACTORS: list[tuple[str, re.Pattern]] = [
    ("inflation", re.compile(r"(?i)(\bcpi\b|\bpce\b|\bppi\b|inflation)")),
    ("labor", re.compile(
        r"(?i)(payrolls|non-?farm|\bnfp\b|unemployment|jobless|jolts|"
        r"jobs (report|outlook)|weakening jobs)"
    )),
    ("growth", re.compile(r"(?i)(\bgdp\b|\bism\b|\bpmi\b|retail sales)")),
    ("oil", re.compile(r"(?i)(opec|\bbrent\b|\bwti\b|crude oil|oil prices?)")),
    ("risk", re.compile(
        r"(?i)(tariff|trade war|section 301|escalat|"
        r"imposes? (new )?(sanctions|duties))"
    )),
    ("usd", re.compile(r"(?i)(\bdxy\b|us dollar|dollar (rises|slips|index))")),
    ("rates", re.compile(
        r"(?i)(fomc|federal reserve|\bfed\b|rate hike|rate cut|funds rate|"
        r"jackson hole|dot plot|\bsep\b|ecb )"
    )),
]

_PRINT = re.compile(
    r"(?i)(\bcpi\b|\bpce\b|\bppi\b|payrolls|non-?farm|\bnfp\b|jobless claims|"
    r"jolts|\bgdp\b|\bism\b|\bpmi\b|retail sales)"
)
_DECISION = re.compile(
    r"(?i)(fomc (decision|statement|holds|hikes|cuts)|"
    r"fed (holds|hikes|cuts|raises) (rates?|the funds)|"
    r"raises? (the )?(funds )?rate|cuts? (the )?(funds )?rate|"
    r"dot plot|summary of economic projections|"
    r"dropping all mention|strips away all rate guidance|"
    r"opec\+?.{0,40}(cut|boost|output|agree)|"
    r"(imposes?|announces?|levies?|officially).{0,24}tariff)"
)
_SURPRISE = re.compile(
    r"(?i)(hotter than|cooler than|comes? in (hot|warm|cool|soft|hotter)|"
    r"above (forecast|expect|consensus)|below (forecast|expect|consensus)|"
    r"more than (expected|forecast)|less than (expected|forecast)|"
    r"beats? (forecast|expect)|misses? (forecast|expect)|"
    r"(sticky|humid) .{0,12}(cpi|pce|ppi)|"
    r"retail sales.{0,20}(warm|hot|beat|miss|soft)|"
    r"cool(er)? (cpi|pce|ppi|inflation))"
)
_HAWK = re.compile(
    r"(?i)(hike|hawk|hotter|comes? in (hot|warm)|warm ahead|"
    r"above (forecast|expect|consensus)|raises? (rates?|the funds)|"
    r"hot (cpi|pce|ppi)|strong (payroll|jobs|gdp)|"
    r"retail sales.{0,20}(warm|hot|beat)|sticky)"
)
_DOVE = re.compile(
    r"(?i)(dovish|cooler|comes? in (cool|soft)|"
    r"below (forecast|expect|consensus)|cuts? (rates?|the funds)|"
    r"soft (jobs|payroll|cpi|pce)|miss(es)? (forecast|expect)|"
    r"weaker (jobs|payroll)|holds? rates?|cool(er)? (cpi|pce|inflation)|"
    r"odds fall)"
)
_PREVIEW = re.compile(
    r"(?i)(ahead of|preview|what to (watch|expect)|investors (seek|await|weigh|eye|brace)|"
    r"keeps? .{0,30}(fed|outlook|in focus)|in focus|"
    r"expected to (hold|hike|cut)|(hike|cut) (odds|bets)|odds (fall|rise)|"
    r"cointoss|wager on .{0,20}fed|"
    r"wall street (slips|gains|loses|inch)|"
    r"asian (shares|stocks) (rise|fall)|"
    r"futures (inch|rise|fall))"
)
_SPEECH = re.compile(
    r"(?i)(warsh|hammack|powell|fed (official|member|governor|chair)).{0,70}"
    r"(says|said|warns|urges|hints|calls for|speech|address|essay)|"
    r"quieter fed|in closely watched speech"
)
_RUMOR = re.compile(
    r"(?i)(considers?|may (impose|hike|cut)|report says|sources say|"
    r"unconfirmed|rumors? of)"
)
_NEWSLETTER = re.compile(
    r"(?i)(after comparing every|these \d win|bond etf paying|"
    r"\d reasons .{0,40}(fomc|fed)|crypto daily)"
)

PRIMARY = {
    "inflation": ("TLT", "long Treasuries / duration"),
    "labor": ("QQQ", "Nasdaq / duration"),
    "growth": ("SPY", "broad US equity"),
    "rates": ("TLT", "long Treasuries / duration"),
    "usd": ("UUP", "US dollar"),
    "oil": ("XLE", "Energy / crude"),
    "risk": ("SPY", "broad US equity"),
}

# hawkish surprise: duration + credit down, dollar up.
_HAWK_BASKET = (
    ("QQQ", "Nasdaq / duration", "down", "A_MAC_02"),
    ("TLT", "long Treasuries", "down", "A_MAC_02"),
    ("UUP", "US dollar", "up", "A_MAC_02"),
    ("HYG", "high-yield credit", "down", "A_MAC_02"),
    ("SPY", "broad US equity", "down", "A_MAC_02"),
)
_OIL_TIGHT = (
    ("XLE", "Energy", "up", "A_MAC_05"),
    ("USO", "crude oil", "up", "A_MAC_05"),
    ("SPY", "broad US equity", "down", "A_MAC_05"),
)
_RISK_OFF = (
    ("SPY", "broad US equity", "down", "A_MAC_04"),
    ("TLT", "long Treasuries", "up", "A_MAC_04"),
    ("GLD", "gold / risk hedge", "up", "A_MAC_04"),
)


def factor_of(text: str) -> str:
    for name, pat in _FACTORS:
        if pat.search(text):
            return name
    return "rates"


def macro_status(text: str) -> dict[str, Any]:
    """Q5 + sign + factor for a ticker-less macro headline."""
    factor = factor_of(text)
    has_print = bool(_PRINT.search(text))
    has_decision = bool(_DECISION.search(text))
    has_surprise = bool(_SURPRISE.search(text))
    hawk = bool(_HAWK.search(text))
    dove = bool(_DOVE.search(text))
    preview = bool(_PREVIEW.search(text))
    speech = bool(_SPEECH.search(text))
    rumor = bool(_RUMOR.search(text))
    newsletter = bool(_NEWSLETTER.search(text))

    if newsletter:
        return {
            "q5": "regime", "sign": None, "factor": factor,
            "why": "newsletter / odds piece — no new constraint",
            "event_class": "discard",
        }
    if rumor and not (has_decision or has_print and has_surprise):
        return {
            "q5": "regime", "sign": None, "factor": factor,
            "why": "rumor / considers — not a binding print",
            "event_class": "rumor",
        }
    if speech and not (has_decision or has_surprise):
        return {
            "q5": "regime", "sign": None, "factor": factor,
            "why": "Fed-path speech is weather until the print or the decision",
            "event_class": "regime_state",
        }
    live = has_decision or has_surprise or (has_print and has_surprise)
    if preview and not live:
        return {
            "q5": "regime", "sign": None, "factor": factor,
            "why": "ahead-of / odds / in-focus — weather until the number prints",
            "event_class": "regime_state",
        }
    if live:
        sign = None
        if dove and not hawk:
            sign = "cut"
        elif hawk and not dove:
            sign = "raise"
        elif dove and hawk:
            sign = "cut"
        if factor == "oil" and re.search(r"(?i)opec", text):
            if re.search(r"(?i)(cut|output cut|agree to cut)", text):
                sign = "raise"
            elif re.search(r"(?i)(boost|increase|raise output)", text):
                sign = "cut"
        if factor == "risk" and has_decision:
            sign = sign or "raise"
        why = (
            f"{factor} print/decision reprices the factor basket"
            if has_print or has_decision else
            f"{factor} surprise in the title"
        )
        return {
            "q5": "impulse", "sign": sign, "factor": factor,
            "why": why, "event_class": "factor_impulse",
        }
    # Bare Fed / dollar color with no number.
    return {
        "q5": "regime", "sign": None, "factor": factor,
        "why": "macro color without a print or a decision",
        "event_class": "regime_state",
    }


def apply_macro(cls: Classification, text: str) -> Classification:
    """Override a factor_impulse (or newly-caught macro) with Q5 + sign."""
    st = macro_status(text)
    ev = st["event_class"]
    return Classification(
        event_class=ev,
        sign=st["sign"],
        q5=st["q5"],
        constraint=f"{st['factor']}/{st['sign'] or 'unsigned'}: {cls.constraint}"[:200],
        split=cls.split,
        split_facts=list(cls.split_facts),
        why=st["why"],
        family=family_of(ev),
        factor=st["factor"],
    )


def _ent(tick: str, name: str, direction: str, axiom: str,
         role: str = "named", if_unknown: str | None = None) -> Entity:
    return Entity(
        name=name,
        ticker=tick,
        role=role,
        direction=direction,
        horizon="0-1d",
        unit_vs_parent="slice",
        tradeable_expression="proxy",
        relation="factor",
        axiom_id=axiom,
        inferred=True,
        if_unknown=if_unknown,
    )


def _flip(direction: str, dove: bool) -> str:
    if not dove:
        return direction
    if direction == "up":
        return "down"
    if direction == "down":
        return "up"
    return direction


def macro_entities(text: str, cls: Classification) -> list[Entity]:
    """Locked factor basket. Never a random single-name from the lede."""
    st = macro_status(text)
    factor = cls.factor or st["factor"]
    sign = cls.sign or st["sign"]
    dove = sign == "cut"
    if st["q5"] == "regime" or st["event_class"] in {"regime_state", "rumor", "discard"}:
        return []
    if sign not in {"raise", "cut"}:
        tick, name = PRIMARY.get(factor, PRIMARY["rates"])
        return [_ent(
            tick, name, "not_determined", "A_MAC_01",
            if_unknown="gap vs consensus not in the title",
        )]
    if factor == "oil":
        bag = _OIL_TIGHT
        axiom = "A_MAC_05"
    elif factor == "risk":
        bag = _RISK_OFF
        axiom = "A_MAC_04"
        dove = False  # risk-off basket is already signed; do not invert
    else:
        bag = _HAWK_BASKET
        axiom = "A_MAC_02"
    out = []
    for tick, name, direction, ax in bag:
        out.append(_ent(tick, name, _flip(direction, dove), ax))
    return out
