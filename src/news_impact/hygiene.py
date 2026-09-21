"""Harvest + label hygiene for news_impact. Not a new taxonomy.

Five filters, all deterministic:

1. Reaction titles are tape, not tradable impulse.
2. Guidance reaffirm / raise+miss never become a single UP.
3. Published timestamp honesty (entry_clock=published|retrieved_only).
4. Macro/factor_impulse reprints collapse to one (factor, session, sign).
5. Horizon-aware 0-1d: skip short-horizon grades for long-horizon classes.

Grade cut from #305 still holds: 0-1d / 1-4w count only when q5=impulse,
direction in {up, down}, tradeable_expression=direct. factor_impulse
legs stay ungraded context. This module adds a separate headline-basket
column so Fed reprints are not double-counted into that story rate.
"""
from __future__ import annotations

import re
from typing import Any

# --- 1) price-reaction titles ------------------------------------------------

# plunge / plunges / plunged — not "outlook plunge" / "guidance plunge"
_PLUNGE = re.compile(r"(?i)(?<!outlook )(?<!guidance )\bplung(?:e|es|ed)\b")
# surge/surges + optional percent, or stock/shares/price surge
_SURGE = re.compile(
    r"(?i)("
    r"\bsurges?\s+(?:over\s+)?\d+(?:\.\d+)?\s*%"
    r"|\b(?:stock|shares?|prices?)\b.{0,28}\bsurges?\b"
    r"|\bsurges?\b.{0,20}\b(?:stock|shares?|prices?)\b"
    r")"
)
_REBOUND = re.compile(r"(?i)\brebounds?\b")
# falls X% / fall(s) N%
_FALL_PCT = re.compile(r"(?i)\bfalls?\s+\d+(?:\.\d+)?\s*%")
# drives N% drop / drives … % drop
_DRIVES_DROP = re.compile(
    r"(?i)\bdrives?\b.{0,32}(?:\d+(?:\.\d+)?\s*)?%\s+drop"
)
# stock/shares + fall/plunge/surge/rebound (MarketWatch tape reprints)
_STOCK_TAPE = re.compile(
    r"(?i)\b(?:stock|shares?)\b.{0,24}\b(?:falls?|plung(?:e|es|ed)|surges?|rebounds?)\b"
)

REACTION_TITLE_CASES = (
    ("AAPL plunges after weak print", True),
    ("Shares plunged 9% in premarket", True),
    ("Salesforce stock surges as AI fuels growth", True),
    ("Vodafone Idea surges 5% in weak market", True),
    ("Bitcoin surges over 25% on short squeeze", True),
    ("Stocks rebound as crude oil weakens", True),
    ("Treasury yields rebound after buyback", True),
    ("Gold falls 1.2% amid firmer dollar", True),
    ("News drives 8% drop in the name", True),
    ("Barclays update drives BCS 3% drop", True),
    ("Las Vegas Sands Corp. stock falls Wednesday", True),
    ("ASML nearly sold out of 2027 EUV capacity", False),
    ("Boston Scientific says cyberattack will hit Q3", False),
    ("Cencora reaffirms fiscal 2026 guidance", False),
    ("Company raises guidance after strong quarter", False),
    ("Japan's exports surge on chips demand", False),
    ("Bookings surge for the new iPhone", False),
    ("Outlook plunges after the miss", False),
    ("Guidance plunge forces a cut", False),
)


def is_reaction_title(title: str) -> bool:
    """True when the title is a price-reaction reprint, not a new constraint."""
    t = str(title or "").strip()
    if not t:
        return False
    # Guidance/outlook plunge is a print, not tape.
    if re.search(r"(?i)\b(outlook|guidance)\b.{0,16}\bplung", t):
        return False
    if _PLUNGE.search(t):
        return True
    if _SURGE.search(t):
        return True
    if _REBOUND.search(t):
        return True
    if _FALL_PCT.search(t):
        return True
    if _DRIVES_DROP.search(t):
        return True
    if _STOCK_TAPE.search(t):
        return True
    return False


def reaction_discard_why() -> str:
    return "price-reaction title — already in the tape"


# --- 2) guidance sign hygiene ------------------------------------------------

_REAFFIRM = re.compile(
    r"(?i)(reaffirm(?:s|ed|ing)?|maintains?\s+guidance)"
)
_RAISE_GUIDANCE = re.compile(
    r"(?i)(rais(?:e|es|ed)\s+.{0,24}guidance|raises?\s+guidance|"
    r"guidance.{0,16}rais(?:e|es|ed))"
)
_MISS_EPS = re.compile(
    r"(?i)(miss(?:es|ed)?\s+(?:eps|estimates)|misses?\s+eps)"
)
_CUT_GUIDANCE = re.compile(
    r"(?i)((?:cut|soft|no longer expects|plunge).{0,30}(?:outlook|guidance)|"
    r"(?:outlook|guidance).{0,20}(?:cut|plunge|soft))"
)


def guidance_hygiene(text: str) -> dict[str, Any]:
    """Normalize guidance sign / direction. Never a single UP on mixed facts.

    reaffirm / maintains guidance → sign=None, direction=not_determined.
    raise guidance AND miss EPS → mixed (split flag), never a single UP.
    """
    blob = str(text or "")
    reaffirm = bool(_REAFFIRM.search(blob))
    raises = bool(_RAISE_GUIDANCE.search(blob))
    misses = bool(_MISS_EPS.search(blob))
    cuts = bool(_CUT_GUIDANCE.search(blob))

    if raises and misses:
        return {
            "sign": None,
            "direction": "mixed",
            "split": True,
            "split_facts": ["guidance", "print_vs_priced"],
            "why": "raise guidance + miss EPS — mixed, not a single UP",
        }
    if reaffirm and not raises and not cuts:
        return {
            "sign": None,
            "direction": "not_determined",
            "split": False,
            "split_facts": [],
            "why": "reaffirm / maintains guidance — not a raise",
        }
    if raises and not cuts:
        return {
            "sign": "raise",
            "direction": "up",
            "split": False,
            "split_facts": [],
            "why": "raises guidance",
        }
    if cuts and not raises:
        return {
            "sign": "cut",
            "direction": "down",
            "split": False,
            "split_facts": [],
            "why": "cuts / soft guidance",
        }
    if cuts and raises:
        return {
            "sign": None,
            "direction": "mixed",
            "split": True,
            "split_facts": ["guidance"],
            "why": "raise and cut in the same title — mixed",
        }
    return {
        "sign": None,
        "direction": "not_determined",
        "split": False,
        "split_facts": [],
        "why": "guidance unsigned",
    }


def guidance_direction(sign: str | None, text: str = "") -> str:
    """Entity direction from a (possibly hygiened) guidance sign."""
    hy = guidance_hygiene(text) if text else None
    if hy and hy.get("direction"):
        return str(hy["direction"])
    if sign == "cut":
        return "down"
    if sign == "raise":
        return "up"
    return "not_determined"


# --- 3) published timestamp honesty ------------------------------------------

ENTRY_CLOCK_PUBLISHED = "published"
ENTRY_CLOCK_RETRIEVED = "retrieved_only"


def entry_clock_of(row: dict | None, published_at: str | None = None) -> str:
    """published if the source gave a parseable Published time, else retrieved_only.

    Never invent a published timestamp. Missing Published still retrieves
    the item; the backtest table marks entry_clock=retrieved_only.
    """
    from .grade import parse_when

    raw = published_at
    if raw is None and row:
        raw = row.get("published_at")
    if parse_when(raw):
        return ENTRY_CLOCK_PUBLISHED
    return ENTRY_CLOCK_RETRIEVED


def signal_dt_honest(row: dict):
    """Entry clock: Published when present, else retrieved. Do not invent Published."""
    from .grade import parse_when

    pub = parse_when(row.get("published_at"))
    if pub is not None:
        return pub
    return parse_when(row.get("retrieved_at")) or parse_when(row.get("known_at"))


# --- 5) horizon-aware 0-1d ---------------------------------------------------

# Natural horizon is not 0-1d. Stay in the MD with horizon noted;
# 0-1d hit-rate columns skip them. 1-4w may still count.
SKIP_01D_EVENT_CLASSES = frozenset({
    "gate",
    "capacity",
    "blast_cyber",
})
_CHIPS = re.compile(r"(?i)\bchips\b.{0,24}(act|award)")


def is_chips_award(title: str | None, event_class: str = "") -> bool:
    if event_class == "gate" and _CHIPS.search(str(title or "")):
        return True
    return bool(_CHIPS.search(str(title or ""))) and event_class in {
        "", "gate", "capacity",
    }


def skips_01d_horizon(
    target: dict | None = None,
    row: dict | None = None,
) -> bool:
    """True → do not count this row in the 0-1d hit-rate denominator."""
    ev = ""
    horizon = "0-1d"
    title = ""
    if target:
        ev = str(target.get("event_class") or "")
        horizon = str(target.get("horizon") or "0-1d")
    if row:
        ev = ev or str((row.get("classification") or {}).get("event_class") or "")
        title = str(row.get("title") or "")
    if ev in SKIP_01D_EVENT_CLASSES:
        return True
    if is_chips_award(title, ev):
        return True
    if ev == "blast_cyber" or (ev.startswith("blast") and "cyber" in ev):
        return True
    _ = horizon  # horizon is informational; class list is authoritative
    return False


def horizon_note(target: dict | None, row: dict | None = None) -> str:
    ev = str((target or {}).get("event_class") or "")
    if row and not ev:
        ev = str((row.get("classification") or {}).get("event_class") or "")
    hz = str((target or {}).get("horizon") or "")
    if ev in SKIP_01D_EVENT_CLASSES or is_chips_award(
        str((row or {}).get("title") or ""), ev,
    ):
        return f"horizon={hz or '1-4w+'} · 0-1d skipped"
    return ""


# --- 4) macro story collapse -------------------------------------------------

MACRO_BASKET = ("QQQ", "TLT", "UUP", "HYG", "SPY")


def macro_story_key(row: dict) -> tuple[str, str, str]:
    """(factor, session, sign) for factor_impulse reprints."""
    from .grade import entry_calendar_date, parse_when

    cls = row.get("classification") or {}
    factor = str(row.get("macro_factor") or cls.get("factor") or "")
    sign = str(cls.get("sign") or "")
    session = ""
    for g in row.get("performance") or []:
        if isinstance(g, dict) and g.get("entry_date"):
            session = str(g["entry_date"])
            break
    if not session:
        when = (
            parse_when(row.get("published_at"))
            or parse_when(row.get("retrieved_at"))
            or parse_when(row.get("known_at"))
        )
        session = entry_calendar_date(when) if when else "unknown"
    return (factor, session, sign)


def majority_basket_agree(legs: list[dict], agree_key: str) -> bool | None:
    """Headline hit: majority of QQQ/TLT/UUP/HYG/SPY agreeing with implied sign."""
    votes = []
    for g in legs:
        if not isinstance(g, dict):
            continue
        if g.get("ticker") not in MACRO_BASKET:
            continue
        v = g.get(agree_key)
        if v is True or v is False:
            votes.append(bool(v))
    if not votes:
        return None
    return sum(votes) > (len(votes) / 2.0)


def collapse_macro_stories(results: list[dict]) -> dict[str, Any]:
    """One grade row per (factor, session, sign). Reprints do not add legs.

    Headline-level basket hit is the majority of QQQ/TLT/UUP/HYG/SPY.
    Leg counts are reported separately and are not added to the headline
    0-1d / 1-4w denominators.
    """
    groups: dict[tuple[str, str, str], list[dict]] = {}
    for r in results:
        cls = r.get("classification") or {}
        if cls.get("event_class") != "factor_impulse":
            continue
        groups.setdefault(macro_story_key(r), []).append(r)

    n_head_1d = hit_head_1d = 0
    n_head_20d = hit_head_20d = 0
    n_leg_1d = hit_leg_1d = 0
    n_leg_20d = hit_leg_20d = 0
    reprints_dropped = 0
    stories = []
    seen_legs: set[tuple[str, str, str]] = set()

    for key, bag in sorted(groups.items()):
        factor, session, sign = key
        reprints_dropped += max(0, len(bag) - 1)
        # First story supplies the canonical legs; later reprints are ignored.
        canonical = bag[0]
        legs: list[dict] = []
        for g in canonical.get("performance") or []:
            if not isinstance(g, dict):
                continue
            tick = str(g.get("ticker") or "")
            lk = (tick, session, sign)
            if not tick or lk in seen_legs:
                continue
            seen_legs.add(lk)
            legs.append(g)
        # If later reprints named extra basket tickers, keep unique ones.
        for extra in bag[1:]:
            for g in extra.get("performance") or []:
                if not isinstance(g, dict):
                    continue
                tick = str(g.get("ticker") or "")
                lk = (tick, session, sign)
                if not tick or lk in seen_legs:
                    continue
                seen_legs.add(lk)
                legs.append(g)

        agree_1d = majority_basket_agree(legs, "agree_1d")
        agree_20d = majority_basket_agree(legs, "agree_20d")
        if agree_1d is not None:
            n_head_1d += 1
            hit_head_1d += int(agree_1d is True)
        if agree_20d is not None:
            n_head_20d += 1
            hit_head_20d += int(agree_20d is True)
        for g in legs:
            if g.get("direction") not in {"up", "down"}:
                continue
            if g.get("ret_1d") is not None:
                n_leg_1d += 1
                hit_leg_1d += int(g.get("agree_1d") is True)
            if g.get("ret_20d") is not None:
                n_leg_20d += 1
                hit_leg_20d += int(g.get("agree_20d") is True)
        stories.append({
            "factor": factor,
            "session": session,
            "sign": sign,
            "n_reprints": len(bag),
            "n_legs": len(legs),
            "agree_1d": agree_1d,
            "agree_20d": agree_20d,
        })

    def _rate(h: int, n: int) -> float | None:
        return round(h / n, 4) if n else None

    return {
        "n_stories": len(groups),
        "reprints_collapsed": reprints_dropped,
        "headline_n_1d": n_head_1d,
        "headline_hit_1d": hit_head_1d,
        "headline_hit_rate_1d": _rate(hit_head_1d, n_head_1d),
        "headline_n_20d": n_head_20d,
        "headline_hit_20d": hit_head_20d,
        "headline_hit_rate_20d": _rate(hit_head_20d, n_head_20d),
        "leg_n_1d": n_leg_1d,
        "leg_hit_1d": hit_leg_1d,
        "leg_hit_rate_1d": _rate(hit_leg_1d, n_leg_1d),
        "leg_n_20d": n_leg_20d,
        "leg_hit_20d": hit_leg_20d,
        "leg_hit_rate_20d": _rate(hit_leg_20d, n_leg_20d),
        "stories": stories,
        "grade_rule": (
            "macro headline = one row per (factor, session, sign); "
            "hit = majority of QQQ/TLT/UUP/HYG/SPY agreeing with the "
            "implied sign. Legs are transparency only — not in the "
            "headline 0-1d / 1-4w denominators."
        ),
    }
