"""US equity news decision framework (the gold-set rubric).

Applied to every candidate event/headline cluster BEFORE ticker expansion.

Dimensions (always):
  us_relevance, channel, geography, severity, horizon, action_object,
  keep (keep|drop|conditional|single_name), polarity, confidence
"""
from __future__ import annotations

import re
from dataclasses import asdict, dataclass, field


@dataclass
class FrameworkScore:
    keep: str  # keep | conditional | drop | single_name
    us_relevance: str  # high | medium | low | none
    us_relevance_why: str
    channel: str  # rates | risk | sector_policy | sector_fundamental | substitution | sentiment | none
    geography: str  # us_domestic | us_supply_chain | global_priced | foreign_weak_link
    severity: str  # regime | session | noise
    horizon: str  # 1d | 1d-1w | 1w | 1w-1m | 1m+
    action_object: str  # spx | sector_etf | basket | single_name | none
    action_object_detail: str  # e.g. XLE, XLK, CRM basket
    polarity: str  # bullish | bearish | mixed | neutral | context
    polarity_why: str
    confidence: float  # 0-1
    drop_reason: str = ""
    notes: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return asdict(self)


# --- polarity lexicons (order matters for Fed) ---
DOVISH = re.compile(
    r"(?i)("
    r"rate\s+cut|cuts?\s+rates?|eases?\s+rate[- ]?hike|allays?\s+rate[- ]?hike|"
    r"cooling\s+(fed\s+)?rate\s+hike|hike\s+(fears?|odds|expectations?).{0,20}"
    r"(cool|fall|ease|cut)|cut\s+chances?\s+of.{0,20}hike|"
    r"soft\s+jobs?.{0,30}(rate|fed|hike)|weak\s+jobs?.{0,30}(rate|hike|fed)|"
    r"dovish|fewer\s+hikes|no\s+hike|pause\s+rates?"
    r")"
)
HAWKISH = re.compile(
    r"(?i)("
    r"rate\s+hike|hikes?\s+rates?|hawkish|hotter\s+than\s+expected\s+inflation|"
    r"higher\s+for\s+longer|reaccelerate|strong\s+jobs?.{0,20}(hike|fed)"
    r")"
)

BULLISH_MKT = re.compile(
    r"(?i)(surge|soar|record\s+high|rally|beat|raises?\s+guidance|demand\s+spike|"
    r"commits?\s+to|buyback|spending\s+boost)"
)
BEARISH_MKT = re.compile(
    r"(?i)(selloff|slump|plunge|swing\s+wildly|compression|cancel|abandon|"
    r"tariff|disruption|scramble|war\s+live|targeted)"
)


def infer_polarity_from_text(titles: list[str]) -> tuple[str, str]:
    blob = " | ".join(titles)
    d, h = bool(DOVISH.search(blob)), bool(HAWKISH.search(blob))
    if d and not h:
        return "dovish", "dovish language dominant (eases hike fears / cut odds / soft jobs→Fed)"
    if h and not d:
        return "hawkish", "hawkish language dominant"
    if d and h:
        return "mixed", "both dovish and hawkish cues present"
    b, e = len(BULLISH_MKT.findall(blob)), len(BEARISH_MKT.findall(blob))
    if b > e and b:
        return "bullish", "bullish verbs/outcomes in headlines"
    if e > b and e:
        return "bearish", "bearish verbs/outcomes in headlines"
    return "neutral", "no clear directional lexicon"


def score_event(
    event_key: str,
    channel: str,
    horizon_default: str,
    titles: list[str],
    headline_count: int,
    mechanism: str = "",
) -> FrameworkScore:
    """Apply keep/channel/object/horizon framework to a matched event."""
    blob = " | ".join(titles)
    pol, pol_why = infer_polarity_from_text(titles)
    notes: list[str] = []

    # --- defaults by event family ---
    table = _EVENT_FRAMEWORK.get(event_key, {})
    keep = table.get("keep", "conditional")
    us_rel = table.get("us_relevance", "medium")
    us_why = table.get("us_relevance_why", mechanism or event_key)
    geo = table.get("geography", "us_domestic")
    sev = table.get("severity", "session")
    horizon = table.get("horizon", horizon_default)
    obj = table.get("action_object", "basket")
    obj_detail = table.get("action_object_detail", "")
    conf = float(table.get("confidence", 0.55))

    # severity bump if many confirming headlines
    if headline_count >= 5:
        if sev == "noise":
            sev = "session"
        elif sev == "session":
            sev = "regime" if event_key in ("hormuz_energy_risk", "fed_rate_path") else sev
        conf = min(0.95, conf + 0.1)
        notes.append(f"headline_count={headline_count} supports non-noise severity")

    # Fed / labor polarity → directional confidence
    if event_key in ("fed_rate_path", "weak_labor_print"):
        if pol in ("dovish", "hawkish"):
            conf = min(0.9, conf + 0.15)
            notes.append(f"rates polarity={pol}")
        else:
            conf = max(0.25, conf - 0.2)
            notes.append("rates polarity unclear → lower confidence / soft book")
            if event_key == "fed_rate_path":
                keep = "conditional"

    # SaaS: explicit bearish sector narrative
    if event_key == "saas_multiple_compression":
        if re.search(r"(?i)swings?\s+off|shakes\s+off|vows\s+to\s+spend|buyback", blob):
            notes.append("some names defending (buybacks) — basket still soft, not uniform")
            conf = max(0.4, conf - 0.05)
        pol = "bearish"
        pol_why = "sector narrative is multiple compression / swing risk"

    # Tariff solar: domestic winners possible
    if event_key == "tariff_semis_solar":
        if re.search(r"(?i)corning|domestic|u\.?s\.?\s+manufactur|shares\s+pop", blob):
            notes.append("domestic beneficiary headlines present — do not treat all solar/semi as equal sells")
            pol = "mixed"
            pol_why = "tariff hurts importers; some domestic suppliers can pop"
            conf = min(0.85, conf + 0.05)

    # Hormuz: rhetoric vs targeting
    if event_key == "hormuz_energy_risk":
        if re.search(r"(?i)targeted|tanker|jet\s+fuel|scramble|blockade", blob):
            conf = min(0.92, conf + 0.1)
            notes.append("transit/tanker/fuel language → higher severity")
            sev = "regime" if headline_count >= 3 else "session"
        pol = "mixed"  # oil + / airlines −
        pol_why = "bullish upstream oil risk premium; bearish fuel-cost airlines"

    # AI power: pollution framing still power demand
    if event_key == "ai_power_demand":
        pol = "bullish"
        pol_why = "load growth narrative for generation/power scarcity names"

    # Chip demand vs tariff conflict handled in interactions
    if event_key == "ai_chip_demand_spike":
        pol = "bullish"
        pol_why = "named AI demand / fab investment narrative"

    if event_key == "offshore_wind_cancel":
        pol = "bearish"
        pol_why = "US offshore wind policy/project destruction"
        notes.append("prefer pure-play wind/renewables exposure over broad IPPs when possible")

    if event_key == "fcc_media_ownership":
        pol = "bullish"
        pol_why = "consolidation optionality for broadcasters"

    if keep == "drop":
        return FrameworkScore(
            keep="drop", us_relevance="none", us_relevance_why=us_why,
            channel="none", geography=geo, severity="noise", horizon="1d",
            action_object="none", action_object_detail="",
            polarity="neutral", polarity_why="dropped", confidence=0.1,
            drop_reason=table.get("drop_reason", "fails US equity relevance gate"),
            notes=notes,
        )

    return FrameworkScore(
        keep=keep,
        us_relevance=us_rel,
        us_relevance_why=us_why,
        channel=channel or table.get("channel", "sector_fundamental"),
        geography=geo,
        severity=sev,
        horizon=horizon,
        action_object=obj,
        action_object_detail=obj_detail,
        polarity=pol,
        polarity_why=pol_why,
        confidence=round(conf, 2),
        notes=notes,
    )


# Static framework priors per event (the gold-set encoded)
_EVENT_FRAMEWORK: dict[str, dict] = {
    "hormuz_energy_risk": {
        "keep": "keep",
        "us_relevance": "high",
        "us_relevance_why": "Global oil chokepoint the US market prices daily (WTI/Brent, XLE, fuel costs)",
        "geography": "global_priced",
        "severity": "session",
        "horizon": "1d-1w",
        "action_object": "sector_etf",
        "action_object_detail": "XLE long bias / airline basket short / optional SPX if risk-off",
        "channel": "risk",
        "confidence": 0.75,
    },
    "tariff_semis_solar": {
        "keep": "keep",
        "us_relevance": "high",
        "us_relevance_why": "US policy directly hits costs/structure of solar & semi supply chains",
        "geography": "us_domestic",
        "severity": "regime",
        "horizon": "1w-1m",
        "action_object": "basket",
        "action_object_detail": "solar basket first-order; semis second-order unless chip-specific",
        "channel": "sector_policy",
        "confidence": 0.7,
    },
    "fed_rate_path": {
        "keep": "keep",
        "us_relevance": "high",
        "us_relevance_why": "Discount rate / risk appetite for all US duration and banks",
        "geography": "us_domestic",
        "severity": "session",
        "horizon": "1d-1w",
        "action_object": "spx",
        "action_object_detail": "SPX beta + rate-sensitive baskets (not a single stock thesis)",
        "channel": "rates",
        "confidence": 0.65,
    },
    "weak_labor_print": {
        "keep": "keep",
        "us_relevance": "high",
        "us_relevance_why": "Hard US macro print that moves Fed path and risk appetite",
        "geography": "us_domestic",
        "severity": "session",
        "horizon": "1d-1w",
        "action_object": "spx",
        "action_object_detail": "SPX/rates path; gold; not ‘jobs ETF’",
        "channel": "rates",
        "confidence": 0.7,
    },
    "saas_multiple_compression": {
        "keep": "keep",
        "us_relevance": "high",
        "us_relevance_why": "US-listed crowded software basket re-rating",
        "geography": "us_domestic",
        "severity": "session",
        "horizon": "1d-1w",
        "action_object": "basket",
        "action_object_detail": "IGV/software app basket (not XLK megacap-only)",
        "channel": "sector_fundamental",
        "confidence": 0.7,
    },
    "ai_power_demand": {
        "keep": "keep",
        "us_relevance": "high",
        "us_relevance_why": "US data-center load → US generation/utilities cash-flow narrative",
        "geography": "us_domestic",
        "severity": "session",
        "horizon": "1w-1m",
        "action_object": "basket",
        "action_object_detail": "CEG/VST/NRG-type power; not pollution pure-plays",
        "channel": "sector_fundamental",
        "confidence": 0.65,
    },
    "ai_chip_demand_spike": {
        "keep": "keep",
        "us_relevance": "high",
        "us_relevance_why": "US-listed semi demand / domestic fab narrative",
        "geography": "us_domestic",
        "severity": "session",
        "horizon": "1d-1w",
        "action_object": "basket",
        "action_object_detail": "SOX/SMH bias; NVDA often first-price",
        "channel": "sector_fundamental",
        "confidence": 0.6,
    },
    "fcc_media_ownership": {
        "keep": "keep",
        "us_relevance": "medium",
        "us_relevance_why": "US regulatory structure for broadcast consolidation",
        "geography": "us_domestic",
        "severity": "session",
        "horizon": "1w-1m",
        "action_object": "basket",
        "action_object_detail": "broadcast group basket — not whole XLC",
        "channel": "sector_policy",
        "confidence": 0.55,
    },
    "offshore_wind_cancel": {
        "keep": "keep",
        "us_relevance": "medium",
        "us_relevance_why": "US project/policy destruction in offshore wind CapEx",
        "geography": "us_domestic",
        "severity": "session",
        "horizon": "1w-1m",
        "action_object": "basket",
        "action_object_detail": "offshore wind / pure renewables — weak map to broad IPPs",
        "channel": "sector_policy",
        "confidence": 0.5,
    },
    "regional_bank_ai_lending": {
        "keep": "conditional",
        "us_relevance": "medium",
        "us_relevance_why": "US regional loan growth narrative — soft without credit confirm",
        "geography": "us_domestic",
        "severity": "noise",
        "horizon": "1w",
        "action_object": "basket",
        "action_object_detail": "KRE / regional bank basket",
        "channel": "sector_fundamental",
        "confidence": 0.4,
    },
    "airport_labor_disruption": {
        "keep": "keep",
        "us_relevance": "high",
        "us_relevance_why": "US travel node shock with substitute demand (CAR pattern)",
        "geography": "us_domestic",
        "severity": "session",
        "horizon": "1d-1w",
        "action_object": "basket",
        "action_object_detail": "airlines sell / CAR buy",
        "channel": "substitution",
        "confidence": 0.7,
    },
    "port_labor_disruption": {
        "keep": "keep",
        "us_relevance": "high",
        "us_relevance_why": "US port throughput / supply chain",
        "geography": "us_supply_chain",
        "severity": "session",
        "horizon": "1w",
        "action_object": "basket",
        "action_object_detail": "shipping / trucking / rail",
        "channel": "substitution",
        "confidence": 0.65,
    },
    "defense_spending_surge": {
        "keep": "keep",
        "us_relevance": "high",
        "us_relevance_why": "US/allied budget → US prime contractors",
        "geography": "us_domestic",
        "severity": "session",
        "horizon": "1w-1m",
        "action_object": "basket",
        "action_object_detail": "ITA / defense primes",
        "channel": "sector_policy",
        "confidence": 0.6,
    },
}


def apply_interactions(events: list[dict]) -> list[str]:
    """Catalyst-style interaction rules across concurrent events."""
    keys = {e.get("event") for e in events}
    msgs = []
    if "saas_multiple_compression" in keys and "weak_labor_print" in keys:
        msgs.append(
            "INTERACTION: SaaS compression + weak labor both live → do NOT buy software "
            "on dovish-rates hope; SaaS sector narrative dominates application software."
        )
    if "ai_chip_demand_spike" in keys and "tariff_semis_solar" in keys:
        msgs.append(
            "INTERACTION: AI chip demand (bullish) vs solar/semi tariff (bearish) → "
            "net semis mixed; prefer not to double-count NVDA both ways without split."
        )
    if "ai_power_demand" in keys and "offshore_wind_cancel" in keys:
        msgs.append(
            "INTERACTION: AI power demand (bullish IPPs) vs offshore wind cancel (bearish "
            "pure wind) → do not short CEG/VST solely on wind headlines."
        )
    if "fed_rate_path" in keys and "weak_labor_print" in keys:
        msgs.append(
            "INTERACTION: Weak labor is the driver of Fed path this window — treat as one "
            "rates cluster, not two independent scores."
        )
    return msgs
