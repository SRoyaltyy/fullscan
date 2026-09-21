"""Closed enums and result objects for the news-impact router."""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any

PIPELINE_VERSION = "news_impact_v2"

# Locked ship enum (families + sign). A new class needs a new loser AND winner set.
EVENT_CLASSES = (
    "capacity",
    "demand",
    "input_cost",
    "inventory_print",
    "channel_stock",
    "gate",
    "trial_readout",
    "access_control",
    "sanction_lift",
    "standard_mandate",
    "ip_ruling",
    "market_structure",
    "tax_fiscal",
    "price_cap",
    "subsidy",
    "breakup_remedy",
    "print_vs_priced",
    "guidance",
    "preannounce",
    "peer_spill",
    "factor_impulse",
    "corporate_action_mna",
    "corporate_action_spinoff",
    "dilution",
    "capital_return",
    "listing_flow",
    "lockup_expiry",
    "credit_funding",
    "distress_restruct",
    "integrity",
    "key_person",
    "insider_flow",
    "blast_legal",
    "blast_ops",
    "blast_cyber",
    "product_harm",
    "labor_stop",
    "cat_weather",
    "flow_index",
    "flow_mechanical",
    "flow_forced_liq",
    "regime_state",
    "regime_break",
    "statement_public",
    "rumor",
    "fx_translation",
    "regulatory_probe",
    "activist_campaign",
    "policy_personnel",
    "sovereign_credit",
    "deal_review",
    "labor_organize",
    "strategic_review",
    "reserve_revision",
    "discard",
)

FAMILY_OF = {
    "blast_legal": "blast",
    "blast_ops": "blast",
    "blast_cyber": "blast",
    "product_harm": "blast",
    "labor_stop": "blast",
    "cat_weather": "blast",
    "market_structure": "structure",
    "tax_fiscal": "structure",
    "price_cap": "structure",
    "subsidy": "structure",
    "breakup_remedy": "structure",
    "standard_mandate": "structure",
    "access_control": "structure",
    "sanction_lift": "structure",
    "fx_translation": "structure",
    "capacity": "quantity",
    "demand": "quantity",
    "input_cost": "quantity",
    "inventory_print": "quantity",
    "channel_stock": "quantity",
    "reserve_revision": "quantity",
    "gate": "permission",
    "trial_readout": "permission",
    "ip_ruling": "permission",
    "print_vs_priced": "print",
    "guidance": "print",
    "preannounce": "print",
    "factor_impulse": "print",
    "peer_spill": "print",
    "corporate_action_mna": "firm",
    "corporate_action_spinoff": "firm",
    "dilution": "firm",
    "capital_return": "firm",
    "credit_funding": "firm",
    "distress_restruct": "firm",
    "integrity": "firm",
    "key_person": "firm",
    "insider_flow": "firm",
    "activist_campaign": "firm",
    "strategic_review": "firm",
    "regulatory_probe": "firm",
    "deal_review": "firm",
    "listing_flow": "flow",
    "lockup_expiry": "flow",
    "flow_index": "flow",
    "flow_mechanical": "flow",
    "flow_forced_liq": "flow",
    "regime_state": "time",
    "regime_break": "time",
    "statement_public": "time",
    "rumor": "time",
    "discard": "time",
    "policy_personnel": "structure",
    "sovereign_credit": "firm",
    "labor_organize": "blast",
}

SIGNS = ("add", "destroy", "up", "down", "open", "shut", "tighten", "lift", "cut", "raise")
Q5_STATUS = ("impulse", "regime", "regime_break")
DIRECTIONS = ("up", "down", "mixed", "not_determined")
ROLES = (
    "named", "parent_of_named", "substitute", "complement",
    "unscathed_rival", "arms_dealer", "incumbent_intermediary", "new_venue",
    "supplier", "customer", "competitor",
)
HORIZONS = ("0-1d", "1-4w", "1-6m", "6m+")
UNIT_VS_PARENT = ("whole", "slice", "unknown")
TRADEABLE = ("direct", "proxy", "none")

DISCARD_OR_WEATHER = frozenset({"discard", "regime_state", "rumor"})


def family_of(event_class: str) -> str:
    return FAMILY_OF.get(event_class, "time")


@dataclass
class Entity:
    name: str
    ticker: str | None
    role: str
    direction: str
    horizon: str = "0-1d"
    named_unit: str | None = None
    unit_vs_parent: str = "whole"
    tradeable_expression: str = "direct"
    relation: str = "primary"
    basis: str = "absolute"
    stays_out: bool = False
    wins_either_outcome: bool = False
    if_unknown: str | None = None
    axiom_id: str | None = None
    inferred: bool = False

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class Classification:
    event_class: str
    sign: str | None
    q5: str
    constraint: str
    split: bool = False
    split_facts: list[str] = field(default_factory=list)
    why: str = ""
    family: str = "time"
    factor: str = ""

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["family"] = family_of(self.event_class)
        return d


def empty_entity_ok(cls: Classification) -> bool:
    """entities:[] is legal only for weather reprints or true discards."""
    return cls.event_class in DISCARD_OR_WEATHER or cls.q5 == "regime"


def is_usable(cls: Classification, entities: list[Entity]) -> bool:
    """Usable = impulse/regime_break with a real mechanism, not weather/junk.

    A named entity (even not_determined) counts. Empty lists are only
    usable-illegal weather/discard — those are correctly *not* usable.
    """
    if cls.q5 == "regime" or cls.event_class in DISCARD_OR_WEATHER:
        return False
    if cls.event_class == "statement_public" and not entities:
        return False
    if entities:
        return True
    # Identified mechanism, no listed expression yet (private names).
    return cls.event_class not in DISCARD_OR_WEATHER and cls.q5 != "regime"


def is_tradable(cls: Classification, entities: list[Entity]) -> bool:
    """Tradable = usable AND a listed ticker with an up/down call.

    Ticker-less macro is tradable when the factor basket is signed
    (QQQ/TLT/UUP/XLE/SPY). not_determined and private names are not.
    """
    if not is_usable(cls, entities):
        return False
    return any(
        e.direction in {"up", "down"}
        and bool(e.ticker)
        and e.tradeable_expression != "none"
        for e in entities
    )
