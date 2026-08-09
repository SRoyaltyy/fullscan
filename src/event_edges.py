"""Event families + primary/substitute edges (catalyst_analysis-compatible).

News is event-first. Each family maps to signed buckets; buckets expand to
tickers via Finviz Industry (and optional description keywords).

Weights align with CATALYST_WEIGHTS scale (roughly 1-10).
"""
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class Edge:
    bucket: str
    side: str  # buy | sell
    weight: float = 1.0
    note: str = ""


@dataclass(frozen=True)
class EventFamily:
    key: str
    taxonomy_labels: tuple[str, ...]  # nearest catalyst_analysis labels
    base_weight: int
    patterns: tuple[str, ...]  # title regex fragments (compiled later)
    primary: tuple[Edge, ...]
    substitute: tuple[Edge, ...] = ()
    amp_damp: str = ""
    mean_revert: str = ""  # when the edge likely dies
    us_equity_default: bool = True


# Bucket names → Finviz Industry strings (exact match preferred)
BUCKET_TO_INDUSTRIES: dict[str, tuple[str, ...]] = {
    "airlines": ("Airlines",),
    "airports": ("Airports & Air Services",),
    "car_rental": ("Rental & Leasing Services",),
    "trucking": ("Trucking",),
    "railroads": ("Railroads",),
    "marine_shipping": ("Marine Shipping",),
    "air_freight": ("Integrated Freight & Logistics",),
    "oil_ep": ("Oil & Gas E&P",),
    "oil_integrated": ("Oil & Gas Integrated",),
    "oil_midstream": ("Oil & Gas Midstream",),
    "oil_services": ("Oil & Gas Equipment & Services",),
    "oil_refining": ("Oil & Gas Refining & Marketing",),
    "semiconductors": ("Semiconductors",),
    "semi_equipment": ("Semiconductor Equipment & Materials",),
    "solar": ("Solar",),
    "utilities_renewable": ("Utilities - Renewable",),
    "utilities_regulated": (
        "Utilities - Regulated Electric",
        "Utilities - Diversified",
        "Utilities - Independent Power Producers",
    ),
    "aerospace_defense": ("Aerospace & Defense",),
    "gold": ("Gold",),
    "copper": ("Copper",),
    "aluminum": ("Aluminum",),
    "steel": ("Steel",),
    "banks_regional": ("Banks - Regional",),
    "banks_diversified": ("Banks - Diversified",),
    "reit_office": ("REIT - Office",),
    "reit_industrial": ("REIT - Industrial",),
    "reit_data_center": ("REIT - Specialty",),  # imperfect; filter via keywords
    "software_app": ("Software - Application",),
    "software_infra": ("Software - Infrastructure",),
    "telecom": ("Telecom Services",),
    "broadcasting": ("Broadcasting",),
    "advertising": ("Advertising Agencies",),
    "auto_manufacturers": ("Auto Manufacturers",),
    "auto_parts": ("Auto Parts",),
    "lodging": ("Lodging",),
    "travel_services": ("Travel Services",),
}

# Optional description keywords to refine a bucket (AND with industry when set)
BUCKET_DESC_KEYWORDS: dict[str, tuple[str, ...]] = {
    "car_rental": ("rental car", "car rental", "vehicle rental", "avis", "budget",
                   "hertz", "enterprise holdings"),
    "airlines": ("airline", "passenger airline", "air carrier"),
    "solar": ("solar", "photovoltaic", "pv module"),
    "reit_data_center": ("data center", "datacenter"),
}

EVENT_FAMILIES: list[EventFamily] = [
    EventFamily(
        key="airport_labor_disruption",
        taxonomy_labels=(
            "Supply chain shock (factory fire, shipping disruption)",
            "Operational setback (trial halted, satellite failure, production halt)",
        ),
        base_weight=8,
        patterns=(
            r"airport\s+(workers?\s+)?strike",
            r"airline\s+(workers?\s+)?strike",
            r"flight\s+attendants?\s+strike",
            r"pilots?\s+union\s+strike",
            r"ground\s+(?:to\s+a\s+halt|stoppage).{0,40}airport",
            r"airports?\s+.{0,30}(walkout|work\s+stoppage)",
            r"TSA\s+call[- ]?outs",
            r"ATC\s+(staffing|shortage|delay)",
        ),
        primary=(
            Edge("airlines", "sell", 1.0, "direct traffic/cost hit"),
            Edge("airports", "sell", 0.6, "ops disruption"),
        ),
        substitute=(
            Edge("car_rental", "buy", 1.2, "ground substitute for disrupted air legs"),
            Edge("lodging", "buy", 0.4, "stranded overnight — weaker/noisier"),
            Edge("railroads", "buy", 0.3, "partial modal shift — weak"),
        ),
        amp_damp=(
            "Airport/airline labor disruption: [+] systemwide/multi-hub, peak travel "
            "weekend, limited rental fleet at hubs [−] single airport, resolved same day, "
            "pure leisure cancel with no alternative trip"
        ),
        mean_revert="when strike/agreement ends or flights normalize",
    ),
    EventFamily(
        key="port_labor_disruption",
        taxonomy_labels=(
            "Supply chain shock (factory fire, shipping disruption)",
        ),
        base_weight=8,
        patterns=(
            r"port\s+(workers?\s+)?strike",
            r"longshore(men)?\s+strike",
            r"ILWU\s+strike",
            r"dockworkers?\s+(strike|walkout)",
            r"port\s+of\s+(los\s+angeles|long\s+beach|oakland|seattle).{0,40}(strike|shutdown)",
        ),
        primary=(
            Edge("marine_shipping", "sell", 0.8, "throughput risk"),
        ),
        substitute=(
            Edge("trucking", "buy", 0.7, "modal shift inland — conditional"),
            Edge("railroads", "buy", 0.6, "intermodal alternative — conditional"),
            Edge("air_freight", "buy", 0.5, "expedite freight — conditional"),
        ),
        amp_damp=(
            "Port strike: [+] West Coast multi-day, peak import season, low inventory "
            "[−] short threat only, East Coast only, already priced multi-week"
        ),
        mean_revert="contract ratification / port reopen",
    ),
    EventFamily(
        key="hormuz_energy_risk",
        taxonomy_labels=(
            "Geopolitical event that hurts sector (sanctions, conflict disrupting supply chain)",
            "Commodity price move favorable to the company",  # for E&P when oil spikes
        ),
        base_weight=8,
        patterns=(
            r"strait\s+of\s+hormuz",
            r"hormuz",
            r"iran.{0,40}(tanker|oil\s+export|blockade)",
            r"red\s+sea.{0,30}(attack|shipping|houthi)",
        ),
        primary=(
            Edge("oil_ep", "buy", 1.0, "oil risk premium"),
            Edge("oil_integrated", "buy", 0.7, "oil leverage"),
            Edge("airlines", "sell", 0.7, "fuel cost"),
        ),
        substitute=(),
        amp_damp=(
            "Hormuz/Red Sea energy risk: [+] actual transit threat, oil jumps [−] "
            "rhetoric only, oil unchanged"
        ),
        mean_revert="de-escalation / oil giveback",
    ),
    EventFamily(
        key="tariff_semis_solar",
        taxonomy_labels=(
            "Government policy (tariffs, subsidies, mandates)",
            "Policy reversal/new regulation/tax increase",
        ),
        base_weight=7,
        patterns=(
            r"tariff.{0,40}(semiconductor|chip|solar|photovoltaic)",
            r"(semiconductor|chip|solar).{0,40}tariff",
            r"section\s+301.{0,30}(solar|chip|semiconductor)",
        ),
        primary=(
            Edge("semiconductors", "sell", 0.6, "cost/uncertainty — context dependent"),
            Edge("semi_equipment", "sell", 0.5, "capex hesitation risk"),
            Edge("solar", "sell", 0.8, "module/cost hit if import-heavy"),
        ),
        substitute=(
            Edge("utilities_renewable", "sell", 0.4, "project economics if solar cost up"),
        ),
        amp_damp=(
            "Tariffs on solar/semis: [+] domestic producers may benefit (override sell) "
            "[−] import-dependent, already announced and priced"
        ),
        mean_revert="policy walk-back or phase-in delays",
    ),
    EventFamily(
        key="fed_rate_path",
        taxonomy_labels=(
            "Institutional policy (Fed rate cut, QE, stimulus)",
            "Rate hike/monetary tightening/liquidity withdrawal",
        ),
        base_weight=9,
        patterns=(
            r"\bFOMC\b",
            r"federal\s+reserve.{0,20}(rate|cut|hike)",
            r"\bPowell\b.{0,30}(rate|inflation)",
            r"rate\s+(cut|hike)\s+(odds|probability|expectations)",
        ),
        primary=(
            Edge("banks_regional", "buy", 0.3, "curve/NIM — direction needs cut vs hike disambiguation"),
            Edge("reit_office", "sell", 0.3, "duration — if hikes; invert if cuts"),
            Edge("software_app", "sell", 0.2, "duration — if hikes; invert if cuts"),
        ),
        amp_damp=(
            "Fed path: disambiguate CUT vs HIKE in classifier; duration assets opposite to hikes"
        ),
        mean_revert="after decision + press conference digested",
        us_equity_default=True,
    ),
    EventFamily(
        key="fcc_media_ownership",
        taxonomy_labels=(
            "Government policy (tariffs, subsidies, mandates)",
            "Regulatory approval (FDA, FCC, FTC clearance)",
        ),
        base_weight=6,
        patterns=(
            r"FCC.{0,40}(ownership|broadcast)",
            r"television\s+ownership\s+limit",
            r"national\s+TV\s+ownership",
        ),
        primary=(
            Edge("broadcasting", "buy", 0.8, "consolidation optionality"),
        ),
        mean_revert="if rule challenged in court",
    ),
    EventFamily(
        key="offshore_wind_cancel",
        taxonomy_labels=(
            "Policy reversal/new regulation/tax increase",
            "Operational setback (trial halted, satellite failure, production halt)",
        ),
        base_weight=6,
        patterns=(
            r"offshore\s+wind.{0,40}(abandon|cancel|scrap|payout)",
            r"(abandon|cancel).{0,40}offshore\s+wind",
        ),
        primary=(
            Edge("utilities_renewable", "sell", 0.7, "US offshore pipeline hit"),
            Edge("solar", "sell", 0.3, "sentiment spillover — weak"),
        ),
        mean_revert="new subsidy / project restart",
    ),
    EventFamily(
        key="defense_spending_surge",
        taxonomy_labels=(
            "Geopolitical event that boosts sector (e.g., defense spending surge)",
        ),
        base_weight=7,
        patterns=(
            r"defense\s+(budget|spending|bill)\s+(increase|boost|surge)",
            r"NATO\s+spending",
            r"arms\s+package.{0,20}(Ukraine|Taiwan|Israel)",
        ),
        primary=(
            Edge("aerospace_defense", "buy", 1.0, "contract/budget tailwind"),
        ),
        mean_revert="budget fight / continuing resolution freeze",
    ),
]


def all_families() -> list[EventFamily]:
    return list(EVENT_FAMILIES)


def family_by_key(key: str) -> EventFamily | None:
    for f in EVENT_FAMILIES:
        if f.key == key:
            return f
    return None
