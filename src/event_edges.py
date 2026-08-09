"""Event families + primary/substitute edges (catalyst_analysis-compatible)."""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Edge:
    bucket: str
    side: str  # buy | sell
    weight: float = 1.0
    note: str = ""


@dataclass(frozen=True)
class EventFamily:
    key: str
    taxonomy_labels: tuple[str, ...]
    base_weight: int
    patterns: tuple[str, ...]
    primary: tuple[Edge, ...]
    substitute: tuple[Edge, ...] = ()
    amp_damp: str = ""
    mean_revert: str = ""
    us_equity_default: bool = True


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
    "semiconductors": ("Semiconductors",),
    "semi_equipment": ("Semiconductor Equipment & Materials",),
    "solar": ("Solar",),
    "utilities_renewable": ("Utilities - Renewable",),
    "aerospace_defense": ("Aerospace & Defense",),
    "gold": ("Gold",),
    "banks_regional": ("Banks - Regional",),
    "banks_diversified": ("Banks - Diversified",),
    "reit_office": ("REIT - Office",),
    "software_app": ("Software - Application",),
    "broadcasting": ("Broadcasting",),
    "lodging": ("Lodging",),
}

BUCKET_DESC_KEYWORDS: dict[str, tuple[str, ...]] = {
    "car_rental": ("rental car", "car rental", "vehicle rental", "avis", "hertz"),
    "airlines": ("airline", "passenger airline"),
    "solar": ("solar", "photovoltaic"),
}

# Prefer these liquid names when present — stops "every E&P on earth" dumps
BUCKET_PREFERRED: dict[str, tuple[str, ...]] = {
    "oil_ep": ("COP", "EOG", "OXY", "FANG", "DVN", "APA", "CTRA", "MRO", "PR"),
    "oil_integrated": ("XOM", "CVX"),  # US-listed majors only for default book
    "airlines": ("DAL", "UAL", "AAL", "LUV", "ALK"),
    "car_rental": ("CAR", "HTZ"),
    "semiconductors": ("NVDA", "AVGO", "AMD", "QCOM", "MU", "INTC", "TXN"),
    "semi_equipment": ("ASML", "AMAT", "LRCX", "KLAC", "AMKR"),
    "solar": ("FSLR", "ENPH", "SEDG", "RUN"),
    "utilities_renewable": ("NEE", "CEG", "VST", "GEV"),
    "aerospace_defense": ("LMT", "RTX", "NOC", "GD", "HII"),
    "banks_regional": ("PNC", "USB", "TFC", "CFG", "KEY", "RF"),
    "banks_diversified": ("JPM", "BAC", "WFC", "C"),
    "reit_office": ("BXP", "VNO", "SLG", "KRC"),
    "software_app": ("CRM", "NOW", "ADBE", "INTU", "WDAY"),
    "broadcasting": ("FOXA", "FOX", "NXST", "SBGI", "TGNA"),
    "gold": ("NEM", "GOLD", "AEM"),
    "lodging": ("MAR", "HLT", "H"),
    "trucking": ("ODFL", "XPO", "SAIA"),
    "railroads": ("UNP", "CSX", "NSC"),
    "marine_shipping": ("ZIM", "MATX", "SBLK"),
    "air_freight": ("FDX", "UPS"),
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
            Edge("lodging", "buy", 0.4, "stranded overnight — weaker"),
        ),
        amp_damp=(
            "Airport/airline labor disruption: [+] systemwide/multi-hub [−] single "
            "airport, resolved same day"
        ),
        mean_revert="strike ends / flights normalize",
    ),
    EventFamily(
        key="port_labor_disruption",
        taxonomy_labels=("Supply chain shock (factory fire, shipping disruption)",),
        base_weight=8,
        patterns=(
            r"port\s+(workers?\s+)?strike",
            r"longshore(men)?\s+strike",
            r"ILWU\s+strike",
            r"dockworkers?\s+(strike|walkout)",
        ),
        primary=(Edge("marine_shipping", "sell", 0.8, "throughput risk"),),
        substitute=(
            Edge("trucking", "buy", 0.7, "modal shift — conditional"),
            Edge("railroads", "buy", 0.6, "intermodal — conditional"),
            Edge("air_freight", "buy", 0.5, "expedite — conditional"),
        ),
        mean_revert="contract ratification",
    ),
    EventFamily(
        key="hormuz_energy_risk",
        taxonomy_labels=(
            "Geopolitical event that hurts sector (sanctions, conflict disrupting supply chain)",
            "Commodity price move favorable to the company",
        ),
        base_weight=8,
        patterns=(
            r"strait\s+of\s+hormuz",
            r"\bhormuz\b",
            r"iran.{0,40}(tanker|oil\s+export|blockade)",
            r"red\s+sea.{0,30}(attack|shipping|houthi)",
        ),
        primary=(
            Edge("oil_ep", "buy", 1.0, "oil risk premium"),
            Edge("oil_integrated", "buy", 0.7, "US integrated oil leverage"),
            Edge("airlines", "sell", 0.7, "jet fuel cost"),
        ),
        amp_damp=(
            "Hormuz risk: [+] transit threat + oil up [−] rhetoric only / oil flat. "
            "Do not expand to pure natural-gas E&P as oil proxies."
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
        ),
        primary=(
            Edge("semiconductors", "sell", 0.6, "cost/uncertainty"),
            Edge("semi_equipment", "sell", 0.5, "capex risk"),
            Edge("solar", "sell", 0.8, "import cost hit"),
        ),
        mean_revert="policy walk-back",
    ),
    # Fed: patterns are deliberately STRICT — gold blogs about "rate hike expectations"
    # must not drive a 85-name book. Polarity is resolved in news_actions.
    EventFamily(
        key="fed_rate_path",
        taxonomy_labels=(
            "Institutional policy (Fed rate cut, QE, stimulus)",
            "Rate hike/monetary tightening/liquidity withdrawal",
        ),
        base_weight=9,
        patterns=(
            r"\bFOMC\b",
            r"\bFed\b\s+(cuts?|hikes?|holds?)\s+(rates?|by)",
            r"federal\s+reserve\s+(cuts?|hikes?)",
            r"\bPowell\b.{0,20}(cut|hike|pause)",
            r"FedWatch.{0,30}(cut|hike)",
        ),
        primary=(
            # sides are defaults for HIKE; inverted on dovish in news_actions
            Edge("banks_diversified", "buy", 0.4, "curve — polarity adjusted"),
            Edge("reit_office", "sell", 0.5, "duration — polarity adjusted"),
            Edge("software_app", "sell", 0.4, "duration — polarity adjusted"),
            Edge("gold", "sell", 0.3, "real-rate — polarity adjusted"),
        ),
        amp_damp="Disambiguate CUT/DOVISH vs HIKE/HAWKISH before applying sides",
        mean_revert="after FOMC + 1 session",
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
        primary=(Edge("broadcasting", "buy", 0.8, "consolidation optionality"),),
        mean_revert="court challenge",
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
            Edge("utilities_renewable", "sell", 0.7, "US offshore pipeline"),
        ),
        mean_revert="project restart",
    ),
    EventFamily(
        key="defense_spending_surge",
        taxonomy_labels=(
            "Geopolitical event that boosts sector (e.g., defense spending surge)",
        ),
        base_weight=7,
        patterns=(
            r"defense\s+(budget|spending)\s+(increase|boost|surge|raise)",
            r"NATO\s+spending\s+(target|increase)",
        ),
        primary=(Edge("aerospace_defense", "buy", 1.0, "budget tailwind"),),
        mean_revert="CR / budget freeze",
    ),
]


def all_families() -> list[EventFamily]:
    return list(EVENT_FAMILIES)
