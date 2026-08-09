"""Event families + primary/substitute edges + reasoning templates."""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Edge:
    bucket: str
    side: str
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
    # Framework fields for the reasoning block
    mechanism: str = ""
    channel: str = ""  # risk | rates | sector_policy | sector_fundamental | substitution
    horizon: str = "1d-1w"
    us_equity_default: bool = True
    # Optional bridge from news_parse macro/sector theme tags
    parse_themes: tuple[str, ...] = ()


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
    "utilities_power": ("Utilities - Independent Power Producers",
                        "Utilities - Diversified",
                        "Utilities - Regulated Electric"),
    "aerospace_defense": ("Aerospace & Defense",),
    "gold": ("Gold",),
    "banks_regional": ("Banks - Regional",),
    "banks_diversified": ("Banks - Diversified",),
    "reit_office": ("REIT - Office",),
    "software_app": ("Software - Application",),
    "software_infra": ("Software - Infrastructure",),
    "broadcasting": ("Broadcasting",),
    "lodging": ("Lodging",),
}

BUCKET_DESC_KEYWORDS: dict[str, tuple[str, ...]] = {
    "car_rental": ("rental car", "car rental", "avis", "hertz"),
    "airlines": ("airline", "passenger airline"),
    "solar": ("solar", "photovoltaic"),
}

BUCKET_PREFERRED: dict[str, tuple[str, ...]] = {
    "oil_ep": ("COP", "EOG", "OXY", "FANG", "DVN", "APA"),
    "oil_integrated": ("XOM", "CVX"),
    "airlines": ("DAL", "UAL", "AAL", "LUV", "ALK"),
    "car_rental": ("CAR", "HTZ"),
    "semiconductors": ("NVDA", "AVGO", "AMD", "QCOM", "MU", "INTC"),
    "semi_equipment": ("ASML", "AMAT", "LRCX", "KLAC"),
    "solar": ("FSLR", "ENPH", "SEDG", "RUN"),
    "utilities_renewable": ("NEE", "CEG", "VST"),
    "utilities_power": ("CEG", "VST", "NRG", "TLN", "NEE"),
    "aerospace_defense": ("LMT", "RTX", "NOC", "GD"),
    "banks_regional": ("PNC", "USB", "TFC", "CFG", "KEY"),
    "banks_diversified": ("JPM", "BAC", "WFC", "C"),
    "reit_office": ("BXP", "VNO", "SLG"),
    "software_app": ("CRM", "NOW", "ADBE", "WDAY", "TEAM", "SNOW"),
    "software_infra": ("MSFT", "ORCL", "PANW", "CRWD"),
    "broadcasting": ("FOXA", "NXST", "TGNA"),
    "gold": ("NEM", "GOLD", "AEM"),
    "lodging": ("MAR", "HLT"),
    "trucking": ("ODFL", "XPO"),
    "railroads": ("UNP", "CSX", "NSC"),
    "marine_shipping": ("ZIM", "MATX"),
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
            r"TSA\s+call[- ]?outs",
            r"ATC\s+(staffing|shortage|delay)",
        ),
        primary=(
            Edge("airlines", "sell", 1.0, "traffic disruption + cost"),
            Edge("airports", "sell", 0.5, "ops"),
        ),
        substitute=(
            Edge("car_rental", "buy", 1.2, "forced ground substitute (CAR pattern)"),
            Edge("lodging", "buy", 0.4, "stranded overnight"),
        ),
        mechanism=(
            "Air node blocked → passenger demand does not vanish → airlines lose volume; "
            "substitutes (rental cars, sometimes hotels) gain short-term scarcity pricing."
        ),
        channel="substitution",
        horizon="1d-1w",
        amp_damp="[+] multi-hub / peak travel [−] single airport, same-day resolve",
        mean_revert="strike ends / schedule normalizes",
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
        primary=(Edge("marine_shipping", "sell", 0.8, "throughput"),),
        substitute=(
            Edge("trucking", "buy", 0.6, "modal shift"),
            Edge("railroads", "buy", 0.5, "intermodal"),
            Edge("air_freight", "buy", 0.5, "expedite"),
        ),
        mechanism="Port closed → ocean throughput −; inland/air freight may pick residual volume.",
        channel="substitution",
        horizon="1w",
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
            Edge("oil_ep", "buy", 1.0, "oil risk premium → producer cash flow leverage"),
            Edge("oil_integrated", "buy", 0.7, "US integrated oil"),
            Edge("airlines", "sell", 0.7, "jet fuel is a major cost; disruption raises fuel"),
        ),
        mechanism=(
            "Hormuz/Red Sea transit risk → oil risk premium → upstream producers benefit if "
            "price holds; airlines and other fuel-intensive transport are cost-shocked. "
            "Geography is global oil, but US E&P still price to WTI/Brent. Not a pure-gas trade."
        ),
        channel="risk",
        horizon="1d-1w",
        amp_damp="[+] real transit threat + oil up [−] rhetoric only, oil unchanged",
        mean_revert="de-escalation / oil giveback",
        parse_themes=("geopolitics",),
    ),
    EventFamily(
        key="tariff_semis_solar",
        taxonomy_labels=(
            "Government policy (tariffs, subsidies, mandates)",
            "Policy reversal/new regulation/tax increase",
        ),
        base_weight=7,
        patterns=(
            r"tariff.{0,50}(semiconductor|chip|solar|photovoltaic)",
            r"(semiconductor|chip|solar|photovoltaic).{0,50}tariff",
            r"solar\s+tariffs",
        ),
        primary=(
            Edge("solar", "sell", 0.9, "import-heavy project economics / module cost"),
            Edge("semiconductors", "sell", 0.5, "cost & policy uncertainty — less direct if domestic"),
            Edge("semi_equipment", "sell", 0.4, "capex hesitation risk"),
        ),
        mechanism=(
            "US tariff on solar/semi inputs raises cost or uncertainty for import-linked supply "
            "chains. Solar project IRRs and some module names are first-order; mega-cap semis "
            "are second-order unless the tariff text is chip-specific. Domestic producers can "
            "be relative winners — edge defaults are cautious sells pending issuer mix."
        ),
        channel="sector_policy",
        horizon="1w-1m",
        mean_revert="walk-back / exclusions / phase-in delay",
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
            r"\bFed\b\s+(cuts?|hikes?|holds?)\s+(rates?|by)",
            r"federal\s+reserve\s+(cuts?|hikes?)",
            r"\bPowell\b.{0,20}(cut|hike|pause)",
            r"rate\s+(cut|hike)\s+(odds|probability|expectations)",
            r"cooling\s+(fed\s+)?rate\s+hike",
            r"rate\s+futures?.{0,40}(cut|hike)",
            r"jobs?\s+data.{0,40}(rate|fed|hike|cut)",
            r"after\s+jobs\s+data.{0,30}(hike|cut|fed)",
        ),
        primary=(
            Edge("banks_diversified", "buy", 0.35, "curve/NIM — polarity adjusted"),
            Edge("reit_office", "sell", 0.45, "duration — polarity adjusted"),
            Edge("software_app", "sell", 0.35, "duration growth — polarity adjusted"),
            Edge("gold", "sell", 0.35, "real rate — polarity adjusted"),
        ),
        mechanism=(
            "Discount-rate channel: hawkish (hikes / higher-for-longer) pressures long-duration "
            "growth and rate-sensitive REITs, mixed for banks via curve; dovish (cuts / cooling "
            "hike odds) inverts those duration sides and often supports gold. Labor prints move "
            "Fed path expectations without being a ‘jobs ETF’ trade."
        ),
        channel="rates",
        horizon="1d-1w",
        amp_damp="Sides flip on dovish vs hawkish; unknown polarity → no hard book",
        mean_revert="after FOMC / next print",
        parse_themes=("fed_path", "rates", "labor"),
    ),
    EventFamily(
        key="weak_labor_print",
        taxonomy_labels=(
            "Macro headwinds (inflation spike, recession, unemployment surge)",
            "Institutional policy (Fed rate cut, QE, stimulus)",
        ),
        base_weight=7,
        patterns=(
            r"weak\s+jobs?\s+report",
            r"non-?farm\s+payrolls?\s+(drop|miss|disappoint|fall)",
            r"payrolls?\s+(drop|disappoint|miss)",
            r"jobs?\s+report\s+clouds",
            r"unemployment\s+(rises|jumped|unexpected)",
        ),
        primary=(
            Edge("banks_diversified", "sell", 0.3, "growth scare / credit cycle fear"),
            Edge("software_app", "buy", 0.25, "if rates path dovish dominates — soft"),
            Edge("gold", "buy", 0.4, "growth scare + cut odds"),
        ),
        mechanism=(
            "Weak US labor print → growth scare + higher cut odds. Near-term: risk assets can "
            "wobble while duration/gold catch a bid if the Fed path dominates. Not automatic "
            "risk-off for every name — check whether rates or recession narrative wins the day."
        ),
        channel="rates",
        horizon="1d-1w",
        mean_revert="next labor/CPI or Fed speak",
        parse_themes=("labor",),
    ),
    EventFamily(
        key="saas_multiple_compression",
        taxonomy_labels=(
            "Sector headwind/index exclusion/rotation away",
            "Sector rotation out of the industry",
        ),
        base_weight=6,
        patterns=(
            r"saas\s*pocalypse",
            r"saas(pocalypse)?",
            r"software\s+stocks\s+(swing|selloff|slump|plunge)",
            r"software\s+multiple\s+compression",
            r"growth\s+software\s+selloff",
        ),
        primary=(
            Edge("software_app", "sell", 1.0, "multiple/narrative compression"),
        ),
        mechanism=(
            "‘SaaSpocalypse’ / software selloff is a sector-narrative + duration channel: "
            "high-multiple application software re-rates when growth/AI monetization or rates "
            "shift. This is not single-ticker earnings — it is basket risk for crowded software."
        ),
        channel="sector_fundamental",
        horizon="1d-1w",
        mean_revert="stabilization in relative performance vs XLK/SPY",
    ),
    EventFamily(
        key="ai_power_demand",
        taxonomy_labels=(
            "Sector tailwind/index inclusion",
            "Capacity expansion announced (new factory, satellite constellation)",
        ),
        base_weight=6,
        patterns=(
            r"data\s*center.{0,40}(power|electric|grid|pollut)",
            r"(power|electric).{0,40}data\s*center",
            r"AI.{0,20}(power|electricity|energy\s+demand)",
        ),
        primary=(
            Edge("utilities_power", "buy", 0.9, "load growth / power scarcity narrative"),
        ),
        mechanism=(
            "Hyperscale/AI data centers raise electricity load expectations → independent power "
            "and some utilities with generation leverage are the listed US expression. Headline "
            "pollution angle is secondary; the equity channel is power demand and capacity."
        ),
        channel="sector_fundamental",
        horizon="1w-1m",
        mean_revert="if projects delayed / power deals disappoint",
        parse_themes=(),  # often in single_name bucket today
    ),
    EventFamily(
        key="ai_chip_demand_spike",
        taxonomy_labels=(
            "Sector tailwind/index inclusion",
            "Operational milestone (e.g., first patient dosed, satellite commissioned)",
        ),
        base_weight=6,
        patterns=(
            r"SpaceX.{0,40}(Nvidia|chips?|GPU)",
            r"Nvidia.{0,40}(SpaceX|surge|largest\s+weekly)",
            r"chip\s+equipment.{0,30}(SpaceX|Terafab|AI)",
        ),
        primary=(
            Edge("semiconductors", "buy", 0.7, "incremental AI/demand narrative"),
            Edge("semi_equipment", "buy", 0.5, "fab/tooling second order"),
        ),
        mechanism=(
            "Named AI/hyperscale demand (e.g. SpaceX chip commit) supports semi and equipment "
            "risk appetite. Differs from tariff shock: this is demand +, not cost +. Still often "
            "priced fast in NVDA — equipment/secondaries may lag."
        ),
        channel="sector_fundamental",
        horizon="1d-1w",
        mean_revert="fade if no follow-through orders",
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
        mechanism="FCC ownership relief → M&A optionality for broadcast groups.",
        channel="sector_policy",
        horizon="1w-1m",
        mean_revert="legal challenge",
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
        primary=(Edge("utilities_renewable", "sell", 0.7, "US offshore pipeline"),),
        mechanism="Project cancel/payout → negative signal for US offshore wind economics/policy.",
        channel="sector_policy",
        horizon="1w-1m",
        mean_revert="restart / new subsidy",
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
        primary=(Edge("aerospace_defense", "buy", 1.0, "budget/contract tailwind"),),
        mechanism="Higher defense outlays → backlog/earnings path for primes.",
        channel="sector_policy",
        horizon="1w-1m",
        mean_revert="budget freeze",
    ),
    EventFamily(
        key="regional_bank_ai_lending",
        taxonomy_labels=(
            "Sector tailwind/index inclusion",
            "Earnings guidance raise",
        ),
        base_weight=5,
        patterns=(
            r"regional\s+banks?.{0,40}(lending|loan|AI)",
            r"AI\s+boom.{0,40}regional\s+banks?",
            r"regional\s+banks?.{0,30}lending\s+picks\s+up",
        ),
        primary=(Edge("banks_regional", "buy", 0.8, "loan growth narrative"),),
        mechanism=(
            "Lending pickup at regionals is a fundamental channel (NII volume), not a Fed path "
            "trade. Confirm with rates regime — weak if credit stress headlines dominate."
        ),
        channel="sector_fundamental",
        horizon="1w",
        mean_revert="credit quality scare",
        parse_themes=(),
    ),
]


def all_families() -> list[EventFamily]:
    return list(EVENT_FAMILIES)
