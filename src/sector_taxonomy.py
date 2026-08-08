"""Sector factor taxonomy — machine shape, mirror of catalyst TAXONOMY_LIST / WEIGHTS.

Not prose. Each label has: polarity (+/-), weight 1–10, search phrases, amp/damp.
SHARED applies to every sector. Per-sector lists are v1 mandatory checklists
(top weights); extended labels can be promoted later via lessons.

Scoring (parallel to catalyst net):
  Sector_Net = Σ (adj_w × conf) for + HITs − Σ (adj_w × conf) for − HITs
  → Strong Lead / Lead / Neutral / Lag / Strong Lag
  Horizons: 1d, 3d, 1w, 1m (same HIT grid; horizon is outcome window).
"""
from __future__ import annotations

from typing import Any

# Finviz 11 sectors (exact display names used in Finviz)
FINVIZ_SECTORS = [
    "Basic Materials",
    "Communication Services",
    "Consumer Cyclical",
    "Consumer Defensive",
    "Energy",
    "Financial",
    "Healthcare",
    "Industrials",
    "Real Estate",
    "Technology",
    "Utilities",
]

# Representative sector ETFs for deterministic relative-performance channel
SECTOR_ETFS = {
    "Basic Materials": "XLB",
    "Communication Services": "XLC",
    "Consumer Cyclical": "XLY",
    "Consumer Defensive": "XLP",
    "Energy": "XLE",
    "Financial": "XLF",
    "Healthcare": "XLV",
    "Industrials": "XLI",
    "Real Estate": "XLRE",
    "Technology": "XLK",
    "Utilities": "XLU",
}

HORIZONS = ("1d", "3d", "1w", "1m")

# Net → label thresholds (same spirit as catalyst Strong Bullish bands)
NET_BANDS = [
    (20, "Strong Lead"),
    (8, "Lead"),
    (-8, "Neutral"),
    (-20, "Lag"),
    (None, "Strong Lag"),
]


def net_to_label(net: float) -> str:
    if net >= 20:
        return "Strong Lead"
    if net >= 8:
        return "Lead"
    if net >= -8:
        return "Neutral"
    if net >= -20:
        return "Lag"
    return "Strong Lag"


def _L(label: str, polarity: str, weight: int, searches: list[str], amp_damp: str,
       mandatory: bool = True) -> dict[str, Any]:
    return {
        "label": label,
        "polarity": polarity,  # "+" or "-"
        "weight": weight,
        "searches": searches,
        "amp_damp": amp_damp,
        "mandatory": mandatory,
    }


# ── SHARED (every sector) ───────────────────────────────────────────────
SHARED: list[dict] = [
    _L("Risk-on tape / equity beta expansion", "+", 7,
       ["risk on equity market breadth", "high beta leadership stocks"],
       "[+] cyclicals/tech/financials [−] defensives if pure flight-from-safety"),
    _L("Risk-off tape / flight to safety", "-", 8,
       ["risk off flight to safety stocks", "defensive rotation equities"],
       "[+] utilities/staples/REITs as relative winners [−] cyclicals; polarity "
       "for cyclicals is negative, for defensives treat as regime context"),
    _L("Real yields rising", "-", 8,
       ["real yields rising TIPS", "10 year real yield technology duration"],
       "[+] some banks/floaters [−] duration/growth/tech/REITs"),
    _L("Real yields falling", "+", 8,
       ["real yields falling", "TIPS real yield decline growth stocks"],
       "[+] duration/growth [−] if driven by recession scare without easing"),
    _L("USD strengthening", "-", 6,
       ["US dollar index surge DXY", "strong dollar commodity exporters"],
       "[+] importers [−] commodity producers / EM-linked / exporters"),
    _L("USD weakening", "+", 6,
       ["US dollar weakness DXY", "weak dollar commodities"],
       "[+] commodities / exporters [−] pure domestic if no other catalyst"),
    _L("Sector breadth expansion (% names up)", "+", 7,
       ["sector breadth advance decline", "equal weight vs cap weight sector"],
       "[+] healthy leadership [−] if only 1–2 mega names carry ETF"),
    _L("Sector breadth failure (ETF up, names flat)", "-", 7,
       ["sector ETF up equal weight lag", "narrow leadership sector"],
       "[+] fade extension [−] if mega-cap IS the sector thesis (e.g. Mag7)"),
    _L("Large-cap leadership inside sector", "+", 4,
       ["large cap leadership sector rotation"],
       "context: quality bid; not always bullish for small-cap basket"),
    _L("Small/mid leadership inside sector", "+", 4,
       ["small mid cap leadership sector"],
       "context: risk appetite inside sector"),
    _L("High-beta leadership inside sector", "+", 5,
       ["high beta stocks outperforming sector"],
       "[+] risk-on confirmation [−] late-cycle chase"),
    _L("Low-beta leadership inside sector", "+", 5,
       ["low beta defensive leadership sector"],
       "[+] defensive regime [−] for cyclical sector scores"),
    _L("Sector ETF inflow / relative volume spike", "+", 5,
       ["sector ETF flows volume spike"],
       "[+] attention/positioning [−] already extended on flows"),
    _L("Sector ETF outflow / volume dry-up", "-", 5,
       ["sector ETF outflows", "sector relative volume dry up"],
       "[+] washout setup later [−] near-term demand"),
    _L("Crowded long (extreme relative performance + valuation)", "-", 6,
       ["sector crowded long valuation extreme relative performance"],
       "[+] mean-reversion risk [−] structural multi-year compounder exceptions"),
    _L("Index rebalance / inclusion tailwind", "+", 4,
       ["index rebalance sector inclusion forced buying"],
       "[+] mechanical demand [−] one-day noise"),
    _L("Index exclusion / forced selling", "-", 5,
       ["index exclusion forced selling sector"],
       "[+] temporary [−] if fundamental too"),
]

# ── Per-sector mandatory checklists (v1) ─────────────────────────────────
SECTORS: dict[str, list[dict]] = {
    "Basic Materials": [
        _L("Industrial metal price surge (copper/aluminum/iron ore)", "+", 9,
           ["copper price surge inventory LME", "iron ore steel margin 2026"],
           "[+] unhedged producers, high commodity beta [−] processors with lag"),
        _L("Gold/silver price surge (monetary metals)", "+", 8,
           ["gold silver ETF flows price surge"],
           "[+] miners with leverage [−] already priced parabolic tape"),
        _L("China PMI / property demand rebound", "+", 9,
           ["China PMI manufacturing metals demand", "China property metals"],
           "[+] confirmed credit/property headlines [−] one soft print"),
        _L("Inventory draw (LME/exchange stocks down)", "+", 7,
           ["LME copper inventory draw"],
           "[+] tight market [−] seasonal noise"),
        _L("Supply disruption (mine/export ban)", "+", 8,
           ["mine strike copper supply disruption export ban"],
           "[+] lasting outage [−] quickly resolved"),
        _L("Critical-minerals policy / domestic tariff support", "+", 7,
           ["critical minerals tariff policy domestic"],
           "[+] domestic producers [−] cost hit for processors"),
        _L("Industrial metal price collapse", "-", 9,
           ["copper price collapse", "iron ore price crash"],
           "[+] hedged [−] unhedged E&P-style miners"),
        _L("China demand shock / property stress", "-", 9,
           ["China property stress metals demand"],
           "sector spine negative when confirmed"),
        _L("USD spike vs commodity complex", "-", 7,
           ["strong dollar commodities selloff"],
           "amplifies price collapse HITs"),
        _L("Supply glut / new capacity online", "-", 7,
           ["copper supply glut new mine capacity"],
           "[+] demand still stronger [−] margin compression"),
        _L("Margin compression / cost inflation without pricing power", "-", 7,
           ["mining margin compression cost inflation"],
           "dampen if prices rising faster than costs"),
        _L("Sector rotation into materials", "+", 6,
           ["sector rotation basic materials XLB flows"],
           "confirm with breadth"),
        _L("Sector rotation out of materials", "-", 6,
           ["sector rotation out materials XLB"],
           "confirm with breadth failure"),
    ],
    "Communication Services": [
        _L("Digital ad spend recovery / upside commentary", "+", 8,
           ["digital advertising spend outlook", "Meta Google ad revenue guidance"],
           "[+] pure ad platforms [−] diversified media"),
        _L("AI product monetization proof (ads/cloud attach)", "+", 8,
           ["AI advertising monetization Meta Google"],
           "need proof not narrative"),
        _L("Platform engagement / MAU acceleration", "+", 7,
           ["platform MAU engagement growth"],
           "[+] when monetization follows [−] empty engagement"),
        _L("Antitrust relief / favorable ruling", "+", 8,
           ["antitrust big tech ruling favorable"],
           "[+] structural remedy avoided [−] already priced multi-year case"),
        _L("Telecom ARPU / subscriber beat", "+", 6,
           ["wireless ARPU churn subscriber"],
           "telecom sub-industry"),
        _L("Ad budget cut / digital ad recession signal", "-", 8,
           ["digital ad recession brand spend cut"],
           "sector spine negative"),
        _L("Regulatory crackdown (antitrust, app store, content)", "-", 9,
           ["app store fee regulation antitrust"],
           "amplify structural remedy risk"),
        _L("Engagement deceleration / platform fatigue", "-", 7,
           ["social platform engagement decline"],
           ""),
        _L("Telecom price war / churn spike", "-", 7,
           ["wireless price war churn"],
           ""),
        _L("Sector rotation into communication services", "+", 5,
           ["XLC sector rotation flows"], ""),
        _L("Sector rotation out of communication services", "-", 6,
           ["XLC sector rotation out"], ""),
    ],
    "Consumer Cyclical": [
        _L("Retail sales / card spend upside", "+", 8,
           ["US retail sales control group", "credit card spend consumer"],
           "coincident with confidence amplifies"),
        _L("Consumer confidence jump", "+", 7,
           ["consumer confidence Conference Board"],
           "amplify when spend data confirms"),
        _L("Employment / wage support for discretionary", "+", 7,
           ["wage growth employment consumer discretionary"],
           ""),
        _L("Credit conditions easing for consumers", "+", 8,
           ["consumer credit conditions easing"],
           ""),
        _L("Auto SAAR / dealer inventory healthy", "+", 7,
           ["auto SAAR inventory"],
           ""),
        _L("Travel / hotel RevPAR beat", "+", 7,
           ["hotel RevPAR travel demand"],
           ""),
        _L("Retail miss / traffic down", "-", 8,
           ["retail sales miss traffic decline"],
           ""),
        _L("Consumer confidence collapse", "-", 8,
           ["consumer confidence plunge"],
           ""),
        _L("Jobless claims / unemployment spike", "-", 9,
           ["jobless claims unemployment spike"],
           "hard kill for discretionary"),
        _L("Credit tightening / delinquency rise", "-", 9,
           ["credit card delinquency rate auto"],
           "[+] luxury cash buyers dampen [−] subprime retail"),
        _L("Gasoline spike crushing discretionary", "-", 6,
           ["gasoline prices consumer spending"],
           ""),
        _L("Sector rotation into discretionary", "+", 6,
           ["XLY sector rotation"], ""),
        _L("Sector rotation out of discretionary", "-", 6,
           ["XLY sector rotation out"], ""),
    ],
    "Consumer Defensive": [
        _L("Flight-to-safety relative strength vs cyclicals", "+", 8,
           ["defensive rotation equity market staples"],
           "PRIMARY regime signal; amp when cyclicals breadth fails; "
           "dampen if only 1–2 mega names"),
        _L("Input cost relief (ag, packaging, freight)", "+", 8,
           ["food packaging freight costs producers"],
           ""),
        _L("Pricing power held without volume collapse", "+", 7,
           ["consumer staples volume pricing power"],
           ""),
        _L("Volume stabilization / sequential improvement", "+", 7,
           ["staples volume sequential improvement"],
           ""),
        _L("Staples earnings beat stable margins", "+", 6,
           ["consumer staples earnings margins"],
           ""),
        _L("Volume decline accelerating", "-", 8,
           ["staples volume decline"],
           ""),
        _L("Elasticity break (price up, volume down hard)", "-", 8,
           ["staples price elasticity volume"],
           ""),
        _L("Input cost spike without pricing power", "-", 8,
           ["food CPI input costs producers"],
           ""),
        _L("Risk-on rotation away from defensives", "-", 7,
           ["risk on rotation away staples XLP"],
           ""),
        _L("Private-label share gain against brands", "-", 6,
           ["private label share food brands"],
           ""),
        _L("Sector rotation into defensives", "+", 6,
           ["XLP defensive sector flows"], ""),
        _L("Sector rotation out of defensives", "-", 6,
           ["XLP sector rotation out"], ""),
    ],
    "Energy": [
        _L("Crude oil price surge (WTI/Brent)", "+", 9,
           ["WTI Brent crude price surge"],
           "[+] unhedged E&P [−] refiners if cracks collapse; fully hedged"),
        _L("Natural gas price surge", "+", 8,
           ["Henry Hub natural gas price"],
           "[+] pure gas [−] pure oil names N/A"),
        _L("Inventory draw (EIA crude/products)", "+", 8,
           ["EIA crude inventory draw"],
           ""),
        _L("OPEC+ cut / supply discipline", "+", 9,
           ["OPEC+ production decision cut"],
           "[+] credible compliance, low spare capacity [−] cheating expected"),
        _L("Crack spread / refining margin expansion", "+", 7,
           ["crack spread refining margin"],
           "refiner sub-industry"),
        _L("Geopolitical supply risk premium", "+", 8,
           ["geopolitical oil supply risk"],
           "temporary premium fades fast"),
        _L("Crude price collapse", "-", 9,
           ["crude oil price collapse"],
           ""),
        _L("OPEC+ production increase / quota break", "-", 9,
           ["OPEC+ production increase"],
           ""),
        _L("Demand destruction (recession/China weak)", "-", 8,
           ["oil demand destruction China recession"],
           ""),
        _L("Inventory build", "-", 8,
           ["EIA crude inventory build"],
           ""),
        _L("Crack spread collapse", "-", 7,
           ["refining crack spread collapse"],
           ""),
        _L("Sector rotation into energy", "+", 6,
           ["XLE sector rotation flows"], ""),
        _L("Sector rotation out of energy", "-", 6,
           ["XLE sector rotation out"], ""),
    ],
    "Financial": [
        _L("Yield curve steepening (NIM tailwind)", "+", 9,
           ["US yield curve 2s10s steepening"],
           "[+] money-center banks [−] already priced"),
        _L("Credit spreads tightening", "+", 8,
           ["HY IG credit spreads tightening"],
           ""),
        _L("Bank NII / NIM beat", "+", 8,
           ["bank net interest margin beat"],
           ""),
        _L("Credit quality stable or improving", "+", 8,
           ["bank credit quality charge-offs"],
           ""),
        _L("Regional bank stress easing", "+", 8,
           ["regional bank stress easing"],
           ""),
        _L("Capital markets / IB / trading surge", "+", 7,
           ["investment banking trading revenue banks"],
           ""),
        _L("Credit spreads blowing out", "-", 9,
           ["HY credit spreads blow out"],
           "hard risk-off for financials"),
        _L("Charge-off / delinquency spike", "-", 9,
           ["credit card delinquency banks charge-off"],
           ""),
        _L("CRE concentration stress", "-", 9,
           ["commercial real estate bank exposure"],
           "[+] regionals with office books"),
        _L("Deposit flight / funding stress", "-", 9,
           ["bank deposit flight funding stress"],
           ""),
        _L("Yield curve inversion / flattening hurting NIM", "-", 8,
           ["yield curve inversion bank NIM"],
           ""),
        _L("Sector rotation into financials", "+", 6,
           ["XLF sector rotation"], ""),
        _L("Sector rotation out of financials", "-", 6,
           ["XLF sector rotation out"], ""),
    ],
    "Healthcare": [
        _L("FDA approval / favorable panel (sector breadth)", "+", 9,
           ["FDA approval panel biotech"],
           "amplify only when biotech breadth or policy-wide; "
           "single-ticker FDA must NOT dominate sector Net alone"),
        _L("Positive late-stage trial readout (breadth)", "+", 9,
           ["Phase 3 trial readout biotech sector"],
           "same breadth rule"),
        _L("CMS / Medicare Advantage rate upside", "+", 8,
           ["CMS Medicare Advantage rates"],
           "managed care / providers"),
        _L("Biotech risk-on / XBI leadership", "+", 7,
           ["biotech ETF flows XBI leadership"],
           ""),
        _L("Drug pricing policy relief", "+", 8,
           ["IRA drug pricing relief"],
           ""),
        _L("FDA rejection / CRL / trial failure (breadth)", "-", 9,
           ["FDA CRL rejection biotech"],
           "sector-relevant only if cluster"),
        _L("Medicare rate cut / reimbursement pressure", "-", 8,
           ["Medicare rate cut reimbursement"],
           ""),
        _L("Drug pricing crackdown / IRA expansion risk", "-", 8,
           ["IRA drug pricing expansion"],
           ""),
        _L("Biotech risk-off / funding winter", "-", 7,
           ["biotech funding winter risk off"],
           ""),
        _L("Utilization spike hurting insurers", "-", 8,
           ["medical utilization spike insurers"],
           ""),
        _L("Sector rotation into healthcare", "+", 5,
           ["XLV sector rotation"], ""),
        _L("Sector rotation out of healthcare", "-", 6,
           ["XLV sector rotation out"], ""),
    ],
    "Industrials": [
        _L("ISM manufacturing / new orders expansion", "+", 9,
           ["ISM manufacturing PMI new orders"],
           "SECTOR SPINE — amp machinery/transports"),
        _L("Durable goods / CapEx upside", "+", 8,
           ["durable goods orders CapEx"],
           ""),
        _L("Grid / electrical equipment backlog (AI power)", "+", 8,
           ["electrical equipment data center backlog"],
           "semi-independent of classic ISM"),
        _L("Aerospace & defense order / budget upside", "+", 8,
           ["defense budget appropriations orders"],
           "do not cancel ISM weakness with one award"),
        _L("Freight / trucking / rail volume recovery", "+", 7,
           ["trucking freight rates volume"],
           ""),
        _L("Reshoring / industrial policy funding", "+", 7,
           ["reshoring industrial policy funding"],
           ""),
        _L("ISM contraction", "-", 9,
           ["ISM manufacturing contraction"],
           "spine negative"),
        _L("CapEx cuts / order cancellation", "-", 8,
           ["CapEx cuts order cancellations industrials"],
           ""),
        _L("Freight recession", "-", 7,
           ["freight recession trucking"],
           ""),
        _L("Construction slowdown", "-", 7,
           ["nonresidential construction spending slowdown"],
           ""),
        _L("Sector rotation into industrials", "+", 6,
           ["XLI sector rotation"], ""),
        _L("Sector rotation out of industrials", "-", 6,
           ["XLI sector rotation out"], ""),
    ],
    "Real Estate": [
        _L("Rates falling / REIT duration relief", "+", 9,
           ["10 year yield REIT", "falling yields REITs"],
           "DOMINATES short horizon for all REITs"),
        _L("Data-center REIT demand / rent upside", "+", 8,
           ["data center REIT occupancy rent"],
           "property-type dispersion 1w–1m"),
        _L("Industrial REIT occupancy / rent growth", "+", 7,
           ["industrial warehouse rent growth"],
           ""),
        _L("Refinancing window opening", "+", 7,
           ["REIT refinancing window"],
           ""),
        _L("Cap-rate compression", "+", 7,
           ["cap rates commercial real estate compression"],
           ""),
        _L("Rates rising / REIT selloff", "-", 9,
           ["rising yields REIT selloff"],
           "spine negative short horizon"),
        _L("Office vacancy / mark-to-market stress", "-", 9,
           ["office vacancy rates US REIT"],
           "office sub-type"),
        _L("Refinancing wall stress", "-", 8,
           ["REIT refinancing wall"],
           ""),
        _L("Cap-rate expansion", "-", 7,
           ["cap rate expansion commercial"],
           ""),
        _L("Sector rotation into REITs", "+", 6,
           ["XLRE sector rotation"], ""),
        _L("Sector rotation out of real estate", "-", 6,
           ["XLRE sector rotation out"], ""),
    ],
    "Technology": [
        _L("Hyperscaler CapEx raise / AI infra spend upside", "+", 9,
           ["hyperscaler CapEx guidance Microsoft Amazon Google Meta"],
           "[+] semis/hardware [−] pure software until consumption confirms"),
        _L("Semiconductor demand / foundry utilization up", "+", 9,
           ["foundry utilization TSMC", "semiconductor demand"],
           ""),
        _L("HBM / advanced packaging shortage pricing power", "+", 8,
           ["HBM supply demand shortage"],
           ""),
        _L("Cloud consumption growth acceleration", "+", 8,
           ["cloud revenue growth Azure AWS"],
           ""),
        _L("Real yields falling (duration tailwind)", "+", 7,
           ["real yields technology valuations"],
           "cross-cutting duration"),
        _L("Hyperscaler CapEx cut / AI spend peak narrative", "-", 9,
           ["hyperscaler CapEx cut AI spend peak"],
           "hard sector kill"),
        _L("Semi downturn / inventory correction", "-", 9,
           ["semiconductor inventory correction downturn"],
           ""),
        _L("Cloud growth deceleration", "-", 8,
           ["cloud growth deceleration Azure AWS"],
           ""),
        _L("Export controls tightening", "-", 8,
           ["chip export controls China"],
           ""),
        _L("Real yields rising", "-", 8,
           ["real yields rising technology"],
           ""),
        _L("Software multiple compression / growth scare", "-", 7,
           ["software multiple compression growth scare"],
           ""),
        _L("Sector rotation into technology", "+", 6,
           ["XLK sector rotation"], ""),
        _L("Sector rotation out of technology", "-", 6,
           ["XLK sector rotation out"], ""),
    ],
    "Utilities": [
        _L("Data-center load growth / power demand upside", "+", 9,
           ["data center electricity demand utilities"],
           "NEW regime: can override mild rate moves when AI power active"),
        _L("Rates falling (bond-proxy bid)", "+", 8,
           ["10 year yield utilities performance"],
           "classic regime"),
        _L("Favorable rate case / allowed ROE", "+", 8,
           ["utility rate case decision ROE"],
           ""),
        _L("Nuclear / gas generation policy support", "+", 7,
           ["nuclear power policy SMR utilities"],
           ""),
        _L("Grid CapEx approval / recovery", "+", 7,
           ["grid interconnection queue CapEx utilities"],
           ""),
        _L("Rates rising (bond-proxy selloff)", "-", 8,
           ["rising yields utilities selloff"],
           "dampen if load-growth narrative dominates"),
        _L("Adverse rate case", "-", 8,
           ["utility adverse rate case"],
           ""),
        _L("Load growth disappointment", "-", 7,
           ["utility load growth disappointment"],
           ""),
        _L("Regulatory disallowance / project cancel", "-", 7,
           ["utility project cancel disallowance"],
           ""),
        _L("Risk-on rotation away from utilities", "-", 7,
           ["risk on rotation away utilities XLU"],
           ""),
        _L("Sector rotation into utilities", "+", 6,
           ["XLU sector rotation"], ""),
        _L("Sector rotation out of utilities", "-", 6,
           ["XLU sector rotation out"], ""),
    ],
}


def all_labels(sector: str) -> list[dict]:
    """SHARED + sector-specific mandatory labels."""
    return list(SHARED) + list(SECTORS.get(sector, []))


def taxonomy_list(sector: str) -> list[str]:
    return [x["label"] for x in all_labels(sector)]


def weights_map(sector: str) -> dict[str, int]:
    return {x["label"]: x["weight"] for x in all_labels(sector)}


def polarity_map(sector: str) -> dict[str, str]:
    return {x["label"]: x["polarity"] for x in all_labels(sector)}


def amp_damp_table(sector: str) -> str:
    lines = []
    for x in all_labels(sector):
        lines.append(f"{x['label']}: {x['amp_damp']}")
    return "\n".join(lines)


def make_search_templates(sector: str, year: str = "2026") -> list[str]:
    """Concrete queries — parallel to _make_catalyst_templates."""
    qs: list[str] = []
    for x in all_labels(sector):
        for phrase in x["searches"]:
            qs.append(f"{phrase} {year}")
            qs.append(f"{sector} {phrase}")
    qs.append(f"{sector} sector rotation institutional flows {year}")
    qs.append(f"{sector} earnings guidance aggregate {year}")
    qs.append(f"{sector} ETF relative performance breadth {year}")
    etf = SECTOR_ETFS.get(sector)
    if etf:
        qs.append(f"{etf} ETF performance flows {year}")
    # dedupe preserve order
    seen = set()
    out = []
    for q in qs:
        if q not in seen:
            seen.add(q)
            out.append(q)
    return out


def validate() -> list[str]:
    errs = []
    for s in FINVIZ_SECTORS:
        if s not in SECTORS:
            errs.append(f"missing sector block: {s}")
            continue
        labs = taxonomy_list(s)
        if len(labs) != len(set(labs)):
            errs.append(f"duplicate labels in {s}")
        for x in SECTORS[s]:
            if x["weight"] < 1 or x["weight"] > 10:
                errs.append(f"bad weight {s} {x['label']}")
            if x["polarity"] not in ("+", "-"):
                errs.append(f"bad polarity {s} {x['label']}")
    return errs


if __name__ == "__main__":
    e = validate()
    print("errors:", e or "none")
    for s in FINVIZ_SECTORS:
        print(f"{s}: {len(taxonomy_list(s))} labels, "
              f"{len(make_search_templates(s))} search templates, ETF={SECTOR_ETFS[s]}")
