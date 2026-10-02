"""Free intake coverage vocabulary. Hints are not final Lane classifications."""
from __future__ import annotations

from .news_impact.schema import EVENT_CLASSES

PHRASES = {
    "capacity": ["opens plant", "production capacity", "expands manufacturing", "new factory"],
    "demand": ["awarded contract", "supply agreement", "purchase order", "lands deal", "multibillion-dollar deal"],
    "input_cost": ["input costs", "fuel surcharge", "raw material costs"],
    "inventory_print": ["crude inventories", "inventory draw", "inventory build"],
    "channel_stock": ["channel inventory", "dealer inventory", "destocking"],
    "reserve_revision": ["reserve estimate", "proven reserves", "resource estimate"],
    "gate": ["FDA approves", "FDA approval", "FDA clears", "FDA clearance", "complete response letter", "marketing authorization"],
    "trial_readout": ["primary endpoint", "phase 3 results", "phase 2 results", "trial results", "trial readout"],
    "ip_ruling": ["patent ruling", "patent invalidated", "patent infringement verdict"],
    "access_control": ["export controls", "export ban", "import ban", "license revoked"],
    "sanction_lift": ["lifts sanctions", "sanctions lifted", "removes sanctions"],
    "standard_mandate": ["final rule", "mandatory standard", "compliance deadline"],
    "market_structure": ["market structure", "trading rules", "tokenized securities"],
    "tax_fiscal": ["signed into law", "tax bill", "tax increase", "fiscal package", "executive order"],
    "price_cap": ["price cap", "price ceiling", "regulated prices"],
    "subsidy": ["government grant", "subsidy", "subsidies", "tax credit awarded"],
    "breakup_remedy": ["ordered divestiture", "breakup order", "antitrust remedy"],
    "fx_translation": ["currency headwind", "foreign exchange impact", "currency devaluation"],
    "policy_personnel": ["Fed chair nomination", "central bank governor appointed", "Treasury secretary appointed"],
    "print_vs_priced": ["earnings results", "reports earnings", "quarterly results", "CPI report", "PCE inflation", "jobs report", "GDP report"],
    "guidance": ["raises guidance", "cuts guidance", "raises outlook", "lowers outlook", "withdraws guidance"],
    "preannounce": ["preliminary results", "profit warning", "preannounces", "pre-announces"],
    "peer_spill": ["read across", "read-across", "peer earnings"],
    "factor_impulse": ["rate cut", "rate hike", "FOMC decision", "payrolls", "inflation data"],
    "corporate_action_mna": ["definitive agreement", "acquisition", "acquires", "to acquire", "merger agreement", "tender offer"],
    "corporate_action_spinoff": ["spin-off", "spinoff", "spin off", "separates business"],
    "dilution": ["public offering", "private placement", "at-the-market offering", "share issuance", "registered direct offering"],
    "capital_return": ["share repurchase", "buyback", "special dividend", "increases dividend"],
    "credit_funding": ["credit facility", "notes offering", "debt financing", "refinancing"],
    "distress_restruct": ["chapter 11", "bankruptcy", "debt restructuring", "defaults on"],
    "integrity": ["accounting fraud", "restates results", "restatement", "auditor resigns"],
    "key_person": ["joins board", "joins the board", "appointed CEO", "names CEO", "steps down", "resigns", "named chairman", "board appointment"],
    "insider_flow": ["insider purchase", "insider sale", "insider buying", "insider selling"],
    "activist_campaign": ["activist stake", "activist investor", "proxy fight", "board challenge"],
    "strategic_review": ["strategic alternatives", "strategic review", "explores sale"],
    "regulatory_probe": ["SEC investigation", "DOJ investigation", "regulatory probe", "antitrust investigation"],
    "deal_review": ["merger approved", "blocks merger", "merger clearance", "antitrust approval"],
    "sovereign_credit": ["sovereign downgrade", "sovereign default", "credit rating downgrade"],
    "blast_legal": ["court rules", "injunction", "jury verdict", "damages awarded", "court judgment"],
    "blast_ops": ["plant shutdown", "production halted", "factory fire", "service outage", "pipeline rupture"],
    "blast_cyber": ["ransomware", "data breach", "cyberattack", "cyber attack"],
    "product_harm": ["product recall", "recalls vehicles", "safety recall", "drug recall"],
    "labor_stop": ["workers strike", "walkout", "labor strike", "strike begins", "strike ends"],
    "labor_organize": ["union vote", "union election", "collective bargaining agreement"],
    "cat_weather": ["hurricane landfall", "earthquake", "flood shuts", "wildfire evacuation"],
    "listing_flow": ["IPO", "uplisting", "delisting", "trading suspension", "direct listing"],
    "lockup_expiry": ["lockup expires", "lock-up expiration", "lockup expiration"],
    "flow_index": ["index inclusion", "added to S&P", "removed from S&P", "index rebalance"],
    "flow_mechanical": ["ETF rebalance", "options expiration", "index reconstitution"],
    "flow_forced_liq": ["margin call", "forced liquidation", "fund liquidation"],
    "regime_state": ["yield curve", "credit spreads", "oil prices", "market volatility"],
    "regime_break": ["ceasefire agreement", "reopens shipping", "peace agreement", "emergency rate decision"],
    "statement_public": ["Fed says", "Kashkari says", "Powell says", "policy speech", "company announces"],
    "rumor": ["reportedly in talks", "rumored acquisition", "leaked filing"],
    "discard": [],
}

EXTRA_SEARCHES = {
    "business": "business OR earnings OR contracts OR company announcements",
    "technology": 'technology OR "artificial intelligence" OR semiconductor OR cloud',
    "products": '"launches" OR "generally available" OR "commercial release" OR "new AI model"',
    "fda": 'FDA (approval OR clearance OR "clinical trial")',
    "fed": '"Federal Reserve" OR "FOMC" OR "Kashkari" OR "Powell"',
    "government": '"Treasury" OR "Commerce Department" OR "Energy Department" OR "executive order"',
    "legislation": '"signed into law" OR "Senate passes" OR "House passes" OR "final rule"',
    "judgments": '"court rules" OR "injunction" OR "antitrust ruling" OR "bankruptcy court"',
    "corporate_announcements": '"company announces" OR "awarded contract" OR "joins board" OR "new product"',
    "central_banks": '"ECB" OR "Bank of Japan" OR "Bank of England" OR "PBOC"',
    "geopolitics": '"sanctions" OR "tariffs" OR "ceasefire" OR "Strait of Hormuz"',
}


def validate() -> None:
    if set(PHRASES) != set(EVENT_CLASSES):
        raise ValueError("Search coverage must account for every locked Lane class")


def search_specs() -> list[dict]:
    validate()
    specs = []
    for cls, phrases in PHRASES.items():
        if phrases:
            # Keep query lengths short; alternative wording is a discovery net.
            specs.append({"id": f"lane_{cls}", "query": " OR ".join(f'"{p}"' for p in phrases),
                          "terms": phrases,
                          "classes": [cls], "interval_minutes": 30, "kind": "search"})
    specs.extend({"id": f"topic_{key}", "query": q, "classes": [],
                  "interval_minutes": 15, "kind": "search"} for key, q in EXTRA_SEARCHES.items())
    return specs
