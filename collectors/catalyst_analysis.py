#!/usr/bin/env python3
"""
Catalyst Analysis Engine v2 – Full Async, Completely Granular Searches,
Company‑Name‑Aware Prompts, Verbose Logging

Phase 0 : DB Snapshot
Phase 1 : Async Event Hunter (~70 granular catalyst searches, LLM extraction)
Phase 2 : Async Company Context (~35 granular context searches, LLM sensitivity)
Phase 3 : Weighting (Python)
Phase 4 : Final Synthesis (LLM verdict – classifies events into grid)
All intermediate data is printed directly to the log.
Schedule: Daily, after data collectors finish
"""

import os, json, time, re, asyncio, aiohttp
from datetime import datetime, date, timedelta, timezone
from openai import OpenAI

# ── Config ──────────────────────────────────────────────
SEARXNG_URL          = os.environ["SEARXNG_URL"]
SEARXNG_TIMEOUT      = 15
SEARCH_CONCURRENCY   = 10         # higher concurrency for many small queries
SEARCH_DELAY         = 0.1        # shorter delay between batches
MODEL                = "deepseek-chat"
TODAY                = date.today().isoformat()
LOOKBACK_START       = (date.today() - timedelta(days=185)).isoformat()

# ── SearXNG async executor ─────────────────────────────
async def search_single(session, query, searxng_url, categories="general,news"):
    try:
        async with session.get(
            f"{searxng_url}/search",
            params={"q": query, "format": "json", "categories": categories},
            timeout=aiohttp.ClientTimeout(total=SEARXNG_TIMEOUT),
            headers={"User-Agent": "CatalystEngine/2.0"}
        ) as resp:
            data = await resp.json()
            results = data.get("results", [])
            formatted = []
            for r in results[:6]:
                formatted.append(
                    f"[{r.get('engine','?')}] {r.get('title','')}\n"
                    f"Date: {r.get('publishedDate','')}\n"
                    f"Snippet: {r.get('content','')[:400]}\n"
                    f"URL: {r.get('url','')}"
                )
            return query, "\n\n".join(formatted) if formatted else f"NO_RESULTS: {query}"
    except Exception as e:
        return query, f"SEARCH_ERROR: {e}"

async def batch_search(queries, searxng_url, concurrency=SEARCH_CONCURRENCY):
    semaphore = asyncio.Semaphore(concurrency)
    async def bounded_search(session, q):
        async with semaphore:
            return await search_single(session, q, searxng_url)
    async with aiohttp.ClientSession() as session:
        tasks = [bounded_search(session, q) for q in queries]
        results = await asyncio.gather(*tasks)
    return dict(results)

# ── Finviz snapshot from DB ─────────────────────────────
def build_health_snapshot(ticker, conn):
    cur = conn.cursor()
    cur.execute("SELECT * FROM company_financials WHERE ticker = %s", (ticker,))
    cols = [desc[0] for desc in cur.description]
    row = cur.fetchone()
    finviz = dict(zip(cols, row)) if row else {}
    cur.execute("""
        SELECT company_name, sector, industry, country, description
        FROM company_profiles WHERE ticker = %s
    """, (ticker,))
    prof = cur.fetchone()
    profile = {}
    if prof:
        profile = {
            "company_name": prof[0],
            "sector": prof[1],
            "industry": prof[2],
            "country": prof[3],
            "description": prof[4],
        }
    cur.close()
    return {"profile": profile, "finviz": finviz}

# ── LLM setup ──────────────────────────────────────────
client = OpenAI(
    api_key=os.environ.get("DEEPSEEK_API_KEY"),
    base_url="https://api.deepseek.com",
)

def safe_create(**kwargs):
    for attempt in range(3):
        try:
            return client.chat.completions.create(**kwargs)
        except Exception as e:
            print(f"  ⚠️  API error (attempt {attempt+1}/3): {e}")
            if attempt < 2:
                time.sleep(2 * (attempt + 1))
            else:
                raise

# ── GRANULAR SEARCH TEMPLATES ─────────────────────────
#   Every single concept has its own query.
#   {full_name} = "Company Name SERV" format.

def _make_catalyst_templates(full_name):
    """Return a list of ~70 individual, focused catalyst queries."""
    return [
        # ── Contracts ──
        f"{full_name} contract award win 2025 2026",
        f"{full_name} contract expansion 2025 2026",
        f"{full_name} contract loss non-renewal 2025 2026",

        # ── Partnerships ──
        f"{full_name} strategic partnership 2025 2026",
        f"{full_name} alliance joint venture 2025 2026",
        f"{full_name} partnership dissolution 2025 2026",

        # ── Products / Regulatory ──
        f"{full_name} new product launch 2025 2026",
        f"{full_name} FDA approval 2025 2026",
        f"{full_name} regulatory greenlight 2025 2026",
        f"{full_name} product delay failure 2025 2026",
        f"{full_name} product recall safety 2025 2026",

        # ── Analyst ──
        f"{full_name} analyst upgrade 2025 2026",
        f"{full_name} analyst downgrade 2025 2026",
        f"{full_name} analyst price target increase 2025 2026",
        f"{full_name} analyst price target cut 2025 2026",
        f"{full_name} analyst initiation coverage 2025 2026",

        # ── Management ──
        f"{full_name} CEO change departure 2025 2026",
        f"{full_name} CFO management change 2025 2026",
        f"{full_name} board member appointment 2025 2026",
        f"{full_name} executive resignation scandal 2025 2026",

        # ── Capital / Financing ──
        f"{full_name} capital raise PIPE 2025 2026",
        f"{full_name} funding round offering 2025 2026",
        f"{full_name} dilutive offering down round 2025 2026",
        f"{full_name} share buyback repurchase 2025 2026",
        f"{full_name} dividend increase 2025 2026",
        f"{full_name} dividend cut suspension 2025 2026",

        # ── Earnings ──
        f"{full_name} earnings beat 2025 2026",
        f"{full_name} earnings miss 2025 2026",
        f"{full_name} earnings guidance raise 2025 2026",
        f"{full_name} earnings guidance cut 2025 2026",
        f"{full_name} revenue results 2025 2026",
        f"{full_name} EBITDA EPS results 2025 2026",

        # ── M&A ──
        f"{full_name} acquisition merger 2025 2026",
        f"{full_name} divestiture spin-off 2025 2026",
        f"{full_name} failed acquisition overpayment 2025 2026",

        # ── Operations ──
        f"{full_name} operational milestone 2025 2026",
        f"{full_name} capacity expansion factory 2025 2026",
        f"{full_name} operational setback trial halted 2025 2026",
        f"{full_name} production halt 2025 2026",

        # ── Insider Trading ──
        f"{full_name} insider buying CEO CFO 2025 2026",
        f"{full_name} insider selling CEO CFO 2025 2026",
        f"{full_name} Form 4 SEC filing insider 2025 2026",
        f"{full_name} insider trading cluster sales 2025 2026",

        # ── Activist / Institutional ──
        f"{full_name} activist investor 13D stake 2025 2026",
        f"{full_name} activist exits stake 2025 2026",
        f"{full_name} institutional ownership 13F increase 2025 2026",
        f"{full_name} institutional ownership decline 2025 2026",

        # ── Supply Chain ──
        f"{full_name} supply chain disruption 2025 2026",
        f"{full_name} factory fire shipping 2025 2026",
        f"{full_name} supply chain de-risking reshoring 2025 2026",

        # ── IP / Litigation ──
        f"{full_name} patent grant 2025 2026",
        f"{full_name} patent litigation lawsuit 2025 2026",
        f"{full_name} IP theft 2025 2026",

        # ── Sector / Macro ──
        f"{full_name} sector tailwind 2025 2026",
        f"{full_name} sector headwind rotation 2025 2026",
        f"{full_name} commodity price impact 2025 2026",
        f"{full_name} tariff trade policy 2025 2026",
        f"{full_name} government subsidy mandate 2025 2026",

        # ── Technical / Market Mechanics ──
        f"{full_name} short interest 2025 2026",
        f"{full_name} short squeeze 2025 2026",
        f"{full_name} bear raid short attack 2025 2026",
        f"{full_name} technical breakout 2026",
        f"{full_name} technical breakdown death cross 2026",

        # ── Geopolitical / Regulatory ──
        f"{full_name} geopolitical impact sanctions 2025 2026",
        f"{full_name} regulatory approval 2025 2026",
        f"{full_name} regulatory denial antitrust 2025 2026",
    ]

def _make_context_templates(full_name):
    """Return a list of ~35 individual, focused context queries."""
    return [
        # ── Revenue ──
        f"{full_name} revenue breakdown by segment",
        f"{full_name} revenue by customer type",
        f"{full_name} revenue geography domestic international",
        f"{full_name} revenue government contracts percentage",
        f"{full_name} commercial vs consumer revenue mix",

        # ── Customer Concentration ──
        f"{full_name} customer concentration largest client",
        f"{full_name} customer concentration risk",

        # ── Business Model ──
        f"{full_name} business model",
        f"{full_name} recurring revenue subscription",

        # ── Cost Structure ──
        f"{full_name} cost structure input costs",
        f"{full_name} raw materials commodities exposure",
        f"{full_name} COGS breakdown",

        # ── Supply Chain ──
        f"{full_name} supply chain manufacturing",
        f"{full_name} China exposure manufacturing",
        f"{full_name} supplier concentration risk",

        # ── Operating Leverage ──
        f"{full_name} operating leverage fixed variable costs",
        f"{full_name} margin structure",

        # ── Competitive Position ──
        f"{full_name} competitive advantage moat",
        f"{full_name} market share",
        f"{full_name} pricing power",
        f"{full_name} industry barriers to entry",
        f"{full_name} switching costs",
        f"{full_name} competitors peer comparison",

        # ── Financial ──
        f"{full_name} debt structure maturity",
        f"{full_name} interest rate sensitivity floating fixed",
        f"{full_name} cash burn rate runway",

        # ── Regulatory ──
        f"{full_name} regulatory environment",
        f"{full_name} government contracts exposure",

        # ── Management ──
        f"{full_name} CEO track record",
        f"{full_name} management capital allocation",
        f"{full_name} insider ownership percentage",

        # ── Litigation ──
        f"{full_name} litigation risk pending lawsuits",
        f"{full_name} patent portfolio IP protection",

        # ── Growth ──
        f"{full_name} organic vs acquisitive growth",
        f"{full_name} order backlog visibility",
        f"{full_name} capacity expansion plans",
        f"{full_name} end market growth rate",
    ]

# ── Taxonomy, weights (unchanged from your working version) ─────
TAXONOMY_LIST = [
    "Contract win/expansion",
    "Strategic partnership/alliance",
    "Product launch/FDA approval/regulatory greenlight",
    "Analyst upgrade/PT increase",
    "Positive personnel change (new CEO, CFO, board members)",
    "Capital infusion (PIPE, funding round, favorable terms)",
    "Earnings beat (revenue, EBITDA, EPS)",
    "Earnings guidance raise",
    "Share repurchase program/increased dividend",
    "Successful acquisition/synergy realization",
    "Deleveraging/sale of toxic assets/spin-off of loss-making unit",
    "Operational milestone (e.g., first patient dosed, satellite commissioned)",
    "Insider buying (cluster purchases by executives/directors)",
    "Activist investor accumulation (e.g., 9.9% stake filing)",
    "Capacity expansion announced (new factory, satellite constellation)",
    "Strategic pivot/rebranding to high-growth area",
    "Supply chain de-risking (dual sourcing, reshoring)",
    "Patent grant/IP protection",
    "Customer concentration expansion (existing customer deepens relationship)",
    "Government policy (tariffs, subsidies, mandates)",
    "Institutional policy (Fed rate cut, QE, stimulus)",
    "Favorable court ruling/patent grant",
    "Geopolitical event that boosts sector (e.g., defense spending surge)",
    "Sector tailwind/index inclusion",
    "Regulatory approval (FDA, FCC, FTC clearance)",
    "Macro tailwinds (CPI cooling, GDP growth surprise, soft landing)",
    "Sector rotation into the stock's industry",
    "Commodity price move favorable to the company",
    "ESG mandate/green subsidy qualification",
    "Currency tailwind (stronger home currency if importing)",
    "Technical breakout (above key moving averages, resistance levels)",
    "Short squeeze (rapid covering of heavily shorted stock)",
    "Institutional ownership increase (13F filings showing accumulation)",
    "Contract loss/non-renewal/reduction in scope",
    "Partnership dissolution/breakdown/rival alliance",
    "Product delay/failure/rejection/safety recall",
    "Analyst downgrade/price target cut",
    "Negative personnel change (departures, resignations, scandals)",
    "Dilutive offering/distressed fundraising/down round",
    "Earnings miss (revenue, EBITDA, EPS)",
    "Earnings guidance cut",
    "Suspension of buyback/dividend cut/elimination",
    "Failed acquisition/overpayment/goodwill impairment",
    "Accumulation of debt/retention or deepening of toxic assets/failed divestiture",
    "Operational setback (trial halted, satellite failure, production halt)",
    "Insider selling (especially by CEO/CFO, or clustered sales)",
    "Activist exits stake/files hostile 13D to force changes",
    "Capacity underutilization/overexpansion write-down",
    "Strategic pivot failure/loss of identity",
    "Supply chain shock (factory fire, shipping disruption)",
    "Patent litigation loss/IP theft",
    "Customer concentration risk (over-reliance on one client)",
    "Policy reversal/new regulation/tax increase",
    "Rate hike/monetary tightening/liquidity withdrawal",
    "Adverse litigation outcome/patent invalidation/antitrust ruling",
    "Geopolitical event that hurts sector (sanctions, conflict disrupting supply chain)",
    "Sector headwind/index exclusion/rotation away",
    "Regulatory denial or delay/antitrust block",
    "Macro headwinds (inflation spike, recession, unemployment surge)",
    "Sector rotation out of the industry",
    "Unfavorable commodity price move (higher input costs)",
    "ESG controversy/exclusion from green funds/carbon tax",
    "Currency headwind (dollar strength for exporters)",
    "Technical breakdown (below support, \"death cross\")",
    "Short attack/bear raid (activist short report)/large new short positions",
    "Institutional ownership decline (major holders reducing stakes)",
]

CATALYST_WEIGHTS = {
    "Contract win/expansion": 8,
    "Strategic partnership/alliance": 6,
    "Product launch/FDA approval/regulatory greenlight": 9,
    "Analyst upgrade/PT increase": 5,
    "Positive personnel change (new CEO, CFO, board members)": 4,
    "Capital infusion (PIPE, funding round, favorable terms)": 6,
    "Earnings beat (revenue, EBITDA, EPS)": 8,
    "Earnings guidance raise": 7,
    "Share repurchase program/increased dividend": 5,
    "Successful acquisition/synergy realization": 7,
    "Deleveraging/sale of toxic assets/spin-off of loss-making unit": 5,
    "Operational milestone (e.g., first patient dosed, satellite commissioned)": 6,
    "Insider buying (cluster purchases by executives/directors)": 7,
    "Activist investor accumulation (e.g., 9.9% stake filing)": 7,
    "Capacity expansion announced (new factory, satellite constellation)": 6,
    "Strategic pivot/rebranding to high-growth area": 5,
    "Supply chain de-risking (dual sourcing, reshoring)": 5,
    "Patent grant/IP protection": 4,
    "Customer concentration expansion (existing customer deepens relationship)": 5,
    "Government policy (tariffs, subsidies, mandates)": 7,
    "Institutional policy (Fed rate cut, QE, stimulus)": 9,
    "Favorable court ruling/patent grant": 8,
    "Geopolitical event that boosts sector (e.g., defense spending surge)": 7,
    "Sector tailwind/index inclusion": 5,
    "Regulatory approval (FDA, FCC, FTC clearance)": 9,
    "Macro tailwinds (CPI cooling, GDP growth surprise, soft landing)": 8,
    "Sector rotation into the stock's industry": 6,
    "Commodity price move favorable to the company": 6,
    "ESG mandate/green subsidy qualification": 4,
    "Currency tailwind (stronger home currency if importing)": 3,
    "Technical breakout (above key moving averages, resistance levels)": 4,
    "Short squeeze (rapid covering of heavily shorted stock)": 8,
    "Institutional ownership increase (13F filings showing accumulation)": 6,
    "Contract loss/non-renewal/reduction in scope": 8,
    "Partnership dissolution/breakdown/rival alliance": 6,
    "Product delay/failure/rejection/safety recall": 9,
    "Analyst downgrade/price target cut": 5,
    "Negative personnel change (departures, resignations, scandals)": 5,
    "Dilutive offering/distressed fundraising/down round": 7,
    "Earnings miss (revenue, EBITDA, EPS)": 8,
    "Earnings guidance cut": 7,
    "Suspension of buyback/dividend cut/elimination": 5,
    "Failed acquisition/overpayment/goodwill impairment": 6,
    "Accumulation of debt/retention or deepening of toxic assets/failed divestiture": 6,
    "Operational setback (trial halted, satellite failure, production halt)": 7,
    "Insider selling (especially by CEO/CFO, or clustered sales)": 7,
    "Activist exits stake/files hostile 13D to force changes": 7,
    "Capacity underutilization/overexpansion write-down": 5,
    "Strategic pivot failure/loss of identity": 5,
    "Supply chain shock (factory fire, shipping disruption)": 8,
    "Patent litigation loss/IP theft": 7,
    "Customer concentration risk (over-reliance on one client)": 6,
    "Policy reversal/new regulation/tax increase": 7,
    "Rate hike/monetary tightening/liquidity withdrawal": 9,
    "Adverse litigation outcome/patent invalidation/antitrust ruling": 8,
    "Geopolitical event that hurts sector (sanctions, conflict disrupting supply chain)": 8,
    "Sector headwind/index exclusion/rotation away": 6,
    "Regulatory denial or delay/antitrust block": 9,
    "Macro headwinds (inflation spike, recession, unemployment surge)": 8,
    "Sector rotation out of the industry": 6,
    "Unfavorable commodity price move (higher input costs)": 6,
    "ESG controversy/exclusion from green funds/carbon tax": 4,
    "Currency headwind (dollar strength for exporters)": 3,
    "Technical breakdown (below support, \"death cross\")": 5,
    "Short attack/bear raid (activist short report)/large new short positions": 8,
    "Institutional ownership decline (major holders reducing stakes)": 6,
}

# ── Prompt Templates (with company‑name verification) ──
STEP1_TEMPLATE = """
You are an event extraction engine. Your input is the search results below
for {full_name} (ticker: {ticker}).

CRITICAL: The articles below may mention other companies. You MUST verify
that the article refers to {full_name} BEFORE extracting an event.
- If a snippet mentions ANI Pharmaceuticals, Pipe, Pool, Apple, Verisk,
  Bar Harbor Bankshares, or any company that is NOT {full_name}, DISCARD IT.
- Only create events for snippets that explicitly mention {full_name} or
  its ticker {ticker}.

Rules:
- Describe each event in one sentence.
- Include the EXACT date (YYYY-MM-DD) from the snippet.
- Include a VERBATIM excerpt (≤150 chars, quoted) from the snippet.
- Include all source URLs related to the event.
- If multiple snippets describe the same event, merge them and set
  confidence higher.
- Use ONLY the provided snippets. Do NOT add your own knowledge.

Search results:
{search_results_json}

Return ONLY a JSON array. No other text.
[
  {{
    "event_date": "2026-03-02",
    "description": "{full_name} reported Q4 revenue of $27.3M.",
    "evidence_excerpt": "\\"revenue of $27.3M\\"",
    "source_urls": ["https://..."],
    "confidence": 90
  }},
  ...
]
"""

AMP_DAMP_TABLE = """
Contract win/expansion:
  [+] High customer concentration, low past revenue growth, small market cap
  [−] Diversified customer base, large cap, contract small relative to revenue

Strategic partnership/alliance:
  [+] Niche industry with high barriers, low institutional ownership, high R&D
  [−] Many existing partnerships, low switching costs

Product launch/FDA approval/regulatory greenlight:
  [+] Biotech/pharma sector, single-product, low cash, high short interest
  [−] Diversified product portfolio, large cap, approval widely expected

Analyst upgrade/PT increase:
  [+] Low analyst coverage, stock near 52-week low, low inst ownership, high short float, PT above current price
  [−] High coverage, PT still below current price (reclassify negative)

Positive personnel change:
  [+] Company in distress, recent scandals, high insider ownership
  [−] Stable company, routine appointment, large cap

Capital infusion:
  [+] High debt, low cash, negative FCF, high short interest
  [−] Already cash-rich, infusion is dilutive

Earnings beat:
  [+] High short interest, stock beaten down, low expectations
  [−] Stock rallied into earnings, beat narrow, peers also beat

Earnings guidance raise:
  [+] Same as beat + CEO credibility, analyst lag
  [−] Raise expected, macro tailwinds obvious, raise small

Share repurchase/dividend increase:
  [+] High cash, low debt, undervalued (P/B < 1), insider buying alongside
  [−] Low cash, high debt, token repurchase, dividend cut history

Successful acquisition/synergy realization:
  [+] Recent acquisition, synergy ahead of plan, accretive
  [−] Integration risk, overpayment history, small deal

Deleveraging/sale of toxic assets/spin-off:
  [+] High debt, negative credit outlook, toxic assets
  [−] Already well-capitalised, sale of core asset

Operational milestone:
  [+] Pre-revenue (biotech/space), regulatory catalyst pending, high R&D
  [−] Routine maintenance, non-value-creating

Insider buying (cluster):
  [+] High insider ownership already, buying after crash, multiple C-suite
  [−] Small amounts, one insider, buying at ATH, option exercise

Activist investor accumulation:
  [+] Underperforming, high cash, breakup value > market cap, low inst ownership
  [−] Management addressing issues, activist poor track record

Capacity expansion:
  [+] High utilisation, growing backlogs, sector demand surging, high margins
  [−] Industry overcapacity, debt-funded, demand weakening

Strategic pivot/rebranding:
  [+] Old business declining, high debt (pivot desperate), CEO credible
  [−] Stable business, pivot faddish, execution risk high

Supply chain de-risking:
  [+] High China exposure, tariff sensitivity, recent supply shocks
  [−] Already diversified, de-risking costly

Patent grant/IP protection:
  [+] Tech/pharma, high R&D, history of IP theft, narrow moat
  [−] Many patents already, patent narrow, workaround easy

Customer concentration expansion:
  [+] High customer concentration currently, expansion to new sectors
  [−] Already diversified, new customer immaterial

Government policy (tariffs/subsidies/mandates):
  [+] Sector directly affected, domestic capacity, bipartisan support
  [−] Policy temporary, company relies on imports, unfunded

Institutional policy (Fed rate cut/QE/stimulus):
  [+] High debt, floating-rate, growth/tech, low cash flow
  [−] Low debt, fixed-rate, cut already priced in

Favorable court ruling/patent grant:
  [+] Litigation priced in, binary outcome, damages large
  [−] Ruling narrow, appeal likely, stock didn't move

Geopolitical event that boosts sector:
  [+] Defence sector, domestic production, govt contract exposure
  [−] Event temporary, indirect benefit

Sector tailwind/index inclusion:
  [+] Small cap added to major index, sector ETF inflows, low liquidity
  [−] Already in index, inclusion priced in, momentum exhausted

Regulatory approval:
  [+] Binary event, no alternatives, high legal costs if denied
  [−] Approval expected, minimal incremental revenue

Macro tailwinds:
  [+] High cyclicality, beta > 1.5, high operating leverage
  [−] Defensive sector, tailwind temporary

Sector rotation into industry:
  [+] Underperformed long, low relative valuations, low inst ownership
  [−] Rotation already happened, industry still overvalued

Commodity price move favorable:
  [+] High commodity sensitivity, unhedged, producer
  [−] Fully hedged, commodity small input cost

ESG mandate/green subsidy:
  [+] Renewable/green sector, high capital requirements
  [−] Already funded, subsidy small

Currency tailwind:
  [+] High export %, high foreign revenue, unhedged
  [−] Hedged, import costs offset, small foreign exposure

Technical breakout:
  [+] High short interest, breakout on volume, long downtrend before
  [−] Low volume breakout, already overbought

Short squeeze:
  [+] Short float > 20%, days-to-cover > 4, positive catalyst cluster
  [−] Short float < 10%, no positive catalyst

Institutional ownership increase:
  [+] Low inst ownership, concentrated fund, recent decline
  [−] Already highly owned, passive flow

Contract loss/non-renewal:
  [+] High customer concentration, contract large % revenue
  [−] Diversified, contract small

Partnership dissolution:
  [+] Partner critical, exclusive, no alternatives
  [−] Small partnership, many alternatives

Product delay/failure/recall:
  [+] Single-product, safety risk, large revenue exposure
  [−] Diverse products, delay minor

Analyst downgrade/PT cut:
  [+] Low coverage, respected analyst, stock near highs
  [−] High coverage, perma-bear, stock already at lows

Negative personnel change:
  [+] Founder/CEO departure, key rainmaker, during crisis
  [−] Routine succession, company stable

Dilutive offering:
  [+] Low cash, high debt, negative FCF, stock down
  [−] Small offering, debt-for-equity swap deleverages

Earnings miss:
  [+] High short interest, high expectations, revenue miss, guidance cut alongside
  [−] Miss small, macro driven, peers also missed

Earnings guidance cut:
  [+] Cut large, structural, peers not cutting, previously guided positive
  [−] Cut small, temporary, peers also cut

Suspension of buyback/dividend cut:
  [+] Cash-strapped, previous commitment, signals distress
  [−] Cut to fund high-return project

Failed acquisition/overpayment:
  [+] High debt taken, goodwill impairment large, integration disaster
  [−] Small deal, regulatory block

Accumulation of debt/toxic assets:
  [+] Already high leverage, deteriorating metrics, near covenant breach
  [−] Accretive debt for growth, low cost

Operational setback:
  [+] Single facility, no backup, revenue concentration
  [−] Diversified operations, insurance covers

Insider selling (cluster):
  [+] CEO/CFO selling after beat, large amounts, no 10b5-1, multiple execs
  [−] Routine 10b5-1, small amounts, one insider

Activist exits/file hostile 13D:
  [+] Activist good track record, large position, underperformed
  [−] Activist exits fast, position small

Capacity underutilization/overexpansion:
  [+] High fixed costs, demand weakening, industry overcapacity
  [−] Temporary underutilisation, upturn expected

Strategic pivot failure:
  [+] Pivot expensive, CEO staked reputation, high debt
  [−] Pivot small experiment, quickly reversed

Supply chain shock:
  [+] Single-source, long lead times, no inventory
  [−] Diversified suppliers, buffer inventory

Patent litigation loss/IP theft:
  [+] Core patent, high royalty income, competitive advantage lost
  [−] Peripheral patent, workaround exists

Customer concentration risk:
  [+] Single customer > 30% revenue, no long-term contract
  [−] Diversified, contract locked in

Policy reversal/new regulation/tax increase:
  [+] Industry directly targeted, high cost impact
  [−] Sector exempt, impact small

Rate hike/monetary tightening:
  [+] High debt, floating rate, low interest coverage, negative FCF
  [−] Low debt, fixed-rate, cash-rich

Adverse litigation/antitrust:
  [+] Binary penalties, large damages, core at risk
  [−] Nuisance suit, low probability

Geopolitical event that hurts sector:
  [+] High exposure to conflict region, supply chain disruption
  [−] Diversified geography, domestic focus

Sector headwind/index exclusion:
  [+] Index fund selling forced, low liquidity
  [−] Exclusion expected, small ETF weight

Regulatory denial/antitrust block:
  [+] Deal-breaker, no alternative, sunk cost
  [−] Denial expected, alternative paths

Macro headwinds:
  [+] High cyclicality, consumer discretionary, high operating leverage
  [−] Defensive sector, high cash, flexible costs

Sector rotation out:
  [+] High valuation premium, high beta, crowded institutional positioning
  [−] Already underowned, attractive value

Unfavorable commodity price move:
  [+] High input cost sensitivity, unhedged, low pricing power
  [−] Hedged, high pricing power, small input cost

ESG controversy/carbon tax:
  [+] High emission industry, no offset plan, brand risk
  [−] Already green, tax small

Currency headwind:
  [+] High export revenue, unhedged, domestic costs in strong currency
  [−] Hedged, foreign costs decline, small foreign share

Technical breakdown:
  [+] Breakdown on high volume, death cross, preceded by rally
  [−] Low volume, already oversold

Short attack/bear raid:
  [+] High short interest, credible short report, fraud allegation
  [−] Already heavily shorted, report low credibility

Institutional ownership decline:
  [+] Concentrated holders, high-conviction fund exiting, after pop
  [−] Passive rebalancing, one small fund
"""

STEP2_TEMPLATE = """
You are a COMPANY CONTEXT ANALYST. Your inputs are:
1. A financial snapshot of {full_name} (ticker: {ticker}) from a database.
2. Search snippets about {full_name} (ticker: {ticker})'s business model,
   operations, and risks.

CRITICAL: Only use snippets that explicitly refer to {full_name}. Discard
any snippet about a different company.

FINANCIAL SNAPSHOT:
{snapshot}

CONTEXT SEARCH SNIPPETS:
{context_search_results}

PHASE 1: Structured Context Questionnaire
Answer every question using the snapshot and snippets.
Write "DATUM_MISSING" if data is not available.

1. REVENUE STRUCTURE
   a) % revenue from government / commercial / consumer?
   b) % revenue from domestic (US) / international?
   c) Top 3 customers and their approximate revenue share?
   d) Revenue concentration risk (high/medium/low)?

2. COST STRUCTURE & SUPPLY CHAIN
   a) Top 3 input costs (e.g., labour, cloud, aluminium)?
   b) % of COGS that is commodity-linked?
   c) Manufacturing in-house vs. outsourced?
   d) % of supply chain exposed to China or geopolitical risk zones?
   e) Supplier concentration risk (high/medium/low)?

3. COMPETITIVE POSITION
   a) Pricing power? (can raise prices without losing volume – yes/no/partial)
   b) Customer switching costs? (high/medium/low)
   c) Industry structure? (fragmented/oligopoly/monopoly)
   d) Approximate market share?
   e) Top 3 competitors?

4. FINANCIAL SENSITIVITIES (snapshot + snippets)
   a) Debt-to-equity ratio
   b) % of debt that is fixed-rate vs. floating-rate?
   c) Interest coverage ratio (EBIT / interest expense)
   d) Cash runway (current assets / monthly cash burn)
   e) Revenue growth trend (accelerating / flat / declining)
   f) Gross margin trend (expanding / stable / compressing)
   g) Profit margin trend
   h) Free cash flow (positive / negative / trend)

5. EXTERNAL EXPOSURES
   a) Tariff sensitivity? (high/medium/low + which tariffs)
   b) Commodity price sensitivity? (which commodities, % of revenue/cost)
   c) Currency sensitivity? (which pairs, % exposure)
   d) Key regulatory agencies (FDA, DoD, SEC, FTC, etc.)
   e) Geopolitical risk concentration? (regions, specific conflicts)
   f) Government contract exposure? (high/medium/low + % revenue)

6. MANAGEMENT & RISKS
   a) CEO track record (tenure, prior successes/failures)
   b) Insider ownership % and recent buy/sell trend
   c) Pending litigation (list cases, materiality, potential damages)
   d) Regulatory investigations (list, status, potential impact)

7. GROWTH TRAJECTORY
   a) Organic vs. acquisitive growth mix
   b) Order-book or backlog visibility
   c) Capacity expansion plans
   d) Primary end-market growth rate

Cite snippet IDs or snapshot fields for each answer.

PHASE 2: Sensitivity Multiplier Generation

Now, using the completed context questionnaire, compute a multiplier for
EVERY catalyst in the TAXONOMY list. Use the AMPLIFIER/DAMPENER REFERENCE
TABLE below.

Rules:
- Default multiplier = 1.0 (no change) unless conditions are met.
- Amplified (hits harder): 1.2–1.5.  Dampened (hits weaker): 0.5–0.8.
- Irrelevant (cannot affect this company): 0.0.
- Provide a one-sentence rationale citing the specific extracted fact.

AMPLIFIER/DAMPENER REFERENCE TABLE:
{amp_damp_table}

TAXONOMY:
{taxonomy_list_str}

OUTPUT FORMAT:
Return ONLY this JSON.
{{
  "ticker": "{ticker}",
  "extracted_context": {{
    "revenue_structure": {{ ... }},
    "cost_structure": {{ ... }},
    "competitive_position": {{ ... }},
    "financial_sensitivities": {{ ... }},
    "external_exposures": {{ ... }},
    "management_risks": {{ ... }},
    "growth_trajectory": {{ ... }}
  }},
  "sensitivity_profile": {{
    "Contract win/expansion": {{
      "multiplier": 1.3,
      "rationale": "High revenue concentration amplifies contract wins. [source: Q1c]"
    }},
    ... for ALL catalysts ...
  }},
  "missing_data": ["list any questionnaire items answered DATUM_MISSING"]
}}
"""

STEP4_TEMPLATE = """
You are a FINAL CATALYST SYNTHESIZER for {full_name} (ticker: {ticker}) on {today}.

INPUTS:
1. Raw events – a flat list of events extracted from web searches. These
   have been verified to be about {full_name} only.
{raw_events_json}

2. Weighted taxonomy – each catalyst has a base weight and an
   adjusted_weight that already incorporates company context.
{weighted_taxonomy_json}

3. Financial snapshot:
{snapshot_json}

TASKS:
A. Classify each raw event into one or more catalyst categories from the
   taxonomy.  Use the EXACT taxonomy label.  An event can trigger
   multiple catalysts.

B. Build the FULL catalyst grid.  For every catalyst in the taxonomy:
   - status: "HIT" if at least one event was classified to it,
     "MISS" if no event applies but the catalyst is relevant,
     "N/A" if the catalyst does not apply to this company.
   - For each HIT, include the event's date, excerpt, source URLs,
     and confidence.  Use the adjusted_weight from the weighted taxonomy.
   - For MISS or N/A, adjusted_weight = 0.

C. Apply INTERACTION RULES:
   1. If "Insider selling (cluster)" is HIT AND "Earnings beat" is HIT
      within 14 days, reduce the beat's adjusted_weight by 1 and add a
      synthetic negative catalyst "Insider-earnings divergence" with
      adjusted_weight=1.
   2. If snapshot shows float_short > 20% AND positive HITs dominate,
      add a positive catalyst "Short squeeze potential" with
      adjusted_weight=3.
   3. If snapshot shows analyst target price < current price, any
      "Analyst upgrade/PT increase" HIT is reclassified as negative
      (type: negative, taxonomy: "Analyst downgrade/PT cut").
   4. If both "Technical breakdown" and "Earnings beat" are HIT,
      reduce the breakdown's adjusted_weight by 2 (fundamentals may
      override momentum).

D. Compute FINAL SCORES:
   - Positive_Score = sum(adjusted_weight × confidence/100) for all
     positive HITs.
   - Negative_Score = sum(adjusted_weight × confidence/100) for all
     negative HITs.
   - Net = Positive_Score − Negative_Score.
   - Map:
     Net >= 20  → Strong Bullish
     Net >=  8  → Bullish
     Net >= -8  → Neutral
     Net >= -20 → Bearish
     else       → Strong Bearish
   - Conviction = min(100, abs(Net) * 2)

E. Write a `catalyst_stack` – a 4-sentence narrative that references
   specific dates, ties the most impactful events to the company's
   context, and explains the net signal.

F. Identify the single `key_assumption` that, if wrong, would flip
   the call.

OUTPUT FORMAT:
Return ONLY this JSON.
{{
  "ticker": "{ticker}",
  "analysis_date": "{today}",
  "current_price": "...",
  "catalyst_grid": [
    {{
      "taxonomy": "Contract win/expansion",
      "type": "positive",
      "category": "internal",
      "status": "HIT",
      "base_weight": 8,
      "adjusted_weight": 10,
      "event_date": "2025-10-14",
      "evidence_excerpt": "\\"...$165.15M Army contract...\\"",
      "source_urls": ["https://..."],
      "confidence": 90,
      "sensitivity_rationale": "Amplified – high govt contract exposure."
    }},
    ... every catalyst
  ],
  "catalyst_stack": "...",
  "net_signal": "Bullish",
  "conviction": 78,
  "key_assumption": "..."
}}
"""

# ── Helpers for prompt formatting ──────────────────────
def _format_step1(full_name, ticker, today, lookback_start, search_results_json, taxonomy_list_str):
    return STEP1_TEMPLATE.format(
        full_name=full_name,
        ticker=ticker,
        today=today,
        lookback_start=lookback_start,
        search_results_json=search_results_json,
        taxonomy_list_str=taxonomy_list_str,
    )

def _format_step2(full_name, ticker, snapshot, context_search_results, amp_damp_table, taxonomy_list_str):
    return STEP2_TEMPLATE.format(
        full_name=full_name,
        ticker=ticker,
        snapshot=json.dumps(snapshot, indent=2, default=str),
        context_search_results=context_search_results,
        amp_damp_table=amp_damp_table,
        taxonomy_list_str=taxonomy_list_str,
    )

def _format_step4(full_name, ticker, today, raw_events_json, weighted_taxonomy_json, snapshot_json):
    return STEP4_TEMPLATE.format(
        full_name=full_name,
        ticker=ticker,
        today=today,
        raw_events_json=raw_events_json,
        weighted_taxonomy_json=weighted_taxonomy_json,
        snapshot_json=snapshot_json,
    )

# ── Robust JSON parser ──────────────────────────────────
def parse_json(raw):
    """Parse JSON, handling common LLM output quirks."""
    raw = raw.strip()
    if raw.startswith("```"):
        raw = raw.split("\n", 1)[1].rsplit("```", 1)[0].strip()

    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        pass

    cleaned = re.sub(r",\s*}", "}", raw)
    cleaned = re.sub(r",\s*]", "]", cleaned)
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        pass

    repaired = re.sub(r'(?<!\\)"(?=(?:(?<!\\)(?:\\\\)*\\")*[^"]*$)', r'\"', cleaned)
    try:
        return json.loads(repaired)
    except json.JSONDecodeError:
        pass

    try:
        fix_resp = safe_create(
            model=MODEL,
            messages=[
                {"role": "system", "content": "Return ONLY valid JSON. Close all open brackets/braces."},
                {"role": "user", "content": f"Fix this JSON:\n\n{raw[:20000]}"}
            ],
            temperature=0.0,
            max_tokens=500
        )
        fixed2 = fix_resp.choices[0].message.content.strip()
        if fixed2.startswith("```"):
            fixed2 = fixed2.split("\n", 1)[1].rsplit("```", 1)[0].strip()
        return json.loads(fixed2)
    except Exception:
        pass

    last_good = 0
    brace_count = 0
    in_string = False
    escaped = False
    for i, ch in enumerate(cleaned):
        if escaped:
            escaped = False
            continue
        if ch == '\\':
            escaped = True
            continue
        if ch == '"' and not escaped:
            in_string = not in_string
            continue
        if in_string:
            continue
        if ch == '{':
            brace_count += 1
        elif ch == '}':
            brace_count -= 1
            if brace_count == 0:
                last_good = i + 1
    if last_good > 0:
        truncated = cleaned[:last_good] + "\n    ]\n  }"
        try:
            return json.loads(truncated)
        except json.JSONDecodeError:
            pass

    raise ValueError(f"All JSON repair strategies failed. Raw start: {raw[:200]}")

# ── LLM caller ─────────────────────────────────────────
def call_llm(prompt, user_msg, temperature=0.3, max_tokens=15000):
    messages = [
        {"role": "system", "content": prompt},
        {"role": "user", "content": user_msg}
    ]
    resp = safe_create(
        model=MODEL,
        messages=messages,
        temperature=temperature,
        max_tokens=max_tokens
    )
    return resp.choices[0].message.content.strip()

def build_evidence_grid_from_events(events, taxonomy_list):
    grid = []
    for catalyst in taxonomy_list:
        grid.append({"catalyst": catalyst, "status": "MISS", "confidence": 0})
    return grid

# ── Async analysis pipeline ────────────────────────────
async def analyze_stock_async(ticker, snapshot, searxng_url):
    # Determine full name for search
    full_name = snapshot.get("profile", {}).get("company_name", ticker)
    if not full_name or full_name == ticker:
        full_name = ticker
    else:
        full_name = f"{full_name} {ticker}"

    taxonomy_list_str = "\n".join(TAXONOMY_LIST)

    # Build granular search queries
    catalyst_queries = _make_catalyst_templates(full_name)
    context_queries = _make_context_templates(full_name)

    print(f"  Full name for searches: {full_name}")
    print(f"  ⏳ Preparing {len(catalyst_queries)} catalyst + {len(context_queries)} context search queries...")

    # Print all queries
    print("\n  ── Catalyst Queries ──")
    for i, q in enumerate(catalyst_queries):
        print(f"    [{i+1}] {q}")
    print("  ── Context Queries ──")
    for i, q in enumerate(context_queries):
        print(f"    [{i+1}] {q}")

    print(f"\n  🔎 Launching all {len(catalyst_queries) + len(context_queries)} searches in parallel...")
    catalyst_task = batch_search(catalyst_queries, searxng_url)
    context_task = batch_search(context_queries, searxng_url)
    catalyst_results, context_results = await asyncio.gather(catalyst_task, context_task)

    cat_with = sum(1 for v in catalyst_results.values() if not v.startswith('SEARCH_ERROR') and not v.startswith('NO_RESULTS'))
    ctx_with = sum(1 for v in context_results.values() if not v.startswith('SEARCH_ERROR') and not v.startswith('NO_RESULTS'))
    print(f"  ✅ Catalyst searches: {cat_with}/{len(catalyst_queries)} with results")
    print(f"  ✅ Context searches:  {ctx_with}/{len(context_queries)} with results")

    # ── DUMP SEARCH RESULTS ──
    print("\n" + "="*80)
    print("  🔎 CATALYST SEARCH RESULTS")
    print("="*80)
    for q, v in catalyst_results.items():
        print(f"\n  QUERY: {q}")
        print(f"  RESULT: {v[:500]}{'...' if len(v)>500 else ''}")
        print("  ──")
    print("\n" + "="*80)
    print("  🔎 CONTEXT SEARCH RESULTS")
    print("="*80)
    for q, v in context_results.items():
        print(f"\n  QUERY: {q}")
        print(f"  RESULT: {v[:500]}{'...' if len(v)>500 else ''}")
        print("  ──")

    # Prepare prompts
    search_results_str = "\n\n".join([f"Query: {q}\n{v}" for q, v in catalyst_results.items()])
    prompt1 = _format_step1(full_name, ticker, TODAY, LOOKBACK_START, search_results_str, taxonomy_list_str)

    context_str = "\n\n".join([f"Query: {q}\n{v}" for q, v in context_results.items()])
    prompt2 = _format_step2(full_name, ticker, snapshot, context_str, AMP_DAMP_TABLE, taxonomy_list_str)

    print("  🧠 Running Step 1 (event extraction) and Step 2 (context sensitivity) in parallel...")
    step1_task = asyncio.to_thread(call_llm, prompt=prompt1, user_msg=f"Extract all events for {full_name}.")
    step2_task = asyncio.to_thread(call_llm, prompt=prompt2, user_msg=f"Analyze company context for {full_name}.")
    step1_raw, step2_raw = await asyncio.gather(step1_task, step2_task)
    print("  ✅ Step 1 LLM done.")
    print("  ✅ Step 2 LLM done.")

    # Parse Step 1
    try:
        raw_events = parse_json(step1_raw)
        if isinstance(raw_events, dict):
            raw_events = raw_events.get("events", raw_events.get("evidence_grid", []))
        if not isinstance(raw_events, list):
            raise ValueError("Step 1 output is not a list")
        print(f"  📋 Step 1 extracted {len(raw_events)} raw events for {full_name}")
        # Dump raw events
        print("\n" + "="*80)
        print("  📋 RAW EVENTS (STEP 1 OUTPUT)")
        print("="*80)
        for i, ev in enumerate(raw_events):
            print(f"  [{i+1}] Date: {ev.get('event_date','?')}")
            print(f"      Description: {ev.get('description','')}")
            print(f"      Excerpt: {ev.get('evidence_excerpt','')}")
            print(f"      URLs: {ev.get('source_urls',[])}")
            print(f"      Confidence: {ev.get('confidence','')}")
            print("  ──")
    except Exception as e:
        print(f"  ❌ Failed to parse Step 1 output: {e}")
        return {"error": "Step 1 parse failure", "raw": step1_raw[:500]}

    # Parse Step 2
    try:
        context_profile = parse_json(step2_raw)
    except Exception as e:
        print(f"  ❌ Failed to parse Step 2 JSON: {e}")
        return {"error": "Step 2 parse failure", "raw": step2_raw[:500]}

    # Dump context questionnaire
    extracted = context_profile.get("extracted_context", {})
    if extracted:
        print("\n" + "="*80)
        print("  📝 EXTRACTED CONTEXT (STEP 2 PART 1)")
        print("="*80)
        for section, answers in extracted.items():
            print(f"\n  [{section}]")
            for q, a in answers.items():
                print(f"    {q}: {a}")

    # Dump sensitivity profile
    sensitivity = context_profile.get("sensitivity_profile", {})
    if sensitivity:
        print("\n" + "="*80)
        print("  📈 SENSITIVITY PROFILE (STEP 2 PART 2)")
        print("="*80)
        for cat, val in sensitivity.items():
            print(f"  {cat}: multiplier={val.get('multiplier')} | {val.get('rationale','')}")

    # Step 3: Weighting
    weighted_taxonomy = {}
    for catalyst, profile in sensitivity.items():
        base = CATALYST_WEIGHTS.get(catalyst, 5)
        multiplier = profile.get("multiplier", 1.0)
        adjusted = round(base * multiplier)
        weighted_taxonomy[catalyst] = {
            "base_weight": base,
            "multiplier": multiplier,
            "adjusted_weight": max(0, min(10, adjusted)),
            "rationale": profile.get("rationale", "")
        }

    print("\n" + "="*80)
    print("  ⚖️  WEIGHTED TAXONOMY (STEP 3)")
    print("="*80)
    for cat, w in weighted_taxonomy.items():
        print(f"  {cat}: base={w['base_weight']} × {w['multiplier']} = {w['adjusted_weight']} | {w.get('rationale','')}")

    # Step 4: Final synthesis
    prompt4 = _format_step4(
        full_name=full_name,
        ticker=ticker,
        today=TODAY,
        raw_events_json=json.dumps(raw_events, indent=2),
        weighted_taxonomy_json=json.dumps(weighted_taxonomy, indent=2),
        snapshot_json=json.dumps(snapshot, indent=2, default=str)
    )
    print("  🧠 Running Step 4 (final synthesis)...")
    final_raw = call_llm(prompt=prompt4, user_msg=f"Synthesize final analysis for {full_name}.", temperature=0.3, max_tokens=15000)
    try:
        final_result = parse_json(final_raw)
    except Exception as e:
        print(f"  ❌ Failed to parse Step 4 JSON: {e}")
        return {"error": "Step 4 parse failure", "raw": final_raw[:500]}

    return final_result

# ── Main synchronous wrapper ────────────────────────────
def analyze_stock(ticker, snapshot, searxng_url):
    return asyncio.run(analyze_stock_async(ticker, snapshot, searxng_url))

# ── Main loop (test on one ticker) ────────────────────
if __name__ == "__main__":
    try:
        from db.connection import get_connection
        HAS_DB = True
    except ModuleNotFoundError:
        HAS_DB = False
        print("⚠️  psycopg2 not installed – no DB health check will be performed.")

    try:
        with open("data/exposure_profiles.json") as f:
            profiles = json.load(f)
    except FileNotFoundError:
        profiles = {}
        print("⚠️  No exposure_profiles.json.")

    conn = None
    cur = None

    # ── Set ticker here ──
    tickers = ["SERV"]

    for ticker in tickers:
        print(f"\n{'='*60}\n📊 Snapshot for {ticker}…")
        if HAS_DB:
            if conn is None:
                conn = get_connection()
                cur = conn.cursor()
            snap = build_health_snapshot(ticker, conn)
        else:
            snap = {}
        print(f"Snapshot keys: {list(snap.get('finviz', {}).keys())[:10] if snap else 'N/A'}")

        print(f"\n{'='*60}\n🚀 Starting full catalyst analysis for {ticker}…\n{'='*60}")
        start = time.time()
        result = analyze_stock(ticker, snap, SEARXNG_URL)
        elapsed = time.time() - start
        print(f"\n⏱️  Analysis completed in {elapsed:.1f}s")

        if "error" in result:
            print(f"❌ Error: {result['error']}")
        else:
            print(f"✅ Net signal: {result.get('net_signal')} (conviction {result.get('conviction')})")
            print(f"   Catalyst stack: {result.get('catalyst_stack','')}")
            print(f"   Key assumption: {result.get('key_assumption','')}")
            grid = result.get("catalyst_grid", [])
            hits = [c for c in grid if c.get("status") == "HIT"]
            misses = [c for c in grid if c.get("status") == "MISS"]
            nas = [c for c in grid if c.get("status") == "N/A"]
            print(f"   Grid: {len(grid)} catalysts | HIT: {len(hits)} | MISS: {len(misses)} | N/A: {len(nas)}")

            print("\n" + "="*80)
            print("  🟢🔴 ALL HIT CATALYSTS")
            print("="*80)
            for h in hits:
                print(f"  [{h.get('type','?')}] {h.get('taxonomy')}")
                print(f"      Date: {h.get('event_date','?')}")
                print(f"      Base/Adj Weight: {h.get('base_weight')}/{h.get('adjusted_weight')}")
                print(f"      Excerpt: {h.get('evidence_excerpt','')}")
                print(f"      URLs: {h.get('source_urls',[])}")
                print(f"      Confidence: {h.get('confidence','')}")
                print(f"      Rationale: {h.get('sensitivity_rationale','')}")
                print("  ──")

            print("\n" + "="*80)
            print("  ⚪ MISS CATALYSTS")
            print("="*80)
            for m in misses:
                print(f"  {m.get('taxonomy')}")

            if nas:
                print("\n" + "="*80)
                print("  ⛔ N/A CATALYSTS")
                print("="*80)
                for n in nas:
                    print(f"  {n.get('taxonomy')}")

        time.sleep(2)

    if cur: cur.close()
    if conn: conn.close()
    print(f"\n{'='*60}\n🏁 All analyses complete.")
