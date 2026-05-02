#!/usr/bin/env python3
"""
Catalyst Analysis Engine v2 – Full Async, Granular Searches,
Full Company Name Resolution, Finviz-First Evidence,
Smart Snippet Filtering, Gemini Fact-Check Layer

Phase 0   : DB Snapshot + Company Name Resolution
Phase 0.5 : FinvizFinance per‑ticker news scrape (PRIMARY EVIDENCE)
Phase 1   : Async Event Hunter (LLM extracts only NEW events)
Phase 2   : Async Company Context (LLM sensitivity)
Phase 3   : Weighting (Python)
Phase 4   : Final Synthesis (LLM verdict)
Phase 5   : Gemini Fact‑Check (verify all 66 catalysts + audit multipliers)
Phase 6   : Merge corrections & recalculate net_signal
All intermediate data is printed directly to the log.
Schedule: Daily, after data collectors finish
"""

import os, json, time, re, asyncio, aiohttp, requests
from datetime import datetime, date, timedelta, timezone
from openai import OpenAI

# ── Config ──────────────────────────────────────────────
SEARXNG_URL          = os.environ["SEARXNG_URL"]
SEARXNG_TIMEOUT      = 15
SEARCH_CONCURRENCY   = 6          # reduced to avoid OOM on SearXNG
SEARCH_DELAY         = 0.1
MODEL                = "deepseek-chat"
GEMINI_API_KEY       = os.environ.get("GEMINI_API_KEY", "")
GEMINI_MODEL         = "gemini-2.5-pro"
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
            for r in results[:4]:   # reduced from 6 to 4 to reduce memory pressure
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

# ── Filter snippets to only those mentioning the company ──
def _snippet_is_relevant(snippet_text, full_name, ticker, aliases=None):
    """Return True if the snippet mentions the company or its ticker."""
    check = snippet_text.lower()
    if ticker.lower() in check:
        return True
    name_words = full_name.lower().split()
    matches = sum(1 for w in name_words if w in check)
    if matches >= min(2, len(name_words)):
        return True
    if aliases:
        for alias in aliases:
            if alias.lower() in check:
                return True
    return False

def _filter_search_results(results_dict, full_name, ticker, aliases=None):
    """Replace irrelevant search results with 'NO_RELEVANT_RESULTS'."""
    filtered = {}
    for query, text in results_dict.items():
        if text.startswith("NO_RESULTS") or text.startswith("SEARCH_ERROR"):
            filtered[query] = text
        elif _snippet_is_relevant(text, full_name, ticker, aliases):
            filtered[query] = text
        else:
            filtered[query] = "NO_RELEVANT_RESULTS"
    return filtered

# ── Company Name Resolution (multiple fallbacks) ────────
async def _resolve_company_name_async(ticker, searxng_url):
    """Try multiple sources to get the full company name for a ticker.
    Returns (official_name, aliases_list)."""
    try:
        from finvizfinance.quote import finvizfinance
        stock = finvizfinance(ticker)
        fund = stock.ticker_fundament()
        if fund and fund.get('Company'):
            name = fund['Company'].strip()
            if name and name.lower() != ticker.lower():
                print(f"  ✅ Company name from Finviz: {name}")
                return name, []
    except Exception:
        pass

    try:
        import yfinance as yf
        info = yf.Ticker(ticker).info
        for key in ('longName', 'shortName', 'displayName'):
            name = info.get(key, '')
            if name and ticker.lower() in name.lower():
                for suffix in (' Common Stock', ' Inc.', ' Inc', ' Corp.', ' Corp',
                               ' Corporation', ' Ltd.', ' Ltd', ' PLC', ' Class A',
                               ' - Ordinary Shares', ' Holdings'):
                    name = name.replace(suffix, '')
                name = name.strip()
                if name and name.lower() != ticker.lower():
                    print(f"  ✅ Company name from Yahoo Finance: {name}")
                    return name, []
    except Exception:
        pass

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"{searxng_url}/search",
                params={"q": f"{ticker} stock company name", "format": "json", "categories": "general"},
                timeout=aiohttp.ClientTimeout(total=10),
                headers={"User-Agent": "CatalystEngine/2.0"}
            ) as resp:
                data = await resp.json()
                results = data.get("results", [])
                for r in results[:3]:
                    snippet = r.get("content", "")
                    title = r.get("title", "")
                    combined = f"{title} {snippet}".lower()
                    patterns = [
                        rf"{ticker.lower()}\s*[|–\-—]\s*([A-Z][A-Za-z\s]+?)(?:\s+Stock|\s+Inc|\s+Corp|\s+Profile|,)",
                    ]
                    for pat in patterns:
                        m = re.search(pat, combined, re.IGNORECASE)
                        if m:
                            name = m.group(1).strip()
                            if name and len(name) > 3 and name.lower() != ticker.lower():
                                print(f"  ✅ Company name from web search: {name}")
                                return name, []
    except Exception:
        pass

    print(f"  ⚠️  Could not resolve company name for {ticker}; using ticker as name.")
    return ticker, []

def resolve_company_name(ticker, searxng_url):
    """Synchronous wrapper."""
    return asyncio.run(_resolve_company_name_async(ticker, searxng_url))

# ── Finviz snapshot from DB ─────────────────────────────
def build_health_snapshot(ticker, conn):
    cur = conn.cursor()
    cur.execute("SELECT * FROM company_financials WHERE ticker = %s", (ticker,))
    cols = [desc[0] for desc in cur.description]
    row = cur.fetchone()
    finviz = dict(zip(cols, row)) if row else {}
    db_company = finviz.get("company", "").strip()
    cur.execute("""
        SELECT company_name, sector, industry, country, description
        FROM company_profiles WHERE ticker = %s
    """, (ticker,))
    prof = cur.fetchone()
    profile = {}
    if prof:
        profile = {
            "company_name": prof[0] or db_company or ticker,
            "sector": prof[1],
            "industry": prof[2],
            "country": prof[3],
            "description": prof[4],
        }
    else:
        profile = {
            "company_name": db_company or ticker,
            "sector": finviz.get("sector"),
            "industry": finviz.get("industry"),
            "country": finviz.get("country"),
            "description": "",
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

# ── FinvizFinance news scrape ──────────────────────────
def scrape_finviz_news(ticker):
    """Return structured news headlines for a ticker from Finviz. Returns list of dicts."""
    try:
        from finvizfinance.quote import finvizfinance
        stock = finvizfinance(ticker)
        news_df = stock.ticker_news()
        if news_df is None or news_df.empty:
            return []
        events = []
        for _, row in news_df.iterrows():
            d = row.get('Date', '')
            try:
                parsed = datetime.strptime(str(d), "%I:%M %p %m/%d/%Y")
                date_str = parsed.strftime("%Y-%m-%d")
            except ValueError:
                date_str = str(d)[:10]
            title = str(row.get('Title', ''))
            source = str(row.get('Source', ''))
            link = str(row.get('Link', ''))
            urls = [link] if link and link.startswith('http') else [f"https://finviz.com/quote.ashx?t={ticker}"]
            events.append({
                "event_date": date_str,
                "description": f"{title} (via {source})",
                "evidence_excerpt": title[:150],
                "source_urls": urls,
                "confidence": 85,
                "source": "finviz",
                "finviz_source": source
            })
        return events
    except ImportError:
        print("  ⚠️  finvizfinance not installed – skipping Finviz news scrape.")
        return []
    except Exception as e:
        print(f"  ⚠️  Finviz news scrape failed: {e}")
        return []

# ── Gemini Fact‑Check Layer ────────────────────────────
def gemini_factcheck(full_name, ticker, grid, snapshot, weighted_taxonomy, taxonomy_list):
    """Verify ALL 66 grid items + audit multipliers using Gemini with Google Search Grounding."""
    if not GEMINI_API_KEY:
        print("  ⚠️  GEMINI_API_KEY not set – skipping Gemini fact‑check.")
        return grid, weighted_taxonomy

    print("  🔍 Running Gemini fact‑check layer…")

    hits = [c for c in grid if c.get("status") == "HIT"]
    misses = [c for c in grid if c.get("status") == "MISS"]
    nas = [c for c in grid if c.get("status") == "N/A"]

    prompt = f"""
You are a financial fact‑checking engine for {full_name} (ticker: {ticker}).
TODAY is {TODAY}. The lookback window is {LOOKBACK_START} to {TODAY}.

Below is a catalyst grid generated by another model.
Your task: verify EVERY entry. Use Google Search to find evidence.

────────────── VERIFICATION RULES ──────────────
For HIT items:
   a) Does this event actually exist? Search Google.
   b) Is it specifically about {full_name} (or {ticker})?
   c) Is the date correct? (YYYY-MM-DD)
   d) Is the taxonomy classification correct?
   e) Impact strength: 1‑10 (10 = major market mover)

For MISS items:
   a) Search Google for "{full_name} + [catalyst description]"
   b) If you find evidence the event occurred, return it as a corrected HIT
   c) If truly nothing found, confirm as MISS

For N/A items:
   a) Quick sanity check — is this truly inapplicable?

For multiplier accuracy:
   Check if the sensitivity multipliers make sense for this specific company.
   If a multiplier is wrong, provide the corrected value and rationale.

────────────── HIT ITEMS ──────────────
{json.dumps(hits, indent=2)}

────────────── MISS ITEMS ──────────────
{json.dumps(misses, indent=2)}

────────────── N/A ITEMS ──────────────
{json.dumps(nas, indent=2)}

────────────── CURRENT MULTIPLIERS ──────────────
{json.dumps({k: v.get("multiplier", 1.0) for k, v in weighted_taxonomy.items()}, indent=2)}

────────────── FINANCIAL SNAPSHOT ──────────────
{json.dumps(snapshot, indent=2, default=str)}

Return ONLY this JSON:
{{
  "corrected_grid": [
    {{
      "taxonomy": "exact label from the grid",
      "original_status": "HIT|MISS|N/A",
      "corrected_status": "HIT|MISS|N/A",
      "correction_type": "confirmed|reclassified|new_find|source_disputed|date_corrected|multiplier_override|impact_adjusted",
      "event_date": "YYYY-MM-DD if HIT",
      "evidence_excerpt": "≤150 chars, verbatim from source",
      "source_urls": ["urls"],
      "source_quality": "high|medium|low",
      "confidence": 0-100,
      "impact_strength": 1-10,
      "corrected_multiplier": null or adjusted value,
      "rationale": "one sentence why"
    }}
  ],
  "multiplier_overrides": {{
    "catalyst_label": {{"multiplier": 0.0, "rationale": "reason"}}
  }}
}}
"""

    url = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent"
    headers = {"Content-Type": "application/json"}
    params = {"key": GEMINI_API_KEY}

    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "tools": [{"googleSearch": {}}],
        "generationConfig": {"temperature": 0.1, "maxOutputTokens": 8192}
    }

    try:
        resp = requests.post(url, headers=headers, params=params, json=payload, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        candidates = data.get("candidates", [])
        if not candidates:
            raise ValueError("No candidates in Gemini response")
        content_parts = candidates[0].get("content", {}).get("parts", [])
        gemini_text = "\n".join(p.get("text", "") for p in content_parts)
        if not gemini_text:
            raise ValueError("Empty text from Gemini")
    except Exception as e:
        print(f"  ❌ Gemini API call failed: {e}")
        return grid, weighted_taxonomy

    try:
        gemini_text = gemini_text.strip()
        if gemini_text.startswith("```"):
            gemini_text = gemini_text.split("\n", 1)[1].rsplit("```", 1)[0].strip()
        corrections = json.loads(gemini_text)
    except json.JSONDecodeError as e:
        print(f"  ❌ Failed to parse Gemini JSON: {e}")
        print(f"  Raw Gemini output (first 500 chars): {gemini_text[:500]}")
        return grid, weighted_taxonomy

    corrected_grid = corrections.get("corrected_grid", [])
    multiplier_overrides = corrections.get("multiplier_overrides", {})

    grid_lookup = {c.get("taxonomy"): c for c in grid}

    for corr in corrected_grid:
        taxonomy = corr.get("taxonomy")
        if not taxonomy or taxonomy not in grid_lookup:
            continue
        entry = grid_lookup[taxonomy]
        ct = corr.get("correction_type", "confirmed")
        if ct == "confirmed":
            continue
        if ct in ("new_find", "reclassified"):
            entry["status"] = corr.get("corrected_status", entry.get("status"))
            if corr.get("event_date"):
                entry["event_date"] = corr["event_date"]
            if corr.get("evidence_excerpt"):
                entry["evidence_excerpt"] = corr["evidence_excerpt"]
            if corr.get("source_urls"):
                entry["source_urls"] = corr["source_urls"]
            if corr.get("impact_strength"):
                entry["impact_strength"] = corr["impact_strength"]
            if corr.get("corrected_multiplier"):
                entry["adjusted_weight"] = min(10, round(corr["corrected_multiplier"]))
            if corr.get("rationale"):
                entry["sensitivity_rationale"] = corr["rationale"]
        elif ct == "source_disputed":
            entry["confidence"] = min(entry.get("confidence", 50), 40)
            if corr.get("evidence_excerpt"):
                entry["evidence_excerpt"] = corr["evidence_excerpt"]
        elif ct == "date_corrected":
            entry["event_date"] = corr.get("event_date", entry.get("event_date"))
        elif ct in ("multiplier_override", "impact_adjusted"):
            if corr.get("corrected_multiplier"):
                entry["adjusted_weight"] = min(10, round(corr["corrected_multiplier"]))
            if corr.get("impact_strength"):
                entry["impact_strength"] = corr["impact_strength"]

    for catalyst, override in multiplier_overrides.items():
        if catalyst in weighted_taxonomy:
            new_mult = override.get("multiplier")
            if new_mult is not None:
                base = weighted_taxonomy[catalyst]["base_weight"]
                weighted_taxonomy[catalyst]["multiplier"] = new_mult
                weighted_taxonomy[catalyst]["adjusted_weight"] = max(0, min(10, round(base * new_mult)))
            if override.get("rationale"):
                weighted_taxonomy[catalyst]["rationale"] = override["rationale"]

    change_count = sum(1 for c in corrected_grid if c.get("correction_type") != "confirmed")
    mult_changes = len(multiplier_overrides)
    print(f"  ✅ Gemini applied {change_count} grid corrections and {mult_changes} multiplier overrides.")
    return grid, weighted_taxonomy

# ── GRANULAR SEARCH TEMPLATES ─────────────────────────
def _make_catalyst_templates(full_name):
    return [
        f"{full_name} contract award win 2025 2026",
        f"{full_name} contract expansion 2025 2026",
        f"{full_name} contract loss non-renewal 2025 2026",
        f"{full_name} strategic partnership 2025 2026",
        f"{full_name} alliance joint venture 2025 2026",
        f"{full_name} partnership dissolution 2025 2026",
        f"{full_name} new product launch 2025 2026",
        f"{full_name} FDA approval 2025 2026",
        f"{full_name} regulatory greenlight 2025 2026",
        f"{full_name} product delay failure 2025 2026",
        f"{full_name} product recall safety 2025 2026",
        f"{full_name} analyst upgrade 2025 2026",
        f"{full_name} analyst downgrade 2025 2026",
        f"{full_name} analyst price target increase 2025 2026",
        f"{full_name} analyst price target cut 2025 2026",
        f"{full_name} analyst initiation coverage 2025 2026",
        f"{full_name} CEO change departure 2025 2026",
        f"{full_name} CFO management change 2025 2026",
        f"{full_name} board member appointment 2025 2026",
        f"{full_name} executive resignation scandal 2025 2026",
        f"{full_name} capital raise PIPE 2025 2026",
        f"{full_name} funding round offering 2025 2026",
        f"{full_name} dilutive offering down round 2025 2026",
        f"{full_name} share buyback repurchase 2025 2026",
        f"{full_name} dividend increase 2025 2026",
        f"{full_name} dividend cut suspension 2025 2026",
        f"{full_name} earnings beat 2025 2026",
        f"{full_name} earnings miss 2025 2026",
        f"{full_name} earnings guidance raise 2025 2026",
        f"{full_name} earnings guidance cut 2025 2026",
        f"{full_name} revenue results 2025 2026",
        f"{full_name} EBITDA EPS results 2025 2026",
        f"{full_name} acquisition merger 2025 2026",
        f"{full_name} divestiture spin-off 2025 2026",
        f"{full_name} failed acquisition overpayment 2025 2026",
        f"{full_name} operational milestone 2025 2026",
        f"{full_name} capacity expansion factory 2025 2026",
        f"{full_name} operational setback trial halted 2025 2026",
        f"{full_name} production halt 2025 2026",
        f"{full_name} insider buying CEO CFO 2025 2026",
        f"{full_name} insider selling CEO CFO 2025 2026",
        f"{full_name} Form 4 SEC filing insider 2025 2026",
        f"{full_name} insider trading cluster sales 2025 2026",
        f"{full_name} activist investor 13D stake 2025 2026",
        f"{full_name} activist exits stake 2025 2026",
        f"{full_name} institutional ownership 13F increase 2025 2026",
        f"{full_name} institutional ownership decline 2025 2026",
        f"{full_name} supply chain disruption 2025 2026",
        f"{full_name} factory fire shipping 2025 2026",
        f"{full_name} supply chain de-risking reshoring 2025 2026",
        f"{full_name} patent grant 2025 2026",
        f"{full_name} patent litigation lawsuit 2025 2026",
        f"{full_name} IP theft 2025 2026",
        f"{full_name} sector tailwind 2025 2026",
        f"{full_name} sector headwind rotation 2025 2026",
        f"{full_name} commodity price impact 2025 2026",
        f"{full_name} tariff trade policy 2025 2026",
        f"{full_name} government subsidy mandate 2025 2026",
        f"{full_name} short interest 2025 2026",
        f"{full_name} short squeeze 2025 2026",
        f"{full_name} bear raid short attack 2025 2026",
        f"{full_name} technical breakout 2026",
        f"{full_name} technical breakdown death cross 2026",
        f"{full_name} geopolitical impact sanctions 2025 2026",
        f"{full_name} regulatory approval 2025 2026",
        f"{full_name} regulatory denial antitrust 2025 2026",
    ]

def _make_context_templates(full_name):
    return [
        f"{full_name} revenue breakdown by segment",
        f"{full_name} revenue by customer type",
        f"{full_name} revenue geography domestic international",
        f"{full_name} revenue government contracts percentage",
        f"{full_name} commercial vs consumer revenue mix",
        f"{full_name} customer concentration largest client",
        f"{full_name} customer concentration risk",
        f"{full_name} business model",
        f"{full_name} recurring revenue subscription",
        f"{full_name} cost structure input costs",
        f"{full_name} raw materials commodities exposure",
        f"{full_name} COGS breakdown",
        f"{full_name} supply chain manufacturing",
        f"{full_name} China exposure manufacturing",
        f"{full_name} supplier concentration risk",
        f"{full_name} operating leverage fixed variable costs",
        f"{full_name} margin structure",
        f"{full_name} competitive advantage moat",
        f"{full_name} market share",
        f"{full_name} pricing power",
        f"{full_name} industry barriers to entry",
        f"{full_name} switching costs",
        f"{full_name} competitors peer comparison",
        f"{full_name} debt structure maturity",
        f"{full_name} interest rate sensitivity floating fixed",
        f"{full_name} cash burn rate runway",
        f"{full_name} regulatory environment",
        f"{full_name} government contracts exposure",
        f"{full_name} CEO track record",
        f"{full_name} management capital allocation",
        f"{full_name} insider ownership percentage",
        f"{full_name} litigation risk pending lawsuits",
        f"{full_name} patent portfolio IP protection",
        f"{full_name} organic vs acquisitive growth",
        f"{full_name} order backlog visibility",
        f"{full_name} capacity expansion plans",
        f"{full_name} end market growth rate",
    ]

# ── Taxonomy ────────────────────────────────────────────
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

# ── AMPLIFIER / DAMPENER TABLE (re‑added) ───────────────
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

# ── Prompt Templates ───────────────────────────────────
STEP1_TEMPLATE = """
You are an event extraction engine for {full_name} ({ticker}).

BELOW ARE KNOWN EVENTS FROM FINVIZ (ticker‑verified, high confidence):
{finviz_events_json}

CRITICAL: These Finviz events are ALREADY CONFIRMED. Do NOT duplicate them.
Only extract NEW, DISTINCT events from the search results below that are
NOT already covered by the Finviz events.  Limit your output to at most 30
new events ― prioritise those with the highest impact.

Before extracting, verify that the snippet refers SPECIFICALLY to
{full_name} or its ticker {ticker}. If an article is about a different
company, DISCARD IT.

Rules:
- Describe each event in one sentence.
- Include the EXACT date (YYYY-MM-DD) from the snippet.
- Include a VERBATIM excerpt (≤150 chars, quoted) from the snippet.
- Include all source URLs related to the event.
- If multiple snippets describe the same event, merge them.
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

STEP2_TEMPLATE = """
You are a COMPANY CONTEXT ANALYST. Your inputs are:
1. A financial snapshot of {full_name} ({ticker}) from a database.
2. Search snippets about {full_name} ({ticker})'s business model,
   operations, and risks.

CRITICAL: Only use snippets that explicitly refer to {full_name} or {ticker}.
Discard any snippet about a different company.

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
   c) Top 3 customers and approximate revenue share?
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
You are a FINAL CATALYST SYNTHESIZER for {full_name} ({ticker}) on {today}.

INPUTS:
1. Finviz confirmed events (ticker‑verified, high confidence):
{finviz_events_json}

2. Additional raw events extracted from web searches:
{raw_events_json}

3. Weighted taxonomy (company‑context‑adjusted):
{weighted_taxonomy_json}

4. Financial snapshot:
{snapshot_json}

TASKS:
A. Merge Finviz events + raw events.  Give Finviz events a confidence boost
   (+10 over raw confidence).  Deduplicate by matching descriptions.

B. Classify every event into one or more catalyst categories from the
   taxonomy.  Use the EXACT taxonomy label.

C. Build the FULL catalyst grid (66 items).  Status: HIT / MISS / N/A.
   Use adjusted_weight from the weighted taxonomy for HITs.

D. Apply INTERACTION RULES:
   1. If "Insider selling (cluster)" HIT AND "Earnings beat" HIT
      within 14 days, reduce the beat's adjusted_weight by 1 and add
      "Insider-earnings divergence" (negative, weight=1).
   2. If float_short > 20% AND positive HITs dominate, add
      "Short squeeze potential" (positive, weight=3).
   3. If analyst target < current price, reclassify
      "Analyst upgrade/PT increase" as negative.
   4. If both "Technical breakdown" and "Earnings beat" are HIT,
      reduce breakdown's weight by 2.

E. Compute FINAL SCORES:
   - Positive_Score = sum(adjusted_weight × confidence/100) for positive HITs.
   - Negative_Score = sum(adjusted_weight × confidence/100) for negative HITs.
   - Net = Positive_Score − Negative_Score.
   - Net >= 20→Strong Bullish, >=8→Bullish, >=-8→Neutral, >=-20→Bearish,
     else→Strong Bearish.
   - Conviction = min(100, abs(Net) * 2)

F. Write a catalyst_stack (4 sentences, with dates).
G. Identify the key_assumption.

OUTPUT FORMAT: Return ONLY this JSON. (Every catalyst must appear.)
{{
  "ticker": "{ticker}",
  "analysis_date": "{today}",
  "current_price": "...",
  "catalyst_grid": [ ... ],
  "catalyst_stack": "...",
  "net_signal": "...",
  "conviction": 0-100,
  "key_assumption": "..."
}}
"""

# ── Helpers for prompt formatting ──────────────────────
def _format_step1(full_name, ticker, today, lookback_start, search_results_json, finviz_events_json, taxonomy_list_str):
    return STEP1_TEMPLATE.format(
        full_name=full_name, ticker=ticker, today=today,
        lookback_start=lookback_start, search_results_json=search_results_json,
        finviz_events_json=finviz_events_json, taxonomy_list_str=taxonomy_list_str,
    )

def _format_step2(full_name, ticker, snapshot, context_search_results, amp_damp_table, taxonomy_list_str):
    return STEP2_TEMPLATE.format(
        full_name=full_name, ticker=ticker,
        snapshot=json.dumps(snapshot, indent=2, default=str),
        context_search_results=context_search_results,
        amp_damp_table=amp_damp_table, taxonomy_list_str=taxonomy_list_str,
    )

def _format_step4(full_name, ticker, today, raw_events_json, finviz_events_json, weighted_taxonomy_json, snapshot_json):
    return STEP4_TEMPLATE.format(
        full_name=full_name, ticker=ticker, today=today,
        raw_events_json=raw_events_json, finviz_events_json=finviz_events_json,
        weighted_taxonomy_json=weighted_taxonomy_json,
        snapshot_json=snapshot_json,
    )

# ── Robust JSON parser ──────────────────────────────────
def parse_json(raw):
    """Parse JSON, handling common LLM output quirks including truncation."""
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
    in_string = False
    escaped = False
    last_quote_pos = -1
    for i, ch in enumerate(cleaned):
        if escaped: escaped = False; continue
        if ch == '\\': escaped = True; continue
        if ch == '"': in_string = not in_string; last_quote_pos = i
    if in_string and last_quote_pos > 0:
        truncated = cleaned[:last_quote_pos + 1] + '"]'
        open_brackets = truncated.count("[") - truncated.count("]")
        open_braces = truncated.count("{") - truncated.count("}")
        truncated += "]" * open_brackets + "}" * open_braces
        try:
            return json.loads(truncated)
        except json.JSONDecodeError:
            pass
    open_brackets = cleaned.count("[") - cleaned.count("]")
    open_braces = cleaned.count("{") - cleaned.count("}")
    if open_brackets > 0 or open_braces > 0:
        truncated = cleaned + "]" * open_brackets + "}" * open_braces
        try:
            return json.loads(truncated)
        except json.JSONDecodeError:
            pass
    try:
        fix_resp = safe_create(
            model=MODEL,
            messages=[
                {"role": "system", "content": "Return ONLY valid JSON. Close all open brackets/braces."},
                {"role": "user", "content": f"Fix this JSON:\n\n{raw[:20000]}"}
            ],
            temperature=0.0, max_tokens=500
        )
        fixed2 = fix_resp.choices[0].message.content.strip()
        if fixed2.startswith("```"):
            fixed2 = fixed2.split("\n", 1)[1].rsplit("```", 1)[0].strip()
        return json.loads(fixed2)
    except Exception:
        pass
    raise ValueError(f"All JSON repair strategies failed. Raw start: {raw[:200]}")

# ── LLM caller ─────────────────────────────────────────
def call_llm(prompt, user_msg, temperature=0.3, max_tokens=40000):
    messages = [{"role": "system", "content": prompt}, {"role": "user", "content": user_msg}]
    resp = safe_create(model=MODEL, messages=messages, temperature=temperature, max_tokens=max_tokens)
    return resp.choices[0].message.content.strip()

# ── Recalculate signal ──────────────────────────────────
def recalculate_signal(grid):
    pos_score = sum(c.get("adjusted_weight", 0) * c.get("confidence", 50) / 100
                    for c in grid if c.get("status") == "HIT" and c.get("type") == "positive")
    neg_score = sum(c.get("adjusted_weight", 0) * c.get("confidence", 50) / 100
                    for c in grid if c.get("status") == "HIT" and c.get("type") == "negative")
    net = pos_score - neg_score
    if net >= 20: signal = "Strong Bullish"
    elif net >= 8: signal = "Bullish"
    elif net >= -8: signal = "Neutral"
    elif net >= -20: signal = "Bearish"
    else: signal = "Strong Bearish"
    conviction = min(100, int(abs(net) * 2))
    return signal, conviction

# ── Async analysis pipeline ────────────────────────────
async def analyze_stock_async(ticker, snapshot, searxng_url):
    # Resolve company name
    db_name = snapshot.get("profile", {}).get("company_name", "")
    if db_name and db_name.lower() != ticker.lower() and len(db_name) > 2:
        official_name = db_name
        aliases = []
        print(f"  🏢 Using DB company name: {official_name}")
    else:
        official_name, aliases = resolve_company_name(ticker, searxng_url)

    if official_name.lower() != ticker.lower():
        full_name = f"{official_name} ({ticker})"
    else:
        full_name = ticker

    taxonomy_list_str = "\n".join(TAXONOMY_LIST)

    # ═══ Phase 0.5: Finviz news scrape ═══
    print("  📰 Scraping Finviz news…")
    finviz_events = scrape_finviz_news(ticker)
    print(f"  📰 Finviz returned {len(finviz_events)} headlines")
    # PRINT THE FIRST N HEADLINES FOR VERIFICATION
    print("\n  ── FINVIZ HEADLINES (first 10) ──")
    for i, ev in enumerate(finviz_events[:10]):
        print(f"    [{i+1}] {ev['event_date']} | {ev.get('finviz_source','?')} | {ev['description'][:120]}")
    if len(finviz_events) > 10:
        print(f"    ... and {len(finviz_events) - 10} more")

    # ═══ Phase 1 & 2: Granular Searches ═══
    catalyst_queries = _make_catalyst_templates(full_name)
    context_queries = _make_context_templates(full_name)

    print(f"\n  Full name for searches: {full_name}")
    print(f"  ⏳ Preparing {len(catalyst_queries)} catalyst + {len(context_queries)} context search queries…")

    print(f"\n  🔎 Launching all {len(catalyst_queries) + len(context_queries)} searches in parallel…")
    catalyst_task = batch_search(catalyst_queries, searxng_url)
    context_task = batch_search(context_queries, searxng_url)
    catalyst_results, context_results = await asyncio.gather(catalyst_task, context_task)

    # ── FILTER irrelevant snippets ──
    catalyst_results = _filter_search_results(catalyst_results, official_name, ticker, aliases)
    context_results = _filter_search_results(context_results, official_name, ticker, aliases)

    cat_with = sum(1 for v in catalyst_results.values() if v not in ("NO_RELEVANT_RESULTS",) and not v.startswith("NO_RESULTS") and not v.startswith("SEARCH_ERROR"))
    ctx_with = sum(1 for v in context_results.values() if v not in ("NO_RELEVANT_RESULTS",) and not v.startswith("NO_RESULTS") and not v.startswith("SEARCH_ERROR"))
    print(f"  ✅ Catalyst searches: {cat_with}/{len(catalyst_queries)} with relevant results")
    print(f"  ✅ Context searches:  {ctx_with}/{len(context_queries)} with relevant results")

    # ── DUMP SEARCH RESULTS ──
    print("\n" + "="*80)
    print("  🔎 CATALYST SEARCH RESULTS (relevance‑filtered)")
    print("="*80)
    for q, v in catalyst_results.items():
        if v not in ("NO_RELEVANT_RESULTS",) and not v.startswith("NO_RESULTS") and not v.startswith("SEARCH_ERROR"):
            print(f"\n  QUERY: {q}")
            print(f"  RESULT: {v[:400]}{'...' if len(v)>400 else ''}")
            print("  ──")
    print("\n" + "="*80)
    print("  🔎 CONTEXT SEARCH RESULTS (relevance‑filtered)")
    print("="*80)
    for q, v in context_results.items():
        if v not in ("NO_RELEVANT_RESULTS",) and not v.startswith("NO_RESULTS") and not v.startswith("SEARCH_ERROR"):
            print(f"\n  QUERY: {q}")
            print(f"  RESULT: {v[:400]}{'...' if len(v)>400 else ''}")
            print("  ──")

    search_results_str = "\n\n".join([f"Query: {q}\n{v}" for q, v in catalyst_results.items()])
    finviz_json = json.dumps(finviz_events, indent=2)
    prompt1 = _format_step1(full_name, ticker, TODAY, LOOKBACK_START, search_results_str, finviz_json, taxonomy_list_str)

    context_str = "\n\n".join([f"Query: {q}\n{v}" for q, v in context_results.items()])
    prompt2 = _format_step2(full_name, ticker, snapshot, context_str, AMP_DAMP_TABLE, taxonomy_list_str)

    print("  🧠 Running Step 1 (event extraction) and Step 2 (context sensitivity) in parallel…")
    step1_task = asyncio.to_thread(call_llm, prompt=prompt1, user_msg=f"Extract NEW events for {full_name}.", temperature=0.3, max_tokens=40000)
    step2_task = asyncio.to_thread(call_llm, prompt=prompt2, user_msg=f"Context for {full_name}.", temperature=0.3, max_tokens=40000)
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
        print(f"  📋 Step 1 extracted {len(raw_events)} NEW raw events")
        print("\n" + "="*80)
        print("  📋 NEW RAW EVENTS (STEP 1 OUTPUT)")
        print("="*80)
        for i, ev in enumerate(raw_events):
            print(f"  [{i+1}] Date: {ev.get('event_date','?')}")
            print(f"      Description: {ev.get('description','')}")
            print(f"      Excerpt: {ev.get('evidence_excerpt','')[:100]}…")
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

    extracted = context_profile.get("extracted_context", {})
    if extracted:
        print("\n" + "="*80)
        print("  📝 EXTRACTED CONTEXT (STEP 2 PART 1)")
        print("="*80)
        for section, answers in extracted.items():
            print(f"\n  [{section}]")
            for q, a in answers.items():
                print(f"    {q}: {a}")

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
            "base_weight": base, "multiplier": multiplier,
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
        full_name=full_name, ticker=ticker, today=TODAY,
        raw_events_json=json.dumps(raw_events, indent=2),
        finviz_events_json=json.dumps(finviz_events, indent=2),
        weighted_taxonomy_json=json.dumps(weighted_taxonomy, indent=2),
        snapshot_json=json.dumps(snapshot, indent=2, default=str)
    )
    print("  🧠 Running Step 4 (final synthesis)…")
    final_raw = call_llm(prompt=prompt4, user_msg=f"Finalize {full_name}.", temperature=0.3, max_tokens=40000)
    try:
        final_result = parse_json(final_raw)
    except Exception as e:
        print(f"  ❌ Failed to parse Step 4 JSON: {e}")
        return {"error": "Step 4 parse failure", "raw": final_raw[:500]}

    # Phase 5: Gemini Fact‑Check
    grid = final_result.get("catalyst_grid", [])
    corrected_grid, updated_taxonomy = gemini_factcheck(
        full_name=full_name, ticker=ticker, grid=grid, snapshot=snapshot,
        weighted_taxonomy=weighted_taxonomy, taxonomy_list=TAXONOMY_LIST
    )

    # Phase 6: Recalculate
    final_result["catalyst_grid"] = corrected_grid
    new_signal, new_conviction = recalculate_signal(corrected_grid)
    final_result["net_signal"] = new_signal
    final_result["conviction"] = new_conviction
    print(f"  🔄 Post‑Gemini signal: {new_signal} (conviction {new_conviction})")

    return final_result

# ── Main wrapper ────────────────────────────────────────
def analyze_stock(ticker, snapshot, searxng_url):
    return asyncio.run(analyze_stock_async(ticker, snapshot, searxng_url))

# ── Main loop ──────────────────────────────────────────
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
