#!/usr/bin/env python3
"""
Catalyst Analysis Engine v2 – Backtest-Capable, Robust Gemini, Full Transparency.
- Ticker(s) and cutoff date set at the top of the file.
- Gemini calls via Google GenAI SDK (the working method).
- CUTOFF_DATE discards all events after the specified date (None = live).
- Hardened JSON parser for Gemini responses.
- Prints every HIT with headline, event ID, and source link.
"""

import os, json, time, re, asyncio, aiohttp
from datetime import datetime, date, timedelta, timezone
from openai import OpenAI
from google import genai
from google.genai import types as google_types

# ═══════════════════════════════════════════════════════
#  EDIT HERE – TICKER(S) AND BACKTEST CUTOFF DATE
# ═══════════════════════════════════════════════════════
TICKERS = ["SERV"]
CUTOFF_DATE = "2026-03-15"          # e.g. "2026-03-15" — discard events after this; set to None for live
# ═══════════════════════════════════════════════════════

# ── Config ──────────────────────────────────────────────
SEARXNG_URL          = os.environ["SEARXNG_URL"]
SEARXNG_TIMEOUT      = 15
SEARCH_CONCURRENCY   = 6
MODEL                = "deepseek-chat"
GEMINI_API_KEY       = os.environ.get("GEMINI_API_KEY", "")
GEMINI_MODEL         = "gemini-2.5-flash"
TODAY                = date.today().isoformat()
LOOKBACK_START       = (date.today() - timedelta(days=185)).isoformat()

# Convert CUTOFF_DATE to string for easy comparison
CUTOFF_DATE = CUTOFF_DATE.strip() if CUTOFF_DATE else None

# ── Gemini health check (Google GenAI SDK) ──────────────
def gemini_health_check():
    if not GEMINI_API_KEY:
        return False
    try:
        client = genai.Client(api_key=GEMINI_API_KEY)
        tools = google_types.Tool(google_search=google_types.GoogleSearch())
        response = client.models.generate_content(
            model=GEMINI_MODEL,
            contents="Google Search: What is the current stock price of Apple (AAPL)?",
            config=google_types.GenerateContentConfig(
                tools=[tools],
                temperature=0.0,
                max_output_tokens=128,
            ),
        )
        return response.candidates is not None and len(response.candidates) > 0
    except Exception as e:
        print(f"  ⚠️  Gemini health check error: {e}")
        return False

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
            for r in results[:4]:
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

# ── Relevance filter ────────────────────────────────────
def _snippet_is_relevant(snippet_text, full_name, ticker, aliases=None):
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
    filtered = {}
    for query, text in results_dict.items():
        if text.startswith("NO_RESULTS") or text.startswith("SEARCH_ERROR"):
            filtered[query] = text
        elif _snippet_is_relevant(text, full_name, ticker, aliases):
            filtered[query] = text
        else:
            filtered[query] = "NO_RELEVANT_RESULTS"
    return filtered

# ── Company Name Resolution ─────────────────────────────
async def _resolve_company_name_async(ticker, searxng_url):
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
                    m = re.search(rf"{ticker.lower()}\s*[|–\-—]\s*([A-Z][A-Za-z\s]+?)(?:\s+Stock|\s+Inc|\s+Corp|\s+Profile|,)", combined)
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
            "sector": prof[1], "industry": prof[2],
            "country": prof[3], "description": prof[4],
        }
    else:
        profile = {
            "company_name": db_company or ticker,
            "sector": finviz.get("sector"), "industry": finviz.get("industry"),
            "country": finviz.get("country"), "description": "",
        }
    cur.close()
    return {"profile": profile, "finviz": finviz}

# ── LLM setup ──────────────────────────────────────────
client = OpenAI(api_key=os.environ.get("DEEPSEEK_API_KEY"), base_url="https://api.deepseek.com")

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

# ── Finviz news scrape ──────────────────────────────────
def scrape_finviz_news(ticker):
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
            # Apply cutoff
            if CUTOFF_DATE and date_str > CUTOFF_DATE:
                continue
            title = str(row.get('Title', ''))
            source = str(row.get('Source', ''))
            link = str(row.get('Link', ''))
            urls = [link] if link and link.startswith('http') else [f"https://finviz.com/quote.ashx?t={ticker}"]
            events.append({
                "event_date": date_str,
                "headline": title,
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

# ── Gemini unified call (Google GenAI SDK – robust) ─────
def gemini_robust_parse_json(text):
    """Try multiple strategies to parse Gemini's JSON output."""
    text = text.strip()
    if text.startswith("```"):
        text = text.split("\n", 1)[1].rsplit("```", 1)[0].strip()
    
    # Attempt 1: direct
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    # Attempt 2: fix missing commas between array elements (e.g. `} {` → `}, {`)
    fixed = re.sub(r'\}\s*\{', '}, {', text)
    try:
        return json.loads(fixed)
    except json.JSONDecodeError:
        pass

    # Attempt 3: remove trailing commas before ] or }
    cleaned = re.sub(r",\s*]", "]", fixed)
    cleaned = re.sub(r",\s*}", "}", cleaned)
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        pass

    # Attempt 4: find the outermost JSON object/array and try to parse that substring
    start = text.find('{')
    end = text.rfind('}')
    if start != -1 and end != -1 and end > start:
        substr = text[start:end+1]
        try:
            return json.loads(substr)
        except json.JSONDecodeError:
            pass

    # Attempt 5: try to close unclosed brackets/braces
    ob = text.count("[") - text.count("]")
    cb = text.count("{") - text.count("}")
    if ob > 0 or cb > 0:
        closed = text + "]"*ob + "}"*cb
        try:
            return json.loads(closed)
        except json.JSONDecodeError:
            pass

    raise ValueError(f"Failed to parse Gemini JSON. Start: {text[:200]}")

def gemini_unified_check(full_name, ticker, grid, snapshot, weighted_taxonomy, taxonomy_list):
    if not GEMINI_API_KEY:
        return grid, weighted_taxonomy, []

    print("  🔍 Running lightweight Gemini fact‑check + MISS gap‑filler…")

    miss_items = [c for c in grid if c.get("status") == "MISS"]
    hit_items  = [c for c in grid if c.get("status") == "HIT"]
    compact_hits = [{
        "taxonomy": c.get("taxonomy"),
        "event_date": c.get("event_date"),
        "evidence_excerpt": c.get("evidence_excerpt","")[:120],
        "source_urls": c.get("source_urls",[])[:2]
    } for c in hit_items]

    prompt = f"""
You are a financial fact‑checker for {full_name} (ticker: {ticker}).
TODAY is {TODAY}. Lookback: {LOOKBACK_START} to {TODAY}.

MISS CATALYSTS (search Google for evidence and return new HITs if found):
{json.dumps([c.get("taxonomy") for c in miss_items], indent=2)}

HIT CATALYSTS (verify they are about {full_name} and correctly classified):
{json.dumps(compact_hits, indent=2)}

Return ONLY this tiny JSON:
{{
  "new_hits_from_miss": [ {{"taxonomy":"...","event_date":"...","evidence_excerpt":"≤120 chars","source_urls":["..."],"confidence":80}} ],
  "corrected_hits": [ {{"taxonomy":"...","original_status":"HIT","corrected_status":"MISS|HIT","correction_type":"source_disputed|reclassified","rationale":"short"}} ]
}}
"""

    try:
        gemini_client = genai.Client(api_key=GEMINI_API_KEY)
        tools = google_types.Tool(google_search=google_types.GoogleSearch())

        for attempt in range(3):
            try:
                response = gemini_client.models.generate_content(
                    model=GEMINI_MODEL,
                    contents=prompt,
                    config=google_types.GenerateContentConfig(
                        tools=[tools],
                        temperature=0.1,
                        max_output_tokens=4096,
                    ),
                )
                if not response.candidates:
                    raise ValueError("No candidates returned")
                parts = response.candidates[0].content.parts
                text = "".join(p.text for p in parts if p.text).strip()
                break
            except Exception as e:
                error_str = str(e)
                if "429" in error_str or "RESOURCE_EXHAUSTED" in error_str:
                    wait = 2 ** attempt
                    print(f"  ⚠️  Gemini rate‑limited (429), waiting {wait}s…")
                    time.sleep(wait)
                    continue
                else:
                    raise
        else:
            print("  ❌ All Gemini attempts failed; skipping Gemini corrections.")
            return grid, weighted_taxonomy, []

    except Exception as e:
        print(f"  ⚠️  Gemini SDK call failed: {e}")
        return grid, weighted_taxonomy, []

    # Parse response
    try:
        result = gemini_robust_parse_json(text)
    except Exception as e:
        print(f"  ❌ Failed to parse Gemini JSON: {e}")
        return grid, weighted_taxonomy, []

    # Apply corrections
    corrected_hits = result.get("corrected_hits", [])
    new_hits = result.get("new_hits_from_miss", [])

    grid_lookup = {c.get("taxonomy"): c for c in grid}
    for corr in corrected_hits:
        t = corr.get("taxonomy")
        if not t or t not in grid_lookup: continue
        entry = grid_lookup[t]
        if corr.get("corrected_status") == "MISS":
            entry["status"] = "MISS"
            entry["adjusted_weight"] = 0
            entry["event_date"] = None
            entry["evidence_excerpt"] = ""
            entry["source_urls"] = []
            entry["confidence"] = 0
        elif corr.get("corrected_status") == "HIT":
            entry["status"] = "HIT"

    for hit in new_hits:
        hit["status"] = "HIT"
        hit["type"] = "positive" if hit.get("taxonomy") in POSITIVE_CATALYSTS else "negative"
        hit["base_weight"] = CATALYST_WEIGHTS.get(hit["taxonomy"], 5)
        hit["adjusted_weight"] = round(hit["base_weight"] * weighted_taxonomy.get(hit["taxonomy"], {}).get("multiplier", 1.0))
        hit.setdefault("confidence", 80)
        hit.setdefault("event_date", "?")
        hit.setdefault("source_urls", [])
        hit.setdefault("evidence_excerpt", "")
        hit.setdefault("headline", "")
        grid.append(hit)

    print(f"  ✅ Gemini applied {len(corrected_hits)} corrections, found {len(new_hits)} new MISS HITs.")
    return grid, weighted_taxonomy, new_hits

# ── Search templates (unchanged) ────────────────────────


def _make_catalyst_templates(full_name):
    return [
        f"{full_name} contract partnership agreement 2025 2026",
        f"{full_name} strategic alliance joint venture 2025 2026",
        f"{full_name} product launch FDA approval regulatory greenlight 2025 2026",
        f"{full_name} analyst upgrade downgrade price target initiation 2025 2026",
        f"{full_name} CEO CFO management change appointment departure 2025 2026",
        f"{full_name} capital raise PIPE funding round offering 2025 2026",
        f"{full_name} share buyback repurchase dividend increase 2025 2026",
        f"{full_name} earnings beat miss revenue EBITDA EPS results 2025 2026",
        f"{full_name} earnings guidance raise cut outlook 2025 2026",
        f"{full_name} acquisition merger divestiture spin-off 2025 2026",
        f"{full_name} operational milestone capacity expansion 2025 2026",
        f"{full_name} product failure recall safety issue 2025 2026",
        f"{full_name} insider buying selling CEO CFO Form 4 2025 2026",
        f"{full_name} activist investor 13D stake accumulation exit 2025 2026",
        f"{full_name} institutional ownership 13F filing increase decrease 2025 2026",
        f"{full_name} supply chain disruption factory fire shipping 2025 2026",
        f"{full_name} patent grant litigation IP theft lawsuit 2025 2026",
        f"{full_name} sector tailwind headwind rotation 2025 2026",
        f"{full_name} commodity price impact input cost 2025 2026",
        f"{full_name} tariff trade policy impact 2025 2026",
        f"{full_name} government subsidy mandate regulation 2025 2026",
        f"{full_name} short interest short squeeze bear raid 2025 2026",
        f"{full_name} technical breakout breakdown death cross 2026",
        f"{full_name} geopolitical impact sanctions conflict 2025 2026",
        f"{full_name} regulatory approval denial antitrust block 2025 2026",
        f"{full_name} revenue growth customer concentration 2025 2026",
        f"{full_name} cost structure margin profitability 2025 2026",
        f"{full_name} competitive position market share moat 2025 2026",
        f"{full_name} debt financing liquidity cash burn 2025 2026",
        f"{full_name} litigation lawsuit investigation 2025 2026",
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

# ── Taxonomy & Weights (unchanged) ──────────────────────
TAXONOMY_LIST = [
    "Contract win/expansion", "Strategic partnership/alliance",
    "Product launch/FDA approval/regulatory greenlight", "Analyst upgrade/PT increase",
    "Positive personnel change (new CEO, CFO, board members)",
    "Capital infusion (PIPE, funding round, favorable terms)",
    "Earnings beat (revenue, EBITDA, EPS)", "Earnings guidance raise",
    "Share repurchase program/increased dividend", "Successful acquisition/synergy realization",
    "Deleveraging/sale of toxic assets/spin-off of loss-making unit",
    "Operational milestone (e.g., first patient dosed, satellite commissioned)",
    "Insider buying (cluster purchases by executives/directors)",
    "Activist investor accumulation (e.g., 9.9% stake filing)",
    "Capacity expansion announced (new factory, satellite constellation)",
    "Strategic pivot/rebranding to high-growth area",
    "Supply chain de-risking (dual sourcing, reshoring)", "Patent grant/IP protection",
    "Customer concentration expansion (existing customer deepens relationship)",
    "Government policy (tariffs, subsidies, mandates)",
    "Institutional policy (Fed rate cut, QE, stimulus)", "Favorable court ruling/patent grant",
    "Geopolitical event that boosts sector (e.g., defense spending surge)",
    "Sector tailwind/index inclusion", "Regulatory approval (FDA, FCC, FTC clearance)",
    "Macro tailwinds (CPI cooling, GDP growth surprise, soft landing)",
    "Sector rotation into the stock's industry", "Commodity price move favorable to the company",
    "ESG mandate/green subsidy qualification", "Currency tailwind (stronger home currency if importing)",
    "Technical breakout (above key moving averages, resistance levels)",
    "Short squeeze (rapid covering of heavily shorted stock)",
    "Institutional ownership increase (13F filings showing accumulation)",
    "Contract loss/non-renewal/reduction in scope", "Partnership dissolution/breakdown/rival alliance",
    "Product delay/failure/rejection/safety recall", "Analyst downgrade/price target cut",
    "Negative personnel change (departures, resignations, scandals)",
    "Dilutive offering/distressed fundraising/down round", "Earnings miss (revenue, EBITDA, EPS)",
    "Earnings guidance cut", "Suspension of buyback/dividend cut/elimination",
    "Failed acquisition/overpayment/goodwill impairment",
    "Accumulation of debt/retention or deepening of toxic assets/failed divestiture",
    "Operational setback (trial halted, satellite failure, production halt)",
    "Insider selling (especially by CEO/CFO, or clustered sales)",
    "Activist exits stake/files hostile 13D to force changes",
    "Capacity underutilization/overexpansion write-down", "Strategic pivot failure/loss of identity",
    "Supply chain shock (factory fire, shipping disruption)", "Patent litigation loss/IP theft",
    "Customer concentration risk (over-reliance on one client)",
    "Policy reversal/new regulation/tax increase", "Rate hike/monetary tightening/liquidity withdrawal",
    "Adverse litigation outcome/patent invalidation/antitrust ruling",
    "Geopolitical event that hurts sector (sanctions, conflict disrupting supply chain)",
    "Sector headwind/index exclusion/rotation away", "Regulatory denial or delay/antitrust block",
    "Macro headwinds (inflation spike, recession, unemployment surge)",
    "Sector rotation out of the industry", "Unfavorable commodity price move (higher input costs)",
    "ESG controversy/exclusion from green funds/carbon tax",
    "Currency headwind (dollar strength for exporters)",
    "Technical breakdown (below support, \"death cross\")",
    "Short attack/bear raid (activist short report)/large new short positions",
    "Institutional ownership decline (major holders reducing stakes)",
]

CATALYST_WEIGHTS = {
    "Contract win/expansion": 8, "Strategic partnership/alliance": 6,
    "Product launch/FDA approval/regulatory greenlight": 9, "Analyst upgrade/PT increase": 5,
    "Positive personnel change (new CEO, CFO, board members)": 4,
    "Capital infusion (PIPE, funding round, favorable terms)": 6,
    "Earnings beat (revenue, EBITDA, EPS)": 8, "Earnings guidance raise": 7,
    "Share repurchase program/increased dividend": 5, "Successful acquisition/synergy realization": 7,
    "Deleveraging/sale of toxic assets/spin-off of loss-making unit": 5,
    "Operational milestone (e.g., first patient dosed, satellite commissioned)": 6,
    "Insider buying (cluster purchases by executives/directors)": 7,
    "Activist investor accumulation (e.g., 9.9% stake filing)": 7,
    "Capacity expansion announced (new factory, satellite constellation)": 6,
    "Strategic pivot/rebranding to high-growth area": 5,
    "Supply chain de-risking (dual sourcing, reshoring)": 5, "Patent grant/IP protection": 4,
    "Customer concentration expansion (existing customer deepens relationship)": 5,
    "Government policy (tariffs, subsidies, mandates)": 7,
    "Institutional policy (Fed rate cut, QE, stimulus)": 9, "Favorable court ruling/patent grant": 8,
    "Geopolitical event that boosts sector (e.g., defense spending surge)": 7,
    "Sector tailwind/index inclusion": 5, "Regulatory approval (FDA, FCC, FTC clearance)": 9,
    "Macro tailwinds (CPI cooling, GDP growth surprise, soft landing)": 8,
    "Sector rotation into the stock's industry": 6, "Commodity price move favorable to the company": 6,
    "ESG mandate/green subsidy qualification": 4, "Currency tailwind (stronger home currency if importing)": 3,
    "Technical breakout (above key moving averages, resistance levels)": 4,
    "Short squeeze (rapid covering of heavily shorted stock)": 8,
    "Institutional ownership increase (13F filings showing accumulation)": 6,
    "Contract loss/non-renewal/reduction in scope": 8, "Partnership dissolution/breakdown/rival alliance": 6,
    "Product delay/failure/rejection/safety recall": 9, "Analyst downgrade/price target cut": 5,
    "Negative personnel change (departures, resignations, scandals)": 5,
    "Dilutive offering/distressed fundraising/down round": 7, "Earnings miss (revenue, EBITDA, EPS)": 8,
    "Earnings guidance cut": 7, "Suspension of buyback/dividend cut/elimination": 5,
    "Failed acquisition/overpayment/goodwill impairment": 6,
    "Accumulation of debt/retention or deepening of toxic assets/failed divestiture": 6,
    "Operational setback (trial halted, satellite failure, production halt)": 7,
    "Insider selling (especially by CEO/CFO, or clustered sales)": 7,
    "Activist exits stake/files hostile 13D to force changes": 7,
    "Capacity underutilization/overexpansion write-down": 5, "Strategic pivot failure/loss of identity": 5,
    "Supply chain shock (factory fire, shipping disruption)": 8, "Patent litigation loss/IP theft": 7,
    "Customer concentration risk (over-reliance on one client)": 6,
    "Policy reversal/new regulation/tax increase": 7, "Rate hike/monetary tightening/liquidity withdrawal": 9,
    "Adverse litigation outcome/patent invalidation/antitrust ruling": 8,
    "Geopolitical event that hurts sector (sanctions, conflict disrupting supply chain)": 8,
    "Sector headwind/index exclusion/rotation away": 6, "Regulatory denial or delay/antitrust block": 9,
    "Macro headwinds (inflation spike, recession, unemployment surge)": 8,
    "Sector rotation out of the industry": 6, "Unfavorable commodity price move (higher input costs)": 6,
    "ESG controversy/exclusion from green funds/carbon tax": 4,
    "Currency headwind (dollar strength for exporters)": 3,
    "Technical breakdown (below support, \"death cross\")": 5,
    "Short attack/bear raid (activist short report)/large new short positions": 8,
    "Institutional ownership decline (major holders reducing stakes)": 6,
}

POSITIVE_CATALYSTS = set([k for k in CATALYST_WEIGHTS if k in TAXONOMY_LIST and TAXONOMY_LIST.index(k) < 33])

# ── Prompt templates ───────────────────────────────────
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

AMP_DAMP_TABLE = """Contract win/expansion: [+] High customer concentration, low past revenue growth, small market cap [−] Diversified customer base, large cap, contract small relative to revenue
Strategic partnership/alliance: [+] Niche industry with high barriers, low institutional ownership, high R&D [−] Many existing partnerships, low switching costs
Product launch/FDA approval/regulatory greenlight: [+] Biotech/pharma sector, single-product, low cash, high short interest [−] Diversified product portfolio, large cap, approval widely expected
Analyst upgrade/PT increase: [+] Low analyst coverage, stock near 52-week low, low inst ownership, high short float, PT above current price [−] High coverage, PT still below current price (reclassify negative)
Positive personnel change: [+] Company in distress, recent scandals, high insider ownership [−] Stable company, routine appointment, large cap
Capital infusion: [+] High debt, low cash, negative FCF, high short interest [−] Already cash-rich, infusion is dilutive
Earnings beat: [+] High short interest, stock beaten down, low expectations [−] Stock rallied into earnings, beat narrow, peers also beat
Earnings guidance raise: [+] Same as beat + CEO credibility, analyst lag [−] Raise expected, macro tailwinds obvious, raise small
Share repurchase/dividend increase: [+] High cash, low debt, undervalued (P/B < 1), insider buying alongside [−] Low cash, high debt, token repurchase, dividend cut history
Successful acquisition/synergy realization: [+] Recent acquisition, synergy ahead of plan, accretive [−] Integration risk, overpayment history, small deal
Deleveraging/sale of toxic assets/spin-off: [+] High debt, negative credit outlook, toxic assets [−] Already well-capitalised, sale of core asset
Operational milestone: [+] Pre-revenue (biotech/space), regulatory catalyst pending, high R&D [−] Routine maintenance, non-value-creating
Insider buying (cluster): [+] High insider ownership already, buying after crash, multiple C-suite [−] Small amounts, one insider, buying at ATH, option exercise
Activist investor accumulation: [+] Underperforming, high cash, breakup value > market cap, low inst ownership [−] Management addressing issues, activist poor track record
Capacity expansion: [+] High utilisation, growing backlogs, sector demand surging, high margins [−] Industry overcapacity, debt-funded, demand weakening
Strategic pivot/rebranding: [+] Old business declining, high debt (pivot desperate), CEO credible [−] Stable business, pivot faddish, execution risk high
Supply chain de-risking: [+] High China exposure, tariff sensitivity, recent supply shocks [−] Already diversified, de-risking costly
Patent grant/IP protection: [+] Tech/pharma, high R&D, history of IP theft, narrow moat [−] Many patents already, patent narrow, workaround easy
Customer concentration expansion: [+] High customer concentration currently, expansion to new sectors [−] Already diversified, new customer immaterial
Government policy (tariffs/subsidies/mandates): [+] Sector directly affected, domestic capacity, bipartisan support [−] Policy temporary, company relies on imports, unfunded
Institutional policy (Fed rate cut/QE/stimulus): [+] High debt, floating-rate, growth/tech, low cash flow [−] Low debt, fixed-rate, cut already priced in
Favorable court ruling/patent grant: [+] Litigation priced in, binary outcome, damages large [−] Ruling narrow, appeal likely, stock didn't move
Geopolitical event that boosts sector: [+] Defence sector, domestic production, govt contract exposure [−] Event temporary, indirect benefit
Sector tailwind/index inclusion: [+] Small cap added to major index, sector ETF inflows, low liquidity [−] Already in index, inclusion priced in, momentum exhausted
Regulatory approval: [+] Binary event, no alternatives, high legal costs if denied [−] Approval expected, minimal incremental revenue
Macro tailwinds: [+] High cyclicality, beta > 1.5, high operating leverage [−] Defensive sector, tailwind temporary
Sector rotation into industry: [+] Underperformed long, low relative valuations, low inst ownership [−] Rotation already happened, industry still overvalued
Commodity price move favorable: [+] High commodity sensitivity, unhedged, producer [−] Fully hedged, commodity small input cost
ESG mandate/green subsidy: [+] Renewable/green sector, high capital requirements [−] Already funded, subsidy small
Currency tailwind: [+] High export %, high foreign revenue, unhedged [−] Hedged, import costs offset, small foreign exposure
Technical breakout: [+] High short interest, breakout on volume, long downtrend before [−] Low volume breakout, already overbought
Short squeeze: [+] Short float > 20%, days-to-cover > 4, positive catalyst cluster [−] Short float < 10%, no positive catalyst
Institutional ownership increase: [+] Low inst ownership, concentrated fund, recent decline [−] Already highly owned, passive flow
Contract loss/non-renewal: [+] High customer concentration, contract large % revenue [−] Diversified, contract small
Partnership dissolution: [+] Partner critical, exclusive, no alternatives [−] Small partnership, many alternatives
Product delay/failure/recall: [+] Single-product, safety risk, large revenue exposure [−] Diverse products, delay minor
Analyst downgrade/PT cut: [+] Low coverage, respected analyst, stock near highs [−] High coverage, perma-bear, stock already at lows
Negative personnel change: [+] Founder/CEO departure, key rainmaker, during crisis [−] Routine succession, company stable
Dilutive offering: [+] Low cash, high debt, negative FCF, stock down [−] Small offering, debt-for-equity swap deleverages
Earnings miss: [+] High short interest, high expectations, revenue miss, guidance cut alongside [−] Miss small, macro driven, peers also missed
Earnings guidance cut: [+] Cut large, structural, peers not cutting, previously guided positive [−] Cut small, temporary, peers also cut
Suspension of buyback/dividend cut: [+] Cash-strapped, previous commitment, signals distress [−] Cut to fund high-return project
Failed acquisition/overpayment: [+] High debt taken, goodwill impairment large, integration disaster [−] Small deal, regulatory block
Accumulation of debt/toxic assets: [+] Already high leverage, deteriorating metrics, near covenant breach [−] Accretive debt for growth, low cost
Operational setback: [+] Single facility, no backup, revenue concentration [−] Diversified operations, insurance covers
Insider selling (cluster): [+] CEO/CFO selling after beat, large amounts, no 10b5-1, multiple execs [−] Routine 10b5-1, small amounts, one insider
Activist exits/file hostile 13D: [+] Activist good track record, large position, underperformed [−] Activist exits fast, position small
Capacity underutilization/overexpansion: [+] High fixed costs, demand weakening, industry overcapacity [−] Temporary underutilisation, upturn expected
Strategic pivot failure: [+] Pivot expensive, CEO staked reputation, high debt [−] Pivot small experiment, quickly reversed
Supply chain shock: [+] Single-source, long lead times, no inventory [−] Diversified suppliers, buffer inventory
Patent litigation loss/IP theft: [+] Core patent, high royalty income, competitive advantage lost [−] Peripheral patent, workaround exists
Customer concentration risk: [+] Single customer > 30% revenue, no long-term contract [−] Diversified, contract locked in
Policy reversal/new regulation/tax increase: [+] Industry directly targeted, high cost impact [−] Sector exempt, impact small
Rate hike/monetary tightening: [+] High debt, floating rate, low interest coverage, negative FCF [−] Low debt, fixed-rate, cash-rich
Adverse litigation/antitrust: [+] Binary penalties, large damages, core at risk [−] Nuisance suit, low probability
Geopolitical event that hurts sector: [+] High exposure to conflict region, supply chain disruption [−] Diversified geography, domestic focus
Sector headwind/index exclusion: [+] Index fund selling forced, low liquidity [−] Exclusion expected, small ETF weight
Regulatory denial/antitrust block: [+] Deal-breaker, no alternative, sunk cost [−] Denial expected, alternative paths
Macro headwinds: [+] High cyclicality, consumer discretionary, high operating leverage [−] Defensive sector, high cash, flexible costs
Sector rotation out: [+] High valuation premium, high beta, crowded institutional positioning [−] Already underowned, attractive value
Unfavorable commodity price move: [+] High input cost sensitivity, unhedged, low pricing power [−] Hedged, high pricing power, small input cost
ESG controversy/carbon tax: [+] High emission industry, no offset plan, brand risk [−] Already green, tax small
Currency headwind: [+] High export revenue, unhedged, domestic costs in strong currency [−] Hedged, foreign costs decline, small foreign share
Technical breakdown: [+] Breakdown on high volume, death cross, preceded by rally [−] Low volume, already oversold
Short attack/bear raid: [+] High short interest, credible short report, fraud allegation [−] Already heavily shorted, report low credibility
Institutional ownership decline: [+] Concentrated holders, high-conviction fund exiting, after pop [−] Passive rebalancing, one small fund"""

STEP2_TEMPLATE = """
You are a COMPANY CONTEXT ANALYST. Your inputs are:
1. A financial snapshot of {full_name} ({ticker}) from a database.
2. Search snippets about {full_name} ({ticker})'s business model, operations, and risks.

CRITICAL: Only use snippets that explicitly refer to {full_name} or {ticker}. Discard any snippet about a different company.

FINANCIAL SNAPSHOT:
{snapshot}

CONTEXT SEARCH SNIPPETS:
{context_search_results}

PHASE 1: Structured Context Questionnaire
Answer every question using the snapshot and snippets. Write "DATUM_MISSING" if data is not available.

1. REVENUE STRUCTURE a) % revenue government/commercial/consumer? b) domestic/international? c) Top 3 customers & share? d) concentration risk?
2. COST STRUCTURE & SUPPLY CHAIN a) Top 3 input costs? b) % COGS commodity-linked? c) in-house vs outsourced? d) % supply chain China/geopolitical? e) supplier concentration risk?
3. COMPETITIVE POSITION a) pricing power? b) switching costs? c) industry structure? d) market share? e) top 3 competitors?
4. FINANCIAL SENSITIVITIES a) debt/equity b) fixed vs floating debt % c) interest coverage d) cash runway e) revenue growth trend f) gross margin trend g) profit margin trend h) FCF trend.
5. EXTERNAL EXPOSURES a) tariff sensitivity b) commodity sensitivity c) currency sensitivity d) key regulators e) geopolitical risk f) govt contract exposure.
6. MANAGEMENT & RISKS a) CEO track record b) insider ownership % trend c) pending litigation d) regulatory investigations.
7. GROWTH TRAJECTORY a) organic vs acquisitive b) backlog visibility c) capacity plans d) end-market growth rate.

Cite snippet IDs or snapshot fields. Then compute a sensitivity multiplier for EVERY catalyst using the AMPLIFIER/DAMPENER table below.

AMPLIFIER/DAMPENER REFERENCE TABLE:
{amp_damp_table}

TAXONOMY:
{taxonomy_list_str}

OUTPUT FORMAT: Return ONLY this JSON.
{{
  "ticker": "{ticker}",
  "extracted_context": {{ ... }},
  "sensitivity_profile": {{ "Contract win/expansion": {{"multiplier": 1.3, "rationale": "High customer concentration amplifies contract wins. [source: Q1c]"}} ... }},
  "missing_data": [...]
}}
"""

STEP4_TEMPLATE = """
You are a FINAL CATALYST SYNTHESIZER for {full_name} ({ticker}) on {today}.

INPUTS:
1. Merged event list (Finviz + raw) – each with an EVENT_ID:
{merged_events_json}

2. Weighted taxonomy (company‑context‑adjusted):
{weighted_taxonomy_json}

3. Financial snapshot:
{snapshot_json}

TASKS:
A. Classify each event into one or more catalyst categories from the taxonomy. Use EXACT taxonomy label.
B. Build the FULL catalyst grid (66 items). For each catalyst, set status: HIT / MISS / N/A.
   - For each HIT, copy event_date, evidence_excerpt, source_urls, confidence from the event.
   - Include the EVENT_ID numbers that contributed to this HIT (list of integers).
   - Use the adjusted_weight from the weighted taxonomy for that catalyst.
C. Apply INTERACTION RULES:
   1. If "Insider selling (cluster)" HIT AND "Earnings beat" HIT within 14 days, reduce beat's weight by 1 and add "Insider-earnings divergence" (negative, weight=1).
   2. If float_short > 20% AND positive HITs dominate, add "Short squeeze potential" (positive, weight=3).
   3. If analyst target < current price, reclassify "Analyst upgrade/PT increase" as negative.
   4. If both "Technical breakdown" and "Earnings beat" are HIT, reduce breakdown's weight by 2.
D. Compute FINAL SCORES: Positive_Score = sum(adjusted_weight * confidence/100) for positive HITs, Negative_Score similarly. Net = Positive − Negative. Map: Net>=20→Strong Bullish, >=8→Bullish, >=-8→Neutral, >=-20→Bearish, else→Strong Bearish. Conviction = min(100, abs(Net)*2).
E. Write a catalyst_stack (4 sentences, with dates). F. Identify key_assumption.

OUTPUT FORMAT: Return ONLY this JSON.
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
      "event_ids": [0, 3],
      "event_date": "2025-10-14",
      "evidence_excerpt": "...",
      "source_urls": ["https://..."],
      "confidence": 90
    }},
    ... every catalyst
  ],
  "catalyst_stack": "...",
  "net_signal": "Bullish",
  "conviction": 78,
  "key_assumption": "..."
}}
"""

# ── Prompt formatters ───────────────────────────────────
def _format_step1(full_name, ticker, today, lookback_start, search_results_json, finviz_events_json):
    return STEP1_TEMPLATE.format(full_name=full_name, ticker=ticker, today=today,
                                lookback_start=lookback_start, search_results_json=search_results_json,
                                finviz_events_json=finviz_events_json)

def _format_step2(full_name, ticker, snapshot, context_search_results, taxonomy_list_str):
    return STEP2_TEMPLATE.format(full_name=full_name, ticker=ticker,
                                snapshot=json.dumps(snapshot, indent=2, default=str),
                                context_search_results=context_search_results,
                                amp_damp_table=AMP_DAMP_TABLE, taxonomy_list_str=taxonomy_list_str)

def _format_step4(full_name, ticker, today, merged_events_json, weighted_taxonomy_json, snapshot_json):
    return STEP4_TEMPLATE.format(full_name=full_name, ticker=ticker, today=today,
                                merged_events_json=merged_events_json,
                                weighted_taxonomy_json=weighted_taxonomy_json,
                                snapshot_json=snapshot_json)

# ── JSON parser (DeepSeek) ─────────────────────────────
def parse_json(raw):
    if raw.startswith("```"): raw = raw.split("\n", 1)[1].rsplit("```", 1)[0].strip()
    try: return json.loads(raw)
    except: pass
    cleaned = re.sub(r",\s*}", "}", raw); cleaned = re.sub(r",\s*]", "]", cleaned)
    try: return json.loads(cleaned)
    except: pass
    raise ValueError(f"Failed to parse JSON. Start: {raw[:200]}")

# ── LLM caller ─────────────────────────────────────────
def call_llm(prompt, user_msg, temperature=0.3, max_tokens=40000):
    messages = [{"role": "system", "content": prompt}, {"role": "user", "content": user_msg}]
    resp = safe_create(model=MODEL, messages=messages, temperature=temperature, max_tokens=max_tokens)
    return resp.choices[0].message.content.strip()

# ── Recalculate signal ─────────────────────────────────
def recalculate_signal(grid):
    pos = sum(c.get("adjusted_weight",0) * c.get("confidence",50)/100 for c in grid if c.get("status")=="HIT" and c.get("type")=="positive")
    neg = sum(c.get("adjusted_weight",0) * c.get("confidence",50)/100 for c in grid if c.get("status")=="HIT" and c.get("type")=="negative")
    net = pos - neg
    if net>=20: signal="Strong Bullish"
    elif net>=8: signal="Bullish"
    elif net>=-8: signal="Neutral"
    elif net>=-20: signal="Bearish"
    else: signal="Strong Bearish"
    conviction = min(100, int(abs(net)*2))
    return signal, conviction

# ── Async pipeline ──────────────────────────────────────
async def analyze_stock_async(ticker, snapshot, searxng_url):
    if not gemini_health_check():
        print("  ❌ Gemini health check FAILED. Skipping entire analysis to preserve DeepSeek tokens.")
        return {"error": "Gemini health check failed; analysis aborted."}

    db_name = snapshot["profile"].get("company_name", "")
    if db_name and db_name.lower() != ticker.lower() and len(db_name)>2:
        official_name = db_name
        aliases = []
        print(f"  🏢 Using DB company name: {official_name}")
    else:
        official_name, aliases = resolve_company_name(ticker, searxng_url)
    full_name = f"{official_name} ({ticker})" if official_name.lower() != ticker.lower() else ticker

    finviz_events = scrape_finviz_news(ticker)
    print(f"  📰 Finviz returned {len(finviz_events)} headlines (after cutoff)")

    catalyst_queries = _make_catalyst_templates(full_name)
    context_queries = _make_context_templates(full_name)
    print(f"  ⏳ {len(catalyst_queries)} catalyst + {len(context_queries)} context queries…")
    catalyst_task = batch_search(catalyst_queries, searxng_url)
    context_task = batch_search(context_queries, searxng_url)
    catalyst_results, context_results = await asyncio.gather(catalyst_task, context_task)

    catalyst_results = _filter_search_results(catalyst_results, official_name, ticker, aliases)
    context_results = _filter_search_results(context_results, official_name, ticker, aliases)

    # Prompt generation
    search_results_str = "\n\n".join(f"Query: {q}\n{v}" for q,v in catalyst_results.items())
    finviz_json = json.dumps(finviz_events, indent=2)
    prompt1 = _format_step1(full_name, ticker, TODAY, LOOKBACK_START, search_results_str, finviz_json)

    context_str = "\n\n".join(f"Query: {q}\n{v}" for q,v in context_results.items())
    prompt2 = _format_step2(full_name, ticker, snapshot, context_str, "\n".join(TAXONOMY_LIST))

    step1_task = asyncio.to_thread(call_llm, prompt=prompt1, user_msg=f"Extract events for {full_name}.", max_tokens=40000)
    step2_task = asyncio.to_thread(call_llm, prompt=prompt2, user_msg=f"Context for {full_name}.", max_tokens=40000)
    step1_raw, step2_raw = await asyncio.gather(step1_task, step2_task)
    print("  ✅ Step 1 + Step 2 LLM done.")

    try:
        raw_events = parse_json(step1_raw)
        if isinstance(raw_events, dict): raw_events = raw_events.get("events", raw_events.get("evidence_grid", []))
        if not isinstance(raw_events, list): raise ValueError("not a list")
    except Exception as e:
        print(f"  ❌ Step 1 parse failed: {e}")
        return {"error": "Step 1 parse failure", "raw": step1_raw[:500]}

    # Filter raw events by cutoff
    if CUTOFF_DATE:
        raw_events = [e for e in raw_events if e.get("event_date", "9999") <= CUTOFF_DATE]
    print(f"  📋 Step 1 extracted {len(raw_events)} new raw events (after cutoff)")

    try:
        context_profile = parse_json(step2_raw)
    except Exception as e:
        print(f"  ❌ Step 2 parse failed: {e}")
        return {"error": "Step 2 parse failure", "raw": step2_raw[:500]}

    sensitivity = context_profile.get("sensitivity_profile", {})
    weighted_taxonomy = {}
    for cat, prof in sensitivity.items():
        base = CATALYST_WEIGHTS.get(cat, 5)
        mult = prof.get("multiplier", 1.0)
        adj = round(base * mult)
        weighted_taxonomy[cat] = {"base_weight": base, "multiplier": mult,
                                  "adjusted_weight": max(0, min(10, adj)),
                                  "rationale": prof.get("rationale","")}

    merged_events = []
    idx = 0
    for ev in finviz_events:
        merged_events.append({**ev, "id": idx}); idx += 1
    for ev in raw_events:
        merged_events.append({**ev, "id": idx}); idx += 1

    merged_events.sort(key=lambda e: e.get("confidence", 0), reverse=True)
    merged_events = merged_events[:50]
    print(f"  🔗 Merged event list: {len(merged_events)} total (capped at 50)")

    merged_json = json.dumps([{"id": e["id"], "description": e["description"], "event_date": e["event_date"],
                               "evidence_excerpt": e.get("evidence_excerpt", ""), "source_urls": e.get("source_urls", []),
                               "confidence": e.get("confidence", 70)} for e in merged_events], indent=2)
    prompt4 = _format_step4(full_name, ticker, TODAY, merged_json,
                            json.dumps(weighted_taxonomy, indent=2),
                            json.dumps(snapshot, indent=2, default=str))
    final_raw = call_llm(prompt=prompt4, user_msg=f"Finalize {full_name}.", temperature=0.1, max_tokens=25000)
    try:
        final_result = parse_json(final_raw)
    except Exception as e:
        print(f"  ❌ Step 4 parse failed: {e}")
        return {"error": "Step 4 parse failure", "raw": final_raw[:500]}

    grid = final_result.get("catalyst_grid", [])
    events_by_id = {e["id"]: e for e in merged_events}

    for entry in grid:
        label = entry.get("taxonomy", "")
        if "type" not in entry or not entry.get("type"):
            entry["type"] = "positive" if label in POSITIVE_CATALYSTS else "negative" if label in CATALYST_WEIGHTS else "unknown"
        if "base_weight" not in entry or entry.get("base_weight") is None:
            entry["base_weight"] = CATALYST_WEIGHTS.get(label, 5)
        if "adjusted_weight" not in entry or entry.get("adjusted_weight") is None:
            wt = weighted_taxonomy.get(label, {})
            entry["adjusted_weight"] = wt.get("adjusted_weight", entry["base_weight"])
        if entry.get("status") == "HIT" and entry.get("event_ids"):
            ids = entry["event_ids"]
            if isinstance(ids, int): ids = [ids]
            primary_id = ids[0] if ids else None
            if primary_id is not None and primary_id in events_by_id:
                ev = events_by_id[primary_id]
                entry["event_date"] = ev.get("event_date", "?")
                entry["evidence_excerpt"] = ev.get("evidence_excerpt", "")
                entry["headline"] = ev.get("headline", ev.get("description", ""))
                entry["source_urls"] = ev.get("source_urls", [])
                entry["confidence"] = ev.get("confidence", 70)
                entry["event_id"] = primary_id
            else:
                entry["event_date"] = "?"
                entry["source_urls"] = []
                entry["evidence_excerpt"] = ""
                entry["headline"] = ""
                entry["event_id"] = None
        elif entry.get("status") != "HIT":
            entry["event_date"] = None
            entry["source_urls"] = []
            entry["evidence_excerpt"] = ""
            entry["headline"] = ""
            entry["event_id"] = None
        for k in ("base_weight","adjusted_weight"):
            if k in entry and entry[k] is not None:
                entry[k] = int(round(entry[k]))

    existing_tax = {c.get("taxonomy") for c in grid}
    for i, label in enumerate(TAXONOMY_LIST):
        if label not in existing_tax:
            wt = weighted_taxonomy.get(label, {})
            cat_type = "internal" if i < 33 else ("external" if i < 60 else "market_mechanic")
            grid.append({
                "taxonomy": label,
                "type": "positive" if label in POSITIVE_CATALYSTS else "negative",
                "category": cat_type,
                "status": "MISS",
                "base_weight": CATALYST_WEIGHTS.get(label, 5),
                "adjusted_weight": wt.get("adjusted_weight", CATALYST_WEIGHTS.get(label, 5)),
                "event_date": None, "evidence_excerpt": "", "headline": "",
                "source_urls": [], "event_id": None, "confidence": 0
            })

    time.sleep(2)
    grid, weighted_taxonomy, new_hits = gemini_unified_check(
        full_name, ticker, grid, snapshot, weighted_taxonomy, TAXONOMY_LIST
    )

    dedup = {}
    for entry in grid:
        tax = entry.get("taxonomy")
        if tax not in dedup or entry.get("adjusted_weight",0) > dedup[tax].get("adjusted_weight",0):
            dedup[tax] = entry
    grid = list(dedup.values())

    final_result["catalyst_grid"] = grid
    new_signal, new_conviction = recalculate_signal(grid)
    final_result["net_signal"] = new_signal
    final_result["conviction"] = new_conviction
    print(f"  🔄 Final signal: {new_signal} (conviction {new_conviction})")
    return final_result

# ── Main entry ──────────────────────────────────────────
def analyze_stock(ticker, snapshot, searxng_url):
    return asyncio.run(analyze_stock_async(ticker, snapshot, searxng_url))

if __name__ == "__main__":
    try:
        from db.connection import get_connection
        HAS_DB = True
    except:
        HAS_DB = False
        print("⚠️  psycopg2 not installed")
    conn = None; cur = None
    for ticker in TICKERS:
        print(f"\n{'='*60}\n📊 Snapshot for {ticker}…")
        if HAS_DB:
            if conn is None: conn = get_connection(); cur = conn.cursor()
            snap = build_health_snapshot(ticker, conn)
        else:
            snap = {}
        if CUTOFF_DATE:
            print(f"   ⚠️  Backtest mode: discarding events after {CUTOFF_DATE}")
        start = time.time()
        result = analyze_stock(ticker, snap, SEARXNG_URL)
        elapsed = time.time() - start
        print(f"\n⏱️  Analysis completed in {elapsed:.1f}s")
        if "error" in result:
            print(f"❌ Error: {result['error']}")
        else:
            grid = result.get("catalyst_grid", [])
            positive_hits = [c for c in grid if c.get("status")=="HIT" and c.get("type")=="positive"]
            negative_hits = [c for c in grid if c.get("status")=="HIT" and c.get("type")=="negative"]
            print(f"✅ Net signal: {result.get('net_signal')} (conviction {result.get('conviction')})")
            print(f"   🟢 Positive HITs: {len(positive_hits)}")
            for h in positive_hits:
                eid = h.get("event_id", "?")
                hl = h.get("headline", h.get("evidence_excerpt", ""))[:80].strip()
                urls = h.get("source_urls", [])
                link = urls[0] if urls else ""
                print(f"      #{eid} | {h['taxonomy']} | {h.get('event_date','?')} | {hl}")
                print(f"         wt {h.get('base_weight','?')}/{h.get('adjusted_weight','?')} | conf {h.get('confidence','?')} | {link[:70]}")
            print(f"   🔴 Negative HITs: {len(negative_hits)}")
            for h in negative_hits:
                eid = h.get("event_id", "?")
                hl = h.get("headline", h.get("evidence_excerpt", ""))[:80].strip()
                urls = h.get("source_urls", [])
                link = urls[0] if urls else ""
                print(f"      #{eid} | {h['taxonomy']} | {h.get('event_date','?')} | {hl}")
                print(f"         wt {h.get('base_weight','?')}/{h.get('adjusted_weight','?')} | conf {h.get('confidence','?')} | {link[:70]}")
        time.sleep(2)
    if cur: cur.close()
    if conn: conn.close()
    print("\n🏁 All analyses complete.")
