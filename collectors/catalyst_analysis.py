#!/usr/bin/env python3
"""
Catalyst Analysis Engine – Strict Date‑Aware Version

– DeepSeek function calling → SearXNG search → structured catalyst output
– Enforces 6‑month lookback & exact event dates
– Stores results in Supabase `signals` table (optional)
Schedule: Daily, after data collectors finish
"""

import os, json, time, requests
from datetime import datetime, date, timedelta, timezone
from openai import OpenAI

# ── Config ──────────────────────────────────────────────
SEARXNG_URL          = os.environ["SEARXNG_URL"]
SEARXNG_TIMEOUT      = 15
MAX_SEARCHES         = 8          # per stock – enough to cover every lens
SEARCH_DELAY         = 0.7        # seconds
MODEL                = "deepseek-chat"   # or "deepseek-v4-flash" when available
TODAY                = date.today().isoformat()
LOOKBACK_START       = (date.today() - timedelta(days=185)).isoformat()  # ~6 months

# ── Tools ───────────────────────────────────────────────
TOOLS = [{
    "type": "function",
    "function": {
        "name": "web_search",
        "description": (
            "Search the live web for real‑time financial news, filings, contracts, "
            "regulatory decisions, patents, and analyst actions. "
            "Use this whenever up‑to‑date information is required. "
            "Prefix site‑specific queries with 'site:domain.com'."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": (
                        "Search query. Be specific: include company name, topic, "
                        "and year if relevant. For date‑sensitive info, include month/year."
                    )
                },
                "categories": {
                    "type": "string",
                    "enum": ["general", "news"],
                    "description": "'news' for time‑sensitive headlines; 'general' for broader research."
                }
            },
            "required": ["query"]
        }
    }
}]

# ── Client ──────────────────────────────────────────────
client = OpenAI(
    api_key=os.environ.get("DEEPSEEK_API_KEY"),
    base_url="https://api.deepseek.com",
)

# ── SearXNG executor ────────────────────────────────────
def web_search(query: str, categories: str = "general,news") -> str:
    try:
        resp = requests.get(
            f"{SEARXNG_URL}/search",
            params={"q": query, "format": "json", "categories": categories},
            timeout=SEARXNG_TIMEOUT,
            headers={"User-Agent": "CatalystEngine/1.0"},
        )
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        return f"SEARCH_ERROR: {e}"

    results = data.get("results", [])
    if not results:
        return "SEARCH_RESULT: No results."

    formatted = []
    for r in results[:8]:
        engine  = r.get("engine", "?")
        title   = r.get("title", "")
        snippet = r.get("content", "")[:500]
        url     = r.get("url", "")
        pubdate = r.get("publishedDate") or ""
        formatted.append(f"[{engine}] {title}\nDate: {pubdate}\nSnippet: {snippet}\nURL: {url}")
    return "\n\n".join(formatted)

# ── System Prompt (THE CRITICAL PART) ───────────────────
SYSTEM_PROMPT = f"""\
You are a rigorous event‑driven equity analyst with a 1‑2 week horizon.

TODAY IS {TODAY}. The analysis must use ONLY information from the LAST 6 MONTHS
({LOOKBACK_START} to {TODAY}). Ignore any event outside this window.

CATALYST TAXONOMY (every active catalyst must be classified into one of these):
Positive Internal: Contract win/expansion, Strategic partnership, Product launch/FDA approval,
  Analyst upgrade/PT increase, Positive personnel change, Capital infusion, Earnings beat,
  Guidance raise, Share repurchase/dividend increase, Successful acquisition,
  Deleveraging/asset sale, Operational milestone, Insider buying (cluster),
  Activist investor accumulation, Capacity expansion, Strategic pivot, Supply chain de‑risking,
  Patent grant/IP protection, Customer concentration expansion.
Positive External: Government policy (subsidies/mandates), Institutional policy (rate cut/QE),
  Favorable court ruling, Geopolitical boost, Sector tailwind/index inclusion,
  Regulatory approval, Macro tailwind (CPI cooling/GDP beat), Sector rotation in,
  Commodity price favorable, ESG/green subsidy, Currency tailwind, Technical breakout.
Positive Market Mechanics: Short squeeze, Institutional accumulation (13F filings).

Negative Internal: Contract loss, Partnership dissolution, Product delay/failure/recall,
  Analyst downgrade/PT cut, Negative personnel change, Dilutive offering,
  Earnings miss, Guidance cut, Dividend/buyback suspension, Failed acquisition,
  Debt accumulation/toxic assets, Operational setback, Insider selling (clustered),
  Activist exit, Capacity underutilization, Strategic pivot failure,
  Supply chain shock, Patent litigation loss/IP theft, Customer concentration risk.
Negative External: Policy reversal/tax increase, Rate hike/monetary tightening,
  Adverse litigation/antitrust, Geopolitical hurt (sanctions/conflict), Sector headwind/index exclusion,
  Regulatory denial/delay, Macro headwind (inflation/recession), Sector rotation out,
  Commodity price unfavorable, ESG controversy/carbon tax, Currency headwind, Technical breakdown.
Negative Market Mechanics: Short attack/bear raid, Institutional ownership decline.

REQUIRED SEARCH LENSES (you must cover all of these via web_search):
1. Company‑specific news (contracts, product, management, earnings, guidance, insider trades)
2. Analyst & institutional sentiment (upgrades, downgrades, PT changes, 13F filings)
3. Sector & peer events (NVIDIA, Lumentum, Coherent – whatever is relevant)
4. Supply chain & geopolitical risks (tariffs, China, factory news)
5. Regulatory & legislative environment
6. Macro & commodity context (Fed, inflation, oil, AI capex)

CRITICAL RULES FOR OUTPUT:
1. EVERY active catalyst MUST include an EXACT DATE in the summary (e.g., "On March 15, 2026, AAOI announced...").
   If the exact date cannot be determined, use the most specific date available (at least month/year)
   and mark confidence lower.
2. NEVER use vague phrases like "recently," "in the past," "has had a parabolic run."
   Always pin events to specific dates drawn from search result publication dates or article content.
3. If multiple sources reference the same event, combine them into ONE catalyst with higher confidence.
4. Every catalyst must be classified into the taxonomy above.
5. The final JSON MUST contain:
   {{
     "ticker": "...",
     "analysis_date": "{TODAY}",
     "lookback_start": "{LOOKBACK_START}",
     "active_catalysts": [
       {{
         "type": "positive|negative|neutral",
         "category": "internal|external|market_mechanic",
         "taxonomy": "exact taxonomy label (e.g., 'Contract win/expansion')",
         "summary": "Detailed summary with EXACT DATE. E.g., 'On March 18, 2026, AAOI secured a $200M order for 1.6T transceivers...'",
         "source_urls": ["url1", "url2"],
         "confidence": 0-100
       }}
     ],
     "catalyst_stack": "A 4‑sentence narrative synthesising the net effect. Must reference specific dates.",
     "net_signal": "Strong Bullish|Bullish|Neutral|Bearish|Strong Bearish",
     "conviction": 0-100,
     "key_assumption": "The single assumption that, if wrong, would flip the call.",
     "search_queries_used": ["..."]
   }}
"""

# ── Main analysis function ──────────────────────────────
def analyze_stock(ticker: str, exposure_profile: dict, previous_signals: list = None) -> dict:
    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": f"""\
Ticker: {ticker}
Exposure profile: {json.dumps(exposure_profile)}
Previous signals (from DB): {json.dumps(previous_signals) if previous_signals else 'None'}

Today is {TODAY}. Analyse all active catalysts for {ticker} within the last 6 months ({LOOKBACK_START} to {TODAY}).
Use web_search to cover ALL required lenses. Be exhaustive. Pin every catalyst to an exact date."""}
    ]

    search_count = 0
    search_queries_used = []

    # Round 1 – tool call
    response = client.chat.completions.create(
        model=MODEL, messages=messages, tools=TOOLS, tool_choice="auto", temperature=0.3
    )
    msg = response.choices[0].message

    # Execute tools loop
    while msg.tool_calls and search_count < MAX_SEARCHES:
        messages.append(msg)
        for tc in msg.tool_calls:
            if tc.function.name != "web_search":
                continue
            args = json.loads(tc.function.arguments)
            query = args.get("query", "")
            cats  = args.get("categories", "general,news")
            print(f"  🔍 Searching [{search_count+1}/{MAX_SEARCHES}]: {query[:100]}…")
            result_text = web_search(query, cats)
            messages.append({
                "role": "tool",
                "tool_call_id": tc.id,
                "content": result_text,
            })
            search_queries_used.append(query)
            search_count += 1
            time.sleep(SEARCH_DELAY)

        response = client.chat.completions.create(
            model=MODEL, messages=messages, tools=TOOLS, tool_choice="auto", temperature=0.3
        )
        msg = response.choices[0].message

    # If the model still wants more searches, force final answer
    if not msg.content or not msg.content.strip():
        messages.append({"role": "user", "content": (
            "You have completed all available searches. Based strictly on the results, "
            "produce the final JSON analysis now. Do NOT ask for more searches."
        )})
        response = client.chat.completions.create(
            model=MODEL, messages=messages, temperature=0.3
        )
        msg = response.choices[0].message

    # Extract JSON from final text
    final_text = msg.content.strip()
    if final_text.startswith("```"):
        final_text = final_text.split("\n", 1)[1].rsplit("```", 1)[0]
    final_text = final_text.strip()

    try:
        result = json.loads(final_text)
        result["search_count"] = search_count
        result["search_queries_used"] = search_queries_used
        return result
    except json.JSONDecodeError:
        return {
            "error": "JSON parse failed",
            "raw": final_text,
            "search_count": search_count,
            "search_queries_used": search_queries_used,
        }

# ── Main loop (for GitHub Actions) ──────────────────────
if __name__ == "__main__":
    from db.connection import get_connection

    # Load exposure profiles (maintain this file manually)
    try:
        with open("data/exposure_profiles.json") as f:
            profiles = json.load(f)
    except FileNotFoundError:
        profiles = {}

    conn = get_connection()
    cur = conn.cursor()

    # Get active tickers
    cur.execute("SELECT ticker FROM ticker_master WHERE is_active = true ORDER BY ticker")
    tickers = [row[0] for row in cur.fetchall()]

    for ticker in tickers:
        profile = profiles.get(ticker)
        if not profile:
            print(f"⏭️  Skipping {ticker} – no exposure profile")
            continue

        print(f"\n{'='*60}\n📊 Analyzing {ticker}…\n{'='*60}")
        result = analyze_stock(ticker, profile)

        # Print summary
        if "error" in result:
            print(f"❌ Error: {result}")
        else:
            print(f"✅ Net signal: {result.get('net_signal')} (conviction {result.get('conviction')})")
            print(f"   Catalysts: {len(result.get('active_catalysts', []))}")
            for cat in result.get("active_catalysts", []):
                print(f"     - [{cat['type']}] {cat['summary'][:120]}…")

        # Store in DB
        if "error" not in result:
            cur.execute("""
                INSERT INTO signals (ticker, analysis_date, net_signal, conviction, catalyst_stack, key_assumption, raw_json, search_count)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (ticker, analysis_date) DO UPDATE SET
                    net_signal = EXCLUDED.net_signal,
                    conviction = EXCLUDED.conviction,
                    catalyst_stack = EXCLUDED.catalyst_stack,
                    key_assumption = EXCLUDED.key_assumption,
                    raw_json = EXCLUDED.raw_json,
                    search_count = EXCLUDED.search_count
            """, (
                result["ticker"],
                result.get("analysis_date", TODAY),
                result.get("net_signal"),
                result.get("conviction"),
                result.get("catalyst_stack"),
                result.get("key_assumption"),
                json.dumps(result),
                result.get("search_count"),
            ))
            conn.commit()
            print(f"   💾 Stored in signals table.")

        time.sleep(2)   # be kind to SearXNG & DeepSeek

    cur.close()
    conn.close()
    print(f"\n{'='*60}\n🏁 All analyses complete.")
