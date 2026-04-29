#!/usr/bin/env python3
"""
Catalyst Analysis Engine – Deep Context, Unlimited Searches

- DeepSeek function calling → SearXNG search → mandatory context audit → structured output
- Enforces 6‑month lookback & exact event dates
- Context rules prevent misclassification (e.g., analyst target below current price = negative)
- Stores results in Supabase `signals` table (optional)
Schedule: Daily, after data collectors finish
"""

import os, json, time, requests, re
from datetime import datetime, date, timedelta, timezone
from openai import OpenAI

# ── Config ──────────────────────────────────────────────
SEARXNG_URL          = os.environ["SEARXNG_URL"]
SEARXNG_TIMEOUT      = 15
MAX_SEARCHES         = 100        # unlimited SearXNG – cover every lens exhaustively
SEARCH_DELAY         = 0.5        # seconds between searches (rate‑limit safety)
MODEL                = "deepseek-chat"
TODAY                = date.today().isoformat()
LOOKBACK_START       = (date.today() - timedelta(days=185)).isoformat()

# ── Tools ───────────────────────────────────────────────
TOOLS = [{
    "type": "function",
    "function": {
        "name": "web_search",
        "description": (
            "Search the live web for real‑time financial news, filings, contracts, "
            "regulatory decisions, patents, insider trades, and analyst actions. "
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
                        "and year/month for date‑sensitive info."
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

# ── System Prompt ───────────────────────────────────────
SYSTEM_PROMPT = f"""\
You are a rigorous event‑driven equity analyst with a 1‑2 week horizon.

TODAY IS {TODAY}. Use ONLY information from {LOOKBACK_START} to {TODAY}.

──────────────────────────────────────────────────────
CATALYST TAXONOMY (classify every catalyst into one)
──────────────────────────────────────────────────────
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

──────────────────────────────────────────────────────
CONTEXT RULES – APPLY THESE TO EVERY CATALYST
──────────────────────────────────────────────────────

1. ANALYST PRICE TARGETS:
   ALWAYS compare the target price to the CURRENT stock price.
   - If target < current price → NEGATIVE (analysts see downside)
   - If target > current price → POSITIVE (analysts see upside)
   - Report: "Target $X vs current $Y = Z% premium/discount"
   - Distinguish consensus (average) from individual analyst targets.
   - An increase in target that still sits below the current price IS STILL NEGATIVE.

2. INSIDER TRADING:
   Distinguish routine 10b5‑1 plan sales from discretionary cluster sales.
   Report the NET: total $ bought vs total $ sold.
   CEO/CFO cluster sales outside of 10b5‑1 plans = high negative weight.

3. SHORT INTEREST:
   >15% of float = elevated short‑squeeze risk (bullish if positive catalysts exist).
   Rising short interest + negative catalysts = bear‑raid potential.
   Include days‑to‑cover ratio.

4. VALUATION:
   Compare P/E, P/S, EV/EBITDA to sector medians and 5‑year historical averages.
   Stock trading >200% above consensus analyst target = significant downside risk.

5. FINANCIAL HEALTH:
   Negative ROE, negative profit margins, or rapidly rising liabilities are
   negative catalysts even when revenue is growing.

6. TARIFF / GEOGRAPHIC EXPOSURE:
   Quantify % of revenue or manufacturing exposed to geopolitical risk.
   Weigh against current tariff/trade policy stance.

──────────────────────────────────────────────────────
REQUIRED SEARCH LENSES (exhaust them all)
──────────────────────────────────────────────────────
1.  Company‑specific news (contracts, products, management, earnings, guidance)
2.  Insider trading – buying AND selling, Form 4 filings
3.  Analyst actions – upgrades, downgrades, price target changes, initiations
4.  Institutional ownership – 13F filings, major holder changes
5.  Short interest – % of float, days‑to‑cover, trend
6.  Financial health – debt, cash, profitability, liabilities
7.  Valuation – P/E, P/S, EV/EBITDA vs peers
8.  Sector & peer events (competitors, suppliers, customers)
9.  Supply chain & geopolitical risks (tariffs, China exposure, factory news)
10. Regulatory & legislative environment (SEC, FDA, Congress, export controls)
11. Macro context (Fed, rates, inflation, commodity prices, GDP)
12. Technical analysis (moving averages, support/resistance, volume)

──────────────────────────────────────────────────────
OUTPUT RULES
──────────────────────────────────────────────────────
1. EVERY active catalyst MUST include an EXACT DATE (e.g., "On March 18, 2026…").
2. NEVER use "recently," "in the past," "has had a parabolic run."
3. Combine duplicate events from multiple sources into ONE catalyst with higher confidence.
4. Every catalyst must be classified into the taxonomy above.
5. Output ONLY the JSON object below, no other text.

{{
  "ticker": "...",
  "analysis_date": "{TODAY}",
  "lookback_start": "{LOOKBACK_START}",
  "current_price": "extracted from search results or state if unknown",
  "active_catalysts": [
    {{
      "type": "positive|negative|neutral",
      "category": "internal|external|market_mechanic",
      "taxonomy": "exact taxonomy label",
      "summary": "Detailed summary with EXACT DATE. For analyst targets, include 'Target $X vs current $Y = Z% premium/discount'.",
      "source_urls": ["url1", "url2"],
      "confidence": 0-100
    }}
  ],
  "catalyst_stack": "4‑sentence narrative with specific dates.",
  "net_signal": "Strong Bullish|Bullish|Neutral|Bearish|Strong Bearish",
  "conviction": 0-100,
  "key_assumption": "The single assumption that, if wrong, would flip the call.",
  "search_queries_used": ["..."]
}}
"""

# ── Audit prompt ────────────────────────────────────────
AUDIT_PROMPT = f"""\
You are a forensic financial audit layer. Review the catalyst analysis below.
TODAY IS {TODAY}. The stock's current price should appear in the search results.

CORRECT EVERY MISCLASSIFICATION:
1. If any analyst price target is BELOW the current stock price, reclassify it as
   NEGATIVE (type: "negative", taxonomy: "Analyst downgrade/PT cut") regardless of
   whether the target was recently raised.
2. If a target is ABOVE the current price, keep it positive.
3. For insider transactions: if net selling exceeds net buying, add a negative catalyst
   explicitly. If sales are routine 10b5‑1, note that but keep the negative.
4. If short interest >15% and positive catalysts dominate, add a positive
   "Short squeeze" catalyst to market_mechanic.
5. If ROE is negative or profit margins are negative, add a negative catalyst.
6. If debt/equity is rising rapidly or liabilities surged, add a negative catalyst.
7. Remove any vague language; every event must cite an exact date.

Return ONLY the corrected JSON with NO additional commentary.
"""

# ── Main analysis function ──────────────────────────────
def analyze_stock(ticker: str, exposure_profile: dict, previous_signals: list = None) -> dict:
    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": f"""\
Ticker: {ticker}
Exposure profile: {json.dumps(exposure_profile)}
Previous signals (from DB): {json.dumps(previous_signals) if previous_signals else 'None'}

Today is {TODAY}. Analyse ALL active catalysts for {ticker} within the last 6 months ({LOOKBACK_START} to {TODAY}).
Use web_search to cover ALL 12 required lenses. Be exhaustive. Pin every catalyst to an exact date.
Always compare analyst targets to the CURRENT stock price — a target below the current price is NEGATIVE."""}
    ]

    search_count = 0
    search_queries_used = []

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

    # ── Phase 1: Web search loop ──
    try:
        response = safe_create(
            model=MODEL, messages=messages, tools=TOOLS, tool_choice="auto", temperature=0.3
        )
    except Exception as e:
        return {"error": f"Initial API call failed: {e}", "search_count": 0}

    msg = response.choices[0].message

    while msg.tool_calls and search_count < MAX_SEARCHES:
        messages.append(msg)

        for tc in msg.tool_calls:
            if tc.function.name != "web_search":
                messages.append({
                    "role": "tool",
                    "tool_call_id": tc.id,
                    "content": "TOOL_NOT_SUPPORTED"
                })
                continue

            args = json.loads(tc.function.arguments)
            query = args.get("query", "")
            cats  = args.get("categories", "general,news")

            if search_count < MAX_SEARCHES:
                print(f"  🔍 [{search_count+1}/{MAX_SEARCHES}] {query[:100]}…")
                result_text = web_search(query, cats)
                search_queries_used.append(query)
                search_count += 1
            else:
                result_text = "SEARCH_SKIPPED: Limit reached."
                print(f"  ⏭️  Skipping: {query[:80]}…")

            messages.append({
                "role": "tool",
                "tool_call_id": tc.id,
                "content": result_text,
            })
            time.sleep(SEARCH_DELAY)

        if search_count >= MAX_SEARCHES:
            break

        try:
            response = safe_create(
                model=MODEL, messages=messages, tools=TOOLS, tool_choice="auto", temperature=0.3
            )
        except Exception as e:
            return {"error": f"API call failed during search loop: {e}", "search_count": search_count}

        msg = response.choices[0].message

    # ── Force final answer if needed ──
    if (msg.tool_calls and search_count >= MAX_SEARCHES) or not (msg.content and msg.content.strip()):
        messages.append({
            "role": "user",
            "content": "You have exhausted all searches. Produce the final JSON analysis NOW. Output ONLY the JSON object."
        })
        try:
            response = safe_create(model=MODEL, messages=messages, temperature=0.2)
        except Exception as e:
            return {"error": f"API call failed during forced final: {e}", "search_count": search_count}
        msg = response.choices[0].message

    # ── Extract JSON from Phase 1 ──
    final_text = (msg.content or "").strip()
    if not final_text:
        return {"error": "Empty final response", "search_count": search_count}

    if final_text.startswith("```"):
        final_text = final_text.split("\n", 1)[1].rsplit("```", 1)[0]
    final_text = final_text.strip()

    # ── Phase 2: Context audit ──
    print("  🧠 Running context audit layer…")
    audit_messages = [
        {"role": "system", "content": AUDIT_PROMPT},
        {"role": "user", "content": f"Current stock: {ticker}. Review and correct this analysis:\n\n{final_text}"}
    ]
    try:
        audit_response = safe_create(model=MODEL, messages=audit_messages, temperature=0.2)
        audit_text = (audit_response.choices[0].message.content or "").strip()
        if audit_text:
            if audit_text.startswith("```"):
                audit_text = audit_text.split("\n", 1)[1].rsplit("```", 1)[0]
            audit_text = audit_text.strip()
            if audit_text:
                final_text = audit_text
                print("  ✅ Audit layer applied corrections.")
    except Exception as e:
        print(f"  ⚠️  Audit layer failed (using raw output): {e}")

    # ── JSON repair ──
    try:
        result = json.loads(final_text)
    except json.JSONDecodeError:
        fixed = re.sub(r",\s*}", "}", final_text)
        fixed = re.sub(r",\s*]", "]", fixed)
        try:
            result = json.loads(fixed)
        except json.JSONDecodeError:
            try:
                fix_resp = safe_create(
                    model=MODEL,
                    messages=[
                        {"role": "system", "content": "Return only valid JSON."},
                        {"role": "user", "content": f"Fix this JSON:\n\n{final_text}"}
                    ],
                    temperature=0.0
                )
                fixed2 = fix_resp.choices[0].message.content.strip()
                if fixed2.startswith("```"):
                    fixed2 = fixed2.split("\n", 1)[1].rsplit("```", 1)[0]
                result = json.loads(fixed2)
            except Exception:
                return {
                    "error": "JSON parse failed after repair",
                    "raw": final_text,
                    "search_count": search_count,
                    "search_queries_used": search_queries_used,
                }

    result["search_count"] = search_count
    result["search_queries_used"] = search_queries_used
    return result


# ── Main loop ───────────────────────────────────────────
if __name__ == "__main__":
    try:
        from db.connection import get_connection
        HAS_DB = True
    except ModuleNotFoundError:
        HAS_DB = False
        print("⚠️  psycopg2 not installed – running without database storage.")

    try:
        with open("data/exposure_profiles.json") as f:
            profiles = json.load(f)
    except FileNotFoundError:
        profiles = {}
        print("⚠️  No exposure_profiles.json found – using empty profiles.")

    conn = None
    cur = None
    tickers = []

    if HAS_DB:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("SELECT ticker FROM ticker_master WHERE is_active = true ORDER BY ticker")
        tickers = [row[0] for row in cur.fetchall()]
    else:
        tickers = list(profiles.keys())

    for ticker in tickers:
        profile = profiles.get(ticker, {})
        print(f"\n{'='*60}\n📊 Analyzing {ticker}…\n{'='*60}")
        result = analyze_stock(ticker, profile)

        if "error" in result:
            print(f"❌ Error: {result['error']}")
        else:
            print(f"✅ Net signal: {result.get('net_signal')} (conviction {result.get('conviction')})")
            print(f"   Catalysts: {len(result.get('active_catalysts', []))}")
            for cat in result.get("active_catalysts", []):
                print(f"     - [{cat['type']}] {cat['summary'][:120]}…")

        if HAS_DB and "error" not in result:
            cur.execute("""
                INSERT INTO signals (ticker, analysis_date, net_signal, conviction,
                    catalyst_stack, key_assumption, raw_json, search_count)
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

        time.sleep(2)

    if cur: cur.close()
    if conn: conn.close()
    print(f"\n{'='*60}\n🏁 All analyses complete.")
