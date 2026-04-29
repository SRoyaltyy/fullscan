#!/usr/bin/env python3
"""
Catalyst Analysis Engine – with Health Check 2.0 & Full Catalyst Grid

Phase 0: DB Health Check → narrative evaluation
Phase 1: DeepSeek + SearXNG (35 searches) → exhaustive catalyst grid → structured output
Phase 2: Audit layer
Schedule: Daily, after data collectors finish
"""

import os, json, time, requests, re
from datetime import datetime, date, timedelta, timezone
from openai import OpenAI

# ── Config ──────────────────────────────────────────────
SEARXNG_URL          = os.environ["SEARXNG_URL"]
SEARXNG_TIMEOUT      = 15
MAX_SEARCHES         = 35
SEARCH_DELAY         = 0.5
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
                    "description": "Specific query, include company name and date."
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

# ── Health Check Prompt ─────────────────────────────────
HEALTH_CHECK_SYSTEM = """\
You are a forensic financial analyst. Given a raw snapshot of a company's financial metrics (valuation, profitability, growth, technicals, ownership, etc.), produce a concise, plain‑English evaluation that covers:

1. Business profile (sector, industry, market cap, institutional ownership)
2. Financial health & profitability (margins, ROE, debt, liquidity)
3. Valuation (P/E, forward P/E, PEG, vs. peers if deducible)
4. Momentum & technicals (performance over various periods, RSI, SMA positioning, 52‑week high/low)
5. Ownership & short interest (insider/inst ownership, short float, days to cover)
6. Growth trajectory (EPS & sales growth past & future)
7. Overall pre‑search verdict: Bullish / Bearish / Neutral with a brief reason and missing pieces to investigate.

Output a single block of plain text (no JSON). Keep it under 500 words. Be direct — no fluff, no recap of the prompt."""

# ── Full Catalyst Taxonomy with pre‑defined weights ─────
CATALYST_WEIGHTS = {
    "Contract win/expansion": 8,
    "Strategic partnership/alliance": 6,
    "Product launch/FDA approval/regulatory greenlight": 9,
    "Analyst upgrade/PT increase": 5,
    "Positive personnel change": 4,
    "Capital infusion (PIPE/funding round/favorable terms)": 6,
    "Earnings beat (revenue/EBITDA/EPS)": 8,
    "Earnings guidance raise": 7,
    "Share repurchase/dividend increase": 5,
    "Successful acquisition/synergy realization": 7,
    "Deleveraging/sale of toxic assets/spin-off": 5,
    "Operational milestone (trial/satellite/patient)": 6,
    "Insider buying (cluster purchases)": 7,
    "Activist investor accumulation (9.9% stake)": 7,
    "Capacity expansion announced": 6,
    "Strategic pivot/rebranding to high‑growth": 5,
    "Supply chain de‑risking (dual sourcing/reshoring)": 5,
    "Patent grant/IP protection": 4,
    "Customer concentration expansion": 5,
    "Government policy (tariffs/subsidies/mandates)": 7,
    "Institutional policy (Fed rate cut/QE/stimulus)": 9,
    "Favorable court ruling/patent grant": 8,
    "Geopolitical event that boosts sector": 7,
    "Sector tailwind/index inclusion": 5,
    "Regulatory approval (FDA/FCC/FTC clearance)": 9,
    "Macro tailwinds (CPI cooling/GDP beat/soft landing)": 8,
    "Sector rotation into the stock's industry": 6,
    "Commodity price move favorable to company": 6,
    "ESG mandate/green subsidy qualification": 4,
    "Currency tailwind": 3,
    "Technical breakout (above key moving averages)": 4,
    "Short squeeze": 8,
    "Institutional ownership increase (13F filings)": 6,
    "Contract loss/non‑renewal/reduction": 8,
    "Partnership dissolution/breakdown": 6,
    "Product delay/failure/rejection/safety recall": 9,
    "Analyst downgrade/PT cut": 5,
    "Negative personnel change (departures/scandals)": 5,
    "Dilutive offering/distressed fundraising/down round": 7,
    "Earnings miss (revenue/EBITDA/EPS)": 8,
    "Earnings guidance cut": 7,
    "Suspension of buyback/dividend cut": 5,
    "Failed acquisition/overpayment/goodwill impairment": 6,
    "Accumulation of debt/toxic assets/failed divestiture": 6,
    "Operational setback (trial halt/satellite failure/production halt)": 7,
    "Insider selling (clustered CEO/CFO sales)": 7,
    "Activist exits stake/13D hostile": 7,
    "Capacity underutilization/overexpansion write‑down": 5,
    "Strategic pivot failure/loss of identity": 5,
    "Supply chain shock (fire/shipping disruption)": 8,
    "Patent litigation loss/IP theft": 7,
    "Customer concentration risk (over‑reliance)": 6,
    "Policy reversal/new regulation/tax increase": 7,
    "Rate hike/monetary tightening/liquidity withdrawal": 9,
    "Adverse litigation outcome/patent invalidation/antitrust ruling": 8,
    "Geopolitical event that hurts sector": 8,
    "Sector headwind/index exclusion/rotation away": 6,
    "Regulatory denial or delay/antitrust block": 9,
    "Macro headwinds (inflation spike/recession/unemployment surge)": 8,
    "Sector rotation out of the industry": 6,
    "Unfavorable commodity price move (higher input costs)": 6,
    "ESG controversy/exclusion from green funds/carbon tax": 4,
    "Currency headwind (dollar strength for exporters)": 3,
    "Technical breakdown (below support, death cross)": 5,
    "Short attack/bear raid (activist short report)": 8,
    "Institutional ownership decline (major holders reducing)": 6,
}

# ── System Prompt (main analysis) ───────────────────────
SYSTEM_PROMPT_TEMPLATE = """\
You are a rigorous event‑driven equity analyst with a 1‑2 week horizon.

TODAY IS {today}. Use ONLY information from {lookback_start} to {today}.

────────────── HEALTH CHECK (pre‑computed from internal database) ──────────────
{health_check}

────────────── CATALYST TAXONOMY & WEIGHTS ──────────────
You MUST output a COMPLETE catalyst grid that includes EVERY catalyst below.
For each catalyst, set status: "HIT" if evidence was found in the searches, "MISS" if the catalyst type was looked for but no evidence found, "N/A" if the catalyst does not apply to this company.
Use the weight provided; adjust ±1 only if the magnitude is unusually large or small.
Weights:
{weights}

────────────── REQUIRED SEARCH LENSES ──────────────────
1. Company‑specific news (contracts, products, management, earnings, guidance)
2. Insider trading – buying AND selling, Form 4 filings
3. Analyst actions – upgrades, downgrades, PT changes, initiations
4. Institutional ownership – 13F filings, major holder changes
5. Short interest – % of float, days‑to‑cover, trend
6. Financial health – debt, cash, profitability, liabilities
7. Valuation – P/E, P/S, EV/EBITDA vs peers
8. Sector & peer events
9. Supply chain & geopolitical risks
10. Regulatory & legislative environment
11. Macro context (Fed, rates, inflation, commodities, GDP)
12. Technical analysis (moving averages, support/resistance, volume)

────────────── CONTEXT RULES ──────────────────────────
1. ANALYST TARGETS: If target < current price → NEGATIVE.
2. INSIDER TRADING: Distinguish 10b5‑1 routine from discretionary cluster sales.
3. SHORT INTEREST: >15% = squeeze risk (bullish if positive catalysts).
4. VALUATION: Compare to sector medians; >200% above consensus analyst target = high downside risk.
5. FINANCIAL HEALTH: Negative ROE or profit margins → negative catalyst.
6. TARIFF/GEO: Quantify exposure % and weigh against current policy.

────────────── OUTPUT FORMAT ───────────────────────────
Return ONLY the JSON object below. Do NOT add commentary.

{{
  "ticker": "...",
  "analysis_date": "{today}",
  "lookback_start": "{lookback_start}",
  "current_price": "extracted from search results or health check",
  "catalyst_grid": [
    {{
      "taxonomy": "exact label from the list",
      "type": "positive|negative",
      "category": "internal|external|market_mechanic",
      "status": "HIT|MISS|N/A",
      "weight": 0‑10,
      "evidence": "If HIT: exact date & source summary. If MISS: empty string.",
      "source_urls": [],
      "confidence": 0‑100
    }}
  ],
  "catalyst_stack": "4‑sentence narrative with specific dates.",
  "net_signal": "Strong Bullish|Bullish|Neutral|Bearish|Strong Bearish",
  "conviction": 0‑100,
  "key_assumption": "Single assumption that, if wrong, flips the call.",
  "search_queries_used": ["..."]
}}
"""

# ── Audit prompt ────────────────────────────────────────
AUDIT_PROMPT = """\
You are a forensic financial audit layer. Review the catalyst analysis below.
TODAY IS {today}. The stock's current price should appear in the analysis.

CORRECT EVERY MISCLASSIFICATION:
1. If any analyst price target is BELOW the current stock price, reclassify it as NEGATIVE ("type": "negative", "taxonomy": "Analyst downgrade/PT cut") regardless of whether the target was recently raised.
2. If a target is ABOVE the current price, keep it positive.
3. For insider transactions: if net selling exceeds net buying, add a negative catalyst explicitly. If sales are routine 10b5‑1, note that but keep the negative.
4. If short interest >15% and positive catalysts dominate, add a positive "Short squeeze" catalyst to market_mechanic.
5. If ROE is negative or profit margins are negative, add a negative catalyst.
6. If debt/equity is rising rapidly or liabilities surged, add a negative catalyst.
7. Remove any vague language; every event must cite an exact date.
8. Ensure the catalyst_grid contains EVERY catalyst from the taxonomy, even if only with status "MISS" or "N/A".
9. The top‑level JSON MUST contain "net_signal" and "conviction" fields. If they are missing, infer them from the catalyst_stack.

Return ONLY the corrected JSON with NO additional commentary.
"""

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

# ── Build health snapshot from DB ───────────────────────
def build_health_snapshot(ticker: str, conn) -> dict:
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

# ── Generate health check evaluation (LLM call) ─────────
def generate_health_check(ticker: str, snapshot: dict) -> str:
    snapshot_text = json.dumps(snapshot, indent=2, default=str)
    messages = [
        {"role": "system", "content": HEALTH_CHECK_SYSTEM},
        {"role": "user", "content": f"Ticker: {ticker}\nSnapshot:\n{snapshot_text}\n\nProduce the evaluation."}
    ]
    try:
        resp = safe_create(model=MODEL, messages=messages, temperature=0.3, max_tokens=800)
        return resp.choices[0].message.content.strip()
    except Exception as e:
        print(f"  ⚠️  Health check LLM failed: {e}")
        return "Health check unavailable."

# ── Main analysis function ──────────────────────────────
def analyze_stock(ticker: str, health_check_text: str) -> dict:
    weights_str = "\n".join([f"  - {k}: {v}" for k, v in CATALYST_WEIGHTS.items()])
    system_prompt = SYSTEM_PROMPT_TEMPLATE.format(
        today=TODAY,
        lookback_start=LOOKBACK_START,
        health_check=health_check_text,
        weights=weights_str,
    )

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": f"""\
Ticker: {ticker}
Today is {TODAY}. Analyse ALL catalysts for {ticker} within the last 6 months ({LOOKBACK_START} to {TODAY}).
Use web_search to cover ALL required lenses. Be exhaustive. Pin every catalyst to an exact date.
Always compare analyst targets to the CURRENT stock price."""}
    ]

    search_count = 0
    search_queries_used = []

    # Phase 1: Web search loop
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
            cats = args.get("categories", "general,news")

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

    # Force final answer if needed
    if (msg.tool_calls and search_count >= MAX_SEARCHES) or not (msg.content and msg.content.strip()):
        messages.append({
            "role": "user",
            "content": "All searches exhausted. Produce the final JSON analysis NOW. Output ONLY the JSON object."
        })
        try:
            response = safe_create(model=MODEL, messages=messages, temperature=0.2)
        except Exception as e:
            return {"error": f"API call failed during forced final: {e}", "search_count": search_count}
        msg = response.choices[0].message

    # Extract JSON
    final_text = (msg.content or "").strip()
    if not final_text:
        return {"error": "Empty final response", "search_count": search_count}

    if final_text.startswith("```"):
        final_text = final_text.split("\n", 1)[1].rsplit("```", 1)[0]
    final_text = final_text.strip()

    # —— DIAGNOSTIC: show raw output before audit ——
    print("  📝 RAW FINAL OUTPUT (first 800 chars):", final_text[:800])

    # Phase 2: Audit
    print("  🧠 Running context audit layer…")
    audit_messages = [
        {"role": "system", "content": AUDIT_PROMPT.format(today=TODAY)},
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
        print(f"  ⚠️  Audit layer failed: {e}")

    # JSON repair
        # ── JSON repair ──
    try:
        result = json.loads(final_text)
    except json.JSONDecodeError as first_exc:
        print(f"  ⚠️  Initial JSON parse failed: {first_exc}")
        print(f"  📄 Faulty JSON (first 2000 chars): {final_text[:2000]}")
        print(f"  📄 Faulty JSON (last 500 chars): {final_text[-500:]}")

        # 1. Simple clean: trailing commas
        fixed = re.sub(r",\s*}", "}", final_text)
        fixed = re.sub(r",\s*]", "]", fixed)
        try:
            result = json.loads(fixed)
            print("  ✅ Fixed with trailing‑comma removal.")
        except json.JSONDecodeError:
            # 2. Ask DeepSeek to fix, but with a size limit
            repair_payload = fixed[:30000]  # never send more than 30K chars
            try:
                fix_resp = safe_create(
                    model=MODEL,
                    messages=[
                        {"role": "system", "content": (
                            "Return ONLY valid JSON. If the input is truncated, close all open "
                            "brackets/braces and add ']' to close the catalyst_grid array "
                            "and '}' to close the outer object."
                        )},
                        {"role": "user", "content": f"Fix this JSON:\n\n{repair_payload}"}
                    ],
                    temperature=0.0,
                    max_tokens=800  # keep the fix attempt small
                )
                fixed2 = fix_resp.choices[0].message.content.strip()
                if fixed2.startswith("```"):
                    fixed2 = fixed2.split("\n", 1)[1].rsplit("```", 1)[0]
                result = json.loads(fixed2)
                print("  ✅ Fixed with DeepSeek repair.")
            except Exception as repair_exc:
                print(f"  ❌ DeepSeek repair also failed: {repair_exc}")
                # Dump the actual broken text to the workflow log so we can debug
                print("  ╔══════════════════════════════════════╗")
                print("  ║  BEGIN BROKEN JSON (up to 5K chars) ║")
                print("  ╚══════════════════════════════════════╝")
                print(final_text[:5000])
                return {
                    "error": f"JSON parse failed after repair: {repair_exc}",
                    "raw_preview": final_text[:2000],
                    "search_count": search_count,
                    "search_queries_used": search_queries_used,
                }

    # ── Fallback for missing net_signal ──
    if result.get("net_signal") is None:
        stack = (result.get("catalyst_stack") or "").lower()
        if "strong bullish" in stack:
            result["net_signal"] = "Strong Bullish"
        elif "bullish" in stack:
            result["net_signal"] = "Bullish"
        elif "strong bearish" in stack:
            result["net_signal"] = "Strong Bearish"
        elif "bearish" in stack:
            result["net_signal"] = "Bearish"
        else:
            # Count positive vs negative hits
            positive = sum(1 for c in result.get("catalyst_grid", []) if c.get("type") == "positive" and c.get("status") == "HIT")
            negative = sum(1 for c in result.get("catalyst_grid", []) if c.get("type") == "negative" and c.get("status") == "HIT")
            if positive > negative:
                result["net_signal"] = "Bullish" if positive - negative >= 2 else "Cautiously Bullish"
            elif negative > positive:
                result["net_signal"] = "Bearish" if negative - positive >= 2 else "Cautiously Bearish"
            else:
                result["net_signal"] = "Neutral"
        if result.get("conviction") is None:
            result["conviction"] = 50

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
        print("⚠️  psycopg2 not installed – no DB health check will be performed.")

    try:
        with open("data/exposure_profiles.json") as f:
            profiles = json.load(f)
    except FileNotFoundError:
        profiles = {}
        print("⚠️  No exposure_profiles.json.")

    conn = None
    cur = None

    # ═══════════════════════════════════════════════════
    # TEST MODE: only BBAI
    # ═══════════════════════════════════════════════════
    tickers = ["BBAI"]

    # Full watchlist (uncomment when ready):
    # if HAS_DB:
    #     conn = get_connection()
    #     cur = conn.cursor()
    #     cur.execute("SELECT ticker FROM ticker_master WHERE is_active = true ORDER BY ticker")
    #     tickers = [row[0] for row in cur.fetchall()]
    # else:
    #     tickers = list(profiles.keys())

    for ticker in tickers:
        profile = profiles.get(ticker, {})
        print(f"\n{'='*60}\n📊 Health check for {ticker}…\n{'='*60}")

        health_text = ""
        if HAS_DB:
            try:
                if conn is None:
                    conn = get_connection()
                    cur = conn.cursor()
                snap = build_health_snapshot(ticker, conn)
                health_text = generate_health_check(ticker, snap)
                print(health_text)
            except Exception as e:
                print(f"⚠️  Health check error: {e}")

        print(f"\n{'='*60}\n📊 Catalyst analysis for {ticker}…\n{'='*60}")
        result = analyze_stock(ticker, health_text)

        if "error" in result:
            print(f"❌ Error: {result['error']}")
        else:
            print(f"✅ Net signal: {result.get('net_signal')} (conviction {result.get('conviction')})")
            grid = result.get("catalyst_grid", [])
            print(f"   Catalyst grid: {len(grid)} items")
            for item in grid[:5]:
                print(f"     [{item.get('type','?')}] {item.get('taxonomy')}: {item.get('status')} (weight {item.get('weight')})")
            # Print hit count
            hits = sum(1 for c in grid if c.get("status") == "HIT")
            print(f"   HIT: {hits}, MISS: {len(grid)-hits}")

        if HAS_DB and "error" not in result:
            if conn is None:
                conn = get_connection()
                cur = conn.cursor()
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
                result.get("ticker", ticker),
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
