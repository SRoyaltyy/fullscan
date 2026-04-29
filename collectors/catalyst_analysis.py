"""
Catalyst Analysis Engine
- DeepSeek function calling → SearXNG web search → DB cache → structured signals
Schedule: Daily, after all data collectors finish
"""

import os, json, time, requests
from datetime import datetime, timedelta, timezone
from openai import OpenAI

# -------------------- CONFIG --------------------
SEARXNG_URL   = os.environ["SEARXNG_URL"]         # https://your-app.up.railway.app
SEARXNG_TIMEOUT = 15                               # seconds
MAX_SEARCHES_PER_STOCK = 5                         # prevent runaway tool calls
SEARCH_DELAY  = 0.6                                # seconds between searches (rate‑limit safety)

# -------------------- TOOLS DEFINITION --------------------
TOOLS = [{
    "type": "function",
    "function": {
        "name": "web_search",
        "description": (
            "Search the live web for real‑time financial news, filings, contracts, "
            "regulatory decisions, patents, and analyst actions. "
            "Use this whenever up‑to‑date information is required. "
            "For site‑specific searches, prefix the query with 'site:domain.com'."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": (
                        "Search query. Be specific: include company name, topic, "
                        "and year if relevant. Examples:\n"
                        "- 'AAOI Applied Optoelectronics latest contract award 2026'\n"
                        "- 'semiconductor optical transceiver market outlook 2026'\n"
                        "- 'site:finviz.com AAOI news today'"
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

# -------------------- SEARXNG EXECUTOR --------------------
def web_search(query: str, categories: str = "general,news") -> str:
    """Execute a search against the SearXNG instance. Returns formatted results or error text."""
    try:
        resp = requests.get(
            f"{SEARXNG_URL}/search",
            params={"q": query, "format": "json", "categories": categories},
            timeout=SEARXNG_TIMEOUT,
            headers={"User-Agent": "CatalystEngine/1.0"}
        )
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        return f"SEARCH_ERROR: Could not execute search ({e})"

    results = data.get("results", [])
    if not results:
        return "SEARCH_RESULT: No results found for this query."

    formatted = []
    for r in results[:8]:
        engine  = r.get("engine", "unknown")
        title   = r.get("title", "No title")
        snippet = r.get("content", "")[:400]
        url     = r.get("url", "")
        date    = r.get("publishedDate") or ""
        formatted.append(f"[{engine}] {title}\n{date}\n{snippet}\n{url}")

    return "\n\n".join(formatted)

# -------------------- CLIENT --------------------
client = OpenAI(
    api_key=os.environ["DEEPSEEK_API_KEY"],
    base_url="https://api.deepseek.com"
)

# -------------------- MAIN ANALYSIS FUNCTION --------------------
def analyze_stock(ticker: str, exposure_profile: dict, last_signals: list[str] = None) -> dict:
    system_prompt = """\
You are an event‑driven equity analyst with a 1‑2 week horizon.
You have access to a live web search tool.  USE IT — do not guess about recent events.
For each stock, you MUST search for:
1.  Latest company‑specific news (contracts, product launches, management changes)
2.  Regulatory / legislative developments affecting the sector
3.  Relevant macro data (rates, commodity prices, geopolitical events)
4.  Competitor or peer events that may spill over

After gathering data, output a STRICT JSON object with these fields:
{
  "ticker": "...",
  "date": "YYYY-MM-DD",
  "active_catalysts": [
    {"type": "positive|negative|neutral", "category": "internal|external|market_mechanic",
     "summary": "one sentence", "source_urls": ["..."], "confidence": 0-100}
  ],
  "catalyst_stack": "A 4‑sentence narrative explaining why the net effect tips bullish or bearish in the next 1‑2 weeks.",
  "net_signal": "Strong Bullish|Bullish|Neutral|Bearish|Strong Bearish",
  "conviction": 0-100,
  "key_assumption": "The single assumption that, if wrong, would flip the call.",
  "search_queries_used": ["..."]
}
"""

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": f"""\
Ticker: {ticker}
Exposure profile: {json.dumps(exposure_profile)}
Previous signals (from DB): {json.dumps(last_signals) if last_signals else 'None'}

Analyse all active catalysts for {ticker} using web search.  Be exhaustive.  Cite sources."""}
    ]

    search_count = 0
    search_queries_used = []

    # -------- Round 1 with tools --------
    response = client.chat.completions.create(
        model="deepseek-chat",    # or "deepseek-v4-flash"
        messages=messages,
        tools=TOOLS,
        tool_choice="auto",
        temperature=0.3,
    )
    msg = response.choices[0].message

    # -------- Execute tools loop --------
    while msg.tool_calls and search_count < MAX_SEARCHES_PER_STOCK:
        messages.append(msg)

        for tc in msg.tool_calls:
            if tc.function.name != "web_search":
                continue
            args = json.loads(tc.function.arguments)
            query = args.get("query", "")
            categories = args.get("categories", "general,news")
            print(f"  🔍 Searching: {query[:100]}…")
            result_text = web_search(query, categories)
            messages.append({
                "role": "tool",
                "tool_call_id": tc.id,
                "content": result_text
            })
            search_queries_used.append(query)
            search_count += 1
            time.sleep(SEARCH_DELAY)

        # Ask again, still with tools available
        response = client.chat.completions.create(
            model="deepseek-chat",
            messages=messages,
            tools=TOOLS,
            tool_choice="auto",
            temperature=0.3,
        )
        msg = response.choices[0].message

    # -------- FALLBACK if model didn't produce text --------
    # (e.g. it wanted more searches but we hit the limit)
    if not msg.content or not msg.content.strip():
        # Force a final synthesis without tools
        messages.append({"role": "user", "content": (
            "You have now completed all searches.  Based strictly on the search results "
            "and the internal data provided above, produce the final JSON analysis.  "
            "Do NOT ask for more searches — use only the information you have."
        )})
        response = client.chat.completions.create(
            model="deepseek-chat",
            messages=messages,
            # no tools → forces text output
            temperature=0.3,
        )
        msg = response.choices[0].message

    # -------- Extract JSON --------
    final_text = msg.content.strip()
    # Remove markdown code fences if present
    if final_text.startswith("```"):
        final_text = final_text.split("\n", 1)[1]
        final_text = final_text.rsplit("```", 1)[0]
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
            "search_queries_used": search_queries_used
        }
if __name__ == "__main__":
    # Example: analyse AAOI
    profile = {
        "sector": "Optical Networking",
        "debt_load": "medium",
        "govt_contract_exposure": 0.7,
        "key_revenue_drivers": ["data centre transceivers", "CATV", "defence optics"],
        "key_risks": ["China supply chain", "tariffs", "optical component pricing"]
    }

    result = analyze_stock("AAOI", profile)
    print(json.dumps(result, indent=2))
