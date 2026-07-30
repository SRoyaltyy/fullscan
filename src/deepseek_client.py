"""DeepSeek API client (OpenAI-compatible) with a web_search tool loop.

Search backend: SearXNG if SEARXNG_URL is set, else DuckDuckGo (ddgs package).
Stages needing tools must run on deepseek-chat (deepseek-reasoner has no
function-calling support).
"""
from __future__ import annotations

import json
import time

import requests

from . import config

SEARCH_TOOL = {
    "type": "function",
    "function": {
        "name": "web_search",
        "description": "Search the live web. Returns titles, URLs, snippets.",
        "parameters": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "search query"},
            },
            "required": ["query"],
        },
    },
}


def web_search(query: str, max_results: int = 6) -> str:
    """Return JSON string of results; never raises."""
    try:
        if config.SEARXNG_URL:
            r = requests.get(f"{config.SEARXNG_URL}/search",
                             params={"q": query, "format": "json"},
                             timeout=20)
            r.raise_for_status()
            items = [{"title": x.get("title"), "url": x.get("url"),
                      "snippet": x.get("content")}
                     for x in r.json().get("results", [])[:max_results]]
        else:
            from ddgs import DDGS
            with DDGS() as ddgs:
                items = [{"title": x.get("title"), "url": x.get("href"),
                          "snippet": x.get("body")}
                         for x in ddgs.text(query, max_results=max_results)]
        return json.dumps({"query": query, "results": items},
                          ensure_ascii=False)
    except Exception as e:  # noqa: BLE001
        return json.dumps({"query": query, "error": str(e)})


def _post(payload: dict, retries: int = 4) -> dict:
    url = f"{config.DEEPSEEK_BASE_URL}/chat/completions"
    headers = {"Authorization": f"Bearer {config.DEEPSEEK_API_KEY}",
               "Content-Type": "application/json"}
    last = None
    for attempt in range(retries):
        try:
            r = requests.post(url, headers=headers, json=payload, timeout=300)
            if r.status_code in (429, 500, 502, 503):
                last = f"HTTP {r.status_code}: {r.text[:200]}"
                time.sleep(20 * (attempt + 1))
                continue
            r.raise_for_status()
            return r.json()
        except requests.RequestException as e:
            last = str(e)
            time.sleep(20 * (attempt + 1))
    raise RuntimeError(f"DeepSeek call failed after {retries} tries: {last}")


def chat(messages: list[dict], model: str, tools: bool = False,
         max_tokens: int = 8000, temperature: float = 0.2) -> str:
    """Chat completion; if tools=True, runs a web_search tool loop and
    returns the final assistant text."""
    payload = {"model": model, "messages": messages,
               "max_tokens": max_tokens, "temperature": temperature}
    if tools:
        payload["tools"] = [SEARCH_TOOL]
        payload["tool_choice"] = "auto"

    for _round in range(config.MAX_TOOL_ROUNDS if tools else 1):
        payload["messages"] = messages
        resp = _post(payload)
        msg = resp["choices"][0]["message"]
        calls = msg.get("tool_calls") or []
        if not calls:
            return msg.get("content") or ""
        messages.append({"role": "assistant", "content": msg.get("content"),
                         "tool_calls": calls})
        for call in calls:
            args = json.loads(call["function"]["arguments"] or "{}")
            result = web_search(args.get("query", ""))
            messages.append({"role": "tool", "tool_call_id": call["id"],
                             "content": result})
    # tool budget exhausted -> one final no-tool answer
    payload.pop("tools", None)
    payload.pop("tool_choice", None)
    resp = _post(payload)
    return resp["choices"][0]["message"].get("content") or ""
