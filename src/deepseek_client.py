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


def _searxng(query: str, max_results: int) -> list[dict]:
    r = requests.get(f"{config.SEARXNG_URL}/search",
                     params={"q": query, "format": "json"},
                     timeout=10)
    r.raise_for_status()
    return [{"title": x.get("title"), "url": x.get("url"),
             "snippet": x.get("content")}
            for x in r.json().get("results", [])[:max_results]]


def _ddg(query: str, max_results: int) -> list[dict]:
    from ddgs import DDGS
    with DDGS() as ddgs:
        return [{"title": x.get("title"), "url": x.get("href"),
                 "snippet": x.get("body")}
                for x in ddgs.text(query, max_results=max_results)]


def web_search(query: str, max_results: int = 6) -> str:
    """Return JSON string of results; never raises. SearXNG first (if
    configured), DuckDuckGo as automatic fallback when SearXNG errors."""
    backend = "searxng" if config.SEARXNG_URL else "ddg"
    try:
        items = (_searxng(query, max_results) if config.SEARXNG_URL
                 else _ddg(query, max_results))
        return json.dumps({"query": query, "backend": backend,
                           "results": items}, ensure_ascii=False)
    except Exception as e:  # noqa: BLE001
        if config.SEARXNG_URL:  # searxng failed -> transparent DDG fallback
            print(f"[search] searxng failed ({e}); falling back to DDG")
            try:
                items = _ddg(query, max_results)
                return json.dumps({"query": query, "backend": "ddg_fallback",
                                   "results": items}, ensure_ascii=False)
            except Exception as e2:  # noqa: BLE001
                return json.dumps({"query": query,
                                   "error": f"searxng: {e}; ddg: {e2}"})
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
         max_tokens: int = 8000, temperature: float = 0.2,
         transcript_path: str | None = None,
         trace_path: str | None = None, stage_label: str = "") -> str:
    """Chat completion; if tools=True, runs a web_search tool loop and
    returns the final assistant text. If transcript_path is set, the FULL
    conversation (every tool call + every search result + final answer) is
    dumped there as JSON for audit. If trace_path is set, a human-readable
    step-by-step reasoning log is written there as Markdown."""
    import copy
    import os

    payload = {"model": model, "messages": messages,
               "max_tokens": max_tokens, "temperature": temperature}
    if tools:
        payload["tools"] = [SEARCH_TOOL]
        payload["tool_choice"] = "auto"

    trace = [f"# Reasoning trace — {stage_label or 'llm run'}", ""]
    sys_chars = sum(len(str(m.get('content') or '')) for m in messages)
    trace.append(f"**Step 0 — Setup.** Loaded the rubric, standing lessons, "
                 f"and Channel 1 data ({sys_chars:,} characters of input). "
                 f"Model: `{model}`. "
                 + ("Web search is ENABLED; the model must research current "
                    "events before judging." if tools else
                    "Web search is disabled for this stage; the model works "
                    "only from the documents it was given."))
    trace.append("")

    step = 0
    final = None
    for _round in range(config.MAX_TOOL_ROUNDS if tools else 1):
        payload["messages"] = messages
        resp = _post(payload)
        msg = resp["choices"][0]["message"]
        calls = msg.get("tool_calls") or []
        if not calls:
            final = msg.get("content") or ""
            messages.append({"role": "assistant", "content": final})
            step += 1
            trace.append(f"**Step {step} — Done researching.** The model "
                         f"stopped searching and wrote its full analysis "
                         f"({len(final):,} characters).")
            break
        step += 1
        messages.append({"role": "assistant", "content": msg.get("content"),
                         "tool_calls": calls})
        for call in calls:
            args = json.loads(call["function"]["arguments"] or "{}")
            q = args.get("query", "")
            result = web_search(q)
            try:
                parsed = json.loads(result)
                n = len(parsed.get("results", []))
                if parsed.get("error"):
                    trace.append(f"**Step {step} — Research.** The model "
                                 f"wanted to know: *\"{q}\"* → ❌ search "
                                 f"failed ({parsed['error'][:120]})")
                else:
                    trace.append(f"**Step {step} — Research.** The model "
                                 f"wanted to know: *\"{q}\"* → got {n} "
                                 f"results (via {parsed.get('backend', '?')})")
                    for it in parsed.get("results", [])[:3]:
                        trace.append(f"  - {it.get('title', '?')} "
                                     f"({it.get('url', '')})")
            except ValueError:
                trace.append(f"**Step {step} — Research.** searched "
                             f"*\"{q}\"* (unparsed result)")
            step += 1
            messages.append({"role": "tool", "tool_call_id": call["id"],
                             "content": result})
    if final is None:
        # tool budget exhausted -> one final no-tool answer
        trace.append(f"**Step {step + 1} — Search budget exhausted.** Forced "
                     "to conclude with what it already gathered.")
        payload.pop("tools", None)
        payload.pop("tool_choice", None)
        resp = _post(payload)
        final = resp["choices"][0]["message"].get("content") or ""
        messages.append({"role": "assistant", "content": final})

    if transcript_path:
        try:
            os.makedirs(os.path.dirname(transcript_path), exist_ok=True)
            with open(transcript_path, "w", encoding="utf-8") as fh:
                json.dump({"model": model,
                           "messages": copy.deepcopy(messages)}, fh,
                          indent=2, ensure_ascii=False, default=str)
        except OSError as e:
            print(f"[transcript] save failed: {e}")
    if trace_path:
        try:
            os.makedirs(os.path.dirname(trace_path), exist_ok=True)
            with open(trace_path, "w", encoding="utf-8") as fh:
                fh.write("\n\n".join(trace) + "\n")
        except OSError as e:
            print(f"[trace] save failed: {e}")
    return final
