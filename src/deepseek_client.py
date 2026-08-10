"""DeepSeek API client (OpenAI-compatible) with a web_search tool loop.

Search backend chain (every step logged with result counts):
  1. own SearXNG (SEARXNG_URL)        — 2 attempts, 25s timeout;
                                        EMPTY result set counts as failure
                                        (instance up but engines blocked)
  2. ddgs package (DuckDuckGo API-ish)
  3. DuckDuckGo HTML endpoint scrape  — no key, verified working
  4. Google News RSS                  — no key, verified working; great for
                                        event/news queries

Stages needing tools must run on deepseek-chat (deepseek-reasoner has no
function-calling support).
"""
from __future__ import annotations

import json
import re
import time
import xml.etree.ElementTree as ET
from html import unescape
from urllib.parse import quote_plus, unquote

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

SEARXNG_TIMEOUT = 25
SEARXNG_ATTEMPTS = 2
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126.0 Safari/537.36")


def _searxng(query: str, max_results: int) -> list[dict]:
    r = requests.get(f"{config.SEARXNG_URL}/search",
                     params={"q": query, "format": "json"},
                     timeout=SEARXNG_TIMEOUT)
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


def _strip_tags(s: str) -> str:
    return unescape(re.sub(r"<[^>]+>", "", s or "")).strip()


def _ddg_html(query: str, max_results: int) -> list[dict]:
    """DuckDuckGo HTML endpoint — no key, no library."""
    r = requests.get("https://html.duckduckgo.com/html/",
                     params={"q": query}, headers={"User-Agent": UA},
                     timeout=20)
    r.raise_for_status()
    doc = r.text
    links = re.findall(
        r'<a[^>]*class="result__a"[^>]*href="([^"]+)"[^>]*>(.*?)</a>', doc)
    snips = re.findall(r'class="result__snippet"[^>]*>(.*?)</a>', doc)
    out = []
    for i, (u, t) in enumerate(links[:max_results]):
        m = re.search(r"uddg=([^&]+)", u)
        url = unquote(m.group(1)) if m else u
        snippet = _strip_tags(snips[i]) if i < len(snips) else ""
        out.append({"title": _strip_tags(t), "url": url, "snippet": snippet})
    if not out:
        raise RuntimeError("ddg_html: no results parsed (possible block page)")
    return out


def _gnews_rss(query: str, max_results: int) -> list[dict]:
    """Google News RSS — no key. News-flavoured results."""
    url = ("https://news.google.com/rss/search?q=" + quote_plus(query)
           + "&hl=en-US&gl=US&ceid=US:en")
    r = requests.get(url, headers={"User-Agent": UA}, timeout=20)
    r.raise_for_status()
    root = ET.fromstring(r.content)
    out = []
    for it in root.findall(".//item")[:max_results]:
        out.append({
            "title": it.findtext("title", ""),
            "url": it.findtext("link", ""),
            "snippet": f"{it.findtext('pubDate', '')} — "
                       f"{_strip_tags(it.findtext('description', ''))[:300]}",
        })
    if not out:
        raise RuntimeError("gnews_rss: no items parsed")
    return out


def web_search(query: str, max_results: int = 6) -> str:
    """Return JSON string of results; never raises. Walks the backend chain,
    logging which backend served (or failed) each query."""
    errors = []

    # 1. own SearXNG (empty result set = broken upstream engines -> fail over)
    if config.SEARXNG_URL:
        for attempt in range(SEARXNG_ATTEMPTS):
            try:
                items = _searxng(query, max_results)
                if not items:
                    raise RuntimeError("searxng returned 0 results "
                                       "(upstream engines likely blocked)")
                print(f"[search] searxng OK ({len(items)} results)")
                return json.dumps({"query": query, "backend": "searxng",
                                   "results": items}, ensure_ascii=False)
            except Exception as e:  # noqa: BLE001
                errors.append(f"searxng#{attempt + 1}: {e}")
                if attempt < SEARXNG_ATTEMPTS - 1:
                    time.sleep(3)
        print(f"[search] searxng failed x{SEARXNG_ATTEMPTS} "
              f"({errors[-1][:100]})")

    # 2. ddgs package
    try:
        items = _ddg(query, max_results)
        if not items:
            raise RuntimeError("ddgs returned 0 results")
        print(f"[search] ddg(lib) OK ({len(items)} results)")
        return json.dumps({"query": query, "backend": "ddg",
                           "results": items}, ensure_ascii=False)
    except Exception as e:  # noqa: BLE001
        errors.append(f"ddgs: {e}")
        print(f"[search] ddg(lib) failed: {str(e)[:100]}")

    # 3. DDG HTML scrape
    try:
        items = _ddg_html(query, max_results)
        print(f"[search] ddg_html OK ({len(items)} results)")
        return json.dumps({"query": query, "backend": "ddg_html",
                           "results": items}, ensure_ascii=False)
    except Exception as e:  # noqa: BLE001
        errors.append(f"ddg_html: {e}")
        print(f"[search] ddg_html failed: {str(e)[:100]}")

    # 4. Google News RSS
    try:
        items = _gnews_rss(query, max_results)
        print(f"[search] gnews_rss OK ({len(items)} results)")
        return json.dumps({"query": query, "backend": "gnews_rss",
                           "results": items}, ensure_ascii=False)
    except Exception as e:  # noqa: BLE001
        errors.append(f"gnews_rss: {e}")
        print(f"[search] gnews_rss failed: {str(e)[:100]}")

    print(f"[search] ALL BACKENDS FAILED for query: {query!r}")
    return json.dumps({"query": query,
                       "error": " | ".join(str(e)[:120] for e in errors)})


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
         trace_path: str | None = None, stage_label: str = "",
         max_rounds: int | None = None) -> str:
    """Chat completion; if tools=True, runs a web_search tool loop and
    returns the final assistant text. If transcript_path is set, the FULL
    conversation (every tool call + every search result + final answer) is
    dumped there as JSON for audit. If trace_path is set, a human-readable
    step-by-step reasoning log is written there as Markdown. max_rounds
    overrides config.MAX_TOOL_ROUNDS for search-heavy stages."""
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

    rounds = (max_rounds or config.MAX_TOOL_ROUNDS) if tools else 1
    step = 0
    final = None
    for _round in range(rounds):
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
