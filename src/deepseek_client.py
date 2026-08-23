"""Multi-provider LLM client (OpenAI-compatible) with a web_search tool loop.

Routes by model id:
  grok-* / xai/* / x-ai/*  →  https://api.x.ai/v1   (XAI_API_KEY)
  anything else            →  https://api.deepseek.com  (DEEPSEEK_API_KEY)

If the primary provider fails (or has no key), the call falls back to the
other provider when that key is present.

The actual search backend chain lives in src/websearch.py (single source of
truth, shared with the collectors):
  1. own SearXNG (SEARXNG_URL)        — 2 attempts, 25s timeout;
                                        EMPTY result set counts as failure
                                        (instance up but engines blocked)
  2. ddgs package (DuckDuckGo API-ish)
  3. DuckDuckGo HTML endpoint scrape  — no key, verified working
  4. Google News RSS                  — no key, verified working; great for
                                        event/news queries

Stages needing tools should use a tool-capable model (deepseek-chat or
grok-4.6). deepseek-reasoner has no function-calling support.
"""
from __future__ import annotations

import json
import time

import requests

from . import config
from .websearch import search_results

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
    """Return JSON string of results; never raises. Delegates to the shared
    backend chain in src/websearch.py (logs which backend served/failed)."""
    backend, items, errors = search_results(query, max_results)
    if backend is None:
        return json.dumps({"query": query,
                           "error": " | ".join(str(e)[:120]
                                               for e in errors)})
    return json.dumps({"query": query, "backend": backend,
                       "results": items}, ensure_ascii=False)


def _prepare_payload(payload: dict) -> dict:
    """Copy payload and apply provider-specific fields."""
    out = dict(payload)
    model = out.get("model") or ""
    if config.is_xai_model(model):
        # Chat Completions on xAI deprecates max_tokens in favor of
        # max_completion_tokens (counts visible output only).
        if "max_tokens" in out and "max_completion_tokens" not in out:
            out["max_completion_tokens"] = out.pop("max_tokens")
        if config.XAI_REASONING_EFFORT:
            out["reasoning_effort"] = config.XAI_REASONING_EFFORT
        out.setdefault("prompt_cache_key", "fullscan")
    return out


def _post(payload: dict, retries: int = 4) -> dict:
    model = payload.get("model") or ""
    provider = config.provider_for(model)
    key = config.api_key_for(model)
    if not key:
        raise RuntimeError(
            f"no API key for {provider} model {model!r} "
            f"({'XAI_API_KEY' if provider == 'xai' else 'DEEPSEEK_API_KEY'} "
            "is empty)"
        )
    url = f"{config.base_url_for(model)}/chat/completions"
    body = _prepare_payload(payload)
    headers = {"Authorization": f"Bearer {key}",
               "Content-Type": "application/json"}
    timeout = 600 if provider == "xai" else 300
    last = None
    for attempt in range(retries):
        try:
            r = requests.post(url, headers=headers, json=body, timeout=timeout)
            if r.status_code in (429, 500, 502, 503):
                last = f"HTTP {r.status_code}: {r.text[:200]}"
                time.sleep(20 * (attempt + 1))
                continue
            if r.status_code >= 400:
                last = f"HTTP {r.status_code}: {r.text[:300]}"
                # 4xx (other than 429) will not heal by waiting
                if r.status_code not in (408, 409):
                    break
                time.sleep(20 * (attempt + 1))
                continue
            return r.json()
        except requests.RequestException as e:
            last = str(e)
            time.sleep(20 * (attempt + 1))
    raise RuntimeError(
        f"{provider} call ({model}) failed after {retries} tries: {last}"
    )


def _post_resilient(payload: dict, tools: bool = False) -> dict:
    """POST, then on hard failure retry once on the other provider."""
    try:
        return _post(payload)
    except RuntimeError as exc:
        orig = payload.get("model") or ""
        fb = config.fallback_model(orig, tools=tools)
        if not fb or fb == orig or not config.has_key_for(fb):
            raise
        print(f"[llm] {orig} failed ({exc}); falling back to {fb}")
        retry = dict(payload)
        retry["model"] = fb
        return _post(retry)


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

    resolved = config.resolve_model(model, tools=tools)
    if resolved != model:
        print(f"[llm] {model} has no key — using {resolved}")
        model = resolved
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
        resp = _post_resilient(payload, tools=tools)
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
        resp = _post_resilient(payload, tools=False)
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


def chat_nonempty(messages: list[dict], ladder: list[tuple[str, int]],
                  tools: bool = False, temperature: float = 0.2,
                  transcript_path: str | None = None,
                  trace_path: str | None = None,
                  stage_label: str = "") -> str:
    """Call chat() down a (model, max_tokens) ladder until a NON-EMPTY answer
    comes back; returns '' if every rung fails.

    Guards against a reasoner burning its entire max_tokens budget on
    hidden reasoning and returning content='' — which previously produced
    blank reflect files and junk empty lessons. Typical ladder:
        config.reflect_ladder()
        # e.g. [(grok-4.6, 12000), (grok-4.6, 16000),
        #       (deepseek-reasoner, 12000), (deepseek-chat, 8000)]
    """
    for i, (model, max_tokens) in enumerate(ladder):
        try:
            text = chat([dict(m) for m in messages], model=model, tools=tools,
                        max_tokens=max_tokens, temperature=temperature,
                        transcript_path=transcript_path, trace_path=trace_path,
                        stage_label=stage_label)
        except Exception as exc:  # noqa: BLE001 — next rung is the recovery
            print(f"[llm] error on attempt {i + 1} "
                  f"(model={model}, max_tokens={max_tokens}): {exc} "
                  "— trying next rung")
            continue
        if text and text.strip():
            if i:
                print(f"[llm] recovered on attempt {i + 1} "
                      f"(model={model}, max_tokens={max_tokens})")
            return text
        print(f"[llm] EMPTY answer on attempt {i + 1} "
              f"(model={model}, max_tokens={max_tokens}) — trying next rung")
    return ""


def describe_routing() -> str:
    """Human-readable provider map. Safe to print; never dumps keys."""
    slots = [
        ("MODEL_PREDICT", config.MODEL_PREDICT),
        ("MODEL_OUTCOME", config.MODEL_OUTCOME),
        ("MODEL_JUDGE", config.MODEL_JUDGE),
        ("MODEL_REFLECT", config.MODEL_REFLECT),
        ("MODEL_DISTILL", config.MODEL_DISTILL),
        ("MODEL_DEEPTHINK", config.MODEL_DEEPTHINK),
    ]
    lines = [
        f"DEEPSEEK_API_KEY: {'set' if config.DEEPSEEK_API_KEY else 'MISSING'}",
        f"XAI_API_KEY:      {'set' if config.XAI_API_KEY else 'MISSING'}",
        f"XAI_BASE_URL:     {config.XAI_BASE_URL}",
        "",
        "slot              model                    provider  key",
    ]
    for name, model in slots:
        resolved = config.resolve_model(model)
        provider = config.provider_for(resolved)
        key_ok = "yes" if config.has_key_for(resolved) else "NO"
        note = f"  (resolved from {model})" if resolved != model else ""
        lines.append(
            f"{name:<18}{resolved:<25}{provider:<10}{key_ok}{note}"
        )
    lines.append("")
    lines.append("reflect ladder: " + ", ".join(
        f"{m}@{n}" for m, n in config.reflect_ladder()
    ))
    if not config.XAI_API_KEY:
        lines.append(
            "NOTE: SuperGrok on grok.com/Cursor is not an API. "
            "Add XAI_API_KEY from https://console.x.ai to use Grok."
        )
    return "\n".join(lines)


if __name__ == "__main__":
    print(describe_routing())
