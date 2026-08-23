"""LLM client: OpenClaw gateway (Grok 4.6) primary, DeepSeek fallback.

PRIMARY — OpenClaw gateway (OPENCLAW_GATEWAY_URL set):
  POST {gateway}/v1/chat/completions with the gateway bearer token.
  `model` = agent target (openclaw/default); the real backend model
  (xai/grok-4.6) rides the `x-openclaw-model` header. Grok researches
  with its OWN native web/X search inside the gateway turn — the local
  SearXNG tool loop is NOT sent on this path. Research stages are asked
  to append a RESEARCH appendix (queries, sources, facts) so the output
  stays auditable.

FALLBACK — DeepSeek API (DEEPSEEK_API_KEY set):
  The original client, unchanged: OpenAI-compatible chat completions
  with a client-side web_search function-tool loop.

The search backend chain for the FALLBACK path lives in src/websearch.py
(single source of truth, shared with the collectors):
  1. own SearXNG (SEARXNG_URL)        — 2 attempts, 25s timeout;
                                        EMPTY result set counts as failure
                                        (instance up but engines blocked)
  2. ddgs package (DuckDuckGo API-ish)
  3. DuckDuckGo HTML endpoint scrape  — no key, verified working
  4. Google News RSS                  — no key, verified working; great for
                                        event/news queries

On the DeepSeek path, stages needing tools must run on deepseek-chat
(deepseek-reasoner has no function-calling support).
"""
from __future__ import annotations

import json
import time
import uuid

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


def _post(payload: dict, retries: int = 4) -> dict:
    if not config.DEEPSEEK_API_KEY:
        raise RuntimeError("DEEPSEEK_API_KEY not set (fallback unavailable)")
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


# ---------------------------------------------------------------------------
# OpenClaw gateway (primary)
# ---------------------------------------------------------------------------

# Circuit breaker: once the gateway hard-fails (network/HTTP after retries)
# we stop retrying it for the rest of this process, so a 22-call sector day
# does not spend 22 × retries waiting on a dead box.
_OPENCLAW_STATE = {"down": False, "reason": ""}

# Appended to the system prompt on research stages (tools=True). The
# RESEARCH appendix goes at the END so output contracts (first-line
# lesson confirmation, JSON blocks, score markers) are untouched.
NATIVE_SEARCH_NOTE = (
    "\n\n[RESEARCH MODE] You have native web search and X search — use "
    "them yourself to research live sources before answering; there is no "
    "external search tool in this conversation. Produce the required "
    "output exactly as specified above, then append a final section "
    "titled RESEARCH APPENDIX listing the queries you ran, the key "
    "sources (title + URL + timestamp where available), and the specific "
    "facts you took from each. Never invent sources."
)


def openclaw_available() -> bool:
    return config.openclaw_enabled() and not _OPENCLAW_STATE["down"]


def _mark_openclaw_down(reason: str) -> None:
    _OPENCLAW_STATE["down"] = True
    _OPENCLAW_STATE["reason"] = reason[:300]
    print(f"[openclaw] gateway marked DOWN for this run: {reason[:200]}")


def _post_openclaw(messages: list[dict], max_tokens: int,
                   temperature: float, stage_label: str = "",
                   retries: int = 2) -> dict:
    url = f"{config.OPENCLAW_GATEWAY_URL}/v1/chat/completions"
    headers = {"Content-Type": "application/json"}
    if config.OPENCLAW_TOKEN:
        headers["Authorization"] = f"Bearer {config.OPENCLAW_TOKEN}"
    if config.OPENCLAW_BACKEND_MODEL:
        headers["x-openclaw-model"] = config.OPENCLAW_BACKEND_MODEL
    # Unique session per call: each pipeline stage must be stateless, and
    # the full conversation is resent every time anyway.
    headers["x-openclaw-session-key"] = (
        f"fullscan-{(stage_label or 'stage').replace(' ', '-')[:48]}"
        f"-{uuid.uuid4().hex[:12]}"
    )
    payload = {"model": config.OPENCLAW_AGENT, "messages": messages,
               "max_tokens": max_tokens, "temperature": temperature}
    last = None
    for attempt in range(retries):
        try:
            # (connect, read) tuple: an unreachable gateway (e.g. a
            # GitHub-hosted runner that can't see the ECS loopback, or a
            # firewalled port that drops SYNs) fails in seconds instead
            # of eating the full read timeout before DeepSeek fallback.
            r = requests.post(url, headers=headers, json=payload,
                              timeout=(15, config.OPENCLAW_TIMEOUT))
            if r.status_code in (429, 500, 502, 503):
                last = f"HTTP {r.status_code}: {r.text[:200]}"
                time.sleep(15 * (attempt + 1))
                continue
            r.raise_for_status()
            return r.json()
        except requests.RequestException as e:
            last = str(e)
            time.sleep(15 * (attempt + 1))
    raise RuntimeError(f"OpenClaw call failed after {retries} tries: {last}")


def _openclaw_chat(messages: list[dict], tools: bool, max_tokens: int,
                   temperature: float, transcript_path: str | None,
                   trace_path: str | None, stage_label: str) -> str:
    """One agent turn against the gateway. Grok does its own research
    (native web/X search) inside the turn. Returns '' on failure so the
    caller can fall back to DeepSeek."""
    import copy
    import os

    msgs = [dict(m) for m in messages]
    if tools and msgs and msgs[0].get("role") == "system":
        msgs[0]["content"] = str(msgs[0].get("content") or "") + NATIVE_SEARCH_NOTE
    elif tools:
        msgs.insert(0, {"role": "system",
                        "content": NATIVE_SEARCH_NOTE.strip()})

    try:
        resp = _post_openclaw(msgs, max_tokens=max_tokens,
                              temperature=temperature,
                              stage_label=stage_label)
        final = (resp["choices"][0]["message"].get("content") or "").strip()
    except (RuntimeError, KeyError, IndexError, TypeError) as e:
        _mark_openclaw_down(str(e))
        return ""

    if not final:
        print(f"[openclaw] EMPTY answer ({stage_label or 'llm run'}) — "
              "will fall back to DeepSeek")
        return ""

    if transcript_path:
        try:
            os.makedirs(os.path.dirname(transcript_path), exist_ok=True)
            with open(transcript_path, "w", encoding="utf-8") as fh:
                json.dump({"provider": "openclaw",
                           "agent": config.OPENCLAW_AGENT,
                           "backend_model": config.OPENCLAW_BACKEND_MODEL,
                           "messages": copy.deepcopy(msgs)
                           + [{"role": "assistant", "content": final}]},
                          fh, indent=2, ensure_ascii=False, default=str)
        except OSError as e:
            print(f"[transcript] save failed: {e}")
    if trace_path:
        try:
            os.makedirs(os.path.dirname(trace_path), exist_ok=True)
            sys_chars = sum(len(str(m.get("content") or "")) for m in msgs)
            with open(trace_path, "w", encoding="utf-8") as fh:
                fh.write("\n\n".join([
                    f"# Reasoning trace — {stage_label or 'llm run'}", "",
                    f"**Step 0 — Setup.** Loaded {sys_chars:,} characters "
                    f"of input. Provider: OpenClaw gateway, backend model "
                    f"`{config.OPENCLAW_BACKEND_MODEL}`. "
                    + ("Native web/X search was ENABLED inside the agent "
                       "turn; see the RESEARCH APPENDIX at the end of the "
                       "output for queries and sources." if tools else
                       "Search was disabled for this stage; the model "
                       "worked only from the documents it was given."),
                    "",
                    f"**Step 1 — Done.** The agent returned its full "
                    f"analysis ({len(final):,} characters).",
                ]) + "\n")
        except OSError as e:
            print(f"[trace] save failed: {e}")
    return final


def chat(messages: list[dict], model: str, tools: bool = False,
         max_tokens: int = 8000, temperature: float = 0.2,
         transcript_path: str | None = None,
         trace_path: str | None = None, stage_label: str = "",
         max_rounds: int | None = None) -> str:
    """Chat completion. PRIMARY: OpenClaw gateway (Grok 4.6 with native
    web/X search — `model` is ignored on that path). FALLBACK: DeepSeek
    with the client-side web_search tool loop, exactly as before.

    If tools=True the model researches before answering (natively on
    OpenClaw; via the SearXNG loop on DeepSeek). If transcript_path is
    set, the FULL conversation is dumped there as JSON for audit. If
    trace_path is set, a human-readable step-by-step reasoning log is
    written there as Markdown. max_rounds overrides
    config.MAX_TOOL_ROUNDS for search-heavy DeepSeek stages."""
    import copy
    import os

    # ---- primary: OpenClaw / Grok ----
    if openclaw_available():
        text = _openclaw_chat(messages, tools=tools, max_tokens=max_tokens,
                              temperature=temperature,
                              transcript_path=transcript_path,
                              trace_path=trace_path,
                              stage_label=stage_label)
        if text:
            return text
        if not config.DEEPSEEK_API_KEY:
            print("[llm] OpenClaw failed and no DEEPSEEK_API_KEY fallback")
            return ""
        print(f"[llm] falling back to DeepSeek (model={model})")
    elif config.openclaw_enabled() and not config.DEEPSEEK_API_KEY:
        # gateway configured but marked down, and no fallback either
        print("[llm] OpenClaw gateway down and no DEEPSEEK_API_KEY fallback")
        return ""

    # ---- fallback: DeepSeek + SearXNG tool loop (original client) ----
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


def chat_nonempty(messages: list[dict], ladder: list[tuple[str, int]],
                  tools: bool = False, temperature: float = 0.2,
                  transcript_path: str | None = None,
                  trace_path: str | None = None,
                  stage_label: str = "") -> str:
    """Call chat() down a (model, max_tokens) ladder until a NON-EMPTY answer
    comes back; returns '' if every rung fails.

    Guards against deepseek-reasoner burning its entire max_tokens budget on
    hidden reasoning and returning content='' — which previously produced
    blank reflect files and junk empty lessons. Typical ladder:
        [(config.MODEL_REFLECT, 12000),
         (config.MODEL_REFLECT, 16000),
         (config.MODEL_PREDICT, 8000)]
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
    """Human-readable provider map. Never prints secret values."""
    lines = [
        "PRIMARY  — OpenClaw gateway (Grok, native web/X search)",
        f"  OPENCLAW_GATEWAY_URL:   {config.OPENCLAW_GATEWAY_URL or 'NOT SET'}",
        f"  OPENCLAW_TOKEN:         {'set' if config.OPENCLAW_TOKEN else 'MISSING'}",
        f"  agent / backend model:  {config.OPENCLAW_AGENT} / "
        f"{config.OPENCLAW_BACKEND_MODEL}",
        f"  timeout:                {config.OPENCLAW_TIMEOUT}s",
        "",
        "FALLBACK — DeepSeek API (+ SearXNG tool loop)",
        f"  DEEPSEEK_API_KEY:       {'set' if config.DEEPSEEK_API_KEY else 'MISSING'}",
        f"  models:                 predict/outcome={config.MODEL_PREDICT}/"
        f"{config.MODEL_OUTCOME}, reflect/distill={config.MODEL_REFLECT}/"
        f"{config.MODEL_DISTILL}",
        "",
    ]
    if config.openclaw_enabled():
        lines.append("Every LLM stage runs on Grok via OpenClaw first; "
                     "DeepSeek fires only if the gateway fails or answers "
                     "empty.")
    else:
        lines.append("OpenClaw gateway not configured — all stages run on "
                     "DeepSeek exactly as before. Set OPENCLAW_GATEWAY_URL "
                     "(+ OPENCLAW_TOKEN) to switch the primary to Grok 4.6.")
    if not config.has_llm():
        lines.append("WARNING: no LLM configured at all — LLM stages will "
                     "be skipped or exit.")
    return "\n".join(lines)


def probe() -> bool:
    """Live round-trip through the full chain. Returns True on success."""
    print(describe_routing())
    print("\n--- probe: sending one tiny request through chat() ---")
    try:
        text = chat(
            [{"role": "system", "content": "Answer in one short sentence."},
             {"role": "user",
              "content": "Reply with the word OK and the model/provider "
                         "you are."}],
            model=config.MODEL_PREDICT, tools=False, max_tokens=100)
    except Exception as e:  # noqa: BLE001
        print(f"probe FAILED: {e}")
        return False
    if text.strip():
        print(f"probe answer: {text.strip()[:300]}")
        if _OPENCLAW_STATE["down"]:
            print(f"NOTE: OpenClaw was marked down "
                  f"({_OPENCLAW_STATE['reason']}); this answer came from "
                  "the DeepSeek fallback.")
        return True
    print("probe FAILED: empty answer from every provider")
    return False


if __name__ == "__main__":
    import sys
    if "--probe" in sys.argv:
        raise SystemExit(0 if probe() else 1)
    print(describe_routing())
