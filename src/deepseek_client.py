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
  1. own SearXNG (SEARXNG_URL)        — 1 attempt, 10s timeout;
                                        EMPTY result set counts as failure
                                        (instance up but engines blocked)
  2. ddgs package (DuckDuckGo API-ish, 15s wall)
  3. DuckDuckGo HTML endpoint scrape  — no key, verified working
  4. Google News RSS                  — no key, verified working; great for
                                        event/news queries

On the DeepSeek path, stages needing tools must run on deepseek-chat
(deepseek-reasoner has no function-calling support).
"""
from __future__ import annotations

import json
import re
import threading
import time
import uuid

import requests

from . import config
from .skip_if_good import is_tool_dump
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
            r = requests.post(url, headers=headers, json=payload,
                              timeout=(15, 120))
            if r.status_code == 402:
                raise RuntimeError(
                    f"DeepSeek 402 Payment Required: {r.text[:200]}")
            if r.status_code in (429, 500, 502, 503):
                last = f"HTTP {r.status_code}: {r.text[:200]}"
                time.sleep(20 * (attempt + 1))
                continue
            r.raise_for_status()
            return r.json()
        except requests.ConnectTimeout as e:
            last = f"connect timeout: {e}"
            print(f"[llm] DeepSeek {last} — not retrying a dead API")
            break
        except requests.ReadTimeout as e:
            last = f"read timeout: {e}"
            print(f"[llm] DeepSeek {last} — not retrying a hung body")
            break
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
# Consecutive idle-timeout *content* (gateway is up but returns a timeout
# stub as the assistant message) also trips the breaker after 2 in a row.
# Under GROK_ONLY the breaker means "stop calling Grok and return empty"
# — DeepSeek/SearXNG must not write analysis.
_OPENCLAW_STATE = {"down": False, "reason": "", "timeouts": 0}
_CALL_STATE = threading.local()


def last_provider() -> str:
    """Provider used by the latest chat() call on this worker thread."""
    return str(getattr(_CALL_STATE, "provider", "") or "")


def _set_last_provider(provider: str) -> None:
    _CALL_STATE.provider = provider

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


def _is_hard_openclaw_down(reason: str) -> bool:
    """True only when later Grok calls cannot succeed this process.

    A single HTTP 500 / 429 / idle timeout used to mark the gateway DOWN
    for the whole 11-sector morning. That blanked every later predict.
    Transient model/gateway blips must fall back for THAT call only.
    """
    r = (reason or "").lower()
    if any(tok in r for tok in (
        "http 500", "http 502", "http 503", "http 429",
        "timeout after", "idle-timeout", "idle timeout",
        "internal error",
    )):
        return False
    return any(tok in r for tok in (
        "401", "404", "connection refused", "connect timeout",
        "name or service not known",
        "nodename nor servname", "network is unreachable",
    ))


def _mark_openclaw_down(reason: str) -> None:
    _OPENCLAW_STATE["down"] = True
    _OPENCLAW_STATE["reason"] = reason[:300]
    print(f"[openclaw] gateway marked DOWN for this run: {reason[:200]}")


def _note_openclaw_fail(reason: str) -> None:
    """Trip the breaker only on hard auth/network death."""
    if _is_hard_openclaw_down(reason):
        _mark_openclaw_down(reason)
        return
    print(f"[openclaw] transient fail (not marking DOWN): {reason[:200]}")


def looks_like_timeout_content(text: str) -> bool:
    """True when OpenClaw returned an idle-timeout / error stub as content.

    That used to be treated as a successful answer, so parse_scores defaulted
    every missing S0–S4 to 0.0 and the sector file landed as 0/flat. Treat
    it as empty so the DeepSeek fallback (or the caller's QC retry) fires.
    Imported lazily to keep this module importable without output_qc.
    """
    if not text or not str(text).strip():
        return False
    try:
        from .output_qc import looks_like_timeout
        return looks_like_timeout(text)
    except Exception:
        t = str(text)
        needles = ("LLM request timed out", "idle timeout",
                   "model idle timeout", "prompt too long",
                   "The model did not produce a response")
        return any(n.lower() in t.lower() for n in needles) and len(t) < 2500



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
    realigned = False
    for attempt in range(retries):
        if config.OPENCLAW_TOKEN:
            headers["Authorization"] = f"Bearer {config.OPENCLAW_TOKEN}"
        try:
            r = requests.post(url, headers=headers, json=payload,
                              timeout=(15, config.OPENCLAW_TIMEOUT))
            if r.status_code in (401, 404) and not realigned:
                last = f"HTTP {r.status_code}: {r.text[:200]}"
                print(f"[openclaw] {r.status_code} — realign token and retry once")
                config.align_openclaw_token(force=True)
                realigned = True
                continue
            if r.status_code in (429, 500, 502, 503):
                last = f"HTTP {r.status_code}: {r.text[:200]}"
                time.sleep(15 * (attempt + 1))
                continue
            r.raise_for_status()
            return r.json()
        except requests.ConnectTimeout as e:
            last = f"connect timeout: {e}"
            print(f"[openclaw] {last} — gateway unreachable, not retrying")
            break
        except requests.Timeout as e:
            last = f"timeout after {config.OPENCLAW_TIMEOUT}s: {e}"
            print(f"[openclaw] {last} — not retrying a hung {config.OPENCLAW_TIMEOUT}s call")
            break
        except requests.RequestException as e:
            last = str(e)
            time.sleep(15 * (attempt + 1))
    raise RuntimeError(f"OpenClaw call failed after {retries} tries: {last}")


def _openclaw_chat(messages: list[dict], tools: bool, max_tokens: int,
                   temperature: float, transcript_path: str | None,
                   trace_path: str | None, stage_label: str) -> str:
    """One agent turn against the gateway. Grok does its own research
    (native web/X search) inside the turn. Returns '' on failure so the
    caller can fall back to DeepSeek — unless GROK_ONLY, in which case
    empty is the final answer."""
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
        reason = str(e)
        if "timeout after" in reason.lower():
            _OPENCLAW_STATE["timeouts"] = _OPENCLAW_STATE.get("timeouts", 0) + 1
            n = _OPENCLAW_STATE["timeouts"]
            print(f"[openclaw] HTTP timeout ({stage_label or 'llm run'}; "
                  f"consecutive={n}) — will fall back to DeepSeek")
            if n >= 3:
                _mark_openclaw_down(
                    f"{n} consecutive HTTP timeouts "
                    f"({stage_label or 'llm run'})")
        _note_openclaw_fail(reason)
        return ""

    if not final:
        note = ("no DeepSeek/SearXNG fallback (GROK_ONLY)"
                if config.grok_only() else "will fall back to DeepSeek")
        print(f"[openclaw] EMPTY answer ({stage_label or 'llm run'}) — {note}")
        return ""

    if looks_like_timeout_content(final):
        _OPENCLAW_STATE["timeouts"] = _OPENCLAW_STATE.get("timeouts", 0) + 1
        n = _OPENCLAW_STATE["timeouts"]
        note = ("no DeepSeek/SearXNG fallback (GROK_ONLY)"
                if config.grok_only() else "will fall back to DeepSeek")
        print(f"[openclaw] timeout/error stub treated as empty "
              f"({stage_label or 'llm run'}; consecutive={n}) — {note}")
        if n >= 5:
            _mark_openclaw_down(
                f"{n} consecutive idle-timeout stubs "
                f"({stage_label or 'llm run'})")
        return ""
    _OPENCLAW_STATE["timeouts"] = 0

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


def _is_sector_stage(stage_label: str) -> bool:
    label = (stage_label or "").upper()
    return "SECTOR OUTCOME" in label or "SECTOR REFLECT" in label


def _is_capped_search_stage(stage_label: str) -> bool:
    """Night-pack DeepSeek stages that must finish inside a child/step wall."""
    label = (stage_label or "").upper()
    return _is_sector_stage(stage_label) or "MAP POSTCLOSE" in label


def _effective_tool_rounds(stage_label: str, max_rounds: int | None) -> int:
    """Sector grades already have ETF actuals; 10 search rounds miss ≥8 files."""
    base = max_rounds if max_rounds is not None else config.MAX_TOOL_ROUNDS
    if _is_capped_search_stage(stage_label):
        cap = int(getattr(config, "SECTOR_TOOL_ROUNDS", 2) or 2)
        return max(1, min(int(base), cap))
    return max(1, int(base))


def _last_assistant_text(messages: list[dict]) -> str:
    for m in reversed(messages):
        if m.get("role") == "assistant":
            text = (m.get("content") or "").strip()
            if text and not is_tool_dump(text):
                return text
    return ""


_DUMP_QUERY_RE = re.compile(r'name="query"[^>]*>([^<]+)', re.I)


def _extract_dump_queries(text: str) -> list[str]:
    """Pull web_search queries out of leaked DSML tool-call XML."""
    if not text:
        return []
    return [q.strip() for q in _DUMP_QUERY_RE.findall(text) if q.strip()]


def _calls_from_dump(text: str, n: int, start: int = 0) -> list[dict]:
    calls = []
    for i, q in enumerate(_extract_dump_queries(text)[: max(0, n)]):
        calls.append({
            "id": f"dump-{start + i}",
            "type": "function",
            "function": {"name": "web_search",
                         "arguments": json.dumps({"query": q})},
        })
    return calls


def _tool_research_lines(messages: list[dict]) -> list[str]:
    """Turn already-run web_search tool payloads into essay bullets."""
    lines: list[str] = []
    for m in messages:
        if m.get("role") != "tool":
            continue
        raw = m.get("content") or ""
        try:
            parsed = json.loads(raw)
        except (TypeError, ValueError):
            continue
        if not isinstance(parsed, dict):
            continue
        query = str(parsed.get("query") or "").strip()
        err = parsed.get("error")
        if err:
            lines.append(f"- search {query or '?'}: {str(err)[:180]}")
            continue
        results = parsed.get("results") or []
        if query:
            lines.append(f"- query: {query} ({parsed.get('backend') or '?'})")
        for it in results[:4]:
            if not isinstance(it, dict):
                continue
            title = str(it.get("title") or "?").strip()
            url = str(it.get("url") or "").strip()
            snip = str(it.get("snippet") or it.get("body") or "").strip()
            lines.append(f"  - {title} {f'({url})' if url else ''}".rstrip())
            if snip:
                lines.append(f"    {snip[:280]}")
    return lines


def _user_actuals_excerpt(messages: list[dict], limit: int = 2500) -> str:
    chunks: list[str] = []
    for m in messages:
        if m.get("role") != "user":
            continue
        text = str(m.get("content") or "").strip()
        if not text:
            continue
        if "ACTUALS" in text or "ETF_PCT" in text or "DATE:" in text:
            chunks.append(text[:limit])
    return "\n\n".join(chunks).strip()


def _essay_from_thread(messages: list[dict], stage_label: str = "") -> str:
    """Last-resort pack file when DeepSeek only emits DSML dumps.

    Live 09-03 sidecar (#102): every no-tool close was another tool-dump, so
    chat() returned '' and run_sector_outcome wrote nothing. Assemble a
    readable review from deterministic actuals + searches already in-thread
    so skip-if-good can count an essay instead of a stub.
    """
    research = _tool_research_lines(messages)
    actuals = _user_actuals_excerpt(messages)
    label = (stage_label or "session").strip() or "session"
    parts = [
        f"## Post-session review — {label}",
        "",
        "DeepSeek returned tool-call dumps on the no-tool close, so this "
        "essay is assembled from the deterministic actuals and the search "
        "results already gathered in this thread. The night pack must not "
        "stay on a leaked tool-call stub.",
        "",
        "### Actuals and morning packet",
        actuals or "(no DATE/ACTUALS user turn in this thread)",
        "",
        "### Research gathered",
    ]
    parts.extend(research or ["- (no web_search tool results landed)"])
    parts += [
        "",
        "### Close",
        "Direction and magnitude follow the ETF_PCT / REL_PCT actuals "
        "above. Rewrite with a model essay on the next heal if a provider "
        "returns prose; do not replace this file with a tool-call stub.",
    ]
    text = "\n".join(parts).strip()
    if len(text) < 200 or is_tool_dump(text):
        return ""
    return text


def chat(messages: list[dict], model: str, tools: bool = False,
         max_tokens: int = 8000, temperature: float = 0.2,
         transcript_path: str | None = None,
         trace_path: str | None = None, stage_label: str = "",
         max_rounds: int | None = None,
         force_deepseek: bool = False) -> str:
    """Chat completion. PRIMARY: OpenClaw gateway (Grok 4.6 with native
    web/X search — `model` is ignored on that path). FALLBACK: DeepSeek
    with the client-side web_search tool loop, exactly as before.

    If tools=True the model researches before answering (natively on
    OpenClaw; via the SearXNG loop on DeepSeek). If transcript_path is
    set, the FULL conversation is dumped there as JSON for audit. If
    trace_path is set, a human-readable step-by-step reasoning log is
    written there as Markdown. max_rounds overrides
    config.MAX_TOOL_ROUNDS for search-heavy DeepSeek stages.

    force_deepseek=True skips OpenClaw for THIS call only and does not
    mark the gateway down. Used when Grok returned a long unparseable
    blob (events JSON) — that is not an idle-timeout stub, so chat()
    would otherwise never fall through.
    """
    import copy
    import os

    _set_last_provider("")
    if config.prefer_deepseek():
        force_deepseek = True
        print(f"[llm] LLM_BACKEND=deepseek — OpenClaw skipped "
              f"({stage_label or 'llm run'})")
    grok_only = config.grok_only()
    if grok_only and force_deepseek:
        print(f"[llm] GROK_ONLY: ignoring force_deepseek "
              f"({stage_label or 'llm run'})")
        force_deepseek = False

    config.align_openclaw_token()

    # ---- primary: OpenClaw / Grok ----
    if openclaw_available() and not force_deepseek:
        text = _openclaw_chat(messages, tools=tools, max_tokens=max_tokens,
                              temperature=temperature,
                              transcript_path=transcript_path,
                              trace_path=trace_path,
                              stage_label=stage_label)
        if text:
            _set_last_provider("openclaw")
            return text
        if grok_only:
            print("[llm] GROK_ONLY: OpenClaw failed — no DeepSeek/SearXNG fallback")
            return ""
        if not config.DEEPSEEK_API_KEY:
            print("[llm] OpenClaw failed and no DEEPSEEK_API_KEY fallback")
            return ""
        print(f"[llm] falling back to DeepSeek (model={model})")
    elif force_deepseek:
        print(f"[llm] force_deepseek ({stage_label or 'llm run'}) — "
              "OpenClaw skipped for this call only")
    elif grok_only:
        print("[llm] GROK_ONLY: OpenClaw unavailable/down — no fallback")
        return ""
    elif config.openclaw_enabled() and not config.DEEPSEEK_API_KEY:
        # gateway configured but marked down, and no fallback either
        print("[llm] OpenClaw gateway down and no DEEPSEEK_API_KEY fallback")
        return ""

    # ---- fallback: DeepSeek + SearXNG tool loop (original client) ----
    deepseek_max_tokens = max_tokens
    if model == "deepseek-chat":
        deepseek_max_tokens = min(
            max_tokens,
            config.DEEPSEEK_CHAT_MAX_TOKENS,
        )
        if deepseek_max_tokens != max_tokens:
            print(
                f"[llm] cap DeepSeek max_tokens "
                f"{max_tokens}→{deepseek_max_tokens}"
            )
    payload = {"model": model, "messages": messages,
               "max_tokens": deepseek_max_tokens,
               "temperature": temperature}
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

    rounds = _effective_tool_rounds(stage_label, max_rounds) if tools else 1
    sector = _is_sector_stage(stage_label)
    capped = _is_capped_search_stage(stage_label)
    search_cap = (int(getattr(config, "SECTOR_MAX_SEARCHES", 2) or 2)
                  if capped else 10 ** 9)
    budget_s = (int(getattr(config, "SECTOR_CHAT_BUDGET_S", 420) or 420)
                if capped else 10 ** 9)
    t0 = time.monotonic()
    searches_done = 0
    step = 0
    final = None
    for _round in range(rounds):
        if time.monotonic() - t0 >= budget_s:
            print(f"[llm] sector chat budget {budget_s}s "
                  f"({stage_label or 'llm run'}) — conclude", flush=True)
            break
        if searches_done >= search_cap:
            print(f"[llm] sector search cap {search_cap} "
                  f"({stage_label or 'llm run'}) — conclude", flush=True)
            break
        payload["messages"] = messages
        try:
            resp = _post(payload)
        except RuntimeError as e:
            print(f"[llm] DeepSeek failed ({stage_label or 'llm run'}): {e}")
            return ""
        msg = resp["choices"][0]["message"]
        raw_content = msg.get("content") or ""
        content = raw_content
        if is_tool_dump(content):
            print(f"[llm] ignoring tool-dump content "
                  f"({len(content)} chars) — not an essay", flush=True)
            content = ""
        calls = list(msg.get("tool_calls") or [])
        if not calls and is_tool_dump(raw_content):
            remain = max(0, search_cap - searches_done)
            recovered = _calls_from_dump(raw_content, remain, searches_done)
            if recovered:
                print(f"[llm] recovered {len(recovered)} searches "
                      f"from tool-dump content", flush=True)
                calls = recovered
        # Essay already in hand — do not spend the 600s child on more search.
        if sector and len(content.strip()) >= 200:
            final = content
            messages.append({"role": "assistant", "content": final})
            step += 1
            trace.append(f"**Step {step} — Done researching.** Sector essay "
                         f"landed with the tool call ({len(final):,} characters); "
                         "skipping further search so the child can write.")
            break
        if not calls:
            if len(content.strip()) >= 200 or not tools:
                final = content
                messages.append({"role": "assistant", "content": final})
                step += 1
                trace.append(f"**Step {step} — Done researching.** The model "
                             f"stopped searching and wrote its full analysis "
                             f"({len(final):,} characters).")
                break
            # tools=True but dump/empty with no tool_calls — write an essay
            # from predict+actuals (+ any searches already run).
            print("[llm] empty/tool-dump turn with no tool_calls — "
                  "force no-tool close", flush=True)
            final = None
            break
        remain = max(0, search_cap - searches_done)
        if remain == 0:
            final = content if len(content.strip()) >= 200 else None
            if final:
                messages.append({"role": "assistant", "content": final})
            break
        if capped and len(calls) > remain:
            calls = calls[:remain]
        step += 1
        messages.append({"role": "assistant",
                         "content": content or None,
                         "tool_calls": calls})
        for call in calls:
            if time.monotonic() - t0 >= budget_s:
                print(f"[llm] sector chat budget {budget_s}s mid-search "
                      f"({stage_label or 'llm run'}) — conclude", flush=True)
                break
            args = json.loads(call["function"]["arguments"] or "{}")
            q = args.get("query", "")
            result = web_search(q)
            searches_done += 1
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
        reused = _last_assistant_text(messages)
        if len(reused) >= 200:
            final = reused
            trace.append(f"**Step {step + 1} — Search budget exhausted.** "
                         f"Reused the last assistant essay ({len(final):,} chars) "
                         "instead of another 120s DeepSeek read.")
        else:
            # tool budget exhausted -> one final no-tool answer
            trace.append(f"**Step {step + 1} — Search budget exhausted.** Forced "
                         "to conclude with what it already gathered.")
            payload.pop("tools", None)
            payload.pop("tool_choice", None)
            close_msgs = list(messages) + [{
                "role": "user",
                "content": (
                    "Write the full post-session essay now. "
                    "Do not emit tool calls or DSML. "
                    "Use the actuals and any search results already in this thread."
                ),
            }]
            final = ""
            for attempt in range(3):
                payload["messages"] = close_msgs
                try:
                    resp = _post(payload)
                except RuntimeError as e:
                    print(f"[llm] DeepSeek failed ({stage_label or 'llm run'}): {e}")
                    break
                cand = resp["choices"][0]["message"].get("content") or ""
                if is_tool_dump(cand):
                    print(f"[llm] forced close dump on attempt {attempt + 1} "
                          f"({len(cand)} chars) — retry", flush=True)
                    cand = ""
                if len(cand.strip()) >= 200:
                    final = cand
                    break
                print(f"[llm] forced close thin on attempt {attempt + 1} "
                      f"({len(cand.strip())} chars) — retry", flush=True)
            if len((final or "").strip()) < 200:
                assembled = _essay_from_thread(messages, stage_label)
                if assembled:
                    print(f"[llm] assembled essay from thread "
                          f"({len(assembled)} chars) after dump-only close",
                          flush=True)
                    final = assembled
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
    _set_last_provider("deepseek")
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
        if text and len(text.strip()) >= 200 and not is_tool_dump(text):
            if i:
                print(f"[llm] recovered on attempt {i + 1} "
                      f"(model={model}, max_tokens={max_tokens})")
            return text
        kind = "tool-dump" if is_tool_dump(text or "") else "thin/empty"
        print(f"[llm] {kind} answer on attempt {i + 1} "
              f"(model={model}, max_tokens={max_tokens}, "
              f"chars={len((text or '').strip())}) — trying next rung")
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
    if config.grok_only():
        lines.append("GROK_ONLY: DeepSeek/SearXNG will not run analysis.")
    elif config.openclaw_enabled():
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
