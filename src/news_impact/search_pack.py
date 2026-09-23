"""Fact pump: Google AI Overview (Gemini grounding) then existing web parse.

Never store implied winners. Quotes / dates / named entities only.
"""
from __future__ import annotations

import json
import os
import re
import urllib.error
import urllib.parse
import urllib.request

from src import websearch


OVERVIEW_SYSTEM = (
    "You extract facts from web search for a trading desk. "
    "Return ONLY dated facts, named entities, quotes, and URLs. "
    "Do NOT name winners, losers, sectors to buy, or tickers to trade. "
    "If a fact is not in search results, omit it."
)


def gemini_api_key() -> str:
    """GEMINI_API_KEY or the studio key already on the one-shot runner."""
    return (
        os.environ.get("GEMINI_API_KEY")
        or os.environ.get("GOOGLE_AI_STUDIO_API_KEY")
        or ""
    ).strip()


def _overview_flash_id(raw: str, current: str) -> str:
    """Flash ID named by a Gemini 404. Not pro / plus."""
    for mid in re.findall(r"models/([A-Za-z0-9._\-]+)", raw or ""):
        low = mid.lower()
        if mid == current or "flash" not in low:
            continue
        if any(bad in low for bad in ("pro", "ultra", "plus", "paid")):
            continue
        return mid
    return ""


def google_ai_overview(
    query: str,
    max_facts: int = 8,
    _model: str | None = None,
    _followed: bool = False,
) -> tuple[str, list[dict], list[str]]:
    """Gemini + Google Search grounding = Google's search AI (free-tier key).

    Returns (backend, facts[{text,url,source}], errors).
    """
    key = gemini_api_key()
    errors: list[str] = []
    if not key:
        return "", [], ["google_overview: no GEMINI_API_KEY or GOOGLE_AI_STUDIO_API_KEY"]
    model = _model or os.environ.get("GEMINI_OVERVIEW_MODEL") or "gemini-2.5-flash"
    url = (
        "https://generativelanguage.googleapis.com/v1beta/models/"
        + urllib.parse.quote(model)
        + ":generateContent?key="
        + urllib.parse.quote(key)
    )
    payload = {
        "systemInstruction": {"parts": [{"text": OVERVIEW_SYSTEM}]},
        "contents": [{
            "role": "user",
            "parts": [{
                "text": (
                    "Search and extract dated quotes, named entities, and URLs "
                    "for this query. Do not name winners or tickers to trade. "
                    "JSON object only: {\"facts\":[{\"text\":\"\",\"url\":\"\"}]}\n\n"
                    f"Query: {query}"
                ),
            }],
        }],
        "tools": [{"google_search": {}}],
        "generationConfig": {"temperature": 0.1, "maxOutputTokens": 800},
    }
    try:
        req = urllib.request.Request(
            url,
            data=json.dumps(payload).encode(),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=25) as r:
            body = json.loads(r.read().decode() or "{}")
    except urllib.error.HTTPError as e:
        raw = e.read().decode("utf-8", "replace")[:500]
        errors.append(f"google_overview HTTP {e.code}")
        named = _overview_flash_id(raw, model) if e.code == 404 else ""
        if named and not _followed:
            return google_ai_overview(
                query, max_facts, _model=named, _followed=True,
            )
        if e.code in (429, 404) and not _followed and model != "gemini-2.5-flash-lite":
            return google_ai_overview(
                query, max_facts, _model="gemini-2.5-flash-lite",
            )
        return "", [], errors
    except Exception as e:  # noqa: BLE001
        errors.append(f"google_overview: {e}")
        return "", [], errors

    cand = ((body.get("candidates") or [{}])[0])
    parts = ((cand.get("content") or {}).get("parts") or [])
    text = "".join(p.get("text") or "" for p in parts)
    facts: list[dict] = []
    parsed = _extract_json(text)
    if isinstance(parsed, dict):
        for row in parsed.get("facts") or []:
            if not isinstance(row, dict):
                continue
            t = str(row.get("text") or "").strip()
            if t:
                facts.append({
                    "text": t[:400],
                    "url": str(row.get("url") or ""),
                    "source": "google_ai_overview",
                })
    ground = (cand.get("groundingMetadata") or {})
    for ch in (ground.get("groundingChunks") or [])[:max_facts]:
        web = ch.get("web") or {}
        title = str(web.get("title") or "").strip()
        uri = str(web.get("uri") or "").strip()
        if title or uri:
            facts.append({
                "text": title[:400],
                "url": uri,
                "source": "google_ai_overview",
            })
    # Dedup
    seen, out = set(), []
    for f in facts:
        key = (f.get("text"), f.get("url"))
        if key in seen:
            continue
        seen.add(key)
        out.append(f)
        if len(out) >= max_facts:
            break
    if out:
        return "google_ai_overview", out, errors
    if text.strip():
        return "google_ai_overview", [{
            "text": text.strip()[:400],
            "url": "",
            "source": "google_ai_overview",
        }], errors
    errors.append("google_overview: empty")
    return "", [], errors


def _extract_json(text: str):
    text = (text or "").strip()
    try:
        return json.loads(text)
    except Exception:
        pass
    a, b = text.find("{"), text.rfind("}")
    if a >= 0 and b > a:
        try:
            return json.loads(text[a:b + 1])
        except Exception:
            return None
    return None


def overview_first(query: str, max_facts: int = 8) -> dict:
    """Call Google AI Overview and stop. Web search is the caller's fallback.

    overview_called is true once this function runs, including a missing-key
    return. The one-shot treats a skipped call while a Gemini key is present
    as a bug.
    """
    backend, facts, errors = google_ai_overview(query, max_facts=max_facts)
    for fact in facts:
        if fact.get("text") and not fact.get("status"):
            fact["status"] = "quote"
    return {
        "backend": backend or "google_ai_overview",
        "facts": facts,
        "errors": errors,
        "query": query,
        "overview_called": True,
    }


def search_facts(query: str, max_results: int = 6) -> dict:
    """Overview first, then SearXNG → DDG → Google News RSS."""
    backend, facts, errors = google_ai_overview(query, max_facts=max_results)
    if facts:
        return {"backend": backend, "facts": facts, "errors": errors}
    wb, items, err2 = websearch.search_results(query, max_results)
    facts = []
    for it in items or []:
        facts.append({
            "text": f"{it.get('title') or ''}: {it.get('snippet') or ''}"[:400],
            "url": it.get("url") or "",
            "source": wb or "websearch",
        })
    return {
        "backend": wb or "none",
        "facts": facts,
        "errors": errors + list(err2 or []),
    }


def pack_for_article(title: str, body: str = "", enabled: bool = True) -> dict:
    """Build a context pack. Disabled → empty (offline backtest)."""
    if not enabled:
        return {"backend": "off", "facts": [], "errors": [], "query": ""}
    query = (title or "").strip()
    if body:
        query = f"{query} {body[:160]}".strip()
    if not query:
        return {"backend": "off", "facts": [], "errors": [], "query": ""}
    row = search_facts(query)
    row["query"] = query
    return row
