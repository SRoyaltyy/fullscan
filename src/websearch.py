"""Shared multi-backend web search — single source of truth.

Used by src/deepseek_client.py (LLM tool loop) and by the collectors
(collectors/Deepseek.py, collectors/catalyst_analysis.py) as their
fallback when the primary SearXNG instance fails.

Backend chain (every step logged with result counts):
  1. own SearXNG (config.SEARXNG_URL) — 1 attempt, 10s timeout;
     EMPTY result set counts as failure (instance up but engines blocked)
  2. ddgs package (DuckDuckGo API-ish), 15s wall — unbounded ddgs hung
     the first ubuntu sector outcome
  3. DuckDuckGo HTML endpoint scrape — no key, verified working
  4. Google News RSS — no key, verified working; great for news queries

search_results() never raises: it returns (backend, items, errors).
"""
from __future__ import annotations

import os
import re
import time
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FutTimeout
from html import unescape
from urllib.parse import quote_plus, unquote

import requests

from . import config


def _env_int(name: str, default: int, minimum: int = 1) -> int:
    raw = (os.environ.get(name) or "").strip()
    if not raw:
        return default
    try:
        return max(minimum, int(raw))
    except ValueError:
        return default


# GH ubuntu often cannot reach the private SearXNG. Two 25s attempts plus
# an unbounded ddgs hang ate the first sector outcome (~30+ min, 0 persist).
SEARXNG_TIMEOUT = _env_int("SEARXNG_TIMEOUT", 10)
SEARXNG_ATTEMPTS = _env_int("SEARXNG_ATTEMPTS", 1)
DDG_TIMEOUT = _env_int("DDG_TIMEOUT", 15)
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126.0 Safari/537.36")


def _run_bounded(fn, timeout_s, *args):
    """Call fn(*args) with a hard wall. The worker thread is abandoned.

    Executor shutdown must not wait — a hung ddgs thread is why the first
    ubuntu sector outcome never persisted.
    """
    ex = ThreadPoolExecutor(max_workers=1)
    fut = ex.submit(fn, *args)
    try:
        return fut.result(timeout=timeout_s)
    except FutTimeout as e:
        raise TimeoutError(f"{getattr(fn, '__name__', fn)} exceeded "
                           f"{timeout_s}s") from e
    finally:
        ex.shutdown(wait=False, cancel_futures=True)


def _searxng(query: str, max_results: int) -> list[dict]:
    r = requests.get(f"{config.SEARXNG_URL}/search",
                     params={"q": query, "format": "json"},
                     timeout=SEARXNG_TIMEOUT)
    r.raise_for_status()
    return [{"title": x.get("title"), "url": x.get("url"),
             "snippet": x.get("content")}
            for x in r.json().get("results", [])[:max_results]]


def _ddg_unbounded(query: str, max_results: int) -> list[dict]:
    from ddgs import DDGS
    with DDGS() as ddgs:
        return [{"title": x.get("title"), "url": x.get("href"),
                 "snippet": x.get("body")}
                for x in ddgs.text(query, max_results=max_results)]


def _ddg(query: str, max_results: int) -> list[dict]:
    return _run_bounded(_ddg_unbounded, DDG_TIMEOUT, query, max_results)


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


def search_results(query: str, max_results: int = 6,
                   skip_searxng: bool = False) -> tuple:
    """Walk the backend chain. Never raises.

    Returns (backend_name, items, errors):
      backend_name — "searxng" | "ddg" | "ddg_html" | "gnews_rss" | None
      items        — list of {"title", "url", "snippet"} (empty on failure)
      errors       — list of "backend: message" strings for audit logs
    skip_searxng=True lets callers that already tried SearXNG themselves
    (the async collectors) start the chain at the fallbacks.
    """
    errors: list[str] = []

    # 1. own SearXNG (empty result set = broken upstream engines -> fail over)
    if not skip_searxng and config.SEARXNG_URL:
        for attempt in range(SEARXNG_ATTEMPTS):
            try:
                items = _searxng(query, max_results)
                if not items:
                    raise RuntimeError("searxng returned 0 results "
                                       "(upstream engines likely blocked)")
                print(f"[search] searxng OK ({len(items)} results)")
                return "searxng", items, errors
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
        return "ddg", items, errors
    except Exception as e:  # noqa: BLE001
        errors.append(f"ddgs: {e}")
        print(f"[search] ddg(lib) failed: {str(e)[:100]}")

    # 3. DDG HTML scrape
    try:
        items = _ddg_html(query, max_results)
        print(f"[search] ddg_html OK ({len(items)} results)")
        return "ddg_html", items, errors
    except Exception as e:  # noqa: BLE001
        errors.append(f"ddg_html: {e}")
        print(f"[search] ddg_html failed: {str(e)[:100]}")

    # 4. Google News RSS
    try:
        items = _gnews_rss(query, max_results)
        print(f"[search] gnews_rss OK ({len(items)} results)")
        return "gnews_rss", items, errors
    except Exception as e:  # noqa: BLE001
        errors.append(f"gnews_rss: {e}")
        print(f"[search] gnews_rss failed: {str(e)[:100]}")

    print(f"[search] ALL BACKENDS FAILED for query: {query!r}")
    return None, [], errors
