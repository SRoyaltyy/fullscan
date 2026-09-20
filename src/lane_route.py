"""Lane inbox router — official free APIs only.

$0 hoppers. Never Pro/ paid OpenRouter IDs. No browser / cloakbrowser.

Inbox:  02_lessons/lane/inbox.json
Outbox: 02_lessons/lane/outbox/YYYY-MM-DD.json

Templates
---------
Existing (ticker required): key_people, key_products, revenue_mix, custom
New:
  news_to_tickers  — articles[{title, body, known_at?}] → listed tickers
  company_dig      — ticker + brief/questions → research JSON

Hopper preference (never call Pro/ paid IDs)
--------------------------------------------
default (key_people / key_products / revenue_mix / custom):
  OpenRouter :free → DeepSeek → Qwen/DashScope → Zhipu Flash → Moonshot
  → SiliconFlow non-Pro → ModelScope → GitHub Models → Cloudflare
  → SambaNova → Ollama → HF free → Groq last-resort → Gemini

news_to_tickers (high volume):
  Zhipu Flash first (quality) → SiliconFlow mid free
  (Qwen/Qwen2.5-7B-Instruct + allowlisted non-Pro) → OpenRouter :free overflow
  → remaining default hoppers

company_dig (longer context):
  SiliconFlow Qwen / DeepSeek free non-Pro → native DeepSeek
  → OpenRouter :free overflow → Zhipu Flash (quality digs OK)
  → remaining default hoppers

#290 Grok-news overlay can enqueue news_to_tickers later. This module
does not rewrite that book and does not touch flatten_robust / cash book.
"""
from __future__ import annotations

import datetime
import json
import os
import pathlib
import time
import urllib.error
import urllib.request

SYSTEM = (
    "You answer simple factual questions about public companies for a trading desk.\n"
    "Return ONE JSON object only. No markdown fences. No preamble. No commentary.\n"
    "Use the schema the user provides. If a field is unknown, use null and list it in \"unknowns\".\n"
    "Never invent precise financial figures — null is better than a guess."
)
SYSTEM_NEWS = (
    "You map news developments to exact listed equity tickers for a trading desk.\n"
    "Return ONE JSON object only. No markdown fences. No preamble. No commentary.\n"
    "Exact listed tickers only — no theme-basket or ETF expansion."
)
SYSTEM_DIG = (
    "You write a company research dig for a trading desk.\n"
    "Return ONE JSON object only. No markdown fences. No preamble. No commentary.\n"
    "Never invent precise financial figures — null is better than a guess."
)

GROQ_MODELS = [
    "openai/gpt-oss-20b",
    "openai/gpt-oss-120b",
    "qwen/qwen3.8-27b",
    "qwen/qwen3.6-27b",
]
GEMINI_MODELS = ["gemini-2.5-flash-lite", "gemini-2.5-flash"]
# $0 only: documented free router and/or :free suffix. Never paid IDs.
# CN-origin :free first (GLM / InclusionAI Ling). Live OpenRouter catalog
# has no Qwen/DeepSeek :free right now — those go through native hoppers.
_OR_CANDIDATES = [
    "openrouter/free",
    "z-ai/glm-5.2:free",
    "inclusionai/ling-3.0-flash-fin:free",
    "inclusionai/ling-3.0-flash-sante:free",
    "minimax/minimax-m3:free",
    "google/gemma-4-31b-it:free",
    "google/gemma-4-26b-a4b-it:free",
    "nvidia/nemotron-3.5-lightning:free",
]
OR_MODELS = [m for m in _OR_CANDIDATES if m == "openrouter/free" or str(m).endswith(":free")]
DS_MODELS = ["deepseek-flash", "deepseek-chat"]
QWEN_MODELS = ["qwen-turbo", "qwen2.5-7b-instruct", "qwen3-8b"]
QWEN_URLS = [
    "https://dashscope.aliyuncs.com/compatible-mode/v1/chat/completions",
    "https://dashscope-intl.aliyuncs.com/compatible-mode/v1/chat/completions",
]
SF_MODELS = [m for m in (
    "Qwen/Qwen3-8B",
    "Qwen/Qwen2.5-7B-Instruct",
    "THUDM/GLM-Z1-9B-0414",
) if not str(m).startswith("Pro/")]
# Extra SiliconFlow free non-Pro IDs for longer company_dig context only.
SF_DIG_MODELS = [m for m in (
    "deepseek-ai/DeepSeek-R1-Distill-Qwen-7B",
) if not str(m).startswith("Pro/")]
SF_URLS = [
    "https://api.siliconflow.cn/v1/chat/completions",
    "https://api.siliconflow.com/v1/chat/completions",
]
MS_MODELS = [
    "Qwen/Qwen2.5-7B-Instruct",
    "Qwen/Qwen3-8B",
    "deepseek-ai/DeepSeek-R1-Distill-Qwen-7B",
]
# Zhipu documented free Flash family only — never glm-5.x / glm-4.7 paid.
ZHIPU_MODELS = ["glm-4.7-flash", "glm-4-flash-250414", "glm-4.5-flash"]
ZHIPU_URLS = [
    "https://open.bigmodel.cn/api/paas/v4/chat/completions",
    "https://api.z.ai/api/paas/v4/chat/completions",
]
# Moonshot smallest first so a free voucher lasts; skip 402 when empty.
MOONSHOT_MODELS = ["moonshot-v1-8k", "moonshot-v1-32k"]
MOONSHOT_URLS = [
    "https://api.moonshot.cn/v1/chat/completions",
    "https://api.moonshot.ai/v1/chat/completions",
]
# GitHub Models playground/API retired 2026-07-30; keep as optional skip-clean hopper.
GH_MODELS = [
    "microsoft/Phi-4-mini-instruct",
    "openai/gpt-4o-mini",
    "meta/Llama-3.2-3B-Instruct",
]
# Workers AI free allocation (10k neurons/day). No paid-plan-only models.
CF_MODELS = [
    "@cf/ibm-granite/granite-4.0-h-micro",
    "@cf/meta/llama-3.2-1b-instruct",
    "@cf/meta/llama-3.2-3b-instruct",
    "@cf/meta/llama-3.1-8b-instruct",
]
# SambaNova Cloud free tier (no payment method linked).
SN_MODELS = [
    "Meta-Llama-3.1-8B-Instruct",
    "gemma-4-31B-it",
    "Meta-Llama-3.3-70B-Instruct",
    "DeepSeek-V3.1",
]
HF_FALLBACK = [
    "HuggingFaceTB/SmolLM3-3B",
    "google/gemma-2-2b-it",
    "Qwen/Qwen2.5-7B-Instruct",
]
PACE = 2.1
DEAD_PROVIDER = (401, 403, 410, 402)

# Default / existing-template hopper order (same as the green smoke path).
DEFAULT_LANES = [
    "openrouter", "deepseek", "qwen", "zhipu", "moonshot",
    "siliconflow", "modelscope",
    "github_models", "cloudflare", "sambanova",
    "ollama", "hf", "groq", "gemini",
]
# news_to_tickers: Zhipu Flash → SF mid free → OpenRouter :free overflow.
NEWS_HEAD = ["zhipu", "siliconflow", "openrouter"]
# company_dig: SF Qwen/DeepSeek free → native DeepSeek → OR :free → Zhipu.
DIG_HEAD = ["siliconflow", "deepseek", "openrouter", "zhipu"]

LEGACY_TEMPLATES = ("key_people", "key_products", "revenue_mix", "custom")
KNOWN_TEMPLATES = LEGACY_TEMPLATES + ("news_to_tickers", "company_dig")

_HF_CACHE: list[str] = []
_SKIP: set[str] = set()


def _dedupe(names: list[str]) -> list[str]:
    seen, out = set(), []
    for name in names:
        if name not in seen:
            seen.add(name)
            out.append(name)
    return out


def lanes_for(tmpl: str) -> list[str]:
    """Hopper order for a template. Never includes Pro/ paid IDs."""
    tmpl = str(tmpl or "custom").strip()
    if tmpl == "news_to_tickers":
        return _dedupe(NEWS_HEAD + DEFAULT_LANES)
    if tmpl == "company_dig":
        return _dedupe(DIG_HEAD + DEFAULT_LANES)
    return list(DEFAULT_LANES)


def sf_models_for(tmpl: str) -> list[str]:
    """SiliconFlow allowlist: strip Pro/; prefer mid-free Qwen for news, Qwen/DeepSeek for digs."""
    ids = [m for m in SF_MODELS if not str(m).startswith("Pro/")]
    tmpl = str(tmpl or "").strip()
    if tmpl == "news_to_tickers":
        prefer = "Qwen/Qwen2.5-7B-Instruct"
        ids = [prefer] + [m for m in ids if m != prefer] if prefer in ids else ids
    elif tmpl == "company_dig":
        extra = [m for m in SF_DIG_MODELS if not str(m).startswith("Pro/")]
        prefer = [
            "Qwen/Qwen3-8B",
            "Qwen/Qwen2.5-7B-Instruct",
            *extra,
        ]
        ids = _dedupe([m for m in prefer if m in ids or m in extra] + ids)
    return [m for m in ids if not str(m).startswith("Pro/")]


def token_budget(tmpl: str) -> int:
    if tmpl == "company_dig":
        return 1600
    if tmpl == "news_to_tickers":
        return 900
    return 320


def system_for(tmpl: str) -> str:
    if tmpl == "news_to_tickers":
        return SYSTEM_NEWS
    if tmpl == "company_dig":
        return SYSTEM_DIG
    return SYSTEM


def articles_from(q: dict) -> list[dict]:
    """Accept articles[] or a single title/body on the question itself."""
    raw = q.get("articles")
    if isinstance(raw, dict):
        raw = [raw]
    if not raw:
        if q.get("title") or q.get("body") or q.get("text") or q.get("content"):
            raw = [q]
        else:
            raw = []
    out = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        title = str(item.get("title") or "").strip()
        body = str(
            item.get("body") or item.get("text") or item.get("content") or ""
        ).strip()
        known = str(item.get("known_at") or item.get("published_at") or "").strip()
        if title or body:
            row = {"title": title, "body": body}
            if known:
                row["known_at"] = known
            out.append(row)
    return out


def inbox_error(q) -> str | None:
    """None if the question is routable. news_to_tickers does not need a ticker."""
    if not isinstance(q, dict):
        return "question must be an object"
    tmpl = str(q.get("template") or "custom").strip() or "custom"
    if tmpl == "news_to_tickers":
        if not articles_from(q):
            return "news_to_tickers needs articles[{title,body}]"
        return None
    if tmpl == "company_dig":
        if not str(q.get("ticker") or "").strip():
            return "company_dig needs ticker"
        return None
    if not q.get("ticker"):
        return "ticker is required"
    return None


def _prompt_news_to_tickers(q: dict) -> str:
    # Prefer Zhipu Flash, else SF mid-free Qwen2.5-7B, OpenRouter :free overflow.
    # #290 Grok-news overlay can enqueue this template later (articles + known_at).
    blocks = []
    for i, art in enumerate(articles_from(q), 1):
        known = art.get("known_at") or ""
        head = f"{i}."
        if known:
            head += f" known_at={known}"
        blocks.append(
            f"{head}\nTitle: {art.get('title') or '(untitled)'}\n"
            f"Body: {art.get('body') or ''}"
        )
    article_text = "\n\n".join(blocks) if blocks else "(no articles)"
    return (
        "Map these news article(s) to exact listed equity tickers.\n\n"
        "Rules:\n"
        "- Return ONE JSON object only. No markdown. No prose dump.\n"
        "- Exact listed tickers only. Do not expand to theme baskets, ETFs, "
        "or sector peers that are not causally linked to this development.\n"
        "- Judge from market sector + causal relationship to the news.\n"
        "- polarity is exactly one of: bull, bear, unclear.\n"
        "- If no listed ticker is clearly affected, return "
        '{"tickers":[],"notes":"why"}.\n\n'
        f"Articles:\n{article_text}\n\n"
        "Schema (STRICT JSON):\n"
        '{"tickers":[{"symbol":"AAPL","sector":"Technology",'
        '"link":"why this development affects them",'
        '"polarity":"bull|bear|unclear"}],"notes":""}'
    )


def _prompt_company_dig(q: dict) -> str:
    # Prefer SF Qwen/DeepSeek free non-Pro, OpenRouter :free overflow; Zhipu Flash OK.
    ticker = str(q.get("ticker") or "").strip().upper()
    brief = str(q.get("brief") or q.get("question") or "").strip()
    extra_q = q.get("questions") or q.get("ask") or []
    if isinstance(extra_q, str):
        extra_q = [extra_q]
    asks = [str(x).strip() for x in extra_q if str(x).strip()]
    ask_block = "\n".join(f"- {a}" for a in asks) if asks else "(none)"
    return (
        f"Ticker: {ticker}\n"
        f"Research brief: {brief or '(none)'}\n"
        f"Questions:\n{ask_block}\n\n"
        "Write a company dig. Return ONE JSON object only. No markdown.\n"
        "Unknown or uncited figures = null. Do not invent numbers.\n\n"
        "Schema (STRICT JSON):\n"
        "{"
        '"business":"",'
        '"competitors":[{"name":"","ticker":"","note":""}],'
        '"catalysts":[{"item":"","horizon":""}],'
        '"risks":[{"item":"","severity":""}],'
        '"key_metrics":{},'
        '"sources_claimed":[]'
        "}"
    )


def _prompt_legacy(q: dict) -> str:
    ticker = str(q.get("ticker") or "").strip().upper()
    tmpl = q.get("template") or "custom"
    question = (q.get("question") or tmpl).strip()
    return (
        f"Ticker: {ticker}\nTemplate: {tmpl}\nQuestion: {question}\n\n"
        "Return ONE JSON object. Unknown fields = null. No markdown."
    )


def prompt_for(q: dict):
    """Return (ticker, tmpl, question, prompt) for any known template."""
    ticker = str(q.get("ticker") or "").strip().upper()
    tmpl = str(q.get("template") or "custom").strip() or "custom"
    if tmpl == "news_to_tickers":
        arts = articles_from(q)
        question = (q.get("question") or q.get("id") or "news_to_tickers").strip()
        if arts and not question:
            question = arts[0].get("title") or "news_to_tickers"
        return ticker, tmpl, question, _prompt_news_to_tickers(q)
    if tmpl == "company_dig":
        question = str(q.get("brief") or q.get("question") or "company_dig").strip()
        return ticker, tmpl, question, _prompt_company_dig(q)
    question = (q.get("question") or tmpl).strip()
    return ticker, tmpl, question, _prompt_legacy(q)


def extract_json(text):
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


def http_json(url, payload=None, headers=None, timeout=20):
    headers = dict(headers or {})
    data = None
    method = "GET"
    if payload is not None:
        data = json.dumps(payload).encode()
        headers = {"Content-Type": "application/json", **headers}
        method = "POST"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            raw = r.read().decode()
            try:
                body = json.loads(raw) if raw else {}
            except Exception:
                body = {"error": raw[:400]}
            return r.status, body, dict(r.headers)
    except urllib.error.HTTPError as e:
        raw = e.read().decode("utf-8", "replace")
        try:
            body = json.loads(raw)
        except Exception:
            body = {"error": raw[:400]}
        return e.code, body, dict(e.headers)
    except Exception as e:
        return 0, {"error": str(e)}, {}


def openai_chat(url, key, model, prompt, extra=None, max_tokens=320, system=None):
    system = SYSTEM if system is None else system
    timeout = 60 if max_tokens > 400 else 30
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": prompt},
        ],
        "max_tokens": max_tokens,
        "temperature": 0.1,
        "response_format": {"type": "json_object"},
    }
    status, body, _ = http_json(
        url, payload, {"Authorization": "Bearer " + key, **(extra or {})}, timeout=timeout,
    )
    if status == 400:
        payload.pop("response_format", None)
        status, body, _ = http_json(
            url, payload, {"Authorization": "Bearer " + key, **(extra or {})}, timeout=timeout,
        )
    if status != 200:
        return None, status, str(body.get("error") or body)[:240]
    text = (((body.get("choices") or [{}])[0].get("message") or {}).get("content")) or ""
    parsed = extract_json(text)
    if parsed is None:
        return None, status, "not json"
    return parsed, status, model


def gemini_chat(key, model, prompt, max_tokens=320, system=None):
    system = SYSTEM if system is None else system
    timeout = 60 if max_tokens > 400 else 30
    url = (
        "https://generativelanguage.googleapis.com/v1beta/models/"
        + urllib.request.quote(model)
        + ":generateContent?key="
        + urllib.request.quote(key)
    )
    status, body, _ = http_json(
        url,
        {
            "systemInstruction": {"parts": [{"text": system}]},
            "contents": [{"role": "user", "parts": [{"text": prompt}]}],
            "generationConfig": {
                "temperature": 0.1,
                "maxOutputTokens": max_tokens,
                "responseMimeType": "application/json",
            },
        },
        {},
        timeout=timeout,
    )
    if status != 200:
        return None, status, str(body.get("error") or body)[:240]
    parts = (((body.get("candidates") or [{}])[0].get("content") or {}).get("parts") or [])
    text = "".join(p.get("text") or "" for p in parts)
    parsed = extract_json(text)
    if parsed is None:
        return None, status, "not json"
    return parsed, status, model


def _or_is_free(model):
    return model == "openrouter/free" or str(model).endswith(":free")


def _rotate(status):
    """Next model after 429 / 5xx (and other model-level failures)."""
    if status == 429:
        time.sleep(2)
    return status == 429 or status == 0 or status >= 500


def _ok(ticker, tmpl, question, lane, model, parsed):
    return {
        "ok": True, "ticker": ticker, "template": tmpl, "question": question,
        "lane": lane, "model": model, "json": parsed, "via": "direct",
    }


def _provider_is_free(p):
    if p.get("is_free") is True:
        return True
    pr = p.get("pricing") or {}
    if "input" not in pr and "output" not in pr:
        return False
    try:
        return float(pr.get("input") or 0) == 0 and float(pr.get("output") or 0) == 0
    except (TypeError, ValueError):
        return False


def hf_free_models(token):
    global _HF_CACHE
    if _HF_CACHE:
        return _HF_CACHE
    status, body, _ = http_json(
        "https://router.huggingface.co/v1/models",
        None,
        {"Authorization": "Bearer " + token},
        timeout=20,
    )
    found = []
    if status == 200:
        for m in (body.get("data") or []):
            mid = (m.get("id") or "").strip()
            if not mid:
                continue
            for p in m.get("providers") or []:
                if _provider_is_free(p):
                    prov = (p.get("provider") or "").strip()
                    found.append(f"{mid}:{prov}" if prov else mid)
                    break
    seen, out = set(), []
    for x in found:
        if x not in seen:
            seen.add(x)
            out.append(x)
    _HF_CACHE = out[:6] or list(HF_FALLBACK)
    if not out:
        print("  hf catalog had no is_free providers — using fallback (skip on 402)")
    return _HF_CACHE


def ollama_models(base):
    status, body, _ = http_json(base + "/api/tags", None, {}, timeout=8)
    if status == 0:
        return []
    names = []
    for m in (body.get("models") or []):
        n = m.get("name") or m.get("model")
        if n:
            names.append(n)
    return names[:4] or ["llama3.2", "llama3.1", "mistral"]


def ollama_chat(base, model, prompt, max_tokens=320, system=None):
    dummy = os.environ.get("OLLAMA_API_KEY") or "ollama"
    parsed, status, info = openai_chat(
        base + "/v1/chat/completions", dummy, model, prompt,
        max_tokens=max_tokens, system=system,
    )
    if parsed is not None or status not in (0, 404):
        return parsed, status, info
    system = SYSTEM if system is None else system
    status, body, _ = http_json(
        base + "/api/chat",
        {
            "model": model,
            "stream": False,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": prompt},
            ],
            "options": {"temperature": 0.1, "num_predict": max_tokens},
        },
        {},
        timeout=60 if max_tokens > 400 else 30,
    )
    if status != 200:
        return None, status, str(body.get("error") or body)[:240]
    text = ((body.get("message") or {}).get("content")) or body.get("response") or ""
    parsed = extract_json(text)
    if parsed is None:
        return None, status, "not json"
    return parsed, status, model


def hop_models(lane, models, call, abandon_404=False):
    """call(model) -> (parsed, status, info). None = skip to next provider."""
    if lane in _SKIP:
        print(f"  {lane} skip (cached)")
        return None, None
    n404 = 0
    for model in models:
        parsed, status, info = call(model)
        print(f"  {lane}/{model} status={status}")
        if parsed is not None:
            return parsed, info
        if status in DEAD_PROVIDER:
            print(f"  {lane} skip ({status})")
            _SKIP.add(lane)
            return None, None
        if status == 404 and abandon_404:
            n404 += 1
            if n404 >= 2:
                print(f"  {lane} skip (repeated 404)")
                _SKIP.add(lane)
                return None, None
        _rotate(status)
    return None, None


def first_live_url(urls, key, model, prompt, max_tokens=320, system=None):
    """Try regional bases; use the first that answers HTTP (not connect-fail)."""
    last = (None, 0, "no url")
    for url in urls:
        parsed, status, info = openai_chat(
            url, key, model, prompt, max_tokens=max_tokens, system=system,
        )
        last = (parsed, status, info)
        if parsed is not None or status != 0:
            return last
    return last


def load_keys():
    keys = {}
    for k, env in (
        ("openrouter", "OPENROUTER_API_KEY"),
        ("deepseek", "DEEPSEEK_API_KEY"),
        ("siliconflow", "SILICONFLOW_API_KEY"),
        ("github_models", "GITHUB_MODELS_TOKEN"),
        ("sambanova", "SAMBANOVA_API_KEY"),
        ("hf", "HF_TOKEN"),
        ("groq", "GROQ_API_KEY"),
        ("gemini", "GEMINI_API_KEY"),
    ):
        if os.environ.get(env):
            keys[k] = os.environ[env]
    qwen_key = os.environ.get("DASHSCOPE_API_KEY") or os.environ.get("QWEN_API_KEY") or ""
    if qwen_key:
        keys["qwen"] = qwen_key
    zhipu_key = os.environ.get("ZHIPU_API_KEY") or os.environ.get("GLM_API_KEY") or ""
    if zhipu_key:
        keys["zhipu"] = zhipu_key
    if os.environ.get("MOONSHOT_API_KEY"):
        keys["moonshot"] = os.environ["MOONSHOT_API_KEY"]
    ms_key = (
        os.environ.get("MODELSCOPE_API_KEY")
        or os.environ.get("MODELSCOPE_API_TOKEN")
        or os.environ.get("MODELSCOPE_SDK_TOKEN")
        or ""
    )
    if ms_key:
        keys["modelscope"] = ms_key
    if os.environ.get("CLOUDFLARE_API_TOKEN") and os.environ.get("CLOUDFLARE_ACCOUNT_ID"):
        keys["cloudflare"] = os.environ["CLOUDFLARE_API_TOKEN"]
        keys["cloudflare_account"] = os.environ["CLOUDFLARE_ACCOUNT_ID"]
    elif os.environ.get("CLOUDFLARE_API_TOKEN") or os.environ.get("CLOUDFLARE_ACCOUNT_ID"):
        print("cloudflare hopper skipped — need both CLOUDFLARE_API_TOKEN and CLOUDFLARE_ACCOUNT_ID")
    ollama_url = (os.environ.get("OLLAMA_URL") or "").rstrip("/")
    gh_direct = keys.get("github_models") or os.environ.get("GITHUB_TOKEN") or ""
    return keys, ollama_url, gh_direct


def ask_lane(lane, prompt, ctx, max_tokens=320, system=None, tmpl="custom"):
    """Try one $0 hopper. Returns (parsed, info) or (None, None)."""
    keys = ctx["keys"]
    ollama_url = ctx["ollama_url"]
    gh_direct = ctx["gh_direct"]

    def oc(url, key, model, extra=None):
        return openai_chat(
            url, key, model, prompt, extra, max_tokens=max_tokens, system=system,
        )

    def flu(urls, key, model):
        return first_live_url(
            urls, key, model, prompt, max_tokens=max_tokens, system=system,
        )

    if lane == "openrouter":
        if not keys.get("openrouter"):
            return None, None
        extra = {"HTTP-Referer": "https://github.com/SRoyaltyy/fullscan", "X-Title": "Lane"}
        return hop_models(
            "openrouter",
            [m for m in OR_MODELS if _or_is_free(m)],
            lambda model: oc(
                "https://openrouter.ai/api/v1/chat/completions",
                keys["openrouter"], model, extra,
            ),
        )
    if lane == "deepseek":
        if not keys.get("deepseek"):
            return None, None
        return hop_models(
            "deepseek",
            DS_MODELS,
            lambda model: oc(
                "https://api.deepseek.com/chat/completions",
                keys["deepseek"], model,
            ),
        )
    if lane == "qwen":
        if not keys.get("qwen"):
            return None, None
        return hop_models(
            "qwen",
            QWEN_MODELS,
            lambda model: flu(QWEN_URLS, keys["qwen"], model),
        )
    if lane == "zhipu":
        if not keys.get("zhipu"):
            return None, None
        return hop_models(
            "zhipu",
            ZHIPU_MODELS,
            lambda model: flu(ZHIPU_URLS, keys["zhipu"], model),
        )
    if lane == "moonshot":
        if not keys.get("moonshot"):
            return None, None
        return hop_models(
            "moonshot",
            MOONSHOT_MODELS,
            lambda model: flu(MOONSHOT_URLS, keys["moonshot"], model),
        )
    if lane == "siliconflow":
        if not keys.get("siliconflow"):
            return None, None
        return hop_models(
            "siliconflow",
            sf_models_for(tmpl),
            lambda model: flu(SF_URLS, keys["siliconflow"], model),
        )
    if lane == "modelscope":
        if not keys.get("modelscope"):
            return None, None
        return hop_models(
            "modelscope",
            MS_MODELS,
            lambda model: oc(
                "https://api-inference.modelscope.cn/v1/chat/completions",
                keys["modelscope"], model,
            ),
        )
    if lane == "github_models":
        if not gh_direct:
            return None, None
        extra = {
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }
        return hop_models(
            "github_models",
            GH_MODELS,
            lambda model: oc(
                "https://models.github.ai/inference/chat/completions",
                gh_direct, model, extra,
            ),
            abandon_404=True,
        )
    if lane == "cloudflare":
        if not (keys.get("cloudflare") and keys.get("cloudflare_account")):
            return None, None
        cf_url = (
            "https://api.cloudflare.com/client/v4/accounts/"
            + urllib.request.quote(keys["cloudflare_account"], safe="")
            + "/ai/v1/chat/completions"
        )
        return hop_models(
            "cloudflare",
            CF_MODELS,
            lambda model: oc(cf_url, keys["cloudflare"], model),
        )
    if lane == "sambanova":
        if not keys.get("sambanova"):
            return None, None
        return hop_models(
            "sambanova",
            SN_MODELS,
            lambda model: oc(
                "https://api.sambanova.ai/v1/chat/completions",
                keys["sambanova"], model,
            ),
        )
    if lane == "ollama":
        if not ollama_url or "ollama" in _SKIP:
            return None, None
        models = ollama_models(ollama_url)
        if not models:
            print("  ollama skip (OLLAMA_URL unreachable)")
            _SKIP.add("ollama")
            return None, None
        return hop_models(
            "ollama",
            models,
            lambda model: ollama_chat(
                ollama_url, model, prompt, max_tokens=max_tokens, system=system,
            ),
        )
    if lane == "hf":
        if not keys.get("hf"):
            return None, None
        return hop_models(
            "hf",
            hf_free_models(keys["hf"]),
            lambda model: oc(
                "https://router.huggingface.co/v1/chat/completions",
                keys["hf"], model,
            ),
        )
    if lane == "groq":
        if not keys.get("groq"):
            return None, None
        return hop_models(
            "groq",
            GROQ_MODELS,
            lambda model: oc(
                "https://api.groq.com/openai/v1/chat/completions",
                keys["groq"], model,
            ),
        )
    if lane == "gemini":
        if not keys.get("gemini"):
            return None, None
        return hop_models(
            "gemini",
            GEMINI_MODELS,
            lambda model: gemini_chat(
                keys["gemini"], model, prompt, max_tokens=max_tokens, system=system,
            ),
        )
    return None, None


def direct_ask(q, ctx):
    ticker, tmpl, question, prompt = prompt_for(q)
    budget = token_budget(tmpl)
    system = system_for(tmpl)
    for lane in lanes_for(tmpl):
        parsed, info = ask_lane(
            lane, prompt, ctx, max_tokens=budget, system=system, tmpl=tmpl,
        )
        if parsed is not None:
            return _ok(ticker, tmpl, question, lane, info, parsed)
    return {
        "ok": False, "ticker": ticker, "template": tmpl, "question": question,
        "error": "all direct lanes failed", "via": "direct",
    }


def via_lane(origin, keys, q):
    if not origin or "YOUR-LANE" in origin:
        return None
    for _attempt in range(4):
        status, body, headers = http_json(
            origin + "/api/ask",
            {"keys": keys, "questions": [q]},
            {},
            timeout=20,
        )
        if status == 429:
            wait = 15
            try:
                wait = int(float(headers.get("Retry-After") or headers.get("retry-after") or 15))
            except Exception:
                pass
            time.sleep(min(60, max(2, wait)))
            continue
        if status in (0, 502, 503, 504) or (status >= 500):
            return None
        results = (body or {}).get("results") or []
        return results[0] if results else None
    return None


def load_questions(inbox):
    questions = inbox.get("questions") if isinstance(inbox, dict) else inbox
    if isinstance(questions, dict):
        questions = [questions]
    return questions or []


def route_inbox(
    inbox_path="02_lessons/lane/inbox.json",
    outbox_dir="02_lessons/lane/outbox",
):
    origin = os.environ.get("LANE_URL", "").rstrip("/")
    path = pathlib.Path(inbox_path)
    if not path.exists():
        print("no inbox")
        raise SystemExit(0)
    inbox = json.loads(path.read_text())
    questions = load_questions(inbox)

    keys, ollama_url, gh_direct = load_keys()
    if not keys and not ollama_url and not gh_direct:
        print("no free-lane secrets set — skip (add OPENROUTER_API_KEY)")
        raise SystemExit(0)

    ctx = {"keys": keys, "ollama_url": ollama_url, "gh_direct": gh_direct}
    _SKIP.clear()

    out = []
    for i, q in enumerate(questions):
        err = inbox_error(q)
        if err:
            out.append({"ok": False, "error": err, "raw": q})
            continue
        label = (q.get("ticker") or q.get("id") or "?").strip()
        print(f"lane {i + 1}/{len(questions)} {label} {q.get('template')}")
        row = via_lane(origin, keys, q)
        if row is None:
            print("  lane hop failed — direct")
            row = direct_ask(q, ctx)
        out.append(row)
        time.sleep(PACE)

    day = datetime.datetime.utcnow().strftime("%Y-%m-%d")
    dest = pathlib.Path(outbox_dir) / f"{day}.json"
    dest.parent.mkdir(parents=True, exist_ok=True)
    existing = []
    if dest.exists():
        try:
            existing = json.loads(dest.read_text()).get("results") or []
        except Exception:
            existing = []
    dest.write_text(json.dumps({
        "generated_at": datetime.datetime.utcnow().isoformat() + "Z",
        "count": len(existing) + len(out),
        "ok": sum(1 for r in out if r.get("ok")),
        "results": existing + out,
    }, indent=2), encoding="utf-8")
    print("wrote", dest, "n=", len(out), "ok=", sum(1 for r in out if r.get("ok")))
    return out


def main() -> None:
    route_inbox()


if __name__ == "__main__":
    main()
