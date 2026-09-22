"""Lane inbox router — official free APIs only.

$0 hoppers. Never Pro/ paid OpenRouter IDs. No browser / cloakbrowser.

Inbox:  02_lessons/lane/inbox.json
Outbox: 02_lessons/lane/outbox/YYYY-MM-DD.json

Templates
---------
Existing (ticker required): key_people, key_products, revenue_mix, custom
New:
  news_to_tickers  — articles[{title, body, known_at?}] → listed tickers
  news_classify    — articles[{title, body}] → {event_class, sign, q5}
  news_impact      — one family analyst → entities[] up/down
  company_dig      — ticker + brief/questions → research JSON

Hopper preference (never call Pro/ paid IDs)
--------------------------------------------
Cyrus 2026-09-21: primary allowlists are current 2025+ flash only.
On 429 / rate-limit, abandon that provider (add to skip) — do not fall
down older sibling IDs on the same lane. Banned from primary / news:
glm-4-flash-250414, any glm-4-flash that is not 4.7 / 5.x,
qwen2.5-7b-instruct and similar pre-2025 small IDs.

default (key_people / key_products / revenue_mix / custom):
  OpenRouter :free → Qwen/DashScope qwen-flash
  → Zhipu glm-4.7-flash → Moonshot → SiliconFlow current non-Pro
  → ModelScope → Mistral / NVIDIA NIM / Pollinations free-strain
  → TokenHub overflow (glm-5.3-flash / flashx /
  deepseek-v4-flash, then hy3) → GitHub Models → Cloudflare
  → SambaNova → Ollama → HF free → Groq last-resort → Gemini
  (gemini key: GEMINI_API_KEY, else GOOGLE_AI_STUDIO_API_KEY)
  Native api.deepseek.com is PAID — not on the $0 path. Opt-in only via
  LANE_ALLOW_PAID_DEEPSEEK=1 (after OpenRouter when enabled).

news_to_tickers + news_classify + news_impact + news sector scan
(high volume, same policy):
  Zhipu glm-4.7-flash → SiliconFlow current (Qwen3-8B)
  → OpenRouter :free current → DashScope qwen-flash
  → remaining default hoppers (TokenHub still behind true $0)

company_dig (longer context):
  SiliconFlow current Qwen / DeepSeek free non-Pro (SF/ModelScope IDs)
  → OpenRouter :free overflow → Zhipu glm-4.7-flash
  → remaining default hoppers (TokenHub still behind true $0)
  Native DeepSeek only when LANE_ALLOW_PAID_DEEPSEEK=1.

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
# Mistral Experiment plan (rate-limited $0). Flash / edge only — no large/medium.
MISTRAL_MODELS = ["ministral-8b-2512", "ministral-3b-2512", "mistral-small-latest"]
MISTRAL_URL = "https://api.mistral.ai/v1/chat/completions"
# NVIDIA NIM hosted free-trial tier (OpenAI-compatible). Small / nano only.
NVIDIA_NIM_MODELS = [
    "nvidia/nemotron-mini-4b-instruct",
    "meta/llama-3.2-3b-instruct",
    "meta/llama-3.1-8b-instruct",
]
NVIDIA_NIM_URL = "https://integrate.api.nvidia.com/v1/chat/completions"
# Pollinations OpenAI-compatible text (gen.pollinations.ai). Flash aliases only;
# spends Quest/free pollen — 402 skips clean when empty. Never Pro / paid-only.
POLLINATIONS_MODELS = [
    "gemini-fast",
    "qwen3.7-flash",
    "deepseek",
]
POLLINATIONS_URL = "https://gen.pollinations.ai/v1/chat/completions"
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
# Current DashScope flash only. Never plus / max / Pro / paid / pre-2025 small.
QWEN_MODELS = [m for m in (
    "qwen-flash",
) if "plus" not in m.lower() and "max" not in m.lower()
    and "paid" not in m.lower() and not m.lower().startswith("pro")
    and "/pro" not in m.lower() and "-pro" not in m.lower()]
# Public DashScope OpenAI-compatible fallbacks. DASHSCOPE_BASE_URL (env/secret)
# is prepended by qwen_urls() — never log or commit that value.
QWEN_URLS = [
    "https://dashscope.aliyuncs.com/compatible-mode/v1/chat/completions",
    "https://dashscope-intl.aliyuncs.com/compatible-mode/v1/chat/completions",
]
# Tencent TokenHub (Guangzhou CN). Flash IDs first if $0-safe on this host;
# hy3 is overflow only. Never Pro/plus/paid. TOKENHUB_BASE_URL or
# TENCENT_BASE_URL is prepended by tokenhub_urls() — never log that value.
# Default public host only (do not add intl or legacy Hunyuan product URLs).
TOKENHUB_DEFAULT_BASE = "https://tokenhub.tencentmaas.com/v1"
# Flash / flashx first; hy3 last. tokenhub_models() re-filters Pro/plus/paid.
_TOKENHUB_CANDIDATES = (
    "glm-5.3-flash",
    "glm-5.3-flashx",
    "deepseek-v4-flash",
    "hy3",
)
TOKENHUB_MODELS = [
    m for m in _TOKENHUB_CANDIDATES
    if "plus" not in m.lower() and "paid" not in m.lower()
    and not m.lower().startswith("pro")
    and "/pro" not in m.lower() and "-pro" not in m.lower()
]
SF_MODELS = [m for m in (
    "Qwen/Qwen3-8B",
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
    "Qwen/Qwen3-8B",
    "deepseek-ai/DeepSeek-R1-Distill-Qwen-7B",
]
# Zhipu current free Flash only. Never glm-5.x (TokenHub) and never older
# glm-4-flash siblings (those are last-resort, not primary / news).
ZHIPU_MODELS = ["glm-4.7-flash"]
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
]
PACE = 2.1
DEAD_PROVIDER = (401, 403, 410, 402)
RATE_LIMIT = (429,)

# Cyrus 2026-09-21 — last-resort only. Prefer excluding from every primary
# allowlist. Not wired into hop_models. News scan never uses this list.
LAST_RESORT_MODELS = (
    "glm-4-flash-250414",
    "glm-4.5-flash",
    "qwen2.5-7b-instruct",
    "Qwen/Qwen2.5-7B-Instruct",
)

# Default / existing-template hopper order (same as the green smoke path).
# TokenHub is overflow behind true $0 hoppers (OR :free, DashScope, Zhipu
# Flash, SF non-Pro, Mistral / NIM / Pollinations, …). Do not move it ahead
# of those lanes.
# Native "deepseek" (api.deepseek.com) is PAID — not in this $0 list.
# Opt-in via LANE_ALLOW_PAID_DEEPSEEK=1 (see lanes_for / ask_lane).
DEFAULT_LANES = [
    "openrouter", "qwen", "zhipu", "moonshot",
    "siliconflow", "modelscope",
    "mistral", "nvidia_nim", "pollinations",
    "tokenhub",
    "github_models", "cloudflare", "sambanova",
    "ollama", "hf", "groq", "gemini",
]
# news_to_tickers + classify/impact + news sector scan: current flash first.
# Zhipu glm-4.7-flash → SF Qwen3-8B → OR :free → DashScope qwen-flash.
NEWS_HEAD = ["zhipu", "siliconflow", "openrouter", "qwen"]
# company_dig: SF Qwen / DeepSeek free non-Pro → OR :free → Zhipu.
# Native DeepSeek stays off the free head (paid opt-in only).
DIG_HEAD = ["siliconflow", "openrouter", "zhipu"]

LEGACY_TEMPLATES = ("key_people", "key_products", "revenue_mix", "custom")
NEWS_TEMPLATES = ("news_to_tickers", "news_classify", "news_impact")
KNOWN_TEMPLATES = LEGACY_TEMPLATES + NEWS_TEMPLATES + ("company_dig",)

_HF_CACHE: list[str] = []
_SKIP: set[str] = set()


def _dedupe(names: list[str]) -> list[str]:
    seen, out = set(), []
    for name in names:
        if name not in seen:
            seen.add(name)
            out.append(name)
    return out


def allow_paid_deepseek() -> bool:
    """Native api.deepseek.com is paid. Default OFF even when key is present.

    Set LANE_ALLOW_PAID_DEEPSEEK=1 to put the deepseek hopper back in the
    plan and allow ask_lane to call it. TokenHub deepseek-v4-flash and
    SiliconFlow/ModelScope deepseek-ai/* IDs are separate free-tier hosts.
    """
    raw = (os.environ.get("LANE_ALLOW_PAID_DEEPSEEK") or "").strip().lower()
    return raw in ("1", "true", "yes", "on")


def _with_paid_deepseek(order: list[str], *, after: str) -> list[str]:
    """Insert native deepseek after `after` only when paid opt-in is set."""
    order = [n for n in order if n != "deepseek"]
    if not allow_paid_deepseek():
        return order
    if after in order:
        i = order.index(after) + 1
        return order[:i] + ["deepseek"] + order[i:]
    return ["deepseek"] + order


def openai_compat_chat_url(base: str) -> str:
    """Normalize an OpenAI-compatible base to a chat/completions URL.

    Append /chat/completions when missing. Do not log the raw env value.
    """
    base = (base or "").strip().rstrip("/")
    if not base:
        return ""
    if base.endswith("/chat/completions"):
        return base
    return base + "/chat/completions"


def dashscope_chat_url(base: str) -> str:
    """Normalize a DashScope compatible-mode base to a chat/completions URL.

    James's secret is the /compatible-mode/v1 base. Append /chat/completions
    when missing. Do not log or return the raw env value from callers.
    """
    return openai_compat_chat_url(base)


def tokenhub_chat_url(base: str) -> str:
    """Normalize a TokenHub / Tencent OpenAI-compatible base.

    Same append rule as DashScope. Do not log the raw env value.
    """
    return openai_compat_chat_url(base)


def qwen_urls() -> list[str]:
    """DASHSCOPE_BASE_URL first (if set), then public dashscope*.aliyuncs.com."""
    custom = dashscope_chat_url(os.environ.get("DASHSCOPE_BASE_URL") or "")
    public = list(QWEN_URLS)
    if custom:
        return _dedupe([custom] + public)
    return public


def is_banned_primary(mid: str) -> bool:
    """True if a model ID is banned from primary / news hoppers.

    Bans glm-4-flash-250414, any glm-4-flash that is not 4.7 / 5.x,
    glm-4.5-flash (old glm-4 family), qwen2.5-7b-instruct and similar.
    """
    raw = str(mid or "").strip()
    if not raw:
        return True
    low = raw.lower()
    if any(n.lower() in low for n in LAST_RESORT_MODELS):
        return True
    if "qwen2.5-7b" in low:
        return True
    # glm-4-flash* that is not 4.7 (glm-4.7-flash does not contain this stem).
    if "glm-4-flash" in low and "4.7" not in low:
        return True
    # glm-4.x-flash except 4.7 (covers glm-4.5-flash).
    if low.startswith("glm-4.") and "flash" in low and "glm-4.7" not in low:
        return True
    return False


def qwen_models() -> list[str]:
    """Allowlisted current DashScope flash. qwen-flash only; skip plus/max/Pro."""
    return [m for m in QWEN_MODELS
            if "plus" not in m.lower() and "max" not in m.lower()
            and "paid" not in m.lower() and not m.lower().startswith("pro")
            and "/pro" not in m.lower() and "-pro" not in m.lower()
            and not is_banned_primary(m)]


def _tokenhub_id_ok(mid: str) -> bool:
    """True if a TokenHub model ID is not Pro / plus / paid."""
    low = str(mid or "").lower()
    if not low:
        return False
    if "plus" in low or "paid" in low:
        return False
    if low.startswith("pro") or "/pro" in low or "-pro" in low:
        return False
    return True


def tokenhub_key() -> str:
    """Bearer: TOKENHUB_API_KEY, else TENCENT_API_KEY, else HUNYUAN_API_KEY.

    HUNYUAN_API_KEY is an optional alias only — no Hunyuan-product URL.
    Never log or return this from print paths.
    """
    return (
        (os.environ.get("TOKENHUB_API_KEY") or "").strip()
        or (os.environ.get("TENCENT_API_KEY") or "").strip()
        or (os.environ.get("HUNYUAN_API_KEY") or "").strip()
    )


def gemini_key() -> str:
    """GEMINI_API_KEY first; else GOOGLE_AI_STUDIO_API_KEY (same Studio API).

    Does not rotate or delete GEMINI_API_KEY. Never log the value.
    """
    return (
        (os.environ.get("GEMINI_API_KEY") or "").strip()
        or (os.environ.get("GOOGLE_AI_STUDIO_API_KEY") or "").strip()
    )


def nvidia_nim_key() -> str:
    """NVIDIA_NIM_API_KEY, else NVIDIA_API_KEY alias. Never log the value."""
    return (
        (os.environ.get("NVIDIA_NIM_API_KEY") or "").strip()
        or (os.environ.get("NVIDIA_API_KEY") or "").strip()
    )


def mistral_key() -> str:
    """MISTRAL_API_KEY. Never log the value."""
    return (os.environ.get("MISTRAL_API_KEY") or "").strip()


def pollinations_key() -> str:
    """POLLINATIONS_API_KEY for gen.pollinations.ai. Never log the value."""
    return (os.environ.get("POLLINATIONS_API_KEY") or "").strip()


def tokenhub_urls() -> list[str]:
    """TOKENHUB_BASE_URL then TENCENT_BASE_URL, then CN TokenHub default.

    Custom env bases are tried first (same append-/chat/completions rule as
    DashScope). Never log or commit those values. Do not add intl or legacy
    Hunyuan-product hosts as hardcoded fallbacks.
    """
    custom = []
    for env in ("TOKENHUB_BASE_URL", "TENCENT_BASE_URL"):
        url = tokenhub_chat_url(os.environ.get(env) or "")
        if url:
            custom.append(url)
    default = tokenhub_chat_url(TOKENHUB_DEFAULT_BASE)
    return _dedupe((custom + [default]) if default else custom)


def tokenhub_models() -> list[str]:
    """$0-safe flash / flashx first; hy3 overflow last. Skip Pro/plus/paid."""
    flash, overflow = [], []
    for mid in TOKENHUB_MODELS:
        if not _tokenhub_id_ok(mid):
            continue
        low = mid.lower()
        if "flash" in low:
            flash.append(mid)
        elif mid == "hy3":
            overflow.append(mid)
    return flash + overflow


def lanes_for(tmpl: str) -> list[str]:
    """Hopper order for a template. Never includes Pro/ paid IDs by default.

    Native DeepSeek (api.deepseek.com) is paid and excluded unless
    LANE_ALLOW_PAID_DEEPSEEK=1. Free DeepSeek-named IDs on TokenHub /
    SiliconFlow / ModelScope remain on the $0 path via those lanes.
    """
    tmpl = str(tmpl or "custom").strip()
    if tmpl in NEWS_TEMPLATES:
        return _with_paid_deepseek(
            _dedupe(NEWS_HEAD + DEFAULT_LANES), after="openrouter",
        )
    if tmpl == "company_dig":
        return _with_paid_deepseek(
            _dedupe(DIG_HEAD + DEFAULT_LANES), after="siliconflow",
        )
    return _with_paid_deepseek(list(DEFAULT_LANES), after="openrouter")


def sf_models_for(tmpl: str) -> list[str]:
    """SiliconFlow allowlist: strip Pro/; current Qwen3 first for news/digs."""
    ids = [
        m for m in SF_MODELS
        if not str(m).startswith("Pro/") and not is_banned_primary(m)
    ]
    tmpl = str(tmpl or "").strip()
    if tmpl in NEWS_TEMPLATES:
        prefer = "Qwen/Qwen3-8B"
        ids = [prefer] + [m for m in ids if m != prefer] if prefer in ids else ids
    elif tmpl == "company_dig":
        extra = [
            m for m in SF_DIG_MODELS
            if not str(m).startswith("Pro/") and not is_banned_primary(m)
        ]
        prefer = [
            "Qwen/Qwen3-8B",
            *extra,
        ]
        ids = _dedupe([m for m in prefer if m in ids or m in extra] + ids)
    return [
        m for m in ids
        if not str(m).startswith("Pro/") and not is_banned_primary(m)
    ]


def primary_models_for(lane: str, tmpl: str = "custom") -> list[str]:
    """Current 2025+ primary IDs for a hopper. Never last-resort / banned IDs."""
    tmpl = str(tmpl or "custom").strip()
    if lane == "openrouter":
        raw = [m for m in OR_MODELS if _or_is_free(m)]
    elif lane == "deepseek":
        raw = list(DS_MODELS)
    elif lane == "qwen":
        raw = qwen_models()
    elif lane == "zhipu":
        raw = list(ZHIPU_MODELS)
    elif lane == "moonshot":
        raw = list(MOONSHOT_MODELS)
    elif lane == "siliconflow":
        raw = sf_models_for(tmpl)
    elif lane == "modelscope":
        raw = list(MS_MODELS)
    elif lane == "mistral":
        raw = list(MISTRAL_MODELS)
    elif lane == "nvidia_nim":
        raw = list(NVIDIA_NIM_MODELS)
    elif lane == "pollinations":
        raw = list(POLLINATIONS_MODELS)
    elif lane == "tokenhub":
        raw = tokenhub_models()
    elif lane == "github_models":
        raw = list(GH_MODELS)
    elif lane == "cloudflare":
        raw = list(CF_MODELS)
    elif lane == "sambanova":
        raw = list(SN_MODELS)
    elif lane == "hf":
        raw = list(HF_FALLBACK)
    elif lane == "groq":
        raw = list(GROQ_MODELS)
    elif lane == "gemini":
        raw = list(GEMINI_MODELS)
    else:
        raw = []
    return [m for m in raw if not is_banned_primary(m)]


def hopper_plan(tmpl: str = "custom") -> list[tuple[str, list[str]]]:
    """Provider sequence + primary model IDs for a template."""
    return [(hop, primary_models_for(hop, tmpl)) for hop in lanes_for(tmpl)]


def token_budget(tmpl: str) -> int:
    if tmpl == "company_dig":
        return 1600
    if tmpl == "news_to_tickers":
        return 900
    if tmpl == "news_classify":
        return 400
    if tmpl == "news_impact":
        return 900
    return 320


def system_for(tmpl: str) -> str:
    if tmpl == "news_to_tickers":
        return SYSTEM_NEWS
    if tmpl == "news_classify":
        from .news_impact.prompts import CLASSIFIER_SYSTEM
        return CLASSIFIER_SYSTEM
    if tmpl == "news_impact":
        from .news_impact.prompts import ANALYST_SYSTEM
        return ANALYST_SYSTEM
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
    if tmpl in NEWS_TEMPLATES:
        if not articles_from(q):
            return f"{tmpl} needs articles[{{title,body}}]"
        return None
    if tmpl == "company_dig":
        if not str(q.get("ticker") or "").strip():
            return "company_dig needs ticker"
        return None
    if not q.get("ticker"):
        return "ticker is required"
    return None


def _prompt_news_to_tickers(q: dict) -> str:
    # Prefer glm-4.7-flash, else SF Qwen3-8B, OpenRouter :free, qwen-flash.
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
    if tmpl in ("news_classify", "news_impact"):
        from .news_impact.prompts import analyst_prompt, classifier_prompt
        arts = articles_from(q)
        art = arts[0] if arts else {}
        question = (q.get("question") or q.get("id") or art.get("title") or tmpl).strip()
        if tmpl == "news_classify":
            prompt = classifier_prompt(
                art.get("title") or "", art.get("body") or "",
                art.get("known_at") or "",
            )
        else:
            prompt = analyst_prompt(
                art.get("title") or "",
                art.get("body") or "",
                str(q.get("family") or "blast"),
                str(q.get("event_class") or "blast_legal"),
                q.get("sign"),
                str(q.get("q5") or "impulse"),
                str(q.get("constraint") or ""),
                q.get("pack_facts") or [],
            )
        return ticker, tmpl, question, prompt
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
    """Next current model after connect-fail / 5xx. 429 abandons the provider."""
    return status == 0 or status >= 500


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
    """call(model) -> (parsed, status, info). None = skip to next provider.

    Cyrus 2026-09-21: never try banned last-resort IDs. On 429 / rate-limit,
    abandon this provider for the request (add to skip) — do not fall down
    older sibling IDs on the same lane.
    """
    if lane in _SKIP:
        print(f"  {lane} skip (cached)")
        return None, None
    models = [m for m in models if not is_banned_primary(m)]
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
        if status in RATE_LIMIT:
            print(f"  {lane} skip (429)")
            _SKIP.add(lane)
            time.sleep(2)
            return None, None
        if status == 404 and abandon_404:
            n404 += 1
            if n404 >= 2:
                print(f"  {lane} skip (repeated 404)")
                _SKIP.add(lane)
                return None, None
        _rotate(status)
    return None, None


def first_live_url(urls, key, model, prompt, max_tokens=320, system=None,
                   skip_statuses=()):
    """Try regional bases; use the first that answers HTTP (not connect-fail).

    skip_statuses: treat those HTTP codes as "wrong host, try next base"
    (TokenHub: 401/403 = wrong product/region). Never log the URL.
    """
    last = (None, 0, "no url")
    for url in urls:
        parsed, status, info = openai_chat(
            url, key, model, prompt, max_tokens=max_tokens, system=system,
        )
        last = (parsed, status, info)
        if parsed is not None:
            return last
        if status == 0 or status in skip_statuses:
            continue
        if status != 0:
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
    ):
        if os.environ.get(env):
            keys[k] = os.environ[env]
    gem_key = gemini_key()
    if gem_key:
        keys["gemini"] = gem_key
    mistral = mistral_key()
    if mistral:
        keys["mistral"] = mistral
    nim = nvidia_nim_key()
    if nim:
        keys["nvidia_nim"] = nim
    polli = pollinations_key()
    if polli:
        keys["pollinations"] = polli
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
    th_key = tokenhub_key()
    if th_key:
        keys["tokenhub"] = th_key
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
            primary_models_for("openrouter", tmpl),
            lambda model: oc(
                "https://openrouter.ai/api/v1/chat/completions",
                keys["openrouter"], model, extra,
            ),
        )
    if lane == "deepseek":
        # Paid host — skip unless explicitly opted in (even if key present).
        if not allow_paid_deepseek():
            return None, None
        if not keys.get("deepseek"):
            return None, None
        return hop_models(
            "deepseek",
            primary_models_for("deepseek", tmpl),
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
            primary_models_for("qwen", tmpl),
            lambda model: flu(qwen_urls(), keys["qwen"], model),
        )
    if lane == "zhipu":
        if not keys.get("zhipu"):
            return None, None
        return hop_models(
            "zhipu",
            primary_models_for("zhipu", tmpl),
            lambda model: flu(ZHIPU_URLS, keys["zhipu"], model),
        )
    if lane == "moonshot":
        if not keys.get("moonshot"):
            return None, None
        return hop_models(
            "moonshot",
            primary_models_for("moonshot", tmpl),
            lambda model: flu(MOONSHOT_URLS, keys["moonshot"], model),
        )
    if lane == "siliconflow":
        if not keys.get("siliconflow"):
            return None, None
        return hop_models(
            "siliconflow",
            primary_models_for("siliconflow", tmpl),
            lambda model: flu(SF_URLS, keys["siliconflow"], model),
        )
    if lane == "modelscope":
        if not keys.get("modelscope"):
            return None, None
        return hop_models(
            "modelscope",
            primary_models_for("modelscope", tmpl),
            lambda model: oc(
                "https://api-inference.modelscope.cn/v1/chat/completions",
                keys["modelscope"], model,
            ),
        )
    if lane == "mistral":
        if not keys.get("mistral"):
            return None, None
        return hop_models(
            "mistral",
            primary_models_for("mistral", tmpl),
            lambda model: oc(MISTRAL_URL, keys["mistral"], model),
        )
    if lane == "nvidia_nim":
        if not keys.get("nvidia_nim"):
            return None, None
        return hop_models(
            "nvidia_nim",
            primary_models_for("nvidia_nim", tmpl),
            lambda model: oc(NVIDIA_NIM_URL, keys["nvidia_nim"], model),
        )
    if lane == "pollinations":
        # OpenAI-compatible chat at gen.pollinations.ai. Missing key / 402
        # (empty Quest pollen) skip clean — same DEAD_PROVIDER contract.
        if not keys.get("pollinations"):
            return None, None
        return hop_models(
            "pollinations",
            primary_models_for("pollinations", tmpl),
            lambda model: oc(POLLINATIONS_URL, keys["pollinations"], model),
        )
    if lane == "tokenhub":
        if not keys.get("tokenhub"):
            return None, None
        return hop_models(
            "tokenhub",
            primary_models_for("tokenhub", tmpl),
            lambda model: first_live_url(
                tokenhub_urls(), keys["tokenhub"], model, prompt,
                max_tokens=max_tokens, system=system,
                skip_statuses=(401, 403),
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
            primary_models_for("github_models", tmpl),
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
            primary_models_for("cloudflare", tmpl),
            lambda model: oc(cf_url, keys["cloudflare"], model),
        )
    if lane == "sambanova":
        if not keys.get("sambanova"):
            return None, None
        return hop_models(
            "sambanova",
            primary_models_for("sambanova", tmpl),
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
            [m for m in hf_free_models(keys["hf"]) if not is_banned_primary(m)],
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
            primary_models_for("groq", tmpl),
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
            primary_models_for("gemini", tmpl),
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
