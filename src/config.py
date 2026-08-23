"""Shared config for the prediction pipeline. All credentials come from env."""
import os

# --- credentials (never hardcode) ---
DEEPSEEK_API_KEY = os.environ.get("DEEPSEEK_API_KEY", "")
XAI_API_KEY = os.environ.get("XAI_API_KEY", "")
SEARXNG_URL = os.environ.get("SEARXNG_URL", "").rstrip("/")
FRED_API_KEY = os.environ.get("FRED_API_KEY", "")
DATABASE_URL = os.environ.get("DATABASE_URL", "")
DATABASE_KEY = os.environ.get("DATABASE_KEY", "")  # reserved (REST fallback)

# SuperGrok on grok.com / Cursor is NOT an API. Nightly Actions need a
# key from https://console.x.ai stored as the GitHub secret XAI_API_KEY.
XAI_BASE_URL = os.environ.get("XAI_BASE_URL", "https://api.x.ai/v1").rstrip("/")
DEEPSEEK_BASE_URL = os.environ.get("DEEPSEEK_BASE_URL",
                                   "https://api.deepseek.com").rstrip("/")
MODEL_GROK = os.environ.get("MODEL_GROK", "grok-4.6")
# Optional; empty = use the model's own default. grok-4.6 already
# reasons at "high". Only set this if you know the model accepts it
# (chat-completions `reasoning_effort` is documented for grok-4.3).
XAI_REASONING_EFFORT = os.environ.get("XAI_REASONING_EFFORT", "").strip()


def is_xai_model(model: str) -> bool:
    m = (model or "").lower().strip()
    return m.startswith(("grok-", "xai/", "x-ai/"))


def _default_reasoner() -> str:
    return MODEL_GROK if XAI_API_KEY else "deepseek-reasoner"


# High-volume tool-calling stages stay on cheap DeepSeek unless overridden.
# Reasoning stages (reflect / distill / deepthink / book-reflect) use Grok
# automatically when XAI_API_KEY is present.
MODEL_PREDICT = os.environ.get("MODEL_PREDICT") or "deepseek-chat"
MODEL_OUTCOME = os.environ.get("MODEL_OUTCOME") or "deepseek-chat"
MODEL_JUDGE = os.environ.get("MODEL_JUDGE") or "deepseek-chat"
MODEL_REFLECT = os.environ.get("MODEL_REFLECT") or _default_reasoner()
MODEL_DISTILL = os.environ.get("MODEL_DISTILL") or _default_reasoner()
MODEL_DEEPTHINK = os.environ.get("MODEL_DEEPTHINK") or _default_reasoner()


def provider_for(model: str) -> str:
    return "xai" if is_xai_model(model) else "deepseek"


def api_key_for(model: str) -> str:
    return XAI_API_KEY if is_xai_model(model) else DEEPSEEK_API_KEY


def base_url_for(model: str) -> str:
    return XAI_BASE_URL if is_xai_model(model) else DEEPSEEK_BASE_URL


def has_key_for(model: str) -> bool:
    return bool(api_key_for(model))


def has_any_llm_key() -> bool:
    return bool(DEEPSEEK_API_KEY or XAI_API_KEY)


def fallback_model(model: str, tools: bool = False) -> str | None:
    """Cross-provider spare tire. Grok → DeepSeek (reasoner, or chat if
    tools=True). DeepSeek with no key → Grok if an xAI key exists."""
    if is_xai_model(model) and DEEPSEEK_API_KEY:
        return "deepseek-chat" if tools else "deepseek-reasoner"
    if (not is_xai_model(model)) and XAI_API_KEY and not DEEPSEEK_API_KEY:
        return MODEL_GROK
    return None


def resolve_model(model: str, tools: bool = False) -> str:
    """Pick a model we actually have a key for."""
    if has_key_for(model):
        return model
    fb = fallback_model(model, tools=tools)
    if fb and has_key_for(fb):
        return fb
    return model


def require_llm(model: str) -> None:
    """Exit if neither the requested model nor its fallback has a key."""
    if has_key_for(model):
        return
    fb = fallback_model(model)
    if fb and has_key_for(fb):
        return
    if is_xai_model(model):
        raise SystemExit(
            "XAI_API_KEY not set. SuperGrok on grok.com/Cursor is not an "
            "API key. Create one at https://console.x.ai and add it as "
            "the GitHub secret XAI_API_KEY (or set DEEPSEEK_API_KEY to "
            "fall back)."
        )
    raise SystemExit(
        "DEEPSEEK_API_KEY not set (and no XAI_API_KEY fallback either)"
    )


def reflect_ladder() -> list[tuple[str, int]]:
    """(model, max_tokens) rungs: primary reasoner twice, then fallbacks."""
    rungs: list[tuple[str, int]] = [
        (MODEL_REFLECT, 12000),
        (MODEL_REFLECT, 16000),
    ]
    fb = fallback_model(MODEL_REFLECT, tools=False)
    if fb and fb != MODEL_REFLECT:
        rungs.append((fb, 12000))
    if MODEL_PREDICT not in {m for m, _ in rungs}:
        rungs.append((MODEL_PREDICT, 8000))
    return rungs

# --- repo paths ---
GROUNDING = "00_grounding"
DAILY_GENERAL = "01_daily/general"
DAILY_SECTORS = "01_daily/sectors"
CHANNEL1_DIR = "01_daily/_channel1"
LESSONS_CANDIDATE = "02_lessons/candidate"
LESSONS_ACTIVE = "02_lessons/active"
LESSONS_ARCHIVE = "02_lessons/archive"
SCOREBOARD_JSON = "03_scoreboard/scoreboard.json"
MONTHLY_SUMMARY = "03_scoreboard/monthly_summary.md"
CONSOLIDATED_MEMORY = "04_consolidated_memory.md"
MEMORY_ARCHIVE = "04_archive"

TOPIC = "general"
MEMORY_WINDOW_DAYS = 10          # rolling predict/reflect files injected
MAX_TOOL_ROUNDS = 10             # web_search rounds per LLM stage
TZ = "America/New_York"
