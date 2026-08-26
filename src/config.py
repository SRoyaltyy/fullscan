"""Shared config for the prediction pipeline. All credentials come from env."""
import os

# --- credentials (never hardcode) ---
DEEPSEEK_API_KEY = os.environ.get("DEEPSEEK_API_KEY", "")
SEARXNG_URL = os.environ.get("SEARXNG_URL", "").rstrip("/")
FRED_API_KEY = os.environ.get("FRED_API_KEY", "")
DATABASE_URL = os.environ.get("DATABASE_URL", "")
DATABASE_KEY = os.environ.get("DATABASE_KEY", "")  # reserved (REST fallback)

# --- OpenClaw gateway = SOLE analysis engine (Grok 4.6 via SuperGrok) ---
# The gateway runs on the always-on box (Alibaba ECS, Singapore) and
# exposes an OpenAI-compatible POST /v1/chat/completions. When
# OPENCLAW_GATEWAY_URL is set, EVERY LLM stage runs on Grok. DeepSeek
# is disabled unless GROK_ONLY=0 is set explicitly.
# On the OpenClaw path the model uses its own native web/X search —
# the local SearXNG tool loop is fallback-only (and unused when Grok-only).
OPENCLAW_GATEWAY_URL = os.environ.get("OPENCLAW_GATEWAY_URL", "").rstrip("/")
OPENCLAW_TOKEN = os.environ.get("OPENCLAW_TOKEN", "")
# OpenAI `model` field = agent target; the backend model rides a header.
OPENCLAW_AGENT = os.environ.get("OPENCLAW_AGENT", "openclaw/default")
OPENCLAW_BACKEND_MODEL = os.environ.get("OPENCLAW_BACKEND_MODEL",
                                        "xai/grok-4.6")
# 3h per call so a long Grok research turn is not killed as trash.
# Job-level GitHub timeout must be >= this (see preopen_all.yml).
OPENCLAW_TIMEOUT = int(os.environ.get("OPENCLAW_TIMEOUT", "10800"))

# --- DeepSeek (opt-in fallback only; off whenever Grok is configured) ---
# Function-calling stages on the DeepSeek path must use deepseek-chat
# (deepseek-reasoner has no tools support). Leave the key set for
# emergency GROK_ONLY=0 runs; production must not call it.
MODEL_PREDICT = os.environ.get("MODEL_PREDICT", "deepseek-chat")
MODEL_OUTCOME = os.environ.get("MODEL_OUTCOME", "deepseek-chat")
MODEL_REFLECT = os.environ.get("MODEL_REFLECT", "deepseek-reasoner")
MODEL_DISTILL = os.environ.get("MODEL_DISTILL", "deepseek-reasoner")
DEEPSEEK_BASE_URL = os.environ.get("DEEPSEEK_BASE_URL",
                                   "https://api.deepseek.com")


def openclaw_enabled() -> bool:
    return bool(OPENCLAW_GATEWAY_URL)


def grok_only() -> bool:
    """True when DeepSeek must not run analysis.

    GROK_ONLY=1/0 forces the switch. Default: on whenever the OpenClaw
    gateway is configured — Grok is the sole analysis engine.
    """
    raw = (os.environ.get("GROK_ONLY") or "").strip().lower()
    if raw in ("0", "false", "no", "off"):
        return False
    if raw in ("1", "true", "yes", "on"):
        return True
    return bool(OPENCLAW_GATEWAY_URL)


def has_llm() -> bool:
    """True if the configured analysis engine is available."""
    if grok_only():
        return bool(OPENCLAW_GATEWAY_URL)
    return bool(OPENCLAW_GATEWAY_URL or DEEPSEEK_API_KEY)


def require_llm() -> None:
    if grok_only() and not OPENCLAW_GATEWAY_URL:
        raise SystemExit(
            "GROK_ONLY is on — set OPENCLAW_GATEWAY_URL (+ OPENCLAW_TOKEN) "
            "so Grok 4.6 is the analysis engine. DeepSeek is not used."
        )
    if not has_llm():
        raise SystemExit(
            "No LLM configured. Set OPENCLAW_GATEWAY_URL (+ OPENCLAW_TOKEN) "
            "to use Grok 4.6 through the OpenClaw gateway."
        )

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
