"""Shared config for the prediction pipeline. All credentials come from env."""
import os

# --- credentials (never hardcode) ---
DEEPSEEK_API_KEY = os.environ.get("DEEPSEEK_API_KEY", "")
SEARXNG_URL = os.environ.get("SEARXNG_URL", "").rstrip("/")
FRED_API_KEY = os.environ.get("FRED_API_KEY", "")
DATABASE_URL = os.environ.get("DATABASE_URL", "")
DATABASE_KEY = os.environ.get("DATABASE_KEY", "")  # reserved (REST fallback)

# --- OpenClaw gateway = PRIMARY LLM (Grok 4.6 via SuperGrok OAuth) ---
# The gateway runs on the always-on box (Alibaba ECS, Singapore) and
# exposes an OpenAI-compatible POST /v1/chat/completions. When
# OPENCLAW_GATEWAY_URL is set, EVERY LLM stage goes to Grok first and
# only falls back to DeepSeek if the gateway fails or answers empty.
# On the OpenClaw path the model uses its own native web/X search —
# the local SearXNG tool loop is fallback-only.
OPENCLAW_GATEWAY_URL = os.environ.get("OPENCLAW_GATEWAY_URL", "").rstrip("/")
OPENCLAW_TOKEN = os.environ.get("OPENCLAW_TOKEN", "")
# OpenAI `model` field = agent target; the backend model rides a header.
OPENCLAW_AGENT = os.environ.get("OPENCLAW_AGENT", "openclaw/default")
OPENCLAW_BACKEND_MODEL = os.environ.get("OPENCLAW_BACKEND_MODEL",
                                        "xai/grok-4.6")
OPENCLAW_TIMEOUT = int(os.environ.get("OPENCLAW_TIMEOUT", "900"))

# --- DeepSeek (FALLBACK provider; also primary if OpenClaw is unset) ---
# Function-calling stages on the DeepSeek path must use deepseek-chat
# (deepseek-reasoner has no tools support).
MODEL_PREDICT = os.environ.get("MODEL_PREDICT", "deepseek-chat")
MODEL_OUTCOME = os.environ.get("MODEL_OUTCOME", "deepseek-chat")
MODEL_REFLECT = os.environ.get("MODEL_REFLECT", "deepseek-reasoner")
MODEL_DISTILL = os.environ.get("MODEL_DISTILL", "deepseek-reasoner")
DEEPSEEK_BASE_URL = os.environ.get("DEEPSEEK_BASE_URL",
                                   "https://api.deepseek.com")


def openclaw_enabled() -> bool:
    return bool(OPENCLAW_GATEWAY_URL)


def has_llm() -> bool:
    """True if at least one LLM path is configured."""
    return bool(OPENCLAW_GATEWAY_URL or DEEPSEEK_API_KEY)


def require_llm() -> None:
    if not has_llm():
        raise SystemExit(
            "No LLM configured. Set OPENCLAW_GATEWAY_URL (+ OPENCLAW_TOKEN) "
            "to use Grok 4.6 through the OpenClaw gateway, and/or "
            "DEEPSEEK_API_KEY as the fallback provider."
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
