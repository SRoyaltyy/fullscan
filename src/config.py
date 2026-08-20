"""Shared config for the prediction pipeline. All credentials come from env."""
import os

# --- credentials (never hardcode) ---
DEEPSEEK_API_KEY = os.environ.get("DEEPSEEK_API_KEY", "")
SEARXNG_URL = os.environ.get("SEARXNG_URL", "").rstrip("/")
FRED_API_KEY = os.environ.get("FRED_API_KEY", "")
DATABASE_URL = os.environ.get("DATABASE_URL", "")
DATABASE_KEY = os.environ.get("DATABASE_KEY", "")  # unused; src/ never writes the DB

# --- DeepSeek models (function-calling stages must use deepseek-chat;
#     deepseek-reasoner does not support tools) ---
MODEL_PREDICT = os.environ.get("MODEL_PREDICT", "deepseek-chat")
MODEL_OUTCOME = os.environ.get("MODEL_OUTCOME", "deepseek-chat")
MODEL_REFLECT = os.environ.get("MODEL_REFLECT", "deepseek-reasoner")
MODEL_DISTILL = os.environ.get("MODEL_DISTILL", "deepseek-reasoner")
DEEPSEEK_BASE_URL = os.environ.get("DEEPSEEK_BASE_URL",
                                   "https://api.deepseek.com")

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
