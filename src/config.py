"""Shared config for the prediction pipeline. All credentials come from env."""
import json
import os

# --- credentials (never hardcode) ---
DEEPSEEK_API_KEY = os.environ.get("DEEPSEEK_API_KEY", "")
SEARXNG_URL = os.environ.get("SEARXNG_URL", "").rstrip("/")
FRED_API_KEY = os.environ.get("FRED_API_KEY", "")
DATABASE_URL = os.environ.get("DATABASE_URL", "")
DATABASE_KEY = os.environ.get("DATABASE_KEY", "")  # reserved (REST fallback)

# --- OpenClaw gateway = primary analysis engine (Grok 4.6 via SuperGrok) ---
# The gateway runs on the always-on box (Alibaba ECS, Singapore) and
# exposes an OpenAI-compatible POST /v1/chat/completions. When
# OPENCLAW_GATEWAY_URL is set, every LLM stage tries Grok first. Workflows
# set GROK_ONLY=0 so DeepSeek can take over on outage, quota, or empty output.
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
# Live gateway bearer from ~/.openclaw/openclaw.json is 48 chars.
# The GitHub OPENCLAW_TOKEN secret is 64 chars and 401s the classroom.
LIVE_OPENCLAW_TOKEN_LEN = 48
STALE_OPENCLAW_SECRET_LEN = 64
_OPENCLAW_TOKEN_ALIGNED = False

# --- DeepSeek fallback ---
# Function-calling stages on the DeepSeek path must use deepseek-chat
# (deepseek-reasoner has no tools support). Leave the key set for
# fallback runs; GROK_ONLY=1 remains available for an explicit Grok-only run.
MODEL_PREDICT = os.environ.get("MODEL_PREDICT", "deepseek-chat")
MODEL_OUTCOME = os.environ.get("MODEL_OUTCOME", "deepseek-chat")
MODEL_REFLECT = os.environ.get("MODEL_REFLECT", "deepseek-reasoner")
MODEL_DISTILL = os.environ.get("MODEL_DISTILL", "deepseek-reasoner")
DEEPSEEK_BASE_URL = os.environ.get("DEEPSEEK_BASE_URL",
                                   "https://api.deepseek.com")
# DeepSeek Chat rejects larger output budgets even when Grok accepts them.
# Keep fallback requests valid instead of forwarding Grok's 24k/40k caps.
DEEPSEEK_CHAT_MAX_TOKENS = int(
    os.environ.get("DEEPSEEK_CHAT_MAX_TOKENS", "8192")
)


def openclaw_enabled() -> bool:
    return bool(OPENCLAW_GATEWAY_URL)


def llm_backend() -> str:
    """auto | grok | deepseek — set by LLM_BACKEND or apply_llm_backend()."""
    raw = (os.environ.get("LLM_BACKEND") or "").strip().lower()
    if raw in ("auto", "grok", "deepseek"):
        return raw
    return "auto"


def prefer_deepseek() -> bool:
    """True when the operator forced DeepSeek (no Grok attempt)."""
    if llm_backend() == "deepseek":
        return True
    raw = (os.environ.get("FORCE_DEEPSEEK") or "").strip().lower()
    return raw in ("1", "true", "yes", "on")


def apply_llm_backend(name: str | None = None) -> str:
    """Normalize LLM_BACKEND + GROK_ONLY for this process.

    auto     — Grok first, DeepSeek if Grok is down/empty (GROK_ONLY=0)
    grok     — Grok only, no DeepSeek spend (GROK_ONLY=1)
    deepseek — skip the gateway, DeepSeek + SearXNG only
    """
    chosen = (name or os.environ.get("LLM_BACKEND") or "auto").strip().lower()
    if chosen not in ("auto", "grok", "deepseek"):
        chosen = "auto"
    os.environ["LLM_BACKEND"] = chosen
    if chosen == "grok":
        os.environ["GROK_ONLY"] = "1"
        os.environ.pop("FORCE_DEEPSEEK", None)
    elif chosen == "deepseek":
        os.environ["GROK_ONLY"] = "0"
        os.environ["FORCE_DEEPSEEK"] = "1"
    else:
        os.environ["GROK_ONLY"] = "0"
        os.environ.pop("FORCE_DEEPSEEK", None)
    print(f"[llm] backend={chosen} GROK_ONLY={os.environ.get('GROK_ONLY')} "
          f"FORCE_DEEPSEEK={os.environ.get('FORCE_DEEPSEEK', '0')}",
          flush=True)
    return chosen


def grok_only() -> bool:
    """True when DeepSeek must not run analysis.

    GROK_ONLY=1/0 forces the switch. LLM_BACKEND=grok is the same as 1;
    LLM_BACKEND=deepseek or auto forces 0. Default with no env: Grok
    first, DeepSeek if Grok is empty or transient-fails. Never pin
    Grok-only just because the gateway URL is set.
    """
    if prefer_deepseek():
        return False
    if llm_backend() == "grok":
        return True
    raw = (os.environ.get("GROK_ONLY") or "").strip().lower()
    if raw in ("0", "false", "no", "off"):
        return False
    if raw in ("1", "true", "yes", "on"):
        return True
    return False


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


def _token_from_openclaw_json(path: str) -> str:
    try:
        with open(path, encoding="utf-8") as fh:
            data = json.load(fh)
    except (OSError, ValueError):
        return ""
    if not isinstance(data, dict):
        return ""
    gw = data.get("gateway") or {}
    auth = gw.get("auth") if isinstance(gw.get("auth"), dict) else {}
    return str(auth.get("token") or gw.get("token") or auth.get("password") or "")


def live_openclaw_json_token() -> str:
    """Gateway bearer from ECS json. Prefer 48-char; skip empty files."""
    seen: set[str] = set()
    found: list[str] = []
    for path in (
        "/home/gha/.openclaw/openclaw.json",
        os.path.expanduser("~/.openclaw/openclaw.json"),
    ):
        if not path or path in seen:
            continue
        seen.add(path)
        token = _token_from_openclaw_json(path)
        if token:
            found.append(token)
    for token in found:
        if len(token) == LIVE_OPENCLAW_TOKEN_LEN:
            return token
    return found[0] if found else ""


def pick_openclaw_token(json_token: str = "", env_token: str = "") -> str:
    """Prefer 48-char live json. 64-char GitHub secret is the 401 path."""
    json_token = json_token or ""
    env_token = env_token or ""
    if len(json_token) == LIVE_OPENCLAW_TOKEN_LEN:
        return json_token
    if len(env_token) == LIVE_OPENCLAW_TOKEN_LEN:
        return env_token
    if json_token and len(json_token) != STALE_OPENCLAW_SECRET_LEN:
        return json_token
    if env_token and len(env_token) != STALE_OPENCLAW_SECRET_LEN:
        return env_token
    return json_token or env_token


def align_openclaw_token(*, force: bool = False) -> str:
    """Overwrite env/config with the live json token. Prints lengths only."""
    global OPENCLAW_TOKEN, OPENCLAW_GATEWAY_URL, _OPENCLAW_TOKEN_ALIGNED
    if _OPENCLAW_TOKEN_ALIGNED and not force:
        return OPENCLAW_TOKEN
    _OPENCLAW_TOKEN_ALIGNED = True
    json_token = live_openclaw_json_token()
    env_token = (os.environ.get("OPENCLAW_TOKEN")
                 or os.environ.get("OPENCLAW_GATEWAY_TOKEN")
                 or OPENCLAW_TOKEN or "")
    picked = pick_openclaw_token(json_token, env_token)
    if picked:
        OPENCLAW_TOKEN = picked
        os.environ["OPENCLAW_TOKEN"] = picked
        os.environ["OPENCLAW_GATEWAY_TOKEN"] = picked
        print(f"[openclaw] aligned token json_len={len(json_token)} "
              f"env_len={len(env_token)} using_len={len(picked)}")
        if len(picked) == STALE_OPENCLAW_SECRET_LEN:
            print("[openclaw] WARN: using 64-char token (GitHub secret / "
                  "401 path). Want 48-char live json.")
    else:
        print("[openclaw] no gateway token in json or env")
    if os.path.isfile("/home/gha/.openclaw/openclaw.json"):
        OPENCLAW_GATEWAY_URL = "http://127.0.0.1:18789"
        os.environ["OPENCLAW_GATEWAY_URL"] = OPENCLAW_GATEWAY_URL
        print("[openclaw] OPENCLAW_GATEWAY_URL -> http://127.0.0.1:18789")
    return picked

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
def _env_int(name: str, default: int, minimum: int = 1) -> int:
    raw = (os.environ.get(name) or "").strip()
    if not raw:
        return default
    try:
        return max(minimum, int(raw))
    except ValueError:
        return default


MAX_TOOL_ROUNDS = _env_int("MAX_TOOL_ROUNDS", 10)  # web_search rounds per LLM stage
# Sector outcome already has deterministic ETF actuals in the prompt.
# Each DeepSeek read is 120s and the per-sector child dies at 600s.
# 4 tool rounds + 1 forced close = 5 × 120s = 600s with no time to write.
SECTOR_TOOL_ROUNDS = _env_int("SECTOR_TOOL_ROUNDS", 2)
# One DeepSeek message can emit many tool_calls. Each search walks
# SearXNG→ddgs→html→gnews (up to ~65s). Two total searches leave
# room for the no-tool close inside the 600s child.
SECTOR_MAX_SEARCHES = _env_int("SECTOR_MAX_SEARCHES", 2)
SECTOR_CHAT_BUDGET_S = _env_int("SECTOR_CHAT_BUDGET_S", 420)
TZ = "America/New_York"