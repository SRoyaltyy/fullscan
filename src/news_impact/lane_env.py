"""Redacted Lane env check. Names and PRESENT/MISSING only. Never values.

#314's runner had ZHIPU / SILICONFLOW / OPENROUTER / DASHSCOPE / TOKENHUB
in the job env and still did not hop: the push path left --lane off, and
the scoreboard text claimed "no provider keys". GITHUB_TOKEN is always
set on Actions and is not a current-flash provider.
"""
from __future__ import annotations

import os
from typing import Any

# Printed in the Action log. Related aliases included so a present key
# under another name is visible before Lane reads it.
ENV_NAMES = (
    "ZHIPU_API_KEY",
    "GLM_API_KEY",
    "SILICONFLOW_API_KEY",
    "OPENROUTER_API_KEY",
    "DASHSCOPE_API_KEY",
    "QWEN_API_KEY",
    "DASHSCOPE_BASE_URL",
    "TOKENHUB_API_KEY",
    "TOKENHUB_BASE_URL",
    "TENCENT_API_KEY",
    "TENCENT_BASE_URL",
    "HUNYUAN_API_KEY",
    "MOONSHOT_API_KEY",
    "DEEPSEEK_API_KEY",
    "GROQ_API_KEY",
    "GEMINI_API_KEY",
    "HF_TOKEN",
    "SAMBANOVA_API_KEY",
    "MODELSCOPE_API_KEY",
    "MODELSCOPE_API_TOKEN",
    "MODELSCOPE_SDK_TOKEN",
    "OLLAMA_URL",
    "LANE_URL",
    "GITHUB_MODELS_TOKEN",
    "CLOUDFLARE_API_TOKEN",
    "CLOUDFLARE_ACCOUNT_ID",
)

# Hopper -> env names Lane's load_keys() actually reads. If any name is
# PRESENT, that hopper must appear in load_keys() or the mapping is a bug.
HOPPER_ENV = {
    "zhipu": ("ZHIPU_API_KEY", "GLM_API_KEY"),
    "siliconflow": ("SILICONFLOW_API_KEY",),
    "openrouter": ("OPENROUTER_API_KEY",),
    "qwen": ("DASHSCOPE_API_KEY", "QWEN_API_KEY"),
    "tokenhub": ("TOKENHUB_API_KEY", "TENCENT_API_KEY", "HUNYUAN_API_KEY"),
    "moonshot": ("MOONSHOT_API_KEY",),
    "deepseek": ("DEEPSEEK_API_KEY",),
    "groq": ("GROQ_API_KEY",),
    "gemini": ("GEMINI_API_KEY",),
    "hf": ("HF_TOKEN",),
    "sambanova": ("SAMBANOVA_API_KEY",),
    "modelscope": ("MODELSCOPE_API_KEY", "MODELSCOPE_API_TOKEN", "MODELSCOPE_SDK_TOKEN"),
}

# Current-flash / $0 hoppers that may start a Lane run. GITHUB_TOKEN is
# not in this set (Actions always injects it; GitHub Models is retired).
CURRENT_HOPPERS = frozenset(HOPPER_ENV)


def _present(name: str) -> bool:
    return bool((os.environ.get(name) or "").strip())


def env_status() -> dict[str, str]:
    """Name -> PRESENT or MISSING. No values."""
    return {name: ("PRESENT" if _present(name) else "MISSING") for name in ENV_NAMES}


def print_redacted_env(status: dict[str, str] | None = None) -> dict[str, str]:
    """First lines of a Lane Action. Flush so the log cannot hide them."""
    status = status if status is not None else env_status()
    print("LANE ENV CHECK (redacted, names only)", flush=True)
    for name in ENV_NAMES:
        print(f"  {name}: {status.get(name, 'MISSING')}", flush=True)
    return status


def provider_ready(keys: dict) -> bool:
    return bool(CURRENT_HOPPERS & set(keys or ()))


def readiness() -> dict[str, Any]:
    """Compare redacted env names to lane.load_keys().

    bug=True means a named secret is PRESENT and Lane did not pick it up.
    ready=True means at least one current-flash hopper key loaded.
    """
    from src import lane_route as lane

    status = env_status()
    keys, _ollama, _gh = lane.load_keys()
    expected = [
        hop for hop, names in HOPPER_ENV.items()
        if any(status.get(n) == "PRESENT" for n in names)
    ]
    missed = [hop for hop in expected if hop not in (keys or {})]
    loaded = sorted(h for h in (keys or {}) if h in CURRENT_HOPPERS)
    return {
        "env": status,
        "ready": provider_ready(keys) and not missed,
        "bug": bool(missed),
        "missed_hoppers": missed,
        "loaded_hoppers": loaded,
        "github_token_ignored": _present("GITHUB_TOKEN"),
    }


def require_lane_env(strict: bool = False) -> dict[str, Any]:
    """Print the check. Exit via SystemExit on a mapping bug or, if strict, on missing keys."""
    info = readiness()
    print_redacted_env(info["env"])
    loaded = ", ".join(info["loaded_hoppers"]) or "(none)"
    print(f"  loaded_hoppers: {loaded}", flush=True)
    print(f"  github_token_ignored: {info['github_token_ignored']}", flush=True)
    if info["bug"]:
        missed = ", ".join(info["missed_hoppers"])
        print(
            f"LANE ENV BUG: PRESENT in the environment but Lane load_keys missed: {missed}",
            flush=True,
        )
        raise SystemExit(2)
    if strict and not info["ready"]:
        print(
            "LANE ENV MISSING: no current-flash provider key. "
            "Refusing a deterministic board labeled as Lane.",
            flush=True,
        )
        raise SystemExit(1)
    if info["ready"]:
        print("LANE ENV READY", flush=True)
    else:
        print("LANE ENV NOT READY (deterministic job may continue)", flush=True)
    return info
