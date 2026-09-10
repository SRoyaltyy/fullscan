"""Hand a child step its wall-clock deadline so it concludes, not dies.

2026-09-09 dispatch: run_preopen_all killed news_actions and general
predict at the 960s subprocess ceiling. Inside, OpenClaw had hung for
the full OPENCLAW_TIMEOUT (900s) and the DeepSeek fallback had 60s left
— SIGKILL, no file, day board PARTIAL. The subprocess ceiling and the
HTTP timeouts did not know about each other.

The orchestrators now export FULLSCAN_STEP_DEADLINE (epoch seconds) in
the child's environment. deepseek_client reads it and shrinks every
HTTP read timeout / tool-loop budget so the last call still has room to
write the essay before the parent's timeout fires.
"""
from __future__ import annotations

import os
import time

ENV = "FULLSCAN_STEP_DEADLINE"
# Leave the parent this much to see the child exit and land the file.
MARGIN_S = 15


def child_env(timeout_s: int | None, base: dict | None = None) -> dict:
    """Copy of the environment with the step deadline set (or cleared)."""
    env = dict(base if base is not None else os.environ)
    if timeout_s is None or timeout_s <= 0:
        env.pop(ENV, None)
        return env
    env[ENV] = f"{time.time() + max(1, timeout_s - MARGIN_S):.0f}"
    return env


def remaining_s() -> float | None:
    """Seconds left before this step's deadline; None when unbounded."""
    raw = (os.environ.get(ENV) or "").strip()
    if not raw:
        return None
    try:
        return float(raw) - time.time()
    except ValueError:
        return None


def bounded(default: int, reserve: int = 0, floor: int = 20) -> int:
    """`default` shrunk so `reserve` seconds stay after it, never < floor."""
    rem = remaining_s()
    if rem is None:
        return default
    return max(floor, min(default, int(rem - reserve)))
