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

import contextlib
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


def share(n_left: int, floor_s: float, ceiling_s: float,
          reserve_s: float = 0) -> float | None:
    """Seconds one of `n_left` remaining items may use.

    None = unbounded step. 0.0 = not even `floor_s` is left, do not start.
    Otherwise an even split of what remains (after `reserve_s` for the
    parent's own tail work), clamped to [floor_s, ceiling_s]. The 09-09
    dispatch ran 11 sector predicts against one 2400s wall: sectors 1-9
    each took what they liked and the parent SIGKILLed the last two.
    """
    rem = remaining_s()
    if rem is None:
        return None
    usable = rem - reserve_s
    if usable < floor_s:
        return 0.0
    return max(floor_s, min(ceiling_s, usable / max(1, n_left)))


@contextlib.contextmanager
def narrowed(budget_s: float | None):
    """Narrow this process's deadline to `budget_s` from now, then restore.

    LLM / DB clients read the env at call time, so every read timeout and
    tool loop inside the block honours the slice instead of the parent's
    whole-step ceiling. None / <= 0 leaves the deadline untouched.
    """
    prev = os.environ.get(ENV)
    if budget_s is not None and budget_s > 0:
        new = time.time() + budget_s
        if prev:
            try:
                new = min(new, float(prev))
            except ValueError:
                pass
        os.environ[ENV] = f"{new:.0f}"
    try:
        yield
    finally:
        if prev is None:
            os.environ.pop(ENV, None)
        else:
            os.environ[ENV] = prev
