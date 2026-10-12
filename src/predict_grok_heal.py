"""Grok-only heal for the Pre-Open general predict.

If 01_daily/general/<date>_predict.md is missing / fails QC, or the OpenClaw
breaker opened, or OpenClaw failed on auth (401/403/expired OAuth/refresh),
hard-restart the gateway (scripts/openclaw_oauth_guard.py hard_restart, the
OPENCLAW_FORCE_RESTART=true path), prove a real 17+25 -> 42 call, reset the
breaker, and retry the predict step with GROK_ONLY=1. At most MAX_RETRIES,
and never at/after 09:00 ET (that would be a backfill). Never DeepSeek.
"""
from __future__ import annotations

import importlib.util
import os
import re
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")
MAX_RETRIES = 2
AUTH_RE = re.compile(
    r"\b(401|403)\b|unauthori[sz]ed|forbidden|expired|refresh[ _-]?token|"
    r"oauth|invalid[_ ]grant|device login", re.I)
ROOT = Path(__file__).resolve().parent.parent


def before_cutoff(now: datetime | None = None) -> bool:
    now = (now or datetime.now(ET)).astimezone(ET)
    return (now.hour, now.minute) < (9, 0)


def is_auth_failure(text: str) -> bool:
    return bool(AUTH_RE.search(text or ""))


def breaker_opened() -> bool:
    from . import deepseek_client as dc
    return dc._breaker_open()


def reset_breaker() -> None:
    from . import deepseek_client as dc
    f = dc._breaker_file()
    if f:
        try:
            Path(f).write_text("", encoding="utf-8")
        except OSError:
            pass
    dc.reset_openclaw_breaker("predict heal restart")


def _guard_mod():
    p = ROOT / "scripts" / "openclaw_oauth_guard.py"
    spec = importlib.util.spec_from_file_location("openclaw_oauth_guard", p)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)  # type: ignore[union-attr]
    return m


def restart_and_verify(guard=None) -> tuple[bool, dict]:
    """Hard restart + real 17+25 completion. Returns (ok, result)."""
    g = guard or _guard_mod()
    result: dict = {}
    g.hard_restart(result)
    ok = bool(g.real_completion(result, "predict_heal"))
    return ok, result


def heal(date: str, predict_ok, run_predict, *, log_text=lambda: "",
         guard=None, now=None, max_retries: int = MAX_RETRIES) -> dict:
    """predict_ok() -> bool; run_predict(env) -> returncode;
    log_text() -> recent predict-step output (auth detection)."""
    out = {"retries": 0, "ok": bool(predict_ok()), "reason": ""}
    auth = is_auth_failure(log_text())
    if out["ok"] and not breaker_opened() and not auth:
        return out
    out["ok"] = False
    while out["retries"] < max_retries:
        if not before_cutoff(now() if callable(now) else now):
            out["reason"] = "past 09:00 ET — no retry (would be a backfill)"
            break
        ok, res = restart_and_verify(guard)
        out["restart"] = res
        if not ok:
            out["reason"] = ("gateway failed 17+25 after hard restart"
                             + (" — auth: device login needed" if auth else ""))
            break
        reset_breaker()
        out["retries"] += 1
        env = dict(os.environ, GROK_ONLY="1", LLM_BACKEND="grok")
        for k in ("DEEPSEEK_API_KEY", "XAI_API_KEY", "OPENCLAW_XAI_API_KEY",
                  "GROK_API_KEY"):
            env.pop(k, None)
        run_predict(env)
        if predict_ok() and not breaker_opened():
            out["ok"] = True
            out["reason"] = ""
            return out
        auth = is_auth_failure(log_text())
        out["reason"] = ("auth still failing after restart — device login needed"
                         if auth else "predict still missing/QC fail")
    return out
