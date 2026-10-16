#!/usr/bin/env python3
"""Pre-05:00 ET Grok auth canary. Read-only: no renew, no restart, no paid key.

One small real completion through the OpenClaw gateway (the same path the
Pre-Open predict uses). Exit 0 = Grok answered 42. Exit 3 = DEVICE LOGIN
NEEDED (auth/device-login text, 401/403, or expired OAuth). Exit 1 = other
failure (timeout, gateway down). Writes a one-line summary to
$GITHUB_STEP_SUMMARY.
"""
from __future__ import annotations

import json
import os
import re
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import openclaw_oauth_guard as g  # noqa: E402

AUTH_RE = re.compile(r"device[ _-]?login|device code|re-?auth|unauthori[sz]ed|invalid_grant|"
                     r"oauth|login (is )?(needed|required)|token (has )?expired|sign in", re.I)


def classify(http: int | None, text: str, minutes_left: float | None) -> str:
    if http in (401, 403) or AUTH_RE.search(text or ""):
        return "DEVICE_LOGIN"
    if http == 200 and "42" in (text or ""):
        return "OK"
    if minutes_left is not None and minutes_left <= 0:
        return "DEVICE_LOGIN"
    return "FAIL"


LONG_PROMPT = (
    "Write a detailed, numbered 40-point checklist (each point 3-4 sentences) for "
    "auditing a pre-market equity screening pipeline: data freshness, clock windows, "
    "sealing, fail-closed gates, broker paper mirrors, and post-close review. "
    "After the checklist, on its own last line, write the number 42."
)


def call(long: bool = False) -> tuple[int | None, str, float]:
    tok = g.gateway_token()
    prompt = LONG_PROMPT if long else "What is 17+25? Reply with only the number."
    body = json.dumps({
        "model": os.environ.get("OPENCLAW_AGENT", "openclaw/default"),
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": 6000 if long else 16, "temperature": 0,
    }).encode()
    req = urllib.request.Request(g.GW + "/v1/chat/completions", data=body, method="POST")
    req.add_header("Content-Type", "application/json")
    req.add_header("x-openclaw-model", g.MODEL)
    req.add_header("x-openclaw-session-key", f"fullscan-grok-canary-{'long-' if long else ''}{int(time.time())}")
    if tok:
        req.add_header("Authorization", f"Bearer {tok}")
    t0 = time.time()
    try:
        timeout = int(os.environ.get("CANARY_LONG_TIMEOUT", "420")) if long else g.CALL_TIMEOUT
        with urllib.request.urlopen(req, timeout=timeout) as r:
            raw = r.read()[:200000].decode("utf-8", "replace")
        try:
            d = json.loads(raw)
            text = ((d.get("choices") or [{}])[0].get("message") or {}).get("content") or raw
        except ValueError:
            text = raw
        return 200, text, time.time() - t0
    except urllib.error.HTTPError as e:
        return e.code, e.read()[:4000].decode("utf-8", "replace"), time.time() - t0
    except Exception as e:  # timeout, refused
        return None, f"{type(e).__name__}: {e}", time.time() - t0


def main() -> int:
    for k in g.PAID_ENV:
        os.environ.pop(k, None)
    # No-write: this script only prints and appends to $GITHUB_STEP_SUMMARY.
    long = os.environ.get("CANARY_LONG", "").lower() in ("1", "true", "yes")
    exp, src, _ = g.xai_oauth_expiry()
    left = (exp - datetime.now(timezone.utc)).total_seconds() / 60 if exp else None
    try:
        from zoneinfo import ZoneInfo
        exp_et = exp.astimezone(ZoneInfo("America/New_York")).strftime("%Y-%m-%d %H:%M ET") if exp else "unknown"
    except Exception:
        exp_et = str(exp)
    print(f"xAI OAuth expiry: {exp_et} (source {src}; minutes left {left and round(left)})")
    http, text, secs = call(long)
    verdict = classify(http, text[-400:] if long else text, left)
    if long and verdict == "OK":
        print(f"long call: {secs:.1f}s, {len(text)} chars")
        if secs <= 75:
            print(f"::warning::long call finished in {secs:.1f}s (not >75s)")
    snippet = re.sub(r"(eyJ[\w.-]{10,}|[A-Za-z0-9_\-]{40,})", "<redacted>", re.sub(r"\s+", " ", text))[:200]
    if verdict == "OK":
        line = (f"✅ Grok auth canary OK{' (long)' if long else ''}: answered in {secs:.1f}s"
                f"{f', {len(text)} chars' if long else ''}; OAuth expiry {exp_et}, minutes left {left and round(left)}")
    elif verdict == "DEVICE_LOGIN":
        line = (f"❌ DEVICE LOGIN NEEDED: Grok (xAI OAuth) on ECS needs an interactive device login "
                f"before the 05:49 ET Pre-Open. http={http} minutes_left={left and round(left)} reply: {snippet}")
    else:
        line = f"❌ Grok canary FAILED (not clearly auth): http={http} secs={secs:.1f} reply: {snippet}"
    print(line)
    out = os.environ.get("GITHUB_STEP_SUMMARY")
    if out:
        with open(out, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    if verdict == "DEVICE_LOGIN":
        print("::error title=DEVICE LOGIN NEEDED::" + line)
        return 3
    return 0 if verdict == "OK" else 1


if __name__ == "__main__":
    raise SystemExit(main())
