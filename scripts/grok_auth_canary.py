#!/usr/bin/env python3
"""ONE-SHOT SuperGrok heal-word probe (branch-only). Expects exact word heal."""
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

AUTH_RE = re.compile(
    r"device[ _-]?login|device code|re-?auth|unauthori[sz]ed|invalid_grant|"
    r"oauth|login (is )?(needed|required)|token (has )?expired|sign in",
    re.I,
)


def classify(http: int | None, text: str, minutes_left: float | None) -> str:
    t = (text or "").strip()
    if http in (401, 403) or AUTH_RE.search(t):
        return "DEVICE_LOGIN"
    # Accept exact heal, or heal as the only alphanumeric token
    low = re.sub(r"[^a-z]", "", t.lower())
    if http == 200 and (low == "heal" or t.strip().lower() == "heal"):
        return "OK"
    if minutes_left is not None and minutes_left <= 0:
        return "DEVICE_LOGIN"
    return "FAIL"


def call() -> tuple[int | None, str, float]:
    tok = g.gateway_token()
    body = json.dumps({
        "model": os.environ.get("OPENCLAW_AGENT", "openclaw/default"),
        "messages": [{"role": "user", "content": "Reply with only the word: heal"}],
        "max_tokens": 16,
        "temperature": 0,
    }).encode()
    req = urllib.request.Request(g.GW + "/v1/chat/completions", data=body, method="POST")
    req.add_header("Content-Type", "application/json")
    req.add_header("x-openclaw-model", g.MODEL)
    req.add_header("x-openclaw-session-key", f"fullscan-heal-probe-{int(time.time())}")
    if tok:
        req.add_header("Authorization", f"Bearer {tok}")
    t0 = time.time()
    try:
        with urllib.request.urlopen(req, timeout=g.CALL_TIMEOUT) as r:
            raw = r.read()[:20000].decode("utf-8", "replace")
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
    exp, src, _ = g.xai_oauth_expiry()
    left = (exp - datetime.now(timezone.utc)).total_seconds() / 60 if exp else None
    http, text, secs = call()
    verdict = classify(http, text, left)
    snippet = re.sub(r"(eyJ[\w.-]{10,}|[A-Za-z0-9_\-]{40,})", "<redacted>", re.sub(r"\s+", " ", text))[:200]
    if verdict == "OK":
        line = f"✅ HEAL PROBE OK: Grok returned heal in {secs:.1f}s; OAuth minutes left {left and round(left)}; reply: {snippet}"
    elif verdict == "DEVICE_LOGIN":
        line = (f"❌ DEVICE LOGIN NEEDED: http={http} minutes_left={left and round(left)} reply: {snippet}")
    else:
        timed = http is None and "Timeout" in (text or "")
        tag = "TIMEOUT" if timed else "FAIL"
        line = f"❌ HEAL PROBE {tag}: http={http} secs={secs:.1f} reply: {snippet}"
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
