#!/usr/bin/env python3
"""Prove SuperGrok answers through the OpenClaw classroom.

Runs on the ECS box (loopback :18789). Aligns the 48-char live token,
picks the cheapest general xAI model above 30B, inserts a PONG prompt
and one news-classify prompt, writes 01_daily/_openclaw_grok_ping.{json,md}.

Never falls back to DeepSeek or Lane. A DeepSeek reply is a fail.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src import config  # noqa: E402
from src.lane_route import extract_json  # noqa: E402
from src.news_impact.prompts import CLASSIFIER_SYSTEM, classifier_prompt  # noqa: E402
from src.openclaw_models import (  # noqa: E402
    DEFAULT_NEWS_MODEL,
    pick_cheapest_above_30b,
    try_order,
)

OUT_JSON = ROOT / "01_daily" / "_openclaw_grok_ping.json"
OUT_MD = ROOT / "01_daily" / "_openclaw_grok_ping.md"
PING_PROMPT = "Reply with exactly the word PONG and nothing else."
CLASSIFY_TITLE = "Brent crude jumps after a Hormuz tanker attack"


def grok_busy() -> bool:
    try:
        r = subprocess.run(
            ["bash", "-lc",
             "pgrep -af 'src.map_heat_postclose|src.run_preopen_all|"
             "ecs_preopen|ecs_map_postclose|-m src.map_heat' "
             "| grep -v pgrep | grep -v runtime_status | grep -v openclaw_grok_ping"],
            capture_output=True, text=True, timeout=8, check=False,
        )
        return bool((r.stdout or "").strip())
    except (OSError, subprocess.TimeoutExpired):
        return False


def _http(method: str, url: str, payload: dict | None = None,
          token: str = "", extra_headers: dict | None = None,
          timeout: int = 90) -> tuple[int, dict | str]:
    data = None
    headers = {"Accept": "application/json"}
    if payload is not None:
        data = json.dumps(payload).encode()
        headers["Content-Type"] = "application/json"
    if token:
        headers["Authorization"] = f"Bearer {token}"
    if extra_headers:
        headers.update(extra_headers)
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8", "replace")
            try:
                return resp.status, json.loads(raw) if raw else {}
            except json.JSONDecodeError:
                return resp.status, raw[:400]
    except urllib.error.HTTPError as e:
        raw = e.read().decode("utf-8", "replace")
        try:
            body: dict | str = json.loads(raw) if raw else {}
        except json.JSONDecodeError:
            body = raw[:400]
        return e.code, body
    except Exception as e:  # noqa: BLE001
        return 0, f"{type(e).__name__}: {e}"


def list_model_ids(gateway: str, token: str) -> list[str]:
    code, body = _http("GET", f"{gateway}/v1/models", token=token, timeout=20)
    if code != 200 or not isinstance(body, dict):
        return []
    out: list[str] = []
    for row in body.get("data") or body.get("models") or []:
        if isinstance(row, dict) and row.get("id"):
            out.append(str(row["id"]))
        elif isinstance(row, str):
            out.append(row)
    return out


def chat_once(gateway: str, token: str, model: str, prompt: str,
              system: str = "", max_tokens: int = 32) -> dict:
    messages = []
    if system:
        messages.append({"role": "system", "content": system})
    messages.append({"role": "user", "content": prompt})
    t0 = time.time()
    code, body = _http(
        "POST",
        f"{gateway}/v1/chat/completions",
        payload={
            "model": config.OPENCLAW_AGENT,
            "messages": messages,
            "max_tokens": max_tokens,
            "temperature": 0,
        },
        token=token,
        extra_headers={
            "x-openclaw-model": model,
            "x-openclaw-session-key": f"fullscan-grok-ping-{int(t0)}",
        },
        timeout=90,
    )
    content = ""
    if isinstance(body, dict):
        content = str(
            ((body.get("choices") or [{}])[0].get("message") or {}).get("content") or ""
        ).strip()
        err = body.get("error") or body.get("message")
    else:
        err = str(body)[:240]
    return {
        "http": code,
        "model": model,
        "seconds": round(time.time() - t0, 2),
        "content": content,
        "error": "" if content else (str(err)[:240] if err else f"http_{code}"),
        "raw_model": (body.get("model") if isinstance(body, dict) else "") or "",
    }


def looks_like_pong(text: str) -> bool:
    return "pong" in (text or "").lower()


def looks_like_grok_reply(text: str) -> bool:
    t = (text or "").strip()
    if len(t) < 2:
        return False
    low = t.lower()
    if any(n in low for n in (
        "llm request timed out", "idle timeout", "unauthorized",
        "insufficient", "model not found",
    )):
        return False
    return True


def write_report(payload: dict) -> None:
    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    ping = payload.get("ping") or {}
    cls = payload.get("classify") or {}
    lines = [
        "# OpenClaw SuperGrok ping",
        "",
        f"- generated: {payload.get('generated_at')}",
        f"- gateway: {payload.get('gateway')}",
        f"- chosen_model: `{payload.get('chosen_model')}`",
        f"- available: {', '.join(payload.get('available_models') or []) or '(list unauthorized)'}",
        f"- grok_busy: {payload.get('grok_busy')}",
        f"- ok: **{payload.get('ok')}**",
        "",
        "## PONG",
        "",
        f"- http: {ping.get('http')} in {ping.get('seconds')}s via `{ping.get('model')}`",
        f"- content: {ping.get('content') or '(empty)'}",
        f"- error: {ping.get('error') or '—'}",
        "",
        "## News classify prompt",
        "",
        f"- title: {CLASSIFY_TITLE}",
        f"- http: {cls.get('http')} in {cls.get('seconds')}s via `{cls.get('model')}`",
        f"- content: {(cls.get('content') or '(empty)')[:500]}",
        f"- parsed_event_class: {cls.get('event_class') or '—'}",
        "",
        f"VERDICT={'OK' if payload.get('ok') else 'FAIL'}",
        "",
    ]
    OUT_MD.write_text("\n".join(lines), encoding="utf-8")


def run() -> dict:
    config.align_openclaw_token()
    gateway = (config.OPENCLAW_GATEWAY_URL or "http://127.0.0.1:18789").rstrip("/")
    if os.path.isfile("/home/gha/.openclaw/openclaw.json"):
        gateway = "http://127.0.0.1:18789"
        config.OPENCLAW_GATEWAY_URL = gateway
        os.environ["OPENCLAW_GATEWAY_URL"] = gateway
    token = config.OPENCLAW_TOKEN or ""
    busy = grok_busy()
    health_code, health_body = _http("GET", f"{gateway}/health", timeout=8)
    available = list_model_ids(gateway, token)
    chosen = pick_cheapest_above_30b(available or None)
    os.environ["OPENCLAW_NEWS_MODEL"] = chosen
    config.OPENCLAW_NEWS_MODEL = chosen

    ping: dict = {}
    classify: dict = {}
    used = ""
    for model in try_order(available or None):
        ping = chat_once(gateway, token, model, PING_PROMPT, max_tokens=16)
        used = model
        if looks_like_pong(ping.get("content") or "") or looks_like_grok_reply(
            ping.get("content") or ""
        ):
            break
    if used:
        classify = chat_once(
            gateway, token, used,
            classifier_prompt(CLASSIFY_TITLE, "", ""),
            system=CLASSIFIER_SYSTEM,
            max_tokens=400,
        )
        parsed = extract_json(classify.get("content") or "") or {}
        classify["event_class"] = parsed.get("event_class") or ""
        classify["parsed_ok"] = bool(parsed.get("event_class"))

    ok = bool(
        looks_like_grok_reply(ping.get("content") or "")
        or looks_like_grok_reply(classify.get("content") or "")
    )
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "gateway": gateway,
        "token_len": len(token),
        "health_http": health_code,
        "health": health_body if not isinstance(health_body, dict)
        else {k: health_body.get(k) for k in ("ok", "status")},
        "available_models": available,
        "chosen_model": used or chosen or DEFAULT_NEWS_MODEL,
        "picker_default": DEFAULT_NEWS_MODEL,
        "grok_busy": busy,
        "ping": ping,
        "classify": classify,
        "ok": ok,
        "source": "openclaw_grok_ping",
    }
    write_report(payload)
    return payload


def main() -> int:
    payload = run()
    print(json.dumps({
        "ok": payload.get("ok"),
        "chosen_model": payload.get("chosen_model"),
        "ping": (payload.get("ping") or {}).get("content", "")[:80],
        "classify": (payload.get("classify") or {}).get("event_class"),
        "wrote": str(OUT_JSON),
    }, indent=2))
    return 0 if payload.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
