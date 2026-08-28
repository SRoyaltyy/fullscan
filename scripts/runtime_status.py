#!/usr/bin/env python3
"""Snapshot OpenClaw runtime doors for the phone Claw tab.

  --write   probe this box, push 01_daily/_runtime.json
  --heal    restart the gateway/timers if a door is down, then write

Never PONG while a Grok job is running.
Never GH-dispatch ECS Grok jobs from here.
OAuth login is xai_device_reauth.py — this script only reports it.
"""
from __future__ import annotations

import argparse
import json
import os
import socket
import subprocess
import sys
import urllib.error
import urllib.request
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")
ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "01_daily" / "_runtime.json"
PUSH = ROOT / "scripts" / "safe_git_push.sh"
ENABLE_CHAT = ROOT / "scripts" / "enable_openclaw_chat.sh"
OPENCLAW_ENV = Path(os.environ.get("HOME") or "/home/gha") / ".openclaw" / ".env"
JSON_CFGS = [
    Path("/home/gha/.openclaw/openclaw.json"),
    Path.home() / ".openclaw" / "openclaw.json",
]
GW = (os.environ.get("OPENCLAW_GATEWAY_URL") or "http://127.0.0.1:18789").rstrip("/")
LIVE_TOKEN_LEN = 48
GROK_NEEDLES = (
    "ecs_preopen.sh",
    "ecs_map_postclose.sh",
    "src.run_preopen_all",
    "src.map_heat_postclose",
    "src.run_stock_book_all",
    "src.run_outcome",
    "src.run_sector_outcome",
    "src.learn_cycle",
    "src.catalyst_daily",
)


def now_iso() -> str:
    return datetime.now(ET).isoformat()


def grok_busy() -> bool:
    try:
        r = subprocess.run(
            ["pgrep", "-af", "|".join(GROK_NEEDLES)],
            capture_output=True, text=True, timeout=8, check=False)
    except (OSError, subprocess.SubprocessError):
        return False
    blob = r.stdout or ""
    return any(
        line and "runtime_status" not in line and "pgrep" not in line
        for line in blob.splitlines()
    )


def _json(path: Path):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, ValueError):
        return None


def live_token() -> tuple[str, int]:
    for p in JSON_CFGS:
        data = _json(p)
        if not isinstance(data, dict):
            continue
        gw = data.get("gateway") or {}
        auth = gw.get("auth") if isinstance(gw.get("auth"), dict) else {}
        token = str(auth.get("token") or gw.get("token") or auth.get("password") or "")
        if token:
            return token, len(token)
    env = os.environ.get("OPENCLAW_TOKEN") or os.environ.get("OPENCLAW_GATEWAY_TOKEN") or ""
    return env, len(env)


def port_open(host: str = "127.0.0.1", port: int = 18789) -> bool:
    try:
        with socket.create_connection((host, port), timeout=2):
            return True
    except OSError:
        return False


def systemctl(*args: str) -> tuple[int, str]:
    try:
        r = subprocess.run(
            ["systemctl", "--user", *args],
            capture_output=True, text=True, timeout=8, check=False)
        return r.returncode, (r.stdout or r.stderr or "").strip()
    except (OSError, subprocess.SubprocessError) as e:
        return 1, str(e)


def models_check() -> tuple[int | None, str]:
    try:
        r = subprocess.run(
            ["openclaw", "models", "status", "--check"],
            capture_output=True, text=True, timeout=20,
            env={**os.environ, "HOME": os.environ.get("HOME") or "/home/gha"},
            check=False)
        blob = ((r.stdout or "") + "\n" + (r.stderr or "")).strip()
        return r.returncode, blob
    except (OSError, subprocess.SubprocessError) as e:
        return None, str(e)


def chat_ping(token: str) -> dict:
    url = f"{GW}/v1/chat/completions"
    body = json.dumps({
        "model": "openclaw/default",
        "messages": [{"role": "user", "content": "Reply with PONG only."}],
        "max_tokens": 8,
    }).encode("utf-8")
    req = urllib.request.Request(
        url, data=body, method="POST",
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}" if token else "",
        })
    try:
        with urllib.request.urlopen(req, timeout=12) as resp:
            raw = json.loads(resp.read().decode("utf-8", "replace"))
        content = (
            ((raw.get("choices") or [{}])[0].get("message") or {}).get("content") or ""
        )
        model = raw.get("model") or ""
        return {"http": 200, "content": content, "model": model}
    except urllib.error.HTTPError as e:
        return {"http": e.code, "error": str(e)}
    except (OSError, urllib.error.URLError, json.JSONDecodeError, ValueError) as e:
        return {"http": 0, "error": str(e)}


def door(id: str, name: str, group: str, status: str, required: bool,
         detail: str, action: str) -> dict:
    return {
        "id": id, "name": name, "group": group, "status": status,
        "required": required, "detail": detail[:180], "action": action,
    }


def snapshot() -> dict:
    busy = grok_busy()
    token, token_n = live_token()
    up = port_open()
    gw_rc, gw_out = systemctl("is-active", "fullscan-openclaw-gateway.service")
    pre_rc, _ = systemctl("is-enabled", "fullscan-preopen.timer")
    post_rc, _ = systemctl("is-enabled", "fullscan-map-postclose.timer")
    code, blob = models_check()
    oauth_st = "OK" if code == 0 else ("WARN" if code == 2 else "FAIL")
    oauth_why = "models status --check ok" if code == 0 else (blob or "oauth fail").replace("\n", " ")[:140]
    env_key = False
    try:
        env_key = "XAI_API_KEY=" in OPENCLAW_ENV.read_text(encoding="utf-8")
    except OSError:
        env_key = bool(os.environ.get("XAI_API_KEY"))

    pong_st, pong_detail, model_st, model_detail = "SKIP", "not probed", "SKIP", "not probed"
    if busy:
        pong_st, pong_detail = "SKIP", "Grok job running — not poking the classroom"
        model_st, model_detail = "SKIP", "probe skipped"
    elif not up:
        pong_st, pong_detail = "FAIL", "port down"
        model_st, model_detail = "FAIL", "port down"
    else:
        ping = chat_ping(token)
        content = (ping.get("content") or "")
        if ping.get("http") == 200 and "PONG" in content.upper():
            pong_st, pong_detail = "OK", f"model={ping.get('model')}"
        elif ping.get("http") == 401:
            pong_st, pong_detail = "FAIL", "HTTP 401 — wrong token"
        else:
            pong_st, pong_detail = "FAIL", f"http={ping.get('http')} {ping.get('error') or content}"[:140]
        model = (ping.get("model") or "").lower()
        if ping.get("http") == 200 and "deepseek" in model:
            model_st, model_detail = "FAIL", f"model={ping.get('model')} — DeepSeek fallback"
        elif ping.get("http") == 200:
            model_st, model_detail = "OK", f"model={ping.get('model') or 'openclaw/default'}"
        else:
            model_st, model_detail = "FAIL", "no 200 chat"

    if token_n == LIVE_TOKEN_LEN:
        tok_st, tok_det, tok_req = "OK", f"live_len={token_n}", True
    elif token_n == 64:
        tok_st, tok_det, tok_req = "FAIL", "64-char secret — 401 path", True
    elif token_n:
        tok_st, tok_det, tok_req = "WARN", f"len={token_n}", False
    else:
        tok_st, tok_det, tok_req = "FAIL", "no token", True

    gw_st = "OK" if gw_rc == 0 and "active" in (gw_out or "active") else "FAIL"
    doors = [
        door("ecs", "ECS box", "box", "OK", True, "this job is on ecs-openclaw", "none"),
        door("gateway", "OpenClaw gateway", "box",
             gw_st, True, gw_out or "inactive", "heal" if gw_st == "FAIL" else "none"),
        door("port", "Port 18789", "classroom",
             "OK" if up else "FAIL", True,
             GW if up else "connection refused", "heal" if not up else "none"),
        door("pong", "PONG", "classroom", pong_st, pong_st != "SKIP",
             pong_detail, "heal" if pong_st == "FAIL" else "none"),
        door("model", "Classroom model", "classroom", model_st, model_st != "SKIP",
             model_detail, "heal" if model_st == "FAIL" else "none"),
        door("token", "OpenClaw token", "classroom", tok_st, tok_req, tok_det,
             "heal" if tok_st == "FAIL" else "none"),
        door("oauth", "xAI auth", "auth", oauth_st, True, oauth_why,
             "reauth" if oauth_st != "OK" else "none"),
        door("api_key", "XAI_API_KEY", "auth",
             "OK" if env_key else "WARN", False,
             "present" if env_key else "missing — OAuth dies ~6h", "none"),
        door("preopen_timer", "Pre-open timer", "clock",
             "OK" if pre_rc == 0 else "FAIL", True,
             "enabled" if pre_rc == 0 else "disabled",
             "heal" if pre_rc != 0 else "none"),
        door("postclose_timer", "Post-close timer", "clock",
             "OK" if post_rc == 0 else "FAIL", True,
             "enabled" if post_rc == 0 else "disabled",
             "heal" if post_rc != 0 else "none"),
    ]
    required_fail = sum(1 for d in doors if d["required"] and d["status"] == "FAIL")
    return {
        "ok": required_fail == 0,
        "n_fail": required_fail,
        "grok_busy": busy,
        "updated_at": now_iso(),
        "source": "runtime_status",
        "doors": doors,
    }


def dump(payload: dict) -> None:
    OUT.parent.mkdir(parents=True, exist_ok=True)
    body = dict(payload)
    body["updated_at"] = now_iso()
    OUT.write_text(json.dumps(body, indent=2) + "\n", encoding="utf-8")
    print(f"[runtime] wrote {OUT} ok={body.get('ok')} fails={body.get('n_fail')}", flush=True)


def push(msg: str) -> None:
    if not PUSH.is_file():
        return
    env = {**os.environ, "HOME": os.environ.get("HOME") or "/home/gha"}
    r = subprocess.run(
        ["bash", str(PUSH), msg, "01_daily/_runtime.json"],
        cwd=str(ROOT), env=env, capture_output=True, text=True, timeout=90,
        check=False)
    print((r.stdout or "")[-400:], flush=True)
    if r.returncode != 0:
        print((r.stderr or "")[-400:], flush=True)
    try:
        urllib.request.urlopen(
            "https://purge.jsdelivr.net/gh/SRoyaltyy/fullscan@main/01_daily/_runtime.json",
            timeout=8,
        ).read()
    except (OSError, urllib.error.URLError):
        pass


def heal(payload: dict) -> list[str]:
    actions: list[str] = []
    if grok_busy():
        print("[runtime] Grok job running — will not bounce OpenClaw", flush=True)
        return actions
    door_bad = any(
        d["required"] and d["status"] == "FAIL" and d["id"] in
        {"gateway", "port", "pong", "model", "token"}
        for d in payload.get("doors") or []
    )
    if door_bad and ENABLE_CHAT.is_file():
        try:
            r = subprocess.run(
                ["bash", str(ENABLE_CHAT)], cwd=str(ROOT),
                capture_output=True, text=True, timeout=180,
                env={**os.environ, "HOME": os.environ.get("HOME") or "/home/gha",
                     "GROK_ONLY": "1"})
            tail = (r.stdout or r.stderr or "")[-240:].replace("\n", " | ")
            actions.append(f"enable_openclaw_chat.sh exit={r.returncode} {tail[:160]}")
        except (OSError, subprocess.SubprocessError) as e:
            actions.append(f"enable_openclaw_chat.sh failed: {e}")
    for unit, did in (
        ("fullscan-preopen.timer", "preopen_timer"),
        ("fullscan-map-postclose.timer", "postclose_timer"),
    ):
        d = next((x for x in payload.get("doors") or [] if x["id"] == did), None)
        if d and d["status"] != "OK":
            systemctl("enable", "--now", unit)
            actions.append(f"systemctl enable --now {unit}")
    for a in actions:
        print(f"[runtime] heal {a}", flush=True)
    return actions


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--write", action="store_true")
    ap.add_argument("--heal", action="store_true")
    args = ap.parse_args()
    os.environ.setdefault("HOME", "/home/gha")
    payload = snapshot()
    if args.heal:
        acts = heal(payload)
        payload = snapshot()
        payload["healed"] = acts
    dump(payload)
    push("chore: runtime doors for Claw")
    raise SystemExit(0 if payload.get("ok") else 1)


if __name__ == "__main__":
    if "--write" not in sys.argv and "--heal" not in sys.argv:
        sys.argv.append("--write")
    main()
