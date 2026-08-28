#!/usr/bin/env python3
"""Snapshot OpenClaw runtime doors for the phone Claw tab.

  --write   probe this box, push 01_daily/_runtime.json
  --heal    restart the gateway/timers / patch 1800s timeouts if a door is down

Catches the things that actually kill pre-open and post-close:
  OpenClaw dead while systemd still says active
  48-char live json vs 64-char GitHub secret (401 path)
  HTTP 401 / 403 from the classroom
  timeoutSeconds still 1800 (Grok turns stub at 30m; want 10800)
  last pre-open / post-close run timed_out or 401/403 stubs in the packet

Never PONG while a Grok job is running.
Never GH-dispatch ECS Grok jobs from here.
OAuth login is xai_device_reauth.py — this script only reports it.
"""
from __future__ import annotations

import argparse
import json
import os
import re
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
ENSURE_TIMEOUTS = ROOT / "scripts" / "ensure_openclaw_timeouts.sh"
OPENCLAW_ENV = Path(os.environ.get("HOME") or "/home/gha") / ".openclaw" / ".env"
JSON_CFGS = [
    Path("/home/gha/.openclaw/openclaw.json"),
    Path.home() / ".openclaw" / "openclaw.json",
]
GW = (os.environ.get("OPENCLAW_GATEWAY_URL") or "http://127.0.0.1:18789").rstrip("/")
LIVE_TOKEN_LEN = 48
STALE_SECRET_LEN = 64
WANT_TIMEOUT = 10800
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
STUB_RE = re.compile(
    r"(LLM request timed out|model idle timeout|idle timeout|gateway timeout|"
    r"HTTP 401|401 Unauthorized|HTTP 403|403 Forbidden|"
    r"Aliyun 403|maps were not scraped|TIMEOUT/STUB|"
    r"The model did not produce a response)",
    re.I,
)
TIMEOUT_RE = re.compile(
    r"(LLM request timed out|model idle timeout|idle timeout|gateway timeout|"
    r"runTimeoutSeconds|timed out after)",
    re.I,
)


def now_iso() -> str:
    return datetime.now(ET).isoformat()


def et_dates(now: datetime | None = None) -> list[str]:
    now = now or datetime.now(ET)
    today = now.date()
    from datetime import timedelta
    return [
        today.isoformat(),
        (today + timedelta(days=1)).isoformat(),
        (today - timedelta(days=1)).isoformat(),
    ]


def grok_busy() -> bool:
    try:
        r = subprocess.run(
            ["pgrep", "-af", "."],
            capture_output=True, text=True, timeout=8, check=False)
    except (OSError, subprocess.SubprocessError):
        return False
    blob = r.stdout or ""
    return any(
        n in line and "runtime_status" not in line and "pgrep" not in line
        for line in blob.splitlines()
        for n in GROK_NEEDLES
    )


def _json(path: Path):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, ValueError):
        return None


def live_token() -> tuple[str, int, str, int]:
    """Return (json_token, json_len, env_token, env_len). Prefer 48-char json."""
    json_tok = ""
    for p in JSON_CFGS:
        data = _json(p)
        if not isinstance(data, dict):
            continue
        gw = data.get("gateway") or {}
        auth = gw.get("auth") if isinstance(gw.get("auth"), dict) else {}
        token = str(auth.get("token") or gw.get("token") or auth.get("password") or "")
        if token:
            json_tok = token
            break
    env = os.environ.get("OPENCLAW_TOKEN") or os.environ.get("OPENCLAW_GATEWAY_TOKEN") or ""
    return json_tok, len(json_tok), env, len(env)


def pick_token(json_tok: str, env: str) -> str:
    if len(json_tok) == LIVE_TOKEN_LEN:
        return json_tok
    if len(env) == LIVE_TOKEN_LEN:
        return env
    if json_tok and len(json_tok) != STALE_SECRET_LEN:
        return json_tok
    if env and len(env) != STALE_SECRET_LEN:
        return env
    return json_tok or env


def token_verdict(json_n: int, env_n: int) -> tuple[str, bool, str, str]:
    """status, required, detail, action."""
    if json_n == LIVE_TOKEN_LEN and env_n == STALE_SECRET_LEN:
        return "WARN", False, f"json={json_n} env={env_n} — using live json; env is 64-char 401 path", "none"
    if json_n == LIVE_TOKEN_LEN:
        return "OK", True, f"json={json_n} env={env_n}", "none"
    if json_n == 0 and env_n == STALE_SECRET_LEN:
        return "FAIL", True, "only 64-char GitHub secret — 401 / SearXNG path", "heal"
    if json_n == STALE_SECRET_LEN:
        return "FAIL", True, f"json is 64-char secret — 401 path (env={env_n})", "heal"
    if env_n == STALE_SECRET_LEN and json_n == 0:
        return "FAIL", True, "64-char secret, no live json", "heal"
    if json_n or env_n:
        return "WARN", False, f"json={json_n} env={env_n}", "none"
    return "FAIL", True, "no token in json or env", "heal"


def timeout_fields(data: dict | None) -> dict[str, int | None]:
    if not isinstance(data, dict):
        return {}
    defaults = (data.get("agents") or {}).get("defaults") or {}
    sub = defaults.get("subagents") or {}
    xai = ((data.get("models") or {}).get("providers") or {}).get("xai") or {}
    def _n(v):
        try:
            return int(v)
        except (TypeError, ValueError):
            return None
    return {
        "agents.defaults.timeoutSeconds": _n(defaults.get("timeoutSeconds")),
        "subagents.runTimeoutSeconds": _n(sub.get("runTimeoutSeconds") if isinstance(sub, dict) else None),
        "models.providers.xai.timeoutSeconds": _n(xai.get("timeoutSeconds") if isinstance(xai, dict) else None),
    }


def timeout_verdict(fields: dict[str, int | None]) -> tuple[str, bool, str, str]:
    if not fields:
        return "FAIL", True, "openclaw.json unreadable — cannot see timeoutSeconds", "heal"
    bad = []
    for k, v in fields.items():
        if v is None:
            bad.append(f"{k}=missing")
        elif v < WANT_TIMEOUT:
            tag = "1800 kills Grok at 30m" if v == 1800 else f"{v}s too short"
            bad.append(f"{k}={v} ({tag}, want {WANT_TIMEOUT})")
    if bad:
        return "FAIL", True, "; ".join(bad), "heal"
    return "OK", True, f"all {WANT_TIMEOUT}s", "none"


def process_verdict(unit_active: bool, pid: int, pid_alive: bool,
                    port_up: bool, pong_ok: bool) -> tuple[str, bool, str, str]:
    """systemd 'active' is not enough — the PID can be dead."""
    if pong_ok and port_up:
        return "OK", True, f"pid={pid or '—'} pong ok", "none"
    if unit_active and not pid_alive:
        return "FAIL", True, (
            f"unit active, process dead (MainPID={pid or 0}) — OpenClaw died"
        ), "heal"
    if not port_up:
        return "FAIL", True, "no living OpenClaw, port 18789 down", "heal"
    if not unit_active and port_up and not pong_ok:
        return "FAIL", True, "port up but unit inactive and not answering", "heal"
    if not pid_alive:
        return "FAIL", True, "OpenClaw process not running", "heal"
    return "WARN", False, f"pid={pid} port={'up' if port_up else 'down'}", "heal"


def pong_verdict(http: int, content: str, error: str) -> tuple[str, bool, str, str]:
    blob = f"{content} {error}"
    if http == 200 and "PONG" in (content or "").upper():
        if TIMEOUT_RE.search(content or ""):
            return "FAIL", True, "timeout stub in PONG body", "heal"
        return "OK", True, "PONG", "none"
    if http == 401:
        return "FAIL", True, "HTTP 401 — wrong token (64 vs 48)", "heal"
    if http == 403:
        return "FAIL", True, "HTTP 403 — classroom refused the token", "heal"
    if http in (502, 503):
        return "FAIL", True, f"HTTP {http} — gateway down/restarting", "heal"
    if TIMEOUT_RE.search(blob) or "timed out" in blob.lower():
        return "FAIL", True, f"timeout http={http} {error or content}"[:160], "heal"
    if http == 200:
        return "FAIL", True, f"no PONG in body {content[:80]}", "heal"
    return "FAIL", True, f"http={http} {error or content}"[:160], "heal"


def run_verdict(conclusion: str, systemd_result: str) -> tuple[str, bool, str, str]:
    conc = (conclusion or "").lower()
    res = (systemd_result or "").lower()
    if conc == "timed_out" or res == "timeout":
        return "FAIL", True, f"timed_out (GHA={conclusion or '—'} systemd={systemd_result or '—'})", "heal"
    if conc in ("cancelled",):
        return "WARN", False, f"latest={conclusion}", "none"
    if res in ("failure", "failed", "exit-code") or conc == "failure":
        return "WARN", False, f"latest={conclusion or '—'} systemd={systemd_result or '—'}", "none"
    if conc == "success" or res == "success":
        return "OK", True, f"latest={conclusion or 'success'} systemd={systemd_result or '—'}", "none"
    if conc in ("in_progress", "queued"):
        return "SKIP", False, f"latest={conclusion}", "none"
    if not conc and not res:
        return "WARN", False, "no run yet", "none"
    return "WARN", False, f"latest={conclusion or '—'} systemd={systemd_result or '—'}", "none"


def port_open(host: str = "127.0.0.1", port: int = 18789) -> bool:
    try:
        with socket.create_connection((host, port), timeout=2):
            return True
    except OSError:
        return False


def systemctl(*args: str) -> tuple[int, str]:
    try:
        r = subprocess.run(
            ["systemctl", *args],
            capture_output=True, text=True, timeout=8, check=False)
        return r.returncode, (r.stdout or r.stderr or "").strip()
    except (OSError, subprocess.SubprocessError) as e:
        return 1, str(e)


def unit_show(unit: str) -> dict[str, str]:
    rc, out = systemctl("show", unit, "-p", "MainPID", "-p", "ActiveState",
                        "-p", "SubState", "-p", "Result", "-p", "UnitFileState")
    parsed: dict[str, str] = {}
    for line in (out or "").splitlines():
        if "=" in line:
            k, v = line.split("=", 1)
            parsed[k.strip()] = v.strip()
    parsed["_rc"] = str(rc)
    return parsed


def pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    return Path(f"/proc/{pid}").exists()


def pgrep_openclaw() -> bool:
    try:
        r = subprocess.run(
            ["pgrep", "-af", "openclaw"],
            capture_output=True, text=True, timeout=8, check=False)
    except (OSError, subprocess.SubprocessError):
        return False
    return any(
        line and "pgrep" not in line and "runtime_status" not in line
        for line in (r.stdout or "").splitlines()
    )


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
        "model": os.environ.get("OPENCLAW_AGENT") or "openclaw/default",
        "messages": [{"role": "user", "content": "Reply with exactly the word PONG"}],
        "max_tokens": 16,
        "temperature": 0,
    }).encode("utf-8")
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}" if token else "",
        "x-api-key": token or "",
    }
    req = urllib.request.Request(url, data=body, method="POST", headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=45) as resp:
            raw = resp.read().decode("utf-8", "replace")
            code = resp.status
    except urllib.error.HTTPError as e:
        raw = e.read().decode("utf-8", "replace") if e.fp else ""
        return {"http": e.code, "error": raw[:160] or str(e), "content": "", "model": ""}
    except (OSError, urllib.error.URLError, TimeoutError) as e:
        return {"http": 0, "error": str(e)[:160], "content": "", "model": ""}
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return {"http": code, "error": raw[:160], "content": "", "model": ""}
    content = str(((data.get("choices") or [{}])[0].get("message") or {}).get("content") or "")
    return {"http": code, "content": content, "model": str(data.get("model") or ""), "error": ""}


def gh_latest(wf: str) -> dict:
    token = os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN") or ""
    if not token:
        return {}
    url = (f"https://api.github.com/repos/SRoyaltyy/fullscan/actions/"
           f"workflows/{wf}/runs?per_page=3")
    req = urllib.request.Request(url, headers={
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "User-Agent": "fullscan-runtime",
    })
    try:
        with urllib.request.urlopen(req, timeout=12) as resp:
            payload = json.loads(resp.read().decode())
    except (OSError, urllib.error.URLError, json.JSONDecodeError, TimeoutError, ValueError):
        return {}
    runs = payload.get("workflow_runs") or []
    if not runs:
        return {}
    r = runs[0]
    return {
        "conclusion": str(r.get("conclusion") or ""),
        "status": str(r.get("status") or ""),
        "html_url": str(r.get("html_url") or ""),
        "display": str(r.get("display_title") or r.get("name") or ""),
    }


def scan_stubs() -> tuple[str, bool, str, str]:
    hits: list[str] = []
    dates = et_dates()
    paths: list[Path] = []
    for d in dates:
        paths.extend([
            ROOT / "01_daily" / "general" / f"{d}_predict.md",
            ROOT / "01_daily" / "map_heat" / f"{d}_research_baseline.md",
            ROOT / "01_daily" / "map_heat" / f"{d}_map_heat.json",
            ROOT / "01_daily" / "news" / f"{d}_finviz_digest.json",
        ])
    for path in paths:
        if not path.is_file():
            continue
        try:
            text = path.read_text(encoding="utf-8", errors="ignore")[:12000]
        except OSError:
            continue
        if STUB_RE.search(text):
            m = STUB_RE.search(text)
            label = m.group(0) if m else "stub"
            hits.append(f"{path.name}: {label}")
        if path.suffix == ".json":
            data = _json(path)
            if isinstance(data, dict):
                notes = str(data.get("notes") or data.get("one_paragraph") or "")
                tape = data.get("tape")
                if "not scraped" in notes.lower() or "403" in notes:
                    hits.append(f"{path.name}: Elite 403 / not scraped")
                if isinstance(tape, list) and not tape and "map_heat" in path.name:
                    hits.append(f"{path.name}: EMPTY TAPE (403 overlay)")
    if not hits:
        return "OK", False, "no 401/403/timeout stubs in latest packets", "none"
    return "FAIL", True, "; ".join(hits[:3]), "heal"


def door(id: str, name: str, group: str, status: str, required: bool,
         detail: str, action: str) -> dict:
    return {
        "id": id, "name": name, "group": group, "status": status,
        "required": required, "detail": (detail or "")[:180], "action": action,
    }


def snapshot() -> dict:
    busy = grok_busy()
    json_tok, json_n, env_tok, env_n = live_token()
    token = pick_token(json_tok, env_tok)
    tok_st, tok_req, tok_det, tok_act = token_verdict(json_n, env_n)
    up = port_open()
    gw = unit_show("fullscan-openclaw-gateway.service")
    try:
        pid = int(gw.get("MainPID") or 0)
    except ValueError:
        pid = 0
    unit_active = gw.get("ActiveState") == "active"
    alive = pid_alive(pid) or pgrep_openclaw()

    oc = None
    for p in JSON_CFGS:
        oc = _json(p)
        if oc:
            break
    t_fields = timeout_fields(oc)
    t_st, t_req, t_det, t_act = timeout_verdict(t_fields)

    pre_en = unit_show("fullscan-preopen.timer").get("UnitFileState") or ""
    post_en = unit_show("fullscan-map-postclose.timer").get("UnitFileState") or ""
    pre_svc = unit_show("fullscan-preopen.service")
    post_svc = unit_show("fullscan-map-postclose.service")
    pre_run = gh_latest("preopen_all.yml")
    post_run = gh_latest("map_heat_postclose.yml")
    pre_st, pre_req, pre_det, pre_act = run_verdict(
        pre_run.get("conclusion") or pre_run.get("status") or "",
        pre_svc.get("Result") or "")
    post_st, post_req, post_det, post_act = run_verdict(
        post_run.get("conclusion") or post_run.get("status") or "",
        post_svc.get("Result") or "")
    if pre_run.get("html_url"):
        pre_det = f"{pre_det}"
    stub_st, stub_req, stub_det, stub_act = scan_stubs()

    code, blob = models_check()
    oauth_st = "OK" if code == 0 else ("WARN" if code == 2 else "FAIL")
    oauth_why = (
        "models status --check ok" if code == 0
        else (blob or "oauth fail").replace("\n", " ")[:140]
    )
    env_key = False
    try:
        env_key = "XAI_API_KEY=" in OPENCLAW_ENV.read_text(encoding="utf-8")
    except OSError:
        env_key = bool(os.environ.get("XAI_API_KEY"))

    pong_st, pong_req, pong_detail, pong_act = "SKIP", False, "not probed", "none"
    model_st, model_req, model_detail, model_act = "SKIP", False, "not probed", "none"
    ping: dict = {}
    if busy:
        pong_st, pong_detail = "SKIP", "Grok job running — not poking the classroom"
        model_st, model_detail = "SKIP", "probe skipped"
    elif not up:
        pong_st, pong_req, pong_detail, pong_act = "FAIL", True, "port down", "heal"
        model_st, model_req, model_detail, model_act = "FAIL", True, "port down", "heal"
    else:
        ping = chat_ping(token)
        pong_st, pong_req, pong_detail, pong_act = pong_verdict(
            int(ping.get("http") or 0),
            str(ping.get("content") or ""),
            str(ping.get("error") or ""),
        )
        if ping.get("model"):
            pong_detail = f"{pong_detail} model={ping.get('model')}"
        model = (ping.get("model") or "").lower()
        if ping.get("http") == 200 and "deepseek" in model:
            model_st, model_req, model_detail, model_act = (
                "FAIL", True, f"model={ping.get('model')} — DeepSeek fallback", "heal")
        elif ping.get("http") == 200:
            model_st, model_req, model_detail, model_act = (
                "OK", True, f"model={ping.get('model') or 'openclaw/default'}", "none")
        else:
            model_st, model_req, model_detail, model_act = (
                "FAIL", True, "no 200 chat", "heal")

    if pong_st == "FAIL" and "401" in pong_detail and tok_st != "FAIL":
        tok_st, tok_req, tok_det, tok_act = (
            "FAIL", True, f"{tok_det} — PONG 401", "heal")

    pong_ok = pong_st == "OK"
    proc_st, proc_req, proc_det, proc_act = process_verdict(
        unit_active, pid, alive, up, pong_ok)
    gw_st = "OK" if unit_active and (alive or pong_ok or up) else (
        "FAIL" if not up else "WARN")
    if unit_active and not alive and not pong_ok:
        gw_st = "FAIL"
    gw_det = (
        f"{gw.get('ActiveState') or 'unknown'} pid={pid} "
        f"{'alive' if alive else 'DEAD'}"
    )
    if gw_st == "FAIL":
        gw_act = "heal"
    else:
        gw_act = "none"
        gw_st = "OK" if (unit_active or up) else "FAIL"

    pre_timer_ok = pre_en in ("enabled", "enabled-runtime")
    post_timer_ok = post_en in ("enabled", "enabled-runtime")

    grok_only = (os.environ.get("GROK_ONLY") or "1").strip() not in ("0", "false", "off")

    doors = [
        door("ecs", "ECS box", "box", "OK", True, "this job is on ecs-openclaw", "none"),
        door("process", "OpenClaw process", "box",
             proc_st, proc_req, proc_det, proc_act),
        door("gateway", "OpenClaw gateway", "box",
             gw_st, True, gw_det, gw_act),
        door("port", "Port 18789", "classroom",
             "OK" if up else "FAIL", True,
             GW if up else "connection refused", "heal" if not up else "none"),
        door("pong", "PONG", "classroom", pong_st, pong_req, pong_detail, pong_act),
        door("model", "Classroom model", "classroom",
             model_st, model_req, model_detail, model_act),
        door("token", "Token 48 vs 64", "classroom",
             tok_st, tok_req, tok_det, tok_act),
        door("timeout", "Grok turn timeout", "classroom",
             t_st, t_req, t_det, t_act),
        door("grok_only", "GROK_ONLY", "classroom",
             "OK" if grok_only else "WARN", False,
             "on" if grok_only else "off — DeepSeek fallback path", "none"),
        door("oauth", "xAI auth", "auth", oauth_st, True, oauth_why,
             "reauth" if oauth_st != "OK" else "none"),
        door("api_key", "XAI_API_KEY", "auth",
             "OK" if env_key else "WARN", False,
             "present" if env_key else "missing — OAuth dies ~6h", "none"),
        door("preopen_timer", "Pre-open timer", "preopen",
             "OK" if pre_timer_ok else "FAIL", True,
             pre_en or "disabled", "heal" if not pre_timer_ok else "none"),
        door("preopen_run", "Pre-open last run", "preopen",
             pre_st, pre_req, pre_det, pre_act),
        door("postclose_timer", "Post-close timer", "postclose",
             "OK" if post_timer_ok else "FAIL", True,
             post_en or "disabled", "heal" if not post_timer_ok else "none"),
        door("postclose_run", "Post-close last run", "postclose",
             post_st, post_req, post_det, post_act),
        door("stub", "Packet stubs", "postclose",
             stub_st, stub_req, stub_det, stub_act),
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
    doors = payload.get("doors") or []
    ids = {d["id"] for d in doors if d.get("required") and d.get("status") == "FAIL"}
    env = {**os.environ, "HOME": os.environ.get("HOME") or "/home/gha", "GROK_ONLY": "1"}
    if "timeout" in ids and ENSURE_TIMEOUTS.is_file():
        try:
            r = subprocess.run(
                ["bash", str(ENSURE_TIMEOUTS)], cwd=str(ROOT),
                capture_output=True, text=True, timeout=120, env=env, check=False)
            tail = (r.stdout or r.stderr or "")[-200:].replace("\n", " | ")
            actions.append(f"ensure_openclaw_timeouts.sh exit={r.returncode} {tail[:140]}")
        except (OSError, subprocess.SubprocessError) as e:
            actions.append(f"ensure_openclaw_timeouts.sh failed: {e}")
    door_bad = bool(ids & {"gateway", "port", "pong", "model", "token", "process", "stub"})
    if door_bad and ENABLE_CHAT.is_file():
        try:
            r = subprocess.run(
                ["bash", str(ENABLE_CHAT)], cwd=str(ROOT),
                capture_output=True, text=True, timeout=180, env=env, check=False)
            tail = (r.stdout or r.stderr or "")[-240:].replace("\n", " | ")
            actions.append(f"enable_openclaw_chat.sh exit={r.returncode} {tail[:160]}")
        except (OSError, subprocess.SubprocessError) as e:
            actions.append(f"enable_openclaw_chat.sh failed: {e}")
    for unit, did in (
        ("fullscan-preopen.timer", "preopen_timer"),
        ("fullscan-map-postclose.timer", "postclose_timer"),
    ):
        if did in ids:
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
