#!/usr/bin/env python3
"""Snapshot OpenClaw runtime doors for the phone Claw tab.

  --write   probe this box, push 01_daily/_runtime.json
  --heal    restart the gateway/timers / patch 1800s timeouts if a door is down

Catches the things that actually kill pre-open and post-close:
  OpenClaw dead while systemd still says active
  48-char live json vs 64-char GitHub secret (401 path)
  HTTP 401 / 403 from the classroom or Elite scrape
  timeoutSeconds / OPENCLAW_TIMEOUT still 1800 (Grok turns stub at 30m; want 10800)
  GROK_ONLY missing / DeepSeek key still injected into a Grok job
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
from datetime import datetime, timedelta
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
    "-m src.map_heat",
    "map_heat_postclose",
    "run_preopen_all",
)
WF_FILES = (
    ROOT / ".github" / "workflows" / "preopen_all.yml",
    ROOT / ".github" / "workflows" / "map_heat_postclose.yml",
)
SH_FILES = (
    ROOT / "scripts" / "ecs_preopen.sh",
    ROOT / "scripts" / "ecs_map_postclose.sh",
)
TIMEOUT_ASSIGN = re.compile(
    r"OPENCLAW_TIMEOUT(?:\s*[:=]\s*[\"']?(\d+)|[^\n]*?:-[\"']?(\d+))"
)
STUB_RE = re.compile(
    r"(LLM request timed out|model idle timeout|idle timeout|gateway timeout|"
    r"HTTP 401|401 Unauthorized|HTTP 403|403 Forbidden|"
    r"Aliyun 403|maps were not scraped|TIMEOUT/STUB|"
    r"The model did not produce a response|"
    r"EMERGENCY: GROK_ONLY suspended|falling back to DeepSeek|"
    r"live OpenClaw token len=64|using_token=len=64)",
    re.I,
)
TIMEOUT_RE = re.compile(
    r"(LLM request timed out|model idle timeout|idle timeout|gateway timeout|"
    r"runTimeoutSeconds|timed out after|OPENCLAW_TIMEOUT[=:] ?1800)",
    re.I,
)


def now_iso() -> str:
    return datetime.now(ET).isoformat()


def et_dates(now: datetime | None = None) -> list[str]:
    now = now or datetime.now(ET)
    today = now.date()
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


def gh_grok_busy() -> bool:
    """Queued/in-progress Grok workflows own the only ECS runner."""
    for wf in ("map_heat_postclose.yml", "preopen_all.yml"):
        st = str(gh_latest(wf).get("status") or "").lower()
        if st in ("in_progress", "queued", "waiting", "requested"):
            return True
    return False


def _json(path: Path):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, ValueError):
        return None


def _text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8", errors="ignore")
    except OSError:
        return ""


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
        return ("WARN", True,
                f"json={json_n} env={env_n} — live json wins; env is 64-char 401 path",
                "heal")
    if json_n == LIVE_TOKEN_LEN:
        return "OK", True, f"json={json_n} env={env_n}", "none"
    if json_n == 0 and env_n == STALE_SECRET_LEN:
        return "FAIL", True, "only 64-char GitHub secret — 401 / SearXNG path", "heal"
    if json_n == STALE_SECRET_LEN:
        return "FAIL", True, f"json is 64-char secret — 401 path (env={env_n})", "heal"
    if env_n == STALE_SECRET_LEN and json_n == 0:
        return "FAIL", True, "64-char secret, no live json", "heal"
    if json_n or env_n:
        return "WARN", True, f"json={json_n} env={env_n}", "heal"
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


def yaml_timeout_hits() -> list[tuple[str, int]]:
    hits: list[tuple[str, int]] = []
    for p in (*WF_FILES, *SH_FILES):
        text = _text(p)
        if not text:
            continue
        for m in TIMEOUT_ASSIGN.finditer(text):
            raw = m.group(1) or m.group(2)
            try:
                hits.append((p.name, int(raw)))
            except (TypeError, ValueError):
                continue
    return hits


def timeout_verdict(fields: dict[str, int | None],
                    yaml_hits: list[tuple[str, int]] | None = None,
                    ) -> tuple[str, bool, str, str]:
    yaml_hits = yaml_hits if yaml_hits is not None else yaml_timeout_hits()
    bad: list[str] = []
    if not fields:
        bad.append("openclaw.json unreadable")
    for k, v in (fields or {}).items():
        if v is None:
            bad.append(f"{k}=missing")
        elif v < WANT_TIMEOUT:
            tag = "1800 kills Grok at 30m" if v == 1800 else f"{v}s too short"
            bad.append(f"{k}={v} ({tag}, want {WANT_TIMEOUT})")
    for name, v in yaml_hits:
        if v < WANT_TIMEOUT:
            tag = "1800 kills Grok at 30m" if v == 1800 else f"{v}s too short"
            bad.append(f"{name} OPENCLAW_TIMEOUT={v} ({tag}, want {WANT_TIMEOUT})")
    if bad:
        return "FAIL", True, "; ".join(bad), "heal"
    yaml_note = ""
    if yaml_hits:
        yaml_note = " yaml=" + ",".join(f"{n}:{v}" for n, v in yaml_hits)
    return "OK", True, f"all {WANT_TIMEOUT}s{yaml_note}", "none"


def yaml_grok_only_verdict() -> tuple[str, bool, str, str]:
    missing: list[str] = []
    deepseek: list[str] = []
    for p in WF_FILES:
        text = _text(p)
        if not text:
            missing.append(f"{p.name} unreadable")
            continue
        if not re.search(r'GROK_ONLY:\s*["\']?1', text):
            missing.append(f"{p.name} missing GROK_ONLY=1")
        if re.search(r"DEEPSEEK_API_KEY:\s*\$\{\{\s*secrets\.DEEPSEEK_API_KEY", text):
            deepseek.append(p.name)
    if missing:
        return "FAIL", True, "; ".join(missing), "heal"
    if deepseek:
        return ("WARN", False,
                f"{', '.join(deepseek)} still injects DEEPSEEK_API_KEY — GROK_ONLY must block it",
                "none")
    return "OK", False, "GROK_ONLY=1 in preopen + postclose yaml", "none"


def yaml_token_secret_note() -> str:
    notes: list[str] = []
    for p in WF_FILES:
        text = _text(p)
        if re.search(r"OPENCLAW_TOKEN:\s*\$\{\{\s*secrets\.OPENCLAW_TOKEN", text):
            notes.append(f"{p.name} injects 64-char GitHub secret")
    return "; ".join(notes)


def yaml_finviz_ecs_verdict() -> tuple[str, bool, str, str]:
    text = _text(ROOT / ".github" / "workflows" / "map_heat_postclose.yml")
    if not text:
        return "WARN", False, "postclose yaml unreadable", "none"
    if re.search(r"src\.map_heat\s+--date", text) and re.search(r"--force", text):
        return ("WARN", True,
                "post-close still runs map_heat --force on ECS (Aliyun 403)",
                "none")
    return "OK", True, "post-close does not scrape Finviz HTML on ECS", "none"


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
    return "WARN", True, f"pid={pid} port={'up' if port_up else 'down'}", "heal"


def pong_verdict(http: int, content: str, error: str) -> tuple[str, bool, str, str]:
    blob = f"{content} {error}"
    if http == 401:
        return "FAIL", True, "HTTP 401 — wrong token (64 vs 48)", "heal"
    if http == 403:
        return "FAIL", True, "HTTP 403 — classroom refused the token", "heal"
    if http in (502, 503):
        return "FAIL", True, f"HTTP {http} — gateway down/restarting", "heal"
    if TIMEOUT_RE.search(blob) or "timed out" in blob.lower():
        return "FAIL", True, f"timeout http={http} {error or content}"[:160], "heal"
    if http == 200 and "PONG" in (content or "").upper():
        return "OK", True, "PONG", "none"
    if http == 200 and (content or "").strip():
        # OpenClaw persona answers "Who am I?" after enable_openclaw_chat.
        # HTTP 200 + a live body means the classroom is up. Healing it
        # restarts the session and makes the greeting worse.
        return ("OK", True,
                f"classroom live ({(content or '').strip().splitlines()[0][:60]})",
                "none")
    if http == 200:
        return "FAIL", True, "HTTP 200 empty body", "heal"
    return "FAIL", True, f"http={http} {error or content}"[:160], "heal"


def run_verdict(conclusion: str, systemd_result: str) -> tuple[str, bool, str, str]:
    conc = (conclusion or "").lower()
    res = (systemd_result or "").lower()
    if conc == "timed_out" or res == "timeout":
        return "FAIL", False, f"timed_out (GHA={conclusion or '—'} systemd={systemd_result or '—'})", "none"
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


def scan_needles() -> dict[str, list[str]]:
    """401 / 403 / 1800 / 64-char / DeepSeek in latest packets + health JSON."""
    found: dict[str, list[str]] = {
        "401": [], "403": [], "1800": [], "64": [], "deepseek": [],
    }
    dates = et_dates()
    paths: list[Path] = []
    daily = ROOT / "01_daily"
    if daily.is_dir():
        paths.extend(sorted(daily.glob("*_pipeline_health*.json"))[-6:])
        paths.extend(sorted(daily.glob("*_preopen_status.json"))[-3:])
        paths.extend(sorted(daily.glob("*_grok_review.json"))[-3:])
    for d in dates:
        paths.extend([
            daily / "general" / f"{d}_predict.md",
            daily / "map_heat" / f"{d}_research_baseline.md",
            daily / "map_heat" / f"{d}_map_heat.json",
            daily / "news" / f"{d}_finviz_digest.json",
        ])
        tr = daily / "_transcripts"
        if tr.is_dir():
            paths.extend(sorted(tr.glob(f"{d}_map_postclose_*.json"))[-4:])
    seen: set[str] = set()
    for path in paths:
        key = str(path)
        if key in seen or not path.is_file():
            continue
        seen.add(key)
        try:
            text = path.read_text(encoding="utf-8", errors="ignore")[:20000]
        except OSError:
            continue
        low = text.lower()
        label = path.name
        if "401" in low or "unauthorized" in low:
            found["401"].append(label)
        if "403" in low or "aliyun" in low or "maps were not scraped" in low:
            found["403"].append(label)
        if "1800" in text or TIMEOUT_RE.search(text):
            found["1800"].append(label)
        if "len=64" in low or "64-char" in low or "token len=64" in low:
            found["64"].append(label)
        if "deepseek" in low and ("fallback" in low or "emergency" in low):
            found["deepseek"].append(label)
        if STUB_RE.search(text) and "403" not in found["403"]:
            if "403" in (STUB_RE.search(text).group(0) if STUB_RE.search(text) else ""):
                found["403"].append(label)
    return found


def scan_stubs(needles: dict[str, list[str]] | None = None) -> tuple[str, bool, str, str]:
    """Grok/classroom stubs only. Finviz 403 / empty tape is the http403 door.

    Leftover '403' in a digest or map_heat overlay used to FAIL this door
    every morning even when OpenClaw was fine. Heal then bounced the
    classroom for a rate-limit, which never un-403s Finviz.
    """
    needles = needles if needles is not None else scan_needles()
    hits: list[str] = []
    for key, label in (("401", "401"), ("1800", "1800/timeout"),
                       ("64", "64-char token"), ("deepseek", "DeepSeek fallback")):
        names = needles.get(key) or []
        if names:
            hits.append(f"{label} in {', '.join(names[:2])}")
    if not hits:
        return "OK", True, "no 401/timeout/64/DeepSeek stubs in this session's Grok packets", "none"
    return "FAIL", True, "; ".join(hits[:4]), "heal"


def demote_stub_if_live(st: str, req: bool, det: str, act: str, pong_st: str
                        ) -> tuple[str, bool, str, str]:
    """Leftover packet text is history when PONG is live. Do not bounce OpenClaw."""
    if st == "FAIL" and pong_st in ("OK", "SKIP"):
        return ("WARN", False,
                f"history — {det} (classroom live)", "none")
    return st, req, det, act


def door(id: str, name: str, group: str, status: str, required: bool,
         detail: str, action: str) -> dict:
    return {
        "id": id, "name": name, "group": group, "status": status,
        "required": required, "detail": (detail or "")[:180], "action": action,
    }


def snapshot() -> dict:
    busy = grok_busy() or gh_grok_busy()
    json_tok, json_n, env_tok, env_n = live_token()
    token = pick_token(json_tok, env_tok)
    tok_st, tok_req, tok_det, tok_act = token_verdict(json_n, env_n)
    secret_note = yaml_token_secret_note()
    if secret_note and tok_st == "OK":
        tok_st, tok_req, tok_det, tok_act = (
            "WARN", True, f"{tok_det} — {secret_note}", "heal")
    elif secret_note:
        tok_det = f"{tok_det} — {secret_note}"
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
    yaml_hits = yaml_timeout_hits()
    t_st, t_req, t_det, t_act = timeout_verdict(t_fields, yaml_hits)
    grok_st, grok_req, grok_det, grok_act = yaml_grok_only_verdict()
    finviz_st, finviz_req, finviz_det, finviz_act = yaml_finviz_ecs_verdict()

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
    needles = scan_needles()
    stub_st, stub_req, stub_det, stub_act = scan_stubs(needles)

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
        pong_st, pong_req, pong_detail, pong_act = (
            "SKIP", False, "Grok job running — not poking the classroom", "none")
        model_st, model_req, model_detail, model_act = (
            "SKIP", False, "probe skipped", "none")
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

    http401_st, http401_req, http401_det, http401_act = "OK", True, "no 401", "none"
    if pong_st == "FAIL" and "401" in pong_detail:
        http401_st, http401_req, http401_det, http401_act = (
            "FAIL", True, pong_detail, "heal")
    elif pong_st == "OK":
        http401_st, http401_req, http401_det, http401_act = (
            "OK", True, "live PONG 200 — last packet 401 is history", "none")
    elif needles.get("401"):
        http401_st, http401_req, http401_det, http401_act = (
            "WARN", False, f"401 in last packet {', '.join(needles['401'][:2])} — not live", "none")

    http403_st, http403_req, http403_det, http403_act = finviz_st, finviz_req, finviz_det, finviz_act
    # ECS heal_targets must not bounce OpenClaw for Finviz 403.
    if http403_act == "heal":
        http403_act = "none"
    tape_hits = []
    for d in et_dates():
        mh = ROOT / "01_daily" / "map_heat" / f"{d}_map_heat.json"
        data = _json(mh)
        if not isinstance(data, dict):
            continue
        notes = str(data.get("notes") or data.get("one_paragraph") or "")
        tape = data.get("tape")
        if "not scraped" in notes.lower() or "403" in notes:
            tape_hits.append(f"{mh.name}: Elite 403 / not scraped")
        if isinstance(tape, list) and not tape:
            tape_hits.append(f"{mh.name}: EMPTY TAPE")
    if needles.get("403"):
        tape_hits.append(f"403 in {', '.join(needles['403'][:2])}")
    if tape_hits and http403_st != "FAIL":
        http403_st, http403_req, http403_det, http403_act = (
            "FAIL", True, "; ".join(tape_hits[:3]), "none")
    # PONG 403 stays on the pong door. http403 is Finviz — dashboard heals by re-scraping at 5s.
    stub_st, stub_req, stub_det, stub_act = demote_stub_if_live(
        stub_st, stub_req, stub_det, stub_act, pong_st)

    if needles.get("64") and tok_st == "OK":
        tok_st, tok_req, tok_det, tok_act = (
            "WARN", False,
            f"{tok_det} — last packet used 64-char ({', '.join(needles['64'][:2])})",
            "none")
    if needles.get("1800") and t_st == "OK":
        t_st, t_req, t_det, t_act = (
            "WARN", False,
            f"{t_det} — last packet had 1800 ({', '.join(needles['1800'][:2])})",
            "none")

    pong_ok = pong_st in ("OK", "SKIP")
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
        door("http401", "HTTP 401", "classroom",
             http401_st, http401_req, http401_det, http401_act),
        door("grok_only", "GROK_ONLY", "classroom",
             grok_st, grok_req, grok_det, grok_act),
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
        door("http403", "HTTP 403", "postclose",
             http403_st, http403_req, http403_det, http403_act),
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


CLASSROOM_HEAL_FAIL = {
    "gateway", "port", "pong", "model", "process", "timeout", "token", "http401",
}
CLASSROOM_HEAL_UNPROVEN = {
    "process", "timeout", "http401", "gateway", "port",
}


def heal_targets(doors: list) -> set[str]:
    ids: set[str] = set()
    for d in doors or []:
        i = str(d.get("id") or "")
        st = str(d.get("status") or "")
        det = str(d.get("detail") or "")
        if i in CLASSROOM_HEAL_FAIL and st == "FAIL":
            ids.add(i)
        if i in CLASSROOM_HEAL_UNPROVEN and st == "WARN" and "unproven" in det:
            ids.add(i)
        if i in ("preopen_timer", "postclose_timer") and st == "FAIL":
            ids.add(i)
    return ids


def heal(payload: dict) -> list[str]:
    actions: list[str] = []
    if grok_busy() or gh_grok_busy():
        print("[runtime] Grok job running — will not bounce OpenClaw", flush=True)
        return actions
    ids = heal_targets(payload.get("doors") or [])
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
    door_bad = bool(ids & {
        "gateway", "port", "pong", "model", "token", "process", "http401",
    })
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
