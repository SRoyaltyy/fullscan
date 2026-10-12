#!/usr/bin/env python3
"""Keep OpenClaw on the SuperGrok OAuth login and prove the gateway answers.

Modes (run as root on ECS; CLI calls drop to user gha):
  renew  - read xAI OAuth profile expiry; if it is within RENEW_WINDOW_MIN of
           expiry (or already expired), force OpenClaw's own refresh-token
           path by running one tiny local Grok call, then re-read expiry.
           Never uses XAI_API_KEY (it is stripped from the child env).
  guard  - MainPID alive (systemctl show + /proc/PID) and port 18789 answers a
           real small completion (arithmetic, not PONG). On failure: hard
           restart via scripts/enable_openclaw_chat.sh (force_restart=true
           path) and re-verify.
  all    - renew then guard.

Exit 0 = healthy. Exit 2 = renew failed (needs interactive device login).
Exit 3 = guard failed after hard restart. Writes a status JSON for the GH
workflow so a failure becomes a red run. Prints no tokens.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")
ROOT = Path(__file__).resolve().parent.parent
GHA = os.environ.get("FULLSCAN_USER", "gha")
HOME = os.environ.get("FULLSCAN_HOME", "/home/gha")
CFG = Path(HOME) / ".openclaw" / "openclaw.json"
UNIT = "fullscan-openclaw-gateway"
GW = os.environ.get("OPENCLAW_GUARD_URL", "http://127.0.0.1:18789").rstrip("/")
STATUS = Path(os.environ.get("OPENCLAW_GUARD_STATUS",
                             f"{HOME}/fullscan-logs/openclaw_guard_status.json"))
RENEW_WINDOW_MIN = int(os.environ.get("OPENCLAW_RENEW_WINDOW_MIN", "90"))
MIN_LEFT_MIN = int(os.environ.get("OPENCLAW_MIN_LEFT_MIN", "30"))
CALL_TIMEOUT = int(os.environ.get("OPENCLAW_GUARD_CALL_TIMEOUT", "90"))
MODEL = os.environ.get("OPENCLAW_BACKEND_MODEL", "xai/grok-4.3")
PAID_ENV = ("XAI_API_KEY", "OPENCLAW_XAI_API_KEY", "GROK_API_KEY")


def log(msg: str) -> None:
    print(f"[oauth-guard {datetime.now(ET):%H:%M:%S} ET] {msg}", flush=True)


def as_gha(cmd: list[str], timeout: int) -> subprocess.CompletedProcess:
    env = {k: v for k, v in os.environ.items() if k not in PAID_ENV}
    env.update(HOME=HOME, USER=GHA)
    if os.geteuid() == 0:
        unset = [x for k in PAID_ENV for x in ("-u", k)]
        cmd = ["sudo", "-n", "-u", GHA, "-H", "env", *unset, f"HOME={HOME}", *cmd]
    try:
        return subprocess.run(cmd, capture_output=True, text=True,
                              timeout=timeout, env=env, check=False)
    except subprocess.TimeoutExpired as e:
        return subprocess.CompletedProcess(cmd, 124, e.stdout or "", "timeout")
    except FileNotFoundError as e:
        return subprocess.CompletedProcess(cmd, 127, "", str(e))


# ---------------------------------------------------------------- renew
def _walk(o):
    if isinstance(o, dict):
        yield o
        for v in o.values():
            yield from _walk(v)
    elif isinstance(o, list):
        for v in o:
            yield from _walk(v)


def _to_dt(v) -> datetime | None:
    if v is None:
        return None
    if isinstance(v, (int, float)):
        s = v / 1000 if v > 1e11 else v
        return datetime.fromtimestamp(s, timezone.utc)
    try:
        return datetime.fromisoformat(str(v).replace("Z", "+00:00"))
    except ValueError:
        return None


def xai_oauth_expiry() -> tuple[datetime | None, str, bool | None]:
    """Return (expires_utc, source, has_refresh) for the xAI OAuth profile."""
    for args in (["openclaw", "models", "auth", "list", "--provider", "xai", "--json"],
                 ["openclaw", "models", "status", "--json"]):
        r = as_gha(args, 60)
        try:
            data = json.loads(r.stdout)
        except Exception:
            continue
        for d in _walk(data):
            blob = json.dumps(d)[:400].lower()
            if "xai" not in blob or "oauth" not in blob:
                continue
            for k in ("expires", "expiresAt", "expires_at", "expiry"):
                dt = _to_dt(d.get(k))
                if dt:
                    has_r = None
                    for rk in ("hasRefresh", "hasRefreshToken", "refreshable"):
                        if rk in d:
                            has_r = bool(d[rk])
                    return dt, " ".join(args[1:4]), has_r
    # Text fallback: "xai:... [xai/oauth; expires 2026-05-18T22:46:23.273Z]"
    r = as_gha(["openclaw", "models", "status"], 60)
    m = re.search(r"xai[^\n]*oauth[^\n]*expires\s+([0-9T:\-.]+Z?)", r.stdout or "", re.I)
    if m:
        return _to_dt(m.group(1)), "models status (text)", None
    return None, "unreadable", None


def renew(result: dict) -> bool:
    ver = as_gha(["openclaw", "--version"], 30)
    result["openclaw_version"] = (ver.stdout or ver.stderr).strip()[:80]
    log(f"openclaw {result['openclaw_version']}")
    env_has_paid = any(os.environ.get(k) for k in PAID_ENV)
    result["paid_key_in_env_stripped"] = env_has_paid
    exp, src, has_r = xai_oauth_expiry()
    now = datetime.now(timezone.utc)
    left = (exp - now).total_seconds() / 60 if exp else None
    result["expires_before"] = exp.isoformat() if exp else None
    result["minutes_left_before"] = round(left, 1) if left is not None else None
    result["expiry_source"] = src
    result["has_refresh_token"] = has_r
    log(f"xAI OAuth expires={result['expires_before']} left_min={result['minutes_left_before']} via {src}")
    if exp is None:
        result["renew"] = "FAIL: no xAI OAuth profile readable (login missing?)"
        return False
    if left > RENEW_WINDOW_MIN:
        result["renew"] = "SKIP: token fresh"
        return True
    # OpenClaw refreshes lazily: an expired profile is refreshed by the
    # runtime on the next xAI call and written back to its store. To renew
    # ahead of the window we wait out nothing - we make the call now; if the
    # token is still valid but close, the post-check below decides.
    call = as_gha(["openclaw", "infer", "model", "run", "--local", "--model", MODEL,
                   "Reply with only the number: 17+25"], 180)
    out = (call.stdout or "") + (call.stderr or "")
    result["renew_call_rc"] = call.returncode
    result["renew_call_answer_ok"] = "42" in (call.stdout or "")
    exp2, _, _ = xai_oauth_expiry()
    left2 = (exp2 - datetime.now(timezone.utc)).total_seconds() / 60 if exp2 else None
    result["expires_after"] = exp2.isoformat() if exp2 else None
    result["minutes_left_after"] = round(left2, 1) if left2 is not None else None
    log(f"after call rc={call.returncode} expires={result['expires_after']} left_min={result['minutes_left_after']}")
    if re.search(r"cloudflare|captcha|invalid_grant|re-?auth|refresh.*fail", out, re.I):
        result["renew"] = "FAIL: refresh rejected - interactive device login needed"
        result["renew_error_hint"] = re.sub(r"\s+", " ", out)[-240:]
        return False
    if left2 is None or left2 < MIN_LEFT_MIN:
        result["renew"] = ("FAIL: token not extended (OpenClaw refreshes only after expiry; "
                           "re-run after expiry or do device login)")
        return False
    result["renew"] = "OK" if (exp2 and exp and exp2 > exp) else "OK: still valid"
    return True


# ---------------------------------------------------------------- guard
def gateway_token() -> str:
    try:
        d = json.loads(CFG.read_text(encoding="utf-8"))
    except Exception:
        return ""
    gw = d.get("gateway") or {}
    auth = gw.get("auth") if isinstance(gw.get("auth"), dict) else {}
    return str(auth.get("token") or gw.get("token") or auth.get("password") or "")


def pid_alive(result: dict) -> bool:
    r = subprocess.run(["systemctl", "show", "-p", "MainPID", "-p", "ActiveState",
                        "-p", "SubState", UNIT], capture_output=True, text=True)
    kv = dict(l.split("=", 1) for l in r.stdout.splitlines() if "=" in l)
    pid = int(kv.get("MainPID", "0") or 0)
    alive = pid > 0 and Path(f"/proc/{pid}").exists()
    comm = ""
    if alive:
        try:
            comm = Path(f"/proc/{pid}/comm").read_text().strip()
            state = Path(f"/proc/{pid}/status").read_text()
            if re.search(r"^State:\s+Z", state, re.M):
                alive = False
        except OSError:
            alive = False
    result.update(main_pid=pid, pid_comm=comm, unit_active=kv.get("ActiveState"),
                  unit_sub=kv.get("SubState"), pid_alive=alive)
    if kv.get("ActiveState") == "active" and not alive:
        log("AUDIT: systemd says active but MainPID is dead/zombie")
    return alive


def real_completion(result: dict, tag: str) -> bool:
    tok = gateway_token()
    result["gateway_token_len"] = len(tok)  # 48 = live json; 64 = stale GH secret
    body = json.dumps({
        "model": os.environ.get("OPENCLAW_AGENT", "openclaw/default"),
        "messages": [{"role": "user", "content": "What is 17+25? Reply with only the number."}],
        "max_tokens": 16, "temperature": 0,
    }).encode()
    req = urllib.request.Request(GW + "/v1/chat/completions", data=body, method="POST")
    req.add_header("Content-Type", "application/json")
    req.add_header("x-openclaw-model", MODEL)
    req.add_header("x-openclaw-session-key", f"fullscan-oauth-guard-{int(time.time())}")
    if tok:
        req.add_header("Authorization", f"Bearer {tok}")
    t0 = time.time()
    try:
        with urllib.request.urlopen(req, timeout=CALL_TIMEOUT) as resp:
            data = json.loads(resp.read()[:20000])
        content = ((data.get("choices") or [{}])[0].get("message") or {}).get("content") or ""
        ok = "42" in content and "timed out" not in content.lower()
        result[f"call_{tag}"] = {"http": 200, "secs": round(time.time() - t0, 1),
                                 "answer": content[:40], "ok": ok}
    except urllib.error.HTTPError as e:
        ok = False
        result[f"call_{tag}"] = {"http": e.code, "secs": round(time.time() - t0, 1), "ok": False}
    except Exception as e:
        ok = False
        result[f"call_{tag}"] = {"error": type(e).__name__, "secs": round(time.time() - t0, 1),
                                 "ok": False}
    log(f"completion[{tag}] {result[f'call_{tag}']}")
    return ok


def hard_restart(result: dict) -> None:
    log("hard restart via enable_openclaw_chat.sh (OPENCLAW_FORCE_RESTART=true)")
    env = {k: v for k, v in os.environ.items() if k not in PAID_ENV}
    env["OPENCLAW_FORCE_RESTART"] = "true"
    r = subprocess.run(["bash", str(ROOT / "scripts" / "enable_openclaw_chat.sh")],
                       env=env, capture_output=True, text=True, timeout=600)
    tail = "\n".join((r.stdout or "").splitlines()[-6:])
    print(tail)
    result["restart_rc"] = r.returncode
    result["restarted"] = True


GROK_JOBS = r"src.map_heat_postclose|src.run_preopen_all|ecs_preopen|ecs_map_postclose|-m src.map_heat"


def grok_job_running() -> bool:
    r = subprocess.run(["pgrep", "-af", GROK_JOBS], capture_output=True, text=True)
    lines = [l for l in r.stdout.splitlines() if "pgrep" not in l and "oauth_guard" not in l]
    return bool(lines)


def guard(result: dict) -> bool:
    if pid_alive(result) and real_completion(result, "pre"):
        result["guard"] = "OK"
        return True
    if grok_job_running() and os.environ.get("OPENCLAW_GUARD_RESTART_DURING_JOB") != "1":
        # Never bounce the gateway under a live Pre-Open / map job.
        result["guard"] = "FAIL: gateway unhealthy, restart skipped (Grok job running)"
        return False
    hard_restart(result)
    time.sleep(10)
    if pid_alive(result) and real_completion(result, "post_restart"):
        result["guard"] = "OK after hard restart"
        return True
    result["guard"] = "FAIL after hard restart"
    return False


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("mode", choices=["renew", "guard", "all"])
    a = ap.parse_args()
    result: dict = {"mode": a.mode, "started_et": datetime.now(ET).isoformat()}
    rc = 0
    if a.mode in ("renew", "all") and not renew(result):
        rc = 2
    if a.mode in ("guard", "all") and not guard(result):
        rc = 3 if rc == 0 else rc
    result["ok"] = rc == 0
    result["exit"] = rc
    result["finished_et"] = datetime.now(ET).isoformat()
    try:
        STATUS.parent.mkdir(parents=True, exist_ok=True)
        STATUS.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    except OSError as e:
        log(f"could not write status: {e}")
    print(json.dumps(result, indent=2))
    if rc:
        log(f"FAILED rc={rc} - {result.get('renew') or ''} {result.get('guard') or ''}")
    return rc


if __name__ == "__main__":
    sys.exit(main())
