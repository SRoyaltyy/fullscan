#!/usr/bin/env python3
"""Start xAI device-code login on this box and publish the code for the phone.

The GH job must NOT occupy the one ECS runner for 15 minutes. This script:

  --check    models status; if dead and Grok is idle, daemonize a waiter
  --force    start a waiter even if status looks ok
  --daemon   (internal) PTY-login, write 01_daily/_xai_reauth.json, git push,
             wait ≤15 min for the human to approve, write ok/fail, push again

Phone app polls the public JSON and one-taps accounts.x.ai/oauth2/device?user_code=.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import signal
import subprocess
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")
ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "01_daily" / "_xai_reauth.json"
LOG = Path(os.environ.get("FULLSCAN_LOG", "/home/gha/fullscan-logs/xai_reauth.log"))
PID = Path(os.environ.get("FULLSCAN_REAUTH_PID", "/home/gha/fullscan-logs/xai_reauth.pid"))
DEVICE_URI = "https://accounts.x.ai/oauth2/device"
WAIT_S = 12 * 60
PUSH = ROOT / "scripts" / "safe_git_push.sh"

CODE_RE = re.compile(
    r"(?:user[_\s-]*code|enter(?:\s+the)?\s+code|code)\s*[:=]\s*"
    r"([A-Z0-9]{4,8}(?:-[A-Z0-9]{4,8})?)",
    re.I,
)
URI_RE = re.compile(
    r"https?://(?:auth\.x\.ai|accounts\.x\.ai|x\.ai)/[^\s\"']+",
    re.I,
)
BARE_CODE_RE = re.compile(r"\b([A-Z0-9]{4}-[A-Z0-9]{4})\b")


def complete_uri(code: str | None, printed: str | None = None) -> str:
    printed = (printed or "").strip()
    if code and "user_code=" in printed and "accounts.x.ai" in printed:
        return printed
    if not code:
        return DEVICE_URI
    return f"{DEVICE_URI}?user_code={code}"


def now_iso() -> str:
    return datetime.now(ET).isoformat()


def parse_device_output(blob: str) -> tuple[str | None, str]:
    """Return (user_code, verification_uri) from openclaw / oauth CLI text."""
    text = blob or ""
    uri = DEVICE_URI
    m_uri = URI_RE.search(text)
    if m_uri:
        uri = m_uri.group(0).rstrip(").,;")
    code = None
    m = CODE_RE.search(text)
    if m:
        code = m.group(1).upper()
    if not code:
        m2 = BARE_CODE_RE.search(text)
        if m2:
            code = m2.group(1).upper()
    return code, uri


def load() -> dict:
    try:
        return json.loads(OUT.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


def dump(payload: dict) -> None:
    OUT.parent.mkdir(parents=True, exist_ok=True)
    body = dict(payload)
    body["updated_at"] = now_iso()
    OUT.write_text(json.dumps(body, indent=2) + "\n", encoding="utf-8")
    print(f"[reauth] wrote {OUT} status={body.get('status')} code={body.get('user_code')}",
          flush=True)


def push(msg: str) -> None:
    if not PUSH.is_file():
        return
    env = {**os.environ, "HOME": os.environ.get("HOME") or "/home/gha"}
    r = subprocess.run(
        ["bash", str(PUSH), msg, "01_daily/_xai_reauth.json"],
        cwd=str(ROOT), env=env, capture_output=True, text=True, timeout=90,
        check=False)
    print((r.stdout or "")[-400:], flush=True)
    if r.returncode != 0:
        print((r.stderr or "")[-400:], flush=True)


def grok_busy() -> bool:
    try:
        r = subprocess.run(
            ["pgrep", "-af", "map_heat_postclose|run_preopen_all|map_heat_research"],
            capture_output=True, text=True, timeout=8, check=False)
    except (OSError, subprocess.SubprocessError):
        return False
    blob = r.stdout or ""
    return any(
        line and "xai_device_reauth" not in line and "pgrep" not in line
        for line in blob.splitlines()
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


def pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def existing_waiter() -> int | None:
    try:
        pid = int(PID.read_text(encoding="utf-8").strip())
    except (OSError, ValueError):
        return None
    return pid if pid_alive(pid) else None


def waiting_fresh(existing: dict) -> bool:
    if existing.get("status") != "waiting" or not existing.get("user_code"):
        return False
    exp = existing.get("expires_at")
    if not exp:
        return True
    try:
        when = datetime.fromisoformat(str(exp).replace("Z", "+00:00"))
        if when.tzinfo is None:
            when = when.replace(tzinfo=ET)
        return when > datetime.now(ET)
    except ValueError:
        return True


def spawn_daemon(force: bool) -> int:
    """Start a waiter that can outlive this process.

    GitHub Actions reaps setsid orphans when the job exits — that killed
    openclaw before a user_code was ever pushed. Prefer systemd --user.
    On a dedicated GHA reauth job, fall back to in-process.
    """
    script = str(Path(__file__).resolve())
    args = [sys.executable, script, "--daemon"]
    if force:
        args.append("--force")
    env = {**os.environ, "HOME": os.environ.get("HOME") or "/home/gha"}
    unit = "xai-reauth"
    sysd = [
        "systemd-run", "--user", "--collect",
        f"--unit={unit}",
        "--same-dir",
        f"--working-directory={ROOT}",
        f"--setenv=HOME={env['HOME']}",
        *args,
    ]
    try:
        subprocess.run(
            ["systemctl", "--user", "reset-failed", unit],
            capture_output=True, timeout=8, check=False,
        )
        r = subprocess.run(sysd, capture_output=True, text=True, timeout=15, env=env)
        if r.returncode == 0:
            print(f"[reauth] systemd-run {(r.stdout or r.stderr or '').strip()}",
                  flush=True)
            return 0
        print(f"[reauth] systemd-run rc={r.returncode} {(r.stderr or '')[-240:]}",
              flush=True)
    except (OSError, subprocess.SubprocessError) as e:
        print(f"[reauth] systemd-run unavailable: {e}", flush=True)

    if os.environ.get("GITHUB_ACTIONS") == "true":
        print("[reauth] GITHUB_ACTIONS — running waiter in this process", flush=True)
        return daemon(force)

    LOG.parent.mkdir(parents=True, exist_ok=True)
    logf = open(LOG, "a", encoding="utf-8")
    proc = subprocess.Popen(
        ["setsid", *args],
        cwd=str(ROOT),
        stdout=logf,
        stderr=subprocess.STDOUT,
        start_new_session=True,
        env=env,
    )
    print(f"[reauth] setsid pid={proc.pid} (dies if a GHA job is ending)", flush=True)
    return 0


def start_login_pty() -> tuple[int, int]:
    import pty
    pid, fd = pty.fork()
    if pid == 0:
        os.environ.setdefault("HOME", "/home/gha")
        os.environ.setdefault("TERM", "xterm")
        os.execvp("openclaw", [
            "openclaw", "models", "auth", "login",
            "--provider", "xai", "--method", "oauth",
        ])
    return pid, fd


def read_until_code(fd: int, timeout: float) -> tuple[str | None, str, str]:
    buf = ""
    end = time.time() + timeout
    while time.time() < end:
        import select
        ready, _, _ = select.select([fd], [], [], 1.0)
        if not ready:
            code, uri = parse_device_output(buf)
            if code:
                return code, uri, buf
            continue
        try:
            chunk = os.read(fd, 4096)
        except OSError:
            break
        if not chunk:
            break
        buf += chunk.decode("utf-8", "replace")
        sys.stdout.write(chunk.decode("utf-8", "replace"))
        sys.stdout.flush()
        code, uri = parse_device_output(buf)
        if code:
            return code, uri, buf
    code, uri = parse_device_output(buf)
    return code, uri, buf


def wait_pid(pid: int, timeout: float) -> int:
    end = time.time() + timeout
    while time.time() < end:
        wpid, status = os.waitpid(pid, os.WNOHANG)
        if wpid == pid:
            if os.WIFEXITED(status):
                return os.WEXITSTATUS(status)
            return 1
        time.sleep(1)
    try:
        os.kill(pid, signal.SIGTERM)
    except OSError:
        pass
    return 124


def daemon(force: bool) -> int:
    PID.parent.mkdir(parents=True, exist_ok=True)
    PID.write_text(str(os.getpid()), encoding="utf-8")
    existing = load()
    if not force and waiting_fresh(existing) and existing_waiter():
        print("[reauth] waiter already running", flush=True)
        return 0
    print("[reauth] starting openclaw device login", flush=True)
    try:
        pid, fd = start_login_pty()
    except (OSError, FileNotFoundError) as e:
        dump({
            "status": "needs_reauth",
            "oauth": "FAIL",
            "pong_ok": None,
            "reason": f"could not start openclaw login: {e}"[:240],
            "user_code": None,
            "verification_uri": DEVICE_URI,
            "expires_at": None,
            "source": "xai_device_reauth",
        })
        push("chore: xAI reauth — openclaw login missing")
        return 1
    code, uri, blob = read_until_code(fd, timeout=45)
    if not code:
        dump({
            "status": "needs_reauth",
            "oauth": "FAIL",
            "pong_ok": None,
            "reason": "device login started but no user_code in 45s",
            "user_code": None,
            "verification_uri": uri or DEVICE_URI,
            "expires_at": None,
            "source": "xai_device_reauth",
            "debug_len": len(blob),
        })
        push("chore: xAI reauth — no device code yet")
        try:
            os.kill(pid, signal.SIGTERM)
        except OSError:
            pass
        return 1
    expires = (datetime.now(ET) + timedelta(minutes=14)).isoformat()
    dump({
        "status": "waiting",
        "oauth": "FAIL",
        "pong_ok": None,
        "reason": "Approve this code on the phone at accounts.x.ai",
        "user_code": code,
        "verification_uri": complete_uri(code, uri),
        "expires_at": expires,
        "source": "xai_device_reauth",
        "pid": pid,
    })
    push(f"chore: xAI device code {code}")
    rc = wait_pid(pid, WAIT_S)
    try:
        os.close(fd)
    except OSError:
        pass
    if rc == 0:
        dump({
            "status": "ok",
            "oauth": "OK",
            "pong_ok": None,
            "reason": "device login completed",
            "user_code": None,
            "verification_uri": DEVICE_URI,
            "expires_at": None,
            "source": "xai_device_reauth",
        })
        push("chore: xAI device login ok")
        print("[reauth] login ok", flush=True)
        return 0
    dump({
        "status": "needs_reauth",
        "oauth": "FAIL",
        "pong_ok": None,
        "reason": f"device login did not finish rc={rc}",
        "user_code": None,
        "verification_uri": DEVICE_URI,
        "expires_at": None,
        "source": "xai_device_reauth",
    })
    push("chore: xAI device login unfinished")
    return 1


def check(force: bool) -> int:
    existing = load()
    if waiting_fresh(existing):
        print("[reauth] already waiting — not starting another", flush=True)
        if force:
            return spawn_daemon(True)
        return 0
    waiter = existing_waiter()
    if waiter:
        print(f"[reauth] daemon {waiter} still alive", flush=True)
        return 0
    code, blob = models_check()
    print(f"[reauth] models --check rc={code}", flush=True)
    if code == 0 and not force:
        dump({
            "status": "ok",
            "oauth": "OK",
            "pong_ok": None,
            "reason": "models status --check ok",
            "user_code": None,
            "verification_uri": DEVICE_URI,
            "expires_at": None,
            "source": "xai_device_reauth",
        })
        push("chore: xAI auth still ok")
        return 0
    if grok_busy() and not force:
        dump({
            "status": "needs_reauth" if code != 0 else existing.get("status") or "ok",
            "oauth": "FAIL" if code == 1 else ("WARN" if code == 2 else "OK"),
            "pong_ok": None,
            "reason": "Grok job running — not starting device login. Phone: wait, then Start.",
            "user_code": None,
            "verification_uri": DEVICE_URI,
            "expires_at": None,
            "source": "xai_device_reauth",
        })
        push("chore: xAI needs reauth (Grok busy)")
        return 0
    extra = (blob or "").replace("\n", " ")[:160]
    dump({
        "status": "needs_reauth",
        "oauth": "FAIL" if code == 1 else ("WARN" if code == 2 else "FAIL"),
        "pong_ok": None,
        "reason": extra or "starting device login",
        "user_code": None,
        "verification_uri": DEVICE_URI,
        "expires_at": None,
        "source": "xai_device_reauth",
    })
    push("chore: xAI needs reauth — starting device login")
    return spawn_daemon(force)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--check", action="store_true")
    ap.add_argument("--force", action="store_true")
    ap.add_argument("--daemon", action="store_true")
    args = ap.parse_args()
    os.environ.setdefault("HOME", "/home/gha")
    if args.daemon:
        raise SystemExit(daemon(args.force))
    if args.check:
        raise SystemExit(check(args.force))
    if os.environ.get("GITHUB_ACTIONS") == "true":
        raise SystemExit(daemon(True))
    raise SystemExit(spawn_daemon(True))


if __name__ == "__main__":
    main()
