#!/usr/bin/env python3
"""Start xAI device-code login on this box and publish the code for the phone.

The GH job must NOT occupy the one ECS runner for 15 minutes. This script:

  --check    models status; if dead and Grok is idle, daemonize a waiter
  --force    start a waiter even if status looks ok
  --daemon   (internal) PTY-login, write 01_daily/_xai_reauth.json, Contents-API
             publish status=waiting + user_code, THEN wait ≤12 min for approval.
             Git push is not the publish path (ECS returns invalid credentials).

James auto-clicks Allow. Do not publish the user_code for Cyrus's phone.
Set XAI_REAUTH_SEND_CODE_TO_CYRUS=1 only to restore that backup.
"""
from __future__ import annotations

import argparse
import base64
import json
import os
import re
import signal
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")
ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "01_daily" / "_xai_reauth.json"
LOG = Path(os.environ.get("FULLSCAN_LOG", "/home/gha/fullscan-logs/xai_reauth.log"))
PID = Path(os.environ.get("FULLSCAN_REAUTH_PID", "/home/gha/fullscan-logs/xai_reauth.pid"))
DEVICE_URI = "https://accounts.x.ai/oauth2/device"
WAIT_S = 20 * 60  # < 30-min code life; expires_at below caps it
PUSH = ROOT / "scripts" / "safe_git_push.sh"
PUBLISH_TRIES = 5

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


def _github_token() -> str:
    return (os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN") or "").strip()


def _contents_api_url() -> str:
    repo = (os.environ.get("GITHUB_REPOSITORY") or "SRoyaltyy/fullscan").strip()
    return f"https://api.github.com/repos/{repo}/contents/01_daily/_xai_reauth.json"


def _api_headers(token: str) -> dict:
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "User-Agent": "fullscan-xai-reauth",
        "X-GitHub-Api-Version": "2022-11-28",
    }


def _purge_jsdelivr() -> None:
    try:
        urllib.request.urlopen(
            "https://purge.jsdelivr.net/gh/SRoyaltyy/fullscan@main/01_daily/_xai_reauth.json",
            timeout=8,
        ).read()
        print("[reauth] purged jsdelivr", flush=True)
    except (OSError, urllib.error.URLError) as e:
        print(f"[reauth] jsdelivr purge skipped: {e}", flush=True)


def _contents_sha(url: str, headers: dict) -> str | None:
    req = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        sha = data.get("sha")
        return str(sha) if sha else None
    except urllib.error.HTTPError as e:
        if e.code == 404:
            e.close()
            return None
        err = e.read().decode("utf-8", "replace")[:240]
        print(f"[reauth] contents GET HTTP {e.code}: {err}", flush=True)
        return None
    except (OSError, urllib.error.URLError, json.JSONDecodeError) as e:
        print(f"[reauth] contents GET failed: {e}", flush=True)
        return None


def _contents_put(url: str, headers: dict, payload: dict) -> int:
    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        method="PUT",
        headers={**headers, "Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            resp.read()
            print(f"[reauth] contents PUT {resp.status}", flush=True)
            return int(resp.status)
    except urllib.error.HTTPError as e:
        err = e.read().decode("utf-8", "replace")[:240]
        print(f"[reauth] contents PUT HTTP {e.code}: {err}", flush=True)
        return int(e.code)
    except (OSError, urllib.error.URLError) as e:
        print(f"[reauth] contents PUT failed: {e}", flush=True)
        return 0


def send_code_to_cyrus() -> bool:
    """The phone backup. Off. James auto-clicks Allow."""
    return os.environ.get("XAI_REAUTH_SEND_CODE_TO_CYRUS", "") == "1"


def publish_contents(message: str) -> bool:
    """PUT 01_daily/_xai_reauth.json via the GitHub Contents API.

    ECS git fetch/push returns invalid credentials, so this is the path that
    lands the file on main. The waiting user_code used to go out here so
    Cyrus's phone could tap Allow. That backup is off unless
    XAI_REAUTH_SEND_CODE_TO_CYRUS=1. James auto-clicks Allow.
    """
    try:
        preview = json.loads(OUT.read_text(encoding="utf-8")) if OUT.is_file() else {}
    except (OSError, json.JSONDecodeError):
        preview = {}
    token = _github_token()
    if not token:
        print("[reauth] contents publish skipped: no GITHUB_TOKEN", flush=True)
        return False
    if not OUT.is_file():
        print(f"[reauth] contents publish skipped: missing {OUT}", flush=True)
        return False
    try:
        body = OUT.read_bytes()
        preview = json.loads(body.decode("utf-8"))
    except (OSError, json.JSONDecodeError) as e:
        print(f"[reauth] contents publish skipped: {e}", flush=True)
        return False
    url = _contents_api_url()
    headers = _api_headers(token)
    sha = _contents_sha(url, headers)
    payload = {
        "message": message,
        "content": base64.b64encode(body).decode("ascii"),
        "branch": "main",
    }
    if sha:
        payload["sha"] = sha
    status = _contents_put(url, headers, payload)
    # 409/422 = main moved under us (stale sha). Re-read the sha and retry.
    tries = 1
    while status in (0, 409, 422, 500, 502, 503) and tries < PUBLISH_TRIES:
        time.sleep(min(2 * tries, 6))
        tries += 1
        sha = _contents_sha(url, headers)
        if sha:
            payload["sha"] = sha
        else:
            payload.pop("sha", None)
        print(f"[reauth] contents PUT retry {tries}/{PUBLISH_TRIES}", flush=True)
        status = _contents_put(url, headers, payload)
    if status not in (200, 201):
        print(
            f"[reauth] contents publish failed status={preview.get('status')} "
            f"code={preview.get('user_code')} http={status}",
            flush=True,
        )
        return False
    print(
        f"[reauth] contents publish ok status={preview.get('status')} "
        f"code={preview.get('user_code')}",
        flush=True,
    )
    _purge_jsdelivr()
    return True


def push(msg: str) -> None:
    """Land a terminal reauth JSON. Contents API first; git push is optional.

    The waiting user_code path must call publish_contents itself and must
    not call this before wait_pid. safe_git_push from ECS blocks on
    invalid credentials for up to 90s and still does not update main.
    """
    publish_contents(msg)
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
    _purge_jsdelivr()


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


def hostname() -> str:
    try:
        return socket.gethostname()
    except OSError:
        return ""


def waiting_live(existing: dict) -> bool:
    """A 'waiting' file counts only if its waiter is alive HERE and the code
    has not expired. GitHub reaps the in-process waiter when its job ends, so
    a leftover waiting file (2026-10-15 run 38821015203) must not block a
    fresh login. Missing waiter_pid/host/expires_at => not live.
    """
    if existing.get("status") != "waiting" or not existing.get("user_code"):
        return False
    exp = existing.get("expires_at")
    if not exp:
        return False
    try:
        when = datetime.fromisoformat(str(exp).replace("Z", "+00:00"))
    except ValueError:
        return False
    if when.tzinfo is None:
        when = when.replace(tzinfo=ET)
    if when <= datetime.now(ET):
        return False
    if not existing.get("host") or existing.get("host") != hostname():
        return False
    try:
        wpid = int(existing.get("waiter_pid") or 0)
    except (TypeError, ValueError):
        return False
    return pid_alive(wpid)


waiting_fresh = waiting_live  # old name


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
    # GitHub's job has no user bus, so `systemd-run --user` dies with
    # DBUS_SESSION_BUS_ADDRESS unset (2026-09-02 run 33597448920) and the
    # waiter is reaped when the job exits. Point at the linger bus when
    # it exists; the in-process fallback below still runs if it does not.
    uid = os.getuid()
    runtime = f"/run/user/{uid}"
    if os.path.isdir(runtime) and not env.get("XDG_RUNTIME_DIR"):
        env["XDG_RUNTIME_DIR"] = runtime
        env.setdefault("DBUS_SESSION_BUS_ADDRESS", f"unix:path={runtime}/bus")
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
    if not force and waiting_live(existing):
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
    expires = (datetime.now(ET) + timedelta(minutes=29)).isoformat()
    dump({
        "status": "waiting",
        "oauth": "FAIL",
        "pong_ok": None,
        "reason": "James auto-clicks Allow; not sending the code to Cyrus",
        "user_code": code,
        "verification_uri": complete_uri(code, uri),
        "expires_at": expires,
        "source": "xai_device_reauth",
        "pid": pid,
        "waiter_pid": os.getpid(),
        "host": hostname(),
    })
    print(f"[reauth] device url for James: {complete_uri(code, uri)}", flush=True)
    # Land the code on main BEFORE wait_pid so James's auto-click can see it
    # (#595). #604 gated this behind XAI_REAUTH_SEND_CODE_TO_CYRUS, which
    # left main showing a stale code for the whole 12-minute wait.
    if not publish_contents(f"chore: xAI device code {code}"):
        print("[reauth] WARN: waiting code not on main", flush=True)
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
    if waiting_live(existing):
        print("[reauth] already waiting (live waiter) — not starting another", flush=True)
        if force:
            return spawn_daemon(True)
        return 0
    if existing.get("status") == "waiting":
        print("[reauth] stale waiting file (waiter dead/expired/other host) — starting fresh",
              flush=True)
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
            "reason": "Grok job running — not starting device login. James auto-clicks Allow when the run is free.",
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
