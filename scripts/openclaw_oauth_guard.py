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
from datetime import datetime, timedelta, timezone
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
MIN_LEFT_MIN = int(os.environ.get("OPENCLAW_MIN_LEFT_MIN", "0"))
# A token with hours left can still die before 05:55 ET. The 00:41 ET run on
# 2026-10-15 skipped (267 min left) and the expiry was 05:08 ET.
MORNING_COVER_ET = (6, 30)
MORNING_COVER_HORIZON = timedelta(hours=8)
CALL_TIMEOUT = int(os.environ.get("OPENCLAW_GUARD_CALL_TIMEOUT", "90"))
MODEL = os.environ.get("OPENCLAW_BACKEND_MODEL", "xai/grok-4.3")
PAID_ENV = ("XAI_API_KEY", "OPENCLAW_XAI_API_KEY", "GROK_API_KEY")


def morning_cover_deadline(now: datetime) -> datetime | None:
    """Next 06:30 ET, when that morning is close enough that a renew helps.

    A 6h token refreshed at 21:10 ET cannot reach the next 06:30, so that
    slot keeps the plain 90-minute window. The 00:10 ET slot must renew a
    token that would die at 05:08, before Pre-Open.
    """
    et_now = now.astimezone(ET)
    hour, minute = MORNING_COVER_ET
    target = et_now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if et_now >= target:
        target = target + timedelta(days=1)
    if target - et_now > MORNING_COVER_HORIZON:
        return None
    return target.astimezone(timezone.utc)


def token_is_fresh(left_min: float | None, exp: datetime | None,
                   now: datetime | None = None) -> bool:
    """True only when the token outlasts the 90-minute window and 06:30 ET."""
    if exp is None or left_min is None:
        return False
    if left_min <= RENEW_WINDOW_MIN:
        return False
    now = now or datetime.now(timezone.utc)
    if exp.tzinfo is None:
        exp = exp.replace(tzinfo=timezone.utc)
    # Pre-Open sets this to the run length. A token that dies before the
    # run ends is not fresh, even when it still clears the 90-minute rule.
    # The 04:40 guard is a second job on the only ECS runner, so it queues
    # behind Pre-Open and never renews (2026-10-15, run 38704557910).
    raw_cover = (os.environ.get("OPENCLAW_RUN_COVER_MIN") or "").strip()
    if raw_cover:
        try:
            run_end = now + timedelta(minutes=float(raw_cover))
        except ValueError:
            run_end = None
        if run_end is not None and exp <= run_end:
            return False
    cover = morning_cover_deadline(now)
    if cover is not None and exp <= cover:
        return False
    return True


def renew_verdict(exp_before: datetime | None, exp_after: datetime | None,
                  now: datetime) -> str:
    """OK, OK: still valid, EXPIRED, or NEEDS_FORCE.

    A 17+25 that returns 42 without moving the expiry is not a renew.
    OpenClaw only refreshes a profile that is already expired, so a token
    that is still valid and dies before 06:30 ET has to be nudged.
    """
    left = None
    if exp_after is not None:
        left = (exp_after - now).total_seconds() / 60
    if token_is_fresh(left, exp_after, now):
        if exp_before and exp_after and exp_after > exp_before:
            return "OK"
        return "OK: still valid"
    if left is None or left <= MIN_LEFT_MIN:
        return "EXPIRED"
    return "NEEDS_FORCE"


_EXPIRE_KEYS = ("expires", "expiresat", "expires_at", "expiry")


def _nudge_expiry(node, target: datetime, past: str) -> bool:
    """Set an expires field that matches `target` to the past. No other fields."""
    changed = False
    if isinstance(node, dict):
        for key, val in list(node.items()):
            if key.lower() in _EXPIRE_KEYS:
                dt = _to_dt(val)
                if dt is not None and abs((dt - target).total_seconds()) < 2:
                    node[key] = past if isinstance(val, str) else 0
                    changed = True
            elif isinstance(val, (dict, list)):
                changed = _nudge_expiry(val, target, past) or changed
    elif isinstance(node, list):
        for val in node:
            if isinstance(val, (dict, list)):
                changed = _nudge_expiry(val, target, past) or changed
    return changed


def nudge_auth_expiry(exp: datetime, root: Path | None = None) -> list[tuple[Path, str]]:
    """Make the still-valid xAI profile look expired so the next call refreshes.

    Returns (path, original text) so a failed refresh can be undone.
    Does not log file contents. Skips openclaw.json (the gateway config).
    """
    root = root or (Path(HOME) / ".openclaw")
    if not root.is_dir():
        return []
    past = "2000-01-01T00:00:00.000Z"
    backups: list[tuple[Path, str]] = []
    for path in root.rglob("*.json"):
        if path.name == "openclaw.json" or ".bak" in path.name:
            continue
        try:
            if path.stat().st_size > 2_000_000:
                continue
            original = path.read_text(encoding="utf-8")
            data = json.loads(original)
        except (OSError, json.JSONDecodeError):
            continue
        if not _nudge_expiry(data, exp, past):
            continue
        path.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")
        backups.append((path, original))
        log(f"nudged expiry in {path.name} so the refresh token runs")
    return backups


def restore_auth(backups: list[tuple[Path, str]]) -> None:
    for path, original in backups:
        try:
            path.write_text(original, encoding="utf-8")
        except OSError as exc:
            log(f"could not restore {path.name}: {exc}")


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


def _is_xai_oauth_dict(d: dict) -> bool:
    """True when this node is (or clearly describes) an xAI OAuth profile."""
    if not isinstance(d, dict):
        return False
    provider = str(d.get("provider") or d.get("Provider") or "").lower()
    ptype = str(d.get("type") or d.get("method") or d.get("mode") or "").lower()
    pid = str(d.get("id") or d.get("profileId") or d.get("profile_id") or "").lower()
    if provider == "xai" and ptype in ("oauth", "device_code", "device-code"):
        return True
    if provider == "xai" and ptype and ptype != "api_key":
        # auth list JSON uses type=oauth; tolerate token-shaped xAI profiles too
        if any(k in d for k in ("expires", "expiresAt", "expires_at", "expiry", "refresh")):
            return True
    if pid.startswith("xai:") and ("oauth" in pid or ptype == "oauth"):
        return True
    return False


def _expiry_from_dict(d: dict) -> tuple[datetime | None, bool | None]:
    dt = None
    for k in ("expires", "expiresAt", "expires_at", "expiry"):
        dt = _to_dt(d.get(k))
        if dt:
            break
    has_r = None
    for rk in ("hasRefresh", "hasRefreshToken", "refreshable", "refresh"):
        if rk in d:
            has_r = bool(d[rk]) if rk != "refresh" else bool(d.get("refresh"))
            break
    return dt, has_r


def _parse_auth_list_json(data) -> tuple[datetime | None, bool | None]:
    """Parse `openclaw models auth list --json` (profiles[] summarized, no tokens)."""
    if not isinstance(data, dict):
        return None, None
    profiles = data.get("profiles")
    if isinstance(profiles, list):
        for p in profiles:
            if isinstance(p, dict) and _is_xai_oauth_dict(p):
                dt, has_r = _expiry_from_dict(p)
                if dt:
                    return dt, has_r
    if isinstance(profiles, dict):
        for pid, cred in profiles.items():
            if not isinstance(cred, dict):
                continue
            node = dict(cred)
            node.setdefault("id", pid)
            if _is_xai_oauth_dict(node):
                dt, has_r = _expiry_from_dict(node)
                if dt:
                    return dt, has_r
    return None, None


def _xai_oauth_expiry_from_files(root: Path | None = None) -> tuple[datetime | None, str, bool | None]:
    """Read ~/.openclaw auth-profiles.json directly (works even when CLI runs as gha
    but the file was written as root after a device login). No token values logged.
    """
    root = root or (Path(HOME) / ".openclaw")
    if not root.is_dir():
        return None, "unreadable", None
    candidates: list[Path] = []
    for name in ("auth-profiles.json", "oauth.json", "auth.json"):
        candidates.extend(root.rglob(name))
    # Prefer agent auth-profiles.json over other hits
    candidates.sort(key=lambda p: (0 if p.name == "auth-profiles.json" else 1,
                                   0 if "/agents/" in str(p) else 1,
                                   str(p)))
    for path in candidates:
        try:
            if path.stat().st_size > 2_000_000:
                continue
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        # Store shapes: {profiles: {id: cred}} or list
        profiles = data.get("profiles") if isinstance(data, dict) else None
        nodes: list[dict] = []
        if isinstance(profiles, dict):
            for pid, cred in profiles.items():
                if isinstance(cred, dict):
                    node = dict(cred)
                    node.setdefault("id", pid)
                    nodes.append(node)
        elif isinstance(profiles, list):
            nodes.extend(p for p in profiles if isinstance(p, dict))
        elif isinstance(data, dict) and _is_xai_oauth_dict(data):
            nodes.append(data)
        for node in nodes:
            if not _is_xai_oauth_dict(node):
                continue
            dt, has_r = _expiry_from_dict(node)
            if dt:
                return dt, f"file:{path.name}", has_r
    return None, "unreadable", None


def ensure_auth_store_owned_by_gha(root: Path | None = None) -> list[str]:
    """Device login often runs as root on the ECS GH runner with HOME=/home/gha.
    That leaves auth-profiles.json root-owned; `sudo -u gha openclaw ...` then
    cannot read the profile (expiry_source=unreadable) even though the gateway
    already got the token via refreshRunningGatewayAuthState. Fix ownership.
    """
    if os.geteuid() != 0:
        return []
    root = root or (Path(HOME) / ".openclaw")
    if not root.is_dir():
        return []
    try:
        import pwd
        pw = pwd.getpwnam(GHA)
        uid, gid = pw.pw_uid, pw.pw_gid
    except KeyError:
        return []
    fixed: list[str] = []
    names = {"auth-profiles.json", "oauth.json", "auth.json", "auth-state.json",
             "openclaw-agent.sqlite"}
    for path in root.rglob("*"):
        if not path.is_file():
            continue
        if path.name not in names and path.suffix not in (".json", ".sqlite"):
            continue
        if path.name == "openclaw.json":
            continue
        try:
            st = path.stat()
            if st.st_uid == uid and st.st_gid == gid:
                continue
            os.chown(path, uid, gid)
            # gha must read+write for renew nudge / CLI list
            mode = st.st_mode & 0o777
            if mode & 0o600 != 0o600:
                path.chmod(mode | 0o600)
            fixed.append(path.name)
        except OSError:
            continue
    return fixed


def xai_oauth_expiry() -> tuple[datetime | None, str, bool | None]:
    """Return (expires_utc, source, has_refresh) for the xAI OAuth profile."""
    # Root-owned auth files after a forced reauth: fix before CLI as gha.
    fixed = ensure_auth_store_owned_by_gha()
    if fixed:
        log(f"chowned auth store files to {GHA}: {', '.join(sorted(set(fixed))[:8])}")

    for args in (["openclaw", "models", "auth", "list", "--provider", "xai", "--json"],
                 ["openclaw", "models", "status", "--json"]):
        r = as_gha(args, 60)
        raw = (r.stdout or "").strip()
        # Strip a leading banner / spinner junk before the JSON object/array.
        for start in ("{", "["):
            i = raw.find(start)
            if i > 0:
                raw = raw[i:]
                break
        try:
            data = json.loads(raw)
        except Exception:
            continue
        dt, has_r = _parse_auth_list_json(data)
        if dt:
            return dt, " ".join(args[1:4]), has_r
        # Walk nested dicts without the old 400-char dump (long JWTs hid "xai"/"oauth").
        for d in _walk(data):
            if not _is_xai_oauth_dict(d):
                continue
            dt, has_r = _expiry_from_dict(d)
            if dt:
                return dt, " ".join(args[1:4]), has_r
    # Text fallback: "xai:... [xai/oauth; expires 2026-05-18T22:46:23.273Z]"
    r = as_gha(["openclaw", "models", "status"], 60)
    m = re.search(r"xai[^\n]*oauth[^\n]*expires\s+([0-9T:\-.]+Z?)", r.stdout or "", re.I)
    if m:
        return _to_dt(m.group(1)), "models status (text)", None
    # File fallback: same store the CLI should read, but as this process (often root).
    dt, src, has_r = _xai_oauth_expiry_from_files()
    if dt:
        return dt, src, has_r
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
    if token_is_fresh(left, exp, now):
        result["renew"] = "SKIP: token fresh"
        return True
    if left is not None and left > RENEW_WINDOW_MIN:
        log("token outlasts the 90-minute window but dies before 06:30 ET "
            "— renewing so 05:55 Pre-Open is covered")
    # OpenClaw refreshes lazily: an expired profile is refreshed by the
    # runtime on the next xAI call and written back to its store. To renew
    # ahead of the window we wait out nothing - we make the call now; if the
    # token is still valid but close, the post-check below decides.
    # Proven on ECS 2026-10-12 (OpenClaw 2026.7.1-2): one real completion
    # through the gateway with an expired profile refreshed it unattended
    # (new expiry = call time + 6h). `openclaw infer --local` returned rc=1
    # and did not refresh, so we renew through the gateway first and only
    # fall back to the CLI.
    gw_ok = real_completion(result, "renew")
    out = ""
    if not gw_ok:
        call = as_gha(["openclaw", "infer", "model", "run", "--local", "--model", MODEL,
                       "Reply with only the number: 17+25"], 180)
        out = (call.stdout or "") + (call.stderr or "")
        result["renew_call_rc"] = call.returncode
        tail = re.sub(r"\s+", " ", out)[-400:]
        result["renew_call_tail"] = re.sub(r"(eyJ[\w.-]{10,}|[A-Za-z0-9_\-]{40,})", "<redacted>", tail)
    exp2, _, _ = xai_oauth_expiry()
    left2 = (exp2 - datetime.now(timezone.utc)).total_seconds() / 60 if exp2 else None
    result["expires_after"] = exp2.isoformat() if exp2 else None
    result["minutes_left_after"] = round(left2, 1) if left2 is not None else None
    log(f"after renew call gw_ok={gw_ok} expires={result['expires_after']} left_min={result['minutes_left_after']}")
    if re.search(r"cloudflare|captcha|invalid_grant|re-?auth|refresh.*fail", out, re.I):
        result["renew"] = "FAIL: refresh rejected - interactive device login needed"
        result["renew_error_hint"] = re.sub(r"\s+", " ", out)[-240:]
        return False
    now2 = datetime.now(timezone.utc)
    verdict = renew_verdict(exp, exp2, now2)
    if verdict in ("OK", "OK: still valid"):
        result["renew"] = verdict
        return True
    if verdict == "EXPIRED":
        # Still expired after a real call = refresh failed -> device login.
        result["renew"] = "FAIL: token still expired after a real call - interactive device login needed"
        return False
    # Still valid, and the 42 did not move the expiry past 06:30 ET.
    # OpenClaw refreshes only an expired profile. Nudge that field, call
    # once more, and put the file back if the expiry still does not move.
    if grok_job_running():
        result["renew"] = ("FAIL: token dies before 06:30 ET and a Grok job is "
                           "running — profile not touched")
        return False
    backups = nudge_auth_expiry(exp2 or exp)
    if not backups:
        result["renew"] = ("FAIL: token dies before 06:30 ET and no auth profile "
                           "expiry could be nudged — device login needed")
        return False
    real_completion(result, "force_refresh")
    exp3, _, _ = xai_oauth_expiry()
    now3 = datetime.now(timezone.utc)
    left3 = (exp3 - now3).total_seconds() / 60 if exp3 else None
    result["expires_after"] = exp3.isoformat() if exp3 else None
    result["minutes_left_after"] = round(left3, 1) if left3 is not None else None
    log(f"after forced refresh expires={result['expires_after']} left_min={result['minutes_left_after']}")
    if renew_verdict(exp, exp3, now3) in ("OK", "OK: still valid"):
        result["renew"] = "OK"
        return True
    restore_auth(backups)
    result["renew"] = ("FAIL: refresh did not extend expiry past 06:30 ET "
                       "— interactive device login needed")
    return False


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


def _process_ancestors() -> set[int]:
    """This process and its parents. ecs_preopen.sh is the parent when it renews."""
    pids = {os.getpid()}
    pid = os.getpid()
    for _ in range(8):
        try:
            stat = Path(f"/proc/{pid}/status").read_text(encoding="utf-8")
        except OSError:
            break
        match = re.search(r"^PPid:\s+(\d+)", stat, re.M)
        if not match:
            break
        pid = int(match.group(1))
        if pid <= 1 or pid in pids:
            break
        pids.add(pid)
    return pids


def _other_grok_job(line: str, ancestors: set[int]) -> bool:
    if "pgrep" in line or "oauth_guard" in line:
        return False
    match = re.match(r"\s*(\d+)", line)
    if match and int(match.group(1)) in ancestors:
        return False
    return True


def grok_job_running() -> bool:
    r = subprocess.run(["pgrep", "-af", GROK_JOBS], capture_output=True, text=True)
    ancestors = _process_ancestors()
    lines = [l for l in r.stdout.splitlines() if _other_grok_job(l, ancestors)]
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
