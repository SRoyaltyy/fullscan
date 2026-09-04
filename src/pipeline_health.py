"""Diagnose + heal pre-open / post-close packets until required FAILs are gone.

Two daily jobs (fix ON by default):
  --job preopen    07:00 ET  door, GH scrape, last-night baseline, morning essays
  --job postclose  00:30 ET  door, captain research, previous session book/outcomes
  --job auto       pick from the ET clock (used by the workflow cron)

Loop: audit → fix the OpenClaw door / timers on this box → start the owning
job (systemd or detached python on ECS; GitHub dispatch only for ubuntu
Finviz/weather/AB/Pages) → wait for the missing files → re-audit.

Does NOT scrape Finviz HTML on ECS (Aliyun 403). Does NOT PONG OpenClaw
while a Grok job is already running. OAuth "expiring" is a warning, not a
stop. Missing files still start their owning job. Permanent xAI auth is an
API key in ~/.openclaw/.env (OAuth access tokens die ~6h). The phone Claw
tab + xai_reauth.yml publish a device code to 01_daily/_xai_reauth.json.

Trash that counts as FAIL:
  missing / empty / garbled JSON
  timeout / connection-refused stubs
  carry-forward / pass-over from another date
  explicit skip (morning_bootstrap, maps not scraped)
  DeepSeek/SearXNG fallback when the file claims Grok
"""
from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from . import config, output_qc
from .sector_taxonomy import FINVIZ_SECTORS

ROOT = Path(__file__).resolve().parent.parent
ET = ZoneInfo(config.TZ)
PERSIST = Path(os.environ.get("FULLSCAN_PERSIST", "/home/gha/fullscan-persist"))
GW = (os.environ.get("OPENCLAW_GATEWAY_URL") or "http://127.0.0.1:18789").rstrip("/")
JSON_CFGS = (
    Path(os.path.expanduser("~/.openclaw/openclaw.json")),
    Path("/home/gha/.openclaw/openclaw.json"),
)
LIVE_TOKEN_LEN = 48
STALE_SECRET_LEN = 64
ENABLE_CHAT = ROOT / "scripts" / "enable_openclaw_chat.sh"
FIX_STATE = ROOT / "data" / "health" / "fix_state.json"
PAGES_URL = "https://sroyaltyy.github.io/fullscan/dashboard/"
HEAL_LOG = Path("/home/gha/fullscan-logs")
REAUTH_JSON = ROOT / "01_daily" / "_xai_reauth.json"
DEVICE_URI = "https://accounts.x.ai/oauth2/device"
XAI_REAUTH = ROOT / "scripts" / "xai_device_reauth.py"

# (workflow, title, required_for_jobs, date_role)
# date_role: preopen | postclose | book
WORKFLOWS = [
    ("finviz_preopen_scrape.yml", "Finviz pre-open scrape (GH-hosted Elite)",
     {"preopen"}, "preopen"),
    ("map_heat_postclose.yml", "Map heat captain research (post-close)",
     {"preopen", "postclose"}, "postclose"),
    ("postclose_all.yml", "Post-Close ALL",
     {"postclose"}, "postclose"),
    ("preopen_all.yml", "Pre-Open ALL",
     {"preopen"}, "preopen"),
    ("collect-news-rss.yml", "Collect: RSS News", set(), "preopen"),
    ("collect-news-newsapi.yml", "Collect: NewsAPI", set(), "preopen"),
    ("collect-news-reddit.yml", "Collect: Reddit", set(), "preopen"),
    ("collect-market-yfinance.yml", "Collect: yfinance Market Data", set(), "preopen"),
    ("collect-macro-fred.yml", "Collect: FRED Macro", set(), "preopen"),
    ("collect-catalyst.yml", "Collect: Catalyst", set(), "preopen"),
    ("insider_fetch.yml", "Insider Fetch", set(), "preopen"),
    ("events_daily.yml", "Event Scanner (fallback yml)", set(), "preopen"),
    ("news_judge.yml", "News judge (fallback yml)", set(), "preopen"),
    ("label_weather.yml", "Label + Weather", {"postclose"}, "book"),
    ("ab_checklist.yml", "A+B1 Checklist", {"postclose"}, "book"),
    ("catalyst_daily.yml", "Catalyst daily (midday extra)", set(), "preopen"),
    ("daily_pipeline.yml", "Daily pipeline outcome+reflect", {"postclose"}, "book"),
    ("sector_daily.yml", "Sector Daily outcome+reflect", {"postclose"}, "book"),
    ("learn_cycle.yml", "Learn Cycle", {"postclose"}, "book"),
    ("news_grade.yml", "News Actions Grader", set(), "book"),
    ("hit_board.yml", "HIT Board", set(), "book"),
    ("stock_book_all.yml", "Stock Book ALL", {"postclose"}, "book"),
    ("excel_bot.yml", "Excel Bot (cluster signals)", set(), "preopen"),
    ("deploy-dashboard.yml", "Deploy dashboard to Pages", set(), "book"),
]

# More-specific prefixes first.
FIX_MAP = [
    ("runtime.", "door"),
    ("postclose.", "postclose_all.yml"),
    ("scrape.", "finviz_preopen_scrape.yml"),
    ("preopen.", "preopen_all.yml"),
    ("book.weather", "stock_book_all.yml"),
    ("book.membership", "stock_book_all.yml"),
    ("book.join", "stock_book_all.yml"),
    ("book.finviz", "stock_book_all.yml"),
    ("book.ab", "stock_book_all.yml"),
    ("book.peers", "stock_book_all.yml"),
    ("book.", "stock_book_all.yml"),
    ("outcome.sector", "postclose_all.yml"),
    ("outcome.", "postclose_all.yml"),
    ("learn.", "postclose_all.yml"),
    ("pages.", "deploy-dashboard.yml"),
]

# ubuntu-latest — safe to GH-dispatch while this health job holds the ECS runner.
# stock_book_all heals as skip-llm / skip-extras on ubuntu so a hung Grok
# pre-open cannot queue the ranker behind itself.
UBUNTU_WORKFLOWS = {
    "finviz_preopen_scrape.yml",
    "label_weather.yml",
    "ab_checklist.yml",
    "deploy-dashboard.yml",
    "hit_board.yml",
    "news_grade.yml",
    "ab_enrich.yml",
    "stock_book_all.yml",
}

# ECS Grok jobs — never GH-dispatch from health; never start if OAuth is dead.
GROK_WORKFLOWS = {
    "preopen_all.yml",
    "postclose_all.yml",
    "map_heat_postclose.yml",
    "daily_pipeline.yml",
    "sector_daily.yml",
    "learn_cycle.yml",
    "catalyst_daily.yml",
}

ECS_UNITS = {
    "preopen_all.yml": "fullscan-preopen.service",
    "postclose_all.yml": "fullscan-map-postclose.service",
    "map_heat_postclose.yml": "fullscan-map-postclose.service",
    "door": "fullscan-openclaw-gateway.service",
}

ECS_SCRIPTS = {
    "preopen_all.yml": ROOT / "scripts" / "ecs_preopen.sh",
    "postclose_all.yml": ROOT / "scripts" / "ecs_map_postclose.sh",
    "map_heat_postclose.yml": ROOT / "scripts" / "ecs_map_postclose.sh",
}

GROK_NEEDLES = (
    "ecs_preopen.sh",
    "ecs_map_postclose.sh",
    "src.run_preopen_all",
    "src.run_postclose_all",
    "src.map_heat_postclose",
    "src.run_stock_book_all",
    "src.run_outcome",
    "src.run_sector_outcome",
    "src.learn_cycle",
    "src.catalyst_daily",
)

HUMAN_STEPS = {"runtime.oauth"}
OPENCLAW_ENV = Path("/home/gha/.openclaw/.env")


@dataclass
class Check:
    step: str
    name: str
    group: str
    status: str
    required: bool
    detail: str = ""
    path: str = ""


@dataclass
class Report:
    job: str
    date: str
    source_date: str
    target_date: str
    book_date: str = ""
    generated_at: str = ""
    checks: list[Check] = field(default_factory=list)
    fix_actions: list[str] | None = None
    round: int = 0

    @property
    def n_fail(self) -> int:
        return sum(1 for c in self.checks if c.status == "FAIL" and c.required)

    @property
    def n_warn(self) -> int:
        return sum(1 for c in self.checks if c.status == "WARN")

    @property
    def ok(self) -> bool:
        return self.n_fail == 0


def _today(now: datetime | None = None) -> str:
    return (now or datetime.now(ET)).date().isoformat()


def _prev_weekday(date: str) -> str:
    d = datetime.fromisoformat(date).date() - timedelta(days=1)
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d.isoformat()


def _next_weekday(date: str) -> str:
    d = datetime.fromisoformat(date).date() + timedelta(days=1)
    while d.weekday() >= 5:
        d += timedelta(days=1)
    return d.isoformat()


def packet_dates(
    job: str,
    now: datetime | None = None,
    date: str | None = None,
    source: str | None = None,
    target: str | None = None,
) -> tuple[str, str, str, str]:
    """Return (session, source, target, book).

    preopen 07:00 D:          scrape/essays D, baseline dated D (night of D-1)
    postclose 00:30 D:        research dated D, book D-1
    postclose after 22:00 D:  research dated next weekday, book D
    afternoon D (13:00-22:00): last-night research dated D, book D
    """
    now = now or datetime.now(ET)
    session = date or now.date().isoformat()
    hm = now.hour * 100 + now.minute
    prev = _prev_weekday(session)
    nxt = _next_weekday(session)
    if job == "preopen":
        src, tgt, book = prev, session, session
    elif job in ("postclose", "afternoon", "all"):
        if hm >= 2200:
            src, tgt, book = session, nxt, session
        elif hm < 500:
            src, tgt, book = prev, session, prev
        else:
            src, tgt, book = prev, session, session
    else:
        src, tgt, book = prev, session, session
    if source:
        src = source
        if job in ("postclose", "afternoon") and not date:
            book = source
    if target:
        tgt = target
    if job == "preopen":
        book = session
    return session, src, tgt, book


def pick_job(job: str, now: datetime | None = None) -> str:
    if job and job not in ("auto", ""):
        return job
    now = now or datetime.now(ET)
    hm = now.hour * 100 + now.minute
    if 500 <= hm < 1200:
        return "preopen"
    return "postclose"


def _add(report: Report, **kw) -> Check:
    c = Check(**kw)
    report.checks.append(c)
    flag = {"OK": "OK  ", "FAIL": "FAIL", "WARN": "WARN"}[c.status]
    opt = "" if c.required else " (optional)"
    path = f"  {c.path}" if c.path else ""
    print(f"  [{flag}] {c.name:<48}{opt} {c.detail}{path}", flush=True)
    return c


def _read(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except OSError:
        return ""


def _json(path: Path):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, ValueError):
        return None


def _tail(token: str) -> str:
    t = str(token or "")
    if len(t) < 4:
        return "empty" if not t else "short"
    return t[-4:]


def _live_token() -> tuple[str, int]:
    for p in JSON_CFGS:
        data = _json(p)
        if not isinstance(data, dict):
            continue
        gw = data.get("gateway") or {}
        auth = gw.get("auth") if isinstance(gw.get("auth"), dict) else {}
        token = str(auth.get("token") or gw.get("token") or auth.get("password") or "")
        if token:
            return token, len(token)
    return "", 0


def restore_persist(*dates: str) -> None:
    if not PERSIST.is_dir():
        return
    n = 0
    for date in dates:
        if not date:
            continue
        for folder in (
            PERSIST / "01_daily",
            PERSIST / "01_daily" / "general",
            PERSIST / "01_daily" / "events",
            PERSIST / "01_daily" / "news",
            PERSIST / "01_daily" / "map_heat",
            PERSIST / "01_daily" / "catalyst",
            PERSIST / "01_daily" / "weather",
            PERSIST / "01_daily" / "_transcripts",
            PERSIST / "data" / "catalyst",
            PERSIST / "data" / "join",
            PERSIST / "data" / "universe",
            PERSIST / "data" / "ab_checklist",
            PERSIST / "data" / "peers",
            PERSIST / "data" / "stock_book",
            PERSIST / "data" / "exports",
        ):
            if not folder.is_dir():
                continue
            for src in list(folder.glob(f"{date}*")) + list(folder.glob(f"{date}_*")):
                rel = src.relative_to(PERSIST)
                dest = ROOT / rel
                dest.parent.mkdir(parents=True, exist_ok=True)
                try:
                    if src.is_dir():
                        shutil.copytree(src, dest, dirs_exist_ok=True)
                    else:
                        shutil.copy2(src, dest)
                    n += 1
                except OSError:
                    pass
        sec = PERSIST / "01_daily" / "sectors" / date
        if sec.is_dir():
            dest = ROOT / "01_daily" / "sectors" / date
            try:
                shutil.copytree(sec, dest, dirs_exist_ok=True)
                n += 1
            except OSError:
                pass
    if n:
        print(f"[health] persist restore: {n} paths", flush=True)


def _payload_dates(data: dict) -> list[tuple[str, str]]:
    out = []
    for k in ("date", "scan_date", "asof", "source_heat_date", "source_date"):
        v = data.get(k)
        if v:
            out.append((k, str(v)[:10]))
    return out


def _stale_note(data: dict, expected: str) -> str:
    bad = []
    for k, v in _payload_dates(data):
        if v and v != expected and re.match(r"\d{4}-\d{2}-\d{2}$", v):
            bad.append(f"{k}={v}")
    if not bad:
        return ""
    return "PASS-OVER from " + ", ".join(bad) + f" (want {expected})"


def _timeoutish(text: str) -> bool:
    return bool(text) and output_qc.looks_like_timeout(text)


def _deepseekish(text: str) -> bool:
    t = (text or "").lower()
    return "deepseek" in t and ("fallback" in t or "model" in t)


def artifact(report: Report, *, step: str, name: str, group: str, path: Path,
             required: bool, expected_date: str, qc=None) -> None:
    if not path.exists():
        _add(report, step=step, name=name, group=group,
             status="FAIL" if required else "WARN", required=required,
             detail="DID NOT RUN — file missing", path=str(path))
        return
    size = path.stat().st_size if path.is_file() else 0
    if path.is_file() and size < 8:
        _add(report, step=step, name=name, group=group,
             status="FAIL" if required else "WARN", required=required,
             detail=f"EMPTY ({size} bytes)", path=str(path))
        return
    text = _read(path) if path.is_file() else ""
    if _timeoutish(text):
        _add(report, step=step, name=name, group=group, status="FAIL",
             required=required, detail="TIMEOUT/STUB (garbled OpenClaw reply)",
             path=str(path))
        return
    data = None
    if path.suffix == ".json":
        data = _json(path)
        if data is None:
            _add(report, step=step, name=name, group=group, status="FAIL",
                 required=required, detail="GARBLED JSON", path=str(path))
            return
        stale = _stale_note(data, expected_date)
        if stale:
            _add(report, step=step, name=name, group=group, status="FAIL",
                 required=required, detail=stale, path=str(path))
            return
        if data.get("carried_from") or data.get("phase") == "carried":
            _add(report, step=step, name=name, group=group, status="FAIL",
                 required=required,
                 detail=f"CARRY-FORWARD carried_from={data.get('carried_from')}",
                 path=str(path))
            return
        if data.get("phase") == "morning_bootstrap":
            _add(report, step=step, name=name, group=group, status="FAIL",
                 required=required,
                 detail="SKIPPED — morning_bootstrap (post-close baseline never ran)",
                 path=str(path))
            return
    if "CARRIED FORWARD" in text or "carried_from" in text:
        _add(report, step=step, name=name, group=group, status="FAIL",
             required=required, detail="CARRY-FORWARD in body", path=str(path))
        return
    if qc is not None:
        r = qc(path)
        if not r.ok:
            why = r.reason or "qc_fail"
            if r.carried:
                why = f"CARRY-FORWARD ({why})"
            if r.timeout:
                why = f"TIMEOUT/STUB ({why})"
            if r.empty:
                why = f"EMPTY ({why})"
            _add(report, step=step, name=name, group=group,
                 status="FAIL" if required else "WARN", required=required,
                 detail=why, path=str(path))
            return
    extra = f"{size} bytes"
    if isinstance(data, dict) and data.get("phase"):
        extra = f"phase={data.get('phase')} {extra}"
    _add(report, step=step, name=name, group=group, status="OK",
         required=required, detail=extra, path=str(path))


def _port_open() -> bool:
    try:
        with socket.create_connection(("127.0.0.1", 18789), timeout=2):
            return True
    except OSError:
        return False


def _chat_ping(token: str) -> dict:
    body = json.dumps({
        "model": os.environ.get("OPENCLAW_AGENT") or "openclaw/default",
        "messages": [{"role": "user", "content": "Reply with exactly the word PONG"}],
        "max_tokens": 16, "temperature": 0,
    }).encode()
    req = urllib.request.Request(
        f"{GW}/v1/chat/completions", data=body, method="POST",
        headers={"Authorization": f"Bearer {token}", "x-api-key": token,
                 "Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=45) as resp:
            raw, code = resp.read().decode("utf-8", "replace"), resp.status
    except urllib.error.HTTPError as e:
        raw, code = (e.read().decode("utf-8", "replace") if e.fp else ""), e.code
    except (urllib.error.URLError, TimeoutError, OSError) as e:
        return {"http": 0, "error": str(e)[:160], "content": "", "model": ""}
    try:
        d = json.loads(raw)
    except json.JSONDecodeError:
        return {"http": code, "error": raw[:160], "content": "", "model": ""}
    content = str(((d.get("choices") or [{}])[0].get("message") or {}).get("content") or "")
    return {"http": code, "content": content.strip(), "model": str(d.get("model") or ""),
            "error": str(d.get("error") or "")[:160]}


def _systemctl(*args: str) -> str:
    try:
        r = subprocess.run(["systemctl", *args], capture_output=True,
                           text=True, timeout=20)
        return (r.stdout or r.stderr or "").strip()
    except (OSError, subprocess.SubprocessError):
        return ""


def _pgrep(*needles: str) -> bool:
    try:
        r = subprocess.run(["pgrep", "-af", "."], capture_output=True,
                           text=True, timeout=8)
    except (OSError, subprocess.SubprocessError):
        return False
    blob = r.stdout or ""
    return any(n in blob for n in needles)


def _unit_active(unit: str) -> bool:
    return _systemctl("is-active", unit) == "active"


def grok_busy() -> bool:
    if _unit_active("fullscan-preopen.service"):
        return True
    if _unit_active("fullscan-map-postclose.service"):
        return True
    return _pgrep(*GROK_NEEDLES)


def _models_status() -> str:
    try:
        r = subprocess.run(
            ["openclaw", "models", "status"], capture_output=True,
            text=True, timeout=20,
            env={**os.environ, "HOME": os.environ.get("HOME") or "/home/gha"})
        return (r.stdout or "") + "\n" + (r.stderr or "")
    except (OSError, subprocess.SubprocessError) as e:
        return str(e)


def _models_check() -> tuple[int | None, str]:
    """openclaw models status --check: 0 ok, 1 expired/missing, 2 expiring."""
    try:
        r = subprocess.run(
            ["openclaw", "models", "status", "--check"],
            capture_output=True, text=True, timeout=20,
            env={**os.environ, "HOME": os.environ.get("HOME") or "/home/gha"})
        blob = ((r.stdout or "") + "\n" + (r.stderr or "")).strip()
        return r.returncode, blob
    except (OSError, subprocess.SubprocessError) as e:
        return None, str(e)


def oauth_verdict(check_code: int | None, pong_ok: bool) -> tuple[str, bool, str]:
    """Return (status, required, reason). Never treat 'expiring' as dead.

    code 0 = ok, 2 = expiring (still usable), 1 = expired/missing.
    If the classroom still PONGs, we do not block Grok jobs.
    """
    if check_code == 0:
        return "OK", True, "models status --check ok"
    if check_code == 2:
        return "WARN", False, "xAI token expiring but still usable"
    if pong_ok:
        return "WARN", False, (
            "status says expired/unknown but OpenClaw PONG works — "
            "starting Grok jobs anyway")
    if check_code == 1:
        return "FAIL", True, (
            "xAI OAuth expired. OAuth cannot be made permanent (~6h access "
            "token; refresh is blocked). Permanent: XAI_API_KEY from "
            "console.x.ai in ~/.openclaw/.env")
    return "WARN", False, "could not parse openclaw models status --check"


def _load_reauth() -> dict:
    try:
        return json.loads(REAUTH_JSON.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


def reauth_payload_from_report(report: Report) -> dict:
    oauth = next((c for c in report.checks if c.step == "runtime.oauth"), None)
    pong = next((c for c in report.checks if c.step == "runtime.pong"), None)
    oauth_st = oauth.status if oauth else "WARN"
    pong_ok = bool(pong and pong.status == "OK")
    if oauth_st == "FAIL":
        status = "needs_reauth"
    elif oauth_st == "WARN":
        status = "expiring"
    else:
        status = "ok"
    return {
        "status": status,
        "oauth": oauth_st,
        "pong_ok": pong_ok,
        "reason": (oauth.detail if oauth else "")[:240],
        "user_code": None,
        "verification_uri": DEVICE_URI,
        "expires_at": None,
        "updated_at": datetime.now(ET).isoformat(),
        "source": "pipeline_health",
        "job": report.job,
        "date": report.date,
    }


def write_reauth_status(report: Report) -> None:
    """Publish a tiny public JSON the phone Claw tab polls.

    Never clobber an in-flight device code (status=waiting + unexpired).
    """
    incoming = reauth_payload_from_report(report)
    existing = _load_reauth()
    if existing.get("status") == "waiting" and existing.get("user_code"):
        still = True
        exp = existing.get("expires_at")
        if exp:
            try:
                when = datetime.fromisoformat(str(exp).replace("Z", "+00:00"))
                if when.tzinfo is None:
                    when = when.replace(tzinfo=ET)
                still = when > datetime.now(ET)
            except ValueError:
                still = True
        if still:
            incoming["status"] = "waiting"
            incoming["user_code"] = existing.get("user_code")
            incoming["verification_uri"] = existing.get("verification_uri") or DEVICE_URI
            incoming["expires_at"] = existing.get("expires_at")
            incoming["source"] = existing.get("source") or "xai_device_reauth"
    REAUTH_JSON.parent.mkdir(parents=True, exist_ok=True)
    REAUTH_JSON.write_text(json.dumps(incoming, indent=2) + "\n", encoding="utf-8")


def _spawn_device_reauth() -> str | None:
    if not XAI_REAUTH.is_file():
        return None
    existing = _load_reauth()
    if existing.get("status") == "waiting" and existing.get("user_code"):
        return None
    if grok_busy():
        return None
    HEAL_LOG.mkdir(parents=True, exist_ok=True)
    log = HEAL_LOG / "xai_reauth.log"
    logf = open(log, "a", encoding="utf-8")
    subprocess.Popen(
        ["setsid", sys.executable, str(XAI_REAUTH), "--force"],
        cwd=str(ROOT),
        stdout=logf,
        stderr=subprocess.STDOUT,
        start_new_session=True,
        env={**os.environ, "HOME": os.environ.get("HOME") or "/home/gha"},
    )
    return "started xAI device-code waiter (phone Claw tab)"


def _read_xai_api_key() -> str:
    env = (os.environ.get("XAI_API_KEY") or "").strip()
    if env.startswith("xai-"):
        return env
    for path in (OPENCLAW_ENV, Path(os.path.expanduser("~/.openclaw/.env"))):
        try:
            for line in path.read_text(encoding="utf-8").splitlines():
                if line.startswith("XAI_API_KEY="):
                    val = line.split("=", 1)[1].strip().strip('"').strip("'")
                    if val.startswith("xai-"):
                        return val
        except OSError:
            continue
    return ""


def _ensure_xai_api_key() -> str | None:
    """Install a long-lived console key onto the gateway host if we have one."""
    key = _read_xai_api_key()
    if not key:
        return None
    os.environ["XAI_API_KEY"] = key
    path = OPENCLAW_ENV
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        existing = path.read_text(encoding="utf-8") if path.exists() else ""
    except OSError:
        existing = ""
    lines = [ln for ln in existing.splitlines() if not ln.startswith("XAI_API_KEY=")]
    lines.append(f"XAI_API_KEY={key}")
    body = "\n".join(lines).rstrip() + "\n"
    if existing == body:
        os.environ["XAI_API_KEY"] = key
        return None
    path.write_text(body, encoding="utf-8")
    try:
        path.chmod(0o600)
    except OSError:
        pass
    return f"wrote XAI_API_KEY to {path} (permanent, no OAuth expiry)"


def _file_contains(path: Path, needle: str) -> bool:
    try:
        return needle in path.read_text(encoding="utf-8", errors="ignore")
    except OSError:
        return False


def _slug(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", s.lower()).strip("_")


def _pull_paths(paths: list[str]) -> None:
    if not paths:
        return
    try:
        subprocess.run(["git", "fetch", "origin", "main"], cwd=str(ROOT),
                       capture_output=True, timeout=40, check=False)
        subprocess.run(["git", "checkout", "origin/main", "--", *paths],
                       cwd=str(ROOT), capture_output=True, timeout=40, check=False)
    except (OSError, subprocess.SubprocessError) as e:
        print(f"[health] git pull skipped: {e}", flush=True)


def _pull_scrape_paths(date: str) -> None:
    _pull_paths([
        f"01_daily/news/{date}_finviz_digest.json",
        f"01_daily/news/{date}_finviz_digest.md",
        f"01_daily/map_heat/{date}_map_heat.json",
        f"01_daily/map_heat/{date}_map_heat.md",
    ])


def _pull_book_paths(date: str) -> None:
    _pull_paths([
        f"01_daily/weather/{date}_weather.json",
        f"data/universe/{date}_membership.csv",
        f"data/join/{date}_ranked.csv",
        f"data/exports/finviz_{date}.csv",
        f"data/ab_checklist/{date}_ab_checklist.csv",
        f"data/ab_checklist/{date}_ab_checklist_enriched.csv",
        f"data/peers/{date}_peer_rs.csv",
        f"data/stock_book/{date}_stock_book.json",
        f"data/stock_book/{date}_green.json",
        f"01_daily/{date}_stock_book.md",
        f"01_daily/{date}_learnings.md",
        "dashboard/index.html",
        "03_scoreboard/HIT_BOARD.md",
        "03_scoreboard/PAPER_TRADING.md",
        "03_scoreboard/STOCK_BOOK_BACKTEST.md",
        "03_scoreboard/LEARNINGS.md",
    ])


# ---------------------------------------------------------------------------
# A. Door
# ---------------------------------------------------------------------------

def check_runtime(report: Report, skip_probe: bool = False) -> None:
    print("\n== A. DOOR (OpenClaw) ==", flush=True)
    home = os.environ.get("HOME") or ""
    _add(report, step="runtime.home", name="HOME is /home/gha on ECS",
         group="runtime",
         status="OK" if home.rstrip("/") == "/home/gha" else "WARN",
         required=False, detail=f"HOME={home!r}")

    live, live_n = _live_token()
    env = os.environ.get("OPENCLAW_TOKEN") or ""
    if live_n == LIVE_TOKEN_LEN and env and len(env) == STALE_SECRET_LEN and live != env:
        os.environ["OPENCLAW_TOKEN"] = live
        os.environ["OPENCLAW_GATEWAY_TOKEN"] = live
        _add(report, step="runtime.token", name="OpenClaw token (48 json vs 64 secret)",
             group="runtime", status="WARN", required=False,
             detail=f"MISMATCH — using live json tail={_tail(live)} (secret ignored)")
    elif live_n == 0 and len(env) == STALE_SECRET_LEN:
        _add(report, step="runtime.token", name="OpenClaw token (48 json vs 64 secret)",
             group="runtime", status="FAIL", required=True,
             detail="only 64-char GitHub secret present — 401 / SearXNG path")
    elif live_n == LIVE_TOKEN_LEN or (live_n == 0 and len(env) == LIVE_TOKEN_LEN):
        if live:
            os.environ["OPENCLAW_TOKEN"] = live
        _add(report, step="runtime.token", name="OpenClaw token (48 json vs 64 secret)",
             group="runtime", status="OK", required=True,
             detail=f"live_len={live_n or len(env)} tail={_tail(live or env)}")
    elif live_n or env:
        _add(report, step="runtime.token", name="OpenClaw token (48 json vs 64 secret)",
             group="runtime", status="WARN", required=False,
             detail=f"json_len={live_n} env_len={len(env)}")
    else:
        _add(report, step="runtime.token", name="OpenClaw token (48 json vs 64 secret)",
             group="runtime", status="FAIL", required=True,
             detail="no token in json or env")

    token = live or env
    port_up = _port_open()
    if port_up:
        _add(report, step="runtime.port", name="OpenClaw port 18789 listening",
             group="runtime", status="OK", required=True, detail=GW)
    else:
        _add(report, step="runtime.port", name="OpenClaw port 18789 listening",
             group="runtime", status="FAIL", required=True,
             detail="connection refused")

    if skip_probe:
        _add(report, step="runtime.pong", name="OpenClaw PONG",
             group="runtime", status="OK" if port_up else "FAIL",
             required=not port_up,
             detail="skipped — Grok job running (will not interrupt)")
        _add(report, step="runtime.model", name="Classroom model is Grok not DeepSeek",
             group="runtime", status="OK" if port_up else "FAIL",
             required=not port_up, detail="probe skipped")
    elif port_up:
        ping = _chat_ping(token)
        if ping.get("http") == 200 and "PONG" in (ping.get("content") or "").upper():
            _add(report, step="runtime.pong", name="OpenClaw PONG",
                 group="runtime", status="OK", required=True,
                 detail=f"model={ping.get('model')}")
        elif ping.get("http") == 401:
            _add(report, step="runtime.pong", name="OpenClaw PONG",
                 group="runtime", status="FAIL", required=True,
                 detail="HTTP 401 — wrong token (64 vs 48)")
        else:
            _add(report, step="runtime.pong", name="OpenClaw PONG",
                 group="runtime", status="FAIL", required=True,
                 detail=f"http={ping.get('http')} {ping.get('error') or ping.get('content')}"[:180])
        model = (ping.get("model") or "").lower()
        if ping.get("http") == 200 and "deepseek" in model:
            _add(report, step="runtime.model", name="Classroom model is Grok not DeepSeek",
                 group="runtime", status="FAIL", required=True,
                 detail=f"model={ping.get('model')} — DeepSeek fallback")
        elif ping.get("http") == 200:
            _add(report, step="runtime.model", name="Classroom model is Grok not DeepSeek",
                 group="runtime", status="OK", required=True,
                 detail=f"model={ping.get('model') or 'openclaw/default'}")
        else:
            _add(report, step="runtime.model", name="Classroom model is Grok not DeepSeek",
                 group="runtime", status="FAIL", required=True, detail="no 200 chat")
    else:
        _add(report, step="runtime.pong", name="OpenClaw PONG",
             group="runtime", status="FAIL", required=True, detail="port down")
        _add(report, step="runtime.model", name="Classroom model is Grok not DeepSeek",
             group="runtime", status="FAIL", required=True, detail="port down")

    code, blob = _models_check()
    pong_ok = any(c.step == "runtime.pong" and c.status == "OK" for c in report.checks)
    st, req, why = oauth_verdict(code, pong_ok)
    extra = (blob or "").replace("\n", " ")[:120]
    _add(report, step="runtime.oauth", name="xAI auth (OAuth or API key)",
         group="runtime", status=st, required=req,
         detail=why + (f" | {extra}" if extra and st != "OK" else ""))
    key = _read_xai_api_key()
    if key:
        _add(report, step="runtime.api_key", name="XAI_API_KEY on this box (permanent)",
             group="runtime", status="OK", required=False,
             detail=f"present tail={_tail(key)}")
    elif st != "OK":
        _add(report, step="runtime.api_key", name="XAI_API_KEY on this box (permanent)",
             group="runtime", status="WARN", required=False,
             detail="missing — OAuth dies ~6h. Put a console.x.ai key in ~/.openclaw/.env")

    grok_only = config.grok_only()
    _add(report, step="runtime.grok_only", name="GROK_ONLY (this health process)",
         group="runtime",
         status="OK" if grok_only else "WARN", required=False,
         detail="on" if grok_only else "off here — heal exports GROK_ONLY=1")


# ---------------------------------------------------------------------------
# B. Clocks — n=0 is WARN; missing files in C–H are the real FAIL
# ---------------------------------------------------------------------------

def _gh_runs(date_map: dict[str, str]) -> list[dict]:
    token = os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN") or ""
    repo = os.environ.get("GITHUB_REPOSITORY") or "SRoyaltyy/fullscan"
    if not token:
        return []
    out = []
    for wf, title, _jobs, _role in WORKFLOWS:
        date = date_map.get(wf, "")
        url = (f"https://api.github.com/repos/{repo}/actions/workflows/{wf}"
               f"/runs?per_page=10")
        req = urllib.request.Request(url, headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "User-Agent": "fullscan-health",
        })
        try:
            with urllib.request.urlopen(req, timeout=20) as resp:
                payload = json.loads(resp.read().decode())
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError, OSError) as e:
            out.append({"wf": wf, "title": title, "error": str(e)[:120]})
            continue
        today = []
        for run in payload.get("workflow_runs") or []:
            created = str(run.get("created_at") or "")
            try:
                utc = datetime.fromisoformat(created.replace("Z", "+00:00"))
                et_d = utc.astimezone(ET).date().isoformat()
            except ValueError:
                et_d = ""
            if et_d != date:
                continue
            today.append({
                "conclusion": run.get("conclusion"),
                "status": run.get("status"),
                "event": run.get("event"),
                "html_url": run.get("html_url"),
            })
        out.append({"wf": wf, "title": title, "n": len(today),
                    "latest": today[0] if today else None})
    return out


def check_clocks(report: Report, job: str, preopen_date: str,
                 postclose_night: str, book_date: str) -> None:
    print("\n== B. DID THE JOBS FIRE ==", flush=True)
    for unit, label, req in (
        ("fullscan-preopen.timer", "systemd pre-open timer enabled", job == "preopen"),
        ("fullscan-preopen.service", "systemd pre-open service (now)", False),
        ("fullscan-map-postclose.timer", "systemd post-close timer enabled",
         job == "postclose"),
        ("fullscan-openclaw-gateway.service", "OpenClaw gateway unit", False),
    ):
        if "timer" in unit:
            val = _systemctl("is-enabled", unit)
            ok = val in ("enabled", "enabled-runtime")
        else:
            val = _systemctl("is-active", unit)
            ok = val == "active"
        _add(report, step=f"clock.{unit}", name=label, group="clock",
             status="OK" if ok else ("FAIL" if req else "WARN"), required=req,
             detail=val or "not found")

    clock = ROOT / "01_daily" / "_ecs_clock.md"
    artifact(report, step="clock.ecs_clock", name="ECS clock file written",
             group="clock", path=clock, required=False, expected_date=preopen_date)

    token = os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN") or ""
    if not token:
        _add(report, step="clock.gh", name="GitHub Actions runs for this ET date",
             group="clock", status="WARN", required=False,
             detail="no GITHUB_TOKEN — cannot list runs")
        return

    def _when(role: str) -> str:
        if role == "postclose":
            return postclose_night
        if role == "book":
            return book_date
        return preopen_date

    date_map = {wf: _when(role) for wf, _t, _jobs, role in WORKFLOWS}
    rows_by_wf = {r.get("wf"): r for r in _gh_runs(date_map)}
    for wf, title, jobs, role in WORKFLOWS:
        if job != "all" and job not in jobs:
            continue
        req = job in jobs
        when = _when(role)
        row = rows_by_wf.get(wf) or {"wf": wf, "title": title, "n": 0, "latest": None}
        latest = row.get("latest") or {}
        if row.get("error"):
            _add(report, step=f"clock.{wf}", name=title, group="clock",
                 status="WARN", required=False, detail=row["error"])
            continue
        if not latest:
            _add(report, step=f"clock.{wf}", name=f"{title} ran on {when}",
                 group="clock", status="WARN", required=False,
                 detail=f"n=0 on {when} — file checks below decide")
            continue
        conc = latest.get("conclusion") or latest.get("status") or "?"
        if conc == "success":
            st = "OK"
        elif conc in ("in_progress", "queued", None):
            st = "WARN"
        else:
            # A failed GH run is a clue. Files in C–H decide the packet.
            st = "WARN"
        _add(report, step=f"clock.{wf}", name=f"{title} ran on {when}",
             group="clock", status=st, required=False,
             detail=f"n={row.get('n')} latest={conc} event={latest.get('event')}",
             path=latest.get("html_url") or "")


# ---------------------------------------------------------------------------
# C–H artifact checks (same contract as the Map)
# ---------------------------------------------------------------------------

def check_scrape(report: Report, date: str) -> None:
    print(f"\n== C. FINVIZ PRE-OPEN SCRAPE (date {date}) ==", flush=True)
    news = ROOT / "01_daily" / "news"
    heat = ROOT / "01_daily" / "map_heat"
    artifact(report, step="scrape.digest_json",
             name="Finviz Elite digest JSON", group="scrape",
             path=news / f"{date}_finviz_digest.json", required=True,
             expected_date=date, qc=output_qc.qc_finviz_digest)
    artifact(report, step="scrape.digest_md",
             name="Finviz Elite digest MD", group="scrape",
             path=news / f"{date}_finviz_digest.md", required=False,
             expected_date=date)
    mh = heat / f"{date}_map_heat.json"
    artifact(report, step="scrape.map_heat",
             name="Map heat JSON (groups + morning overlay)", group="scrape",
             path=mh, required=True, expected_date=date, qc=output_qc.qc_map_heat)
    data = _json(mh) if mh.exists() else None
    if isinstance(data, dict):
        overlay_at = str(data.get("overlay_at") or "")
        tape = data.get("tape") or []
        if not tape:
            _add(report, step="scrape.tape", name="Futures tape non-empty",
                 group="scrape", status="FAIL", required=True,
                 detail="EMPTY TAPE (403 overlay / scrape skipped)")
        elif overlay_at.startswith(date):
            _add(report, step="scrape.tape", name="Futures tape + overlay_at today",
                 group="scrape", status="OK", required=True,
                 detail=f"overlay_at={overlay_at} tape_n={len(tape)}")
        else:
            _add(report, step="scrape.tape", name="Futures tape + overlay_at today",
                 group="scrape", status="FAIL", required=True,
                 detail=f"overlay_at={overlay_at or 'missing'} (pass-over / not overlaid today)")
        notes = str(data.get("notes") or data.get("one_paragraph") or "")
        if "not scraped" in notes.lower() or "elite session empty" in notes.lower():
            _add(report, step="scrape.skipped", name="Maps actually scraped (not skipped)",
                 group="scrape", status="FAIL", required=True,
                 detail="notes say maps were not scraped / Elite 403")


def check_postclose(report: Report, source: str, target: str) -> None:
    print(f"\n== D. POST-CLOSE (night of {source} → files dated {target}) ==",
          flush=True)
    heat = ROOT / "01_daily" / "map_heat"
    tr = ROOT / "01_daily" / "_transcripts"
    artifact(report, step="postclose.map_heat_json",
             name=f"{target}_map_heat.json (industry groups + captains)",
             group="postclose", path=heat / f"{target}_map_heat.json",
             required=True, expected_date=target)
    # Tape is the 05:40 GH overlay — empty tape here is not a post-close FAIL.
    artifact(report, step="postclose.map_heat_md",
             name=f"{target}_map_heat.md", group="postclose",
             path=heat / f"{target}_map_heat.md", required=False,
             expected_date=target)
    base = heat / f"{target}_research_baseline.json"
    artifact(report, step="postclose.baseline_json",
             name=f"{target}_research_baseline.json (captain cards)",
             group="postclose", path=base, required=True,
             expected_date=target, qc=output_qc.qc_map_heat_baseline)
    artifact(report, step="postclose.baseline_md",
             name=f"{target}_research_baseline.md", group="postclose",
             path=heat / f"{target}_research_baseline.md", required=True,
             expected_date=target)
    data = _json(base) if base.exists() else None
    if isinstance(data, dict):
        n = len(data.get("cards") or [])
        cov = data.get("coverage")
        try:
            cov_f = float(cov) if cov is not None else 1.0
        except (TypeError, ValueError):
            cov_f = 0.0
        _add(report, step="postclose.coverage",
             name="Captain coverage ≥ 90%", group="postclose",
             status="OK" if cov_f >= 0.90 and n >= 20 else "FAIL",
             required=True, detail=f"cards={n} coverage={cov}")
    blob = _read(heat / f"{target}_research_baseline.md")
    blob += _read(base) if base.exists() else ""
    if "searxng" in blob.lower() and "0 results" in blob.lower():
        _add(report, step="postclose.searxng",
             name="Research used Grok native search (not SearXNG)",
             group="postclose", status="FAIL", required=True,
             detail="transcript/baseline mentions SearXNG 0 results")
    if _deepseekish(blob):
        _add(report, step="postclose.deepseek",
             name="Research used Grok (not DeepSeek fallback)",
             group="postclose", status="FAIL", required=True,
             detail="DeepSeek fallback text in baseline")
    trans = list(tr.glob(f"{target}_map_postclose_*.json")) if tr.is_dir() else []
    _add(report, step="postclose.transcripts",
         name="Post-close LLM transcripts", group="postclose",
         status="OK" if trans else "WARN", required=False,
         detail=f"n={len(trans)}")


def check_preopen(report: Report, date: str) -> None:
    print(f"\n== E. PRE-OPEN ALL (every step, date {date}) ==", flush=True)
    news = ROOT / "01_daily" / "news"
    ev = ROOT / "01_daily" / "events"
    heat = ROOT / "01_daily" / "map_heat"
    cat = ROOT / "01_daily" / "catalyst"
    gen = ROOT / "01_daily" / "general"
    sec = ROOT / "01_daily" / "sectors" / date

    artifact(report, step="preopen.in_digest",
             name="INPUT: Finviz digest from 05:40 scrape",
             group="preopen", path=news / f"{date}_finviz_digest.json",
             required=True, expected_date=date, qc=output_qc.qc_finviz_digest)
    artifact(report, step="preopen.in_baseline",
             name="INPUT: last-night captain baseline",
             group="preopen", path=heat / f"{date}_research_baseline.json",
             required=True, expected_date=date, qc=output_qc.qc_map_heat_baseline)

    artifact(report, step="preopen.news_parse_json",
             name="News parse JSON", group="preopen",
             path=news / f"{date}_parsed.json", required=True,
             expected_date=date, qc=output_qc.qc_news_parse)
    evj = ev / f"{date}_events.json"
    artifact(report, step="preopen.events",
             name="Event scanner (primary, NOT carry)", group="preopen",
             path=evj, required=True, expected_date=date,
             qc=output_qc.qc_events_path)
    data = _json(evj) if evj.exists() else None
    if isinstance(data, dict):
        catcher = data.get("catcher") or {}
        if catcher.get("ran"):
            _add(report, step="preopen.catcher",
                 name="Event catcher second pass ran", group="preopen",
                 status="OK", required=True,
                 detail=f"found={catcher.get('found')}")
        else:
            _add(report, step="preopen.catcher",
                 name="Event catcher second pass ran", group="preopen",
                 status="FAIL", required=True,
                 detail="catcher key missing — second pass DID NOT RUN")
        latest = _json(ev / "latest.json")
        if isinstance(latest, dict) and str(latest.get("scan_date") or "")[:10] not in ("", date):
            _add(report, step="preopen.events_latest",
                 name="events/latest.json points at today", group="preopen",
                 status="FAIL", required=True,
                 detail=f"PASS-OVER latest scan_date={latest.get('scan_date')}")

    artifact(report, step="preopen.judge_md",
             name="News judge MD", group="preopen",
             path=news / f"{date}_judge.md", required=True,
             expected_date=date, qc=output_qc.qc_news_judge)
    artifact(report, step="preopen.research_json",
             name="Map-heat morning refresh JSON", group="preopen",
             path=heat / f"{date}_research.json", required=True,
             expected_date=date)
    artifact(report, step="preopen.research_md",
             name="Map-heat morning refresh MD", group="preopen",
             path=heat / f"{date}_research.md", required=True,
             expected_date=date, qc=output_qc.qc_map_heat_research)
    artifact(report, step="preopen.actions",
             name="News actions JSON", group="preopen",
             path=news / f"{date}_actions.json", required=False,
             expected_date=date, qc=output_qc.qc_news_actions)

    dj = cat / f"{date}_dossiers.json"
    artifact(report, step="preopen.catalyst_json",
             name="Catalyst dossiers JSON", group="preopen",
             path=dj, required=False, expected_date=date)
    djs = _json(dj) if dj.exists() else None
    if isinstance(djs, dict):
        rows = djs.get("dossiers") or []
        native = [r for r in rows if isinstance(r, dict)
                  and r.get("search_backend") == "grok_native" and not r.get("error")]
        searx = [r for r in rows if isinstance(r, dict)
                 and "searx" in str(r.get("search_backend") or "").lower()]
        if searx and not native:
            _add(report, step="preopen.catalyst_backend",
                 name="Catalyst search_backend=grok_native", group="preopen",
                 status="FAIL", required=True,
                 detail=f"SearXNG on {len(searx)} dossiers — re-run with GROK_ONLY")
        elif native:
            _add(report, step="preopen.catalyst_backend",
                 name="Catalyst search_backend=grok_native", group="preopen",
                 status="OK", required=False, detail=f"grok_native={len(native)}/{len(rows)}")

    artifact(report, step="preopen.general",
             name="General market predict", group="preopen",
             path=gen / f"{date}_predict.md", required=True,
             expected_date=date, qc=output_qc.qc_general_predict)
    n_ok = 0
    for sector in FINVIZ_SECTORS:
        p = sec / f"{_slug(sector)}_predict.md"
        before = len(report.checks)
        artifact(report, step=f"preopen.sector.{_slug(sector)}",
                 name=f"Sector predict — {sector}", group="preopen",
                 path=p, required=True, expected_date=date,
                 qc=output_qc.qc_sector_predict)
        if report.checks[-1].status == "OK" and len(report.checks) > before:
            n_ok += 1
    _add(report, step="preopen.sector_count",
         name="≥8/11 quality sector predicts", group="preopen",
         status="OK" if n_ok >= 8 else "FAIL", required=True,
         detail=f"{n_ok}/11")
    artifact(report, step="preopen.board",
             name="Sector board JSON", group="preopen",
             path=sec / "_board.json", required=False, expected_date=date)
    artifact(report, step="preopen.qc",
             name="Pre-open QC JSON", group="preopen",
             path=ROOT / "01_daily" / f"{date}_preopen_qc.json",
             required=True, expected_date=date)
    stj = ROOT / "01_daily" / f"{date}_preopen_status.json"
    artifact(report, step="preopen.status_json",
             name="Pre-open status JSON", group="preopen",
             path=stj, required=True, expected_date=date)
    st = _json(stj) if stj.exists() else None
    if isinstance(st, dict):
        _add(report, step="preopen.status_all_ok",
             name="preopen_status.all_ok true", group="preopen",
             status="OK" if st.get("all_ok") else "FAIL", required=True,
             detail=f"all_ok={st.get('all_ok')} grok_ok={st.get('grok_ok')}")
    gr = ROOT / "01_daily" / f"{date}_grok_review.json"
    artifact(report, step="preopen.grok_review_json",
             name="Grok text review JSON", group="preopen",
             path=gr, required=True, expected_date=date)
    g = _json(gr) if gr.exists() else None
    if isinstance(g, dict):
        _add(report, step="preopen.grok_review_ok",
             name="Grok text review ok=true", group="preopen",
             status="OK" if g.get("ok") else "FAIL", required=True,
             detail=str(g.get("notes") or g.get("fails") or "")[:160])


def check_bookchain(report: Report, date: str) -> None:
    print(f"\n== F. STOCK BOOK CHAIN (date {date}) ==", flush=True)
    artifact(report, step="book.finviz", name="INPUT: Finviz Elite export",
             group="book", path=ROOT / "data" / "exports" / f"finviz_{date}.csv",
             required=True, expected_date=date)
    artifact(report, step="book.membership", name="Universe labels (segments)",
             group="book",
             path=ROOT / "data" / "universe" / f"{date}_membership.csv",
             required=True, expected_date=date)
    wx = ROOT / "01_daily" / "weather" / f"{date}_weather.json"
    artifact(report, step="book.weather", name="Weather / regime JSON",
             group="book", path=wx, required=True, expected_date=date)
    wj = _json(wx) if wx.exists() else None
    if isinstance(wj, dict):
        nsec = len((wj.get("signals") or {}).get("sectors") or {})
        _add(report, step="book.weather_sectors",
             name="Weather signals.sectors ≥ 5", group="book",
             status="OK" if nsec >= 5 else "FAIL", required=True,
             detail=f"n={nsec}")
    artifact(report, step="book.join", name="Join ranked CSV", group="book",
             path=ROOT / "data" / "join" / f"{date}_ranked.csv",
             required=True, expected_date=date)
    artifact(report, step="book.ab_raw", name="AB checklist (raw)",
             group="book",
             path=ROOT / "data" / "ab_checklist" / f"{date}_ab_checklist.csv",
             required=False, expected_date=date)
    artifact(report, step="book.ab", name="AB checklist (enriched) — s_ab",
             group="book",
             path=ROOT / "data" / "ab_checklist" / f"{date}_ab_checklist_enriched.csv",
             required=True, expected_date=date)
    artifact(report, step="book.peers", name="Peer relative strength — s_peer",
             group="book",
             path=ROOT / "data" / "peers" / f"{date}_peer_rs.csv",
             required=True, expected_date=date)
    artifact(report, step="book.book_json", name="Stock book JSON (5 horizons)",
             group="book",
             path=ROOT / "data" / "stock_book" / f"{date}_stock_book.json",
             required=True, expected_date=date)
    artifact(report, step="book.green_json",
             name="Green pile grades (all-green BUY)",
             group="book",
             path=ROOT / "data" / "stock_book" / f"{date}_green.json",
             required=True, expected_date=date)
    artifact(report, step="book.book_md", name="Stock book MD", group="book",
             path=ROOT / "01_daily" / f"{date}_stock_book.md",
             required=True, expected_date=date)
    artifact(report, step="book.backtest", name="Stock book backtest (repo-level)",
             group="book", path=ROOT / "03_scoreboard" / "STOCK_BOOK_BACKTEST.md",
             required=True, expected_date=date)
    artifact(report, step="book.paper_md", name="Paper trading summary",
             group="book", path=ROOT / "03_scoreboard" / "PAPER_TRADING.md",
             required=True, expected_date=date)
    artifact(report, step="book.dashboard", name="Dashboard HTML (Pages source)",
             group="book", path=ROOT / "dashboard" / "index.html",
             required=True, expected_date=date)
    hit = ROOT / "03_scoreboard" / "HIT_BOARD.md"
    artifact(report, step="book.hit_board", name="HIT_BOARD",
             group="book", path=hit, required=False, expected_date=date)


def check_outcomes(report: Report, date: str) -> None:
    print(f"\n== G. OUTCOME / REFLECT / GRADING (date {date}) ==", flush=True)
    gen = ROOT / "01_daily" / "general"
    artifact(report, step="outcome.general", name="General outcome (graded call)",
             group="outcome", path=gen / f"{date}_outcome.md",
             required=True, expected_date=date)
    artifact(report, step="outcome.reflect", name="General reflect MD",
             group="outcome", path=gen / f"{date}_reflect.md",
             required=False, expected_date=date)
    cand_dir = ROOT / "02_lessons" / "candidate"
    cand = list(cand_dir.glob(f"{date}*")) if cand_dir.is_dir() else []
    _add(report, step="outcome.candidate", name="Candidate lesson filed today",
         group="outcome", status="OK" if cand else "WARN", required=False,
         detail=f"n={len(cand)}")
    sec = ROOT / "01_daily" / "sectors" / date
    n_ok = sum(1 for sector in FINVIZ_SECTORS
               if (sec / f"{_slug(sector)}_outcome.md").exists()
               and (sec / f"{_slug(sector)}_outcome.md").stat().st_size >= 8)
    _add(report, step="outcome.sector_count", name="Sector outcomes graded (>=8/11)",
         group="outcome", status="OK" if n_ok >= 8 else "FAIL", required=True,
         detail=f"{n_ok}/11")


def check_learning(report: Report, date: str) -> None:
    print(f"\n== H. LEARNING LOOP (date {date}) ==", flush=True)
    lm = ROOT / "03_scoreboard" / "LEARNINGS.md"
    artifact(report, step="learn.dated",
             name=f"{date}_learnings.md (session copy)",
             group="learn",
             path=ROOT / "01_daily" / f"{date}_learnings.md",
             required=True, expected_date=date)
    artifact(report, step="learn.learnings", name="LEARNINGS.md digest",
             group="learn", path=lm, required=True, expected_date=date)
    if lm.exists() and not _file_contains(lm, date):
        _add(report, step="learn.learnings_fresh", name="LEARNINGS.md refreshed today",
             group="learn", status="WARN", required=False,
             detail=f"no '{date}' inside")
    artifact(report, step="learn.mutable_policy",
             name="mutable_policy.md (machine injection)", group="learn",
             path=ROOT / "00_grounding" / "mutable_policy.md",
             required=True, expected_date=date)
    artifact(report, step="learn.book_policy",
             name="book_policy.json (learned ranker weights)", group="learn",
             path=ROOT / "00_grounding" / "book_policy.json",
             required=False, expected_date=date)


def check_pages(report: Report, date: str) -> None:
    print("\n== I. DASHBOARD ON GITHUB PAGES ==", flush=True)
    try:
        req = urllib.request.Request(PAGES_URL, headers={
            "User-Agent": "fullscan-health", "Cache-Control": "no-cache",
        })
        with urllib.request.urlopen(req, timeout=15) as resp:
            code = resp.getcode()
            body = resp.read(300000).decode("utf-8", "ignore")
        injected = "__DATA__" not in body
        _add(report, step="pages.dashboard",
             name="Live dashboard reachable + data injected",
             group="pages", status="OK" if (code == 200 and injected) else "FAIL",
             required=True, detail=f"HTTP {code}, data_injected={injected}",
             path=PAGES_URL)
    except (urllib.error.URLError, TimeoutError, OSError) as e:
        _add(report, step="pages.dashboard",
             name="Live dashboard reachable + data injected",
             group="pages", status="FAIL", required=True,
             detail=str(e)[:160], path=PAGES_URL)


# ---------------------------------------------------------------------------
# Heal
# ---------------------------------------------------------------------------

def _workflow_for_step(step: str) -> str | None:
    if step in HUMAN_STEPS:
        return None
    # GH run history is observational. File FAILs in C–H own the heal.
    if step.startswith("clock.") and step.endswith(".yml"):
        return None
    for prefix, wf in FIX_MAP:
        if step.startswith(prefix):
            return wf
    return None


def _dispatch_payload(wf: str, date: str, source: str, target: str, book: str) -> dict:
    if wf == "preopen_all.yml":
        return {"ref": "main", "inputs": {"run_date": date, "force": "true"}}
    if wf == "finviz_preopen_scrape.yml":
        return {"ref": "main", "inputs": {"run_date": date, "force": "true"}}
    if wf == "postclose_all.yml":
        return {"ref": "main", "inputs": {
            "run_date": source or date, "force": "true"}}
    if wf == "map_heat_postclose.yml":
        return {"ref": "main", "inputs": {
            "source_date": source, "target_date": target, "force": "true"}}
    if wf == "stock_book_all.yml":
        return {"ref": "main", "inputs": {
            "run_date": book or date, "force": "true",
            "runner": "ubuntu", "skip_llm": "true", "skip_extras": "true"}}
    if wf == "daily_pipeline.yml":
        return {"ref": "main", "inputs": {
            "stage": "outcome", "run_date": book or date, "force": "true"}}
    if wf == "sector_daily.yml":
        return {"ref": "main", "inputs": {
            "stage": "outcome_reflect", "run_date": book or date, "force": "true"}}
    if wf == "label_weather.yml":
        return {"ref": "main", "inputs": {"run_date": book or date}}
    if wf == "ab_checklist.yml":
        return {"ref": "main", "inputs": {"date": book or date}}
    if wf == "catalyst_daily.yml":
        return {"ref": "main", "inputs": {"run_date": date, "force": "true"}}
    return {"ref": "main"}


def _gh_request(path: str, method: str = "GET", payload: dict | None = None):
    token = os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN") or ""
    repo = os.environ.get("GITHUB_REPOSITORY") or "SRoyaltyy/fullscan"
    if not token:
        return None, "no GITHUB_TOKEN"
    headers = {"Authorization": f"Bearer {token}",
               "Accept": "application/vnd.github+json",
               "User-Agent": "fullscan-health"}
    data = None
    if payload is not None:
        headers["Content-Type"] = "application/json"
        data = json.dumps(payload).encode()
    req = urllib.request.Request(
        f"https://api.github.com/repos/{repo}{path}", method=method,
        headers=headers, data=data)
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            return resp.getcode(), ""
    except urllib.error.HTTPError as e:
        return e.code, str(e)[:160]
    except (urllib.error.URLError, TimeoutError, OSError) as e:
        return None, str(e)[:160]


def _gh_workflow_busy(wf: str) -> bool:
    token = os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN") or ""
    repo = os.environ.get("GITHUB_REPOSITORY") or "SRoyaltyy/fullscan"
    if not token:
        return False
    req = urllib.request.Request(
        f"https://api.github.com/repos/{repo}/actions/workflows/{wf}/runs?per_page=5",
        headers={"Authorization": f"Bearer {token}",
                 "Accept": "application/vnd.github+json",
                 "User-Agent": "fullscan-health"})
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            payload = json.loads(resp.read().decode())
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError, OSError):
        return False
    return any(r.get("status") in ("queued", "in_progress")
               for r in payload.get("workflow_runs") or [])


def _start_unit(unit: str) -> str:
    _systemctl("reset-failed", unit)
    out = _systemctl("start", unit)
    time.sleep(2)
    state = _systemctl("is-active", unit) or "unknown"
    return f"systemctl start {unit} → {state} {out}".strip()


def _heal_env(extra: dict[str, str] | None = None) -> dict[str, str]:
    env = {**os.environ, "GROK_ONLY": "0", "HOME": "/home/gha",
           "FULLSCAN_HOME": "/home/gha", "PYTHONUNBUFFERED": "1"}
    live, n = _live_token()
    if n == LIVE_TOKEN_LEN:
        env["OPENCLAW_TOKEN"] = live
        env["OPENCLAW_GATEWAY_TOKEN"] = live
    key = _read_xai_api_key()
    if key:
        env["XAI_API_KEY"] = key
    if extra:
        env.update(extra)
    return env


def _spawn(cmd: list[str], extra_env: dict[str, str], log_name: str) -> str:
    HEAL_LOG.mkdir(parents=True, exist_ok=True)
    log = HEAL_LOG / log_name
    env = _heal_env(extra_env)
    with open(log, "ab") as fh:
        stamp = datetime.now(ET).isoformat()
        fh.write(f"\n--- heal spawn {stamp} {cmd}\n".encode())
        fh.flush()
        subprocess.Popen(
            cmd, cwd=str(ROOT), env=env,
            stdout=fh, stderr=subprocess.STDOUT,
            start_new_session=True,
        )
    return f"spawned {' '.join(cmd)} log={log}"


def _spawn_cmd(wf: str, date: str, source: str, target: str, book: str
               ) -> tuple[list[str], dict[str, str]] | None:
    py = str(ROOT / ".venv" / "bin" / "python")
    if not Path(py).exists():
        py = "python3"
    if wf == "preopen_all.yml":
        script = ECS_SCRIPTS[wf]
        return ["bash", str(script)], {"RUN_DATE": date, "FORCE": "true"}
    if wf == "postclose_all.yml":
        script = ECS_SCRIPTS[wf]
        return ["bash", str(script)], {
            "SOURCE_DATE": source, "TARGET_DATE": target, "FORCE": "true"}
    if wf == "map_heat_postclose.yml":
        script = ECS_SCRIPTS[wf]
        return ["bash", str(script)], {
            "SOURCE_DATE": source, "TARGET_DATE": target, "FORCE": "true"}
    if wf == "stock_book_all.yml":
        return [py, "-m", "src.run_stock_book_all", "--date", book,
                "--force", "--skip-llm", "--skip-extras"], {}
    if wf == "daily_pipeline.yml":
        return [py, "-m", "src.run_outcome", "--date", book], {}
    if wf == "sector_daily.yml":
        return [py, "-m", "src.run_sector_outcome", "--date", book], {}
    if wf == "learn_cycle.yml":
        return [py, "-m", "src.learn_cycle"], {}
    if wf == "catalyst_daily.yml":
        return [py, "-m", "src.catalyst_daily", "--date", date], {}
    return None


def _already_running(wf: str) -> bool:
    unit = ECS_UNITS.get(wf)
    if unit and _unit_active(unit):
        return True
    if wf in UBUNTU_WORKFLOWS and _gh_workflow_busy(wf):
        return True
    needles = {
        "preopen_all.yml": ("ecs_preopen.sh", "src.run_preopen_all"),
        "postclose_all.yml": ("ecs_map_postclose.sh", "src.run_postclose_all",
                             "src.map_heat_postclose"),
        "map_heat_postclose.yml": ("ecs_map_postclose.sh", "src.map_heat_postclose",
                                  "src.run_postclose_all"),
        "stock_book_all.yml": ("src.run_stock_book_all",),
        "daily_pipeline.yml": ("src.run_outcome", "src.run_reflect",
                              "src.run_postclose_all"),
        "sector_daily.yml": ("src.run_sector_outcome", "src.run_sector_reflect",
                            "src.run_postclose_all"),
        "learn_cycle.yml": ("src.learn_cycle", "src.run_postclose_all"),
        "catalyst_daily.yml": ("src.catalyst_daily",),
    }.get(wf, ())
    return bool(needles) and _pgrep(*needles)


def _expected_files(wf: str, date: str, source: str, target: str, book: str) -> list[Path]:
    if wf == "finviz_preopen_scrape.yml":
        return [ROOT / "01_daily" / "news" / f"{date}_finviz_digest.json"]
    if wf == "map_heat_postclose.yml":
        return [ROOT / "01_daily" / "map_heat" / f"{target}_research_baseline.json"]
    if wf == "postclose_all.yml":
        closed = source or date
        return [
            ROOT / "01_daily" / "general" / f"{closed}_outcome.md",
            ROOT / "01_daily" / "map_heat" / f"{target}_research_baseline.json",
            ROOT / "01_daily" / f"{closed}_learnings.md",
        ]
    if wf == "preopen_all.yml":
        return [
            ROOT / "01_daily" / f"{date}_preopen_status.json",
            ROOT / "data" / "stock_book" / f"{date}_stock_book.json",
            ROOT / "data" / "stock_book" / f"{date}_green.json",
        ]
    if wf == "label_weather.yml":
        return [
            ROOT / "01_daily" / "weather" / f"{book}_weather.json",
            ROOT / "data" / "join" / f"{book}_ranked.csv",
        ]
    if wf == "ab_checklist.yml":
        return [ROOT / "data" / "ab_checklist" / f"{book}_ab_checklist_enriched.csv"]
    if wf == "stock_book_all.yml":
        return [
            ROOT / "data" / "stock_book" / f"{book}_stock_book.json",
            ROOT / "data" / "stock_book" / f"{book}_green.json",
        ]
    if wf == "daily_pipeline.yml":
        return [ROOT / "01_daily" / "general" / f"{book}_outcome.md"]
    if wf == "learn_cycle.yml":
        return [ROOT / "01_daily" / f"{book}_learnings.md"]
    return []


def _oauth_dead(report: Report) -> bool:
    return any(c.step == "runtime.oauth" and c.status == "FAIL" for c in report.checks)


def _should_heal(c: Check) -> bool:
    if c.status != "FAIL" or not c.required:
        return False
    if c.step in HUMAN_STEPS:
        return False
    if c.step.startswith("clock.") and c.step.endswith(".yml"):
        return False
    return True


def _healable(report: Report) -> list[Check]:
    return [c for c in report.checks if _should_heal(c)]


def _human_fails(report: Report) -> list[Check]:
    return [c for c in report.checks
            if c.status == "FAIL" and c.required and c.step in HUMAN_STEPS]


def fix_local(report: Report) -> list[str]:
    os.environ.setdefault("GROK_ONLY", "0")
    os.environ.setdefault("HOME", "/home/gha")
    actions: list[str] = []
    installed = _ensure_xai_api_key()
    if installed:
        actions.append(installed)
        print(f"[heal] {installed}", flush=True)
    oauth_dead = any(
        c.step == "runtime.oauth" and c.status == "FAIL" for c in report.checks)
    if oauth_dead:
        spawned = _spawn_device_reauth()
        if spawned:
            actions.append(spawned)
            print(f"[heal] {spawned}", flush=True)
    if grok_busy():
        print("[heal] Grok job running — will not bounce OpenClaw", flush=True)
        return actions
    door_bad = any(
        c.status == "FAIL" and c.required and c.step.startswith("runtime.")
        and c.step != "runtime.oauth"
        for c in report.checks)
    wrote_key = bool(installed and installed.startswith("wrote"))
    if (door_bad or wrote_key) and ENABLE_CHAT.exists():
        print("[heal] OpenClaw door/auth — enable_openclaw_chat.sh", flush=True)
        try:
            r = subprocess.run(
                ["bash", str(ENABLE_CHAT)], cwd=str(ROOT),
                capture_output=True, text=True, timeout=180,
                env=_heal_env())
            tail = (r.stdout or r.stderr or "")[-400:].replace("\n", " | ")
            msg = f"enable_openclaw_chat.sh exit={r.returncode} {tail[:180]}"
            actions.append(msg)
            print(f"[heal] {msg}", flush=True)
        except (OSError, subprocess.SubprocessError) as e:
            actions.append(f"enable_openclaw_chat.sh failed: {e}")

    for unit, need in (
        ("fullscan-preopen.timer",
         any(c.step == "clock.fullscan-preopen.timer" and c.status != "OK"
             for c in report.checks)),
        ("fullscan-map-postclose.timer",
         any(c.step == "clock.fullscan-map-postclose.timer" and c.status != "OK"
             for c in report.checks)),
    ):
        if not need:
            continue
        _systemctl("enable", "--now", unit)
        actions.append(f"systemctl enable --now {unit}")
    return actions


def fix_jobs(report: Report, date: str, source: str, target: str, book: str,
             already: set[str] | None = None
             ) -> tuple[list[str], set[str]]:
    actions: list[str] = []
    started: set[str] = set(already or ())
    state = _json(FIX_STATE) if FIX_STATE.exists() else None
    if not isinstance(state, dict):
        state = {}
    today = dict(state.get(date) or {})

    for c in report.checks:
        if not _should_heal(c):
            if c.step == "runtime.oauth" and c.status == "FAIL":
                print(f"[heal] {c.step}: {c.detail} — still starting Grok jobs",
                      flush=True)
            continue
        wf = _workflow_for_step(c.step)
        if not wf or wf == "pipeline_health.yml" or wf == "door":
            continue
        if wf in started:
            print(f"[heal] {wf} already started this run — wait", flush=True)
            continue
        if wf == "preopen_all.yml" and (
                "map_heat_postclose.yml" in started
                or "postclose_all.yml" in started
                or "finviz_preopen_scrape.yml" in started
                or _already_running("map_heat_postclose.yml")
                or _already_running("postclose_all.yml")
                or _already_running("finviz_preopen_scrape.yml")):
            print("[heal] scrape/baseline still in flight — hold preopen", flush=True)
            continue
        if wf == "stock_book_all.yml" and (
                "label_weather.yml" in started
                or "ab_checklist.yml" in started):
            print("[heal] weather/AB still in flight — hold book", flush=True)
            continue
        if wf in ("daily_pipeline.yml", "sector_daily.yml", "postclose_all.yml") and (
                "stock_book_all.yml" in started or _already_running("stock_book_all.yml")):
            print("[heal] book still in flight — hold outcomes", flush=True)
            continue
        if wf == "learn_cycle.yml" and (
                "daily_pipeline.yml" in started or "sector_daily.yml" in started
                or "postclose_all.yml" in started):
            print("[heal] outcomes still in flight — hold learn", flush=True)
            continue
        if wf in GROK_WORKFLOWS and grok_busy():
            print(f"[heal] Grok already busy — hold {wf}", flush=True)
            started.add(wf)
            continue

        if _already_running(wf):
            print(f"[heal] {wf} already running — wait", flush=True)
            started.add(wf)
            continue

        if wf in UBUNTU_WORKFLOWS:
            payload = _dispatch_payload(wf, date, source, target, book)
            code, err = _gh_request(f"/actions/workflows/{wf}/dispatches",
                                    method="POST", payload=payload)
            if code == 204:
                started.add(wf)
                today[c.step] = datetime.now(ET).isoformat()
                msg = f"dispatched {wf} force (for {c.step})"
                actions.append(msg)
                print(f"[heal] {c.step} FAIL -> {msg}", flush=True)
            else:
                print(f"[heal] dispatch {wf} failed: {err or code}", flush=True)
                actions.append(f"dispatch {wf} failed: {err or code}")
            continue

        # ECS Grok job: never GH-dispatch (would queue behind this health run).
        unit = ECS_UNITS.get(wf)
        use_unit = bool(unit)
        if wf in ("map_heat_postclose.yml", "postclose_all.yml"):
            # unit always uses today→next weekday; dated heal must spawn the script
            today_et = _today()
            if source != today_et:
                use_unit = False
        if use_unit and unit:
            msg = _start_unit(unit)
            actions.append(msg)
            print(f"[heal] {c.step} FAIL -> {msg}", flush=True)
            started.add(wf)
            today[c.step] = datetime.now(ET).isoformat()
            continue

        spawn = _spawn_cmd(wf, date, source, target, book)
        if not spawn:
            print(f"[heal] no spawn map for {wf}", flush=True)
            continue
        cmd, extra = spawn
        msg = _spawn(cmd, extra, f"heal-{wf.replace('.yml', '')}.log")
        actions.append(msg)
        print(f"[heal] {c.step} FAIL -> {msg}", flush=True)
        started.add(wf)
        today[c.step] = datetime.now(ET).isoformat()

    state[date] = today
    FIX_STATE.parent.mkdir(parents=True, exist_ok=True)
    FIX_STATE.write_text(json.dumps(state, indent=2), encoding="utf-8")
    if not actions:
        print("[heal] nothing new started this round", flush=True)
    return actions, started


def wait_for_heal(started: set[str], date: str, source: str, target: str,
                  book: str, seconds: int) -> None:
    if not started:
        time.sleep(min(20, max(5, seconds)))
        return
    deadline = time.time() + max(15, seconds)
    if started & GROK_WORKFLOWS:
        seconds = max(seconds, 600)
        deadline = time.time() + max(15, seconds)
    paths: list[Path] = []
    for wf in started:
        paths.extend(_expected_files(wf, date, source, target, book))
    print(f"[heal] waiting up to {seconds}s for {len(paths)} files from {sorted(started)}",
          flush=True)
    while time.time() < deadline:
        restore_persist(date, target, source, book)
        if "finviz_preopen_scrape.yml" in started:
            _pull_scrape_paths(date)
        if started & {"label_weather.yml", "ab_checklist.yml", "stock_book_all.yml",
                      "deploy-dashboard.yml", "hit_board.yml"}:
            _pull_book_paths(book)
        if paths and all(p.exists() and p.stat().st_size >= 8 for p in paths):
            print("[heal] expected files landed", flush=True)
            return
        time.sleep(20)
    print("[heal] wait budget spent — re-audit anyway", flush=True)


def audit(job: str, date: str, source: str, target: str, book: str,
          skip_probe: bool = False) -> Report:
    restore_persist(date, target, source, book)
    if job in ("all", "preopen", "scrape"):
        _pull_scrape_paths(date)
    if job in ("all", "postclose", "afternoon"):
        _pull_book_paths(book)
    report = Report(
        job=job, date=date, source_date=source, target_date=target,
        book_date=book, generated_at=datetime.now(ET).isoformat(),
    )
    print("=" * 72, flush=True)
    print(f"  PIPELINE HEALTH  job={job}", flush=True)
    print(f"  preopen={date}  postclose {source} → {target}  book={book}",
          flush=True)
    print("=" * 72, flush=True)
    check_runtime(report, skip_probe=skip_probe)
    if job in ("all", "preopen", "postclose"):
        check_clocks(report, job, date, source, book)
    if job in ("all", "preopen", "scrape"):
        check_scrape(report, date)
    if job in ("all", "postclose", "preopen"):
        check_postclose(report, source, target)
    if job in ("all", "preopen"):
        check_preopen(report, date)
    if job in ("all", "postclose", "afternoon"):
        check_bookchain(report, book)
        check_outcomes(report, book)
        check_learning(report, book)
        check_pages(report, book)
    return report


def render(report: Report, fix_actions: list[str] | None = None) -> str:
    lines = [
        f"# Pipeline health — {report.job}",
        "",
        f"pre-open date={report.date}  post-close source={report.source_date}  "
        f"post-close target={report.target_date}  book={report.book_date}",
        f"generated {report.generated_at}  round={report.round}",
        f"**result={'PASS' if report.ok else 'FAIL'}**  "
        f"required_fails={report.n_fail}  warns={report.n_warn}",
        "",
        "Heal loop: audit → fix OpenClaw door / timers on this box → "
        "start systemd or spawn the owning ECS job (ubuntu workflows are "
        "GH-dispatched with force=true) → wait for files → re-audit. "
        "Finviz HTML is never scraped on ECS. xAI OAuth dies ~6h and cannot "
        "be refreshed (Cloudflare). Permanent auth is XAI_API_KEY in "
        "`~/.openclaw/.env`. An expiring token does not block Grok jobs.",
        "",
        "| status | step | group | required | detail | path |",
        "| --- | --- | --- | --- | --- | --- |",
    ]
    for c in report.checks:
        det = (c.detail or "").replace("|", "/")
        path = (c.path or "").replace("|", "/")
        lines.append(
            f"| {c.status} | {c.name} | {c.group} | "
            f"{'yes' if c.required else 'no'} | {det} | `{path}` |"
        )
    lines += ["", "## Fix actions", ""]
    if not fix_actions:
        lines.append("_none this run_")
    else:
        lines += [f"- {a}" for a in fix_actions]
    humans = _human_fails(report)
    if humans:
        lines += ["", "## Human-only (cannot auto-heal)", ""]
        lines += [f"- `{c.step}`: {c.detail}" for c in humans]
    lines.append("")
    return "\n".join(lines) + "\n"


def write_report(report: Report) -> None:
    out = ROOT / "01_daily"
    out.mkdir(parents=True, exist_ok=True)
    tag = report.job if report.job in ("preopen", "postclose") else "all"
    md = out / f"{report.date}_pipeline_health_{tag}.md"
    js = out / f"{report.date}_pipeline_health_{tag}.json"
    md2 = out / f"{report.date}_pipeline_health.md"
    js2 = out / f"{report.date}_pipeline_health.json"
    payload = {
        "job": report.job, "date": report.date,
        "source_date": report.source_date, "target_date": report.target_date,
        "book_date": report.book_date,
        "generated_at": report.generated_at, "ok": report.ok,
        "n_fail": report.n_fail, "n_warn": report.n_warn,
        "round": report.round,
        "fix_actions": report.fix_actions,
        "checks": [asdict(c) for c in report.checks],
    }
    body = json.dumps(payload, indent=2)
    text = render(report, report.fix_actions)
    for p in (js, js2):
        p.write_text(body, encoding="utf-8")
    for p in (md, md2):
        p.write_text(text, encoding="utf-8")
    write_reauth_status(report)
    print(f"[health] wrote {md} and {md2}", flush=True)


def run(job: str, date: str | None, source: str | None, target: str | None,
        write: bool, fix: bool = True) -> Report:
    job = pick_job(job)
    os.environ.setdefault("GROK_ONLY", "0")
    session, src, tgt, book = packet_dates(job, date=date, source=source, target=target)
    budget = int(os.environ.get("HEALTH_HEAL_SECONDS") or (
        "10800" if job == "postclose" else "9000"))
    rounds = int(os.environ.get("HEALTH_HEAL_ROUNDS") or "16")
    wait_s = int(os.environ.get("HEALTH_HEAL_WAIT") or "180")
    deadline = time.time() + budget
    all_actions: list[str] = []
    ever_started: set[str] = set()
    report = audit(job, session, src, tgt, book, skip_probe=grok_busy())
    report.round = 1
    if not fix or report.ok:
        report.fix_actions = all_actions or None
        if write:
            write_report(report)
        print("\n== SUMMARY ==", flush=True)
        print(f"  {'PASS' if report.ok else 'FAIL'}  required_fails={report.n_fail}  "
              f"warns={report.n_warn}", flush=True)
        return report

    for i in range(1, rounds + 1):
        if report.ok or time.time() >= deadline:
            break
        if not _healable(report):
            print("[heal] nothing left to auto-heal — stopping", flush=True)
            break
        print(f"\n== HEAL ROUND {i}/{rounds}  fails={report.n_fail} ==", flush=True)
        all_actions += fix_local(report)
        actions, started = fix_jobs(report, session, src, tgt, book,
                                    already=ever_started)
        ever_started |= started
        all_actions += actions
        if i >= rounds or time.time() >= deadline:
            break
        wait_for_heal(started, session, src, tgt, book,
                      min(wait_s, max(30, int(deadline - time.time()))))
        report = audit(job, session, src, tgt, book, skip_probe=grok_busy())
        report.round = i + 1

    if not report.ok and all_actions:
        report = audit(job, session, src, tgt, book, skip_probe=grok_busy())
        report.round = max(report.round, rounds)
    report.fix_actions = all_actions
    if write:
        write_report(report)
    print("\n== SUMMARY ==", flush=True)
    print(f"  {'PASS' if report.ok else 'FAIL'}  required_fails={report.n_fail}  "
          f"warns={report.n_warn}  heals={len(all_actions)}", flush=True)
    for a in all_actions:
        print(f"  FIX: {a}", flush=True)
    for h in _human_fails(report):
        print(f"  HUMAN: {h.step} {h.detail}", flush=True)
    return report


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--job", default="auto",
                    choices=["auto", "all", "runtime", "door", "preopen", "postclose",
                             "scrape", "afternoon"])
    ap.add_argument("--date", default=None, help="pre-open session (ET today)")
    ap.add_argument("--source-date", default=None,
                    help="completed session (post-close night / book date)")
    ap.add_argument("--target-date", default=None,
                    help="next session (post-close file date); default = --date")
    ap.add_argument("--write", action="store_true")
    ap.add_argument("--fix", action="store_true", default=False,
                    help="heal FAILs (default OFF — audit only)")
    ap.add_argument("--no-fix", action="store_true",
                    help="audit only, do not start/dispatch anything")
    args = ap.parse_args()
    fix = False if args.no_fix else bool(args.fix)
    report = run(args.job, args.date, args.source_date, args.target_date,
                 args.write, fix=fix)
    # Always 0: the report is the product. Occupying ECS to "heal"
    # is what caused the missing files this job then failed on.
    raise SystemExit(0)


if __name__ == "__main__":
    main()
