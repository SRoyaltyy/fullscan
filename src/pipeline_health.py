"""Read-only auditor: did pre-open / post-close actually run, and is the packet good?

This module does NOT start OpenClaw, does NOT scrape Finviz, does NOT
write predicts. It only inspects what is already on disk (plus a live
PONG if the gateway happens to be up).

Trash that counts as FAIL (not a green check):
  missing / empty / garbled JSON
  timeout / connection-refused stubs
  carry-forward / pass-over from another date
  explicit skip (morning_bootstrap, maps not scraped)
  DeepSeek/SearXNG fallback when the file claims Grok

CLI:
  python -m src.pipeline_health [--date YYYY-MM-DD] [--write]
  python -m src.pipeline_health --job postclose --target-date YYYY-MM-DD
  python -m src.pipeline_health --job preopen --date YYYY-MM-DD
"""
from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import socket
import subprocess
import urllib.error
import urllib.request
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from . import config, output_qc
from .map_heat_postclose import next_weekday
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

# GitHub workflows that must have fired for a complete day.
# (workflow file, human title, required, audit-date role)
# role "preopen" = judged against the pre-open date; "postclose" = the night before.
WORKFLOWS = [
    ("finviz_preopen_scrape.yml", "Finviz pre-open scrape (GH-hosted Elite)", True, "preopen"),
    ("map_heat_postclose.yml", "Map heat captain research (post-close)", True, "postclose"),
    ("preopen_all.yml", "Pre-Open ALL", True, "preopen"),
    ("collect-news-rss.yml", "Collect: RSS News", False, "preopen"),
    ("collect-news-newsapi.yml", "Collect: NewsAPI", False, "preopen"),
    ("collect-reddit.yml", "Collect: Reddit", False, "preopen"),
    ("collect-market-yfinance.yml", "Collect: yfinance Market Data", False, "preopen"),
    ("collect-macro-fred.yml", "Collect: FRED Macro", False, "preopen"),
    ("collect-catalyst.yml", "Collect: Catalyst", False, "preopen"),
    ("insider_fetch.yml", "Insider Fetch (Finviz B2)", False, "preopen"),
    ("events_daily.yml", "Event Scanner", True, "preopen"),
    ("news_judge.yml", "News judge", True, "preopen"),
    ("label_weather.yml", "Label + Weather", True, "preopen"),
    ("ab_checklist.yml", "A+B1 Checklist", False, "preopen"),
    ("catalyst_daily.yml", "Catalyst daily (bounded dossiers)", False, "preopen"),
    ("daily_pipeline.yml", "Autonomous Daily Market Pipeline", True, "preopen"),
    ("sector_daily.yml", "Sector Daily (auto)", True, "preopen"),
    ("learn_cycle.yml", "Learn Cycle", True, "preopen"),
    ("news_grade.yml", "News Actions Grader", True, "preopen"),
    ("hit_board.yml", "HIT Board", False, "preopen"),
    ("stock_book_all.yml", "Stock Book ALL (one-shot)", True, "preopen"),
    ("deploy-dashboard.yml", "Deploy dashboard to Pages", False, "preopen"),
]


@dataclass
class Check:
    step: str           # stable id, e.g. postclose.map_heat
    name: str
    group: str          # runtime | clock | scrape | postclose | preopen
    status: str         # OK | FAIL | WARN
    required: bool
    detail: str = ""
    path: str = ""


@dataclass
class Report:
    job: str
    date: str
    source_date: str
    target_date: str
    generated_at: str
    checks: list[Check] = field(default_factory=list)
    fix_actions: list[str] | None = None

    @property
    def n_fail(self) -> int:
        return sum(1 for c in self.checks if c.status == "FAIL" and c.required)

    @property
    def n_warn(self) -> int:
        return sum(1 for c in self.checks if c.status == "WARN")

    @property
    def ok(self) -> bool:
        return self.n_fail == 0


def _today() -> str:
    return datetime.now(ET).date().isoformat()


def _prev_weekday(date: str) -> str:
    d = datetime.fromisoformat(date).date() - timedelta(days=1)
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d.isoformat()


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
            PERSIST / "01_daily" / "_transcripts",
            PERSIST / "data" / "catalyst",
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


def _payload_dates(data: dict) -> list[str]:
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
             required: bool, expected_date: str, kind: str = "file",
             qc=None) -> None:
    """One on-disk artifact: missing / empty / garbled / timeout / stale / QC."""
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
                           text=True, timeout=8)
        return (r.stdout or r.stderr or "").strip()
    except (OSError, subprocess.SubprocessError):
        return ""


def _models_status() -> str:
    try:
        r = subprocess.run(["openclaw", "models", "status"], capture_output=True,
                           text=True, timeout=20,
                           env={**os.environ, "HOME": os.environ.get("HOME") or "/home/gha"})
        return (r.stdout or "") + "\n" + (r.stderr or "")
    except (OSError, subprocess.SubprocessError) as e:
        return str(e)


def _gh_runs(date_map: dict[str, str]) -> list[dict]:
    """date_map: workflow file -> ET date to audit against."""
    token = os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN") or ""
    repo = os.environ.get("GITHUB_REPOSITORY") or "SRoyaltyy/fullscan"
    if not token:
        return []
    out = []
    for wf, title, _req, _role in WORKFLOWS:
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


# ---------------------------------------------------------------------------
# A. Runtime (observational — never starts anything)
# ---------------------------------------------------------------------------

def check_runtime(report: Report) -> None:
    print("\n== A. RUNTIME (observational, nothing is started) ==", flush=True)
    home = os.environ.get("HOME") or ""
    _add(report, step="runtime.home", name="HOME is /home/gha on ECS",
         group="runtime",
         status="OK" if home.rstrip("/") == "/home/gha" else "WARN",
         required=False, detail=f"HOME={home!r}")

    live, live_n = _live_token()
    env = os.environ.get("OPENCLAW_TOKEN") or ""
    if live_n == LIVE_TOKEN_LEN and env and len(env) == STALE_SECRET_LEN and live != env:
        _add(report, step="runtime.token", name="OpenClaw token (48 json vs 64 secret)",
             group="runtime", status="WARN", required=False,
             detail=f"MISMATCH json_len={live_n} tail={_tail(live)} "
                    f"secret_len={len(env)} tail={_tail(env)} — json is the live one")
    elif live_n == 0 and len(env) == STALE_SECRET_LEN:
        _add(report, step="runtime.token", name="OpenClaw token (48 json vs 64 secret)",
             group="runtime", status="FAIL", required=True,
             detail="only 64-char GitHub secret present — 401 / SearXNG path")
    elif live_n == LIVE_TOKEN_LEN or (live_n == 0 and len(env) == LIVE_TOKEN_LEN):
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

    if _port_open():
        _add(report, step="runtime.port", name="OpenClaw port 18789 listening",
             group="runtime", status="OK", required=True, detail=GW)
        ping = _chat_ping(live or env)
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
        _add(report, step="runtime.port", name="OpenClaw port 18789 listening",
             group="runtime", status="FAIL", required=True,
             detail="connection refused (observational — health does not start it)")
        _add(report, step="runtime.pong", name="OpenClaw PONG",
             group="runtime", status="FAIL", required=True, detail="port down")
        _add(report, step="runtime.model", name="Classroom model is Grok not DeepSeek",
             group="runtime", status="FAIL", required=True, detail="port down")

    st = _models_status().lower()
    if "expir" in st and "xai" in st:
        _add(report, step="runtime.oauth", name="xAI OAuth not expired",
             group="runtime", status="FAIL", required=True,
             detail="xAI token expiring/expired")
    elif "xai" in st:
        _add(report, step="runtime.oauth", name="xAI OAuth not expired",
             group="runtime", status="OK", required=True, detail="xai profile present")
    else:
        _add(report, step="runtime.oauth", name="xAI OAuth not expired",
             group="runtime", status="WARN", required=False,
             detail="could not parse openclaw models status")

    grok_only = config.grok_only()
    _add(report, step="runtime.grok_only", name="GROK_ONLY (this health process)",
         group="runtime",
         status="OK" if grok_only else "WARN", required=False,
         detail="on" if grok_only else "off here — post-close/pre-open yml must set GROK_ONLY=1")


# ---------------------------------------------------------------------------
# B. Did the clocks / GitHub jobs fire
# ---------------------------------------------------------------------------

def check_clocks(report: Report, preopen_date: str, postclose_night: str) -> None:
    print("\n== B. DID THE JOBS FIRE ==", flush=True)
    for unit, label, req in (
        ("fullscan-preopen.timer", "systemd pre-open timer enabled", False),
        ("fullscan-preopen.service", "systemd pre-open service (now)", False),
        ("fullscan-map-postclose.timer", "systemd post-close timer enabled", False),
        ("fullscan-openclaw-gateway.service", "OpenClaw gateway unit", False),
    ):
        if "timer" in unit:
            val = _systemctl("is-enabled", unit)
            ok = val in ("enabled", "enabled-runtime")
        else:
            val = _systemctl("is-active", unit)
            ok = val == "active"
        _add(report, step=f"clock.{unit}", name=label, group="clock",
             status="OK" if ok else "WARN", required=req,
             detail=val or "not found")

    clock = ROOT / "01_daily" / "_ecs_clock.md"
    artifact(report, step="clock.ecs_clock", name="ECS clock file written",
             group="clock", path=clock, required=False, expected_date=preopen_date)

    pairs = [(wf, title, postclose_night if role == "postclose" else preopen_date,
              req)
             for wf, title, req, role in WORKFLOWS]
    token = os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN") or ""
    if not token:
        _add(report, step="clock.gh", name="GitHub Actions runs for this ET date",
             group="clock", status="WARN", required=False,
             detail="no GITHUB_TOKEN — cannot list runs")
        return
    date_map = {wf: (postclose_night if role == "postclose" else preopen_date)
                for wf, _t, _req, role in WORKFLOWS}
    rows_by_wf = {r.get("wf"): r for r in _gh_runs(date_map)}
    for wf, title, when, req in pairs:
        row = rows_by_wf.get(wf) or {"wf": wf, "title": title, "n": 0, "latest": None}
        latest = row.get("latest") or {}
        if row.get("error"):
            _add(report, step=f"clock.{wf}", name=title, group="clock",
                 status="WARN", required=False, detail=row["error"])
            continue
        if not latest:
            _add(report, step=f"clock.{wf}", name=f"{title} ran on {when}",
                 group="clock", status="FAIL" if req else "WARN", required=req,
                 detail=f"DID NOT RUN on {when} (n=0)")
            continue
        conc = latest.get("conclusion") or latest.get("status") or "?"
        st = "OK" if conc == "success" else (
            "WARN" if conc in ("in_progress", "queued", None) else "FAIL")
        _add(report, step=f"clock.{wf}", name=f"{title} ran on {when}",
             group="clock", status=st if req or st == "OK" else "WARN",
             required=req,
             detail=f"n={row.get('n')} latest={conc} event={latest.get('event')}",
             path=latest.get("html_url") or "")


# ---------------------------------------------------------------------------
# C. Finviz scrape (05:40 GH-hosted) — inputs to pre-open
# ---------------------------------------------------------------------------

def check_scrape(report: Report, date: str) -> None:
    print(f"\n== C. FINVIZ PRE-OPEN SCRAPE (GH-hosted, date {date}) ==", flush=True)
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
        _add(report, step="scrape.econ", name="Econ calendar rows",
             group="scrape",
             status="OK" if (data.get("econ") or []) else "WARN",
             required=False, detail=f"n={len(data.get('econ') or [])}")
        _add(report, step="scrape.earnings", name="Earnings calendar rows",
             group="scrape",
             status="OK" if (data.get("earnings") or []) else "WARN",
             required=False, detail=f"n={len(data.get('earnings') or [])}")
        _add(report, step="scrape.ticker_news", name="Ticker-tagged Finviz news",
             group="scrape",
             status="OK" if (data.get("ticker_news") or []) else "WARN",
             required=False, detail=f"n={len(data.get('ticker_news') or [])}")
        notes = str(data.get("notes") or data.get("one_paragraph") or "")
        if "not scraped" in notes.lower() or "elite session empty" in notes.lower():
            _add(report, step="scrape.skipped", name="Maps actually scraped (not skipped)",
                 group="scrape", status="FAIL", required=True,
                 detail="notes say maps were not scraped / Elite 403")


# ---------------------------------------------------------------------------
# D. Post-close (night of source → files dated target)
# ---------------------------------------------------------------------------

def check_postclose(report: Report, source: str, target: str) -> None:
    print(f"\n== D. POST-CLOSE (night of {source} → files dated {target}) ==",
          flush=True)
    heat = ROOT / "01_daily" / "map_heat"
    tr = ROOT / "01_daily" / "_transcripts"
    artifact(report, step="postclose.map_heat_json",
             name=f"{target}_map_heat.json (industry groups + captains)",
             group="postclose", path=heat / f"{target}_map_heat.json",
             required=True, expected_date=target, qc=output_qc.qc_map_heat)
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
        _add(report, step="postclose.coverage",
             name="Captain coverage ≥ 90%", group="postclose",
             status="OK" if (cov is None or float(cov) >= 0.90) and n >= 20 else "FAIL",
             required=True, detail=f"cards={n} coverage={cov}")
        _add(report, step="postclose.opportunities",
             name="Opportunities / vetoes / parent_splits present",
             group="postclose",
             status="OK" if (data.get("opportunities") or data.get("parent_splits")) else "WARN",
             required=False,
             detail=f"opp={len(data.get('opportunities') or [])} "
                    f"veto={len(data.get('vetoes') or [])} "
                    f"splits={len(data.get('parent_splits') or [])}")
        traces = list(heat.glob(f"{target}_postclose_*_trace.md"))
        _add(report, step="postclose.traces",
             name="Per-sector Grok traces on disk", group="postclose",
             status="OK" if traces else "WARN", required=False,
             detail=f"n={len(traces)}")
    trans = list(tr.glob(f"{target}_map_postclose_*.json")) if tr.is_dir() else []
    _add(report, step="postclose.transcripts",
         name="Post-close LLM transcripts", group="postclose",
         status="OK" if trans else "WARN", required=False,
         detail=f"n={len(trans)}")
    # Fallback path in the packet
    blob = _read(heat / f"{target}_research_baseline.md") + _read(base) if base.exists() else ""
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


# ---------------------------------------------------------------------------
# E. Pre-open ALL — every step
# ---------------------------------------------------------------------------

def _slug(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", s.lower()).strip("_")


def check_preopen(report: Report, date: str) -> None:
    print(f"\n== E. PRE-OPEN ALL (every step, date {date}) ==", flush=True)
    news = ROOT / "01_daily" / "news"
    ev = ROOT / "01_daily" / "events"
    heat = ROOT / "01_daily" / "map_heat"
    cat = ROOT / "01_daily" / "catalyst"
    gen = ROOT / "01_daily" / "general"
    sec = ROOT / "01_daily" / "sectors" / date

    # 1. consume scrape (already section C) — still record as preopen prereq
    artifact(report, step="preopen.in_digest",
             name="INPUT: Finviz digest from 05:40 scrape",
             group="preopen", path=news / f"{date}_finviz_digest.json",
             required=True, expected_date=date, qc=output_qc.qc_finviz_digest)
    artifact(report, step="preopen.in_baseline",
             name="INPUT: last-night captain baseline",
             group="preopen", path=heat / f"{date}_research_baseline.json",
             required=True, expected_date=date, qc=output_qc.qc_map_heat_baseline)

    # 2. news parse
    artifact(report, step="preopen.news_parse_json",
             name="News parse JSON", group="preopen",
             path=news / f"{date}_parsed.json", required=True,
             expected_date=date, qc=output_qc.qc_news_parse)
    artifact(report, step="preopen.news_parse_md",
             name="News parse MD", group="preopen",
             path=news / f"{date}_parsed.md", required=False,
             expected_date=date)

    # 3. events + 4. catcher
    evj = ev / f"{date}_events.json"
    artifact(report, step="preopen.events",
             name="Event scanner (primary, NOT carry)", group="preopen",
             path=evj, required=True, expected_date=date,
             qc=output_qc.qc_events_path)
    artifact(report, step="preopen.events_md",
             name="Events MD", group="preopen",
             path=ev / f"{date}_events.md", required=False, expected_date=date)
    data = _json(evj) if evj.exists() else None
    if isinstance(data, dict):
        catcher = data.get("catcher") or {}
        if catcher.get("ran"):
            _add(report, step="preopen.catcher",
                 name="Event catcher second pass ran", group="preopen",
                 status="OK", required=True,
                 detail=f"found={catcher.get('found')} replaced={catcher.get('replaced_primary')}")
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
        elif isinstance(latest, dict):
            _add(report, step="preopen.events_latest",
                 name="events/latest.json points at today", group="preopen",
                 status="OK", required=False, detail=f"scan_date={latest.get('scan_date')}")

    # 5. news judge
    artifact(report, step="preopen.judge_md",
             name="News judge MD", group="preopen",
             path=news / f"{date}_judge.md", required=True,
             expected_date=date, qc=output_qc.qc_news_judge)
    artifact(report, step="preopen.judge_json",
             name="News judge JSON (parsed tilts)", group="preopen",
             path=news / f"{date}_judge.json", required=False,
             expected_date=date)

    # 6. morning captain refresh
    artifact(report, step="preopen.research_json",
             name="Map-heat morning refresh JSON", group="preopen",
             path=heat / f"{date}_research.json", required=True,
             expected_date=date)
    artifact(report, step="preopen.research_md",
             name="Map-heat morning refresh MD", group="preopen",
             path=heat / f"{date}_research.md", required=True,
             expected_date=date, qc=output_qc.qc_map_heat_research)

    # 7. news actions
    artifact(report, step="preopen.actions",
             name="News actions JSON", group="preopen",
             path=news / f"{date}_actions.json", required=False,
             expected_date=date, qc=output_qc.qc_news_actions)

    # 8. catalyst
    dj = cat / f"{date}_dossiers.json"
    artifact(report, step="preopen.catalyst_json",
             name="Catalyst dossiers JSON", group="preopen",
             path=dj, required=False, expected_date=date)
    artifact(report, step="preopen.catalyst_md",
             name="Catalyst dossiers MD", group="preopen",
             path=cat / f"{date}_dossiers.md", required=False, expected_date=date)
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
                 status="FAIL", required=False,
                 detail=f"SearXNG/other backend on {len(searx)} dossiers, grok_native={len(native)}")
        elif native:
            _add(report, step="preopen.catalyst_backend",
                 name="Catalyst search_backend=grok_native", group="preopen",
                 status="OK", required=False, detail=f"grok_native={len(native)}/{len(rows)}")

    # 9. general predict
    artifact(report, step="preopen.general",
             name="General market predict", group="preopen",
             path=gen / f"{date}_predict.md", required=True,
             expected_date=date, qc=output_qc.qc_general_predict)

    # 10. 11 sector predicts
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

    # 11. sector board
    artifact(report, step="preopen.board",
             name="Sector board JSON", group="preopen",
             path=sec / "_board.json", required=False, expected_date=date)

    # 12. output_qc + status + grok review
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
             detail=f"all_ok={st.get('all_ok')} grok_ok={st.get('grok_ok')} "
                    f"missing={st.get('missing_required')}")
    artifact(report, step="preopen.status_md",
             name="Pre-open status MD", group="preopen",
             path=ROOT / "01_daily" / f"{date}_preopen_status.md",
             required=False, expected_date=date)
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
    artifact(report, step="preopen.grok_review_md",
             name="Grok text review MD", group="preopen",
             path=ROOT / "01_daily" / f"{date}_grok_review.md",
             required=False, expected_date=date)


# ---------------------------------------------------------------------------
# F. Stock-book chain (afternoon) — the ranker and everything it eats
# ---------------------------------------------------------------------------

def _file_contains(path: Path, needle: str) -> bool:
    try:
        return needle in path.read_text(encoding="utf-8", errors="ignore")
    except OSError:
        return False


def check_bookchain(report: Report, date: str) -> None:
    print(f"\n== F. STOCK BOOK CHAIN (date {date}) ==", flush=True)
    artifact(report, step="book.finviz", name="INPUT: Finviz Elite export",
             group="book", path=ROOT / "data" / "exports" / f"finviz_{date}.csv",
             required=True, expected_date=date)
    artifact(report, step="book.membership", name="Universe labels (segments)",
             group="book",
             path=ROOT / "data" / "universe" / f"{date}_membership.csv",
             required=True, expected_date=date)
    artifact(report, step="book.weather", name="Weather / regime JSON",
             group="book",
             path=ROOT / "01_daily" / "weather" / f"{date}_weather.json",
             required=True, expected_date=date)
    artifact(report, step="book.join", name="Join ranked CSV", group="book",
             path=ROOT / "data" / "join" / f"{date}_ranked.csv",
             required=True, expected_date=date)
    artifact(report, step="book.input_health", name="Input-health preflight",
             group="book",
             path=ROOT / "data" / "stock_book" / f"{date}_input_health.json",
             required=False, expected_date=date)
    artifact(report, step="book.book_json", name="Stock book JSON (5 horizons)",
             group="book",
             path=ROOT / "data" / "stock_book" / f"{date}_stock_book.json",
             required=True, expected_date=date)
    artifact(report, step="book.book_md", name="Stock book MD", group="book",
             path=ROOT / "01_daily" / f"{date}_stock_book.md",
             required=True, expected_date=date)
    bt = ROOT / "03_scoreboard" / "STOCK_BOOK_BACKTEST.md"
    artifact(report, step="book.backtest", name="Stock book backtest (repo-level)",
             group="book", path=bt, required=True, expected_date=date)
    if bt.exists() and not _file_contains(bt, date):
        _add(report, step="book.backtest_fresh", name="Backtest refreshed today",
             group="book", status="WARN", required=False,
             detail=f"no '{date}' inside backtest md")
    artifact(report, step="book.paper_md", name="Paper trading summary",
             group="book", path=ROOT / "03_scoreboard" / "PAPER_TRADING.md",
             required=True, expected_date=date)
    artifact(report, step="book.paper_curve", name="Paper equity curve CSV",
             group="book", path=ROOT / "data" / "paper" / "equity_curve.csv",
             required=True, expected_date=date)
    artifact(report, step="book.dashboard", name="Dashboard HTML (Pages source)",
             group="book", path=ROOT / "dashboard" / "index.html",
             required=True, expected_date=date)
    artifact(report, step="book.learn_ledger", name="BOOK_LEARN weight ledger",
             group="book", path=ROOT / "03_scoreboard" / "BOOK_LEARN.md",
             required=False, expected_date=date)
    artifact(report, step="book.gaps", name="BOOK_GAPS blind-spot scan",
             group="book", path=ROOT / "03_scoreboard" / "BOOK_GAPS.md",
             required=False, expected_date=date)


# ---------------------------------------------------------------------------
# G. Outcome / reflect / grading (evening)
# ---------------------------------------------------------------------------

def check_outcomes(report: Report, date: str) -> None:
    print(f"\n== G. OUTCOME / REFLECT / GRADING (date {date}) ==", flush=True)
    gen = ROOT / "01_daily" / "general"
    artifact(report, step="outcome.general", name="General outcome (graded call)",
             group="outcome", path=gen / f"{date}_outcome.md",
             required=True, expected_date=date)
    artifact(report, step="outcome.reflect_trace", name="Reflect ran (trace)",
             group="outcome", path=gen / f"{date}_reflect_trace.md",
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
    artifact(report, step="outcome.horizon_board",
             name="HORIZON_BOARD (multi-timeframe grades)", group="outcome",
             path=ROOT / "03_scoreboard" / "HORIZON_BOARD.md",
             required=False, expected_date=date)


# ---------------------------------------------------------------------------
# H. Learning loop products
# ---------------------------------------------------------------------------

def check_learning(report: Report, date: str) -> None:
    print(f"\n== H. LEARNING LOOP (date {date}) ==", flush=True)
    lm = ROOT / "03_scoreboard" / "LEARNINGS.md"
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
    artifact(report, step="learn.efficacy", name="LESSON_EFFICACY.md",
             group="learn", path=ROOT / "03_scoreboard" / "LESSON_EFFICACY.md",
             required=False, expected_date=date)
    artifact(report, step="learn.book_policy",
             name="book_policy.json (learned ranker weights)", group="learn",
             path=ROOT / "00_grounding" / "book_policy.json",
             required=False, expected_date=date)


# ---------------------------------------------------------------------------
# I. Live dashboard on GitHub Pages
# ---------------------------------------------------------------------------

PAGES_URL = "https://sroyaltyy.github.io/fullscan/dashboard/"


def check_pages(report: Report, date: str) -> None:
    print("\n== I. DASHBOARD ON GITHUB PAGES ==", flush=True)
    try:
        req = urllib.request.Request(PAGES_URL, headers={
            "User-Agent": "fullscan-health",
            "Cache-Control": "no-cache",
        })
        with urllib.request.urlopen(req, timeout=15) as resp:
            code = resp.getcode()
            body = resp.read(300000).decode("utf-8", "ignore")
        injected = "__DATA__" not in body
        _add(report, step="pages.dashboard", name="Live dashboard reachable + data injected",
             group="pages", status="OK" if (code == 200 and injected) else "FAIL",
             required=True,
             detail=f"HTTP {code}, data_injected={injected}", path=PAGES_URL)
    except (urllib.error.URLError, TimeoutError, OSError) as e:
        _add(report, step="pages.dashboard", name="Live dashboard reachable + data injected",
             group="pages", status="FAIL", required=True,
             detail=str(e)[:160], path=PAGES_URL)


# ---------------------------------------------------------------------------
# J. FIX MODE — re-dispatch the workflow that owns each required FAIL
# ---------------------------------------------------------------------------

# step prefix -> owning workflow (None = not auto-fixable)
FIX_MAP = [
    ("postclose.", "map_heat_postclose.yml"),
    ("scrape.", "finviz_preopen_scrape.yml"),
    ("preopen.", "preopen_all.yml"),
    ("book.", "stock_book_all.yml"),
    ("outcome.general", "daily_pipeline.yml"),
    ("outcome.reflect", "daily_pipeline.yml"),
    ("outcome.candidate", "daily_pipeline.yml"),
    ("outcome.sector", "sector_daily.yml"),
    ("outcome.horizon", "daily_pipeline.yml"),
    ("learn.", "learn_cycle.yml"),
    ("pages.", "deploy-dashboard.yml"),
]
# clock.<workflow>.yml steps map onto themselves.
FIX_STATE = ROOT / "data" / "health" / "fix_state.json"


def _workflow_for_step(step: str) -> str | None:
    if step.startswith("clock.") and step.endswith(".yml"):
        wf = step[len("clock."):]
        return None if wf == "pipeline_health.yml" else wf
    for prefix, wf in FIX_MAP:
        if step.startswith(prefix):
            return wf
    return None


def _gh_request(path: str, method: str = "GET", payload: dict | None = None):
    token = os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN") or ""
    repo = os.environ.get("GITHUB_REPOSITORY") or "SRoyaltyy/fullscan"
    if not token:
        return None, "no GITHUB_TOKEN"
    req = urllib.request.Request(
        f"https://api.github.com/repos/{repo}{path}", method=method,
        headers={"Authorization": f"Bearer {token}",
                 "Accept": "application/vnd.github+json",
                 "User-Agent": "fullscan-health"},
        data=json.dumps(payload).encode() if payload is not None else None)
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            return resp.getcode(), ""
    except urllib.error.HTTPError as e:
        return e.code, str(e)[:120]
    except (urllib.error.URLError, TimeoutError, OSError) as e:
        return None, str(e)[:120]


def _gh_workflow_busy(wf: str) -> bool:
    token = os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN") or ""
    repo = os.environ.get("GITHUB_REPOSITORY") or "SRoyaltyy/fullscan"
    if not token:
        return True  # cannot verify -> do not risk a duplicate dispatch
    req = urllib.request.Request(
        f"https://api.github.com/repos/{repo}/actions/workflows/{wf}/runs?per_page=5",
        headers={"Authorization": f"Bearer {token}",
                 "Accept": "application/vnd.github+json",
                 "User-Agent": "fullscan-health"})
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            payload = json.loads(resp.read().decode())
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError, OSError):
        return True
    return any(r.get("status") in ("queued", "in_progress")
               for r in payload.get("workflow_runs") or [])


def fix_failures(report: Report, date: str) -> list[str]:
    """Guarded auto-fix: for every required FAIL with an owning workflow,
    re-dispatch that workflow — at most once per step per day, never when
    the workflow is already queued/running, never pipeline_health itself."""
    actions: list[str] = []
    state = _json(FIX_STATE) if FIX_STATE.exists() else None
    if not isinstance(state, dict):
        state = {}
    today = dict(state.get(date) or {})
    dispatched: set[str] = set()
    for c in report.checks:
        if c.status != "FAIL" or not c.required:
            continue
        wf = _workflow_for_step(c.step)
        if not wf:
            continue
        if c.step in today:
            print(f"[fix] {c.step}: already re-dispatched today — skip", flush=True)
            continue
        if wf in dispatched:
            continue
        if _gh_workflow_busy(wf):
            print(f"[fix] {wf} queued/running — skip {c.step}", flush=True)
            continue
        code, err = _gh_request(f"/actions/workflows/{wf}/dispatches",
                                method="POST", payload={"ref": "main"})
        if code == 204:
            dispatched.add(wf)
            today[c.step] = datetime.now(ET).isoformat()
            msg = f"re-dispatched {wf} (for {c.step})"
            actions.append(msg)
            print(f"[fix] {c.step} FAIL -> {msg}", flush=True)
        else:
            print(f"[fix] dispatch {wf} failed: {err or code}", flush=True)
    state[date] = today
    FIX_STATE.parent.mkdir(parents=True, exist_ok=True)
    FIX_STATE.write_text(json.dumps(state, indent=2), encoding="utf-8")
    if not actions:
        print("[fix] nothing actionable (no required FAIL with an owner, "
              "or all guarded)", flush=True)
    return actions


def render(report: Report, fix_actions: list[str] | None = None) -> str:
    lines = [
        f"# Pipeline health — {report.job}",
        "",
        f"pre-open date={report.date}  post-close source={report.source_date}  "
        f"post-close target={report.target_date}",
        f"generated {report.generated_at}",
        f"**result={'PASS' if report.ok else 'FAIL'}**  "
        f"required_fails={report.n_fail}  warns={report.n_warn}",
        "",
        "This job **does not run** research, scrape, or OpenClaw. "
        "It only checks whether each step already ran and whether the file is "
        "empty / garbled / a timeout stub / a carry-forward / the wrong date."
        + ("" if fix_actions is None else
           " With `--fix`, required FAILs re-dispatch their owning workflow."),
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
    lines += [
        "",
        "## Fix actions",
        "",
    ]
    if fix_actions is None:
        lines.append("fix mode off (pass `--fix` to re-dispatch failing required steps)")
    elif fix_actions:
        lines += [f"- {a}" for a in fix_actions]
    else:
        lines.append("fix mode on — nothing actionable (guards: ≤1 dispatch/step/day, "
                     "skip busy workflows, required FAILs only)")
    lines += [
        "",
        "## What each job is supposed to produce",
        "",
        "Post-close night of SOURCE writes **TARGET-dated** files "
        "(`_map_heat.json`, `_research_baseline.json`).",
        "GH Finviz scrape ~05:40 ET writes **today’s** digest and overlays tape onto that map heat.",
        "Pre-open 05:55 ET consumes those, then writes parse / events / catcher / judge / "
        "morning refresh / actions / catalyst / general predict / 11 sector predicts / "
        "board / QC / Grok review.",
        "",
    ]
    return "\n".join(lines) + "\n"


def write_report(report: Report) -> None:
    out = ROOT / "01_daily"
    out.mkdir(parents=True, exist_ok=True)
    md = out / f"{report.date}_pipeline_health.md"
    js = out / f"{report.date}_pipeline_health.json"
    payload = {
        "job": report.job, "date": report.date,
        "source_date": report.source_date, "target_date": report.target_date,
        "generated_at": report.generated_at, "ok": report.ok,
        "n_fail": report.n_fail, "n_warn": report.n_warn,
        "fix_actions": report.fix_actions,
        "checks": [asdict(c) for c in report.checks],
    }
    js.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    md.write_text(render(report, report.fix_actions), encoding="utf-8")
    print(f"[health] wrote {md}", flush=True)
    print(f"[health] wrote {js}", flush=True)


def run(job: str, date: str | None, source: str | None, target: str | None,
        write: bool, fix: bool = False) -> Report:
    today = date or _today()
    source = source or _prev_weekday(today)
    target = target or today
    # Evening: last night hasn't written *tomorrow* yet if we pass target=today
    # in the morning. Callers override. Default: morning audit of today's packet
    # = post-close target is TODAY (written last night).
    restore_persist(today, target, source)
    report = Report(
        job=job, date=today, source_date=source, target_date=target,
        generated_at=datetime.now(ET).isoformat(),
    )
    print("=" * 72, flush=True)
    print(f"  PIPELINE HEALTH  job={job}  fix={'on' if fix else 'off'}", flush=True)
    print(f"  preopen={today}  postclose {source} → {target}", flush=True)
    print("=" * 72, flush=True)
    if job in ("all", "door", "runtime"):
        check_runtime(report)
    if job in ("all", "preopen", "postclose"):
        check_clocks(report, today, source)
    if job in ("all", "preopen", "scrape"):
        check_scrape(report, today)
    if job in ("all", "postclose"):
        check_postclose(report, source, target)
    if job in ("all", "preopen"):
        check_preopen(report, today)
    if job in ("all", "afternoon"):
        check_bookchain(report, today)
        check_outcomes(report, today)
        check_learning(report, today)
        check_pages(report, today)
    if fix:
        report.fix_actions = fix_failures(report, today)
    print("\n== SUMMARY ==", flush=True)
    print(f"  {'PASS' if report.ok else 'FAIL'}  required_fails={report.n_fail}  "
          f"warns={report.n_warn}", flush=True)
    if report.fix_actions:
        for a in report.fix_actions:
            print(f"  FIX: {a}", flush=True)
    if write:
        write_report(report)
    return report


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--job", default="all",
                    choices=["all", "runtime", "door", "preopen", "postclose",
                             "scrape", "afternoon"])
    ap.add_argument("--date", default=None, help="pre-open session (ET today)")
    ap.add_argument("--source-date", default=None, help="completed session (post-close night)")
    ap.add_argument("--target-date", default=None,
                    help="next session (post-close file date); default = --date")
    ap.add_argument("--write", action="store_true")
    ap.add_argument("--fix", action="store_true",
                    help="re-dispatch the owning workflow for each required FAIL "
                         "(guarded: <=1/step/day, never when busy, never self)")
    args = ap.parse_args()
    report = run(args.job, args.date, args.source_date, args.target_date,
                 args.write, fix=args.fix)
    raise SystemExit(0 if report.ok else 1)


if __name__ == "__main__":
    main()
