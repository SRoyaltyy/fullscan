"""Fail-closed healthcheck for the pre-open / post-close door.

Prints three blocks on every run:

  1. Door / known bugs  (OpenClaw, Grok token 48-vs-64, auth, SearXNG)
  2. Prerequisites      (files the PREVIOUS job must have already written)
  3. Expected outputs   (files THIS job must write; SKIP in --phase before)

CLI:
  python -m src.pipeline_health --job door|preopen|postclose
                                [--phase before|after]
                                [--date YYYY-MM-DD]
                                [--source-date YYYY-MM-DD]
                                [--target-date YYYY-MM-DD]
                                [--write]

Exit 1 if any required check is FAIL. WARN does not fail the job.

Date math
---------
Post-close on the night of SOURCE (completed session) writes files
dated TARGET (next NYSE weekday). Pre-open on TARGET morning consumes
those TARGET-dated files, overlays futures, then writes the rest of
the TARGET packet.
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
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from . import config, output_qc
from .map_heat_postclose import next_weekday
from .sector_taxonomy import FINVIZ_SECTORS

ROOT = Path(__file__).resolve().parent.parent
ET = ZoneInfo(config.TZ)
GW = (os.environ.get("OPENCLAW_GATEWAY_URL") or "http://127.0.0.1:18789").rstrip("/")
JSON_CFG = Path(os.path.expanduser("~/.openclaw/openclaw.json"))
JSON_CFG_GHA = Path("/home/gha/.openclaw/openclaw.json")

# Live gateway tokens we have seen are 48 chars. Stale GitHub secrets were 64.
LIVE_TOKEN_LEN = 48
STALE_SECRET_LEN = 64


@dataclass
class Check:
    name: str
    group: str          # door | bug | prereq | output
    status: str         # OK | FAIL | WARN | SKIP
    required: bool
    detail: str = ""
    path: str = ""


@dataclass
class Report:
    job: str
    phase: str
    date: str
    source_date: str
    target_date: str
    generated_at: str
    checks: list[Check] = field(default_factory=list)

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


def _add(report: Report, **kw) -> Check:
    c = Check(**kw)
    report.checks.append(c)
    flag = {"OK": "OK  ", "FAIL": "FAIL", "WARN": "WARN", "SKIP": "SKIP"}[c.status]
    req = "" if c.required else " (optional)"
    path = f"  {c.path}" if c.path else ""
    print(f"  [{flag}] {c.name:<42}{req} {c.detail}{path}", flush=True)
    return c


def _exists(path: Path) -> bool:
    return path.exists() and (path.is_dir() or path.stat().st_size > 0)


def _json(path: Path):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, ValueError):
        return None


def _tail(token: str) -> str:
    t = str(token or "")
    if len(t) < 4:
        return "short" if t else "empty"
    return t[-4:]


def _read_live_token() -> tuple[str, Path | None]:
    for p in (JSON_CFG, JSON_CFG_GHA):
        data = _json(p)
        if not isinstance(data, dict):
            continue
        gw = data.get("gateway") or {}
        auth = gw.get("auth") if isinstance(gw.get("auth"), dict) else {}
        token = str(auth.get("token") or gw.get("token") or auth.get("password") or "")
        if token:
            return token, p
    return "", None


def align_openclaw_token() -> dict:
    """Prefer ~/.openclaw/openclaw.json over the GitHub secret (48 vs 64)."""
    live, src = _read_live_token()
    env = os.environ.get("OPENCLAW_TOKEN") or ""
    info = {
        "live_len": len(live),
        "live_tail": _tail(live),
        "env_len": len(env),
        "env_tail": _tail(env),
        "src": str(src) if src else "",
        "mismatch": bool(live and env and live != env),
    }
    if live:
        os.environ["OPENCLAW_TOKEN"] = live
        os.environ["OPENCLAW_GATEWAY_TOKEN"] = live
        config.OPENCLAW_TOKEN = live
        os.environ.setdefault("OPENCLAW_GATEWAY_URL", "http://127.0.0.1:18789")
        config.OPENCLAW_GATEWAY_URL = os.environ["OPENCLAW_GATEWAY_URL"].rstrip("/")
        if os.environ.get("GITHUB_ENV"):
            with open(os.environ["GITHUB_ENV"], "a", encoding="utf-8") as fh:
                fh.write(f"OPENCLAW_TOKEN<<EOF\n{live}\nEOF\n")
                fh.write(f"OPENCLAW_GATEWAY_TOKEN<<EOF\n{live}\nEOF\n")
    return info


def _port_open(host: str = "127.0.0.1", port: int = 18789) -> bool:
    try:
        with socket.create_connection((host, port), timeout=2):
            return True
    except OSError:
        return False


def _chat_ping(token: str, timeout: int = 45) -> dict:
    """POST /v1/chat/completions. Never prints the token."""
    url = f"{GW}/v1/chat/completions"
    body = json.dumps({
        "model": os.environ.get("OPENCLAW_AGENT") or "openclaw/default",
        "messages": [{"role": "user", "content": "Reply with exactly the word PONG"}],
        "max_tokens": 16,
        "temperature": 0,
    }).encode("utf-8")
    req = urllib.request.Request(
        url, data=body, method="POST",
        headers={
            "Authorization": f"Bearer {token}",
            "x-api-key": token,
            "Content-Type": "application/json",
            "Accept": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8", "replace")
            code = resp.status
    except urllib.error.HTTPError as e:
        raw = e.read().decode("utf-8", "replace") if e.fp else ""
        code = e.code
    except (urllib.error.URLError, TimeoutError, OSError) as e:
        return {"http": 0, "error": str(e)[:160], "content": "", "model": ""}
    model = content = ""
    try:
        data = json.loads(raw)
        model = str(data.get("model") or "")
        content = str(((data.get("choices") or [{}])[0].get("message") or {}).get("content") or "")
        err = data.get("error")
        if err and not content:
            return {"http": code, "error": str(err)[:200], "content": "", "model": model}
    except json.JSONDecodeError:
        return {"http": code, "error": raw[:160], "content": "", "model": ""}
    return {"http": code, "content": content.strip(), "model": model, "error": ""}


def _openclaw_models_status() -> str:
    try:
        r = subprocess.run(
            ["openclaw", "models", "status"],
            capture_output=True, text=True, timeout=20,
            env={**os.environ, "HOME": os.environ.get("HOME") or "/home/gha"},
        )
        return (r.stdout or "") + "\n" + (r.stderr or "")
    except (OSError, subprocess.SubprocessError) as e:
        return f"(openclaw cli failed: {e})"


def _systemctl(*args: str) -> str:
    try:
        r = subprocess.run(
            ["systemctl", *args], capture_output=True, text=True, timeout=8,
        )
        return (r.stdout or r.stderr or "").strip()
    except (OSError, subprocess.SubprocessError):
        return ""


def _finviz_ecs_probe() -> tuple[str, str]:
    """Elite HTML from this box. Aliyun historically 403 / Cloudflare."""
    cookie = (
        os.environ.get("FINVIZ_AUTH")
        or os.environ.get("AUTH_TOKEN_FINVIZ")
        or ""
    ).strip()
    url = "https://elite.finviz.com/groups.ashx?g=sector&v=140&o=name"
    headers = {"User-Agent": "Mozilla/5.0 fullscan-health"}
    if cookie:
        headers["Cookie"] = f"auth={cookie}" if "=" not in cookie else cookie
    req = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            raw = resp.read()[:4000]
            code = resp.status
    except urllib.error.HTTPError as e:
        raw = e.read()[:4000] if e.fp else b""
        code = e.code
    except (urllib.error.URLError, TimeoutError, OSError) as e:
        return "FAIL", f"request_error {e}"[:160]
    text = raw.decode("utf-8", "replace").lower()
    if code == 403 or "just a moment" in text or "cf-challenge" in text:
        return "FAIL", f"HTTP {code} cloudflare/403 (ECS Elite HTML is blocked)"
    if "login_submit" in text and "password" in text:
        return "FAIL", f"HTTP {code} landed on login page (cookie dead)"
    if code == 200 and ("spy" in text or "sector" in text or "industry" in text):
        return "OK", f"HTTP {code} elite HTML looks real"
    return "WARN", f"HTTP {code} body_len={len(raw)} (not obviously elite)"


def _slug(sector: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", sector.lower()).strip("_")


def _file_check(report: Report, name: str, group: str, path: Path,
                required: bool, phase: str, qc=None, extra: str = "") -> None:
    """SKIP outputs in --phase before so we don't fail a job that hasn't run."""
    if group == "output" and phase == "before":
        _add(report, name=name, group=group, status="SKIP", required=required,
             detail="not written yet (phase=before)", path=str(path))
        return
    if not path.exists():
        _add(report, name=name, group=group,
             status="FAIL" if required else "WARN",
             required=required, detail="missing" + (f" — {extra}" if extra else ""),
             path=str(path))
        return
    if path.is_file() and path.stat().st_size < 8:
        _add(report, name=name, group=group,
             status="FAIL" if required else "WARN",
             required=required, detail=f"blank ({path.stat().st_size} bytes)",
             path=str(path))
        return
    if qc is not None:
        r = qc(path)
        st = "OK" if r.ok else ("FAIL" if required else "WARN")
        _add(report, name=name, group=group, status=st, required=required,
             detail=(r.reason or "OK"), path=str(path))
        return
    _add(report, name=name, group=group, status="OK", required=required,
         detail=extra or f"{path.stat().st_size} bytes", path=str(path))


# ---------------------------------------------------------------------------
# 1. Door / known bugs
# ---------------------------------------------------------------------------

def check_door(report: Report) -> dict:
    print("\n== 1. DOOR / KNOWN BUGS ==", flush=True)
    tok = align_openclaw_token()

    home = os.environ.get("HOME") or ""
    if home.rstrip("/") == "/home/gha":
        _add(report, name="HOME for OpenClaw/git", group="door", status="OK",
             required=True, detail=home)
    else:
        _add(report, name="HOME for OpenClaw/git", group="bug", status="WARN",
             required=False, detail=f"HOME={home!r} (ECS runner wants /home/gha)")

    live_n, env_n = tok["live_len"], tok["env_len"]
    if not tok["live_len"] and not tok["env_len"]:
        _add(report, name="OpenClaw token present", group="bug", status="FAIL",
             required=True, detail="json and env both empty")
    elif tok["mismatch"]:
        # This is the 64-vs-48 bug. Live json wins after align; still FAIL if
        # we cannot ping with the live token below.
        kind = "STALE_SECRET" if env_n == STALE_SECRET_LEN and live_n == LIVE_TOKEN_LEN else "MISMATCH"
        _add(report, name="Grok/OpenClaw token 48 vs 64", group="bug",
             status="WARN", required=False,
             detail=(f"{kind}: json_len={live_n} tail={tok['live_tail']} "
                     f"env_len={env_n} tail={tok['env_tail']} — using json"))
    elif live_n == LIVE_TOKEN_LEN or env_n == LIVE_TOKEN_LEN:
        _add(report, name="Grok/OpenClaw token 48 vs 64", group="bug",
             status="OK", required=True,
             detail=f"live_len={live_n or env_n} tail={tok['live_tail'] or tok['env_tail']}")
    elif env_n == STALE_SECRET_LEN and live_n == 0:
        _add(report, name="Grok/OpenClaw token 48 vs 64", group="bug",
             status="FAIL", required=True,
             detail=f"only env token len={env_n} (stale 64-char secret, no json token)")
    else:
        _add(report, name="Grok/OpenClaw token 48 vs 64", group="bug",
             status="WARN", required=False,
             detail=f"json_len={live_n} env_len={env_n} (unexpected lengths)")

    looks_ds = False
    ping: dict = {}
    if _port_open():
        _add(report, name="OpenClaw port 18789", group="door", status="OK",
             required=True, detail=f"{GW} listening")
    else:
        _add(report, name="OpenClaw port 18789", group="door", status="FAIL",
             required=True, detail="connection refused — gateway down")
        _add(report, name="OpenClaw PONG (Grok)", group="door", status="FAIL",
             required=True, detail="skipped — port down")
        _add(report, name="Chat model is Grok not DeepSeek", group="bug",
             status="FAIL", required=True, detail="skipped — port down")
    if _port_open():
        token = os.environ.get("OPENCLAW_TOKEN") or ""
        ping = _chat_ping(token)
        if ping.get("http") == 200 and "PONG" in (ping.get("content") or "").upper():
            _add(report, name="OpenClaw PONG (Grok)", group="door", status="OK",
                 required=True, detail=f"http=200 model={ping.get('model') or '?'}")
        elif ping.get("http") == 401:
            _add(report, name="OpenClaw PONG (Grok)", group="door", status="FAIL",
                 required=True,
                 detail="HTTP 401 — token is not the live gateway token (48-vs-64)")
        else:
            _add(report, name="OpenClaw PONG (Grok)", group="door", status="FAIL",
                 required=True,
                 detail=f"http={ping.get('http')} err={ping.get('error') or ping.get('content') or ''}"[:180])
        model = (ping.get("model") or "").lower()
        content = (ping.get("content") or "").lower()
        looks_ds = "deepseek" in model or "deepseek" in content
        looks_grok = "grok" in model or "xai" in model or model.startswith("openclaw/")
        if ping.get("http") == 200 and looks_ds:
            _add(report, name="Chat model is Grok not DeepSeek", group="bug",
                 status="FAIL", required=True,
                 detail=f"model={ping.get('model')} — DeepSeek fallback, SearXNG path")
        elif ping.get("http") == 200 and (looks_grok or "PONG" in (ping.get("content") or "").upper()):
            _add(report, name="Chat model is Grok not DeepSeek", group="bug",
                 status="OK", required=True, detail=f"model={ping.get('model') or 'openclaw/default'}")
        else:
            _add(report, name="Chat model is Grok not DeepSeek", group="bug",
                 status="FAIL", required=True, detail=f"model={ping.get('model') or 'none'}")

    grok_only = config.grok_only()
    if grok_only:
        _add(report, name="GROK_ONLY (no DeepSeek analysis)", group="bug",
             status="OK", required=True,
             detail="GROK_ONLY on — DeepSeek must not write essays")
    else:
        _add(report, name="GROK_ONLY (no DeepSeek analysis)", group="bug",
             status="FAIL", required=True,
             detail="GROK_ONLY off — post-close will silently fall back to DeepSeek/SearXNG")

    searx = (os.environ.get("SEARXNG_URL") or config.SEARXNG_URL or "").strip()
    if grok_only and ping.get("http") == 200 and not looks_ds:
        _add(report, name="SearXNG is not the research path", group="bug",
             status="OK", required=True,
             detail="Grok classroom answered; native web/X should be used. "
                    f"SEARXNG_URL={'set (fallback only)' if searx else 'unset'}")
    elif searx:
        _add(report, name="SearXNG is not the research path", group="bug",
             status="FAIL", required=True,
             detail="Grok did not answer — websearch.py will hit SearXNG first (0 results / DDG)")
    else:
        _add(report, name="SearXNG is not the research path", group="bug",
             status="WARN", required=False, detail="SEARXNG_URL unset and Grok not confirmed")

    status_txt = _openclaw_models_status().lower()
    if "expir" in status_txt and "xai" in status_txt:
        _add(report, name="xAI OAuth not expired", group="bug", status="FAIL",
             required=True, detail="openclaw models status: xAI token expiring/expired")
    elif "missing" in status_txt and "openai" in status_txt and "xai" not in status_txt:
        _add(report, name="xAI OAuth not expired", group="bug", status="FAIL",
             required=True, detail="openai auth missing and no xai profile")
    elif "xai" in status_txt:
        _add(report, name="xAI OAuth not expired", group="bug", status="OK",
             required=True, detail="xai profile present")
    else:
        _add(report, name="xAI OAuth not expired", group="bug", status="WARN",
             required=False, detail="could not parse `openclaw models status`")

    cfg = JSON_CFG if JSON_CFG.exists() else JSON_CFG_GHA
    data = _json(cfg) or {}
    gw = data.get("gateway") or {}
    http = (gw.get("http") or {}).get("endpoints") or {}
    chat_on = bool((http.get("chatCompletions") or {}).get("enabled"))
    _add(report, name="chatCompletions.enabled", group="door",
         status="OK" if chat_on else "FAIL", required=True,
         detail=f"{cfg} enabled={chat_on}")

    default = ""
    agents = data.get("agents") or data
    # default model lives in several shapes across OpenClaw versions
    default = str(
        data.get("agents", {}).get("defaults", {}).get("model")
        or (data.get("agents") or {}).get("model")
        or ""
    )
    if not default:
        try:
            default = str(((data.get("agents") or {}).get("main") or {}).get("model") or "")
        except Exception:
            default = ""
    if "openai" in default.lower() and "grok" not in default.lower() and "xai" not in default.lower():
        _add(report, name="Default model is Grok", group="bug", status="FAIL",
             required=True, detail=f"default={default} (openai missing auth)")
    else:
        _add(report, name="Default model is Grok", group="door", status="OK",
             required=False, detail=f"default={default or 'openclaw/default → xai/grok-4.6'}")

    return tok


# ---------------------------------------------------------------------------
# 2–3. Post-close
# ---------------------------------------------------------------------------

def postclose_paths(target: str) -> dict[str, Path]:
    heat = ROOT / "01_daily" / "map_heat"
    tr = ROOT / "01_daily" / "_transcripts"
    return {
        "map_heat_json": heat / f"{target}_map_heat.json",
        "map_heat_md": heat / f"{target}_map_heat.md",
        "baseline_json": heat / f"{target}_research_baseline.json",
        "baseline_md": heat / f"{target}_research_baseline.md",
        "transcript": tr / f"{target}_map_postclose_synthesis.json",
    }


def check_postclose_prereqs(report: Report, source: str, target: str) -> None:
    print("\n== 2. PREREQUISITES (post-close) ==", flush=True)
    print(f"  night of {source} writes files dated {target} (next session)", flush=True)

    unit = _systemctl("is-active", "fullscan-preopen.service")
    if unit == "active":
        _add(report, name="Not colliding with pre-open", group="prereq",
             status="FAIL", required=True, detail="fullscan-preopen.service is active")
    else:
        _add(report, name="Not colliding with pre-open", group="prereq",
             status="OK", required=True, detail=f"preopen={unit or 'inactive'}")

    st, det = _finviz_ecs_probe()
    _add(report, name="ECS Finviz Elite HTML (groups/tape)", group="prereq",
         status=st, required=True,
         detail=det + " — post-close `map_heat --force` scrapes on ECS")

    # yfinance is used for SPY/sector reaction; best-effort
    try:
        import yfinance  # noqa: F401
        _add(report, name="yfinance import (market reaction)", group="prereq",
             status="OK", required=False, detail="installed")
    except ImportError:
        _add(report, name="yfinance import (market reaction)", group="prereq",
             status="WARN", required=False, detail="missing — reaction context skipped")

    prompt = ROOT / "00_grounding" / "map_heat_research_prompt.md"
    _add(report, name="Captain research prompt on disk", group="prereq",
         status="OK" if prompt.exists() else "FAIL", required=True,
         path=str(prompt),
         detail="ok" if prompt.exists() else "missing")


def check_postclose_outputs(report: Report, target: str, phase: str) -> None:
    print("\n== 3. EXPECTED OUTPUTS (post-close → dated {target}) ==".format(target=target),
          flush=True)
    p = postclose_paths(target)
    _file_check(report, f"{target}_map_heat.json (groups+captains+tape)",
                "output", p["map_heat_json"], True, phase, qc=output_qc.qc_map_heat)
    _file_check(report, f"{target}_map_heat.md",
                "output", p["map_heat_md"], False, phase)
    _file_check(report, f"{target}_research_baseline.json (captain cards)",
                "output", p["baseline_json"], True, phase,
                qc=output_qc.qc_map_heat_baseline)
    _file_check(report, f"{target}_research_baseline.md",
                "output", p["baseline_md"], True, phase)
    _file_check(report, "post-close synthesis transcript",
                "output", p["transcript"], False, phase)


# ---------------------------------------------------------------------------
# 2–3. Pre-open
# ---------------------------------------------------------------------------

def preopen_input_paths(date: str) -> dict[str, Path]:
    """What Thursday 05:55 needs from Wednesday 22:00 + Thursday 05:40 scrape."""
    heat = ROOT / "01_daily" / "map_heat"
    news = ROOT / "01_daily" / "news"
    return {
        "digest_json": news / f"{date}_finviz_digest.json",
        "digest_md": news / f"{date}_finviz_digest.md",
        "map_heat_json": heat / f"{date}_map_heat.json",
        "map_heat_md": heat / f"{date}_map_heat.md",
        "baseline_json": heat / f"{date}_research_baseline.json",
        "baseline_md": heat / f"{date}_research_baseline.md",
    }


def preopen_output_paths(date: str) -> dict[str, Path]:
    news = ROOT / "01_daily" / "news"
    ev = ROOT / "01_daily" / "events"
    heat = ROOT / "01_daily" / "map_heat"
    cat = ROOT / "01_daily" / "catalyst"
    gen = ROOT / "01_daily" / "general"
    sec = ROOT / "01_daily" / "sectors" / date
    return {
        "parsed_json": news / f"{date}_parsed.json",
        "parsed_md": news / f"{date}_parsed.md",
        "events_json": ev / f"{date}_events.json",
        "events_md": ev / f"{date}_events.md",
        "judge_md": news / f"{date}_judge.md",
        "actions_json": news / f"{date}_actions.json",
        "research_json": heat / f"{date}_research.json",
        "research_md": heat / f"{date}_research.md",
        "dossiers_json": cat / f"{date}_dossiers.json",
        "dossiers_md": cat / f"{date}_dossiers.md",
        "predict_md": gen / f"{date}_predict.md",
        "board_json": sec / "_board.json",
        "qc_json": ROOT / "01_daily" / f"{date}_preopen_qc.json",
        "status_json": ROOT / "01_daily" / f"{date}_preopen_status.json",
        "status_md": ROOT / "01_daily" / f"{date}_preopen_status.md",
        "grok_review_md": ROOT / "01_daily" / f"{date}_grok_review.md",
    }


def check_preopen_prereqs(report: Report, date: str) -> None:
    print("\n== 2. PREREQUISITES (pre-open) — from last night + 05:40 scrape ==",
          flush=True)
    print(f"  session {date}: needs post-close files dated {date} "
          f"(written the previous night) plus GH-hosted Elite scrape", flush=True)
    p = preopen_input_paths(date)

    _file_check(report,
                "Finviz digest (GH-hosted scrape, NOT ECS)",
                "prereq", p["digest_json"], True, "after",
                qc=output_qc.qc_finviz_digest,
                extra="finviz_preopen_scrape.yml ~05:40 ET")
    _file_check(report, "Finviz digest markdown",
                "prereq", p["digest_md"], False, "after")

    _file_check(report,
                "Map heat JSON (post-close groups + morning overlay)",
                "prereq", p["map_heat_json"], True, "after",
                qc=output_qc.qc_map_heat,
                extra="empty tape = day-fail; overlay must stamp today's overlay_at")

    heat = _json(p["map_heat_json"]) if p["map_heat_json"].exists() else None
    if isinstance(heat, dict):
        overlay_at = str(heat.get("overlay_at") or "")
        tape = heat.get("tape") or []
        if overlay_at.startswith(date) and tape:
            _add(report, name="Morning overlay_at + non-empty tape", group="prereq",
                 status="OK", required=True,
                 detail=f"overlay_at={overlay_at} tape_n={len(tape)}")
        elif not tape:
            _add(report, name="Morning overlay_at + non-empty tape", group="prereq",
                 status="FAIL", required=True,
                 detail="empty futures tape (ECS 403 overlay or scrape missed)")
        else:
            _add(report, name="Morning overlay_at + non-empty tape", group="prereq",
                 status="WARN", required=True,
                 detail=f"overlay_at={overlay_at or 'missing'} (want prefix {date})")

    # Last night's captain baseline. Missing = WARN (morning_bootstrap).
    if p["baseline_json"].exists():
        _file_check(report, "Post-close captain baseline (last night)",
                    "prereq", p["baseline_json"], False, "after",
                    qc=output_qc.qc_map_heat_baseline)
    else:
        _add(report, name="Post-close captain baseline (last night)",
             group="prereq", status="WARN", required=False,
             detail="missing — morning_bootstrap stub, s_heat=0 for the day",
             path=str(p["baseline_json"]))

    timer = _systemctl("is-enabled", "fullscan-preopen.timer")
    _add(report, name="systemd fullscan-preopen.timer", group="prereq",
         status="OK" if timer in ("enabled", "enabled-runtime") else "WARN",
         required=False, detail=timer or "not found")


def check_preopen_outputs(report: Report, date: str, phase: str) -> None:
    print(f"\n== 3. EXPECTED OUTPUTS (pre-open {date}) ==", flush=True)
    p = preopen_output_paths(date)
    _file_check(report, "News parse JSON", "output", p["parsed_json"], True, phase,
                qc=output_qc.qc_news_parse)
    _file_check(report, "News parse markdown", "output", p["parsed_md"], False, phase)
    _file_check(report, "Events JSON (NOT a carry-forward)", "output",
                p["events_json"], True, phase, qc=output_qc.qc_events_path)
    _file_check(report, "Events markdown", "output", p["events_md"], False, phase)
    _file_check(report, "News judge", "output", p["judge_md"], True, phase,
                qc=output_qc.qc_news_judge)
    _file_check(report, "News actions", "output", p["actions_json"], False, phase,
                qc=output_qc.qc_news_actions)
    _file_check(report, "Map-heat morning refresh JSON", "output",
                p["research_json"], True, phase)
    _file_check(report, "Map-heat morning refresh MD (CAPTAIN_CARDS_OK)",
                "output", p["research_md"], True, phase,
                qc=output_qc.qc_map_heat_research)
    _file_check(report, "Catalyst dossiers JSON (optional)", "output",
                p["dossiers_json"], False, phase)
    _file_check(report, "Catalyst dossiers MD", "output", p["dossiers_md"], False, phase)
    _file_check(report, "General market predict", "output", p["predict_md"], True,
                phase, qc=output_qc.qc_general_predict)

    n_ok = 0
    for sector in FINVIZ_SECTORS:
        sp = ROOT / "01_daily" / "sectors" / date / f"{_slug(sector)}_predict.md"
        if phase == "before":
            _add(report, name=f"Sector predict — {sector}", group="output",
                 status="SKIP", required=True,
                 detail="not written yet (phase=before)", path=str(sp))
            continue
        r = output_qc.qc_sector_predict(sp)
        if r.ok:
            n_ok += 1
        _add(report, name=f"Sector predict — {sector}", group="output",
             status="OK" if r.ok else "FAIL", required=True,
             detail=r.reason or "OK", path=str(sp))
    if phase == "after":
        st = "OK" if n_ok >= 8 else "FAIL"
        _add(report, name="≥8/11 quality sector predicts", group="output",
             status=st, required=True, detail=f"{n_ok}/11")

    _file_check(report, "Sector board JSON", "output", p["board_json"], False, phase)
    _file_check(report, "Pre-open QC JSON", "output", p["qc_json"], True, phase)
    _file_check(report, "Pre-open status JSON", "output", p["status_json"], True, phase)
    _file_check(report, "Grok text review", "output", p["grok_review_md"], True, phase)


# ---------------------------------------------------------------------------
# Render / main
# ---------------------------------------------------------------------------

def render(report: Report) -> str:
    lines = [
        f"# Pipeline health — {report.job} / {report.phase}",
        "",
        f"date={report.date}  source={report.source_date}  target={report.target_date}",
        f"generated {report.generated_at}",
        f"result={'PASS' if report.ok else 'FAIL'}  "
        f"required_fails={report.n_fail}  warns={report.n_warn}",
        "",
        "## How to read this",
        "",
        "- **Door / bugs**: OpenClaw must PONG with the **live 48-char json token**, "
        "not the stale 64-char GitHub secret. Model must be Grok. "
        "GROK_ONLY must be on or we fall into SearXNG/DeepSeek.",
        "- **Prereqs**: files a *previous* workflow already had to write. "
        "Pre-open on date D needs post-close files **dated D** (written the night before) "
        "plus the 05:40 GH Finviz scrape.",
        "- **Outputs**: files *this* job is supposed to produce. "
        "`SKIP` means `--phase before` (job has not run yet).",
        "",
    ]
    groups = [
        ("door", "1. Door"),
        ("bug", "1b. Known bugs"),
        ("prereq", "2. Prerequisites (inputs from earlier jobs)"),
        ("output", "3. Expected outputs"),
    ]
    for key, title in groups:
        rows = [c for c in report.checks if c.group == key]
        if not rows:
            continue
        lines.append(f"## {title}")
        lines.append("")
        lines.append("| status | check | required | detail | path |")
        lines.append("| --- | --- | --- | --- | --- |")
        for c in rows:
            det = (c.detail or "").replace("|", "/")
            path = (c.path or "").replace("|", "/")
            lines.append(
                f"| {c.status} | {c.name} | {'yes' if c.required else 'no'} | {det} | `{path}` |"
            )
        lines.append("")
    lines.append("## Contract (what each job owes the next)")
    lines.append("")
    lines.append("### Post-close (22:00 ET, night of SOURCE, files dated TARGET)")
    lines.append("")
    lines.append("| file | required | consumed by |")
    lines.append("| --- | --- | --- |")
    lines.append("| `01_daily/map_heat/{TARGET}_map_heat.json` | yes | pre-open overlay + morning refresh |")
    lines.append("| `01_daily/map_heat/{TARGET}_map_heat.md` | no | humans |")
    lines.append("| `01_daily/map_heat/{TARGET}_research_baseline.json` | yes | morning refresh; missing → bootstrap, s_heat=0 |")
    lines.append("| `01_daily/map_heat/{TARGET}_research_baseline.md` | yes | humans / QC |")
    lines.append("")
    lines.append("### GH Finviz scrape (05:40 ET, date = TARGET/today)")
    lines.append("")
    lines.append("| file | required | consumed by |")
    lines.append("| --- | --- | --- |")
    lines.append("| `01_daily/news/{DATE}_finviz_digest.json` | yes | news parse / digest layer |")
    lines.append("| `01_daily/news/{DATE}_finviz_digest.md` | no | humans |")
    lines.append("| overlay on `{DATE}_map_heat.json` (`overlay_at`, `tape`, `econ`, `earnings`) | yes | morning refresh; empty tape fails the day |")
    lines.append("")
    lines.append("### Pre-open ALL (05:55 ET, date = today ET)")
    lines.append("")
    lines.append("| file | required |")
    lines.append("| --- | --- |")
    lines.append("| `01_daily/news/{DATE}_parsed.json` | yes |")
    lines.append("| `01_daily/events/{DATE}_events.json` (not carry) | yes |")
    lines.append("| `01_daily/news/{DATE}_judge.md` | yes |")
    lines.append("| `01_daily/news/{DATE}_actions.json` | no (WARN) |")
    lines.append("| `01_daily/map_heat/{DATE}_research.json` + `_research.md` | yes |")
    lines.append("| `01_daily/catalyst/{DATE}_dossiers.json` | no |")
    lines.append("| `01_daily/general/{DATE}_predict.md` | yes |")
    lines.append("| `01_daily/sectors/{DATE}/<11 slugs>_predict.md` (≥8 quality) | yes |")
    lines.append("| `01_daily/sectors/{DATE}/_board.json` | no |")
    lines.append("| `01_daily/{DATE}_preopen_qc.json` | yes |")
    lines.append("| `01_daily/{DATE}_preopen_status.json` | yes |")
    lines.append("| `01_daily/{DATE}_grok_review.md` | yes |")
    lines.append("")
    return "\n".join(lines) + "\n"


def write_report(report: Report) -> Path:
    out_dir = ROOT / "01_daily"
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = report.date
    md = out_dir / f"{stamp}_pipeline_health_{report.job}.md"
    js = out_dir / f"{stamp}_pipeline_health_{report.job}.json"
    payload = {
        "job": report.job,
        "phase": report.phase,
        "date": report.date,
        "source_date": report.source_date,
        "target_date": report.target_date,
        "generated_at": report.generated_at,
        "ok": report.ok,
        "n_fail": report.n_fail,
        "n_warn": report.n_warn,
        "checks": [asdict(c) for c in report.checks],
    }
    js.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    md.write_text(render(report), encoding="utf-8")
    print(f"[pipeline-health] wrote {md}", flush=True)
    print(f"[pipeline-health] wrote {js}", flush=True)
    return md


def run(job: str, phase: str, date: str | None, source: str | None,
        target: str | None, write: bool) -> Report:
    os.environ.setdefault("HOME", "/home/gha")
    today = date or _today()
    source = source or today
    target = target or next_weekday(source)
    if job == "preopen":
        session = today
    elif job == "postclose":
        session = target
    else:
        session = today
    report = Report(
        job=job, phase=phase, date=session,
        source_date=source, target_date=target,
        generated_at=datetime.now(ET).isoformat(),
    )
    print("=" * 72, flush=True)
    print(f"  PIPELINE HEALTH  job={job} phase={phase}", flush=True)
    print(f"  today={today}  source={source}  target={target}", flush=True)
    print("=" * 72, flush=True)

    check_door(report)
    if job == "postclose":
        check_postclose_prereqs(report, source, target)
        check_postclose_outputs(report, target, phase)
    elif job == "preopen":
        check_preopen_prereqs(report, today)
        check_preopen_outputs(report, today, phase)

    print("\n== SUMMARY ==", flush=True)
    print(f"  {'PASS' if report.ok else 'FAIL'}  required_fails={report.n_fail}  warns={report.n_warn}",
          flush=True)
    if write:
        write_report(report)
    return report


def main() -> None:
    now = datetime.now(ET)
    hm = now.hour * 100 + now.minute
    ap = argparse.ArgumentParser()
    ap.add_argument("--job", choices=["door", "preopen", "postclose", "auto"],
                    default="auto")
    ap.add_argument("--phase", choices=["before", "after"], default="before")
    ap.add_argument("--date", default=None, help="pre-open session date (ET today)")
    ap.add_argument("--source-date", default=None, help="completed session (post-close)")
    ap.add_argument("--target-date", default=None, help="next session (post-close writes)")
    ap.add_argument("--write", action="store_true")
    args = ap.parse_args()
    job = args.job
    if job == "auto":
        # Night window (21:00–04:00 ET) = post-close health; else pre-open.
        job = "postclose" if (hm >= 2100 or hm < 400) else "preopen"
        print(f"[pipeline-health] auto job={job} (ET {hm:04d})", flush=True)
    report = run(
        job=job, phase=args.phase, date=args.date,
        source=args.source_date, target=args.target_date, write=args.write,
    )
    raise SystemExit(0 if report.ok else 1)


if __name__ == "__main__":
    main()
