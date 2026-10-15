"""Fast-fail a hung OpenClaw gateway so the morning packet is not stuck.

On 2026-10-14 the gateway port was up and a tiny "17+25" completion
returned in under 10 seconds. The events call (native web/X search)
then sat until the 240s client cap, twice. The gateway's own timer is
10800s, so that hung turn stayed inside the process and the next call
waited behind it. The breaker opened, the free GLM fallback was told
it was "engaged", and (before #597) it was routed back into the same
dead gateway and returned nothing.

This module does three small things:

- prove the gateway with a tiny completion, not a port check
- restart the gateway once per run when a read times out, so the
  10800s zombie dies
- give the general predict one no-search Grok try after that restart
"""
from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PROBE_PROMPT = "What is 17+25? Reply with only the number."
STATUS_NAME = "_openclaw_preopen_health.json"


def _markers() -> tuple[Path, Path]:
    base = (os.environ.get("OPENCLAW_BREAKER_FILE") or "").strip()
    if base:
        return Path(base + ".restarted"), Path(base + ".predict_shot")
    run = os.environ.get("GITHUB_RUN_ID") or "local"
    folder = Path(os.environ.get("RUNNER_TEMP") or "/tmp")
    return folder / f"openclaw_restarted_{run}", folder / f"openclaw_predict_shot_{run}"


def _enabled() -> bool:
    return os.environ.get("OPENCLAW_RESTART_ON_TIMEOUT", "") == "1"


def restart_gateway(run=None) -> bool:
    """Hard-bounce the gateway. Bounded so a stuck restart cannot eat the step."""
    script = ROOT / "scripts" / "enable_openclaw_chat.sh"
    if not script.is_file():
        print("[openclaw-fastfail] restart script missing", flush=True)
        return False
    env = {k: v for k, v in os.environ.items()
           if k not in ("XAI_API_KEY", "OPENCLAW_XAI_API_KEY", "GROK_API_KEY")}
    env["OPENCLAW_FORCE_RESTART"] = "true"
    runner = run or subprocess.run
    try:
        proc = runner(
            ["bash", str(script)],
            env=env,
            capture_output=True,
            text=True,
            timeout=75,
            check=False,
        )
    except subprocess.TimeoutExpired:
        print("[openclaw-fastfail] gateway restart exceeded 75s", flush=True)
        return False
    tail = "\n".join(((getattr(proc, "stdout", "") or "")).splitlines()[-4:])
    if tail:
        print(f"[openclaw-fastfail] restart tail: {tail}", flush=True)
    return getattr(proc, "returncode", 1) == 0


def note_hung_call(restart=None) -> bool:
    """One restart per run after a read timeout. Does not clear the breaker.

    Later calls in this process stay on the fast-fail path. The predict
    step spends a single no-search shot via predict_attempt().
    """
    if not _enabled():
        return False
    restarted, _shot = _markers()
    if restarted.exists():
        return False
    ok = (restart or restart_gateway)()
    try:
        restarted.parent.mkdir(parents=True, exist_ok=True)
        restarted.write_text("1\n", encoding="utf-8")
    except OSError as exc:
        print(f"[openclaw-fastfail] could not mark restart: {exc}", flush=True)
    print(f"[openclaw-fastfail] hung read — gateway restart "
          f"{'ok' if ok else 'failed'} (once this run)", flush=True)
    return ok


def _clear_breaker() -> None:
    from . import deepseek_client as dc
    path = dc._breaker_file()
    if path:
        try:
            Path(path).write_text("", encoding="utf-8")
        except OSError:
            pass
    dc.reset_openclaw_breaker("fastfail predict shot")


def predict_attempt(restart=None) -> str:
    """How the general predict may call Grok: search, nosearch, or skip.

    A healthy gateway gets one native-search turn. After any hung read
    this run, the predict gets one no-search turn (the documents are
    already in the prompt) and then the free fallback.
    """
    from . import config
    from . import deepseek_client as dc
    if not config.openclaw_enabled():
        return "skip"
    restarted, shot = _markers()
    hung = dc._breaker_open() or restarted.exists()
    if not hung:
        return "search"
    if shot.exists():
        return "skip"
    try:
        shot.parent.mkdir(parents=True, exist_ok=True)
        shot.write_text("1\n", encoding="utf-8")
    except OSError:
        return "skip"
    if not restarted.exists():
        note_hung_call(restart=restart)
    _clear_breaker()
    return "nosearch"


def completion_probe(timeout_s: int = 25, post=None) -> dict:
    """Tiny 17+25 completion. A listening port is not this check."""
    from . import config
    url = (config.OPENCLAW_GATEWAY_URL or "").rstrip("/")
    if not url:
        return {"ok": False, "reason": "no gateway url"}
    model = os.environ.get("OPENCLAW_BACKEND_MODEL") or "xai/grok-4.6"
    payload = {
        "model": config.OPENCLAW_AGENT,
        "messages": [{"role": "user", "content": PROBE_PROMPT}],
        "max_tokens": 16,
        "temperature": 0,
    }
    headers = {
        "Content-Type": "application/json",
        "x-openclaw-model": model,
        "x-openclaw-session-key": f"fullscan-preopen-probe-{os.getpid()}",
    }
    if config.OPENCLAW_TOKEN:
        headers["Authorization"] = f"Bearer {config.OPENCLAW_TOKEN}"

    def _default_post():
        import requests
        return requests.post(url + "/v1/chat/completions", headers=headers,
                             json=payload, timeout=(10, timeout_s))

    try:
        resp = (post or _default_post)()
    except Exception as exc:  # noqa: BLE001 — probe must not raise
        return {"ok": False, "reason": type(exc).__name__, "detail": str(exc)[:180],
                "model": model}
    code = getattr(resp, "status_code", 0)
    text = ""
    try:
        body = resp.json()
        text = ((body.get("choices") or [{}])[0].get("message") or {}).get("content") or ""
    except Exception:  # noqa: BLE001
        text = (getattr(resp, "text", "") or "")[:180]
    ok = code == 200 and "42" in text and "timed out" not in text.lower()
    return {"ok": ok, "http": code, "answer": text[:40], "model": model,
            "reason": "ok" if ok else f"http {code} answer={text[:40]!r}"}


def ensure_before_essays(probe=None, restart=None) -> dict:
    """Call before the LLM stages. Restart once if the tiny completion fails."""
    from . import config
    if not _enabled() or not config.openclaw_enabled():
        return {"ok": False, "reason": "fastfail off or no gateway"}
    do_probe = probe or completion_probe
    first = do_probe()
    if first.get("ok"):
        _write_status(first)
        print(f"[openclaw-fastfail] probe ok model={first.get('model')} "
              f"answer={first.get('answer')!r}", flush=True)
        return first
    print(f"[openclaw-fastfail] probe failed ({first.get('reason')}) "
          "— restarting gateway once", flush=True)
    # Marker so the predict spends its one no-search shot instead of
    # opening another native-search turn on the same hung gateway.
    note_hung_call(restart=restart or restart_gateway)
    second = do_probe()
    second["restarted"] = True
    _write_status(second)
    print(f"[openclaw-fastfail] probe after restart ok={second.get('ok')} "
          f"{second.get('reason')}", flush=True)
    return second


def _write_status(payload: dict) -> None:
    path = ROOT / "01_daily" / STATUS_NAME
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    except OSError as exc:
        print(f"[openclaw-fastfail] status write failed: {exc}", flush=True)
