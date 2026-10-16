#!/usr/bin/env python3
"""Preflight: check a workflow's prerequisites before anything is written.

    python3 scripts/preflight.py --workflow preopen_all [--date YYYY-MM-DD]

Reads config/prereq_checklist.json ("checks" block), runs each check,
writes 01_daily/_progress/<date>_<workflow>_preflight.json and .md naming
every failed item in plain words, and exits 1 if any failed. Whole run is
bounded by DEADLINE_S (under 60s). Standalone: does not import
src/workflow_progress.py (#605).

Born 2026-10-15: Pre-Open 38714326944 ran on ubuntu, skipped Grok 108
times and came back green with no predict. Run 38720850552 on ECS got
grok-4.6 HTTP 200 and was aborted at the 75s read cap.
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parent.parent
CONFIG = ROOT / "config" / "prereq_checklist.json"
DEADLINE_S = 55
ET = ZoneInfo("America/New_York")
_T0 = time.monotonic()


def _left() -> float:
    return max(1.0, DEADLINE_S - (time.monotonic() - _T0))


# ---- checks: each returns (ok, detail) -------------------------------------

def check_runner_is_ecs(item, env, date):
    envkind = (env.get("RUNNER_ENVIRONMENT") or "").strip()
    name = env.get("RUNNER_NAME") or ""
    if envkind == "github-hosted" or name.startswith("GitHub Actions"):
        return False, f"runner '{name}' is GitHub-hosted"
    if envkind != "self-hosted":
        return False, f"runner environment '{envkind or 'unknown'}' is not self-hosted"
    return True, f"self-hosted runner '{name}'"


def check_backend_grok(item, env, date):
    want = item.get("expect", "grok")
    got = (env.get("LLM_BACKEND") or "").strip().lower()
    return got == want, f"LLM_BACKEND={got or '(unset)'}"


def check_gateway_up(item, env, date):
    url = item.get("url", "http://127.0.0.1:18789/health")
    t = min(float(item.get("timeout_s", 5)), _left())
    try:
        with urllib.request.urlopen(url, timeout=t) as r:  # noqa: S310
            code = r.status
    except urllib.error.HTTPError as e:  # gateway answered
        code = e.code
    except Exception as e:  # noqa: BLE001
        return False, f"{url}: {type(e).__name__}: {e}"
    return code < 500, f"{url} -> HTTP {code}"


def _expiry_minutes_left(env) -> float | None:
    code = ("import sys; sys.path.insert(0, 'scripts'); "
            "import openclaw_oauth_guard as g; e, _, _ = g.xai_oauth_expiry(); "
            "print(e.isoformat() if e else '')")
    r = subprocess.run([sys.executable, "-c", code], cwd=ROOT, env=env,
                       capture_output=True, text=True, timeout=min(60, _left()))
    out = (r.stdout or "").strip().splitlines()
    if not out or not out[-1]:
        return None
    exp = datetime.fromisoformat(out[-1].replace("Z", "+00:00"))
    if exp.tzinfo is None:
        exp = exp.replace(tzinfo=timezone.utc)
    return (exp - datetime.now(timezone.utc)).total_seconds() / 60.0


def check_xai_token_outlives_run(item, env, date):
    need = float(item.get("min_left_min", 75))
    try:
        left = _expiry_minutes_left(env)
    except Exception as e:  # noqa: BLE001
        return False, f"could not read xAI expiry: {type(e).__name__}"
    if left is not None and left >= need:
        return True, f"{left:.0f} min left"
    # One bounded renew attempt (same command as the Pre-Open renew step).
    if env.get("PREFLIGHT_TRY_RENEW", "1") == "1" and _left() > 15:
        try:
            subprocess.run(
                ["sudo", "-n", "env", "-u", "XAI_API_KEY", "-u", "OPENCLAW_XAI_API_KEY",
                 "-u", "GROK_API_KEY", "FULLSCAN_HOME=/home/gha", "HOME=/home/gha",
                 f"OPENCLAW_RUN_COVER_MIN={int(need)}", "python3",
                 "scripts/openclaw_oauth_guard.py", "renew"],
                cwd=ROOT, capture_output=True, timeout=_left() - 8)
            left = _expiry_minutes_left(env)
        except Exception:  # noqa: BLE001
            pass
        if left is not None and left >= need:
            return True, f"{left:.0f} min left after renew"
    shown = "unknown" if left is None else f"{left:.0f} min"
    return False, f"xAI login has {shown} left, need {need:.0f}"


def check_no_unapproved_paid_fallback(item, env, date):
    approved = set(item.get("approved", [""]))
    fb = (env.get("LLM_FALLBACK_MODEL") or "").strip()
    return fb in approved, f"LLM_FALLBACK_MODEL={fb or '(none)'}"


def check_read_cap_fits(item, env, date):
    need = int(item.get("min_s", 300))
    raw = (env.get("OPENCLAW_PREDICT_READ_CAP_S") or "").strip()
    try:
        cap = int(raw)
    except ValueError:
        return False, f"OPENCLAW_PREDICT_READ_CAP_S={raw or '(unset)'}"
    # 0 = no cap (falls back to the full read timeout), which fits.
    return cap == 0 or cap >= need, f"OPENCLAW_PREDICT_READ_CAP_S={cap}, need >= {need}"


def check_no_twin_preopen(item, env, date):
    notes = []
    lock = env.get("PREOPEN_LOCK", "/home/gha/fullscan-persist/locks/preopen.lock")
    if Path(lock).parent.is_dir():
        import fcntl
        try:
            with open(lock, "a") as fh:
                fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
                fcntl.flock(fh, fcntl.LOCK_UN)
        except BlockingIOError:
            return False, f"ECS systemd Pre-Open holds {lock}"
        except OSError as e:
            notes.append(f"lock unreadable ({e.__class__.__name__})")
    me = env.get("GITHUB_RUN_ID")
    try:
        runs = _preopen_runs(env)
    except Exception as e:  # noqa: BLE001
        runs = None
        notes.append(f"runs API unchecked ({type(e).__name__})")
    if runs:
        mine = next((x for x in runs if str(x.get("id")) == str(me)), None)
        my_created = (mine or {}).get("created_at") or "9999"
        twins = [str(x["id"]) for x in runs
                 if str(x.get("id")) != str(me)
                 and x.get("status") in ACTIVE
                 and (x.get("created_at") or "") < my_created]
        if twins:
            return False, "earlier Pre-Open run(s) still active: " + ", ".join(twins)
        # The orchestrator heal may not start one when today already has a
        # success (manual and schedule starts are not limited by this).
        if env.get("PREOPEN_STARTED_BY") == "orchestrator":
            done = [str(x["id"]) for x in runs
                    if str(x.get("id")) != str(me) and x.get("conclusion") == "success"
                    and _et_date(x.get("created_at")) == date]
            if done:
                return False, f"orchestrator start refused: Pre-Open for {date} already succeeded ({', '.join(done)})"
    return True, "no twin" + (f"; {'; '.join(notes)}" if notes else "")


ACTIVE = {"in_progress", "queued", "waiting", "pending", "requested"}


def _et_date(iso: str | None) -> str:
    if not iso:
        return ""
    return datetime.fromisoformat(iso.replace("Z", "+00:00")).astimezone(ET).strftime("%Y-%m-%d")


def _preopen_runs(env) -> list | None:
    repo, tok = env.get("GITHUB_REPOSITORY"), env.get("GITHUB_TOKEN")
    if not (repo and tok):
        return None
    url = (f"https://api.github.com/repos/{repo}/actions/workflows/"
           "preopen_all.yml/runs?per_page=30")
    req = urllib.request.Request(url, headers={
        "Authorization": f"Bearer {tok}", "Accept": "application/vnd.github+json"})
    with urllib.request.urlopen(req, timeout=min(10, _left())) as r:  # noqa: S310
        return json.load(r).get("workflow_runs", [])


def check_predict_on_main(item, env, date):
    rel = f"01_daily/general/{date}_predict.md"
    r = subprocess.run(["git", "cat-file", "-e", f"origin/main:{rel}"], cwd=ROOT,
                       capture_output=True, timeout=min(15, _left()))
    if r.returncode == 0:
        return True, f"{rel} on origin/main"
    return False, f"{rel} not on origin/main"


def _git(args, cwd):
    return subprocess.run(["git", *args], cwd=cwd, capture_output=True,
                          text=True, timeout=min(15, _left()))


CODE_PATHS = ("src", "scripts", "config", ".github")


def check_ecs_code_current(item, env, date):
    """'ecs code stale': this checkout's code must match GitHub main.

    Main gets data-only auto commits every few minutes, so HEAD == main
    would fail on a race. Fetch main and compare only code paths.
    """
    f = _git(["fetch", "--quiet", "--depth=1", "origin",
              "+refs/heads/main:refs/remotes/origin/main"], ROOT)
    if f.returncode != 0:
        return False, f"ecs code stale: cannot fetch GitHub main ({f.stderr.strip()[:120]})"
    head = _git(["rev-parse", "--short", "HEAD"], ROOT).stdout.strip()
    main = _git(["rev-parse", "--short", "origin/main"], ROOT).stdout.strip()
    d = _git(["diff", "--name-only", "HEAD", "origin/main", "--", *CODE_PATHS], ROOT)
    if d.returncode != 0:
        return False, f"ecs code stale: diff failed ({d.stderr.strip()[:120]})"
    changed = [x for x in d.stdout.splitlines() if x.strip()]
    if changed:
        return False, (f"ecs code stale: HEAD {head} vs main {main} differ in "
                       f"{len(changed)} code file(s): {', '.join(changed[:5])}")
    return True, f"code matches main (HEAD {head}, main {main})"


CHECKS = {k[len("check_"):]: v for k, v in globals().items() if k.startswith("check_")}


def run(workflow: str, date: str, env: dict, out_dir: Path) -> int:
    cfg = json.loads(CONFIG.read_text(encoding="utf-8"))
    items = cfg.get("checks", {}).get(workflow)
    if items is None:
        print(f"no preflight checks for '{workflow}'", file=sys.stderr)
        return 2
    results = []
    for it in items:
        fn = CHECKS.get(it["id"])
        if _left() <= 1:
            ok, detail = False, "preflight ran out of time before this check"
        elif fn is None:
            ok, detail = False, "no code for this check"
        else:
            try:
                ok, detail = fn(it, env, date)
            except Exception as e:  # noqa: BLE001
                ok, detail = False, f"check crashed: {type(e).__name__}: {e}"
        results.append({"id": it["id"], "ok": bool(ok),
                        "plain": it.get("plain", it["id"]).replace("<date>", date),
                        "detail": detail})
    failed = [r for r in results if not r["ok"]]
    now_et = datetime.now(ET).strftime("%Y-%m-%d %H:%M ET")
    doc = {"workflow": workflow, "date": date, "checked_at": now_et,
           "run_id": env.get("GITHUB_RUN_ID"), "ok": not failed,
           "failed": [r["id"] for r in failed], "items": results}
    out_dir.mkdir(parents=True, exist_ok=True)
    stem = out_dir / f"{date}_{workflow}_preflight"
    stem.with_suffix(".json").write_text(json.dumps(doc, indent=1) + "\n", encoding="utf-8")
    lines = [f"# Preflight {workflow} {date}", "",
             f"Checked {now_et}" + (f", run {doc['run_id']}" if doc["run_id"] else ""), "",
             ("**PASS** all items OK. The run may write." if not failed else
              f"**STOPPED** before writing. {len(failed)} item(s) failed:"), ""]
    for r in failed:
        lines.append(f"- ❌ **{r['id']}**: {r['plain']} ({r['detail']})")
    if failed:
        lines.append("")
    for r in results:
        if r["ok"]:
            lines.append(f"- ✅ {r['id']}: {r['detail']}")
    stem.with_suffix(".md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print("\n".join(lines))
    summ = env.get("GITHUB_STEP_SUMMARY")
    if summ:
        with open(summ, "a", encoding="utf-8") as fh:
            fh.write("\n".join(lines) + "\n")
    for r in failed:
        print(f"::error::preflight {r['id']}: {r['plain']} ({r['detail']})")
    return 1 if failed else 0


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--workflow", required=True)
    ap.add_argument("--date", default="")
    ap.add_argument("--out-dir", default=str(ROOT / "01_daily" / "_progress"))
    a = ap.parse_args(argv)
    date = a.date or datetime.now(ET).strftime("%Y-%m-%d")
    return run(a.workflow, date, dict(os.environ), Path(a.out_dir))


if __name__ == "__main__":
    sys.exit(main())
