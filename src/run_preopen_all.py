"""One-button PRE-OPEN ALL: every predictive write that must land before 09:30 ET.

Does in one ECS job (skip-if-good, fail-closed QC):

  finviz digest → events (+ catcher, NEVER carry) → news parse → news judge
  → news actions → general predict → 11 sector predicts → sector board
  → output_qc (trash / timeout stubs / carry-forwards) → workflow check

NOT included (those run later on their own crons, still required):
  outcome / reflect / horizon grade, learn_cycle, deepthink, weekly
  promotion, weather, AB checklist, stock book, paper dashboard.

CLI:
  python -m src.run_preopen_all [--date YYYY-MM-DD] [--force]
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import urllib.error
import urllib.request
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from . import config, output_qc, preopen

ROOT = Path(__file__).resolve().parent.parent
ET = ZoneInfo(config.TZ)

# Logical modules this one-button job is responsible for. Keys match
# daily_orchestrator.yml workflow files (minus .yml) where possible.
REQUIRED = [
    ("finviz_digest", "Finviz daily digest", True),
    ("events", "Event scanner", True),
    ("news_parse", "News parse", True),
    ("news_judge", "News judge", True),
    ("news_actions", "News actions", False),
    ("general_predict", "General market predict", True),
    ("sector_predict", "Per-sector predict (11)", True),
]


def _today() -> str:
    return datetime.now(ET).date().isoformat()


def _run(cmd: list[str]) -> int:
    print(f"\n>>> {' '.join(cmd)}", flush=True)
    r = subprocess.run(cmd, cwd=str(ROOT), env=os.environ.copy())
    return r.returncode


def _p(*parts: str) -> Path:
    return ROOT.joinpath(*parts)


def _exists(*parts: str) -> bool:
    return _p(*parts).exists()


def _force_args(force: bool) -> list[str]:
    return ["--force"] if force else []


def _github_runs_today(date: str) -> list[dict]:
    """Best-effort: which related workflows actually ran today (ET)."""
    token = os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN") or ""
    repo = os.environ.get("GITHUB_REPOSITORY") or "SRoyaltyy/fullscan"
    if not token:
        return []
    names = [
        "preopen_all.yml",
        "finviz_digest.yml",
        "events_daily.yml",
        "news_parse.yml",
        "news_judge.yml",
        "news_actions.yml",
        "daily_pipeline.yml",
        "sector_daily.yml",
    ]
    out: list[dict] = []
    for wf in names:
        url = (
            f"https://api.github.com/repos/{repo}/actions/workflows/{wf}"
            f"/runs?per_page=8"
        )
        req = urllib.request.Request(url, headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "fullscan-preopen-all",
        })
        try:
            with urllib.request.urlopen(req, timeout=20) as resp:
                payload = json.loads(resp.read().decode("utf-8"))
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError, OSError) as e:
            out.append({"workflow": wf, "error": str(e)[:160]})
            continue
        today_runs = []
        for run in payload.get("workflow_runs") or []:
            created = str(run.get("created_at") or "")
            try:
                utc = datetime.fromisoformat(created.replace("Z", "+00:00"))
                et_date = utc.astimezone(ET).date().isoformat()
            except ValueError:
                et_date = ""
            if et_date != date:
                continue
            today_runs.append({
                "id": run.get("id"),
                "status": run.get("status"),
                "conclusion": run.get("conclusion"),
                "event": run.get("event"),
                "html_url": run.get("html_url"),
            })
        latest = today_runs[0] if today_runs else None
        out.append({
            "workflow": wf,
            "n_today": len(today_runs),
            "latest": latest,
        })
    return out


def run(date: str | None = None, force: bool = False) -> None:
    date = date or _today()
    print("")
    print("=" * 72)
    print(f"  PRE-OPEN ALL — {date} (America/New_York)")
    print("  Predictive only. Must finish before 09:30 ET.")
    print("  Skip-if-good: quality files for THIS day are not overwritten.")
    print("  Carry-forwards / timeout stubs are trash and fail the job.")
    print("=" * 72)

    preopen.refuse_if_late("preopen_all", force=force)

    # Already quality-done today? (High quality = full output_qc pass, not
    # "a file exists".) Then do NOT waste the window and do NOT touch the
    # good copies — a re-dispatch after success must be a no-op.
    if not force:
        pre = output_qc.preopen_report(date)
        if pre.get("all_ok"):
            print(f"[preopen-all] {date}: every required predictive "
                  f"artifact is already quality-ok "
                  f"(sectors {pre.get('sector_n_ok')}/"
                  f"{pre.get('sector_n_total')}) — nothing to do")
            return

    attempts: list[dict] = []

    def step(key: str, title: str, cmd: list[str]) -> int:
        print(f"\n[preopen-all] → {title}")
        code = _run(cmd)
        attempts.append({"key": key, "title": title, "cmd": cmd,
                         "returncode": code})
        if code != 0:
            print(f"[preopen-all] WARN: {title} exited {code}")
        return code

    fa = _force_args(force)
    py = sys.executable

    step("news_parse", "News parse",
         [py, "-m", "src.news_parse", "--hours", "48", "--limit", "400",
          "--date", date, *fa])
    step("finviz_digest", "Finviz daily digest",
         [py, "-m", "src.finviz_digest", "--date", date, *fa])
    step("events", "Event scanner (primary)",
         [py, "-m", "src.run_events", "--date", date, *fa])
    step("events_catcher", "Event catcher (gap hunt, no carry)",
         [py, "-m", "src.run_events_catcher", "--date", date, *fa])
    # Deliberately NO events_fallback — carry is trash for pre-open.
    step("news_judge", "News judge",
         [py, "-m", "src.run_news_judge", "--date", date, *fa])
    step("news_actions", "News actions",
         [py, "-m", "src.news_actions", "--hours", "48", "--limit", "400",
          "--date", date, *fa])
    step("general_predict", "General market predict",
         [py, "-m", "src.run_predict", "--date", date, *fa])
    step("sector_predict", "Per-sector predict (all 11)",
         [py, "-m", "src.run_sector_predict", "--date", date, *fa])
    step("sector_board", "Sector board",
         [py, "-m", "src.sector_board", "--date", date])

    qc_path = output_qc.write_preopen_report(date)
    report = output_qc.preopen_report(date)
    print("")
    print(output_qc.render(report))
    print(f"[preopen-all] wrote {qc_path}")

    gh_runs = _github_runs_today(date)
    missing_required = []
    print("")
    print("-" * 72)
    print("  WORKFLOW / ARTIFACT CHECK")
    print("-" * 72)
    by_kind = {}
    for item in report.get("items") or []:
        by_kind.setdefault(item.get("kind"), []).append(item)

    for key, title, required in REQUIRED:
        if key == "sector_predict":
            n_ok = int(report.get("sector_n_ok") or 0)
            n_tot = int(report.get("sector_n_total") or 11)
            ok = n_ok >= 8
            detail = f"{n_ok}/{n_tot} quality sector predicts (need >=8)"
        elif key == "general_predict":
            rows = by_kind.get("general_predict") or []
            ok = bool(rows) and all(r.get("ok") for r in rows)
            detail = rows[0].get("reason") if rows and not ok else ""
            detail = detail or ("OK" if ok else "missing")
        elif key == "events":
            rows = by_kind.get("events") or []
            ok = bool(rows) and all(r.get("ok") for r in rows)
            detail = rows[0].get("reason") if rows and not ok else ""
            detail = detail or ("OK" if ok else "missing")
        elif key == "news_parse":
            rows = by_kind.get("news_parse") or []
            ok = bool(rows) and all(r.get("ok") for r in rows)
            detail = rows[0].get("reason") if rows and not ok else ""
            detail = detail or ("OK" if ok else "missing")
        elif key == "news_judge":
            rows = by_kind.get("news_judge") or []
            ok = bool(rows) and all(r.get("ok") for r in rows)
            detail = rows[0].get("reason") if rows and not ok else ""
            detail = detail or ("OK" if ok else "missing")
        elif key == "news_actions":
            rows = by_kind.get("news_actions") or []
            ok = bool(rows) and all(r.get("ok") for r in rows)
            detail = rows[0].get("reason") if rows and not ok else ""
            detail = detail or ("OK" if ok else "missing")
        elif key == "finviz_digest":
            rows = by_kind.get("finviz_digest") or []
            ok = bool(rows) and all(r.get("ok") for r in rows)
            detail = rows[0].get("reason") if rows and not ok else ""
            detail = detail or ("OK" if ok else "missing")
        else:
            ok, detail = False, "unknown"
        flag = "OK  " if ok else ("FAIL" if required else "WARN")
        print(f"  [{flag}] {title:<28} {detail}")
        if required and not ok:
            missing_required.append(key)

    if gh_runs:
        print("")
        print("  GitHub workflow runs today (informational; ALL covers them):")
        for row in gh_runs:
            latest = row.get("latest") or {}
            if row.get("error"):
                print(f"    {row['workflow']}: api-error {row['error']}")
                continue
            st = latest.get("conclusion") or latest.get("status") or "none"
            n = row.get("n_today") or 0
            print(f"    {row['workflow']:<22} n={n} latest={st}")

    status = {
        "date": date,
        "generated_at": datetime.now(ET).isoformat(),
        "all_ok": bool(report.get("all_ok")) and not missing_required,
        "qc_all_ok": bool(report.get("all_ok")),
        "missing_required": missing_required,
        "attempts": [
            {"key": a["key"], "title": a["title"], "returncode": a["returncode"]}
            for a in attempts
        ],
        "github_runs": gh_runs,
        "qc": {
            "sector_n_ok": report.get("sector_n_ok"),
            "sector_n_total": report.get("sector_n_total"),
            "items": [
                {"kind": i.get("kind"), "ok": i.get("ok"),
                 "reason": i.get("reason"), "path": i.get("path"),
                 "size": i.get("size")}
                for i in (report.get("items") or [])
            ],
        },
    }
    status_path = _p("01_daily", f"{date}_preopen_status.json")
    status_path.write_text(json.dumps(status, indent=2), encoding="utf-8")
    md_path = _p("01_daily", f"{date}_preopen_status.md")
    md_lines = [
        f"# Pre-open ALL status — {date}",
        "",
        f"all_ok={status['all_ok']}  qc_all_ok={status['qc_all_ok']}  "
        f"missing={missing_required or 'none'}",
        "",
        "Predictive modules (must land before 09:30 ET). Lessons / outcome /",
        "deepthink / weekly / dashboard run later on their own crons.",
        "",
    ]
    for a in attempts:
        md_lines.append(f"- {a['title']}: exit {a['returncode']}")
    md_path.write_text("\n".join(md_lines) + "\n", encoding="utf-8")
    print(f"[preopen-all] wrote {status_path}")
    print(f"[preopen-all] wrote {md_path}")

    if missing_required or not report.get("all_ok"):
        raise SystemExit(
            f"[preopen-all] FAIL {date}: trash or missing required artifacts "
            f"{missing_required or '(see QC)'}. Not committing as success."
        )
    print(f"[preopen-all] PASS {date} — all required predictive artifacts are quality")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=None)
    ap.add_argument("--force", action="store_true",
                    help="Ignore 09:25 ET cutoff and skip-if-good")
    args = ap.parse_args()
    run(date=args.date, force=args.force)


if __name__ == "__main__":
    main()
