"""One-button PRE-OPEN ALL: every predictive write that must land before 09:30 ET.

Does in one ECS job (skip-if-good, fail-closed QC):

  finviz digest → map heat (tables) → events (+ catcher)
  → news parse → news judge → map heat research (captains + opportunity)
  → news actions → general predict → 11 sector predicts → sector board
  → output_qc (regex) → Grok reads the files as text → workflow check

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
import shutil
import subprocess
import sys
import urllib.error
import urllib.request
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from . import config, grok_review, output_qc, preopen

ROOT = Path(__file__).resolve().parent.parent
ET = ZoneInfo(config.TZ)
# Lives on the ECS disk, OUTSIDE the Actions work tree. checkout --clean
# must not be able to delete a finished day's files, or skip-if-good is a lie.
PERSIST = Path(os.environ.get("FULLSCAN_PERSIST", "/home/gha/fullscan-persist"))

# Logical modules this one-button job is responsible for. Keys match
# daily_orchestrator.yml workflow files (minus .yml) where possible.
REQUIRED = [
    ("finviz_digest", "Finviz daily digest", True),
    ("events", "Event scanner", True),
    ("news_parse", "News parse", True),
    ("news_judge", "News judge", True),
    ("map_heat_research", "Map heat research (captains)", False),
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


def _date_paths(root: Path, date: str) -> list[Path]:
    """Today's predictive artifacts only. Missing paths are omitted."""
    hits: list[Path] = []
    for folder in (
        root / "01_daily",
        root / "01_daily" / "general",
        root / "01_daily" / "events",
        root / "01_daily" / "news",
        root / "01_daily" / "map_heat",
        root / "01_daily" / "_transcripts",
        root / "01_daily" / "_channel1",
    ):
        if folder.is_dir():
            hits.extend(sorted(folder.glob(f"{date}*")))
            hits.extend(sorted(folder.glob(f"{date}_*")))
    sector = root / "01_daily" / "sectors" / date
    if sector.exists():
        hits.append(sector)
    # unique, keep dirs
    out: list[Path] = []
    seen = set()
    for p in hits:
        rp = p.resolve() if p.exists() else p
        if rp in seen or not p.exists():
            continue
        seen.add(rp)
        out.append(p)
    return out


def restore_persist(date: str) -> int:
    """Copy a finished day back into the checkout so skip-if-good can see it."""
    if not PERSIST.is_dir():
        return 0
    n = 0
    for src in _date_paths(PERSIST, date):
        rel = src.relative_to(PERSIST)
        dest = ROOT / rel
        dest.parent.mkdir(parents=True, exist_ok=True)
        if src.is_dir():
            shutil.copytree(src, dest, dirs_exist_ok=True)
        else:
            shutil.copy2(src, dest)
        n += 1
    if n:
        print(f"[preopen-all] persist restore {date}: {n} paths from {PERSIST}",
              flush=True)
    return n


def snapshot_persist(date: str) -> int:
    """Mirror today's artifacts off the checkout so a later clean cannot wipe them."""
    try:
        PERSIST.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        print(f"[preopen-all] persist mkdir failed: {e}", flush=True)
        return 0
    n = 0
    for src in _date_paths(ROOT, date):
        rel = src.relative_to(ROOT)
        dest = PERSIST / rel
        dest.parent.mkdir(parents=True, exist_ok=True)
        if src.is_dir():
            shutil.copytree(src, dest, dirs_exist_ok=True)
        else:
            shutil.copy2(src, dest)
        n += 1
    if n:
        print(f"[preopen-all] persist snapshot {date}: {n} paths → {PERSIST}",
              flush=True)
    return n


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
    print("  Persist: /home/gha/fullscan-persist survives Actions checkout.")
    print("  Carry-forwards / timeout stubs are trash and fail the job.")
    print("  Grok reads the actual MD/JSON once. Regex is not enough.")
    print("=" * 72)

    skip_writes = False
    restore_persist(date)
    if not force:
        pre = output_qc.preopen_report(date)
        grok_ok = grok_review.prior_ok(date)
        if pre.get("all_ok") and grok_ok:
            print(f"[preopen-all] {date}: every required predictive "
                  f"artifact is already quality-ok "
                  f"(sectors {pre.get('sector_n_ok')}/"
                  f"{pre.get('sector_n_total')}; Grok text review passed) "
                  f"— nothing to do")
            return
        if pre.get("all_ok") and not grok_ok:
            print(f"[preopen-all] {date}: mechanical QC already ok — "
                  f"Grok will read the files as text (no rewrite)")
            skip_writes = True
        else:
            preopen.refuse_if_late("preopen_all", force=force)
    else:
        preopen.refuse_if_late("preopen_all", force=force)

    attempts: list[dict] = []

    def step(key: str, title: str, cmd: list[str]) -> int:
        print(f"\n[preopen-all] → {title}")
        code = _run(cmd)
        attempts.append({"key": key, "title": title, "cmd": cmd,
                         "returncode": code})
        if code != 0:
            print(f"[preopen-all] WARN: {title} exited {code}")
        snapshot_persist(date)
        return code

    fa = _force_args(force)
    py = sys.executable

    if not skip_writes:
        step("news_parse", "News parse",
             [py, "-m", "src.news_parse", "--hours", "48", "--limit", "400",
              "--date", date, *fa])
        step("finviz_digest", "Finviz daily digest",
             [py, "-m", "src.finviz_digest", "--date", date, *fa])
        step("map_heat", "Map heat (industry / captains / tape)",
             [py, "-m", "src.map_heat", "--date", date, *fa])
        step("events", "Event scanner (primary)",
             [py, "-m", "src.run_events", "--date", date, *fa])
        step("events_catcher", "Event catcher (gap hunt, no carry)",
             [py, "-m", "src.run_events_catcher", "--date", date, *fa])
        # Deliberately NO events_fallback — carry is trash for pre-open.
        step("news_judge", "News judge",
             [py, "-m", "src.run_news_judge", "--date", date, *fa])
        # Exhaustive captain research ran post-close. Pre-open does ONE
        # overnight delta refresh only; never 11 sector batches in the
        # time-critical window.
        step("map_heat_research", "Map heat morning delta refresh",
             [py, "-m", "src.map_heat_refresh", "--date", date, *fa])
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

    grok = grok_review.review_preopen(date, mechanical_report=report)
    print("")
    print("-" * 72)
    print("  GROK TEXT REVIEW")
    print("-" * 72)
    print(f"  ok={grok.get('ok')}  {grok.get('notes') or ''}")
    for f in grok.get("fails") or []:
        print(f"  FAIL  {f.get('path')}: {f.get('reason')}")

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
        elif key == "map_heat_research":
            rows = by_kind.get("map_heat_research") or []
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
        "all_ok": bool(report.get("all_ok")) and not missing_required
                  and bool(grok.get("ok")),
        "qc_all_ok": bool(report.get("all_ok")),
        "grok_ok": bool(grok.get("ok")),
        "grok_fails": grok.get("fails") or [],
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
        f"grok_ok={status['grok_ok']}  missing={missing_required or 'none'}",
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
    snapshot_persist(date)

    if missing_required or not report.get("all_ok") or not grok.get("ok"):
        raise SystemExit(
            f"[preopen-all] FAIL {date}: trash or missing required artifacts "
            f"{missing_required or '(see QC/Grok review)'}. "
            f"qc_all_ok={bool(report.get('all_ok'))} grok_ok={bool(grok.get('ok'))}. "
            f"Not committing as success."
        )
    print(f"[preopen-all] PASS {date} — regex QC and Grok text review both ok")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=None)
    ap.add_argument("--force", action="store_true",
                    help="Ignore 09:25 ET cutoff and skip-if-good")
    args = ap.parse_args()
    run(date=args.date, force=args.force)


if __name__ == "__main__":
    main()
