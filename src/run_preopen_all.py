"""One-button PRE-OPEN ALL: morning packet + stock book before 09:30 ET.

Does in one ECS job (per-step skip-if-good, fail-closed QC):

  (Finviz digest + map-heat overlay already landed by GH-hosted
   finviz_preopen_scrape.yml on ubuntu-latest — Elite login, not ECS)
  → wait for GH scrape + last-night captain baseline
  → weather / join / AB (deterministic; unblocks the ranker even if Grok dies)
  → news parse → events (+ catcher) → news judge
  → map heat research (morning delta over last night's baseline)
  → news actions
  → general predict → 11 sector predicts → sector board
  → stock book + paper dashboard  (--with-book, default on)
  → catalyst dossiers (layer 3; optional, AFTER the book)
  → output_qc (regex) → Grok reads the files as text (skipped if prior-ok)

Post-close grades / learn / tonight's captain research live in
src.run_postclose_all — do not run them here.

Live Finviz HTML is NOT scraped here. Aliyun ECS 403s public finviz.com.
GH-hosted ubuntu-latest + Elite login writes digest + overlay ~05:40 ET;
this job waits ~10 min, git-pulls those files, then runs Grok.

CLI:
  python -m src.run_preopen_all [--date YYYY-MM-DD] [--force]
                               [--bypass-cutoff] [--no-book]
                               [--llm-backend auto]
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from . import config, grok_review, output_qc, preopen

ROOT = Path(__file__).resolve().parent.parent
ET = ZoneInfo(config.TZ)
# Lives outside the Actions work tree so checkout --clean cannot wipe a
# finished day. ECS owns /home/gha; GH-hosted ubuntu cannot mkdir that
# (Errno 13) — fall back to $HOME so snapshot/restore still work.


def persist_dir() -> Path:
    """Writable sidecar. Prefer FULLSCAN_PERSIST, then runner HOME."""
    raw = (os.environ.get("FULLSCAN_PERSIST") or "").strip()
    home = (os.environ.get("FULLSCAN_HOME")
            or os.environ.get("HOME") or "").strip()
    candidates: list[Path] = []
    if raw:
        candidates.append(Path(raw))
    if home:
        candidates.append(Path(home) / "fullscan-persist")
    candidates.append(Path("/home/gha/fullscan-persist"))
    seen: set[str] = set()
    for p in candidates:
        key = str(p)
        if key in seen:
            continue
        seen.add(key)
        try:
            p.mkdir(parents=True, exist_ok=True)
            probe = p / ".writable"
            probe.write_text("ok", encoding="utf-8")
            probe.unlink(missing_ok=True)
            return p
        except OSError:
            continue
    fallback = ROOT / ".persist"
    fallback.mkdir(parents=True, exist_ok=True)
    return fallback


# Logical modules this one-button job is responsible for. Keys match
# daily_orchestrator.yml workflow files (minus .yml) where possible.
REQUIRED = [
    ("finviz_digest", "Finviz daily digest", True),
    ("events", "Event scanner", True),
    ("news_parse", "News parse", True),
    ("news_judge", "News judge", True),
    ("map_heat", "Map heat tables (post-close + overlay)", True),
    ("map_heat_baseline", "Map heat post-close baseline", False),
    ("map_heat_research", "Map heat research (captains)", True),
    ("news_actions", "News actions", False),
    ("catalyst", "Catalyst dossiers (layer 3)", False),
    ("general_predict", "General market predict", True),
    ("sector_predict", "Per-sector predict (11)", True),
]


def _today() -> str:
    return datetime.now(ET).date().isoformat()


def _run(cmd: list[str], timeout_s: int | None = None) -> int:
    print(f"\n>>> {' '.join(cmd)}", flush=True)
    try:
        r = subprocess.run(
            cmd, cwd=str(ROOT), env=os.environ.copy(), timeout=timeout_s)
    except subprocess.TimeoutExpired:
        print(f"[preopen-all] WARN: timed out after {timeout_s}s: "
              f"{' '.join(cmd)}", flush=True)
        return 124
    return r.returncode


def _p(*parts: str) -> Path:
    return ROOT.joinpath(*parts)


def _exists(*parts: str) -> bool:
    return _p(*parts).exists()


def _exists_gt(*parts: str, min_bytes: int = 200) -> bool:
    p = _p(*parts)
    try:
        return p.is_file() and p.stat().st_size >= min_bytes
    except OSError:
        return False


def _date_paths(root: Path, date: str) -> list[Path]:
    """Today's predictive artifacts only. Missing paths are omitted."""
    hits: list[Path] = []
    for folder in (
        root / "01_daily",
        root / "01_daily" / "general",
        root / "01_daily" / "events",
        root / "01_daily" / "news",
        root / "01_daily" / "map_heat",
        root / "01_daily" / "catalyst",
        root / "01_daily" / "weather",
        root / "01_daily" / "_transcripts",
        root / "01_daily" / "_channel1",
        root / "data" / "catalyst",
        root / "data" / "universe",
        root / "data" / "join",
        root / "data" / "ab_checklist",
        root / "data" / "peers",
        root / "data" / "stock_book",
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
    persist = persist_dir()
    if not persist.is_dir():
        return 0
    n = 0
    for src in _date_paths(persist, date):
        rel = src.relative_to(persist)
        dest = ROOT / rel
        dest.parent.mkdir(parents=True, exist_ok=True)
        if src.is_dir():
            shutil.copytree(src, dest, dirs_exist_ok=True)
        else:
            shutil.copy2(src, dest)
        n += 1
    if n:
        print(f"[preopen-all] persist restore {date}: {n} paths from {persist}",
              flush=True)
    return n


def snapshot_persist(date: str) -> int:
    """Mirror today's artifacts off the checkout so a later clean cannot wipe them."""
    persist = persist_dir()
    n = 0
    for src in _date_paths(ROOT, date):
        rel = src.relative_to(ROOT)
        dest = persist / rel
        dest.parent.mkdir(parents=True, exist_ok=True)
        if src.is_dir():
            shutil.copytree(src, dest, dirs_exist_ok=True)
        else:
            shutil.copy2(src, dest)
        n += 1
    if n:
        print(f"[preopen-all] persist snapshot {date}: {n} paths → {persist}",
              flush=True)
    return n


def _force_args(force: bool) -> list[str]:
    return ["--force"] if force else []


def _land(date: str, key: str, title: str = "") -> None:
    """QC this step and push it to main before the next file starts."""
    try:
        from . import land_file
        land_file.land(date, key, title=title or key)
    except Exception as e:  # noqa: BLE001 — never abort the packet on land
        print(f"[preopen-all] WARN: land {key} failed: {e}", flush=True)


def _deepseek_credits_ok() -> bool:
    """Do not start essays if DeepSeek is already 402 / no key on ubuntu."""
    try:
        from . import deepseek_client as dc
        hit = dc.credits_preflight()
    except Exception as e:  # noqa: BLE001
        print(f"[preopen-all] WARN: DeepSeek preflight crashed: {e}", flush=True)
        return True
    if hit.get("ok"):
        print(f"[preopen-all] DeepSeek preflight ok ({hit.get('reason')})",
              flush=True)
        return True
    print(f"[preopen-all] DeepSeek preflight FAIL {hit.get('reason')} — "
          f"{hit.get('detail') or ''} essays will empty; skip LLM packet",
          flush=True)
    return False


def _scrape_ready(date: str) -> bool:
    digest = _p("01_daily", "news", f"{date}_finviz_digest.json")
    heat = _p("01_daily", "map_heat", f"{date}_map_heat.json")
    # QC is the skip-if-good gate. overlay_at alone used to look "ready"
    # after an Aliyun 403 stamped morning_overlay with an empty tape.
    if not output_qc.qc_finviz_digest(digest).ok:
        return False
    if not output_qc.qc_map_heat(heat).ok:
        return False
    try:
        payload = json.loads(heat.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return False
    overlay_at = str(payload.get("overlay_at") or "")
    return overlay_at.startswith(date) and bool(payload.get("tape") or [])


def _pull_origin_paths(date: str, paths: list[str], label: str) -> None:
    """Best-effort: take files from origin/main into this work tree."""
    try:
        subprocess.run(
            ["git", "fetch", "origin", "main"],
            cwd=str(ROOT), capture_output=True, timeout=60, check=False,
        )
        listed = subprocess.run(
            ["git", "ls-tree", "-r", "--name-only", "origin/main"],
            cwd=str(ROOT), capture_output=True, text=True, timeout=30,
            check=False,
        )
        have = set((listed.stdout or "").splitlines())
        wanted = [p for p in paths if p in have]
        if not wanted:
            return
        subprocess.run(
            ["git", "checkout", "origin/main", "--", *wanted],
            cwd=str(ROOT), capture_output=True, timeout=30, check=False,
        )
    except (OSError, subprocess.SubprocessError) as e:
        print(f"[preopen-all] {label} pull skipped: {e}", flush=True)


def _pull_scrape_artifacts(date: str) -> None:
    """Best-effort: take GH-hosted digest + overlay from origin/main."""
    _pull_origin_paths(date, [
        f"01_daily/news/{date}_finviz_digest.json",
        f"01_daily/news/{date}_finviz_digest.md",
        "01_daily/news/latest_finviz_digest.md",
        f"01_daily/map_heat/{date}_map_heat.json",
        f"01_daily/map_heat/{date}_map_heat.md",
    ], "scrape")


def _baseline_ready(date: str) -> bool:
    return output_qc.qc_map_heat_baseline(
        _p("01_daily", "map_heat", f"{date}_research_baseline.json")).ok


def _pull_night_baseline(date: str) -> None:
    _pull_origin_paths(date, [
        f"01_daily/map_heat/{date}_research_baseline.json",
        f"01_daily/map_heat/{date}_research_baseline.md",
    ], "baseline")


def wait_for_night_baseline(date: str, timeout_s: int | None = None) -> bool:
    """Night packet must exist before morning research. Do not invent captains."""
    timeout_s = int(os.environ.get("NIGHT_BASELINE_WAIT", timeout_s or 180))
    if _baseline_ready(date):
        print("[preopen-all] night captain baseline already on disk", flush=True)
        return True
    print(f"[preopen-all] waiting up to {timeout_s}s for last-night "
          f"captain baseline", flush=True)
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        _pull_night_baseline(date)
        if _baseline_ready(date):
            print("[preopen-all] night captain baseline landed", flush=True)
            return True
        time.sleep(20)
    print("[preopen-all] WARN: night baseline missing after wait — "
          "trying last session copy (holiday / weekend hole)", flush=True)
    try:
        from .skip_if_good import last_closed_session
        prev = last_closed_session()
    except Exception:
        prev = ""
    if prev and prev != date:
        src = _p("01_daily", "map_heat", f"{prev}_research_baseline.json")
        dest = _p("01_daily", "map_heat", f"{date}_research_baseline.json")
        if src.is_file():
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src, dest)
            md = _p("01_daily", "map_heat", f"{prev}_research_baseline.md")
            if md.is_file():
                shutil.copy2(md, dest.with_suffix(".md"))
            print(f"[preopen-all] copied night baseline {prev} → {date}",
                  flush=True)
            return _baseline_ready(date)
    print("[preopen-all] WARN: night baseline still missing — "
          "map-heat refresh will bootstrap / QC-fail", flush=True)
    return False


def wait_for_gh_scrape(date: str, timeout_s: int | None = None) -> bool:
    """Wait for ubuntu-latest Elite scrape. Do not scrape Finviz on ECS."""
    timeout_s = int(os.environ.get("FINVIZ_SCRAPE_WAIT", timeout_s or 900))
    if _scrape_ready(date):
        print("[preopen-all] GH Finviz scrape already on disk", flush=True)
        return True
    print(f"[preopen-all] waiting up to {timeout_s}s for GH-hosted "
          f"finviz_preopen_scrape (digest + overlay)", flush=True)
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        _pull_scrape_artifacts(date)
        if _scrape_ready(date):
            print("[preopen-all] GH Finviz scrape landed", flush=True)
            return True
        time.sleep(20)
    print("[preopen-all] WARN: GH Finviz scrape not on disk after wait — "
          "QC will fail if digest/overlay missing", flush=True)
    return False


def _github_runs_today(date: str) -> list[dict]:
    """Best-effort: which related workflows actually ran today (ET)."""
    token = os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN") or ""
    repo = os.environ.get("GITHUB_REPOSITORY") or "SRoyaltyy/fullscan"
    if not token:
        return []
    names = [
        "preopen_all.yml",
        "finviz_preopen_scrape.yml",
        "finviz_digest.yml",
        "events_daily.yml",
        "news_parse.yml",
        "news_judge.yml",
        "news_actions.yml",
        "catalyst_daily.yml",
        "daily_pipeline.yml",
        "sector_daily.yml",
        "map_heat_postclose.yml",
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


def _packet_step_done(key: str, date: str) -> bool:
    """True when this one packet step is already quality-ok (no LLM rewrite)."""
    from . import catalyst_daily

    if key == "news_parse":
        return output_qc.qc_news_parse(
            _p("01_daily", "news", f"{date}_parsed.json")).ok
    if key in ("events", "events_catcher"):
        return output_qc.qc_events_date(date).ok
    if key == "news_judge":
        return output_qc.qc_news_judge(
            _p("01_daily", "news", f"{date}_judge.md")).ok
    if key == "map_heat_research":
        return output_qc.qc_map_heat_research(
            _p("01_daily", "map_heat", f"{date}_research.json")).ok
    if key == "news_actions":
        return output_qc.qc_news_actions(
            _p("01_daily", "news", f"{date}_actions.json")).ok
    if key == "general_predict":
        return output_qc.qc_general_predict(
            _p("01_daily", "general", f"{date}_predict.md")).ok
    if key == "sector_predict":
        sector_dir = _p("01_daily", "sectors", date)
        n_ok = 0
        if sector_dir.is_dir():
            for p in sector_dir.glob("*_predict.md"):
                if output_qc.qc_sector_predict(p).ok:
                    n_ok += 1
        return n_ok >= 8
    if key == "sector_board":
        return _exists("01_daily", "sectors", date, "_board.json")
    if key == "weather":
        from . import skip_if_good
        return skip_if_good.check_label_weather(date)
    if key == "catalyst":
        return catalyst_daily.already_good(date)
    return False


def run(date: str | None = None, force: bool = False,
        with_book: bool = True, llm_backend: str | None = None,
        bypass_cutoff: bool = False) -> None:
    date = date or _today()
    if bypass_cutoff:
        os.environ["PREOPEN_BYPASS_CUTOFF"] = "1"
    config.apply_llm_backend(llm_backend)
    print("")
    print("=" * 72)
    print(f"  PRE-OPEN ALL — {date} (America/New_York)")
    print("  Packet + stock book. Must finish before 09:30 ET.")
    print("  Skip-if-good: each quality file for THIS day is not rewritten.")
    print(f"  Persist: {persist_dir()} survives Actions checkout.")
    print("  Carry-forwards / timeout stubs are trash and fail the job.")
    print("=" * 72)

    skip_writes = False
    skip_essays = False
    restore_persist(date)
    clock_late = preopen.past_predict_cutoff()
    late = (not force) and (not bypass_cutoff) and clock_late
    # A late heal must not spend 15 minutes waiting on scrape/baseline
    # even when --bypass-cutoff lets essays run (900s used to eat parse).
    wait_for_gh_scrape(date, timeout_s=45 if clock_late else None)
    wait_for_night_baseline(date, timeout_s=20 if clock_late else None)
    if _scrape_ready(date):
        _land(date, "finviz_digest", "Finviz digest")
        _land(date, "map_heat", "Map heat tables")
    if _baseline_ready(date):
        _land(date, "map_heat_baseline", "Map heat night baseline")
    # 09:25 gates LLM essays, not weather / parse / AB / join / the book.
    if late:
        print(f"[preopen-all] {date}: past 09:25 ET — skip LLM essays; "
              "parse still runs if missing (digest/events fallback)")
        skip_essays = True
    elif not force:
        pre = output_qc.preopen_report(date)
        grok_ok = grok_review.prior_ok(date)
        if pre.get("all_ok") and grok_ok:
            print(f"[preopen-all] {date}: predictive packet already quality-ok "
                  f"(sectors {pre.get('sector_n_ok')}/"
                  f"{pre.get('sector_n_total')}; Grok text review passed) "
                  f"— skip packet writes")
            skip_writes = True
        elif pre.get("all_ok") and not grok_ok:
            print(f"[preopen-all] {date}: mechanical QC already ok — "
                  f"Grok will read the files as text (no rewrite)")
            skip_writes = True

    attempts: list[dict] = []

    llm_steps = {
        # news_parse is file/DB (digest fallback) — not an essay. Keep it
        # off this set so 09:25 cannot skip the last required packet hole.
        "events", "events_catcher", "news_judge",
        "map_heat_research", "news_actions", "general_predict",
        "sector_predict", "catalyst",
    }

    def step(key: str, title: str, cmd: list[str],
             timeout_s: int | None = None) -> int:
        if ((not force) and (not bypass_cutoff) and key in llm_steps
                and preopen.past_predict_cutoff()):
            print(f"[preopen-all] skip {title} (past 09:25 ET — book still runs)")
            attempts.append({"key": key, "title": title, "cmd": cmd,
                             "returncode": 0, "skipped": True})
            return 0
        if not force and _packet_step_done(key, date):
            print(f"[preopen-all] skip {title} (already quality-ok)")
            attempts.append({"key": key, "title": title, "cmd": cmd,
                             "returncode": 0, "skipped": True})
            return 0
        print(f"\n[preopen-all] → {title}")
        code = _run(cmd, timeout_s=timeout_s)
        attempts.append({"key": key, "title": title, "cmd": cmd,
                         "returncode": code})
        if code != 0:
            print(f"[preopen-all] WARN: {title} exited {code}")
        snapshot_persist(date)
        if code == 0 or _packet_step_done(key, date):
            _land(date, key, title)
        return code

    fa = _force_args(force)
    py = sys.executable

    # Weather / join / AB have no LLM clock. Run them before essays so a
    # slow Grok morning cannot leave the ranker with 0/4 book outputs.
    if force or not _exists_gt("data", "universe", f"{date}_membership.csv",
                               min_bytes=50_000):
        print("[preopen-all] → Universe labels (segments)")
        _run([py, "-m", "src.segments", "--date", date], timeout_s=180)
        snapshot_persist(date)
        _land(date, "universe", "Universe labels")
    # 2026-09-09: live Channel 1 ate 180s (exit 124). Bound live, then offline.
    step("weather", "Weather / regime",
         [py, "-m", "src.weather", "--date", date], timeout_s=50)
    from . import skip_if_good
    if not skip_if_good.check_label_weather(date):
        print("[preopen-all] weather missing/thin — retry --offline")
        _run([py, "-m", "src.weather", "--date", date, "--offline"],
             timeout_s=60)
        snapshot_persist(date)
        _land(date, "weather", "Weather / regime (offline retry)")
    if force or not _exists_gt("data", "join", f"{date}_ranked.csv",
                               min_bytes=5_000):
        print("[preopen-all] → Join / match rank")
        _run([py, "-m", "src.join", "--date", date], timeout_s=180)
        snapshot_persist(date)
        _land(date, "join", "Join / match rank")
    if force or not _exists_gt("data", "ab_checklist",
                               f"{date}_ab_checklist_enriched.csv",
                               min_bytes=5_000):
        if not _exists_gt("data", "ab_checklist",
                          f"{date}_ab_checklist.csv", min_bytes=10_000):
            print("[preopen-all] → AB checklist")
            _run([py, "-m", "src.ab_checklist", "--date", date],
                 timeout_s=1500)
        print("[preopen-all] → AB enrich")
        _run([py, "-m", "src.ab_enrich", "--date", date], timeout_s=180)
        snapshot_persist(date)
        _land(date, "ab", "AB checklist")

    if not skip_writes and not skip_essays and not _deepseek_credits_ok():
        print("[preopen-all] skip LLM essays (DeepSeek credits); "
              "parse still runs if missing")
        skip_essays = True

    if not skip_writes:
        # Parse is required on the day-board and does not need DeepSeek —
        # digest / Channel 1 cache / events are enough when Postgres dies.
        parse_t = 120
        step("news_parse", "News parse",
             [py, "-m", "src.news_parse", "--hours", "48", "--limit", "400",
              "--date", date, *fa], timeout_s=parse_t)
        parsed_p = _p("01_daily", "news", f"{date}_parsed.json")
        if not output_qc.qc_news_parse(parsed_p).ok:
            print("[preopen-all] parse thin — retry --limit 80 (file/DB)")
            _run([py, "-m", "src.news_parse", "--hours", "48",
                  "--limit", "80", "--date", date, *fa], timeout_s=90)
            snapshot_persist(date)
            if output_qc.qc_news_parse(parsed_p).ok:
                _land(date, "news_parse", "News parse (limit-80 retry)")

    if not skip_writes and not skip_essays:
        # A single hung Grok call at 10800s ate 2026-09-04 (0 essays on ECS).
        # Morning packet must fail over to DeepSeek in minutes, not hours.
        prev_llm_to = os.environ.get("OPENCLAW_TIMEOUT")
        morning_to = os.environ.get("PREOPEN_LLM_TIMEOUT", "420")
        os.environ["OPENCLAW_TIMEOUT"] = morning_to
        try:
            llm_sub_t = max(120, int(morning_to) + 60)
        except ValueError:
            llm_sub_t = 480
        print(f"[preopen-all] morning LLM HTTP timeout {morning_to}s "
              f"subprocess {llm_sub_t}s "
              "(hung Grok fails over; 10800s ate 2026-09-04)")
        try:
            step("events", "Event scanner (primary)",
                 [py, "-m", "src.run_events", "--date", date, *fa],
                 timeout_s=llm_sub_t)
            step("events_catcher", "Event catcher (gap hunt, no carry)",
                 [py, "-m", "src.run_events_catcher", "--date", date, *fa],
                 timeout_s=llm_sub_t)
            # Actions before judge so the same-run fallback can stitch them.
            step("news_actions", "News actions",
                 [py, "-m", "src.news_actions", "--hours", "48", "--limit", "400",
                  "--date", date, *fa], timeout_s=llm_sub_t)
            step("news_judge", "News judge",
                 [py, "-m", "src.run_news_judge", "--date", date, *fa],
                 timeout_s=llm_sub_t)
            # Required essays BEFORE optional map-heat (540s ate general
            # on 2026-09-09 inside the 55m step).
            step("general_predict", "General market predict",
                 [py, "-m", "src.run_predict", "--date", date, *fa],
                 timeout_s=llm_sub_t)
            step("sector_predict", "Per-sector predict (all 11)",
                 [py, "-m", "src.run_sector_predict", "--date", date, *fa],
                 timeout_s=2400)
            print("[preopen-all] → Sector board (rebuild after essays)")
            _run([py, "-m", "src.sector_board", "--date", date], timeout_s=60)
            _land(date, "sector_board", "Sector board")
            prev_timeout = os.environ.get("OPENCLAW_TIMEOUT")
            map_heat_http = os.environ.get("MAP_HEAT_REFRESH_TIMEOUT", "480")
            os.environ["OPENCLAW_TIMEOUT"] = map_heat_http
            try:
                try:
                    map_heat_sub = max(180, int(map_heat_http) + 60)
                except ValueError:
                    map_heat_sub = 540
                heat_code = step(
                    "map_heat_research", "Map heat morning delta refresh",
                    [py, "-m", "src.map_heat_refresh", "--date", date, *fa],
                    timeout_s=map_heat_sub)
                if heat_code == 124:
                    print("[preopen-all] map heat refresh timed out — "
                          "writing night baseline passthrough (no 2nd 21m wait)")
                    step("map_heat_research",
                         "Map heat passthrough after timeout",
                         [py, "-m", "src.map_heat_refresh", "--date", date,
                          "--passthrough", *fa],
                         timeout_s=60)
            finally:
                os.environ["OPENCLAW_TIMEOUT"] = prev_timeout or morning_to
        finally:
            if prev_llm_to is None:
                os.environ.pop("OPENCLAW_TIMEOUT", None)
            else:
                os.environ["OPENCLAW_TIMEOUT"] = prev_llm_to

    # Predicts are optional weather inputs. Refresh join so s_join
    # sees the same-day general/sector essays when they landed.
    # Do not use step() — skip-if-good would skip this refresh.
    # Book is next. Catalyst / Grok review wait until BUY/SELL is on disk —
    # 2026-09-02 eight dossiers ate the morning and the ranker never started.
    print("[preopen-all] → Weather / join refresh (before book)")
    _run([py, "-m", "src.weather", "--date", date], timeout_s=50)
    if not skip_if_good.check_label_weather(date):
        print("[preopen-all] weather refresh thin — retry --offline")
        _run([py, "-m", "src.weather", "--date", date, "--offline"],
             timeout_s=60)
    _run([py, "-m", "src.join", "--date", date], timeout_s=180)
    snapshot_persist(date)

    book_ok = True
    if with_book:
        from . import run_stock_book_all, skip_if_good
        already = (not force) and skip_if_good.check_stock_book_all(date)
        # ubuntu land_book may have written green.json before essays.
        # Re-rank whenever essays had a chance to land (not a late 09:25 heal).
        if already and late:
            print(f"[preopen-all] skip stock book (already on disk for {date}; "
                  "past 09:25 — not replacing with an emptier rank)")
        else:
            print(f"\n[preopen-all] → Stock book + paper dashboard ({date})")
            try:
                # Essays already ran (or packet was already ok). Rank only —
                # paper/sleeve/catalyst must not delay green.json.
                run_stock_book_all.run(
                    date=date, force=force, skip_llm=True,
                    skip_extras=True, refresh_ranker=True, top=25)
            except SystemExit as e:
                print(f"[preopen-all] WARN: stock book exited: {e}")
            except Exception as e:  # noqa: BLE001 — still publish what we wrote
                print(f"[preopen-all] WARN: stock book crashed: {e}")
        snapshot_persist(date)
        book_ok = skip_if_good.check_stock_book_all(date)
        if not book_ok:
            print(f"[preopen-all] WARN: stock book still missing for {date}")
        else:
            # Push the ranker layer now so 09:30 sees BUY/SELL even if
            # paper/sleeve/catalyst hang after this.
            print("[preopen-all] → push book + green.json + ranker inputs")
            _land(date, "stock_book", "Stock book + green")
            if force or not preopen.past_predict_cutoff():
                print("[preopen-all] → paper / sleeve (after book is on main)")
                _run([py, "-m", "src.paper_trade", "--date", date, "--top", "10"])
                _land(date, "paper", "Paper dashboard")
                _run([py, "-m", "src.sleeve_combine_bt",
                      "--mode", "io_boost", "--hold", "3d"])
                print("[preopen-all] → flatten_hard_red live card (after book)")
                _run([py, "-m", "src.sleeve_merge", "--card",
                      "--date", date, "--write-card"])
                snapshot_persist(date)
                _land(date, "flatten", "Flatten live card")
    else:
        print("[preopen-all] --no-book: leaving stock book to a later click")

    if not skip_writes and not skip_essays:
        step("catalyst", "Catalyst dossiers (after book)",
             [py, "-m", "src.catalyst_daily", "--date", date, *fa],
             timeout_s=1800)

    qc_path = output_qc.write_preopen_report(date)
    report = output_qc.preopen_report(date)
    print("")
    print(output_qc.render(report))
    print(f"[preopen-all] wrote {qc_path}")

    if (not force) and grok_review.prior_ok(date):
        grok = {"ok": True, "notes": "prior Grok text review still good — skipped",
                "fails": []}
        print("[preopen-all] skip Grok text review (prior_ok)")
    elif (not force) and (not bypass_cutoff) and preopen.past_predict_cutoff():
        grok = {"ok": True, "notes": "past 09:25 ET — skipped; book already landed",
                "fails": []}
        print("[preopen-all] skip Grok text review (past 09:25 ET)")
    else:
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
        elif key == "catalyst":
            from . import catalyst_daily
            ok = catalyst_daily.already_good(date)
            detail = "OK grok_native" if ok else "missing / not grok_native"
        elif key == "finviz_digest":
            rows = by_kind.get("finviz_digest") or []
            ok = bool(rows) and all(r.get("ok") for r in rows)
            detail = rows[0].get("reason") if rows and not ok else ""
            detail = detail or ("OK" if ok else "missing")
        elif key == "map_heat":
            rows = by_kind.get("map_heat") or []
            ok = bool(rows) and all(r.get("ok") for r in rows)
            detail = rows[0].get("reason") if rows and not ok else ""
            detail = detail or ("OK" if ok else "missing")
        elif key == "map_heat_baseline":
            rows = by_kind.get("map_heat_baseline") or []
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
                  and bool(grok.get("ok")) and (not with_book or book_ok),
        "qc_all_ok": bool(report.get("all_ok")),
        "book_ok": book_ok,
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
        f"book_ok={status['book_ok']}  grok_ok={status['grok_ok']}  "
        f"missing={missing_required or 'none'}",
        "",
        "Predictive modules + stock book (must land before 09:30 ET).",
        "Outcome / learn / tonight's captain research = Post-Close ALL.",
        "",
    ]
    for a in attempts:
        md_lines.append(f"- {a['title']}: exit {a['returncode']}")
    md_path.write_text("\n".join(md_lines) + "\n", encoding="utf-8")
    print(f"[preopen-all] wrote {status_path}")
    print(f"[preopen-all] wrote {md_path}")
    snapshot_persist(date)
    _land(date, "status", "Pre-open status packet")

    degraded = bool(
        missing_required or not report.get("all_ok") or not grok.get("ok")
        or (with_book and not book_ok)
    )
    if degraded:
        print(
            f"[preopen-all] DEGRADED {date}: wrote whatever landed and will "
            f"still commit/publish. missing={missing_required or 'none'} "
            f"qc_all_ok={bool(report.get('all_ok'))} grok_ok={bool(grok.get('ok'))} "
            f"book_ok={book_ok}. Exit 0 so git + Pages still run."
        )
        return
    print(f"[preopen-all] PASS {date} — packet"
          f"{' + stock book' if with_book else ''} ok")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=None)
    ap.add_argument("--force", action="store_true",
                    help="Ignore 09:25 ET cutoff AND skip-if-good (full rewrite)")
    ap.add_argument("--bypass-cutoff", action="store_true",
                    help="Ignore 09:25 ET cutoff only; skip-if-good still on")
    ap.add_argument("--no-book", action="store_true",
                    help="Packet only — do not rank or paper-trade")
    ap.add_argument("--llm-backend", default=None,
                    choices=["auto", "grok", "deepseek"],
                    help="auto=Grok then DeepSeek; grok=Grok only; deepseek=no Grok")
    args = ap.parse_args()
    run(date=args.date, force=args.force,
        with_book=not args.no_book, llm_backend=args.llm_backend,
        bypass_cutoff=args.bypass_cutoff)


if __name__ == "__main__":
    main()
