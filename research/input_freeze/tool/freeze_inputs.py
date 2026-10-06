#!/usr/bin/env python3
"""Daily pre-open input freeze (research/input_freeze/<YYYY-MM-DD>/).

Copies the newest version of each daily model input that was committed
BEFORE the freeze time (about 09:20 ET) into an append-only day folder,
with a manifest.json that pins every file to its source commit, commit
time and sha256. A day folder, once written, is never rewritten.

Stdlib only. Subcommands:
  gate    decide freeze / sleep / early / late / exists / closed for today
  freeze  write (or with --dry-run, only print) the day folder
  late    write the 'late: not frozen' manifest for a day
  check   fail if any existing day folder changed between two commits
  plan    print the input spec

Never touches anything outside research/input_freeze/.
"""
from __future__ import annotations

import argparse
import datetime as dt
import gzip
import hashlib
import json
import os
import re
import subprocess
import sys
from pathlib import Path
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")
UTC = dt.timezone.utc
SCHEMA = "input_freeze/v1"
FREEZE_ROOT = "research/input_freeze"
DAY_RE = re.compile(r"^research/input_freeze/(\d{4}-\d{2}-\d{2})/")
FIRST_DATE = "2026-10-07"          # no folders (not even 'late') before this
TARGET_ET = (9, 20)                 # freeze at 09:20 ET
LATEST_START_ET = (9, 28)           # do not start a freeze at/after 09:28 ET
OPEN_ET = (9, 30)
MAX_WAIT_MIN = 210                  # a trigger earlier than this just exits
GZIP_OVER = 64 * 1024               # gzip stored copies above 64 KB
COPY_CAP = 12 * 1024 * 1024         # above 12 MB raw: pointer only (sha+commit)
DEFAULT_MAX_LAG = 4                 # calendar days (Fri -> Mon + one holiday)

# NYSE full-day closures. Early closes still open at 09:30 and are frozen.
NYSE_HOLIDAYS = {
    "2026-01-01", "2026-01-19", "2026-02-16", "2026-04-03", "2026-05-25",
    "2026-06-19", "2026-07-03", "2026-09-07", "2026-11-26", "2026-12-25",
    "2027-01-01", "2027-01-18", "2027-02-15", "2027-03-26", "2027-05-31",
    "2027-06-18", "2027-07-05", "2027-09-06", "2027-11-25", "2027-12-24",
}

# (category, repo, path pattern, options). {date} = YYYY-MM-DD; the newest
# date <= the freeze date wins. Fixed paths copy the version at the cutoff.
SPEC: list[tuple[str, str, str, dict]] = [
    # Finviz pre-open scrape + digest
    ("finviz_preopen", "fullscan", "01_daily/news/{date}_finviz_digest_preopen.json", {"max_lag": 0}),
    ("finviz_preopen", "fullscan", "01_daily/news/{date}_finviz_digest.json", {"max_lag": 0}),
    ("finviz_preopen", "fullscan", "01_daily/news/{date}_finviz_digest.md", {"max_lag": 0}),
    ("finviz_preopen", "fullscan", "data/exports/finviz_{date}.csv", {}),
    ("finviz_preopen", "fullscan", "data/exports/finviz_{date}.scraped_at", {}),
    # Finviz homepage market digest (morning) + last session's close digest
    ("finviz_homepage", "fullscan", "01_daily/news/{date}_finviz_market_digest.json", {"max_lag": 0}),
    ("finviz_homepage", "fullscan", "01_daily/news/{date}_finviz_market_digest.md", {"max_lag": 0}),
    ("finviz_homepage", "fullscan", "01_daily/news/{date}_finviz_market_digest_close.json", {"before_day": True}),
    # Pre-Open ALL status + outputs
    ("preopen_all", "fullscan", "01_daily/{date}_preopen_status.json", {"max_lag": 0}),
    ("preopen_all", "fullscan", "01_daily/{date}_preopen_status.md", {"max_lag": 0}),
    ("preopen_all", "fullscan", "01_daily/{date}_preopen_qc.json", {"max_lag": 0}),
    ("preopen_all", "fullscan", "01_daily/{date}_grok_review.json", {"max_lag": 0}),
    ("preopen_all", "fullscan", "01_daily/general/{date}_predict.md", {"max_lag": 0}),
    ("preopen_all", "fullscan", "01_daily/sectors/{date}/*_predict.md", {"max_lag": 0}),
    ("preopen_all", "fullscan", "01_daily/sectors/{date}/_board.json", {"max_lag": 0}),
    ("preopen_all", "fullscan", "01_daily/sectors/{date}/_qc.json", {"max_lag": 0}),
    ("preopen_all", "fullscan", "01_daily/events/{date}_events.json", {"max_lag": 0}),
    ("preopen_all", "fullscan", "01_daily/news/{date}_parsed.json", {"max_lag": 0}),
    ("preopen_all", "fullscan", "01_daily/news/{date}_judge.json", {"max_lag": 0}),
    ("preopen_all", "fullscan", "01_daily/news/{date}_judge.md", {"max_lag": 0}),
    ("preopen_all", "fullscan", "01_daily/news/{date}_actions.json", {"max_lag": 0}),
    ("preopen_all", "fullscan", "01_daily/map_heat/{date}_map_heat.json", {"max_lag": 0}),
    ("preopen_all", "fullscan", "01_daily/map_heat/{date}_research.json", {"max_lag": 0}),
    ("preopen_all", "fullscan", "01_daily/weather/{date}_weather.json", {"max_lag": 0}),
    ("preopen_all", "fullscan", "01_daily/catalyst/{date}_dossiers.json", {"max_lag": 0}),
    ("preopen_all", "fullscan", "01_daily/_channel1/{date}_predict.json", {"max_lag": 0}),
    # Stock Book (latest) + ranker inputs
    ("stock_book", "fullscan", "data/stock_book/{date}_stock_book.json", {}),
    ("stock_book", "fullscan", "data/stock_book/{date}_green.json", {}),
    ("stock_book", "fullscan", "data/stock_book/{date}_suggestions.json", {}),
    ("stock_book", "fullscan", "data/stock_book/{date}_input_health.json", {}),
    ("stock_book", "fullscan", "01_daily/{date}_stock_book.md", {}),
    ("stock_book", "fullscan", "data/join/{date}_ranked.csv", {}),
    ("stock_book", "fullscan", "data/peers/{date}_peer_rs.csv", {}),
    # Macro / free news intake
    ("macro_news", "fullscan", "01_daily/news/{date}_intake.md", {}),
    ("macro_news", "fullscan", "data/news_intake/{date}/headline_groups.json", {}),
    ("macro_news", "fullscan", "data/news_intake/{date}/review_queue.json", {}),
    ("macro_news", "fullscan", "data/news_intake/{date}/parsed.json", {}),
    # Lane outputs
    ("lane", "fullscan", "01_daily/news/{date}_lane_sectors.json", {}),
    ("lane", "fullscan", "02_lessons/lane/outbox_sectors/{date}.json", {}),
    ("lane", "fullscan", "02_lessons/lane/outbox/{date}.json", {}),
    ("lane", "fullscan", "data/news_intake/{date}/lane_queue.json", {}),
    # AB scores + checklist
    ("ab", "fullscan", "data/ab_checklist/{date}_ab_checklist.json", {}),
    ("ab", "fullscan", "data/ab_checklist/{date}_ab_checklist.csv", {}),
    ("ab", "fullscan", "data/ab_checklist/{date}_ab_checklist_enriched.csv", {}),
    ("ab", "fullscan", "data/checklist/{date}_checklist.json", {}),
    # Theme Radar (fullscan copy + theme-radar repo)
    ("theme_radar", "fullscan", "data/theme_radar_snapshots/{date}.csv.gz", {}),
    ("theme_radar", "fullscan", "data/theme_radar_snapshots/MANIFEST.json", {}),
    ("theme_radar", "fullscan", "data/theme_radar/oppset_clock_b/oppset_flagged.csv", {}),
    ("theme_radar", "theme-radar", "data/snapshots/{date}.csv", {}),
    ("theme_radar", "theme-radar", "data/scores/{date}_1d.csv", {}),
    ("theme_radar", "theme-radar", "data/scores/{date}_1w.csv", {}),
    ("theme_radar", "theme-radar", "data/scores/{date}_1m.csv", {}),
    ("theme_radar", "theme-radar", "data/scores/{date}_segments.csv", {}),
    ("theme_radar", "theme-radar", "data/composite/{date}_composite_rank.csv", {}),
    ("theme_radar", "theme-radar", "data/composite/{date}_y_snapshot.json", {}),
    ("theme_radar", "theme-radar", "research/shadow_log/plans/plan_{date}.csv", {"max_lag": 0}),
    ("theme_radar", "theme-radar", "research/shadow_log/plans/letters_{date}.csv", {"max_lag": 0}),
    # Excel bot (Grok Update sheets are not in a repo -> recorded missing)
    ("excel_bot", "fullscan", "excel_bot/suggestions/suggestions.csv", {}),
    ("excel_bot", "fullscan", "excel_bot/freeze_manifest.json", {}),
    ("excel_bot", "fullscan", "excel_bot/daily/{date}_excel_bot.md", {"before_day": True}),
    ("excel_bot", "fullscan", "excel_bot/daily/{date}_excel_bot_draft.md", {}),
    ("grok_update_sheets", "external", "Grok Update sheets (not in a git repo)", {}),
    # JEV / insider / hype-factor / day-movers
    ("jev", "fullscan", "01_daily/news/{date}_jev_keep.json", {}),
    ("jev", "fullscan", "dashboard/news-dir/scores/latest.json", {}),
    ("insider", "fullscan", "data/insider/{date}_insider.json", {}),
    ("insider", "fullscan", "data/insider/{date}_insider_trades.csv", {}),
    ("hype_factor", "fullscan", "data/grok_automations/{date}_hype-factor.json", {}),
    ("day_movers", "fullscan", "dashboard/day-movers/days.json", {}),
    ("day_movers", "fullscan", "dashboard/day-movers/cam-index.json", {}),
    # Strategy tickets as they stand at freeze time
    ("strategy_tickets", "fullscan", "data/day_board/{date}_strategy_tickets.json", {"max_lag": 0}),
    ("strategy_tickets", "fullscan", "data/day_board/{date}_tickets.json", {"max_lag": 0}),
    ("strategy_tickets", "fullscan", "data/day_board/{date}.json", {"max_lag": 0}),
    ("strategy_tickets", "fullscan", "data/factor_mine/send_inputs/{date}.json", {"max_lag": 0}),
    ("strategy_tickets", "fullscan", "data/day_board/today_strategies.json", {}),
]


# ---------------------------------------------------------------- clock
def parse_now(s: str | None) -> dt.datetime:
    if not s:
        return dt.datetime.now(UTC)
    t = dt.datetime.fromisoformat(s.replace("Z", "+00:00"))
    if t.tzinfo is None:
        t = t.replace(tzinfo=UTC)
    return t.astimezone(UTC)


def et_at(day: str, hm: tuple[int, int]) -> dt.datetime:
    d = dt.date.fromisoformat(day)
    return dt.datetime(d.year, d.month, d.day, hm[0], hm[1], tzinfo=ET).astimezone(UTC)


def is_session(day: str) -> bool:
    return dt.date.fromisoformat(day).weekday() < 5 and day not in NYSE_HOLIDAYS


def iso_z(t: dt.datetime) -> str:
    return t.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def gate(now: dt.datetime, folder_exists: bool, day: str | None = None) -> dict:
    """Decision for one trigger. Never asks to rewrite an existing folder."""
    day = day or now.astimezone(ET).date().isoformat()
    target, latest, open_ = et_at(day, TARGET_ET), et_at(day, LATEST_START_ET), et_at(day, OPEN_ET)
    out = {"date": day, "now_utc": iso_z(now), "target_utc": iso_z(target),
           "open_utc": iso_z(open_), "sleep_s": 0}
    if day < FIRST_DATE:
        return {**out, "action": "skip", "why": f"before first freeze date {FIRST_DATE}"}
    if not is_session(day):
        return {**out, "action": "closed", "why": "weekend or NYSE holiday"}
    if folder_exists:
        return {**out, "action": "exists", "why": "day folder already written (append-only)"}
    if now >= latest:
        return {**out, "action": "late", "why": f"fired at {iso_z(now)}, at/after 09:28 ET"}
    wait = (target - now).total_seconds()
    if wait > MAX_WAIT_MIN * 60:
        return {**out, "action": "early", "why": f"{wait/60:.0f} min before 09:20 ET; a later trigger owns it"}
    return {**out, "action": "freeze", "sleep_s": max(0, int(wait)),
            "why": "sleep to 09:20 ET then freeze" if wait > 0 else "freeze now"}


# ------------------------------------------------------------------ git
def git(repo: Path, *args: str, binary: bool = False):
    p = subprocess.run(["git", "-C", str(repo), *args], capture_output=True, check=False)
    if p.returncode != 0:
        raise RuntimeError(f"git {' '.join(args)}: {p.stderr.decode(errors='replace').strip()}")
    return p.stdout if binary else p.stdout.decode("utf-8", "replace")


def cutoff_commit(repo: Path, ref: str, freeze: dt.datetime) -> tuple[str, str] | None:
    """Newest first-parent commit on ref with committer time < freeze."""
    out = git(repo, "log", "--first-parent", "--format=%H %ct", ref).splitlines()
    ts = freeze.timestamp()
    for line in out:
        sha, ct = line.split()
        if int(ct) < ts:
            return sha, iso_z(dt.datetime.fromtimestamp(int(ct), UTC))
    return None


def last_touch(repo: Path, commit: str, path: str) -> tuple[str, dt.datetime]:
    line = git(repo, "log", "-1", "--format=%H %ct", commit, "--", path).strip()
    sha, ct = line.split()
    return sha, dt.datetime.fromtimestamp(int(ct), UTC)


def pattern_regex(pat: str) -> re.Pattern:
    rx = re.escape(pat).replace(re.escape("{date}"), r"(\d{4}-\d{2}-\d{2})")
    rx = rx.replace(r"\*", r"[^/]+")
    return re.compile("^" + rx + "$")


def pick(paths: list[str], pat: str, day: str, before_day: bool = False) -> tuple[str | None, list[str]]:
    """(chosen as-of date, matching paths for it). Fixed paths: (None, [path])."""
    if "{date}" not in pat:
        rx = pattern_regex(pat)
        return None, sorted(p for p in paths if rx.match(p))
    rx = pattern_regex(pat)
    best: dict[str, list[str]] = {}
    for p in paths:
        m = rx.match(p)
        if not m:
            continue
        d = m.group(1)
        if d > day or (before_day and d >= day):
            continue
        best.setdefault(d, []).append(p)
    if not best:
        return None, []
    d = max(best)
    return d, sorted(best[d])


def lag_days(asof: str, day: str) -> int:
    return (dt.date.fromisoformat(day) - dt.date.fromisoformat(asof)).days


# ---------------------------------------------------------------- build
def build(day: str, freeze: dt.datetime, repos: dict[str, Path | None],
          refs: dict[str, str] | None = None) -> tuple[dict, dict[str, bytes]]:
    """Return (manifest, {stored_relpath: bytes}). Pure read of git objects."""
    refs = refs or {}
    sources: dict[str, dict] = {}
    trees: dict[str, list[str]] = {}
    for name, path in repos.items():
        if path is None or not (path / ".git").exists():
            sources[name] = {"status": "missing", "why": "repo not readable"}
            continue
        try:
            cc = cutoff_commit(path, refs.get(name, "HEAD"), freeze)
        except RuntimeError as e:
            sources[name] = {"status": "missing", "why": str(e)[:200]}
            continue
        if not cc:
            sources[name] = {"status": "missing", "why": "no commit before freeze time"}
            continue
        sources[name] = {"status": "ok", "cutoff_commit": cc[0], "cutoff_commit_time_utc": cc[1]}
        trees[name] = git(path, "ls-tree", "-r", "--name-only", cc[0]).splitlines()

    files: list[dict] = []
    blobs: dict[str, bytes] = {}
    for cat, repo, pat, opt in SPEC:
        base = {"category": cat, "repo": repo, "pattern": pat}
        if repo not in trees:
            files.append({**base, "status": "missing",
                          "why": "not in a git repo" if repo == "external" else f"{repo} not readable"})
            continue
        asof, hits = pick(trees[repo], pat, day, bool(opt.get("before_day")))
        if not hits:
            files.append({**base, "status": "missing", "why": "no version committed before freeze"})
            continue
        lag = lag_days(asof, day) if asof else None
        max_lag = opt.get("max_lag", DEFAULT_MAX_LAG)
        for p in hits:
            sha, ct = last_touch(repos[repo], sources[repo]["cutoff_commit"], p)
            row = {**base, "source_path": p, "as_of_date": asof, "lag_days": lag,
                   "source_commit": sha, "source_commit_time_utc": iso_z(ct)}
            if ct >= freeze:  # cannot happen with a cutoff tree; kept as a hard rule
                files.append({**row, "status": "after_freeze"})
                continue
            data = git(repos[repo], "show", f"{sources[repo]['cutoff_commit']}:{p}", binary=True)
            row.update({"bytes": len(data), "sha256": hashlib.sha256(data).hexdigest()})
            if lag is not None and lag > max_lag:
                files.append({**row, "status": "stale", "why": f"newest is {lag}d old (max {max_lag}); pointer only"})
                continue
            if len(data) > COPY_CAP:
                files.append({**row, "status": "pointer_only", "why": f"> {COPY_CAP // 2**20} MB; pinned by commit + sha256"})
                continue
            gz = len(data) > GZIP_OVER and not p.endswith(".gz")
            stored = gzip.compress(data, compresslevel=9, mtime=0) if gz else data
            rel = f"files/{repo}/{p}" + (".gz" if gz else "")
            blobs[rel] = stored
            files.append({**row, "status": "copied", "stored_path": rel, "gzip": gz,
                          "stored_bytes": len(stored),
                          "stored_sha256": hashlib.sha256(stored).hexdigest()})

    cats: dict[str, dict] = {}
    for f in files:
        c = cats.setdefault(f["category"], {"copied": 0, "pointer_only": 0, "stale": 0, "missing": 0, "after_freeze": 0})
        c[f["status"]] += 1
    manifest = {
        "schema": SCHEMA, "date": day, "status": "frozen",
        "freeze_time_utc": iso_z(freeze), "open_time_utc": iso_z(et_at(day, OPEN_ET)),
        "rule": "only versions whose source commit time < freeze_time_utc; folder is append-only",
        "sources": sources, "categories": cats,
        "totals": {"files": len(files),
                   "copied": sum(1 for f in files if f["status"] == "copied"),
                   "stored_bytes": sum(len(b) for b in blobs.values())},
        "files": files,
    }
    return manifest, blobs


def write_day(out_root: Path, day: str, manifest: dict, blobs: dict[str, bytes]) -> Path:
    folder = out_root / day
    if folder.exists():
        raise SystemExit(f"[input-freeze] REFUSE: {folder} exists (append-only)")
    for rel, data in sorted(blobs.items()):
        dst = folder / rel
        dst.parent.mkdir(parents=True, exist_ok=True)
        dst.write_bytes(data)
    (folder / "manifest.json").write_text(json.dumps(manifest, indent=1, sort_keys=False) + "\n", encoding="utf-8")
    return folder


def late_manifest(g: dict, meta: dict) -> dict:
    return {"schema": SCHEMA, "date": g["date"], "status": "late: not frozen",
            "fired_utc": g["now_utc"], "open_time_utc": g["open_utc"],
            "why": g["why"], **meta, "files": []}


# ---------------------------------------------------------------- check
def changed_day_files(repo: Path, base: str, head: str) -> list[str]:
    """Paths under an existing (in base) day folder that differ in head."""
    base_files = [p for p in git(repo, "ls-tree", "-r", "--name-only", base, "--", FREEZE_ROOT).splitlines()
                  if DAY_RE.match(p)]
    base_days = {DAY_RE.match(p).group(1) for p in base_files}
    diff = git(repo, "diff", "--name-status", "--no-renames", base, head, "--", FREEZE_ROOT).splitlines()
    bad = []
    for line in diff:
        status, path = line.split("\t", 1)
        m = DAY_RE.match(path)
        if m and m.group(1) in base_days:
            bad.append(f"{status} {path}")
    return bad


def only_new_day(repo: Path, base: str, head: str, day: str) -> list[str]:
    """Every path changed base..head must be under research/input_freeze/<day>/."""
    diff = git(repo, "diff", "--name-only", "--no-renames", base, head).splitlines()
    return [p for p in diff if not p.startswith(f"{FREEZE_ROOT}/{day}/")]


# ------------------------------------------------------------------ cli
def folder_exists(out_root: str, day: str, ref: str = "", repo: str = ".") -> bool:
    """Day folder present on ref (e.g. origin/main) or, without ref, on disk."""
    if not ref:
        return (Path(out_root) / day).exists()
    return bool(git(Path(repo), "ls-tree", "--name-only", ref, f"{out_root}/{day}").strip())


def _out(kv: dict) -> None:
    gh = os.environ.get("GITHUB_OUTPUT")
    if gh:
        with open(gh, "a", encoding="utf-8") as fh:
            for k, v in kv.items():
                fh.write(f"{k}={v}\n")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    g = sub.add_parser("gate"); g.add_argument("--now"); g.add_argument("--out-root", default=FREEZE_ROOT)
    g.add_argument("--ref", default="", help="check the day folder on this ref (e.g. origin/main)")
    f = sub.add_parser("freeze")
    f.add_argument("--date"); f.add_argument("--now", help="freeze time (default now)")
    f.add_argument("--fullscan", default="."); f.add_argument("--theme-radar", default="")
    f.add_argument("--fullscan-ref", default="HEAD"); f.add_argument("--theme-radar-ref", default="HEAD")
    f.add_argument("--out-root", default=FREEZE_ROOT); f.add_argument("--dry-run", action="store_true")
    f.add_argument("--meta", default="{}")
    f.add_argument("--at-target", action="store_true", help="freeze time = 09:20 ET on --date (dry runs)")
    lt = sub.add_parser("late"); lt.add_argument("--now"); lt.add_argument("--out-root", default=FREEZE_ROOT)
    lt.add_argument("--ref", default="")
    lt.add_argument("--meta", default="{}"); lt.add_argument("--dry-run", action="store_true")
    c = sub.add_parser("check"); c.add_argument("--base", required=True); c.add_argument("--head", default="HEAD")
    c.add_argument("--repo", default="."); c.add_argument("--only-day", default="")
    sub.add_parser("plan")
    a = ap.parse_args(argv)

    if a.cmd == "plan":
        for row in SPEC:
            print("\t".join([row[0], row[1], row[2], json.dumps(row[3])]))
        return 0

    if a.cmd == "gate":
        now = parse_now(a.now)
        day = now.astimezone(ET).date().isoformat()
        d = gate(now, folder_exists(a.out_root, day, a.ref), day)
        print(json.dumps(d))
        _out({"action": d["action"], "date": d["date"], "sleep_s": d["sleep_s"]})
        return 0

    if a.cmd == "late":
        now = parse_now(a.now)
        day = now.astimezone(ET).date().isoformat()
        d = gate(now, folder_exists(a.out_root, day, a.ref) or (Path(a.out_root) / day).exists(), day)
        if d["action"] != "late":
            print(f"[input-freeze] not late ({d['action']}) — nothing written")
            return 0
        m = late_manifest(d, json.loads(a.meta))
        print(json.dumps(m, indent=1))
        if not a.dry_run:
            write_day(Path(a.out_root), day, m, {})
        return 0

    if a.cmd == "freeze":
        freeze = parse_now(a.now)
        day = a.date or freeze.astimezone(ET).date().isoformat()
        if a.at_target:
            freeze = et_at(day, TARGET_ET)
        if not a.dry_run and freeze >= et_at(day, OPEN_ET):
            raise SystemExit("[input-freeze] REFUSE: freeze time is not before 09:30 ET")
        repos = {"fullscan": Path(a.fullscan),
                 "theme-radar": Path(a.theme_radar) if a.theme_radar else None}
        manifest, blobs = build(day, freeze, repos,
                                {"fullscan": a.fullscan_ref, "theme-radar": a.theme_radar_ref})
        manifest.update(json.loads(a.meta))
        if a.dry_run:
            manifest["dry_run"] = True
        summary = {k: manifest[k] for k in ("date", "freeze_time_utc", "categories", "totals", "sources")}
        print(json.dumps(summary, indent=1))
        for row in manifest["files"]:
            print(f"  {row['status']:<12} {row['category']:<18} {row.get('source_path', row['pattern'])}"
                  f"  {row.get('bytes', '')} -> {row.get('stored_bytes', '')}  {row.get('why', '')}")
        if a.dry_run:
            print("[input-freeze] DRY RUN — nothing written")
            return 0
        folder = write_day(Path(a.out_root), day, manifest, blobs)
        print(f"[input-freeze] wrote {folder}")
        return 0

    if a.cmd == "check":
        repo = Path(a.repo)
        bad = changed_day_files(repo, a.base, a.head)
        if a.only_day:
            bad += [f"outside {a.only_day}: {p}" for p in only_new_day(repo, a.base, a.head, a.only_day)]
        if bad:
            print("[input-freeze] IRONCLAD FAIL — a written day folder changed (append-only):")
            for b in bad:
                print("  " + b)
            return 1
        print("[input-freeze] append-only OK — no existing day folder changed")
        return 0
    return 2


if __name__ == "__main__":
    sys.exit(main())
