"""Read-only provenance for the #336 sequential Factor Mine rebuild.

The sequential walk scores ``data/factor_mine/snapshots/<date>.json`` and
fills from ``data/factor_mine/retro_prices/ohlc.parquet``. The snapshot
rows were built by ``factor_mine_retro.materialize``, which picks each
packet with ``git log --pretty=%cI`` (the committer clock). This script
does not treat that clock as proof. A blob counts as pre-open only when
GitHub recorded it before 09:30 America/New_York that session:

* the earliest Actions run whose ``head_sha`` is the commit and whose
  event is ``push`` (the committing push), or
* a PushEvent ``created_at`` from the repo events API when that feed
  still has the push, or
* an earlier Actions run (pull request or any later run on that head)
  whose start is still at or before 09:30 — that only proves the commit
  was already on GitHub, it is not a push time.

A run that starts after 09:30 does not prove the commit was late, except
a push-event run, which is the push itself.

Nothing here writes a ledger, a snapshot, ``panel.json``, or a state file.
The HOT4 / holdup replay uses ``factor_mine_sequential.walk(..., persist=False)``
and only the days this audit marks proven-frozen.
"""
from __future__ import annotations

import csv
import io
import json
import subprocess
import sys
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[2]
OUT_MD = Path(__file__).resolve().parent / "INPUT_PROVENANCE_336.md"
MERGE = "5f13a4415ea0cfe460155afb3b7677070a2cd188"
ET = ZoneInfo("America/New_York")
REPO = "SRoyaltyy/fullscan"
CACHE = Path("/tmp/fm_audit_336")

PANEL_ERA = "2026-09-09"
# AI text is never regenerated. An early file counts only when a pre-open
# copy exists and its shape matches a proven copy from the panel era.
AI_ROLES = {
    "digest", "map_heat", "catalyst", "baseline", "predict",
    "actions", "judge", "events", "research",
}
# Deterministic outputs. Rebuildable for days before the panel era only
# when today's code matches the pre-open copy on every later proven day.
REBUILD_KEYS = (
    "ab_a1_a15", "price_features", "ohlc_hot", "yday_gainer",
    "yday_mover", "overnight", "probable", "earn_react",
)
A_FLAGS = (
    "A01_rsi_value", "A02_rsi_cross_30", "A03_rsi_cross_50",
    "A04_rsi_cross_70", "A05_body_red_green_2day",
    "A06_volume_red_green_2day", "A07_rvol", "A08_bollinger_position",
    "A09_above_sma50", "A10_sma20_50_80_stack", "A11_three_section_lows",
    "A12_green_body_vs_wick_2day", "A13_red_body_vs_wick_2day",
    "A15_tape_recovery_setup",
)
PRICE_FIELDS = (
    "ohlc_ret_1", "ohlc_ret_5", "ohlc_ret_10", "ohlc_rvol", "ohlc_hot_score",
)

SESSIONS = (
    "2026-08-13", "2026-08-14", "2026-08-17", "2026-08-18", "2026-08-19",
    "2026-08-20", "2026-08-21", "2026-08-24", "2026-08-25", "2026-08-26",
    "2026-08-27", "2026-08-28", "2026-08-31", "2026-09-01", "2026-09-02",
    "2026-09-03", "2026-09-04", "2026-09-08", "2026-09-09", "2026-09-10",
    "2026-09-11", "2026-09-14", "2026-09-15", "2026-09-16", "2026-09-17",
    "2026-09-18", "2026-09-21", "2026-09-22", "2026-09-23", "2026-09-24",
    "2026-09-25",
)

# What factor_mine_retro.NAMED_INPUTS records on each snapshot.
NAMED = {
    "digest": "01_daily/news/{d}_finviz_digest.json",
    "map_heat": "01_daily/map_heat/{d}_map_heat.json",
    "export": "data/exports/finviz_{d}.csv",
    "join": "data/join/{d}_ranked.csv",
    "catalyst": "01_daily/catalyst/{d}_dossiers.json",
    "baseline": "01_daily/map_heat/{d}_research_baseline.json",
    "weather": "01_daily/weather/{d}_weather.json",
    "predict": "01_daily/general/{d}_predict.md",
    "actions": "01_daily/news/{d}_actions.json",
    "judge": "01_daily/news/{d}_judge.json",
    "events": "01_daily/events/{d}_events.json",
    "research": "01_daily/map_heat/{d}_research.json",
}

GROUP = {
    "digest": "news",
    "actions": "news",
    "judge": "news",
    "predict": "predict",
    "join": "join",
    "export": "packet",
    "map_heat": "packet",
    "catalyst": "packet",
    "baseline": "packet",
    "weather": "packet",
    "events": "packet",
    "research": "packet",
}

FINGERPRINT_DIRS = (
    ROOT / "data" / "factor_mine" / "ledgers",
    ROOT / "data" / "factor_mine" / "state",
    ROOT / "data" / "factor_mine" / "snapshots",
    ROOT / "data" / "factor_mine" / "prices",
)
FINGERPRINT_FILES = (
    ROOT / "data" / "factor_mine" / "panel.json",
    ROOT / "data" / "factor_mine" / "freeze_manifest.json",
    ROOT / "data" / "factor_mine" / "retro_prices" / "ohlc.parquet",
)


def run(args: list[str], check: bool = True) -> subprocess.CompletedProcess:
    proc = subprocess.run(
        args, cwd=ROOT, capture_output=True, text=True, check=False,
    )
    if check and proc.returncode != 0:
        raise RuntimeError(
            f"{' '.join(args[:6])} failed {proc.returncode}: {proc.stderr[-400:]}"
        )
    return proc


def parse_time(stamp: str) -> datetime | None:
    stamp = (stamp or "").strip()
    if not stamp:
        return None
    if stamp.endswith("Z"):
        stamp = stamp[:-1] + "+00:00"
    try:
        ts = datetime.fromisoformat(stamp)
    except ValueError:
        return None
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc)


def cutoff_for(day: str) -> datetime:
    local = datetime.strptime(day, "%Y-%m-%d").replace(
        hour=9, minute=30, tzinfo=ET,
    )
    return local.astimezone(timezone.utc)


def git_bytes(sha: str, path: str) -> bytes | None:
    proc = subprocess.run(
        ["git", "show", f"{sha}:{path}"],
        cwd=ROOT, capture_output=True, check=False,
    )
    if proc.returncode != 0:
        return None
    return proc.stdout


def ls_files(prefix: str) -> list[str]:
    proc = run(["git", "ls-files", prefix])
    return [line for line in proc.stdout.splitlines() if line]


def fingerprint() -> dict[str, tuple[int, int]]:
    out: dict[str, tuple[int, int]] = {}
    for folder in FINGERPRINT_DIRS:
        if not folder.is_dir():
            continue
        for path in sorted(folder.rglob("*")):
            if not path.is_file():
                continue
            st = path.stat()
            out[str(path.relative_to(ROOT))] = (st.st_size, st.st_mtime_ns)
    for path in FINGERPRINT_FILES:
        if path.is_file():
            st = path.stat()
            out[str(path.relative_to(ROOT))] = (st.st_size, st.st_mtime_ns)
    return out


def load_history(paths: list[str]) -> dict[str, list[tuple[datetime, str]]]:
    """Committer-clock history. This is what #336 selected. It is not proof."""
    history: dict[str, list[tuple[datetime, str]]] = {p: [] for p in paths}
    if not paths:
        return history
    proc = run([
        "git", "log", "--name-only", "--pretty=format:%H %cI", "--", *paths,
    ])
    seen: dict[str, set[str]] = defaultdict(set)
    current_sha = ""
    current_time: datetime | None = None
    for line in proc.stdout.splitlines():
        if not line.strip():
            continue
        maybe, _, rest = line.partition(" ")
        if len(maybe) == 40 and parse_time(rest) and "/" not in maybe:
            current_sha = maybe
            current_time = parse_time(rest)
            continue
        path = line.strip()
        if path not in history or current_time is None or current_sha in seen[path]:
            continue
        seen[path].add(current_sha)
        history[path].append((current_time, current_sha))
    for path in history:
        history[path].sort(key=lambda item: item[0])
    return history


def code_sha(rows: list[tuple[datetime, str]], cutoff: datetime) -> str | None:
    """Last commit whose committer timestamp is at or before the open."""
    chosen = None
    for ts, sha in rows:
        if ts <= cutoff:
            chosen = sha
        else:
            break
    return chosen


def gh_tsv(url: str) -> str:
    proc = run(["gh", "api", "--paginate", url, "--jq",
                ".workflow_runs[] | [.head_sha,.event,(.run_started_at // .created_at),.created_at,(.id|tostring),.html_url] | @tsv"],
               check=False)
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr[-500:])
    return proc.stdout


def fetch_actions() -> dict[str, list[dict]]:
    CACHE.mkdir(parents=True, exist_ok=True)
    cache = CACHE / "actions.tsv"
    if not cache.is_file():
        days = []
        start = datetime(2026, 8, 1)
        end = datetime(2026, 9, 26)
        cur = start
        while cur <= end:
            days.append(cur.strftime("%Y-%m-%d"))
            cur = cur.fromordinal(cur.toordinal() + 1)
        chunks = []

        def one(day: str) -> str:
            url = (
                f"repos/{REPO}/actions/runs?per_page=100"
                f"&created={day}..{day}"
            )
            return gh_tsv(url)

        with ThreadPoolExecutor(max_workers=6) as pool:
            futs = {pool.submit(one, day): day for day in days}
            for fut in as_completed(futs):
                day = futs[fut]
                text = fut.result()
                chunks.append(text)
                print(f"[audit] actions {day} lines={text.count(chr(10))}", flush=True)
        cache.write_text("".join(chunks), encoding="utf-8")
    found: dict[str, list[dict]] = defaultdict(list)
    for line in cache.read_text(encoding="utf-8").splitlines():
        parts = line.split("\t")
        if len(parts) < 6:
            continue
        sha, event, started, _created, run_id, url = parts[:6]
        ts = parse_time(started)
        if not sha or ts is None:
            continue
        found[sha].append({
            "event": event,
            "started": ts,
            "run_id": run_id,
            "url": url,
        })
    return found


def fetch_events() -> tuple[dict[str, datetime], dict]:
    """Repo events API. GitHub only keeps a short recent window."""
    CACHE.mkdir(parents=True, exist_ok=True)
    cache = CACHE / "events.json"
    if cache.is_file():
        payload = json.loads(cache.read_text(encoding="utf-8"))
    else:
        pages = []
        oldest = None
        newest = None
        for page in range(1, 11):
            proc = run([
                "gh", "api",
                f"repos/{REPO}/events?per_page=100&page={page}",
            ], check=False)
            if proc.returncode != 0:
                break
            try:
                batch = json.loads(proc.stdout or "[]")
            except json.JSONDecodeError:
                break
            if not batch:
                break
            pages.extend(batch)
            for ev in batch:
                ts = ev.get("created_at")
                if ts and (oldest is None or ts < oldest):
                    oldest = ts
                if ts and (newest is None or ts > newest):
                    newest = ts
            if len(batch) < 100:
                break
        payload = {"events": pages, "oldest": oldest, "newest": newest, "n": len(pages)}
        cache.write_text(json.dumps({
            "oldest": oldest, "newest": newest, "n": len(pages),
            "pushes": [
                {
                    "created_at": ev.get("created_at"),
                    "head": (ev.get("payload") or {}).get("head"),
                    "shas": [
                        c.get("sha") for c in ((ev.get("payload") or {}).get("commits") or [])
                        if c.get("sha")
                    ],
                }
                for ev in pages if ev.get("type") == "PushEvent"
            ],
        }), encoding="utf-8")
        payload = json.loads(cache.read_text(encoding="utf-8"))
    times: dict[str, datetime] = {}
    for push in payload.get("pushes") or []:
        ts = parse_time(push.get("created_at") or "")
        if ts is None:
            continue
        shas = list(push.get("shas") or [])
        head = push.get("head")
        if head:
            shas.append(head)
        for sha in shas:
            prev = times.get(sha)
            if prev is None or ts < prev:
                times[sha] = ts
    meta = {
        "n": payload.get("n"),
        "oldest": payload.get("oldest"),
        "newest": payload.get("newest"),
        "push_shas": len(times),
    }
    return times, meta


def _lookup_cache_path() -> Path:
    return CACHE / "lookup.json"


def _load_lookup_cache(have: dict[str, list[dict]]) -> set[str]:
    path = _lookup_cache_path()
    if not path.is_file():
        return set()
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return set()
    seen = set()
    for sha, rows in (payload.get("shas") or {}).items():
        seen.add(sha)
        if sha in have:
            continue
        parsed = []
        for row in rows or []:
            ts = parse_time(row.get("started") or "")
            if ts is None:
                continue
            parsed.append({
                "event": row.get("event") or "",
                "started": ts,
                "run_id": row.get("run_id") or "",
                "url": row.get("url") or "",
            })
        if parsed:
            have[sha] = parsed
    return seen


def _save_lookup_cache(found: dict[str, list[dict]], seen: set[str]) -> None:
    CACHE.mkdir(parents=True, exist_ok=True)
    shas = {}
    for sha in seen:
        rows = found.get(sha) or []
        shas[sha] = [
            {
                "event": row.get("event") or "",
                "started": iso(row.get("started")),
                "run_id": row.get("run_id") or "",
                "url": row.get("url") or "",
            }
            for row in rows
        ]
    _lookup_cache_path().write_text(
        json.dumps({"shas": shas}), encoding="utf-8",
    )


def lookup_missing(shas: list[str], have: dict[str, list[dict]]) -> None:
    seen = _load_lookup_cache(have)
    missing = [s for s in shas if s and s not in have and s not in seen]
    if not missing:
        return

    def one(sha: str) -> tuple[str, list[dict]]:
        url = f"repos/{REPO}/actions/runs?per_page=20&head_sha={sha}"
        proc = run(["gh", "api", url], check=False)
        if proc.returncode != 0:
            return sha, []
        try:
            doc = json.loads(proc.stdout or "{}")
        except json.JSONDecodeError:
            return sha, []
        rows = []
        for item in doc.get("workflow_runs") or []:
            ts = parse_time(item.get("run_started_at") or item.get("created_at") or "")
            if ts is None:
                continue
            rows.append({
                "event": item.get("event") or "",
                "started": ts,
                "run_id": str(item.get("id") or ""),
                "url": item.get("html_url") or "",
            })
        return sha, rows

    print(f"[audit] lookup {len(missing)} shas with no cached run", flush=True)
    done = 0
    with ThreadPoolExecutor(max_workers=8) as pool:
        futs = [pool.submit(one, sha) for sha in missing]
        for fut in as_completed(futs):
            sha, rows = fut.result()
            if rows:
                have[sha] = rows
            seen.add(sha)
            done += 1
            if done % 100 == 0 or done == len(missing):
                print(f"[audit] lookup {done}/{len(missing)}", flush=True)
                _save_lookup_cache(have, seen)
    _save_lookup_cache(have, seen)


def prove(sha: str | None, runs: dict[str, list[dict]],
          events: dict[str, datetime]) -> dict:
    if not sha:
        return {"sha": None, "kind": "absent", "time": None, "status": "absent"}
    rows = list(runs.get(sha) or [])
    push = [r for r in rows if r["event"] == "push"]
    prs = [r for r in rows if r["event"] == "pull_request"]
    event_time = events.get(sha)
    if push:
        row = min(push, key=lambda item: item["started"])
        return {
            "sha": sha, "kind": "actions_push", "time": row["started"],
            "run_id": row["run_id"], "url": row["url"], "event": "push",
        }
    if event_time is not None:
        return {
            "sha": sha, "kind": "events_push", "time": event_time,
            "run_id": "", "url": "", "event": "PushEvent",
        }
    if prs:
        row = min(prs, key=lambda item: item["started"])
        return {
            "sha": sha, "kind": "actions_pull_request", "time": row["started"],
            "run_id": row["run_id"], "url": row["url"], "event": "pull_request",
        }
    if rows:
        row = min(rows, key=lambda item: item["started"])
        return {
            "sha": sha, "kind": "actions_head", "time": row["started"],
            "run_id": row["run_id"], "url": row["url"], "event": row["event"],
        }
    return {"sha": sha, "kind": "none", "time": None, "status": "not_proven"}


def before_open(proof: dict, cutoff: datetime) -> str:
    """proven_before, pushed_after, not_proven, or absent."""
    if proof.get("kind") == "absent" or not proof.get("sha"):
        return "absent"
    ts = proof.get("time")
    kind = proof.get("kind")
    if ts is None:
        return "not_proven"
    if ts <= cutoff:
        return "proven_before"
    if kind in ("actions_push", "events_push"):
        return "pushed_after"
    return "not_proven"


def latest_proven(rows: list[tuple[datetime, str]], proofs: dict[str, dict],
                  cutoff: datetime) -> str | None:
    best_sha = None
    best_time: datetime | None = None
    for _ts, sha in rows:
        proof = proofs[sha]
        if before_open(proof, cutoff) != "proven_before":
            continue
        ts = proof["time"]
        if best_time is None or ts >= best_time:
            best_time = ts
            best_sha = sha
    return best_sha


def short(sha: str | None) -> str:
    if not sha:
        return "—"
    return sha[:12]


def iso(ts: datetime | None) -> str:
    if ts is None:
        return "—"
    return ts.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%SZ")


def load_snapshot(day: str, history) -> tuple[dict, str | None]:
    """Rows #336 published, or the first commit of a snapshot the merge lacks."""
    path = f"data/factor_mine/snapshots/{day}.json"
    raw = git_bytes(MERGE, path)
    if raw is not None:
        return json.loads(raw), MERGE
    rows = history.get(path) or []
    if not rows:
        return {}, None
    sha = rows[0][1]
    raw = git_bytes(sha, path)
    if raw is None:
        return {}, sha
    return json.loads(raw), sha


def as_rows(blob: bytes | None, kind: str) -> list[dict] | None:
    if blob is None:
        return None
    if kind == "json-rows":
        try:
            doc = json.loads(blob)
        except json.JSONDecodeError:
            return None
        rows = doc.get("rows") if isinstance(doc, dict) else None
        if not isinstance(rows, list):
            return None
        return [r for r in rows if isinstance(r, dict)]
    if kind == "csv":
        text = blob.decode("utf-8", errors="replace")
        reader = csv.DictReader(io.StringIO(text))
        return [dict(rec) for rec in reader]
    return None


def row_key(rec: dict, fields: tuple[str, ...]) -> tuple:
    for field in fields:
        val = rec.get(field)
        if val is None:
            for k, v in rec.items():
                if k.lower() == field.lower():
                    val = v
                    break
        if val not in (None, ""):
            return (field, str(val).strip().upper())
    return ("?", json.dumps(rec, sort_keys=True)[:80])


def norm(value):
    if isinstance(value, dict):
        return json.dumps(value, sort_keys=True, default=str)
    if isinstance(value, list):
        return json.dumps(value, sort_keys=True, default=str)
    if isinstance(value, float):
        return round(value, 6)
    if isinstance(value, str):
        try:
            num = float(value)
        except ValueError:
            return value
        if value.strip() and any(c in value for c in ".eE"):
            return round(num, 6)
    return value


def diff_records(used: list[dict], proven: list[dict],
                 key_fields: tuple[str, ...]) -> dict:
    umap: dict[tuple, dict] = {}
    pmap: dict[tuple, dict] = {}
    for rec in used:
        umap[row_key(rec, key_fields)] = rec
    for rec in proven:
        pmap[row_key(rec, key_fields)] = rec
    added = sorted(k for k in umap if k not in pmap)
    removed = sorted(k for k in pmap if k not in umap)
    changed = []
    fields: set[str] = set()
    same = 0
    for key in umap.keys() & pmap.keys():
        a, b = umap[key], pmap[key]
        names = set(a) | set(b)
        diff_fields = []
        for name in names:
            if norm(a.get(name)) != norm(b.get(name)):
                diff_fields.append(name)
        if diff_fields:
            changed.append(key)
            fields.update(diff_fields)
        else:
            same += 1
    status = "same"
    if added or removed or changed:
        status = "changed" if (same or changed) and (added or removed or changed) else "changed"
        if not same and not changed and added and not removed:
            status = "added"
        elif not same and not changed and removed and not added:
            status = "removed"
        elif added or removed or changed:
            status = "changed"
    if not used and not proven:
        status = "same"
    return {
        "status": status,
        "same": same,
        "added": len(added),
        "removed": len(removed),
        "changed": len(changed),
        "fields": sorted(fields),
        "added_ids": [k[-1] for k in added[:12]],
        "removed_ids": [k[-1] for k in removed[:12]],
    }


def panel_rows_for(blob: bytes | None, day: str) -> list[dict]:
    rows = as_rows(blob, "json-rows") or []
    return [r for r in rows if str(r.get("date") or "")[:10] == day]


def ab_paths(day: str, tracked: set[str]) -> list[str]:
    names = (
        f"data/ab_checklist/{day}_ab_slim.csv",
        f"data/ab_checklist/{day}_ab_checklist_enriched.csv",
        f"data/ab_checklist/{day}_ab_checklist.csv",
    )
    return [name for name in names if name in tracked]


def sector_paths(day: str, tracked: list[str]) -> list[str]:
    prefix = f"01_daily/sectors/{day}/"
    out = []
    for path in tracked:
        if not path.startswith(prefix):
            continue
        name = path.rsplit("/", 1)[-1]
        if "trace" in name:
            continue
        out.append(path)
    return out


def build_inputs(day: str, snap: dict, snap_commit: str | None, history,
                 tracked_ab: set[str], sector_files: list[str]) -> list[dict]:
    cutoff = cutoff_for(day)
    sources = snap.get("sources") or {}
    items = []

    def add(group: str, role: str, path: str, used: str | None, how: str) -> None:
        items.append({
            "group": group, "role": role, "path": path,
            "used": used, "how": how, "day": day,
        })

    snap_path = f"data/factor_mine/snapshots/{day}.json"
    how = "blob in the #336 merge" if snap_commit == MERGE else "first commit of this snapshot; not in the #336 merge"
    add("snapshot", "sequential rows_for_day", snap_path, snap_commit, how)

    add("panel", "not opened by the walk; rows compared below",
        "data/factor_mine/panel.json",
        code_sha(history.get("data/factor_mine/panel.json") or [], cutoff),
        "commit date, file not read")

    for key, tmpl in NAMED.items():
        path = tmpl.format(d=day)
        recorded = sources.get(path)
        if path in sources:
            used = recorded or None
            how = "snapshot.sources (commit-date pick)"
        else:
            used = code_sha(history.get(path) or [], cutoff)
            how = "commit-date pick; not on this snapshot"
        add(GROUP.get(key, "packet"), f"named input {key}", path, used, how)

    ab = ab_paths(day, tracked_ab)
    if ab:
        # ticker_lookback.build_index opens the first of these that exists.
        chosen = None
        for path in ab:
            sha = code_sha(history.get(path) or [], cutoff)
            if sha:
                chosen = (path, sha)
                break
        if chosen:
            add("ab", "build_index opens this file", chosen[0], chosen[1], "commit-date pick")
        else:
            add("ab", "build_index opens this file", ab[0], None, "no commit-date blob")
    else:
        add("ab", "build_index opens this file",
            f"data/ab_checklist/{day}_ab_checklist.csv", None, "path never committed")

    sectors = sector_paths(day, sector_files)
    if not sectors:
        add("sector", "materialize copies the day folder; build_panel does not open it",
            f"01_daily/sectors/{day}/", None, "no tracked file")
    for path in sectors:
        add("sector", "copied by materialize when the committer clock is before the open",
            path, code_sha(history.get(path) or [], cutoff), "commit-date pick")

    sug = "excel_bot/suggestions/suggestions.csv"
    add("excel", "pin_excel_signals records this sha; build_panel does not open it",
        sug, code_sha(history.get(sug) or [], cutoff), "commit-date pick")
    daily = f"excel_bot/daily/{day}_excel_bot.md"
    add("excel", "pin_excel_signals daily note",
        daily, code_sha(history.get(daily) or [], cutoff), "commit-date pick")

    pin = f"data/factor_mine/prices/{day}.json"
    add("price", "live price pin; sequential walk does not open it",
        pin, code_sha(history.get(pin) or [], cutoff), "commit-date pick")
    return items


def annotate(items: list[dict], history, proofs, cutoff: datetime) -> None:
    for item in items:
        path = item["path"]
        rows = history.get(path) or []
        item["proven"] = latest_proven(rows, proofs, cutoff)
        used = item.get("used")
        item["used_proof"] = proofs.get(used) if used else {"kind": "absent", "time": None, "sha": None}
        item["used_status"] = before_open(item["used_proof"], cutoff) if used else "absent"
        proven = item.get("proven")
        if used == proven:
            item["match"] = "same" if used else "both absent"
        elif used and not proven:
            item["match"] = "no pre-open copy"
        elif proven and not used:
            item["match"] = "pre-open copy was not the blob the code selected"
        else:
            item["match"] = "different blob"


def diff_item(item: dict, day: str) -> dict | None:
    if item["match"] == "same" or item["match"] == "both absent":
        return {"status": "same", "same": None, "added": 0, "removed": 0,
                "changed": 0, "fields": []}
    used_sha = item.get("used")
    proven_sha = item.get("proven")
    path = item["path"]
    if path.endswith("/"):
        return None
    kind = None
    keys = ("ticker", "Ticker")
    if path.endswith(".csv"):
        kind = "csv"
    elif path == "data/factor_mine/panel.json" or path.endswith(f"snapshots/{day}.json"):
        kind = "json-rows"
        keys = ("ticker",)
    elif path.endswith("_board.json") or path.endswith("_finviz_digest.json"):
        return file_level(item)
    else:
        return file_level(item)
    used_blob = git_bytes(used_sha, path) if used_sha else None
    proven_blob = git_bytes(proven_sha, path) if proven_sha else None
    if kind == "json-rows" and path.endswith("panel.json"):
        used_rows = panel_rows_for(used_blob, day)
        proven_rows = panel_rows_for(proven_blob, day)
    elif kind == "json-rows":
        used_rows = as_rows(used_blob, "json-rows") or []
        proven_rows = as_rows(proven_blob, "json-rows") or []
        if path.endswith(f"snapshots/{day}.json"):
            used_rows = [r for r in used_rows if str(r.get("date") or day)[:10] == day]
            proven_rows = [r for r in proven_rows if str(r.get("date") or day)[:10] == day]
    else:
        used_rows = as_rows(used_blob, "csv") or []
        proven_rows = as_rows(proven_blob, "csv") or []
        if "suggestions.csv" in path:
            keys = ("signal_date", "ticker", "strategy", "side")
    return diff_records(used_rows, proven_rows, keys)


def file_level(item: dict) -> dict:
    used_sha = item.get("used")
    proven_sha = item.get("proven")
    path = item["path"]
    ub = git_bytes(used_sha, path) if used_sha else None
    pb = git_bytes(proven_sha, path) if proven_sha else None
    if ub == pb:
        return {"status": "same", "same": 1, "added": 0, "removed": 0,
                "changed": 0, "fields": []}
    if ub and not pb:
        return {"status": "added", "same": 0, "added": 1, "removed": 0,
                "changed": 0, "fields": ["file"]}
    if pb and not ub:
        return {"status": "removed", "same": 0, "added": 0, "removed": 1,
                "changed": 0, "fields": ["file"]}
    return {"status": "changed", "same": 0, "added": 0, "removed": 0,
            "changed": 1, "fields": ["file"]}


def candidate_diff(day: str, snap: dict, panel_proven_sha: str | None) -> dict:
    used_rows = [r for r in (snap.get("rows") or []) if isinstance(r, dict)]
    blob = git_bytes(panel_proven_sha, "data/factor_mine/panel.json") if panel_proven_sha else None
    proven_rows = panel_rows_for(blob, day)
    return diff_records(used_rows, proven_rows, ("ticker",))


def day_frozen(items: list[dict], rows: dict, parquet_status: str) -> tuple[bool, list[str]]:
    """A day is proven-frozen when every blob the rebuild selected is the
    latest blob GitHub had before 09:30, and the candidate rows match the
    pre-open panel rows for that date.

    The fill tape is one shared parquet. If that file was pushed after the
    open, the session is not frozen.
    """
    reasons = []
    if parquet_status != "proven_before":
        reasons.append(f"fill tape {parquet_status}")
    for item in items:
        if item["group"] == "price" and item["used_status"] == "absent" and item["match"] == "both absent":
            continue
        if item["group"] == "panel":
            # The walk does not open panel.json. The candidate-row diff covers it.
            continue
        if item["match"] not in ("same", "both absent"):
            reasons.append(f"{item['group']} {item['path']} {item['match']}")
        elif item.get("used") and item["used_status"] != "proven_before":
            reasons.append(f"{item['group']} {item['path']} {item['used_status']}")
    if rows.get("status") != "same":
        reasons.append(
            f"candidate rows {rows.get('status')} "
            f"added={rows.get('added')} removed={rows.get('removed')} "
            f"changed={rows.get('changed')}"
        )
    return (not reasons), reasons


def recompute(dates: list[str], *, drop_glnd: bool) -> dict:
    """Score HOT4 and holdup on ``dates`` only. persist=False writes nothing."""
    if not dates:
        return {"days": 0, "drop_glnd": drop_glnd, "recipes": {}}
    sys.path.insert(0, str(ROOT))
    from src import factor_mine_sequential as seq
    from src.factor_mine_book import load_regime
    from src.factor_mine_retro import retro_recipes
    from src import factor_mine as fm

    want = {"union_hot_n4_h1", "union_hot_n4_holdup"}
    recipes = [r for r in retro_recipes() if r.get("name") in want]
    store = seq.load_bar_store(dates)
    regime = load_regime()
    fees = fm.pt_fees()
    history: dict[str, list] = {}

    def rows_for(date: str):
        return seq.rows_for_day(date)

    def bars_for(date: str):
        return {k: v for k, v in store.items() if str(k[1])[:10] == date}

    seq.walk(
        dates, recipes, rows_for=rows_for, bars_for=bars_for,
        persist=False, fees=fees, regime=regime,
        exclude=("GLND" if drop_glnd else None),
        history=history,
    )
    out = {"days": len(dates), "drop_glnd": drop_glnd, "recipes": {}}
    for name, records in history.items():
        out["recipes"][name] = {
            "n": len(records),
            "compound_pct": seq.compound(records),
            "equity": None if not records else records[-1].get("equity"),
            "days": [
                {
                    "date": rec.get("date"),
                    "mean": rec.get("mean"),
                    "equity": rec.get("equity"),
                    "buys": [seq.order_ticker(x) for x in (rec.get("buys") or [])],
                    "sells": [seq.order_ticker(x) for x in (rec.get("sells") or [])],
                }
                for rec in records
            ],
        }
    return out


def named_item(items: list[dict], key: str) -> dict | None:
    role = f"named input {key}"
    for item in items:
        if item.get("role") == role:
            return item
    return None


def blob_shape(path: str, blob: bytes) -> tuple:
    """Coarse format: kind plus keys, columns, or markdown headings."""
    if path.endswith(".json"):
        try:
            doc = json.loads(blob)
        except json.JSONDecodeError:
            return ("json-bad",)
        if isinstance(doc, dict):
            return ("json-object", tuple(sorted(str(k) for k in doc)))
        if isinstance(doc, list):
            if doc and isinstance(doc[0], dict):
                return ("json-list", tuple(sorted(str(k) for k in doc[0])))
            return ("json-list",)
        return ("json-other", type(doc).__name__)
    if path.endswith(".csv"):
        line = blob.splitlines()[0] if blob else b""
        cols = tuple(sorted(c.strip() for c in line.decode("utf-8", "replace").split(",")))
        return ("csv", cols)
    if path.endswith(".md"):
        text = blob.decode("utf-8", "replace")
        heads = tuple(
            line.strip() for line in text.splitlines()
            if line.startswith("#") and len(line) < 100
        )[:8]
        return ("md", heads)
    return ("other", path.rsplit(".", 1)[-1])


def sector_role(path: str) -> str:
    name = path.rsplit("/", 1)[-1]
    if name.startswith("_board"):
        return "board-json"
    if name.startswith("_BOARD"):
        return "board-md"
    if name.startswith("_qc"):
        return "qc"
    if name.endswith("_predict.md"):
        return "predict"
    if name.endswith("_outcome.md"):
        return "outcome"
    if name.endswith("_reflect.md"):
        return "reflect"
    return name


def close_num(saved, fresh) -> bool:
    try:
        a = float(saved)
        b = float(fresh)
    except (TypeError, ValueError):
        return saved == fresh
    if a != a and b != b:
        return True
    scale = max(abs(a), abs(b), 1.0)
    return abs(a - b) <= 1e-3 * scale or abs(a - b) <= 1e-4


def _ab_module():
    sys.path.insert(0, str(ROOT))
    from src import ab_checklist as ab
    return ab


def compare_ab_rules(days: dict, history, proofs) -> dict:
    """Part A flags from today's loader versus each pre-open AB file.

    Bars dated the session itself are left out. At 09:30 that bar does
    not exist yet. The live ``run()`` filter is ``date <= asof``, which
    only matches the morning file when the store has not yet grown the
    session bar.
    """
    def flag_file(day: str):
        cutoff = cutoff_for(day)
        for suffix in (
            "_ab_slim.csv",
            "_ab_checklist_enriched.csv",
            "_ab_checklist.csv",
        ):
            path = f"data/ab_checklist/{day}{suffix}"
            sha = latest_proven(history.get(path) or [], proofs, cutoff)
            if not sha:
                continue
            blob = git_bytes(sha, path)
            header = blob.splitlines()[0] if blob else b""
            if b"flag_A01_rsi_value" not in header:
                continue
            return sha, path
        return None

    flag_days = [day for day in SESSIONS if flag_file(day)]
    tested = []
    for day in flag_days:
        if day < PANEL_ERA:
            continue
        sha, path = flag_file(day)
        tested.append((day, sha, path))
    out = {
        "key": "ab_a1_a15",
        "kind": "price rules A1-A15",
        "tested": len(tested),
        "matched": 0,
        "days": [],
        "rebuildable": False,
        "note": "Compared with the server-proven pre-open AB file that contains A flags. Part B is not rebuilt.",
        "flag_days": flag_days,
    }
    if not tested:
        out["note"] = "No 09-09+ day has a server-proven pre-open AB file."
        return out
    import pandas as pd
    ab = _ab_module()
    store = pd.read_parquet(ROOT / "data" / "prices" / "ohlc.parquet")
    store["date"] = pd.to_datetime(store["date"])
    groups = {
        t: g.drop(columns=["ticker"]).set_index("date").sort_index()
        for t, g in store.groupby("ticker")
    }
    matched_days = 0
    for day, sha, path in tested:
        blob = git_bytes(sha, path)
        if not blob:
            out["days"].append({"day": day, "match": False, "reason": "blob missing"})
            continue
        frame = pd.read_csv(io.BytesIO(blob))
        flag_cols = [f"flag_{name}" for name in A_FLAGS if f"flag_{name}" in frame.columns]
        if "Ticker" not in frame.columns or not flag_cols:
            out["days"].append({
                "day": day, "match": False, "rows": len(frame),
                "reason": "no Ticker or A flags",
            })
            continue
        asof = pd.Timestamp(day)
        same = 0
        bad_flags: dict[str, int] = defaultdict(int)
        last_bars: dict[str, int] = defaultdict(int)
        for rec in frame.itertuples(index=False):
            ticker = str(getattr(rec, "Ticker") or "").strip().upper()
            bars = groups.get(ticker)
            fresh = {name: 0 for name in A_FLAGS}
            if bars is not None:
                window = bars[bars.index < asof]
                part = ab._part_a(window)
                passed = ab._pass_a(part)
                fresh.update(passed)
                if part.get("asof_session"):
                    last_bars[str(part["asof_session"])] += 1
            row_ok = True
            for col in flag_cols:
                name = col[len("flag_"):]
                try:
                    saved = int(float(getattr(rec, col)))
                except (TypeError, ValueError):
                    saved = None
                if saved != int(fresh.get(name, 0)):
                    row_ok = False
                    bad_flags[name] += 1
            if row_ok:
                same += 1
        saved_end = ""
        if "pair_day_b" in frame.columns:
            mode = frame["pair_day_b"].astype(str).mode()
            saved_end = str(mode.iloc[0]) if len(mode) else ""
        day_match = same == len(frame) and len(frame) > 0
        if day_match:
            matched_days += 1
        top = sorted(bad_flags.items(), key=lambda kv: -kv[1])[:6]
        rebuilt_end = ""
        if last_bars:
            rebuilt_end = max(last_bars.items(), key=lambda kv: kv[1])[0]
        out["days"].append({
            "day": day,
            "path": path,
            "sha": sha,
            "rows": len(frame),
            "same": same,
            "match": day_match,
            "flags": flag_cols,
            "mismatch_flags": top,
            "saved_last_bar": saved_end,
            "rebuilt_last_bar": rebuilt_end,
        })
        print(f"[audit] ab {day} same={same}/{len(frame)} match={day_match}", flush=True)
    out["matched"] = matched_days
    out["rebuildable"] = bool(tested) and matched_days == len(tested)
    return out


def _snapshot_sources(snap: dict) -> dict[str, list[str]]:
    buckets: dict[str, list[str]] = defaultdict(list)
    for row in snap.get("rows") or []:
        if not isinstance(row, dict):
            continue
        ticker = str(row.get("ticker") or "").strip().upper()
        if not ticker:
            continue
        for src in row.get("sources") or []:
            if ticker not in buckets[src]:
                buckets[src].append(ticker)
    return buckets


def compare_generated_lists(days: dict, history, proofs) -> dict:
    """Today's list builders versus #336 snapshot membership.

    The snapshot was pushed on 2026-09-25. It is not a pre-open copy, so
    a match here does not make the list rebuildable for earlier days.
    A day is tested only when the prior Finviz export has a server time
    at or before this session's 09:30.
    """
    sys.path.insert(0, str(ROOT))
    from src import gainer_asof as ga
    from src import gainer_capture as gc
    from src import ohlc_ripper as ohlc

    keys = ("ohlc_hot", "yday_gainer", "yday_mover", "overnight", "probable", "earn_react")
    built_report = {
        key: {
            "key": key,
            "tested": 0,
            "matched": 0,
            "preopen_tested": 0,
            "preopen_matched": 0,
            "rebuildable": False,
            "days": [],
            "note": (
                "No pre-open file stores this list. "
                "The rate below is against the #336 snapshot, which is not a pre-open copy."
            ),
        }
        for key in keys
    }
    look = list(SESSIONS)
    try:
        look = gc.lookback_calendar(list(SESSIONS))
    except Exception as exc:
        print(f"[audit] calendar fallback {exc}", flush=True)
    original = ga.load_finviz
    state = {"cutoff_day": None, "frames": {}}

    def load_finviz(date: str):
        import pandas as pd
        key = (state["cutoff_day"], date)
        if key in state["frames"]:
            return state["frames"][key]
        path = f"data/exports/finviz_{date}.csv"
        sha = latest_proven(history.get(path) or [], proofs, cutoff_for(state["cutoff_day"]))
        frame = pd.DataFrame()
        if sha:
            blob = git_bytes(sha, path)
            if blob:
                frame = pd.read_csv(io.BytesIO(blob))
        state["frames"][key] = frame
        return frame

    ga.load_finviz = load_finviz
    try:
        for day in SESSIONS:
            if day < PANEL_ERA:
                continue
            snap_rows = (days[day].get("snap_rows") or [])
            if not snap_rows:
                continue
            prior = gc.knowable_export_date(look, day)
            # knowable_export_date reads the working tree. Prefer the
            # snapshot's recorded prior export when it is proven.
            recorded = None
            # filled by caller via days[day]['prior_export'] if set
            recorded = days[day].get("prior_export")
            prior = recorded or prior
            if not prior:
                continue
            export_sha = latest_proven(
                history.get(f"data/exports/finviz_{prior}.csv") or [],
                proofs, cutoff_for(day),
            )
            if not export_sha:
                for key in keys:
                    built_report[key]["days"].append({
                        "day": day, "tested": False,
                        "reason": f"prior export {prior} not proven before the open",
                    })
                continue
            state["cutoff_day"] = day
            nxt = gc.next_session(look, day)
            built = {
                "yday_gainer": gc.yesterday_gainers(prior, top_n=25),
                "yday_mover": gc.yesterday_movers(prior, top_n=20),
                "ohlc_hot": ohlc.liquid_hot(prior, day, top_n=30),
                "overnight": gc.overnight_scheduled(prior, day, nxt),
                "probable": ohlc.continuation(prior, day, top_n=ohlc.CONT_TOP_N),
                "earn_react": gc.earnings_reaction(prior, day),
            }
            saved = _snapshot_sources({"rows": snap_rows})
            for key in keys:
                fresh = [t for t in built[key] if t]
                old = saved.get(key) or []
                ok = fresh == old
                built_report[key]["tested"] += 1
                if ok:
                    built_report[key]["matched"] += 1
                built_report[key]["days"].append({
                    "day": day,
                    "tested": True,
                    "match": ok,
                    "set_match": set(fresh) == set(old),
                    "fresh": len(fresh),
                    "saved": len(old),
                    "only_fresh": [t for t in fresh if t not in set(old)][:8],
                    "only_saved": [t for t in old if t not in set(fresh)][:8],
                    "prior": prior,
                    "export_sha": export_sha,
                })
            print(
                "[audit] lists "
                + day
                + " "
                + " ".join(f"{k}={built_report[k]['days'][-1]['match']}" for k in keys),
                flush=True,
            )
    finally:
        ga.load_finviz = original
    return built_report


def compare_price_features(days: dict) -> dict:
    """Today's ohlc.features versus snapshot row fields.

    Those rows were not on GitHub before the open. A match does not
    make the features rebuildable.
    """
    sys.path.insert(0, str(ROOT))
    from src import ohlc_ripper as ohlc
    out = {
        "key": "price_features",
        "tested": 0,
        "matched": 0,
        "rebuildable": False,
        "days": [],
        "note": (
            "No pre-open file stores ret/rvol/hot_score. "
            "The rate below is against the #336 snapshot, which is not a pre-open copy."
        ),
    }
    for day in SESSIONS:
        if day < PANEL_ERA:
            continue
        rows = days[day].get("snap_rows") or []
        if not rows:
            continue
        same = 0
        bad = 0
        for row in rows:
            ticker = str(row.get("ticker") or "").strip().upper()
            feat = ohlc.features(ticker, day) if ticker else {}
            ok = True
            for field, src in (
                ("ohlc_ret_1", "ret_1"),
                ("ohlc_ret_5", "ret_5"),
                ("ohlc_ret_10", "ret_10"),
                ("ohlc_rvol", "rvol"),
                ("ohlc_hot_score", "hot_score"),
            ):
                if not close_num(row.get(field), feat.get(src)):
                    ok = False
                    break
            if ok:
                same += 1
            else:
                bad += 1
        day_match = same == len(rows) and len(rows) > 0
        out["tested"] += 1
        if day_match:
            out["matched"] += 1
        out["days"].append({
            "day": day, "rows": len(rows), "same": same, "bad": bad, "match": day_match,
        })
        print(f"[audit] px {day} same={same}/{len(rows)}", flush=True)
    return out


def _ai_shapes(days: dict) -> dict[str, set]:
    shapes: dict[str, set] = defaultdict(set)
    for day in SESSIONS:
        if day < PANEL_ERA:
            continue
        for key in AI_ROLES:
            item = named_item(days[day]["items"], key)
            sha = (item or {}).get("proven")
            path = (item or {}).get("path")
            if not sha or not path:
                continue
            blob = git_bytes(sha, path)
            if blob is None:
                continue
            shapes[key].add(blob_shape(path, blob))
        for item in days[day]["items"]:
            if item["group"] != "sector" or not item.get("proven"):
                continue
            blob = git_bytes(item["proven"], item["path"])
            if blob is None:
                continue
            shapes["sector:" + sector_role(item["path"])].add(blob_shape(item["path"], blob))
        for item in days[day]["items"]:
            if item["group"] != "excel" or not item.get("proven"):
                continue
            blob = git_bytes(item["proven"], item["path"])
            if blob is None:
                continue
            role = "excel-csv" if item["path"].endswith(".csv") else "excel-md"
            shapes[role].add(blob_shape(item["path"], blob))
    return shapes


def _file_cell(day: str, item: dict | None, *, ai: bool, shapes: set) -> str:
    if not item or not item.get("proven"):
        return "missing"
    if day >= PANEL_ERA or not ai:
        return "available-proven"
    blob = git_bytes(item["proven"], item["path"])
    if blob is None or not shapes:
        return "missing"
    if blob_shape(item["path"], blob) not in shapes:
        return "missing"
    return "available-proven"


def availability(days: dict, ab_proof: dict) -> dict:
    """Per day, per input: available-proven, rebuildable-verified, or missing."""
    shapes = _ai_shapes(days)
    ab_rebuild = bool(ab_proof.get("rebuildable"))
    grid = {}
    for day in SESSIONS:
        items = days[day]["items"]
        cell = {}
        for key in (
            "digest", "map_heat", "export", "join", "catalyst", "baseline",
            "weather", "predict", "actions", "judge", "events", "research",
        ):
            item = named_item(items, key)
            ai = key in AI_ROLES
            cell[key] = _file_cell(day, item, ai=ai, shapes=shapes.get(key) or set())
        ab_item = next((it for it in items if it["group"] == "ab"), None)
        cell["ab"] = _file_cell(day, ab_item, ai=False, shapes=set())
        if day in set(ab_proof.get("flag_days") or []):
            cell["ab_a1_a15"] = "available-proven"
        elif day < PANEL_ERA and ab_rebuild:
            cell["ab_a1_a15"] = "rebuildable-verified"
        else:
            cell["ab_a1_a15"] = "missing"
        sector_items = [it for it in items if it["group"] == "sector" and not it["path"].endswith("/")]
        if not sector_items:
            cell["sector"] = "missing"
        else:
            ok = True
            for item in sector_items:
                role = "sector:" + sector_role(item["path"])
                status = _file_cell(
                    day, item, ai=True, shapes=shapes.get(role) or set(),
                )
                if status != "available-proven":
                    ok = False
                    break
            cell["sector"] = "available-proven" if ok else "missing"
        excel_items = [it for it in items if it["group"] == "excel"]
        excel_ok = True
        saw_excel = False
        for item in excel_items:
            if item["match"] == "both absent" and not item.get("proven"):
                continue
            saw_excel = True
            role = "excel-csv" if item["path"].endswith(".csv") else "excel-md"
            status = _file_cell(day, item, ai=True, shapes=shapes.get(role) or set())
            if status != "available-proven":
                excel_ok = False
        cell["excel"] = "available-proven" if saw_excel and excel_ok else "missing"
        pin = next((it for it in items if it["group"] == "price"), None)
        cell["price_pin"] = _file_cell(day, pin, ai=False, shapes=set())
        for key in REBUILD_KEYS:
            if key == "ab_a1_a15":
                continue
            # No pre-open copy of these outputs, and the snapshot match
            # is not a license. Early and late days stay missing.
            cell[key] = "missing"
        cell["alarm"] = "missing"
        cell["flatten"] = "missing"
        cell["mover_buy"] = "missing"
        grid[day] = cell
    return grid


HOT4_PARTS = (
    "yday_gainer", "yday_mover", "ohlc_hot", "overnight", "probable",
    "earn_react", "price_features", "alarm", "flatten", "mover_buy",
)


def hot4_days(grid: dict) -> list[str]:
    ready = []
    for day in SESSIONS:
        cell = grid[day]
        if all(cell.get(part) in ("available-proven", "rebuildable-verified") for part in HOT4_PARTS):
            ready.append(day)
    return ready


def rebuild_report(days: dict, history, proofs) -> dict:
    print("[audit] rebuild proofs", flush=True)
    ab_proof = compare_ab_rules(days, history, proofs)
    lists = compare_generated_lists(days, history, proofs)
    prices = compare_price_features(days)
    grid = availability(days, ab_proof)
    ready = hot4_days(grid)
    return {
        "ab": ab_proof,
        "lists": lists,
        "prices": prices,
        "grid": grid,
        "hot4_days": ready,
    }


def esc(text: str) -> str:
    return str(text).replace("|", "\\|")


def _rate(matched: int, tested: int) -> str:
    if not tested:
        return "no days"
    pct = 100.0 * matched / tested
    return f"{matched}/{tested} ({pct:.1f}%)"


def render_per_input(w, report: dict) -> None:
    block = report.get("per_input") or {}
    if not block:
        return
    w("## Per-input availability")
    w("")
    w("This section is per input, not per day. A derived input is "
      "rebuildable for sessions before 2026-09-09 only when today's code "
      "matches the server-proven pre-open copy on every session from "
      "2026-09-09 onward that has one. AI text is never regenerated. "
      "An early AI file counts only when that pre-open copy exists and "
      "its keys, columns, or headings match a proven copy from 2026-09-09 on.")
    w("")
    w("Cell values are `available-proven`, `rebuildable-verified`, or `missing`.")
    w("")
    ab = block["ab"]
    w("### Match rate against the pre-open copy")
    w("")
    w("| Input | Proven days (09-09 on) | Matched | Match rate | Rebuildable for earlier days |")
    w("| --- | ---: | ---: | --- | --- |")
    w(f"| AB price rules A1-A15 | {ab['tested']} | {ab['matched']} | "
      f"{_rate(ab['matched'], ab['tested'])} | "
      f"{'yes' if ab['rebuildable'] else 'no'} |")
    for key in (
        "price_features", "ohlc_hot", "yday_gainer", "yday_mover",
        "overnight", "probable", "earn_react",
    ):
        w(f"| {key} | 0 | — | no pre-open copy of this output | no |")
    w("")
    w(ab["note"])
    w("")
    if ab.get("days"):
        w("AB days. The rebuilt last bar is the newest bar strictly before "
          "09:30 that today's price store still has. The saved last bar is "
          "the mode of `pair_day_b` in the pre-open file.")
        w("")
        w("| Day | Rows | Same flags | Match | Saved last bar | Rebuilt last bar | Flags that differ |")
        w("| --- | ---: | ---: | --- | --- | --- | --- |")
        for row in ab["days"]:
            flags = ", ".join(f"{name} {n}" for name, n in row.get("mismatch_flags") or []) or "—"
            if row.get("reason"):
                flags = row["reason"]
            w(f"| {row['day']} | {row.get('rows', '—')} | {row.get('same', '—')} | "
              f"{'yes' if row.get('match') else 'no'} | {row.get('saved_last_bar') or '—'} | "
              f"{row.get('rebuilt_last_bar') or '—'} | {esc(flags)} |")
        w("")
    w("### Same generator against the #336 snapshot")
    w("")
    w("The snapshot files were pushed on 2026-09-25, after every open in "
      "this window. A match here shows that today's function still emits "
      "the membership or the price fields stored in that late file. It "
      "does not make the input rebuildable.")
    w("")
    w("| Input | Days compared | Matched | Rate |")
    w("| --- | ---: | ---: | --- |")
    prices = block["prices"]
    w(f"| price_features | {prices['tested']} | {prices['matched']} | "
      f"{_rate(prices['matched'], prices['tested'])} |")
    for key, rec in block["lists"].items():
        w(f"| {key} | {rec['tested']} | {rec['matched']} | "
          f"{_rate(rec['matched'], rec['tested'])} |")
    w("")
    w(prices["note"])
    w("")
    for key, rec in block["lists"].items():
        misses = [d for d in rec["days"] if d.get("tested") and not d.get("match")]
        if not misses:
            continue
        w(f"`{key}` mismatches:")
        w("")
        for row in misses[:8]:
            same_names = "same names, different order" if row.get("set_match") else "different names"
            w(f"- {row['day']}: {same_names}; rebuilt {row['fresh']}, snapshot {row['saved']}, "
              f"only in rebuild {', '.join(row['only_fresh']) or '—'}, "
              f"only in snapshot {', '.join(row['only_saved']) or '—'}.")
        w("")
    px_miss = [d for d in prices["days"] if not d.get("match")]
    if px_miss:
        w("Price-feature days that are not an exact row match: "
          + ", ".join(f"{d['day']} ({d['same']}/{d['rows']})" for d in px_miss)
          + ".")
        w("")
    w("### Day by input")
    w("")
    cols = (
        ("digest", "digest"),
        ("map_heat", "map_heat"),
        ("export", "export"),
        ("join", "join"),
        ("catalyst", "catalyst"),
        ("baseline", "baseline"),
        ("weather", "weather"),
        ("predict", "predict"),
        ("actions", "actions"),
        ("judge", "judge"),
        ("events", "events"),
        ("research", "research"),
        ("ab", "ab file"),
        ("ab_a1_a15", "A1-A15"),
        ("sector", "sector"),
        ("excel", "excel"),
        ("price_pin", "price pin"),
        ("price_features", "price feat."),
        ("ohlc_hot", "ohlc_hot"),
        ("yday_gainer", "yday_gainer"),
        ("yday_mover", "yday_mover"),
        ("overnight", "overnight"),
        ("probable", "probable"),
        ("earn_react", "earn_react"),
        ("alarm", "alarm"),
        ("flatten", "flatten"),
        ("mover_buy", "mover_buy"),
    )
    header = "| Day | " + " | ".join(label for _key, label in cols) + " |"
    sep = "| --- | " + " | ".join("---" for _ in cols) + " |"
    w(header)
    w(sep)
    short_cell = {
        "available-proven": "available-proven",
        "rebuildable-verified": "rebuildable-verified",
        "missing": "missing",
    }
    grid = block["grid"]
    for day in SESSIONS:
        cells = " | ".join(short_cell[grid[day][key]] for key, _label in cols)
        w(f"| {day[5:]} | {cells} |")
    w("")
    w("Alarm, flatten, and mover_buy have no pre-open copy of the list or "
      "the bit, and they are not rebuilt. Alarm is the camera gate HOT4 "
      "forbids. Flatten and mover_buy are union members that come from the "
      "stock-book wish list, not from the price-tape functions above.")
    w("")
    w("### HOT4 and holdup components")
    w("")
    w("HOT4 (`union_hot_n4_h1`) and holdup (`union_hot_n4_holdup`) use the "
      "same union: yday_gainer, yday_mover, ohlc_hot, overnight, probable, "
      "earn_react, plus price features for the hot_score rank, plus the "
      "alarm gate, plus flatten and mover_buy. Holdup adds a carry rule, "
      "not a new file. A component can run when its cell is "
      "`available-proven` or `rebuildable-verified`. The recipe is scored "
      "only on days where every one of those components can run.")
    w("")
    ready = block["hot4_days"]
    if ready:
        w("Days where every component can run: " + ", ".join(ready) + ".")
    else:
        w("Days where every component can run: none.")
    w("")
    w("| Day | Components that can run | Recipe |")
    w("| --- | --- | --- |")
    for day in SESSIONS:
        cell = grid[day]
        can = [part for part in HOT4_PARTS
               if cell.get(part) in ("available-proven", "rebuildable-verified")]
        recipe = "yes" if day in ready else "no"
        shown = ", ".join(can) if can else "—"
        w(f"| {day[5:]} | {shown} | {recipe} |")
    w("")
    w("Replay of HOT4 and holdup on that set only, from $10,000, "
      "`persist=False`. Days outside the set are left out.")
    w("")
    for label, score in (
        ("HOT4 and holdup on input-ready days, GLND kept", report.get("score_input_keep") or {}),
        ("HOT4 and holdup on input-ready days, GLND removed", report.get("score_input_drop") or {}),
    ):
        w(f"**{label}.**")
        if not score.get("days"):
            w("No session has every HOT4 input available-proven or rebuildable-verified, "
              "so there is no return and no ending equity.")
            w("")
            continue
        for name, rec in score["recipes"].items():
            w(f"- {name}: {rec['compound_pct']}% , ending equity {rec['equity']}, "
              f"sessions {rec['n']}.")
        w("")


def render(report: dict) -> str:
    lines = []
    w = lines.append
    w("# Input provenance for the #336 sequential Factor Mine rebuild")
    w("")
    w("Read-only audit. No ledger, snapshot, panel, or state file was written.")
    w("")
    w("## Plain summary")
    w("")
    frozen = report["frozen_days"]
    not_frozen = [d for d in SESSIONS if d not in frozen]
    w(f"Sessions checked: {len(SESSIONS)} ({SESSIONS[0]} through {SESSIONS[-1]}).")
    w(f"Proven-frozen: {len(frozen)}. Not proven: {len(not_frozen)}.")
    if frozen:
        w("Proven-frozen days: " + ", ".join(frozen) + ".")
    else:
        w("Proven-frozen days: none.")
    w("Not proven: " + ", ".join(not_frozen) + ".")
    w("")
    w("Per input, none of the price lists, price features, or A1-A15 rules "
      "matched a server-proven pre-open copy on every day from 2026-09-09 on, "
      "so none are rebuildable for earlier sessions. HOT4 and holdup still have "
      "no day on which every component is available-proven or rebuildable-verified. "
      "The table is in Per-input availability.")
    w("")
    w("HOT4 is `union_hot_n4_h1`. Holdup is `union_hot_n4_holdup`. "
      "The replay runs only on proven-frozen days, in that order, starting from $10,000. "
      "A day that is not proven is left out, not scored as a flat day.")
    w("")
    for label, block in (
        ("HOT4 and holdup, GLND kept", report["score_keep"]),
        ("HOT4 and holdup, GLND removed", report["score_drop"]),
    ):
        w(f"**{label}.**")
        if not block.get("days"):
            w("No proven-frozen sessions, so there is no return and no ending equity.")
            w("")
            continue
        for name, rec in block["recipes"].items():
            w(f"- {name}: {rec['compound_pct']}% , ending equity {rec['equity']}, "
              f"sessions {rec['n']}.")
        w("")
    tape = report["tape"]
    w(f"The fill tape the scorer opens is `{tape['path']}`, commit `{short(tape['sha'])}`, "
      f"server time {iso(tape['proof'].get('time'))} via {tape['proof'].get('kind')}. "
      "That is one file for every session. It is pre-open only for a session whose "
      "09:30 ET is after that server time.")
    w("")
    render_per_input(w, report)
    w("## What the #336 path opens")
    w("")
    w("`src/factor_mine_sequential.run_books` steps each session in "
      "`factor_mine_retro.SESSIONS` (2026-08-13 through 2026-09-24). "
      "2026-09-25 is on disk as a later live snapshot and is included in the trace; "
      "that walk's date list does not score it.")
    w("")
    w("For session D the walk calls `rows_for_day(D)`, which opens only "
      "`data/factor_mine/snapshots/D.json` and keeps rows dated D. "
      "Fills come from `load_bar_store`, which reads "
      "`data/factor_mine/retro_prices/ohlc.parquet` and then drops bars after D. "
      "The prior session is `data/factor_mine/state/<recipe>/<prior>.json` when a "
      "file is already there. This audit does not rewrite those files. "
      "The published snapshots and the parquet first appear in merge "
      f"`{MERGE}` (the #336 merge).")
    w("")
    w("Those snapshot rows were built earlier in the same rebuild by "
      "`factor_mine_retro.materialize` plus `build_panel`. `materialize` copies "
      "each packet's last commit whose **committer** time is at or before 09:30 ET. "
      "The snapshot `sources` map is that pick for the twelve named inputs. "
      "`build_index` then opens the AB checklist (slim, else enriched, else the "
      "plain checklist). Sector files are copied into the overlay when their "
      "committer time is early enough; `build_panel` does not open the sector folder. "
      "`pin_excel_signals` records `excel_bot/suggestions/suggestions.csv` and each "
      "final daily note, again by committer time. `build_panel` does not open them. "
      "The live price pin `data/factor_mine/prices/D.json` is not what the sequential "
      "walk reads. `data/factor_mine/panel.json` is not opened by `rows_for_day` or "
      "by `build_panel`; the candidate rows below are the snapshot rows compared "
      "with whatever rows for D were in the last pre-open `panel.json`.")
    w("")
    w("Committer timestamps are shown only as the clock the code used. "
      "They do not prove the file was on GitHub.")
    w("")
    w("## Server clock")
    w("")
    ev = report["events_meta"]
    w(f"Actions runs were listed per day from 2026-08-01 through 2026-09-26 "
      f"({report['n_action_shas']} distinct head SHAs). "
      f"The repo events API returned {ev.get('n')} events, "
      f"from {ev.get('oldest')} to {ev.get('newest')}. "
      f"Those PushEvent payloads omit the commits array; the push head is present, "
      f"and that matched {ev.get('push_shas')} SHAs. "
      "The retained window starts on 2026-09-25, so it cannot prove an August or "
      "earlier-September blob. Those days use the Actions run.")
    w("")
    w("A blob is **proven before the open** when a server time is at or before "
      "09:30 ET. A push-event Actions run (or a PushEvent) after 09:30 means the "
      "commit was pushed after the open. Any other run that starts after 09:30 "
      "does not prove the commit was late, and it also does not prove it was early, "
      "so the blob stays **not proven**.")
    w("")
    w("## Days")
    w("")
    w("| date | proven-frozen | candidate rows vs pre-open panel | inputs that miss |")
    w("| --- | --- | --- | --- |")
    for day in SESSIONS:
        info = report["days"][day]
        rows = info["rows"]
        if rows["status"] == "same" and not rows["same"]:
            row_txt = "both empty (snapshot scored no names, and the pre-open panel had none for this date)"
        elif rows["status"] == "added" and not rows["same"] and not rows["removed"]:
            row_txt = f"added {rows['added']} (pre-open panel had no row for this date)"
        else:
            row_txt = (
                f"{rows['status']} same={rows['same']} added={rows['added']} "
                f"removed={rows['removed']} changed={rows['changed']}"
            )
        if rows["fields"]:
            row_txt += " fields=" + ",".join(rows["fields"][:8])
            if len(rows["fields"]) > 8:
                row_txt += f" (+{len(rows['fields']) - 8})"
        miss = info["reasons"][:4]
        extra = ""
        if len(info["reasons"]) > 4:
            extra = f" (+{len(info['reasons']) - 4} more)"
        flag = "yes" if info["frozen"] else "no"
        w(f"| {day} | {flag} | {esc(row_txt)} | {esc('; '.join(miss) + extra)} |")
    w("")
    w("## Inputs by day")
    w("")
    w("Sector files are one summary row. A sector day matches only when every "
      "non-trace file in that folder matches. `used` is the blob the code selected. "
      "`pre-open` is the latest blob with a server time at or before 09:30 ET.")
    w("")
    for day in SESSIONS:
        info = report["days"][day]
        w(f"### {day}")
        w("")
        snap = info["snap_meta"]
        w(f"Snapshot label `{snap.get('label')}`, rows {snap.get('n_rows')}, "
          f"code sha the snapshot records `{short(snap.get('code_sha'))}`.")
        w("")
        w("| group | path | used | server | verdict | pre-open | match |")
        w("| --- | --- | --- | --- | --- | --- | --- |")
        shown = []
        sector_items = [it for it in info["items"] if it["group"] == "sector"]
        for item in info["items"]:
            if item["group"] == "sector":
                continue
            proof = item["used_proof"]
            shown.append(item)
            w("| {group} | `{path}` | `{used}` | {when} {kind} | {status} | `{pre}` | {match} |".format(
                group=item["group"],
                path=esc(item["path"]),
                used=short(item.get("used")),
                when=iso(proof.get("time")),
                kind=proof.get("kind") or "",
                status=item["used_status"],
                pre=short(item.get("proven")),
                match=esc(item["match"]),
            ))
        if sector_items:
            bad = [it for it in sector_items if it["match"] not in ("same", "both absent")
                   or (it.get("used") and it["used_status"] != "proven_before")]
            w(f"| sector | `01_daily/sectors/{day}/` ({len(sector_items)} files) | | | "
              f"{'fail' if bad else 'ok'} | | {len(sector_items) - len(bad)} match, {len(bad)} miss |")
        w("")
        if sector_items:
            bad = [it for it in sector_items if it["match"] not in ("same", "both absent")
                   or (it.get("used") and it["used_status"] != "proven_before")]
            if bad:
                w("Sector misses:")
                w("")
                for it in bad[:8]:
                    w(f"- `{it['path']}` used `{short(it.get('used'))}` "
                      f"{it['used_status']} pre-open `{short(it.get('proven'))}` {it['match']}")
                if len(bad) > 8:
                    w(f"- and {len(bad) - 8} more")
                w("")
        diff = info.get("file_diffs") or []
        interesting = [d for d in diff if d["diff"]["status"] != "same"]
        if interesting:
            w("Row diff, blob the code selected versus the proven pre-open blob:")
            w("")
            for entry in interesting[:12]:
                d = entry["diff"]
                fields = ",".join(d["fields"][:10]) or "—"
                w(f"- `{entry['path']}`: {d['status']} same={d['same']} "
                  f"added={d['added']} removed={d['removed']} changed={d['changed']} "
                  f"fields={fields}")
            if len(interesting) > 12:
                w(f"- and {len(interesting) - 12} more files")
            w("")
        rows = info["rows"]
        fields = ",".join(rows["fields"]) if rows["fields"] else "—"
        w("Candidate rows the walk scored (snapshot) versus rows dated this session "
          "in the last `panel.json` proven before the open: "
          f"**{rows['status']}** same={rows['same']} added={rows['added']} "
          f"removed={rows['removed']} changed={rows['changed']}. Fields: {fields}.")
        if rows["added_ids"]:
            w("Added tickers (up to 12): " + ", ".join(rows["added_ids"]) + ".")
        if rows["removed_ids"]:
            w("Removed tickers (up to 12): " + ", ".join(rows["removed_ids"]) + ".")
        w("")
    w("## Panel rewrites")
    w("")
    w("`data/factor_mine/panel.json` commits on this branch, oldest first. "
      "Server time is the proof above, not the committer clock.")
    w("")
    w("| commit | committer (not proof) | server | proof | sessions in the file |")
    w("| --- | --- | --- | --- | --- |")
    for row in report["panel_log"]:
        w(f"| `{short(row['sha'])}` | {row['committer']} | {iso(row['time'])} | "
          f"{row['kind']} | {row['span']} |")
    w("")
    w(f"Commits that change the file and are reachable from HEAD: {len(report['panel_log'])}. "
      "The first is the commit that added it "
      "(`f5b46d20eec2`, server push 2026-09-09 07:00:04Z). "
      "`git log -- data/factor_mine/panel.json` simplifies that list to 21. "
      "The two it drops, `75ff1cd1b5b0` and `6e140da069f2`, still changed the file "
      "and were the default-branch head on 2026-09-19 (a workflow_dispatch run started on each).")
    w("")
    w("## Recompute")
    w("")
    w("Implemented as `recompute` in `research/audit/input_provenance_336.py`. "
      "It calls `factor_mine_sequential.walk` with `persist=False` and the two recipes, "
      "then again with `exclude='GLND'`. An empty proven-frozen list returns no return. "
      "Before and after the run the script compares size and mtime of the ledger directory, "
      "the state directory, snapshots, price pins, `panel.json`, the freeze manifest, "
      "and the retro parquet.")
    w("")
    w(f"Fingerprint unchanged: {report['fingerprint_ok']}.")
    w("")
    return "\n".join(lines) + "\n"


def panel_span(sha: str) -> str:
    blob = git_bytes(sha, "data/factor_mine/panel.json")
    if not blob:
        return "—"
    try:
        doc = json.loads(blob)
    except json.JSONDecodeError:
        return "unreadable"
    dates = sorted({str(r.get("date") or "")[:10] for r in (doc.get("rows") or []) if r.get("date")})
    if not dates:
        return "no dated rows"
    return f"{dates[0]} .. {dates[-1]} ({len(dates)})"


def main() -> None:
    before = fingerprint()
    print("[audit] fingerprint files", len(before), flush=True)
    tracked = set(ls_files("data/ab_checklist"))
    sectors = ls_files("01_daily/sectors")
    paths = {
        "data/factor_mine/panel.json",
        "excel_bot/suggestions/suggestions.csv",
        "data/factor_mine/retro_prices/ohlc.parquet",
    }
    for day in SESSIONS:
        paths.add(f"data/factor_mine/snapshots/{day}.json")
        paths.add(f"data/factor_mine/prices/{day}.json")
        paths.add(f"excel_bot/daily/{day}_excel_bot.md")
        for tmpl in NAMED.values():
            paths.add(tmpl.format(d=day))
        for path in ab_paths(day, tracked):
            paths.add(path)
        for path in sector_paths(day, sectors):
            paths.add(path)
    print(f"[audit] paths {len(paths)}", flush=True)
    history = load_history(sorted(paths))
    print("[audit] history loaded", flush=True)
    runs = fetch_actions()
    events, events_meta = fetch_events()
    print(f"[audit] events {events_meta}", flush=True)
    needed = set()
    for rows in history.values():
        for _ts, sha in rows:
            needed.add(sha)
    lookup_missing(sorted(needed), runs)
    proofs: dict[str, dict] = {}
    for sha in needed:
        proofs[sha] = prove(sha, runs, events)

    tape_sha = None
    tape_hist = history.get("data/factor_mine/retro_prices/ohlc.parquet") or []
    for _ts, sha in tape_hist:
        if sha == MERGE:
            tape_sha = MERGE
    if tape_sha is None and tape_hist:
        tape_sha = tape_hist[-1][1]
    if tape_sha and tape_sha not in proofs:
        proofs[tape_sha] = prove(tape_sha, runs, events)
    tape_proof = proofs.get(tape_sha) or {"kind": "none", "time": None}

    days = {}
    for day in SESSIONS:
        snap, snap_commit = load_snapshot(day, history)
        items = build_inputs(day, snap, snap_commit, history, tracked, sectors)
        for item in items:
            used = item.get("used")
            if used and used not in proofs:
                proofs[used] = prove(used, runs, events)
        annotate(items, history, proofs, cutoff_for(day))
        file_diffs = []
        for item in items:
            if item["group"] in ("snapshot", "panel", "sector"):
                continue
            if item["match"] in ("same", "both absent"):
                continue
            diff = diff_item(item, day)
            if diff:
                file_diffs.append({"path": item["path"], "diff": diff})
        panel_item = next(it for it in items if it["group"] == "panel")
        rows = candidate_diff(day, snap, panel_item.get("proven"))
        parquet_status = before_open(tape_proof, cutoff_for(day))
        frozen, reasons = day_frozen(items, rows, parquet_status)
        days[day] = {
            "items": items,
            "rows": rows,
            "frozen": frozen,
            "reasons": reasons,
            "file_diffs": file_diffs,
            "snap_meta": {
                "label": snap.get("label"),
                "n_rows": snap.get("n_rows"),
                "code_sha": snap.get("code_sha"),
            },
            "snap_rows": [r for r in (snap.get("rows") or []) if isinstance(r, dict)],
            "prior_export": (snap.get("candidates") or {}).get("prior_export"),
        }
        print(f"[audit] {day} frozen={frozen} rows={rows['status']} reasons={len(reasons)}", flush=True)

    frozen_days = [d for d in SESSIONS if days[d]["frozen"]]
    per_input = rebuild_report(days, history, proofs)
    score_keep = recompute(frozen_days, drop_glnd=False)
    score_drop = recompute(frozen_days, drop_glnd=True)
    input_days = per_input["hot4_days"]
    score_input_keep = recompute(input_days, drop_glnd=False)
    score_input_drop = recompute(input_days, drop_glnd=True)
    cache_py = ROOT / "src" / "_ab_checklist_cached.py"
    if cache_py.is_file():
        cache_py.unlink()
    after = fingerprint()
    if after != before:
        moved = [k for k in set(before) | set(after) if before.get(k) != after.get(k)]
        raise SystemExit(f"fingerprint changed: {moved[:8]}")

    panel_log = []
    for ts, sha in history.get("data/factor_mine/panel.json") or []:
        proof = proofs.get(sha) or prove(sha, runs, events)
        panel_log.append({
            "sha": sha,
            "committer": iso(ts),
            "time": proof.get("time"),
            "kind": proof.get("kind"),
            "span": panel_span(sha),
        })

    report = {
        "frozen_days": frozen_days,
        "days": days,
        "score_keep": score_keep,
        "score_drop": score_drop,
        "per_input": per_input,
        "score_input_keep": score_input_keep,
        "score_input_drop": score_input_drop,
        "fingerprint_ok": True,
        "events_meta": events_meta,
        "n_action_shas": len(runs),
        "tape": {
            "path": "data/factor_mine/retro_prices/ohlc.parquet",
            "sha": tape_sha,
            "proof": tape_proof,
        },
        "panel_log": panel_log,
    }
    OUT_MD.write_text(render(report), encoding="utf-8")
    print(f"[audit] wrote {OUT_MD}", flush=True)
    print(f"[audit] frozen {frozen_days}", flush=True)


if __name__ == "__main__":
    main()
