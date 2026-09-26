#!/usr/bin/env python3
"""Build research/audit/FULLSCAN_FILE_PROOF.csv from Actions job-log pushes.

A commit is timed only when a producer-workflow log contains
`[safe-push] pushed <sha>` or `old..new <ref> -> main`. The server time is
that log line's timestamp. Committer time is never a proof. A commit that
no run pushed is not proven.

PROVEN requires a pre-open blob (log time <= 13:30 UTC that session),
coverage (the blob contains that morning's date, not merely a dated path),
and, for headline-built inputs, STALE_CONTENT=no. Later rows are listed and
do not count. Headline days are the #331 quarantine list.
"""
from __future__ import annotations

import csv
import hashlib
import io
import json
import os
import re
import subprocess
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path("/workspace")
CACHE = Path("/tmp/fm_audit_336")
OUT_CSV = ROOT / "research/audit/FULLSCAN_FILE_PROOF.csv"
OUT_MD = ROOT / "research/audit/FULLSCAN_FILE_PROOF.md"
EXCEL_PROOF = ROOT / "research/audit/excel_preopen_proof.csv"
EXCEL_RUN = re.compile(
    r"actions_run (\d+) \(([^)]+)\) job (\d+).*?"
    r"run (\d{4}-\d{2}-\d{2}T[\d:.]+Z)\.\.(\d{4}-\d{2}-\d{2}T[\d:.]+Z)"
)
HITS = CACHE / "push_hits.tsv"
MAIN = "origin/main"

SESSIONS = (
    "2026-08-13", "2026-08-14", "2026-08-17", "2026-08-18", "2026-08-19",
    "2026-08-20", "2026-08-21", "2026-08-24", "2026-08-25", "2026-08-26",
    "2026-08-27", "2026-08-28", "2026-08-31", "2026-09-01", "2026-09-02",
    "2026-09-03", "2026-09-04", "2026-09-08", "2026-09-09", "2026-09-10",
    "2026-09-11", "2026-09-14", "2026-09-15", "2026-09-16", "2026-09-17",
    "2026-09-18", "2026-09-21", "2026-09-22", "2026-09-23", "2026-09-24",
    "2026-09-25",
)

# Headline-built inputs. Quarantine dates make STALE_CONTENT=yes.
HEADLINE = {
    "actions", "judge", "catalyst", "map_heat", "digest", "parsed",
    "market_digest", "research", "baseline", "events",
}

TEMPLATES = [
    ("ab_checklist", "data/ab_checklist/{d}_ab_checklist.csv"),
    ("ab_enriched", "data/ab_checklist/{d}_ab_checklist_enriched.csv"),
    ("predict", "01_daily/general/{d}_predict.md"),
    ("actions", "01_daily/news/{d}_actions.json"),
    ("judge", "01_daily/news/{d}_judge.json"),
    ("map_heat", "01_daily/map_heat/{d}_map_heat.json"),
    ("catalyst", "01_daily/catalyst/{d}_dossiers.json"),
    ("digest", "01_daily/news/{d}_finviz_digest.json"),
    ("parsed", "01_daily/news/{d}_parsed.json"),
    ("market_digest", "01_daily/news/{d}_finviz_market_digest.json"),
    ("export", "data/exports/finviz_{d}.csv"),
    ("join", "data/join/{d}_ranked.csv"),
    ("baseline", "01_daily/map_heat/{d}_research_baseline.json"),
    ("weather", "01_daily/weather/{d}_weather.json"),
    ("events", "01_daily/events/{d}_events.json"),
    ("research", "01_daily/map_heat/{d}_research.json"),
    ("stock_book", "data/stock_book/{d}_stock_book.json"),
    ("stock_suggestions", "data/stock_book/{d}_suggestions.json"),
    ("green", "data/stock_book/{d}_green.json"),
    ("peers", "data/peers/{d}_peer_rs.csv"),
    ("universe_membership", "data/universe/{d}_membership.csv"),
    ("segment_stats", "data/universe/{d}_segment_stats.csv"),
    ("quote_colors", "data/quote_colors/{d}_quote_colors.csv"),
    ("excel_daily", "excel_bot/daily/{d}_excel_bot.md"),
    ("excel_suggestions", "excel_bot/suggestions/suggestions.csv"),
]

SCORE_RE = re.compile(
    r"Prediction:\s*(UP|DOWN|FLAT).*?total score\s*(-?[\d.]+)",
    re.I | re.S,
)
TS_RE = re.compile(r"^(.*T\d{2}:\d{2}:\d{2})(\.\d+)?(.*)$")

COLUMNS = [
    "date", "input", "path", "blob_sha", "first_server_time_utc",
    "server_time_utc", "server_kind", "run_id", "run_start_utc",
    "run_finish_utc", "before_0930", "coverage", "coverage_note",
    "later_modified", "later_shas", "late_rows", "generator_workflow",
    "generator_script", "generator_commit", "stale_content",
    "byte_identical_to_head", "code_selected_sha_committer_clock_not_proof",
    "code_selected_matches", "proven", "status",
    "candidate_from_per_day_files",
]


def parse_ts(raw: str) -> datetime | None:
    text = (raw or "").strip()
    if not text:
        return None
    text = text.replace("Z", "+00:00")
    match = TS_RE.match(text)
    if not match:
        return None
    frac = match.group(2) or ""
    if frac:
        frac = "." + (frac[1:] + "000000")[:6]
    text = match.group(1) + frac + (match.group(3) or "+00:00")
    try:
        stamp = datetime.fromisoformat(text)
    except ValueError:
        return None
    if stamp.tzinfo is None:
        stamp = stamp.replace(tzinfo=timezone.utc)
    return stamp.astimezone(timezone.utc)


def cutoff_for(day: str) -> datetime:
    return datetime.strptime(day + " 13:30:00", "%Y-%m-%d %H:%M:%S").replace(
        tzinfo=timezone.utc)


def git_text(args: list[str]) -> str:
    proc = subprocess.run(
        ["git", *args], cwd=ROOT, capture_output=True, text=True, check=False,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"git {' '.join(args[:6])} failed: {proc.stderr[-300:]}")
    return proc.stdout


class BlobStore:
    def __init__(self) -> None:
        self.proc = subprocess.Popen(
            ["git", "cat-file", "--batch"],
            cwd=ROOT, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
        )

    def get(self, sha: str) -> bytes:
        if not sha:
            return b""
        assert self.proc.stdin and self.proc.stdout
        self.proc.stdin.write(f"{sha}\n".encode())
        self.proc.stdin.flush()
        header = self.proc.stdout.readline().decode("utf-8", "replace")
        parts = header.split()
        if len(parts) < 3 or parts[1] == "missing":
            return b""
        size = int(parts[2])
        data = self.proc.stdout.read(size)
        self.proc.stdout.read(1)
        return data

    def close(self) -> None:
        if self.proc.stdin:
            self.proc.stdin.close()
        self.proc.wait()


def load_parents() -> dict[str, str]:
    parents: dict[str, str] = {}
    for line in git_text(["rev-list", "--parents", MAIN]).splitlines():
        parts = line.split()
        if not parts:
            continue
        parents[parts[0]] = parts[1] if len(parts) > 1 else ""
    return parents


def load_indexes(parents: dict[str, str]):
    by7: dict[str, list[str]] = defaultdict(list)
    by8: dict[str, list[str]] = defaultdict(list)
    for sha in parents:
        by7[sha[:7]].append(sha)
        by8[sha[:8]].append(sha)
    return by7, by8


def resolve(prefix: str, by7, by8, parents: dict[str, str],
            parent_prefix: str = "") -> str | None:
    prefix = (prefix or "").strip().lower()
    if not prefix or not re.fullmatch(r"[0-9a-f]{7,40}", prefix):
        return None
    if len(prefix) == 40 and prefix in parents:
        return prefix
    if len(prefix) >= 8:
        cands = [s for s in by8.get(prefix[:8], []) if s.startswith(prefix)]
    else:
        cands = list(by7.get(prefix, []))
    if len(cands) == 1:
        return cands[0]
    if parent_prefix and len(cands) > 1:
        filt = [s for s in cands if (parents.get(s) or "").startswith(parent_prefix)]
        if len(filt) == 1:
            return filt[0]
    return None


def expand(old: str, new: str, parents: dict[str, str],
           cache: dict) -> list[str]:
    key = (old, new)
    if key in cache:
        return cache[key]
    if not new:
        cache[key] = []
        return []
    if parents.get(new) == old or not old:
        cache[key] = [new]
        return [new]
    out: list[str] = []
    cur = new
    seen: set[str] = set()
    while cur and cur not in seen and len(out) < 40:
        if cur == old:
            break
        seen.add(cur)
        out.append(cur)
        cur = parents.get(cur) or ""
        if not cur:
            break
    if old and cur != old and (not out or parents.get(out[-1]) != old):
        proc = subprocess.run(
            ["git", "rev-list", "--max-count=40", f"{old}..{new}"],
            cwd=ROOT, capture_output=True, text=True, check=False,
        )
        if proc.returncode == 0 and proc.stdout.strip():
            out = [line.strip() for line in proc.stdout.splitlines() if line.strip()]
    cache[key] = out
    return out


def load_proofs(parents, by7, by8) -> tuple[dict[str, dict], dict]:
    proofs: dict[str, dict] = {}
    stats = {"hits": 0, "unresolved": 0, "commits": 0}
    cache: dict = {}
    with HITS.open(encoding="utf-8") as fh:
        reader = csv.DictReader(fh, delimiter="\t")
        for row in reader:
            stats["hits"] += 1
            new = resolve(row.get("sha_new") or "", by7, by8, parents,
                          row.get("sha_old") or "")
            old = resolve(row.get("sha_old") or "", by7, by8, parents)
            if not new:
                stats["unresolved"] += 1
                continue
            when = parse_ts(row.get("line_ts") or "")
            kind = "log_line"
            if when is None:
                start = parse_ts(row.get("run_started_at") or "")
                finish = parse_ts(row.get("run_updated_at") or "")
                if start and finish and start <= finish:
                    when = finish
                    kind = "run_finish"
            if when is None:
                stats["unresolved"] += 1
                continue
            shas = expand(old or "", new, parents, cache)
            if new not in shas:
                shas = [new, *shas]
            rec = {
                "time": when,
                "kind": kind,
                "run_id": row.get("run_id") or "",
                "run_start": row.get("run_started_at") or "",
                "run_finish": row.get("run_updated_at") or "",
                "workflow": row.get("workflow_name") or "",
                "head_sha": row.get("head_sha") or "",
            }
            for sha in shas:
                prev = proofs.get(sha)
                if prev is None or when < prev["time"]:
                    proofs[sha] = rec
    stats["commits"] = len(proofs)
    return proofs, stats


def load_history() -> dict[str, list[dict]]:
    """First-parent commits on main that touched each path. Oldest first."""
    text = git_text([
        "log", "--first-parent", "--raw", "--no-abbrev", "--date=iso-strict",
        "--pretty=format:C %H %cI", MAIN, "--",
        "01_daily", "data/ab_checklist", "data/exports", "data/join",
        "data/stock_book", "data/peers", "data/universe", "data/quote_colors",
        "excel_bot/daily", "excel_bot/suggestions",
    ])
    history: dict[str, list[dict]] = defaultdict(list)
    sha = ""
    committer = ""
    for line in text.splitlines():
        if line.startswith("C "):
            parts = line.split()
            sha = parts[1]
            committer = parts[2] if len(parts) > 2 else ""
            continue
        if not line.startswith(":") or "\t" not in line:
            continue
        meta, _, path = line.partition("\t")
        path = path.split("\t")[-1].strip()
        bits = meta.split()
        if len(bits) < 5 or not sha:
            continue
        status = bits[4]
        newblob = bits[3]
        if status.startswith(("D", "U")) or newblob == "0" * 40:
            continue
        history[path].append({
            "commit": sha,
            "committer": committer,
            "blob": newblob,
        })
    for path in history:
        history[path].reverse()
    return history


def load_head_blobs() -> dict[str, str]:
    text = git_text([
        "ls-tree", "-r", MAIN, "--",
        "01_daily", "data/ab_checklist", "data/exports", "data/join",
        "data/stock_book", "data/peers", "data/universe", "data/quote_colors",
        "excel_bot/daily", "excel_bot/suggestions",
    ])
    blobs = {}
    for line in text.splitlines():
        meta, _, path = line.partition("\t")
        parts = meta.split()
        if len(parts) >= 3 and path:
            blobs[path] = parts[2]
    return blobs


def load_quarantine() -> dict[str, str]:
    path = ROOT / "data/quarantine_sessions.json"
    data = json.loads(path.read_text())
    return {row["date"]: row.get("reason") or "quarantine" for row in data["sessions"]}


def script_for(workflow: str, path: str) -> str:
    w = (workflow or "").lower()
    if path.endswith("_ab_checklist.csv"):
        return "src/ab_checklist.py"
    if "enriched" in path or path.endswith("_peer_rs.csv"):
        return "src/ab_enrich.py"
    if path.endswith("_actions.json"):
        return "src/news_actions.py"
    if path.endswith("_judge.json"):
        return "src/run_news_judge.py"
    if path.endswith("_parsed.json"):
        return "src/news_parse.py"
    if "/sectors/" in path:
        return "src/run_sector_predict.py"
    if path.endswith("_predict.md"):
        return "src/run_preopen_all.py"
    if path.endswith("_weather.json"):
        return "src/weather.py"
    if path.endswith("_research_baseline.json") or path.endswith("_research.json"):
        return "src/map_heat_postclose.py"
    if path.endswith("_map_heat.json"):
        if "map heat" in w or "post-close" in w:
            return "src/map_heat_postclose.py"
        return "src/map_heat.py"
    if path.endswith("_dossiers.json"):
        return "src/catalyst_daily.py"
    if path.endswith("_events.json"):
        return "src/run_events.py"
    if "/exports/finviz_" in path:
        return "collectors/finviz_financials.py"
    if path.endswith("_finviz_market_digest.json"):
        return "src/finviz_market_digest.py"
    if path.endswith("_finviz_digest.json"):
        return "src/finviz_digest.py"
    if path.endswith("_ranked.csv") or path.endswith("_membership.csv") or path.endswith("_segment_stats.csv"):
        return "src/join.py"
    if "/stock_book/" in path:
        return "src/run_stock_book_all.py"
    if "quote_colors" in path:
        return "src/quote_colors.py"
    if path.startswith("excel_bot/"):
        return "excel_bot (excel_bot.yml)"
    return ""


def coverage_of(path: str, data: bytes, day: str) -> tuple[str, str]:
    if not data or len(data.strip()) < 20:
        return "no", "empty"
    day_b = day.encode()
    if path.endswith(".csv") or path.endswith("suggestions.csv"):
        text = data.decode("utf-8", "replace")
        reader = csv.reader(io.StringIO(text))
        try:
            header = next(reader)
        except StopIteration:
            return "no", "empty"
        idx = {name.strip().strip('"'): i for i, name in enumerate(header)}
        rows = 0
        matched = 0
        asof_cols = [c for c in ("asof_date", "run_date", "signal_date") if c in idx]
        for row in reader:
            if not any(cell.strip() for cell in row):
                continue
            rows += 1
            if any(i < len(row) and row[i][:10] == day for i in (idx[c] for c in asof_cols)):
                matched += 1
        if asof_cols:
            if matched == 0:
                return "no", f"as-of column has 0 rows dated {day} ({rows} data rows)"
            return "yes", f"as-of rows={matched} of {rows}"
        if day_b in data and rows > 0:
            return "yes", f"session date in body, rows={rows}"
        if rows > 0:
            return "no", f"rows={rows} but session date is not in the body"
        return "no", "empty"
    if path.endswith(".json"):
        if day_b not in data:
            return "no", "session date is not in the json"
        # Prefer an explicit date field when the file is small enough to parse.
        if len(data) <= 2_000_000:
            try:
                obj = json.loads(data.decode("utf-8"))
            except json.JSONDecodeError:
                obj = None
            if isinstance(obj, dict):
                meta = obj.get("meta") if isinstance(obj.get("meta"), dict) else {}
                for value in (obj.get("date"), obj.get("scan_date"), meta.get("date")):
                    if isinstance(value, str) and value[:10] not in ("", day) and value[:10] != day:
                        if day not in str(value):
                            return "no", f"date field is {value[:10]}"
                    if isinstance(value, str) and value[:10] == day:
                        return "yes", f"date field {value[:10]}"
        return "yes", "session date in body"
    if path.endswith(".md"):
        if day_b in data:
            return "yes", "session date in markdown"
        return "no", "session date is not in the markdown"
    if day_b in data:
        return "yes", "session date in body"
    return "no", "session date is not in the body"


def row_index(path: str, data: bytes, day: str) -> dict[str, str]:
    if not data or not (path.endswith(".csv") or path.endswith("suggestions.csv")):
        return {}
    text = data.decode("utf-8", "replace")
    reader = csv.reader(io.StringIO(text))
    try:
        header = next(reader)
    except StopIteration:
        return {}
    idx = {name.strip().strip('"'): i for i, name in enumerate(header)}
    date_cols = [idx[c] for c in ("asof_date", "run_date", "signal_date") if c in idx]
    tcol = idx.get("Ticker", idx.get("ticker"))
    shared = path.endswith("suggestions.csv")
    out: dict[str, str] = {}
    for row in reader:
        if shared and date_cols:
            if not any(i < len(row) and row[i][:10] == day for i in date_cols):
                continue
        if tcol is not None and tcol < len(row) and row[tcol].strip():
            key = row[tcol].strip()
            if shared:
                bits = []
                for col in ("side", "strategy", "signal_date"):
                    if col in idx and idx[col] < len(row):
                        bits.append(row[idx[col]])
                key = key + "|" + "|".join(bits)
        else:
            key = "|".join(row[:3])[:100]
        digest = hashlib.sha1("\t".join(row).encode("utf-8", "replace")).hexdigest()[:12]
        out[key] = digest
    return out


def json_tickers(data: bytes) -> set[str] | None:
    if not data or len(data) > 2_000_000 or not data.lstrip().startswith(b"{"):
        return None
    try:
        obj = json.loads(data.decode("utf-8"))
    except json.JSONDecodeError:
        return None
    found: set[str] = set()

    def walk(node) -> None:
        if isinstance(node, dict):
            ticker = node.get("ticker")
            if isinstance(ticker, str) and ticker.strip():
                found.add(ticker.strip())
            for value in node.values():
                walk(value)
        elif isinstance(node, list):
            for item in node:
                walk(item)

    walk(obj)
    return found


def late_rows(path: str, pre: bytes, head: bytes, day: str) -> str:
    if pre == head:
        return ""
    if path.endswith(".csv") or path.endswith("suggestions.csv"):
        before = row_index(path, pre, day)
        after = row_index(path, head, day)
        if not before and not after:
            return "body_differs"
        added = [key for key in after if key not in before]
        changed = [key for key in after if key in before and after[key] != before[key]]
        parts = []
        if added:
            show = ",".join(added[:25])
            extra = f" (+{len(added) - 25} more)" if len(added) > 25 else ""
            parts.append(f"added={len(added)}:{show}{extra}")
        if changed:
            show = ",".join(changed[:15])
            extra = f" (+{len(changed) - 15} more)" if len(changed) > 15 else ""
            parts.append(f"changed={len(changed)}:{show}{extra}")
        return "; ".join(parts) or "body_differs"
    pre_t = json_tickers(pre)
    head_t = json_tickers(head)
    if pre_t is not None and head_t is not None:
        added = sorted(head_t - pre_t)
        if not added and pre != head:
            return "body_differs same_tickers"
        show = ",".join(added[:25])
        extra = f" (+{len(added) - 25} more)" if len(added) > 25 else ""
        return f"added={len(added)}:{show}{extra}" if added else ""
    return "body_differs"


def sector_role(path: str) -> str:
    name = path.rsplit("/", 1)[-1]
    if name.startswith("_BOARD") or name.startswith("_board"):
        return "sector_board"
    if name.startswith("_qc"):
        return "sector_qc"
    if name.endswith("_predict.md"):
        return "sector_predict"
    if name.endswith("_outcome.md"):
        return "sector_outcome"
    if name.endswith("_reflect.md"):
        return "sector_reflect"
    return "sector_other"


def blank_row(day: str, role: str, path: str, stale: str) -> dict:
    return {
        "date": day,
        "input": role,
        "path": path,
        "blob_sha": "",
        "first_server_time_utc": "",
        "server_time_utc": "",
        "server_kind": "",
        "run_id": "",
        "run_start_utc": "",
        "run_finish_utc": "",
        "before_0930": "no",
        "coverage": "n/a",
        "coverage_note": "",
        "later_modified": "no",
        "later_shas": "",
        "late_rows": "",
        "generator_workflow": "",
        "generator_script": "",
        "generator_commit": "",
        "stale_content": stale,
        "byte_identical_to_head": "n/a",
        "code_selected_sha_committer_clock_not_proof": "",
        "code_selected_matches": "n/a",
        "proven": "no",
        "status": "MISSING",
        "candidate_from_per_day_files": "no",
    }


def eval_path(day: str, role: str, path: str, history, proofs, head_blobs,
              blobs: BlobStore, quarantine: dict[str, str]) -> dict:
    stale = "n/a"
    if role in HEADLINE:
        stale = "yes" if day in quarantine else "no"
    versions = history.get(path) or []
    if not versions:
        row = blank_row(day, role, path, stale)
        row["coverage_note"] = "path never committed on main"
        return row
    cutoff = cutoff_for(day)
    timed = []
    for item in versions:
        proof = proofs.get(item["commit"])
        timed.append((item, proof))
    proven_before = [(item, proof) for item, proof in timed
                     if proof and proof["time"] <= cutoff]
    any_proof = [(item, proof) for item, proof in timed if proof]
    first = min(any_proof, key=lambda pair: pair[1]["time"]) if any_proof else None
    chosen = proven_before[-1] if proven_before else None
    # Committer-clock pick used by materialize(). Not a server proof.
    code = None
    code_ts = None
    for item, _proof in timed:
        stamp = parse_ts(item["committer"])
        if stamp and stamp <= cutoff and (code_ts is None or stamp >= code_ts):
            code = item
            code_ts = stamp
    head_blob = head_blobs.get(path, "")
    row = blank_row(day, role, path, stale)
    if first:
        row["first_server_time_utc"] = first[1]["time"].strftime("%Y-%m-%dT%H:%M:%SZ")
    if chosen is None:
        row["status"] = "NOT_PROVEN"
        row["coverage"] = "n/a"
        row["coverage_note"] = "no Actions log pushed a commit of this file before 13:30 UTC"
        if first:
            proof = first[1]
            row["server_time_utc"] = proof["time"].strftime("%Y-%m-%dT%H:%M:%SZ")
            row["server_kind"] = proof["kind"]
            row["run_id"] = proof["run_id"]
            row["run_start_utc"] = proof["run_start"]
            row["run_finish_utc"] = proof["run_finish"]
            row["generator_workflow"] = proof["workflow"]
            row["generator_script"] = script_for(proof["workflow"], path)
            row["generator_commit"] = proof["head_sha"]
            row["blob_sha"] = first[0]["blob"]
        if code:
            row["code_selected_sha_committer_clock_not_proof"] = code["commit"]
        return row
    item, proof = chosen
    pre_blob = item["blob"]
    data = blobs.get(pre_blob)
    cov, note = coverage_of(path, data, day)
    later = []
    seen_chosen = False
    for ver, ver_proof in timed:
        if not seen_chosen:
            if ver["commit"] == item["commit"] and ver["blob"] == pre_blob:
                seen_chosen = True
            continue
        stamp = ""
        if ver_proof:
            stamp = ver_proof["time"].strftime("%Y-%m-%dT%H:%M:%SZ")
        else:
            stamp = "no-log-proof"
        later.append(f"{ver['commit'][:12]}@{stamp}")
    head_data = blobs.get(head_blob) if head_blob else b""
    differ = bool(head_blob) and head_blob != pre_blob
    late = late_rows(path, data, head_data, day) if differ else ""
    # suggestions.csv is cumulative. Rows for other days do not change this morning.
    day_changed = bool(late) if path.endswith("suggestions.csv") else differ
    row.update({
        "blob_sha": pre_blob,
        "server_time_utc": proof["time"].strftime("%Y-%m-%dT%H:%M:%SZ"),
        "server_kind": proof["kind"],
        "run_id": proof["run_id"],
        "run_start_utc": proof["run_start"],
        "run_finish_utc": proof["run_finish"],
        "before_0930": "yes",
        "coverage": cov,
        "coverage_note": note,
        "later_modified": "yes" if day_changed else "no",
        "later_shas": ";".join(later[:12]),
        "late_rows": late,
        "generator_workflow": proof["workflow"],
        "generator_script": script_for(proof["workflow"], path),
        "generator_commit": proof["head_sha"],
        "byte_identical_to_head": "yes" if head_blob == pre_blob else "no",
        "code_selected_sha_committer_clock_not_proof": code["commit"] if code else "",
        "code_selected_matches": "yes" if code and code["blob"] == pre_blob else "no",
    })
    if cov != "yes":
        row["status"] = "NO_COVERAGE"
        row["proven"] = "no"
    elif stale == "yes":
        row["status"] = "STALE"
        row["proven"] = "no"
    elif day_changed:
        row["status"] = "PROVEN_BUT_CHANGED"
        row["proven"] = "yes"
    else:
        row["status"] = "PROVEN"
        row["proven"] = "yes"
    return row


def parse_score(data: bytes) -> float | None:
    text = data.decode("utf-8", "replace")
    match = SCORE_RE.search(text)
    if match:
        try:
            return float(match.group(2))
        except ValueError:
            return None
    weather = re.search(r'"general_score"\s*:\s*(-?[\d.]+)', text)
    if weather:
        try:
            return float(weather.group(1))
        except ValueError:
            return None
    return None


def derived_row(day: str, role: str, base: dict | None, blobs: BlobStore,
                fallback: dict | None = None) -> dict:
    source = base
    note_prefix = "S from predict.md"
    if (not base or base.get("status") == "MISSING") and fallback:
        source = fallback
        note_prefix = "S from weather general_score (predict.md missing)"
    if not source:
        row = blank_row(day, role, f"01_daily/general/{day}_predict.md", "n/a")
        row["coverage_note"] = "no predict.md and no weather.json"
        return row
    row = dict(source)
    row["input"] = role
    row["stale_content"] = "n/a"
    if source.get("blob_sha") and source.get("before_0930") == "yes":
        score = parse_score(blobs.get(source["blob_sha"]))
    else:
        score = None
    if score is None:
        if source.get("status") == "MISSING":
            row["status"] = "MISSING"
        elif source.get("before_0930") != "yes":
            row["status"] = "NOT_PROVEN"
        else:
            row["status"] = "NO_COVERAGE"
            row["coverage"] = "no"
        row["proven"] = "no"
        row["coverage_note"] = note_prefix + "; score not in the pre-open blob"
        return row
    fired = "yes" if score <= -3.0 else "no"
    row["coverage"] = "yes"
    row["coverage_note"] = f"{note_prefix}; score={score}; hard_red_fired={fired}"
    if source.get("stale_content") == "yes":
        # Weather and predict are not headline inputs. Keep the base status.
        pass
    if source.get("proven") == "yes":
        row["proven"] = "yes"
        row["status"] = source["status"]
    else:
        row["proven"] = "no"
        row["status"] = source["status"]
    if role == "hard_red":
        row["coverage_note"] += "; threshold S<=-3; derived from the same file, not a per-day hard_red_exceptions blob"
    return row


def catalog(history_paths: set[str]) -> dict[str, list[tuple[str, str]]]:
    by_day: dict[str, list[tuple[str, str]]] = {day: [] for day in SESSIONS}
    for day in SESSIONS:
        seen = set()
        for role, tmpl in TEMPLATES:
            path = tmpl.format(d=day)
            by_day[day].append((role, path))
            seen.add(path)
        sector_paths = sorted(
            p for p in history_paths
            if p.startswith(f"01_daily/sectors/{day}/") and "trace" not in p.lower()
        )
        if not sector_paths:
            by_day[day].append(("sector_predict", f"01_daily/sectors/{day}/"))
        for path in sector_paths:
            if path in seen:
                continue
            by_day[day].append((sector_role(path), path))
    return by_day


# Owner proof: session N uses the prior trading day's signal_date. Only the
# pre-open rows count. These three tickers landed after 13:30 UTC.
EXCEL_LATE_ROWS = {
    "2026-09-02": "added=2:CMII/L1_long_green_tp8_lowvol,CMII/L2_long_green_tp3_lowvol",
    "2026-09-04": "added=2:AUBN/L1_long_green_tp8_lowvol,AUBN/L2_long_green_tp3_lowvol",
    "2026-09-11": "added=2:SVCC/L1_long_green_tp8_lowvol,SVCC/L2_long_green_tp3_lowvol",
}
EXCEL_LATE_SHA = {
    "2026-09-02": "1b3ec8c8",
    "2026-09-04": "fb22c0b5",
    "2026-09-11": "87aeb549",
}


def git_blob(commit: str, path: str) -> str:
    if not commit or not path or path.startswith("("):
        return ""
    proc = subprocess.run(
        ["git", "rev-parse", "--verify", f"{commit}:{path}"],
        cwd=ROOT, capture_output=True, text=True, check=False,
    )
    if proc.returncode != 0:
        return ""
    return proc.stdout.strip()


def resolve_commit(prefix: str) -> str:
    prefix = (prefix or "").strip()
    if not prefix:
        return ""
    proc = subprocess.run(
        ["git", "rev-parse", "--verify", f"{prefix}^{{commit}}"],
        cwd=ROOT, capture_output=True, text=True, check=False,
    )
    if proc.returncode != 0:
        return ""
    return proc.stdout.strip()


def apply_excel_owner_proof(rows: list[dict]) -> None:
    """Replace excel_daily and excel_suggestions with the owner proof table.

    A session counts only when excel_preopen_proof.csv says PROVEN or
    PROVEN_BUT_CHANGED. MISSING owner rows (no bot yet, or zero signals)
    are NOT_PROVEN. The owner column proven=True on a zero-row day is an
    earlier suggestions blob, not this session's signals.
    """
    if not EXCEL_PROOF.exists():
        raise SystemExit(f"missing {EXCEL_PROOF}")
    by_day: dict[str, dict] = {}
    with EXCEL_PROOF.open(encoding="utf-8", newline="") as fh:
        for rec in csv.DictReader(fh):
            by_day[rec["session_date"]] = rec
    head_ref = "origin/main"
    for row in rows:
        if row["input"] not in ("excel_daily", "excel_suggestions"):
            continue
        owner = by_day.get(row["date"])
        if owner is None:
            row["status"] = "NOT_PROVEN"
            row["proven"] = "no"
            row["before_0930"] = "no"
            row["coverage"] = "no"
            row["coverage_note"] = "no owner row in excel_preopen_proof.csv"
            row["stale_content"] = "n/a"
            row["candidate_from_per_day_files"] = "no"
            continue
        status = (owner.get("status") or "").strip()
        counts = status in ("PROVEN", "PROVEN_BUT_CHANGED")
        try:
            n_pre = int(owner.get("n_signal_rows_preopen") or 0)
        except ValueError:
            n_pre = 0
        match = EXCEL_RUN.search(owner.get("server_proof_kind") or "")
        commit = resolve_commit(owner.get("preopen_commit") or "")
        note = (owner.get("diff_note") or "").strip()
        if row["input"] == "excel_suggestions":
            path = "excel_bot/suggestions/suggestions.csv"
            blob = git_blob(commit, path) if counts and commit else ""
            head_blob = git_blob(head_ref, path)
            identical = "yes" if blob and blob == head_blob else ("no" if blob else "n/a")
        else:
            files = [
                part.strip()
                for part in (owner.get("signal_file") or "").split(";")
                if part.strip() and part.strip() != "(none)"
            ]
            if counts and files:
                path = ";".join(files)
                blobs = [git_blob(commit, part) for part in files]
                blob = ";".join(item for item in blobs if item)
                identical = "yes" if blobs and all(
                    item and item == git_blob(head_ref, part)
                    for item, part in zip(blobs, files)
                ) else "no"
            else:
                path = "(none)"
                blob = ""
                identical = "n/a"
        row["path"] = path
        row["blob_sha"] = blob
        server = (owner.get("server_time_utc") or "").strip()
        row["first_server_time_utc"] = server
        row["server_time_utc"] = server
        row["server_kind"] = "log_line" if server else ""
        row["run_id"] = match.group(1) if match else ""
        row["run_start_utc"] = match.group(4) if match else ""
        row["run_finish_utc"] = match.group(5) if match else ""
        row["generator_workflow"] = (
            "Excel Bot (cluster signals daily)" if match else ""
        )
        row["generator_script"] = "excel_bot (excel_bot.yml)" if match else ""
        row["generator_commit"] = ""
        row["stale_content"] = "n/a"
        row["byte_identical_to_head"] = identical
        row["coverage_note"] = note
        row["candidate_from_per_day_files"] = "no"
        if counts and n_pre > 0:
            row["status"] = status
            row["proven"] = "yes"
            row["before_0930"] = "yes"
            row["coverage"] = "yes"
            if status == "PROVEN_BUT_CHANGED":
                row["later_modified"] = "yes"
                row["late_rows"] = EXCEL_LATE_ROWS.get(row["date"], "")
                row["later_shas"] = EXCEL_LATE_SHA.get(row["date"], "")
            else:
                row["later_modified"] = "no"
                row["late_rows"] = ""
                row["later_shas"] = ""
        else:
            row["status"] = "NOT_PROVEN"
            row["proven"] = "no"
            row["before_0930"] = "no"
            row["coverage"] = "no"
            row["later_modified"] = "no"
            row["late_rows"] = ""
            row["later_shas"] = ""
            row["blob_sha"] = ""
            row["byte_identical_to_head"] = "n/a"


def day_proven(rows: list[dict], role: str) -> bool:
    group = [row for row in rows if row["input"] == role]
    if not group:
        return False
    if any(row["path"].endswith("/") for row in group):
        return False
    return all(row["proven"] == "yes" for row in group)


def render_md(rows: list[dict], stats: dict, quarantine: dict[str, str]) -> str:
    roles = []
    for role, _tmpl in TEMPLATES:
        roles.append(role)
    roles.extend(["S", "hard_red", "sector_predict", "sector_outcome",
                  "sector_reflect", "sector_board", "sector_qc", "sector_other"])
    by_day: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        by_day[row["date"]].append(row)
    lines = [
        "# Fullscan per-file pre-open proof",
        "",
        "Sessions 2026-08-13 through 2026-09-25 (31 days). A file is PROVEN only when an Actions job log shows a run pushed that commit to main (`[safe-push] pushed <sha>` or `old..new -> main`) at or before 13:30 UTC that day, the blob contains that morning's date, and headline inputs are not on the #331 quarantine list. Committer timestamps are not proof. A commit in no run's range is not proven. `PROVEN_BUT_CHANGED` still counts: only the pre-open blob counts, and `late_rows` lists what landed later.",
        "",
        f"Log lines read: {stats['hits']}. Distinct commits timed: {stats['commits']}. Unresolved short SHAs or lines with no timestamp: {stats['unresolved']}.",
        "",
        "The day's full candidate list (ohlc_hot, probable, hot_score) cannot be rebuilt from these per-day files on any session. Those buckets read `data/prices/ohlc.parquet`, which is not a per-day file. `candidate_from_per_day_files` is `no` on every row.",
        "",
        "S is the predict.md score (`Prediction: … total score`). When predict.md was never committed, S falls back to `weather.json` `general_score`. Hard-red is that score at or below -3. `data/hard_red_exceptions/latest.json` is not a per-day file and is not given a fake day proof. Sector `*trace*` files are omitted.",
        "",
        f"Headline inputs ({', '.join(sorted(HEADLINE))}) have `stale_content=yes` on the {len(quarantine)} quarantine sessions (7 stale-dated, 11 undated Finviz). Those rows count only when `before_0930=yes` and `stale_content=no`.",
        "",
        "Excel rows use the owner table `research/audit/excel_preopen_proof.csv`: session N reads `suggestions.csv` rows whose `signal_date` is the prior trading day, and the `excel_bot.yml` run must have finished before 13:30 UTC. A same-day filename is not the session key.",
        "",
        "| input | proven days | total |",
        "|---|---:|---:|",
    ]
    seen = set()
    ordered = []
    for role in roles:
        if role not in seen:
            ordered.append(role)
            seen.add(role)
    for role in ordered:
        if not any(row["input"] == role for row in rows):
            continue
        proven_days = sum(1 for day in SESSIONS if day_proven(by_day[day], role))
        lines.append(f"| {role} | {proven_days} | {len(SESSIONS)} |")
    lines += [
        "",
        "Join, peers, universe membership, and segment stats have ranked rows and a dated filename, and the body does not contain the session date. `green.json` is the same: a pre-open copy on some days, with no session date in the json. Coverage fails for those, so they are not PROVEN.",
        "",
        "Excel, owner proof, these 31 sessions: 14 PROVEN (08-31, 09-03, 09-08, 09-10, 09-14 through 09-18, 09-21 through 09-25) and 3 PROVEN_BUT_CHANGED, where only the pre-open rows count (09-02 drop CMII, 09-04 drop AUBN, 09-11 drop SVCC). Not proven: 08-13 through 08-28, 09-01, and 09-09. GitHub API spot-check agreed. Run 33322096008 finished 2026-08-30T16:22:25Z and its job log says `[safe-push] pushed 2cc2571` (commit `2cc2571f494e`), before the 08-31 open. Run 34490029194 finished 2026-09-10T15:22:39Z and pushed `4dc97fcb`, before the 09-11 open; SVCC arrived in run 34610907074, which finished 2026-09-11T15:22:17Z, after that open. Run 34240092081 (09-08) was cancelled and left no 09-09 signals.",
        "",
        "Theme Radar landed [research/lever_panel/server_time_proof.csv](https://github.com/SRoyaltyy/theme-radar/blob/19973230e4d80c74565e1246ada911503a808fb7/research/lever_panel/server_time_proof.csv) at `19973230e4d80c74565e1246ada911503a808fb7` (2026-09-26T03:56:51Z). All 234 rows are `PROVEN` from that repo's Actions logs. There is no separate \"not proven frozen\" file next to the lever panel. That table is Theme Radar's Finviz lever panel, not this repo's `panel.json`.",
        "",
        "`code_selected_sha_committer_clock_not_proof` is the commit `materialize()` would pick with committer time. It is not a server time. `code_selected_matches=yes` means that blob is the log-proven pre-open blob.",
        "",
        "## Panel rows before the open",
        "",
        "The sequential walk reads `data/factor_mine/snapshots/{date}.json`, not `panel.json`. Every snapshot file and `data/factor_mine/retro_prices/ohlc.parquet` share one commit, `5f13a4415ea0` (merge #336). That commit is not in any harvested run's push range, so no snapshot row has a log-proven pre-open copy. **0 of 31** sessions have a pre-open panel row.",
        "",
        "`panel.json` does have Factor strategy mine log lines for 16 of 22 commits. None of those pushes is at or before 13:30 UTC on the session whose rows it adds. On 09-15 through 09-25 a prior night's blob is already on main before the open, and that blob's last session is the previous day (`rows_that_day=0`). Six `panel.json` commits have no log line, including the first appearance `f5b46d20eec2` (merge #172). The snapshot commit `5f13a4415ea0` is a separate merge and is also outside every harvested push range. A push-triggered Pages run whose `head_sha` is that commit is not a push range, so it is not proof.",
        "",
        "## Rebuild match and HOT4 / holdup",
        "",
        "Rebuild match is unchanged from `INPUT_PROVENANCE_336.md`. Today's Part A does not reproduce the server-proven pre-open A1-A15 files (0/3 days: 09-21, 09-22, 09-24). Price-list outputs and price features have no pre-open artifact of their own. Snapshot comparisons are not a license to rebuild earlier days.",
        "",
        "HOT4 (`union_hot_n4_h1`) and holdup (`union_hot_n4_holdup`) need yday_gainer, yday_mover, ohlc_hot, overnight, probable, earn_react, price features, alarm, flatten, and mover_buy together. Those outputs are not proven per-day files. The price store is not a per-day file. Alarm, flatten, and mover_buy have no pre-open list. Headline inputs on the 18 quarantine sessions do not count. **No session** has every component proven under this rule, with GLND kept or removed, so there is no return.",
        "",
    ]
    return "\n".join(lines) + "\n"


def main() -> None:
    if not HITS.exists():
        raise SystemExit(f"missing {HITS}; run harvest_push_logs.py first")
    print("loading parents", flush=True)
    parents = load_parents()
    by7, by8 = load_indexes(parents)
    print(f"commits {len(parents)}", flush=True)
    proofs, stats = load_proofs(parents, by7, by8)
    print(f"proofs {stats}", flush=True)
    history = load_history()
    head_blobs = load_head_blobs()
    quarantine = load_quarantine()
    print(f"paths {len(history)} quarantine {len(quarantine)}", flush=True)
    blobs = BlobStore()
    rows: list[dict] = []
    try:
        plan = catalog(set(history) | set(head_blobs))
        for day in SESSIONS:
            for role, path in plan[day]:
                rows.append(eval_path(day, role, path, history, proofs, head_blobs, blobs, quarantine))
            by_role = {row["input"]: row for row in rows if row["date"] == day and row["input"] in ("predict", "weather")}
            rows.append(derived_row(day, "S", by_role.get("predict"), blobs, by_role.get("weather")))
            rows.append(derived_row(day, "hard_red", by_role.get("predict"), blobs, by_role.get("weather")))
            print(f"day {day} rows {len(rows)}", flush=True)
    finally:
        blobs.close()
    apply_excel_owner_proof(rows)
    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    with OUT_CSV.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=COLUMNS, lineterminator="\n")
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in COLUMNS})
    OUT_MD.write_text(render_md(rows, stats, quarantine), encoding="utf-8")
    proven = sum(1 for row in rows if row["proven"] == "yes")
    print(f"wrote {OUT_CSV} rows {len(rows)} proven_rows {proven}", flush=True)
    print(f"wrote {OUT_MD}", flush=True)


if __name__ == "__main__":
    main()
