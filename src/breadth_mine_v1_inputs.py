"""Earliest committed bytes for each breadth_mine_v1 input.

A file labelled for day D is the first git commit of that path.
panel.json is one file for many dates: the earliest commit whose blob
contains rows labelled D. Later rewrites are not read.
"""
from __future__ import annotations

import json
import subprocess
from collections import Counter
from pathlib import Path

from src.breadth_mine_v1_protocol import (
    BAR_BLOB_SHA,
    BAR_COMMIT,
    BAR_PATH,
    BAR_SHA256,
    MANIFEST_PATH,
    SESSIONS,
    STUDY,
    STUDY_LABEL,
)

ROOT = Path(__file__).resolve().parents[1]

DAY_PATHS = {
    "stock_book": "data/stock_book/{d}_stock_book.json",
    "actions": "01_daily/news/{d}_actions.json",
    "ab_checklist": "data/ab_checklist/{d}_ab_checklist.csv",
    "ab_enriched": "data/ab_checklist/{d}_ab_checklist_enriched.csv",
    "export": "data/exports/finviz_{d}.csv",
    "catalyst": "01_daily/catalyst/{d}_dossiers.json",
    "judge": "01_daily/news/{d}_judge.json",
    "heat": "01_daily/map_heat/{d}_map_heat.json",
    "predict": "01_daily/general/{d}_predict.md",
}


def _run(args: list[str]) -> subprocess.CompletedProcess:
    return subprocess.run(args, cwd=ROOT, check=False, capture_output=True, text=True)


def path_exists(path: str) -> bool:
    proc = _run(["git", "cat-file", "-e", f"HEAD:{path}"])
    return proc.returncode == 0


def earliest_commit(path: str) -> str | None:
    proc = _run(["git", "log", "--diff-filter=A", "--reverse", "--pretty=%H", "--", path])
    if proc.returncode != 0:
        return None
    line = proc.stdout.splitlines()
    return line[0].strip() if line else None


def blob_at(commit: str, path: str) -> str:
    proc = _run(["git", "rev-parse", f"{commit}:{path}"])
    if proc.returncode != 0:
        raise SystemExit(f"blob missing {commit}:{path}")
    return proc.stdout.strip()


def git_blob(sha: str) -> bytes:
    return subprocess.check_output(["git", "cat-file", "-p", sha], cwd=ROOT)


def _panel_dates(blob: bytes) -> dict[str, int]:
    data = json.loads(blob)
    counts: Counter[str] = Counter()
    for row in data.get("rows") or []:
        if isinstance(row, dict) and row.get("date"):
            counts[str(row["date"])] += 1
    return dict(counts)


def panel_rows() -> list[dict]:
    proc = _run(["git", "rev-list", "--reverse", "HEAD", "--", "data/factor_mine/panel.json"])
    if proc.returncode != 0:
        raise SystemExit("panel history missing")
    commits = [line.strip() for line in proc.stdout.splitlines() if line.strip()]
    found: dict[str, dict] = {}
    seen_blobs: dict[str, dict[str, int]] = {}
    for commit in commits:
        if len(found) == len(SESSIONS):
            break
        blob = blob_at(commit, "data/factor_mine/panel.json")
        counts = seen_blobs.get(blob)
        if counts is None:
            counts = _panel_dates(git_blob(blob))
            seen_blobs[blob] = counts
        for day in SESSIONS:
            if day in found or day not in counts or counts[day] < 1:
                continue
            found[day] = {
                "blob_sha": blob,
                "commit": commit,
                "date": day,
                "input": "panel",
                "n_rows": counts[day],
                "path": "data/factor_mine/panel.json",
            }
    return [found[day] for day in SESSIONS if day in found]


def day_file_rows() -> list[dict]:
    rows = []
    for day in SESSIONS:
        for kind, pattern in DAY_PATHS.items():
            path = pattern.format(d=day)
            if not path_exists(path):
                continue
            commit = earliest_commit(path)
            if commit is None:
                continue
            rows.append({
                "blob_sha": blob_at(commit, path),
                "commit": commit,
                "date": day,
                "input": kind,
                "path": path,
            })
    proc = _run(["git", "ls-tree", "-r", "--name-only", "HEAD", "01_daily/sectors"])
    if proc.returncode != 0:
        return rows
    for path in proc.stdout.splitlines():
        if not path.endswith("_predict.md") or path.endswith("_predict_trace.md"):
            continue
        parts = path.split("/")
        if len(parts) < 4:
            continue
        day = parts[2]
        if day not in SESSIONS:
            continue
        if not path_exists(path):
            continue
        commit = earliest_commit(path)
        if commit is None:
            continue
        rows.append({
            "blob_sha": blob_at(commit, path),
            "commit": commit,
            "date": day,
            "input": "sector",
            "path": path,
        })
    return rows


def build_manifest() -> dict:
    rows = panel_rows() + day_file_rows()
    rows.sort(key=lambda row: (row["date"], row["input"], row["path"]))
    return {
        "bars": {
            "blob_sha": BAR_BLOB_SHA,
            "commit": BAR_COMMIT,
            "path": BAR_PATH,
            "sha256": BAR_SHA256,
        },
        "inputs": rows,
        "label": STUDY_LABEL,
        "study": STUDY,
    }


def write_manifest() -> Path:
    payload = build_manifest()
    raw = (json.dumps(payload, indent=2, sort_keys=True) + "\n").encode("utf-8")
    MANIFEST_PATH.parent.mkdir(parents=True, exist_ok=True)
    MANIFEST_PATH.write_bytes(raw)
    return MANIFEST_PATH


def main() -> None:
    path = write_manifest()
    data = json.loads(path.read_text(encoding="utf-8"))
    kinds: Counter[str] = Counter(row["input"] for row in data["inputs"])
    print(f"wrote {path} rows {len(data['inputs'])} {dict(kinds)}", flush=True)


if __name__ == "__main__":
    main()
