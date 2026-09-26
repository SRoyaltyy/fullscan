"""Earliest committed bytes for each breadth_rank_v1 input.

A file labelled for day D is the first git commit of that path.
panel.json is one file for many dates: the earliest commit whose blob
contains rows labelled D. Later rewrites are not read.
"""
from __future__ import annotations

import csv
import io
import json
import math
import re
import subprocess
from collections import Counter
from pathlib import Path

from src.breadth_rank_v1_protocol import (
    BAR_BLOB_SHA,
    BAR_PATH,
    BAR_SHA256,
    DROPPED_SHA256,
    MANIFEST_PATH,
    SESSIONS,
    SPLIT_SHA256,
    STUDY,
    STUDY_LABEL,
)

ROOT = Path(__file__).resolve().parents[1]
SCORE_RE = re.compile(r"^-\s*total_score:\s*\*\*([+-]?[\d.]+)")
DIR_RE = re.compile(r"^-\s*predicted_direction:\s*\*\*(.+?)\*\*", re.I)
SECTOR_ALIAS = {
    "financial": "financial",
    "financials": "financial",
    "financial_services": "financial",
}
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


def num(raw) -> float | None:
    if raw is None:
        return None
    text = str(raw).strip().replace("%", "").replace(",", "")
    if text in ("", "-", "—", "None", "nan", "NaN"):
        return None
    try:
        value = float(text)
    except ValueError:
        return None
    if not math.isfinite(value):
        return None
    return value


def parse_predict(text: str) -> tuple[float | None, str | None]:
    score = None
    direction = None
    for line in text.splitlines():
        line = line.strip()
        matched = SCORE_RE.match(line)
        if matched:
            score = float(matched.group(1))
            continue
        matched = DIR_RE.match(line)
        if matched:
            direction = matched.group(1).strip().lower()
    return score, direction


def sector_slug(name: str) -> str:
    text = " ".join(str(name or "").strip().lower().replace("&", " and ").split())
    slug = text.replace(" ", "_")
    return SECTOR_ALIAS.get(slug, slug)


def actions_map(blob: bytes) -> tuple[dict[str, float], set[str]]:
    data = json.loads(blob)
    items = data.get("ticker_actions") or []
    if isinstance(items, dict):
        seq = []
        for ticker, row in items.items():
            if isinstance(row, dict):
                seq.append({"ticker": ticker, **row})
        items = seq
    scores: dict[str, float] = {}
    named: set[str] = set()
    for row in items:
        if not isinstance(row, dict):
            continue
        ticker = str(row.get("ticker") or "").strip().upper()
        if not ticker:
            continue
        named.add(ticker)
        side = str(row.get("side") or "").lower()
        try:
            net = float(row.get("net") or 0.0)
        except (TypeError, ValueError):
            net = 0.0
        if side in ("sell", "short"):
            signed = -abs(net) if net else -1.0
        elif side in ("buy", "long"):
            signed = abs(net) if net else 1.0
        else:
            signed = net
        scores.setdefault(ticker, signed)
    return scores, named


def ab_map(checklist: bytes, enriched: bytes) -> dict[str, float]:
    def scores(blob: bytes) -> dict[str, float]:
        out: dict[str, float] = {}
        text = blob.decode("utf-8", errors="replace")
        for row in csv.DictReader(io.StringIO(text)):
            ticker = str(row.get("Ticker") or "").strip().upper()
            raw = row.get("score")
            if not ticker or raw in (None, ""):
                continue
            try:
                out[ticker] = float(raw)
            except ValueError:
                continue
        return out

    right = scores(enriched)
    common = set(scores(checklist)) & set(right)
    return {ticker: math.tanh(right[ticker] / 8.0) for ticker in common}


def heat_map(blob: bytes) -> tuple[dict[str, float], set[str]]:
    data = json.loads(blob)
    scores: dict[str, float] = {}
    named: set[str] = set()

    def walk(obj) -> None:
        if isinstance(obj, dict):
            if "ticker" in obj and "d1" in obj:
                ticker = str(obj.get("ticker") or "").strip().upper()
                if ticker:
                    named.add(ticker)
                    d1 = num(obj.get("d1"))
                    if d1 is not None:
                        scores.setdefault(ticker, d1)
            for value in obj.values():
                walk(value)
        elif isinstance(obj, list):
            for value in obj:
                walk(value)

    walk(data)
    return scores, named


def book_map(blob: bytes) -> tuple[set[str], set[str], dict[str, str]]:
    data = json.loads(blob)
    buys: set[str] = set()
    named: set[str] = set()
    sectors: dict[str, str] = {}
    books = data.get("books") or {}
    for book in books.values():
        if not isinstance(book, dict):
            continue
        for key in ("buy", "sell"):
            for row in book.get(key) or []:
                if not isinstance(row, dict):
                    continue
                ticker = str(row.get("ticker") or "").strip().upper()
                if not ticker:
                    continue
                named.add(ticker)
                if key == "buy":
                    buys.add(ticker)
                sector = str(row.get("sector") or "").strip()
                if sector:
                    sectors.setdefault(ticker, sector)
    return buys, named, sectors


def panel_names(blob: bytes, session: str, n_rows: int) -> list[str]:
    data = json.loads(blob)
    names = []
    for row in data.get("rows") or []:
        if isinstance(row, dict) and row.get("date") == session:
            ticker = str(row.get("ticker") or "").strip().upper()
            if ticker:
                names.append(ticker)
    if len(names) != int(n_rows):
        raise SystemExit(f"{session}: panel n_rows {len(names)} != {n_rows}")
    return names


def catalyst_names(blob: bytes) -> set[str]:
    data = json.loads(blob)
    out = set()
    for row in data.get("targets") or []:
        if isinstance(row, dict):
            ticker = str(row.get("ticker") or "").strip().upper()
            if ticker:
                out.add(ticker)
    return out


def judge_map(blob: bytes) -> tuple[dict[str, float], set[str]]:
    data = json.loads(blob)
    scores: dict[str, float] = {}
    named: set[str] = set()
    tickers = data.get("tickers") or {}
    if isinstance(tickers, dict):
        for ticker, score in tickers.items():
            name = str(ticker).strip().upper()
            if not name:
                continue
            named.add(name)
            value = num(score)
            if value is not None:
                scores[name] = value
    return scores, named


def finviz_map(blob: bytes, wanted: set[str]) -> tuple[dict[str, float], dict[str, str]]:
    import pandas as pd

    columns = {"Ticker", "Performance (Week)", "Sector"}
    frame = pd.read_csv(io.BytesIO(blob), usecols=lambda name: name in columns, low_memory=False)
    week: dict[str, float] = {}
    sectors: dict[str, str] = {}
    if "Ticker" not in frame.columns:
        return week, sectors
    ticker_i = frame.columns.get_loc("Ticker")
    week_i = frame.columns.get_loc("Performance (Week)") if "Performance (Week)" in frame.columns else None
    sector_i = frame.columns.get_loc("Sector") if "Sector" in frame.columns else None
    for row in frame.to_numpy():
        ticker = str(row[ticker_i] or "").strip().upper()
        if ticker not in wanted:
            continue
        if week_i is not None:
            parsed = num(row[week_i])
            if parsed is not None:
                week[ticker] = parsed
        if sector_i is not None:
            sector = str(row[sector_i] or "").strip()
            if sector and sector.lower() != "nan":
                sectors[ticker] = sector
    return week, sectors


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
            "dropped_sha256": DROPPED_SHA256,
            "path": BAR_PATH,
            "sha256": BAR_SHA256,
            "splits_sha256": SPLIT_SHA256,
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


def _names_from_row(row: dict, cache: dict[str, bytes]) -> set[str]:
    blob = cache.get(row["blob_sha"])
    if blob is None:
        blob = git_blob(row["blob_sha"])
        cache[row["blob_sha"]] = blob
    kind = row["input"]
    if kind == "stock_book":
        _buys, named, _sec = book_map(blob)
        return named
    if kind == "panel":
        return set(panel_names(blob, row["date"], int(row["n_rows"])))
    if kind == "catalyst":
        return catalyst_names(blob)
    if kind == "judge":
        _scores, named = judge_map(blob)
        return named
    if kind == "actions":
        _scores, named = actions_map(blob)
        return named
    if kind == "heat":
        _scores, named = heat_map(blob)
        return named
    return set()


def collect_tickers() -> list[str]:
    names = {"IWM"}
    cache: dict[str, bytes] = {}
    for row in panel_rows() + day_file_rows():
        names |= _names_from_row(row, cache)
    return sorted(names)


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--write", action="store_true")
    parser.add_argument("--tickers", action="store_true")
    args = parser.parse_args()
    if args.tickers:
        names = collect_tickers()
        print(f"tickers {len(names)}", flush=True)
        return
    path = write_manifest()
    data = json.loads(path.read_text(encoding="utf-8"))
    kinds: Counter[str] = Counter(row["input"] for row in data["inputs"])
    print(f"wrote {path} rows {len(data['inputs'])} {dict(kinds)}", flush=True)


if __name__ == "__main__":
    main()
