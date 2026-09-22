"""Slim headline ingest from SRoyaltyy/theme-radar snapshots.

Do NOT vendor the 11MB .raw.csv or merge repos. Visibility = four columns:
Ticker, News Title, Daily Digest, News Time.

Lookup order:
1. THEME_RADAR_ROOT or vendor/theme-radar (Actions checkout)
2. data/theme_radar_snapshots/ (optional slim copies)
3. raw.githubusercontent.com (public; named dates only)
"""
from __future__ import annotations

import csv
import gzip
import io
import os
import re
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
VENDOR = ROOT / "vendor" / "theme-radar" / "data" / "snapshots"
LOCAL_SLIM = ROOT / "data" / "theme_radar_snapshots"
REMOTE = (
    "https://raw.githubusercontent.com/SRoyaltyy/theme-radar/"
    "main/data/snapshots/{date}.csv"
)
_DATE = re.compile(r"(\d{4}-\d{2}-\d{2})")


def _roots() -> list[Path]:
    env = (os.environ.get("THEME_RADAR_ROOT") or "").strip()
    out: list[Path] = []
    if env:
        p = Path(env)
        out.append(p / "data" / "snapshots" if (p / "data" / "snapshots").is_dir() else p)
    out.extend([VENDOR, LOCAL_SLIM])
    return out


def _is_raw_snapshot(path: Path) -> bool:
    return ".raw.csv" in path.name


def _date_of_path(path: Path) -> str:
    m = _DATE.search(path.name)
    return m.group(1) if m else ""


def _candidates(root: Path, date: str | None) -> list[Path]:
    if not root.is_dir():
        return []
    named = bool(date and str(date).lower() not in {"all", "*", "history", ""})
    if named:
        found = []
        for name in (f"{date}.csv", f"{date}.csv.gz"):
            p = root / name
            if p.is_file() and not _is_raw_snapshot(p):
                found.append(p)
        return found
    found = list(root.glob("20??-??-??.csv"))
    found.extend(root.glob("20??-??-??.csv.gz"))
    return sorted(p for p in found if p.is_file() and not _is_raw_snapshot(p))


def snapshot_paths(date: str | None = None) -> list[Path]:
    """One file per snapshot date. Earlier roots win (env, vendor, slim pack)."""
    seen: set[str] = set()
    files: list[Path] = []
    for root in _roots():
        for path in _candidates(root, date):
            key = _date_of_path(path) or path.name
            if key in seen:
                continue
            seen.add(key)
            files.append(path)
    return files


def _read_text(path: Path) -> str:
    try:
        if path.name.endswith(".gz"):
            with gzip.open(path, "rt", encoding="utf-8", errors="replace") as fh:
                return fh.read()
        return path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return ""


def _header_map(fields: list[str] | None) -> dict[str, str]:
    out = {}
    for raw in fields or []:
        out[re.sub(r"\s+", " ", raw.strip().lower())] = raw
    return out


def _col(hmap: dict[str, str], *names: str) -> str | None:
    for n in names:
        if n in hmap:
            return hmap[n]
    for k, raw in hmap.items():
        for n in names:
            if n in k:
                return raw
    return None


def _fetch_remote(date: str) -> str:
    url = REMOTE.format(date=date)
    try:
        with urllib.request.urlopen(url, timeout=45) as r:
            return r.read().decode("utf-8", errors="replace")
    except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, OSError):
        return ""


def _rows_from_text(text: str, source_file: str, retrieved: str) -> list[dict]:
    if not text.strip():
        return []
    reader = csv.DictReader(io.StringIO(text))
    hmap = _header_map(reader.fieldnames)
    tick_c = _col(hmap, "ticker")
    title_c = _col(hmap, "news title")
    digest_c = _col(hmap, "daily digest")
    time_c = _col(hmap, "news time")
    name_c = _col(hmap, "company")
    sector_c = _col(hmap, "sector")
    out: list[dict] = []
    seen: set[str] = set()
    for raw in reader:
        title = (raw.get(title_c) or "").strip() if title_c else ""
        if not title:
            continue
        tick = (raw.get(tick_c) or "").strip().upper() if tick_c else ""
        published = (raw.get(time_c) or "").strip() if time_c else ""
        key = f"{tick}|{title.lower()[:160]}|{published}"
        if key in seen:
            continue
        seen.add(key)
        digest = (raw.get(digest_c) or "").strip() if digest_c else ""
        body = digest
        # Elite Ticker is the listed expression. Surface it so the family
        # router can name the issuer when the headline has no $TICKER.
        if tick and f"${tick}" not in body and f"({tick})" not in body:
            body = f"{body} Listed ticker: ${tick}.".strip()
        out.append({
            "title": title[:300],
            "body": body[:800],
            "url": "",
            "source": "theme_radar_elite",
            "harvest_source": "theme_radar_elite",
            "source_file": source_file,
            "published_at": published,
            "retrieved_at": retrieved,
            "known_at": published or retrieved,
            "ticker_hint": tick,
            "company": (raw.get(name_c) or "").strip() if name_c else "",
            "sectors": [raw[sector_c].strip()] if sector_c and raw.get(sector_c) else [],
        })
    return out


def load_theme_radar(
    date: str | None = None,
    allow_remote: bool = True,
) -> list[dict]:
    arts: list[dict] = []
    local = snapshot_paths(date)
    seen_dates: set[str] = set()
    for path in local:
        m = _DATE.search(path.name)
        retrieved = m.group(1) if m else path.stem
        seen_dates.add(retrieved)
        text = _read_text(path)
        arts.extend(_rows_from_text(text, str(path), retrieved))
    need = []
    if date and date.lower() not in {"all", "*", "history", "", None}:
        if date not in seen_dates:
            need = [date]
    if allow_remote:
        for d in need:
            text = _fetch_remote(d)
            arts.extend(_rows_from_text(text, REMOTE.format(date=d), d))
    return arts


def dedupe_elite(arts: list[dict]) -> list[dict]:
    """Unique by ticker + title. Keep the earliest News Time (leak-free)."""
    best: dict[str, dict] = {}
    order: list[str] = []
    for art in arts:
        title = str(art.get("title") or "").strip()
        if not title:
            continue
        tick = str(art.get("ticker_hint") or "").upper()
        key = f"{tick}|{title.lower()[:160]}"
        if key not in best:
            best[key] = art
            order.append(key)
            continue
        old = str(best[key].get("published_at") or "9999")
        new = str(art.get("published_at") or "9999")
        if new < old:
            best[key] = art
    return [best[k] for k in order]


def patch_load_corpus() -> None:
    """Combine parsed + grok dumps + theme-radar titles. Idempotent."""
    from . import backtest
    from .grok_automations import prefer_source

    if getattr(backtest.load_corpus, "_theme_radar_patched", False):
        return
    orig = backtest.load_corpus

    def wrapped(date: str | None = None):
        arts = orig(date)
        extra = load_theme_radar(
            date,
            allow_remote=bool(date and str(date).lower() not in {"all", "*", "history"}),
        )
        return prefer_source(list(arts) + extra)

    wrapped._theme_radar_patched = True  # type: ignore[attr-defined]
    backtest.load_corpus = wrapped


def amrx_acceptance(arts: list[dict] | None = None) -> dict[str, Any]:
    arts = arts if arts is not None else load_theme_radar("2026-09-18", allow_remote=True)
    hits = [
        a for a in arts
        if (a.get("ticker_hint") or "").upper() == "AMRX"
        and re.search(r"(?i)lanreotide|somatuline|fda approval", a.get("title") or "")
    ]
    return {
        "n_titles_0918": len(arts),
        "amrx_hits": len(hits),
        "published_at": hits[0]["published_at"] if hits else "",
        "title": hits[0]["title"] if hits else "",
        "ok": bool(hits) and "2026-09-18 16:01" in (hits[0].get("published_at") or ""),
    }
