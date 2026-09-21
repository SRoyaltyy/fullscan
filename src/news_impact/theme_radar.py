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


def snapshot_paths(date: str | None = None) -> list[Path]:
    files: list[Path] = []
    for root in _roots():
        if not root.is_dir():
            continue
        if date and date.lower() not in {"all", "*", "history", ""}:
            p = root / f"{date}.csv"
            if p.is_file():
                files.append(p)
            continue
        files.extend(sorted(root.glob("20??-??-??.csv")))
    return [p for p in files if not p.name.endswith(".raw.csv")]


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
        out.append({
            "title": title[:300],
            "body": digest[:800],
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
        try:
            text = path.read_text(encoding="utf-8", errors="replace")
        except OSError:
            continue
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
