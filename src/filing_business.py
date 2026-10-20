"""Pull the business narrative out of a PR642 full-filing object store.

The archive keeps every exhibit. The correlation profile uses Item 1 of a
10-K or 40-F. A 20-F that only points at another report adds nothing, and the
Finviz description remains the profile. Eight-Ks stay out: they are events,
not the description of the business.
"""
from __future__ import annotations

import gzip
import json
import re
from pathlib import Path

ANNUAL = {"10-K", "10-K/A", "20-F", "20-F/A", "40-F", "40-F/A"}
_ITEM = re.compile(r"(?i)\bitem\s+1\b(?!\s*[.\s]*a\b)")
_ITEM_1A = re.compile(r"(?i)\bitem\s+1a\b")
_TOC_NEAR = re.compile(r"(?i)item\s+1a\b")


def _load_text(objects: Path, manifest: dict) -> str:
    key = manifest.get("texts", {}).get("key") or ""
    primary = manifest.get("primary_document") or ""
    if not key or not primary:
        return ""
    raw = (objects / key).read_bytes()
    if raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    payload = json.loads(raw)
    body = payload.get(primary) or {}
    if isinstance(body, dict):
        return str(body.get("text") or "")
    return str(body or "")


def item1_business(text: str) -> str:
    """Return the real Item 1, skipping the table-of-contents mention."""
    start = None
    for match in _ITEM.finditer(text):
        window = text[match.end(): match.end() + 220]
        if _TOC_NEAR.search(window):
            continue
        if window.lower().count("item ") >= 2:
            continue
        start = match.end()
        break
    if start is None:
        return ""
    end_match = _ITEM_1A.search(text, start)
    end = end_match.start() if end_match else min(len(text), start + 20000)
    chunk = text[start:end].strip()
    if len(chunk) < 400:
        return ""
    return chunk[:20000]


def business_text(text: str, form: str) -> str:
    """Annual business narrative. A 20-F that only cross-references another
    report contributes nothing; the Finviz description stays the profile."""
    form = form.upper()
    if form.startswith("10-K") or form.startswith("40-F"):
        item = item1_business(text)
        if len(item) >= 800:
            return item
    return ""


def load_business_texts(objects: Path) -> dict[str, str]:
    """Map ticker -> annual business prose for every company under objects."""
    objects = Path(objects)
    root = objects / "company-filings" / "full-v1"
    if not root.is_dir():
        root = objects
    out: dict[str, str] = {}
    if not root.is_dir():
        return out
    for company_path in root.glob("*/company.json"):
        try:
            company = json.loads(company_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        tickers = [str(t).upper() for t in company.get("tickers") or [] if t]
        annuals = [
            row for row in company.get("filings") or []
            if str(row.get("form") or "").upper() in ANNUAL and row.get("text_complete")
        ]
        if not annuals or not tickers:
            continue
        annuals.sort(key=lambda row: str(row.get("filing_date") or ""))
        chosen = annuals[-1]
        manifest_key = chosen.get("manifest_key") or ""
        manifest_path = objects / manifest_key if manifest_key else None
        if manifest_path is None or not manifest_path.is_file():
            # company.json filings omit the key; rebuild it from the accession.
            accession = chosen.get("accession") or ""
            cik = company.get("cik") or company_path.parent.name
            manifest_path = company_path.parent / accession / "manifest.json"
        if not manifest_path.is_file():
            continue
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            text = _load_text(objects, manifest)
            prose = business_text(text, str(chosen.get("form") or ""))
        except (OSError, json.JSONDecodeError, KeyError, ValueError):
            continue
        if len(prose) < 400:
            continue
        for ticker in tickers:
            out[ticker] = prose
    return out
