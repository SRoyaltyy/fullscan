"""Finviz industry membership + keyword match helpers (no web)."""
from __future__ import annotations

import re
from functools import lru_cache
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
EXPORT_DIR = ROOT / "data" / "exports"

# Bare tokens that cause false positives (e.g. "Gambling" → Buffett "gambling")
GENERIC_INDUSTRY_TOKENS = {
    "gambling", "gold", "silver", "copper", "steel", "solar", "uranium",
    "tobacco", "leisure", "lodging", "publishing", "broadcasting", "restaurants",
    "airlines", "trucking", "railroads", "aluminum", "chemicals", "biotechnology",
}

# Extra tokens for industries whose Finviz name alone under-matches headlines
INDUSTRY_ALIASES: dict[str, list[str]] = {
    "Semiconductors": [
        "semiconductor", "semiconductors", "chipmaker", "chip makers", "hbm",
        "dram", "nand", "foundry", "wafer", "gpu", "ai chip", "fab ",
    ],
    "Semiconductor Equipment & Materials": [
        "semiconductor equipment", "wafer fab equipment", "asml", "lithography",
        "deposition", "etch ", "semicap", "wfe ",
    ],
    "Software - Application": [
        "saas", "application software", "enterprise software",
    ],
    "Software - Infrastructure": [
        "cloud infrastructure", "cybersecurity", "infrastructure software",
    ],
    "Internet Content & Information": [
        "digital ads", "ad revenue", "search advertising", "social media platform",
    ],
    "Oil & Gas E&P": [
        "crude oil", "wti", "brent", "e&p", "exploration and production",
        "oil inventories", "opec",
    ],
    "Oil & Gas Integrated": ["integrated oil", "major oil"],
    "Airlines": ["airline", "airlines", "jet fuel", "passenger traffic"],
    "Biotechnology": ["biotech", "fda approval", "phase 3", "phase iii", "crl "],
    "Banks - Regional": ["regional bank", "regional banks", "community bank"],
    "Solar": ["solar panel", "photovoltaic", "pv module", "tariff solar"],
    "Uranium": ["uranium", "nuclear fuel"],
    "Copper": ["copper price", "lme copper"],
    "Gold": ["gold price", "spot gold"],
    "REIT - Office": ["office reit", "office vacancy"],
    "Communication Equipment": [
        "optical transceiver", "optical networking", "coherent optics",
        "telecom equipment",
    ],
    "Gambling": [
        "sports betting", "online casino", "igaming", "draftkings", "flutter",
    ],
}


def _export_on_or_before(as_of: str | None) -> Path | None:
    files = sorted(EXPORT_DIR.glob("finviz_????-??-??.csv"))
    if not files:
        return None
    if not as_of:
        return files[-1]
    ok = [f for f in files if f.stem.replace("finviz_", "") <= as_of]
    return ok[-1] if ok else files[0]


@lru_cache(maxsize=8)
def load_universe(as_of: str | None = None) -> pd.DataFrame:
    path = _export_on_or_before(as_of)
    if path is None:
        return pd.DataFrame()
    df = pd.read_csv(path, low_memory=False)
    tcol = "Ticker" if "Ticker" in df.columns else df.columns[0]
    df["Ticker"] = df[tcol].astype(str).str.strip().str.upper()
    if "Industry" not in df.columns:
        raise SystemExit(f"no Industry column in {path}")
    df["Industry"] = df["Industry"].astype(str).str.strip()
    df["Sector"] = df.get("Sector", "").astype(str)
    df["Company"] = df.get("Company", "").astype(str)
    df["_export"] = path.name
    return df.drop_duplicates("Ticker", keep="first")


def list_industries(as_of: str | None = None) -> list[str]:
    df = load_universe(as_of)
    if df.empty:
        return []
    return sorted(df["Industry"].dropna().unique().tolist())


def resolve_industry(query: str, as_of: str | None = None) -> str:
    inds = list_industries(as_of)
    if not inds:
        raise SystemExit("no Finviz export — cannot resolve industry")
    q = (query or "").strip()
    if not q:
        raise SystemExit("empty industry")
    for i in inds:
        if i.lower() == q.lower():
            return i
    hits = [i for i in inds if q.lower() in i.lower()]
    if len(hits) == 1:
        return hits[0]
    if len(hits) > 1:
        preview = "\n".join(f"  - {h}" for h in hits[:30])
        raise SystemExit(
            f"ambiguous industry '{q}' — matches {len(hits)} names:\n{preview}\n"
            "Pass a more specific string."
        )
    toks = [t for t in re.split(r"\W+", q.lower()) if t]
    hits = [i for i in inds if all(t in i.lower() for t in toks)]
    if len(hits) == 1:
        return hits[0]
    sample = "\n".join(f"  - {i}" for i in inds[:25])
    raise SystemExit(
        f"unknown industry '{q}'. Examples:\n{sample}\n"
        "Use: python -m src.industry_predict --list"
    )


def members(industry: str, as_of: str | None = None) -> pd.DataFrame:
    df = load_universe(as_of)
    return df[df["Industry"] == industry].copy()


def match_patterns(industry: str, members_df: pd.DataFrame) -> list[re.Pattern]:
    pats: list[re.Pattern] = []
    esc = re.escape(industry)
    if industry.lower() not in GENERIC_INDUSTRY_TOKENS:
        pats.append(re.compile(rf"(?i)\b{esc}\b"))
    for tok in re.split(r"[\s/&\-]+", industry):
        tl = tok.lower()
        if len(tok) < 4 or tl in {"fund", "closed", "other", "services"}:
            continue
        if tl in GENERIC_INDUSTRY_TOKENS:
            continue
        pats.append(re.compile(rf"(?i)\b{re.escape(tok)}\b"))
    for a in INDUSTRY_ALIASES.get(industry, []):
        pats.append(re.compile(rf"(?i){re.escape(a)}"))
    for t in members_df["Ticker"].astype(str):
        if len(t) >= 2:
            pats.append(re.compile(rf"(?i)\b{re.escape(t)}\b"))
    for name in members_df["Company"].astype(str):
        name = re.sub(
            r"\b(Inc|Corp|Ltd|PLC|Co|Company|Holdings|Group|N\.?V\.?|S\.?A\.?)\b",
            "",
            name,
            flags=re.I,
        )
        name = re.sub(r"\s+", " ", name).strip()
        if len(name) >= 5:
            pats.append(re.compile(rf"(?i)\b{re.escape(name)}\b"))
        parts = name.split()
        if len(parts) >= 2 and len(parts[0]) >= 3:
            short = " ".join(parts[:2])
            if len(short) >= 5:
                pats.append(re.compile(rf"(?i)\b{re.escape(short)}\b"))
    seen = set()
    out = []
    for p in pats:
        k = p.pattern
        if k not in seen:
            seen.add(k)
            out.append(p)
    return out


def title_matches(title: str, patterns: list[re.Pattern]) -> bool:
    if not title:
        return False
    return any(p.search(title) for p in patterns)
