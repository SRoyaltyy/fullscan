"""Retrieve a short Finviz candidate pack from the on-disk universe CSV.

Never dump 11k descriptions into a model. Query Industry / Sector /
Company / Description with tokens from the headline + event_class, return
<=40 rows. Inference (Lane) may only emit tickers from this pack or an
explicit not_in_pack flag.
"""
from __future__ import annotations

import csv
import glob
import os
import re
from functools import lru_cache
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
EXPORT_GLOB = str(ROOT / "data" / "exports" / "finviz_*.csv")

_CLASS_TOKENS: dict[str, tuple[str, ...]] = {
    "blast_ops": ("airport", "airline", "rental", "travel", "tsa"),
    "input_cost": ("airline", "jet fuel", "refiner", "energy", "freight"),
    "market_structure": (
        "broker", "exchange", "crypto", "digital asset", "tokenization",
        "market maker", "custody",
    ),
    "capacity": ("semiconductor", "foundry", "memory", "equipment", "euv"),
    "gate": ("biotech", "drug", "pharma", "defense", "contract"),
    "trial_readout": ("biotech", "drug manufacturer"),
    "blast_legal": ("software", "internet", "semiconductor"),
    "blast_cyber": ("software", "cyber", "security"),
}

_INDUSTRY_HINT = {
    "rental": ("rental", "leasing"),
    "airline": ("airline",),
    "airport": ("airline", "airports", "rental"),
    "tsa": ("airline", "rental"),
    "jet fuel": ("airline", "oil & gas refining", "oil & gas integrated"),
    "tokeniz": ("capital markets", "financial data", "exchange"),
    "exchange": ("financial data", "exchange", "capital markets"),
    "foundry": ("semiconductor"),
    "euv": ("semiconductor equipment", "semiconductor"),
}


def latest_export(root: Path | None = None) -> Path | None:
    pat = str((root or ROOT) / "data" / "exports" / "finviz_*.csv")
    files = sorted(glob.glob(pat))
    return Path(files[-1]) if files else None


def _norm(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip().lower())


def _header_map(fieldnames: list[str] | None) -> dict[str, str]:
    out: dict[str, str] = {}
    for raw in fieldnames or []:
        key = _norm(raw).replace(".", " ")
        out[key] = raw
    return out


def _col(hmap: dict[str, str], *aliases: str) -> str | None:
    for a in aliases:
        if a in hmap:
            return hmap[a]
    for k, raw in hmap.items():
        for a in aliases:
            if a in k:
                return raw
    return None


@lru_cache(maxsize=4)
def load_universe(path: str | None = None) -> tuple[str, tuple[dict[str, str], ...]]:
    """(source_path, rows). Cached per path. Empty if no export."""
    p = Path(path) if path else latest_export()
    if p is None or not p.is_file():
        return "", tuple()
    with p.open(newline="", encoding="utf-8", errors="replace") as fh:
        reader = csv.DictReader(fh)
        hmap = _header_map(reader.fieldnames)
        tick_c = _col(hmap, "ticker", "symbol")
        name_c = _col(hmap, "company", "name")
        sector_c = _col(hmap, "sector")
        ind_c = _col(hmap, "industry")
        desc_c = _col(hmap, "description", "company description", "profile")
        rows: list[dict[str, str]] = []
        for raw in reader:
            tick = (raw.get(tick_c) or "").strip().upper() if tick_c else ""
            if not tick or len(tick) > 6:
                continue
            rows.append({
                "ticker": tick,
                "name": (raw.get(name_c) or tick).strip() if name_c else tick,
                "sector": (raw.get(sector_c) or "").strip() if sector_c else "",
                "industry": (raw.get(ind_c) or "").strip() if ind_c else "",
                "description": (raw.get(desc_c) or "").strip() if desc_c else "",
            })
        return str(p), tuple(rows)


def tokens_for(title: str, body: str = "", event_class: str = "") -> list[str]:
    blob = f"{title or ''} {body or ''}"
    found: list[str] = []
    for key, extra in _INDUSTRY_HINT.items():
        if re.search(rf"(?i)\b{re.escape(key)}", blob):
            found.extend(extra)
    found.extend(_CLASS_TOKENS.get(str(event_class or ""), ()))
    # Company-ish words longer than 3 chars from the title, skip junk.
    for w in re.findall(r"[A-Za-z][A-Za-z&.-]{3,}", title or ""):
        if w.lower() in {"this", "that", "with", "from", "after", "stock",
                         "shares", "today", "week", "news", "says"}:
            continue
        found.append(w.lower())
    # de-dupe, keep order
    seen, out = set(), []
    for t in found:
        t = t.strip().lower()
        if t and t not in seen:
            seen.add(t)
            out.append(t)
    return out[:16]


def lookup_candidates(
    title: str,
    body: str = "",
    event_class: str = "",
    limit: int = 40,
    universe_path: str | None = None,
) -> dict[str, Any]:
    src, rows = load_universe(universe_path)
    toks = tokens_for(title, body, event_class)
    if not rows or not toks:
        return {
            "source": src or "missing",
            "n_universe": len(rows),
            "tokens": toks,
            "candidates": [],
        }
    scored: list[tuple[int, dict[str, str]]] = []
    for row in rows:
        hay = _norm(
            f"{row['ticker']} {row['name']} {row['sector']} "
            f"{row['industry']} {row['description'][:400]}"
        )
        score = 0
        for t in toks:
            if t in hay:
                score += 3 if t in _norm(row["industry"]) or t in _norm(row["name"]) else 1
        if score:
            scored.append((score, row))
    scored.sort(key=lambda x: (-x[0], x[1]["ticker"]))
    cands = []
    for score, row in scored[: max(1, min(limit, 40))]:
        cands.append({
            **row,
            "description": row["description"][:220],
            "score": score,
        })
    return {
        "source": src,
        "n_universe": len(rows),
        "tokens": toks,
        "candidates": cands,
    }


def attach_candidates(
    entities: list,
    pack: dict,
    event_class: str = "",
) -> list:
    """If the family named nobody listed, surface top Finviz hits as inferred ND.

    Does not invent direction. Lane / later passes may promote from the pack.
    """
    from .schema import Entity

    cands = (pack or {}).get("finviz_candidates") or []
    if not cands:
        return entities
    have = {
        (getattr(e, "ticker", None) or (e.get("ticker") if isinstance(e, dict) else None) or "").upper()
        for e in (entities or [])
    }
    listed = [e for e in (entities or []) if (
        getattr(e, "ticker", None) or (e.get("ticker") if isinstance(e, dict) else None)
    )]
    if listed:
        return entities
    extra = []
    for row in cands[:5]:
        tick = str(row.get("ticker") or "").upper()
        if not tick or tick in have:
            continue
        extra.append(Entity(
            name=str(row.get("name") or tick),
            ticker=tick,
            role="finviz_pack",
            direction="not_determined",
            horizon="1-4w",
            inferred=True,
            if_unknown="from finviz industry/description pack; no direction yet",
            tradeable_expression="none",
        ))
        have.add(tick)
    return list(entities or []) + extra
