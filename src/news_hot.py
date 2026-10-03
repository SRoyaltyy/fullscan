"""Hotness: could this article be about company X?

Code only. Heat comes from words shared with Finviz_Description, then the
granular industry, then the sector. The company name is a bonus. A hit is
a candidate, not a keep.

CSV columns: Ticker, Company, Industry, Sector, Finviz_Description.
"""
from __future__ import annotations

import csv
import json
import math
import re
from collections import Counter
from pathlib import Path

_WORD = re.compile(r"[a-z0-9]{3,}")
_TICKER = re.compile(r"\b[A-Z]{2,5}\b")
_FALSE_TICKER = frozenset("AI ALL AND ARE FOR HAS ITS NOT OUR THE WAS YOU".split())
_STOP = frozenset(
    "the and for are was not but you all can our its that this with from "
    "have has had been were they their company provides offers including "
    "through about into over also operates based products services "
    "primarily segment segments business businesses inc corp corporation "
    "limited ltd plc group holdings holding which other their".split()
)


def tokens(text: str) -> set[str]:
    return {w for w in _WORD.findall((text or "").lower()) if w not in _STOP}


def _name_hit(name: str, low: str) -> bool:
    parts = [p for p in re.split(r"[^a-z0-9]+", name.lower()) if len(p) >= 4]
    return any(re.search(rf"\b{re.escape(p)}\b", low) for p in parts)


class HotBoard:
    def __init__(self, companies: list[dict]):
        self.companies: list[dict] = []
        df: Counter[str] = Counter()
        packed = []
        for raw in companies:
            ticker = str(raw.get("ticker") or raw.get("Ticker") or "").upper().strip()
            name = str(raw.get("name") or raw.get("Company") or raw.get("company_name") or "").strip()
            desc = tokens(str(raw.get("description") or raw.get("Finviz_Description") or ""))
            industry = tokens(str(raw.get("industry") or raw.get("Industry") or ""))
            sector = tokens(str(raw.get("sector") or raw.get("Sector") or ""))
            if not ticker or not (desc or industry or name):
                continue
            packed.append({"ticker": ticker, "name": name, "desc": desc, "industry": industry, "sector": sector})
            df.update(desc | industry | sector)
        n = max(len(packed), 1)
        self.idf = {w: math.log((n + 1) / (c + 1)) + 1.0 for w, c in df.items()}
        self.companies = packed

    def score(self, text: str) -> list[dict]:
        body = text or ""
        words = tokens(body)
        named = {t for t in _TICKER.findall(body) if t not in _FALSE_TICKER}
        low = body.lower()
        ranked: list[dict] = []
        for co in self.companies:
            d = words & co["desc"]
            ind = words & co["industry"]
            sec = words & co["sector"]
            name_hit = co["ticker"] in named or _name_hit(co["name"], low)
            if not (d or ind or sec or name_hit):
                continue
            hot = (
                sum(self.idf.get(w, 1.0) for w in d)
                + 0.6 * sum(self.idf.get(w, 1.0) for w in ind)
                + 0.15 * sum(self.idf.get(w, 1.0) for w in sec)
            )
            if name_hit:
                hot += 1.5
            if hot <= 0:
                continue
            ranked.append({
                "ticker": co["ticker"],
                "name": co["name"],
                "hot": round(hot, 3),
                "name_hit": name_hit,
                "overlap": sorted(d or ind or sec)[:8],
            })
        ranked.sort(key=lambda r: -r["hot"])
        return ranked

    def link(self, text: str, min_hot: float = 2.0, gap: float = 1.5) -> dict:
        ranked = self.score(text)
        warm = [r for r in ranked if r["hot"] >= min_hot]
        if not warm:
            return {"kind": "none", "hits": []}
        top = warm[0]["hot"]
        clear = [r for r in warm if top - r["hot"] < gap]
        kind = "company" if len(clear) == 1 else "sector"
        return {"kind": kind, "hits": clear[:8]}


def load_companies(path: str | Path) -> list[dict]:
    path = Path(path)
    if path.suffix.lower() == ".csv":
        with path.open(newline="", encoding="utf-8", errors="replace") as f:
            return [r for r in csv.DictReader(f)]
    raw = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(raw, dict):
        raw = raw.get("companies") or raw.get("items") or []
    return [r for r in raw if isinstance(r, dict)]


def main() -> None:
    import argparse
    p = argparse.ArgumentParser(description="Score article hotness against Finviz descriptions")
    p.add_argument("companies", help="CSV or JSON with Ticker, Company, Industry, Sector, Finviz_Description")
    p.add_argument("text", help="Headline or article text")
    args = p.parse_args()
    board = HotBoard(load_companies(args.companies))
    print(json.dumps(board.link(args.text), indent=2))


if __name__ == "__main__":
    main()
