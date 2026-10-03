"""Hotness: could this article be about company X?

Code only. Heat comes from rare words shared with the company description
(Finviz_Description, plus industry). The company name is a bonus, not a
requirement. A hit is a candidate, not a keep.
"""
from __future__ import annotations

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
    "limited ltd plc group holdings holding".split()
)


def tokens(text: str) -> list[str]:
    return [w for w in _WORD.findall((text or "").lower()) if w not in _STOP]


def _name_hit(name: str, low: str) -> bool:
    parts = [p for p in re.split(r"[^a-z0-9]+", name.lower()) if len(p) >= 4]
    return any(re.search(rf"\b{re.escape(p)}\b", low) for p in parts)


class HotBoard:
    def __init__(self, companies: list[dict]):
        self.companies: list[dict] = []
        df: Counter[str] = Counter()
        for raw in companies:
            ticker = str(raw.get("ticker") or "").upper().strip()
            name = str(raw.get("name") or raw.get("company_name") or "").strip()
            desc = " ".join(
                str(raw.get(k) or "")
                for k in ("description", "industry", "sector")
            )
            if not ticker or not (name or desc):
                continue
            words = set(tokens(f"{name} {desc}"))
            if not words:
                continue
            self.companies.append({"ticker": ticker, "name": name, "words": words})
            df.update(words)
        n = max(len(self.companies), 1)
        self.idf = {w: math.log((n + 1) / (c + 1)) + 1.0 for w, c in df.items()}

    def score(self, text: str) -> list[dict]:
        body = text or ""
        words = set(tokens(body))
        named = {t for t in _TICKER.findall(body) if t not in _FALSE_TICKER}
        low = body.lower()
        ranked: list[dict] = []
        for co in self.companies:
            overlap = words & co["words"]
            name_hit = co["ticker"] in named or _name_hit(co["name"], low)
            if not overlap and not name_hit:
                continue
            hot = sum(self.idf.get(w, 1.0) for w in overlap)
            if name_hit:
                hot += 1.5
            ranked.append({
                "ticker": co["ticker"],
                "name": co["name"],
                "hot": round(hot, 3),
                "name_hit": name_hit,
                "overlap": sorted(overlap)[:8],
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
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    if isinstance(raw, dict):
        raw = raw.get("companies") or raw.get("items") or []
    return [r for r in raw if isinstance(r, dict)]


def main() -> None:
    import argparse
    p = argparse.ArgumentParser(description="Score article hotness against company blurbs")
    p.add_argument("companies", help="JSON list of ticker, name, description")
    p.add_argument("text", help="Headline or article text")
    args = p.parse_args()
    board = HotBoard(load_companies(args.companies))
    print(json.dumps(board.link(args.text), indent=2))


if __name__ == "__main__":
    main()
