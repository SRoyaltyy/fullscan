"""Hotness: is this article a candidate for company X?

Code only. No model call. A hit means the article shares rare words with
the company blurb, or names the company. It is not a keep.

Companies are {"ticker", "name", "description"}. Descriptions are the
FMP company_profiles text, or any export of it.
"""
from __future__ import annotations

import json
import math
import re
from collections import Counter
from pathlib import Path

_WORD = re.compile(r"[a-z]{3,}")
_TICKER = re.compile(r"\b[A-Z]{1,5}\b")
_STOP = frozenset(
    "the and for are was not but you all can our its that this with from "
    "have has had been were they their company provides offers including "
    "through about into over also operates based products services "
    "primarily segment segments business businesses inc corp corporation "
    "limited ltd plc group holdings holding".split()
)


def tokens(text: str) -> list[str]:
    return [w for w in _WORD.findall((text or "").lower()) if w not in _STOP]


class HotBoard:
    def __init__(self, companies: list[dict]):
        self.companies: list[dict] = []
        df: Counter[str] = Counter()
        for raw in companies:
            ticker = str(raw.get("ticker") or "").upper().strip()
            name = str(raw.get("name") or raw.get("company_name") or "").strip()
            desc = str(raw.get("description") or "")
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
        """Return companies warm to this text, hottest first."""
        body = text or ""
        words = set(tokens(body))
        named = set(_TICKER.findall(body))
        low = body.lower()
        ranked: list[dict] = []
        for co in self.companies:
            overlap = words & co["words"]
            if not overlap and co["ticker"] not in named and co["name"].lower() not in low:
                continue
            hot = sum(self.idf.get(w, 1.0) for w in overlap)
            name_hit = co["ticker"] in named or (co["name"] and co["name"].lower() in low)
            if name_hit:
                hot += 3.0
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
        """One clear name, a tied sector, or no company link."""
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
