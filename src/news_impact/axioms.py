"""Typed axiom library. Models may retrieve by id. They may not invent new ones."""
from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
AXIOM_PATH = ROOT / "00_grounding" / "news_impact_axioms.json"

# Built-in fallback if the JSON is missing (tests / fresh clone).
BUILTIN: list[dict] = [
    {
        "id": "A_AIR_01",
        "domain": "airlines",
        "text": "Airline earnings over a holiday weekend move with completed flights and load, not terminal dwell time.",
    },
    {
        "id": "A_AIR_02",
        "domain": "airlines",
        "text": "If the buyer still wants the trip and flying is blocked, they drive: listed substitutes are rental cars.",
    },
    {
        "id": "A_AIR_03",
        "domain": "airlines",
        "text": "Jet-fuel demand follows departures. Who pays the input (airlines) is not who sells it (energy).",
    },
    {
        "id": "A_Q5_01",
        "domain": "regime",
        "text": "A constraint already in the tape is weather. Only a verified change in the constraint is news.",
    },
    {
        "id": "A_MED_01",
        "domain": "media",
        "text": "Physical White House access is an input to live pool video, not to publishing text. CNN is a slice of WBD.",
    },
    {
        "id": "A_AT_01",
        "domain": "antitrust",
        "text": "A consumer class complaint is not a finding. Day-1 listed impact is legal overhang, not tokens shipped.",
    },
    {
        "id": "A_AT_02",
        "domain": "antitrust",
        "text": "Gemini is a slice of GOOGL. Claude/ChatGPT/Grok have no clean whole-company ticker.",
    },
    {
        "id": "A_AT_03",
        "domain": "antitrust",
        "text": "A competitor not in the harm set wins relatively either legal outcome (unscathed rival).",
    },
    {
        "id": "A_AT_04",
        "domain": "antitrust",
        "text": "If two labs race on the same scarce input, the input seller's sign does not follow who wins the race. A compute cap flips that.",
    },
    {
        "id": "A_TSV_01",
        "domain": "market_structure",
        "text": "Exchanges and brokers collect rent from being the only legal venue.",
    },
    {
        "id": "A_TSV_04",
        "domain": "market_structure",
        "text": "A business that exists only inside a time-limited exemption has renewal risk.",
    },
    {
        "id": "A_TSV_05",
        "domain": "market_structure",
        "text": "Synthetics are out of the TSV order; offshore HOOD-style tokens are not the approved product.",
    },
    {
        "id": "A_TSV_06",
        "domain": "market_structure",
        "text": "Nasdaq/NYSE/DTCC are already building tokenization rails; incumbent response can flip old-rent from down to mixed.",
    },
    {
        "id": "A_CAP_01",
        "domain": "capacity",
        "text": "New supply at an oligopoly node hits incumbent sellers and helps buyers of cheaper output.",
    },
    {
        "id": "A_CAP_02",
        "domain": "capacity",
        "text": "Sold-out tools are scarce add for the toolmaker, not extra wafers for the foundry.",
    },
    {
        "id": "A_INS_01",
        "domain": "integrity",
        "text": "A Nasdaq 10-Q delay is a governance print on the named issuer's sector, never Technology-by-exchange.",
    },
]


def load_axioms() -> list[dict]:
    if AXIOM_PATH.is_file():
        try:
            data = json.loads(AXIOM_PATH.read_text(encoding="utf-8"))
            rows = data.get("axioms") if isinstance(data, dict) else data
            if isinstance(rows, list) and rows:
                return [r for r in rows if isinstance(r, dict) and r.get("id")]
        except (OSError, json.JSONDecodeError, TypeError):
            pass
    return list(BUILTIN)


def by_id() -> dict[str, dict]:
    return {a["id"]: a for a in load_axioms()}


def for_domain(domain: str) -> list[dict]:
    want = (domain or "").lower()
    return [a for a in load_axioms() if want in str(a.get("domain") or "").lower()]


def retrieve(ids: list[str]) -> list[dict]:
    idx = by_id()
    out = []
    for i in ids:
        if i in idx:
            out.append(idx[i])
    return out
