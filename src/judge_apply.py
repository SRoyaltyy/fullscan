"""Parse news_judge markdown into a machine file the rest of the pipeline can use.

The judge used to be prompt decoration. This module turns B1_INJECT + TOP_ITEMS
into ticker tilts, sector tilts, and a risk tilt that weather / news_actions /
stock_book actually add to scores.
"""
from __future__ import annotations

import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
NEWS_DIR = ROOT / "01_daily" / "news"

_TICKER = re.compile(r"\(([A-Z]{1,5})\)")
_SECTOR_LINE = re.compile(
    r"SECTOR\s+([A-Za-z][A-Za-z /&-]+):\s*\[(bullish|bearish|mixed|hawkish)\]",
    re.I,
)
_MACRO_LINE = re.compile(
    r"MACRO\s+\w+:\s*\[(bullish|bearish|mixed|hawkish)\]",
    re.I,
)
_POL = re.compile(r"pol=(bullish|bearish|hawkish|mixed)", re.I)

SECTOR_ALIAS = {
    "tech": "Technology",
    "technology": "Technology",
    "software": "Technology",
    "semis": "Technology",
    "semiconductor": "Technology",
    "materials": "Basic Materials",
    "basic materials": "Basic Materials",
    "energy": "Energy",
    "financial": "Financial",
    "financials": "Financial",
    "banks": "Financial",
    "healthcare": "Healthcare",
    "health": "Healthcare",
    "utilities": "Utilities",
    "utility": "Utilities",
    "industrials": "Industrials",
    "industrial": "Industrials",
    "consumer": "Consumer Cyclical",
    "discretionary": "Consumer Cyclical",
    "consumer-discretionary": "Consumer Cyclical",
    "consumer discretionary": "Consumer Cyclical",
    "consumer cyclical": "Consumer Cyclical",
    "consumer-cyclical": "Consumer Cyclical",
    "staples": "Consumer Defensive",
    "defensive": "Consumer Defensive",
    "consumer defensive": "Consumer Defensive",
    "consumer-defensive": "Consumer Defensive",
    "real estate": "Real Estate",
    "reit": "Real Estate",
    "communication": "Communication Services",
    "communication services": "Communication Services",
    "comms": "Communication Services",
}

KNOWN_NAMES = {
    "ADBE": "ADBE", "ADI": "ADI", "AU": "AU", "WMT": "WMT", "TGT": "TGT",
    "AAL": "AAL", "DAL": "DAL", "UAL": "UAL", "XOM": "XOM", "CVX": "CVX",
    "NVDA": "NVDA", "AAPL": "AAPL", "MSFT": "MSFT", "AMZN": "AMZN",
    "META": "META", "GOOGL": "GOOGL", "TSLA": "TSLA", "JPM": "JPM",
    "BAC": "BAC", "GS": "GS", "XLE": "XLE", "XLK": "XLK", "XLU": "XLU",
    "Adobe": "ADBE", "Walmart": "WMT", "Target": "TGT",
    "American Airlines": "AAL", "Analog Devices": "ADI",
    "Moderna": "MRNA", "Merck": "MRK",
    "BlackBerry": "BB", "Blackberry": "BB",
    "Salesforce": "CRM", "Amgen": "AMGN", "Repatha": "AMGN",
    "Broadcom": "AVGO", "Cardinal Health": "CAH",
    "NextEra": "NEE", "Constellation": "CEG", "Vistra": "VST",
}


def _pol_to_sign(p: str) -> int:
    p = (p or "").lower()
    if p in ("bullish", "positive"):
        return 1
    if p in ("bearish", "hawkish", "negative"):
        return -1
    return 0


def parse_judge_md(text: str) -> dict:
    tickers: dict[str, float] = {}
    sectors: dict[str, str] = {}
    macros: list[str] = []

    for m in _SECTOR_LINE.finditer(text):
        raw, pol = m.group(1).strip().lower(), m.group(2).lower()
        name = SECTOR_ALIAS.get(raw, raw.title())
        prev = sectors.get(name)
        if prev in ("bearish", "hawkish") and pol == "mixed":
            pass
        elif prev == "bullish" and pol == "mixed":
            pass
        elif prev in ("bearish", "hawkish") and pol == "bullish":
            sectors[name] = "mixed"
        elif prev == "bullish" and pol in ("bearish", "hawkish"):
            sectors[name] = "mixed"
        else:
            sectors[name] = pol
        # tickers on the same line
        line = text[m.start(): text.find("\n", m.start())]
        for t in _TICKER.findall(line):
            tickers[t] = tickers.get(t, 0) + _pol_to_sign(pol) * 2.0
        for name_s, t in KNOWN_NAMES.items():
            if re.search(rf"\b{re.escape(name_s)}\b", line):
                tickers[t] = tickers.get(t, 0) + _pol_to_sign(pol) * 2.0

    for m in _MACRO_LINE.finditer(text):
        macros.append(m.group(1).lower())

    for line in text.splitlines():
        pol_m = _POL.search(line)
        if not pol_m:
            continue
        sign = _pol_to_sign(pol_m.group(1))
        if not sign:
            continue
        for t in _TICKER.findall(line):
            tickers[t] = tickers.get(t, 0) + sign * 1.5
        for name_s, t in KNOWN_NAMES.items():
            if re.search(rf"\b{re.escape(name_s)}\b", line):
                tickers[t] = tickers.get(t, 0) + sign * 1.5

    b1 = ""
    if "B1_INJECT:" in text:
        b1 = text.split("B1_INJECT:", 1)[1]
        if "NEWS_PARSE_END" in b1:
            b1 = b1.split("NEWS_PARSE_END", 1)[0]
        b1 = b1.strip()

    bear = sum(1 for m in macros if m in ("bearish", "hawkish"))
    bull = sum(1 for m in macros if m == "bullish")
    if bear > bull:
        risk = "off"
    elif bull > bear:
        risk = "on"
    else:
        risk = None

    return {
        "risk_tilt": risk,
        "macros": macros,
        "sector_tilts": sectors,
        "tickers": {k: round(v, 2) for k, v in tickers.items() if abs(v) >= 0.5},
        "b1_inject": b1[:2500],
    }


def load_or_parse(date_str: str) -> dict:
    js = NEWS_DIR / f"{date_str}_judge.json"
    if js.exists():
        try:
            return json.loads(js.read_text(encoding="utf-8"))
        except Exception:
            pass
    md = NEWS_DIR / f"{date_str}_judge.md"
    if not md.exists():
        md = NEWS_DIR / "latest_judge.md"
    if not md.exists():
        return {}
    parsed = parse_judge_md(md.read_text(encoding="utf-8"))
    parsed["date"] = date_str
    try:
        NEWS_DIR.mkdir(parents=True, exist_ok=True)
        js.write_text(json.dumps(parsed, indent=1), encoding="utf-8")
    except OSError:
        pass
    return parsed
