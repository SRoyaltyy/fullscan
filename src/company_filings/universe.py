"""Preliminary universe for the company-filing parse.

Same liquidity gates as the stock book (src/stock_book.py): market cap at least
$80M, average volume at least 500k shares, ATR at least 2.5% of price. Exchange
traded funds, closed-end funds, and shell companies are dropped. Those names
have no operating business for a filing to describe.
"""
from __future__ import annotations

import csv
from pathlib import Path

MIN_MARKET_CAP_M = 80.0
MIN_AVG_VOL_K = 500.0
MIN_ATR_PCT = 2.5

DROP_INDUSTRIES = frozenset({
    "Exchange Traded Fund",
    "Closed-End Fund - Debt",
    "Closed-End Fund - Equity",
    "Closed-End Fund - Foreign",
    "Shell Companies",
})


def _num(value: str) -> float:
    text = (value or "").replace(",", "").replace("%", "").replace("$", "").strip()
    if not text or text in {"-", "—"}:
        return 0.0
    try:
        return float(text)
    except ValueError:
        return 0.0


def latest_export(export_dir: Path) -> Path:
    files = sorted(export_dir.glob("finviz_????-??-??.csv"))
    if not files:
        raise FileNotFoundError(f"no finviz export in {export_dir}")
    return files[-1]


def load_universe(path: Path) -> list[dict]:
    """Return operating companies that pass the liquidity gates, largest first."""
    kept: list[dict] = []
    with path.open(newline="") as handle:
        for row in csv.DictReader(handle):
            industry = (row.get("Industry") or "").strip()
            if industry in DROP_INDUSTRIES:
                continue
            ticker = (row.get("Ticker") or "").strip().upper()
            if not ticker or any(ch.isspace() for ch in ticker):
                continue
            market_cap_m = _num(row.get("Market Cap") or "")
            avg_vol_k = _num(row.get("Average Volume") or "")
            price = _num(row.get("Price") or "")
            atr = _num(row.get("Average True Range") or "")
            atr_pct = (100.0 * atr / price) if price > 0 else 0.0
            if market_cap_m < MIN_MARKET_CAP_M or avg_vol_k < MIN_AVG_VOL_K or atr_pct < MIN_ATR_PCT:
                continue
            kept.append({
                "ticker": ticker,
                "name": (row.get("Company") or "").strip(),
                "sector": (row.get("Sector") or "").strip(),
                "industry": industry,
                "country": (row.get("Country") or "").strip(),
                "market_cap_m": market_cap_m,
            })
    kept.sort(key=lambda row: row["market_cap_m"], reverse=True)
    return kept


def attach_ciks(rows: list[dict], ticker_to_cik: dict[str, str]) -> tuple[list[dict], list[dict]]:
    """Split rows into those with a CIK and those the SEC ticker file does not list.

    Finviz writes class shares with a hyphen. EDGAR sometimes uses a dot.
    """
    matched: list[dict] = []
    missing: list[dict] = []
    for row in rows:
        ticker = row["ticker"]
        cik = ticker_to_cik.get(ticker) or ticker_to_cik.get(ticker.replace("-", ".")) or ticker_to_cik.get(ticker.replace(".", "-"))
        if not cik:
            missing.append(row)
            continue
        found = dict(row)
        found["cik"] = str(int(cik)).zfill(10)
        matched.append(found)
    return matched, missing


def group_by_cik(rows: list[dict]) -> list[dict]:
    """One parse per CIK. Dual-class tickers share the filing."""
    grouped: dict[str, dict] = {}
    for row in rows:
        current = grouped.get(row["cik"])
        if current is None or row["market_cap_m"] > current["market_cap_m"]:
            grouped[row["cik"]] = {
                **row,
                "tickers": [row["ticker"]],
            }
        else:
            current["tickers"].append(row["ticker"])
    ordered = sorted(grouped.values(), key=lambda row: row["market_cap_m"], reverse=True)
    return ordered
