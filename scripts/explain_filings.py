#!/usr/bin/env python3
"""Print which EDGAR forms a ticker has, and whether the annual document downloads.

Uses the same relay as the parser. Does not write the corpus.
"""
from __future__ import annotations

import sys
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.company_filings.edgar import Client, EdgarError
from src.company_filings.run import _load_rows
from src.company_filings.select import select_filings

TICKERS = [
    ("KEP", "0000887225"),
    ("ENIC", "0001659939"),
    ("MDA", "0001857047"),
    ("HONA", "0002089271"),
    ("DXYZ", "0001843974"),
    ("OGC", "0001487326"),
    ("CSAN", ""),
]


def main() -> int:
    client = Client(per_sec=2)
    print("relay:", "on" if client.relay_url else "off", flush=True)
    mapping = client.ticker_map()
    cosan = [(ticker, mapping[ticker]) for ticker in mapping if "COSAN" in ticker or ticker.startswith("CSAN")]
    print("cosan-like tickers:", cosan[:12], flush=True)
    for ticker, cik in TICKERS:
        if not cik:
            cik = mapping.get(ticker) or ""
        if not cik:
            print(f"{ticker} no_cik", flush=True)
            continue
        cik = str(int(cik)).zfill(10)
        try:
            payload = client.submissions(cik)
        except EdgarError as exc:
            print(f"{ticker} submissions_error {exc}", flush=True)
            continue
        rows = _load_rows(client, payload)
        print(f"{ticker} {payload.get('name')} rows={len(rows)} {Counter(r['form'] for r in rows).most_common(10)}", flush=True)
        chosen = select_filings(rows)
        print("  chosen", [(r["form"], r["accession"], (r.get("primary") or "")[:70]) for r in chosen[:6]], flush=True)
        annual = next((r for r in chosen if r["form"] in {"10-K", "20-F", "40-F", "S-1", "F-1"}), None)
        if annual and annual.get("primary"):
            try:
                raw = client.document(cik, annual["accession"], annual["primary"])
                print(f"  annual_bytes {len(raw)} head {raw[:60]!r}", flush=True)
            except EdgarError as exc:
                print(f"  annual_error {exc}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
