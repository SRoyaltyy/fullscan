#!/usr/bin/env python3
"""Parse SEC filing text for every stock that passes the preliminary filters.

The corpus is a local SQLite file under data/company_filings/. It is not
committed. Re-running skips a company whose selected filings have not changed.

    python scripts/parse_company_filings.py --ticker RKLB
    python scripts/parse_company_filings.py --universe --workers 3
"""
from __future__ import annotations

import argparse
import json
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.company_filings.edgar import Client, EdgarBlocked
from src.company_filings.run import db_over_budget, parse_company
from src.company_filings.store import Store
from src.company_filings.universe import attach_ciks, group_by_cik, latest_export, load_universe


def _companies(args, client: Client) -> list[dict]:
    if args.ticker:
        mapping = client.ticker_map()
        rows = []
        for ticker in args.ticker:
            symbol = ticker.strip().upper()
            cik = mapping.get(symbol) or mapping.get(symbol.replace("-", ".")) or mapping.get(symbol.replace(".", "-"))
            if not cik:
                print(f"{symbol}  no_cik", flush=True)
                continue
            rows.append({
                "ticker": symbol,
                "cik": str(int(cik)).zfill(10),
                "name": "",
                "sector": "",
                "industry": "",
                "market_cap_m": 0,
                "tickers": [symbol],
            })
        return rows
    export = Path(args.export) if args.export else latest_export(ROOT / "data" / "exports")
    universe = load_universe(export)
    matched, missing = attach_ciks(universe, client.ticker_map())
    print(
        f"universe {export.name}: {len(universe)} pass filters, "
        f"{len(matched)} have a CIK, {len(missing)} do not",
        flush=True,
    )
    grouped = group_by_cik(matched)
    if args.limit:
        grouped = grouped[:args.limit]
    return grouped, missing


def main() -> int:
    parser = argparse.ArgumentParser(description="Parse SEC filing text into the company record")
    parser.add_argument("--ticker", action="append", help="One ticker. Repeatable. Skips the universe filter.")
    parser.add_argument("--universe", action="store_true", help="Every stock that passes the preliminary filters")
    parser.add_argument("--export", default="", help="Finviz CSV. Defaults to the latest dated export.")
    parser.add_argument("--limit", type=int, default=0, help="Stop after this many companies")
    parser.add_argument("--workers", type=int, default=3)
    parser.add_argument("--db", default=str(ROOT / "data" / "company_filings" / "filings.sqlite"))
    parser.add_argument("--force", action="store_true", help="Reparse even when the filing set is unchanged")
    args = parser.parse_args()
    if not args.ticker and not args.universe:
        parser.error("pass --ticker or --universe")

    db_path = Path(args.db)
    store = Store(db_path)
    client = Client(per_sec=5)
    try:
        loaded = _companies(args, client)
    except EdgarBlocked as exc:
        print(exc, flush=True)
        store.close()
        return 2
    missing: list[dict] = []
    if isinstance(loaded, tuple):
        companies, missing = loaded
    else:
        companies = loaded
    for row in missing:
        store.mark({**row, "cik": "", "tickers": [row["ticker"]]}, "no_cik", "not in SEC company_tickers")

    status_path = db_path.parent / "run_status.json"
    lock = threading.Lock()
    counts = {"ok": 0, "skip": 0, "no_filing": 0, "error": 0}
    stopped = False

    def work(company: dict) -> tuple[str, str]:
        if db_over_budget(db_path):
            return company["ticker"], "stopped"
        try:
            outcome = parse_company(company, client, store, force=args.force)
        except EdgarBlocked as exc:
            return company["ticker"], f"blocked: {exc}"
        return company["ticker"], outcome

    workers = 1 if args.ticker and len(companies) == 1 else max(1, args.workers)
    blocked_message = ""
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(work, company) for company in companies]
        for done, future in enumerate(as_completed(futures), start=1):
            ticker, outcome = future.result()
            if outcome.startswith("blocked:"):
                blocked_message = outcome.split(":", 1)[1].strip()
                pool.shutdown(cancel_futures=True, wait=False)
                break
            with lock:
                if outcome == "stopped":
                    stopped = True
                else:
                    counts[outcome] = counts.get(outcome, 0) + 1
                snapshot = dict(counts)
            print(f"{done}/{len(companies)} {ticker} {outcome}", flush=True)
            if done % 25 == 0 or done == len(companies):
                summary = store.summary()
                summary["run"] = snapshot
                summary["stopped_for_storage"] = stopped
                status_path.write_text(json.dumps(summary, indent=2))
    summary = store.summary()
    summary["run"] = counts
    summary["stopped_for_storage"] = stopped
    if blocked_message:
        summary["blocked"] = blocked_message
    status_path.write_text(json.dumps(summary, indent=2))
    print(json.dumps(summary, indent=2), flush=True)
    store.close()
    if blocked_message:
        print(blocked_message, flush=True)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
