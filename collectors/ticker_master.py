"""
Collector: Ticker Master List
Source: SEC EDGAR company_tickers.json
Schedule: Weekly
Populates: tickers table (full universe)
"""

import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import requests
from db.connection import get_connection

SEC_URL = "https://www.sec.gov/files/company_tickers.json"
HEADERS = {
    "User-Agent": "StockCatalystEngine your_name@your_email.com"
}

BATCH_SIZE = 500


def collect():
    start = time.time()
    print("[ticker_master] Fetching SEC EDGAR ticker list...", flush=True)

    resp = requests.get(SEC_URL, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    print(f"[ticker_master] Got {len(data)} entries from SEC.", flush=True)

    conn = get_connection()
    cur = conn.cursor()

    # Build full list first
    rows = []
    for _, entry in data.items():
        ticker = entry.get("ticker", "").upper()
        name = entry.get("title", "")
        cik = str(entry.get("cik_str", ""))
        if ticker:
            rows.append((ticker, name, cik))

    # Batch upsert
    count = 0
    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i : i + BATCH_SIZE]
        args = []
        placeholders = []
        for ticker, name, cik in batch:
            placeholders.append("(%s, %s, %s, NOW())")
            args.extend([ticker, name, cik])

        sql = (
            "INSERT INTO tickers (ticker, company_name, cik, updated_at) VALUES "
            + ", ".join(placeholders)
            + " ON CONFLICT (ticker) DO UPDATE SET "
            "company_name = EXCLUDED.company_name, "
            "cik = EXCLUDED.cik, "
            "updated_at = NOW()"
        )
        cur.execute(sql, args)
        count += len(batch)
        print(f"[ticker_master] Upserted {count}/{len(rows)}...", flush=True)

    conn.commit()
    duration = time.time() - start

    cur.execute(
        "INSERT INTO collection_log(collector, status, records_added, duration_sec) VALUES(%s,%s,%s,%s)",
        ("ticker_master", "ok", count, round(duration, 1)),
    )
    conn.commit()
    cur.close()
    conn.close()
    print(f"[ticker_master] Loaded {count} tickers in {duration:.1f}s.", flush=True)


if __name__ == "__main__":
    collect()
