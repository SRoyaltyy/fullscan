"""
Collector: Ticker Master List
Source: SEC EDGAR company_tickers.json
Schedule: Weekly
Populates: tickers table (full universe — NOT the curated ticker_master watchlist)
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


def collect():
    start = time.time()
    print("[ticker_master] Fetching SEC EDGAR ticker list...")

    resp = requests.get(SEC_URL, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    conn = get_connection()
    cur = conn.cursor()

    count = 0
    for _, entry in data.items():
        ticker = entry.get("ticker", "").upper()
        name = entry.get("title", "")
        cik = str(entry.get("cik_str", ""))

        if not ticker:
            continue

        cur.execute("""
            INSERT INTO tickers (ticker, company_name, cik, updated_at)
            VALUES (%s, %s, %s, NOW())
            ON CONFLICT (ticker) DO UPDATE SET
                company_name = EXCLUDED.company_name,
                cik = EXCLUDED.cik,
                updated_at = NOW()
        """, (ticker, name, cik))
        count += 1

    conn.commit()
    duration = time.time() - start

    cur.execute(
        "INSERT INTO collection_log(collector, status, records_added, duration_sec) VALUES(%s,%s,%s,%s)",
        ("ticker_master", "ok", count, round(duration, 1)),
    )
    conn.commit()
    cur.close()
    conn.close()
    print(f"[ticker_master] Loaded {count} tickers in {duration:.1f}s.")


if __name__ == "__main__":
    collect()
