"""
Collector: Ticker Master List
Source: SEC EDGAR company_tickers.json
Schedule: Weekly
Populates: tickers table

The SEC publishes a JSON mapping of every CIK to its ticker and company name.
This is the authoritative source for "what stocks exist on US exchanges."
"""

import requests
import sys
from db.db_utils import get_db, ensure_schema

SEC_URL = "https://www.sec.gov/files/company_tickers.json"
HEADERS = {
    # SEC REQUIRES this header. Replace with your real name and email.
    "User-Agent": "StockCatalystEngine your_name@your_email.com"
}


def collect():
    print("[ticker_master] Fetching SEC EDGAR ticker list...")

    resp = requests.get(SEC_URL, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    db = get_db()
    ensure_schema(db)
    cursor = db.cursor()

    count = 0
    for _, entry in data.items():
        ticker = entry.get("ticker", "").upper()
        name = entry.get("title", "")
        cik = str(entry.get("cik_str", ""))

        if not ticker:
            continue

        cursor.execute("""
            INSERT INTO tickers (ticker, company_name, cik, updated_at)
            VALUES (?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(ticker) DO UPDATE SET
                company_name = excluded.company_name,
                cik = excluded.cik,
                updated_at = CURRENT_TIMESTAMP
        """, (ticker, name, cik))
        count += 1

    db.commit()
    db.close()
    print(f"[ticker_master] Loaded {count} tickers.")


if __name__ == "__main__":
    collect()
