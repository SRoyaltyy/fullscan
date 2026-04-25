"""
Collector: SEC EDGAR Recent Filings
Source: SEC EDGAR full-text search API (EFTS)
Schedule: Every 3 hours on weekdays
Populates: sec_filings table

Fetches recent 8-K, 10-K, 10-Q, 13F, Form 4, SC 13D filings.
These are the raw material for detecting:
  - Material events (8-K): contract wins, acquisitions, leadership changes
  - Quarterly/Annual reports (10-Q/10-K): earnings, risk factors
  - Institutional ownership (13F-HR): who's buying/selling
  - Insider transactions (Form 4): executive buying/selling
  - Activist stakes (SC 13D/G): activist investor accumulation
"""

import requests
import time
from datetime import datetime, timedelta
from db.db_utils import get_db, ensure_schema

HEADERS = {
    "User-Agent": "StockCatalystEngine your_name@your_email.com"
}

EFTS_URL = "https://efts.sec.gov/LATEST/search-index"
EDGAR_SEARCH_URL = "https://efts.sec.gov/LATEST/search-index"

# Filing types we care about, mapped to why
FORM_TYPES = [
    "8-K",          # Material events
    "10-K",         # Annual reports
    "10-Q",         # Quarterly reports
    "13F-HR",       # Institutional holdings
    "4",            # Insider transactions
    "SC 13D",       # Activist investor (>5% stake)
    "SC 13G",       # Passive large holder (>5% stake)
    "S-1",          # IPO registration
    "DEF 14A",      # Proxy statement (executive comp, board votes)
]


def fetch_recent_filings(form_type: str, days_back: int = 2):
    """
    Use the SEC EDGAR full-text search to find recent filings of a given type.
    Returns a list of filing dicts.
    """
    end_date = datetime.utcnow().strftime("%Y-%m-%d")
    start_date = (datetime.utcnow() - timedelta(days=days_back)).strftime("%Y-%m-%d")

    filings = []

    try:
        # EDGAR EFTS search endpoint
        resp = requests.get(
            "https://efts.sec.gov/LATEST/search-index",
            params={
                "q": "",
                "dateRange": "custom",
                "startdt": start_date,
                "enddt": end_date,
                "forms": form_type,
                "from": 0,
                "size": 50,         # max per request
            },
            headers=HEADERS,
            timeout=20,
        )
        resp.raise_for_status()
        data = resp.json()

        for hit in data.get("hits", {}).get("hits", []):
            source = hit.get("_source", {})
            filing = {
                "cik": str(source.get("ciks", [""])[0]) if source.get("ciks") else "",
                "company_name": (source.get("display_names", [""])[0]
                                 if source.get("display_names") else ""),
                "ticker": (source.get("tickers", [""])[0]
                           if source.get("tickers") else ""),
                "form_type": source.get("form_type", form_type),
                "filing_date": source.get("file_date", ""),
                "url": (f"https://www.sec.gov/Archives/edgar/data/"
                        f"{source.get('ciks', [''])[0]}/"
                        f"{source.get('file_num', '')}".replace("-", "")),
                "description": source.get("display_description", "")[:500],
            }

            # Build a proper EDGAR URL from the accession number if available
            accession = source.get("accession_no", "")
            if accession and filing["cik"]:
                acc_clean = accession.replace("-", "")
                filing["url"] = (
                    f"https://www.sec.gov/Archives/edgar/data/"
                    f"{filing['cik']}/{acc_clean}/{accession}-index.htm"
                )

            filings.append(filing)

    except Exception as e:
        print(f"  [ERROR] EFTS search for {form_type}: {e}")

    return filings


def collect():
    print("[sec_filings] Collecting recent SEC filings...")

    db = get_db()
    ensure_schema(db)
    cursor = db.cursor()

    total_new = 0

    for form_type in FORM_TYPES:
        try:
            filings = fetch_recent_filings(form_type, days_back=3)

            new_count = 0
            for f in filings:
                cursor.execute("""
                    INSERT OR IGNORE INTO sec_filings
                        (cik, ticker, company_name, form_type,
                         filing_date, url, description)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    f["cik"], f["ticker"], f["company_name"],
                    f["form_type"], f["filing_date"],
                    f["url"], f["description"],
                ))
                if cursor.rowcount > 0:
                    new_count += 1

            total_new += new_count
            print(f"  {form_type:10s}: {new_count} new filings (of {len(filings)} fetched)")

            time.sleep(0.3)  # SEC rate limit: 10 req/sec, we're well under

        except Exception as e:
            print(f"  [ERROR] {form_type}: {e}")

    db.commit()
    db.close()
    print(f"[sec_filings] Done. {total_new} new filings collected.")


if __name__ == "__main__":
    collect()
