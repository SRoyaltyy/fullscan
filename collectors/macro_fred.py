"""
Collector: FRED Macroeconomic Data
Source: Federal Reserve Economic Data (FRED) API
Schedule: Daily
Populates: macro_indicators table

FRED has 800,000+ time series. We pull the ~25 most market-relevant ones.
Free tier: 120 requests/minute, no daily limit.
"""

import requests
import os
import time
from db.db_utils import get_db, ensure_schema

API_KEY = os.environ.get("FRED_API_KEY", "")
BASE_URL = "https://api.stlouisfed.org/fred/series/observations"

# ── Key macro series ───────────────────────────────────────────
# Format: FRED_SERIES_ID → human-readable name
SERIES = {
    # Inflation & Prices
    "CPIAUCSL":         "CPI — All Urban Consumers",
    "CPILFESL":         "Core CPI (ex Food & Energy)",
    "PCEPI":            "PCE Price Index",
    "PPIACO":           "PPI — All Commodities",

    # Interest Rates & Yields
    "FEDFUNDS":         "Federal Funds Effective Rate",
    "DFF":              "Federal Funds Daily Rate",
    "DGS2":             "2-Year Treasury Yield",
    "DGS10":            "10-Year Treasury Yield",
    "DGS30":            "30-Year Treasury Yield",
    "T10Y2Y":           "10Y–2Y Treasury Spread",
    "T10Y3M":           "10Y–3M Treasury Spread",
    "MORTGAGE30US":     "30-Year Fixed Mortgage Rate",

    # Employment
    "UNRATE":           "Unemployment Rate",
    "PAYEMS":           "Total Nonfarm Payrolls",
    "ICSA":             "Initial Jobless Claims (Weekly)",

    # GDP & Output
    "GDP":              "Gross Domestic Product",
    "INDPRO":           "Industrial Production Index",

    # Consumer & Sentiment
    "RSAFS":            "Advance Retail Sales",
    "UMCSENT":          "Consumer Sentiment (UMich)",

    # Housing
    "HOUST":            "Housing Starts",
    "PERMIT":           "Building Permits",

    # Money Supply & Dollar
    "M2SL":             "M2 Money Supply",
    "DTWEXBGS":         "Trade-Weighted US Dollar Index",

    # Volatility
    "VIXCLS":           "CBOE VIX Volatility Index",
}


def collect():
    if not API_KEY:
        print("[fred] FRED_API_KEY not set — skipping.")
        return

    print("[fred] Collecting macroeconomic data from FRED...")

    db = get_db()
    ensure_schema(db)
    cursor = db.cursor()

    total_points = 0
    errors = 0

    for series_id, series_name in SERIES.items():
        try:
            resp = requests.get(BASE_URL, params={
                "series_id": series_id,
                "api_key": API_KEY,
                "file_type": "json",
                "sort_order": "desc",
                "limit": 60,       # last 60 observations
            }, timeout=15)

            resp.raise_for_status()
            data = resp.json()

            count = 0
            for obs in data.get("observations", []):
                val = obs.get("value", ".")
                if val == ".":     # FRED uses "." for missing/pending data
                    continue

                try:
                    cursor.execute("""
                        INSERT INTO macro_indicators
                            (series_id, series_name, date, value)
                        VALUES (?, ?, ?, ?)
                        ON CONFLICT(series_id, date) DO UPDATE SET
                            value = excluded.value,
                            updated_at = CURRENT_TIMESTAMP
                    """, (series_id, series_name, obs["date"], float(val)))
                    count += 1
                except (ValueError, Exception):
                    pass

            total_points += count
            print(f"  {series_id} ({series_name}): {count} data points")

            time.sleep(0.2)  # be gentle with the API

        except Exception as e:
            print(f"  [ERROR] {series_id}: {e}")
            errors += 1

    db.commit()
    db.close()
    print(f"[fred] Done. {total_points} data points updated, {errors} errors.")


if __name__ == "__main__":
    collect()
