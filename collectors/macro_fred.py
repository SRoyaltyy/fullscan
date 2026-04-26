"""
Collector: FRED Macroeconomic Data
Source: Federal Reserve Economic Data (FRED) API
Schedule: Daily
Populates: macro_indicators table
"""

import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import requests
from db.connection import get_connection

API_KEY = os.environ.get("FRED_API_KEY", "")
BASE_URL = "https://api.stlouisfed.org/fred/series/observations"

SERIES = {
    "CPIAUCSL":         "CPI — All Urban Consumers",
    "CPILFESL":         "Core CPI (ex Food & Energy)",
    "PCEPI":            "PCE Price Index",
    "PPIACO":           "PPI — All Commodities",
    "FEDFUNDS":         "Federal Funds Effective Rate",
    "DFF":              "Federal Funds Daily Rate",
    "DGS2":             "2-Year Treasury Yield",
    "DGS10":            "10-Year Treasury Yield",
    "DGS30":            "30-Year Treasury Yield",
    "T10Y2Y":           "10Y–2Y Treasury Spread",
    "T10Y3M":           "10Y–3M Treasury Spread",
    "MORTGAGE30US":     "30-Year Fixed Mortgage Rate",
    "UNRATE":           "Unemployment Rate",
    "PAYEMS":           "Total Nonfarm Payrolls",
    "ICSA":             "Initial Jobless Claims (Weekly)",
    "GDP":              "Gross Domestic Product",
    "INDPRO":           "Industrial Production Index",
    "RSAFS":            "Advance Retail Sales",
    "UMCSENT":          "Consumer Sentiment (UMich)",
    "HOUST":            "Housing Starts",
    "PERMIT":           "Building Permits",
    "M2SL":             "M2 Money Supply",
    "DTWEXBGS":         "Trade-Weighted US Dollar Index",
    "VIXCLS":           "CBOE VIX Volatility Index",
}


def collect():
    if not API_KEY:
        print("[fred] FRED_API_KEY not set — skipping.")
        return

    print("[fred] Collecting macroeconomic data from FRED...")

    conn = get_connection()
    cur = conn.cursor()

    total_points = 0
    errors = 0

    for series_id, series_name in SERIES.items():
        try:
            resp = requests.get(BASE_URL, params={
                "series_id": series_id,
                "api_key": API_KEY,
                "file_type": "json",
                "sort_order": "desc",
                "limit": 60,
            }, timeout=15)

            resp.raise_for_status()
            data = resp.json()

            count = 0
            for obs in data.get("observations", []):
                val = obs.get("value", ".")
                if val == ".":
                    continue

                try:
                    cur.execute("""
                        INSERT INTO macro_indicators
                            (indicator, series_id, series_name, date, value, source)
                        VALUES (%s, %s, %s, %s, %s, 'FRED')
                        ON CONFLICT (indicator, date) DO UPDATE SET
                            value = EXCLUDED.value,
                            collected_at = NOW()
                    """, (series_id, series_id, series_name, obs["date"], float(val)))
                    count += 1
                except (ValueError, Exception):
                    conn.rollback()

            total_points += count
            print(f"  {series_id} ({series_name}): {count} data points")

            time.sleep(0.2)

        except Exception as e:
            print(f"  [ERROR] {series_id}: {e}")
            errors += 1

    conn.commit()

    cur.execute(
        "INSERT INTO collection_log(collector, status, records_added) VALUES(%s,%s,%s)",
        ("macro_fred", "ok", total_points),
    )
    conn.commit()

    cur.close()
    conn.close()
    print(f"[fred] Done. {total_points} data points updated, {errors} errors.")


if __name__ == "__main__":
    collect()
