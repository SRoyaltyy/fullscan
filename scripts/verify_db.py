"""
Quick database health check. Run after collectors to see what's in the DB.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from db.db_utils import get_db


def verify():
    db = get_db()
    cursor = db.cursor()

    print("=" * 60)
    print("DATABASE STATUS")
    print("=" * 60)

    tables = {
        "tickers":              "Ticker Master List",
        "news":                 "News Articles",
        "macro_indicators":     "Macro Indicators (FRED)",
        "commodity_prices":     "Commodity/Index Prices",
        "stock_prices":         "Stock Prices",
        "sec_filings":          "SEC Filings",
        "company_financials":   "Company Financials",
        "world_events":         "World Events",
    }

    for table, label in tables.items():
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        print(f"\n{label} ({table}): {count:,} rows")

        if count > 0 and table == "news":
            # Show source breakdown for news
            cursor.execute("""
                SELECT source, COUNT(*) as cnt
                FROM news
                GROUP BY source
                ORDER BY cnt DESC
                LIMIT 10
            """)
            for row in cursor.fetchall():
                print(f"    {row[0]:40s}  {row[1]:>6,} articles")

        if count > 0 and table == "macro_indicators":
            cursor.execute("""
                SELECT series_id, series_name, date, value
                FROM macro_indicators
                ORDER BY date DESC
                LIMIT 5
            """)
            for row in cursor.fetchall():
                print(f"    {row[0]:15s} {row[1]:35s}  {row[2]}  {row[3]}")

        if count > 0 and table == "commodity_prices":
            cursor.execute("""
                SELECT symbol, name, date, close
                FROM commodity_prices
                ORDER BY date DESC
                LIMIT 8
            """)
            for row in cursor.fetchall():
                print(f"    {row[0]:12s} {row[1]:35s}  {row[2]}  {row[3]:.2f}")

    db.close()
    print(f"\n{'=' * 60}")


if __name__ == "__main__":
    verify()
