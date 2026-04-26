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
        "macro_indicators":     "Macro Indicators",
        "commodity_prices":     "Commodity/Index Prices",
        "stock_prices":         "Stock Prices",
        "sec_filings":          "SEC Filings",
        "company_financials":   "Company Financials (Finviz)",
        "world_events":         "World Events",
        "collection_log":       "Collection Log",
    }

    for table, label in tables.items():
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
        except Exception:
            count = -1  # table doesn't exist yet
        print(f"\n{label} ({table}): {count:,} rows")

        if count <= 0:
            continue

        if table == "news":
            cursor.execute("""
                SELECT source, COUNT(*) as cnt FROM news
                GROUP BY source ORDER BY cnt DESC LIMIT 10
            """)
            for row in cursor.fetchall():
                print(f"    {row[0]:40s}  {row[1]:>6,} articles")

        if table == "macro_indicators":
            cursor.execute("""
                SELECT indicator, source, date, value
                FROM macro_indicators ORDER BY date DESC LIMIT 5
            """)
            for row in cursor.fetchall():
                print(f"    {row[0]:25s} {(row[1] or ''):8s}  {row[2]}  {row[3]}")

        if table == "commodity_prices":
            cursor.execute("""
                SELECT symbol, name, date, close
                FROM commodity_prices ORDER BY date DESC LIMIT 8
            """)
            for row in cursor.fetchall():
                print(f"    {row[0]:12s} {row[1]:35s}  {row[2]}  {row[3]:.2f}")

        if table == "company_financials":
            cursor.execute("""
                SELECT sector, COUNT(*) FROM company_financials
                GROUP BY sector ORDER BY COUNT(*) DESC LIMIT 5
            """)
            for row in cursor.fetchall():
                print(f"    {(row[0] or 'N/A'):30s}  {row[1]:>5} stocks")

        if table == "collection_log":
            cursor.execute("""
                SELECT collector, status, records_added, completed_at
                FROM collection_log ORDER BY completed_at DESC LIMIT 5
            """)
            for row in cursor.fetchall():
                print(f"    {row[0]:20s} {row[1]:4s}  {row[2]:>6} records  {row[3]}")

    db.close()
    print(f"\n{'=' * 60}")


if __name__ == "__main__":
    verify()
