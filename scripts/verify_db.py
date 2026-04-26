"""
Quick database health check. Run after collectors to see what's in the DB.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from db.connection import get_connection


def verify():
    conn = get_connection()
    cur = conn.cursor()

    print("=" * 60)
    print("DATABASE STATUS (PostgreSQL)")
    print("=" * 60)

    tables = {
        "ticker_master":        "Tracked Watchlist",
        "tickers":              "SEC Ticker Universe",
        "news":                 "News Articles",
        "macro_indicators":     "Macro Indicators",
        "commodity_prices":     "Commodity/Index Prices",
        "stock_prices":         "Stock Prices",
        "sec_filings":          "SEC Filings",
        "insider_trades":       "Insider Trades",
        "sec_financials":       "SEC Fundamentals",
        "company_profiles":     "Company Profiles",
        "company_financials":   "Company Financials (Finviz)",
        "world_events":         "World Events",
        "collection_log":       "Collection Log",
    }

    for table, label in tables.items():
        try:
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            count = cur.fetchone()[0]
        except Exception:
            conn.rollback()
            count = -1
        status = f"{count:,} rows" if count >= 0 else "TABLE NOT FOUND"
        print(f"\n{label} ({table}): {status}")

        if count <= 0:
            continue

        if table == "ticker_master":
            cur.execute("SELECT ticker FROM ticker_master ORDER BY ticker LIMIT 30")
            tickers = [r[0] for r in cur.fetchall()]
            print(f"    Tickers: {', '.join(tickers)}")

        if table == "news":
            cur.execute("""
                SELECT source, COUNT(*) as cnt FROM news
                GROUP BY source ORDER BY cnt DESC LIMIT 10
            """)
            for row in cur.fetchall():
                print(f"    {row[0]:40s}  {row[1]:>6,} articles")

        if table == "macro_indicators":
            cur.execute("""
                SELECT indicator, source, date, value
                FROM macro_indicators ORDER BY date DESC LIMIT 5
            """)
            for row in cur.fetchall():
                print(f"    {row[0]:25s} {(row[1] or ''):8s}  {row[2]}  {row[3]}")

        if table == "commodity_prices":
            cur.execute("""
                SELECT symbol, name, date, close
                FROM commodity_prices ORDER BY date DESC LIMIT 8
            """)
            for row in cur.fetchall():
                print(f"    {row[0]:12s} {row[1]:35s}  {row[2]}  {row[3]:.2f}")

        if table == "sec_filings":
            cur.execute("""
                SELECT form_type, COUNT(*) FROM sec_filings
                GROUP BY form_type ORDER BY COUNT(*) DESC
            """)
            for row in cur.fetchall():
                print(f"    {row[0]:15s}  {row[1]:>5} filings")

        if table == "insider_trades":
            cur.execute("""
                SELECT ticker, owner_name, transaction_type, shares, total_value, trade_date
                FROM insider_trades ORDER BY trade_date DESC LIMIT 5
            """)
            for row in cur.fetchall():
                val = f"${row[4]:,.0f}" if row[4] else "N/A"
                print(f"    {row[0]:6s} {(row[1] or '')[:25]:25s} {(row[2] or ''):6s} {row[3] or 0:>12,.0f} sh  {val:>14}  {row[5]}")

        if table == "company_financials":
            cur.execute("""
                SELECT sector, COUNT(*) FROM company_financials
                GROUP BY sector ORDER BY COUNT(*) DESC LIMIT 5
            """)
            for row in cur.fetchall():
                print(f"    {(row[0] or 'N/A'):30s}  {row[1]:>5} stocks")

        if table == "collection_log":
            cur.execute("""
                SELECT collector, status, records_added, completed_at
                FROM collection_log ORDER BY completed_at DESC LIMIT 8
            """)
            for row in cur.fetchall():
                print(f"    {row[0]:25s} {row[1]:4s}  {row[2]:>6} records  {row[3]}")

    cur.close()
    conn.close()
    print(f"\n{'=' * 60}")


if __name__ == "__main__":
    verify()
