"""
Run all collectors sequentially.
Single command to populate the entire database.
"""

import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from collectors import (
    ticker_master,
    rss_news,
    news_newsapi,
    news_reddit,
    macro_fred,
    macro_sentiment,
    market_yfinance,
    finviz_financials,
)

# PostgreSQL-native collectors
from collectors import sec_filings_collector
from collectors import company_profiles_collector
from collectors import sec_fundamentals_collector


def main():
    collectors = [
        ("Ticker Master List (SEC Universe)",   ticker_master.collect),
        ("RSS News",                            rss_news.collect),
        ("NewsAPI",                             news_newsapi.collect),
        ("Reddit",                              news_reddit.collect),
        ("FRED Macro Data",                     macro_fred.collect),
        ("Macro & Sentiment",                   macro_sentiment.main),
        ("yfinance Market",                     market_yfinance.collect),
        ("Finviz Financials",                   finviz_financials.main),
        ("Company Profiles (FMP)",              company_profiles_collector.main),
        ("SEC Filings & Insider Trades",        sec_filings_collector.main),
        ("SEC Fundamentals (EDGAR)",            sec_fundamentals_collector.main),
    ]

    print("=" * 60)
    print("FULLSCAN — Full Collection Run")
    print("=" * 60)

    total_start = time.time()
    results = []

    for name, func in collectors:
        print(f"\n{'─' * 60}")
        print(f"▶ {name}")
        print(f"{'─' * 60}")
        start = time.time()
        try:
            func()
            results.append((name, "OK", time.time() - start))
        except Exception as e:
            results.append((name, f"FAILED: {e}", time.time() - start))
            print(f"  [FATAL] {name} crashed: {e}")

    total_duration = time.time() - total_start

    print(f"\n{'=' * 60}")
    print("SUMMARY")
    print(f"{'=' * 60}")
    for name, status, dur in results:
        print(f"  {name:<45} {status:<10} ({dur:.1f}s)")
    print(f"\n  Total time: {total_duration:.1f}s")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
