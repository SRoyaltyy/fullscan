"""
Run all collectors sequentially. Useful for:
  - Initial data load (populating the DB from scratch)
  - Local testing before pushing to GitHub Actions
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from collectors import (
    ticker_master,
    news_rss,
    news_newsapi,
    news_reddit,
    macro_fred,
    macro_sentiment,
    market_yfinance,
    sec_filings,
    finviz_financials,
)


def main():
    collectors = [
        ("Ticker Master List",      ticker_master.collect),
        ("RSS News",                news_rss.collect),
        ("NewsAPI",                 news_newsapi.collect),
        ("Reddit",                  news_reddit.collect),
        ("FRED Macro Data",         macro_fred.collect),
        ("Macro & Sentiment",       macro_sentiment.main),
        ("yfinance Market",         market_yfinance.collect),
        ("SEC Filings",             sec_filings.collect),
        ("Finviz Financials",       finviz_financials.main),
    ]

    print("=" * 60)
    print("FULLSCAN — Full Collection Run")
    print("=" * 60)

    for name, func in collectors:
        print(f"\n{'─' * 60}")
        print(f"▶ {name}")
        print(f"{'─' * 60}")
        try:
            func()
        except Exception as e:
            print(f"  [FATAL] {name} crashed: {e}")

    print(f"\n{'=' * 60}")
    print("All collectors finished.")
    print("=" * 60)


if __name__ == "__main__":
    main()
