#!/usr/bin/env python3
"""Initialize the fullscan database schema. Idempotent — safe to run repeatedly."""

import os
import sqlite3
from pathlib import Path

DB_PATH = os.environ.get("DB_PATH", str(Path(__file__).parent.parent / "data" / "fullscan.db"))


def init():
    Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.executescript("""
        PRAGMA journal_mode=WAL;

        -- ========== MACRO / SENTIMENT TIME SERIES ==========
        CREATE TABLE IF NOT EXISTS macro_indicators (
            indicator   TEXT NOT NULL,
            series_id   TEXT,
            date        TEXT NOT NULL,
            value       REAL,
            source      TEXT NOT NULL,
            collected_at TEXT DEFAULT (datetime('now')),
            PRIMARY KEY (indicator, date)
        );
        CREATE INDEX IF NOT EXISTS idx_macro_date ON macro_indicators(date);

        -- ========== COMPANY FINANCIALS (latest Finviz snapshot) ==========
        CREATE TABLE IF NOT EXISTS company_financials (
            ticker              TEXT PRIMARY KEY,
            company             TEXT,
            sector              TEXT,
            industry            TEXT,
            country             TEXT,
            market_cap          REAL,
            pe                  REAL,
            forward_pe          REAL,
            peg                 REAL,
            ps                  REAL,
            pb                  REAL,
            p_cash              REAL,
            p_fcf               REAL,
            dividend_yield      REAL,
            payout_ratio        REAL,
            eps                 REAL,
            eps_growth_this_y   REAL,
            eps_growth_next_y   REAL,
            eps_growth_past_5y  REAL,
            eps_growth_next_5y  REAL,
            sales_growth_past_5y REAL,
            eps_growth_qoq      REAL,
            sales_growth_qoq    REAL,
            shares_outstanding  REAL,
            float_shares        REAL,
            insider_own         REAL,
            insider_trans       REAL,
            inst_own            REAL,
            inst_trans          REAL,
            float_short         REAL,
            short_ratio         REAL,
            roa                 REAL,
            roe                 REAL,
            roi                 REAL,
            current_ratio       REAL,
            quick_ratio         REAL,
            lt_debt_equity      REAL,
            debt_equity         REAL,
            gross_margin        REAL,
            oper_margin         REAL,
            profit_margin       REAL,
            perf_week           REAL,
            perf_month          REAL,
            perf_quarter        REAL,
            perf_half_y         REAL,
            perf_year           REAL,
            perf_ytd            REAL,
            volatility_week     REAL,
            volatility_month    REAL,
            sma20               REAL,
            sma50               REAL,
            sma200              REAL,
            high_50d            REAL,
            low_50d             REAL,
            high_52w            REAL,
            low_52w             REAL,
            rsi                 REAL,
            change_from_open    REAL,
            gap                 REAL,
            analyst_recom       REAL,
            avg_volume          REAL,
            rel_volume          REAL,
            price               REAL,
            change              REAL,
            volume              REAL,
            earnings_date       TEXT,
            target_price        REAL,
            ipo_date            TEXT,
            beta                REAL,
            atr                 REAL,
            snapshot_date       TEXT,
            collected_at        TEXT DEFAULT (datetime('now'))
        );
        CREATE INDEX IF NOT EXISTS idx_fin_sector ON company_financials(sector);

        -- ========== NEWS ARTICLES (all sources) ==========
        CREATE TABLE IF NOT EXISTS news_articles (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            title        TEXT NOT NULL,
            url          TEXT UNIQUE,
            source       TEXT,
            published_at TEXT,
            summary      TEXT,
            content      TEXT,
            tickers      TEXT,          -- JSON array, e.g. ["AAPL","MSFT"]
            sentiment    REAL,          -- placeholder for Phase 3
            collector    TEXT,          -- rss | newsapi | reddit
            collected_at TEXT DEFAULT (datetime('now'))
        );
        CREATE INDEX IF NOT EXISTS idx_news_pub  ON news_articles(published_at);
        CREATE INDEX IF NOT EXISTS idx_news_coll ON news_articles(collector);

        -- ========== COLLECTION LOG ==========
        CREATE TABLE IF NOT EXISTS collection_log (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            collector      TEXT NOT NULL,
            status         TEXT NOT NULL,
            records_added  INTEGER DEFAULT 0,
            error_message  TEXT,
            duration_sec   REAL,
            completed_at   TEXT DEFAULT (datetime('now'))
        );
    """)
    conn.close()
    print(f"Database ready at {DB_PATH}")


if __name__ == "__main__":
    init()
