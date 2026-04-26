-- ============================================================
-- FULLSCAN — UNIFIED DATABASE SCHEMA
-- ============================================================
-- Single source of truth.  All collectors write to data/pipeline.db.
-- Every statement uses IF NOT EXISTS so this is safe to run repeatedly.
-- ============================================================

-- Master list of all US-traded stocks (SEC EDGAR)
CREATE TABLE IF NOT EXISTS tickers (
    ticker          TEXT PRIMARY KEY,
    company_name    TEXT,
    cik             TEXT,
    exchange        TEXT,
    sector          TEXT,
    industry        TEXT,
    description     TEXT,
    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- News articles from all sources (RSS, NewsAPI, Reddit)
CREATE TABLE IF NOT EXISTS news (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    source          TEXT NOT NULL,
    title           TEXT NOT NULL,
    url             TEXT UNIQUE,
    summary         TEXT,
    published_at    TEXT,
    collected_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    related_tickers TEXT,
    sentiment_score REAL
);

-- Macro & sentiment time-series (FRED, yfinance, CNN Fear & Greed)
-- PRIMARY KEY (indicator, date) supports both:
--   macro_fred.py   → indicator = FRED series id  ("CPIAUCSL")
--   macro_sentiment  → indicator = human label     ("CPI", "VIX")
CREATE TABLE IF NOT EXISTS macro_indicators (
    indicator    TEXT NOT NULL,
    series_id    TEXT,
    series_name  TEXT,
    date         TEXT NOT NULL,
    value        REAL,
    source       TEXT,
    collected_at TEXT DEFAULT (datetime('now')),
    PRIMARY KEY (indicator, date)
);

-- Commodity futures, index prices, FX rates (yfinance)
CREATE TABLE IF NOT EXISTS commodity_prices (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol          TEXT NOT NULL,
    name            TEXT,
    date            TEXT NOT NULL,
    open            REAL,
    high            REAL,
    low             REAL,
    close           REAL,
    volume          INTEGER,
    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(symbol, date)
);

-- Individual stock price history
CREATE TABLE IF NOT EXISTS stock_prices (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker          TEXT NOT NULL,
    date            TEXT NOT NULL,
    open            REAL,
    high            REAL,
    low             REAL,
    close           REAL,
    adj_close       REAL,
    volume          INTEGER,
    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(ticker, date)
);

-- SEC filing metadata (8-K, 10-K, 10-Q, 13F, Form 4, etc.)
CREATE TABLE IF NOT EXISTS sec_filings (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    cik             TEXT,
    ticker          TEXT,
    company_name    TEXT,
    form_type       TEXT NOT NULL,
    filing_date     TEXT,
    url             TEXT UNIQUE,
    description     TEXT,
    collected_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Company financials — Finviz screener snapshot
-- Wiped and repopulated on each finviz_financials run
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

-- Major world events with concrete dates
CREATE TABLE IF NOT EXISTS world_events (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    title           TEXT NOT NULL,
    description     TEXT,
    event_date      TEXT,
    source          TEXT,
    url             TEXT,
    category        TEXT,
    impact_sectors  TEXT,
    collected_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Collector run log (used by macro_sentiment, finviz_financials, future collectors)
CREATE TABLE IF NOT EXISTS collection_log (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    collector      TEXT NOT NULL,
    status         TEXT NOT NULL,
    records_added  INTEGER DEFAULT 0,
    error_message  TEXT,
    duration_sec   REAL,
    completed_at   TEXT DEFAULT (datetime('now'))
);

-- ============================================================
-- INDEXES
-- ============================================================
CREATE INDEX IF NOT EXISTS idx_news_source      ON news(source);
CREATE INDEX IF NOT EXISTS idx_news_collected    ON news(collected_at);
CREATE INDEX IF NOT EXISTS idx_news_published    ON news(published_at);
CREATE INDEX IF NOT EXISTS idx_macro_series      ON macro_indicators(series_id);
CREATE INDEX IF NOT EXISTS idx_macro_date        ON macro_indicators(date);
CREATE INDEX IF NOT EXISTS idx_commodity_symbol  ON commodity_prices(symbol);
CREATE INDEX IF NOT EXISTS idx_stock_ticker      ON stock_prices(ticker);
CREATE INDEX IF NOT EXISTS idx_sec_type          ON sec_filings(form_type);
CREATE INDEX IF NOT EXISTS idx_sec_ticker        ON sec_filings(ticker);
CREATE INDEX IF NOT EXISTS idx_fin_sector        ON company_financials(sector);
CREATE INDEX IF NOT EXISTS idx_fin_industry      ON company_financials(industry);
