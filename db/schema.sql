-- ============================================================
-- STOCK CATALYST ENGINE — DATABASE SCHEMA
-- ============================================================

-- Master list of all US-traded stocks
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

-- News articles from all sources (RSS, NewsAPI, Reddit, etc.)
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

-- Macroeconomic indicators from FRED
CREATE TABLE IF NOT EXISTS macro_indicators (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    series_id       TEXT NOT NULL,
    series_name     TEXT,
    date            TEXT NOT NULL,
    value           REAL,
    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(series_id, date)
);

-- Commodity prices, index prices, futures
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

-- Company financial fundamentals
CREATE TABLE IF NOT EXISTS company_financials (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker              TEXT NOT NULL,
    period              TEXT NOT NULL,
    revenue             REAL,
    net_income          REAL,
    ebitda              REAL,
    eps                 REAL,
    total_assets        REAL,
    total_liabilities   REAL,
    total_debt          REAL,
    cash_and_equivalents REAL,
    free_cash_flow      REAL,
    updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(ticker, period)
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

-- Indexes for fast lookups
CREATE INDEX IF NOT EXISTS idx_news_source ON news(source);
CREATE INDEX IF NOT EXISTS idx_news_collected ON news(collected_at);
CREATE INDEX IF NOT EXISTS idx_news_published ON news(published_at);
CREATE INDEX IF NOT EXISTS idx_macro_series ON macro_indicators(series_id);
CREATE INDEX IF NOT EXISTS idx_commodity_symbol ON commodity_prices(symbol);
CREATE INDEX IF NOT EXISTS idx_stock_prices_ticker ON stock_prices(ticker);
CREATE INDEX IF NOT EXISTS idx_sec_filings_type ON sec_filings(form_type);
CREATE INDEX IF NOT EXISTS idx_sec_filings_ticker ON sec_filings(ticker);
CREATE INDEX IF NOT EXISTS idx_financials_ticker ON company_financials(ticker);
