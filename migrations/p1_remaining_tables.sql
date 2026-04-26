-- ============================================================
-- P1: Tables that were only in SQLite — now in PostgreSQL
-- Run AFTER p0_tables.sql
-- ============================================================

-- Full SEC EDGAR ticker universe (reference / lookup)
CREATE TABLE IF NOT EXISTS tickers (
    ticker          VARCHAR(16) PRIMARY KEY,
    company_name    TEXT,
    cik             VARCHAR(20),
    exchange        VARCHAR(50),
    sector          TEXT,
    industry        TEXT,
    description     TEXT,
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

-- News articles from all sources (RSS, NewsAPI, Reddit)
CREATE TABLE IF NOT EXISTS news (
    id              SERIAL PRIMARY KEY,
    source          TEXT NOT NULL,
    title           TEXT NOT NULL,
    url             TEXT UNIQUE,
    summary         TEXT,
    published_at    TEXT,
    collected_at    TIMESTAMPTZ DEFAULT NOW(),
    related_tickers TEXT,
    sentiment_score REAL
);

-- Macro & sentiment time-series (FRED, yfinance, CNN Fear & Greed)
CREATE TABLE IF NOT EXISTS macro_indicators (
    indicator    TEXT NOT NULL,
    series_id    TEXT,
    series_name  TEXT,
    date         DATE NOT NULL,
    value        REAL,
    source       TEXT,
    collected_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (indicator, date)
);

-- Commodity futures, index prices, FX rates (yfinance)
CREATE TABLE IF NOT EXISTS commodity_prices (
    id          SERIAL PRIMARY KEY,
    symbol      TEXT NOT NULL,
    name        TEXT,
    date        DATE NOT NULL,
    open        REAL,
    high        REAL,
    low         REAL,
    close       REAL,
    volume      BIGINT,
    updated_at  TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(symbol, date)
);

-- Individual stock price history
CREATE TABLE IF NOT EXISTS stock_prices (
    id          SERIAL PRIMARY KEY,
    ticker      VARCHAR(10) NOT NULL,
    date        DATE NOT NULL,
    open        REAL,
    high        REAL,
    low         REAL,
    close       REAL,
    adj_close   REAL,
    volume      BIGINT,
    updated_at  TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(ticker, date)
);

-- Company financials — Finviz screener snapshot (wiped + repopulated each run)
CREATE TABLE IF NOT EXISTS company_financials (
    ticker              VARCHAR(16) PRIMARY KEY,
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
    snapshot_date       DATE,
    collected_at        TIMESTAMPTZ DEFAULT NOW()
);

-- Major world events
CREATE TABLE IF NOT EXISTS world_events (
    id              SERIAL PRIMARY KEY,
    title           TEXT NOT NULL,
    description     TEXT,
    event_date      DATE,
    source          TEXT,
    url             TEXT,
    category        TEXT,
    impact_sectors  TEXT,
    collected_at    TIMESTAMPTZ DEFAULT NOW()
);

-- Collector run log
CREATE TABLE IF NOT EXISTS collection_log (
    id             SERIAL PRIMARY KEY,
    collector      TEXT NOT NULL,
    status         TEXT NOT NULL,
    records_added  INTEGER DEFAULT 0,
    error_message  TEXT,
    duration_sec   REAL,
    completed_at   TIMESTAMPTZ DEFAULT NOW()
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_tickers_cik         ON tickers(cik);
CREATE INDEX IF NOT EXISTS idx_news_source         ON news(source);
CREATE INDEX IF NOT EXISTS idx_news_collected       ON news(collected_at);
CREATE INDEX IF NOT EXISTS idx_news_published       ON news(published_at);
CREATE INDEX IF NOT EXISTS idx_macro_series         ON macro_indicators(series_id);
CREATE INDEX IF NOT EXISTS idx_macro_date           ON macro_indicators(date);
CREATE INDEX IF NOT EXISTS idx_commodity_symbol     ON commodity_prices(symbol);
CREATE INDEX IF NOT EXISTS idx_commodity_date       ON commodity_prices(date);
CREATE INDEX IF NOT EXISTS idx_stock_ticker_date    ON stock_prices(ticker, date);
CREATE INDEX IF NOT EXISTS idx_fin_sector           ON company_financials(sector);
CREATE INDEX IF NOT EXISTS idx_fin_industry         ON company_financials(industry);
CREATE INDEX IF NOT EXISTS idx_collection_log_time  ON collection_log(completed_at);
