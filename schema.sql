-- schema.sql
-- Run this against your database to create all required tables.

-- ════════════════════════════════════════════════════════
-- TICKER MASTER — the source of truth for tracked symbols
-- ════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS ticker_master (
    ticker          VARCHAR(16) PRIMARY KEY,
    company_name    TEXT,
    is_active       BOOLEAN DEFAULT TRUE,
    added_at        TIMESTAMPTZ DEFAULT NOW()
);

-- ════════════════════════════════════════════════════════
-- COMPANY PROFILES — populated by company_profiles_collector
-- ════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS company_profiles (
    ticker              VARCHAR(16) PRIMARY KEY,
    company_name        TEXT,
    legal_name          TEXT,
    description         TEXT,
    ceo                 TEXT,
    sector              TEXT,
    industry            TEXT,
    exchange            VARCHAR(32),
    country             VARCHAR(64),
    website             TEXT,
    ipo_date            DATE,
    employees           INTEGER,
    market_cap          BIGINT,
    beta                DOUBLE PRECISION,
    is_etf              BOOLEAN DEFAULT FALSE,
    is_actively_trading BOOLEAN DEFAULT TRUE,
    cik                 VARCHAR(20),
    isin                VARCHAR(20),
    cusip               VARCHAR(20),
    source              VARCHAR(32),
    collected_at        TIMESTAMPTZ,
    updated_at          TIMESTAMPTZ
);

-- ════════════════════════════════════════════════════════
-- SEC FILINGS — populated by sec_filings_collector (8-K, Form 4 metadata)
-- ════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS sec_filings (
    id                  SERIAL PRIMARY KEY,
    ticker              VARCHAR(16) NOT NULL,
    company_name        TEXT,
    cik                 VARCHAR(20),
    form_type           VARCHAR(16),
    filing_date         DATE,
    accepted_date       TEXT,
    accession_number    TEXT UNIQUE NOT NULL,
    filing_url          TEXT,
    items_reported      TEXT[],
    items_description   TEXT,
    has_press_release   BOOLEAN DEFAULT FALSE,
    source              VARCHAR(32),
    collected_at        TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_sec_filings_ticker ON sec_filings (ticker);
CREATE INDEX IF NOT EXISTS idx_sec_filings_date   ON sec_filings (filing_date);

-- ════════════════════════════════════════════════════════
-- INSIDER TRADES — populated by sec_filings_collector (Form 4 transactions)
-- ════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS insider_trades (
    id                  SERIAL PRIMARY KEY,
    ticker              VARCHAR(16) NOT NULL,
    filing_date         DATE,
    trade_date          DATE,
    accession_number    TEXT,
    owner_name          TEXT,
    owner_relationship  TEXT,
    transaction_code    VARCHAR(4),
    transaction_type    VARCHAR(32),
    shares              DOUBLE PRECISION,
    price_per_share     DOUBLE PRECISION,
    total_value         DOUBLE PRECISION,
    shares_owned_after  DOUBLE PRECISION,
    direct_or_indirect  VARCHAR(2),
    source              VARCHAR(32),
    collected_at        TIMESTAMPTZ,
    UNIQUE (ticker, accession_number, owner_name, trade_date, transaction_code, shares)
);

CREATE INDEX IF NOT EXISTS idx_insider_trades_ticker ON insider_trades (ticker);
CREATE INDEX IF NOT EXISTS idx_insider_trades_date   ON insider_trades (trade_date);

-- ════════════════════════════════════════════════════════
-- SEC FINANCIALS — populated by sec_fundamentals_collector (10-K, 10-Q)
-- ════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS sec_financials (
    id                      SERIAL PRIMARY KEY,
    ticker                  VARCHAR(16) NOT NULL,
    period_type             VARCHAR(16) NOT NULL,       -- 'annual' or 'quarterly'
    period_end_date         DATE NOT NULL,
    fiscal_year             INTEGER,
    fiscal_quarter          INTEGER,
    revenue                 BIGINT,
    cost_of_revenue         BIGINT,
    gross_profit            BIGINT,
    research_and_dev        BIGINT,
    selling_general_admin   BIGINT,
    operating_expense       BIGINT,
    operating_income        BIGINT,
    interest_expense        BIGINT,
    pretax_income           BIGINT,
    income_tax              BIGINT,
    net_income              BIGINT,
    eps_basic               DOUBLE PRECISION,
    eps_diluted             DOUBLE PRECISION,
    cash_and_equivalents    BIGINT,
    short_term_investments  BIGINT,
    total_current_assets    BIGINT,
    total_assets            BIGINT,
    total_current_liabilities BIGINT,
    long_term_debt          BIGINT,
    total_liabilities       BIGINT,
    stockholders_equity     BIGINT,
    operating_cash_flow     BIGINT,
    capital_expenditure     BIGINT,
    free_cash_flow          BIGINT,
    dividends_paid          BIGINT,
    shares_outstanding      BIGINT,
    filing_accession        TEXT,
    source                  VARCHAR(32),
    collected_at            TIMESTAMPTZ,
    UNIQUE (ticker, period_type, period_end_date)
);

CREATE INDEX IF NOT EXISTS idx_sec_financials_ticker ON sec_financials (ticker);
CREATE INDEX IF NOT EXISTS idx_sec_financials_period ON sec_financials (period_end_date);

-- ════════════════════════════════════════════════════════
-- SEED ticker_master with an initial watchlist
-- ════════════════════════════════════════════════════════
INSERT INTO ticker_master (ticker, company_name) VALUES
    ('AAPL',  'Apple Inc.'),
    ('MSFT',  'Microsoft Corporation'),
    ('GOOGL', 'Alphabet Inc.'),
    ('AMZN',  'Amazon.com Inc.'),
    ('NVDA',  'NVIDIA Corporation'),
    ('META',  'Meta Platforms Inc.'),
    ('TSLA',  'Tesla Inc.'),
    ('JPM',   'JPMorgan Chase & Co.'),
    ('V',     'Visa Inc.'),
    ('UNH',   'UnitedHealth Group Inc.'),
    ('JNJ',   'Johnson & Johnson'),
    ('WMT',   'Walmart Inc.'),
    ('MA',    'Mastercard Inc.'),
    ('PG',    'Procter & Gamble Co.'),
    ('HD',    'Home Depot Inc.'),
    ('XOM',   'Exxon Mobil Corporation'),
    ('BAC',   'Bank of America Corp.'),
    ('COST',  'Costco Wholesale Corp.'),
    ('ABBV',  'AbbVie Inc.'),
    ('CRM',   'Salesforce Inc.')
ON CONFLICT (ticker) DO NOTHING;
