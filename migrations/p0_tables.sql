-- ============================================================
-- P0 TABLES: Company Profiles, Financials, Filings, Insider Trades
-- ============================================================

-- 1. Company Profiles (from FMP)
CREATE TABLE IF NOT EXISTS company_profiles (
    id              SERIAL PRIMARY KEY,
    ticker          VARCHAR(10) NOT NULL UNIQUE,
    company_name    TEXT,
    legal_name      TEXT,
    description     TEXT,
    ceo             TEXT,
    sector          TEXT,
    industry        TEXT,
    exchange        VARCHAR(50),
    country         VARCHAR(10),
    website         TEXT,
    ipo_date        DATE,
    employees       INTEGER,
    market_cap      BIGINT,
    beta            DECIMAL(8,4),
    is_etf          BOOLEAN DEFAULT FALSE,
    is_actively_trading BOOLEAN DEFAULT TRUE,
    cik             VARCHAR(20),
    isin            VARCHAR(20),
    cusip           VARCHAR(20),
    source          VARCHAR(30) DEFAULT 'fmp',
    collected_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 2. Historical Financials (from SEC EDGAR)
CREATE TABLE IF NOT EXISTS sec_financials (
    id                      SERIAL PRIMARY KEY,
    ticker                  VARCHAR(10) NOT NULL,
    period_type             VARCHAR(12) NOT NULL,
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
    eps_basic               DECIMAL(12,4),
    eps_diluted             DECIMAL(12,4),
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
    filing_accession        VARCHAR(30),
    source                  VARCHAR(30) DEFAULT 'edgartools',
    collected_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(ticker, period_type, period_end_date)
);

-- 3. SEC Filing Events (8-K, 10-K, 10-Q, Form 4 metadata)
CREATE TABLE IF NOT EXISTS sec_filings (
    id                  SERIAL PRIMARY KEY,
    ticker              VARCHAR(10),
    company_name        TEXT,
    cik                 VARCHAR(20),
    form_type           VARCHAR(20) NOT NULL,
    filing_date         DATE,
    accepted_date       TIMESTAMP,
    accession_number    VARCHAR(30) NOT NULL UNIQUE,
    filing_url          TEXT,
    items_reported      TEXT[],
    items_description   TEXT,
    has_press_release   BOOLEAN DEFAULT FALSE,
    source              VARCHAR(30) DEFAULT 'edgartools',
    collected_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 4. Insider Trades (Form 4)
CREATE TABLE IF NOT EXISTS insider_trades (
    id                  SERIAL PRIMARY KEY,
    ticker              VARCHAR(10) NOT NULL,
    filing_date         DATE,
    trade_date          DATE,
    accession_number    VARCHAR(30),
    owner_name          TEXT,
    owner_relationship  TEXT,
    transaction_code    VARCHAR(5),
    transaction_type    VARCHAR(20),
    shares              DECIMAL(16,4),
    price_per_share     DECIMAL(12,4),
    total_value         DECIMAL(18,2),
    shares_owned_after  DECIMAL(16,4),
    direct_or_indirect  VARCHAR(1),
    source              VARCHAR(30) DEFAULT 'edgartools',
    collected_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(ticker, accession_number, owner_name, trade_date, transaction_code, shares)
);

-- INDEXES
CREATE INDEX IF NOT EXISTS idx_sec_fin_ticker ON sec_financials(ticker);
CREATE INDEX IF NOT EXISTS idx_sec_fin_period ON sec_financials(ticker, period_type, period_end_date DESC);
CREATE INDEX IF NOT EXISTS idx_sec_filings_ticker ON sec_filings(ticker);
CREATE INDEX IF NOT EXISTS idx_sec_filings_date ON sec_filings(filing_date DESC);
CREATE INDEX IF NOT EXISTS idx_sec_filings_form ON sec_filings(form_type);
CREATE INDEX IF NOT EXISTS idx_insider_ticker ON insider_trades(ticker);
CREATE INDEX IF NOT EXISTS idx_insider_date ON insider_trades(filing_date DESC);
CREATE INDEX IF NOT EXISTS idx_insider_owner ON insider_trades(owner_name);
CREATE INDEX IF NOT EXISTS idx_profiles_ticker ON company_profiles(ticker);
