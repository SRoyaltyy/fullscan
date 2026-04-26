#!/usr/bin/env python3
"""
sec_fundamentals_collector.py
Pulls historical financial statements from SEC EDGAR via EdgarTools.
Populates: sec_financials table.
Schedule: Weekly
"""

import os
import sys
import time
import logging
import psycopg2
import pandas as pd
from datetime import datetime, date

# ── EdgarTools Setup ──────────────────────────────────
os.environ["EDGAR_IDENTITY"] = os.getenv("EDGAR_IDENTITY", "TradingBot bot@example.com")

from edgar import Company, set_identity
set_identity(os.getenv("EDGAR_IDENTITY", "TradingBot bot@example.com"))

# ── CONFIG ────────────────────────────────────────────
DB_HOST         = os.getenv("DB_HOST", "localhost")
DB_PORT         = os.getenv("DB_PORT", "5432")
DB_NAME         = os.getenv("DB_NAME", "trading_bot")
DB_USER         = os.getenv("DB_USER", "postgres")
DB_PASSWORD     = os.getenv("DB_PASSWORD", "")

ANNUAL_FILINGS_TO_PULL  = 5
QUARTERLY_FILINGS_TO_PULL = 12
DELAY_BETWEEN_TICKERS = 2.0

# ── LOGGING ───────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# ── DATABASE ──────────────────────────────────────────
def get_db_connection():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASSWORD
    )

def get_tracked_tickers(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT ticker FROM ticker_master ORDER BY ticker")
        return [row[0] for row in cur.fetchall()]

def get_existing_periods(conn, ticker):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT period_type, period_end_date FROM sec_financials WHERE ticker = %s",
            (ticker,)
        )
        return {(row[0], row[1]) for row in cur.fetchall()}

def upsert_financial_row(conn, row):
    sql = """
        INSERT INTO sec_financials (
            ticker, period_type, period_end_date, fiscal_year, fiscal_quarter,
            revenue, cost_of_revenue, gross_profit,
            research_and_dev, selling_general_admin, operating_expense, operating_income,
            interest_expense, pretax_income, income_tax, net_income,
            eps_basic, eps_diluted,
            cash_and_equivalents, short_term_investments,
            total_current_assets, total_assets,
            total_current_liabilities, long_term_debt, total_liabilities,
            stockholders_equity,
            operating_cash_flow, capital_expenditure, free_cash_flow, dividends_paid,
            shares_outstanding,
            filing_accession, source, collected_at
        ) VALUES (
            %(ticker)s, %(period_type)s, %(period_end_date)s, %(fiscal_year)s, %(fiscal_quarter)s,
            %(revenue)s, %(cost_of_revenue)s, %(gross_profit)s,
            %(research_and_dev)s, %(selling_general_admin)s, %(operating_expense)s, %(operating_income)s,
            %(interest_expense)s, %(pretax_income)s, %(income_tax)s, %(net_income)s,
            %(eps_basic)s, %(eps_diluted)s,
            %(cash_and_equivalents)s, %(short_term_investments)s,
            %(total_current_assets)s, %(total_assets)s,
            %(total_current_liabilities)s, %(long_term_debt)s, %(total_liabilities)s,
            %(stockholders_equity)s,
            %(operating_cash_flow)s, %(capital_expenditure)s, %(free_cash_flow)s, %(dividends_paid)s,
            %(shares_outstanding)s,
            %(filing_accession)s, %(source)s, %(collected_at)s
        )
        ON CONFLICT (ticker, period_type, period_end_date) DO UPDATE SET
            revenue                 = EXCLUDED.revenue,
            cost_of_revenue         = EXCLUDED.cost_of_revenue,
            gross_profit            = EXCLUDED.gross_profit,
            research_and_dev        = EXCLUDED.research_and_dev,
            selling_general_admin   = EXCLUDED.selling_general_admin,
            operating_expense       = EXCLUDED.operating_expense,
            operating_income        = EXCLUDED.operating_income,
            interest_expense        = EXCLUDED.interest_expense,
            pretax_income           = EXCLUDED.pretax_income,
            income_tax              = EXCLUDED.income_tax,
            net_income              = EXCLUDED.net_income,
            eps_basic               = EXCLUDED.eps_basic,
            eps_diluted             = EXCLUDED.eps_diluted,
            cash_and_equivalents    = EXCLUDED.cash_and_equivalents,
            short_term_investments  = EXCLUDED.short_term_investments,
            total_current_assets    = EXCLUDED.total_current_assets,
            total_assets            = EXCLUDED.total_assets,
            total_current_liabilities = EXCLUDED.total_current_liabilities,
            long_term_debt          = EXCLUDED.long_term_debt,
            total_liabilities       = EXCLUDED.total_liabilities,
            stockholders_equity     = EXCLUDED.stockholders_equity,
            operating_cash_flow     = EXCLUDED.operating_cash_flow,
            capital_expenditure     = EXCLUDED.capital_expenditure,
            free_cash_flow          = EXCLUDED.free_cash_flow,
            dividends_paid          = EXCLUDED.dividends_paid,
            shares_outstanding      = EXCLUDED.shares_outstanding,
            filing_accession        = EXCLUDED.filing_accession,
            collected_at            = EXCLUDED.collected_at
    """
    with conn.cursor() as cur:
        cur.execute(sql, row)
    conn.commit()

# ── LABEL MAPS ────────────────────────────────────────
INCOME_LABEL_MAP = {
    "revenue":              ["Revenue", "Revenues", "Total Revenue", "Net Revenue",
                             "Sales Revenue", "Net Sales"],
    "cost_of_revenue":      ["Cost of Revenue", "Cost of Goods Sold", "Cost Of Revenue",
                             "Cost of Sales", "Cost of Goods and Services Sold"],
    "gross_profit":         ["Gross Profit"],
    "research_and_dev":     ["Research and Development", "Research And Development Expense",
                             "Research & Development"],
    "selling_general_admin":["Selling, General and Administrative", "Selling General And Administrative",
                             "SG&A", "Selling, General & Administrative"],
    "operating_expense":    ["Operating Expenses", "Total Operating Expenses", "Operating Expense"],
    "operating_income":     ["Operating Income", "Operating Income (Loss)",
                             "Income from Operations", "Income From Operations"],
    "interest_expense":     ["Interest Expense", "Interest Expense, Net"],
    "pretax_income":        ["Income Before Tax", "Income Before Income Taxes",
                             "Pretax Income", "Income Before Taxes"],
    "income_tax":           ["Income Tax Expense", "Income Tax", "Provision for Income Taxes"],
    "net_income":           ["Net Income", "Net Income (Loss)", "Net Income Loss",
                             "Net Income Attributable to Common Stockholders"],
    "eps_basic":            ["EPS (Basic)", "Basic EPS", "Earnings Per Share, Basic", "EPS Basic"],
    "eps_diluted":          ["EPS (Diluted)", "Diluted EPS", "Earnings Per Share, Diluted", "EPS Diluted"],
}

BALANCE_LABEL_MAP = {
    "cash_and_equivalents":     ["Cash and Cash Equivalents", "Cash And Cash Equivalents",
                                 "Cash", "Cash and Short-term Investments"],
    "short_term_investments":   ["Short-term Investments", "Short Term Investments",
                                 "Marketable Securities Current"],
    "total_current_assets":     ["Total Current Assets", "Current Assets"],
    "total_assets":             ["Total Assets", "Assets"],
    "total_current_liabilities":["Total Current Liabilities", "Current Liabilities"],
    "long_term_debt":           ["Long-term Debt", "Long Term Debt",
                                 "Non-current Long Term Debt", "Long-Term Debt"],
    "total_liabilities":        ["Total Liabilities", "Liabilities"],
    "stockholders_equity":      ["Total Stockholders' Equity", "Stockholders' Equity",
                                 "Total Equity", "Stockholders Equity",
                                 "Total Stockholders Equity"],
}

CASHFLOW_LABEL_MAP = {
    "operating_cash_flow":  ["Operating Cash Flow", "Net Cash from Operating Activities",
                             "Net Cash Provided by Operating Activities", "Cash from Operations"],
    "capital_expenditure":  ["Capital Expenditure", "Capital Expenditures",
                             "Purchases of Property and Equipment",
                             "Purchase of Property, Plant and Equipment"],
    "free_cash_flow":       ["Free Cash Flow"],
    "dividends_paid":       ["Dividends Paid", "Payment of Dividends", "Payments of Dividends"],
}

# ── EXTRACTION HELPERS ────────────────────────────────
def extract_value_from_df(df, label_variants, column):
    if df is None or df.empty:
        return None
    for label in label_variants:
        if label in df.index:
            val = df.loc[label, column]
            if pd.notna(val):
                try:
                    return int(float(val))
                except (ValueError, TypeError, OverflowError):
                    return None
        for idx_label in df.index:
            if isinstance(idx_label, str) and label.lower() == idx_label.lower():
                val = df.loc[idx_label, column]
                if pd.notna(val):
                    try:
                        return int(float(val))
                    except (ValueError, TypeError, OverflowError):
                        return None
    return None

def extract_value_from_df_decimal(df, label_variants, column):
    if df is None or df.empty:
        return None
    for label in label_variants:
        if label in df.index:
            val = df.loc[label, column]
            if pd.notna(val):
                try:
                    return round(float(val), 4)
                except (ValueError, TypeError):
                    return None
        for idx_label in df.index:
            if isinstance(idx_label, str) and label.lower() == idx_label.lower():
                val = df.loc[idx_label, column]
                if pd.notna(val):
                    try:
                        return round(float(val), 4)
                    except (ValueError, TypeError):
                        return None
    return None

def parse_period_date(col_label):
    if isinstance(col_label, date):
        return col_label
    if isinstance(col_label, datetime):
        return col_label.date()
    if isinstance(col_label, str):
        for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%Y-%m-%dT%H:%M:%S"):
            try:
                return datetime.strptime(col_label.strip(), fmt).date()
            except ValueError:
                continue
    return None

def infer_fiscal_quarter(period_end):
    if period_end is None:
        return None
    month = period_end.month
    if month in (1, 2, 3):
        return 1
    elif month in (4, 5, 6):
        return 2
    elif month in (7, 8, 9):
        return 3
    else:
        return 4

# ── FILING PROCESSOR ──────────────────────────────────
def process_filing_financials(ticker, filing, period_type, conn, existing_periods):
    stored = 0
    accession = getattr(filing, "accession_number", None) or getattr(filing, "accession_no", None)

    try:
        obj = filing.obj()
        if obj is None or not hasattr(obj, "financials") or obj.financials is None:
            logger.warning(f"    No financials object for {ticker} filing {accession}")
            return 0
    except Exception as e:
        logger.warning(f"    Could not parse filing object for {ticker}: {e}")
        return 0

    financials = obj.financials

    income_df = None
    balance_df = None
    cashflow_df = None

    try:
        stmt = financials.income_statement
        if stmt is not None and hasattr(stmt, "to_dataframe"):
            income_df = stmt.to_dataframe(view="summary")
    except Exception as e:
        logger.debug(f"    Income statement parse error: {e}")

    try:
        stmt = financials.balance_sheet
        if stmt is not None and hasattr(stmt, "to_dataframe"):
            balance_df = stmt.to_dataframe(view="summary")
    except Exception as e:
        logger.debug(f"    Balance sheet parse error: {e}")

    try:
        stmt = getattr(financials, "cash_flow_statement", None) or \
               getattr(financials, "cashflow_statement", None)
        if callable(stmt):
            stmt = stmt()
        if stmt is not None and hasattr(stmt, "to_dataframe"):
            cashflow_df = stmt.to_dataframe(view="summary")
    except Exception as e:
        logger.debug(f"    Cash flow parse error: {e}")

    all_columns = set()
    for df in [income_df, balance_df, cashflow_df]:
        if df is not None and not df.empty:
            all_columns.update(df.columns.tolist())

    if not all_columns:
        logger.warning(f"    No period columns found for {ticker}")
        return 0

    now = datetime.utcnow()

    for col in all_columns:
        period_end = parse_period_date(col)
        if period_end is None:
            continue
        if (period_type, period_end) in existing_periods:
            continue

        fiscal_year = period_end.year
        fiscal_quarter = infer_fiscal_quarter(period_end) if period_type == "quarterly" else None

        row = {
            "ticker":                   ticker,
            "period_type":              period_type,
            "period_end_date":          period_end,
            "fiscal_year":              fiscal_year,
            "fiscal_quarter":           fiscal_quarter,
            "revenue":                  extract_value_from_df(income_df, INCOME_LABEL_MAP["revenue"], col),
            "cost_of_revenue":          extract_value_from_df(income_df, INCOME_LABEL_MAP["cost_of_revenue"], col),
            "gross_profit":             extract_value_from_df(income_df, INCOME_LABEL_MAP["gross_profit"], col),
            "research_and_dev":         extract_value_from_df(income_df, INCOME_LABEL_MAP["research_and_dev"], col),
            "selling_general_admin":    extract_value_from_df(income_df, INCOME_LABEL_MAP["selling_general_admin"], col),
            "operating_expense":        extract_value_from_df(income_df, INCOME_LABEL_MAP["operating_expense"], col),
            "operating_income":         extract_value_from_df(income_df, INCOME_LABEL_MAP["operating_income"], col),
            "interest_expense":         extract_value_from_df(income_df, INCOME_LABEL_MAP["interest_expense"], col),
            "pretax_income":            extract_value_from_df(income_df, INCOME_LABEL_MAP["pretax_income"], col),
            "income_tax":               extract_value_from_df(income_df, INCOME_LABEL_MAP["income_tax"], col),
            "net_income":               extract_value_from_df(income_df, INCOME_LABEL_MAP["net_income"], col),
            "eps_basic":                extract_value_from_df_decimal(income_df, INCOME_LABEL_MAP["eps_basic"], col),
            "eps_diluted":              extract_value_from_df_decimal(income_df, INCOME_LABEL_MAP["eps_diluted"], col),
            "cash_and_equivalents":     extract_value_from_df(balance_df, BALANCE_LABEL_MAP["cash_and_equivalents"], col),
            "short_term_investments":   extract_value_from_df(balance_df, BALANCE_LABEL_MAP["short_term_investments"], col),
            "total_current_assets":     extract_value_from_df(balance_df, BALANCE_LABEL_MAP["total_current_assets"], col),
            "total_assets":             extract_value_from_df(balance_df, BALANCE_LABEL_MAP["total_assets"], col),
            "total_current_liabilities":extract_value_from_df(balance_df, BALANCE_LABEL_MAP["total_current_liabilities"], col),
            "long_term_debt":           extract_value_from_df(balance_df, BALANCE_LABEL_MAP["long_term_debt"], col),
            "total_liabilities":        extract_value_from_df(balance_df, BALANCE_LABEL_MAP["total_liabilities"], col),
            "stockholders_equity":      extract_value_from_df(balance_df, BALANCE_LABEL_MAP["stockholders_equity"], col),
            "operating_cash_flow":      extract_value_from_df(cashflow_df, CASHFLOW_LABEL_MAP["operating_cash_flow"], col),
            "capital_expenditure":      extract_value_from_df(cashflow_df, CASHFLOW_LABEL_MAP["capital_expenditure"], col),
            "free_cash_flow":           extract_value_from_df(cashflow_df, CASHFLOW_LABEL_MAP["free_cash_flow"], col),
            "dividends_paid":           extract_value_from_df(cashflow_df, CASHFLOW_LABEL_MAP["dividends_paid"], col),
            "shares_outstanding":       None,
            "filing_accession":         str(accession) if accession else None,
            "source":                   "edgartools",
            "collected_at":             now,
        }

        if row["free_cash_flow"] is None and row["operating_cash_flow"] is not None and row["capital_expenditure"] is not None:
            row["free_cash_flow"] = row["operating_cash_flow"] + row["capital_expenditure"]

        if row["revenue"] is None and row["total_assets"] is None and row["net_income"] is None:
            logger.debug(f"    Skipping empty period {period_end} for {ticker}")
            continue

        try:
            upsert_financial_row(conn, row)
            existing_periods.add((period_type, period_end))
            stored += 1
        except Exception as e:
            logger.error(f"    DB error for {ticker} period {period_end}: {e}")
            conn.rollback()

    return stored

# ── PER-TICKER COLLECTOR ──────────────────────────────
def collect_fundamentals_for_ticker(ticker, conn):
    logger.info(f"  Fetching SEC data for {ticker}...")

    try:
        company = Company(ticker)
    except Exception as e:
        logger.error(f"  Could not find {ticker} on EDGAR: {e}")
        return 0

    existing_periods = get_existing_periods(conn, ticker)
    total_stored = 0

    # Annual (10-K)
    try:
        annual_filings = company.get_filings(form="10-K").head(ANNUAL_FILINGS_TO_PULL)
        logger.info(f"    Found {len(annual_filings)} annual (10-K) filings")
        for i, filing in enumerate(annual_filings):
            try:
                count = process_filing_financials(ticker, filing, "annual", conn, existing_periods)
                total_stored += count
                if count > 0:
                    logger.info(f"    10-K [{i+1}]: stored {count} period(s)")
            except Exception as e:
                logger.warning(f"    10-K [{i+1}] processing error: {e}")
            time.sleep(0.5)
    except Exception as e:
        logger.warning(f"    Could not retrieve 10-K filings for {ticker}: {e}")

    # Quarterly (10-Q)
    try:
        quarterly_filings = company.get_filings(form="10-Q").head(QUARTERLY_FILINGS_TO_PULL)
        logger.info(f"    Found {len(quarterly_filings)} quarterly (10-Q) filings")
        for i, filing in enumerate(quarterly_filings):
            try:
                count = process_filing_financials(ticker, filing, "quarterly", conn, existing_periods)
                total_stored += count
                if count > 0:
                    logger.info(f"    10-Q [{i+1}]: stored {count} period(s)")
            except Exception as e:
                logger.warning(f"    10-Q [{i+1}] processing error: {e}")
            time.sleep(0.5)
    except Exception as e:
        logger.warning(f"    Could not retrieve 10-Q filings for {ticker}: {e}")

    return total_stored

# ── MAIN ──────────────────────────────────────────────
def main():
    logger.info("=" * 60)
    logger.info("SEC FUNDAMENTALS COLLECTOR — Starting")
    logger.info("=" * 60)

    conn = get_db_connection()
    tickers = get_tracked_tickers(conn)
    logger.info(f"Found {len(tickers)} tickers in ticker_master")

    total_periods = 0
    success_tickers = 0
    fail_tickers = 0

    for i, ticker in enumerate(tickers):
        logger.info(f"[{i+1}/{len(tickers)}] Processing: {ticker}")
        try:
            count = collect_fundamentals_for_ticker(ticker, conn)
            total_periods += count
            success_tickers += 1
            if count > 0:
                logger.info(f"  ✓ {ticker}: {count} financial periods stored")
            else:
                logger.info(f"  ○ {ticker}: no new data (may already be up to date)")
        except Exception as e:
            logger.error(f"  ✗ {ticker} failed entirely: {e}")
            fail_tickers += 1
        time.sleep(DELAY_BETWEEN_TICKERS)

    conn.close()
    logger.info("=" * 60)
    logger.info(f"COMPLETE — {total_periods} total periods stored across {success_tickers} tickers ({fail_tickers} failed)")
    logger.info("=" * 60)

if __name__ == "__main__":
    main()
