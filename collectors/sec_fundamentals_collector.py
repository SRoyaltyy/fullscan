#!/usr/bin/env python3
"""
SEC Fundamentals Collector — yfinance edition
Pulls historical financial statements from Yahoo Finance (free, no API key).
Populates: sec_financials table.
Schedule: Weekly
"""

import os, sys, time, logging
from datetime import datetime, date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import yfinance as yf
import pandas as pd
from db.connection import get_connection

DATABASE_URL = os.getenv("DATABASE_URL")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# ── Helpers ───────────────────────────────────────────
def safe_int(val):
    try:
        if pd.isna(val):
            return None
        return int(round(float(val)))
    except (ValueError, TypeError):
        return None

def safe_float(val):
    try:
        if pd.isna(val):
            return None
        return round(float(val), 4)
    except (ValueError, TypeError):
        return None

# ── Upsert a single row ───────────────────────────────
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
            source, collected_at
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
            %(source)s, %(collected_at)s
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
            source                  = EXCLUDED.source,
            collected_at            = EXCLUDED.collected_at
    """
    with conn.cursor() as cur:
        cur.execute(sql, row)
    conn.commit()

# ── Get a value from a DF using multiple possible labels ──
def get_value(df, labels, col):
    if df is None or df.empty:
        return None
    for label in labels:
        if label in df.index:
            val = df.loc[label, col]
            if not pd.isna(val):
                return safe_int(val)  # all financial fields are integers (except EPS)
    return None

def get_eps(df, labels, col):
    if df is None or df.empty:
        return None
    for label in labels:
        if label in df.index:
            val = df.loc[label, col]
            if not pd.isna(val):
                return safe_float(val)
    return None

# ── Per‑ticker collector ──────────────────────────────
def collect_financials_for_ticker(ticker, conn):
    total = 0
    try:
        t = yf.Ticker(ticker)
    except Exception as e:
        logger.warning(f"  ✗ Could not create Ticker object for {ticker}: {e}")
        return 0

    now = datetime.utcnow()
    cutoff = now - timedelta(days=5*365)   # last 5 years

    # ── Income statement (annual) ──
    try:
        income_df = t.financials
    except Exception as e:
        logger.warning(f"  ✗ Could not fetch income for {ticker}: {e}")
        income_df = None

    # ── Balance sheet (annual) ──
    try:
        balance_df = t.balance_sheet
    except Exception as e:
        logger.warning(f"  ✗ Could not fetch balance sheet for {ticker}: {e}")
        balance_df = None

    # ── Cash flow (annual) ──
    try:
        cashflow_df = t.cashflow
    except Exception as e:
        logger.warning(f"  ✗ Could not fetch cash flow for {ticker}: {e}")
        cashflow_df = None

    if income_df is None and balance_df is None and cashflow_df is None:
        return 0

    # ── Build a set of all period columns ──
    all_cols = set()
    for df in [income_df, balance_df, cashflow_df]:
        if df is not None and not df.empty:
            for col in df.columns:
                try:
                    d = col.date() if hasattr(col, 'date') else pd.Timestamp(col).date()
                    if d >= cutoff.date():
                        all_cols.add(d)
                except:
                    pass

    for period_end in all_cols:
        year = period_end.year
        row = {
            "ticker": ticker,
            "period_type": "annual",
            "period_end_date": period_end,
            "fiscal_year": year,
            "fiscal_quarter": None,
            "revenue": None,
            "cost_of_revenue": None,
            "gross_profit": None,
            "research_and_dev": None,
            "selling_general_admin": None,
            "operating_expense": None,
            "operating_income": None,
            "interest_expense": None,
            "pretax_income": None,
            "income_tax": None,
            "net_income": None,
            "eps_basic": None,
            "eps_diluted": None,
            "cash_and_equivalents": None,
            "short_term_investments": None,
            "total_current_assets": None,
            "total_assets": None,
            "total_current_liabilities": None,
            "long_term_debt": None,
            "total_liabilities": None,
            "stockholders_equity": None,
            "operating_cash_flow": None,
            "capital_expenditure": None,
            "free_cash_flow": None,
            "dividends_paid": None,
            "shares_outstanding": None,
            "source": "yfinance",
            "collected_at": now,
        }

        if income_df is not None and not income_df.empty:
            row["revenue"]               = get_value(income_df, ["Total Revenue", "Revenue", "Total Revenues"], period_end)
            row["cost_of_revenue"]       = get_value(income_df, ["Cost of Revenue", "Cost Of Goods Sold", "Cost of Sales"], period_end)
            row["gross_profit"]          = get_value(income_df, ["Gross Profit"], period_end)
            row["research_and_dev"]      = get_value(income_df, ["Research And Development", "Research & Development", "R&D Expense"], period_end)
            row["selling_general_admin"] = get_value(income_df, ["Selling General And Administrative", "SG&A Expense", "Selling, General & Administrative"], period_end)
            row["operating_expense"]     = get_value(income_df, ["Total Operating Expenses", "Operating Expenses"], period_end)
            row["operating_income"]      = get_value(income_df, ["Operating Income", "Operating Income (Loss)"], period_end)
            row["interest_expense"]      = get_value(income_df, ["Interest Expense", "Interest Expense Net"], period_end)
            row["pretax_income"]         = get_value(income_df, ["Pretax Income", "Income Before Tax", "Income Before Income Taxes"], period_end)
            row["income_tax"]            = get_value(income_df, ["Income Tax Expense", "Provision For Income Taxes"], period_end)
            row["net_income"]            = get_value(income_df, ["Net Income", "Net Income Common Stockholders"], period_end)
            row["eps_basic"]             = get_eps(income_df, ["Basic EPS", "EPS Basic"], period_end)
            row["eps_diluted"]           = get_eps(income_df, ["Diluted EPS", "EPS Diluted"], period_end)

        if balance_df is not None and not balance_df.empty:
            row["cash_and_equivalents"]      = get_value(balance_df, ["Cash And Cash Equivalents", "Cash & Equivalents"], period_end)
            row["total_current_assets"]      = get_value(balance_df, ["Total Current Assets", "Current Assets"], period_end)
            row["total_assets"]              = get_value(balance_df, ["Total Assets"], period_end)
            row["total_current_liabilities"] = get_value(balance_df, ["Total Current Liabilities", "Current Liabilities"], period_end)
            row["long_term_debt"]            = get_value(balance_df, ["Long Term Debt", "Long-Term Debt", "Non-Current Long Term Debt"], period_end)
            row["total_liabilities"]         = get_value(balance_df, ["Total Liabilities Net Minority Interest", "Total Liabilities"], period_end)
            row["stockholders_equity"]       = get_value(balance_df, ["Stockholders Equity", "Total Stockholders Equity", "Total Equity"], period_end)

        if cashflow_df is not None and not cashflow_df.empty:
            row["operating_cash_flow"]  = get_value(cashflow_df, ["Operating Cash Flow", "Cash From Operating Activities", "Net Cash From Operations"], period_end)
            row["capital_expenditure"]  = get_value(cashflow_df, ["Capital Expenditure", "Capital Expenditures", "Purchase Of Plant Property Equipment"], period_end)
            row["dividends_paid"]       = get_value(cashflow_df, ["Dividends Paid", "Payment Of Dividends"], period_end)
            # Free cash flow attempt
            row["free_cash_flow"] = get_value(cashflow_df, ["Free Cash Flow"], period_end)
            if row["free_cash_flow"] is None and row["operating_cash_flow"] is not None and row["capital_expenditure"] is not None:
                # Capital expenditure is usually negative, so add
                row["free_cash_flow"] = row["operating_cash_flow"] + row["capital_expenditure"]

        # Skip rows where all key fields are None
        if all(row[k] is None for k in ["revenue", "total_assets", "net_income","operating_income","stockholders_equity"]):
            continue

        try:
            upsert_financial_row(conn, row)
            total += 1
        except Exception as e:
            logger.error(f"  DB insert error for {ticker} ({period_end}): {e}")
            conn.rollback()

    return total


# ── Main ──────────────────────────────────────────────
def main():
    logger.info("=" * 60)
    logger.info("SEC FUNDAMENTALS COLLECTOR (yfinance) — Starting")
    logger.info("=" * 60)

    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT ticker FROM ticker_master WHERE is_active = true ORDER BY ticker")
    tickers = [row[0] for row in cur.fetchall()]
    cur.close()

    logger.info(f"Found {len(tickers)} tickers in ticker_master")

    grand_total = 0
    for i, ticker in enumerate(tickers):
        logger.info(f"[{i+1}/{len(tickers)}] Processing: {ticker}")
        try:
            count = collect_financials_for_ticker(ticker, conn)
            grand_total += count
            logger.info(f"  ✓ {ticker}: {count} financial periods stored")
        except Exception as e:
            logger.error(f"  ✗ {ticker} failed: {e}")
        time.sleep(1.5)  # be gentle to yfinance

    conn.close()
    logger.info("=" * 60)
    logger.info(f"COMPLETE — {grand_total} total annual periods stored across {len(tickers)} tickers")
    logger.info("=" * 60)

if __name__ == "__main__":
    main()
