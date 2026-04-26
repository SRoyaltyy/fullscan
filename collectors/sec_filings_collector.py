#!/usr/bin/env python3
"""
sec_filings_collector.py
Monitors new 8-K filings and Form 4 insider trades for tracked tickers.
Populates: sec_filings, insider_trades tables.
Schedule: Daily
"""

import os
import sys
import time
import logging
import psycopg2
from datetime import datetime, timedelta, date

os.environ["EDGAR_IDENTITY"] = os.getenv("EDGAR_IDENTITY", "TradingBot bot@example.com")

from edgar import Company, set_identity
set_identity(os.getenv("EDGAR_IDENTITY", "TradingBot bot@example.com"))

# ── CONFIG ────────────────────────────────────────────
DB_HOST         = os.getenv("DB_HOST", "localhost")
DB_PORT         = os.getenv("DB_PORT", "5432")
DB_NAME         = os.getenv("DB_NAME", "trading_bot")
DB_USER         = os.getenv("DB_USER", "postgres")
DB_PASSWORD     = os.getenv("DB_PASSWORD", "")

LOOKBACK_DAYS   = 30
FORM4_LIMIT     = 20
EIGHTK_LIMIT    = 10
DELAY_BETWEEN_TICKERS = 1.5

EIGHTK_ITEM_DESCRIPTIONS = {
    "1.01": "Entry into Material Definitive Agreement",
    "1.02": "Termination of Material Definitive Agreement",
    "1.03": "Bankruptcy or Receivership",
    "2.01": "Completion of Acquisition or Disposition",
    "2.02": "Results of Operations and Financial Condition",
    "2.03": "Creation of Direct Financial Obligation",
    "2.05": "Costs for Exit or Disposal Activities",
    "2.06": "Material Impairments",
    "3.01": "Notice of Delisting",
    "3.02": "Unregistered Sales of Equity Securities",
    "4.01": "Changes in Registrant's Certifying Accountant",
    "4.02": "Non-Reliance on Previously Issued Financial Statements",
    "5.01": "Changes in Control of Registrant",
    "5.02": "Departure/Election of Directors or Officers",
    "5.03": "Amendments to Articles of Incorporation or Bylaws",
    "5.07": "Submission of Matters to Vote of Security Holders",
    "7.01": "Regulation FD Disclosure",
    "8.01": "Other Events",
    "9.01": "Financial Statements and Exhibits",
}

TRANSACTION_CODES = {
    "P": "BUY", "S": "SELL", "A": "GRANT", "D": "DISPOSITION",
    "F": "TAX_WITHHOLDING", "M": "EXERCISE", "C": "CONVERSION",
    "G": "GIFT", "J": "OTHER",
}

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

def accession_exists_in_filings(conn, accession_number):
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM sec_filings WHERE accession_number = %s", (accession_number,))
        return cur.fetchone() is not None

def insert_filing(conn, row):
    sql = """
        INSERT INTO sec_filings (
            ticker, company_name, cik, form_type, filing_date,
            accepted_date, accession_number, filing_url,
            items_reported, items_description, has_press_release,
            source, collected_at
        ) VALUES (
            %(ticker)s, %(company_name)s, %(cik)s, %(form_type)s, %(filing_date)s,
            %(accepted_date)s, %(accession_number)s, %(filing_url)s,
            %(items_reported)s, %(items_description)s, %(has_press_release)s,
            %(source)s, %(collected_at)s
        )
        ON CONFLICT (accession_number) DO NOTHING
    """
    with conn.cursor() as cur:
        cur.execute(sql, row)
    conn.commit()

def insert_insider_trade(conn, row):
    sql = """
        INSERT INTO insider_trades (
            ticker, filing_date, trade_date, accession_number,
            owner_name, owner_relationship,
            transaction_code, transaction_type,
            shares, price_per_share, total_value, shares_owned_after,
            direct_or_indirect,
            source, collected_at
        ) VALUES (
            %(ticker)s, %(filing_date)s, %(trade_date)s, %(accession_number)s,
            %(owner_name)s, %(owner_relationship)s,
            %(transaction_code)s, %(transaction_type)s,
            %(shares)s, %(price_per_share)s, %(total_value)s, %(shares_owned_after)s,
            %(direct_or_indirect)s,
            %(source)s, %(collected_at)s
        )
        ON CONFLICT (ticker, accession_number, owner_name, trade_date, transaction_code, shares)
        DO NOTHING
    """
    with conn.cursor() as cur:
        cur.execute(sql, row)
    conn.commit()

# ── 8-K PROCESSING ───────────────────────────────────
def process_8k_filings(ticker, company, conn, since_date):
    stored = 0
    try:
        filings = company.get_filings(form="8-K").head(EIGHTK_LIMIT)
    except Exception as e:
        logger.warning(f"    Could not fetch 8-K filings for {ticker}: {e}")
        return 0

    for filing in filings:
        try:
            accession = str(getattr(filing, "accession_number", "") or
                           getattr(filing, "accession_no", ""))
            if not accession:
                continue

            filing_date = getattr(filing, "filing_date", None)
            if filing_date and isinstance(filing_date, str):
                filing_date = datetime.strptime(filing_date, "%Y-%m-%d").date()
            if filing_date and filing_date < since_date:
                continue
            if accession_exists_in_filings(conn, accession):
                continue

            items_reported = []
            items_desc_parts = []
            has_press_release = False

            try:
                eightk = filing.obj()
                if eightk and hasattr(eightk, "items"):
                    raw_items = eightk.items or []
                    for item in raw_items:
                        item_str = str(item).strip()
                        items_reported.append(item_str)
                        if item_str in EIGHTK_ITEM_DESCRIPTIONS:
                            items_desc_parts.append(
                                f"{item_str}: {EIGHTK_ITEM_DESCRIPTIONS[item_str]}"
                            )
                if eightk and hasattr(eightk, "press_releases"):
                    has_press_release = bool(eightk.press_releases)
            except Exception as e:
                logger.debug(f"    Could not parse 8-K object: {e}")

            row = {
                "ticker":           ticker,
                "company_name":     getattr(filing, "company", None),
                "cik":              str(getattr(filing, "cik", "")),
                "form_type":        "8-K",
                "filing_date":      filing_date,
                "accepted_date":    getattr(filing, "accepted_date", None),
                "accession_number": accession,
                "filing_url":       getattr(filing, "document_url", None) or
                                    getattr(filing, "filing_url", None),
                "items_reported":   items_reported if items_reported else None,
                "items_description": "; ".join(items_desc_parts) if items_desc_parts else None,
                "has_press_release": has_press_release,
                "source":           "edgartools",
                "collected_at":     datetime.utcnow(),
            }
            insert_filing(conn, row)
            stored += 1
            items_summary = ", ".join(items_reported[:3]) if items_reported else "no items parsed"
            logger.info(f"    8-K: {filing_date} — [{items_summary}]")
        except Exception as e:
            logger.warning(f"    8-K processing error: {e}")
            conn.rollback()
        time.sleep(0.3)

    return stored

# ── FORM 4 PROCESSING ────────────────────────────────
def _build_trade_row_from_obj(ticker, filing_date, accession, owner_name, relationship, txn):
    now = datetime.utcnow()
    code = getattr(txn, "transaction_code", None) or getattr(txn, "code", None)
    shares = getattr(txn, "shares", None) or getattr(txn, "transaction_shares", None)
    price = getattr(txn, "price", None) or getattr(txn, "price_per_share", None)
    trade_date = getattr(txn, "transaction_date", None) or getattr(txn, "date", None)
    shares_after = getattr(txn, "shares_owned_following", None)
    di = getattr(txn, "direct_or_indirect", None)

    if isinstance(code, str):
        code = code.strip()
    if shares:
        try: shares = float(shares)
        except: shares = None
    if price:
        try: price = float(price)
        except: price = None
    if trade_date and isinstance(trade_date, str):
        try: trade_date = datetime.strptime(trade_date, "%Y-%m-%d").date()
        except: trade_date = None
    if trade_date and isinstance(trade_date, datetime):
        trade_date = trade_date.date()
    if shares_after:
        try: shares_after = float(shares_after)
        except: shares_after = None
    if di and isinstance(di, str):
        di = "D" if "DIRECT" in di.upper() or di.strip().upper() == "D" else "I"

    transaction_type = TRANSACTION_CODES.get(code, "OTHER") if code else None
    total_value = round(shares * price, 2) if shares and price else None

    return {
        "ticker":               ticker,
        "filing_date":          filing_date,
        "trade_date":           trade_date or filing_date,
        "accession_number":     accession,
        "owner_name":           owner_name,
        "owner_relationship":   relationship,
        "transaction_code":     code,
        "transaction_type":     transaction_type,
        "shares":               shares,
        "price_per_share":      price,
        "total_value":          total_value,
        "shares_owned_after":   shares_after,
        "direct_or_indirect":   di,
        "source":               "edgartools",
        "collected_at":         now,
    }

def _build_trade_row_from_series(ticker, filing_date, accession, owner_name, relationship, txn):
    now = datetime.utcnow()
    code = None
    for key in ["transaction_code", "transactionCode", "code"]:
        if key in txn.index:
            code = str(txn[key]).strip() if txn[key] else None
            if code: break

    shares = None
    for key in ["shares", "transaction_shares", "transactionShares"]:
        if key in txn.index:
            try: shares = float(txn[key])
            except: pass
            if shares: break

    price = None
    for key in ["price", "price_per_share", "pricePerShare"]:
        if key in txn.index:
            try: price = float(txn[key])
            except: pass
            if price: break

    trade_date = None
    for key in ["transaction_date", "transactionDate", "date"]:
        if key in txn.index and txn[key]:
            val = txn[key]
            if isinstance(val, (date, datetime)):
                trade_date = val if isinstance(val, date) else val.date()
            elif isinstance(val, str):
                try: trade_date = datetime.strptime(val, "%Y-%m-%d").date()
                except: pass
            if trade_date: break

    shares_after = None
    for key in ["shares_owned_following", "sharesOwnedFollowing", "post_transaction_shares"]:
        if key in txn.index:
            try: shares_after = float(txn[key])
            except: pass
            if shares_after: break

    di = None
    for key in ["direct_or_indirect", "directOrIndirect", "ownership_nature"]:
        if key in txn.index and txn[key]:
            di_val = str(txn[key]).strip().upper()
            di = "D" if "DIRECT" in di_val or di_val == "D" else "I"
            break

    transaction_type = TRANSACTION_CODES.get(code, "OTHER") if code else None
    total_value = round(shares * price, 2) if shares and price else None

    return {
        "ticker": ticker, "filing_date": filing_date, "trade_date": trade_date or filing_date,
        "accession_number": accession, "owner_name": owner_name, "owner_relationship": relationship,
        "transaction_code": code, "transaction_type": transaction_type, "shares": shares,
        "price_per_share": price, "total_value": total_value, "shares_owned_after": shares_after,
        "direct_or_indirect": di, "source": "edgartools", "collected_at": now,
    }

def process_form4_filings(ticker, company, conn, since_date):
    stored = 0
    try:
        filings = company.get_filings(form="4").head(FORM4_LIMIT)
    except Exception as e:
        logger.warning(f"    Could not fetch Form 4 filings for {ticker}: {e}")
        return 0

    for filing in filings:
        try:
            accession = str(getattr(filing, "accession_number", "") or
                           getattr(filing, "accession_no", ""))
            if not accession:
                continue
            filing_date = getattr(filing, "filing_date", None)
            if filing_date and isinstance(filing_date, str):
                filing_date = datetime.strptime(filing_date, "%Y-%m-%d").date()
            if filing_date and filing_date < since_date:
                continue

            try:
                form4 = filing.obj()
            except Exception:
                continue
            if form4 is None:
                continue

            owner_name = getattr(form4, "reporting_owner_name", None) or \
                         getattr(form4, "reporting_owner", None)
            if owner_name and not isinstance(owner_name, str):
                owner_name = str(owner_name)
            owner_relationship = getattr(form4, "reporting_owner_relationship", None)
            if owner_relationship and not isinstance(owner_relationship, str):
                owner_relationship = str(owner_relationship)

            transactions = getattr(form4, "transactions", None)
            if transactions is None:
                filing_row = {
                    "ticker": ticker, "company_name": getattr(filing, "company", None),
                    "cik": str(getattr(filing, "cik", "")), "form_type": "4",
                    "filing_date": filing_date, "accepted_date": getattr(filing, "accepted_date", None),
                    "accession_number": accession,
                    "filing_url": getattr(filing, "document_url", None),
                    "items_reported": None,
                    "items_description": f"Insider: {owner_name}" if owner_name else None,
                    "has_press_release": False, "source": "edgartools", "collected_at": datetime.utcnow(),
                }
                try:
                    insert_filing(conn, filing_row)
                except Exception:
                    conn.rollback()
                continue

            try:
                if hasattr(transactions, "iterrows"):
                    for _, txn in transactions.iterrows():
                        trade_row = _build_trade_row_from_series(
                            ticker, filing_date, accession, owner_name, owner_relationship, txn
                        )
                        if trade_row:
                            insert_insider_trade(conn, trade_row)
                            stored += 1
                elif hasattr(transactions, "__iter__"):
                    for txn in transactions:
                        trade_row = _build_trade_row_from_obj(
                            ticker, filing_date, accession, owner_name, owner_relationship, txn
                        )
                        if trade_row:
                            insert_insider_trade(conn, trade_row)
                            stored += 1
            except Exception as e:
                logger.debug(f"    Transaction iteration error: {e}")
                conn.rollback()

            if owner_name:
                logger.info(f"    Form 4: {filing_date} — {owner_name} ({owner_relationship})")
        except Exception as e:
            logger.warning(f"    Form 4 processing error: {e}")
            conn.rollback()
        time.sleep(0.3)

    return stored

# ── MAIN ──────────────────────────────────────────────
def main():
    logger.info("=" * 60)
    logger.info("SEC FILINGS COLLECTOR — Starting")
    logger.info("=" * 60)

    conn = get_db_connection()
    tickers = get_tracked_tickers(conn)
    logger.info(f"Found {len(tickers)} tickers in ticker_master")

    since_date = (datetime.utcnow() - timedelta(days=LOOKBACK_DAYS)).date()
    logger.info(f"Looking back to: {since_date}")

    total_8k = 0
    total_form4 = 0

    for i, ticker in enumerate(tickers):
        logger.info(f"[{i+1}/{len(tickers)}] Processing: {ticker}")
        try:
            company = Company(ticker)
        except Exception as e:
            logger.error(f"  Could not find {ticker} on EDGAR: {e}")
            continue

        try:
            count = process_8k_filings(ticker, company, conn, since_date)
            total_8k += count
        except Exception as e:
            logger.error(f"  8-K collector error for {ticker}: {e}")

        try:
            count = process_form4_filings(ticker, company, conn, since_date)
            total_form4 += count
        except Exception as e:
            logger.error(f"  Form 4 collector error for {ticker}: {e}")

        time.sleep(DELAY_BETWEEN_TICKERS)

    conn.close()
    logger.info("=" * 60)
    logger.info(f"COMPLETE — {total_8k} new 8-K filings, {total_form4} new insider trades stored")
    logger.info("=" * 60)

if __name__ == "__main__":
    main()
