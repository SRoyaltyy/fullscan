#!/usr/bin/env python3
"""
company_profiles_collector.py
Pulls company profile data from Financial Modeling Prep (FMP).
Populates: company_profiles table.
Schedule: Weekly
"""

import os
import sys
import time
import logging
import requests
import psycopg2
from datetime import datetime

# ── CONFIG ────────────────────────────────────────────
FMP_API_KEY     = os.getenv("FMP_API_KEY")
FMP_BASE_URL    = "https://financialmodelingprep.com/api/v3"

DB_HOST         = os.getenv("DB_HOST", "localhost")
DB_PORT         = os.getenv("DB_PORT", "5432")
DB_NAME         = os.getenv("DB_NAME", "trading_bot")
DB_USER         = os.getenv("DB_USER", "postgres")
DB_PASSWORD     = os.getenv("DB_PASSWORD", "")

RATE_LIMIT_SEC  = 0.5

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

def upsert_profile(conn, profile):
    sql = """
        INSERT INTO company_profiles (
            ticker, company_name, legal_name, description, ceo,
            sector, industry, exchange, country, website,
            ipo_date, employees, market_cap, beta,
            is_etf, is_actively_trading, cik, isin, cusip,
            source, collected_at, updated_at
        ) VALUES (
            %(ticker)s, %(company_name)s, %(legal_name)s, %(description)s, %(ceo)s,
            %(sector)s, %(industry)s, %(exchange)s, %(country)s, %(website)s,
            %(ipo_date)s, %(employees)s, %(market_cap)s, %(beta)s,
            %(is_etf)s, %(is_actively_trading)s, %(cik)s, %(isin)s, %(cusip)s,
            %(source)s, %(collected_at)s, %(updated_at)s
        )
        ON CONFLICT (ticker) DO UPDATE SET
            company_name        = EXCLUDED.company_name,
            legal_name          = EXCLUDED.legal_name,
            description         = EXCLUDED.description,
            ceo                 = EXCLUDED.ceo,
            sector              = EXCLUDED.sector,
            industry            = EXCLUDED.industry,
            exchange            = EXCLUDED.exchange,
            country             = EXCLUDED.country,
            website             = EXCLUDED.website,
            ipo_date            = EXCLUDED.ipo_date,
            employees           = EXCLUDED.employees,
            market_cap          = EXCLUDED.market_cap,
            beta                = EXCLUDED.beta,
            is_etf              = EXCLUDED.is_etf,
            is_actively_trading = EXCLUDED.is_actively_trading,
            cik                 = EXCLUDED.cik,
            isin                = EXCLUDED.isin,
            cusip               = EXCLUDED.cusip,
            updated_at          = EXCLUDED.updated_at
    """
    with conn.cursor() as cur:
        cur.execute(sql, profile)
    conn.commit()

# ── FMP API ───────────────────────────────────────────
def fetch_fmp_profile(ticker):
    url = f"{FMP_BASE_URL}/profile/{ticker}"
    params = {"apikey": FMP_API_KEY}
    try:
        resp = requests.get(url, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        if not data or len(data) == 0:
            logger.warning(f"  No profile data returned for {ticker}")
            return None
        return data[0]
    except requests.exceptions.RequestException as e:
        logger.error(f"  HTTP error fetching {ticker}: {e}")
        return None
    except (ValueError, KeyError) as e:
        logger.error(f"  Parse error for {ticker}: {e}")
        return None

def parse_fmp_profile(ticker, raw):
    now = datetime.utcnow()
    employees = None
    if raw.get("fullTimeEmployees"):
        try:
            employees = int(raw["fullTimeEmployees"])
        except (ValueError, TypeError):
            pass
    ipo_date = None
    if raw.get("ipoDate"):
        try:
            ipo_date = datetime.strptime(raw["ipoDate"], "%Y-%m-%d").date()
        except ValueError:
            pass
    return {
        "ticker":               ticker,
        "company_name":         raw.get("companyName"),
        "legal_name":           raw.get("companyName"),
        "description":          raw.get("description"),
        "ceo":                  raw.get("ceo"),
        "sector":               raw.get("sector"),
        "industry":             raw.get("industry"),
        "exchange":             raw.get("exchangeShortName") or raw.get("exchange"),
        "country":              raw.get("country"),
        "website":              raw.get("website"),
        "ipo_date":             ipo_date,
        "employees":            employees,
        "market_cap":           raw.get("mktCap"),
        "beta":                 raw.get("beta"),
        "is_etf":               raw.get("isEtf", False),
        "is_actively_trading":  raw.get("isActivelyTrading", True),
        "cik":                  raw.get("cik"),
        "isin":                 raw.get("isin"),
        "cusip":                raw.get("cusip"),
        "source":               "fmp",
        "collected_at":         now,
        "updated_at":           now,
    }

# ── MAIN ──────────────────────────────────────────────
def main():
    if not FMP_API_KEY:
        logger.error("FMP_API_KEY not set — aborting.")
        sys.exit(1)

    logger.info("=" * 60)
    logger.info("COMPANY PROFILES COLLECTOR — Starting")
    logger.info("=" * 60)

    conn = get_db_connection()
    tickers = get_tracked_tickers(conn)
    logger.info(f"Found {len(tickers)} tickers in ticker_master")

    if len(tickers) > 250:
        logger.warning(f"  {len(tickers)} tickers exceeds FMP free tier. Processing first 240.")
        tickers = tickers[:240]

    success_count = 0
    fail_count = 0

    for i, ticker in enumerate(tickers):
        logger.info(f"[{i+1}/{len(tickers)}] Fetching profile: {ticker}")
        raw = fetch_fmp_profile(ticker)
        if raw is None:
            fail_count += 1
            time.sleep(RATE_LIMIT_SEC)
            continue
        profile = parse_fmp_profile(ticker, raw)
        try:
            upsert_profile(conn, profile)
            desc_preview = (profile["description"] or "")[:80]
            logger.info(f"  ✓ {profile['company_name']} | {profile['sector']} | {desc_preview}...")
            success_count += 1
        except Exception as e:
            logger.error(f"  ✗ DB error for {ticker}: {e}")
            conn.rollback()
            fail_count += 1
        time.sleep(RATE_LIMIT_SEC)

    conn.close()
    logger.info("=" * 60)
    logger.info(f"COMPLETE — {success_count} profiles saved, {fail_count} failed")
    logger.info("=" * 60)

if __name__ == "__main__":
    main()
