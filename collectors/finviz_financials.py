#!/usr/bin/env python3
"""
Finviz Elite Financial Data Collector

Three modes (tried in order):
  1. Authenticated CSV export — fast bulk download (needs FINVIZ_EMAIL + FINVIZ_PASSWORD)
  2. Manual CSV drop — reads data/exports/finviz_latest.csv you saved from the browser
  3. (no fallback — modes 1 or 2 must succeed)

NOTE ON ToS: Finviz prohibits automated scraping. Mode 1 automates what you'd
do manually with your paid Elite account. Review their Terms before enabling.
If you're uncomfortable with Mode 1, save the CSV manually (Mode 2).
"""

import os
import sys
import time
from datetime import datetime
from io import StringIO
from pathlib import Path

# Allow running as module or script
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
import requests

from db.db_utils import get_db, ensure_schema

EXPORTS_DIR = Path(__file__).parent.parent / "data" / "exports"
FINVIZ_EMAIL = os.environ.get("FINVIZ_EMAIL", "")
FINVIZ_PASSWORD = os.environ.get("FINVIZ_PASSWORD", "")

FINVIZ_LOGIN_URL = "https://finviz.com/login_submit.ashx"
FINVIZ_EXPORT_URL = "https://elite.finviz.com/export.ashx"

VIEWS = {
    111: "Overview",
    121: "Valuation",
    131: "Financial",
    141: "Ownership",
    151: "Performance",
    161: "Technical",
}

COLUMN_MAP = {
    "Ticker": "ticker", "Company": "company", "Sector": "sector",
    "Industry": "industry", "Country": "country", "Market Cap": "market_cap",
    "P/E": "pe", "Fwd P/E": "forward_pe", "PEG": "peg", "P/S": "ps",
    "P/B": "pb", "P/C": "p_cash", "P/FCF": "p_fcf",
    "Dividend": "dividend_yield", "Payout Ratio": "payout_ratio",
    "EPS": "eps", "EPS this Y": "eps_growth_this_y",
    "EPS next Y": "eps_growth_next_y", "EPS past 5Y": "eps_growth_past_5y",
    "EPS next 5Y": "eps_growth_next_5y", "Sales past 5Y": "sales_growth_past_5y",
    "EPS Q/Q": "eps_growth_qoq", "Sales Q/Q": "sales_growth_qoq",
    "Outstanding": "shares_outstanding", "Float": "float_shares",
    "Insider Own": "insider_own", "Insider Trans": "insider_trans",
    "Inst Own": "inst_own", "Inst Trans": "inst_trans",
    "Float Short": "float_short", "Short Ratio": "short_ratio",
    "ROA": "roa", "ROE": "roe", "ROI": "roi",
    "Curr Ratio": "current_ratio", "Quick Ratio": "quick_ratio",
    "LTDebt/Eq": "lt_debt_equity", "Debt/Eq": "debt_equity",
    "Gross M": "gross_margin", "Oper M": "oper_margin", "Profit M": "profit_margin",
    "Perf Week": "perf_week", "Perf Month": "perf_month",
    "Perf Quart": "perf_quarter", "Perf Half": "perf_half_y",
    "Perf Year": "perf_year", "Perf YTD": "perf_ytd",
    "Volatility W": "volatility_week", "Volatility M": "volatility_month",
    "SMA20": "sma20", "SMA50": "sma50", "SMA200": "sma200",
    "50D High": "high_50d", "50D Low": "low_50d",
    "52W High": "high_52w", "52W Low": "low_52w",
    "RSI": "rsi", "from Open": "change_from_open", "Gap": "gap",
    "Recom": "analyst_recom", "Avg Volume": "avg_volume",
    "Rel Volume": "rel_volume", "Price": "price", "Change": "change",
    "Volume": "volume", "Earnings Date": "earnings_date",
    "Target Price": "target_price", "IPO Date": "ipo_date",
    "Beta": "beta", "ATR": "atr",
}

TEXT_COLS = {"ticker", "company", "sector", "industry", "country", "earnings_date", "ipo_date", "snapshot_date"}


def parse_value(val, col_name):
    if col_name in TEXT_COLS:
        return str(val).strip() if pd.notna(val) and str(val).strip() not in ("-", "") else None
    if pd.isna(val) or str(val).strip() in ("-", ""):
        return None
    s = str(val).strip()
    if s.endswith("%"):
        try:
            return float(s[:-1])
        except ValueError:
            return None
    multipliers = {"B": 1e9, "M": 1e6, "K": 1e3}
    if s[-1] in multipliers:
        try:
            return float(s[:-1]) * multipliers[s[-1]]
        except ValueError:
            return None
    try:
        return float(s.replace(",", ""))
    except ValueError:
        return s


# ── Mode 1: Authenticated CSV export ───────────────────────────────
def try_elite_export():
    if not FINVIZ_EMAIL or not FINVIZ_PASSWORD:
        return None

    print("  Attempting Finviz Elite login...")
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
    })

    session.post(FINVIZ_LOGIN_URL, data={
        "email": FINVIZ_EMAIL,
        "password": FINVIZ_PASSWORD,
    }, allow_redirects=True)

    test = session.get(f"{FINVIZ_EXPORT_URL}?v=111", stream=True)
    first_bytes = test.content[:200].decode("utf-8", errors="ignore")
    if first_bytes.strip().startswith("<!") or test.status_code != 200:
        print("  Login may have failed (got HTML instead of CSV).")
        return None

    print("  Login OK. Downloading views...")
    dfs = []
    for view_id, view_name in VIEWS.items():
        r = session.get(f"{FINVIZ_EXPORT_URL}?v={view_id}")
        if r.status_code != 200 or r.text.strip().startswith("<!"):
            print(f"    {view_name} (v={view_id}): SKIP — non-CSV response")
            continue
        df = pd.read_csv(StringIO(r.text))
        if "No." in df.columns:
            df = df.drop(columns=["No."])
        print(f"    {view_name} (v={view_id}): {len(df)} stocks, {len(df.columns)} cols")
        dfs.append(df)
        time.sleep(1)

    if not dfs:
        return None

    merged = dfs[0]
    for df in dfs[1:]:
        new_cols = ["Ticker"] + [c for c in df.columns if c not in merged.columns]
        if len(new_cols) > 1:
            merged = merged.merge(df[new_cols], on="Ticker", how="outer")
    return merged


# ── Mode 2: Manual CSV drop ────────────────────────────────────────
def try_manual_csv():
    manual_path = EXPORTS_DIR / "finviz_latest.csv"
    if not manual_path.exists():
        return None
    print(f"  Reading manual CSV: {manual_path}")
    df = pd.read_csv(manual_path)
    if "No." in df.columns:
        df = df.drop(columns=["No."])
    return df


# ── Store in DB ─────────────────────────────────────────────────────
def store(conn, df, snapshot_date):
    available = {}
    for finviz_col, sql_col in COLUMN_MAP.items():
        if finviz_col in df.columns:
            available[finviz_col] = sql_col

    if "Ticker" not in df.columns and "ticker" not in df.columns:
        print("  ERROR: no Ticker column found in data")
        return 0

    conn.execute("DELETE FROM company_financials")

    count = 0
    for _, row in df.iterrows():
        values = {"snapshot_date": snapshot_date}
        for fv_col, sql_col in available.items():
            values[sql_col] = parse_value(row[fv_col], sql_col)

        if not values.get("ticker"):
            continue

        cols = ", ".join(values.keys())
        placeholders = ", ".join(["?"] * len(values))
        conn.execute(f"INSERT OR REPLACE INTO company_financials ({cols}) VALUES ({placeholders})", list(values.values()))
        count += 1

    conn.commit()
    return count


def main():
    start_time = time.time()
    EXPORTS_DIR.mkdir(parents=True, exist_ok=True)

    conn = get_db()
    ensure_schema(conn)

    snapshot_date = datetime.now().strftime("%Y-%m-%d")
    df = None

    print("=== Finviz Elite CSV Export ===")
    df = try_elite_export()

    if df is None:
        print("\n=== Checking for manual CSV ===")
        df = try_manual_csv()

    if df is None:
        print("\nNo data source available. Options:")
        print("  1. Set FINVIZ_EMAIL + FINVIZ_PASSWORD env vars for automated export")
        print("  2. Save CSV from elite.finviz.com/screener → data/exports/finviz_latest.csv")
        conn.close()
        sys.exit(1)

    print(f"\n  Raw data: {len(df)} stocks, {len(df.columns)} columns")

    archive_path = EXPORTS_DIR / f"finviz_{snapshot_date}.csv"
    df.to_csv(archive_path, index=False)
    print(f"  Archived to {archive_path}")

    count = store(conn, df, snapshot_date)
    duration = time.time() - start_time

    conn.execute(
        "INSERT INTO collection_log(collector,status,records_added,duration_sec) VALUES(?,?,?,?)",
        ("finviz_financials", "ok", count, round(duration, 1)),
    )
    conn.commit()

    sectors = conn.execute("SELECT sector, COUNT(*) FROM company_financials GROUP BY sector ORDER BY COUNT(*) DESC LIMIT 10").fetchall()
    print(f"\n  Stored {count} stocks")
    print(f"  Top sectors:")
    for s, c in sectors:
        print(f"    {s or 'N/A':<30} {c:>5}")

    print(f"\n  Done in {duration:.1f}s")
    conn.close()


if __name__ == "__main__":
    main()
