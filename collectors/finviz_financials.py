#!/usr/bin/env python3
"""
Finviz Elite Financial Data Collector

Two modes (tried in order):
  1. Authenticated CSV export — fast bulk download (needs FINVIZ_EMAIL + FINVIZ_PASSWORD)
  2. Manual CSV drop — reads data/exports/finviz_latest.csv you saved from the browser
"""
import re
import os
import sys
import time
from datetime import datetime
from io import StringIO
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
import requests
from psycopg2.extras import execute_values

from db.connection import get_connection

EXPORTS_DIR = Path(__file__).parent.parent / "data" / "exports"
FINVIZ_EMAIL = os.environ.get("FINVIZ_EMAIL", "")
FINVIZ_PASSWORD = os.environ.get("FINVIZ_PASSWORD", "")

FINVIZ_LOGIN_URL = "https://finviz.com/login_submit.ashx"
FINVIZ_EXPORT_URL = "https://elite.finviz.com/export.ashx"

REQUEST_TIMEOUT = 30  # seconds — never hang forever

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
        print("  No FINVIZ_EMAIL/PASSWORD set — skipping Elite export.", flush=True)
        return None

    print("  Attempting Finviz Elite login...", flush=True)
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
    })

    try:
        login_resp = session.post(FINVIZ_LOGIN_URL, data={
            "email": FINVIZ_EMAIL,
            "password": FINVIZ_PASSWORD,
        }, allow_redirects=True, timeout=REQUEST_TIMEOUT)
        print(f"  Login response: {login_resp.status_code}", flush=True)
    except requests.exceptions.Timeout:
        print("  Login request timed out.", flush=True)
        return None
    except requests.exceptions.RequestException as e:
        print(f"  Login request failed: {e}", flush=True)
        return None

    try:
        test = session.get(f"{FINVIZ_EXPORT_URL}?v=111", timeout=REQUEST_TIMEOUT)
        first_bytes = test.content[:200].decode("utf-8", errors="ignore")
        if first_bytes.strip().startswith("<!") or test.status_code != 200:
            print(f"  Login failed (status={test.status_code}, got HTML instead of CSV).", flush=True)
            return None
    except requests.exceptions.Timeout:
        print("  Test export request timed out.", flush=True)
        return None
    except requests.exceptions.RequestException as e:
        print(f"  Test export failed: {e}", flush=True)
        return None

    print("  Login OK. Downloading views...", flush=True)
    dfs = []
    for view_id, view_name in VIEWS.items():
        try:
            r = session.get(f"{FINVIZ_EXPORT_URL}?v={view_id}", timeout=REQUEST_TIMEOUT)
            if r.status_code != 200 or r.text.strip().startswith("<!"):
                print(f"    {view_name} (v={view_id}): SKIP — non-CSV response", flush=True)
                continue
            df = pd.read_csv(StringIO(r.text))
            if "No." in df.columns:
                df = df.drop(columns=["No."])
            print(f"    {view_name} (v={view_id}): {len(df)} stocks, {len(df.columns)} cols", flush=True)
            dfs.append(df)
            time.sleep(1)
        except requests.exceptions.Timeout:
            print(f"    {view_name} (v={view_id}): SKIP — timed out", flush=True)
        except Exception as e:
            print(f"    {view_name} (v={view_id}): SKIP — {e}", flush=True)

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
    print(f"  Reading manual CSV: {manual_path}", flush=True)
    df = pd.read_csv(manual_path)
    if "No." in df.columns:
        df = df.drop(columns=["No."])
    return df


# ── Store in DB (BULK INSERT — FAST) ─────────────────────────────────
def store(conn, cur, df, snapshot_date):
    # -- Normalize all DataFrame column names (strip, collapse spaces) --
    df.columns = [re.sub(r"\s+", " ", str(c).strip()) for c in df.columns]

    # -- Build a reverse map from normalized Finviz name -> sql column --
    norm_map = {}
    for fv_col, sql_col in COLUMN_MAP.items():
        norm_map[re.sub(r"\s+", " ", fv_col.strip())] = sql_col

    available = {}
    for norm_col in df.columns:
        if norm_col in norm_map:
            available[norm_col] = norm_map[norm_col]

    # -- Diagnostic: show which COLUMN_MAP keys were NOT found --
    missing = [k for k in COLUMN_MAP.keys() if re.sub(r"\s+", " ", k.strip()) not in df.columns]
    if missing:
        print(f"  ⚠️  {len(missing)} column(s) not found in CSV: {missing[:15]}...", flush=True)

    if "Ticker" not in df.columns:
        print("  ERROR: no Ticker column found in data", flush=True)
        return 0

    # -- Build rows for bulk insert --
    cols_ordered = ["snapshot_date"] + list(available.values())
    rows = []
    for _, row in df.iterrows():
        values = [snapshot_date]
        ticker = None
        for norm_col, sql_col in available.items():
            val = parse_value(row[norm_col], sql_col)
            values.append(val)
            if sql_col == "ticker":
                ticker = val
        if not ticker:
            continue
        rows.append(tuple(values))

    # -- Show first row sample for validation --
    if rows:
        print(f"  Sample row ({rows[0][cols_ordered.index('ticker')]}):", flush=True)
        for i, col in enumerate(cols_ordered):
            print(f"    {col}: {rows[0][i]}", flush=True)

    # -- Bulk insert --
    cur.execute("TRUNCATE company_financials")
    cols_str = ", ".join(cols_ordered)
    from psycopg2.extras import execute_values
    execute_values(
        cur,
        f"INSERT INTO company_financials ({cols_str}) VALUES %s ON CONFLICT DO NOTHING",
        rows,
        page_size=1000
    )
    conn.commit()
    return len(rows)


def main():
    start_time = time.time()
    EXPORTS_DIR.mkdir(parents=True, exist_ok=True)

    print("=== Finviz Financial Data Collector ===", flush=True)

    conn = get_connection()
    cur = conn.cursor()

    snapshot_date = datetime.now().strftime("%Y-%m-%d")
    df = None

    print("\n--- Mode 1: Elite CSV Export ---", flush=True)
    df = try_elite_export()

    if df is None:
        print("\n--- Mode 2: Manual CSV ---", flush=True)
        df = try_manual_csv()

    if df is None:
        print("\nNo data source available. Options:", flush=True)
        print("  1. Set FINVIZ_EMAIL + FINVIZ_PASSWORD env vars for automated export", flush=True)
        print("  2. Save CSV from elite.finviz.com/screener → data/exports/finviz_latest.csv", flush=True)

        duration = time.time() - start_time
        cur.execute(
            "INSERT INTO collection_log(collector, status, records_added, duration_sec) VALUES(%s,%s,%s,%s)",
            ("finviz_financials", "skipped", 0, round(duration, 1)),
        )
        conn.commit()
        cur.close()
        conn.close()
        print(f"\n  Finished in {duration:.1f}s (no data collected)", flush=True)
        return

    print(f"\n  Raw data: {len(df)} stocks, {len(df.columns)} columns", flush=True)

    archive_path = EXPORTS_DIR / f"finviz_{snapshot_date}.csv"
    df.to_csv(archive_path, index=False)
    print(f"  Archived to {archive_path}", flush=True)

    count = store(conn, cur, df, snapshot_date)
    duration = time.time() - start_time

    cur.execute(
        "INSERT INTO collection_log(collector, status, records_added, duration_sec) VALUES(%s,%s,%s,%s)",
        ("finviz_financials", "ok", count, round(duration, 1)),
    )
    conn.commit()

    cur.execute(
        "SELECT sector, COUNT(*) FROM company_financials GROUP BY sector ORDER BY COUNT(*) DESC LIMIT 10"
    )
    sectors = cur.fetchall()
    print(f"\n  Stored {count} stocks", flush=True)
    print(f"  Top sectors:", flush=True)
    for s, c in sectors:
        print(f"    {s or 'N/A':<30} {c:>5}", flush=True)

    print(f"\n  Done in {duration:.1f}s", flush=True)
    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
