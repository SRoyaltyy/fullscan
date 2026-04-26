#!/usr/bin/env python3
"""
Macro & Sentiment Collector
Sources: FRED (economic indicators), yfinance (VIX, indices, commodities), CNN (Fear & Greed)
Backfills 6 months on first run. Prints daily change report.
"""

import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

# Allow running as:  python -m collectors.macro_sentiment  OR  python collectors/macro_sentiment.py
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
import requests
import yfinance as yf

from db.db_utils import get_db, ensure_schema

try:
    from fredapi import Fred
    HAS_FRED = True
except ImportError:
    HAS_FRED = False

# ── Config ──────────────────────────────────────────────────────────
FRED_API_KEY = os.environ.get("FRED_API_KEY", "")
BACKFILL_DAYS = 180

FRED_SERIES = {
    "CPI":                  "CPIAUCSL",
    "CORE_CPI":             "CPILFESL",
    "FED_FUNDS_RATE":       "DFF",
    "UNEMPLOYMENT":         "UNRATE",
    "GDP":                  "GDP",
    "NONFARM_PAYROLLS":     "PAYEMS",
    "INITIAL_CLAIMS":       "ICSA",
    "RETAIL_SALES":         "RSAFS",
    "INDUSTRIAL_PRODUCTION":"INDPRO",
    "CONSUMER_SENTIMENT":   "UMCSENT",
    "PPI":                  "PPIACO",
    "M2_MONEY_SUPPLY":      "M2SL",
    "10Y_TREASURY":         "DGS10",
    "2Y_TREASURY":          "DGS2",
    "YIELD_CURVE":          "T10Y2Y",
    "HY_SPREAD":            "BAMLH0A0HYM2",
}

YFINANCE_TICKERS = {
    "VIX":              "^VIX",
    "SP500":            "^GSPC",
    "NASDAQ":           "^IXIC",
    "DOW":              "^DJI",
    "RUSSELL_2000":     "^RUT",
    "CRUDE_OIL":        "CL=F",
    "GOLD":             "GC=F",
    "SILVER":           "SI=F",
    "COPPER":           "HG=F",
    "NATURAL_GAS":      "NG=F",
    "CORN":             "ZC=F",
    "WHEAT":            "ZW=F",
    "SOYBEANS":         "ZS=F",
    "US_DOLLAR_INDEX":  "DX-Y.NYB",
    "20Y_BOND_ETF":     "TLT",
    "BITCOIN":          "BTC-USD",
}

FEAR_GREED_URL = "https://production.dataviz.cnn.io/index/fearandgreed/graphdata"

REPORT_SECTIONS = {
    "MARKET SENTIMENT":  ["VIX", "FEAR_GREED", "HY_SPREAD"],
    "MAJOR INDICES":     ["SP500", "NASDAQ", "DOW", "RUSSELL_2000"],
    "RATES & BONDS":     ["FED_FUNDS_RATE", "10Y_TREASURY", "2Y_TREASURY", "YIELD_CURVE", "20Y_BOND_ETF"],
    "MACRO INDICATORS":  [
        "CPI", "CORE_CPI", "UNEMPLOYMENT", "GDP", "NONFARM_PAYROLLS",
        "INITIAL_CLAIMS", "RETAIL_SALES", "INDUSTRIAL_PRODUCTION",
        "CONSUMER_SENTIMENT", "PPI", "M2_MONEY_SUPPLY",
    ],
    "COMMODITIES":       ["CRUDE_OIL", "GOLD", "SILVER", "COPPER", "NATURAL_GAS", "CORN", "WHEAT", "SOYBEANS"],
    "CURRENCY & CRYPTO": ["US_DOLLAR_INDEX", "BITCOIN"],
}


# ── Database helpers ────────────────────────────────────────────────
def latest_date(conn, indicator):
    row = conn.execute(
        "SELECT MAX(date) FROM macro_indicators WHERE indicator=?", (indicator,)
    ).fetchone()
    return row[0] if row and row[0] else None


def upsert(conn, indicator, series_id, date_str, value, source):
    conn.execute(
        "INSERT OR REPLACE INTO macro_indicators(indicator,series_id,date,value,source) VALUES(?,?,?,?,?)",
        (indicator, series_id, date_str, value, source),
    )


# ── Collectors ──────────────────────────────────────────────────────
def collect_fred(conn):
    if not HAS_FRED:
        print("  SKIP: fredapi not installed (pip install fredapi)")
        return 0
    if not FRED_API_KEY:
        print("  SKIP: FRED_API_KEY not set")
        return 0

    fred = Fred(api_key=FRED_API_KEY)
    today = datetime.now()
    total = 0

    for name, sid in FRED_SERIES.items():
        ld = latest_date(conn, name)
        start = (datetime.strptime(ld, "%Y-%m-%d") + timedelta(days=1)) if ld else (today - timedelta(days=BACKFILL_DAYS))
        try:
            data = fred.get_series(sid, observation_start=start.strftime("%Y-%m-%d"))
            n = 0
            for dt, val in data.items():
                if pd.notna(val):
                    upsert(conn, name, sid, dt.strftime("%Y-%m-%d"), float(val), "FRED")
                    n += 1
            total += n
            print(f"  {name:<25} +{n}")
        except Exception as e:
            print(f"  {name:<25} ERROR: {e}")
        time.sleep(0.2)

    conn.commit()
    return total


def collect_yfinance(conn):
    today = datetime.now()
    total = 0

    for name, ticker in YFINANCE_TICKERS.items():
        ld = latest_date(conn, name)
        start = (datetime.strptime(ld, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d") if ld else (today - timedelta(days=BACKFILL_DAYS)).strftime("%Y-%m-%d")
        end = (today + timedelta(days=1)).strftime("%Y-%m-%d")

        try:
            df = yf.download(ticker, start=start, end=end, progress=False, auto_adjust=True)
            if df.empty:
                print(f"  {name:<25} no new data")
                continue

            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)

            n = 0
            for dt_idx, row in df.iterrows():
                date_str = dt_idx.strftime("%Y-%m-%d") if hasattr(dt_idx, "strftime") else str(dt_idx)[:10]
                val = float(row["Close"])
                upsert(conn, name, ticker, date_str, val, "yfinance")
                n += 1
            total += n
            print(f"  {name:<25} +{n}")
        except Exception as e:
            print(f"  {name:<25} ERROR: {e}")

    conn.commit()
    return total


def collect_fear_greed(conn):
    try:
        resp = requests.get(
            FEAR_GREED_URL,
            headers={"User-Agent": "Mozilla/5.0 (compatible; fullscan/1.0)"},
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()

        n = 0
        fg = data.get("fear_and_greed", {})
        score = fg.get("score")
        if score is not None:
            upsert(conn, "FEAR_GREED", "cnn_fg", datetime.now().strftime("%Y-%m-%d"), float(score), "CNN")
            n += 1

        for point in data.get("fear_and_greed_historical", {}).get("data", []):
            ts, val = point.get("x"), point.get("y")
            if ts and val is not None:
                date_str = datetime.utcfromtimestamp(ts / 1000).strftime("%Y-%m-%d")
                upsert(conn, "FEAR_GREED", "cnn_fg", date_str, float(val), "CNN")
                n += 1

        conn.commit()
        print(f"  {'FEAR_GREED':<25} +{n}")
        return n
    except Exception as e:
        print(f"  {'FEAR_GREED':<25} ERROR: {e}")
        return 0


# ── Change report ───────────────────────────────────────────────────
def get_historical_value(conn, indicator, ref_date, window_days=7):
    earliest = (ref_date - timedelta(days=window_days)).isoformat()
    row = conn.execute(
        "SELECT value FROM macro_indicators WHERE indicator=? AND date<=? AND date>=? ORDER BY date DESC LIMIT 1",
        (indicator, ref_date.isoformat(), earliest),
    ).fetchone()
    return row[0] if row else None


def fmt_change(current, previous):
    if current is None or previous is None or previous == 0:
        return "N/A"
    pct = ((current - previous) / abs(previous)) * 100
    return f"{pct:+.1f}%"


def fmt_val(value, indicator):
    if value is None:
        return "N/A"
    if abs(value) >= 1_000_000_000:
        return f"{value/1e9:,.1f}B"
    if abs(value) >= 1_000_000:
        return f"{value/1e6:,.1f}M"
    if abs(value) >= 10_000:
        return f"{value:,.0f}"
    return f"{value:,.2f}"


def print_report(conn):
    today = datetime.now().date()
    periods = {"Day": 1, "Week": 7, "Month": 30, "Quarter": 91}

    print(f"\n{'='*80}")
    print(f"  MACRO & SENTIMENT REPORT — {today}")
    print(f"{'='*80}")

    for section_name, indicators in REPORT_SECTIONS.items():
        print(f"\n  {section_name}")
        print(f"  {'Indicator':<25}{'Current':>12}{'Day':>9}{'Week':>9}{'Month':>9}{'Quarter':>9}")
        print(f"  {'-'*73}")

        for ind in indicators:
            row = conn.execute(
                "SELECT value, date FROM macro_indicators WHERE indicator=? ORDER BY date DESC LIMIT 1",
                (ind,),
            ).fetchone()

            if not row or row[0] is None:
                print(f"  {ind:<25}{'N/A':>12}")
                continue

            current_val = row[0]
            current_date = datetime.strptime(row[1], "%Y-%m-%d").date()
            val_str = fmt_val(current_val, ind)

            changes = {}
            for pname, days in periods.items():
                ref = current_date - timedelta(days=days)
                prev = get_historical_value(conn, ind, ref)
                changes[pname] = fmt_change(current_val, prev)

            print(f"  {ind:<25}{val_str:>12}{changes['Day']:>9}{changes['Week']:>9}{changes['Month']:>9}{changes['Quarter']:>9}")

    total = conn.execute("SELECT COUNT(DISTINCT indicator) FROM macro_indicators").fetchone()[0]
    points = conn.execute("SELECT COUNT(*) FROM macro_indicators").fetchone()[0]
    earliest = conn.execute("SELECT MIN(date) FROM macro_indicators").fetchone()[0]
    print(f"\n  Coverage: {total} indicators, {points} data points, from {earliest}")
    print(f"{'='*80}\n")


# ── Main ────────────────────────────────────────────────────────────
def main():
    start_time = time.time()
    conn = get_db()
    ensure_schema(conn)

    print("=== FRED Economic Indicators ===")
    fred_n = collect_fred(conn)

    print("\n=== Market Data (yfinance) ===")
    yf_n = collect_yfinance(conn)

    print("\n=== CNN Fear & Greed Index ===")
    fg_n = collect_fear_greed(conn)

    duration = time.time() - start_time
    total_n = fred_n + yf_n + fg_n

    conn.execute(
        "INSERT INTO collection_log(collector,status,records_added,duration_sec) VALUES(?,?,?,?)",
        ("macro_sentiment", "ok", total_n, round(duration, 1)),
    )
    conn.commit()

    print_report(conn)
    print(f"Done in {duration:.1f}s — {total_n} records added/updated")

    conn.close()


if __name__ == "__main__":
    main()
