#!/usr/bin/env python3
"""
Macro & Sentiment Collector
Sources: FRED (economic indicators), yfinance (VIX, indices, commodities), CNN (Fear & Greed)
Backfills 6 months on first run. Prints daily change report.
"""

import os
import sys
import time
from datetime import datetime, timedelta, date, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
import requests
import yfinance as yf

from db.connection import get_connection

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
def _to_date(val):
    """Normalize a date value (date object or string) to datetime.date."""
    if val is None:
        return None
    if isinstance(val, date):
        return val
    return datetime.strptime(str(val)[:10], "%Y-%m-%d").date()


def latest_date(cur, indicator):
    cur.execute("SELECT MAX(date) FROM macro_indicators WHERE indicator=%s", (indicator,))
    row = cur.fetchone()
    return _to_date(row[0]) if row and row[0] else None


def upsert(cur, indicator, series_id, date_str, value, source):
    cur.execute(
        """INSERT INTO macro_indicators(indicator, series_id, date, value, source)
           VALUES(%s, %s, %s, %s, %s)
           ON CONFLICT (indicator, date) DO UPDATE SET
               value = EXCLUDED.value,
               source = EXCLUDED.source,
               collected_at = NOW()""",
        (indicator, series_id, date_str, value, source),
    )


# ── Collectors ──────────────────────────────────────────────────────
def collect_fred(conn, cur):
    if not HAS_FRED:
        print("  SKIP: fredapi not installed (pip install fredapi)")
        return 0
    if not FRED_API_KEY:
        print("  SKIP: FRED_API_KEY not set")
        return 0

    fred = Fred(api_key=FRED_API_KEY)
    today = datetime.now().date()
    total = 0

    for name, sid in FRED_SERIES.items():
        ld = latest_date(cur, name)
        start = (ld + timedelta(days=1)) if ld else (today - timedelta(days=BACKFILL_DAYS))
        try:
            data = fred.get_series(sid, observation_start=start.strftime("%Y-%m-%d"))
            n = 0
            for dt, val in data.items():
                if pd.notna(val):
                    upsert(cur, name, sid, dt.strftime("%Y-%m-%d"), float(val), "FRED")
                    n += 1
            total += n
            print(f"  {name:<25} +{n}")
        except Exception as e:
            conn.rollback()
            print(f"  {name:<25} ERROR: {e}")
        time.sleep(0.2)

    conn.commit()
    return total


def collect_yfinance(conn, cur):
    today = datetime.now().date()
    total = 0

    for name, ticker in YFINANCE_TICKERS.items():
        ld = latest_date(cur, name)
        start = (ld + timedelta(days=1)).strftime("%Y-%m-%d") if ld else (today - timedelta(days=BACKFILL_DAYS)).strftime("%Y-%m-%d")
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
                upsert(cur, name, ticker, date_str, val, "yfinance")
                n += 1
            total += n
            print(f"  {name:<25} +{n}")
        except Exception as e:
            conn.rollback()
            print(f"  {name:<25} ERROR: {e}")

    conn.commit()
    return total


def collect_fear_greed(conn, cur):
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
            upsert(cur, "FEAR_GREED", "cnn_fg", datetime.now().strftime("%Y-%m-%d"), float(score), "CNN")
            n += 1

        for point in data.get("fear_and_greed_historical", {}).get("data", []):
            ts, val = point.get("x"), point.get("y")
            if ts and val is not None:
                date_str = datetime.fromtimestamp(ts / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
                upsert(cur, "FEAR_GREED", "cnn_fg", date_str, float(val), "CNN")
                n += 1

        conn.commit()
        print(f"  {'FEAR_GREED':<25} +{n}")
        return n
    except Exception as e:
        conn.rollback()
        print(f"  {'FEAR_GREED':<25} ERROR: {e}")
        return 0


# ── Change report ───────────────────────────────────────────────────
def get_historical_value(cur, indicator, ref_date, window_days=7):
    earliest = ref_date - timedelta(days=window_days)
    cur.execute(
        "SELECT value FROM macro_indicators WHERE indicator=%s AND date<=%s AND date>=%s ORDER BY date DESC LIMIT 1",
        (indicator, ref_date, earliest),
    )
    row = cur.fetchone()
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


def print_report(cur):
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
            cur.execute(
                "SELECT value, date FROM macro_indicators WHERE indicator=%s ORDER BY date DESC LIMIT 1",
                (ind,),
            )
            row = cur.fetchone()

            if not row or row[0] is None:
                print(f"  {ind:<25}{'N/A':>12}")
                continue

            current_val = row[0]
            current_date = _to_date(row[1])
            val_str = fmt_val(current_val, ind)

            changes = {}
            for pname, days in periods.items():
                ref = current_date - timedelta(days=days)
                prev = get_historical_value(cur, ind, ref)
                changes[pname] = fmt_change(current_val, prev)

            print(f"  {ind:<25}{val_str:>12}{changes['Day']:>9}{changes['Week']:>9}{changes['Month']:>9}{changes['Quarter']:>9}")

    cur.execute("SELECT COUNT(DISTINCT indicator) FROM macro_indicators")
    total = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM macro_indicators")
    points = cur.fetchone()[0]
    cur.execute("SELECT MIN(date) FROM macro_indicators")
    earliest = cur.fetchone()[0]
    print(f"\n  Coverage: {total} indicators, {points} data points, from {earliest}")
    print(f"{'='*80}\n")


# ── Main ────────────────────────────────────────────────────────────
def main():
    start_time = time.time()
    conn = get_connection()
    cur = conn.cursor()

    print("=== FRED Economic Indicators ===")
    fred_n = collect_fred(conn, cur)

    print("\n=== Market Data (yfinance) ===")
    yf_n = collect_yfinance(conn, cur)

    print("\n=== CNN Fear & Greed Index ===")
    fg_n = collect_fear_greed(conn, cur)

    duration = time.time() - start_time
    total_n = fred_n + yf_n + fg_n

    cur.execute(
        "INSERT INTO collection_log(collector, status, records_added, duration_sec) VALUES(%s,%s,%s,%s)",
        ("macro_sentiment", "ok", total_n, round(duration, 1)),
    )
    conn.commit()

    print_report(cur)
    print(f"Done in {duration:.1f}s — {total_n} records added/updated")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
