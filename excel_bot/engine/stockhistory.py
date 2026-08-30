"""STOCKHISTORY-compatible data layer.

Replicates Excel's STOCKHISTORY(ticker, start, end, interval, headers, 0,
0,1,2,3,4,5) semantics on top of Yahoo Finance daily OHLCV:
  - interval 0 = daily rows, interval 1 = weekly aggregation (Mon-Sun:
    date=first trading day, open=first, high=max, low=min, close=last,
    volume=sum)
  - headers=1 -> first row: Date, Close, Open, High, Low, Volume
  - dates returned as Excel serial integers (ascending)
"""
import csv
import json
import os
import subprocess
import sys
from datetime import date, datetime, timedelta

YF_TOOL = (r"C:\Users\user\AppData\Roaming\kimi-desktop\daimon-share\daimon"
           r"\runtime\kimi-code\home\plugins\managed\yahoo_finance\scripts"
           r"\yahoo_finance_tool.py")
DATA_DIR = "data"

PROPS = ["Date", "Close", "Open", "High", "Low", "Volume"]  # property order 0-5


def serial(d: date) -> int:
    return (datetime(d.year, d.month, d.day) - datetime(1899, 12, 30)).days


def fetch_daily(ticker, start: date, end: date, workspace=".") -> list:
    """Fetch daily OHLCV from Yahoo datasource (cached as CSV). Returns list of
    dicts {date, open, high, low, close, volume} ascending."""
    os.makedirs(os.path.join(workspace, DATA_DIR), exist_ok=True)
    safe = ticker.replace(":", "_").replace("/", "_")
    csv_path = os.path.abspath(os.path.join(
        workspace, DATA_DIR, f"{safe}_{start}_{end}.csv"))
    if not os.path.exists(csv_path):
        params = {"ticker": ticker, "start_date": str(start), "end_date": str(end),
                  "interval": "1d", "file_path": csv_path}
        pf = os.path.join(workspace, DATA_DIR, "params.json")
        json.dump(params, open(pf, "w"))
        r = subprocess.run([sys.executable, YF_TOOL, "call",
                            "--api-name", "get_historical_stock_prices",
                            "--params-file", pf],
                           capture_output=True, text=True, timeout=180,
                           encoding="utf-8", errors="replace")
        if not os.path.exists(csv_path):
            raise RuntimeError(f"data fetch failed for {ticker}: "
                               f"{r.stdout[-300:]} {r.stderr[-300:]}")
    rows = []
    with open(csv_path, newline="", encoding="utf-8") as fh:
        for rec in csv.DictReader(fh):
            try:
                d = datetime.fromisoformat(rec["Date"].replace("Z", "+00:00")).date()
                rows.append({
                    "date": d,
                    "open": float(rec["Open"]),
                    "high": float(rec["High"]),
                    "low": float(rec["Low"]),
                    "close": float(rec["Close"]),
                    "volume": float(rec["Volume"]),
                })
            except (ValueError, KeyError):
                continue
    rows.sort(key=lambda r: r["date"])
    return rows


def weekly_aggregate(rows):
    """Excel STOCKHISTORY weekly: ISO weeks (Mon start)."""
    weeks = {}
    order = []
    for r in rows:
        monday = r["date"] - timedelta(days=r["date"].weekday())
        if monday not in weeks:
            weeks[monday] = []
            order.append(monday)
        weeks[monday].append(r)
    out = []
    for monday in order:
        wk = weeks[monday]
        out.append({
            "date": wk[0]["date"],
            "open": wk[0]["open"],
            "high": max(x["high"] for x in wk),
            "low": min(x["low"] for x in wk),
            "close": wk[-1]["close"],
            "volume": sum(x["volume"] for x in wk),
        })
    return out


def stockhistory_array(ticker, start: date, end: date, interval: int,
                       workspace="."):
    """Full STOCKHISTORY-equivalent 2D array incl. header row.
    Columns: Date, Close, Open, High, Low, Volume (property order 0,1,2,3,4,5).
    Dates as Excel serials."""
    rows = fetch_daily(ticker, start, end, workspace)
    rows = [r for r in rows if start <= r["date"] <= end]
    if interval == 1:
        rows = weekly_aggregate(rows)
    grid = [list(PROPS)]
    for r in rows:
        grid.append([serial(r["date"]), r["close"], r["open"],
                     r["high"], r["low"], r["volume"]])
    return grid


def make_provider(workspace="."):
    """Evaluator STOCKHISTORY hook: vals = [ticker, start_serial, end_serial,
    interval, headers, ...] -> 2D array."""
    def provider(vals):
        ticker = str(vals[0])
        start = (datetime(1899, 12, 30) + timedelta(days=float(vals[1]))).date()
        end = (datetime(1899, 12, 30) + timedelta(days=float(vals[2]))).date()
        interval = int(vals[3])
        grid = stockhistory_array(ticker, start, end, interval, workspace)
        if int(vals[4]) == 0:  # headers=0 -> strip header row
            grid = grid[1:]
        return grid
    return provider
