"""Split-consistency gate for the cleaned breadth_mine_v1e tape.

A consecutive-session jump is this open divided by the previous close.
The gate fails unless every such jump is explained by a Yahoo split
on the later date.
"""
from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BAR_FILE = ROOT / "research" / "breadth_mine_v1e" / "bars" / "ohlc.parquet"
SPLIT_FILE = ROOT / "research" / "breadth_mine_v1e" / "bars" / "splits.json"
SPLIT_TOL = 0.25


def split_explains(ratio: float, split: float) -> bool:
    if split <= 0:
        return False
    factor = 1.0 / split
    for candidate in (factor, split):
        if candidate > 0 and abs(ratio - candidate) / candidate <= SPLIT_TOL:
            return True
    return False


def load_bars() -> dict[str, list[dict]]:
    import pandas as pd

    frame = pd.read_parquet(BAR_FILE)
    frame["date"] = frame["date"].astype(str).str.slice(0, 10)
    frame["ticker"] = frame["ticker"].astype(str).str.upper()
    bars: dict[str, list[dict]] = {}
    for ticker, group in frame.groupby("ticker", sort=False):
        group = group.sort_values("date")
        bars[str(ticker)] = [
            {"date": str(date), "open": float(op), "close": float(cl)}
            for date, op, cl in zip(group["date"], group["open"], group["close"])
        ]
    return bars


def load_splits() -> dict[tuple[str, str], float]:
    raw = json.loads(SPLIT_FILE.read_text(encoding="utf-8"))
    out = {}
    for key, value in raw.items():
        ticker, date = key.split("|", 1)
        out[(ticker, date)] = float(value)
    return out


def session_jumps(bars: dict[str, list[dict]] | None = None,
                  splits: dict[tuple[str, str], float] | None = None) -> list[dict]:
    if bars is None:
        bars = load_bars()
    if splits is None:
        splits = load_splits()
    flags = []
    for ticker in sorted(bars):
        prev = None
        for row in bars[ticker]:
            if prev is not None and prev["close"] > 0 and row["open"] > 0:
                ratio = row["open"] / prev["close"]
                if ratio > 3.0 or ratio < 1.0 / 3.0:
                    split = splits.get((ticker, row["date"]))
                    explained = split is not None and split_explains(ratio, split)
                    flags.append({
                        "date": row["date"],
                        "explained": explained,
                        "ratio": round(float(ratio), 6),
                        "ticker": ticker,
                    })
            prev = row
    return flags


def unexplained(flags: list[dict] | None = None) -> list[dict]:
    if flags is None:
        flags = session_jumps()
    return [row for row in flags if not row["explained"]]


def assert_split_consistent() -> None:
    bad = unexplained()
    if bad:
        sample = ", ".join(f"{row['ticker']} {row['date']}" for row in bad[:8])
        raise SystemExit(f"IRONCLAD 26: {len(bad)} unexplained jumps, including {sample}")


if __name__ == "__main__":
    assert_split_consistent()
