"""Fresh Yahoo split-adjusted bars for breadth_mine_v1b.

The pinned store at data/prices/ohlc.parquet keeps the first print it
stored. A later split does not rewrite those rows, so one ticker can
mix pre-split and post-split prices. This module pulls one snapshot
for the tickers the study and the earlier fill records use, with
Yahoo auto_adjust left false: split-adjusted, dividends not applied.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[1]
BAR_DIR = ROOT / "research" / "breadth_mine_v1b" / "bars"
BAR_FILE = BAR_DIR / "ohlc.parquet"
SPLIT_FILE = BAR_DIR / "splits.json"
# Later print over earlier print. A same-bar close/open is the other leg.
RATIO_HI = 3.0
RATIO_LO = 1.0 / 3.0
# A split explains a jump when the price factor is within this relative gap.
SPLIT_TOL = 0.25
# An open fill changes when the fresh open differs by more than this.
FILL_REL = 0.001
FILL_ABS = 0.01
SCHEMA = pa.schema([
    ("date", pa.string()),
    ("ticker", pa.string()),
    ("open", pa.float64()),
    ("high", pa.float64()),
    ("low", pa.float64()),
    ("close", pa.float64()),
    ("volume", pa.float64()),
])


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_bars(rows: list[dict], path: Path = BAR_FILE) -> None:
    rows = sorted(rows, key=lambda row: (row["ticker"], row["date"]))
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.table(
        {
            "date": [row["date"] for row in rows],
            "ticker": [row["ticker"] for row in rows],
            "open": [float(row["open"]) for row in rows],
            "high": [float(row["high"]) for row in rows],
            "low": [float(row["low"]) for row in rows],
            "close": [float(row["close"]) for row in rows],
            "volume": [float(row["volume"]) for row in rows],
        },
        schema=SCHEMA,
    )
    pq.write_table(table, path, compression="zstd")


def write_splits(events: list[dict], path: Path = SPLIT_FILE) -> None:
    clean = []
    for event in events:
        factor = float(event["split"])
        if factor <= 0 or abs(factor - 1.0) <= 1e-12:
            continue
        clean.append({
            "date": str(event["date"])[:10],
            "split": round(factor, 8),
            "ticker": str(event["ticker"]).upper(),
        })
    clean.sort(key=lambda row: (row["ticker"], row["date"], row["split"]))
    # One event per ticker and date. The first factor wins.
    seen = set()
    unique = []
    for row in clean:
        key = (row["ticker"], row["date"])
        if key in seen:
            continue
        seen.add(key)
        unique.append(row)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(unique, indent=2) + "\n", encoding="utf-8")


def load_bars(path: Path = BAR_FILE) -> dict[str, list[dict]]:
    table = pq.read_table(path, schema=SCHEMA)
    frame = table.to_pydict()
    out: dict[str, list[dict]] = {}
    for i, ticker in enumerate(frame["ticker"]):
        out.setdefault(ticker, []).append({
            "date": frame["date"][i],
            "open": float(frame["open"][i]),
            "high": float(frame["high"][i]),
            "low": float(frame["low"][i]),
            "close": float(frame["close"][i]),
            "volume": float(frame["volume"][i]),
        })
    return out


def load_splits(path: Path = SPLIT_FILE) -> dict[tuple[str, str], float]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    return {(row["ticker"], row["date"]): float(row["split"]) for row in raw}


def _ratio(later: float, earlier: float) -> float | None:
    if earlier <= 0 or later <= 0:
        return None
    return later / earlier


def _extreme(ratio: float | None) -> bool:
    if ratio is None:
        return False
    return ratio > RATIO_HI or ratio < RATIO_LO


def price_factor(split: float) -> float:
    """Yahoo's stock-split column is the share factor. Price moves by 1/split."""
    if split <= 0:
        return 1.0
    return 1.0 / split


def split_explains(ratio: float, split: float) -> bool:
    factor = price_factor(split)
    for candidate in (factor, split):
        if candidate <= 0:
            continue
        if abs(ratio - candidate) / candidate <= SPLIT_TOL:
            return True
    return False


def scan_jumps(bars: dict[str, list[dict]], splits: dict[tuple[str, str], float]) -> list[dict]:
    """Flag a later print when the consecutive open/close ratio breaks 3x.

    The series for a ticker is open, close, open, close, in date order.
    The two legs are close/open on the same bar, and open divided by the
    previous bar's close. The date is the later print's session. A Yahoo
    split on that date explains the leg when the price factor matches.
    """
    flags = []
    for ticker in sorted(bars):
        rows = bars[ticker]
        prev_close = None
        prev_date = None
        for row in rows:
            opx = float(row["open"])
            cpx = float(row["close"])
            if prev_close is not None:
                ratio = _ratio(opx, prev_close)
                if _extreme(ratio):
                    flags.append(_flag(ticker, row["date"], "open_over_prev_close", ratio, prev_date, splits))
            ratio = _ratio(cpx, opx)
            if _extreme(ratio):
                flags.append(_flag(ticker, row["date"], "close_over_open", ratio, row["date"], splits))
            prev_close = cpx
            prev_date = row["date"]
    flags.sort(key=lambda row: (row["ticker"], row["date"], row["leg"]))
    return flags


def _flag(ticker: str, date: str, leg: str, ratio: float, _anchor: str | None,
          splits: dict[tuple[str, str], float]) -> dict:
    split = splits.get((ticker, date))
    explained = split is not None and split_explains(ratio, split)
    return {
        "date": date,
        "explained": explained,
        "leg": leg,
        "ratio": round(float(ratio), 6),
        "split": None if split is None else round(float(split), 8),
        "ticker": ticker,
    }


def unexplained(flags: list[dict]) -> list[dict]:
    return [row for row in flags if not row["explained"]]


def opens_index(bars: dict[str, list[dict]]) -> dict[tuple[str, str], float]:
    return {
        (ticker, row["date"]): float(row["open"])
        for ticker, rows in bars.items()
        for row in rows
    }


def fill_changed(fill_price: float, fresh_open: float | None) -> bool:
    if fresh_open is None or fresh_open <= 0 or fill_price <= 0:
        return True
    gap = abs(fresh_open - fill_price)
    return gap > FILL_ABS and gap / fill_price > FILL_REL


def fetch_snapshot(tickers: list[str], start: str, end: str) -> tuple[list[dict], list[dict], list[str]]:
    """Download one consistent Yahoo snapshot. end is exclusive."""
    import yfinance as yf

    from src.price_store import _flatten_actions, _flatten_yf

    bars: list[dict] = []
    splits: list[dict] = []
    missing: list[str] = []
    batch = 40
    pending = [ticker.strip().upper() for ticker in tickers if ticker.strip()]
    # Deduplicate, keep order.
    seen = set()
    ordered = []
    for ticker in pending:
        if ticker not in seen:
            seen.add(ticker)
            ordered.append(ticker)
    pending = ordered
    while pending:
        chunk = pending[:batch]
        pending = pending[batch:]
        try:
            raw = yf.download(
                tickers=chunk, start=start, end=end, group_by="ticker",
                auto_adjust=False, actions=True, threads=True, progress=False,
                repair=False,
            )
        except Exception as exc:
            print(f"batch failed {chunk[0]}..{chunk[-1]}: {exc}", flush=True)
            if len(chunk) == 1:
                missing.append(chunk[0])
                continue
            pending = chunk + pending
            batch = max(1, batch // 2)
            continue
        if raw is None or getattr(raw, "empty", True):
            missing.extend(chunk)
            continue
        flat = _flatten_yf(raw, chunk)
        got = set()
        if flat is not None and len(flat):
            for row in flat.itertuples(index=False):
                day = str(row.date)[:10]
                opx = float(row.open) if row.open == row.open else 0.0
                high = float(row.high) if row.high == row.high else opx
                low = float(row.low) if row.low == row.low else opx
                close = float(row.close) if row.close == row.close else 0.0
                volume = float(row.volume) if row.volume == row.volume else 0.0
                if close <= 0 or opx <= 0:
                    continue
                ticker = str(row.ticker).upper()
                got.add(ticker)
                bars.append({
                    "date": day, "ticker": ticker, "open": opx, "high": high,
                    "low": low, "close": close, "volume": volume,
                })
        actions = _flatten_actions(raw, chunk)
        if actions is not None and len(actions):
            for row in actions.itertuples(index=False):
                factor = float(row.split) if row.split == row.split else 0.0
                if factor <= 0 or abs(factor - 1.0) <= 1e-12:
                    continue
                splits.append({
                    "date": str(row.date)[:10],
                    "ticker": str(row.ticker).upper(),
                    "split": factor,
                })
        missing.extend([ticker for ticker in chunk if ticker not in got])
        print(f"got {len(got)}/{len(chunk)} rows {len(bars)} left {len(pending)}", flush=True)
    return bars, splits, missing


def pinned_opens(path: Path) -> dict[tuple[str, str], float]:
    import pandas as pd

    frame = pd.read_parquet(path, columns=["date", "ticker", "open"])
    frame["d"] = pd.to_datetime(frame["date"]).dt.strftime("%Y-%m-%d")
    out = {}
    for ticker, day, opx in zip(frame["ticker"], frame["d"], frame["open"], strict=True):
        px = float(opx)
        if px > 0:
            out[(str(ticker).upper(), day)] = px
    return out
