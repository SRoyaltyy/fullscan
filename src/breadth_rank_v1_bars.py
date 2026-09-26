"""Fresh Yahoo split-adjusted bars for breadth_rank_v1.

auto_adjust is false, the same setting the price store locks: open,
high, low, and close are split-adjusted, and dividends are not applied.
Adj Close is not stored. A ticker with any consecutive-session jump
above 3x or below 1/3 is dropped, so the committed file is one scale.
IRONCLAD 26 still fails the run when an unexplained jump remains.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from src.price_store import AUTO_ADJUST, _flatten_actions, _flatten_yf

ROOT = Path(__file__).resolve().parents[1]
BAR_DIR = ROOT / "research" / "breadth_rank_v1" / "bars"
BAR_FILE = BAR_DIR / "ohlc.parquet"
SPLIT_FILE = BAR_DIR / "splits.json"
DROPPED_FILE = BAR_DIR / "DROPPED.json"
RATIO_HI = 3.0
RATIO_LO = 1.0 / 3.0
SPLIT_TOL = 0.25
FETCH_START = "2026-03-02"
FETCH_END = "2026-09-26"
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


def write_dropped(payload: dict, path: Path = DROPPED_FILE) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def load_bars(path: Path = BAR_FILE, max_date: str | None = None) -> dict[str, list[dict]]:
    """Load bars. max_date keeps rows on or before that date and drops the rest.

    The train phase passes 2026-09-13 so a bar dated on or after 2026-09-14
    is not returned.
    """
    filters = None
    if max_date is not None:
        filters = [("date", "<=", max_date)]
    table = pq.read_table(path, schema=SCHEMA, filters=filters)
    frame = table.to_pydict()
    out: dict[str, list[dict]] = {}
    for i, ticker in enumerate(frame["ticker"]):
        day = frame["date"][i]
        if max_date is not None and day > max_date:
            raise SystemExit(f"bar dated {day} passed the {max_date} cutoff")
        out.setdefault(ticker, []).append({
            "date": day,
            "open": float(frame["open"][i]),
            "high": float(frame["high"][i]),
            "low": float(frame["low"][i]),
            "close": float(frame["close"][i]),
            "volume": float(frame["volume"][i]),
        })
    for rows in out.values():
        rows.sort(key=lambda row: row["date"])
    return out


def load_splits(path: Path = SPLIT_FILE) -> dict[tuple[str, str], float]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    return {(row["ticker"], row["date"]): float(row["split"]) for row in raw}


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


def session_jumps(bars: dict[str, list[dict]],
                  splits: dict[tuple[str, str], float] | None = None) -> list[dict]:
    """Flag this session's open divided by the previous session's close."""
    if splits is None:
        splits = {}
    flags = []
    for ticker in sorted(bars):
        prev = None
        for row in bars[ticker]:
            if prev is not None and prev["close"] > 0 and row["open"] > 0:
                ratio = row["open"] / prev["close"]
                if ratio > RATIO_HI or ratio < RATIO_LO:
                    split = splits.get((ticker, row["date"]))
                    explained = split is not None and split_explains(ratio, split)
                    flags.append({
                        "date": row["date"],
                        "explained": explained,
                        "ratio": round(float(ratio), 6),
                        "split": None if split is None else round(float(split), 8),
                        "ticker": ticker,
                    })
            prev = row
    return flags


def unexplained(flags: list[dict] | None = None,
                bars: dict[str, list[dict]] | None = None,
                splits: dict[tuple[str, str], float] | None = None) -> list[dict]:
    if flags is None:
        if bars is None:
            bars = load_bars()
        if splits is None:
            splits = load_splits()
        flags = session_jumps(bars, splits)
    return [row for row in flags if not row["explained"]]


def assert_split_consistent(bars: dict[str, list[dict]] | None = None,
                            splits: dict[tuple[str, str], float] | None = None) -> None:
    bad = unexplained(bars=bars, splits=splits)
    if bad:
        sample = ", ".join(f"{row['ticker']} {row['date']}" for row in bad[:8])
        raise SystemExit(
            f"IRONCLAD 26: {len(bad)} unexplained consecutive-session jumps, including {sample}"
        )


def drop_extreme(rows: list[dict], splits: list[dict]) -> tuple[list[dict], list[dict], dict]:
    """Drop every ticker with a 3x consecutive-session jump.

    An explained split jump is dropped too. Keeping it would leave two
    scales in one series. IWM is never dropped; a jump there stops the fetch.
    """
    by_ticker: dict[str, list[dict]] = {}
    for row in rows:
        by_ticker.setdefault(row["ticker"], []).append(row)
    for ticker_rows in by_ticker.values():
        ticker_rows.sort(key=lambda row: row["date"])
    split_map = {(row["ticker"], row["date"]): float(row["split"]) for row in splits}
    flags = session_jumps(by_ticker, split_map)
    dropped = sorted({row["ticker"] for row in flags})
    if "IWM" in dropped:
        raise SystemExit("IWM has a consecutive-session jump and cannot be the baseline")
    kept_rows = [row for row in rows if row["ticker"] not in set(dropped)]
    kept_splits = [row for row in splits if row["ticker"] not in set(dropped)]
    payload = {
        "dropped": dropped,
        "jumps": flags,
        "n_dropped": len(dropped),
        "rule": "drop any ticker with open/previous-close above 3x or below 1/3",
    }
    return kept_rows, kept_splits, payload


def fetch_snapshot(tickers: list[str], start: str = FETCH_START, end: str = FETCH_END):
    """Download one Yahoo snapshot. end is exclusive."""
    if AUTO_ADJUST:
        raise SystemExit("auto_adjust must stay false so dividends are not applied")
    import yfinance as yf

    bars: list[dict] = []
    splits: list[dict] = []
    missing: list[str] = []
    batch = 40
    seen = set()
    pending = []
    for ticker in tickers:
        name = ticker.strip().upper()
        if name and name not in seen:
            seen.add(name)
            pending.append(name)
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


def fetch_and_write(tickers: list[str]) -> dict:
    rows, splits, missing = fetch_snapshot(tickers)
    if "IWM" not in {row["ticker"] for row in rows}:
        raise SystemExit("IWM missing from the Yahoo snapshot")
    kept_rows, kept_splits, dropped = drop_extreme(rows, splits)
    dropped["missing_download"] = sorted(set(missing) - set(dropped["dropped"]))
    write_bars(kept_rows)
    write_splits(kept_splits)
    write_dropped(dropped)
    loaded = load_bars()
    assert_split_consistent(loaded, load_splits())
    print(
        f"wrote {len(kept_rows)} bars, {len(kept_splits)} splits, "
        f"dropped {dropped['n_dropped']}, missing {len(dropped['missing_download'])}",
        flush=True,
    )
    return dropped


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--fetch", action="store_true")
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    if args.check or not args.fetch:
        assert_split_consistent()
        print("split consistent")
        return
    from src.breadth_rank_v1_inputs import collect_tickers

    tickers = collect_tickers()
    print(f"tickers {len(tickers)}", flush=True)
    fetch_and_write(tickers)


if __name__ == "__main__":
    main()
