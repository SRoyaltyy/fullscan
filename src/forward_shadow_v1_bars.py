"""Pinned Yahoo split-adjusted bars for forward_shadow_v1.

auto_adjust stays false. Open, high, low, and close are the split-adjusted
columns. Dividends are not applied. A consecutive stored session whose
open/previous-close jumps above 3x or below 1/3 must be explained by a
Yahoo split between those sessions.
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta

from src.forward_shadow_v1 import ForwardError, bars_dir
from src.price_store import AUTO_ADJUST

RATIO_HI = 3.0
RATIO_LO = 1.0 / 3.0
SPLIT_TOL = 0.25


def price_factor(split: float) -> float:
    if split <= 0:
        return 1.0
    return 1.0 / split


def _explains(ratio: float, factor: float) -> bool:
    if factor <= 0:
        return False
    return abs(ratio - factor) / factor <= SPLIT_TOL


def split_explains(ratio: float, splits: list[float]) -> bool:
    """True when the share-factor product, or any one split, matches the jump."""
    if not splits:
        return False
    price_prod = 1.0
    share_prod = 1.0
    for split in splits:
        if split <= 0:
            continue
        price_prod *= price_factor(split)
        share_prod *= split
    if _explains(ratio, price_prod) or _explains(ratio, share_prod):
        return True
    for split in splits:
        if _explains(ratio, price_factor(split)) or _explains(ratio, split):
            return True
    return False


def session_jumps(docs: list[dict]) -> list[dict]:
    """Jump from the previous stored session close to this session open."""
    by_ticker: dict[str, list[tuple[str, float, float]]] = {}
    splits: dict[tuple[str, str], float] = {}
    for doc in docs:
        session = str(doc.get("session") or "")
        for row in doc.get("bars") or []:
            ticker = str(row.get("ticker") or "").upper()
            if not ticker or not session:
                continue
            opx = float(row.get("open") or 0)
            close = float(row.get("close") or 0)
            if opx <= 0 or close <= 0:
                continue
            by_ticker.setdefault(ticker, []).append((session, opx, close))
        for event in doc.get("splits") or []:
            ticker = str(event.get("ticker") or "").upper()
            day = str(event.get("date") or "")[:10]
            factor = float(event.get("split") or 0)
            if ticker and day and factor > 0 and abs(factor - 1.0) > 1e-12:
                splits[(ticker, day)] = factor
    flags = []
    for ticker in sorted(by_ticker):
        rows = sorted(by_ticker[ticker], key=lambda item: item[0])
        prev = None
        for session, opx, close in rows:
            if prev is not None and prev[2] > 0 and opx > 0:
                ratio = opx / prev[2]
                if ratio > RATIO_HI or ratio < RATIO_LO:
                    factors = [
                        factor for (name, day), factor in splits.items()
                        if name == ticker and prev[0] < day <= session
                    ]
                    flags.append({
                        "date": session,
                        "explained": split_explains(ratio, factors),
                        "ratio": round(float(ratio), 6),
                        "ticker": ticker,
                    })
            prev = (session, opx, close)
    return flags


def unexplained(docs: list[dict]) -> list[dict]:
    return [row for row in session_jumps(docs) if not row["explained"]]


def assert_split_consistent(docs: list[dict]) -> None:
    bad = unexplained(docs)
    if bad:
        sample = ", ".join(f"{row['ticker']} {row['date']}" for row in bad[:8])
        raise ForwardError(
            f"IRONCLAD 26: {len(bad)} unexplained consecutive-session jumps, including {sample}"
        )


def load_docs(root: Path | None = None) -> list[dict]:
    directory = bars_dir(root)
    docs = []
    if not directory.is_dir():
        return docs
    for path in sorted(directory.glob("*.json")):
        docs.append(json.loads(path.read_text(encoding="utf-8")))
    return docs


def check_stored(root: Path | None = None) -> None:
    assert_split_consistent(load_docs(root))


def _next_day(session: str) -> str:
    day = datetime.strptime(session, "%Y-%m-%d") + timedelta(days=1)
    return day.date().isoformat()


def pin_session(session: str, tickers: list[str], *, fetch=None) -> dict:
    """Download one session. The caller writes the file once."""
    if AUTO_ADJUST:
        raise ForwardError("auto_adjust must stay false so dividends are not applied")
    names = []
    seen = set()
    for ticker in tickers:
        name = str(ticker or "").upper().strip()
        if name and name not in seen:
            seen.add(name)
            names.append(name)
    if fetch is None:
        fetch = fetch_yahoo
    bars, splits, missing = fetch(names, session, _next_day(session))
    kept = []
    for row in bars:
        if str(row.get("date") or "")[:10] != session:
            continue
        if float(row.get("open") or 0) <= 0 or float(row.get("close") or 0) <= 0:
            continue
        kept.append({
            "close": round(float(row["close"]), 6),
            "date": session,
            "high": round(float(row.get("high") or row["close"]), 6),
            "low": round(float(row.get("low") or row["close"]), 6),
            "open": round(float(row["open"]), 6),
            "ticker": str(row["ticker"]).upper(),
            "volume": float(row.get("volume") or 0),
        })
    kept.sort(key=lambda row: row["ticker"])
    split_rows = []
    for event in splits:
        factor = float(event.get("split") or 0)
        if factor <= 0 or abs(factor - 1.0) <= 1e-12:
            continue
        split_rows.append({
            "date": str(event["date"])[:10],
            "split": round(factor, 8),
            "ticker": str(event["ticker"]).upper(),
        })
    split_rows.sort(key=lambda row: (row["ticker"], row["date"], row["split"]))
    return {
        "adjustment": "split-adjusted, dividends not applied",
        "auto_adjust": False,
        "bars": kept,
        "missing": sorted(set(missing)),
        "session": session,
        "splits": split_rows,
    }


def fetch_yahoo(tickers: list[str], start: str, end: str):
    """Yahoo daily bars. ``end`` is exclusive. auto_adjust is false."""
    import yfinance as yf

    from src.price_store import _flatten_actions, _flatten_yf

    if not tickers:
        return [], [], []
    bars: list[dict] = []
    splits: list[dict] = []
    missing: list[str] = []
    batch = 40
    pending = list(tickers)
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
            print(f"yahoo batch failed {chunk[0]}: {exc}", flush=True)
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
                opx = float(row.open) if row.open == row.open else 0.0
                close = float(row.close) if row.close == row.close else 0.0
                if opx <= 0 or close <= 0:
                    continue
                ticker = str(row.ticker).upper()
                got.add(ticker)
                bars.append({
                    "close": close,
                    "date": str(row.date)[:10],
                    "high": float(row.high) if row.high == row.high else opx,
                    "low": float(row.low) if row.low == row.low else opx,
                    "open": opx,
                    "ticker": ticker,
                    "volume": float(row.volume) if row.volume == row.volume else 0.0,
                })
        actions = _flatten_actions(raw, chunk)
        if actions is not None and len(actions):
            for row in actions.itertuples(index=False):
                factor = float(row.split) if row.split == row.split else 0.0
                if factor <= 0 or abs(factor - 1.0) <= 1e-12:
                    continue
                splits.append({
                    "date": str(row.date)[:10],
                    "split": factor,
                    "ticker": str(row.ticker).upper(),
                })
        missing.extend([ticker for ticker in chunk if ticker not in got])
    return bars, splits, missing


def main(argv: list[str] | None = None) -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Check pinned forward_shadow bars")
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args(argv)
    check_stored(None)
    print("split consistent")
    del args


if __name__ == "__main__":
    main()
