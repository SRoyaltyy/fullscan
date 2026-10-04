"""Display closes for the h1 page. Does not rewrite a sealed ledger line.

The close book is cash plus each lot at that session's close. Holdings
store ``last_px`` as the open, so the page must not use that field next
to the sealed equity. This file is the close (and the prior close, and
the 09:30 open) the page reads.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from research.hot_n4_clean_v4.forward.book import H1, reset_book, use_book
from research.hot_n4_clean_v4.forward.ledger import load

# Same pin as research.hot_n4_clean_v4.forward.prices.PIN_END. A later
# session is the jsonl print. This module does not import the price
# runner, which pulls the strategy code.
PIN_END = "2026-09-25"

OHLC = ROOT / "data" / "prices" / "ohlc.parquet"
OUT = H1.page / "closes.json"


def _book_rows(records: list[dict]) -> list[dict]:
    by: dict[str, dict] = {}
    for row in records:
        kind = row.get("kind")
        if kind == "session":
            by[row["date"]] = row
        elif kind == "mark" and row["date"] not in by:
            by[row["date"]] = row
        elif kind == "fill" and row["date"] not in by and "equity_primary" in row:
            by[row["date"]] = row
    return [by[day] for day in sorted(by)]


def _trades(records: list[dict]) -> dict[str, dict]:
    """Buys and sells for each day. A mark does not replace them."""
    out: dict[str, dict] = {}
    for row in records:
        if row.get("kind") in ("session", "open_fill", "fill"):
            out[row["date"]] = row
    return out


def _tickers(records: list[dict]) -> set[str]:
    names: set[str] = set()
    for row in records:
        kind = row.get("kind")
        if kind in ("session", "open_fill", "fill", "mark"):
            for key in ("buys", "sells", "holdings", "added_buys", "added_sells"):
                for leg in row.get(key) or []:
                    ticker = leg.get("ticker")
                    if ticker:
                        names.add(str(ticker))
        elif kind == "close" and row.get("ticker"):
            names.add(str(row["ticker"]))
    return names


def _stored(tickers: set[str]) -> dict[str, dict[str, dict]]:
    """Unadjusted parquet open/close, the print the close book marks on."""
    import pyarrow.parquet as pq

    if not tickers or not OHLC.is_file():
        return {}
    table = pq.read_table(
        OHLC,
        columns=["date", "ticker", "open", "close"],
        filters=[("ticker", "in", sorted(tickers))],
    )
    frame = table.to_pydict()
    by: dict[str, dict[str, dict]] = {}
    for day, ticker, op, close in zip(frame["date"], frame["ticker"], frame["open"], frame["close"]):
        iso = str(day)[:10]
        by.setdefault(str(ticker), {})[iso] = {"open": float(op), "close": float(close)}
    return by


def _jsonl_rows(folder: Path) -> list[dict]:
    path = folder / "prices.jsonl"
    if not path.is_file():
        return []
    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line:
            continue
        row = json.loads(line)
        rows.append(row)
    return rows


def _overlay(stored: dict[str, dict[str, dict]], folder: Path) -> None:
    """Post-pin jsonl replaces the parquet print. Pinned dates stay."""
    for row in _jsonl_rows(folder):
        if str(row["date"]) <= PIN_END:
            continue
        stored.setdefault(str(row["ticker"]), {})[str(row["date"])] = {
            "open": float(row["open"]),
            "close": float(row["close"]),
        }


def _prior(series: dict[str, dict], day: str) -> float | None:
    earlier = [iso for iso in series if iso < day]
    if not earlier:
        return None
    return float(series[max(earlier)]["close"])


def build_closes(records: list[dict], folder: Path | None = None) -> dict:
    folder = folder or H1.folder
    stored = _stored(_tickers(records))
    _overlay(stored, folder)
    books = _book_rows(records)
    trades = _trades(records)
    out: dict[str, dict] = {}
    prev_held: list[dict] = []
    gaps = []
    for book in books:
        day = book["date"]
        trade = trades.get(day) or {}
        names = {row["ticker"] for row in book.get("holdings") or []}
        names.update(row["ticker"] for row in trade.get("sells") or [])
        names.update(row["ticker"] for row in prev_held)
        day_out = {}
        for ticker in sorted(names):
            series = stored.get(ticker) or {}
            bar = series.get(day)
            if not bar:
                continue
            prior = _prior(series, day)
            day_out[ticker] = {
                "close": float(bar["close"]),
                "open": float(bar["open"]),
                "prior_close": prior,
            }
        out[day] = day_out
        if "equity_primary" in book:
            stock = 0.0
            missing = []
            for lot in book.get("holdings") or []:
                px = (day_out.get(lot["ticker"]) or {}).get("close")
                if px is None:
                    missing.append(lot["ticker"])
                    continue
                stock += int(lot["shares"]) * float(px)
            gap = float(book["equity_primary"]) - (float(book["cash_primary"]) + stock)
            if missing or abs(gap) > 0.02:
                gaps.append((day, round(gap, 4), missing))
        prev_held = list(book.get("holdings") or [])
    if gaps:
        detail = "; ".join(f"{day} gap {gap} missing {missing}" for day, gap, missing in gaps[:8])
        raise RuntimeError(f"close book does not add up: {detail}")
    return out


def write_closes(records: list[dict] | None = None) -> Path:
    token = use_book(H1)
    try:
        rows = load(H1.folder) if records is None else records
    finally:
        reset_book(token)
    payload = build_closes(rows, H1.folder)
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return OUT


def main() -> None:
    path = write_closes()
    print(f"h1 closes {path} days {len(json.loads(path.read_text()))}")


if __name__ == "__main__":
    main()
