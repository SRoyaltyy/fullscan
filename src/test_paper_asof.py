"""Sparse price rows must not zero the classic paper book.

Run: python -m src.test_paper_asof
"""
from __future__ import annotations

import json
import tempfile
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd

from src import paper_trade as pt


def test_asof_close_ignores_sparse_tail() -> None:
    idx = pd.to_datetime(["2026-09-11", "2026-09-14"])
    prices = pd.DataFrame(
        {"AAA": [10.0, float("nan")], "BBB": [20.0, 21.0]},
        index=idx,
    )
    row = pt.asof_closes(prices, "2026-09-14")
    assert row["AAA"] == 10.0
    assert row["BBB"] == 21.0
    earlier = pt.asof_closes(prices, "2026-09-11")
    assert earlier["AAA"] == 10.0
    assert earlier["BBB"] == 20.0


def test_intraday_last_is_not_a_session_close() -> None:
    et = ZoneInfo("America/New_York")
    intraday = datetime(2026, 9, 22, 10, 15, tzinfo=et)
    assert pt.accept_session_print("2026-09-22", intraday, 100) is None
    closed = datetime(2026, 9, 22, 16, 0, tzinfo=et)
    assert pt.accept_session_print("2026-09-22", closed, 100) == 100.0
    assert pt.accept_session_print("2026-09-21", closed, 100) is None
    assert pt.accept_session_print("2026-09-22", closed, 0) is None


def test_patch_fills_null_daily_close_only() -> None:
    orig = pt._chart_session_close

    def fake(ticker: str, bar_date: str) -> float | None:
        assert bar_date == "2026-09-22"
        return {"SPY": 773.38, "AAA": 10.5, "BBB": 4.0}.get(str(ticker).upper())

    pt._chart_session_close = fake
    try:
        idx = pd.to_datetime(["2026-09-21", "2026-09-22"])
        panel = pd.DataFrame(
            {"AAA": [10.0, float("nan")], "SPY": [770.0, float("nan")]},
            index=idx,
        )
        out = pt._patch_end_session(panel, ["AAA", "BBB", "SPY"], "2026-09-22")
        end = pd.Timestamp("2026-09-22")
        assert out.at[end, "AAA"] == 10.5
        assert out.at[pd.Timestamp("2026-09-21"), "AAA"] == 10.0
        assert abs(float(out.at[end, "SPY"]) - 773.38) < 1e-6
        assert out.at[end, "BBB"] == 4.0
    finally:
        pt._chart_session_close = orig


def test_open_session_does_not_invent_a_tail_row() -> None:
    orig = pt._chart_session_close
    pt._chart_session_close = lambda ticker, bar_date: None
    try:
        idx = pd.to_datetime(["2026-09-21"])
        panel = pd.DataFrame({"AAA": [10.0]}, index=idx)
        out = pt._patch_end_session(panel, ["AAA"], "2026-09-22")
        assert pd.Timestamp("2026-09-22") not in out.index
        assert out.at[pd.Timestamp("2026-09-21"), "AAA"] == 10.0
    finally:
        pt._chart_session_close = orig


def _book(tickers: list[str]) -> dict:
    buy = [{"ticker": t} for t in tickers]
    books = {}
    for h in pt.HORIZONS:
        books[h] = {"buy": buy if h == "1d" else [], "buy_by_size": {}}
    return {"meta": {}, "books": books}


def test_sparse_tail_still_buys_and_sells() -> None:
    orig = pt.load_day_meta
    pt.load_day_meta = lambda date: {}
    try:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            b1 = root / "2026-09-11_stock_book.json"
            b2 = root / "2026-09-14_stock_book.json"
            b1.write_text(json.dumps(_book(["AAA"])), encoding="utf-8")
            b2.write_text(json.dumps(_book(["BBB"])), encoding="utf-8")
            idx = pd.to_datetime(["2026-09-11", "2026-09-14"])
            prices = pd.DataFrame(
                {
                    "AAA": [10.0, float("nan")],
                    "BBB": [20.0, float("nan")],
                    "SPY": [100.0, float("nan")],
                },
                index=idx,
            )
            # Same-day print wins when the tail row actually has it.
            prices.loc[pd.Timestamp("2026-09-14"), "BBB"] = 21.0
            fees = pt.load_fees()
            ix = {"2026-09-11": 0, "2026-09-14": 1}
            _st, _curve, trades = pt.run_sim(
                [("2026-09-11", b1), ("2026-09-14", b2)],
                prices, 10_000, 10, fees, session_ix=ix,
            )
    finally:
        pt.load_day_meta = orig
    buys = [t for t in trades if t["side"] == "buy" and t["ticker"] == "BBB"]
    sells = [t for t in trades if t["side"] == "sell" and t["ticker"] == "AAA"]
    assert len(buys) == 1 and buys[0]["date"] == "2026-09-14"
    assert buys[0]["price"] == 21.0
    assert len(sells) == 1 and sells[0]["date"] == "2026-09-14"
    assert sells[0]["price"] == 10.0


def test_hole_fill_is_not_capped_at_120(tmp: Path | None = None) -> None:
    orig_cache = pt.PRICE_CACHE
    orig_dir = pt.PAPER_DIR
    orig_yahoo = pt._yahoo_close_panel
    orig_official = pt._official_close_panel
    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            idx = pd.to_datetime(["2026-09-11"])
            pd.DataFrame({"SPY": [1.0]}, index=idx).to_csv(root / "prices_cache.csv")
            pt.PRICE_CACHE = root / "prices_cache.csv"
            pt.PAPER_DIR = root
            pt._official_close_panel = lambda *a, **k: pd.DataFrame()
            seen: dict[str, int] = {}

            def fake(tickers, start, end):
                seen["n"] = len(tickers)
                idx2 = pd.to_datetime(["2026-09-11", "2026-09-14"])
                data = {t: [10.0, 11.0] for t in tickers}
                return pd.DataFrame(data, index=idx2)

            pt._yahoo_close_panel = fake
            names = [f"T{i:03d}" for i in range(150)]
            out = pt.get_prices(names, "2026-09-11", "2026-09-14")
            assert seen["n"] == 150
            assert float(out.loc[pd.Timestamp("2026-09-14"), "T000"]) == 11.0
            # Existing cache print wins over the hole-fill.
            assert float(out.loc[pd.Timestamp("2026-09-11"), "SPY"]) == 1.0
    finally:
        pt.PRICE_CACHE = orig_cache
        pt.PAPER_DIR = orig_dir
        pt._yahoo_close_panel = orig_yahoo
        pt._official_close_panel = orig_official


def main() -> None:
    test_asof_close_ignores_sparse_tail()
    test_intraday_last_is_not_a_session_close()
    test_patch_fills_null_daily_close_only()
    test_open_session_does_not_invent_a_tail_row()
    test_sparse_tail_still_buys_and_sells()
    test_hole_fill_is_not_capped_at_120()
    print("test_paper_asof: 6 ok")


if __name__ == "__main__":
    main()
