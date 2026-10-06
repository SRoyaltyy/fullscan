"""Printed paper days stay byte-for-byte. A later run only appends.

Run: python -m src.test_paper_append
"""
from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from src import paper_trade as pt


def _book(tickers: list[str]) -> dict:
    buy = [{"ticker": t} for t in tickers]
    hb = {"buy": buy, "buy_by_size": {}}
    return {"books": {h: hb for h in pt.HORIZONS}, "meta": {}}


def _sleeve(cash: float = 5000.0) -> dict:
    return {
        "cash": cash,
        "pos": {
            "AAA": {
                "shares": 10,
                "entry_date": "2026-10-01",
                "entry_px": 10.0,
                "cost": 100.0,
            }
        },
        "realized": 0.0,
        "fees": 1.0,
        "trades": 1,
        "wins": 0,
        "closed": 0,
    }


def _prices() -> pd.DataFrame:
    idx = pd.to_datetime(["2026-10-01", "2026-10-02", "2026-10-06"])
    return pd.DataFrame(
        {"SPY": [100.0, 101.0, 102.0], "AAA": [10.0, 11.0, 12.0]},
        index=idx,
    )


def _layout(tmp: Path):
    paper = tmp / "paper"
    books = tmp / "books"
    paper.mkdir()
    books.mkdir()
    (books / "2026-10-01_stock_book.json").write_text(
        json.dumps(_book(["AAA"])), encoding="utf-8")
    (books / "2026-10-02_stock_book.json").write_text(
        json.dumps(_book(["AAA"])), encoding="utf-8")
    (books / "2026-10-06_stock_book.json").write_text(
        json.dumps(_book(["AAA"])), encoding="utf-8")
    state = {f"{h}_{k}": _sleeve() for h in pt.HORIZONS for k in ("top", "size")}
    (paper / "state.json").write_text(json.dumps(state), encoding="utf-8")
    header = "date,sleeve,equity,cash,invested,fees_cum,realized_cum\n"
    row = "2026-10-01,1d_top,5100.0,5000.0,100.0,1.0,0.0\n"
    spy = "2026-10-01,SPY (benchmark),10000.0,,,,\n"
    blob = (header + row + spy).encode("utf-8")
    (paper / "equity_curve.csv").write_bytes(blob)
    return paper, books, blob


def _patch(monkeypatch, tmp: Path, prices: pd.DataFrame):
    monkeypatch.setattr(pt, "PAPER_DIR", tmp / "paper")
    monkeypatch.setattr(pt, "BOOK_DIR", tmp / "books")
    monkeypatch.setattr(pt, "DASH_DIR", tmp / "dash")
    monkeypatch.setattr(pt, "SCOREBOARD", tmp / "score")
    monkeypatch.setattr(pt, "get_prices", lambda *a, **k: prices)
    monkeypatch.setattr(pt, "load_day_meta", lambda date: {})
    pt._META_CACHE.clear()
    monkeypatch.setattr(
        pt, "_SESSION_CAL", ["2026-10-01", "2026-10-02", "2026-10-06"], raising=False,
    )


def test_resume_appends_later_books_and_keeps_printed_bytes(tmp_path, monkeypatch):
    paper, _books, blob = _layout(tmp_path)
    _patch(monkeypatch, tmp_path, _prices())
    pt.run(date="2026-10-06", top_n=10, capital=10000)
    out = (paper / "equity_curve.csv").read_bytes()
    assert out.startswith(blob)
    text = out.decode("utf-8")
    assert "2026-10-02,1d_top," in text
    assert "2026-10-06,1d_top," in text
    assert "2026-10-05" not in text
    again = (paper / "equity_curve.csv").read_bytes()
    pt.run(date="2026-10-06", top_n=10, capital=10000)
    assert (paper / "equity_curve.csv").read_bytes() == again


def test_missing_state_refuses_to_rewrite_the_curve(tmp_path, monkeypatch):
    paper, _books, blob = _layout(tmp_path)
    (paper / "state.json").unlink()
    _patch(monkeypatch, tmp_path, _prices())
    try:
        pt.run(date="2026-10-06", top_n=10, capital=10000)
    except SystemExit as exc:
        assert "refusing to rewrite" in str(exc)
    else:
        raise AssertionError("missing state rewrote the curve")
    assert (paper / "equity_curve.csv").read_bytes() == blob


def test_unpriced_resume_does_not_skip_ahead(tmp_path, monkeypatch):
    paper, books, blob = _layout(tmp_path)
    # 10-02 has no bar at all, and nothing on or before it. 10-06 must stay missing.
    idx = pd.to_datetime(["2026-10-06"])
    prices = pd.DataFrame({"SPY": [102.0], "AAA": [12.0]}, index=idx)
    _patch(monkeypatch, tmp_path, prices)
    # Drop the 10-01 book so the only sessions after the curve are 10-02 then 10-06.
    (books / "2026-10-01_stock_book.json").unlink()
    pt.run(date="2026-10-06", top_n=10, capital=10000)
    assert (paper / "equity_curve.csv").read_bytes() == blob


if __name__ == "__main__":
    import tempfile

    class _MP:
        def setattr(self, obj, name, value):
            setattr(obj, name, value)

    with tempfile.TemporaryDirectory() as d:
        test_resume_appends_later_books_and_keeps_printed_bytes(Path(d), _MP())
    with tempfile.TemporaryDirectory() as d:
        test_missing_state_refuses_to_rewrite_the_curve(Path(d), _MP())
    with tempfile.TemporaryDirectory() as d:
        test_unpriced_resume_does_not_skip_ahead(Path(d), _MP())
    print("paper append ok")
