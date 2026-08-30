"""Ticker-first lookback regression tests against committed artifacts."""
from __future__ import annotations

import tempfile
from pathlib import Path

from openpyxl import load_workbook

from src import ticker_lookback as tl
from src import ticker_lookback_run as run


def test_enriched_ab_dates_are_indexed() -> None:
    assert "2026-08-24" in tl.session_dates()
    idx = tl.build_index()
    sess = next(x for x in idx["sessions"] if x["date"] == "2026-08-24")
    assert sess["n_ab"] > 1000


def test_any_finviz_name_gets_cards_without_book() -> None:
    payload = run.scan_tickers(
        ["AAPL"], from_date="2026-08-24", to_date="2026-08-25")
    rec = payload["names"][0]
    assert rec["n_sessions"] == 2
    assert rec["n_with_print"] == 2
    assert all("finviz" in d["sources"] for d in rec["days"])
    assert all(len(d.get("finviz_factors") or {}) >= 10 for d in rec["days"])
    assert all(len(d.get("ab_factors") or {}) >= 10 for d in rec["days"])


def test_phone_html_and_returns() -> None:
    payload = run.scan_tickers(
        ["AAPL"], from_date="2026-08-19", to_date="2026-08-20")
    page = run.render_html(payload)
    assert 'name="viewport"' in page
    assert "🟢 positive" in page
    assert "<th>1d</th><th>3d</th><th>1w</th>" in page
    assert "AAPL" in page
    assert payload["names"][0]["days"][0]["forward_returns"]["1d"] is not None
    changes = payload["names"][0]["days"][0]["price_changes"]
    assert changes["price"] is not None
    assert changes["1d"] is not None
    with tempfile.TemporaryDirectory() as d:
        p = Path(d) / "lookback.xlsx"
        run.write_xlsx(payload, p)
        wb = load_workbook(p)
        assert "AAPL" in wb.sheetnames
        assert wb["AAPL"]["C2"].value is not None


if __name__ == "__main__":
    test_enriched_ab_dates_are_indexed()
    test_any_finviz_name_gets_cards_without_book()
    test_phone_html_and_returns()
    print("3 tests passed")
