"""Ticker-first lookback regression tests against committed artifacts."""
from __future__ import annotations

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
    assert "Finviz full-market factors" in page
    assert "After signal close" in page
    assert "AAPL" in page
    assert payload["names"][0]["days"][0]["forward_returns"]["1d"] is not None


if __name__ == "__main__":
    test_enriched_ab_dates_are_indexed()
    test_any_finviz_name_gets_cards_without_book()
    test_phone_html_and_returns()
    print("3 tests passed")
