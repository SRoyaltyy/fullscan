"""20-day prior OHLC ripper ranker is leak-free."""
from __future__ import annotations

from src import flatten_lookback_action as fla
from src import gainer_capture as gc
from src import ohlc_ripper as ohlc
from src import sleeve_merge as sm


def test_features_use_prior_bars_only() -> None:
    feat = ohlc.features("TLN", "2026-08-14")
    assert feat["ok"] is True
    bars = ohlc.prior_bars("TLN", "2026-08-14")
    assert bars
    assert all(b["date"] < "2026-08-14" for b in bars)
    assert "ret_5" in feat and "rvol" in feat and "hot_score" in feat


def test_too_extended_cuts_exploded_tape() -> None:
    assert ohlc.too_extended({"ret_5": 22.0, "rvol": 1.0}) is True
    assert ohlc.too_extended({"ret_5": 4.0, "rvol": 3.1}) is True
    assert ohlc.too_extended({"ret_5": 4.0, "rvol": 1.2}) is False


def test_814_watchlist_keeps_earn_and_adds_ohlc_hot() -> None:
    payload = sm.load_payload()
    books = sm.list_books()
    cal = sm.session_calendar(payload, books)
    plan = fla.flatten_day_targets("2026-08-14")
    movers = fla.collect_mover_buys(payload, "2026-08-13", "2026-08-14", top_n=25)
    wl = gc.watchlist(
        "2026-08-14", cal=cal,
        flatten_picks=plan["tickers"],
        mover_buys=movers.get("by_date", {}).get("2026-08-14") or [],
    )
    ticks = set(wl["tickers"])
    assert len(ticks & {"CAPR", "CELC", "HTFL", "NMAX", "NPWR", "NU"}) >= 3
    assert wl.get("n_ohlc_hot", 0) >= 10
    assert any("ohlc_hot" in (r.get("reasons") or []) for r in wl["rows"])
    assert plan["tickers"][:3] == ["TLN", "VST", "NRG"]
    row = next(r for r in wl["rows"] if r["ticker"] in ticks)
    assert "ohlc_hot_score" in row
    assert "ohlc_ret_5" in row


def test_html_has_ohlc_columns() -> None:
    from src.test_flatten_lookback_action import _sample_payload
    page = fla.render_html(_sample_payload())
    assert ">5d%<" in page or ">5d%" in page
    assert "RVOL" in page
    assert "OHLC" in page


if __name__ == "__main__":
    test_features_use_prior_bars_only()
    test_too_extended_cuts_exploded_tape()
    test_814_watchlist_keeps_earn_and_adds_ohlc_hot()
    test_html_has_ohlc_columns()
    print("4 ohlc-ripper tests passed")
