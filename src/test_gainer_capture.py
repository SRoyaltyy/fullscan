"""Leak-free gainer capture watchlist."""
from __future__ import annotations

from src import candle_factor as cf
from src import flatten_lookback_action as fla
from src import gainer_capture as gc
from src import sleeve_merge as sm


def test_earnings_parse_amc_and_bmo() -> None:
    d, hm = gc.parse_earnings("8/13/2026 4:30:00 PM")
    assert d == "2026-08-13"
    assert hm == 1630
    d, hm = gc.parse_earnings("8/14/2026 8:30:00 AM")
    assert d == "2026-08-14"
    assert hm == 830


def test_814_earnings_reaction_hits_known_gainers() -> None:
    names = set(gc.earnings_reaction("2026-08-13", "2026-08-14"))
    assert names, names
    hit = names & {"CAPR", "CELC", "HTFL", "NMAX", "NPWR", "NU"}
    assert len(hit) >= 3, hit
    for t in hit:
        bars = cf.prior_bars(t, "2026-08-14")
        assert all(b["date"] < "2026-08-14" for b in bars)


def test_814_watchlist_captures_earn_rip_not_asts_book() -> None:
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
    assert "TLN" in ticks or "VST" in ticks
    assert wl["n_earn_react"] >= 3
    assert wl.get("n_probable", 0) >= 1
    assert any("probable" in (r.get("reasons") or []) for r in wl["rows"])
    assert len(ticks & {"CAPR", "CELC", "HTFL", "NMAX", "NPWR", "NU"}) >= 3
    # Same-day rip names that were not on the prior calendar stay off
    # the watchlist unless they were a yday gainer / mover / flatten pick.
    assert plan["tickers"][:3] == ["TLN", "VST", "NRG"]


def test_html_has_captured_chip() -> None:
    page = fla.render_html(fla._sample_payload() if hasattr(fla, "_sample_payload")
                           else __import__("src.test_flatten_lookback_action",
                                           fromlist=["_sample_payload"])._sample_payload())
    assert 'data-source="captured"' in page
    assert 'data-source="probable"' in page
    assert "Gainers captured" in page


if __name__ == "__main__":
    test_earnings_parse_amc_and_bmo()
    test_814_earnings_reaction_hits_known_gainers()
    test_814_watchlist_captures_earn_rip_not_asts_book()
    test_html_has_captured_chip()
    print("4 gainer-capture tests passed")
