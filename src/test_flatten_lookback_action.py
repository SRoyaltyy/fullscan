"""Flatten lookback: 8-14 is 3d robust, not ASTS; date/source/cameras."""
from __future__ import annotations

from src import flatten_lookback_action as fla
from src import lookback_action as act


def test_flatten_814_is_tln_not_asts() -> None:
    plan = fla.flatten_day_targets("2026-08-14")
    names = [t.upper() for t in plan["tickers"]]
    assert plan["route"] == "io"
    assert plan["flatten_ok"] is False
    assert names[:3] == ["TLN", "VST", "NRG"]
    assert "ASTS" not in names
    assert "SITM" not in names
    assert "DOCN" not in names


def test_flatten_813_is_3d_book_not_2w_ino_method() -> None:
    plan = fla.flatten_day_targets("2026-08-13")
    names = [t.upper() for t in plan["tickers"]]
    assert plan["route"] == "io"
    assert "BTSG" in names
    assert names[0] != "INO"


def test_hard_red_still_exposes_wishlist() -> None:
    plan = fla.flatten_day_targets("2026-08-18")
    assert plan["route"] == "hold"
    assert plan["hard_red"] is True
    assert plan["flatten_ok"] is False
    assert plan["tickers"], "wish-list must still print on hard-red sit"


def test_filter_rows_date_and_source() -> None:
    rows = [
        {"date": "2026-08-14", "ticker": "TLN", "sources": ["flatten", "gainers"]},
        {"date": "2026-08-14", "ticker": "ASTS", "sources": ["gainers"]},
        {"date": "2026-08-20", "ticker": "AG", "sources": ["flatten", "movers"]},
        {"date": "2026-08-14", "ticker": "AAPL", "sources": ["custom"]},
    ]
    only_flat = fla.filter_rows(rows, source="flatten")
    assert [r["ticker"] for r in only_flat] == ["TLN", "AG"]
    day = fla.filter_rows(rows, source="flatten", date="2026-08-14")
    assert [r["ticker"] for r in day] == ["TLN"]
    gain = fla.filter_rows(rows, source="gainers", date="2026-08-14")
    assert {r["ticker"] for r in gain} == {"TLN", "ASTS"}
    custom = fla.filter_rows(rows, source="custom", tickers=["AAPL"])
    assert [r["ticker"] for r in custom] == ["AAPL"]


def _sample_payload() -> dict:
    return {
        "generated_at": "t",
        "policy": "flatten_robust",
        "from_date": "2026-08-14",
        "to_date": "2026-08-14",
        "n_sessions": 1,
        "n_flatten_days": 1,
        "n_gainers": 1,
        "n_movers": 0,
        "preset": "featured",
        "session_dates": ["2026-08-14"],
        "custom_tickers": [],
        "daily": [{
            "date": "2026-08-14", "score": 5.5, "route": "io",
            "flatten_ok": False, "n_priced_buys": 0,
            "have_prior_book": True,
            "tickers": ["TLN", "VST", "NRG"],
            "why": "no flatten (0 priced BUYs, prior book=yes)",
        }],
        "rows": [{
            "date": "2026-08-14",
            "ticker": "TLN",
            "sources": ["flatten"],
            "flatten_route": "io",
            "flatten_rank": 1,
            "flatten_why": "no flatten (0 priced BUYs, prior book=yes)",
            "action_call": "HOLD",
            "action_label": "HOLD · 2026-08-14 09:30 ET",
            "action_reason": "no featured long / fade / lane",
            "cond_tally": "2/3/1",
            "lane": "standard",
            "lane_label": "standard",
            "session_bar": {"open": 200.0, "close": 205.0, "close_open_pct": 2.5},
            "horizon_dates": {"1d": "2026-08-17", "3d": "2026-08-19", "1w": "2026-08-21"},
            "price_changes": {"1d": 1.0, "3d": 2.0, "1w": 3.0},
            "hits": {"1d": None, "3d": None, "1w": None},
            "boxes": {"join": "good", "vol": "good", "ab": "neutral"},
            "domains": {"market": "good", "setup": "neutral"},
            "labeled": "join🟢 vol🟢",
            "labeled_domains": "mkt🟢 set🟡",
            "marks_cell": "🔵 — —",
            "marks": {"blue": True, "alarm": False, "white": False, "have_compare": True},
            "setups": [{"id": "pair:vol=good|ab=good", "short": "vol+AB", "verdict": "long"}],
        }],
    }


def test_html_has_toggles_cameras_and_date_filter() -> None:
    page = fla.render_html(_sample_payload())
    assert 'data-source="flatten"' in page
    assert 'data-source="gainers"' in page
    assert 'data-source="movers"' in page
    assert 'data-source="custom"' in page
    assert 'id="dateSel"' in page
    assert 'value="2026-08-14"' in page
    assert 'id="tickerIn"' in page
    assert "TLN" in page
    assert "join" in page
    assert "Hall pass" in page
    assert "Hits 1d/3d/1w" in page
    assert "Action 09:30 ET" in page
    assert "data-date=\"2026-08-14\"" in page
    assert "data-src=\"flatten\"" in page
    md = fla.render_markdown(_sample_payload(), source="flatten", date="2026-08-14")
    assert "TLN" in md
    assert "Cameras" in md or "join" in md
    assert act.OPEN_CLOCK in md


if __name__ == "__main__":
    test_flatten_814_is_tln_not_asts()
    test_flatten_813_is_3d_book_not_2w_ino_method()
    test_hard_red_still_exposes_wishlist()
    test_filter_rows_date_and_source()
    test_html_has_toggles_cameras_and_date_filter()
    print("5 flatten-lookback-action tests passed")
