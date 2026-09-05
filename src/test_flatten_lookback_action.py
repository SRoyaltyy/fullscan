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
        {"date": "2026-08-18", "ticker": "XYZ", "sources": ["losers"]},
        {"date": "2026-08-14", "ticker": "CAPR", "sources": ["captured", "gainers"]},
        {"date": "2026-08-14", "ticker": "AAPL", "sources": ["custom"]},
    ]
    only_flat = fla.filter_rows(rows, source="flatten")
    assert [r["ticker"] for r in only_flat] == ["TLN", "AG"]
    day = fla.filter_rows(rows, source="flatten", date="2026-08-14")
    assert [r["ticker"] for r in day] == ["TLN"]
    gain = fla.filter_rows(rows, source="gainers", date="2026-08-14")
    assert {r["ticker"] for r in gain} == {"TLN", "ASTS", "CAPR"}
    lose = fla.filter_rows(rows, source="losers")
    assert [r["ticker"] for r in lose] == ["XYZ"]
    cap = fla.filter_rows(rows, source="captured")
    assert [r["ticker"] for r in cap] == ["CAPR"]
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
        "n_losers": 0,
        "universe": "flatten",
        "tally": {
            "flatten_picks": 1, "winners": 1, "losers": 0, "flats": 0,
            "lose_rate": 0.0, "win_rate": 1.0,
            "gainers": {"universe": 1, "chosen": 1, "captured": 1,
                        "chosen_rate": 1.0, "captured_rate": 1.0},
            "movers": {"universe": 0, "chosen": 0, "captured": 0,
                       "chosen_rate": None, "captured_rate": None},
            "losers_tape": {"universe": 0, "chosen": 0, "captured": 0,
                            "chosen_rate": None, "captured_rate": None},
            "captured": {"universe": 4, "chosen": 1, "captured": 1,
                         "gainer_hits": 2, "loser_hits": 0},
            "low_s": {"picks": 0, "losers": 0, "lose_rate": None},
        },
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
            "day_change": 2.5,
            "outcome": "win",
            "candle_body_rg": 1.6,
            "candle_vol_rg": 1.2,
            "candle_capture": True,
            "candle_pattern": "engulf",
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
    assert 'data-source="losers"' in page
    assert 'data-source="captured"' in page
    assert 'data-source="custom"' in page
    assert "Gainers captured" in page
    assert "Gainers chosen" in page
    assert "Losers chosen" in page
    assert "Flatten lose rate" in page
    assert "R:G" in page
    assert "RVOL" in page
    assert "5d%" in page
    assert "engulf" in page
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
    assert "Chosen tally" in md
    assert "Top losers" in md


def test_build_tally_counts_chosen_and_lose_rate() -> None:
    rows = [
        {"ticker": "TLN", "sources": ["flatten", "gainers", "captured"], "outcome": "win",
         "flatten_score": 5.5, "flatten_ok": False, "candle_capture": True},
        {"ticker": "VST", "sources": ["flatten"], "outcome": "lose",
         "flatten_score": -4.0, "flatten_ok": False, "candle_capture": False},
        {"ticker": "ASTS", "sources": ["gainers"], "outcome": "win",
         "candle_capture": True},
        {"ticker": "XYZ", "sources": ["losers"], "outcome": "lose",
         "candle_capture": False},
        {"ticker": "AG", "sources": ["flatten", "movers"], "outcome": "win",
         "flatten_score": 1.2, "flatten_ok": True, "candle_capture": False},
        {"ticker": "CAPR", "sources": ["captured", "gainers"], "outcome": "win",
         "candle_capture": True},
    ]
    flat = [r for r in rows if "flatten" in r["sources"]]
    tally = fla._build_tally(rows, [{"date": "2026-08-14"}], flat)
    assert tally["flatten_picks"] == 3
    assert tally["winners"] == 2
    assert tally["losers"] == 1
    assert tally["lose_rate"] == 0.333
    assert tally["gainers"]["universe"] == 3
    assert tally["gainers"]["chosen"] == 1
    assert tally["gainers"]["captured"] == 3
    assert tally["losers_tape"]["chosen"] == 0
    assert tally["movers"]["chosen"] == 1
    assert tally["captured"]["gainer_hits"] == 2
    assert tally["captured"]["universe"] == 2


if __name__ == "__main__":
    test_flatten_814_is_tln_not_asts()
    test_flatten_813_is_3d_book_not_2w_ino_method()
    test_hard_red_still_exposes_wishlist()
    test_filter_rows_date_and_source()
    test_html_has_toggles_cameras_and_date_filter()
    test_build_tally_counts_chosen_and_lose_rate()
    print("6 flatten-lookback-action tests passed")
