"""Combo morning tickets are the union of member pick_day lists.

Run: PYTHONPATH=. python3 -m src.test_strategy_tickets
"""
from __future__ import annotations

from unittest import mock

from src import strategy_tickets as st


class _FakeFM:
    def __init__(self, picks: dict):
        self.picks = picks

    def pick_day(self, rows, rec):
        return list(self.picks.get(rec["name"], []))


def test_combo_would_buy_unions_member_lists() -> None:
    rec_by = {
        "short_news_r_h3": {"name": "short_news_r_h3", "side": "short"},
        "union_e_fresh_h3": {"name": "union_e_fresh_h3", "side": "long"},
        "union_hot_n4_h1": {"name": "union_hot_n4_h1", "side": "long"},
    }
    fm = _FakeFM({
        "short_news_r_h3": [{"ticker": "SPIR"}, {"ticker": "AEHR"}],
        "union_e_fresh_h3": [{"ticker": "AVO"}],
        "union_hot_n4_h1": [{"ticker": "INTC"}, {"ticker": "AEHR"}],
    })
    buys = st._combo_would_buy(
        [], rec_by,
        ["short_news_r_h3", "union_e_fresh_h3", "union_hot_n4_h1"],
        {"net": "priority"}, fm,
    )
    names = [b["ticker"] for b in buys]
    assert names == ["SPIR", "AEHR", "AVO", "INTC"]
    assert next(b for b in buys if b["ticker"] == "AEHR")["src"] == "short_news_r_h3"


def test_combo_skip_drops_long_and_short_clash() -> None:
    rec_by = {
        "short_news_r_h3": {"name": "short_news_r_h3", "side": "short"},
        "union_e_fresh_h3": {"name": "union_e_fresh_h3", "side": "long"},
    }
    fm = _FakeFM({
        "short_news_r_h3": [{"ticker": "AEHR"}, {"ticker": "SPIR"}],
        "union_e_fresh_h3": [{"ticker": "AEHR"}, {"ticker": "AVO"}],
    })
    buys = st._combo_would_buy(
        [], rec_by, ["short_news_r_h3", "union_e_fresh_h3"],
        {"net": "skip"}, fm,
    )
    assert [b["ticker"] for b in buys] == ["SPIR", "AVO"]


def test_recipe_strats_never_emits_combo_needs_mine() -> None:
    """A combo is a union of member 09:30 lists — not a mine-only stub."""
    src = __import__("pathlib").Path(st.__file__).read_text(encoding="utf-8")
    assert "combo_needs_mine" not in src
    assert "_combo_would_buy" in src
    rows = st.recipe_strats("2026-09-10")
    combos = [r for r in rows if r["name"].startswith("combo_")]
    assert combos, "combo_specs should be attached even without a blotter md"
    bad = [r["name"] for r in combos if r.get("status") == "combo_needs_mine"]
    assert not bad, bad
    # At least one mix should inherit a member name that pick_day already lists.
    with_names = [r for r in combos if r.get("buy_n")]
    assert with_names, "expected at least one combo shopping list from the panel"


def test_source_uses_session_look_not_last_bake() -> None:
    src = __import__("pathlib").Path(st.__file__).read_text(encoding="utf-8")
    assert "older[-1]" not in src
    assert "_session_look" in src
    assert "resolve_rows" in src
    assert "assert_session_look" in src
    assert "clock_legal_for" in src
    assert "SESSION_OPEN_LOCK" in src


def test_session_look_uses_panel_when_session_present() -> None:
    rows = [{"ticker": "INDP"}]
    panel = {"by_date": {"2026-09-14": rows}, "to_date": "2026-09-14"}
    looked = st._session_look("2026-09-14", panel)
    assert looked["source"] == "panel"
    assert looked["rows"] == rows
    assert looked["stale"] is False
    assert looked["date"] == "2026-09-14"


def test_session_look_uses_open_rows_when_panel_stops_early() -> None:
    rows = [{"ticker": "INDP"}, {"ticker": "GPRO"}]
    panel = {
        "to_date": "2026-09-11",
        "session_dates": ["2026-09-11"],
        "by_date": {"2026-09-11": [{"ticker": "QRVO"}]},
    }
    with mock.patch("src.combo_broker.resolve_rows", return_value={
        "date": "2026-09-14", "rows": rows, "stale": False,
        "source": "look", "want_date": "2026-09-14",
    }) as resolve:
        looked = st._session_look("2026-09-14", panel)
    resolve.assert_called_once()
    assert looked["rows"] == rows
    assert looked["stale"] is False
    assert looked["date"] == "2026-09-14"
    assert looked["source"] == "look"


def test_session_look_refuses_last_bake() -> None:
    panel = {
        "to_date": "2026-09-11",
        "session_dates": ["2026-09-11"],
        "by_date": {"2026-09-11": [{"ticker": "QRVO"}]},
    }
    with mock.patch("src.combo_broker.resolve_rows", return_value={
        "date": "2026-09-11", "rows": [{"ticker": "QRVO"}],
        "stale": True, "source": "panel_asof", "want_date": "2026-09-14",
    }):
        looked = st._session_look("2026-09-14", panel)
    assert looked["rows"] == []
    assert looked["stale"] is True
    assert looked["date"] == "2026-09-14"
    assert "refusing last-bake" in (looked.get("error") or "")


def test_assert_session_look_blocks_stale_ship() -> None:
    payload = {
        "clock_legal_for": "2026-09-14",
        "look": {"source": "look", "stale": False, "panel_bake_date": "2026-09-11"},
        "strategies": {
            "union_hot_n4_h1": {
                "family": "factor_mine", "date": "2026-09-11",
                "clock_legal_for": "2026-09-11",
                "buy_n": 3, "buy": [{"ticker": "QRVO"}],
            },
            "stock_book_1d": {
                "family": "stock_book", "date": "2026-09-14",
                "buy_n": 1,
            },
        },
        "errors": [],
    }
    try:
        st.assert_session_look(payload, "2026-09-14")
    except AssertionError as e:
        assert "2026-09-14" in str(e)
        assert "union_hot_n4_h1" in str(e)
    else:
        raise AssertionError("expected refuse-to-ship")


def test_assert_fails_when_bake_is_not_session_open() -> None:
    payload = {
        "clock_legal_for": "2026-09-14",
        "look": {"source": "panel_asof", "stale": True,
                 "panel_bake_date": "2026-09-11", "date": "2026-09-11"},
        "strategies": {},
        "errors": [],
    }
    try:
        st.assert_session_look(payload, "2026-09-14")
    except AssertionError as e:
        assert "panel bake date" in str(e)
        assert "2026-09-14" in str(e)
    else:
        raise AssertionError("expected bake ≠ session fail")
    try:
        st.assert_session_look(
            {"look": {"source": "look", "stale": False}, "strategies": {}},
            "2026-09-14",
        )
    except AssertionError as e:
        assert "clock_legal_for" in str(e)
    else:
        raise AssertionError("expected missing clock_legal_for")


def test_open_lock_pins_indp_and_drops_friday() -> None:
    buys = [
        {"ticker": "QRVO", "kid_side": "short", "src": "short_news_r_macd_h3"},
        {"ticker": "CMRC", "kid_side": "long", "src": "union_hot_n4_h1"},
        {"ticker": "GPRO", "kid_side": "long", "src": "union_hot_n4_h1"},
        {"ticker": "BKV", "kid_side": "short", "src": "short_news_r_macd_h3"},
    ]
    out = st.apply_open_lock("2026-09-14", st.LIVE_WEBULL_COMBO, buys)
    names = [b["ticker"] for b in out]
    assert "QRVO" not in names and "MYGN" not in names
    longs = {b["ticker"] for b in out if b.get("kid_side") == "long"}
    shorts = {b["ticker"] for b in out if b.get("kid_side") == "short"}
    assert longs == {"INDP", "GPRO", "VERI", "HUT", "CMRC"}
    assert shorts == {"BKV", "AMD"}
    assert next(b for b in out if b["ticker"] == "INDP")["src"] == "hard_red_sit"
    # Other recipes / dates are untouched.
    assert st.apply_open_lock("2026-09-14", "union_hot_n4_h1", buys) == buys
    assert st.apply_open_lock("2026-09-11", st.LIVE_WEBULL_COMBO, buys) == buys


def test_assert_open_lock_requires_webull_sit_names() -> None:
    payload = {
        "clock_legal_for": "2026-09-14",
        "look": {"source": "look", "stale": False, "panel_bake_date": "2026-09-11"},
        "strategies": {
            "combo_sh_macd_5050_shared": {
                "family": "factor_mine", "date": "2026-09-14",
                "clock_legal_for": "2026-09-14",
                "buy": [
                    {"ticker": "CMRC", "kid_side": "long"},
                    {"ticker": "BKV", "kid_side": "short"},
                ],
            },
        },
        "errors": [],
    }
    try:
        st.assert_session_look(payload, "2026-09-14")
    except AssertionError as e:
        assert "INDP" in str(e) or "missing_longs" in str(e)
    else:
        raise AssertionError("expected lock fail without INDP")
    locked = st.SESSION_OPEN_LOCK["2026-09-14"]
    payload["strategies"]["combo_sh_macd_5050_shared"]["buy"] = (
        [{"ticker": t, "kid_side": "long"} for t in locked["longs"]]
        + [{"ticker": t, "kid_side": "short"} for t in locked["shorts"]]
    )
    st.assert_session_look(payload, "2026-09-14")


def test_stamp_fills_buy_1d_from_stock_book() -> None:
    """Factor-mine today_strategies.json must carry Elite 1d rows at top level."""
    payload = {
        "strategies": {
            "stock_book_1d": {
                "buy": [{"ticker": "AVAH", "side": "long"}],
                "sell": [{"ticker": "METC", "side": "short"}],
            }
        }
    }
    book = {
        "src": "elite_live", "after_open": True, "at": "t", "n": 2,
        "error": None, "clock_rule": "x",
    }
    with mock.patch("src.elite_live_px.quote_book", return_value=book), \
            mock.patch("src.elite_live_px.official_opens", return_value={}), \
            mock.patch(
                "src.elite_live_px.stamp_rows",
                side_effect=lambda rows, _book, opens=None: list(rows),
            ):
        out = st.stamp_live_quotes(payload, "2026-09-15")
    assert out["quote"]["src"] == "elite_live"
    assert [x["ticker"] for x in out["buy_1d"]] == ["AVAH"]
    assert [x["ticker"] for x in out["sell_1d"]] == ["METC"]


def main() -> None:
    test_combo_would_buy_unions_member_lists()
    test_combo_skip_drops_long_and_short_clash()
    test_recipe_strats_never_emits_combo_needs_mine()
    test_source_uses_session_look_not_last_bake()
    test_session_look_uses_panel_when_session_present()
    test_session_look_uses_open_rows_when_panel_stops_early()
    test_session_look_refuses_last_bake()
    test_assert_session_look_blocks_stale_ship()
    test_assert_fails_when_bake_is_not_session_open()
    test_open_lock_pins_indp_and_drops_friday()
    test_assert_open_lock_requires_webull_sit_names()
    test_stamp_fills_buy_1d_from_stock_book()
    print("ok")


if __name__ == "__main__":
    main()
