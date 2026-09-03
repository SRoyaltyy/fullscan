"""09:30 BUY/SELL/HOLD — no same-day Change% leak."""
from __future__ import annotations

from src import lookback_action as act
from src import ticker_lookback as tl
from src import ticker_lookback_run as run


def _day(**kw):
    boxes = kw.pop("boxes", {"join": "good", "ab": "good", "vol": "good",
                             "peer": "neutral", "gen": "neutral"})
    base = {
        "date": "2026-08-20",
        "ticker": "TEST",
        "class": "asof_0930",
        "boxes": boxes,
        "lattice_live": False,
        "lane": None,
        "setups": kw.pop("setups", None),
        "signal_alarm": False,
        "signal_improved": False,
        "region": {"tone": "good"},
        "stretch": {"tone": "missing"},
    }
    base.update(kw)
    return base


def test_fade_is_sell() -> None:
    day = _day(setups=[{
        "id": "tag_context:first_crack", "verdict": "fade",
        "short": "first crack", "edge_1d": -1.22, "label": "first crack",
    }])
    packed = act.action_call(day)
    assert packed["action"] == "SELL"
    assert "fade" in packed["reason"]


def test_blocked_is_no_buy() -> None:
    packed = act.action_call(_day(lattice_live=True, lane="blocked"))
    assert packed["action"] == "NO BUY"


def test_probable_is_buy() -> None:
    packed = act.action_call(_day(lattice_live=True, lane="probable", setups=[]))
    assert packed["action"] == "BUY"
    strict = act.action_call(
        _day(lattice_live=True, lane="probable", setups=[]),
        params=act.preset_params("strict"),
    )
    assert strict["action"] != "BUY"


def test_featured_long_setup_buys_pre_lattice() -> None:
    day = _day(setups=[{
        "id": "pair:vol=good|ab=good", "verdict": "long",
        "short": "vol+AB", "edge_1d": 2.76, "label": "vol+AB",
    }])
    packed = act.action_call(day)
    assert packed["action"] == "BUY"
    assert "vol+AB" in packed["reason"]


def test_weak_setup_is_hold() -> None:
    day = _day(setups=[{
        "id": "factor:judge=neutral", "verdict": "long",
        "short": "jdg🟡", "edge_1d": 1.35, "label": "judge yellow",
    }])
    packed = act.action_call(day)
    assert packed["action"] == "HOLD"


def test_call_ignores_same_day_change() -> None:
    day = _day(setups=[{
        "id": "pair:vol=good|ab=good", "verdict": "long",
        "short": "vol+AB", "edge_1d": 2.76, "label": "vol+AB",
    }])
    a = act.action_call(day)
    day["change_pct"] = 18.0
    day["price_changes"] = {"1d": 9.0, "3d": 12.0, "1w": 20.0}
    day["on_1d_buy"] = True
    b = act.action_call(day)
    assert a["action"] == b["action"]
    assert a["reason"] == b["reason"]


def test_grade_buy_sell() -> None:
    assert act.grade_call("BUY", {"1d": 1.2, "3d": -0.4, "1w": 2.0}) == {
        "1d": True, "3d": False, "1w": True,
    }
    assert act.grade_call("SELL", {"1d": -1.2, "3d": 0.4})["1d"] is True
    assert act.grade_call("HOLD", {"1d": 5.0})["1d"] is None


def test_lookback_sheet_has_action_column() -> None:
    payload = run.scan_tickers(
        ["CRM"], from_date="2026-09-01", to_date="2026-09-01")
    from src import ticker_lookback_setups as setups
    setups.attach_setups(payload)
    act.attach_actions(payload)
    day = payload["names"][0]["days"][0]
    assert day["action_call"] in act.ACTIONS
    assert day["lane"] == "probable"
    assert day["action_call"] == "BUY"
    md = run.render_md(payload)
    page = run.render_html(payload)
    assert "| Action |" in md
    assert "<th>Action</th>" in page
    assert "BUY" in md
    assert day["action_stamp"] == "2026-09-01 09:30 ET"
    assert day["action_label"] == "BUY · 2026-09-01 09:30 ET"
    assert "BUY · 2026-09-01 09:30 ET" in md
    assert "BUY · 2026-09-01 09:30 ET" in page
    assert "| o→c |" in md
    assert "<th>o→c</th>" in page
    assert "| Open |" in md
    assert "<th>Open</th>" in page
    assert act.cond_tally(day) != "—"


def test_action_clocks_are_open_not_close() -> None:
    assert act.format_action("SELL", "2026-08-17") == "SELL · 2026-08-17 09:30 ET"
    assert act.format_price(3.82, "2026-08-17") == "$3.82 · 2026-08-17 16:00 ET"
    assert act.format_price(3.72, "2026-08-17", act.OPEN_CLOCK) == (
        "$3.72 · 2026-08-17 09:30 ET")
    assert act.format_open_close(2.69, "2026-08-17") == (
        f"{tl.BOX_ICON['good']} +2.69% · 2026-08-17 09:30→16:00 ET")
    assert act.format_ret(-12.69, "2026-08-18") == (
        f"{tl.BOX_ICON['bad']} -12.69% · 2026-08-18 16:00 ET")
    assert act.format_ret(0.1, "2026-08-18") == (
        f"{tl.BOX_ICON['neutral']} +0.10% · 2026-08-18 16:00 ET")
    assert act.cond_tally({
        "condition": {"tone": "good", "n": 4, "good": 3, "neutral": 0, "bad": 1},
    }) == "3/0/1"
    bar = tl.session_bar("AAPL", "2026-08-17")
    assert bar["open"] is not None
    assert bar["close"] is not None
    assert bar["close_open_pct"] == round(
        100.0 * (bar["close"] / bar["open"] - 1.0), 3)
    later = tl.session_bar("AAPL", "2026-08-24")
    assert later["open"] is not None
    assert later["close"] is not None
    assert later["close_open_pct"] is not None


if __name__ == "__main__":
    test_fade_is_sell()
    test_blocked_is_no_buy()
    test_probable_is_buy()
    test_featured_long_setup_buys_pre_lattice()
    test_weak_setup_is_hold()
    test_call_ignores_same_day_change()
    test_grade_buy_sell()
    test_lookback_sheet_has_action_column()
    test_action_clocks_are_open_not_close()
    print("9 lookback-action tests passed")
