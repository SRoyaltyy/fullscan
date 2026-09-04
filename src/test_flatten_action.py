"""Live flatten ACTION: holdings → tickets, no lookahead.

Run: python3 -m src.test_flatten_action
"""
from __future__ import annotations

from pathlib import Path

from src import flatten_action as fa
from src import sleeve_merge as sm

ROOT = Path(__file__).resolve().parent.parent
POL = {
    **sm.DEFAULT,
    "name": "flatten_switch_recycle",
    "engine": "flatten_switch",
    "io_sleeve": "2w_size",
    "long_top_n": 10,
    "long_pct": 0.10,
    "day_cap": 1.00,
    "sizeup": 1.0,
    "allow_short": False,
    "min_buys": 5,
    "rotate_mover": True,
    "carry_last_book": True,
}


def _load():
    payload = sm.load_payload()
    books = sm.list_books()
    return payload, books


def test_policy_is_recycle() -> None:
    pol = fa.winning_policy()
    assert pol["name"] == "flatten_switch_recycle"
    assert pol["rotate_mover"] is True
    assert pol["carry_last_book"] is True
    assert pol["min_buys"] >= 5
    assert pol["book_for_flatten"] == "yesterday"


def test_score_from_today_predict_md() -> None:
    s = fa.score_from_predict_md("2026-09-04")
    assert s is not None, "09-04 predict.md should parse S"
    assert s >= 2.0


def test_holdings_asof_0903_still_in_io() -> None:
    payload, books = _load()
    if not payload or not books:
        print("skip holdings (no payload/books)")
        return
    held = fa.holdings_asof(payload, books, POL, 100_000, "2026-09-03", None)
    io = held["io_pos"]
    assert io, held
    assert "SOFI" in io or "HOOD" in io, sorted(io)
    assert held["cash"] < 200
    assert not held["mv_pos"]


def test_0820_open_flattens_io() -> None:
    payload, books = _load()
    if not payload or not books:
        print("skip 08-20 (no payload/books)")
        return
    cal = sm.session_calendar(payload, books)
    prev = fa.prev_session(cal, "2026-08-20")
    held = fa.holdings_asof(payload, books, POL, 100_000, prev, None)
    up = fa.gather_upstream("2026-08-20", payload, books, POL, "open")
    card = fa.propose_open("2026-08-20", held, up, POL)
    assert card["route"] == "mover", card["flags"]
    sells = [t for t in card["tickets"] if t["side"] == "SELL"]
    buys = [t for t in card["tickets"] if t["side"] == "BUY"]
    assert sells, card["tickets"]
    assert all(t["sleeve"] == "io_core" for t in sells)
    assert buys, "expected mover BUYs"
    assert all(t["sleeve"] == "mover_long" for t in buys)
    # today's book must not be required for the flatten decision
    assert card["flags"]["flatten_ok"] is True
    assert up["prior_book"] is not None


def test_open_never_needs_today_book() -> None:
    payload, books = _load()
    if not payload or not books:
        print("skip today-book (no payload/books)")
        return
    up = fa.gather_upstream("2026-08-21", payload, books, POL, "open")
    held = {"cash": 100.0, "io_pos": {}, "mv_pos": []}
    a = fa.route_flags(up, held, POL)
    up2 = dict(up)
    up2["today_book"] = {"books": {"2w": {"buy": [{"ticker": "FAKE"}]}}}
    b = fa.route_flags(up2, held, POL)
    assert a["flatten_ok"] == b["flatten_ok"]
    assert a["route_mover"] == b["route_mover"]


def test_0814_green_without_buys_holds_io() -> None:
    payload, books = _load()
    if not payload or not books:
        print("skip 08-14 (no payload/books)")
        return
    cal = sm.session_calendar(payload, books)
    prev = fa.prev_session(cal, "2026-08-14")
    held = fa.holdings_asof(payload, books, POL, 100_000, prev, None)
    up = fa.gather_upstream("2026-08-14", payload, books, POL, "open")
    card = fa.propose_open("2026-08-14", held, up, POL)
    assert card["route"] == "io", card["flags"]
    assert card["flags"]["flatten_ok"] is False
    assert any(t["side"] == "HOLD" for t in card["tickets"])


def test_0904_green_but_mover_action_missing() -> None:
    """Today's S is green; mover stamps are not in — HOLD, do not flatten."""
    payload, books = _load()
    if not payload or not books:
        print("skip 09-04 (no payload/books)")
        return
    cal = sm.session_calendar(payload, books)
    prev = fa.prev_session(cal, "2026-09-04") or "2026-09-03"
    held = fa.holdings_asof(payload, books, POL, 100_000, prev, None)
    up = fa.gather_upstream("2026-09-04", payload, books, POL, "open")
    assert up["score"] is not None and up["score"] >= 1.0
    assert "mover_lookback_action" in up["missing"]
    card = fa.propose_open("2026-09-04", held, up, POL)
    assert card["route"] == "io", card["flags"]
    assert card["flags"]["flatten_ok"] is False
    assert any(t["side"] == "HOLD" for t in card["tickets"])


def test_keep_open_does_not_change_headline() -> None:
    payload, books = _load()
    if not payload or not books:
        print("skip keep_open (no payload/books)")
        return
    closed = sm.run_flatten_switch(payload, books, POL, 100_000)
    st = sm.stats(closed, sm.io_top_return())
    assert st["total_ret_pct"] >= 21.0, st


def main() -> None:
    test_policy_is_recycle()
    test_score_from_today_predict_md()
    test_open_never_needs_today_book()
    test_keep_open_does_not_change_headline()
    test_holdings_asof_0903_still_in_io()
    test_0820_open_flattens_io()
    test_0814_green_without_buys_holds_io()
    test_0904_green_but_mover_action_missing()
    print("test_flatten_action: 8 ok")


if __name__ == "__main__":
    main()
