"""Sealed h1 plan is the Webull paper send list.

Loading 2026-10-02 yields SDEV/QSI/GLND/TJGC buys and EGG/KOD sells.
A Factor Mine HOT4 rebuild cannot change that list. The sealed log is
not rewritten.

Run: python -m src.test_h1_sealed_webull
"""
from __future__ import annotations

import json
from pathlib import Path
from unittest import mock

from src.futubull_exec import BrokerSnap
from src.h1_sealed_exec import H1_LOG, sealed_h1_orders
from src.webull_exec import HOT4, plan_hot4_for_broker

ROOT = Path(__file__).resolve().parent.parent
LEDGER = ROOT / "research" / "hot_n4_clean_v4" / "forward_h1" / "LEDGER.jsonl"
DAY = "2026-10-02"
BUY_SHARES = (("SDEV", 479), ("QSI", 1219), ("GLND", 398), ("TJGC", 53))
SELL_SHARES = (("EGG", 774), ("KOD", 33))


def _plan_line() -> dict:
    for line in H1_LOG.read_text(encoding="utf-8").splitlines():
        obj = json.loads(line)
        if obj.get("kind") == "plan" and obj.get("date") == DAY:
            return obj
    raise AssertionError(f"no plan line for {DAY}")


def _orders():
    return sealed_h1_orders(DAY)


def test_sealed_2026_10_02_names_and_share_counts() -> None:
    plan = _plan_line()
    assert [row["ticker"] for row in plan["picks"]] == [
        "SDEV", "QSI", "GLND", "TJGC"]
    assert [(row["ticker"], row["shares"]) for row in plan["planned_sells"]] == list(
        SELL_SHARES)
    orders = _orders()
    assert [(row["ticker"], row["shares"]) for row in orders["buys"]] == list(BUY_SHARES)
    assert [(row["ticker"], row["shares"]) for row in orders["sells"]] == list(SELL_SHARES)
    assert orders["plan_sha256"] == plan["sha256"]


def test_hot4_rebuild_does_not_change_the_ticket_set() -> None:
    """pick_day and a divergent published panel cannot change the send list."""
    before_log = H1_LOG.read_bytes()
    before_led = LEDGER.read_bytes()
    snap = BrokerSnap(env="paper", cash=20_000, positions={}, connected=True)
    payload = {
        "date": DAY,
        "strategies": {
            HOT4: {
                "date": DAY,
                "status": "ok",
                "s": -9,
                "buy": [{"ticker": "DELL", "side": "long", "px": 10}],
                "sell": [{"ticker": "FEAM", "side": "long"}],
            }
        },
    }

    def boom(*_a, **_k):
        raise AssertionError("pick_day")

    with mock.patch("src.factor_mine.pick_day", side_effect=boom), \
            mock.patch(
                "src.strategy_tickets.hot4_recipe_tickers",
                side_effect=boom,
            ), \
            mock.patch(
                "src.strategy_tickets.hot4_recipe_sells",
                side_effect=boom,
            ), \
            mock.patch(
                "src.combo_broker.resolve_rows",
                side_effect=boom,
            ):
        first = plan_hot4_for_broker(DAY, snap, payload=payload, panel={"by_date": {}})
        payload["strategies"][HOT4]["buy"] = [
            {"ticker": "ZZZZ", "side": "long", "px": 1}]
        second = plan_hot4_for_broker(DAY, snap, payload=payload)
    assert H1_LOG.read_bytes() == before_log
    assert LEDGER.read_bytes() == before_led
    assert first["look_error"] == ""
    assert first["source"] == "sealed_h1"
    assert first["policy"] == HOT4
    buys = [(t["ticker"], t["shares"]) for t in first["tickets"] if t["side"] == "BUY"]
    sells = [(t["ticker"], t["shares"]) for t in first["tickets"] if t["side"] == "SELL"]
    assert buys == list(BUY_SHARES)
    assert sells == list(SELL_SHARES)
    assert first["tickets"][0]["side"] == "SELL"
    assert all(t.get("sealed_shares") for t in first["tickets"])
    assert first["tickets"] == second["tickets"]
    names = {t["ticker"] for t in first["tickets"]}
    assert "DELL" not in names and "ZZZZ" not in names and "FEAM" not in names


def test_short_paper_cash_fails_closed() -> None:
    before = H1_LOG.read_bytes()
    snap = BrokerSnap(env="paper", cash=1000, positions={"EGG": {"shares": 1}},
                      connected=True)

    def boom(*_a, **_k):
        raise AssertionError("pick_day")

    with mock.patch("src.factor_mine.pick_day", side_effect=boom):
        card = plan_hot4_for_broker(DAY, snap, payload={
            "strategies": {HOT4: {"buy": [{"ticker": "DELL"}], "sell": []}},
        })
    assert H1_LOG.read_bytes() == before
    assert card["tickets"] == []
    assert "not rebuilding HOT4" in card["look_error"]
    assert [row["ticker"] for row in card["would_buy"]["rows"]] == [
        "SDEV", "QSI", "GLND", "TJGC"]
    assert [(row["ticker"], row["shares"]) for row in card["would_sell"]["rows"]] == list(
        SELL_SHARES)
    # Paper lot size is not substituted for the sealed sell.
    assert card["would_sell"]["rows"][0]["shares"] == 774


def test_sealed_buy_is_not_silently_resized() -> None:
    from types import SimpleNamespace
    from src import webull_exec as we

    placed = []

    def place_order(account_id, bodies):
        placed.append(bodies[0]["symbol"])
        return {"code": "SUCCESS", "data": [{"order_id": "oid", "client_order_id": "x"}]}

    api = we.PaperAPI()
    api.account_id = "paper-test"
    api.trade = SimpleNamespace(
        account_v2=SimpleNamespace(
            get_account_list=lambda: {"data": [{"account_id": "paper-test"}]},
            get_account_balance=lambda account_id: {
                "available_cash": "10.00",
                "buying_power": "10.00",
            },
            get_account_position=lambda account_id: {"positions": []},
        ),
        order_v3=SimpleNamespace(place_order=place_order),
    )
    got = api.place_batch([{
        "ticker": "SDEV", "side": "BUY", "shares": 479, "px": 3.66,
        "date": DAY, "sealed_shares": True,
    }])
    assert placed == []
    row = got[we.client_order_id(DAY, "BUY", "SDEV")]
    assert row["ok"] is False
    assert "not rebuilding HOT4" in row["error"]
    assert row["shares"] == 479


def main() -> None:
    test_sealed_2026_10_02_names_and_share_counts()
    test_hot4_rebuild_does_not_change_the_ticket_set()
    test_short_paper_cash_fails_closed()
    test_sealed_buy_is_not_silently_resized()
    print("test_h1_sealed_webull: 4 ok")


if __name__ == "__main__":
    main()
