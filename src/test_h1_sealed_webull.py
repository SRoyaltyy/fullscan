"""Sealed h1 new-buy set is the Webull paper send list.

The 2026-10-02 plan lists SDEV/QSI/GLND/TJGC. The open fill bought only
QSI and TJGC, because SDEV and GLND were already held. A Factor Mine
HOT4 rebuild cannot change that list. A carry name sized as a buy fails
closed. The sealed log is not rewritten.

Run: python -m src.test_h1_sealed_webull
"""
from __future__ import annotations

import json
import tempfile
from pathlib import Path
from unittest import mock

from src.futubull_exec import BrokerSnap
from src.h1_sealed_exec import (
    H1_LOG, SealedH1Error, assert_buys_match_new_set, sealed_h1_orders,
)
from src.webull_exec import HOT4, plan_hot4_for_broker

ROOT = Path(__file__).resolve().parent.parent
LEDGER = ROOT / "research" / "hot_n4_clean_v4" / "forward_h1" / "LEDGER.jsonl"
DAY = "2026-10-02"
PLAN_PICKS = ("SDEV", "QSI", "GLND", "TJGC")
BUY_SHARES = (("QSI", 2624), ("TJGC", 104))
PREOPEN_SHARES = (("QSI", 2438), ("TJGC", 107))
SELL_SHARES = (("EGG", 774), ("KOD", 33))
CARRY = ("SDEV", "GLND")


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
    assert [row["ticker"] for row in plan["picks"]] == list(PLAN_PICKS)
    assert [(row["ticker"], row["shares"]) for row in plan["planned_sells"]] == list(
        SELL_SHARES)
    orders = _orders()
    assert [(row["ticker"], row["shares"]) for row in orders["buys"]] == list(BUY_SHARES)
    assert [(row["ticker"], row["shares"]) for row in orders["sells"]] == list(SELL_SHARES)
    assert orders["carry"] == list(CARRY)
    assert orders["new_buy_source"] == "open_fill"
    assert orders["plan_sha256"] == plan["sha256"]
    bought = {row["ticker"] for row in orders["buys"]}
    assert "SDEV" not in bought and "GLND" not in bought


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
    assert "SDEV" not in names and "GLND" not in names
    held = [row for row in first["skipped"] if row["kind"] == "held"]
    assert [row["ticker"] for row in held] == list(CARRY)


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
        "QSI", "TJGC"]
    assert "SDEV" not in card["look_error"]
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
        "ticker": "QSI", "side": "BUY", "shares": 2624, "px": 1.38,
        "date": DAY, "sealed_shares": True,
    }])
    assert placed == []
    row = got[we.client_order_id(DAY, "BUY", "QSI")]
    assert row["ok"] is False
    assert "not rebuilding HOT4" in row["error"]
    assert row["shares"] == 2624


def test_carry_name_sized_as_buy_fails_closed() -> None:
    """A buy for a name the sealed book already holds is refused."""
    orders = _orders()
    bad_buys = list(orders["buys"]) + [{
        "ticker": "SDEV", "shares": 479, "px": 3.66, "rank": 1, "sources": [],
    }]
    try:
        assert_buys_match_new_set(
            bad_buys, carry=orders["carry"], expected=["QSI", "TJGC"],
        )
    except SealedH1Error as exc:
        text = str(exc)
        assert "SDEV" in text
        assert "carry" in text
        assert "not rebuilding HOT4" in text
    else:
        raise AssertionError("carry buy was accepted")

    def poisoned(date, log_path=None):
        bad = dict(orders)
        bad["buys"] = bad_buys
        return bad

    snap = BrokerSnap(env="paper", cash=20_000, positions={}, connected=True)
    with mock.patch("src.h1_sealed_exec.sealed_h1_orders", side_effect=poisoned):
        card = plan_hot4_for_broker(DAY, snap, payload={"strategies": {}})
    assert card["tickets"] == []
    assert card["policy"] == HOT4
    assert "SDEV" in card["look_error"]
    assert "carry" in card["look_error"]
    assert "not rebuilding HOT4" in card["look_error"]


def test_preopen_sizes_only_new_buys_before_the_fill() -> None:
    """Before the open fill is sealed, carry names are still not sized."""
    kept = []
    for line in H1_LOG.read_text(encoding="utf-8").splitlines(True):
        obj = json.loads(line)
        if obj.get("date") == DAY and obj.get("kind") in {
            "open_fill", "open_fill_correction", "mark", "close",
        }:
            continue
        kept.append(line)
    before_log = H1_LOG.read_bytes()
    before_led = LEDGER.read_bytes()
    with tempfile.TemporaryDirectory() as folder:
        path = Path(folder) / "h1_log.jsonl"
        path.write_text("".join(kept), encoding="utf-8")
        orders = sealed_h1_orders(DAY, log_path=path)
    assert H1_LOG.read_bytes() == before_log
    assert LEDGER.read_bytes() == before_led
    assert orders["new_buy_source"] == "preopen"
    assert orders["carry"] == list(CARRY)
    assert [(row["ticker"], row["shares"]) for row in orders["buys"]] == list(
        PREOPEN_SHARES)
    bought = {row["ticker"] for row in orders["buys"]}
    assert "SDEV" not in bought and "GLND" not in bought


def test_each_sealed_day_matches_the_open_fill_new_buys() -> None:
    """Buy tickets are the effective open fill, never a carried pick."""
    # 2026-09-28's correction is the fill readers use. USDE stayed held.
    sep = sealed_h1_orders("2026-09-28")
    assert [(row["ticker"], row["shares"]) for row in sep["buys"]] == [
        ("SRFM", 2941), ("SHMD", 713), ("FEAM", 1211),
    ]
    assert "USDE" in sep["carry"]
    for day in ("2026-09-29", "2026-09-30", "2026-10-01", DAY):
        orders = sealed_h1_orders(day)
        assert orders["new_buy_source"] == "open_fill"
        bought = {row["ticker"] for row in orders["buys"]}
        assert not (bought & set(orders["carry"]))
    friday = _orders()
    assert [(row["ticker"], row["shares"]) for row in friday["buys"]] == list(
        BUY_SHARES)


def main() -> None:
    test_sealed_2026_10_02_names_and_share_counts()
    test_hot4_rebuild_does_not_change_the_ticket_set()
    test_short_paper_cash_fails_closed()
    test_sealed_buy_is_not_silently_resized()
    test_carry_name_sized_as_buy_fails_closed()
    test_preopen_sizes_only_new_buys_before_the_fill()
    test_each_sealed_day_matches_the_open_fill_new_buys()
    print("test_h1_sealed_webull: 7 ok")


if __name__ == "__main__":
    main()
