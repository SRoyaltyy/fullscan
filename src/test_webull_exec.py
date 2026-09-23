"""Webull paper sender: sandbox host, live tickets only, no silent REAL.

Run: python -m src.test_webull_exec
"""
from __future__ import annotations

from pathlib import Path

from src.futubull_exec import BrokerSnap, send_card
from src.webull_exec import (
    HOT4,
    client_order_id,
    load_hot4_published,
    order_body,
    paper_host,
    parse_account_id,
    parse_balance,
    parse_order_id,
    parse_positions,
    HOT4_CASH_HAIRCUT,
    cash_still_free,
    clamp_buy_shares,
    plan_hot4_for_broker,
    refuse_real,
    size_hot4_sells,
    size_hot4_tickets,
)


def test_refuse_real_without_flags() -> None:
    assert refuse_real("paper", True, True) is None
    assert refuse_real("real", False, True) is None
    assert "pass --live" in (refuse_real("real", True, False) or "")
    assert "WEBULL_LIVE" in (refuse_real("real", True, True) or "")


def test_paper_never_uses_live_host() -> None:
    assert paper_host("paper") == "api.sandbox.webull.com"
    assert paper_host("simulate") == "api.sandbox.webull.com"
    assert paper_host("real") == "api.webull.com"


def test_client_order_id_stable_and_short() -> None:
    a = client_order_id("2026-09-11", "BUY", "HOOD")
    b = client_order_id("2026-09-11", "BUY", "HOOD")
    c = client_order_id("2026-09-11", "SELL", "HOOD")
    assert a == b == "fs20260911BHOOD"
    assert c == "fs20260911SHOOD"
    assert len(a) <= 32
    assert client_order_id("2026-09-11", "BUY", "BRK-B") == "fs20260911BBRKB"


def test_parse_account_and_book() -> None:
    accounts = {"data": [
        {"account_id": "paper-1", "account_type": "PAPER"},
        {"account_id": "paper-2", "account_type": "PAPER"},
    ]}
    assert parse_account_id(accounts) == "paper-1"
    assert parse_account_id(accounts, "paper-2") == "paper-2"
    cash, power = parse_balance({
        "available_cash": "98765.4",
        "buying_power": "120000",
    })
    assert abs(cash - 98765.4) < 1e-6
    assert abs(power - 120000) < 1e-6
    camel_cash, camel_power = parse_balance({
        "data": {"availableCash": "5000", "buyingPower": "8000"},
    })
    assert abs(camel_cash - 5000) < 1e-6
    assert abs(camel_power - 8000) < 1e-6
    pos = parse_positions({"positions": [
        {"symbol": "SOFI", "quantity": "10", "cost_price": "19",
         "last_price": "18", "market_value": "180"},
        {"symbol": "SKIP", "quantity": "0"},
    ]})
    assert pos["SOFI"]["shares"] == 10
    assert "SKIP" not in pos
    assert parse_order_id({"data": [{"order_id": "abc"}]}) == "abc"


def test_dry_run_does_not_place() -> None:
    class Boom:
        def place(self, *a, **k):
            raise AssertionError("dry-run must not place")

    snap = BrokerSnap(env="paper", cash=10, positions={})
    card = {"date": "2026-09-11", "tickets": [
        {"side": "BUY", "ticker": "BVS", "shares": 1, "px": 14.0,
         "clock": "16:00 ET", "sleeve": "io_core", "status": "plan",
         "date": "2026-09-11"},
    ], "would_buy": {"rows": [{"ticker": "HOOD", "shares": 99}]}}
    last = send_card(card, snap, submit=False, opend=Boom(), env="paper")
    assert last["sent"][0]["status"] == "dry_run"
    assert last["n_would"] == 1
    assert last["n_tickets"] == 1


def test_submit_uses_paper_place() -> None:
    seen = []

    class Fake:
        def place(self, ticket, env):
            seen.append((ticket["ticker"], env))
            return {"ok": True, "order_id": "oid-1"}

    snap = BrokerSnap(env="paper", cash=1000, positions={})
    card = {"date": "2026-09-11", "tickets": [
        {"side": "BUY", "ticker": "BVS", "shares": 2, "px": 14.0,
         "status": "plan"},
    ], "would_buy": {"rows": []}}
    last = send_card(card, snap, submit=True, opend=Fake(), env="paper")
    assert seen == [("BVS", "paper")]
    assert last["sent"][0]["status"] == "submitted"
    assert last["sent"][0]["order_id"] == "oid-1"


def test_env_strips_quoted_secrets() -> None:
    import os
    from src.webull_exec import _env
    os.environ["WEBULL_APP_KEY"] = '  "abc123"  '
    try:
        assert _env("WEBULL_APP_KEY") == "abc123"
    finally:
        os.environ.pop("WEBULL_APP_KEY", None)


def test_not_connected_writes_last_without_replay(tmp_path=None) -> None:
    from unittest import mock
    from src import webull_exec as we

    class Dead:
        env = "paper"
        host = "api.sandbox.webull.com"
        err = "sandbox 401"

        def connect(self) -> bool:
            return False

    with mock.patch.object(we, "PaperAPI", return_value=Dead()), \
            mock.patch.object(we, "_plan", return_value={
                "tickets": [], "stale": False, "policy": HOT4,
            }), \
            mock.patch.object(we, "write_last") as wl, \
            mock.patch.object(we, "inject_today_from_disk"):
        rc = we.run("2026-09-17", submit=True, write=True)
    assert rc == 2
    last = wl.call_args[0][0]
    assert last["connected"] is False
    assert last["n_tickets"] == 0
    assert last["source"] == "hot4"
    assert last["combo"] == ""
    assert last["policy"] == HOT4
    assert "401" in (last.get("error") or "")


def test_hot4_submit_refuses_divergent_wire() -> None:
    from unittest import mock
    from src import webull_exec as we

    class Alive:
        env = "paper"
        host = "api.sandbox.webull.com"
        err = None

        def connect(self) -> bool:
            return True

        def snapshot(self):
            return BrokerSnap(env="paper", cash=10_000, positions={},
                              connected=True, acc_id="paper-1")

        def place(self, *a, **k):
            raise AssertionError("divergent HOT4 must not place")

        def place_batch(self, *a, **k):
            raise AssertionError("divergent HOT4 must not place")

    card = {
        "date": "2026-09-21", "stale": False, "policy": HOT4,
        "tickets": [{
            "side": "BUY", "ticker": "DELL", "shares": 1, "px": 10.0,
            "status": "plan", "date": "2026-09-21",
        }],
        "would_buy": {"rows": [
            {"ticker": t, "side": "long"}
            for t in ("DELL", "GME", "UMC", "VSTS")
        ]},
        "hard_red": False,
    }
    with mock.patch.object(we, "PaperAPI", return_value=Alive()), \
            mock.patch.object(we, "_plan", return_value=card), \
            mock.patch(
                "src.strategy_tickets.assert_hot4_wire",
                side_effect=ValueError(
                    "HOT4 buys ['DELL', 'GME', 'UMC', 'VSTS'] diverge from "
                    "Factor Mine recipe ['FEAM', 'TJGC', 'LVWR', 'SECZ'] "
                    "for 2026-09-21"),
            ), \
            mock.patch.object(we, "write_last") as wl, \
            mock.patch.object(we, "inject_today_from_disk"):
        rc = we.run("2026-09-21", submit=True, write=True, source="hot4")
    assert rc == 2
    last = wl.call_args[0][0]
    assert last["submit"] is False
    assert last["sent"] == []
    assert "diverge" in (last.get("error") or "")


def test_stale_combo_does_not_submit() -> None:
    from unittest import mock
    from src import webull_exec as we

    class Alive:
        env = "paper"
        host = "api.sandbox.webull.com"
        err = None

        def connect(self) -> bool:
            return True

        def snapshot(self):
            return BrokerSnap(env="paper", cash=10_000, positions={},
                              connected=True, acc_id="paper-1")

        def place(self, *a, **k):
            raise AssertionError("stale look must not place")

    card = {
        "date": "2026-09-11", "stale": True, "policy": "combo_sh_macd_5050_shared",
        "combo": "combo_sh_macd_5050_shared", "tickets": [
            {"side": "BUY", "ticker": "HOT1", "shares": 10, "px": 10.0,
             "status": "plan", "date": "2026-09-11"},
        ], "would_buy": {"rows": []},
    }
    with mock.patch.object(we, "PaperAPI", return_value=Alive()), \
            mock.patch.object(we, "_plan", return_value=card), \
            mock.patch.object(we, "write_last") as wl, \
            mock.patch.object(we, "inject_today_from_disk"):
        rc = we.run("2026-09-14", submit=True, write=True, source="combo")
    assert rc == 2
    last = wl.call_args[0][0]
    assert last["submit"] is False
    assert last["sent"][0]["status"] == "dry_run"
    assert last["stale"] is True


def _hot4_buys() -> list[dict]:
    return [
        {"ticker": "INDP", "src": "yday_mover", "side": "long", "px": 3.23},
        {"ticker": "GPRO", "src": "ohlc_hot", "side": "long", "px": 1.26},
        {"ticker": "INSP", "src": "ohlc_hot", "side": "long", "px": 72.75},
        {"ticker": "TJGC", "src": "ohlc_hot", "side": "long", "px": 10.98},
        {"ticker": "SHRT", "src": "news_red", "side": "short", "px": 5.0},
    ]


def test_hot4_tickets_long_only_skip_held_cash_and_sit() -> None:
    buys = _hot4_buys()
    tickets, skips = size_hot4_tickets(
        buys, cash=10_000, held={"GPRO"}, date="2026-09-17", s=7.383,
    )
    by_t = {t["ticker"]: t for t in tickets}
    assert "SHRT" not in by_t
    assert "GPRO" not in by_t
    assert by_t["INDP"]["side"] == "BUY"
    assert by_t["INSP"]["side"] == "BUY"
    assert by_t["TJGC"]["side"] == "BUY"
    assert all(t["order_type"] == "MARKET" for t in tickets)
    assert all(t["sleeve"] == HOT4 for t in tickets)
    assert any(s["ticker"] == "SHRT" and s["kind"] == "short" for s in skips)
    assert any(s["ticker"] == "GPRO" and s["kind"] == "held" for s in skips)
    sit_tickets, sit_skips = size_hot4_tickets(
        buys, cash=10_000, held=set(), date="2026-09-17", s=-3.1,
    )
    assert sit_tickets == []
    assert any(s["kind"] == "hard_red" for s in sit_skips)
    tiny, tiny_skips = size_hot4_tickets(
        [{"ticker": "INSP", "side": "long", "px": 72.75}],
        cash=10, held=set(), date="2026-09-17", s=7.0,
    )
    assert tiny == []
    assert any(s["kind"] == "cash" for s in tiny_skips)

    payload = {
        "date": "2026-09-17",
        "strategies": {
            HOT4: {
                "name": HOT4, "date": "2026-09-17", "side": "long",
                "buy": buys[:4], "sell": [], "sit": False, "s": 7.383,
            }
        },
    }
    pub = load_hot4_published("2026-09-17", payload)
    assert [b["ticker"] for b in pub["buy"]] == ["INDP", "GPRO", "INSP", "TJGC"]
    snap = BrokerSnap(env="paper", cash=10_000, positions={"INDP": {"shares": 1}})
    card = plan_hot4_for_broker("2026-09-17", snap, payload=payload)
    assert card["policy"] == HOT4
    assert card["hard_red"] is False
    assert card["order_type"] == "MARKET"
    names = {t["ticker"] for t in card["tickets"]}
    assert "INDP" not in names
    assert "GPRO" in names and "INSP" in names and "TJGC" in names
    assert all(t["side"] == "BUY" for t in card["tickets"])
    assert len(card["would_buy"]["rows"]) == 4


def test_hot4_sells_size_from_paper_lots() -> None:
    """List-drop exits use the paper lot. Unheld and Clock-B leftovers do not sell."""
    payload = {
        "date": "2026-09-22",
        "strategies": {
            HOT4: {
                "name": HOT4, "date": "2026-09-22", "side": "long",
                "status": "ok", "s": -0.5, "sit": False,
                "buy": [
                    {"ticker": t, "side": "long", "px": 10}
                    for t in ("SECZ", "GRAL", "NUAI", "INDP")
                ],
                "sell": [
                    {"ticker": t, "side": "long", "src": "list-drop"}
                    for t in ("FEAM", "TJGC", "LVWR")
                ],
            }
        },
    }
    snap = BrokerSnap(
        env="paper", cash=5_000, connected=True,
        positions={
            "FEAM": {"shares": 40, "cost_px": 3.5, "last_px": 3.2},
            "DELL": {"shares": 100, "cost_px": 20, "last_px": 21},
        },
    )
    card = plan_hot4_for_broker("2026-09-22", snap, payload=payload)
    assert [r["ticker"] for r in card["would_sell"]["rows"]] == [
        "FEAM", "TJGC", "LVWR",
    ]
    assert card["tickets"][0]["side"] == "SELL"
    sells = [t for t in card["tickets"] if t["side"] == "SELL"]
    assert [t["ticker"] for t in sells] == ["FEAM"]
    assert sells[0]["shares"] == 40
    assert sells[0]["order_type"] == "MARKET"
    assert "DELL" not in {t["ticker"] for t in card["tickets"]}
    assert any(s["ticker"] == "TJGC" and s["kind"] == "unheld" for s in card["skipped"])
    assert any(s["ticker"] == "LVWR" and s["kind"] == "unheld" for s in card["skipped"])
    buys = [t for t in card["tickets"] if t["side"] == "BUY"]
    assert buys and all(t["side"] == "BUY" for t in buys)
    bare, bare_skips = size_hot4_sells(
        ["FEAM", "NOPE"], positions={"FEAM": {"shares": 2}}, date="2026-09-22",
    )
    assert [(t["ticker"], t["shares"]) for t in bare] == [("FEAM", 2)]
    assert any(s["kind"] == "unheld" and s["ticker"] == "NOPE" for s in bare_skips)
    payload["strategies"][HOT4]["sit"] = True
    payload["strategies"][HOT4]["s"] = -4
    red = plan_hot4_for_broker("2026-09-22", snap, payload=payload)
    assert [t["ticker"] for t in red["tickets"] if t["side"] == "SELL"] == ["FEAM"]
    assert [t for t in red["tickets"] if t["side"] == "BUY"] == []


def test_hot4_zero_cash_is_honest() -> None:
    payload = {
        "date": "2026-09-17",
        "strategies": {
            HOT4: {
                "name": HOT4, "date": "2026-09-17",
                "buy": _hot4_buys()[:4], "sit": False, "s": 7.383,
            }
        },
    }
    snap = BrokerSnap(env="paper", cash=0, positions={}, buying_power=10_000)
    card = plan_hot4_for_broker("2026-09-17", snap, payload=payload)
    assert card["tickets"] == []
    assert len(card["would_buy"]["rows"]) == 4
    assert all(s["kind"] == "cash" for s in card["skipped"])
    assert "cannot buy 1 share" in card["why"]


def test_paper_order_is_market_not_limit() -> None:
    body = order_body({
        "side": "BUY", "ticker": "INDP", "shares": 3, "px": 3.23,
        "date": "2026-09-17",
    })
    assert body["order_type"] == "MARKET"
    assert body["support_trading_session"] == "CORE"
    assert body["time_in_force"] == "DAY"
    assert "limit_price" not in body
    assert body["symbol"] == "INDP"
    assert body["quantity"] == "3"
    assert body["side"] == "BUY"


def test_place_batch_sends_one_order_at_a_time() -> None:
    from types import SimpleNamespace
    from src import webull_exec as we

    seen = []

    def place_order(account_id, bodies):
        seen.append((account_id, bodies))
        assert isinstance(bodies, list) and len(bodies) == 1
        assert bodies[0]["combo_type"] == "NORMAL"
        assert not isinstance(bodies[0]["combo_type"], list)
        body = bodies[0]
        return {
            "code": "SUCCESS",
            "data": [{
                "client_order_id": body["client_order_id"],
                "order_id": "oid-" + body["symbol"],
            }],
        }

    api = we.PaperAPI()
    api.account_id = "paper-test"
    api.trade = SimpleNamespace(order_v3=SimpleNamespace(place_order=place_order))
    tickets = [
        {"ticker": "DELL", "side": "BUY", "shares": 1, "date": "2026-09-21"},
        {"ticker": "GME", "side": "BUY", "shares": 1, "date": "2026-09-21"},
        {"ticker": "UMC", "side": "BUY", "shares": 1, "date": "2026-09-21"},
        {"ticker": "VSTS", "side": "BUY", "shares": 1, "date": "2026-09-21"},
    ]
    got = api.place_batch(tickets)
    assert len(seen) == 4
    for ticket, (aid, bodies) in zip(tickets, seen):
        assert aid == "paper-test"
        assert len(bodies) == 1
        assert bodies[0]["symbol"] == ticket["ticker"]
        coid = client_order_id("2026-09-21", "BUY", ticket["ticker"])
        assert bodies[0]["client_order_id"] == coid
        assert got[coid]["ok"] is True
        assert got[coid]["order_id"] == "oid-" + ticket["ticker"]


def test_place_batch_keeps_later_names_after_one_reject() -> None:
    from types import SimpleNamespace
    from src import webull_exec as we

    def place_order(account_id, bodies):
        body = bodies[0]
        assert len(bodies) == 1
        if body["symbol"] == "GME":
            return {"code": "ERROR", "msg": "reject"}
        return {"code": "0", "data": [{"order_id": "ok-" + body["symbol"]}]}

    api = we.PaperAPI()
    api.account_id = "paper-test"
    api.trade = SimpleNamespace(order_v3=SimpleNamespace(place_order=place_order))
    tickets = [
        {"ticker": "DELL", "side": "BUY", "shares": 1, "date": "2026-09-21"},
        {"ticker": "GME", "side": "BUY", "shares": 1, "date": "2026-09-21"},
        {"ticker": "UMC", "side": "BUY", "shares": 1, "date": "2026-09-21"},
    ]
    got = api.place_batch(tickets)
    assert got[client_order_id("2026-09-21", "BUY", "DELL")]["ok"] is True
    assert got[client_order_id("2026-09-21", "BUY", "GME")]["ok"] is False
    assert got[client_order_id("2026-09-21", "BUY", "UMC")]["ok"] is True
    assert got[client_order_id("2026-09-21", "BUY", "UMC")]["order_id"] == "ok-UMC"


def test_cash_still_free_holds_unfilled_reserve() -> None:
    assert cash_still_free(None, None, 0) is None
    # Pre-open ack: snapshot cash has not moved, so the reserve stays held back.
    assert cash_still_free(1_000_000, 1_000_000, 750_000) == 250_000
    # Snapshot already dropped by the fills — do not subtract the reserve again.
    assert cash_still_free(1_000_000, 247_000, 752_000) == 247_000
    # Partial drop: only the unseen remainder of the reserve is held back.
    assert cash_still_free(1_000_000, 900_000, 200_000) == 800_000
    assert clamp_buy_shares(30, 10, 270) == 27
    assert clamp_buy_shares(30, 10, 10_000) == 30
    assert clamp_buy_shares(30, 10, 9) == 0


def test_hot4_slip_buffer_covers_richer_open_fills() -> None:
    """2026-09-21: equal split of $999,662 left VSTS unfunded after richer fills."""
    cash = 999662.39
    buys = [
        {"ticker": "DELL", "side": "long", "px": 583.42},
        {"ticker": "GME", "side": "long", "px": 22.84},
        {"ticker": "UMC", "side": "long", "px": 24.96},
        {"ticker": "VSTS", "side": "long", "px": 13.82},
    ]
    # The rigid plan that stuck: each name took cash/4 at the plan px.
    rigid_px = [583.42, 22.84, 24.96, 13.82]
    per = cash / 4
    rigid_shares = [int(per // px) for px in rigid_px]
    assert rigid_shares == [428, 10942, 10012, 18083]
    rich = {"DELL": 587.89, "GME": 22.90, "UMC": 24.99}
    rigid_spent = sum(sh * rich[name] for name, sh in zip(
        ("DELL", "GME", "UMC"), rigid_shares))
    assert cash - rigid_spent < rigid_shares[3] * 13.82

    tickets, skips = size_hot4_tickets(
        buys, cash=cash, held=set(), date="2026-09-21", s=12.871,
    )
    assert skips == []
    assert [t["ticker"] for t in tickets] == ["DELL", "GME", "UMC", "VSTS"]
    for ticket, rigid in zip(tickets, rigid_shares):
        assert ticket["shares"] < rigid
    planned = sum(t["notional"] for t in tickets)
    assert planned <= cash * HOT4_CASH_HAIRCUT + 0.05
    assert planned < cash
    spent = sum(t["shares"] * rich[t["ticker"]] for t in tickets[:3])
    assert cash - spent >= tickets[3]["notional"] - 0.05


def _cash_book_api(state: dict, on_place=None):
    from types import SimpleNamespace
    from src import webull_exec as we

    def place_order(account_id, bodies):
        assert isinstance(bodies, list) and len(bodies) == 1
        assert bodies[0]["combo_type"] == "NORMAL"
        assert not isinstance(bodies[0]["combo_type"], list)
        if on_place is not None:
            on_place(bodies[0], state)
        body = bodies[0]
        return {
            "code": "SUCCESS",
            "data": [{
                "client_order_id": body["client_order_id"],
                "order_id": "oid-" + body["symbol"],
            }],
        }

    api = we.PaperAPI()
    api.account_id = "paper-test"
    api.trade = SimpleNamespace(
        account_v2=SimpleNamespace(
            get_account_list=lambda: {
                "data": [{"account_id": "paper-test", "account_type": "PAPER"}],
            },
            get_account_balance=lambda account_id: {
                "available_cash": f"{state['cash']:.4f}",
                "buying_power": f"{state['cash']:.4f}",
            },
            get_account_position=lambda account_id: {"positions": []},
        ),
        order_v3=SimpleNamespace(place_order=place_order),
    )
    return api


def test_place_batch_shrinks_later_leg_when_earlier_fills_eat_cash() -> None:
    state = {"cash": 900.0}
    placed = []

    def on_place(body, book):
        qty = int(body["quantity"])
        placed.append((body["symbol"], qty))
        book["cash"] -= qty * 10.5

    api = _cash_book_api(state, on_place)
    tickets = [
        {"ticker": "AAA", "side": "BUY", "shares": 30, "px": 10.0, "date": "2026-09-21"},
        {"ticker": "BBB", "side": "BUY", "shares": 30, "px": 10.0, "date": "2026-09-21"},
        {"ticker": "CCC", "side": "BUY", "shares": 30, "px": 10.0, "date": "2026-09-21"},
    ]
    got = api.place_batch(tickets)
    assert placed[0] == ("AAA", 30)
    assert placed[1] == ("BBB", 30)
    assert placed[2] == ("CCC", 27)
    coid = client_order_id("2026-09-21", "BUY", "CCC")
    assert got[coid]["ok"] is True
    assert got[coid]["shares"] == 27
    assert got[coid]["resized_from"] == 30
    assert got[coid]["order_id"] == "oid-CCC"


def test_place_batch_skips_leg_that_cannot_buy_one_share() -> None:
    state = {"cash": 100.0}
    placed = []

    def on_place(body, book):
        placed.append(body["symbol"])
        book["cash"] -= int(body["quantity"]) * 90.0

    api = _cash_book_api(state, on_place)
    tickets = [
        {"ticker": "AAA", "side": "BUY", "shares": 1, "px": 80.0, "date": "2026-09-21"},
        {"ticker": "BBB", "side": "BUY", "shares": 1, "px": 80.0, "date": "2026-09-21"},
    ]
    got = api.place_batch(tickets)
    assert placed == ["AAA"]
    b = client_order_id("2026-09-21", "BUY", "BBB")
    assert got[b]["skipped"] is True
    assert got[b]["shares"] == 0
    assert got[b]["ok"] is True
    assert "order_id" not in got[b] or got[b].get("order_id", "") == ""


def test_place_batch_keeps_haircut_plan_when_preopen_cash_is_unchanged() -> None:
    tickets, skips = size_hot4_tickets(
        [
            {"ticker": "AAA", "side": "long", "px": 10},
            {"ticker": "BBB", "side": "long", "px": 10},
            {"ticker": "CCC", "side": "long", "px": 10},
        ],
        cash=1200, held=set(), date="2026-09-21", s=5,
    )
    assert skips == []
    assert sum(t["notional"] for t in tickets) <= 1200 * HOT4_CASH_HAIRCUT + 0.05
    state = {"cash": 1200.0}
    placed = []

    def on_place(body, book):
        placed.append((body["symbol"], int(body["quantity"])))

    api = _cash_book_api(state, on_place)
    got = api.place_batch(tickets)
    assert [(t["ticker"], t["shares"]) for t in tickets] == placed
    for ticket in tickets:
        coid = client_order_id("2026-09-21", "BUY", ticket["ticker"])
        assert got[coid]["ok"] is True
        assert got[coid]["shares"] == ticket["shares"]
        assert "resized_from" not in got[coid]


def test_rejected_leg_does_not_reserve_cash() -> None:
    from types import SimpleNamespace
    from src import webull_exec as we

    state = {"cash": 100.0}
    placed = []

    def place_order(account_id, bodies):
        assert len(bodies) == 1
        body = bodies[0]
        if body["symbol"] == "AAA":
            return {"code": "ERROR", "msg": "reject"}
        placed.append((body["symbol"], int(body["quantity"])))
        return {"code": "0", "data": [{"order_id": "ok-" + body["symbol"]}]}

    api = we.PaperAPI()
    api.account_id = "paper-test"
    api.trade = SimpleNamespace(
        account_v2=SimpleNamespace(
            get_account_list=lambda: {"data": [{"account_id": "paper-test"}]},
            get_account_balance=lambda account_id: {
                "available_cash": f"{state['cash']:.2f}",
                "buying_power": f"{state['cash']:.2f}",
            },
            get_account_position=lambda account_id: {"positions": []},
        ),
        order_v3=SimpleNamespace(place_order=place_order),
    )
    got = api.place_batch([
        {"ticker": "AAA", "side": "BUY", "shares": 1, "px": 80.0, "date": "2026-09-21"},
        {"ticker": "BBB", "side": "BUY", "shares": 1, "px": 80.0, "date": "2026-09-21"},
    ])
    assert placed == [("BBB", 1)]
    assert got[client_order_id("2026-09-21", "BUY", "AAA")]["ok"] is False
    assert got[client_order_id("2026-09-21", "BUY", "BBB")]["ok"] is True
    assert got[client_order_id("2026-09-21", "BUY", "BBB")]["shares"] == 1


def test_yml_warms_before_bell_and_has_one_automatic_sender() -> None:
    root = Path(__file__).resolve().parent.parent
    yml = (root / ".github/workflows/webull_paper.yml").read_text()
    assert "7 12,13" in yml
    assert "src.paper_open" in yml
    assert "--owner actions" in yml
    assert "--ready" not in yml
    assert "workflow_run:" not in yml
    assert "  push:" not in yml
    assert "src.webull_exec" not in (root / ".github/workflows/open_0930.yml").read_text()
    pub = (root / ".github/workflows/publish_strategy_tickets.yml").read_text()
    assert "src.paper_open" in pub
    assert "--submit" in pub
    assert "--ready" in pub
    assert "WEBULL_APP_KEY" in pub
    owner = (root / "00_grounding" / "paper_open_owner.json").read_text()
    assert '"owner": "actions"' in owner


def main() -> None:
    test_refuse_real_without_flags()
    test_paper_never_uses_live_host()
    test_client_order_id_stable_and_short()
    test_parse_account_and_book()
    test_dry_run_does_not_place()
    test_submit_uses_paper_place()
    test_env_strips_quoted_secrets()
    test_not_connected_writes_last_without_replay()
    test_hot4_submit_refuses_divergent_wire()
    test_stale_combo_does_not_submit()
    test_yml_warms_before_bell_and_has_one_automatic_sender()
    test_hot4_tickets_long_only_skip_held_cash_and_sit()
    test_hot4_sells_size_from_paper_lots()
    test_hot4_zero_cash_is_honest()
    test_paper_order_is_market_not_limit()
    test_place_batch_sends_one_order_at_a_time()
    test_place_batch_keeps_later_names_after_one_reject()
    test_cash_still_free_holds_unfilled_reserve()
    test_hot4_slip_buffer_covers_richer_open_fills()
    test_place_batch_shrinks_later_leg_when_earlier_fills_eat_cash()
    test_place_batch_skips_leg_that_cannot_buy_one_share()
    test_place_batch_keeps_haircut_plan_when_preopen_cash_is_unchanged()
    test_rejected_leg_does_not_reserve_cash()
    print("test_webull_exec: 23 ok")


if __name__ == "__main__":
    main()
