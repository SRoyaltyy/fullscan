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
    """Pinned form: strategy, session date, ticker, side. No random, no clock."""
    a = client_order_id("2026-10-07", "BUY", "SDEV")
    b = client_order_id("2026-10-07", "BUY", "SDEV", strategy="h1")
    c = client_order_id("2026-10-07", "SELL", "SDEV")
    assert a == b == "h1-2026-10-07-SDEV-buy"
    assert c == "h1-2026-10-07-SDEV-sell"
    assert len(a) <= 32
    assert client_order_id("2026-09-11", "BUY", "HOOD") == "h1-2026-09-11-HOOD-buy"
    assert client_order_id("2026-09-11", "SELL", "HOOD") == "h1-2026-09-11-HOOD-sell"
    assert client_order_id("2026-09-11", "BUY", "BRK-B") == "h1-2026-09-11-BRKB-buy"
    long_name = "ABCDEFGHIJKLMNOPQRST"
    long_id = client_order_id("2026-10-07", "BUY", long_name)
    assert long_id == client_order_id("2026-10-07", "buy", long_name)
    assert len(long_id) <= 32
    assert long_id.startswith("h1-")
    assert long_id.endswith("-buy")
    empty = client_order_id("", "BUY", "")
    assert empty == client_order_id("", "BUY", "")
    assert len(empty) <= 32


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
    assert pos["SOFI"]["quantity"] == 10
    assert pos["SOFI"]["available_quantity"] is None
    assert "SKIP" not in pos
    assert parse_order_id({"data": [{"order_id": "abc"}]}) == "abc"


def test_parse_positions_keeps_held_quantity_when_available_is_zero() -> None:
    """SDK-shaped rows. available_quantity 0 must not drop a positive quantity."""
    pos = parse_positions({
        "has_next": False,
        "positions": [
            {
                "instrument_id": "913000001",
                "symbol": "QSI",
                "instrument_type": "EQUITY",
                "currency": "USD",
                "quantity": "2482",
                "available_quantity": "0",
                "cost_price": "1.28",
                "last_price": "1.30",
                "market_value": "3226.60",
                "account_id": "SECRET-ACCOUNT",
            },
            {
                "symbol": "DNA",
                "quantity": "214",
                "available_quantity": "214",
                "cost_price": "14.50",
                "last_price": "14.50",
                "market_value": "3103.00",
            },
            {"symbol": "FLAT", "quantity": "0", "available_quantity": "0"},
        ],
    })
    assert pos["QSI"]["shares"] == 2482
    assert pos["QSI"]["quantity"] == 2482
    assert pos["QSI"]["available_quantity"] == 0
    assert pos["QSI"]["cost_px"] == 1.28
    assert pos["QSI"]["raw"] == {
        "symbol": "QSI",
        "quantity": "2482",
        "available_quantity": "0",
    }
    assert "account_id" not in pos["QSI"]["raw"]
    assert "instrument_id" not in pos["QSI"]["raw"]
    assert pos["DNA"]["shares"] == 214
    assert pos["DNA"]["available_quantity"] == 214
    assert "FLAT" not in pos


def _position_api(get_account_position):
    from types import SimpleNamespace
    from src import webull_exec as we

    api = we.PaperAPI()
    api.account_id = "paper-test"
    api.trade = SimpleNamespace(
        account_v2=SimpleNamespace(
            get_account_list=lambda: {
                "data": [{"account_id": "paper-test", "account_type": "PAPER"}],
            },
            get_account_balance=lambda account_id: {
                "available_cash": "1000",
                "buying_power": "1000",
            },
            get_account_position=get_account_position,
        ),
    )
    return api


def test_snapshot_reads_one_page_when_the_method_does_not_page() -> None:
    """AccountV2.get_account_position(account_id) has no page cursor."""
    calls = []

    def get_account_position(account_id):
        calls.append(account_id)
        return {
            "positions": [{
                "symbol": "QSI",
                "quantity": "2482",
                "available_quantity": "0",
                "cost_price": "1.28",
            }],
        }

    snap = _position_api(get_account_position).snapshot()
    assert snap.connected is True
    assert calls == ["paper-test"]
    assert snap.positions["QSI"]["shares"] == 2482
    assert snap.positions["QSI"]["available_quantity"] == 0


def test_snapshot_follows_position_pages() -> None:
    pages = {
        None: {
            "has_next": True,
            "last_instrument_id": "i-dna",
            "positions": [{
                "symbol": "DNA",
                "instrument_id": "i-dna",
                "quantity": "214",
                "available_quantity": "214",
                "cost_price": "14.50",
            }],
        },
        "i-dna": {
            "has_next": False,
            "positions": [{
                "symbol": "QSI",
                "instrument_id": "i-qsi",
                "quantity": "2482",
                "available_quantity": "0",
                "cost_price": "1.28",
                "account_id": "SECRET-ACCOUNT",
            }],
        },
    }
    seen = []

    def get_account_position(account_id, page_size=10, last_instrument_id=None):
        seen.append((account_id, page_size, last_instrument_id))
        return pages[last_instrument_id]

    snap = _position_api(get_account_position).snapshot()
    assert snap.connected is True
    assert seen == [
        ("paper-test", 100, None),
        ("paper-test", 100, "i-dna"),
    ]
    assert snap.positions["DNA"]["shares"] == 214
    assert snap.positions["QSI"]["shares"] == 2482
    assert snap.positions["QSI"]["available_quantity"] == 0
    assert "SECRET-ACCOUNT" not in str(snap.positions)


def test_position_pagination_stops_at_the_cap() -> None:
    from src import webull_exec as we

    def get_account_position(account_id, page_size=10, last_instrument_id=None):
        n = 0 if last_instrument_id is None else int(last_instrument_id)
        return {
            "has_next": True,
            "last_instrument_id": str(n + 1),
            "positions": [{
                "symbol": "S" + str(n),
                "quantity": "1",
                "instrument_id": str(n),
            }],
        }

    snap = _position_api(get_account_position).snapshot()
    assert snap.connected is False
    assert "position pagination exceeded " + str(we._POSITION_PAGE_CAP) in (
        snap.error or "")


def test_unavailable_lot_is_still_held_for_a_sealed_sell() -> None:
    """paper_open release() reads shares from snapshot() -> parse_positions."""
    from src import paper_drift

    positions = parse_positions({"positions": [{
        "symbol": "QSI",
        "quantity": "2482",
        "available_quantity": "0",
        "cost_price": "1.28",
    }]})
    sendable, skipped, foreign, warning = paper_drift.partition_tickets(
        [{"ticker": "QSI", "side": "SELL", "shares": 2482, "date": "2026-10-07"}],
        positions,
        "2026-10-07",
        positions_known=True,
    )
    assert foreign == []
    assert skipped == []
    assert warning is None
    assert sendable[0]["ticker"] == "QSI"
    assert positions["QSI"]["shares"] == 2482


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
    """A sealed-plan failure is not submitted and does not call pick_day."""
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
            raise AssertionError("sealed-plan failure must not place")

        def place_batch(self, *a, **k):
            raise AssertionError("sealed-plan failure must not place")

    card = {
        "date": "2026-09-21", "stale": False, "policy": HOT4,
        "tickets": [],
        "would_buy": {"rows": []},
        "would_sell": {"rows": []},
        "hard_red": False,
        "look_error": (
            "paper cash 1.00 cannot fund sealed h1 buys; "
            "refusing; not rebuilding HOT4"
        ),
        "why": "refusing; not rebuilding HOT4",
        "skipped": [],
    }
    with mock.patch.object(we, "PaperAPI", return_value=Alive()), \
            mock.patch.object(we, "_plan", return_value=card), \
            mock.patch(
                "src.factor_mine.pick_day",
                side_effect=AssertionError("pick_day"),
            ), \
            mock.patch.object(we, "write_last") as wl, \
            mock.patch.object(we, "inject_today_from_disk"):
        rc = we.run("2026-09-21", submit=True, write=True, source="hot4")
    assert rc == 2
    last = wl.call_args[0][0]
    assert last["submit"] is False
    assert last["sent"] == []
    assert "not rebuilding HOT4" in (last.get("why") or "")


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
    # The published panel is not the send list. 2026-09-17 has no sealed plan.
    snap = BrokerSnap(env="paper", cash=10_000, positions={"INDP": {"shares": 1}})
    card = plan_hot4_for_broker("2026-09-17", snap, payload=payload)
    assert card["policy"] == HOT4
    assert card["source"] == "sealed_h1"
    assert card["tickets"] == []
    assert "not rebuilding HOT4" in card["look_error"]
    assert "INDP" not in card["look_error"]


def test_hot4_sells_size_from_paper_lots() -> None:
    """The leftover sizer still uses the paper lot. The sealed send path does not."""
    bare, bare_skips = size_hot4_sells(
        ["FEAM", "NOPE"], positions={"FEAM": {"shares": 2}}, date="2026-09-22",
    )
    assert [(t["ticker"], t["shares"]) for t in bare] == [("FEAM", 2)]
    assert any(s["kind"] == "unheld" and s["ticker"] == "NOPE" for s in bare_skips)


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
    card = plan_hot4_for_broker("2026-10-02", snap, payload=payload)
    assert card["tickets"] == []
    assert [r["ticker"] for r in card["would_buy"]["rows"]] == ["QSI", "TJGC"]
    assert "not rebuilding HOT4" in card["why"]
    assert "cannot fund sealed h1 buys" in card["why"]


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


def test_empty_account_id_is_discovered_and_not_printed(tmp_path=None) -> None:
    """Keys alone resolve the sandbox account id. The id is not printed."""
    import io
    import os
    import tempfile
    from contextlib import redirect_stdout

    from src import webull_exec as we

    if tmp_path is None:
        tmp_path = Path(tempfile.mkdtemp())
    env_path = tmp_path / "paper.env"
    we.write_paper_env(env_path, {
        "WEBULL_APP_KEY": "test-key",
        "WEBULL_APP_SECRET": "test-secret",
    })
    saved = {name: os.environ.get(name) for name in (
        "WEBULL_APP_KEY", "WEBULL_APP_SECRET", "WEBULL_ACCOUNT_ID",
    )}

    class Fake:
        env = "paper"
        host = we.PAPER_HOST
        account_id = ""
        err = None

        def connect(self):
            return True

        def snapshot(self):
            self.account_id = "paper-discovered"
            return BrokerSnap(
                env="paper", cash=25.0, positions={}, connected=True,
                acc_id="paper-discovered",
            )

    buf = io.StringIO()
    try:
        with redirect_stdout(buf):
            status = we.discover_and_persist_account_id(env_path, api=Fake())
        assert status == "discovered"
        stored = we.read_paper_env(env_path)
        assert stored["WEBULL_ACCOUNT_ID"] == "paper-discovered"
        assert stored["WEBULL_APP_KEY"] == "test-key"
        assert "paper-discovered" not in buf.getvalue()
        assert "test-secret" not in buf.getvalue()
        again = we.discover_and_persist_account_id(env_path, api=Fake())
        assert again == "present"
        assert we.read_paper_env(env_path)["WEBULL_ACCOUNT_ID"] == "paper-discovered"

        class Live:
            env = "real"
            host = we.LIVE_HOST

            def connect(self):
                raise AssertionError("live connect")

        try:
            we.discover_and_persist_account_id(env_path, api=Live())
        except RuntimeError as exc:
            assert "non-sandbox" in str(exc)
        else:
            raise AssertionError("live host was accepted")
    finally:
        for name, value in saved.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value


def test_yml_warms_before_bell_and_has_one_automatic_sender() -> None:
    root = Path(__file__).resolve().parent.parent
    yml = (root / ".github/workflows/webull_paper.yml").read_text()
    assert "7 12,13" in yml
    assert "src.paper_open" in yml
    assert "--owner actions" in yml
    # --ready is the seal trigger only, and only before 09:30 ET.
    assert "h1 append-only forward" in yml
    assert "workflow_run:" in yml
    assert "-lt 930" in yml
    assert yml.index("[ \"$SEAL_EVENT\" = \"workflow_run\" ]") < yml.index("ARGS+=(--ready)")
    assert "  push:" not in yml
    assert "src.webull_exec" not in (root / ".github/workflows/open_0930.yml").read_text()
    pub = (root / ".github/workflows/publish_strategy_tickets.yml").read_text()
    assert "python -m src.paper_open" not in pub
    assert "--submit" not in pub
    assert "--ready" not in pub
    assert "WEBULL_APP_KEY" not in pub
    assert "data/paper_open/" not in pub
    assert "webull_last.json" not in pub
    assert "PAPER_OPEN_SENDER" in yml
    assert "github.event_name == 'workflow_run'" in yml
    assert "github.event_name == 'schedule'" not in yml
    assert "inputs.submit" not in yml
    assert "not the h1 seal; no paper submit and no paper_open write" in yml
    owner = (root / "00_grounding" / "paper_open_owner.json").read_text()
    assert '"owner": "actions"' in owner


def test_session_query_fails_closed_without_a_history_method() -> None:
    from types import SimpleNamespace
    from src.webull_exec import PaperAPI

    api = PaperAPI()
    api.account_id = "aid-1"
    api.trade = SimpleNamespace(
        order_v3=SimpleNamespace(
            list_order_open=lambda aid: {"data": []},
            get_order_open=lambda aid: (_ for _ in ()).throw(RuntimeError("no")),
            get_order_detail=lambda aid, oid: (_ for _ in ()).throw(RuntimeError("no")),
        ),
    )
    try:
        api.list_session_orders("2026-10-07")
    except RuntimeError as exc:
        assert "filled-order" in str(exc)
    else:
        raise AssertionError("query must fail closed")


def test_session_query_keeps_open_and_filled_for_that_day() -> None:
    from types import SimpleNamespace
    from src.webull_exec import PaperAPI

    def history(aid, start_date=None, end_date=None):
        assert aid == "aid-1"
        assert start_date == "2026-10-07" and end_date == "2026-10-08"
        return {"orders": [
            {"order_id": "F1", "client_order_id": "h1-2026-10-07-SDEV-buy",
             "symbol": "SDEV", "side": "BUY", "status": "FILLED",
             "filled_time": "2026-10-07T09:30:01-04:00"},
            {"order_id": "OLD", "client_order_id": "h1-2026-10-06-SDEV-buy",
             "symbol": "SDEV", "side": "BUY", "status": "FILLED",
             "filled_time": "2026-10-06T09:30:01-04:00"},
            {"order_id": "C1", "symbol": "AAA", "side": "BUY", "status": "CANCELLED",
             "order_time": "2026-10-07T09:31:00-04:00"},
        ]}

    api = PaperAPI()
    api.account_id = "aid-1"
    api.trade = SimpleNamespace(
        order_v3=SimpleNamespace(
            list_order_open=lambda aid: {"orders": [
                {"order_id": "O1", "client_order_id": "h1-2026-10-07-AAA-buy",
                 "symbol": "AAA", "side": "BUY", "status": "SUBMITTED"},
            ]},
            get_order_open=lambda aid: (_ for _ in ()).throw(AssertionError("second")),
            get_order_detail=lambda aid, oid: (_ for _ in ()).throw(AssertionError("detail")),
            get_order_history=history,
        ),
    )
    rows = api.list_session_orders("2026-10-07")
    assert {row["order_id"] for row in rows} == {"O1", "F1"}


def test_history_time_candidates_pin_formats() -> None:
    """Formats after the sandbox rejected RFC822 -0400 and a bare date.

    The SDK docstring says yyyy-MM-dd'T'HH:mm:ss.SSSZ and gives no example.
    The client's x-timestamp header is yyyy-MM-dd'T'HH:mm:ssZ.
    """
    from src.webull_exec import history_time_candidates

    got = dict(history_time_candidates("2026-10-06"))
    assert list(got) == ["iso_offset", "utc_millis_z", "utc_z", "epoch_ms"]
    assert got["iso_offset"] == [
        ("2026-10-06T00:00:00.000-04:00", "2026-10-06T23:59:59.999-04:00"),
    ]
    assert got["utc_millis_z"] == [
        ("2026-10-06T04:00:00.000Z", "2026-10-07T03:59:59.999Z"),
    ]
    assert got["utc_z"] == [
        ("2026-10-06T04:00:00Z", "2026-10-07T03:59:59Z"),
    ]
    assert got["epoch_ms"] == [("1791259200000", "1791345599999")]
    for windows in got.values():
        for start, end in windows:
            assert start != "2026-10-06"
            assert "-0400" not in start and "-0400" not in end
    # Fall-back Sunday stays two windows, each at most 24h, in one format.
    fall = dict(history_time_candidates("2026-11-01"))
    assert fall["utc_millis_z"] == [
        ("2026-11-01T04:00:00.000Z", "2026-11-02T03:59:59.999Z"),
        ("2026-11-02T04:00:00.000Z", "2026-11-02T04:59:59.999Z"),
    ]


def test_list_order_history_gets_sdk_timestamps_not_a_bare_date() -> None:
    from types import SimpleNamespace
    from src.webull_exec import PaperAPI

    seen = []

    def list_order_history(account_id, start_time=None, end_time=None,
                           pagination_key=None):
        seen.append((start_time, end_time, pagination_key))
        assert start_time != "2026-10-06" and end_time != "2026-10-06"
        if pagination_key == "p2":
            return {"orders": [{
                "order_id": "P2", "client_order_id": "c2",
                "symbol": "AAA", "side": "BUY", "status": "FILLED",
            }]}
        return {
            "pagination_key": "p2",
            "orders": [{
                "order_id": "P1", "client_order_id": "c1",
                "symbol": "SDEV", "side": "BUY", "status": "FILLED",
            }],
        }

    api = PaperAPI()
    api.account_id = "aid-1"
    api.trade = SimpleNamespace(
        order_v3=SimpleNamespace(list_order_history=list_order_history),
    )
    rows = api.list_filled_orders("2026-10-06")
    assert seen == [
        ("2026-10-06T00:00:00.000-04:00", "2026-10-06T23:59:59.999-04:00", None),
        ("2026-10-06T00:00:00.000-04:00", "2026-10-06T23:59:59.999-04:00", "p2"),
    ]
    assert {row["order_id"] for row in rows} == {"P1", "P2"}
    assert api.history_formats["2026-10-06"] == "iso_offset"


def test_get_order_history_page_size_is_100() -> None:
    from types import SimpleNamespace
    from src.webull_exec import PaperAPI

    def list_order_history(account_id, start_time=None, end_time=None):
        raise RuntimeError("HTTP 417 invalid start_time")

    def get_order_history(account_id, page_size=None, start_date=None, end_date=None):
        assert page_size == 100
        assert start_date == "2026-10-06" and end_date == "2026-10-07"
        return {"orders": [{
            "order_id": "Z", "symbol": "AAA", "side": "BUY", "status": "FILLED",
        }]}

    api = PaperAPI()
    api.account_id = "aid-1"
    api.trade = SimpleNamespace(
        order_v3=SimpleNamespace(
            list_order_history=list_order_history,
            get_order_history=get_order_history,
        ),
    )
    rows = api.list_filled_orders("2026-10-06")
    assert [row["order_id"] for row in rows] == ["Z"]


def test_morning_status_records_fill_price_without_placing(tmp_path) -> None:
    from datetime import datetime, timedelta
    from types import SimpleNamespace
    from src import paper_open as po
    from src.webull_exec import PaperAPI

    day = "2026-10-06"
    coid = client_order_id(day, "BUY", "SDEV")
    seen = []

    def list_order_history(account_id, start_time=None, end_time=None,
                           pagination_key=None):
        seen.append(start_time)
        return {"orders": [{
            "order_id": "OID-S",
            "client_order_id": coid,
            "symbol": "SDEV",
            "side": "BUY",
            "status": "FILLED",
            "avg_filled_price": "4.25",
            "filled_quantity": "10",
            "price": "9.99",
            "filled_time": day + "T09:31:00-04:00",
        }]}

    api = PaperAPI()
    api.account_id = "aid-1"
    api.trade = SimpleNamespace(
        order_v3=SimpleNamespace(
            list_order_open=lambda aid: {"orders": []},
            get_order_open=lambda aid: (_ for _ in ()).throw(RuntimeError("no")),
            get_order_detail=lambda aid, oid: (_ for _ in ()).throw(RuntimeError("no")),
            list_order_history=list_order_history,
        ),
    )
    placed = []
    api.place_batch = lambda tickets: placed.append(tickets) or {}
    early = datetime.fromisoformat(day + "T08:41:00-04:00")
    result = po.release(
        {
            "date": day,
            "prepared_at": (early - timedelta(seconds=10)).isoformat(),
            "fingerprint": "t",
            "card": {"tickets": [{
                "ticker": "SDEV", "side": "BUY", "shares": 10,
                "px": 9.99, "date": day,
            }]},
        },
        api, lambda: early, tmp_path / "seal.json",
        submit=True, standing=True,
    )
    assert placed == []
    assert seen == ["2026-10-06T00:00:00.000-04:00"]
    assert api.last_history_format == "iso_offset"
    assert result["status"] == "already_submitted"
    sent = result["sent"][0]
    assert sent["status"] == "already_submitted"
    assert sent["avg_fill_px"] == 4.25
    assert sent["filled_qty"] == 10
    assert sent["broker_status"] == "FILLED"
    assert sent["avg_fill_px"] != 9.99


def test_rejected_time_format_tries_the_next_and_keeps_the_full_error() -> None:
    from types import SimpleNamespace
    from src.webull_exec import PaperAPI, history_error_text

    dump = "Request:{ " + ("x" * 400)
    msg = (
        "ServerException:HTTP Status: 417, Code: OPENAPI_PARAM_ERR, "
        "Msg: Parameter error, invalid start_time, value: "
        "2026-10-06T00:00:00.000-04:00, RequestID: abc-full"
    )
    assert history_error_text(RuntimeError(dump + msg)) == msg
    assert "RequestID: abc-full" in msg
    seen = []

    def list_order_history(account_id, start_time=None, end_time=None,
                           pagination_key=None):
        seen.append(start_time)
        if start_time and str(start_time).endswith("-04:00"):
            raise RuntimeError(dump + msg)
        return {"orders": [{
            "order_id": "OK", "symbol": "AAA", "side": "BUY", "status": "FILLED",
            "filled_time": "2026-10-06T09:31:00-04:00",
        }]}

    api = PaperAPI()
    api.account_id = "aid-1"
    api.trade = SimpleNamespace(
        order_v3=SimpleNamespace(list_order_history=list_order_history),
    )
    rows = api.list_filled_orders("2026-10-06")
    assert rows[0]["order_id"] == "OK"
    assert seen[0] == "2026-10-06T00:00:00.000-04:00"
    assert seen[1] == "2026-10-06T04:00:00.000Z"
    assert api.history_formats["2026-10-06"] == "utc_millis_z"

    date_msg = msg.replace("invalid start_time", "invalid start_date,end_date")

    def reject_times(account_id, start_time=None, end_time=None, pagination_key=None):
        raise RuntimeError(dump + msg)

    def reject_dates(account_id, page_size=None, start_date=None, end_date=None):
        raise RuntimeError(dump + date_msg)

    failed = PaperAPI()
    failed.account_id = "aid-1"
    failed.trade = SimpleNamespace(
        order_v3=SimpleNamespace(
            list_order_history=reject_times,
            get_order_history=reject_dates,
        ),
    )
    try:
        failed.list_filled_orders("2026-10-06")
    except RuntimeError as exc:
        text = str(exc)
    else:
        raise AssertionError("history failure must raise")
    assert "RequestID: abc-full" in text
    assert "invalid start_time" in text
    assert "invalid start_date,end_date" in text
    assert "Request:{" not in text
    assert "HTTP Status: 417" in text


def test_working_order_limit_is_not_a_fill_price() -> None:
    from src.webull_exec import match_sealed_orders

    day = "2026-10-06"
    coid = client_order_id(day, "BUY", "SDEV")
    found, missing = match_sealed_orders(
        [{"ticker": "SDEV", "side": "BUY", "shares": 10, "date": day}],
        [{
            "order_id": "1", "client_order_id": coid, "symbol": "SDEV",
            "side": "BUY", "status": "SUBMITTED", "price": "9.99",
            "order_time": day + "T08:00:00-04:00",
        }],
        day,
    )
    assert missing == []
    assert "avg_fill_px" not in found[0]
    assert found[0]["broker_status"] == "SUBMITTED"


def test_broker_guard_present_partial_and_query_failed() -> None:
    """No local status file. The sandbox book decides what may be sent."""
    import tempfile
    from datetime import datetime, timedelta
    from src import paper_open as po

    day = "2026-10-07"
    bell = datetime.fromisoformat(day + "T09:30:00-04:00")
    early = datetime.fromisoformat(day + "T08:41:00-04:00")
    tickets = [
        {"ticker": "SDEV", "side": "BUY", "shares": 10, "px": 5, "date": day},
        {"ticker": "AAA", "side": "BUY", "shares": 8, "px": 5, "date": day},
    ]
    coid_s = client_order_id(day, "BUY", "SDEV")
    assert coid_s == "h1-2026-10-07-SDEV-buy"
    book = [
        {"order_id": "OID-S", "client_order_id": coid_s, "symbol": "SDEV",
         "side": "BUY", "status": "FILLED",
         "filled_time": day + "T09:30:01-04:00"},
        {"order_id": "OID-A", "client_order_id": "fs-old-aaa", "symbol": "AAA",
         "side": "BUY", "status": "SUBMITTED"},
    ]

    class Book:
        host = "api.sandbox.webull.com"

        def __init__(self, rows=None, error=None):
            self.rows = list(rows or [])
            self.error = error
            self.calls = []

        def list_session_orders(self, date):
            assert date == day
            if self.error:
                raise RuntimeError(self.error)
            return list(self.rows)

        def place_batch(self, batch):
            self.calls.append(list(batch))
            return {
                client_order_id(t["date"], t["side"], t["ticker"]):
                {"ok": True, "order_id": "new-" + t["ticker"]}
                for t in batch
            }

    def plan():
        return {
            "date": day,
            "prepared_at": (bell - timedelta(seconds=10)).isoformat(),
            "fingerprint": "t",
            "card": {"tickets": [dict(t) for t in tickets]},
        }

    folder = Path(tempfile.mkdtemp())
    assert not (folder / f"{day}_status.json").exists()
    present = Book(rows=book)
    result = po.release(
        plan(), present, lambda: early, folder / "seal.json",
        submit=True, standing=True)
    assert present.calls == []
    assert result["status"] == "already_submitted"
    assert {row["ticker"] for row in result["found"]} == {"SDEV", "AAA"}
    assert {row["match"] for row in result["found"]} == {"client_order_id", "symbol_side"}
    bell_book = Book(rows=book)
    bell_result = po.release(
        plan(), bell_book, lambda: bell, folder / "bell.json", submit=True)
    assert bell_book.calls == []
    assert bell_result["status"] == "already_submitted"

    partial = Book(rows=[book[0]])
    partial_result = po.release(
        plan(), partial, lambda: early, folder / "partial.json",
        submit=True, standing=True)
    assert partial_result["status"] == "acknowledged"
    assert [row["ticker"] for row in partial.calls[0]] == ["AAA"]
    by = {row["ticker"]: row["status"] for row in partial_result["sent"]}
    assert by["SDEV"] == "already_submitted"
    assert by["AAA"] == "acknowledged"
    assert [row["ticker"] for row in partial_result["found"]] == ["SDEV"]

    failed = Book(error="sandbox down")
    journal = folder / "failed.json"
    failed_result = po.release(
        plan(), failed, lambda: early, journal, submit=True, standing=True)
    assert failed.calls == []
    assert failed_result["status"] == "query_failed"
    assert failed_result["sent"] == []
    assert not journal.exists()


def test_run_refuses_submit_after_the_open() -> None:
    """``webull_exec --submit`` and sleeve_merge ``--submit-webull`` share this."""
    from datetime import datetime
    from unittest import mock
    from src import webull_exec as we

    class Alive:
        env = "paper"
        host = "api.sandbox.webull.com"
        err = None

        def connect(self):
            return True

        def snapshot(self):
            return BrokerSnap(env="paper", cash=1_000_000, positions={},
                              connected=True, acc_id="paper-1")

        def place(self, *a, **k):
            raise AssertionError("late webull_exec submit must not place")

        def place_batch(self, *a, **k):
            raise AssertionError("late webull_exec submit must not place")

    card = {
        "date": "2026-10-06", "stale": False, "policy": HOT4,
        "tickets": [
            {"side": "BUY", "ticker": "PACB", "shares": 1113, "px": 2.87,
             "status": "plan", "date": "2026-10-06"},
            {"side": "BUY", "ticker": "DNA", "shares": 214, "px": 3.0,
             "status": "plan", "date": "2026-10-06"},
            {"side": "BUY", "ticker": "QSI", "shares": 2482, "px": 2.0,
             "status": "plan", "date": "2026-10-06"},
        ],
        "would_buy": {"rows": []},
        "skipped": [],
        "hard_red": False,
    }
    late = datetime.fromisoformat("2026-10-06T10:17:54-04:00")
    with mock.patch.object(we, "PaperAPI", return_value=Alive()), \
            mock.patch.object(we, "_plan", return_value=card), \
            mock.patch.object(we, "write_last") as wrote, \
            mock.patch.object(we, "inject_today_from_disk"):
        rc = we.run("2026-10-06", submit=True, write=True, source="hot4", clock=late)
    assert rc == 2
    last = wrote.call_args[0][0]
    assert last["status"] == "missed_deadline"
    assert last["submit"] is False
    assert last["sent"]
    assert all(row["status"] == "dry_run" for row in last["sent"])
    early = datetime.fromisoformat("2026-10-06T08:41:00-04:00")
    with mock.patch.object(we, "PaperAPI", return_value=Alive()), \
            mock.patch.object(we, "_plan", return_value=card), \
            mock.patch.object(we, "write_last") as wrote_early, \
            mock.patch.object(we, "inject_today_from_disk"):
        rc_early = we.run(
            "2026-10-06", submit=True, write=True, source="hot4", clock=early)
    assert rc_early == 2
    early_last = wrote_early.call_args[0][0]
    assert early_last["status"] == "refused"
    assert early_last["submit"] is False
    assert all(row["status"] == "dry_run" for row in early_last["sent"])


def main() -> None:
    test_refuse_real_without_flags()
    test_paper_never_uses_live_host()
    test_client_order_id_stable_and_short()
    test_session_query_fails_closed_without_a_history_method()
    test_session_query_keeps_open_and_filled_for_that_day()
    test_broker_guard_present_partial_and_query_failed()
    test_parse_account_and_book()
    test_parse_positions_keeps_held_quantity_when_available_is_zero()
    test_snapshot_reads_one_page_when_the_method_does_not_page()
    test_snapshot_follows_position_pages()
    test_position_pagination_stops_at_the_cap()
    test_unavailable_lot_is_still_held_for_a_sealed_sell()
    test_dry_run_does_not_place()
    test_submit_uses_paper_place()
    test_env_strips_quoted_secrets()
    test_not_connected_writes_last_without_replay()
    test_hot4_submit_refuses_divergent_wire()
    test_stale_combo_does_not_submit()
    test_empty_account_id_is_discovered_and_not_printed()
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
    test_run_refuses_submit_after_the_open()
    print("test_webull_exec: 33 ok")


if __name__ == "__main__":
    main()
