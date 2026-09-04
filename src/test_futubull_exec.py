"""Futubull sender: live tickets only, broker holdings, no silent REAL.

Run: python -m src.test_futubull_exec
"""
from __future__ import annotations

from src.futubull_exec import (
    BrokerSnap,
    plan_for_broker,
    refuse_real,
    send_card,
    sim_from_broker,
    tickets_to_send,
)


def test_refuse_real_without_flags() -> None:
    assert refuse_real("simulate", True, True) is None
    assert refuse_real("real", False, True) is None
    assert "pass --live" in (refuse_real("real", True, False) or "")
    assert "FUTU_LIVE" in (refuse_real("real", True, True) or "")


def test_tickets_never_include_would_buy() -> None:
    card = {
        "tickets": [
            {"side": "BUY", "ticker": "HOOD", "shares": 10, "px": 100,
             "status": "plan"},
            {"side": "BUY", "ticker": "ASND", "shares": 0, "px": 271,
             "status": "plan"},
        ],
        "would_buy": {"rows": [
            {"ticker": "HOOD", "shares": 106, "side": "BUY"},
            {"ticker": "ASND", "shares": 48, "side": "BUY"},
        ]},
    }
    got = tickets_to_send(card)
    assert [t["ticker"] for t in got] == ["HOOD"]


def test_empty_broker_can_fill_io_list() -> None:
    """A flat Futubull paper account is not the $100k replay — it can buy."""
    from pathlib import Path
    if not (Path("03_scoreboard") / "mover_lookback_action.json").is_file():
        print("skip empty-broker (no payload)")
        return
    snap = BrokerSnap(env="simulate", cash=100_000, positions={})
    card = plan_for_broker("2026-09-04", snap)
    buys = [t["ticker"] for t in tickets_to_send(card) if t["side"] == "BUY"]
    assert buys, "flat account on an io day should get 2w_size tickets"
    assert "HOOD" in buys or "XP" in buys or len(buys) >= 3
    assert card["cash_open"] == 100_000


def test_broker_hold_skips_that_name() -> None:
    from pathlib import Path
    if not (Path("03_scoreboard") / "mover_lookback_action.json").is_file():
        print("skip broker-hold (no payload)")
        return
    snap = BrokerSnap(env="simulate", cash=100_000, positions={
        "HOOD": {"shares": 50, "cost_px": 110, "last_px": 124, "mv": 6200},
    })
    card = plan_for_broker("2026-09-04", snap)
    buys = {t["ticker"] for t in tickets_to_send(card) if t["side"] == "BUY"}
    assert "HOOD" not in buys
    assert any(s.get("ticker") == "HOOD" and s.get("reason") == "already held"
               for s in card["skipped"])


def test_dry_run_does_not_place() -> None:
    class Boom:
        def place(self, *a, **k):
            raise AssertionError("dry-run must not place")
    snap = BrokerSnap(env="simulate", cash=10, positions={})
    card = {"date": "2026-09-04", "tickets": [
        {"side": "BUY", "ticker": "BVS", "shares": 1, "px": 14.0,
         "clock": "16:00 ET", "sleeve": "io_core", "status": "plan"},
    ], "would_buy": {"rows": [{"ticker": "HOOD", "shares": 99}]}}
    last = send_card(card, snap, submit=False, opend=Boom(), env="simulate")
    assert last["sent"][0]["status"] == "dry_run"
    assert last["n_would"] == 1
    assert last["n_tickets"] == 1


def test_sim_from_broker_maps_positions() -> None:
    snap = BrokerSnap(env="simulate", cash=50, positions={
        "SOFI": {"shares": 10, "cost_px": 19, "last_px": 18, "mv": 180},
    })
    sim = sim_from_broker(snap, {"calendar": ["2026-09-03"]})
    assert sim["cash"] == 50
    assert sim["open_io"]["SOFI"]["shares"] == 10
    assert sim["open_mover"] == []


def main() -> None:
    test_refuse_real_without_flags()
    test_tickets_never_include_would_buy()
    test_sim_from_broker_maps_positions()
    test_dry_run_does_not_place()
    test_empty_broker_can_fill_io_list()
    test_broker_hold_skips_that_name()
    print("test_futubull_exec: 6 ok")


if __name__ == "__main__":
    main()
