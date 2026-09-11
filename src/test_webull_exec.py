"""Webull paper sender: sandbox host, live tickets only, no silent REAL.

Run: python -m src.test_webull_exec
"""
from __future__ import annotations

from pathlib import Path

from src.futubull_exec import BrokerSnap, send_card
from src.webull_exec import (
    client_order_id,
    paper_host,
    parse_account_id,
    parse_balance,
    parse_order_id,
    parse_positions,
    refuse_real,
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


def test_yml_poke_on_main_submits() -> None:
    """Cloud agent cannot workflow_dispatch; a main poke must submit paper."""
    yml = Path(__file__).resolve().parent.parent.joinpath(
        ".github", "workflows", "webull_paper.yml"
    ).read_text(encoding="utf-8")
    assert "branches: [main]" in yml
    assert '".github/workflows/webull_paper.yml"' in yml
    assert "github.event_name == 'push'" in yml


def main() -> None:
    test_refuse_real_without_flags()
    test_paper_never_uses_live_host()
    test_client_order_id_stable_and_short()
    test_parse_account_and_book()
    test_dry_run_does_not_place()
    test_submit_uses_paper_place()
    test_yml_poke_on_main_submits()
    print("test_webull_exec: 7 ok")


if __name__ == "__main__":
    main()
