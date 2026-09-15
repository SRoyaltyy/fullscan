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
                "tickets": [], "stale": False, "policy": "combo_sh_macd_5050_shared",
            }), \
            mock.patch.object(we, "write_last") as wl, \
            mock.patch.object(we, "inject_today_from_disk"):
        rc = we.run("2026-09-14", submit=True, write=True)
    assert rc == 0
    last = wl.call_args[0][0]
    assert last["connected"] is False
    assert last["n_tickets"] == 0
    assert last["source"] == "combo"
    assert last["combo"] == "combo_sh_macd_5050_shared"
    assert "401" in (last.get("error") or "")


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
    assert rc == 0
    last = wl.call_args[0][0]
    assert last["submit"] is False
    assert last["sent"][0]["status"] == "dry_run"
    assert last["stale"] is True


def test_yml_poke_on_main_submits() -> None:
    """Cloud agent cannot workflow_dispatch; a main poke must submit paper."""
    yml = Path(__file__).resolve().parent.parent.joinpath(
        ".github", "workflows", "webull_paper.yml"
    ).read_text(encoding="utf-8")
    assert "branches: [main]" in yml
    assert '".github/workflows/webull_paper.yml"' in yml
    assert "github.event_name == 'push'" in yml
    assert "github.event_name == 'schedule'" in yml
    assert "--source combo" in yml
    assert "combo_sh_macd_5050_shared" in yml
    assert "POKE 2026-09-14" in yml
    assert 'cron: "30 13 * * 1-5"' in yml
    assert 'cron: "30 14 * * 1-5"' in yml
    assert "src.open_0930_clock" in yml
    assert "workflow_dispatch" in yml
    # A 01:00 merge poke is clock-gated; daytime poke still submits.


def main() -> None:
    test_refuse_real_without_flags()
    test_paper_never_uses_live_host()
    test_client_order_id_stable_and_short()
    test_parse_account_and_book()
    test_dry_run_does_not_place()
    test_submit_uses_paper_place()
    test_env_strips_quoted_secrets()
    test_not_connected_writes_last_without_replay()
    test_stale_combo_does_not_submit()
    test_yml_poke_on_main_submits()
    print("test_webull_exec: 10 ok")


if __name__ == "__main__":
    main()
