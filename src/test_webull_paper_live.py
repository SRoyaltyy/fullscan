"""Webull paper live board: observe-only, sandbox host, journal join."""
from __future__ import annotations

from pathlib import Path

from src import webull_exec as we
from src import webull_paper_live as wpl
from src.futubull_exec import BrokerSnap

ROOT = Path(__file__).resolve().parent.parent
WF = ROOT / ".github" / "workflows"


def test_parse_orders_normalizes_open_and_filled() -> None:
    rows = we.parse_orders({
        "data": [
            {"symbol": "US.DELL", "side": "BUY", "status": "SUBMITTED",
             "quantity": "428", "filled_quantity": "0",
             "client_order_id": "fs20260921BDELL",
             "order_id": "OID1", "order_type": "MARKET",
             "time_in_force": "DAY", "support_trading_session": "CORE"},
            {"symbol": "GME", "side": "BUY", "order_status": "FILLED",
             "qty": 10, "filled_qty": 10, "clientOrderId": "fsX",
             "orderId": "OID2"},
        ]
    })
    assert rows[0]["ticker"] == "DELL"
    assert rows[0]["status"] == "SUBMITTED"
    assert rows[0]["shares"] == 428
    assert rows[1]["ticker"] == "GME"
    assert rows[1]["status"] == "FILLED"
    assert rows[1]["filled_qty"] == 10


def test_join_marks_missing_when_ack_not_on_book() -> None:
    journal = {"sent": [
        {"ticker": "DELL", "side": "BUY", "shares": 428,
         "client_order_id": "fs20260921BDELL", "order_id": "OID1",
         "status": "acknowledged", "ok": True},
    ]}
    joined = wpl.join_journal(journal, [])
    assert joined[0]["bucket"] == "missing"
    assert joined[0]["on_broker"] is False
    broker = [{"ticker": "DELL", "side": "BUY", "shares": 428,
               "filled_qty": 0, "status": "SUBMITTED",
               "client_order_id": "fs20260921BDELL", "order_id": "OID1"}]
    joined = wpl.join_journal(journal, broker)
    assert joined[0]["bucket"] == "working"
    assert joined[0]["on_broker"] is True


def test_observe_never_places() -> None:
    class BoomAPI:
        host = we.PAPER_HOST
        account_id = "paper-1"
        err = None
        placed = 0

        def connect(self):
            return True

        def snapshot(self):
            return BrokerSnap(env="paper", cash=999662.39, buying_power=999662.39,
                              positions={"OLD": {"shares": 1, "cost_px": 10,
                                                 "last_px": 11, "mv": 11}},
                              connected=True, acc_id="paper-1")

        def list_open_orders(self):
            return [{"ticker": "DELL", "side": "BUY", "shares": 428,
                     "filled_qty": 0, "status": "SUBMITTED",
                     "client_order_id": "fs20260921BDELL", "order_id": "OID1"}]

        def list_history_orders(self, a, b):
            return []

        def order_detail(self, coid):
            return None

        def place(self, *a, **k):
            self.placed += 1
            raise AssertionError("observe must not place")

        def place_batch(self, *a, **k):
            self.placed += 1
            raise AssertionError("observe must not place")

    journal = {"date": "2026-09-21", "status": "acknowledged", "standing": True,
               "host": we.PAPER_HOST, "cash": 999662.39, "sent": [
                   {"ticker": "DELL", "side": "BUY", "shares": 428,
                    "client_order_id": "fs20260921BDELL", "order_id": "OID1",
                    "status": "acknowledged", "ok": True}]}
    api = BoomAPI()
    doc = wpl.observe(date="2026-09-21", api=api, journal=journal, write=False)
    assert api.placed == 0
    assert doc["host"] == we.PAPER_HOST
    assert doc["place"] is False
    assert doc["connected"] is True
    assert doc["open_n"] == 1
    assert doc["orders"][0]["on_broker"] is True
    assert doc["account_tail"] == "er-1"


def test_journal_only_skips_broker(tmp_path) -> None:
    journal = {"date": "2026-09-21", "status": "acknowledged", "standing": True,
               "sent": [{"ticker": "GME", "side": "BUY", "shares": 1,
                         "client_order_id": "fs20260921BGME",
                         "status": "acknowledged", "ok": True}]}
    doc = wpl.observe(date="2026-09-21", journal=journal, journal_only=True,
                      write=True, root=tmp_path)
    assert doc["source"] == "journal"
    assert doc["connected"] is False
    assert (tmp_path / "data" / "webull_paper" / "live.json").is_file()
    assert (tmp_path / "dashboard" / "webull-paper" / "live.json").is_file()


def test_workflow_is_observe_only() -> None:
    yml = (WF / "webull_paper_live.yml").read_text(encoding="utf-8")
    assert "src.webull_paper_live" in yml
    assert "--submit" not in yml
    assert "place_order" not in yml
    assert "Does not place" in yml or "Does not place, cancel" in yml
    assert "webull-openapi-python-sdk" in yml
    html = (ROOT / "dashboard" / "webull-paper" / "index.html").read_text(
        encoding="utf-8")
    assert "raw.githubusercontent.com/SRoyaltyy/fullscan/main/data/webull_paper/live.json" in html
    assert "setInterval(load, 15000)" in html
    pub = (ROOT / "scripts" / "publish_dashboard.sh").read_text(encoding="utf-8")
    assert "webull-paper" in pub
    dep = (WF / "deploy-dashboard.yml").read_text(encoding="utf-8")
    assert "webull-paper" in dep


if __name__ == "__main__":
    test_parse_orders_normalizes_open_and_filled()
    test_join_marks_missing_when_ack_not_on_book()
    test_observe_never_places()
    print("ok webull paper live")
