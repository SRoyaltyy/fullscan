"""Read-only sandbox snapshot. No orders, no rewrite of the 10-06 gap."""
from datetime import datetime
import ast
import json
from pathlib import Path

import pytest

from src import paper_account_snapshot as snap
from src.webull_exec import PAPER_HOST, PaperAPI

ROOT = Path(__file__).resolve().parent.parent
REAL_LOG = ROOT / "data" / "paper_open" / "drift_log.jsonl"
WF = ROOT / ".github" / "workflows" / "webull_paper_positions.yml"
ET = snap.ET
OUTSIDE = datetime(2026, 10, 6, 16, 30, tzinfo=ET)


def _order(ticker, side, order_id, coid, status, price, filled_qty, qty=None):
    return {
        "symbol": ticker,
        "side": side,
        "order_id": order_id,
        "client_order_id": coid,
        "status": status,
        "avg_filled_price": price,
        "filled_quantity": filled_qty,
        "total_quantity": qty if qty is not None else filled_qty,
        "filled_time": "2026-10-06T10:18:00-04:00",
    }


class Spy:
    def __init__(self, positions=None, by_date=None, open_orders=None,
                 host=PAPER_HOST, env="paper", connect=True, connected=True):
        self.host = host
        self.env = env
        self.err = "not connected"
        self.positions = positions or {}
        self.by_date = {} if by_date is None else by_date
        self.open_orders = open_orders or []
        self._connect = connect
        self._connected = connected
        self.calls = []
        self.placed = False

    def connect(self):
        self.calls.append("connect")
        return self._connect

    def snapshot(self):
        self.calls.append("snapshot")
        return type("Snap", (), {
            "connected": self._connected,
            "positions": self.positions,
            "error": "" if self._connected else "down",
        })()

    def list_open_orders(self):
        self.calls.append("list_open_orders")
        return list(self.open_orders)

    def list_filled_orders(self, day):
        self.calls.append("list_filled_orders")
        got = self.by_date.get(day, [])
        if isinstance(got, Exception):
            raise got
        return list(got)

    def place(self, *args, **kwargs):
        self.placed = True
        raise AssertionError("place")

    def place_batch(self, *args, **kwargs):
        self.placed = True
        raise AssertionError("place_batch")

    def cancel_order(self, *args, **kwargs):
        self.placed = True
        raise AssertionError("cancel_order")

    def modify_order(self, *args, **kwargs):
        self.placed = True
        raise AssertionError("modify_order")

    def place_order(self, *args, **kwargs):
        self.placed = True
        raise AssertionError("place_order")


def _filled_book():
    return {
        "2026-10-06": [
            _order("PACB", "BUY", "3IG4QHJ4IC309B8FJNLRCJD0GA",
                   "fs20261006BPACB", "FILLED", 2.5, 1113),
            _order("DNA", "BUY", "3UHAD2KIHSSID8SO8KDQLQKR09",
                   "fs20261006BDNA", "FILLED", 15.0, 214),
            _order("QSI", "BUY", "HUIIVGPG79II8OCHIG4IDT7818",
                   "fs20261006BQSI", "FILLED", 1.25, 2482),
            _order("GLND", "SELL", "NJJO7E6QDTMI40CLS4EGQ4KRL8",
                   "fs20261006SGLND", "FILLED", 4.0, 633),
            _order("NAUT", "SELL", "1GIIVILVRVQI877IGQUS6D5ASA",
                   "fs20261006SNAUT", "FILLED", 2.0, 1668),
        ],
    }


def test_freeze_window_matches_the_book_write_close():
    assert snap.in_write_freeze(datetime(2026, 10, 6, 3, 0, tzinfo=ET))
    assert snap.in_write_freeze(datetime(2026, 10, 6, 8, 15, tzinfo=ET))
    assert snap.in_write_freeze(datetime(2026, 10, 6, 9, 39, tzinfo=ET))
    assert not snap.in_write_freeze(datetime(2026, 10, 6, 2, 59, tzinfo=ET))
    assert not snap.in_write_freeze(datetime(2026, 10, 6, 9, 40, tzinfo=ET))
    assert not snap.in_write_freeze(datetime(2026, 10, 6, 16, 25, tzinfo=ET))


def test_freeze_does_not_read_or_write(tmp_path):
    log = tmp_path / "drift_log.jsonl"
    log.write_text('{"kind":"doc"}\n', encoding="utf-8")
    before = log.read_bytes()
    spy = Spy()
    frozen = datetime(2026, 10, 6, 8, 0, tzinfo=ET)
    result = snap.run_snapshot(api=spy, clock=frozen, path=log)
    assert result["wrote"] is False
    assert result["frozen"] is True
    assert spy.calls == []
    assert spy.placed is False
    assert log.read_bytes() == before


def test_live_host_is_refused_before_connect(tmp_path):
    log = tmp_path / "drift_log.jsonl"
    log.write_text("{}\n", encoding="utf-8")
    before = log.read_bytes()
    spy = Spy(host="api.webull.com", env="real")
    with pytest.raises(snap.SandboxRefused, match="sandbox"):
        snap.run_snapshot(api=spy, clock=OUTSIDE, path=log)
    assert spy.calls == []
    assert spy.placed is False
    assert log.read_bytes() == before


def test_sandbox_label_is_required_even_if_env_says_paper():
    spy = Spy(host="api.webull.com", env="paper")
    with pytest.raises(snap.SandboxRefused):
        snap.require_sandbox(spy)


def test_readonly_guard_refuses_order_methods_without_calling_them():
    spy = Spy()
    guard = snap.ReadOnlyPaper(spy)
    for name in snap.FORBIDDEN_CALLS:
        with pytest.raises(snap.SnapshotRefused, match="refuses"):
            getattr(guard, name)()
    assert spy.placed is False
    assert spy.calls == []
    assert snap.ALLOWED_CALLS.isdisjoint(snap.FORBIDDEN_CALLS)


def test_source_does_not_call_order_methods():
    src = Path(snap.__file__).read_text(encoding="utf-8")
    tree = ast.parse(src)
    constants = []
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            assert node.name not in snap.FORBIDDEN_CALLS
        if isinstance(node, ast.Attribute):
            assert node.attr not in snap.FORBIDDEN_CALLS
        if isinstance(node, ast.Constant) and node.value in snap.FORBIDDEN_CALLS:
            constants.append(node.value)
    assert sorted(constants) == sorted(snap.FORBIDDEN_CALLS)
    assert "import paper_open" not in src
    assert "paper_backstop" not in src
    assert ".write_text(" not in src
    assert "open('w'" not in src
    assert 'open("w"' not in src


def test_send_paths_do_not_grow_a_snapshot_hook():
    for rel in ("src/paper_open.py", "src/paper_backstop.py", "src/webull_exec.py"):
        text = (ROOT / rel).read_text(encoding="utf-8")
        assert "paper_account_snapshot" not in text
        assert "account_snapshot" not in text


def test_default_client_is_the_paper_sandbox():
    api = snap.default_api()
    assert isinstance(api, PaperAPI)
    assert api.host == PAPER_HOST == "api.sandbox.webull.com"
    assert api.env == "paper"


def test_incident_constants_match_the_gap_line():
    lines = REAL_LOG.read_text(encoding="utf-8").splitlines()
    gap = json.loads(lines[1])
    assert gap["kind"] == "gap"
    assert gap["date"] == "2026-10-06"
    assert gap["feam"]["held_shares"] == snap.FEAM_BOOK_SHARES == 939
    assert gap["feam"]["error"] == "OPENAPI_ORDER_NOT_SUPPORT_REVERSE_OPTION"
    assert gap["sdev"]["sealed_book_shares"] == snap.SDEV_BOOK_SHARES == 1222
    assert gap["sdev"]["sealed_entry"] == snap.SDEV_ENTRY
    assert gap["sdev"]["bought"] is False
    got = {(row["ticker"], row["side"], row["order_id"]) for row in snap.incident_orders(gap)}
    want = {
        (row["ticker"], row["side"], row["order_id"]) for row in snap.FALLBACK_ORDERS
    }
    assert got == want
    assert "FEAM" not in {row["ticker"] for row in snap.incident_orders(gap)}


def test_order_dates_keep_the_incident_and_skip_the_weekend():
    days = snap.order_dates(OUTSIDE)
    assert "2026-10-06" in days
    assert "2026-09-30" in days
    assert "2026-10-03" not in days
    assert "2026-10-04" not in days
    later = snap.order_dates(datetime(2027, 4, 6, 16, 0, tzinfo=ET))
    assert later[0] == "2026-10-06" or "2026-10-06" in later
    assert len(later) <= snap.MAX_ORDER_DAYS
    assert "2027-04-06" in later


def test_snapshot_appends_correction_and_does_not_place(tmp_path):
    raw = REAL_LOG.read_bytes()
    log = tmp_path / "drift_log.jsonl"
    log.write_bytes(raw)
    positions = {
        "PACB": {"shares": 1113, "cost_px": 2.5},
        "DNA": {"shares": 214, "cost_px": 15.0},
        "QSI": {"shares": 2482, "cost_px": 1.25},
    }
    spy = Spy(positions=positions, by_date=_filled_book())
    result = snap.run_snapshot(api=spy, clock=OUTSIDE, path=log)
    assert result["wrote"] is True
    assert spy.placed is False
    assert set(spy.calls) <= {"connect", "snapshot", "list_open_orders", "list_filled_orders"}
    assert "place" not in spy.calls
    assert "cancel_order" not in spy.calls
    got = log.read_bytes()
    assert got.startswith(raw)
    assert REAL_LOG.read_bytes() == raw
    new = [json.loads(line) for line in got.decode().splitlines()[2:]]
    assert [row["kind"] for row in new] == ["account_snapshot", "correction"]
    account = new[0]
    assert account["host"] == "api.sandbox.webull.com"
    assert account["timestamp"].startswith("2026-10-06T16:30:00")
    assert account["orders_sent"] is False
    listed = {row["ticker"]: row for row in account["positions"]}
    assert listed["PACB"] == {"ticker": "PACB", "qty": 1113, "avg_cost": 2.5}
    assert "FEAM" not in listed
    assert "SDEV" not in listed
    assert any(row["ticker"] == "GLND" and row["status"] == "FILLED" for row in account["orders"])
    correction = new[1]
    assert correction["host"] == "api.sandbox.webull.com"
    assert "held_shares 939" in correction["statement"]
    assert "sealed book's share count" in correction["statement"]
    assert "not the account's" in correction["statement"]
    assert correction["feam"]["held"] is False
    assert correction["feam"]["qty"] == 0
    assert correction["feam"]["sealed_book_shares"] == 939
    assert correction["sdev"]["held"] is False
    assert correction["sdev"]["qty"] == 0
    assert correction["sdev"]["sealed_book_shares"] == 1222
    assert correction["sdev"]["sealed_entry"] == "2026-09-30"
    assert correction["catch_up_order"] is False
    assert "no catch-up order" in correction["note"]
    prices = {name: row["price"] for name, row in correction["fills"].items()}
    assert prices == {"PACB": 2.5, "DNA": 15.0, "QSI": 1.25, "GLND": 4.0, "NAUT": 2.0}
    assert correction["fills"]["PACB"]["side"] == "BUY"
    assert correction["fills"]["PACB"]["filled"] is True
    assert correction["fills"]["GLND"]["side"] == "SELL"
    assert correction["fills"]["NAUT"]["filled"] is True
    assert "FEAM" not in correction["fills"]
    # A second snapshot appends again and leaves the gap and the first pair.
    again = snap.run_snapshot(api=spy, clock=OUTSIDE, path=log)
    assert again["wrote"] is True
    final = log.read_bytes()
    assert final.startswith(got)
    assert json.loads(final.decode().splitlines()[1])["feam"]["held_shares"] == 939
    assert REAL_LOG.read_bytes() == raw


def test_actual_holdings_are_not_replaced_by_the_book_count(tmp_path):
    """939 stays the book's figure even when the account also shows a qty."""
    log = tmp_path / "drift_log.jsonl"
    log.write_bytes(REAL_LOG.read_bytes())
    positions = {
        "FEAM": {"shares": 10, "cost_px": 3.5},
        "SDEV": {"shares": 50, "cost_px": 4.0},
    }
    by_date = {
        "2026-10-06": [
            _order("PACB", "BUY", "3IG4QHJ4IC309B8FJNLRCJD0GA",
                   "fs20261006BPACB", "FILLED", 2.5, 1113),
            _order("DNA", "BUY", "3UHAD2KIHSSID8SO8KDQLQKR09",
                   "fs20261006BDNA", "SUBMITTED", None, 0, qty=214),
            _order("GLND", "SELL", "NJJO7E6QDTMI40CLS4EGQ4KRL8",
                   "fs20261006SGLND", "REJECTED", 9.0, 0, qty=633),
            _order("NAUT", "SELL", "1GIIVILVRVQI877IGQUS6D5ASA",
                   "fs20261006SNAUT", "FILLED", 2.0, 100, qty=1668),
        ],
    }
    spy = Spy(positions=positions, by_date=by_date)
    result = snap.run_snapshot(api=spy, clock=OUTSIDE, path=log)
    correction = result["correction"]
    assert correction["feam"]["held"] is True
    assert correction["feam"]["qty"] == 10
    assert correction["feam"]["avg_cost"] == 3.5
    assert "does not hold FEAM" not in correction["note"]
    assert "holds FEAM (10 shares)" in correction["note"]
    assert correction["sdev"]["held"] is True
    assert correction["sdev"]["qty"] == 50
    assert correction["sdev"]["sealed_book_shares"] == 1222
    assert correction["fills"]["DNA"]["filled"] is False
    assert correction["fills"]["DNA"]["price"] is None
    assert correction["fills"]["GLND"]["filled"] is False
    assert correction["fills"]["GLND"]["status"] == "REJECTED"
    assert correction["fills"]["GLND"]["price"] is None
    assert correction["fills"]["NAUT"]["filled"] is True
    assert correction["fills"]["NAUT"]["price"] == 2.0
    assert correction["fills"]["NAUT"]["filled_qty"] == 100
    assert correction["fills"]["QSI"]["filled"] is False
    assert correction["fills"]["QSI"]["status"] == "not_on_book"
    assert "held_shares 939" in correction["statement"]
    assert spy.placed is False


def test_failed_history_does_not_invent_a_missed_fill(tmp_path):
    log = tmp_path / "drift_log.jsonl"
    log.write_text('{"kind":"doc"}\n', encoding="utf-8")
    spy = Spy(
        positions={},
        by_date={"2026-10-06": RuntimeError("history down")},
        open_orders=[],
    )
    result = snap.run_snapshot(api=spy, clock=OUTSIDE, path=log)
    correction = result["correction"]
    for row in correction["fills"].values():
        assert row["filled"] is None
        assert row["status"] == "not_observed"
        assert row["price"] is None
    assert "held_shares 939" in correction["statement"]
    assert correction["feam"]["held"] is False
    assert correction["sdev"]["held"] is False


def test_failed_position_read_writes_nothing(tmp_path):
    log = tmp_path / "drift_log.jsonl"
    before = b'{"kind":"doc"}\n'
    log.write_bytes(before)
    spy = Spy(connected=False)
    with pytest.raises(RuntimeError, match="position snapshot"):
        snap.run_snapshot(api=spy, clock=OUTSIDE, path=log)
    assert log.read_bytes() == before
    assert spy.placed is False


def test_workflow_is_manual_ubuntu_sandbox_and_readonly():
    text = WF.read_text(encoding="utf-8")
    paper = (ROOT / ".github" / "workflows" / "webull_paper.yml").read_text(encoding="utf-8")
    install = (ROOT / ".github" / "workflows" / "install_paper_open.yml").read_text(
        encoding="utf-8")
    head = text.split("\njobs:", 1)[0]
    assert "workflow_dispatch:" in head
    assert "schedule:" not in head
    assert "workflow_run:" not in head
    assert "pull_request:" not in head
    assert "push:" not in head
    assert "runs-on: ubuntu-latest" in text
    assert "runs-on: ubuntu-latest" in paper
    assert "runs-on: [self-hosted, ecs]" in install
    assert "runs-on: [self-hosted, ecs]" not in text
    for key in ("WEBULL_APP_KEY", "WEBULL_APP_SECRET", "WEBULL_ACCOUNT_ID", "GITHUB_TOKEN"):
        assert "secrets." + key in text
        assert "secrets." + key in paper
    assert "api.sandbox.webull.com" in text
    assert "api.webull.com" not in text
    assert "python -m src.paper_account_snapshot" in text
    assert "src.paper_open" not in text
    assert "src.paper_backstop" not in text
    assert "--submit" not in text
    assert "--ready" not in text
    assert "data/paper_open/drift_log.jsonl" in text
    assert "scripts/safe_git_push.sh" in text
    assert '-ge 300' in text
    assert '-lt 940' in text
    for name in (
        "place_order", "place_batch", "cancel_order", "modify_order",
        "replace_order", "amend_order",
    ):
        assert name not in text
