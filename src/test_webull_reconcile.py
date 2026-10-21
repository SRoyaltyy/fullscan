"""Run: python -m pytest -q src/test_webull_reconcile.py"""
from datetime import datetime
from types import SimpleNamespace
import json
import pytest
from src import webull_reconcile as r
from src import webull_exec as we

BOOK = {"KOD": 27, "NAUT": 1234, "PENG": 34, "SDEV": 1222}
LIVE = {"KOD": 31, "NAUT": 1398, "CIEN": 1, "DNA": 18, "FEAM": 920, "OPCH": 13, "PACB": 124, "PENG": 33}


def test_known_drift_orders():
    got = [(o["side"], o["ticker"], o["shares"]) for o in r.plan_orders(BOOK, LIVE)]
    assert got == [("SELL", "CIEN", 1), ("SELL", "DNA", 18), ("SELL", "FEAM", 920), ("SELL", "KOD", 4),
                   ("SELL", "NAUT", 164), ("SELL", "OPCH", 13), ("SELL", "PACB", 124),
                   ("BUY", "PENG", 1), ("BUY", "SDEV", 1222)]
    assert all(o["side"] == "SELL" for o in r.plan_orders(BOOK, LIVE, buys=False))


def test_window():
    et = r.ET
    assert not r.submit_window_ok(datetime(2026, 10, 21, 9, 39, tzinfo=et))
    assert r.submit_window_ok(datetime(2026, 10, 21, 9, 40, tzinfo=et))
    assert not r.submit_window_ok(datetime(2026, 10, 24, 11, 0, tzinfo=et))


class Api:
    host = we.PAPER_HOST
    trade = object()

    def __init__(self, ok=True):
        self.ok = ok

    def snapshot(self):
        return SimpleNamespace(connected=self.ok, positions={k: {"shares": v} for k, v in LIVE.items()})


def _recs(monkeypatch):
    monkeypatch.setattr(r, "book_holdings", lambda records=None: (dict(BOOK), "mark:2026-10-08"))


def test_dry_run_notes_each_order_add_only(tmp_path, monkeypatch):
    _recs(monkeypatch)
    log = tmp_path / "l.jsonl"
    log.write_text('{"old":1}\n')
    orders = r.run(api=Api(), now=datetime(2026, 10, 21, 20, 0, tzinfo=r.ET), log=log)
    lines = log.read_text().splitlines()
    assert lines[0] == '{"old":1}' and len(lines) == 1 + len(orders) == 10
    assert json.loads(lines[1])["submitted"] is False


def test_failed_read_fails_closed(tmp_path, monkeypatch):
    _recs(monkeypatch)
    with pytest.raises(r.ReconcileError):
        r.run(api=Api(ok=False), now=datetime(2026, 10, 21, 10, 0, tzinfo=r.ET), log=tmp_path / "l")


def test_submit_outside_window_refused(tmp_path, monkeypatch):
    _recs(monkeypatch)
    with pytest.raises(r.ReconcileError):
        r.run(submit=True, api=Api(), now=datetime(2026, 10, 21, 9, 0, tzinfo=r.ET), log=tmp_path / "l")


def test_rc_ids_differ_from_h1():
    assert we.client_order_id("2026-10-21", "SELL", "KOD", strategy="rc") != we.client_order_id("2026-10-21", "SELL", "KOD")


def test_real_book_is_readable():
    book, src = r.book_holdings()
    assert book and src
