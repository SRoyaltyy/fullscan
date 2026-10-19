"""Retry-chain gate for the Webull paper seal send (2026-10-19)."""
import json
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from src import webull_seal_gate as g

ET = ZoneInfo('America/New_York')
ROOT = Path(__file__).resolve().parent.parent
FIX = ROOT / 'src/fixtures/tickets_2026-10-16_ready.json'


def _root(tmp_path, *, plan_date='2026-10-16', journal=False, status=None):
    log = tmp_path / g.H1_LOG
    log.parent.mkdir(parents=True)
    log.write_text(json.dumps({'kind': 'plan', 'date': plan_date}) + '\n')
    po = tmp_path / 'data/paper_open'
    po.mkdir(parents=True)
    if journal:
        (po / '2026-10-16_submit.json').write_text('{}')
    if status is not None:
        (po / '2026-10-16_status.json').write_text(json.dumps(status))
    return tmp_path


def no_remote(path):
    return None


def at(h, m):
    return datetime(2026, 10, 16, h, m, tzinfo=ET)


def quiet(*a, **k):
    return None


def test_locked_set_matches_paper_open():
    from src import paper_open
    assert g.LOCKED_STATUSES == paper_open.LOCKED_STATUSES


def test_gate_go_on_sealed_day_before_cutoff(tmp_path):
    ok, why = g.gate(at(8, 5), _root(tmp_path), no_remote)
    assert ok, why


@pytest.mark.parametrize('h,m', [(9, 25), (9, 30), (6, 59)])
def test_gate_outside_window_is_no_go(tmp_path, h, m):
    assert g.gate(at(h, m), _root(tmp_path), no_remote)[0] is False


def test_gate_no_sealed_plan_is_no_go(tmp_path):
    ok, why = g.gate(at(8, 5), _root(tmp_path, plan_date='2026-10-15'), no_remote)
    assert not ok and 'no sealed h1 plan' in why


def test_gate_weekend_no_go(tmp_path):
    assert g.gate(datetime(2026, 10, 17, 8, 5, tzinfo=ET), _root(tmp_path), no_remote)[0] is False


def test_gate_refuses_after_journal_local_or_remote(tmp_path):
    assert g.gate(at(8, 5), _root(tmp_path / 'a', journal=True), no_remote)[0] is False
    remote = lambda p: {} if p.endswith('_submit.json') else None
    ok, why = g.gate(at(8, 5), _root(tmp_path / 'b'), remote)
    assert not ok and 'on main' in why


def test_gate_blocked_status_is_retryable_locked_is_not(tmp_path):
    blocked = {'status': 'blocked', 'error': 'required inputs are not validated'}
    assert g.gate(at(8, 5), _root(tmp_path / 'blk', status=blocked), no_remote)[0] is True
    for st in ('missed_deadline', 'acknowledged', 'failed', 'releasing'):
        r = _root(tmp_path / st, status={'status': st})
        assert g.gate(at(8, 5), r, no_remote)[0] is False


def test_wait_polls_until_ready_then_go(tmp_path):
    root = _root(tmp_path)
    ready = json.loads(FIX.read_text())
    not_ready = {**ready, 'decision_readiness': {'ready': False}}
    seq = [not_ready, not_ready, ready]
    t = [at(8, 5)]
    slept = []

    def remote(path):
        if 'strategy_tickets' in path:
            return seq.pop(0)
        return None

    def sleep(s):
        slept.append(s)
        t[0] = t[0].replace(minute=t[0].minute + 2)

    ok, why = g.wait_ready(clock=lambda: t[0], sleep=sleep, root=root, remote=remote,
                           interval=120, log=quiet)
    assert ok and 'ticket ready' in why
    assert slept == [120, 120]


def test_wait_stops_at_cutoff_without_send(tmp_path):
    root = _root(tmp_path)
    t = [at(9, 20)]

    def sleep(s):
        t[0] = t[0].replace(minute=t[0].minute + 2)

    remote = lambda p: ({'date': '2026-10-16', 'decision_readiness': {'ready': False}}
                        if 'strategy_tickets' in p else None)
    ok, why = g.wait_ready(clock=lambda: t[0], sleep=sleep, root=root, remote=remote, log=quiet)
    assert not ok and '09:25' in why


def test_wait_stops_if_other_sender_journaled(tmp_path):
    root = _root(tmp_path)
    calls = {'n': 0}

    def remote(p):
        if p.endswith('_submit.json'):
            calls['n'] += 1
            return {} if calls['n'] > 1 else None
        if 'strategy_tickets' in p:
            return {'date': '2026-10-16', 'decision_readiness': {'ready': False}}
        return None

    ok, why = g.wait_ready(clock=lambda: at(8, 10), sleep=lambda s: None, root=root,
                           remote=remote, log=quiet)
    assert not ok and 'journal' in why


class _NoSubmitAPI:
    err = None
    def connect(self): return True
    def snapshot(self):
        from src.webull_exec import BrokerSnap
        return BrokerSnap(env='paper', cash=100000, positions={}, connected=True)
    def list_session_orders(self, date): return []
    def place_batch(self, tickets): raise AssertionError('submit attempted')


def test_dry_run_on_sealed_2026_10_16_plan_never_submits(tmp_path, monkeypatch):
    """Real sealed 10-16 h1 plan + the real ready ticket, up to just before submit."""
    from src import paper_open as po, webull_exec as we
    monkeypatch.setenv('PAPER_OPEN_CHECK_REMOTE', '0')
    api = _NoSubmitAPI()
    api.host = we.PAPER_HOST
    rc = po.submit_ready(submit=False, clock=lambda: at(8, 50), api=api,
                         state_dir=tmp_path, payload=json.loads(FIX.read_text()))
    assert rc == 0
    status = json.loads((tmp_path / '2026-10-16_status.json').read_text())
    assert status['status'] == 'dry_run' and status.get('submit') is False
    buys = sorted(t['ticker'] for t in (status.get('tickets') or status['card']['tickets']) if t['side'] == 'BUY')
    assert buys == ['BBWI', 'INDP', 'QTEX', 'XP']
    assert not (tmp_path / '2026-10-16_submit.json').exists()


def test_send_fails_closed_when_ticket_not_ready(tmp_path, monkeypatch):
    from src import paper_open as po, webull_exec as we
    monkeypatch.setenv('PAPER_OPEN_CHECK_REMOTE', '0')
    payload = json.loads(FIX.read_text())
    payload['decision_readiness'] = {'ready': False}
    api = _NoSubmitAPI()
    api.host = we.PAPER_HOST
    rc = po.submit_ready(submit=True, clock=lambda: at(8, 50), api=api,
                         state_dir=tmp_path, payload=payload)
    assert rc == 2
    st = json.loads((tmp_path / '2026-10-16_status.json').read_text())
    assert st['status'] == 'blocked' and 'not validated' in st['error']
