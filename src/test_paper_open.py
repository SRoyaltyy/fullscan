from datetime import datetime, timedelta
import json
from unittest.mock import patch
import pytest
from . import paper_open as po, webull_exec as we
from .futubull_exec import BrokerSnap

DATE = '2026-09-17'
BELL = datetime.fromisoformat(DATE+'T09:30:00-04:00')


def payload():
    return {'date': DATE, 'decision_readiness': {'ready': True, 'fingerprint': 'abc',
        'completed_at': (BELL-timedelta(minutes=2)).isoformat()},
        'strategies': {we.HOT4: {'date': DATE, 'status': 'ok', 's': 1,
            'buy': [{'ticker': 'ABC', 'px': 10, 'side': 'long'}]}}}


class API:
    host = we.PAPER_HOST
    err = None
    def __init__(self): self.calls = []
    def connect(self): return True
    def snapshot(self): return BrokerSnap(env='paper', cash=1000, positions={}, connected=True)
    def place_batch(self, tickets):
        self.calls.append(tickets)
        return {we.client_order_id(t['date'], t['side'], t['ticker']):
                {'ok': True, 'order_id': 'broker-'+t['ticker']} for t in tickets}


def plan():
    return po.make_plan(payload(), API().snapshot(), BELL-timedelta(seconds=15))


def test_empty_valid_selection_does_not_reconstruct_winners():
    p = payload(); p['strategies'][we.HOT4]['buy'] = []
    with patch('src.combo_broker.resolve_rows', side_effect=AssertionError('must not backfill')):
        assert not po.make_plan(p, API().snapshot(), BELL-timedelta(seconds=15))['card']['tickets']


@pytest.mark.parametrize('case', ['wrong_date', 'late', 'naive', 'missing_inputs', 'missing_score', 'error'])
def test_invalid_decisions_never_arm(case):
    p = payload()
    if case == 'wrong_date': p['date'] = '2026-09-16'
    if case == 'late': p['decision_readiness']['completed_at'] = (BELL+timedelta(seconds=1)).isoformat()
    if case == 'naive': p['decision_readiness']['completed_at'] = DATE+'T09:00:00'
    if case == 'missing_inputs': p['decision_readiness']['ready'] = False
    if case == 'missing_score': p['strategies'][we.HOT4]['s'] = None
    if case == 'error': p['strategies'][we.HOT4]['status'] = 'look_stale'
    with pytest.raises((ValueError, TypeError)):
        po.make_plan(p, API().snapshot(), BELL-timedelta(seconds=15))


def test_batch_release_and_restart_do_not_double_submit(tmp_path):
    api = API(); journal = tmp_path/'attempt.json'
    result = po.release(plan(), api, lambda: BELL, journal, submit=True)
    assert len(api.calls) == 1
    assert result['status'] == 'acknowledged'
    assert result['fill_status'] == 'not_observed'
    with pytest.raises(ValueError, match='already attempted'):
        po.release(plan(), api, lambda: BELL, journal, submit=True)
    assert len(api.calls) == 1


@pytest.mark.parametrize('offset', [-1, 3, 3600])
def test_no_early_or_late_submission(tmp_path, offset):
    api = API()
    with pytest.raises(ValueError, match='outside'):
        po.release(plan(), api, lambda: BELL+timedelta(seconds=offset), tmp_path/'x', submit=True)
    assert not api.calls


def test_unknown_batch_outcome_is_durable(tmp_path):
    api = API()
    api.place_batch = lambda _: (_ for _ in ()).throw(TimeoutError())
    result = po.release(plan(), api, lambda: BELL, tmp_path/'x', submit=True)
    assert result['status'] == 'failed'
    assert json.loads((tmp_path/'x').read_text())['sent'][0]['status'] == 'unknown'


def test_warm_worker_releases_without_network_preparation_at_bell(tmp_path):
    t = [BELL-timedelta(seconds=35)]
    def clock(): return t[0]
    def sleep(seconds): t[0] += timedelta(seconds=seconds)
    calls = []
    def loader(date):
        calls.append(clock())
        assert clock() < BELL-timedelta(seconds=5)
        return payload()
    api = API()
    with patch.object(we, 'write_last'):
        assert po.run(submit=True, clock=clock, sleep=sleep, loader=loader,
                      api=api, state_dir=tmp_path) == 0
    assert t[0] == BELL
    assert len(api.calls) == 1


def test_zero_cash_is_blocked_not_fake_success():
    snap = BrokerSnap(env='paper', cash=0, positions={}, connected=True)
    with pytest.raises(ValueError, match='fund/price'):
        po.make_plan(payload(), snap, BELL-timedelta(seconds=15))


def test_unknown_balance_not_silently_zero():
    with pytest.raises(ValueError): we.parse_balance({'data': {'unrecognized': 10000}})
    assert we.parse_balance({'data': {'currency_assets': [
        {'currency': 'HKD', 'cash_balance': '99'},
        {'currency': 'USD', 'cash_balance': '1000'}]}}) == (1000, 1000)


def test_http_200_business_rejection_is_not_acceptance():
    from types import SimpleNamespace
    api = we.PaperAPI()
    api.account_id = 'paper-test'
    api.trade = SimpleNamespace(order_v3=SimpleNamespace(place_order=lambda *args:
        {'code': 'ERROR', 'data': [{'client_order_id': 'x', 'order_id': 'fake'}]}))
    assert not api.place_batch([{'ticker': 'ABC', 'side': 'BUY', 'shares': 1, 'date': DATE}])['x']['ok']


def test_slow_journal_cannot_allow_a_late_send(tmp_path):
    api = API()
    times = iter([BELL, BELL, BELL+timedelta(seconds=3)])
    result = po.release(plan(), api, lambda: next(times), tmp_path/'x', submit=True)
    assert not api.calls
    assert result['status'] == 'failed'
    assert result['sent'][0]['status'] == 'missed_deadline'


def test_later_fallback_schedule_preserves_first_attempt(tmp_path):
    original = {'date':DATE, 'status':'acknowledged', 'sent':[{'ticker':'ABC'}]}
    for suffix in ('submit','status'):
        (tmp_path/f'{DATE}_{suffix}.json').write_text(json.dumps(original))
    api=API()
    assert po.run(submit=True,clock=lambda:BELL+timedelta(minutes=10),api=api,state_dir=tmp_path)==0
    assert not api.calls
    assert json.loads((tmp_path/f'{DATE}_status.json').read_text())==original
