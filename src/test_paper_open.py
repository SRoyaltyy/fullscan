from datetime import datetime, timedelta
import json
from unittest.mock import patch
import pytest
from . import paper_open as po, webull_exec as we
from .futubull_exec import BrokerSnap


def _legacy_payload_plan(date, snap, payload=None, panel=None):
    """Journal tests still build a card from the synthetic payload.

    Production ``plan_hot4_for_broker`` reads the sealed h1 plan. Tests
    that are not in the sealed set below keep this stand-in so the clock
    and journal cases stay on ticker ABC.
    """
    del panel
    published = {}
    if isinstance(payload, dict):
        published = (payload.get("strategies") or {}).get(we.HOT4) or {}
    buys = list(published.get("buy") or [])
    sells = list(published.get("sell") or [])
    positions = getattr(snap, "positions", None) or {}
    cash = max(float(getattr(snap, "cash", 0) or 0), 0.0)
    sell_tickets, sell_skips = we.size_hot4_sells(
        sells, positions=positions, date=date)
    buy_tickets, buy_skips = we.size_hot4_tickets(
        buys, cash=cash, held=set(positions), date=date,
        s=published.get("s"), sit=bool(published.get("sit")))
    return {
        "date": date,
        "want_date": date,
        "policy": we.HOT4,
        "stale": False,
        "look_error": "",
        "hard_red": False,
        "tickets": sell_tickets + buy_tickets,
        "skipped": sell_skips + buy_skips,
        "order_type": "MARKET",
        "why": "synthetic payload",
        "score": published.get("s"),
        "source": "test_payload",
    }


_SEALED_PLAN_TESTS = {
    "test_submit_ignores_hot4_buys_that_diverge_from_sealed_book",
    "test_submit_ignores_hot4_sells_that_diverge_from_sealed_book",
    "test_plan_sells_sealed_shares_before_buys",
    "test_plan_sends_sealed_sell_even_when_paper_does_not_hold_it",
    "test_empty_payload_does_not_rebuild_hot4",
    "test_sealed_cash_short_fails_closed_not_hot4",
    "test_ready_submit_flat_sit_still_sends_sealed_plan",
    "test_flat_sit_does_not_hide_sealed_plan",
    "test_account_positions_do_not_fail_closed_against_sealed_tickets",
    "test_unheld_sealed_sell_is_drift_skipped_and_not_sent",
    "test_2026_10_07_from_drifted_1006_account_sends_only_that_days_tickets",
}


@pytest.fixture(autouse=True)
def synthetic_hot4_matches_payload(monkeypatch, request):
    """Clock and journal tests keep a payload stand-in.

    Sealed-book tests call the real planner.
    """
    if request.node.name in _SEALED_PLAN_TESTS:
        return

    def _accept(date, buys, sells=None, panel=None):
        return [
            str(b.get("ticker") or "").upper()
            for b in (buys or [])
            if isinstance(b, dict) and b.get("ticker")
        ]

    monkeypatch.setattr("src.strategy_tickets.assert_hot4_wire", _accept)
    monkeypatch.setattr(we, "plan_hot4_for_broker", _legacy_payload_plan)

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
    def list_session_orders(self, date):
        return []
    def place_batch(self, tickets):
        self.calls.append(tickets)
        return {we.client_order_id(t['date'], t['side'], t['ticker']):
                {'ok': True, 'order_id': 'broker-'+t['ticker']} for t in tickets}


def plan():
    return po.make_plan(payload(), API().snapshot(), BELL-timedelta(seconds=15))


def _sealed_day_payload(day='2026-10-02'):
    p = payload()
    p['date'] = day
    p['decision_readiness']['completed_at'] = day + 'T09:28:00-04:00'
    hot = p['strategies'][we.HOT4]
    hot['date'] = day
    hot['buy'] = [
        {'ticker': t, 'side': 'long', 'px': 10}
        for t in ('DELL', 'GME', 'UMC', 'VSTS')
    ]
    hot['sell'] = [{'ticker': 'FEAM', 'side': 'long', 'px': 4}]
    return p


def _sealed_clock(day='2026-10-02'):
    return datetime.fromisoformat(day + 'T09:29:45-04:00')


def _rich_snap(**positions):
    return BrokerSnap(
        env='paper', cash=20_000, positions=positions, connected=True)


def _boom_pick_day(monkeypatch):
    monkeypatch.setattr(
        'src.factor_mine.pick_day',
        lambda *a, **k: (_ for _ in ()).throw(AssertionError('pick_day')),
    )


def test_submit_ignores_hot4_buys_that_diverge_from_sealed_book(monkeypatch):
    _boom_pick_day(monkeypatch)
    monkeypatch.setattr(
        'src.strategy_tickets.hot4_recipe_tickers',
        lambda date, panel=None: ['FEAM', 'TJGC', 'LVWR', 'SECZ'],
    )
    card = po.make_plan(
        _sealed_day_payload(), _rich_snap(), _sealed_clock())['card']
    buys = [t['ticker'] for t in card['tickets'] if t['side'] == 'BUY']
    assert buys == ['QSI', 'TJGC']
    assert 'DELL' not in buys
    assert 'FEAM' not in buys


def test_submit_ignores_hot4_sells_that_diverge_from_sealed_book(monkeypatch):
    _boom_pick_day(monkeypatch)
    monkeypatch.setattr(
        'src.strategy_tickets.hot4_recipe_sells',
        lambda date, panel=None, **_kw: ['FEAM', 'TJGC', 'LVWR'],
    )
    card = po.make_plan(
        _sealed_day_payload(), _rich_snap(), _sealed_clock())['card']
    sells = [(t['ticker'], t['shares']) for t in card['tickets'] if t['side'] == 'SELL']
    assert sells == [('EGG', 774), ('KOD', 33)]
    assert card['tickets'][0]['side'] == 'SELL'


def test_plan_sells_sealed_shares_before_buys(monkeypatch):
    _boom_pick_day(monkeypatch)
    snap = _rich_snap(FEAM={'shares': 12, 'last_px': 4})
    card = po.make_plan(_sealed_day_payload(), snap, _sealed_clock())['card']
    assert card['tickets'][0]['side'] == 'SELL'
    sells = [(t['ticker'], t['shares']) for t in card['tickets'] if t['side'] == 'SELL']
    buys = [t['ticker'] for t in card['tickets'] if t['side'] == 'BUY']
    assert sells == [('EGG', 774), ('KOD', 33)]
    assert buys == ['QSI', 'TJGC']
    assert 'FEAM' not in {t['ticker'] for t in card['tickets']}


def test_plan_sends_sealed_sell_even_when_paper_does_not_hold_it(monkeypatch):
    _boom_pick_day(monkeypatch)
    card = po.make_plan(
        _sealed_day_payload(), _rich_snap(), _sealed_clock())['card']
    sells = [(t['ticker'], t['shares']) for t in card['tickets'] if t['side'] == 'SELL']
    assert sells == [('EGG', 774), ('KOD', 33)]


def test_empty_payload_does_not_rebuild_hot4(monkeypatch):
    _boom_pick_day(monkeypatch)
    p = _sealed_day_payload()
    p['strategies'][we.HOT4]['buy'] = []
    p['strategies'][we.HOT4]['sell'] = []
    with patch('src.combo_broker.resolve_rows', side_effect=AssertionError('must not backfill')):
        card = po.make_plan(p, _rich_snap(), _sealed_clock())['card']
    assert [t['ticker'] for t in card['tickets'] if t['side'] == 'BUY'] == [
        'QSI', 'TJGC']


def test_sealed_cash_short_fails_closed_not_hot4(monkeypatch):
    _boom_pick_day(monkeypatch)
    snap = BrokerSnap(env='paper', cash=1000, positions={}, connected=True)
    with pytest.raises(ValueError, match='not rebuilding HOT4'):
        po.make_plan(_sealed_day_payload(), snap, _sealed_clock())


def _flat_sit_payload(day=DATE):
    """HOT4 sitting with no legs and no score. Matches a no-panel morning."""
    p = payload()
    p['date'] = day
    p['decision_readiness']['completed_at'] = day + 'T06:00:00-04:00'
    hot = p['strategies'][we.HOT4]
    hot['date'] = day
    hot['status'] = 'sit'
    hot['s'] = None
    hot['buy'] = []
    hot['sell'] = []
    hot['sit'] = False
    hot['note'] = f'no same-day panel rows for {day} — sitting, no live lookup'
    p['look'] = {'stale': False, 'source': 'no_same_day_panel'}
    return p


def test_sit_with_orders_still_requires_finite_score():
    p = payload()
    p['strategies'][we.HOT4]['status'] = 'sit'
    p['strategies'][we.HOT4]['s'] = None
    with pytest.raises((ValueError, TypeError)):
        po.validate_payload(p, DATE, BELL - timedelta(seconds=15))


def test_ready_submit_flat_sit_still_sends_sealed_plan(tmp_path, monkeypatch):
    """A Factor Mine sit with s=None does not drop the sealed h1 plan."""
    day = '2026-10-02'
    _boom_pick_day(monkeypatch)
    early = datetime.fromisoformat(day + 'T06:20:00-04:00')

    class Rich(API):
        def snapshot(self):
            return BrokerSnap(
                env='paper', cash=20_000, positions={
                    'EGG': {'shares': 774, 'last_px': 5.05},
                    'KOD': {'shares': 33, 'last_px': 95.41},
                }, connected=True)

    api = Rich()

    def _regime_missing(*_a, **_k):
        raise SystemExit('missing mover_lookback_action.json')

    with patch.object(we, 'write_last'), \
            patch.object(po, 'remote_session_journal', return_value=None), \
            patch('src.sleeve_merge.load_payload', side_effect=_regime_missing), \
            patch('src.factor_mine_book.load_regime', side_effect=_regime_missing):
        rc = po.submit_ready(
            submit=True, clock=lambda: early, loader=lambda _: _flat_sit_payload(day),
            api=api, state_dir=tmp_path)
    assert rc == 0
    assert api.calls
    sent = [(row['side'], row['ticker']) for row in api.calls[0]]
    assert ('SELL', 'EGG') in sent and ('SELL', 'KOD') in sent
    assert [t for side, t in sent if side == 'BUY'] == ['QSI', 'TJGC']
    journal = json.loads((tmp_path / f'{day}_submit.json').read_text())
    assert journal['status'] == 'acknowledged'
    assert journal['standing'] is True
    assert 'NoneType' not in json.dumps(journal)


def test_flat_sit_does_not_hide_sealed_plan(tmp_path, monkeypatch):
    """Short paper cash fails closed. It does not swap in a HOT4 rebuild."""
    day = '2026-10-02'
    _boom_pick_day(monkeypatch)
    monkeypatch.setattr(
        'src.strategy_tickets.hot4_recipe_tickers',
        lambda date, panel=None: ['FEAM'],
    )
    early = datetime.fromisoformat(day + 'T06:20:00-04:00')
    api = API()
    with patch.object(we, 'write_last'), \
            patch.object(po, 'remote_session_journal', return_value=None):
        rc = po.submit_ready(
            submit=True, clock=lambda: early, loader=lambda _: _flat_sit_payload(day),
            api=api, state_dir=tmp_path)
    assert rc == 2
    assert api.calls == []
    status = json.loads((tmp_path / f'{day}_status.json').read_text())
    assert status['status'] == 'blocked'
    assert 'not rebuilding HOT4' in status['error']
    assert 'diverge' not in status['error']


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
    with patch.object(we, 'write_last'), \
            patch.object(po, 'remote_session_journal', return_value=None):
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
    ticket = {'ticker': 'ABC', 'side': 'BUY', 'shares': 1, 'date': DATE}
    coid = we.client_order_id(DATE, 'BUY', 'ABC')
    got = api.place_batch([ticket])
    assert coid in got
    assert not got[coid]['ok']


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
    with patch.object(po, 'remote_session_journal', return_value=None):
        assert po.run(submit=True,clock=lambda:BELL+timedelta(minutes=10),api=api,state_dir=tmp_path)==0
    assert not api.calls
    saved = json.loads((tmp_path/f'{DATE}_status.json').read_text())
    assert saved['status'] == original['status']
    assert saved['sent'] == original['sent']
    assert saved['events'][-1]['submit'] is False
    assert saved['events'][-1]['status'] == 'missed_deadline'


def early_payload():
    p = payload()
    p['decision_readiness']['completed_at'] = DATE + 'T06:00:00-04:00'
    return p


def test_ready_submit_places_standing_orders_before_bell(tmp_path):
    api = API()
    early = datetime.fromisoformat(DATE + 'T06:20:00-04:00')
    with patch.object(we, 'write_last'), \
            patch.object(po, 'remote_session_journal', return_value=None):
        rc = po.submit_ready(submit=True, clock=lambda: early, loader=lambda _: early_payload(),
                             api=api, state_dir=tmp_path)
    assert rc == 0
    assert len(api.calls) == 1
    journal = json.loads((tmp_path / f'{DATE}_submit.json').read_text())
    assert journal['status'] == 'acknowledged'
    assert journal['standing'] is True
    assert journal['fingerprint'] == 'abc'
    assert journal['sent'][0]['client_order_id'] == we.client_order_id(DATE, 'BUY', 'ABC')
    body = we.order_body({'date': DATE, 'side': 'BUY', 'ticker': 'ABC', 'shares': 1})
    assert body['order_type'] == 'MARKET'
    assert body['support_trading_session'] == 'CORE'
    assert body['time_in_force'] == 'DAY'


class Book(API):
    def __init__(self, rows=None, error=None):
        super().__init__()
        self.rows = list(rows or [])
        self.error = error

    def list_session_orders(self, date):
        if self.error:
            raise RuntimeError(self.error)
        return list(self.rows)


def _two_name_payload():
    p = early_payload()
    p['strategies'][we.HOT4]['buy'] = [
        {'ticker': 'SDEV', 'px': 10, 'side': 'long'},
        {'ticker': 'AAA', 'px': 10, 'side': 'long'},
    ]
    return p


def test_orders_already_at_broker_write_already_submitted_without_status_file(tmp_path):
    """A crash after the place, before any status file, must not send again."""
    early = datetime.fromisoformat(DATE + 'T08:41:00-04:00')
    body = _two_name_payload()
    coid_s = we.client_order_id(DATE, 'BUY', 'SDEV')
    assert coid_s == 'h1-' + DATE + '-SDEV-buy'
    api = Book(rows=[
        {'order_id': 'OID-S', 'client_order_id': coid_s, 'symbol': 'SDEV',
         'side': 'BUY', 'status': 'FILLED', 'filled_time': DATE + 'T09:30:01-04:00'},
        {'order_id': 'OID-A', 'client_order_id': 'older-id', 'symbol': 'AAA',
         'side': 'BUY', 'status': 'SUBMITTED'},
    ])
    status = tmp_path / f'{DATE}_status.json'
    assert not status.exists()
    with patch.object(we, 'write_last'), \
            patch.object(po, 'remote_session_journal', return_value=None):
        rc = po.submit_ready(
            submit=True, clock=lambda: early, loader=lambda _: body,
            api=api, state_dir=tmp_path)
    assert rc == 0
    assert api.calls == []
    saved = json.loads(status.read_text())
    assert saved['status'] == 'already_submitted'
    assert {row['ticker'] for row in saved['found']} == {'SDEV', 'AAA'}
    assert {row['match'] for row in saved['found']} == {'client_order_id', 'symbol_side'}


def test_partial_broker_book_submits_only_the_missing_orders(tmp_path):
    early = datetime.fromisoformat(DATE + 'T08:41:00-04:00')
    body = _two_name_payload()
    coid_s = we.client_order_id(DATE, 'BUY', 'SDEV')
    api = Book(rows=[
        {'order_id': 'OID-S', 'client_order_id': coid_s, 'symbol': 'SDEV',
         'side': 'BUY', 'status': 'SUBMITTED'},
    ])
    with patch.object(we, 'write_last'), \
            patch.object(po, 'remote_session_journal', return_value=None):
        rc = po.submit_ready(
            submit=True, clock=lambda: early, loader=lambda _: body,
            api=api, state_dir=tmp_path)
    assert rc == 0
    assert len(api.calls) == 1
    assert [row['ticker'] for row in api.calls[0]] == ['AAA']
    saved = json.loads((tmp_path / f'{DATE}_status.json').read_text())
    assert saved['status'] == 'acknowledged'
    assert [row['ticker'] for row in saved['found']] == ['SDEV']
    by = {row['ticker']: row['status'] for row in saved['sent']}
    assert by['SDEV'] == 'already_submitted'
    assert by['AAA'] == 'acknowledged'


def test_broker_query_failure_submits_nothing(tmp_path):
    early = datetime.fromisoformat(DATE + 'T08:41:00-04:00')
    api = Book(error='sandbox order list down')
    with patch.object(we, 'write_last'), \
            patch.object(po, 'remote_session_journal', return_value=None):
        rc = po.submit_ready(
            submit=True, clock=lambda: early, loader=lambda _: _two_name_payload(),
            api=api, state_dir=tmp_path)
    assert rc == 2
    assert api.calls == []
    assert not (tmp_path / f'{DATE}_submit.json').exists()
    saved = json.loads((tmp_path / f'{DATE}_status.json').read_text())
    assert saved['status'] == 'query_failed'
    assert saved['sent'] == []


def test_ready_refire_does_not_double_place(tmp_path):
    api = API()
    early = datetime.fromisoformat(DATE + 'T06:20:00-04:00')
    with patch.object(we, 'write_last'), \
            patch.object(po, 'remote_session_journal', return_value=None):
        assert po.submit_ready(submit=True, clock=lambda: early, loader=lambda _: early_payload(),
                               api=api, state_dir=tmp_path) == 0
        assert po.submit_ready(submit=True, clock=lambda: early, loader=lambda _: early_payload(),
                               api=api, state_dir=tmp_path) == 0
    assert len(api.calls) == 1


def test_fallback_run_noops_after_ready_journal(tmp_path):
    api = API()
    early = datetime.fromisoformat(DATE + 'T06:20:00-04:00')
    with patch.object(we, 'write_last'), \
            patch.object(po, 'remote_session_journal', return_value=None):
        assert po.submit_ready(submit=True, clock=lambda: early, loader=lambda _: early_payload(),
                               api=api, state_dir=tmp_path) == 0
        t = [BELL - timedelta(seconds=20)]
        def clock():
            return t[0]
        def sleep(seconds):
            t[0] += timedelta(seconds=seconds)
        assert po.run(submit=True, clock=clock, sleep=sleep, loader=lambda _: payload(),
                      api=api, state_dir=tmp_path) == 0
    assert len(api.calls) == 1


def test_fallback_run_noops_on_remote_ready_journal(tmp_path):
    api = API()
    remote = {'date': DATE, 'status': 'acknowledged', 'fingerprint': 'abc', 'sent': []}
    with patch.object(po, 'remote_session_journal', return_value=remote):
        assert po.run(submit=True, clock=lambda: BELL - timedelta(minutes=10),
                      api=api, state_dir=tmp_path) == 0
    assert not api.calls


def test_ready_submit_after_bell_when_decisions_arrive_late(tmp_path):
    """At 09:30 the standing path records a miss. It does not buy the print."""
    api = API()
    late = datetime.fromisoformat(DATE + 'T10:05:00-04:00')
    p = payload()
    p['decision_readiness']['completed_at'] = (BELL + timedelta(minutes=20)).isoformat()
    loaded = []

    def loader(_date):
        loaded.append(_date)
        return p

    with patch.object(we, 'write_last'), \
            patch.object(po, 'remote_session_journal', return_value=None):
        rc = po.submit_ready(submit=True, clock=lambda: late, loader=loader,
                             api=api, state_dir=tmp_path)
    assert rc == 2
    assert api.calls == []
    assert loaded == []
    assert not (tmp_path / f'{DATE}_submit.json').exists()
    saved = json.loads((tmp_path / f'{DATE}_status.json').read_text())
    assert saved['status'] == 'missed_deadline'
    assert saved['submit'] is False
    with pytest.raises(ValueError, match='after decision clock'):
        po.make_plan(p, API().snapshot(), late)


def test_owner_gate_actions_blocks_ecs(tmp_path, monkeypatch):
    rec = tmp_path / 'owner.json'
    rec.write_text(json.dumps({'owner': 'actions'}))
    monkeypatch.setenv('PAPER_OPEN_OWNER_FILE', str(rec))
    assert po.owner_enabled('actions')
    assert not po.owner_enabled('ecs')
    with patch.object(po, 'submit_ready', return_value=0) as ready, \
            patch.object(po, 'run', return_value=0) as run:
        assert po.main(['--submit', '--ready', '--owner', 'ecs']) == 0
        ready.assert_not_called()
        run.assert_not_called()
        assert po.main(['--submit', '--ready', '--owner', 'actions']) == 2
        ready.assert_not_called()
        monkeypatch.setenv('PAPER_OPEN_SENDER', 'seal')
        assert po.main(['--submit', '--ready', '--owner', 'actions']) == 0
        ready.assert_called_once()
        run.assert_not_called()
        monkeypatch.delenv('PAPER_OPEN_SENDER')
        assert po.main(['--submit', '--owner', 'actions']) == 2
        run.assert_not_called()
        monkeypatch.setenv('PAPER_OPEN_SENDER', 'backstop')
        assert po.main(['--submit', '--owner', 'actions']) == 0
        run.assert_called_once()


def test_committed_owner_is_actions():
    rec = json.loads((po.ROOT / '00_grounding' / 'paper_open_owner.json').read_text())
    assert rec['owner'] == 'actions'
    yml = (po.ROOT / '.github/workflows/install_paper_open.yml').read_text()
    assert "'owner': 'ecs'" not in yml
    assert '"owner": "ecs"' not in yml


def test_release_records_clamped_shares_and_cash_skip(tmp_path):
    class Shrink(API):
        def place_batch(self, tickets):
            self.calls.append(tickets)
            out = {}
            for t in tickets:
                coid = we.client_order_id(t['date'], t['side'], t['ticker'])
                if t['ticker'] == 'CCC':
                    out[coid] = {
                        'ok': True, 'skipped': True, 'shares': 0,
                        'resized_from': t['shares'],
                        'error': 'remaining cash 1.00 < 1 share @ 10.00',
                    }
                elif t['ticker'] == 'BBB':
                    out[coid] = {
                        'ok': True, 'order_id': 'broker-BBB',
                        'shares': t['shares'] - 3, 'resized_from': t['shares'],
                    }
                else:
                    out[coid] = {
                        'ok': True, 'order_id': 'broker-' + t['ticker'],
                        'shares': t['shares'],
                    }
            return out

    base = plan()
    base['card']['tickets'] = [
        {'ticker': 'AAA', 'side': 'BUY', 'shares': 30, 'px': 10, 'date': DATE},
        {'ticker': 'BBB', 'side': 'BUY', 'shares': 30, 'px': 10, 'date': DATE},
        {'ticker': 'CCC', 'side': 'BUY', 'shares': 30, 'px': 10, 'date': DATE},
    ]
    api = Shrink()
    result = po.release(base, api, lambda: BELL, tmp_path / 'x', submit=True)
    assert result['status'] == 'acknowledged'
    by = {row['ticker']: row for row in result['sent']}
    assert by['AAA']['status'] == 'acknowledged' and by['AAA']['shares'] == 30
    assert by['BBB']['shares'] == 27 and by['BBB']['resized_from'] == 30
    assert by['BBB']['status'] == 'acknowledged'
    assert by['CCC']['status'] == 'skipped_cash'
    assert by['CCC']['shares'] == 0
    assert len(api.calls) == 1
    saved = json.loads((tmp_path / 'x').read_text())
    assert [row['status'] for row in saved['sent']] == [
        'acknowledged', 'acknowledged', 'skipped_cash']


def test_load_local_reads_dated_tickets(tmp_path):
    board = tmp_path / 'data' / 'day_board'
    board.mkdir(parents=True)
    (board / f'{DATE}_strategy_tickets.json').write_text(json.dumps(payload()))
    got = po.load_local(DATE, root=tmp_path)
    assert got['decision_readiness']['fingerprint'] == 'abc'


FLAT_DATE = '2026-09-28'
FLAT_READY = datetime.fromisoformat(FLAT_DATE + 'T08:03:00-04:00')
FLAT_RUN = datetime.fromisoformat(FLAT_DATE + 'T08:07:00-04:00')


class GuardAPI(API):
    """Other-date path must not touch cancels."""

    def list_open_orders(self):
        raise AssertionError('must not list open orders')

    def cancel_order(self, order_id):
        raise AssertionError('must not cancel')


class FlattenAPI:
    host = we.PAPER_HOST
    err = None

    def __init__(self, *, cancel_ok=True, snap_error=None, list_error=None, orders=None):
        self.events = []
        self.calls = []
        self.cancel_ok = cancel_ok
        self.snap_error = snap_error
        self.list_error = list_error
        self.orders = orders if orders is not None else [
            {'order_id': 'OID-1', 'client_order_id': 'fsOLD1', 'symbol': 'AAA', 'side': 'BUY'},
            {'order_id': 'OID-2', 'client_order_id': 'fsOLD2', 'symbol': 'BBB', 'side': 'SELL'},
        ]
        self.positions = {
            'AAA': {'shares': 10, 'last_px': 5},
            'BBB': {'shares': 2, 'last_px': 8},
            'CCC': {'shares': 0, 'last_px': 1},
            'DDD': {'shares': 0.4, 'last_px': 2},
        }

    def connect(self):
        self.events.append('connect')
        return True

    def list_open_orders(self):
        self.events.append('list')
        if self.list_error:
            raise RuntimeError(self.list_error)
        return list(self.orders)

    def cancel_order(self, order_id):
        self.events.append(('cancel', order_id))
        if not self.cancel_ok:
            return {'ok': False, 'order_id': order_id, 'error': 'rejected'}
        return {'ok': True, 'order_id': order_id}

    def snapshot(self):
        self.events.append('snapshot')
        if self.snap_error:
            return BrokerSnap(
                env='paper', cash=0, positions={}, connected=False, error=self.snap_error)
        return BrokerSnap(
            env='paper', cash=1000, positions=self.positions, connected=True)

    def place_batch(self, tickets):
        self.events.append('place')
        self.calls.append(tickets)
        return {
            we.client_order_id(t['date'], t['side'], t['ticker']): {
                'ok': True, 'order_id': 'broker-' + t['ticker'], 'shares': t['shares'],
            }
            for t in tickets
        }


def _refuse_hot4(monkeypatch):
    monkeypatch.setattr(
        po.we, 'plan_hot4_for_broker',
        lambda *a, **k: (_ for _ in ()).throw(AssertionError('plan_hot4_for_broker')))
    monkeypatch.setattr(
        'src.strategy_tickets.assert_hot4_wire',
        lambda *a, **k: (_ for _ in ()).throw(AssertionError('assert_hot4_wire')))
    monkeypatch.setattr(
        po, 'load_local',
        lambda *a, **k: (_ for _ in ()).throw(AssertionError('load_local')))
    monkeypatch.setattr(
        po, 'load_published',
        lambda *a, **k: (_ for _ in ()).throw(AssertionError('load_published')))


def test_paper_flatten_flag_is_only_2026_09_28():
    doc = json.loads((po.ROOT / '00_grounding' / 'paper_flatten.json').read_text())
    assert doc['date'] == FLAT_DATE
    assert doc['mode'] == 'flatten_account'
    assert datetime.fromisoformat(FLAT_DATE).weekday() == 0
    assert po.flatten_gate(FLAT_READY)[0] == 'flatten'
    assert po.flatten_gate(datetime.fromisoformat(DATE + 'T08:03:00-04:00')) is None
    assert po.flatten_gate(datetime.fromisoformat('2026-09-29T08:03:00-04:00')) is None


def _assert_flatten_sells(api, journal):
    assert api.events == [
        'connect', 'list', ('cancel', 'OID-1'), ('cancel', 'OID-2'), 'snapshot', 'place']
    assert len(api.calls) == 1
    tickets = api.calls[0]
    assert [(t['side'], t['ticker'], t['shares']) for t in tickets] == [
        ('SELL', 'AAA', 10), ('SELL', 'BBB', 2)]
    assert not any(t['side'] == 'BUY' for t in tickets)
    for ticket in tickets:
        body = we.order_body(ticket)
        assert body['order_type'] == 'MARKET'
        assert body['support_trading_session'] == 'CORE'
        assert body['time_in_force'] == 'DAY'
        assert body['side'] == 'SELL'
    assert journal['date'] == FLAT_DATE
    assert journal['mode'] == 'flatten_account'
    assert journal['status'] == 'acknowledged'
    assert journal['submit'] is True
    assert [c['order_id'] for c in journal['cancels']] == ['OID-1', 'OID-2']
    assert all(c['ok'] is True for c in journal['cancels'])
    assert [(r['side'], r['ticker'], r['order_id'], r['status']) for r in journal['sent']] == [
        ('SELL', 'AAA', 'broker-AAA', 'acknowledged'),
        ('SELL', 'BBB', 'broker-BBB', 'acknowledged'),
    ]
    assert not any(r['side'] == 'BUY' for r in journal['sent'])


def test_ready_flatten_cancels_then_sells_all_and_writes_journal(tmp_path, monkeypatch):
    _refuse_hot4(monkeypatch)
    api = FlattenAPI()
    with patch.object(we, 'write_last'), \
            patch.object(po, 'remote_session_journal', return_value=None):
        rc = po.submit_ready(
            submit=True, clock=lambda: FLAT_READY,
            loader=lambda _: (_ for _ in ()).throw(AssertionError('loader')),
            api=api, state_dir=tmp_path)
        assert rc == 0
        journal = json.loads((tmp_path / f'{FLAT_DATE}_submit.json').read_text())
        _assert_flatten_sells(api, journal)
        frozen = len(api.events)
        assert po.submit_ready(
            submit=True, clock=lambda: FLAT_READY,
            loader=lambda _: (_ for _ in ()).throw(AssertionError('loader')),
            api=api, state_dir=tmp_path) == 0

        def sleep(_seconds):
            raise AssertionError('later run must no-op')

        def loader(_date):
            raise AssertionError('later run must not load')

        assert po.run(
            submit=True, clock=lambda: FLAT_RUN, sleep=sleep, loader=loader,
            api=api, state_dir=tmp_path) == 0
        assert len(api.events) == frozen


def test_run_flatten_cancels_then_sells_without_waiting(tmp_path, monkeypatch):
    _refuse_hot4(monkeypatch)
    api = FlattenAPI()

    def sleep(_seconds):
        raise AssertionError('flatten must not wait for the bell')

    def loader(_date):
        raise AssertionError('loader')

    with patch.object(we, 'write_last'), \
            patch.object(po, 'remote_session_journal', return_value=None):
        rc = po.run(
            submit=True, clock=lambda: FLAT_RUN, sleep=sleep, loader=loader,
            api=api, state_dir=tmp_path)
    assert rc == 0
    journal = json.loads((tmp_path / f'{FLAT_DATE}_submit.json').read_text())
    _assert_flatten_sells(api, journal)


def test_other_date_ready_path_unchanged(tmp_path):
    api = GuardAPI()
    early = datetime.fromisoformat(DATE + 'T06:20:00-04:00')
    with patch.object(we, 'write_last'), \
            patch.object(po, 'remote_session_journal', return_value=None):
        rc = po.submit_ready(
            submit=True, clock=lambda: early, loader=lambda _: early_payload(),
            api=api, state_dir=tmp_path)
    assert rc == 0
    assert len(api.calls) == 1
    assert api.calls[0][0]['side'] == 'BUY'
    assert api.calls[0][0]['ticker'] == 'ABC'
    journal = json.loads((tmp_path / f'{DATE}_submit.json').read_text())
    assert journal['status'] == 'acknowledged'
    assert journal.get('mode') != 'flatten_account'
    assert 'cancels' not in journal


def test_other_date_run_path_unchanged(tmp_path):
    t = [BELL - timedelta(seconds=35)]

    def clock():
        return t[0]

    def sleep(seconds):
        t[0] += timedelta(seconds=seconds)

    calls = []

    def loader(date):
        calls.append(clock())
        assert clock() < BELL - timedelta(seconds=5)
        return payload()

    api = GuardAPI()
    with patch.object(we, 'write_last'), \
            patch.object(po, 'remote_session_journal', return_value=None):
        assert po.run(
            submit=True, clock=clock, sleep=sleep, loader=loader,
            api=api, state_dir=tmp_path) == 0
    assert t[0] == BELL
    assert calls
    assert len(api.calls) == 1
    assert api.calls[0][0]['side'] == 'BUY'
    assert api.calls[0][0]['ticker'] == 'ABC'


def test_flatten_cancel_failure_sends_no_orders(tmp_path, capsys):
    api = FlattenAPI(cancel_ok=False)
    with patch.object(we, 'write_last'), \
            patch.object(po, 'remote_session_journal', return_value=None):
        rc = po.submit_ready(
            submit=True, clock=lambda: FLAT_READY, loader=lambda _: payload(),
            api=api, state_dir=tmp_path)
    assert rc == 2
    assert api.calls == []
    assert api.events == ['connect', 'list', ('cancel', 'OID-1')]
    assert not (tmp_path / f'{FLAT_DATE}_submit.json').exists()
    status = json.loads((tmp_path / f'{FLAT_DATE}_status.json').read_text())
    assert status['status'] == 'blocked'
    assert status['mode'] == 'flatten_account'
    assert 'OID-1' in status['error']
    out = capsys.readouterr().out
    assert 'FLATTEN ABORT' in out
    assert 'no orders sent' in out


def test_flatten_snapshot_failure_sends_no_orders(tmp_path, capsys):
    api = FlattenAPI(snap_error='positions HTTP 500')
    with patch.object(we, 'write_last'), \
            patch.object(po, 'remote_session_journal', return_value=None):
        rc = po.submit_ready(
            submit=True, clock=lambda: FLAT_READY, loader=lambda _: payload(),
            api=api, state_dir=tmp_path)
    assert rc == 2
    assert api.calls == []
    assert 'place' not in api.events
    assert api.events[-1] == 'snapshot'
    assert ('cancel', 'OID-1') in api.events
    assert ('cancel', 'OID-2') in api.events
    assert not (tmp_path / f'{FLAT_DATE}_submit.json').exists()
    status = json.loads((tmp_path / f'{FLAT_DATE}_status.json').read_text())
    assert status['status'] == 'blocked'
    assert 'snapshot failed' in status['error']
    out = capsys.readouterr().out
    assert 'FLATTEN ABORT' in out
    assert 'no orders sent' in out


def test_flatten_open_order_list_failure_sends_no_orders(tmp_path, capsys):
    api = FlattenAPI(list_error='list down')
    with patch.object(we, 'write_last'), \
            patch.object(po, 'remote_session_journal', return_value=None):
        rc = po.run(
            submit=True, clock=lambda: FLAT_RUN, sleep=lambda _s: (_ for _ in ()).throw(
                AssertionError('must not wait')),
            loader=lambda _: (_ for _ in ()).throw(AssertionError('loader')),
            api=api, state_dir=tmp_path)
    assert rc == 2
    assert api.calls == []
    assert api.events == ['connect', 'list']
    assert not (tmp_path / f'{FLAT_DATE}_submit.json').exists()
    out = capsys.readouterr().out
    assert 'FLATTEN ABORT' in out
    assert 'no orders sent' in out


def test_flatten_non_paper_host_sends_nothing(tmp_path, capsys):
    api = FlattenAPI()
    api.host = 'api.webull.com'
    with patch.object(po, 'remote_session_journal', return_value=None):
        rc = po.submit_ready(
            submit=True, clock=lambda: FLAT_READY, loader=lambda _: payload(),
            api=api, state_dir=tmp_path)
    assert rc == 2
    assert api.events == []
    assert api.calls == []
    assert not (tmp_path / f'{FLAT_DATE}_submit.json').exists()
    assert 'no orders sent' in capsys.readouterr().out


def test_ecs_owner_skips_on_flatten_date(tmp_path, monkeypatch):
    rec = tmp_path / 'owner.json'
    rec.write_text(json.dumps({'owner': 'actions'}))
    monkeypatch.setenv('PAPER_OPEN_OWNER_FILE', str(rec))
    monkeypatch.setattr(
        po, 'flatten_gate',
        lambda *a, **k: (_ for _ in ()).throw(AssertionError('ecs must not read the flatten flag')))
    with patch.object(po, 'submit_ready', side_effect=AssertionError('submit')), \
            patch.object(po, 'run', side_effect=AssertionError('run')):
        assert po.main(['--submit', '--ready', '--owner', 'ecs']) == 0
        assert po.main(['--submit', '--owner', 'ecs']) == 0


def test_list_open_orders_reuses_standtest_probe_calls():
    from types import SimpleNamespace
    calls = []

    def list_order_open(aid):
        calls.append(('list_order_open', aid))
        raise RuntimeError('missing')

    def get_order_open(aid):
        calls.append(('get_order_open', aid))
        raise RuntimeError('missing too')

    def get_order_detail(aid, oid):
        calls.append(('get_order_detail', aid, oid))
        raise AssertionError('detail needs an id')

    def v2_open(aid):
        calls.append(('v2', aid))
        return {
            'data': [
                {'order_id': 'OID-9', 'client_order_id': 'c9', 'symbol': 'ZZ'},
                {'order_id': 'OID-9', 'symbol': 'ZZ'},
            ],
        }

    api = we.PaperAPI()
    api.account_id = 'aid-1'
    api.trade = SimpleNamespace(
        order_v3=SimpleNamespace(
            list_order_open=list_order_open,
            get_order_open=get_order_open,
            get_order_detail=get_order_detail,
        ),
        order_v2=SimpleNamespace(get_order_open=v2_open),
    )
    rows = api.list_open_orders()
    assert calls == [
        ('list_order_open', 'aid-1'),
        ('get_order_open', 'aid-1'),
        ('v2', 'aid-1'),
    ]
    assert rows == [{'order_id': 'OID-9', 'client_order_id': 'c9', 'symbol': 'ZZ'}]
    calls.clear()

    def first(aid):
        calls.append(('list_order_open', aid))
        return {'orders': [{'order_id': 'A', 'symbol': 'ZZ'}]}

    api.trade = SimpleNamespace(
        order_v3=SimpleNamespace(
            list_order_open=first,
            get_order_open=lambda aid: (_ for _ in ()).throw(AssertionError('second')),
            get_order_detail=get_order_detail,
        ),
        order_v2=SimpleNamespace(
            get_order_open=lambda aid: (_ for _ in ()).throw(AssertionError('v2'))),
    )
    assert api.list_open_orders() == [{'order_id': 'A', 'symbol': 'ZZ'}]
    assert calls == [('list_order_open', 'aid-1')]


def test_open_order_list_failure_is_not_an_empty_book():
    from types import SimpleNamespace

    def boom(aid):
        raise RuntimeError('down')

    api = we.PaperAPI()
    api.account_id = 'aid-1'
    api.trade = SimpleNamespace(
        order_v3=SimpleNamespace(
            list_order_open=boom,
            get_order_open=boom,
            get_order_detail=boom,
        ),
    )
    with pytest.raises(RuntimeError, match='open-order list failed'):
        api.list_open_orders()


def test_cancel_order_reuses_probe_call():
    from types import SimpleNamespace
    seen = {}

    def cancel_order(aid, oid):
        seen['args'] = (aid, oid)
        return {'order_id': oid, 'status': 'CANCELLED'}

    api = we.PaperAPI()
    api.account_id = 'aid-1'
    api.trade = SimpleNamespace(order_v3=SimpleNamespace(cancel_order=cancel_order))
    assert api.cancel_order('OID-9') == {'ok': True, 'order_id': 'OID-9'}
    assert seen['args'] == ('aid-1', 'OID-9')

    def reject(aid, oid):
        return {'code': 'ERROR', 'msg': 'nope'}

    api.trade = SimpleNamespace(order_v3=SimpleNamespace(cancel_order=reject))
    bad = api.cancel_order('OID-9')
    assert bad['ok'] is False
    assert bad['order_id'] == 'OID-9'

    api.host = 'api.webull.com'
    with pytest.raises(RuntimeError, match='sandbox'):
        api.cancel_order('OID-9')


INCIDENT_DAY = '2026-10-06'
INCIDENT_AT = datetime.fromisoformat(INCIDENT_DAY + 'T10:17:54-04:00')
MORNING_MISS = {
    'date': INCIDENT_DAY,
    'status': 'missed_deadline',
    'observed_at': INCIDENT_DAY + 'T09:34:28.027503-04:00',
}
# Names the 10:17 ET standing submit actually placed, plus FEAM which the
# sandbox rejected. The regression must not send any of them.
INCIDENT_NAMES = ('FEAM', 'GLND', 'NAUT', 'PACB', 'DNA', 'QSI')


def _incident_payload():
    p = payload()
    p['date'] = INCIDENT_DAY
    p['decision_readiness']['completed_at'] = INCIDENT_DAY + 'T10:00:00-04:00'
    p['decision_readiness']['fingerprint'] = 'ac14608197441d33'
    hot = p['strategies'][we.HOT4]
    hot['date'] = INCIDENT_DAY
    hot['s'] = 2.623
    hot['sell'] = [
        {'ticker': t, 'side': 'long', 'px': 3.0} for t in ('FEAM', 'GLND', 'NAUT')
    ]
    hot['buy'] = [
        {'ticker': t, 'side': 'long', 'px': 3.0} for t in ('PACB', 'DNA', 'QSI')
    ]
    return p


class _IncidentAPI(API):
    def snapshot(self):
        return BrokerSnap(
            env='paper', cash=1_000_000, connected=True,
            positions={'FEAM': {'shares': 939}, 'GLND': {'shares': 633},
                       'NAUT': {'shares': 1668}})

    def place_batch(self, tickets):
        names = [t.get('ticker') for t in tickets]
        raise AssertionError('standing submit after 09:30 placed ' + ','.join(names))


def test_standing_submit_at_1017_et_on_2026_10_06_sends_nothing(tmp_path):
    """The publish-tickets run at 10:17:54 ET must not place or overwrite.

    That morning the h1 send was already ``missed_deadline`` (09:34 ET).
    The later standing batch used client ids ``fs20261006S<TICKER>`` and
    acknowledged GLND, NAUT, PACB, DNA and QSI. FEAM was rejected. This
    clock now appends an event and leaves the first record intact.
    """
    status = tmp_path / f'{INCIDENT_DAY}_status.json'
    status.write_text(json.dumps(MORNING_MISS))
    api = _IncidentAPI()
    loaded = []

    def loader(_date):
        loaded.append(_date)
        return _incident_payload()

    with patch.object(we, 'write_last') as last, \
            patch.object(po, 'remote_session_journal', return_value=None):
        rc = po.submit_ready(
            submit=True, clock=lambda: INCIDENT_AT, loader=loader,
            payload=_incident_payload(), api=api, state_dir=tmp_path)
    assert rc == 2
    assert api.calls == []
    assert loaded == []
    assert not (tmp_path / f'{INCIDENT_DAY}_submit.json').exists()
    assert not last.called
    saved = json.loads(status.read_text())
    assert saved['status'] == 'missed_deadline'
    assert saved['observed_at'] == MORNING_MISS['observed_at']
    assert 'prepared_at' not in saved
    assert saved.get('submit') is not True
    assert saved['events'][-1]['status'] == 'missed_deadline'
    assert saved['events'][-1]['submit'] is False
    assert saved['events'][-1]['standing'] is True
    assert saved['events'][-1]['source'] == 'submit_ready'


def test_standing_submit_after_open_with_no_status_file_sends_nothing(tmp_path):
    """Same 10:17 ET batch when the morning file is missing. Still no order."""
    api = _IncidentAPI()
    with patch.object(we, 'write_last'), \
            patch.object(po, 'remote_session_journal', return_value=None):
        rc = po.submit_ready(
            submit=True, clock=lambda: INCIDENT_AT,
            loader=lambda _: _incident_payload(), api=api, state_dir=tmp_path)
    assert rc == 2
    assert api.calls == []
    assert not (tmp_path / f'{INCIDENT_DAY}_submit.json').exists()
    saved = json.loads((tmp_path / f'{INCIDENT_DAY}_status.json').read_text())
    assert saved['status'] == 'missed_deadline'
    assert saved['submit'] is False
    assert saved['standing'] is True
    assert 'events' not in saved
    assert 'card' not in saved
    assert 'prepared_at' not in saved


def test_standing_release_at_the_bell_does_not_place(tmp_path):
    bell = datetime.fromisoformat(INCIDENT_DAY + 'T09:30:00-04:00')
    body = _incident_payload()
    body['decision_readiness']['completed_at'] = INCIDENT_DAY + 'T09:00:00-04:00'
    plan = po.make_plan(body, _IncidentAPI().snapshot(), bell - timedelta(seconds=15),
                        allow_after_bell=True)
    api = _IncidentAPI()
    result = po.release(plan, api, lambda: bell, tmp_path / 'journal.json',
                        submit=True, standing=True)
    assert api.calls == []
    assert result['status'] == 'missed_deadline'
    assert result['submit'] is False
    assert result['sent'] == []
    assert not (tmp_path / 'journal.json').exists()
    names = {t['ticker'] for t in plan['card']['tickets']}
    assert names == set(INCIDENT_NAMES)


def test_submitted_status_is_not_replaced(tmp_path):
    original = {
        'date': DATE, 'status': 'acknowledged', 'submit': True,
        'sent': [{'ticker': 'ABC', 'order_id': 'OID-KEEP', 'status': 'acknowledged'}],
    }
    (tmp_path / f'{DATE}_status.json').write_text(json.dumps(original))
    api = API()
    early = datetime.fromisoformat(DATE + 'T08:41:00-04:00')
    with patch.object(we, 'write_last'), \
            patch.object(po, 'remote_session_journal', return_value=None):
        rc = po.submit_ready(
            submit=True, clock=lambda: early, loader=lambda _: early_payload(),
            api=api, state_dir=tmp_path)
    assert rc == 0
    assert api.calls == []
    assert not (tmp_path / f'{DATE}_submit.json').exists()
    saved = json.loads((tmp_path / f'{DATE}_status.json').read_text())
    assert saved['status'] == 'acknowledged'
    assert saved['sent'] == original['sent']
    assert saved['events'][-1]['status'] == 'refused_existing_record'
    assert saved['events'][-1]['submit'] is False


def test_flatten_after_deadline_sends_nothing(tmp_path, monkeypatch):
    _refuse_hot4(monkeypatch)
    api = FlattenAPI()
    late = datetime.fromisoformat(FLAT_DATE + 'T10:17:54-04:00')
    with patch.object(we, 'write_last'), \
            patch.object(po, 'remote_session_journal', return_value=None):
        rc = po.submit_ready(
            submit=True, clock=lambda: late,
            loader=lambda _: (_ for _ in ()).throw(AssertionError('loader')),
            api=api, state_dir=tmp_path)
    assert rc == 2
    assert api.calls == []
    assert api.events == []
    assert not (tmp_path / f'{FLAT_DATE}_submit.json').exists()
    saved = json.loads((tmp_path / f'{FLAT_DATE}_status.json').read_text())
    assert saved['status'] == 'missed_deadline'
    assert saved['submit'] is False


def test_bell_path_started_after_the_open_sends_nothing(tmp_path):
    api = _IncidentAPI()

    def boom(*_a, **_k):
        raise AssertionError('late bell path must not wait or load')

    with patch.object(we, 'write_last'), \
            patch.object(po, 'remote_session_journal', return_value=None):
        rc = po.run(submit=True, clock=lambda: INCIDENT_AT, sleep=boom, loader=boom,
                    api=api, state_dir=tmp_path)
    assert rc == 2
    assert api.calls == []
    saved = json.loads((tmp_path / f'{INCIDENT_DAY}_status.json').read_text())
    assert saved['status'] == 'missed_deadline'
    assert saved['submit'] is False


def test_drift_log_records_2026_10_06_and_is_append_only(tmp_path):
    """The 10-06 gap is a committed line. A later append does not rewrite it."""
    from src import paper_drift

    real = po.ROOT / 'data' / 'paper_open' / 'drift_log.jsonl'
    raw = real.read_bytes()
    lines = raw.decode().splitlines()
    assert lines[0].startswith('{"kind":"doc"')
    gap = json.loads(lines[1])
    assert gap['kind'] == 'gap'
    assert gap['date'] == '2026-10-06'
    assert gap['plan_commit'].startswith('dcec598c3d')
    assert gap['prepared_at'].startswith('2026-10-06T10:17:54')
    assert gap['lateness_ms'] == 2874717.075
    assert gap['sdev']['bought'] is False
    assert 'already held on the sealed book' in gap['sdev']['reason']
    assert gap['sdev']['sealed_book_shares'] == 1222
    assert gap['feam']['sold'] is False
    assert gap['feam']['held_shares'] == 939
    assert gap['feam']['error'] == 'OPENAPI_ORDER_NOT_SUPPORT_REVERSE_OPTION'
    assert gap['feam']['order_id'] == ''
    sold = {row['ticker']: row['order_id'] for row in gap['sells_sent_late']}
    bought = {row['ticker']: row['order_id'] for row in gap['buys_sent_late']}
    assert sold == {
        'GLND': 'NJJO7E6QDTMI40CLS4EGQ4KRL8',
        'NAUT': '1GIIVILVRVQI877IGQUS6D5ASA',
    }
    assert bought == {
        'PACB': '3IG4QHJ4IC309B8FJNLRCJD0GA',
        'DNA': '3UHAD2KIHSSID8SO8KDQLQKR09',
        'QSI': 'HUIIVGPG79II8OCHIG4IDT7818',
    }
    copy = tmp_path / 'drift_log.jsonl'
    copy.write_bytes(raw)
    paper_drift.append_drift({'kind': 'warning', 'date': '2026-10-07', 'note': 'later'}, copy)
    got = copy.read_bytes()
    assert got.startswith(raw)
    assert got != raw
    assert json.loads(got.decode().splitlines()[1]) == gap
    assert real.read_bytes() == raw


def test_account_positions_do_not_fail_closed_against_sealed_tickets(monkeypatch):
    """Tickets are the sealed book. A drifted sandbox account does not block."""
    _boom_pick_day(monkeypatch)
    day = '2026-10-02'
    empty = po.make_plan(_sealed_day_payload(day), _rich_snap(), _sealed_clock(day))
    drifted = po.make_plan(
        _sealed_day_payload(day),
        _rich_snap(**{
            'FEAM': {'shares': 939},
            'PACB': {'shares': 1113},
            'DNA': {'shares': 214},
            'QSI': {'shares': 2482},
            'SDEV': {'shares': 1},
        }),
        _sealed_clock(day),
    )
    assert empty['card']['tickets'] == drifted['card']['tickets']
    sells = [(t['ticker'], t['shares']) for t in empty['card']['tickets'] if t['side'] == 'SELL']
    buys = [t['ticker'] for t in empty['card']['tickets'] if t['side'] == 'BUY']
    assert sells == [('EGG', 774), ('KOD', 33)]
    assert buys == ['QSI', 'TJGC']
    assert empty['card']['look_error'] == ''


def test_unheld_sealed_sell_is_drift_skipped_and_not_sent(tmp_path, monkeypatch):
    """A sealed sell the account does not hold is logged and not placed."""
    _boom_pick_day(monkeypatch)
    day = '2026-10-02'
    early = datetime.fromisoformat(day + 'T06:20:00-04:00')

    class Flat(API):
        def snapshot(self):
            return BrokerSnap(env='paper', cash=1_000_000, positions={}, connected=True)

    api = Flat()
    with patch.object(we, 'write_last'), \
            patch.object(po, 'remote_session_journal', return_value=None):
        rc = po.submit_ready(
            submit=True, clock=lambda: early, loader=lambda _: _flat_sit_payload(day),
            api=api, state_dir=tmp_path)
    assert rc == 0
    assert api.calls
    placed = [(row['side'], row['ticker']) for row in api.calls[0]]
    assert ('SELL', 'EGG') not in placed and ('SELL', 'KOD') not in placed
    assert [t for side, t in placed if side == 'BUY'] == ['QSI', 'TJGC']
    journal = json.loads((tmp_path / f'{day}_submit.json').read_text())
    skipped = [row['ticker'] for row in journal['sent'] if row['status'] == 'drift-skipped']
    assert skipped == ['EGG', 'KOD']
    log = (tmp_path / 'drift_log.jsonl').read_text()
    assert 'drift-skipped' in log
    assert 'EGG' in log and 'KOD' in log


def test_2026_10_07_from_drifted_1006_account_sends_only_that_days_tickets(
        tmp_path, monkeypatch):
    """10-07 seal sends the 10-07 plan only. The drifted 10-06 account is not corrected."""
    import hashlib
    from src.h1_sealed_exec import canonical_bytes

    def seal_line(obj):
        body = {k: v for k, v in obj.items() if k != 'sha256'}
        digest = hashlib.sha256(canonical_bytes(body)).hexdigest()
        full = dict(body)
        full['sha256'] = digest
        return canonical_bytes(full)

    mark = {
        'kind': 'mark', 'date': '2026-10-06',
        'holdings': [
            {'ticker': 'SDEV', 'shares': 1222, 'last_px': 3.94, 'entry_date': '2026-09-30'},
            {'ticker': 'FEAM', 'shares': 939, 'last_px': 3.88, 'entry_date': '2026-10-05'},
        ],
    }
    plan_line = {
        'kind': 'plan', 'date': '2026-10-07', 'bar_cutoff': '2026-10-06',
        'cash_before': 10000.0, 'committed_at': '2026-10-07T12:00:00Z',
        'excluded_unexplained_legs': [], 'holdup_on': False, 'morning_s': 1.0,
        'picks': [{
            'ticker': 'AAPL', 'rank': 1, 'fv_price': 100.0,
            'fv_avg_volume': 50000.0, 'sources': ['yday_mover'],
        }],
        'planned_sells': [{'ticker': 'SDEV', 'shares': 1222, 'reason': 'hold-expired'}],
        'recipe': 'union_hot_n4_h1__w0',
    }
    log = tmp_path / 'h1_log.jsonl'
    log.write_bytes((json.dumps(mark) + '\n').encode() + seal_line(plan_line))
    monkeypatch.setattr('src.h1_sealed_exec.H1_LOG', log)
    _boom_pick_day(monkeypatch)
    day = '2026-10-07'
    early = datetime.fromisoformat(day + 'T08:00:00-04:00')
    drifted = {
        'FEAM': {'shares': 939},
        'PACB': {'shares': 1113},
        'DNA': {'shares': 214},
        'QSI': {'shares': 2482},
    }

    class Drifted(API):
        def snapshot(self):
            return BrokerSnap(
                env='paper', cash=1_000_000, positions=drifted, connected=True)

    body = _sealed_day_payload(day)
    body['decision_readiness']['completed_at'] = day + 'T06:00:00-04:00'
    hot = body['strategies'][we.HOT4]
    hot['buy'] = [
        {'ticker': t, 'side': 'long', 'px': 1}
        for t in ('SDEV', 'PACB', 'DNA', 'QSI')
    ]
    hot['sell'] = [
        {'ticker': t, 'side': 'long'} for t in ('FEAM', 'GLND', 'NAUT')
    ]
    card = po.make_plan(body, Drifted().snapshot(), early)['card']
    assert [(t['ticker'], t['side'], t['shares']) for t in card['tickets']] == [
        ('SDEV', 'SELL', 1222), ('AAPL', 'BUY', 146),
    ]
    assert card['look_error'] == ''
    api = Drifted()
    real = (po.ROOT / 'data' / 'paper_open' / 'drift_log.jsonl').read_bytes()
    status_before = (po.ROOT / 'data' / 'paper_open' / '2026-10-06_status.json').read_bytes()
    with patch.object(we, 'write_last'), \
            patch.object(po, 'remote_session_journal', return_value=None):
        rc = po.submit_ready(
            submit=True, clock=lambda: early, loader=lambda _: body,
            api=api, state_dir=tmp_path)
    assert rc == 0
    assert len(api.calls) == 1
    placed = api.calls[0]
    assert [(row['side'], row['ticker'], row['shares']) for row in placed] == [
        ('BUY', 'AAPL', 146),
    ]
    assert all(str(row.get('date') or day) == day for row in placed)
    banned = {'SDEV', 'FEAM', 'GLND', 'NAUT', 'PACB', 'DNA', 'QSI'}
    assert banned.isdisjoint({row['ticker'] for row in placed})
    journal = json.loads((tmp_path / f'{day}_submit.json').read_text())
    skipped = [row for row in journal['sent'] if row['status'] == 'drift-skipped']
    assert [(row['ticker'], row['side']) for row in skipped] == [('SDEV', 'SELL')]
    warning = json.loads((tmp_path / 'drift_log.jsonl').read_text().splitlines()[-1])
    assert warning['kind'] == 'warning'
    assert warning['date'] == day
    held = {row['ticker'] for row in warning['held_not_on_plan']}
    assert held == {'DNA', 'FEAM', 'PACB', 'QSI'}
    assert 'no catch-up' in warning['note']
    assert (po.ROOT / 'data' / 'paper_open' / 'drift_log.jsonl').read_bytes() == real
    assert (po.ROOT / 'data' / 'paper_open' / '2026-10-06_status.json').read_bytes() == status_before
