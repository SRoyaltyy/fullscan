from datetime import datetime, timedelta
import json
from unittest.mock import patch
import pytest
from . import paper_open as po, webull_exec as we
from .futubull_exec import BrokerSnap


@pytest.fixture(autouse=True)
def synthetic_hot4_matches_payload(monkeypatch, request):
    """Synthetic tickets are not the live Factor Mine panel.

    The divergence test runs the real submit check.
    """
    if request.node.name in (
        "test_submit_refuses_hot4_buys_that_diverge_from_recipe",
        "test_submit_refuses_hot4_sells_that_diverge_from_recipe",
    ):
        return

    def _accept(date, buys, sells=None, panel=None):
        return [
            str(b.get("ticker") or "").upper()
            for b in (buys or [])
            if isinstance(b, dict) and b.get("ticker")
        ]

    monkeypatch.setattr("src.strategy_tickets.assert_hot4_wire", _accept)

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


def test_submit_refuses_hot4_buys_that_diverge_from_recipe(monkeypatch):
    monkeypatch.setattr(
        "src.strategy_tickets.hot4_recipe_tickers",
        lambda date, panel=None: ["FEAM", "TJGC", "LVWR", "SECZ"],
    )
    p = payload()
    p["strategies"][we.HOT4]["buy"] = [
        {"ticker": t, "side": "long", "px": 10}
        for t in ("DELL", "GME", "UMC", "VSTS")
    ]
    with pytest.raises(ValueError, match="diverge"):
        po.make_plan(p, API().snapshot(), BELL - timedelta(seconds=15))


def test_submit_refuses_hot4_sells_that_diverge_from_recipe(monkeypatch):
    monkeypatch.setattr(
        "src.strategy_tickets.hot4_recipe_tickers",
        lambda date, panel=None: ["ABC"],
    )
    monkeypatch.setattr(
        "src.strategy_tickets.hot4_recipe_sells",
        lambda date, panel=None, **_kw: ["FEAM", "TJGC", "LVWR"],
    )
    p = payload()
    p["strategies"][we.HOT4]["sell"] = [
        {"ticker": t, "side": "long"} for t in ("DELL", "GME")
    ]
    with pytest.raises(ValueError, match="diverge"):
        po.make_plan(p, API().snapshot(), BELL - timedelta(seconds=15))


def test_plan_sells_held_lot_before_buys():
    p = payload()
    p["strategies"][we.HOT4]["buy"] = [{"ticker": "ABC", "px": 10, "side": "long"}]
    p["strategies"][we.HOT4]["sell"] = [{"ticker": "FEAM", "side": "long", "px": 4}]
    snap = BrokerSnap(
        env="paper", cash=1000, connected=True,
        positions={"FEAM": {"shares": 12, "last_px": 4}},
    )
    card = po.make_plan(p, snap, BELL - timedelta(seconds=15))["card"]
    assert card["tickets"][0]["side"] == "SELL"
    assert card["tickets"][0]["ticker"] == "FEAM"
    assert card["tickets"][0]["shares"] == 12
    assert card["tickets"][1]["side"] == "BUY"
    assert card["tickets"][1]["ticker"] == "ABC"


def test_plan_does_not_sell_unheld_name():
    p = payload()
    p["strategies"][we.HOT4]["sell"] = [{"ticker": "FEAM", "side": "long"}]
    snap = BrokerSnap(env="paper", cash=1000, positions={}, connected=True)
    card = po.make_plan(p, snap, BELL - timedelta(seconds=15))["card"]
    assert all(t["side"] != "SELL" for t in card["tickets"])
    assert any(s.get("kind") == "unheld" and s.get("ticker") == "FEAM"
               for s in card["skipped"])


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
    assert json.loads((tmp_path/f'{DATE}_status.json').read_text())==original


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
    api = API()
    late = datetime.fromisoformat(DATE + 'T10:05:00-04:00')
    p = payload()
    p['decision_readiness']['completed_at'] = (BELL + timedelta(minutes=20)).isoformat()
    with patch.object(we, 'write_last'), \
            patch.object(po, 'remote_session_journal', return_value=None):
        rc = po.submit_ready(submit=True, clock=lambda: late, loader=lambda _: p,
                             api=api, state_dir=tmp_path)
    assert rc == 0
    assert len(api.calls) == 1
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
        assert po.main(['--submit', '--ready', '--owner', 'actions']) == 0
        ready.assert_called_once()
        run.assert_not_called()


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
