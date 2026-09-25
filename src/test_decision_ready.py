import json
from types import SimpleNamespace
from unittest.mock import patch
from . import decision_ready as dr, stock_book_diag as diag


def test_inputs_not_existing_outputs_control_readiness(tmp_path):
    specs = [{'key': 'stock_book', 'files': [
        {'key': 'in_a', 'role': 'input', 'rel': 'a.json'},
        {'key': 'join', 'role': 'required', 'rel': 'join.csv'},
        {'key': 'book_json', 'role': 'required', 'rel': 'not_yet_built.json'}]}]
    panel = tmp_path/'data/factor_mine/panel.json'
    panel.parent.mkdir(parents=True); panel.write_text('{}')
    (tmp_path/'a.json').write_text('{}')
    def check(item, date):
        exists = (tmp_path/item['rel']).exists()
        return SimpleNamespace(status='OK' if exists else 'MISSING', reason='')
    with patch.object(dr, 'ROOT', tmp_path), patch.object(diag, 'workflow_specs', return_value=specs), \
         patch.object(diag, '_check_file', side_effect=check):
        before = dr.evaluate('2026-09-17')
        assert not before['ready']
        (tmp_path/'join.csv').write_text('ticker\nABC\n')
        ready = dr.evaluate('2026-09-17')
        assert ready['ready']  # no circular dependency on book/dashboard outputs
        (tmp_path/'a.json').write_text('{"new":1}')
        assert dr.evaluate('2026-09-17')['fingerprint'] != ready['fingerprint']


def test_dispatch_for_upstream_input_but_not_own_publication():
    date = dr.datetime.now(dr.ET).date().isoformat()
    with patch.object(dr, 'dispatch', return_value=True) as dispatch:
        assert dr.notify_changed([f'01_daily/news/{date}_actions.json'])
        assert dispatch.call_count == 1
        assert not dr.notify_changed(['dashboard/factor-mine/today.json',
                                      f'data/day_board/{date}_strategy_tickets.json'])
        assert dispatch.call_count == 1


def test_ready_false_when_hashed_peer_rs_never_landed_on_main():
    proof = {
        'ready': True,
        'fingerprint': 'abc',
        'inputs': {'data/peers/2026-09-25_peer_rs.csv': 'deadbeef'},
        'blockers': [],
    }
    with patch.object(dr, 'blob_sha256_on_main', return_value=False):
        gated = dr.apply_main_gate(dict(proof))
    assert gated['ready'] is False
    assert gated['main_missing'] == ['data/peers/2026-09-25_peer_rs.csv']
    assert any(b['reason'] == 'not_on_main' for b in gated['blockers'])

    from . import strategy_tickets as st

    def _proof():
        return {
            'ready': True,
            'fingerprint': 'abc',
            'inputs': {'data/peers/2026-09-25_peer_rs.csv': 'deadbeef'},
            'blockers': [],
        }

    with patch.object(dr, 'evaluate', side_effect=lambda _date: _proof()), \
         patch.object(dr, 'blob_sha256_on_main', return_value=False), \
         patch.object(st, 'stock_book_strats', return_value=[]), \
         patch.object(st, 'excel_strats', return_value=[]), \
         patch.object(st, 'flatten_strat', return_value={'name':'flat','family':'flatten'}), \
         patch.object(st, 'recipe_strats', return_value=[]), \
         patch.object(st, 'stamp_live_quotes', side_effect=lambda p, d: p), \
         patch.object(st, 'attach_hard_red_research', side_effect=lambda p, d: p):
        payload = st.build('2026-09-25')
    assert payload['decision_readiness']['ready'] is False
    assert 'data/peers/2026-09-25_peer_rs.csv' in payload['decision_readiness']['main_missing']

    matched = dict(proof)
    with patch.object(dr, 'blob_sha256_on_main', return_value='deadbeef'):
        assert dr.apply_main_gate(matched)['ready'] is True


def test_peer_rs_is_a_land_path():
    from . import land_file
    paths = land_file.step_paths('2026-09-25', 'peer_rs')
    assert any(p.name == '2026-09-25_peer_rs.csv' for p in paths)


def test_input_change_during_build_invalidates_decision():
    from . import strategy_tickets as st
    before = {'ready': True, 'fingerprint': 'before'}
    after = {'ready': True, 'fingerprint': 'after'}
    with patch.object(dr, 'evaluate', side_effect=[before, after]), \
         patch.object(st, 'stock_book_strats', return_value=[]), \
         patch.object(st, 'excel_strats', return_value=[]), \
         patch.object(st, 'flatten_strat', return_value={'name':'flat','family':'flatten'}), \
         patch.object(st, 'recipe_strats', return_value=[]), \
         patch.object(st, 'stamp_live_quotes', side_effect=lambda p,d:p), \
         patch.object(st, 'attach_hard_red_research', side_effect=lambda p,d:p):
        payload = st.build('2026-09-17')
    assert payload['decision_readiness']['ready'] is False


def test_slim_preserves_market_score_and_input_proof(tmp_path):
    from . import strategy_tickets as st
    date = '2026-09-17'
    payload = {'date':date,'generated_at':date+'T09:00:00-04:00',
               'decision_readiness':{'ready': True, 'fingerprint':'abc'},
               'strategies':{'test':{'date':date,'buy':[],'sell':[],'s':-4,'status':'sit'}}}
    with patch.object(st, 'DAY', tmp_path/'day'), patch.object(st, 'FM_DIR', tmp_path/'fm'), \
         patch.object(st, 'DASH_FM', tmp_path/'dash'), patch.object(st, 'ROOT', tmp_path), \
         patch.object(st, 'assert_session_look'), \
         patch('src.hard_red_sit_research.write_per_sleeve'):
        st.write(date, payload)
    slim=json.loads((tmp_path/'day/today_strategies.json').read_text())
    assert slim['strategies']['test']['s']==-4
    assert slim['decision_readiness']['ready']


def test_old_ready_artifact_cannot_hide_failed_rebuild(tmp_path):
    import pytest
    from . import stock_book, publish_live_boards
    date = '2026-09-17'
    path = tmp_path / 'data/day_board' / f'{date}_strategy_tickets.json'
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps({'decision_readiness': {'ready':True, 'fingerprint':'abc',
        'completed_at':date+'T09:00:00-04:00'}, 'strategies':{'union_hot_n4_h1':{'status':'ok'}}}))
    with patch.object(dr, 'ROOT', tmp_path), \
         patch.object(dr, 'evaluate', return_value={'ready':True, 'fingerprint':'abc'}), \
         patch.object(stock_book, 'build', return_value=(None,{})), \
         patch.object(stock_book, 'write_report'), \
         patch.object(publish_live_boards, 'publish', return_value={}):
        with pytest.raises(RuntimeError, match='incomplete'):
            dr.publish(date)


def _draft_publish_patches(tmp_path, publish_out):
    from datetime import datetime
    from zoneinfo import ZoneInfo
    from . import stock_book, publish_live_boards

    et = ZoneInfo('America/New_York')

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            base = datetime(2026, 9, 25, 16, 15, tzinfo=et)
            return base if tz is not None else base.replace(tzinfo=None)

    return (
        patch.object(dr, 'ROOT', tmp_path),
        patch.object(dr, 'datetime', Clock),
        patch.object(dr, 'evaluate', return_value={'ready': True, 'fingerprint': 'abc'}),
        patch.object(stock_book, 'build', return_value=(None, {})),
        patch.object(stock_book, 'write_report'),
        patch.object(publish_live_boards, 'publish', return_value=publish_out),
        patch('src.book_suggestions.refresh_factor_live_poller'),
    )


def _enter_all(patches):
    for item in patches:
        item.start()

    def stop():
        for item in reversed(patches):
            item.stop()
    return stop


def test_locked_draft_publication_is_complete(tmp_path):
    import io
    import contextlib
    date = '2026-09-25'
    evening = {
        'decision_readiness': {
            'ready': True,
            'fingerprint': 'abc',
            'completed_at': '2026-09-25T16:15:02-04:00',
        },
        'strategies': {'union_hot_n4_h1': {'status': 'ok'}},
    }
    morning = {
        'decision_readiness': {
            'ready': True,
            'fingerprint': 'morning',
            'completed_at': '2026-09-25T08:30:00-04:00',
        },
        'strategies': {'union_hot_n4_h1': {'status': 'ok'}},
    }
    board = tmp_path / 'data/day_board'
    board.mkdir(parents=True)
    dated = board / f'{date}_strategy_tickets.json'
    draft = board / f'{date}_strategy_tickets_draft.json'
    dated.write_text(json.dumps(morning))
    draft.write_text(json.dumps(evening))
    stop = _enter_all(_draft_publish_patches(tmp_path, {'ticket_lock': 'draft'}))
    try:
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            assert dr.publish(date) == 0
        log = buf.getvalue()
        assert 'Publication complete' in log
        assert 'WARN' in log
        assert json.loads(dated.read_text())['decision_readiness']['fingerprint'] == 'morning'
    finally:
        stop()


def test_locked_draft_fails_when_dated_file_matches_or_draft_is_missing(tmp_path):
    import pytest
    date = '2026-09-25'
    body = {
        'decision_readiness': {
            'ready': True,
            'fingerprint': 'abc',
            'completed_at': '2026-09-25T16:15:02-04:00',
        },
        'strategies': {'union_hot_n4_h1': {'status': 'ok'}},
    }
    board = tmp_path / 'data/day_board'
    board.mkdir(parents=True)
    dated = board / f'{date}_strategy_tickets.json'
    draft = board / f'{date}_strategy_tickets_draft.json'
    text = json.dumps(body)
    dated.write_text(text)
    draft.write_text(text)
    stop = _enter_all(_draft_publish_patches(tmp_path, {'ticket_lock': 'draft'}))
    try:
        with pytest.raises(RuntimeError, match='modified'):
            dr.publish(date)
        draft.unlink()
        with pytest.raises(RuntimeError, match='draft write failed'):
            dr.publish(date)
    finally:
        stop()


def test_strategy_error_still_refuses_success_on_the_draft_path(tmp_path):
    import pytest
    date = '2026-09-25'
    evening = {
        'decision_readiness': {
            'ready': True,
            'fingerprint': 'abc',
            'completed_at': '2026-09-25T16:15:02-04:00',
        },
        'strategies': {'union_hot_n4_h1': {'status': 'ok'}},
    }
    board = tmp_path / 'data/day_board'
    board.mkdir(parents=True)
    (board / f'{date}_strategy_tickets.json').write_text(json.dumps({
        'decision_readiness': {'ready': True, 'fingerprint': 'morning',
                               'completed_at': '2026-09-25T08:30:00-04:00'},
        'strategies': {'union_hot_n4_h1': {'status': 'ok'}},
    }))
    (board / f'{date}_strategy_tickets_draft.json').write_text(json.dumps(evening))
    stop = _enter_all(_draft_publish_patches(
        tmp_path, {'ticket_lock': 'draft', 'strategy_error': 'boom'}))
    try:
        with pytest.raises(RuntimeError, match='incomplete'):
            dr.publish(date)
    finally:
        stop()


def test_day_board_merge_never_treats_strategy_sidecar_as_a_board(tmp_path):
    from . import day_board as db
    ours = tmp_path/'ours'; ours.mkdir()
    board = tmp_path/'board'; board.mkdir()
    date = '2026-09-17'
    original = {'date':date,'strategies':{'hot4':{'buy':[]}},'generated_at':'2026-09-17T09:20:00'}
    (ours/f'{date}_strategy_tickets.json').write_text(json.dumps(original))
    (ours/f'{date}.json').write_text(json.dumps({'date':date,'lands':[]}))
    with patch.object(db, 'BOARD_DIR', board), patch.object(db, 'write_json') as write:
        db.merge_ours_dir(str(ours))
    assert write.call_args.args[0]['date'] == date
    assert 'strategies' not in write.call_args.args[0]
    assert not (board/f'{date}_strategy_tickets.json').exists()
