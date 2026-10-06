"""Offline tests for ushadow.py (no network). Run: python -m pytest -q research/updown_rel1_hv_v1/test_ushadow.py"""
import datetime as dt, io, json, os, sys
from pathlib import Path
import numpy as np, pandas as pd, pytest
sys.path.insert(0, str(Path(__file__).resolve().parent))
import ushadow as U

ET = U.ET


def at(s):  # 'YYYY-MM-DD HH:MM' ET -> utc datetime
    return dt.datetime.fromisoformat(s).replace(tzinfo=ET).astimezone(dt.timezone.utc)


def test_gate_times():
    g = U.due(at('2026-10-06 18:00')); assert g['freeze_date'] == '2026-10-07' and g['write_ok']
    g = U.due(at('2026-10-06 17:00')); assert g['freeze_date'] is None          # before final bars
    g = U.due(at('2026-10-07 09:10')); assert g['freeze_date'] == '2026-10-07'
    g = U.due(at('2026-10-07 09:21')); assert g['freeze_date'] is None and not g['write_ok']   # quiet window
    g = U.due(at('2026-10-07 09:46')); assert g['freeze_date'] is None and g['write_ok']
    g = U.due(at('2026-10-09 18:00')); assert g['freeze_date'] == '2026-10-12'  # Fri -> Mon
    g = U.due(at('2026-10-10 11:00')); assert g['freeze_date'] == '2026-10-12'  # Saturday
    g = U.due(at('2026-11-25 18:00')); assert g['freeze_date'] == '2026-11-27'  # Thanksgiving skipped
    g = U.due(at('2026-11-02 09:00')); assert g['freeze_date'] == '2026-11-02'  # after the DST switch
    g = U.due(at('2026-10-05 18:00')); assert g['freeze_date'] == '2026-10-06' or g['freeze_date'] is None
    assert U.due(at('2026-10-02 18:00'))['freeze_date'] is None                # before the start date


def test_holidays_match_history():
    # every weekday of 2025-01..2026-09 that is not a holiday is a session; checked against the research panel dates offline
    s = U.sessions_between('2026-09-01', '2026-09-30')
    assert pd.Timestamp('2026-09-07') not in s and len(s) == 21


def mk_repo(tmp):
    p = U.Paths(tmp)
    p.days.mkdir(parents=True); p.exp_days.mkdir(parents=True)
    return p


def lock_day(p, d, stamp):
    dd = p.days / d; dd.mkdir()
    (dd / 'picks.json').write_text('{"x":1}\n')
    U.append_lock(p.locks, {'date': d, 'frozen_at_utc': stamp, 'files': {p.rel(dd / 'picks.json'): U.sha256_file(dd / 'picks.json')}})


def test_locks_ok_and_tamper(tmp_path):
    p = mk_repo(tmp_path)
    lock_day(p, '2026-10-07', '2026-10-07T01:00:00+00:00')
    lock_day(p, '2026-10-08', '2026-10-08T13:00:00+00:00')
    assert U.lock_check(p, log=lambda *a: None) == []
    base = p.locks.read_bytes()
    # changed locked file
    (p.days / '2026-10-07' / 'picks.json').write_text('{"x":2}\n')
    assert any('changed' in e for e in U.lock_check(p, log=lambda *a: None))
    (p.days / '2026-10-07' / 'picks.json').write_text('{"x":1}\n')
    # rewritten lock line (re-fingerprint) -> hash chain + prefix check catch it
    lines = p.locks.read_text().splitlines()
    e = json.loads(lines[0]); e['frozen_at_utc'] = '2026-10-07T02:00:00+00:00'
    p.locks.write_text(json.dumps(e, sort_keys=True, separators=(',', ':')) + '\n' + lines[1] + '\n')
    errs = []
    U.check_ledger(p, p.locks, p.days, base, None, None, errs)
    assert any('append-only' in x for x in errs) and any('chain' in x for x in errs)


def test_locks_deadline_unlocked_and_retro(tmp_path):
    p = mk_repo(tmp_path)
    lock_day(p, '2026-10-07', '2026-10-07T13:25:00+00:00')         # 09:25 ET exactly -> too late
    assert any('not before 09:25' in e for e in U.lock_check(p, log=lambda *a: None))
    p2 = mk_repo(tmp_path / 'b')
    (p2.days / '2026-10-09').mkdir()
    assert any('without a lock line' in e for e in U.lock_check(p2, log=lambda *a: None))
    p3 = mk_repo(tmp_path / 'c')
    lock_day(p3, '2026-10-06', '2026-10-06T01:00:00+00:00')         # before start -> retroactive
    assert any('before the start' in e for e in U.lock_check(p3, log=lambda *a: None))


def test_missed_marker(tmp_path, monkeypatch):
    p = mk_repo(tmp_path)
    monkeypatch.setenv('USHADOW_NOW', '2026-10-08T14:00:00+00:00')   # Oct 8 10:00 ET; Oct 7 and 8 not frozen
    U.mark_missed(p, log=lambda *a: None)
    assert [e['date'] for e in U.read_jsonl(p.missed)] == ['2026-10-07', '2026-10-08']
    U.mark_missed(p, log=lambda *a: None)                            # idempotent
    assert len(U.read_jsonl(p.missed)) == 2
    with pytest.raises(U.LockError):
        U.freeze_day(pd.Timestamp('2026-10-07'), U.Paths(U.REPO), out=p, inp={}, enforce_time=False)


def synth_bars(n_t=1150, n_d=320, end='2026-10-06', seed=0):
    rng = np.random.default_rng(seed)
    days = [d for d in pd.bdate_range(end=end, periods=n_d + 10) if U.is_session(d)][-n_d:]
    rows = []
    for i in range(n_t):
        p = np.exp(np.cumsum(rng.normal(0, 0.02, len(days)))) * rng.uniform(6, 200)
        o = p * np.exp(rng.normal(0, 0.01, len(days)))
        h = np.maximum(o, p) * (1 + rng.uniform(0, 0.02, len(days))); l = np.minimum(o, p) * (1 - rng.uniform(0, 0.02, len(days)))
        v = rng.uniform(2e5, 5e6, len(days))
        rows.append(pd.DataFrame({'date': days, 'ticker': f'T{i:04d}', 'open': o, 'high': h, 'low': l, 'close': p, 'volume': v}))
    B = pd.concat(rows, ignore_index=True); B['date'] = B.date.astype('datetime64[ns]')
    return B, days


@pytest.fixture(scope='module')
def synth():
    B, days = synth_bars()
    N = U.next_session(days[-1])
    spy = pd.DataFrame({'spy': np.linspace(400, 500, len(days)), 'vix': 15 + np.sin(np.arange(len(days)))}, index=pd.DatetimeIndex(days))
    E = pd.DataFrame({'edate': [days[-3], N + pd.Timedelta(days=7)], 'ticker': ['T0001', 'T0002'], 'surp': [5.0, np.nan]})
    E['edate'] = E.edate.astype('datetime64[ns]')
    imap = pd.DataFrame({'ticker': B.ticker.unique(), 'Sector': ['S%d' % (i % 7) for i in range(B.ticker.nunique())],
                         'Industry': ['I%d' % (i % 40) for i in range(B.ticker.nunique())]})
    S = pd.DataFrame({'ticker': ['T0005'], 'date': [days[-50]], 'ratio': [2.0]}); S['date'] = S.date.astype('datetime64[ns]')
    return B, days, N, spy, E, imap, S


def test_features_ignore_future_rows(synth):
    B, days, N, spy, E, imap, S = synth
    X1 = U.build_features(B, S, N, imap, spy, E, days)
    planted = B[B.date == days[-1]].copy(); planted['date'] = N; planted[['open', 'high', 'low', 'close']] *= 50
    planted2 = planted.copy(); planted2['date'] = U.next_session(N)
    X2 = U.build_features(pd.concat([B, planted, planted2]), S, N, imap, spy, E, days)
    pd.testing.assert_frame_equal(X1[U.FEATS].reset_index(drop=True), X2[U.FEATS].reset_index(drop=True))
    assert X1.L1000.sum() == 1000 and set(U.FEATS) <= set(X1.columns)
    assert X1.loc[X1.ticker == 'T0001', 'e_days'].iloc[0] == 3 and X1.loc[X1.ticker == 'T0002', 'e_upcoming'].iloc[0] == 1


def test_score_pick_and_restart_identical(synth, tmp_path):
    B, days, N, spy, E, imap, S = synth
    X = U.build_features(B, S, N, imap, spy, E, days)
    L = X[X.L1000 == 1].sort_values('ticker').reset_index(drop=True)
    models, meta = U.load_models(U.HERE)
    F = U.to_f32_frame(L); Sc = U.score_and_pick(F, L.ticker, models)
    assert (Sc.side == 'long').sum() == 50 and (Sc.side == 'short').sum() == 50
    assert Sc.eligible.sum() == 500 and abs(Sc.w.sum()) < 1e-12
    b = U.f32_csv(pd.concat([L[['ticker']], F], axis=1)); (tmp_path / 'f.csv.gz').write_bytes(b)
    F2 = pd.read_csv(tmp_path / 'f.csv.gz', dtype={c: 'float32' for c in U.FEATS})
    S2 = U.score_and_pick(F2, F2.ticker, models)
    assert (S2.d1.values == Sc.d1.values).all() and (S2.side.values == Sc.side.values).all()


def test_models_frozen():
    meta = json.loads((U.HERE / 'models' / 'frozen_meta.json').read_text())
    assert meta['features'] == U.FEATS
    for k, v in meta['models'].items():
        assert U.sha256_file(U.HERE / 'models' / f'{k}.txt') == v['sha256']
        assert v['train_last'] < '2026-10-07'


def test_kill_promote_rules():
    def R(n, mu):
        x = np.full(n, mu) + np.r_[0.001, -0.001] .repeat(n // 2 + 1)[:n]
        return pd.DataFrame({'gross': x, 'net5': x, 'net10': x, 'net15': x, 'hedged_net10': x, 'iwm_oo1': 0 * x, 'rand_med_net10': 0 * x,
                             'net10_ex_best': x, 'turnover': 1.0 + 0 * x})
    assert U.summarize(R(30, 0.0005), {'A': .01}, 30, [])['status'].startswith('collecting')
    assert U.summarize(R(60, -0.0002), {'A': .01}, 60, [])['killed']
    assert U.summarize(R(25, -0.003), {'A': .01}, 25, [])['killed']           # early kill
    s = U.summarize(R(119, 0.003), {'A': .01}, 119, []); assert s['status'].startswith('collecting')
    s = U.summarize(R(120, 0.003), {'A': .01}, 120, []); assert s['status'].startswith('PROMOTION REVIEW')


def test_label_everywhere():
    assert 'failed multiple-testing haircut (t 2.02 vs 3.37)' in U.LABEL
    root = U.HERE
    assert 'failed multiple-testing haircut (t 2.02 vs 3.37)' in (root / 'README.md').read_text()
    page = (U.REPO / 'dashboard' / 'updown-shadow' / 'index.html')
    if page.exists():
        assert 'failed multiple-testing haircut (t 2.02 vs 3.37)' in page.read_text()
