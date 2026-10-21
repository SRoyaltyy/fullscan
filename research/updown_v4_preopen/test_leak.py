"""Leak test for updown_v4_preopen: no feature for prediction day N may use information timestamped at/after 09:25 ET on N."""
import json, os
import numpy as np, pandas as pd
import pytest
from research.updown_v3_preopen.features import build as v3_build, CUTOFF_ET
from .blocks import BLOCKS, stock_blocks, option_block, hmm_filtered

HERE = os.path.dirname(os.path.abspath(__file__)); ET = 'America/New_York'
STOCK = BLOCKS['ta'] + BLOCKS['div'] + BLOCKS['gr']

def _bars(n_t=30, n_d=320, seed=1):
    rng = np.random.default_rng(seed); days = pd.bdate_range('2024-01-01', periods=n_d); rows = []
    for k in range(n_t):
        c = 20 * np.exp(np.cumsum(rng.normal(0, .02, n_d))); o = c * np.exp(rng.normal(0, .01, n_d))
        rows.append(pd.DataFrame({'date': days, 'ticker': f'T{k}', 'open': o, 'high': np.maximum(o, c) * 1.01,
                                  'low': np.minimum(o, c) * .99, 'close': c, 'volume': rng.integers(1e5, 1e6, n_d).astype(float)}))
    return pd.concat(rows, ignore_index=True), list(days)

def _cboe(days, seed=2):
    rng = np.random.default_rng(seed)
    return {k: pd.DataFrame({'date': days, 'close': base * np.exp(np.cumsum(rng.normal(0, .03, len(days))))})
            for k, base in [('VIX', 18), ('VIX9D', 17), ('VIX3M', 20), ('SKEW', 130), ('VVIX', 90)]}

def _panel(b, days, cb):
    f = v3_build(b, days, top_n=1000, min_price=0)
    f = f.merge(stock_blocks(b).rename(columns={'date': 'feat_asof'}), on=['feat_asof', 'ticker'], how='left')
    return f.merge(option_block(cb).rename(columns={'date': 'feat_asof'}), on='feat_asof', how='left')

@pytest.mark.parametrize('k', [250, 290, 319])
def test_future_bars_and_cboe_cannot_change_features(k):
    b, days = _bars(); cb = _cboe(days); N = days[k]
    f0 = _panel(b, days, cb)
    b2 = b.copy(); m = b2.date >= N; rng = np.random.default_rng(9)
    for c in ['open', 'high', 'low', 'close', 'volume']: b2.loc[m, c] = b2.loc[m, c] * rng.uniform(.3, 3, m.sum())
    cb2 = {n: x.assign(close=np.where(x.date >= N, x.close * rng.uniform(.3, 3, len(x)), x.close)) for n, x in cb.items()}
    f1 = _panel(b2, days, cb2)
    cols = STOCK + BLOCKS['opt']
    a = f0[f0.date == N].set_index('ticker')[cols].sort_index(); z = f1[f1.date == N].set_index('ticker')[cols].sort_index()
    assert len(a) == 30 and a[BLOCKS['ta']].notna().all().all()
    pd.testing.assert_frame_equal(a, z)
    assert (f0.feat_asof < f0.date).all()

def test_hmm_is_filtered_and_fit_before_window():
    rng = np.random.default_rng(3); d = pd.bdate_range('2019-01-01', periods=700)
    r = np.concatenate([rng.normal(0, .005, 300), rng.normal(0, .02, 100), rng.normal(.001, .008, 300)])
    obs = pd.DataFrame({'r': r}, index=d); obs['v'] = obs.r.rolling(20, min_periods=10).std(); obs = obs.dropna()
    s = obs.index[500]; t = obs.index[600]
    h0 = hmm_filtered(obs, s).set_index('date')
    o2 = obs.copy(); o2.loc[o2.index >= t] *= 5      # change the future (after t, after the fit window)
    h1 = hmm_filtered(o2, s).set_index('date')
    pd.testing.assert_frame_equal(h0[h0.index < t], h1[h1.index < t])   # filtered: probabilities before t unaffected
    o3 = obs.copy(); o3.loc[o3.index >= s] *= 5      # changing data on/after the fit cutoff must not change the fit
    h2 = hmm_filtered(o3, s).set_index('date')
    pd.testing.assert_frame_equal(h0[h0.index < s], h2[h2.index < s])

def test_labels_and_event_outcomes_are_not_features():
    feats = set(sum(BLOCKS.values(), []))
    assert not {c for c in feats if c.startswith('fwd') or c.startswith('ev_') or c.startswith('y_')}
    assert not {'gap', 'open', 'o2c', 'lprice'} & feats

def test_published_history_asof():
    p = os.path.join(HERE, 'feature_asof.csv')
    if not os.path.exists(p): pytest.skip('no published history yet')
    a = pd.read_csv(p, parse_dates=['date', 'max_feat_asof'])
    assert len(a) > 1000 and (a.max_feat_asof < a.date).all()
    cut = lambda d: pd.Timestamp(f'{d.date()} {CUTOFF_ET}').tz_localize(ET)
    close = lambda d: pd.Timestamp(f'{d.date()} 16:15').tz_localize(ET)   # CBOE index close prints by ~16:15 ET
    assert all(close(x) < cut(d) for d, x in zip(a.date, a.max_feat_asof))

def test_source_manifest():
    man = json.load(open(os.path.join(HERE, 'SOURCES.json')))
    for s in man['sources']:
        if s['used_in_model']:
            assert s['day_offset'] <= -1 or (s['day_offset'] == 0 and s['latest_time_et'] < CUTOFF_ET), s
        assert s.get('cost') is not None, s
