"""Leak test for updown_v3_preopen: fails if any feature for prediction day N could use information
timestamped at or after 09:25 ET on day N."""
import gzip, json, os
import numpy as np, pandas as pd
import pytest
from .features import build, FEATS, CUTOFF_ET

HERE = os.path.dirname(os.path.abspath(__file__))
ET = 'America/New_York'

def _bars(n_t=40, n_d=320, seed=1):
    rng = np.random.default_rng(seed)
    days = pd.bdate_range('2024-01-01', periods=n_d)
    rows = []
    for k in range(n_t):
        c = 20 * np.exp(np.cumsum(rng.normal(0, .02, n_d)))
        o = c * np.exp(rng.normal(0, .01, n_d))
        rows.append(pd.DataFrame({'date': days, 'ticker': f'T{k}', 'open': o, 'high': np.maximum(o, c) * 1.01,
                                  'low': np.minimum(o, c) * .99, 'close': c, 'volume': rng.integers(1e5, 1e6, n_d).astype(float)}))
    return pd.concat(rows, ignore_index=True), list(days)

def _cutoff(d):
    return pd.Timestamp(f'{pd.Timestamp(d).date()} {CUTOFF_ET}').tz_localize(ET)

def _asof_ts(d):   # a bar dated D is final at the 16:00 ET close of D
    return pd.Timestamp(f'{pd.Timestamp(d).date()} 16:00').tz_localize(ET)

def test_feature_asof_before_cutoff_synthetic():
    b, days = _bars()
    f = build(b, days, top_n=1000, min_price=0)
    assert len(f) > 0
    bad = [(d, a) for d, a in zip(f.date, f.feat_asof) if not _asof_ts(a) < _cutoff(d)]
    assert not bad, f'features timestamped at/after 09:25 ET on prediction day: {bad[:5]}'

@pytest.mark.parametrize('k', [250, 290, 319])
def test_future_bars_cannot_change_features(k):
    """Scramble every bar dated >= N (including day N's own open). Features for day N must not move."""
    b, days = _bars()
    N = days[k]
    f0 = build(b, days, top_n=1000, min_price=0)
    b2 = b.copy(); m = b2.date >= N
    rng = np.random.default_rng(9)
    for c in ['open', 'high', 'low', 'close', 'volume']:
        b2.loc[m, c] = b2.loc[m, c] * rng.uniform(.3, 3, m.sum())
    f1 = build(b2, days, top_n=1000, min_price=0)
    a = f0[f0.date == N].set_index('ticker')[FEATS].sort_index()
    z = f1[f1.date == N].set_index('ticker')[FEATS].sort_index()
    assert len(a) == 40
    pd.testing.assert_frame_equal(a, z)

def test_no_same_day_open_features():
    banned = {'gap', 'gap_z', 'r_gap', 'mkt_gap', 'gap_rel', 'open', 'o2c', 'lprice'}
    assert not banned & set(FEATS)

def test_published_history_asof():
    """Every published day: the newest input timestamp (feature bars, plus any listed source) is before 09:25 ET."""
    p = os.path.join(HERE, 'feature_asof.csv')
    if not os.path.exists(p):
        pytest.skip('no published history yet')
    a = pd.read_csv(p, parse_dates=['date', 'max_feat_asof'])
    assert len(a) > 1000
    bad = a[[not _asof_ts(x) < _cutoff(d) for d, x in zip(a.date, a.max_feat_asof)]]
    assert bad.empty, bad.head()
    assert (a.max_feat_asof < a.date).all()

def test_source_manifest():
    man = json.load(open(os.path.join(HERE, 'SOURCES.json')))
    for s in man['sources']:
        if not s['used_in_model']:
            continue
        # each used source must be available by the given ET time on the prior session (offset -1) or earlier
        assert s['day_offset'] <= -1 or (s['day_offset'] == 0 and s['latest_time_et'] < CUTOFF_ET), s
