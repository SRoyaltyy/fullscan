"""Leak test for swing_confluence_v1: signals and controls on bar D (used for prediction day N = D+1) must not move when
any bar dated >= N is scrambled; outcomes are never inputs; earnings flags only use scheduled dates; IV is as-of <= D."""
import numpy as np, pandas as pd, pytest
from .build import signals, attach_iv, earnings_flags, CTRL, TARGETS

def _bars(n_t=12, n_d=300, seed=1):
    rng = np.random.default_rng(seed); days = pd.bdate_range('2024-01-01', periods=n_d); rows = []
    for k in range(n_t):
        c = 20 * np.exp(np.cumsum(rng.normal(0, .02, n_d))); o = c * np.exp(rng.normal(0, .01, n_d))
        rows.append(pd.DataFrame({'date': days, 'ticker': f'T{k}', 'open': o, 'high': np.maximum(o, c) * 1.01, 'low': np.minimum(o, c) * .99,
                                  'close': c, 'volume': rng.integers(1e5, 1e6, n_d).astype(float)}))
    return pd.concat(rows).sort_values(['ticker', 'date']).reset_index(drop=True), days

FEATS = ['k', 'l_rv20', 'l_rv60', 'l_absr0']

@pytest.mark.parametrize('i', [200, 260, 299])
def test_future_bars_cannot_move_signals(i):
    b, days = _bars(); N = days[i]; D = days[i - 1]
    s0 = signals(b); b2 = b.copy(); m = b2.date >= N; rng = np.random.default_rng(7)
    for c in ['open', 'high', 'low', 'close', 'volume']: b2.loc[m, c] *= rng.uniform(.3, 3, m.sum())
    s1 = signals(b2)
    a = s0[s0.date == D].set_index('ticker')[FEATS]; z = s1[s1.date == D].set_index('ticker')[FEATS]
    assert a.l_rv20.notna().all(); pd.testing.assert_frame_equal(a, z)

def test_outcomes_not_inputs():
    assert not set(TARGETS) & set(CTRL) and not any(c.startswith(('fwd', 'abs', 'rv', 'sabs')) for c in CTRL)

def test_iv_asof_not_future():
    P = pd.DataFrame({'date': pd.to_datetime(['2024-03-04', '2024-03-05']), 'ticker': 'A', 'hv20': .3})
    IV = pd.DataFrame({'date': pd.to_datetime(['2024-03-01', '2024-03-05', '2024-03-06']), 'ticker': 'A', 'iv_current': [.2, .9, 5.],
                       'iv_year_high': 1., 'iv_year_low': .1, 'hv_current': .3, 'iv_week_ago': .2, 'iv_month_ago': .2})
    o = attach_iv(P, IV).set_index('date')
    assert o.loc['2024-03-04', 'iv'] == .2 and o.loc['2024-03-05', 'iv'] == .9   # row dated D feeds N=D+1, never a later row

def test_earnings_after_close_shifts_to_next_session():
    days = np.array(pd.bdate_range('2024-01-01', periods=10))
    P = pd.DataFrame({'date': days, 'ticker': 'A'})
    E = pd.DataFrame({'ticker': ['A'], 'edate': [str(pd.Timestamp(days[4]).date())], 'amc': [1]})
    o = earnings_flags(P, E, days)
    assert o.earn_next1d.tolist() == [0, 0, 0, 0, 1, 0, 0, 0, 0, 0]   # move session = day 5; flagged on D = day 4
