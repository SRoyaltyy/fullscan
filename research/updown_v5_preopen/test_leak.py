"""Leak test for updown_v5_preopen free-source blocks: nothing public at/after 09:25 ET on N may reach features for N
(feat_asof = D = N-1)."""
import numpy as np, pandas as pd
from .sources import BLOCKS, asof, iv_block, short_block, insider_block, ftd_block, regime_block

D = pd.bdate_range('2024-01-01', periods=80)

def _P():
    return pd.DataFrame({'feat_asof': D, 'ticker': 'A', 'date': list(D[1:]) + [D[-1] + pd.offsets.BDay(1)]})

def test_iv_short_asof_rows_dated_after_D_ignored():
    IV = pd.DataFrame({'date': D, 'ticker': 'A', 'iv_current': np.linspace(.2, .4, 80), 'iv_week_ago': .2, 'iv_month_ago': .2,
                       'iv_year_high': .5, 'iv_year_low': .1, 'hv_current': .25})
    k = 50; f0 = asof(_P(), iv_block(IV), BLOCKS['iv'], 'iv_date', tol='5D')
    IV2 = IV.copy(); IV2.loc[IV2.date > D[k], 'iv_current'] = 9.0
    f1 = asof(_P(), iv_block(IV2), BLOCKS['iv'], 'iv_date', tol='5D')
    pd.testing.assert_frame_equal(f0[f0.feat_asof <= D[k]].reset_index(drop=True), f1[f1.feat_asof <= D[k]].reset_index(drop=True))
    F = pd.DataFrame({'date': D, 'ticker': 'A', 'short': np.arange(80.), 'total': 200.})
    s0 = asof(_P(), short_block(F), BLOCKS['short'], 'sv_date'); F2 = F.copy(); F2.loc[F2.date > D[k], 'short'] = 199
    s1 = asof(_P(), short_block(F2), BLOCKS['short'], 'sv_date')
    pd.testing.assert_frame_equal(s0[s0.feat_asof <= D[k]].reset_index(drop=True), s1[s1.feat_asof <= D[k]].reset_index(drop=True))

def test_insider_counts_only_filing_date_le_D():
    T = pd.DataFrame({'ticker': ['A'], 'filing_date': [D[40]], 'code': ['P']})
    X = insider_block(T, np.array(D), {'A'}).set_index('feat_asof')
    assert X.loc[D[39], 'ins_buy21'] == 0 and X.loc[D[40], 'ins_buy21'] == 1 and X.loc[D[61], 'ins_buy21'] == 0

def test_ftd_lag_35_days():
    FT = pd.DataFrame({'settle_date': [D[10]], 'ticker': ['A'], 'qty': [1000.]})
    X = ftd_block(FT, None); o = asof(_P(), X, ['ftd10'], 'avail', tol='60D').set_index('feat_asof')
    assert o.loc[o.index < D[10] + pd.Timedelta('35D'), 'ftd10'].isna().all()

def test_regime_fred_lag():
    cb = {k: pd.DataFrame({'date': D, 'close': 20.}) for k in ['VIX', 'VIX3M', 'VVIX', 'SKEW']}
    fred = {n: pd.Series(np.arange(80.), index=D) for n in ['BAMLH0A0HYM2', 'T10Y2Y']}
    fred['NFCI'] = pd.Series(np.arange(80.), index=D)
    o = regime_block(cb, fred, np.array(D)).set_index('feat_asof')
    assert o.loc[D[50], 'hy_spread'] == 48   # FRED value dated D-2 sessions
    assert o.loc[D[50], 'nfci'] <= 45        # weekly NFCI lagged 7 calendar days

def test_labels_not_features():
    f = sum(BLOCKS.values(), []); assert not [c for c in f if c.startswith(('y_', 'fwd', 'ret_fwd'))]
