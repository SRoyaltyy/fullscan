"""Event study (out-of-sample period only: prediction days >= 2020-01-01, same as the walk-forward test span).
Every firing counts (no filtering of misses). Return = log close D -> close D+h, D = signal bar.
Excess = event return minus the same-date universe mean; up-rate edge = event up rate minus same-date universe up rate.
t-stats: per-date means, then averaged in non-overlapping h-session blocks so overlapping windows don't inflate them."""
import numpy as np, pandas as pd
from .walkforward import _t, _p2, holm
EVENTS = ['ev_macd_bull_cross', 'ev_macd_bear_cross', 'ev_rsi_up30', 'ev_rsi_dn70', 'ev_squeeze_break_up', 'ev_squeeze_break_dn',
          'ev_bull_div14', 'ev_bear_div14', 'ev_green_extreme', 'ev_red_extreme']
HZ = {'1d': 1, '5d': 5, '21d': 21, '63d': 63}

def study(E, start='2020-01-01', split='2023-01-01'):
    """E: one row per (date N, ticker) in the model universe with ev_* flags and fwd<k> columns."""
    E = E[E.date >= start]; res = []; pv = {}
    for h, k in HZ.items():
        r = E[f'fwd{k}']; ok = r.notna(); X = E[ok].assign(r=r[ok], up=(r[ok] > 0).astype(float))
        g = X.groupby('date')[['r', 'up']].transform('mean'); X = X.assign(xr=X.r - g.r, xu=X.up - g.up)
        days = np.array(sorted(X.date.unique())); blk = pd.Series(np.arange(len(days)) // k, index=days)
        base_up = float(X.groupby('date').up.mean().mean()); base_r = float(X.groupby('date').r.mean().mean())
        for ev in EVENTS:
            S = X[X[ev] > 0]
            if len(S) < 30: res.append(dict(event=ev, h=h, n=int(len(S)))); continue
            d = S.groupby('date')[['r', 'up', 'xr', 'xu']].mean(); d['b'] = blk.loc[d.index].values
            b = d.groupby('b')[['xr', 'xu']].mean()
            half = [float(d[(d.index < split) if i == 0 else (d.index >= split)].xr.mean()) for i in (0, 1)]
            row = dict(event=ev, h=h, n=int(len(S)), n_dates=int(len(d)), n_blocks=int(len(b)),
                       up_rate=float(S.up.mean()), base_up_rate=base_up, up_edge=float(S.xu.mean()),
                       mean_ret=float(S.r.mean()), base_mean_ret=base_r, mean_excess=float(S.xr.mean()),
                       t_excess=_t(b.xr), t_up_edge=_t(b.xu), excess_2020_22=half[0], excess_2023_26=half[1])
            row['p_excess'] = _p2(row['t_excess']); pv[(ev, h)] = row['p_excess']; res.append(row)
    adj = holm({k: v for k, v in pv.items() if np.isfinite(v)})
    for row in res:
        row['p_holm'] = adj.get((row['event'], row['h']))
        row['real'] = bool(row.get('p_holm') is not None and row['p_holm'] < .05 and
                           np.sign(row['excess_2020_22']) == np.sign(row['excess_2023_26']) == np.sign(row['mean_excess']))
    return res

# ---- signal confluence ----------------------------------------------------------------------------------------
FAMILIES = {
    'volume_surge': ['ev_volume_surge'],
    'bb_squeeze': ['ev_squeeze_break_up', 'ev_squeeze_break_dn'],
    'divergence': ['ev_bull_div14', 'ev_bear_div14'],
    'macd_rsi_cross': ['ev_macd_bull_cross', 'ev_macd_bear_cross', 'ev_rsi_up30', 'ev_rsi_dn70'],
    'green_red_extreme': ['ev_green_extreme', 'ev_red_extreme'],
    'options_vix_spike': ['ev_vix_spike'],   # market-wide (free CBOE VIX); no free per-ticker option history
}

def confluence(E, window=5, start='2020-01-01', split='2023-01-01'):
    """E: (date N, ticker) universe rows with ev_* and fwd<k>, sorted by ticker/date. A family counts if any of its
    flags fired on bars D-window+1..D (trailing only). k = number of families active. Compare k=2, 3, 4+ against k=1."""
    E = E.sort_values(['ticker', 'date']).copy(); fam = []
    for f, cols in FAMILIES.items():
        x = (E[cols].max(axis=1) > 0).astype(float)
        E['fam_' + f] = x.groupby(E.ticker).transform(lambda s: s.rolling(window, min_periods=1).max()); fam.append('fam_' + f)
    E['k'] = E[fam].sum(axis=1).clip(upper=4).astype(int)
    E = E[E.date >= start]; res = []; pv = {}
    for h, k in HZ.items():
        r = E[f'fwd{k}']; ok = r.notna(); X = E[ok].assign(r=r[ok], up=(r[ok] > 0).astype(float), ar=r[ok].abs())
        g = X.groupby('date')[['r', 'up', 'ar']].transform('mean'); X = X.assign(xr=X.r - g.r, xu=X.up - g.up, xa=X.ar - g.ar)
        days = np.array(sorted(X.date.unique())); blk = pd.Series(np.arange(len(days)) // k, index=days)
        def blocks(S):
            d = S.groupby('date')[['xr', 'xu', 'xa']].mean(); d['b'] = blk.loc[d.index].values; return d, d.groupby('b')[['xr', 'xu', 'xa']].mean()
        d1, b1 = blocks(X[X.k == 1])
        for lvl in (1, 2, 3, 4):
            S = X[X.k == lvl]
            if len(S) < 30: res.append(dict(k=lvl, h=h, n=int(len(S)))); continue
            d, b = blocks(S); j = b.join(b1, rsuffix='_1', how='inner')
            half = [float(d[(d.index < split) if i == 0 else (d.index >= split)].xr.mean()) for i in (0, 1)]
            row = dict(k=('4+' if lvl == 4 else str(lvl)), h=h, n=int(len(S)), n_blocks=int(len(b)), up_rate=float(S.up.mean()),
                       up_edge=float(S.xu.mean()), mean_excess=float(S.xr.mean()), abs_move_excess=float(S.xa.mean()),
                       t_excess=_t(b.xr), t_abs_move=_t(b.xa), excess_2020_22=half[0], excess_2023_26=half[1])
            if lvl > 1:
                row['excess_minus_single'] = float(S.xr.mean() - X[X.k == 1].xr.mean())
                row['t_vs_single'] = _t(j.xr - j.xr_1); row['t_abs_vs_single'] = _t(j.xa - j.xa_1)
                row['p_vs_single'] = _p2(row['t_vs_single']); row['p_abs_vs_single'] = _p2(row['t_abs_vs_single'])
                pv[(row['k'], h, 'ret')] = row['p_vs_single']; pv[(row['k'], h, 'abs')] = row['p_abs_vs_single']
            res.append(row)
    adj = holm({kk: v for kk, v in pv.items() if np.isfinite(v)})
    for row in res:
        if 'p_vs_single' in row:
            row['p_vs_single_holm'] = adj.get((row['k'], row['h'], 'ret')); row['p_abs_vs_single_holm'] = adj.get((row['k'], row['h'], 'abs'))
            row['beats_single'] = bool(row['p_vs_single_holm'] < .05 and row['excess_2020_22'] * row['excess_2023_26'] > 0)
    return res
