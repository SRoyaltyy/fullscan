"""updown_v3_preopen features. Every feature for prediction day N uses only bars dated < N
(i.e. known at the prior close, 16:00 ET on N-1). Nothing from day N's open or later."""
import numpy as np, pandas as pd

FEATS = ['ret1','ret3','ret5','ret10','ret20','ret60','ret120','ret250','vol20','vol60','range_prev','clv_prev',
         'o2c_prev','c2o_prev','ldv20','rvol1','rvol5','pos20','dd250','green10',
         'mkt_ret1','mkt_ret5','mkt_ret20','breadth1','mkt_vol20','r_ret1','r_ret5','r_ret20','r_vol20','r_rvol1','dow']
HORIZONS = {'1d': 1, '5d': 5, '21d': 21, '63d': 63}
CUTOFF_ET = '09:25'

def build(bars, trading_days=None, top_n=1500, min_price=5.0):
    """bars: date,ticker,open,high,low,close,volume (one row per ticker-session).
    trading_days: sorted session dates to predict for (may extend 1 past last bar = today's pre-open).
    Returns one row per (date=N, ticker) with features, feat_asof (= last bar date used) and labels y_<h>."""
    b = bars.sort_values(['ticker', 'date']).reset_index(drop=True)
    days = np.array(sorted(b.date.unique())) if trading_days is None else np.array(sorted(trading_days))
    # reindex each ticker on the session calendar so shift(k) == k sessions
    g = b.groupby('ticker', sort=False)
    c = b.close; lc = np.log(c.where(c > 0))
    r = lc - g['close'].shift(1).pipe(lambda s: np.log(s.where(s > 0)))
    f = pd.DataFrame({'date': b.date, 'ticker': b.ticker})
    # quantities "as of close of this bar"; shifted to the NEXT session below
    for k in [1, 3, 5, 10, 20, 60, 120, 250]:
        f[f'ret{k}'] = lc - lc.groupby(b.ticker).shift(k)
    rr = r.groupby(b.ticker)
    f['vol20'] = rr.transform(lambda s: s.rolling(20, min_periods=15).std())
    f['vol60'] = rr.transform(lambda s: s.rolling(60, min_periods=40).std())
    f['range_prev'] = (b.high - b.low) / c
    f['clv_prev'] = ((c - b.low) - (b.high - c)) / (b.high - b.low).replace(0, np.nan)
    f['o2c_prev'] = np.log(c / b.open)
    f['c2o_prev'] = np.log(b.open / g['close'].shift(1))
    dv = (c * b.volume).groupby(b.ticker)
    dv20 = dv.transform(lambda s: s.rolling(20, min_periods=15).mean())
    f['ldv20'] = np.log1p(dv20)
    f['rvol1'] = np.log1p(c * b.volume) - f['ldv20']
    f['rvol5'] = np.log1p(dv.transform(lambda s: s.rolling(5).mean())) - f['ldv20']
    hi20 = b.high.groupby(b.ticker).transform(lambda s: s.rolling(20, min_periods=15).max())
    lo20 = b.low.groupby(b.ticker).transform(lambda s: s.rolling(20, min_periods=15).min())
    f['pos20'] = (c - lo20) / (hi20 - lo20).replace(0, np.nan)
    f['dd250'] = np.log(c / c.groupby(b.ticker).transform(lambda s: s.rolling(250, min_periods=60).max()))
    f['green10'] = (r > 0).astype(float).groupby(b.ticker).transform(lambda s: s.rolling(10).mean())
    f['_close'] = c
    # map each bar (dated D) to the next session N > D: features for N use bar D only
    pos = np.searchsorted(days, f.date.values, side='right')
    ok = pos < len(days)
    f = f[ok].copy(); f['feat_asof'] = f.date; f['date'] = days[pos[ok]]
    # guard: require the bar to be the immediately preceding session (no stale carry over gaps)
    prev_idx = np.searchsorted(days, f.date.values) - 1
    f = f[(prev_idx >= 0) & (days[np.clip(prev_idx, 0, None)] == f.feat_asof.values)]
    # market (date-level) features: cross-sectional stats of the already-lagged columns
    m = f.groupby('date').agg(mkt_ret1=('ret1', 'median'), mkt_ret5=('ret5', 'median'), mkt_ret20=('ret20', 'median'),
                              breadth1=('ret1', lambda s: (s > 0).mean()), mkt_vol20=('vol20', 'median'))
    f = f.merge(m, left_on='date', right_index=True, how='left')
    f = f[(f._close >= min_price) & f.ldv20.notna()]
    f['dvrank'] = f.groupby('date').ldv20.rank(ascending=False, method='first')
    f = f[f.dvrank <= top_n].drop(columns='dvrank')
    for col in ['ret1', 'ret5', 'ret20', 'vol20', 'rvol1']:
        f['r_' + col] = f.groupby('date')[col].rank(pct=True)
    f['dow'] = pd.to_datetime(f.date).dt.dayofweek.astype(float)
    return f.reset_index(drop=True)

def add_labels(f, bars, horizons=HORIZONS):
    """y_h = 1 if close at session N+h-1 > close at N-1 (the last known close). NaN if not yet realized."""
    b = bars[['date', 'ticker', 'close']].sort_values(['ticker', 'date'])
    for name, h in horizons.items():
        fut = b.groupby('ticker').close.shift(-(h - 1))
        lab = pd.DataFrame({'date': b.date, 'ticker': b.ticker, 'fc': fut.values})
        f = f.merge(lab.rename(columns={'fc': 'fc_' + name}), on=['date', 'ticker'], how='left')
        f['y_' + name] = np.where(f['fc_' + name].notna(), (f['fc_' + name] > f._close).astype(float), np.nan)
        f = f.drop(columns='fc_' + name)
    return f
