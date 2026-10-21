"""updown_v4_preopen feature blocks. Every column is computed on the bar dated D (as of its 16:00 ET close) and
is joined to prediction day N = next session after D, exactly like v3's features. Nothing from day N."""
import numpy as np, pandas as pd

BLOCKS = {
    'ta': ['rsi14', 'mfi14', 'macd_n', 'macds_n', 'macdh_n', 'bb_pctb', 'bb_width'],
    'opt': ['vix', 'vix_chg5', 'vix9d_vix', 'vix_vix3m', 'skew_idx', 'vvix'],
    'hmm': ['hmm_calm', 'hmm_trend', 'hmm_stress'],
    'div': [f'{s}div_{ind}_{w}' for s in ('bull', 'bear') for ind in ('rsi', 'mfi', 'macd') for w in (14, 20)] + ['div_net14', 'div_net20'],
    'gr': ['up_share5', 'up_share10', 'up_share20', 'udvol5', 'udvol10', 'udvol20'],
}

def _rsi(c, n=14):
    d = c.diff(); up = d.clip(lower=0).ewm(alpha=1 / n, min_periods=n, adjust=False).mean()
    dn = (-d.clip(upper=0)).ewm(alpha=1 / n, min_periods=n, adjust=False).mean()
    return 100 - 100 / (1 + up / dn.replace(0, np.nan))

def _per_ticker(g):
    c, h, l, v = g.close, g.high, g.low, g.volume
    o = pd.DataFrame(index=g.index)
    o['rsi14'] = _rsi(c)
    tp = (h + l + c) / 3; mf = tp * v; dtp = tp.diff()
    pos = mf.where(dtp > 0, 0.0).rolling(14, min_periods=14).sum(); neg = mf.where(dtp < 0, 0.0).rolling(14, min_periods=14).sum()
    o['mfi14'] = 100 - 100 / (1 + pos / neg.replace(0, np.nan))
    e12 = c.ewm(span=12, adjust=False, min_periods=12).mean(); e26 = c.ewm(span=26, adjust=False, min_periods=26).mean()
    macd = e12 - e26; sig = macd.ewm(span=9, adjust=False, min_periods=9).mean()
    o['macd_n'] = macd / c; o['macds_n'] = sig / c; o['macdh_n'] = (macd - sig) / c
    m20 = c.rolling(20, min_periods=20).mean(); s20 = c.rolling(20, min_periods=20).std()
    up, lo = m20 + 2 * s20, m20 - 2 * s20
    o['bb_pctb'] = (c - lo) / (up - lo).replace(0, np.nan); o['bb_width'] = (up - lo) / m20
    # divergence: price makes a new w-day closing high (low) but the indicator stays below (above) its prior w-day max (min)
    for w in (14, 20):
        phi = c >= c.rolling(w, min_periods=w).max(); plo = c <= c.rolling(w, min_periods=w).min()
        for name, x in (('rsi', o.rsi14), ('mfi', o.mfi14), ('macd', macd)):
            o[f'beardiv_{name}_{w}'] = (phi & (x < x.shift(1).rolling(w - 1, min_periods=w - 1).max())).astype(float)
            o[f'bulldiv_{name}_{w}'] = (plo & (x > x.shift(1).rolling(w - 1, min_periods=w - 1).min())).astype(float)
        o[f'div_net{w}'] = sum(o[f'bulldiv_{n}_{w}'] for n in ('rsi', 'mfi', 'macd')) - sum(o[f'beardiv_{n}_{w}'] for n in ('rsi', 'mfi', 'macd'))
    r = c.diff()
    for w in (5, 10, 20):
        o[f'up_share{w}'] = (r > 0).astype(float).rolling(w, min_periods=w).mean()
        uv = v.where(r > 0, 0.0).rolling(w, min_periods=w).sum(); dv = v.where(r < 0, 0.0).rolling(w, min_periods=w).sum()
        o[f'udvol{w}'] = np.log((uv + 1) / (dv + 1))
    # event flags (same bar-D timing; thresholds fixed a priori, nothing fitted)
    rsi = o.rsi14; on = lambda x: (x & ~x.shift(1, fill_value=False))
    o['ev_macd_bull_cross'] = (macd > sig) & (macd.shift(1) <= sig.shift(1))
    o['ev_macd_bear_cross'] = (macd < sig) & (macd.shift(1) >= sig.shift(1))
    o['ev_rsi_up30'] = (rsi > 30) & (rsi.shift(1) <= 30)
    o['ev_rsi_dn70'] = (rsi < 70) & (rsi.shift(1) >= 70)
    bw = o.bb_width; sq = bw <= bw.rolling(120, min_periods=60).quantile(0.2)
    recent = sq.shift(1).astype(float).rolling(5, min_periods=1).max() > 0
    o['ev_squeeze_break_up'] = recent & (c > up) & (c.shift(1) <= up.shift(1))
    o['ev_squeeze_break_dn'] = recent & (c < lo) & (c.shift(1) >= lo.shift(1))
    o['ev_bull_div14'] = on((o[[f'bulldiv_{n}_14' for n in ('rsi', 'mfi', 'macd')]].max(axis=1) > 0))
    o['ev_bear_div14'] = on((o[[f'beardiv_{n}_14' for n in ('rsi', 'mfi', 'macd')]].max(axis=1) > 0))
    o['ev_green_extreme'] = on(o.up_share20 >= 0.75)
    o['ev_red_extreme'] = on(o.up_share20 <= 0.25)
    lc = np.log(c)
    for k in (1, 5, 21, 63):
        o[f'fwd{k}'] = lc.shift(-k) - lc   # forward return close D -> close D+k (labels only, never a feature)
    return o

def stock_blocks(bars):
    """bars: date,ticker,open,high,low,close,volume. Returns date (=bar date D),ticker + per-stock block columns."""
    b = bars.sort_values(['ticker', 'date']).reset_index(drop=True)
    parts = [_per_ticker(g) for _, g in b.groupby('ticker', sort=False)]
    o = pd.concat(parts).loc[b.index]
    o.insert(0, 'ticker', b.ticker.values); o.insert(0, 'date', b.date.values)
    for col in o.columns[2:]:
        o[col] = o[col].astype('float32')
    return o

def option_block(cboe):
    """cboe: dict name -> DataFrame(date, close) of CBOE index closes (VIX, VIX9D, VIX3M, SKEW, VVIX), dated D (final ~16:15 ET on D)."""
    s = {k: v.set_index('date').close for k, v in cboe.items()}
    d = pd.DataFrame(s).sort_index()
    o = pd.DataFrame(index=d.index)
    o['vix'] = d.VIX; o['vix_chg5'] = np.log(d.VIX / d.VIX.shift(5))
    o['vix9d_vix'] = d.VIX9D / d.VIX; o['vix_vix3m'] = d.VIX / d.VIX3M; o['skew_idx'] = d.SKEW; o['vvix'] = d.VVIX
    return o.rename_axis('date').reset_index()

def mkt_obs(stock_panel_b):
    """date-level observations for the HMM, from bar-D cross-section: median daily log return and its 20d vol."""
    m = stock_panel_b.groupby('date').r1.median().sort_index()
    return pd.DataFrame({'r': m, 'v': m.rolling(20, min_periods=10).std()}).dropna()

def hmm_filtered(obs, fit_end, seed=0):
    """Fit a 3-state Gaussian HMM on obs dated < fit_end only; return FILTERED (forward-only) state probabilities for
    every date in obs: P(state_D | obs up to and including D). No backward pass, so no look-ahead.
    States labelled by mean vol: lowest = calm, highest = stress, middle = trend."""
    from hmmlearn.hmm import GaussianHMM
    from scipy.special import logsumexp
    X = obs[['r', 'v']].values * np.array([100.0, 100.0])
    tr = X[obs.index < fit_end]
    m = GaussianHMM(n_components=3, covariance_type='full', n_iter=200, random_state=seed).fit(tr)
    ll = m._compute_log_likelihood(X); la = np.log(m.transmat_ + 1e-300); a = np.log(m.startprob_ + 1e-300) + ll[0]
    out = np.empty_like(ll); out[0] = a - logsumexp(a)
    for t in range(1, len(X)):
        a = logsumexp(out[t - 1][:, None] + la, axis=0) + ll[t]; out[t] = a - logsumexp(a)
    order = np.argsort(m.means_[:, 1])
    P = np.exp(out)[:, order]
    return pd.DataFrame({'date': obs.index, 'hmm_calm': P[:, 0], 'hmm_trend': P[:, 1], 'hmm_stress': P[:, 2]})
