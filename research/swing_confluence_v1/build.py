"""swing_confluence_v1 (research only). python -m research.swing_confluence_v1.build <bars...> --cboe D --earn E.csv --iv DIR --out OUT
Question: when >= 3 of v4's signal families fire within a trailing 5-bar window, is the SIZE of the next move (|log return|,
realized vol) bigger than the base rate, after controlling for prior realized vol, earnings and IV-minus-RV?
Timing: every signal/control is computed on bar D (prior close) and used for prediction day N = D+1; outcomes are D -> D+h."""
import sys, os, json, gc, argparse, numpy as np, pandas as pd
from research.updown_v4_preopen.blocks import stock_blocks
from research.updown_v4_preopen.events import FAMILIES as V4F
from research.updown_v4_preopen.walkforward import _t, _p2, holm
from .data import load_bars, universe
FAMILIES = {k: v for k, v in V4F.items() if k != 'options_vix_spike'}   # the five per-stock families named in the brief
HZ = {'1d': 1, '5d': 5, '21d': 21}
TARGETS = ['abs1d', 'abs5d', 'abs21d', 'rv5d', 'rv21d']
CTRL = ['l_rv20', 'l_rv60', 'l_absr0', 'l_dv', 'earn_next', 'earn_prev5', 'l_vix']
SPLIT, START = '2023-01-01', '2020-01-01'

def signals(B):
    S = stock_blocks(B)
    v20 = B.groupby('ticker').volume.transform(lambda s: s.shift(1).rolling(20, min_periods=15).mean())
    S['ev_volume_surge'] = (B.volume.values >= 2 * v20.values).astype('float32')
    fam = []
    for f, cols in FAMILIES.items():
        x = (S[cols].max(axis=1) > 0).astype('float32')
        S['fam_' + f] = x.groupby(S.ticker).transform(lambda s: s.rolling(5, min_periods=1).max()); fam.append('fam_' + f)
    S['k'] = S[fam].sum(axis=1)
    S['l_vr'] = np.log((B.volume.values + 1) / (v20.values + 1))   # bar-D volume vs prior 20-day avg (robustness control)
    r = np.log(B.close).groupby(B.ticker).diff()
    g = r.groupby(B.ticker)
    S['l_rv20'] = np.log(g.transform(lambda s: s.rolling(20, min_periods=15).std()) * np.sqrt(252) + 1e-4)
    S['l_rv60'] = np.log(g.transform(lambda s: s.rolling(60, min_periods=40).std()) * np.sqrt(252) + 1e-4)
    S['hv20'] = np.exp(S.l_rv20)
    S['l_absr0'] = np.log(r.abs() + 1e-3)
    for h, k in HZ.items():   # outcomes only (never features)
        S['abs' + h] = np.log(S[f'fwd{k}'].abs() + 1e-3)
        S['sabs' + h] = np.expm1(S[f'fwd{k}']).abs()
        if k > 1:
            fr = pd.concat([g.shift(-j) for j in range(1, k + 1)], axis=1)
            S['rv' + h] = np.log(fr.std(axis=1) * np.sqrt(252) + 1e-4)
    return S[['date', 'ticker', 'k', 'l_vr', 'l_rv20', 'l_rv60', 'hv20', 'l_absr0'] + [c for c in S.columns if c.startswith(('abs', 'sabs', 'rv'))] + ['fwd1', 'fwd5', 'fwd21']]

def earnings_flags(P, earn, days):
    """earn: ticker, edate, amc. Move session = edate (pre-market/intraday) or next session (after close).
    earn_next_h: a move session in D+1..D+h (scheduled dates are public weeks ahead); earn_prev5: in D-4..D."""
    E = earn[earn.edate.astype(str).str.len() == 10].copy(); E['edate'] = pd.to_datetime(E.edate)
    pos = np.minimum(np.searchsorted(days, E.edate.values), len(days) - 1)
    pos = pos + ((E.amc.values == 1) & (days[pos] == E.edate.values)).astype(int)
    E['mi'] = pos
    di = pd.Series(np.arange(len(days)), index=days); P = P.copy(); P['di'] = P.date.map(di).values
    em = E.groupby('ticker').mi.apply(lambda s: np.sort(s.values)).to_dict()
    for h, k in HZ.items(): P['earn_next' + h] = 0.0
    P['earn_prev5'] = 0.0
    for t, idx in P.groupby('ticker').groups.items():
        m = em.get(t)
        if m is None or not len(m): continue
        d = P.loc[idx, 'di'].values
        nxt = m[np.minimum(np.searchsorted(m, d, side='right'), len(m) - 1)]; nd = np.where(nxt > d, nxt - d, 10**6)
        prv = m[np.maximum(np.searchsorted(m, d, side='right') - 1, 0)]; pd_ = np.where(prv <= d, d - prv, 10**6)
        for h, k in HZ.items(): P.loc[idx, 'earn_next' + h] = (nd <= k).astype(float)
        P.loc[idx, 'earn_prev5'] = (pd_ <= 4).astype(float)
    P['earn_known'] = P.ticker.isin(set(em)).astype(float)
    return P.drop(columns='di')

def load_iv(d):
    out = []
    for f in sorted(os.listdir(d)):
        if not f.endswith('.json'): continue
        r = json.load(open(f'{d}/{f}'))
        if r: out.append(pd.DataFrame(r))
    X = pd.concat(out).rename(columns={'act_symbol': 'ticker'}); X['date'] = pd.to_datetime(X.date)
    for c in X.columns:
        if c not in ('date', 'ticker'): X[c] = pd.to_numeric(X[c], errors='coerce')
    return X

def attach_iv(P, IV):
    """as-of join: IV row dated <= D (EOD on D or earlier) feeds prediction day D+1. Stale beyond 5 calendar days -> NaN."""
    IV = IV.rename(columns={'date': 'iv_date'}).sort_values('iv_date')
    P = P.sort_values('date')
    P = pd.merge_asof(P, IV[['iv_date', 'ticker', 'iv_current', 'iv_year_high', 'iv_year_low', 'hv_current', 'iv_week_ago', 'iv_month_ago']],
                      left_on='date', right_on='iv_date', by='ticker', direction='backward', tolerance=pd.Timedelta('5D'))
    P['iv'] = P.iv_current
    P['iv_rank'] = (P.iv - P.iv_year_low) / (P.iv_year_high - P.iv_year_low).replace(0, np.nan)
    P['iv_minus_rv'] = P.iv - P.hv20
    P['l_iv'] = np.log(P.iv.clip(lower=0.01))
    return P

def demean(X, by):
    return X - X.groupby(by).transform('mean')

def blocks_t(d, k):
    """d: per-date series; mean over non-overlapping k-session blocks, then t."""
    b = pd.Series(d.values, index=d.index).groupby(np.arange(len(d)) // k).mean(); return _t(b), int(len(b))

def walkforward_resid(P, y, ctrl, days):
    """Yearly windows from 2020; OLS fitted on date-demeaned rows dated before (window start - h sessions). Returns OOS residual
    e = (y - ybar_date) - (X - Xbar_date) @ beta_train, i.e. the part of the move size the controls do not explain. ybar_date is
    the same-date universe mean (the base rate), used only as the benchmark, never as a feature."""
    h = int(''.join(ch for ch in y if ch.isdigit())); Q = P[['date', y] + ctrl].dropna()
    Yd = demean(Q[[y]], Q.date)[y]; Xd = demean(Q[ctrl], Q.date)
    res = pd.Series(np.nan, index=Q.index); log = []
    for yr in range(2020, 2027):
        s = pd.Timestamp(f'{yr}-01-01'); e = pd.Timestamp(f'{yr + 1}-01-01')
        cut = days[max(np.searchsorted(days, np.datetime64(s)) - h, 0)]
        tr = Q.date < cut; te = (Q.date >= s) & (Q.date < e)
        if te.sum() == 0: continue
        beta = np.linalg.lstsq(Xd[tr].values, Yd[tr].values, rcond=None)[0]
        res[te] = Yd[te].values - Xd[te].values @ beta
        log.append(dict(target=y, window=yr, train_max=str(Q.date[tr].max().date()), n_train=int(tr.sum()), n_test=int(te.sum())))
    return res, log

def study(P, days, tag, ctrl_extra=()):
    rows, pv, logs = [], {}, []
    for y in TARGETS:
        h = y[-3:] if y.endswith('21d') else y[-2:]; k = HZ[h]; ec = 'earn_next' + h
        ctrl = [c if c != 'earn_next' else ec for c in CTRL] + list(ctrl_extra)
        Q = P[(P.date >= '2019-01-01') & P[y].notna()]
        raw = demean(Q[[y]], Q.date)[y]
        cres, lg = walkforward_resid(Q, y, ctrl, days); logs += lg
        Q = Q.assign(raw=raw, ctl=cres); Q = Q[Q.date >= START]
        for lvl, m in (('3+', Q.k >= 3), ('4+', Q.k >= 4), ('1', Q.k == 1)):
            for kind in ('raw', 'ctl'):
                S = Q[m & Q[kind].notna()]
                if len(S) < 30: continue
                d = S.groupby('date')[kind].mean(); t, nb = blocks_t(d, k)
                half = [float(d[d.index < SPLIT].mean()), float(d[d.index >= SPLIT].mean())]
                th = [blocks_t(d[d.index < SPLIT], k)[0], blocks_t(d[d.index >= SPLIT], k)[0]]
                r = dict(spec=tag, target=y, k=lvl, kind=kind, n=int(len(S)), n_dates=int(len(d)), n_blocks=nb,
                         excess_log=float(d.mean()), excess_pct=float(np.expm1(d.mean())), t=t, p=_p2(t),
                         excess_2020_22=half[0], excess_2023_26=half[1], t_2020_22=th[0], t_2023_26=th[1])
                if 'sabs' + h in S and y.startswith('abs'):
                    r['mean_abs_move'] = float(S['sabs' + h].mean()); r['universe_abs_move'] = float(Q['sabs' + h].mean())
                rows.append(r)
                if lvl != '1': pv[(tag, y, lvl, kind)] = r['p']
    return rows, pv, logs

def straddle(P, start=START):
    """Paper ATM straddle per $ of spot: payoff |S_{D+h}/S_D - 1| minus cost 0.798 * IV * sqrt(h/252) (Brenner-Subrahmanyam),
    IV = DoltHub iv_current dated <= D. No bid/ask, no vega path, no early exercise: an upper bound on any real edge."""
    out = []; Q = P[(P.date >= start) & P.iv.notna()]
    for h, k in HZ.items():
        X = Q[Q['sabs' + h].notna()].assign(pnl=lambda x: x['sabs' + h] - 0.798 * x.iv * np.sqrt(k / 252))
        X = X.assign(xp=demean(X[['pnl']], X.date).pnl)
        for lvl, m in (('all', X.k >= 0), ('3+', X.k >= 3), ('4+', X.k >= 4)):
            S = X[m]; d = S.groupby('date').pnl.mean(); dx = S.groupby('date').xp.mean()
            t, nb = blocks_t(d, k); tx, _ = blocks_t(dx, k)
            out.append(dict(h=h, k=lvl, n=int(len(S)), mean_pnl=float(S.pnl.mean()), t_pnl=t, p_pnl=_p2(t),
                            pnl_vs_same_day_names=float(S.xp.mean()), t_vs_same_day=tx, p_vs_same_day=_p2(tx),
                            pnl_2020_22=float(d[d.index < SPLIT].mean()), pnl_2023_26=float(d[d.index >= SPLIT].mean())))
    return out

if __name__ == '__main__':
    a = argparse.ArgumentParser(); a.add_argument('bars', nargs='+'); a.add_argument('--cboe'); a.add_argument('--earn'); a.add_argument('--iv'); a.add_argument('--out')
    A = a.parse_args(); os.makedirs(A.out, exist_ok=True)
    B = load_bars(A.bars); U = universe(B); U = U[U.date >= '2018-10-01']
    B = B[B.ticker.isin(set(U.ticker))].reset_index(drop=True)
    days = np.array(sorted(B.date.unique()))
    ck = f'{A.out}/sig.parquet'
    if os.path.exists(ck): S = pd.read_parquet(ck)
    else: S = signals(B); S.to_parquet(ck)
    del B; gc.collect()
    P = U.merge(S, on=['date', 'ticker'], how='left'); P['l_dv'] = np.log(P.dv); del S; gc.collect()
    vx = pd.read_csv(f'{A.cboe}/VIX_History.csv'); vx.columns = [c.lower() for c in vx.columns]
    P['l_vix'] = np.log(P.date.map(pd.Series(vx.close.astype(float).values, index=pd.to_datetime(vx.date))).astype(float))
    P = earnings_flags(P, pd.read_csv(A.earn), days)
    P = attach_iv(P, load_iv(A.iv))
    print('panel', P.shape, 'iv coverage 2020+', float(P[P.date >= START].iv.notna().mean()), flush=True)
    rows, pv, logs = study(P, days, 'base')
    r5, p5, _ = study(P, days, 'ctrl_plus_volume_ratio', ctrl_extra=('l_vr',)); rows += r5; pv.update(p5)
    Pk = P[P.earn_known > 0]; r2, p2, _ = study(Pk[(Pk.earn_next1d == 0) & (Pk.earn_next5d == 0) & (Pk.earn_next21d == 0) & (Pk.earn_prev5 == 0)], days, 'no_earnings_window'); rows += r2; pv.update(p2)
    Pi = P[P.iv.notna()]
    r3, p3, _ = study(Pi, days, 'iv_subset_ctrl_iv_minus_rv', ctrl_extra=('iv_minus_rv', 'l_iv', 'iv_rank')); rows += r3; pv.update(p3)
    r4, p4, _ = study(Pi, days, 'iv_subset_no_iv_ctrl'); rows += r4; pv.update(p4)
    # IV minus RV as a predictor of move size on its own (walk-forward slope sign, Holm in the same family)
    ivp = []
    for y in TARGETS:
        Q = Pi[(Pi.date >= START) & Pi[y].notna() & Pi.iv_minus_rv.notna()]
        Q = Q.assign(yd=demean(Q[[y]], Q.date)[y], xd=demean(Q[['iv_minus_rv']], Q.date).iv_minus_rv)
        d = Q.groupby('date').apply(lambda g: np.corrcoef(g.xd, g.yd)[0, 1] if len(g) > 20 else np.nan).dropna()
        k = HZ[y[-3:] if y.endswith('21d') else y[-2:]]; t, nb = blocks_t(d, k)
        ivp.append(dict(target=y, mean_daily_corr=float(d.mean()), t=t, p=_p2(t), corr_2020_22=float(d[d.index < SPLIT].mean()), corr_2023_26=float(d[d.index >= SPLIT].mean())))
        pv[('iv_minus_rv_predictor', y, '-', 'corr')] = ivp[-1]['p']
    adj = holm({k: v for k, v in pv.items() if np.isfinite(v)})
    for r in rows:
        r['p_holm'] = adj.get((r['spec'], r['target'], r['k'], r['kind']))
        r['real'] = bool(r['p_holm'] is not None and r['p_holm'] < .05 and r['excess_log'] > 0 and r['excess_2020_22'] > 0 and r['excess_2023_26'] > 0)
    for r in ivp: r['p_holm'] = adj[('iv_minus_rv_predictor', r['target'], '-', 'corr')]
    real_ctl = [r for r in rows if r['spec'] == 'base' and r['kind'] == 'ctl' and r['real']]
    st = straddle(P) if real_ctl else []
    json.dump(dict(families=FAMILIES, controls=CTRL, n_tests_holm=len(adj), results=rows, iv_minus_rv=ivp, straddle=st, windows=logs,
                   iv_coverage_2020plus=float(P[P.date >= START].iv.notna().mean())), open(f'{A.out}/results.json', 'w'), indent=1, default=float)
    if not real_ctl: json.dump(dict(note='not run: no controlled result survived', diag=straddle(P)), open(f'{A.out}/straddle_diag.json', 'w'), indent=1, default=float)
    print('done', flush=True)
