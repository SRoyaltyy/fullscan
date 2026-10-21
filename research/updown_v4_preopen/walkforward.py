"""updown_v4_preopen walk-forward: identical windows, purge, model and Platt step as v3 variant B (the published v3),
with optional extra feature blocks. The HMM block is refit at every window start on data before it (see blocks.hmm_filtered)."""
import numpy as np, pandas as pd
from math import erf, sqrt
from sklearn.linear_model import LogisticRegression
from research.updown_v3_preopen.walkforward import windows, fit, FEATS_B
from research.updown_v3_preopen.features import HORIZONS
from .blocks import BLOCKS, hmm_filtered

def _logit(p):
    p = np.clip(p, 1e-4, 1 - 1e-4); return np.log(p) - np.log1p(-p)

def run(P, h, feats, obs=None, log=None, nmax=600_000):
    H = HORIZONS[h]; y = 'y_' + h
    days = np.array(sorted(P.date.unique())); starts = windows(days); out = []
    use_hmm = any(f in BLOCKS['hmm'] for f in feats)
    for i, s in enumerate(starts):
        e = starts[i + 1] if i + 1 < len(starts) else pd.Timestamp('2100-01-01')
        te = P[(P.date >= s) & (P.date < e)]
        if te.empty: continue
        idx = np.searchsorted(days, np.datetime64(s)); cut = days[max(idx - H, 0)]
        tr = P[(P.date < cut) & P[y].notna()]
        assert tr.date.max() < cut
        if use_hmm:   # fit on market obs dated before the test window only; filtered probabilities
            Hm = hmm_filtered(obs, s).set_index('date')
            tr = tr.join(Hm, on='date'); te = te.join(Hm, on='date')
        cidx = np.searchsorted(days, np.datetime64(cut)); c0 = days[max(cidx - 250, 0)]; ccut = days[max(cidx - 250 - H, 0)]
        cal = tr[tr.date >= c0]; trA = tr[tr.date < ccut]
        mA = fit(trA, h, nmax=nmax, feats=feats)
        pl = LogisticRegression(C=1.0).fit(_logit(mA.predict_proba(cal[feats].astype('float32'))[:, 1]).reshape(-1, 1), cal[y].values.astype(int))
        m = fit(tr, h, nmax=nmax, feats=feats)
        o = te[['date', 'ticker', y]].copy()
        o['p'] = pl.predict_proba(_logit(m.predict_proba(te[feats].astype('float32'))[:, 1]).reshape(-1, 1))[:, 1]
        o['base'] = tr[y].mean(); out.append(o)
        if log is not None:
            log.append(dict(h=h, window=str(s.date()), train_max=str(tr.date.max().date()), test_first=str(te.date.min().date()),
                            test_last=str(te.date.max().date()), n_train=len(tr), n_test=len(te), base=float(tr[y].mean())))
    return pd.concat(out)

def _p2(t):
    return float(2 * (1 - 0.5 * (1 + erf(abs(t) / sqrt(2))))) if np.isfinite(t) else float('nan')

def _t(x):
    x = np.asarray(x, float); return float(x.mean() / (x.std(ddof=1) / np.sqrt(len(x)))) if len(x) > 2 else float('nan')

def daily(O, h):
    y = 'y_' + h; O = O[O[y].notna()]
    d = O.assign(b_m=(O.p - O[y]) ** 2, b_b=(O.base - O[y]) ** 2)
    return d.groupby('date')[['b_m', 'b_b']].mean()

def score(g, h, g_base=None):
    """g: per-date Brier frame. t-stats on every H-th date (non-overlapping labels)."""
    H = HORIZONS[h]; gs = g.iloc[::H]
    r = dict(dates=int(len(g)), n_indep=int(len(gs)), brier=float(g.b_m.mean()), brier_base_rate=float(g.b_b.mean()),
             skill_vs_coin=float(1 - g.b_m.mean() / .25), skill_vs_base=float(1 - g.b_m.mean() / g.b_b.mean()),
             t_vs_coin=_t(.25 - gs.b_m), t_vs_base=_t(gs.b_b - gs.b_m))
    r['p_vs_coin'] = _p2(r['t_vs_coin']); r['p_vs_base'] = _p2(r['t_vs_base'])
    if g_base is not None:
        j = g[['b_m']].join(g_base[['b_m']], rsuffix='_0', how='inner').iloc[::H]
        r['brier_without'] = float(g_base.b_m.mean()); r['delta_vs_without'] = float(g.b_m.mean() - g_base.b_m.mean())
        r['t_vs_without'] = _t(j.b_m_0 - j.b_m); r['p_vs_without'] = _p2(r['t_vs_without'])
    return r

def holm(pvals):
    """Holm step-down adjusted p-values (dict key -> p)."""
    k = sorted(pvals, key=lambda x: pvals[x]); m = len(k); adj = {}; run = 0.0
    for i, key in enumerate(k):
        run = max(run, min(1.0, (m - i) * pvals[key])); adj[key] = run
    return adj
