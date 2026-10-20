"""Honest walk-forward for updown_v3_preopen. Yearly refit (quarterly in the final year); each model trains only on
rows whose label was fully realized before the first test day (purge = h sessions)."""
import json, numpy as np, pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier
from .features import FEATS, HORIZONS
MKT = ['mkt_ret1', 'mkt_ret5', 'mkt_ret20', 'breadth1', 'mkt_vol20', 'dow']
FEATS_B = [f for f in FEATS if f not in MKT]   # variant B: stock-level only

def windows(days, first='2020-01-01'):
    starts = list(pd.date_range(first, '2026-01-01', freq='12MS')) + list(pd.date_range('2026-04-01', '2026-10-01', freq='3MS'))
    return [pd.Timestamp(s) for s in starts]

def fit(tr, h, seed=0, nmax=1_200_000, feats=FEATS):
    if len(tr) > nmax: tr = tr.sample(nmax, random_state=seed)
    m = HistGradientBoostingClassifier(max_iter=200, learning_rate=0.05, max_leaf_nodes=31, min_samples_leaf=3000,
                                       l2_regularization=10.0, random_state=seed)
    X = tr[feats].astype('float32')
    for c in X.columns[X.isna().all()]: X[c] = 0.0   # e.g. ret250 before 250 bars exist
    return m.fit(X, tr['y_' + h].values.astype(int))

def run(P, h, log, variant='A'):
    H = HORIZONS[h]; ycol = 'y_' + h
    days = np.array(sorted(P.date.unique())); starts = windows(days); out = []
    for i, s in enumerate(starts):
        e = starts[i + 1] if i + 1 < len(starts) else pd.Timestamp('2100-01-01')
        te = P[(P.date >= s) & (P.date < e)]
        if te.empty: continue
        idx = np.searchsorted(days, np.datetime64(s)); cut = days[max(idx - H, 0)]
        tr = P[(P.date < cut) & P[ycol].notna()]
        # label of a train row dated d ends at close of session d+H-1 < first test day s
        assert tr.date.max() < cut
        if variant == 'A':
            m = fit(tr, h)
            o = te[['date', 'ticker', ycol]].copy(); o['p'] = m.predict_proba(te[FEATS].astype('float32'))[:, 1]
        else:
            # B: stock-level features; Platt shrink fitted on the last 250 training sessions by a model that never saw them
            from sklearn.linear_model import LogisticRegression
            cidx = np.searchsorted(days, np.datetime64(cut)); c0 = days[max(cidx - 250, 0)]; ccut = days[max(cidx - 250 - H, 0)]
            cal = tr[tr.date >= c0]; trA = tr[tr.date < ccut]
            mA = fit(trA, h, feats=FEATS_B)
            zc = np.log(np.clip(mA.predict_proba(cal[FEATS_B].astype('float32'))[:, 1], 1e-4, 1 - 1e-4)); zc = zc - np.log1p(-np.exp(zc))
            pl = LogisticRegression(C=1.0).fit(zc.reshape(-1, 1), cal[ycol].values.astype(int))
            m = fit(tr, h, feats=FEATS_B)
            z = np.log(np.clip(m.predict_proba(te[FEATS_B].astype('float32'))[:, 1], 1e-4, 1 - 1e-4)); z = z - np.log1p(-np.exp(z))
            o = te[['date', 'ticker', ycol]].copy(); o['p'] = pl.predict_proba(z.reshape(-1, 1))[:, 1]
        o['base'] = tr[ycol].mean(); out.append(o)
        log.append(dict(h=h, window=str(s.date()), train_first=str(tr.date.min().date()), train_max=str(tr.date.max().date()),
                        test_first=str(te.date.min().date()), test_last=str(te.date.max().date()),
                        sessions_gap=int(idx - np.searchsorted(days, np.datetime64(tr.date.max()))), n_train=len(tr), n_test=len(te),
                        base=float(tr[ycol].mean()), variant=variant))
        print(log[-1], flush=True)
    return pd.concat(out)

def score(O, h):
    H = HORIZONS[h]; y = 'y_' + h; O = O[O[y].notna()]
    d = O.assign(b_m=(O.p - O[y]) ** 2, b_c=0.25, b_b=(O.base - O[y]) ** 2, hit=((O.p > .5) == (O[y] == 1)).astype(float),
                 hit_b=((O.base > .5) == (O[y] == 1)).astype(float))
    g = d.groupby('date')[['b_m', 'b_c', 'b_b', 'hit', 'hit_b', y]].mean()
    # t-stats on non-overlapping dates only (every H-th date) so overlapping labels don't inflate them
    gs = g.iloc[::H]
    def t(x): return float(x.mean() / (x.std(ddof=1) / np.sqrt(len(x)))) if len(x) > 2 else float('nan')
    return dict(h=h, rows=int(len(O)), dates=int(len(g)), first=str(g.index.min().date()), last=str(g.index.max().date()),
                brier=float(g.b_m.mean()), brier_coin=0.25, brier_base=float(g.b_b.mean()),
                skill_vs_coin=float(1 - g.b_m.mean() / 0.25), skill_vs_base=float(1 - g.b_m.mean() / g.b_b.mean()),
                t_vs_coin=t(gs.b_c - gs.b_m), t_vs_base=t(gs.b_b - gs.b_m), n_indep_dates=int(len(gs)),
                hit=float(g.hit.mean()), hit_base=float(g.hit_b.mean()), up_rate=float(g[y].mean()),
                beats_coin=bool(g.b_m.mean() < 0.25 and t(gs.b_c - gs.b_m) > 2), beats_base=bool(g.b_m.mean() < g.b_b.mean() and t(gs.b_b - gs.b_m) > 2))
