"""python -m research.updown_v5_preopen.build <bars...> --raw DIR --out OUT   (offline, box, nice -n 19; checkpointed per variant x horizon)
updown_v5 = v4's base (v3 variant B features, same windows/purge/HistGB+Platt) + five free-source blocks (see sources.py).
Direction only (P(up)). Research only; nothing goes to Webull."""
import json, os, sys, gc, argparse, numpy as np, pandas as pd
from research.updown_v3_preopen.features import build as v3_build, add_labels, HORIZONS
from research.updown_v3_preopen.walkforward import FEATS_B
from research.updown_v4_preopen.walkforward import run, daily, score, holm
from research.swing_confluence_v1.data import load_bars
from research.swing_confluence_v1.build import load_iv
from .sources import BLOCKS, asof, iv_block, short_block, insider_block, ftd_block, regime_block, load_finra, load_ftd, load_form4
TOP_N = 500

def panel(B, days, raw, ck=None):
    if ck and os.path.exists(ck): F = pd.read_parquet(ck)
    else:
        F = add_labels(v3_build(B, days, top_n=TOP_N), B)
        if ck: F.to_parquet(ck)
    for c in F.columns:
        if F[c].dtype == 'float64' and c != '_close': F[c] = F[c].astype('float32')
    T = set(F.ticker)
    F = asof(F, iv_block(load_iv(f'{raw}/iv')), BLOCKS['iv'], 'iv_date', tol='5D')
    F = asof(F, short_block(load_finra(f'{raw}/finra', T)), BLOCKS['short'], 'sv_date', tol='7D')
    F = F.merge(insider_block(load_form4(f'{raw}/form4', T), np.array(days[:-1]), T), on=['feat_asof', 'ticker'], how='left')
    F[BLOCKS['insider']] = F[BLOCKS['insider']].fillna(0)
    FT = ftd_block(load_ftd(f'{raw}/ftd', T), None)
    F = asof(F, FT, ['ftd10'], 'avail', tol='60D')
    v20 = B.assign(v=B.volume).sort_values(['ticker', 'date']); v20['vol20_ftd'] = v20.groupby('ticker').v.transform(lambda s: s.rolling(20, min_periods=10).mean())
    F = F.merge(v20[['date', 'ticker', 'vol20_ftd']].rename(columns={'date': 'feat_asof'}), on=['feat_asof', 'ticker'], how='left')
    F['ftd_ratio'] = F.ftd10.fillna(0) / F.vol20_ftd; F['ftd_log'] = np.log1p(F.ftd10.fillna(0)); F = F.drop(columns=['ftd10', 'vol20_ftd'])
    cb = {}
    for k in ['VIX', 'VIX3M', 'VVIX', 'SKEW']:
        x = pd.read_csv(f'{raw}/cboe/{k}_History.csv' if os.path.exists(f'{raw}/cboe/{k}_History.csv') else f'{raw}/{k}_History.csv'); x.columns = [c.lower() for c in x.columns]
        col = 'close' if 'close' in x.columns else x.columns[-1]
        cb[k] = pd.DataFrame({'date': pd.to_datetime(x.date), 'close': pd.to_numeric(x[col], errors='coerce')})
    fred = {}
    for n in ['BAMLH0A0HYM2', 'T10Y2Y', 'NFCI']:
        x = pd.read_csv(f'{raw}/fred/{n}.csv'); x.columns = ['date', 'v']
        fred[n] = pd.Series(pd.to_numeric(x.v, errors='coerce').values, index=pd.to_datetime(x.date)).dropna()
    F = F.merge(regime_block(cb, fred, np.array(days[:-1])), on='feat_asof', how='left')
    allc = sum(BLOCKS.values(), [])
    F[allc] = F[allc].replace([np.inf, -np.inf], np.nan).astype('float32')
    return F

if __name__ == '__main__':
    a = argparse.ArgumentParser(); a.add_argument('bars', nargs='+'); a.add_argument('--raw'); a.add_argument('--out'); A = a.parse_args()
    OUT = A.out; os.makedirs(OUT, exist_ok=True); pp = f'{OUT}/panel.parquet'
    if os.path.exists(pp): F = pd.read_parquet(pp)
    else:
        B = load_bars(A.bars); days = sorted(B.date.unique()) + [B.date.max() + pd.offsets.BDay(1)]
        F = panel(B, days, A.raw, ck=f'{OUT}/base_panel.parquet'); del B; gc.collect()
        F = F.drop(columns=[c for c in F.columns if c.startswith('_') and c != '_close']); F.to_parquet(pp)
    print('panel', F.shape, {b: round(float(F[F.date >= '2020-01-01'][c].notna().mean()), 3) for b, cs in BLOCKS.items() for c in cs[:1]}, flush=True)
    VARIANTS = {'base_v3': FEATS_B, **{f'+{b}': FEATS_B + BLOCKS[b] for b in BLOCKS}, 'v5_all': FEATS_B + sum(BLOCKS.values(), [])}
    res, log = {}, []
    for h in HORIZONS:
        G = {}
        for name, feats in VARIANTS.items():
            ck = f"{OUT}/g_{name.replace('+', 'p_')}_{h}.parquet"
            if os.path.exists(ck): G[name] = pd.read_parquet(ck)
            else:
                O = run(F, h, feats, log=log if name == 'v5_all' else None, nmax=400_000); G[name] = daily(O, h); G[name].to_parquet(ck); del O; gc.collect()
            res.setdefault(name, {})[h] = score(G[name], h, None if name == 'base_v3' else G['base_v3'])
            print(h, name, {k: round(v, 5) for k, v in res[name][h].items() if isinstance(v, float)}, flush=True)
    for key, fam in [('p_vs_without', [n for n in VARIANTS if n.startswith('+')]), ('p_vs_coin', list(VARIANTS)), ('p_vs_base', list(VARIANTS))]:
        adj = holm({(n, h): res[n][h][key] for n in fam for h in HORIZONS if np.isfinite(res[n][h].get(key, np.nan))})
        for (n, h), p in adj.items(): res[n][h][key + '_holm'] = p
    for n in res:
        for h in res[n]:
            r = res[n][h]
            r['beats_coin'] = bool(r['brier'] < .25 and r['p_vs_coin_holm'] < .05)
            r['beats_base'] = bool(r['brier'] < r['brier_base_rate'] and r['p_vs_base_holm'] < .05)
            if 'p_vs_without_holm' in r: r['block_helps'] = bool(r['delta_vs_without'] < 0 and r['p_vs_without_holm'] < .05)
    cov = {c: float(F[F.date >= '2020-01-01'][c].notna().mean()) for c in sum(BLOCKS.values(), [])}
    json.dump(dict(top_n=TOP_N, variants=VARIANTS, coverage_2020plus=cov, windows=log, results=res), open(f'{OUT}/backtest.json', 'w'), indent=1, default=float)
    print('done', flush=True)
