"""Offline one-off build (box, nice -n 19): python -m research.updown_v4_preopen.build <bars.parquet> <cboe_dir> <out>
bars = v3's bars (bars_all + free yfinance top-ups), cboe_dir = free CBOE index history CSVs."""
import json, os, sys, gc, numpy as np, pandas as pd
from research.updown_v3_preopen.features import build as v3_build, add_labels, HORIZONS
from research.updown_v3_preopen.walkforward import FEATS_B
from .blocks import BLOCKS, stock_blocks, option_block
from .walkforward import run, daily, score, holm
from .events import study, EVENTS
TOP_N = 500
bars_p, cdir, OUT = sys.argv[1:4]; os.makedirs(OUT, exist_ok=True)
B = pd.read_parquet(bars_p); B = B[B.date <= '2026-10-19']
days = sorted(B.date.unique()) + [pd.Timestamp('2026-10-20')]
F = add_labels(v3_build(B, days, top_n=TOP_N), B)
for c in F.columns:
    if F[c].dtype == 'float64' and c != '_close': F[c] = F[c].astype('float32')
S = stock_blocks(B).rename(columns={'date': 'feat_asof'}); del B; gc.collect()
F = F.merge(S, on=['feat_asof', 'ticker'], how='left'); del S; gc.collect()
cb = {}
for k in ['VIX', 'VIX9D', 'VIX3M', 'SKEW', 'VVIX']:
    x = pd.read_csv(f'{cdir}/{k}_History.csv'); x.columns = [c.lower() for c in x.columns]
    col = 'close' if 'close' in x.columns else x.columns[-1]
    cb[k] = pd.DataFrame({'date': pd.to_datetime(x.date), 'close': x[col].astype(float)})
Ob = option_block(cb).rename(columns={'date': 'feat_asof'})
F = F.merge(Ob, on='feat_asof', how='left')
F[BLOCKS['opt']] = F[BLOCKS['opt']].astype('float32')
obs = F.groupby('date').ret1.median().sort_index().to_frame('r'); obs['v'] = obs.r.rolling(20, min_periods=10).std(); obs = obs.dropna()
print('panel', F.shape, F.date.min(), F.date.max(), flush=True)
F.drop(columns=[c for c in F.columns if c.startswith('ev_') or c.startswith('fwd')]).to_parquet(f'{OUT}/panel.parquet')
# event study (no fitting)
ev = study(F[['date', 'ticker'] + EVENTS + ['fwd1', 'fwd5', 'fwd21', 'fwd63']])
json.dump(ev, open(f'{OUT}/events.json', 'w'), indent=1, default=float); print('events done', flush=True)
F = F.drop(columns=[c for c in F.columns if c.startswith('ev_') or c.startswith('fwd') or c.startswith('_') and c != '_close']); gc.collect()
VARIANTS = {'base_v3': FEATS_B, **{f'+{b}': FEATS_B + BLOCKS[b] for b in BLOCKS}, 'v4_all': FEATS_B + sum(BLOCKS.values(), [])}
res, log = {}, []
for h in HORIZONS:
    G = {}
    for name, feats in VARIANTS.items():
        O = run(F, h, feats, obs=obs, log=log if name == 'v4_all' else None)
        G[name] = daily(O, h)
        res.setdefault(name, {})[h] = score(G[name], h, None if name == 'base_v3' else G['base_v3'])
        print(h, name, {k: round(v, 5) for k, v in res[name][h].items() if isinstance(v, float)}, flush=True)
        if name == 'v4_all': O.rename(columns={'y_' + h: 'y'}).assign(h=h).to_parquet(f'{OUT}/preds_{h}.parquet')
        del O; gc.collect()
# multiple-testing penalty (Holm): family 1 = 5 blocks x 4 horizons (with vs without); family 2 = all 7 variants x 4 horizons vs coin; family 3 vs base rate
for key, fam in [('p_vs_without', [n for n in VARIANTS if n.startswith('+')]), ('p_vs_coin', list(VARIANTS)), ('p_vs_base', list(VARIANTS))]:
    adj = holm({(n, h): res[n][h][key] for n in fam for h in HORIZONS if np.isfinite(res[n][h].get(key, np.nan))})
    for (n, h), p in adj.items(): res[n][h][key + '_holm'] = p
for n in res:
    for h in res[n]:
        r = res[n][h]
        r['beats_coin'] = bool(r['brier'] < .25 and r['p_vs_coin_holm'] < .05)
        r['beats_base'] = bool(r['brier'] < r['brier_base_rate'] and r['p_vs_base_holm'] < .05)
        if 'p_vs_without_holm' in r: r['block_helps'] = bool(r['delta_vs_without'] < 0 and r['p_vs_without_holm'] < .05)
json.dump(dict(top_n=TOP_N, variants={k: v for k, v in VARIANTS.items()}, windows=log, results=res), open(f'{OUT}/backtest.json', 'w'), indent=1, default=float)
print('done', flush=True)
