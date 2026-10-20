"""Write research/updown_v3_preopen/ history + dashboard/updown-v3/data from out/ (offline)."""
import gzip, json, os, sys, numpy as np, pandas as pd
src, R, D = sys.argv[1], sys.argv[2], sys.argv[3]
os.makedirs(f'{R}/history', exist_ok=True); os.makedirs(f'{D}/data/t', exist_ok=True)
bt = json.load(open(f'{src}/backtest.json')); P = pd.read_parquet(f'{src}/preds.parquet')
F = pd.read_parquet(f'{src}/panel.parquet', columns=['date', 'feat_asof'])
HZ = [h for h in ['1d', '5d', '21d', '63d'] if h in bt['results']]
W = P.pivot_table(index=['date', 'ticker'], columns='h', values=['p', 'y'], aggfunc='first')
W.columns = [f'{a}_{b}' for a, b in W.columns]; W = W.reset_index().sort_values(['date', 'ticker'])
# research history: one csv.gz per year (p = walk-forward OOS P(up); y = realized, blank if unknown)
for yr, g in W.groupby(W.date.dt.year):
    o = g[['date', 'ticker'] + [f'p_{h}' for h in HZ] + [f'y_{h}' for h in HZ]].copy()
    o['date'] = o.date.dt.strftime('%Y-%m-%d')
    for h in HZ: o[f'p_{h}'] = o[f'p_{h}'].round(4)
    o.to_csv(f'{R}/history/{yr}.csv.gz', index=False, float_format='%.4g', compression={'method': 'gzip', 'mtime': 0})
a = F.groupby('date').feat_asof.agg(['min', 'max']).reset_index()
a.columns = ['date', 'min_feat_asof', 'max_feat_asof']
a = a[a.date.isin(W.date.unique())]
for c in a.columns: a[c] = a[c].dt.strftime('%Y-%m-%d')
a.to_csv(f'{R}/feature_asof.csv', index=False)
json.dump(bt, open(f'{R}/backtest.json', 'w'), indent=1)
# reliability: decile bins of p per horizon
rel = {}
for h in HZ:
    q = P[(P.h == h) & P.y.notna()]
    q = q.assign(b=pd.qcut(q.p, 10, labels=False, duplicates='drop'))
    rel[h] = [[round(float(x.p.mean()), 4), round(float(x.y.mean()), 4)] for _, x in q.groupby('b')]
dates = sorted(W.date.dt.strftime('%Y-%m-%d').unique()); di = {d: i for i, d in enumerate(dates)}
W['di'] = W.date.dt.strftime('%Y-%m-%d').map(di)
tick = sorted(W.ticker.unique())
shard = {t: (t[0] if t[0].isalpha() else '0') for t in tick}
lastd = W[W.di == len(dates) - 1]
idx = dict(model='updown_v3_preopen', built=pd.Timestamp.now(tz='Asia/Hong_Kong').strftime('%Y-%m-%d %H:%M HKT'), horizons=HZ,
           dates=dates, tickers=tick, shard=shard, results=bt['results'], reliability=rel,
           latest_mean={h: (None if lastd[f'p_{h}'].isna().all() else round(float(lastd[f'p_{h}'].mean()), 4)) for h in HZ},
           format='data/t/<shard>.json.gz: {ticker:{i:[date index],p:{h:[per-mille or null]},y:{h:"1/0/- string"}}}')
json.dump(idx, open(f'{D}/data/index.json', 'w'), separators=(',', ':'))
for s in sorted(set(shard.values())):
    out = {}
    for t, g in W[W.ticker.isin([k for k, v in shard.items() if v == s])].groupby('ticker'):
        out[t] = dict(i=g.di.tolist(),
                      p={h: [None if np.isnan(v) else int(round(v * 1000)) for v in g[f'p_{h}'].values] for h in HZ},
                      y={h: ''.join('-' if np.isnan(v) else str(int(v)) for v in g[f'y_{h}'].values) for h in HZ})
    with gzip.GzipFile(f'{D}/data/t/{s}.json.gz', 'wb', mtime=0) as fh:
        fh.write(json.dumps(out, separators=(',', ':')).encode())
print('ok', len(dates), len(tick))
