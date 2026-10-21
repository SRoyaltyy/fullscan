"""Write research/updown_v4_preopen history + dashboard/updown-v4/data from out/ (offline).
Usage: python -m research.updown_v4_preopen.export out research/updown_v4_preopen dashboard/updown-v4"""
import gzip, json, os, sys, numpy as np, pandas as pd
src, R, D = sys.argv[1], sys.argv[2], sys.argv[3]
os.makedirs(f'{R}/history', exist_ok=True); os.makedirs(f'{D}/data/t', exist_ok=True); os.makedirs(f'{D}/data/c', exist_ok=True)
bt = json.load(open(f'{src}/backtest.json')); ev = json.load(open(f'{src}/events.json')); cf = json.load(open(f'{src}/confluence.json'))
HZ = ['1d', '5d', '21d', '63d']
P = pd.concat([pd.read_parquet(f'{src}/preds_{h}.parquet') for h in HZ])
F = pd.read_parquet(f'{src}/panel.parquet', columns=['date', 'ticker', 'feat_asof', '_close'])
assert (F.feat_asof < F.date).all()
W = P.pivot_table(index=['date', 'ticker'], columns='h', values=['p', 'y'], aggfunc='first')
W.columns = [f'{a}_{b}' for a, b in W.columns]; W = W.reset_index().sort_values(['date', 'ticker'])
for h in HZ:
    if f'y_{h}' not in W: W[f'y_{h}'] = np.nan
for yr, g in W.groupby(W.date.dt.year):
    o = g[['date', 'ticker'] + [f'p_{h}' for h in HZ] + [f'y_{h}' for h in HZ]].copy(); o['date'] = o.date.dt.strftime('%Y-%m-%d')
    for h in HZ: o[f'p_{h}'] = o[f'p_{h}'].round(4)
    o.to_csv(f'{R}/history/{yr}.csv.gz', index=False, float_format='%.4g', compression={'method': 'gzip', 'mtime': 0})
a = F.groupby('date').feat_asof.agg(['min', 'max']).reset_index(); a.columns = ['date', 'min_feat_asof', 'max_feat_asof']
a = a[a.date.isin(W.date.unique())]
for c in a.columns: a[c] = a[c].dt.strftime('%Y-%m-%d')
a.to_csv(f'{R}/feature_asof.csv', index=False)
json.dump(bt, open(f'{R}/backtest.json', 'w'), indent=1); json.dump(ev, open(f'{R}/events.json', 'w'), indent=1); json.dump(cf, open(f'{R}/confluence.json', 'w'), indent=1)
rel, res = {}, {}
for h in HZ:
    q = P[(P.h == h) & P.y.notna()]
    qb = q.assign(b=pd.qcut(q.p, 10, labels=False, duplicates='drop'))
    rel[h] = [[round(float(x.p.mean()), 4), round(float(x.y.mean()), 4), int(len(x))] for _, x in qb.groupby('b')]
    s = dict(bt['results']['v4_all'][h]); g = q.assign(hit=((q.p > .5) == (q.y == 1)).astype(float), hb=((q.base > .5) == (q.y == 1)).astype(float)).groupby('date')
    s.update(brier_base=s['brier_base_rate'], hit=float(g.hit.mean().mean()), hit_base=float(g.hb.mean().mean()), rows=int(len(q)), n_indep_dates=s['n_indep'])
    res[h] = s
abl = {n: {h: {k: v for k, v in r.items()} for h, r in bt['results'][n].items()} for n in bt['results']}
dates = sorted(W.date.dt.strftime('%Y-%m-%d').unique()); di = {d: i for i, d in enumerate(dates)}
W['di'] = W.date.dt.strftime('%Y-%m-%d').map(di); tick = sorted(W.ticker.unique())
shard = {t: (t[0] if t[0].isalpha() else '0') for t in tick}; lastd = W[W.di == len(dates) - 1]
idx = dict(model='updown_v4_preopen', built=pd.Timestamp.now(tz='Asia/Hong_Kong').strftime('%Y-%m-%d %H:%M HKT'), horizons=HZ,
           dates=dates, tickers=tick, shard=shard, results=res, reliability=rel, ablation=abl, events=ev, confluence=cf, top_n=bt['top_n'],
           latest_mean={h: (None if lastd[f'p_{h}'].isna().all() else round(float(lastd[f'p_{h}'].mean()), 4)) for h in HZ},
           format='data/t/<shard>.json.gz: {ticker:{i:[date index],p:{h:[per-mille or null]},y:{h:"1/0/- string"}}}; data/c/<shard>.json.gz: {ticker:[prior close aligned with i]}')
json.dump(idx, open(f'{D}/data/index.json', 'w'), separators=(',', ':'))
Fc = F.assign(d=F.date.dt.strftime('%Y-%m-%d')).drop_duplicates(['d', 'ticker']).set_index(['ticker', 'd'])._close
for s in sorted(set(shard.values())):
    out, cl = {}, {}
    for t, g in W[W.ticker.isin([k for k, v in shard.items() if v == s])].groupby('ticker'):
        out[t] = dict(i=g.di.tolist(), p={h: [None if np.isnan(v) else int(round(v * 1000)) for v in g[f'p_{h}'].values] for h in HZ},
                      y={h: ''.join('-' if np.isnan(v) else str(int(v)) for v in g[f'y_{h}'].values) for h in HZ})
        ct = Fc.loc[t] if t in Fc.index.get_level_values(0) else None
        cl[t] = [None if ct is None or dates[i] not in ct.index or pd.isna(ct[dates[i]]) else round(float(ct[dates[i]]), 2) for i in out[t]['i']]
    for name, obj in (('t', out), ('c', cl)):
        with gzip.GzipFile(f'{D}/data/{name}/{s}.json.gz', 'wb', mtime=0) as fh: fh.write(json.dumps(obj, separators=(',', ':')).encode())
print('ok', len(dates), len(tick))
