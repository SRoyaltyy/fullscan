"""Display-only add-on: write dashboard/updown-v3/data/c/<shard>.json.gz with each ticker's
last known close (the N-1 close the model's features use, column `_close` in panel.parquet),
aligned 1:1 with the date indices `i` already in data/t/<shard>.json.gz.
Does not touch the model, backtest, or data/t files.  Usage: python export_close.py out/ dashboard/updown-v3"""
import gzip, json, os, sys, pandas as pd
src, D = sys.argv[1], sys.argv[2]
IX = json.load(open(f'{D}/data/index.json'))
dates = IX['dates']
F = pd.read_parquet(f'{src}/panel.parquet', columns=['date', 'ticker', '_close', 'feat_asof'])
F['d'] = F.date.dt.strftime('%Y-%m-%d')
assert (F.feat_asof < F.date).all(), 'close must be strictly prior to the prediction day'
F = F[F.d.isin(set(dates))].drop_duplicates(['d', 'ticker']).set_index(['ticker', 'd'])._close
os.makedirs(f'{D}/data/c', exist_ok=True)
for s in sorted(set(IX['shard'].values())):
    T = json.load(gzip.open(f'{D}/data/t/{s}.json.gz'))
    out = {}
    for t, r in T.items():
        g = F.get(t)
        out[t] = [None if g is None or dates[i] not in g.index or pd.isna(g[dates[i]]) else float(f'{g[dates[i]]:.4g}' if g[dates[i]] < 1 else round(float(g[dates[i]]), 2)) for i in r['i']]
    with gzip.GzipFile(f'{D}/data/c/{s}.json.gz', 'wb', mtime=0) as fh:
        fh.write(json.dumps(out, separators=(',', ':')).encode())
print('ok closes', len(IX['shard']))
