"""python -m research.updown_v4_preopen.confluence_build <bars.parquet> <cboe_dir> <out>  (reuses out/panel.parquet universe)"""
import json, sys, numpy as np, pandas as pd
from .blocks import stock_blocks
from .events import confluence, FAMILIES
bars_p, cdir, OUT = sys.argv[1:4]
B = pd.read_parquet(bars_p); B = B[B.date <= '2026-10-19'].sort_values(['ticker', 'date'])
S = stock_blocks(B)
v20 = B.groupby('ticker').volume.transform(lambda s: s.shift(1).rolling(20, min_periods=15).mean())
S['ev_volume_surge'] = (B.volume.values >= 2 * v20.values).astype('float32')   # bar-D volume >= 2x its prior 20-day average
vx = pd.read_csv(f'{cdir}/VIX_History.csv'); vx.columns = [c.lower() for c in vx.columns]
vx = pd.Series(vx.close.astype(float).values, index=pd.to_datetime(vx.date)).sort_index()
spike = (vx / vx.shift(1) - 1 >= 0.15) | (vx >= vx.rolling(252, min_periods=120).quantile(0.95))   # VIX close up >=15% or in its trailing-year top 5%
S['ev_vix_spike'] = S.date.map(spike).fillna(False).astype('float32')
U = pd.read_parquet(f'{OUT}/panel.parquet', columns=['date', 'ticker', 'feat_asof'])
cols = sorted(set(sum(FAMILIES.values(), [])))
E = U.merge(S.rename(columns={'date': 'feat_asof'})[['feat_asof', 'ticker'] + cols + ['fwd1', 'fwd5', 'fwd21', 'fwd63']], on=['feat_asof', 'ticker'], how='left')
res = confluence(E)
json.dump(dict(window=5, families={k: v for k, v in FAMILIES.items()}, results=res), open(f'{OUT}/confluence.json', 'w'), indent=1, default=float)
for r in res: print({k: (round(v, 4) if isinstance(v, float) else v) for k, v in r.items()})
