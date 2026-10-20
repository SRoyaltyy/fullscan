"""Offline one-off build: python -m pkg.build  (run with nice -n 19)."""
import json, os, sys, numpy as np, pandas as pd
from .features import build, add_labels, FEATS, HORIZONS
from .walkforward import run, score
OUT = sys.argv[1] if len(sys.argv) > 1 else 'out'
os.makedirs(OUT, exist_ok=True)
A = pd.read_parquet('/workspace/updown/bars_all.parquet').drop(columns='__index_level_0__', errors='ignore')
T = pd.read_parquet('topup2_bars.parquet')
T['date'] = pd.to_datetime(T.date).dt.tz_localize(None).dt.normalize()
# seam check on the overlap (bars_all is kept for <= its last date)
ov = A[A.date >= '2026-09-15'].merge(T, on=['date', 'ticker'], suffixes=('', '_t'))
ratio = (ov.close_t / ov.close)
seam = dict(overlap_rows=len(ov), median_ratio=float(ratio.median()), share_within_0p5pct=float(((ratio - 1).abs() < .005).mean()))
print('seam', seam, flush=True)
bad = set(ov.loc[(ratio - 1).abs() > .02, 'ticker'])   # tickers whose adjustment differs across sources -> drop topup rows
T = T[(T.date > A.date.max()) & ~T.ticker.isin(bad)]
B = pd.concat([A, T[A.columns]], ignore_index=True)
B = B[(B.date >= '2018-01-01') & (B.close > 0) & B.open.notna()].drop_duplicates(['date', 'ticker'])
days = sorted(B.date.unique()) + [pd.Timestamp('2026-10-20')]  # today's pre-open (features from 10-19 close)
F = add_labels(build(B, days), B)
F.to_parquet(f'{OUT}/panel.parquet')
print('panel', F.shape, F.date.min(), F.date.max(), flush=True)
log, res, preds = [], {}, []
for h in HORIZONS:
    O = run(F, h, log)
    res[h] = score(O, h); print(res[h], flush=True)
    preds.append(O.rename(columns={'y_' + h: 'y', 'p': 'p', 'base': 'base'}).assign(h=h))
P = pd.concat(preds); P.to_parquet(f'{OUT}/preds.parquet')
json.dump(dict(seam=seam, dropped_tickers_seam=len(bad), windows=log, results=res), open(f'{OUT}/backtest.json', 'w'), indent=1, default=str)
