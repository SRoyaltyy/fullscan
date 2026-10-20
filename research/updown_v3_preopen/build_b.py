import json, sys, pandas as pd
from .features import HORIZONS
from .walkforward import run, score
OUT = sys.argv[1]; F = pd.read_parquet(f'{OUT}/panel.parquet'); A = json.load(open(f'{OUT}/backtest.json'))
log, res, preds = [], {}, []
for h in HORIZONS:
    O = run(F, h, log, variant='B'); res[h] = score(O, h); print(res[h], flush=True)
    preds.append(O.rename(columns={'y_' + h: 'y'}).assign(h=h))
pd.concat(preds).to_parquet(f'{OUT}/preds_B.parquet')
json.dump(dict(seam=A['seam'], dropped_tickers_seam=A['dropped_tickers_seam'], windows=log, results=res,
               variant_A=dict(windows=A['windows'], results=A['results'])), open(f'{OUT}/backtest_B.json', 'w'), indent=1, default=str)
