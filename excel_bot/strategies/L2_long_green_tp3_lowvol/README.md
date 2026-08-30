# Strategy: L2_long_green_tp3_lowvol

- **Universe:** stocks matching `volM:low(<3%)`
- **Signal:** `tol3_core_score_ml2` = {"name": "tol3_core_score_ml2", "kind": "tolerant", "key": "core_score", "thresh": 0.5, "tol": 3, "min_len": 2}
- **Side:** long green
- **Entry:** buy at the close of the day the cluster is confirmed (the first day its color is knowable)
- **Exit:** tp3
- **Costs:** 0.1% round trip (0.3% microcaps)

## Results

```
{
  "discovery": {
    "n": 2635,
    "avg_net": 0.006982303605313094,
    "t": 11.161299356927362,
    "win": 0.6561669829222011,
    "tickers": 569
  },
  "holdout": {
    "n": 1869,
    "avg_net": 0.006464002140181916,
    "t": 8.233821641340079,
    "win": 0.6506153023006955,
    "tickers": 391
  },
  "unknown": {
    "n": 5,
    "avg_net": 0.01538,
    "t": 1.1292217327459617,
    "win": 0.8,
    "tickers": 1
  }
}
```

Every trade in trades.csv: exact dates, prices, exit reason, and the highlight colors (human-readable) of the entry and exit days for eyeball-checking against Excel.
