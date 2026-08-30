# Strategy: L1_long_green_tp8_lowvol

- **Universe:** stocks matching `volM:low(<3%)`
- **Signal:** `tol3_core_score_ml2` = {"name": "tol3_core_score_ml2", "kind": "tolerant", "key": "core_score", "thresh": 0.5, "tol": 3, "min_len": 2}
- **Side:** long green
- **Entry:** buy at the close of the day the cluster is confirmed (the first day its color is knowable)
- **Exit:** tp8
- **Costs:** 0.1% round trip (0.3% microcaps)

## Results

```
{
  "discovery": {
    "n": 2635,
    "avg_net": 0.012680645161290322,
    "t": 12.533736088692075,
    "win": 0.5138519924098671,
    "tickers": 569
  },
  "holdout": {
    "n": 1869,
    "avg_net": 0.012234991974317818,
    "t": 10.018797297549911,
    "win": 0.5131086142322098,
    "tickers": 391
  },
  "unknown": {
    "n": 5,
    "avg_net": -0.011036000000000002,
    "t": -0.26897712297724713,
    "win": 0.4,
    "tickers": 1
  }
}
```

Every trade in trades.csv: exact dates, prices, exit reason, and the highlight colors (human-readable) of the entry and exit days for eyeball-checking against Excel.
