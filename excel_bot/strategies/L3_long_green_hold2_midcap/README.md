# Strategy: L3_long_green_hold2_midcap

- **Universe:** stocks matching `mid(1-10B):all`
- **Signal:** `tol2_core_score_ml3` = {"name": "tol2_core_score_ml3", "kind": "tolerant", "key": "core_score", "thresh": 0.5, "tol": 2, "min_len": 3}
- **Side:** long green
- **Entry:** buy at the close of the day the cluster is confirmed (the first day its color is knowable)
- **Exit:** hold2
- **Costs:** 0.1% round trip (0.3% microcaps)

## Results

```
{
  "discovery": {
    "n": 4045,
    "avg_net": 0.006230252163164401,
    "t": 7.362399696326439,
    "win": 0.5567367119901112,
    "tickers": 647
  },
  "holdout": {
    "n": 2689,
    "avg_net": 0.005540502045370026,
    "t": 5.692403262976797,
    "win": 0.5589438452956489,
    "tickers": 438
  }
}
```

Every trade in trades.csv: exact dates, prices, exit reason, and the highlight colors (human-readable) of the entry and exit days for eyeball-checking against Excel.
