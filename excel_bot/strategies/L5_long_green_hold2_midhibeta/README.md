# Strategy: L5_long_green_hold2_midhibeta

- **Universe:** stocks matching `mid:beta>1.5`
- **Signal:** `tol2_A_ml3` = {"name": "tol2_A_ml3", "kind": "tolerant", "key": "a", "tol": 2, "min_len": 3}
- **Side:** long green
- **Entry:** buy at the close of the day the cluster is confirmed (the first day its color is knowable)
- **Exit:** hold2
- **Costs:** 0.1% round trip (0.3% microcaps)

## Results

```
{
  "discovery": {
    "n": 542,
    "avg_net": 0.01333389298892989,
    "t": 4.920827565705533,
    "win": 0.5867158671586716,
    "tickers": 114
  },
  "holdout": {
    "n": 349,
    "avg_net": 0.01032724928366762,
    "t": 2.5057654969720184,
    "win": 0.5702005730659025,
    "tickers": 74
  }
}
```

Every trade in trades.csv: exact dates, prices, exit reason, and the highlight colors (human-readable) of the entry and exit days for eyeball-checking against Excel.
