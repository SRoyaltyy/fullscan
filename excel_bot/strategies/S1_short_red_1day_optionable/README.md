# Strategy: S1_short_red_1day_optionable

- **Universe:** stocks matching `opt:Yes`
- **Signal:** `hyst_core_score_e3_x2` = {"name": "hyst_core_score_e3_x2", "kind": "hyst", "key": "core_score", "enter": 3, "exit": 2, "min_len": 2}
- **Side:** short red
- **Entry:** buy at the close of the day the cluster is confirmed (the first day its color is knowable)
- **Exit:** hold1
- **Costs:** 0.1% round trip (0.3% microcaps)

## Results

```
{
  "discovery": {
    "n": 9815,
    "avg_net": 0.005219280692817117,
    "t": 13.151618569868912,
    "win": 0.5534386143657667,
    "tickers": 1420
  },
  "holdout": {
    "n": 6623,
    "avg_net": 0.004972344858825306,
    "t": 10.469486601868647,
    "win": 0.5488449343197946,
    "tickers": 966
  },
  "unknown": {
    "n": 5,
    "avg_net": 0.008018,
    "t": 0.8055222835286633,
    "win": 0.8,
    "tickers": 1
  }
}
```

Every trade in trades.csv: exact dates, prices, exit reason, and the highlight colors (human-readable) of the entry and exit days for eyeball-checking against Excel.
