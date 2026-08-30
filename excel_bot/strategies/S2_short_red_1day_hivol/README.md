# Strategy: S2_short_red_1day_hivol

- **Universe:** stocks matching `volM:high(>8%)`
- **Signal:** `hyst_core_score_e5_x2` = {"name": "hyst_core_score_e5_x2", "kind": "hyst", "key": "core_score", "enter": 5, "exit": 2, "min_len": 2}
- **Side:** short red
- **Entry:** buy at the close of the day the cluster is confirmed (the first day its color is knowable)
- **Exit:** hold1
- **Costs:** 0.1% round trip (0.3% microcaps)

## Results

```
{
  "discovery": {
    "n": 4455,
    "avg_net": 0.012158572390572392,
    "t": 9.900341349759383,
    "win": 0.5840628507295174,
    "tickers": 519
  },
  "holdout": {
    "n": 2901,
    "avg_net": 0.011182092381937264,
    "t": 8.438900874893072,
    "win": 0.5794553602206136,
    "tickers": 344
  }
}
```

Every trade in trades.csv: exact dates, prices, exit reason, and the highlight colors (human-readable) of the entry and exit days for eyeball-checking against Excel.
