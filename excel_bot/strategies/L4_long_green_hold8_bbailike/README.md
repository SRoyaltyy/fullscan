# Strategy: L4_long_green_hold8_bbailike

- **Universe:** stocks matching `mid:BBAI-like(hi-beta,unprof)`
- **Signal:** `hyst_core_score_e3_x0` = {"name": "hyst_core_score_e3_x0", "kind": "hyst", "key": "core_score", "enter": 3, "exit": 0, "min_len": 2}
- **Side:** long green
- **Entry:** buy at the close of the day the cluster is confirmed (the first day its color is knowable)
- **Exit:** hold8
- **Costs:** 0.1% round trip (0.3% microcaps)

## Results

```
{
  "discovery": {
    "n": 526,
    "avg_net": 0.028027794676806085,
    "t": 4.050920902174311,
    "win": 0.46577946768060835,
    "tickers": 65
  },
  "holdout": {
    "n": 268,
    "avg_net": 0.019420037313432836,
    "t": 2.3543291139728457,
    "win": 0.4626865671641791,
    "tickers": 36
  }
}
```

Every trade in trades.csv: exact dates, prices, exit reason, and the highlight colors (human-readable) of the entry and exit days for eyeball-checking against Excel.
