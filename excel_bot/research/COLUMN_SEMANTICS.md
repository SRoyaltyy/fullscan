# Excel A–F → H/I semantics (emulator)

Source: `engine/model.json` formulas, representative daily row.

## STOCKHISTORY feed (A–F)

In the real file, A–F are the daily STOCKHISTORY spill (`IR:IW`):

| Letter | Spill | Field |
|---|---|---|
| A | IR | Date |
| B | IS | Close |
| C | IT | Open |
| D | IU | High |
| E | IV | Low |
| F | IW | Volume |

Rows 3+ wrap errors by carrying the prior bar (`IF(ISERROR(ISx), ISx-1, ISx)`).
The GitHub replica seeds `IR:IW` (and weekly `AP:AU`) from Yahoo daily OHLCV
via `stockhistory.py` / `fastfetch.py` / `data/rows/<T>.json`.

`--from-cache` replays the xlsx’s last-calc NNE snapshot. Do **not** use it
for backtests.

## Labels (never same-row features)

```
H[t] = (B[t] − C[t]) / C[t]          # intraday, close vs open
I[t] = (B[t] − B[t−1]) / B[t−1]      # daily, close vs yesterday
```

Excel stores these as **fractions** (0.02 = +2%).

Same-row identity:

```
gap[t] = (C[t] − B[t−1]) / B[t−1]    # overnight, knowable at 09:30
I[t]   = gap[t] + H[t] × (1 + gap[t])
```

## Other first-class A–O numbers

| Col | Formula shape | Open value? |
|---|---|---|
| G | F[t]/F[t−1] | no (needs today’s volume) |
| J | (C[t]−C[t−1])/C[t−1] | yes |
| K | (D[t]−C[t])/(C[t]+0.001) upside wick | no |
| M | −(E[t]−C[t])/C[t] downside wick | no |
| N | EL×1{\|H\|≥0.03} | no |
| AH | COUNTIF(prior H, "<=-0.05") | yes (prior H only) |
| O | composite of prior H/F/N/EL/CP + same-row DD | **fill** open; **number** close |

Deeper columns (P…JL) are candle text, streaks, weekly AP:AU, VIX cache,
and 0/1 flags — all still rooted in A–F plus a static `[1]Change!` lookup
(Q/R) and `[1]VIX`. They can *encode* the same tape differently; they do
not add a second live data vendor.

## Clock landmines

- `core_score` / anything that reads D,E,F,H,I on the same row → close entry.
- B/G/K/M: fill may paint early; the **number** needs the close.
- O green fill is morning-knowable; O’s number is not.
- Upper rows (t−1, t−2, …) of any letter are fair at the next open.
