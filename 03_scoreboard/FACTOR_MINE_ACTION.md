# Factor mine action — 2026-08-13 → 2026-09-04

Cash-accounted blotters for the leak-free 09:30 recipes. Each recipe is a **daily cash + holdings state machine**: morning leftover cash and the lots we actually hold are the only inputs to that session's buys/sells. We can only sell shares on hand and only spend leftover cash (whole shares, Futubull fees). An independent fill-replay **audit** flags any violation.

## Rule check (read this)

- **Butterfly state:** day N open cash/held = day N−1 close after fills. A miss on 8-13 leftover changes every later ticket.
- **Per-name marks:** every session (including no-fill days) lists each held ticker's prior close, 09:30 open, overnight $, close, and intraday $. That is the only reason equity moves when we do not trade.
- **Cash / shares / fees:** leftover split (or rank-weight / top-heavy / half) among *new* names. Skip if the split cannot buy 1 share.
- **Sell:** list-drop after min-hold, or time-stop / cut-loser / trail. Never sell a ticker we do not hold.
- **S-boost:** on mornings with general S ≥ +5, optional sizeup (1.35×) and/or +4 names — still capped by leftover cash. Hard-red S ≤ −3 sits.
- **Flatten wish-list ≠ live tickets.** `flatten_h*` buys the wish-list on io/HOLD mornings. `flatten_live_*` is the gated book.

Phone: `dashboard/factor-mine/index.html`. Sister: [flatten lookback](../dashboard/flatten-lookback/) · [sleeve merge](../dashboard/sleeve-merge/) · [strategy board](../dashboard/strategy-board/).

Live `flatten_robust` is not changed.

## Featured books

| Strategy | Size | Sell | Boost | Book % | Signal-only % | Starts YES | Fills | Skips | Audit | MD |
|---|---|---|---|---:|---:|---:|---:|---:|---|---|
| `flatten_h5` | leftover | list | none | +22.84 | +67.92 | 16/17 | 75 | 194 | PASS | [flatten_h5.md](factor_mine/flatten_h5.md) |
| `flatten_h5_cut` | leftover | cut_loser | none | +22.84 | +67.92 | 16/17 | 75 | 194 | PASS | [flatten_h5_cut.md](factor_mine/flatten_h5_cut.md) |
| `flatten_h5_sizeup` | leftover | list | sizeup | +22.84 | +67.92 | 16/17 | 75 | 194 | PASS | [flatten_h5_sizeup.md](factor_mine/flatten_h5_sizeup.md) |
| `flatten_h5_time` | leftover | time | none | +22.84 | +67.92 | 16/17 | 75 | 194 | PASS | [flatten_h5_time.md](factor_mine/flatten_h5_time.md) |
| `flatten_h5_trail` | leftover | trail | none | +22.84 | +67.92 | 16/17 | 75 | 194 | PASS | [flatten_h5_trail.md](factor_mine/flatten_h5_trail.md) |
| `flatten_h5_sboost` | leftover | list | both | +22.18 | +67.92 | 16/17 | 79 | 202 | PASS | [flatten_h5_sboost.md](factor_mine/flatten_h5_sboost.md) |
| `union_last_green_h5` | leftover | list | none | +19.41 | +52.19 | 12/17 | 83 | 216 | PASS | [union_last_green_h5.md](factor_mine/union_last_green_h5.md) |
| `union_h5` | leftover | list | none | +18.84 | +58.01 | 14/17 | 100 | 242 | PASS | [union_h5.md](factor_mine/union_h5.md) |
| `flatten_h1` | leftover | list | none | +15.53 | +21.67 | 16/17 | 110 | 41 | PASS | [flatten_h1.md](factor_mine/flatten_h1.md) |
| `flatten_h3` | leftover | list | none | +11.77 | +44.29 | 16/17 | 77 | 128 | PASS | [flatten_h3.md](factor_mine/flatten_h3.md) |
| `flatten_live_h5` | leftover | list | none | +5.85 | +8.02 | 7/17 | 26 | 55 | PASS | [flatten_live_h5.md](factor_mine/flatten_live_h5.md) |
| `union_news_g_h5` | leftover | list | none | -11.63 | +152.34 | 7/17 | 83 | 194 | PASS | [union_news_g_h5.md](factor_mine/union_news_g_h5.md) |
| `flatten_h5_rankw` | rank_w | list | none | +17.31 | +67.92 | 17/17 | 72 | 187 | PASS | [flatten_h5_rankw.md](factor_mine/flatten_h5_rankw.md) |

## All other blotters

- [`flatten_vol_g_h3`](factor_mine/flatten_vol_g_h3.md)
- [`probable_h5`](factor_mine/probable_h5.md)
- [`yday_gainer_h5`](factor_mine/yday_gainer_h5.md)
- [`ohlc_hot_h5`](factor_mine/ohlc_hot_h5.md)
- [`union_vol_g_h5`](factor_mine/union_vol_g_h5.md)
- [`union_coil_off_h5`](factor_mine/union_coil_off_h5.md)
- [`union_white_h5`](factor_mine/union_white_h5.md)
- [`union_h5_exit_alarm`](factor_mine/union_h5_exit_alarm.md)
- [`flatten_h5_topheavy`](factor_mine/flatten_h5_topheavy.md)
- [`flatten_h5_half`](factor_mine/flatten_h5_half.md)
