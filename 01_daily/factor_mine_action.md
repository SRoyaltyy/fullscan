# Factor mine action — 2026-08-13 → 2026-09-04

Cash-accounted blotters for the leak-free 09:30 recipes. Same rules as the paper sleeve book: **$10k, whole shares, Futubull fees, leftover cash split, sell first, min-hold, 09:30 open fills, hard-red S≤−3 sit, shorts as a liability (equity ≥ 2× notional)**. Signal-only percentages in the research table are **not** fills.

## Rule check (read this)

- **Cash / shares / fees:** every recipe below is a $10k sleeve. Leftover cash is split among *new* names only. A name that costs more than its split is skipped (not a fractional).
- **Hard-red:** morning S ≤ −3 sits — no new buys. Existing lots still sell when min-hold is met.
- **Flatten wish-list ≠ live tickets.** `flatten_h*` buys the 3d robust wish-list on io/HOLD mornings (8-13, 8-14, 8-17, …). Live `flatten_robust` Action those days is HOLD. `flatten_live_*` is the gated book (only 8-20 / 8-21 mover fires in this window).
- **Shorts** are sized from equity (not short-sale cash) and marked as a liability. The first mine's +300–1000% short books were a marking bug and are gone.

Phone: `dashboard/factor-mine/index.html`. Sister: [flatten lookback](../dashboard/flatten-lookback/) · [sleeve merge](../dashboard/sleeve-merge/) · [strategy board](../dashboard/strategy-board/).

Live `flatten_robust` is not changed.

## Featured books

| Strategy | Book % | Signal-only % | Starts YES | Fills | Skips | MD |
|---|---:|---:|---:|---:|---:|---|
| `flatten_h5` | +22.84 | +67.92 | 16/17 | 75 | 194 | [flatten_h5.md](factor_mine/flatten_h5.md) |
| `flatten_h1` | +15.53 | +21.67 | 16/17 | 110 | 41 | [flatten_h1.md](factor_mine/flatten_h1.md) |
| `flatten_h3` | +11.77 | +44.29 | 16/17 | 77 | 128 | [flatten_h3.md](factor_mine/flatten_h3.md) |
| `union_h3` | +9.86 | +34.19 | 16/17 | 97 | 156 | [union_h3.md](factor_mine/union_h3.md) |
| `union_h1` | +14.54 | +18.57 | 16/17 | 136 | 52 | [union_h1.md](factor_mine/union_h1.md) |
| `union_h5` | +18.84 | +58.01 | 14/17 | 100 | 242 | [union_h5.md](factor_mine/union_h5.md) |
| `union_join_present_h1` | +13.80 | +18.15 | 16/17 | 136 | 52 | [union_join_present_h1.md](factor_mine/union_join_present_h1.md) |
| `union_h3_exit_news_r` | +9.85 | +30.35 | 16/17 | 97 | 156 | [union_h3_exit_news_r.md](factor_mine/union_h3_exit_news_r.md) |
| `flatten_live_h1` | +9.78 | +4.99 | 7/17 | 32 | 0 | [flatten_live_h1.md](factor_mine/flatten_live_h1.md) |
| `flatten_live_h3` | +4.92 | +9.59 | 7/17 | 26 | 34 | [flatten_live_h3.md](factor_mine/flatten_live_h3.md) |
| `flatten_live_h5` | +5.85 | +8.02 | 7/17 | 26 | 55 | [flatten_live_h5.md](factor_mine/flatten_live_h5.md) |
| `union_e_fresh_h3` | +27.57 | -12.67 | 16/17 | 69 | 114 | [union_e_fresh_h3.md](factor_mine/union_e_fresh_h3.md) |
| `union_news_g_h5` | -10.92 | +152.54 | 4/17 | 81 | 189 | [union_news_g_h5.md](factor_mine/union_news_g_h5.md) |
| `union_white_coil_h1` | +2.05 | +7.42 | 11/17 | 124 | 8 | [union_white_coil_h1.md](factor_mine/union_white_coil_h1.md) |
| `union_e_green_h3` | +47.92 | +23.38 | 15/17 | 64 | 96 | [union_e_green_h3.md](factor_mine/union_e_green_h3.md) |

## All other blotters

- [`probable_h1`](factor_mine/probable_h1.md)
- [`probable_h3`](factor_mine/probable_h3.md)
- [`probable_h5`](factor_mine/probable_h5.md)
- [`yday_gainer_h1`](factor_mine/yday_gainer_h1.md)
- [`yday_gainer_h3`](factor_mine/yday_gainer_h3.md)
- [`yday_gainer_h5`](factor_mine/yday_gainer_h5.md)
- [`ohlc_hot_h1`](factor_mine/ohlc_hot_h1.md)
- [`ohlc_hot_h3`](factor_mine/ohlc_hot_h3.md)
- [`ohlc_hot_h5`](factor_mine/ohlc_hot_h5.md)
- [`union_vol_g_h1`](factor_mine/union_vol_g_h1.md)
- [`union_vol_g_h3`](factor_mine/union_vol_g_h3.md)
- [`union_vol_missing_h1`](factor_mine/union_vol_missing_h1.md)
- [`union_vol_missing_h3`](factor_mine/union_vol_missing_h3.md)
- [`union_ab_g_h1`](factor_mine/union_ab_g_h1.md)
- [`union_ab_g_h3`](factor_mine/union_ab_g_h3.md)
- [`union_join_g_h1`](factor_mine/union_join_g_h1.md)
- [`union_join_g_h3`](factor_mine/union_join_g_h3.md)
- [`union_join_present_h3`](factor_mine/union_join_present_h3.md)
- [`union_news_g_h1`](factor_mine/union_news_g_h1.md)
- [`union_news_g_h3`](factor_mine/union_news_g_h3.md)
- [`union_news_present_h1`](factor_mine/union_news_present_h1.md)
- [`union_news_present_h3`](factor_mine/union_news_present_h3.md)
- [`union_news_missing_h1`](factor_mine/union_news_missing_h1.md)
- [`union_news_missing_h3`](factor_mine/union_news_missing_h3.md)
- [`union_catal_present_h1`](factor_mine/union_catal_present_h1.md)
- [`union_catal_present_h3`](factor_mine/union_catal_present_h3.md)
- [`union_blue_h1`](factor_mine/union_blue_h1.md)
- [`union_blue_h3`](factor_mine/union_blue_h3.md)
- [`union_white_h1`](factor_mine/union_white_h1.md)
- [`union_white_h3`](factor_mine/union_white_h3.md)
- [`union_last_green_h1`](factor_mine/union_last_green_h1.md)
- [`union_last_green_h3`](factor_mine/union_last_green_h3.md)
- [`union_last_red_h1`](factor_mine/union_last_red_h1.md)
- [`union_last_red_h3`](factor_mine/union_last_red_h3.md)
- [`union_candle_h1`](factor_mine/union_candle_h1.md)
- [`union_candle_h3`](factor_mine/union_candle_h3.md)
- [`union_coil_off_h1`](factor_mine/union_coil_off_h1.md)
- [`union_coil_off_h3`](factor_mine/union_coil_off_h3.md)
- [`union_earn_react_h1`](factor_mine/union_earn_react_h1.md)
- [`union_earn_react_h3`](factor_mine/union_earn_react_h3.md)
- [`union_e_fresh_h1`](factor_mine/union_e_fresh_h1.md)
- [`union_r_up_h1`](factor_mine/union_r_up_h1.md)
- [`union_r_up_h3`](factor_mine/union_r_up_h3.md)
- [`union_break10_h1`](factor_mine/union_break10_h1.md)
- [`union_break10_h3`](factor_mine/union_break10_h3.md)
- [`union_vol_g_h5`](factor_mine/union_vol_g_h5.md)
- [`union_coil_off_h5`](factor_mine/union_coil_off_h5.md)
- [`union_last_green_h5`](factor_mine/union_last_green_h5.md)
- [`union_white_h5`](factor_mine/union_white_h5.md)
- [`union_vol_ab_h1`](factor_mine/union_vol_ab_h1.md)
- [`union_vol_ab_h3`](factor_mine/union_vol_ab_h3.md)
- [`union_blue_vol_h1`](factor_mine/union_blue_vol_h1.md)
- [`union_blue_vol_h3`](factor_mine/union_blue_vol_h3.md)
- [`union_news_vol_h1`](factor_mine/union_news_vol_h1.md)
- [`union_news_vol_h3`](factor_mine/union_news_vol_h3.md)
- [`union_e_green_h1`](factor_mine/union_e_green_h1.md)
- [`probable_probable_ok_h1`](factor_mine/probable_probable_ok_h1.md)
- [`probable_probable_ok_h3`](factor_mine/probable_probable_ok_h3.md)
- [`union_vol_green_h1`](factor_mine/union_vol_green_h1.md)
- [`union_vol_green_h3`](factor_mine/union_vol_green_h3.md)
- [`union_coil_green_h1`](factor_mine/union_coil_green_h1.md)
- [`union_coil_green_h3`](factor_mine/union_coil_green_h3.md)
- [`union_blue_coil_h1`](factor_mine/union_blue_coil_h1.md)
- [`union_blue_coil_h3`](factor_mine/union_blue_coil_h3.md)
- [`union_join_vol_green_h1`](factor_mine/union_join_vol_green_h1.md)
- [`union_join_vol_green_h3`](factor_mine/union_join_vol_green_h3.md)
- [`union_white_coil_h3`](factor_mine/union_white_coil_h3.md)
- [`flatten_vol_g_h3`](factor_mine/flatten_vol_g_h3.md)
- [`ohlc_hot_coil_h1`](factor_mine/ohlc_hot_coil_h1.md)
- [`union_hot_score_h1`](factor_mine/union_hot_score_h1.md)
- [`union_hot_score_h3`](factor_mine/union_hot_score_h3.md)
- [`union_candle_score_h1`](factor_mine/union_candle_score_h1.md)
- [`union_candle_score_h3`](factor_mine/union_candle_score_h3.md)
- [`union_ret_5_h1`](factor_mine/union_ret_5_h1.md)
- [`union_ret_5_h3`](factor_mine/union_ret_5_h3.md)
- [`union_cond_h1`](factor_mine/union_cond_h1.md)
- [`union_cond_h3`](factor_mine/union_cond_h3.md)
- [`union_w_hot_cond_h1`](factor_mine/union_w_hot_cond_h1.md)
- [`union_w_hot_cond_h3`](factor_mine/union_w_hot_cond_h3.md)
- [`union_w_hot_candle_h1`](factor_mine/union_w_hot_candle_h1.md)
- [`union_w_hot_candle_h3`](factor_mine/union_w_hot_candle_h3.md)
- [`union_hot_n4_h1`](factor_mine/union_hot_n4_h1.md)
- [`union_hot_n12_h1`](factor_mine/union_hot_n12_h1.md)
- [`union_cond_n4_h3`](factor_mine/union_cond_n4_h3.md)
- [`union_h3_exit_alarm`](factor_mine/union_h3_exit_alarm.md)
- [`union_h5_exit_alarm`](factor_mine/union_h5_exit_alarm.md)
- [`union_h3_exit_red`](factor_mine/union_h3_exit_red.md)
- [`coil_h3_exit_alarm`](factor_mine/coil_h3_exit_alarm.md)
- [`short_alarm_h1`](factor_mine/short_alarm_h1.md)
- [`short_alarm_h3`](factor_mine/short_alarm_h3.md)
- [`short_news_r_h1`](factor_mine/short_news_r_h1.md)
- [`short_news_r_h3`](factor_mine/short_news_r_h3.md)
- [`short_r_down_h1`](factor_mine/short_r_down_h1.md)
- [`short_r_down_h3`](factor_mine/short_r_down_h3.md)
- [`short_extended_h1`](factor_mine/short_extended_h1.md)
- [`short_extended_h3`](factor_mine/short_extended_h3.md)
- [`short_last_red_h1`](factor_mine/short_last_red_h1.md)
- [`short_last_red_h3`](factor_mine/short_last_red_h3.md)
