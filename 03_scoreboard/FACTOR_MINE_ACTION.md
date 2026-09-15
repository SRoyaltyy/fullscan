# Factor mine action — 2026-08-13 → 2026-09-15

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
| `combo_jse_333_shared` | leftover | list | none | +31.46 | — | 15/23 | 254 | 289 | PASS | [combo_jse_333_shared.md](factor_mine/combo_jse_333_shared.md) |
| `combo_ej_5050_shared` | leftover | list | none | +30.45 | — | 19/23 | 222 | 230 | PASS | [combo_ej_5050_shared.md](factor_mine/combo_ej_5050_shared.md) |
| `combo_es_9010_shared` | leftover | list | none | +28.70 | — | 20/23 | 176 | 258 | PASS | [combo_es_9010_shared.md](factor_mine/combo_es_9010_shared.md) |
| `combo_se_3070_shared` | leftover | list | none | +28.16 | — | 20/23 | 182 | 261 | PASS | [combo_se_3070_shared.md](factor_mine/combo_se_3070_shared.md) |
| `union_e_fresh_h3` | leftover | list | none | +27.98 | -13.56 | 20/23 | 96 | 168 | PASS | [union_e_fresh_h3.md](factor_mine/union_e_fresh_h3.md) |
| `combo_sh_5050_shared` | leftover | list | none | +41.54 | — | 21/23 | 155 | 125 | PASS | [combo_sh_5050_shared.md](factor_mine/combo_sh_5050_shared.md) |
| `combo_sh_3070_shared` | leftover | list | none | +40.84 | — | 21/23 | 155 | 125 | PASS | [combo_sh_3070_shared.md](factor_mine/combo_sh_3070_shared.md) |
| `combo_seh_333_skip` | leftover | list | none | +32.71 | — | 21/23 | 259 | 304 | PASS | [combo_seh_333_skip.md](factor_mine/combo_seh_333_skip.md) |
| `union_hot_n4_h1` | leftover | list | none | +32.21 | +51.98 | 21/23 | 87 | 38 | PASS | [union_hot_n4_h1.md](factor_mine/union_hot_n4_h1.md) |
| `combo_seh_502525_shared` | leftover | list | none | +30.74 | — | 21/23 | 267 | 298 | PASS | [combo_seh_502525_shared.md](factor_mine/combo_seh_502525_shared.md) |
| `combo_seh_333_shared` | leftover | list | none | +29.78 | — | 21/23 | 269 | 299 | PASS | [combo_seh_333_shared.md](factor_mine/combo_seh_333_shared.md) |
| `combo_seh_333_weather` | leftover | list | none | +29.78 | — | 21/23 | 269 | 299 | PASS | [combo_seh_333_weather.md](factor_mine/combo_seh_333_weather.md) |
| `combo_seh_403525_shared` | leftover | list | none | +29.64 | — | 21/23 | 267 | 298 | PASS | [combo_seh_403525_shared.md](factor_mine/combo_seh_403525_shared.md) |
| `combo_seh_404020_shared` | leftover | list | none | +29.14 | — | 21/23 | 269 | 299 | PASS | [combo_seh_404020_shared.md](factor_mine/combo_seh_404020_shared.md) |
| `combo_es_8020_shared` | leftover | list | none | +29.06 | — | 20/23 | 182 | 261 | PASS | [combo_es_8020_shared.md](factor_mine/combo_es_8020_shared.md) |
| `combo_ps_7030_shared` | leftover | list | none | +28.80 | — | 21/23 | 134 | 111 | PASS | [combo_ps_7030_shared.md](factor_mine/combo_ps_7030_shared.md) |
| `union_news_pack_net2_h1` | leftover | list | none | +28.55 | +22.32 | 21/23 | 56 | 19 | PASS | [union_news_pack_net2_h1.md](factor_mine/union_news_pack_net2_h1.md) |
| `combo_ee1_3070_shared` | leftover | list | none | +27.98 | — | 20/23 | 96 | 215 | PASS | [combo_ee1_3070_shared.md](factor_mine/combo_ee1_3070_shared.md) |
| `combo_ee1_5050_shared` | leftover | list | none | +27.98 | — | 20/23 | 96 | 215 | PASS | [combo_ee1_5050_shared.md](factor_mine/combo_ee1_5050_shared.md) |
| `combo_ee1_7030_shared` | leftover | list | none | +27.98 | — | 20/23 | 96 | 215 | PASS | [combo_ee1_7030_shared.md](factor_mine/combo_ee1_7030_shared.md) |
| `combo_ehs_601525_shared` | leftover | list | none | +27.55 | — | 21/23 | 271 | 300 | PASS | [combo_ehs_601525_shared.md](factor_mine/combo_ehs_601525_shared.md) |
| `combo_eh_3070_shared` | leftover | list | none | +27.20 | — | 21/23 | 213 | 223 | PASS | [combo_eh_3070_shared.md](factor_mine/combo_eh_3070_shared.md) |
| `combo_ehs_702010_shared` | leftover | list | none | +27.17 | — | 21/23 | 267 | 298 | PASS | [combo_ehs_702010_shared.md](factor_mine/combo_ehs_702010_shared.md) |
| `combo_eh_5050_shared` | leftover | list | none | +27.16 | — | 21/23 | 213 | 223 | PASS | [combo_eh_5050_shared.md](factor_mine/combo_eh_5050_shared.md) |
| `combo_eh_7030_shared` | leftover | list | none | +26.66 | — | 21/23 | 213 | 223 | PASS | [combo_eh_7030_shared.md](factor_mine/combo_eh_7030_shared.md) |
| `union_news_pack_h1` | leftover | list | none | +24.09 | +21.62 | 21/23 | 62 | 20 | PASS | [union_news_pack_h1.md](factor_mine/union_news_pack_h1.md) |
| `combo_hn_7030_shared` | leftover | list | none | +23.55 | — | 21/23 | 209 | 106 | PASS | [combo_hn_7030_shared.md](factor_mine/combo_hn_7030_shared.md) |
| `combo_her_5050_shared` | leftover | list | none | +22.20 | — | 21/23 | 215 | 224 | PASS | [combo_her_5050_shared.md](factor_mine/combo_her_5050_shared.md) |
| `union_earn_react_h3` | leftover | list | none | +21.91 | -24.30 | 19/23 | 98 | 170 | PASS | [union_earn_react_h3.md](factor_mine/union_earn_react_h3.md) |
| `combo_seh_333_split` | leftover | list | none | +21.58 | — | 21/23 | 259 | 299 | PASS | [combo_seh_333_split.md](factor_mine/combo_seh_333_split.md) |
| `combo_he1_5050_shared` | leftover | list | none | +20.20 | — | 21/23 | 223 | 86 | PASS | [combo_he1_5050_shared.md](factor_mine/combo_he1_5050_shared.md) |
| `combo_seh_451540_shared` | leftover | list | none | +34.46 | — | 21/23 | 261 | 295 | PASS | [combo_seh_451540_shared.md](factor_mine/combo_seh_451540_shared.md) |
| `combo_sh_7030_shared` | leftover | list | none | +33.07 | — | 21/23 | 155 | 125 | PASS | [combo_sh_7030_shared.md](factor_mine/combo_sh_7030_shared.md) |
| `combo_seh_601525_shared` | leftover | list | none | +30.78 | — | 21/23 | 261 | 295 | PASS | [combo_seh_601525_shared.md](factor_mine/combo_seh_601525_shared.md) |
| `combo_se_5050_skip` | leftover | list | none | +28.83 | — | 16/23 | 156 | 255 | PASS | [combo_se_5050_skip.md](factor_mine/combo_se_5050_skip.md) |
| `combo_jer_5050_shared` | leftover | list | none | +28.44 | — | 17/23 | 226 | 235 | PASS | [combo_jer_5050_shared.md](factor_mine/combo_jer_5050_shared.md) |
| `combo_p2s_5050_shared` | leftover | list | none | +27.36 | — | 21/23 | 128 | 110 | PASS | [combo_p2s_5050_shared.md](factor_mine/combo_p2s_5050_shared.md) |
| `combo_ps_5050_shared` | leftover | list | none | +26.51 | — | 21/23 | 132 | 112 | PASS | [combo_ps_5050_shared.md](factor_mine/combo_ps_5050_shared.md) |
| `combo_ers_7030_shared` | leftover | list | none | +25.09 | — | 15/23 | 186 | 268 | PASS | [combo_ers_7030_shared.md](factor_mine/combo_ers_7030_shared.md) |
| `combo_se1_5050_shared` | leftover | list | none | +24.66 | — | 16/23 | 204 | 129 | PASS | [combo_se1_5050_shared.md](factor_mine/combo_se1_5050_shared.md) |
| `combo_ser_5050_shared` | leftover | list | none | +24.31 | — | 16/23 | 166 | 248 | PASS | [combo_ser_5050_shared.md](factor_mine/combo_ser_5050_shared.md) |
| `combo_sj_5050_shared` | leftover | list | none | +21.33 | — | 16/23 | 202 | 130 | PASS | [combo_sj_5050_shared.md](factor_mine/combo_sj_5050_shared.md) |
| `combo_e1s_7030_shared` | leftover | list | none | +21.26 | — | 16/23 | 204 | 129 | PASS | [combo_e1s_7030_shared.md](factor_mine/combo_e1s_7030_shared.md) |
| `combo_sj_3070_shared` | leftover | list | none | +20.70 | — | 16/23 | 202 | 130 | PASS | [combo_sj_3070_shared.md](factor_mine/combo_sj_3070_shared.md) |
| `combo_sj_7030_shared` | leftover | list | none | +16.73 | — | 16/23 | 202 | 130 | PASS | [combo_sj_7030_shared.md](factor_mine/combo_sj_7030_shared.md) |
| `combo_snj_333_shared` | leftover | list | none | +14.93 | — | 16/23 | 332 | 194 | PASS | [combo_snj_333_shared.md](factor_mine/combo_snj_333_shared.md) |
| `combo_sn_5050_shared` | leftover | list | none | +12.81 | — | 16/23 | 210 | 156 | PASS | [combo_sn_5050_shared.md](factor_mine/combo_sn_5050_shared.md) |
| `combo_je1_5050_shared` | leftover | list | none | +12.79 | — | 14/23 | 270 | 86 | PASS | [combo_je1_5050_shared.md](factor_mine/combo_je1_5050_shared.md) |
| `flatten_live_h1` | leftover | list | none | +8.22 | +4.45 | 7/23 | 48 | 0 | PASS | [flatten_live_h1.md](factor_mine/flatten_live_h1.md) |
| `flatten_live_h3` | leftover | list | none | -6.24 | +6.87 | 0/23 | 42 | 45 | PASS | [flatten_live_h3.md](factor_mine/flatten_live_h3.md) |
| `flatten_live_h5` | leftover | list | none | -5.51 | +5.13 | 0/23 | 42 | 87 | PASS | [flatten_live_h5.md](factor_mine/flatten_live_h5.md) |
| `union_news_g_h5` | leftover | list | none | -10.90 | +118.78 | 0/23 | 94 | 256 | PASS | [union_news_g_h5.md](factor_mine/union_news_g_h5.md) |
| `union_white_coil_h1` | leftover | list | none | -6.14 | -3.25 | 4/23 | 126 | 0 | PASS | [union_white_coil_h1.md](factor_mine/union_white_coil_h1.md) |
| `union_news_or_h1` | leftover | list | none | +2.48 | +20.27 | 14/23 | 138 | 65 | PASS | [union_news_or_h1.md](factor_mine/union_news_or_h1.md) |
| `union_news_or_net4_h1` | leftover | list | none | -8.06 | -1.79 | 0/23 | 98 | 23 | PASS | [union_news_or_net4_h1.md](factor_mine/union_news_or_net4_h1.md) |
| `union_news_or_net4_conv_h1` | conviction | list | none | -3.84 | +0.19 | 0/23 | 60 | 24 | PASS | [union_news_or_net4_conv_h1.md](factor_mine/union_news_or_net4_conv_h1.md) |
| `short_news_head_h3` | leftover | list | none | +10.85 | +20.59 | 16/23 | 51 | 69 | PASS | [short_news_head_h3.md](factor_mine/short_news_head_h3.md) |
| `union_news_g_cam91_n1_h1` | leftover | list | none | -2.85 | -0.09 | 0/23 | 4 | 0 | PASS | [union_news_g_cam91_n1_h1.md](factor_mine/union_news_g_cam91_n1_h1.md) |
| `union_news_both_h1` | leftover | list | none | +7.92 | +4.91 | 21/23 | 4 | 1 | PASS | [union_news_both_h1.md](factor_mine/union_news_both_h1.md) |
| `union_news_g_cam71_h1` | leftover | list | none | -10.46 | -4.11 | 0/23 | 50 | 2 | PASS | [union_news_g_cam71_h1.md](factor_mine/union_news_g_cam71_h1.md) |
| `union_news_g_conv_h1` | conviction | list | none | +1.09 | +14.82 | 2/23 | 76 | 42 | PASS | [union_news_g_conv_h1.md](factor_mine/union_news_g_conv_h1.md) |
| `union_e_green_h3` | leftover | list | none | -0.34 | -19.21 | 11/23 | 65 | 98 | PASS | [union_e_green_h3.md](factor_mine/union_e_green_h3.md) |
| `flatten_h5` | leftover | list | none | +1.28 | +5.37 | 2/23 | 96 | 263 | PASS | [flatten_h5.md](factor_mine/flatten_h5.md) |
| `flatten_h5_rankw` | rank_w | list | none | -0.48 | +5.37 | 0/23 | 90 | 254 | PASS | [flatten_h5_rankw.md](factor_mine/flatten_h5_rankw.md) |
| `flatten_h5_time` | leftover | time | none | +1.28 | +5.37 | 2/23 | 96 | 263 | PASS | [flatten_h5_time.md](factor_mine/flatten_h5_time.md) |
| `flatten_h5_sboost` | leftover | list | both | -1.17 | +5.37 | 1/23 | 100 | 269 | PASS | [flatten_h5_sboost.md](factor_mine/flatten_h5_sboost.md) |
| `union_h5_sboost` | leftover | list | both | -1.56 | +21.01 | 0/23 | 132 | 360 | PASS | [union_h5_sboost.md](factor_mine/union_h5_sboost.md) |
| `flatten_live_h1_sizeup` | leftover | list | sizeup | +8.22 | +4.45 | 7/23 | 48 | 0 | PASS | [flatten_live_h1_sizeup.md](factor_mine/flatten_live_h1_sizeup.md) |
| `union_h3_cut` | leftover | cut_loser | none | -1.94 | +14.69 | 0/23 | 124 | 229 | PASS | [union_h3_cut.md](factor_mine/union_h3_cut.md) |
| `union_h1_topheavy` | topheavy | list | none | +5.29 | +12.60 | 6/23 | 164 | 88 | PASS | [union_h1_topheavy.md](factor_mine/union_h1_topheavy.md) |

## All other blotters

- [`union_h1`](factor_mine/union_h1.md)
- [`union_h3`](factor_mine/union_h3.md)
- [`union_h5`](factor_mine/union_h5.md)
- [`flatten_h1`](factor_mine/flatten_h1.md)
- [`flatten_h3`](factor_mine/flatten_h3.md)
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
- [`union_join_present_h1`](factor_mine/union_join_present_h1.md)
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
- [`union_hot_n12_h1`](factor_mine/union_hot_n12_h1.md)
- [`union_cond_n4_h3`](factor_mine/union_cond_n4_h3.md)
- [`union_h3_exit_alarm`](factor_mine/union_h3_exit_alarm.md)
- [`union_h5_exit_alarm`](factor_mine/union_h5_exit_alarm.md)
- [`union_h3_exit_red`](factor_mine/union_h3_exit_red.md)
- [`union_h3_exit_news_r`](factor_mine/union_h3_exit_news_r.md)
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
- [`flatten_h5_topheavy`](factor_mine/flatten_h5_topheavy.md)
- [`flatten_h5_half`](factor_mine/flatten_h5_half.md)
- [`flatten_h5_cut`](factor_mine/flatten_h5_cut.md)
- [`flatten_h5_trail`](factor_mine/flatten_h5_trail.md)
- [`flatten_h5_sizeup`](factor_mine/flatten_h5_sizeup.md)
- [`flatten_h3_rankw`](factor_mine/flatten_h3_rankw.md)
- [`flatten_h3_topheavy`](factor_mine/flatten_h3_topheavy.md)
- [`flatten_h3_half`](factor_mine/flatten_h3_half.md)
- [`flatten_h3_time`](factor_mine/flatten_h3_time.md)
- [`flatten_h3_cut`](factor_mine/flatten_h3_cut.md)
- [`flatten_h3_trail`](factor_mine/flatten_h3_trail.md)
- [`flatten_h3_sboost`](factor_mine/flatten_h3_sboost.md)
- [`flatten_h3_sizeup`](factor_mine/flatten_h3_sizeup.md)
- [`flatten_live_h1_rankw`](factor_mine/flatten_live_h1_rankw.md)
- [`flatten_live_h1_topheavy`](factor_mine/flatten_live_h1_topheavy.md)
- [`flatten_live_h1_half`](factor_mine/flatten_live_h1_half.md)
- [`flatten_live_h1_time`](factor_mine/flatten_live_h1_time.md)
- [`flatten_live_h1_cut`](factor_mine/flatten_live_h1_cut.md)
- [`flatten_live_h1_trail`](factor_mine/flatten_live_h1_trail.md)
- [`flatten_live_h1_sboost`](factor_mine/flatten_live_h1_sboost.md)
- [`union_h5_rankw`](factor_mine/union_h5_rankw.md)
- [`union_h5_topheavy`](factor_mine/union_h5_topheavy.md)
- [`union_h5_half`](factor_mine/union_h5_half.md)
- [`union_h5_time`](factor_mine/union_h5_time.md)
- [`union_h5_cut`](factor_mine/union_h5_cut.md)
- [`union_h5_trail`](factor_mine/union_h5_trail.md)
- [`union_h5_sizeup`](factor_mine/union_h5_sizeup.md)
- [`union_h3_rankw`](factor_mine/union_h3_rankw.md)
- [`union_h3_topheavy`](factor_mine/union_h3_topheavy.md)
- [`union_h3_half`](factor_mine/union_h3_half.md)
- [`union_h3_time`](factor_mine/union_h3_time.md)
- [`union_h3_trail`](factor_mine/union_h3_trail.md)
- [`union_h3_sboost`](factor_mine/union_h3_sboost.md)
- [`union_h3_sizeup`](factor_mine/union_h3_sizeup.md)
- [`union_h1_rankw`](factor_mine/union_h1_rankw.md)
- [`union_h1_half`](factor_mine/union_h1_half.md)
- [`union_h1_time`](factor_mine/union_h1_time.md)
- [`union_h1_cut`](factor_mine/union_h1_cut.md)
- [`union_h1_trail`](factor_mine/union_h1_trail.md)
- [`union_h1_sboost`](factor_mine/union_h1_sboost.md)
- [`union_h1_sizeup`](factor_mine/union_h1_sizeup.md)
- [`union_news_pack_h3`](factor_mine/union_news_pack_h3.md)
- [`union_news_head_h1`](factor_mine/union_news_head_h1.md)
- [`union_news_head_h3`](factor_mine/union_news_head_h3.md)
- [`union_news_g_cond_h1`](factor_mine/union_news_g_cond_h1.md)
- [`short_news_pack_h3`](factor_mine/short_news_pack_h3.md)
- [`union_news_pack_net3_h1`](factor_mine/union_news_pack_net3_h1.md)
- [`union_news_or_net2_h1`](factor_mine/union_news_or_net2_h1.md)
- [`short_news_or_h3`](factor_mine/short_news_or_h3.md)
- [`union_rsi_os_h1`](factor_mine/union_rsi_os_h1.md)
- [`union_rsi_os_h3`](factor_mine/union_rsi_os_h3.md)
- [`union_macd_up_h1`](factor_mine/union_macd_up_h1.md)
- [`union_macd_up_h3`](factor_mine/union_macd_up_h3.md)
- [`union_macd_xup_h1`](factor_mine/union_macd_xup_h1.md)
- [`union_flow_in_h1`](factor_mine/union_flow_in_h1.md)
- [`union_flow_in_h3`](factor_mine/union_flow_in_h3.md)
- [`union_rsi_os_macd_h1`](factor_mine/union_rsi_os_macd_h1.md)
- [`union_flow_in_white_h1`](factor_mine/union_flow_in_white_h1.md)
- [`union_rsi_h1`](factor_mine/union_rsi_h1.md)
- [`union_macd_hist_h1`](factor_mine/union_macd_hist_h1.md)
- [`short_rsi_ob_h1`](factor_mine/short_rsi_ob_h1.md)
- [`short_rsi_ob_h3`](factor_mine/short_rsi_ob_h3.md)
- [`short_macd_dn_h3`](factor_mine/short_macd_dn_h3.md)
- [`combo_se_5050_shared`](factor_mine/combo_se_5050_shared.md)
- [`combo_se_5050_weather`](factor_mine/combo_se_5050_weather.md)
- [`combo_nse_333_shared`](factor_mine/combo_nse_333_shared.md)
- [`combo_en_7030_shared`](factor_mine/combo_en_7030_shared.md)
- [`combo_se_7030_shared`](factor_mine/combo_se_7030_shared.md)
- [`combo_en_5050_shared`](factor_mine/combo_en_5050_shared.md)
- [`combo_ner_5050_shared`](factor_mine/combo_ner_5050_shared.md)
- [`combo_se_5050_split`](factor_mine/combo_se_5050_split.md)
- [`combo_en_3070_shared`](factor_mine/combo_en_3070_shared.md)
- [`combo_hn_5050_shared`](factor_mine/combo_hn_5050_shared.md)
- [`combo_seh_502525_split`](factor_mine/combo_seh_502525_split.md)
- [`combo_hj_5050_shared`](factor_mine/combo_hj_5050_shared.md)
- [`combo_ef_7030_shared`](factor_mine/combo_ef_7030_shared.md)
- [`combo_hn_3070_shared`](factor_mine/combo_hn_3070_shared.md)
- [`combo_eer_5050_shared`](factor_mine/combo_eer_5050_shared.md)
- [`combo_sn_3070_shared`](factor_mine/combo_sn_3070_shared.md)
- [`combo_sf_7030_shared`](factor_mine/combo_sf_7030_shared.md)
- [`combo_sn_7030_shared`](factor_mine/combo_sn_7030_shared.md)
- [`combo_fse_333_shared`](factor_mine/combo_fse_333_shared.md)
- [`combo_sf_5050_shared`](factor_mine/combo_sf_5050_shared.md)
- [`combo_fes_403030_shared`](factor_mine/combo_fes_403030_shared.md)
- [`combo_ef_5050_shared`](factor_mine/combo_ef_5050_shared.md)
- [`combo_fe_5050_shared`](factor_mine/combo_fe_5050_shared.md)
- [`combo_ne1_5050_shared`](factor_mine/combo_ne1_5050_shared.md)
- [`combo_sf_3070_shared`](factor_mine/combo_sf_3070_shared.md)
- [`combo_hf_5050_shared`](factor_mine/combo_hf_5050_shared.md)
- [`combo_fer_5050_shared`](factor_mine/combo_fer_5050_shared.md)
- [`combo_nj_5050_shared`](factor_mine/combo_nj_5050_shared.md)
- [`combo_jf_5050_shared`](factor_mine/combo_jf_5050_shared.md)
- [`combo_ef_3070_shared`](factor_mine/combo_ef_3070_shared.md)
- [`combo_nf_5050_shared`](factor_mine/combo_nf_5050_shared.md)
- [`combo_fh_7030_shared`](factor_mine/combo_fh_7030_shared.md)
- [`combo_fe1_5050_shared`](factor_mine/combo_fe1_5050_shared.md)
- [`combo_e1er_5050_shared`](factor_mine/combo_e1er_5050_shared.md)
