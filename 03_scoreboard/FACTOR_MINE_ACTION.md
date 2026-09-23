# Factor mine action — 2026-08-13 → 2026-09-23

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
| `combo_jse_333_shared` | leftover | list | none | +26.79 | — | 8/29 | 416 | 356 | PASS | [combo_jse_333_shared.md](factor_mine/combo_jse_333_shared.md) |
| `combo_ej_5050_shared` | leftover | list | none | +33.96 | — | 25/29 | 337 | 256 | PASS | [combo_ej_5050_shared.md](factor_mine/combo_ej_5050_shared.md) |
| `combo_es_9010_shared` | leftover | list | none | +31.90 | — | 25/29 | 234 | 303 | PASS | [combo_es_9010_shared.md](factor_mine/combo_es_9010_shared.md) |
| `combo_se_3070_shared` | leftover | list | none | +28.86 | — | 23/29 | 244 | 308 | PASS | [combo_se_3070_shared.md](factor_mine/combo_se_3070_shared.md) |
| `union_e_fresh_h3` | leftover | list | none | +34.49 | +3.19 | 25/29 | 116 | 192 | PASS | [union_e_fresh_h3.md](factor_mine/union_e_fresh_h3.md) |
| `union_news_pack_net2_h1` | leftover | list | none | +21.18 | +11.59 | 21/29 | 62 | 24 | PASS | [union_news_pack_net2_h1.md](factor_mine/union_news_pack_net2_h1.md) |
| `union_hot_n4_holdup` | leftover | list | holdup | +52.85 | +63.05 | 26/29 | 89 | 74 | PASS | [union_hot_n4_holdup.md](factor_mine/union_hot_n4_holdup.md) |
| `overnight_mega_h1` | leftover | list | none | -5.76 | -14.12 | 0/29 | 24 | 14 | PASS | [overnight_mega_h1.md](factor_mine/overnight_mega_h1.md) |
| `combo_sh_macd_5050_shared` | leftover | list | none | +37.03 | — | 26/29 | 184 | 116 | PASS | [combo_sh_macd_5050_shared.md](factor_mine/combo_sh_macd_5050_shared.md) |
| `combo_sh_3070_shared` | leftover | list | none | +34.69 | — | 24/29 | 209 | 148 | PASS | [combo_sh_3070_shared.md](factor_mine/combo_sh_3070_shared.md) |
| `combo_ee1_3070_shared` | leftover | list | none | +34.49 | — | 25/29 | 116 | 239 | PASS | [combo_ee1_3070_shared.md](factor_mine/combo_ee1_3070_shared.md) |
| `combo_ee1_5050_shared` | leftover | list | none | +34.49 | — | 25/29 | 116 | 239 | PASS | [combo_ee1_5050_shared.md](factor_mine/combo_ee1_5050_shared.md) |
| `combo_ee1_7030_shared` | leftover | list | none | +34.49 | — | 25/29 | 116 | 239 | PASS | [combo_ee1_7030_shared.md](factor_mine/combo_ee1_7030_shared.md) |
| `combo_se_5050_skip` | leftover | list | none | +33.86 | — | 25/29 | 236 | 312 | PASS | [combo_se_5050_skip.md](factor_mine/combo_se_5050_skip.md) |
| `combo_seh_333_skip` | leftover | list | none | +32.50 | — | 25/29 | 343 | 352 | PASS | [combo_seh_333_skip.md](factor_mine/combo_seh_333_skip.md) |
| `combo_sh_5050_shared` | leftover | list | none | +32.15 | — | 24/29 | 209 | 148 | PASS | [combo_sh_5050_shared.md](factor_mine/combo_sh_5050_shared.md) |
| `combo_es_8020_shared` | leftover | list | none | +31.39 | — | 24/29 | 244 | 308 | PASS | [combo_es_8020_shared.md](factor_mine/combo_es_8020_shared.md) |
| `union_hot_n4_h1` | leftover | list | none | +30.77 | +63.05 | 25/29 | 115 | 39 | PASS | [union_hot_n4_h1.md](factor_mine/union_hot_n4_h1.md) |
| `union_ret_5_h3` | leftover | list | none | +28.56 | +54.78 | 26/29 | 188 | 281 | PASS | [union_ret_5_h3.md](factor_mine/union_ret_5_h3.md) |
| `combo_ehs_702010_shared` | leftover | list | none | +27.84 | — | 25/29 | 347 | 343 | PASS | [combo_ehs_702010_shared.md](factor_mine/combo_ehs_702010_shared.md) |
| `combo_eh_7030_shared` | leftover | list | none | +27.43 | — | 25/29 | 274 | 250 | PASS | [combo_eh_7030_shared.md](factor_mine/combo_eh_7030_shared.md) |
| `combo_eh_5050_shared` | leftover | list | none | +26.54 | — | 25/29 | 274 | 250 | PASS | [combo_eh_5050_shared.md](factor_mine/combo_eh_5050_shared.md) |
| `combo_ehs_601525_shared` | leftover | list | none | +26.29 | — | 24/29 | 357 | 348 | PASS | [combo_ehs_601525_shared.md](factor_mine/combo_ehs_601525_shared.md) |
| `combo_en_7030_shared` | leftover | list | none | +25.67 | — | 25/29 | 294 | 277 | PASS | [combo_en_7030_shared.md](factor_mine/combo_en_7030_shared.md) |
| `combo_eh_3070_shared` | leftover | list | none | +25.11 | — | 25/29 | 272 | 249 | PASS | [combo_eh_3070_shared.md](factor_mine/combo_eh_3070_shared.md) |
| `combo_he1_5050_shared` | leftover | list | none | +23.08 | — | 25/29 | 286 | 87 | PASS | [combo_he1_5050_shared.md](factor_mine/combo_he1_5050_shared.md) |
| `combo_jer_5050_shared` | leftover | list | none | +27.14 | — | 9/29 | 341 | 259 | PASS | [combo_jer_5050_shared.md](factor_mine/combo_jer_5050_shared.md) |
| `combo_sj_3070_shared` | leftover | list | none | +20.56 | — | 11/29 | 312 | 152 | PASS | [combo_sj_3070_shared.md](factor_mine/combo_sj_3070_shared.md) |
| `combo_sf_7030_shared` | leftover | list | none | +14.34 | — | 13/29 | 218 | 433 | PASS | [combo_sf_7030_shared.md](factor_mine/combo_sf_7030_shared.md) |
| `combo_sf_5050_shared` | leftover | list | none | +13.47 | — | 8/29 | 214 | 433 | PASS | [combo_sf_5050_shared.md](factor_mine/combo_sf_5050_shared.md) |
| `combo_sf_3070_shared` | leftover | list | none | +9.16 | — | 4/29 | 213 | 431 | PASS | [combo_sf_3070_shared.md](factor_mine/combo_sf_3070_shared.md) |
| `flatten_live_h1` | leftover | list | none | +8.22 | +4.45 | 7/29 | 48 | 0 | PASS | [flatten_live_h1.md](factor_mine/flatten_live_h1.md) |
| `flatten_live_h3` | leftover | list | none | -6.24 | +6.87 | 0/29 | 42 | 45 | PASS | [flatten_live_h3.md](factor_mine/flatten_live_h3.md) |
| `flatten_live_h5` | leftover | list | none | -5.51 | +5.13 | 0/29 | 42 | 87 | PASS | [flatten_live_h5.md](factor_mine/flatten_live_h5.md) |
| `union_news_g_h5` | leftover | list | none | -17.20 | +95.63 | 1/29 | 127 | 322 | PASS | [union_news_g_h5.md](factor_mine/union_news_g_h5.md) |
| `union_white_coil_h1` | leftover | list | none | -3.97 | -4.33 | 9/29 | 144 | 0 | PASS | [union_white_coil_h1.md](factor_mine/union_white_coil_h1.md) |
| `overnight_mega_h2` | leftover | list | none | -7.35 | -6.81 | 1/29 | 10 | 26 | PASS | [overnight_mega_h2.md](factor_mine/overnight_mega_h2.md) |
| `overnight_h1` | leftover | list | none | -19.14 | -21.82 | 0/29 | 133 | 71 | PASS | [overnight_h1.md](factor_mine/overnight_h1.md) |
| `overnight_mega_green_h1` | leftover | list | none | -8.87 | -9.06 | 0/29 | 20 | 7 | PASS | [overnight_mega_green_h1.md](factor_mine/overnight_mega_green_h1.md) |
| `combo_oh_5050_shared` | leftover | list | none | +14.65 | — | 25/29 | 139 | 53 | PASS | [combo_oh_5050_shared.md](factor_mine/combo_oh_5050_shared.md) |
| `union_news_pack_h1` | leftover | list | none | +16.67 | +10.47 | 21/29 | 68 | 25 | PASS | [union_news_pack_h1.md](factor_mine/union_news_pack_h1.md) |
| `union_news_or_h1` | leftover | list | none | -3.79 | +7.99 | 0/29 | 195 | 65 | PASS | [union_news_or_h1.md](factor_mine/union_news_or_h1.md) |
| `union_news_or_net3_h1` | leftover | list | none | -19.34 | -13.10 | 0/29 | 141 | 29 | PASS | [union_news_or_net3_h1.md](factor_mine/union_news_or_net3_h1.md) |
| `union_news_or_net4_h1` | leftover | list | none | -12.26 | -9.75 | 0/29 | 131 | 25 | PASS | [union_news_or_net4_h1.md](factor_mine/union_news_or_net4_h1.md) |
| `union_news_or_net4_rw_h1` | rank_w | list | none | -5.50 | -7.33 | 1/29 | 89 | 23 | PASS | [union_news_or_net4_rw_h1.md](factor_mine/union_news_or_net4_rw_h1.md) |
| `union_news_or_net4_conv_h1` | conviction | list | none | +1.49 | -7.33 | 7/29 | 87 | 24 | PASS | [union_news_or_net4_conv_h1.md](factor_mine/union_news_or_net4_conv_h1.md) |
| `short_news_head_h3` | leftover | list | none | +4.62 | +1.11 | 12/29 | 76 | 89 | PASS | [short_news_head_h3.md](factor_mine/short_news_head_h3.md) |
| `combo_ps_5050_shared` | leftover | list | none | +10.81 | — | 16/29 | 170 | 137 | PASS | [combo_ps_5050_shared.md](factor_mine/combo_ps_5050_shared.md) |
| `combo_ps_7030_shared` | leftover | list | none | +14.42 | — | 18/29 | 170 | 137 | PASS | [combo_ps_7030_shared.md](factor_mine/combo_ps_7030_shared.md) |
| `combo_p2s_5050_shared` | leftover | list | none | +12.59 | — | 18/29 | 164 | 136 | PASS | [combo_p2s_5050_shared.md](factor_mine/combo_p2s_5050_shared.md) |
| `union_news_g_cam91_n1_h1` | leftover | list | none | +6.39 | -2.85 | 27/29 | 6 | 0 | PASS | [union_news_g_cam91_n1_h1.md](factor_mine/union_news_g_cam91_n1_h1.md) |
| `union_news_both_h1` | leftover | list | none | +7.29 | +4.21 | 21/29 | 6 | 2 | PASS | [union_news_both_h1.md](factor_mine/union_news_both_h1.md) |
| `union_news_g_cam71_h1` | leftover | list | none | -8.16 | -10.07 | 6/29 | 62 | 8 | PASS | [union_news_g_cam71_h1.md](factor_mine/union_news_g_cam71_h1.md) |
| `union_news_g_conv_h1` | conviction | list | none | +11.63 | +8.30 | 19/29 | 111 | 42 | PASS | [union_news_g_conv_h1.md](factor_mine/union_news_g_conv_h1.md) |
| `union_e_green_h3` | leftover | list | none | +1.42 | -22.09 | 21/29 | 67 | 103 | PASS | [union_e_green_h3.md](factor_mine/union_e_green_h3.md) |
| `union_white_any_h1` | leftover | list | none | -10.19 | -10.91 | 0/29 | 176 | 0 | PASS | [union_white_any_h1.md](factor_mine/union_white_any_h1.md) |
| `union_white_any_h2` | leftover | list | none | -5.81 | +5.07 | 4/29 | 160 | 81 | PASS | [union_white_any_h2.md](factor_mine/union_white_any_h2.md) |
| `union_white_any_h3` | leftover | list | none | -16.38 | -7.98 | 2/29 | 130 | 141 | PASS | [union_white_any_h3.md](factor_mine/union_white_any_h3.md) |
| `union_white_any_h5` | leftover | list | none | -0.85 | -15.92 | 1/29 | 129 | 265 | PASS | [union_white_any_h5.md](factor_mine/union_white_any_h5.md) |
| `union_white_both_n4_h1` | leftover | list | none | +15.14 | +7.29 | 7/29 | 86 | 0 | PASS | [union_white_both_n4_h1.md](factor_mine/union_white_both_n4_h1.md) |
| `union_white_both_n4_h2` | leftover | list | none | +19.87 | +21.04 | 17/29 | 72 | 40 | PASS | [union_white_both_n4_h2.md](factor_mine/union_white_both_n4_h2.md) |
| `union_white_both_n4_h5` | leftover | list | none | +18.79 | +21.47 | 9/29 | 66 | 133 | PASS | [union_white_both_n4_h5.md](factor_mine/union_white_both_n4_h5.md) |
| `union_white_both_n4_h5_s12` | leftover | list | none | +20.63 | +21.47 | 12/29 | 66 | 129 | PASS | [union_white_both_n4_h5_s12.md](factor_mine/union_white_both_n4_h5_s12.md) |
| `flatten_h5_s8` | leftover | list | none | +6.28 | +0.02 | 3/29 | 134 | 305 | PASS | [flatten_h5_s8.md](factor_mine/flatten_h5_s8.md) |
| `flatten_h5` | leftover | list | none | +8.93 | +0.02 | 11/29 | 127 | 330 | PASS | [flatten_h5.md](factor_mine/flatten_h5.md) |
| `flatten_h5_rankw` | rank_w | list | none | +5.03 | +0.02 | 7/29 | 115 | 321 | PASS | [flatten_h5_rankw.md](factor_mine/flatten_h5_rankw.md) |
| `flatten_h5_time` | leftover | time | none | +8.93 | +0.02 | 11/29 | 127 | 330 | PASS | [flatten_h5_time.md](factor_mine/flatten_h5_time.md) |
| `flatten_h5_sboost` | leftover | list | both | +6.55 | +0.02 | 10/29 | 131 | 336 | PASS | [flatten_h5_sboost.md](factor_mine/flatten_h5_sboost.md) |
| `union_h5_sboost` | leftover | list | both | +5.61 | +17.89 | 11/29 | 189 | 488 | PASS | [union_h5_sboost.md](factor_mine/union_h5_sboost.md) |
| `flatten_live_h1_sizeup` | leftover | list | sizeup | +8.22 | +4.45 | 7/29 | 48 | 0 | PASS | [flatten_live_h1_sizeup.md](factor_mine/flatten_live_h1_sizeup.md) |
| `union_h3_cut` | leftover | cut_loser | none | -1.71 | +4.58 | 0/29 | 176 | 278 | PASS | [union_h3_cut.md](factor_mine/union_h3_cut.md) |
| `union_h1_topheavy` | topheavy | list | none | +3.52 | +5.17 | 6/29 | 250 | 87 | PASS | [union_h1_topheavy.md](factor_mine/union_h1_topheavy.md) |

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
- [`overnight_h3`](factor_mine/overnight_h3.md)
- [`overnight_h5`](factor_mine/overnight_h5.md)
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
- [`union_earn_react_h3`](factor_mine/union_earn_react_h3.md)
- [`union_overnight_h1`](factor_mine/union_overnight_h1.md)
- [`union_overnight_h3`](factor_mine/union_overnight_h3.md)
- [`union_e_fresh_h1`](factor_mine/union_e_fresh_h1.md)
- [`union_r_up_h1`](factor_mine/union_r_up_h1.md)
- [`union_r_up_h3`](factor_mine/union_r_up_h3.md)
- [`union_break10_h1`](factor_mine/union_break10_h1.md)
- [`union_break10_h3`](factor_mine/union_break10_h3.md)
- [`union_rsi_os_h1`](factor_mine/union_rsi_os_h1.md)
- [`union_rsi_os_h3`](factor_mine/union_rsi_os_h3.md)
- [`union_macd_up_h1`](factor_mine/union_macd_up_h1.md)
- [`union_macd_up_h3`](factor_mine/union_macd_up_h3.md)
- [`union_macd_xup_h1`](factor_mine/union_macd_xup_h1.md)
- [`union_macd_xup_h3`](factor_mine/union_macd_xup_h3.md)
- [`union_flow_in_h1`](factor_mine/union_flow_in_h1.md)
- [`union_flow_in_h3`](factor_mine/union_flow_in_h3.md)
- [`union_vol_g_h5`](factor_mine/union_vol_g_h5.md)
- [`union_coil_off_h5`](factor_mine/union_coil_off_h5.md)
- [`union_last_green_h5`](factor_mine/union_last_green_h5.md)
- [`union_white_h5`](factor_mine/union_white_h5.md)
- [`union_rsi_os_h5`](factor_mine/union_rsi_os_h5.md)
- [`union_flow_in_h5`](factor_mine/union_flow_in_h5.md)
- [`union_news_head_h1`](factor_mine/union_news_head_h1.md)
- [`union_news_g_cond_h1`](factor_mine/union_news_g_cond_h1.md)
- [`union_news_g_cam61_h1`](factor_mine/union_news_g_cam61_h1.md)
- [`union_news_or_net2_h1`](factor_mine/union_news_or_net2_h1.md)
- [`union_news_or_net5_h1`](factor_mine/union_news_or_net5_h1.md)
- [`union_news_pack_net3_h1`](factor_mine/union_news_pack_net3_h1.md)
- [`union_news_pack_h3`](factor_mine/union_news_pack_h3.md)
- [`union_news_head_h3`](factor_mine/union_news_head_h3.md)
- [`union_news_or_h3`](factor_mine/union_news_or_h3.md)
- [`union_news_both_h3`](factor_mine/union_news_both_h3.md)
- [`union_news_g_cond_h3`](factor_mine/union_news_g_cond_h3.md)
- [`union_news_g_cam71_h3`](factor_mine/union_news_g_cam71_h3.md)
- [`union_news_g_cam61_h3`](factor_mine/union_news_g_cam61_h3.md)
- [`union_news_or_net2_h3`](factor_mine/union_news_or_net2_h3.md)
- [`union_news_or_net3_h3`](factor_mine/union_news_or_net3_h3.md)
- [`union_news_or_net4_h3`](factor_mine/union_news_or_net4_h3.md)
- [`union_news_or_net5_h3`](factor_mine/union_news_or_net5_h3.md)
- [`union_news_pack_net3_h3`](factor_mine/union_news_pack_net3_h3.md)
- [`union_news_pack_net2_h3`](factor_mine/union_news_pack_net2_h3.md)
- [`union_news_g_cam71_n2_h1`](factor_mine/union_news_g_cam71_n2_h1.md)
- [`union_news_g_conv_h3`](factor_mine/union_news_g_conv_h3.md)
- [`union_news_g_cam71_conv_h1`](factor_mine/union_news_g_cam71_conv_h1.md)
- [`short_news_pack_h3`](factor_mine/short_news_pack_h3.md)
- [`short_news_or_h3`](factor_mine/short_news_or_h3.md)
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
- [`union_rsi_os_macd_h1`](factor_mine/union_rsi_os_macd_h1.md)
- [`union_rsi_os_macd_h3`](factor_mine/union_rsi_os_macd_h3.md)
- [`union_flow_in_white_h1`](factor_mine/union_flow_in_white_h1.md)
- [`union_flow_in_white_h3`](factor_mine/union_flow_in_white_h3.md)
- [`flatten_vol_g_h3`](factor_mine/flatten_vol_g_h3.md)
- [`ohlc_hot_coil_h1`](factor_mine/ohlc_hot_coil_h1.md)
- [`union_hot_score_h1`](factor_mine/union_hot_score_h1.md)
- [`union_hot_score_h3`](factor_mine/union_hot_score_h3.md)
- [`union_candle_score_h1`](factor_mine/union_candle_score_h1.md)
- [`union_candle_score_h3`](factor_mine/union_candle_score_h3.md)
- [`union_ret_5_h1`](factor_mine/union_ret_5_h1.md)
- [`union_cond_h1`](factor_mine/union_cond_h1.md)
- [`union_cond_h3`](factor_mine/union_cond_h3.md)
- [`union_w_hot_cond_h1`](factor_mine/union_w_hot_cond_h1.md)
- [`union_w_hot_cond_h3`](factor_mine/union_w_hot_cond_h3.md)
- [`union_w_hot_candle_h1`](factor_mine/union_w_hot_candle_h1.md)
- [`union_w_hot_candle_h3`](factor_mine/union_w_hot_candle_h3.md)
- [`union_rsi_h1`](factor_mine/union_rsi_h1.md)
- [`union_rsi_h3`](factor_mine/union_rsi_h3.md)
- [`union_macd_hist_h1`](factor_mine/union_macd_hist_h1.md)
- [`union_macd_hist_h3`](factor_mine/union_macd_hist_h3.md)
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
- [`short_rsi_ob_h1`](factor_mine/short_rsi_ob_h1.md)
- [`short_rsi_ob_h3`](factor_mine/short_rsi_ob_h3.md)
- [`short_macd_dn_h1`](factor_mine/short_macd_dn_h1.md)
- [`short_macd_dn_h3`](factor_mine/short_macd_dn_h3.md)
- [`short_news_r_macd_h3`](factor_mine/short_news_r_macd_h3.md)
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
- [`union_white_yday_h1`](factor_mine/union_white_yday_h1.md)
- [`union_white_yday_h3`](factor_mine/union_white_yday_h3.md)
- [`flatten_white_yday_h5`](factor_mine/flatten_white_yday_h5.md)
- [`union_white_both_n4_h3`](factor_mine/union_white_both_n4_h3.md)
- [`union_clk_mom_break_peer_h1`](factor_mine/union_clk_mom_break_peer_h1.md)
- [`union_clk_fresh_cat_coil_h1`](factor_mine/union_clk_fresh_cat_coil_h1.md)
- [`union_clk_earn_guide_react_h1`](factor_mine/union_clk_earn_guide_react_h1.md)
- [`short_clk_neg_weak_fail_h3`](factor_mine/short_clk_neg_weak_fail_h3.md)
- [`short_clk_ext_veto_h3`](factor_mine/short_clk_ext_veto_h3.md)
- [`union_clk_hold_vs_sector_h1`](factor_mine/union_clk_hold_vs_sector_h1.md)
- [`union_clk_insider_cash_stab_h3`](factor_mine/union_clk_insider_cash_stab_h3.md)
- [`union_clk_flow_coil_h1`](factor_mine/union_clk_flow_coil_h1.md)
- [`union_clk_r_up_coil_h1`](factor_mine/union_clk_r_up_coil_h1.md)
- [`union_clk_nr7_mom_h1`](factor_mine/union_clk_nr7_mom_h1.md)
- [`union_oppset_h1`](factor_mine/union_oppset_h1.md)
- [`oppset_h1`](factor_mine/oppset_h1.md)
- [`union_clk_mom_break_peer_opp_h1`](factor_mine/union_clk_mom_break_peer_opp_h1.md)
- [`union_clk_fresh_cat_coil_opp_h1`](factor_mine/union_clk_fresh_cat_coil_opp_h1.md)
- [`short_clk_neg_weak_fail_opp_h3`](factor_mine/short_clk_neg_weak_fail_opp_h3.md)
- [`short_clk_ext_veto_opp_h3`](factor_mine/short_clk_ext_veto_opp_h3.md)
- [`union_clk_hold_vs_sector_opp_h1`](factor_mine/union_clk_hold_vs_sector_opp_h1.md)
- [`union_clk_nr7_mom_opp_h1`](factor_mine/union_clk_nr7_mom_opp_h1.md)
- [`combo_se_5050_shared`](factor_mine/combo_se_5050_shared.md)
- [`combo_se_5050_weather`](factor_mine/combo_se_5050_weather.md)
- [`combo_seh_333_shared`](factor_mine/combo_seh_333_shared.md)
- [`combo_seh_333_weather`](factor_mine/combo_seh_333_weather.md)
- [`combo_seh_404020_shared`](factor_mine/combo_seh_404020_shared.md)
- [`combo_sh_7030_shared`](factor_mine/combo_sh_7030_shared.md)
- [`combo_seh_403525_shared`](factor_mine/combo_seh_403525_shared.md)
- [`combo_en_5050_shared`](factor_mine/combo_en_5050_shared.md)
- [`combo_seh_502525_shared`](factor_mine/combo_seh_502525_shared.md)
- [`combo_seh_451540_shared`](factor_mine/combo_seh_451540_shared.md)
- [`combo_se_7030_shared`](factor_mine/combo_se_7030_shared.md)
- [`combo_seh_333_split`](factor_mine/combo_seh_333_split.md)
- [`combo_eer_5050_shared`](factor_mine/combo_eer_5050_shared.md)
- [`combo_ef_7030_shared`](factor_mine/combo_ef_7030_shared.md)
- [`combo_seh_601525_shared`](factor_mine/combo_seh_601525_shared.md)
- [`combo_her_5050_shared`](factor_mine/combo_her_5050_shared.md)
- [`combo_hn_7030_shared`](factor_mine/combo_hn_7030_shared.md)
- [`combo_se_5050_split`](factor_mine/combo_se_5050_split.md)
- [`combo_en_3070_shared`](factor_mine/combo_en_3070_shared.md)
- [`combo_sj_5050_shared`](factor_mine/combo_sj_5050_shared.md)
- [`combo_hj_5050_shared`](factor_mine/combo_hj_5050_shared.md)
- [`combo_je1_5050_shared`](factor_mine/combo_je1_5050_shared.md)
- [`combo_ers_7030_shared`](factor_mine/combo_ers_7030_shared.md)
- [`combo_ser_5050_shared`](factor_mine/combo_ser_5050_shared.md)
- [`combo_seh_502525_split`](factor_mine/combo_seh_502525_split.md)
- [`combo_fse_333_shared`](factor_mine/combo_fse_333_shared.md)
- [`combo_ner_5050_shared`](factor_mine/combo_ner_5050_shared.md)
- [`combo_hn_5050_shared`](factor_mine/combo_hn_5050_shared.md)
- [`combo_fes_403030_shared`](factor_mine/combo_fes_403030_shared.md)
- [`combo_ef_5050_shared`](factor_mine/combo_ef_5050_shared.md)
- [`combo_fe_5050_shared`](factor_mine/combo_fe_5050_shared.md)
- [`combo_nse_333_shared`](factor_mine/combo_nse_333_shared.md)
- [`combo_sj_7030_shared`](factor_mine/combo_sj_7030_shared.md)
- [`combo_e1s_7030_shared`](factor_mine/combo_e1s_7030_shared.md)
- [`combo_se1_5050_shared`](factor_mine/combo_se1_5050_shared.md)
- [`combo_hf_5050_shared`](factor_mine/combo_hf_5050_shared.md)
- [`combo_hn_3070_shared`](factor_mine/combo_hn_3070_shared.md)
- [`combo_fer_5050_shared`](factor_mine/combo_fer_5050_shared.md)
- [`combo_ef_3070_shared`](factor_mine/combo_ef_3070_shared.md)
- [`combo_fh_7030_shared`](factor_mine/combo_fh_7030_shared.md)
- [`combo_ne1_5050_shared`](factor_mine/combo_ne1_5050_shared.md)
- [`combo_fe1_5050_shared`](factor_mine/combo_fe1_5050_shared.md)
- [`combo_jf_5050_shared`](factor_mine/combo_jf_5050_shared.md)
- [`combo_snj_333_shared`](factor_mine/combo_snj_333_shared.md)
- [`combo_nj_5050_shared`](factor_mine/combo_nj_5050_shared.md)
- [`combo_nf_5050_shared`](factor_mine/combo_nf_5050_shared.md)
- [`combo_sn_7030_shared`](factor_mine/combo_sn_7030_shared.md)
- [`combo_e1er_5050_shared`](factor_mine/combo_e1er_5050_shared.md)
- [`combo_sn_5050_shared`](factor_mine/combo_sn_5050_shared.md)
- [`combo_sn_3070_shared`](factor_mine/combo_sn_3070_shared.md)
