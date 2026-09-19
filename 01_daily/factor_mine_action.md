# Factor mine action — 2026-08-13 → 2026-09-18

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
| `combo_jse_333_shared` | leftover | list | none | +32.37 | — | 20/26 | 303 | 297 | PASS | [combo_jse_333_shared.md](factor_mine/combo_jse_333_shared.md) |
| `combo_ej_5050_shared` | leftover | list | none | +38.77 | — | 26/26 | 265 | 236 | PASS | [combo_ej_5050_shared.md](factor_mine/combo_ej_5050_shared.md) |
| `combo_es_9010_shared` | leftover | list | none | +36.62 | — | 25/26 | 194 | 266 | PASS | [combo_es_9010_shared.md](factor_mine/combo_es_9010_shared.md) |
| `combo_se_3070_shared` | leftover | list | none | +29.80 | — | 24/26 | 200 | 269 | PASS | [combo_se_3070_shared.md](factor_mine/combo_se_3070_shared.md) |
| `union_e_fresh_h3` | leftover | list | none | +38.47 | +4.57 | 25/26 | 107 | 174 | PASS | [union_e_fresh_h3.md](factor_mine/union_e_fresh_h3.md) |
| `combo_ee1_3070_shared` | leftover | list | none | +38.47 | — | 25/26 | 107 | 221 | PASS | [combo_ee1_3070_shared.md](factor_mine/combo_ee1_3070_shared.md) |
| `combo_ee1_5050_shared` | leftover | list | none | +38.47 | — | 25/26 | 107 | 221 | PASS | [combo_ee1_5050_shared.md](factor_mine/combo_ee1_5050_shared.md) |
| `combo_ee1_7030_shared` | leftover | list | none | +38.47 | — | 25/26 | 107 | 221 | PASS | [combo_ee1_7030_shared.md](factor_mine/combo_ee1_7030_shared.md) |
| `combo_sh_3070_shared` | leftover | list | none | +36.38 | — | 22/26 | 169 | 128 | PASS | [combo_sh_3070_shared.md](factor_mine/combo_sh_3070_shared.md) |
| `combo_es_8020_shared` | leftover | list | none | +34.06 | — | 25/26 | 200 | 269 | PASS | [combo_es_8020_shared.md](factor_mine/combo_es_8020_shared.md) |
| `combo_eh_7030_shared` | leftover | list | none | +33.06 | — | 25/26 | 232 | 229 | PASS | [combo_eh_7030_shared.md](factor_mine/combo_eh_7030_shared.md) |
| `combo_seh_333_skip` | leftover | list | none | +32.98 | — | 25/26 | 283 | 313 | PASS | [combo_seh_333_skip.md](factor_mine/combo_seh_333_skip.md) |
| `combo_ehs_702010_shared` | leftover | list | none | +32.22 | — | 25/26 | 292 | 306 | PASS | [combo_ehs_702010_shared.md](factor_mine/combo_ehs_702010_shared.md) |
| `combo_jer_5050_shared` | leftover | list | none | +31.81 | — | 24/26 | 270 | 239 | PASS | [combo_jer_5050_shared.md](factor_mine/combo_jer_5050_shared.md) |
| `union_hot_n4_h1` | leftover | list | none | +31.73 | +51.64 | 22/26 | 94 | 38 | PASS | [union_hot_n4_h1.md](factor_mine/union_hot_n4_h1.md) |
| `combo_eh_5050_shared` | leftover | list | none | +31.35 | — | 25/26 | 232 | 229 | PASS | [combo_eh_5050_shared.md](factor_mine/combo_eh_5050_shared.md) |
| `combo_en_7030_shared` | leftover | list | none | +30.07 | — | 25/26 | 236 | 267 | PASS | [combo_en_7030_shared.md](factor_mine/combo_en_7030_shared.md) |
| `combo_ehs_601525_shared` | leftover | list | none | +29.54 | — | 25/26 | 296 | 308 | PASS | [combo_ehs_601525_shared.md](factor_mine/combo_ehs_601525_shared.md) |
| `combo_eh_3070_shared` | leftover | list | none | +29.27 | — | 25/26 | 232 | 229 | PASS | [combo_eh_3070_shared.md](factor_mine/combo_eh_3070_shared.md) |
| `combo_se_5050_skip` | leftover | list | none | +28.95 | — | 20/26 | 173 | 264 | PASS | [combo_se_5050_skip.md](factor_mine/combo_se_5050_skip.md) |
| `combo_seh_333_shared` | leftover | list | none | +27.15 | — | 22/26 | 294 | 307 | PASS | [combo_seh_333_shared.md](factor_mine/combo_seh_333_shared.md) |
| `combo_seh_333_weather` | leftover | list | none | +27.15 | — | 22/26 | 294 | 307 | PASS | [combo_seh_333_weather.md](factor_mine/combo_seh_333_weather.md) |
| `combo_sj_3070_shared` | leftover | list | none | +27.13 | — | 26/26 | 243 | 133 | PASS | [combo_sj_3070_shared.md](factor_mine/combo_sj_3070_shared.md) |
| `union_news_pack_net2_h1` | leftover | list | none | +26.01 | +16.92 | 21/26 | 58 | 19 | PASS | [union_news_pack_net2_h1.md](factor_mine/union_news_pack_net2_h1.md) |
| `combo_en_5050_shared` | leftover | list | none | +25.51 | — | 24/26 | 240 | 263 | PASS | [combo_en_5050_shared.md](factor_mine/combo_en_5050_shared.md) |
| `combo_he1_5050_shared` | leftover | list | none | +23.41 | — | 25/26 | 238 | 86 | PASS | [combo_he1_5050_shared.md](factor_mine/combo_he1_5050_shared.md) |
| `combo_eer_5050_shared` | leftover | list | none | +22.73 | — | 24/26 | 125 | 243 | PASS | [combo_eer_5050_shared.md](factor_mine/combo_eer_5050_shared.md) |
| `combo_hn_7030_shared` | leftover | list | none | +22.41 | — | 22/26 | 230 | 108 | PASS | [combo_hn_7030_shared.md](factor_mine/combo_hn_7030_shared.md) |
| `combo_je1_5050_shared` | leftover | list | none | +22.32 | — | 26/26 | 312 | 86 | PASS | [combo_je1_5050_shared.md](factor_mine/combo_je1_5050_shared.md) |
| `union_vol_green_h1` | leftover | list | none | +21.69 | +5.03 | 26/26 | 176 | 60 | PASS | [union_vol_green_h1.md](factor_mine/union_vol_green_h1.md) |
| `union_news_pack_h1` | leftover | list | none | +21.55 | +16.25 | 21/26 | 64 | 20 | PASS | [union_news_pack_h1.md](factor_mine/union_news_pack_h1.md) |
| `combo_seh_333_split` | leftover | list | none | +21.47 | — | 22/26 | 284 | 308 | PASS | [combo_seh_333_split.md](factor_mine/combo_seh_333_split.md) |
| `combo_her_5050_shared` | leftover | list | none | +21.29 | — | 22/26 | 233 | 228 | PASS | [combo_her_5050_shared.md](factor_mine/combo_her_5050_shared.md) |
| `combo_ef_7030_shared` | leftover | list | none | +20.33 | — | 21/26 | 205 | 437 | PASS | [combo_ef_7030_shared.md](factor_mine/combo_ef_7030_shared.md) |
| `combo_hj_5050_shared` | leftover | list | none | +20.29 | — | 26/26 | 236 | 77 | PASS | [combo_hj_5050_shared.md](factor_mine/combo_hj_5050_shared.md) |
| `union_join_vol_green_h1` | leftover | list | none | +20.16 | +10.96 | 26/26 | 166 | 39 | PASS | [union_join_vol_green_h1.md](factor_mine/union_join_vol_green_h1.md) |
| `combo_sh_5050_shared` | leftover | list | none | +34.80 | — | 18/26 | 169 | 128 | PASS | [combo_sh_5050_shared.md](factor_mine/combo_sh_5050_shared.md) |
| `combo_sj_5050_shared` | leftover | list | none | +24.66 | — | 22/26 | 243 | 133 | PASS | [combo_sj_5050_shared.md](factor_mine/combo_sj_5050_shared.md) |
| `combo_e1s_7030_shared` | leftover | list | none | +17.38 | — | 11/26 | 218 | 131 | PASS | [combo_e1s_7030_shared.md](factor_mine/combo_e1s_7030_shared.md) |
| `combo_sf_3070_shared` | leftover | list | none | +3.77 | — | 2/26 | 186 | 369 | PASS | [combo_sf_3070_shared.md](factor_mine/combo_sf_3070_shared.md) |
| `flatten_live_h1` | leftover | list | none | +8.22 | +4.45 | 7/26 | 48 | 0 | PASS | [flatten_live_h1.md](factor_mine/flatten_live_h1.md) |
| `flatten_live_h3` | leftover | list | none | -6.24 | +6.87 | 0/26 | 42 | 45 | PASS | [flatten_live_h3.md](factor_mine/flatten_live_h3.md) |
| `flatten_live_h5` | leftover | list | none | -5.51 | +5.13 | 0/26 | 42 | 87 | PASS | [flatten_live_h5.md](factor_mine/flatten_live_h5.md) |
| `union_news_g_h5` | leftover | list | none | -12.84 | +93.04 | 0/26 | 107 | 271 | PASS | [union_news_g_h5.md](factor_mine/union_news_g_h5.md) |
| `union_white_coil_h1` | leftover | list | none | -6.04 | -3.01 | 9/26 | 134 | 0 | PASS | [union_white_coil_h1.md](factor_mine/union_white_coil_h1.md) |
| `union_news_or_h1` | leftover | list | none | +0.50 | +16.47 | 4/26 | 154 | 66 | PASS | [union_news_or_h1.md](factor_mine/union_news_or_h1.md) |
| `union_news_or_net4_h1` | leftover | list | none | -7.21 | -1.08 | 4/26 | 108 | 23 | PASS | [union_news_or_net4_h1.md](factor_mine/union_news_or_net4_h1.md) |
| `union_news_or_net4_conv_h1` | conviction | list | none | -0.67 | +0.92 | 4/26 | 70 | 24 | PASS | [union_news_or_net4_conv_h1.md](factor_mine/union_news_or_net4_conv_h1.md) |
| `short_news_head_h3` | leftover | list | none | +1.50 | -1.06 | 3/26 | 57 | 72 | PASS | [short_news_head_h3.md](factor_mine/short_news_head_h3.md) |
| `combo_ps_5050_shared` | leftover | list | none | +13.81 | — | 12/26 | 141 | 115 | PASS | [combo_ps_5050_shared.md](factor_mine/combo_ps_5050_shared.md) |
| `combo_ps_7030_shared` | leftover | list | none | +15.13 | — | 16/26 | 143 | 114 | PASS | [combo_ps_7030_shared.md](factor_mine/combo_ps_7030_shared.md) |
| `combo_p2s_5050_shared` | leftover | list | none | +14.63 | — | 16/26 | 137 | 113 | PASS | [combo_p2s_5050_shared.md](factor_mine/combo_p2s_5050_shared.md) |
| `union_news_g_cam91_n1_h1` | leftover | list | none | -2.85 | -0.09 | 0/26 | 4 | 0 | PASS | [union_news_g_cam91_n1_h1.md](factor_mine/union_news_g_cam91_n1_h1.md) |
| `union_news_both_h1` | leftover | list | none | +7.92 | +4.91 | 21/26 | 4 | 1 | PASS | [union_news_both_h1.md](factor_mine/union_news_both_h1.md) |
| `union_news_g_cam71_h1` | leftover | list | none | -8.27 | -2.91 | 4/26 | 57 | 2 | PASS | [union_news_g_cam71_h1.md](factor_mine/union_news_g_cam71_h1.md) |
| `union_news_g_conv_h1` | conviction | list | none | +4.45 | +15.96 | 7/26 | 88 | 42 | PASS | [union_news_g_conv_h1.md](factor_mine/union_news_g_conv_h1.md) |
| `union_e_green_h3` | leftover | list | none | +12.39 | -1.64 | 18/26 | 67 | 99 | PASS | [union_e_green_h3.md](factor_mine/union_e_green_h3.md) |
| `flatten_h5` | leftover | list | none | +3.39 | +3.80 | 2/26 | 111 | 288 | PASS | [flatten_h5.md](factor_mine/flatten_h5.md) |
| `flatten_h5_rankw` | rank_w | list | none | +0.12 | +3.80 | 2/26 | 104 | 278 | PASS | [flatten_h5_rankw.md](factor_mine/flatten_h5_rankw.md) |
| `flatten_h5_time` | leftover | time | none | +3.39 | +3.80 | 2/26 | 111 | 288 | PASS | [flatten_h5_time.md](factor_mine/flatten_h5_time.md) |
| `flatten_h5_sboost` | leftover | list | both | +0.97 | +3.80 | 2/26 | 115 | 294 | PASS | [flatten_h5_sboost.md](factor_mine/flatten_h5_sboost.md) |
| `union_h5_sboost` | leftover | list | both | -6.71 | +21.23 | 4/26 | 162 | 404 | PASS | [union_h5_sboost.md](factor_mine/union_h5_sboost.md) |
| `flatten_live_h1_sizeup` | leftover | list | sizeup | +8.22 | +4.45 | 7/26 | 48 | 0 | PASS | [flatten_live_h1_sizeup.md](factor_mine/flatten_live_h1_sizeup.md) |
| `union_h3_cut` | leftover | cut_loser | none | +1.71 | +12.27 | 6/26 | 147 | 253 | PASS | [union_h3_cut.md](factor_mine/union_h3_cut.md) |
| `union_h1_topheavy` | topheavy | list | none | +6.18 | +10.70 | 9/26 | 200 | 88 | PASS | [union_h1_topheavy.md](factor_mine/union_h1_topheavy.md) |

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
- [`union_vol_green_h3`](factor_mine/union_vol_green_h3.md)
- [`union_coil_green_h1`](factor_mine/union_coil_green_h1.md)
- [`union_coil_green_h3`](factor_mine/union_coil_green_h3.md)
- [`union_blue_coil_h1`](factor_mine/union_blue_coil_h1.md)
- [`union_blue_coil_h3`](factor_mine/union_blue_coil_h3.md)
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
- [`combo_seh_451540_shared`](factor_mine/combo_seh_451540_shared.md)
- [`combo_seh_403525_shared`](factor_mine/combo_seh_403525_shared.md)
- [`combo_seh_404020_shared`](factor_mine/combo_seh_404020_shared.md)
- [`combo_sh_7030_shared`](factor_mine/combo_sh_7030_shared.md)
- [`combo_seh_502525_shared`](factor_mine/combo_seh_502525_shared.md)
- [`combo_seh_601525_shared`](factor_mine/combo_seh_601525_shared.md)
- [`combo_se_5050_shared`](factor_mine/combo_se_5050_shared.md)
- [`combo_se_5050_weather`](factor_mine/combo_se_5050_weather.md)
- [`combo_en_3070_shared`](factor_mine/combo_en_3070_shared.md)
- [`combo_se_5050_split`](factor_mine/combo_se_5050_split.md)
- [`combo_sj_7030_shared`](factor_mine/combo_sj_7030_shared.md)
- [`combo_ers_7030_shared`](factor_mine/combo_ers_7030_shared.md)
- [`combo_ner_5050_shared`](factor_mine/combo_ner_5050_shared.md)
- [`combo_hn_5050_shared`](factor_mine/combo_hn_5050_shared.md)
- [`combo_seh_502525_split`](factor_mine/combo_seh_502525_split.md)
- [`combo_nse_333_shared`](factor_mine/combo_nse_333_shared.md)
- [`combo_se1_5050_shared`](factor_mine/combo_se1_5050_shared.md)
- [`combo_se_7030_shared`](factor_mine/combo_se_7030_shared.md)
- [`combo_ser_5050_shared`](factor_mine/combo_ser_5050_shared.md)
- [`combo_hn_3070_shared`](factor_mine/combo_hn_3070_shared.md)
- [`combo_ef_5050_shared`](factor_mine/combo_ef_5050_shared.md)
- [`combo_fe_5050_shared`](factor_mine/combo_fe_5050_shared.md)
- [`combo_snj_333_shared`](factor_mine/combo_snj_333_shared.md)
- [`combo_ne1_5050_shared`](factor_mine/combo_ne1_5050_shared.md)
- [`combo_hf_5050_shared`](factor_mine/combo_hf_5050_shared.md)
- [`combo_fse_333_shared`](factor_mine/combo_fse_333_shared.md)
- [`combo_fer_5050_shared`](factor_mine/combo_fer_5050_shared.md)
- [`combo_jf_5050_shared`](factor_mine/combo_jf_5050_shared.md)
- [`combo_fes_403030_shared`](factor_mine/combo_fes_403030_shared.md)
- [`combo_nj_5050_shared`](factor_mine/combo_nj_5050_shared.md)
- [`combo_ef_3070_shared`](factor_mine/combo_ef_3070_shared.md)
- [`combo_nf_5050_shared`](factor_mine/combo_nf_5050_shared.md)
- [`combo_sn_3070_shared`](factor_mine/combo_sn_3070_shared.md)
- [`combo_fh_7030_shared`](factor_mine/combo_fh_7030_shared.md)
- [`combo_sf_5050_shared`](factor_mine/combo_sf_5050_shared.md)
- [`combo_fe1_5050_shared`](factor_mine/combo_fe1_5050_shared.md)
- [`combo_sf_7030_shared`](factor_mine/combo_sf_7030_shared.md)
- [`combo_e1er_5050_shared`](factor_mine/combo_e1er_5050_shared.md)
- [`combo_sn_5050_shared`](factor_mine/combo_sn_5050_shared.md)
- [`combo_sn_7030_shared`](factor_mine/combo_sn_7030_shared.md)
