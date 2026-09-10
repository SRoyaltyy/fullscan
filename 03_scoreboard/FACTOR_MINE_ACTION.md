# Factor mine action — 2026-08-13 → 2026-09-09

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
| `combo_sh_5050_shared` | leftover | list | none | +36.01 | — | 17/19 | 142 | 104 | PASS | [combo_sh_5050_shared.md](factor_mine/combo_sh_5050_shared.md) |
| `combo_jse_333_shared` | leftover | list | none | +32.54 | — | 16/19 | 216 | 224 | PASS | [combo_jse_333_shared.md](factor_mine/combo_jse_333_shared.md) |
| `combo_sh_7030_shared` | leftover | list | none | +31.06 | — | 17/19 | 142 | 104 | PASS | [combo_sh_7030_shared.md](factor_mine/combo_sh_7030_shared.md) |
| `combo_sh_3070_shared` | leftover | list | none | +30.89 | — | 17/19 | 142 | 104 | PASS | [combo_sh_3070_shared.md](factor_mine/combo_sh_3070_shared.md) |
| `combo_seh_451540_shared` | leftover | list | none | +29.88 | — | 17/19 | 232 | 246 | PASS | [combo_seh_451540_shared.md](factor_mine/combo_seh_451540_shared.md) |
| `combo_se_5050_skip` | leftover | list | none | +29.29 | — | 16/19 | 134 | 213 | PASS | [combo_se_5050_skip.md](factor_mine/combo_se_5050_skip.md) |
| `combo_seh_601525_shared` | leftover | list | none | +29.05 | — | 17/19 | 232 | 246 | PASS | [combo_seh_601525_shared.md](factor_mine/combo_seh_601525_shared.md) |
| `combo_seh_502525_shared` | leftover | list | none | +28.56 | — | 17/19 | 238 | 249 | PASS | [combo_seh_502525_shared.md](factor_mine/combo_seh_502525_shared.md) |
| `combo_ej_5050_shared` | leftover | list | none | +28.38 | — | 12/19 | 190 | 178 | PASS | [combo_ej_5050_shared.md](factor_mine/combo_ej_5050_shared.md) |
| `combo_seh_333_skip` | leftover | list | none | +27.79 | — | 16/19 | 230 | 255 | PASS | [combo_seh_333_skip.md](factor_mine/combo_seh_333_skip.md) |
| `combo_jer_5050_shared` | leftover | list | none | +27.39 | — | 13/19 | 194 | 183 | PASS | [combo_jer_5050_shared.md](factor_mine/combo_jer_5050_shared.md) |
| `combo_se1_5050_shared` | leftover | list | none | +27.35 | — | 16/19 | 182 | 103 | PASS | [combo_se1_5050_shared.md](factor_mine/combo_se1_5050_shared.md) |
| `combo_se_5050_shared` | leftover | list | none | +27.31 | — | 16/19 | 138 | 208 | PASS | [combo_se_5050_shared.md](factor_mine/combo_se_5050_shared.md) |
| `combo_se_5050_weather` | leftover | list | none | +27.31 | — | 16/19 | 138 | 208 | PASS | [combo_se_5050_weather.md](factor_mine/combo_se_5050_weather.md) |
| `combo_se_3070_shared` | leftover | list | none | +26.75 | — | 16/19 | 160 | 219 | PASS | [combo_se_3070_shared.md](factor_mine/combo_se_3070_shared.md) |
| `combo_seh_403525_shared` | leftover | list | none | +26.43 | — | 16/19 | 238 | 249 | PASS | [combo_seh_403525_shared.md](factor_mine/combo_seh_403525_shared.md) |
| `combo_seh_404020_shared` | leftover | list | none | +26.41 | — | 16/19 | 240 | 250 | PASS | [combo_seh_404020_shared.md](factor_mine/combo_seh_404020_shared.md) |
| `combo_es_8020_shared` | leftover | list | none | +26.33 | — | 14/19 | 160 | 219 | PASS | [combo_es_8020_shared.md](factor_mine/combo_es_8020_shared.md) |
| `combo_nse_333_shared` | leftover | list | none | +25.87 | — | 16/19 | 244 | 276 | PASS | [combo_nse_333_shared.md](factor_mine/combo_nse_333_shared.md) |
| `combo_ser_5050_shared` | leftover | list | none | +25.76 | — | 16/19 | 144 | 206 | PASS | [combo_ser_5050_shared.md](factor_mine/combo_ser_5050_shared.md) |
| `combo_ers_7030_shared` | leftover | list | none | +25.07 | — | 15/19 | 164 | 226 | PASS | [combo_ers_7030_shared.md](factor_mine/combo_ers_7030_shared.md) |
| `combo_seh_333_shared` | leftover | list | none | +24.98 | — | 16/19 | 240 | 250 | PASS | [combo_seh_333_shared.md](factor_mine/combo_seh_333_shared.md) |
| `combo_seh_333_weather` | leftover | list | none | +24.98 | — | 16/19 | 240 | 250 | PASS | [combo_seh_333_weather.md](factor_mine/combo_seh_333_weather.md) |
| `combo_es_9010_shared` | leftover | list | none | +24.90 | — | 13/19 | 154 | 216 | PASS | [combo_es_9010_shared.md](factor_mine/combo_es_9010_shared.md) |
| `combo_sj_5050_shared` | leftover | list | none | +24.70 | — | 17/19 | 180 | 94 | PASS | [combo_sj_5050_shared.md](factor_mine/combo_sj_5050_shared.md) |
| `combo_se_7030_shared` | leftover | list | none | +24.59 | — | 16/19 | 136 | 197 | PASS | [combo_se_7030_shared.md](factor_mine/combo_se_7030_shared.md) |
| `combo_ehs_601525_shared` | leftover | list | none | +23.94 | — | 16/19 | 242 | 251 | PASS | [combo_ehs_601525_shared.md](factor_mine/combo_ehs_601525_shared.md) |
| `combo_sj_3070_shared` | leftover | list | none | +22.91 | — | 17/19 | 180 | 94 | PASS | [combo_sj_3070_shared.md](factor_mine/combo_sj_3070_shared.md) |
| `combo_e1s_7030_shared` | leftover | list | none | +22.56 | — | 16/19 | 182 | 103 | PASS | [combo_e1s_7030_shared.md](factor_mine/combo_e1s_7030_shared.md) |
| `combo_ner_5050_shared` | leftover | list | none | +20.18 | — | 7/19 | 198 | 218 | PASS | [combo_ner_5050_shared.md](factor_mine/combo_ner_5050_shared.md) |
| `combo_sj_7030_shared` | leftover | list | none | +19.91 | — | 17/19 | 180 | 94 | PASS | [combo_sj_7030_shared.md](factor_mine/combo_sj_7030_shared.md) |
| `combo_snj_333_shared` | leftover | list | none | +18.41 | — | 17/19 | 300 | 144 | PASS | [combo_snj_333_shared.md](factor_mine/combo_snj_333_shared.md) |
| `combo_sn_5050_shared` | leftover | list | none | +17.69 | — | 17/19 | 194 | 129 | PASS | [combo_sn_5050_shared.md](factor_mine/combo_sn_5050_shared.md) |
| `combo_sf_7030_shared` | leftover | list | none | +17.50 | — | 12/19 | 146 | 296 | PASS | [combo_sf_7030_shared.md](factor_mine/combo_sf_7030_shared.md) |
| `combo_sn_3070_shared` | leftover | list | none | +16.02 | — | 17/19 | 196 | 128 | PASS | [combo_sn_3070_shared.md](factor_mine/combo_sn_3070_shared.md) |
| `combo_sf_5050_shared` | leftover | list | none | +15.81 | — | 8/19 | 146 | 300 | PASS | [combo_sf_5050_shared.md](factor_mine/combo_sf_5050_shared.md) |
| `combo_sn_7030_shared` | leftover | list | none | +15.21 | — | 17/19 | 194 | 129 | PASS | [combo_sn_7030_shared.md](factor_mine/combo_sn_7030_shared.md) |
| `combo_je1_5050_shared` | leftover | list | none | +13.45 | — | 14/19 | 238 | 50 | PASS | [combo_je1_5050_shared.md](factor_mine/combo_je1_5050_shared.md) |
| `combo_ne1_5050_shared` | leftover | list | none | +10.45 | — | 7/19 | 240 | 85 | PASS | [combo_ne1_5050_shared.md](factor_mine/combo_ne1_5050_shared.md) |
| `flatten_live_h1` | leftover | list | none | +8.22 | +4.45 | 7/19 | 48 | 0 | PASS | [flatten_live_h1.md](factor_mine/flatten_live_h1.md) |
| `flatten_live_h3` | leftover | list | none | -4.36 | +6.87 | 0/19 | 34 | 45 | PASS | [flatten_live_h3.md](factor_mine/flatten_live_h3.md) |
| `flatten_live_h5` | leftover | list | none | -3.47 | +7.46 | 0/19 | 34 | 71 | PASS | [flatten_live_h5.md](factor_mine/flatten_live_h5.md) |
| `union_e_fresh_h3` | leftover | list | none | +22.49 | -19.89 | 12/19 | 85 | 139 | PASS | [union_e_fresh_h3.md](factor_mine/union_e_fresh_h3.md) |
| `union_news_g_h5` | leftover | list | none | -9.61 | +139.34 | 1/19 | 81 | 224 | PASS | [union_news_g_h5.md](factor_mine/union_news_g_h5.md) |
| `union_white_coil_h1` | leftover | list | none | -5.86 | -2.64 | 0/19 | 128 | 0 | PASS | [union_white_coil_h1.md](factor_mine/union_white_coil_h1.md) |
| `union_e_green_h3` | leftover | list | none | -10.28 | -36.94 | 0/19 | 63 | 92 | PASS | [union_e_green_h3.md](factor_mine/union_e_green_h3.md) |
| `flatten_h5` | leftover | list | none | +10.54 | +40.74 | 6/19 | 79 | 224 | PASS | [flatten_h5.md](factor_mine/flatten_h5.md) |
| `flatten_h5_rankw` | rank_w | list | none | +6.37 | +40.74 | 4/19 | 74 | 217 | PASS | [flatten_h5_rankw.md](factor_mine/flatten_h5_rankw.md) |
| `flatten_h5_time` | leftover | time | none | +10.54 | +40.74 | 6/19 | 79 | 224 | PASS | [flatten_h5_time.md](factor_mine/flatten_h5_time.md) |
| `flatten_h5_sboost` | leftover | list | both | +8.02 | +40.74 | 6/19 | 83 | 230 | PASS | [flatten_h5_sboost.md](factor_mine/flatten_h5_sboost.md) |
| `union_h5_sboost` | leftover | list | both | +5.76 | +30.08 | 3/19 | 111 | 303 | PASS | [union_h5_sboost.md](factor_mine/union_h5_sboost.md) |
| `flatten_live_h1_sizeup` | leftover | list | sizeup | +8.22 | +4.45 | 7/19 | 48 | 0 | PASS | [flatten_live_h1_sizeup.md](factor_mine/flatten_live_h1_sizeup.md) |
| `union_h3_cut` | leftover | cut_loser | none | +4.57 | +15.11 | 5/19 | 110 | 191 | PASS | [union_h3_cut.md](factor_mine/union_h3_cut.md) |
| `union_h1_topheavy` | topheavy | list | none | +8.47 | -0.56 | 7/19 | 148 | 65 | PASS | [union_h1_topheavy.md](factor_mine/union_h1_topheavy.md) |

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
- [`combo_ee1_3070_shared`](factor_mine/combo_ee1_3070_shared.md)
- [`combo_ee1_5050_shared`](factor_mine/combo_ee1_5050_shared.md)
- [`combo_ee1_7030_shared`](factor_mine/combo_ee1_7030_shared.md)
- [`combo_en_7030_shared`](factor_mine/combo_en_7030_shared.md)
- [`combo_en_5050_shared`](factor_mine/combo_en_5050_shared.md)
- [`combo_ehs_702010_shared`](factor_mine/combo_ehs_702010_shared.md)
- [`combo_en_3070_shared`](factor_mine/combo_en_3070_shared.md)
- [`combo_eh_7030_shared`](factor_mine/combo_eh_7030_shared.md)
- [`combo_se_5050_split`](factor_mine/combo_se_5050_split.md)
- [`combo_eh_5050_shared`](factor_mine/combo_eh_5050_shared.md)
- [`combo_seh_333_split`](factor_mine/combo_seh_333_split.md)
- [`combo_eh_3070_shared`](factor_mine/combo_eh_3070_shared.md)
- [`combo_eer_5050_shared`](factor_mine/combo_eer_5050_shared.md)
- [`combo_seh_502525_split`](factor_mine/combo_seh_502525_split.md)
- [`combo_ef_7030_shared`](factor_mine/combo_ef_7030_shared.md)
- [`combo_hn_7030_shared`](factor_mine/combo_hn_7030_shared.md)
- [`combo_sf_3070_shared`](factor_mine/combo_sf_3070_shared.md)
- [`combo_fse_333_shared`](factor_mine/combo_fse_333_shared.md)
- [`combo_her_5050_shared`](factor_mine/combo_her_5050_shared.md)
- [`combo_hn_5050_shared`](factor_mine/combo_hn_5050_shared.md)
- [`combo_fes_403030_shared`](factor_mine/combo_fes_403030_shared.md)
- [`combo_he1_5050_shared`](factor_mine/combo_he1_5050_shared.md)
- [`combo_hn_3070_shared`](factor_mine/combo_hn_3070_shared.md)
- [`combo_ef_5050_shared`](factor_mine/combo_ef_5050_shared.md)
- [`combo_fe_5050_shared`](factor_mine/combo_fe_5050_shared.md)
- [`combo_hj_5050_shared`](factor_mine/combo_hj_5050_shared.md)
- [`combo_fer_5050_shared`](factor_mine/combo_fer_5050_shared.md)
- [`combo_jf_5050_shared`](factor_mine/combo_jf_5050_shared.md)
- [`combo_nf_5050_shared`](factor_mine/combo_nf_5050_shared.md)
- [`combo_ef_3070_shared`](factor_mine/combo_ef_3070_shared.md)
- [`combo_nj_5050_shared`](factor_mine/combo_nj_5050_shared.md)
- [`combo_hf_5050_shared`](factor_mine/combo_hf_5050_shared.md)
- [`combo_fh_7030_shared`](factor_mine/combo_fh_7030_shared.md)
- [`combo_fe1_5050_shared`](factor_mine/combo_fe1_5050_shared.md)
- [`combo_e1er_5050_shared`](factor_mine/combo_e1er_5050_shared.md)
