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
| `combo_jse_333_shared` | leftover | list | none | +33.03 | — | 26/26 | 374 | 370 | PASS | [combo_jse_333_shared.md](factor_mine/combo_jse_333_shared.md) |
| `combo_ej_5050_shared` | leftover | list | none | +36.17 | — | 26/26 | 288 | 256 | PASS | [combo_ej_5050_shared.md](factor_mine/combo_ej_5050_shared.md) |
| `combo_es_9010_shared` | leftover | list | none | +36.09 | — | 26/26 | 224 | 313 | PASS | [combo_es_9010_shared.md](factor_mine/combo_es_9010_shared.md) |
| `combo_se_3070_shared` | leftover | list | none | +32.80 | — | 26/26 | 238 | 318 | PASS | [combo_se_3070_shared.md](factor_mine/combo_se_3070_shared.md) |
| `union_e_fresh_h3` | leftover | list | none | +38.46 | +2.44 | 25/26 | 109 | 184 | PASS | [union_e_fresh_h3.md](factor_mine/union_e_fresh_h3.md) |
| `combo_ee1_3070_shared` | leftover | list | none | +38.46 | — | 25/26 | 109 | 233 | PASS | [combo_ee1_3070_shared.md](factor_mine/combo_ee1_3070_shared.md) |
| `combo_ee1_5050_shared` | leftover | list | none | +38.46 | — | 25/26 | 109 | 233 | PASS | [combo_ee1_5050_shared.md](factor_mine/combo_ee1_5050_shared.md) |
| `combo_ee1_7030_shared` | leftover | list | none | +38.46 | — | 25/26 | 109 | 233 | PASS | [combo_ee1_7030_shared.md](factor_mine/combo_ee1_7030_shared.md) |
| `combo_se_5050_skip` | leftover | list | none | +38.42 | — | 26/26 | 231 | 323 | PASS | [combo_se_5050_skip.md](factor_mine/combo_se_5050_skip.md) |
| `combo_es_8020_shared` | leftover | list | none | +34.98 | — | 26/26 | 236 | 319 | PASS | [combo_es_8020_shared.md](factor_mine/combo_es_8020_shared.md) |
| `union_hot_n4_h1` | leftover | list | none | +31.97 | +71.06 | 26/26 | 92 | 35 | PASS | [union_hot_n4_h1.md](factor_mine/union_hot_n4_h1.md) |
| `combo_sh_3070_shared` | leftover | list | none | +31.92 | — | 26/26 | 201 | 161 | PASS | [combo_sh_3070_shared.md](factor_mine/combo_sh_3070_shared.md) |
| `combo_eh_7030_shared` | leftover | list | none | +31.91 | — | 26/26 | 236 | 240 | PASS | [combo_eh_7030_shared.md](factor_mine/combo_eh_7030_shared.md) |
| `combo_seh_333_skip` | leftover | list | none | +31.29 | — | 26/26 | 331 | 361 | PASS | [combo_seh_333_skip.md](factor_mine/combo_seh_333_skip.md) |
| `combo_ehs_702010_shared` | leftover | list | none | +31.21 | — | 26/26 | 329 | 353 | PASS | [combo_ehs_702010_shared.md](factor_mine/combo_ehs_702010_shared.md) |
| `combo_en_7030_shared` | leftover | list | none | +31.19 | — | 25/26 | 264 | 291 | PASS | [combo_en_7030_shared.md](factor_mine/combo_en_7030_shared.md) |
| `combo_se_5050_shared` | leftover | list | none | +30.24 | — | 26/26 | 236 | 317 | PASS | [combo_se_5050_shared.md](factor_mine/combo_se_5050_shared.md) |
| `combo_se_5050_weather` | leftover | list | none | +30.24 | — | 26/26 | 236 | 317 | PASS | [combo_se_5050_weather.md](factor_mine/combo_se_5050_weather.md) |
| `combo_ehs_601525_shared` | leftover | list | none | +30.10 | — | 26/26 | 342 | 358 | PASS | [combo_ehs_601525_shared.md](factor_mine/combo_ehs_601525_shared.md) |
| `combo_eh_5050_shared` | leftover | list | none | +30.07 | — | 26/26 | 240 | 242 | PASS | [combo_eh_5050_shared.md](factor_mine/combo_eh_5050_shared.md) |
| `combo_jer_5050_shared` | leftover | list | none | +29.40 | — | 23/26 | 293 | 258 | PASS | [combo_jer_5050_shared.md](factor_mine/combo_jer_5050_shared.md) |
| `combo_sh_5050_shared` | leftover | list | none | +29.13 | — | 26/26 | 201 | 161 | PASS | [combo_sh_5050_shared.md](factor_mine/combo_sh_5050_shared.md) |
| `combo_eh_3070_shared` | leftover | list | none | +28.18 | — | 26/26 | 234 | 239 | PASS | [combo_eh_3070_shared.md](factor_mine/combo_eh_3070_shared.md) |
| `combo_seh_404020_shared` | leftover | list | none | +27.75 | — | 26/26 | 342 | 358 | PASS | [combo_seh_404020_shared.md](factor_mine/combo_seh_404020_shared.md) |
| `combo_en_5050_shared` | leftover | list | none | +27.73 | — | 25/26 | 278 | 286 | PASS | [combo_en_5050_shared.md](factor_mine/combo_en_5050_shared.md) |
| `combo_seh_333_shared` | leftover | list | none | +27.08 | — | 26/26 | 340 | 357 | PASS | [combo_seh_333_shared.md](factor_mine/combo_seh_333_shared.md) |
| `combo_seh_333_weather` | leftover | list | none | +27.08 | — | 26/26 | 340 | 357 | PASS | [combo_seh_333_weather.md](factor_mine/combo_seh_333_weather.md) |
| `combo_seh_403525_shared` | leftover | list | none | +26.68 | — | 26/26 | 340 | 357 | PASS | [combo_seh_403525_shared.md](factor_mine/combo_seh_403525_shared.md) |
| `combo_se_7030_shared` | leftover | list | none | +25.54 | — | 21/26 | 234 | 316 | PASS | [combo_se_7030_shared.md](factor_mine/combo_se_7030_shared.md) |
| `combo_seh_502525_shared` | leftover | list | none | +24.48 | — | 26/26 | 340 | 357 | PASS | [combo_seh_502525_shared.md](factor_mine/combo_seh_502525_shared.md) |
| `combo_sj_3070_shared` | leftover | list | none | +23.82 | — | 26/26 | 289 | 174 | PASS | [combo_sj_3070_shared.md](factor_mine/combo_sj_3070_shared.md) |
| `combo_en_3070_shared` | leftover | list | none | +23.72 | — | 24/26 | 276 | 285 | PASS | [combo_en_3070_shared.md](factor_mine/combo_en_3070_shared.md) |
| `combo_nse_333_shared` | leftover | list | none | +23.51 | — | 24/26 | 368 | 401 | PASS | [combo_nse_333_shared.md](factor_mine/combo_nse_333_shared.md) |
| `combo_eer_5050_shared` | leftover | list | none | +22.72 | — | 24/26 | 127 | 254 | PASS | [combo_eer_5050_shared.md](factor_mine/combo_eer_5050_shared.md) |
| `combo_seh_333_split` | leftover | list | none | +22.21 | — | 26/26 | 324 | 353 | PASS | [combo_seh_333_split.md](factor_mine/combo_seh_333_split.md) |
| `combo_her_5050_shared` | leftover | list | none | +21.69 | — | 26/26 | 237 | 238 | PASS | [combo_her_5050_shared.md](factor_mine/combo_her_5050_shared.md) |
| `combo_he1_5050_shared` | leftover | list | none | +20.99 | — | 26/26 | 252 | 85 | PASS | [combo_he1_5050_shared.md](factor_mine/combo_he1_5050_shared.md) |
| `combo_se_5050_split` | leftover | list | none | +20.62 | — | 26/26 | 222 | 313 | PASS | [combo_se_5050_split.md](factor_mine/combo_se_5050_split.md) |
| `combo_ef_7030_shared` | leftover | list | none | +20.39 | — | 21/26 | 217 | 452 | PASS | [combo_ef_7030_shared.md](factor_mine/combo_ef_7030_shared.md) |
| `combo_sj_5050_shared` | leftover | list | none | +20.08 | — | 22/26 | 289 | 174 | PASS | [combo_sj_5050_shared.md](factor_mine/combo_sj_5050_shared.md) |
| `combo_sh_macd_5050_shared` | leftover | list | none | +32.24 | — | 26/26 | 161 | 115 | PASS | [combo_sh_macd_5050_shared.md](factor_mine/combo_sh_macd_5050_shared.md) |
| `combo_ser_5050_shared` | leftover | list | none | +21.15 | — | 4/26 | 243 | 323 | PASS | [combo_ser_5050_shared.md](factor_mine/combo_ser_5050_shared.md) |
| `combo_ner_5050_shared` | leftover | list | none | +20.55 | — | 14/26 | 281 | 289 | PASS | [combo_ner_5050_shared.md](factor_mine/combo_ner_5050_shared.md) |
| `combo_ers_7030_shared` | leftover | list | none | +20.33 | — | 15/26 | 243 | 323 | PASS | [combo_ers_7030_shared.md](factor_mine/combo_ers_7030_shared.md) |
| `combo_je1_5050_shared` | leftover | list | none | +16.71 | — | 26/26 | 336 | 93 | PASS | [combo_je1_5050_shared.md](factor_mine/combo_je1_5050_shared.md) |
| `combo_e1s_7030_shared` | leftover | list | none | +16.60 | — | 26/26 | 268 | 169 | PASS | [combo_e1s_7030_shared.md](factor_mine/combo_e1s_7030_shared.md) |
| `combo_se1_5050_shared` | leftover | list | none | +14.98 | — | 20/26 | 268 | 169 | PASS | [combo_se1_5050_shared.md](factor_mine/combo_se1_5050_shared.md) |
| `combo_ps_7030_shared` | leftover | list | none | +14.41 | — | 19/26 | 199 | 159 | PASS | [combo_ps_7030_shared.md](factor_mine/combo_ps_7030_shared.md) |
| `combo_ps_5050_shared` | leftover | list | none | +11.88 | — | 18/26 | 199 | 159 | PASS | [combo_ps_5050_shared.md](factor_mine/combo_ps_5050_shared.md) |
| `combo_sf_7030_shared` | leftover | list | none | +7.91 | — | 5/26 | 224 | 411 | PASS | [combo_sf_7030_shared.md](factor_mine/combo_sf_7030_shared.md) |
| `flatten_live_h1` | leftover | list | none | +8.22 | +4.45 | 7/26 | 48 | 0 | PASS | [flatten_live_h1.md](factor_mine/flatten_live_h1.md) |
| `flatten_live_h3` | leftover | list | none | -6.24 | +6.87 | 0/26 | 42 | 45 | PASS | [flatten_live_h3.md](factor_mine/flatten_live_h3.md) |
| `flatten_live_h5` | leftover | list | none | -5.51 | +5.13 | 0/26 | 42 | 87 | PASS | [flatten_live_h5.md](factor_mine/flatten_live_h5.md) |
| `union_news_g_h5` | leftover | list | none | -16.45 | +87.73 | 0/26 | 133 | 312 | PASS | [union_news_g_h5.md](factor_mine/union_news_g_h5.md) |
| `union_white_coil_h1` | leftover | list | none | -9.79 | -1.10 | 1/26 | 142 | 0 | PASS | [union_white_coil_h1.md](factor_mine/union_white_coil_h1.md) |
| `union_news_pack_h1` | leftover | list | none | +10.42 | +15.48 | 22/26 | 84 | 29 | PASS | [union_news_pack_h1.md](factor_mine/union_news_pack_h1.md) |
| `union_news_pack_net2_h1` | leftover | list | none | +10.75 | +11.92 | 17/26 | 78 | 27 | PASS | [union_news_pack_net2_h1.md](factor_mine/union_news_pack_net2_h1.md) |
| `union_news_or_h1` | leftover | list | none | +2.99 | +16.41 | 16/26 | 184 | 76 | PASS | [union_news_or_h1.md](factor_mine/union_news_or_h1.md) |
| `union_news_or_net3_h1` | leftover | list | none | -4.11 | -2.54 | 0/26 | 144 | 34 | PASS | [union_news_or_net3_h1.md](factor_mine/union_news_or_net3_h1.md) |
| `union_news_or_net4_h1` | leftover | list | none | -1.06 | -0.41 | 0/26 | 140 | 25 | PASS | [union_news_or_net4_h1.md](factor_mine/union_news_or_net4_h1.md) |
| `union_news_or_net4_rw_h1` | rank_w | list | none | -0.57 | -1.38 | 4/26 | 90 | 23 | PASS | [union_news_or_net4_rw_h1.md](factor_mine/union_news_or_net4_rw_h1.md) |
| `union_news_or_net4_conv_h1` | conviction | list | none | +4.96 | -1.38 | 8/26 | 86 | 25 | PASS | [union_news_or_net4_conv_h1.md](factor_mine/union_news_or_net4_conv_h1.md) |
| `short_news_head_h3` | leftover | list | none | +5.44 | +8.21 | 12/26 | 99 | 114 | PASS | [short_news_head_h3.md](factor_mine/short_news_head_h3.md) |
| `combo_p2s_5050_shared` | leftover | list | none | +10.65 | — | 18/26 | 193 | 157 | PASS | [combo_p2s_5050_shared.md](factor_mine/combo_p2s_5050_shared.md) |
| `union_news_g_cam91_n1_h1` | leftover | list | none | -2.85 | -0.09 | 0/26 | 4 | 0 | PASS | [union_news_g_cam91_n1_h1.md](factor_mine/union_news_g_cam91_n1_h1.md) |
| `union_news_both_h1` | leftover | list | none | +7.29 | +8.40 | 21/26 | 6 | 3 | PASS | [union_news_both_h1.md](factor_mine/union_news_both_h1.md) |
| `union_news_g_cam71_h1` | leftover | list | none | +1.13 | +1.03 | 10/26 | 73 | 3 | PASS | [union_news_g_cam71_h1.md](factor_mine/union_news_g_cam71_h1.md) |
| `union_news_g_conv_h1` | conviction | list | none | +5.00 | +7.97 | 8/26 | 92 | 46 | PASS | [union_news_g_conv_h1.md](factor_mine/union_news_g_conv_h1.md) |
| `union_e_green_h3` | leftover | list | none | -0.88 | -21.98 | 12/26 | 66 | 101 | PASS | [union_e_green_h3.md](factor_mine/union_e_green_h3.md) |
| `union_white_any_h1` | leftover | list | none | -9.25 | -5.52 | 0/26 | 166 | 0 | PASS | [union_white_any_h1.md](factor_mine/union_white_any_h1.md) |
| `union_white_any_h2` | leftover | list | none | -5.35 | +1.93 | 3/26 | 147 | 76 | PASS | [union_white_any_h2.md](factor_mine/union_white_any_h2.md) |
| `union_white_any_h3` | leftover | list | none | -13.12 | -11.63 | 0/26 | 120 | 144 | PASS | [union_white_any_h3.md](factor_mine/union_white_any_h3.md) |
| `union_white_any_h5` | leftover | list | none | +0.47 | -11.24 | 1/26 | 125 | 250 | PASS | [union_white_any_h5.md](factor_mine/union_white_any_h5.md) |
| `union_white_both_n4_h1` | leftover | list | none | +13.12 | +9.03 | 7/26 | 84 | 0 | PASS | [union_white_both_n4_h1.md](factor_mine/union_white_both_n4_h1.md) |
| `union_white_both_n4_h2` | leftover | list | none | +15.86 | +14.76 | 10/26 | 68 | 39 | PASS | [union_white_both_n4_h2.md](factor_mine/union_white_both_n4_h2.md) |
| `union_white_both_n4_h5` | leftover | list | none | +19.41 | +18.10 | 7/26 | 63 | 124 | PASS | [union_white_both_n4_h5.md](factor_mine/union_white_both_n4_h5.md) |
| `union_white_both_n4_h5_s12` | leftover | list | none | +20.16 | +18.10 | 10/26 | 64 | 119 | PASS | [union_white_both_n4_h5_s12.md](factor_mine/union_white_both_n4_h5_s12.md) |
| `flatten_h5_s8` | leftover | list | none | +3.87 | +3.80 | 2/26 | 112 | 267 | PASS | [flatten_h5_s8.md](factor_mine/flatten_h5_s8.md) |
| `flatten_h5` | leftover | list | none | +3.39 | +3.80 | 2/26 | 111 | 288 | PASS | [flatten_h5.md](factor_mine/flatten_h5.md) |
| `flatten_h5_rankw` | rank_w | list | none | +0.12 | +3.80 | 2/26 | 104 | 278 | PASS | [flatten_h5_rankw.md](factor_mine/flatten_h5_rankw.md) |
| `flatten_h5_time` | leftover | time | none | +3.39 | +3.80 | 2/26 | 111 | 288 | PASS | [flatten_h5_time.md](factor_mine/flatten_h5_time.md) |
| `flatten_h5_sboost` | leftover | list | both | +0.97 | +3.80 | 2/26 | 115 | 294 | PASS | [flatten_h5_sboost.md](factor_mine/flatten_h5_sboost.md) |
| `union_h5_sboost` | leftover | list | both | -0.12 | +18.51 | 5/26 | 160 | 402 | PASS | [union_h5_sboost.md](factor_mine/union_h5_sboost.md) |
| `flatten_live_h1_sizeup` | leftover | list | sizeup | +8.22 | +4.45 | 7/26 | 48 | 0 | PASS | [flatten_live_h1_sizeup.md](factor_mine/flatten_live_h1_sizeup.md) |
| `union_h3_cut` | leftover | cut_loser | none | +2.86 | +13.91 | 6/26 | 147 | 252 | PASS | [union_h3_cut.md](factor_mine/union_h3_cut.md) |
| `union_h1_topheavy` | topheavy | list | none | +7.64 | +11.57 | 10/26 | 200 | 87 | PASS | [union_h1_topheavy.md](factor_mine/union_h1_topheavy.md) |

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
- [`union_ret_5_h3`](factor_mine/union_ret_5_h3.md)
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
- [`combo_seh_451540_shared`](factor_mine/combo_seh_451540_shared.md)
- [`combo_sh_7030_shared`](factor_mine/combo_sh_7030_shared.md)
- [`combo_seh_601525_shared`](factor_mine/combo_seh_601525_shared.md)
- [`combo_hn_7030_shared`](factor_mine/combo_hn_7030_shared.md)
- [`combo_hj_5050_shared`](factor_mine/combo_hj_5050_shared.md)
- [`combo_seh_502525_split`](factor_mine/combo_seh_502525_split.md)
- [`combo_hn_5050_shared`](factor_mine/combo_hn_5050_shared.md)
- [`combo_sj_7030_shared`](factor_mine/combo_sj_7030_shared.md)
- [`combo_snj_333_shared`](factor_mine/combo_snj_333_shared.md)
- [`combo_hn_3070_shared`](factor_mine/combo_hn_3070_shared.md)
- [`combo_ef_5050_shared`](factor_mine/combo_ef_5050_shared.md)
- [`combo_fe_5050_shared`](factor_mine/combo_fe_5050_shared.md)
- [`combo_ne1_5050_shared`](factor_mine/combo_ne1_5050_shared.md)
- [`combo_fse_333_shared`](factor_mine/combo_fse_333_shared.md)
- [`combo_fes_403030_shared`](factor_mine/combo_fes_403030_shared.md)
- [`combo_sn_3070_shared`](factor_mine/combo_sn_3070_shared.md)
- [`combo_hf_5050_shared`](factor_mine/combo_hf_5050_shared.md)
- [`combo_fer_5050_shared`](factor_mine/combo_fer_5050_shared.md)
- [`combo_sf_5050_shared`](factor_mine/combo_sf_5050_shared.md)
- [`combo_sn_5050_shared`](factor_mine/combo_sn_5050_shared.md)
- [`combo_nj_5050_shared`](factor_mine/combo_nj_5050_shared.md)
- [`combo_sn_7030_shared`](factor_mine/combo_sn_7030_shared.md)
- [`combo_ef_3070_shared`](factor_mine/combo_ef_3070_shared.md)
- [`combo_jf_5050_shared`](factor_mine/combo_jf_5050_shared.md)
- [`combo_sf_3070_shared`](factor_mine/combo_sf_3070_shared.md)
- [`combo_nf_5050_shared`](factor_mine/combo_nf_5050_shared.md)
- [`combo_fh_7030_shared`](factor_mine/combo_fh_7030_shared.md)
- [`combo_fe1_5050_shared`](factor_mine/combo_fe1_5050_shared.md)
- [`combo_e1er_5050_shared`](factor_mine/combo_e1er_5050_shared.md)
