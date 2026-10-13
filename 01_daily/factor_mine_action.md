# Factor mine action — 2026-08-13 → 2026-10-13

Sessions before `2026-08-13` are **reconstructed** (rebuilt inputs, not a frozen 09:30 snapshot). From `2026-08-13` each day's inputs and buy/sell decisions are append-only.

Sessions before a recipe's `created_on` are **in-sample**. Holdup, overnight-mega, and Clock-B start `2026-09-21`. White-horizon and stop brackets start `2026-09-14`.

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
| `combo_jse_333_shared` | leftover | list | none | +1.30 | — | 7/30 | 584 | 387 | PASS | [combo_jse_333_shared.md](factor_mine/combo_jse_333_shared.md) |
| `combo_ej_5050_shared` | leftover | list | none | -6.88 | — | 26/30 | 474 | 283 | PASS | [combo_ej_5050_shared.md](factor_mine/combo_ej_5050_shared.md) |
| `combo_es_9010_shared` | leftover | list | none | -9.83 | — | 25/30 | 290 | 319 | PASS | [combo_es_9010_shared.md](factor_mine/combo_es_9010_shared.md) |
| `combo_se_3070_shared` | leftover | list | none | -5.91 | — | 25/30 | 300 | 324 | PASS | [combo_se_3070_shared.md](factor_mine/combo_se_3070_shared.md) |
| `union_e_fresh_h3` | leftover | list | none | -17.19 | +7.74 | 29/30 | 147 | 198 | PASS | [union_e_fresh_h3.md](factor_mine/union_e_fresh_h3.md) |
| `union_news_pack_net2_h1` | leftover | list | none | -5.38 | +7.99 | 21/30 | 78 | 28 | PASS | [union_news_pack_net2_h1.md](factor_mine/union_news_pack_net2_h1.md) |
| `union_hot_n4_holdup` | leftover | list | holdup | +42.89 | +89.81 | 29/30 | 112 | 79 | PASS | [union_hot_n4_holdup.md](factor_mine/union_hot_n4_holdup.md) |
| `overnight_mega_h1` | leftover | list | none | -8.21 | -15.28 | 0/30 | 26 | 15 | PASS | [overnight_mega_h1.md](factor_mine/overnight_mega_h1.md) |
| `combo_sh_macd_5050_shared` | leftover | list | none | +102.70 | — | 29/30 | 262 | 122 | PASS | [combo_sh_macd_5050_shared.md](factor_mine/combo_sh_macd_5050_shared.md) |
| `combo_sh_7030_shared` | leftover | list | none | +100.88 | — | 29/30 | 292 | 159 | PASS | [combo_sh_7030_shared.md](factor_mine/combo_sh_7030_shared.md) |
| `combo_seh_601525_shared` | leftover | list | none | +84.89 | — | 29/30 | 456 | 365 | PASS | [combo_seh_601525_shared.md](factor_mine/combo_seh_601525_shared.md) |
| `combo_sh_5050_shared` | leftover | list | none | +83.79 | — | 29/30 | 292 | 159 | PASS | [combo_sh_5050_shared.md](factor_mine/combo_sh_5050_shared.md) |
| `combo_sh_3070_shared` | leftover | list | none | +78.91 | — | 29/30 | 292 | 159 | PASS | [combo_sh_3070_shared.md](factor_mine/combo_sh_3070_shared.md) |
| `combo_seh_333_shared` | leftover | list | none | +77.72 | — | 29/30 | 464 | 369 | PASS | [combo_seh_333_shared.md](factor_mine/combo_seh_333_shared.md) |
| `combo_seh_333_weather` | leftover | list | none | +77.72 | — | 29/30 | 464 | 369 | PASS | [combo_seh_333_weather.md](factor_mine/combo_seh_333_weather.md) |
| `combo_seh_451540_shared` | leftover | list | none | +75.43 | — | 29/30 | 454 | 364 | PASS | [combo_seh_451540_shared.md](factor_mine/combo_seh_451540_shared.md) |
| `combo_seh_502525_shared` | leftover | list | none | +73.71 | — | 29/30 | 464 | 369 | PASS | [combo_seh_502525_shared.md](factor_mine/combo_seh_502525_shared.md) |
| `combo_seh_333_skip` | leftover | list | none | +70.17 | — | 29/30 | 453 | 373 | PASS | [combo_seh_333_skip.md](factor_mine/combo_seh_333_skip.md) |
| `combo_ehs_601525_shared` | leftover | list | none | +68.47 | — | 28/30 | 466 | 370 | PASS | [combo_ehs_601525_shared.md](factor_mine/combo_ehs_601525_shared.md) |
| `combo_seh_404020_shared` | leftover | list | none | +66.77 | — | 29/30 | 468 | 370 | PASS | [combo_seh_404020_shared.md](factor_mine/combo_seh_404020_shared.md) |
| `combo_seh_403525_shared` | leftover | list | none | +66.20 | — | 29/30 | 466 | 369 | PASS | [combo_seh_403525_shared.md](factor_mine/combo_seh_403525_shared.md) |
| `combo_ehs_702010_shared` | leftover | list | none | +51.39 | — | 29/30 | 448 | 365 | PASS | [combo_ehs_702010_shared.md](factor_mine/combo_ehs_702010_shared.md) |
| `combo_oh_5050_shared` | leftover | list | none | +45.58 | — | 29/30 | 190 | 62 | PASS | [combo_oh_5050_shared.md](factor_mine/combo_oh_5050_shared.md) |
| `combo_eh_3070_shared` | leftover | list | none | +28.60 | — | 29/30 | 348 | 269 | PASS | [combo_eh_3070_shared.md](factor_mine/combo_eh_3070_shared.md) |
| `combo_hj_5050_shared` | leftover | list | none | +23.92 | — | 29/30 | 440 | 100 | PASS | [combo_hj_5050_shared.md](factor_mine/combo_hj_5050_shared.md) |
| `union_w_hot_cond_h1` | leftover | list | none | +21.60 | +31.34 | 29/30 | 369 | 96 | PASS | [union_w_hot_cond_h1.md](factor_mine/union_w_hot_cond_h1.md) |
| `combo_eh_5050_shared` | leftover | list | none | +21.20 | — | 29/30 | 350 | 270 | PASS | [combo_eh_5050_shared.md](factor_mine/combo_eh_5050_shared.md) |
| `union_w_hot_cond_h3` | leftover | list | none | +20.26 | +337.30 | 29/30 | 273 | 286 | PASS | [union_w_hot_cond_h3.md](factor_mine/union_w_hot_cond_h3.md) |
| `combo_jer_5050_shared` | leftover | list | none | -11.96 | — | 22/30 | 476 | 286 | PASS | [combo_jer_5050_shared.md](factor_mine/combo_jer_5050_shared.md) |
| `combo_je1_5050_shared` | leftover | list | none | -10.83 | — | 25/30 | 524 | 110 | PASS | [combo_je1_5050_shared.md](factor_mine/combo_je1_5050_shared.md) |
| `combo_sf_7030_shared` | leftover | list | none | +0.27 | — | 8/30 | 294 | 449 | PASS | [combo_sf_7030_shared.md](factor_mine/combo_sf_7030_shared.md) |
| `combo_sf_5050_shared` | leftover | list | none | -0.39 | — | 8/30 | 286 | 449 | PASS | [combo_sf_5050_shared.md](factor_mine/combo_sf_5050_shared.md) |
| `union_hot_n4_h1` | leftover | list | none | +17.88 | +89.81 | 29/30 | 133 | 47 | PASS | [union_hot_n4_h1.md](factor_mine/union_hot_n4_h1.md) |
| `flatten_live_h1` | leftover | list | none | -1.72 | +4.45 | 7/30 | 48 | 0 | PASS | [flatten_live_h1.md](factor_mine/flatten_live_h1.md) |
| `flatten_live_h3` | leftover | list | none | -9.53 | +6.87 | 0/30 | 42 | 45 | PASS | [flatten_live_h3.md](factor_mine/flatten_live_h3.md) |
| `flatten_live_h5` | leftover | list | none | -9.67 | +5.13 | 0/30 | 42 | 87 | PASS | [flatten_live_h5.md](factor_mine/flatten_live_h5.md) |
| `union_news_g_h5` | leftover | list | none | -25.88 | +99.58 | 1/30 | 180 | 344 | PASS | [union_news_g_h5.md](factor_mine/union_news_g_h5.md) |
| `union_white_coil_h1` | leftover | list | none | -21.24 | -4.35 | 0/30 | 200 | 0 | PASS | [union_white_coil_h1.md](factor_mine/union_white_coil_h1.md) |
| `overnight_mega_h2` | leftover | list | none | -4.65 | -8.07 | 1/30 | 12 | 27 | PASS | [overnight_mega_h2.md](factor_mine/overnight_mega_h2.md) |
| `overnight_h1` | leftover | list | none | -25.50 | -22.61 | 0/30 | 142 | 80 | PASS | [overnight_h1.md](factor_mine/overnight_h1.md) |
| `overnight_mega_green_h1` | leftover | list | none | +0.00 | -9.06 | 0/30 | 20 | 7 | PASS | [overnight_mega_green_h1.md](factor_mine/overnight_mega_green_h1.md) |
| `union_news_pack_h1` | leftover | list | none | -4.47 | +6.91 | 21/30 | 86 | 29 | PASS | [union_news_pack_h1.md](factor_mine/union_news_pack_h1.md) |
| `union_news_or_h1` | leftover | list | none | -34.00 | +9.53 | 0/30 | 268 | 79 | PASS | [union_news_or_h1.md](factor_mine/union_news_or_h1.md) |
| `union_news_or_net3_h1` | leftover | list | none | -42.71 | -9.17 | 0/30 | 180 | 40 | PASS | [union_news_or_net3_h1.md](factor_mine/union_news_or_net3_h1.md) |
| `union_news_or_net4_h1` | leftover | list | none | -43.17 | -6.67 | 0/30 | 166 | 35 | PASS | [union_news_or_net4_h1.md](factor_mine/union_news_or_net4_h1.md) |
| `union_news_or_net4_rw_h1` | rank_w | list | none | -38.01 | -7.44 | 5/30 | 120 | 30 | PASS | [union_news_or_net4_rw_h1.md](factor_mine/union_news_or_net4_rw_h1.md) |
| `union_news_or_net4_conv_h1` | conviction | list | none | -37.65 | -7.44 | 12/30 | 118 | 31 | PASS | [union_news_or_net4_conv_h1.md](factor_mine/union_news_or_net4_conv_h1.md) |
| `short_news_head_h3` | leftover | list | none | -11.31 | +2.90 | 2/30 | 97 | 94 | PASS | [short_news_head_h3.md](factor_mine/short_news_head_h3.md) |
| `combo_ps_5050_shared` | leftover | list | none | -7.70 | — | 2/30 | 218 | 146 | PASS | [combo_ps_5050_shared.md](factor_mine/combo_ps_5050_shared.md) |
| `combo_ps_7030_shared` | leftover | list | none | -5.61 | — | 2/30 | 218 | 146 | PASS | [combo_ps_7030_shared.md](factor_mine/combo_ps_7030_shared.md) |
| `combo_p2s_5050_shared` | leftover | list | none | -8.42 | — | 2/30 | 210 | 145 | PASS | [combo_p2s_5050_shared.md](factor_mine/combo_p2s_5050_shared.md) |
| `union_news_g_cam91_n1_h1` | leftover | list | none | -22.39 | -2.85 | 27/30 | 8 | 1 | PASS | [union_news_g_cam91_n1_h1.md](factor_mine/union_news_g_cam91_n1_h1.md) |
| `union_news_both_h1` | leftover | list | none | -3.46 | +4.21 | 21/30 | 8 | 2 | PASS | [union_news_both_h1.md](factor_mine/union_news_both_h1.md) |
| `union_news_g_cam71_h1` | leftover | list | none | -23.10 | -11.63 | 4/30 | 84 | 12 | PASS | [union_news_g_cam71_h1.md](factor_mine/union_news_g_cam71_h1.md) |
| `union_news_g_conv_h1` | conviction | list | none | -36.27 | +13.54 | 26/30 | 158 | 51 | PASS | [union_news_g_conv_h1.md](factor_mine/union_news_g_conv_h1.md) |
| `union_e_green_h3` | leftover | list | none | -11.46 | -18.73 | 20/30 | 76 | 104 | PASS | [union_e_green_h3.md](factor_mine/union_e_green_h3.md) |
| `union_white_any_h1` | leftover | list | none | -14.85 | -10.91 | 0/30 | 248 | 0 | PASS | [union_white_any_h1.md](factor_mine/union_white_any_h1.md) |
| `union_white_any_h2` | leftover | list | none | -9.73 | +6.04 | 5/30 | 221 | 94 | PASS | [union_white_any_h2.md](factor_mine/union_white_any_h2.md) |
| `union_white_any_h3` | leftover | list | none | -21.38 | -4.29 | 2/30 | 177 | 149 | PASS | [union_white_any_h3.md](factor_mine/union_white_any_h3.md) |
| `union_white_any_h5` | leftover | list | none | -13.72 | -11.72 | 1/30 | 176 | 277 | PASS | [union_white_any_h5.md](factor_mine/union_white_any_h5.md) |
| `union_white_both_n4_h1` | leftover | list | none | -5.61 | +6.63 | 7/30 | 98 | 0 | PASS | [union_white_both_n4_h1.md](factor_mine/union_white_both_n4_h1.md) |
| `union_white_both_n4_h2` | leftover | list | none | -2.88 | +20.82 | 13/30 | 82 | 47 | PASS | [union_white_both_n4_h2.md](factor_mine/union_white_both_n4_h2.md) |
| `union_white_both_n4_h5` | leftover | list | none | +0.89 | +22.84 | 9/30 | 78 | 141 | PASS | [union_white_both_n4_h5.md](factor_mine/union_white_both_n4_h5.md) |
| `union_white_both_n4_h5_s12` | leftover | list | none | +1.98 | +22.84 | 12/30 | 77 | 137 | PASS | [union_white_both_n4_h5_s12.md](factor_mine/union_white_both_n4_h5_s12.md) |
| `flatten_h5_s8` | leftover | list | none | -11.45 | +4.47 | 3/30 | 198 | 322 | PASS | [flatten_h5_s8.md](factor_mine/flatten_h5_s8.md) |
| `flatten_h5` | leftover | list | none | -12.37 | +4.47 | 4/30 | 180 | 349 | PASS | [flatten_h5.md](factor_mine/flatten_h5.md) |
| `flatten_h5_rankw` | rank_w | list | none | -14.03 | +4.47 | 4/30 | 174 | 337 | PASS | [flatten_h5_rankw.md](factor_mine/flatten_h5_rankw.md) |
| `flatten_h5_time` | leftover | time | none | -12.37 | +4.47 | 4/30 | 180 | 349 | PASS | [flatten_h5_time.md](factor_mine/flatten_h5_time.md) |
| `flatten_h5_sboost` | leftover | list | both | -12.37 | +4.47 | 3/30 | 184 | 355 | PASS | [flatten_h5_sboost.md](factor_mine/flatten_h5_sboost.md) |
| `union_h5_sboost` | leftover | list | both | -14.74 | +16.74 | 11/30 | 273 | 518 | PASS | [union_h5_sboost.md](factor_mine/union_h5_sboost.md) |
| `flatten_live_h1_sizeup` | leftover | list | sizeup | -1.72 | +4.45 | 7/30 | 48 | 0 | PASS | [flatten_live_h1_sizeup.md](factor_mine/flatten_live_h1_sizeup.md) |
| `union_h3_cut` | leftover | cut_loser | none | -23.47 | -4.92 | 0/30 | 259 | 293 | PASS | [union_h3_cut.md](factor_mine/union_h3_cut.md) |
| `union_h1_topheavy` | topheavy | list | none | -18.89 | -0.80 | 9/30 | 362 | 107 | PASS | [union_h1_topheavy.md](factor_mine/union_h1_topheavy.md) |

## All other blotters

- [`coil_h3_exit_alarm`](factor_mine/coil_h3_exit_alarm.md)
- [`combo_e1er_5050_shared`](factor_mine/combo_e1er_5050_shared.md)
- [`combo_e1s_7030_shared`](factor_mine/combo_e1s_7030_shared.md)
- [`combo_ee1_3070_shared`](factor_mine/combo_ee1_3070_shared.md)
- [`combo_ee1_5050_shared`](factor_mine/combo_ee1_5050_shared.md)
- [`combo_ee1_7030_shared`](factor_mine/combo_ee1_7030_shared.md)
- [`combo_eer_5050_shared`](factor_mine/combo_eer_5050_shared.md)
- [`combo_ef_3070_shared`](factor_mine/combo_ef_3070_shared.md)
- [`combo_ef_5050_shared`](factor_mine/combo_ef_5050_shared.md)
- [`combo_ef_7030_shared`](factor_mine/combo_ef_7030_shared.md)
- [`combo_eh_7030_shared`](factor_mine/combo_eh_7030_shared.md)
- [`combo_en_3070_shared`](factor_mine/combo_en_3070_shared.md)
- [`combo_en_5050_shared`](factor_mine/combo_en_5050_shared.md)
- [`combo_en_7030_shared`](factor_mine/combo_en_7030_shared.md)
- [`combo_ers_7030_shared`](factor_mine/combo_ers_7030_shared.md)
- [`combo_es_8020_shared`](factor_mine/combo_es_8020_shared.md)
- [`combo_fe1_5050_shared`](factor_mine/combo_fe1_5050_shared.md)
- [`combo_fe_5050_shared`](factor_mine/combo_fe_5050_shared.md)
- [`combo_fer_5050_shared`](factor_mine/combo_fer_5050_shared.md)
- [`combo_fes_403030_shared`](factor_mine/combo_fes_403030_shared.md)
- [`combo_fh_7030_shared`](factor_mine/combo_fh_7030_shared.md)
- [`combo_fse_333_shared`](factor_mine/combo_fse_333_shared.md)
- [`combo_he1_5050_shared`](factor_mine/combo_he1_5050_shared.md)
- [`combo_her_5050_shared`](factor_mine/combo_her_5050_shared.md)
- [`combo_hf_5050_shared`](factor_mine/combo_hf_5050_shared.md)
- [`combo_hn_3070_shared`](factor_mine/combo_hn_3070_shared.md)
- [`combo_hn_5050_shared`](factor_mine/combo_hn_5050_shared.md)
- [`combo_hn_7030_shared`](factor_mine/combo_hn_7030_shared.md)
- [`combo_jf_5050_shared`](factor_mine/combo_jf_5050_shared.md)
- [`combo_ne1_5050_shared`](factor_mine/combo_ne1_5050_shared.md)
- [`combo_ner_5050_shared`](factor_mine/combo_ner_5050_shared.md)
- [`combo_nf_5050_shared`](factor_mine/combo_nf_5050_shared.md)
- [`combo_nj_5050_shared`](factor_mine/combo_nj_5050_shared.md)
- [`combo_nse_333_shared`](factor_mine/combo_nse_333_shared.md)
- [`combo_se1_5050_shared`](factor_mine/combo_se1_5050_shared.md)
- [`combo_se_5050_shared`](factor_mine/combo_se_5050_shared.md)
- [`combo_se_5050_skip`](factor_mine/combo_se_5050_skip.md)
- [`combo_se_5050_split`](factor_mine/combo_se_5050_split.md)
- [`combo_se_5050_weather`](factor_mine/combo_se_5050_weather.md)
- [`combo_se_7030_shared`](factor_mine/combo_se_7030_shared.md)
- [`combo_seh_333_split`](factor_mine/combo_seh_333_split.md)
- [`combo_seh_502525_split`](factor_mine/combo_seh_502525_split.md)
- [`combo_ser_5050_shared`](factor_mine/combo_ser_5050_shared.md)
- [`combo_sf_3070_shared`](factor_mine/combo_sf_3070_shared.md)
- [`combo_sj_3070_shared`](factor_mine/combo_sj_3070_shared.md)
- [`combo_sj_5050_shared`](factor_mine/combo_sj_5050_shared.md)
- [`combo_sj_7030_shared`](factor_mine/combo_sj_7030_shared.md)
- [`combo_sn_3070_shared`](factor_mine/combo_sn_3070_shared.md)
- [`combo_sn_5050_shared`](factor_mine/combo_sn_5050_shared.md)
- [`combo_sn_7030_shared`](factor_mine/combo_sn_7030_shared.md)
- [`combo_snj_333_shared`](factor_mine/combo_snj_333_shared.md)
- [`flatten_h1`](factor_mine/flatten_h1.md)
- [`flatten_h3`](factor_mine/flatten_h3.md)
- [`flatten_h3_cut`](factor_mine/flatten_h3_cut.md)
- [`flatten_h3_half`](factor_mine/flatten_h3_half.md)
- [`flatten_h3_rankw`](factor_mine/flatten_h3_rankw.md)
- [`flatten_h3_sboost`](factor_mine/flatten_h3_sboost.md)
- [`flatten_h3_sizeup`](factor_mine/flatten_h3_sizeup.md)
- [`flatten_h3_time`](factor_mine/flatten_h3_time.md)
- [`flatten_h3_topheavy`](factor_mine/flatten_h3_topheavy.md)
- [`flatten_h3_trail`](factor_mine/flatten_h3_trail.md)
- [`flatten_h5_cut`](factor_mine/flatten_h5_cut.md)
- [`flatten_h5_half`](factor_mine/flatten_h5_half.md)
- [`flatten_h5_sizeup`](factor_mine/flatten_h5_sizeup.md)
- [`flatten_h5_topheavy`](factor_mine/flatten_h5_topheavy.md)
- [`flatten_h5_trail`](factor_mine/flatten_h5_trail.md)
- [`flatten_live_h1_cut`](factor_mine/flatten_live_h1_cut.md)
- [`flatten_live_h1_half`](factor_mine/flatten_live_h1_half.md)
- [`flatten_live_h1_rankw`](factor_mine/flatten_live_h1_rankw.md)
- [`flatten_live_h1_sboost`](factor_mine/flatten_live_h1_sboost.md)
- [`flatten_live_h1_time`](factor_mine/flatten_live_h1_time.md)
- [`flatten_live_h1_topheavy`](factor_mine/flatten_live_h1_topheavy.md)
- [`flatten_live_h1_trail`](factor_mine/flatten_live_h1_trail.md)
- [`flatten_vol_g_h3`](factor_mine/flatten_vol_g_h3.md)
- [`flatten_white_yday_h5`](factor_mine/flatten_white_yday_h5.md)
- [`ohlc_hot_coil_h1`](factor_mine/ohlc_hot_coil_h1.md)
- [`ohlc_hot_h1`](factor_mine/ohlc_hot_h1.md)
- [`ohlc_hot_h3`](factor_mine/ohlc_hot_h3.md)
- [`ohlc_hot_h5`](factor_mine/ohlc_hot_h5.md)
- [`oppset_h1`](factor_mine/oppset_h1.md)
- [`overnight_h3`](factor_mine/overnight_h3.md)
- [`overnight_h5`](factor_mine/overnight_h5.md)
- [`probable_h1`](factor_mine/probable_h1.md)
- [`probable_h3`](factor_mine/probable_h3.md)
- [`probable_h5`](factor_mine/probable_h5.md)
- [`probable_probable_ok_h1`](factor_mine/probable_probable_ok_h1.md)
- [`probable_probable_ok_h3`](factor_mine/probable_probable_ok_h3.md)
- [`short_alarm_h1`](factor_mine/short_alarm_h1.md)
- [`short_alarm_h3`](factor_mine/short_alarm_h3.md)
- [`short_clk_ext_veto_h3`](factor_mine/short_clk_ext_veto_h3.md)
- [`short_clk_ext_veto_opp_h3`](factor_mine/short_clk_ext_veto_opp_h3.md)
- [`short_clk_neg_weak_fail_h3`](factor_mine/short_clk_neg_weak_fail_h3.md)
- [`short_clk_neg_weak_fail_opp_h3`](factor_mine/short_clk_neg_weak_fail_opp_h3.md)
- [`short_extended_h1`](factor_mine/short_extended_h1.md)
- [`short_extended_h3`](factor_mine/short_extended_h3.md)
- [`short_last_red_h1`](factor_mine/short_last_red_h1.md)
- [`short_last_red_h3`](factor_mine/short_last_red_h3.md)
- [`short_macd_dn_h1`](factor_mine/short_macd_dn_h1.md)
- [`short_macd_dn_h3`](factor_mine/short_macd_dn_h3.md)
- [`short_news_or_h3`](factor_mine/short_news_or_h3.md)
- [`short_news_pack_h3`](factor_mine/short_news_pack_h3.md)
- [`short_news_r_h1`](factor_mine/short_news_r_h1.md)
- [`short_news_r_h3`](factor_mine/short_news_r_h3.md)
- [`short_news_r_macd_h3`](factor_mine/short_news_r_macd_h3.md)
- [`short_r_down_h1`](factor_mine/short_r_down_h1.md)
- [`short_r_down_h3`](factor_mine/short_r_down_h3.md)
- [`short_rsi_ob_h1`](factor_mine/short_rsi_ob_h1.md)
- [`short_rsi_ob_h3`](factor_mine/short_rsi_ob_h3.md)
- [`union_ab_g_h1`](factor_mine/union_ab_g_h1.md)
- [`union_ab_g_h3`](factor_mine/union_ab_g_h3.md)
- [`union_blue_coil_h1`](factor_mine/union_blue_coil_h1.md)
- [`union_blue_coil_h3`](factor_mine/union_blue_coil_h3.md)
- [`union_blue_h1`](factor_mine/union_blue_h1.md)
- [`union_blue_h3`](factor_mine/union_blue_h3.md)
- [`union_blue_vol_h1`](factor_mine/union_blue_vol_h1.md)
- [`union_blue_vol_h3`](factor_mine/union_blue_vol_h3.md)
- [`union_break10_h1`](factor_mine/union_break10_h1.md)
- [`union_break10_h3`](factor_mine/union_break10_h3.md)
- [`union_candle_h1`](factor_mine/union_candle_h1.md)
- [`union_candle_h3`](factor_mine/union_candle_h3.md)
- [`union_candle_score_h1`](factor_mine/union_candle_score_h1.md)
- [`union_candle_score_h3`](factor_mine/union_candle_score_h3.md)
- [`union_catal_present_h1`](factor_mine/union_catal_present_h1.md)
- [`union_catal_present_h3`](factor_mine/union_catal_present_h3.md)
- [`union_clk_earn_guide_react_h1`](factor_mine/union_clk_earn_guide_react_h1.md)
- [`union_clk_flow_coil_h1`](factor_mine/union_clk_flow_coil_h1.md)
- [`union_clk_fresh_cat_coil_h1`](factor_mine/union_clk_fresh_cat_coil_h1.md)
- [`union_clk_fresh_cat_coil_opp_h1`](factor_mine/union_clk_fresh_cat_coil_opp_h1.md)
- [`union_clk_hold_vs_sector_h1`](factor_mine/union_clk_hold_vs_sector_h1.md)
- [`union_clk_hold_vs_sector_opp_h1`](factor_mine/union_clk_hold_vs_sector_opp_h1.md)
- [`union_clk_insider_cash_stab_h3`](factor_mine/union_clk_insider_cash_stab_h3.md)
- [`union_clk_mom_break_peer_h1`](factor_mine/union_clk_mom_break_peer_h1.md)
- [`union_clk_mom_break_peer_opp_h1`](factor_mine/union_clk_mom_break_peer_opp_h1.md)
- [`union_clk_nr7_mom_h1`](factor_mine/union_clk_nr7_mom_h1.md)
- [`union_clk_nr7_mom_opp_h1`](factor_mine/union_clk_nr7_mom_opp_h1.md)
- [`union_clk_r_up_coil_h1`](factor_mine/union_clk_r_up_coil_h1.md)
- [`union_coil_green_h1`](factor_mine/union_coil_green_h1.md)
- [`union_coil_green_h3`](factor_mine/union_coil_green_h3.md)
- [`union_coil_off_h1`](factor_mine/union_coil_off_h1.md)
- [`union_coil_off_h3`](factor_mine/union_coil_off_h3.md)
- [`union_coil_off_h5`](factor_mine/union_coil_off_h5.md)
- [`union_cond_h1`](factor_mine/union_cond_h1.md)
- [`union_cond_h3`](factor_mine/union_cond_h3.md)
- [`union_cond_n4_h3`](factor_mine/union_cond_n4_h3.md)
- [`union_e_fresh_h1`](factor_mine/union_e_fresh_h1.md)
- [`union_e_green_h1`](factor_mine/union_e_green_h1.md)
- [`union_earn_react_h1`](factor_mine/union_earn_react_h1.md)
- [`union_earn_react_h3`](factor_mine/union_earn_react_h3.md)
- [`union_flow_in_h1`](factor_mine/union_flow_in_h1.md)
- [`union_flow_in_h3`](factor_mine/union_flow_in_h3.md)
- [`union_flow_in_h5`](factor_mine/union_flow_in_h5.md)
- [`union_flow_in_white_h1`](factor_mine/union_flow_in_white_h1.md)
- [`union_flow_in_white_h3`](factor_mine/union_flow_in_white_h3.md)
- [`union_h1`](factor_mine/union_h1.md)
- [`union_h1_cut`](factor_mine/union_h1_cut.md)
- [`union_h1_half`](factor_mine/union_h1_half.md)
- [`union_h1_rankw`](factor_mine/union_h1_rankw.md)
- [`union_h1_sboost`](factor_mine/union_h1_sboost.md)
- [`union_h1_sizeup`](factor_mine/union_h1_sizeup.md)
- [`union_h1_time`](factor_mine/union_h1_time.md)
- [`union_h1_trail`](factor_mine/union_h1_trail.md)
- [`union_h3`](factor_mine/union_h3.md)
- [`union_h3_exit_alarm`](factor_mine/union_h3_exit_alarm.md)
- [`union_h3_exit_news_r`](factor_mine/union_h3_exit_news_r.md)
- [`union_h3_exit_red`](factor_mine/union_h3_exit_red.md)
- [`union_h3_half`](factor_mine/union_h3_half.md)
- [`union_h3_rankw`](factor_mine/union_h3_rankw.md)
- [`union_h3_sboost`](factor_mine/union_h3_sboost.md)
- [`union_h3_sizeup`](factor_mine/union_h3_sizeup.md)
- [`union_h3_time`](factor_mine/union_h3_time.md)
- [`union_h3_topheavy`](factor_mine/union_h3_topheavy.md)
- [`union_h3_trail`](factor_mine/union_h3_trail.md)
- [`union_h5`](factor_mine/union_h5.md)
- [`union_h5_cut`](factor_mine/union_h5_cut.md)
- [`union_h5_exit_alarm`](factor_mine/union_h5_exit_alarm.md)
- [`union_h5_half`](factor_mine/union_h5_half.md)
- [`union_h5_rankw`](factor_mine/union_h5_rankw.md)
- [`union_h5_sizeup`](factor_mine/union_h5_sizeup.md)
- [`union_h5_time`](factor_mine/union_h5_time.md)
- [`union_h5_topheavy`](factor_mine/union_h5_topheavy.md)
- [`union_h5_trail`](factor_mine/union_h5_trail.md)
- [`union_hot_n12_h1`](factor_mine/union_hot_n12_h1.md)
- [`union_hot_score_h1`](factor_mine/union_hot_score_h1.md)
- [`union_hot_score_h3`](factor_mine/union_hot_score_h3.md)
- [`union_join_g_h1`](factor_mine/union_join_g_h1.md)
- [`union_join_g_h3`](factor_mine/union_join_g_h3.md)
- [`union_join_present_h1`](factor_mine/union_join_present_h1.md)
- [`union_join_present_h3`](factor_mine/union_join_present_h3.md)
- [`union_join_vol_green_h1`](factor_mine/union_join_vol_green_h1.md)
- [`union_join_vol_green_h3`](factor_mine/union_join_vol_green_h3.md)
- [`union_last_green_h1`](factor_mine/union_last_green_h1.md)
- [`union_last_green_h3`](factor_mine/union_last_green_h3.md)
- [`union_last_green_h5`](factor_mine/union_last_green_h5.md)
- [`union_last_red_h1`](factor_mine/union_last_red_h1.md)
- [`union_last_red_h3`](factor_mine/union_last_red_h3.md)
- [`union_macd_hist_h1`](factor_mine/union_macd_hist_h1.md)
- [`union_macd_hist_h3`](factor_mine/union_macd_hist_h3.md)
- [`union_macd_up_h1`](factor_mine/union_macd_up_h1.md)
- [`union_macd_up_h3`](factor_mine/union_macd_up_h3.md)
- [`union_macd_xup_h1`](factor_mine/union_macd_xup_h1.md)
- [`union_macd_xup_h3`](factor_mine/union_macd_xup_h3.md)
- [`union_news_both_h3`](factor_mine/union_news_both_h3.md)
- [`union_news_g_cam61_h1`](factor_mine/union_news_g_cam61_h1.md)
- [`union_news_g_cam61_h3`](factor_mine/union_news_g_cam61_h3.md)
- [`union_news_g_cam71_conv_h1`](factor_mine/union_news_g_cam71_conv_h1.md)
- [`union_news_g_cam71_h3`](factor_mine/union_news_g_cam71_h3.md)
- [`union_news_g_cam71_n2_h1`](factor_mine/union_news_g_cam71_n2_h1.md)
- [`union_news_g_cond_h1`](factor_mine/union_news_g_cond_h1.md)
- [`union_news_g_cond_h3`](factor_mine/union_news_g_cond_h3.md)
- [`union_news_g_conv_h3`](factor_mine/union_news_g_conv_h3.md)
- [`union_news_g_h1`](factor_mine/union_news_g_h1.md)
- [`union_news_g_h3`](factor_mine/union_news_g_h3.md)
- [`union_news_head_h1`](factor_mine/union_news_head_h1.md)
- [`union_news_head_h3`](factor_mine/union_news_head_h3.md)
- [`union_news_missing_h1`](factor_mine/union_news_missing_h1.md)
- [`union_news_missing_h3`](factor_mine/union_news_missing_h3.md)
- [`union_news_or_h3`](factor_mine/union_news_or_h3.md)
- [`union_news_or_net2_h1`](factor_mine/union_news_or_net2_h1.md)
- [`union_news_or_net2_h3`](factor_mine/union_news_or_net2_h3.md)
- [`union_news_or_net3_h3`](factor_mine/union_news_or_net3_h3.md)
- [`union_news_or_net4_h3`](factor_mine/union_news_or_net4_h3.md)
- [`union_news_or_net5_h1`](factor_mine/union_news_or_net5_h1.md)
- [`union_news_or_net5_h3`](factor_mine/union_news_or_net5_h3.md)
- [`union_news_pack_h3`](factor_mine/union_news_pack_h3.md)
- [`union_news_pack_net2_h3`](factor_mine/union_news_pack_net2_h3.md)
- [`union_news_pack_net3_h1`](factor_mine/union_news_pack_net3_h1.md)
- [`union_news_pack_net3_h3`](factor_mine/union_news_pack_net3_h3.md)
- [`union_news_present_h1`](factor_mine/union_news_present_h1.md)
- [`union_news_present_h3`](factor_mine/union_news_present_h3.md)
- [`union_news_vol_h1`](factor_mine/union_news_vol_h1.md)
- [`union_news_vol_h3`](factor_mine/union_news_vol_h3.md)
- [`union_oppset_h1`](factor_mine/union_oppset_h1.md)
- [`union_overnight_h1`](factor_mine/union_overnight_h1.md)
- [`union_overnight_h3`](factor_mine/union_overnight_h3.md)
- [`union_r_up_h1`](factor_mine/union_r_up_h1.md)
- [`union_r_up_h3`](factor_mine/union_r_up_h3.md)
- [`union_ret_5_h1`](factor_mine/union_ret_5_h1.md)
- [`union_ret_5_h3`](factor_mine/union_ret_5_h3.md)
- [`union_rsi_h1`](factor_mine/union_rsi_h1.md)
- [`union_rsi_h3`](factor_mine/union_rsi_h3.md)
- [`union_rsi_os_h1`](factor_mine/union_rsi_os_h1.md)
- [`union_rsi_os_h3`](factor_mine/union_rsi_os_h3.md)
- [`union_rsi_os_h5`](factor_mine/union_rsi_os_h5.md)
- [`union_rsi_os_macd_h1`](factor_mine/union_rsi_os_macd_h1.md)
- [`union_rsi_os_macd_h3`](factor_mine/union_rsi_os_macd_h3.md)
- [`union_vol_ab_h1`](factor_mine/union_vol_ab_h1.md)
- [`union_vol_ab_h3`](factor_mine/union_vol_ab_h3.md)
- [`union_vol_g_h1`](factor_mine/union_vol_g_h1.md)
- [`union_vol_g_h3`](factor_mine/union_vol_g_h3.md)
- [`union_vol_g_h5`](factor_mine/union_vol_g_h5.md)
- [`union_vol_green_h1`](factor_mine/union_vol_green_h1.md)
- [`union_vol_green_h3`](factor_mine/union_vol_green_h3.md)
- [`union_vol_missing_h1`](factor_mine/union_vol_missing_h1.md)
- [`union_vol_missing_h3`](factor_mine/union_vol_missing_h3.md)
- [`union_w_hot_candle_h1`](factor_mine/union_w_hot_candle_h1.md)
- [`union_w_hot_candle_h3`](factor_mine/union_w_hot_candle_h3.md)
- [`union_white_both_n4_h3`](factor_mine/union_white_both_n4_h3.md)
- [`union_white_coil_h3`](factor_mine/union_white_coil_h3.md)
- [`union_white_h1`](factor_mine/union_white_h1.md)
- [`union_white_h3`](factor_mine/union_white_h3.md)
- [`union_white_h5`](factor_mine/union_white_h5.md)
- [`union_white_yday_h1`](factor_mine/union_white_yday_h1.md)
- [`union_white_yday_h3`](factor_mine/union_white_yday_h3.md)
- [`yday_gainer_h1`](factor_mine/yday_gainer_h1.md)
- [`yday_gainer_h3`](factor_mine/yday_gainer_h3.md)
- [`yday_gainer_h5`](factor_mine/yday_gainer_h5.md)
