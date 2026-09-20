# Factor strategy mine — 2026-08-13 → 2026-09-09

Leak-free 09:30 recipes: **339** · candidate rows **1668** · fill `09:30 open, whole shares, Futubull fees, leftover split, sell first, hard-red sit, cash+holdings audit`.

Cash book: $10k, whole shares, Futubull fees, leftover split, sell first, min-hold, 09:30 open, hard-red S≤−3 sit, shorts marked as a liability. Each session starts from leftover cash and lots actually held (butterfly). Cash-start buttons wake a sleeve on date X with $10k and no lots (same rules). Stock investigator quotes 09:30 cameras / coaches / news from repo files. Size / sell / S-boost tweaks sit on the same ledger. Signal-only % is the old equal-weight path (not a fill). `flatten_h*` = wish-list (io/HOLD mornings still buy). `flatten_live_*` = only when the live flatten gate fires. Research only — does not change live `flatten_robust`.

Scoreboard files: slim `factor_mine.json` (stats / recipes / series) plus gzip shards in `factor_mine/shards/` (`books`, `starts`, `daily`, `probe`, `sim`). One file used to be 104MB and GitHub rejected the publish (100MB cap).

Combination books: **79** mixes on the same $10k cash ledger (shared leftover or split sleeves, official 09:30 / 16:00, hard-red sit, owner min-hold). Outperformers: `combo_sh_macd_5050_shared`, `combo_sh_5050_shared`, `combo_jse_333_shared`, `combo_sh_3070_shared`, `combo_sh_7030_shared`, `combo_ej_5050_shared`, `combo_jer_5050_shared`, `combo_seh_451540_shared`, `combo_seh_601525_shared`, `combo_seh_333_skip`, `combo_se_5050_skip`, `combo_se_3070_shared`, `combo_es_8020_shared`, `combo_seh_502525_shared`, `combo_es_9010_shared`, `combo_seh_403525_shared`, `combo_se1_5050_shared`, `combo_seh_404020_shared`, `combo_se_5050_shared`, `combo_se_5050_weather`, `combo_seh_333_shared`, `combo_seh_333_weather`, `combo_nse_333_shared`, `combo_ers_7030_shared`, `combo_ehs_601525_shared`, `combo_ser_5050_shared`, `combo_p2s_5050_shared`, `combo_sj_5050_shared`, `combo_ps_5050_shared`, `combo_ps_7030_shared`, `combo_sj_3070_shared`, `combo_e1s_7030_shared`, `combo_ner_5050_shared`, `combo_sj_7030_shared`, `combo_sf_5050_shared`, `combo_snj_333_shared`, `combo_sf_3070_shared`, `combo_sf_7030_shared`, `combo_sn_5050_shared`, `combo_je1_5050_shared`, `combo_sn_3070_shared`, `combo_sn_7030_shared`, `combo_ne1_5050_shared`.

A combo outperforms its members on the cash book when the audit passes and either (1) Book% is strictly higher than every member, or (2) Book% stays within 2pp of the richest member, max drawdown is strictly smaller than every member's drawdown, start-rate is at least the best member, both halves are green, and the worst session is no worse than the worst member session. Effectiveness / Signal% do not decide WIN.

Action blotters: [FACTOR_MINE_ACTION.md](FACTOR_MINE_ACTION.md).

| Strategy | Side | H | Size | Sell | Boost | Win% | $ days | Starts YES | Med start | Top-g | Losers | AvgW | AvgL | Book% | Signal% | Audit | Eff |
|---|---|---:|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---:|
| `combo_ps_5050_shared` | mix | 5 | leftover | list | none | 66% | 74% | 17/19 | +9.56 | 0 | 0 | +4.50 | -4.45 | +21.19 | — | PASS | 75.716 |
| `combo_p2s_5050_shared` | mix | 5 | leftover | list | none | 65% | 68% | 17/19 | +11.07 | 0 | 0 | +4.63 | -4.11 | +21.77 | — | PASS | 75.429 |
| `combo_sh_7030_shared` | mix | 5 | leftover | list | none | 62% | 74% | 17/19 | +9.60 | 0 | 0 | +5.48 | -4.79 | +28.70 | — | PASS | 75.124 |
| `combo_sh_5050_shared` | mix | 5 | leftover | list | none | 62% | 68% | 17/19 | +9.60 | 0 | 0 | +5.50 | -4.72 | +33.46 | — | PASS | 74.89 |
| `combo_ps_7030_shared` | mix | 5 | leftover | list | none | 65% | 68% | 17/19 | +9.86 | 0 | 0 | +4.46 | -4.52 | +20.79 | — | PASS | 74.063 |
| `combo_sh_macd_5050_shared` | mix | 5 | leftover | list | none | 62% | 58% | 17/19 | +9.05 | 0 | 0 | +5.46 | -4.57 | +35.66 | — | PASS | 72.99 |
| `combo_sh_3070_shared` | mix | 5 | leftover | list | none | 60% | 63% | 17/19 | +7.19 | 0 | 0 | +5.54 | -4.61 | +29.81 | — | PASS | 72.928 |
| `combo_sj_5050_shared` | mix | 5 | leftover | list | none | 57% | 79% | 17/19 | +12.29 | 0 | 0 | +5.25 | -5.04 | +21.25 | — | PASS | 72.571 |
| `combo_sn_5050_shared` | mix | 5 | leftover | list | none | 60% | 74% | 17/19 | +8.76 | 0 | 0 | +4.33 | -4.63 | +14.13 | — | PASS | 71.59 |
| `combo_sj_3070_shared` | mix | 5 | leftover | list | none | 57% | 74% | 17/19 | +12.14 | 0 | 0 | +5.23 | -5.06 | +20.50 | — | PASS | 71.368 |
| `combo_sn_3070_shared` | mix | 5 | leftover | list | none | 58% | 74% | 17/19 | +6.30 | 0 | 0 | +4.43 | -4.45 | +12.88 | — | PASS | 71.075 |
| `combo_sn_7030_shared` | mix | 5 | leftover | list | none | 60% | 68% | 17/19 | +6.59 | 0 | 0 | +4.18 | -4.83 | +11.42 | — | PASS | 69.782 |
| `combo_snj_333_shared` | mix | 5 | leftover | list | none | 54% | 68% | 17/19 | +8.76 | 0 | 0 | +4.66 | -4.76 | +15.05 | — | PASS | 69.237 |
| `combo_jse_333_shared` | mix | 5 | leftover | list | none | 60% | 63% | 16/19 | +3.32 | 0 | 0 | +6.02 | -6.21 | +30.34 | — | PASS | 69.064 |
| `combo_sj_7030_shared` | mix | 5 | leftover | list | none | 54% | 68% | 17/19 | +10.43 | 0 | 0 | +5.31 | -4.99 | +16.61 | — | PASS | 69.021 |
| `combo_se1_5050_shared` | mix | 5 | leftover | list | none | 58% | 74% | 16/19 | +8.07 | 0 | 0 | +4.84 | -6.51 | +24.44 | — | PASS | 69.003 |
| `combo_seh_502525_shared` | mix | 5 | leftover | list | none | 50% | 74% | 17/19 | +3.64 | 0 | 0 | +6.56 | -6.45 | +25.80 | — | PASS | 68.153 |
| `combo_e1s_7030_shared` | mix | 5 | leftover | list | none | 57% | 74% | 16/19 | +4.98 | 0 | 0 | +4.93 | -6.31 | +19.92 | — | PASS | 68.087 |
| `combo_seh_601525_shared` | mix | 5 | leftover | list | none | 50% | 68% | 17/19 | +6.54 | 0 | 0 | +6.59 | -6.56 | +27.01 | — | PASS | 67.051 |
| `combo_seh_451540_shared` | mix | 5 | leftover | list | none | 50% | 63% | 17/19 | +6.19 | 0 | 0 | +6.60 | -6.53 | +27.38 | — | PASS | 66.094 |
| `combo_seh_333_skip` | mix | 5 | leftover | list | none | 54% | 63% | 16/19 | +2.15 | 0 | 0 | +6.39 | -6.59 | +26.60 | — | PASS | 66.039 |
| `combo_seh_404020_shared` | mix | 5 | leftover | list | none | 52% | 63% | 16/19 | +3.16 | 0 | 0 | +6.36 | -6.61 | +24.26 | — | PASS | 64.737 |
| `combo_seh_403525_shared` | mix | 5 | leftover | list | none | 50% | 63% | 16/19 | +3.07 | 0 | 0 | +6.59 | -6.40 | +24.61 | — | PASS | 64.62 |
| `combo_seh_333_shared` | mix | 5 | leftover | list | none | 51% | 63% | 16/19 | +2.15 | 0 | 0 | +6.45 | -6.50 | +23.79 | — | PASS | 64.481 |
| `combo_seh_333_weather` | mix | 5 | leftover | list | none | 51% | 63% | 16/19 | +2.15 | 0 | 0 | +6.45 | -6.50 | +23.79 | — | PASS | 64.481 |
| `combo_nse_333_shared` | mix | 5 | leftover | list | none | 52% | 47% | 16/19 | +2.50 | 0 | 0 | +5.46 | -6.40 | +23.57 | — | PASS | 61.499 |
| `combo_se_5050_shared` | mix | 5 | leftover | list | none | 60% | 58% | 14/19 | +2.52 | 0 | 0 | +6.38 | -8.05 | +24.21 | — | PASS | 61.361 |
| `combo_se_5050_weather` | mix | 5 | leftover | list | none | 60% | 58% | 14/19 | +2.52 | 0 | 0 | +6.38 | -8.05 | +24.21 | — | PASS | 61.361 |
| `combo_se_5050_skip` | mix | 5 | leftover | list | none | 62% | 53% | 14/19 | +4.21 | 0 | 0 | +6.32 | -8.60 | +26.07 | — | PASS | 61.221 |
| `combo_je1_5050_shared` | mix | 5 | leftover | list | none | 50% | 47% | 14/19 | +3.84 | 0 | 0 | +4.79 | -5.34 | +13.45 | — | PASS | 60.181 |
| `combo_ehs_601525_shared` | mix | 5 | leftover | list | none | 47% | 53% | 14/19 | +2.88 | 0 | 0 | +6.74 | -6.19 | +22.98 | — | PASS | 58.59 |
| `combo_es_8020_shared` | mix | 5 | leftover | list | none | 51% | 58% | 14/19 | +2.92 | 0 | 0 | +6.79 | -7.34 | +25.91 | — | PASS | 58.536 |
| `combo_ej_5050_shared` | mix | 5 | leftover | list | none | 49% | 53% | 12/19 | +0.90 | 0 | 0 | +6.63 | -6.17 | +28.38 | — | PASS | 58.487 |
| `combo_se_3070_shared` | mix | 5 | leftover | list | none | 53% | 53% | 14/19 | +4.04 | 0 | 0 | +6.63 | -7.72 | +25.93 | — | PASS | 58.198 |
| `combo_ser_5050_shared` | mix | 5 | leftover | list | none | 56% | 47% | 15/19 | +0.93 | 0 | 0 | +6.58 | -8.55 | +22.03 | — | PASS | 58.187 |
| `combo_jer_5050_shared` | mix | 5 | leftover | list | none | 45% | 53% | 13/19 | +0.90 | 0 | 0 | +6.96 | -6.48 | +27.39 | — | PASS | 57.711 |
| `combo_es_9010_shared` | mix | 5 | leftover | list | none | 48% | 58% | 13/19 | +2.92 | 0 | 0 | +7.32 | -6.37 | +25.00 | — | PASS | 57.129 |
| `combo_ers_7030_shared` | mix | 5 | leftover | list | none | 50% | 42% | 15/19 | +1.61 | 0 | 0 | +6.68 | -8.39 | +23.32 | — | PASS | 55.175 |
| `combo_sf_5050_shared` | mix | 5 | leftover | list | none | 64% | 74% | 8/19 | +0.00 | 0 | 0 | +7.13 | -4.45 | +16.00 | — | PASS | 51.109 |
| `combo_sf_7030_shared` | mix | 5 | leftover | list | none | 64% | 74% | 9/19 | +0.00 | 0 | 0 | +6.69 | -5.37 | +14.65 | — | PASS | 50.445 |
| `combo_sf_3070_shared` | mix | 5 | leftover | list | none | 61% | 63% | 7/19 | -2.63 | 0 | 0 | +7.58 | -4.73 | +14.77 | — | PASS | 46.37 |
| `combo_ne1_5050_shared` | mix | 5 | leftover | list | none | 52% | 42% | 7/19 | -0.12 | 0 | 0 | +4.02 | -5.36 | +10.15 | — | PASS | 39.619 |
| `combo_ner_5050_shared` | mix | 5 | leftover | list | none | 44% | 47% | 6/19 | -0.48 | 0 | 0 | +5.74 | -6.46 | +19.86 | — | PASS | 35.101 |
| `union_hot_n4_holdup` | long | 1 | leftover | list | holdup | 53% | 63% | 17/19 | +9.63 | 9 | 21 | +7.50 | -5.31 | +37.49 | +32.12 | PASS | 74.99 |
| `union_news_pack_net2_h1` | long | 1 | leftover | list | none | 60% | 47% | 17/19 | +11.06 | 1 | 4 | +3.56 | -2.68 | +19.16 | +21.29 | PASS | 74.022 |
| `union_news_pack_net3_h1` | long | 1 | leftover | list | none | 60% | 47% | 17/19 | +11.06 | 1 | 4 | +3.56 | -2.68 | +19.16 | +20.84 | PASS | 73.72 |
| `union_news_pack_h1` | long | 1 | leftover | list | none | 58% | 47% | 17/19 | +6.75 | 2 | 8 | +3.39 | -3.37 | +15.08 | +19.46 | PASS | 69.739 |
| `short_news_r_macd_h3` | short | 3 | leftover | list | none | 62% | 68% | 17/19 | +6.81 | 6 | 29 | +5.08 | -5.97 | +13.39 | +20.32 | PASS | 67.102 |
| `combo_hn_3070_shared` | mix | 5 | leftover | list | none | 55% | 58% | 15/19 | +1.68 | 0 | 0 | +4.41 | -4.31 | +12.39 | — | PASS | 66.898 |
| `short_news_head_h3` | short | 3 | leftover | list | none | 58% | 68% | 17/19 | +8.78 | 5 | 29 | +6.42 | -4.86 | +13.60 | +16.54 | PASS | 66.591 |
| `union_news_or_h1` | long | 1 | leftover | list | none | 52% | 47% | 17/19 | +3.98 | 2 | 21 | +4.08 | -3.58 | +5.60 | +9.58 | PASS | 66.179 |
| `union_news_g_h1` | long | 1 | leftover | list | none | 54% | 47% | 16/19 | +3.20 | 2 | 20 | +3.87 | -3.94 | +5.80 | +12.49 | PASS | 64.92 |
| `union_news_g_cond_h1` | long | 1 | leftover | list | none | 52% | 42% | 17/19 | +3.60 | 2 | 22 | +4.08 | -3.82 | +4.58 | +8.98 | PASS | 64.118 |
| `short_news_or_h3` | short | 3 | leftover | list | none | 57% | 63% | 17/19 | +7.33 | 7 | 34 | +5.20 | -6.11 | +11.88 | +12.36 | PASS | 64.117 |
| `short_news_r_h3` | short | 3 | leftover | list | none | 57% | 63% | 17/19 | +7.33 | 7 | 34 | +5.20 | -6.11 | +11.88 | +12.36 | PASS | 64.117 |
| `union_news_g_conv_h1` | long | 1 | conviction | list | none | 54% | 47% | 14/19 | +2.07 | 1 | 9 | +3.77 | -3.63 | +11.96 | +10.73 | PASS | 63.961 |
| `combo_seh_502525_split` | mix | 5 | leftover | list | none | 49% | 68% | 16/19 | +2.85 | 0 | 0 | +6.73 | -6.27 | +13.75 | — | PASS | 63.572 |
| `short_news_r_h1` | short | 1 | leftover | list | none | 53% | 42% | 16/19 | +1.71 | 4 | 13 | +3.52 | -3.88 | +0.63 | +5.04 | PASS | 62.108 |
| `union_news_or_net4_h1` | long | 1 | leftover | list | none | 47% | 42% | 16/19 | +2.37 | 1 | 12 | +3.97 | -3.64 | +6.75 | +5.49 | PASS | 61.574 |
| `union_news_or_net4_rw_h1` | long | 1 | rank_w | list | none | 52% | 47% | 12/19 | +1.77 | 1 | 5 | +3.33 | -3.15 | +8.93 | +7.62 | PASS | 61.016 |
| `combo_seh_333_split` | mix | 5 | leftover | list | none | 50% | 58% | 15/19 | +2.06 | 0 | 0 | +6.45 | -6.38 | +15.74 | — | PASS | 60.659 |
| `union_clk_mom_break_peer_opp_h1` | long | 1 | leftover | list | none | 50% | 37% | 14/19 | +8.77 | 8 | 15 | +3.90 | -3.49 | +12.31 | +11.10 | PASS | 60.653 |
| `combo_nj_5050_shared` | mix | 5 | leftover | list | none | 49% | 42% | 15/19 | +4.69 | 0 | 0 | +4.40 | -4.46 | +5.97 | — | PASS | 60.357 |
| `combo_se_7030_shared` | mix | 5 | leftover | list | none | 62% | 58% | 13/19 | +0.20 | 0 | 0 | +6.31 | -7.77 | +21.42 | — | PASS | 60.339 |
| `union_hot_n4_h1` | long | 1 | leftover | list | none | 53% | 42% | 11/19 | +1.74 | 9 | 21 | +5.90 | -4.45 | +18.84 | +32.12 | PASS | 59.647 |
| `union_news_or_net4_conv_h1` | long | 1 | conviction | list | none | 52% | 47% | 11/19 | +2.07 | 1 | 5 | +3.32 | -3.45 | +11.63 | +7.62 | PASS | 59.628 |
| `combo_se_5050_split` | mix | 5 | leftover | list | none | 49% | 58% | 16/19 | +3.69 | 0 | 0 | +7.18 | -6.86 | +16.00 | — | PASS | 59.505 |
| `union_join_vol_green_h1` | long | 1 | leftover | list | none | 46% | 37% | 15/19 | +8.60 | 5 | 17 | +5.22 | -4.56 | +11.14 | +4.98 | PASS | 59.306 |
| `union_white_both_n4_h5` | long | 5 | leftover | list | none | 39% | 63% | 10/19 | +0.01 | 9 | 23 | +11.38 | -3.15 | +15.66 | +17.40 | PASS | 59.045 |
| `union_white_both_n4_h5_s12` | long | 5 | leftover | list | none | 39% | 63% | 10/19 | +0.01 | 9 | 23 | +11.38 | -3.15 | +15.66 | +17.40 | PASS | 59.045 |
| `combo_hn_5050_shared` | mix | 5 | leftover | list | none | 55% | 42% | 11/19 | +1.70 | 0 | 0 | +4.38 | -4.33 | +13.76 | — | PASS | 58.447 |
| `combo_hn_7030_shared` | mix | 5 | leftover | list | none | 54% | 42% | 11/19 | +1.75 | 0 | 0 | +4.34 | -4.37 | +15.69 | — | PASS | 58.271 |
| `short_clk_ext_veto_opp_h3` | short | 3 | leftover | list | none | 51% | 53% | 16/19 | +1.81 | 11 | 49 | +7.59 | -6.26 | +2.48 | -17.12 | PASS | 57.252 |
| `union_news_or_net4_h3` | long | 3 | leftover | list | none | 44% | 47% | 16/19 | +0.53 | 6 | 31 | +6.22 | -4.86 | +0.49 | +2.59 | PASS | 57.148 |
| `union_news_or_net5_h1` | long | 1 | leftover | list | none | 41% | 32% | 16/19 | +1.87 | 1 | 7 | +3.41 | -3.56 | +4.69 | +4.71 | PASS | 56.888 |
| `union_news_g_conv_h3` | long | 3 | conviction | list | none | 52% | 53% | 15/19 | +3.86 | 5 | 34 | +4.79 | -6.42 | +2.51 | +224.83 | PASS | 56.692 |
| `union_news_vol_h1` | long | 1 | leftover | list | none | 48% | 32% | 14/19 | +0.73 | 3 | 12 | +4.21 | -4.29 | +2.83 | +5.80 | PASS | 56.395 |
| `union_news_g_cam71_h1` | long | 1 | leftover | list | none | 37% | 37% | 15/19 | +4.27 | 1 | 2 | +2.70 | -3.18 | +4.27 | +4.65 | PASS | 55.885 |
| `combo_hj_5050_shared` | mix | 5 | leftover | list | none | 49% | 37% | 11/19 | +1.74 | 0 | 0 | +5.04 | -4.41 | +12.23 | — | PASS | 55.281 |
| `union_news_or_net2_h1` | long | 1 | leftover | list | none | 48% | 37% | 12/19 | +1.36 | 1 | 15 | +4.12 | -3.67 | +1.36 | +3.50 | PASS | 55.003 |
| `combo_ehs_702010_shared` | mix | 5 | leftover | list | none | 41% | 53% | 12/19 | +2.03 | 0 | 0 | +7.67 | -5.40 | +21.26 | — | PASS | 54.875 |
| `union_e_fresh_h1` | long | 1 | leftover | list | none | 48% | 47% | 12/19 | +0.36 | 6 | 31 | +4.53 | -6.33 | +9.32 | +8.21 | PASS | 54.257 |
| `union_e_fresh_h3` | long | 3 | leftover | list | none | 44% | 58% | 12/19 | +1.26 | 14 | 66 | +9.90 | -7.53 | +22.49 | -17.84 | PASS | 54.082 |
| `combo_he1_5050_shared` | mix | 5 | leftover | list | none | 51% | 42% | 10/19 | +0.57 | 0 | 0 | +4.97 | -5.47 | +13.23 | — | PASS | 54.031 |
| `combo_ee1_3070_shared` | mix | 5 | leftover | list | none | 37% | 58% | 12/19 | +1.26 | 0 | 0 | +9.90 | -7.53 | +22.49 | — | PASS | 53.912 |
| `combo_ee1_5050_shared` | mix | 5 | leftover | list | none | 37% | 58% | 12/19 | +1.26 | 0 | 0 | +9.90 | -7.53 | +22.49 | — | PASS | 53.912 |
| `combo_ee1_7030_shared` | mix | 5 | leftover | list | none | 37% | 58% | 12/19 | +1.26 | 0 | 0 | +9.90 | -7.53 | +22.49 | — | PASS | 53.912 |
| `union_news_head_h1` | long | 1 | leftover | list | none | 51% | 32% | 12/19 | +1.09 | 2 | 16 | +4.05 | -4.01 | -2.77 | +4.42 | PASS | 53.896 |
| `combo_ef_7030_shared` | mix | 5 | leftover | list | none | 47% | 63% | 11/19 | +0.09 | 0 | 0 | +8.49 | -7.00 | +14.99 | — | PASS | 53.55 |
| `combo_eh_7030_shared` | mix | 5 | leftover | list | none | 36% | 47% | 12/19 | +1.18 | 0 | 0 | +8.47 | -5.61 | +18.76 | — | PASS | 52.812 |
| `union_news_or_net3_h1` | long | 1 | leftover | list | none | 47% | 32% | 12/19 | +0.98 | 1 | 15 | +4.05 | -3.97 | +0.98 | +3.98 | PASS | 52.352 |
| `union_oppset_h1` | long | 1 | leftover | list | none | 54% | 37% | 10/19 | +0.60 | 9 | 38 | +4.82 | -5.43 | +0.68 | +5.82 | PASS | 52.237 |
| `union_last_red_h3` | long | 3 | leftover | list | none | 43% | 58% | 12/19 | +0.77 | 11 | 82 | +6.75 | -4.70 | +2.56 | +10.79 | PASS | 51.997 |
| `short_alarm_h1` | short | 1 | leftover | list | none | 51% | 21% | 12/19 | +0.32 | 5 | 33 | +4.60 | -2.94 | +1.71 | +6.64 | PASS | 51.732 |
| `union_vol_green_h1` | long | 1 | leftover | list | none | 43% | 37% | 11/19 | +3.39 | 9 | 26 | +5.25 | -5.36 | +7.18 | -6.88 | PASS | 51.164 |
| `union_news_g_cam71_h3` | long | 3 | leftover | list | none | 41% | 42% | 16/19 | +0.41 | 1 | 11 | +4.13 | -7.09 | +0.30 | -1.32 | PASS | 51.137 |
| `union_clk_hold_vs_sector_opp_h1` | long | 1 | leftover | list | none | 44% | 42% | 11/19 | +0.08 | 1 | 20 | +4.27 | -4.39 | +3.09 | -0.10 | PASS | 50.264 |
| `union_white_h1` | long | 1 | leftover | list | none | 39% | 42% | 10/19 | +2.65 | 1 | 16 | +5.73 | -5.01 | +6.17 | -0.22 | PASS | 49.551 |
| `union_white_both_n4_h2` | long | 2 | leftover | list | none | 48% | 63% | 8/19 | +0.00 | 6 | 10 | +9.26 | -6.92 | +14.45 | +17.48 | PASS | 48.075 |
| `short_alarm_h3` | short | 3 | leftover | list | none | 52% | 42% | 12/19 | +1.35 | 9 | 60 | +7.29 | -8.49 | +2.59 | +4.05 | PASS | 47.604 |
| `union_news_pack_net2_h3` | long | 3 | leftover | list | none | 47% | 32% | 12/19 | +4.53 | 2 | 15 | +5.94 | -7.31 | -8.62 | +2.06 | PASS | 47.199 |
| `short_clk_ext_veto_h3` | short | 3 | leftover | list | none | 52% | 47% | 10/19 | +0.89 | 21 | 81 | +6.65 | -7.62 | -3.74 | -95.92 | PASS | 46.395 |
| `combo_oh_5050_shared` | mix | 5 | leftover | list | none | 56% | 42% | 9/19 | +0.00 | 0 | 0 | +5.74 | -4.49 | +5.75 | — | PASS | 46.303 |
| `union_news_g_cam71_conv_h1` | long | 1 | conviction | list | none | 33% | 26% | 10/19 | +1.30 | 1 | 1 | +2.68 | -3.46 | +2.49 | +3.33 | PASS | 45.799 |
| `union_white_both_n4_h1` | long | 1 | leftover | list | none | 52% | 37% | 7/19 | -0.04 | 2 | 5 | +6.84 | -3.99 | +11.38 | +6.39 | PASS | 45.341 |
| `union_news_pack_net3_h3` | long | 3 | leftover | list | none | 42% | 32% | 12/19 | +4.53 | 2 | 15 | +5.94 | -7.31 | -8.62 | -2.11 | PASS | 45.123 |
| `union_news_pack_h3` | long | 3 | leftover | list | none | 44% | 32% | 12/19 | +2.25 | 4 | 20 | +5.16 | -8.72 | -10.15 | -6.78 | PASS | 44.339 |
| `short_rsi_ob_h3` | short | 3 | leftover | list | none | 47% | 42% | 10/19 | +1.95 | 18 | 81 | +6.24 | -7.34 | -3.60 | -126.70 | PASS | 44.257 |
| `union_news_or_net3_h3` | long | 3 | leftover | list | none | 42% | 37% | 10/19 | +0.03 | 6 | 37 | +6.24 | -5.59 | -2.51 | -6.43 | PASS | 43.916 |
| `union_white_yday_h1` | long | 1 | leftover | list | none | 52% | 37% | 8/19 | +0.00 | 1 | 3 | +4.30 | -4.15 | +4.08 | +8.14 | PASS | 43.609 |
| `short_last_red_h3` | short | 3 | leftover | list | none | 52% | 32% | 10/19 | +1.54 | 12 | 76 | +3.73 | -6.27 | -5.95 | -26.06 | PASS | 43.31 |
| `flatten_h3_time` | long | 3 | leftover | time | none | 51% | 53% | 6/19 | -1.38 | 10 | 51 | +7.37 | -3.76 | +5.84 | +19.85 | PASS | 42.389 |
| `union_h3` | long | 3 | leftover | list | none | 47% | 63% | 5/19 | -2.95 | 13 | 68 | +8.80 | -4.00 | +3.23 | +19.22 | PASS | 41.994 |
| `union_h3_sizeup` | long | 3 | leftover | list | sizeup | 47% | 63% | 5/19 | -2.95 | 13 | 68 | +8.80 | -4.00 | +3.23 | +19.22 | PASS | 41.994 |
| `union_h3_cut` | long | 3 | leftover | cut_loser | none | 47% | 63% | 5/19 | -1.69 | 13 | 68 | +8.80 | -4.21 | +4.57 | +19.22 | PASS | 41.634 |
| `union_h3_trail` | long | 3 | leftover | trail | none | 47% | 63% | 5/19 | -1.69 | 13 | 68 | +8.80 | -4.21 | +4.57 | +19.22 | PASS | 41.634 |
| `union_h3_sboost` | long | 3 | leftover | list | both | 47% | 63% | 5/19 | -2.95 | 13 | 68 | +8.46 | -4.16 | +3.17 | +19.22 | PASS | 41.139 |
| `combo_hf_5050_shared` | mix | 5 | leftover | list | none | 55% | 53% | 6/19 | -2.39 | 0 | 0 | +6.39 | -4.65 | +6.56 | — | PASS | 41.07 |
| `combo_fse_333_shared` | mix | 5 | leftover | list | none | 54% | 68% | 7/19 | -0.78 | 0 | 0 | +6.80 | -6.85 | +14.06 | — | PASS | 41.053 |
| `union_h3_time` | long | 3 | leftover | time | none | 47% | 63% | 5/19 | -1.15 | 13 | 68 | +8.04 | -4.31 | +5.05 | +19.22 | PASS | 40.577 |
| `flatten_live_h1_topheavy` | long | 1 | topheavy | list | none | 54% | 16% | 7/19 | -1.75 | 1 | 5 | +6.75 | -3.75 | +7.19 | +4.45 | PASS | 40.573 |
| `combo_fes_403030_shared` | mix | 5 | leftover | list | none | 52% | 68% | 7/19 | -1.41 | 0 | 0 | +7.10 | -6.67 | +12.63 | — | PASS | 40.499 |
| `flatten_h1` | long | 1 | leftover | list | none | 47% | 42% | 7/19 | -1.72 | 2 | 20 | +4.44 | -3.72 | +7.05 | +4.76 | PASS | 40.383 |
| `union_h1_topheavy` | long | 1 | topheavy | list | none | 45% | 47% | 7/19 | -1.75 | 4 | 27 | +4.38 | -4.03 | +8.47 | +8.66 | PASS | 40.128 |
| `oppset_h1` | long | 1 | leftover | list | none | 55% | 37% | 8/19 | +0.00 | 10 | 37 | +5.23 | -5.63 | -0.93 | +6.06 | PASS | 40.117 |
| `flatten_h5` | long | 5 | leftover | list | none | 49% | 68% | 6/19 | -3.27 | 18 | 79 | +9.48 | -6.23 | +10.54 | +33.17 | PASS | 39.877 |
| `flatten_h5_cut` | long | 5 | leftover | cut_loser | none | 49% | 68% | 6/19 | -3.27 | 18 | 79 | +9.48 | -6.23 | +10.54 | +33.17 | PASS | 39.877 |
| `flatten_h5_sizeup` | long | 5 | leftover | list | sizeup | 49% | 68% | 6/19 | -3.27 | 18 | 79 | +9.48 | -6.23 | +10.54 | +33.17 | PASS | 39.877 |
| `flatten_h5_time` | long | 5 | leftover | time | none | 49% | 68% | 6/19 | -3.27 | 18 | 79 | +9.48 | -6.23 | +10.54 | +33.17 | PASS | 39.877 |
| `flatten_h5_trail` | long | 5 | leftover | trail | none | 49% | 68% | 6/19 | -3.27 | 18 | 79 | +9.48 | -6.23 | +10.54 | +33.17 | PASS | 39.877 |
| `flatten_h5_sboost` | long | 5 | leftover | list | both | 49% | 68% | 6/19 | -3.27 | 18 | 79 | +9.21 | -5.95 | +8.02 | +33.17 | PASS | 39.639 |
| `flatten_live_h1` | long | 1 | leftover | list | none | 54% | 16% | 7/19 | -1.72 | 1 | 5 | +6.31 | -4.05 | +8.22 | +4.45 | PASS | 39.533 |
| `flatten_live_h1_cut` | long | 1 | leftover | cut_loser | none | 54% | 16% | 7/19 | -1.72 | 1 | 5 | +6.31 | -4.05 | +8.22 | +4.45 | PASS | 39.533 |
| `flatten_live_h1_sboost` | long | 1 | leftover | list | both | 54% | 16% | 7/19 | -1.72 | 1 | 5 | +6.31 | -4.05 | +8.22 | +4.45 | PASS | 39.533 |
| `flatten_live_h1_sizeup` | long | 1 | leftover | list | sizeup | 54% | 16% | 7/19 | -1.72 | 1 | 5 | +6.31 | -4.05 | +8.22 | +4.45 | PASS | 39.533 |
| `flatten_live_h1_time` | long | 1 | leftover | time | none | 54% | 16% | 7/19 | -1.72 | 1 | 5 | +6.31 | -4.05 | +8.22 | +4.45 | PASS | 39.533 |
| `flatten_live_h1_trail` | long | 1 | leftover | trail | none | 54% | 16% | 7/19 | -1.72 | 1 | 5 | +6.31 | -4.05 | +8.22 | +4.45 | PASS | 39.533 |
| `flatten_live_h1_half` | long | 1 | half | list | none | 54% | 16% | 7/19 | -0.93 | 1 | 5 | +6.58 | -3.90 | +3.74 | +4.45 | PASS | 39.481 |
| `flatten_h3` | long | 3 | leftover | list | none | 51% | 53% | 3/19 | -3.02 | 10 | 51 | +8.12 | -3.64 | +2.63 | +19.85 | PASS | 39.32 |
| `flatten_h3_cut` | long | 3 | leftover | cut_loser | none | 51% | 53% | 3/19 | -3.02 | 10 | 51 | +8.12 | -3.64 | +2.63 | +19.85 | PASS | 39.32 |
| `flatten_h3_sizeup` | long | 3 | leftover | list | sizeup | 51% | 53% | 3/19 | -3.02 | 10 | 51 | +8.12 | -3.64 | +2.63 | +19.85 | PASS | 39.32 |
| `flatten_h3_trail` | long | 3 | leftover | trail | none | 51% | 53% | 3/19 | -3.02 | 10 | 51 | +8.12 | -3.64 | +2.63 | +19.85 | PASS | 39.32 |
| `union_hot_score_h1` | long | 1 | leftover | list | none | 50% | 42% | 7/19 | -0.54 | 19 | 40 | +5.84 | -6.38 | +2.35 | +19.48 | PASS | 39.165 |
| `union_hot_score_h3` | long | 3 | leftover | list | none | 49% | 58% | 7/19 | -0.26 | 32 | 94 | +11.38 | -8.58 | +7.89 | +215.47 | PASS | 39.133 |
| `combo_nf_5050_shared` | mix | 5 | leftover | list | none | 50% | 63% | 3/19 | -3.16 | 0 | 0 | +6.91 | -4.27 | +8.14 | — | PASS | 39.098 |
| `union_last_green_h1` | long | 1 | leftover | list | none | 45% | 37% | 7/19 | -1.51 | 6 | 24 | +4.66 | -3.64 | +4.74 | +10.23 | PASS | 39.055 |
| `union_white_both_n4_h3` | long | 3 | leftover | list | none | 45% | 37% | 7/19 | -3.22 | 8 | 19 | +9.42 | -4.22 | +6.31 | +26.06 | PASS | 38.989 |
| `flatten_h3_sboost` | long | 3 | leftover | list | both | 51% | 53% | 3/19 | -3.02 | 10 | 51 | +7.81 | -3.63 | +2.56 | +19.85 | PASS | 38.92 |
| `union_ret_5_h3` | long | 3 | leftover | list | none | 49% | 58% | 8/19 | +0.00 | 26 | 100 | +9.21 | -8.01 | +11.52 | +32.61 | PASS | 38.723 |
| `flatten_h5_s8` | long | 5 | leftover | list | none | 49% | 68% | 6/19 | -1.23 | 18 | 79 | +9.46 | -7.71 | +12.59 | +33.17 | PASS | 38.715 |
| `union_w_hot_candle_h1` | long | 1 | leftover | list | none | 51% | 37% | 7/19 | -1.96 | 15 | 36 | +4.77 | -5.51 | +0.23 | +16.14 | PASS | 38.22 |
| `yday_gainer_h1` | long | 1 | leftover | list | none | 50% | 37% | 7/19 | -0.91 | 7 | 41 | +4.99 | -4.21 | +6.16 | +11.54 | PASS | 38.18 |
| `combo_jf_5050_shared` | mix | 5 | leftover | list | none | 50% | 68% | 2/19 | -1.23 | 0 | 0 | +6.68 | -4.38 | +8.45 | — | PASS | 38.132 |
| `flatten_h3_half` | long | 3 | half | list | none | 51% | 53% | 3/19 | -2.54 | 10 | 51 | +7.14 | -3.54 | +0.88 | +19.85 | PASS | 38.003 |
| `combo_en_5050_shared` | mix | 5 | leftover | list | none | 46% | 47% | 7/19 | -0.13 | 0 | 0 | +5.62 | -6.13 | +21.05 | — | PASS | 37.965 |
| `union_h1_rankw` | long | 1 | rank_w | list | none | 45% | 47% | 6/19 | -3.02 | 4 | 27 | +4.27 | -4.20 | +4.01 | +8.66 | PASS | 37.784 |
| `union_join_present_h3` | long | 3 | leftover | list | none | 44% | 53% | 5/19 | -2.98 | 12 | 70 | +9.71 | -4.63 | +3.35 | +16.25 | PASS | 37.699 |
| `flatten_white_yday_h5` | long | 5 | leftover | list | none | 56% | 47% | 3/19 | -6.19 | 3 | 16 | +10.57 | -4.45 | +6.01 | +14.41 | PASS | 37.613 |
| `union_h1_time` | long | 1 | leftover | time | none | 45% | 42% | 6/19 | -1.72 | 4 | 27 | +4.17 | -3.63 | +4.40 | +8.66 | PASS | 37.466 |
| `union_ret_5_h1` | long | 1 | leftover | list | none | 47% | 42% | 7/19 | -0.31 | 13 | 43 | +5.95 | -5.89 | +2.20 | +7.36 | PASS | 37.32 |
| `union_h1_cut` | long | 1 | leftover | cut_loser | none | 45% | 42% | 6/19 | -1.72 | 4 | 27 | +4.34 | -3.98 | +5.01 | +8.66 | PASS | 37.251 |
| `flatten_live_h1_rankw` | long | 1 | rank_w | list | none | 54% | 16% | 6/19 | -2.65 | 1 | 5 | +6.14 | -4.12 | +4.03 | +4.45 | PASS | 37.245 |
| `combo_eh_5050_shared` | mix | 5 | leftover | list | none | 42% | 42% | 8/19 | +0.00 | 0 | 0 | +7.48 | -6.02 | +17.72 | — | PASS | 37.211 |
| `union_h1` | long | 1 | leftover | list | none | 45% | 42% | 6/19 | -1.72 | 4 | 27 | +4.34 | -4.07 | +5.10 | +8.66 | PASS | 37.151 |
| `union_h1_sizeup` | long | 1 | leftover | list | sizeup | 45% | 42% | 6/19 | -1.72 | 4 | 27 | +4.34 | -4.07 | +5.10 | +8.66 | PASS | 37.151 |
| `union_h1_trail` | long | 1 | leftover | trail | none | 45% | 42% | 6/19 | -1.72 | 4 | 27 | +4.34 | -4.07 | +5.10 | +8.66 | PASS | 37.151 |
| `union_h1_sboost` | long | 1 | leftover | list | both | 45% | 42% | 6/19 | -1.72 | 4 | 27 | +4.30 | -4.17 | +5.16 | +8.66 | PASS | 36.99 |
| `flatten_h3_rankw` | long | 3 | rank_w | list | none | 51% | 47% | 3/19 | -1.87 | 10 | 51 | +7.94 | -3.88 | -0.55 | +19.85 | PASS | 36.861 |
| `combo_en_3070_shared` | mix | 5 | leftover | list | none | 46% | 42% | 7/19 | -0.33 | 0 | 0 | +5.71 | -5.91 | +20.07 | — | PASS | 36.781 |
| `union_w_hot_cond_h1` | long | 1 | leftover | list | none | 48% | 42% | 6/19 | -0.93 | 16 | 34 | +5.33 | -6.40 | -0.85 | +7.44 | PASS | 36.672 |
| `union_hot_n12_h1` | long | 1 | leftover | list | none | 51% | 42% | 6/19 | -2.25 | 25 | 58 | +4.85 | -5.98 | -3.41 | +12.63 | PASS | 36.644 |
| `union_join_g_h1` | long | 1 | leftover | list | none | 49% | 37% | 6/19 | -1.72 | 5 | 31 | +4.12 | -4.45 | +4.05 | +6.12 | PASS | 36.604 |
| `union_h3_exit_alarm` | long | 3 | leftover | list | none | 44% | 58% | 5/19 | -2.66 | 12 | 70 | +7.26 | -4.40 | +3.46 | +14.08 | PASS | 36.532 |
| `combo_fh_7030_shared` | mix | 5 | leftover | list | none | 53% | 63% | 2/19 | -2.96 | 0 | 0 | +6.49 | -4.94 | +3.65 | — | PASS | 36.471 |
| `union_candle_score_h1` | long | 1 | leftover | list | none | 50% | 32% | 6/19 | -1.49 | 11 | 34 | +4.71 | -4.41 | +1.46 | +4.27 | PASS | 36.382 |
| `combo_en_7030_shared` | mix | 5 | leftover | list | none | 40% | 47% | 7/19 | -0.24 | 0 | 0 | +6.30 | -5.76 | +21.20 | — | PASS | 36.295 |
| `union_join_present_h1` | long | 1 | leftover | list | none | 44% | 42% | 6/19 | -1.72 | 3 | 30 | +4.34 | -4.17 | +4.69 | +7.19 | PASS | 36.2 |
| `combo_her_5050_shared` | mix | 5 | leftover | list | none | 43% | 53% | 7/19 | -0.37 | 0 | 0 | +7.04 | -7.87 | +13.97 | — | PASS | 36.154 |
| `flatten_h5_topheavy` | long | 5 | topheavy | list | none | 49% | 58% | 5/19 | -2.25 | 18 | 79 | +9.14 | -5.75 | +5.87 | +33.17 | PASS | 36.101 |
| `short_extended_h3` | short | 3 | leftover | list | none | 53% | 58% | 7/19 | -0.30 | 28 | 88 | +8.76 | -8.63 | -6.10 | -19.31 | PASS | 36.094 |
| `short_rsi_ob_h1` | short | 1 | leftover | list | none | 49% | 42% | 7/19 | -0.51 | 9 | 37 | +4.52 | -5.17 | -7.84 | -11.48 | PASS | 35.906 |
| `combo_eh_3070_shared` | mix | 5 | leftover | list | none | 43% | 37% | 8/19 | +0.00 | 0 | 0 | +7.20 | -6.31 | +16.17 | — | PASS | 35.859 |
| `union_h5_time` | long | 5 | leftover | time | none | 48% | 74% | 4/19 | -3.15 | 23 | 103 | +9.43 | -7.02 | +7.26 | +31.96 | PASS | 35.737 |
| `union_h1_half` | long | 1 | half | list | none | 45% | 37% | 6/19 | -1.02 | 4 | 27 | +4.18 | -4.15 | +1.16 | +8.66 | PASS | 35.21 |
| `flatten_h3_topheavy` | long | 3 | topheavy | list | none | 51% | 47% | 3/19 | -2.00 | 10 | 51 | +6.20 | -3.74 | +1.10 | +19.85 | PASS | 35.193 |
| `union_break10_h1` | long | 1 | leftover | list | none | 49% | 32% | 6/19 | -3.41 | 13 | 28 | +5.19 | -5.53 | -6.25 | +3.84 | PASS | 34.915 |
| `union_w_hot_cond_h3` | long | 3 | leftover | list | none | 47% | 53% | 6/19 | -3.15 | 27 | 90 | +9.40 | -8.01 | +5.39 | +162.15 | PASS | 34.864 |
| `union_h3_rankw` | long | 3 | rank_w | list | none | 47% | 58% | 1/19 | -5.22 | 13 | 68 | +8.61 | -3.96 | -1.42 | +19.22 | PASS | 34.848 |
| `union_h5` | long | 5 | leftover | list | none | 48% | 68% | 4/19 | -3.15 | 23 | 103 | +9.33 | -7.02 | +6.32 | +31.96 | PASS | 34.474 |
| `union_h5_cut` | long | 5 | leftover | cut_loser | none | 48% | 68% | 4/19 | -3.15 | 23 | 103 | +9.33 | -7.02 | +6.32 | +31.96 | PASS | 34.474 |
| `union_h5_sizeup` | long | 5 | leftover | list | sizeup | 48% | 68% | 4/19 | -3.15 | 23 | 103 | +9.33 | -7.02 | +6.32 | +31.96 | PASS | 34.474 |
| `union_h5_trail` | long | 5 | leftover | trail | none | 48% | 68% | 4/19 | -3.15 | 23 | 103 | +9.33 | -7.02 | +6.32 | +31.96 | PASS | 34.474 |
| `union_macd_up_h1` | long | 1 | leftover | list | none | 43% | 32% | 6/19 | -1.81 | 8 | 31 | +4.74 | -3.99 | +1.47 | +10.53 | PASS | 34.196 |
| `flatten_h5_rankw` | long | 5 | rank_w | list | none | 49% | 58% | 4/19 | -2.29 | 18 | 79 | +8.78 | -6.36 | +6.37 | +33.17 | PASS | 33.808 |
| `union_blue_h1` | long | 1 | leftover | list | none | 43% | 32% | 6/19 | -1.72 | 5 | 24 | +4.25 | -4.22 | +1.85 | +1.68 | PASS | 33.754 |
| `union_h3_half` | long | 3 | half | list | none | 47% | 58% | 0/19 | -4.61 | 13 | 68 | +8.37 | -3.75 | -1.92 | +19.22 | PASS | 33.722 |
| `union_vol_g_h1` | long | 1 | leftover | list | none | 50% | 37% | 5/19 | -0.69 | 9 | 35 | +5.64 | -5.60 | -0.69 | +14.44 | PASS | 33.698 |
| `union_h3_topheavy` | long | 3 | topheavy | list | none | 47% | 53% | 2/19 | -2.37 | 13 | 68 | +7.05 | -4.14 | +2.12 | +19.22 | PASS | 33.278 |
| `union_flow_in_h1` | long | 1 | leftover | list | none | 48% | 32% | 2/19 | -4.29 | 3 | 6 | +4.91 | -4.62 | +7.20 | +10.93 | PASS | 33.127 |
| `union_news_present_h1` | long | 1 | leftover | list | none | 42% | 32% | 6/19 | -1.72 | 3 | 29 | +4.26 | -4.04 | +2.47 | +4.65 | PASS | 32.984 |
| `union_last_red_h1` | long | 1 | leftover | list | none | 51% | 37% | 5/19 | -1.54 | 6 | 45 | +3.58 | -4.82 | +0.35 | +16.08 | PASS | 32.777 |
| `union_clk_mom_break_peer_h1` | long | 1 | leftover | list | none | 50% | 26% | 4/19 | -0.63 | 9 | 25 | +3.89 | -3.94 | -1.54 | +4.57 | PASS | 32.548 |
| `union_ab_g_h1` | long | 1 | leftover | list | none | 40% | 26% | 6/19 | -1.72 | 3 | 21 | +4.25 | -3.57 | +3.21 | -2.31 | PASS | 32.314 |
| `combo_fe1_5050_shared` | mix | 5 | leftover | list | none | 50% | 58% | 2/19 | -3.73 | 0 | 0 | +6.02 | -5.75 | +3.30 | — | PASS | 32.245 |
| `union_candle_h1` | long | 1 | leftover | list | none | 42% | 32% | 5/19 | -1.63 | 5 | 31 | +5.29 | -4.34 | +0.38 | +2.51 | PASS | 32.212 |
| `union_earn_react_h3` | long | 3 | leftover | list | none | 38% | 47% | 8/19 | +0.00 | 12 | 64 | +9.50 | -9.12 | +18.62 | -29.25 | PASS | 31.915 |
| `union_h5_sboost` | long | 5 | leftover | list | both | 48% | 68% | 3/19 | -3.15 | 23 | 103 | +8.79 | -8.02 | +5.76 | +31.96 | PASS | 31.909 |
| `union_cond_h1` | long | 1 | leftover | list | none | 48% | 32% | 3/19 | -3.25 | 2 | 21 | +4.33 | -4.19 | -2.70 | +0.74 | PASS | 31.669 |
| `combo_ef_3070_shared` | mix | 5 | leftover | list | none | 49% | 63% | 2/19 | -3.04 | 0 | 0 | +8.79 | -7.10 | +7.41 | — | PASS | 31.562 |
| `combo_ef_5050_shared` | mix | 5 | leftover | list | none | 47% | 63% | 2/19 | -1.33 | 0 | 0 | +8.75 | -6.94 | +10.75 | — | PASS | 31.405 |
| `combo_fe_5050_shared` | mix | 5 | leftover | list | none | 47% | 63% | 2/19 | -1.33 | 0 | 0 | +8.75 | -6.94 | +10.75 | — | PASS | 31.405 |
| `union_macd_up_h3` | long | 3 | leftover | list | none | 40% | 53% | 4/19 | -3.23 | 15 | 78 | +8.04 | -4.69 | -0.82 | +8.25 | PASS | 31.389 |
| `union_h3_exit_news_r` | long | 3 | leftover | list | none | 43% | 53% | 1/19 | -4.22 | 12 | 70 | +9.71 | -4.95 | +2.10 | +12.69 | PASS | 31.308 |
| `union_news_or_net2_h3` | long | 3 | leftover | list | none | 45% | 37% | 7/19 | -2.23 | 7 | 44 | +6.22 | -5.41 | -4.28 | -5.21 | PASS | 31.089 |
| `yday_gainer_h5` | long | 5 | leftover | list | none | 47% | 42% | 7/19 | -0.74 | 20 | 98 | +11.96 | -8.56 | -1.78 | +21.95 | PASS | 30.968 |
| `union_news_vol_h3` | long | 3 | leftover | list | none | 50% | 53% | 4/19 | -1.56 | 7 | 36 | +4.58 | -4.61 | -1.65 | +183.65 | PASS | 30.897 |
| `flatten_h5_half` | long | 5 | half | list | none | 49% | 58% | 1/19 | -3.69 | 18 | 79 | +8.21 | -4.88 | +3.15 | +33.17 | PASS | 30.883 |
| `union_earn_react_h1` | long | 1 | leftover | list | none | 46% | 32% | 7/19 | -1.66 | 5 | 35 | +4.60 | -7.92 | +1.77 | +1.01 | PASS | 30.804 |
| `union_h5_topheavy` | long | 5 | topheavy | list | none | 48% | 63% | 2/19 | -3.38 | 23 | 103 | +9.06 | -6.86 | +3.77 | +31.96 | PASS | 30.364 |
| `union_blue_vol_h1` | long | 1 | leftover | list | none | 46% | 37% | 4/19 | -0.99 | 6 | 29 | +5.93 | -5.68 | -3.40 | +11.03 | PASS | 30.121 |
| `flatten_live_h3` | long | 3 | leftover | list | none | 58% | 16% | 0/19 | -7.72 | 4 | 14 | +9.65 | -3.63 | -4.36 | +6.87 | PASS | 29.955 |
| `short_extended_h1` | short | 1 | leftover | list | none | 54% | 37% | 2/19 | -1.47 | 13 | 45 | +5.81 | -6.32 | -8.23 | -3.35 | PASS | 29.658 |
| `union_last_green_h5` | long | 5 | leftover | list | none | 48% | 63% | 3/19 | -3.52 | 20 | 104 | +8.47 | -8.68 | +7.04 | +26.30 | PASS | 29.629 |
| `union_h5_half` | long | 5 | half | list | none | 48% | 63% | 1/19 | -3.89 | 23 | 103 | +8.29 | -5.84 | +3.33 | +31.96 | PASS | 29.476 |
| `combo_eer_5050_shared` | mix | 5 | leftover | list | none | 36% | 42% | 8/19 | +0.00 | 0 | 0 | +9.22 | -9.07 | +15.36 | — | PASS | 29.45 |
| `union_news_or_net5_h3` | long | 3 | leftover | list | none | 41% | 47% | 7/19 | -2.91 | 4 | 25 | +3.76 | -5.18 | -5.17 | -5.13 | PASS | 29.388 |
| `union_w_hot_candle_h3` | long | 3 | leftover | list | none | 49% | 53% | 2/19 | -3.35 | 25 | 89 | +8.84 | -8.82 | +3.63 | +165.07 | PASS | 29.206 |
| `union_clk_fresh_cat_coil_h1` | long | 1 | leftover | list | none | 48% | 37% | 0/19 | -4.39 | 4 | 21 | +3.59 | -3.38 | -3.75 | -3.30 | PASS | 28.851 |
| `union_h3_exit_red` | long | 3 | leftover | list | none | 52% | 42% | 2/19 | -3.65 | 16 | 68 | +5.93 | -6.58 | +0.76 | +20.19 | PASS | 28.838 |
| `union_h5_rankw` | long | 5 | rank_w | list | none | 48% | 63% | 2/19 | -5.24 | 23 | 103 | +7.91 | -7.45 | +1.75 | +31.96 | PASS | 28.761 |
| `overnight_mega_h2` | long | 2 | leftover | list | none | 57% | 11% | 7/19 | +0.00 | 0 | 11 | +3.54 | -4.53 | +0.50 | +6.08 | PASS | 28.339 |
| `probable_probable_ok_h3` | long | 3 | leftover | list | none | 52% | 32% | 6/19 | -14.46 | 11 | 32 | +5.80 | -8.29 | -17.29 | +7.99 | PASS | 28.32 |
| `union_news_or_h3` | long | 3 | leftover | list | none | 48% | 26% | 6/19 | -2.30 | 12 | 61 | +5.61 | -5.23 | -4.68 | +182.66 | PASS | 28.315 |
| `probable_h1` | long | 1 | leftover | list | none | 48% | 26% | 3/19 | -2.19 | 7 | 40 | +4.02 | -3.85 | -1.58 | +6.12 | PASS | 28.21 |
| `union_break10_h3` | long | 3 | leftover | list | none | 47% | 42% | 4/19 | -4.61 | 19 | 76 | +7.50 | -7.41 | -5.00 | +18.30 | PASS | 28.106 |
| `union_rsi_os_h1` | long | 1 | leftover | list | none | 57% | 37% | 4/19 | -7.61 | 2 | 15 | +2.83 | -6.71 | -13.89 | +12.83 | PASS | 28.06 |
| `union_news_g_cond_h3` | long | 3 | leftover | list | none | 48% | 26% | 6/19 | -2.30 | 12 | 61 | +5.61 | -5.46 | -5.53 | +180.94 | PASS | 27.961 |
| `combo_fer_5050_shared` | mix | 5 | leftover | list | none | 45% | 58% | 2/19 | -1.33 | 0 | 0 | +8.58 | -8.11 | +9.05 | — | PASS | 27.913 |
| `union_last_green_h3` | long | 3 | leftover | list | none | 54% | 42% | 1/19 | -4.33 | 16 | 68 | +5.70 | -7.10 | -0.45 | +27.84 | PASS | 27.911 |
| `union_white_h5` | long | 5 | leftover | list | none | 40% | 68% | 2/19 | -6.05 | 14 | 57 | +9.42 | -6.28 | +4.50 | +39.12 | PASS | 27.68 |
| `union_white_any_h5` | long | 5 | leftover | list | none | 37% | 68% | 1/19 | -6.05 | 13 | 53 | +9.22 | -5.12 | +10.33 | +1.96 | PASS | 27.542 |
| `union_h5_exit_alarm` | long | 5 | leftover | list | none | 47% | 58% | 2/19 | -2.13 | 21 | 105 | +7.64 | -6.99 | +4.66 | +25.75 | PASS | 27.316 |
| `union_blue_coil_h3` | long | 3 | leftover | list | none | 41% | 58% | 0/19 | -4.23 | 9 | 50 | +6.44 | -4.83 | -5.16 | -18.42 | PASS | 27.026 |
| `union_clk_fresh_cat_coil_opp_h1` | long | 1 | leftover | list | none | 46% | 21% | 1/19 | -2.34 | 12 | 20 | +4.76 | -3.96 | -3.99 | -5.82 | PASS | 26.988 |
| `union_white_any_h2` | long | 2 | leftover | list | none | 43% | 47% | 1/19 | -3.26 | 8 | 28 | +6.52 | -6.10 | +1.62 | +8.62 | PASS | 26.97 |
| `yday_gainer_h3` | long | 3 | leftover | list | none | 49% | 42% | 2/19 | -0.06 | 16 | 78 | +9.04 | -6.79 | -4.55 | +31.61 | PASS | 26.738 |
| `union_white_any_h1` | long | 1 | leftover | list | none | 38% | 32% | 3/19 | -4.79 | 1 | 14 | +5.58 | -5.07 | -2.94 | -1.24 | PASS | 26.727 |
| `probable_probable_ok_h1` | long | 1 | leftover | list | none | 45% | 26% | 1/19 | -0.50 | 3 | 13 | +3.57 | -3.39 | -1.44 | -0.50 | PASS | 26.383 |
| `union_candle_score_h3` | long | 3 | leftover | list | none | 47% | 42% | 2/19 | -2.64 | 16 | 85 | +6.99 | -6.01 | +2.03 | +13.79 | PASS | 26.243 |
| `union_white_yday_h3` | long | 3 | leftover | list | none | 47% | 47% | 2/19 | -4.01 | 4 | 33 | +4.39 | -5.20 | +1.35 | +1.76 | PASS | 26.231 |
| `ohlc_hot_h1` | long | 1 | leftover | list | none | 44% | 42% | 1/19 | -3.50 | 15 | 33 | +3.33 | -5.24 | -8.34 | -0.16 | PASS | 26.116 |
| `union_news_present_h3` | long | 3 | leftover | list | none | 41% | 47% | 1/19 | -4.23 | 11 | 64 | +6.76 | -4.64 | -4.69 | +7.24 | PASS | 25.864 |
| `union_clk_nr7_mom_h1` | long | 1 | leftover | list | none | 48% | 26% | 0/19 | -1.06 | 2 | 10 | +5.80 | -3.92 | -0.76 | +19.58 | PASS | 25.503 |
| `union_cond_n4_h3` | long | 3 | leftover | list | none | 45% | 42% | 2/19 | -6.95 | 4 | 36 | +5.89 | -5.88 | +1.24 | -8.50 | PASS | 25.456 |
| `union_clk_hold_vs_sector_h1` | long | 1 | leftover | list | none | 47% | 32% | 1/19 | -3.60 | 4 | 25 | +3.24 | -5.17 | -1.97 | -0.22 | PASS | 25.247 |
| `combo_e1er_5050_shared` | mix | 5 | leftover | list | none | 44% | 21% | 7/19 | -1.13 | 0 | 0 | +4.50 | -8.10 | -2.58 | — | PASS | 25.202 |
| `union_macd_hist_h1` | long | 1 | leftover | list | none | 43% | 37% | 1/19 | -1.36 | 8 | 46 | +3.76 | -3.94 | -5.26 | -6.17 | PASS | 24.774 |
| `union_join_g_h3` | long | 3 | leftover | list | none | 45% | 37% | 0/19 | -5.60 | 13 | 71 | +7.46 | -4.83 | -2.63 | +2.22 | PASS | 24.66 |
| `ohlc_hot_coil_h1` | long | 1 | leftover | list | none | 47% | 26% | 1/19 | -7.25 | 9 | 13 | +3.02 | -4.17 | -19.57 | -7.52 | PASS | 24.561 |
| `union_blue_h3` | long | 3 | leftover | list | none | 42% | 47% | 0/19 | -4.23 | 11 | 56 | +6.85 | -5.25 | -6.87 | -7.31 | PASS | 24.31 |
| `flatten_vol_g_h3` | long | 3 | leftover | list | none | 53% | 21% | 1/19 | -5.10 | 5 | 18 | +9.63 | -5.62 | -11.12 | -6.56 | PASS | 24.25 |
| `union_news_g_h3` | long | 3 | leftover | list | none | 51% | 32% | 2/19 | -3.88 | 14 | 56 | +4.55 | -6.07 | -8.77 | +186.62 | PASS | 24.248 |
| `union_rsi_h1` | long | 1 | leftover | list | none | 45% | 53% | 0/19 | -3.19 | 5 | 57 | +4.72 | -6.98 | -6.37 | -8.48 | PASS | 24.105 |
| `union_vol_ab_h1` | long | 1 | leftover | list | none | 47% | 21% | 0/19 | -3.44 | 6 | 21 | +4.74 | -4.39 | -1.27 | +38.42 | PASS | 24.025 |
| `union_coil_green_h1` | long | 1 | leftover | list | none | 40% | 37% | 0/19 | -3.89 | 10 | 24 | +3.29 | -4.75 | -4.50 | -2.98 | PASS | 23.801 |
| `union_vol_g_h3` | long | 3 | leftover | list | none | 45% | 47% | 2/19 | -3.91 | 16 | 76 | +8.64 | -7.16 | -7.15 | +16.75 | PASS | 23.793 |
| `union_ab_g_h3` | long | 3 | leftover | list | none | 38% | 32% | 1/19 | -4.93 | 9 | 54 | +7.68 | -3.90 | -4.93 | -11.53 | PASS | 23.31 |
| `short_macd_dn_h1` | short | 1 | leftover | list | none | 49% | 21% | 0/19 | -4.48 | 7 | 34 | +3.41 | -3.76 | -7.78 | -7.66 | PASS | 23.154 |
| `probable_h3` | long | 3 | leftover | list | none | 47% | 37% | 3/19 | -0.31 | 16 | 82 | +6.75 | -8.43 | -4.85 | +13.30 | PASS | 23.122 |
| `union_news_g_cam61_h1` | long | 1 | leftover | list | none | 34% | 11% | 5/19 | -1.05 | 1 | 7 | +2.88 | -3.94 | -2.04 | +0.14 | PASS | 22.647 |
| `union_news_g_cam61_h3` | long | 3 | leftover | list | none | 44% | 26% | 4/19 | -1.58 | 2 | 19 | +4.04 | -5.68 | -3.64 | -11.58 | PASS | 22.564 |
| `union_join_vol_green_h3` | long | 3 | leftover | list | none | 47% | 32% | 3/19 | -3.96 | 11 | 42 | +6.07 | -8.08 | -10.40 | +11.97 | PASS | 22.26 |
| `union_blue_coil_h1` | long | 1 | leftover | list | none | 42% | 26% | 0/19 | -4.96 | 3 | 20 | +3.51 | -4.53 | -6.29 | -6.87 | PASS | 22.221 |
| `union_flow_in_h5` | long | 5 | leftover | list | none | 46% | 37% | 2/19 | -3.76 | 5 | 31 | +4.15 | -5.25 | +1.48 | -11.99 | PASS | 22.151 |
| `union_white_any_h3` | long | 3 | leftover | list | none | 40% | 47% | 1/19 | -7.71 | 9 | 45 | +7.36 | -5.98 | +1.88 | -1.05 | PASS | 21.855 |
| `union_blue_vol_h3` | long | 3 | leftover | list | none | 41% | 42% | 1/19 | -3.94 | 12 | 57 | +8.97 | -6.08 | -7.07 | +10.91 | PASS | 21.833 |
| `union_white_h3` | long | 3 | leftover | list | none | 40% | 42% | 2/19 | -7.39 | 10 | 48 | +7.40 | -5.97 | -2.88 | +9.69 | PASS | 21.674 |
| `union_white_coil_h1` | long | 1 | leftover | list | none | 36% | 32% | 0/19 | -5.68 | 2 | 17 | +5.10 | -4.03 | -4.64 | -5.97 | PASS | 21.647 |
| `union_cond_h3` | long | 3 | leftover | list | none | 41% | 47% | 0/19 | -5.58 | 8 | 76 | +6.55 | -6.27 | -2.88 | -11.92 | PASS | 21.631 |
| `union_white_coil_h3` | long | 3 | leftover | list | none | 42% | 42% | 1/19 | -4.55 | 9 | 41 | +6.36 | -6.01 | -0.09 | -5.19 | PASS | 21.506 |
| `union_vol_g_h5` | long | 5 | leftover | list | none | 43% | 37% | 2/19 | -5.32 | 21 | 90 | +14.32 | -8.68 | -9.85 | +9.94 | PASS | 20.97 |
| `union_news_head_h3` | long | 3 | leftover | list | none | 47% | 21% | 2/19 | -7.06 | 13 | 49 | +5.49 | -5.32 | -12.35 | +168.85 | PASS | 20.829 |
| `union_candle_h3` | long | 3 | leftover | list | none | 47% | 32% | 0/19 | -4.72 | 16 | 78 | +6.45 | -6.42 | -4.39 | +6.80 | PASS | 20.474 |
| `ohlc_hot_h3` | long | 3 | leftover | list | none | 46% | 42% | 0/19 | -2.60 | 24 | 90 | +6.20 | -7.16 | -2.40 | +99.68 | PASS | 20.336 |
| `short_macd_dn_h3` | short | 3 | leftover | list | none | 48% | 37% | 0/19 | -2.11 | 16 | 74 | +4.04 | -5.82 | -10.27 | -18.50 | PASS | 20.107 |
| `probable_h5` | long | 5 | leftover | list | none | 46% | 26% | 5/19 | -0.74 | 20 | 102 | +8.72 | -9.74 | -13.68 | +2.04 | PASS | 20.002 |
| `flatten_live_h5` | long | 5 | leftover | list | none | 46% | 26% | 0/19 | -7.72 | 5 | 16 | +9.42 | -6.63 | -3.47 | +7.46 | PASS | 19.965 |
| `overnight_mega_h1` | long | 1 | leftover | list | none | 30% | 16% | 1/19 | +0.00 | 0 | 4 | +5.65 | -4.38 | -0.27 | -10.50 | PASS | 19.572 |
| `union_flow_in_h3` | long | 3 | leftover | list | none | 48% | 26% | 0/19 | -2.50 | 5 | 24 | +3.51 | -6.77 | -1.78 | -11.72 | PASS | 19.037 |
| `union_rsi_h3` | long | 3 | leftover | list | none | 43% | 42% | 1/19 | -8.92 | 13 | 102 | +8.74 | -7.79 | -8.92 | -10.90 | PASS | 18.968 |
| `union_macd_xup_h1` | long | 1 | leftover | list | none | 35% | 32% | 1/19 | -2.69 | 4 | 17 | +2.86 | -5.03 | -12.90 | -13.82 | PASS | 18.928 |
| `union_vol_green_h3` | long | 3 | leftover | list | none | 50% | 21% | 2/19 | -9.64 | 17 | 62 | +6.85 | -8.15 | -15.86 | +8.06 | PASS | 18.905 |
| `union_vol_ab_h3` | long | 3 | leftover | list | none | 42% | 26% | 1/19 | -4.80 | 10 | 49 | +7.40 | -5.24 | -4.80 | +33.05 | PASS | 18.843 |
| `short_last_red_h1` | short | 1 | leftover | list | none | 45% | 16% | 0/19 | -2.68 | 6 | 45 | +3.75 | -4.63 | -9.76 | -15.53 | PASS | 18.367 |
| `union_e_green_h1` | long | 1 | leftover | list | none | 47% | 32% | 0/19 | -10.21 | 2 | 16 | +4.13 | -9.48 | -26.62 | -22.35 | PASS | 18.336 |
| `union_macd_hist_h3` | long | 3 | leftover | list | none | 38% | 37% | 1/19 | -4.85 | 15 | 94 | +5.30 | -4.65 | -1.91 | -26.12 | PASS | 18.204 |
| `union_coil_off_h1` | long | 1 | leftover | list | none | 40% | 21% | 0/19 | -10.70 | 6 | 34 | +3.39 | -4.78 | -12.55 | -8.15 | PASS | 18.0 |
| `short_clk_neg_weak_fail_h3` | short | 3 | leftover | list | none | 46% | 32% | 0/19 | -5.26 | 13 | 72 | +6.37 | -6.12 | -10.86 | -29.39 | PASS | 17.918 |
| `ohlc_hot_h5` | long | 5 | leftover | list | none | 43% | 42% | 0/19 | -4.25 | 34 | 110 | +9.08 | -8.62 | -8.51 | +86.42 | PASS | 17.336 |
| `overnight_h3` | long | 3 | leftover | list | none | 42% | 42% | 1/19 | -9.40 | 13 | 72 | +6.79 | -9.29 | -9.40 | -26.07 | PASS | 17.216 |
| `short_clk_neg_weak_fail_opp_h3` | short | 3 | leftover | list | none | 47% | 42% | 0/19 | -3.78 | 5 | 38 | +5.40 | -5.43 | -21.48 | -42.13 | PASS | 17.099 |
| `union_coil_green_h3` | long | 3 | leftover | list | none | 37% | 32% | 0/19 | -3.74 | 20 | 77 | +6.02 | -6.89 | -0.32 | -14.61 | PASS | 16.637 |
| `overnight_h5` | long | 5 | leftover | list | none | 40% | 42% | 1/19 | -9.00 | 16 | 80 | +12.40 | -12.55 | -10.02 | -31.84 | PASS | 16.191 |
| `union_news_g_h5` | long | 5 | leftover | list | none | 42% | 42% | 1/19 | -0.81 | 15 | 84 | +4.83 | -7.91 | -9.61 | +147.32 | PASS | 16.141 |
| `overnight_h1` | long | 1 | leftover | list | none | 31% | 32% | 1/19 | -5.97 | 1 | 34 | +5.42 | -9.89 | -10.88 | -13.82 | PASS | 14.916 |
| `union_overnight_h3` | long | 3 | leftover | list | none | 43% | 32% | 1/19 | -11.24 | 12 | 67 | +6.31 | -9.49 | -11.24 | -26.94 | PASS | 14.87 |
| `union_overnight_h1` | long | 1 | leftover | list | none | 32% | 26% | 1/19 | -5.97 | 1 | 33 | +5.59 | -9.47 | -14.48 | -17.29 | PASS | 13.706 |
| `union_macd_xup_h3` | long | 3 | leftover | list | none | 39% | 26% | 1/19 | -8.93 | 8 | 40 | +4.62 | -9.36 | -22.00 | -47.03 | PASS | 12.501 |
| `coil_h3_exit_alarm` | long | 3 | leftover | list | none | 35% | 26% | 0/19 | -8.46 | 12 | 79 | +4.37 | -6.43 | -4.74 | -13.72 | PASS | 12.336 |
| `union_coil_off_h3` | long | 3 | leftover | list | none | 36% | 26% | 0/19 | -8.77 | 15 | 78 | +4.47 | -8.24 | -7.71 | -22.38 | PASS | 11.926 |
| `union_rsi_os_h3` | long | 3 | leftover | list | none | 49% | 37% | 2/19 | -15.94 | 2 | 31 | +2.51 | -9.06 | -24.54 | -24.88 | PASS | 11.22 |
| `union_e_green_h3` | long | 3 | leftover | list | none | 38% | 32% | 0/19 | -10.28 | 4 | 41 | +5.45 | -8.76 | -10.28 | -32.54 | PASS | 10.55 |
| `union_coil_off_h5` | long | 5 | leftover | list | none | 37% | 37% | 0/19 | -6.34 | 21 | 104 | +3.94 | -113.61 | -0.84 | -30.02 | PASS | 10.123 |
| `union_rsi_os_h5` | long | 5 | leftover | list | none | 41% | 32% | 1/19 | -10.11 | 2 | 32 | +3.41 | -14.90 | -18.40 | -38.02 | PASS | 5.746 |
| `union_news_both_h1` *(thin)* | long | 1 | leftover | list | none | 100% | 5% | 10/19 | +0.04 | 0 | 0 | +0.04 | — | +0.04 | +0.73 | PASS | 49.215 |
| `union_catal_present_h3` *(thin)* | long | 3 | leftover | list | none | 50% | 16% | 10/19 | +12.16 | 2 | 2 | +12.05 | -2.56 | +12.16 | +15.01 | PASS | 41.473 |
| `union_catal_present_h1` *(thin)* | long | 1 | leftover | list | none | 50% | 11% | 10/19 | +6.91 | 0 | 0 | +8.76 | -2.74 | +6.91 | +7.17 | PASS | 41.3 |
| `union_flow_in_white_h1` *(thin)* | long | 1 | leftover | list | none | 55% | 21% | 14/19 | +0.29 | 0 | 2 | +2.59 | -2.21 | -0.98 | +2.80 | PASS | 36.527 |
| `union_news_missing_h1` *(thin)* | long | 1 | leftover | list | none | 56% | 16% | 11/19 | +0.19 | 0 | 2 | +3.78 | -2.55 | +1.64 | +2.18 | PASS | 35.297 |
| `short_news_pack_h3` *(thin)* | short | 3 | leftover | list | none | 53% | 37% | 12/19 | +2.29 | 2 | 5 | +3.39 | -8.54 | +3.06 | -0.19 | PASS | 32.267 |
| `union_clk_nr7_mom_opp_h1` *(thin)* | long | 1 | leftover | list | none | 75% | 16% | 12/19 | +0.31 | 0 | 4 | +4.03 | -2.38 | +10.45 | +0.01 | PASS | 28.966 |
| `union_clk_earn_guide_react_h1` *(thin)* | long | 1 | leftover | list | none | 50% | 16% | 10/19 | +0.59 | 0 | 3 | +5.17 | -4.79 | -0.12 | +1.48 | PASS | 21.692 |
| `union_news_missing_h3` *(thin)* | long | 3 | leftover | list | none | 56% | 32% | 1/19 | -0.45 | 1 | 7 | +9.22 | -3.20 | +3.13 | +4.96 | PASS | 17.189 |
| `union_vol_missing_h1` *(thin)* | long | 1 | leftover | list | none | 50% | 5% | 1/19 | +0.00 | 0 | 2 | +7.93 | -3.05 | +1.44 | +1.88 | PASS | 10.593 |
| `union_vol_missing_h3` *(thin)* | long | 3 | leftover | list | none | 50% | 16% | 1/19 | +0.00 | 1 | 5 | +11.98 | -3.98 | +3.58 | +5.59 | PASS | 9.385 |
| `union_news_g_cam71_n2_h1` *(thin)* | long | 1 | topheavy | list | none | 35% | 26% | 4/19 | -2.64 | 1 | 1 | +2.34 | -3.55 | -3.05 | -1.08 | PASS | 7.187 |
| `union_news_g_cam91_n1_h1` *(thin)* | long | 1 | leftover | list | none | 50% | 5% | 0/19 | -3.61 | 0 | 0 | +0.85 | -3.84 | -2.85 | -0.09 | PASS | 1.729 |
| `union_clk_flow_coil_h1` *(thin)* | long | 1 | leftover | list | none | 25% | 26% | 0/19 | -7.35 | 0 | 2 | +8.29 | -3.97 | -8.34 | -15.43 | PASS | 1.129 |
| `union_clk_insider_cash_stab_h3` *(thin)* | long | 3 | leftover | list | none | 50% | 0% | 0/19 | -5.54 | 0 | 1 | +4.62 | -5.91 | -5.54 | +2.50 | PASS | -1.921 |
| `union_flow_in_white_h3` *(thin)* | long | 3 | leftover | list | none | 45% | 21% | 1/19 | -2.84 | 0 | 5 | +2.43 | -4.46 | -2.39 | +2.48 | PASS | -3.024 |
| `overnight_mega_green_h1` *(thin)* | long | 1 | leftover | list | none | 33% | 16% | 1/19 | +0.00 | 0 | 2 | +2.51 | -6.48 | -3.38 | -8.49 | PASS | -3.432 |
| `union_rsi_os_macd_h1` *(thin)* | long | 1 | leftover | list | none | 33% | 5% | 1/19 | -5.91 | 1 | 2 | +7.49 | -6.33 | -5.91 | +0.01 | PASS | -7.608 |
| `union_rsi_os_macd_h3` *(thin)* | long | 3 | leftover | list | none | 33% | 11% | 1/19 | -5.42 | 1 | 3 | +11.98 | -5.88 | -5.42 | +7.53 | PASS | -8.885 |
| `short_r_down_h1` *(thin)* | short | 1 | leftover | list | none | — | 0% | 0/19 | +0.00 | 0 | 0 | — | — | +0.00 | +0.00 | PASS | -15.0 |
| `short_r_down_h3` *(thin)* | short | 3 | leftover | list | none | — | 0% | 0/19 | +0.00 | 0 | 0 | — | — | +0.00 | +0.00 | PASS | -15.0 |
| `union_clk_r_up_coil_h1` *(thin)* | long | 1 | leftover | list | none | — | 0% | 0/19 | +0.00 | 0 | 0 | — | — | +0.00 | +0.00 | PASS | -15.0 |
| `union_r_up_h1` *(thin)* | long | 1 | leftover | list | none | — | 0% | 0/19 | +0.00 | 0 | 0 | — | — | +0.00 | +0.00 | PASS | -15.0 |
| `union_r_up_h3` *(thin)* | long | 3 | leftover | list | none | — | 0% | 0/19 | +0.00 | 0 | 0 | — | — | +0.00 | +0.00 | PASS | -15.0 |
| `union_news_both_h3` *(thin)* | long | 3 | leftover | list | none | 0% | 11% | 0/19 | -0.50 | 0 | 1 | — | -0.51 | -0.50 | -0.72 | PASS | -22.969 |
