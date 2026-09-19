# Factor strategy mine — 2026-08-13 → 2026-09-18

Leak-free 09:30 recipes: **270** · candidate rows **1969** · fill `09:30 open, whole shares, Futubull fees, leftover split, sell first, hard-red sit, cash+holdings audit`.

Cash book: $10k, whole shares, Futubull fees, leftover split, sell first, min-hold, 09:30 open, hard-red S≤−3 sit, shorts marked as a liability. Each session starts from leftover cash and lots actually held (butterfly). Cash-start buttons wake a sleeve on date X with $10k and no lots (same rules). Stock investigator quotes 09:30 cameras / coaches / news from repo files. Size / sell / S-boost tweaks sit on the same ledger. Signal-only % is the old equal-weight path (not a fill). `flatten_h*` = wish-list (io/HOLD mornings still buy). `flatten_live_*` = only when the live flatten gate fires. Research only — does not change live `flatten_robust`.

Scoreboard files: slim `factor_mine.json` (stats / recipes / series) plus gzip shards in `factor_mine/shards/` (`books`, `starts`, `daily`, `probe`, `sim`). One file used to be 104MB and GitHub rejected the publish (100MB cap).

Combination books: **77** mixes on the same $10k cash ledger (shared leftover or split sleeves, official 09:30 / 16:00, hard-red sit, owner min-hold). Outperformers: `combo_sh_3070_shared`, `combo_sh_5050_shared`, `combo_ej_5050_shared`, `combo_seh_333_skip`, `combo_es_9010_shared`, `combo_jer_5050_shared`, `combo_e1s_7030_shared`, `combo_sj_3070_shared`, `combo_je1_5050_shared`, `combo_ps_7030_shared`, `combo_sn_3070_shared`, `combo_sf_3070_shared`.

A combo outperforms its members on the cash book when the audit passes and either (1) Book% is strictly higher than every member, or (2) Book% stays within 2pp of the richest member, max drawdown is strictly smaller than every member's drawdown, start-rate is at least the best member, both halves are green, and the worst session is no worse than the worst member session. Effectiveness / Signal% do not decide WIN.

Action blotters: [FACTOR_MINE_ACTION.md](FACTOR_MINE_ACTION.md).

| Strategy | Side | H | Size | Sell | Boost | Win% | $ days | Starts YES | Med start | Top-g | Losers | AvgW | AvgL | Book% | Signal% | Audit | Eff |
|---|---|---:|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---:|
| `combo_sh_3070_shared` | mix | 5 | leftover | list | none | 58% | 69% | 26/26 | +10.28 | 0 | 0 | +5.64 | -4.40 | +42.21 | — | PASS | 77.249 |
| `combo_sh_5050_shared` | mix | 5 | leftover | list | none | 60% | 73% | 23/26 | +6.98 | 0 | 0 | +5.60 | -4.49 | +41.07 | — | PASS | 75.43 |
| `combo_seh_333_skip` | mix | 5 | leftover | list | none | 52% | 73% | 26/26 | +9.42 | 0 | 0 | +6.46 | -6.60 | +36.98 | — | PASS | 72.36 |
| `combo_ej_5050_shared` | mix | 5 | leftover | list | none | 49% | 58% | 25/26 | +6.78 | 0 | 0 | +6.06 | -6.06 | +37.17 | — | PASS | 67.917 |
| `combo_je1_5050_shared` | mix | 5 | leftover | list | none | 51% | 46% | 25/26 | +4.27 | 0 | 0 | +4.69 | -5.04 | +16.62 | — | PASS | 65.488 |
| `combo_es_9010_shared` | mix | 5 | leftover | list | none | 46% | 62% | 25/26 | +10.11 | 0 | 0 | +7.38 | -6.88 | +36.62 | — | PASS | 64.714 |
| `combo_ps_7030_shared` | mix | 5 | leftover | list | none | 62% | 69% | 16/26 | +1.33 | 0 | 0 | +4.77 | -4.39 | +14.48 | — | PASS | 64.597 |
| `combo_jer_5050_shared` | mix | 5 | leftover | list | none | 46% | 54% | 22/26 | +2.52 | 0 | 0 | +6.44 | -6.34 | +30.04 | — | PASS | 61.387 |
| `combo_sj_3070_shared` | mix | 5 | leftover | list | none | 56% | 62% | 11/26 | -0.98 | 0 | 0 | +5.14 | -4.52 | +17.33 | — | PASS | 45.681 |
| `combo_e1s_7030_shared` | mix | 5 | leftover | list | none | 56% | 62% | 11/26 | -0.20 | 0 | 0 | +5.20 | -6.08 | +17.38 | — | PASS | 43.816 |
| `combo_sn_3070_shared` | mix | 5 | leftover | list | none | 55% | 62% | 2/26 | -5.20 | 0 | 0 | +4.54 | -4.53 | +3.80 | — | PASS | 34.74 |
| `combo_sf_3070_shared` | mix | 5 | leftover | list | none | 51% | 54% | 2/26 | -7.80 | 0 | 0 | +7.32 | -6.76 | +3.77 | — | PASS | 27.657 |
| `union_hot_n4_h1` | long | 1 | leftover | list | none | 52% | 50% | 26/26 | +13.27 | 17 | 34 | +5.81 | -4.36 | +32.99 | +54.31 | PASS | 73.286 |
| `combo_sh_7030_shared` | mix | 5 | leftover | list | none | 60% | 77% | 19/26 | +6.56 | 0 | 0 | +5.57 | -4.55 | +31.78 | — | PASS | 71.005 |
| `combo_seh_404020_shared` | mix | 5 | leftover | list | none | 50% | 69% | 26/26 | +5.07 | 0 | 0 | +6.43 | -6.60 | +29.16 | — | PASS | 69.678 |
| `combo_seh_333_shared` | mix | 5 | leftover | list | none | 50% | 69% | 26/26 | +6.41 | 0 | 0 | +6.51 | -6.54 | +30.29 | — | PASS | 69.674 |
| `combo_seh_333_weather` | mix | 5 | leftover | list | none | 50% | 69% | 26/26 | +6.41 | 0 | 0 | +6.51 | -6.54 | +30.29 | — | PASS | 69.674 |
| `combo_he1_5050_shared` | mix | 5 | leftover | list | none | 50% | 50% | 26/26 | +11.21 | 0 | 0 | +5.14 | -5.40 | +25.35 | — | PASS | 68.811 |
| `probable_probable_ok_h1` | long | 1 | leftover | list | none | 55% | 35% | 26/26 | +9.58 | 8 | 22 | +4.07 | -3.24 | +8.51 | +16.14 | PASS | 68.302 |
| `combo_seh_451540_shared` | mix | 5 | leftover | list | none | 49% | 77% | 23/26 | +4.61 | 0 | 0 | +6.55 | -6.66 | +32.34 | — | PASS | 68.298 |
| `combo_seh_333_split` | mix | 5 | leftover | list | none | 50% | 65% | 26/26 | +6.25 | 0 | 0 | +6.50 | -6.38 | +21.84 | — | PASS | 67.758 |
| `union_w_hot_candle_h1` | long | 1 | leftover | list | none | 52% | 46% | 26/26 | +7.11 | 25 | 56 | +4.52 | -5.20 | +9.32 | +30.26 | PASS | 67.357 |
| `union_hot_score_h1` | long | 1 | leftover | list | none | 51% | 50% | 26/26 | +5.95 | 30 | 62 | +5.35 | -6.10 | +9.08 | +32.61 | PASS | 67.337 |
| `combo_ehs_601525_shared` | mix | 5 | leftover | list | none | 46% | 62% | 26/26 | +6.81 | 0 | 0 | +6.87 | -6.17 | +30.91 | — | PASS | 67.317 |
| `union_e_fresh_h3` | long | 3 | leftover | list | none | 48% | 65% | 25/26 | +13.08 | 20 | 84 | +9.20 | -7.47 | +38.47 | +4.57 | PASS | 67.139 |
| `combo_hj_5050_shared` | mix | 5 | leftover | list | none | 49% | 46% | 25/26 | +7.16 | 0 | 0 | +4.99 | -4.13 | +18.90 | — | PASS | 67.134 |
| `union_w_hot_cond_h1` | long | 1 | leftover | list | none | 50% | 50% | 26/26 | +7.79 | 26 | 55 | +5.13 | -5.77 | +7.33 | +23.25 | PASS | 67.132 |
| `combo_ee1_3070_shared` | mix | 5 | leftover | list | none | 40% | 65% | 25/26 | +13.08 | 0 | 0 | +9.20 | -7.47 | +38.47 | — | PASS | 66.843 |
| `combo_ee1_5050_shared` | mix | 5 | leftover | list | none | 40% | 65% | 25/26 | +13.08 | 0 | 0 | +9.20 | -7.47 | +38.47 | — | PASS | 66.843 |
| `combo_ee1_7030_shared` | mix | 5 | leftover | list | none | 40% | 65% | 25/26 | +13.08 | 0 | 0 | +9.20 | -7.47 | +38.47 | — | PASS | 66.843 |
| `combo_seh_403525_shared` | mix | 5 | leftover | list | none | 49% | 69% | 23/26 | +5.42 | 0 | 0 | +6.62 | -6.45 | +29.39 | — | PASS | 66.669 |
| `union_hot_n12_h1` | long | 1 | leftover | list | none | 53% | 46% | 26/26 | +6.54 | 38 | 86 | +4.56 | -5.71 | +4.59 | +26.96 | PASS | 66.416 |
| `combo_hn_7030_shared` | mix | 5 | leftover | list | none | 51% | 46% | 23/26 | +7.63 | 0 | 0 | +4.39 | -4.55 | +23.40 | — | PASS | 66.318 |
| `combo_ehs_702010_shared` | mix | 5 | leftover | list | none | 42% | 58% | 26/26 | +9.81 | 0 | 0 | +7.41 | -5.87 | +33.12 | — | PASS | 65.94 |
| `combo_eh_5050_shared` | mix | 5 | leftover | list | none | 42% | 54% | 26/26 | +12.39 | 0 | 0 | +7.24 | -6.32 | +32.11 | — | PASS | 65.688 |
| `union_vol_green_h1` | long | 1 | leftover | list | none | 47% | 46% | 25/26 | +8.39 | 18 | 50 | +5.70 | -4.89 | +16.21 | +0.56 | PASS | 65.552 |
| `combo_eh_7030_shared` | mix | 5 | leftover | list | none | 38% | 54% | 26/26 | +12.80 | 0 | 0 | +7.96 | -5.98 | +33.56 | — | PASS | 65.112 |
| `combo_hn_5050_shared` | mix | 5 | leftover | list | none | 52% | 46% | 22/26 | +4.14 | 0 | 0 | +4.44 | -4.50 | +18.23 | — | PASS | 65.016 |
| `union_news_pack_net2_h1` | long | 1 | leftover | list | none | 57% | 46% | 17/26 | +8.50 | 1 | 8 | +3.98 | -2.94 | +18.35 | +11.57 | PASS | 65.008 |
| `union_join_vol_green_h1` | long | 1 | leftover | list | none | 47% | 42% | 25/26 | +3.85 | 10 | 39 | +4.93 | -4.06 | +14.67 | +5.67 | PASS | 64.803 |
| `combo_seh_502525_split` | mix | 5 | leftover | list | none | 49% | 69% | 23/26 | +2.77 | 0 | 0 | +6.72 | -6.14 | +16.15 | — | PASS | 64.734 |
| `combo_es_8020_shared` | mix | 5 | leftover | list | none | 48% | 58% | 25/26 | +7.42 | 0 | 0 | +7.00 | -7.24 | +34.06 | — | PASS | 64.124 |
| `combo_en_5050_shared` | mix | 5 | leftover | list | none | 45% | 54% | 25/26 | +4.89 | 0 | 0 | +5.68 | -6.38 | +27.10 | — | PASS | 63.987 |
| `combo_eh_3070_shared` | mix | 5 | leftover | list | none | 43% | 46% | 26/26 | +11.44 | 0 | 0 | +6.96 | -6.60 | +30.25 | — | PASS | 63.754 |
| `union_vol_ab_h1` | long | 1 | leftover | list | none | 53% | 35% | 24/26 | +6.96 | 15 | 41 | +5.28 | -4.63 | +8.60 | +62.52 | PASS | 63.695 |
| `combo_en_7030_shared` | mix | 5 | leftover | list | none | 41% | 54% | 25/26 | +7.70 | 0 | 0 | +6.13 | -6.23 | +31.14 | — | PASS | 63.432 |
| `combo_her_5050_shared` | mix | 5 | leftover | list | none | 43% | 58% | 26/26 | +7.34 | 0 | 0 | +6.93 | -7.80 | +22.02 | — | PASS | 63.358 |
| `union_e_fresh_h1` | long | 1 | leftover | list | none | 48% | 46% | 25/26 | +5.68 | 9 | 43 | +4.85 | -6.19 | +15.43 | +9.13 | PASS | 63.106 |
| `union_ret_5_h1` | long | 1 | leftover | list | none | 48% | 42% | 24/26 | +4.63 | 23 | 66 | +5.93 | -5.38 | +9.66 | +19.20 | PASS | 63.039 |
| `combo_hn_3070_shared` | mix | 5 | leftover | list | none | 52% | 54% | 19/26 | +2.10 | 0 | 0 | +4.47 | -4.46 | +13.12 | — | PASS | 62.976 |
| `combo_seh_601525_shared` | mix | 5 | leftover | list | none | 49% | 77% | 18/26 | +4.17 | 0 | 0 | +6.53 | -6.69 | +27.38 | — | PASS | 62.711 |
| `combo_seh_502525_shared` | mix | 5 | leftover | list | none | 49% | 69% | 19/26 | +3.58 | 0 | 0 | +6.56 | -6.53 | +28.38 | — | PASS | 62.573 |
| `combo_se_5050_split` | mix | 5 | leftover | list | none | 49% | 62% | 24/26 | +5.17 | 0 | 0 | +7.31 | -6.72 | +19.36 | — | PASS | 62.506 |
| `combo_se_3070_shared` | mix | 5 | leftover | list | none | 51% | 54% | 24/26 | +5.03 | 0 | 0 | +6.83 | -7.51 | +29.80 | — | PASS | 62.295 |
| `combo_se_5050_skip` | mix | 5 | leftover | list | none | 60% | 58% | 20/26 | +3.90 | 0 | 0 | +6.44 | -7.95 | +28.95 | — | PASS | 62.189 |
| `union_news_pack_h1` | long | 1 | leftover | list | none | 56% | 46% | 17/26 | +5.23 | 2 | 11 | +3.77 | -3.50 | +14.16 | +10.93 | PASS | 62.174 |
| `union_ret_5_h3` | long | 3 | leftover | list | none | 45% | 54% | 26/26 | +5.62 | 39 | 149 | +9.65 | -8.28 | +18.93 | +58.98 | PASS | 60.81 |
| `union_break10_h1` | long | 1 | leftover | list | none | 49% | 42% | 22/26 | +3.20 | 21 | 51 | +5.17 | -5.23 | +0.20 | +9.46 | PASS | 60.669 |
| `union_hot_score_h3` | long | 3 | leftover | list | none | 45% | 58% | 24/26 | +3.67 | 45 | 145 | +11.81 | -8.74 | +12.82 | +262.00 | PASS | 60.48 |
| `yday_gainer_h3` | long | 3 | leftover | list | none | 51% | 46% | 25/26 | +6.21 | 28 | 120 | +8.56 | -7.14 | +1.40 | +34.82 | PASS | 59.57 |
| `union_last_green_h1` | long | 1 | leftover | list | none | 48% | 35% | 19/26 | +2.90 | 12 | 42 | +4.59 | -3.45 | +7.87 | +16.67 | PASS | 58.924 |
| `union_w_hot_candle_h3` | long | 3 | leftover | list | none | 47% | 54% | 24/26 | +3.00 | 39 | 134 | +8.48 | -8.79 | +10.40 | +232.52 | PASS | 58.813 |
| `union_news_pack_net3_h1` | long | 1 | leftover | list | none | 56% | 38% | 14/26 | +0.57 | 1 | 8 | +3.77 | -2.94 | +9.70 | +6.73 | PASS | 58.035 |
| `combo_ef_7030_shared` | mix | 5 | leftover | list | none | 43% | 65% | 21/26 | +5.16 | 0 | 0 | +8.00 | -7.83 | +20.33 | — | PASS | 57.085 |
| `combo_en_3070_shared` | mix | 5 | leftover | list | none | 44% | 42% | 21/26 | +1.27 | 0 | 0 | +5.71 | -6.24 | +22.02 | — | PASS | 57.001 |
| `union_vol_ab_h3` | long | 3 | leftover | list | none | 48% | 35% | 26/26 | +5.24 | 22 | 88 | +8.35 | -6.98 | +4.31 | +80.43 | PASS | 56.804 |
| `union_w_hot_cond_h3` | long | 3 | leftover | list | none | 43% | 54% | 22/26 | +3.33 | 41 | 142 | +9.76 | -8.20 | +12.02 | +220.35 | PASS | 56.294 |
| `short_last_red_h3` | short | 3 | leftover | list | none | 55% | 42% | 23/26 | +4.06 | 19 | 119 | +5.11 | -5.95 | -1.68 | -13.67 | PASS | 56.229 |
| `union_flow_in_h1` | long | 1 | leftover | list | none | 47% | 38% | 15/26 | +1.87 | 4 | 6 | +4.38 | -4.60 | +9.45 | +15.47 | PASS | 56.104 |
| `probable_h3` | long | 3 | leftover | list | none | 51% | 42% | 25/26 | +6.21 | 26 | 123 | +6.61 | -8.48 | +1.09 | +16.45 | PASS | 55.986 |
| `union_break10_h3` | long | 3 | leftover | list | none | 47% | 46% | 23/26 | +3.10 | 33 | 124 | +8.67 | -7.94 | +1.37 | +42.89 | PASS | 55.702 |
| `combo_nj_5050_shared` | mix | 5 | leftover | list | none | 49% | 35% | 18/26 | +0.91 | 0 | 0 | +4.31 | -4.39 | +4.86 | — | PASS | 55.064 |
| `union_news_or_h1` | long | 1 | leftover | list | none | 52% | 38% | 14/26 | +0.10 | 4 | 31 | +4.13 | -3.86 | +2.01 | +18.15 | PASS | 54.136 |
| `combo_eer_5050_shared` | mix | 5 | leftover | list | none | 38% | 46% | 24/26 | +6.58 | 0 | 0 | +8.48 | -8.87 | +22.73 | — | PASS | 54.121 |
| `union_ab_g_h1` | long | 1 | leftover | list | none | 43% | 35% | 17/26 | +2.27 | 9 | 39 | +4.02 | -3.50 | +5.55 | +3.31 | PASS | 53.158 |
| `union_macd_up_h1` | long | 1 | leftover | list | none | 45% | 38% | 16/26 | +0.97 | 15 | 53 | +4.79 | -4.17 | +2.44 | +13.07 | PASS | 53.078 |
| `ohlc_hot_coil_h1` | long | 1 | leftover | list | none | 53% | 38% | 14/26 | +1.20 | 12 | 24 | +3.31 | -3.82 | -16.07 | -4.32 | PASS | 52.018 |
| `union_vol_g_h3` | long | 3 | leftover | list | none | 48% | 46% | 20/26 | +3.94 | 26 | 118 | +8.85 | -8.28 | -0.76 | +20.03 | PASS | 51.205 |
| `union_join_g_h1` | long | 1 | leftover | list | none | 46% | 38% | 14/26 | +1.30 | 9 | 52 | +4.29 | -4.18 | +5.45 | +7.62 | PASS | 51.171 |
| `union_cond_h1` | long | 1 | leftover | list | none | 48% | 35% | 13/26 | +0.39 | 5 | 43 | +4.13 | -4.10 | -2.40 | +3.59 | PASS | 49.563 |
| `union_vol_green_h3` | long | 3 | leftover | list | none | 50% | 31% | 20/26 | +6.18 | 32 | 104 | +7.08 | -8.05 | -4.82 | +33.65 | PASS | 48.874 |
| `union_join_vol_green_h3` | long | 3 | leftover | list | none | 48% | 38% | 18/26 | +6.08 | 19 | 80 | +7.26 | -7.78 | -1.45 | +31.60 | PASS | 48.477 |
| `short_last_red_h1` | short | 1 | leftover | list | none | 50% | 23% | 17/26 | +2.39 | 12 | 68 | +3.98 | -4.68 | -6.65 | -6.19 | PASS | 48.372 |
| `probable_probable_ok_h3` | long | 3 | leftover | list | none | 56% | 35% | 16/26 | +8.21 | 18 | 57 | +5.60 | -8.07 | -10.58 | +22.92 | PASS | 48.011 |
| `combo_p2s_5050_shared` | mix | 5 | leftover | list | none | 63% | 65% | 8/26 | -0.24 | 0 | 0 | +4.84 | -4.20 | +10.73 | — | PASS | 46.656 |
| `combo_sj_5050_shared` | mix | 5 | leftover | list | none | 56% | 65% | 11/26 | -2.39 | 0 | 0 | +5.13 | -4.53 | +13.15 | — | PASS | 45.801 |
| `combo_ps_5050_shared` | mix | 5 | leftover | list | none | 64% | 65% | 7/26 | -0.90 | 0 | 0 | +4.71 | -4.49 | +10.15 | — | PASS | 45.265 |
| `union_e_green_h3` | long | 3 | leftover | list | none | 42% | 42% | 18/26 | +13.38 | 6 | 46 | +5.47 | -8.76 | +12.39 | -1.64 | PASS | 45.139 |
| `union_earn_react_h3` | long | 3 | leftover | list | none | 42% | 50% | 13/26 | +1.33 | 15 | 83 | +8.88 | -9.12 | +20.11 | -16.65 | PASS | 45.09 |
| `union_e_green_h1` | long | 1 | leftover | list | none | 48% | 38% | 14/26 | +0.35 | 4 | 19 | +4.16 | -9.48 | -19.60 | -13.97 | PASS | 44.947 |
| `union_flow_in_h3` | long | 3 | leftover | list | none | 47% | 38% | 15/26 | +2.48 | 5 | 28 | +1.95 | -4.94 | +1.44 | -3.23 | PASS | 44.389 |
| `yday_gainer_h5` | long | 5 | leftover | list | none | 49% | 46% | 13/26 | +0.79 | 39 | 147 | +8.56 | -9.87 | +2.83 | +18.27 | PASS | 44.016 |
| `union_news_g_conv_h1` | long | 1 | conviction | list | none | 54% | 46% | 11/26 | -3.00 | 1 | 14 | +3.76 | -4.64 | +6.41 | +15.30 | PASS | 43.536 |
| `union_macd_up_h3` | long | 3 | leftover | list | none | 41% | 42% | 13/26 | +0.24 | 26 | 122 | +7.63 | -6.30 | -0.56 | +18.14 | PASS | 43.522 |
| `combo_jse_333_shared` | mix | 5 | leftover | list | none | 56% | 58% | 9/26 | -1.30 | 0 | 0 | +5.76 | -5.89 | +27.05 | — | PASS | 42.83 |
| `combo_e1er_5050_shared` | mix | 5 | leftover | list | none | 44% | 23% | 17/26 | +1.06 | 0 | 0 | +4.79 | -7.83 | -1.55 | — | PASS | 42.708 |
| `combo_sj_7030_shared` | mix | 5 | leftover | list | none | 54% | 58% | 11/26 | -3.57 | 0 | 0 | +5.13 | -4.58 | +7.39 | — | PASS | 42.639 |
| `combo_snj_333_shared` | mix | 5 | leftover | list | none | 52% | 58% | 10/26 | -4.23 | 0 | 0 | +4.60 | -4.63 | +8.19 | — | PASS | 41.831 |
| `union_blue_vol_h3` | long | 3 | leftover | list | none | 41% | 38% | 13/26 | +2.09 | 17 | 90 | +9.92 | -7.51 | -3.96 | +15.00 | PASS | 41.738 |
| `combo_se1_5050_shared` | mix | 5 | leftover | list | none | 56% | 65% | 8/26 | -1.70 | 0 | 0 | +5.10 | -6.25 | +14.72 | — | PASS | 41.468 |
| `combo_hf_5050_shared` | mix | 5 | leftover | list | none | 47% | 54% | 12/26 | -0.71 | 0 | 0 | +6.12 | -5.92 | +7.53 | — | PASS | 39.506 |
| `union_coil_green_h1` | long | 1 | leftover | list | none | 47% | 38% | 11/26 | -0.01 | 13 | 35 | +3.51 | -4.26 | +1.53 | +0.92 | PASS | 38.656 |
| `union_news_g_cond_h1` | long | 1 | leftover | list | none | 52% | 35% | 10/26 | -0.55 | 4 | 32 | +4.13 | -4.13 | +1.01 | +17.51 | PASS | 38.637 |
| `union_news_g_h1` | long | 1 | leftover | list | none | 52% | 38% | 9/26 | -0.88 | 3 | 31 | +3.94 | -4.23 | +2.51 | +16.18 | PASS | 38.606 |
| `combo_ne1_5050_shared` | mix | 5 | leftover | list | none | 51% | 38% | 10/26 | -0.53 | 0 | 0 | +4.21 | -5.28 | +9.80 | — | PASS | 38.332 |
| `yday_gainer_h1` | long | 1 | leftover | list | none | 53% | 38% | 7/26 | -0.74 | 16 | 61 | +5.26 | -4.03 | +5.53 | +16.19 | PASS | 37.707 |
| `flatten_live_h1_topheavy` | long | 1 | topheavy | list | none | 54% | 12% | 7/26 | +0.00 | 1 | 5 | +6.75 | -3.75 | +7.19 | +4.45 | PASS | 37.243 |
| `union_h3_cut` | long | 3 | leftover | cut_loser | none | 46% | 54% | 9/26 | -0.68 | 19 | 108 | +8.80 | -5.62 | +3.78 | +10.67 | PASS | 37.071 |
| `union_h3_trail` | long | 3 | leftover | trail | none | 46% | 54% | 9/26 | -0.68 | 19 | 108 | +8.80 | -5.62 | +3.78 | +10.67 | PASS | 37.071 |
| `union_h3_time` | long | 3 | leftover | time | none | 46% | 54% | 9/26 | -0.68 | 19 | 108 | +8.04 | -5.72 | +4.25 | +10.67 | PASS | 36.346 |
| `flatten_live_h1` | long | 1 | leftover | list | none | 54% | 12% | 7/26 | +0.00 | 1 | 5 | +6.31 | -4.05 | +8.22 | +4.45 | PASS | 36.203 |
| `flatten_live_h1_cut` | long | 1 | leftover | cut_loser | none | 54% | 12% | 7/26 | +0.00 | 1 | 5 | +6.31 | -4.05 | +8.22 | +4.45 | PASS | 36.203 |
| `flatten_live_h1_sboost` | long | 1 | leftover | list | both | 54% | 12% | 7/26 | +0.00 | 1 | 5 | +6.31 | -4.05 | +8.22 | +4.45 | PASS | 36.203 |
| `flatten_live_h1_sizeup` | long | 1 | leftover | list | sizeup | 54% | 12% | 7/26 | +0.00 | 1 | 5 | +6.31 | -4.05 | +8.22 | +4.45 | PASS | 36.203 |
| `flatten_live_h1_time` | long | 1 | leftover | time | none | 54% | 12% | 7/26 | +0.00 | 1 | 5 | +6.31 | -4.05 | +8.22 | +4.45 | PASS | 36.203 |
| `flatten_live_h1_trail` | long | 1 | leftover | trail | none | 54% | 12% | 7/26 | +0.00 | 1 | 5 | +6.31 | -4.05 | +8.22 | +4.45 | PASS | 36.203 |
| `union_candle_score_h1` | long | 1 | leftover | list | none | 51% | 27% | 9/26 | -0.75 | 16 | 52 | +4.43 | -4.14 | +0.49 | +7.29 | PASS | 36.198 |
| `flatten_live_h1_half` | long | 1 | half | list | none | 54% | 12% | 7/26 | +0.00 | 1 | 5 | +6.58 | -3.90 | +3.74 | +4.45 | PASS | 36.151 |
| `combo_ner_5050_shared` | mix | 5 | leftover | list | none | 42% | 50% | 10/26 | -0.95 | 0 | 0 | +5.80 | -6.70 | +19.11 | — | PASS | 35.996 |
| `union_h1_topheavy` | long | 1 | topheavy | list | none | 45% | 46% | 6/26 | -3.59 | 9 | 46 | +4.45 | -3.88 | +4.71 | +7.08 | PASS | 35.546 |
| `combo_sn_5050_shared` | mix | 5 | leftover | list | none | 57% | 65% | 2/26 | -6.75 | 0 | 0 | +4.44 | -4.68 | +0.18 | — | PASS | 35.236 |
| `union_h3_sboost` | long | 3 | leftover | list | both | 46% | 54% | 8/26 | -0.26 | 19 | 108 | +8.46 | -6.00 | +2.81 | +10.67 | PASS | 35.177 |
| `combo_ef_5050_shared` | mix | 5 | leftover | list | none | 43% | 58% | 11/26 | -0.32 | 0 | 0 | +8.23 | -7.73 | +11.13 | — | PASS | 34.944 |
| `combo_fe_5050_shared` | mix | 5 | leftover | list | none | 43% | 58% | 11/26 | -0.32 | 0 | 0 | +8.23 | -7.73 | +11.13 | — | PASS | 34.944 |
| `flatten_live_h1_rankw` | long | 1 | rank_w | list | none | 54% | 12% | 6/26 | +0.00 | 1 | 5 | +6.14 | -4.12 | +4.03 | +4.45 | PASS | 34.27 |
| `union_news_or_net4_conv_h1` | long | 1 | conviction | list | none | 49% | 42% | 5/26 | -9.20 | 1 | 10 | +3.50 | -4.44 | -0.53 | -0.84 | PASS | 34.161 |
| `union_news_head_h1` | long | 1 | leftover | list | none | 52% | 31% | 6/26 | -0.21 | 4 | 23 | +4.12 | -4.18 | -3.96 | +16.42 | PASS | 33.726 |
| `combo_se_5050_shared` | mix | 5 | leftover | list | none | 56% | 54% | 4/26 | -0.50 | 0 | 0 | +6.64 | -7.54 | +21.01 | — | PASS | 33.716 |
| `combo_se_5050_weather` | mix | 5 | leftover | list | none | 56% | 54% | 4/26 | -0.50 | 0 | 0 | +6.64 | -7.54 | +21.01 | — | PASS | 33.716 |
| `union_last_green_h3` | long | 3 | leftover | list | none | 52% | 38% | 10/26 | -1.30 | 25 | 108 | +5.87 | -7.94 | +2.53 | +38.87 | PASS | 33.561 |
| `union_h3` | long | 3 | leftover | list | none | 46% | 54% | 6/26 | -0.68 | 19 | 108 | +8.80 | -5.97 | +2.19 | +10.67 | PASS | 33.497 |
| `union_h3_sizeup` | long | 3 | leftover | list | sizeup | 46% | 54% | 6/26 | -0.68 | 19 | 108 | +8.80 | -5.97 | +2.19 | +10.67 | PASS | 33.497 |
| `flatten_h1` | long | 1 | leftover | list | none | 43% | 38% | 6/26 | -4.91 | 3 | 31 | +4.21 | -3.60 | +2.06 | -3.23 | PASS | 33.115 |
| `combo_jf_5050_shared` | mix | 5 | leftover | list | none | 46% | 62% | 5/26 | -4.27 | 0 | 0 | +5.78 | -5.39 | +4.88 | — | PASS | 33.038 |
| `combo_fh_7030_shared` | mix | 5 | leftover | list | none | 45% | 58% | 6/26 | -2.55 | 0 | 0 | +6.32 | -6.03 | +1.42 | — | PASS | 32.715 |
| `union_candle_h1` | long | 1 | leftover | list | none | 44% | 27% | 8/26 | -0.63 | 8 | 50 | +4.79 | -4.11 | -0.23 | +3.65 | PASS | 32.268 |
| `union_h3_exit_red` | long | 3 | leftover | list | none | 50% | 38% | 9/26 | -1.08 | 25 | 108 | +5.84 | -7.30 | +2.97 | +26.50 | PASS | 32.183 |
| `combo_sn_7030_shared` | mix | 5 | leftover | list | none | 57% | 62% | 0/26 | -7.10 | 0 | 0 | +4.29 | -4.91 | -1.59 | — | PASS | 32.114 |
| `short_news_head_h3` | short | 3 | leftover | list | none | 58% | 62% | 3/26 | -3.92 | 6 | 36 | +6.53 | -6.31 | +1.50 | -1.06 | PASS | 32.108 |
| `union_join_present_h3` | long | 3 | leftover | list | none | 43% | 46% | 6/26 | -0.68 | 17 | 110 | +9.71 | -5.88 | +2.55 | +2.95 | PASS | 31.595 |
| `short_news_or_h3` | short | 3 | leftover | list | none | 58% | 54% | 3/26 | -2.80 | 8 | 41 | +5.42 | -5.57 | +2.96 | +0.90 | PASS | 31.385 |
| `short_news_r_h3` | short | 3 | leftover | list | none | 58% | 54% | 3/26 | -2.80 | 8 | 41 | +5.42 | -5.57 | +2.96 | +0.90 | PASS | 31.385 |
| `union_candle_score_h3` | long | 3 | leftover | list | none | 45% | 42% | 10/26 | -0.56 | 23 | 131 | +6.68 | -6.46 | +3.65 | +20.07 | PASS | 30.934 |
| `probable_h1` | long | 1 | leftover | list | none | 52% | 31% | 3/26 | -1.82 | 15 | 59 | +4.61 | -3.74 | -1.82 | +13.26 | PASS | 30.784 |
| `union_join_g_h3` | long | 3 | leftover | list | none | 43% | 38% | 9/26 | -2.09 | 19 | 107 | +7.45 | -5.87 | -0.28 | +3.44 | PASS | 30.656 |
| `union_h3_exit_news_r` | long | 3 | leftover | list | none | 43% | 46% | 5/26 | -1.45 | 17 | 109 | +9.71 | -5.94 | +1.50 | +0.48 | PASS | 30.492 |
| `ohlc_hot_h1` | long | 1 | leftover | list | none | 46% | 42% | 6/26 | -1.99 | 15 | 52 | +3.24 | -4.78 | -8.73 | -0.16 | PASS | 30.395 |
| `union_h3_exit_alarm` | long | 3 | leftover | list | none | 44% | 50% | 6/26 | -0.44 | 17 | 110 | +6.97 | -5.83 | +2.93 | +3.30 | PASS | 30.334 |
| `combo_nf_5050_shared` | mix | 5 | leftover | list | none | 44% | 58% | 3/26 | -5.30 | 0 | 0 | +6.46 | -5.67 | +2.95 | — | PASS | 30.288 |
| `short_alarm_h3` | short | 3 | leftover | list | none | 51% | 31% | 12/26 | -0.78 | 11 | 80 | +7.29 | -8.49 | +1.73 | -1.12 | PASS | 30.219 |
| `combo_se_7030_shared` | mix | 5 | leftover | list | none | 56% | 50% | 2/26 | -5.50 | 0 | 0 | +6.48 | -7.56 | +14.58 | — | PASS | 30.203 |
| `union_h3_topheavy` | long | 3 | topheavy | list | none | 46% | 46% | 6/26 | -2.09 | 19 | 108 | +7.05 | -6.12 | +0.55 | +10.67 | PASS | 30.096 |
| `union_blue_h3` | long | 3 | leftover | list | none | 44% | 38% | 9/26 | -2.59 | 18 | 93 | +6.85 | -6.31 | -5.91 | -8.90 | PASS | 29.857 |
| `short_news_r_h1` | short | 1 | leftover | list | none | 53% | 42% | 1/26 | -5.20 | 5 | 17 | +2.89 | -3.64 | -5.91 | -8.94 | PASS | 29.705 |
| `union_h3_rankw` | long | 3 | rank_w | list | none | 46% | 50% | 4/26 | -6.43 | 19 | 108 | +8.61 | -5.96 | -5.21 | +10.67 | PASS | 29.542 |
| `union_h3_half` | long | 3 | half | list | none | 46% | 50% | 3/26 | -4.82 | 19 | 108 | +8.37 | -5.36 | -3.96 | +10.67 | PASS | 29.355 |
| `union_earn_react_h1` | long | 1 | leftover | list | none | 48% | 31% | 8/26 | -0.19 | 7 | 47 | +4.79 | -7.61 | +1.59 | +0.99 | PASS | 29.275 |
| `union_coil_off_h1` | long | 1 | leftover | list | none | 42% | 27% | 8/26 | -3.72 | 8 | 51 | +3.86 | -4.04 | -7.28 | -9.78 | PASS | 29.126 |
| `combo_fe1_5050_shared` | mix | 5 | leftover | list | none | 46% | 54% | 4/26 | -4.56 | 0 | 0 | +5.91 | -6.48 | +0.68 | — | PASS | 28.85 |
| `union_h5` | long | 5 | leftover | list | none | 46% | 62% | 6/26 | -13.17 | 31 | 149 | +8.85 | -8.41 | -6.04 | +19.50 | PASS | 28.8 |
| `union_h5_cut` | long | 5 | leftover | cut_loser | none | 46% | 62% | 6/26 | -13.17 | 31 | 149 | +8.85 | -8.41 | -6.04 | +19.50 | PASS | 28.8 |
| `union_h5_sizeup` | long | 5 | leftover | list | sizeup | 46% | 62% | 6/26 | -13.17 | 31 | 149 | +8.85 | -8.41 | -6.04 | +19.50 | PASS | 28.8 |
| `union_h5_trail` | long | 5 | leftover | trail | none | 46% | 62% | 6/26 | -13.17 | 31 | 149 | +8.85 | -8.41 | -6.04 | +19.50 | PASS | 28.8 |
| `union_h5_time` | long | 5 | leftover | time | none | 46% | 62% | 6/26 | -13.17 | 31 | 149 | +8.95 | -8.41 | -6.72 | +19.50 | PASS | 28.765 |
| `combo_nse_333_shared` | mix | 5 | leftover | list | none | 50% | 46% | 1/26 | -3.02 | 0 | 0 | +5.61 | -6.33 | +17.38 | — | PASS | 28.686 |
| `combo_ef_3070_shared` | mix | 5 | leftover | list | none | 45% | 58% | 5/26 | -2.87 | 0 | 0 | +8.20 | -7.92 | +4.45 | — | PASS | 28.682 |
| `union_h1_rankw` | long | 1 | rank_w | list | none | 45% | 46% | 0/26 | -8.36 | 9 | 46 | +4.40 | -3.91 | -4.71 | +7.08 | PASS | 28.263 |
| `union_blue_coil_h3` | long | 3 | leftover | list | none | 46% | 50% | 4/26 | -5.00 | 16 | 84 | +6.07 | -5.91 | -5.87 | -17.45 | PASS | 28.214 |
| `combo_sf_5050_shared` | mix | 5 | leftover | list | none | 53% | 58% | 1/26 | -10.69 | 0 | 0 | +6.99 | -6.61 | +1.31 | — | PASS | 27.864 |
| `short_alarm_h1` | short | 1 | leftover | list | none | 49% | 15% | 3/26 | -0.78 | 7 | 41 | +4.67 | -2.82 | +0.71 | -0.80 | PASS | 27.845 |
| `union_h1` | long | 1 | leftover | list | none | 45% | 42% | 0/26 | -7.70 | 9 | 46 | +4.43 | -3.89 | -2.96 | +7.08 | PASS | 27.812 |
| `union_h1_sizeup` | long | 1 | leftover | list | sizeup | 45% | 42% | 0/26 | -7.70 | 9 | 46 | +4.43 | -3.89 | -2.96 | +7.08 | PASS | 27.812 |
| `union_h1_trail` | long | 1 | leftover | trail | none | 45% | 42% | 0/26 | -7.70 | 9 | 46 | +4.43 | -3.89 | -2.96 | +7.08 | PASS | 27.812 |
| `union_h1_sboost` | long | 1 | leftover | list | both | 45% | 42% | 0/26 | -9.19 | 9 | 46 | +4.47 | -4.05 | -4.51 | +7.08 | PASS | 27.414 |
| `union_h1_time` | long | 1 | leftover | time | none | 45% | 38% | 0/26 | -7.83 | 9 | 46 | +4.43 | -3.67 | -3.72 | +7.08 | PASS | 27.294 |
| `combo_sf_7030_shared` | mix | 5 | leftover | list | none | 53% | 58% | 1/26 | -11.71 | 0 | 0 | +6.57 | -6.78 | +0.26 | — | PASS | 27.256 |
| `union_join_present_h1` | long | 1 | leftover | list | none | 45% | 42% | 0/26 | -7.70 | 7 | 50 | +4.42 | -3.97 | -3.37 | +2.85 | PASS | 27.113 |
| `union_h1_cut` | long | 1 | leftover | cut_loser | none | 45% | 38% | 0/26 | -7.77 | 9 | 46 | +4.43 | -3.84 | -3.16 | +7.08 | PASS | 27.087 |
| `union_macd_hist_h1` | long | 1 | leftover | list | none | 46% | 38% | 3/26 | -3.04 | 9 | 68 | +3.80 | -3.82 | -6.35 | -6.97 | PASS | 26.969 |
| `combo_fse_333_shared` | mix | 5 | leftover | list | none | 48% | 58% | 2/26 | -5.46 | 0 | 0 | +6.72 | -7.55 | +6.70 | — | PASS | 26.946 |
| `union_h1_half` | long | 1 | half | list | none | 45% | 38% | 0/26 | -4.44 | 9 | 46 | +4.34 | -3.94 | -3.32 | +7.08 | PASS | 26.809 |
| `union_last_green_h5` | long | 5 | leftover | list | none | 48% | 54% | 6/26 | -7.53 | 35 | 152 | +7.88 | -10.91 | -3.57 | +59.50 | PASS | 26.661 |
| `combo_fes_403030_shared` | mix | 5 | leftover | list | none | 46% | 58% | 2/26 | -5.19 | 0 | 0 | +6.94 | -7.40 | +5.52 | — | PASS | 26.313 |
| `union_h5_sboost` | long | 5 | leftover | list | both | 46% | 62% | 4/26 | -13.19 | 31 | 149 | +8.39 | -8.87 | -7.29 | +19.50 | PASS | 26.159 |
| `combo_ser_5050_shared` | mix | 5 | leftover | list | none | 52% | 42% | 2/26 | -6.95 | 0 | 0 | +6.76 | -7.83 | +12.61 | — | PASS | 25.991 |
| `union_h5_topheavy` | long | 5 | topheavy | list | none | 46% | 58% | 4/26 | -9.38 | 31 | 149 | +8.63 | -8.59 | -5.28 | +19.50 | PASS | 25.985 |
| `union_h5_half` | long | 5 | half | list | none | 46% | 58% | 3/26 | -5.18 | 31 | 149 | +7.93 | -7.63 | -0.53 | +19.50 | PASS | 25.903 |
| `flatten_h5` | long | 5 | leftover | list | none | 44% | 62% | 2/26 | -5.41 | 19 | 107 | +8.90 | -8.31 | +3.39 | +3.80 | PASS | 25.9 |
| `flatten_h5_cut` | long | 5 | leftover | cut_loser | none | 44% | 62% | 2/26 | -5.41 | 19 | 107 | +8.90 | -8.31 | +3.39 | +3.80 | PASS | 25.9 |
| `flatten_h5_sizeup` | long | 5 | leftover | list | sizeup | 44% | 62% | 2/26 | -5.41 | 19 | 107 | +8.90 | -8.31 | +3.39 | +3.80 | PASS | 25.9 |
| `flatten_h5_time` | long | 5 | leftover | time | none | 44% | 62% | 2/26 | -5.41 | 19 | 107 | +8.90 | -8.31 | +3.39 | +3.80 | PASS | 25.9 |
| `flatten_h5_trail` | long | 5 | leftover | trail | none | 44% | 62% | 2/26 | -5.41 | 19 | 107 | +8.90 | -8.31 | +3.39 | +3.80 | PASS | 25.9 |
| `union_white_h1` | long | 1 | leftover | list | none | 36% | 35% | 4/26 | -4.53 | 3 | 26 | +5.79 | -4.97 | -1.37 | -7.92 | PASS | 25.544 |
| `flatten_h5_sboost` | long | 5 | leftover | list | both | 44% | 62% | 2/26 | -5.41 | 19 | 107 | +8.68 | -8.12 | +0.97 | +3.80 | PASS | 25.517 |
| `flatten_h3_sboost` | long | 3 | leftover | list | both | 45% | 42% | 1/26 | -8.77 | 11 | 74 | +7.81 | -5.30 | -5.58 | +2.48 | PASS | 25.48 |
| `flatten_h3` | long | 3 | leftover | list | none | 45% | 42% | 1/26 | -8.77 | 11 | 74 | +8.12 | -5.53 | -5.53 | +2.48 | PASS | 25.458 |
| `flatten_h3_cut` | long | 3 | leftover | cut_loser | none | 45% | 42% | 1/26 | -8.77 | 11 | 74 | +8.12 | -5.53 | -5.53 | +2.48 | PASS | 25.458 |
| `flatten_h3_sizeup` | long | 3 | leftover | list | sizeup | 45% | 42% | 1/26 | -8.77 | 11 | 74 | +8.12 | -5.53 | -5.53 | +2.48 | PASS | 25.458 |
| `flatten_h3_trail` | long | 3 | leftover | trail | none | 45% | 42% | 1/26 | -8.77 | 11 | 74 | +8.12 | -5.53 | -5.53 | +2.48 | PASS | 25.458 |
| `union_news_vol_h1` | long | 1 | leftover | list | none | 48% | 27% | 0/26 | -2.15 | 5 | 19 | +4.28 | -4.60 | -0.56 | +18.14 | PASS | 25.372 |
| `flatten_h3_half` | long | 3 | half | list | none | 45% | 42% | 1/26 | -4.79 | 11 | 74 | +7.14 | -5.12 | -3.89 | +2.48 | PASS | 25.323 |
| `union_news_or_net4_h1` | long | 1 | leftover | list | none | 45% | 35% | 1/26 | -13.31 | 1 | 18 | +3.92 | -4.26 | -9.74 | -3.36 | PASS | 25.296 |
| `combo_ers_7030_shared` | mix | 5 | leftover | list | none | 47% | 42% | 2/26 | -1.98 | 0 | 0 | +6.95 | -8.08 | +18.27 | — | PASS | 25.137 |
| `union_rsi_os_h3` | long | 3 | leftover | list | none | 47% | 38% | 10/26 | -1.59 | 4 | 41 | +5.61 | -5.61 | -8.16 | -29.19 | PASS | 25.005 |
| `flatten_h3_time` | long | 3 | leftover | time | none | 45% | 42% | 1/26 | -8.04 | 11 | 74 | +7.37 | -5.73 | -2.54 | +2.48 | PASS | 24.996 |
| `union_news_present_h1` | long | 1 | leftover | list | none | 43% | 35% | 0/26 | -7.70 | 7 | 49 | +4.37 | -3.87 | -5.43 | +0.42 | PASS | 24.718 |
| `union_h5_exit_alarm` | long | 5 | leftover | list | none | 46% | 54% | 4/26 | -10.53 | 28 | 150 | +7.34 | -7.68 | -5.02 | +17.74 | PASS | 24.697 |
| `union_h5_rankw` | long | 5 | rank_w | list | none | 46% | 58% | 4/26 | -11.92 | 31 | 149 | +7.50 | -8.84 | -8.94 | +19.50 | PASS | 24.657 |
| `union_news_or_net2_h1` | long | 1 | leftover | list | none | 46% | 27% | 0/26 | -3.02 | 1 | 23 | +4.18 | -4.14 | -3.41 | +0.96 | PASS | 24.484 |
| `union_vol_g_h1` | long | 1 | leftover | list | none | 49% | 35% | 0/26 | -7.54 | 15 | 63 | +5.76 | -5.50 | -8.11 | +6.05 | PASS | 24.472 |
| `union_news_present_h3` | long | 3 | leftover | list | none | 41% | 42% | 4/26 | -1.14 | 16 | 104 | +6.76 | -5.91 | -5.45 | -5.04 | PASS | 24.285 |
| `union_blue_h1` | long | 1 | leftover | list | none | 44% | 31% | 0/26 | -7.93 | 9 | 43 | +4.29 | -4.08 | -6.25 | -7.03 | PASS | 24.145 |
| `flatten_h3_rankw` | long | 3 | rank_w | list | none | 45% | 38% | 1/26 | -7.21 | 11 | 74 | +7.94 | -5.70 | -7.21 | +2.48 | PASS | 24.055 |
| `short_extended_h1` | short | 1 | leftover | list | none | 52% | 31% | 0/26 | -6.17 | 24 | 64 | +5.19 | -6.13 | -13.89 | -10.49 | PASS | 24.035 |
| `flatten_h5_topheavy` | long | 5 | topheavy | list | none | 44% | 54% | 2/26 | -4.38 | 19 | 107 | +8.62 | -8.14 | +1.17 | +3.80 | PASS | 23.965 |
| `short_rsi_ob_h1` | short | 1 | leftover | list | none | 49% | 31% | 0/26 | -5.82 | 16 | 54 | +4.26 | -4.77 | -13.21 | -14.59 | PASS | 23.929 |
| `union_news_g_cam71_h1` | long | 1 | leftover | list | none | 40% | 35% | 1/26 | -10.99 | 1 | 4 | +3.02 | -4.28 | -10.99 | -5.15 | PASS | 23.911 |
| `union_ab_g_h3` | long | 3 | leftover | list | none | 40% | 31% | 5/26 | -5.22 | 16 | 91 | +7.68 | -5.70 | -5.94 | -9.26 | PASS | 23.596 |
| `union_news_pack_h3` | long | 3 | leftover | list | none | 44% | 31% | 6/26 | -3.35 | 5 | 25 | +5.34 | -7.33 | -13.79 | -10.79 | PASS | 23.225 |
| `union_cond_h3` | long | 3 | leftover | list | none | 41% | 46% | 5/26 | -7.24 | 15 | 119 | +6.11 | -7.63 | -6.06 | -12.96 | PASS | 23.121 |
| `flatten_h5_half` | long | 5 | half | list | none | 44% | 54% | 1/26 | -5.05 | 19 | 107 | +7.81 | -7.00 | -0.55 | +3.80 | PASS | 23.026 |
| `union_cond_n4_h3` | long | 3 | leftover | list | none | 43% | 42% | 4/26 | -8.60 | 7 | 57 | +5.54 | -6.84 | -5.09 | -5.10 | PASS | 22.949 |
| `flatten_h3_topheavy` | long | 3 | topheavy | list | none | 45% | 38% | 1/26 | -6.64 | 11 | 74 | +6.20 | -5.72 | -5.37 | +2.48 | PASS | 22.792 |
| `union_last_red_h1` | long | 1 | leftover | list | none | 48% | 35% | 0/26 | -13.56 | 11 | 66 | +3.85 | -4.70 | -13.25 | +2.25 | PASS | 22.662 |
| `flatten_h5_rankw` | long | 5 | rank_w | list | none | 44% | 50% | 2/26 | -5.72 | 19 | 107 | +8.25 | -8.53 | +0.12 | +3.80 | PASS | 22.571 |
| `short_extended_h3` | short | 3 | leftover | list | none | 54% | 46% | 0/26 | -10.49 | 43 | 126 | +8.53 | -9.03 | -16.31 | -38.35 | PASS | 22.546 |
| `short_macd_dn_h3` | short | 3 | leftover | list | none | 48% | 42% | 2/26 | -1.63 | 21 | 108 | +4.27 | -5.71 | -10.55 | -18.69 | PASS | 22.512 |
| `union_blue_coil_h1` | long | 1 | leftover | list | none | 44% | 27% | 0/26 | -8.96 | 8 | 36 | +3.81 | -4.24 | -11.38 | -13.16 | PASS | 22.334 |
| `probable_h5` | long | 5 | leftover | list | none | 49% | 31% | 8/26 | -12.89 | 36 | 150 | +6.75 | -10.63 | -12.89 | -3.78 | PASS | 22.069 |
| `combo_fer_5050_shared` | mix | 5 | leftover | list | none | 41% | 50% | 2/26 | -4.00 | 0 | 0 | +8.17 | -8.52 | +5.96 | — | PASS | 21.915 |
| `union_news_vol_h3` | long | 3 | leftover | list | none | 49% | 46% | 1/26 | -8.21 | 9 | 51 | +4.25 | -5.30 | -9.66 | +192.71 | PASS | 21.738 |
| `union_white_coil_h1` | long | 1 | leftover | list | none | 42% | 23% | 0/26 | -3.89 | 3 | 19 | +4.43 | -3.84 | -7.44 | -4.44 | PASS | 21.462 |
| `union_blue_vol_h1` | long | 1 | leftover | list | none | 45% | 31% | 0/26 | -9.14 | 9 | 50 | +5.46 | -5.32 | -11.54 | -0.41 | PASS | 21.381 |
| `union_candle_h3` | long | 3 | leftover | list | none | 45% | 27% | 4/26 | -2.69 | 23 | 118 | +6.90 | -7.35 | -5.47 | +10.69 | PASS | 21.306 |
| `union_vol_g_h5` | long | 5 | leftover | list | none | 45% | 42% | 4/26 | -4.59 | 34 | 138 | +12.33 | -10.84 | -9.22 | +3.92 | PASS | 21.283 |
| `coil_h3_exit_alarm` | long | 3 | leftover | list | none | 40% | 27% | 8/26 | -4.38 | 18 | 118 | +4.23 | -7.55 | -3.91 | -7.51 | PASS | 20.782 |
| `union_white_h5` | long | 5 | leftover | list | none | 34% | 62% | 3/26 | -9.67 | 16 | 71 | +9.42 | -9.14 | +0.88 | +22.97 | PASS | 20.707 |
| `union_last_red_h3` | long | 3 | leftover | list | none | 41% | 46% | 1/26 | -2.88 | 17 | 121 | +6.50 | -6.56 | -1.25 | -5.83 | PASS | 20.536 |
| `flatten_live_h3` | long | 3 | leftover | list | none | 58% | 12% | 0/26 | -6.24 | 4 | 14 | +8.60 | -8.85 | -6.24 | +6.87 | PASS | 20.399 |
| `short_rsi_ob_h3` | short | 3 | leftover | list | none | 49% | 35% | 1/26 | -5.58 | 32 | 120 | +5.80 | -7.06 | -11.57 | -124.10 | PASS | 19.892 |
| `ohlc_hot_h3` | long | 3 | leftover | list | none | 45% | 42% | 2/26 | -6.51 | 30 | 139 | +5.86 | -7.25 | -6.51 | +81.13 | PASS | 19.44 |
| `union_coil_green_h3` | long | 3 | leftover | list | none | 43% | 23% | 5/26 | -9.74 | 25 | 107 | +3.87 | -7.71 | -11.32 | -6.46 | PASS | 17.869 |
| `union_coil_off_h3` | long | 3 | leftover | list | none | 41% | 27% | 4/26 | -5.24 | 21 | 114 | +4.47 | -8.29 | -8.70 | -9.49 | PASS | 17.029 |
| `union_macd_xup_h1` | long | 1 | leftover | list | none | 39% | 31% | 0/26 | -3.01 | 5 | 35 | +2.76 | -4.62 | -15.17 | -17.79 | PASS | 16.637 |
| `flatten_live_h5` | long | 5 | leftover | list | none | 46% | 27% | 0/26 | -5.51 | 5 | 16 | +9.01 | -11.62 | -5.51 | +5.13 | PASS | 16.556 |
| `ohlc_hot_h5` | long | 5 | leftover | list | none | 44% | 42% | 2/26 | -7.60 | 42 | 166 | +8.16 | -9.81 | -12.37 | +63.80 | PASS | 16.179 |
| `union_news_g_h3` | long | 3 | leftover | list | none | 48% | 27% | 0/26 | -13.05 | 16 | 81 | +4.53 | -6.64 | -20.87 | +170.77 | PASS | 15.982 |
| `union_rsi_os_h1` | long | 1 | leftover | list | none | 49% | 27% | 0/26 | -16.92 | 3 | 23 | +2.48 | -6.29 | -22.54 | -2.98 | PASS | 15.448 |
| `union_white_coil_h3` | long | 3 | leftover | list | none | 41% | 42% | 0/26 | -16.86 | 8 | 43 | +5.20 | -7.71 | -16.25 | -21.73 | PASS | 15.317 |
| `union_news_head_h3` | long | 3 | leftover | list | none | 47% | 19% | 0/26 | -13.66 | 15 | 69 | +5.36 | -6.00 | -22.97 | +176.16 | PASS | 14.435 |
| `union_coil_off_h5` | long | 5 | leftover | list | none | 44% | 35% | 4/26 | -14.58 | 28 | 147 | +3.90 | -57.28 | -10.59 | -18.95 | PASS | 14.346 |
| `union_white_h3` | long | 3 | leftover | list | none | 35% | 35% | 2/26 | -16.81 | 12 | 61 | +7.40 | -8.14 | -15.44 | +0.38 | PASS | 13.35 |
| `union_rsi_h1` | long | 1 | leftover | list | none | 43% | 15% | 0/26 | -18.12 | 10 | 76 | +3.63 | -6.11 | -24.74 | -16.34 | PASS | 13.061 |
| `flatten_vol_g_h3` | long | 3 | leftover | list | none | 46% | 19% | 1/26 | -17.31 | 5 | 28 | +9.63 | -9.84 | -25.48 | -21.05 | PASS | 12.59 |
| `union_news_g_h5` | long | 5 | leftover | list | none | 38% | 38% | 0/26 | -8.60 | 18 | 116 | +5.46 | -8.21 | -15.31 | +96.78 | PASS | 10.949 |
| `union_news_both_h1` *(thin)* | long | 1 | leftover | list | none | 100% | 12% | 21/26 | +7.89 | 0 | 0 | +3.69 | — | +7.92 | +4.91 | PASS | 58.689 |
| `union_flow_in_white_h1` *(thin)* | long | 1 | leftover | list | none | 60% | 15% | 17/26 | +0.05 | 0 | 2 | +2.59 | -2.35 | +0.64 | +4.31 | PASS | 35.016 |
| `short_news_pack_h3` *(thin)* | short | 3 | leftover | list | none | 54% | 35% | 12/26 | +0.00 | 2 | 5 | +3.31 | -2.87 | +5.51 | +5.19 | PASS | 21.203 |
| `union_news_missing_h1` *(thin)* | long | 1 | leftover | list | none | 56% | 12% | 11/26 | +0.00 | 0 | 2 | +3.78 | -2.55 | +1.64 | +2.18 | PASS | 20.552 |
| `union_news_missing_h3` *(thin)* | long | 3 | leftover | list | none | 56% | 23% | 1/26 | +0.00 | 1 | 7 | +9.22 | -3.20 | +3.13 | +4.96 | PASS | 15.136 |
| `union_vol_missing_h1` *(thin)* | long | 1 | leftover | list | none | 50% | 4% | 1/26 | +0.00 | 0 | 2 | +7.93 | -3.05 | +1.44 | +1.88 | PASS | 9.958 |
| `union_vol_missing_h3` *(thin)* | long | 3 | leftover | list | none | 50% | 12% | 1/26 | +0.00 | 1 | 5 | +11.98 | -3.98 | +3.58 | +5.59 | PASS | 8.183 |
| `union_news_g_cam91_n1_h1` *(thin)* | long | 1 | leftover | list | none | 50% | 4% | 0/26 | -2.85 | 0 | 0 | +0.85 | -3.84 | -2.85 | -0.09 | PASS | 1.447 |
| `union_catal_present_h1` *(thin)* | long | 1 | leftover | list | none | 38% | 12% | 0/26 | -5.14 | 0 | 1 | +0.56 | -2.82 | -5.14 | +2.50 | PASS | -4.978 |
| `union_catal_present_h3` *(thin)* | long | 3 | leftover | list | none | 25% | 15% | 0/26 | -10.75 | 1 | 3 | +4.67 | -4.28 | -10.75 | -7.21 | PASS | -8.702 |
| `union_rsi_os_macd_h1` *(thin)* | long | 1 | leftover | list | none | 29% | 8% | 1/26 | -12.12 | 1 | 4 | +4.44 | -7.11 | -12.12 | -6.47 | PASS | -14.049 |
| `short_r_down_h1` *(thin)* | short | 1 | leftover | list | none | — | 0% | 0/26 | +0.00 | 0 | 0 | — | — | +0.00 | +0.00 | PASS | -15.0 |
| `short_r_down_h3` *(thin)* | short | 3 | leftover | list | none | — | 0% | 0/26 | +0.00 | 0 | 0 | — | — | +0.00 | +0.00 | PASS | -15.0 |
| `union_r_up_h1` *(thin)* | long | 1 | leftover | list | none | — | 0% | 0/26 | +0.00 | 0 | 0 | — | — | +0.00 | +0.00 | PASS | -15.0 |
| `union_r_up_h3` *(thin)* | long | 3 | leftover | list | none | — | 0% | 0/26 | +0.00 | 0 | 0 | — | — | +0.00 | +0.00 | PASS | -15.0 |
