# Factor strategy mine — 2026-08-13 → 2026-09-14

Leak-free 09:30 recipes: **270** · candidate rows **1710** · fill `09:30 open, whole shares, Futubull fees, leftover split, sell first, hard-red sit, cash+holdings audit`.

Cash book: $10k, whole shares, Futubull fees, leftover split, sell first, min-hold, 09:30 open, hard-red S≤−3 sit, shorts marked as a liability. Each session starts from leftover cash and lots actually held (butterfly). Cash-start buttons wake a sleeve on date X with $10k and no lots (same rules). Stock investigator quotes 09:30 cameras / coaches / news from repo files. Size / sell / S-boost tweaks sit on the same ledger. Signal-only % is the old equal-weight path (not a fill). `flatten_h*` = wish-list (io/HOLD mornings still buy). `flatten_live_*` = only when the live flatten gate fires. Research only — does not change live `flatten_robust`.

Combination books: **77** mixes on the same $10k cash ledger (shared leftover or split sleeves, official 09:30 / 16:00, hard-red sit, owner min-hold). Outperformers: `combo_sh_5050_shared`, `combo_sh_3070_shared`, `combo_jse_333_shared`, `combo_sh_7030_shared`, `combo_seh_451540_shared`, `combo_se_5050_skip`, `combo_seh_601525_shared`, `combo_seh_502525_shared`, `combo_seh_333_skip`, `combo_p2s_5050_shared`, `combo_ps_7030_shared`, `combo_se_5050_shared`, `combo_se_5050_weather`, `combo_ps_5050_shared`, `combo_es_8020_shared`, `combo_se_3070_shared`, `combo_seh_403525_shared`, `combo_seh_404020_shared`, `combo_ej_5050_shared`, `combo_es_9010_shared`, `combo_se1_5050_shared`, `combo_seh_333_shared`, `combo_seh_333_weather`, `combo_ser_5050_shared`, `combo_jer_5050_shared`, `combo_ers_7030_shared`, `combo_sj_5050_shared`, `combo_e1s_7030_shared`, `combo_sj_3070_shared`, `combo_sj_7030_shared`, `combo_snj_333_shared`, `combo_sn_5050_shared`, `combo_je1_5050_shared`, `combo_nf_5050_shared`.

A combo outperforms its members on the cash book when the audit passes and either (1) Book% is strictly higher than every member, or (2) Book% stays within 2pp of the richest member, max drawdown is strictly smaller than every member's drawdown, start-rate is at least the best member, both halves are green, and the worst session is no worse than the worst member session. Effectiveness / Signal% do not decide WIN.

Action blotters: [FACTOR_MINE_ACTION.md](FACTOR_MINE_ACTION.md).

| Strategy | Side | H | Size | Sell | Boost | Win% | $ days | Starts YES | Med start | Top-g | Losers | AvgW | AvgL | Book% | Signal% | Audit | Eff |
|---|---|---:|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---:|
| `combo_p2s_5050_shared` | mix | 5 | leftover | list | none | 68% | 77% | 21/22 | +18.55 | 0 | 0 | +4.75 | -3.42 | +31.42 | — | PASS | 81.565 |
| `combo_ps_5050_shared` | mix | 5 | leftover | list | none | 69% | 77% | 21/22 | +17.44 | 0 | 0 | +4.63 | -3.78 | +30.56 | — | PASS | 80.787 |
| `combo_ps_7030_shared` | mix | 5 | leftover | list | none | 66% | 77% | 21/22 | +17.47 | 0 | 0 | +4.69 | -3.72 | +31.28 | — | PASS | 80.036 |
| `combo_sh_5050_shared` | mix | 5 | leftover | list | none | 61% | 73% | 21/22 | +10.69 | 0 | 0 | +5.41 | -3.90 | +40.10 | — | PASS | 78.717 |
| `combo_sh_7030_shared` | mix | 5 | leftover | list | none | 61% | 77% | 21/22 | +10.80 | 0 | 0 | +5.38 | -3.96 | +33.90 | — | PASS | 78.566 |
| `combo_sh_3070_shared` | mix | 5 | leftover | list | none | 60% | 68% | 21/22 | +6.40 | 0 | 0 | +5.45 | -3.81 | +35.38 | — | PASS | 76.808 |
| `combo_se1_5050_shared` | mix | 5 | leftover | list | none | 58% | 82% | 21/22 | +10.43 | 0 | 0 | +4.98 | -6.24 | +28.74 | — | PASS | 73.469 |
| `combo_seh_601525_shared` | mix | 5 | leftover | list | none | 48% | 82% | 21/22 | +8.36 | 0 | 0 | +6.39 | -6.47 | +32.08 | — | PASS | 70.721 |
| `combo_e1s_7030_shared` | mix | 5 | leftover | list | none | 57% | 77% | 20/22 | +7.02 | 0 | 0 | +5.08 | -6.04 | +23.58 | — | PASS | 70.465 |
| `combo_jse_333_shared` | mix | 5 | leftover | list | none | 56% | 64% | 21/22 | +4.59 | 0 | 0 | +5.80 | -5.81 | +34.15 | — | PASS | 70.283 |
| `combo_seh_333_skip` | mix | 5 | leftover | list | none | 52% | 73% | 21/22 | +5.06 | 0 | 0 | +6.25 | -6.41 | +31.57 | — | PASS | 70.212 |
| `combo_seh_451540_shared` | mix | 5 | leftover | list | none | 48% | 77% | 21/22 | +7.79 | 0 | 0 | +6.41 | -6.44 | +33.74 | — | PASS | 70.089 |
| `combo_seh_502525_shared` | mix | 5 | leftover | list | none | 49% | 73% | 21/22 | +7.79 | 0 | 0 | +6.39 | -6.33 | +31.92 | — | PASS | 69.155 |
| `combo_sj_5050_shared` | mix | 5 | leftover | list | none | 56% | 77% | 17/22 | +8.08 | 0 | 0 | +5.09 | -4.40 | +25.24 | — | PASS | 68.988 |
| `combo_se_5050_skip` | mix | 5 | leftover | list | none | 61% | 64% | 21/22 | +7.58 | 0 | 0 | +6.18 | -7.82 | +32.80 | — | PASS | 68.824 |
| `combo_seh_404020_shared` | mix | 5 | leftover | list | none | 50% | 68% | 21/22 | +5.96 | 0 | 0 | +6.21 | -6.42 | +30.12 | — | PASS | 68.233 |
| `combo_seh_403525_shared` | mix | 5 | leftover | list | none | 49% | 68% | 21/22 | +6.62 | 0 | 0 | +6.42 | -6.26 | +30.14 | — | PASS | 68.058 |
| `combo_seh_333_shared` | mix | 5 | leftover | list | none | 49% | 68% | 21/22 | +5.06 | 0 | 0 | +6.30 | -6.36 | +28.70 | — | PASS | 67.828 |
| `combo_seh_333_weather` | mix | 5 | leftover | list | none | 49% | 68% | 21/22 | +5.06 | 0 | 0 | +6.30 | -6.36 | +28.70 | — | PASS | 67.828 |
| `combo_sn_5050_shared` | mix | 5 | leftover | list | none | 59% | 73% | 17/22 | +8.48 | 0 | 0 | +4.42 | -4.54 | +16.66 | — | PASS | 67.79 |
| `combo_sj_3070_shared` | mix | 5 | leftover | list | none | 56% | 73% | 17/22 | +3.91 | 0 | 0 | +5.08 | -4.39 | +22.93 | — | PASS | 67.727 |
| `combo_se_5050_shared` | mix | 5 | leftover | list | none | 57% | 64% | 21/22 | +7.58 | 0 | 0 | +6.39 | -7.35 | +30.82 | — | PASS | 67.186 |
| `combo_se_5050_weather` | mix | 5 | leftover | list | none | 57% | 64% | 21/22 | +7.58 | 0 | 0 | +6.39 | -7.35 | +30.82 | — | PASS | 67.186 |
| `combo_sj_7030_shared` | mix | 5 | leftover | list | none | 54% | 68% | 17/22 | +8.35 | 0 | 0 | +5.13 | -4.42 | +20.55 | — | PASS | 65.677 |
| `combo_snj_333_shared` | mix | 5 | leftover | list | none | 52% | 64% | 17/22 | +4.79 | 0 | 0 | +4.60 | -4.60 | +17.52 | — | PASS | 64.388 |
| `combo_se_3070_shared` | mix | 5 | leftover | list | none | 51% | 59% | 21/22 | +7.29 | 0 | 0 | +6.60 | -7.37 | +30.50 | — | PASS | 63.891 |
| `combo_es_8020_shared` | mix | 5 | leftover | list | none | 48% | 59% | 21/22 | +5.78 | 0 | 0 | +6.78 | -7.06 | +30.54 | — | PASS | 63.276 |
| `combo_ser_5050_shared` | mix | 5 | leftover | list | none | 53% | 50% | 21/22 | +5.97 | 0 | 0 | +6.48 | -7.68 | +28.26 | — | PASS | 62.269 |
| `combo_ej_5050_shared` | mix | 5 | leftover | list | none | 47% | 50% | 19/22 | +2.01 | 0 | 0 | +6.22 | -6.21 | +30.11 | — | PASS | 61.772 |
| `combo_es_9010_shared` | mix | 5 | leftover | list | none | 45% | 59% | 20/22 | +5.99 | 0 | 0 | +7.20 | -6.64 | +29.22 | — | PASS | 61.316 |
| `combo_ers_7030_shared` | mix | 5 | leftover | list | none | 48% | 50% | 20/22 | +5.75 | 0 | 0 | +6.69 | -8.01 | +27.58 | — | PASS | 58.994 |
| `combo_jer_5050_shared` | mix | 5 | leftover | list | none | 43% | 50% | 17/22 | +1.30 | 0 | 0 | +6.49 | -6.46 | +28.20 | — | PASS | 57.507 |
| `combo_je1_5050_shared` | mix | 5 | leftover | list | none | 50% | 45% | 14/22 | +0.73 | 0 | 0 | +4.73 | -5.28 | +12.61 | — | PASS | 56.009 |
| `combo_nf_5050_shared` | mix | 5 | leftover | list | none | 45% | 59% | 2/22 | -6.16 | 0 | 0 | +6.77 | -5.89 | +3.23 | — | PASS | 30.994 |
| `union_news_pack_net2_h1` | long | 1 | leftover | list | none | 57% | 50% | 21/22 | +19.69 | 1 | 6 | +3.98 | -2.68 | +28.55 | +21.21 | PASS | 75.91 |
| `union_news_pack_h1` | long | 1 | leftover | list | none | 57% | 50% | 21/22 | +15.21 | 2 | 9 | +3.77 | -3.37 | +24.09 | +20.95 | PASS | 72.885 |
| `short_news_or_h3` | short | 3 | leftover | list | none | 63% | 64% | 21/22 | +9.75 | 6 | 38 | +5.30 | -4.34 | +16.22 | +26.34 | PASS | 69.899 |
| `short_news_r_h3` | short | 3 | leftover | list | none | 63% | 64% | 21/22 | +9.75 | 6 | 38 | +5.30 | -4.34 | +16.22 | +26.34 | PASS | 69.899 |
| `union_hot_n4_h1` | long | 1 | leftover | list | none | 53% | 45% | 19/22 | +4.06 | 13 | 26 | +5.56 | -4.16 | +22.17 | +44.88 | PASS | 68.354 |
| `combo_seh_502525_split` | mix | 5 | leftover | list | none | 48% | 73% | 21/22 | +5.17 | 0 | 0 | +6.58 | -5.87 | +17.69 | — | PASS | 67.38 |
| `union_news_pack_net3_h1` | long | 1 | leftover | list | none | 56% | 41% | 17/22 | +11.03 | 1 | 6 | +3.77 | -2.68 | +19.12 | +16.37 | PASS | 67.281 |
| `combo_sn_7030_shared` | mix | 5 | leftover | list | none | 59% | 73% | 17/22 | +8.49 | 0 | 0 | +4.27 | -4.74 | +14.96 | — | PASS | 67.18 |
| `combo_se_7030_shared` | mix | 5 | leftover | list | none | 58% | 59% | 21/22 | +4.76 | 0 | 0 | +6.27 | -7.32 | +26.82 | — | PASS | 65.92 |
| `combo_seh_333_split` | mix | 5 | leftover | list | none | 50% | 64% | 21/22 | +4.36 | 0 | 0 | +6.29 | -6.15 | +19.30 | — | PASS | 65.813 |
| `short_news_head_h3` | short | 3 | leftover | list | none | 65% | 68% | 17/22 | +7.64 | 5 | 32 | +6.43 | -4.82 | +13.78 | +24.01 | PASS | 65.751 |
| `combo_sn_3070_shared` | mix | 5 | leftover | list | none | 58% | 68% | 16/22 | +4.39 | 0 | 0 | +4.52 | -4.37 | +14.02 | — | PASS | 65.048 |
| `combo_hn_5050_shared` | mix | 5 | leftover | list | none | 53% | 45% | 19/22 | +0.29 | 0 | 0 | +4.33 | -4.51 | +13.78 | — | PASS | 65.025 |
| `combo_hn_7030_shared` | mix | 5 | leftover | list | none | 52% | 45% | 19/22 | +1.97 | 0 | 0 | +4.28 | -4.53 | +16.68 | — | PASS | 65.021 |
| `combo_ehs_601525_shared` | mix | 5 | leftover | list | none | 46% | 59% | 21/22 | +6.02 | 0 | 0 | +6.58 | -6.06 | +27.66 | — | PASS | 64.937 |
| `combo_se_5050_split` | mix | 5 | leftover | list | none | 49% | 68% | 21/22 | +6.76 | 0 | 0 | +7.15 | -6.43 | +20.68 | — | PASS | 64.562 |
| `short_news_r_h1` | short | 1 | leftover | list | none | 52% | 41% | 21/22 | +2.26 | 4 | 17 | +3.02 | -3.22 | +1.53 | +6.05 | PASS | 63.473 |
| `combo_nse_333_shared` | mix | 5 | leftover | list | none | 50% | 55% | 20/22 | +4.94 | 0 | 0 | +5.47 | -6.28 | +26.40 | — | PASS | 63.364 |
| `combo_he1_5050_shared` | mix | 5 | leftover | list | none | 50% | 45% | 19/22 | +2.17 | 0 | 0 | +4.95 | -5.49 | +15.02 | — | PASS | 62.824 |
| `combo_ehs_702010_shared` | mix | 5 | leftover | list | none | 40% | 55% | 21/22 | +5.44 | 0 | 0 | +7.29 | -5.62 | +25.44 | — | PASS | 62.529 |
| `union_e_fresh_h3` | long | 3 | leftover | list | none | 47% | 59% | 20/22 | +5.56 | 16 | 80 | +9.39 | -7.45 | +27.67 | -11.54 | PASS | 62.11 |
| `combo_ee1_3070_shared` | mix | 5 | leftover | list | none | 36% | 59% | 20/22 | +5.56 | 0 | 0 | +9.39 | -7.45 | +27.67 | — | PASS | 60.827 |
| `combo_ee1_5050_shared` | mix | 5 | leftover | list | none | 36% | 59% | 20/22 | +5.56 | 0 | 0 | +9.39 | -7.45 | +27.67 | — | PASS | 60.827 |
| `combo_ee1_7030_shared` | mix | 5 | leftover | list | none | 36% | 59% | 20/22 | +5.56 | 0 | 0 | +9.39 | -7.45 | +27.67 | — | PASS | 60.827 |
| `union_ret_5_h3` | long | 3 | leftover | list | none | 47% | 59% | 20/22 | +4.64 | 31 | 120 | +9.90 | -8.69 | +16.91 | +45.10 | PASS | 60.647 |
| `combo_eh_7030_shared` | mix | 5 | leftover | list | none | 35% | 50% | 20/22 | +5.10 | 0 | 0 | +7.94 | -5.89 | +23.27 | — | PASS | 59.456 |
| `combo_eh_5050_shared` | mix | 5 | leftover | list | none | 40% | 50% | 19/22 | +4.18 | 0 | 0 | +7.08 | -6.26 | +21.91 | — | PASS | 58.994 |
| `union_hot_score_h3` | long | 3 | leftover | list | none | 45% | 59% | 18/22 | +2.62 | 37 | 116 | +12.32 | -9.09 | +11.21 | +225.25 | PASS | 58.885 |
| `combo_her_5050_shared` | mix | 5 | leftover | list | none | 42% | 55% | 19/22 | +3.40 | 0 | 0 | +6.68 | -7.87 | +16.95 | — | PASS | 58.099 |
| `combo_hj_5050_shared` | mix | 5 | leftover | list | none | 47% | 41% | 15/22 | +1.45 | 0 | 0 | +4.82 | -4.15 | +12.03 | — | PASS | 57.498 |
| `union_news_or_h1` | long | 1 | leftover | list | none | 52% | 41% | 14/22 | +0.96 | 4 | 26 | +4.04 | -3.90 | +2.64 | +14.08 | PASS | 57.043 |
| `union_e_fresh_h1` | long | 1 | leftover | list | none | 48% | 45% | 17/22 | +0.62 | 7 | 40 | +4.74 | -6.44 | +9.55 | +9.20 | PASS | 56.953 |
| `union_w_hot_cond_h1` | long | 1 | leftover | list | none | 48% | 45% | 15/22 | +0.93 | 20 | 43 | +5.09 | -5.78 | +0.93 | +14.26 | PASS | 56.812 |
| `combo_eh_3070_shared` | mix | 5 | leftover | list | none | 41% | 41% | 19/22 | +3.98 | 0 | 0 | +6.80 | -6.55 | +19.86 | — | PASS | 56.806 |
| `union_news_g_cond_h1` | long | 1 | leftover | list | none | 51% | 36% | 14/22 | +0.20 | 4 | 27 | +4.04 | -4.22 | +2.33 | +13.46 | PASS | 55.254 |
| `union_vol_green_h1` | long | 1 | leftover | list | none | 45% | 41% | 15/22 | +2.34 | 14 | 37 | +4.90 | -5.10 | +8.58 | -2.22 | PASS | 55.103 |
| `combo_sf_7030_shared` | mix | 5 | leftover | list | none | 57% | 64% | 11/22 | +0.28 | 0 | 0 | +6.61 | -6.79 | +15.46 | — | PASS | 54.539 |
| `combo_ef_7030_shared` | mix | 5 | leftover | list | none | 42% | 64% | 16/22 | +1.33 | 0 | 0 | +8.07 | -8.22 | +15.88 | — | PASS | 53.957 |
| `union_earn_react_h3` | long | 3 | leftover | list | none | 41% | 50% | 19/22 | +2.86 | 13 | 78 | +8.74 | -9.31 | +21.87 | -22.33 | PASS | 53.763 |
| `combo_en_7030_shared` | mix | 5 | leftover | list | none | 39% | 45% | 16/22 | +1.52 | 0 | 0 | +6.03 | -6.22 | +23.33 | — | PASS | 53.737 |
| `union_w_hot_candle_h1` | long | 1 | leftover | list | none | 53% | 41% | 11/22 | +1.78 | 20 | 43 | +4.48 | -5.36 | +1.84 | +23.81 | PASS | 53.32 |
| `union_hot_score_h1` | long | 1 | leftover | list | none | 51% | 45% | 11/22 | +0.82 | 24 | 49 | +5.56 | -6.31 | +1.21 | +24.96 | PASS | 53.111 |
| `union_news_pack_h3` | long | 3 | leftover | list | none | 43% | 36% | 19/22 | +9.62 | 5 | 21 | +5.46 | -7.33 | -2.29 | -2.08 | PASS | 53.094 |
| `union_w_hot_candle_h3` | long | 3 | leftover | list | none | 49% | 55% | 14/22 | +0.25 | 31 | 105 | +8.70 | -9.55 | +7.41 | +194.75 | PASS | 52.963 |
| `union_join_vol_green_h1` | long | 1 | leftover | list | none | 44% | 36% | 11/22 | +1.13 | 8 | 28 | +4.88 | -4.27 | +10.16 | +4.66 | PASS | 50.201 |
| `short_alarm_h1` | short | 1 | leftover | list | none | 51% | 18% | 12/22 | +0.32 | 6 | 37 | +4.60 | -2.94 | +1.71 | +10.25 | PASS | 49.629 |
| `short_last_red_h3` | short | 3 | leftover | list | none | 53% | 41% | 14/22 | +0.37 | 14 | 90 | +4.88 | -6.13 | -5.13 | -18.20 | PASS | 49.448 |
| `union_ret_5_h1` | long | 1 | leftover | list | none | 47% | 36% | 11/22 | +0.12 | 17 | 55 | +5.76 | -5.60 | +1.64 | +11.12 | PASS | 49.226 |
| `combo_en_5050_shared` | mix | 5 | leftover | list | none | 44% | 45% | 11/22 | +0.15 | 0 | 0 | +5.54 | -6.48 | +21.34 | — | PASS | 48.979 |
| `union_vol_g_h3` | long | 3 | leftover | list | none | 47% | 45% | 14/22 | +0.71 | 22 | 94 | +8.66 | -8.55 | -2.70 | +24.05 | PASS | 47.035 |
| `combo_hn_3070_shared` | mix | 5 | leftover | list | none | 53% | 50% | 10/22 | +0.00 | 0 | 0 | +4.36 | -4.49 | +11.02 | — | PASS | 45.519 |
| `short_alarm_h3` | short | 3 | leftover | list | none | 54% | 36% | 12/22 | +1.35 | 10 | 70 | +7.29 | -8.49 | +2.59 | +16.22 | PASS | 45.205 |
| `union_earn_react_h1` | long | 1 | leftover | list | none | 48% | 32% | 11/22 | +0.04 | 6 | 44 | +4.81 | -7.92 | +1.82 | +4.30 | PASS | 44.301 |
| `combo_eer_5050_shared` | mix | 5 | leftover | list | none | 35% | 41% | 12/22 | +1.38 | 0 | 0 | +8.54 | -8.98 | +16.73 | — | PASS | 41.128 |
| `combo_sf_5050_shared` | mix | 5 | leftover | list | none | 57% | 68% | 7/22 | -2.71 | 0 | 0 | +7.05 | -6.64 | +12.08 | — | PASS | 40.835 |
| `union_news_g_h1` | long | 1 | leftover | list | none | 51% | 41% | 9/22 | -0.28 | 3 | 27 | +3.97 | -4.40 | +2.98 | +12.27 | PASS | 40.194 |
| `union_flow_in_h1` | long | 1 | leftover | list | none | 50% | 41% | 6/22 | -0.39 | 4 | 5 | +4.41 | -4.79 | +7.02 | +16.17 | PASS | 39.848 |
| `union_candle_score_h1` | long | 1 | leftover | list | none | 53% | 23% | 10/22 | +0.00 | 15 | 42 | +4.60 | -4.05 | +0.73 | +12.13 | PASS | 39.578 |
| `combo_nj_5050_shared` | mix | 5 | leftover | list | none | 48% | 36% | 10/22 | +0.00 | 0 | 0 | +4.31 | -4.55 | +3.84 | — | PASS | 38.962 |
| `flatten_live_h1_topheavy` | long | 1 | topheavy | list | none | 54% | 14% | 7/22 | +0.00 | 1 | 5 | +6.75 | -3.75 | +7.19 | +4.45 | PASS | 38.888 |
| `flatten_live_h1` | long | 1 | leftover | list | none | 54% | 14% | 7/22 | +0.00 | 1 | 5 | +6.31 | -4.05 | +8.22 | +4.45 | PASS | 37.848 |
| `flatten_live_h1_cut` | long | 1 | leftover | cut_loser | none | 54% | 14% | 7/22 | +0.00 | 1 | 5 | +6.31 | -4.05 | +8.22 | +4.45 | PASS | 37.848 |
| `flatten_live_h1_sboost` | long | 1 | leftover | list | both | 54% | 14% | 7/22 | +0.00 | 1 | 5 | +6.31 | -4.05 | +8.22 | +4.45 | PASS | 37.848 |
| `flatten_live_h1_sizeup` | long | 1 | leftover | list | sizeup | 54% | 14% | 7/22 | +0.00 | 1 | 5 | +6.31 | -4.05 | +8.22 | +4.45 | PASS | 37.848 |
| `flatten_live_h1_time` | long | 1 | leftover | time | none | 54% | 14% | 7/22 | +0.00 | 1 | 5 | +6.31 | -4.05 | +8.22 | +4.45 | PASS | 37.848 |
| `flatten_live_h1_trail` | long | 1 | leftover | trail | none | 54% | 14% | 7/22 | +0.00 | 1 | 5 | +6.31 | -4.05 | +8.22 | +4.45 | PASS | 37.848 |
| `flatten_live_h1_half` | long | 1 | half | list | none | 54% | 14% | 7/22 | +0.00 | 1 | 5 | +6.58 | -3.90 | +3.74 | +4.45 | PASS | 37.796 |
| `union_break10_h1` | long | 1 | leftover | list | none | 48% | 32% | 10/22 | +0.00 | 17 | 40 | +4.89 | -5.38 | -5.53 | +5.01 | PASS | 37.239 |
| `yday_gainer_h1` | long | 1 | leftover | list | none | 50% | 36% | 7/22 | -0.08 | 10 | 50 | +4.85 | -3.96 | +6.12 | +12.37 | PASS | 36.963 |
| `union_w_hot_cond_h3` | long | 3 | leftover | list | none | 43% | 55% | 10/22 | +0.00 | 32 | 112 | +10.10 | -8.59 | +8.71 | +186.69 | PASS | 36.503 |
| `combo_fse_333_shared` | mix | 5 | leftover | list | none | 49% | 68% | 7/22 | -0.23 | 0 | 0 | +6.67 | -7.67 | +12.97 | — | PASS | 36.467 |
| `union_e_green_h3` | long | 3 | leftover | list | none | 40% | 36% | 11/22 | +0.92 | 5 | 43 | +5.07 | -8.76 | -2.48 | -17.96 | PASS | 35.756 |
| `flatten_live_h1_rankw` | long | 1 | rank_w | list | none | 54% | 14% | 6/22 | +0.00 | 1 | 5 | +6.14 | -4.12 | +4.03 | +4.45 | PASS | 35.738 |
| `union_white_h1` | long | 1 | leftover | list | none | 41% | 36% | 9/22 | -0.20 | 3 | 21 | +5.74 | -4.77 | +3.97 | -0.52 | PASS | 35.721 |
| `union_h1_topheavy` | long | 1 | topheavy | list | none | 44% | 41% | 6/22 | -3.28 | 5 | 35 | +4.29 | -4.16 | +5.08 | +7.58 | PASS | 35.088 |
| `combo_hf_5050_shared` | mix | 5 | leftover | list | none | 49% | 50% | 6/22 | -0.82 | 0 | 0 | +6.10 | -6.03 | +4.44 | — | PASS | 34.936 |
| `flatten_h1` | long | 1 | leftover | list | none | 45% | 36% | 6/22 | -5.01 | 2 | 24 | +4.44 | -3.96 | +1.83 | -0.75 | PASS | 34.685 |
| `union_break10_h3` | long | 3 | leftover | list | none | 46% | 45% | 10/22 | +0.00 | 24 | 94 | +8.55 | -8.08 | -1.50 | +26.84 | PASS | 34.595 |
| `union_last_green_h1` | long | 1 | leftover | list | none | 47% | 32% | 5/22 | -1.92 | 8 | 32 | +4.47 | -3.57 | +2.76 | +13.02 | PASS | 34.442 |
| `short_rsi_ob_h3` | short | 3 | leftover | list | none | 50% | 41% | 10/22 | +0.00 | 22 | 89 | +6.13 | -7.34 | -4.02 | -128.80 | PASS | 34.122 |
| `union_hot_n12_h1` | long | 1 | leftover | list | none | 51% | 36% | 6/22 | -0.28 | 30 | 73 | +4.65 | -5.74 | -4.14 | +16.70 | PASS | 34.037 |
| `combo_ne1_5050_shared` | mix | 5 | leftover | list | none | 51% | 36% | 5/22 | -1.86 | 0 | 0 | +4.16 | -5.50 | +8.22 | — | PASS | 33.674 |
| `combo_sf_3070_shared` | mix | 5 | leftover | list | none | 55% | 59% | 2/22 | -3.80 | 0 | 0 | +7.39 | -6.85 | +9.52 | — | PASS | 32.119 |
| `union_news_g_conv_h1` | long | 1 | conviction | list | none | 51% | 41% | 2/22 | -7.88 | 1 | 14 | +3.77 | -4.44 | +0.47 | +9.10 | PASS | 32.032 |
| `combo_ner_5050_shared` | mix | 5 | leftover | list | none | 42% | 45% | 6/22 | -0.67 | 0 | 0 | +5.66 | -6.80 | +19.34 | — | PASS | 31.88 |
| `union_h1_cut` | long | 1 | leftover | cut_loser | none | 44% | 36% | 4/22 | -3.65 | 5 | 35 | +4.25 | -4.14 | +1.22 | +7.58 | PASS | 31.305 |
| `union_h1` | long | 1 | leftover | list | none | 44% | 36% | 4/22 | -3.87 | 5 | 35 | +4.25 | -4.19 | +1.06 | +7.58 | PASS | 31.222 |
| `union_h1_sizeup` | long | 1 | leftover | list | sizeup | 44% | 36% | 4/22 | -3.87 | 5 | 35 | +4.25 | -4.19 | +1.06 | +7.58 | PASS | 31.222 |
| `union_h1_trail` | long | 1 | leftover | trail | none | 44% | 36% | 4/22 | -3.87 | 5 | 35 | +4.25 | -4.19 | +1.06 | +7.58 | PASS | 31.222 |
| `union_h1_sboost` | long | 1 | leftover | list | both | 44% | 36% | 4/22 | -3.87 | 5 | 35 | +4.22 | -4.27 | +1.12 | +7.58 | PASS | 31.106 |
| `union_join_present_h1` | long | 1 | leftover | list | none | 45% | 36% | 4/22 | -3.87 | 4 | 38 | +4.25 | -4.28 | +0.65 | +7.96 | PASS | 30.858 |
| `union_h3_cut` | long | 3 | leftover | cut_loser | none | 46% | 55% | 1/22 | -4.13 | 15 | 81 | +8.80 | -5.41 | +0.46 | +11.37 | PASS | 30.738 |
| `union_h3_trail` | long | 3 | leftover | trail | none | 46% | 55% | 1/22 | -4.13 | 15 | 81 | +8.80 | -5.41 | +0.46 | +11.37 | PASS | 30.738 |
| `combo_jf_5050_shared` | mix | 5 | leftover | list | none | 45% | 64% | 1/22 | -5.08 | 0 | 0 | +6.15 | -5.63 | +3.94 | — | PASS | 30.181 |
| `combo_fes_403030_shared` | mix | 5 | leftover | list | none | 47% | 64% | 3/22 | -0.70 | 0 | 0 | +6.92 | -7.49 | +10.74 | — | PASS | 30.176 |
| `union_news_or_net2_h1` | long | 1 | leftover | list | none | 45% | 32% | 4/22 | -2.00 | 1 | 18 | +4.06 | -4.16 | -1.62 | -1.71 | PASS | 30.127 |
| `union_h3_time` | long | 3 | leftover | time | none | 46% | 55% | 1/22 | -4.12 | 15 | 81 | +8.04 | -5.52 | +0.93 | +11.37 | PASS | 29.954 |
| `combo_en_3070_shared` | mix | 5 | leftover | list | none | 43% | 36% | 5/22 | -1.50 | 0 | 0 | +5.62 | -6.33 | +18.48 | — | PASS | 29.855 |
| `union_candle_score_h3` | long | 3 | leftover | list | none | 48% | 41% | 6/22 | -0.54 | 20 | 104 | +6.86 | -6.53 | +3.84 | +19.13 | PASS | 29.804 |
| `union_vol_g_h1` | long | 1 | leftover | list | none | 47% | 36% | 4/22 | -1.25 | 13 | 50 | +5.38 | -5.59 | -1.27 | +12.96 | PASS | 29.535 |
| `combo_fh_7030_shared` | mix | 5 | leftover | list | none | 46% | 59% | 1/22 | -4.99 | 0 | 0 | +6.35 | -6.12 | +0.17 | — | PASS | 29.508 |
| `union_candle_h1` | long | 1 | leftover | list | none | 44% | 27% | 4/22 | -2.52 | 7 | 38 | +5.03 | -4.19 | -1.54 | +4.39 | PASS | 29.486 |
| `short_last_red_h1` | short | 1 | leftover | list | none | 49% | 23% | 7/22 | -0.18 | 6 | 56 | +3.95 | -4.50 | -8.96 | -7.91 | PASS | 29.332 |
| `ohlc_hot_coil_h1` | long | 1 | leftover | list | none | 50% | 27% | 5/22 | -6.59 | 9 | 21 | +2.97 | -3.99 | -19.20 | -8.01 | PASS | 29.131 |
| `short_rsi_ob_h1` | short | 1 | leftover | list | none | 48% | 36% | 3/22 | -0.96 | 11 | 41 | +4.47 | -4.89 | -8.72 | -11.27 | PASS | 28.938 |
| `union_h3` | long | 3 | leftover | list | none | 46% | 55% | 0/22 | -5.51 | 15 | 81 | +8.80 | -5.82 | -1.10 | +11.37 | PASS | 28.792 |
| `union_h3_sizeup` | long | 3 | leftover | list | sizeup | 46% | 55% | 0/22 | -5.51 | 15 | 81 | +8.80 | -5.82 | -1.10 | +11.37 | PASS | 28.792 |
| `union_news_vol_h3` | long | 3 | leftover | list | none | 50% | 50% | 4/22 | -2.25 | 9 | 45 | +4.24 | -4.61 | -2.85 | +195.15 | PASS | 28.652 |
| `probable_probable_ok_h3` | long | 3 | leftover | list | none | 56% | 32% | 6/22 | -0.11 | 13 | 41 | +5.80 | -8.29 | -17.39 | +13.96 | PASS | 28.581 |
| `union_h1_rankw` | long | 1 | rank_w | list | none | 44% | 41% | 1/22 | -3.88 | 5 | 35 | +4.27 | -4.21 | +0.07 | +7.58 | PASS | 28.577 |
| `union_h3_sboost` | long | 3 | leftover | list | both | 46% | 55% | 0/22 | -5.51 | 15 | 81 | +8.46 | -5.87 | -1.17 | +11.37 | PASS | 28.422 |
| `probable_h1` | long | 1 | leftover | list | none | 49% | 27% | 3/22 | -1.65 | 9 | 49 | +3.95 | -3.65 | -1.65 | +8.99 | PASS | 28.234 |
| `union_blue_h1` | long | 1 | leftover | list | none | 43% | 27% | 3/22 | -2.54 | 6 | 28 | +4.16 | -4.07 | -0.79 | -1.60 | PASS | 28.127 |
| `union_h3_half` | long | 3 | half | list | none | 46% | 50% | 0/22 | -5.50 | 15 | 81 | +8.37 | -5.10 | -4.27 | +11.37 | PASS | 28.056 |
| `union_h1_time` | long | 1 | leftover | time | none | 44% | 36% | 1/22 | -3.65 | 5 | 35 | +4.10 | -3.81 | +0.61 | +7.58 | PASS | 28.052 |
| `flatten_h3_sboost` | long | 3 | leftover | list | both | 47% | 45% | 0/22 | -6.47 | 11 | 59 | +7.81 | -4.93 | -3.21 | +5.26 | PASS | 28.019 |
| `probable_probable_ok_h1` | long | 1 | leftover | list | none | 49% | 27% | 1/22 | -0.58 | 5 | 19 | +3.50 | -3.15 | -1.51 | +5.47 | PASS | 27.915 |
| `flatten_h3` | long | 3 | leftover | list | none | 47% | 45% | 0/22 | -6.47 | 11 | 59 | +8.12 | -5.21 | -3.15 | +5.26 | PASS | 27.898 |
| `flatten_h3_cut` | long | 3 | leftover | cut_loser | none | 47% | 45% | 0/22 | -6.47 | 11 | 59 | +8.12 | -5.21 | -3.15 | +5.26 | PASS | 27.898 |
| `flatten_h3_sizeup` | long | 3 | leftover | list | sizeup | 47% | 45% | 0/22 | -6.47 | 11 | 59 | +8.12 | -5.21 | -3.15 | +5.26 | PASS | 27.898 |
| `flatten_h3_trail` | long | 3 | leftover | trail | none | 47% | 45% | 0/22 | -6.47 | 11 | 59 | +8.12 | -5.21 | -3.15 | +5.26 | PASS | 27.898 |
| `flatten_h3_half` | long | 3 | half | list | none | 47% | 45% | 0/22 | -3.43 | 11 | 59 | +7.14 | -4.76 | -2.53 | +5.26 | PASS | 27.692 |
| `short_extended_h3` | short | 3 | leftover | list | none | 53% | 50% | 2/22 | -1.80 | 35 | 101 | +9.36 | -9.14 | -8.25 | -29.25 | PASS | 27.489 |
| `union_macd_up_h1` | long | 1 | leftover | list | none | 43% | 27% | 3/22 | -3.76 | 10 | 41 | +4.52 | -4.20 | -2.38 | +9.10 | PASS | 27.35 |
| `flatten_h3_time` | long | 3 | leftover | time | none | 47% | 45% | 0/22 | -5.71 | 11 | 59 | +7.37 | -5.45 | -0.12 | +5.26 | PASS | 27.322 |
| `union_white_coil_h1` | long | 1 | leftover | list | none | 44% | 27% | 4/22 | -5.67 | 4 | 21 | +4.37 | -3.90 | -5.70 | -1.83 | PASS | 27.317 |
| `flatten_h5` | long | 5 | leftover | list | none | 44% | 64% | 2/22 | -6.18 | 19 | 88 | +9.13 | -9.28 | +2.56 | +3.92 | PASS | 27.296 |
| `flatten_h5_cut` | long | 5 | leftover | cut_loser | none | 44% | 64% | 2/22 | -6.18 | 19 | 88 | +9.13 | -9.28 | +2.56 | +3.92 | PASS | 27.296 |
| `flatten_h5_sizeup` | long | 5 | leftover | list | sizeup | 44% | 64% | 2/22 | -6.18 | 19 | 88 | +9.13 | -9.28 | +2.56 | +3.92 | PASS | 27.296 |
| `flatten_h5_time` | long | 5 | leftover | time | none | 44% | 64% | 2/22 | -6.18 | 19 | 88 | +9.13 | -9.28 | +2.56 | +3.92 | PASS | 27.296 |
| `flatten_h5_trail` | long | 5 | leftover | trail | none | 44% | 64% | 2/22 | -6.18 | 19 | 88 | +9.13 | -9.28 | +2.56 | +3.92 | PASS | 27.296 |
| `union_h3_rankw` | long | 3 | rank_w | list | none | 46% | 50% | 0/22 | -6.97 | 15 | 81 | +8.61 | -5.78 | -5.74 | +11.37 | PASS | 27.071 |
| `union_news_head_h1` | long | 1 | leftover | list | none | 50% | 23% | 2/22 | -2.32 | 4 | 22 | +4.19 | -4.50 | -6.21 | +13.01 | PASS | 27.034 |
| `union_join_present_h3` | long | 3 | leftover | list | none | 44% | 45% | 0/22 | -4.14 | 14 | 83 | +9.71 | -5.72 | -0.75 | +10.10 | PASS | 26.951 |
| `flatten_h5_sboost` | long | 5 | leftover | list | both | 44% | 64% | 2/22 | -6.99 | 19 | 88 | +8.88 | -9.00 | +0.10 | +3.92 | PASS | 26.936 |
| `yday_gainer_h3` | long | 3 | leftover | list | none | 51% | 41% | 2/22 | -0.19 | 20 | 92 | +9.02 | -7.09 | -4.65 | +30.51 | PASS | 26.735 |
| `union_e_green_h1` | long | 1 | leftover | list | none | 48% | 36% | 5/22 | -3.13 | 3 | 16 | +4.19 | -9.48 | -22.39 | -13.12 | PASS | 26.726 |
| `union_h3_exit_news_r` | long | 3 | leftover | list | none | 44% | 45% | 0/22 | -4.75 | 14 | 83 | +9.71 | -5.78 | -1.93 | +7.29 | PASS | 26.69 |
| `union_h3_topheavy` | long | 3 | topheavy | list | none | 46% | 45% | 1/22 | -4.37 | 15 | 81 | +7.05 | -5.99 | -0.62 | +11.37 | PASS | 26.506 |
| `union_h5_time` | long | 5 | leftover | time | none | 46% | 64% | 1/22 | -6.90 | 26 | 117 | +9.14 | -9.15 | +1.81 | +18.42 | PASS | 26.46 |
| `flatten_h3_rankw` | long | 3 | rank_w | list | none | 47% | 41% | 0/22 | -5.39 | 11 | 59 | +7.94 | -5.38 | -4.18 | +5.26 | PASS | 26.416 |
| `union_h5` | long | 5 | leftover | list | none | 46% | 64% | 1/22 | -7.58 | 26 | 117 | +9.04 | -9.18 | +0.95 | +18.42 | PASS | 26.261 |
| `union_h5_cut` | long | 5 | leftover | cut_loser | none | 46% | 64% | 1/22 | -7.58 | 26 | 117 | +9.04 | -9.18 | +0.95 | +18.42 | PASS | 26.261 |
| `union_h5_sizeup` | long | 5 | leftover | list | sizeup | 46% | 64% | 1/22 | -7.58 | 26 | 117 | +9.04 | -9.18 | +0.95 | +18.42 | PASS | 26.261 |
| `union_h5_trail` | long | 5 | leftover | trail | none | 46% | 64% | 1/22 | -7.58 | 26 | 117 | +9.04 | -9.18 | +0.95 | +18.42 | PASS | 26.261 |
| `union_rsi_os_h3` | long | 3 | leftover | list | none | 51% | 36% | 10/22 | +0.00 | 2 | 30 | +3.62 | -6.02 | -8.97 | -21.06 | PASS | 26.197 |
| `union_h5_topheavy` | long | 5 | topheavy | list | none | 46% | 59% | 2/22 | -5.23 | 26 | 117 | +8.80 | -9.42 | +0.35 | +18.42 | PASS | 26.141 |
| `union_news_or_net4_conv_h1` | long | 1 | conviction | list | none | 46% | 36% | 0/22 | -12.24 | 1 | 8 | +3.53 | -4.19 | -7.86 | -6.41 | PASS | 26.138 |
| `union_join_g_h1` | long | 1 | leftover | list | none | 46% | 32% | 1/22 | -3.79 | 5 | 42 | +4.05 | -4.50 | +0.10 | +2.38 | PASS | 26.022 |
| `union_coil_green_h1` | long | 1 | leftover | list | none | 44% | 36% | 0/22 | -4.02 | 11 | 27 | +3.18 | -3.97 | -2.46 | -1.44 | PASS | 25.939 |
| `combo_fe1_5050_shared` | mix | 5 | leftover | list | none | 47% | 55% | 0/22 | -5.71 | 0 | 0 | +5.92 | -6.80 | -0.48 | — | PASS | 25.659 |
| `union_h5_sboost` | long | 5 | leftover | list | both | 46% | 64% | 1/22 | -7.58 | 26 | 117 | +8.55 | -9.71 | +0.40 | +18.42 | PASS | 25.652 |
| `union_blue_vol_h1` | long | 1 | leftover | list | none | 42% | 32% | 4/22 | -2.28 | 7 | 39 | +5.53 | -5.47 | -4.92 | +1.78 | PASS | 25.635 |
| `union_h3_exit_alarm` | long | 3 | leftover | list | none | 44% | 50% | 0/22 | -6.87 | 14 | 83 | +6.97 | -5.54 | -1.43 | +9.42 | PASS | 25.569 |
| `union_last_red_h3` | long | 3 | leftover | list | none | 42% | 50% | 3/22 | -0.95 | 13 | 94 | +6.75 | -6.43 | +0.73 | +6.10 | PASS | 25.559 |
| `combo_ef_5050_shared` | mix | 5 | leftover | list | none | 43% | 59% | 1/22 | -2.04 | 0 | 0 | +8.35 | -8.09 | +9.04 | — | PASS | 25.448 |
| `combo_fe_5050_shared` | mix | 5 | leftover | list | none | 43% | 59% | 1/22 | -2.04 | 0 | 0 | +8.35 | -8.09 | +9.04 | — | PASS | 25.448 |
| `union_h5_half` | long | 5 | half | list | none | 46% | 59% | 1/22 | -4.54 | 26 | 117 | +8.07 | -7.99 | +0.52 | +18.42 | PASS | 25.411 |
| `combo_ef_3070_shared` | mix | 5 | leftover | list | none | 45% | 59% | 1/22 | -5.21 | 0 | 0 | +8.38 | -8.32 | +3.72 | — | PASS | 25.271 |
| `union_h1_half` | long | 1 | half | list | none | 44% | 32% | 0/22 | -1.87 | 5 | 35 | +4.09 | -4.30 | -0.82 | +7.58 | PASS | 25.171 |
| `union_news_vol_h1` | long | 1 | leftover | list | none | 47% | 27% | 0/22 | -2.52 | 5 | 18 | +4.36 | -4.82 | -0.26 | +9.42 | PASS | 25.134 |
| `union_rsi_os_h1` | long | 1 | leftover | list | none | 54% | 32% | 4/22 | -7.48 | 2 | 18 | +2.73 | -4.97 | -10.67 | +4.98 | PASS | 25.133 |
| `union_vol_green_h3` | long | 3 | leftover | list | none | 48% | 27% | 7/22 | -1.02 | 23 | 79 | +6.73 | -8.64 | -11.20 | +16.52 | PASS | 25.113 |
| `short_extended_h1` | short | 1 | leftover | list | none | 52% | 32% | 0/22 | -1.94 | 18 | 54 | +5.66 | -6.08 | -9.99 | -6.56 | PASS | 25.042 |
| `flatten_h3_topheavy` | long | 3 | topheavy | list | none | 47% | 41% | 0/22 | -5.21 | 11 | 59 | +6.20 | -5.42 | -2.72 | +5.26 | PASS | 24.979 |
| `union_cond_h1` | long | 1 | leftover | list | none | 47% | 32% | 0/22 | -4.13 | 4 | 35 | +4.46 | -4.92 | -8.30 | -3.44 | PASS | 24.875 |
| `ohlc_hot_h1` | long | 1 | leftover | list | none | 44% | 41% | 1/22 | -5.32 | 15 | 41 | +3.21 | -4.94 | -10.10 | +0.23 | PASS | 24.832 |
| `union_last_green_h5` | long | 5 | leftover | list | none | 47% | 59% | 2/22 | -7.73 | 23 | 122 | +8.08 | -11.40 | +2.23 | +34.42 | PASS | 24.814 |
| `union_last_green_h3` | long | 3 | leftover | list | none | 53% | 36% | 0/22 | -5.33 | 18 | 83 | +6.01 | -7.81 | -1.60 | +31.43 | PASS | 24.117 |
| `union_blue_coil_h3` | long | 3 | leftover | list | none | 41% | 50% | 0/22 | -4.84 | 10 | 59 | +6.44 | -5.84 | -5.76 | -25.53 | PASS | 24.105 |
| `union_white_h5` | long | 5 | leftover | list | none | 44% | 64% | 1/22 | -8.95 | 16 | 62 | +10.05 | -8.83 | -0.46 | +35.97 | PASS | 24.101 |
| `union_join_vol_green_h3` | long | 3 | leftover | list | none | 43% | 32% | 7/22 | -1.97 | 15 | 60 | +6.36 | -8.43 | -8.83 | +12.90 | PASS | 24.031 |
| `flatten_h5_topheavy` | long | 5 | topheavy | list | none | 44% | 55% | 1/22 | -4.98 | 19 | 88 | +8.82 | -9.04 | +0.70 | +3.92 | PASS | 24.019 |
| `union_vol_ab_h1` | long | 1 | leftover | list | none | 49% | 23% | 0/22 | -1.41 | 10 | 31 | +4.54 | -4.76 | -1.41 | +46.07 | PASS | 24.0 |
| `union_vol_g_h5` | long | 5 | leftover | list | none | 46% | 36% | 6/22 | -4.95 | 28 | 109 | +13.48 | -10.85 | -13.36 | +13.02 | PASS | 23.845 |
| `union_blue_vol_h3` | long | 3 | leftover | list | none | 37% | 41% | 6/22 | -3.07 | 14 | 71 | +8.67 | -7.05 | -6.83 | +116.72 | PASS | 23.732 |
| `union_h3_exit_red` | long | 3 | leftover | list | none | 51% | 36% | 0/22 | -5.03 | 18 | 83 | +5.97 | -7.06 | -1.17 | +23.57 | PASS | 23.639 |
| `union_news_present_h1` | long | 1 | leftover | list | none | 43% | 27% | 0/22 | -3.87 | 4 | 37 | +4.16 | -4.17 | -1.52 | +5.41 | PASS | 23.467 |
| `union_macd_hist_h1` | long | 1 | leftover | list | none | 46% | 36% | 0/22 | -3.70 | 9 | 60 | +3.85 | -4.00 | -6.92 | -4.83 | PASS | 23.396 |
| `probable_h3` | long | 3 | leftover | list | none | 49% | 36% | 3/22 | -0.19 | 18 | 96 | +6.85 | -8.67 | -4.95 | +12.17 | PASS | 23.329 |
| `flatten_h5_half` | long | 5 | half | list | none | 44% | 55% | 0/22 | -4.95 | 19 | 88 | +7.96 | -7.43 | -0.55 | +3.92 | PASS | 23.18 |
| `union_last_red_h1` | long | 1 | leftover | list | none | 49% | 32% | 0/22 | -3.99 | 6 | 55 | +3.47 | -4.92 | -2.91 | +9.27 | PASS | 23.045 |
| `flatten_h5_rankw` | long | 5 | rank_w | list | none | 44% | 50% | 1/22 | -5.62 | 19 | 88 | +8.45 | -9.62 | +1.08 | +3.92 | PASS | 22.681 |
| `union_h5_exit_alarm` | long | 5 | leftover | list | none | 47% | 55% | 0/22 | -6.25 | 24 | 118 | +7.45 | -8.18 | -0.43 | +20.44 | PASS | 22.668 |
| `combo_fer_5050_shared` | mix | 5 | leftover | list | none | 41% | 55% | 1/22 | -3.43 | 0 | 0 | +8.19 | -8.94 | +6.57 | — | PASS | 22.572 |
| `union_h5_rankw` | long | 5 | rank_w | list | none | 46% | 59% | 0/22 | -8.72 | 26 | 117 | +7.65 | -9.73 | -3.49 | +18.42 | PASS | 22.552 |
| `union_macd_up_h3` | long | 3 | leftover | list | none | 40% | 45% | 0/22 | -4.20 | 19 | 94 | +8.04 | -5.70 | -4.20 | +5.92 | PASS | 22.473 |
| `union_flow_in_h3` | long | 3 | leftover | list | none | 48% | 27% | 4/22 | -3.05 | 5 | 24 | +1.89 | -4.94 | -4.03 | -4.57 | PASS | 22.41 |
| `union_vol_ab_h3` | long | 3 | leftover | list | none | 40% | 32% | 5/22 | -2.08 | 15 | 63 | +7.40 | -7.28 | -2.64 | +41.44 | PASS | 22.38 |
| `union_macd_xup_h1` | long | 1 | leftover | list | none | 35% | 32% | 5/22 | -1.12 | 4 | 24 | +2.87 | -4.72 | -12.77 | -18.33 | PASS | 22.234 |
| `union_news_or_net4_h1` | long | 1 | leftover | list | none | 42% | 32% | 0/22 | -12.29 | 1 | 15 | +4.05 | -4.16 | -11.74 | -9.27 | PASS | 22.178 |
| `union_ab_g_h1` | long | 1 | leftover | list | none | 40% | 23% | 0/22 | -3.87 | 4 | 31 | +4.12 | -3.82 | -0.82 | -4.01 | PASS | 21.872 |
| `short_macd_dn_h3` | short | 3 | leftover | list | none | 47% | 41% | 1/22 | -1.16 | 15 | 83 | +3.99 | -5.67 | -10.02 | -21.96 | PASS | 21.6 |
| `union_blue_coil_h1` | long | 1 | leftover | list | none | 43% | 23% | 0/22 | -6.43 | 4 | 24 | +3.37 | -4.28 | -7.75 | -9.21 | PASS | 21.532 |
| `union_news_g_cam71_h1` | long | 1 | leftover | list | none | 37% | 32% | 0/22 | -10.46 | 1 | 3 | +2.93 | -4.12 | -10.46 | -4.11 | PASS | 21.504 |
| `union_news_present_h3` | long | 3 | leftover | list | none | 42% | 41% | 0/22 | -7.46 | 13 | 77 | +6.76 | -5.75 | -8.50 | +1.56 | PASS | 21.317 |
| `yday_gainer_h5` | long | 5 | leftover | list | none | 49% | 41% | 1/22 | -2.56 | 26 | 115 | +9.26 | -9.47 | -5.78 | +17.71 | PASS | 21.156 |
| `union_blue_h3` | long | 3 | leftover | list | none | 41% | 41% | 0/22 | -6.46 | 12 | 66 | +6.85 | -6.22 | -9.03 | -17.93 | PASS | 21.148 |
| `flatten_live_h3` | long | 3 | leftover | list | none | 58% | 14% | 0/22 | -6.24 | 4 | 14 | +8.60 | -8.85 | -6.24 | +6.87 | PASS | 20.819 |
| `combo_e1er_5050_shared` | mix | 5 | leftover | list | none | 45% | 18% | 5/22 | -0.63 | 0 | 0 | +4.67 | -8.06 | -3.22 | — | PASS | 20.633 |
| `union_join_g_h3` | long | 3 | leftover | list | none | 42% | 32% | 0/22 | -6.73 | 14 | 85 | +7.46 | -5.79 | -5.11 | -3.74 | PASS | 20.168 |
| `union_coil_off_h1` | long | 1 | leftover | list | none | 41% | 23% | 0/22 | -9.99 | 6 | 38 | +3.46 | -3.86 | -9.99 | -10.73 | PASS | 19.804 |
| `union_white_h3` | long | 3 | leftover | list | none | 41% | 41% | 2/22 | -10.90 | 10 | 51 | +7.74 | -6.89 | -10.77 | +6.27 | PASS | 19.714 |
| `union_cond_n4_h3` | long | 3 | leftover | list | none | 41% | 41% | 0/22 | -8.96 | 7 | 41 | +5.90 | -6.94 | -8.37 | -4.16 | PASS | 19.42 |
| `union_candle_h3` | long | 3 | leftover | list | none | 47% | 27% | 0/22 | -3.60 | 18 | 90 | +7.14 | -7.15 | -5.42 | +8.38 | PASS | 19.402 |
| `union_news_g_h3` | long | 3 | leftover | list | none | 48% | 27% | 0/22 | -3.05 | 17 | 65 | +4.55 | -5.92 | -11.66 | +172.99 | PASS | 19.193 |
| `union_cond_h3` | long | 3 | leftover | list | none | 36% | 50% | 0/22 | -8.90 | 11 | 92 | +6.55 | -7.43 | -6.93 | -17.08 | PASS | 18.393 |
| `ohlc_hot_h3` | long | 3 | leftover | list | none | 44% | 41% | 0/22 | -3.35 | 25 | 109 | +6.20 | -7.16 | -3.16 | +77.25 | PASS | 18.261 |
| `flatten_live_h5` | long | 5 | leftover | list | none | 46% | 32% | 0/22 | -5.54 | 5 | 16 | +9.01 | -11.65 | -5.54 | +5.13 | PASS | 17.521 |
| `union_ab_g_h3` | long | 3 | leftover | list | none | 37% | 27% | 0/22 | -8.73 | 11 | 70 | +7.68 | -5.44 | -8.73 | -17.33 | PASS | 16.822 |
| `union_white_coil_h3` | long | 3 | leftover | list | none | 41% | 41% | 0/22 | -12.31 | 8 | 44 | +5.31 | -7.18 | -8.53 | -16.52 | PASS | 16.312 |
| `union_news_head_h3` | long | 3 | leftover | list | none | 47% | 14% | 0/22 | -4.50 | 16 | 57 | +5.49 | -5.24 | -14.88 | +183.55 | PASS | 16.289 |
| `union_rsi_h1` | long | 1 | leftover | list | none | 43% | 23% | 0/22 | -8.09 | 6 | 67 | +3.90 | -5.96 | -11.87 | -8.62 | PASS | 16.206 |
| `flatten_vol_g_h3` | long | 3 | leftover | list | none | 47% | 18% | 0/22 | -16.36 | 5 | 19 | +9.63 | -9.36 | -21.80 | -21.92 | PASS | 14.73 |
| `ohlc_hot_h5` | long | 5 | leftover | list | none | 40% | 41% | 0/22 | -8.34 | 36 | 132 | +9.07 | -9.75 | -10.71 | +54.35 | PASS | 14.096 |
| `union_news_g_h5` | long | 5 | leftover | list | none | 39% | 41% | 0/22 | -3.68 | 19 | 98 | +5.57 | -8.05 | -10.80 | +121.72 | PASS | 13.272 |
| `probable_h5` | long | 5 | leftover | list | none | 48% | 27% | 0/22 | -3.37 | 24 | 119 | +7.20 | -10.44 | -18.57 | -2.95 | PASS | 12.744 |
| `union_coil_off_h3` | long | 3 | leftover | list | none | 36% | 27% | 0/22 | -9.81 | 16 | 85 | +4.47 | -8.48 | -9.81 | -19.71 | PASS | 12.251 |
| `coil_h3_exit_alarm` | long | 3 | leftover | list | none | 35% | 27% | 0/22 | -8.36 | 13 | 92 | +4.34 | -7.45 | -6.26 | -17.43 | PASS | 11.865 |
| `union_coil_green_h3` | long | 3 | leftover | list | none | 36% | 23% | 0/22 | -8.96 | 19 | 85 | +3.87 | -6.65 | -8.72 | -17.69 | PASS | 11.443 |
| `union_coil_off_h5` | long | 5 | leftover | list | none | 38% | 36% | 0/22 | -11.30 | 22 | 112 | +3.90 | -72.70 | -5.98 | -27.55 | PASS | 10.535 |
| `union_news_both_h1` *(thin)* | long | 1 | leftover | list | none | 100% | 14% | 21/22 | +7.89 | 0 | 0 | +3.69 | — | +7.92 | +4.91 | PASS | 62.779 |
| `short_news_pack_h3` *(thin)* | short | 3 | leftover | list | none | 60% | 41% | 21/22 | +7.50 | 1 | 6 | +3.31 | -2.00 | +10.01 | +12.86 | PASS | 48.791 |
| `union_flow_in_white_h1` *(thin)* | long | 1 | leftover | list | none | 64% | 23% | 17/22 | +0.29 | 0 | 2 | +3.42 | -2.35 | +9.83 | +12.54 | PASS | 44.443 |
| `union_catal_present_h3` *(thin)* | long | 3 | leftover | list | none | 40% | 9% | 21/22 | +0.13 | 1 | 1 | +5.43 | -2.57 | +0.13 | +2.62 | PASS | 41.25 |
| `union_news_missing_h1` *(thin)* | long | 1 | leftover | list | none | 56% | 14% | 11/22 | +0.19 | 0 | 2 | +3.78 | -2.55 | +1.64 | +2.18 | PASS | 32.894 |
| `union_news_missing_h3` *(thin)* | long | 3 | leftover | list | none | 56% | 27% | 1/22 | +0.00 | 1 | 7 | +9.22 | -3.20 | +3.13 | +4.96 | PASS | 16.149 |
| `union_catal_present_h1` *(thin)* | long | 1 | leftover | list | none | 40% | 5% | 0/22 | -2.79 | 0 | 0 | +7.67 | -1.88 | -2.79 | +6.22 | PASS | 11.491 |
| `union_vol_missing_h1` *(thin)* | long | 1 | leftover | list | none | 50% | 5% | 1/22 | +0.00 | 0 | 2 | +7.93 | -3.05 | +1.44 | +1.88 | PASS | 10.273 |
| `union_vol_missing_h3` *(thin)* | long | 3 | leftover | list | none | 50% | 14% | 1/22 | +0.00 | 1 | 5 | +11.98 | -3.98 | +3.58 | +5.59 | PASS | 8.778 |
| `union_news_g_cam91_n1_h1` *(thin)* | long | 1 | leftover | list | none | 50% | 5% | 0/22 | -2.85 | 0 | 0 | +0.85 | -3.84 | -2.85 | -0.09 | PASS | 1.587 |
| `union_rsi_os_macd_h1` *(thin)* | long | 1 | leftover | list | none | 33% | 5% | 1/22 | -5.91 | 1 | 2 | +7.49 | -6.33 | -5.91 | +0.01 | PASS | -7.927 |
| `short_r_down_h1` *(thin)* | short | 1 | leftover | list | none | — | 0% | 0/22 | +0.00 | 0 | 0 | — | — | +0.00 | +0.00 | PASS | -15.0 |
| `short_r_down_h3` *(thin)* | short | 3 | leftover | list | none | — | 0% | 0/22 | +0.00 | 0 | 0 | — | — | +0.00 | +0.00 | PASS | -15.0 |
| `union_r_up_h1` *(thin)* | long | 1 | leftover | list | none | — | 0% | 0/22 | +0.00 | 0 | 0 | — | — | +0.00 | +0.00 | PASS | -15.0 |
| `union_r_up_h3` *(thin)* | long | 3 | leftover | list | none | — | 0% | 0/22 | +0.00 | 0 | 0 | — | — | +0.00 | +0.00 | PASS | -15.0 |
