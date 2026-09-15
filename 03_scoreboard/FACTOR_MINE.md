# Factor strategy mine — 2026-08-13 → 2026-09-15

Leak-free 09:30 recipes: **270** · candidate rows **1776** · fill `09:30 open, whole shares, Futubull fees, leftover split, sell first, hard-red sit, cash+holdings audit`.

Cash book: $10k, whole shares, Futubull fees, leftover split, sell first, min-hold, 09:30 open, hard-red S≤−3 sit, shorts marked as a liability. Each session starts from leftover cash and lots actually held (butterfly). Cash-start buttons wake a sleeve on date X with $10k and no lots (same rules). Stock investigator quotes 09:30 cameras / coaches / news from repo files. Size / sell / S-boost tweaks sit on the same ledger. Signal-only % is the old equal-weight path (not a fill). `flatten_h*` = wish-list (io/HOLD mornings still buy). `flatten_live_*` = only when the live flatten gate fires. Research only — does not change live `flatten_robust`.

Combination books: **77** mixes on the same $10k cash ledger (shared leftover or split sleeves, official 09:30 / 16:00, hard-red sit, owner min-hold). Outperformers: `combo_sh_5050_shared`, `combo_sh_3070_shared`, `combo_seh_451540_shared`, `combo_sh_7030_shared`, `combo_seh_333_skip`, `combo_jse_333_shared`, `combo_seh_601525_shared`, `combo_seh_502525_shared`, `combo_ej_5050_shared`, `combo_es_8020_shared`, `combo_se_5050_skip`, `combo_ps_7030_shared`, `combo_es_9010_shared`, `combo_jer_5050_shared`, `combo_se_3070_shared`, `combo_p2s_5050_shared`, `combo_ps_5050_shared`, `combo_ers_7030_shared`, `combo_se1_5050_shared`, `combo_ser_5050_shared`, `combo_sj_5050_shared`, `combo_e1s_7030_shared`, `combo_sj_3070_shared`, `combo_sj_7030_shared`, `combo_snj_333_shared`, `combo_sn_5050_shared`, `combo_je1_5050_shared`.

A combo outperforms its members on the cash book when the audit passes and either (1) Book% is strictly higher than every member, or (2) Book% stays within 2pp of the richest member, max drawdown is strictly smaller than every member's drawdown, start-rate is at least the best member, both halves are green, and the worst session is no worse than the worst member session. Effectiveness / Signal% do not decide WIN.

Action blotters: [FACTOR_MINE_ACTION.md](FACTOR_MINE_ACTION.md).

| Strategy | Side | H | Size | Sell | Boost | Win% | $ days | Starts YES | Med start | Top-g | Losers | AvgW | AvgL | Book% | Signal% | Audit | Eff |
|---|---|---:|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---:|
| `combo_p2s_5050_shared` | mix | 5 | leftover | list | none | 68% | 74% | 21/23 | +14.55 | 0 | 0 | +4.75 | -3.42 | +27.36 | — | PASS | 79.518 |
| `combo_ps_5050_shared` | mix | 5 | leftover | list | none | 69% | 74% | 21/23 | +13.05 | 0 | 0 | +4.63 | -3.78 | +26.51 | — | PASS | 78.667 |
| `combo_sh_5050_shared` | mix | 5 | leftover | list | none | 61% | 74% | 21/23 | +10.28 | 0 | 0 | +5.70 | -3.90 | +41.54 | — | PASS | 78.627 |
| `combo_sh_7030_shared` | mix | 5 | leftover | list | none | 61% | 78% | 21/23 | +8.57 | 0 | 0 | +5.68 | -3.96 | +33.07 | — | PASS | 78.303 |
| `combo_ps_7030_shared` | mix | 5 | leftover | list | none | 66% | 74% | 21/23 | +15.15 | 0 | 0 | +4.69 | -3.72 | +28.80 | — | PASS | 78.151 |
| `combo_sh_3070_shared` | mix | 5 | leftover | list | none | 60% | 70% | 21/23 | +9.35 | 0 | 0 | +5.75 | -3.81 | +40.84 | — | PASS | 77.467 |
| `combo_seh_333_skip` | mix | 5 | leftover | list | none | 52% | 74% | 21/23 | +6.06 | 0 | 0 | +6.46 | -6.41 | +32.71 | — | PASS | 70.162 |
| `combo_seh_451540_shared` | mix | 5 | leftover | list | none | 48% | 78% | 21/23 | +8.13 | 0 | 0 | +6.63 | -6.44 | +34.46 | — | PASS | 69.953 |
| `combo_seh_601525_shared` | mix | 5 | leftover | list | none | 48% | 78% | 21/23 | +6.92 | 0 | 0 | +6.62 | -6.47 | +30.78 | — | PASS | 69.361 |
| `combo_seh_502525_shared` | mix | 5 | leftover | list | none | 49% | 70% | 21/23 | +5.50 | 0 | 0 | +6.61 | -6.33 | +30.74 | — | PASS | 67.899 |
| `combo_sj_5050_shared` | mix | 5 | leftover | list | none | 56% | 70% | 16/23 | +4.70 | 0 | 0 | +5.12 | -4.39 | +21.33 | — | PASS | 65.219 |
| `combo_sn_5050_shared` | mix | 5 | leftover | list | none | 59% | 70% | 16/23 | +3.67 | 0 | 0 | +4.42 | -4.54 | +12.81 | — | PASS | 64.965 |
| `combo_sj_3070_shared` | mix | 5 | leftover | list | none | 56% | 65% | 16/23 | +2.01 | 0 | 0 | +5.11 | -4.38 | +20.70 | — | PASS | 64.254 |
| `combo_se1_5050_shared` | mix | 5 | leftover | list | none | 58% | 70% | 16/23 | +4.70 | 0 | 0 | +4.99 | -6.24 | +24.66 | — | PASS | 64.137 |
| `combo_e1s_7030_shared` | mix | 5 | leftover | list | none | 57% | 65% | 16/23 | +2.89 | 0 | 0 | +5.10 | -6.04 | +21.26 | — | PASS | 62.576 |
| `combo_jse_333_shared` | mix | 5 | leftover | list | none | 56% | 61% | 15/23 | +2.37 | 0 | 0 | +5.82 | -5.81 | +31.46 | — | PASS | 62.149 |
| `combo_sj_7030_shared` | mix | 5 | leftover | list | none | 54% | 61% | 16/23 | +4.92 | 0 | 0 | +5.16 | -4.41 | +16.73 | — | PASS | 62.0 |
| `combo_snj_333_shared` | mix | 5 | leftover | list | none | 52% | 61% | 16/23 | +1.80 | 0 | 0 | +4.62 | -4.60 | +14.93 | — | PASS | 61.779 |
| `combo_ej_5050_shared` | mix | 5 | leftover | list | none | 47% | 52% | 19/23 | +2.32 | 0 | 0 | +6.25 | -6.21 | +30.45 | — | PASS | 61.696 |
| `combo_es_9010_shared` | mix | 5 | leftover | list | none | 45% | 61% | 20/23 | +4.51 | 0 | 0 | +7.20 | -6.64 | +28.70 | — | PASS | 61.095 |
| `combo_es_8020_shared` | mix | 5 | leftover | list | none | 48% | 57% | 20/23 | +4.66 | 0 | 0 | +6.78 | -7.06 | +29.06 | — | PASS | 60.907 |
| `combo_se_5050_skip` | mix | 5 | leftover | list | none | 61% | 57% | 16/23 | +3.84 | 0 | 0 | +6.18 | -7.82 | +28.83 | — | PASS | 60.823 |
| `combo_se_3070_shared` | mix | 5 | leftover | list | none | 51% | 52% | 20/23 | +3.89 | 0 | 0 | +6.60 | -7.37 | +28.16 | — | PASS | 60.522 |
| `combo_jer_5050_shared` | mix | 5 | leftover | list | none | 43% | 52% | 17/23 | +1.54 | 0 | 0 | +6.52 | -6.46 | +28.44 | — | PASS | 57.38 |
| `combo_je1_5050_shared` | mix | 5 | leftover | list | none | 50% | 43% | 14/23 | +0.89 | 0 | 0 | +4.76 | -5.27 | +12.79 | — | PASS | 55.033 |
| `combo_ser_5050_shared` | mix | 5 | leftover | list | none | 53% | 43% | 16/23 | +2.70 | 0 | 0 | +6.48 | -7.68 | +24.31 | — | PASS | 54.263 |
| `combo_ers_7030_shared` | mix | 5 | leftover | list | none | 48% | 43% | 15/23 | +2.55 | 0 | 0 | +6.69 | -8.01 | +25.09 | — | PASS | 51.255 |
| `union_news_pack_net2_h1` | long | 1 | leftover | list | none | 60% | 48% | 21/23 | +17.85 | 1 | 6 | +3.98 | -2.68 | +28.55 | +22.32 | PASS | 75.692 |
| `union_hot_n4_h1` | long | 1 | leftover | list | none | 54% | 48% | 21/23 | +12.61 | 15 | 27 | +6.18 | -4.16 | +32.21 | +51.98 | PASS | 72.958 |
| `union_news_pack_h1` | long | 1 | leftover | list | none | 58% | 48% | 21/23 | +14.36 | 2 | 9 | +3.77 | -3.37 | +24.09 | +21.62 | PASS | 72.17 |
| `combo_hn_7030_shared` | mix | 5 | leftover | list | none | 52% | 48% | 21/23 | +7.83 | 0 | 0 | +4.53 | -4.53 | +23.55 | — | PASS | 68.287 |
| `combo_hn_5050_shared` | mix | 5 | leftover | list | none | 53% | 48% | 21/23 | +4.57 | 0 | 0 | +4.57 | -4.51 | +18.62 | — | PASS | 67.978 |
| `combo_seh_404020_shared` | mix | 5 | leftover | list | none | 50% | 70% | 21/23 | +5.22 | 0 | 0 | +6.43 | -6.42 | +29.14 | — | PASS | 67.907 |
| `combo_seh_333_shared` | mix | 5 | leftover | list | none | 49% | 70% | 21/23 | +6.06 | 0 | 0 | +6.52 | -6.36 | +29.78 | — | PASS | 67.816 |
| `combo_seh_333_weather` | mix | 5 | leftover | list | none | 49% | 70% | 21/23 | +6.06 | 0 | 0 | +6.52 | -6.36 | +29.78 | — | PASS | 67.816 |
| `combo_seh_403525_shared` | mix | 5 | leftover | list | none | 49% | 70% | 21/23 | +5.70 | 0 | 0 | +6.64 | -6.26 | +29.64 | — | PASS | 67.814 |
| `combo_seh_502525_split` | mix | 5 | leftover | list | none | 48% | 74% | 21/23 | +5.00 | 0 | 0 | +6.81 | -5.87 | +18.41 | — | PASS | 67.301 |
| `combo_hn_3070_shared` | mix | 5 | leftover | list | none | 53% | 57% | 19/23 | +2.51 | 0 | 0 | +4.60 | -4.49 | +13.89 | — | PASS | 67.054 |
| `union_news_pack_net3_h1` | long | 1 | leftover | list | none | 58% | 39% | 17/23 | +9.19 | 1 | 6 | +3.77 | -2.68 | +19.12 | +17.01 | PASS | 67.017 |
| `combo_seh_333_split` | mix | 5 | leftover | list | none | 50% | 65% | 21/23 | +6.30 | 0 | 0 | +6.51 | -6.15 | +21.58 | — | PASS | 66.028 |
| `combo_he1_5050_shared` | mix | 5 | leftover | list | none | 50% | 48% | 21/23 | +6.79 | 0 | 0 | +5.20 | -5.49 | +20.20 | — | PASS | 65.678 |
| `combo_ehs_601525_shared` | mix | 5 | leftover | list | none | 46% | 61% | 21/23 | +5.85 | 0 | 0 | +6.81 | -6.06 | +27.55 | — | PASS | 64.843 |
| `combo_hj_5050_shared` | mix | 5 | leftover | list | none | 47% | 43% | 20/23 | +5.52 | 0 | 0 | +5.11 | -4.14 | +16.79 | — | PASS | 63.975 |
| `combo_sn_3070_shared` | mix | 5 | leftover | list | none | 58% | 65% | 16/23 | +1.92 | 0 | 0 | +4.52 | -4.37 | +11.65 | — | PASS | 63.619 |
| `union_w_hot_cond_h1` | long | 1 | leftover | list | none | 51% | 48% | 20/23 | +4.27 | 23 | 45 | +5.41 | -6.01 | +3.80 | +19.66 | PASS | 63.575 |
| `combo_sn_7030_shared` | mix | 5 | leftover | list | none | 59% | 65% | 16/23 | +4.30 | 0 | 0 | +4.27 | -4.74 | +11.21 | — | PASS | 63.5 |
| `union_w_hot_candle_h1` | long | 1 | leftover | list | none | 54% | 43% | 19/23 | +3.78 | 22 | 45 | +4.76 | -5.36 | +5.92 | +25.97 | PASS | 63.098 |
| `combo_ehs_702010_shared` | mix | 5 | leftover | list | none | 40% | 57% | 21/23 | +6.88 | 0 | 0 | +7.56 | -5.62 | +27.17 | — | PASS | 62.795 |
| `union_e_fresh_h3` | long | 3 | leftover | list | none | 47% | 61% | 20/23 | +5.82 | 19 | 81 | +9.39 | -7.45 | +27.98 | -13.56 | PASS | 62.151 |
| `combo_eh_5050_shared` | mix | 5 | leftover | list | none | 40% | 52% | 21/23 | +8.89 | 0 | 0 | +7.41 | -6.26 | +27.16 | — | PASS | 62.143 |
| `union_hot_score_h1` | long | 1 | leftover | list | none | 52% | 48% | 18/23 | +2.26 | 26 | 51 | +5.89 | -6.31 | +5.23 | +27.21 | PASS | 61.809 |
| `combo_eh_7030_shared` | mix | 5 | leftover | list | none | 35% | 52% | 21/23 | +7.44 | 0 | 0 | +8.32 | -5.89 | +26.66 | — | PASS | 61.245 |
| `combo_se_5050_split` | mix | 5 | leftover | list | none | 49% | 61% | 20/23 | +5.32 | 0 | 0 | +7.15 | -6.43 | +19.01 | — | PASS | 61.217 |
| `combo_her_5050_shared` | mix | 5 | leftover | list | none | 42% | 57% | 21/23 | +7.55 | 0 | 0 | +7.00 | -7.87 | +22.20 | — | PASS | 61.016 |
| `union_hot_score_h3` | long | 3 | leftover | list | none | 46% | 61% | 20/23 | +4.12 | 39 | 121 | +12.32 | -9.09 | +13.51 | +235.30 | PASS | 61.009 |
| `short_news_head_h3` | short | 3 | leftover | list | none | 59% | 65% | 16/23 | +4.87 | 5 | 32 | +6.43 | -4.82 | +10.85 | +20.59 | PASS | 60.874 |
| `combo_ee1_3070_shared` | mix | 5 | leftover | list | none | 36% | 61% | 20/23 | +5.82 | 0 | 0 | +9.39 | -7.45 | +27.98 | — | PASS | 60.527 |
| `combo_ee1_5050_shared` | mix | 5 | leftover | list | none | 36% | 61% | 20/23 | +5.82 | 0 | 0 | +9.39 | -7.45 | +27.98 | — | PASS | 60.527 |
| `combo_ee1_7030_shared` | mix | 5 | leftover | list | none | 36% | 61% | 20/23 | +5.82 | 0 | 0 | +9.39 | -7.45 | +27.98 | — | PASS | 60.527 |
| `union_vol_green_h1` | long | 1 | leftover | list | none | 45% | 43% | 19/23 | +5.36 | 16 | 40 | +5.29 | -5.10 | +12.95 | -0.59 | PASS | 60.408 |
| `combo_eh_3070_shared` | mix | 5 | leftover | list | none | 41% | 43% | 21/23 | +10.04 | 0 | 0 | +7.13 | -6.55 | +27.20 | — | PASS | 60.329 |
| `short_news_or_h3` | short | 3 | leftover | list | none | 58% | 57% | 16/23 | +6.19 | 7 | 37 | +5.30 | -4.34 | +12.47 | +22.98 | PASS | 59.664 |
| `short_news_r_h3` | short | 3 | leftover | list | none | 58% | 57% | 16/23 | +6.19 | 7 | 37 | +5.30 | -4.34 | +12.47 | +22.98 | PASS | 59.664 |
| `combo_se_5050_shared` | mix | 5 | leftover | list | none | 57% | 57% | 16/23 | +3.84 | 0 | 0 | +6.39 | -7.35 | +26.87 | — | PASS | 59.19 |
| `combo_se_5050_weather` | mix | 5 | leftover | list | none | 57% | 57% | 16/23 | +3.84 | 0 | 0 | +6.39 | -7.35 | +26.87 | — | PASS | 59.19 |
| `combo_nse_333_shared` | mix | 5 | leftover | list | none | 50% | 48% | 16/23 | +2.34 | 0 | 0 | +5.47 | -6.28 | +23.84 | — | PASS | 56.702 |
| `union_news_or_h1` | long | 1 | leftover | list | none | 53% | 39% | 14/23 | +0.96 | 4 | 26 | +4.18 | -3.96 | +2.48 | +20.27 | PASS | 56.555 |
| `short_news_r_h1` | short | 1 | leftover | list | none | 52% | 39% | 16/23 | +1.22 | 4 | 16 | +2.97 | -3.18 | +0.49 | +9.31 | PASS | 56.406 |
| `union_e_fresh_h1` | long | 1 | leftover | list | none | 48% | 43% | 17/23 | +0.72 | 8 | 42 | +4.76 | -6.44 | +9.66 | +6.47 | PASS | 55.76 |
| `union_break10_h1` | long | 1 | leftover | list | none | 49% | 39% | 15/23 | +1.12 | 19 | 42 | +5.20 | -5.38 | -1.88 | +8.03 | PASS | 55.087 |
| `union_news_g_cond_h1` | long | 1 | leftover | list | none | 52% | 35% | 14/23 | +0.04 | 4 | 27 | +4.18 | -4.27 | +1.63 | +19.62 | PASS | 54.735 |
| `combo_se_7030_shared` | mix | 5 | leftover | list | none | 58% | 52% | 13/23 | +1.24 | 0 | 0 | +6.27 | -7.32 | +22.97 | — | PASS | 54.714 |
| `union_w_hot_candle_h3` | long | 3 | leftover | list | none | 48% | 57% | 16/23 | +1.78 | 34 | 112 | +8.70 | -9.55 | +9.06 | +203.53 | PASS | 54.551 |
| `union_join_vol_green_h1` | long | 1 | leftover | list | none | 46% | 35% | 15/23 | +0.11 | 9 | 30 | +4.92 | -4.26 | +10.39 | +4.66 | PASS | 54.45 |
| `combo_en_7030_shared` | mix | 5 | leftover | list | none | 39% | 48% | 16/23 | +1.53 | 0 | 0 | +6.03 | -6.22 | +23.55 | — | PASS | 53.857 |
| `union_earn_react_h3` | long | 3 | leftover | list | none | 41% | 52% | 19/23 | +2.86 | 15 | 80 | +8.74 | -9.31 | +21.91 | -24.30 | PASS | 53.721 |
| `union_news_pack_h3` | long | 3 | leftover | list | none | 47% | 35% | 19/23 | +6.37 | 5 | 21 | +5.46 | -7.33 | -5.11 | -3.09 | PASS | 52.925 |
| `combo_en_5050_shared` | mix | 5 | leftover | list | none | 44% | 48% | 14/23 | +0.15 | 0 | 0 | +5.54 | -6.48 | +21.50 | — | PASS | 52.598 |
| `combo_ef_7030_shared` | mix | 5 | leftover | list | none | 42% | 61% | 16/23 | +1.03 | 0 | 0 | +8.07 | -8.22 | +15.73 | — | PASS | 52.547 |
| `union_ret_5_h1` | long | 1 | leftover | list | none | 49% | 39% | 12/23 | +3.39 | 19 | 55 | +6.11 | -5.59 | +6.03 | +15.45 | PASS | 52.368 |
| `union_w_hot_cond_h3` | long | 3 | leftover | list | none | 45% | 57% | 14/23 | +1.18 | 36 | 119 | +10.10 | -8.59 | +10.04 | +203.87 | PASS | 51.953 |
| `union_ret_5_h3` | long | 3 | leftover | list | none | 47% | 57% | 13/23 | +2.66 | 33 | 124 | +9.90 | -8.69 | +14.79 | +47.04 | PASS | 51.255 |
| `union_vol_g_h3` | long | 3 | leftover | list | none | 50% | 48% | 16/23 | +1.66 | 23 | 97 | +8.66 | -8.55 | -1.78 | +27.69 | PASS | 50.892 |
| `union_break10_h3` | long | 3 | leftover | list | none | 49% | 48% | 14/23 | +1.14 | 28 | 100 | +8.55 | -8.08 | +0.72 | +35.31 | PASS | 50.356 |
| `short_last_red_h3` | short | 3 | leftover | list | none | 52% | 43% | 14/23 | +1.24 | 16 | 99 | +4.88 | -6.13 | -4.30 | -23.13 | PASS | 48.465 |
| `short_alarm_h1` | short | 1 | leftover | list | none | 51% | 17% | 12/23 | +0.32 | 7 | 40 | +4.60 | -2.94 | +1.71 | +1.58 | PASS | 48.42 |
| `short_alarm_h3` | short | 3 | leftover | list | none | 54% | 35% | 12/23 | +1.35 | 11 | 74 | +7.29 | -8.49 | +2.59 | +7.52 | PASS | 43.785 |
| `union_hot_n12_h1` | long | 1 | leftover | list | none | 52% | 43% | 10/23 | +0.00 | 32 | 74 | +4.89 | -5.73 | -1.54 | +20.01 | PASS | 40.888 |
| `combo_hf_5050_shared` | mix | 5 | leftover | list | none | 49% | 57% | 10/23 | +0.00 | 0 | 0 | +6.39 | -6.02 | +7.19 | — | PASS | 40.742 |
| `union_vol_ab_h1` | long | 1 | leftover | list | none | 53% | 30% | 11/23 | +0.00 | 12 | 33 | +5.20 | -4.76 | +1.51 | +54.65 | PASS | 40.397 |
| `union_news_g_h1` | long | 1 | leftover | list | none | 53% | 39% | 9/23 | -0.28 | 3 | 26 | +3.97 | -4.40 | +2.98 | +18.27 | PASS | 40.321 |
| `union_candle_score_h1` | long | 1 | leftover | list | none | 53% | 26% | 10/23 | +0.00 | 16 | 44 | +4.60 | -4.11 | +1.27 | +10.26 | PASS | 39.701 |
| `flatten_live_h1_topheavy` | long | 1 | topheavy | list | none | 54% | 13% | 7/23 | +0.00 | 1 | 5 | +6.75 | -3.75 | +7.19 | +4.45 | PASS | 38.421 |
| `combo_nj_5050_shared` | mix | 5 | leftover | list | none | 48% | 35% | 10/23 | +0.00 | 0 | 0 | +4.33 | -4.54 | +3.95 | — | PASS | 38.365 |
| `combo_sf_5050_shared` | mix | 5 | leftover | list | none | 57% | 61% | 6/23 | -5.75 | 0 | 0 | +7.05 | -6.63 | +9.74 | — | PASS | 37.455 |
| `flatten_live_h1` | long | 1 | leftover | list | none | 54% | 13% | 7/23 | +0.00 | 1 | 5 | +6.31 | -4.05 | +8.22 | +4.45 | PASS | 37.38 |
| `flatten_live_h1_cut` | long | 1 | leftover | cut_loser | none | 54% | 13% | 7/23 | +0.00 | 1 | 5 | +6.31 | -4.05 | +8.22 | +4.45 | PASS | 37.38 |
| `flatten_live_h1_sboost` | long | 1 | leftover | list | both | 54% | 13% | 7/23 | +0.00 | 1 | 5 | +6.31 | -4.05 | +8.22 | +4.45 | PASS | 37.38 |
| `flatten_live_h1_sizeup` | long | 1 | leftover | list | sizeup | 54% | 13% | 7/23 | +0.00 | 1 | 5 | +6.31 | -4.05 | +8.22 | +4.45 | PASS | 37.38 |
| `flatten_live_h1_time` | long | 1 | leftover | time | none | 54% | 13% | 7/23 | +0.00 | 1 | 5 | +6.31 | -4.05 | +8.22 | +4.45 | PASS | 37.38 |
| `flatten_live_h1_trail` | long | 1 | leftover | trail | none | 54% | 13% | 7/23 | +0.00 | 1 | 5 | +6.31 | -4.05 | +8.22 | +4.45 | PASS | 37.38 |
| `flatten_live_h1_half` | long | 1 | half | list | none | 54% | 13% | 7/23 | +0.00 | 1 | 5 | +6.58 | -3.90 | +3.74 | +4.45 | PASS | 37.329 |
| `combo_sf_7030_shared` | mix | 5 | leftover | list | none | 57% | 61% | 6/23 | -2.95 | 0 | 0 | +6.61 | -6.78 | +11.48 | — | PASS | 37.276 |
| `yday_gainer_h1` | long | 1 | leftover | list | none | 52% | 35% | 7/23 | -0.08 | 12 | 53 | +4.85 | -3.96 | +6.12 | +19.13 | PASS | 37.089 |
| `union_flow_in_h1` | long | 1 | leftover | list | none | 46% | 35% | 6/23 | -2.10 | 4 | 6 | +4.41 | -4.60 | +5.18 | +12.48 | PASS | 36.455 |
| `union_h1_topheavy` | long | 1 | topheavy | list | none | 46% | 43% | 6/23 | -3.08 | 7 | 38 | +4.29 | -4.18 | +5.29 | +12.60 | PASS | 35.836 |
| `flatten_live_h1_rankw` | long | 1 | rank_w | list | none | 54% | 13% | 6/23 | +0.00 | 1 | 5 | +6.14 | -4.12 | +4.03 | +4.45 | PASS | 35.323 |
| `flatten_h1` | long | 1 | leftover | list | none | 45% | 39% | 6/23 | -4.58 | 3 | 26 | +4.44 | -3.98 | +2.30 | -0.77 | PASS | 34.956 |
| `union_white_h1` | long | 1 | leftover | list | none | 40% | 35% | 9/23 | -0.20 | 2 | 18 | +5.79 | -5.12 | +3.30 | -1.22 | PASS | 34.712 |
| `union_last_green_h1` | long | 1 | leftover | list | none | 48% | 30% | 5/23 | -1.92 | 9 | 35 | +4.47 | -3.57 | +2.76 | +13.27 | PASS | 34.301 |
| `union_earn_react_h1` | long | 1 | leftover | list | none | 48% | 30% | 11/23 | +0.00 | 7 | 46 | +4.83 | -7.92 | +1.92 | +1.69 | PASS | 33.559 |
| `combo_ne1_5050_shared` | mix | 5 | leftover | list | none | 51% | 35% | 5/23 | -1.81 | 0 | 0 | +4.17 | -5.50 | +8.27 | — | PASS | 33.234 |
| `union_news_g_conv_h1` | long | 1 | conviction | list | none | 53% | 39% | 2/23 | -7.88 | 1 | 13 | +3.78 | -4.72 | +1.09 | +14.82 | PASS | 32.46 |
| `combo_ner_5050_shared` | mix | 5 | leftover | list | none | 42% | 48% | 6/23 | -0.56 | 0 | 0 | +5.66 | -6.80 | +19.38 | — | PASS | 32.344 |
| `short_rsi_ob_h3` | short | 3 | leftover | list | none | 49% | 39% | 10/23 | +0.00 | 26 | 99 | +6.13 | -7.34 | -5.27 | -127.06 | PASS | 32.2 |
| `union_h1` | long | 1 | leftover | list | none | 46% | 39% | 4/23 | -3.58 | 7 | 38 | +4.25 | -4.21 | +1.40 | +12.60 | PASS | 32.138 |
| `union_h1_sizeup` | long | 1 | leftover | list | sizeup | 46% | 39% | 4/23 | -3.58 | 7 | 38 | +4.25 | -4.21 | +1.40 | +12.60 | PASS | 32.138 |
| `union_h1_trail` | long | 1 | leftover | trail | none | 46% | 39% | 4/23 | -3.58 | 7 | 38 | +4.25 | -4.21 | +1.40 | +12.60 | PASS | 32.138 |
| `union_h1_sboost` | long | 1 | leftover | list | both | 46% | 39% | 4/23 | -3.58 | 7 | 38 | +4.22 | -4.28 | +1.46 | +12.60 | PASS | 32.022 |
| `union_vol_green_h3` | long | 3 | leftover | list | none | 50% | 30% | 11/23 | +0.00 | 27 | 83 | +6.73 | -8.64 | -8.81 | +23.82 | PASS | 31.469 |
| `union_join_present_h1` | long | 1 | leftover | list | none | 46% | 39% | 4/23 | -3.58 | 5 | 42 | +4.25 | -4.29 | +0.99 | +8.17 | PASS | 31.379 |
| `ohlc_hot_coil_h1` | long | 1 | leftover | list | none | 51% | 35% | 5/23 | -5.85 | 11 | 21 | +2.97 | -3.99 | -18.58 | -5.34 | PASS | 31.342 |
| `union_vol_g_h1` | long | 1 | leftover | list | none | 50% | 35% | 5/23 | -1.12 | 13 | 52 | +5.67 | -5.59 | -1.15 | +15.90 | PASS | 31.322 |
| `union_h1_cut` | long | 1 | leftover | cut_loser | none | 46% | 35% | 4/23 | -3.65 | 7 | 38 | +4.25 | -4.14 | +1.21 | +12.60 | PASS | 31.316 |
| `union_vol_ab_h3` | long | 3 | leftover | list | none | 46% | 35% | 10/23 | +0.00 | 17 | 69 | +7.40 | -7.28 | -0.28 | +56.06 | PASS | 30.968 |
| `combo_en_3070_shared` | mix | 5 | leftover | list | none | 43% | 39% | 5/23 | -1.38 | 0 | 0 | +5.62 | -6.33 | +18.66 | — | PASS | 30.589 |
| `union_join_g_h1` | long | 1 | leftover | list | none | 47% | 35% | 4/23 | -3.50 | 7 | 45 | +4.05 | -4.50 | +0.43 | +5.11 | PASS | 30.418 |
| `combo_fh_7030_shared` | mix | 5 | leftover | list | none | 47% | 61% | 1/23 | -3.80 | 0 | 0 | +6.51 | -6.24 | +1.16 | — | PASS | 30.203 |
| `union_news_or_net2_h1` | long | 1 | leftover | list | none | 47% | 30% | 4/23 | -2.37 | 1 | 19 | +4.24 | -4.22 | -2.37 | +3.20 | PASS | 30.101 |
| `combo_fse_333_shared` | mix | 5 | leftover | list | none | 49% | 61% | 3/23 | -2.73 | 0 | 0 | +6.67 | -7.67 | +10.66 | — | PASS | 30.062 |
| `combo_sf_3070_shared` | mix | 5 | leftover | list | none | 55% | 57% | 1/23 | -6.70 | 0 | 0 | +7.39 | -6.84 | +7.66 | — | PASS | 30.006 |
| `union_flow_in_h3` | long | 3 | leftover | list | none | 48% | 35% | 9/23 | -0.43 | 5 | 25 | +1.89 | -4.94 | -1.43 | -3.19 | PASS | 29.828 |
| `union_h1_rankw` | long | 1 | rank_w | list | none | 46% | 43% | 1/23 | -3.59 | 7 | 38 | +4.27 | -4.22 | +0.37 | +12.60 | PASS | 29.594 |
| `union_candle_h1` | long | 1 | leftover | list | none | 45% | 26% | 4/23 | -2.52 | 7 | 41 | +5.03 | -4.19 | -1.54 | +4.80 | PASS | 29.444 |
| `combo_jf_5050_shared` | mix | 5 | leftover | list | none | 45% | 61% | 1/23 | -5.38 | 0 | 0 | +6.18 | -5.62 | +3.60 | — | PASS | 29.282 |
| `union_candle_score_h3` | long | 3 | leftover | list | none | 46% | 43% | 6/23 | -0.45 | 22 | 111 | +6.86 | -6.53 | +4.00 | +18.39 | PASS | 29.056 |
| `union_h3_cut` | long | 3 | leftover | cut_loser | none | 48% | 52% | 0/23 | -6.44 | 16 | 89 | +8.80 | -5.41 | -1.94 | +14.69 | PASS | 29.047 |
| `union_h3_trail` | long | 3 | leftover | trail | none | 48% | 52% | 0/23 | -6.44 | 16 | 89 | +8.80 | -5.41 | -1.94 | +14.69 | PASS | 29.047 |
| `combo_nf_5050_shared` | mix | 5 | leftover | list | none | 45% | 57% | 1/23 | -6.56 | 0 | 0 | +6.77 | -5.88 | +2.83 | — | PASS | 29.007 |
| `probable_probable_ok_h1` | long | 1 | leftover | list | none | 52% | 26% | 1/23 | -0.58 | 6 | 19 | +3.50 | -3.15 | -1.51 | +9.91 | PASS | 28.937 |
| `union_news_head_h1` | long | 1 | leftover | list | none | 51% | 26% | 2/23 | -2.32 | 4 | 20 | +4.20 | -4.37 | -5.89 | +16.01 | PASS | 28.785 |
| `probable_h1` | long | 1 | leftover | list | none | 51% | 26% | 3/23 | -1.65 | 11 | 52 | +3.95 | -3.65 | -1.65 | +15.55 | PASS | 28.662 |
| `short_last_red_h1` | short | 1 | leftover | list | none | 48% | 22% | 7/23 | -0.18 | 9 | 59 | +3.95 | -4.50 | -8.96 | -13.50 | PASS | 28.533 |
| `union_h3_time` | long | 3 | leftover | time | none | 48% | 52% | 0/23 | -6.43 | 16 | 89 | +8.04 | -5.52 | -1.47 | +14.69 | PASS | 28.261 |
| `union_h3` | long | 3 | leftover | list | none | 48% | 52% | 0/23 | -7.75 | 16 | 89 | +8.80 | -5.82 | -3.46 | +14.69 | PASS | 28.242 |
| `union_h3_sizeup` | long | 3 | leftover | list | sizeup | 48% | 52% | 0/23 | -7.75 | 16 | 89 | +8.80 | -5.82 | -3.46 | +14.69 | PASS | 28.242 |
| `union_h1_time` | long | 1 | leftover | time | none | 46% | 35% | 1/23 | -3.65 | 7 | 38 | +4.10 | -3.81 | +0.61 | +12.60 | PASS | 28.21 |
| `combo_fes_403030_shared` | mix | 5 | leftover | list | none | 47% | 61% | 2/23 | -3.01 | 0 | 0 | +6.92 | -7.48 | +8.71 | — | PASS | 28.185 |
| `union_macd_up_h1` | long | 1 | leftover | list | none | 44% | 30% | 3/23 | -3.46 | 11 | 44 | +4.52 | -4.21 | -2.04 | +10.10 | PASS | 28.13 |
| `union_news_or_net4_conv_h1` | long | 1 | conviction | list | none | 49% | 39% | 0/23 | -12.24 | 1 | 9 | +3.50 | -4.52 | -3.84 | +0.19 | PASS | 27.978 |
| `union_h3_sboost` | long | 3 | leftover | list | both | 48% | 52% | 0/23 | -7.75 | 16 | 89 | +8.46 | -5.87 | -3.52 | +14.69 | PASS | 27.874 |
| `union_h3_half` | long | 3 | half | list | none | 48% | 48% | 0/23 | -6.55 | 16 | 89 | +8.37 | -5.10 | -5.34 | +14.69 | PASS | 27.743 |
| `union_blue_h1` | long | 1 | leftover | list | none | 44% | 26% | 3/23 | -2.54 | 7 | 34 | +4.16 | -4.07 | -0.79 | +0.16 | PASS | 27.37 |
| `union_white_coil_h1` | long | 1 | leftover | list | none | 43% | 26% | 4/23 | -5.67 | 3 | 18 | +4.43 | -3.84 | -6.14 | -3.25 | PASS | 27.034 |
| `flatten_h3_sboost` | long | 3 | leftover | list | both | 47% | 43% | 0/23 | -7.66 | 11 | 64 | +7.81 | -4.93 | -4.47 | +5.75 | PASS | 26.972 |
| `flatten_h3` | long | 3 | leftover | list | none | 47% | 43% | 0/23 | -7.66 | 11 | 64 | +8.12 | -5.21 | -4.41 | +5.75 | PASS | 26.85 |
| `flatten_h3_cut` | long | 3 | leftover | cut_loser | none | 47% | 43% | 0/23 | -7.66 | 11 | 64 | +8.12 | -5.21 | -4.41 | +5.75 | PASS | 26.85 |
| `flatten_h3_sizeup` | long | 3 | leftover | list | sizeup | 47% | 43% | 0/23 | -7.66 | 11 | 64 | +8.12 | -5.21 | -4.41 | +5.75 | PASS | 26.85 |
| `flatten_h3_trail` | long | 3 | leftover | trail | none | 47% | 43% | 0/23 | -7.66 | 11 | 64 | +8.12 | -5.21 | -4.41 | +5.75 | PASS | 26.85 |
| `union_blue_vol_h1` | long | 1 | leftover | list | none | 45% | 30% | 4/23 | -2.10 | 7 | 40 | +5.57 | -5.47 | -4.74 | +8.18 | PASS | 26.752 |
| `flatten_h3_half` | long | 3 | half | list | none | 47% | 43% | 0/23 | -4.05 | 11 | 64 | +7.14 | -4.76 | -3.15 | +5.75 | PASS | 26.739 |
| `union_h3_rankw` | long | 3 | rank_w | list | none | 48% | 48% | 0/23 | -8.55 | 16 | 89 | +8.61 | -5.78 | -7.33 | +14.69 | PASS | 26.68 |
| `union_cond_h1` | long | 1 | leftover | list | none | 49% | 30% | 0/23 | -4.13 | 4 | 36 | +4.24 | -4.29 | -4.26 | +3.27 | PASS | 26.372 |
| `union_h1_half` | long | 1 | half | list | none | 46% | 35% | 0/23 | -1.75 | 7 | 38 | +4.09 | -4.31 | -0.67 | +12.60 | PASS | 26.291 |
| `flatten_h3_time` | long | 3 | leftover | time | none | 47% | 43% | 0/23 | -6.96 | 11 | 64 | +7.37 | -5.45 | -1.40 | +5.75 | PASS | 26.272 |
| `union_e_green_h3` | long | 3 | leftover | list | none | 40% | 39% | 11/23 | +0.00 | 6 | 45 | +5.07 | -8.76 | -0.34 | -19.21 | PASS | 26.161 |
| `flatten_h5` | long | 5 | leftover | list | none | 45% | 61% | 2/23 | -7.33 | 19 | 95 | +9.13 | -9.26 | +1.28 | +5.37 | PASS | 26.127 |
| `flatten_h5_cut` | long | 5 | leftover | cut_loser | none | 45% | 61% | 2/23 | -7.33 | 19 | 95 | +9.13 | -9.26 | +1.28 | +5.37 | PASS | 26.127 |
| `flatten_h5_sizeup` | long | 5 | leftover | list | sizeup | 45% | 61% | 2/23 | -7.33 | 19 | 95 | +9.13 | -9.26 | +1.28 | +5.37 | PASS | 26.127 |
| `flatten_h5_time` | long | 5 | leftover | time | none | 45% | 61% | 2/23 | -7.33 | 19 | 95 | +9.13 | -9.26 | +1.28 | +5.37 | PASS | 26.127 |
| `flatten_h5_trail` | long | 5 | leftover | trail | none | 45% | 61% | 2/23 | -7.33 | 19 | 95 | +9.13 | -9.26 | +1.28 | +5.37 | PASS | 26.127 |
| `union_join_vol_green_h3` | long | 3 | leftover | list | none | 46% | 35% | 7/23 | -0.00 | 17 | 63 | +6.36 | -8.43 | -7.02 | +15.45 | PASS | 25.994 |
| `union_rsi_os_h3` | long | 3 | leftover | list | none | 51% | 39% | 10/23 | +0.00 | 2 | 31 | +3.62 | -6.02 | -7.33 | -19.79 | PASS | 25.99 |
| `union_news_vol_h1` | long | 1 | leftover | list | none | 49% | 26% | 0/23 | -2.52 | 5 | 16 | +4.36 | -4.82 | -0.26 | +18.37 | PASS | 25.948 |
| `probable_probable_ok_h3` | long | 3 | leftover | list | none | 54% | 30% | 5/23 | -1.46 | 15 | 46 | +5.80 | -8.29 | -18.50 | +9.87 | PASS | 25.717 |
| `union_e_green_h1` | long | 1 | leftover | list | none | 48% | 35% | 5/23 | -3.13 | 4 | 18 | +4.19 | -9.48 | -22.39 | -16.29 | PASS | 25.703 |
| `union_join_present_h3` | long | 3 | leftover | list | none | 45% | 43% | 0/23 | -6.44 | 14 | 92 | +9.71 | -5.72 | -3.12 | +8.12 | PASS | 25.683 |
| `union_coil_green_h1` | long | 1 | leftover | list | none | 45% | 35% | 0/23 | -4.02 | 12 | 33 | +3.18 | -3.97 | -2.46 | -0.96 | PASS | 25.469 |
| `union_h3_exit_news_r` | long | 3 | leftover | list | none | 45% | 43% | 0/23 | -7.00 | 14 | 92 | +9.71 | -5.78 | -4.27 | +5.36 | PASS | 25.425 |
| `flatten_h3_rankw` | long | 3 | rank_w | list | none | 47% | 39% | 0/23 | -6.87 | 11 | 64 | +7.94 | -5.38 | -5.67 | +5.75 | PASS | 25.372 |
| `yday_gainer_h3` | long | 3 | leftover | list | none | 51% | 39% | 1/23 | -1.53 | 23 | 100 | +9.02 | -7.09 | -5.94 | +30.86 | PASS | 25.003 |
| `union_h3_topheavy` | long | 3 | topheavy | list | none | 48% | 43% | 0/23 | -6.47 | 16 | 89 | +7.05 | -5.99 | -2.78 | +14.69 | PASS | 24.932 |
| `union_vol_g_h5` | long | 5 | leftover | list | none | 48% | 39% | 6/23 | -4.04 | 29 | 114 | +13.48 | -10.84 | -13.31 | +11.52 | PASS | 24.879 |
| `short_rsi_ob_h1` | short | 1 | leftover | list | none | 48% | 35% | 0/23 | -2.25 | 13 | 46 | +4.44 | -4.93 | -9.91 | -11.66 | PASS | 24.793 |
| `combo_fe1_5050_shared` | mix | 5 | leftover | list | none | 47% | 52% | 0/23 | -6.01 | 0 | 0 | +5.94 | -6.79 | -0.85 | — | PASS | 24.79 |
| `combo_ef_5050_shared` | mix | 5 | leftover | list | none | 43% | 57% | 1/23 | -2.38 | 0 | 0 | +8.35 | -8.08 | +8.70 | — | PASS | 24.784 |
| `combo_fe_5050_shared` | mix | 5 | leftover | list | none | 43% | 57% | 1/23 | -2.38 | 0 | 0 | +8.35 | -8.08 | +8.70 | — | PASS | 24.784 |
| `flatten_h5_sboost` | long | 5 | leftover | list | both | 45% | 61% | 1/23 | -8.13 | 19 | 95 | +8.88 | -8.99 | -1.17 | +5.37 | PASS | 24.681 |
| `union_h3_exit_alarm` | long | 3 | leftover | list | none | 45% | 48% | 0/23 | -7.96 | 14 | 92 | +6.97 | -5.54 | -2.60 | +8.77 | PASS | 24.656 |
| `union_h5_time` | long | 5 | leftover | time | none | 48% | 61% | 0/23 | -8.95 | 27 | 127 | +9.14 | -9.14 | -0.42 | +21.01 | PASS | 24.639 |
| `short_extended_h3` | short | 3 | leftover | list | none | 53% | 48% | 0/23 | -2.84 | 37 | 106 | +9.36 | -9.14 | -9.15 | -31.91 | PASS | 24.602 |
| `combo_eer_5050_shared` | mix | 5 | leftover | list | none | 35% | 39% | 7/23 | -2.08 | 0 | 0 | +8.54 | -8.98 | +12.73 | — | PASS | 24.598 |
| `union_news_or_net4_h1` | long | 1 | leftover | list | none | 45% | 35% | 0/23 | -11.94 | 1 | 16 | +3.96 | -4.30 | -8.06 | -1.79 | PASS | 24.581 |
| `combo_ef_3070_shared` | mix | 5 | leftover | list | none | 45% | 57% | 1/23 | -5.75 | 0 | 0 | +8.38 | -8.31 | +3.14 | — | PASS | 24.576 |
| `union_h5` | long | 5 | leftover | list | none | 48% | 61% | 0/23 | -9.32 | 27 | 127 | +9.04 | -9.16 | -1.00 | +21.01 | PASS | 24.488 |
| `union_h5_cut` | long | 5 | leftover | cut_loser | none | 48% | 61% | 0/23 | -9.32 | 27 | 127 | +9.04 | -9.16 | -1.00 | +21.01 | PASS | 24.488 |
| `union_h5_sizeup` | long | 5 | leftover | list | sizeup | 48% | 61% | 0/23 | -9.32 | 27 | 127 | +9.04 | -9.16 | -1.00 | +21.01 | PASS | 24.488 |
| `union_h5_trail` | long | 5 | leftover | trail | none | 48% | 61% | 0/23 | -9.32 | 27 | 127 | +9.04 | -9.16 | -1.00 | +21.01 | PASS | 24.488 |
| `short_extended_h1` | short | 1 | leftover | list | none | 52% | 30% | 0/23 | -3.21 | 20 | 56 | +5.66 | -6.31 | -11.16 | -7.11 | PASS | 24.368 |
| `union_rsi_os_h1` | long | 1 | leftover | list | none | 54% | 30% | 4/23 | -7.41 | 2 | 18 | +2.52 | -5.31 | -10.59 | +4.98 | PASS | 24.304 |
| `union_news_present_h1` | long | 1 | leftover | list | none | 44% | 30% | 0/23 | -3.58 | 5 | 41 | +4.16 | -4.18 | -1.19 | +5.61 | PASS | 24.302 |
| `flatten_h3_topheavy` | long | 3 | topheavy | list | none | 47% | 39% | 0/23 | -6.53 | 11 | 64 | +6.20 | -5.42 | -4.07 | +5.75 | PASS | 23.956 |
| `union_h5_sboost` | long | 5 | leftover | list | both | 48% | 61% | 0/23 | -9.32 | 27 | 127 | +8.55 | -9.70 | -1.56 | +21.01 | PASS | 23.874 |
| `union_h5_half` | long | 5 | half | list | none | 48% | 57% | 0/23 | -5.22 | 27 | 127 | +8.07 | -7.99 | -0.24 | +21.01 | PASS | 23.851 |
| `union_ab_g_h1` | long | 1 | leftover | list | none | 43% | 26% | 0/23 | -3.58 | 6 | 33 | +4.12 | -3.85 | -0.49 | -0.65 | PASS | 23.761 |
| `union_macd_hist_h1` | long | 1 | leftover | list | none | 47% | 35% | 0/23 | -3.56 | 9 | 60 | +3.83 | -3.95 | -6.79 | -5.17 | PASS | 23.701 |
| `union_last_green_h5` | long | 5 | leftover | list | none | 46% | 57% | 2/23 | -9.70 | 27 | 130 | +8.08 | -11.40 | +0.14 | +29.05 | PASS | 23.483 |
| `union_last_red_h1` | long | 1 | leftover | list | none | 50% | 30% | 0/23 | -3.99 | 8 | 57 | +3.47 | -4.92 | -2.91 | +12.39 | PASS | 23.449 |
| `union_news_vol_h3` | long | 3 | leftover | list | none | 51% | 48% | 0/23 | -6.39 | 9 | 46 | +4.24 | -4.61 | -6.94 | +194.07 | PASS | 23.323 |
| `union_h5_topheavy` | long | 5 | topheavy | list | none | 48% | 57% | 0/23 | -7.00 | 27 | 127 | +8.80 | -9.40 | -1.50 | +21.01 | PASS | 23.282 |
| `short_macd_dn_h3` | short | 3 | leftover | list | none | 48% | 43% | 2/23 | -0.53 | 18 | 93 | +3.99 | -5.67 | -9.44 | -19.63 | PASS | 23.256 |
| `union_last_green_h3` | long | 3 | leftover | list | none | 53% | 35% | 0/23 | -7.38 | 20 | 90 | +6.01 | -7.81 | -3.77 | +24.78 | PASS | 23.171 |
| `ohlc_hot_h1` | long | 1 | leftover | list | none | 44% | 39% | 0/23 | -5.55 | 15 | 45 | +3.21 | -4.93 | -10.33 | -2.12 | PASS | 23.129 |
| `union_h3_exit_red` | long | 3 | leftover | list | none | 51% | 35% | 0/23 | -7.09 | 20 | 90 | +5.97 | -7.06 | -3.35 | +17.32 | PASS | 22.73 |
| `union_blue_coil_h3` | long | 3 | leftover | list | none | 42% | 48% | 0/23 | -7.17 | 11 | 72 | +6.44 | -5.84 | -8.08 | -23.31 | PASS | 22.479 |
| `flatten_h5_half` | long | 5 | half | list | none | 45% | 52% | 0/23 | -5.32 | 19 | 95 | +7.96 | -7.42 | -0.97 | +5.37 | PASS | 22.314 |
| `union_h5_rankw` | long | 5 | rank_w | list | none | 48% | 57% | 0/23 | -9.00 | 27 | 127 | +7.65 | -9.72 | -4.72 | +21.01 | PASS | 22.059 |
| `union_macd_xup_h1` | long | 1 | leftover | list | none | 37% | 30% | 5/23 | -1.12 | 4 | 27 | +2.87 | -4.72 | -12.77 | -16.54 | PASS | 21.975 |
| `union_white_h5` | long | 5 | leftover | list | none | 36% | 65% | 2/23 | -11.27 | 15 | 63 | +9.42 | -8.73 | +1.59 | +15.74 | PASS | 21.955 |
| `flatten_h5_topheavy` | long | 5 | topheavy | list | none | 45% | 52% | 0/23 | -6.11 | 19 | 95 | +8.82 | -9.03 | -0.71 | +5.37 | PASS | 21.868 |
| `combo_fer_5050_shared` | mix | 5 | leftover | list | none | 41% | 52% | 1/23 | -3.85 | 0 | 0 | +8.19 | -8.94 | +6.16 | — | PASS | 21.811 |
| `union_cond_n4_h3` | long | 3 | leftover | list | none | 45% | 43% | 0/23 | -6.80 | 7 | 48 | +5.89 | -5.88 | -3.16 | -5.75 | PASS | 21.763 |
| `union_h5_exit_alarm` | long | 5 | leftover | list | none | 47% | 52% | 0/23 | -7.22 | 24 | 129 | +7.45 | -8.18 | -1.44 | +19.12 | PASS | 21.614 |
| `union_last_red_h3` | long | 3 | leftover | list | none | 43% | 48% | 0/23 | -2.74 | 14 | 103 | +6.75 | -6.43 | -1.08 | +8.56 | PASS | 21.559 |
| `union_macd_up_h3` | long | 3 | leftover | list | none | 41% | 43% | 0/23 | -6.56 | 19 | 102 | +8.04 | -5.70 | -6.56 | +3.62 | PASS | 21.419 |
| `union_news_g_cam71_h1` | long | 1 | leftover | list | none | 37% | 30% | 0/23 | -10.46 | 1 | 3 | +2.93 | -4.12 | -10.46 | -4.11 | PASS | 21.226 |
| `union_blue_coil_h1` | long | 1 | leftover | list | none | 43% | 22% | 0/23 | -6.43 | 5 | 30 | +3.37 | -4.28 | -7.75 | -8.06 | PASS | 20.861 |
| `flatten_live_h3` | long | 3 | leftover | list | none | 58% | 13% | 0/23 | -6.24 | 4 | 14 | +8.60 | -8.85 | -6.24 | +6.87 | PASS | 20.699 |
| `flatten_h5_rankw` | long | 5 | rank_w | list | none | 45% | 48% | 0/23 | -7.05 | 19 | 95 | +8.45 | -9.60 | -0.48 | +5.37 | PASS | 20.549 |
| `union_news_present_h3` | long | 3 | leftover | list | none | 42% | 39% | 0/23 | -9.66 | 13 | 86 | +6.76 | -5.75 | -10.68 | -0.26 | PASS | 20.123 |
| `union_join_g_h3` | long | 3 | leftover | list | none | 44% | 30% | 0/23 | -8.51 | 16 | 92 | +7.46 | -5.79 | -6.91 | -2.34 | PASS | 19.983 |
| `yday_gainer_h5` | long | 5 | leftover | list | none | 49% | 43% | 0/23 | -2.47 | 30 | 125 | +8.98 | -9.47 | -5.73 | +8.74 | PASS | 19.899 |
| `union_blue_h3` | long | 3 | leftover | list | none | 42% | 39% | 0/23 | -8.60 | 13 | 79 | +6.85 | -6.22 | -11.11 | -14.87 | PASS | 19.76 |
| `probable_h3` | long | 3 | leftover | list | none | 50% | 35% | 0/23 | -1.53 | 21 | 104 | +6.85 | -8.67 | -6.23 | +12.47 | PASS | 19.466 |
| `union_coil_off_h1` | long | 1 | leftover | list | none | 41% | 22% | 0/23 | -9.99 | 6 | 46 | +3.46 | -3.86 | -9.99 | -11.71 | PASS | 19.042 |
| `union_news_g_h3` | long | 3 | leftover | list | none | 51% | 26% | 0/23 | -5.05 | 16 | 70 | +4.55 | -5.92 | -13.49 | +181.85 | PASS | 19.038 |
| `union_cond_h3` | long | 3 | leftover | list | none | 40% | 48% | 0/23 | -8.51 | 14 | 104 | +6.55 | -7.47 | -6.43 | -17.44 | PASS | 18.692 |
| `ohlc_hot_h3` | long | 3 | leftover | list | none | 45% | 43% | 0/23 | -3.09 | 29 | 121 | +6.20 | -7.16 | -2.90 | +70.90 | PASS | 18.637 |
| `union_blue_vol_h3` | long | 3 | leftover | list | none | 40% | 39% | 1/23 | -4.87 | 13 | 74 | +8.94 | -7.54 | -7.96 | +7.57 | PASS | 18.436 |
| `union_candle_h3` | long | 3 | leftover | list | none | 47% | 26% | 0/23 | -5.69 | 19 | 100 | +7.14 | -7.15 | -7.47 | +2.66 | PASS | 18.282 |
| `union_white_coil_h3` | long | 3 | leftover | list | none | 42% | 48% | 0/23 | -16.97 | 8 | 42 | +5.20 | -6.60 | -13.49 | -20.75 | PASS | 17.451 |
| `flatten_live_h5` | long | 5 | leftover | list | none | 46% | 30% | 0/23 | -5.51 | 5 | 16 | +9.01 | -11.62 | -5.51 | +5.13 | PASS | 17.258 |
| `union_ab_g_h3` | long | 3 | leftover | list | none | 40% | 26% | 0/23 | -10.92 | 12 | 77 | +7.68 | -5.44 | -10.92 | -16.16 | PASS | 17.023 |
| `union_news_head_h3` | long | 3 | leftover | list | none | 49% | 17% | 0/23 | -6.47 | 15 | 61 | +5.49 | -5.19 | -16.54 | +181.88 | PASS | 16.779 |
| `flatten_vol_g_h3` | long | 3 | leftover | list | none | 52% | 17% | 0/23 | -18.85 | 5 | 20 | +9.63 | -9.36 | -24.16 | -18.45 | PASS | 15.758 |
| `union_rsi_h1` | long | 1 | leftover | list | none | 45% | 17% | 0/23 | -10.24 | 9 | 69 | +3.83 | -5.96 | -13.91 | -5.94 | PASS | 15.668 |
| `union_white_h3` | long | 3 | leftover | list | none | 38% | 35% | 1/23 | -15.08 | 11 | 53 | +7.40 | -7.09 | -13.67 | -2.54 | PASS | 14.655 |
| `ohlc_hot_h5` | long | 5 | leftover | list | none | 42% | 43% | 0/23 | -8.08 | 40 | 145 | +8.78 | -9.75 | -10.65 | +48.39 | PASS | 14.576 |
| `combo_e1er_5050_shared` | mix | 5 | leftover | list | none | 45% | 17% | 0/23 | -4.22 | 0 | 0 | +4.69 | -8.06 | -6.72 | — | PASS | 14.435 |
| `union_news_g_h5` | long | 5 | leftover | list | none | 41% | 39% | 0/23 | -5.77 | 18 | 104 | +5.68 | -8.05 | -10.90 | +118.78 | PASS | 13.359 |
| `coil_h3_exit_alarm` | long | 3 | leftover | list | none | 38% | 26% | 0/23 | -8.97 | 13 | 101 | +4.34 | -7.45 | -6.89 | -16.10 | PASS | 12.121 |
| `probable_h5` | long | 5 | leftover | list | none | 48% | 26% | 0/23 | -4.68 | 28 | 128 | +7.03 | -10.44 | -18.67 | -11.14 | PASS | 12.099 |
| `union_coil_off_h3` | long | 3 | leftover | list | none | 38% | 26% | 0/23 | -11.40 | 16 | 98 | +4.47 | -8.48 | -11.40 | -20.35 | PASS | 11.635 |
| `union_coil_green_h3` | long | 3 | leftover | list | none | 40% | 22% | 0/23 | -12.18 | 22 | 96 | +3.87 | -6.65 | -11.96 | -18.89 | PASS | 11.625 |
| `union_coil_off_h5` | long | 5 | leftover | list | none | 40% | 35% | 0/23 | -12.84 | 23 | 129 | +3.90 | -72.70 | -7.37 | -28.30 | PASS | 9.35 |
| `union_news_both_h1` *(thin)* | long | 1 | leftover | list | none | 100% | 13% | 21/23 | +7.89 | 0 | 0 | +3.69 | — | +7.92 | +4.91 | PASS | 61.621 |
| `union_flow_in_white_h1` *(thin)* | long | 1 | leftover | list | none | 60% | 17% | 17/23 | +0.29 | 0 | 2 | +2.59 | -2.35 | +0.64 | +4.31 | PASS | 37.551 |
| `short_news_pack_h3` *(thin)* | short | 3 | leftover | list | none | 54% | 39% | 12/23 | +3.13 | 2 | 5 | +3.31 | -2.00 | +5.59 | +5.19 | PASS | 36.107 |
| `union_news_missing_h1` *(thin)* | long | 1 | leftover | list | none | 56% | 13% | 11/23 | +0.00 | 0 | 2 | +3.78 | -2.55 | +1.64 | +2.18 | PASS | 22.232 |
| `union_news_missing_h3` *(thin)* | long | 3 | leftover | list | none | 56% | 26% | 1/23 | +0.00 | 1 | 7 | +9.22 | -3.20 | +3.13 | +4.96 | PASS | 15.863 |
| `union_catal_present_h1` *(thin)* | long | 1 | leftover | list | none | 40% | 4% | 0/23 | -2.79 | 0 | 0 | +7.67 | -1.88 | -2.79 | +6.22 | PASS | 11.451 |
| `union_vol_missing_h1` *(thin)* | long | 1 | leftover | list | none | 50% | 4% | 1/23 | +0.00 | 0 | 2 | +7.93 | -3.05 | +1.44 | +1.88 | PASS | 10.183 |
| `union_vol_missing_h3` *(thin)* | long | 3 | leftover | list | none | 50% | 13% | 1/23 | +0.00 | 1 | 5 | +11.98 | -3.98 | +3.58 | +5.59 | PASS | 8.608 |
| `union_news_g_cam91_n1_h1` *(thin)* | long | 1 | leftover | list | none | 50% | 4% | 0/23 | -2.85 | 0 | 0 | +0.85 | -3.84 | -2.85 | -0.09 | PASS | 1.547 |
| `union_catal_present_h3` *(thin)* | long | 3 | leftover | list | none | 20% | 9% | 0/23 | -7.96 | 1 | 2 | +8.08 | -2.57 | -7.96 | -5.67 | PASS | -1.454 |
| `union_rsi_os_macd_h1` *(thin)* | long | 1 | leftover | list | none | 33% | 4% | 1/23 | -5.91 | 1 | 2 | +7.49 | -6.33 | -5.91 | +0.01 | PASS | -8.017 |
| `short_r_down_h1` *(thin)* | short | 1 | leftover | list | none | — | 0% | 0/23 | +0.00 | 0 | 0 | — | — | +0.00 | +0.00 | PASS | -15.0 |
| `short_r_down_h3` *(thin)* | short | 3 | leftover | list | none | — | 0% | 0/23 | +0.00 | 0 | 0 | — | — | +0.00 | +0.00 | PASS | -15.0 |
| `union_r_up_h1` *(thin)* | long | 1 | leftover | list | none | — | 0% | 0/23 | +0.00 | 0 | 0 | — | — | +0.00 | +0.00 | PASS | -15.0 |
| `union_r_up_h3` *(thin)* | long | 3 | leftover | list | none | — | 0% | 0/23 | +0.00 | 0 | 0 | — | — | +0.00 | +0.00 | PASS | -15.0 |
