# Factor strategy mine — 2026-08-13 → 2026-09-18

Leak-free 09:30 recipes: **271** · candidate rows **1980** · fill `09:30 open, whole shares, Futubull fees, leftover split, sell first, hard-red sit, cash+holdings audit`.

Cash book: $10k, whole shares, Futubull fees, leftover split, sell first, min-hold, 09:30 open, hard-red S≤−3 sit, shorts marked as a liability. Each session starts from leftover cash and lots actually held (butterfly). Cash-start buttons wake a sleeve on date X with $10k and no lots (same rules). Stock investigator quotes 09:30 cameras / coaches / news from repo files. Size / sell / S-boost tweaks sit on the same ledger. Signal-only % is the old equal-weight path (not a fill). `flatten_h*` = wish-list (io/HOLD mornings still buy). `flatten_live_*` = only when the live flatten gate fires. Research only — does not change live `flatten_robust`.

Scoreboard files: slim `factor_mine.json` (stats / recipes / series) plus gzip shards in `factor_mine/shards/` (`books`, `starts`, `daily`, `probe`, `sim`). One file used to be 104MB and GitHub rejected the publish (100MB cap).

Combination books: **77** mixes on the same $10k cash ledger (shared leftover or split sleeves, official 09:30 / 16:00, hard-red sit, owner min-hold). Outperformers: `combo_ej_5050_shared`, `combo_es_9010_shared`, `combo_sh_3070_shared`, `combo_sh_5050_shared`, `combo_jer_5050_shared`, `combo_sj_3070_shared`, `combo_sj_5050_shared`, `combo_je1_5050_shared`, `combo_e1s_7030_shared`, `combo_sf_3070_shared`.

A combo outperforms its members on the cash book when the audit passes and either (1) Book% is strictly higher than every member, or (2) Book% stays within 2pp of the richest member, max drawdown is strictly smaller than every member's drawdown, start-rate is at least the best member, both halves are green, and the worst session is no worse than the worst member session. Effectiveness / Signal% do not decide WIN.

Action blotters: [FACTOR_MINE_ACTION.md](FACTOR_MINE_ACTION.md).

| Strategy | Side | H | Size | Sell | Boost | Win% | $ days | Starts YES | Med start | Top-g | Losers | AvgW | AvgL | Book% | Signal% | Audit | Eff |
|---|---|---:|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---:|
| `combo_sj_3070_shared` | mix | 5 | leftover | list | none | 57% | 65% | 26/26 | +7.46 | 0 | 0 | +5.00 | -4.59 | +27.13 | — | PASS | 72.981 |
| `combo_sh_3070_shared` | mix | 5 | leftover | list | none | 57% | 65% | 22/26 | +5.79 | 0 | 0 | +5.81 | -4.20 | +36.38 | — | PASS | 72.006 |
| `combo_ej_5050_shared` | mix | 5 | leftover | list | none | 50% | 58% | 26/26 | +8.10 | 0 | 0 | +5.88 | -6.13 | +38.77 | — | PASS | 69.75 |
| `combo_sj_5050_shared` | mix | 5 | leftover | list | none | 57% | 69% | 22/26 | +7.49 | 0 | 0 | +4.99 | -4.59 | +24.66 | — | PASS | 69.515 |
| `combo_sh_5050_shared` | mix | 5 | leftover | list | none | 58% | 69% | 18/26 | +4.53 | 0 | 0 | +5.76 | -4.28 | +34.80 | — | PASS | 68.983 |
| `combo_je1_5050_shared` | mix | 5 | leftover | list | none | 52% | 50% | 26/26 | +9.40 | 0 | 0 | +4.58 | -5.09 | +22.32 | — | PASS | 68.686 |
| `combo_es_9010_shared` | mix | 5 | leftover | list | none | 46% | 62% | 25/26 | +10.11 | 0 | 0 | +7.38 | -6.88 | +36.62 | — | PASS | 64.714 |
| `combo_jer_5050_shared` | mix | 5 | leftover | list | none | 47% | 54% | 24/26 | +4.04 | 0 | 0 | +6.23 | -6.41 | +31.81 | — | PASS | 64.199 |
| `combo_e1s_7030_shared` | mix | 5 | leftover | list | none | 56% | 62% | 11/26 | -0.20 | 0 | 0 | +5.20 | -6.08 | +17.38 | — | PASS | 43.816 |
| `combo_sf_3070_shared` | mix | 5 | leftover | list | none | 51% | 54% | 2/26 | -7.80 | 0 | 0 | +7.32 | -6.76 | +3.77 | — | PASS | 27.657 |
| `union_news_pack_net2_h1` | long | 1 | leftover | list | none | 58% | 46% | 21/26 | +15.50 | 1 | 7 | +3.98 | -2.64 | +26.01 | +16.92 | PASS | 71.607 |
| `combo_seh_333_skip` | mix | 5 | leftover | list | none | 51% | 73% | 25/26 | +6.29 | 0 | 0 | +6.61 | -6.50 | +32.98 | — | PASS | 70.589 |
| `union_hot_n4_h1` | long | 1 | leftover | list | none | 53% | 46% | 22/26 | +12.17 | 16 | 33 | +6.18 | -4.00 | +31.73 | +51.64 | PASS | 69.973 |
| `combo_hj_5050_shared` | mix | 5 | leftover | list | none | 49% | 46% | 26/26 | +8.59 | 0 | 0 | +4.94 | -3.97 | +20.29 | — | PASS | 69.05 |
| `union_vol_green_h1` | long | 1 | leftover | list | none | 48% | 46% | 26/26 | +13.49 | 20 | 46 | +5.68 | -5.03 | +21.69 | +5.03 | PASS | 68.601 |
| `union_news_pack_h1` | long | 1 | leftover | list | none | 57% | 46% | 21/26 | +12.01 | 2 | 10 | +3.77 | -3.27 | +21.55 | +16.25 | PASS | 68.235 |
| `union_join_vol_green_h1` | long | 1 | leftover | list | none | 50% | 42% | 26/26 | +8.87 | 11 | 35 | +4.72 | -4.14 | +20.16 | +10.96 | PASS | 68.2 |
| `union_e_fresh_h3` | long | 3 | leftover | list | none | 48% | 65% | 25/26 | +13.08 | 20 | 84 | +9.20 | -7.47 | +38.47 | +4.57 | PASS | 67.139 |
| `combo_ee1_3070_shared` | mix | 5 | leftover | list | none | 40% | 65% | 25/26 | +13.08 | 0 | 0 | +9.20 | -7.47 | +38.47 | — | PASS | 66.843 |
| `combo_ee1_5050_shared` | mix | 5 | leftover | list | none | 40% | 65% | 25/26 | +13.08 | 0 | 0 | +9.20 | -7.47 | +38.47 | — | PASS | 66.843 |
| `combo_ee1_7030_shared` | mix | 5 | leftover | list | none | 40% | 65% | 25/26 | +13.08 | 0 | 0 | +9.20 | -7.47 | +38.47 | — | PASS | 66.843 |
| `combo_he1_5050_shared` | mix | 5 | leftover | list | none | 50% | 46% | 25/26 | +9.51 | 0 | 0 | +5.25 | -5.27 | +23.41 | — | PASS | 66.685 |
| `probable_probable_ok_h1` | long | 1 | leftover | list | none | 53% | 35% | 26/26 | +6.19 | 7 | 20 | +3.65 | -3.25 | +5.22 | +15.77 | PASS | 66.275 |
| `combo_ehs_601525_shared` | mix | 5 | leftover | list | none | 45% | 62% | 25/26 | +5.58 | 0 | 0 | +6.96 | -6.17 | +29.54 | — | PASS | 66.064 |
| `combo_p2s_5050_shared` | mix | 5 | leftover | list | none | 64% | 65% | 16/26 | +1.59 | 0 | 0 | +4.84 | -4.10 | +14.63 | — | PASS | 65.622 |
| `combo_ps_7030_shared` | mix | 5 | leftover | list | none | 63% | 69% | 16/26 | +1.81 | 0 | 0 | +4.77 | -4.31 | +15.13 | — | PASS | 65.307 |
| `combo_jse_333_shared` | mix | 5 | leftover | list | none | 57% | 62% | 20/26 | +2.92 | 0 | 0 | +5.70 | -5.86 | +32.37 | — | PASS | 65.306 |
| `combo_hn_7030_shared` | mix | 5 | leftover | list | none | 51% | 46% | 22/26 | +6.76 | 0 | 0 | +4.46 | -4.43 | +22.41 | — | PASS | 65.3 |
| `combo_sj_7030_shared` | mix | 5 | leftover | list | none | 54% | 62% | 21/26 | +6.33 | 0 | 0 | +5.06 | -4.55 | +18.41 | — | PASS | 65.167 |
| `union_w_hot_cond_h1` | long | 1 | leftover | list | none | 50% | 46% | 25/26 | +5.67 | 24 | 53 | +5.32 | -5.85 | +5.23 | +19.62 | PASS | 65.035 |
| `combo_ehs_702010_shared` | mix | 5 | leftover | list | none | 40% | 58% | 25/26 | +9.15 | 0 | 0 | +7.65 | -5.79 | +32.22 | — | PASS | 64.657 |
| `combo_eh_5050_shared` | mix | 5 | leftover | list | none | 41% | 54% | 25/26 | +11.72 | 0 | 0 | +7.51 | -6.22 | +31.35 | — | PASS | 64.476 |
| `combo_seh_333_shared` | mix | 5 | leftover | list | none | 49% | 65% | 22/26 | +3.95 | 0 | 0 | +6.66 | -6.45 | +27.15 | — | PASS | 64.371 |
| `combo_seh_333_weather` | mix | 5 | leftover | list | none | 49% | 65% | 22/26 | +3.95 | 0 | 0 | +6.66 | -6.45 | +27.15 | — | PASS | 64.371 |
| `combo_sh_7030_shared` | mix | 5 | leftover | list | none | 58% | 77% | 13/26 | +2.76 | 0 | 0 | +5.73 | -4.34 | +25.97 | — | PASS | 64.269 |
| `combo_es_8020_shared` | mix | 5 | leftover | list | none | 48% | 58% | 25/26 | +7.42 | 0 | 0 | +7.00 | -7.24 | +34.06 | — | PASS | 64.124 |
| `combo_eh_7030_shared` | mix | 5 | leftover | list | none | 37% | 54% | 25/26 | +12.43 | 0 | 0 | +8.34 | -5.89 | +33.06 | — | PASS | 63.994 |
| `union_vol_ab_h1` | long | 1 | leftover | list | none | 53% | 35% | 24/26 | +6.96 | 15 | 41 | +5.28 | -4.63 | +8.60 | +62.52 | PASS | 63.695 |
| `combo_seh_502525_split` | mix | 5 | leftover | list | none | 48% | 69% | 22/26 | +2.54 | 0 | 0 | +6.91 | -6.04 | +15.87 | — | PASS | 63.668 |
| `union_news_pack_net3_h1` | long | 1 | leftover | list | none | 57% | 38% | 17/26 | +7.04 | 1 | 7 | +3.77 | -2.64 | +16.77 | +11.85 | PASS | 63.589 |
| `combo_en_7030_shared` | mix | 5 | leftover | list | none | 41% | 54% | 25/26 | +6.86 | 0 | 0 | +6.17 | -6.22 | +30.07 | — | PASS | 63.268 |
| `union_e_fresh_h1` | long | 1 | leftover | list | none | 48% | 46% | 25/26 | +5.68 | 9 | 43 | +4.85 | -6.19 | +15.43 | +9.13 | PASS | 63.106 |
| `combo_seh_333_split` | mix | 5 | leftover | list | none | 49% | 62% | 22/26 | +5.96 | 0 | 0 | +6.66 | -6.29 | +21.47 | — | PASS | 63.009 |
| `combo_hn_5050_shared` | mix | 5 | leftover | list | none | 51% | 46% | 20/26 | +3.12 | 0 | 0 | +4.50 | -4.35 | +16.98 | — | PASS | 62.87 |
| `union_hot_score_h3` | long | 3 | leftover | list | none | 45% | 62% | 25/26 | +7.54 | 43 | 145 | +11.81 | -8.74 | +16.45 | +265.34 | PASS | 62.616 |
| `combo_se_5050_split` | mix | 5 | leftover | list | none | 49% | 62% | 24/26 | +5.17 | 0 | 0 | +7.31 | -6.72 | +19.36 | — | PASS | 62.506 |
| `combo_se_3070_shared` | mix | 5 | leftover | list | none | 51% | 54% | 24/26 | +5.03 | 0 | 0 | +6.83 | -7.51 | +29.80 | — | PASS | 62.295 |
| `combo_se_5050_skip` | mix | 5 | leftover | list | none | 60% | 58% | 20/26 | +3.90 | 0 | 0 | +6.44 | -7.95 | +28.95 | — | PASS | 62.189 |
| `combo_en_5050_shared` | mix | 5 | leftover | list | none | 45% | 50% | 24/26 | +3.50 | 0 | 0 | +5.70 | -6.47 | +25.51 | — | PASS | 62.099 |
| `combo_eh_3070_shared` | mix | 5 | leftover | list | none | 42% | 42% | 25/26 | +10.56 | 0 | 0 | +7.20 | -6.51 | +29.27 | — | PASS | 61.711 |
| `union_last_green_h1` | long | 1 | leftover | list | none | 48% | 35% | 21/26 | +3.17 | 12 | 40 | +4.47 | -3.54 | +8.16 | +16.16 | PASS | 60.947 |
| `combo_seh_451540_shared` | mix | 5 | leftover | list | none | 48% | 73% | 17/26 | +2.58 | 0 | 0 | +6.71 | -6.56 | +27.53 | — | PASS | 60.805 |
| `combo_seh_404020_shared` | mix | 5 | leftover | list | none | 49% | 65% | 17/26 | +2.63 | 0 | 0 | +6.58 | -6.50 | +26.16 | — | PASS | 59.592 |
| `combo_seh_403525_shared` | mix | 5 | leftover | list | none | 48% | 65% | 17/26 | +2.86 | 0 | 0 | +6.78 | -6.36 | +26.18 | — | PASS | 59.438 |
| `combo_hn_3070_shared` | mix | 5 | leftover | list | none | 52% | 54% | 15/26 | +0.91 | 0 | 0 | +4.54 | -4.33 | +12.06 | — | PASS | 59.101 |
| `combo_her_5050_shared` | mix | 5 | leftover | list | none | 42% | 54% | 22/26 | +6.71 | 0 | 0 | +7.18 | -7.72 | +21.29 | — | PASS | 58.4 |
| `union_w_hot_candle_h3` | long | 3 | leftover | list | none | 46% | 58% | 23/26 | +4.25 | 38 | 137 | +8.48 | -8.79 | +11.69 | +229.23 | PASS | 58.257 |
| `union_ret_5_h1` | long | 1 | leftover | list | none | 48% | 38% | 20/26 | +1.31 | 21 | 64 | +5.83 | -5.54 | +7.07 | +15.00 | PASS | 57.825 |
| `yday_gainer_h3` | long | 3 | leftover | list | none | 51% | 42% | 24/26 | +4.92 | 30 | 120 | +8.56 | -7.14 | +0.22 | +36.41 | PASS | 57.821 |
| `combo_ef_7030_shared` | mix | 5 | leftover | list | none | 43% | 65% | 21/26 | +5.16 | 0 | 0 | +8.00 | -7.83 | +20.33 | — | PASS | 57.085 |
| `union_break10_h3` | long | 3 | leftover | list | none | 47% | 50% | 23/26 | +5.66 | 32 | 119 | +8.15 | -7.94 | +3.94 | +47.50 | PASS | 56.963 |
| `union_vol_ab_h3` | long | 3 | leftover | list | none | 48% | 35% | 26/26 | +5.24 | 22 | 88 | +8.35 | -6.98 | +4.31 | +80.43 | PASS | 56.804 |
| `combo_nj_5050_shared` | mix | 5 | leftover | list | none | 49% | 38% | 18/26 | +0.79 | 0 | 0 | +4.24 | -4.34 | +4.82 | — | PASS | 56.31 |
| `combo_seh_601525_shared` | mix | 5 | leftover | list | none | 48% | 73% | 13/26 | +0.26 | 0 | 0 | +6.70 | -6.59 | +22.64 | — | PASS | 56.191 |
| `yday_gainer_h1` | long | 1 | leftover | list | none | 53% | 38% | 15/26 | +1.25 | 17 | 59 | +4.87 | -4.04 | +7.55 | +20.51 | PASS | 55.701 |
| `union_join_g_h1` | long | 1 | leftover | list | none | 47% | 42% | 17/26 | +3.25 | 10 | 51 | +4.07 | -4.18 | +7.49 | +11.23 | PASS | 55.433 |
| `union_vol_g_h3` | long | 3 | leftover | list | none | 48% | 46% | 24/26 | +5.77 | 28 | 119 | +8.85 | -8.28 | +0.99 | +24.84 | PASS | 55.376 |
| `combo_seh_502525_shared` | mix | 5 | leftover | list | none | 48% | 65% | 13/26 | +0.09 | 0 | 0 | +6.72 | -6.43 | +24.20 | — | PASS | 55.19 |
| `union_w_hot_candle_h1` | long | 1 | leftover | list | none | 51% | 42% | 15/26 | +0.06 | 23 | 58 | +4.76 | -5.21 | +2.08 | +21.42 | PASS | 54.42 |
| `combo_eer_5050_shared` | mix | 5 | leftover | list | none | 38% | 46% | 24/26 | +6.58 | 0 | 0 | +8.48 | -8.87 | +22.73 | — | PASS | 54.121 |
| `union_last_red_h3` | long | 3 | leftover | list | none | 44% | 50% | 22/26 | +3.67 | 20 | 121 | +6.50 | -6.56 | +5.40 | +9.39 | PASS | 53.849 |
| `short_macd_dn_h3` | short | 3 | leftover | list | none | 50% | 42% | 23/26 | +1.23 | 21 | 111 | +4.27 | -5.71 | -7.94 | -15.49 | PASS | 53.396 |
| `union_macd_up_h1` | long | 1 | leftover | list | none | 45% | 35% | 17/26 | +2.26 | 17 | 53 | +4.91 | -4.59 | +3.77 | +15.08 | PASS | 53.396 |
| `union_w_hot_cond_h3` | long | 3 | leftover | list | none | 44% | 54% | 18/26 | +1.89 | 38 | 136 | +9.76 | -8.20 | +10.63 | +214.79 | PASS | 52.983 |
| `union_ret_5_h3` | long | 3 | leftover | list | none | 45% | 54% | 18/26 | +2.10 | 36 | 147 | +9.65 | -8.28 | +14.97 | +42.95 | PASS | 52.691 |
| `probable_h3` | long | 3 | leftover | list | none | 50% | 38% | 22/26 | +4.93 | 28 | 124 | +6.61 | -8.48 | -0.08 | +17.24 | PASS | 52.001 |
| `union_vol_green_h3` | long | 3 | leftover | list | none | 52% | 31% | 21/26 | +6.83 | 34 | 101 | +7.08 | -8.05 | -4.20 | +41.78 | PASS | 51.499 |
| `combo_ps_5050_shared` | mix | 5 | leftover | list | none | 65% | 65% | 12/26 | +0.00 | 0 | 0 | +4.71 | -4.40 | +13.81 | — | PASS | 51.245 |
| `ohlc_hot_h1` | long | 1 | leftover | list | none | 45% | 42% | 17/26 | +2.79 | 16 | 54 | +3.21 | -4.93 | -4.17 | +3.57 | PASS | 50.974 |
| `union_join_vol_green_h3` | long | 3 | leftover | list | none | 50% | 38% | 19/26 | +6.03 | 20 | 77 | +7.26 | -7.78 | -1.42 | +37.40 | PASS | 50.957 |
| `union_candle_h1` | long | 1 | leftover | list | none | 45% | 31% | 15/26 | +1.06 | 9 | 48 | +5.12 | -4.23 | +1.48 | +4.44 | PASS | 50.912 |
| `union_ab_g_h1` | long | 1 | leftover | list | none | 42% | 35% | 15/26 | +0.64 | 8 | 40 | +3.85 | -3.56 | +3.88 | +2.77 | PASS | 50.194 |
| `ohlc_hot_h3` | long | 3 | leftover | list | none | 45% | 46% | 18/26 | +1.85 | 33 | 138 | +6.10 | -7.21 | +1.85 | +111.94 | PASS | 47.372 |
| `probable_probable_ok_h3` | long | 3 | leftover | list | none | 54% | 31% | 16/26 | +4.47 | 17 | 54 | +5.60 | -8.07 | -13.63 | +21.25 | PASS | 45.747 |
| `union_candle_h3` | long | 3 | leftover | list | none | 46% | 31% | 16/26 | +2.55 | 24 | 116 | +6.90 | -7.35 | +0.46 | +20.27 | PASS | 45.172 |
| `union_news_pack_h3` | long | 3 | leftover | list | none | 44% | 35% | 16/26 | +4.08 | 5 | 24 | +5.34 | -7.33 | -7.10 | -3.50 | PASS | 45.161 |
| `union_e_green_h3` | long | 3 | leftover | list | none | 42% | 42% | 18/26 | +13.38 | 6 | 46 | +5.47 | -8.76 | +12.39 | -1.64 | PASS | 45.139 |
| `union_earn_react_h3` | long | 3 | leftover | list | none | 42% | 50% | 13/26 | +1.33 | 15 | 83 | +8.88 | -9.12 | +20.11 | -16.65 | PASS | 45.09 |
| `union_e_green_h1` | long | 1 | leftover | list | none | 48% | 38% | 14/26 | +0.35 | 4 | 19 | +4.16 | -9.48 | -19.60 | -13.97 | PASS | 44.947 |
| `combo_snj_333_shared` | mix | 5 | leftover | list | none | 52% | 58% | 11/26 | -2.39 | 0 | 0 | +4.56 | -4.56 | +9.59 | — | PASS | 43.284 |
| `combo_e1er_5050_shared` | mix | 5 | leftover | list | none | 44% | 23% | 17/26 | +1.06 | 0 | 0 | +4.79 | -7.83 | -1.55 | — | PASS | 42.708 |
| `union_blue_vol_h3` | long | 3 | leftover | list | none | 41% | 38% | 13/26 | +2.06 | 18 | 90 | +9.92 | -7.51 | -3.97 | +17.73 | PASS | 41.844 |
| `combo_se1_5050_shared` | mix | 5 | leftover | list | none | 56% | 65% | 8/26 | -1.70 | 0 | 0 | +5.10 | -6.25 | +14.72 | — | PASS | 41.468 |
| `union_hot_score_h1` | long | 1 | leftover | list | none | 50% | 46% | 11/26 | -0.73 | 27 | 63 | +5.89 | -6.13 | +2.39 | +23.46 | PASS | 40.859 |
| `union_hot_n12_h1` | long | 1 | leftover | list | none | 52% | 42% | 11/26 | -0.82 | 33 | 87 | +4.91 | -5.56 | -2.35 | +18.93 | PASS | 39.784 |
| `combo_hf_5050_shared` | mix | 5 | leftover | list | none | 46% | 54% | 12/26 | -1.05 | 0 | 0 | +6.32 | -5.79 | +7.47 | — | PASS | 39.379 |
| `ohlc_hot_h5` | long | 5 | leftover | list | none | 44% | 46% | 13/26 | +1.32 | 45 | 165 | +8.16 | -9.87 | -4.30 | +91.67 | PASS | 39.048 |
| `union_h1_topheavy` | long | 1 | topheavy | list | none | 45% | 46% | 9/26 | -2.29 | 11 | 46 | +4.38 | -3.88 | +6.18 | +10.70 | PASS | 38.903 |
| `union_news_g_conv_h1` | long | 1 | conviction | list | none | 54% | 42% | 7/26 | -4.89 | 1 | 14 | +3.76 | -4.49 | +4.45 | +15.96 | PASS | 38.767 |
| `probable_h1` | long | 1 | leftover | list | none | 52% | 31% | 11/26 | -0.41 | 16 | 58 | +4.19 | -3.75 | -0.41 | +16.89 | PASS | 38.305 |
| `union_break10_h1` | long | 1 | leftover | list | none | 48% | 38% | 11/26 | -0.83 | 20 | 52 | +5.20 | -5.25 | -3.73 | +5.39 | PASS | 37.941 |
| `flatten_live_h1_topheavy` | long | 1 | topheavy | list | none | 54% | 12% | 7/26 | +0.00 | 1 | 5 | +6.75 | -3.75 | +7.19 | +4.45 | PASS | 37.243 |
| `union_coil_green_h1` | long | 1 | leftover | list | none | 46% | 38% | 10/26 | -2.09 | 13 | 34 | +3.51 | -4.10 | -0.55 | -0.60 | PASS | 37.225 |
| `ohlc_hot_coil_h1` | long | 1 | leftover | list | none | 54% | 38% | 8/26 | -0.48 | 11 | 24 | +3.67 | -3.78 | -17.33 | -5.03 | PASS | 36.774 |
| `union_blue_coil_h3` | long | 3 | leftover | list | none | 46% | 50% | 12/26 | -1.47 | 19 | 87 | +6.07 | -5.91 | -2.37 | -12.31 | PASS | 36.346 |
| `flatten_live_h1` | long | 1 | leftover | list | none | 54% | 12% | 7/26 | +0.00 | 1 | 5 | +6.31 | -4.05 | +8.22 | +4.45 | PASS | 36.203 |
| `flatten_live_h1_cut` | long | 1 | leftover | cut_loser | none | 54% | 12% | 7/26 | +0.00 | 1 | 5 | +6.31 | -4.05 | +8.22 | +4.45 | PASS | 36.203 |
| `flatten_live_h1_sboost` | long | 1 | leftover | list | both | 54% | 12% | 7/26 | +0.00 | 1 | 5 | +6.31 | -4.05 | +8.22 | +4.45 | PASS | 36.203 |
| `flatten_live_h1_sizeup` | long | 1 | leftover | list | sizeup | 54% | 12% | 7/26 | +0.00 | 1 | 5 | +6.31 | -4.05 | +8.22 | +4.45 | PASS | 36.203 |
| `flatten_live_h1_time` | long | 1 | leftover | time | none | 54% | 12% | 7/26 | +0.00 | 1 | 5 | +6.31 | -4.05 | +8.22 | +4.45 | PASS | 36.203 |
| `flatten_live_h1_trail` | long | 1 | leftover | trail | none | 54% | 12% | 7/26 | +0.00 | 1 | 5 | +6.31 | -4.05 | +8.22 | +4.45 | PASS | 36.203 |
| `flatten_live_h1_half` | long | 1 | half | list | none | 54% | 12% | 7/26 | +0.00 | 1 | 5 | +6.58 | -3.90 | +3.74 | +4.45 | PASS | 36.151 |
| `combo_jf_5050_shared` | mix | 5 | leftover | list | none | 45% | 65% | 6/26 | -3.47 | 0 | 0 | +5.78 | -5.29 | +5.80 | — | PASS | 35.21 |
| `combo_en_3070_shared` | mix | 5 | leftover | list | none | 44% | 42% | 9/26 | -0.35 | 0 | 0 | +5.72 | -6.27 | +20.00 | — | PASS | 35.119 |
| `union_news_or_h1` | long | 1 | leftover | list | none | 52% | 42% | 4/26 | -1.45 | 4 | 31 | +4.12 | -3.85 | +0.50 | +16.47 | PASS | 34.958 |
| `combo_ef_5050_shared` | mix | 5 | leftover | list | none | 43% | 58% | 11/26 | -0.32 | 0 | 0 | +8.23 | -7.73 | +11.13 | — | PASS | 34.944 |
| `combo_fe_5050_shared` | mix | 5 | leftover | list | none | 43% | 58% | 11/26 | -0.32 | 0 | 0 | +8.23 | -7.73 | +11.13 | — | PASS | 34.944 |
| `combo_sn_3070_shared` | mix | 5 | leftover | list | none | 55% | 62% | 2/26 | -7.53 | 0 | 0 | +4.54 | -4.49 | +1.50 | — | PASS | 34.492 |
| `union_cond_h1` | long | 1 | leftover | list | none | 48% | 35% | 8/26 | -0.90 | 5 | 43 | +4.17 | -4.10 | -3.63 | +2.75 | PASS | 34.423 |
| `flatten_live_h1_rankw` | long | 1 | rank_w | list | none | 54% | 12% | 6/26 | +0.00 | 1 | 5 | +6.14 | -4.12 | +4.03 | +4.45 | PASS | 34.27 |
| `short_last_red_h3` | short | 3 | leftover | list | none | 52% | 42% | 12/26 | -1.10 | 22 | 119 | +5.11 | -5.95 | -6.56 | -25.35 | PASS | 34.241 |
| `combo_se_5050_shared` | mix | 5 | leftover | list | none | 56% | 54% | 4/26 | -0.50 | 0 | 0 | +6.64 | -7.54 | +21.01 | — | PASS | 33.716 |
| `combo_se_5050_weather` | mix | 5 | leftover | list | none | 56% | 54% | 4/26 | -0.50 | 0 | 0 | +6.64 | -7.54 | +21.01 | — | PASS | 33.716 |
| `union_h3_cut` | long | 3 | leftover | cut_loser | none | 46% | 50% | 6/26 | -2.73 | 20 | 108 | +8.80 | -5.62 | +1.71 | +12.27 | PASS | 33.371 |
| `union_h3_trail` | long | 3 | leftover | trail | none | 46% | 50% | 6/26 | -2.73 | 20 | 108 | +8.80 | -5.62 | +1.71 | +12.27 | PASS | 33.371 |
| `combo_ne1_5050_shared` | mix | 5 | leftover | list | none | 51% | 38% | 5/26 | -2.00 | 0 | 0 | +4.21 | -5.26 | +8.31 | — | PASS | 33.334 |
| `flatten_h1` | long | 1 | leftover | list | none | 43% | 38% | 6/26 | -4.91 | 3 | 31 | +4.21 | -3.60 | +2.06 | -3.23 | PASS | 33.115 |
| `union_news_or_net2_h1` | long | 1 | leftover | list | none | 46% | 35% | 7/26 | -1.27 | 1 | 22 | +4.17 | -4.09 | -1.27 | +2.21 | PASS | 33.079 |
| `combo_sn_5050_shared` | mix | 5 | leftover | list | none | 57% | 65% | 0/26 | -9.47 | 0 | 0 | +4.45 | -4.64 | -2.64 | — | PASS | 33.003 |
| `union_news_or_net4_conv_h1` | long | 1 | conviction | list | none | 49% | 42% | 4/26 | -9.30 | 1 | 10 | +3.50 | -4.27 | -0.67 | +0.92 | PASS | 32.997 |
| `combo_fh_7030_shared` | mix | 5 | leftover | list | none | 45% | 58% | 6/26 | -2.42 | 0 | 0 | +6.44 | -6.00 | +1.33 | — | PASS | 32.777 |
| `union_h3_time` | long | 3 | leftover | time | none | 46% | 50% | 6/26 | -2.73 | 20 | 108 | +8.04 | -5.72 | +2.18 | +12.27 | PASS | 32.646 |
| `union_h3_sboost` | long | 3 | leftover | list | both | 46% | 54% | 5/26 | -0.85 | 20 | 108 | +8.46 | -6.00 | +1.97 | +12.27 | PASS | 32.432 |
| `union_news_g_h1` | long | 1 | leftover | list | none | 52% | 42% | 2/26 | -2.39 | 3 | 31 | +3.93 | -4.24 | +0.97 | +14.54 | PASS | 32.281 |
| `short_news_head_h3` | short | 3 | leftover | list | none | 58% | 62% | 3/26 | -3.92 | 6 | 36 | +6.53 | -6.31 | +1.50 | -1.06 | PASS | 32.108 |
| `combo_sn_7030_shared` | mix | 5 | leftover | list | none | 57% | 62% | 0/26 | -9.54 | 0 | 0 | +4.30 | -4.87 | -4.07 | — | PASS | 31.847 |
| `union_h3` | long | 3 | leftover | list | none | 46% | 50% | 5/26 | -2.73 | 20 | 108 | +8.80 | -5.97 | +0.13 | +12.27 | PASS | 31.721 |
| `union_h3_sizeup` | long | 3 | leftover | list | sizeup | 46% | 50% | 5/26 | -2.73 | 20 | 108 | +8.80 | -5.97 | +0.13 | +12.27 | PASS | 31.721 |
| `short_news_or_h3` | short | 3 | leftover | list | none | 58% | 54% | 3/26 | -2.80 | 8 | 41 | +5.42 | -5.57 | +2.96 | +0.90 | PASS | 31.385 |
| `short_news_r_h3` | short | 3 | leftover | list | none | 58% | 54% | 3/26 | -2.80 | 8 | 41 | +5.42 | -5.57 | +2.96 | +0.90 | PASS | 31.385 |
| `union_news_g_cond_h1` | long | 1 | leftover | list | none | 51% | 38% | 2/26 | -2.08 | 4 | 32 | +4.12 | -4.13 | -0.56 | +15.84 | PASS | 31.359 |
| `yday_gainer_h5` | long | 5 | leftover | list | none | 49% | 46% | 10/26 | -1.34 | 41 | 147 | +8.56 | -9.87 | +1.85 | +19.67 | PASS | 31.14 |
| `union_join_g_h3` | long | 3 | leftover | list | none | 43% | 35% | 10/26 | -2.70 | 20 | 107 | +7.45 | -5.87 | -1.00 | +6.29 | PASS | 31.009 |
| `union_last_red_h1` | long | 1 | leftover | list | none | 50% | 35% | 6/26 | -2.56 | 13 | 66 | +3.72 | -4.86 | -1.46 | +13.61 | PASS | 30.943 |
| `union_blue_h3` | long | 3 | leftover | list | none | 44% | 42% | 9/26 | -2.21 | 20 | 94 | +6.85 | -6.31 | -5.52 | -6.71 | PASS | 30.74 |
| `union_last_green_h5` | long | 5 | leftover | list | none | 49% | 58% | 8/26 | -3.50 | 36 | 150 | +7.88 | -10.91 | +0.73 | +61.92 | PASS | 30.462 |
| `combo_se_7030_shared` | mix | 5 | leftover | list | none | 56% | 50% | 2/26 | -5.50 | 0 | 0 | +6.48 | -7.56 | +14.58 | — | PASS | 30.203 |
| `union_join_present_h3` | long | 3 | leftover | list | none | 44% | 42% | 5/26 | -2.73 | 18 | 110 | +9.71 | -5.88 | +0.50 | +4.44 | PASS | 29.821 |
| `short_news_r_h1` | short | 1 | leftover | list | none | 53% | 42% | 1/26 | -5.20 | 5 | 17 | +2.89 | -3.64 | -5.91 | -8.94 | PASS | 29.705 |
| `union_h3_rankw` | long | 3 | rank_w | list | none | 46% | 50% | 4/26 | -7.45 | 20 | 108 | +8.61 | -5.96 | -6.21 | +12.27 | PASS | 29.658 |
| `union_white_coil_h1` | long | 1 | leftover | list | none | 40% | 27% | 9/26 | -2.40 | 4 | 23 | +4.43 | -3.84 | -6.04 | -3.01 | PASS | 29.578 |
| `union_news_or_net4_h1` | long | 1 | leftover | list | none | 45% | 38% | 4/26 | -10.74 | 1 | 17 | +3.92 | -4.17 | -7.21 | -1.08 | PASS | 29.526 |
| `union_h3_half` | long | 3 | half | list | none | 46% | 50% | 3/26 | -5.50 | 20 | 108 | +8.37 | -5.36 | -4.65 | +12.27 | PASS | 29.517 |
| `union_h5_time` | long | 5 | leftover | time | none | 47% | 62% | 6/26 | -11.09 | 32 | 149 | +8.95 | -8.41 | -4.93 | +21.23 | PASS | 29.296 |
| `union_h5` | long | 5 | leftover | list | none | 47% | 62% | 6/26 | -11.09 | 32 | 149 | +8.85 | -8.41 | -4.56 | +21.23 | PASS | 29.287 |
| `union_h5_cut` | long | 5 | leftover | cut_loser | none | 47% | 62% | 6/26 | -11.09 | 32 | 149 | +8.85 | -8.41 | -4.56 | +21.23 | PASS | 29.287 |
| `union_h5_sizeup` | long | 5 | leftover | list | sizeup | 47% | 62% | 6/26 | -11.09 | 32 | 149 | +8.85 | -8.41 | -4.56 | +21.23 | PASS | 29.287 |
| `union_h5_trail` | long | 5 | leftover | trail | none | 47% | 62% | 6/26 | -11.09 | 32 | 149 | +8.85 | -8.41 | -4.56 | +21.23 | PASS | 29.287 |
| `union_earn_react_h1` | long | 1 | leftover | list | none | 48% | 31% | 8/26 | -0.19 | 7 | 47 | +4.79 | -7.61 | +1.59 | +0.99 | PASS | 29.275 |
| `union_h3_exit_news_r` | long | 3 | leftover | list | none | 44% | 46% | 4/26 | -4.38 | 17 | 109 | +9.71 | -5.94 | -1.58 | +0.74 | PASS | 29.259 |
| `combo_fe1_5050_shared` | mix | 5 | leftover | list | none | 46% | 54% | 4/26 | -4.56 | 0 | 0 | +5.91 | -6.48 | +0.68 | — | PASS | 28.85 |
| `union_h1_rankw` | long | 1 | rank_w | list | none | 45% | 46% | 0/26 | -7.39 | 11 | 46 | +4.34 | -3.91 | -3.39 | +10.70 | PASS | 28.722 |
| `union_flow_in_h1` | long | 1 | leftover | list | none | 44% | 31% | 1/26 | -4.92 | 4 | 6 | +4.41 | -4.60 | +2.15 | +7.02 | PASS | 28.72 |
| `combo_ef_3070_shared` | mix | 5 | leftover | list | none | 45% | 58% | 5/26 | -2.87 | 0 | 0 | +8.20 | -7.92 | +4.45 | — | PASS | 28.682 |
| `union_h3_exit_alarm` | long | 3 | leftover | list | none | 44% | 46% | 5/26 | -2.49 | 18 | 110 | +6.97 | -5.83 | +0.85 | +5.07 | PASS | 28.553 |
| `union_h1` | long | 1 | leftover | list | none | 45% | 42% | 0/26 | -4.90 | 11 | 46 | +4.36 | -3.89 | -0.03 | +10.70 | PASS | 28.508 |
| `union_h1_sizeup` | long | 1 | leftover | list | sizeup | 45% | 42% | 0/26 | -4.90 | 11 | 46 | +4.36 | -3.89 | -0.03 | +10.70 | PASS | 28.508 |
| `union_h1_trail` | long | 1 | leftover | trail | none | 45% | 42% | 0/26 | -4.90 | 11 | 46 | +4.36 | -3.89 | -0.03 | +10.70 | PASS | 28.508 |
| `combo_nse_333_shared` | mix | 5 | leftover | list | none | 50% | 46% | 1/26 | -4.44 | 0 | 0 | +5.63 | -6.32 | +15.76 | — | PASS | 28.47 |
| `union_h3_topheavy` | long | 3 | topheavy | list | none | 46% | 42% | 5/26 | -3.43 | 20 | 108 | +7.05 | -6.12 | -0.90 | +12.27 | PASS | 28.414 |
| `union_news_g_cam71_h1` | long | 1 | leftover | list | none | 39% | 38% | 4/26 | -8.27 | 1 | 3 | +3.02 | -4.12 | -8.27 | -2.91 | PASS | 28.359 |
| `combo_nf_5050_shared` | mix | 5 | leftover | list | none | 43% | 58% | 1/26 | -6.18 | 0 | 0 | +6.56 | -5.65 | +1.99 | — | PASS | 28.093 |
| `union_macd_hist_h1` | long | 1 | leftover | list | none | 47% | 35% | 4/26 | -1.93 | 9 | 68 | +4.00 | -3.91 | -5.25 | -7.02 | PASS | 28.061 |
| `union_h1_time` | long | 1 | leftover | time | none | 45% | 38% | 0/26 | -4.93 | 11 | 46 | +4.34 | -3.62 | -0.72 | +10.70 | PASS | 28.024 |
| `union_h1_sboost` | long | 1 | leftover | list | both | 45% | 42% | 0/26 | -6.54 | 11 | 46 | +4.22 | -4.02 | -1.65 | +10.70 | PASS | 27.91 |
| `union_last_green_h3` | long | 3 | leftover | list | none | 53% | 35% | 5/26 | -4.46 | 26 | 106 | +5.87 | -7.94 | -1.12 | +40.98 | PASS | 27.895 |
| `combo_sf_5050_shared` | mix | 5 | leftover | list | none | 53% | 58% | 1/26 | -10.69 | 0 | 0 | +6.99 | -6.61 | +1.31 | — | PASS | 27.864 |
| `union_join_present_h1` | long | 1 | leftover | list | none | 45% | 42% | 0/26 | -4.90 | 9 | 50 | +4.36 | -3.97 | -0.47 | +6.33 | PASS | 27.804 |
| `union_h1_cut` | long | 1 | leftover | cut_loser | none | 45% | 38% | 0/26 | -4.98 | 11 | 46 | +4.36 | -3.84 | -0.23 | +10.70 | PASS | 27.778 |
| `union_h3_exit_red` | long | 3 | leftover | list | none | 51% | 35% | 5/26 | -4.51 | 26 | 106 | +5.84 | -7.30 | -0.66 | +28.76 | PASS | 27.484 |
| `union_h1_half` | long | 1 | half | list | none | 45% | 38% | 0/26 | -2.92 | 11 | 46 | +4.28 | -3.94 | -1.82 | +10.70 | PASS | 27.284 |
| `union_candle_score_h1` | long | 1 | leftover | list | none | 51% | 27% | 0/26 | -2.94 | 17 | 53 | +4.45 | -4.09 | -1.71 | +6.13 | PASS | 27.268 |
| `combo_sf_7030_shared` | mix | 5 | leftover | list | none | 53% | 58% | 1/26 | -11.71 | 0 | 0 | +6.57 | -6.78 | +0.26 | — | PASS | 27.256 |
| `combo_fse_333_shared` | mix | 5 | leftover | list | none | 48% | 58% | 2/26 | -5.46 | 0 | 0 | +6.72 | -7.55 | +6.70 | — | PASS | 26.946 |
| `union_news_head_h1` | long | 1 | leftover | list | none | 51% | 31% | 0/26 | -4.42 | 4 | 24 | +4.11 | -4.20 | -7.85 | +12.99 | PASS | 26.892 |
| `union_h5_sboost` | long | 5 | leftover | list | both | 47% | 62% | 4/26 | -11.12 | 32 | 149 | +8.39 | -8.87 | -6.71 | +21.23 | PASS | 26.509 |
| `union_h5_topheavy` | long | 5 | topheavy | list | none | 47% | 58% | 4/26 | -8.14 | 32 | 149 | +8.63 | -8.59 | -4.22 | +21.23 | PASS | 26.409 |
| `combo_ner_5050_shared` | mix | 5 | leftover | list | none | 43% | 46% | 1/26 | -2.22 | 0 | 0 | +5.82 | -6.79 | +17.57 | — | PASS | 26.405 |
| `combo_fes_403030_shared` | mix | 5 | leftover | list | none | 46% | 58% | 2/26 | -5.19 | 0 | 0 | +6.94 | -7.40 | +5.52 | — | PASS | 26.313 |
| `union_h5_half` | long | 5 | half | list | none | 47% | 58% | 3/26 | -5.30 | 32 | 149 | +7.93 | -7.63 | -0.58 | +21.23 | PASS | 26.159 |
| `union_macd_up_h3` | long | 3 | leftover | list | none | 42% | 42% | 5/26 | -4.22 | 27 | 118 | +7.63 | -6.30 | -4.93 | +21.44 | PASS | 26.029 |
| `combo_ser_5050_shared` | mix | 5 | leftover | list | none | 52% | 42% | 2/26 | -6.95 | 0 | 0 | +6.76 | -7.83 | +12.61 | — | PASS | 25.991 |
| `flatten_h5` | long | 5 | leftover | list | none | 44% | 62% | 2/26 | -5.41 | 19 | 107 | +8.90 | -8.31 | +3.39 | +3.80 | PASS | 25.9 |
| `flatten_h5_cut` | long | 5 | leftover | cut_loser | none | 44% | 62% | 2/26 | -5.41 | 19 | 107 | +8.90 | -8.31 | +3.39 | +3.80 | PASS | 25.9 |
| `flatten_h5_sizeup` | long | 5 | leftover | list | sizeup | 44% | 62% | 2/26 | -5.41 | 19 | 107 | +8.90 | -8.31 | +3.39 | +3.80 | PASS | 25.9 |
| `flatten_h5_time` | long | 5 | leftover | time | none | 44% | 62% | 2/26 | -5.41 | 19 | 107 | +8.90 | -8.31 | +3.39 | +3.80 | PASS | 25.9 |
| `flatten_h5_trail` | long | 5 | leftover | trail | none | 44% | 62% | 2/26 | -5.41 | 19 | 107 | +8.90 | -8.31 | +3.39 | +3.80 | PASS | 25.9 |
| `union_white_h1` | long | 1 | leftover | list | none | 36% | 35% | 4/26 | -4.53 | 3 | 26 | +5.79 | -4.97 | -1.37 | -7.92 | PASS | 25.544 |
| `flatten_h5_sboost` | long | 5 | leftover | list | both | 44% | 62% | 2/26 | -5.41 | 19 | 107 | +8.68 | -8.12 | +0.97 | +3.80 | PASS | 25.517 |
| `flatten_h3_sboost` | long | 3 | leftover | list | both | 45% | 42% | 1/26 | -8.77 | 11 | 74 | +7.81 | -5.30 | -5.58 | +2.48 | PASS | 25.48 |
| `short_alarm_h1` | short | 1 | leftover | list | none | 50% | 19% | 0/26 | -3.52 | 8 | 41 | +4.46 | -2.81 | -1.85 | -6.32 | PASS | 25.471 |
| `flatten_h3` | long | 3 | leftover | list | none | 45% | 42% | 1/26 | -8.77 | 11 | 74 | +8.12 | -5.53 | -5.53 | +2.48 | PASS | 25.458 |
| `flatten_h3_cut` | long | 3 | leftover | cut_loser | none | 45% | 42% | 1/26 | -8.77 | 11 | 74 | +8.12 | -5.53 | -5.53 | +2.48 | PASS | 25.458 |
| `flatten_h3_sizeup` | long | 3 | leftover | list | sizeup | 45% | 42% | 1/26 | -8.77 | 11 | 74 | +8.12 | -5.53 | -5.53 | +2.48 | PASS | 25.458 |
| `flatten_h3_trail` | long | 3 | leftover | trail | none | 45% | 42% | 1/26 | -8.77 | 11 | 74 | +8.12 | -5.53 | -5.53 | +2.48 | PASS | 25.458 |
| `union_news_present_h1` | long | 1 | leftover | list | none | 44% | 35% | 0/26 | -4.90 | 9 | 49 | +4.30 | -3.87 | -2.56 | +3.82 | PASS | 25.414 |
| `flatten_h3_half` | long | 3 | half | list | none | 45% | 42% | 1/26 | -4.79 | 11 | 74 | +7.14 | -5.12 | -3.89 | +2.48 | PASS | 25.323 |
| `combo_ers_7030_shared` | mix | 5 | leftover | list | none | 47% | 42% | 2/26 | -1.98 | 0 | 0 | +6.95 | -8.08 | +18.27 | — | PASS | 25.137 |
| `union_h5_exit_alarm` | long | 5 | leftover | list | none | 47% | 54% | 4/26 | -8.94 | 29 | 150 | +7.34 | -7.68 | -3.86 | +19.49 | PASS | 25.136 |
| `union_rsi_os_h3` | long | 3 | leftover | list | none | 47% | 38% | 10/26 | -1.59 | 4 | 41 | +5.61 | -5.61 | -8.16 | -29.19 | PASS | 25.005 |
| `flatten_h3_time` | long | 3 | leftover | time | none | 45% | 42% | 1/26 | -8.04 | 11 | 74 | +7.37 | -5.73 | -2.54 | +2.48 | PASS | 24.996 |
| `short_extended_h1` | short | 1 | leftover | list | none | 53% | 31% | 1/26 | -7.66 | 22 | 65 | +5.46 | -6.24 | -15.23 | -5.49 | PASS | 24.925 |
| `union_news_vol_h1` | long | 1 | leftover | list | none | 47% | 27% | 0/26 | -3.14 | 5 | 19 | +4.28 | -4.60 | -1.56 | +16.94 | PASS | 24.925 |
| `union_h5_rankw` | long | 5 | rank_w | list | none | 47% | 58% | 4/26 | -10.73 | 32 | 149 | +7.50 | -8.84 | -9.75 | +21.23 | PASS | 24.798 |
| `union_blue_h1` | long | 1 | leftover | list | none | 44% | 31% | 0/26 | -5.64 | 11 | 43 | +4.25 | -4.07 | -3.85 | -4.15 | PASS | 24.637 |
| `union_blue_vol_h1` | long | 1 | leftover | list | none | 45% | 31% | 3/26 | -6.44 | 10 | 50 | +5.31 | -5.32 | -8.94 | +1.96 | PASS | 24.617 |
| `union_vol_g_h1` | long | 1 | leftover | list | none | 49% | 35% | 0/26 | -6.00 | 16 | 64 | +5.51 | -5.45 | -6.66 | +8.33 | PASS | 24.471 |
| `flatten_h3_rankw` | long | 3 | rank_w | list | none | 45% | 38% | 1/26 | -7.21 | 11 | 74 | +7.94 | -5.70 | -7.21 | +2.48 | PASS | 24.055 |
| `flatten_h5_topheavy` | long | 5 | topheavy | list | none | 44% | 54% | 2/26 | -4.38 | 19 | 107 | +8.62 | -8.14 | +1.17 | +3.80 | PASS | 23.965 |
| `short_extended_h3` | short | 3 | leftover | list | none | 54% | 46% | 1/26 | -7.59 | 40 | 128 | +8.53 | -9.03 | -13.56 | -29.34 | PASS | 23.77 |
| `union_news_present_h3` | long | 3 | leftover | list | none | 41% | 38% | 4/26 | -3.22 | 17 | 104 | +6.76 | -5.91 | -7.39 | -3.66 | PASS | 23.5 |
| `short_rsi_ob_h1` | short | 1 | leftover | list | none | 49% | 31% | 0/26 | -10.48 | 17 | 54 | +4.23 | -4.77 | -17.48 | -19.75 | PASS | 23.332 |
| `flatten_h5_half` | long | 5 | half | list | none | 44% | 54% | 1/26 | -5.05 | 19 | 107 | +7.81 | -7.00 | -0.55 | +3.80 | PASS | 23.026 |
| `union_blue_coil_h1` | long | 1 | leftover | list | none | 44% | 27% | 0/26 | -6.58 | 10 | 37 | +3.68 | -4.23 | -8.00 | -9.93 | PASS | 22.966 |
| `flatten_h3_topheavy` | long | 3 | topheavy | list | none | 45% | 38% | 1/26 | -6.64 | 11 | 74 | +6.20 | -5.72 | -5.37 | +2.48 | PASS | 22.792 |
| `union_ab_g_h3` | long | 3 | leftover | list | none | 39% | 31% | 5/26 | -6.07 | 16 | 93 | +7.68 | -5.70 | -6.82 | -10.44 | PASS | 22.751 |
| `flatten_h5_rankw` | long | 5 | rank_w | list | none | 44% | 50% | 2/26 | -5.72 | 19 | 107 | +8.25 | -8.53 | +0.12 | +3.80 | PASS | 22.571 |
| `short_last_red_h1` | short | 1 | leftover | list | none | 48% | 23% | 2/26 | -3.33 | 14 | 68 | +3.85 | -4.75 | -11.83 | -14.92 | PASS | 22.388 |
| `union_cond_h3` | long | 3 | leftover | list | none | 39% | 46% | 5/26 | -8.44 | 15 | 119 | +6.11 | -7.63 | -7.27 | -14.79 | PASS | 22.36 |
| `combo_fer_5050_shared` | mix | 5 | leftover | list | none | 41% | 50% | 2/26 | -4.00 | 0 | 0 | +8.17 | -8.52 | +5.96 | — | PASS | 21.915 |
| `union_vol_g_h5` | long | 5 | leftover | list | none | 45% | 42% | 4/26 | -2.67 | 36 | 139 | +12.33 | -10.84 | -7.37 | +8.09 | PASS | 21.62 |
| `union_news_vol_h3` | long | 3 | leftover | list | none | 48% | 46% | 1/26 | -8.21 | 9 | 51 | +4.25 | -5.30 | -9.66 | +189.75 | PASS | 21.361 |
| `union_candle_score_h3` | long | 3 | leftover | list | none | 43% | 42% | 1/26 | -3.27 | 24 | 136 | +6.88 | -6.32 | +0.86 | +7.94 | PASS | 20.966 |
| `probable_h5` | long | 5 | leftover | list | none | 49% | 31% | 7/26 | -11.74 | 38 | 151 | +6.75 | -10.63 | -13.41 | -3.12 | PASS | 20.87 |
| `union_white_h5` | long | 5 | leftover | list | none | 34% | 62% | 3/26 | -9.67 | 16 | 71 | +9.42 | -9.14 | +0.88 | +22.97 | PASS | 20.707 |
| `union_flow_in_h3` | long | 3 | leftover | list | none | 44% | 31% | 4/26 | -2.88 | 5 | 27 | +1.87 | -4.94 | -4.10 | -13.57 | PASS | 20.659 |
| `flatten_live_h3` | long | 3 | leftover | list | none | 58% | 12% | 0/26 | -6.24 | 4 | 14 | +8.60 | -8.85 | -6.24 | +6.87 | PASS | 20.399 |
| `union_white_coil_h3` | long | 3 | leftover | list | none | 39% | 46% | 5/26 | -15.63 | 9 | 47 | +5.20 | -7.71 | -15.04 | -20.56 | PASS | 19.89 |
| `union_coil_off_h1` | long | 1 | leftover | list | none | 41% | 23% | 0/26 | -7.40 | 8 | 51 | +3.83 | -4.18 | -10.88 | -12.89 | PASS | 19.535 |
| `short_alarm_h3` | short | 3 | leftover | list | none | 52% | 35% | 0/26 | -3.44 | 12 | 80 | +7.29 | -8.49 | -0.94 | -6.49 | PASS | 19.522 |
| `union_macd_xup_h1` | long | 1 | leftover | list | none | 38% | 31% | 2/26 | -0.59 | 4 | 28 | +2.83 | -4.75 | -12.89 | -15.92 | PASS | 18.793 |
| `short_rsi_ob_h3` | short | 3 | leftover | list | none | 49% | 35% | 0/26 | -10.40 | 34 | 119 | +5.80 | -7.06 | -16.06 | -121.10 | PASS | 18.311 |
| `union_cond_n4_h3` | long | 3 | leftover | list | none | 41% | 42% | 0/26 | -11.29 | 7 | 58 | +5.54 | -6.84 | -7.84 | -9.96 | PASS | 17.732 |
| `union_coil_green_h3` | long | 3 | leftover | list | none | 41% | 23% | 5/26 | -12.24 | 25 | 106 | +3.87 | -7.69 | -14.04 | -8.48 | PASS | 17.147 |
| `union_coil_off_h3` | long | 3 | leftover | list | none | 39% | 31% | 4/26 | -8.70 | 21 | 113 | +4.47 | -8.29 | -12.13 | -13.23 | PASS | 16.786 |
| `flatten_live_h5` | long | 5 | leftover | list | none | 46% | 27% | 0/26 | -5.51 | 5 | 16 | +9.01 | -11.62 | -5.51 | +5.13 | PASS | 16.556 |
| `union_news_g_h3` | long | 3 | leftover | list | none | 47% | 27% | 0/26 | -10.12 | 16 | 82 | +4.53 | -6.64 | -18.20 | +165.63 | PASS | 16.192 |
| `union_rsi_os_h1` | long | 1 | leftover | list | none | 49% | 27% | 0/26 | -16.92 | 3 | 23 | +2.48 | -6.29 | -22.54 | -2.98 | PASS | 15.448 |
| `union_news_head_h3` | long | 3 | leftover | list | none | 46% | 19% | 0/26 | -11.42 | 15 | 71 | +5.36 | -6.00 | -21.04 | +167.68 | PASS | 14.26 |
| `coil_h3_exit_alarm` | long | 3 | leftover | list | none | 38% | 27% | 3/26 | -8.85 | 17 | 120 | +4.23 | -7.55 | -8.38 | -14.04 | PASS | 14.241 |
| `union_coil_off_h5` | long | 5 | leftover | list | none | 42% | 35% | 4/26 | -14.85 | 28 | 146 | +3.90 | -57.28 | -10.85 | -22.30 | PASS | 13.802 |
| `union_white_h3` | long | 3 | leftover | list | none | 35% | 35% | 2/26 | -16.81 | 12 | 61 | +7.40 | -8.14 | -15.44 | +0.38 | PASS | 13.35 |
| `union_rsi_h1` | long | 1 | leftover | list | none | 43% | 15% | 0/26 | -19.88 | 10 | 76 | +3.69 | -6.11 | -26.37 | -18.21 | PASS | 12.671 |
| `flatten_vol_g_h3` | long | 3 | leftover | list | none | 46% | 19% | 1/26 | -17.31 | 5 | 28 | +9.63 | -9.84 | -25.48 | -21.05 | PASS | 12.59 |
| `union_news_g_h5` | long | 5 | leftover | list | none | 38% | 42% | 0/26 | -8.97 | 18 | 117 | +5.46 | -8.21 | -12.84 | +93.04 | PASS | 11.947 |
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
| `short_r_down_h1` *(thin)* | short | 1 | leftover | list | none | — | 0% | 0/26 | +0.00 | 0 | 0 | — | — | +0.00 | +0.00 | PASS | -15.0 |
| `short_r_down_h3` *(thin)* | short | 3 | leftover | list | none | — | 0% | 0/26 | +0.00 | 0 | 0 | — | — | +0.00 | +0.00 | PASS | -15.0 |
| `union_r_up_h1` *(thin)* | long | 1 | leftover | list | none | — | 0% | 0/26 | +0.00 | 0 | 0 | — | — | +0.00 | +0.00 | PASS | -15.0 |
| `union_r_up_h3` *(thin)* | long | 3 | leftover | list | none | — | 0% | 0/26 | +0.00 | 0 | 0 | — | — | +0.00 | +0.00 | PASS | -15.0 |
| `union_rsi_os_macd_h1` *(thin)* | long | 1 | leftover | list | none | 22% | 4% | 0/26 | -20.68 | 1 | 6 | +4.44 | -7.74 | -20.68 | -13.91 | PASS | -22.241 |
| `union_hot_n4_holdup` | long | 1 | leftover | list | holdup | 53% | 62% | 25/26 | +16.54 | 16 | 33 | +7.37 | -5.21 | +56.52 | +51.64 | PASS | 76.528 |
