# Factor strategy mine — 2026-08-13 → 2026-09-18

Leak-free 09:30 recipes: **270** · candidate rows **1790** · fill `09:30 open, whole shares, Futubull fees, leftover split, sell first, hard-red sit, cash+holdings audit`.

Cash book: $10k, whole shares, Futubull fees, leftover split, sell first, min-hold, 09:30 open, hard-red S≤−3 sit, shorts marked as a liability. Each session starts from leftover cash and lots actually held (butterfly). Cash-start buttons wake a sleeve on date X with $10k and no lots (same rules). Stock investigator quotes 09:30 cameras / coaches / news from repo files. Size / sell / S-boost tweaks sit on the same ledger. Signal-only % is the old equal-weight path (not a fill). `flatten_h*` = wish-list (io/HOLD mornings still buy). `flatten_live_*` = only when the live flatten gate fires. Research only — does not change live `flatten_robust`.

Scoreboard files: slim `factor_mine.json` (stats / recipes / series) plus gzip shards in `factor_mine/shards/` (`books`, `starts`, `daily`, `probe`, `sim`). One file used to be 104MB and GitHub rejected the publish (100MB cap).

Combination books: **77** mixes on the same $10k cash ledger (shared leftover or split sleeves, official 09:30 / 16:00, hard-red sit, owner min-hold). Outperformers: `combo_sh_5050_shared`, `combo_sh_3070_shared`, `combo_seh_451540_shared`, `combo_ps_7030_shared`, `combo_sh_7030_shared`, `combo_se_5050_skip`, `combo_es_8020_shared`, `combo_es_9010_shared`, `combo_p2s_5050_shared`, `combo_se_3070_shared`, `combo_ps_5050_shared`, `combo_se1_5050_shared`, `combo_ers_7030_shared`, `combo_ser_5050_shared`, `combo_jer_5050_shared`, `combo_e1s_7030_shared`, `combo_sj_5050_shared`, `combo_sj_3070_shared`, `combo_sj_7030_shared`, `combo_sn_5050_shared`.

A combo outperforms its members on the cash book when the audit passes and either (1) Book% is strictly higher than every member, or (2) Book% stays within 2pp of the richest member, max drawdown is strictly smaller than every member's drawdown, start-rate is at least the best member, both halves are green, and the worst session is no worse than the worst member session. Effectiveness / Signal% do not decide WIN.

Action blotters: [FACTOR_MINE_ACTION.md](FACTOR_MINE_ACTION.md).

| Strategy | Side | H | Size | Sell | Boost | Win% | $ days | Starts YES | Med start | Top-g | Losers | AvgW | AvgL | Book% | Signal% | Audit | Eff |
|---|---|---:|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---:|
| `combo_p2s_5050_shared` | mix | 5 | leftover | list | none | 65% | 69% | 21/26 | +12.94 | 0 | 0 | +4.84 | -4.18 | +27.40 | — | PASS | 73.59 |
| `combo_ps_5050_shared` | mix | 5 | leftover | list | none | 66% | 69% | 21/26 | +10.23 | 0 | 0 | +4.71 | -4.50 | +26.55 | — | PASS | 73.032 |
| `combo_ps_7030_shared` | mix | 5 | leftover | list | none | 64% | 69% | 21/26 | +13.82 | 0 | 0 | +4.77 | -4.39 | +28.82 | — | PASS | 72.603 |
| `combo_sh_3070_shared` | mix | 5 | leftover | list | none | 56% | 62% | 21/26 | +5.47 | 0 | 0 | +6.00 | -3.81 | +36.12 | — | PASS | 71.144 |
| `combo_sh_5050_shared` | mix | 5 | leftover | list | none | 57% | 65% | 17/26 | +3.60 | 0 | 0 | +5.95 | -3.88 | +36.73 | — | PASS | 68.412 |
| `combo_sh_7030_shared` | mix | 5 | leftover | list | none | 57% | 69% | 16/26 | +1.46 | 0 | 0 | +5.92 | -3.93 | +28.51 | — | PASS | 66.864 |
| `combo_sn_5050_shared` | mix | 5 | leftover | list | none | 58% | 65% | 16/26 | +2.45 | 0 | 0 | +4.48 | -4.86 | +12.85 | — | PASS | 61.201 |
| `combo_seh_451540_shared` | mix | 5 | leftover | list | none | 48% | 69% | 17/26 | +2.47 | 0 | 0 | +6.80 | -6.24 | +29.59 | — | PASS | 60.806 |
| `combo_se1_5050_shared` | mix | 5 | leftover | list | none | 57% | 65% | 16/26 | +2.57 | 0 | 0 | +5.05 | -6.44 | +24.70 | — | PASS | 60.544 |
| `combo_e1s_7030_shared` | mix | 5 | leftover | list | none | 56% | 62% | 16/26 | +2.91 | 0 | 0 | +5.15 | -6.25 | +21.28 | — | PASS | 59.079 |
| `combo_es_9010_shared` | mix | 5 | leftover | list | none | 46% | 54% | 20/26 | +2.93 | 0 | 0 | +7.38 | -6.88 | +27.65 | — | PASS | 56.968 |
| `combo_es_8020_shared` | mix | 5 | leftover | list | none | 48% | 50% | 20/26 | +2.73 | 0 | 0 | +7.00 | -7.24 | +28.11 | — | PASS | 56.831 |
| `combo_se_5050_skip` | mix | 5 | leftover | list | none | 60% | 50% | 16/26 | +2.21 | 0 | 0 | +6.44 | -7.95 | +28.21 | — | PASS | 56.639 |
| `combo_se_3070_shared` | mix | 5 | leftover | list | none | 51% | 46% | 20/26 | +3.06 | 0 | 0 | +6.83 | -7.51 | +27.34 | — | PASS | 56.484 |
| `combo_ser_5050_shared` | mix | 5 | leftover | list | none | 52% | 38% | 16/26 | +0.79 | 0 | 0 | +6.76 | -7.83 | +23.75 | — | PASS | 50.393 |
| `combo_ers_7030_shared` | mix | 5 | leftover | list | none | 47% | 38% | 15/26 | +1.15 | 0 | 0 | +6.95 | -8.08 | +24.34 | — | PASS | 47.815 |
| `combo_sj_5050_shared` | mix | 5 | leftover | list | none | 54% | 65% | 12/26 | -1.05 | 0 | 0 | +5.17 | -4.62 | +17.47 | — | PASS | 46.918 |
| `combo_sj_3070_shared` | mix | 5 | leftover | list | none | 54% | 62% | 10/26 | -1.48 | 0 | 0 | +5.16 | -4.62 | +16.83 | — | PASS | 44.122 |
| `combo_sj_7030_shared` | mix | 5 | leftover | list | none | 52% | 58% | 12/26 | -1.46 | 0 | 0 | +5.22 | -4.63 | +13.02 | — | PASS | 43.97 |
| `combo_jer_5050_shared` | mix | 5 | leftover | list | none | 43% | 46% | 6/26 | -2.46 | 0 | 0 | +6.73 | -6.47 | +23.75 | — | PASS | 32.918 |
| `union_news_pack_net2_h1` | long | 1 | leftover | list | none | 60% | 42% | 21/26 | +17.85 | 1 | 6 | +3.98 | -2.68 | +28.55 | +22.32 | PASS | 71.955 |
| `union_news_pack_h1` | long | 1 | leftover | list | none | 58% | 42% | 21/26 | +14.36 | 2 | 9 | +3.77 | -3.37 | +24.09 | +21.62 | PASS | 68.433 |
| `union_hot_n4_h1` | long | 1 | leftover | list | none | 48% | 42% | 21/26 | +8.71 | 16 | 29 | +6.50 | -3.48 | +27.68 | +45.62 | PASS | 68.12 |
| `union_news_pack_net3_h1` | long | 1 | leftover | list | none | 58% | 35% | 17/26 | +9.19 | 1 | 6 | +3.77 | -2.68 | +19.12 | +17.01 | PASS | 63.982 |
| `combo_seh_502525_split` | mix | 5 | leftover | list | none | 48% | 65% | 21/26 | +3.51 | 0 | 0 | +6.98 | -5.76 | +16.66 | — | PASS | 62.517 |
| `combo_hn_7030_shared` | mix | 5 | leftover | list | none | 50% | 42% | 19/26 | +4.09 | 0 | 0 | +4.76 | -4.16 | +19.28 | — | PASS | 62.188 |
| `combo_hn_5050_shared` | mix | 5 | leftover | list | none | 51% | 42% | 19/26 | +0.92 | 0 | 0 | +4.79 | -4.14 | +14.55 | — | PASS | 61.882 |
| `combo_seh_333_split` | mix | 5 | leftover | list | none | 49% | 58% | 21/26 | +4.43 | 0 | 0 | +6.74 | -5.99 | +19.37 | — | PASS | 61.396 |
| `combo_he1_5050_shared` | mix | 5 | leftover | list | none | 49% | 42% | 20/26 | +2.99 | 0 | 0 | +5.39 | -5.06 | +16.12 | — | PASS | 60.523 |
| `combo_seh_333_skip` | mix | 5 | leftover | list | none | 51% | 65% | 16/26 | +2.01 | 0 | 0 | +6.70 | -6.16 | +27.67 | — | PASS | 60.019 |
| `combo_sn_3070_shared` | mix | 5 | leftover | list | none | 56% | 62% | 16/26 | +1.93 | 0 | 0 | +4.58 | -4.69 | +11.67 | — | PASS | 59.947 |
| `combo_sn_7030_shared` | mix | 5 | leftover | list | none | 58% | 62% | 16/26 | +1.78 | 0 | 0 | +4.34 | -5.04 | +11.24 | — | PASS | 59.877 |
| `combo_hj_5050_shared` | mix | 5 | leftover | list | none | 46% | 38% | 19/26 | +1.85 | 0 | 0 | +5.34 | -3.83 | +12.75 | — | PASS | 59.356 |
| `combo_seh_601525_shared` | mix | 5 | leftover | list | none | 48% | 69% | 16/26 | +0.69 | 0 | 0 | +6.79 | -6.26 | +26.05 | — | PASS | 59.281 |
| `union_e_fresh_h3` | long | 3 | leftover | list | none | 48% | 54% | 20/26 | +3.60 | 19 | 82 | +9.20 | -7.47 | +26.85 | -7.87 | PASS | 58.096 |
| `combo_seh_502525_shared` | mix | 5 | leftover | list | none | 48% | 62% | 16/26 | +1.22 | 0 | 0 | +6.81 | -6.12 | +25.94 | — | PASS | 58.028 |
| `combo_seh_403525_shared` | mix | 5 | leftover | list | none | 48% | 62% | 16/26 | +1.64 | 0 | 0 | +6.87 | -6.05 | +24.77 | — | PASS | 57.963 |
| `combo_seh_333_shared` | mix | 5 | leftover | list | none | 48% | 62% | 16/26 | +2.01 | 0 | 0 | +6.75 | -6.13 | +24.91 | — | PASS | 57.949 |
| `combo_seh_333_weather` | mix | 5 | leftover | list | none | 48% | 62% | 16/26 | +2.01 | 0 | 0 | +6.75 | -6.13 | +24.91 | — | PASS | 57.949 |
| `combo_ee1_3070_shared` | mix | 5 | leftover | list | none | 40% | 54% | 20/26 | +3.60 | 0 | 0 | +9.20 | -7.47 | +26.85 | — | PASS | 57.834 |
| `combo_ee1_5050_shared` | mix | 5 | leftover | list | none | 40% | 54% | 20/26 | +3.60 | 0 | 0 | +9.20 | -7.47 | +26.85 | — | PASS | 57.834 |
| `combo_ee1_7030_shared` | mix | 5 | leftover | list | none | 40% | 54% | 20/26 | +3.60 | 0 | 0 | +9.20 | -7.47 | +26.85 | — | PASS | 57.834 |
| `combo_eh_5050_shared` | mix | 5 | leftover | list | none | 41% | 46% | 20/26 | +3.97 | 0 | 0 | +7.59 | -5.89 | +22.18 | — | PASS | 57.52 |
| `short_last_red_h3` | short | 3 | leftover | list | none | 55% | 46% | 23/26 | +4.34 | 16 | 107 | +5.11 | -5.95 | -1.39 | -14.78 | PASS | 57.454 |
| `combo_seh_404020_shared` | mix | 5 | leftover | list | none | 49% | 62% | 15/26 | +1.08 | 0 | 0 | +6.68 | -6.18 | +24.25 | — | PASS | 57.05 |
| `combo_se_5050_split` | mix | 5 | leftover | list | none | 49% | 54% | 20/26 | +4.78 | 0 | 0 | +7.31 | -6.72 | +18.39 | — | PASS | 56.919 |
| `short_news_head_h3` | short | 3 | leftover | list | none | 59% | 62% | 16/26 | +1.80 | 5 | 33 | +6.53 | -6.31 | +10.94 | +18.37 | PASS | 56.246 |
| `combo_eh_3070_shared` | mix | 5 | leftover | list | none | 42% | 38% | 20/26 | +4.70 | 0 | 0 | +7.30 | -6.16 | +22.47 | — | PASS | 55.841 |
| `union_w_hot_cond_h1` | long | 1 | leftover | list | none | 47% | 42% | 17/26 | +1.20 | 24 | 48 | +5.55 | -5.39 | +0.74 | +14.67 | PASS | 55.822 |
| `short_news_or_h3` | short | 3 | leftover | list | none | 58% | 54% | 16/26 | +1.97 | 7 | 38 | +5.42 | -5.57 | +12.51 | +20.71 | PASS | 55.551 |
| `short_news_r_h3` | short | 3 | leftover | list | none | 58% | 54% | 16/26 | +1.97 | 7 | 38 | +5.42 | -5.57 | +12.51 | +20.71 | PASS | 55.551 |
| `union_w_hot_candle_h1` | long | 1 | leftover | list | none | 50% | 42% | 15/26 | +0.72 | 23 | 49 | +4.95 | -4.78 | +2.82 | +20.98 | PASS | 55.276 |
| `combo_se_5050_shared` | mix | 5 | leftover | list | none | 56% | 50% | 16/26 | +2.16 | 0 | 0 | +6.64 | -7.54 | +26.26 | — | PASS | 55.217 |
| `combo_se_5050_weather` | mix | 5 | leftover | list | none | 56% | 50% | 16/26 | +2.16 | 0 | 0 | +6.64 | -7.54 | +26.26 | — | PASS | 55.217 |
| `short_macd_dn_h3` | short | 3 | leftover | list | none | 52% | 42% | 23/26 | +2.18 | 19 | 101 | +4.27 | -5.71 | -7.01 | -13.13 | PASS | 54.972 |
| `combo_her_5050_shared` | mix | 5 | leftover | list | none | 42% | 50% | 19/26 | +3.35 | 0 | 0 | +7.28 | -7.30 | +17.48 | — | PASS | 54.947 |
| `union_news_or_h1` | long | 1 | leftover | list | none | 53% | 35% | 14/26 | +0.70 | 4 | 26 | +4.18 | -3.96 | +2.48 | +20.26 | PASS | 53.898 |
| `union_hot_score_h3` | long | 3 | leftover | list | none | 42% | 58% | 18/26 | +1.01 | 40 | 133 | +12.45 | -8.74 | +10.05 | +211.01 | PASS | 53.838 |
| `union_vol_green_h1` | long | 1 | leftover | list | none | 44% | 42% | 15/26 | +2.08 | 17 | 41 | +5.92 | -5.00 | +9.38 | -3.62 | PASS | 53.807 |
| `union_break10_h1` | long | 1 | leftover | list | none | 49% | 38% | 15/26 | +1.15 | 19 | 42 | +5.78 | -5.38 | -1.84 | +7.79 | PASS | 53.615 |
| `combo_ehs_702010_shared` | mix | 5 | leftover | list | none | 41% | 50% | 16/26 | +0.66 | 0 | 0 | +7.71 | -5.54 | +21.96 | — | PASS | 53.547 |
| `short_news_r_h1` | short | 1 | leftover | list | none | 52% | 35% | 16/26 | +1.22 | 4 | 16 | +2.97 | -3.18 | +0.49 | +9.31 | PASS | 53.497 |
| `combo_eh_7030_shared` | mix | 5 | leftover | list | none | 37% | 46% | 16/26 | +2.69 | 0 | 0 | +8.38 | -5.58 | +21.52 | — | PASS | 52.941 |
| `union_e_fresh_h1` | long | 1 | leftover | list | none | 48% | 38% | 17/26 | +0.41 | 8 | 42 | +4.76 | -6.44 | +9.66 | +6.30 | PASS | 52.623 |
| `combo_nse_333_shared` | mix | 5 | leftover | list | none | 50% | 42% | 15/26 | +1.52 | 0 | 0 | +5.70 | -6.49 | +23.37 | — | PASS | 52.432 |
| `union_news_g_cond_h1` | long | 1 | leftover | list | none | 52% | 31% | 14/26 | +0.04 | 4 | 27 | +4.18 | -4.27 | +1.63 | +19.61 | PASS | 52.178 |
| `combo_en_7030_shared` | mix | 5 | leftover | list | none | 41% | 42% | 15/26 | +0.83 | 0 | 0 | +6.30 | -6.29 | +22.74 | — | PASS | 50.42 |
| `short_rsi_ob_h3` | short | 3 | leftover | list | none | 49% | 46% | 17/26 | +2.19 | 27 | 105 | +5.80 | -7.85 | -3.23 | -127.12 | PASS | 48.455 |
| `union_earn_react_h3` | long | 3 | leftover | list | none | 42% | 46% | 17/26 | +0.34 | 15 | 81 | +8.88 | -9.12 | +20.88 | -19.31 | PASS | 48.402 |
| `short_last_red_h1` | short | 1 | leftover | list | none | 50% | 27% | 16/26 | +0.58 | 9 | 62 | +3.70 | -4.32 | -8.30 | -9.77 | PASS | 47.932 |
| `union_ret_5_h3` | long | 3 | leftover | list | none | 42% | 54% | 13/26 | +0.33 | 34 | 136 | +10.16 | -8.28 | +13.20 | +34.37 | PASS | 47.195 |
| `union_news_pack_h3` | long | 3 | leftover | list | none | 45% | 31% | 16/26 | +4.54 | 5 | 23 | +5.34 | -7.33 | -6.73 | -3.09 | PASS | 45.017 |
| `combo_hn_3070_shared` | mix | 5 | leftover | list | none | 52% | 50% | 9/26 | -1.12 | 0 | 0 | +4.81 | -4.13 | +9.94 | — | PASS | 43.327 |
| `combo_snj_333_shared` | mix | 5 | leftover | list | none | 51% | 58% | 10/26 | -1.45 | 0 | 0 | +4.66 | -4.73 | +11.32 | — | PASS | 42.27 |
| `combo_ehs_601525_shared` | mix | 5 | leftover | list | none | 45% | 54% | 12/26 | -0.12 | 0 | 0 | +7.04 | -5.88 | +22.47 | — | PASS | 41.487 |
| `union_hot_score_h1` | long | 1 | leftover | list | none | 48% | 46% | 11/26 | -1.16 | 27 | 55 | +5.99 | -5.69 | +1.95 | +22.12 | PASS | 41.047 |
| `combo_jse_333_shared` | mix | 5 | leftover | list | none | 55% | 54% | 8/26 | -1.29 | 0 | 0 | +6.01 | -6.02 | +26.79 | — | PASS | 40.895 |
| `combo_se_7030_shared` | mix | 5 | leftover | list | none | 56% | 46% | 12/26 | +0.00 | 0 | 0 | +6.48 | -7.56 | +22.60 | — | PASS | 40.195 |
| `union_ret_5_h1` | long | 1 | leftover | list | none | 45% | 38% | 12/26 | -2.23 | 20 | 58 | +6.19 | -5.04 | +2.93 | +10.88 | PASS | 39.641 |
| `union_rsi_os_h3` | long | 3 | leftover | list | none | 51% | 38% | 13/26 | +0.50 | 2 | 32 | +5.61 | -5.61 | -6.21 | -19.79 | PASS | 39.133 |
| `combo_hf_5050_shared` | mix | 5 | leftover | list | none | 46% | 54% | 10/26 | -3.23 | 0 | 0 | +6.47 | -5.45 | +5.23 | — | PASS | 38.338 |
| `union_news_g_h1` | long | 1 | leftover | list | none | 53% | 35% | 9/26 | +0.00 | 3 | 26 | +3.97 | -4.40 | +2.98 | +18.26 | PASS | 38.292 |
| `combo_je1_5050_shared` | mix | 5 | leftover | list | none | 49% | 38% | 10/26 | -2.37 | 0 | 0 | +4.76 | -5.21 | +9.19 | — | PASS | 37.834 |
| `flatten_live_h1_topheavy` | long | 1 | topheavy | list | none | 54% | 12% | 7/26 | +0.00 | 1 | 5 | +6.75 | -3.75 | +7.19 | +4.45 | PASS | 37.243 |
| `short_alarm_h1` | short | 1 | leftover | list | none | 51% | 15% | 12/26 | +0.00 | 7 | 40 | +4.60 | -2.94 | +1.71 | +1.58 | PASS | 36.513 |
| `flatten_live_h1` | long | 1 | leftover | list | none | 54% | 12% | 7/26 | +0.00 | 1 | 5 | +6.31 | -4.05 | +8.22 | +4.45 | PASS | 36.203 |
| `flatten_live_h1_cut` | long | 1 | leftover | cut_loser | none | 54% | 12% | 7/26 | +0.00 | 1 | 5 | +6.31 | -4.05 | +8.22 | +4.45 | PASS | 36.203 |
| `flatten_live_h1_sboost` | long | 1 | leftover | list | both | 54% | 12% | 7/26 | +0.00 | 1 | 5 | +6.31 | -4.05 | +8.22 | +4.45 | PASS | 36.203 |
| `flatten_live_h1_sizeup` | long | 1 | leftover | list | sizeup | 54% | 12% | 7/26 | +0.00 | 1 | 5 | +6.31 | -4.05 | +8.22 | +4.45 | PASS | 36.203 |
| `flatten_live_h1_time` | long | 1 | leftover | time | none | 54% | 12% | 7/26 | +0.00 | 1 | 5 | +6.31 | -4.05 | +8.22 | +4.45 | PASS | 36.203 |
| `flatten_live_h1_trail` | long | 1 | leftover | trail | none | 54% | 12% | 7/26 | +0.00 | 1 | 5 | +6.31 | -4.05 | +8.22 | +4.45 | PASS | 36.203 |
| `flatten_live_h1_half` | long | 1 | half | list | none | 54% | 12% | 7/26 | +0.00 | 1 | 5 | +6.58 | -3.90 | +3.74 | +4.45 | PASS | 36.151 |
| `union_join_vol_green_h1` | long | 1 | leftover | list | none | 44% | 31% | 10/26 | -3.04 | 10 | 31 | +4.92 | -4.17 | +6.92 | +1.42 | PASS | 36.142 |
| `yday_gainer_h1` | long | 1 | leftover | list | none | 52% | 31% | 7/26 | -0.08 | 12 | 53 | +4.85 | -3.96 | +6.12 | +19.05 | PASS | 35.409 |
| `combo_ej_5050_shared` | mix | 5 | leftover | list | none | 47% | 46% | 6/26 | -2.10 | 0 | 0 | +6.44 | -6.20 | +25.70 | — | PASS | 35.284 |
| `union_flow_in_h1` | long | 1 | leftover | list | none | 46% | 31% | 6/26 | -2.10 | 4 | 6 | +4.41 | -4.60 | +5.18 | +12.48 | PASS | 34.901 |
| `combo_nj_5050_shared` | mix | 5 | leftover | list | none | 47% | 31% | 9/26 | -3.15 | 0 | 0 | +4.33 | -4.49 | +0.65 | — | PASS | 34.802 |
| `flatten_live_h1_rankw` | long | 1 | rank_w | list | none | 54% | 12% | 6/26 | +0.00 | 1 | 5 | +6.14 | -4.12 | +4.03 | +4.45 | PASS | 34.27 |
| `combo_sf_7030_shared` | mix | 5 | leftover | list | none | 53% | 58% | 6/26 | -3.40 | 0 | 0 | +6.57 | -6.78 | +10.13 | — | PASS | 33.999 |
| `union_w_hot_cond_h3` | long | 3 | leftover | list | none | 41% | 54% | 10/26 | -1.82 | 37 | 129 | +10.48 | -8.20 | +6.76 | +178.61 | PASS | 33.926 |
| `union_hot_n12_h1` | long | 1 | leftover | list | none | 50% | 42% | 5/26 | -2.91 | 33 | 77 | +5.02 | -5.36 | -4.54 | +15.23 | PASS | 33.564 |
| `union_w_hot_candle_h3` | long | 3 | leftover | list | none | 44% | 54% | 9/26 | -1.12 | 35 | 124 | +9.22 | -8.79 | +5.91 | +183.86 | PASS | 33.258 |
| `union_break10_h3` | long | 3 | leftover | list | none | 47% | 42% | 10/26 | +0.00 | 28 | 105 | +9.03 | -7.94 | -1.47 | +33.28 | PASS | 32.665 |
| `combo_en_5050_shared` | mix | 5 | leftover | list | none | 45% | 42% | 6/26 | -0.19 | 0 | 0 | +5.83 | -6.55 | +20.87 | — | PASS | 32.579 |
| `combo_ne1_5050_shared` | mix | 5 | leftover | list | none | 51% | 31% | 5/26 | -1.81 | 0 | 0 | +4.17 | -5.50 | +8.27 | — | PASS | 31.805 |
| `combo_ef_7030_shared` | mix | 5 | leftover | list | none | 43% | 58% | 7/26 | -0.90 | 0 | 0 | +8.00 | -7.83 | +13.93 | — | PASS | 31.481 |
| `union_earn_react_h1` | long | 1 | leftover | list | none | 48% | 27% | 11/26 | +0.00 | 7 | 46 | +4.83 | -7.92 | +1.92 | +1.53 | PASS | 31.477 |
| `union_news_g_conv_h1` | long | 1 | conviction | list | none | 53% | 35% | 2/26 | -7.88 | 1 | 13 | +3.78 | -4.72 | +1.09 | +14.81 | PASS | 31.305 |
| `union_vol_g_h3` | long | 3 | leftover | list | none | 47% | 46% | 10/26 | -0.95 | 24 | 103 | +9.20 | -8.28 | -4.31 | +18.06 | PASS | 31.303 |
| `ohlc_hot_coil_h1` | long | 1 | leftover | list | none | 51% | 35% | 5/26 | -1.70 | 11 | 20 | +3.23 | -3.99 | -18.41 | -5.60 | PASS | 31.206 |
| `short_alarm_h3` | short | 3 | leftover | list | none | 54% | 31% | 12/26 | +0.00 | 11 | 77 | +7.29 | -8.49 | +2.59 | -0.13 | PASS | 30.912 |
| `combo_ner_5050_shared` | mix | 5 | leftover | list | none | 42% | 42% | 6/26 | -1.05 | 0 | 0 | +5.96 | -6.88 | +18.79 | — | PASS | 30.715 |
| `union_h1_topheavy` | long | 1 | topheavy | list | none | 42% | 42% | 3/26 | -6.23 | 8 | 41 | +4.03 | -3.76 | +1.95 | +7.77 | PASS | 30.436 |
| `combo_sf_5050_shared` | mix | 5 | leftover | list | none | 53% | 58% | 2/26 | -5.54 | 0 | 0 | +6.99 | -6.61 | +8.78 | — | PASS | 30.398 |
| `union_news_or_net2_h1` | long | 1 | leftover | list | none | 47% | 27% | 4/26 | -2.00 | 1 | 19 | +4.24 | -4.22 | -2.37 | +3.20 | PASS | 28.897 |
| `combo_nf_5050_shared` | mix | 5 | leftover | list | none | 43% | 58% | 1/26 | -5.92 | 0 | 0 | +6.70 | -5.70 | +2.29 | — | PASS | 28.866 |
| `union_white_h1` | long | 1 | leftover | list | none | 36% | 35% | 6/26 | -2.75 | 3 | 20 | +5.44 | -4.76 | +0.40 | -5.17 | PASS | 28.635 |
| `probable_probable_ok_h1` | long | 1 | leftover | list | none | 52% | 23% | 1/26 | -0.26 | 6 | 19 | +3.50 | -3.15 | -1.51 | +9.93 | PASS | 28.21 |
| `union_last_green_h1` | long | 1 | leftover | list | none | 47% | 31% | 0/26 | -5.23 | 10 | 35 | +4.37 | -3.42 | -0.70 | +8.69 | PASS | 28.1 |
| `combo_fh_7030_shared` | mix | 5 | leftover | list | none | 45% | 58% | 0/26 | -3.92 | 0 | 0 | +6.58 | -5.65 | -0.24 | — | PASS | 27.994 |
| `combo_jf_5050_shared` | mix | 5 | leftover | list | none | 43% | 58% | 1/26 | -5.97 | 0 | 0 | +6.12 | -5.53 | +2.93 | — | PASS | 27.976 |
| `union_news_missing_h3` | long | 3 | leftover | list | none | 33% | 27% | 1/26 | -1.82 | 2 | 11 | +9.22 | -3.20 | +1.25 | -2.44 | PASS | 27.932 |
| `union_news_head_h1` | long | 1 | leftover | list | none | 51% | 23% | 2/26 | -2.32 | 4 | 20 | +4.20 | -4.37 | -5.89 | +16.00 | PASS | 27.931 |
| `union_vol_g_h1` | long | 1 | leftover | list | none | 48% | 35% | 3/26 | -4.25 | 14 | 53 | +5.67 | -5.38 | -4.94 | +10.70 | PASS | 27.776 |
| `probable_h1` | long | 1 | leftover | list | none | 51% | 23% | 3/26 | -1.09 | 11 | 52 | +3.95 | -3.65 | -1.65 | +15.48 | PASS | 27.685 |
| `combo_sf_3070_shared` | mix | 5 | leftover | list | none | 51% | 54% | 1/26 | -5.90 | 0 | 0 | +7.32 | -6.76 | +7.17 | — | PASS | 27.661 |
| `union_candle_score_h1` | long | 1 | leftover | list | none | 49% | 27% | 0/26 | -2.91 | 17 | 46 | +4.32 | -3.76 | -1.78 | +5.89 | PASS | 27.296 |
| `union_vol_ab_h1` | long | 1 | leftover | list | none | 50% | 31% | 0/26 | -3.34 | 13 | 34 | +5.20 | -4.51 | -2.45 | +47.64 | PASS | 27.287 |
| `union_news_or_net4_conv_h1` | long | 1 | conviction | list | none | 49% | 35% | 0/26 | -12.24 | 1 | 9 | +3.50 | -4.52 | -3.84 | +0.19 | PASS | 27.076 |
| `union_macd_up_h1` | long | 1 | leftover | list | none | 44% | 27% | 3/26 | -3.46 | 11 | 44 | +4.52 | -4.21 | -2.04 | +9.81 | PASS | 27.053 |
| `union_h1_rankw` | long | 1 | rank_w | list | none | 42% | 42% | 0/26 | -6.68 | 8 | 41 | +4.01 | -3.80 | -2.88 | +7.77 | PASS | 26.738 |
| `combo_fse_333_shared` | mix | 5 | leftover | list | none | 48% | 58% | 1/26 | -3.79 | 0 | 0 | +6.72 | -7.55 | +9.14 | — | PASS | 26.604 |
| `flatten_h1` | long | 1 | leftover | list | none | 41% | 38% | 0/26 | -7.44 | 4 | 29 | +4.13 | -3.48 | -0.75 | -5.26 | PASS | 26.588 |
| `union_blue_h1` | long | 1 | leftover | list | none | 44% | 23% | 3/26 | -2.54 | 7 | 34 | +4.16 | -4.07 | -0.79 | -0.25 | PASS | 26.393 |
| `union_h1` | long | 1 | leftover | list | none | 42% | 38% | 0/26 | -6.45 | 8 | 41 | +4.01 | -3.77 | -1.63 | +7.77 | PASS | 26.185 |
| `union_h1_sizeup` | long | 1 | leftover | list | sizeup | 42% | 38% | 0/26 | -6.45 | 8 | 41 | +4.01 | -3.77 | -1.63 | +7.77 | PASS | 26.185 |
| `union_h1_trail` | long | 1 | leftover | trail | none | 42% | 38% | 0/26 | -6.45 | 8 | 41 | +4.01 | -3.77 | -1.63 | +7.77 | PASS | 26.185 |
| `union_h3_cut` | long | 3 | leftover | cut_loser | none | 43% | 50% | 0/26 | -7.60 | 17 | 98 | +8.80 | -5.62 | -3.33 | +6.09 | PASS | 26.088 |
| `union_h3_trail` | long | 3 | leftover | trail | none | 43% | 50% | 0/26 | -7.60 | 17 | 98 | +8.80 | -5.62 | -3.33 | +6.09 | PASS | 26.088 |
| `union_h1_sboost` | long | 1 | leftover | list | both | 42% | 38% | 0/26 | -6.45 | 8 | 41 | +3.99 | -3.85 | -1.57 | +7.77 | PASS | 26.069 |
| `combo_fes_403030_shared` | mix | 5 | leftover | list | none | 46% | 58% | 1/26 | -3.85 | 0 | 0 | +6.94 | -7.40 | +7.48 | — | PASS | 25.899 |
| `combo_en_3070_shared` | mix | 5 | leftover | list | none | 45% | 35% | 1/26 | -1.75 | 0 | 0 | +5.85 | -6.43 | +18.25 | — | PASS | 25.746 |
| `union_h1_time` | long | 1 | leftover | time | none | 42% | 35% | 0/26 | -6.53 | 8 | 41 | +3.88 | -3.49 | -2.41 | +7.77 | PASS | 25.555 |
| `union_blue_vol_h1` | long | 1 | leftover | list | none | 45% | 27% | 4/26 | -1.38 | 7 | 40 | +5.57 | -5.47 | -4.74 | +8.05 | PASS | 25.548 |
| `union_join_present_h1` | long | 1 | leftover | list | none | 42% | 38% | 0/26 | -6.45 | 6 | 45 | +4.00 | -3.85 | -2.03 | +3.52 | PASS | 25.469 |
| `union_h1_cut` | long | 1 | leftover | cut_loser | none | 42% | 35% | 0/26 | -6.53 | 8 | 41 | +4.01 | -3.73 | -1.81 | +7.77 | PASS | 25.454 |
| `union_h3` | long | 3 | leftover | list | none | 43% | 50% | 0/26 | -7.60 | 17 | 98 | +8.80 | -5.97 | -4.88 | +6.09 | PASS | 25.405 |
| `union_h3_sizeup` | long | 3 | leftover | list | sizeup | 43% | 50% | 0/26 | -7.60 | 17 | 98 | +8.80 | -5.97 | -4.88 | +6.09 | PASS | 25.405 |
| `union_h3_time` | long | 3 | leftover | time | none | 43% | 50% | 0/26 | -7.60 | 17 | 98 | +8.04 | -5.72 | -2.86 | +6.09 | PASS | 25.363 |
| `union_news_vol_h1` | long | 1 | leftover | list | none | 49% | 23% | 0/26 | -1.91 | 5 | 16 | +4.36 | -4.82 | -0.26 | +18.37 | PASS | 25.346 |
| `union_h3_sboost` | long | 3 | leftover | list | both | 43% | 50% | 0/26 | -7.60 | 17 | 98 | +8.46 | -6.00 | -4.94 | +6.09 | PASS | 25.062 |
| `combo_ef_3070_shared` | mix | 5 | leftover | list | none | 45% | 58% | 1/26 | -3.89 | 0 | 0 | +8.20 | -7.92 | +3.45 | — | PASS | 25.043 |
| `flatten_h5` | long | 5 | leftover | list | none | 42% | 62% | 1/26 | -5.42 | 20 | 101 | +8.90 | -8.31 | +3.38 | -1.38 | PASS | 25.036 |
| `flatten_h5_cut` | long | 5 | leftover | cut_loser | none | 42% | 62% | 1/26 | -5.42 | 20 | 101 | +8.90 | -8.31 | +3.38 | -1.38 | PASS | 25.036 |
| `flatten_h5_sizeup` | long | 5 | leftover | list | sizeup | 42% | 62% | 1/26 | -5.42 | 20 | 101 | +8.90 | -8.31 | +3.38 | -1.38 | PASS | 25.036 |
| `flatten_h5_time` | long | 5 | leftover | time | none | 42% | 62% | 1/26 | -5.42 | 20 | 101 | +8.90 | -8.31 | +3.38 | -1.38 | PASS | 25.036 |
| `flatten_h5_trail` | long | 5 | leftover | trail | none | 42% | 62% | 1/26 | -5.42 | 20 | 101 | +8.90 | -8.31 | +3.38 | -1.38 | PASS | 25.036 |
| `combo_fe1_5050_shared` | mix | 5 | leftover | list | none | 46% | 54% | 0/26 | -6.44 | 0 | 0 | +5.89 | -6.59 | -1.30 | — | PASS | 24.925 |
| `union_vol_ab_h3` | long | 3 | leftover | list | none | 45% | 35% | 5/26 | -2.23 | 19 | 74 | +8.53 | -6.98 | -2.94 | +48.75 | PASS | 24.809 |
| `union_coil_green_h1` | long | 1 | leftover | list | none | 45% | 31% | 0/26 | -4.02 | 12 | 32 | +3.18 | -3.97 | -2.46 | -1.22 | PASS | 24.789 |
| `flatten_h5_sboost` | long | 5 | leftover | list | both | 42% | 62% | 1/26 | -5.42 | 20 | 101 | +8.68 | -8.12 | +0.96 | -1.38 | PASS | 24.654 |
| `union_e_green_h3` | long | 3 | leftover | list | none | 42% | 35% | 11/26 | +0.00 | 6 | 45 | +5.47 | -8.76 | -0.88 | -13.35 | PASS | 24.644 |
| `union_vol_green_h3` | long | 3 | leftover | list | none | 49% | 27% | 7/26 | -2.81 | 29 | 89 | +7.58 | -8.05 | -12.42 | +17.60 | PASS | 24.634 |
| `union_join_g_h1` | long | 1 | leftover | list | none | 44% | 35% | 0/26 | -6.38 | 8 | 48 | +3.85 | -4.06 | -2.48 | +0.67 | PASS | 24.556 |
| `short_rsi_ob_h1` | short | 1 | leftover | list | none | 48% | 35% | 0/26 | -2.48 | 13 | 46 | +4.44 | -5.33 | -10.13 | -11.11 | PASS | 24.534 |
| `union_candle_h1` | long | 1 | leftover | list | none | 45% | 27% | 0/26 | -5.15 | 7 | 41 | +4.90 | -4.19 | -4.78 | -0.43 | PASS | 24.463 |
| `combo_ef_5050_shared` | mix | 5 | leftover | list | none | 43% | 54% | 1/26 | -3.06 | 0 | 0 | +8.23 | -7.73 | +8.07 | — | PASS | 24.458 |
| `combo_fe_5050_shared` | mix | 5 | leftover | list | none | 43% | 54% | 1/26 | -3.06 | 0 | 0 | +8.23 | -7.73 | +8.07 | — | PASS | 24.458 |
| `flatten_h3_sboost` | long | 3 | leftover | list | both | 43% | 42% | 0/26 | -8.79 | 12 | 70 | +7.81 | -5.30 | -5.63 | -2.63 | PASS | 24.352 |
| `flatten_h3` | long | 3 | leftover | list | none | 43% | 42% | 0/26 | -8.79 | 12 | 70 | +8.12 | -5.53 | -5.58 | -2.63 | PASS | 24.33 |
| `flatten_h3_cut` | long | 3 | leftover | cut_loser | none | 43% | 42% | 0/26 | -8.79 | 12 | 70 | +8.12 | -5.53 | -5.58 | -2.63 | PASS | 24.33 |
| `flatten_h3_sizeup` | long | 3 | leftover | list | sizeup | 43% | 42% | 0/26 | -8.79 | 12 | 70 | +8.12 | -5.53 | -5.58 | -2.63 | PASS | 24.33 |
| `flatten_h3_trail` | long | 3 | leftover | trail | none | 43% | 42% | 0/26 | -8.79 | 12 | 70 | +8.12 | -5.53 | -5.58 | -2.63 | PASS | 24.33 |
| `union_e_green_h1` | long | 1 | leftover | list | none | 48% | 31% | 5/26 | -3.13 | 4 | 18 | +4.19 | -9.48 | -22.39 | -16.47 | PASS | 24.273 |
| `union_join_vol_green_h3` | long | 3 | leftover | list | none | 46% | 31% | 6/26 | -3.10 | 18 | 65 | +7.42 | -7.78 | -9.90 | +15.78 | PASS | 24.186 |
| `union_h1_half` | long | 1 | half | list | none | 42% | 31% | 0/26 | -3.39 | 8 | 41 | +3.83 | -3.90 | -2.31 | +7.77 | PASS | 24.149 |
| `union_cond_h1` | long | 1 | leftover | list | none | 45% | 27% | 0/26 | -4.41 | 5 | 39 | +3.98 | -3.91 | -7.12 | -0.82 | PASS | 24.03 |
| `union_h3_half` | long | 3 | half | list | none | 43% | 42% | 0/26 | -8.19 | 17 | 98 | +8.37 | -5.36 | -7.33 | +6.09 | PASS | 23.935 |
| `union_h3_rankw` | long | 3 | rank_w | list | none | 43% | 46% | 0/26 | -10.12 | 17 | 98 | +8.61 | -5.96 | -8.93 | +6.09 | PASS | 23.878 |
| `flatten_h3_time` | long | 3 | leftover | time | none | 43% | 42% | 0/26 | -8.08 | 12 | 70 | +7.37 | -5.73 | -2.54 | -2.63 | PASS | 23.875 |
| `combo_eer_5050_shared` | mix | 5 | leftover | list | none | 38% | 35% | 7/26 | -0.64 | 0 | 0 | +8.48 | -8.87 | +12.46 | — | PASS | 23.868 |
| `union_news_or_net4_h1` | long | 1 | leftover | list | none | 45% | 31% | 0/26 | -11.61 | 1 | 16 | +3.96 | -4.30 | -8.06 | -1.79 | PASS | 23.779 |
| `union_news_present_h1` | long | 1 | leftover | list | none | 44% | 27% | 0/26 | -3.58 | 5 | 41 | +4.16 | -4.18 | -1.19 | +5.23 | PASS | 23.6 |
| `union_h5_time` | long | 5 | leftover | time | none | 44% | 62% | 0/26 | -5.86 | 29 | 137 | +8.95 | -8.41 | -0.36 | +14.55 | PASS | 23.515 |
| `short_extended_h3` | short | 3 | leftover | list | none | 55% | 46% | 0/26 | -2.76 | 37 | 113 | +8.53 | -9.59 | -9.09 | -31.76 | PASS | 23.476 |
| `union_join_present_h3` | long | 3 | leftover | list | none | 41% | 42% | 0/26 | -7.60 | 15 | 100 | +9.71 | -5.88 | -4.53 | -1.31 | PASS | 23.436 |
| `short_extended_h1` | short | 1 | leftover | list | none | 52% | 27% | 0/26 | -3.28 | 20 | 56 | +5.66 | -6.78 | -11.23 | -7.11 | PASS | 23.346 |
| `union_h5` | long | 5 | leftover | list | none | 44% | 62% | 0/26 | -5.86 | 29 | 137 | +8.85 | -8.41 | -1.08 | +14.55 | PASS | 23.341 |
| `union_h5_cut` | long | 5 | leftover | cut_loser | none | 44% | 62% | 0/26 | -5.86 | 29 | 137 | +8.85 | -8.41 | -1.08 | +14.55 | PASS | 23.341 |
| `union_h5_sizeup` | long | 5 | leftover | list | sizeup | 44% | 62% | 0/26 | -5.86 | 29 | 137 | +8.85 | -8.41 | -1.08 | +14.55 | PASS | 23.341 |
| `union_h5_trail` | long | 5 | leftover | trail | none | 44% | 62% | 0/26 | -5.86 | 29 | 137 | +8.85 | -8.41 | -1.08 | +14.55 | PASS | 23.341 |
| `union_h5_topheavy` | long | 5 | topheavy | list | none | 44% | 58% | 1/26 | -6.39 | 29 | 137 | +8.63 | -8.59 | -1.24 | +14.55 | PASS | 23.275 |
| `flatten_h3_half` | long | 3 | half | list | none | 43% | 38% | 0/26 | -5.91 | 12 | 70 | +7.14 | -5.12 | -5.03 | -2.63 | PASS | 23.263 |
| `union_h3_exit_news_r` | long | 3 | leftover | list | none | 41% | 42% | 0/26 | -8.36 | 15 | 100 | +9.71 | -5.94 | -5.67 | -3.83 | PASS | 23.185 |
| `flatten_h5_topheavy` | long | 5 | topheavy | list | none | 42% | 54% | 1/26 | -4.38 | 20 | 101 | +8.62 | -8.14 | +1.16 | -1.38 | PASS | 23.1 |
| `union_rsi_os_h1` | long | 1 | leftover | list | none | 54% | 27% | 4/26 | -4.12 | 2 | 18 | +2.52 | -5.31 | -10.59 | +4.98 | PASS | 23.1 |
| `flatten_h3_rankw` | long | 3 | rank_w | list | none | 43% | 38% | 0/26 | -7.25 | 12 | 70 | +7.94 | -5.70 | -7.25 | -2.63 | PASS | 22.928 |
| `union_h5_sboost` | long | 5 | leftover | list | both | 44% | 62% | 0/26 | -7.45 | 29 | 137 | +8.39 | -8.87 | -1.77 | +14.55 | PASS | 22.707 |
| `union_last_red_h1` | long | 1 | leftover | list | none | 48% | 31% | 0/26 | -5.66 | 8 | 60 | +3.37 | -4.59 | -5.31 | +7.70 | PASS | 22.36 |
| `union_h3_topheavy` | long | 3 | topheavy | list | none | 43% | 42% | 0/26 | -6.73 | 17 | 98 | +7.05 | -6.12 | -4.26 | +6.09 | PASS | 22.345 |
| `union_ab_g_h1` | long | 1 | leftover | list | none | 39% | 27% | 0/26 | -6.45 | 7 | 36 | +3.76 | -3.37 | -3.39 | -4.92 | PASS | 22.255 |
| `union_h3_exit_alarm` | long | 3 | leftover | list | none | 41% | 46% | 0/26 | -7.37 | 15 | 100 | +6.97 | -5.83 | -4.13 | -0.72 | PASS | 22.184 |
| `flatten_h5_half` | long | 5 | half | list | none | 42% | 54% | 0/26 | -5.82 | 20 | 101 | +7.81 | -7.00 | -1.30 | -1.38 | PASS | 22.049 |
| `ohlc_hot_h1` | long | 1 | leftover | list | none | 44% | 35% | 0/26 | -4.02 | 15 | 46 | +3.21 | -5.03 | -10.52 | -2.44 | PASS | 22.02 |
| `flatten_h5_rankw` | long | 5 | rank_w | list | none | 42% | 50% | 1/26 | -5.74 | 20 | 101 | +8.25 | -8.53 | +0.10 | -1.38 | PASS | 21.706 |
| `union_h5_half` | long | 5 | half | list | none | 44% | 54% | 0/26 | -6.22 | 29 | 137 | +7.93 | -7.63 | -1.48 | +14.55 | PASS | 21.674 |
| `flatten_h3_topheavy` | long | 3 | topheavy | list | none | 43% | 38% | 0/26 | -6.64 | 12 | 70 | +6.20 | -5.72 | -5.38 | -2.63 | PASS | 21.67 |
| `union_last_green_h3` | long | 3 | leftover | list | none | 51% | 35% | 0/26 | -10.15 | 22 | 96 | +5.87 | -7.94 | -6.95 | +20.61 | PASS | 21.613 |
| `union_candle_score_h3` | long | 3 | leftover | list | none | 43% | 42% | 1/26 | -2.87 | 23 | 121 | +6.78 | -6.46 | +1.25 | +10.17 | PASS | 21.554 |
| `union_vol_g_h5` | long | 5 | leftover | list | none | 44% | 38% | 6/26 | -4.29 | 32 | 122 | +12.33 | -10.84 | -14.59 | +2.22 | PASS | 21.464 |
| `union_flow_in_h3` | long | 3 | leftover | list | none | 46% | 31% | 4/26 | -0.21 | 5 | 27 | +1.95 | -4.94 | -2.79 | -7.30 | PASS | 21.46 |
| `combo_fer_5050_shared` | mix | 5 | leftover | list | none | 41% | 50% | 1/26 | -4.30 | 0 | 0 | +8.17 | -8.52 | +5.58 | — | PASS | 21.348 |
| `union_news_vol_h3` | long | 3 | leftover | list | none | 51% | 42% | 0/26 | -5.99 | 9 | 47 | +4.25 | -5.30 | -7.46 | +203.03 | PASS | 21.283 |
| `union_blue_coil_h3` | long | 3 | leftover | list | none | 44% | 42% | 0/26 | -5.59 | 12 | 73 | +6.07 | -5.91 | -9.76 | -18.53 | PASS | 21.226 |
| `union_macd_hist_h1` | long | 1 | leftover | list | none | 43% | 31% | 0/26 | -6.11 | 10 | 64 | +3.59 | -3.76 | -9.53 | -8.91 | PASS | 21.16 |
| `yday_gainer_h3` | long | 3 | leftover | list | none | 50% | 35% | 0/26 | -3.88 | 23 | 106 | +8.56 | -7.14 | -8.22 | +30.99 | PASS | 21.114 |
| `union_h5_exit_alarm` | long | 5 | leftover | list | none | 44% | 54% | 0/26 | -6.32 | 26 | 138 | +7.34 | -7.68 | -1.02 | +12.27 | PASS | 21.003 |
| `union_h5_rankw` | long | 5 | rank_w | list | none | 44% | 58% | 0/26 | -9.25 | 29 | 137 | +7.50 | -8.84 | -4.87 | +14.55 | PASS | 20.988 |
| `union_macd_xup_h1` | long | 1 | leftover | list | none | 37% | 27% | 5/26 | -0.44 | 4 | 27 | +2.87 | -4.72 | -12.77 | -16.55 | PASS | 20.645 |
| `union_news_g_cam71_h1` | long | 1 | leftover | list | none | 37% | 27% | 0/26 | -10.46 | 1 | 3 | +2.93 | -4.12 | -10.46 | -4.11 | PASS | 20.524 |
| `flatten_live_h3` | long | 3 | leftover | list | none | 58% | 12% | 0/26 | -6.24 | 4 | 14 | +8.60 | -8.85 | -6.24 | +6.87 | PASS | 20.399 |
| `union_blue_coil_h1` | long | 1 | leftover | list | none | 43% | 19% | 0/26 | -5.17 | 5 | 30 | +3.37 | -4.28 | -7.75 | -8.45 | PASS | 20.359 |
| `union_h3_exit_red` | long | 3 | leftover | list | none | 49% | 31% | 0/26 | -10.20 | 22 | 96 | +5.84 | -7.30 | -6.55 | +13.40 | PASS | 20.347 |
| `union_last_green_h5` | long | 5 | leftover | list | none | 47% | 50% | 1/26 | -8.68 | 31 | 138 | +7.88 | -10.91 | -5.30 | +38.53 | PASS | 20.328 |
| `union_candle_h3` | long | 3 | leftover | list | none | 46% | 27% | 3/26 | -5.58 | 20 | 105 | +6.90 | -7.35 | -8.20 | +2.67 | PASS | 20.133 |
| `union_news_missing_h1` | long | 1 | leftover | list | none | 30% | 15% | 0/26 | -2.90 | 1 | 5 | +2.97 | -1.85 | -1.39 | -1.85 | PASS | 20.039 |
| `union_white_coil_h1` | long | 1 | leftover | list | none | 38% | 23% | 0/26 | -4.98 | 4 | 20 | +4.12 | -3.54 | -8.54 | -6.88 | PASS | 19.939 |
| `union_last_red_h3` | long | 3 | leftover | list | none | 40% | 46% | 0/26 | -4.46 | 14 | 109 | +6.50 | -6.56 | -2.75 | -3.49 | PASS | 19.311 |
| `union_white_h5` | long | 5 | leftover | list | none | 33% | 62% | 1/26 | -10.72 | 16 | 66 | +9.42 | -9.14 | -0.25 | +9.72 | PASS | 19.306 |
| `union_blue_h3` | long | 3 | leftover | list | none | 43% | 35% | 0/26 | -8.39 | 14 | 79 | +6.85 | -6.31 | -12.56 | -9.56 | PASS | 19.158 |
| `union_news_present_h3` | long | 3 | leftover | list | none | 41% | 38% | 0/26 | -6.29 | 13 | 90 | +6.76 | -5.91 | -10.34 | -2.06 | PASS | 18.962 |
| `union_coil_off_h1` | long | 1 | leftover | list | none | 41% | 19% | 0/26 | -6.54 | 6 | 45 | +3.46 | -3.86 | -9.99 | -11.87 | PASS | 18.656 |
| `union_blue_vol_h3` | long | 3 | leftover | list | none | 41% | 35% | 1/26 | -2.72 | 14 | 75 | +9.92 | -7.51 | -8.48 | +14.87 | PASS | 18.65 |
| `union_join_g_h3` | long | 3 | leftover | list | none | 40% | 31% | 0/26 | -9.63 | 17 | 98 | +7.45 | -5.87 | -8.06 | -9.56 | PASS | 18.62 |
| `union_macd_up_h3` | long | 3 | leftover | list | none | 40% | 38% | 0/26 | -7.00 | 20 | 107 | +7.63 | -6.30 | -7.73 | +6.04 | PASS | 18.558 |
| `union_cond_n4_h3` | long | 3 | leftover | list | none | 39% | 42% | 0/26 | -10.06 | 8 | 53 | +5.85 | -6.84 | -6.56 | -12.45 | PASS | 18.488 |
| `probable_probable_ok_h3` | long | 3 | leftover | list | none | 53% | 27% | 0/26 | -3.85 | 15 | 50 | +5.60 | -8.07 | -20.49 | +5.97 | PASS | 17.835 |
| `yday_gainer_h5` | long | 5 | leftover | list | none | 48% | 38% | 0/26 | -6.09 | 33 | 132 | +8.56 | -9.87 | -6.09 | +14.92 | PASS | 17.63 |
| `probable_h3` | long | 3 | leftover | list | none | 49% | 31% | 0/26 | -3.92 | 21 | 110 | +6.61 | -8.48 | -8.50 | +12.58 | PASS | 17.076 |
| `union_news_g_h3` | long | 3 | leftover | list | none | 50% | 23% | 0/26 | -8.30 | 16 | 73 | +4.53 | -6.64 | -16.44 | +189.62 | PASS | 16.846 |
| `union_cond_h3` | long | 3 | leftover | list | none | 37% | 46% | 0/26 | -10.14 | 15 | 110 | +6.38 | -7.63 | -9.10 | -23.07 | PASS | 16.818 |
| `ohlc_hot_h3` | long | 3 | leftover | list | none | 44% | 38% | 0/26 | -4.95 | 30 | 124 | +6.00 | -7.25 | -4.95 | +78.11 | PASS | 16.627 |
| `flatten_live_h5` | long | 5 | leftover | list | none | 46% | 27% | 0/26 | -5.51 | 5 | 16 | +9.01 | -11.62 | -5.51 | +5.13 | PASS | 16.556 |
| `union_ab_g_h3` | long | 3 | leftover | list | none | 36% | 27% | 0/26 | -11.54 | 13 | 83 | +7.68 | -5.70 | -12.21 | -22.28 | PASS | 15.453 |
| `union_white_coil_h3` | long | 3 | leftover | list | none | 38% | 46% | 0/26 | -17.32 | 9 | 45 | +5.20 | -7.71 | -16.71 | -26.99 | PASS | 15.297 |
| `union_news_head_h3` | long | 3 | leftover | list | none | 49% | 15% | 0/26 | -9.66 | 15 | 63 | +5.36 | -6.00 | -19.40 | +192.75 | PASS | 14.771 |
| `combo_e1er_5050_shared` | mix | 5 | leftover | list | none | 44% | 19% | 0/26 | -3.96 | 0 | 0 | +4.69 | -8.07 | -6.47 | — | PASS | 14.549 |
| `union_rsi_h1` | long | 1 | leftover | list | none | 41% | 15% | 0/26 | -9.11 | 10 | 70 | +3.61 | -5.49 | -16.44 | -9.52 | PASS | 14.146 |
| `ohlc_hot_h5` | long | 5 | leftover | list | none | 42% | 42% | 0/26 | -5.99 | 41 | 149 | +8.16 | -9.81 | -10.90 | +61.07 | PASS | 13.814 |
| `flatten_vol_g_h3` | long | 3 | leftover | list | none | 44% | 19% | 0/26 | -17.31 | 6 | 22 | +9.63 | -9.84 | -25.48 | -23.82 | PASS | 13.381 |
| `union_white_h3` | long | 3 | leftover | list | none | 34% | 35% | 1/26 | -17.78 | 12 | 56 | +7.40 | -8.14 | -16.42 | -10.44 | PASS | 12.955 |
| `union_news_g_h5` | long | 5 | leftover | list | none | 39% | 38% | 0/26 | -6.90 | 18 | 106 | +5.46 | -8.21 | -10.92 | +110.47 | PASS | 11.882 |
| `union_coil_off_h3` | long | 3 | leftover | list | none | 38% | 23% | 0/26 | -10.67 | 17 | 101 | +4.47 | -8.29 | -13.93 | -16.19 | PASS | 10.456 |
| `probable_h5` | long | 5 | leftover | list | none | 48% | 23% | 0/26 | -6.80 | 30 | 136 | +6.75 | -10.63 | -19.34 | -6.97 | PASS | 10.428 |
| `union_coil_green_h3` | long | 3 | leftover | list | none | 40% | 19% | 0/26 | -12.92 | 23 | 98 | +3.87 | -7.71 | -14.55 | -11.98 | PASS | 10.177 |
| `coil_h3_exit_alarm` | long | 3 | leftover | list | none | 37% | 23% | 0/26 | -13.48 | 14 | 106 | +4.34 | -7.55 | -13.03 | -16.66 | PASS | 9.837 |
| `union_coil_off_h5` | long | 5 | leftover | list | none | 42% | 31% | 0/26 | -14.70 | 24 | 133 | +3.90 | -57.28 | -10.70 | -24.95 | PASS | 8.429 |
| `union_news_both_h1` *(thin)* | long | 1 | leftover | list | none | 100% | 12% | 21/26 | +7.89 | 0 | 0 | +3.69 | — | +7.92 | +4.91 | PASS | 58.689 |
| `union_flow_in_white_h1` *(thin)* | long | 1 | leftover | list | none | 60% | 15% | 17/26 | +0.05 | 0 | 2 | +2.59 | -2.35 | +0.64 | +4.31 | PASS | 35.016 |
| `short_news_pack_h3` *(thin)* | short | 3 | leftover | list | none | 54% | 35% | 12/26 | +0.00 | 2 | 5 | +3.31 | -2.87 | +5.51 | +5.19 | PASS | 21.203 |
| `union_catal_present_h1` *(thin)* | long | 1 | leftover | list | none | 40% | 4% | 0/26 | -2.79 | 0 | 0 | +7.67 | -1.88 | -2.79 | +6.22 | PASS | 11.351 |
| `union_vol_missing_h1` *(thin)* | long | 1 | leftover | list | none | 50% | 4% | 1/26 | +0.00 | 0 | 2 | +7.93 | -3.05 | +1.44 | +1.88 | PASS | 9.958 |
| `union_vol_missing_h3` *(thin)* | long | 3 | leftover | list | none | 50% | 12% | 1/26 | +0.00 | 1 | 5 | +11.98 | -3.98 | +3.58 | +5.59 | PASS | 8.183 |
| `union_news_g_cam91_n1_h1` *(thin)* | long | 1 | leftover | list | none | 50% | 4% | 0/26 | -2.85 | 0 | 0 | +0.85 | -3.84 | -2.85 | -0.09 | PASS | 1.447 |
| `union_catal_present_h3` *(thin)* | long | 3 | leftover | list | none | 20% | 8% | 0/26 | -9.42 | 1 | 2 | +8.08 | -4.28 | -9.42 | -5.67 | PASS | -7.425 |
| `union_rsi_os_macd_h1` *(thin)* | long | 1 | leftover | list | none | 33% | 4% | 1/26 | -5.91 | 1 | 2 | +7.49 | -6.33 | -5.91 | +0.01 | PASS | -8.242 |
| `short_r_down_h1` *(thin)* | short | 1 | leftover | list | none | — | 0% | 0/26 | +0.00 | 0 | 0 | — | — | +0.00 | +0.00 | PASS | -15.0 |
| `short_r_down_h3` *(thin)* | short | 3 | leftover | list | none | — | 0% | 0/26 | +0.00 | 0 | 0 | — | — | +0.00 | +0.00 | PASS | -15.0 |
| `union_r_up_h1` *(thin)* | long | 1 | leftover | list | none | — | 0% | 0/26 | +0.00 | 0 | 0 | — | — | +0.00 | +0.00 | PASS | -15.0 |
| `union_r_up_h3` *(thin)* | long | 3 | leftover | list | none | — | 0% | 0/26 | +0.00 | 0 | 0 | — | — | +0.00 | +0.00 | PASS | -15.0 |
