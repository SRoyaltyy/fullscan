# Factor strategy mine — 2026-08-13 → 2026-09-08

Leak-free 09:30 recipes: **161** · candidate rows **1410** · fill `09:30 open, whole shares, Futubull fees, leftover split, sell first, hard-red sit, cash+holdings audit`.

Cash book: $10k, whole shares, Futubull fees, leftover split, sell first, min-hold, 09:30 open, hard-red S≤−3 sit, shorts marked as a liability. Each session starts from leftover cash and lots actually held (butterfly). Cash-start buttons wake a sleeve on date X with $10k and no lots (same rules). Stock investigator quotes 09:30 cameras / coaches / news from repo files. Size / sell / S-boost tweaks sit on the same ledger. Signal-only % is the old equal-weight path (not a fill). `flatten_h*` = wish-list (io/HOLD mornings still buy). `flatten_live_*` = only when the live flatten gate fires. Research only — does not change live `flatten_robust`.

Action blotters: [FACTOR_MINE_ACTION.md](FACTOR_MINE_ACTION.md).

| Strategy | Side | H | Size | Sell | Boost | Win% | $ days | Starts YES | Med start | Top-g | Losers | AvgW | AvgL | Book% | Signal% | Audit | Eff |
|---|---|---:|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---:|
| `flatten_h1` | long | 1 | leftover | list | none | 58% | 56% | 18/18 | +5.97 | 2 | 14 | +4.74 | -3.51 | +16.17 | +23.20 | PASS | 76.375 |
| `flatten_h3_time` | long | 3 | leftover | time | none | 64% | 61% | 17/18 | +4.74 | 6 | 45 | +7.40 | -3.81 | +11.83 | +45.15 | PASS | 76.121 |
| `flatten_h3_half` | long | 3 | half | list | none | 64% | 72% | 17/18 | +2.49 | 6 | 45 | +6.32 | -3.87 | +7.21 | +45.15 | PASS | 76.085 |
| `flatten_h3` | long | 3 | leftover | list | none | 64% | 56% | 17/18 | +4.74 | 6 | 45 | +7.40 | -4.04 | +12.04 | +45.15 | PASS | 74.473 |
| `flatten_h3_cut` | long | 3 | leftover | cut_loser | none | 64% | 56% | 17/18 | +4.74 | 6 | 45 | +7.40 | -4.04 | +12.04 | +45.15 | PASS | 74.473 |
| `flatten_h3_sizeup` | long | 3 | leftover | list | sizeup | 64% | 56% | 17/18 | +4.74 | 6 | 45 | +7.40 | -4.04 | +12.04 | +45.15 | PASS | 74.473 |
| `flatten_h3_trail` | long | 3 | leftover | trail | none | 64% | 56% | 17/18 | +4.74 | 6 | 45 | +7.40 | -4.04 | +12.04 | +45.15 | PASS | 74.473 |
| `flatten_h5` | long | 5 | leftover | list | none | 59% | 78% | 17/18 | +6.46 | 13 | 69 | +10.07 | -6.04 | +23.15 | +67.10 | PASS | 74.388 |
| `flatten_h5_cut` | long | 5 | leftover | cut_loser | none | 59% | 78% | 17/18 | +6.46 | 13 | 69 | +10.07 | -6.04 | +23.15 | +67.10 | PASS | 74.388 |
| `flatten_h5_sizeup` | long | 5 | leftover | list | sizeup | 59% | 78% | 17/18 | +6.46 | 13 | 69 | +10.07 | -6.04 | +23.15 | +67.10 | PASS | 74.388 |
| `flatten_h5_time` | long | 5 | leftover | time | none | 59% | 78% | 17/18 | +6.46 | 13 | 69 | +10.07 | -6.04 | +23.15 | +67.10 | PASS | 74.388 |
| `flatten_h5_trail` | long | 5 | leftover | trail | none | 59% | 78% | 17/18 | +6.46 | 13 | 69 | +10.07 | -6.04 | +23.15 | +67.10 | PASS | 74.388 |
| `flatten_h3_sboost` | long | 3 | leftover | list | both | 64% | 56% | 17/18 | +4.74 | 6 | 45 | +7.17 | -4.01 | +11.97 | +45.15 | PASS | 74.252 |
| `flatten_h5_sboost` | long | 5 | leftover | list | both | 59% | 78% | 17/18 | +6.46 | 13 | 69 | +9.68 | -6.06 | +22.48 | +67.10 | PASS | 73.947 |
| `union_h3_half` | long | 3 | half | list | none | 60% | 78% | 17/18 | +3.08 | 11 | 61 | +6.41 | -4.72 | +6.86 | +42.06 | PASS | 73.718 |
| `union_h3_time` | long | 3 | leftover | time | none | 60% | 67% | 17/18 | +6.75 | 11 | 61 | +7.07 | -4.63 | +14.47 | +42.06 | PASS | 73.477 |
| `union_h3` | long | 3 | leftover | list | none | 60% | 67% | 17/18 | +6.75 | 11 | 61 | +7.07 | -4.87 | +14.41 | +42.06 | PASS | 73.104 |
| `union_h3_cut` | long | 3 | leftover | cut_loser | none | 60% | 67% | 17/18 | +6.75 | 11 | 61 | +7.07 | -4.87 | +14.41 | +42.06 | PASS | 73.104 |
| `union_h3_sizeup` | long | 3 | leftover | list | sizeup | 60% | 67% | 17/18 | +6.75 | 11 | 61 | +7.07 | -4.87 | +14.41 | +42.06 | PASS | 73.104 |
| `union_h3_trail` | long | 3 | leftover | trail | none | 60% | 67% | 17/18 | +6.75 | 11 | 61 | +7.07 | -4.87 | +14.41 | +42.06 | PASS | 73.104 |
| `union_h3_sboost` | long | 3 | leftover | list | both | 60% | 67% | 17/18 | +6.75 | 11 | 61 | +6.93 | -5.04 | +14.35 | +42.06 | PASS | 72.715 |
| `union_h1_rankw` | long | 1 | rank_w | list | none | 54% | 56% | 17/18 | +6.67 | 6 | 22 | +4.71 | -3.72 | +15.11 | +19.07 | PASS | 72.37 |
| `flatten_h5_half` | long | 5 | half | list | none | 59% | 72% | 17/18 | +3.34 | 13 | 69 | +8.90 | -4.97 | +12.63 | +67.10 | PASS | 72.322 |
| `flatten_h3_rankw` | long | 3 | rank_w | list | none | 64% | 44% | 18/18 | +4.49 | 6 | 45 | +7.25 | -4.36 | +7.51 | +45.15 | PASS | 72.113 |
| `union_h1_topheavy` | long | 1 | topheavy | list | none | 54% | 50% | 17/18 | +5.31 | 6 | 22 | +4.79 | -3.53 | +16.32 | +19.07 | PASS | 71.89 |
| `flatten_h5_rankw` | long | 5 | rank_w | list | none | 59% | 67% | 18/18 | +7.22 | 13 | 69 | +9.29 | -6.18 | +17.23 | +67.10 | PASS | 71.852 |
| `union_h1` | long | 1 | leftover | list | none | 54% | 50% | 17/18 | +5.05 | 6 | 22 | +4.72 | -3.53 | +15.12 | +19.07 | PASS | 71.61 |
| `union_h1_cut` | long | 1 | leftover | cut_loser | none | 54% | 50% | 17/18 | +5.05 | 6 | 22 | +4.72 | -3.53 | +15.12 | +19.07 | PASS | 71.61 |
| `union_h1_sizeup` | long | 1 | leftover | list | sizeup | 54% | 50% | 17/18 | +5.05 | 6 | 22 | +4.72 | -3.53 | +15.12 | +19.07 | PASS | 71.61 |
| `union_h1_trail` | long | 1 | leftover | trail | none | 54% | 50% | 17/18 | +5.05 | 6 | 22 | +4.72 | -3.53 | +15.12 | +19.07 | PASS | 71.61 |
| `union_h1_sboost` | long | 1 | leftover | list | both | 54% | 50% | 17/18 | +5.05 | 6 | 22 | +4.66 | -3.68 | +15.18 | +19.07 | PASS | 71.259 |
| `union_h1_time` | long | 1 | leftover | time | none | 54% | 50% | 17/18 | +5.38 | 6 | 22 | +4.96 | -3.93 | +15.19 | +19.07 | PASS | 71.235 |
| `union_h5_time` | long | 5 | leftover | time | none | 57% | 78% | 16/18 | +6.75 | 18 | 91 | +9.34 | -6.87 | +22.96 | +70.08 | PASS | 70.061 |
| `union_h5` | long | 5 | leftover | list | none | 57% | 78% | 16/18 | +6.75 | 18 | 91 | +9.28 | -6.87 | +22.85 | +70.08 | PASS | 70.004 |
| `union_h5_cut` | long | 5 | leftover | cut_loser | none | 57% | 78% | 16/18 | +6.75 | 18 | 91 | +9.28 | -6.87 | +22.85 | +70.08 | PASS | 70.004 |
| `union_h5_sizeup` | long | 5 | leftover | list | sizeup | 57% | 78% | 16/18 | +6.75 | 18 | 91 | +9.28 | -6.87 | +22.85 | +70.08 | PASS | 70.004 |
| `union_h5_trail` | long | 5 | leftover | trail | none | 57% | 78% | 16/18 | +6.75 | 18 | 91 | +9.28 | -6.87 | +22.85 | +70.08 | PASS | 70.004 |
| `flatten_h5_topheavy` | long | 5 | topheavy | list | none | 59% | 61% | 17/18 | +5.89 | 13 | 69 | +9.61 | -6.12 | +17.94 | +67.10 | PASS | 69.787 |
| `flatten_h3_topheavy` | long | 3 | topheavy | list | none | 64% | 50% | 16/18 | +4.71 | 6 | 45 | +5.73 | -4.18 | +9.18 | +45.15 | PASS | 69.248 |
| `union_h1_half` | long | 1 | half | list | none | 54% | 44% | 17/18 | +2.25 | 6 | 22 | +4.72 | -3.55 | +5.70 | +19.07 | PASS | 69.04 |
| `union_h5_sboost` | long | 5 | leftover | list | both | 57% | 78% | 16/18 | +6.75 | 18 | 91 | +8.68 | -7.78 | +22.19 | +70.08 | PASS | 68.736 |
| `union_h5_half` | long | 5 | half | list | none | 57% | 72% | 17/18 | +3.08 | 18 | 91 | +8.37 | -6.02 | +10.56 | +70.08 | PASS | 68.642 |
| `union_h3_exit_news_r` | long | 3 | leftover | list | none | 57% | 61% | 16/18 | +6.74 | 12 | 64 | +7.37 | -5.38 | +14.39 | +35.77 | PASS | 68.631 |
| `union_join_present_h3` | long | 3 | leftover | list | none | 57% | 61% | 16/18 | +6.74 | 12 | 64 | +7.37 | -5.38 | +14.39 | +35.77 | PASS | 68.631 |
| `union_h5_rankw` | long | 5 | rank_w | list | none | 57% | 72% | 17/18 | +6.33 | 18 | 91 | +8.94 | -7.09 | +14.37 | +70.08 | PASS | 68.559 |
| `union_h3_rankw` | long | 3 | rank_w | list | none | 60% | 50% | 17/18 | +5.98 | 11 | 61 | +6.92 | -5.14 | +8.53 | +42.06 | PASS | 68.358 |
| `union_h3_topheavy` | long | 3 | topheavy | list | none | 60% | 50% | 17/18 | +5.68 | 11 | 61 | +6.03 | -5.06 | +12.99 | +42.06 | PASS | 68.262 |
| `union_join_present_h1` | long | 1 | leftover | list | none | 51% | 44% | 16/18 | +3.12 | 6 | 24 | +4.72 | -3.73 | +12.30 | +16.74 | PASS | 67.175 |
| `union_last_green_h1` | long | 1 | leftover | list | none | 51% | 39% | 16/18 | +4.67 | 6 | 26 | +5.32 | -3.48 | +13.07 | +14.95 | PASS | 67.009 |
| `union_h5_topheavy` | long | 5 | topheavy | list | none | 57% | 67% | 16/18 | +5.68 | 18 | 91 | +8.93 | -6.96 | +17.54 | +70.08 | PASS | 66.652 |
| `union_h3_exit_alarm` | long | 3 | leftover | list | none | 57% | 56% | 16/18 | +3.79 | 12 | 64 | +6.44 | -5.21 | +9.77 | +27.07 | PASS | 66.153 |
| `union_candle_score_h1` | long | 1 | leftover | list | none | 45% | 50% | 17/18 | +4.92 | 7 | 37 | +5.48 | -4.13 | +9.82 | +3.49 | PASS | 65.254 |
| `union_news_present_h3` | long | 3 | leftover | list | none | 58% | 56% | 16/18 | +6.83 | 11 | 58 | +5.59 | -5.42 | +7.05 | +28.19 | PASS | 65.179 |
| `union_candle_score_h3` | long | 3 | leftover | list | none | 49% | 56% | 17/18 | +4.13 | 16 | 81 | +8.61 | -5.53 | +12.65 | +24.39 | PASS | 64.504 |
| `union_last_green_h5` | long | 5 | leftover | list | none | 54% | 72% | 15/18 | +5.05 | 20 | 97 | +9.48 | -8.41 | +22.60 | +68.43 | PASS | 64.111 |
| `union_hot_n4_h1` | long | 1 | leftover | list | none | 51% | 44% | 15/18 | +1.22 | 3 | 23 | +7.10 | -4.51 | +10.55 | +4.12 | PASS | 63.993 |
| `union_news_present_h1` | long | 1 | leftover | list | none | 50% | 33% | 16/18 | +3.12 | 6 | 23 | +4.45 | -3.70 | +11.68 | +14.11 | PASS | 63.891 |
| `union_w_hot_candle_h3` | long | 3 | leftover | list | none | 51% | 61% | 17/18 | +2.00 | 17 | 84 | +7.47 | -6.93 | +11.58 | +25.75 | PASS | 63.377 |
| `union_news_vol_h1` | long | 1 | leftover | list | none | 47% | 39% | 18/18 | +3.62 | 2 | 12 | +4.00 | -5.65 | +5.80 | +6.23 | PASS | 62.183 |
| `union_join_g_h1` | long | 1 | leftover | list | none | 53% | 44% | 13/18 | +4.16 | 5 | 23 | +4.35 | -4.21 | +9.49 | +14.92 | PASS | 62.148 |
| `union_h5_exit_alarm` | long | 5 | leftover | list | none | 55% | 61% | 15/18 | +3.79 | 19 | 92 | +7.82 | -7.35 | +14.67 | +64.33 | PASS | 61.787 |
| `union_break10_h1` | long | 1 | leftover | list | none | 49% | 39% | 16/18 | +4.15 | 5 | 30 | +5.67 | -4.97 | +1.05 | -1.26 | PASS | 61.496 |
| `union_e_fresh_h3` | long | 3 | leftover | list | none | 50% | 61% | 16/18 | +6.89 | 11 | 62 | +8.33 | -13.59 | +29.24 | -16.62 | PASS | 60.337 |
| `union_white_coil_h1` | long | 1 | leftover | list | none | 57% | 39% | 12/18 | +2.02 | 3 | 13 | +4.58 | -4.11 | +2.02 | +9.14 | PASS | 60.091 |
| `union_last_red_h3` | long | 3 | leftover | list | none | 51% | 61% | 14/18 | +1.22 | 11 | 65 | +6.33 | -6.73 | +4.24 | +25.82 | PASS | 59.103 |
| `union_h3_exit_red` | long | 3 | leftover | list | none | 55% | 50% | 14/18 | +3.73 | 15 | 72 | +6.43 | -6.80 | +7.31 | +38.86 | PASS | 58.736 |
| `union_last_green_h3` | long | 3 | leftover | list | none | 55% | 50% | 14/18 | +3.73 | 15 | 72 | +6.43 | -6.80 | +7.31 | +40.35 | PASS | 58.736 |
| `union_ret_5_h3` | long | 3 | leftover | list | none | 50% | 61% | 15/18 | +1.77 | 19 | 98 | +9.33 | -8.79 | +13.32 | +21.00 | PASS | 58.729 |
| `union_ret_5_h1` | long | 1 | leftover | list | none | 43% | 56% | 14/18 | +2.17 | 8 | 50 | +6.04 | -5.29 | +5.77 | +1.23 | PASS | 58.365 |
| `union_e_green_h3` | long | 3 | leftover | list | none | 48% | 44% | 15/18 | +11.20 | 8 | 46 | +7.33 | -12.38 | +50.00 | +17.90 | PASS | 56.945 |
| `short_news_r_h3` | short | 3 | leftover | list | none | 59% | 44% | 13/18 | +2.17 | 3 | 36 | +5.38 | -4.08 | +6.05 | +9.29 | PASS | 56.275 |
| `union_break10_h3` | long | 3 | leftover | list | none | 49% | 44% | 15/18 | +1.87 | 14 | 69 | +6.66 | -6.44 | +0.50 | +6.49 | PASS | 55.618 |
| `union_ab_g_h1` | long | 1 | leftover | list | none | 46% | 33% | 11/18 | +2.60 | 4 | 16 | +4.61 | -3.96 | +9.31 | +4.73 | PASS | 54.983 |
| `short_news_r_h1` | short | 1 | leftover | list | none | 50% | 39% | 11/18 | +0.70 | 2 | 17 | +4.05 | -3.11 | -0.71 | +6.60 | PASS | 54.032 |
| `short_alarm_h1` | short | 1 | leftover | list | none | 48% | 28% | 12/18 | +0.27 | 7 | 30 | +4.51 | -2.93 | +1.75 | +1.42 | PASS | 53.655 |
| `union_vol_g_h3` | long | 3 | leftover | list | none | 45% | 39% | 16/18 | +3.73 | 16 | 72 | +9.08 | -8.21 | +1.56 | +9.27 | PASS | 53.037 |
| `union_ab_g_h3` | long | 3 | leftover | list | none | 56% | 22% | 13/18 | +0.68 | 7 | 47 | +6.05 | -5.25 | +0.68 | +9.98 | PASS | 52.61 |
| `union_join_g_h3` | long | 3 | leftover | list | none | 55% | 44% | 9/18 | +0.18 | 10 | 59 | +7.33 | -5.97 | +2.44 | +17.15 | PASS | 52.599 |
| `union_coil_off_h3` | long | 3 | leftover | list | none | 50% | 44% | 13/18 | +2.38 | 13 | 70 | +4.18 | -6.93 | +8.57 | +14.12 | PASS | 51.872 |
| `union_last_red_h1` | long | 1 | leftover | list | none | 48% | 50% | 9/18 | +0.09 | 5 | 32 | +4.04 | -4.88 | +1.64 | +8.15 | PASS | 51.872 |
| `short_alarm_h3` | short | 3 | leftover | list | none | 57% | 50% | 12/18 | +0.68 | 8 | 56 | +7.09 | -9.39 | +1.92 | +10.79 | PASS | 51.817 |
| `union_earn_react_h3` | long | 3 | leftover | list | none | 41% | 50% | 15/18 | +6.70 | 9 | 60 | +8.98 | -14.31 | +24.06 | -30.11 | PASS | 51.678 |
| `union_news_vol_h3` | long | 3 | leftover | list | none | 42% | 61% | 13/18 | +2.13 | 5 | 36 | +5.59 | -5.34 | +3.16 | -0.83 | PASS | 51.474 |
| `flatten_vol_g_h3` | long | 3 | leftover | list | none | 45% | 33% | 14/18 | +3.33 | 3 | 12 | +8.37 | -7.08 | -2.00 | -0.20 | PASS | 51.428 |
| `union_news_g_h1` | long | 1 | leftover | list | none | 48% | 44% | 9/18 | +0.12 | 2 | 20 | +3.49 | -5.22 | +1.80 | +1.37 | PASS | 51.267 |
| `union_white_coil_h3` | long | 3 | leftover | list | none | 54% | 44% | 11/18 | +2.49 | 6 | 38 | +5.16 | -7.01 | +5.81 | +7.24 | PASS | 50.628 |
| `union_coil_off_h5` | long | 5 | leftover | list | none | 49% | 50% | 14/18 | +4.77 | 18 | 92 | +4.30 | -23.59 | +14.05 | +17.22 | PASS | 49.957 |
| `union_cond_h3` | long | 3 | leftover | list | none | 48% | 56% | 9/18 | +0.39 | 7 | 72 | +7.51 | -6.04 | +1.39 | +5.59 | PASS | 49.829 |
| `union_blue_h3` | long | 3 | leftover | list | none | 54% | 44% | 9/18 | +2.02 | 11 | 56 | +6.24 | -6.88 | -1.30 | +7.09 | PASS | 49.689 |
| `probable_probable_ok_h1` | long | 1 | leftover | list | none | 45% | 33% | 10/18 | +0.21 | 5 | 19 | +4.78 | -4.33 | -1.01 | +1.27 | PASS | 49.48 |
| `union_coil_green_h3` | long | 3 | leftover | list | none | 45% | 50% | 12/18 | +1.08 | 15 | 71 | +4.91 | -6.05 | +6.01 | +0.57 | PASS | 48.967 |
| `union_white_h5` | long | 5 | leftover | list | none | 51% | 56% | 9/18 | +0.44 | 14 | 58 | +9.97 | -8.15 | +2.04 | +11.45 | PASS | 48.86 |
| `union_cond_n4_h3` | long | 3 | leftover | list | none | 48% | 56% | 9/18 | +0.77 | 3 | 34 | +4.91 | -6.24 | +2.57 | +10.05 | PASS | 48.49 |
| `union_news_g_h3` | long | 3 | leftover | list | none | 47% | 56% | 9/18 | +0.56 | 11 | 60 | +5.42 | -6.31 | -1.30 | -1.36 | PASS | 48.117 |
| `union_w_hot_cond_h3` | long | 3 | leftover | list | none | 47% | 61% | 9/18 | +0.89 | 15 | 93 | +9.31 | -10.32 | +6.78 | +7.66 | PASS | 47.647 |
| `coil_h3_exit_alarm` | long | 3 | leftover | list | none | 53% | 39% | 9/18 | +0.28 | 11 | 68 | +4.77 | -7.70 | +5.61 | +11.37 | PASS | 46.652 |
| `yday_gainer_h3` | long | 3 | leftover | list | none | 47% | 39% | 9/18 | +3.56 | 20 | 72 | +9.66 | -7.97 | -1.27 | +21.27 | PASS | 46.169 |
| `probable_probable_ok_h3` | long | 3 | leftover | list | none | 48% | 33% | 9/18 | +1.71 | 12 | 44 | +7.18 | -8.57 | -3.22 | +6.24 | PASS | 41.883 |
| `union_blue_coil_h3` | long | 3 | leftover | list | none | 57% | 50% | 8/18 | -0.84 | 10 | 54 | +6.26 | -6.44 | -0.84 | +4.30 | PASS | 41.077 |
| `union_blue_h1` | long | 1 | leftover | list | none | 49% | 39% | 7/18 | -1.39 | 7 | 23 | +4.77 | -5.08 | +6.25 | +7.20 | PASS | 40.023 |
| `union_e_green_h1` | long | 1 | leftover | list | none | 53% | 44% | 8/18 | +0.00 | 3 | 25 | +6.36 | -12.69 | +5.99 | +4.66 | PASS | 38.331 |
| `union_white_h1` | long | 1 | leftover | list | none | 49% | 33% | 8/18 | -0.77 | 2 | 20 | +5.48 | -5.31 | -0.77 | +2.54 | PASS | 37.748 |
| `yday_gainer_h1` | long | 1 | leftover | list | none | 45% | 33% | 7/18 | -0.67 | 10 | 35 | +6.14 | -4.94 | +3.72 | +4.76 | PASS | 36.87 |
| `union_coil_off_h1` | long | 1 | leftover | list | none | 45% | 39% | 7/18 | -0.74 | 5 | 30 | +4.12 | -4.55 | +2.47 | +0.79 | PASS | 36.282 |
| `union_candle_h1` | long | 1 | leftover | list | none | 43% | 33% | 7/18 | -1.00 | 4 | 30 | +5.63 | -4.85 | +2.01 | +1.29 | PASS | 35.809 |
| `union_vol_g_h1` | long | 1 | leftover | list | none | 47% | 33% | 7/18 | -1.26 | 9 | 38 | +6.69 | -5.34 | +0.07 | +1.38 | PASS | 35.668 |
| `union_w_hot_candle_h1` | long | 1 | leftover | list | none | 46% | 39% | 7/18 | -0.07 | 6 | 41 | +4.82 | -4.90 | +0.41 | +1.37 | PASS | 35.649 |
| `union_join_vol_green_h1` | long | 1 | leftover | list | none | 51% | 39% | 6/18 | -0.04 | 4 | 27 | +5.26 | -5.23 | -2.08 | -2.91 | PASS | 35.052 |
| `union_hot_score_h1` | long | 1 | leftover | list | none | 47% | 39% | 6/18 | -1.47 | 7 | 44 | +6.36 | -6.10 | -0.97 | +0.18 | PASS | 34.615 |
| `yday_gainer_h5` | long | 5 | leftover | list | none | 52% | 39% | 7/18 | -1.10 | 20 | 89 | +12.99 | -8.51 | -7.02 | +20.39 | PASS | 33.241 |
| `union_hot_n12_h1` | long | 1 | leftover | list | none | 46% | 44% | 5/18 | -0.99 | 8 | 62 | +4.86 | -5.89 | -4.09 | -1.84 | PASS | 32.599 |
| `union_hot_score_h3` | long | 3 | leftover | list | none | 48% | 44% | 6/18 | -0.17 | 16 | 91 | +9.63 | -8.54 | +4.23 | +13.16 | PASS | 31.587 |
| `union_candle_h3` | long | 3 | leftover | list | none | 48% | 33% | 7/18 | -2.32 | 12 | 74 | +6.97 | -6.32 | -5.22 | +11.17 | PASS | 31.046 |
| `probable_h3` | long | 3 | leftover | list | none | 46% | 39% | 7/18 | -0.39 | 21 | 74 | +7.95 | -9.53 | -1.66 | +8.08 | PASS | 30.898 |
| `union_white_h3` | long | 3 | leftover | list | none | 44% | 39% | 7/18 | -1.06 | 7 | 50 | +8.33 | -6.57 | +2.58 | -0.69 | PASS | 30.677 |
| `union_cond_h1` | long | 1 | leftover | list | none | 42% | 33% | 4/18 | -2.27 | 1 | 29 | +5.22 | -4.49 | -2.87 | -1.99 | PASS | 30.445 |
| `union_vol_g_h5` | long | 5 | leftover | list | none | 49% | 33% | 7/18 | -1.55 | 23 | 84 | +13.62 | -9.04 | -9.32 | +25.44 | PASS | 30.191 |
| `union_w_hot_cond_h1` | long | 1 | leftover | list | none | 47% | 39% | 3/18 | -1.57 | 7 | 41 | +6.32 | -5.93 | -6.55 | -0.74 | PASS | 30.152 |
| `union_e_fresh_h1` | long | 1 | leftover | list | none | 47% | 44% | 3/18 | -4.72 | 4 | 30 | +5.37 | -11.01 | +3.76 | -4.91 | PASS | 29.077 |
| `union_blue_coil_h1` | long | 1 | leftover | list | none | 54% | 33% | 0/18 | -4.12 | 5 | 21 | +3.98 | -5.70 | -4.02 | +1.99 | PASS | 28.13 |
| `ohlc_hot_h3` | long | 3 | leftover | list | none | 46% | 44% | 5/18 | -0.97 | 13 | 76 | +5.33 | -7.81 | +0.32 | -4.88 | PASS | 27.16 |
| `ohlc_hot_coil_h1` | long | 1 | leftover | list | none | 39% | 33% | 5/18 | -0.77 | 1 | 6 | +1.87 | -4.05 | -12.04 | +1.52 | PASS | 26.877 |
| `union_coil_green_h1` | long | 1 | leftover | list | none | 49% | 39% | 0/18 | -7.19 | 6 | 27 | +4.39 | -5.85 | -3.51 | +3.66 | PASS | 26.684 |
| `union_blue_vol_h1` | long | 1 | leftover | list | none | 40% | 39% | 4/18 | -6.21 | 6 | 38 | +6.37 | -6.17 | -9.98 | -11.10 | PASS | 25.696 |
| `union_vol_green_h1` | long | 1 | leftover | list | none | 44% | 28% | 3/18 | -3.27 | 8 | 42 | +5.77 | -5.55 | -6.25 | -11.55 | PASS | 25.055 |
| `ohlc_hot_h1` | long | 1 | leftover | list | none | 42% | 50% | 0/18 | -1.22 | 7 | 34 | +2.49 | -6.12 | -5.98 | -3.86 | PASS | 23.647 |
| `short_last_red_h3` | short | 3 | leftover | list | none | 45% | 28% | 4/18 | -0.94 | 12 | 65 | +4.70 | -6.75 | -8.69 | -24.88 | PASS | 23.355 |
| `union_blue_vol_h3` | long | 3 | leftover | list | none | 41% | 39% | 4/18 | -1.18 | 11 | 61 | +8.95 | -7.99 | -4.94 | -10.16 | PASS | 23.352 |
| `short_extended_h1` | short | 1 | leftover | list | none | 50% | 33% | 0/18 | -2.93 | 10 | 47 | +5.48 | -7.58 | -10.08 | +1.14 | PASS | 22.81 |
| `probable_h1` | long | 1 | leftover | list | none | 44% | 22% | 0/18 | -3.49 | 10 | 34 | +4.87 | -4.61 | -3.49 | +0.91 | PASS | 22.723 |
| `short_extended_h3` | short | 3 | leftover | list | none | 50% | 50% | 0/18 | -1.26 | 23 | 87 | +8.90 | -9.46 | -6.69 | -10.27 | PASS | 22.712 |
| `union_news_g_h5` | long | 5 | leftover | list | none | 51% | 28% | 5/18 | -3.71 | 14 | 85 | +5.17 | -7.71 | -12.83 | +144.33 | PASS | 22.091 |
| `probable_h5` | long | 5 | leftover | list | none | 51% | 11% | 7/18 | -0.78 | 21 | 94 | +9.47 | -10.37 | -18.16 | +4.76 | PASS | 21.915 |
| `short_last_red_h1` | short | 1 | leftover | list | none | 42% | 22% | 1/18 | -1.32 | 6 | 35 | +4.27 | -4.31 | -8.82 | -8.01 | PASS | 21.777 |
| `union_vol_ab_h1` | long | 1 | leftover | list | none | 41% | 22% | 1/18 | -4.47 | 6 | 26 | +5.41 | -6.44 | -2.28 | -12.38 | PASS | 20.393 |
| `union_earn_react_h1` | long | 1 | leftover | list | none | 44% | 28% | 2/18 | -4.72 | 3 | 34 | +5.72 | -12.87 | -3.25 | -10.92 | PASS | 20.349 |
| `union_vol_green_h3` | long | 3 | leftover | list | none | 42% | 22% | 5/18 | -1.62 | 18 | 76 | +7.66 | -8.78 | -8.41 | -7.73 | PASS | 19.65 |
| `ohlc_hot_h5` | long | 5 | leftover | list | none | 46% | 28% | 3/18 | -0.97 | 21 | 93 | +6.67 | -7.58 | -8.55 | +79.09 | PASS | 18.982 |
| `union_join_vol_green_h3` | long | 3 | leftover | list | none | 39% | 28% | 5/18 | -2.75 | 10 | 51 | +5.91 | -9.73 | -9.38 | -10.64 | PASS | 18.437 |
| `union_vol_ab_h3` | long | 3 | leftover | list | none | 37% | 22% | 1/18 | -6.82 | 8 | 49 | +6.98 | -7.79 | -6.82 | -4.24 | PASS | 12.594 |
| `union_news_missing_h1` *(thin)* | long | 1 | leftover | list | none | 50% | 17% | 11/18 | +0.49 | 0 | 2 | +5.90 | -2.32 | +1.92 | +2.56 | PASS | 39.115 |
| `flatten_live_h1_topheavy` *(thin)* | long | 1 | topheavy | list | none | 56% | 17% | 7/18 | +0.00 | 1 | 1 | +7.90 | -3.17 | +8.83 | +4.99 | PASS | 29.049 |
| `flatten_live_h1_half` *(thin)* | long | 1 | half | list | none | 56% | 17% | 7/18 | +0.00 | 1 | 1 | +7.69 | -3.34 | +4.55 | +4.99 | PASS | 27.441 |
| `flatten_live_h1` *(thin)* | long | 1 | leftover | list | none | 56% | 17% | 7/18 | +0.00 | 1 | 1 | +7.23 | -3.69 | +9.78 | +4.99 | PASS | 26.491 |
| `flatten_live_h1_cut` *(thin)* | long | 1 | leftover | cut_loser | none | 56% | 17% | 7/18 | +0.00 | 1 | 1 | +7.23 | -3.69 | +9.78 | +4.99 | PASS | 26.491 |
| `flatten_live_h1_sboost` *(thin)* | long | 1 | leftover | list | both | 56% | 17% | 7/18 | +0.00 | 1 | 1 | +7.23 | -3.69 | +9.78 | +4.99 | PASS | 26.491 |
| `flatten_live_h1_sizeup` *(thin)* | long | 1 | leftover | list | sizeup | 56% | 17% | 7/18 | +0.00 | 1 | 1 | +7.23 | -3.69 | +9.78 | +4.99 | PASS | 26.491 |
| `flatten_live_h1_time` *(thin)* | long | 1 | leftover | time | none | 56% | 17% | 7/18 | +0.00 | 1 | 1 | +7.23 | -3.69 | +9.78 | +4.99 | PASS | 26.491 |
| `flatten_live_h1_trail` *(thin)* | long | 1 | leftover | trail | none | 56% | 17% | 7/18 | +0.00 | 1 | 1 | +7.23 | -3.69 | +9.78 | +4.99 | PASS | 26.491 |
| `flatten_live_h1_rankw` *(thin)* | long | 1 | rank_w | list | none | 56% | 17% | 7/18 | +0.00 | 1 | 1 | +7.12 | -3.76 | +6.55 | +4.99 | PASS | 25.697 |
| `flatten_live_h3` *(thin)* | long | 3 | leftover | list | none | 50% | 22% | 7/18 | +0.00 | 2 | 7 | +8.40 | -5.14 | +4.92 | +9.59 | PASS | 16.189 |
| `union_vol_missing_h1` *(thin)* | long | 1 | leftover | list | none | 50% | 6% | 1/18 | +0.00 | 0 | 2 | +7.93 | -3.05 | +1.44 | +1.88 | PASS | 10.728 |
| `union_vol_missing_h3` *(thin)* | long | 3 | leftover | list | none | 50% | 17% | 1/18 | +0.00 | 1 | 5 | +11.98 | -3.98 | +3.58 | +5.59 | PASS | 9.636 |
| `flatten_live_h5` *(thin)* | long | 5 | leftover | list | none | 38% | 22% | 7/18 | +0.00 | 3 | 9 | +10.21 | -6.91 | +5.85 | +8.02 | PASS | 9.002 |
| `union_news_missing_h3` *(thin)* | long | 3 | leftover | list | none | 44% | 33% | 1/18 | -0.06 | 1 | 7 | +6.62 | -3.49 | +3.51 | +5.14 | PASS | 7.765 |
| `union_catal_present_h1` *(thin)* | long | 1 | leftover | list | none | 33% | 17% | 4/18 | -3.25 | 0 | 1 | +2.17 | -5.70 | -3.25 | +2.85 | PASS | 0.78 |
| `short_r_down_h1` *(thin)* | short | 1 | leftover | list | none | — | 0% | 0/18 | +0.00 | 0 | 0 | — | — | +0.00 | +0.00 | PASS | -15.0 |
| `short_r_down_h3` *(thin)* | short | 3 | leftover | list | none | — | 0% | 0/18 | +0.00 | 0 | 0 | — | — | +0.00 | +0.00 | PASS | -15.0 |
| `union_r_up_h1` *(thin)* | long | 1 | leftover | list | none | — | 0% | 0/18 | +0.00 | 0 | 0 | — | — | +0.00 | +0.00 | PASS | -15.0 |
| `union_r_up_h3` *(thin)* | long | 3 | leftover | list | none | — | 0% | 0/18 | +0.00 | 0 | 0 | — | — | +0.00 | +0.00 | PASS | -15.0 |
| `union_catal_present_h3` *(thin)* | long | 3 | leftover | list | none | 0% | 11% | 0/18 | -9.38 | 0 | 5 | — | -10.34 | -9.38 | -11.48 | PASS | -28.47 |
