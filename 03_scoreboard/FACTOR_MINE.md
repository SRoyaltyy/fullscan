# Factor strategy mine — 2026-08-13 → 2026-09-04

Leak-free 09:30 recipes: **161** · candidate rows **1347** · fill `09:30 open, whole shares, Futubull fees, leftover split, sell first, hard-red sit, cash+holdings audit`.

Cash book: $10k, whole shares, Futubull fees, leftover split, sell first, min-hold, 09:30 open, hard-red S≤−3 sit, shorts marked as a liability. Each session starts from leftover cash and lots actually held (butterfly). Size / sell / S-boost tweaks sit on the same ledger. Signal-only % is the old equal-weight path (not a fill). `flatten_h*` = wish-list (io/HOLD mornings still buy). `flatten_live_*` = only when the live flatten gate fires. Research only — does not change live `flatten_robust`.

Action blotters: [FACTOR_MINE_ACTION.md](FACTOR_MINE_ACTION.md).

| Strategy | Side | H | Size | Sell | Boost | Win% | $ days | Starts YES | Med start | Top-g | Losers | AvgW | AvgL | Book% | Signal% | Audit | Eff |
|---|---|---:|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---:|
| `flatten_h3_half` | long | 3 | half | list | none | 63% | 76% | 16/17 | +2.33 | 6 | 42 | +6.32 | -3.87 | +7.04 | +44.29 | PASS | 76.539 |
| `flatten_h3_time` | long | 3 | leftover | time | none | 63% | 65% | 16/17 | +4.48 | 6 | 42 | +7.40 | -3.81 | +11.56 | +44.29 | PASS | 76.43 |
| `flatten_h5` | long | 5 | leftover | list | none | 60% | 82% | 16/17 | +6.20 | 13 | 65 | +10.07 | -6.04 | +22.84 | +67.92 | PASS | 75.97 |
| `flatten_h5_cut` | long | 5 | leftover | cut_loser | none | 60% | 82% | 16/17 | +6.20 | 13 | 65 | +10.07 | -6.04 | +22.84 | +67.92 | PASS | 75.97 |
| `flatten_h5_sizeup` | long | 5 | leftover | list | sizeup | 60% | 82% | 16/17 | +6.20 | 13 | 65 | +10.07 | -6.04 | +22.84 | +67.92 | PASS | 75.97 |
| `flatten_h5_time` | long | 5 | leftover | time | none | 60% | 82% | 16/17 | +6.20 | 13 | 65 | +10.07 | -6.04 | +22.84 | +67.92 | PASS | 75.97 |
| `flatten_h5_trail` | long | 5 | leftover | trail | none | 60% | 82% | 16/17 | +6.20 | 13 | 65 | +10.07 | -6.04 | +22.84 | +67.92 | PASS | 75.97 |
| `flatten_h5_sboost` | long | 5 | leftover | list | both | 60% | 82% | 16/17 | +6.20 | 13 | 65 | +9.68 | -6.06 | +22.18 | +67.92 | PASS | 75.53 |
| `flatten_h1` | long | 1 | leftover | list | none | 57% | 59% | 16/17 | +5.38 | 2 | 14 | +4.91 | -3.53 | +15.53 | +21.67 | PASS | 74.982 |
| `flatten_h3` | long | 3 | leftover | list | none | 63% | 59% | 16/17 | +4.48 | 6 | 42 | +7.40 | -4.04 | +11.77 | +44.29 | PASS | 74.713 |
| `flatten_h3_cut` | long | 3 | leftover | cut_loser | none | 63% | 59% | 16/17 | +4.48 | 6 | 42 | +7.40 | -4.04 | +11.77 | +44.29 | PASS | 74.713 |
| `flatten_h3_sizeup` | long | 3 | leftover | list | sizeup | 63% | 59% | 16/17 | +4.48 | 6 | 42 | +7.40 | -4.04 | +11.77 | +44.29 | PASS | 74.713 |
| `flatten_h3_trail` | long | 3 | leftover | trail | none | 63% | 59% | 16/17 | +4.48 | 6 | 42 | +7.40 | -4.04 | +11.77 | +44.29 | PASS | 74.713 |
| `flatten_h5_rankw` | long | 5 | rank_w | list | none | 60% | 76% | 17/17 | +7.30 | 13 | 65 | +9.29 | -6.18 | +17.31 | +67.92 | PASS | 74.62 |
| `flatten_h3_sboost` | long | 3 | leftover | list | both | 63% | 59% | 16/17 | +4.48 | 6 | 42 | +7.17 | -4.01 | +11.69 | +44.29 | PASS | 74.492 |
| `flatten_h5_topheavy` | long | 5 | topheavy | list | none | 60% | 71% | 17/17 | +6.17 | 13 | 65 | +9.61 | -6.12 | +18.26 | +67.92 | PASS | 73.915 |
| `flatten_h5_half` | long | 5 | half | list | none | 60% | 76% | 16/17 | +3.17 | 13 | 65 | +8.90 | -4.97 | +12.43 | +67.92 | PASS | 73.857 |
| `union_h3_half` | long | 3 | half | list | none | 59% | 82% | 16/17 | +1.24 | 9 | 57 | +6.41 | -4.72 | +4.95 | +34.19 | PASS | 73.806 |
| `flatten_h3_rankw` | long | 3 | rank_w | list | none | 63% | 47% | 17/17 | +4.57 | 6 | 42 | +7.25 | -4.36 | +7.59 | +44.29 | PASS | 72.359 |
| `union_h3_time` | long | 3 | leftover | time | none | 59% | 65% | 16/17 | +2.51 | 9 | 57 | +7.07 | -4.63 | +9.91 | +34.19 | PASS | 71.863 |
| `union_h1_rankw` | long | 1 | rank_w | list | none | 52% | 59% | 16/17 | +5.55 | 5 | 21 | +4.55 | -3.80 | +13.90 | +18.57 | PASS | 71.765 |
| `union_h3` | long | 3 | leftover | list | none | 59% | 65% | 16/17 | +2.51 | 9 | 57 | +7.07 | -4.87 | +9.86 | +34.19 | PASS | 71.49 |
| `union_h3_cut` | long | 3 | leftover | cut_loser | none | 59% | 65% | 16/17 | +2.51 | 9 | 57 | +7.07 | -4.87 | +9.86 | +34.19 | PASS | 71.49 |
| `union_h3_sizeup` | long | 3 | leftover | list | sizeup | 59% | 65% | 16/17 | +2.51 | 9 | 57 | +7.07 | -4.87 | +9.86 | +34.19 | PASS | 71.49 |
| `union_h3_trail` | long | 3 | leftover | trail | none | 59% | 65% | 16/17 | +2.51 | 9 | 57 | +7.07 | -4.87 | +9.86 | +34.19 | PASS | 71.49 |
| `union_h1_topheavy` | long | 1 | topheavy | list | none | 52% | 53% | 16/17 | +4.75 | 5 | 21 | +4.63 | -3.59 | +15.68 | +18.57 | PASS | 71.325 |
| `union_h1_time` | long | 1 | leftover | time | none | 52% | 47% | 16/17 | +5.22 | 5 | 21 | +5.19 | -3.45 | +14.99 | +18.57 | PASS | 71.116 |
| `union_h3_sboost` | long | 3 | leftover | list | both | 59% | 65% | 16/17 | +2.51 | 9 | 57 | +6.93 | -5.04 | +9.80 | +34.19 | PASS | 71.101 |
| `flatten_h3_topheavy` | long | 3 | topheavy | list | none | 63% | 53% | 16/17 | +4.98 | 6 | 42 | +5.73 | -4.18 | +9.47 | +44.29 | PASS | 70.898 |
| `union_h1` | long | 1 | leftover | list | none | 52% | 47% | 16/17 | +4.49 | 5 | 21 | +4.57 | -3.61 | +14.54 | +18.57 | PASS | 69.857 |
| `union_h1_cut` | long | 1 | leftover | cut_loser | none | 52% | 47% | 16/17 | +4.49 | 5 | 21 | +4.57 | -3.61 | +14.54 | +18.57 | PASS | 69.857 |
| `union_h1_sizeup` | long | 1 | leftover | list | sizeup | 52% | 47% | 16/17 | +4.49 | 5 | 21 | +4.57 | -3.61 | +14.54 | +18.57 | PASS | 69.857 |
| `union_h1_trail` | long | 1 | leftover | trail | none | 52% | 47% | 16/17 | +4.49 | 5 | 21 | +4.57 | -3.61 | +14.54 | +18.57 | PASS | 69.857 |
| `union_h1_sboost` | long | 1 | leftover | list | both | 52% | 47% | 16/17 | +4.49 | 5 | 21 | +4.51 | -3.76 | +14.59 | +18.57 | PASS | 69.531 |
| `union_h5_rankw` | long | 5 | rank_w | list | none | 58% | 76% | 16/17 | +3.78 | 16 | 86 | +8.94 | -7.09 | +12.88 | +58.01 | PASS | 69.503 |
| `union_h5` | long | 5 | leftover | list | none | 58% | 82% | 14/17 | +2.89 | 16 | 86 | +9.28 | -6.87 | +18.84 | +58.01 | PASS | 69.082 |
| `union_h5_cut` | long | 5 | leftover | cut_loser | none | 58% | 82% | 14/17 | +2.89 | 16 | 86 | +9.28 | -6.87 | +18.84 | +58.01 | PASS | 69.082 |
| `union_h5_sizeup` | long | 5 | leftover | list | sizeup | 58% | 82% | 14/17 | +2.89 | 16 | 86 | +9.28 | -6.87 | +18.84 | +58.01 | PASS | 69.082 |
| `union_h5_trail` | long | 5 | leftover | trail | none | 58% | 82% | 14/17 | +2.89 | 16 | 86 | +9.28 | -6.87 | +18.84 | +58.01 | PASS | 69.082 |
| `union_h5_time` | long | 5 | leftover | time | none | 58% | 82% | 14/17 | +2.52 | 16 | 86 | +9.34 | -6.87 | +18.41 | +58.01 | PASS | 69.057 |
| `union_h3_exit_news_r` | long | 3 | leftover | list | none | 57% | 59% | 16/17 | +2.51 | 10 | 59 | +7.37 | -5.38 | +9.85 | +30.35 | PASS | 68.721 |
| `union_join_present_h3` | long | 3 | leftover | list | none | 57% | 59% | 16/17 | +2.51 | 10 | 59 | +7.37 | -5.38 | +9.85 | +30.35 | PASS | 68.721 |
| `union_join_present_h1` | long | 1 | leftover | list | none | 51% | 47% | 16/17 | +4.49 | 5 | 22 | +4.57 | -3.81 | +13.80 | +18.15 | PASS | 68.625 |
| `union_last_green_h1` | long | 1 | leftover | list | none | 50% | 41% | 16/17 | +4.64 | 5 | 24 | +5.06 | -3.59 | +13.14 | +15.19 | PASS | 67.901 |
| `union_h5_sboost` | long | 5 | leftover | list | both | 58% | 82% | 14/17 | +2.89 | 16 | 86 | +8.68 | -7.78 | +18.18 | +58.01 | PASS | 67.813 |
| `union_h5_topheavy` | long | 5 | topheavy | list | none | 58% | 71% | 15/17 | +3.37 | 16 | 86 | +8.93 | -6.96 | +15.08 | +58.01 | PASS | 67.303 |
| `union_h1_half` | long | 1 | half | list | none | 52% | 41% | 16/17 | +2.13 | 5 | 21 | +4.43 | -3.73 | +5.58 | +18.57 | PASS | 66.942 |
| `union_h5_half` | long | 5 | half | list | none | 58% | 76% | 14/17 | +1.24 | 16 | 86 | +8.37 | -6.02 | +9.27 | +58.01 | PASS | 66.675 |
| `union_h3_rankw` | long | 3 | rank_w | list | none | 59% | 53% | 15/17 | +3.45 | 9 | 57 | +6.92 | -5.14 | +5.94 | +34.19 | PASS | 66.547 |
| `union_h3_topheavy` | long | 3 | topheavy | list | none | 59% | 53% | 15/17 | +3.37 | 9 | 57 | +6.03 | -5.06 | +10.51 | +34.19 | PASS | 66.469 |
| `union_h3_exit_alarm` | long | 3 | leftover | list | none | 57% | 53% | 16/17 | +2.51 | 10 | 59 | +6.12 | -5.21 | +8.38 | +26.70 | PASS | 66.34 |
| `union_candle_score_h1` | long | 1 | leftover | list | none | 45% | 53% | 16/17 | +4.55 | 7 | 33 | +5.78 | -4.14 | +9.42 | +2.71 | PASS | 66.238 |
| `union_w_hot_candle_h3` | long | 3 | leftover | list | none | 53% | 65% | 16/17 | +3.73 | 17 | 77 | +7.47 | -6.93 | +13.09 | +27.64 | PASS | 65.708 |
| `union_news_present_h3` | long | 3 | leftover | list | none | 57% | 53% | 16/17 | +2.60 | 9 | 53 | +5.59 | -5.42 | +2.81 | +23.08 | PASS | 65.266 |
| `union_candle_score_h3` | long | 3 | leftover | list | none | 49% | 59% | 16/17 | +3.36 | 16 | 75 | +8.61 | -5.53 | +11.72 | +23.12 | PASS | 65.235 |
| `short_news_r_h3` | short | 3 | leftover | list | none | 60% | 53% | 17/17 | +3.06 | 3 | 35 | +5.38 | -4.08 | +6.97 | +10.62 | PASS | 65.146 |
| `union_news_present_h1` | long | 1 | leftover | list | none | 49% | 35% | 16/17 | +4.49 | 5 | 21 | +4.24 | -3.80 | +13.17 | +15.48 | PASS | 65.065 |
| `short_news_r_h1` | short | 1 | leftover | list | none | 52% | 41% | 17/17 | +1.70 | 2 | 16 | +3.88 | -3.10 | +0.28 | +7.11 | PASS | 65.024 |
| `union_ab_g_h1` | long | 1 | leftover | list | none | 44% | 41% | 16/17 | +3.92 | 3 | 14 | +4.94 | -4.14 | +10.69 | +5.13 | PASS | 64.619 |
| `union_e_fresh_h3` | long | 3 | leftover | list | none | 51% | 65% | 16/17 | +5.53 | 11 | 56 | +8.33 | -13.59 | +27.57 | -12.67 | PASS | 63.879 |
| `union_h5_exit_alarm` | long | 5 | leftover | list | none | 57% | 65% | 14/17 | +2.89 | 17 | 87 | +7.65 | -7.35 | +13.73 | +61.86 | PASS | 62.684 |
| `union_join_g_h1` | long | 1 | leftover | list | none | 53% | 47% | 12/17 | +4.52 | 5 | 21 | +4.52 | -4.24 | +9.87 | +14.43 | PASS | 62.373 |
| `union_break10_h3` | long | 3 | leftover | list | none | 50% | 53% | 16/17 | +3.42 | 14 | 62 | +6.66 | -6.44 | +2.04 | +6.99 | PASS | 61.157 |
| `union_blue_h1` | long | 1 | leftover | list | none | 48% | 41% | 13/17 | +1.29 | 6 | 20 | +5.00 | -4.36 | +8.13 | +9.21 | PASS | 60.998 |
| `union_e_green_h1` | long | 1 | leftover | list | none | 53% | 47% | 15/17 | +4.28 | 3 | 25 | +6.41 | -12.33 | +11.27 | +4.66 | PASS | 60.692 |
| `union_last_green_h5` | long | 5 | leftover | list | none | 52% | 76% | 12/17 | +2.93 | 17 | 91 | +9.48 | -8.41 | +19.41 | +52.19 | PASS | 60.578 |
| `union_e_green_h3` | long | 3 | leftover | list | none | 50% | 41% | 15/17 | +9.63 | 8 | 41 | +7.33 | -12.38 | +47.92 | +23.38 | PASS | 59.535 |
| `union_ret_5_h3` | long | 3 | leftover | list | none | 49% | 65% | 14/17 | +3.59 | 19 | 92 | +9.33 | -9.04 | +15.42 | +21.11 | PASS | 59.236 |
| `union_hot_n4_h1` | long | 1 | leftover | list | none | 52% | 47% | 10/17 | +0.53 | 3 | 20 | +7.39 | -4.54 | +9.79 | +2.44 | PASS | 59.119 |
| `union_white_coil_h1` | long | 1 | leftover | list | none | 56% | 35% | 11/17 | +0.47 | 3 | 13 | +4.58 | -4.33 | +2.05 | +7.42 | PASS | 57.999 |
| `union_hot_score_h3` | long | 3 | leftover | list | none | 52% | 53% | 14/17 | +2.12 | 16 | 84 | +9.63 | -8.54 | +6.61 | +17.08 | PASS | 57.887 |
| `union_ret_5_h1` | long | 1 | leftover | list | none | 42% | 59% | 11/17 | +1.44 | 8 | 44 | +6.64 | -5.21 | +4.96 | -1.46 | PASS | 56.348 |
| `union_ab_g_h3` | long | 3 | leftover | list | none | 57% | 29% | 13/17 | +0.79 | 6 | 41 | +6.05 | -5.25 | +0.79 | +12.84 | PASS | 56.278 |
| `union_h3_exit_red` | long | 3 | leftover | list | none | 55% | 47% | 12/17 | +0.66 | 12 | 66 | +6.43 | -6.80 | +4.14 | +31.33 | PASS | 55.72 |
| `union_last_green_h3` | long | 3 | leftover | list | none | 55% | 47% | 12/17 | +0.66 | 12 | 66 | +6.43 | -6.80 | +4.14 | +32.74 | PASS | 55.72 |
| `short_alarm_h1` | short | 1 | leftover | list | none | 48% | 29% | 12/17 | +1.10 | 6 | 26 | +4.51 | -2.93 | +2.60 | +2.41 | PASS | 55.249 |
| `union_earn_react_h3` | long | 3 | leftover | list | none | 42% | 53% | 15/17 | +5.35 | 9 | 54 | +8.98 | -14.31 | +22.58 | -26.79 | PASS | 55.14 |
| `union_break10_h1` | long | 1 | leftover | list | none | 47% | 41% | 11/17 | +3.09 | 5 | 27 | +5.97 | -5.42 | +0.00 | -3.22 | PASS | 55.061 |
| `union_w_hot_cond_h3` | long | 3 | leftover | list | none | 50% | 65% | 11/17 | +2.47 | 15 | 83 | +9.31 | -9.66 | +9.54 | +13.41 | PASS | 54.818 |
| `short_alarm_h3` | short | 3 | leftover | list | none | 58% | 59% | 12/17 | +1.52 | 7 | 52 | +7.09 | -9.39 | +2.77 | +11.88 | PASS | 54.642 |
| `yday_gainer_h1` | long | 1 | leftover | list | none | 44% | 41% | 11/17 | +0.18 | 9 | 32 | +5.98 | -5.08 | +4.56 | +5.63 | PASS | 54.278 |
| `flatten_vol_g_h3` | long | 3 | leftover | list | none | 45% | 41% | 14/17 | +3.66 | 3 | 12 | +8.37 | -7.08 | -1.69 | +0.40 | PASS | 54.187 |
| `union_join_g_h3` | long | 3 | leftover | list | none | 56% | 41% | 9/17 | +0.69 | 10 | 53 | +7.33 | -5.97 | +2.25 | +18.26 | PASS | 53.267 |
| `union_news_vol_h1` | long | 1 | leftover | list | none | 43% | 35% | 13/17 | +1.92 | 1 | 12 | +3.93 | -5.53 | +2.72 | +3.52 | PASS | 53.009 |
| `union_white_h1` | long | 1 | leftover | list | none | 47% | 35% | 10/17 | +0.32 | 2 | 16 | +5.56 | -5.33 | +0.95 | +3.48 | PASS | 52.035 |
| `union_news_vol_h3` | long | 3 | leftover | list | none | 37% | 53% | 16/17 | +2.35 | 3 | 35 | +3.99 | -5.48 | +0.89 | -1.72 | PASS | 50.45 |
| `union_blue_h3` | long | 3 | leftover | list | none | 54% | 41% | 9/17 | +1.79 | 8 | 49 | +6.24 | -6.88 | -1.53 | +8.43 | PASS | 50.146 |
| `ohlc_hot_h3` | long | 3 | leftover | list | none | 52% | 53% | 10/17 | +0.04 | 13 | 71 | +5.65 | -7.81 | +1.06 | -2.36 | PASS | 49.63 |
| `ohlc_hot_coil_h1` | long | 1 | leftover | list | none | 39% | 41% | 12/17 | +0.82 | 1 | 6 | +1.74 | -4.39 | -10.54 | +1.52 | PASS | 49.045 |
| `union_coil_off_h5` | long | 5 | leftover | list | none | 47% | 53% | 10/17 | +0.74 | 15 | 85 | +4.30 | -23.59 | +9.72 | +7.47 | PASS | 44.862 |
| `union_white_h5` | long | 5 | leftover | list | none | 48% | 65% | 8/17 | -0.86 | 14 | 50 | +9.97 | -8.15 | +6.18 | +14.63 | PASS | 40.304 |
| `union_blue_coil_h3` | long | 3 | leftover | list | none | 55% | 47% | 8/17 | -1.73 | 7 | 49 | +6.26 | -6.44 | -2.31 | +3.07 | PASS | 39.94 |
| `union_w_hot_candle_h1` | long | 1 | leftover | list | none | 45% | 47% | 8/17 | -0.70 | 6 | 36 | +4.88 | -5.11 | +0.71 | +0.69 | PASS | 39.55 |
| `union_last_red_h3` | long | 3 | leftover | list | none | 48% | 59% | 8/17 | -0.62 | 11 | 63 | +6.32 | -6.74 | +2.39 | +21.46 | PASS | 39.467 |
| `union_coil_green_h1` | long | 1 | leftover | list | none | 49% | 41% | 7/17 | -0.31 | 5 | 26 | +3.95 | -4.59 | +3.41 | +4.19 | PASS | 38.759 |
| `union_candle_h1` | long | 1 | leftover | list | none | 43% | 35% | 7/17 | -0.85 | 3 | 27 | +5.69 | -4.77 | +2.13 | +1.03 | PASS | 36.873 |
| `union_hot_n12_h1` | long | 1 | leftover | list | none | 46% | 53% | 6/17 | -0.01 | 8 | 55 | +5.18 | -6.09 | -3.12 | -2.24 | PASS | 36.734 |
| `union_vol_g_h1` | long | 1 | leftover | list | none | 45% | 35% | 7/17 | -0.69 | 8 | 35 | +6.37 | -5.20 | +0.64 | +1.35 | PASS | 36.068 |
| `union_cond_h3` | long | 3 | leftover | list | none | 48% | 53% | 6/17 | -2.40 | 6 | 67 | +7.51 | -6.04 | +0.40 | +4.26 | PASS | 35.495 |
| `union_last_red_h1` | long | 1 | leftover | list | none | 44% | 47% | 5/17 | -0.52 | 5 | 31 | +4.32 | -5.00 | +0.75 | +6.47 | PASS | 34.709 |
| `union_w_hot_cond_h1` | long | 1 | leftover | list | none | 48% | 47% | 4/17 | -0.81 | 7 | 34 | +6.13 | -6.32 | -4.34 | +0.19 | PASS | 34.643 |
| `union_hot_score_h1` | long | 1 | leftover | list | none | 47% | 41% | 5/17 | -1.34 | 7 | 39 | +6.58 | -6.60 | -0.85 | -0.76 | PASS | 34.23 |
| `union_white_coil_h3` | long | 3 | leftover | list | none | 48% | 41% | 8/17 | -0.27 | 5 | 36 | +5.16 | -7.01 | +4.51 | +1.34 | PASS | 33.976 |
| `union_coil_off_h1` | long | 1 | leftover | list | none | 43% | 35% | 6/17 | -0.89 | 4 | 29 | +3.73 | -4.07 | +2.33 | -0.74 | PASS | 33.975 |
| `union_coil_off_h3` | long | 3 | leftover | list | none | 47% | 41% | 8/17 | -1.94 | 10 | 65 | +4.18 | -6.93 | +4.00 | +5.43 | PASS | 33.397 |
| `union_e_fresh_h1` | long | 1 | leftover | list | none | 47% | 53% | 4/17 | -0.09 | 4 | 30 | +5.31 | -10.33 | +8.62 | -4.91 | PASS | 33.351 |
| `union_blue_coil_h1` | long | 1 | leftover | list | none | 52% | 35% | 3/17 | -2.42 | 4 | 18 | +4.09 | -5.04 | -2.42 | +3.85 | PASS | 33.292 |
| `probable_probable_ok_h1` | long | 1 | leftover | list | none | 44% | 29% | 7/17 | -0.46 | 4 | 18 | +3.71 | -4.58 | -1.69 | +1.91 | PASS | 32.995 |
| `union_join_vol_green_h1` | long | 1 | leftover | list | none | 50% | 35% | 5/17 | -0.53 | 4 | 23 | +5.27 | -5.44 | -2.54 | -4.08 | PASS | 32.992 |
| `yday_gainer_h5` | long | 5 | leftover | list | none | 49% | 41% | 7/17 | -2.47 | 17 | 83 | +12.99 | -8.51 | -8.09 | +8.88 | PASS | 32.865 |
| `union_coil_green_h3` | long | 3 | leftover | list | none | 45% | 47% | 7/17 | -2.61 | 12 | 64 | +4.91 | -6.05 | +2.12 | +0.72 | PASS | 32.135 |
| `ohlc_hot_h1` | long | 1 | leftover | list | none | 43% | 53% | 5/17 | -1.23 | 7 | 31 | +2.49 | -6.10 | -5.07 | -3.30 | PASS | 32.072 |
| `coil_h3_exit_alarm` | long | 3 | leftover | list | none | 51% | 35% | 7/17 | -2.13 | 8 | 64 | +4.08 | -7.70 | +4.05 | +8.25 | PASS | 32.039 |
| `ohlc_hot_h5` | long | 5 | leftover | list | none | 53% | 35% | 8/17 | -0.27 | 21 | 86 | +6.67 | -7.67 | -8.27 | +86.73 | PASS | 31.376 |
| `union_candle_h3` | long | 3 | leftover | list | none | 48% | 29% | 7/17 | -2.67 | 11 | 66 | +6.97 | -6.32 | -5.27 | +13.07 | PASS | 31.316 |
| `union_news_g_h3` | long | 3 | leftover | list | none | 45% | 53% | 6/17 | -0.63 | 9 | 56 | +4.15 | -6.43 | -4.81 | -3.60 | PASS | 31.169 |
| `union_cond_n4_h3` | long | 3 | leftover | list | none | 45% | 53% | 5/17 | -3.99 | 3 | 32 | +4.91 | -6.24 | -0.18 | +5.57 | PASS | 31.098 |
| `union_news_g_h1` | long | 1 | leftover | list | none | 46% | 47% | 2/17 | -1.23 | 1 | 18 | +3.31 | -5.36 | -0.31 | +0.23 | PASS | 30.703 |
| `union_white_h3` | long | 3 | leftover | list | none | 40% | 41% | 6/17 | -0.97 | 7 | 42 | +8.33 | -6.57 | +2.69 | +2.15 | PASS | 29.846 |
| `union_cond_h1` | long | 1 | leftover | list | none | 41% | 35% | 3/17 | -1.72 | 1 | 26 | +5.22 | -4.39 | -2.31 | -2.71 | PASS | 29.604 |
| `union_vol_g_h3` | long | 3 | leftover | list | none | 42% | 35% | 8/17 | -0.42 | 13 | 68 | +9.08 | -8.21 | -2.49 | +1.81 | PASS | 29.262 |
| `yday_gainer_h3` | long | 3 | leftover | list | none | 46% | 35% | 5/17 | -0.46 | 17 | 68 | +9.66 | -7.97 | -5.11 | +13.89 | PASS | 28.956 |
| `probable_h1` | long | 1 | leftover | list | none | 42% | 29% | 4/17 | -3.17 | 9 | 32 | +4.33 | -4.69 | -3.17 | +1.13 | PASS | 28.48 |
| `union_vol_g_h5` | long | 5 | leftover | list | none | 46% | 29% | 7/17 | -4.40 | 20 | 80 | +13.62 | -9.04 | -10.12 | +7.06 | PASS | 27.915 |
| `short_last_red_h3` | short | 3 | leftover | list | none | 47% | 35% | 4/17 | -0.14 | 12 | 63 | +4.70 | -6.74 | -7.96 | -22.66 | PASS | 26.018 |
| `union_earn_react_h1` | long | 1 | leftover | list | none | 44% | 35% | 4/17 | -0.09 | 3 | 34 | +5.70 | -12.62 | +1.42 | -10.92 | PASS | 25.692 |
| `short_last_red_h1` | short | 1 | leftover | list | none | 44% | 29% | 2/17 | -3.06 | 6 | 34 | +4.54 | -4.45 | -8.15 | -7.21 | PASS | 25.65 |
| `union_vol_green_h1` | long | 1 | leftover | list | none | 44% | 29% | 3/17 | -3.49 | 7 | 37 | +5.35 | -5.88 | -6.39 | -11.83 | PASS | 25.093 |
| `short_extended_h1` | short | 1 | leftover | list | none | 52% | 35% | 0/17 | -1.87 | 10 | 42 | +5.05 | -7.95 | -8.50 | +3.26 | PASS | 24.139 |
| `union_blue_vol_h1` | long | 1 | leftover | list | none | 38% | 35% | 3/17 | -5.08 | 5 | 34 | +6.67 | -5.98 | -9.72 | -10.68 | PASS | 23.589 |
| `probable_h3` | long | 3 | leftover | list | none | 44% | 29% | 4/17 | -4.25 | 18 | 71 | +8.38 | -9.42 | -6.50 | +0.05 | PASS | 23.343 |
| `short_extended_h3` | short | 3 | leftover | list | none | 50% | 47% | 0/17 | -2.92 | 23 | 81 | +8.30 | -9.66 | -8.24 | -11.79 | PASS | 22.126 |
| `probable_probable_ok_h3` | long | 3 | leftover | list | none | 45% | 29% | 4/17 | -5.05 | 9 | 42 | +7.17 | -8.44 | -11.54 | -8.12 | PASS | 21.692 |
| `union_news_g_h5` | long | 5 | leftover | list | none | 54% | 29% | 4/17 | -3.51 | 12 | 80 | +3.75 | -7.57 | -10.92 | +152.54 | PASS | 21.673 |
| `union_vol_ab_h1` | long | 1 | leftover | list | none | 38% | 29% | 0/17 | -3.67 | 5 | 22 | +5.43 | -6.15 | -1.48 | -13.14 | PASS | 20.01 |
| `probable_h5` | long | 5 | leftover | list | none | 48% | 12% | 6/17 | -3.53 | 18 | 88 | +9.47 | -10.37 | -19.05 | -5.25 | PASS | 19.697 |
| `union_vol_green_h3` | long | 3 | leftover | list | none | 41% | 24% | 5/17 | -4.74 | 14 | 70 | +7.66 | -8.78 | -10.93 | -13.47 | PASS | 19.323 |
| `union_blue_vol_h3` | long | 3 | leftover | list | none | 35% | 35% | 3/17 | -4.08 | 8 | 56 | +8.95 | -7.96 | -5.07 | -11.54 | PASS | 18.622 |
| `union_join_vol_green_h3` | long | 3 | leftover | list | none | 40% | 29% | 4/17 | -2.32 | 9 | 44 | +5.91 | -9.73 | -8.86 | -10.84 | PASS | 18.378 |
| `union_vol_ab_h3` | long | 3 | leftover | list | none | 32% | 24% | 0/17 | -6.81 | 7 | 44 | +6.98 | -7.79 | -6.81 | -3.28 | PASS | 9.23 |
| `union_news_missing_h1` *(thin)* | long | 1 | leftover | list | none | 50% | 18% | 11/17 | +0.49 | 0 | 2 | +5.90 | -2.32 | +1.92 | +2.56 | PASS | 40.211 |
| `flatten_live_h1_topheavy` *(thin)* | long | 1 | topheavy | list | none | 56% | 18% | 7/17 | +0.00 | 1 | 1 | +7.90 | -3.17 | +8.83 | +4.99 | PASS | 29.817 |
| `flatten_live_h1_half` *(thin)* | long | 1 | half | list | none | 56% | 18% | 7/17 | +0.00 | 1 | 1 | +7.69 | -3.34 | +4.55 | +4.99 | PASS | 28.21 |
| `flatten_live_h1` *(thin)* | long | 1 | leftover | list | none | 56% | 18% | 7/17 | +0.00 | 1 | 1 | +7.23 | -3.69 | +9.78 | +4.99 | PASS | 27.259 |
| `flatten_live_h1_cut` *(thin)* | long | 1 | leftover | cut_loser | none | 56% | 18% | 7/17 | +0.00 | 1 | 1 | +7.23 | -3.69 | +9.78 | +4.99 | PASS | 27.259 |
| `flatten_live_h1_sboost` *(thin)* | long | 1 | leftover | list | both | 56% | 18% | 7/17 | +0.00 | 1 | 1 | +7.23 | -3.69 | +9.78 | +4.99 | PASS | 27.259 |
| `flatten_live_h1_sizeup` *(thin)* | long | 1 | leftover | list | sizeup | 56% | 18% | 7/17 | +0.00 | 1 | 1 | +7.23 | -3.69 | +9.78 | +4.99 | PASS | 27.259 |
| `flatten_live_h1_time` *(thin)* | long | 1 | leftover | time | none | 56% | 18% | 7/17 | +0.00 | 1 | 1 | +7.23 | -3.69 | +9.78 | +4.99 | PASS | 27.259 |
| `flatten_live_h1_trail` *(thin)* | long | 1 | leftover | trail | none | 56% | 18% | 7/17 | +0.00 | 1 | 1 | +7.23 | -3.69 | +9.78 | +4.99 | PASS | 27.259 |
| `flatten_live_h1_rankw` *(thin)* | long | 1 | rank_w | list | none | 56% | 18% | 7/17 | +0.00 | 1 | 1 | +7.12 | -3.76 | +6.55 | +4.99 | PASS | 26.465 |
| `flatten_live_h3` *(thin)* | long | 3 | leftover | list | none | 50% | 29% | 7/17 | +0.00 | 2 | 7 | +8.40 | -5.14 | +4.92 | +9.59 | PASS | 18.199 |
| `flatten_live_h5` *(thin)* | long | 5 | leftover | list | none | 38% | 29% | 7/17 | +0.00 | 3 | 9 | +10.21 | -6.91 | +5.85 | +8.02 | PASS | 11.012 |
| `union_vol_missing_h1` *(thin)* | long | 1 | leftover | list | none | 50% | 6% | 1/17 | +0.00 | 0 | 2 | +7.93 | -3.05 | +1.44 | +1.88 | PASS | 10.872 |
| `union_vol_missing_h3` *(thin)* | long | 3 | leftover | list | none | 50% | 18% | 1/17 | +0.00 | 1 | 5 | +11.98 | -3.98 | +3.58 | +5.59 | PASS | 9.912 |
| `union_news_missing_h3` *(thin)* | long | 3 | leftover | list | none | 44% | 35% | 1/17 | -0.06 | 1 | 7 | +6.62 | -3.49 | +3.51 | +5.14 | PASS | 8.237 |
| `union_catal_present_h1` *(thin)* | long | 1 | leftover | list | none | 33% | 18% | 4/17 | -3.25 | 0 | 1 | +2.17 | -5.70 | -3.25 | +2.85 | PASS | 1.304 |
| `short_r_down_h1` *(thin)* | short | 1 | leftover | list | none | — | 0% | 0/17 | +0.00 | 0 | 0 | — | — | +0.00 | +0.00 | PASS | -15.0 |
| `short_r_down_h3` *(thin)* | short | 3 | leftover | list | none | — | 0% | 0/17 | +0.00 | 0 | 0 | — | — | +0.00 | +0.00 | PASS | -15.0 |
| `union_r_up_h1` *(thin)* | long | 1 | leftover | list | none | — | 0% | 0/17 | +0.00 | 0 | 0 | — | — | +0.00 | +0.00 | PASS | -15.0 |
| `union_r_up_h3` *(thin)* | long | 3 | leftover | list | none | — | 0% | 0/17 | +0.00 | 0 | 0 | — | — | +0.00 | +0.00 | PASS | -15.0 |
| `union_catal_present_h3` *(thin)* | long | 3 | leftover | list | none | 17% | 12% | 4/17 | -7.86 | 0 | 4 | +3.17 | -10.34 | -7.86 | -10.11 | PASS | -16.169 |
