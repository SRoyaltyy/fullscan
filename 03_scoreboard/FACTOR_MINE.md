# Factor strategy mine — 2026-08-13 → 2026-09-08

Leak-free 09:30 recipes: **161** · candidate rows **1511** · fill `09:30 open, whole shares, Futubull fees, leftover split, sell first, hard-red sit, cash+holdings audit`.

Cash book: $10k, whole shares, Futubull fees, leftover split, sell first, min-hold, 09:30 open, hard-red S≤−3 sit, shorts marked as a liability. Each session starts from leftover cash and lots actually held (butterfly). Cash-start buttons wake a sleeve on date X with $10k and no lots (same rules). Stock investigator quotes 09:30 cameras / coaches / news from repo files. Size / sell / S-boost tweaks sit on the same ledger. Signal-only % is the old equal-weight path (not a fill). `flatten_h*` = wish-list (io/HOLD mornings still buy). `flatten_live_*` = only when the live flatten gate fires. Research only — does not change live `flatten_robust`.

Action blotters: [FACTOR_MINE_ACTION.md](FACTOR_MINE_ACTION.md).

| Strategy | Side | H | Size | Sell | Boost | Win% | $ days | Starts YES | Med start | Top-g | Losers | AvgW | AvgL | Book% | Signal% | Audit | Eff |
|---|---|---:|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---:|
| `flatten_h3_time` | long | 3 | leftover | time | none | 62% | 63% | 17/19 | +4.85 | 9 | 49 | +6.78 | -3.81 | +12.64 | +46.49 | PASS | 73.629 |
| `flatten_h5` | long | 5 | leftover | list | none | 56% | 79% | 17/19 | +7.62 | 17 | 73 | +10.07 | -6.04 | +27.30 | +75.94 | PASS | 73.408 |
| `flatten_h5_cut` | long | 5 | leftover | cut_loser | none | 56% | 79% | 17/19 | +7.62 | 17 | 73 | +10.07 | -6.04 | +27.30 | +75.94 | PASS | 73.408 |
| `flatten_h5_sizeup` | long | 5 | leftover | list | sizeup | 56% | 79% | 17/19 | +7.62 | 17 | 73 | +10.07 | -6.04 | +27.30 | +75.94 | PASS | 73.408 |
| `flatten_h5_time` | long | 5 | leftover | time | none | 56% | 79% | 17/19 | +7.62 | 17 | 73 | +10.07 | -6.04 | +27.30 | +75.94 | PASS | 73.408 |
| `flatten_h5_trail` | long | 5 | leftover | trail | none | 56% | 79% | 17/19 | +7.62 | 17 | 73 | +10.07 | -6.04 | +27.30 | +75.94 | PASS | 73.408 |
| `union_h3_time` | long | 3 | leftover | time | none | 57% | 68% | 17/19 | +8.50 | 13 | 63 | +7.50 | -4.33 | +16.34 | +42.44 | PASS | 73.101 |
| `flatten_h5_sboost` | long | 5 | leftover | list | both | 56% | 79% | 17/19 | +7.62 | 17 | 73 | +9.68 | -6.06 | +26.59 | +75.94 | PASS | 72.963 |
| `flatten_h3_half` | long | 3 | half | list | none | 62% | 68% | 17/19 | +1.60 | 9 | 49 | +6.00 | -3.87 | +6.30 | +46.49 | PASS | 72.571 |
| `flatten_h3` | long | 3 | leftover | list | none | 62% | 58% | 17/19 | +4.85 | 9 | 49 | +6.78 | -4.04 | +12.88 | +46.49 | PASS | 72.091 |
| `flatten_h3_cut` | long | 3 | leftover | cut_loser | none | 62% | 58% | 17/19 | +4.85 | 9 | 49 | +6.78 | -4.04 | +12.88 | +46.49 | PASS | 72.091 |
| `flatten_h3_sizeup` | long | 3 | leftover | list | sizeup | 62% | 58% | 17/19 | +4.85 | 9 | 49 | +6.78 | -4.04 | +12.88 | +46.49 | PASS | 72.091 |
| `flatten_h3_trail` | long | 3 | leftover | trail | none | 62% | 58% | 17/19 | +4.85 | 9 | 49 | +6.78 | -4.04 | +12.88 | +46.49 | PASS | 72.091 |
| `flatten_h3_sboost` | long | 3 | leftover | list | both | 62% | 58% | 17/19 | +4.85 | 9 | 49 | +6.63 | -4.01 | +12.81 | +46.49 | PASS | 71.96 |
| `union_h3_half` | long | 3 | half | list | none | 57% | 79% | 17/19 | +2.86 | 13 | 63 | +6.08 | -4.50 | +6.65 | +42.44 | PASS | 71.837 |
| `union_h3` | long | 3 | leftover | list | none | 57% | 68% | 17/19 | +8.23 | 13 | 63 | +6.59 | -4.53 | +16.00 | +42.44 | PASS | 71.659 |
| `union_h3_cut` | long | 3 | leftover | cut_loser | none | 57% | 68% | 17/19 | +8.23 | 13 | 63 | +6.59 | -4.53 | +16.00 | +42.44 | PASS | 71.659 |
| `union_h3_sizeup` | long | 3 | leftover | list | sizeup | 57% | 68% | 17/19 | +8.23 | 13 | 63 | +6.59 | -4.53 | +16.00 | +42.44 | PASS | 71.659 |
| `union_h3_trail` | long | 3 | leftover | trail | none | 57% | 68% | 17/19 | +8.23 | 13 | 63 | +6.59 | -4.53 | +16.00 | +42.44 | PASS | 71.659 |
| `union_h3_sboost` | long | 3 | leftover | list | both | 57% | 68% | 17/19 | +8.23 | 13 | 63 | +6.52 | -4.71 | +15.93 | +42.44 | PASS | 71.284 |
| `flatten_h3_topheavy` | long | 3 | topheavy | list | none | 62% | 53% | 18/19 | +4.86 | 9 | 49 | +5.50 | -4.18 | +10.19 | +46.49 | PASS | 70.143 |
| `flatten_h5_half` | long | 5 | half | list | none | 56% | 68% | 17/19 | +2.79 | 17 | 73 | +8.90 | -4.97 | +13.22 | +75.94 | PASS | 69.817 |
| `flatten_h5_rankw` | long | 5 | rank_w | list | none | 56% | 68% | 17/19 | +7.94 | 17 | 73 | +9.29 | -6.18 | +21.76 | +75.94 | PASS | 69.657 |
| `flatten_h1` | long | 1 | leftover | list | none | 53% | 53% | 17/19 | +2.58 | 2 | 18 | +4.67 | -3.95 | +12.46 | +18.63 | PASS | 69.365 |
| `union_h5_time` | long | 5 | leftover | time | none | 54% | 79% | 16/19 | +9.57 | 21 | 93 | +9.34 | -6.87 | +26.12 | +78.01 | PASS | 69.152 |
| `union_h5` | long | 5 | leftover | list | none | 54% | 79% | 16/19 | +8.61 | 21 | 93 | +9.28 | -6.87 | +25.11 | +78.01 | PASS | 68.959 |
| `union_h5_cut` | long | 5 | leftover | cut_loser | none | 54% | 79% | 16/19 | +8.61 | 21 | 93 | +9.28 | -6.87 | +25.11 | +78.01 | PASS | 68.959 |
| `union_h5_sizeup` | long | 5 | leftover | list | sizeup | 54% | 79% | 16/19 | +8.61 | 21 | 93 | +9.28 | -6.87 | +25.11 | +78.01 | PASS | 68.959 |
| `union_h5_trail` | long | 5 | leftover | trail | none | 54% | 79% | 16/19 | +8.61 | 21 | 93 | +9.28 | -6.87 | +25.11 | +78.01 | PASS | 68.959 |
| `flatten_h5_topheavy` | long | 5 | topheavy | list | none | 56% | 63% | 17/19 | +6.95 | 17 | 73 | +9.61 | -6.12 | +21.40 | +75.94 | PASS | 68.881 |
| `flatten_h3_rankw` | long | 3 | rank_w | list | none | 62% | 47% | 17/19 | +5.37 | 9 | 49 | +6.65 | -4.36 | +9.04 | +46.49 | PASS | 68.647 |
| `union_h3_topheavy` | long | 3 | topheavy | list | none | 57% | 53% | 18/19 | +7.13 | 13 | 63 | +5.76 | -4.70 | +14.59 | +42.44 | PASS | 68.457 |
| `union_h1_rankw` | long | 1 | rank_w | list | none | 49% | 53% | 17/19 | +4.45 | 6 | 24 | +5.03 | -4.31 | +12.73 | +16.86 | PASS | 67.791 |
| `union_h3_exit_news_r` | long | 3 | leftover | list | none | 55% | 63% | 16/19 | +8.24 | 13 | 66 | +6.82 | -5.02 | +15.99 | +34.83 | PASS | 67.537 |
| `union_join_present_h3` | long | 3 | leftover | list | none | 55% | 63% | 16/19 | +8.22 | 13 | 66 | +6.82 | -5.02 | +15.98 | +34.53 | PASS | 67.535 |
| `union_h1_time` | long | 1 | leftover | time | none | 49% | 47% | 17/19 | +4.06 | 6 | 24 | +4.91 | -3.94 | +13.80 | +16.86 | PASS | 67.301 |
| `union_h1` | long | 1 | leftover | list | none | 49% | 47% | 17/19 | +4.30 | 6 | 24 | +5.04 | -4.11 | +14.32 | +16.86 | PASS | 67.278 |
| `union_h1_cut` | long | 1 | leftover | cut_loser | none | 49% | 47% | 17/19 | +4.30 | 6 | 24 | +5.04 | -4.11 | +14.32 | +16.86 | PASS | 67.278 |
| `union_h1_sizeup` | long | 1 | leftover | list | sizeup | 49% | 47% | 17/19 | +4.30 | 6 | 24 | +5.04 | -4.11 | +14.32 | +16.86 | PASS | 67.278 |
| `union_h1_trail` | long | 1 | leftover | trail | none | 49% | 47% | 17/19 | +4.30 | 6 | 24 | +5.04 | -4.11 | +14.32 | +16.86 | PASS | 67.278 |
| `union_h5_half` | long | 5 | half | list | none | 54% | 74% | 17/19 | +3.75 | 21 | 93 | +8.37 | -6.02 | +10.43 | +78.01 | PASS | 67.224 |
| `union_h1_sboost` | long | 1 | leftover | list | both | 49% | 47% | 17/19 | +4.30 | 6 | 24 | +4.96 | -4.20 | +14.38 | +16.86 | PASS | 67.057 |
| `union_h3_rankw` | long | 3 | rank_w | list | none | 57% | 53% | 17/19 | +7.39 | 13 | 63 | +6.48 | -4.85 | +10.05 | +42.44 | PASS | 67.019 |
| `union_h5_sboost` | long | 5 | leftover | list | both | 54% | 79% | 15/19 | +8.61 | 21 | 93 | +8.68 | -7.78 | +24.47 | +78.01 | PASS | 66.379 |
| `union_h1_topheavy` | long | 1 | topheavy | list | none | 49% | 47% | 16/19 | +2.77 | 6 | 24 | +5.11 | -4.10 | +13.51 | +16.86 | PASS | 65.942 |
| `union_hot_n4_h1` | long | 1 | leftover | list | none | 49% | 47% | 16/19 | +2.92 | 3 | 24 | +7.59 | -4.42 | +12.41 | +3.61 | PASS | 64.684 |
| `union_h3_exit_alarm` | long | 3 | leftover | list | none | 54% | 58% | 16/19 | +4.32 | 13 | 66 | +6.09 | -5.09 | +10.30 | +26.30 | PASS | 64.529 |
| `union_h1_half` | long | 1 | half | list | none | 49% | 42% | 17/19 | +1.59 | 6 | 24 | +4.91 | -4.23 | +5.01 | +16.86 | PASS | 64.51 |
| `union_h5_topheavy` | long | 5 | topheavy | list | none | 54% | 68% | 15/19 | +7.44 | 21 | 93 | +8.93 | -6.96 | +19.50 | +78.01 | PASS | 64.362 |
| `union_join_present_h1` | long | 1 | leftover | list | none | 48% | 47% | 16/19 | +2.85 | 6 | 26 | +4.94 | -4.35 | +11.99 | +14.68 | PASS | 64.349 |
| `union_news_present_h3` | long | 3 | leftover | list | none | 56% | 58% | 16/19 | +8.31 | 12 | 60 | +5.36 | -5.04 | +8.54 | +27.02 | PASS | 64.273 |
| `union_candle_score_h1` | long | 1 | leftover | list | none | 42% | 53% | 17/19 | +4.57 | 8 | 38 | +5.55 | -4.16 | +9.73 | +3.31 | PASS | 63.761 |
| `union_h5_rankw` | long | 5 | rank_w | list | none | 54% | 74% | 14/19 | +7.34 | 21 | 93 | +8.94 | -7.09 | +16.20 | +78.01 | PASS | 63.487 |
| `union_white_coil_h1` | long | 1 | leftover | list | none | 61% | 37% | 15/19 | +1.22 | 3 | 14 | +4.06 | -4.24 | +2.81 | +10.04 | PASS | 63.447 |
| `union_ret_5_h1` | long | 1 | leftover | list | none | 44% | 53% | 18/19 | +3.64 | 8 | 49 | +6.17 | -5.24 | +7.22 | +2.54 | PASS | 63.016 |
| `union_candle_score_h3` | long | 3 | leftover | list | none | 46% | 58% | 17/19 | +3.68 | 18 | 83 | +7.76 | -5.20 | +13.23 | +24.16 | PASS | 62.777 |
| `union_news_present_h1` | long | 1 | leftover | list | none | 46% | 37% | 16/19 | +2.85 | 6 | 25 | +4.70 | -4.40 | +11.37 | +12.10 | PASS | 61.081 |
| `union_w_hot_candle_h3` | long | 3 | leftover | list | none | 48% | 63% | 16/19 | +2.81 | 19 | 87 | +7.24 | -6.40 | +12.26 | +24.35 | PASS | 60.721 |
| `union_break10_h1` | long | 1 | leftover | list | none | 48% | 42% | 16/19 | +4.69 | 5 | 31 | +5.35 | -4.63 | +1.54 | -0.68 | PASS | 60.607 |
| `union_e_fresh_h3` | long | 3 | leftover | list | none | 48% | 63% | 16/19 | +6.73 | 11 | 63 | +10.15 | -11.46 | +29.35 | -17.95 | PASS | 60.423 |
| `short_news_r_h3` | short | 3 | leftover | list | none | 57% | 47% | 16/19 | +4.27 | 3 | 36 | +5.29 | -4.08 | +8.22 | +9.76 | PASS | 59.552 |
| `short_news_r_h1` | short | 1 | leftover | list | none | 48% | 42% | 15/19 | +0.64 | 2 | 17 | +4.05 | -2.80 | -0.42 | +5.63 | PASS | 59.357 |
| `union_news_g_h1` | long | 1 | leftover | list | none | 47% | 42% | 16/19 | +1.31 | 2 | 20 | +3.46 | -5.22 | +3.57 | +3.34 | PASS | 59.35 |
| `union_news_vol_h3` | long | 3 | leftover | list | none | 46% | 58% | 18/19 | +6.14 | 6 | 41 | +5.66 | -5.34 | +7.15 | +6.32 | PASS | 58.935 |
| `union_h5_exit_alarm` | long | 5 | leftover | list | none | 52% | 63% | 14/19 | +3.63 | 21 | 95 | +7.76 | -7.41 | +14.71 | +63.58 | PASS | 58.799 |
| `union_last_green_h5` | long | 5 | leftover | list | none | 50% | 74% | 13/19 | +3.52 | 19 | 103 | +9.48 | -8.41 | +20.59 | +61.29 | PASS | 58.498 |
| `union_last_red_h3` | long | 3 | leftover | list | none | 48% | 58% | 14/19 | +0.96 | 13 | 71 | +6.13 | -5.69 | +4.06 | +24.00 | PASS | 56.993 |
| `union_news_vol_h1` | long | 1 | leftover | list | none | 47% | 37% | 15/19 | +1.55 | 3 | 14 | +4.10 | -5.35 | +3.68 | +7.11 | PASS | 56.716 |
| `union_ret_5_h3` | long | 3 | leftover | list | none | 49% | 58% | 15/19 | +2.76 | 19 | 102 | +8.58 | -8.13 | +14.28 | +20.44 | PASS | 56.598 |
| `union_last_green_h1` | long | 1 | leftover | list | none | 48% | 42% | 11/19 | +0.90 | 6 | 31 | +4.94 | -4.02 | +9.21 | +9.28 | PASS | 55.871 |
| `union_e_green_h3` | long | 3 | leftover | list | none | 45% | 42% | 15/19 | +11.11 | 8 | 47 | +8.85 | -9.82 | +49.90 | +15.44 | PASS | 55.769 |
| `union_h3_exit_red` | long | 3 | leftover | list | none | 52% | 53% | 13/19 | +3.69 | 14 | 76 | +5.63 | -6.63 | +7.34 | +32.17 | PASS | 54.937 |
| `union_last_green_h3` | long | 3 | leftover | list | none | 52% | 53% | 13/19 | +3.69 | 14 | 76 | +5.63 | -6.63 | +7.34 | +33.59 | PASS | 54.937 |
| `union_coil_off_h1` | long | 1 | leftover | list | none | 43% | 42% | 13/19 | +1.31 | 5 | 32 | +4.25 | -4.45 | +4.58 | +0.91 | PASS | 54.31 |
| `union_coil_off_h3` | long | 3 | leftover | list | none | 48% | 47% | 15/19 | +2.73 | 13 | 74 | +4.10 | -6.33 | +9.19 | +15.27 | PASS | 53.824 |
| `union_cond_h1` | long | 1 | leftover | list | none | 42% | 42% | 12/19 | +2.35 | 1 | 29 | +5.17 | -4.32 | +1.73 | +0.60 | PASS | 53.688 |
| `union_vol_g_h3` | long | 3 | leftover | list | none | 44% | 42% | 16/19 | +2.78 | 17 | 73 | +7.85 | -7.13 | +1.92 | +8.45 | PASS | 52.629 |
| `union_earn_react_h3` | long | 3 | leftover | list | none | 39% | 53% | 15/19 | +3.14 | 9 | 61 | +10.95 | -12.29 | +24.10 | -31.22 | PASS | 52.019 |
| `union_candle_h1` | long | 1 | leftover | list | none | 41% | 37% | 12/19 | +0.88 | 4 | 33 | +5.44 | -4.79 | +3.96 | +0.48 | PASS | 51.549 |
| `union_break10_h3` | long | 3 | leftover | list | none | 46% | 42% | 13/19 | +1.31 | 14 | 73 | +6.66 | -5.67 | +0.22 | +6.25 | PASS | 50.742 |
| `union_white_coil_h3` | long | 3 | leftover | list | none | 57% | 42% | 11/19 | +1.58 | 7 | 42 | +4.91 | -6.83 | +5.78 | +6.94 | PASS | 50.313 |
| `flatten_vol_g_h3` | long | 3 | leftover | list | none | 46% | 32% | 14/19 | +3.32 | 5 | 16 | +7.91 | -7.08 | -2.01 | +2.23 | PASS | 50.144 |
| `union_ab_g_h3` | long | 3 | leftover | list | none | 52% | 21% | 13/19 | +0.27 | 9 | 50 | +5.47 | -5.21 | +0.27 | +9.09 | PASS | 49.71 |
| `union_coil_off_h5` | long | 5 | leftover | list | none | 47% | 53% | 14/19 | +5.43 | 19 | 96 | +4.30 | -23.59 | +14.75 | +18.78 | PASS | 49.286 |
| `union_join_vol_green_h1` | long | 1 | leftover | list | none | 49% | 37% | 10/19 | +0.07 | 4 | 27 | +5.25 | -4.88 | -1.99 | -3.13 | PASS | 49.254 |
| `probable_probable_ok_h1` | long | 1 | leftover | list | none | 44% | 37% | 10/19 | +0.74 | 5 | 20 | +4.51 | -4.50 | -0.52 | -0.94 | PASS | 48.53 |
| `union_coil_green_h3` | long | 3 | leftover | list | none | 41% | 53% | 12/19 | +0.75 | 15 | 75 | +4.18 | -5.59 | +6.61 | +0.36 | PASS | 47.276 |
| `probable_probable_ok_h3` | long | 3 | leftover | list | none | 48% | 37% | 12/19 | +0.28 | 12 | 45 | +6.46 | -8.10 | -2.64 | +5.45 | PASS | 46.099 |
| `union_cond_h3` | long | 3 | leftover | list | none | 49% | 58% | 9/19 | +0.00 | 8 | 74 | +7.08 | -5.54 | +1.75 | +11.49 | PASS | 40.831 |
| `union_blue_coil_h3` | long | 3 | leftover | list | none | 56% | 47% | 9/19 | +0.00 | 11 | 54 | +5.25 | -6.16 | -1.22 | +6.50 | PASS | 40.748 |
| `union_join_g_h1` | long | 1 | leftover | list | none | 47% | 42% | 8/19 | -0.37 | 5 | 27 | +4.47 | -4.59 | +4.77 | +8.00 | PASS | 40.358 |
| `union_join_g_h3` | long | 3 | leftover | list | none | 52% | 47% | 8/19 | -0.18 | 12 | 64 | +6.48 | -5.75 | +2.46 | +13.26 | PASS | 39.286 |
| `union_cond_n4_h3` | long | 3 | leftover | list | none | 49% | 58% | 9/19 | +0.00 | 5 | 36 | +5.19 | -6.08 | +3.55 | +12.70 | PASS | 39.143 |
| `union_blue_h3` | long | 3 | leftover | list | none | 52% | 47% | 9/19 | +0.00 | 11 | 56 | +5.60 | -6.58 | -1.26 | +6.79 | PASS | 39.062 |
| `union_blue_h1` | long | 1 | leftover | list | none | 45% | 42% | 7/19 | -1.74 | 7 | 23 | +4.70 | -4.76 | +4.96 | +5.14 | PASS | 38.557 |
| `union_white_h5` | long | 5 | leftover | list | none | 51% | 58% | 8/19 | -0.41 | 17 | 60 | +9.97 | -8.15 | +3.60 | +15.36 | PASS | 37.651 |
| `union_e_green_h1` | long | 1 | leftover | list | none | 52% | 42% | 8/19 | +0.00 | 3 | 25 | +6.36 | -12.69 | +5.99 | +3.31 | PASS | 36.813 |
| `union_w_hot_cond_h3` | long | 3 | leftover | list | none | 45% | 58% | 9/19 | +0.00 | 15 | 93 | +8.98 | -8.90 | +7.44 | +8.83 | PASS | 36.766 |
| `union_vol_g_h1` | long | 1 | leftover | list | none | 43% | 37% | 8/19 | -0.77 | 9 | 37 | +6.51 | -5.43 | +0.57 | -1.03 | PASS | 36.241 |
| `union_news_g_h3` | long | 3 | leftover | list | none | 46% | 53% | 9/19 | +0.00 | 11 | 64 | +4.64 | -6.13 | -1.40 | +0.12 | PASS | 35.889 |
| `coil_h3_exit_alarm` | long | 3 | leftover | list | none | 52% | 42% | 9/19 | +0.00 | 10 | 73 | +4.31 | -6.99 | +5.59 | +12.60 | PASS | 35.837 |
| `union_hot_n12_h1` | long | 1 | leftover | list | none | 45% | 47% | 7/19 | -0.33 | 8 | 63 | +5.00 | -5.73 | -3.44 | -1.62 | PASS | 35.496 |
| `yday_gainer_h1` | long | 1 | leftover | list | none | 43% | 37% | 7/19 | -1.15 | 10 | 36 | +5.63 | -5.36 | +3.22 | +1.54 | PASS | 35.307 |
| `union_last_red_h1` | long | 1 | leftover | list | none | 44% | 47% | 6/19 | -0.69 | 5 | 33 | +3.74 | -4.97 | +0.42 | +6.20 | PASS | 34.86 |
| `yday_gainer_h3` | long | 3 | leftover | list | none | 46% | 37% | 9/19 | +0.00 | 20 | 74 | +8.51 | -7.14 | -1.76 | +16.31 | PASS | 34.558 |
| `union_ab_g_h1` | long | 1 | leftover | list | none | 39% | 32% | 7/19 | -1.20 | 4 | 19 | +4.52 | -4.61 | +5.25 | -0.34 | PASS | 34.129 |
| `flatten_live_h3` | long | 3 | leftover | list | none | 46% | 26% | 7/19 | -2.32 | 4 | 13 | +8.40 | -5.14 | +2.49 | +7.46 | PASS | 33.006 |
| `union_white_h1` | long | 1 | leftover | list | none | 49% | 37% | 4/19 | -1.47 | 2 | 20 | +5.38 | -5.25 | -1.47 | +2.03 | PASS | 32.458 |
| `union_w_hot_cond_h1` | long | 1 | leftover | list | none | 46% | 42% | 4/19 | -1.80 | 7 | 41 | +6.44 | -5.55 | -5.30 | +0.30 | PASS | 32.319 |
| `flatten_live_h1_topheavy` | long | 1 | topheavy | list | none | 38% | 16% | 6/19 | -5.02 | 1 | 5 | +7.90 | -4.28 | +3.38 | +1.27 | PASS | 32.244 |
| `union_hot_score_h1` | long | 1 | leftover | list | none | 45% | 37% | 5/19 | -2.15 | 7 | 45 | +6.47 | -5.95 | -1.65 | +0.11 | PASS | 31.971 |
| `union_w_hot_candle_h1` | long | 1 | leftover | list | none | 44% | 37% | 5/19 | -0.58 | 7 | 42 | +5.03 | -4.84 | -0.11 | +1.23 | PASS | 31.792 |
| `union_vol_green_h1` | long | 1 | leftover | list | none | 40% | 32% | 8/19 | -0.46 | 8 | 43 | +6.06 | -5.41 | -3.51 | -12.86 | PASS | 31.677 |
| `flatten_live_h1_half` | long | 1 | half | list | none | 38% | 16% | 6/19 | -2.48 | 1 | 5 | +7.69 | -4.42 | +1.96 | +1.27 | PASS | 31.517 |
| `flatten_live_h1` | long | 1 | leftover | list | none | 38% | 16% | 6/19 | -4.80 | 1 | 5 | +7.23 | -4.54 | +4.52 | +1.27 | PASS | 31.155 |
| `flatten_live_h1_cut` | long | 1 | leftover | cut_loser | none | 38% | 16% | 6/19 | -4.80 | 1 | 5 | +7.23 | -4.54 | +4.52 | +1.27 | PASS | 31.155 |
| `flatten_live_h1_sboost` | long | 1 | leftover | list | both | 38% | 16% | 6/19 | -4.80 | 1 | 5 | +7.23 | -4.54 | +4.52 | +1.27 | PASS | 31.155 |
| `flatten_live_h1_sizeup` | long | 1 | leftover | list | sizeup | 38% | 16% | 6/19 | -4.80 | 1 | 5 | +7.23 | -4.54 | +4.52 | +1.27 | PASS | 31.155 |
| `flatten_live_h1_time` | long | 1 | leftover | time | none | 38% | 16% | 6/19 | -4.80 | 1 | 5 | +7.23 | -4.54 | +4.52 | +1.27 | PASS | 31.155 |
| `flatten_live_h1_trail` | long | 1 | leftover | trail | none | 38% | 16% | 6/19 | -4.80 | 1 | 5 | +7.23 | -4.54 | +4.52 | +1.27 | PASS | 31.155 |
| `union_candle_h3` | long | 3 | leftover | list | none | 46% | 32% | 8/19 | -2.37 | 12 | 76 | +5.99 | -5.83 | -5.29 | +12.39 | PASS | 30.464 |
| `yday_gainer_h5` | long | 5 | leftover | list | none | 48% | 37% | 7/19 | -2.89 | 20 | 95 | +12.99 | -8.51 | -9.32 | +14.30 | PASS | 30.389 |
| `flatten_live_h1_rankw` | long | 1 | rank_w | list | none | 38% | 16% | 6/19 | -5.28 | 1 | 5 | +7.12 | -4.68 | +0.93 | +1.27 | PASS | 30.246 |
| `short_alarm_h1` | short | 1 | leftover | list | none | 51% | 26% | 3/19 | -0.79 | 7 | 32 | +4.37 | -3.74 | +0.68 | +3.24 | PASS | 29.626 |
| `union_vol_ab_h1` | long | 1 | leftover | list | none | 37% | 26% | 7/19 | -1.22 | 6 | 26 | +5.51 | -5.43 | +1.03 | -11.79 | PASS | 29.333 |
| `union_vol_g_h5` | long | 5 | leftover | list | none | 48% | 32% | 7/19 | -1.66 | 24 | 86 | +13.62 | -9.04 | -9.49 | +27.08 | PASS | 29.181 |
| `union_e_fresh_h1` | long | 1 | leftover | list | none | 47% | 42% | 3/19 | -4.72 | 4 | 30 | +5.37 | -11.01 | +3.76 | -5.66 | PASS | 28.386 |
| `union_hot_score_h3` | long | 3 | leftover | list | none | 44% | 42% | 5/19 | -0.22 | 16 | 96 | +9.63 | -7.82 | +4.18 | +10.85 | PASS | 28.385 |
| `short_alarm_h3` | short | 3 | leftover | list | none | 58% | 47% | 2/19 | -1.10 | 10 | 58 | +7.09 | -9.39 | +0.12 | +8.78 | PASS | 28.092 |
| `probable_h3` | long | 3 | leftover | list | none | 44% | 32% | 7/19 | -0.88 | 21 | 77 | +7.33 | -8.53 | -3.20 | +4.75 | PASS | 28.006 |
| `union_blue_coil_h1` | long | 1 | leftover | list | none | 50% | 37% | 0/19 | -3.80 | 5 | 20 | +3.62 | -5.44 | -3.80 | +0.89 | PASS | 27.732 |
| `union_coil_green_h1` | long | 1 | leftover | list | none | 46% | 42% | 1/19 | -5.82 | 6 | 31 | +4.47 | -5.91 | -2.11 | +2.93 | PASS | 27.42 |
| `union_white_h3` | long | 3 | leftover | list | none | 44% | 37% | 6/19 | -1.62 | 9 | 51 | +6.95 | -6.66 | +2.00 | +1.41 | PASS | 27.353 |
| `union_blue_vol_h1` | long | 1 | leftover | list | none | 40% | 42% | 4/19 | -4.83 | 6 | 36 | +6.36 | -6.11 | -9.50 | -11.22 | PASS | 26.69 |
| `union_news_g_h5` | long | 5 | leftover | list | none | 50% | 32% | 8/19 | -2.04 | 14 | 90 | +5.17 | -7.71 | -10.37 | +141.13 | PASS | 26.674 |
| `ohlc_hot_coil_h1` | long | 1 | leftover | list | none | 37% | 32% | 5/19 | -0.77 | 1 | 6 | +1.87 | -4.05 | -12.04 | +0.81 | PASS | 25.422 |
| `short_extended_h1` | short | 1 | leftover | list | none | 52% | 32% | 0/19 | -4.72 | 10 | 49 | +5.19 | -7.79 | -11.76 | +2.12 | PASS | 22.604 |
| `ohlc_hot_h1` | long | 1 | leftover | list | none | 41% | 47% | 0/19 | -2.60 | 7 | 37 | +2.35 | -5.70 | -7.31 | -4.44 | PASS | 22.332 |
| `short_extended_h3` | short | 3 | leftover | list | none | 51% | 47% | 0/19 | -1.67 | 24 | 93 | +8.47 | -9.74 | -7.17 | -9.22 | PASS | 22.296 |
| `short_last_red_h3` | short | 3 | leftover | list | none | 47% | 26% | 3/19 | -1.07 | 15 | 70 | +4.50 | -6.34 | -8.94 | -25.06 | PASS | 22.269 |
| `union_blue_vol_h3` | long | 3 | leftover | list | none | 43% | 37% | 3/19 | -1.31 | 13 | 62 | +7.74 | -6.88 | -5.42 | -7.12 | PASS | 21.962 |
| `probable_h1` | long | 1 | leftover | list | none | 41% | 26% | 0/19 | -3.62 | 10 | 36 | +4.38 | -4.85 | -3.62 | -1.98 | PASS | 21.608 |
| `short_last_red_h1` | short | 1 | leftover | list | none | 44% | 21% | 0/19 | -1.66 | 6 | 36 | +4.38 | -4.30 | -8.88 | -6.35 | PASS | 21.432 |
| `union_vol_green_h3` | long | 3 | leftover | list | none | 40% | 26% | 6/19 | -1.26 | 18 | 77 | +6.81 | -7.62 | -8.09 | -7.83 | PASS | 21.316 |
| `probable_h5` | long | 5 | leftover | list | none | 48% | 16% | 7/19 | -1.29 | 21 | 100 | +9.47 | -10.37 | -17.92 | +1.98 | PASS | 20.855 |
| `union_earn_react_h1` | long | 1 | leftover | list | none | 43% | 26% | 2/19 | -4.72 | 3 | 34 | +5.72 | -12.87 | -3.25 | -11.62 | PASS | 19.999 |
| `ohlc_hot_h3` | long | 3 | leftover | list | none | 42% | 42% | 0/19 | -2.18 | 13 | 83 | +5.33 | -7.47 | -1.10 | -7.36 | PASS | 18.055 |
| `ohlc_hot_h5` | long | 5 | leftover | list | none | 41% | 26% | 3/19 | -2.11 | 21 | 102 | +6.67 | -7.58 | -8.09 | +69.96 | PASS | 16.041 |
| `union_vol_ab_h3` | long | 3 | leftover | list | none | 36% | 26% | 1/19 | -6.93 | 10 | 51 | +5.99 | -6.49 | -6.93 | -0.13 | PASS | 13.92 |
| `union_join_vol_green_h3` | long | 3 | leftover | list | none | 35% | 26% | 1/19 | -2.98 | 10 | 53 | +5.49 | -8.11 | -9.59 | -11.75 | PASS | 11.312 |
| `union_news_missing_h1` *(thin)* | long | 1 | leftover | list | none | 50% | 16% | 11/19 | +0.49 | 0 | 2 | +5.90 | -2.32 | +1.92 | +2.56 | PASS | 38.134 |
| `union_vol_missing_h1` *(thin)* | long | 1 | leftover | list | none | 50% | 5% | 1/19 | +0.00 | 0 | 2 | +7.93 | -3.05 | +1.44 | +1.88 | PASS | 10.593 |
| `union_vol_missing_h3` *(thin)* | long | 3 | leftover | list | none | 50% | 16% | 1/19 | +0.00 | 1 | 5 | +11.98 | -3.98 | +3.58 | +5.59 | PASS | 9.385 |
| `flatten_live_h5` *(thin)* | long | 5 | leftover | list | none | 38% | 26% | 7/19 | -2.32 | 5 | 15 | +10.21 | -6.91 | +3.40 | +5.92 | PASS | 8.004 |
| `union_news_missing_h3` *(thin)* | long | 3 | leftover | list | none | 44% | 32% | 1/19 | -0.06 | 1 | 7 | +6.62 | -3.49 | +3.51 | +5.14 | PASS | 7.34 |
| `union_catal_present_h1` *(thin)* | long | 1 | leftover | list | none | 20% | 11% | 4/19 | -4.59 | 0 | 1 | +0.80 | -5.70 | -4.59 | +0.66 | PASS | -7.953 |
| `short_r_down_h1` *(thin)* | short | 1 | leftover | list | none | — | 0% | 0/19 | +0.00 | 0 | 0 | — | — | +0.00 | +0.00 | PASS | -15.0 |
| `short_r_down_h3` *(thin)* | short | 3 | leftover | list | none | — | 0% | 0/19 | +0.00 | 0 | 0 | — | — | +0.00 | +0.00 | PASS | -15.0 |
| `union_r_up_h1` *(thin)* | long | 1 | leftover | list | none | — | 0% | 0/19 | +0.00 | 0 | 0 | — | — | +0.00 | +0.00 | PASS | -15.0 |
| `union_r_up_h3` *(thin)* | long | 3 | leftover | list | none | — | 0% | 0/19 | +0.00 | 0 | 0 | — | — | +0.00 | +0.00 | PASS | -15.0 |
| `union_catal_present_h3` *(thin)* | long | 3 | leftover | list | none | 0% | 16% | 0/19 | -9.28 | 0 | 4 | — | -10.34 | -9.28 | -11.75 | PASS | -26.568 |
