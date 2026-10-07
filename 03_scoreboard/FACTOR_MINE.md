# Factor strategy mine — 2026-08-13 → 2026-10-07

Leak-free 09:30 recipes: **339** · candidate rows **3111** · fill `09:30 open, whole shares, Futubull fees, leftover split, sell first, hard-red sit, cash+holdings audit`.

Cash book: $10k, whole shares, Futubull fees, leftover split, sell first, min-hold, 09:30 open, hard-red S≤−3 sit, shorts marked as a liability. Each session starts from leftover cash and lots actually held (butterfly). Cash-start buttons wake a sleeve on date X with $10k and no lots (same rules). Stock investigator quotes 09:30 cameras / coaches / news from repo files. Size / sell / S-boost tweaks sit on the same ledger. Signal-only % is the old equal-weight path (not a fill). `flatten_h*` = wish-list (io/HOLD mornings still buy). `flatten_live_*` = only when the live flatten gate fires. Research only — does not change live `flatten_robust`.

Sessions before `2026-08-13` are **reconstructed** (rebuilt inputs, not a frozen 09:30 snapshot). From `2026-08-13` each day's inputs and buy/sell decisions are append-only.

Sessions before a recipe's creation date are **designed_after** (the rule was not in the book yet) and stay out of the real total. Holdup, overnight-mega, and Clock-B start `2026-09-21`. White-horizon and the stop brackets start `2026-09-14`.

Scoreboard files: slim `factor_mine.json` (stats / recipes / series) plus gzip shards in `factor_mine/shards/` (`books`, `starts`, `daily`, `probe`, `sim`). One file used to be 104MB and GitHub rejected the publish (100MB cap).

Combination books: **79** mixes on the same $10k cash ledger (shared leftover or split sleeves, official 09:30 / 16:00, hard-red sit, owner min-hold). Outperformers: `combo_sh_3070_shared`, `combo_sh_macd_5050_shared`, `combo_sh_5050_shared`, `combo_jer_5050_shared`, `combo_je1_5050_shared`, `combo_sf_7030_shared`, `combo_sf_5050_shared`.

A combo outperforms its members on the cash book when the audit passes and either (1) Book% is strictly higher than every member, or (2) Book% stays within 2pp of the richest member, max drawdown is strictly smaller than every member's drawdown, start-rate is at least the best member, both halves are green, and the worst session is no worse than the worst member session. Effectiveness / Signal% do not decide WIN.

Action blotters: [FACTOR_MINE_ACTION.md](FACTOR_MINE_ACTION.md).

| Strategy | Side | H | Size | Sell | Boost | Win% | $ days | Starts YES | Med start | Top-g | Losers | AvgW | AvgL | Book% | Signal% | Audit | Eff |
|---|---|---:|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---:|
| `combo_sh_5050_shared` | mix | 5 | leftover | list | none | 56% | 70% | 29/30 | +34.04 | 0 | 0 | +6.29 | -6.16 | +76.31 | — | PASS | 74.555 |
| `combo_sh_3070_shared` | mix | 5 | leftover | list | none | 55% | 63% | 29/30 | +45.46 | 0 | 0 | +6.33 | -6.06 | +77.56 | — | PASS | 72.943 |
| `combo_sh_macd_5050_shared` | mix | 5 | leftover | list | none | 57% | 60% | 29/30 | +42.33 | 0 | 0 | +6.32 | -5.90 | +95.16 | — | PASS | 72.898 |
| `combo_je1_5050_shared` | mix | 5 | leftover | list | none | 48% | 47% | 25/30 | +3.15 | 0 | 0 | +4.57 | -4.82 | -3.67 | — | PASS | 61.606 |
| `combo_jer_5050_shared` | mix | 5 | leftover | list | none | 43% | 50% | 22/30 | +1.04 | 0 | 0 | +6.32 | -5.76 | -6.89 | — | PASS | 57.146 |
| `combo_sf_5050_shared` | mix | 5 | leftover | list | none | 52% | 60% | 8/30 | -4.10 | 0 | 0 | +6.35 | -6.33 | -8.80 | — | PASS | 35.009 |
| `combo_sf_7030_shared` | mix | 5 | leftover | list | none | 53% | 57% | 8/30 | -2.71 | 0 | 0 | +6.11 | -6.85 | -8.46 | — | PASS | 34.221 |
| `union_hot_n4_holdup` | long | 1 | leftover | list | holdup | 51% | 53% | 29/30 | +44.44 | 22 | 40 | +9.93 | -6.31 | +48.28 | +89.81 | PASS | 75.291 |
| `combo_sh_7030_shared` | mix | 5 | leftover | list | none | 56% | 70% | 29/30 | +16.54 | 0 | 0 | +6.26 | -6.20 | +89.64 | — | PASS | 74.417 |
| `combo_oh_5050_shared` | mix | 5 | leftover | list | none | 52% | 43% | 29/30 | +37.96 | 0 | 0 | +7.28 | -6.09 | +52.11 | — | PASS | 71.9 |
| `union_hot_n4_h1` | long | 1 | leftover | list | none | 51% | 43% | 29/30 | +38.12 | 22 | 40 | +7.78 | -6.19 | +22.36 | +89.81 | PASS | 71.701 |
| `combo_he1_5050_shared` | mix | 5 | leftover | list | none | 50% | 53% | 29/30 | +27.57 | 0 | 0 | +5.75 | -5.82 | +20.90 | — | PASS | 70.775 |
| `combo_seh_502525_shared` | mix | 5 | leftover | list | none | 48% | 73% | 29/30 | +15.91 | 0 | 0 | +7.12 | -6.89 | +63.97 | — | PASS | 70.638 |
| `combo_seh_451540_shared` | mix | 5 | leftover | list | none | 47% | 70% | 29/30 | +25.44 | 0 | 0 | +7.15 | -7.13 | +66.23 | — | PASS | 69.688 |
| `combo_seh_601525_shared` | mix | 5 | leftover | list | none | 47% | 73% | 29/30 | +13.20 | 0 | 0 | +7.14 | -7.12 | +73.66 | — | PASS | 69.571 |
| `combo_seh_333_skip` | mix | 5 | leftover | list | none | 50% | 63% | 29/30 | +26.98 | 0 | 0 | +7.08 | -7.04 | +63.42 | — | PASS | 69.441 |
| `union_hot_score_h1` | long | 1 | leftover | list | none | 50% | 50% | 29/30 | +17.81 | 38 | 72 | +6.64 | -6.33 | +26.95 | +50.67 | PASS | 68.791 |
| `combo_seh_333_shared` | mix | 5 | leftover | list | none | 48% | 63% | 29/30 | +22.01 | 0 | 0 | +7.05 | -6.95 | +69.90 | — | PASS | 68.79 |
| `combo_seh_333_weather` | mix | 5 | leftover | list | none | 48% | 63% | 29/30 | +22.01 | 0 | 0 | +7.05 | -6.95 | +69.90 | — | PASS | 68.79 |
| `combo_seh_403525_shared` | mix | 5 | leftover | list | none | 48% | 63% | 29/30 | +17.14 | 0 | 0 | +7.17 | -6.82 | +57.26 | — | PASS | 68.733 |
| `combo_hn_7030_shared` | mix | 5 | leftover | list | none | 46% | 47% | 29/30 | +24.38 | 0 | 0 | +5.38 | -5.52 | +24.08 | — | PASS | 68.633 |
| `combo_seh_404020_shared` | mix | 5 | leftover | list | none | 49% | 60% | 29/30 | +13.58 | 0 | 0 | +7.00 | -6.90 | +57.69 | — | PASS | 68.234 |
| `combo_ee1_3070_shared` | mix | 5 | leftover | list | none | 43% | 63% | 29/30 | +14.46 | 0 | 0 | +9.47 | -7.62 | -11.31 | — | PASS | 67.911 |
| `combo_ee1_5050_shared` | mix | 5 | leftover | list | none | 43% | 63% | 29/30 | +14.46 | 0 | 0 | +9.47 | -7.62 | -11.31 | — | PASS | 67.911 |
| `combo_ee1_7030_shared` | mix | 5 | leftover | list | none | 43% | 63% | 29/30 | +14.46 | 0 | 0 | +9.47 | -7.62 | -11.31 | — | PASS | 67.911 |
| `union_w_hot_cond_h1` | long | 1 | leftover | list | none | 50% | 50% | 29/30 | +14.80 | 30 | 64 | +5.70 | -6.10 | +22.43 | +31.34 | PASS | 67.799 |
| `combo_hn_5050_shared` | mix | 5 | leftover | list | none | 47% | 47% | 29/30 | +15.75 | 0 | 0 | +5.45 | -5.40 | +9.50 | — | PASS | 67.743 |
| `combo_seh_333_split` | mix | 5 | leftover | list | none | 49% | 63% | 29/30 | +13.82 | 0 | 0 | +7.12 | -6.95 | +4.77 | — | PASS | 67.668 |
| `short_news_r_macd_h3` | short | 3 | leftover | list | none | 65% | 57% | 29/30 | +6.11 | 8 | 41 | +4.78 | -5.56 | +4.66 | +37.55 | PASS | 67.301 |
| `union_e_fresh_h3` | long | 3 | leftover | list | none | 47% | 63% | 29/30 | +14.46 | 21 | 95 | +9.47 | -7.62 | -14.27 | +7.74 | PASS | 67.006 |
| `combo_seh_502525_split` | mix | 5 | leftover | list | none | 47% | 67% | 29/30 | +7.87 | 0 | 0 | +7.33 | -6.76 | -0.19 | — | PASS | 66.704 |
| `combo_hn_3070_shared` | mix | 5 | leftover | list | none | 47% | 53% | 28/30 | +6.35 | 0 | 0 | +5.56 | -5.31 | -7.25 | — | PASS | 66.648 |
| `combo_eh_5050_shared` | mix | 5 | leftover | list | none | 43% | 53% | 29/30 | +27.24 | 0 | 0 | +8.18 | -6.91 | +24.54 | — | PASS | 66.631 |
| `union_ret_5_h1` | long | 1 | leftover | list | none | 50% | 43% | 29/30 | +10.02 | 31 | 73 | +6.13 | -5.86 | +20.74 | +36.33 | PASS | 66.495 |
| `combo_hj_5050_shared` | mix | 5 | leftover | list | none | 44% | 43% | 29/30 | +13.94 | 0 | 0 | +5.75 | -4.87 | +30.50 | — | PASS | 66.412 |
| `union_hot_n12_h1` | long | 1 | leftover | list | none | 51% | 47% | 29/30 | +11.04 | 45 | 102 | +5.73 | -5.95 | +12.90 | +34.72 | PASS | 66.373 |
| `combo_eh_7030_shared` | mix | 5 | leftover | list | none | 41% | 53% | 29/30 | +22.72 | 0 | 0 | +8.60 | -6.69 | +13.47 | — | PASS | 66.217 |
| `union_w_hot_candle_h1` | long | 1 | leftover | list | none | 50% | 43% | 29/30 | +12.88 | 30 | 64 | +5.23 | -5.62 | +21.10 | +36.45 | PASS | 66.041 |
| `union_clk_mom_break_peer_opp_h1` | long | 1 | leftover | list | none | 54% | 33% | 26/30 | +3.14 | 11 | 26 | +3.90 | -3.23 | -0.35 | +20.76 | PASS | 65.393 |
| `union_clk_nr7_mom_h1` | long | 1 | leftover | list | none | 52% | 40% | 25/30 | +13.74 | 3 | 11 | +5.24 | -3.69 | -3.22 | +27.44 | PASS | 65.333 |
| `combo_ehs_601525_shared` | mix | 5 | leftover | list | none | 46% | 53% | 28/30 | +14.11 | 0 | 0 | +7.22 | -6.75 | +62.08 | — | PASS | 65.3 |
| `combo_eh_3070_shared` | mix | 5 | leftover | list | none | 43% | 47% | 29/30 | +29.87 | 0 | 0 | +8.03 | -7.10 | +31.66 | — | PASS | 65.171 |
| `combo_ehs_702010_shared` | mix | 5 | leftover | list | none | 42% | 50% | 29/30 | +18.18 | 0 | 0 | +7.82 | -6.39 | +52.66 | — | PASS | 64.763 |
| `combo_her_5050_shared` | mix | 5 | leftover | list | none | 43% | 53% | 29/30 | +21.89 | 0 | 0 | +7.71 | -8.18 | +17.53 | — | PASS | 64.657 |
| `combo_se_5050_skip` | mix | 5 | leftover | list | none | 55% | 63% | 25/30 | +7.63 | 0 | 0 | +6.35 | -7.71 | -9.87 | — | PASS | 64.427 |
| `combo_ej_5050_shared` | mix | 5 | leftover | list | none | 45% | 53% | 26/30 | +6.02 | 0 | 0 | +6.15 | -5.52 | -2.33 | — | PASS | 63.876 |
| `union_hot_score_h3` | long | 3 | leftover | list | none | 43% | 63% | 29/30 | +14.05 | 58 | 175 | +11.72 | -8.19 | +9.48 | +392.57 | PASS | 63.698 |
| `union_w_hot_cond_h3` | long | 3 | leftover | list | none | 45% | 60% | 29/30 | +16.26 | 52 | 163 | +9.61 | -8.08 | +16.69 | +337.30 | PASS | 63.43 |
| `union_vol_ab_h1` | long | 1 | leftover | list | none | 51% | 37% | 26/30 | +7.25 | 19 | 47 | +5.60 | -4.40 | -7.05 | +53.27 | PASS | 63.384 |
| `union_ret_5_h3` | long | 3 | leftover | list | none | 45% | 57% | 29/30 | +17.59 | 53 | 176 | +9.39 | -7.91 | +0.55 | +97.62 | PASS | 63.376 |
| `union_news_g_conv_h1` | long | 1 | conviction | list | none | 50% | 43% | 26/30 | +3.53 | 1 | 21 | +4.07 | -4.97 | -33.13 | +13.54 | PASS | 63.155 |
| `oppset_h1` | long | 1 | leftover | list | none | 54% | 37% | 26/30 | +7.35 | 17 | 56 | +6.04 | -5.08 | -4.53 | +17.09 | PASS | 62.852 |
| `union_w_hot_candle_h3` | long | 3 | leftover | list | none | 45% | 60% | 29/30 | +15.15 | 49 | 163 | +9.35 | -8.17 | +1.61 | +319.57 | PASS | 62.826 |
| `union_oppset_h1` | long | 1 | leftover | list | none | 54% | 37% | 26/30 | +8.02 | 16 | 57 | +5.71 | -4.92 | -5.36 | +17.28 | PASS | 62.789 |
| `combo_hf_5050_shared` | mix | 5 | leftover | list | none | 44% | 53% | 29/30 | +6.26 | 0 | 0 | +7.17 | -6.39 | +4.88 | — | PASS | 62.472 |
| `combo_es_9010_shared` | mix | 5 | leftover | list | none | 46% | 63% | 25/30 | +10.23 | 0 | 0 | +7.10 | -6.64 | -11.27 | — | PASS | 62.012 |
| `combo_es_8020_shared` | mix | 5 | leftover | list | none | 48% | 57% | 25/30 | +8.47 | 0 | 0 | +6.72 | -7.13 | -10.70 | — | PASS | 60.859 |
| `union_news_pack_net2_h1` | long | 1 | leftover | list | none | 50% | 43% | 21/30 | +6.85 | 1 | 13 | +3.77 | -3.30 | -6.56 | +7.99 | PASS | 60.685 |
| `union_clk_hold_vs_sector_opp_h1` | long | 1 | leftover | list | none | 47% | 37% | 26/30 | +8.62 | 6 | 36 | +4.87 | -3.81 | +0.83 | +2.00 | PASS | 60.596 |
| `combo_se_3070_shared` | mix | 5 | leftover | list | none | 50% | 53% | 25/30 | +5.57 | 0 | 0 | +6.54 | -7.32 | -11.61 | — | PASS | 60.513 |
| `union_clk_fresh_cat_coil_opp_h1` | long | 1 | leftover | list | none | 49% | 30% | 25/30 | +3.88 | 18 | 32 | +4.54 | -3.83 | +4.76 | +2.25 | PASS | 60.091 |
| `union_join_vol_green_h1` | long | 1 | leftover | list | none | 46% | 40% | 24/30 | +1.58 | 11 | 45 | +4.92 | -4.24 | +4.38 | +14.99 | PASS | 59.919 |
| `union_break10_h3` | long | 3 | leftover | list | none | 47% | 53% | 25/30 | +13.38 | 44 | 142 | +8.78 | -6.52 | +5.82 | +49.44 | PASS | 59.467 |
| `union_news_pack_h1` | long | 1 | leftover | list | none | 49% | 47% | 21/30 | +4.43 | 2 | 16 | +3.56 | -3.78 | -7.68 | +6.91 | PASS | 59.006 |
| `combo_en_5050_shared` | mix | 5 | leftover | list | none | 43% | 47% | 25/30 | +3.15 | 0 | 0 | +5.90 | -6.21 | -16.38 | — | PASS | 58.694 |
| `combo_en_7030_shared` | mix | 5 | leftover | list | none | 41% | 47% | 25/30 | +6.52 | 0 | 0 | +6.13 | -6.12 | -11.68 | — | PASS | 58.606 |
| `union_clk_hold_vs_sector_h1` | long | 1 | leftover | list | none | 46% | 37% | 25/30 | +4.06 | 7 | 49 | +3.85 | -4.42 | -13.87 | -2.27 | PASS | 57.02 |
| `short_clk_ext_veto_opp_h3` | short | 3 | leftover | list | none | 53% | 47% | 26/30 | +1.95 | 12 | 54 | +7.03 | -6.12 | +3.16 | -20.56 | PASS | 56.986 |
| `union_e_fresh_h1` | long | 1 | leftover | list | none | 48% | 37% | 24/30 | +3.28 | 10 | 50 | +4.60 | -5.57 | -8.00 | +4.74 | PASS | 56.544 |
| `union_macd_up_h3` | long | 3 | leftover | list | none | 42% | 47% | 26/30 | +9.39 | 32 | 142 | +7.94 | -6.19 | -1.68 | +63.53 | PASS | 56.179 |
| `combo_eer_5050_shared` | mix | 5 | leftover | list | none | 40% | 47% | 28/30 | +7.83 | 0 | 0 | +8.81 | -8.96 | -11.31 | — | PASS | 56.003 |
| `combo_se_5050_split` | mix | 5 | leftover | list | none | 50% | 57% | 21/30 | +2.38 | 0 | 0 | +7.03 | -6.89 | -14.26 | — | PASS | 55.974 |
| `union_vol_ab_h3` | long | 3 | leftover | list | none | 47% | 37% | 27/30 | +4.57 | 29 | 108 | +8.73 | -6.75 | -9.16 | +81.51 | PASS | 55.44 |
| `combo_se_5050_shared` | mix | 5 | leftover | list | none | 53% | 60% | 16/30 | +2.22 | 0 | 0 | +6.32 | -7.59 | -13.75 | — | PASS | 54.391 |
| `combo_se_5050_weather` | mix | 5 | leftover | list | none | 53% | 60% | 16/30 | +2.22 | 0 | 0 | +6.32 | -7.59 | -13.75 | — | PASS | 54.391 |
| `combo_ef_7030_shared` | mix | 5 | leftover | list | none | 41% | 60% | 22/30 | +4.95 | 0 | 0 | +7.99 | -7.26 | -11.04 | — | PASS | 54.379 |
| `union_earn_react_h3` | long | 3 | leftover | list | none | 42% | 50% | 25/30 | +0.13 | 16 | 94 | +8.14 | -9.05 | -22.59 | -14.12 | PASS | 53.491 |
| `union_macd_hist_h1` | long | 1 | leftover | list | none | 47% | 40% | 20/30 | +1.58 | 13 | 74 | +4.14 | -4.06 | -3.24 | +3.37 | PASS | 52.748 |
| `combo_fh_7030_shared` | mix | 5 | leftover | list | none | 43% | 63% | 17/30 | +1.39 | 0 | 0 | +7.33 | -6.62 | -3.23 | — | PASS | 52.549 |
| `short_news_r_h1` | short | 1 | leftover | list | none | 53% | 40% | 16/30 | +0.08 | 7 | 27 | +3.29 | -3.81 | -9.92 | +4.61 | PASS | 52.327 |
| `short_macd_dn_h3` | short | 3 | leftover | list | none | 53% | 47% | 18/30 | +0.61 | 27 | 135 | +9.14 | -6.88 | +4.60 | -1.81 | PASS | 51.532 |
| `short_last_red_h1` | short | 1 | leftover | list | none | 50% | 33% | 19/30 | +1.07 | 13 | 75 | +4.68 | -4.27 | +1.07 | -2.93 | PASS | 51.227 |
| `union_candle_score_h3` | long | 3 | leftover | list | none | 46% | 50% | 21/30 | +1.69 | 30 | 154 | +6.08 | -6.23 | -13.37 | +43.18 | PASS | 50.851 |
| `union_join_vol_green_h3` | long | 3 | leftover | list | none | 46% | 40% | 21/30 | +6.62 | 23 | 97 | +7.67 | -6.87 | +3.69 | +58.93 | PASS | 49.243 |
| `combo_ef_5050_shared` | mix | 5 | leftover | list | none | 41% | 60% | 16/30 | +0.52 | 0 | 0 | +8.36 | -7.09 | -12.25 | — | PASS | 48.223 |
| `combo_fe_5050_shared` | mix | 5 | leftover | list | none | 41% | 60% | 16/30 | +0.52 | 0 | 0 | +8.36 | -7.09 | -12.25 | — | PASS | 48.223 |
| `combo_fse_333_shared` | mix | 5 | leftover | list | none | 47% | 53% | 15/30 | +0.50 | 0 | 0 | +6.42 | -7.22 | -11.34 | — | PASS | 47.525 |
| `union_vol_g_h3` | long | 3 | leftover | list | none | 47% | 50% | 21/30 | +2.24 | 31 | 140 | +7.88 | -13.27 | -10.06 | -14.17 | PASS | 47.276 |
| `union_white_both_n4_h2` | long | 2 | leftover | list | none | 50% | 50% | 13/30 | -0.43 | 9 | 18 | +7.73 | -5.32 | -2.88 | +20.82 | PASS | 46.104 |
| `union_flow_in_h5` | long | 5 | leftover | list | none | 52% | 40% | 16/30 | +0.34 | 8 | 46 | +4.08 | -8.06 | -15.10 | -2.74 | PASS | 43.936 |
| `union_news_pack_net3_h1` | long | 1 | leftover | list | none | 49% | 37% | 14/30 | +0.00 | 1 | 12 | +3.56 | -3.30 | -20.99 | +3.78 | PASS | 41.564 |
| `union_break10_h1` | long | 1 | leftover | list | none | 49% | 40% | 14/30 | +0.00 | 27 | 54 | +5.19 | -5.32 | -11.91 | +11.56 | PASS | 40.563 |
| `union_white_both_n4_h1` | long | 1 | leftover | list | none | 46% | 37% | 7/30 | -4.82 | 4 | 12 | +6.99 | -3.25 | -5.61 | +6.63 | PASS | 40.274 |
| `union_e_green_h3` | long | 3 | leftover | list | none | 41% | 33% | 20/30 | +0.43 | 5 | 49 | +5.47 | -8.76 | -11.46 | -18.73 | PASS | 40.212 |
| `union_news_pack_net2_h3` | long | 3 | leftover | list | none | 40% | 30% | 16/30 | +0.40 | 3 | 28 | +5.95 | -7.59 | -13.18 | -9.47 | PASS | 39.372 |
| `combo_sj_3070_shared` | mix | 5 | leftover | list | none | 52% | 57% | 10/30 | -7.19 | 0 | 0 | +4.94 | -4.79 | -2.35 | — | PASS | 39.37 |
| `union_clk_fresh_cat_coil_h1` | long | 1 | leftover | list | none | 46% | 37% | 13/30 | -1.07 | 8 | 42 | +3.60 | -3.46 | -11.87 | -4.91 | PASS | 38.533 |
| `combo_sj_5050_shared` | mix | 5 | leftover | list | none | 52% | 57% | 9/30 | -7.14 | 0 | 0 | +4.93 | -4.81 | -1.16 | — | PASS | 38.18 |
| `union_news_or_net4_conv_h1` | long | 1 | conviction | list | none | 46% | 40% | 12/30 | -1.05 | 1 | 16 | +3.78 | -4.48 | -37.65 | -7.44 | PASS | 38.001 |
| `union_white_both_n4_h5_s12` | long | 5 | leftover | list | none | 43% | 60% | 12/30 | -1.07 | 13 | 34 | +8.48 | -7.09 | +1.98 | +22.84 | PASS | 37.812 |
| `union_last_green_h1` | long | 1 | leftover | list | none | 44% | 40% | 9/30 | -1.91 | 16 | 50 | +4.77 | -3.48 | -7.15 | +7.64 | PASS | 37.54 |
| `union_macd_up_h1` | long | 1 | leftover | list | none | 45% | 40% | 11/30 | -0.51 | 16 | 59 | +5.12 | -4.52 | -13.82 | +22.07 | PASS | 36.951 |
| `combo_jse_333_shared` | mix | 5 | leftover | list | none | 50% | 53% | 7/30 | -3.48 | 0 | 0 | +5.77 | -5.77 | -2.95 | — | PASS | 36.54 |
| `union_h5_time` | long | 5 | leftover | time | none | 45% | 67% | 12/30 | -1.65 | 36 | 170 | +9.33 | -8.01 | -9.08 | +16.74 | PASS | 36.501 |
| `union_h5_topheavy` | long | 5 | topheavy | list | none | 45% | 60% | 14/30 | +0.00 | 36 | 170 | +8.99 | -7.98 | -15.43 | +16.74 | PASS | 36.118 |
| `flatten_live_h1_topheavy` | long | 1 | topheavy | list | none | 54% | 10% | 7/30 | +0.00 | 1 | 5 | +6.75 | -3.75 | -1.75 | +4.45 | PASS | 36.038 |
| `combo_se_7030_shared` | mix | 5 | leftover | list | none | 52% | 57% | 8/30 | -1.18 | 0 | 0 | +6.39 | -7.37 | -12.84 | — | PASS | 35.774 |
| `union_h5` | long | 5 | leftover | list | none | 45% | 63% | 12/30 | -1.79 | 36 | 170 | +9.26 | -7.90 | -9.08 | +16.74 | PASS | 35.632 |
| `union_h5_cut` | long | 5 | leftover | cut_loser | none | 45% | 63% | 12/30 | -1.79 | 36 | 170 | +9.26 | -7.90 | -9.08 | +16.74 | PASS | 35.632 |
| `union_h5_sizeup` | long | 5 | leftover | list | sizeup | 45% | 63% | 12/30 | -1.79 | 36 | 170 | +9.26 | -7.90 | -9.08 | +16.74 | PASS | 35.632 |
| `union_h5_trail` | long | 5 | leftover | trail | none | 45% | 63% | 12/30 | -1.79 | 36 | 170 | +9.26 | -7.90 | -9.08 | +16.74 | PASS | 35.632 |
| `union_h1_topheavy` | long | 1 | topheavy | list | none | 42% | 43% | 9/30 | -2.27 | 9 | 53 | +4.47 | -4.03 | -17.49 | -0.80 | PASS | 35.489 |
| `combo_p2s_5050_shared` | mix | 5 | leftover | list | none | 59% | 60% | 2/30 | -4.04 | 0 | 0 | +4.59 | -4.85 | -16.06 | — | PASS | 35.333 |
| `flatten_live_h1` | long | 1 | leftover | list | none | 54% | 10% | 7/30 | +0.00 | 1 | 5 | +6.31 | -4.05 | -1.72 | +4.45 | PASS | 34.997 |
| `flatten_live_h1_cut` | long | 1 | leftover | cut_loser | none | 54% | 10% | 7/30 | +0.00 | 1 | 5 | +6.31 | -4.05 | -1.72 | +4.45 | PASS | 34.997 |
| `flatten_live_h1_sboost` | long | 1 | leftover | list | both | 54% | 10% | 7/30 | +0.00 | 1 | 5 | +6.31 | -4.05 | -1.72 | +4.45 | PASS | 34.997 |
| `flatten_live_h1_sizeup` | long | 1 | leftover | list | sizeup | 54% | 10% | 7/30 | +0.00 | 1 | 5 | +6.31 | -4.05 | -1.72 | +4.45 | PASS | 34.997 |
| `flatten_live_h1_time` | long | 1 | leftover | time | none | 54% | 10% | 7/30 | +0.00 | 1 | 5 | +6.31 | -4.05 | -1.72 | +4.45 | PASS | 34.997 |
| `flatten_live_h1_trail` | long | 1 | leftover | trail | none | 54% | 10% | 7/30 | +0.00 | 1 | 5 | +6.31 | -4.05 | -1.72 | +4.45 | PASS | 34.997 |
| `flatten_live_h1_half` | long | 1 | half | list | none | 54% | 10% | 7/30 | +0.00 | 1 | 5 | +6.58 | -3.90 | -0.93 | +4.45 | PASS | 34.946 |
| `combo_ps_5050_shared` | mix | 5 | leftover | list | none | 59% | 60% | 2/30 | -6.25 | 0 | 0 | +4.46 | -5.03 | -16.64 | — | PASS | 34.711 |
| `combo_sj_7030_shared` | mix | 5 | leftover | list | none | 50% | 53% | 7/30 | -5.88 | 0 | 0 | +4.96 | -4.82 | +0.45 | — | PASS | 34.304 |
| `combo_ps_7030_shared` | mix | 5 | leftover | list | none | 59% | 57% | 2/30 | -3.83 | 0 | 0 | +4.42 | -5.09 | -13.67 | — | PASS | 34.255 |
| `combo_nj_5050_shared` | mix | 5 | leftover | list | none | 45% | 40% | 9/30 | -3.35 | 0 | 0 | +4.53 | -4.59 | -21.92 | — | PASS | 34.134 |
| `union_flow_in_h1` | long | 1 | leftover | list | none | 47% | 30% | 6/30 | -1.69 | 4 | 9 | +4.23 | -4.36 | +9.85 | +17.31 | PASS | 34.09 |
| `ohlc_hot_coil_h1` | long | 1 | leftover | list | none | 50% | 37% | 9/30 | -1.03 | 11 | 34 | +3.99 | -3.95 | -17.56 | -11.86 | PASS | 33.903 |
| `union_h5_sboost` | long | 5 | leftover | list | both | 45% | 63% | 11/30 | -2.10 | 36 | 170 | +8.62 | -8.47 | -10.97 | +16.74 | PASS | 33.899 |
| `combo_fes_403030_shared` | mix | 5 | leftover | list | none | 46% | 53% | 11/30 | -0.09 | 0 | 0 | +6.59 | -7.16 | -11.01 | — | PASS | 33.522 |
| `union_white_both_n4_h5` | long | 5 | leftover | list | none | 43% | 57% | 9/30 | -1.07 | 13 | 34 | +8.49 | -8.82 | +0.89 | +22.84 | PASS | 33.264 |
| `flatten_live_h1_rankw` | long | 1 | rank_w | list | none | 54% | 10% | 6/30 | +0.00 | 1 | 5 | +6.14 | -4.12 | -2.65 | +4.45 | PASS | 33.192 |
| `short_macd_dn_h1` | short | 1 | leftover | list | none | 55% | 30% | 5/30 | -2.95 | 12 | 65 | +5.63 | -4.12 | +0.70 | +6.85 | PASS | 32.956 |
| `flatten_h5_topheavy` | long | 5 | topheavy | list | none | 44% | 53% | 12/30 | -1.25 | 22 | 123 | +8.38 | -7.88 | -19.58 | +4.47 | PASS | 32.929 |
| `combo_fe1_5050_shared` | mix | 5 | leftover | list | none | 43% | 53% | 10/30 | -2.04 | 0 | 0 | +5.67 | -5.99 | -11.29 | — | PASS | 32.653 |
| `union_clk_mom_break_peer_h1` | long | 1 | leftover | list | none | 50% | 33% | 4/30 | -1.97 | 12 | 47 | +3.75 | -3.63 | -10.46 | +1.42 | PASS | 31.498 |
| `yday_gainer_h1` | long | 1 | leftover | list | none | 48% | 37% | 6/30 | -4.75 | 15 | 73 | +4.67 | -4.46 | -29.14 | -1.36 | PASS | 31.491 |
| `union_news_or_net4_rw_h1` | long | 1 | rank_w | list | none | 46% | 43% | 5/30 | -6.82 | 1 | 16 | +3.65 | -4.63 | -38.01 | -7.44 | PASS | 30.947 |
| `short_news_head_h3` | short | 3 | leftover | list | none | 61% | 57% | 2/30 | -4.24 | 10 | 54 | +5.56 | -5.86 | -15.05 | +2.90 | PASS | 30.795 |
| `flatten_white_yday_h5` | long | 5 | leftover | list | none | 50% | 37% | 12/30 | -1.75 | 5 | 25 | +9.10 | -10.47 | -6.71 | +1.97 | PASS | 30.558 |
| `union_white_yday_h1` | long | 1 | leftover | list | none | 47% | 33% | 3/30 | -7.02 | 3 | 16 | +4.43 | -3.62 | -14.08 | +1.95 | PASS | 30.401 |
| `short_rsi_ob_h1` | short | 1 | leftover | list | none | 49% | 37% | 6/30 | -1.71 | 17 | 67 | +4.15 | -4.84 | -2.48 | -20.71 | PASS | 30.224 |
| `combo_e1s_7030_shared` | mix | 5 | leftover | list | none | 54% | 53% | 1/30 | -5.14 | 0 | 0 | +4.81 | -5.89 | -20.35 | — | PASS | 29.971 |
| `combo_en_3070_shared` | mix | 5 | leftover | list | none | 43% | 40% | 5/30 | -1.50 | 0 | 0 | +5.93 | -6.11 | -21.34 | — | PASS | 29.665 |
| `combo_ef_3070_shared` | mix | 5 | leftover | list | none | 42% | 57% | 7/30 | -0.83 | 0 | 0 | +8.31 | -7.23 | -12.65 | — | PASS | 29.491 |
| `union_last_red_h3` | long | 3 | leftover | list | none | 39% | 50% | 10/30 | -0.77 | 25 | 141 | +8.46 | -6.99 | -30.51 | +0.15 | PASS | 29.409 |
| `combo_se1_5050_shared` | mix | 5 | leftover | list | none | 55% | 50% | 1/30 | -7.22 | 0 | 0 | +4.74 | -6.03 | -18.68 | — | PASS | 29.389 |
| `combo_ers_7030_shared` | mix | 5 | leftover | list | none | 47% | 47% | 6/30 | -1.57 | 0 | 0 | +6.41 | -7.76 | -18.11 | — | PASS | 29.223 |
| `union_h5_half` | long | 5 | half | list | none | 45% | 63% | 5/30 | -1.80 | 36 | 170 | +8.24 | -7.00 | -8.17 | +16.74 | PASS | 29.027 |
| `short_alarm_h1` | short | 1 | leftover | list | none | 51% | 20% | 5/30 | -0.60 | 7 | 52 | +4.23 | -3.36 | +4.50 | +3.57 | PASS | 28.829 |
| `combo_ner_5050_shared` | mix | 5 | leftover | list | none | 41% | 40% | 6/30 | -1.75 | 0 | 0 | +5.81 | -6.48 | -20.33 | — | PASS | 28.805 |
| `union_news_g_conv_h3` | long | 3 | conviction | list | none | 49% | 40% | 10/30 | -1.23 | 7 | 61 | +4.66 | -7.74 | -20.36 | +282.84 | PASS | 28.641 |
| `union_vol_green_h1` | long | 1 | leftover | list | none | 44% | 40% | 5/30 | -8.85 | 20 | 62 | +5.11 | -7.08 | -16.45 | -4.91 | PASS | 28.447 |
| `probable_h1` | long | 1 | leftover | list | none | 48% | 33% | 3/30 | -3.40 | 14 | 68 | +4.02 | -3.86 | -15.13 | -1.12 | PASS | 28.071 |
| `union_last_green_h5` | long | 5 | leftover | list | none | 45% | 57% | 8/30 | -4.67 | 43 | 176 | +8.87 | -10.73 | -8.84 | +44.35 | PASS | 27.987 |
| `combo_sf_3070_shared` | mix | 5 | leftover | list | none | 49% | 53% | 3/30 | -4.21 | 0 | 0 | +6.64 | -6.26 | -12.54 | — | PASS | 27.947 |
| `combo_snj_333_shared` | mix | 5 | leftover | list | none | 48% | 53% | 0/30 | -11.34 | 0 | 0 | +4.63 | -4.85 | -31.56 | — | PASS | 27.853 |
| `short_clk_neg_weak_fail_h3` | short | 3 | leftover | list | none | 52% | 40% | 8/30 | -1.45 | 23 | 128 | +6.35 | -6.45 | +0.44 | -21.45 | PASS | 27.84 |
| `flatten_h5` | long | 5 | leftover | list | none | 44% | 60% | 4/30 | -2.04 | 22 | 123 | +8.64 | -8.00 | -15.30 | +4.47 | PASS | 27.832 |
| `flatten_h5_cut` | long | 5 | leftover | cut_loser | none | 44% | 60% | 4/30 | -2.04 | 22 | 123 | +8.64 | -8.00 | -15.30 | +4.47 | PASS | 27.832 |
| `flatten_h5_sizeup` | long | 5 | leftover | list | sizeup | 44% | 60% | 4/30 | -2.04 | 22 | 123 | +8.64 | -8.00 | -15.30 | +4.47 | PASS | 27.832 |
| `flatten_h5_time` | long | 5 | leftover | time | none | 44% | 60% | 4/30 | -2.04 | 22 | 123 | +8.64 | -8.00 | -15.30 | +4.47 | PASS | 27.832 |
| `flatten_h5_trail` | long | 5 | leftover | trail | none | 44% | 60% | 4/30 | -2.04 | 22 | 123 | +8.64 | -8.00 | -15.30 | +4.47 | PASS | 27.832 |
| `union_vol_green_h3` | long | 3 | leftover | list | none | 48% | 37% | 12/30 | -0.14 | 36 | 123 | +6.53 | -13.92 | -10.11 | -2.67 | PASS | 27.357 |
| `combo_ne1_5050_shared` | mix | 5 | leftover | list | none | 48% | 37% | 1/30 | -3.63 | 0 | 0 | +4.18 | -5.12 | -24.31 | — | PASS | 27.333 |
| `union_h5_rankw` | long | 5 | rank_w | list | none | 45% | 57% | 6/30 | -1.73 | 36 | 170 | +8.29 | -8.27 | -14.41 | +16.74 | PASS | 27.234 |
| `combo_sn_7030_shared` | mix | 5 | leftover | list | none | 52% | 53% | 0/30 | -11.28 | 0 | 0 | +4.30 | -5.28 | -29.01 | — | PASS | 27.19 |
| `union_h5_exit_alarm` | long | 5 | leftover | list | none | 45% | 53% | 6/30 | -2.33 | 34 | 173 | +7.69 | -7.19 | -12.39 | +17.52 | PASS | 27.19 |
| `union_news_g_h1` | long | 1 | leftover | list | none | 47% | 40% | 0/30 | -5.17 | 6 | 42 | +4.22 | -4.57 | -31.14 | +8.62 | PASS | 27.168 |
| `combo_sn_5050_shared` | mix | 5 | leftover | list | none | 52% | 53% | 0/30 | -12.74 | 0 | 0 | +4.44 | -5.10 | -38.34 | — | PASS | 27.162 |
| `combo_nse_333_shared` | mix | 5 | leftover | list | none | 47% | 47% | 1/30 | -6.62 | 0 | 0 | +5.62 | -6.08 | -26.96 | — | PASS | 27.15 |
| `short_extended_h1` | short | 1 | leftover | list | none | 52% | 33% | 3/30 | -7.45 | 30 | 76 | +5.40 | -6.54 | -1.95 | -19.86 | PASS | 27.1 |
| `union_white_any_h2` | long | 2 | leftover | list | none | 41% | 47% | 5/30 | -6.84 | 11 | 45 | +6.40 | -5.96 | -10.31 | +6.04 | PASS | 26.87 |
| `union_news_or_h1` | long | 1 | leftover | list | none | 47% | 37% | 0/30 | -4.79 | 5 | 42 | +4.28 | -4.18 | -31.03 | +9.53 | PASS | 26.846 |
| `combo_sn_3070_shared` | mix | 5 | leftover | list | none | 51% | 53% | 0/30 | -14.02 | 0 | 0 | +4.53 | -4.98 | -45.09 | — | PASS | 26.838 |
| `combo_ser_5050_shared` | mix | 5 | leftover | list | none | 49% | 53% | 1/30 | -2.02 | 0 | 0 | +6.29 | -7.67 | -18.29 | — | PASS | 26.817 |
| `union_h1_rankw` | long | 1 | rank_w | list | none | 42% | 43% | 0/30 | -4.62 | 9 | 53 | +4.32 | -4.12 | -15.80 | -0.80 | PASS | 26.767 |
| `flatten_h5_s8` | long | 5 | leftover | list | none | 44% | 60% | 3/30 | -4.17 | 22 | 123 | +8.66 | -7.93 | -14.41 | +4.47 | PASS | 26.726 |
| `short_news_or_h3` | short | 3 | leftover | list | none | 59% | 50% | 0/30 | -4.88 | 13 | 61 | +4.84 | -6.33 | -12.53 | -1.39 | PASS | 26.649 |
| `flatten_h5_sboost` | long | 5 | leftover | list | both | 44% | 60% | 3/30 | -2.14 | 22 | 123 | +8.44 | -7.85 | -15.30 | +4.47 | PASS | 26.619 |
| `short_news_r_h3` | short | 3 | leftover | list | none | 58% | 50% | 0/30 | -4.92 | 13 | 61 | +4.93 | -6.33 | -12.53 | -1.35 | PASS | 26.399 |
| `combo_jf_5050_shared` | mix | 5 | leftover | list | none | 41% | 53% | 2/30 | -6.42 | 0 | 0 | +5.72 | -5.28 | -12.94 | — | PASS | 26.285 |
| `union_news_head_h1` | long | 1 | leftover | list | none | 49% | 30% | 0/30 | -5.06 | 7 | 30 | +4.47 | -4.40 | -31.67 | +13.09 | PASS | 26.232 |
| `union_join_g_h1` | long | 1 | leftover | list | none | 44% | 37% | 0/30 | -4.13 | 9 | 57 | +4.06 | -3.99 | -15.82 | -3.42 | PASS | 25.811 |
| `union_candle_score_h1` | long | 1 | leftover | list | none | 49% | 30% | 0/30 | -6.44 | 18 | 64 | +4.43 | -4.40 | -12.75 | +8.19 | PASS | 25.721 |
| `union_news_g_cond_h1` | long | 1 | leftover | list | none | 47% | 33% | 0/30 | -5.28 | 5 | 43 | +4.28 | -4.32 | -31.03 | +8.94 | PASS | 25.536 |
| `union_h1` | long | 1 | leftover | list | none | 42% | 37% | 0/30 | -6.14 | 9 | 53 | +4.36 | -4.04 | -16.04 | -0.80 | PASS | 25.468 |
| `union_h1_sizeup` | long | 1 | leftover | list | sizeup | 42% | 37% | 0/30 | -6.14 | 9 | 53 | +4.36 | -4.04 | -16.04 | -0.80 | PASS | 25.468 |
| `union_h1_trail` | long | 1 | leftover | trail | none | 42% | 37% | 0/30 | -6.14 | 9 | 53 | +4.36 | -4.04 | -16.04 | -0.80 | PASS | 25.468 |
| `union_join_present_h1` | long | 1 | leftover | list | none | 42% | 37% | 0/30 | -5.93 | 8 | 56 | +4.41 | -3.89 | -12.79 | -0.36 | PASS | 25.427 |
| `union_white_both_n4_h3` | long | 3 | leftover | list | none | 44% | 37% | 3/30 | -2.12 | 11 | 28 | +7.85 | -6.20 | -5.12 | +32.51 | PASS | 25.271 |
| `union_cond_h1` | long | 1 | leftover | list | none | 47% | 30% | 0/30 | -4.95 | 4 | 48 | +4.29 | -3.98 | -13.10 | -4.88 | PASS | 25.189 |
| `union_h1_time` | long | 1 | leftover | time | none | 42% | 33% | 0/30 | -6.30 | 9 | 53 | +4.18 | -3.74 | -16.11 | -0.80 | PASS | 24.91 |
| `union_h1_sboost` | long | 1 | leftover | list | both | 42% | 37% | 0/30 | -8.03 | 9 | 53 | +4.25 | -4.13 | -17.66 | -0.80 | PASS | 24.909 |
| `flatten_h1` | long | 1 | leftover | list | none | 41% | 37% | 0/30 | -7.52 | 3 | 37 | +4.24 | -3.76 | -17.77 | -9.51 | PASS | 24.9 |
| `flatten_h5_rankw` | long | 5 | rank_w | list | none | 44% | 50% | 4/30 | -2.23 | 22 | 123 | +8.25 | -8.17 | -18.50 | +4.47 | PASS | 24.871 |
| `union_candle_h3` | long | 3 | leftover | list | none | 44% | 33% | 8/30 | -0.51 | 31 | 140 | +6.12 | -6.87 | -17.96 | +24.03 | PASS | 24.85 |
| `union_h1_cut` | long | 1 | leftover | cut_loser | none | 42% | 33% | 0/30 | -6.22 | 9 | 53 | +4.36 | -4.00 | -16.11 | -0.80 | PASS | 24.825 |
| `union_h3_cut` | long | 3 | leftover | cut_loser | none | 42% | 50% | 0/30 | -8.48 | 21 | 122 | +8.30 | -5.84 | -19.79 | -4.92 | PASS | 24.462 |
| `union_h3_trail` | long | 3 | leftover | trail | none | 42% | 50% | 0/30 | -8.48 | 21 | 122 | +8.30 | -5.84 | -19.79 | -4.92 | PASS | 24.462 |
| `union_white_h1` | long | 1 | leftover | list | none | 35% | 33% | 3/30 | -5.79 | 4 | 32 | +6.14 | -4.21 | -13.79 | -8.86 | PASS | 24.326 |
| `union_h1_half` | long | 1 | half | list | none | 42% | 33% | 0/30 | -4.18 | 9 | 53 | +4.32 | -4.12 | -10.00 | -0.80 | PASS | 24.295 |
| `union_news_present_h1` | long | 1 | leftover | list | none | 42% | 33% | 0/30 | -5.93 | 8 | 54 | +4.27 | -3.90 | -12.79 | -2.20 | PASS | 24.261 |
| `union_last_green_h3` | long | 3 | leftover | list | none | 49% | 37% | 3/30 | -4.49 | 32 | 129 | +6.19 | -7.21 | -18.28 | +37.42 | PASS | 24.202 |
| `union_join_present_h3` | long | 3 | leftover | list | none | 40% | 47% | 1/30 | -6.39 | 21 | 126 | +8.62 | -5.67 | -21.95 | +0.57 | PASS | 24.115 |
| `union_news_pack_h3` | long | 3 | leftover | list | none | 40% | 30% | 11/30 | -0.15 | 5 | 32 | +5.43 | -8.52 | -14.78 | -16.54 | PASS | 24.086 |
| `union_h3` | long | 3 | leftover | list | none | 42% | 50% | 0/30 | -8.48 | 21 | 122 | +8.31 | -6.05 | -19.79 | -4.92 | PASS | 24.012 |
| `union_h3_sizeup` | long | 3 | leftover | list | sizeup | 42% | 50% | 0/30 | -8.48 | 21 | 122 | +8.31 | -6.05 | -19.79 | -4.92 | PASS | 24.012 |
| `probable_probable_ok_h1` | long | 1 | leftover | list | none | 45% | 30% | 0/30 | -6.17 | 9 | 34 | +3.85 | -3.71 | -12.77 | -9.22 | PASS | 23.974 |
| `union_h3_half` | long | 3 | half | list | none | 42% | 50% | 0/30 | -7.07 | 21 | 122 | +7.97 | -5.58 | -11.63 | -4.92 | PASS | 23.966 |
| `union_h3_time` | long | 3 | leftover | time | none | 42% | 50% | 0/30 | -8.48 | 21 | 122 | +7.59 | -5.94 | -20.00 | -4.92 | PASS | 23.824 |
| `union_ab_g_h1` | long | 1 | leftover | list | none | 40% | 33% | 0/30 | -4.58 | 7 | 44 | +3.73 | -3.56 | -16.19 | -11.54 | PASS | 23.786 |
| `union_earn_react_h1` | long | 1 | leftover | list | none | 47% | 27% | 4/30 | -1.77 | 8 | 54 | +4.53 | -6.80 | -10.71 | -3.08 | PASS | 23.698 |
| `yday_gainer_h5` | long | 5 | leftover | list | none | 46% | 43% | 6/30 | -1.29 | 48 | 184 | +9.59 | -10.11 | -15.25 | +30.33 | PASS | 23.613 |
| `union_candle_h1` | long | 1 | leftover | list | none | 43% | 33% | 0/30 | -8.37 | 11 | 59 | +4.33 | -4.42 | -9.00 | -3.26 | PASS | 23.55 |
| `short_alarm_h3` | short | 3 | leftover | list | none | 55% | 37% | 2/30 | -1.77 | 14 | 108 | +6.95 | -6.84 | +0.41 | -2.80 | PASS | 23.433 |
| `union_h3_sboost` | long | 3 | leftover | list | both | 42% | 50% | 0/30 | -9.24 | 21 | 122 | +8.10 | -6.28 | -19.37 | -4.92 | PASS | 23.352 |
| `union_h3_exit_red` | long | 3 | leftover | list | none | 46% | 37% | 3/30 | -6.93 | 32 | 129 | +6.39 | -6.54 | -19.65 | +22.17 | PASS | 23.344 |
| `combo_nf_5050_shared` | mix | 5 | leftover | list | none | 38% | 50% | 0/30 | -5.40 | 0 | 0 | +6.38 | -5.47 | -17.87 | — | PASS | 23.329 |
| `combo_fer_5050_shared` | mix | 5 | leftover | list | none | 39% | 53% | 3/30 | -3.11 | 0 | 0 | +7.95 | -7.88 | -14.34 | — | PASS | 23.282 |
| `union_e_green_h1` | long | 1 | leftover | list | none | 47% | 30% | 5/30 | -4.75 | 3 | 19 | +4.06 | -8.73 | -9.58 | -17.92 | PASS | 23.022 |
| `union_news_or_net4_h1` | long | 1 | leftover | list | none | 44% | 33% | 0/30 | -10.93 | 2 | 25 | +4.01 | -4.26 | -43.17 | -6.67 | PASS | 22.972 |
| `flatten_h3_time` | long | 3 | leftover | time | none | 42% | 40% | 2/30 | -12.51 | 13 | 85 | +6.57 | -5.83 | -22.54 | -10.41 | PASS | 22.896 |
| `union_h3_exit_alarm` | long | 3 | leftover | list | none | 41% | 47% | 1/30 | -6.17 | 21 | 126 | +6.68 | -5.88 | -22.68 | +1.45 | PASS | 22.499 |
| `union_flow_in_h3` | long | 3 | leftover | list | none | 49% | 33% | 3/30 | -2.41 | 7 | 34 | +3.22 | -6.12 | -12.71 | +15.53 | PASS | 22.486 |
| `union_h3_rankw` | long | 3 | rank_w | list | none | 42% | 47% | 0/30 | -8.89 | 21 | 122 | +8.26 | -6.27 | -21.79 | -4.92 | PASS | 22.472 |
| `flatten_h3_sboost` | long | 3 | leftover | list | both | 42% | 40% | 1/30 | -14.01 | 13 | 85 | +7.17 | -5.45 | -22.28 | -10.41 | PASS | 22.439 |
| `flatten_h3` | long | 3 | leftover | list | none | 42% | 40% | 1/30 | -14.01 | 13 | 85 | +7.40 | -5.63 | -22.28 | -10.41 | PASS | 22.436 |
| `flatten_h3_cut` | long | 3 | leftover | cut_loser | none | 42% | 40% | 1/30 | -14.01 | 13 | 85 | +7.40 | -5.63 | -22.28 | -10.41 | PASS | 22.436 |
| `flatten_h3_sizeup` | long | 3 | leftover | list | sizeup | 42% | 40% | 1/30 | -14.01 | 13 | 85 | +7.40 | -5.63 | -22.28 | -10.41 | PASS | 22.436 |
| `flatten_h3_trail` | long | 3 | leftover | trail | none | 42% | 40% | 1/30 | -14.01 | 13 | 85 | +7.40 | -5.63 | -22.28 | -10.41 | PASS | 22.436 |
| `flatten_h5_half` | long | 5 | half | list | none | 44% | 50% | 1/30 | -4.71 | 22 | 123 | +7.47 | -6.56 | -9.23 | +4.47 | PASS | 22.417 |
| `union_news_or_net2_h1` | long | 1 | leftover | list | none | 43% | 27% | 0/30 | -6.33 | 2 | 32 | +4.17 | -4.17 | -27.45 | -3.45 | PASS | 22.372 |
| `union_news_g_cam71_conv_h1` | long | 1 | conviction | list | none | 31% | 27% | 6/30 | -1.83 | 1 | 8 | +3.63 | -5.17 | -25.15 | -13.48 | PASS | 22.353 |
| `ohlc_hot_h1` | long | 1 | leftover | list | none | 44% | 37% | 0/30 | -4.45 | 17 | 61 | +3.50 | -4.98 | -20.95 | +6.95 | PASS | 22.247 |
| `union_white_h5` | long | 5 | leftover | list | none | 43% | 60% | 2/30 | -4.47 | 19 | 79 | +9.11 | -9.19 | -12.01 | +36.60 | PASS | 22.243 |
| `union_news_g_cam71_h1` | long | 1 | leftover | list | none | 34% | 33% | 4/30 | -9.11 | 1 | 9 | +3.65 | -4.90 | -23.10 | -11.63 | PASS | 22.239 |
| `flatten_h3_half` | long | 3 | half | list | none | 42% | 40% | 0/30 | -6.84 | 13 | 85 | +6.85 | -5.36 | -11.41 | -10.41 | PASS | 22.191 |
| `union_news_pack_net3_h3` | long | 3 | leftover | list | none | 37% | 23% | 11/30 | -2.87 | 3 | 26 | +6.18 | -7.59 | -25.11 | -17.00 | PASS | 22.031 |
| `yday_gainer_h3` | long | 3 | leftover | list | none | 46% | 40% | 2/30 | -4.07 | 33 | 146 | +8.39 | -7.69 | -17.12 | +49.33 | PASS | 21.979 |
| `union_h3_exit_news_r` | long | 3 | leftover | list | none | 41% | 43% | 0/30 | -7.11 | 20 | 127 | +8.34 | -5.90 | -23.83 | +1.29 | PASS | 21.944 |
| `union_coil_green_h1` | long | 1 | leftover | list | none | 41% | 30% | 0/30 | -7.01 | 14 | 50 | +3.39 | -4.12 | -13.64 | -12.21 | PASS | 21.734 |
| `flatten_h3_rankw` | long | 3 | rank_w | list | none | 42% | 37% | 1/30 | -9.26 | 13 | 85 | +7.22 | -5.83 | -22.88 | -10.41 | PASS | 21.633 |
| `union_white_coil_h1` | long | 1 | leftover | list | none | 39% | 30% | 0/30 | -3.47 | 4 | 24 | +4.96 | -4.02 | -21.24 | -4.35 | PASS | 21.605 |
| `combo_e1er_5050_shared` | mix | 5 | leftover | list | none | 44% | 17% | 8/30 | -1.25 | 0 | 0 | +4.51 | -7.18 | -8.00 | — | PASS | 21.522 |
| `union_h3_topheavy` | long | 3 | topheavy | list | none | 42% | 43% | 0/30 | -6.03 | 21 | 122 | +7.04 | -6.38 | -24.02 | -4.92 | PASS | 21.441 |
| `union_news_vol_h1` | long | 1 | leftover | list | none | 43% | 23% | 0/30 | -9.92 | 6 | 24 | +4.62 | -4.76 | -26.70 | +0.74 | PASS | 21.394 |
| `flatten_h3_topheavy` | long | 3 | topheavy | list | none | 42% | 37% | 2/30 | -9.22 | 13 | 85 | +5.74 | -5.86 | -25.54 | -10.41 | PASS | 21.313 |
| `short_last_red_h3` | short | 3 | leftover | list | none | 56% | 40% | 0/30 | -12.23 | 26 | 136 | +5.54 | -7.62 | -2.57 | -38.90 | PASS | 21.236 |
| `union_join_g_h3` | long | 3 | leftover | list | none | 41% | 33% | 2/30 | -6.83 | 23 | 127 | +7.32 | -5.72 | -23.75 | +0.61 | PASS | 21.018 |
| `ohlc_hot_h3` | long | 3 | leftover | list | none | 47% | 40% | 4/30 | -5.46 | 35 | 167 | +5.88 | -7.15 | -13.20 | +107.52 | PASS | 20.983 |
| `union_blue_h1` | long | 1 | leftover | list | none | 39% | 23% | 0/30 | -5.75 | 12 | 52 | +4.31 | -4.00 | -20.64 | -5.21 | PASS | 20.939 |
| `short_extended_h3` | short | 3 | leftover | list | none | 56% | 43% | 1/30 | -24.11 | 57 | 159 | +7.43 | -10.31 | -10.93 | -55.91 | PASS | 20.886 |
| `union_macd_hist_h3` | long | 3 | leftover | list | none | 41% | 40% | 5/30 | -4.93 | 27 | 155 | +4.75 | -5.60 | -11.79 | -17.17 | PASS | 20.84 |
| `union_vol_g_h1` | long | 1 | leftover | list | none | 45% | 33% | 0/30 | -12.81 | 17 | 73 | +5.33 | -7.15 | -17.83 | -4.96 | PASS | 20.642 |
| `union_news_present_h3` | long | 3 | leftover | list | none | 39% | 43% | 1/30 | -6.39 | 20 | 121 | +6.87 | -5.75 | -21.95 | -4.75 | PASS | 20.586 |
| `union_news_or_net5_h1` | long | 1 | leftover | list | none | 40% | 27% | 1/30 | -11.37 | 1 | 16 | +3.59 | -4.55 | -28.76 | -10.75 | PASS | 20.34 |
| `probable_h3` | long | 3 | leftover | list | none | 46% | 37% | 3/30 | -3.77 | 29 | 150 | +7.00 | -7.92 | -20.83 | +7.17 | PASS | 20.302 |
| `union_last_red_h1` | long | 1 | leftover | list | none | 48% | 27% | 0/30 | -12.51 | 14 | 75 | +3.56 | -5.18 | -23.71 | +1.25 | PASS | 20.174 |
| `flatten_vol_g_h3` | long | 3 | leftover | list | none | 48% | 23% | 6/30 | -16.09 | 6 | 28 | +8.96 | -8.94 | -30.75 | -19.12 | PASS | 20.106 |
| `flatten_live_h3` | long | 3 | leftover | list | none | 58% | 10% | 0/30 | -6.24 | 4 | 14 | +8.60 | -8.85 | -9.53 | +6.87 | PASS | 20.091 |
| `union_news_g_cam71_n2_h1` | long | 1 | topheavy | list | none | 34% | 27% | 4/30 | -10.72 | 1 | 6 | +3.31 | -5.12 | -24.05 | -13.89 | PASS | 20.043 |
| `union_news_or_net3_h1` | long | 1 | leftover | list | none | 43% | 27% | 0/30 | -15.51 | 2 | 29 | +4.02 | -4.49 | -42.71 | -9.17 | PASS | 20.031 |
| `union_news_or_net4_h3` | long | 3 | leftover | list | none | 42% | 33% | 2/30 | -17.44 | 9 | 52 | +6.76 | -5.63 | -25.85 | -11.70 | PASS | 19.771 |
| `union_vol_g_h5` | long | 5 | leftover | list | none | 44% | 43% | 4/30 | -4.30 | 40 | 166 | +11.68 | -10.37 | -19.36 | +4.97 | PASS | 19.751 |
| `union_white_any_h5` | long | 5 | leftover | list | none | 38% | 63% | 1/30 | -7.47 | 17 | 76 | +8.28 | -9.13 | -12.84 | -11.72 | PASS | 19.75 |
| `union_white_any_h1` | long | 1 | leftover | list | none | 34% | 27% | 0/30 | -8.82 | 4 | 29 | +6.50 | -4.28 | -14.85 | -10.91 | PASS | 19.377 |
| `short_clk_ext_veto_h3` | short | 3 | leftover | list | none | 48% | 40% | 0/30 | -13.05 | 33 | 116 | +7.25 | -7.57 | -4.86 | -97.41 | PASS | 19.331 |
| `union_white_yday_h3` | long | 3 | leftover | list | none | 43% | 43% | 2/30 | -12.70 | 7 | 48 | +5.46 | -7.98 | -11.45 | -11.78 | PASS | 19.199 |
| `union_blue_h3` | long | 3 | leftover | list | none | 39% | 40% | 2/30 | -11.56 | 23 | 115 | +6.32 | -6.27 | -24.54 | -3.24 | PASS | 18.969 |
| `union_cond_h3` | long | 3 | leftover | list | none | 41% | 47% | 0/30 | -7.30 | 17 | 134 | +5.59 | -6.72 | -14.01 | -15.16 | PASS | 18.645 |
| `union_news_vol_h3` | long | 3 | leftover | list | none | 46% | 43% | 0/30 | -11.67 | 11 | 60 | +5.03 | -6.11 | -32.83 | +139.18 | PASS | 18.574 |
| `union_news_or_net2_h3` | long | 3 | leftover | list | none | 41% | 40% | 1/30 | -10.81 | 10 | 77 | +5.94 | -6.19 | -24.19 | -9.69 | PASS | 18.254 |
| `ohlc_hot_h5` | long | 5 | leftover | list | none | 45% | 43% | 4/30 | -6.28 | 49 | 202 | +6.93 | -9.00 | -17.05 | +88.18 | PASS | 18.147 |
| `short_rsi_ob_h3` | short | 3 | leftover | list | none | 50% | 37% | 1/30 | -14.84 | 38 | 148 | +5.64 | -8.03 | -10.78 | -115.85 | PASS | 17.937 |
| `union_coil_off_h1` | long | 1 | leftover | list | none | 41% | 27% | 0/30 | -9.85 | 8 | 63 | +3.07 | -4.23 | -18.73 | -17.20 | PASS | 17.889 |
| `union_cond_n4_h3` | long | 3 | leftover | list | none | 42% | 40% | 1/30 | -6.09 | 9 | 66 | +5.10 | -8.13 | -16.55 | -7.67 | PASS | 17.874 |
| `union_macd_xup_h1` | long | 1 | leftover | list | none | 38% | 30% | 0/30 | -6.94 | 10 | 43 | +3.48 | -4.66 | -7.83 | -19.02 | PASS | 17.626 |
| `union_blue_vol_h1` | long | 1 | leftover | list | none | 41% | 27% | 0/30 | -13.53 | 10 | 56 | +5.29 | -6.97 | -19.02 | -5.26 | PASS | 17.174 |
| `union_blue_coil_h3` | long | 3 | leftover | list | none | 40% | 43% | 1/30 | -9.03 | 17 | 109 | +6.22 | -10.71 | -25.36 | -38.70 | PASS | 17.094 |
| `union_news_g_cam71_h3` | long | 3 | leftover | list | none | 42% | 30% | 2/30 | -15.32 | 2 | 21 | +6.12 | -7.87 | -31.06 | -9.70 | PASS | 16.855 |
| `probable_probable_ok_h3` | long | 3 | leftover | list | none | 50% | 30% | 0/30 | -5.37 | 23 | 85 | +5.98 | -7.34 | -29.87 | +1.46 | PASS | 16.535 |
| `union_news_or_net5_h3` | long | 3 | leftover | list | none | 41% | 37% | 1/30 | -17.47 | 5 | 38 | +5.00 | -6.26 | -23.33 | -14.84 | PASS | 16.211 |
| `short_clk_neg_weak_fail_opp_h3` | short | 3 | leftover | list | none | 50% | 40% | 0/30 | -7.81 | 11 | 60 | +5.12 | -6.19 | -5.80 | -53.45 | PASS | 16.208 |
| `union_rsi_os_h3` | long | 3 | leftover | list | none | 45% | 43% | 6/30 | -5.71 | 5 | 57 | +5.17 | -9.41 | -17.07 | -42.97 | PASS | 16.057 |
| `union_ab_g_h3` | long | 3 | leftover | list | none | 36% | 27% | 2/30 | -14.36 | 20 | 111 | +7.06 | -5.55 | -26.80 | -10.05 | PASS | 16.032 |
| `union_news_g_h3` | long | 3 | leftover | list | none | 47% | 27% | 1/30 | -14.83 | 21 | 104 | +4.88 | -6.90 | -30.21 | +177.31 | PASS | 15.948 |
| `flatten_live_h5` | long | 5 | leftover | list | none | 46% | 23% | 0/30 | -5.51 | 5 | 16 | +9.01 | -11.62 | -9.67 | +5.13 | PASS | 15.838 |
| `probable_h5` | long | 5 | leftover | list | none | 45% | 33% | 4/30 | -5.03 | 43 | 188 | +7.94 | -10.23 | -15.08 | -15.54 | PASS | 15.832 |
| `union_white_h3` | long | 3 | leftover | list | none | 38% | 40% | 3/30 | -14.10 | 12 | 66 | +6.74 | -7.50 | -20.43 | +12.04 | PASS | 15.817 |
| `union_news_or_net3_h3` | long | 3 | leftover | list | none | 39% | 33% | 1/30 | -18.32 | 9 | 62 | +5.98 | -5.98 | -27.08 | -21.14 | PASS | 15.601 |
| `union_blue_vol_h3` | long | 3 | leftover | list | none | 39% | 37% | 2/30 | -4.88 | 19 | 107 | +9.58 | -12.79 | -15.70 | -21.78 | PASS | 15.391 |
| `union_news_head_h3` | long | 3 | leftover | list | none | 47% | 20% | 1/30 | -14.17 | 20 | 87 | +5.57 | -6.34 | -31.78 | +184.18 | PASS | 15.139 |
| `union_rsi_h1` | long | 1 | leftover | list | none | 42% | 33% | 0/30 | -22.28 | 9 | 93 | +4.35 | -8.36 | -36.83 | -22.60 | PASS | 15.051 |
| `union_news_g_cond_h3` | long | 3 | leftover | list | none | 45% | 23% | 1/30 | -12.81 | 17 | 107 | +5.67 | -6.46 | -30.95 | +177.97 | PASS | 15.047 |
| `union_news_or_h3` | long | 3 | leftover | list | none | 45% | 23% | 1/30 | -12.81 | 17 | 107 | +5.48 | -6.44 | -30.95 | +179.67 | PASS | 15.034 |
| `union_blue_coil_h1` | long | 1 | leftover | list | none | 38% | 20% | 0/30 | -17.05 | 8 | 49 | +3.77 | -6.42 | -18.36 | -23.64 | PASS | 14.924 |
| `union_macd_xup_h3` | long | 3 | leftover | list | none | 35% | 33% | 5/30 | -5.06 | 18 | 91 | +5.58 | -8.06 | -6.70 | -55.44 | PASS | 14.815 |
| `overnight_mega_h1` | long | 1 | leftover | list | none | 30% | 10% | 0/30 | +0.00 | 0 | 5 | +5.65 | -5.19 | -8.21 | -15.28 | PASS | 14.724 |
| `overnight_mega_h2` | long | 2 | leftover | list | none | 48% | 7% | 1/30 | +0.00 | 0 | 12 | +3.54 | -6.97 | -4.65 | -8.07 | PASS | 13.976 |
| `union_rsi_os_h1` | long | 1 | leftover | list | none | 46% | 33% | 0/30 | -20.27 | 3 | 33 | +2.54 | -7.06 | -34.61 | -7.54 | PASS | 13.747 |
| `union_news_g_cam61_h3` | long | 3 | leftover | list | none | 43% | 23% | 1/30 | -17.48 | 3 | 32 | +4.82 | -6.90 | -26.42 | -22.84 | PASS | 13.624 |
| `union_white_any_h3` | long | 3 | leftover | list | none | 37% | 37% | 2/30 | -16.75 | 12 | 64 | +6.17 | -8.32 | -21.38 | -4.29 | PASS | 13.489 |
| `union_rsi_h3` | long | 3 | leftover | list | none | 42% | 37% | 0/30 | -9.26 | 19 | 166 | +7.58 | -8.58 | -20.61 | -34.58 | PASS | 13.45 |
| `union_coil_green_h3` | long | 3 | leftover | list | none | 39% | 27% | 1/30 | -15.02 | 28 | 138 | +5.64 | -7.22 | -25.91 | -21.07 | PASS | 13.317 |
| `union_news_g_cam61_h1` | long | 1 | leftover | list | none | 34% | 13% | 1/30 | -15.20 | 1 | 16 | +3.47 | -5.17 | -24.90 | -17.43 | PASS | 12.825 |
| `overnight_h3` | long | 3 | leftover | list | none | 43% | 33% | 0/30 | -11.15 | 17 | 101 | +6.06 | -8.92 | -17.00 | -40.60 | PASS | 12.402 |
| `union_white_coil_h3` | long | 3 | leftover | list | none | 37% | 33% | 0/30 | -16.57 | 9 | 50 | +6.16 | -7.87 | -26.85 | -22.27 | PASS | 12.065 |
| `union_news_g_h5` | long | 5 | leftover | list | none | 39% | 40% | 1/30 | -5.12 | 23 | 139 | +5.13 | -9.19 | -22.96 | +99.58 | PASS | 11.629 |
| `overnight_h5` | long | 5 | leftover | list | none | 42% | 43% | 0/30 | -12.36 | 21 | 114 | +6.79 | -12.55 | -22.19 | -37.95 | PASS | 11.571 |
| `coil_h3_exit_alarm` | long | 3 | leftover | list | none | 36% | 27% | 1/30 | -13.80 | 18 | 144 | +4.38 | -6.88 | -27.56 | -18.91 | PASS | 10.622 |
| `union_coil_off_h3` | long | 3 | leftover | list | none | 37% | 27% | 1/30 | -15.05 | 22 | 141 | +4.46 | -7.50 | -29.32 | -24.98 | PASS | 10.396 |
| `union_overnight_h3` | long | 3 | leftover | list | none | 42% | 27% | 0/30 | -11.15 | 15 | 94 | +5.70 | -9.19 | -17.00 | -45.53 | PASS | 10.071 |
| `overnight_h1` | long | 1 | leftover | list | none | 31% | 23% | 0/30 | -20.70 | 1 | 53 | +6.24 | -9.79 | -25.50 | -22.61 | PASS | 9.057 |
| `union_overnight_h1` | long | 1 | leftover | list | none | 31% | 20% | 0/30 | -20.70 | 1 | 49 | +6.47 | -9.43 | -25.70 | -26.59 | PASS | 8.544 |
| `union_coil_off_h5` | long | 5 | leftover | list | none | 37% | 40% | 0/30 | -11.65 | 32 | 179 | +3.73 | -44.74 | -21.84 | -38.99 | PASS | 8.204 |
| `union_rsi_os_h5` | long | 5 | leftover | list | none | 34% | 33% | 4/30 | -7.75 | 6 | 61 | +6.53 | -14.87 | -23.06 | -62.78 | PASS | 5.236 |
| `union_news_both_h1` *(thin)* | long | 1 | leftover | list | none | 80% | 10% | 21/30 | +7.29 | 0 | 0 | +3.69 | -0.73 | -3.46 | +4.21 | PASS | 57.594 |
| `union_news_g_cam91_n1_h1` *(thin)* | long | 1 | leftover | list | none | 33% | 7% | 27/30 | +12.33 | 0 | 0 | +7.21 | -3.84 | -22.39 | -2.85 | PASS | 38.39 |
| `union_flow_in_white_h1` *(thin)* | long | 1 | leftover | list | none | 58% | 17% | 15/30 | +0.05 | 0 | 2 | +3.42 | -2.21 | -3.32 | +10.92 | PASS | 34.797 |
| `union_news_both_h3` *(thin)* | long | 3 | leftover | list | none | 20% | 13% | 21/30 | +4.04 | 0 | 2 | +4.37 | -0.51 | -3.79 | +1.48 | PASS | 25.771 |
| `union_clk_nr7_mom_opp_h1` *(thin)* | long | 1 | leftover | list | none | 75% | 10% | 12/30 | +0.00 | 0 | 4 | +4.03 | -2.38 | +0.31 | +0.01 | PASS | 12.018 |
| `union_news_missing_h1` *(thin)* | long | 1 | leftover | list | none | 50% | 3% | 1/30 | +0.00 | 0 | 2 | +7.93 | -3.05 | +0.00 | +1.88 | PASS | 9.724 |
| `union_vol_missing_h1` *(thin)* | long | 1 | leftover | list | none | 50% | 3% | 1/30 | +0.00 | 0 | 2 | +7.93 | -3.05 | +0.00 | +1.88 | PASS | 9.724 |
| `short_news_pack_h3` *(thin)* | short | 3 | leftover | list | none | 47% | 27% | 9/30 | +0.00 | 3 | 7 | +3.39 | -7.55 | +7.76 | -6.15 | PASS | 8.406 |
| `union_news_missing_h3` *(thin)* | long | 3 | leftover | list | none | 50% | 10% | 1/30 | +0.00 | 1 | 5 | +11.98 | -3.98 | +0.00 | +5.59 | PASS | 7.745 |
| `union_vol_missing_h3` *(thin)* | long | 3 | leftover | list | none | 50% | 10% | 1/30 | +0.00 | 1 | 5 | +11.98 | -3.98 | +0.00 | +5.59 | PASS | 7.745 |
| `union_clk_flow_coil_h1` *(thin)* | long | 1 | leftover | list | none | 29% | 27% | 10/30 | -5.56 | 1 | 4 | +5.53 | -4.04 | -4.55 | -14.71 | PASS | 7.348 |
| `union_catal_present_h3` *(thin)* | long | 3 | leftover | list | none | 31% | 20% | 0/30 | -5.51 | 2 | 8 | +12.05 | -4.89 | -11.49 | -22.70 | PASS | -1.618 |
| `union_flow_in_white_h3` *(thin)* | long | 3 | leftover | list | none | 50% | 13% | 2/30 | +0.00 | 0 | 6 | +3.30 | -4.79 | -13.15 | +4.80 | PASS | -1.906 |
| `union_catal_present_h1` *(thin)* | long | 1 | leftover | list | none | 31% | 17% | 0/30 | -7.89 | 0 | 3 | +6.03 | -3.97 | -5.50 | -5.09 | PASS | -1.988 |
| `union_rsi_os_macd_h3` *(thin)* | long | 3 | leftover | list | none | 50% | 17% | 1/30 | -5.75 | 2 | 5 | +8.24 | -7.11 | +9.98 | +8.48 | PASS | -2.572 |
| `union_clk_earn_guide_react_h1` *(thin)* | long | 1 | leftover | list | none | 42% | 10% | 0/30 | -5.37 | 1 | 5 | +5.68 | -5.63 | -4.37 | +8.19 | PASS | -4.513 |
| `overnight_mega_green_h1` *(thin)* | long | 1 | leftover | list | none | 35% | 10% | 0/30 | +0.00 | 0 | 2 | +2.51 | -6.44 | +0.00 | -9.06 | PASS | -5.621 |
| `union_rsi_os_macd_h1` *(thin)* | long | 1 | leftover | list | none | 17% | 7% | 1/30 | -8.27 | 1 | 3 | +7.49 | -3.44 | +3.68 | -3.93 | PASS | -9.015 |
| `union_clk_insider_cash_stab_h3` *(thin)* | long | 3 | leftover | list | none | 29% | 3% | 0/30 | -2.96 | 0 | 3 | +2.33 | -3.18 | -8.36 | -5.54 | PASS | -13.718 |
| `short_r_down_h1` *(thin)* | short | 1 | leftover | list | none | — | 0% | 0/30 | +0.00 | 0 | 0 | — | — | +0.00 | +0.00 | PASS | -15.0 |
| `short_r_down_h3` *(thin)* | short | 3 | leftover | list | none | — | 0% | 0/30 | +0.00 | 0 | 0 | — | — | +0.00 | +0.00 | PASS | -15.0 |
| `union_clk_r_up_coil_h1` *(thin)* | long | 1 | leftover | list | none | — | 0% | 0/30 | +0.00 | 0 | 0 | — | — | +0.00 | +0.00 | PASS | -15.0 |
| `union_r_up_h1` *(thin)* | long | 1 | leftover | list | none | — | 0% | 0/30 | +0.00 | 0 | 0 | — | — | +0.00 | +0.00 | PASS | -15.0 |
| `union_r_up_h3` *(thin)* | long | 3 | leftover | list | none | — | 0% | 0/30 | +0.00 | 0 | 0 | — | — | +0.00 | +0.00 | PASS | -15.0 |
