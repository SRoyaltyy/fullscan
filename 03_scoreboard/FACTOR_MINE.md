# Factor strategy mine — 2026-08-13 → 2026-09-21

Leak-free 09:30 recipes: **339** · candidate rows **2306** · fill `09:30 open, whole shares, Futubull fees, leftover split, sell first, hard-red sit, cash+holdings audit`.

Cash book: $10k, whole shares, Futubull fees, leftover split, sell first, min-hold, 09:30 open, hard-red S≤−3 sit, shorts marked as a liability. Each session starts from leftover cash and lots actually held (butterfly). Cash-start buttons wake a sleeve on date X with $10k and no lots (same rules). Stock investigator quotes 09:30 cameras / coaches / news from repo files. Size / sell / S-boost tweaks sit on the same ledger. Signal-only % is the old equal-weight path (not a fill). `flatten_h*` = wish-list (io/HOLD mornings still buy). `flatten_live_*` = only when the live flatten gate fires. Research only — does not change live `flatten_robust`.

Scoreboard files: slim `factor_mine.json` (stats / recipes / series) plus gzip shards in `factor_mine/shards/` (`books`, `starts`, `daily`, `probe`, `sim`). One file used to be 104MB and GitHub rejected the publish (100MB cap).

Combination books: **79** mixes on the same $10k cash ledger (shared leftover or split sleeves, official 09:30 / 16:00, hard-red sit, owner min-hold). Outperformers: `combo_es_9010_shared`, `combo_seh_333_skip`, `combo_es_8020_shared`, `combo_sh_3070_shared`, `combo_se_5050_skip`, `combo_ehs_702010_shared`, `combo_ej_5050_shared`, `combo_sh_5050_shared`, `combo_eh_7030_shared`, `combo_sh_macd_5050_shared`, `combo_jer_5050_shared`, `combo_sj_3070_shared`, `combo_ers_7030_shared`, `combo_sj_5050_shared`, `combo_ser_5050_shared`, `combo_je1_5050_shared`, `combo_e1s_7030_shared`, `combo_sf_5050_shared`, `combo_sf_7030_shared`, `combo_sf_3070_shared`.

A combo outperforms its members on the cash book when the audit passes and either (1) Book% is strictly higher than every member, or (2) Book% stays within 2pp of the richest member, max drawdown is strictly smaller than every member's drawdown, start-rate is at least the best member, both halves are green, and the worst session is no worse than the worst member session. Effectiveness / Signal% do not decide WIN.

Action blotters: [FACTOR_MINE_ACTION.md](FACTOR_MINE_ACTION.md).

| Strategy | Side | H | Size | Sell | Boost | Win% | $ days | Starts YES | Med start | Top-g | Losers | AvgW | AvgL | Book% | Signal% | Audit | Eff |
|---|---|---:|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---:|
| `combo_sh_5050_shared` | mix | 5 | leftover | list | none | 60% | 70% | 27/27 | +12.87 | 0 | 0 | +5.98 | -5.65 | +38.80 | — | PASS | 77.247 |
| `combo_sh_3070_shared` | mix | 5 | leftover | list | none | 59% | 63% | 27/27 | +16.61 | 0 | 0 | +6.01 | -5.54 | +41.77 | — | PASS | 75.636 |
| `combo_sh_macd_5050_shared` | mix | 5 | leftover | list | none | 59% | 59% | 27/27 | +14.69 | 0 | 0 | +6.09 | -5.08 | +38.33 | — | PASS | 75.128 |
| `combo_sj_3070_shared` | mix | 5 | leftover | list | none | 56% | 63% | 27/27 | +4.91 | 0 | 0 | +4.90 | -4.81 | +27.10 | — | PASS | 71.708 |
| `combo_seh_333_skip` | mix | 5 | leftover | list | none | 51% | 67% | 27/27 | +14.48 | 0 | 0 | +6.75 | -6.82 | +42.20 | — | PASS | 71.47 |
| `combo_se_5050_skip` | mix | 5 | leftover | list | none | 55% | 67% | 26/27 | +12.22 | 0 | 0 | +6.22 | -7.88 | +41.28 | — | PASS | 68.754 |
| `combo_ej_5050_shared` | mix | 5 | leftover | list | none | 47% | 56% | 26/27 | +8.55 | 0 | 0 | +6.05 | -5.54 | +39.54 | — | PASS | 67.541 |
| `combo_je1_5050_shared` | mix | 5 | leftover | list | none | 51% | 48% | 26/27 | +6.85 | 0 | 0 | +4.54 | -4.85 | +19.80 | — | PASS | 66.838 |
| `combo_ehs_702010_shared` | mix | 5 | leftover | list | none | 43% | 52% | 27/27 | +16.78 | 0 | 0 | +7.61 | -6.13 | +39.82 | — | PASS | 66.246 |
| `combo_es_9010_shared` | mix | 5 | leftover | list | none | 45% | 67% | 26/27 | +15.45 | 0 | 0 | +7.13 | -6.72 | +44.06 | — | PASS | 66.033 |
| `combo_eh_7030_shared` | mix | 5 | leftover | list | none | 40% | 56% | 26/27 | +18.42 | 0 | 0 | +8.22 | -6.38 | +38.41 | — | PASS | 66.019 |
| `combo_es_8020_shared` | mix | 5 | leftover | list | none | 47% | 59% | 26/27 | +13.89 | 0 | 0 | +6.70 | -7.24 | +41.82 | — | PASS | 64.855 |
| `combo_sj_5050_shared` | mix | 5 | leftover | list | none | 56% | 63% | 19/27 | +2.35 | 0 | 0 | +4.89 | -4.83 | +21.52 | — | PASS | 63.443 |
| `combo_jer_5050_shared` | mix | 5 | leftover | list | none | 44% | 52% | 24/27 | +3.56 | 0 | 0 | +6.23 | -5.81 | +32.35 | — | PASS | 61.949 |
| `combo_e1s_7030_shared` | mix | 5 | leftover | list | none | 56% | 56% | 15/27 | +1.34 | 0 | 0 | +4.92 | -6.26 | +14.41 | — | PASS | 55.458 |
| `combo_ers_7030_shared` | mix | 5 | leftover | list | none | 46% | 44% | 20/27 | +1.22 | 0 | 0 | +6.36 | -7.90 | +23.76 | — | PASS | 52.462 |
| `combo_ser_5050_shared` | mix | 5 | leftover | list | none | 48% | 52% | 15/27 | +0.22 | 0 | 0 | +6.24 | -7.81 | +21.50 | — | PASS | 49.66 |
| `combo_sf_5050_shared` | mix | 5 | leftover | list | none | 54% | 59% | 8/27 | -4.08 | 0 | 0 | +6.64 | -6.73 | +10.21 | — | PASS | 36.496 |
| `combo_sf_7030_shared` | mix | 5 | leftover | list | none | 55% | 59% | 6/27 | -3.17 | 0 | 0 | +6.38 | -7.32 | +9.94 | — | PASS | 34.413 |
| `combo_sf_3070_shared` | mix | 5 | leftover | list | none | 50% | 56% | 3/27 | -4.74 | 0 | 0 | +7.02 | -6.60 | +7.88 | — | PASS | 29.696 |
| `combo_sh_7030_shared` | mix | 5 | leftover | list | none | 60% | 70% | 27/27 | +6.64 | 0 | 0 | +5.95 | -5.70 | +27.40 | — | PASS | 75.462 |
| `union_hot_n4_holdup` | long | 1 | leftover | list | holdup | 50% | 56% | 25/27 | +23.74 | 21 | 36 | +10.13 | -5.84 | +55.01 | +60.34 | PASS | 75.184 |
| `union_hot_n4_h1` | long | 1 | leftover | list | none | 50% | 44% | 26/27 | +18.66 | 21 | 36 | +7.11 | -5.09 | +36.04 | +60.34 | PASS | 71.604 |
| `combo_seh_333_shared` | mix | 5 | leftover | list | none | 49% | 67% | 27/27 | +11.33 | 0 | 0 | +6.80 | -6.74 | +34.87 | — | PASS | 69.9 |
| `combo_seh_333_weather` | mix | 5 | leftover | list | none | 49% | 67% | 27/27 | +11.33 | 0 | 0 | +6.80 | -6.74 | +34.87 | — | PASS | 69.9 |
| `combo_oh_5050_shared` | mix | 5 | leftover | list | none | 53% | 44% | 26/27 | +15.29 | 0 | 0 | +6.71 | -5.17 | +19.26 | — | PASS | 69.881 |
| `combo_seh_502525_shared` | mix | 5 | leftover | list | none | 48% | 70% | 27/27 | +7.67 | 0 | 0 | +6.87 | -6.68 | +30.60 | — | PASS | 69.842 |
| `combo_he1_5050_shared` | mix | 5 | leftover | list | none | 51% | 56% | 26/27 | +14.38 | 0 | 0 | +5.48 | -5.61 | +26.86 | — | PASS | 69.827 |
| `combo_seh_451540_shared` | mix | 5 | leftover | list | none | 48% | 70% | 27/27 | +10.86 | 0 | 0 | +6.90 | -6.93 | +32.28 | — | PASS | 69.776 |
| `combo_jse_333_shared` | mix | 5 | leftover | list | none | 52% | 59% | 27/27 | +4.69 | 0 | 0 | +5.70 | -5.89 | +34.89 | — | PASS | 69.123 |
| `combo_hj_5050_shared` | mix | 5 | leftover | list | none | 48% | 48% | 26/27 | +10.15 | 0 | 0 | +5.55 | -4.26 | +22.92 | — | PASS | 68.91 |
| `combo_seh_403525_shared` | mix | 5 | leftover | list | none | 48% | 63% | 27/27 | +9.13 | 0 | 0 | +6.92 | -6.60 | +33.39 | — | PASS | 68.883 |
| `combo_seh_601525_shared` | mix | 5 | leftover | list | none | 48% | 70% | 27/27 | +5.76 | 0 | 0 | +6.89 | -6.93 | +26.41 | — | PASS | 68.767 |
| `union_clk_mom_break_peer_opp_h1` | long | 1 | leftover | list | none | 52% | 41% | 26/27 | +4.58 | 10 | 26 | +3.95 | -3.20 | +16.86 | +17.93 | PASS | 68.568 |
| `combo_hn_7030_shared` | mix | 5 | leftover | list | none | 50% | 48% | 26/27 | +9.95 | 0 | 0 | +4.93 | -4.84 | +23.07 | — | PASS | 68.466 |
| `combo_hn_5050_shared` | mix | 5 | leftover | list | none | 51% | 48% | 26/27 | +6.43 | 0 | 0 | +5.00 | -4.72 | +18.23 | — | PASS | 68.437 |
| `union_white_both_n4_h2` | long | 2 | leftover | list | none | 52% | 52% | 23/27 | +3.69 | 9 | 18 | +7.78 | -5.36 | +24.18 | +14.70 | PASS | 68.344 |
| `combo_seh_333_split` | mix | 5 | leftover | list | none | 49% | 63% | 27/27 | +8.61 | 0 | 0 | +6.83 | -6.79 | +24.07 | — | PASS | 67.635 |
| `combo_seh_404020_shared` | mix | 5 | leftover | list | none | 49% | 56% | 27/27 | +8.39 | 0 | 0 | +6.75 | -6.69 | +33.10 | — | PASS | 67.539 |
| `combo_ehs_601525_shared` | mix | 5 | leftover | list | none | 47% | 56% | 27/27 | +12.55 | 0 | 0 | +6.98 | -6.52 | +37.37 | — | PASS | 67.485 |
| `combo_seh_502525_split` | mix | 5 | leftover | list | none | 48% | 67% | 27/27 | +5.93 | 0 | 0 | +7.16 | -6.59 | +17.92 | — | PASS | 67.443 |
| `union_w_hot_cond_h1` | long | 1 | leftover | list | none | 50% | 52% | 26/27 | +8.11 | 30 | 57 | +5.43 | -5.92 | +8.39 | +24.27 | PASS | 67.279 |
| `short_news_r_macd_h3` | short | 3 | leftover | list | none | 60% | 59% | 27/27 | +4.72 | 7 | 36 | +5.08 | -5.79 | +14.54 | +18.67 | PASS | 66.956 |
| `union_ret_5_h1` | long | 1 | leftover | list | none | 49% | 48% | 26/27 | +6.28 | 29 | 63 | +5.85 | -5.40 | +13.37 | +23.48 | PASS | 66.933 |
| `union_hot_score_h1` | long | 1 | leftover | list | none | 49% | 48% | 26/27 | +9.26 | 36 | 64 | +6.20 | -6.00 | +12.35 | +34.11 | PASS | 66.925 |
| `combo_ee1_3070_shared` | mix | 5 | leftover | list | none | 42% | 67% | 25/27 | +14.36 | 0 | 0 | +9.15 | -7.62 | +40.04 | — | PASS | 66.775 |
| `combo_ee1_5050_shared` | mix | 5 | leftover | list | none | 42% | 67% | 25/27 | +14.36 | 0 | 0 | +9.15 | -7.62 | +40.04 | — | PASS | 66.775 |
| `combo_ee1_7030_shared` | mix | 5 | leftover | list | none | 42% | 67% | 25/27 | +14.36 | 0 | 0 | +9.15 | -7.62 | +40.04 | — | PASS | 66.775 |
| `union_vol_ab_h1` | long | 1 | leftover | list | none | 54% | 37% | 26/27 | +7.54 | 17 | 41 | +5.09 | -4.33 | +11.20 | +64.76 | PASS | 66.467 |
| `combo_eh_5050_shared` | mix | 5 | leftover | list | none | 43% | 56% | 26/27 | +17.93 | 0 | 0 | +7.80 | -6.59 | +36.69 | — | PASS | 66.216 |
| `union_hot_n12_h1` | long | 1 | leftover | list | none | 52% | 48% | 26/27 | +7.27 | 42 | 90 | +5.04 | -5.76 | +5.79 | +26.05 | PASS | 66.106 |
| `union_e_fresh_h3` | long | 3 | leftover | list | none | 47% | 67% | 25/27 | +14.36 | 20 | 89 | +9.15 | -7.62 | +40.04 | +8.98 | PASS | 66.006 |
| `combo_se_5050_shared` | mix | 5 | leftover | list | none | 53% | 63% | 26/27 | +5.69 | 0 | 0 | +6.27 | -7.74 | +30.81 | — | PASS | 65.974 |
| `combo_se_5050_weather` | mix | 5 | leftover | list | none | 53% | 63% | 26/27 | +5.69 | 0 | 0 | +6.27 | -7.74 | +30.81 | — | PASS | 65.974 |
| `union_join_vol_green_h1` | long | 1 | leftover | list | none | 47% | 41% | 26/27 | +6.36 | 11 | 39 | +4.79 | -3.94 | +21.16 | +9.03 | PASS | 65.856 |
| `union_oppset_h1` | long | 1 | leftover | list | none | 54% | 37% | 26/27 | +8.29 | 16 | 56 | +5.71 | -4.90 | +8.29 | +18.00 | PASS | 65.641 |
| `oppset_h1` | long | 1 | leftover | list | none | 54% | 37% | 26/27 | +7.45 | 17 | 56 | +6.04 | -5.06 | +6.53 | +15.93 | PASS | 65.386 |
| `combo_eh_3070_shared` | mix | 5 | leftover | list | none | 43% | 52% | 26/27 | +16.05 | 0 | 0 | +7.65 | -6.79 | +34.69 | — | PASS | 65.032 |
| `union_candle_score_h1` | long | 1 | leftover | list | none | 51% | 33% | 27/27 | +2.54 | 18 | 51 | +4.59 | -4.05 | +5.37 | +10.86 | PASS | 65.031 |
| `combo_hn_3070_shared` | mix | 5 | leftover | list | none | 51% | 52% | 22/27 | +2.83 | 0 | 0 | +5.12 | -4.62 | +12.86 | — | PASS | 64.743 |
| `short_news_r_h1` | short | 1 | leftover | list | none | 52% | 41% | 27/27 | +3.06 | 5 | 21 | +3.32 | -3.95 | +2.27 | +4.91 | PASS | 64.457 |
| `union_news_pack_net2_h1` | long | 1 | leftover | list | none | 53% | 44% | 21/27 | +11.27 | 1 | 12 | +3.77 | -3.37 | +21.18 | +11.59 | PASS | 64.21 |
| `union_w_hot_candle_h1` | long | 1 | leftover | list | none | 50% | 44% | 25/27 | +5.62 | 29 | 59 | +5.09 | -5.24 | +4.92 | +25.07 | PASS | 64.18 |
| `combo_se_3070_shared` | mix | 5 | leftover | list | none | 50% | 52% | 26/27 | +9.89 | 0 | 0 | +6.51 | -7.45 | +37.56 | — | PASS | 63.82 |
| `union_clk_hold_vs_sector_opp_h1` | long | 1 | leftover | list | none | 46% | 41% | 26/27 | +9.13 | 6 | 36 | +5.16 | -3.82 | +12.44 | +1.07 | PASS | 63.561 |
| `combo_p2s_5050_shared` | mix | 5 | leftover | list | none | 61% | 67% | 17/27 | +3.63 | 0 | 0 | +4.64 | -5.14 | +14.13 | — | PASS | 63.095 |
| `union_clk_nr7_mom_h1` | long | 1 | leftover | list | none | 48% | 30% | 25/27 | +10.53 | 3 | 10 | +5.52 | -3.74 | +11.56 | +24.10 | PASS | 62.872 |
| `combo_se_5050_split` | mix | 5 | leftover | list | none | 51% | 56% | 26/27 | +5.78 | 0 | 0 | +6.94 | -7.12 | +20.82 | — | PASS | 62.759 |
| `combo_her_5050_shared` | mix | 5 | leftover | list | none | 42% | 56% | 26/27 | +12.50 | 0 | 0 | +7.32 | -7.93 | +25.84 | — | PASS | 62.666 |
| `union_break10_h1` | long | 1 | leftover | list | none | 50% | 44% | 23/27 | +5.47 | 26 | 46 | +5.08 | -4.99 | -0.12 | +12.06 | PASS | 62.63 |
| `union_clk_fresh_cat_coil_opp_h1` | long | 1 | leftover | list | none | 49% | 33% | 25/27 | +2.67 | 17 | 34 | +4.49 | -3.81 | +1.01 | +2.07 | PASS | 62.522 |
| `union_news_pack_h1` | long | 1 | leftover | list | none | 52% | 48% | 21/27 | +7.80 | 2 | 15 | +3.56 | -3.88 | +16.67 | +10.47 | PASS | 62.451 |
| `combo_ps_5050_shared` | mix | 5 | leftover | list | none | 61% | 67% | 17/27 | +1.72 | 0 | 0 | +4.52 | -5.32 | +12.28 | — | PASS | 62.422 |
| `combo_ps_7030_shared` | mix | 5 | leftover | list | none | 61% | 63% | 17/27 | +4.99 | 0 | 0 | +4.48 | -5.40 | +16.00 | — | PASS | 62.142 |
| `combo_en_5050_shared` | mix | 5 | leftover | list | none | 45% | 48% | 25/27 | +4.11 | 0 | 0 | +5.68 | -6.26 | +25.93 | — | PASS | 61.706 |
| `union_white_both_n4_h1` | long | 1 | leftover | list | none | 50% | 41% | 14/27 | +0.32 | 5 | 11 | +6.59 | -3.16 | +20.63 | +5.88 | PASS | 61.323 |
| `union_e_fresh_h1` | long | 1 | leftover | list | none | 49% | 41% | 25/27 | +4.92 | 9 | 45 | +4.69 | -5.92 | +14.05 | +7.59 | PASS | 61.16 |
| `yday_gainer_h1` | long | 1 | leftover | list | none | 51% | 41% | 21/27 | +2.53 | 14 | 64 | +4.80 | -3.91 | +9.08 | +13.27 | PASS | 60.325 |
| `union_ret_5_h3` | long | 3 | leftover | list | none | 46% | 56% | 24/27 | +10.95 | 46 | 156 | +9.99 | -8.21 | +25.16 | +50.62 | PASS | 60.272 |
| `combo_en_7030_shared` | mix | 5 | leftover | list | none | 41% | 44% | 25/27 | +6.85 | 0 | 0 | +6.04 | -6.10 | +29.80 | — | PASS | 60.223 |
| `short_clk_ext_veto_opp_h3` | short | 3 | leftover | list | none | 52% | 48% | 26/27 | +2.88 | 12 | 54 | +7.55 | -6.12 | +4.84 | -17.21 | PASS | 59.97 |
| `union_w_hot_candle_h3` | long | 3 | leftover | list | none | 45% | 56% | 25/27 | +7.56 | 44 | 148 | +9.42 | -8.81 | +15.40 | +205.15 | PASS | 59.42 |
| `union_hot_score_h3` | long | 3 | leftover | list | none | 44% | 56% | 24/27 | +4.17 | 52 | 156 | +12.54 | -8.74 | +13.17 | +254.38 | PASS | 59.054 |
| `union_vol_ab_h3` | long | 3 | leftover | list | none | 50% | 37% | 26/27 | +6.27 | 24 | 92 | +9.14 | -6.73 | +5.66 | +86.09 | PASS | 58.699 |
| `union_clk_mom_break_peer_h1` | long | 1 | leftover | list | none | 52% | 37% | 19/27 | +0.68 | 11 | 41 | +3.95 | -3.71 | +1.62 | +5.40 | PASS | 57.899 |
| `union_break10_h3` | long | 3 | leftover | list | none | 48% | 48% | 23/27 | +4.00 | 38 | 125 | +9.34 | -7.43 | +3.46 | +38.38 | PASS | 57.824 |
| `probable_h1` | long | 1 | leftover | list | none | 50% | 33% | 22/27 | +1.21 | 13 | 63 | +4.07 | -3.65 | +1.29 | +10.91 | PASS | 57.482 |
| `union_w_hot_cond_h3` | long | 3 | leftover | list | none | 44% | 56% | 23/27 | +6.96 | 46 | 150 | +10.35 | -8.30 | +12.92 | +207.91 | PASS | 57.476 |
| `combo_nj_5050_shared` | mix | 5 | leftover | list | none | 49% | 37% | 20/27 | +0.60 | 0 | 0 | +4.32 | -4.32 | +5.10 | — | PASS | 57.058 |
| `union_last_red_h3` | long | 3 | leftover | list | none | 40% | 56% | 25/27 | +5.61 | 19 | 128 | +8.60 | -6.74 | +7.45 | -0.59 | PASS | 56.636 |
| `combo_se_7030_shared` | mix | 5 | leftover | list | none | 52% | 59% | 18/27 | +1.79 | 0 | 0 | +6.35 | -7.52 | +24.99 | — | PASS | 56.595 |
| `union_last_green_h1` | long | 1 | leftover | list | none | 46% | 41% | 15/27 | +1.12 | 15 | 43 | +4.86 | -3.25 | +8.23 | +14.51 | PASS | 56.362 |
| `union_news_pack_net3_h1` | long | 1 | leftover | list | none | 52% | 37% | 16/27 | +3.24 | 1 | 11 | +3.56 | -3.37 | +12.40 | +7.24 | PASS | 56.189 |
| `combo_ef_7030_shared` | mix | 5 | leftover | list | none | 42% | 59% | 22/27 | +4.16 | 0 | 0 | +7.88 | -7.49 | +19.50 | — | PASS | 56.13 |
| `combo_sj_7030_shared` | mix | 5 | leftover | list | none | 53% | 59% | 14/27 | +0.78 | 0 | 0 | +4.93 | -4.80 | +13.00 | — | PASS | 55.961 |
| `union_clk_hold_vs_sector_h1` | long | 1 | leftover | list | none | 42% | 30% | 25/27 | +1.98 | 7 | 48 | +3.92 | -4.20 | +1.98 | -3.94 | PASS | 55.933 |
| `union_macd_up_h1` | long | 1 | leftover | list | none | 45% | 44% | 18/27 | +2.97 | 16 | 53 | +4.48 | -4.07 | +5.19 | +13.88 | PASS | 55.819 |
| `short_alarm_h3` | short | 3 | leftover | list | none | 54% | 37% | 27/27 | +1.42 | 13 | 95 | +6.77 | -9.42 | +3.64 | +8.48 | PASS | 55.616 |
| `combo_eer_5050_shared` | mix | 5 | leftover | list | none | 39% | 48% | 24/27 | +7.78 | 0 | 0 | +8.49 | -8.96 | +24.11 | — | PASS | 54.222 |
| `union_white_both_n4_h5_s12` | long | 5 | leftover | list | none | 43% | 59% | 16/27 | +1.77 | 13 | 33 | +9.82 | -7.09 | +22.12 | +17.06 | PASS | 53.614 |
| `union_candle_score_h3` | long | 3 | leftover | list | none | 45% | 44% | 23/27 | +2.43 | 26 | 139 | +6.66 | -6.35 | +7.18 | +24.90 | PASS | 53.408 |
| `union_earn_react_h3` | long | 3 | leftover | list | none | 41% | 52% | 21/27 | +0.44 | 15 | 88 | +8.14 | -9.05 | +21.00 | -13.13 | PASS | 51.75 |
| `union_join_vol_green_h3` | long | 3 | leftover | list | none | 51% | 37% | 20/27 | +5.14 | 20 | 82 | +8.18 | -7.44 | -2.34 | +36.90 | PASS | 51.642 |
| `combo_en_3070_shared` | mix | 5 | leftover | list | none | 45% | 41% | 16/27 | +0.40 | 0 | 0 | +5.72 | -6.13 | +20.76 | — | PASS | 51.068 |
| `union_white_both_n4_h5` | long | 5 | leftover | list | none | 43% | 52% | 16/27 | +0.08 | 13 | 33 | +9.82 | -8.82 | +19.79 | +17.06 | PASS | 50.427 |
| `union_macd_up_h3` | long | 3 | leftover | list | none | 42% | 44% | 17/27 | +2.83 | 28 | 126 | +8.48 | -6.11 | +2.83 | +20.04 | PASS | 48.847 |
| `flatten_white_yday_h5` | long | 5 | leftover | list | none | 53% | 41% | 17/27 | +0.12 | 4 | 22 | +9.10 | -10.47 | +3.27 | -9.28 | PASS | 48.097 |
| `combo_ef_5050_shared` | mix | 5 | leftover | list | none | 41% | 56% | 15/27 | +0.11 | 0 | 0 | +8.28 | -7.30 | +11.65 | — | PASS | 47.794 |
| `combo_fe_5050_shared` | mix | 5 | leftover | list | none | 41% | 56% | 15/27 | +0.11 | 0 | 0 | +8.28 | -7.30 | +11.65 | — | PASS | 47.794 |
| `combo_snj_333_shared` | mix | 5 | leftover | list | none | 52% | 59% | 12/27 | -0.55 | 0 | 0 | +4.52 | -4.78 | +9.96 | — | PASS | 43.906 |
| `combo_hf_5050_shared` | mix | 5 | leftover | list | none | 47% | 52% | 13/27 | -0.40 | 0 | 0 | +7.00 | -6.04 | +8.46 | — | PASS | 40.647 |
| `union_h1_topheavy` | long | 1 | topheavy | list | none | 45% | 44% | 11/27 | -0.12 | 8 | 48 | +4.55 | -3.78 | +8.17 | +8.59 | PASS | 40.393 |
| `short_news_head_h3` | short | 3 | leftover | list | none | 56% | 63% | 12/27 | -0.81 | 7 | 42 | +5.73 | -6.68 | +6.07 | -2.05 | PASS | 40.185 |
| `union_clk_fresh_cat_coil_h1` | long | 1 | leftover | list | none | 46% | 41% | 13/27 | -0.33 | 7 | 38 | +3.51 | -3.37 | -2.82 | -4.57 | PASS | 40.101 |
| `combo_e1er_5050_shared` | mix | 5 | leftover | list | none | 45% | 19% | 15/27 | +0.34 | 0 | 0 | +4.61 | -7.55 | -2.27 | — | PASS | 39.529 |
| `short_news_or_h3` | short | 3 | leftover | list | none | 54% | 56% | 13/27 | -0.81 | 10 | 49 | +4.94 | -7.03 | +4.62 | -5.67 | PASS | 39.045 |
| `short_news_r_h3` | short | 3 | leftover | list | none | 54% | 56% | 13/27 | -0.81 | 10 | 49 | +5.03 | -7.03 | +4.45 | -5.63 | PASS | 38.739 |
| `union_news_g_conv_h1` | long | 1 | conviction | list | none | 51% | 44% | 7/27 | -4.57 | 1 | 16 | +3.75 | -4.43 | +5.01 | +12.75 | PASS | 37.711 |
| `union_h1_rankw` | long | 1 | rank_w | list | none | 45% | 44% | 9/27 | -1.24 | 8 | 48 | +4.41 | -3.89 | +3.30 | +8.59 | PASS | 37.465 |
| `union_white_yday_h1` | long | 1 | leftover | list | none | 49% | 37% | 6/27 | -2.35 | 3 | 12 | +4.49 | -3.37 | +1.82 | +1.46 | PASS | 37.112 |
| `flatten_live_h1_topheavy` | long | 1 | topheavy | list | none | 54% | 11% | 7/27 | +0.00 | 1 | 5 | +6.75 | -3.75 | +7.19 | +4.45 | PASS | 36.91 |
| `union_flow_in_h1` | long | 1 | leftover | list | none | 43% | 33% | 9/27 | -2.01 | 4 | 9 | +4.34 | -4.11 | +7.33 | +9.35 | PASS | 36.807 |
| `union_h1` | long | 1 | leftover | list | none | 45% | 41% | 9/27 | -2.66 | 8 | 48 | +4.43 | -3.82 | +2.52 | +8.59 | PASS | 36.733 |
| `union_h1_sizeup` | long | 1 | leftover | list | sizeup | 45% | 41% | 9/27 | -2.66 | 8 | 48 | +4.43 | -3.82 | +2.52 | +8.59 | PASS | 36.733 |
| `union_h1_trail` | long | 1 | leftover | trail | none | 45% | 41% | 9/27 | -2.66 | 8 | 48 | +4.43 | -3.82 | +2.52 | +8.59 | PASS | 36.733 |
| `union_vol_green_h1` | long | 1 | leftover | list | none | 45% | 44% | 10/27 | -2.37 | 20 | 51 | +5.34 | -7.75 | +8.06 | -5.18 | PASS | 36.611 |
| `combo_fse_333_shared` | mix | 5 | leftover | list | none | 47% | 56% | 12/27 | -0.53 | 0 | 0 | +6.57 | -7.41 | +13.19 | — | PASS | 36.448 |
| `union_h1_time` | long | 1 | leftover | time | none | 45% | 37% | 9/27 | -2.74 | 8 | 48 | +4.34 | -3.49 | +2.01 | +8.59 | PASS | 36.338 |
| `combo_se1_5050_shared` | mix | 5 | leftover | list | none | 57% | 56% | 5/27 | -1.82 | 0 | 0 | +4.84 | -6.42 | +12.82 | — | PASS | 36.123 |
| `union_h1_cut` | long | 1 | leftover | cut_loser | none | 45% | 37% | 9/27 | -2.74 | 8 | 48 | +4.43 | -3.78 | +2.35 | +8.59 | PASS | 36.032 |
| `union_white_coil_h1` | long | 1 | leftover | list | none | 41% | 30% | 13/27 | -0.06 | 4 | 20 | +4.97 | -3.95 | -1.78 | -0.11 | PASS | 35.984 |
| `flatten_live_h1` | long | 1 | leftover | list | none | 54% | 11% | 7/27 | +0.00 | 1 | 5 | +6.31 | -4.05 | +8.22 | +4.45 | PASS | 35.869 |
| `flatten_live_h1_cut` | long | 1 | leftover | cut_loser | none | 54% | 11% | 7/27 | +0.00 | 1 | 5 | +6.31 | -4.05 | +8.22 | +4.45 | PASS | 35.869 |
| `flatten_live_h1_sboost` | long | 1 | leftover | list | both | 54% | 11% | 7/27 | +0.00 | 1 | 5 | +6.31 | -4.05 | +8.22 | +4.45 | PASS | 35.869 |
| `flatten_live_h1_sizeup` | long | 1 | leftover | list | sizeup | 54% | 11% | 7/27 | +0.00 | 1 | 5 | +6.31 | -4.05 | +8.22 | +4.45 | PASS | 35.869 |
| `flatten_live_h1_time` | long | 1 | leftover | time | none | 54% | 11% | 7/27 | +0.00 | 1 | 5 | +6.31 | -4.05 | +8.22 | +4.45 | PASS | 35.869 |
| `flatten_live_h1_trail` | long | 1 | leftover | trail | none | 54% | 11% | 7/27 | +0.00 | 1 | 5 | +6.31 | -4.05 | +8.22 | +4.45 | PASS | 35.869 |
| `flatten_live_h1_half` | long | 1 | half | list | none | 54% | 11% | 7/27 | +0.00 | 1 | 5 | +6.58 | -3.90 | +3.74 | +4.45 | PASS | 35.818 |
| `combo_fes_403030_shared` | mix | 5 | leftover | list | none | 45% | 56% | 12/27 | -0.89 | 0 | 0 | +6.76 | -7.34 | +11.03 | — | PASS | 35.783 |
| `combo_fh_7030_shared` | mix | 5 | leftover | list | none | 45% | 63% | 7/27 | -1.62 | 0 | 0 | +7.08 | -6.26 | +3.00 | — | PASS | 35.775 |
| `combo_nse_333_shared` | mix | 5 | leftover | list | none | 49% | 48% | 8/27 | -0.88 | 0 | 0 | +5.50 | -6.16 | +20.08 | — | PASS | 35.683 |
| `combo_sn_5050_shared` | mix | 5 | leftover | list | none | 56% | 63% | 3/27 | -4.18 | 0 | 0 | +4.35 | -5.10 | +0.64 | — | PASS | 35.289 |
| `union_join_present_h1` | long | 1 | leftover | list | none | 44% | 41% | 8/27 | -3.32 | 7 | 52 | +4.48 | -3.87 | +1.73 | +7.11 | PASS | 35.054 |
| `union_candle_h1` | long | 1 | leftover | list | none | 45% | 30% | 9/27 | -1.85 | 11 | 48 | +4.41 | -3.75 | +0.94 | +2.11 | PASS | 34.51 |
| `combo_sn_3070_shared` | mix | 5 | leftover | list | none | 55% | 59% | 3/27 | -4.63 | 0 | 0 | +4.44 | -4.96 | +0.75 | — | PASS | 34.293 |
| `ohlc_hot_coil_h1` | long | 1 | leftover | list | none | 49% | 41% | 7/27 | -1.68 | 12 | 29 | +4.54 | -3.94 | -18.75 | -9.22 | PASS | 34.292 |
| `short_last_red_h1` | short | 1 | leftover | list | none | 49% | 30% | 12/27 | -0.02 | 9 | 71 | +4.59 | -4.48 | -7.91 | -4.19 | PASS | 34.277 |
| `combo_sn_7030_shared` | mix | 5 | leftover | list | none | 56% | 59% | 3/27 | -3.77 | 0 | 0 | +4.26 | -5.21 | +0.42 | — | PASS | 34.201 |
| `flatten_live_h1_rankw` | long | 1 | rank_w | list | none | 54% | 11% | 6/27 | +0.00 | 1 | 5 | +6.14 | -4.12 | +4.03 | +4.45 | PASS | 33.969 |
| `ohlc_hot_h1` | long | 1 | leftover | list | none | 45% | 44% | 10/27 | -1.13 | 16 | 53 | +3.40 | -5.09 | -9.86 | -2.98 | PASS | 33.946 |
| `probable_probable_ok_h1` | long | 1 | leftover | list | none | 47% | 30% | 8/27 | -1.78 | 8 | 28 | +3.95 | -3.29 | -4.54 | +2.78 | PASS | 33.897 |
| `union_news_present_h1` | long | 1 | leftover | list | none | 44% | 37% | 8/27 | -3.32 | 7 | 50 | +4.35 | -3.88 | +0.18 | +5.14 | PASS | 33.838 |
| `combo_jf_5050_shared` | mix | 5 | leftover | list | none | 44% | 59% | 6/27 | -4.18 | 0 | 0 | +5.72 | -5.15 | +5.01 | — | PASS | 33.547 |
| `union_join_g_h1` | long | 1 | leftover | list | none | 46% | 37% | 7/27 | -2.28 | 9 | 53 | +4.12 | -4.00 | +0.94 | +2.35 | PASS | 33.171 |
| `combo_ne1_5050_shared` | mix | 5 | leftover | list | none | 51% | 37% | 5/27 | -2.13 | 0 | 0 | +4.10 | -5.14 | +7.01 | — | PASS | 32.808 |
| `union_h3_cut` | long | 3 | leftover | cut_loser | none | 44% | 52% | 5/27 | -4.91 | 19 | 111 | +9.02 | -5.30 | +0.30 | +10.32 | PASS | 32.378 |
| `union_h3_trail` | long | 3 | leftover | trail | none | 44% | 52% | 5/27 | -4.91 | 19 | 111 | +9.02 | -5.30 | +0.30 | +10.32 | PASS | 32.378 |
| `union_h5_time` | long | 5 | leftover | time | none | 46% | 67% | 7/27 | -4.92 | 31 | 151 | +8.94 | -8.07 | +1.79 | +19.55 | PASS | 32.316 |
| `union_blue_vol_h3` | long | 3 | leftover | list | none | 42% | 37% | 13/27 | -0.52 | 18 | 94 | +10.17 | -7.06 | -3.96 | +18.41 | PASS | 32.118 |
| `union_news_or_net4_rw_h1` | long | 1 | rank_w | list | none | 47% | 44% | 4/27 | -11.39 | 1 | 11 | +3.35 | -4.13 | -4.84 | -2.17 | PASS | 32.065 |
| `union_news_or_net4_conv_h1` | long | 1 | conviction | list | none | 47% | 41% | 4/27 | -9.02 | 1 | 11 | +3.31 | -4.18 | -0.36 | -2.17 | PASS | 31.897 |
| `union_h3_time` | long | 3 | leftover | time | none | 44% | 52% | 5/27 | -4.91 | 19 | 111 | +8.37 | -5.38 | +0.85 | +10.32 | PASS | 31.721 |
| `union_h1_sboost` | long | 1 | leftover | list | both | 45% | 41% | 4/27 | -4.11 | 8 | 48 | +4.31 | -3.92 | +1.08 | +8.59 | PASS | 31.583 |
| `flatten_h5_topheavy` | long | 5 | topheavy | list | none | 45% | 56% | 8/27 | -2.24 | 20 | 108 | +8.62 | -8.14 | +5.33 | +2.14 | PASS | 31.523 |
| `union_h5` | long | 5 | leftover | list | none | 46% | 63% | 7/27 | -4.92 | 31 | 151 | +8.84 | -8.09 | +1.72 | +19.55 | PASS | 31.488 |
| `union_h5_cut` | long | 5 | leftover | cut_loser | none | 46% | 63% | 7/27 | -4.92 | 31 | 151 | +8.84 | -8.09 | +1.72 | +19.55 | PASS | 31.488 |
| `union_h5_sizeup` | long | 5 | leftover | list | sizeup | 46% | 63% | 7/27 | -4.92 | 31 | 151 | +8.84 | -8.09 | +1.72 | +19.55 | PASS | 31.488 |
| `union_h5_trail` | long | 5 | leftover | trail | none | 46% | 63% | 7/27 | -4.92 | 31 | 151 | +8.84 | -8.09 | +1.72 | +19.55 | PASS | 31.488 |
| `union_news_head_h1` | long | 1 | leftover | list | none | 49% | 30% | 5/27 | -1.28 | 5 | 25 | +4.16 | -3.97 | -4.80 | +16.69 | PASS | 31.467 |
| `combo_fe1_5050_shared` | mix | 5 | leftover | list | none | 46% | 56% | 6/27 | -4.10 | 0 | 0 | +5.75 | -6.31 | +1.31 | — | PASS | 31.245 |
| `union_vol_g_h3` | long | 3 | leftover | list | none | 49% | 44% | 10/27 | -1.10 | 27 | 123 | +8.64 | -7.77 | -5.12 | +23.98 | PASS | 31.122 |
| `union_join_present_h3` | long | 3 | leftover | list | none | 43% | 48% | 5/27 | -4.78 | 18 | 113 | +9.76 | -5.54 | -0.91 | +7.99 | PASS | 30.94 |
| `union_h3` | long | 3 | leftover | list | none | 44% | 52% | 4/27 | -4.91 | 19 | 111 | +9.02 | -5.62 | -1.15 | +10.32 | PASS | 30.748 |
| `union_h3_sizeup` | long | 3 | leftover | list | sizeup | 44% | 52% | 4/27 | -4.91 | 19 | 111 | +9.02 | -5.62 | -1.15 | +10.32 | PASS | 30.748 |
| `union_news_g_h1` | long | 1 | leftover | list | none | 49% | 41% | 2/27 | -2.21 | 4 | 35 | +4.00 | -4.25 | +0.17 | +12.06 | PASS | 30.539 |
| `probable_h3` | long | 3 | leftover | list | none | 47% | 37% | 12/27 | -0.62 | 27 | 131 | +7.76 | -8.08 | -5.65 | +15.78 | PASS | 30.474 |
| `union_h5_topheavy` | long | 5 | topheavy | list | none | 46% | 59% | 7/27 | -3.55 | 31 | 151 | +8.60 | -8.27 | +1.27 | +19.55 | PASS | 30.421 |
| `combo_ner_5050_shared` | mix | 5 | leftover | list | none | 42% | 44% | 6/27 | -1.15 | 0 | 0 | +5.58 | -6.57 | +17.85 | — | PASS | 30.332 |
| `union_h3_half` | long | 3 | half | list | none | 44% | 52% | 3/27 | -5.46 | 19 | 111 | +8.99 | -5.05 | -4.44 | +10.32 | PASS | 30.194 |
| `union_news_or_h1` | long | 1 | leftover | list | none | 48% | 37% | 2/27 | -1.95 | 4 | 35 | +4.05 | -3.78 | -0.63 | +11.84 | PASS | 30.089 |
| `union_white_any_h2` | long | 2 | leftover | list | none | 44% | 48% | 5/27 | -2.94 | 11 | 41 | +6.41 | -5.72 | -1.24 | +6.88 | PASS | 30.066 |
| `union_h3_sboost` | long | 3 | leftover | list | both | 44% | 52% | 4/27 | -5.51 | 19 | 111 | +8.68 | -5.81 | -2.81 | +10.32 | PASS | 29.939 |
| `union_blue_h1` | long | 1 | leftover | list | none | 42% | 26% | 7/27 | -1.07 | 11 | 46 | +4.36 | -4.09 | -0.90 | -1.75 | PASS | 29.883 |
| `union_h5_sboost` | long | 5 | leftover | list | both | 46% | 63% | 6/27 | -4.89 | 31 | 151 | +8.36 | -8.56 | +0.84 | +19.55 | PASS | 29.849 |
| `combo_ef_3070_shared` | mix | 5 | leftover | list | none | 43% | 59% | 6/27 | -1.52 | 0 | 0 | +8.23 | -7.46 | +6.02 | — | PASS | 29.767 |
| `union_h5_exit_alarm` | long | 5 | leftover | list | none | 48% | 52% | 7/27 | -2.60 | 30 | 152 | +7.12 | -7.11 | +3.46 | +26.91 | PASS | 29.647 |
| `union_vol_green_h3` | long | 3 | leftover | list | none | 51% | 30% | 11/27 | -0.58 | 33 | 107 | +7.07 | -7.70 | -10.84 | +25.02 | PASS | 29.524 |
| `union_h3_exit_news_r` | long | 3 | leftover | list | none | 43% | 44% | 5/27 | -6.14 | 17 | 112 | +9.58 | -5.75 | -2.73 | +5.31 | PASS | 29.473 |
| `union_white_h1` | long | 1 | leftover | list | none | 37% | 37% | 6/27 | -5.78 | 4 | 28 | +5.55 | -4.21 | +0.21 | -8.32 | PASS | 29.157 |
| `union_last_green_h5` | long | 5 | leftover | list | none | 48% | 59% | 7/27 | -6.37 | 39 | 154 | +7.47 | -11.01 | -1.52 | +63.87 | PASS | 28.939 |
| `union_h3_exit_alarm` | long | 3 | leftover | list | none | 44% | 48% | 5/27 | -4.57 | 18 | 113 | +7.19 | -5.64 | -1.28 | +8.93 | PASS | 28.812 |
| `union_h3_rankw` | long | 3 | rank_w | list | none | 44% | 48% | 4/27 | -7.76 | 19 | 111 | +8.83 | -5.78 | -6.47 | +10.32 | PASS | 28.809 |
| `flatten_h5_s8` | long | 5 | leftover | list | none | 45% | 63% | 3/27 | -5.43 | 20 | 108 | +8.86 | -7.94 | +5.48 | +2.14 | PASS | 28.68 |
| `union_news_g_cond_h1` | long | 1 | leftover | list | none | 48% | 33% | 2/27 | -2.61 | 4 | 36 | +4.05 | -3.96 | -1.71 | +11.23 | PASS | 28.59 |
| `flatten_h5` | long | 5 | leftover | list | none | 45% | 63% | 3/27 | -2.75 | 20 | 108 | +8.90 | -8.31 | +6.20 | +2.14 | PASS | 28.567 |
| `flatten_h5_cut` | long | 5 | leftover | cut_loser | none | 45% | 63% | 3/27 | -2.75 | 20 | 108 | +8.90 | -8.31 | +6.20 | +2.14 | PASS | 28.567 |
| `flatten_h5_sizeup` | long | 5 | leftover | list | sizeup | 45% | 63% | 3/27 | -2.75 | 20 | 108 | +8.90 | -8.31 | +6.20 | +2.14 | PASS | 28.567 |
| `flatten_h5_time` | long | 5 | leftover | time | none | 45% | 63% | 3/27 | -2.75 | 20 | 108 | +8.90 | -8.31 | +6.20 | +2.14 | PASS | 28.567 |
| `flatten_h5_trail` | long | 5 | leftover | trail | none | 45% | 63% | 3/27 | -2.75 | 20 | 108 | +8.90 | -8.31 | +6.20 | +2.14 | PASS | 28.567 |
| `short_macd_dn_h1` | short | 1 | leftover | list | none | 51% | 26% | 2/27 | -5.16 | 12 | 58 | +6.18 | -4.40 | -8.78 | -3.36 | PASS | 28.504 |
| `union_h3_topheavy` | long | 3 | topheavy | list | none | 44% | 44% | 5/27 | -4.38 | 19 | 111 | +7.54 | -5.91 | -1.85 | +10.32 | PASS | 28.439 |
| `flatten_h5_sboost` | long | 5 | leftover | list | both | 45% | 63% | 3/27 | -2.75 | 20 | 108 | +8.68 | -8.12 | +3.76 | +2.14 | PASS | 28.181 |
| `union_h5_half` | long | 5 | half | list | none | 46% | 59% | 4/27 | -3.52 | 31 | 151 | +7.93 | -7.20 | +1.53 | +19.55 | PASS | 27.989 |
| `union_earn_react_h1` | long | 1 | leftover | list | none | 48% | 30% | 7/27 | -0.33 | 7 | 49 | +4.63 | -7.25 | +0.41 | -0.44 | PASS | 27.982 |
| `flatten_h1` | long | 1 | leftover | list | none | 42% | 37% | 1/27 | -6.84 | 3 | 32 | +4.26 | -3.42 | +0.03 | -5.88 | PASS | 27.926 |
| `yday_gainer_h3` | long | 3 | leftover | list | none | 49% | 41% | 5/27 | -1.23 | 29 | 126 | +9.12 | -7.48 | -5.63 | +35.64 | PASS | 27.422 |
| `union_news_g_cam71_h1` | long | 1 | leftover | list | none | 38% | 37% | 5/27 | -9.29 | 1 | 5 | +2.80 | -4.23 | -9.29 | -5.55 | PASS | 27.158 |
| `union_last_green_h3` | long | 3 | leftover | list | none | 51% | 37% | 4/27 | -5.46 | 29 | 113 | +6.68 | -7.48 | -2.22 | +39.09 | PASS | 27.138 |
| `union_h1_half` | long | 1 | half | list | none | 45% | 37% | 0/27 | -1.90 | 8 | 48 | +4.41 | -3.87 | -1.12 | +8.59 | PASS | 27.01 |
| `union_news_present_h3` | long | 3 | leftover | list | none | 43% | 44% | 5/27 | -4.78 | 17 | 108 | +7.50 | -5.56 | -8.66 | +2.28 | PASS | 26.916 |
| `union_macd_hist_h1` | long | 1 | leftover | list | none | 43% | 37% | 5/27 | -1.30 | 10 | 73 | +3.81 | -4.11 | -6.49 | -8.06 | PASS | 26.868 |
| `yday_gainer_h5` | long | 5 | leftover | list | none | 49% | 48% | 6/27 | -5.83 | 40 | 158 | +8.56 | -9.87 | -0.31 | +26.47 | PASS | 26.782 |
| `union_macd_xup_h1` | long | 1 | leftover | list | none | 37% | 30% | 8/27 | -1.16 | 10 | 32 | +3.47 | -4.12 | -12.38 | -15.52 | PASS | 26.63 |
| `combo_nf_5050_shared` | mix | 5 | leftover | list | none | 42% | 52% | 1/27 | -6.76 | 0 | 0 | +6.62 | -5.48 | +0.44 | — | PASS | 26.607 |
| `short_alarm_h1` | short | 1 | leftover | list | none | 52% | 19% | 3/27 | -1.04 | 7 | 48 | +4.04 | -3.63 | +0.10 | +4.59 | PASS | 26.577 |
| `union_h3_exit_red` | long | 3 | leftover | list | none | 49% | 37% | 4/27 | -5.67 | 29 | 113 | +6.74 | -6.59 | -6.00 | +24.54 | PASS | 26.484 |
| `union_h5_rankw` | long | 5 | rank_w | list | none | 46% | 59% | 4/27 | -6.46 | 31 | 151 | +7.69 | -8.54 | -1.91 | +19.55 | PASS | 26.458 |
| `flatten_h3_sboost` | long | 3 | leftover | list | both | 45% | 41% | 2/27 | -11.94 | 12 | 75 | +7.38 | -5.10 | -8.84 | +2.07 | PASS | 26.09 |
| `flatten_h3` | long | 3 | leftover | list | none | 45% | 41% | 2/27 | -11.94 | 12 | 75 | +7.64 | -5.32 | -8.79 | +2.07 | PASS | 26.048 |
| `flatten_h3_cut` | long | 3 | leftover | cut_loser | none | 45% | 41% | 2/27 | -11.94 | 12 | 75 | +7.64 | -5.32 | -8.79 | +2.07 | PASS | 26.048 |
| `flatten_h3_sizeup` | long | 3 | leftover | list | sizeup | 45% | 41% | 2/27 | -11.94 | 12 | 75 | +7.64 | -5.32 | -8.79 | +2.07 | PASS | 26.048 |
| `flatten_h3_trail` | long | 3 | leftover | trail | none | 45% | 41% | 2/27 | -11.94 | 12 | 75 | +7.64 | -5.32 | -8.79 | +2.07 | PASS | 26.048 |
| `union_blue_coil_h3` | long | 3 | leftover | list | none | 43% | 44% | 4/27 | -6.86 | 17 | 94 | +6.70 | -5.75 | -7.62 | -21.01 | PASS | 25.87 |
| `union_join_g_h3` | long | 3 | leftover | list | none | 44% | 33% | 5/27 | -7.68 | 21 | 112 | +7.87 | -5.76 | -6.03 | +1.71 | PASS | 25.798 |
| `union_blue_h3` | long | 3 | leftover | list | none | 43% | 41% | 5/27 | -6.49 | 21 | 98 | +7.02 | -6.19 | -10.70 | -2.62 | PASS | 25.753 |
| `flatten_h3_time` | long | 3 | leftover | time | none | 45% | 41% | 2/27 | -11.23 | 12 | 75 | +6.99 | -5.50 | -5.81 | +2.07 | PASS | 25.659 |
| `union_cond_h1` | long | 1 | leftover | list | none | 48% | 30% | 0/27 | -4.81 | 4 | 43 | +3.94 | -3.80 | -6.69 | -0.19 | PASS | 25.658 |
| `union_white_h5` | long | 5 | leftover | list | none | 45% | 59% | 3/27 | -7.73 | 19 | 73 | +9.83 | -8.67 | -1.16 | +45.07 | PASS | 25.492 |
| `union_flow_in_h3` | long | 3 | leftover | list | none | 47% | 33% | 6/27 | -1.42 | 7 | 34 | +3.27 | -6.00 | -0.82 | -1.38 | PASS | 25.47 |
| `flatten_h3_half` | long | 3 | half | list | none | 45% | 41% | 1/27 | -5.66 | 12 | 75 | +6.81 | -4.98 | -4.77 | +2.07 | PASS | 25.378 |
| `flatten_h5_rankw` | long | 5 | rank_w | list | none | 45% | 52% | 3/27 | -2.99 | 20 | 108 | +8.25 | -8.53 | +2.85 | +2.14 | PASS | 25.312 |
| `union_ab_g_h1` | long | 1 | leftover | list | none | 43% | 33% | 0/27 | -4.82 | 7 | 41 | +3.84 | -3.37 | -0.57 | -3.15 | PASS | 25.261 |
| `flatten_h3_rankw` | long | 3 | rank_w | list | none | 45% | 37% | 2/27 | -8.15 | 12 | 75 | +7.45 | -5.47 | -8.15 | +2.07 | PASS | 25.034 |
| `union_news_g_cam71_conv_h1` | long | 1 | conviction | list | none | 35% | 30% | 5/27 | -7.55 | 1 | 4 | +2.73 | -4.47 | -6.44 | -7.53 | PASS | 24.845 |
| `union_news_pack_net2_h3` | long | 3 | leftover | list | none | 40% | 30% | 10/27 | -0.13 | 3 | 27 | +5.95 | -7.59 | -11.73 | -9.16 | PASS | 24.802 |
| `union_news_g_cam71_n2_h1` | long | 1 | topheavy | list | none | 38% | 30% | 5/27 | -11.09 | 1 | 3 | +2.46 | -4.36 | -13.78 | -9.09 | PASS | 24.429 |
| `union_macd_xup_h3` | long | 3 | leftover | list | none | 36% | 33% | 12/27 | -0.54 | 19 | 72 | +6.08 | -8.29 | -17.65 | -52.32 | PASS | 24.193 |
| `union_news_or_net2_h1` | long | 1 | leftover | list | none | 45% | 30% | 0/27 | -3.85 | 1 | 27 | +4.08 | -3.93 | -4.21 | -0.25 | PASS | 24.186 |
| `union_e_green_h3` | long | 3 | leftover | list | none | 40% | 33% | 12/27 | +0.00 | 5 | 47 | +5.47 | -8.76 | -0.88 | -21.98 | PASS | 23.927 |
| `short_rsi_ob_h1` | short | 1 | leftover | list | none | 49% | 30% | 1/27 | -6.33 | 17 | 59 | +3.88 | -5.19 | -13.33 | -14.18 | PASS | 23.84 |
| `union_flow_in_h5` | long | 5 | leftover | list | none | 51% | 41% | 4/27 | -1.31 | 8 | 47 | +3.82 | -8.58 | +5.97 | -13.36 | PASS | 23.835 |
| `short_last_red_h3` | short | 3 | leftover | list | none | 56% | 41% | 2/27 | -6.10 | 20 | 123 | +5.13 | -7.13 | -11.81 | -15.97 | PASS | 23.809 |
| `union_news_vol_h1` | long | 1 | leftover | list | none | 44% | 26% | 0/27 | -4.04 | 5 | 21 | +4.23 | -4.29 | -2.45 | +14.64 | PASS | 23.697 |
| `flatten_h3_topheavy` | long | 3 | topheavy | list | none | 45% | 37% | 2/27 | -8.21 | 12 | 75 | +5.86 | -5.49 | -7.25 | +2.07 | PASS | 23.694 |
| `flatten_h5_half` | long | 5 | half | list | none | 45% | 52% | 1/27 | -5.29 | 20 | 108 | +7.81 | -7.00 | -0.66 | +2.14 | PASS | 23.679 |
| `union_e_green_h1` | long | 1 | leftover | list | none | 47% | 30% | 5/27 | -3.13 | 3 | 18 | +4.06 | -8.96 | -22.25 | -18.85 | PASS | 23.674 |
| `union_white_both_n4_h3` | long | 3 | leftover | list | none | 45% | 30% | 2/27 | -7.72 | 11 | 28 | +8.18 | -6.20 | -0.93 | +21.47 | PASS | 23.436 |
| `union_news_or_net4_h1` | long | 1 | leftover | list | none | 44% | 33% | 0/27 | -13.13 | 1 | 21 | +3.77 | -3.87 | -10.25 | -4.72 | PASS | 23.428 |
| `union_white_any_h5` | long | 5 | leftover | list | none | 40% | 59% | 3/27 | -9.59 | 17 | 70 | +9.10 | -8.55 | +0.66 | -6.25 | PASS | 23.29 |
| `union_blue_vol_h1` | long | 1 | leftover | list | none | 45% | 33% | 3/27 | -8.70 | 10 | 51 | +5.31 | -7.76 | -11.23 | +1.04 | PASS | 23.243 |
| `union_coil_green_h1` | long | 1 | leftover | list | none | 40% | 33% | 0/27 | -3.25 | 12 | 43 | +3.49 | -4.10 | -3.85 | -8.49 | PASS | 23.025 |
| `short_macd_dn_h3` | short | 3 | leftover | list | none | 49% | 44% | 2/27 | -3.11 | 26 | 119 | +4.45 | -6.46 | -9.76 | -18.97 | PASS | 22.766 |
| `short_extended_h1` | short | 1 | leftover | list | none | 52% | 26% | 0/27 | -11.43 | 30 | 64 | +4.98 | -6.64 | -18.44 | -14.86 | PASS | 22.709 |
| `union_cond_n4_h3` | long | 3 | leftover | list | none | 42% | 44% | 5/27 | -8.11 | 8 | 58 | +4.67 | -8.00 | -7.46 | -6.34 | PASS | 22.354 |
| `combo_fer_5050_shared` | mix | 5 | leftover | list | none | 39% | 48% | 3/27 | -3.44 | 0 | 0 | +7.84 | -8.14 | +6.58 | — | PASS | 22.324 |
| `probable_probable_ok_h3` | long | 3 | leftover | list | none | 51% | 26% | 4/27 | -1.94 | 21 | 68 | +6.65 | -7.29 | -20.01 | +14.89 | PASS | 21.931 |
| `union_vol_g_h1` | long | 1 | leftover | list | none | 48% | 33% | 0/27 | -11.03 | 15 | 65 | +5.58 | -7.83 | -10.55 | +3.90 | PASS | 21.855 |
| `short_rsi_ob_h3` | short | 3 | leftover | list | none | 52% | 37% | 2/27 | -5.81 | 33 | 131 | +5.60 | -7.91 | -11.40 | -125.83 | PASS | 21.692 |
| `short_clk_ext_veto_h3` | short | 3 | leftover | list | none | 51% | 44% | 0/27 | -8.30 | 30 | 108 | +7.64 | -7.88 | -12.21 | -96.25 | PASS | 21.657 |
| `short_extended_h3` | short | 3 | leftover | list | none | 54% | 41% | 0/27 | -13.02 | 51 | 136 | +8.34 | -10.02 | -16.63 | -36.93 | PASS | 21.498 |
| `union_last_red_h1` | long | 1 | leftover | list | none | 49% | 30% | 0/27 | -7.61 | 9 | 70 | +3.53 | -5.26 | -8.74 | +6.24 | PASS | 21.442 |
| `union_news_or_net5_h1` | long | 1 | leftover | list | none | 41% | 30% | 0/27 | -13.74 | 1 | 13 | +3.19 | -4.11 | -11.82 | -4.55 | PASS | 20.922 |
| `union_news_pack_h3` | long | 3 | leftover | list | none | 40% | 30% | 7/27 | -2.70 | 5 | 31 | +5.43 | -8.52 | -13.42 | -16.25 | PASS | 20.922 |
| `union_white_any_h1` | long | 1 | leftover | list | none | 36% | 30% | 0/27 | -9.02 | 4 | 25 | +5.63 | -4.37 | -9.73 | -10.39 | PASS | 20.586 |
| `union_cond_h3` | long | 3 | leftover | list | none | 42% | 44% | 2/27 | -9.23 | 15 | 121 | +5.96 | -6.82 | -8.39 | -13.95 | PASS | 20.541 |
| `union_news_vol_h3` | long | 3 | leftover | list | none | 47% | 44% | 1/27 | -10.16 | 9 | 54 | +4.25 | -5.14 | -11.47 | +189.34 | PASS | 20.385 |
| `short_clk_neg_weak_fail_h3` | short | 3 | leftover | list | none | 50% | 37% | 1/27 | -6.77 | 21 | 115 | +6.39 | -6.48 | -11.26 | -29.75 | PASS | 20.371 |
| `flatten_live_h3` | long | 3 | leftover | list | none | 58% | 11% | 0/27 | -6.24 | 4 | 14 | +8.60 | -8.85 | -6.24 | +6.87 | PASS | 20.313 |
| `union_white_yday_h3` | long | 3 | leftover | list | none | 43% | 41% | 2/27 | -10.85 | 6 | 45 | +5.68 | -7.05 | -9.90 | -9.09 | PASS | 20.041 |
| `ohlc_hot_h5` | long | 5 | leftover | list | none | 43% | 41% | 6/27 | -5.74 | 44 | 176 | +8.36 | -9.66 | -10.70 | +58.64 | PASS | 19.98 |
| `ohlc_hot_h3` | long | 3 | leftover | list | none | 45% | 44% | 2/27 | -3.29 | 31 | 148 | +5.72 | -7.53 | -3.29 | +77.45 | PASS | 19.936 |
| `union_news_or_net3_h1` | long | 1 | leftover | list | none | 43% | 26% | 0/27 | -14.69 | 1 | 25 | +3.74 | -4.17 | -16.09 | -7.86 | PASS | 19.917 |
| `union_news_g_conv_h3` | long | 3 | conviction | list | none | 47% | 37% | 2/27 | -12.40 | 6 | 52 | +4.65 | -7.55 | -13.03 | +215.31 | PASS | 19.681 |
| `union_candle_h3` | long | 3 | leftover | list | none | 46% | 26% | 2/27 | -7.49 | 27 | 118 | +6.68 | -7.14 | -10.14 | +13.10 | PASS | 19.317 |
| `union_white_h3` | long | 3 | leftover | list | none | 41% | 41% | 3/27 | -15.16 | 12 | 62 | +6.98 | -6.75 | -15.01 | +10.45 | PASS | 19.163 |
| `union_news_or_net4_h3` | long | 3 | leftover | list | none | 41% | 33% | 2/27 | -21.15 | 7 | 46 | +6.05 | -5.32 | -21.15 | -19.04 | PASS | 19.042 |
| `union_news_pack_net3_h3` | long | 3 | leftover | list | none | 37% | 22% | 7/27 | -4.45 | 3 | 25 | +6.18 | -7.59 | -15.53 | -16.71 | PASS | 18.533 |
| `probable_h5` | long | 5 | leftover | list | none | 48% | 33% | 5/27 | -8.05 | 37 | 162 | +6.61 | -10.49 | -15.18 | +1.28 | PASS | 18.302 |
| `union_news_g_cam61_h1` | long | 1 | leftover | list | none | 35% | 19% | 4/27 | -13.23 | 1 | 12 | +2.91 | -4.74 | -15.94 | -10.18 | PASS | 18.179 |
| `union_coil_off_h1` | long | 1 | leftover | list | none | 40% | 26% | 0/27 | -6.90 | 7 | 57 | +3.15 | -4.21 | -13.11 | -13.94 | PASS | 18.031 |
| `union_news_or_net2_h3` | long | 3 | leftover | list | none | 42% | 30% | 2/27 | -12.43 | 8 | 67 | +6.08 | -5.90 | -16.17 | -12.85 | PASS | 17.843 |
| `union_ab_g_h3` | long | 3 | leftover | list | none | 39% | 26% | 2/27 | -13.24 | 17 | 96 | +7.49 | -5.59 | -13.91 | -12.58 | PASS | 17.651 |
| `union_blue_coil_h1` | long | 1 | leftover | list | none | 41% | 22% | 0/27 | -11.60 | 8 | 43 | +3.88 | -6.90 | -14.09 | -18.01 | PASS | 17.595 |
| `union_vol_g_h5` | long | 5 | leftover | list | none | 47% | 41% | 0/27 | -4.41 | 35 | 145 | +12.33 | -10.84 | -12.03 | +8.22 | PASS | 17.531 |
| `union_rsi_h1` | long | 1 | leftover | list | none | 44% | 37% | 0/27 | -16.81 | 9 | 84 | +4.56 | -8.78 | -20.70 | -16.43 | PASS | 17.337 |
| `union_news_g_h3` | long | 3 | leftover | list | none | 48% | 26% | 2/27 | -14.76 | 18 | 90 | +4.60 | -6.54 | -22.24 | +165.48 | PASS | 17.221 |
| `short_clk_neg_weak_fail_opp_h3` | short | 3 | leftover | list | none | 49% | 41% | 1/27 | -6.95 | 11 | 61 | +5.21 | -6.18 | -25.00 | -53.88 | PASS | 17.185 |
| `union_coil_green_h3` | long | 3 | leftover | list | none | 40% | 30% | 2/27 | -8.06 | 26 | 121 | +5.83 | -6.96 | -6.04 | -12.68 | PASS | 16.964 |
| `union_clk_flow_coil_h1` | long | 1 | leftover | list | none | 20% | 26% | 0/27 | -8.70 | 1 | 4 | +5.65 | -3.36 | -10.57 | -18.42 | PASS | 16.771 |
| `union_rsi_os_h3` | long | 3 | leftover | list | none | 46% | 41% | 6/27 | -3.17 | 4 | 50 | +5.13 | -9.38 | -21.92 | -34.89 | PASS | 16.641 |
| `union_white_any_h3` | long | 3 | leftover | list | none | 41% | 37% | 2/27 | -15.16 | 12 | 60 | +6.38 | -7.50 | -12.94 | -5.65 | PASS | 16.63 |
| `flatten_live_h5` | long | 5 | leftover | list | none | 46% | 26% | 0/27 | -5.51 | 5 | 16 | +9.01 | -11.62 | -5.51 | +5.13 | PASS | 16.358 |
| `union_news_or_h3` | long | 3 | leftover | list | none | 45% | 22% | 2/27 | -14.65 | 15 | 93 | +5.55 | -5.94 | -18.83 | +164.98 | PASS | 16.32 |
| `union_news_g_cond_h3` | long | 3 | leftover | list | none | 45% | 22% | 2/27 | -14.65 | 15 | 93 | +5.55 | -6.11 | -19.51 | +163.37 | PASS | 16.083 |
| `union_news_head_h3` | long | 3 | leftover | list | none | 46% | 19% | 2/27 | -14.25 | 17 | 75 | +5.42 | -5.88 | -23.41 | +171.89 | PASS | 16.046 |
| `union_rsi_os_h1` | long | 1 | leftover | list | none | 49% | 33% | 0/27 | -14.22 | 3 | 28 | +2.45 | -7.06 | -26.62 | +2.03 | PASS | 15.922 |
| `union_news_g_cam71_h3` | long | 3 | leftover | list | none | 41% | 30% | 2/27 | -18.59 | 1 | 17 | +4.13 | -7.28 | -19.09 | -16.16 | PASS | 15.828 |
| `flatten_vol_g_h3` | long | 3 | leftover | list | none | 51% | 19% | 1/27 | -20.52 | 5 | 26 | +9.63 | -9.35 | -28.39 | -23.66 | PASS | 15.63 |
| `union_white_coil_h3` | long | 3 | leftover | list | none | 40% | 33% | 0/27 | -8.76 | 9 | 46 | +6.49 | -7.02 | -8.03 | -13.24 | PASS | 15.616 |
| `union_news_or_net5_h3` | long | 3 | leftover | list | none | 41% | 33% | 2/27 | -22.14 | 4 | 34 | +3.76 | -5.74 | -26.18 | -20.43 | PASS | 15.478 |
| `overnight_mega_h1` | long | 1 | leftover | list | none | 31% | 11% | 0/27 | +0.00 | 0 | 5 | +5.65 | -5.19 | -5.76 | -14.12 | PASS | 15.26 |
| `union_rsi_h3` | long | 3 | leftover | list | none | 44% | 37% | 0/27 | -6.64 | 18 | 152 | +7.91 | -8.40 | -13.16 | -19.23 | PASS | 15.096 |
| `union_news_or_net3_h3` | long | 3 | leftover | list | none | 39% | 26% | 2/27 | -21.42 | 7 | 54 | +5.92 | -5.99 | -23.68 | -27.52 | PASS | 15.008 |
| `overnight_h5` | long | 5 | leftover | list | none | 43% | 44% | 1/27 | -5.96 | 19 | 105 | +9.12 | -12.55 | -11.63 | -27.10 | PASS | 14.997 |
| `overnight_mega_h2` | long | 2 | leftover | list | none | 50% | 7% | 1/27 | +0.00 | 0 | 12 | +3.54 | -6.97 | -7.35 | -6.81 | PASS | 14.614 |
| `union_news_g_cam61_h3` | long | 3 | leftover | list | none | 44% | 19% | 2/27 | -20.07 | 2 | 27 | +4.04 | -6.36 | -23.02 | -26.66 | PASS | 13.88 |
| `union_macd_hist_h3` | long | 3 | leftover | list | none | 36% | 37% | 1/27 | -6.84 | 20 | 150 | +5.24 | -5.68 | -6.63 | -37.98 | PASS | 13.705 |
| `overnight_h3` | long | 3 | leftover | list | none | 45% | 33% | 0/27 | -12.05 | 15 | 92 | +6.08 | -8.89 | -15.18 | -29.20 | PASS | 13.274 |
| `union_news_g_h5` | long | 5 | leftover | list | none | 40% | 37% | 2/27 | -7.34 | 20 | 122 | +5.21 | -8.60 | -14.43 | +107.25 | PASS | 13.121 |
| `coil_h3_exit_alarm` | long | 3 | leftover | list | none | 37% | 26% | 2/27 | -10.47 | 15 | 125 | +4.51 | -7.13 | -10.07 | -12.62 | PASS | 12.71 |
| `union_coil_off_h3` | long | 3 | leftover | list | none | 37% | 26% | 2/27 | -11.92 | 20 | 124 | +4.86 | -8.26 | -15.14 | -21.01 | PASS | 12.187 |
| `union_coil_off_h5` | long | 5 | leftover | list | none | 40% | 41% | 2/27 | -13.21 | 27 | 157 | +3.69 | -54.08 | -9.03 | -31.66 | PASS | 11.772 |
| `overnight_h1` | long | 1 | leftover | list | none | 32% | 26% | 0/27 | -11.08 | 1 | 48 | +6.32 | -9.38 | -16.15 | -19.44 | PASS | 11.515 |
| `union_overnight_h1` | long | 1 | leftover | list | none | 32% | 22% | 0/27 | -11.08 | 1 | 44 | +6.57 | -8.93 | -18.96 | -23.59 | PASS | 11.066 |
| `union_overnight_h3` | long | 3 | leftover | list | none | 44% | 26% | 0/27 | -12.26 | 13 | 86 | +5.70 | -9.19 | -16.88 | -35.08 | PASS | 10.718 |
| `union_rsi_os_h5` | long | 5 | leftover | list | none | 37% | 33% | 5/27 | -2.96 | 4 | 53 | +4.64 | -14.37 | -28.26 | -52.22 | PASS | 7.403 |
| `union_news_both_h1` *(thin)* | long | 1 | leftover | list | none | 80% | 11% | 21/27 | +7.29 | 0 | 0 | +3.69 | -0.73 | +7.29 | +4.21 | PASS | 59.761 |
| `union_rsi_os_macd_h3` *(thin)* | long | 3 | leftover | list | none | 50% | 15% | 24/27 | +3.06 | 2 | 4 | +8.24 | -5.88 | +3.06 | +18.13 | PASS | 34.31 |
| `union_news_both_h3` *(thin)* | long | 3 | leftover | list | none | 20% | 15% | 21/27 | +4.04 | 0 | 2 | +4.37 | -0.51 | +4.04 | +1.48 | PASS | 28.012 |
| `union_catal_present_h1` *(thin)* | long | 1 | leftover | list | none | 30% | 15% | 10/27 | -5.37 | 0 | 2 | +8.76 | -2.82 | +1.03 | -2.59 | PASS | 15.377 |
| `union_clk_nr7_mom_opp_h1` *(thin)* | long | 1 | leftover | list | none | 75% | 11% | 12/27 | +0.00 | 0 | 4 | +4.03 | -2.38 | +10.45 | +0.01 | PASS | 13.35 |
| `union_flow_in_white_h1` *(thin)* | long | 1 | leftover | list | none | 54% | 19% | 2/27 | -2.57 | 0 | 3 | +3.42 | -2.21 | +5.23 | +8.08 | PASS | 11.02 |
| `union_news_missing_h1` *(thin)* | long | 1 | leftover | list | none | 50% | 4% | 1/27 | +0.00 | 0 | 2 | +7.93 | -3.05 | +1.44 | +1.88 | PASS | 9.891 |
| `union_vol_missing_h1` *(thin)* | long | 1 | leftover | list | none | 50% | 4% | 1/27 | +0.00 | 0 | 2 | +7.93 | -3.05 | +1.44 | +1.88 | PASS | 9.891 |
| `short_news_pack_h3` *(thin)* | short | 3 | leftover | list | none | 47% | 30% | 9/27 | +0.00 | 3 | 7 | +3.39 | -7.55 | +0.61 | -6.15 | PASS | 9.83 |
| `union_news_missing_h3` *(thin)* | long | 3 | leftover | list | none | 50% | 11% | 1/27 | +0.00 | 1 | 5 | +11.98 | -3.98 | +3.58 | +5.59 | PASS | 8.059 |
| `union_vol_missing_h3` *(thin)* | long | 3 | leftover | list | none | 50% | 11% | 1/27 | +0.00 | 1 | 5 | +11.98 | -3.98 | +3.58 | +5.59 | PASS | 8.059 |
| `union_news_g_cam91_n1_h1` *(thin)* | long | 1 | leftover | list | none | 50% | 4% | 0/27 | -2.85 | 0 | 0 | +0.85 | -3.84 | -2.85 | -0.09 | PASS | 1.417 |
| `union_catal_present_h3` *(thin)* | long | 3 | leftover | list | none | 30% | 19% | 0/27 | -4.69 | 2 | 6 | +12.05 | -4.43 | -0.74 | -12.69 | PASS | 0.193 |
| `union_clk_earn_guide_react_h1` *(thin)* | long | 1 | leftover | list | none | 45% | 7% | 0/27 | -3.11 | 1 | 4 | +5.68 | -6.17 | -5.43 | +10.90 | PASS | -2.451 |
| `union_flow_in_white_h3` *(thin)* | long | 3 | leftover | list | none | 46% | 15% | 1/27 | -2.72 | 0 | 7 | +3.30 | -4.79 | -0.57 | +2.11 | PASS | -5.064 |
| `overnight_mega_green_h1` *(thin)* | long | 1 | leftover | list | none | 35% | 11% | 0/27 | +0.00 | 0 | 2 | +2.51 | -6.44 | -8.87 | -9.06 | PASS | -5.399 |
| `union_rsi_os_macd_h1` *(thin)* | long | 1 | leftover | list | none | 17% | 7% | 1/27 | -7.91 | 1 | 3 | +7.49 | -4.14 | -7.91 | -3.93 | PASS | -10.581 |
| `union_clk_insider_cash_stab_h3` *(thin)* | long | 3 | leftover | list | none | 29% | 4% | 0/27 | -1.93 | 0 | 3 | +2.33 | -3.20 | -5.00 | -4.58 | PASS | -13.515 |
| `short_r_down_h1` *(thin)* | short | 1 | leftover | list | none | — | 0% | 0/27 | +0.00 | 0 | 0 | — | — | +0.00 | +0.00 | PASS | -15.0 |
| `short_r_down_h3` *(thin)* | short | 3 | leftover | list | none | — | 0% | 0/27 | +0.00 | 0 | 0 | — | — | +0.00 | +0.00 | PASS | -15.0 |
| `union_clk_r_up_coil_h1` *(thin)* | long | 1 | leftover | list | none | — | 0% | 0/27 | +0.00 | 0 | 0 | — | — | +0.00 | +0.00 | PASS | -15.0 |
| `union_r_up_h1` *(thin)* | long | 1 | leftover | list | none | — | 0% | 0/27 | +0.00 | 0 | 0 | — | — | +0.00 | +0.00 | PASS | -15.0 |
| `union_r_up_h3` *(thin)* | long | 3 | leftover | list | none | — | 0% | 0/27 | +0.00 | 0 | 0 | — | — | +0.00 | +0.00 | PASS | -15.0 |
