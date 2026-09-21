# Factor strategy mine — 2026-08-13 → 2026-09-21

Leak-free 09:30 recipes: **335** · candidate rows **2306** · fill `09:30 open, whole shares, Futubull fees, leftover split, sell first, hard-red sit, cash+holdings audit`.

Cash book: $10k, whole shares, Futubull fees, leftover split, sell first, min-hold, 09:30 open, hard-red S≤−3 sit, shorts marked as a liability. Each session starts from leftover cash and lots actually held (butterfly). Cash-start buttons wake a sleeve on date X with $10k and no lots (same rules). Stock investigator quotes 09:30 cameras / coaches / news from repo files. Size / sell / S-boost tweaks sit on the same ledger. Signal-only % is the old equal-weight path (not a fill). `flatten_h*` = wish-list (io/HOLD mornings still buy). `flatten_live_*` = only when the live flatten gate fires. Research only — does not change live `flatten_robust`.

Scoreboard files: slim `factor_mine.json` (stats / recipes / series) plus gzip shards in `factor_mine/shards/` (`books`, `starts`, `daily`, `probe`, `sim`). One file used to be 104MB and GitHub rejected the publish (100MB cap).

Combination books: **79** mixes on the same $10k cash ledger (shared leftover or split sleeves, official 09:30 / 16:00, hard-red sit, owner min-hold). Outperformers: `combo_es_9010_shared`, `combo_seh_333_skip`, `combo_es_8020_shared`, `combo_sh_3070_shared`, `combo_se_5050_skip`, `combo_ehs_702010_shared`, `combo_ej_5050_shared`, `combo_sh_5050_shared`, `combo_eh_7030_shared`, `combo_sh_macd_5050_shared`, `combo_jer_5050_shared`, `combo_sj_3070_shared`, `combo_ers_7030_shared`, `combo_ser_5050_shared`, `combo_sj_5050_shared`, `combo_je1_5050_shared`, `combo_e1s_7030_shared`, `combo_sf_5050_shared`, `combo_sf_7030_shared`, `combo_sf_3070_shared`.

A combo outperforms its members on the cash book when the audit passes and either (1) Book% is strictly higher than every member, or (2) Book% stays within 2pp of the richest member, max drawdown is strictly smaller than every member's drawdown, start-rate is at least the best member, both halves are green, and the worst session is no worse than the worst member session. Effectiveness / Signal% do not decide WIN.

Action blotters: [FACTOR_MINE_ACTION.md](FACTOR_MINE_ACTION.md).

| Strategy | Side | H | Size | Sell | Boost | Win% | $ days | Starts YES | Med start | Top-g | Losers | AvgW | AvgL | Book% | Signal% | Audit | Eff |
|---|---|---:|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---:|
| `combo_sh_5050_shared` | mix | 5 | leftover | list | none | 60% | 70% | 27/27 | +12.82 | 0 | 0 | +5.98 | -5.65 | +38.73 | — | PASS | 77.448 |
| `combo_sh_3070_shared` | mix | 5 | leftover | list | none | 59% | 63% | 27/27 | +16.57 | 0 | 0 | +6.01 | -5.55 | +41.73 | — | PASS | 75.847 |
| `combo_sh_macd_5050_shared` | mix | 5 | leftover | list | none | 59% | 59% | 27/27 | +14.65 | 0 | 0 | +6.09 | -5.08 | +38.27 | — | PASS | 75.472 |
| `combo_sj_3070_shared` | mix | 5 | leftover | list | none | 56% | 63% | 27/27 | +4.79 | 0 | 0 | +4.90 | -4.81 | +26.98 | — | PASS | 71.853 |
| `combo_seh_333_skip` | mix | 5 | leftover | list | none | 51% | 67% | 27/27 | +14.45 | 0 | 0 | +6.75 | -6.82 | +42.16 | — | PASS | 71.614 |
| `combo_se_5050_skip` | mix | 5 | leftover | list | none | 55% | 67% | 26/27 | +12.22 | 0 | 0 | +6.22 | -7.88 | +41.28 | — | PASS | 68.785 |
| `combo_ej_5050_shared` | mix | 5 | leftover | list | none | 47% | 56% | 26/27 | +8.48 | 0 | 0 | +6.05 | -5.54 | +39.45 | — | PASS | 67.653 |
| `combo_je1_5050_shared` | mix | 5 | leftover | list | none | 51% | 48% | 26/27 | +6.77 | 0 | 0 | +4.54 | -4.85 | +19.71 | — | PASS | 66.959 |
| `combo_ehs_702010_shared` | mix | 5 | leftover | list | none | 43% | 52% | 27/27 | +16.78 | 0 | 0 | +7.61 | -6.14 | +39.81 | — | PASS | 66.39 |
| `combo_eh_7030_shared` | mix | 5 | leftover | list | none | 40% | 56% | 26/27 | +18.41 | 0 | 0 | +8.22 | -6.38 | +38.40 | — | PASS | 66.202 |
| `combo_es_9010_shared` | mix | 5 | leftover | list | none | 45% | 67% | 26/27 | +15.45 | 0 | 0 | +7.13 | -6.72 | +44.06 | — | PASS | 66.063 |
| `combo_es_8020_shared` | mix | 5 | leftover | list | none | 47% | 59% | 26/27 | +13.89 | 0 | 0 | +6.70 | -7.24 | +41.82 | — | PASS | 64.886 |
| `combo_sj_5050_shared` | mix | 5 | leftover | list | none | 56% | 63% | 19/27 | +2.28 | 0 | 0 | +4.89 | -4.83 | +21.41 | — | PASS | 63.585 |
| `combo_jer_5050_shared` | mix | 5 | leftover | list | none | 44% | 52% | 24/27 | +3.49 | 0 | 0 | +6.23 | -5.81 | +32.26 | — | PASS | 62.065 |
| `combo_e1s_7030_shared` | mix | 5 | leftover | list | none | 56% | 56% | 15/27 | +1.34 | 0 | 0 | +4.92 | -6.26 | +14.41 | — | PASS | 55.489 |
| `combo_ers_7030_shared` | mix | 5 | leftover | list | none | 46% | 44% | 20/27 | +1.21 | 0 | 0 | +6.36 | -7.90 | +23.76 | — | PASS | 52.493 |
| `combo_ser_5050_shared` | mix | 5 | leftover | list | none | 48% | 52% | 15/27 | +0.22 | 0 | 0 | +6.24 | -7.81 | +21.50 | — | PASS | 49.691 |
| `combo_sf_5050_shared` | mix | 5 | leftover | list | none | 54% | 59% | 8/27 | -4.08 | 0 | 0 | +6.64 | -6.73 | +10.21 | — | PASS | 36.647 |
| `combo_sf_7030_shared` | mix | 5 | leftover | list | none | 55% | 59% | 6/27 | -3.17 | 0 | 0 | +6.38 | -7.32 | +9.94 | — | PASS | 34.563 |
| `combo_sf_3070_shared` | mix | 5 | leftover | list | none | 50% | 56% | 3/27 | -4.74 | 0 | 0 | +7.02 | -6.60 | +7.88 | — | PASS | 29.846 |
| `combo_sh_7030_shared` | mix | 5 | leftover | list | none | 60% | 70% | 27/27 | +6.62 | 0 | 0 | +5.95 | -5.71 | +27.36 | — | PASS | 75.672 |
| `union_hot_n4_holdup` | long | 1 | leftover | list | holdup | 50% | 56% | 25/27 | +23.72 | 21 | 34 | +10.04 | -5.84 | +55.01 | +60.60 | PASS | 75.48 |
| `union_hot_n4_h1` | long | 1 | leftover | list | none | 50% | 44% | 26/27 | +18.64 | 21 | 34 | +7.11 | -5.10 | +36.01 | +60.60 | PASS | 71.959 |
| `combo_oh_5050_shared` | mix | 5 | leftover | list | none | 53% | 44% | 26/27 | +15.26 | 0 | 0 | +6.71 | -5.17 | +19.23 | — | PASS | 70.058 |
| `combo_seh_333_shared` | mix | 5 | leftover | list | none | 49% | 67% | 27/27 | +11.30 | 0 | 0 | +6.80 | -6.74 | +34.84 | — | PASS | 70.039 |
| `combo_seh_333_weather` | mix | 5 | leftover | list | none | 49% | 67% | 27/27 | +11.30 | 0 | 0 | +6.80 | -6.74 | +34.84 | — | PASS | 70.039 |
| `combo_he1_5050_shared` | mix | 5 | leftover | list | none | 51% | 56% | 26/27 | +14.35 | 0 | 0 | +5.48 | -5.61 | +26.82 | — | PASS | 70.001 |
| `combo_seh_502525_shared` | mix | 5 | leftover | list | none | 48% | 70% | 27/27 | +7.64 | 0 | 0 | +6.87 | -6.68 | +30.58 | — | PASS | 69.983 |
| `combo_seh_451540_shared` | mix | 5 | leftover | list | none | 48% | 70% | 27/27 | +10.82 | 0 | 0 | +6.90 | -6.93 | +32.23 | — | PASS | 69.909 |
| `union_white_both_n4_h2` | long | 2 | leftover | list | none | 52% | 52% | 23/27 | +3.69 | 9 | 15 | +7.78 | -5.36 | +24.18 | +15.11 | PASS | 69.708 |
| `combo_jse_333_shared` | mix | 5 | leftover | list | none | 52% | 59% | 27/27 | +4.62 | 0 | 0 | +5.70 | -5.89 | +34.80 | — | PASS | 69.22 |
| `combo_hj_5050_shared` | mix | 5 | leftover | list | none | 48% | 48% | 26/27 | +10.09 | 0 | 0 | +5.55 | -4.26 | +22.85 | — | PASS | 69.212 |
| `combo_seh_403525_shared` | mix | 5 | leftover | list | none | 48% | 63% | 27/27 | +9.11 | 0 | 0 | +6.92 | -6.60 | +33.36 | — | PASS | 69.023 |
| `combo_seh_601525_shared` | mix | 5 | leftover | list | none | 48% | 70% | 27/27 | +5.73 | 0 | 0 | +6.89 | -6.93 | +26.37 | — | PASS | 68.906 |
| `combo_hn_7030_shared` | mix | 5 | leftover | list | none | 50% | 48% | 26/27 | +9.92 | 0 | 0 | +4.92 | -4.84 | +23.03 | — | PASS | 68.759 |
| `combo_hn_5050_shared` | mix | 5 | leftover | list | none | 51% | 48% | 26/27 | +6.38 | 0 | 0 | +5.00 | -4.72 | +18.16 | — | PASS | 68.726 |
| `union_clk_mom_break_peer_opp_h1` | long | 1 | leftover | list | none | 52% | 41% | 26/27 | +4.56 | 10 | 26 | +3.93 | -3.19 | +16.84 | +17.93 | PASS | 68.564 |
| `combo_seh_333_split` | mix | 5 | leftover | list | none | 49% | 63% | 27/27 | +8.60 | 0 | 0 | +6.83 | -6.79 | +24.07 | — | PASS | 67.778 |
| `combo_seh_404020_shared` | mix | 5 | leftover | list | none | 49% | 56% | 27/27 | +8.36 | 0 | 0 | +6.75 | -6.69 | +33.08 | — | PASS | 67.682 |
| `combo_ehs_601525_shared` | mix | 5 | leftover | list | none | 47% | 56% | 27/27 | +12.55 | 0 | 0 | +6.98 | -6.52 | +37.34 | — | PASS | 67.626 |
| `combo_seh_502525_split` | mix | 5 | leftover | list | none | 48% | 67% | 27/27 | +5.92 | 0 | 0 | +7.16 | -6.59 | +17.91 | — | PASS | 67.586 |
| `union_w_hot_cond_h1` | long | 1 | leftover | list | none | 50% | 52% | 26/27 | +7.97 | 30 | 55 | +5.43 | -5.92 | +8.26 | +24.24 | PASS | 67.444 |
| `short_news_r_macd_h3` | short | 3 | leftover | list | none | 60% | 59% | 27/27 | +4.72 | 7 | 35 | +5.08 | -5.79 | +14.54 | +18.67 | PASS | 67.302 |
| `union_hot_score_h1` | long | 1 | leftover | list | none | 49% | 48% | 26/27 | +9.15 | 37 | 62 | +6.20 | -6.00 | +12.23 | +34.08 | PASS | 67.157 |
| `union_ret_5_h1` | long | 1 | leftover | list | none | 49% | 48% | 26/27 | +6.04 | 30 | 61 | +5.85 | -5.40 | +13.11 | +23.32 | PASS | 67.151 |
| `combo_ee1_3070_shared` | mix | 5 | leftover | list | none | 42% | 67% | 25/27 | +14.36 | 0 | 0 | +9.15 | -7.62 | +40.04 | — | PASS | 66.775 |
| `combo_ee1_5050_shared` | mix | 5 | leftover | list | none | 42% | 67% | 25/27 | +14.36 | 0 | 0 | +9.15 | -7.62 | +40.04 | — | PASS | 66.775 |
| `combo_ee1_7030_shared` | mix | 5 | leftover | list | none | 42% | 67% | 25/27 | +14.36 | 0 | 0 | +9.15 | -7.62 | +40.04 | — | PASS | 66.775 |
| `union_vol_ab_h1` | long | 1 | leftover | list | none | 54% | 37% | 26/27 | +7.56 | 17 | 40 | +5.09 | -4.33 | +11.22 | +64.93 | PASS | 66.616 |
| `combo_eh_5050_shared` | mix | 5 | leftover | list | none | 43% | 56% | 26/27 | +17.92 | 0 | 0 | +7.80 | -6.59 | +36.68 | — | PASS | 66.398 |
| `union_hot_n12_h1` | long | 1 | leftover | list | none | 52% | 48% | 26/27 | +7.13 | 43 | 87 | +5.04 | -5.76 | +5.65 | +25.95 | PASS | 66.32 |
| `union_join_vol_green_h1` | long | 1 | leftover | list | none | 47% | 41% | 26/27 | +6.28 | 12 | 38 | +4.79 | -3.94 | +21.08 | +9.02 | PASS | 66.102 |
| `union_e_fresh_h3` | long | 3 | leftover | list | none | 47% | 67% | 25/27 | +14.36 | 20 | 89 | +9.15 | -7.62 | +40.04 | +8.98 | PASS | 66.006 |
| `combo_se_5050_shared` | mix | 5 | leftover | list | none | 53% | 63% | 26/27 | +5.69 | 0 | 0 | +6.27 | -7.74 | +30.81 | — | PASS | 66.004 |
| `combo_se_5050_weather` | mix | 5 | leftover | list | none | 53% | 63% | 26/27 | +5.69 | 0 | 0 | +6.27 | -7.74 | +30.81 | — | PASS | 66.004 |
| `union_oppset_h1` | long | 1 | leftover | list | none | 54% | 37% | 26/27 | +8.31 | 16 | 56 | +5.71 | -4.90 | +8.31 | +18.00 | PASS | 65.643 |
| `oppset_h1` | long | 1 | leftover | list | none | 54% | 37% | 26/27 | +7.47 | 17 | 56 | +6.04 | -5.06 | +6.55 | +15.93 | PASS | 65.394 |
| `combo_eh_3070_shared` | mix | 5 | leftover | list | none | 43% | 52% | 26/27 | +16.04 | 0 | 0 | +7.65 | -6.79 | +34.65 | — | PASS | 65.21 |
| `combo_hn_3070_shared` | mix | 5 | leftover | list | none | 51% | 52% | 22/27 | +2.79 | 0 | 0 | +5.12 | -4.62 | +12.81 | — | PASS | 65.034 |
| `short_news_r_h1` | short | 1 | leftover | list | none | 52% | 41% | 27/27 | +3.09 | 5 | 20 | +3.32 | -3.95 | +2.29 | +4.91 | PASS | 64.71 |
| `union_news_pack_net2_h1` | long | 1 | leftover | list | none | 53% | 44% | 21/27 | +11.27 | 1 | 12 | +3.77 | -3.37 | +21.18 | +11.59 | PASS | 64.21 |
| `combo_se_3070_shared` | mix | 5 | leftover | list | none | 50% | 52% | 26/27 | +9.89 | 0 | 0 | +6.51 | -7.45 | +37.56 | — | PASS | 63.85 |
| `union_clk_hold_vs_sector_opp_h1` | long | 1 | leftover | list | none | 46% | 41% | 26/27 | +9.13 | 6 | 36 | +5.16 | -3.82 | +12.44 | +1.07 | PASS | 63.561 |
| `union_w_hot_candle_h1` | long | 1 | leftover | list | none | 50% | 44% | 24/27 | +5.41 | 29 | 57 | +5.09 | -5.25 | +4.70 | +24.92 | PASS | 63.402 |
| `combo_p2s_5050_shared` | mix | 5 | leftover | list | none | 61% | 67% | 17/27 | +3.63 | 0 | 0 | +4.64 | -5.14 | +14.13 | — | PASS | 63.124 |
| `union_clk_nr7_mom_h1` | long | 1 | leftover | list | none | 48% | 30% | 25/27 | +10.53 | 3 | 10 | +5.52 | -3.74 | +11.56 | +24.10 | PASS | 62.872 |
| `combo_her_5050_shared` | mix | 5 | leftover | list | none | 42% | 56% | 26/27 | +12.48 | 0 | 0 | +7.32 | -7.93 | +25.83 | — | PASS | 62.849 |
| `union_break10_h1` | long | 1 | leftover | list | none | 50% | 44% | 23/27 | +5.48 | 27 | 45 | +5.08 | -4.99 | -0.11 | +12.08 | PASS | 62.8 |
| `combo_se_5050_split` | mix | 5 | leftover | list | none | 51% | 56% | 26/27 | +5.78 | 0 | 0 | +6.94 | -7.12 | +20.82 | — | PASS | 62.789 |
| `union_clk_fresh_cat_coil_opp_h1` | long | 1 | leftover | list | none | 49% | 33% | 25/27 | +2.57 | 17 | 34 | +4.48 | -3.81 | +0.92 | +2.07 | PASS | 62.492 |
| `combo_ps_5050_shared` | mix | 5 | leftover | list | none | 61% | 67% | 17/27 | +1.72 | 0 | 0 | +4.52 | -5.32 | +12.28 | — | PASS | 62.454 |
| `union_news_pack_h1` | long | 1 | leftover | list | none | 52% | 48% | 21/27 | +7.80 | 2 | 15 | +3.56 | -3.88 | +16.67 | +10.47 | PASS | 62.451 |
| `union_candle_score_h1` | long | 1 | leftover | list | none | 51% | 33% | 24/27 | +2.44 | 18 | 50 | +4.59 | -4.05 | +5.26 | +10.75 | PASS | 62.329 |
| `union_white_both_n4_h1` | long | 1 | leftover | list | none | 50% | 41% | 14/27 | +0.35 | 5 | 9 | +6.59 | -3.16 | +20.67 | +6.25 | PASS | 62.26 |
| `combo_ps_7030_shared` | mix | 5 | leftover | list | none | 61% | 63% | 17/27 | +4.99 | 0 | 0 | +4.48 | -5.40 | +16.00 | — | PASS | 62.174 |
| `combo_en_5050_shared` | mix | 5 | leftover | list | none | 45% | 48% | 25/27 | +4.10 | 0 | 0 | +5.67 | -6.26 | +25.92 | — | PASS | 61.825 |
| `union_e_fresh_h1` | long | 1 | leftover | list | none | 49% | 41% | 25/27 | +4.92 | 9 | 45 | +4.69 | -5.92 | +14.05 | +7.59 | PASS | 61.16 |
| `union_ret_5_h3` | long | 3 | leftover | list | none | 46% | 56% | 24/27 | +10.33 | 47 | 151 | +9.95 | -8.21 | +24.62 | +50.41 | PASS | 60.696 |
| `yday_gainer_h1` | long | 1 | leftover | list | none | 51% | 41% | 21/27 | +2.78 | 14 | 63 | +4.80 | -3.91 | +9.34 | +13.54 | PASS | 60.46 |
| `combo_en_7030_shared` | mix | 5 | leftover | list | none | 41% | 44% | 25/27 | +6.84 | 0 | 0 | +6.03 | -6.10 | +29.80 | — | PASS | 60.338 |
| `short_clk_ext_veto_opp_h3` | short | 3 | leftover | list | none | 52% | 48% | 26/27 | +2.88 | 12 | 53 | +7.55 | -6.12 | +4.84 | -17.21 | PASS | 60.222 |
| `union_hot_score_h3` | long | 3 | leftover | list | none | 44% | 56% | 24/27 | +4.10 | 53 | 150 | +12.54 | -8.74 | +13.01 | +254.30 | PASS | 59.657 |
| `union_vol_ab_h3` | long | 3 | leftover | list | none | 50% | 37% | 26/27 | +6.31 | 24 | 87 | +9.14 | -6.73 | +5.69 | +86.28 | PASS | 59.408 |
| `union_w_hot_candle_h3` | long | 3 | leftover | list | none | 45% | 56% | 24/27 | +7.36 | 44 | 142 | +9.43 | -8.81 | +15.17 | +204.79 | PASS | 59.016 |
| `union_break10_h3` | long | 3 | leftover | list | none | 48% | 48% | 23/27 | +4.00 | 39 | 122 | +9.34 | -7.43 | +3.45 | +38.40 | PASS | 58.182 |
| `union_w_hot_cond_h3` | long | 3 | leftover | list | none | 44% | 56% | 23/27 | +6.81 | 46 | 145 | +10.35 | -8.31 | +12.88 | +207.85 | PASS | 57.927 |
| `probable_h1` | long | 1 | leftover | list | none | 50% | 33% | 22/27 | +1.37 | 13 | 63 | +4.07 | -3.65 | +1.46 | +11.06 | PASS | 57.512 |
| `union_last_red_h3` | long | 3 | leftover | list | none | 40% | 56% | 25/27 | +5.59 | 18 | 127 | +8.60 | -6.75 | +7.43 | -0.56 | PASS | 56.842 |
| `union_last_green_h1` | long | 1 | leftover | list | none | 46% | 41% | 15/27 | +1.33 | 16 | 41 | +4.86 | -3.25 | +8.46 | +14.75 | PASS | 56.655 |
| `combo_se_7030_shared` | mix | 5 | leftover | list | none | 52% | 59% | 18/27 | +1.79 | 0 | 0 | +6.35 | -7.52 | +24.99 | — | PASS | 56.626 |
| `combo_nj_5050_shared` | mix | 5 | leftover | list | none | 49% | 37% | 19/27 | +0.46 | 0 | 0 | +4.31 | -4.32 | +4.96 | — | PASS | 56.354 |
| `combo_ef_7030_shared` | mix | 5 | leftover | list | none | 42% | 59% | 22/27 | +4.23 | 0 | 0 | +7.88 | -7.49 | +19.59 | — | PASS | 56.265 |
| `union_news_pack_net3_h1` | long | 1 | leftover | list | none | 52% | 37% | 16/27 | +3.24 | 1 | 11 | +3.56 | -3.37 | +12.40 | +7.24 | PASS | 56.189 |
| `combo_sj_7030_shared` | mix | 5 | leftover | list | none | 53% | 59% | 14/27 | +0.72 | 0 | 0 | +4.93 | -4.80 | +12.95 | — | PASS | 56.111 |
| `union_macd_up_h1` | long | 1 | leftover | list | none | 45% | 44% | 18/27 | +2.90 | 17 | 51 | +4.48 | -4.07 | +5.12 | +13.89 | PASS | 56.067 |
| `short_alarm_h3` | short | 3 | leftover | list | none | 54% | 37% | 27/27 | +1.39 | 13 | 92 | +6.77 | -9.42 | +3.61 | +8.42 | PASS | 56.06 |
| `union_clk_hold_vs_sector_h1` | long | 1 | leftover | list | none | 42% | 30% | 25/27 | +2.03 | 7 | 48 | +3.92 | -4.20 | +2.03 | -3.90 | PASS | 55.94 |
| `union_white_both_n4_h5_s12` | long | 5 | leftover | list | none | 43% | 59% | 16/27 | +1.77 | 13 | 30 | +9.82 | -7.09 | +22.12 | +17.47 | PASS | 54.978 |
| `union_clk_mom_break_peer_h1` | long | 1 | leftover | list | none | 52% | 37% | 15/27 | +0.55 | 11 | 39 | +3.94 | -3.70 | +1.49 | +5.29 | PASS | 54.372 |
| `combo_eer_5050_shared` | mix | 5 | leftover | list | none | 39% | 48% | 24/27 | +7.78 | 0 | 0 | +8.49 | -8.96 | +24.11 | — | PASS | 54.222 |
| `union_candle_score_h3` | long | 3 | leftover | list | none | 45% | 44% | 23/27 | +2.34 | 26 | 133 | +6.66 | -6.35 | +7.11 | +24.78 | PASS | 53.953 |
| `union_join_vol_green_h3` | long | 3 | leftover | list | none | 51% | 37% | 20/27 | +5.06 | 21 | 78 | +8.18 | -7.44 | -2.41 | +36.89 | PASS | 52.326 |
| `union_white_both_n4_h5` | long | 5 | leftover | list | none | 43% | 52% | 16/27 | +0.08 | 13 | 30 | +9.82 | -8.82 | +19.79 | +17.47 | PASS | 51.791 |
| `union_earn_react_h3` | long | 3 | leftover | list | none | 41% | 52% | 21/27 | +0.44 | 15 | 88 | +8.14 | -9.05 | +21.00 | -13.13 | PASS | 51.75 |
| `combo_en_3070_shared` | mix | 5 | leftover | list | none | 45% | 41% | 16/27 | +0.37 | 0 | 0 | +5.71 | -6.13 | +20.73 | — | PASS | 51.179 |
| `union_macd_up_h3` | long | 3 | leftover | list | none | 42% | 44% | 17/27 | +2.73 | 30 | 121 | +8.48 | -6.12 | +2.73 | +20.05 | PASS | 49.429 |
| `flatten_white_yday_h5` | long | 5 | leftover | list | none | 53% | 41% | 17/27 | +0.13 | 4 | 21 | +9.10 | -10.47 | +3.28 | -8.60 | PASS | 48.724 |
| `combo_ef_5050_shared` | mix | 5 | leftover | list | none | 41% | 56% | 15/27 | +0.15 | 0 | 0 | +8.28 | -7.30 | +11.69 | — | PASS | 47.923 |
| `combo_fe_5050_shared` | mix | 5 | leftover | list | none | 41% | 56% | 15/27 | +0.15 | 0 | 0 | +8.28 | -7.30 | +11.69 | — | PASS | 47.923 |
| `combo_snj_333_shared` | mix | 5 | leftover | list | none | 52% | 59% | 12/27 | -0.71 | 0 | 0 | +4.51 | -4.78 | +9.75 | — | PASS | 44.06 |
| `combo_hf_5050_shared` | mix | 5 | leftover | list | none | 47% | 52% | 13/27 | -0.41 | 0 | 0 | +7.00 | -6.04 | +8.45 | — | PASS | 40.951 |
| `union_h1_topheavy` | long | 1 | topheavy | list | none | 45% | 44% | 11/27 | -0.08 | 9 | 46 | +4.55 | -3.78 | +8.21 | +8.67 | PASS | 40.65 |
| `short_news_head_h3` | short | 3 | leftover | list | none | 56% | 63% | 12/27 | -0.81 | 6 | 41 | +5.73 | -6.68 | +6.07 | -2.05 | PASS | 40.261 |
| `union_clk_fresh_cat_coil_h1` | long | 1 | leftover | list | none | 46% | 41% | 13/27 | -0.52 | 7 | 37 | +3.50 | -3.37 | -2.98 | -4.67 | PASS | 40.16 |
| `combo_e1er_5050_shared` | mix | 5 | leftover | list | none | 45% | 19% | 15/27 | +0.34 | 0 | 0 | +4.61 | -7.55 | -2.27 | — | PASS | 39.529 |
| `short_news_or_h3` | short | 3 | leftover | list | none | 54% | 56% | 13/27 | -0.81 | 9 | 48 | +4.94 | -7.03 | +4.62 | -5.67 | PASS | 39.105 |
| `short_news_r_h3` | short | 3 | leftover | list | none | 54% | 56% | 13/27 | -0.81 | 9 | 48 | +5.03 | -7.03 | +4.45 | -5.63 | PASS | 38.8 |
| `union_news_g_conv_h1` | long | 1 | conviction | list | none | 52% | 44% | 7/27 | -4.78 | 1 | 15 | +3.74 | -4.42 | +4.78 | +12.80 | PASS | 38.268 |
| `union_h1_rankw` | long | 1 | rank_w | list | none | 45% | 44% | 9/27 | -1.22 | 9 | 46 | +4.41 | -3.89 | +3.32 | +8.67 | PASS | 37.718 |
| `union_white_yday_h1` | long | 1 | leftover | list | none | 49% | 37% | 6/27 | -2.40 | 3 | 10 | +4.48 | -3.37 | +1.76 | +1.70 | PASS | 37.561 |
| `union_flow_in_h1` | long | 1 | leftover | list | none | 43% | 33% | 9/27 | -2.00 | 4 | 8 | +4.34 | -4.11 | +7.34 | +9.36 | PASS | 37.094 |
| `union_h1` | long | 1 | leftover | list | none | 45% | 41% | 9/27 | -2.60 | 9 | 46 | +4.43 | -3.82 | +2.58 | +8.67 | PASS | 36.997 |
| `union_h1_sizeup` | long | 1 | leftover | list | sizeup | 45% | 41% | 9/27 | -2.60 | 9 | 46 | +4.43 | -3.82 | +2.58 | +8.67 | PASS | 36.997 |
| `union_h1_trail` | long | 1 | leftover | trail | none | 45% | 41% | 9/27 | -2.60 | 9 | 46 | +4.43 | -3.82 | +2.58 | +8.67 | PASS | 36.997 |
| `flatten_live_h1_topheavy` | long | 1 | topheavy | list | none | 54% | 11% | 7/27 | +0.00 | 1 | 5 | +6.75 | -3.75 | +7.19 | +4.45 | PASS | 36.91 |
| `union_vol_green_h1` | long | 1 | leftover | list | none | 45% | 44% | 10/27 | -2.47 | 21 | 50 | +5.34 | -7.78 | +7.95 | -5.19 | PASS | 36.79 |
| `union_h1_time` | long | 1 | leftover | time | none | 45% | 37% | 9/27 | -2.68 | 9 | 46 | +4.34 | -3.49 | +2.08 | +8.67 | PASS | 36.602 |
| `combo_fse_333_shared` | mix | 5 | leftover | list | none | 47% | 56% | 12/27 | -0.53 | 0 | 0 | +6.57 | -7.41 | +13.19 | — | PASS | 36.548 |
| `union_h1_cut` | long | 1 | leftover | cut_loser | none | 45% | 37% | 9/27 | -2.68 | 9 | 46 | +4.43 | -3.78 | +2.41 | +8.67 | PASS | 36.296 |
| `union_join_present_h1` | long | 1 | leftover | list | none | 44% | 41% | 9/27 | -3.22 | 8 | 50 | +4.48 | -3.87 | +1.84 | +7.23 | PASS | 36.247 |
| `combo_se1_5050_shared` | mix | 5 | leftover | list | none | 57% | 56% | 5/27 | -1.82 | 0 | 0 | +4.84 | -6.42 | +12.82 | — | PASS | 36.154 |
| `combo_fh_7030_shared` | mix | 5 | leftover | list | none | 45% | 63% | 7/27 | -1.62 | 0 | 0 | +7.08 | -6.26 | +2.98 | — | PASS | 36.079 |
| `union_white_coil_h1` | long | 1 | leftover | list | none | 41% | 30% | 13/27 | -0.08 | 4 | 20 | +4.95 | -3.95 | -2.03 | -0.13 | PASS | 35.921 |
| `combo_fes_403030_shared` | mix | 5 | leftover | list | none | 45% | 56% | 12/27 | -0.88 | 0 | 0 | +6.76 | -7.34 | +11.04 | — | PASS | 35.884 |
| `flatten_live_h1` | long | 1 | leftover | list | none | 54% | 11% | 7/27 | +0.00 | 1 | 5 | +6.31 | -4.05 | +8.22 | +4.45 | PASS | 35.869 |
| `flatten_live_h1_cut` | long | 1 | leftover | cut_loser | none | 54% | 11% | 7/27 | +0.00 | 1 | 5 | +6.31 | -4.05 | +8.22 | +4.45 | PASS | 35.869 |
| `flatten_live_h1_sboost` | long | 1 | leftover | list | both | 54% | 11% | 7/27 | +0.00 | 1 | 5 | +6.31 | -4.05 | +8.22 | +4.45 | PASS | 35.869 |
| `flatten_live_h1_sizeup` | long | 1 | leftover | list | sizeup | 54% | 11% | 7/27 | +0.00 | 1 | 5 | +6.31 | -4.05 | +8.22 | +4.45 | PASS | 35.869 |
| `flatten_live_h1_time` | long | 1 | leftover | time | none | 54% | 11% | 7/27 | +0.00 | 1 | 5 | +6.31 | -4.05 | +8.22 | +4.45 | PASS | 35.869 |
| `flatten_live_h1_trail` | long | 1 | leftover | trail | none | 54% | 11% | 7/27 | +0.00 | 1 | 5 | +6.31 | -4.05 | +8.22 | +4.45 | PASS | 35.869 |
| `flatten_live_h1_half` | long | 1 | half | list | none | 54% | 11% | 7/27 | +0.00 | 1 | 5 | +6.58 | -3.90 | +3.74 | +4.45 | PASS | 35.818 |
| `combo_nse_333_shared` | mix | 5 | leftover | list | none | 49% | 48% | 8/27 | -0.92 | 0 | 0 | +5.49 | -6.15 | +20.03 | — | PASS | 35.769 |
| `combo_sn_5050_shared` | mix | 5 | leftover | list | none | 56% | 63% | 3/27 | -4.25 | 0 | 0 | +4.34 | -5.10 | +0.56 | — | PASS | 35.422 |
| `union_news_present_h1` | long | 1 | leftover | list | none | 44% | 37% | 9/27 | -3.22 | 8 | 48 | +4.35 | -3.88 | +0.29 | +5.25 | PASS | 35.042 |
| `union_candle_h1` | long | 1 | leftover | list | none | 45% | 30% | 9/27 | -1.74 | 11 | 47 | +4.41 | -3.75 | +1.07 | +2.21 | PASS | 34.629 |
| `ohlc_hot_coil_h1` | long | 1 | leftover | list | none | 49% | 41% | 7/27 | -1.80 | 12 | 27 | +4.53 | -3.94 | -18.84 | -9.28 | PASS | 34.531 |
| `combo_sn_3070_shared` | mix | 5 | leftover | list | none | 55% | 59% | 3/27 | -4.73 | 0 | 0 | +4.43 | -4.95 | +0.66 | — | PASS | 34.425 |
| `short_last_red_h1` | short | 1 | leftover | list | none | 49% | 30% | 12/27 | -0.01 | 9 | 70 | +4.59 | -4.48 | -7.91 | -4.20 | PASS | 34.374 |
| `combo_sn_7030_shared` | mix | 5 | leftover | list | none | 56% | 59% | 3/27 | -3.81 | 0 | 0 | +4.25 | -5.21 | +0.37 | — | PASS | 34.339 |
| `ohlc_hot_h1` | long | 1 | leftover | list | none | 45% | 44% | 10/27 | -1.41 | 17 | 52 | +3.36 | -5.08 | -10.12 | -3.07 | PASS | 34.045 |
| `flatten_live_h1_rankw` | long | 1 | rank_w | list | none | 54% | 11% | 6/27 | +0.00 | 1 | 5 | +6.14 | -4.12 | +4.03 | +4.45 | PASS | 33.969 |
| `probable_probable_ok_h1` | long | 1 | leftover | list | none | 47% | 30% | 8/27 | -1.56 | 8 | 28 | +3.95 | -3.29 | -4.33 | +2.98 | PASS | 33.939 |
| `combo_jf_5050_shared` | mix | 5 | leftover | list | none | 44% | 59% | 6/27 | -4.19 | 0 | 0 | +5.72 | -5.15 | +5.00 | — | PASS | 33.795 |
| `combo_ef_3070_shared` | mix | 5 | leftover | list | none | 43% | 59% | 10/27 | -1.50 | 0 | 0 | +8.23 | -7.46 | +6.03 | — | PASS | 33.596 |
| `union_join_g_h1` | long | 1 | leftover | list | none | 46% | 37% | 7/27 | -2.08 | 10 | 51 | +4.12 | -4.00 | +1.16 | +2.57 | PASS | 33.46 |
| `combo_ne1_5050_shared` | mix | 5 | leftover | list | none | 51% | 37% | 5/27 | -2.19 | 0 | 0 | +4.10 | -5.14 | +6.94 | — | PASS | 32.917 |
| `union_news_or_net4_rw_h1` | long | 1 | rank_w | list | none | 49% | 44% | 4/27 | -11.51 | 1 | 10 | +3.33 | -4.12 | -4.96 | -2.12 | PASS | 32.842 |
| `union_blue_vol_h3` | long | 3 | leftover | list | none | 42% | 37% | 13/27 | -0.55 | 19 | 90 | +10.17 | -7.07 | -4.01 | +18.43 | PASS | 32.74 |
| `union_news_or_net4_conv_h1` | long | 1 | conviction | list | none | 49% | 41% | 4/27 | -9.23 | 1 | 10 | +3.29 | -4.17 | -0.59 | -2.12 | PASS | 32.662 |
| `union_h3_cut` | long | 3 | leftover | cut_loser | none | 44% | 52% | 5/27 | -4.82 | 19 | 109 | +9.02 | -5.30 | +0.39 | +10.70 | PASS | 32.572 |
| `union_h3_trail` | long | 3 | leftover | trail | none | 44% | 52% | 5/27 | -4.82 | 19 | 109 | +9.02 | -5.30 | +0.39 | +10.70 | PASS | 32.572 |
| `union_h5_time` | long | 5 | leftover | time | none | 46% | 67% | 7/27 | -4.85 | 30 | 149 | +8.94 | -8.07 | +1.86 | +19.96 | PASS | 32.443 |
| `union_news_head_h1` | long | 1 | leftover | list | none | 49% | 30% | 5/27 | -1.33 | 5 | 23 | +4.15 | -3.97 | -4.85 | +16.70 | PASS | 32.02 |
| `union_h3_time` | long | 3 | leftover | time | none | 44% | 52% | 5/27 | -4.82 | 19 | 109 | +8.37 | -5.38 | +0.94 | +10.70 | PASS | 31.92 |
| `union_h1_sboost` | long | 1 | leftover | list | both | 45% | 41% | 4/27 | -3.98 | 9 | 46 | +4.31 | -3.92 | +1.22 | +8.67 | PASS | 31.858 |
| `flatten_h5_topheavy` | long | 5 | topheavy | list | none | 45% | 56% | 8/27 | -2.23 | 20 | 106 | +8.62 | -8.14 | +5.34 | +2.23 | PASS | 31.766 |
| `union_h5` | long | 5 | leftover | list | none | 46% | 63% | 7/27 | -4.85 | 30 | 149 | +8.84 | -8.09 | +1.78 | +19.96 | PASS | 31.614 |
| `union_h5_cut` | long | 5 | leftover | cut_loser | none | 46% | 63% | 7/27 | -4.85 | 30 | 149 | +8.84 | -8.09 | +1.78 | +19.96 | PASS | 31.614 |
| `union_h5_sizeup` | long | 5 | leftover | list | sizeup | 46% | 63% | 7/27 | -4.85 | 30 | 149 | +8.84 | -8.09 | +1.78 | +19.96 | PASS | 31.614 |
| `union_h5_trail` | long | 5 | leftover | trail | none | 46% | 63% | 7/27 | -4.85 | 30 | 149 | +8.84 | -8.09 | +1.78 | +19.96 | PASS | 31.614 |
| `combo_fe1_5050_shared` | mix | 5 | leftover | list | none | 46% | 56% | 6/27 | -4.09 | 0 | 0 | +5.75 | -6.31 | +1.31 | — | PASS | 31.368 |
| `union_vol_g_h3` | long | 3 | leftover | list | none | 49% | 44% | 10/27 | -1.05 | 27 | 121 | +8.61 | -7.77 | -5.06 | +24.46 | PASS | 31.34 |
| `union_join_present_h3` | long | 3 | leftover | list | none | 43% | 48% | 5/27 | -4.70 | 18 | 111 | +9.75 | -5.54 | -0.82 | +8.40 | PASS | 31.133 |
| `union_news_g_h1` | long | 1 | leftover | list | none | 50% | 41% | 2/27 | -2.26 | 4 | 33 | +3.98 | -4.24 | +0.11 | +12.07 | PASS | 31.001 |
| `union_h3` | long | 3 | leftover | list | none | 44% | 52% | 4/27 | -4.82 | 19 | 109 | +9.01 | -5.62 | -1.06 | +10.70 | PASS | 30.942 |
| `union_h3_sizeup` | long | 3 | leftover | list | sizeup | 44% | 52% | 4/27 | -4.82 | 19 | 109 | +9.01 | -5.62 | -1.06 | +10.70 | PASS | 30.942 |
| `union_vol_green_h3` | long | 3 | leftover | list | none | 51% | 30% | 12/27 | -0.67 | 34 | 104 | +7.05 | -7.70 | -10.92 | +25.34 | PASS | 30.869 |
| `probable_h3` | long | 3 | leftover | list | none | 47% | 37% | 12/27 | -0.49 | 27 | 128 | +7.76 | -8.08 | -5.52 | +16.26 | PASS | 30.781 |
| `union_white_any_h2` | long | 2 | leftover | list | none | 44% | 48% | 5/27 | -2.94 | 10 | 38 | +6.42 | -5.72 | -1.24 | +6.98 | PASS | 30.564 |
| `union_news_or_h1` | long | 1 | leftover | list | none | 49% | 37% | 2/27 | -2.00 | 4 | 33 | +4.04 | -3.78 | -0.68 | +11.84 | PASS | 30.551 |
| `union_h5_topheavy` | long | 5 | topheavy | list | none | 46% | 59% | 7/27 | -3.50 | 30 | 149 | +8.60 | -8.27 | +1.31 | +19.96 | PASS | 30.544 |
| `combo_ner_5050_shared` | mix | 5 | leftover | list | none | 42% | 44% | 6/27 | -1.16 | 0 | 0 | +5.57 | -6.56 | +17.85 | — | PASS | 30.446 |
| `union_h3_half` | long | 3 | half | list | none | 44% | 52% | 3/27 | -5.38 | 19 | 109 | +8.98 | -5.05 | -4.36 | +10.70 | PASS | 30.393 |
| `union_h3_sboost` | long | 3 | leftover | list | both | 44% | 52% | 4/27 | -5.36 | 19 | 109 | +8.68 | -5.81 | -2.65 | +10.70 | PASS | 30.144 |
| `union_blue_h1` | long | 1 | leftover | list | none | 42% | 26% | 7/27 | -1.11 | 12 | 45 | +4.36 | -4.09 | -0.93 | -1.77 | PASS | 30.056 |
| `union_h5_sboost` | long | 5 | leftover | list | both | 46% | 63% | 6/27 | -4.82 | 30 | 149 | +8.36 | -8.56 | +0.91 | +19.96 | PASS | 29.976 |
| `union_h3_exit_news_r` | long | 3 | leftover | list | none | 43% | 44% | 5/27 | -6.06 | 19 | 110 | +9.58 | -5.75 | -2.64 | +5.71 | PASS | 29.806 |
| `union_h5_exit_alarm` | long | 5 | leftover | list | none | 48% | 52% | 7/27 | -2.55 | 29 | 150 | +7.12 | -7.11 | +3.52 | +27.39 | PASS | 29.771 |
| `union_white_h1` | long | 1 | leftover | list | none | 37% | 37% | 6/27 | -5.80 | 4 | 26 | +5.55 | -4.21 | +0.19 | -8.23 | PASS | 29.581 |
| `union_last_green_h5` | long | 5 | leftover | list | none | 48% | 59% | 7/27 | -6.30 | 40 | 150 | +7.47 | -11.01 | -1.46 | +64.67 | PASS | 29.395 |
| `union_news_g_cond_h1` | long | 1 | leftover | list | none | 48% | 33% | 2/27 | -2.62 | 4 | 34 | +4.04 | -3.95 | -1.76 | +11.24 | PASS | 29.052 |
| `union_h3_exit_alarm` | long | 3 | leftover | list | none | 44% | 48% | 5/27 | -4.47 | 18 | 111 | +7.18 | -5.64 | -1.19 | +9.33 | PASS | 29.009 |
| `union_h3_rankw` | long | 3 | rank_w | list | none | 44% | 48% | 4/27 | -7.72 | 19 | 109 | +8.82 | -5.78 | -6.43 | +10.70 | PASS | 29.002 |
| `flatten_h5_s8` | long | 5 | leftover | list | none | 45% | 63% | 3/27 | -5.42 | 20 | 106 | +8.86 | -7.94 | +5.49 | +2.23 | PASS | 28.923 |
| `flatten_h5` | long | 5 | leftover | list | none | 45% | 63% | 3/27 | -2.75 | 20 | 106 | +8.90 | -8.31 | +6.20 | +2.23 | PASS | 28.81 |
| `flatten_h5_cut` | long | 5 | leftover | cut_loser | none | 45% | 63% | 3/27 | -2.75 | 20 | 106 | +8.90 | -8.31 | +6.20 | +2.23 | PASS | 28.81 |
| `flatten_h5_sizeup` | long | 5 | leftover | list | sizeup | 45% | 63% | 3/27 | -2.75 | 20 | 106 | +8.90 | -8.31 | +6.20 | +2.23 | PASS | 28.81 |
| `flatten_h5_time` | long | 5 | leftover | time | none | 45% | 63% | 3/27 | -2.75 | 20 | 106 | +8.90 | -8.31 | +6.20 | +2.23 | PASS | 28.81 |
| `flatten_h5_trail` | long | 5 | leftover | trail | none | 45% | 63% | 3/27 | -2.75 | 20 | 106 | +8.90 | -8.31 | +6.20 | +2.23 | PASS | 28.81 |
| `yday_gainer_h3` | long | 3 | leftover | list | none | 49% | 41% | 6/27 | -1.01 | 29 | 123 | +9.11 | -7.48 | -5.41 | +36.33 | PASS | 28.67 |
| `union_h3_topheavy` | long | 3 | topheavy | list | none | 44% | 44% | 5/27 | -4.32 | 19 | 109 | +7.54 | -5.91 | -1.79 | +10.70 | PASS | 28.628 |
| `short_macd_dn_h1` | short | 1 | leftover | list | none | 51% | 26% | 2/27 | -5.20 | 12 | 58 | +6.22 | -4.40 | -8.82 | -3.45 | PASS | 28.543 |
| `flatten_h5_sboost` | long | 5 | leftover | list | both | 45% | 63% | 3/27 | -2.75 | 20 | 106 | +8.68 | -8.12 | +3.77 | +2.23 | PASS | 28.425 |
| `flatten_h1` | long | 1 | leftover | list | none | 42% | 37% | 1/27 | -6.77 | 4 | 30 | +4.26 | -3.42 | +0.10 | -5.80 | PASS | 28.268 |
| `union_h5_half` | long | 5 | half | list | none | 46% | 59% | 4/27 | -3.45 | 30 | 149 | +7.93 | -7.20 | +1.60 | +19.96 | PASS | 28.117 |
| `union_earn_react_h1` | long | 1 | leftover | list | none | 48% | 30% | 7/27 | -0.33 | 7 | 49 | +4.63 | -7.25 | +0.41 | -0.44 | PASS | 27.982 |
| `union_last_green_h3` | long | 3 | leftover | list | none | 51% | 37% | 4/27 | -5.28 | 31 | 109 | +6.67 | -7.48 | -2.04 | +39.76 | PASS | 27.681 |
| `union_macd_hist_h1` | long | 1 | leftover | list | none | 44% | 37% | 5/27 | -1.39 | 11 | 71 | +3.81 | -4.11 | -6.58 | -8.14 | PASS | 27.294 |
| `union_h1_half` | long | 1 | half | list | none | 45% | 37% | 0/27 | -1.87 | 9 | 46 | +4.41 | -3.88 | -1.09 | +8.67 | PASS | 27.269 |
| `union_news_g_cam71_h1` | long | 1 | leftover | list | none | 38% | 37% | 5/27 | -9.35 | 1 | 5 | +2.77 | -4.22 | -9.35 | -5.55 | PASS | 27.12 |
| `union_news_present_h3` | long | 3 | leftover | list | none | 43% | 44% | 5/27 | -4.70 | 17 | 106 | +7.50 | -5.56 | -8.58 | +2.66 | PASS | 27.116 |
| `yday_gainer_h5` | long | 5 | leftover | list | none | 49% | 48% | 6/27 | -5.77 | 39 | 154 | +8.56 | -9.87 | -0.30 | +27.11 | PASS | 27.095 |
| `probable_probable_ok_h3` | long | 3 | leftover | list | none | 51% | 26% | 9/27 | -1.76 | 21 | 65 | +6.64 | -7.29 | -19.86 | +15.44 | PASS | 27.088 |
| `union_h3_exit_red` | long | 3 | leftover | list | none | 49% | 37% | 4/27 | -5.48 | 31 | 109 | +6.74 | -6.59 | -5.82 | +25.14 | PASS | 27.021 |
| `short_alarm_h1` | short | 1 | leftover | list | none | 52% | 19% | 3/27 | -0.84 | 7 | 47 | +4.04 | -3.54 | +0.30 | +4.54 | PASS | 26.892 |
| `combo_nf_5050_shared` | mix | 5 | leftover | list | none | 42% | 52% | 1/27 | -6.75 | 0 | 0 | +6.62 | -5.48 | +0.44 | — | PASS | 26.842 |
| `union_macd_xup_h1` | long | 1 | leftover | list | none | 37% | 30% | 8/27 | -1.26 | 10 | 31 | +3.46 | -4.11 | -12.47 | -15.61 | PASS | 26.77 |
| `union_h5_rankw` | long | 5 | rank_w | list | none | 46% | 59% | 4/27 | -6.43 | 30 | 149 | +7.69 | -8.54 | -1.89 | +19.96 | PASS | 26.579 |
| `union_blue_coil_h3` | long | 3 | leftover | list | none | 43% | 44% | 4/27 | -6.89 | 18 | 89 | +6.70 | -5.75 | -7.65 | -20.99 | PASS | 26.471 |
| `flatten_h3_sboost` | long | 3 | leftover | list | both | 45% | 41% | 2/27 | -11.83 | 12 | 73 | +7.38 | -5.10 | -8.73 | +2.16 | PASS | 26.349 |
| `flatten_h3` | long | 3 | leftover | list | none | 45% | 41% | 2/27 | -11.83 | 12 | 73 | +7.64 | -5.32 | -8.68 | +2.16 | PASS | 26.306 |
| `flatten_h3_cut` | long | 3 | leftover | cut_loser | none | 45% | 41% | 2/27 | -11.83 | 12 | 73 | +7.64 | -5.32 | -8.68 | +2.16 | PASS | 26.306 |
| `flatten_h3_sizeup` | long | 3 | leftover | list | sizeup | 45% | 41% | 2/27 | -11.83 | 12 | 73 | +7.64 | -5.32 | -8.68 | +2.16 | PASS | 26.306 |
| `flatten_h3_trail` | long | 3 | leftover | trail | none | 45% | 41% | 2/27 | -11.83 | 12 | 73 | +7.64 | -5.32 | -8.68 | +2.16 | PASS | 26.306 |
| `union_join_g_h3` | long | 3 | leftover | list | none | 44% | 33% | 5/27 | -7.50 | 21 | 110 | +7.86 | -5.76 | -5.84 | +1.93 | PASS | 26.009 |
| `union_coil_green_h1` | long | 1 | leftover | list | none | 40% | 33% | 3/27 | -3.18 | 13 | 42 | +3.49 | -4.10 | -3.79 | -8.46 | PASS | 25.989 |
| `union_white_h5` | long | 5 | leftover | list | none | 45% | 59% | 3/27 | -7.73 | 18 | 70 | +9.83 | -8.67 | -1.16 | +45.21 | PASS | 25.971 |
| `union_blue_h3` | long | 3 | leftover | list | none | 43% | 41% | 5/27 | -6.52 | 21 | 96 | +7.02 | -6.19 | -10.73 | -2.58 | PASS | 25.946 |
| `flatten_h3_time` | long | 3 | leftover | time | none | 45% | 41% | 2/27 | -11.12 | 12 | 73 | +6.99 | -5.50 | -5.72 | +2.16 | PASS | 25.915 |
| `union_cond_h1` | long | 1 | leftover | list | none | 48% | 30% | 0/27 | -4.79 | 4 | 41 | +3.93 | -3.80 | -6.67 | -0.10 | PASS | 25.832 |
| `union_white_both_n4_h3` | long | 3 | leftover | list | none | 45% | 30% | 2/27 | -7.83 | 11 | 25 | +8.85 | -5.82 | -1.05 | +21.90 | PASS | 25.787 |
| `union_flow_in_h3` | long | 3 | leftover | list | none | 47% | 33% | 6/27 | -1.41 | 7 | 33 | +3.27 | -6.00 | -0.82 | -1.37 | PASS | 25.757 |
| `flatten_h3_half` | long | 3 | half | list | none | 45% | 41% | 1/27 | -5.63 | 12 | 73 | +6.81 | -4.98 | -4.73 | +2.16 | PASS | 25.626 |
| `union_ab_g_h1` | long | 1 | leftover | list | none | 43% | 33% | 0/27 | -4.72 | 8 | 39 | +3.84 | -3.37 | -0.47 | -3.05 | PASS | 25.589 |
| `flatten_h5_rankw` | long | 5 | rank_w | list | none | 45% | 52% | 3/27 | -2.98 | 20 | 106 | +8.25 | -8.53 | +2.85 | +2.23 | PASS | 25.554 |
| `flatten_h3_rankw` | long | 3 | rank_w | list | none | 45% | 37% | 2/27 | -8.12 | 12 | 73 | +7.45 | -5.47 | -8.12 | +2.16 | PASS | 25.28 |
| `union_news_pack_net2_h3` | long | 3 | leftover | list | none | 40% | 30% | 10/27 | -0.14 | 3 | 27 | +5.95 | -7.59 | -11.73 | -9.16 | PASS | 24.801 |
| `union_news_g_cam71_conv_h1` | long | 1 | conviction | list | none | 35% | 30% | 5/27 | -7.76 | 1 | 4 | +2.69 | -4.46 | -6.66 | -7.53 | PASS | 24.783 |
| `union_macd_xup_h3` | long | 3 | leftover | list | none | 36% | 33% | 12/27 | -0.63 | 20 | 69 | +6.08 | -8.29 | -17.73 | -52.37 | PASS | 24.771 |
| `union_news_or_net2_h1` | long | 1 | leftover | list | none | 45% | 30% | 0/27 | -3.88 | 1 | 26 | +4.06 | -3.93 | -4.24 | -0.22 | PASS | 24.639 |
| `union_news_vol_h1` | long | 1 | leftover | list | none | 46% | 26% | 0/27 | -4.12 | 5 | 19 | +4.21 | -4.29 | -2.53 | +14.65 | PASS | 24.562 |
| `union_news_g_cam71_n2_h1` | long | 1 | topheavy | list | none | 38% | 30% | 5/27 | -11.26 | 1 | 3 | +2.42 | -4.36 | -13.95 | -9.09 | PASS | 24.36 |
| `union_flow_in_h5` | long | 5 | leftover | list | none | 51% | 41% | 4/27 | -1.30 | 8 | 46 | +3.82 | -8.58 | +5.98 | -13.39 | PASS | 24.122 |
| `short_rsi_ob_h1` | short | 1 | leftover | list | none | 49% | 30% | 1/27 | -6.09 | 17 | 57 | +3.89 | -5.15 | -13.10 | -14.24 | PASS | 24.096 |
| `union_news_or_net4_h1` | long | 1 | leftover | list | none | 45% | 33% | 0/27 | -13.14 | 1 | 20 | +3.76 | -3.86 | -10.27 | -4.68 | PASS | 24.022 |
| `flatten_h3_topheavy` | long | 3 | topheavy | list | none | 45% | 37% | 2/27 | -8.14 | 12 | 73 | +5.86 | -5.49 | -7.17 | +2.16 | PASS | 23.947 |
| `short_last_red_h3` | short | 3 | leftover | list | none | 56% | 41% | 2/27 | -6.08 | 19 | 121 | +5.13 | -7.12 | -11.79 | -15.98 | PASS | 23.939 |
| `union_e_green_h3` | long | 3 | leftover | list | none | 40% | 33% | 12/27 | +0.00 | 5 | 47 | +5.47 | -8.76 | -0.88 | -21.98 | PASS | 23.927 |
| `flatten_h5_half` | long | 5 | half | list | none | 45% | 52% | 1/27 | -5.27 | 20 | 106 | +7.81 | -7.00 | -0.64 | +2.23 | PASS | 23.923 |
| `union_white_any_h5` | long | 5 | leftover | list | none | 40% | 59% | 3/27 | -9.59 | 16 | 67 | +9.10 | -8.55 | +0.66 | -6.16 | PASS | 23.783 |
| `union_e_green_h1` | long | 1 | leftover | list | none | 47% | 30% | 5/27 | -3.13 | 3 | 18 | +4.06 | -8.96 | -22.25 | -18.85 | PASS | 23.674 |
| `union_blue_vol_h1` | long | 1 | leftover | list | none | 45% | 33% | 3/27 | -8.74 | 11 | 50 | +5.31 | -7.79 | -11.27 | +1.01 | PASS | 23.46 |
| `short_extended_h1` | short | 1 | leftover | list | none | 52% | 26% | 0/27 | -11.31 | 32 | 63 | +4.98 | -6.64 | -18.32 | -14.75 | PASS | 23.177 |
| `union_cond_n4_h3` | long | 3 | leftover | list | none | 42% | 44% | 5/27 | -8.15 | 7 | 57 | +5.18 | -7.66 | -7.51 | -6.34 | PASS | 22.86 |
| `short_macd_dn_h3` | short | 3 | leftover | list | none | 49% | 44% | 2/27 | -3.29 | 24 | 117 | +4.45 | -6.46 | -9.94 | -19.27 | PASS | 22.787 |
| `combo_fer_5050_shared` | mix | 5 | leftover | list | none | 39% | 48% | 3/27 | -3.41 | 0 | 0 | +7.84 | -8.14 | +6.62 | — | PASS | 22.452 |
| `short_extended_h3` | short | 3 | leftover | list | none | 54% | 41% | 0/27 | -12.62 | 53 | 132 | +8.34 | -9.98 | -16.26 | -36.84 | PASS | 22.321 |
| `short_clk_ext_veto_h3` | short | 3 | leftover | list | none | 51% | 44% | 0/27 | -7.87 | 30 | 106 | +7.64 | -7.84 | -11.79 | -96.25 | PASS | 21.992 |
| `union_vol_g_h1` | long | 1 | leftover | list | none | 48% | 33% | 0/27 | -10.94 | 15 | 64 | +5.58 | -7.86 | -10.45 | +4.03 | PASS | 21.971 |
| `union_last_red_h1` | long | 1 | leftover | list | none | 50% | 30% | 0/27 | -7.60 | 9 | 70 | +3.53 | -5.26 | -8.72 | +6.28 | PASS | 21.636 |
| `union_news_vol_h3` | long | 3 | leftover | list | none | 48% | 44% | 1/27 | -10.16 | 9 | 51 | +4.25 | -5.14 | -11.46 | +189.36 | PASS | 21.497 |
| `short_rsi_ob_h3` | short | 3 | leftover | list | none | 52% | 33% | 2/27 | -5.80 | 33 | 126 | +5.60 | -7.91 | -11.40 | -125.81 | PASS | 21.433 |
| `union_news_or_net5_h1` | long | 1 | leftover | list | none | 41% | 30% | 0/27 | -13.78 | 1 | 12 | +3.17 | -4.10 | -11.86 | -4.55 | PASS | 21.178 |
| `union_white_yday_h3` | long | 3 | leftover | list | none | 43% | 41% | 2/27 | -10.96 | 6 | 42 | +5.96 | -6.82 | -10.01 | -8.88 | PASS | 21.079 |
| `union_white_any_h1` | long | 1 | leftover | list | none | 36% | 30% | 0/27 | -9.03 | 4 | 23 | +5.63 | -4.37 | -9.75 | -10.30 | PASS | 21.019 |
| `union_news_pack_h3` | long | 3 | leftover | list | none | 40% | 30% | 7/27 | -2.70 | 5 | 31 | +5.43 | -8.52 | -13.43 | -16.25 | PASS | 20.921 |
| `short_clk_neg_weak_fail_h3` | short | 3 | leftover | list | none | 50% | 37% | 1/27 | -6.63 | 23 | 114 | +6.39 | -6.46 | -11.13 | -29.81 | PASS | 20.915 |
| `union_cond_h3` | long | 3 | leftover | list | none | 42% | 44% | 2/27 | -9.18 | 14 | 117 | +5.96 | -6.82 | -8.34 | -13.87 | PASS | 20.85 |
| `ohlc_hot_h5` | long | 5 | leftover | list | none | 43% | 41% | 6/27 | -5.75 | 46 | 171 | +8.36 | -9.66 | -10.70 | +58.51 | PASS | 20.607 |
| `union_news_g_conv_h3` | long | 3 | conviction | list | none | 48% | 37% | 2/27 | -12.38 | 6 | 50 | +4.65 | -7.55 | -13.01 | +215.45 | PASS | 20.484 |
| `union_news_or_net3_h1` | long | 1 | leftover | list | none | 44% | 26% | 0/27 | -14.71 | 1 | 24 | +3.72 | -4.17 | -16.11 | -7.82 | PASS | 20.465 |
| `ohlc_hot_h3` | long | 3 | leftover | list | none | 45% | 44% | 2/27 | -3.35 | 33 | 144 | +5.72 | -7.53 | -3.35 | +77.30 | PASS | 20.456 |
| `flatten_live_h3` | long | 3 | leftover | list | none | 58% | 11% | 0/27 | -6.24 | 4 | 14 | +8.60 | -8.85 | -6.24 | +6.87 | PASS | 20.313 |
| `union_white_h3` | long | 3 | leftover | list | none | 41% | 41% | 3/27 | -15.18 | 11 | 59 | +7.34 | -6.61 | -15.03 | +10.56 | PASS | 20.028 |
| `union_news_or_net4_h3` | long | 3 | leftover | list | none | 42% | 33% | 2/27 | -21.12 | 7 | 44 | +6.05 | -5.32 | -21.12 | -19.01 | PASS | 19.862 |
| `union_candle_h3` | long | 3 | leftover | list | none | 46% | 26% | 2/27 | -7.48 | 27 | 115 | +6.66 | -7.14 | -10.13 | +13.22 | PASS | 19.587 |
| `probable_h5` | long | 5 | leftover | list | none | 48% | 33% | 5/27 | -7.98 | 36 | 158 | +6.61 | -10.49 | -15.16 | +1.70 | PASS | 18.616 |
| `union_news_pack_net3_h3` | long | 3 | leftover | list | none | 37% | 22% | 7/27 | -4.46 | 3 | 25 | +6.18 | -7.59 | -15.53 | -16.71 | PASS | 18.532 |
| `union_news_or_net2_h3` | long | 3 | leftover | list | none | 43% | 30% | 2/27 | -12.40 | 8 | 65 | +6.08 | -5.90 | -16.15 | -12.82 | PASS | 18.471 |
| `union_coil_off_h1` | long | 1 | leftover | list | none | 40% | 26% | 0/27 | -6.86 | 8 | 56 | +3.15 | -4.21 | -13.07 | -13.91 | PASS | 18.203 |
| `union_news_g_cam61_h1` | long | 1 | leftover | list | none | 35% | 19% | 4/27 | -13.27 | 1 | 12 | +2.89 | -4.73 | -15.97 | -10.18 | PASS | 18.154 |
| `union_ab_g_h3` | long | 3 | leftover | list | none | 39% | 26% | 2/27 | -13.16 | 17 | 92 | +7.49 | -5.59 | -13.84 | -12.48 | PASS | 18.114 |
| `union_blue_coil_h1` | long | 1 | leftover | list | none | 41% | 22% | 0/27 | -11.64 | 9 | 41 | +3.88 | -6.92 | -14.12 | -18.03 | PASS | 17.866 |
| `union_rsi_h1` | long | 1 | leftover | list | none | 44% | 37% | 0/27 | -16.80 | 10 | 81 | +4.56 | -8.81 | -20.69 | -16.40 | PASS | 17.86 |
| `union_news_g_h3` | long | 3 | leftover | list | none | 48% | 26% | 2/27 | -14.77 | 18 | 87 | +4.60 | -6.54 | -22.26 | +165.49 | PASS | 17.817 |
| `union_vol_g_h5` | long | 5 | leftover | list | none | 47% | 41% | 0/27 | -4.40 | 35 | 143 | +12.33 | -10.84 | -12.03 | +8.64 | PASS | 17.756 |
| `union_rsi_os_h3` | long | 3 | leftover | list | none | 48% | 41% | 6/27 | -3.14 | 4 | 49 | +5.13 | -9.41 | -21.89 | -34.84 | PASS | 17.594 |
| `union_white_any_h3` | long | 3 | leftover | list | none | 41% | 37% | 2/27 | -15.18 | 11 | 57 | +6.71 | -7.32 | -12.96 | -5.55 | PASS | 17.451 |
| `union_coil_green_h3` | long | 3 | leftover | list | none | 40% | 30% | 2/27 | -8.03 | 27 | 117 | +5.83 | -6.96 | -6.04 | -12.64 | PASS | 17.423 |
| `short_clk_neg_weak_fail_opp_h3` | short | 3 | leftover | list | none | 49% | 41% | 1/27 | -6.88 | 11 | 61 | +5.22 | -6.16 | -24.94 | -53.88 | PASS | 17.219 |
| `union_news_or_h3` | long | 3 | leftover | list | none | 46% | 22% | 2/27 | -14.67 | 15 | 90 | +5.55 | -5.94 | -18.85 | +164.99 | PASS | 16.918 |
| `union_rsi_os_h1` | long | 1 | leftover | list | none | 51% | 33% | 0/27 | -14.09 | 3 | 27 | +2.45 | -7.05 | -26.51 | +2.11 | PASS | 16.887 |
| `union_clk_flow_coil_h1` | long | 1 | leftover | list | none | 20% | 26% | 0/27 | -8.70 | 1 | 4 | +5.65 | -3.36 | -10.57 | -18.42 | PASS | 16.771 |
| `union_news_head_h3` | long | 3 | leftover | list | none | 46% | 19% | 2/27 | -14.26 | 17 | 72 | +5.42 | -5.88 | -23.43 | +171.90 | PASS | 16.76 |
| `union_news_g_cond_h3` | long | 3 | leftover | list | none | 46% | 22% | 2/27 | -14.67 | 15 | 90 | +5.55 | -6.11 | -19.53 | +163.38 | PASS | 16.68 |
| `flatten_live_h5` | long | 5 | leftover | list | none | 46% | 26% | 0/27 | -5.51 | 5 | 16 | +9.01 | -11.62 | -5.51 | +5.13 | PASS | 16.358 |
| `union_news_g_cam71_h3` | long | 3 | leftover | list | none | 41% | 30% | 2/27 | -18.59 | 1 | 16 | +4.13 | -7.28 | -19.09 | -16.16 | PASS | 16.339 |
| `flatten_vol_g_h3` | long | 3 | leftover | list | none | 51% | 19% | 1/27 | -20.36 | 5 | 25 | +9.63 | -9.35 | -28.24 | -23.51 | PASS | 16.118 |
| `union_news_or_net5_h3` | long | 3 | leftover | list | none | 41% | 33% | 2/27 | -22.15 | 4 | 32 | +3.76 | -5.74 | -26.18 | -20.43 | PASS | 16.042 |
| `union_news_or_net3_h3` | long | 3 | leftover | list | none | 40% | 26% | 2/27 | -21.39 | 7 | 52 | +5.92 | -5.99 | -23.66 | -27.49 | PASS | 15.762 |
| `union_rsi_h3` | long | 3 | leftover | list | none | 45% | 37% | 0/27 | -6.66 | 19 | 149 | +7.91 | -8.41 | -13.18 | -19.04 | PASS | 15.625 |
| `union_white_coil_h3` | long | 3 | leftover | list | none | 40% | 33% | 0/27 | -8.76 | 9 | 46 | +6.49 | -7.02 | -8.03 | -13.26 | PASS | 15.616 |
| `overnight_mega_h1` | long | 1 | leftover | list | none | 31% | 11% | 0/27 | +0.00 | 0 | 5 | +5.65 | -5.19 | -5.76 | -14.12 | PASS | 15.26 |
| `overnight_mega_h2` | long | 2 | leftover | list | none | 50% | 7% | 1/27 | +0.00 | 0 | 12 | +3.54 | -6.97 | -7.35 | -6.81 | PASS | 14.614 |
| `union_macd_hist_h3` | long | 3 | leftover | list | none | 36% | 37% | 1/27 | -6.88 | 21 | 145 | +5.24 | -5.68 | -6.67 | -38.03 | PASS | 14.414 |
| `union_news_g_cam61_h3` | long | 3 | leftover | list | none | 44% | 19% | 2/27 | -20.08 | 2 | 26 | +4.04 | -6.36 | -23.03 | -26.66 | PASS | 14.231 |
| `union_news_g_h5` | long | 5 | leftover | list | none | 40% | 37% | 2/27 | -7.34 | 20 | 119 | +5.21 | -8.60 | -14.43 | +107.26 | PASS | 13.719 |
| `coil_h3_exit_alarm` | long | 3 | leftover | list | none | 37% | 26% | 2/27 | -10.45 | 17 | 121 | +4.50 | -7.13 | -10.05 | -12.59 | PASS | 13.233 |
| `union_coil_off_h3` | long | 3 | leftover | list | none | 37% | 26% | 2/27 | -11.90 | 21 | 120 | +4.85 | -8.26 | -15.12 | -20.98 | PASS | 12.639 |
| `union_coil_off_h5` | long | 5 | leftover | list | none | 40% | 41% | 2/27 | -13.20 | 28 | 153 | +3.69 | -54.08 | -9.02 | -31.64 | PASS | 12.224 |
| `overnight_h1` | long | 1 | leftover | list | none | 32% | 26% | 0/27 | -11.08 | 1 | 47 | +6.32 | -9.38 | -16.15 | -19.44 | PASS | 11.671 |
| `union_rsi_os_h5` | long | 5 | leftover | list | none | 40% | 33% | 5/27 | -2.96 | 5 | 52 | +4.64 | -14.37 | -28.10 | -52.07 | PASS | 9.251 |
| `union_news_both_h1` *(thin)* | long | 1 | leftover | list | none | 80% | 11% | 21/27 | +7.29 | 0 | 0 | +3.69 | -0.73 | +7.29 | +4.21 | PASS | 59.761 |
| `union_rsi_os_macd_h3` *(thin)* | long | 3 | leftover | list | none | 67% | 15% | 24/27 | +3.24 | 2 | 4 | +8.24 | -5.88 | +3.24 | +18.32 | PASS | 41.003 |
| `union_news_both_h3` *(thin)* | long | 3 | leftover | list | none | 20% | 15% | 21/27 | +4.04 | 0 | 2 | +4.37 | -0.51 | +4.04 | +1.48 | PASS | 28.012 |
| `union_catal_present_h1` *(thin)* | long | 1 | leftover | list | none | 30% | 15% | 10/27 | -5.37 | 0 | 2 | +8.76 | -2.82 | +1.03 | -2.59 | PASS | 15.377 |
| `union_clk_nr7_mom_opp_h1` *(thin)* | long | 1 | leftover | list | none | 75% | 11% | 12/27 | +0.00 | 0 | 4 | +4.03 | -2.38 | +10.45 | +0.01 | PASS | 13.35 |
| `union_flow_in_white_h1` *(thin)* | long | 1 | leftover | list | none | 54% | 19% | 2/27 | -2.57 | 0 | 2 | +3.42 | -2.21 | +5.23 | +8.08 | PASS | 12.56 |
| `union_news_missing_h1` *(thin)* | long | 1 | leftover | list | none | 50% | 4% | 1/27 | +0.00 | 0 | 2 | +7.93 | -3.05 | +1.44 | +1.88 | PASS | 9.891 |
| `union_vol_missing_h1` *(thin)* | long | 1 | leftover | list | none | 50% | 4% | 1/27 | +0.00 | 0 | 2 | +7.93 | -3.05 | +1.44 | +1.88 | PASS | 9.891 |
| `short_news_pack_h3` *(thin)* | short | 3 | leftover | list | none | 47% | 30% | 9/27 | +0.00 | 3 | 7 | +3.39 | -7.55 | +0.61 | -6.15 | PASS | 9.83 |
| `union_news_missing_h3` *(thin)* | long | 3 | leftover | list | none | 50% | 11% | 1/27 | +0.00 | 1 | 5 | +11.98 | -3.98 | +3.58 | +5.59 | PASS | 8.059 |
| `union_vol_missing_h3` *(thin)* | long | 3 | leftover | list | none | 50% | 11% | 1/27 | +0.00 | 1 | 5 | +11.98 | -3.98 | +3.58 | +5.59 | PASS | 8.059 |
| `union_news_g_cam91_n1_h1` *(thin)* | long | 1 | leftover | list | none | 50% | 4% | 0/27 | -2.85 | 0 | 0 | +0.85 | -3.84 | -2.85 | -0.09 | PASS | 1.417 |
| `union_catal_present_h3` *(thin)* | long | 3 | leftover | list | none | 30% | 19% | 0/27 | -4.69 | 2 | 6 | +12.05 | -4.43 | -0.74 | -12.69 | PASS | 0.193 |
| `union_clk_earn_guide_react_h1` *(thin)* | long | 1 | leftover | list | none | 45% | 7% | 0/27 | -3.11 | 1 | 4 | +5.68 | -6.17 | -5.43 | +10.90 | PASS | -2.451 |
| `union_flow_in_white_h3` *(thin)* | long | 3 | leftover | list | none | 46% | 15% | 1/27 | -2.72 | 0 | 6 | +3.30 | -4.79 | -0.57 | +2.11 | PASS | -3.524 |
| `overnight_mega_green_h1` *(thin)* | long | 1 | leftover | list | none | 35% | 11% | 0/27 | +0.00 | 0 | 2 | +2.51 | -6.44 | -8.87 | -9.06 | PASS | -5.399 |
| `union_rsi_os_macd_h1` *(thin)* | long | 1 | leftover | list | none | 33% | 7% | 1/27 | -7.76 | 1 | 3 | +3.83 | -4.14 | -7.76 | -3.77 | PASS | -8.309 |
| `union_clk_insider_cash_stab_h3` *(thin)* | long | 3 | leftover | list | none | 29% | 4% | 0/27 | -1.93 | 1 | 2 | +2.33 | -3.20 | -5.00 | -4.58 | PASS | -8.513 |
| `short_r_down_h1` *(thin)* | short | 1 | leftover | list | none | — | 0% | 0/27 | +0.00 | 0 | 0 | — | — | +0.00 | +0.00 | PASS | -15.0 |
| `short_r_down_h3` *(thin)* | short | 3 | leftover | list | none | — | 0% | 0/27 | +0.00 | 0 | 0 | — | — | +0.00 | +0.00 | PASS | -15.0 |
| `union_clk_r_up_coil_h1` *(thin)* | long | 1 | leftover | list | none | — | 0% | 0/27 | +0.00 | 0 | 0 | — | — | +0.00 | +0.00 | PASS | -15.0 |
| `union_r_up_h1` *(thin)* | long | 1 | leftover | list | none | — | 0% | 0/27 | +0.00 | 0 | 0 | — | — | +0.00 | +0.00 | PASS | -15.0 |
| `union_r_up_h3` *(thin)* | long | 3 | leftover | list | none | — | 0% | 0/27 | +0.00 | 0 | 0 | — | — | +0.00 | +0.00 | PASS | -15.0 |
