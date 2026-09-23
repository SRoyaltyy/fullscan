# Factor strategy mine — 2026-08-13 → 2026-09-23

Leak-free 09:30 recipes: **339** · candidate rows **2453** · fill `09:30 open, whole shares, Futubull fees, leftover split, sell first, hard-red sit, cash+holdings audit`.

Cash book: $10k, whole shares, Futubull fees, leftover split, sell first, min-hold, 09:30 open, hard-red S≤−3 sit, shorts marked as a liability. Each session starts from leftover cash and lots actually held (butterfly). Cash-start buttons wake a sleeve on date X with $10k and no lots (same rules). Stock investigator quotes 09:30 cameras / coaches / news from repo files. Size / sell / S-boost tweaks sit on the same ledger. Signal-only % is the old equal-weight path (not a fill). `flatten_h*` = wish-list (io/HOLD mornings still buy). `flatten_live_*` = only when the live flatten gate fires. Research only — does not change live `flatten_robust`.

Scoreboard files: slim `factor_mine.json` (stats / recipes / series) plus gzip shards in `factor_mine/shards/` (`books`, `starts`, `daily`, `probe`, `sim`). One file used to be 104MB and GitHub rejected the publish (100MB cap).

Combination books: **79** mixes on the same $10k cash ledger (shared leftover or split sleeves, official 09:30 / 16:00, hard-red sit, owner min-hold). Outperformers: `combo_sh_macd_5050_shared`, `combo_sh_3070_shared`, `combo_se_5050_skip`, `combo_sh_5050_shared`, `combo_jer_5050_shared`, `combo_sj_3070_shared`, `combo_sf_7030_shared`, `combo_sf_5050_shared`, `combo_sf_3070_shared`.

A combo outperforms its members on the cash book when the audit passes and either (1) Book% is strictly higher than every member, or (2) Book% stays within 2pp of the richest member, max drawdown is strictly smaller than every member's drawdown, start-rate is at least the best member, both halves are green, and the worst session is no worse than the worst member session. Effectiveness / Signal% do not decide WIN.

Action blotters: [FACTOR_MINE_ACTION.md](FACTOR_MINE_ACTION.md).

| Strategy | Side | H | Size | Sell | Boost | Win% | $ days | Starts YES | Med start | Top-g | Losers | AvgW | AvgL | Book% | Signal% | Audit | Eff |
|---|---|---:|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---:|
| `combo_sh_5050_shared` | mix | 5 | leftover | list | none | 58% | 72% | 24/29 | +7.37 | 0 | 0 | +6.29 | -5.77 | +32.15 | — | PASS | 72.005 |
| `combo_sh_macd_5050_shared` | mix | 5 | leftover | list | none | 57% | 55% | 26/29 | +13.46 | 0 | 0 | +6.51 | -5.33 | +37.03 | — | PASS | 70.753 |
| `combo_sh_3070_shared` | mix | 5 | leftover | list | none | 57% | 62% | 24/29 | +10.72 | 0 | 0 | +6.33 | -5.68 | +34.69 | — | PASS | 70.041 |
| `combo_se_5050_skip` | mix | 5 | leftover | list | none | 55% | 62% | 25/29 | +5.12 | 0 | 0 | +6.41 | -7.80 | +33.86 | — | PASS | 65.248 |
| `combo_sj_3070_shared` | mix | 5 | leftover | list | none | 55% | 59% | 11/29 | -0.60 | 0 | 0 | +4.87 | -4.74 | +20.56 | — | PASS | 44.881 |
| `combo_sf_7030_shared` | mix | 5 | leftover | list | none | 53% | 62% | 13/29 | -1.20 | 0 | 0 | +6.25 | -7.13 | +14.34 | — | PASS | 41.849 |
| `combo_sf_5050_shared` | mix | 5 | leftover | list | none | 52% | 62% | 8/29 | -2.78 | 0 | 0 | +6.50 | -6.58 | +13.47 | — | PASS | 37.6 |
| `combo_jer_5050_shared` | mix | 5 | leftover | list | none | 44% | 48% | 9/29 | -0.62 | 0 | 0 | +6.33 | -5.66 | +27.14 | — | PASS | 36.633 |
| `combo_sf_3070_shared` | mix | 5 | leftover | list | none | 49% | 59% | 4/29 | -3.11 | 0 | 0 | +6.85 | -6.46 | +9.16 | — | PASS | 31.769 |
| `union_hot_n4_holdup` | long | 1 | leftover | list | holdup | 51% | 55% | 26/29 | +21.98 | 21 | 39 | +10.07 | -6.27 | +52.85 | +63.05 | PASS | 73.814 |
| `union_hot_n4_h1` | long | 1 | leftover | list | none | 51% | 41% | 25/29 | +10.98 | 21 | 39 | +7.81 | -5.43 | +30.77 | +63.05 | PASS | 67.974 |
| `union_clk_mom_break_peer_opp_h1` | long | 1 | leftover | list | none | 53% | 38% | 26/29 | +3.76 | 11 | 26 | +3.90 | -3.20 | +17.04 | +20.16 | PASS | 66.971 |
| `combo_seh_333_skip` | mix | 5 | leftover | list | none | 50% | 62% | 25/29 | +6.73 | 0 | 0 | +7.13 | -6.80 | +32.50 | — | PASS | 66.162 |
| `short_news_r_h1` | short | 1 | leftover | list | none | 53% | 45% | 28/29 | +3.36 | 7 | 21 | +3.25 | -3.58 | +2.54 | +15.09 | PASS | 65.813 |
| `union_w_hot_cond_h1` | long | 1 | leftover | list | none | 50% | 48% | 27/29 | +5.23 | 30 | 62 | +6.00 | -5.82 | +5.47 | +26.68 | PASS | 65.674 |
| `combo_he1_5050_shared` | mix | 5 | leftover | list | none | 49% | 52% | 25/29 | +10.93 | 0 | 0 | +5.74 | -5.64 | +23.08 | — | PASS | 65.321 |
| `combo_oh_5050_shared` | mix | 5 | leftover | list | none | 51% | 41% | 25/29 | +10.84 | 0 | 0 | +7.29 | -5.44 | +14.65 | — | PASS | 65.094 |
| `union_hot_score_h1` | long | 1 | leftover | list | none | 50% | 45% | 26/29 | +8.02 | 37 | 69 | +6.54 | -5.85 | +11.03 | +39.02 | PASS | 64.897 |
| `combo_sh_7030_shared` | mix | 5 | leftover | list | none | 58% | 72% | 17/29 | +3.14 | 0 | 0 | +6.26 | -5.82 | +23.17 | — | PASS | 64.554 |
| `combo_ee1_3070_shared` | mix | 5 | leftover | list | none | 43% | 62% | 25/29 | +9.78 | 0 | 0 | +9.42 | -7.62 | +34.49 | — | PASS | 64.211 |
| `combo_ee1_5050_shared` | mix | 5 | leftover | list | none | 43% | 62% | 25/29 | +9.78 | 0 | 0 | +9.42 | -7.62 | +34.49 | — | PASS | 64.211 |
| `combo_ee1_7030_shared` | mix | 5 | leftover | list | none | 43% | 62% | 25/29 | +9.78 | 0 | 0 | +9.42 | -7.62 | +34.49 | — | PASS | 64.211 |
| `union_clk_nr7_mom_h1` | long | 1 | leftover | list | none | 51% | 31% | 27/29 | +12.53 | 3 | 10 | +5.18 | -4.11 | +13.58 | +29.04 | PASS | 63.971 |
| `combo_ej_5050_shared` | mix | 5 | leftover | list | none | 47% | 52% | 25/29 | +4.08 | 0 | 0 | +6.16 | -5.41 | +33.96 | — | PASS | 63.907 |
| `combo_seh_333_split` | mix | 5 | leftover | list | none | 49% | 62% | 25/29 | +4.13 | 0 | 0 | +7.13 | -6.78 | +19.86 | — | PASS | 63.785 |
| `union_oppset_h1` | long | 1 | leftover | list | none | 54% | 34% | 26/29 | +8.17 | 16 | 56 | +5.71 | -4.90 | +8.29 | +18.00 | PASS | 63.469 |
| `union_join_vol_green_h1` | long | 1 | leftover | list | none | 47% | 38% | 26/29 | +3.97 | 11 | 40 | +4.80 | -3.98 | +18.51 | +9.88 | PASS | 63.42 |
| `oppset_h1` | long | 1 | leftover | list | none | 54% | 34% | 26/29 | +7.45 | 17 | 56 | +6.04 | -5.06 | +6.53 | +15.93 | PASS | 63.214 |
| `combo_je1_5050_shared` | mix | 5 | leftover | list | none | 51% | 45% | 25/29 | +2.66 | 0 | 0 | +4.56 | -4.77 | +16.20 | — | PASS | 63.006 |
| `combo_p2s_5050_shared` | mix | 5 | leftover | list | none | 61% | 66% | 18/29 | +2.00 | 0 | 0 | +4.60 | -5.11 | +12.59 | — | PASS | 62.881 |
| `union_e_fresh_h3` | long | 3 | leftover | list | none | 46% | 62% | 25/29 | +9.78 | 20 | 92 | +9.42 | -7.62 | +34.49 | +3.19 | PASS | 62.787 |
| `union_hot_n12_h1` | long | 1 | leftover | list | none | 51% | 45% | 25/29 | +4.23 | 44 | 99 | +5.47 | -5.67 | +2.80 | +27.66 | PASS | 62.456 |
| `union_news_pack_net2_h1` | long | 1 | leftover | list | none | 53% | 41% | 21/29 | +9.38 | 1 | 12 | +3.77 | -3.37 | +21.18 | +11.59 | PASS | 62.255 |
| `combo_es_9010_shared` | mix | 5 | leftover | list | none | 45% | 62% | 25/29 | +5.67 | 0 | 0 | +7.34 | -6.63 | +31.90 | — | PASS | 62.223 |
| `union_w_hot_cond_h3` | long | 3 | leftover | list | none | 45% | 59% | 27/29 | +10.19 | 49 | 157 | +9.93 | -7.68 | +18.79 | +256.35 | PASS | 62.092 |
| `combo_ps_7030_shared` | mix | 5 | leftover | list | none | 61% | 62% | 18/29 | +3.48 | 0 | 0 | +4.44 | -5.36 | +14.42 | — | PASS | 61.99 |
| `union_clk_hold_vs_sector_opp_h1` | long | 1 | leftover | list | none | 47% | 38% | 26/29 | +8.91 | 6 | 36 | +4.99 | -3.82 | +12.24 | +1.86 | PASS | 61.679 |
| `union_hot_score_h3` | long | 3 | leftover | list | none | 43% | 59% | 27/29 | +8.91 | 55 | 166 | +12.18 | -8.35 | +18.16 | +276.05 | PASS | 61.408 |
| `combo_eh_5050_shared` | mix | 5 | leftover | list | none | 42% | 52% | 25/29 | +9.18 | 0 | 0 | +8.32 | -6.57 | +26.54 | — | PASS | 61.397 |
| `combo_eh_7030_shared` | mix | 5 | leftover | list | none | 39% | 52% | 25/29 | +9.01 | 0 | 0 | +8.77 | -6.37 | +27.43 | — | PASS | 61.157 |
| `combo_ehs_601525_shared` | mix | 5 | leftover | list | none | 46% | 52% | 24/29 | +3.55 | 0 | 0 | +7.37 | -6.49 | +26.29 | — | PASS | 60.956 |
| `union_clk_fresh_cat_coil_opp_h1` | long | 1 | leftover | list | none | 49% | 31% | 25/29 | +3.88 | 18 | 32 | +4.54 | -3.83 | +2.24 | +1.82 | PASS | 60.899 |
| `combo_seh_502525_split` | mix | 5 | leftover | list | none | 47% | 66% | 22/29 | +2.40 | 0 | 0 | +7.34 | -6.60 | +14.39 | — | PASS | 60.844 |
| `union_white_both_n4_h2` | long | 2 | leftover | list | none | 52% | 48% | 17/29 | +0.07 | 9 | 17 | +7.36 | -5.27 | +19.87 | +21.04 | PASS | 60.682 |
| `combo_ehs_702010_shared` | mix | 5 | leftover | list | none | 41% | 48% | 25/29 | +6.77 | 0 | 0 | +8.03 | -6.14 | +27.84 | — | PASS | 60.585 |
| `combo_seh_333_shared` | mix | 5 | leftover | list | none | 48% | 62% | 21/29 | +1.87 | 0 | 0 | +7.18 | -6.68 | +23.72 | — | PASS | 60.572 |
| `combo_seh_333_weather` | mix | 5 | leftover | list | none | 48% | 62% | 21/29 | +1.87 | 0 | 0 | +7.18 | -6.68 | +23.72 | — | PASS | 60.572 |
| `combo_ps_5050_shared` | mix | 5 | leftover | list | none | 61% | 66% | 16/29 | +0.30 | 0 | 0 | +4.47 | -5.29 | +10.81 | — | PASS | 60.509 |
| `union_news_pack_h1` | long | 1 | leftover | list | none | 52% | 45% | 21/29 | +6.91 | 2 | 15 | +3.56 | -3.88 | +16.67 | +10.47 | PASS | 60.445 |
| `union_ret_5_h3` | long | 3 | leftover | list | none | 46% | 52% | 26/29 | +10.94 | 50 | 166 | +9.67 | -7.99 | +28.56 | +54.78 | PASS | 60.377 |
| `union_vol_ab_h1` | long | 1 | leftover | list | none | 52% | 34% | 22/29 | +3.60 | 18 | 44 | +5.50 | -4.32 | +7.59 | +63.28 | PASS | 60.264 |
| `union_ret_5_h1` | long | 1 | leftover | list | none | 50% | 41% | 22/29 | +1.84 | 30 | 69 | +5.96 | -5.17 | +8.41 | +24.12 | PASS | 60.249 |
| `combo_es_8020_shared` | mix | 5 | leftover | list | none | 47% | 55% | 24/29 | +4.15 | 0 | 0 | +6.90 | -7.12 | +31.39 | — | PASS | 60.178 |
| `union_break10_h3` | long | 3 | leftover | list | none | 47% | 48% | 26/29 | +8.22 | 42 | 137 | +9.22 | -6.41 | +7.76 | +46.30 | PASS | 59.981 |
| `combo_eh_3070_shared` | mix | 5 | leftover | list | none | 42% | 45% | 25/29 | +7.84 | 0 | 0 | +8.17 | -6.75 | +25.11 | — | PASS | 59.652 |
| `combo_hj_5050_shared` | mix | 5 | leftover | list | none | 46% | 45% | 20/29 | +3.20 | 0 | 0 | +5.68 | -4.26 | +16.50 | — | PASS | 59.55 |
| `combo_hn_7030_shared` | mix | 5 | leftover | list | none | 48% | 45% | 20/29 | +4.34 | 0 | 0 | +5.31 | -4.99 | +17.39 | — | PASS | 59.347 |
| `union_w_hot_candle_h3` | long | 3 | leftover | list | none | 44% | 55% | 26/29 | +8.73 | 46 | 157 | +9.69 | -8.16 | +16.63 | +217.79 | PASS | 58.983 |
| `union_news_g_conv_h1` | long | 1 | conviction | list | none | 51% | 45% | 19/29 | +1.45 | 1 | 17 | +3.98 | -4.68 | +11.63 | +8.30 | PASS | 58.709 |
| `combo_en_7030_shared` | mix | 5 | leftover | list | none | 40% | 45% | 25/29 | +3.04 | 0 | 0 | +6.22 | -5.95 | +25.67 | — | PASS | 58.377 |
| `short_news_r_macd_h3` | short | 3 | leftover | list | none | 61% | 55% | 20/29 | +0.26 | 8 | 36 | +5.08 | -5.79 | +9.65 | +28.68 | PASS | 58.352 |
| `combo_se_3070_shared` | mix | 5 | leftover | list | none | 50% | 48% | 23/29 | +1.69 | 0 | 0 | +6.70 | -7.32 | +28.86 | — | PASS | 58.337 |
| `combo_hn_5050_shared` | mix | 5 | leftover | list | none | 49% | 45% | 18/29 | +1.00 | 0 | 0 | +5.38 | -4.88 | +12.58 | — | PASS | 57.531 |
| `short_alarm_h3` | short | 3 | leftover | list | none | 59% | 38% | 28/29 | +4.07 | 14 | 102 | +6.54 | -8.01 | +6.32 | +17.51 | PASS | 57.45 |
| `combo_seh_502525_shared` | mix | 5 | leftover | list | none | 47% | 69% | 16/29 | +0.38 | 0 | 0 | +7.25 | -6.63 | +21.85 | — | PASS | 57.217 |
| `combo_seh_451540_shared` | mix | 5 | leftover | list | none | 47% | 69% | 16/29 | +2.11 | 0 | 0 | +7.29 | -6.86 | +21.75 | — | PASS | 56.892 |
| `union_vol_ab_h3` | long | 3 | leftover | list | none | 49% | 34% | 26/29 | +4.51 | 27 | 100 | +9.06 | -6.33 | +4.51 | +84.79 | PASS | 56.669 |
| `combo_seh_403525_shared` | mix | 5 | leftover | list | none | 47% | 62% | 16/29 | +0.72 | 0 | 0 | +7.30 | -6.55 | +23.12 | — | PASS | 56.128 |
| `union_join_vol_green_h3` | long | 3 | leftover | list | none | 50% | 38% | 26/29 | +7.31 | 21 | 87 | +7.67 | -6.87 | -0.32 | +46.70 | PASS | 55.974 |
| `combo_her_5050_shared` | mix | 5 | leftover | list | none | 42% | 52% | 22/29 | +4.49 | 0 | 0 | +7.83 | -7.81 | +17.60 | — | PASS | 55.758 |
| `union_candle_score_h3` | long | 3 | leftover | list | none | 45% | 45% | 26/29 | +5.28 | 30 | 144 | +6.12 | -6.26 | +10.13 | +31.95 | PASS | 55.42 |
| `union_break10_h1` | long | 1 | leftover | list | none | 50% | 41% | 18/29 | +0.66 | 27 | 52 | +5.04 | -4.93 | -4.27 | +12.28 | PASS | 55.353 |
| `combo_ef_7030_shared` | mix | 5 | leftover | list | none | 42% | 59% | 22/29 | +3.55 | 0 | 0 | +7.96 | -7.39 | +19.15 | — | PASS | 55.194 |
| `union_news_pack_net3_h1` | long | 1 | leftover | list | none | 52% | 34% | 16/29 | +1.32 | 1 | 11 | +3.56 | -3.37 | +12.40 | +7.24 | PASS | 54.655 |
| `combo_en_5050_shared` | mix | 5 | leftover | list | none | 44% | 48% | 19/29 | +0.68 | 0 | 0 | +5.88 | -6.09 | +22.23 | — | PASS | 54.584 |
| `union_macd_up_h3` | long | 3 | leftover | list | none | 41% | 45% | 24/29 | +3.96 | 32 | 132 | +8.13 | -6.15 | +6.57 | +25.77 | PASS | 54.311 |
| `union_macd_hist_h1` | long | 1 | leftover | list | none | 47% | 41% | 21/29 | +2.23 | 12 | 73 | +4.24 | -4.12 | -3.37 | -3.53 | PASS | 54.282 |
| `union_clk_mom_break_peer_h1` | long | 1 | leftover | list | none | 51% | 38% | 16/29 | +0.45 | 11 | 39 | +3.92 | -3.89 | +1.44 | +4.27 | PASS | 53.97 |
| `short_clk_ext_veto_opp_h3` | short | 3 | leftover | list | none | 53% | 45% | 22/29 | +1.07 | 12 | 54 | +7.31 | -6.12 | +3.00 | -17.29 | PASS | 53.835 |
| `union_e_fresh_h1` | long | 1 | leftover | list | none | 47% | 38% | 20/29 | +0.81 | 9 | 47 | +4.61 | -5.77 | +9.59 | +5.21 | PASS | 53.46 |
| `combo_se_5050_split` | mix | 5 | leftover | list | none | 51% | 52% | 17/29 | +1.84 | 0 | 0 | +7.02 | -7.07 | +17.36 | — | PASS | 53.077 |
| `union_w_hot_candle_h1` | long | 1 | leftover | list | none | 50% | 41% | 15/29 | +1.30 | 29 | 64 | +5.23 | -5.35 | +1.30 | +25.74 | PASS | 52.648 |
| `short_last_red_h1` | short | 1 | leftover | list | none | 49% | 31% | 19/29 | +0.31 | 13 | 71 | +4.61 | -4.47 | -7.99 | -4.66 | PASS | 50.665 |
| `combo_fse_333_shared` | mix | 5 | leftover | list | none | 46% | 59% | 16/29 | +0.99 | 0 | 0 | +6.67 | -7.25 | +14.34 | — | PASS | 50.554 |
| `combo_fes_403030_shared` | mix | 5 | leftover | list | none | 45% | 59% | 15/29 | +0.53 | 0 | 0 | +6.86 | -7.19 | +12.28 | — | PASS | 49.062 |
| `combo_eer_5050_shared` | mix | 5 | leftover | list | none | 40% | 45% | 19/29 | +3.41 | 0 | 0 | +8.77 | -8.96 | +19.19 | — | PASS | 47.826 |
| `union_vol_g_h3` | long | 3 | leftover | list | none | 46% | 48% | 21/29 | +2.59 | 30 | 131 | +7.94 | -13.77 | -1.99 | +23.75 | PASS | 47.771 |
| `combo_ef_5050_shared` | mix | 5 | leftover | list | none | 41% | 55% | 15/29 | +0.23 | 0 | 0 | +8.34 | -7.21 | +12.28 | — | PASS | 47.505 |
| `combo_fe_5050_shared` | mix | 5 | leftover | list | none | 41% | 55% | 15/29 | +0.23 | 0 | 0 | +8.34 | -7.21 | +12.28 | — | PASS | 47.505 |
| `union_news_g_conv_h3` | long | 3 | conviction | list | none | 49% | 41% | 18/29 | +1.58 | 7 | 54 | +4.27 | -7.18 | +0.97 | +222.06 | PASS | 47.321 |
| `union_clk_hold_vs_sector_h1` | long | 1 | leftover | list | none | 43% | 28% | 16/29 | +1.09 | 7 | 52 | +3.88 | -4.21 | +1.09 | -4.80 | PASS | 46.125 |
| `combo_sj_5050_shared` | mix | 5 | leftover | list | none | 55% | 59% | 12/29 | -1.75 | 0 | 0 | +4.86 | -4.75 | +16.66 | — | PASS | 45.129 |
| `combo_seh_404020_shared` | mix | 5 | leftover | list | none | 48% | 55% | 14/29 | -0.09 | 0 | 0 | +7.12 | -6.63 | +23.22 | — | PASS | 43.204 |
| `combo_se_5050_shared` | mix | 5 | leftover | list | none | 53% | 59% | 14/29 | -0.78 | 0 | 0 | +6.46 | -7.58 | +24.03 | — | PASS | 42.827 |
| `combo_se_5050_weather` | mix | 5 | leftover | list | none | 53% | 59% | 14/29 | -0.78 | 0 | 0 | +6.46 | -7.58 | +24.03 | — | PASS | 42.827 |
| `combo_sj_7030_shared` | mix | 5 | leftover | list | none | 52% | 59% | 11/29 | -1.00 | 0 | 0 | +4.95 | -4.69 | +9.52 | — | PASS | 42.293 |
| `union_e_green_h3` | long | 3 | leftover | list | none | 40% | 34% | 21/29 | +2.32 | 5 | 47 | +5.47 | -8.76 | +1.42 | -22.09 | PASS | 41.975 |
| `union_vol_green_h3` | long | 3 | leftover | list | none | 50% | 34% | 16/29 | +0.84 | 33 | 114 | +6.24 | -14.50 | -9.51 | +28.32 | PASS | 41.818 |
| `union_white_both_n4_h1` | long | 1 | leftover | list | none | 48% | 38% | 7/29 | -3.37 | 4 | 11 | +6.59 | -3.27 | +15.14 | +7.29 | PASS | 41.632 |
| `short_news_head_h3` | short | 3 | leftover | list | none | 57% | 62% | 12/29 | -1.53 | 10 | 42 | +5.58 | -6.48 | +4.62 | +1.11 | PASS | 40.956 |
| `combo_seh_601525_shared` | mix | 5 | leftover | list | none | 46% | 69% | 9/29 | -0.63 | 0 | 0 | +7.28 | -6.86 | +18.75 | — | PASS | 40.286 |
| `union_clk_fresh_cat_coil_h1` | long | 1 | leftover | list | none | 46% | 38% | 13/29 | -0.42 | 8 | 35 | +3.52 | -3.11 | -0.67 | -3.59 | PASS | 40.188 |
| `combo_jse_333_shared` | mix | 5 | leftover | list | none | 51% | 55% | 8/29 | -1.71 | 0 | 0 | +5.81 | -5.72 | +26.79 | — | PASS | 39.542 |
| `probable_h1` | long | 1 | leftover | list | none | 49% | 34% | 13/29 | -0.31 | 14 | 62 | +4.19 | -3.59 | +0.33 | +10.24 | PASS | 38.843 |
| `combo_hn_3070_shared` | mix | 5 | leftover | list | none | 49% | 52% | 7/29 | -3.32 | 0 | 0 | +5.49 | -4.80 | +7.36 | — | PASS | 38.714 |
| `combo_hf_5050_shared` | mix | 5 | leftover | list | none | 45% | 48% | 13/29 | -1.94 | 0 | 0 | +7.28 | -6.12 | +7.38 | — | PASS | 38.674 |
| `union_last_green_h1` | long | 1 | leftover | list | none | 44% | 41% | 9/29 | -0.90 | 15 | 45 | +4.55 | -3.27 | +6.06 | +12.99 | PASS | 38.428 |
| `union_h5_time` | long | 5 | leftover | time | none | 44% | 69% | 12/29 | -2.64 | 32 | 154 | +9.33 | -8.16 | +8.06 | +17.89 | PASS | 37.443 |
| `union_white_both_n4_h5_s12` | long | 5 | leftover | list | none | 41% | 55% | 12/29 | -0.82 | 13 | 32 | +8.53 | -7.09 | +20.63 | +21.47 | PASS | 37.334 |
| `union_h5` | long | 5 | leftover | list | none | 44% | 66% | 12/29 | -2.88 | 32 | 154 | +9.26 | -8.11 | +6.37 | +17.89 | PASS | 36.495 |
| `union_h5_cut` | long | 5 | leftover | cut_loser | none | 44% | 66% | 12/29 | -2.88 | 32 | 154 | +9.26 | -8.11 | +6.37 | +17.89 | PASS | 36.495 |
| `union_h5_sizeup` | long | 5 | leftover | list | sizeup | 44% | 66% | 12/29 | -2.88 | 32 | 154 | +9.26 | -8.11 | +6.37 | +17.89 | PASS | 36.495 |
| `union_h5_trail` | long | 5 | leftover | trail | none | 44% | 66% | 12/29 | -2.88 | 32 | 154 | +9.26 | -8.11 | +6.37 | +17.89 | PASS | 36.495 |
| `union_h5_topheavy` | long | 5 | topheavy | list | none | 44% | 62% | 13/29 | -0.50 | 32 | 154 | +8.99 | -8.12 | +5.35 | +17.89 | PASS | 36.345 |
| `flatten_live_h1_topheavy` | long | 1 | topheavy | list | none | 54% | 10% | 7/29 | +0.00 | 1 | 5 | +6.75 | -3.75 | +7.19 | +4.45 | PASS | 36.308 |
| `flatten_h5_topheavy` | long | 5 | topheavy | list | none | 42% | 59% | 13/29 | -1.71 | 21 | 109 | +8.38 | -8.25 | +8.02 | +0.02 | PASS | 35.774 |
| `flatten_h5` | long | 5 | leftover | list | none | 42% | 66% | 11/29 | -3.81 | 21 | 109 | +8.64 | -8.36 | +8.93 | +0.02 | PASS | 35.661 |
| `flatten_h5_cut` | long | 5 | leftover | cut_loser | none | 42% | 66% | 11/29 | -3.81 | 21 | 109 | +8.64 | -8.36 | +8.93 | +0.02 | PASS | 35.661 |
| `flatten_h5_sizeup` | long | 5 | leftover | list | sizeup | 42% | 66% | 11/29 | -3.81 | 21 | 109 | +8.64 | -8.36 | +8.93 | +0.02 | PASS | 35.661 |
| `flatten_h5_time` | long | 5 | leftover | time | none | 42% | 66% | 11/29 | -3.81 | 21 | 109 | +8.64 | -8.36 | +8.93 | +0.02 | PASS | 35.661 |
| `flatten_h5_trail` | long | 5 | leftover | trail | none | 42% | 66% | 11/29 | -3.81 | 21 | 109 | +8.64 | -8.36 | +8.93 | +0.02 | PASS | 35.661 |
| `combo_nj_5050_shared` | mix | 5 | leftover | list | none | 47% | 38% | 9/29 | -3.58 | 0 | 0 | +4.49 | -4.25 | +0.68 | — | PASS | 35.445 |
| `flatten_live_h1` | long | 1 | leftover | list | none | 54% | 10% | 7/29 | +0.00 | 1 | 5 | +6.31 | -4.05 | +8.22 | +4.45 | PASS | 35.268 |
| `flatten_live_h1_cut` | long | 1 | leftover | cut_loser | none | 54% | 10% | 7/29 | +0.00 | 1 | 5 | +6.31 | -4.05 | +8.22 | +4.45 | PASS | 35.268 |
| `flatten_live_h1_sboost` | long | 1 | leftover | list | both | 54% | 10% | 7/29 | +0.00 | 1 | 5 | +6.31 | -4.05 | +8.22 | +4.45 | PASS | 35.268 |
| `flatten_live_h1_sizeup` | long | 1 | leftover | list | sizeup | 54% | 10% | 7/29 | +0.00 | 1 | 5 | +6.31 | -4.05 | +8.22 | +4.45 | PASS | 35.268 |
| `flatten_live_h1_time` | long | 1 | leftover | time | none | 54% | 10% | 7/29 | +0.00 | 1 | 5 | +6.31 | -4.05 | +8.22 | +4.45 | PASS | 35.268 |
| `flatten_live_h1_trail` | long | 1 | leftover | trail | none | 54% | 10% | 7/29 | +0.00 | 1 | 5 | +6.31 | -4.05 | +8.22 | +4.45 | PASS | 35.268 |
| `flatten_live_h1_half` | long | 1 | half | list | none | 54% | 10% | 7/29 | +0.00 | 1 | 5 | +6.58 | -3.90 | +3.74 | +4.45 | PASS | 35.216 |
| `combo_fe1_5050_shared` | mix | 5 | leftover | list | none | 45% | 59% | 10/29 | -3.13 | 0 | 0 | +5.72 | -6.26 | +2.28 | — | PASS | 35.201 |
| `combo_snj_333_shared` | mix | 5 | leftover | list | none | 51% | 55% | 5/29 | -8.18 | 0 | 0 | +4.55 | -4.72 | +1.16 | — | PASS | 35.117 |
| `union_macd_up_h1` | long | 1 | leftover | list | none | 45% | 41% | 8/29 | -1.56 | 18 | 54 | +4.66 | -4.25 | +0.56 | +12.21 | PASS | 35.017 |
| `short_macd_dn_h3` | short | 3 | leftover | list | none | 52% | 45% | 10/29 | -1.71 | 27 | 124 | +9.92 | -7.00 | -8.45 | -13.75 | PASS | 35.006 |
| `union_news_or_net4_conv_h1` | long | 1 | conviction | list | none | 47% | 41% | 7/29 | -7.44 | 1 | 11 | +3.52 | -4.22 | +1.49 | -7.33 | PASS | 34.962 |
| `combo_fh_7030_shared` | mix | 5 | leftover | list | none | 44% | 62% | 7/29 | -3.89 | 0 | 0 | +7.35 | -6.37 | +3.06 | — | PASS | 34.895 |
| `union_h5_sboost` | long | 5 | leftover | list | both | 44% | 66% | 11/29 | -3.06 | 32 | 154 | +8.63 | -8.72 | +5.61 | +17.89 | PASS | 34.758 |
| `flatten_h5_sboost` | long | 5 | leftover | list | both | 42% | 66% | 10/29 | -3.81 | 21 | 109 | +8.44 | -8.19 | +6.55 | +0.02 | PASS | 34.422 |
| `yday_gainer_h1` | long | 1 | leftover | list | none | 48% | 38% | 7/29 | -3.72 | 15 | 67 | +4.77 | -4.27 | +2.46 | +6.44 | PASS | 33.888 |
| `flatten_live_h1_rankw` | long | 1 | rank_w | list | none | 54% | 10% | 6/29 | +0.00 | 1 | 5 | +6.14 | -4.12 | +4.03 | +4.45 | PASS | 33.433 |
| `flatten_white_yday_h5` | long | 5 | leftover | list | none | 51% | 41% | 12/29 | -1.25 | 5 | 23 | +9.10 | -10.47 | +1.83 | +2.80 | PASS | 33.375 |
| `short_rsi_ob_h1` | short | 1 | leftover | list | none | 51% | 34% | 9/29 | -1.33 | 18 | 63 | +4.08 | -4.86 | -8.74 | -9.44 | PASS | 33.366 |
| `union_flow_in_h1` | long | 1 | leftover | list | none | 44% | 31% | 6/29 | -2.17 | 4 | 10 | +4.06 | -3.85 | +7.20 | +10.57 | PASS | 33.236 |
| `combo_se_7030_shared` | mix | 5 | leftover | list | none | 51% | 55% | 4/29 | -1.98 | 0 | 0 | +6.54 | -7.37 | +20.35 | — | PASS | 32.622 |
| `union_white_both_n4_h5` | long | 5 | leftover | list | none | 41% | 52% | 9/29 | -0.82 | 13 | 32 | +8.53 | -8.82 | +18.79 | +21.47 | PASS | 32.604 |
| `union_h1_topheavy` | long | 1 | topheavy | list | none | 41% | 41% | 6/29 | -4.50 | 8 | 48 | +4.22 | -3.72 | +3.52 | +5.17 | PASS | 32.578 |
| `short_news_or_h3` | short | 3 | leftover | list | none | 55% | 55% | 5/29 | -1.24 | 13 | 49 | +4.86 | -6.85 | +3.21 | -2.63 | PASS | 32.573 |
| `combo_ef_3070_shared` | mix | 5 | leftover | list | none | 43% | 62% | 8/29 | -1.88 | 0 | 0 | +8.29 | -7.36 | +7.19 | — | PASS | 32.473 |
| `union_h5_rankw` | long | 5 | rank_w | list | none | 44% | 62% | 10/29 | -4.57 | 32 | 154 | +8.29 | -8.33 | -0.12 | +17.89 | PASS | 32.376 |
| `short_news_r_h3` | short | 3 | leftover | list | none | 55% | 55% | 5/29 | -1.29 | 13 | 49 | +4.95 | -6.85 | +3.05 | -2.59 | PASS | 32.311 |
| `union_vol_green_h1` | long | 1 | leftover | list | none | 46% | 41% | 7/29 | -7.86 | 19 | 56 | +5.33 | -7.33 | +2.19 | -7.83 | PASS | 32.218 |
| `union_last_red_h3` | long | 3 | leftover | list | none | 38% | 52% | 11/29 | -0.28 | 26 | 131 | +8.59 | -6.74 | +1.40 | +0.17 | PASS | 31.703 |
| `union_white_yday_h1` | long | 1 | leftover | list | none | 47% | 34% | 3/29 | -7.13 | 2 | 13 | +4.49 | -3.61 | -3.37 | +0.86 | PASS | 31.314 |
| `combo_se1_5050_shared` | mix | 5 | leftover | list | none | 55% | 52% | 1/29 | -5.84 | 0 | 0 | +4.78 | -6.24 | +7.82 | — | PASS | 30.964 |
| `union_last_green_h5` | long | 5 | leftover | list | none | 46% | 62% | 8/29 | -2.66 | 41 | 159 | +8.87 | -10.93 | +2.27 | +60.69 | PASS | 30.847 |
| `combo_e1s_7030_shared` | mix | 5 | leftover | list | none | 54% | 52% | 1/29 | -4.26 | 0 | 0 | +4.87 | -6.09 | +8.03 | — | PASS | 30.838 |
| `union_white_coil_h1` | long | 1 | leftover | list | none | 39% | 31% | 9/29 | -1.96 | 4 | 20 | +4.96 | -4.05 | -3.97 | -4.33 | PASS | 30.753 |
| `combo_sn_5050_shared` | mix | 5 | leftover | list | none | 55% | 59% | 0/29 | -11.11 | 0 | 0 | +4.38 | -5.04 | -6.80 | — | PASS | 30.581 |
| `short_macd_dn_h1` | short | 1 | leftover | list | none | 52% | 28% | 4/29 | -4.93 | 12 | 60 | +5.52 | -4.27 | -8.78 | -0.02 | PASS | 30.349 |
| `combo_sn_7030_shared` | mix | 5 | leftover | list | none | 54% | 55% | 0/29 | -9.01 | 0 | 0 | +4.29 | -5.14 | -4.98 | — | PASS | 29.857 |
| `union_news_or_net4_rw_h1` | long | 1 | rank_w | list | none | 47% | 45% | 1/29 | -9.86 | 1 | 11 | +3.55 | -4.17 | -5.50 | -7.33 | PASS | 29.521 |
| `combo_sn_3070_shared` | mix | 5 | leftover | list | none | 54% | 55% | 0/29 | -12.30 | 0 | 0 | +4.47 | -4.91 | -7.88 | — | PASS | 29.491 |
| `flatten_h5_rankw` | long | 5 | rank_w | list | none | 42% | 55% | 7/29 | -2.17 | 21 | 109 | +8.25 | -8.57 | +5.03 | +0.02 | PASS | 29.199 |
| `union_h5_exit_alarm` | long | 5 | leftover | list | none | 45% | 55% | 6/29 | -1.93 | 32 | 157 | +7.69 | -7.04 | +4.87 | +23.56 | PASS | 28.863 |
| `union_earn_react_h3` | long | 3 | leftover | list | none | 40% | 48% | 9/29 | -3.59 | 15 | 91 | +8.14 | -9.05 | +16.12 | -17.75 | PASS | 28.775 |
| `union_h5_half` | long | 5 | half | list | none | 44% | 59% | 5/29 | -3.44 | 32 | 154 | +8.45 | -7.11 | +1.80 | +17.89 | PASS | 28.624 |
| `union_news_g_h1` | long | 1 | leftover | list | none | 48% | 41% | 0/29 | -5.29 | 4 | 37 | +4.13 | -4.29 | -3.18 | +8.21 | PASS | 28.118 |
| `combo_ne1_5050_shared` | mix | 5 | leftover | list | none | 50% | 38% | 1/29 | -5.66 | 0 | 0 | +4.12 | -5.06 | +2.89 | — | PASS | 28.097 |
| `ohlc_hot_coil_h1` | long | 1 | leftover | list | none | 49% | 38% | 1/29 | -3.59 | 12 | 28 | +4.59 | -4.13 | -19.97 | -10.55 | PASS | 28.06 |
| `union_candle_h1` | long | 1 | leftover | list | none | 43% | 34% | 3/29 | -4.89 | 11 | 51 | +4.27 | -4.03 | -2.18 | +0.19 | PASS | 27.956 |
| `union_flow_in_h5` | long | 5 | leftover | list | none | 54% | 41% | 8/29 | -2.02 | 8 | 48 | +3.82 | -8.06 | +5.25 | -12.43 | PASS | 27.907 |
| `flatten_h5_s8` | long | 5 | leftover | list | none | 42% | 62% | 3/29 | -4.66 | 21 | 109 | +8.66 | -8.13 | +6.28 | +0.02 | PASS | 27.832 |
| `union_news_or_h1` | long | 1 | leftover | list | none | 48% | 38% | 0/29 | -5.03 | 4 | 37 | +4.18 | -3.88 | -3.79 | +7.99 | PASS | 27.691 |
| `combo_nse_333_shared` | mix | 5 | leftover | list | none | 48% | 45% | 1/29 | -7.48 | 0 | 0 | +5.65 | -6.00 | +11.68 | — | PASS | 27.543 |
| `combo_jf_5050_shared` | mix | 5 | leftover | list | none | 41% | 52% | 2/29 | -6.79 | 0 | 0 | +5.94 | -5.02 | +2.21 | — | PASS | 27.518 |
| `union_join_present_h3` | long | 3 | leftover | list | none | 41% | 48% | 3/29 | -5.31 | 20 | 116 | +8.86 | -5.61 | -1.53 | +4.20 | PASS | 27.417 |
| `short_alarm_h1` | short | 1 | leftover | list | none | 52% | 21% | 3/29 | -0.61 | 7 | 51 | +4.08 | -3.49 | +0.55 | +6.11 | PASS | 27.238 |
| `ohlc_hot_h1` | long | 1 | leftover | list | none | 42% | 38% | 6/29 | -3.27 | 17 | 58 | +3.35 | -4.99 | -12.03 | -1.60 | PASS | 27.234 |
| `union_cond_h3` | long | 3 | leftover | list | none | 43% | 52% | 6/29 | -4.75 | 17 | 124 | +5.67 | -6.50 | -3.72 | -9.34 | PASS | 26.968 |
| `union_candle_score_h1` | long | 1 | leftover | list | none | 51% | 28% | 0/29 | -4.80 | 19 | 55 | +4.11 | -4.04 | -3.58 | +7.41 | PASS | 26.891 |
| `combo_en_3070_shared` | mix | 5 | leftover | list | none | 44% | 41% | 1/29 | -2.77 | 0 | 0 | +5.92 | -5.97 | +17.11 | — | PASS | 26.875 |
| `union_blue_h1` | long | 1 | leftover | list | none | 40% | 28% | 4/29 | -1.90 | 11 | 44 | +4.30 | -3.94 | -1.72 | -3.60 | PASS | 26.812 |
| `union_white_any_h2` | long | 2 | leftover | list | none | 41% | 45% | 4/29 | -7.49 | 11 | 40 | +6.32 | -5.84 | -5.81 | +5.07 | PASS | 26.793 |
| `union_last_green_h3` | long | 3 | leftover | list | none | 50% | 38% | 4/29 | -4.07 | 31 | 117 | +6.52 | -7.43 | -2.39 | +36.04 | PASS | 26.79 |
| `short_last_red_h3` | short | 3 | leftover | list | none | 58% | 41% | 4/29 | -5.41 | 26 | 127 | +5.14 | -7.81 | -12.12 | -19.69 | PASS | 26.755 |
| `probable_probable_ok_h1` | long | 1 | leftover | list | none | 47% | 31% | 0/29 | -4.47 | 8 | 28 | +4.03 | -3.34 | -6.75 | +1.86 | PASS | 26.727 |
| `union_h1_rankw` | long | 1 | rank_w | list | none | 41% | 41% | 0/29 | -4.84 | 8 | 48 | +4.11 | -3.82 | -0.39 | +5.17 | PASS | 26.529 |
| `union_news_g_cam71_h1` | long | 1 | leftover | list | none | 37% | 34% | 6/29 | -8.16 | 1 | 6 | +3.17 | -4.45 | -8.16 | -10.07 | PASS | 26.485 |
| `union_news_head_h1` | long | 1 | leftover | list | none | 48% | 31% | 0/29 | -4.42 | 5 | 27 | +4.33 | -4.05 | -7.74 | +12.67 | PASS | 26.387 |
| `union_h3_cut` | long | 3 | leftover | cut_loser | none | 42% | 52% | 0/29 | -6.72 | 20 | 112 | +8.51 | -5.41 | -1.71 | +4.58 | PASS | 26.314 |
| `union_h3_trail` | long | 3 | leftover | trail | none | 42% | 52% | 0/29 | -6.72 | 20 | 112 | +8.51 | -5.41 | -1.71 | +4.58 | PASS | 26.314 |
| `union_news_g_cond_h1` | long | 1 | leftover | list | none | 47% | 34% | 0/29 | -5.26 | 4 | 38 | +4.18 | -4.03 | -4.80 | +7.41 | PASS | 26.309 |
| `short_extended_h1` | short | 1 | leftover | list | none | 51% | 31% | 3/29 | -8.24 | 30 | 71 | +4.91 | -6.77 | -15.59 | -12.13 | PASS | 26.148 |
| `union_h3_exit_news_r` | long | 3 | leftover | list | none | 41% | 45% | 3/29 | -6.31 | 20 | 116 | +8.77 | -5.83 | -3.35 | +0.17 | PASS | 26.07 |
| `union_flow_in_h3` | long | 3 | leftover | list | none | 48% | 38% | 6/29 | -2.05 | 7 | 35 | +3.27 | -6.00 | -1.55 | -1.57 | PASS | 26.031 |
| `union_h3_exit_red` | long | 3 | leftover | list | none | 48% | 38% | 4/29 | -5.82 | 31 | 117 | +6.59 | -6.75 | -6.15 | +21.81 | PASS | 26.016 |
| `union_cond_h1` | long | 1 | leftover | list | none | 47% | 31% | 0/29 | -4.01 | 5 | 43 | +4.15 | -3.85 | -6.38 | -0.92 | PASS | 25.877 |
| `union_h1` | long | 1 | leftover | list | none | 41% | 38% | 0/29 | -6.29 | 8 | 48 | +4.13 | -3.75 | -1.25 | +5.17 | PASS | 25.85 |
| `union_h1_sizeup` | long | 1 | leftover | list | sizeup | 41% | 38% | 0/29 | -6.29 | 8 | 48 | +4.13 | -3.75 | -1.25 | +5.17 | PASS | 25.85 |
| `union_h1_trail` | long | 1 | leftover | trail | none | 41% | 38% | 0/29 | -6.29 | 8 | 48 | +4.13 | -3.75 | -1.25 | +5.17 | PASS | 25.85 |
| `flatten_h3_sboost` | long | 3 | leftover | list | both | 43% | 41% | 2/29 | -12.28 | 13 | 76 | +7.38 | -4.92 | -9.87 | -1.49 | PASS | 25.825 |
| `combo_ser_5050_shared` | mix | 5 | leftover | list | none | 48% | 48% | 1/29 | -5.10 | 0 | 0 | +6.45 | -7.66 | +15.12 | — | PASS | 25.8 |
| `flatten_h3` | long | 3 | leftover | list | none | 43% | 41% | 2/29 | -12.28 | 13 | 76 | +7.64 | -5.12 | -9.82 | -1.49 | PASS | 25.793 |
| `flatten_h3_cut` | long | 3 | leftover | cut_loser | none | 43% | 41% | 2/29 | -12.28 | 13 | 76 | +7.64 | -5.12 | -9.82 | -1.49 | PASS | 25.793 |
| `flatten_h3_sizeup` | long | 3 | leftover | list | sizeup | 43% | 41% | 2/29 | -12.28 | 13 | 76 | +7.64 | -5.12 | -9.82 | -1.49 | PASS | 25.793 |
| `flatten_h3_trail` | long | 3 | leftover | trail | none | 43% | 41% | 2/29 | -12.28 | 13 | 76 | +7.64 | -5.12 | -9.82 | -1.49 | PASS | 25.793 |
| `union_h3_time` | long | 3 | leftover | time | none | 42% | 52% | 0/29 | -6.72 | 20 | 112 | +7.96 | -5.49 | -1.16 | +4.58 | PASS | 25.777 |
| `union_h3` | long | 3 | leftover | list | none | 42% | 52% | 0/29 | -6.72 | 20 | 112 | +8.52 | -5.68 | -3.12 | +4.58 | PASS | 25.737 |
| `union_h3_sizeup` | long | 3 | leftover | list | sizeup | 42% | 52% | 0/29 | -6.72 | 20 | 112 | +8.52 | -5.68 | -3.12 | +4.58 | PASS | 25.737 |
| `union_white_h1` | long | 1 | leftover | list | none | 35% | 34% | 3/29 | -5.39 | 4 | 27 | +6.14 | -4.26 | -0.45 | -8.86 | PASS | 25.694 |
| `union_join_present_h1` | long | 1 | leftover | list | none | 41% | 38% | 0/29 | -5.72 | 7 | 51 | +4.11 | -3.69 | -0.74 | +5.62 | PASS | 25.659 |
| `union_h3_exit_alarm` | long | 3 | leftover | list | none | 41% | 48% | 3/29 | -5.09 | 20 | 116 | +6.79 | -5.88 | -1.88 | +5.10 | PASS | 25.574 |
| `union_join_g_h3` | long | 3 | leftover | list | none | 42% | 34% | 5/29 | -7.39 | 23 | 115 | +7.69 | -5.63 | -5.75 | -1.10 | PASS | 25.547 |
| `union_h1_sboost` | long | 1 | leftover | list | both | 41% | 38% | 0/29 | -7.99 | 8 | 48 | +4.12 | -3.81 | -2.95 | +5.17 | PASS | 25.481 |
| `combo_ner_5050_shared` | mix | 5 | leftover | list | none | 42% | 45% | 1/29 | -4.44 | 0 | 0 | +5.79 | -6.38 | +14.15 | — | PASS | 25.453 |
| `union_h1_time` | long | 1 | leftover | time | none | 41% | 34% | 0/29 | -6.37 | 8 | 48 | +4.07 | -3.47 | -1.73 | +5.17 | PASS | 25.428 |
| `yday_gainer_h3` | long | 3 | leftover | list | none | 46% | 41% | 5/29 | -3.42 | 32 | 134 | +8.39 | -7.61 | -7.66 | +23.14 | PASS | 25.423 |
| `flatten_h3_time` | long | 3 | leftover | time | none | 43% | 41% | 2/29 | -12.28 | 13 | 76 | +6.99 | -5.39 | -6.67 | -1.49 | PASS | 25.291 |
| `union_h1_cut` | long | 1 | leftover | cut_loser | none | 41% | 34% | 0/29 | -6.37 | 8 | 48 | +4.13 | -3.71 | -1.42 | +5.17 | PASS | 25.185 |
| `union_join_g_h1` | long | 1 | leftover | list | none | 43% | 34% | 0/29 | -3.68 | 9 | 52 | +3.80 | -3.75 | -0.46 | +1.78 | PASS | 25.178 |
| `yday_gainer_h5` | long | 5 | leftover | list | none | 46% | 52% | 5/29 | -1.55 | 43 | 170 | +9.60 | -9.79 | -2.56 | +14.74 | PASS | 25.174 |
| `union_h3_sboost` | long | 3 | leftover | list | both | 42% | 52% | 0/29 | -7.61 | 20 | 112 | +8.28 | -5.79 | -4.88 | +4.58 | PASS | 25.123 |
| `union_h1_half` | long | 1 | half | list | none | 41% | 34% | 0/29 | -3.98 | 8 | 48 | +4.23 | -3.74 | -3.22 | +5.17 | PASS | 24.999 |
| `union_white_both_n4_h3` | long | 3 | leftover | list | none | 46% | 34% | 2/29 | -5.61 | 11 | 27 | +7.76 | -6.20 | -0.66 | +28.12 | PASS | 24.883 |
| `combo_nf_5050_shared` | mix | 5 | leftover | list | none | 39% | 52% | 0/29 | -7.33 | 0 | 0 | +6.57 | -5.39 | -1.54 | — | PASS | 24.708 |
| `flatten_h1` | long | 1 | leftover | list | none | 40% | 34% | 0/29 | -8.26 | 3 | 32 | +3.90 | -3.46 | -2.72 | -8.57 | PASS | 24.664 |
| `union_news_g_cam71_conv_h1` | long | 1 | conviction | list | none | 33% | 28% | 6/29 | -3.25 | 1 | 5 | +3.12 | -4.70 | -2.15 | -11.95 | PASS | 24.664 |
| `union_ab_g_h1` | long | 1 | leftover | list | none | 40% | 34% | 0/29 | -6.17 | 7 | 40 | +3.60 | -3.19 | -1.98 | -6.02 | PASS | 24.458 |
| `union_news_present_h1` | long | 1 | leftover | list | none | 41% | 34% | 0/29 | -5.72 | 7 | 49 | +3.96 | -3.68 | -2.31 | +3.67 | PASS | 24.449 |
| `union_macd_hist_h3` | long | 3 | leftover | list | none | 41% | 45% | 8/29 | -4.82 | 24 | 151 | +5.00 | -5.67 | -4.82 | -28.78 | PASS | 24.354 |
| `union_h3_half` | long | 3 | half | list | none | 42% | 48% | 0/29 | -7.77 | 20 | 112 | +7.75 | -5.28 | -6.78 | +4.58 | PASS | 24.34 |
| `union_h3_rankw` | long | 3 | rank_w | list | none | 42% | 48% | 0/29 | -8.82 | 20 | 112 | +8.47 | -5.86 | -7.63 | +4.58 | PASS | 24.092 |
| `union_cond_n4_h3` | long | 3 | leftover | list | none | 42% | 45% | 6/29 | -3.76 | 9 | 62 | +4.61 | -7.77 | -2.73 | -4.92 | PASS | 24.059 |
| `flatten_h3_rankw` | long | 3 | rank_w | list | none | 43% | 38% | 1/29 | -8.65 | 13 | 76 | +7.45 | -5.29 | -9.02 | -1.49 | PASS | 23.941 |
| `union_news_present_h3` | long | 3 | leftover | list | none | 40% | 45% | 3/29 | -5.31 | 19 | 111 | +7.05 | -5.66 | -9.12 | -1.32 | PASS | 23.848 |
| `combo_ers_7030_shared` | mix | 5 | leftover | list | none | 46% | 41% | 1/29 | -5.49 | 0 | 0 | +6.57 | -7.76 | +15.64 | — | PASS | 23.833 |
| `flatten_h3_half` | long | 3 | half | list | none | 43% | 41% | 0/29 | -6.39 | 13 | 76 | +6.61 | -5.00 | -5.93 | -1.49 | PASS | 23.802 |
| `union_news_pack_net2_h3` | long | 3 | leftover | list | none | 40% | 28% | 10/29 | -0.03 | 3 | 27 | +5.95 | -7.59 | -11.73 | -9.16 | PASS | 23.754 |
| `union_news_or_net2_h1` | long | 1 | leftover | list | none | 44% | 31% | 0/29 | -8.08 | 1 | 27 | +4.21 | -3.94 | -8.43 | -5.84 | PASS | 23.688 |
| `union_news_or_net4_h1` | long | 1 | leftover | list | none | 44% | 34% | 0/29 | -14.26 | 1 | 21 | +3.93 | -3.90 | -12.26 | -9.75 | PASS | 23.647 |
| `short_extended_h3` | short | 3 | leftover | list | none | 55% | 45% | 2/29 | -14.93 | 54 | 148 | +7.64 | -10.27 | -18.66 | -41.54 | PASS | 23.609 |
| `short_rsi_ob_h3` | short | 3 | leftover | list | none | 53% | 41% | 3/29 | -6.37 | 36 | 141 | +5.40 | -8.02 | -11.89 | -125.12 | PASS | 23.478 |
| `union_news_vol_h1` | long | 1 | leftover | list | none | 45% | 28% | 0/29 | -8.40 | 5 | 22 | +4.30 | -4.44 | -6.88 | +7.02 | PASS | 23.317 |
| `flatten_h5_half` | long | 5 | half | list | none | 42% | 52% | 1/29 | -4.78 | 21 | 109 | +7.68 | -6.85 | -0.53 | +0.02 | PASS | 23.298 |
| `union_news_g_cam71_h3` | long | 3 | leftover | list | none | 46% | 34% | 6/29 | -13.65 | 2 | 18 | +4.13 | -7.28 | -14.54 | -8.73 | PASS | 23.21 |
| `combo_fer_5050_shared` | mix | 5 | leftover | list | none | 40% | 48% | 3/29 | -3.53 | 0 | 0 | +7.92 | -8.02 | +7.35 | — | PASS | 23.005 |
| `union_h3_topheavy` | long | 3 | topheavy | list | none | 42% | 45% | 0/29 | -5.21 | 20 | 112 | +7.18 | -5.99 | -2.66 | +4.58 | PASS | 22.928 |
| `union_vol_g_h5` | long | 5 | leftover | list | none | 44% | 41% | 7/29 | -4.13 | 38 | 153 | +11.72 | -10.44 | -12.16 | +10.11 | PASS | 22.857 |
| `flatten_h3_topheavy` | long | 3 | topheavy | list | none | 43% | 38% | 1/29 | -8.11 | 13 | 76 | +5.86 | -5.32 | -7.58 | -1.49 | PASS | 22.631 |
| `union_candle_h3` | long | 3 | leftover | list | none | 45% | 28% | 5/29 | -3.41 | 29 | 125 | +6.46 | -6.80 | -6.17 | +13.17 | PASS | 22.603 |
| `union_blue_h3` | long | 3 | leftover | list | none | 41% | 41% | 3/29 | -10.05 | 22 | 101 | +6.57 | -6.11 | -14.16 | -4.61 | PASS | 22.417 |
| `union_e_green_h1` | long | 1 | leftover | list | none | 46% | 28% | 5/29 | -4.50 | 3 | 18 | +3.95 | -8.96 | -23.35 | -21.55 | PASS | 22.311 |
| `union_news_g_cam71_n2_h1` | long | 1 | topheavy | list | none | 35% | 28% | 5/29 | -11.50 | 1 | 4 | +2.93 | -4.64 | -14.28 | -13.43 | PASS | 22.196 |
| `union_white_h5` | long | 5 | leftover | list | none | 41% | 55% | 2/29 | -7.88 | 19 | 73 | +9.14 | -9.26 | -2.61 | +30.10 | PASS | 21.68 |
| `probable_h3` | long | 3 | leftover | list | none | 46% | 38% | 3/29 | -1.87 | 29 | 137 | +7.00 | -8.05 | -6.61 | +11.72 | PASS | 21.628 |
| `flatten_vol_g_h3` | long | 3 | leftover | list | none | 48% | 28% | 5/29 | -15.67 | 6 | 25 | +9.63 | -9.35 | -24.04 | -17.64 | PASS | 21.585 |
| `short_clk_ext_veto_h3` | short | 3 | leftover | list | none | 52% | 41% | 0/29 | -9.49 | 32 | 112 | +7.50 | -7.70 | -13.40 | -96.02 | PASS | 21.481 |
| `short_clk_neg_weak_fail_h3` | short | 3 | leftover | list | none | 52% | 34% | 2/29 | -5.71 | 23 | 124 | +6.43 | -6.67 | -12.19 | -30.12 | PASS | 21.431 |
| `union_vol_g_h1` | long | 1 | leftover | list | none | 47% | 34% | 0/29 | -14.25 | 17 | 68 | +5.26 | -7.35 | -13.77 | +2.59 | PASS | 21.407 |
| `probable_probable_ok_h3` | long | 3 | leftover | list | none | 50% | 31% | 3/29 | -3.59 | 22 | 72 | +6.08 | -7.62 | -20.26 | +8.69 | PASS | 21.299 |
| `union_coil_green_h1` | long | 1 | leftover | list | none | 39% | 31% | 0/29 | -8.41 | 13 | 45 | +3.30 | -4.22 | -9.05 | -10.80 | PASS | 21.172 |
| `union_last_red_h1` | long | 1 | leftover | list | none | 47% | 31% | 0/29 | -11.41 | 14 | 71 | +3.57 | -4.97 | -12.06 | +3.60 | PASS | 21.155 |
| `union_news_or_net5_h1` | long | 1 | leftover | list | none | 40% | 31% | 0/29 | -13.08 | 1 | 13 | +3.41 | -4.24 | -12.44 | -10.44 | PASS | 20.901 |
| `union_news_or_net4_h3` | long | 3 | leftover | list | none | 43% | 38% | 1/29 | -15.37 | 8 | 46 | +6.05 | -5.32 | -16.55 | -16.15 | PASS | 20.851 |
| `union_white_any_h1` | long | 1 | leftover | list | none | 34% | 28% | 0/29 | -8.13 | 4 | 24 | +6.50 | -4.34 | -10.19 | -10.91 | PASS | 20.629 |
| `union_news_vol_h3` | long | 3 | leftover | list | none | 48% | 45% | 0/29 | -9.66 | 10 | 56 | +4.25 | -5.14 | -11.68 | +166.07 | PASS | 20.378 |
| `flatten_live_h3` | long | 3 | leftover | list | none | 58% | 10% | 0/29 | -6.24 | 4 | 14 | +8.60 | -8.85 | -6.24 | +6.87 | PASS | 20.159 |
| `union_blue_coil_h3` | long | 3 | leftover | list | none | 42% | 45% | 2/29 | -8.48 | 18 | 98 | +6.93 | -10.67 | -12.36 | -25.18 | PASS | 20.156 |
| `union_news_pack_h3` | long | 3 | leftover | list | none | 40% | 28% | 7/29 | -1.39 | 5 | 31 | +5.43 | -8.52 | -13.42 | -16.25 | PASS | 20.067 |
| `union_earn_react_h1` | long | 1 | leftover | list | none | 46% | 28% | 0/29 | -4.09 | 7 | 51 | +4.54 | -7.02 | -3.50 | -2.64 | PASS | 20.028 |
| `union_news_or_net3_h1` | long | 1 | leftover | list | none | 43% | 28% | 0/29 | -17.85 | 1 | 25 | +3.89 | -4.24 | -19.34 | -13.10 | PASS | 19.889 |
| `ohlc_hot_h3` | long | 3 | leftover | list | none | 43% | 41% | 3/29 | -4.05 | 34 | 163 | +5.69 | -6.82 | -5.04 | +73.70 | PASS | 19.323 |
| `union_blue_vol_h1` | long | 1 | leftover | list | none | 42% | 31% | 0/29 | -10.12 | 11 | 51 | +5.24 | -7.35 | -13.21 | -2.38 | PASS | 19.262 |
| `union_white_any_h5` | long | 5 | leftover | list | none | 36% | 59% | 1/29 | -10.98 | 17 | 70 | +8.31 | -9.20 | -0.85 | -15.92 | PASS | 19.261 |
| `union_macd_xup_h3` | long | 3 | leftover | list | none | 35% | 34% | 8/29 | -4.89 | 19 | 81 | +5.71 | -7.75 | -20.71 | -55.21 | PASS | 18.841 |
| `union_blue_coil_h1` | long | 1 | leftover | list | none | 39% | 24% | 1/29 | -11.32 | 9 | 42 | +3.94 | -6.47 | -13.73 | -20.46 | PASS | 18.529 |
| `union_macd_xup_h1` | long | 1 | leftover | list | none | 37% | 31% | 0/29 | -5.25 | 10 | 38 | +3.31 | -4.21 | -16.51 | -18.56 | PASS | 18.224 |
| `union_news_or_net5_h3` | long | 3 | leftover | list | none | 44% | 38% | 1/29 | -14.89 | 5 | 34 | +3.77 | -5.74 | -19.38 | -15.10 | PASS | 18.074 |
| `union_coil_off_h1` | long | 1 | leftover | list | none | 40% | 28% | 0/29 | -11.81 | 9 | 57 | +2.91 | -4.24 | -17.74 | -15.38 | PASS | 17.992 |
| `union_blue_vol_h3` | long | 3 | leftover | list | none | 40% | 38% | 3/29 | -3.50 | 20 | 98 | +9.89 | -13.33 | -9.28 | +13.48 | PASS | 17.896 |
| `union_news_pack_net3_h3` | long | 3 | leftover | list | none | 37% | 21% | 7/29 | -4.38 | 3 | 25 | +6.18 | -7.59 | -15.53 | -16.71 | PASS | 17.779 |
| `union_white_yday_h3` | long | 3 | leftover | list | none | 43% | 41% | 0/29 | -15.27 | 6 | 45 | +5.46 | -7.98 | -15.27 | -11.60 | PASS | 17.738 |
| `probable_h5` | long | 5 | leftover | list | none | 46% | 34% | 4/29 | -5.59 | 40 | 173 | +7.96 | -10.40 | -16.75 | -4.41 | PASS | 17.461 |
| `union_ab_g_h3` | long | 3 | leftover | list | none | 38% | 28% | 1/29 | -13.41 | 20 | 98 | +7.33 | -5.41 | -14.06 | -11.46 | PASS | 17.401 |
| `combo_e1er_5050_shared` | mix | 5 | leftover | list | none | 45% | 17% | 3/29 | -3.57 | 0 | 0 | +4.52 | -7.36 | -6.10 | — | PASS | 17.315 |
| `union_white_h3` | long | 3 | leftover | list | none | 40% | 38% | 3/29 | -18.34 | 12 | 61 | +6.74 | -7.50 | -18.34 | +7.72 | PASS | 17.276 |
| `union_clk_flow_coil_h1` | long | 1 | leftover | list | none | 23% | 28% | 0/29 | -8.48 | 1 | 5 | +4.68 | -2.96 | -10.38 | -18.05 | PASS | 17.094 |
| `union_rsi_os_h3` | long | 3 | leftover | list | none | 45% | 41% | 6/29 | -5.62 | 4 | 51 | +5.13 | -9.38 | -23.89 | -44.40 | PASS | 16.66 |
| `union_news_or_net2_h3` | long | 3 | leftover | list | none | 43% | 31% | 0/29 | -11.42 | 9 | 68 | +5.56 | -5.69 | -14.99 | -13.74 | PASS | 16.596 |
| `union_news_or_net3_h3` | long | 3 | leftover | list | none | 41% | 31% | 1/29 | -18.21 | 8 | 55 | +5.41 | -5.84 | -20.63 | -26.38 | PASS | 16.059 |
| `flatten_live_h5` | long | 5 | leftover | list | none | 46% | 24% | 0/29 | -5.51 | 5 | 16 | +9.01 | -11.62 | -5.51 | +5.13 | PASS | 16.0 |
| `union_news_g_cam61_h3` | long | 3 | leftover | list | none | 47% | 24% | 1/29 | -14.91 | 3 | 28 | +3.70 | -6.36 | -18.29 | -25.52 | PASS | 15.94 |
| `union_news_g_h3` | long | 3 | leftover | list | none | 47% | 28% | 0/29 | -14.88 | 19 | 93 | +4.38 | -6.42 | -21.97 | +153.92 | PASS | 15.767 |
| `union_rsi_h1` | long | 1 | leftover | list | none | 42% | 34% | 0/29 | -20.75 | 9 | 87 | +4.38 | -8.12 | -24.50 | -20.14 | PASS | 15.727 |
| `short_clk_neg_weak_fail_opp_h3` | short | 3 | leftover | list | none | 49% | 41% | 0/29 | -11.38 | 11 | 61 | +4.86 | -6.54 | -28.58 | -55.25 | PASS | 15.259 |
| `overnight_mega_h1` | long | 1 | leftover | list | none | 31% | 10% | 0/29 | +0.00 | 0 | 5 | +5.65 | -5.19 | -5.76 | -14.12 | PASS | 15.106 |
| `union_white_any_h3` | long | 3 | leftover | list | none | 39% | 34% | 2/29 | -18.44 | 12 | 59 | +6.17 | -8.32 | -16.38 | -7.98 | PASS | 14.975 |
| `union_news_or_h3` | long | 3 | leftover | list | none | 45% | 24% | 0/29 | -14.78 | 16 | 96 | +5.18 | -5.89 | -18.67 | +153.44 | PASS | 14.842 |
| `union_rsi_h3` | long | 3 | leftover | list | none | 44% | 38% | 0/29 | -9.83 | 19 | 158 | +7.91 | -8.40 | -16.75 | -27.26 | PASS | 14.804 |
| `union_news_g_cond_h3` | long | 3 | leftover | list | none | 45% | 24% | 0/29 | -14.78 | 16 | 96 | +5.18 | -6.04 | -19.37 | +151.90 | PASS | 14.622 |
| `union_rsi_os_h1` | long | 1 | leftover | list | none | 46% | 34% | 0/29 | -22.65 | 3 | 29 | +2.48 | -6.47 | -33.85 | -4.54 | PASS | 14.537 |
| `ohlc_hot_h5` | long | 5 | leftover | list | none | 42% | 41% | 2/29 | -10.71 | 47 | 193 | +6.79 | -9.51 | -10.85 | +56.43 | PASS | 14.466 |
| `overnight_mega_h2` | long | 2 | leftover | list | none | 50% | 7% | 1/29 | +0.00 | 0 | 12 | +3.54 | -6.97 | -7.35 | -6.81 | PASS | 14.45 |
| `overnight_h5` | long | 5 | leftover | list | none | 42% | 45% | 1/29 | -7.20 | 19 | 106 | +6.87 | -12.55 | -12.79 | -31.95 | PASS | 14.423 |
| `union_white_coil_h3` | long | 3 | leftover | list | none | 38% | 38% | 0/29 | -16.12 | 9 | 46 | +6.16 | -7.87 | -15.46 | -20.10 | PASS | 14.369 |
| `union_news_head_h3` | long | 3 | leftover | list | none | 45% | 21% | 0/29 | -14.14 | 18 | 78 | +4.88 | -5.95 | -23.60 | +160.05 | PASS | 14.291 |
| `overnight_h3` | long | 3 | leftover | list | none | 44% | 34% | 0/29 | -12.36 | 15 | 93 | +6.08 | -8.89 | -16.01 | -33.98 | PASS | 13.703 |
| `union_coil_green_h3` | long | 3 | leftover | list | none | 38% | 28% | 0/29 | -13.81 | 28 | 128 | +5.78 | -7.10 | -11.66 | -23.30 | PASS | 13.091 |
| `union_news_g_cam61_h1` | long | 1 | leftover | list | none | 33% | 17% | 0/29 | -17.36 | 1 | 13 | +3.23 | -4.87 | -20.11 | -19.77 | PASS | 13.0 |
| `union_news_g_h5` | long | 5 | leftover | list | none | 39% | 38% | 1/29 | -7.40 | 21 | 125 | +5.43 | -8.86 | -17.20 | +95.63 | PASS | 12.246 |
| `overnight_h1` | long | 1 | leftover | list | none | 31% | 28% | 0/29 | -14.19 | 1 | 49 | +6.05 | -8.51 | -19.14 | -21.82 | PASS | 11.64 |
| `coil_h3_exit_alarm` | long | 3 | leftover | list | none | 37% | 28% | 0/29 | -12.62 | 18 | 130 | +4.35 | -6.80 | -12.03 | -16.90 | PASS | 11.467 |
| `union_overnight_h3` | long | 3 | leftover | list | none | 43% | 28% | 0/29 | -12.37 | 13 | 87 | +5.70 | -9.19 | -17.66 | -39.83 | PASS | 11.053 |
| `union_coil_off_h3` | long | 3 | leftover | list | none | 36% | 28% | 0/29 | -13.95 | 23 | 128 | +4.41 | -7.53 | -17.10 | -26.93 | PASS | 10.609 |
| `union_overnight_h1` | long | 1 | leftover | list | none | 31% | 21% | 0/29 | -14.88 | 1 | 45 | +6.37 | -8.11 | -22.44 | -26.49 | PASS | 10.247 |
| `union_coil_off_h5` | long | 5 | leftover | list | none | 38% | 41% | 0/29 | -14.82 | 32 | 163 | +3.69 | -49.40 | -10.75 | -37.42 | PASS | 9.795 |
| `union_rsi_os_h5` | long | 5 | leftover | list | none | 39% | 34% | 5/29 | -6.67 | 5 | 54 | +6.58 | -14.85 | -32.38 | -60.58 | PASS | 9.652 |
| `union_news_both_h1` *(thin)* | long | 1 | leftover | list | none | 80% | 10% | 21/29 | +7.29 | 0 | 0 | +3.69 | -0.73 | +7.29 | +4.21 | PASS | 58.264 |
| `union_flow_in_white_h1` *(thin)* | long | 1 | leftover | list | none | 58% | 17% | 15/29 | +0.05 | 0 | 2 | +3.42 | -2.21 | +8.07 | +10.92 | PASS | 35.341 |
| `union_news_g_cam91_n1_h1` *(thin)* | long | 1 | leftover | list | none | 33% | 7% | 27/29 | +6.39 | 0 | 0 | +4.79 | -3.84 | +6.39 | -2.85 | PASS | 35.171 |
| `union_news_both_h3` *(thin)* | long | 3 | leftover | list | none | 20% | 14% | 21/29 | +4.04 | 0 | 2 | +4.37 | -0.51 | +4.04 | +1.48 | PASS | 26.466 |
| `union_catal_present_h1` *(thin)* | long | 1 | leftover | list | none | 30% | 14% | 10/29 | +0.00 | 0 | 2 | +8.76 | -2.82 | +1.03 | -2.59 | PASS | 14.533 |
| `union_clk_nr7_mom_opp_h1` *(thin)* | long | 1 | leftover | list | none | 75% | 10% | 12/29 | +0.00 | 0 | 4 | +4.03 | -2.38 | +10.45 | +0.01 | PASS | 12.431 |
| `union_news_missing_h1` *(thin)* | long | 1 | leftover | list | none | 50% | 3% | 1/29 | +0.00 | 0 | 2 | +7.93 | -3.05 | +1.44 | +1.88 | PASS | 9.778 |
| `union_vol_missing_h1` *(thin)* | long | 1 | leftover | list | none | 50% | 3% | 1/29 | +0.00 | 0 | 2 | +7.93 | -3.05 | +1.44 | +1.88 | PASS | 9.778 |
| `short_news_pack_h3` *(thin)* | short | 3 | leftover | list | none | 47% | 28% | 9/29 | +0.00 | 3 | 7 | +3.39 | -7.55 | +0.61 | -6.15 | PASS | 8.847 |
| `union_rsi_os_macd_h3` *(thin)* | long | 3 | leftover | list | none | 50% | 17% | 8/29 | -5.35 | 2 | 4 | +8.24 | -5.88 | -5.35 | +8.48 | PASS | 8.209 |
| `union_news_missing_h3` *(thin)* | long | 3 | leftover | list | none | 50% | 10% | 1/29 | +0.00 | 1 | 5 | +11.98 | -3.98 | +3.58 | +5.59 | PASS | 7.843 |
| `union_vol_missing_h3` *(thin)* | long | 3 | leftover | list | none | 50% | 10% | 1/29 | +0.00 | 1 | 5 | +11.98 | -3.98 | +3.58 | +5.59 | PASS | 7.843 |
| `union_catal_present_h3` *(thin)* | long | 3 | leftover | list | none | 30% | 17% | 0/29 | -0.74 | 2 | 6 | +12.05 | -4.43 | -0.74 | -12.69 | PASS | -0.063 |
| `union_flow_in_white_h3` *(thin)* | long | 3 | leftover | list | none | 50% | 14% | 2/29 | -0.10 | 0 | 6 | +3.30 | -4.79 | +2.10 | +4.80 | PASS | -1.756 |
| `union_clk_earn_guide_react_h1` *(thin)* | long | 1 | leftover | list | none | 45% | 7% | 0/29 | -3.11 | 1 | 4 | +5.68 | -6.17 | -5.43 | +10.90 | PASS | -2.553 |
| `overnight_mega_green_h1` *(thin)* | long | 1 | leftover | list | none | 35% | 10% | 0/29 | +0.00 | 0 | 2 | +2.51 | -6.44 | -8.87 | -9.06 | PASS | -5.553 |
| `union_rsi_os_macd_h1` *(thin)* | long | 1 | leftover | list | none | 17% | 7% | 1/29 | -8.27 | 1 | 3 | +7.49 | -3.44 | -8.27 | -3.93 | PASS | -8.939 |
| `union_clk_insider_cash_stab_h3` *(thin)* | long | 3 | leftover | list | none | 29% | 3% | 0/29 | -2.87 | 0 | 3 | +2.33 | -3.20 | -5.95 | -5.53 | PASS | -13.706 |
| `short_r_down_h1` *(thin)* | short | 1 | leftover | list | none | — | 0% | 0/29 | +0.00 | 0 | 0 | — | — | +0.00 | +0.00 | PASS | -15.0 |
| `short_r_down_h3` *(thin)* | short | 3 | leftover | list | none | — | 0% | 0/29 | +0.00 | 0 | 0 | — | — | +0.00 | +0.00 | PASS | -15.0 |
| `union_clk_r_up_coil_h1` *(thin)* | long | 1 | leftover | list | none | — | 0% | 0/29 | +0.00 | 0 | 0 | — | — | +0.00 | +0.00 | PASS | -15.0 |
| `union_r_up_h1` *(thin)* | long | 1 | leftover | list | none | — | 0% | 0/29 | +0.00 | 0 | 0 | — | — | +0.00 | +0.00 | PASS | -15.0 |
| `union_r_up_h3` *(thin)* | long | 3 | leftover | list | none | — | 0% | 0/29 | +0.00 | 0 | 0 | — | — | +0.00 | +0.00 | PASS | -15.0 |
