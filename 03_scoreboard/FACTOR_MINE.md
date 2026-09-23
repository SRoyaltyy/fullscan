# Factor strategy mine — 2026-08-13 → 2026-09-22

Leak-free 09:30 recipes: **339** · candidate rows **2373** · fill `09:30 open, whole shares, Futubull fees, leftover split, sell first, hard-red sit, cash+holdings audit`.

Cash book: $10k, whole shares, Futubull fees, leftover split, sell first, min-hold, 09:30 open, hard-red S≤−3 sit, shorts marked as a liability. Each session starts from leftover cash and lots actually held (butterfly). Cash-start buttons wake a sleeve on date X with $10k and no lots (same rules). Stock investigator quotes 09:30 cameras / coaches / news from repo files. Size / sell / S-boost tweaks sit on the same ledger. Signal-only % is the old equal-weight path (not a fill). `flatten_h*` = wish-list (io/HOLD mornings still buy). `flatten_live_*` = only when the live flatten gate fires. Research only — does not change live `flatten_robust`.

Scoreboard files: slim `factor_mine.json` (stats / recipes / series) plus gzip shards in `factor_mine/shards/` (`books`, `starts`, `daily`, `probe`, `sim`). One file used to be 104MB and GitHub rejected the publish (100MB cap).

Combination books: **79** mixes on the same $10k cash ledger (shared leftover or split sleeves, official 09:30 / 16:00, hard-red sit, owner min-hold). Outperformers: `combo_sh_3070_shared`, `combo_sh_macd_5050_shared`, `combo_sh_5050_shared`, `combo_ej_5050_shared`, `combo_es_9010_shared`, `combo_jer_5050_shared`, `combo_sf_7030_shared`, `combo_sf_5050_shared`, `combo_sf_3070_shared`.

A combo outperforms its members on the cash book when the audit passes and either (1) Book% is strictly higher than every member, or (2) Book% stays within 2pp of the richest member, max drawdown is strictly smaller than every member's drawdown, start-rate is at least the best member, both halves are green, and the worst session is no worse than the worst member session. Effectiveness / Signal% do not decide WIN.

Action blotters: [FACTOR_MINE_ACTION.md](FACTOR_MINE_ACTION.md).

| Strategy | Side | H | Size | Sell | Boost | Win% | $ days | Starts YES | Med start | Top-g | Losers | AvgW | AvgL | Book% | Signal% | Audit | Eff |
|---|---|---:|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---:|
| `combo_sh_5050_shared` | mix | 5 | leftover | list | none | 59% | 71% | 28/28 | +19.69 | 0 | 0 | +6.19 | -5.52 | +47.24 | — | PASS | 77.707 |
| `combo_sh_3070_shared` | mix | 5 | leftover | list | none | 58% | 68% | 28/28 | +27.40 | 0 | 0 | +6.23 | -5.43 | +55.15 | — | PASS | 76.706 |
| `combo_sh_macd_5050_shared` | mix | 5 | leftover | list | none | 59% | 64% | 28/28 | +28.19 | 0 | 0 | +6.33 | -5.16 | +54.92 | — | PASS | 76.588 |
| `combo_ej_5050_shared` | mix | 5 | leftover | list | none | 46% | 54% | 26/28 | +8.51 | 0 | 0 | +6.32 | -5.22 | +39.66 | — | PASS | 66.618 |
| `combo_es_9010_shared` | mix | 5 | leftover | list | none | 45% | 64% | 25/28 | +9.76 | 0 | 0 | +7.18 | -6.61 | +36.66 | — | PASS | 63.774 |
| `combo_jer_5050_shared` | mix | 5 | leftover | list | none | 43% | 50% | 24/28 | +3.59 | 0 | 0 | +6.50 | -5.49 | +32.53 | — | PASS | 61.218 |
| `combo_sf_7030_shared` | mix | 5 | leftover | list | none | 54% | 61% | 10/28 | -0.79 | 0 | 0 | +6.29 | -7.10 | +14.32 | — | PASS | 38.965 |
| `combo_sf_5050_shared` | mix | 5 | leftover | list | none | 53% | 64% | 8/28 | -1.69 | 0 | 0 | +6.54 | -6.54 | +13.62 | — | PASS | 37.985 |
| `combo_sf_3070_shared` | mix | 5 | leftover | list | none | 49% | 57% | 7/28 | -2.07 | 0 | 0 | +6.90 | -6.44 | +10.13 | — | PASS | 34.059 |
| `union_hot_n4_holdup` | long | 1 | leftover | list | holdup | 52% | 57% | 28/28 | +29.64 | 21 | 36 | +10.14 | -5.75 | +62.41 | +76.30 | PASS | 78.344 |
| `combo_sh_7030_shared` | mix | 5 | leftover | list | none | 59% | 71% | 28/28 | +8.58 | 0 | 0 | +6.17 | -5.56 | +29.76 | — | PASS | 76.101 |
| `union_hot_n4_h1` | long | 1 | leftover | list | none | 52% | 46% | 28/28 | +29.64 | 21 | 36 | +7.63 | -5.20 | +48.65 | +76.30 | PASS | 74.732 |
| `combo_oh_5050_shared` | mix | 5 | leftover | list | none | 53% | 46% | 28/28 | +25.97 | 0 | 0 | +7.14 | -5.25 | +30.30 | — | PASS | 73.153 |
| `combo_he1_5050_shared` | mix | 5 | leftover | list | none | 51% | 57% | 28/28 | +21.65 | 0 | 0 | +5.74 | -5.63 | +34.98 | — | PASS | 72.556 |
| `combo_seh_333_skip` | mix | 5 | leftover | list | none | 51% | 64% | 28/28 | +14.94 | 0 | 0 | +6.99 | -6.78 | +42.77 | — | PASS | 71.4 |
| `combo_hn_7030_shared` | mix | 5 | leftover | list | none | 48% | 50% | 28/28 | +18.19 | 0 | 0 | +5.20 | -4.86 | +32.34 | — | PASS | 70.925 |
| `combo_hn_3070_shared` | mix | 5 | leftover | list | none | 49% | 57% | 28/28 | +7.20 | 0 | 0 | +5.36 | -4.64 | +17.75 | — | PASS | 70.902 |
| `combo_seh_451540_shared` | mix | 5 | leftover | list | none | 48% | 71% | 28/28 | +12.22 | 0 | 0 | +7.14 | -6.81 | +33.94 | — | PASS | 70.635 |
| `combo_hn_5050_shared` | mix | 5 | leftover | list | none | 50% | 50% | 28/28 | +12.67 | 0 | 0 | +5.25 | -4.73 | +25.07 | — | PASS | 70.485 |
| `combo_seh_502525_shared` | mix | 5 | leftover | list | none | 48% | 75% | 27/28 | +6.76 | 0 | 0 | +7.10 | -6.57 | +29.32 | — | PASS | 70.074 |
| `union_hot_score_h1` | long | 1 | leftover | list | none | 50% | 50% | 28/28 | +14.20 | 36 | 65 | +6.26 | -5.78 | +17.40 | +42.54 | PASS | 69.812 |
| `combo_seh_333_shared` | mix | 5 | leftover | list | none | 49% | 64% | 28/28 | +11.03 | 0 | 0 | +7.04 | -6.63 | +34.59 | — | PASS | 69.763 |
| `combo_seh_333_weather` | mix | 5 | leftover | list | none | 49% | 64% | 28/28 | +11.03 | 0 | 0 | +7.04 | -6.63 | +34.59 | — | PASS | 69.763 |
| `combo_hj_5050_shared` | mix | 5 | leftover | list | none | 46% | 46% | 28/28 | +13.21 | 0 | 0 | +5.71 | -4.13 | +26.56 | — | PASS | 69.594 |
| `union_ret_5_h1` | long | 1 | leftover | list | none | 50% | 46% | 28/28 | +8.42 | 29 | 66 | +6.09 | -5.09 | +15.56 | +28.26 | PASS | 68.83 |
| `union_hot_n12_h1` | long | 1 | leftover | list | none | 53% | 50% | 28/28 | +11.07 | 43 | 91 | +5.33 | -5.53 | +9.07 | +31.93 | PASS | 68.791 |
| `union_w_hot_cond_h1` | long | 1 | leftover | list | none | 51% | 50% | 28/28 | +10.40 | 28 | 57 | +5.61 | -5.76 | +10.64 | +29.17 | PASS | 68.787 |
| `union_join_vol_green_h1` | long | 1 | leftover | list | none | 49% | 43% | 28/28 | +8.13 | 11 | 41 | +4.95 | -3.75 | +23.26 | +13.24 | PASS | 68.65 |
| `combo_seh_333_split` | mix | 5 | leftover | list | none | 49% | 64% | 28/28 | +9.57 | 0 | 0 | +7.03 | -6.68 | +25.35 | — | PASS | 68.56 |
| `combo_seh_403525_shared` | mix | 5 | leftover | list | none | 48% | 64% | 27/28 | +7.49 | 0 | 0 | +7.16 | -6.50 | +31.34 | — | PASS | 68.339 |
| `union_w_hot_candle_h1` | long | 1 | leftover | list | none | 52% | 46% | 28/28 | +11.21 | 29 | 58 | +5.19 | -5.22 | +10.61 | +34.99 | PASS | 68.327 |
| `combo_eh_5050_shared` | mix | 5 | leftover | list | none | 43% | 57% | 28/28 | +19.89 | 0 | 0 | +8.21 | -6.50 | +38.93 | — | PASS | 68.303 |
| `union_clk_nr7_mom_h1` | long | 1 | leftover | list | none | 52% | 43% | 28/28 | +17.63 | 3 | 14 | +5.07 | -4.34 | +19.98 | +32.06 | PASS | 67.771 |
| `union_clk_mom_break_peer_opp_h1` | long | 1 | leftover | list | none | 54% | 36% | 26/28 | +4.04 | 11 | 25 | +3.90 | -3.23 | +15.77 | +21.34 | PASS | 67.404 |
| `combo_eh_7030_shared` | mix | 5 | leftover | list | none | 40% | 54% | 28/28 | +17.73 | 0 | 0 | +8.65 | -6.29 | +37.75 | — | PASS | 67.028 |
| `combo_eh_3070_shared` | mix | 5 | leftover | list | none | 43% | 50% | 28/28 | +20.22 | 0 | 0 | +8.05 | -6.69 | +39.51 | — | PASS | 66.808 |
| `combo_seh_404020_shared` | mix | 5 | leftover | list | none | 49% | 61% | 26/28 | +5.74 | 0 | 0 | +6.99 | -6.58 | +29.86 | — | PASS | 66.673 |
| `combo_seh_601525_shared` | mix | 5 | leftover | list | none | 48% | 75% | 24/28 | +4.74 | 0 | 0 | +7.13 | -6.81 | +25.36 | — | PASS | 66.37 |
| `union_vol_ab_h1` | long | 1 | leftover | list | none | 53% | 36% | 26/28 | +8.22 | 17 | 44 | +5.66 | -4.21 | +11.88 | +67.07 | PASS | 66.04 |
| `combo_je1_5050_shared` | mix | 5 | leftover | list | none | 51% | 46% | 26/28 | +7.64 | 0 | 0 | +4.63 | -4.69 | +20.71 | — | PASS | 65.834 |
| `combo_ee1_3070_shared` | mix | 5 | leftover | list | none | 43% | 64% | 25/28 | +12.47 | 0 | 0 | +9.38 | -7.62 | +37.70 | — | PASS | 65.789 |
| `combo_ee1_5050_shared` | mix | 5 | leftover | list | none | 43% | 64% | 25/28 | +12.47 | 0 | 0 | +9.38 | -7.62 | +37.70 | — | PASS | 65.789 |
| `combo_ee1_7030_shared` | mix | 5 | leftover | list | none | 43% | 64% | 25/28 | +12.47 | 0 | 0 | +9.38 | -7.62 | +37.70 | — | PASS | 65.789 |
| `combo_ehs_702010_shared` | mix | 5 | leftover | list | none | 43% | 50% | 27/28 | +14.73 | 0 | 0 | +7.83 | -6.07 | +37.35 | — | PASS | 65.078 |
| `combo_her_5050_shared` | mix | 5 | leftover | list | none | 43% | 57% | 28/28 | +15.38 | 0 | 0 | +7.72 | -7.79 | +29.16 | — | PASS | 65.065 |
| `oppset_h1` | long | 1 | leftover | list | none | 54% | 39% | 26/28 | +7.35 | 17 | 56 | +6.04 | -5.08 | +6.35 | +17.09 | PASS | 64.924 |
| `union_break10_h1` | long | 1 | leftover | list | none | 51% | 43% | 26/28 | +6.15 | 26 | 46 | +4.97 | -4.78 | +0.48 | +13.74 | PASS | 64.898 |
| `union_oppset_h1` | long | 1 | leftover | list | none | 54% | 39% | 26/28 | +8.10 | 16 | 57 | +5.71 | -4.92 | +8.10 | +17.28 | PASS | 64.86 |
| `combo_se_5050_skip` | mix | 5 | leftover | list | none | 55% | 64% | 24/28 | +4.37 | 0 | 0 | +6.32 | -7.80 | +30.99 | — | PASS | 64.763 |
| `union_e_fresh_h3` | long | 3 | leftover | list | none | 46% | 64% | 25/28 | +12.47 | 20 | 90 | +9.38 | -7.62 | +37.70 | +7.23 | PASS | 64.434 |
| `combo_seh_502525_split` | mix | 5 | leftover | list | none | 48% | 68% | 24/28 | +5.12 | 0 | 0 | +7.26 | -6.49 | +17.31 | — | PASS | 64.303 |
| `combo_ehs_601525_shared` | mix | 5 | leftover | list | none | 47% | 54% | 25/28 | +9.25 | 0 | 0 | +7.21 | -6.43 | +33.38 | — | PASS | 64.225 |
| `union_news_pack_net2_h1` | long | 1 | leftover | list | none | 53% | 43% | 21/28 | +11.27 | 1 | 12 | +3.77 | -3.37 | +21.18 | +11.59 | PASS | 63.198 |
| `union_w_hot_cond_h3` | long | 3 | leftover | list | none | 47% | 57% | 27/28 | +8.02 | 45 | 150 | +10.29 | -7.58 | +16.37 | +255.20 | PASS | 63.154 |
| `union_w_hot_candle_h3` | long | 3 | leftover | list | none | 47% | 57% | 28/28 | +8.38 | 44 | 149 | +9.42 | -8.40 | +16.24 | +239.46 | PASS | 62.868 |
| `union_clk_hold_vs_sector_opp_h1` | long | 1 | leftover | list | none | 47% | 39% | 26/28 | +8.62 | 6 | 36 | +4.87 | -3.81 | +11.93 | +2.00 | PASS | 62.667 |
| `union_last_green_h1` | long | 1 | leftover | list | none | 47% | 46% | 21/28 | +3.57 | 15 | 45 | +4.73 | -3.30 | +10.92 | +18.94 | PASS | 62.615 |
| `union_hot_score_h3` | long | 3 | leftover | list | none | 44% | 61% | 27/28 | +8.73 | 52 | 161 | +11.93 | -8.48 | +18.08 | +280.22 | PASS | 62.602 |
| `combo_nj_5050_shared` | mix | 5 | leftover | list | none | 47% | 46% | 25/28 | +2.03 | 0 | 0 | +4.46 | -4.17 | +6.59 | — | PASS | 62.559 |
| `union_news_g_conv_h1` | long | 1 | conviction | list | none | 54% | 46% | 21/28 | +1.87 | 1 | 17 | +3.75 | -4.51 | +12.19 | +12.08 | PASS | 62.466 |
| `short_alarm_h1` | short | 1 | leftover | list | none | 55% | 25% | 28/28 | +1.12 | 7 | 53 | +3.88 | -3.06 | +2.29 | +9.77 | PASS | 62.064 |
| `union_clk_fresh_cat_coil_opp_h1` | long | 1 | leftover | list | none | 49% | 32% | 25/28 | +3.88 | 18 | 32 | +4.54 | -3.83 | +2.24 | +2.25 | PASS | 62.009 |
| `union_break10_h3` | long | 3 | leftover | list | none | 50% | 50% | 26/28 | +7.85 | 39 | 129 | +9.25 | -6.64 | +7.28 | +49.47 | PASS | 61.981 |
| `union_ret_5_h3` | long | 3 | leftover | list | none | 46% | 54% | 27/28 | +8.75 | 47 | 163 | +9.69 | -7.96 | +26.02 | +60.15 | PASS | 61.975 |
| `union_clk_mom_break_peer_h1` | long | 1 | leftover | list | none | 51% | 39% | 24/28 | +1.20 | 10 | 45 | +3.84 | -3.62 | +1.98 | +6.43 | PASS | 61.521 |
| `combo_es_8020_shared` | mix | 5 | leftover | list | none | 47% | 57% | 24/28 | +8.12 | 0 | 0 | +6.78 | -7.09 | +34.69 | — | PASS | 61.443 |
| `union_news_pack_h1` | long | 1 | leftover | list | none | 52% | 46% | 21/28 | +7.80 | 2 | 15 | +3.56 | -3.88 | +16.67 | +10.47 | PASS | 61.412 |
| `combo_en_5050_shared` | mix | 5 | leftover | list | none | 44% | 50% | 25/28 | +4.21 | 0 | 0 | +5.83 | -6.03 | +26.22 | — | PASS | 61.272 |
| `union_white_both_n4_h2` | long | 2 | leftover | list | none | 52% | 46% | 17/28 | +0.21 | 9 | 16 | +7.55 | -5.29 | +20.04 | +22.59 | PASS | 61.151 |
| `combo_sj_3070_shared` | mix | 5 | leftover | list | none | 54% | 61% | 17/28 | +1.36 | 0 | 0 | +4.95 | -4.53 | +22.91 | — | PASS | 60.836 |
| `union_vol_ab_h3` | long | 3 | leftover | list | none | 51% | 39% | 27/28 | +9.04 | 25 | 97 | +9.33 | -6.33 | +8.40 | +101.50 | PASS | 60.693 |
| `probable_probable_ok_h1` | long | 1 | leftover | list | none | 49% | 36% | 23/28 | +3.09 | 8 | 26 | +4.03 | -3.28 | +0.58 | +10.03 | PASS | 60.039 |
| `combo_se_3070_shared` | mix | 5 | leftover | list | none | 50% | 54% | 23/28 | +3.50 | 0 | 0 | +6.59 | -7.27 | +29.61 | — | PASS | 59.865 |
| `short_clk_ext_veto_opp_h3` | short | 3 | leftover | list | none | 53% | 50% | 26/28 | +1.56 | 12 | 54 | +7.25 | -6.12 | +3.65 | -19.37 | PASS | 59.56 |
| `combo_en_7030_shared` | mix | 5 | leftover | list | none | 41% | 43% | 25/28 | +6.09 | 0 | 0 | +6.06 | -5.94 | +29.09 | — | PASS | 59.381 |
| `union_white_both_n4_h5_s12` | long | 5 | leftover | list | none | 50% | 61% | 19/28 | +0.39 | 13 | 31 | +9.82 | -7.09 | +22.75 | +27.95 | PASS | 59.325 |
| `combo_hf_5050_shared` | mix | 5 | leftover | list | none | 47% | 54% | 21/28 | +2.98 | 0 | 0 | +7.25 | -6.05 | +11.19 | — | PASS | 58.671 |
| `union_e_fresh_h1` | long | 1 | leftover | list | none | 48% | 39% | 24/28 | +3.18 | 9 | 46 | +4.69 | -5.92 | +12.16 | +5.86 | PASS | 58.611 |
| `combo_en_3070_shared` | mix | 5 | leftover | list | none | 43% | 46% | 23/28 | +1.23 | 0 | 0 | +5.86 | -5.92 | +21.82 | — | PASS | 58.057 |
| `short_alarm_h3` | short | 3 | leftover | list | none | 57% | 36% | 28/28 | +3.64 | 14 | 101 | +6.68 | -8.15 | +5.89 | +13.90 | PASS | 57.467 |
| `union_white_both_n4_h5` | long | 5 | leftover | list | none | 50% | 57% | 19/28 | +0.39 | 13 | 31 | +9.82 | -8.82 | +21.08 | +27.95 | PASS | 57.006 |
| `combo_ef_7030_shared` | mix | 5 | leftover | list | none | 43% | 61% | 22/28 | +5.62 | 0 | 0 | +8.03 | -7.49 | +21.18 | — | PASS | 56.598 |
| `union_macd_up_h1` | long | 1 | leftover | list | none | 46% | 43% | 18/28 | +4.59 | 16 | 52 | +4.81 | -4.01 | +6.81 | +16.69 | PASS | 56.501 |
| `short_news_r_macd_h3` | short | 3 | leftover | list | none | 63% | 57% | 16/28 | +1.59 | 8 | 39 | +4.96 | -5.80 | +11.16 | +24.72 | PASS | 56.178 |
| `union_news_pack_net3_h1` | long | 1 | leftover | list | none | 52% | 36% | 16/28 | +3.24 | 1 | 11 | +3.56 | -3.37 | +12.40 | +7.24 | PASS | 55.393 |
| `combo_fse_333_shared` | mix | 5 | leftover | list | none | 47% | 57% | 21/28 | +2.00 | 0 | 0 | +6.62 | -7.29 | +15.59 | — | PASS | 55.237 |
| `union_join_vol_green_h3` | long | 3 | leftover | list | none | 53% | 39% | 23/28 | +6.99 | 21 | 87 | +7.53 | -6.91 | -0.67 | +48.50 | PASS | 55.033 |
| `short_news_r_h1` | short | 1 | leftover | list | none | 50% | 39% | 19/28 | +0.39 | 6 | 22 | +3.26 | -3.96 | -0.39 | +4.57 | PASS | 54.913 |
| `union_candle_h1` | long | 1 | leftover | list | none | 47% | 39% | 17/28 | +0.36 | 11 | 50 | +4.36 | -3.72 | +3.41 | +6.14 | PASS | 54.231 |
| `union_macd_up_h3` | long | 3 | leftover | list | none | 44% | 46% | 22/28 | +4.80 | 29 | 129 | +8.12 | -6.09 | +4.80 | +30.85 | PASS | 54.168 |
| `combo_se_5050_split` | mix | 5 | leftover | list | none | 50% | 57% | 17/28 | +2.01 | 0 | 0 | +6.98 | -6.95 | +16.73 | — | PASS | 53.975 |
| `combo_eer_5050_shared` | mix | 5 | leftover | list | none | 40% | 46% | 24/28 | +5.98 | 0 | 0 | +8.72 | -8.96 | +22.03 | — | PASS | 53.473 |
| `union_last_red_h3` | long | 3 | leftover | list | none | 39% | 54% | 23/28 | +3.16 | 24 | 130 | +8.59 | -6.74 | +4.90 | +8.78 | PASS | 53.431 |
| `union_vol_g_h3` | long | 3 | leftover | list | none | 51% | 50% | 21/28 | +3.09 | 29 | 127 | +8.29 | -7.38 | -1.84 | +35.58 | PASS | 53.375 |
| `combo_ef_5050_shared` | mix | 5 | leftover | list | none | 42% | 57% | 20/28 | +1.56 | 0 | 0 | +8.41 | -7.30 | +13.53 | — | PASS | 52.991 |
| `combo_fe_5050_shared` | mix | 5 | leftover | list | none | 42% | 57% | 20/28 | +1.56 | 0 | 0 | +8.41 | -7.30 | +13.53 | — | PASS | 52.991 |
| `union_clk_fresh_cat_coil_h1` | long | 1 | leftover | list | none | 48% | 39% | 14/28 | +0.03 | 7 | 39 | +3.44 | -3.22 | -0.65 | -2.03 | PASS | 51.659 |
| `union_clk_hold_vs_sector_h1` | long | 1 | leftover | list | none | 47% | 36% | 17/28 | +1.73 | 7 | 48 | +3.55 | -4.59 | +1.73 | -1.17 | PASS | 50.638 |
| `yday_gainer_h3` | long | 3 | leftover | list | none | 50% | 43% | 18/28 | +2.25 | 30 | 132 | +9.12 | -7.44 | -2.22 | +42.10 | PASS | 50.482 |
| `ohlc_hot_coil_h1` | long | 1 | leftover | list | none | 48% | 39% | 15/28 | +1.91 | 11 | 34 | +4.40 | -3.92 | -16.12 | -8.80 | PASS | 50.318 |
| `short_last_red_h1` | short | 1 | leftover | list | none | 50% | 32% | 18/28 | +1.03 | 11 | 73 | +4.42 | -4.47 | -7.25 | -3.02 | PASS | 50.317 |
| `combo_fes_403030_shared` | mix | 5 | leftover | list | none | 45% | 57% | 16/28 | +1.45 | 0 | 0 | +6.82 | -7.23 | +13.55 | — | PASS | 50.136 |
| `probable_h3` | long | 3 | leftover | list | none | 50% | 39% | 20/28 | +3.70 | 28 | 136 | +7.55 | -8.01 | -1.27 | +20.37 | PASS | 49.592 |
| `probable_probable_ok_h3` | long | 3 | leftover | list | none | 59% | 32% | 18/28 | +3.23 | 22 | 69 | +6.27 | -7.54 | -14.68 | +35.43 | PASS | 49.164 |
| `union_last_green_h3` | long | 3 | leftover | list | none | 54% | 39% | 14/28 | +0.69 | 30 | 118 | +6.69 | -7.26 | +2.45 | +54.80 | PASS | 48.413 |
| `union_vol_green_h3` | long | 3 | leftover | list | none | 54% | 36% | 18/28 | +1.19 | 32 | 112 | +6.62 | -7.23 | -9.36 | +33.28 | PASS | 47.977 |
| `combo_ef_3070_shared` | mix | 5 | leftover | list | none | 43% | 61% | 14/28 | +0.61 | 0 | 0 | +8.36 | -7.46 | +7.76 | — | PASS | 47.903 |
| `union_candle_score_h3` | long | 3 | leftover | list | none | 47% | 46% | 14/28 | +0.16 | 26 | 143 | +6.47 | -6.12 | +4.47 | +26.81 | PASS | 45.638 |
| `combo_sj_5050_shared` | mix | 5 | leftover | list | none | 54% | 61% | 12/28 | -1.19 | 0 | 0 | +4.94 | -4.54 | +16.88 | — | PASS | 45.444 |
| `union_white_both_n4_h1` | long | 1 | leftover | list | none | 50% | 39% | 7/28 | -1.54 | 4 | 10 | +6.99 | -3.20 | +17.32 | +9.51 | PASS | 44.282 |
| `union_macd_xup_h1` | long | 1 | leftover | list | none | 41% | 36% | 14/28 | +0.27 | 10 | 35 | +3.42 | -4.24 | -11.62 | -13.88 | PASS | 44.182 |
| `union_blue_vol_h3` | long | 3 | leftover | list | none | 43% | 39% | 14/28 | +0.00 | 19 | 98 | +10.24 | -6.93 | -3.51 | +22.73 | PASS | 43.393 |
| `combo_ps_7030_shared` | mix | 5 | leftover | list | none | 60% | 61% | 9/28 | -0.98 | 0 | 0 | +4.42 | -5.21 | +9.05 | — | PASS | 42.812 |
| `combo_jse_333_shared` | mix | 5 | leftover | list | none | 51% | 57% | 11/28 | -0.24 | 0 | 0 | +5.87 | -5.56 | +28.59 | — | PASS | 42.793 |
| `combo_se_5050_shared` | mix | 5 | leftover | list | none | 53% | 61% | 13/28 | -0.62 | 0 | 0 | +6.37 | -7.54 | +23.04 | — | PASS | 42.286 |
| `combo_se_5050_weather` | mix | 5 | leftover | list | none | 53% | 61% | 13/28 | -0.62 | 0 | 0 | +6.37 | -7.54 | +23.04 | — | PASS | 42.286 |
| `combo_fh_7030_shared` | mix | 5 | leftover | list | none | 45% | 64% | 12/28 | -1.09 | 0 | 0 | +7.32 | -6.28 | +4.88 | — | PASS | 41.108 |
| `union_news_or_h1` | long | 1 | leftover | list | none | 50% | 43% | 12/28 | -0.26 | 4 | 35 | +4.04 | -3.81 | +1.30 | +12.27 | PASS | 41.056 |
| `combo_sj_7030_shared` | mix | 5 | leftover | list | none | 52% | 57% | 10/28 | -2.49 | 0 | 0 | +5.05 | -4.49 | +7.68 | — | PASS | 40.643 |
| `combo_snj_333_shared` | mix | 5 | leftover | list | none | 50% | 57% | 10/28 | -4.01 | 0 | 0 | +4.59 | -4.56 | +5.71 | — | PASS | 40.311 |
| `combo_p2s_5050_shared` | mix | 5 | leftover | list | none | 60% | 64% | 4/28 | -1.81 | 0 | 0 | +4.59 | -4.96 | +7.58 | — | PASS | 39.333 |
| `union_news_head_h1` | long | 1 | leftover | list | none | 51% | 36% | 11/28 | -0.05 | 5 | 24 | +4.14 | -3.98 | -3.32 | +15.21 | PASS | 39.286 |
| `union_vol_green_h1` | long | 1 | leftover | list | none | 47% | 46% | 12/28 | -3.92 | 18 | 55 | +5.24 | -7.21 | +6.49 | -4.41 | PASS | 38.753 |
| `union_macd_xup_h3` | long | 3 | leftover | list | none | 41% | 39% | 14/28 | +0.29 | 19 | 78 | +6.08 | -7.85 | -16.93 | -48.34 | PASS | 38.686 |
| `union_news_or_net4_conv_h1` | long | 1 | conviction | list | none | 49% | 43% | 9/28 | -4.67 | 1 | 12 | +3.47 | -3.89 | +4.49 | -2.84 | PASS | 38.391 |
| `combo_ps_5050_shared` | mix | 5 | leftover | list | none | 60% | 64% | 3/28 | -4.08 | 0 | 0 | +4.46 | -5.13 | +5.91 | — | PASS | 37.8 |
| `union_h1_topheavy` | long | 1 | topheavy | list | none | 43% | 43% | 9/28 | -2.30 | 9 | 49 | +4.52 | -3.79 | +5.85 | +6.29 | PASS | 36.996 |
| `union_news_g_cond_h1` | long | 1 | leftover | list | none | 49% | 39% | 9/28 | -0.86 | 4 | 36 | +4.04 | -3.97 | +0.22 | +11.66 | PASS | 36.942 |
| `flatten_live_h1_topheavy` | long | 1 | topheavy | list | none | 54% | 11% | 7/28 | +0.00 | 1 | 5 | +6.75 | -3.75 | +7.19 | +4.45 | PASS | 36.597 |
| `combo_fe1_5050_shared` | mix | 5 | leftover | list | none | 46% | 57% | 11/28 | -1.45 | 0 | 0 | +5.75 | -6.31 | +3.24 | — | PASS | 36.369 |
| `flatten_h5_topheavy` | long | 5 | topheavy | list | none | 47% | 57% | 12/28 | -0.71 | 21 | 108 | +8.62 | -8.14 | +6.63 | +8.56 | PASS | 36.354 |
| `union_h5_topheavy` | long | 5 | topheavy | list | none | 48% | 61% | 12/28 | -2.17 | 33 | 152 | +8.60 | -8.27 | +2.68 | +26.43 | PASS | 36.21 |
| `union_join_present_h1` | long | 1 | leftover | list | none | 44% | 39% | 9/28 | -3.09 | 8 | 50 | +4.45 | -3.77 | +2.12 | +9.89 | PASS | 35.857 |
| `union_join_g_h1` | long | 1 | leftover | list | none | 46% | 39% | 9/28 | -0.84 | 9 | 52 | +4.09 | -3.89 | +2.58 | +4.95 | PASS | 35.848 |
| `union_last_green_h5` | long | 5 | leftover | list | none | 52% | 61% | 12/28 | -3.74 | 40 | 159 | +7.93 | -11.01 | +1.14 | +87.39 | PASS | 35.769 |
| `union_join_present_h3` | long | 3 | leftover | list | none | 45% | 50% | 8/28 | -0.69 | 20 | 114 | +9.63 | -5.46 | +3.27 | +19.00 | PASS | 35.655 |
| `flatten_live_h1` | long | 1 | leftover | list | none | 54% | 11% | 7/28 | +0.00 | 1 | 5 | +6.31 | -4.05 | +8.22 | +4.45 | PASS | 35.557 |
| `flatten_live_h1_cut` | long | 1 | leftover | cut_loser | none | 54% | 11% | 7/28 | +0.00 | 1 | 5 | +6.31 | -4.05 | +8.22 | +4.45 | PASS | 35.557 |
| `flatten_live_h1_sboost` | long | 1 | leftover | list | both | 54% | 11% | 7/28 | +0.00 | 1 | 5 | +6.31 | -4.05 | +8.22 | +4.45 | PASS | 35.557 |
| `flatten_live_h1_sizeup` | long | 1 | leftover | list | sizeup | 54% | 11% | 7/28 | +0.00 | 1 | 5 | +6.31 | -4.05 | +8.22 | +4.45 | PASS | 35.557 |
| `flatten_live_h1_time` | long | 1 | leftover | time | none | 54% | 11% | 7/28 | +0.00 | 1 | 5 | +6.31 | -4.05 | +8.22 | +4.45 | PASS | 35.557 |
| `flatten_live_h1_trail` | long | 1 | leftover | trail | none | 54% | 11% | 7/28 | +0.00 | 1 | 5 | +6.31 | -4.05 | +8.22 | +4.45 | PASS | 35.557 |
| `union_h5_time` | long | 5 | leftover | time | none | 48% | 68% | 9/28 | -2.05 | 33 | 152 | +8.94 | -8.07 | +4.31 | +26.43 | PASS | 35.539 |
| `union_h3_exit_red` | long | 3 | leftover | list | none | 52% | 39% | 12/28 | -2.01 | 30 | 118 | +6.75 | -6.37 | -2.34 | +37.13 | PASS | 35.516 |
| `flatten_live_h1_half` | long | 1 | half | list | none | 54% | 11% | 7/28 | +0.00 | 1 | 5 | +6.58 | -3.90 | +3.74 | +4.45 | PASS | 35.505 |
| `yday_gainer_h1` | long | 1 | leftover | list | none | 50% | 39% | 7/28 | -1.55 | 14 | 66 | +4.76 | -4.09 | +4.67 | +8.45 | PASS | 35.31 |
| `union_coil_green_h1` | long | 1 | leftover | list | none | 42% | 36% | 12/28 | -1.53 | 14 | 45 | +3.42 | -4.16 | -2.35 | -4.17 | PASS | 35.276 |
| `union_news_g_h1` | long | 1 | leftover | list | none | 51% | 46% | 5/28 | -0.31 | 4 | 35 | +3.98 | -4.23 | +1.82 | +12.50 | PASS | 35.246 |
| `union_news_present_h1` | long | 1 | leftover | list | none | 44% | 36% | 9/28 | -3.09 | 8 | 48 | +4.32 | -3.77 | +0.41 | +7.86 | PASS | 34.656 |
| `flatten_h5` | long | 5 | leftover | list | none | 47% | 64% | 8/28 | -1.45 | 21 | 108 | +8.90 | -8.31 | +8.55 | +8.56 | PASS | 34.565 |
| `flatten_h5_cut` | long | 5 | leftover | cut_loser | none | 47% | 64% | 8/28 | -1.45 | 21 | 108 | +8.90 | -8.31 | +8.55 | +8.56 | PASS | 34.565 |
| `flatten_h5_sizeup` | long | 5 | leftover | list | sizeup | 47% | 64% | 8/28 | -1.45 | 21 | 108 | +8.90 | -8.31 | +8.55 | +8.56 | PASS | 34.565 |
| `flatten_h5_time` | long | 5 | leftover | time | none | 47% | 64% | 8/28 | -1.45 | 21 | 108 | +8.90 | -8.31 | +8.55 | +8.56 | PASS | 34.565 |
| `flatten_h5_trail` | long | 5 | leftover | trail | none | 47% | 64% | 8/28 | -1.45 | 21 | 108 | +8.90 | -8.31 | +8.55 | +8.56 | PASS | 34.565 |
| `union_flow_in_h1` | long | 1 | leftover | list | none | 46% | 32% | 6/28 | -1.58 | 4 | 9 | +4.13 | -4.23 | +7.80 | +15.15 | PASS | 34.558 |
| `union_h3_cut` | long | 3 | leftover | cut_loser | none | 46% | 54% | 6/28 | -3.43 | 21 | 111 | +8.94 | -5.26 | +1.75 | +15.50 | PASS | 34.533 |
| `union_h3_trail` | long | 3 | leftover | trail | none | 46% | 54% | 6/28 | -3.43 | 21 | 111 | +8.94 | -5.26 | +1.75 | +15.50 | PASS | 34.533 |
| `ohlc_hot_h1` | long | 1 | leftover | list | none | 45% | 39% | 12/28 | -0.33 | 16 | 56 | +3.31 | -4.98 | -9.29 | +0.88 | PASS | 34.389 |
| `union_news_or_net4_rw_h1` | long | 1 | rank_w | list | none | 49% | 46% | 5/28 | -6.91 | 1 | 12 | +3.36 | -4.04 | -2.40 | -2.84 | PASS | 34.197 |
| `union_h3_time` | long | 3 | leftover | time | none | 46% | 54% | 6/28 | -3.43 | 21 | 111 | +8.32 | -5.34 | +2.30 | +15.50 | PASS | 33.914 |
| `union_h5` | long | 5 | leftover | list | none | 48% | 64% | 8/28 | -2.05 | 33 | 152 | +8.84 | -8.09 | +4.00 | +26.43 | PASS | 33.812 |
| `union_h5_cut` | long | 5 | leftover | cut_loser | none | 48% | 64% | 8/28 | -2.05 | 33 | 152 | +8.84 | -8.09 | +4.00 | +26.43 | PASS | 33.812 |
| `union_h5_sizeup` | long | 5 | leftover | list | sizeup | 48% | 64% | 8/28 | -2.05 | 33 | 152 | +8.84 | -8.09 | +4.00 | +26.43 | PASS | 33.812 |
| `union_h5_trail` | long | 5 | leftover | trail | none | 48% | 64% | 8/28 | -2.05 | 33 | 152 | +8.84 | -8.09 | +4.00 | +26.43 | PASS | 33.812 |
| `flatten_live_h1_rankw` | long | 1 | rank_w | list | none | 54% | 11% | 6/28 | +0.00 | 1 | 5 | +6.14 | -4.12 | +4.03 | +4.45 | PASS | 33.692 |
| `combo_ne1_5050_shared` | mix | 5 | leftover | list | none | 50% | 43% | 5/28 | -1.80 | 0 | 0 | +4.10 | -5.08 | +7.20 | — | PASS | 33.426 |
| `flatten_white_yday_h5` | long | 5 | leftover | list | none | 55% | 39% | 12/28 | +0.00 | 5 | 23 | +9.10 | -10.47 | +1.51 | +4.79 | PASS | 33.297 |
| `union_join_g_h3` | long | 3 | leftover | list | none | 46% | 36% | 11/28 | -2.91 | 22 | 115 | +7.87 | -5.62 | -1.14 | +10.07 | PASS | 33.296 |
| `flatten_h5_sboost` | long | 5 | leftover | list | both | 47% | 64% | 7/28 | -1.45 | 21 | 108 | +8.68 | -8.12 | +6.13 | +8.56 | PASS | 33.289 |
| `union_earn_react_h3` | long | 3 | leftover | list | none | 40% | 50% | 13/28 | -1.24 | 15 | 89 | +8.14 | -9.05 | +18.99 | -14.53 | PASS | 33.265 |
| `union_h3_exit_alarm` | long | 3 | leftover | list | none | 46% | 50% | 8/28 | -1.04 | 20 | 114 | +7.19 | -5.65 | +2.33 | +19.56 | PASS | 33.224 |
| `union_h1_rankw` | long | 1 | rank_w | list | none | 43% | 43% | 6/28 | -2.41 | 9 | 49 | +4.30 | -3.96 | +2.09 | +6.29 | PASS | 33.22 |
| `combo_jf_5050_shared` | mix | 5 | leftover | list | none | 44% | 57% | 6/28 | -4.15 | 0 | 0 | +5.81 | -5.01 | +5.01 | — | PASS | 33.048 |
| `union_h3` | long | 3 | leftover | list | none | 46% | 54% | 5/28 | -3.43 | 21 | 111 | +8.94 | -5.53 | +0.25 | +15.50 | PASS | 33.015 |
| `union_h3_sizeup` | long | 3 | leftover | list | sizeup | 46% | 54% | 5/28 | -3.43 | 21 | 111 | +8.94 | -5.53 | +0.25 | +15.50 | PASS | 33.015 |
| `union_h5_exit_alarm` | long | 5 | leftover | list | none | 50% | 54% | 9/28 | -0.24 | 32 | 154 | +7.12 | -7.00 | +5.77 | +41.15 | PASS | 32.912 |
| `union_white_both_n4_h3` | long | 3 | leftover | list | none | 48% | 36% | 9/28 | -3.35 | 11 | 26 | +8.18 | -6.20 | +1.71 | +31.19 | PASS | 32.625 |
| `union_white_yday_h1` | long | 1 | leftover | list | none | 49% | 36% | 3/28 | -4.78 | 2 | 13 | +4.49 | -3.61 | -0.94 | +3.24 | PASS | 32.612 |
| `union_ab_g_h1` | long | 1 | leftover | list | none | 43% | 36% | 7/28 | -3.77 | 7 | 41 | +3.82 | -3.30 | +0.51 | -2.39 | PASS | 32.535 |
| `union_h5_sboost` | long | 5 | leftover | list | both | 48% | 64% | 7/28 | -2.32 | 33 | 152 | +8.36 | -8.56 | +3.28 | +26.43 | PASS | 32.23 |
| `union_white_h1` | long | 1 | leftover | list | none | 37% | 36% | 9/28 | -2.55 | 4 | 27 | +6.14 | -4.26 | +2.52 | -6.25 | PASS | 32.203 |
| `union_h3_exit_news_r` | long | 3 | leftover | list | none | 45% | 46% | 6/28 | -1.75 | 19 | 114 | +9.46 | -5.70 | +1.37 | +13.45 | PASS | 32.109 |
| `union_h3_topheavy` | long | 3 | topheavy | list | none | 46% | 46% | 7/28 | -2.48 | 21 | 111 | +7.53 | -5.85 | +0.07 | +15.50 | PASS | 31.695 |
| `combo_e1s_7030_shared` | mix | 5 | leftover | list | none | 56% | 57% | 1/28 | -5.96 | 0 | 0 | +4.88 | -6.22 | +5.98 | — | PASS | 31.643 |
| `yday_gainer_h5` | long | 5 | leftover | list | none | 52% | 50% | 10/28 | -2.02 | 42 | 166 | +8.56 | -9.87 | +0.22 | +37.08 | PASS | 31.582 |
| `union_h3_sboost` | long | 3 | leftover | list | both | 46% | 54% | 4/28 | -3.33 | 21 | 111 | +8.62 | -5.64 | -0.46 | +15.50 | PASS | 31.575 |
| `combo_se_7030_shared` | mix | 5 | leftover | list | none | 52% | 61% | 2/28 | -3.84 | 0 | 0 | +6.44 | -7.33 | +18.15 | — | PASS | 31.385 |
| `union_white_coil_h1` | long | 1 | leftover | list | none | 40% | 32% | 9/28 | -1.05 | 4 | 20 | +4.96 | -4.00 | -4.50 | -3.58 | PASS | 31.233 |
| `combo_se1_5050_shared` | mix | 5 | leftover | list | none | 56% | 54% | 1/28 | -8.35 | 0 | 0 | +4.80 | -6.38 | +5.50 | — | PASS | 31.023 |
| `combo_sn_3070_shared` | mix | 5 | leftover | list | none | 53% | 57% | 1/28 | -7.08 | 0 | 0 | +4.39 | -4.80 | -1.98 | — | PASS | 31.019 |
| `union_news_g_conv_h3` | long | 3 | conviction | list | none | 52% | 39% | 10/28 | -0.96 | 7 | 55 | +4.60 | -7.42 | -2.27 | +247.78 | PASS | 30.913 |
| `union_news_present_h3` | long | 3 | leftover | list | none | 45% | 46% | 7/28 | -0.69 | 19 | 109 | +7.48 | -5.52 | -4.72 | +12.70 | PASS | 30.799 |
| `short_news_head_h3` | short | 3 | leftover | list | none | 57% | 61% | 2/28 | -5.62 | 9 | 45 | +5.56 | -6.00 | +0.28 | -4.78 | PASS | 30.755 |
| `combo_ner_5050_shared` | mix | 5 | leftover | list | none | 41% | 46% | 6/28 | -0.63 | 0 | 0 | +5.74 | -6.32 | +18.98 | — | PASS | 30.749 |
| `union_coil_off_h1` | long | 1 | leftover | list | none | 43% | 32% | 11/28 | -4.78 | 8 | 56 | +3.10 | -4.20 | -11.12 | -9.62 | PASS | 30.732 |
| `union_candle_score_h1` | long | 1 | leftover | list | none | 51% | 36% | 2/28 | -2.08 | 18 | 57 | +4.38 | -4.09 | -0.59 | +9.43 | PASS | 30.656 |
| `union_h5_half` | long | 5 | half | list | none | 48% | 61% | 5/28 | -1.58 | 33 | 152 | +7.93 | -7.20 | +2.56 | +26.43 | PASS | 30.252 |
| `union_h3_half` | long | 3 | half | list | none | 46% | 50% | 3/28 | -5.93 | 21 | 111 | +8.55 | -4.98 | -4.95 | +15.50 | PASS | 30.242 |
| `combo_sn_5050_shared` | mix | 5 | leftover | list | none | 54% | 57% | 0/28 | -7.91 | 0 | 0 | +4.30 | -4.93 | -3.40 | — | PASS | 30.16 |
| `union_h3_rankw` | long | 3 | rank_w | list | none | 46% | 50% | 4/28 | -6.89 | 21 | 111 | +8.83 | -5.72 | -5.71 | +15.50 | PASS | 30.139 |
| `flatten_h5_s8` | long | 5 | leftover | list | none | 47% | 64% | 3/28 | -3.62 | 21 | 108 | +8.86 | -8.29 | +7.38 | +8.56 | PASS | 29.909 |
| `combo_sn_7030_shared` | mix | 5 | leftover | list | none | 54% | 57% | 0/28 | -8.47 | 0 | 0 | +4.16 | -5.12 | -4.28 | — | PASS | 29.893 |
| `probable_h1` | long | 1 | leftover | list | none | 49% | 36% | 3/28 | -2.77 | 13 | 64 | +4.14 | -3.60 | -2.44 | +7.18 | PASS | 29.803 |
| `short_macd_dn_h1` | short | 1 | leftover | list | none | 52% | 25% | 3/28 | -4.99 | 12 | 60 | +5.67 | -4.29 | -8.82 | -1.39 | PASS | 29.245 |
| `union_white_h5` | long | 5 | leftover | list | none | 48% | 61% | 6/28 | -5.50 | 19 | 72 | +9.83 | -9.26 | -0.05 | +46.77 | PASS | 29.143 |
| `union_h5_rankw` | long | 5 | rank_w | list | none | 48% | 61% | 5/28 | -5.16 | 33 | 152 | +7.69 | -8.54 | -0.59 | +26.43 | PASS | 28.763 |
| `combo_nse_333_shared` | mix | 5 | leftover | list | none | 48% | 50% | 1/28 | -5.06 | 0 | 0 | +5.56 | -5.96 | +14.58 | — | PASS | 28.704 |
| `combo_nf_5050_shared` | mix | 5 | leftover | list | none | 40% | 54% | 3/28 | -4.08 | 0 | 0 | +6.62 | -5.44 | +2.33 | — | PASS | 28.635 |
| `union_white_any_h2` | long | 2 | leftover | list | none | 44% | 43% | 5/28 | -5.19 | 11 | 40 | +6.33 | -5.96 | -3.47 | +8.08 | PASS | 28.118 |
| `combo_fer_5050_shared` | mix | 5 | leftover | list | none | 40% | 50% | 8/28 | -2.24 | 0 | 0 | +7.99 | -8.14 | +8.47 | — | PASS | 27.993 |
| `union_vol_g_h5` | long | 5 | leftover | list | none | 51% | 43% | 9/28 | -1.03 | 37 | 150 | +12.33 | -10.84 | -10.35 | +25.60 | PASS | 27.986 |
| `flatten_h1` | long | 1 | leftover | list | none | 42% | 39% | 1/28 | -5.62 | 3 | 32 | +4.12 | -3.57 | +0.04 | -6.15 | PASS | 27.885 |
| `flatten_h5_rankw` | long | 5 | rank_w | list | none | 47% | 54% | 4/28 | -2.48 | 21 | 108 | +8.25 | -8.53 | +3.93 | +8.56 | PASS | 27.628 |
| `union_white_any_h5` | long | 5 | leftover | list | none | 43% | 64% | 6/28 | -8.65 | 17 | 69 | +9.10 | -9.20 | +1.71 | -5.15 | PASS | 27.544 |
| `flatten_h3_sboost` | long | 3 | leftover | list | both | 46% | 43% | 2/28 | -10.38 | 13 | 75 | +7.38 | -4.90 | -8.00 | +5.94 | PASS | 27.541 |
| `flatten_h3` | long | 3 | leftover | list | none | 46% | 43% | 2/28 | -10.38 | 13 | 75 | +7.64 | -5.10 | -7.94 | +5.94 | PASS | 27.508 |
| `flatten_h3_cut` | long | 3 | leftover | cut_loser | none | 46% | 43% | 2/28 | -10.38 | 13 | 75 | +7.64 | -5.10 | -7.94 | +5.94 | PASS | 27.508 |
| `flatten_h3_sizeup` | long | 3 | leftover | list | sizeup | 46% | 43% | 2/28 | -10.38 | 13 | 75 | +7.64 | -5.10 | -7.94 | +5.94 | PASS | 27.508 |
| `flatten_h3_trail` | long | 3 | leftover | trail | none | 46% | 43% | 2/28 | -10.38 | 13 | 75 | +7.64 | -5.10 | -7.94 | +5.94 | PASS | 27.508 |
| `flatten_h3_half` | long | 3 | half | list | none | 46% | 43% | 2/28 | -4.77 | 13 | 75 | +6.57 | -4.76 | -4.29 | +5.94 | PASS | 27.466 |
| `flatten_h3_rankw` | long | 3 | rank_w | list | none | 46% | 39% | 3/28 | -6.56 | 13 | 75 | +7.45 | -5.29 | -7.66 | +5.94 | PASS | 27.28 |
| `union_h1` | long | 1 | leftover | list | none | 43% | 39% | 0/28 | -5.50 | 9 | 49 | +4.40 | -3.81 | -0.44 | +6.29 | PASS | 27.114 |
| `union_h1_sizeup` | long | 1 | leftover | list | sizeup | 43% | 39% | 0/28 | -5.50 | 9 | 49 | +4.40 | -3.81 | -0.44 | +6.29 | PASS | 27.114 |
| `union_h1_trail` | long | 1 | leftover | trail | none | 43% | 39% | 0/28 | -5.50 | 9 | 49 | +4.40 | -3.81 | -0.44 | +6.29 | PASS | 27.114 |
| `flatten_h3_time` | long | 3 | leftover | time | none | 46% | 43% | 2/28 | -10.38 | 13 | 75 | +6.99 | -5.38 | -4.32 | +5.94 | PASS | 27.046 |
| `flatten_h5_half` | long | 5 | half | list | none | 47% | 54% | 3/28 | -3.33 | 21 | 108 | +7.81 | -7.00 | +0.74 | +8.56 | PASS | 26.999 |
| `union_blue_h3` | long | 3 | leftover | list | none | 44% | 43% | 6/28 | -4.77 | 22 | 105 | +6.65 | -5.97 | -9.14 | -0.44 | PASS | 26.96 |
| `union_blue_coil_h3` | long | 3 | leftover | list | none | 44% | 46% | 4/28 | -3.79 | 17 | 100 | +7.16 | -5.46 | -7.97 | -21.49 | PASS | 26.755 |
| `union_macd_hist_h1` | long | 1 | leftover | list | none | 45% | 32% | 5/28 | -2.91 | 10 | 73 | +4.02 | -4.16 | -8.04 | -5.20 | PASS | 26.714 |
| `union_h1_time` | long | 1 | leftover | time | none | 43% | 36% | 0/28 | -5.58 | 9 | 49 | +4.31 | -3.51 | -0.93 | +6.29 | PASS | 26.685 |
| `short_news_or_h3` | short | 3 | leftover | list | none | 55% | 54% | 0/28 | -5.55 | 12 | 52 | +4.84 | -6.47 | -1.31 | -8.75 | PASS | 26.622 |
| `union_h1_sboost` | long | 1 | leftover | list | both | 43% | 39% | 0/28 | -7.38 | 9 | 49 | +4.28 | -3.94 | -2.37 | +6.29 | PASS | 26.484 |
| `union_h1_cut` | long | 1 | leftover | cut_loser | none | 43% | 36% | 0/28 | -5.58 | 9 | 49 | +4.40 | -3.78 | -0.61 | +6.29 | PASS | 26.428 |
| `combo_ser_5050_shared` | mix | 5 | leftover | list | none | 48% | 54% | 1/28 | -5.80 | 0 | 0 | +6.34 | -7.62 | +14.26 | — | PASS | 26.399 |
| `union_cond_h3` | long | 3 | leftover | list | none | 44% | 46% | 7/28 | -6.74 | 15 | 125 | +5.69 | -6.54 | -5.72 | -10.07 | PASS | 26.368 |
| `short_news_r_h3` | short | 3 | leftover | list | none | 55% | 54% | 0/28 | -5.63 | 12 | 52 | +4.93 | -6.47 | -1.48 | -8.71 | PASS | 26.35 |
| `union_candle_h3` | long | 3 | leftover | list | none | 49% | 29% | 7/28 | -3.28 | 29 | 125 | +6.68 | -6.90 | -6.04 | +23.80 | PASS | 26.196 |
| `union_rsi_os_h3` | long | 3 | leftover | list | none | 50% | 46% | 13/28 | +0.00 | 4 | 50 | +5.13 | -9.38 | -19.37 | -31.06 | PASS | 26.037 |
| `flatten_h3_topheavy` | long | 3 | topheavy | list | none | 46% | 39% | 3/28 | -6.12 | 13 | 75 | +5.86 | -5.31 | -5.83 | +5.94 | PASS | 26.028 |
| `union_cond_h1` | long | 1 | leftover | list | none | 48% | 29% | 1/28 | -5.04 | 3 | 45 | +4.01 | -4.01 | -7.40 | +0.07 | PASS | 25.969 |
| `union_h1_half` | long | 1 | half | list | none | 43% | 36% | 0/28 | -3.48 | 9 | 49 | +4.37 | -3.90 | -2.71 | +6.29 | PASS | 25.882 |
| `union_flow_in_h3` | long | 3 | leftover | list | none | 48% | 36% | 6/28 | -0.39 | 7 | 33 | +3.19 | -6.30 | -0.28 | +6.34 | PASS | 25.815 |
| `union_news_or_net4_h1` | long | 1 | leftover | list | none | 46% | 36% | 1/28 | -11.12 | 1 | 21 | +3.77 | -3.90 | -9.03 | -4.84 | PASS | 25.622 |
| `union_blue_h1` | long | 1 | leftover | list | none | 41% | 25% | 3/28 | -2.33 | 11 | 47 | +4.36 | -3.93 | -2.15 | -2.98 | PASS | 25.355 |
| `ohlc_hot_h3` | long | 3 | leftover | list | none | 47% | 43% | 7/28 | -1.42 | 32 | 158 | +5.61 | -6.83 | -2.44 | +97.35 | PASS | 24.862 |
| `short_rsi_ob_h1` | short | 1 | leftover | list | none | 50% | 32% | 1/28 | -4.35 | 17 | 63 | +3.89 | -4.83 | -11.49 | -13.10 | PASS | 24.861 |
| `union_cond_n4_h3` | long | 3 | leftover | list | none | 45% | 43% | 7/28 | -5.25 | 9 | 62 | +4.11 | -7.70 | -4.61 | -2.62 | PASS | 24.786 |
| `union_news_or_net2_h1` | long | 1 | leftover | list | none | 45% | 32% | 0/28 | -4.20 | 1 | 27 | +3.96 | -3.88 | -4.56 | -2.00 | PASS | 24.758 |
| `combo_ers_7030_shared` | mix | 5 | leftover | list | none | 47% | 46% | 1/28 | -4.85 | 0 | 0 | +6.46 | -7.72 | +16.24 | — | PASS | 24.6 |
| `union_news_g_cam71_h1` | long | 1 | leftover | list | none | 36% | 36% | 4/28 | -8.10 | 1 | 6 | +3.04 | -4.53 | -8.10 | -7.29 | PASS | 24.528 |
| `union_earn_react_h1` | long | 1 | leftover | list | none | 47% | 29% | 4/28 | -1.87 | 7 | 50 | +4.63 | -7.25 | -1.27 | -2.04 | PASS | 24.385 |
| `union_flow_in_h5` | long | 5 | leftover | list | none | 54% | 39% | 4/28 | -3.12 | 8 | 45 | +4.08 | -8.58 | +4.05 | -8.73 | PASS | 24.264 |
| `union_news_pack_net2_h3` | long | 3 | leftover | list | none | 40% | 29% | 10/28 | -0.03 | 3 | 27 | +5.95 | -7.59 | -11.73 | -9.16 | PASS | 24.257 |
| `union_ab_g_h3` | long | 3 | leftover | list | none | 42% | 29% | 6/28 | -9.56 | 20 | 98 | +7.49 | -5.42 | -10.24 | -2.88 | PASS | 24.159 |
| `union_news_g_cam71_conv_h1` | long | 1 | conviction | list | none | 32% | 29% | 6/28 | -4.72 | 1 | 5 | +2.98 | -4.79 | -3.66 | -9.24 | PASS | 24.067 |
| `union_vol_g_h1` | long | 1 | leftover | list | none | 49% | 39% | 0/28 | -9.11 | 16 | 66 | +5.40 | -7.34 | -8.62 | +6.64 | PASS | 23.935 |
| `union_white_yday_h3` | long | 3 | leftover | list | none | 46% | 46% | 5/28 | -13.13 | 6 | 45 | +5.68 | -8.15 | -13.13 | -9.14 | PASS | 23.919 |
| `short_last_red_h3` | short | 3 | leftover | list | none | 57% | 39% | 2/28 | -6.28 | 24 | 126 | +5.28 | -7.55 | -12.88 | -21.49 | PASS | 23.915 |
| `union_news_vol_h3` | long | 3 | leftover | list | none | 51% | 46% | 2/28 | -6.55 | 10 | 55 | +4.25 | -5.14 | -8.42 | +203.96 | PASS | 23.714 |
| `union_news_vol_h1` | long | 1 | leftover | list | none | 45% | 25% | 0/28 | -5.66 | 5 | 21 | +4.24 | -4.39 | -4.10 | +12.72 | PASS | 23.437 |
| `union_white_any_h1` | long | 1 | leftover | list | none | 36% | 29% | 2/28 | -5.35 | 4 | 24 | +6.50 | -4.34 | -7.52 | -8.37 | PASS | 23.397 |
| `union_e_green_h3` | long | 3 | leftover | list | none | 39% | 32% | 12/28 | -2.11 | 5 | 47 | +5.47 | -8.76 | -2.96 | -23.61 | PASS | 22.934 |
| `overnight_h5` | long | 5 | leftover | list | none | 45% | 46% | 8/28 | -2.00 | 19 | 106 | +9.12 | -12.55 | -7.89 | -24.07 | PASS | 22.858 |
| `union_e_green_h1` | long | 1 | leftover | list | none | 46% | 29% | 5/28 | -5.17 | 3 | 18 | +4.06 | -8.96 | -23.90 | -20.55 | PASS | 22.846 |
| `union_last_red_h1` | long | 1 | leftover | list | none | 48% | 32% | 1/28 | -7.65 | 12 | 72 | +3.60 | -5.12 | -8.33 | +6.94 | PASS | 22.832 |
| `union_coil_green_h3` | long | 3 | leftover | list | none | 45% | 29% | 7/28 | -9.04 | 28 | 127 | +5.80 | -7.13 | -6.84 | -8.40 | PASS | 22.816 |
| `combo_e1er_5050_shared` | mix | 5 | leftover | list | none | 45% | 18% | 8/28 | -1.34 | 0 | 0 | +4.61 | -7.55 | -3.90 | — | PASS | 22.523 |
| `flatten_vol_g_h3` | long | 3 | leftover | list | none | 52% | 25% | 5/28 | -14.33 | 6 | 25 | +9.63 | -9.35 | -22.82 | -13.69 | PASS | 22.378 |
| `union_news_or_net5_h1` | long | 1 | leftover | list | none | 42% | 29% | 1/28 | -11.11 | 1 | 13 | +3.21 | -4.28 | -10.29 | -5.36 | PASS | 22.075 |
| `union_news_or_net4_h3` | long | 3 | leftover | list | none | 44% | 36% | 3/28 | -14.69 | 8 | 47 | +6.05 | -5.32 | -15.94 | -11.90 | PASS | 22.071 |
| `probable_h5` | long | 5 | leftover | list | none | 52% | 36% | 7/28 | -5.97 | 39 | 170 | +6.61 | -10.49 | -14.58 | +9.37 | PASS | 22.064 |
| `union_news_or_net2_h3` | long | 3 | leftover | list | none | 44% | 39% | 3/28 | -6.36 | 9 | 70 | +5.88 | -5.76 | -10.31 | -7.42 | PASS | 21.965 |
| `short_rsi_ob_h3` | short | 3 | leftover | list | none | 52% | 39% | 2/28 | -4.14 | 33 | 138 | +5.56 | -7.72 | -9.81 | -125.34 | PASS | 21.893 |
| `short_macd_dn_h3` | short | 3 | leftover | list | none | 48% | 43% | 2/28 | -5.67 | 27 | 122 | +4.45 | -6.44 | -12.15 | -21.64 | PASS | 21.832 |
| `union_news_g_cam71_n2_h1` | long | 1 | topheavy | list | none | 36% | 29% | 4/28 | -9.51 | 1 | 4 | +2.61 | -5.06 | -12.21 | -10.60 | PASS | 21.83 |
| `short_extended_h1` | short | 1 | leftover | list | none | 50% | 25% | 0/28 | -10.97 | 29 | 68 | +4.84 | -6.28 | -18.02 | -14.62 | PASS | 21.817 |
| `short_clk_ext_veto_h3` | short | 3 | leftover | list | none | 50% | 43% | 0/28 | -7.04 | 30 | 110 | +7.38 | -7.20 | -10.88 | -96.05 | PASS | 21.572 |
| `union_news_or_net3_h1` | long | 1 | leftover | list | none | 45% | 29% | 0/28 | -13.82 | 1 | 25 | +3.79 | -4.25 | -15.32 | -7.92 | PASS | 21.282 |
| `union_news_g_h3` | long | 3 | leftover | list | none | 51% | 29% | 4/28 | -10.86 | 19 | 93 | +4.56 | -6.76 | -18.47 | +185.84 | PASS | 20.961 |
| `union_white_h3` | long | 3 | leftover | list | none | 43% | 39% | 6/28 | -15.86 | 12 | 61 | +6.74 | -7.50 | -15.86 | +11.18 | PASS | 20.725 |
| `short_extended_h3` | short | 3 | leftover | list | none | 52% | 43% | 0/28 | -14.02 | 51 | 144 | +7.63 | -9.71 | -17.76 | -41.01 | PASS | 20.578 |
| `union_news_pack_h3` | long | 3 | leftover | list | none | 40% | 29% | 7/28 | -1.39 | 5 | 31 | +5.43 | -8.52 | -13.42 | -16.25 | PASS | 20.478 |
| `flatten_live_h3` | long | 3 | leftover | list | none | 58% | 11% | 0/28 | -6.24 | 4 | 14 | +8.60 | -8.85 | -6.24 | +6.87 | PASS | 20.233 |
| `coil_h3_exit_alarm` | long | 3 | leftover | list | none | 42% | 29% | 7/28 | -7.86 | 17 | 129 | +4.13 | -6.72 | -7.37 | -2.29 | PASS | 20.174 |
| `union_news_or_h3` | long | 3 | leftover | list | none | 48% | 25% | 4/28 | -10.74 | 16 | 96 | +5.42 | -6.05 | -14.82 | +185.29 | PASS | 20.121 |
| `union_news_head_h3` | long | 3 | leftover | list | none | 50% | 21% | 4/28 | -11.97 | 18 | 77 | +5.33 | -6.14 | -21.75 | +188.84 | PASS | 20.042 |
| `union_coil_off_h3` | long | 3 | leftover | list | none | 43% | 29% | 7/28 | -9.16 | 22 | 126 | +4.57 | -7.66 | -12.51 | -11.94 | PASS | 19.998 |
| `union_news_g_cond_h3` | long | 3 | leftover | list | none | 48% | 25% | 4/28 | -10.74 | 16 | 96 | +5.50 | -6.22 | -15.56 | +183.56 | PASS | 19.965 |
| `short_clk_neg_weak_fail_h3` | short | 3 | leftover | list | none | 49% | 36% | 1/28 | -4.54 | 22 | 122 | +6.52 | -6.60 | -11.09 | -28.61 | PASS | 19.531 |
| `union_macd_hist_h3` | long | 3 | leftover | list | none | 40% | 39% | 4/28 | -5.87 | 23 | 152 | +5.24 | -5.50 | -5.87 | -30.77 | PASS | 19.175 |
| `union_rsi_os_h1` | long | 1 | leftover | list | none | 50% | 39% | 1/28 | -11.71 | 3 | 28 | +2.63 | -6.51 | -24.48 | +3.24 | PASS | 19.053 |
| `union_blue_vol_h1` | long | 1 | leftover | list | none | 43% | 29% | 0/28 | -9.71 | 10 | 51 | +5.29 | -7.28 | -12.80 | +0.42 | PASS | 18.957 |
| `ohlc_hot_h5` | long | 5 | leftover | list | none | 46% | 43% | 4/28 | -7.79 | 45 | 188 | +7.88 | -9.54 | -9.59 | +81.28 | PASS | 18.89 |
| `union_news_g_cam71_h3` | long | 3 | leftover | list | none | 44% | 32% | 3/28 | -15.12 | 2 | 18 | +4.13 | -7.28 | -16.01 | -9.37 | PASS | 18.515 |
| `union_white_any_h3` | long | 3 | leftover | list | none | 42% | 36% | 5/28 | -16.01 | 12 | 59 | +6.17 | -8.32 | -13.84 | -5.03 | PASS | 18.358 |
| `union_news_or_net5_h3` | long | 3 | leftover | list | none | 43% | 36% | 3/28 | -16.27 | 5 | 35 | +3.77 | -5.78 | -20.52 | -13.28 | PASS | 18.337 |
| `union_news_or_net3_h3` | long | 3 | leftover | list | none | 41% | 32% | 3/28 | -16.29 | 8 | 57 | +5.79 | -5.85 | -18.79 | -21.03 | PASS | 18.149 |
| `union_news_pack_net3_h3` | long | 3 | leftover | list | none | 37% | 21% | 7/28 | -4.38 | 3 | 25 | +6.18 | -7.59 | -15.53 | -16.71 | PASS | 18.142 |
| `union_rsi_h1` | long | 1 | leftover | list | none | 43% | 39% | 0/28 | -18.03 | 9 | 87 | +4.48 | -8.40 | -21.88 | -18.57 | PASS | 17.401 |
| `short_clk_neg_weak_fail_opp_h3` | short | 3 | leftover | list | none | 49% | 43% | 1/28 | -6.85 | 11 | 61 | +4.96 | -6.42 | -24.93 | -53.16 | PASS | 17.236 |
| `union_blue_coil_h1` | long | 1 | leftover | list | none | 39% | 21% | 0/28 | -13.66 | 8 | 44 | +3.81 | -6.56 | -16.02 | -19.41 | PASS | 16.422 |
| `flatten_live_h5` | long | 5 | leftover | list | none | 46% | 25% | 0/28 | -5.51 | 5 | 16 | +9.01 | -11.62 | -5.51 | +5.13 | PASS | 16.172 |
| `union_news_g_cam61_h3` | long | 3 | leftover | list | none | 46% | 21% | 3/28 | -16.27 | 3 | 29 | +4.02 | -6.37 | -19.47 | -22.26 | PASS | 16.049 |
| `union_coil_off_h5` | long | 5 | leftover | list | none | 46% | 43% | 3/28 | -11.97 | 29 | 160 | +3.69 | -54.08 | -7.75 | -23.91 | PASS | 15.798 |
| `union_rsi_os_h5` | long | 5 | leftover | list | none | 44% | 36% | 10/28 | -0.53 | 4 | 54 | +4.64 | -14.37 | -25.27 | -47.68 | PASS | 15.455 |
| `overnight_mega_h1` | long | 1 | leftover | list | none | 31% | 11% | 0/28 | +0.00 | 0 | 5 | +5.65 | -5.19 | -5.76 | -14.12 | PASS | 15.18 |
| `union_news_g_h5` | long | 5 | leftover | list | none | 42% | 39% | 3/28 | -5.46 | 21 | 127 | +5.13 | -8.62 | -15.90 | +116.75 | PASS | 14.938 |
| `union_rsi_h3` | long | 3 | leftover | list | none | 44% | 36% | 0/28 | -6.59 | 19 | 156 | +7.91 | -8.40 | -13.51 | -21.95 | PASS | 14.849 |
| `overnight_h3` | long | 3 | leftover | list | none | 45% | 39% | 0/28 | -10.16 | 15 | 93 | +6.08 | -8.89 | -13.90 | -29.24 | PASS | 14.687 |
| `overnight_mega_h2` | long | 2 | leftover | list | none | 50% | 7% | 1/28 | +0.00 | 0 | 12 | +3.54 | -6.97 | -7.35 | -6.81 | PASS | 14.528 |
| `union_news_g_cam61_h1` | long | 1 | leftover | list | none | 35% | 14% | 1/28 | -14.09 | 1 | 13 | +2.96 | -4.92 | -16.82 | -13.38 | PASS | 13.973 |
| `union_white_coil_h3` | long | 3 | leftover | list | none | 38% | 32% | 0/28 | -17.71 | 9 | 46 | +6.49 | -7.87 | -17.05 | -22.41 | PASS | 12.582 |
| `union_overnight_h3` | long | 3 | leftover | list | none | 44% | 32% | 0/28 | -10.17 | 13 | 87 | +5.70 | -9.19 | -15.60 | -35.12 | PASS | 12.192 |
| `overnight_h1` | long | 1 | leftover | list | none | 32% | 25% | 0/28 | -10.06 | 1 | 49 | +6.29 | -8.93 | -15.22 | -19.66 | PASS | 11.661 |
| `union_overnight_h1` | long | 1 | leftover | list | none | 33% | 21% | 0/28 | -10.06 | 1 | 45 | +6.53 | -8.50 | -18.04 | -23.79 | PASS | 11.231 |
| `union_news_both_h1` *(thin)* | long | 1 | leftover | list | none | 80% | 11% | 21/28 | +7.29 | 0 | 0 | +3.69 | -0.73 | +7.29 | +4.21 | PASS | 58.986 |
| `union_rsi_os_macd_h3` *(thin)* | long | 3 | leftover | list | none | 67% | 18% | 27/28 | +3.41 | 2 | 4 | +8.24 | -5.88 | +3.41 | +18.52 | PASS | 43.524 |
| `union_flow_in_white_h1` *(thin)* | long | 1 | leftover | list | none | 58% | 18% | 15/28 | +0.05 | 0 | 2 | +3.42 | -2.21 | +8.07 | +10.92 | PASS | 35.928 |
| `union_news_g_cam91_n1_h1` *(thin)* | long | 1 | leftover | list | none | 33% | 7% | 27/28 | +1.63 | 0 | 0 | +2.64 | -3.84 | +1.63 | -2.85 | PASS | 32.547 |
| `union_news_both_h3` *(thin)* | long | 3 | leftover | list | none | 20% | 14% | 21/28 | +4.04 | 0 | 2 | +4.37 | -0.51 | +4.04 | +1.48 | PASS | 27.213 |
| `union_clk_nr7_mom_opp_h1` *(thin)* | long | 1 | leftover | list | none | 75% | 11% | 12/28 | +0.00 | 0 | 4 | +4.03 | -2.38 | +10.45 | +0.01 | PASS | 12.875 |
| `union_news_missing_h1` *(thin)* | long | 1 | leftover | list | none | 50% | 4% | 1/28 | +0.00 | 0 | 2 | +7.93 | -3.05 | +1.44 | +1.88 | PASS | 9.832 |
| `union_vol_missing_h1` *(thin)* | long | 1 | leftover | list | none | 50% | 4% | 1/28 | +0.00 | 0 | 2 | +7.93 | -3.05 | +1.44 | +1.88 | PASS | 9.832 |
| `short_news_pack_h3` *(thin)* | short | 3 | leftover | list | none | 47% | 29% | 9/28 | +0.00 | 3 | 7 | +3.39 | -7.55 | +0.61 | -6.15 | PASS | 9.321 |
| `union_clk_flow_coil_h1` *(thin)* | long | 1 | leftover | list | none | 31% | 29% | 10/28 | -5.55 | 1 | 4 | +5.03 | -3.71 | -7.40 | -11.66 | PASS | 8.739 |
| `union_news_missing_h3` *(thin)* | long | 3 | leftover | list | none | 50% | 11% | 1/28 | +0.00 | 1 | 5 | +11.98 | -3.98 | +3.58 | +5.59 | PASS | 7.947 |
| `union_vol_missing_h3` *(thin)* | long | 3 | leftover | list | none | 50% | 11% | 1/28 | +0.00 | 1 | 5 | +11.98 | -3.98 | +3.58 | +5.59 | PASS | 7.947 |
| `union_catal_present_h3` *(thin)* | long | 3 | leftover | list | none | 31% | 21% | 0/28 | -4.37 | 2 | 8 | +12.05 | -4.39 | -0.39 | -20.68 | PASS | 0.264 |
| `union_catal_present_h1` *(thin)* | long | 1 | leftover | list | none | 31% | 18% | 0/28 | -6.54 | 0 | 3 | +6.03 | -3.46 | -1.70 | -5.09 | PASS | -0.272 |
| `union_flow_in_white_h3` *(thin)* | long | 3 | leftover | list | none | 50% | 14% | 2/28 | -0.10 | 0 | 6 | +3.30 | -4.79 | +2.10 | +4.80 | PASS | -1.596 |
| `union_clk_earn_guide_react_h1` *(thin)* | long | 1 | leftover | list | none | 45% | 7% | 0/28 | -3.11 | 1 | 4 | +5.68 | -6.17 | -5.43 | +10.90 | PASS | -2.505 |
| `overnight_mega_green_h1` *(thin)* | long | 1 | leftover | list | none | 35% | 11% | 0/28 | +0.00 | 0 | 2 | +2.51 | -6.44 | -8.87 | -9.06 | PASS | -5.479 |
| `union_rsi_os_macd_h1` *(thin)* | long | 1 | leftover | list | none | 17% | 7% | 1/28 | -8.27 | 1 | 3 | +7.49 | -3.44 | -8.27 | -3.93 | PASS | -8.861 |
| `union_clk_insider_cash_stab_h3` *(thin)* | long | 3 | leftover | list | none | 29% | 4% | 0/28 | -3.57 | 0 | 3 | +2.33 | -3.20 | -6.65 | -6.24 | PASS | -13.787 |
| `short_r_down_h1` *(thin)* | short | 1 | leftover | list | none | — | 0% | 0/28 | +0.00 | 0 | 0 | — | — | +0.00 | +0.00 | PASS | -15.0 |
| `short_r_down_h3` *(thin)* | short | 3 | leftover | list | none | — | 0% | 0/28 | +0.00 | 0 | 0 | — | — | +0.00 | +0.00 | PASS | -15.0 |
| `union_clk_r_up_coil_h1` *(thin)* | long | 1 | leftover | list | none | — | 0% | 0/28 | +0.00 | 0 | 0 | — | — | +0.00 | +0.00 | PASS | -15.0 |
| `union_r_up_h1` *(thin)* | long | 1 | leftover | list | none | — | 0% | 0/28 | +0.00 | 0 | 0 | — | — | +0.00 | +0.00 | PASS | -15.0 |
| `union_r_up_h3` *(thin)* | long | 3 | leftover | list | none | — | 0% | 0/28 | +0.00 | 0 | 0 | — | — | +0.00 | +0.00 | PASS | -15.0 |
