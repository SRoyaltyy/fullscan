# Factor strategy mine — 2026-08-13 → 2026-09-18

Leak-free 09:30 recipes: **329** · candidate rows **2542** · fill `09:30 open, whole shares, Futubull fees, leftover split, sell first, hard-red sit, cash+holdings audit`.

Cash book: $10k, whole shares, Futubull fees, leftover split, sell first, min-hold, 09:30 open, hard-red S≤−3 sit, shorts marked as a liability. Each session starts from leftover cash and lots actually held (butterfly). Cash-start buttons wake a sleeve on date X with $10k and no lots (same rules). Stock investigator quotes 09:30 cameras / coaches / news from repo files. Size / sell / S-boost tweaks sit on the same ledger. Signal-only % is the old equal-weight path (not a fill). `flatten_h*` = wish-list (io/HOLD mornings still buy). `flatten_live_*` = only when the live flatten gate fires. Research only — does not change live `flatten_robust`.

Scoreboard files: slim `factor_mine.json` (stats / recipes / series) plus gzip shards in `factor_mine/shards/` (`books`, `starts`, `daily`, `probe`, `sim`). One file used to be 104MB and GitHub rejected the publish (100MB cap).

Combination books: **78** mixes on the same $10k cash ledger (shared leftover or split sleeves, official 09:30 / 16:00, hard-red sit, owner min-hold). Outperformers: `combo_se_5050_skip`, `combo_sh_macd_5050_shared`, `combo_jer_5050_shared`, `combo_sj_3070_shared`, `combo_ser_5050_shared`, `combo_ner_5050_shared`, `combo_ers_7030_shared`, `combo_sj_5050_shared`, `combo_je1_5050_shared`, `combo_e1s_7030_shared`, `combo_se1_5050_shared`, `combo_ps_7030_shared`, `combo_ps_5050_shared`, `combo_sf_7030_shared`.

A combo outperforms its members on the cash book when the audit passes and either (1) Book% is strictly higher than every member, or (2) Book% stays within 2pp of the richest member, max drawdown is strictly smaller than every member's drawdown, start-rate is at least the best member, both halves are green, and the worst session is no worse than the worst member session. Effectiveness / Signal% do not decide WIN.

Action blotters: [FACTOR_MINE_ACTION.md](FACTOR_MINE_ACTION.md).

| Strategy | Side | H | Size | Sell | Boost | Win% | $ days | Starts YES | Med start | Top-g | Losers | AvgW | AvgL | Book% | Signal% | Audit | Eff |
|---|---|---:|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---:|
| `combo_sh_macd_5050_shared` | mix | 5 | leftover | list | none | 62% | 73% | 26/26 | +10.19 | 0 | 0 | +5.78 | -4.66 | +32.24 | — | PASS | 78.372 |
| `combo_sj_3070_shared` | mix | 5 | leftover | list | none | 56% | 62% | 26/26 | +3.67 | 0 | 0 | +4.81 | -4.54 | +23.82 | — | PASS | 70.85 |
| `combo_e1s_7030_shared` | mix | 5 | leftover | list | none | 58% | 58% | 26/26 | +2.94 | 0 | 0 | +4.75 | -5.85 | +16.60 | — | PASS | 68.033 |
| `combo_ps_7030_shared` | mix | 5 | leftover | list | none | 64% | 69% | 19/26 | +4.37 | 0 | 0 | +4.05 | -4.33 | +14.41 | — | PASS | 67.192 |
| `combo_se_5050_skip` | mix | 5 | leftover | list | none | 55% | 54% | 26/26 | +9.02 | 0 | 0 | +5.86 | -7.13 | +38.42 | — | PASS | 66.724 |
| `combo_sj_5050_shared` | mix | 5 | leftover | list | none | 56% | 62% | 22/26 | +1.99 | 0 | 0 | +4.84 | -4.54 | +20.08 | — | PASS | 66.474 |
| `combo_je1_5050_shared` | mix | 5 | leftover | list | none | 50% | 46% | 26/26 | +5.29 | 0 | 0 | +4.62 | -4.91 | +16.71 | — | PASS | 66.181 |
| `combo_ps_5050_shared` | mix | 5 | leftover | list | none | 65% | 62% | 18/26 | +1.66 | 0 | 0 | +4.06 | -4.35 | +11.88 | — | PASS | 64.718 |
| `combo_se1_5050_shared` | mix | 5 | leftover | list | none | 59% | 62% | 20/26 | +0.53 | 0 | 0 | +4.69 | -5.99 | +14.98 | — | PASS | 62.954 |
| `combo_jer_5050_shared` | mix | 5 | leftover | list | none | 43% | 50% | 23/26 | +2.24 | 0 | 0 | +6.44 | -6.01 | +29.40 | — | PASS | 60.415 |
| `combo_ner_5050_shared` | mix | 5 | leftover | list | none | 43% | 46% | 14/26 | +0.01 | 0 | 0 | +5.34 | -6.40 | +20.55 | — | PASS | 49.613 |
| `combo_ers_7030_shared` | mix | 5 | leftover | list | none | 48% | 38% | 15/26 | +0.10 | 0 | 0 | +6.12 | -7.52 | +20.33 | — | PASS | 46.896 |
| `combo_sf_7030_shared` | mix | 5 | leftover | list | none | 55% | 58% | 5/26 | -5.66 | 0 | 0 | +6.10 | -6.61 | +7.91 | — | PASS | 32.758 |
| `combo_ser_5050_shared` | mix | 5 | leftover | list | none | 50% | 54% | 4/26 | -0.87 | 0 | 0 | +6.08 | -7.40 | +21.15 | — | PASS | 30.239 |
| `combo_sh_5050_shared` | mix | 5 | leftover | list | none | 62% | 77% | 26/26 | +9.68 | 0 | 0 | +5.43 | -5.02 | +29.13 | — | PASS | 78.252 |
| `combo_sh_3070_shared` | mix | 5 | leftover | list | none | 60% | 73% | 26/26 | +12.43 | 0 | 0 | +5.50 | -4.86 | +31.92 | — | PASS | 77.308 |
| `union_hot_n4_h1` | long | 1 | leftover | list | none | 51% | 58% | 26/26 | +10.85 | 20 | 29 | +6.77 | -5.08 | +31.97 | +71.06 | PASS | 75.685 |
| `combo_sh_7030_shared` | mix | 5 | leftover | list | none | 62% | 69% | 26/26 | +4.83 | 0 | 0 | +5.40 | -5.07 | +21.25 | — | PASS | 75.456 |
| `union_clk_hold_vs_sector_opp_h1` | long | 1 | leftover | list | none | 52% | 50% | 26/26 | +3.72 | 7 | 29 | +3.77 | -2.54 | +8.41 | +6.29 | PASS | 71.321 |
| `combo_he1_5050_shared` | mix | 5 | leftover | list | none | 50% | 62% | 26/26 | +11.35 | 0 | 0 | +5.27 | -5.79 | +20.99 | — | PASS | 70.816 |
| `combo_jse_333_shared` | mix | 5 | leftover | list | none | 52% | 65% | 26/26 | +3.73 | 0 | 0 | +5.49 | -5.68 | +33.03 | — | PASS | 69.659 |
| `combo_seh_333_skip` | mix | 5 | leftover | list | none | 52% | 62% | 26/26 | +11.76 | 0 | 0 | +6.21 | -6.62 | +31.29 | — | PASS | 69.152 |
| `combo_hn_7030_shared` | mix | 5 | leftover | list | none | 49% | 54% | 25/26 | +6.93 | 0 | 0 | +4.44 | -4.64 | +18.45 | — | PASS | 69.077 |
| `combo_seh_404020_shared` | mix | 5 | leftover | list | none | 51% | 65% | 26/26 | +6.85 | 0 | 0 | +6.12 | -6.64 | +27.75 | — | PASS | 68.929 |
| `combo_hn_5050_shared` | mix | 5 | leftover | list | none | 50% | 50% | 25/26 | +5.21 | 0 | 0 | +4.67 | -4.37 | +15.02 | — | PASS | 68.512 |
| `union_hot_score_h1` | long | 1 | leftover | list | none | 51% | 54% | 25/26 | +6.77 | 33 | 59 | +6.31 | -6.31 | +9.67 | +35.39 | PASS | 68.354 |
| `combo_seh_333_split` | mix | 5 | leftover | list | none | 51% | 65% | 26/26 | +7.12 | 0 | 0 | +6.08 | -6.64 | +22.21 | — | PASS | 68.203 |
| `combo_seh_403525_shared` | mix | 5 | leftover | list | none | 50% | 62% | 26/26 | +7.55 | 0 | 0 | +6.22 | -6.62 | +26.68 | — | PASS | 67.961 |
| `combo_seh_502525_split` | mix | 5 | leftover | list | none | 51% | 65% | 26/26 | +4.34 | 0 | 0 | +6.28 | -6.58 | +17.25 | — | PASS | 67.803 |
| `union_clk_mom_break_peer_h1` | long | 1 | leftover | list | none | 53% | 46% | 26/26 | +1.58 | 9 | 38 | +3.67 | -3.45 | +3.05 | +11.47 | PASS | 67.781 |
| `union_clk_nr7_mom_h1` | long | 1 | leftover | list | none | 48% | 35% | 26/26 | +5.40 | 3 | 10 | +4.25 | -2.62 | +12.48 | +28.14 | PASS | 67.475 |
| `combo_hj_5050_shared` | mix | 5 | leftover | list | none | 46% | 50% | 25/26 | +6.55 | 0 | 0 | +5.27 | -4.55 | +17.64 | — | PASS | 67.2 |
| `combo_seh_451540_shared` | mix | 5 | leftover | list | none | 49% | 62% | 26/26 | +7.64 | 0 | 0 | +6.29 | -6.87 | +24.29 | — | PASS | 67.106 |
| `combo_seh_333_shared` | mix | 5 | leftover | list | none | 50% | 58% | 26/26 | +9.09 | 0 | 0 | +6.24 | -6.60 | +27.08 | — | PASS | 67.039 |
| `combo_seh_333_weather` | mix | 5 | leftover | list | none | 50% | 58% | 26/26 | +9.09 | 0 | 0 | +6.24 | -6.60 | +27.08 | — | PASS | 67.039 |
| `union_w_hot_candle_h1` | long | 1 | leftover | list | none | 51% | 50% | 24/26 | +5.00 | 29 | 51 | +5.56 | -5.58 | +8.91 | +28.26 | PASS | 66.97 |
| `combo_hn_3070_shared` | mix | 5 | leftover | list | none | 52% | 50% | 23/26 | +3.10 | 0 | 0 | +4.53 | -4.52 | +11.67 | — | PASS | 66.864 |
| `combo_ej_5050_shared` | mix | 5 | leftover | list | none | 46% | 54% | 26/26 | +6.50 | 0 | 0 | +6.06 | -5.74 | +36.17 | — | PASS | 66.82 |
| `combo_seh_502525_shared` | mix | 5 | leftover | list | none | 50% | 58% | 26/26 | +5.29 | 0 | 0 | +6.20 | -6.70 | +24.48 | — | PASS | 66.795 |
| `union_e_fresh_h3` | long | 3 | leftover | list | none | 47% | 65% | 25/26 | +13.08 | 21 | 90 | +9.20 | -7.62 | +38.46 | +2.44 | PASS | 66.585 |
| `union_hot_n12_h1` | long | 1 | leftover | list | none | 52% | 50% | 25/26 | +5.26 | 40 | 84 | +5.52 | -6.12 | +4.52 | +27.08 | PASS | 66.438 |
| `combo_ee1_3070_shared` | mix | 5 | leftover | list | none | 40% | 65% | 25/26 | +13.08 | 0 | 0 | +9.20 | -7.62 | +38.46 | — | PASS | 66.413 |
| `combo_ee1_5050_shared` | mix | 5 | leftover | list | none | 40% | 65% | 25/26 | +13.08 | 0 | 0 | +9.20 | -7.62 | +38.46 | — | PASS | 66.413 |
| `combo_ee1_7030_shared` | mix | 5 | leftover | list | none | 40% | 65% | 25/26 | +13.08 | 0 | 0 | +9.20 | -7.62 | +38.46 | — | PASS | 66.413 |
| `combo_eh_7030_shared` | mix | 5 | leftover | list | none | 39% | 58% | 26/26 | +13.11 | 0 | 0 | +7.77 | -6.76 | +31.91 | — | PASS | 66.015 |
| `combo_seh_601525_shared` | mix | 5 | leftover | list | none | 48% | 58% | 26/26 | +3.92 | 0 | 0 | +6.37 | -6.79 | +20.90 | — | PASS | 65.567 |
| `combo_se_5050_split` | mix | 5 | leftover | list | none | 53% | 62% | 26/26 | +6.43 | 0 | 0 | +6.26 | -6.70 | +20.62 | — | PASS | 65.299 |
| `combo_ehs_601525_shared` | mix | 5 | leftover | list | none | 48% | 50% | 26/26 | +8.96 | 0 | 0 | +6.28 | -6.39 | +30.10 | — | PASS | 65.286 |
| `combo_ehs_702010_shared` | mix | 5 | leftover | list | none | 43% | 54% | 26/26 | +11.39 | 0 | 0 | +6.80 | -6.28 | +31.21 | — | PASS | 64.914 |
| `union_ret_5_h1` | long | 1 | leftover | list | none | 51% | 46% | 23/26 | +4.38 | 27 | 55 | +5.64 | -5.93 | +11.12 | +21.79 | PASS | 64.807 |
| `union_news_pack_h1` | long | 1 | leftover | list | none | 54% | 46% | 22/26 | +3.56 | 2 | 15 | +3.13 | -3.28 | +10.42 | +15.48 | PASS | 64.749 |
| `combo_eh_3070_shared` | mix | 5 | leftover | list | none | 41% | 54% | 26/26 | +11.04 | 0 | 0 | +7.54 | -7.13 | +28.18 | — | PASS | 64.724 |
| `union_join_vol_green_h1` | long | 1 | leftover | list | none | 45% | 38% | 26/26 | +4.10 | 10 | 41 | +4.99 | -4.03 | +16.88 | +13.00 | PASS | 64.715 |
| `combo_es_9010_shared` | mix | 5 | leftover | list | none | 45% | 62% | 26/26 | +10.87 | 0 | 0 | +6.49 | -6.63 | +36.09 | — | PASS | 64.65 |
| `combo_se_5050_shared` | mix | 5 | leftover | list | none | 52% | 54% | 26/26 | +4.39 | 0 | 0 | +6.00 | -6.91 | +30.24 | — | PASS | 64.63 |
| `combo_se_5050_weather` | mix | 5 | leftover | list | none | 52% | 54% | 26/26 | +4.39 | 0 | 0 | +6.00 | -6.91 | +30.24 | — | PASS | 64.63 |
| `combo_eh_5050_shared` | mix | 5 | leftover | list | none | 41% | 50% | 26/26 | +12.64 | 0 | 0 | +7.53 | -6.83 | +30.07 | — | PASS | 64.381 |
| `union_vol_ab_h1` | long | 1 | leftover | list | none | 53% | 35% | 24/26 | +5.66 | 15 | 41 | +5.37 | -4.59 | +9.20 | +63.81 | PASS | 63.996 |
| `combo_p2s_5050_shared` | mix | 5 | leftover | list | none | 65% | 58% | 18/26 | +0.88 | 0 | 0 | +4.11 | -4.21 | +10.65 | — | PASS | 63.928 |
| `combo_en_5050_shared` | mix | 5 | leftover | list | none | 46% | 50% | 25/26 | +4.86 | 0 | 0 | +5.25 | -6.11 | +27.73 | — | PASS | 63.598 |
| `combo_nse_333_shared` | mix | 5 | leftover | list | none | 50% | 54% | 24/26 | +1.63 | 0 | 0 | +5.07 | -5.71 | +23.51 | — | PASS | 63.473 |
| `union_w_hot_cond_h1` | long | 1 | leftover | list | none | 50% | 46% | 23/26 | +3.79 | 27 | 55 | +5.72 | -6.21 | +3.79 | +26.94 | PASS | 63.182 |
| `combo_nj_5050_shared` | mix | 5 | leftover | list | none | 47% | 46% | 24/26 | +1.48 | 0 | 0 | +4.21 | -4.21 | +4.77 | — | PASS | 62.908 |
| `combo_se_3070_shared` | mix | 5 | leftover | list | none | 50% | 46% | 26/26 | +7.38 | 0 | 0 | +6.04 | -6.85 | +32.80 | — | PASS | 62.655 |
| `union_e_fresh_h1` | long | 1 | leftover | list | none | 49% | 42% | 25/26 | +5.25 | 10 | 46 | +4.69 | -5.92 | +14.05 | +8.43 | PASS | 62.42 |
| `combo_snj_333_shared` | mix | 5 | leftover | list | none | 53% | 58% | 20/26 | +1.16 | 0 | 0 | +4.30 | -4.52 | +11.96 | — | PASS | 62.241 |
| `combo_es_8020_shared` | mix | 5 | leftover | list | none | 48% | 46% | 26/26 | +8.99 | 0 | 0 | +6.14 | -6.77 | +34.98 | — | PASS | 62.229 |
| `combo_en_3070_shared` | mix | 5 | leftover | list | none | 46% | 50% | 24/26 | +2.86 | 0 | 0 | +5.23 | -6.07 | +23.72 | — | PASS | 62.172 |
| `yday_gainer_h1` | long | 1 | leftover | list | none | 52% | 42% | 21/26 | +3.85 | 14 | 63 | +4.83 | -4.03 | +10.40 | +17.69 | PASS | 61.754 |
| `combo_en_7030_shared` | mix | 5 | leftover | list | none | 39% | 46% | 25/26 | +7.82 | 0 | 0 | +5.87 | -5.86 | +31.19 | — | PASS | 61.597 |
| `union_w_hot_candle_h3` | long | 3 | leftover | list | none | 46% | 62% | 25/26 | +4.24 | 44 | 135 | +9.51 | -9.95 | +12.85 | +225.23 | PASS | 61.514 |
| `combo_se_7030_shared` | mix | 5 | leftover | list | none | 53% | 65% | 21/26 | +1.72 | 0 | 0 | +5.88 | -6.96 | +25.54 | — | PASS | 61.486 |
| `combo_her_5050_shared` | mix | 5 | leftover | list | none | 39% | 50% | 26/26 | +7.73 | 0 | 0 | +7.64 | -7.78 | +21.69 | — | PASS | 61.396 |
| `union_clk_mom_break_peer_opp_h1` | long | 1 | leftover | list | none | 52% | 31% | 19/26 | +0.53 | 8 | 27 | +3.65 | -2.75 | +13.82 | +20.68 | PASS | 60.794 |
| `union_clk_hold_vs_sector_h1` | long | 1 | leftover | list | none | 52% | 46% | 18/26 | +0.95 | 6 | 39 | +3.37 | -3.03 | +4.71 | +6.28 | PASS | 59.559 |
| `probable_h1` | long | 1 | leftover | list | none | 51% | 35% | 22/26 | +2.15 | 13 | 61 | +4.15 | -3.75 | +2.15 | +14.03 | PASS | 59.202 |
| `combo_sj_7030_shared` | mix | 5 | leftover | list | none | 55% | 62% | 16/26 | +0.92 | 0 | 0 | +4.83 | -4.60 | +13.16 | — | PASS | 58.997 |
| `union_hot_score_h3` | long | 3 | leftover | list | none | 42% | 62% | 23/26 | +2.76 | 47 | 148 | +12.16 | -9.53 | +13.99 | +221.69 | PASS | 58.793 |
| `union_news_pack_net2_h1` | long | 1 | leftover | list | none | 53% | 38% | 17/26 | +3.11 | 1 | 13 | +3.19 | -2.95 | +10.75 | +11.92 | PASS | 58.627 |
| `union_ret_5_h3` | long | 3 | leftover | list | none | 41% | 58% | 25/26 | +7.91 | 39 | 144 | +9.15 | -9.23 | +18.07 | +27.98 | PASS | 58.555 |
| `union_vol_ab_h3` | long | 3 | leftover | list | none | 48% | 35% | 26/26 | +5.50 | 22 | 88 | +8.35 | -6.98 | +4.11 | +88.41 | PASS | 56.949 |
| `union_break10_h1` | long | 1 | leftover | list | none | 49% | 38% | 18/26 | +2.86 | 24 | 48 | +5.29 | -5.07 | -2.61 | +7.05 | PASS | 56.337 |
| `union_w_hot_cond_h3` | long | 3 | leftover | list | none | 42% | 54% | 23/26 | +5.10 | 41 | 144 | +10.04 | -9.26 | +14.45 | +199.26 | PASS | 56.315 |
| `union_last_green_h1` | long | 1 | leftover | list | none | 47% | 38% | 15/26 | +1.62 | 15 | 44 | +4.81 | -3.31 | +8.67 | +16.94 | PASS | 56.208 |
| `combo_ef_7030_shared` | mix | 5 | leftover | list | none | 41% | 62% | 21/26 | +5.16 | 0 | 0 | +7.85 | -7.49 | +20.39 | — | PASS | 55.825 |
| `union_news_g_cond_h1` | long | 1 | leftover | list | none | 51% | 38% | 16/26 | +1.91 | 2 | 34 | +3.96 | -3.76 | +2.99 | +16.41 | PASS | 55.621 |
| `union_news_or_h1` | long | 1 | leftover | list | none | 51% | 38% | 16/26 | +1.91 | 2 | 34 | +3.96 | -3.76 | +2.99 | +16.41 | PASS | 55.621 |
| `union_news_g_h1` | long | 1 | leftover | list | none | 52% | 46% | 13/26 | +0.50 | 4 | 33 | +3.72 | -4.02 | +2.82 | +16.13 | PASS | 54.529 |
| `union_break10_h3` | long | 3 | leftover | list | none | 46% | 46% | 21/26 | +3.73 | 36 | 125 | +9.05 | -7.67 | +2.20 | +35.75 | PASS | 54.409 |
| `combo_ne1_5050_shared` | mix | 5 | leftover | list | none | 51% | 46% | 14/26 | +0.83 | 0 | 0 | +4.04 | -4.89 | +9.75 | — | PASS | 54.202 |
| `union_news_pack_net3_h1` | long | 1 | leftover | list | none | 50% | 35% | 15/26 | +1.29 | 1 | 12 | +3.07 | -2.95 | +8.99 | +6.42 | PASS | 54.15 |
| `combo_eer_5050_shared` | mix | 5 | leftover | list | none | 37% | 46% | 24/26 | +6.58 | 0 | 0 | +8.48 | -8.96 | +22.72 | — | PASS | 53.711 |
| `union_clk_fresh_cat_coil_h1` | long | 1 | leftover | list | none | 49% | 38% | 14/26 | +0.33 | 7 | 30 | +3.31 | -3.19 | -2.20 | -0.34 | PASS | 53.321 |
| `short_news_or_h3` | short | 3 | leftover | list | none | 61% | 58% | 13/26 | +0.85 | 8 | 66 | +4.61 | -5.56 | +6.65 | +6.03 | PASS | 52.75 |
| `short_news_r_h3` | short | 3 | leftover | list | none | 60% | 58% | 13/26 | +0.80 | 8 | 66 | +4.67 | -5.55 | +6.55 | +6.07 | PASS | 52.564 |
| `ohlc_hot_coil_h1` | long | 1 | leftover | list | none | 49% | 38% | 14/26 | +1.62 | 11 | 31 | +3.72 | -3.60 | -16.13 | -9.33 | PASS | 50.199 |
| `union_join_vol_green_h3` | long | 3 | leftover | list | none | 48% | 35% | 20/26 | +4.93 | 20 | 82 | +7.26 | -7.65 | -2.70 | +38.67 | PASS | 50.134 |
| `yday_gainer_h3` | long | 3 | leftover | list | none | 50% | 42% | 17/26 | +1.29 | 28 | 121 | +8.56 | -7.66 | -3.15 | +37.34 | PASS | 49.72 |
| `union_flow_in_h3` | long | 3 | leftover | list | none | 48% | 42% | 16/26 | +0.65 | 7 | 50 | +3.03 | -4.56 | +0.58 | +9.42 | PASS | 49.195 |
| `union_vol_g_h3` | long | 3 | leftover | list | none | 48% | 46% | 18/26 | +1.03 | 27 | 122 | +8.73 | -8.28 | -3.37 | +36.99 | PASS | 48.538 |
| `union_candle_score_h3` | long | 3 | leftover | list | none | 44% | 42% | 17/26 | +1.02 | 26 | 135 | +6.32 | -7.04 | +5.78 | +19.43 | PASS | 46.559 |
| `short_news_r_macd_h3` | short | 3 | leftover | list | none | 64% | 58% | 12/26 | +0.00 | 6 | 40 | +5.00 | -4.63 | +12.94 | +23.73 | PASS | 44.953 |
| `probable_h3` | long | 3 | leftover | list | none | 48% | 38% | 15/26 | +1.29 | 26 | 124 | +6.61 | -8.29 | -3.58 | +17.91 | PASS | 44.513 |
| `union_earn_react_h3` | long | 3 | leftover | list | none | 41% | 50% | 13/26 | +1.33 | 15 | 89 | +8.50 | -9.05 | +20.10 | -19.17 | PASS | 44.31 |
| `combo_sn_5050_shared` | mix | 5 | leftover | list | none | 60% | 58% | 11/26 | -1.70 | 0 | 0 | +4.06 | -4.57 | +5.23 | — | PASS | 44.15 |
| `short_alarm_h3` | short | 3 | leftover | list | none | 55% | 35% | 13/26 | +0.11 | 13 | 92 | +6.75 | -8.65 | +1.58 | +10.61 | PASS | 43.598 |
| `combo_sn_7030_shared` | mix | 5 | leftover | list | none | 59% | 54% | 11/26 | -1.55 | 0 | 0 | +3.98 | -4.64 | +4.55 | — | PASS | 42.73 |
| `combo_sn_3070_shared` | mix | 5 | leftover | list | none | 58% | 54% | 10/26 | -0.49 | 0 | 0 | +4.13 | -4.44 | +6.46 | — | PASS | 42.366 |
| `union_white_both_n4_h1` | long | 1 | leftover | list | none | 49% | 38% | 7/26 | -5.01 | 4 | 13 | +6.88 | -3.68 | +13.12 | +9.03 | PASS | 40.842 |
| `union_blue_vol_h3` | long | 3 | leftover | list | none | 40% | 38% | 13/26 | +1.39 | 17 | 99 | +9.31 | -7.27 | -4.57 | +25.45 | PASS | 40.784 |
| `combo_e1er_5050_shared` | mix | 5 | leftover | list | none | 45% | 19% | 15/26 | +0.51 | 0 | 0 | +4.61 | -7.55 | -2.27 | — | PASS | 40.196 |
| `short_news_head_h3` | short | 3 | leftover | list | none | 63% | 50% | 12/26 | -0.19 | 6 | 62 | +4.83 | -6.38 | +5.44 | +8.21 | PASS | 39.844 |
| `union_white_both_n4_h2` | long | 2 | leftover | list | none | 44% | 46% | 10/26 | -2.23 | 8 | 21 | +7.78 | -5.31 | +15.86 | +14.76 | PASS | 39.663 |
| `union_h1_topheavy` | long | 1 | topheavy | list | none | 44% | 46% | 10/26 | -0.48 | 8 | 49 | +4.58 | -3.89 | +7.64 | +11.57 | PASS | 39.428 |
| `union_news_vol_h1` | long | 1 | leftover | list | none | 47% | 42% | 10/26 | -0.21 | 5 | 20 | +4.04 | -4.14 | +5.14 | +16.13 | PASS | 39.416 |
| `probable_probable_ok_h1` | long | 1 | leftover | list | none | 50% | 31% | 12/26 | -0.80 | 8 | 26 | +3.96 | -3.40 | -3.30 | +7.02 | PASS | 39.379 |
| `union_news_head_h1` | long | 1 | leftover | list | none | 50% | 38% | 9/26 | -0.77 | 5 | 25 | +4.51 | -3.84 | -1.69 | +16.70 | PASS | 39.112 |
| `union_news_g_conv_h1` | long | 1 | conviction | list | none | 51% | 38% | 8/26 | -4.47 | 1 | 16 | +3.36 | -3.53 | +5.00 | +7.97 | PASS | 38.244 |
| `combo_hf_5050_shared` | mix | 5 | leftover | list | none | 48% | 54% | 10/26 | -0.57 | 0 | 0 | +6.64 | -6.35 | +6.03 | — | PASS | 38.232 |
| `union_h1_rankw` | long | 1 | rank_w | list | none | 44% | 46% | 9/26 | -1.48 | 8 | 49 | +4.44 | -4.03 | +3.06 | +11.57 | PASS | 37.407 |
| `flatten_live_h1_topheavy` | long | 1 | topheavy | list | none | 54% | 12% | 7/26 | +0.00 | 1 | 5 | +6.75 | -3.75 | +7.19 | +4.45 | PASS | 37.243 |
| `short_clk_ext_veto_opp_h3` | short | 3 | leftover | list | none | 54% | 42% | 11/26 | -0.44 | 14 | 59 | +7.81 | -5.75 | +0.97 | +2.29 | PASS | 37.049 |
| `union_h1` | long | 1 | leftover | list | none | 44% | 42% | 9/26 | -1.16 | 8 | 49 | +4.46 | -3.96 | +4.00 | +11.57 | PASS | 36.909 |
| `union_h1_sizeup` | long | 1 | leftover | list | sizeup | 44% | 42% | 9/26 | -1.16 | 8 | 49 | +4.46 | -3.96 | +4.00 | +11.57 | PASS | 36.909 |
| `union_h1_trail` | long | 1 | leftover | trail | none | 44% | 42% | 9/26 | -1.16 | 8 | 49 | +4.46 | -3.96 | +4.00 | +11.57 | PASS | 36.909 |
| `union_flow_in_h1` | long | 1 | leftover | list | none | 47% | 38% | 6/26 | -3.11 | 4 | 14 | +3.45 | -3.06 | +4.73 | +18.57 | PASS | 36.833 |
| `union_vol_green_h1` | long | 1 | leftover | list | none | 45% | 42% | 9/26 | -5.91 | 18 | 53 | +5.66 | -4.85 | +3.91 | -7.10 | PASS | 36.764 |
| `union_join_g_h1` | long | 1 | leftover | list | none | 46% | 38% | 10/26 | -0.15 | 9 | 53 | +4.06 | -4.21 | +3.19 | +6.43 | PASS | 36.629 |
| `union_join_present_h1` | long | 1 | leftover | list | none | 45% | 42% | 9/26 | -1.21 | 7 | 52 | +4.46 | -4.03 | +3.85 | +10.69 | PASS | 36.609 |
| `union_h1_time` | long | 1 | leftover | time | none | 44% | 38% | 9/26 | -1.27 | 8 | 49 | +4.38 | -3.56 | +3.46 | +11.57 | PASS | 36.569 |
| `union_h1_sboost` | long | 1 | leftover | list | both | 44% | 42% | 9/26 | -2.38 | 8 | 49 | +4.40 | -4.06 | +2.92 | +11.57 | PASS | 36.542 |
| `union_news_or_net4_conv_h1` | long | 1 | conviction | list | none | 46% | 38% | 8/26 | -4.94 | 1 | 11 | +3.19 | -3.53 | +4.96 | -1.38 | PASS | 36.373 |
| `union_macd_up_h1` | long | 1 | leftover | list | none | 44% | 42% | 9/26 | -1.04 | 15 | 54 | +4.58 | -4.25 | +0.98 | +11.47 | PASS | 36.245 |
| `flatten_live_h1` | long | 1 | leftover | list | none | 54% | 12% | 7/26 | +0.00 | 1 | 5 | +6.31 | -4.05 | +8.22 | +4.45 | PASS | 36.203 |
| `flatten_live_h1_cut` | long | 1 | leftover | cut_loser | none | 54% | 12% | 7/26 | +0.00 | 1 | 5 | +6.31 | -4.05 | +8.22 | +4.45 | PASS | 36.203 |
| `flatten_live_h1_sboost` | long | 1 | leftover | list | both | 54% | 12% | 7/26 | +0.00 | 1 | 5 | +6.31 | -4.05 | +8.22 | +4.45 | PASS | 36.203 |
| `flatten_live_h1_sizeup` | long | 1 | leftover | list | sizeup | 54% | 12% | 7/26 | +0.00 | 1 | 5 | +6.31 | -4.05 | +8.22 | +4.45 | PASS | 36.203 |
| `flatten_live_h1_time` | long | 1 | leftover | time | none | 54% | 12% | 7/26 | +0.00 | 1 | 5 | +6.31 | -4.05 | +8.22 | +4.45 | PASS | 36.203 |
| `flatten_live_h1_trail` | long | 1 | leftover | trail | none | 54% | 12% | 7/26 | +0.00 | 1 | 5 | +6.31 | -4.05 | +8.22 | +4.45 | PASS | 36.203 |
| `union_h1_cut` | long | 1 | leftover | cut_loser | none | 44% | 38% | 9/26 | -1.24 | 8 | 49 | +4.46 | -3.91 | +3.82 | +11.57 | PASS | 36.182 |
| `flatten_live_h1_half` | long | 1 | half | list | none | 54% | 12% | 7/26 | +0.00 | 1 | 5 | +6.58 | -3.90 | +3.74 | +4.45 | PASS | 36.151 |
| `union_news_present_h1` | long | 1 | leftover | list | none | 44% | 38% | 9/26 | -1.21 | 7 | 50 | +4.32 | -4.06 | +2.37 | +8.65 | PASS | 35.348 |
| `union_candle_h1` | long | 1 | leftover | list | none | 45% | 31% | 10/26 | -1.40 | 10 | 51 | +4.41 | -3.95 | +1.43 | +3.75 | PASS | 35.211 |
| `union_news_g_cam71_h1` | long | 1 | leftover | list | none | 39% | 38% | 10/26 | -3.36 | 1 | 5 | +3.41 | -4.47 | +1.13 | +1.03 | PASS | 34.825 |
| `flatten_live_h1_rankw` | long | 1 | rank_w | list | none | 54% | 12% | 6/26 | +0.00 | 1 | 5 | +6.14 | -4.12 | +4.03 | +4.45 | PASS | 34.27 |
| `union_news_or_net2_h1` | long | 1 | leftover | list | none | 47% | 38% | 6/26 | -0.67 | 1 | 26 | +3.82 | -3.76 | +0.41 | +5.17 | PASS | 33.712 |
| `union_white_both_n4_h5_s12` | long | 5 | leftover | list | none | 36% | 58% | 10/26 | -0.84 | 12 | 36 | +9.82 | -6.73 | +20.16 | +18.10 | PASS | 33.696 |
| `union_h3_cut` | long | 3 | leftover | cut_loser | none | 44% | 54% | 6/26 | -2.46 | 18 | 109 | +8.81 | -5.46 | +2.86 | +13.91 | PASS | 33.553 |
| `union_h3_trail` | long | 3 | leftover | trail | none | 44% | 54% | 6/26 | -2.46 | 18 | 109 | +8.81 | -5.46 | +2.86 | +13.91 | PASS | 33.553 |
| `combo_ef_5050_shared` | mix | 5 | leftover | list | none | 41% | 54% | 11/26 | -0.30 | 0 | 0 | +8.09 | -7.41 | +11.18 | — | PASS | 33.546 |
| `combo_fe_5050_shared` | mix | 5 | leftover | list | none | 41% | 54% | 11/26 | -0.30 | 0 | 0 | +8.09 | -7.41 | +11.18 | — | PASS | 33.546 |
| `ohlc_hot_h1` | long | 1 | leftover | list | none | 45% | 42% | 9/26 | -0.81 | 16 | 51 | +3.38 | -5.01 | -9.69 | -1.71 | PASS | 33.134 |
| `flatten_h1` | long | 1 | leftover | list | none | 43% | 38% | 6/26 | -4.91 | 3 | 31 | +4.21 | -3.60 | +2.06 | -3.23 | PASS | 33.115 |
| `union_last_green_h3` | long | 3 | leftover | list | none | 52% | 38% | 10/26 | -3.00 | 29 | 111 | +5.87 | -7.94 | +0.34 | +42.22 | PASS | 33.034 |
| `union_h3` | long | 3 | leftover | list | none | 44% | 54% | 6/26 | -2.46 | 18 | 109 | +8.81 | -5.80 | +1.35 | +13.91 | PASS | 32.847 |
| `union_h3_sizeup` | long | 3 | leftover | list | sizeup | 44% | 54% | 6/26 | -2.46 | 18 | 109 | +8.81 | -5.80 | +1.35 | +13.91 | PASS | 32.847 |
| `union_h3_time` | long | 3 | leftover | time | none | 44% | 54% | 6/26 | -2.46 | 18 | 109 | +8.05 | -5.55 | +3.43 | +13.91 | PASS | 32.813 |
| `union_candle_score_h1` | long | 1 | leftover | list | none | 50% | 35% | 5/26 | -2.73 | 17 | 52 | +4.74 | -4.60 | -3.59 | +5.31 | PASS | 32.761 |
| `combo_fh_7030_shared` | mix | 5 | leftover | list | none | 45% | 54% | 6/26 | -2.61 | 0 | 0 | +6.87 | -6.28 | +0.98 | — | PASS | 32.686 |
| `combo_jf_5050_shared` | mix | 5 | leftover | list | none | 44% | 62% | 5/26 | -4.96 | 0 | 0 | +5.79 | -5.31 | +4.18 | — | PASS | 32.466 |
| `short_macd_dn_h3` | short | 3 | leftover | list | none | 49% | 46% | 10/26 | -0.37 | 22 | 113 | +4.66 | -6.03 | -7.22 | -12.64 | PASS | 31.774 |
| `union_news_or_net5_h1` | long | 1 | leftover | list | none | 42% | 35% | 6/26 | -2.35 | 1 | 11 | +3.57 | -3.79 | +1.25 | +4.64 | PASS | 31.767 |
| `union_news_or_net4_rw_h1` | long | 1 | rank_w | list | none | 46% | 38% | 4/26 | -6.89 | 1 | 11 | +3.09 | -3.50 | -0.57 | -1.38 | PASS | 31.586 |
| `union_macd_up_h3` | long | 3 | leftover | list | none | 40% | 42% | 12/26 | -0.08 | 27 | 124 | +7.19 | -6.43 | -0.56 | +17.41 | PASS | 31.384 |
| `union_h3_exit_red` | long | 3 | leftover | list | none | 50% | 38% | 9/26 | -3.21 | 29 | 111 | +5.86 | -6.84 | -3.30 | +28.48 | PASS | 31.328 |
| `union_h5_time` | long | 5 | leftover | time | none | 45% | 65% | 7/26 | -6.07 | 29 | 149 | +8.94 | -8.07 | +0.55 | +18.51 | PASS | 31.27 |
| `union_ab_g_h1` | long | 1 | leftover | list | none | 43% | 31% | 7/26 | -3.73 | 6 | 41 | +3.76 | -3.54 | +0.53 | -0.92 | PASS | 31.063 |
| `short_alarm_h1` | short | 1 | leftover | list | none | 54% | 19% | 5/26 | -0.35 | 7 | 45 | +3.81 | -3.11 | +0.78 | +3.12 | PASS | 30.776 |
| `union_join_present_h3` | long | 3 | leftover | list | none | 42% | 50% | 5/26 | -2.46 | 17 | 112 | +9.72 | -5.71 | +1.45 | +9.68 | PASS | 30.723 |
| `union_last_red_h3` | long | 3 | leftover | list | none | 41% | 50% | 10/26 | -0.04 | 18 | 123 | +6.50 | -6.38 | +1.66 | +9.71 | PASS | 30.495 |
| `union_h5` | long | 5 | leftover | list | none | 45% | 62% | 7/26 | -6.07 | 29 | 149 | +8.84 | -8.09 | +0.44 | +18.51 | PASS | 30.41 |
| `union_h5_cut` | long | 5 | leftover | cut_loser | none | 45% | 62% | 7/26 | -6.07 | 29 | 149 | +8.84 | -8.09 | +0.44 | +18.51 | PASS | 30.41 |
| `union_h5_sizeup` | long | 5 | leftover | list | sizeup | 45% | 62% | 7/26 | -6.07 | 29 | 149 | +8.84 | -8.09 | +0.44 | +18.51 | PASS | 30.41 |
| `union_h5_trail` | long | 5 | leftover | trail | none | 45% | 62% | 7/26 | -6.07 | 29 | 149 | +8.84 | -8.09 | +0.44 | +18.51 | PASS | 30.41 |
| `union_clk_flow_coil_h1` | long | 1 | leftover | list | none | 33% | 27% | 7/26 | -4.96 | 1 | 1 | +3.68 | -2.79 | -9.32 | -4.30 | PASS | 30.4 |
| `union_white_yday_h1` | long | 1 | leftover | list | none | 47% | 31% | 3/26 | -7.55 | 2 | 15 | +4.67 | -3.73 | -4.12 | +4.13 | PASS | 30.317 |
| `union_h3_sboost` | long | 3 | leftover | list | both | 44% | 54% | 4/26 | -3.14 | 18 | 109 | +8.46 | -6.00 | -0.40 | +13.91 | PASS | 30.104 |
| `short_news_r_h1` | short | 1 | leftover | list | none | 51% | 42% | 2/26 | -2.71 | 5 | 28 | +2.70 | -3.66 | -2.77 | -5.63 | PASS | 29.829 |
| `union_h1_half` | long | 1 | half | list | none | 44% | 38% | 3/26 | -1.00 | 8 | 49 | +4.46 | -3.97 | -0.21 | +11.57 | PASS | 29.717 |
| `union_h3_half` | long | 3 | half | list | none | 44% | 54% | 3/26 | -4.89 | 18 | 109 | +8.42 | -5.22 | -3.87 | +13.91 | PASS | 29.644 |
| `probable_probable_ok_h3` | long | 3 | leftover | list | none | 54% | 31% | 10/26 | -0.29 | 20 | 62 | +5.60 | -7.92 | -17.52 | +19.56 | PASS | 29.587 |
| `union_vol_g_h1` | long | 1 | leftover | list | none | 48% | 35% | 5/26 | -2.92 | 15 | 66 | +5.79 | -5.50 | -1.14 | +14.97 | PASS | 29.532 |
| `union_flow_in_white_h1` | long | 1 | leftover | list | none | 38% | 23% | 1/26 | -3.65 | 0 | 3 | +3.13 | -1.48 | +3.72 | +6.27 | PASS | 29.083 |
| `union_blue_vol_h1` | long | 1 | leftover | list | none | 45% | 31% | 7/26 | -1.35 | 9 | 54 | +5.43 | -5.42 | -3.65 | +7.21 | PASS | 29.052 |
| `combo_fse_333_shared` | mix | 5 | leftover | list | none | 47% | 54% | 5/26 | -2.03 | 0 | 0 | +6.20 | -7.09 | +9.27 | — | PASS | 29.028 |
| `union_vol_green_h3` | long | 3 | leftover | list | none | 48% | 31% | 11/26 | -0.74 | 31 | 108 | +7.09 | -7.55 | -9.92 | +15.10 | PASS | 28.992 |
| `union_h3_rankw` | long | 3 | rank_w | list | none | 44% | 50% | 4/26 | -6.56 | 18 | 109 | +8.61 | -5.96 | -5.30 | +13.91 | PASS | 28.784 |
| `combo_fe1_5050_shared` | mix | 5 | leftover | list | none | 46% | 54% | 4/26 | -4.95 | 0 | 0 | +5.75 | -6.31 | +0.34 | — | PASS | 28.762 |
| `union_news_g_cam71_n2_h1` | long | 1 | topheavy | list | none | 43% | 35% | 4/26 | -3.16 | 1 | 3 | +3.06 | -5.14 | -3.16 | -2.33 | PASS | 28.712 |
| `combo_nf_5050_shared` | mix | 5 | leftover | list | none | 42% | 54% | 2/26 | -5.23 | 0 | 0 | +6.43 | -5.26 | +2.61 | — | PASS | 28.632 |
| `combo_sf_5050_shared` | mix | 5 | leftover | list | none | 53% | 50% | 3/26 | -5.34 | 0 | 0 | +6.28 | -6.42 | +5.33 | — | PASS | 28.431 |
| `union_h3_exit_news_r` | long | 3 | leftover | list | none | 42% | 46% | 4/26 | -3.86 | 17 | 112 | +9.72 | -5.92 | -0.40 | +6.36 | PASS | 28.413 |
| `union_white_both_n4_h5` | long | 5 | leftover | list | none | 36% | 54% | 7/26 | -0.84 | 12 | 36 | +9.82 | -8.51 | +19.41 | +18.10 | PASS | 28.406 |
| `combo_fes_403030_shared` | mix | 5 | leftover | list | none | 46% | 54% | 5/26 | -3.19 | 0 | 0 | +6.28 | -7.03 | +6.96 | — | PASS | 28.296 |
| `union_h3_exit_alarm` | long | 3 | leftover | list | none | 42% | 50% | 5/26 | -2.23 | 17 | 112 | +6.97 | -5.95 | +1.09 | +10.07 | PASS | 28.211 |
| `union_earn_react_h1` | long | 1 | leftover | list | none | 48% | 31% | 7/26 | -0.33 | 7 | 50 | +4.63 | -7.25 | +0.41 | -0.76 | PASS | 28.208 |
| `union_h3_topheavy` | long | 3 | topheavy | list | none | 44% | 46% | 5/26 | -3.33 | 18 | 109 | +7.05 | -6.12 | -0.70 | +13.91 | PASS | 28.201 |
| `union_white_h1` | long | 1 | leftover | list | none | 37% | 35% | 6/26 | -4.06 | 4 | 29 | +5.72 | -4.56 | +0.68 | -3.34 | PASS | 28.194 |
| `union_macd_hist_h1` | long | 1 | leftover | list | none | 45% | 35% | 5/26 | -2.05 | 11 | 67 | +4.05 | -4.20 | -5.34 | -1.57 | PASS | 28.173 |
| `oppset_h1` | long | 1 | leftover | list | none | 47% | 35% | 1/26 | -1.56 | 16 | 47 | +3.95 | -3.42 | -4.37 | +3.59 | PASS | 28.004 |
| `combo_ef_3070_shared` | mix | 5 | leftover | list | none | 43% | 58% | 5/26 | -2.91 | 0 | 0 | +8.04 | -7.58 | +4.46 | — | PASS | 27.963 |
| `union_h5_sboost` | long | 5 | leftover | list | both | 45% | 62% | 5/26 | -6.05 | 29 | 149 | +8.36 | -8.56 | -0.12 | +18.51 | PASS | 27.823 |
| `union_last_green_h5` | long | 5 | leftover | list | none | 48% | 58% | 6/26 | -8.45 | 38 | 152 | +7.47 | -11.01 | -3.69 | +63.77 | PASS | 27.411 |
| `combo_sf_3070_shared` | mix | 5 | leftover | list | none | 51% | 54% | 2/26 | -5.06 | 0 | 0 | +6.63 | -6.50 | +3.94 | — | PASS | 27.248 |
| `union_vol_g_h5` | long | 5 | leftover | list | none | 44% | 38% | 12/26 | -0.71 | 35 | 143 | +12.32 | -10.84 | -11.99 | +19.30 | PASS | 27.216 |
| `union_news_or_net4_h1` | long | 1 | leftover | list | none | 45% | 38% | 0/26 | -5.12 | 1 | 19 | +3.82 | -3.73 | -1.06 | -0.41 | PASS | 27.08 |
| `union_oppset_h1` | long | 1 | leftover | list | none | 48% | 27% | 1/26 | -1.55 | 15 | 49 | +4.06 | -3.43 | -4.51 | +3.68 | PASS | 26.915 |
| `union_h5_half` | long | 5 | half | list | none | 45% | 58% | 4/26 | -4.04 | 29 | 149 | +7.93 | -7.20 | +1.04 | +18.51 | PASS | 26.894 |
| `union_cond_h1` | long | 1 | leftover | list | none | 50% | 31% | 0/26 | -3.86 | 4 | 43 | +4.23 | -4.06 | -4.80 | +4.44 | PASS | 26.803 |
| `union_clk_fresh_cat_coil_opp_h1` | long | 1 | leftover | list | none | 43% | 31% | 1/26 | -2.57 | 12 | 33 | +3.72 | -2.93 | -5.32 | -5.75 | PASS | 26.758 |
| `union_h5_exit_alarm` | long | 5 | leftover | list | none | 46% | 54% | 5/26 | -4.67 | 28 | 151 | +7.39 | -7.30 | +1.43 | +25.77 | PASS | 26.619 |
| `yday_gainer_h5` | long | 5 | leftover | list | none | 50% | 46% | 6/26 | -6.58 | 38 | 152 | +8.56 | -9.87 | -1.32 | +23.74 | PASS | 26.587 |
| `union_news_g_cam71_conv_h1` | long | 1 | conviction | list | none | 33% | 35% | 5/26 | -3.58 | 1 | 4 | +3.14 | -4.77 | +0.00 | -3.23 | PASS | 26.394 |
| `flatten_h5_s8` | long | 5 | leftover | list | none | 44% | 62% | 2/26 | -7.02 | 19 | 107 | +8.86 | -7.94 | +3.87 | +3.80 | PASS | 26.192 |
| `union_h5_topheavy` | long | 5 | topheavy | list | none | 45% | 58% | 4/26 | -6.71 | 29 | 149 | +8.60 | -8.27 | -1.80 | +18.51 | PASS | 26.159 |
| `union_blue_coil_h3` | long | 3 | leftover | list | none | 43% | 46% | 4/26 | -6.62 | 16 | 88 | +6.07 | -5.88 | -7.45 | -18.66 | PASS | 26.084 |
| `flatten_h5` | long | 5 | leftover | list | none | 44% | 62% | 2/26 | -5.41 | 19 | 107 | +8.90 | -8.31 | +3.39 | +3.80 | PASS | 25.9 |
| `flatten_h5_cut` | long | 5 | leftover | cut_loser | none | 44% | 62% | 2/26 | -5.41 | 19 | 107 | +8.90 | -8.31 | +3.39 | +3.80 | PASS | 25.9 |
| `flatten_h5_sizeup` | long | 5 | leftover | list | sizeup | 44% | 62% | 2/26 | -5.41 | 19 | 107 | +8.90 | -8.31 | +3.39 | +3.80 | PASS | 25.9 |
| `flatten_h5_time` | long | 5 | leftover | time | none | 44% | 62% | 2/26 | -5.41 | 19 | 107 | +8.90 | -8.31 | +3.39 | +3.80 | PASS | 25.9 |
| `flatten_h5_trail` | long | 5 | leftover | trail | none | 44% | 62% | 2/26 | -5.41 | 19 | 107 | +8.90 | -8.31 | +3.39 | +3.80 | PASS | 25.9 |
| `union_macd_xup_h1` | long | 1 | leftover | list | none | 39% | 31% | 5/26 | -0.31 | 11 | 34 | +3.50 | -4.03 | -8.60 | -14.92 | PASS | 25.87 |
| `short_macd_dn_h1` | short | 1 | leftover | list | none | 49% | 31% | 2/26 | -3.07 | 12 | 56 | +3.20 | -4.03 | -6.54 | -6.12 | PASS | 25.869 |
| `short_last_red_h3` | short | 3 | leftover | list | none | 55% | 42% | 3/26 | -3.69 | 19 | 120 | +4.80 | -5.96 | -9.55 | -19.97 | PASS | 25.733 |
| `union_news_pack_net2_h3` | long | 3 | leftover | list | none | 44% | 31% | 9/26 | -1.08 | 3 | 32 | +4.70 | -6.24 | -12.49 | -11.17 | PASS | 25.603 |
| `union_macd_xup_h3` | long | 3 | leftover | list | none | 38% | 38% | 12/26 | -1.43 | 20 | 75 | +4.05 | -8.56 | -13.97 | -35.97 | PASS | 25.56 |
| `flatten_h5_sboost` | long | 5 | leftover | list | both | 44% | 62% | 2/26 | -5.41 | 19 | 107 | +8.68 | -8.12 | +0.97 | +3.80 | PASS | 25.517 |
| `flatten_h3_sboost` | long | 3 | leftover | list | both | 45% | 42% | 1/26 | -8.77 | 11 | 74 | +7.81 | -5.30 | -5.58 | +2.48 | PASS | 25.48 |
| `flatten_h3` | long | 3 | leftover | list | none | 45% | 42% | 1/26 | -8.77 | 11 | 74 | +8.12 | -5.53 | -5.53 | +2.48 | PASS | 25.458 |
| `flatten_h3_cut` | long | 3 | leftover | cut_loser | none | 45% | 42% | 1/26 | -8.77 | 11 | 74 | +8.12 | -5.53 | -5.53 | +2.48 | PASS | 25.458 |
| `flatten_h3_sizeup` | long | 3 | leftover | list | sizeup | 45% | 42% | 1/26 | -8.77 | 11 | 74 | +8.12 | -5.53 | -5.53 | +2.48 | PASS | 25.458 |
| `flatten_h3_trail` | long | 3 | leftover | trail | none | 45% | 42% | 1/26 | -8.77 | 11 | 74 | +8.12 | -5.53 | -5.53 | +2.48 | PASS | 25.458 |
| `flatten_h3_half` | long | 3 | half | list | none | 45% | 42% | 1/26 | -4.79 | 11 | 74 | +7.14 | -5.12 | -3.89 | +2.48 | PASS | 25.323 |
| `union_news_or_net3_h1` | long | 1 | leftover | list | none | 45% | 35% | 0/26 | -5.43 | 1 | 24 | +3.84 | -3.92 | -4.11 | -2.54 | PASS | 25.241 |
| `union_h5_rankw` | long | 5 | rank_w | list | none | 45% | 58% | 4/26 | -8.25 | 29 | 149 | +7.69 | -8.54 | -3.78 | +18.51 | PASS | 25.157 |
| `union_news_present_h3` | long | 3 | leftover | list | none | 42% | 46% | 4/26 | -2.57 | 16 | 107 | +6.77 | -5.74 | -6.44 | +3.88 | PASS | 25.109 |
| `flatten_h3_time` | long | 3 | leftover | time | none | 45% | 42% | 1/26 | -8.04 | 11 | 74 | +7.37 | -5.73 | -2.54 | +2.48 | PASS | 24.996 |
| `union_coil_green_h1` | long | 1 | leftover | list | none | 42% | 35% | 0/26 | -5.07 | 13 | 41 | +3.45 | -3.76 | -2.21 | -5.13 | PASS | 24.821 |
| `union_e_green_h3` | long | 3 | leftover | list | none | 40% | 35% | 12/26 | +0.00 | 5 | 47 | +5.47 | -8.76 | -0.88 | -21.98 | PASS | 24.612 |
| `union_flow_in_h5` | long | 5 | leftover | list | none | 45% | 50% | 5/26 | -2.46 | 8 | 75 | +3.26 | -6.52 | +2.29 | -9.28 | PASS | 24.215 |
| `union_join_g_h3` | long | 3 | leftover | list | none | 42% | 35% | 4/26 | -4.68 | 19 | 110 | +7.42 | -5.94 | -2.93 | -0.18 | PASS | 24.209 |
| `union_news_vol_h3` | long | 3 | leftover | list | none | 49% | 42% | 3/26 | -6.19 | 9 | 61 | +4.34 | -5.02 | -6.19 | +186.66 | PASS | 24.132 |
| `union_e_green_h1` | long | 1 | leftover | list | none | 47% | 31% | 5/26 | -3.13 | 3 | 18 | +4.06 | -8.96 | -22.25 | -18.85 | PASS | 24.08 |
| `flatten_h3_rankw` | long | 3 | rank_w | list | none | 45% | 38% | 1/26 | -7.21 | 11 | 74 | +7.94 | -5.70 | -7.21 | +2.48 | PASS | 24.055 |
| `union_last_red_h1` | long | 1 | leftover | list | none | 51% | 31% | 1/26 | -5.48 | 9 | 67 | +3.64 | -5.43 | -6.02 | +13.24 | PASS | 24.049 |
| `flatten_h5_topheavy` | long | 5 | topheavy | list | none | 44% | 54% | 2/26 | -4.38 | 19 | 107 | +8.62 | -8.14 | +1.17 | +3.80 | PASS | 23.965 |
| `short_clk_neg_weak_fail_opp_h3` | short | 3 | leftover | list | none | 53% | 42% | 2/26 | -5.73 | 10 | 85 | +3.80 | -4.42 | -13.11 | -20.04 | PASS | 23.753 |
| `short_rsi_ob_h1` | short | 1 | leftover | list | none | 49% | 31% | 0/26 | -5.22 | 16 | 58 | +3.96 | -4.71 | -12.26 | -14.71 | PASS | 23.638 |
| `union_news_g_cam61_h1` | long | 1 | leftover | list | none | 38% | 23% | 4/26 | -3.43 | 1 | 11 | +3.26 | -4.25 | -3.08 | -3.06 | PASS | 23.549 |
| `union_news_pack_h3` | long | 3 | leftover | list | none | 46% | 31% | 7/26 | -3.53 | 5 | 38 | +4.27 | -6.29 | -14.29 | -10.21 | PASS | 23.543 |
| `short_extended_h1` | short | 1 | leftover | list | none | 51% | 27% | 0/26 | -8.32 | 29 | 65 | +5.22 | -6.29 | -15.61 | -9.24 | PASS | 23.311 |
| `union_white_any_h2` | long | 2 | leftover | list | none | 38% | 42% | 3/26 | -7.01 | 10 | 43 | +6.46 | -6.00 | -5.35 | +1.93 | PASS | 23.234 |
| `flatten_h5_half` | long | 5 | half | list | none | 44% | 54% | 1/26 | -5.05 | 19 | 107 | +7.81 | -7.00 | -0.55 | +3.80 | PASS | 23.026 |
| `short_clk_neg_weak_fail_h3` | short | 3 | leftover | list | none | 49% | 38% | 2/26 | -4.12 | 20 | 124 | +6.44 | -5.73 | -5.96 | -18.37 | PASS | 23.012 |
| `union_white_both_n4_h3` | long | 3 | leftover | list | none | 38% | 31% | 4/26 | -5.22 | 10 | 31 | +9.63 | -5.97 | +1.69 | +21.99 | PASS | 22.985 |
| `union_white_coil_h1` | long | 1 | leftover | list | none | 45% | 23% | 1/26 | -6.62 | 4 | 21 | +4.37 | -3.96 | -9.79 | -1.10 | PASS | 22.959 |
| `union_blue_h3` | long | 3 | leftover | list | none | 41% | 38% | 4/26 | -6.75 | 18 | 99 | +6.85 | -6.38 | -11.02 | -6.60 | PASS | 22.809 |
| `flatten_h3_topheavy` | long | 3 | topheavy | list | none | 45% | 38% | 1/26 | -6.64 | 11 | 74 | +6.20 | -5.72 | -5.37 | +2.48 | PASS | 22.792 |
| `flatten_h5_rankw` | long | 5 | rank_w | list | none | 44% | 50% | 2/26 | -5.72 | 19 | 107 | +8.25 | -8.53 | +0.12 | +3.80 | PASS | 22.571 |
| `short_extended_h3` | short | 3 | leftover | list | none | 54% | 42% | 0/26 | -10.66 | 51 | 138 | +8.39 | -9.02 | -14.34 | -34.50 | PASS | 22.558 |
| `ohlc_hot_h3` | long | 3 | leftover | list | none | 46% | 42% | 4/26 | -2.22 | 32 | 144 | +5.87 | -7.64 | -2.22 | +86.18 | PASS | 22.214 |
| `union_cond_h3` | long | 3 | leftover | list | none | 39% | 46% | 4/26 | -7.59 | 13 | 117 | +6.05 | -6.83 | -6.61 | -12.45 | PASS | 21.964 |
| `union_blue_h1` | long | 1 | leftover | list | none | 42% | 23% | 0/26 | -2.75 | 10 | 47 | +4.35 | -4.21 | -2.58 | -1.08 | PASS | 21.947 |
| `union_news_or_net4_h3` | long | 3 | leftover | list | none | 42% | 38% | 1/26 | -11.46 | 7 | 51 | +6.09 | -4.67 | -12.35 | -11.78 | PASS | 21.364 |
| `short_last_red_h1` | short | 1 | leftover | list | none | 48% | 23% | 0/26 | -2.04 | 9 | 69 | +4.66 | -4.63 | -10.05 | -9.80 | PASS | 21.056 |
| `union_candle_h3` | long | 3 | leftover | list | none | 45% | 27% | 4/26 | -4.73 | 25 | 119 | +6.90 | -7.53 | -7.45 | +11.13 | PASS | 20.748 |
| `combo_fer_5050_shared` | mix | 5 | leftover | list | none | 39% | 46% | 2/26 | -3.85 | 0 | 0 | +8.02 | -8.14 | +6.01 | — | PASS | 20.54 |
| `flatten_live_h3` | long | 3 | leftover | list | none | 58% | 12% | 0/26 | -6.24 | 4 | 14 | +8.60 | -8.85 | -6.24 | +6.87 | PASS | 20.399 |
| `union_news_or_net2_h3` | long | 3 | leftover | list | none | 43% | 38% | 2/26 | -7.06 | 9 | 75 | +5.29 | -5.57 | -11.57 | -7.42 | PASS | 20.375 |
| `union_news_pack_net3_h3` | long | 3 | leftover | list | none | 40% | 27% | 6/26 | -2.56 | 3 | 29 | +5.20 | -6.24 | -13.74 | -17.06 | PASS | 20.343 |
| `short_rsi_ob_h3` | short | 3 | leftover | list | none | 51% | 35% | 1/26 | -5.57 | 32 | 128 | +5.63 | -6.93 | -11.03 | -124.72 | PASS | 20.244 |
| `union_news_g_conv_h3` | long | 3 | conviction | list | none | 45% | 38% | 2/26 | -9.36 | 5 | 51 | +4.00 | -5.70 | -9.59 | +204.26 | PASS | 20.24 |
| `union_white_h5` | long | 5 | leftover | list | none | 39% | 58% | 1/26 | -7.87 | 18 | 75 | +9.83 | -9.09 | -1.32 | +37.34 | PASS | 19.996 |
| `short_clk_ext_veto_h3` | short | 3 | leftover | list | none | 49% | 38% | 0/26 | -11.32 | 30 | 109 | +7.40 | -7.31 | -15.49 | -96.29 | PASS | 19.896 |
| `union_coil_off_h1` | long | 1 | leftover | list | none | 41% | 23% | 0/26 | -6.87 | 6 | 52 | +3.23 | -3.62 | -8.57 | -12.13 | PASS | 19.586 |
| `union_rsi_h1` | long | 1 | leftover | list | none | 47% | 31% | 0/26 | -13.50 | 8 | 72 | +3.42 | -4.24 | -19.94 | -11.80 | PASS | 19.503 |
| `ohlc_hot_h5` | long | 5 | leftover | list | none | 44% | 42% | 4/26 | -7.71 | 45 | 171 | +7.88 | -9.54 | -7.71 | +67.51 | PASS | 19.36 |
| `union_white_any_h1` | long | 1 | leftover | list | none | 36% | 27% | 0/26 | -8.55 | 4 | 26 | +5.82 | -4.74 | -9.25 | -5.52 | PASS | 19.352 |
| `union_blue_coil_h1` | long | 1 | leftover | list | none | 42% | 19% | 0/26 | -12.47 | 8 | 39 | +3.79 | -4.39 | -14.84 | -15.65 | PASS | 18.941 |
| `union_cond_n4_h3` | long | 3 | leftover | list | none | 40% | 35% | 3/26 | -10.03 | 6 | 53 | +5.72 | -7.80 | -9.40 | -4.86 | PASS | 18.89 |
| `flatten_white_yday_h5` | long | 5 | leftover | list | none | 47% | 38% | 1/26 | -4.95 | 4 | 25 | +9.10 | -10.47 | -2.75 | +0.36 | PASS | 18.469 |
| `union_white_any_h5` | long | 5 | leftover | list | none | 34% | 62% | 1/26 | -9.75 | 16 | 72 | +9.10 | -9.03 | +0.47 | -11.24 | PASS | 18.382 |
| `union_news_g_cond_h3` | long | 3 | leftover | list | none | 46% | 27% | 1/26 | -9.22 | 15 | 100 | +5.14 | -5.56 | -13.57 | +155.50 | PASS | 17.99 |
| `union_news_or_h3` | long | 3 | leftover | list | none | 46% | 27% | 1/26 | -9.22 | 15 | 100 | +5.14 | -5.56 | -13.57 | +155.50 | PASS | 17.99 |
| `union_news_or_net5_h3` | long | 3 | leftover | list | none | 41% | 38% | 1/26 | -11.46 | 4 | 35 | +3.80 | -5.25 | -15.43 | -7.45 | PASS | 17.633 |
| `probable_h5` | long | 5 | leftover | list | none | 49% | 31% | 4/26 | -7.86 | 35 | 155 | +6.61 | -10.49 | -15.95 | +0.06 | PASS | 17.481 |
| `union_rsi_h3` | long | 3 | leftover | list | none | 43% | 38% | 1/26 | -4.85 | 13 | 137 | +5.94 | -6.03 | -8.76 | -18.43 | PASS | 17.033 |
| `union_ab_g_h3` | long | 3 | leftover | list | none | 38% | 27% | 1/26 | -10.92 | 14 | 93 | +7.68 | -5.83 | -11.59 | -13.45 | PASS | 16.61 |
| `union_news_head_h3` | long | 3 | leftover | list | none | 47% | 19% | 1/26 | -12.46 | 18 | 86 | +5.47 | -5.44 | -18.56 | +161.70 | PASS | 16.599 |
| `union_news_g_h3` | long | 3 | leftover | list | none | 49% | 27% | 0/26 | -9.46 | 19 | 100 | +4.39 | -6.17 | -17.38 | +149.62 | PASS | 16.573 |
| `flatten_live_h5` | long | 5 | leftover | list | none | 46% | 27% | 0/26 | -5.51 | 5 | 16 | +9.01 | -11.62 | -5.51 | +5.13 | PASS | 16.556 |
| `union_news_or_net3_h3` | long | 3 | leftover | list | none | 39% | 35% | 0/26 | -10.56 | 7 | 63 | +5.75 | -5.20 | -12.84 | -20.00 | PASS | 16.337 |
| `union_rsi_os_h3` | long | 3 | leftover | list | none | 43% | 27% | 5/26 | -2.10 | 5 | 66 | +5.17 | -5.98 | -20.99 | -46.46 | PASS | 15.853 |
| `union_coil_off_h3` | long | 3 | leftover | list | none | 39% | 27% | 4/26 | -10.56 | 18 | 114 | +4.47 | -8.29 | -13.85 | -11.68 | PASS | 15.572 |
| `coil_h3_exit_alarm` | long | 3 | leftover | list | none | 38% | 27% | 4/26 | -9.09 | 15 | 120 | +4.34 | -7.55 | -8.67 | -11.55 | PASS | 15.284 |
| `union_news_g_cam61_h3` | long | 3 | leftover | list | none | 43% | 23% | 1/26 | -11.91 | 2 | 28 | +4.00 | -5.77 | -14.78 | -13.88 | PASS | 15.187 |
| `union_rsi_os_h1` | long | 1 | leftover | list | none | 46% | 19% | 0/26 | -8.90 | 4 | 34 | +2.30 | -4.61 | -22.67 | +1.00 | PASS | 15.1 |
| `union_news_g_cam71_h3` | long | 3 | leftover | list | none | 41% | 31% | 1/26 | -18.72 | 1 | 18 | +4.52 | -8.50 | -19.33 | -2.95 | PASS | 15.048 |
| `union_white_coil_h3` | long | 3 | leftover | list | none | 42% | 35% | 1/26 | -18.67 | 8 | 47 | +5.31 | -8.04 | -17.96 | -22.72 | PASS | 14.908 |
| `union_coil_off_h5` | long | 5 | leftover | list | none | 41% | 38% | 4/26 | -13.97 | 25 | 147 | +3.84 | -54.69 | -9.80 | -21.62 | PASS | 14.174 |
| `union_macd_hist_h3` | long | 3 | leftover | list | none | 38% | 31% | 1/26 | -6.45 | 21 | 140 | +5.36 | -5.60 | -5.83 | -32.18 | PASS | 14.074 |
| `union_white_yday_h3` | long | 3 | leftover | list | none | 38% | 38% | 0/26 | -13.18 | 5 | 50 | +6.26 | -7.95 | -12.22 | -14.86 | PASS | 13.972 |
| `union_white_h3` | long | 3 | leftover | list | none | 36% | 38% | 1/26 | -15.30 | 11 | 64 | +7.74 | -7.39 | -15.15 | +3.44 | PASS | 13.845 |
| `union_coil_green_h3` | long | 3 | leftover | list | none | 40% | 23% | 1/26 | -13.21 | 24 | 114 | +4.59 | -7.32 | -14.74 | -13.46 | PASS | 12.594 |
| `flatten_vol_g_h3` | long | 3 | leftover | list | none | 46% | 19% | 1/26 | -17.31 | 5 | 28 | +9.63 | -9.84 | -25.48 | -21.05 | PASS | 12.59 |
| `union_flow_in_white_h3` | long | 3 | leftover | list | none | 35% | 12% | 0/26 | -7.16 | 0 | 9 | +2.70 | -2.45 | -5.14 | -2.17 | PASS | 12.48 |
| `union_news_g_h5` | long | 5 | leftover | list | none | 38% | 38% | 0/26 | -5.41 | 23 | 138 | +5.73 | -7.50 | -16.45 | +87.73 | PASS | 11.198 |
| `union_white_any_h3` | long | 3 | leftover | list | none | 35% | 35% | 0/26 | -15.30 | 11 | 62 | +7.07 | -8.18 | -13.12 | -11.63 | PASS | 11.13 |
| `union_rsi_os_h5` | long | 5 | leftover | list | none | 38% | 31% | 4/26 | -4.99 | 5 | 71 | +4.75 | -11.66 | -23.41 | -60.28 | PASS | 9.916 |
| `union_news_both_h1` *(thin)* | long | 1 | leftover | list | none | 83% | 12% | 21/26 | +7.29 | 0 | 0 | +3.69 | -0.73 | +7.29 | +8.40 | PASS | 61.926 |
| `union_rsi_os_macd_h3` *(thin)* | long | 3 | leftover | list | none | 64% | 23% | 26/26 | +1.01 | 2 | 6 | +5.34 | -3.95 | +1.01 | +9.58 | PASS | 43.806 |
| `union_news_both_h3` *(thin)* | long | 3 | leftover | list | none | 33% | 15% | 21/26 | +4.04 | 0 | 3 | +4.37 | -0.51 | +4.04 | +6.07 | PASS | 32.206 |
| `short_news_pack_h3` *(thin)* | short | 3 | leftover | list | none | 50% | 35% | 12/26 | +0.00 | 2 | 8 | +2.94 | -2.87 | +5.57 | -2.01 | PASS | 16.296 |
| `union_news_missing_h1` *(thin)* | long | 1 | leftover | list | none | 50% | 4% | 1/26 | +0.00 | 0 | 2 | +7.93 | -3.05 | +1.44 | +1.88 | PASS | 9.958 |
| `union_vol_missing_h1` *(thin)* | long | 1 | leftover | list | none | 50% | 4% | 1/26 | +0.00 | 0 | 2 | +7.93 | -3.05 | +1.44 | +1.88 | PASS | 9.958 |
| `union_news_missing_h3` *(thin)* | long | 3 | leftover | list | none | 50% | 12% | 1/26 | +0.00 | 1 | 5 | +11.98 | -3.98 | +3.58 | +5.59 | PASS | 8.183 |
| `union_vol_missing_h3` *(thin)* | long | 3 | leftover | list | none | 50% | 12% | 1/26 | +0.00 | 1 | 5 | +11.98 | -3.98 | +3.58 | +5.59 | PASS | 8.183 |
| `union_catal_present_h1` *(thin)* | long | 1 | leftover | list | none | 29% | 8% | 0/26 | -8.01 | 0 | 1 | +7.67 | -2.82 | -8.01 | +0.16 | PASS | 2.506 |
| `union_clk_nr7_mom_opp_h1` *(thin)* | long | 1 | leftover | list | none | 43% | 23% | 1/26 | -1.20 | 0 | 4 | +1.22 | -1.28 | -0.74 | +0.88 | PASS | 1.658 |
| `union_news_g_cam91_n1_h1` *(thin)* | long | 1 | leftover | list | none | 50% | 4% | 0/26 | -2.85 | 0 | 0 | +0.85 | -3.84 | -2.85 | -0.09 | PASS | 1.447 |
| `union_clk_earn_guide_react_h1` *(thin)* | long | 1 | leftover | list | none | 45% | 8% | 0/26 | -3.11 | 1 | 4 | +5.68 | -6.17 | -5.43 | +10.90 | PASS | -2.395 |
| `union_rsi_os_macd_h1` *(thin)* | long | 1 | leftover | list | none | 27% | 15% | 6/26 | -6.68 | 1 | 3 | +0.40 | -3.05 | -6.68 | -5.83 | PASS | -4.678 |
| `union_catal_present_h3` *(thin)* | long | 3 | leftover | list | none | 14% | 12% | 0/26 | -13.16 | 1 | 4 | +8.08 | -4.28 | -13.16 | -17.08 | PASS | -13.784 |
| `short_r_down_h1` *(thin)* | short | 1 | leftover | list | none | — | 0% | 0/26 | +0.00 | 0 | 0 | — | — | +0.00 | +0.00 | PASS | -15.0 |
| `short_r_down_h3` *(thin)* | short | 3 | leftover | list | none | — | 0% | 0/26 | +0.00 | 0 | 0 | — | — | +0.00 | +0.00 | PASS | -15.0 |
| `union_clk_r_up_coil_h1` *(thin)* | long | 1 | leftover | list | none | — | 0% | 0/26 | +0.00 | 0 | 0 | — | — | +0.00 | +0.00 | PASS | -15.0 |
| `union_r_up_h1` *(thin)* | long | 1 | leftover | list | none | — | 0% | 0/26 | +0.00 | 0 | 0 | — | — | +0.00 | +0.00 | PASS | -15.0 |
| `union_r_up_h3` *(thin)* | long | 3 | leftover | list | none | — | 0% | 0/26 | +0.00 | 0 | 0 | — | — | +0.00 | +0.00 | PASS | -15.0 |
| `union_clk_insider_cash_stab_h3` *(thin)* | long | 3 | leftover | list | none | 17% | 4% | 0/26 | -7.15 | 0 | 4 | +0.08 | -6.82 | -12.31 | -16.14 | PASS | -27.687 |

## Overnight / hold-up splice (leak-free calendar)

Spliced onto the 2026-09-19 remine so Pages shows the researched books.
Same $10k / 09:30 / leftover / fees / hard-red sit rules. Not live.

| Strategy | Side | H | Book% | Trades | Hit | MaxDD | Starts YES |
|---|---|---:|---:|---:|---:|---:|---:|
| `union_hot_n4_holdup` | long | 1 | 56.525 | 74 | 0.6571 | 6.89 | 25/26 |
| `combo_oh_5050_shared` | mix | 5 | 18.612 | 114 | 0.5455 | 9.08 | 22/26 |
| `overnight_mega_h2` | long | 2 | 0.504 | 6 | 0.3333 | 5.43 | 7/26 |
| `overnight_mega_h1` | long | 1 | -0.266 | 20 | 0.7 | 10.51 | 1/26 |
| `overnight_mega_green_h1` | long | 1 | -3.376 | 16 | 0.75 | 11.22 | 1/26 |
| `overnight_h1` | long | 1 | -19.036 | 98 | 0.4694 | 21.22 | 0/26 |
