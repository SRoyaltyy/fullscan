# Factor strategy mine — 2026-08-13 → 2026-09-04

Leak-free 09:30 recipes: **23** · candidate rows **1347** · fill `09:30 open, whole shares, Futubull fees, leftover split, sell first, hard-red sit, cash+holdings audit`.

Cash book: $10k, whole shares, Futubull fees, leftover split, sell first, min-hold, 09:30 open, hard-red S≤−3 sit, shorts marked as a liability. Each session starts from leftover cash and lots actually held (butterfly). Cash-start buttons wake a sleeve on date X with $10k and no lots (same rules). Stock investigator quotes 09:30 cameras / coaches / news from repo files. Size / sell / S-boost tweaks sit on the same ledger. Signal-only % is the old equal-weight path (not a fill). `flatten_h*` = wish-list (io/HOLD mornings still buy). `flatten_live_*` = only when the live flatten gate fires. Research only — does not change live `flatten_robust`.

Action blotters: [FACTOR_MINE_ACTION.md](FACTOR_MINE_ACTION.md).

| Strategy | Side | H | Size | Sell | Boost | Win% | $ days | Starts YES | Med start | Top-g | Losers | AvgW | AvgL | Book% | Signal% | Audit | Eff |
|---|---|---:|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---:|
| `flatten_h5` | long | 5 | leftover | list | none | 60% | 76% | 16/17 | +6.20 | 13 | 65 | +10.07 | -6.04 | +22.84 | +67.92 | PASS | 74.794 |
| `flatten_h5_cut` | long | 5 | leftover | cut_loser | none | 60% | 76% | 16/17 | +6.20 | 13 | 65 | +10.07 | -6.04 | +22.84 | +67.92 | PASS | 74.794 |
| `flatten_h5_sizeup` | long | 5 | leftover | list | sizeup | 60% | 76% | 16/17 | +6.20 | 13 | 65 | +10.07 | -6.04 | +22.84 | +67.92 | PASS | 74.794 |
| `flatten_h5_time` | long | 5 | leftover | time | none | 60% | 76% | 16/17 | +6.20 | 13 | 65 | +10.07 | -6.04 | +22.84 | +67.92 | PASS | 74.794 |
| `flatten_h5_trail` | long | 5 | leftover | trail | none | 60% | 76% | 16/17 | +6.20 | 13 | 65 | +10.07 | -6.04 | +22.84 | +67.92 | PASS | 74.794 |
| `flatten_h5_sboost` | long | 5 | leftover | list | both | 60% | 76% | 16/17 | +6.20 | 13 | 65 | +9.68 | -6.06 | +22.18 | +67.92 | PASS | 74.354 |
| `flatten_h1` | long | 1 | leftover | list | none | 57% | 53% | 16/17 | +5.38 | 2 | 14 | +4.91 | -3.53 | +15.53 | +21.67 | PASS | 73.806 |
| `flatten_h3` | long | 3 | leftover | list | none | 63% | 53% | 16/17 | +4.48 | 6 | 42 | +7.40 | -4.04 | +11.77 | +44.29 | PASS | 73.537 |
| `flatten_h5_rankw` | long | 5 | rank_w | list | none | 60% | 71% | 17/17 | +7.30 | 13 | 65 | +9.29 | -6.18 | +17.31 | +67.92 | PASS | 73.444 |
| `flatten_h5_topheavy` | long | 5 | topheavy | list | none | 60% | 65% | 17/17 | +6.17 | 13 | 65 | +9.61 | -6.12 | +18.26 | +67.92 | PASS | 72.739 |
| `flatten_h5_half` | long | 5 | half | list | none | 60% | 71% | 16/17 | +3.17 | 13 | 65 | +8.90 | -4.97 | +12.43 | +67.92 | PASS | 72.681 |
| `union_h5` | long | 5 | leftover | list | none | 58% | 76% | 14/17 | +2.89 | 16 | 86 | +9.28 | -6.87 | +18.84 | +58.01 | PASS | 67.906 |
| `union_h5_exit_alarm` | long | 5 | leftover | list | none | 57% | 59% | 14/17 | +2.89 | 17 | 87 | +7.65 | -7.35 | +13.73 | +61.86 | PASS | 61.506 |
| `union_last_green_h5` | long | 5 | leftover | list | none | 52% | 71% | 12/17 | +2.93 | 17 | 91 | +9.48 | -8.41 | +19.41 | +52.19 | PASS | 59.402 |
| `flatten_vol_g_h3` | long | 3 | leftover | list | none | 45% | 35% | 14/17 | +3.66 | 3 | 12 | +8.37 | -7.08 | -1.69 | +0.40 | PASS | 53.009 |
| `union_coil_off_h5` | long | 5 | leftover | list | none | 47% | 47% | 10/17 | +0.74 | 15 | 85 | +4.30 | -23.59 | +9.72 | +7.47 | PASS | 43.686 |
| `union_white_h5` | long | 5 | leftover | list | none | 47% | 59% | 8/17 | -2.90 | 14 | 51 | +9.97 | -8.15 | +4.04 | +12.71 | PASS | 37.907 |
| `yday_gainer_h5` | long | 5 | leftover | list | none | 49% | 35% | 7/17 | -2.47 | 17 | 83 | +12.99 | -8.51 | -8.09 | +8.88 | PASS | 31.687 |
| `ohlc_hot_h5` | long | 5 | leftover | list | none | 53% | 29% | 8/17 | -0.27 | 21 | 86 | +6.67 | -7.67 | -8.27 | +86.73 | PASS | 30.2 |
| `union_vol_g_h5` | long | 5 | leftover | list | none | 46% | 29% | 7/17 | -4.40 | 20 | 80 | +13.62 | -9.04 | -10.12 | +7.06 | PASS | 27.915 |
| `union_news_g_h5` | long | 5 | leftover | list | none | 54% | 29% | 7/17 | -3.26 | 13 | 79 | +5.17 | -7.71 | -11.63 | +152.34 | PASS | 27.153 |
| `probable_h5` | long | 5 | leftover | list | none | 48% | 6% | 6/17 | -1.74 | 18 | 88 | +9.47 | -10.37 | -19.05 | -5.25 | PASS | 18.521 |
| `flatten_live_h5` *(thin)* | long | 5 | leftover | list | none | 38% | 24% | 7/17 | +0.00 | 3 | 9 | +10.21 | -6.91 | +5.85 | +8.02 | PASS | 9.836 |
