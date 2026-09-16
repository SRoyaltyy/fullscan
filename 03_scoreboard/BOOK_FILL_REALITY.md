# $10k butterfly fill realities — research only

combo_sh_5050_shared $10k path: ideal +37.49% · market_mid -9.51% · market_adverse -60.28% · limit_open +17.89% · partial_50 -2.38% — market_mid equity dipped under $10k. Research only.

## Scope

- **Research only.** Live `flatten_robust`, hard-red sit, and Webull paper are **untouched**. This is not a wire into the published cash book.
- Ledger is the real leftover-cash butterfly (`factor_mine_book` / combo shared pile), **not** PR 241's 1-share open→same-day-close lab.
- Window `2026-08-13` → `2026-09-15` (23 sessions). Orders sent 09:30 ET.
- Daily OHLC has no true tape. Limit trade-through is low < limit (buy) / high > limit (short). Market path walks the printed high/low room. A missing official open is a miss — close is never substituted.
- ideal is the published Book% fill (official 09:30 open). It is the optimistic published column, not a forecast of the best live fill.
- market_favorable fills at the session favorable extreme (buy at low / short at high) only when that print exists. Optimistic / unlikely — not a forecast.
- market_adverse fills at the session adverse extreme the bar printed (buy at high / short at low). Absolute worst still inside [low, high]. Cannot invent outside the tape.

## Correct a misread

combo_sh_5050_shared Starts YES is wake-$10k-on-each-date replay (path finishes green), not a daily win rate. Win% is the share of sessions whose close equity beat the prior close — do not write 21/23 as a daily win rate.

This run's ideal combo_sh_5050_shared is the current official-tape replay. The Action MD snapshot was +41.54% ($14,153.61, 155 fills, 21/23 starts YES). Paths match through 2026-09-11; on 9/14–9/15 IRD / BNC / CMRC have no official 09:30 in ohlc.parquet or session_bar, so those lots carried instead of selling. Missing open = no fill — close is not substituted. That leftover carry is why this run's ideal is below the snapshot, not a different recipe.

## Method

- Same `pick_day` lists, leftover / sell-first / min-hold / hard-red sit as the published book. Day N open cash/held = day N−1 close after **that** reality's fills.
- Whole shares, Futubull fees, short liability equity ≥ 2× notional. Missing official open = no fill.
- Fill functions: `resolve_reality` + PR 241 `calibrate_slip` / `DELAY_FRAC` / `market_fill` for `market_mid`. Limit trade-through is strict (buy: low < limit; short: high > limit).
- `partial_50`: 50% of the shares `market_mid` would have taken (floor 1 if intended ≥ 2; intended 1 is all-or-none). Fees on filled shares only. Unfilled cash stays leftover.
- `gap_miss`: |open−prior|/prior ≥ 8% against the order (buy gaps up / short gaps down) → miss; else official open.

Regenerate: `PYTHONPATH=. python3 -m src.book_fill_reality --write`.

Dashboard: [book-fill-reality.html](../dashboard/factor-mine/book-fill-reality.html).

Live untouched: `flatten_robust`, `hard_red_sit`, `webull_paper`.

## Book% range (worst … mid … ideal … best)

| Strategy | WORST adverse | mid (market) | ideal | BEST favorable | mid ≥$10k | Starts YES (ideal) | Audit |
|---|---:|---:|---:|---:|---|---:|---|
| `combo_sh_5050_shared` | -60.28% | -9.51% | +37.49% | +242.15% | NO | 21/23 | PASS |
| `combo_sh_3070_shared` | -63.67% | -15.30% | +33.02% | +268.79% | NO | 21/23 | PASS |
| `combo_sh_7030_shared` | -48.57% | -4.13% | +31.91% | +167.66% | NO | 17/23 | PASS |
| `combo_ps_5050_shared` | -32.37% | +2.20% | +27.02% | +98.23% | NO | 21/23 | PASS |
| `combo_p2s_5050_shared` | -32.43% | +2.09% | +27.79% | +102.49% | NO | 21/23 | PASS |
| `combo_seh_451540_shared` | -54.37% | -8.57% | +30.82% | +185.24% | NO | 21/23 | PASS |
| `union_hot_n4_h1` | -63.70% | -21.91% | +20.01% | +207.18% | NO | 15/23 | PASS |
| `union_news_pack_net2_h1` | -28.08% | +2.11% | +24.02% | +62.42% | NO | 21/23 | PASS |
| `short_news_r_h3` | -9.95% | +5.74% | +15.06% | +46.64% | NO | 17/23 | PASS |
| `flatten_h3` / flatten_would | -22.13% | -6.59% | +2.07% | +25.97% | NO | 3/23 | PASS |
| `combo_jse_333_shared` | -34.57% | +4.58% | +31.50% | +135.32% | YES | 15/23 | PASS |
| `combo_ej_5050_shared` | -29.92% | +3.26% | +27.32% | +116.02% | YES | 8/23 | PASS |
| `combo_es_9010_shared` | -25.49% | +4.43% | +24.79% | +89.37% | YES | 17/23 | PASS |
| `combo_se_3070_shared` | -27.47% | +4.83% | +26.38% | +93.58% | YES | 16/23 | PASS |
| `union_e_fresh_h3` | -23.89% | +3.88% | +22.95% | +83.65% | YES | 16/23 | PASS |
| `combo_seh_333_skip` | -45.63% | -4.43% | +28.42% | +147.75% | NO | 21/23 | PASS |
| `combo_seh_502525_shared` | -45.75% | -4.18% | +28.89% | +143.90% | NO | 17/23 | PASS |
| `combo_seh_333_shared` | -46.54% | -6.70% | +25.59% | +140.66% | NO | 21/23 | PASS |
| `combo_seh_333_weather` | -46.54% | -6.70% | +25.59% | +140.66% | NO | 21/23 | PASS |
| `combo_seh_403525_shared` | -43.55% | -3.76% | +26.82% | +135.06% | NO | 20/23 | PASS |
| `combo_seh_404020_shared` | -40.35% | -2.32% | +26.68% | +124.86% | NO | 16/23 | PASS |
| `combo_es_8020_shared` | -25.83% | +4.49% | +26.12% | +90.94% | YES | 14/23 | PASS |
| `combo_ps_7030_shared` | -34.02% | +1.60% | +27.34% | +100.45% | NO | 21/23 | PASS |
| `combo_ee1_3070_shared` | -23.89% | +3.88% | +22.95% | +83.65% | YES | 16/23 | PASS |
| `combo_ee1_5050_shared` | -23.89% | +3.88% | +22.95% | +83.65% | YES | 16/23 | PASS |
| `combo_ee1_7030_shared` | -23.89% | +3.88% | +22.95% | +83.65% | YES | 16/23 | PASS |
| `combo_ehs_601525_shared` | -33.65% | -0.21% | +23.96% | +103.86% | NO | 20/23 | PASS |
| `combo_eh_3070_shared` | -53.45% | -15.93% | +17.07% | +135.76% | NO | 15/23 | PASS |
| `combo_ehs_702010_shared` | -33.66% | -2.12% | +21.62% | +99.84% | NO | 16/23 | PASS |
| `combo_eh_5050_shared` | -44.22% | -10.25% | +18.53% | +113.41% | NO | 17/23 | PASS |
| `combo_eh_7030_shared` | -36.21% | -4.87% | +19.41% | +99.03% | NO | 16/23 | PASS |
| `union_news_pack_h1` | -29.71% | -0.72% | +19.75% | +56.83% | NO | 21/23 | PASS |
| `combo_hn_7030_shared` | -61.03% | -21.34% | +15.74% | +171.40% | NO | 15/23 | PASS |
| `combo_her_5050_shared` | -46.15% | -12.98% | +14.56% | +111.33% | NO | 16/23 | PASS |
| `union_earn_react_h3` | -26.68% | +0.33% | +18.91% | +78.42% | YES | 12/23 | PASS |
| `combo_seh_333_split` | -34.36% | -6.31% | +16.83% | +107.68% | NO | 20/23 | PASS |
| `combo_he1_5050_shared` | -63.77% | -23.86% | +14.13% | +181.72% | NO | 19/23 | PASS |
| `combo_seh_601525_shared` | -45.28% | -3.49% | +29.46% | +145.04% | NO | 17/23 | PASS |
| `combo_se_5050_skip` | -30.22% | +4.51% | +29.03% | +98.71% | YES | 16/23 | PASS |
| `combo_jer_5050_shared` | -29.99% | +2.53% | +26.30% | +117.13% | YES | 8/23 | PASS |
| `combo_ers_7030_shared` | -27.94% | +3.30% | +24.70% | +89.40% | YES | 15/23 | PASS |
| `combo_se1_5050_shared` | -51.29% | -8.26% | +27.56% | +158.50% | NO | 16/23 | PASS |
| `combo_ser_5050_shared` | -30.77% | +2.76% | +25.46% | +93.14% | YES | 16/23 | PASS |
| `combo_sj_5050_shared` | -43.66% | -5.18% | +23.74% | +187.08% | NO | 17/23 | PASS |
| `combo_e1s_7030_shared` | -55.82% | -13.57% | +23.05% | +185.29% | NO | 16/23 | PASS |
| `combo_sj_3070_shared` | -49.02% | -9.74% | +21.84% | +220.53% | NO | 17/23 | PASS |
| `combo_sj_7030_shared` | -32.21% | -1.44% | +19.26% | +120.96% | NO | 17/23 | PASS |
| `combo_snj_333_shared` | -53.80% | -15.16% | +17.14% | +176.87% | NO | 17/23 | PASS |
| `combo_sn_5050_shared` | -45.78% | -10.62% | +16.30% | +112.39% | NO | 17/23 | PASS |
| `combo_je1_5050_shared` | -57.45% | -19.57% | +12.56% | +191.72% | NO | 14/23 | PASS |
| `flatten_live_h1` | -14.76% | -0.23% | +8.22% | +24.22% | NO | 7/23 | PASS |
| `flatten_live_h3` | -17.17% | -10.19% | -6.24% | +3.43% | NO | 0/23 | PASS |
| `flatten_live_h5` | -12.99% | -8.35% | -5.59% | +0.78% | NO | 0/23 | PASS |
| `union_news_g_h5` | -27.33% | -16.36% | -9.38% | +6.54% | NO | 0/23 | PASS |
| `union_white_coil_h1` | -40.19% | -18.24% | -3.65% | +53.26% | NO | 8/23 | PASS |
| `union_news_or_h1` | -44.78% | -16.55% | +3.83% | +71.86% | NO | 15/23 | PASS |
| `union_news_or_net4_h1` | -38.70% | -16.66% | -2.37% | +66.56% | NO | 0/23 | PASS |
| `union_news_or_net4_conv_h1` | -34.65% | -12.69% | +2.12% | +41.90% | NO | 2/23 | PASS |
| `short_news_head_h3` | -12.50% | +4.00% | +14.03% | +38.25% | NO | 17/23 | PASS |
| `union_news_g_cam91_n1_h1` | -9.37% | -4.79% | -2.85% | +1.43% | NO | 0/23 | PASS |
| `union_news_both_h1` | -2.61% | +1.36% | +4.14% | +7.74% | NO | 21/23 | PASS |
| `union_news_g_cam71_h1` | -34.77% | -16.34% | -4.86% | +27.03% | NO | 0/23 | PASS |
| `union_news_g_conv_h1` | -35.99% | -12.05% | +5.23% | +46.52% | NO | 3/23 | PASS |
| `union_e_green_h3` | -51.21% | -25.71% | -6.36% | +73.85% | NO | 4/23 | PASS |
| `flatten_h5` | -15.41% | -0.84% | +7.99% | +27.84% | NO | 3/23 | PASS |
| `flatten_h5_rankw` | -16.41% | -3.60% | +3.89% | +24.27% | NO | 1/23 | PASS |
| `flatten_h5_time` | -15.41% | -0.84% | +7.99% | +27.84% | NO | 3/23 | PASS |
| `flatten_h5_sboost` | -15.90% | -1.36% | +5.54% | +26.77% | NO | 2/23 | PASS |
| `union_h5_sboost` | -18.22% | -4.82% | +3.27% | +24.71% | NO | 2/23 | PASS |
| `flatten_live_h1_sizeup` | -14.76% | -0.23% | +8.22% | +24.22% | NO | 7/23 | PASS |
| `union_h3_cut` | -23.90% | -5.58% | +4.07% | +39.95% | NO | 5/23 | PASS |
| `union_h1_topheavy` | -35.41% | -9.78% | +7.66% | +61.79% | NO | 7/23 | PASS |
| `union_h1` | -41.32% | -14.70% | +4.63% | +75.73% | NO | — | PASS |
| `union_h3` | -24.52% | -6.86% | +2.45% | +38.03% | NO | — | PASS |
| `union_h5` | -17.77% | -4.24% | +3.83% | +26.01% | NO | — | PASS |
| `flatten_h1` | -36.49% | -10.87% | +6.53% | +60.88% | NO | — | PASS |
| `probable_h1` | -45.47% | -20.05% | -2.11% | +91.59% | NO | — | PASS |
| `probable_h3` | -31.21% | -15.37% | -5.38% | +36.87% | NO | — | PASS |
| `probable_h5` | -32.54% | -22.74% | -16.93% | +5.28% | NO | — | PASS |
| `yday_gainer_h1` | -45.23% | -16.09% | +5.60% | +118.34% | NO | — | PASS |
| `yday_gainer_h3` | -30.51% | -14.78% | -5.08% | +41.24% | NO | — | PASS |
| `yday_gainer_h5` | -25.95% | -11.83% | -3.92% | +23.42% | NO | — | PASS |
| `ohlc_hot_h1` | -49.45% | -25.54% | -8.14% | +65.61% | NO | — | PASS |
| `ohlc_hot_h3` | -34.31% | -13.58% | -2.20% | +37.62% | NO | — | PASS |
| `ohlc_hot_h5` | -32.12% | -19.92% | -9.06% | +10.67% | NO | — | PASS |
| `union_vol_g_h1` | -50.39% | -23.06% | -2.12% | +133.58% | NO | — | PASS |
| `union_vol_g_h3` | -37.65% | -20.24% | -8.57% | +35.63% | NO | — | PASS |
| `union_vol_missing_h1` | -6.32% | -1.23% | +1.44% | +8.87% | NO | — | PASS |
| `union_vol_missing_h3` | -4.26% | +0.82% | +3.58% | +10.77% | NO | — | PASS |
| `union_ab_g_h1` | -30.71% | -10.09% | +2.75% | +40.47% | NO | — | PASS |
| `union_ab_g_h3` | -23.23% | -11.93% | -5.54% | +14.84% | NO | — | PASS |
| `union_join_g_h1` | -47.83% | -19.08% | +2.97% | +79.06% | NO | — | PASS |
| `union_join_g_h3` | -29.97% | -13.12% | -3.85% | +29.19% | NO | — | PASS |
| `union_join_present_h1` | -41.62% | -15.06% | +4.22% | +75.23% | NO | — | PASS |
| `union_join_present_h3` | -24.82% | -6.83% | +2.87% | +36.46% | NO | — | PASS |
| `union_news_g_h1` | -46.27% | -17.59% | +4.33% | +73.02% | NO | — | PASS |
| `union_news_g_h3` | -36.16% | -20.96% | -10.86% | +26.47% | NO | — | PASS |
| `union_news_present_h1` | -39.79% | -15.12% | +2.00% | +62.49% | NO | — | PASS |
| `union_news_present_h3` | -29.46% | -14.56% | -5.31% | +24.40% | NO | — | PASS |
| `union_news_missing_h1` | -10.07% | -2.44% | +1.64% | +11.70% | NO | — | PASS |
| `union_news_missing_h3` | -8.54% | -1.12% | +3.13% | +14.61% | NO | — | PASS |
| `union_catal_present_h1` | -7.62% | -4.57% | -2.93% | +2.33% | NO | — | PASS |
| `union_catal_present_h3` | -7.26% | -4.31% | -2.73% | +2.67% | NO | — | PASS |
| `union_blue_h1` | -40.94% | -16.22% | +1.51% | +67.44% | NO | — | PASS |
| `union_blue_h3` | -30.80% | -16.09% | -7.24% | +25.07% | NO | — | PASS |
| `union_white_h1` | -48.51% | -17.50% | +6.31% | +161.00% | NO | — | PASS |
| `union_white_h3` | -27.85% | -12.20% | -2.81% | +34.69% | NO | — | PASS |
| `union_last_green_h1` | -46.63% | -17.77% | +4.09% | +99.37% | NO | — | PASS |
| `union_last_green_h3` | -30.13% | -12.38% | -1.06% | +35.89% | NO | — | PASS |
| `union_last_red_h1` | -53.26% | -23.93% | -0.88% | +91.50% | NO | — | PASS |
| `union_last_red_h3` | -28.88% | -10.12% | +1.34% | +40.06% | NO | — | PASS |
| `union_candle_h1` | -49.66% | -21.75% | -0.24% | +88.23% | NO | — | PASS |
| `union_candle_h3` | -32.59% | -16.50% | -5.07% | +31.98% | NO | — | PASS |
| `union_coil_off_h1` | -49.96% | -25.87% | -8.70% | +56.67% | NO | — | PASS |
| `union_coil_off_h3` | -36.67% | -19.45% | -7.15% | +22.30% | NO | — | PASS |
| `union_earn_react_h1` | -59.09% | -25.15% | +2.11% | +131.03% | NO | — | PASS |
| `union_e_fresh_h1` | -52.37% | -18.32% | +9.71% | +126.27% | NO | — | PASS |
| `union_r_up_h1` | +0.00% | +0.00% | +0.00% | +0.00% | YES | — | PASS |
| `union_r_up_h3` | +0.00% | +0.00% | +0.00% | +0.00% | YES | — | PASS |
| `union_break10_h1` | -56.24% | -27.96% | -5.26% | +108.60% | NO | — | PASS |
| `union_break10_h3` | -39.50% | -18.20% | -4.00% | +41.77% | NO | — | PASS |
| `union_vol_g_h5` | -32.65% | -20.73% | -12.86% | +9.37% | NO | — | PASS |
| `union_coil_off_h5` | -22.46% | -11.08% | -3.71% | +15.51% | NO | — | PASS |
| `union_last_green_h5` | -20.72% | -6.38% | +2.69% | +25.16% | NO | — | PASS |
| `union_white_h5` | -15.23% | -4.32% | +1.81% | +23.94% | NO | — | PASS |
| `union_vol_ab_h1` | -40.78% | -17.17% | -1.06% | +56.53% | NO | — | PASS |
| `union_vol_ab_h3` | -27.11% | -13.46% | -4.64% | +25.51% | NO | — | PASS |
| `union_blue_vol_h1` | -49.19% | -23.68% | -4.62% | +113.66% | NO | — | PASS |
| `union_blue_vol_h3` | -36.08% | -19.35% | -8.26% | +30.48% | NO | — | PASS |
| `union_news_vol_h1` | -37.99% | -15.06% | +0.94% | +56.63% | NO | — | PASS |
| `union_news_vol_h3` | -34.25% | -15.78% | -3.55% | +36.30% | NO | — | PASS |
| `union_e_green_h1` | -73.75% | -47.01% | -23.43% | +114.64% | NO | — | PASS |
| `probable_probable_ok_h1` | -45.17% | -19.71% | -1.97% | +76.32% | NO | — | PASS |
| `probable_probable_ok_h3` | -43.79% | -28.01% | -17.75% | +20.82% | NO | — | PASS |
| `union_vol_green_h1` | -48.05% | -17.03% | +7.34% | +193.53% | NO | — | PASS |
| `union_vol_green_h3` | -44.31% | -27.55% | -15.77% | +28.80% | NO | — | PASS |
| `union_coil_green_h1` | -51.49% | -22.98% | -1.22% | +69.78% | NO | — | PASS |
| `union_coil_green_h3` | -31.34% | -15.71% | -5.24% | +25.25% | NO | — | PASS |
| `union_blue_coil_h1` | -41.21% | -21.12% | -6.81% | +53.12% | NO | — | PASS |
| `union_blue_coil_h3` | -28.88% | -14.47% | -5.73% | +25.11% | NO | — | PASS |
| `union_join_vol_green_h1` | -49.45% | -16.29% | +9.61% | +165.85% | NO | — | PASS |
| `union_join_vol_green_h3` | -42.29% | -24.26% | -11.67% | +28.89% | NO | — | PASS |
| `union_white_coil_h3` | -24.63% | -7.76% | +2.11% | +29.46% | NO | — | PASS |
| `flatten_vol_g_h3` | -35.71% | -24.74% | -18.80% | +6.88% | NO | — | PASS |
| `ohlc_hot_coil_h1` | -53.68% | -33.26% | -18.73% | +39.91% | NO | — | PASS |
| `union_hot_score_h1` | -65.36% | -31.10% | +1.18% | +151.17% | NO | — | PASS |
| `union_hot_score_h3` | -34.30% | -10.65% | +8.86% | +68.96% | NO | — | PASS |
| `union_candle_score_h1` | -48.53% | -20.60% | +1.70% | +101.59% | NO | — | PASS |
| `union_candle_score_h3` | -25.72% | -7.50% | +4.73% | +57.73% | NO | — | PASS |
| `union_ret_5_h1` | -64.09% | -30.21% | +1.22% | +148.44% | NO | — | PASS |
| `union_ret_5_h3` | -30.25% | -6.40% | +10.52% | +71.33% | NO | — | PASS |
| `union_cond_h1` | -48.47% | -21.95% | -2.45% | +71.36% | NO | — | PASS |
| `union_cond_h3` | -25.83% | -10.63% | -2.01% | +28.23% | NO | — | PASS |
| `union_w_hot_cond_h1` | -64.16% | -30.94% | -0.42% | +141.69% | NO | — | PASS |
| `union_w_hot_cond_h3` | -33.63% | -10.97% | +6.50% | +60.60% | NO | — | PASS |
| `union_w_hot_candle_h1` | -60.25% | -27.43% | +1.27% | +135.05% | NO | — | PASS |
| `union_w_hot_candle_h3` | -34.73% | -12.52% | +4.80% | +62.68% | NO | — | PASS |
| `union_hot_n12_h1` | -63.99% | -31.90% | -3.93% | +124.08% | NO | — | PASS |
| `union_cond_n4_h3` | -27.09% | -8.44% | +2.35% | +34.94% | NO | — | PASS |
| `union_h3_exit_alarm` | -24.75% | -8.52% | +2.99% | +36.40% | NO | — | PASS |
| `union_h5_exit_alarm` | -19.69% | -5.23% | +3.34% | +28.94% | NO | — | PASS |
| `union_h3_exit_red` | -31.58% | -13.03% | -0.63% | +40.56% | NO | — | PASS |
| `union_h3_exit_news_r` | -26.29% | -8.50% | +1.64% | +35.16% | NO | — | PASS |
| `coil_h3_exit_alarm` | -29.81% | -13.79% | -3.74% | +25.05% | NO | — | PASS |
| `short_alarm_h1` | -13.68% | -3.78% | +1.71% | +16.86% | NO | — | PASS |
| `short_alarm_h3` | -13.23% | -3.08% | +2.59% | +14.78% | NO | — | PASS |
| `short_news_r_h1` | -20.56% | -7.19% | +0.28% | +32.63% | NO | — | PASS |
| `short_r_down_h1` | +0.00% | +0.00% | +0.00% | +0.00% | YES | — | PASS |
| `short_r_down_h3` | +0.00% | +0.00% | +0.00% | +0.00% | YES | — | PASS |
| `short_extended_h1` | -46.18% | -24.44% | -9.21% | +39.98% | NO | — | PASS |
| `short_extended_h3` | -47.34% | -24.16% | -7.77% | +38.00% | NO | — | PASS |
| `short_last_red_h1` | -36.22% | -19.97% | -9.53% | +30.26% | NO | — | PASS |
| `short_last_red_h3` | -31.75% | -15.38% | -5.29% | +34.91% | NO | — | PASS |
| `flatten_h5_topheavy` | -14.42% | -3.60% | +3.94% | +24.43% | NO | — | PASS |
| `flatten_h5_half` | -14.50% | -4.46% | +1.40% | +14.44% | NO | — | PASS |
| `flatten_h5_cut` | -15.41% | -0.84% | +7.99% | +27.84% | NO | — | PASS |
| `flatten_h5_trail` | -15.41% | -0.84% | +7.99% | +27.84% | NO | — | PASS |
| `flatten_h5_sizeup` | -15.41% | -0.84% | +7.99% | +27.84% | NO | — | PASS |
| `flatten_h3_rankw` | -23.02% | -9.19% | -1.40% | +20.34% | NO | — | PASS |
| `flatten_h3_topheavy` | -20.99% | -7.34% | +0.27% | +21.61% | NO | — | PASS |
| `flatten_h3_half` | -18.41% | -6.78% | +0.02% | +17.38% | NO | — | PASS |
| `flatten_h3_time` | -21.77% | -4.41% | +5.30% | +39.18% | NO | — | PASS |
| `flatten_h3_cut` | -22.13% | -6.59% | +2.07% | +25.97% | NO | — | PASS |
| `flatten_h3_trail` | -22.13% | -6.59% | +2.07% | +25.97% | NO | — | PASS |
| `flatten_h3_sboost` | -21.99% | -6.74% | +2.01% | +25.69% | NO | — | PASS |
| `flatten_h3_sizeup` | -22.13% | -6.59% | +2.07% | +25.97% | NO | — | PASS |
| `flatten_live_h1_rankw` | -16.04% | -3.35% | +4.03% | +17.70% | NO | — | PASS |
| `flatten_live_h1_topheavy` | -15.23% | -1.23% | +7.19% | +20.10% | NO | — | PASS |
| `flatten_live_h1_half` | -7.75% | -0.39% | +3.74% | +11.28% | NO | — | PASS |
| `flatten_live_h1_time` | -14.76% | -0.23% | +8.22% | +24.22% | NO | — | PASS |
| `flatten_live_h1_cut` | -14.76% | -0.23% | +8.22% | +24.22% | NO | — | PASS |
| `flatten_live_h1_trail` | -14.76% | -0.23% | +8.22% | +24.22% | NO | — | PASS |
| `flatten_live_h1_sboost` | -14.76% | -0.23% | +8.22% | +24.22% | NO | — | PASS |
| `union_h5_rankw` | -20.37% | -8.09% | -0.93% | +17.53% | NO | — | PASS |
| `union_h5_topheavy` | -15.77% | -5.02% | +1.73% | +23.36% | NO | — | PASS |
| `union_h5_half` | -14.51% | -4.17% | +1.55% | +18.39% | NO | — | PASS |
| `union_h5_time` | -17.26% | -3.27% | +5.01% | +28.18% | NO | — | PASS |
| `union_h5_cut` | -17.77% | -4.24% | +3.83% | +26.01% | NO | — | PASS |
| `union_h5_trail` | -17.77% | -4.24% | +3.83% | +26.01% | NO | — | PASS |
| `union_h5_sizeup` | -17.77% | -4.24% | +3.83% | +26.01% | NO | — | PASS |
| `union_h3_rankw` | -26.32% | -10.98% | -2.72% | +25.67% | NO | — | PASS |
| `union_h3_topheavy` | -21.50% | -7.53% | +1.12% | +29.23% | NO | — | PASS |
| `union_h3_half` | -21.27% | -9.80% | -2.93% | +18.90% | NO | — | PASS |
| `union_h3_time` | -23.95% | -5.24% | +4.53% | +41.61% | NO | — | PASS |
| `union_h3_trail` | -23.90% | -5.58% | +4.07% | +39.95% | NO | — | PASS |
| `union_h3_sboost` | -23.68% | -7.08% | +2.39% | +37.71% | NO | — | PASS |
| `union_h3_sizeup` | -24.52% | -6.86% | +2.45% | +38.03% | NO | — | PASS |
| `union_h1_rankw` | -37.76% | -13.42% | +3.23% | +56.14% | NO | — | PASS |
| `union_h1_half` | -25.47% | -9.29% | +0.94% | +33.20% | NO | — | PASS |
| `union_h1_time` | -45.66% | -17.24% | +3.94% | +84.64% | NO | — | PASS |
| `union_h1_cut` | -42.41% | -15.01% | +4.54% | +75.73% | NO | — | PASS |
| `union_h1_trail` | -41.81% | -15.01% | +4.63% | +75.73% | NO | — | PASS |
| `union_h1_sboost` | -42.00% | -15.13% | +4.70% | +76.96% | NO | — | PASS |
| `union_h1_sizeup` | -41.32% | -14.70% | +4.63% | +75.73% | NO | — | PASS |
| `union_news_pack_h3` | -30.72% | -16.34% | -7.10% | +10.73% | NO | — | PASS |
| `union_news_head_h1` | -50.68% | -24.42% | -4.70% | +66.35% | NO | — | PASS |
| `union_news_head_h3` | -41.51% | -25.24% | -14.13% | +20.81% | NO | — | PASS |
| `union_news_g_cond_h1` | -45.11% | -17.40% | +2.98% | +71.90% | NO | — | PASS |
| `short_news_pack_h3` | -3.86% | +2.73% | +6.25% | +8.87% | NO | — | PASS |
| `union_news_pack_net3_h1` | -27.20% | +0.27% | +19.12% | +55.59% | NO | — | PASS |
| `union_news_or_net2_h1` | -41.80% | -17.90% | -1.08% | +62.54% | NO | — | PASS |
| `short_news_or_h3` | -9.95% | +5.74% | +15.06% | +46.64% | NO | — | PASS |
| `union_rsi_os_h1` | -47.10% | -25.50% | -10.85% | +74.36% | NO | — | PASS |
| `union_rsi_os_h3` | -42.27% | -23.90% | -11.95% | +10.94% | NO | — | PASS |
| `union_macd_up_h1` | -43.00% | -17.37% | +1.33% | +68.93% | NO | — | PASS |
| `union_macd_up_h3` | -27.38% | -10.70% | -1.16% | +30.33% | NO | — | PASS |
| `union_macd_xup_h1` | -54.93% | -30.33% | -12.05% | +71.04% | NO | — | PASS |
| `union_flow_in_h1` | -42.63% | -14.96% | +5.56% | +93.35% | NO | — | PASS |
| `union_flow_in_h3` | -23.23% | -11.88% | -4.18% | +33.49% | NO | — | PASS |
| `union_rsi_os_macd_h1` | -14.31% | -8.88% | -5.91% | +5.97% | NO | — | PASS |
| `union_flow_in_white_h1` | -20.67% | -7.48% | +0.64% | +21.31% | NO | — | PASS |
| `union_rsi_h1` | -59.21% | -32.59% | -11.77% | +125.57% | NO | — | PASS |
| `union_macd_hist_h1` | -48.16% | -22.45% | -2.78% | +63.57% | NO | — | PASS |
| `short_rsi_ob_h1` | -38.10% | -19.90% | -8.19% | +33.83% | NO | — | PASS |
| `short_rsi_ob_h3` | -35.09% | -15.88% | -3.70% | +34.84% | NO | — | PASS |
| `short_macd_dn_h3` | -36.02% | -19.30% | -8.74% | +27.28% | NO | — | PASS |
| `combo_se_5050_shared` | -30.60% | +3.25% | +27.06% | +97.10% | YES | — | PASS |
| `combo_se_5050_weather` | -30.60% | +3.25% | +27.06% | +97.10% | YES | — | PASS |
| `combo_nse_333_shared` | -35.08% | -0.69% | +24.72% | +105.19% | NO | — | PASS |
| `combo_en_7030_shared` | -27.89% | +0.68% | +20.71% | +83.02% | YES | — | PASS |
| `combo_se_7030_shared` | -26.95% | +3.66% | +24.23% | +81.09% | YES | — | PASS |
| `combo_en_5050_shared` | -31.31% | -0.93% | +19.99% | +84.51% | NO | — | PASS |
| `combo_ner_5050_shared` | -31.05% | -1.57% | +18.65% | +81.68% | NO | — | PASS |
| `combo_se_5050_split` | -18.28% | +3.57% | +17.80% | +62.92% | YES | — | PASS |
| `combo_en_3070_shared` | -34.54% | -3.49% | +18.53% | +88.23% | NO | — | PASS |
| `combo_hn_5050_shared` | -58.18% | -19.71% | +13.47% | +148.09% | NO | — | PASS |
| `combo_seh_502525_split` | -28.98% | -3.96% | +15.57% | +91.75% | NO | — | PASS |
| `combo_hj_5050_shared` | -60.96% | -22.95% | +10.95% | +167.71% | NO | — | PASS |
| `combo_ef_7030_shared` | -23.25% | +0.23% | +14.68% | +56.66% | YES | — | PASS |
| `combo_hn_3070_shared` | -54.69% | -19.04% | +11.42% | +124.45% | NO | — | PASS |
| `combo_eer_5050_shared` | -29.38% | -2.51% | +14.58% | +74.41% | NO | — | PASS |
| `combo_sn_3070_shared` | -50.03% | -14.03% | +14.41% | +120.88% | NO | — | PASS |
| `combo_sf_7030_shared` | -24.23% | +0.01% | +15.28% | +54.63% | NO | — | PASS |
| `combo_sn_7030_shared` | -35.93% | -6.84% | +14.35% | +85.29% | NO | — | PASS |
| `combo_fse_333_shared` | -26.86% | -2.85% | +12.64% | +56.41% | NO | — | PASS |
| `combo_sf_5050_shared` | -23.57% | -1.29% | +12.96% | +50.17% | NO | — | PASS |
| `combo_fes_403030_shared` | -26.56% | -3.67% | +11.07% | +50.58% | NO | — | PASS |
| `combo_ef_5050_shared` | -21.83% | -1.94% | +10.16% | +43.79% | NO | — | PASS |
| `combo_fe_5050_shared` | -21.83% | -1.94% | +10.16% | +43.79% | NO | — | PASS |
| `combo_ne1_5050_shared` | -53.30% | -18.69% | +9.35% | +117.55% | NO | — | PASS |
| `combo_sf_3070_shared` | -19.21% | -1.19% | +11.28% | +41.44% | NO | — | PASS |
| `combo_hf_5050_shared` | -39.22% | -13.19% | +5.29% | +62.32% | NO | — | PASS |
| `combo_fer_5050_shared` | -22.94% | -3.42% | +8.46% | +41.73% | NO | — | PASS |
| `combo_nj_5050_shared` | -50.29% | -19.12% | +4.32% | +118.94% | NO | — | PASS |
| `combo_jf_5050_shared` | -26.32% | -6.94% | +5.78% | +40.35% | NO | — | PASS |
| `combo_ef_3070_shared` | -20.50% | -3.39% | +6.75% | +32.68% | NO | — | PASS |
| `combo_nf_5050_shared` | -26.83% | -6.72% | +6.08% | +40.57% | NO | — | PASS |
| `combo_fh_7030_shared` | -30.43% | -10.74% | +1.84% | +41.22% | NO | — | PASS |
| `combo_fe1_5050_shared` | -35.18% | -12.88% | +2.03% | +49.67% | NO | — | PASS |
| `combo_e1er_5050_shared` | -53.91% | -24.92% | -3.12% | +91.16% | NO | — | PASS |

## Every sleeve × reality

| Strategy | Reality | Book% | Close $ | Max DD vs $10k | Sessions < $10k | Starts YES | Fills | Miss | Partial | Skips | Sess win% |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `combo_sh_5050_shared` | `ideal` | +37.49% | $13,748.76 | +0.00% | 0 | 21/23 | 152 | 4 | 0 | 129 | 65.2% |
| `combo_sh_5050_shared` | `limit_prior` | +34.82% | $13,482.24 | +0.00% | 0 | 17/23 | 126 | 26 | 0 | 136 | 69.6% |
| `combo_sh_5050_shared` | `limit_open` | +17.89% | $11,788.46 | -0.02% | 1 | 21/23 | 138 | 17 | 0 | 137 | 56.5% |
| `combo_sh_5050_shared` | `market_mid` | -9.51% | $9,049.37 | -10.17% | 22 | 0/23 | 152 | 4 | 0 | 129 | 43.5% |
| `combo_sh_5050_shared` | `market_adverse` | -60.28% | $3,971.97 | -60.28% | 23 | 0/23 | 152 | 4 | 0 | 129 | 0.0% |
| `combo_sh_5050_shared` | `market_favorable` | +242.15% | $34,215.44 | +0.00% | 0 | 21/23 | 152 | 4 | 0 | 129 | 91.3% |
| `combo_sh_5050_shared` | `partial_50` | -2.38% | $9,761.95 | -2.38% | 16 | 0/23 | 451 | 24 | 401 | 131 | 47.8% |
| `combo_sh_5050_shared` | `gap_miss` | +38.42% | $13,841.59 | +0.00% | 0 | 21/23 | 148 | 6 | 0 | 129 | 69.6% |
| `combo_sh_3070_shared` | `ideal` | +33.02% | $13,301.60 | -0.02% | 1 | 21/23 | 152 | 4 | 0 | 129 | 60.9% |
| `combo_sh_3070_shared` | `limit_prior` | +36.48% | $13,647.57 | +0.00% | 0 | 21/23 | 126 | 26 | 0 | 136 | 69.6% |
| `combo_sh_3070_shared` | `limit_open` | +14.30% | $11,429.91 | -1.87% | 3 | 21/23 | 138 | 17 | 0 | 137 | 56.5% |
| `combo_sh_3070_shared` | `market_mid` | -15.30% | $8,470.06 | -15.30% | 22 | 0/23 | 152 | 4 | 0 | 129 | 43.5% |
| `combo_sh_3070_shared` | `market_adverse` | -63.67% | $3,632.71 | -63.67% | 23 | 0/23 | 152 | 4 | 0 | 129 | 0.0% |
| `combo_sh_3070_shared` | `market_favorable` | +268.79% | $36,878.58 | +0.00% | 0 | 21/23 | 152 | 4 | 0 | 129 | 91.3% |
| `combo_sh_3070_shared` | `partial_50` | -4.09% | $9,590.79 | -4.09% | 17 | 0/23 | 458 | 24 | 408 | 131 | 43.5% |
| `combo_sh_3070_shared` | `gap_miss` | +34.44% | $13,444.39 | +0.00% | 0 | 21/23 | 148 | 6 | 0 | 129 | 60.9% |
| `combo_sh_7030_shared` | `ideal` | +31.91% | $13,190.93 | +0.00% | 0 | 17/23 | 152 | 4 | 0 | 129 | 69.6% |
| `combo_sh_7030_shared` | `limit_prior` | +28.88% | $12,887.74 | +0.00% | 0 | 17/23 | 126 | 26 | 0 | 136 | 73.9% |
| `combo_sh_7030_shared` | `limit_open` | +18.29% | $11,828.93 | +0.00% | 0 | 17/23 | 138 | 17 | 0 | 137 | 56.5% |
| `combo_sh_7030_shared` | `market_mid` | -4.13% | $9,586.84 | -6.32% | 22 | 2/23 | 152 | 4 | 0 | 129 | 52.2% |
| `combo_sh_7030_shared` | `market_adverse` | -48.57% | $5,142.83 | -48.57% | 23 | 0/23 | 152 | 4 | 0 | 129 | 8.7% |
| `combo_sh_7030_shared` | `market_favorable` | +167.66% | $26,766.42 | +0.00% | 0 | 21/23 | 152 | 4 | 0 | 129 | 91.3% |
| `combo_sh_7030_shared` | `partial_50` | -0.18% | $9,982.06 | -0.96% | 8 | 1/23 | 444 | 22 | 392 | 129 | 56.5% |
| `combo_sh_7030_shared` | `gap_miss` | +32.16% | $13,215.66 | +0.00% | 0 | 17/23 | 148 | 6 | 0 | 129 | 73.9% |
| `combo_ps_5050_shared` | `ideal` | +27.02% | $12,701.62 | +0.00% | 0 | 21/23 | 131 | 2 | 0 | 114 | 69.6% |
| `combo_ps_5050_shared` | `limit_prior` | +14.57% | $11,457.17 | +0.00% | 0 | 21/23 | 102 | 24 | 0 | 120 | 69.6% |
| `combo_ps_5050_shared` | `limit_open` | +20.26% | $12,026.16 | +0.00% | 0 | 21/23 | 123 | 10 | 0 | 117 | 69.6% |
| `combo_ps_5050_shared` | `market_mid` | +2.20% | $10,219.58 | -3.28% | 8 | 13/23 | 131 | 2 | 0 | 114 | 47.8% |
| `combo_ps_5050_shared` | `market_adverse` | -32.37% | $6,762.70 | -32.37% | 22 | 0/23 | 131 | 2 | 0 | 114 | 17.4% |
| `combo_ps_5050_shared` | `market_favorable` | +98.23% | $19,822.76 | +0.00% | 0 | 21/23 | 133 | 2 | 0 | 113 | 87.0% |
| `combo_ps_5050_shared` | `partial_50` | -2.31% | $9,768.89 | -3.73% | 17 | 7/23 | 325 | 14 | 270 | 119 | 30.4% |
| `combo_ps_5050_shared` | `gap_miss` | +26.36% | $12,635.93 | +0.00% | 0 | 21/23 | 129 | 3 | 0 | 113 | 69.6% |
| `combo_p2s_5050_shared` | `ideal` | +27.79% | $12,778.91 | +0.00% | 0 | 21/23 | 127 | 2 | 0 | 112 | 69.6% |
| `combo_p2s_5050_shared` | `limit_prior` | +15.28% | $11,527.71 | +0.00% | 0 | 21/23 | 96 | 24 | 0 | 119 | 65.2% |
| `combo_p2s_5050_shared` | `limit_open` | +21.11% | $12,110.60 | +0.00% | 0 | 21/23 | 119 | 10 | 0 | 115 | 69.6% |
| `combo_p2s_5050_shared` | `market_mid` | +2.09% | $10,209.02 | -5.43% | 8 | 13/23 | 127 | 2 | 0 | 112 | 47.8% |
| `combo_p2s_5050_shared` | `market_adverse` | -32.43% | $6,757.26 | -32.43% | 22 | 0/23 | 125 | 2 | 0 | 113 | 17.4% |
| `combo_p2s_5050_shared` | `market_favorable` | +102.49% | $20,249.28 | +0.00% | 0 | 21/23 | 127 | 2 | 0 | 112 | 87.0% |
| `combo_p2s_5050_shared` | `partial_50` | -1.63% | $9,837.07 | -4.03% | 17 | 11/23 | 317 | 12 | 264 | 116 | 34.8% |
| `combo_p2s_5050_shared` | `gap_miss` | +27.15% | $12,715.09 | +0.00% | 0 | 21/23 | 125 | 3 | 0 | 111 | 69.6% |
| `combo_seh_451540_shared` | `ideal` | +30.82% | $13,081.74 | +0.00% | 0 | 21/23 | 258 | 4 | 0 | 299 | 69.6% |
| `combo_seh_451540_shared` | `limit_prior` | +30.98% | $13,097.54 | +0.00% | 0 | 17/23 | 211 | 41 | 0 | 296 | 69.6% |
| `combo_seh_451540_shared` | `limit_open` | +15.59% | $11,559.18 | +0.00% | 0 | 21/23 | 238 | 21 | 0 | 305 | 65.2% |
| `combo_seh_451540_shared` | `market_mid` | -8.57% | $9,143.07 | -8.57% | 17 | 0/23 | 255 | 4 | 0 | 297 | 34.8% |
| `combo_seh_451540_shared` | `market_adverse` | -54.37% | $4,563.04 | -54.37% | 23 | 0/23 | 250 | 4 | 0 | 294 | 0.0% |
| `combo_seh_451540_shared` | `market_favorable` | +185.24% | $28,524.14 | +0.00% | 0 | 21/23 | 266 | 4 | 0 | 303 | 91.3% |
| `combo_seh_451540_shared` | `partial_50` | -3.58% | $9,642.13 | -3.58% | 12 | 0/23 | 647 | 32 | 547 | 306 | 47.8% |
| `combo_seh_451540_shared` | `gap_miss` | +32.56% | $13,256.49 | +0.00% | 0 | 21/23 | 246 | 11 | 0 | 295 | 69.6% |
| `union_hot_n4_h1` | `ideal` | +20.01% | $12,001.18 | -4.33% | 4 | 15/23 | 84 | 4 | 0 | 42 | 39.1% |
| `union_hot_n4_h1` | `limit_prior` | +24.63% | $12,463.17 | +0.00% | 0 | 19/23 | 73 | 15 | 0 | 54 | 52.2% |
| `union_hot_n4_h1` | `limit_open` | +9.47% | $10,947.09 | -4.58% | 5 | 12/23 | 76 | 11 | 0 | 49 | 34.8% |
| `union_hot_n4_h1` | `market_mid` | -21.91% | $7,808.84 | -21.91% | 22 | 0/23 | 84 | 4 | 0 | 42 | 26.1% |
| `union_hot_n4_h1` | `market_adverse` | -63.70% | $3,630.06 | -63.70% | 23 | 0/23 | 84 | 4 | 0 | 42 | 13.0% |
| `union_hot_n4_h1` | `market_favorable` | +207.18% | $30,718.02 | +0.00% | 0 | 21/23 | 84 | 4 | 0 | 42 | 73.9% |
| `union_hot_n4_h1` | `partial_50` | -8.89% | $9,111.07 | -8.89% | 20 | 0/23 | 258 | 14 | 233 | 39 | 34.8% |
| `union_hot_n4_h1` | `gap_miss` | +22.06% | $12,205.68 | -2.68% | 3 | 15/23 | 82 | 5 | 0 | 43 | 39.1% |
| `union_news_pack_net2_h1` | `ideal` | +24.02% | $12,402.46 | +0.00% | 0 | 21/23 | 55 | 2 | 0 | 21 | 43.5% |
| `union_news_pack_net2_h1` | `limit_prior` | +12.59% | $11,258.77 | +0.00% | 0 | 21/23 | 41 | 13 | 0 | 32 | 47.8% |
| `union_news_pack_net2_h1` | `limit_open` | +17.96% | $11,795.85 | +0.00% | 0 | 21/23 | 53 | 4 | 0 | 23 | 43.5% |
| `union_news_pack_net2_h1` | `market_mid` | +2.11% | $10,210.88 | -4.93% | 6 | 18/23 | 55 | 2 | 0 | 21 | 34.8% |
| `union_news_pack_net2_h1` | `market_adverse` | -28.08% | $7,191.90 | -28.08% | 22 | 0/23 | 53 | 2 | 0 | 22 | 4.3% |
| `union_news_pack_net2_h1` | `market_favorable` | +62.42% | $16,242.54 | +0.00% | 0 | 21/23 | 55 | 2 | 0 | 21 | 65.2% |
| `union_news_pack_net2_h1` | `partial_50` | -3.24% | $9,675.83 | -6.64% | 20 | 10/23 | 116 | 4 | 90 | 23 | 30.4% |
| `union_news_pack_net2_h1` | `gap_miss` | +24.02% | $12,402.46 | +0.00% | 0 | 21/23 | 55 | 2 | 0 | 21 | 43.5% |
| `short_news_r_h3` | `ideal` | +15.06% | $11,506.12 | +0.00% | 0 | 17/23 | 72 | 0 | 0 | 91 | 56.5% |
| `short_news_r_h3` | `limit_prior` | +11.09% | $11,108.60 | -0.51% | 3 | 17/23 | 57 | 11 | 0 | 86 | 52.2% |
| `short_news_r_h3` | `limit_open` | +10.79% | $11,078.87 | -0.52% | 4 | 17/23 | 66 | 6 | 0 | 92 | 56.5% |
| `short_news_r_h3` | `market_mid` | +5.74% | $10,574.14 | -1.69% | 5 | 16/23 | 72 | 0 | 0 | 91 | 47.8% |
| `short_news_r_h3` | `market_adverse` | -9.95% | $9,004.84 | -10.13% | 20 | 0/23 | 72 | 0 | 0 | 91 | 30.4% |
| `short_news_r_h3` | `market_favorable` | +46.64% | $14,664.19 | +0.00% | 0 | 21/23 | 72 | 0 | 0 | 91 | 82.6% |
| `short_news_r_h3` | `partial_50` | +2.96% | $10,296.12 | -0.63% | 5 | 16/23 | 207 | 8 | 179 | 94 | 47.8% |
| `short_news_r_h3` | `gap_miss` | +14.65% | $11,464.79 | +0.00% | 0 | 17/23 | 70 | 1 | 0 | 90 | 56.5% |
| `flatten_h3` | `ideal` | +2.07% | $10,207.09 | +0.00% | 0 | 3/23 | 98 | 0 | 0 | 167 | 43.5% |
| `flatten_h3` | `limit_prior` | -1.46% | $9,854.10 | -1.46% | 7 | 1/23 | 98 | 26 | 0 | 185 | 39.1% |
| `flatten_h3` | `limit_open` | -4.61% | $9,539.41 | -4.61% | 6 | 0/23 | 105 | 8 | 0 | 184 | 43.5% |
| `flatten_h3` | `market_mid` | -6.59% | $9,340.51 | -6.59% | 10 | 0/23 | 82 | 0 | 0 | 159 | 34.8% |
| `flatten_h3` | `market_adverse` | -22.13% | $7,786.96 | -22.13% | 23 | 0/23 | 80 | 0 | 0 | 160 | 26.1% |
| `flatten_h3` | `market_favorable` | +25.97% | $12,597.05 | +0.00% | 0 | 20/23 | 102 | 0 | 0 | 169 | 69.6% |
| `flatten_h3` | `partial_50` | -7.28% | $9,272.18 | -7.28% | 12 | 0/23 | 361 | 23 | 310 | 203 | 34.8% |
| `flatten_h3` | `gap_miss` | +2.04% | $10,203.47 | +0.00% | 0 | 2/23 | 94 | 1 | 0 | 165 | 43.5% |
| `combo_jse_333_shared` | `ideal` | +31.50% | $13,150.34 | +0.00% | 0 | 15/23 | 246 | 16 | 0 | 304 | 56.5% |
| `combo_jse_333_shared` | `limit_prior` | +15.86% | $11,586.15 | +0.00% | 0 | 17/23 | 263 | 58 | 0 | 321 | 60.9% |
| `combo_jse_333_shared` | `limit_open` | +28.50% | $12,850.10 | +0.00% | 0 | 15/23 | 235 | 28 | 0 | 309 | 52.2% |
| `combo_jse_333_shared` | `market_mid` | +4.58% | $10,457.96 | +0.00% | 0 | 1/23 | 244 | 16 | 0 | 305 | 34.8% |
| `combo_jse_333_shared` | `market_adverse` | -34.57% | $6,542.77 | -34.57% | 18 | 0/23 | 230 | 16 | 0 | 300 | 17.4% |
| `combo_jse_333_shared` | `market_favorable` | +135.32% | $23,532.19 | +0.00% | 0 | 21/23 | 256 | 16 | 0 | 303 | 73.9% |
| `combo_jse_333_shared` | `partial_50` | +2.30% | $10,229.94 | +0.00% | 0 | 1/23 | 825 | 60 | 703 | 355 | 47.8% |
| `combo_jse_333_shared` | `gap_miss` | +28.21% | $12,821.03 | +0.00% | 0 | 15/23 | 238 | 20 | 0 | 302 | 56.5% |
| `combo_ej_5050_shared` | `ideal` | +27.32% | $12,732.33 | +0.00% | 0 | 8/23 | 214 | 16 | 0 | 246 | 43.5% |
| `combo_ej_5050_shared` | `limit_prior` | +6.78% | $10,677.59 | +0.00% | 0 | 13/23 | 210 | 51 | 0 | 247 | 43.5% |
| `combo_ej_5050_shared` | `limit_open` | +26.25% | $12,625.23 | +0.00% | 0 | 6/23 | 203 | 25 | 0 | 250 | 47.8% |
| `combo_ej_5050_shared` | `market_mid` | +3.26% | $10,325.99 | +0.00% | 0 | 1/23 | 210 | 16 | 0 | 246 | 26.1% |
| `combo_ej_5050_shared` | `market_adverse` | -29.92% | $7,008.17 | -29.92% | 18 | 0/23 | 206 | 16 | 0 | 246 | 8.7% |
| `combo_ej_5050_shared` | `market_favorable` | +116.02% | $21,602.36 | +0.00% | 0 | 21/23 | 226 | 16 | 0 | 242 | 78.3% |
| `combo_ej_5050_shared` | `partial_50` | -1.69% | $9,830.63 | -1.69% | 4 | 0/23 | 681 | 54 | 580 | 277 | 34.8% |
| `combo_ej_5050_shared` | `gap_miss` | +24.80% | $12,480.39 | +0.00% | 0 | 6/23 | 206 | 22 | 0 | 241 | 43.5% |
| `combo_es_9010_shared` | `ideal` | +24.79% | $12,478.68 | +0.00% | 0 | 17/23 | 176 | 0 | 0 | 258 | 52.2% |
| `combo_es_9010_shared` | `limit_prior` | +6.88% | $10,687.68 | +0.00% | 0 | 20/23 | 154 | 28 | 0 | 253 | 43.5% |
| `combo_es_9010_shared` | `limit_open` | +22.81% | $12,281.05 | +0.00% | 0 | 11/23 | 169 | 9 | 0 | 260 | 47.8% |
| `combo_es_9010_shared` | `market_mid` | +4.43% | $10,443.54 | +0.00% | 0 | 1/23 | 164 | 0 | 0 | 252 | 43.5% |
| `combo_es_9010_shared` | `market_adverse` | -25.49% | $7,451.28 | -25.49% | 17 | 0/23 | 158 | 0 | 0 | 249 | 34.8% |
| `combo_es_9010_shared` | `market_favorable` | +89.37% | $18,937.10 | +0.00% | 0 | 21/23 | 180 | 0 | 0 | 260 | 78.3% |
| `combo_es_9010_shared` | `partial_50` | +0.37% | $10,036.88 | +0.00% | 0 | 1/23 | 555 | 36 | 483 | 298 | 52.2% |
| `combo_es_9010_shared` | `gap_miss` | +24.73% | $12,473.04 | +0.00% | 0 | 16/23 | 168 | 5 | 0 | 254 | 47.8% |
| `combo_se_3070_shared` | `ideal` | +26.38% | $12,637.52 | +0.00% | 0 | 16/23 | 182 | 0 | 0 | 261 | 47.8% |
| `combo_se_3070_shared` | `limit_prior` | +10.03% | $11,002.46 | +0.00% | 0 | 21/23 | 156 | 28 | 0 | 254 | 56.5% |
| `combo_se_3070_shared` | `limit_open` | +23.64% | $12,363.87 | +0.00% | 0 | 13/23 | 173 | 9 | 0 | 262 | 52.2% |
| `combo_se_3070_shared` | `market_mid` | +4.83% | $10,482.99 | +0.00% | 0 | 1/23 | 180 | 0 | 0 | 260 | 39.1% |
| `combo_se_3070_shared` | `market_adverse` | -27.47% | $7,253.17 | -27.47% | 17 | 0/23 | 170 | 0 | 0 | 255 | 26.1% |
| `combo_se_3070_shared` | `market_favorable` | +93.58% | $19,357.83 | +0.00% | 0 | 21/23 | 182 | 0 | 0 | 261 | 82.6% |
| `combo_se_3070_shared` | `partial_50` | +1.49% | $10,148.77 | +0.00% | 0 | 1/23 | 544 | 34 | 469 | 296 | 52.2% |
| `combo_se_3070_shared` | `gap_miss` | +24.56% | $12,456.18 | +0.00% | 0 | 13/23 | 172 | 5 | 0 | 256 | 52.2% |
| `union_e_fresh_h3` | `ideal` | +22.95% | $12,295.24 | +0.00% | 0 | 16/23 | 96 | 0 | 0 | 168 | 52.2% |
| `union_e_fresh_h3` | `limit_prior` | +3.02% | $10,302.30 | +0.00% | 0 | 15/23 | 105 | 20 | 0 | 177 | 39.1% |
| `union_e_fresh_h3` | `limit_open` | +21.86% | $12,185.65 | +0.00% | 0 | 12/23 | 93 | 4 | 0 | 168 | 47.8% |
| `union_e_fresh_h3` | `market_mid` | +3.88% | $10,387.58 | +0.00% | 0 | 1/23 | 86 | 0 | 0 | 163 | 43.5% |
| `union_e_fresh_h3` | `market_adverse` | -23.89% | $7,611.48 | -23.89% | 17 | 0/23 | 78 | 0 | 0 | 159 | 34.8% |
| `union_e_fresh_h3` | `market_favorable` | +83.65% | $18,365.44 | +0.00% | 0 | 21/23 | 114 | 0 | 0 | 177 | 78.3% |
| `union_e_fresh_h3` | `partial_50` | +0.04% | $10,004.11 | +0.00% | 0 | 1/23 | 377 | 28 | 326 | 216 | 39.1% |
| `union_e_fresh_h3` | `gap_miss` | +20.57% | $12,056.62 | +0.00% | 0 | 10/23 | 98 | 4 | 0 | 169 | 47.8% |
| `combo_seh_333_skip` | `ideal` | +28.42% | $12,841.98 | +0.00% | 0 | 21/23 | 256 | 4 | 0 | 308 | 65.2% |
| `combo_seh_333_skip` | `limit_prior` | +27.18% | $12,717.93 | +0.00% | 0 | 21/23 | 215 | 39 | 0 | 309 | 78.3% |
| `combo_seh_333_skip` | `limit_open` | +18.62% | $11,861.95 | +0.00% | 0 | 20/23 | 237 | 22 | 0 | 316 | 60.9% |
| `combo_seh_333_skip` | `market_mid` | -4.43% | $9,557.01 | -4.43% | 8 | 0/23 | 254 | 4 | 0 | 307 | 34.8% |
| `combo_seh_333_skip` | `market_adverse` | -45.63% | $5,437.40 | -45.63% | 23 | 0/23 | 249 | 4 | 0 | 304 | 8.7% |
| `combo_seh_333_skip` | `market_favorable` | +147.75% | $24,775.20 | +0.00% | 0 | 21/23 | 256 | 4 | 0 | 308 | 87.0% |
| `combo_seh_333_skip` | `partial_50` | -1.55% | $9,844.58 | -1.55% | 4 | 0/23 | 686 | 40 | 588 | 329 | 39.1% |
| `combo_seh_333_skip` | `gap_miss` | +28.85% | $12,885.31 | +0.00% | 0 | 21/23 | 246 | 10 | 0 | 305 | 60.9% |
| `combo_seh_502525_shared` | `ideal` | +28.89% | $12,888.60 | +0.00% | 0 | 17/23 | 264 | 4 | 0 | 302 | 60.9% |
| `combo_seh_502525_shared` | `limit_prior` | +25.72% | $12,572.02 | +0.00% | 0 | 17/23 | 219 | 42 | 0 | 301 | 73.9% |
| `combo_seh_502525_shared` | `limit_open` | +17.02% | $11,701.78 | +0.00% | 0 | 17/23 | 247 | 22 | 0 | 311 | 60.9% |
| `combo_seh_502525_shared` | `market_mid` | -4.18% | $9,582.10 | -4.18% | 11 | 0/23 | 260 | 4 | 0 | 300 | 34.8% |
| `combo_seh_502525_shared` | `market_adverse` | -45.75% | $5,425.03 | -45.75% | 23 | 0/23 | 255 | 4 | 0 | 297 | 4.3% |
| `combo_seh_502525_shared` | `market_favorable` | +143.90% | $24,389.78 | +0.00% | 0 | 21/23 | 268 | 4 | 0 | 304 | 91.3% |
| `combo_seh_502525_shared` | `partial_50` | -1.58% | $9,841.59 | -1.58% | 7 | 0/23 | 682 | 38 | 582 | 315 | 47.8% |
| `combo_seh_502525_shared` | `gap_miss` | +29.26% | $12,925.78 | +0.00% | 0 | 17/23 | 252 | 11 | 0 | 298 | 56.5% |
| `combo_seh_333_shared` | `ideal` | +25.59% | $12,558.99 | +0.00% | 0 | 21/23 | 266 | 4 | 0 | 303 | 60.9% |
| `combo_seh_333_shared` | `limit_prior` | +24.40% | $12,439.76 | +0.00% | 0 | 21/23 | 219 | 42 | 0 | 301 | 73.9% |
| `combo_seh_333_shared` | `limit_open` | +15.59% | $11,559.03 | +0.00% | 0 | 20/23 | 247 | 22 | 0 | 311 | 60.9% |
| `combo_seh_333_shared` | `market_mid` | -6.70% | $9,330.26 | -6.70% | 12 | 0/23 | 264 | 4 | 0 | 302 | 34.8% |
| `combo_seh_333_shared` | `market_adverse` | -46.54% | $5,345.69 | -46.54% | 23 | 0/23 | 259 | 4 | 0 | 299 | 4.3% |
| `combo_seh_333_shared` | `market_favorable` | +140.66% | $24,065.77 | +0.00% | 0 | 21/23 | 268 | 4 | 0 | 304 | 87.0% |
| `combo_seh_333_shared` | `partial_50` | -3.08% | $9,692.44 | -3.08% | 9 | 0/23 | 708 | 38 | 605 | 318 | 43.5% |
| `combo_seh_333_shared` | `gap_miss` | +25.94% | $12,593.58 | +0.00% | 0 | 21/23 | 254 | 11 | 0 | 299 | 56.5% |
| `combo_seh_333_weather` | `ideal` | +25.59% | $12,558.99 | +0.00% | 0 | 21/23 | 266 | 4 | 0 | 303 | 60.9% |
| `combo_seh_333_weather` | `limit_prior` | +24.40% | $12,439.76 | +0.00% | 0 | 21/23 | 219 | 42 | 0 | 301 | 73.9% |
| `combo_seh_333_weather` | `limit_open` | +15.59% | $11,559.03 | +0.00% | 0 | 20/23 | 247 | 22 | 0 | 311 | 60.9% |
| `combo_seh_333_weather` | `market_mid` | -6.70% | $9,330.26 | -6.70% | 12 | 0/23 | 264 | 4 | 0 | 302 | 34.8% |
| `combo_seh_333_weather` | `market_adverse` | -46.54% | $5,345.69 | -46.54% | 23 | 0/23 | 259 | 4 | 0 | 299 | 4.3% |
| `combo_seh_333_weather` | `market_favorable` | +140.66% | $24,065.77 | +0.00% | 0 | 21/23 | 268 | 4 | 0 | 304 | 87.0% |
| `combo_seh_333_weather` | `partial_50` | -3.08% | $9,692.44 | -3.08% | 9 | 0/23 | 708 | 38 | 605 | 318 | 43.5% |
| `combo_seh_333_weather` | `gap_miss` | +25.94% | $12,593.58 | +0.00% | 0 | 21/23 | 254 | 11 | 0 | 299 | 56.5% |
| `combo_seh_403525_shared` | `ideal` | +26.82% | $12,681.61 | +0.00% | 0 | 20/23 | 264 | 4 | 0 | 302 | 60.9% |
| `combo_seh_403525_shared` | `limit_prior` | +24.05% | $12,404.79 | +0.00% | 0 | 17/23 | 219 | 42 | 0 | 301 | 78.3% |
| `combo_seh_403525_shared` | `limit_open` | +17.09% | $11,709.31 | +0.00% | 0 | 16/23 | 249 | 22 | 0 | 312 | 52.2% |
| `combo_seh_403525_shared` | `market_mid` | -3.76% | $9,623.78 | -3.76% | 8 | 0/23 | 264 | 4 | 0 | 302 | 34.8% |
| `combo_seh_403525_shared` | `market_adverse` | -43.55% | $5,645.12 | -43.55% | 23 | 0/23 | 260 | 4 | 0 | 300 | 8.7% |
| `combo_seh_403525_shared` | `market_favorable` | +135.06% | $23,506.42 | +0.00% | 0 | 21/23 | 268 | 4 | 0 | 304 | 87.0% |
| `combo_seh_403525_shared` | `partial_50` | -1.88% | $9,811.86 | -1.88% | 7 | 0/23 | 704 | 40 | 602 | 320 | 43.5% |
| `combo_seh_403525_shared` | `gap_miss` | +27.13% | $12,713.12 | +0.00% | 0 | 21/23 | 252 | 11 | 0 | 298 | 60.9% |
| `combo_seh_404020_shared` | `ideal` | +26.68% | $12,668.27 | +0.00% | 0 | 16/23 | 266 | 4 | 0 | 303 | 60.9% |
| `combo_seh_404020_shared` | `limit_prior` | +22.25% | $12,224.77 | +0.00% | 0 | 17/23 | 219 | 42 | 0 | 301 | 69.6% |
| `combo_seh_404020_shared` | `limit_open` | +18.37% | $11,836.69 | +0.00% | 0 | 16/23 | 249 | 22 | 0 | 312 | 52.2% |
| `combo_seh_404020_shared` | `market_mid` | -2.32% | $9,767.98 | -2.32% | 4 | 0/23 | 266 | 4 | 0 | 303 | 34.8% |
| `combo_seh_404020_shared` | `market_adverse` | -40.35% | $5,964.83 | -40.35% | 21 | 0/23 | 262 | 4 | 0 | 301 | 8.7% |
| `combo_seh_404020_shared` | `market_favorable` | +124.86% | $22,486.52 | +0.00% | 0 | 21/23 | 268 | 4 | 0 | 304 | 87.0% |
| `combo_seh_404020_shared` | `partial_50` | -1.80% | $9,820.44 | -1.80% | 7 | 0/23 | 711 | 40 | 609 | 321 | 43.5% |
| `combo_seh_404020_shared` | `gap_miss` | +26.16% | $12,615.89 | +0.00% | 0 | 17/23 | 254 | 11 | 0 | 299 | 60.9% |
| `combo_es_8020_shared` | `ideal` | +26.12% | $12,612.03 | +0.00% | 0 | 14/23 | 182 | 0 | 0 | 261 | 52.2% |
| `combo_es_8020_shared` | `limit_prior` | +8.52% | $10,852.42 | +0.00% | 0 | 21/23 | 154 | 28 | 0 | 253 | 43.5% |
| `combo_es_8020_shared` | `limit_open` | +23.72% | $12,372.21 | +0.00% | 0 | 13/23 | 175 | 9 | 0 | 263 | 43.5% |
| `combo_es_8020_shared` | `market_mid` | +4.49% | $10,449.02 | +0.00% | 0 | 1/23 | 178 | 0 | 0 | 259 | 39.1% |
| `combo_es_8020_shared` | `market_adverse` | -25.83% | $7,417.38 | -25.83% | 17 | 0/23 | 170 | 0 | 0 | 255 | 30.4% |
| `combo_es_8020_shared` | `market_favorable` | +90.94% | $19,093.87 | +0.00% | 0 | 21/23 | 182 | 0 | 0 | 261 | 82.6% |
| `combo_es_8020_shared` | `partial_50` | +0.74% | $10,073.90 | +0.00% | 0 | 1/23 | 553 | 34 | 478 | 296 | 52.2% |
| `combo_es_8020_shared` | `gap_miss` | +24.46% | $12,445.77 | +0.00% | 0 | 13/23 | 172 | 5 | 0 | 256 | 52.2% |
| `combo_ps_7030_shared` | `ideal` | +27.34% | $12,734.10 | +0.00% | 0 | 21/23 | 133 | 2 | 0 | 113 | 69.6% |
| `combo_ps_7030_shared` | `limit_prior` | +16.23% | $11,623.45 | +0.00% | 0 | 21/23 | 102 | 24 | 0 | 120 | 65.2% |
| `combo_ps_7030_shared` | `limit_open` | +21.92% | $12,191.60 | +0.00% | 0 | 21/23 | 125 | 10 | 0 | 116 | 69.6% |
| `combo_ps_7030_shared` | `market_mid` | +1.60% | $10,160.54 | -4.12% | 7 | 16/23 | 133 | 2 | 0 | 113 | 65.2% |
| `combo_ps_7030_shared` | `market_adverse` | -34.02% | $6,598.40 | -34.02% | 22 | 0/23 | 131 | 2 | 0 | 114 | 13.0% |
| `combo_ps_7030_shared` | `market_favorable` | +100.45% | $20,044.88 | +0.00% | 0 | 21/23 | 133 | 2 | 0 | 113 | 82.6% |
| `combo_ps_7030_shared` | `partial_50` | -3.40% | $9,659.58 | -4.91% | 18 | 12/23 | 334 | 14 | 279 | 119 | 30.4% |
| `combo_ps_7030_shared` | `gap_miss` | +26.90% | $12,689.67 | +0.00% | 0 | 21/23 | 131 | 3 | 0 | 112 | 65.2% |
| `combo_ee1_3070_shared` | `ideal` | +22.95% | $12,295.24 | +0.00% | 0 | 16/23 | 96 | 0 | 0 | 215 | 52.2% |
| `combo_ee1_3070_shared` | `limit_prior` | +3.02% | $10,302.30 | +0.00% | 0 | 15/23 | 105 | 20 | 0 | 224 | 39.1% |
| `combo_ee1_3070_shared` | `limit_open` | +21.86% | $12,185.65 | +0.00% | 0 | 12/23 | 93 | 4 | 0 | 215 | 47.8% |
| `combo_ee1_3070_shared` | `market_mid` | +3.88% | $10,387.58 | +0.00% | 0 | 1/23 | 86 | 0 | 0 | 210 | 43.5% |
| `combo_ee1_3070_shared` | `market_adverse` | -23.89% | $7,611.48 | -23.89% | 17 | 0/23 | 78 | 0 | 0 | 206 | 34.8% |
| `combo_ee1_3070_shared` | `market_favorable` | +83.65% | $18,365.44 | +0.00% | 0 | 21/23 | 114 | 0 | 0 | 224 | 78.3% |
| `combo_ee1_3070_shared` | `partial_50` | +0.04% | $10,004.11 | +0.00% | 0 | 1/23 | 377 | 28 | 326 | 263 | 39.1% |
| `combo_ee1_3070_shared` | `gap_miss` | +20.57% | $12,056.62 | +0.00% | 0 | 10/23 | 98 | 4 | 0 | 216 | 47.8% |
| `combo_ee1_5050_shared` | `ideal` | +22.95% | $12,295.24 | +0.00% | 0 | 16/23 | 96 | 0 | 0 | 215 | 52.2% |
| `combo_ee1_5050_shared` | `limit_prior` | +3.02% | $10,302.30 | +0.00% | 0 | 15/23 | 105 | 20 | 0 | 224 | 39.1% |
| `combo_ee1_5050_shared` | `limit_open` | +21.86% | $12,185.65 | +0.00% | 0 | 12/23 | 93 | 4 | 0 | 215 | 47.8% |
| `combo_ee1_5050_shared` | `market_mid` | +3.88% | $10,387.58 | +0.00% | 0 | 1/23 | 86 | 0 | 0 | 210 | 43.5% |
| `combo_ee1_5050_shared` | `market_adverse` | -23.89% | $7,611.48 | -23.89% | 17 | 0/23 | 78 | 0 | 0 | 206 | 34.8% |
| `combo_ee1_5050_shared` | `market_favorable` | +83.65% | $18,365.44 | +0.00% | 0 | 21/23 | 114 | 0 | 0 | 224 | 78.3% |
| `combo_ee1_5050_shared` | `partial_50` | +0.04% | $10,004.11 | +0.00% | 0 | 1/23 | 377 | 28 | 326 | 263 | 39.1% |
| `combo_ee1_5050_shared` | `gap_miss` | +20.57% | $12,056.62 | +0.00% | 0 | 10/23 | 98 | 4 | 0 | 216 | 47.8% |
| `combo_ee1_7030_shared` | `ideal` | +22.95% | $12,295.24 | +0.00% | 0 | 16/23 | 96 | 0 | 0 | 215 | 52.2% |
| `combo_ee1_7030_shared` | `limit_prior` | +3.02% | $10,302.30 | +0.00% | 0 | 15/23 | 105 | 20 | 0 | 224 | 39.1% |
| `combo_ee1_7030_shared` | `limit_open` | +21.86% | $12,185.65 | +0.00% | 0 | 12/23 | 93 | 4 | 0 | 215 | 47.8% |
| `combo_ee1_7030_shared` | `market_mid` | +3.88% | $10,387.58 | +0.00% | 0 | 1/23 | 86 | 0 | 0 | 210 | 43.5% |
| `combo_ee1_7030_shared` | `market_adverse` | -23.89% | $7,611.48 | -23.89% | 17 | 0/23 | 78 | 0 | 0 | 206 | 34.8% |
| `combo_ee1_7030_shared` | `market_favorable` | +83.65% | $18,365.44 | +0.00% | 0 | 21/23 | 114 | 0 | 0 | 224 | 78.3% |
| `combo_ee1_7030_shared` | `partial_50` | +0.04% | $10,004.11 | +0.00% | 0 | 1/23 | 377 | 28 | 326 | 263 | 39.1% |
| `combo_ee1_7030_shared` | `gap_miss` | +20.57% | $12,056.62 | +0.00% | 0 | 10/23 | 98 | 4 | 0 | 216 | 47.8% |
| `combo_ehs_601525_shared` | `ideal` | +23.96% | $12,396.26 | +0.00% | 0 | 20/23 | 268 | 4 | 0 | 304 | 52.2% |
| `combo_ehs_601525_shared` | `limit_prior` | +15.63% | $11,563.33 | +0.00% | 0 | 21/23 | 219 | 42 | 0 | 301 | 60.9% |
| `combo_ehs_601525_shared` | `limit_open` | +19.69% | $11,969.38 | +0.00% | 0 | 13/23 | 249 | 22 | 0 | 312 | 52.2% |
| `combo_ehs_601525_shared` | `market_mid` | -0.21% | $9,978.72 | -0.21% | 3 | 0/23 | 266 | 4 | 0 | 303 | 30.4% |
| `combo_ehs_601525_shared` | `market_adverse` | -33.65% | $6,635.03 | -33.65% | 19 | 0/23 | 264 | 4 | 0 | 302 | 17.4% |
| `combo_ehs_601525_shared` | `market_favorable` | +103.86% | $20,386.55 | +0.00% | 0 | 21/23 | 268 | 4 | 0 | 304 | 87.0% |
| `combo_ehs_601525_shared` | `partial_50` | -2.58% | $9,741.70 | -2.58% | 8 | 0/23 | 721 | 44 | 621 | 325 | 43.5% |
| `combo_ehs_601525_shared` | `gap_miss` | +22.86% | $12,286.53 | +0.00% | 0 | 17/23 | 256 | 11 | 0 | 300 | 52.2% |
| `combo_eh_3070_shared` | `ideal` | +17.07% | $11,706.96 | +0.00% | 0 | 15/23 | 210 | 4 | 0 | 227 | 34.8% |
| `combo_eh_3070_shared` | `limit_prior` | +18.09% | $11,808.68 | +0.00% | 0 | 15/23 | 174 | 34 | 0 | 229 | 43.5% |
| `combo_eh_3070_shared` | `limit_open` | +9.80% | $10,980.53 | +0.00% | 0 | 12/23 | 197 | 18 | 0 | 235 | 34.8% |
| `combo_eh_3070_shared` | `market_mid` | -15.93% | $8,406.97 | -15.93% | 19 | 0/23 | 202 | 4 | 0 | 223 | 26.1% |
| `combo_eh_3070_shared` | `market_adverse` | -53.45% | $4,654.64 | -53.45% | 23 | 0/23 | 195 | 4 | 0 | 219 | 8.7% |
| `combo_eh_3070_shared` | `market_favorable` | +135.76% | $23,575.86 | +0.00% | 0 | 21/23 | 210 | 4 | 0 | 227 | 87.0% |
| `combo_eh_3070_shared` | `partial_50` | -9.07% | $9,093.26 | -9.07% | 20 | 0/23 | 524 | 30 | 442 | 237 | 43.5% |
| `combo_eh_3070_shared` | `gap_miss` | +17.70% | $11,770.28 | +0.00% | 0 | 12/23 | 198 | 11 | 0 | 223 | 34.8% |
| `combo_ehs_702010_shared` | `ideal` | +21.62% | $12,161.59 | +0.00% | 0 | 16/23 | 264 | 4 | 0 | 302 | 47.8% |
| `combo_ehs_702010_shared` | `limit_prior` | +12.95% | $11,295.02 | +0.00% | 0 | 21/23 | 219 | 42 | 0 | 301 | 43.5% |
| `combo_ehs_702010_shared` | `limit_open` | +18.12% | $11,812.41 | +0.00% | 0 | 15/23 | 245 | 22 | 0 | 310 | 39.1% |
| `combo_ehs_702010_shared` | `market_mid` | -2.12% | $9,787.60 | -2.12% | 4 | 0/23 | 262 | 4 | 0 | 301 | 30.4% |
| `combo_ehs_702010_shared` | `market_adverse` | -33.66% | $6,634.30 | -33.66% | 20 | 0/23 | 256 | 4 | 0 | 298 | 17.4% |
| `combo_ehs_702010_shared` | `market_favorable` | +99.84% | $19,983.87 | +0.00% | 0 | 21/23 | 266 | 4 | 0 | 303 | 87.0% |
| `combo_ehs_702010_shared` | `partial_50` | -4.00% | $9,600.43 | -4.00% | 9 | 0/23 | 722 | 46 | 626 | 327 | 43.5% |
| `combo_ehs_702010_shared` | `gap_miss` | +19.35% | $11,935.01 | +0.00% | 0 | 10/23 | 252 | 11 | 0 | 298 | 43.5% |
| `combo_eh_5050_shared` | `ideal` | +18.53% | $11,853.30 | +0.00% | 0 | 17/23 | 210 | 4 | 0 | 227 | 43.5% |
| `combo_eh_5050_shared` | `limit_prior` | +14.69% | $11,469.38 | +0.00% | 0 | 13/23 | 174 | 34 | 0 | 229 | 43.5% |
| `combo_eh_5050_shared` | `limit_open` | +13.34% | $11,333.72 | +0.00% | 0 | 14/23 | 197 | 18 | 0 | 235 | 43.5% |
| `combo_eh_5050_shared` | `market_mid` | -10.25% | $8,974.65 | -10.25% | 13 | 0/23 | 210 | 4 | 0 | 227 | 21.7% |
| `combo_eh_5050_shared` | `market_adverse` | -44.22% | $5,577.67 | -44.22% | 23 | 0/23 | 206 | 4 | 0 | 225 | 17.4% |
| `combo_eh_5050_shared` | `market_favorable` | +113.41% | $21,341.26 | +0.00% | 0 | 21/23 | 210 | 4 | 0 | 227 | 82.6% |
| `combo_eh_5050_shared` | `partial_50` | -7.60% | $9,240.19 | -7.60% | 15 | 0/23 | 552 | 34 | 469 | 245 | 39.1% |
| `combo_eh_5050_shared` | `gap_miss` | +17.48% | $11,748.22 | +0.00% | 0 | 12/23 | 198 | 11 | 0 | 223 | 39.1% |
| `combo_eh_7030_shared` | `ideal` | +19.41% | $11,940.81 | +0.00% | 0 | 16/23 | 210 | 4 | 0 | 227 | 43.5% |
| `combo_eh_7030_shared` | `limit_prior` | +10.89% | $11,088.80 | +0.00% | 0 | 17/23 | 174 | 34 | 0 | 229 | 39.1% |
| `combo_eh_7030_shared` | `limit_open` | +16.13% | $11,612.81 | +0.00% | 0 | 15/23 | 197 | 18 | 0 | 235 | 39.1% |
| `combo_eh_7030_shared` | `market_mid` | -4.87% | $9,512.96 | -4.87% | 9 | 0/23 | 210 | 4 | 0 | 227 | 30.4% |
| `combo_eh_7030_shared` | `market_adverse` | -36.21% | $6,378.58 | -36.21% | 21 | 0/23 | 206 | 4 | 0 | 225 | 21.7% |
| `combo_eh_7030_shared` | `market_favorable` | +99.03% | $19,903.32 | +0.00% | 0 | 21/23 | 210 | 4 | 0 | 227 | 82.6% |
| `combo_eh_7030_shared` | `partial_50` | -6.01% | $9,399.11 | -6.01% | 11 | 0/23 | 559 | 36 | 483 | 246 | 39.1% |
| `combo_eh_7030_shared` | `gap_miss` | +16.95% | $11,695.50 | +0.00% | 0 | 9/23 | 198 | 11 | 0 | 223 | 43.5% |
| `union_news_pack_h1` | `ideal` | +19.75% | $11,974.90 | +0.00% | 0 | 21/23 | 61 | 2 | 0 | 22 | 43.5% |
| `union_news_pack_h1` | `limit_prior` | +11.84% | $11,184.33 | +0.00% | 0 | 21/23 | 45 | 13 | 0 | 34 | 52.2% |
| `union_news_pack_h1` | `limit_open` | +14.36% | $11,435.68 | +0.00% | 0 | 21/23 | 59 | 4 | 0 | 24 | 43.5% |
| `union_news_pack_h1` | `market_mid` | -0.72% | $9,927.55 | -3.46% | 12 | 9/23 | 59 | 2 | 0 | 23 | 39.1% |
| `union_news_pack_h1` | `market_adverse` | -29.71% | $7,029.39 | -29.71% | 22 | 0/23 | 59 | 2 | 0 | 23 | 4.3% |
| `union_news_pack_h1` | `market_favorable` | +56.83% | $15,683.05 | +0.00% | 0 | 21/23 | 61 | 2 | 0 | 22 | 69.6% |
| `union_news_pack_h1` | `partial_50` | -5.52% | $9,448.15 | -6.51% | 20 | 9/23 | 126 | 6 | 98 | 26 | 34.8% |
| `union_news_pack_h1` | `gap_miss` | +19.75% | $11,974.90 | +0.00% | 0 | 21/23 | 61 | 2 | 0 | 22 | 43.5% |
| `combo_hn_7030_shared` | `ideal` | +15.74% | $11,574.30 | -1.90% | 3 | 15/23 | 201 | 14 | 0 | 120 | 39.1% |
| `combo_hn_7030_shared` | `limit_prior` | +14.44% | $11,443.88 | +0.00% | 0 | 14/23 | 166 | 50 | 0 | 157 | 43.5% |
| `combo_hn_7030_shared` | `limit_open` | +6.27% | $10,626.72 | -2.81% | 3 | 12/23 | 187 | 26 | 0 | 133 | 39.1% |
| `combo_hn_7030_shared` | `market_mid` | -21.34% | $7,865.82 | -21.34% | 22 | 0/23 | 201 | 14 | 0 | 120 | 26.1% |
| `combo_hn_7030_shared` | `market_adverse` | -61.03% | $3,896.66 | -61.03% | 23 | 0/23 | 190 | 12 | 0 | 124 | 4.3% |
| `combo_hn_7030_shared` | `market_favorable` | +171.40% | $27,140.38 | +0.00% | 0 | 21/23 | 205 | 14 | 0 | 118 | 78.3% |
| `combo_hn_7030_shared` | `partial_50` | -14.95% | $8,504.72 | -14.95% | 22 | 0/23 | 524 | 28 | 446 | 112 | 26.1% |
| `combo_hn_7030_shared` | `gap_miss` | +16.72% | $11,672.11 | -0.76% | 1 | 15/23 | 197 | 16 | 0 | 122 | 43.5% |
| `combo_her_5050_shared` | `ideal` | +14.56% | $11,455.86 | +0.00% | 0 | 16/23 | 212 | 4 | 0 | 228 | 47.8% |
| `combo_her_5050_shared` | `limit_prior` | +11.94% | $11,193.97 | -0.15% | 2 | 13/23 | 180 | 33 | 0 | 232 | 43.5% |
| `combo_her_5050_shared` | `limit_open` | +9.82% | $10,981.88 | +0.00% | 0 | 14/23 | 201 | 19 | 0 | 238 | 43.5% |
| `combo_her_5050_shared` | `market_mid` | -12.98% | $8,702.43 | -12.98% | 16 | 0/23 | 206 | 4 | 0 | 225 | 21.7% |
| `combo_her_5050_shared` | `market_adverse` | -46.15% | $5,384.60 | -46.15% | 23 | 0/23 | 204 | 4 | 0 | 224 | 17.4% |
| `combo_her_5050_shared` | `market_favorable` | +111.33% | $21,133.07 | +0.00% | 0 | 21/23 | 214 | 4 | 0 | 229 | 82.6% |
| `combo_her_5050_shared` | `partial_50` | -8.66% | $9,133.49 | -8.66% | 20 | 0/23 | 551 | 32 | 468 | 242 | 39.1% |
| `combo_her_5050_shared` | `gap_miss` | +16.72% | $11,672.21 | +0.00% | 0 | 12/23 | 198 | 12 | 0 | 223 | 47.8% |
| `union_earn_react_h3` | `ideal` | +18.91% | $11,890.99 | +0.00% | 0 | 12/23 | 98 | 0 | 0 | 170 | 43.5% |
| `union_earn_react_h3` | `limit_prior` | -1.27% | $9,872.58 | -3.27% | 20 | 16/23 | 103 | 19 | 0 | 177 | 39.1% |
| `union_earn_react_h3` | `limit_open` | +17.88% | $11,787.97 | +0.00% | 0 | 8/23 | 95 | 5 | 0 | 171 | 39.1% |
| `union_earn_react_h3` | `market_mid` | +0.33% | $10,032.59 | +0.00% | 0 | 1/23 | 92 | 0 | 0 | 167 | 39.1% |
| `union_earn_react_h3` | `market_adverse` | -26.68% | $7,331.56 | -26.68% | 18 | 0/23 | 80 | 0 | 0 | 161 | 26.1% |
| `union_earn_react_h3` | `market_favorable` | +78.42% | $17,841.50 | +0.00% | 0 | 21/23 | 118 | 0 | 0 | 180 | 78.3% |
| `union_earn_react_h3` | `partial_50` | -2.45% | $9,755.00 | -2.45% | 8 | 0/23 | 365 | 24 | 310 | 213 | 39.1% |
| `union_earn_react_h3` | `gap_miss` | +19.12% | $11,911.64 | +0.00% | 0 | 9/23 | 106 | 6 | 0 | 174 | 43.5% |
| `combo_seh_333_split` | `ideal` | +16.83% | $11,682.90 | +0.00% | 0 | 20/23 | 256 | 4 | 0 | 303 | 56.5% |
| `combo_seh_333_split` | `limit_prior` | +10.52% | $11,052.30 | +0.00% | 0 | 21/23 | 227 | 43 | 0 | 312 | 43.5% |
| `combo_seh_333_split` | `limit_open` | +11.58% | $11,157.62 | +0.00% | 0 | 19/23 | 237 | 21 | 0 | 310 | 47.8% |
| `combo_seh_333_split` | `market_mid` | -6.31% | $9,368.74 | -6.31% | 13 | 0/23 | 246 | 4 | 0 | 298 | 26.1% |
| `combo_seh_333_split` | `market_adverse` | -34.36% | $6,563.90 | -34.36% | 23 | 0/23 | 246 | 4 | 0 | 298 | 13.0% |
| `combo_seh_333_split` | `market_favorable` | +107.68% | $20,767.93 | +0.00% | 0 | 21/23 | 264 | 4 | 0 | 307 | 87.0% |
| `combo_seh_333_split` | `partial_50` | -3.42% | $9,658.40 | -3.42% | 12 | 0/23 | 673 | 36 | 556 | 331 | 39.1% |
| `combo_seh_333_split` | `gap_miss` | +16.70% | $11,670.41 | +0.00% | 0 | 21/23 | 248 | 10 | 0 | 301 | 60.9% |
| `combo_he1_5050_shared` | `ideal` | +14.13% | $11,413.38 | +0.00% | 0 | 19/23 | 212 | 20 | 0 | 105 | 39.1% |
| `combo_he1_5050_shared` | `limit_prior` | +15.22% | $11,521.85 | +0.00% | 0 | 19/23 | 174 | 53 | 0 | 139 | 47.8% |
| `combo_he1_5050_shared` | `limit_open` | +2.89% | $10,289.38 | +0.00% | 0 | 10/23 | 199 | 33 | 0 | 118 | 39.1% |
| `combo_he1_5050_shared` | `market_mid` | -23.86% | $7,613.62 | -23.86% | 22 | 0/23 | 212 | 20 | 0 | 105 | 21.7% |
| `combo_he1_5050_shared` | `market_adverse` | -63.77% | $3,623.41 | -63.77% | 23 | 0/23 | 208 | 20 | 0 | 107 | 4.3% |
| `combo_he1_5050_shared` | `market_favorable` | +181.72% | $28,171.92 | +0.00% | 0 | 21/23 | 214 | 20 | 0 | 104 | 73.9% |
| `combo_he1_5050_shared` | `partial_50` | -12.67% | $8,732.85 | -12.67% | 21 | 0/23 | 582 | 42 | 497 | 112 | 34.8% |
| `combo_he1_5050_shared` | `gap_miss` | +13.79% | $11,379.50 | +0.00% | 0 | 13/23 | 198 | 27 | 0 | 112 | 39.1% |
| `combo_seh_601525_shared` | `ideal` | +29.46% | $12,946.51 | +0.00% | 0 | 17/23 | 258 | 4 | 0 | 299 | 69.6% |
| `combo_seh_601525_shared` | `limit_prior` | +26.45% | $12,645.50 | +0.00% | 0 | 17/23 | 210 | 41 | 0 | 295 | 73.9% |
| `combo_seh_601525_shared` | `limit_open` | +16.56% | $11,655.89 | +0.00% | 0 | 17/23 | 240 | 22 | 0 | 307 | 69.6% |
| `combo_seh_601525_shared` | `market_mid` | -3.49% | $9,650.95 | -3.49% | 12 | 0/23 | 255 | 4 | 0 | 297 | 34.8% |
| `combo_seh_601525_shared` | `market_adverse` | -45.28% | $5,472.05 | -45.28% | 23 | 0/23 | 250 | 4 | 0 | 294 | 4.3% |
| `combo_seh_601525_shared` | `market_favorable` | +145.04% | $24,503.79 | +0.00% | 0 | 21/23 | 264 | 4 | 0 | 302 | 91.3% |
| `combo_seh_601525_shared` | `partial_50` | -1.00% | $9,899.86 | -1.00% | 5 | 0/23 | 643 | 32 | 543 | 306 | 43.5% |
| `combo_seh_601525_shared` | `gap_miss` | +30.30% | $13,030.50 | +0.00% | 0 | 17/23 | 246 | 11 | 0 | 295 | 65.2% |
| `combo_se_5050_skip` | `ideal` | +29.03% | $12,902.75 | +0.00% | 0 | 16/23 | 156 | 0 | 0 | 255 | 52.2% |
| `combo_se_5050_skip` | `limit_prior` | +13.77% | $11,377.36 | +0.00% | 0 | 17/23 | 152 | 25 | 0 | 259 | 60.9% |
| `combo_se_5050_skip` | `limit_open` | +24.81% | $12,480.91 | +0.00% | 0 | 13/23 | 149 | 9 | 0 | 247 | 52.2% |
| `combo_se_5050_skip` | `market_mid` | +4.51% | $10,451.48 | +0.00% | 0 | 1/23 | 154 | 0 | 0 | 254 | 43.5% |
| `combo_se_5050_skip` | `market_adverse` | -30.22% | $6,978.35 | -30.22% | 17 | 0/23 | 148 | 0 | 0 | 241 | 21.7% |
| `combo_se_5050_skip` | `market_favorable` | +98.71% | $19,871.32 | +0.00% | 0 | 21/23 | 156 | 0 | 0 | 255 | 73.9% |
| `combo_se_5050_skip` | `partial_50` | +4.01% | $10,401.28 | +0.00% | 0 | 1/23 | 513 | 34 | 440 | 301 | 47.8% |
| `combo_se_5050_skip` | `gap_miss` | +26.03% | $12,603.34 | +0.00% | 0 | 14/23 | 150 | 3 | 0 | 252 | 52.2% |
| `combo_jer_5050_shared` | `ideal` | +26.30% | $12,630.17 | +0.00% | 0 | 8/23 | 218 | 16 | 0 | 251 | 43.5% |
| `combo_jer_5050_shared` | `limit_prior` | +4.88% | $10,488.52 | -0.98% | 4 | 13/23 | 212 | 49 | 0 | 246 | 43.5% |
| `combo_jer_5050_shared` | `limit_open` | +24.02% | $12,402.51 | +0.00% | 0 | 6/23 | 209 | 26 | 0 | 256 | 43.5% |
| `combo_jer_5050_shared` | `market_mid` | +2.53% | $10,252.89 | +0.00% | 0 | 1/23 | 216 | 16 | 0 | 250 | 26.1% |
| `combo_jer_5050_shared` | `market_adverse` | -29.99% | $7,001.37 | -29.99% | 19 | 0/23 | 208 | 16 | 0 | 250 | 13.0% |
| `combo_jer_5050_shared` | `market_favorable` | +117.13% | $21,713.23 | +0.00% | 0 | 21/23 | 230 | 16 | 0 | 245 | 82.6% |
| `combo_jer_5050_shared` | `partial_50` | -2.86% | $9,714.33 | -2.86% | 5 | 0/23 | 686 | 52 | 582 | 274 | 39.1% |
| `combo_jer_5050_shared` | `gap_miss` | +25.01% | $12,501.17 | +0.00% | 0 | 6/23 | 208 | 23 | 0 | 245 | 43.5% |
| `combo_ers_7030_shared` | `ideal` | +24.70% | $12,469.83 | +0.00% | 0 | 15/23 | 186 | 0 | 0 | 268 | 39.1% |
| `combo_ers_7030_shared` | `limit_prior` | +6.42% | $10,642.35 | -2.98% | 5 | 21/23 | 162 | 29 | 0 | 262 | 52.2% |
| `combo_ers_7030_shared` | `limit_open` | +21.82% | $12,182.46 | +0.00% | 0 | 14/23 | 177 | 12 | 0 | 272 | 43.5% |
| `combo_ers_7030_shared` | `market_mid` | +3.30% | $10,329.68 | +0.00% | 0 | 1/23 | 186 | 0 | 0 | 268 | 39.1% |
| `combo_ers_7030_shared` | `market_adverse` | -27.94% | $7,206.05 | -27.94% | 18 | 0/23 | 180 | 0 | 0 | 265 | 17.4% |
| `combo_ers_7030_shared` | `market_favorable` | +89.40% | $18,940.29 | +0.00% | 0 | 21/23 | 186 | 0 | 0 | 268 | 82.6% |
| `combo_ers_7030_shared` | `partial_50` | -0.64% | $9,936.03 | -0.64% | 3 | 0/23 | 560 | 32 | 482 | 302 | 47.8% |
| `combo_ers_7030_shared` | `gap_miss` | +24.63% | $12,463.30 | +0.00% | 0 | 15/23 | 174 | 6 | 0 | 262 | 47.8% |
| `combo_se1_5050_shared` | `ideal` | +27.56% | $12,756.00 | +0.00% | 0 | 16/23 | 196 | 16 | 0 | 145 | 69.6% |
| `combo_se1_5050_shared` | `limit_prior` | +13.63% | $11,363.47 | +0.00% | 0 | 17/23 | 156 | 47 | 0 | 162 | 69.6% |
| `combo_se1_5050_shared` | `limit_open` | +18.57% | $11,856.67 | +0.00% | 0 | 16/23 | 187 | 26 | 0 | 152 | 65.2% |
| `combo_se1_5050_shared` | `market_mid` | -8.26% | $9,173.69 | -8.39% | 15 | 0/23 | 196 | 16 | 0 | 145 | 56.5% |
| `combo_se1_5050_shared` | `market_adverse` | -51.29% | $4,870.61 | -51.29% | 23 | 0/23 | 194 | 16 | 0 | 146 | 13.0% |
| `combo_se1_5050_shared` | `market_favorable` | +158.50% | $25,850.09 | +0.00% | 0 | 21/23 | 194 | 16 | 0 | 144 | 87.0% |
| `combo_se1_5050_shared` | `partial_50` | -2.07% | $9,793.41 | -2.07% | 12 | 0/23 | 573 | 36 | 490 | 159 | 43.5% |
| `combo_se1_5050_shared` | `gap_miss` | +26.20% | $12,619.66 | +0.00% | 0 | 17/23 | 184 | 22 | 0 | 151 | 69.6% |
| `combo_ser_5050_shared` | `ideal` | +25.46% | $12,546.24 | +0.00% | 0 | 16/23 | 166 | 0 | 0 | 248 | 43.5% |
| `combo_ser_5050_shared` | `limit_prior` | +10.12% | $11,011.45 | -1.83% | 3 | 17/23 | 162 | 29 | 0 | 262 | 52.2% |
| `combo_ser_5050_shared` | `limit_open` | +21.28% | $12,128.03 | +0.00% | 0 | 10/23 | 159 | 12 | 0 | 253 | 43.5% |
| `combo_ser_5050_shared` | `market_mid` | +2.76% | $10,276.28 | +0.00% | 0 | 1/23 | 166 | 0 | 0 | 248 | 39.1% |
| `combo_ser_5050_shared` | `market_adverse` | -30.77% | $6,922.99 | -30.77% | 19 | 0/23 | 162 | 0 | 0 | 246 | 17.4% |
| `combo_ser_5050_shared` | `market_favorable` | +93.14% | $19,314.08 | +0.00% | 0 | 21/23 | 168 | 0 | 0 | 259 | 73.9% |
| `combo_ser_5050_shared` | `partial_50` | +1.78% | $10,177.74 | +0.00% | 0 | 1/23 | 546 | 32 | 465 | 302 | 47.8% |
| `combo_ser_5050_shared` | `gap_miss` | +24.07% | $12,407.32 | +0.00% | 0 | 14/23 | 158 | 4 | 0 | 244 | 47.8% |
| `combo_sj_5050_shared` | `ideal` | +23.74% | $12,374.29 | -3.44% | 5 | 17/23 | 194 | 16 | 0 | 145 | 69.6% |
| `combo_sj_5050_shared` | `limit_prior` | +23.29% | $12,328.91 | -1.42% | 2 | 17/23 | 170 | 42 | 0 | 155 | 56.5% |
| `combo_sj_5050_shared` | `limit_open` | +16.96% | $11,695.84 | -4.17% | 6 | 17/23 | 182 | 26 | 0 | 150 | 60.9% |
| `combo_sj_5050_shared` | `market_mid` | -5.18% | $9,481.69 | -9.43% | 22 | 7/23 | 194 | 16 | 0 | 145 | 39.1% |
| `combo_sj_5050_shared` | `market_adverse` | -43.66% | $5,633.66 | -43.66% | 22 | 0/23 | 194 | 16 | 0 | 145 | 17.4% |
| `combo_sj_5050_shared` | `market_favorable` | +187.08% | $28,708.23 | +0.00% | 0 | 21/23 | 194 | 16 | 0 | 145 | 87.0% |
| `combo_sj_5050_shared` | `partial_50` | -3.25% | $9,675.46 | -4.85% | 21 | 3/23 | 589 | 42 | 513 | 164 | 26.1% |
| `combo_sj_5050_shared` | `gap_miss` | +21.14% | $12,113.99 | -2.40% | 2 | 17/23 | 186 | 20 | 0 | 147 | 65.2% |
| `combo_e1s_7030_shared` | `ideal` | +23.05% | $12,305.15 | +0.00% | 0 | 16/23 | 196 | 16 | 0 | 145 | 65.2% |
| `combo_e1s_7030_shared` | `limit_prior` | +12.41% | $11,241.29 | +0.00% | 0 | 21/23 | 156 | 47 | 0 | 162 | 69.6% |
| `combo_e1s_7030_shared` | `limit_open` | +13.55% | $11,355.48 | +0.00% | 0 | 16/23 | 187 | 26 | 0 | 152 | 65.2% |
| `combo_e1s_7030_shared` | `market_mid` | -13.57% | $8,642.65 | -13.57% | 15 | 0/23 | 196 | 16 | 0 | 145 | 47.8% |
| `combo_e1s_7030_shared` | `market_adverse` | -55.82% | $4,418.29 | -55.82% | 23 | 0/23 | 196 | 16 | 0 | 145 | 8.7% |
| `combo_e1s_7030_shared` | `market_favorable` | +185.29% | $28,529.00 | +0.00% | 0 | 21/23 | 196 | 16 | 0 | 145 | 87.0% |
| `combo_e1s_7030_shared` | `partial_50` | -3.93% | $9,607.25 | -3.93% | 12 | 0/23 | 592 | 42 | 515 | 165 | 43.5% |
| `combo_e1s_7030_shared` | `gap_miss` | +23.92% | $12,392.15 | +0.00% | 0 | 17/23 | 184 | 22 | 0 | 151 | 69.6% |
| `combo_sj_3070_shared` | `ideal` | +21.84% | $12,184.13 | -4.56% | 5 | 17/23 | 194 | 16 | 0 | 145 | 65.2% |
| `combo_sj_3070_shared` | `limit_prior` | +22.90% | $12,290.33 | -1.71% | 2 | 17/23 | 170 | 42 | 0 | 155 | 56.5% |
| `combo_sj_3070_shared` | `limit_open` | +16.13% | $11,613.05 | -5.36% | 6 | 17/23 | 182 | 26 | 0 | 150 | 65.2% |
| `combo_sj_3070_shared` | `market_mid` | -9.74% | $9,025.64 | -11.50% | 22 | 0/23 | 194 | 16 | 0 | 145 | 34.8% |
| `combo_sj_3070_shared` | `market_adverse` | -49.02% | $5,097.91 | -49.02% | 22 | 0/23 | 194 | 16 | 0 | 145 | 8.7% |
| `combo_sj_3070_shared` | `market_favorable` | +220.53% | $32,052.82 | +0.00% | 0 | 21/23 | 194 | 16 | 0 | 145 | 82.6% |
| `combo_sj_3070_shared` | `partial_50` | -5.01% | $9,499.30 | -5.88% | 21 | 1/23 | 606 | 44 | 533 | 166 | 30.4% |
| `combo_sj_3070_shared` | `gap_miss` | +20.67% | $12,067.29 | -3.44% | 5 | 17/23 | 186 | 20 | 0 | 147 | 65.2% |
| `combo_sj_7030_shared` | `ideal` | +19.26% | $11,925.94 | -2.12% | 2 | 17/23 | 194 | 16 | 0 | 145 | 60.9% |
| `combo_sj_7030_shared` | `limit_prior` | +18.18% | $11,818.20 | -1.12% | 2 | 17/23 | 170 | 42 | 0 | 155 | 56.5% |
| `combo_sj_7030_shared` | `limit_open` | +14.05% | $11,405.25 | -2.77% | 6 | 17/23 | 182 | 26 | 0 | 150 | 60.9% |
| `combo_sj_7030_shared` | `market_mid` | -1.44% | $9,855.85 | -6.65% | 19 | 9/23 | 194 | 16 | 0 | 145 | 39.1% |
| `combo_sj_7030_shared` | `market_adverse` | -32.21% | $6,778.73 | -32.21% | 22 | 0/23 | 192 | 16 | 0 | 146 | 17.4% |
| `combo_sj_7030_shared` | `market_favorable` | +120.96% | $22,095.56 | +0.00% | 0 | 21/23 | 194 | 16 | 0 | 145 | 87.0% |
| `combo_sj_7030_shared` | `partial_50` | -1.26% | $9,873.66 | -3.47% | 16 | 7/23 | 560 | 36 | 483 | 159 | 30.4% |
| `combo_sj_7030_shared` | `gap_miss` | +17.67% | $11,767.02 | -1.50% | 2 | 17/23 | 186 | 20 | 0 | 147 | 60.9% |
| `combo_snj_333_shared` | `ideal` | +17.14% | $11,713.69 | -1.87% | 3 | 17/23 | 319 | 26 | 0 | 219 | 60.9% |
| `combo_snj_333_shared` | `limit_prior` | +9.06% | $10,906.00 | -1.09% | 3 | 17/23 | 269 | 78 | 0 | 255 | 56.5% |
| `combo_snj_333_shared` | `limit_open` | +8.95% | $10,894.81 | -2.93% | 6 | 16/23 | 301 | 43 | 0 | 231 | 56.5% |
| `combo_snj_333_shared` | `market_mid` | -15.16% | $8,484.31 | -15.16% | 22 | 0/23 | 317 | 26 | 0 | 220 | 34.8% |
| `combo_snj_333_shared` | `market_adverse` | -53.80% | $4,620.49 | -53.80% | 22 | 0/23 | 309 | 26 | 0 | 224 | 13.0% |
| `combo_snj_333_shared` | `market_favorable` | +176.87% | $27,686.69 | +0.00% | 0 | 21/23 | 321 | 26 | 0 | 218 | 82.6% |
| `combo_snj_333_shared` | `partial_50` | -10.70% | $8,929.84 | -10.70% | 22 | 0/23 | 851 | 50 | 715 | 230 | 21.7% |
| `combo_snj_333_shared` | `gap_miss` | +15.73% | $11,573.23 | -1.15% | 3 | 17/23 | 309 | 31 | 0 | 222 | 65.2% |
| `combo_sn_5050_shared` | `ideal` | +16.30% | $11,630.05 | +0.00% | 0 | 17/23 | 205 | 10 | 0 | 166 | 69.6% |
| `combo_sn_5050_shared` | `limit_prior` | +4.64% | $10,463.82 | -1.17% | 7 | 17/23 | 162 | 48 | 0 | 188 | 52.2% |
| `combo_sn_5050_shared` | `limit_open` | +8.12% | $10,811.88 | -0.18% | 3 | 16/23 | 193 | 23 | 0 | 174 | 56.5% |
| `combo_sn_5050_shared` | `market_mid` | -10.62% | $8,938.42 | -11.55% | 20 | 1/23 | 205 | 10 | 0 | 166 | 34.8% |
| `combo_sn_5050_shared` | `market_adverse` | -45.78% | $5,422.03 | -45.78% | 22 | 0/23 | 205 | 10 | 0 | 166 | 13.0% |
| `combo_sn_5050_shared` | `market_favorable` | +112.39% | $21,238.66 | +0.00% | 0 | 21/23 | 207 | 10 | 0 | 165 | 87.0% |
| `combo_sn_5050_shared` | `partial_50` | -7.79% | $9,220.54 | -7.79% | 20 | 0/23 | 533 | 22 | 445 | 167 | 21.7% |
| `combo_sn_5050_shared` | `gap_miss` | +15.35% | $11,534.56 | +0.00% | 0 | 17/23 | 201 | 12 | 0 | 166 | 65.2% |
| `combo_je1_5050_shared` | `ideal` | +12.56% | $11,256.23 | +0.00% | 0 | 14/23 | 254 | 32 | 0 | 118 | 39.1% |
| `combo_je1_5050_shared` | `limit_prior` | +7.01% | $10,701.46 | +0.00% | 0 | 19/23 | 216 | 70 | 0 | 156 | 52.2% |
| `combo_je1_5050_shared` | `limit_open` | +7.53% | $10,752.90 | +0.00% | 0 | 9/23 | 243 | 42 | 0 | 128 | 30.4% |
| `combo_je1_5050_shared` | `market_mid` | -19.57% | $8,043.42 | -19.57% | 21 | 0/23 | 254 | 32 | 0 | 118 | 17.4% |
| `combo_je1_5050_shared` | `market_adverse` | -57.45% | $4,254.59 | -57.45% | 23 | 0/23 | 244 | 32 | 0 | 123 | 0.0% |
| `combo_je1_5050_shared` | `market_favorable` | +191.72% | $29,171.67 | +0.00% | 0 | 21/23 | 254 | 32 | 0 | 118 | 69.6% |
| `combo_je1_5050_shared` | `partial_50` | -9.41% | $9,058.72 | -9.41% | 18 | 0/23 | 735 | 62 | 628 | 149 | 26.1% |
| `combo_je1_5050_shared` | `gap_miss` | +10.42% | $11,041.75 | +0.00% | 0 | 11/23 | 238 | 40 | 0 | 126 | 30.4% |
| `flatten_live_h1` | `ideal` | +8.22% | $10,821.57 | +0.00% | 0 | 7/23 | 48 | 0 | 0 | 0 | 13.0% |
| `flatten_live_h1` | `limit_prior` | +7.88% | $10,788.48 | +0.00% | 0 | 7/23 | 40 | 8 | 0 | 8 | 17.4% |
| `flatten_live_h1` | `limit_open` | +7.84% | $10,784.48 | +0.00% | 0 | 7/23 | 48 | 2 | 0 | 2 | 17.4% |
| `flatten_live_h1` | `market_mid` | -0.23% | $9,977.26 | -0.23% | 6 | 0/23 | 48 | 0 | 0 | 0 | 13.0% |
| `flatten_live_h1` | `market_adverse` | -14.76% | $8,523.64 | -14.76% | 18 | 0/23 | 48 | 0 | 0 | 0 | 0.0% |
| `flatten_live_h1` | `market_favorable` | +24.22% | $12,421.60 | +0.00% | 0 | 17/23 | 48 | 0 | 0 | 0 | 21.7% |
| `flatten_live_h1` | `partial_50` | -2.16% | $9,784.31 | -2.17% | 6 | 0/23 | 155 | 16 | 139 | 16 | 26.1% |
| `flatten_live_h1` | `gap_miss` | +3.51% | $10,350.59 | +0.00% | 0 | 6/23 | 46 | 1 | 0 | 1 | 8.7% |
| `flatten_live_h3` | `ideal` | -6.24% | $9,376.07 | -6.24% | 5 | 0/23 | 42 | 0 | 0 | 45 | 13.0% |
| `flatten_live_h3` | `limit_prior` | -5.33% | $9,467.38 | -5.85% | 5 | 0/23 | 40 | 8 | 0 | 51 | 17.4% |
| `flatten_live_h3` | `limit_open` | -6.60% | $9,340.13 | -6.60% | 5 | 0/23 | 42 | 2 | 0 | 47 | 17.4% |
| `flatten_live_h3` | `market_mid` | -10.19% | $8,980.87 | -10.19% | 7 | 0/23 | 38 | 0 | 0 | 43 | 13.0% |
| `flatten_live_h3` | `market_adverse` | -17.17% | $8,282.92 | -17.17% | 16 | 0/23 | 40 | 0 | 0 | 44 | 8.7% |
| `flatten_live_h3` | `market_favorable` | +3.43% | $10,343.23 | +0.00% | 0 | 7/23 | 42 | 0 | 0 | 45 | 26.1% |
| `flatten_live_h3` | `partial_50` | -4.54% | $9,546.20 | -4.58% | 6 | 0/23 | 134 | 16 | 118 | 64 | 30.4% |
| `flatten_live_h3` | `gap_miss` | -6.27% | $9,372.86 | -6.27% | 5 | 0/23 | 40 | 1 | 0 | 44 | 13.0% |
| `flatten_live_h5` | `ideal` | -5.59% | $9,441.08 | -6.83% | 5 | 0/23 | 34 | 16 | 0 | 103 | 26.1% |
| `flatten_live_h5` | `limit_prior` | -5.28% | $9,472.11 | -6.52% | 5 | 0/23 | 32 | 20 | 0 | 103 | 26.1% |
| `flatten_live_h5` | `limit_open` | -5.59% | $9,441.08 | -6.83% | 5 | 0/23 | 34 | 16 | 0 | 103 | 26.1% |
| `flatten_live_h5` | `market_mid` | -8.35% | $9,164.88 | -9.54% | 6 | 0/23 | 30 | 16 | 0 | 97 | 21.7% |
| `flatten_live_h5` | `market_adverse` | -12.99% | $8,700.76 | -14.10% | 14 | 0/23 | 32 | 16 | 0 | 100 | 17.4% |
| `flatten_live_h5` | `market_favorable` | +0.78% | $10,077.80 | -0.51% | 1 | 7/23 | 34 | 16 | 0 | 103 | 30.4% |
| `flatten_live_h5` | `partial_50` | -4.14% | $9,586.01 | -4.75% | 6 | 0/23 | 118 | 16 | 102 | 112 | 34.8% |
| `flatten_live_h5` | `gap_miss` | -5.67% | $9,433.44 | -6.91% | 5 | 0/23 | 32 | 17 | 0 | 100 | 21.7% |
| `union_news_g_h5` | `ideal` | -9.38% | $9,062.48 | -10.34% | 20 | 0/23 | 89 | 9 | 0 | 265 | 39.1% |
| `union_news_g_h5` | `limit_prior` | -8.22% | $9,177.94 | -9.13% | 20 | 1/23 | 86 | 30 | 0 | 262 | 34.8% |
| `union_news_g_h5` | `limit_open` | -10.76% | $8,923.59 | -10.76% | 21 | 0/23 | 97 | 17 | 0 | 276 | 26.1% |
| `union_news_g_h5` | `market_mid` | -16.36% | $8,363.59 | -17.17% | 21 | 0/23 | 87 | 9 | 0 | 262 | 30.4% |
| `union_news_g_h5` | `market_adverse` | -27.33% | $7,267.27 | -28.11% | 22 | 0/23 | 92 | 9 | 0 | 267 | 21.7% |
| `union_news_g_h5` | `market_favorable` | +6.54% | $10,654.55 | -2.31% | 5 | 21/23 | 101 | 9 | 0 | 279 | 39.1% |
| `union_news_g_h5` | `partial_50` | -13.01% | $8,699.10 | -13.01% | 21 | 0/23 | 281 | 19 | 231 | 306 | 17.4% |
| `union_news_g_h5` | `gap_miss` | -9.44% | $9,056.24 | -10.40% | 20 | 0/23 | 87 | 10 | 0 | 262 | 39.1% |
| `union_white_coil_h1` | `ideal` | -3.65% | $9,635.21 | -6.17% | 17 | 8/23 | 122 | 5 | 0 | 5 | 26.1% |
| `union_white_coil_h1` | `limit_prior` | -1.75% | $9,824.87 | -1.75% | 10 | 0/23 | 111 | 18 | 0 | 18 | 34.8% |
| `union_white_coil_h1` | `limit_open` | -6.06% | $9,394.43 | -6.06% | 17 | 0/23 | 117 | 6 | 0 | 6 | 21.7% |
| `union_white_coil_h1` | `market_mid` | -18.24% | $8,176.04 | -19.46% | 23 | 4/23 | 122 | 5 | 0 | 5 | 17.4% |
| `union_white_coil_h1` | `market_adverse` | -40.19% | $5,981.31 | -40.19% | 23 | 0/23 | 122 | 5 | 0 | 5 | 0.0% |
| `union_white_coil_h1` | `market_favorable` | +53.26% | $15,325.60 | +0.00% | 0 | 21/23 | 122 | 5 | 0 | 5 | 65.2% |
| `union_white_coil_h1` | `partial_50` | -11.98% | $8,801.81 | -12.59% | 23 | 4/23 | 393 | 25 | 342 | 25 | 17.4% |
| `union_white_coil_h1` | `gap_miss` | -4.89% | $9,511.38 | -7.36% | 19 | 5/23 | 120 | 6 | 0 | 6 | 26.1% |
| `union_news_or_h1` | `ideal` | +3.83% | $10,383.16 | -0.78% | 1 | 15/23 | 133 | 10 | 0 | 75 | 39.1% |
| `union_news_or_h1` | `limit_prior` | -2.72% | $9,727.88 | -3.76% | 19 | 7/23 | 111 | 31 | 0 | 97 | 30.4% |
| `union_news_or_h1` | `limit_open` | -1.27% | $9,873.40 | -3.33% | 12 | 7/23 | 125 | 16 | 0 | 82 | 34.8% |
| `union_news_or_h1` | `market_mid` | -16.55% | $8,345.53 | -16.55% | 21 | 0/23 | 131 | 10 | 0 | 76 | 13.0% |
| `union_news_or_h1` | `market_adverse` | -44.78% | $5,522.44 | -44.78% | 22 | 0/23 | 131 | 10 | 0 | 76 | 0.0% |
| `union_news_or_h1` | `market_favorable` | +71.86% | $17,186.02 | +0.00% | 0 | 21/23 | 135 | 10 | 0 | 74 | 69.6% |
| `union_news_or_h1` | `partial_50` | -12.08% | $8,791.56 | -12.08% | 21 | 0/23 | 350 | 22 | 293 | 85 | 13.0% |
| `union_news_or_h1` | `gap_miss` | +2.63% | $10,263.52 | -2.09% | 6 | 15/23 | 129 | 11 | 0 | 77 | 34.8% |
| `union_news_or_net4_h1` | `ideal` | -2.37% | $9,763.50 | -2.37% | 3 | 0/23 | 97 | 2 | 0 | 25 | 34.8% |
| `union_news_or_net4_h1` | `limit_prior` | +0.48% | $10,048.00 | -0.79% | 4 | 6/23 | 79 | 16 | 0 | 39 | 34.8% |
| `union_news_or_net4_h1` | `limit_open` | -4.60% | $9,539.75 | -4.60% | 6 | 0/23 | 95 | 5 | 0 | 28 | 34.8% |
| `union_news_or_net4_h1` | `market_mid` | -16.66% | $8,334.53 | -16.66% | 17 | 0/23 | 97 | 2 | 0 | 25 | 26.1% |
| `union_news_or_net4_h1` | `market_adverse` | -38.70% | $6,129.67 | -38.70% | 22 | 0/23 | 95 | 2 | 0 | 26 | 4.3% |
| `union_news_or_net4_h1` | `market_favorable` | +66.56% | $16,656.02 | +0.00% | 0 | 21/23 | 99 | 2 | 0 | 24 | 69.6% |
| `union_news_or_net4_h1` | `partial_50` | -10.64% | $8,936.17 | -10.64% | 18 | 0/23 | 250 | 14 | 207 | 37 | 26.1% |
| `union_news_or_net4_h1` | `gap_miss` | -3.85% | $9,615.34 | -3.85% | 6 | 0/23 | 95 | 3 | 0 | 26 | 30.4% |
| `union_news_or_net4_conv_h1` | `ideal` | +2.12% | $10,211.75 | +0.00% | 0 | 2/23 | 59 | 2 | 0 | 26 | 39.1% |
| `union_news_or_net4_conv_h1` | `limit_prior` | -0.83% | $9,917.13 | -0.83% | 4 | 0/23 | 45 | 13 | 0 | 38 | 39.1% |
| `union_news_or_net4_conv_h1` | `limit_open` | -1.71% | $9,829.43 | -1.71% | 4 | 0/23 | 57 | 4 | 0 | 28 | 34.8% |
| `union_news_or_net4_conv_h1` | `market_mid` | -12.69% | $8,730.87 | -12.69% | 12 | 0/23 | 59 | 2 | 0 | 26 | 34.8% |
| `union_news_or_net4_conv_h1` | `market_adverse` | -34.65% | $6,535.28 | -34.65% | 22 | 0/23 | 57 | 2 | 0 | 27 | 8.7% |
| `union_news_or_net4_conv_h1` | `market_favorable` | +41.90% | $14,190.36 | +0.00% | 0 | 21/23 | 59 | 2 | 0 | 26 | 60.9% |
| `union_news_or_net4_conv_h1` | `partial_50` | -8.89% | $9,111.37 | -8.89% | 12 | 0/23 | 159 | 8 | 133 | 33 | 21.7% |
| `union_news_or_net4_conv_h1` | `gap_miss` | +2.12% | $10,211.75 | +0.00% | 0 | 2/23 | 59 | 2 | 0 | 26 | 39.1% |
| `short_news_head_h3` | `ideal` | +14.03% | $11,403.44 | +0.00% | 0 | 17/23 | 51 | 0 | 0 | 69 | 60.9% |
| `short_news_head_h3` | `limit_prior` | +8.87% | $10,886.68 | -3.83% | 4 | 17/23 | 38 | 9 | 0 | 65 | 52.2% |
| `short_news_head_h3` | `limit_open` | +10.53% | $11,052.62 | -0.69% | 3 | 17/23 | 47 | 3 | 0 | 68 | 52.2% |
| `short_news_head_h3` | `market_mid` | +4.00% | $10,400.41 | -2.66% | 6 | 16/23 | 51 | 0 | 0 | 69 | 39.1% |
| `short_news_head_h3` | `market_adverse` | -12.50% | $8,749.92 | -12.50% | 20 | 0/23 | 51 | 0 | 0 | 69 | 26.1% |
| `short_news_head_h3` | `market_favorable` | +38.25% | $13,825.27 | +0.00% | 0 | 21/23 | 47 | 0 | 0 | 67 | 73.9% |
| `short_news_head_h3` | `partial_50` | +0.87% | $10,086.99 | -0.82% | 7 | 12/23 | 168 | 8 | 150 | 72 | 43.5% |
| `short_news_head_h3` | `gap_miss` | +13.30% | $11,330.30 | +0.00% | 0 | 17/23 | 49 | 1 | 0 | 68 | 56.5% |
| `union_news_g_cam91_n1_h1` | `ideal` | -2.85% | $9,714.45 | -2.85% | 7 | 0/23 | 4 | 0 | 0 | 0 | 4.3% |
| `union_news_g_cam91_n1_h1` | `limit_prior` | -5.07% | $9,493.25 | -5.29% | 7 | 0/23 | 2 | 2 | 0 | 2 | 4.3% |
| `union_news_g_cam91_n1_h1` | `limit_open` | -2.85% | $9,714.45 | -2.85% | 7 | 0/23 | 4 | 0 | 0 | 0 | 4.3% |
| `union_news_g_cam91_n1_h1` | `market_mid` | -4.79% | $9,520.88 | -4.79% | 16 | 0/23 | 4 | 0 | 0 | 0 | 4.3% |
| `union_news_g_cam91_n1_h1` | `market_adverse` | -9.37% | $9,063.32 | -9.37% | 17 | 0/23 | 4 | 0 | 0 | 0 | 0.0% |
| `union_news_g_cam91_n1_h1` | `market_favorable` | +1.43% | $10,143.43 | +0.00% | 0 | 7/23 | 4 | 0 | 0 | 0 | 13.0% |
| `union_news_g_cam91_n1_h1` | `partial_50` | -3.48% | $9,651.58 | -3.55% | 16 | 0/23 | 13 | 2 | 12 | 2 | 13.0% |
| `union_news_g_cam91_n1_h1` | `gap_miss` | -2.85% | $9,714.45 | -2.85% | 7 | 0/23 | 4 | 0 | 0 | 0 | 4.3% |
| `union_news_both_h1` | `ideal` | +4.14% | $10,413.83 | +0.00% | 0 | 21/23 | 3 | 2 | 0 | 3 | 8.7% |
| `union_news_both_h1` | `limit_prior` | +4.19% | $10,419.01 | +0.00% | 0 | 21/23 | 3 | 2 | 0 | 3 | 8.7% |
| `union_news_both_h1` | `limit_open` | +4.14% | $10,413.83 | +0.00% | 0 | 21/23 | 3 | 2 | 0 | 3 | 8.7% |
| `union_news_both_h1` | `market_mid` | +1.36% | $10,135.67 | -0.48% | 11 | 21/23 | 3 | 2 | 0 | 3 | 4.3% |
| `union_news_both_h1` | `market_adverse` | -2.61% | $9,738.57 | -2.61% | 14 | 0/23 | 3 | 2 | 0 | 3 | 0.0% |
| `union_news_both_h1` | `market_favorable` | +7.74% | $10,773.50 | +0.00% | 0 | 21/23 | 3 | 2 | 0 | 3 | 13.0% |
| `union_news_both_h1` | `partial_50` | +0.41% | $10,040.98 | -0.60% | 10 | 21/23 | 8 | 2 | 7 | 3 | 17.4% |
| `union_news_both_h1` | `gap_miss` | +4.14% | $10,413.83 | +0.00% | 0 | 21/23 | 3 | 2 | 0 | 3 | 8.7% |
| `union_news_g_cam71_h1` | `ideal` | -4.86% | $9,514.14 | -4.86% | 7 | 0/23 | 49 | 2 | 0 | 4 | 30.4% |
| `union_news_g_cam71_h1` | `limit_prior` | +2.88% | $10,288.16 | -0.01% | 1 | 13/23 | 37 | 12 | 0 | 15 | 34.8% |
| `union_news_g_cam71_h1` | `limit_open` | -7.03% | $9,297.21 | -7.03% | 7 | 0/23 | 49 | 4 | 0 | 6 | 26.1% |
| `union_news_g_cam71_h1` | `market_mid` | -16.34% | $8,366.18 | -16.34% | 15 | 0/23 | 49 | 2 | 0 | 4 | 17.4% |
| `union_news_g_cam71_h1` | `market_adverse` | -34.77% | $6,523.22 | -34.77% | 18 | 0/23 | 49 | 2 | 0 | 4 | 0.0% |
| `union_news_g_cam71_h1` | `market_favorable` | +27.03% | $12,702.85 | +0.00% | 0 | 21/23 | 49 | 2 | 0 | 4 | 43.5% |
| `union_news_g_cam71_h1` | `partial_50` | -10.13% | $8,986.57 | -10.13% | 15 | 0/23 | 111 | 6 | 89 | 9 | 17.4% |
| `union_news_g_cam71_h1` | `gap_miss` | -4.86% | $9,514.14 | -4.86% | 7 | 0/23 | 49 | 2 | 0 | 4 | 30.4% |
| `union_news_g_conv_h1` | `ideal` | +5.23% | $10,522.67 | +0.00% | 0 | 3/23 | 72 | 8 | 0 | 50 | 39.1% |
| `union_news_g_conv_h1` | `limit_prior` | -1.62% | $9,838.38 | -1.62% | 11 | 0/23 | 58 | 21 | 0 | 64 | 34.8% |
| `union_news_g_conv_h1` | `limit_open` | +0.53% | $10,052.47 | -0.24% | 1 | 3/23 | 70 | 11 | 0 | 53 | 34.8% |
| `union_news_g_conv_h1` | `market_mid` | -12.05% | $8,794.67 | -12.05% | 12 | 0/23 | 72 | 8 | 0 | 50 | 26.1% |
| `union_news_g_conv_h1` | `market_adverse` | -35.99% | $6,401.10 | -35.99% | 22 | 0/23 | 70 | 8 | 0 | 51 | 4.3% |
| `union_news_g_conv_h1` | `market_favorable` | +46.52% | $14,651.54 | +0.00% | 0 | 21/23 | 74 | 8 | 0 | 49 | 65.2% |
| `union_news_g_conv_h1` | `partial_50` | -8.71% | $9,129.53 | -8.71% | 18 | 0/23 | 194 | 14 | 163 | 56 | 21.7% |
| `union_news_g_conv_h1` | `gap_miss` | +5.23% | $10,522.67 | +0.00% | 0 | 3/23 | 72 | 8 | 0 | 50 | 39.1% |
| `union_e_green_h3` | `ideal` | -6.36% | $9,364.42 | -10.28% | 13 | 4/23 | 65 | 0 | 0 | 98 | 30.4% |
| `union_e_green_h3` | `limit_prior` | -26.25% | $7,374.83 | -29.46% | 22 | 5/23 | 71 | 16 | 0 | 106 | 26.1% |
| `union_e_green_h3` | `limit_open` | -6.36% | $9,364.24 | -10.29% | 13 | 4/23 | 63 | 1 | 0 | 97 | 30.4% |
| `union_e_green_h3` | `market_mid` | -25.71% | $7,428.55 | -27.13% | 15 | 4/23 | 53 | 0 | 0 | 92 | 26.1% |
| `union_e_green_h3` | `market_adverse` | -51.21% | $4,879.21 | -51.21% | 16 | 0/23 | 55 | 0 | 0 | 93 | 17.4% |
| `union_e_green_h3` | `market_favorable` | +73.85% | $17,384.87 | +0.00% | 0 | 21/23 | 81 | 0 | 0 | 106 | 56.5% |
| `union_e_green_h3` | `partial_50` | -6.73% | $9,327.22 | -7.47% | 10 | 4/23 | 304 | 24 | 268 | 138 | 30.4% |
| `union_e_green_h3` | `gap_miss` | +5.08% | $10,508.00 | +0.00% | 0 | 11/23 | 71 | 5 | 0 | 101 | 39.1% |
| `flatten_h5` | `ideal` | +7.99% | $10,799.08 | +0.00% | 0 | 3/23 | 90 | 12 | 0 | 275 | 60.9% |
| `flatten_h5` | `limit_prior` | +2.50% | $10,250.07 | +0.00% | 0 | 1/23 | 90 | 33 | 0 | 281 | 52.2% |
| `flatten_h5` | `limit_open` | +5.13% | $10,512.59 | +0.00% | 0 | 1/23 | 103 | 18 | 0 | 295 | 52.2% |
| `flatten_h5` | `market_mid` | -0.84% | $9,916.15 | -0.84% | 4 | 0/23 | 78 | 12 | 0 | 259 | 43.5% |
| `flatten_h5` | `market_adverse` | -15.41% | $8,459.39 | -15.41% | 16 | 0/23 | 76 | 12 | 0 | 258 | 43.5% |
| `flatten_h5` | `market_favorable` | +27.84% | $12,783.65 | +0.00% | 0 | 13/23 | 92 | 12 | 0 | 274 | 65.2% |
| `flatten_h5` | `partial_50` | -5.55% | $9,444.76 | -5.55% | 7 | 0/23 | 323 | 25 | 273 | 324 | 39.1% |
| `flatten_h5` | `gap_miss` | +6.02% | $10,602.12 | +0.00% | 0 | 1/23 | 88 | 13 | 0 | 270 | 60.9% |
| `flatten_h5_rankw` | `ideal` | +3.89% | $10,388.70 | +0.00% | 0 | 1/23 | 85 | 10 | 0 | 264 | 47.8% |
| `flatten_h5_rankw` | `limit_prior` | +2.00% | $10,200.43 | +0.00% | 0 | 2/23 | 93 | 31 | 0 | 282 | 43.5% |
| `flatten_h5_rankw` | `limit_open` | +1.10% | $10,110.25 | +0.00% | 0 | 1/23 | 106 | 16 | 0 | 296 | 47.8% |
| `flatten_h5_rankw` | `market_mid` | -3.60% | $9,639.93 | -3.60% | 6 | 0/23 | 75 | 10 | 0 | 251 | 34.8% |
| `flatten_h5_rankw` | `market_adverse` | -16.41% | $8,358.99 | -16.41% | 23 | 0/23 | 52 | 0 | 0 | 203 | 34.8% |
| `flatten_h5_rankw` | `market_favorable` | +24.27% | $12,426.71 | +0.00% | 0 | 15/23 | 94 | 12 | 0 | 277 | 56.5% |
| `flatten_h5_rankw` | `partial_50` | -8.67% | $9,132.98 | -8.67% | 14 | 0/23 | 312 | 25 | 266 | 323 | 26.1% |
| `flatten_h5_rankw` | `gap_miss` | +2.43% | $10,243.30 | +0.00% | 0 | 1/23 | 83 | 11 | 0 | 257 | 47.8% |
| `flatten_h5_time` | `ideal` | +7.99% | $10,799.08 | +0.00% | 0 | 3/23 | 90 | 12 | 0 | 275 | 60.9% |
| `flatten_h5_time` | `limit_prior` | +2.50% | $10,250.07 | +0.00% | 0 | 1/23 | 90 | 33 | 0 | 281 | 52.2% |
| `flatten_h5_time` | `limit_open` | +5.13% | $10,512.59 | +0.00% | 0 | 1/23 | 103 | 18 | 0 | 295 | 52.2% |
| `flatten_h5_time` | `market_mid` | -0.84% | $9,916.15 | -0.84% | 4 | 0/23 | 78 | 12 | 0 | 259 | 43.5% |
| `flatten_h5_time` | `market_adverse` | -15.41% | $8,459.39 | -15.41% | 16 | 0/23 | 76 | 12 | 0 | 258 | 43.5% |
| `flatten_h5_time` | `market_favorable` | +27.84% | $12,783.65 | +0.00% | 0 | 13/23 | 92 | 12 | 0 | 274 | 65.2% |
| `flatten_h5_time` | `partial_50` | -5.55% | $9,444.76 | -5.55% | 7 | 0/23 | 323 | 26 | 273 | 325 | 39.1% |
| `flatten_h5_time` | `gap_miss` | +6.02% | $10,602.12 | +0.00% | 0 | 1/23 | 88 | 13 | 0 | 270 | 60.9% |
| `flatten_h5_sboost` | `ideal` | +5.54% | $10,553.66 | +0.00% | 0 | 2/23 | 94 | 12 | 0 | 281 | 60.9% |
| `flatten_h5_sboost` | `limit_prior` | +1.58% | $10,158.46 | +0.00% | 0 | 1/23 | 92 | 34 | 0 | 287 | 52.2% |
| `flatten_h5_sboost` | `limit_open` | +4.34% | $10,433.79 | +0.00% | 0 | 1/23 | 104 | 16 | 0 | 295 | 52.2% |
| `flatten_h5_sboost` | `market_mid` | -1.36% | $9,864.20 | -1.36% | 5 | 0/23 | 73 | 10 | 0 | 250 | 43.5% |
| `flatten_h5_sboost` | `market_adverse` | -15.90% | $8,410.43 | -15.90% | 19 | 0/23 | 70 | 0 | 0 | 230 | 43.5% |
| `flatten_h5_sboost` | `market_favorable` | +26.77% | $12,677.19 | +0.00% | 0 | 13/23 | 98 | 12 | 0 | 285 | 65.2% |
| `flatten_h5_sboost` | `partial_50` | -6.24% | $9,376.44 | -6.24% | 9 | 0/23 | 335 | 25 | 283 | 332 | 39.1% |
| `flatten_h5_sboost` | `gap_miss` | +5.44% | $10,544.40 | +0.00% | 0 | 1/23 | 92 | 13 | 0 | 276 | 60.9% |
| `union_h5_sboost` | `ideal` | +3.27% | $10,327.20 | +0.00% | 0 | 2/23 | 126 | 12 | 0 | 372 | 60.9% |
| `union_h5_sboost` | `limit_prior` | -2.52% | $9,748.51 | -3.20% | 5 | 0/23 | 120 | 38 | 0 | 371 | 52.2% |
| `union_h5_sboost` | `limit_open` | +1.00% | $10,100.21 | +0.00% | 0 | 1/23 | 139 | 19 | 0 | 393 | 56.5% |
| `union_h5_sboost` | `market_mid` | -4.82% | $9,517.79 | -4.82% | 6 | 0/23 | 96 | 12 | 0 | 331 | 43.5% |
| `union_h5_sboost` | `market_adverse` | -18.22% | $8,178.39 | -18.22% | 20 | 0/23 | 94 | 12 | 0 | 328 | 43.5% |
| `union_h5_sboost` | `market_favorable` | +24.71% | $12,471.35 | +0.00% | 0 | 19/23 | 130 | 12 | 0 | 374 | 69.6% |
| `union_h5_sboost` | `partial_50` | -6.26% | $9,374.39 | -6.26% | 9 | 0/23 | 418 | 35 | 355 | 429 | 39.1% |
| `union_h5_sboost` | `gap_miss` | +3.21% | $10,321.32 | +0.00% | 0 | 2/23 | 124 | 13 | 0 | 369 | 56.5% |
| `flatten_live_h1_sizeup` | `ideal` | +8.22% | $10,821.57 | +0.00% | 0 | 7/23 | 48 | 0 | 0 | 0 | 13.0% |
| `flatten_live_h1_sizeup` | `limit_prior` | +7.88% | $10,788.48 | +0.00% | 0 | 7/23 | 40 | 8 | 0 | 8 | 17.4% |
| `flatten_live_h1_sizeup` | `limit_open` | +7.84% | $10,784.48 | +0.00% | 0 | 7/23 | 48 | 2 | 0 | 2 | 17.4% |
| `flatten_live_h1_sizeup` | `market_mid` | -0.23% | $9,977.26 | -0.23% | 6 | 0/23 | 48 | 0 | 0 | 0 | 13.0% |
| `flatten_live_h1_sizeup` | `market_adverse` | -14.76% | $8,523.64 | -14.76% | 18 | 0/23 | 48 | 0 | 0 | 0 | 0.0% |
| `flatten_live_h1_sizeup` | `market_favorable` | +24.22% | $12,421.60 | +0.00% | 0 | 17/23 | 48 | 0 | 0 | 0 | 21.7% |
| `flatten_live_h1_sizeup` | `partial_50` | -2.16% | $9,784.31 | -2.17% | 6 | 0/23 | 155 | 16 | 139 | 16 | 26.1% |
| `flatten_live_h1_sizeup` | `gap_miss` | +3.51% | $10,350.59 | +0.00% | 0 | 6/23 | 46 | 1 | 0 | 1 | 8.7% |
| `union_h3_cut` | `ideal` | +4.07% | $10,406.49 | +0.00% | 0 | 5/23 | 124 | 0 | 0 | 229 | 52.2% |
| `union_h3_cut` | `limit_prior` | -4.49% | $9,550.56 | -4.50% | 10 | 0/23 | 116 | 32 | 0 | 241 | 43.5% |
| `union_h3_cut` | `limit_open` | -5.24% | $9,476.14 | -5.24% | 7 | 0/23 | 127 | 10 | 0 | 238 | 47.8% |
| `union_h3_cut` | `market_mid` | -5.58% | $9,441.72 | -5.58% | 9 | 0/23 | 108 | 0 | 0 | 219 | 39.1% |
| `union_h3_cut` | `market_adverse` | -23.90% | $7,609.67 | -23.90% | 23 | 0/23 | 94 | 0 | 0 | 214 | 34.8% |
| `union_h3_cut` | `market_favorable` | +39.95% | $13,994.88 | +0.00% | 0 | 20/23 | 126 | 0 | 0 | 228 | 78.3% |
| `union_h3_cut` | `partial_50` | -9.21% | $9,079.20 | -9.21% | 15 | 0/23 | 444 | 25 | 380 | 259 | 39.1% |
| `union_h3_cut` | `gap_miss` | +4.99% | $10,499.15 | +0.00% | 0 | 5/23 | 124 | 1 | 0 | 227 | 52.2% |
| `union_h1_topheavy` | `ideal` | +7.66% | $10,765.86 | +0.00% | 0 | 7/23 | 156 | 15 | 0 | 103 | 39.1% |
| `union_h1_topheavy` | `limit_prior` | +5.11% | $10,511.27 | +0.00% | 0 | 6/23 | 124 | 36 | 0 | 125 | 34.8% |
| `union_h1_topheavy` | `limit_open` | +2.40% | $10,240.02 | -0.32% | 2 | 6/23 | 149 | 24 | 0 | 113 | 34.8% |
| `union_h1_topheavy` | `market_mid` | -9.78% | $9,021.85 | -9.78% | 22 | 0/23 | 156 | 15 | 0 | 103 | 21.7% |
| `union_h1_topheavy` | `market_adverse` | -35.41% | $6,458.86 | -35.41% | 23 | 0/23 | 156 | 15 | 0 | 103 | 0.0% |
| `union_h1_topheavy` | `market_favorable` | +61.79% | $16,178.95 | +0.00% | 0 | 21/23 | 156 | 15 | 0 | 103 | 65.2% |
| `union_h1_topheavy` | `partial_50` | -9.39% | $9,060.86 | -9.39% | 23 | 0/23 | 482 | 34 | 417 | 119 | 34.8% |
| `union_h1_topheavy` | `gap_miss` | +4.36% | $10,436.31 | +0.00% | 0 | 6/23 | 154 | 16 | 0 | 104 | 34.8% |
| `union_h1` | `ideal` | +4.63% | $10,463.52 | +0.00% | 0 | — | 158 | 15 | 0 | 102 | 34.8% |
| `union_h1` | `limit_prior` | +3.37% | $10,336.68 | +0.00% | 0 | — | 126 | 36 | 0 | 124 | 43.5% |
| `union_h1` | `limit_open` | +0.14% | $10,013.55 | -0.24% | 2 | — | 151 | 24 | 0 | 112 | 21.7% |
| `union_h1` | `market_mid` | -14.70% | $8,529.88 | -14.70% | 23 | — | 158 | 15 | 0 | 102 | 17.4% |
| `union_h1` | `market_adverse` | -41.32% | $5,868.27 | -41.32% | 23 | — | 156 | 15 | 0 | 103 | 0.0% |
| `union_h1` | `market_favorable` | +75.73% | $17,573.22 | +0.00% | 0 | — | 158 | 15 | 0 | 102 | 69.6% |
| `union_h1` | `partial_50` | -9.88% | $9,012.01 | -9.88% | 23 | — | 497 | 34 | 434 | 119 | 21.7% |
| `union_h1` | `gap_miss` | -0.07% | $9,992.98 | -0.07% | 3 | — | 156 | 16 | 0 | 103 | 30.4% |
| `union_h3` | `ideal` | +2.45% | $10,244.95 | +0.00% | 0 | — | 124 | 0 | 0 | 228 | 52.2% |
| `union_h3` | `limit_prior` | -6.40% | $9,359.74 | -6.47% | 10 | — | 116 | 33 | 0 | 241 | 43.5% |
| `union_h3` | `limit_open` | -5.24% | $9,476.14 | -5.24% | 7 | — | 127 | 9 | 0 | 237 | 47.8% |
| `union_h3` | `market_mid` | -6.86% | $9,313.90 | -6.86% | 9 | — | 108 | 0 | 0 | 218 | 39.1% |
| `union_h3` | `market_adverse` | -24.52% | $7,547.77 | -24.52% | 23 | — | 94 | 0 | 0 | 213 | 34.8% |
| `union_h3` | `market_favorable` | +38.03% | $13,803.43 | +0.00% | 0 | — | 126 | 0 | 0 | 227 | 73.9% |
| `union_h3` | `partial_50` | -9.54% | $9,045.58 | -9.54% | 15 | — | 443 | 25 | 379 | 259 | 39.1% |
| `union_h3` | `gap_miss` | +3.51% | $10,351.27 | +0.00% | 0 | — | 124 | 1 | 0 | 226 | 52.2% |
| `union_h5` | `ideal` | +3.83% | $10,382.58 | +0.00% | 0 | — | 118 | 12 | 0 | 355 | 60.9% |
| `union_h5` | `limit_prior` | -1.27% | $9,873.04 | -1.97% | 4 | — | 114 | 37 | 0 | 356 | 47.8% |
| `union_h5` | `limit_open` | +2.00% | $10,199.72 | +0.00% | 0 | — | 129 | 19 | 0 | 374 | 56.5% |
| `union_h5` | `market_mid` | -4.24% | $9,576.00 | -4.24% | 6 | — | 100 | 12 | 0 | 332 | 43.5% |
| `union_h5` | `market_adverse` | -17.77% | $8,223.52 | -17.77% | 18 | — | 98 | 12 | 0 | 329 | 43.5% |
| `union_h5` | `market_favorable` | +26.01% | $12,601.09 | +0.00% | 0 | — | 120 | 12 | 0 | 354 | 69.6% |
| `union_h5` | `partial_50` | -5.01% | $9,499.04 | -5.01% | 7 | — | 388 | 35 | 330 | 409 | 39.1% |
| `union_h5` | `gap_miss` | +3.77% | $10,376.71 | +0.00% | 0 | — | 116 | 13 | 0 | 352 | 56.5% |
| `flatten_h1` | `ideal` | +6.53% | $10,653.11 | +0.00% | 0 | — | 126 | 11 | 0 | 73 | 34.8% |
| `flatten_h1` | `limit_prior` | +6.95% | $10,695.40 | +0.00% | 0 | — | 100 | 28 | 0 | 91 | 47.8% |
| `flatten_h1` | `limit_open` | +3.76% | $10,375.74 | -0.24% | 2 | — | 121 | 19 | 0 | 82 | 26.1% |
| `flatten_h1` | `market_mid` | -10.87% | $8,913.15 | -10.87% | 23 | — | 126 | 11 | 0 | 73 | 17.4% |
| `flatten_h1` | `market_adverse` | -36.49% | $6,350.64 | -36.49% | 23 | — | 126 | 11 | 0 | 73 | 0.0% |
| `flatten_h1` | `market_favorable` | +60.88% | $16,088.03 | +0.00% | 0 | — | 126 | 11 | 0 | 73 | 60.9% |
| `flatten_h1` | `partial_50` | -7.59% | $9,241.14 | -7.59% | 23 | — | 404 | 32 | 355 | 93 | 30.4% |
| `flatten_h1` | `gap_miss` | +1.79% | $10,179.27 | +0.00% | 0 | — | 124 | 12 | 0 | 74 | 30.4% |
| `probable_h1` | `ideal` | -2.11% | $9,788.75 | -2.95% | 13 | — | 152 | 16 | 0 | 104 | 21.7% |
| `probable_h1` | `limit_prior` | -4.10% | $9,590.18 | -4.10% | 14 | — | 125 | 35 | 0 | 123 | 34.8% |
| `probable_h1` | `limit_open` | -6.98% | $9,302.04 | -6.98% | 21 | — | 147 | 21 | 0 | 109 | 17.4% |
| `probable_h1` | `market_mid` | -20.05% | $7,995.14 | -20.05% | 22 | — | 152 | 16 | 0 | 104 | 8.7% |
| `probable_h1` | `market_adverse` | -45.47% | $5,452.87 | -45.47% | 22 | — | 152 | 16 | 0 | 104 | 0.0% |
| `probable_h1` | `market_favorable` | +91.59% | $19,159.00 | +0.00% | 0 | — | 152 | 16 | 0 | 104 | 65.2% |
| `probable_h1` | `partial_50` | -13.38% | $8,661.80 | -13.38% | 22 | — | 486 | 42 | 428 | 130 | 8.7% |
| `probable_h1` | `gap_miss` | -5.01% | $9,498.49 | -5.01% | 21 | — | 142 | 21 | 0 | 109 | 21.7% |
| `probable_h3` | `ideal` | -5.38% | $9,462.07 | -5.59% | 20 | — | 112 | 0 | 0 | 227 | 30.4% |
| `probable_h3` | `limit_prior` | -10.42% | $8,958.10 | -10.42% | 20 | — | 119 | 25 | 0 | 241 | 26.1% |
| `probable_h3` | `limit_open` | -10.66% | $8,933.95 | -10.66% | 22 | — | 133 | 7 | 0 | 241 | 21.7% |
| `probable_h3` | `market_mid` | -15.37% | $8,462.88 | -15.37% | 22 | — | 106 | 0 | 0 | 224 | 21.7% |
| `probable_h3` | `market_adverse` | -31.21% | $6,879.40 | -31.21% | 22 | — | 96 | 0 | 0 | 219 | 17.4% |
| `probable_h3` | `market_favorable` | +36.87% | $13,686.87 | -1.89% | 1 | — | 140 | 0 | 0 | 241 | 60.9% |
| `probable_h3` | `partial_50` | -14.88% | $8,511.89 | -14.88% | 22 | — | 426 | 30 | 373 | 269 | 26.1% |
| `probable_h3` | `gap_miss` | -8.97% | $9,103.19 | -9.15% | 22 | — | 130 | 5 | 0 | 236 | 26.1% |
| `probable_h5` | `ideal` | -16.93% | $8,306.51 | -17.07% | 22 | — | 113 | 15 | 0 | 358 | 26.1% |
| `probable_h5` | `limit_prior` | -18.15% | $8,185.26 | -18.15% | 20 | — | 113 | 34 | 0 | 363 | 30.4% |
| `probable_h5` | `limit_open` | -18.48% | $8,151.59 | -18.48% | 22 | — | 123 | 20 | 0 | 375 | 17.4% |
| `probable_h5` | `market_mid` | -22.74% | $7,725.94 | -22.83% | 22 | — | 109 | 15 | 0 | 352 | 26.1% |
| `probable_h5` | `market_adverse` | -32.54% | $6,746.27 | -32.81% | 22 | — | 85 | 15 | 0 | 317 | 26.1% |
| `probable_h5` | `market_favorable` | +5.28% | $10,528.38 | -5.51% | 6 | — | 118 | 15 | 0 | 365 | 43.5% |
| `probable_h5` | `partial_50` | -14.64% | $8,536.41 | -14.64% | 22 | — | 373 | 37 | 324 | 408 | 21.7% |
| `probable_h5` | `gap_miss` | -16.91% | $8,309.31 | -17.07% | 22 | — | 117 | 17 | 0 | 364 | 26.1% |
| `yday_gainer_h1` | `ideal` | +5.60% | $10,560.30 | -2.78% | 5 | — | 152 | 16 | 0 | 104 | 30.4% |
| `yday_gainer_h1` | `limit_prior` | +2.87% | $10,286.83 | -2.21% | 5 | — | 129 | 32 | 0 | 120 | 39.1% |
| `yday_gainer_h1` | `limit_open` | -1.62% | $9,838.18 | -3.70% | 10 | — | 147 | 22 | 0 | 110 | 26.1% |
| `yday_gainer_h1` | `market_mid` | -16.09% | $8,390.55 | -16.09% | 22 | — | 152 | 16 | 0 | 104 | 13.0% |
| `yday_gainer_h1` | `market_adverse` | -45.23% | $5,476.70 | -45.23% | 22 | — | 152 | 16 | 0 | 104 | 0.0% |
| `yday_gainer_h1` | `market_favorable` | +118.34% | $21,834.02 | +0.00% | 0 | — | 152 | 16 | 0 | 104 | 65.2% |
| `yday_gainer_h1` | `partial_50` | -11.31% | $8,868.61 | -11.31% | 22 | — | 501 | 42 | 444 | 129 | 13.0% |
| `yday_gainer_h1` | `gap_miss` | -1.37% | $9,863.06 | -2.78% | 11 | — | 142 | 21 | 0 | 109 | 26.1% |
| `yday_gainer_h3` | `ideal` | -5.08% | $9,491.88 | -5.29% | 19 | — | 110 | 0 | 0 | 226 | 34.8% |
| `yday_gainer_h3` | `limit_prior` | -6.24% | $9,376.35 | -6.24% | 19 | — | 117 | 22 | 0 | 239 | 34.8% |
| `yday_gainer_h3` | `limit_open` | -10.46% | $8,954.18 | -10.46% | 22 | — | 137 | 8 | 0 | 244 | 34.8% |
| `yday_gainer_h3` | `market_mid` | -14.78% | $8,522.13 | -14.78% | 22 | — | 90 | 0 | 0 | 216 | 21.7% |
| `yday_gainer_h3` | `market_adverse` | -30.51% | $6,948.95 | -30.51% | 22 | — | 86 | 0 | 0 | 214 | 17.4% |
| `yday_gainer_h3` | `market_favorable` | +41.24% | $14,123.80 | +0.00% | 0 | — | 142 | 0 | 0 | 242 | 65.2% |
| `yday_gainer_h3` | `partial_50` | -12.11% | $8,788.81 | -12.11% | 22 | — | 442 | 30 | 390 | 266 | 30.4% |
| `yday_gainer_h3` | `gap_miss` | -5.91% | $9,409.47 | -6.11% | 20 | — | 112 | 5 | 0 | 227 | 34.8% |
| `yday_gainer_h5` | `ideal` | -3.92% | $9,607.69 | -5.72% | 14 | — | 93 | 15 | 0 | 328 | 39.1% |
| `yday_gainer_h5` | `limit_prior` | -8.40% | $9,159.52 | -8.40% | 20 | — | 112 | 34 | 0 | 365 | 43.5% |
| `yday_gainer_h5` | `limit_open` | -8.80% | $9,120.15 | -9.00% | 21 | — | 127 | 23 | 0 | 384 | 43.5% |
| `yday_gainer_h5` | `market_mid` | -11.83% | $8,816.60 | -13.40% | 21 | — | 86 | 15 | 0 | 319 | 34.8% |
| `yday_gainer_h5` | `market_adverse` | -25.95% | $7,404.96 | -26.83% | 22 | — | 65 | 15 | 0 | 287 | 34.8% |
| `yday_gainer_h5` | `market_favorable` | +23.42% | $12,342.02 | -0.71% | 1 | — | 126 | 15 | 0 | 377 | 52.2% |
| `yday_gainer_h5` | `partial_50` | -10.28% | $8,971.60 | -10.28% | 22 | — | 387 | 37 | 340 | 399 | 34.8% |
| `yday_gainer_h5` | `gap_miss` | -7.87% | $9,213.10 | -8.54% | 21 | — | 102 | 17 | 0 | 341 | 39.1% |
| `ohlc_hot_h1` | `ideal` | -8.14% | $9,185.84 | -9.42% | 22 | — | 140 | 13 | 0 | 90 | 39.1% |
| `ohlc_hot_h1` | `limit_prior` | -5.39% | $9,460.55 | -7.26% | 20 | — | 121 | 35 | 0 | 113 | 34.8% |
| `ohlc_hot_h1` | `limit_open` | -13.35% | $8,665.15 | -13.99% | 22 | — | 138 | 20 | 0 | 97 | 39.1% |
| `ohlc_hot_h1` | `market_mid` | -25.54% | $7,446.46 | -25.54% | 22 | — | 140 | 13 | 0 | 90 | 13.0% |
| `ohlc_hot_h1` | `market_adverse` | -49.45% | $5,055.53 | -49.45% | 22 | — | 140 | 13 | 0 | 90 | 4.3% |
| `ohlc_hot_h1` | `market_favorable` | +65.61% | $16,560.59 | +0.00% | 0 | — | 140 | 13 | 0 | 90 | 73.9% |
| `ohlc_hot_h1` | `partial_50` | -15.97% | $8,402.78 | -15.97% | 22 | — | 419 | 39 | 367 | 114 | 8.7% |
| `ohlc_hot_h1` | `gap_miss` | -7.38% | $9,262.23 | -9.03% | 22 | — | 138 | 15 | 0 | 92 | 34.8% |
| `ohlc_hot_h3` | `ideal` | -2.20% | $9,779.90 | -2.70% | 12 | — | 110 | 0 | 0 | 194 | 39.1% |
| `ohlc_hot_h3` | `limit_prior` | -9.67% | $9,032.96 | -9.92% | 20 | — | 117 | 30 | 0 | 218 | 34.8% |
| `ohlc_hot_h3` | `limit_open` | -11.58% | $8,841.57 | -11.77% | 21 | — | 114 | 8 | 0 | 205 | 34.8% |
| `ohlc_hot_h3` | `market_mid` | -13.58% | $8,641.81 | -13.58% | 21 | — | 110 | 0 | 0 | 194 | 30.4% |
| `ohlc_hot_h3` | `market_adverse` | -34.31% | $6,569.07 | -34.31% | 22 | — | 106 | 0 | 0 | 196 | 26.1% |
| `ohlc_hot_h3` | `market_favorable` | +37.62% | $13,762.47 | +0.00% | 0 | — | 122 | 0 | 0 | 198 | 60.9% |
| `ohlc_hot_h3` | `partial_50` | -15.98% | $8,402.41 | -15.98% | 22 | — | 376 | 30 | 325 | 232 | 21.7% |
| `ohlc_hot_h3` | `gap_miss` | -3.38% | $9,661.54 | -3.59% | 13 | — | 124 | 2 | 0 | 199 | 39.1% |
| `ohlc_hot_h5` | `ideal` | -9.06% | $9,094.44 | -11.10% | 21 | — | 109 | 15 | 0 | 320 | 39.1% |
| `ohlc_hot_h5` | `limit_prior` | -16.93% | $8,307.21 | -17.77% | 21 | — | 104 | 39 | 0 | 326 | 26.1% |
| `ohlc_hot_h5` | `limit_open` | -10.57% | $8,942.81 | -11.66% | 21 | — | 108 | 19 | 0 | 320 | 34.8% |
| `ohlc_hot_h5` | `market_mid` | -19.92% | $8,007.61 | -21.98% | 21 | — | 98 | 15 | 0 | 308 | 26.1% |
| `ohlc_hot_h5` | `market_adverse` | -32.12% | $6,787.90 | -33.39% | 22 | — | 102 | 15 | 0 | 312 | 30.4% |
| `ohlc_hot_h5` | `market_favorable` | +10.67% | $11,067.34 | -1.43% | 2 | — | 118 | 15 | 0 | 331 | 47.8% |
| `ohlc_hot_h5` | `partial_50` | -15.64% | $8,436.22 | -16.04% | 22 | — | 322 | 37 | 274 | 363 | 21.7% |
| `ohlc_hot_h5` | `gap_miss` | -10.49% | $8,950.53 | -11.88% | 21 | — | 114 | 18 | 0 | 328 | 39.1% |
| `union_vol_g_h1` | `ideal` | -2.12% | $9,788.29 | -5.42% | 12 | — | 142 | 15 | 0 | 84 | 30.4% |
| `union_vol_g_h1` | `limit_prior` | +5.02% | $10,502.10 | -2.83% | 5 | — | 125 | 29 | 0 | 99 | 34.8% |
| `union_vol_g_h1` | `limit_open` | -4.56% | $9,543.71 | -5.80% | 13 | — | 138 | 19 | 0 | 88 | 30.4% |
| `union_vol_g_h1` | `market_mid` | -23.06% | $7,693.93 | -23.06% | 22 | — | 142 | 15 | 0 | 84 | 21.7% |
| `union_vol_g_h1` | `market_adverse` | -50.39% | $4,961.21 | -50.39% | 22 | — | 142 | 15 | 0 | 84 | 0.0% |
| `union_vol_g_h1` | `market_favorable` | +133.58% | $23,357.62 | +0.00% | 0 | — | 142 | 15 | 0 | 84 | 65.2% |
| `union_vol_g_h1` | `partial_50` | -13.58% | $8,642.39 | -13.58% | 22 | — | 452 | 39 | 402 | 104 | 21.7% |
| `union_vol_g_h1` | `gap_miss` | -6.46% | $9,354.09 | -6.46% | 17 | — | 138 | 17 | 0 | 86 | 26.1% |
| `union_vol_g_h3` | `ideal` | -8.57% | $9,142.80 | -8.57% | 20 | — | 114 | 0 | 0 | 200 | 39.1% |
| `union_vol_g_h3` | `limit_prior` | -1.84% | $9,815.62 | -4.09% | 10 | — | 125 | 20 | 0 | 222 | 43.5% |
| `union_vol_g_h3` | `limit_open` | -9.29% | $9,070.99 | -9.29% | 22 | — | 130 | 7 | 0 | 214 | 34.8% |
| `union_vol_g_h3` | `market_mid` | -20.24% | $7,975.62 | -20.24% | 22 | — | 112 | 0 | 0 | 199 | 30.4% |
| `union_vol_g_h3` | `market_adverse` | -37.65% | $6,235.22 | -37.65% | 22 | — | 112 | 0 | 0 | 199 | 26.1% |
| `union_vol_g_h3` | `market_favorable` | +35.63% | $13,562.67 | -0.79% | 1 | — | 134 | 0 | 0 | 210 | 69.6% |
| `union_vol_g_h3` | `partial_50` | -13.56% | $8,644.19 | -13.56% | 22 | — | 402 | 28 | 354 | 231 | 26.1% |
| `union_vol_g_h3` | `gap_miss` | -6.47% | $9,352.76 | -6.47% | 20 | — | 114 | 3 | 0 | 201 | 39.1% |
| `union_vol_missing_h1` | `ideal` | +1.44% | $10,143.91 | +0.00% | 0 | — | 16 | 0 | 0 | 0 | 4.3% |
| `union_vol_missing_h1` | `limit_prior` | +0.76% | $10,075.47 | +0.00% | 0 | — | 10 | 4 | 0 | 4 | 8.7% |
| `union_vol_missing_h1` | `limit_open` | +1.29% | $10,128.77 | +0.00% | 0 | — | 14 | 2 | 0 | 2 | 4.3% |
| `union_vol_missing_h1` | `market_mid` | -1.23% | $9,877.02 | -1.23% | 23 | — | 16 | 0 | 0 | 0 | 0.0% |
| `union_vol_missing_h1` | `market_adverse` | -6.32% | $9,367.57 | -6.32% | 23 | — | 16 | 0 | 0 | 0 | 0.0% |
| `union_vol_missing_h1` | `market_favorable` | +8.87% | $10,886.78 | +0.00% | 0 | — | 16 | 0 | 0 | 0 | 8.7% |
| `union_vol_missing_h1` | `partial_50` | -0.16% | $9,983.75 | -0.29% | 22 | — | 58 | 0 | 50 | 0 | 17.4% |
| `union_vol_missing_h1` | `gap_miss` | +1.44% | $10,143.91 | +0.00% | 0 | — | 16 | 0 | 0 | 0 | 4.3% |
| `union_vol_missing_h3` | `ideal` | +3.58% | $10,358.15 | +0.00% | 0 | — | 16 | 0 | 0 | 16 | 13.0% |
| `union_vol_missing_h3` | `limit_prior` | +0.41% | $10,040.77 | +0.00% | 0 | — | 10 | 3 | 0 | 13 | 8.7% |
| `union_vol_missing_h3` | `limit_open` | +3.58% | $10,358.27 | +0.00% | 0 | — | 14 | 1 | 0 | 15 | 13.0% |
| `union_vol_missing_h3` | `market_mid` | +0.82% | $10,081.79 | -0.47% | 1 | — | 16 | 0 | 0 | 16 | 8.7% |
| `union_vol_missing_h3` | `market_adverse` | -4.26% | $9,573.52 | -4.26% | 23 | — | 16 | 0 | 0 | 16 | 8.7% |
| `union_vol_missing_h3` | `market_favorable` | +10.77% | $11,076.85 | +0.00% | 0 | — | 16 | 0 | 0 | 16 | 17.4% |
| `union_vol_missing_h3` | `partial_50` | +1.06% | $10,105.68 | -0.29% | 1 | — | 58 | 0 | 50 | 16 | 21.7% |
| `union_vol_missing_h3` | `gap_miss` | +3.58% | $10,358.15 | +0.00% | 0 | — | 16 | 0 | 0 | 16 | 13.0% |
| `union_ab_g_h1` | `ideal` | +2.75% | $10,275.34 | +0.00% | 0 | — | 110 | 15 | 0 | 86 | 21.7% |
| `union_ab_g_h1` | `limit_prior` | +3.98% | $10,397.63 | +0.00% | 0 | — | 92 | 27 | 0 | 99 | 26.1% |
| `union_ab_g_h1` | `limit_open` | +1.46% | $10,145.78 | +0.00% | 0 | — | 109 | 19 | 0 | 91 | 21.7% |
| `union_ab_g_h1` | `market_mid` | -10.09% | $8,990.95 | -10.09% | 12 | — | 110 | 15 | 0 | 86 | 13.0% |
| `union_ab_g_h1` | `market_adverse` | -30.71% | $6,929.06 | -30.71% | 18 | — | 110 | 15 | 0 | 86 | 0.0% |
| `union_ab_g_h1` | `market_favorable` | +40.47% | $14,046.60 | +0.00% | 0 | — | 110 | 15 | 0 | 86 | 52.2% |
| `union_ab_g_h1` | `partial_50` | -9.13% | $9,086.49 | -9.13% | 13 | — | 326 | 34 | 287 | 104 | 13.0% |
| `union_ab_g_h1` | `gap_miss` | -1.98% | $9,801.92 | -1.98% | 7 | — | 106 | 17 | 0 | 88 | 17.4% |
| `union_ab_g_h3` | `ideal` | -5.54% | $9,445.55 | -5.54% | 9 | — | 96 | 0 | 0 | 173 | 26.1% |
| `union_ab_g_h3` | `limit_prior` | -5.16% | $9,484.14 | -5.16% | 8 | — | 84 | 24 | 0 | 184 | 30.4% |
| `union_ab_g_h3` | `limit_open` | -8.71% | $9,128.69 | -8.71% | 12 | — | 95 | 7 | 0 | 181 | 21.7% |
| `union_ab_g_h3` | `market_mid` | -11.93% | $8,807.39 | -11.93% | 13 | — | 92 | 0 | 0 | 171 | 26.1% |
| `union_ab_g_h3` | `market_adverse` | -23.23% | $7,676.87 | -23.23% | 16 | — | 84 | 0 | 0 | 167 | 21.7% |
| `union_ab_g_h3` | `market_favorable` | +14.84% | $11,483.95 | +0.00% | 0 | — | 98 | 0 | 0 | 174 | 52.2% |
| `union_ab_g_h3` | `partial_50` | -10.63% | $8,936.58 | -10.63% | 12 | — | 289 | 21 | 250 | 191 | 17.4% |
| `union_ab_g_h3` | `gap_miss` | -5.02% | $9,497.78 | -5.02% | 9 | — | 92 | 2 | 0 | 171 | 26.1% |
| `union_join_g_h1` | `ideal` | +2.97% | $10,296.72 | +0.00% | 0 | — | 176 | 15 | 0 | 97 | 30.4% |
| `union_join_g_h1` | `limit_prior` | +4.11% | $10,410.93 | -0.75% | 3 | — | 140 | 37 | 0 | 120 | 39.1% |
| `union_join_g_h1` | `limit_open` | +0.93% | $10,092.89 | -0.81% | 2 | — | 169 | 22 | 0 | 105 | 26.1% |
| `union_join_g_h1` | `market_mid` | -19.08% | $8,091.58 | -19.08% | 23 | — | 176 | 15 | 0 | 97 | 13.0% |
| `union_join_g_h1` | `market_adverse` | -47.83% | $5,217.54 | -47.83% | 23 | — | 174 | 15 | 0 | 98 | 0.0% |
| `union_join_g_h1` | `market_favorable` | +79.06% | $17,905.67 | +0.00% | 0 | — | 176 | 15 | 0 | 97 | 69.6% |
| `union_join_g_h1` | `partial_50` | -12.01% | $8,798.81 | -12.01% | 23 | — | 526 | 32 | 456 | 110 | 26.1% |
| `union_join_g_h1` | `gap_miss` | -1.67% | $9,833.42 | -1.67% | 6 | — | 174 | 16 | 0 | 98 | 26.1% |
| `union_join_g_h3` | `ideal` | -3.85% | $9,615.26 | -3.85% | 8 | — | 134 | 0 | 0 | 236 | 30.4% |
| `union_join_g_h3` | `limit_prior` | -5.36% | $9,464.46 | -5.36% | 10 | — | 134 | 33 | 0 | 254 | 34.8% |
| `union_join_g_h3` | `limit_open` | -6.80% | $9,319.72 | -6.80% | 8 | — | 141 | 9 | 0 | 246 | 30.4% |
| `union_join_g_h3` | `market_mid` | -13.12% | $8,687.78 | -13.12% | 13 | — | 118 | 0 | 0 | 230 | 26.1% |
| `union_join_g_h3` | `market_adverse` | -29.97% | $7,002.55 | -29.97% | 23 | — | 104 | 0 | 0 | 225 | 26.1% |
| `union_join_g_h3` | `market_favorable` | +29.19% | $12,918.72 | +0.00% | 0 | — | 138 | 0 | 0 | 238 | 65.2% |
| `union_join_g_h3` | `partial_50` | -11.40% | $8,860.07 | -11.40% | 16 | — | 471 | 21 | 399 | 266 | 26.1% |
| `union_join_g_h3` | `gap_miss` | -2.20% | $9,779.75 | -2.20% | 6 | — | 132 | 1 | 0 | 237 | 34.8% |
| `union_join_present_h1` | `ideal` | +4.22% | $10,422.51 | +0.00% | 0 | — | 158 | 15 | 0 | 102 | 34.8% |
| `union_join_present_h1` | `limit_prior` | +2.64% | $10,264.20 | -0.75% | 3 | — | 126 | 37 | 0 | 125 | 39.1% |
| `union_join_present_h1` | `limit_open` | -0.14% | $9,986.42 | -0.81% | 5 | — | 151 | 24 | 0 | 112 | 26.1% |
| `union_join_present_h1` | `market_mid` | -15.06% | $8,493.46 | -15.06% | 23 | — | 158 | 15 | 0 | 102 | 17.4% |
| `union_join_present_h1` | `market_adverse` | -41.62% | $5,838.54 | -41.62% | 23 | — | 156 | 15 | 0 | 103 | 0.0% |
| `union_join_present_h1` | `market_favorable` | +75.23% | $17,522.75 | +0.00% | 0 | — | 158 | 15 | 0 | 102 | 69.6% |
| `union_join_present_h1` | `partial_50` | -10.22% | $8,978.29 | -10.22% | 23 | — | 491 | 34 | 428 | 119 | 26.1% |
| `union_join_present_h1` | `gap_miss` | -0.44% | $9,955.86 | -0.44% | 3 | — | 156 | 16 | 0 | 103 | 30.4% |
| `union_join_present_h3` | `ideal` | +2.87% | $10,287.33 | +0.00% | 0 | — | 124 | 0 | 0 | 229 | 43.5% |
| `union_join_present_h3` | `limit_prior` | -5.19% | $9,480.66 | -5.19% | 10 | — | 116 | 32 | 0 | 241 | 39.1% |
| `union_join_present_h3` | `limit_open` | -5.83% | $9,417.38 | -5.83% | 8 | — | 127 | 10 | 0 | 239 | 39.1% |
| `union_join_present_h3` | `market_mid` | -6.83% | $9,317.53 | -6.83% | 9 | — | 108 | 0 | 0 | 219 | 39.1% |
| `union_join_present_h3` | `market_adverse` | -24.82% | $7,518.01 | -24.82% | 23 | — | 90 | 0 | 0 | 212 | 30.4% |
| `union_join_present_h3` | `market_favorable` | +36.46% | $13,646.13 | +0.00% | 0 | — | 126 | 0 | 0 | 230 | 73.9% |
| `union_join_present_h3` | `partial_50` | -10.08% | $8,991.73 | -10.08% | 16 | — | 438 | 23 | 373 | 257 | 34.8% |
| `union_join_present_h3` | `gap_miss` | +3.61% | $10,361.38 | +0.00% | 0 | — | 124 | 1 | 0 | 227 | 47.8% |
| `union_news_g_h1` | `ideal` | +4.33% | $10,432.91 | -0.01% | 1 | — | 135 | 10 | 0 | 74 | 39.1% |
| `union_news_g_h1` | `limit_prior` | -5.33% | $9,467.23 | -5.33% | 20 | — | 105 | 37 | 0 | 102 | 30.4% |
| `union_news_g_h1` | `limit_open` | -1.91% | $9,809.51 | -2.32% | 12 | — | 127 | 17 | 0 | 82 | 34.8% |
| `union_news_g_h1` | `market_mid` | -17.59% | $8,241.12 | -17.59% | 20 | — | 133 | 10 | 0 | 75 | 8.7% |
| `union_news_g_h1` | `market_adverse` | -46.27% | $5,372.46 | -46.27% | 22 | — | 133 | 10 | 0 | 75 | 0.0% |
| `union_news_g_h1` | `market_favorable` | +73.02% | $17,301.83 | +0.00% | 0 | — | 135 | 10 | 0 | 74 | 69.6% |
| `union_news_g_h1` | `partial_50` | -12.92% | $8,708.26 | -12.92% | 21 | — | 359 | 22 | 303 | 83 | 13.0% |
| `union_news_g_h1` | `gap_miss` | +2.90% | $10,289.77 | -0.25% | 2 | — | 133 | 11 | 0 | 75 | 39.1% |
| `union_news_g_h3` | `ideal` | -10.86% | $8,913.92 | -10.86% | 19 | — | 109 | 0 | 0 | 181 | 26.1% |
| `union_news_g_h3` | `limit_prior` | -8.41% | $9,158.79 | -8.41% | 20 | — | 97 | 24 | 0 | 184 | 30.4% |
| `union_news_g_h3` | `limit_open` | -9.29% | $9,070.99 | -9.29% | 21 | — | 103 | 6 | 0 | 183 | 26.1% |
| `union_news_g_h3` | `market_mid` | -20.96% | $7,904.30 | -20.96% | 21 | — | 107 | 0 | 0 | 180 | 26.1% |
| `union_news_g_h3` | `market_adverse` | -36.16% | $6,383.64 | -36.16% | 22 | — | 105 | 0 | 0 | 183 | 13.0% |
| `union_news_g_h3` | `market_favorable` | +26.47% | $12,647.15 | +0.00% | 0 | — | 107 | 0 | 0 | 182 | 60.9% |
| `union_news_g_h3` | `partial_50` | -12.72% | $8,728.21 | -12.72% | 21 | — | 319 | 16 | 266 | 195 | 21.7% |
| `union_news_g_h3` | `gap_miss` | -12.51% | $8,748.91 | -12.51% | 20 | — | 107 | 1 | 0 | 180 | 21.7% |
| `union_news_present_h1` | `ideal` | +2.00% | $10,200.51 | -1.04% | 3 | — | 146 | 15 | 0 | 102 | 26.1% |
| `union_news_present_h1` | `limit_prior` | +1.38% | $10,138.06 | -1.49% | 4 | — | 116 | 35 | 0 | 124 | 30.4% |
| `union_news_present_h1` | `limit_open` | -2.25% | $9,774.57 | -2.25% | 12 | — | 137 | 23 | 0 | 112 | 21.7% |
| `union_news_present_h1` | `market_mid` | -15.12% | $8,487.72 | -15.12% | 22 | — | 144 | 15 | 0 | 103 | 17.4% |
| `union_news_present_h1` | `market_adverse` | -39.79% | $6,021.18 | -39.79% | 22 | — | 144 | 15 | 0 | 103 | 0.0% |
| `union_news_present_h1` | `market_favorable` | +62.49% | $16,248.73 | +0.00% | 0 | — | 146 | 15 | 0 | 102 | 65.2% |
| `union_news_present_h1` | `partial_50` | -11.15% | $8,884.54 | -11.15% | 22 | — | 431 | 34 | 374 | 120 | 21.7% |
| `union_news_present_h1` | `gap_miss` | -2.66% | $9,733.64 | -2.66% | 11 | — | 144 | 16 | 0 | 103 | 21.7% |
| `union_news_present_h3` | `ideal` | -5.31% | $9,469.04 | -5.31% | 13 | — | 118 | 0 | 0 | 218 | 39.1% |
| `union_news_present_h3` | `limit_prior` | -6.86% | $9,313.58 | -6.86% | 16 | — | 106 | 27 | 0 | 229 | 30.4% |
| `union_news_present_h3` | `limit_open` | -12.32% | $8,767.70 | -12.32% | 19 | — | 119 | 9 | 0 | 227 | 34.8% |
| `union_news_present_h3` | `market_mid` | -14.56% | $8,543.78 | -14.56% | 22 | — | 122 | 0 | 0 | 220 | 30.4% |
| `union_news_present_h3` | `market_adverse` | -29.46% | $7,053.50 | -29.46% | 22 | — | 118 | 0 | 0 | 218 | 26.1% |
| `union_news_present_h3` | `market_favorable` | +24.40% | $12,440.06 | -2.95% | 1 | — | 122 | 0 | 0 | 220 | 65.2% |
| `union_news_present_h3` | `partial_50` | -12.05% | $8,795.27 | -12.05% | 20 | — | 383 | 25 | 329 | 248 | 30.4% |
| `union_news_present_h3` | `gap_miss` | -5.31% | $9,469.32 | -5.31% | 13 | — | 116 | 1 | 0 | 217 | 39.1% |
| `union_news_missing_h1` | `ideal` | +1.64% | $10,164.19 | +0.00% | 0 | — | 32 | 0 | 0 | 0 | 13.0% |
| `union_news_missing_h1` | `limit_prior` | +0.72% | $10,071.94 | +0.00% | 0 | — | 24 | 6 | 0 | 6 | 8.7% |
| `union_news_missing_h1` | `limit_open` | +1.48% | $10,148.15 | +0.00% | 0 | — | 30 | 2 | 0 | 2 | 13.0% |
| `union_news_missing_h1` | `market_mid` | -2.44% | $9,756.03 | -2.44% | 23 | — | 32 | 0 | 0 | 0 | 0.0% |
| `union_news_missing_h1` | `market_adverse` | -10.07% | $8,993.27 | -10.07% | 23 | — | 32 | 0 | 0 | 0 | 0.0% |
| `union_news_missing_h1` | `market_favorable` | +11.70% | $11,170.05 | +0.00% | 0 | — | 32 | 0 | 0 | 0 | 17.4% |
| `union_news_missing_h1` | `partial_50` | -1.25% | $9,874.61 | -1.25% | 22 | — | 116 | 0 | 100 | 0 | 26.1% |
| `union_news_missing_h1` | `gap_miss` | +1.64% | $10,164.19 | +0.00% | 0 | — | 32 | 0 | 0 | 0 | 13.0% |
| `union_news_missing_h3` | `ideal` | +3.13% | $10,313.47 | +0.00% | 0 | — | 32 | 0 | 0 | 32 | 26.1% |
| `union_news_missing_h3` | `limit_prior` | +0.68% | $10,067.51 | -1.08% | 2 | — | 24 | 5 | 0 | 29 | 21.7% |
| `union_news_missing_h3` | `limit_open` | +3.14% | $10,313.59 | +0.00% | 0 | — | 30 | 1 | 0 | 31 | 26.1% |
| `union_news_missing_h3` | `market_mid` | -1.12% | $9,887.70 | -1.39% | 13 | — | 32 | 0 | 0 | 32 | 13.0% |
| `union_news_missing_h3` | `market_adverse` | -8.54% | $9,146.20 | -8.54% | 23 | — | 32 | 0 | 0 | 32 | 13.0% |
| `union_news_missing_h3` | `market_favorable` | +14.61% | $11,460.62 | +0.00% | 0 | — | 32 | 0 | 0 | 32 | 30.4% |
| `union_news_missing_h3` | `partial_50` | +0.06% | $10,006.25 | -0.29% | 2 | — | 114 | 4 | 100 | 36 | 21.7% |
| `union_news_missing_h3` | `gap_miss` | +3.13% | $10,313.47 | +0.00% | 0 | — | 32 | 0 | 0 | 32 | 26.1% |
| `union_catal_present_h1` | `ideal` | -2.93% | $9,706.61 | -2.93% | 8 | — | 5 | 2 | 0 | 4 | 0.0% |
| `union_catal_present_h1` | `limit_prior` | -1.76% | $9,823.53 | -3.09% | 8 | — | 5 | 4 | 0 | 6 | 8.7% |
| `union_catal_present_h1` | `limit_open` | -2.93% | $9,706.61 | -2.93% | 8 | — | 5 | 2 | 0 | 4 | 0.0% |
| `union_catal_present_h1` | `market_mid` | -4.57% | $9,543.55 | -4.57% | 8 | — | 5 | 2 | 0 | 4 | 0.0% |
| `union_catal_present_h1` | `market_adverse` | -7.62% | $9,238.29 | -7.62% | 8 | — | 5 | 2 | 0 | 4 | 0.0% |
| `union_catal_present_h1` | `market_favorable` | +2.33% | $10,232.91 | +0.00% | 0 | — | 5 | 2 | 0 | 4 | 8.7% |
| `union_catal_present_h1` | `partial_50` | -2.56% | $9,744.29 | -2.56% | 8 | — | 13 | 6 | 13 | 8 | 4.3% |
| `union_catal_present_h1` | `gap_miss` | -2.93% | $9,706.61 | -2.93% | 8 | — | 5 | 2 | 0 | 4 | 0.0% |
| `union_catal_present_h3` | `ideal` | -2.73% | $9,726.54 | -3.97% | 8 | — | 5 | 0 | 0 | 8 | 4.3% |
| `union_catal_present_h3` | `limit_prior` | -1.40% | $9,859.63 | -3.82% | 8 | — | 5 | 0 | 0 | 8 | 8.7% |
| `union_catal_present_h3` | `limit_open` | -2.73% | $9,726.54 | -3.97% | 8 | — | 5 | 0 | 0 | 8 | 4.3% |
| `union_catal_present_h3` | `market_mid` | -4.31% | $9,569.21 | -4.31% | 8 | — | 5 | 0 | 0 | 8 | 4.3% |
| `union_catal_present_h3` | `market_adverse` | -7.26% | $9,273.82 | -7.26% | 8 | — | 5 | 0 | 0 | 8 | 0.0% |
| `union_catal_present_h3` | `market_favorable` | +2.67% | $10,267.20 | -1.55% | 2 | — | 5 | 0 | 0 | 8 | 13.0% |
| `union_catal_present_h3` | `partial_50` | -2.61% | $9,738.92 | -2.61% | 8 | — | 9 | 4 | 9 | 12 | 4.3% |
| `union_catal_present_h3` | `gap_miss` | -2.73% | $9,726.54 | -3.97% | 8 | — | 5 | 0 | 0 | 8 | 4.3% |
| `union_blue_h1` | `ideal` | +1.51% | $10,150.68 | -1.54% | 3 | — | 158 | 16 | 0 | 89 | 26.1% |
| `union_blue_h1` | `limit_prior` | +3.01% | $10,301.12 | -1.10% | 4 | — | 129 | 34 | 0 | 107 | 34.8% |
| `union_blue_h1` | `limit_open` | -2.29% | $9,771.19 | -2.62% | 12 | — | 153 | 21 | 0 | 94 | 17.4% |
| `union_blue_h1` | `market_mid` | -16.22% | $8,377.72 | -16.22% | 22 | — | 158 | 16 | 0 | 89 | 17.4% |
| `union_blue_h1` | `market_adverse` | -40.94% | $5,906.53 | -40.94% | 22 | — | 156 | 16 | 0 | 90 | 0.0% |
| `union_blue_h1` | `market_favorable` | +67.44% | $16,743.96 | +0.00% | 0 | — | 160 | 16 | 0 | 88 | 65.2% |
| `union_blue_h1` | `partial_50` | -11.72% | $8,828.32 | -11.72% | 22 | — | 474 | 37 | 412 | 108 | 17.4% |
| `union_blue_h1` | `gap_miss` | -3.07% | $9,692.98 | -3.07% | 11 | — | 154 | 18 | 0 | 91 | 21.7% |
| `union_blue_h3` | `ideal` | -7.24% | $9,276.44 | -7.24% | 15 | — | 122 | 0 | 0 | 217 | 39.1% |
| `union_blue_h3` | `limit_prior` | -6.84% | $9,315.79 | -6.88% | 16 | — | 117 | 30 | 0 | 232 | 34.8% |
| `union_blue_h3` | `limit_open` | -13.30% | $8,670.44 | -13.30% | 19 | — | 133 | 8 | 0 | 227 | 21.7% |
| `union_blue_h3` | `market_mid` | -16.09% | $8,390.51 | -16.09% | 22 | — | 130 | 0 | 0 | 221 | 30.4% |
| `union_blue_h3` | `market_adverse` | -30.80% | $6,919.88 | -30.80% | 22 | — | 124 | 0 | 0 | 218 | 26.1% |
| `union_blue_h3` | `market_favorable` | +25.07% | $12,507.40 | -2.90% | 1 | — | 136 | 0 | 0 | 224 | 65.2% |
| `union_blue_h3` | `partial_50` | -12.49% | $8,751.37 | -12.49% | 21 | — | 420 | 23 | 359 | 252 | 26.1% |
| `union_blue_h3` | `gap_miss` | -6.70% | $9,329.97 | -6.70% | 15 | — | 128 | 2 | 0 | 220 | 39.1% |
| `union_white_h1` | `ideal` | +6.31% | $10,630.64 | -6.57% | 5 | — | 144 | 9 | 0 | 9 | 39.1% |
| `union_white_h1` | `limit_prior` | +4.02% | $10,401.56 | -5.43% | 5 | — | 127 | 29 | 0 | 29 | 39.1% |
| `union_white_h1` | `limit_open` | +3.13% | $10,312.76 | -6.80% | 6 | — | 137 | 14 | 0 | 14 | 34.8% |
| `union_white_h1` | `market_mid` | -17.50% | $8,249.77 | -17.50% | 23 | — | 144 | 9 | 0 | 9 | 17.4% |
| `union_white_h1` | `market_adverse` | -48.51% | $5,148.94 | -48.51% | 23 | — | 144 | 9 | 0 | 9 | 4.3% |
| `union_white_h1` | `market_favorable` | +161.00% | $26,099.73 | +0.00% | 0 | — | 144 | 9 | 0 | 9 | 69.6% |
| `union_white_h1` | `partial_50` | -8.89% | $9,110.90 | -8.89% | 22 | — | 452 | 29 | 394 | 29 | 17.4% |
| `union_white_h1` | `gap_miss` | +1.54% | $10,153.79 | -6.57% | 7 | — | 142 | 10 | 0 | 10 | 34.8% |
| `union_white_h3` | `ideal` | -2.81% | $9,718.70 | -2.90% | 8 | — | 112 | 1 | 0 | 129 | 39.1% |
| `union_white_h3` | `limit_prior` | -3.00% | $9,699.61 | -3.04% | 9 | — | 121 | 20 | 0 | 146 | 43.5% |
| `union_white_h3` | `limit_open` | -2.88% | $9,711.62 | -2.88% | 5 | — | 123 | 10 | 0 | 139 | 39.1% |
| `union_white_h3` | `market_mid` | -12.20% | $8,780.15 | -12.20% | 13 | — | 104 | 1 | 0 | 125 | 34.8% |
| `union_white_h3` | `market_adverse` | -27.85% | $7,215.17 | -27.85% | 21 | — | 96 | 1 | 0 | 121 | 26.1% |
| `union_white_h3` | `market_favorable` | +34.69% | $13,468.93 | +0.00% | 0 | — | 122 | 1 | 0 | 134 | 73.9% |
| `union_white_h3` | `partial_50` | -5.49% | $9,451.39 | -5.49% | 11 | — | 398 | 29 | 342 | 168 | 30.4% |
| `union_white_h3` | `gap_miss` | -0.89% | $9,911.48 | -0.89% | 5 | — | 112 | 2 | 0 | 131 | 39.1% |
| `union_last_green_h1` | `ideal` | +4.09% | $10,408.59 | +0.00% | 0 | — | 172 | 16 | 0 | 104 | 30.4% |
| `union_last_green_h1` | `limit_prior` | +3.21% | $10,320.89 | +0.00% | 0 | — | 152 | 38 | 0 | 126 | 43.5% |
| `union_last_green_h1` | `limit_open` | +1.38% | $10,138.09 | +0.00% | 0 | — | 166 | 24 | 0 | 112 | 26.1% |
| `union_last_green_h1` | `market_mid` | -17.77% | $8,222.59 | -17.77% | 22 | — | 172 | 16 | 0 | 104 | 17.4% |
| `union_last_green_h1` | `market_adverse` | -46.63% | $5,336.52 | -46.63% | 23 | — | 170 | 16 | 0 | 105 | 0.0% |
| `union_last_green_h1` | `market_favorable` | +99.37% | $19,937.33 | +0.00% | 0 | — | 172 | 16 | 0 | 104 | 65.2% |
| `union_last_green_h1` | `partial_50` | -10.31% | $8,969.03 | -10.31% | 21 | — | 536 | 36 | 465 | 123 | 13.0% |
| `union_last_green_h1` | `gap_miss` | -0.49% | $9,950.65 | -0.49% | 3 | — | 168 | 18 | 0 | 106 | 26.1% |
| `union_last_green_h3` | `ideal` | -1.06% | $9,893.62 | -1.06% | 5 | — | 100 | 0 | 0 | 235 | 34.8% |
| `union_last_green_h3` | `limit_prior` | -7.52% | $9,248.01 | -7.52% | 16 | — | 114 | 16 | 0 | 246 | 30.4% |
| `union_last_green_h3` | `limit_open` | -6.28% | $9,372.50 | -6.28% | 8 | — | 130 | 6 | 0 | 248 | 34.8% |
| `union_last_green_h3` | `market_mid` | -12.38% | $8,761.98 | -12.38% | 12 | — | 86 | 0 | 0 | 228 | 34.8% |
| `union_last_green_h3` | `market_adverse` | -30.13% | $6,986.96 | -30.13% | 22 | — | 86 | 0 | 0 | 228 | 26.1% |
| `union_last_green_h3` | `market_favorable` | +35.89% | $13,589.34 | +0.00% | 0 | — | 130 | 0 | 0 | 246 | 73.9% |
| `union_last_green_h3` | `partial_50` | -9.77% | $9,022.59 | -9.77% | 13 | — | 470 | 27 | 398 | 284 | 26.1% |
| `union_last_green_h3` | `gap_miss` | -1.80% | $9,820.19 | -1.80% | 6 | — | 112 | 2 | 0 | 237 | 34.8% |
| `union_last_red_h1` | `ideal` | -0.88% | $9,911.98 | -1.86% | 6 | — | 168 | 16 | 0 | 102 | 30.4% |
| `union_last_red_h1` | `limit_prior` | -3.88% | $9,611.52 | -3.88% | 19 | — | 135 | 44 | 0 | 131 | 34.8% |
| `union_last_red_h1` | `limit_open` | -8.02% | $9,197.78 | -8.02% | 20 | — | 155 | 24 | 0 | 110 | 30.4% |
| `union_last_red_h1` | `market_mid` | -23.93% | $7,606.65 | -23.93% | 23 | — | 168 | 16 | 0 | 102 | 4.3% |
| `union_last_red_h1` | `market_adverse` | -53.26% | $4,674.19 | -53.26% | 23 | — | 168 | 16 | 0 | 102 | 0.0% |
| `union_last_red_h1` | `market_favorable` | +91.50% | $19,150.04 | +0.00% | 0 | — | 170 | 16 | 0 | 101 | 69.6% |
| `union_last_red_h1` | `partial_50` | -14.43% | $8,556.74 | -14.43% | 23 | — | 541 | 42 | 477 | 126 | 17.4% |
| `union_last_red_h1` | `gap_miss` | -2.92% | $9,707.58 | -2.92% | 12 | — | 164 | 18 | 0 | 104 | 26.1% |
| `union_last_red_h3` | `ideal` | +1.34% | $10,133.99 | +0.00% | 0 | — | 120 | 0 | 0 | 233 | 47.8% |
| `union_last_red_h3` | `limit_prior` | -2.67% | $9,733.23 | -2.67% | 9 | — | 117 | 25 | 0 | 244 | 43.5% |
| `union_last_red_h3` | `limit_open` | -4.57% | $9,542.89 | -4.57% | 12 | — | 113 | 7 | 0 | 231 | 39.1% |
| `union_last_red_h3` | `market_mid` | -10.12% | $8,987.45 | -10.12% | 20 | — | 94 | 0 | 0 | 222 | 30.4% |
| `union_last_red_h3` | `market_adverse` | -28.88% | $7,112.21 | -28.88% | 23 | — | 90 | 0 | 0 | 220 | 30.4% |
| `union_last_red_h3` | `market_favorable` | +40.06% | $14,005.56 | +0.00% | 0 | — | 144 | 0 | 0 | 243 | 73.9% |
| `union_last_red_h3` | `partial_50` | -12.74% | $8,725.97 | -12.74% | 22 | — | 479 | 32 | 415 | 281 | 26.1% |
| `union_last_red_h3` | `gap_miss` | -0.61% | $9,939.31 | -0.61% | 6 | — | 120 | 2 | 0 | 231 | 43.5% |
| `union_candle_h1` | `ideal` | -0.24% | $9,976.24 | -3.41% | 8 | — | 162 | 15 | 0 | 102 | 26.1% |
| `union_candle_h1` | `limit_prior` | +6.29% | $10,628.61 | -0.32% | 1 | — | 144 | 33 | 0 | 120 | 43.5% |
| `union_candle_h1` | `limit_open` | -1.42% | $9,858.23 | -3.40% | 11 | — | 162 | 18 | 0 | 105 | 26.1% |
| `union_candle_h1` | `market_mid` | -21.75% | $7,824.63 | -21.75% | 23 | — | 162 | 15 | 0 | 102 | 13.0% |
| `union_candle_h1` | `market_adverse` | -49.66% | $5,033.84 | -49.66% | 23 | — | 160 | 15 | 0 | 103 | 0.0% |
| `union_candle_h1` | `market_favorable` | +88.23% | $18,823.23 | +0.00% | 0 | — | 162 | 15 | 0 | 102 | 65.2% |
| `union_candle_h1` | `partial_50` | -13.49% | $8,651.20 | -13.49% | 23 | — | 500 | 39 | 437 | 126 | 13.0% |
| `union_candle_h1` | `gap_miss` | -4.88% | $9,511.90 | -4.88% | 17 | — | 158 | 17 | 0 | 104 | 21.7% |
| `union_candle_h3` | `ideal` | -5.07% | $9,492.59 | -5.07% | 14 | — | 118 | 0 | 0 | 230 | 26.1% |
| `union_candle_h3` | `limit_prior` | -3.23% | $9,677.31 | -3.23% | 9 | — | 128 | 16 | 0 | 242 | 34.8% |
| `union_candle_h3` | `limit_open` | -9.11% | $9,088.78 | -9.11% | 17 | — | 118 | 4 | 0 | 235 | 26.1% |
| `union_candle_h3` | `market_mid` | -16.50% | $8,349.46 | -16.50% | 23 | — | 108 | 0 | 0 | 227 | 26.1% |
| `union_candle_h3` | `market_adverse` | -32.59% | $6,740.82 | -32.59% | 23 | — | 100 | 0 | 0 | 223 | 21.7% |
| `union_candle_h3` | `market_favorable` | +31.98% | $13,198.24 | +0.00% | 0 | — | 124 | 0 | 0 | 233 | 65.2% |
| `union_candle_h3` | `partial_50` | -13.83% | $8,617.12 | -13.83% | 23 | — | 438 | 23 | 373 | 262 | 17.4% |
| `union_candle_h3` | `gap_miss` | -4.74% | $9,526.33 | -4.74% | 14 | — | 114 | 2 | 0 | 228 | 26.1% |
| `union_coil_off_h1` | `ideal` | -8.70% | $9,130.06 | -9.28% | 12 | — | 156 | 16 | 0 | 104 | 21.7% |
| `union_coil_off_h1` | `limit_prior` | -11.60% | $8,840.50 | -11.60% | 10 | — | 136 | 40 | 0 | 128 | 30.4% |
| `union_coil_off_h1` | `limit_open` | -12.90% | $8,709.94 | -12.90% | 13 | — | 154 | 22 | 0 | 110 | 13.0% |
| `union_coil_off_h1` | `market_mid` | -25.87% | $7,412.67 | -25.87% | 21 | — | 156 | 16 | 0 | 104 | 4.3% |
| `union_coil_off_h1` | `market_adverse` | -49.96% | $5,003.73 | -49.96% | 23 | — | 156 | 16 | 0 | 104 | 0.0% |
| `union_coil_off_h1` | `market_favorable` | +56.67% | $15,667.27 | +0.00% | 0 | — | 156 | 16 | 0 | 104 | 65.2% |
| `union_coil_off_h1` | `partial_50` | -16.85% | $8,315.28 | -16.85% | 21 | — | 479 | 33 | 417 | 119 | 8.7% |
| `union_coil_off_h1` | `gap_miss` | -8.70% | $9,130.36 | -9.28% | 12 | — | 154 | 17 | 0 | 105 | 21.7% |
| `union_coil_off_h3` | `ideal` | -7.15% | $9,284.79 | -7.72% | 11 | — | 102 | 0 | 0 | 220 | 26.1% |
| `union_coil_off_h3` | `limit_prior` | -10.32% | $8,967.47 | -10.32% | 9 | — | 114 | 25 | 0 | 244 | 26.1% |
| `union_coil_off_h3` | `limit_open` | -10.87% | $8,913.54 | -10.87% | 12 | — | 110 | 8 | 0 | 228 | 17.4% |
| `union_coil_off_h3` | `market_mid` | -19.45% | $8,055.11 | -19.45% | 18 | — | 90 | 0 | 0 | 214 | 17.4% |
| `union_coil_off_h3` | `market_adverse` | -36.67% | $6,332.57 | -36.67% | 23 | — | 96 | 0 | 0 | 217 | 13.0% |
| `union_coil_off_h3` | `market_favorable` | +22.30% | $12,230.52 | +0.00% | 0 | — | 120 | 0 | 0 | 229 | 52.2% |
| `union_coil_off_h3` | `partial_50` | -18.08% | $8,192.32 | -18.08% | 21 | — | 429 | 23 | 366 | 259 | 13.0% |
| `union_coil_off_h3` | `gap_miss` | -8.59% | $9,140.85 | -9.17% | 11 | — | 110 | 1 | 0 | 222 | 21.7% |
| `union_earn_react_h1` | `ideal` | +2.11% | $10,211.25 | -0.75% | 4 | — | 136 | 16 | 0 | 61 | 30.4% |
| `union_earn_react_h1` | `limit_prior` | -3.75% | $9,624.72 | -5.01% | 19 | — | 109 | 38 | 0 | 83 | 43.5% |
| `union_earn_react_h1` | `limit_open` | -4.16% | $9,584.18 | -4.45% | 15 | — | 131 | 22 | 0 | 67 | 26.1% |
| `union_earn_react_h1` | `market_mid` | -25.15% | $7,485.39 | -25.15% | 22 | — | 136 | 16 | 0 | 61 | 13.0% |
| `union_earn_react_h1` | `market_adverse` | -59.09% | $4,091.15 | -59.09% | 23 | — | 136 | 16 | 0 | 61 | 0.0% |
| `union_earn_react_h1` | `market_favorable` | +131.03% | $23,103.01 | +0.00% | 0 | — | 136 | 16 | 0 | 61 | 65.2% |
| `union_earn_react_h1` | `partial_50` | -10.87% | $8,913.35 | -10.87% | 18 | — | 412 | 32 | 355 | 77 | 30.4% |
| `union_earn_react_h1` | `gap_miss` | +2.92% | $10,292.46 | +0.00% | 0 | — | 122 | 23 | 0 | 68 | 34.8% |
| `union_e_fresh_h1` | `ideal` | +9.71% | $10,971.44 | +0.00% | 0 | — | 136 | 16 | 0 | 63 | 43.5% |
| `union_e_fresh_h1` | `limit_prior` | -0.13% | $9,987.42 | -1.57% | 11 | — | 107 | 39 | 0 | 86 | 34.8% |
| `union_e_fresh_h1` | `limit_open` | +3.44% | $10,343.66 | +0.00% | 0 | — | 131 | 22 | 0 | 69 | 30.4% |
| `union_e_fresh_h1` | `market_mid` | -18.32% | $8,167.67 | -18.32% | 21 | — | 136 | 16 | 0 | 63 | 17.4% |
| `union_e_fresh_h1` | `market_adverse` | -52.37% | $4,762.94 | -52.37% | 23 | — | 136 | 16 | 0 | 63 | 0.0% |
| `union_e_fresh_h1` | `market_favorable` | +126.27% | $22,627.22 | +0.00% | 0 | — | 136 | 16 | 0 | 63 | 65.2% |
| `union_e_fresh_h1` | `partial_50` | -7.28% | $9,272.07 | -7.28% | 14 | — | 425 | 32 | 368 | 79 | 26.1% |
| `union_e_fresh_h1` | `gap_miss` | +6.53% | $10,652.86 | +0.00% | 0 | — | 124 | 22 | 0 | 69 | 47.8% |
| `union_r_up_h1` | `ideal` | +0.00% | $10,000.00 | +0.00% | 0 | — | 0 | 0 | 0 | 0 | 0.0% |
| `union_r_up_h1` | `limit_prior` | +0.00% | $10,000.00 | +0.00% | 0 | — | 0 | 0 | 0 | 0 | 0.0% |
| `union_r_up_h1` | `limit_open` | +0.00% | $10,000.00 | +0.00% | 0 | — | 0 | 0 | 0 | 0 | 0.0% |
| `union_r_up_h1` | `market_mid` | +0.00% | $10,000.00 | +0.00% | 0 | — | 0 | 0 | 0 | 0 | 0.0% |
| `union_r_up_h1` | `market_adverse` | +0.00% | $10,000.00 | +0.00% | 0 | — | 0 | 0 | 0 | 0 | 0.0% |
| `union_r_up_h1` | `market_favorable` | +0.00% | $10,000.00 | +0.00% | 0 | — | 0 | 0 | 0 | 0 | 0.0% |
| `union_r_up_h1` | `partial_50` | +0.00% | $10,000.00 | +0.00% | 0 | — | 0 | 0 | 0 | 0 | 0.0% |
| `union_r_up_h1` | `gap_miss` | +0.00% | $10,000.00 | +0.00% | 0 | — | 0 | 0 | 0 | 0 | 0.0% |
| `union_r_up_h3` | `ideal` | +0.00% | $10,000.00 | +0.00% | 0 | — | 0 | 0 | 0 | 0 | 0.0% |
| `union_r_up_h3` | `limit_prior` | +0.00% | $10,000.00 | +0.00% | 0 | — | 0 | 0 | 0 | 0 | 0.0% |
| `union_r_up_h3` | `limit_open` | +0.00% | $10,000.00 | +0.00% | 0 | — | 0 | 0 | 0 | 0 | 0.0% |
| `union_r_up_h3` | `market_mid` | +0.00% | $10,000.00 | +0.00% | 0 | — | 0 | 0 | 0 | 0 | 0.0% |
| `union_r_up_h3` | `market_adverse` | +0.00% | $10,000.00 | +0.00% | 0 | — | 0 | 0 | 0 | 0 | 0.0% |
| `union_r_up_h3` | `market_favorable` | +0.00% | $10,000.00 | +0.00% | 0 | — | 0 | 0 | 0 | 0 | 0.0% |
| `union_r_up_h3` | `partial_50` | +0.00% | $10,000.00 | +0.00% | 0 | — | 0 | 0 | 0 | 0 | 0.0% |
| `union_r_up_h3` | `gap_miss` | +0.00% | $10,000.00 | +0.00% | 0 | — | 0 | 0 | 0 | 0 | 0.0% |
| `union_break10_h1` | `ideal` | -5.26% | $9,474.20 | -11.91% | 19 | — | 152 | 12 | 0 | 97 | 30.4% |
| `union_break10_h1` | `limit_prior` | +7.09% | $10,709.38 | -4.60% | 5 | — | 133 | 29 | 0 | 115 | 39.1% |
| `union_break10_h1` | `limit_open` | -11.59% | $8,840.89 | -12.52% | 22 | — | 148 | 18 | 0 | 103 | 26.1% |
| `union_break10_h1` | `market_mid` | -27.96% | $7,204.15 | -27.96% | 23 | — | 152 | 12 | 0 | 97 | 21.7% |
| `union_break10_h1` | `market_adverse` | -56.24% | $4,376.29 | -56.24% | 23 | — | 150 | 12 | 0 | 98 | 0.0% |
| `union_break10_h1` | `market_favorable` | +108.60% | $20,860.40 | +0.00% | 0 | — | 152 | 12 | 0 | 97 | 65.2% |
| `union_break10_h1` | `partial_50` | -15.52% | $8,447.61 | -15.52% | 23 | — | 498 | 36 | 437 | 117 | 26.1% |
| `union_break10_h1` | `gap_miss` | -7.91% | $9,208.70 | -11.91% | 22 | — | 148 | 14 | 0 | 99 | 30.4% |
| `union_break10_h3` | `ideal` | -4.00% | $9,599.68 | -5.31% | 14 | — | 92 | 0 | 0 | 206 | 39.1% |
| `union_break10_h3` | `limit_prior` | +1.41% | $10,140.50 | -3.90% | 5 | — | 125 | 19 | 0 | 236 | 34.8% |
| `union_break10_h3` | `limit_open` | -7.33% | $9,267.26 | -8.32% | 16 | — | 102 | 4 | 0 | 215 | 39.1% |
| `union_break10_h3` | `market_mid` | -18.20% | $8,179.52 | -18.20% | 23 | — | 82 | 0 | 0 | 202 | 30.4% |
| `union_break10_h3` | `market_adverse` | -39.50% | $6,049.75 | -39.50% | 23 | — | 78 | 0 | 0 | 202 | 21.7% |
| `union_break10_h3` | `market_favorable` | +41.77% | $14,177.23 | +0.00% | 0 | — | 132 | 0 | 0 | 226 | 69.6% |
| `union_break10_h3` | `partial_50` | -15.18% | $8,481.92 | -15.18% | 23 | — | 433 | 27 | 375 | 256 | 30.4% |
| `union_break10_h3` | `gap_miss` | -1.83% | $9,816.84 | -5.31% | 13 | — | 94 | 2 | 0 | 209 | 39.1% |
| `union_vol_g_h5` | `ideal` | -12.86% | $8,713.70 | -13.48% | 21 | — | 88 | 16 | 0 | 294 | 34.8% |
| `union_vol_g_h5` | `limit_prior` | -11.68% | $8,831.61 | -11.68% | 20 | — | 103 | 36 | 0 | 329 | 34.8% |
| `union_vol_g_h5` | `limit_open` | -15.07% | $8,492.50 | -15.07% | 22 | — | 116 | 22 | 0 | 340 | 26.1% |
| `union_vol_g_h5` | `market_mid` | -20.73% | $7,927.16 | -21.32% | 22 | — | 70 | 16 | 0 | 272 | 21.7% |
| `union_vol_g_h5` | `market_adverse` | -32.65% | $6,735.02 | -33.21% | 22 | — | 60 | 16 | 0 | 261 | 21.7% |
| `union_vol_g_h5` | `market_favorable` | +9.37% | $10,937.10 | -4.25% | 3 | — | 118 | 16 | 0 | 337 | 43.5% |
| `union_vol_g_h5` | `partial_50` | -12.17% | $8,782.92 | -12.17% | 22 | — | 346 | 34 | 302 | 356 | 26.1% |
| `union_vol_g_h5` | `gap_miss` | -16.15% | $8,385.35 | -16.54% | 22 | — | 93 | 18 | 0 | 301 | 30.4% |
| `union_coil_off_h5` | `ideal` | -3.71% | $9,628.58 | -4.90% | 5 | — | 91 | 10 | 0 | 310 | 34.8% |
| `union_coil_off_h5` | `limit_prior` | -8.92% | $9,107.71 | -9.14% | 7 | — | 113 | 20 | 0 | 346 | 30.4% |
| `union_coil_off_h5` | `limit_open` | -4.80% | $9,519.64 | -4.93% | 5 | — | 87 | 15 | 0 | 309 | 34.8% |
| `union_coil_off_h5` | `market_mid` | -11.08% | $8,892.39 | -11.12% | 10 | — | 71 | 10 | 0 | 283 | 34.8% |
| `union_coil_off_h5` | `market_adverse` | -22.46% | $7,754.35 | -22.46% | 23 | — | 74 | 8 | 0 | 284 | 26.1% |
| `union_coil_off_h5` | `market_favorable` | +15.51% | $11,551.07 | +0.00% | 0 | — | 121 | 10 | 0 | 355 | 52.2% |
| `union_coil_off_h5` | `partial_50` | -16.18% | $8,382.13 | -16.40% | 21 | — | 376 | 29 | 314 | 403 | 21.7% |
| `union_coil_off_h5` | `gap_miss` | -3.71% | $9,628.75 | -4.90% | 5 | — | 91 | 11 | 0 | 311 | 34.8% |
| `union_last_green_h5` | `ideal` | +2.69% | $10,269.21 | +0.00% | 0 | — | 103 | 10 | 0 | 342 | 56.5% |
| `union_last_green_h5` | `limit_prior` | -9.07% | $9,093.17 | -9.14% | 16 | — | 125 | 27 | 0 | 383 | 30.4% |
| `union_last_green_h5` | `limit_open` | +1.76% | $10,175.77 | +0.00% | 0 | — | 140 | 19 | 0 | 405 | 52.2% |
| `union_last_green_h5` | `market_mid` | -6.38% | $9,362.15 | -6.38% | 5 | — | 80 | 8 | 0 | 305 | 47.8% |
| `union_last_green_h5` | `market_adverse` | -20.72% | $7,928.37 | -20.72% | 18 | — | 84 | 8 | 0 | 310 | 43.5% |
| `union_last_green_h5` | `market_favorable` | +25.16% | $12,515.66 | +0.00% | 0 | — | 119 | 10 | 0 | 366 | 65.2% |
| `union_last_green_h5` | `partial_50` | -7.53% | $9,247.15 | -7.73% | 12 | — | 404 | 45 | 344 | 451 | 47.8% |
| `union_last_green_h5` | `gap_miss` | +2.64% | $10,264.01 | +0.00% | 0 | — | 103 | 11 | 0 | 342 | 56.5% |
| `union_white_h5` | `ideal` | +1.81% | $10,181.08 | +0.00% | 0 | — | 103 | 13 | 0 | 245 | 60.9% |
| `union_white_h5` | `limit_prior` | -3.87% | $9,613.45 | -4.70% | 6 | — | 115 | 31 | 0 | 272 | 52.2% |
| `union_white_h5` | `limit_open` | +0.17% | $10,017.27 | -0.76% | 1 | — | 123 | 22 | 0 | 280 | 56.5% |
| `union_white_h5` | `market_mid` | -4.32% | $9,568.22 | -5.42% | 6 | — | 79 | 13 | 0 | 209 | 43.5% |
| `union_white_h5` | `market_adverse` | -15.23% | $8,477.04 | -16.05% | 13 | — | 81 | 13 | 0 | 212 | 43.5% |
| `union_white_h5` | `market_favorable` | +23.94% | $12,393.57 | +0.00% | 0 | — | 118 | 13 | 0 | 267 | 65.2% |
| `union_white_h5` | `partial_50` | -3.19% | $9,680.71 | -3.19% | 6 | — | 353 | 29 | 296 | 298 | 34.8% |
| `union_white_h5` | `gap_miss` | +1.99% | $10,198.55 | +0.00% | 0 | — | 103 | 14 | 0 | 247 | 60.9% |
| `union_vol_ab_h1` | `ideal` | -1.06% | $9,893.84 | -1.27% | 6 | — | 108 | 15 | 0 | 63 | 21.7% |
| `union_vol_ab_h1` | `limit_prior` | +5.16% | $10,515.98 | +0.00% | 0 | — | 93 | 28 | 0 | 76 | 26.1% |
| `union_vol_ab_h1` | `limit_open` | -1.01% | $9,899.13 | -1.22% | 6 | — | 108 | 16 | 0 | 64 | 21.7% |
| `union_vol_ab_h1` | `market_mid` | -17.17% | $8,282.81 | -17.17% | 14 | — | 108 | 15 | 0 | 63 | 13.0% |
| `union_vol_ab_h1` | `market_adverse` | -40.78% | $5,922.26 | -40.78% | 18 | — | 108 | 15 | 0 | 63 | 0.0% |
| `union_vol_ab_h1` | `market_favorable` | +56.53% | $15,653.27 | +0.00% | 0 | — | 108 | 15 | 0 | 63 | 56.5% |
| `union_vol_ab_h1` | `partial_50` | -9.95% | $9,004.72 | -9.95% | 14 | — | 318 | 37 | 282 | 83 | 13.0% |
| `union_vol_ab_h1` | `gap_miss` | -5.35% | $9,464.72 | -5.53% | 12 | — | 104 | 17 | 0 | 65 | 17.4% |
| `union_vol_ab_h3` | `ideal` | -4.64% | $9,536.39 | -4.81% | 12 | — | 94 | 0 | 0 | 153 | 26.1% |
| `union_vol_ab_h3` | `limit_prior` | -0.72% | $9,927.60 | -1.22% | 5 | — | 89 | 16 | 0 | 161 | 30.4% |
| `union_vol_ab_h3` | `limit_open` | -5.14% | $9,485.79 | -5.31% | 12 | — | 96 | 2 | 0 | 156 | 26.1% |
| `union_vol_ab_h3` | `market_mid` | -13.46% | $8,654.24 | -13.46% | 15 | — | 92 | 0 | 0 | 152 | 17.4% |
| `union_vol_ab_h3` | `market_adverse` | -27.11% | $7,288.87 | -27.11% | 16 | — | 90 | 0 | 0 | 151 | 17.4% |
| `union_vol_ab_h3` | `market_favorable` | +25.51% | $12,550.77 | +0.00% | 0 | — | 98 | 0 | 0 | 155 | 47.8% |
| `union_vol_ab_h3` | `partial_50` | -9.26% | $9,074.35 | -9.26% | 12 | — | 281 | 26 | 247 | 177 | 21.7% |
| `union_vol_ab_h3` | `gap_miss` | -1.97% | $9,802.86 | -2.17% | 8 | — | 94 | 2 | 0 | 155 | 26.1% |
| `union_blue_vol_h1` | `ideal` | -4.62% | $9,537.48 | -11.29% | 20 | — | 136 | 16 | 0 | 60 | 30.4% |
| `union_blue_vol_h1` | `limit_prior` | +4.63% | $10,462.79 | -7.70% | 6 | — | 119 | 27 | 0 | 71 | 39.1% |
| `union_blue_vol_h1` | `limit_open` | -6.47% | $9,352.50 | -11.63% | 22 | — | 132 | 19 | 0 | 63 | 30.4% |
| `union_blue_vol_h1` | `market_mid` | -23.68% | $7,632.29 | -23.68% | 22 | — | 136 | 16 | 0 | 60 | 21.7% |
| `union_blue_vol_h1` | `market_adverse` | -49.19% | $5,081.16 | -49.19% | 22 | — | 134 | 16 | 0 | 61 | 0.0% |
| `union_blue_vol_h1` | `market_favorable` | +113.66% | $21,366.15 | +0.00% | 0 | — | 136 | 16 | 0 | 60 | 65.2% |
| `union_blue_vol_h1` | `partial_50` | -13.69% | $8,630.65 | -13.69% | 22 | — | 461 | 40 | 410 | 82 | 17.4% |
| `union_blue_vol_h1` | `gap_miss` | -8.70% | $9,130.19 | -11.29% | 22 | — | 132 | 18 | 0 | 62 | 26.1% |
| `union_blue_vol_h3` | `ideal` | -8.26% | $9,174.51 | -8.26% | 20 | — | 106 | 0 | 0 | 173 | 34.8% |
| `union_blue_vol_h3` | `limit_prior` | -5.48% | $9,451.65 | -5.56% | 16 | — | 117 | 21 | 0 | 193 | 34.8% |
| `union_blue_vol_h3` | `limit_open` | -12.23% | $8,776.97 | -12.23% | 22 | — | 124 | 7 | 0 | 187 | 21.7% |
| `union_blue_vol_h3` | `market_mid` | -19.35% | $8,064.97 | -19.35% | 22 | — | 102 | 0 | 0 | 171 | 30.4% |
| `union_blue_vol_h3` | `market_adverse` | -36.08% | $6,391.58 | -36.08% | 22 | — | 94 | 0 | 0 | 167 | 26.1% |
| `union_blue_vol_h3` | `market_favorable` | +30.48% | $13,047.49 | -0.85% | 1 | — | 126 | 0 | 0 | 183 | 69.6% |
| `union_blue_vol_h3` | `partial_50` | -13.81% | $8,618.46 | -13.81% | 22 | — | 414 | 24 | 363 | 206 | 21.7% |
| `union_blue_vol_h3` | `gap_miss` | -7.06% | $9,294.40 | -7.06% | 20 | — | 106 | 2 | 0 | 173 | 34.8% |
| `union_news_vol_h1` | `ideal` | +0.94% | $10,094.05 | -0.17% | 1 | — | 79 | 6 | 0 | 37 | 26.1% |
| `union_news_vol_h1` | `limit_prior` | -2.31% | $9,768.54 | -5.03% | 18 | — | 59 | 20 | 0 | 51 | 30.4% |
| `union_news_vol_h1` | `limit_open` | -4.15% | $9,584.59 | -4.15% | 14 | — | 75 | 10 | 0 | 41 | 26.1% |
| `union_news_vol_h1` | `market_mid` | -15.06% | $8,493.61 | -15.06% | 19 | — | 79 | 6 | 0 | 37 | 17.4% |
| `union_news_vol_h1` | `market_adverse` | -37.99% | $6,201.07 | -37.99% | 22 | — | 79 | 6 | 0 | 37 | 0.0% |
| `union_news_vol_h1` | `market_favorable` | +56.63% | $15,663.52 | +0.00% | 0 | — | 79 | 6 | 0 | 37 | 56.5% |
| `union_news_vol_h1` | `partial_50` | -8.91% | $9,109.30 | -8.91% | 19 | — | 250 | 14 | 215 | 43 | 30.4% |
| `union_news_vol_h1` | `gap_miss` | -0.50% | $9,949.79 | -0.58% | 6 | — | 77 | 7 | 0 | 38 | 26.1% |
| `union_news_vol_h3` | `ideal` | -3.55% | $9,644.66 | -4.04% | 14 | — | 73 | 0 | 0 | 103 | 43.5% |
| `union_news_vol_h3` | `limit_prior` | -1.73% | $9,826.93 | -5.39% | 18 | — | 59 | 15 | 0 | 101 | 43.5% |
| `union_news_vol_h3` | `limit_open` | -4.29% | $9,570.68 | -4.83% | 18 | — | 69 | 5 | 0 | 104 | 39.1% |
| `union_news_vol_h3` | `market_mid` | -15.78% | $8,422.36 | -15.78% | 19 | — | 69 | 0 | 0 | 101 | 26.1% |
| `union_news_vol_h3` | `market_adverse` | -34.25% | $6,574.75 | -34.25% | 21 | — | 65 | 0 | 0 | 99 | 17.4% |
| `union_news_vol_h3` | `market_favorable` | +36.30% | $13,629.70 | +0.00% | 0 | — | 73 | 0 | 0 | 103 | 60.9% |
| `union_news_vol_h3` | `partial_50` | -8.95% | $9,104.93 | -8.95% | 21 | — | 233 | 8 | 198 | 111 | 34.8% |
| `union_news_vol_h3` | `gap_miss` | -6.33% | $9,366.75 | -6.78% | 17 | — | 73 | 1 | 0 | 101 | 39.1% |
| `union_e_green_h1` | `ideal` | -23.43% | $7,656.53 | -26.62% | 15 | — | 97 | 2 | 0 | 18 | 30.4% |
| `union_e_green_h1` | `limit_prior` | -32.90% | $6,710.33 | -35.69% | 22 | — | 63 | 17 | 0 | 41 | 26.1% |
| `union_e_green_h1` | `limit_open` | -33.52% | $6,647.48 | -36.29% | 15 | — | 77 | 7 | 0 | 31 | 21.7% |
| `union_e_green_h1` | `market_mid` | -47.01% | $5,299.41 | -48.01% | 22 | — | 97 | 2 | 0 | 18 | 17.4% |
| `union_e_green_h1` | `market_adverse` | -73.75% | $2,624.99 | -73.75% | 23 | — | 97 | 2 | 0 | 18 | 0.0% |
| `union_e_green_h1` | `market_favorable` | +114.64% | $21,464.28 | +0.00% | 0 | — | 97 | 2 | 0 | 18 | 65.2% |
| `union_e_green_h1` | `partial_50` | -20.49% | $7,950.82 | -21.21% | 15 | — | 336 | 16 | 295 | 32 | 21.7% |
| `union_e_green_h1` | `gap_miss` | -15.02% | $8,498.20 | -18.57% | 15 | — | 85 | 8 | 0 | 24 | 34.8% |
| `probable_probable_ok_h1` | `ideal` | -1.97% | $9,803.08 | -2.33% | 17 | — | 86 | 16 | 0 | 54 | 21.7% |
| `probable_probable_ok_h1` | `limit_prior` | -12.67% | $8,732.90 | -13.30% | 11 | — | 71 | 25 | 0 | 63 | 34.8% |
| `probable_probable_ok_h1` | `limit_open` | -10.72% | $8,928.09 | -10.72% | 18 | — | 83 | 19 | 0 | 57 | 13.0% |
| `probable_probable_ok_h1` | `market_mid` | -19.71% | $8,028.81 | -19.71% | 22 | — | 86 | 16 | 0 | 54 | 4.3% |
| `probable_probable_ok_h1` | `market_adverse` | -45.17% | $5,483.02 | -45.17% | 22 | — | 86 | 16 | 0 | 54 | 0.0% |
| `probable_probable_ok_h1` | `market_favorable` | +76.32% | $17,631.93 | +0.00% | 0 | — | 86 | 16 | 0 | 54 | 60.9% |
| `probable_probable_ok_h1` | `partial_50` | -12.78% | $8,722.08 | -12.78% | 22 | — | 301 | 24 | 266 | 62 | 17.4% |
| `probable_probable_ok_h1` | `gap_miss` | -3.02% | $9,697.56 | -3.41% | 18 | — | 80 | 19 | 0 | 57 | 26.1% |
| `probable_probable_ok_h3` | `ideal` | -17.75% | $8,224.84 | -20.02% | 21 | — | 68 | 0 | 0 | 123 | 26.1% |
| `probable_probable_ok_h3` | `limit_prior` | -15.66% | $8,433.89 | -15.66% | 19 | — | 67 | 14 | 0 | 128 | 39.1% |
| `probable_probable_ok_h3` | `limit_open` | -18.61% | $8,139.39 | -18.61% | 21 | — | 75 | 5 | 0 | 129 | 26.1% |
| `probable_probable_ok_h3` | `market_mid` | -28.01% | $7,199.00 | -28.51% | 21 | — | 66 | 0 | 0 | 122 | 26.1% |
| `probable_probable_ok_h3` | `market_adverse` | -43.79% | $5,620.90 | -43.79% | 22 | — | 66 | 0 | 0 | 122 | 26.1% |
| `probable_probable_ok_h3` | `market_favorable` | +20.82% | $12,082.17 | +0.00% | 0 | — | 82 | 0 | 0 | 130 | 65.2% |
| `probable_probable_ok_h3` | `partial_50` | -14.13% | $8,586.72 | -14.20% | 22 | — | 274 | 11 | 241 | 140 | 39.1% |
| `probable_probable_ok_h3` | `gap_miss` | -12.80% | $8,720.11 | -15.20% | 21 | — | 74 | 3 | 0 | 126 | 34.8% |
| `union_vol_green_h1` | `ideal` | +7.34% | $10,733.74 | -7.08% | 6 | — | 136 | 13 | 0 | 73 | 34.8% |
| `union_vol_green_h1` | `limit_prior` | +10.77% | $11,077.34 | -4.10% | 5 | — | 121 | 29 | 0 | 90 | 34.8% |
| `union_vol_green_h1` | `limit_open` | +5.35% | $10,535.06 | -7.46% | 6 | — | 130 | 16 | 0 | 76 | 34.8% |
| `union_vol_green_h1` | `market_mid` | -17.03% | $8,296.61 | -17.03% | 22 | — | 136 | 13 | 0 | 73 | 21.7% |
| `union_vol_green_h1` | `market_adverse` | -48.05% | $5,195.30 | -48.05% | 22 | — | 136 | 13 | 0 | 73 | 4.3% |
| `union_vol_green_h1` | `market_favorable` | +193.53% | $29,353.50 | +0.00% | 0 | — | 136 | 13 | 0 | 73 | 65.2% |
| `union_vol_green_h1` | `partial_50` | -9.26% | $9,073.69 | -9.26% | 21 | — | 436 | 33 | 383 | 92 | 21.7% |
| `union_vol_green_h1` | `gap_miss` | +3.71% | $10,370.52 | -7.08% | 8 | — | 130 | 16 | 0 | 76 | 30.4% |
| `union_vol_green_h3` | `ideal` | -15.77% | $8,422.96 | -15.88% | 22 | — | 102 | 0 | 0 | 182 | 21.7% |
| `union_vol_green_h3` | `limit_prior` | -8.91% | $9,109.29 | -8.91% | 19 | — | 111 | 17 | 0 | 199 | 30.4% |
| `union_vol_green_h3` | `limit_open` | -12.66% | $8,733.80 | -12.76% | 21 | — | 116 | 6 | 0 | 193 | 26.1% |
| `union_vol_green_h3` | `market_mid` | -27.55% | $7,244.93 | -27.55% | 22 | — | 100 | 0 | 0 | 181 | 17.4% |
| `union_vol_green_h3` | `market_adverse` | -44.31% | $5,568.53 | -44.31% | 22 | — | 92 | 0 | 0 | 177 | 13.0% |
| `union_vol_green_h3` | `market_favorable` | +28.80% | $12,879.61 | -1.35% | 1 | — | 126 | 0 | 0 | 194 | 65.2% |
| `union_vol_green_h3` | `partial_50` | -9.83% | $9,016.73 | -9.83% | 20 | — | 391 | 24 | 339 | 216 | 26.1% |
| `union_vol_green_h3` | `gap_miss` | -11.70% | $8,829.56 | -11.85% | 21 | — | 94 | 3 | 0 | 178 | 26.1% |
| `union_coil_green_h1` | `ideal` | -1.22% | $9,878.44 | -1.57% | 7 | — | 147 | 15 | 0 | 99 | 39.1% |
| `union_coil_green_h1` | `limit_prior` | +4.57% | $10,456.94 | +0.00% | 0 | — | 132 | 34 | 0 | 119 | 39.1% |
| `union_coil_green_h1` | `limit_open` | -5.38% | $9,462.20 | -5.38% | 12 | — | 144 | 21 | 0 | 106 | 26.1% |
| `union_coil_green_h1` | `market_mid` | -22.98% | $7,701.90 | -22.98% | 21 | — | 147 | 15 | 0 | 99 | 4.3% |
| `union_coil_green_h1` | `market_adverse` | -51.49% | $4,851.44 | -51.49% | 23 | — | 147 | 15 | 0 | 99 | 0.0% |
| `union_coil_green_h1` | `market_favorable` | +69.78% | $16,978.37 | +0.00% | 0 | — | 147 | 15 | 0 | 99 | 65.2% |
| `union_coil_green_h1` | `partial_50` | -14.58% | $8,541.57 | -14.58% | 21 | — | 467 | 31 | 407 | 115 | 13.0% |
| `union_coil_green_h1` | `gap_miss` | -1.23% | $9,877.26 | -1.58% | 7 | — | 145 | 16 | 0 | 100 | 39.1% |
| `union_coil_green_h3` | `ideal` | -5.24% | $9,476.03 | -5.54% | 12 | — | 87 | 1 | 0 | 211 | 26.1% |
| `union_coil_green_h3` | `limit_prior` | -2.08% | $9,792.21 | -2.08% | 5 | — | 100 | 22 | 0 | 231 | 26.1% |
| `union_coil_green_h3` | `limit_open` | -8.47% | $9,152.83 | -8.47% | 11 | — | 88 | 5 | 0 | 214 | 21.7% |
| `union_coil_green_h3` | `market_mid` | -15.71% | $8,429.33 | -15.71% | 21 | — | 67 | 1 | 0 | 201 | 17.4% |
| `union_coil_green_h3` | `market_adverse` | -31.34% | $6,866.47 | -31.34% | 23 | — | 75 | 1 | 0 | 205 | 13.0% |
| `union_coil_green_h3` | `market_favorable` | +25.25% | $12,525.38 | +0.00% | 0 | — | 115 | 1 | 0 | 223 | 60.9% |
| `union_coil_green_h3` | `partial_50` | -15.89% | $8,410.85 | -15.89% | 21 | — | 411 | 28 | 354 | 257 | 17.4% |
| `union_coil_green_h3` | `gap_miss` | -4.34% | $9,565.47 | -4.63% | 11 | — | 91 | 2 | 0 | 211 | 30.4% |
| `union_blue_coil_h1` | `ideal` | -6.81% | $9,318.95 | -6.81% | 17 | — | 156 | 16 | 0 | 83 | 21.7% |
| `union_blue_coil_h1` | `limit_prior` | -3.77% | $9,622.85 | -3.80% | 13 | — | 130 | 37 | 0 | 104 | 30.4% |
| `union_blue_coil_h1` | `limit_open` | -9.96% | $9,003.93 | -9.96% | 22 | — | 151 | 19 | 0 | 86 | 17.4% |
| `union_blue_coil_h1` | `market_mid` | -21.12% | $7,887.98 | -21.12% | 22 | — | 156 | 16 | 0 | 83 | 13.0% |
| `union_blue_coil_h1` | `market_adverse` | -41.21% | $5,879.22 | -41.21% | 22 | — | 154 | 16 | 0 | 84 | 0.0% |
| `union_blue_coil_h1` | `market_favorable` | +53.12% | $15,312.41 | +0.00% | 0 | — | 156 | 16 | 0 | 83 | 65.2% |
| `union_blue_coil_h1` | `partial_50` | -12.90% | $8,710.00 | -12.90% | 22 | — | 447 | 35 | 385 | 102 | 17.4% |
| `union_blue_coil_h1` | `gap_miss` | -6.80% | $9,319.86 | -6.80% | 17 | — | 154 | 17 | 0 | 84 | 21.7% |
| `union_blue_coil_h3` | `ideal` | -5.73% | $9,427.22 | -5.73% | 13 | — | 116 | 0 | 0 | 206 | 47.8% |
| `union_blue_coil_h3` | `limit_prior` | -5.38% | $9,462.04 | -5.56% | 16 | — | 108 | 23 | 0 | 215 | 43.5% |
| `union_blue_coil_h3` | `limit_open` | -12.22% | $8,778.05 | -12.22% | 17 | — | 123 | 7 | 0 | 213 | 30.4% |
| `union_blue_coil_h3` | `market_mid` | -14.47% | $8,553.41 | -14.47% | 22 | — | 108 | 0 | 0 | 202 | 34.8% |
| `union_blue_coil_h3` | `market_adverse` | -28.88% | $7,112.11 | -28.88% | 22 | — | 106 | 0 | 0 | 201 | 30.4% |
| `union_blue_coil_h3` | `market_favorable` | +25.11% | $12,510.92 | -2.94% | 1 | — | 130 | 0 | 0 | 213 | 69.6% |
| `union_blue_coil_h3` | `partial_50` | -13.49% | $8,650.82 | -13.49% | 22 | — | 401 | 19 | 338 | 239 | 21.7% |
| `union_blue_coil_h3` | `gap_miss` | -5.47% | $9,453.26 | -5.47% | 13 | — | 122 | 1 | 0 | 209 | 47.8% |
| `union_join_vol_green_h1` | `ideal` | +9.61% | $10,960.84 | -7.09% | 6 | — | 124 | 16 | 0 | 55 | 30.4% |
| `union_join_vol_green_h1` | `limit_prior` | +10.46% | $11,046.47 | -1.85% | 5 | — | 113 | 32 | 0 | 71 | 34.8% |
| `union_join_vol_green_h1` | `limit_open` | +6.99% | $10,699.27 | -7.47% | 6 | — | 118 | 20 | 0 | 59 | 30.4% |
| `union_join_vol_green_h1` | `market_mid` | -16.29% | $8,371.31 | -16.29% | 22 | — | 124 | 16 | 0 | 55 | 26.1% |
| `union_join_vol_green_h1` | `market_adverse` | -49.45% | $5,054.65 | -49.45% | 22 | — | 124 | 16 | 0 | 55 | 4.3% |
| `union_join_vol_green_h1` | `market_favorable` | +165.85% | $26,584.75 | +0.00% | 0 | — | 124 | 16 | 0 | 55 | 65.2% |
| `union_join_vol_green_h1` | `partial_50` | -10.17% | $8,982.92 | -10.17% | 21 | — | 411 | 36 | 365 | 75 | 17.4% |
| `union_join_vol_green_h1` | `gap_miss` | +8.09% | $10,808.96 | -5.81% | 7 | — | 118 | 19 | 0 | 58 | 26.1% |
| `union_join_vol_green_h3` | `ideal` | -11.67% | $8,832.90 | -11.67% | 21 | — | 92 | 0 | 0 | 154 | 26.1% |
| `union_join_vol_green_h3` | `limit_prior` | -8.59% | $9,141.43 | -8.59% | 20 | — | 101 | 18 | 0 | 172 | 30.4% |
| `union_join_vol_green_h3` | `limit_open` | -8.67% | $9,132.80 | -8.67% | 20 | — | 104 | 6 | 0 | 164 | 30.4% |
| `union_join_vol_green_h3` | `market_mid` | -24.26% | $7,573.70 | -24.26% | 22 | — | 92 | 0 | 0 | 154 | 21.7% |
| `union_join_vol_green_h3` | `market_adverse` | -42.29% | $5,771.18 | -42.29% | 22 | — | 80 | 0 | 0 | 148 | 17.4% |
| `union_join_vol_green_h3` | `market_favorable` | +28.89% | $12,888.80 | -1.32% | 1 | — | 112 | 0 | 0 | 164 | 65.2% |
| `union_join_vol_green_h3` | `partial_50` | -11.72% | $8,827.73 | -11.72% | 21 | — | 368 | 24 | 323 | 188 | 21.7% |
| `union_join_vol_green_h3` | `gap_miss` | -9.48% | $9,051.66 | -9.48% | 21 | — | 88 | 2 | 0 | 152 | 26.1% |
| `union_white_coil_h3` | `ideal` | +2.11% | $10,210.82 | -0.58% | 2 | — | 98 | 1 | 0 | 109 | 47.8% |
| `union_white_coil_h3` | `limit_prior` | -0.83% | $9,916.56 | -1.04% | 4 | — | 105 | 19 | 0 | 124 | 43.5% |
| `union_white_coil_h3` | `limit_open` | -4.27% | $9,573.21 | -4.27% | 8 | — | 103 | 7 | 0 | 114 | 39.1% |
| `union_white_coil_h3` | `market_mid` | -7.76% | $9,224.38 | -9.12% | 15 | — | 82 | 1 | 0 | 101 | 34.8% |
| `union_white_coil_h3` | `market_adverse` | -24.63% | $7,536.98 | -24.63% | 23 | — | 82 | 1 | 0 | 101 | 26.1% |
| `union_white_coil_h3` | `market_favorable` | +29.46% | $12,945.82 | +0.00% | 0 | — | 106 | 1 | 0 | 113 | 69.6% |
| `union_white_coil_h3` | `partial_50` | -11.41% | $8,859.36 | -11.94% | 23 | — | 359 | 21 | 307 | 141 | 21.7% |
| `union_white_coil_h3` | `gap_miss` | -0.31% | $9,969.11 | -2.91% | 8 | — | 104 | 2 | 0 | 112 | 47.8% |
| `flatten_vol_g_h3` | `ideal` | -18.80% | $8,120.18 | -18.80% | 22 | — | 49 | 0 | 0 | 58 | 17.4% |
| `flatten_vol_g_h3` | `limit_prior` | -11.12% | $8,887.97 | -11.12% | 22 | — | 47 | 11 | 0 | 67 | 17.4% |
| `flatten_vol_g_h3` | `limit_open` | -18.84% | $8,116.07 | -18.84% | 22 | — | 49 | 2 | 0 | 60 | 21.7% |
| `flatten_vol_g_h3` | `market_mid` | -24.74% | $7,526.24 | -24.74% | 22 | — | 43 | 0 | 0 | 55 | 17.4% |
| `flatten_vol_g_h3` | `market_adverse` | -35.71% | $6,429.42 | -35.71% | 22 | — | 43 | 0 | 0 | 55 | 13.0% |
| `flatten_vol_g_h3` | `market_favorable` | +6.88% | $10,687.64 | -2.56% | 2 | — | 49 | 0 | 0 | 58 | 43.5% |
| `flatten_vol_g_h3` | `partial_50` | -15.08% | $8,492.44 | -15.08% | 22 | — | 167 | 16 | 148 | 77 | 17.4% |
| `flatten_vol_g_h3` | `gap_miss` | -18.84% | $8,116.39 | -18.84% | 22 | — | 47 | 1 | 0 | 57 | 17.4% |
| `ohlc_hot_coil_h1` | `ideal` | -18.73% | $8,127.54 | -20.57% | 22 | — | 76 | 12 | 0 | 72 | 26.1% |
| `ohlc_hot_coil_h1` | `limit_prior` | -12.02% | $8,797.58 | -13.22% | 22 | — | 64 | 25 | 0 | 88 | 26.1% |
| `ohlc_hot_coil_h1` | `limit_open` | -21.95% | $7,805.16 | -23.71% | 22 | — | 74 | 16 | 0 | 77 | 21.7% |
| `ohlc_hot_coil_h1` | `market_mid` | -33.26% | $6,673.66 | -33.26% | 22 | — | 76 | 12 | 0 | 72 | 0.0% |
| `ohlc_hot_coil_h1` | `market_adverse` | -53.68% | $4,631.74 | -53.68% | 22 | — | 76 | 12 | 0 | 72 | 0.0% |
| `ohlc_hot_coil_h1` | `market_favorable` | +39.91% | $13,990.55 | +0.00% | 0 | — | 76 | 12 | 0 | 72 | 60.9% |
| `ohlc_hot_coil_h1` | `partial_50` | -18.95% | $8,105.40 | -18.95% | 22 | — | 251 | 30 | 227 | 88 | 8.7% |
| `ohlc_hot_coil_h1` | `gap_miss` | -18.73% | $8,127.54 | -20.57% | 22 | — | 76 | 12 | 0 | 72 | 26.1% |
| `union_hot_score_h1` | `ideal` | +1.18% | $10,117.94 | -10.38% | 7 | — | 172 | 10 | 0 | 87 | 39.1% |
| `union_hot_score_h1` | `limit_prior` | +4.77% | $10,477.23 | -6.14% | 7 | — | 148 | 36 | 0 | 114 | 43.5% |
| `union_hot_score_h1` | `limit_open` | -7.21% | $9,278.70 | -10.66% | 21 | — | 160 | 22 | 0 | 99 | 34.8% |
| `union_hot_score_h1` | `market_mid` | -31.10% | $6,889.89 | -31.10% | 22 | — | 172 | 10 | 0 | 87 | 30.4% |
| `union_hot_score_h1` | `market_adverse` | -65.36% | $3,463.88 | -65.36% | 23 | — | 172 | 10 | 0 | 87 | 8.7% |
| `union_hot_score_h1` | `market_favorable` | +151.17% | $25,116.79 | +0.00% | 0 | — | 172 | 10 | 0 | 87 | 78.3% |
| `union_hot_score_h1` | `partial_50` | -13.60% | $8,639.87 | -13.60% | 22 | — | 523 | 26 | 461 | 87 | 30.4% |
| `union_hot_score_h1` | `gap_miss` | +2.81% | $10,281.09 | -10.28% | 6 | — | 166 | 13 | 0 | 90 | 39.1% |
| `union_hot_score_h3` | `ideal` | +8.86% | $10,886.41 | +0.00% | 0 | — | 124 | 2 | 0 | 208 | 52.2% |
| `union_hot_score_h3` | `limit_prior` | +3.54% | $10,354.44 | -4.08% | 7 | — | 124 | 28 | 0 | 221 | 39.1% |
| `union_hot_score_h3` | `limit_open` | +7.70% | $10,769.92 | -0.13% | 1 | — | 136 | 14 | 0 | 222 | 47.8% |
| `union_hot_score_h3` | `market_mid` | -10.65% | $8,934.97 | -10.65% | 13 | — | 106 | 0 | 0 | 201 | 34.8% |
| `union_hot_score_h3` | `market_adverse` | -34.30% | $6,570.12 | -34.30% | 23 | — | 98 | 0 | 0 | 197 | 21.7% |
| `union_hot_score_h3` | `market_favorable` | +68.96% | $16,896.11 | +0.00% | 0 | — | 148 | 2 | 0 | 219 | 73.9% |
| `union_hot_score_h3` | `partial_50` | -8.50% | $9,150.28 | -8.50% | 20 | — | 468 | 24 | 407 | 227 | 30.4% |
| `union_hot_score_h3` | `gap_miss` | +9.98% | $10,998.48 | +0.00% | 0 | — | 124 | 5 | 0 | 208 | 52.2% |
| `union_candle_score_h1` | `ideal` | +1.70% | $10,170.38 | -2.62% | 7 | — | 169 | 13 | 0 | 99 | 21.7% |
| `union_candle_score_h1` | `limit_prior` | -0.64% | $9,936.19 | -2.02% | 11 | — | 151 | 40 | 0 | 127 | 26.1% |
| `union_candle_score_h1` | `limit_open` | -2.64% | $9,736.24 | -4.83% | 15 | — | 165 | 22 | 0 | 108 | 21.7% |
| `union_candle_score_h1` | `market_mid` | -20.60% | $7,939.70 | -21.04% | 22 | — | 169 | 13 | 0 | 99 | 17.4% |
| `union_candle_score_h1` | `market_adverse` | -48.53% | $5,146.94 | -48.53% | 23 | — | 167 | 13 | 0 | 100 | 0.0% |
| `union_candle_score_h1` | `market_favorable` | +101.59% | $20,158.64 | +0.00% | 0 | — | 169 | 13 | 0 | 99 | 69.6% |
| `union_candle_score_h1` | `partial_50` | -11.74% | $8,826.27 | -12.00% | 22 | — | 526 | 33 | 457 | 116 | 17.4% |
| `union_candle_score_h1` | `gap_miss` | +1.04% | $10,104.21 | -3.25% | 8 | — | 167 | 14 | 0 | 100 | 21.7% |
| `union_candle_score_h3` | `ideal` | +4.73% | $10,473.19 | +0.00% | 0 | — | 117 | 1 | 0 | 224 | 39.1% |
| `union_candle_score_h3` | `limit_prior` | +2.28% | $10,227.67 | -0.83% | 2 | — | 131 | 16 | 0 | 241 | 39.1% |
| `union_candle_score_h3` | `limit_open` | +3.27% | $10,327.32 | +0.00% | 0 | — | 133 | 9 | 0 | 240 | 34.8% |
| `union_candle_score_h3` | `market_mid` | -7.50% | $9,249.77 | -8.03% | 13 | — | 91 | 1 | 0 | 216 | 34.8% |
| `union_candle_score_h3` | `market_adverse` | -25.72% | $7,428.33 | -25.72% | 23 | — | 85 | 1 | 0 | 213 | 26.1% |
| `union_candle_score_h3` | `market_favorable` | +57.73% | $15,773.39 | +0.00% | 0 | — | 143 | 1 | 0 | 237 | 78.3% |
| `union_candle_score_h3` | `partial_50` | -8.12% | $9,188.42 | -8.28% | 14 | — | 462 | 24 | 395 | 264 | 30.4% |
| `union_candle_score_h3` | `gap_miss` | +4.51% | $10,451.50 | +0.00% | 0 | — | 117 | 2 | 0 | 225 | 43.5% |
| `union_ret_5_h1` | `ideal` | +1.22% | $10,122.35 | -6.92% | 5 | — | 174 | 12 | 0 | 96 | 30.4% |
| `union_ret_5_h1` | `limit_prior` | +4.01% | $10,400.66 | -1.59% | 4 | — | 155 | 36 | 0 | 121 | 39.1% |
| `union_ret_5_h1` | `limit_open` | -8.66% | $9,133.61 | -8.66% | 18 | — | 162 | 25 | 0 | 109 | 26.1% |
| `union_ret_5_h1` | `market_mid` | -30.21% | $6,979.15 | -30.21% | 22 | — | 174 | 12 | 0 | 96 | 21.7% |
| `union_ret_5_h1` | `market_adverse` | -64.09% | $3,590.87 | -64.09% | 23 | — | 174 | 12 | 0 | 96 | 4.3% |
| `union_ret_5_h1` | `market_favorable` | +148.44% | $24,844.40 | +0.00% | 0 | — | 174 | 12 | 0 | 96 | 73.9% |
| `union_ret_5_h1` | `partial_50` | -13.26% | $8,673.97 | -13.26% | 22 | — | 573 | 41 | 512 | 113 | 30.4% |
| `union_ret_5_h1` | `gap_miss` | +3.13% | $10,313.17 | -4.59% | 4 | — | 167 | 14 | 0 | 98 | 34.8% |
| `union_ret_5_h3` | `ideal` | +10.52% | $11,051.63 | +0.00% | 0 | — | 130 | 2 | 0 | 231 | 52.2% |
| `union_ret_5_h3` | `limit_prior` | +5.10% | $10,509.85 | -1.28% | 3 | — | 133 | 23 | 0 | 242 | 39.1% |
| `union_ret_5_h3` | `limit_open` | +10.41% | $11,041.46 | +0.00% | 0 | — | 130 | 12 | 0 | 234 | 39.1% |
| `union_ret_5_h3` | `market_mid` | -6.40% | $9,360.41 | -6.40% | 9 | — | 98 | 0 | 0 | 217 | 39.1% |
| `union_ret_5_h3` | `market_adverse` | -30.25% | $6,974.79 | -30.25% | 23 | — | 104 | 0 | 0 | 220 | 30.4% |
| `union_ret_5_h3` | `market_favorable` | +71.33% | $17,133.36 | +0.00% | 0 | — | 156 | 2 | 0 | 243 | 87.0% |
| `union_ret_5_h3` | `partial_50` | -7.86% | $9,214.19 | -7.86% | 17 | — | 500 | 37 | 441 | 262 | 39.1% |
| `union_ret_5_h3` | `gap_miss` | +10.72% | $11,072.31 | +0.00% | 0 | — | 127 | 5 | 0 | 230 | 56.5% |
| `union_cond_h1` | `ideal` | -2.45% | $9,754.84 | -4.11% | 16 | — | 172 | 16 | 0 | 105 | 30.4% |
| `union_cond_h1` | `limit_prior` | +1.59% | $10,158.88 | -2.47% | 4 | — | 147 | 39 | 0 | 129 | 47.8% |
| `union_cond_h1` | `limit_open` | -4.81% | $9,519.33 | -4.81% | 16 | — | 165 | 21 | 0 | 110 | 26.1% |
| `union_cond_h1` | `market_mid` | -21.95% | $7,805.43 | -21.95% | 23 | — | 172 | 16 | 0 | 105 | 17.4% |
| `union_cond_h1` | `market_adverse` | -48.47% | $5,153.47 | -48.47% | 23 | — | 170 | 16 | 0 | 106 | 0.0% |
| `union_cond_h1` | `market_favorable` | +71.36% | $17,136.21 | +0.00% | 0 | — | 174 | 16 | 0 | 104 | 69.6% |
| `union_cond_h1` | `partial_50` | -14.06% | $8,593.84 | -14.06% | 23 | — | 508 | 36 | 438 | 125 | 13.0% |
| `union_cond_h1` | `gap_miss` | -5.29% | $9,470.62 | -6.13% | 16 | — | 168 | 18 | 0 | 107 | 30.4% |
| `union_cond_h3` | `ideal` | -2.01% | $9,799.22 | -2.93% | 8 | — | 126 | 0 | 0 | 241 | 43.5% |
| `union_cond_h3` | `limit_prior` | -5.98% | $9,402.12 | -6.55% | 16 | — | 125 | 27 | 0 | 254 | 39.1% |
| `union_cond_h3` | `limit_open` | -4.95% | $9,505.09 | -4.95% | 8 | — | 135 | 8 | 0 | 249 | 34.8% |
| `union_cond_h3` | `market_mid` | -10.63% | $8,937.44 | -10.63% | 13 | — | 112 | 0 | 0 | 234 | 30.4% |
| `union_cond_h3` | `market_adverse` | -25.83% | $7,416.88 | -25.83% | 22 | — | 102 | 0 | 0 | 229 | 26.1% |
| `union_cond_h3` | `market_favorable` | +28.23% | $12,823.13 | +0.00% | 0 | — | 142 | 0 | 0 | 249 | 69.6% |
| `union_cond_h3` | `partial_50` | -13.11% | $8,689.27 | -13.11% | 20 | — | 449 | 24 | 378 | 278 | 17.4% |
| `union_cond_h3` | `gap_miss` | -1.98% | $9,801.64 | -2.91% | 8 | — | 124 | 1 | 0 | 240 | 43.5% |
| `union_w_hot_cond_h1` | `ideal` | -0.42% | $9,958.10 | -10.85% | 16 | — | 176 | 12 | 0 | 94 | 39.1% |
| `union_w_hot_cond_h1` | `limit_prior` | +4.13% | $10,413.51 | -5.22% | 7 | — | 152 | 37 | 0 | 120 | 43.5% |
| `union_w_hot_cond_h1` | `limit_open` | -6.56% | $9,344.08 | -10.15% | 19 | — | 168 | 22 | 0 | 104 | 34.8% |
| `union_w_hot_cond_h1` | `market_mid` | -30.94% | $6,905.55 | -30.94% | 22 | — | 176 | 12 | 0 | 94 | 26.1% |
| `union_w_hot_cond_h1` | `market_adverse` | -64.16% | $3,583.63 | -64.16% | 23 | — | 174 | 12 | 0 | 95 | 4.3% |
| `union_w_hot_cond_h1` | `market_favorable` | +141.69% | $24,169.27 | +0.00% | 0 | — | 176 | 12 | 0 | 94 | 73.9% |
| `union_w_hot_cond_h1` | `partial_50` | -15.43% | $8,457.12 | -15.43% | 22 | — | 518 | 34 | 455 | 107 | 26.1% |
| `union_w_hot_cond_h1` | `gap_miss` | +1.50% | $10,149.46 | -9.18% | 7 | — | 170 | 15 | 0 | 97 | 39.1% |
| `union_w_hot_cond_h3` | `ideal` | +6.50% | $10,649.88 | +0.00% | 0 | — | 130 | 0 | 0 | 227 | 47.8% |
| `union_w_hot_cond_h3` | `limit_prior` | -2.24% | $9,776.26 | -2.88% | 12 | — | 140 | 30 | 0 | 249 | 34.8% |
| `union_w_hot_cond_h3` | `limit_open` | +4.22% | $10,421.81 | +0.00% | 0 | — | 140 | 7 | 0 | 238 | 43.5% |
| `union_w_hot_cond_h3` | `market_mid` | -10.97% | $8,903.01 | -10.97% | 13 | — | 114 | 0 | 0 | 223 | 34.8% |
| `union_w_hot_cond_h3` | `market_adverse` | -33.63% | $6,637.02 | -33.63% | 23 | — | 106 | 0 | 0 | 219 | 26.1% |
| `union_w_hot_cond_h3` | `market_favorable` | +60.60% | $16,060.37 | +0.00% | 0 | — | 150 | 0 | 0 | 236 | 73.9% |
| `union_w_hot_cond_h3` | `partial_50` | -11.83% | $8,817.14 | -11.83% | 21 | — | 449 | 24 | 386 | 247 | 21.7% |
| `union_w_hot_cond_h3` | `gap_miss` | +6.50% | $10,650.26 | +0.00% | 0 | — | 128 | 1 | 0 | 226 | 47.8% |
| `union_w_hot_candle_h1` | `ideal` | +1.27% | $10,127.38 | -7.11% | 6 | — | 170 | 13 | 0 | 96 | 34.8% |
| `union_w_hot_candle_h1` | `limit_prior` | -0.71% | $9,928.87 | -3.25% | 11 | — | 143 | 46 | 0 | 130 | 34.8% |
| `union_w_hot_candle_h1` | `limit_open` | -5.16% | $9,483.59 | -7.27% | 16 | — | 162 | 24 | 0 | 107 | 34.8% |
| `union_w_hot_candle_h1` | `market_mid` | -27.43% | $7,256.59 | -27.43% | 22 | — | 170 | 13 | 0 | 96 | 26.1% |
| `union_w_hot_candle_h1` | `market_adverse` | -60.25% | $3,974.81 | -60.25% | 23 | — | 170 | 13 | 0 | 96 | 4.3% |
| `union_w_hot_candle_h1` | `market_favorable` | +135.05% | $23,505.33 | +0.00% | 0 | — | 170 | 13 | 0 | 96 | 73.9% |
| `union_w_hot_candle_h1` | `partial_50` | -14.70% | $8,529.99 | -14.70% | 22 | — | 533 | 27 | 467 | 96 | 26.1% |
| `union_w_hot_candle_h1` | `gap_miss` | +3.00% | $10,299.74 | -6.92% | 5 | — | 164 | 16 | 0 | 99 | 34.8% |
| `union_w_hot_candle_h3` | `ideal` | +4.80% | $10,480.15 | +0.00% | 0 | — | 128 | 0 | 0 | 220 | 47.8% |
| `union_w_hot_candle_h3` | `limit_prior` | -2.15% | $9,784.75 | -2.99% | 8 | — | 127 | 25 | 0 | 232 | 34.8% |
| `union_w_hot_candle_h3` | `limit_open` | +5.75% | $10,575.27 | +0.00% | 0 | — | 138 | 10 | 0 | 230 | 47.8% |
| `union_w_hot_candle_h3` | `market_mid` | -12.52% | $8,747.91 | -12.52% | 13 | — | 102 | 0 | 0 | 212 | 34.8% |
| `union_w_hot_candle_h3` | `market_adverse` | -34.73% | $6,527.08 | -34.73% | 23 | — | 96 | 0 | 0 | 209 | 21.7% |
| `union_w_hot_candle_h3` | `market_favorable` | +62.68% | $16,268.19 | +0.00% | 0 | — | 150 | 0 | 0 | 228 | 69.6% |
| `union_w_hot_candle_h3` | `partial_50` | -11.68% | $8,832.04 | -11.68% | 18 | — | 472 | 22 | 410 | 239 | 34.8% |
| `union_w_hot_candle_h3` | `gap_miss` | +4.30% | $10,430.14 | +0.00% | 0 | — | 126 | 3 | 0 | 219 | 43.5% |
| `union_hot_n12_h1` | `ideal` | -3.93% | $9,607.27 | -10.45% | 19 | — | 252 | 18 | 0 | 140 | 34.8% |
| `union_hot_n12_h1` | `limit_prior` | -0.89% | $9,911.36 | -5.64% | 18 | — | 221 | 55 | 0 | 178 | 43.5% |
| `union_hot_n12_h1` | `limit_open` | -10.76% | $8,924.05 | -10.76% | 22 | — | 238 | 33 | 0 | 155 | 30.4% |
| `union_hot_n12_h1` | `market_mid` | -31.90% | $6,809.84 | -31.90% | 23 | — | 252 | 18 | 0 | 140 | 21.7% |
| `union_hot_n12_h1` | `market_adverse` | -63.99% | $3,600.62 | -63.99% | 23 | — | 246 | 18 | 0 | 143 | 8.7% |
| `union_hot_n12_h1` | `market_favorable` | +124.08% | $22,408.23 | +0.00% | 0 | — | 254 | 18 | 0 | 139 | 78.3% |
| `union_hot_n12_h1` | `partial_50` | -16.29% | $8,371.42 | -16.29% | 23 | — | 692 | 38 | 597 | 137 | 21.7% |
| `union_hot_n12_h1` | `gap_miss` | -1.23% | $9,876.62 | -9.29% | 15 | — | 243 | 21 | 0 | 143 | 39.1% |
| `union_cond_n4_h3` | `ideal` | +2.35% | $10,234.54 | +0.00% | 0 | — | 48 | 0 | 0 | 118 | 39.1% |
| `union_cond_n4_h3` | `limit_prior` | -8.71% | $9,128.81 | -9.53% | 17 | — | 57 | 18 | 0 | 130 | 34.8% |
| `union_cond_n4_h3` | `limit_open` | -2.79% | $9,721.25 | -2.79% | 6 | — | 69 | 6 | 0 | 131 | 34.8% |
| `union_cond_n4_h3` | `market_mid` | -8.44% | $9,156.07 | -8.44% | 12 | — | 48 | 0 | 0 | 118 | 26.1% |
| `union_cond_n4_h3` | `market_adverse` | -27.09% | $7,291.20 | -27.09% | 21 | — | 52 | 0 | 0 | 120 | 21.7% |
| `union_cond_n4_h3` | `market_favorable` | +34.94% | $13,493.75 | +0.00% | 0 | — | 72 | 0 | 0 | 130 | 65.2% |
| `union_cond_n4_h3` | `partial_50` | -8.63% | $9,136.89 | -8.63% | 12 | — | 248 | 14 | 215 | 147 | 34.8% |
| `union_cond_n4_h3` | `gap_miss` | +2.35% | $10,234.54 | +0.00% | 0 | — | 48 | 0 | 0 | 118 | 39.1% |
| `union_h3_exit_alarm` | `ideal` | +2.99% | $10,298.58 | +0.00% | 0 | — | 132 | 1 | 0 | 224 | 47.8% |
| `union_h3_exit_alarm` | `limit_prior` | -4.85% | $9,514.84 | -4.85% | 10 | — | 116 | 32 | 0 | 236 | 34.8% |
| `union_h3_exit_alarm` | `limit_open` | -6.44% | $9,355.88 | -6.44% | 8 | — | 133 | 11 | 0 | 235 | 39.1% |
| `union_h3_exit_alarm` | `market_mid` | -8.52% | $9,148.33 | -8.52% | 14 | — | 128 | 1 | 0 | 226 | 34.8% |
| `union_h3_exit_alarm` | `market_adverse` | -24.75% | $7,525.03 | -24.75% | 23 | — | 110 | 1 | 0 | 217 | 34.8% |
| `union_h3_exit_alarm` | `market_favorable` | +36.40% | $13,640.03 | +0.00% | 0 | — | 134 | 1 | 0 | 225 | 78.3% |
| `union_h3_exit_alarm` | `partial_50` | -10.42% | $8,958.29 | -10.42% | 16 | — | 442 | 24 | 377 | 250 | 30.4% |
| `union_h3_exit_alarm` | `gap_miss` | +2.96% | $10,295.56 | +0.00% | 0 | — | 130 | 2 | 0 | 223 | 47.8% |
| `union_h5_exit_alarm` | `ideal` | +3.34% | $10,333.95 | +0.00% | 0 | — | 131 | 11 | 0 | 343 | 52.2% |
| `union_h5_exit_alarm` | `limit_prior` | +0.71% | $10,070.93 | +0.00% | 0 | — | 113 | 38 | 0 | 339 | 43.5% |
| `union_h5_exit_alarm` | `limit_open` | -0.41% | $9,959.24 | -0.41% | 4 | — | 136 | 18 | 0 | 358 | 39.1% |
| `union_h5_exit_alarm` | `market_mid` | -5.23% | $9,476.88 | -5.23% | 7 | — | 125 | 11 | 0 | 338 | 30.4% |
| `union_h5_exit_alarm` | `market_adverse` | -19.69% | $8,031.32 | -19.69% | 22 | — | 123 | 11 | 0 | 335 | 26.1% |
| `union_h5_exit_alarm` | `market_favorable` | +28.94% | $12,894.36 | +0.00% | 0 | — | 133 | 11 | 0 | 346 | 65.2% |
| `union_h5_exit_alarm` | `partial_50` | -5.97% | $9,402.92 | -5.97% | 10 | — | 391 | 34 | 332 | 394 | 30.4% |
| `union_h5_exit_alarm` | `gap_miss` | +3.26% | $10,326.17 | +0.00% | 0 | — | 129 | 12 | 0 | 340 | 52.2% |
| `union_h3_exit_red` | `ideal` | -0.63% | $9,936.84 | -0.63% | 4 | — | 110 | 0 | 0 | 230 | 34.8% |
| `union_h3_exit_red` | `limit_prior` | -6.39% | $9,361.27 | -6.39% | 16 | — | 126 | 16 | 0 | 241 | 30.4% |
| `union_h3_exit_red` | `limit_open` | -4.79% | $9,521.11 | -4.79% | 8 | — | 138 | 7 | 0 | 241 | 34.8% |
| `union_h3_exit_red` | `market_mid` | -13.03% | $8,696.63 | -13.03% | 12 | — | 98 | 0 | 0 | 225 | 30.4% |
| `union_h3_exit_red` | `market_adverse` | -31.58% | $6,841.78 | -31.58% | 22 | — | 100 | 0 | 0 | 226 | 21.7% |
| `union_h3_exit_red` | `market_favorable` | +40.56% | $14,055.76 | +0.00% | 0 | — | 138 | 0 | 0 | 239 | 73.9% |
| `union_h3_exit_red` | `partial_50` | -9.72% | $9,028.42 | -9.72% | 13 | — | 474 | 27 | 402 | 272 | 21.7% |
| `union_h3_exit_red` | `gap_miss` | -2.58% | $9,742.51 | -2.58% | 6 | — | 124 | 2 | 0 | 231 | 34.8% |
| `union_h3_exit_news_r` | `ideal` | +1.64% | $10,163.49 | +0.00% | 0 | — | 118 | 0 | 0 | 226 | 43.5% |
| `union_h3_exit_news_r` | `limit_prior` | -5.72% | $9,428.32 | -5.73% | 10 | — | 114 | 31 | 0 | 239 | 34.8% |
| `union_h3_exit_news_r` | `limit_open` | -6.44% | $9,356.24 | -6.44% | 8 | — | 125 | 9 | 0 | 237 | 39.1% |
| `union_h3_exit_news_r` | `market_mid` | -8.50% | $9,149.85 | -8.50% | 10 | — | 106 | 0 | 0 | 218 | 34.8% |
| `union_h3_exit_news_r` | `market_adverse` | -26.29% | $7,371.39 | -26.29% | 23 | — | 100 | 0 | 0 | 217 | 30.4% |
| `union_h3_exit_news_r` | `market_favorable` | +35.16% | $13,516.47 | +0.00% | 0 | — | 124 | 0 | 0 | 229 | 73.9% |
| `union_h3_exit_news_r` | `partial_50` | -10.42% | $8,958.03 | -10.42% | 16 | — | 442 | 21 | 376 | 255 | 30.4% |
| `union_h3_exit_news_r` | `gap_miss` | +2.19% | $10,219.55 | +0.00% | 0 | — | 122 | 1 | 0 | 226 | 43.5% |
| `coil_h3_exit_alarm` | `ideal` | -3.74% | $9,625.83 | -4.78% | 8 | — | 104 | 2 | 0 | 216 | 26.1% |
| `coil_h3_exit_alarm` | `limit_prior` | -4.57% | $9,542.63 | -4.57% | 8 | — | 101 | 23 | 0 | 227 | 17.4% |
| `coil_h3_exit_alarm` | `limit_open` | -8.93% | $9,106.86 | -8.93% | 12 | — | 104 | 9 | 0 | 221 | 17.4% |
| `coil_h3_exit_alarm` | `market_mid` | -13.79% | $8,620.59 | -13.84% | 18 | — | 96 | 2 | 0 | 212 | 21.7% |
| `coil_h3_exit_alarm` | `market_adverse` | -29.81% | $7,019.27 | -29.81% | 23 | — | 94 | 2 | 0 | 211 | 13.0% |
| `coil_h3_exit_alarm` | `market_favorable` | +25.05% | $12,505.20 | +0.00% | 0 | — | 114 | 2 | 0 | 219 | 52.2% |
| `coil_h3_exit_alarm` | `partial_50` | -15.84% | $8,415.92 | -15.84% | 21 | — | 408 | 21 | 346 | 244 | 17.4% |
| `coil_h3_exit_alarm` | `gap_miss` | -4.65% | $9,534.96 | -5.70% | 9 | — | 106 | 3 | 0 | 215 | 21.7% |
| `short_alarm_h1` | `ideal` | +1.71% | $10,170.79 | -0.07% | 1 | — | 58 | 0 | 0 | 77 | 17.4% |
| `short_alarm_h1` | `limit_prior` | +1.83% | $10,183.54 | +0.00% | 0 | — | 52 | 3 | 0 | 80 | 17.4% |
| `short_alarm_h1` | `limit_open` | +1.58% | $10,157.75 | -0.18% | 1 | — | 56 | 2 | 0 | 79 | 21.7% |
| `short_alarm_h1` | `market_mid` | -3.78% | $9,621.86 | -3.78% | 22 | — | 58 | 0 | 0 | 77 | 8.7% |
| `short_alarm_h1` | `market_adverse` | -13.68% | $8,632.53 | -13.68% | 22 | — | 58 | 0 | 0 | 77 | 0.0% |
| `short_alarm_h1` | `market_favorable` | +16.86% | $11,686.14 | +0.00% | 0 | — | 58 | 0 | 0 | 77 | 30.4% |
| `short_alarm_h1` | `partial_50` | -2.44% | $9,755.50 | -2.44% | 22 | — | 222 | 4 | 195 | 80 | 30.4% |
| `short_alarm_h1` | `gap_miss` | +1.71% | $10,170.79 | -0.07% | 1 | — | 58 | 0 | 0 | 77 | 17.4% |
| `short_alarm_h3` | `ideal` | +2.59% | $10,258.87 | +0.00% | 0 | — | 58 | 0 | 0 | 135 | 34.8% |
| `short_alarm_h3` | `limit_prior` | +1.84% | $10,184.31 | +0.00% | 0 | — | 52 | 8 | 0 | 137 | 34.8% |
| `short_alarm_h3` | `limit_open` | +1.64% | $10,164.37 | -0.87% | 4 | — | 56 | 4 | 0 | 137 | 34.8% |
| `short_alarm_h3` | `market_mid` | -3.08% | $9,692.23 | -3.62% | 21 | — | 58 | 0 | 0 | 135 | 17.4% |
| `short_alarm_h3` | `market_adverse` | -13.23% | $8,677.00 | -13.23% | 22 | — | 58 | 0 | 0 | 135 | 13.0% |
| `short_alarm_h3` | `market_favorable` | +14.78% | $11,478.19 | +0.00% | 0 | — | 58 | 0 | 0 | 135 | 43.5% |
| `short_alarm_h3` | `partial_50` | -1.88% | $9,811.76 | -2.25% | 21 | — | 217 | 6 | 191 | 140 | 39.1% |
| `short_alarm_h3` | `gap_miss` | +2.59% | $10,258.87 | +0.00% | 0 | — | 58 | 0 | 0 | 135 | 34.8% |
| `short_news_r_h1` | `ideal` | +0.28% | $10,027.47 | -3.17% | 13 | — | 74 | 8 | 0 | 28 | 34.8% |
| `short_news_r_h1` | `limit_prior` | +2.29% | $10,229.26 | -2.46% | 9 | — | 59 | 18 | 0 | 38 | 39.1% |
| `short_news_r_h1` | `limit_open` | -0.74% | $9,925.99 | -4.14% | 20 | — | 66 | 12 | 0 | 32 | 39.1% |
| `short_news_r_h1` | `market_mid` | -7.19% | $9,280.67 | -7.72% | 22 | — | 74 | 8 | 0 | 28 | 17.4% |
| `short_news_r_h1` | `market_adverse` | -20.56% | $7,944.43 | -20.56% | 22 | — | 74 | 8 | 0 | 28 | 8.7% |
| `short_news_r_h1` | `market_favorable` | +32.63% | $13,263.05 | +0.00% | 0 | — | 74 | 8 | 0 | 28 | 69.6% |
| `short_news_r_h1` | `partial_50` | -1.41% | $9,859.51 | -2.75% | 22 | — | 213 | 16 | 185 | 34 | 39.1% |
| `short_news_r_h1` | `gap_miss` | -0.31% | $9,968.98 | -3.72% | 17 | — | 72 | 9 | 0 | 29 | 30.4% |
| `short_r_down_h1` | `ideal` | +0.00% | $10,000.00 | +0.00% | 0 | — | 0 | 0 | 0 | 0 | 0.0% |
| `short_r_down_h1` | `limit_prior` | +0.00% | $10,000.00 | +0.00% | 0 | — | 0 | 0 | 0 | 0 | 0.0% |
| `short_r_down_h1` | `limit_open` | +0.00% | $10,000.00 | +0.00% | 0 | — | 0 | 0 | 0 | 0 | 0.0% |
| `short_r_down_h1` | `market_mid` | +0.00% | $10,000.00 | +0.00% | 0 | — | 0 | 0 | 0 | 0 | 0.0% |
| `short_r_down_h1` | `market_adverse` | +0.00% | $10,000.00 | +0.00% | 0 | — | 0 | 0 | 0 | 0 | 0.0% |
| `short_r_down_h1` | `market_favorable` | +0.00% | $10,000.00 | +0.00% | 0 | — | 0 | 0 | 0 | 0 | 0.0% |
| `short_r_down_h1` | `partial_50` | +0.00% | $10,000.00 | +0.00% | 0 | — | 0 | 0 | 0 | 0 | 0.0% |
| `short_r_down_h1` | `gap_miss` | +0.00% | $10,000.00 | +0.00% | 0 | — | 0 | 0 | 0 | 0 | 0.0% |
| `short_r_down_h3` | `ideal` | +0.00% | $10,000.00 | +0.00% | 0 | — | 0 | 0 | 0 | 0 | 0.0% |
| `short_r_down_h3` | `limit_prior` | +0.00% | $10,000.00 | +0.00% | 0 | — | 0 | 0 | 0 | 0 | 0.0% |
| `short_r_down_h3` | `limit_open` | +0.00% | $10,000.00 | +0.00% | 0 | — | 0 | 0 | 0 | 0 | 0.0% |
| `short_r_down_h3` | `market_mid` | +0.00% | $10,000.00 | +0.00% | 0 | — | 0 | 0 | 0 | 0 | 0.0% |
| `short_r_down_h3` | `market_adverse` | +0.00% | $10,000.00 | +0.00% | 0 | — | 0 | 0 | 0 | 0 | 0.0% |
| `short_r_down_h3` | `market_favorable` | +0.00% | $10,000.00 | +0.00% | 0 | — | 0 | 0 | 0 | 0 | 0.0% |
| `short_r_down_h3` | `partial_50` | +0.00% | $10,000.00 | +0.00% | 0 | — | 0 | 0 | 0 | 0 | 0.0% |
| `short_r_down_h3` | `gap_miss` | +0.00% | $10,000.00 | +0.00% | 0 | — | 0 | 0 | 0 | 0 | 0.0% |
| `short_extended_h1` | `ideal` | -9.21% | $9,078.46 | -9.21% | 17 | — | 146 | 13 | 0 | 86 | 30.4% |
| `short_extended_h1` | `limit_prior` | -7.87% | $9,213.18 | -9.37% | 17 | — | 126 | 29 | 0 | 102 | 30.4% |
| `short_extended_h1` | `limit_open` | -11.49% | $8,850.74 | -11.49% | 17 | — | 140 | 19 | 0 | 91 | 30.4% |
| `short_extended_h1` | `market_mid` | -24.44% | $7,556.16 | -24.44% | 21 | — | 146 | 13 | 0 | 86 | 17.4% |
| `short_extended_h1` | `market_adverse` | -46.18% | $5,381.84 | -46.18% | 23 | — | 146 | 13 | 0 | 86 | 0.0% |
| `short_extended_h1` | `market_favorable` | +39.98% | $13,997.64 | +0.00% | 0 | — | 144 | 13 | 0 | 87 | 60.9% |
| `short_extended_h1` | `partial_50` | -13.68% | $8,631.81 | -13.68% | 21 | — | 458 | 32 | 405 | 96 | 30.4% |
| `short_extended_h1` | `gap_miss` | -9.37% | $9,062.56 | -9.76% | 17 | — | 140 | 18 | 0 | 90 | 30.4% |
| `short_extended_h3` | `ideal` | -7.77% | $9,223.18 | -10.59% | 17 | — | 140 | 0 | 0 | 204 | 47.8% |
| `short_extended_h3` | `limit_prior` | -9.22% | $9,078.03 | -12.98% | 18 | — | 122 | 13 | 0 | 200 | 43.5% |
| `short_extended_h3` | `limit_open` | -9.55% | $9,044.54 | -11.88% | 17 | — | 134 | 4 | 0 | 202 | 43.5% |
| `short_extended_h3` | `market_mid` | -24.16% | $7,583.46 | -24.16% | 19 | — | 140 | 0 | 0 | 204 | 34.8% |
| `short_extended_h3` | `market_adverse` | -47.34% | $5,265.88 | -47.34% | 23 | — | 140 | 0 | 0 | 204 | 8.7% |
| `short_extended_h3` | `market_favorable` | +38.00% | $13,800.09 | +0.00% | 0 | — | 138 | 0 | 0 | 203 | 65.2% |
| `short_extended_h3` | `partial_50` | -13.56% | $8,644.42 | -13.94% | 20 | — | 423 | 33 | 379 | 223 | 34.8% |
| `short_extended_h3` | `gap_miss` | -8.13% | $9,186.53 | -10.92% | 17 | — | 136 | 2 | 0 | 202 | 43.5% |
| `short_last_red_h1` | `ideal` | -9.53% | $9,046.95 | -9.76% | 23 | — | 168 | 16 | 0 | 105 | 17.4% |
| `short_last_red_h1` | `limit_prior` | -5.45% | $9,455.19 | -5.85% | 21 | — | 154 | 31 | 0 | 120 | 39.1% |
| `short_last_red_h1` | `limit_open` | -11.84% | $8,816.18 | -12.02% | 23 | — | 159 | 26 | 0 | 115 | 21.7% |
| `short_last_red_h1` | `market_mid` | -19.97% | $8,002.74 | -19.97% | 23 | — | 168 | 16 | 0 | 105 | 0.0% |
| `short_last_red_h1` | `market_adverse` | -36.22% | $6,378.30 | -36.22% | 23 | — | 168 | 16 | 0 | 105 | 0.0% |
| `short_last_red_h1` | `market_favorable` | +30.26% | $13,026.01 | +0.00% | 0 | — | 168 | 16 | 0 | 105 | 69.6% |
| `short_last_red_h1` | `partial_50` | -10.78% | $8,921.61 | -11.09% | 23 | — | 493 | 40 | 427 | 125 | 17.4% |
| `short_last_red_h1` | `gap_miss` | -10.06% | $8,993.91 | -10.29% | 23 | — | 166 | 17 | 0 | 106 | 17.4% |
| `short_last_red_h3` | `ideal` | -5.29% | $9,470.70 | -12.27% | 22 | — | 166 | 0 | 0 | 256 | 34.8% |
| `short_last_red_h3` | `limit_prior` | -5.46% | $9,453.69 | -11.62% | 22 | — | 152 | 13 | 0 | 255 | 39.1% |
| `short_last_red_h3` | `limit_open` | -8.32% | $9,168.14 | -13.04% | 22 | — | 157 | 12 | 0 | 258 | 39.1% |
| `short_last_red_h3` | `market_mid` | -15.38% | $8,461.55 | -18.44% | 23 | — | 166 | 0 | 0 | 256 | 30.4% |
| `short_last_red_h3` | `market_adverse` | -31.75% | $6,825.46 | -32.28% | 23 | — | 166 | 0 | 0 | 256 | 26.1% |
| `short_last_red_h3` | `market_favorable` | +34.91% | $13,490.46 | +0.00% | 0 | — | 166 | 0 | 0 | 256 | 78.3% |
| `short_last_red_h3` | `partial_50` | -8.70% | $9,129.91 | -11.14% | 23 | — | 465 | 32 | 402 | 280 | 30.4% |
| `short_last_red_h3` | `gap_miss` | -5.63% | $9,436.85 | -12.60% | 22 | — | 164 | 1 | 0 | 255 | 34.8% |
| `flatten_h5_topheavy` | `ideal` | +3.94% | $10,394.54 | +0.00% | 0 | — | 92 | 12 | 0 | 274 | 52.2% |
| `flatten_h5_topheavy` | `limit_prior` | +2.73% | $10,273.37 | -0.28% | 1 | — | 94 | 33 | 0 | 287 | 52.2% |
| `flatten_h5_topheavy` | `limit_open` | +1.75% | $10,174.69 | +0.00% | 0 | — | 106 | 16 | 0 | 296 | 52.2% |
| `flatten_h5_topheavy` | `market_mid` | -3.60% | $9,640.18 | -3.60% | 6 | — | 75 | 10 | 0 | 247 | 34.8% |
| `flatten_h5_topheavy` | `market_adverse` | -14.42% | $8,558.02 | -14.42% | 21 | — | 70 | 4 | 0 | 235 | 34.8% |
| `flatten_h5_topheavy` | `market_favorable` | +24.43% | $12,442.68 | +0.00% | 0 | — | 96 | 12 | 0 | 280 | 56.5% |
| `flatten_h5_topheavy` | `partial_50` | -6.98% | $9,302.15 | -7.04% | 12 | — | 313 | 25 | 261 | 324 | 34.8% |
| `flatten_h5_topheavy` | `gap_miss` | +3.90% | $10,389.67 | +0.00% | 0 | — | 90 | 13 | 0 | 271 | 52.2% |
| `flatten_h5_half` | `ideal` | +1.40% | $10,140.39 | +0.00% | 0 | — | 116 | 12 | 0 | 308 | 52.2% |
| `flatten_h5_half` | `limit_prior` | -0.43% | $9,956.96 | -0.96% | 5 | — | 94 | 33 | 0 | 285 | 47.8% |
| `flatten_h5_half` | `limit_open` | +0.63% | $10,062.61 | +0.00% | 0 | — | 113 | 18 | 0 | 310 | 52.2% |
| `flatten_h5_half` | `market_mid` | -4.46% | $9,553.91 | -4.46% | 6 | — | 114 | 12 | 0 | 305 | 39.1% |
| `flatten_h5_half` | `market_adverse` | -14.50% | $8,549.92 | -14.50% | 21 | — | 114 | 12 | 0 | 305 | 30.4% |
| `flatten_h5_half` | `market_favorable` | +14.44% | $11,443.51 | +0.00% | 0 | — | 116 | 12 | 0 | 308 | 60.9% |
| `flatten_h5_half` | `partial_50` | -3.92% | $9,608.26 | -3.92% | 9 | — | 296 | 25 | 244 | 324 | 39.1% |
| `flatten_h5_half` | `gap_miss` | +0.23% | $10,022.95 | -0.28% | 1 | — | 114 | 13 | 0 | 305 | 47.8% |
| `flatten_h5_cut` | `ideal` | +7.99% | $10,799.08 | +0.00% | 0 | — | 90 | 12 | 0 | 275 | 60.9% |
| `flatten_h5_cut` | `limit_prior` | +2.50% | $10,250.07 | +0.00% | 0 | — | 90 | 33 | 0 | 281 | 52.2% |
| `flatten_h5_cut` | `limit_open` | +5.13% | $10,512.59 | +0.00% | 0 | — | 103 | 18 | 0 | 295 | 52.2% |
| `flatten_h5_cut` | `market_mid` | -0.84% | $9,916.15 | -0.84% | 4 | — | 78 | 12 | 0 | 259 | 43.5% |
| `flatten_h5_cut` | `market_adverse` | -15.41% | $8,459.39 | -15.41% | 16 | — | 76 | 12 | 0 | 258 | 43.5% |
| `flatten_h5_cut` | `market_favorable` | +27.84% | $12,783.65 | +0.00% | 0 | — | 92 | 12 | 0 | 274 | 65.2% |
| `flatten_h5_cut` | `partial_50` | -5.55% | $9,444.76 | -5.55% | 7 | — | 323 | 25 | 273 | 324 | 39.1% |
| `flatten_h5_cut` | `gap_miss` | +6.02% | $10,602.12 | +0.00% | 0 | — | 88 | 13 | 0 | 270 | 60.9% |
| `flatten_h5_trail` | `ideal` | +7.99% | $10,799.08 | +0.00% | 0 | — | 90 | 12 | 0 | 275 | 60.9% |
| `flatten_h5_trail` | `limit_prior` | +2.50% | $10,250.07 | +0.00% | 0 | — | 90 | 33 | 0 | 281 | 52.2% |
| `flatten_h5_trail` | `limit_open` | +5.13% | $10,512.59 | +0.00% | 0 | — | 103 | 18 | 0 | 295 | 52.2% |
| `flatten_h5_trail` | `market_mid` | -0.84% | $9,916.15 | -0.84% | 4 | — | 78 | 12 | 0 | 259 | 43.5% |
| `flatten_h5_trail` | `market_adverse` | -15.41% | $8,459.39 | -15.41% | 16 | — | 76 | 12 | 0 | 258 | 43.5% |
| `flatten_h5_trail` | `market_favorable` | +27.84% | $12,783.65 | +0.00% | 0 | — | 92 | 12 | 0 | 274 | 65.2% |
| `flatten_h5_trail` | `partial_50` | -5.55% | $9,444.76 | -5.55% | 7 | — | 323 | 25 | 273 | 324 | 39.1% |
| `flatten_h5_trail` | `gap_miss` | +6.02% | $10,602.12 | +0.00% | 0 | — | 88 | 13 | 0 | 270 | 60.9% |
| `flatten_h5_sizeup` | `ideal` | +7.99% | $10,799.08 | +0.00% | 0 | — | 90 | 12 | 0 | 275 | 60.9% |
| `flatten_h5_sizeup` | `limit_prior` | +2.50% | $10,250.07 | +0.00% | 0 | — | 90 | 33 | 0 | 281 | 52.2% |
| `flatten_h5_sizeup` | `limit_open` | +5.13% | $10,512.59 | +0.00% | 0 | — | 103 | 18 | 0 | 295 | 52.2% |
| `flatten_h5_sizeup` | `market_mid` | -0.84% | $9,916.15 | -0.84% | 4 | — | 78 | 12 | 0 | 259 | 43.5% |
| `flatten_h5_sizeup` | `market_adverse` | -15.41% | $8,459.39 | -15.41% | 16 | — | 76 | 12 | 0 | 258 | 43.5% |
| `flatten_h5_sizeup` | `market_favorable` | +27.84% | $12,783.65 | +0.00% | 0 | — | 92 | 12 | 0 | 274 | 65.2% |
| `flatten_h5_sizeup` | `partial_50` | -5.55% | $9,444.76 | -5.55% | 7 | — | 323 | 25 | 273 | 324 | 39.1% |
| `flatten_h5_sizeup` | `gap_miss` | +6.02% | $10,602.12 | +0.00% | 0 | — | 88 | 13 | 0 | 270 | 60.9% |
| `flatten_h3_rankw` | `ideal` | -1.40% | $9,860.24 | -1.40% | 5 | — | 98 | 0 | 0 | 167 | 39.1% |
| `flatten_h3_rankw` | `limit_prior` | -1.15% | $9,884.66 | -1.15% | 6 | — | 98 | 25 | 0 | 184 | 34.8% |
| `flatten_h3_rankw` | `limit_open` | -7.36% | $9,264.28 | -7.36% | 12 | — | 107 | 8 | 0 | 181 | 34.8% |
| `flatten_h3_rankw` | `market_mid` | -9.19% | $9,080.59 | -9.19% | 19 | — | 82 | 0 | 0 | 161 | 26.1% |
| `flatten_h3_rankw` | `market_adverse` | -23.02% | $7,697.75 | -23.02% | 23 | — | 70 | 0 | 0 | 155 | 21.7% |
| `flatten_h3_rankw` | `market_favorable` | +20.34% | $12,034.20 | +0.00% | 0 | — | 104 | 0 | 0 | 170 | 69.6% |
| `flatten_h3_rankw` | `partial_50` | -10.26% | $8,974.22 | -10.26% | 20 | — | 347 | 19 | 296 | 201 | 26.1% |
| `flatten_h3_rankw` | `gap_miss` | -1.41% | $9,859.28 | -1.41% | 5 | — | 96 | 1 | 0 | 166 | 39.1% |
| `flatten_h3_topheavy` | `ideal` | +0.27% | $10,026.73 | +0.00% | 0 | — | 100 | 0 | 0 | 168 | 39.1% |
| `flatten_h3_topheavy` | `limit_prior` | -1.02% | $9,898.30 | -1.02% | 7 | — | 98 | 26 | 0 | 187 | 43.5% |
| `flatten_h3_topheavy` | `limit_open` | -6.55% | $9,345.16 | -6.55% | 13 | — | 107 | 8 | 0 | 185 | 43.5% |
| `flatten_h3_topheavy` | `market_mid` | -7.34% | $9,265.70 | -7.34% | 14 | — | 86 | 0 | 0 | 161 | 30.4% |
| `flatten_h3_topheavy` | `market_adverse` | -20.99% | $7,900.56 | -20.99% | 23 | — | 84 | 0 | 0 | 162 | 21.7% |
| `flatten_h3_topheavy` | `market_favorable` | +21.61% | $12,160.98 | +0.00% | 0 | — | 102 | 0 | 0 | 169 | 65.2% |
| `flatten_h3_topheavy` | `partial_50` | -8.19% | $9,181.09 | -8.19% | 18 | — | 349 | 23 | 297 | 203 | 34.8% |
| `flatten_h3_topheavy` | `gap_miss` | +0.26% | $10,025.61 | +0.00% | 0 | — | 98 | 1 | 0 | 167 | 39.1% |
| `flatten_h3_half` | `ideal` | +0.02% | $10,002.12 | +0.00% | 0 | — | 122 | 0 | 0 | 179 | 43.5% |
| `flatten_h3_half` | `limit_prior` | -3.00% | $9,699.90 | -3.06% | 8 | — | 100 | 29 | 0 | 184 | 43.5% |
| `flatten_h3_half` | `limit_open` | -5.11% | $9,489.33 | -5.11% | 7 | — | 119 | 8 | 0 | 185 | 43.5% |
| `flatten_h3_half` | `market_mid` | -6.78% | $9,321.72 | -6.78% | 17 | — | 120 | 0 | 0 | 178 | 30.4% |
| `flatten_h3_half` | `market_adverse` | -18.41% | $8,159.08 | -18.41% | 23 | — | 120 | 0 | 0 | 178 | 21.7% |
| `flatten_h3_half` | `market_favorable` | +17.38% | $11,737.56 | +0.00% | 0 | — | 122 | 0 | 0 | 179 | 69.6% |
| `flatten_h3_half` | `partial_50` | -5.19% | $9,481.06 | -5.19% | 16 | — | 329 | 21 | 276 | 201 | 34.8% |
| `flatten_h3_half` | `gap_miss` | -0.62% | $9,937.54 | -0.62% | 4 | — | 120 | 1 | 0 | 178 | 43.5% |
| `flatten_h3_time` | `ideal` | +5.30% | $10,530.52 | +0.00% | 0 | — | 102 | 0 | 0 | 170 | 43.5% |
| `flatten_h3_time` | `limit_prior` | -0.37% | $9,963.38 | -0.62% | 6 | — | 100 | 26 | 0 | 187 | 43.5% |
| `flatten_h3_time` | `limit_open` | -3.99% | $9,601.16 | -3.99% | 5 | — | 107 | 8 | 0 | 186 | 43.5% |
| `flatten_h3_time` | `market_mid` | -4.41% | $9,559.52 | -4.41% | 9 | — | 88 | 0 | 0 | 163 | 34.8% |
| `flatten_h3_time` | `market_adverse` | -21.77% | $7,823.38 | -21.77% | 23 | — | 84 | 0 | 0 | 163 | 30.4% |
| `flatten_h3_time` | `market_favorable` | +39.18% | $13,917.61 | +0.00% | 0 | — | 104 | 0 | 0 | 171 | 73.9% |
| `flatten_h3_time` | `partial_50` | -7.33% | $9,266.61 | -7.33% | 12 | — | 361 | 24 | 310 | 204 | 34.8% |
| `flatten_h3_time` | `gap_miss` | +5.28% | $10,527.67 | +0.00% | 0 | — | 100 | 1 | 0 | 169 | 43.5% |
| `flatten_h3_cut` | `ideal` | +2.07% | $10,207.09 | +0.00% | 0 | — | 98 | 0 | 0 | 167 | 43.5% |
| `flatten_h3_cut` | `limit_prior` | -1.46% | $9,854.10 | -1.46% | 7 | — | 98 | 26 | 0 | 185 | 39.1% |
| `flatten_h3_cut` | `limit_open` | -4.61% | $9,539.41 | -4.61% | 6 | — | 105 | 8 | 0 | 184 | 43.5% |
| `flatten_h3_cut` | `market_mid` | -6.59% | $9,340.51 | -6.59% | 10 | — | 82 | 0 | 0 | 159 | 34.8% |
| `flatten_h3_cut` | `market_adverse` | -22.13% | $7,786.96 | -22.13% | 23 | — | 80 | 0 | 0 | 160 | 26.1% |
| `flatten_h3_cut` | `market_favorable` | +25.97% | $12,597.05 | +0.00% | 0 | — | 102 | 0 | 0 | 169 | 69.6% |
| `flatten_h3_cut` | `partial_50` | -7.28% | $9,272.18 | -7.28% | 12 | — | 361 | 23 | 310 | 203 | 34.8% |
| `flatten_h3_cut` | `gap_miss` | +2.04% | $10,203.47 | +0.00% | 0 | — | 94 | 1 | 0 | 165 | 43.5% |
| `flatten_h3_trail` | `ideal` | +2.07% | $10,207.09 | +0.00% | 0 | — | 98 | 0 | 0 | 167 | 43.5% |
| `flatten_h3_trail` | `limit_prior` | -1.46% | $9,854.10 | -1.46% | 7 | — | 98 | 26 | 0 | 185 | 39.1% |
| `flatten_h3_trail` | `limit_open` | -4.61% | $9,539.41 | -4.61% | 6 | — | 105 | 8 | 0 | 184 | 43.5% |
| `flatten_h3_trail` | `market_mid` | -6.59% | $9,340.51 | -6.59% | 10 | — | 82 | 0 | 0 | 159 | 34.8% |
| `flatten_h3_trail` | `market_adverse` | -22.13% | $7,786.96 | -22.13% | 23 | — | 80 | 0 | 0 | 160 | 26.1% |
| `flatten_h3_trail` | `market_favorable` | +25.97% | $12,597.05 | +0.00% | 0 | — | 102 | 0 | 0 | 169 | 69.6% |
| `flatten_h3_trail` | `partial_50` | -7.28% | $9,272.18 | -7.28% | 12 | — | 361 | 23 | 310 | 203 | 34.8% |
| `flatten_h3_trail` | `gap_miss` | +2.04% | $10,203.47 | +0.00% | 0 | — | 94 | 1 | 0 | 165 | 43.5% |
| `flatten_h3_sboost` | `ideal` | +2.01% | $10,201.24 | +0.00% | 0 | — | 100 | 0 | 0 | 170 | 43.5% |
| `flatten_h3_sboost` | `limit_prior` | -1.40% | $9,860.54 | -1.40% | 7 | — | 100 | 26 | 0 | 188 | 39.1% |
| `flatten_h3_sboost` | `limit_open` | -4.70% | $9,530.40 | -4.70% | 6 | — | 107 | 8 | 0 | 187 | 43.5% |
| `flatten_h3_sboost` | `market_mid` | -6.74% | $9,326.04 | -6.74% | 11 | — | 74 | 0 | 0 | 157 | 34.8% |
| `flatten_h3_sboost` | `market_adverse` | -21.99% | $7,800.65 | -21.99% | 23 | — | 92 | 0 | 0 | 166 | 26.1% |
| `flatten_h3_sboost` | `market_favorable` | +25.69% | $12,568.72 | +0.00% | 0 | — | 106 | 0 | 0 | 173 | 69.6% |
| `flatten_h3_sboost` | `partial_50` | -7.69% | $9,231.03 | -7.69% | 15 | — | 372 | 23 | 319 | 207 | 34.8% |
| `flatten_h3_sboost` | `gap_miss` | +1.97% | $10,196.53 | +0.00% | 0 | — | 98 | 1 | 0 | 169 | 43.5% |
| `flatten_h3_sizeup` | `ideal` | +2.07% | $10,207.09 | +0.00% | 0 | — | 98 | 0 | 0 | 167 | 43.5% |
| `flatten_h3_sizeup` | `limit_prior` | -1.46% | $9,854.10 | -1.46% | 7 | — | 98 | 26 | 0 | 185 | 39.1% |
| `flatten_h3_sizeup` | `limit_open` | -4.61% | $9,539.41 | -4.61% | 6 | — | 105 | 8 | 0 | 184 | 43.5% |
| `flatten_h3_sizeup` | `market_mid` | -6.59% | $9,340.51 | -6.59% | 10 | — | 82 | 0 | 0 | 159 | 34.8% |
| `flatten_h3_sizeup` | `market_adverse` | -22.13% | $7,786.96 | -22.13% | 23 | — | 80 | 0 | 0 | 160 | 26.1% |
| `flatten_h3_sizeup` | `market_favorable` | +25.97% | $12,597.05 | +0.00% | 0 | — | 102 | 0 | 0 | 169 | 69.6% |
| `flatten_h3_sizeup` | `partial_50` | -7.28% | $9,272.18 | -7.28% | 12 | — | 361 | 23 | 310 | 203 | 34.8% |
| `flatten_h3_sizeup` | `gap_miss` | +2.04% | $10,203.47 | +0.00% | 0 | — | 94 | 1 | 0 | 165 | 43.5% |
| `flatten_live_h1_rankw` | `ideal` | +4.03% | $10,403.05 | +0.00% | 0 | — | 48 | 0 | 0 | 0 | 13.0% |
| `flatten_live_h1_rankw` | `limit_prior` | +2.68% | $10,267.57 | +0.00% | 0 | — | 40 | 8 | 0 | 8 | 17.4% |
| `flatten_live_h1_rankw` | `limit_open` | +3.80% | $10,379.65 | +0.00% | 0 | — | 48 | 2 | 0 | 2 | 17.4% |
| `flatten_live_h1_rankw` | `market_mid` | -3.35% | $9,665.13 | -3.35% | 7 | — | 48 | 0 | 0 | 0 | 8.7% |
| `flatten_live_h1_rankw` | `market_adverse` | -16.04% | $8,396.46 | -16.04% | 18 | — | 44 | 0 | 0 | 2 | 0.0% |
| `flatten_live_h1_rankw` | `market_favorable` | +17.70% | $11,769.92 | +0.00% | 0 | — | 48 | 0 | 0 | 0 | 21.7% |
| `flatten_live_h1_rankw` | `partial_50` | -3.86% | $9,614.33 | -3.86% | 7 | — | 151 | 14 | 133 | 14 | 13.0% |
| `flatten_live_h1_rankw` | `gap_miss` | +3.00% | $10,300.30 | +0.00% | 0 | — | 46 | 1 | 0 | 1 | 8.7% |
| `flatten_live_h1_topheavy` | `ideal` | +7.19% | $10,718.60 | +0.00% | 0 | — | 48 | 0 | 0 | 0 | 13.0% |
| `flatten_live_h1_topheavy` | `limit_prior` | +7.10% | $10,709.76 | +0.00% | 0 | — | 40 | 8 | 0 | 8 | 17.4% |
| `flatten_live_h1_topheavy` | `limit_open` | +6.94% | $10,693.65 | +0.00% | 0 | — | 48 | 2 | 0 | 2 | 17.4% |
| `flatten_live_h1_topheavy` | `market_mid` | -1.23% | $9,877.52 | -1.23% | 6 | — | 48 | 0 | 0 | 0 | 13.0% |
| `flatten_live_h1_topheavy` | `market_adverse` | -15.23% | $8,476.54 | -15.23% | 18 | — | 48 | 0 | 0 | 0 | 0.0% |
| `flatten_live_h1_topheavy` | `market_favorable` | +20.10% | $12,010.13 | +0.00% | 0 | — | 48 | 0 | 0 | 0 | 21.7% |
| `flatten_live_h1_topheavy` | `partial_50` | -3.42% | $9,658.23 | -3.42% | 7 | — | 152 | 14 | 135 | 14 | 21.7% |
| `flatten_live_h1_topheavy` | `gap_miss` | +3.93% | $10,393.01 | +0.00% | 0 | — | 46 | 1 | 0 | 1 | 8.7% |
| `flatten_live_h1_half` | `ideal` | +3.74% | $10,373.74 | +0.00% | 0 | — | 48 | 0 | 0 | 0 | 13.0% |
| `flatten_live_h1_half` | `limit_prior` | +3.64% | $10,364.03 | +0.00% | 0 | — | 40 | 8 | 0 | 8 | 17.4% |
| `flatten_live_h1_half` | `limit_open` | +3.56% | $10,356.49 | +0.00% | 0 | — | 48 | 2 | 0 | 2 | 17.4% |
| `flatten_live_h1_half` | `market_mid` | -0.39% | $9,961.10 | -0.39% | 6 | — | 48 | 0 | 0 | 0 | 13.0% |
| `flatten_live_h1_half` | `market_adverse` | -7.75% | $9,225.11 | -7.75% | 18 | — | 48 | 0 | 0 | 0 | 0.0% |
| `flatten_live_h1_half` | `market_favorable` | +11.28% | $11,128.22 | +0.00% | 0 | — | 48 | 0 | 0 | 0 | 21.7% |
| `flatten_live_h1_half` | `partial_50` | -1.32% | $9,868.02 | -1.32% | 7 | — | 140 | 14 | 123 | 14 | 26.1% |
| `flatten_live_h1_half` | `gap_miss` | +1.42% | $10,142.27 | +0.00% | 0 | — | 46 | 1 | 0 | 1 | 8.7% |
| `flatten_live_h1_time` | `ideal` | +8.22% | $10,821.57 | +0.00% | 0 | — | 48 | 0 | 0 | 0 | 13.0% |
| `flatten_live_h1_time` | `limit_prior` | +7.88% | $10,788.48 | +0.00% | 0 | — | 40 | 8 | 0 | 8 | 17.4% |
| `flatten_live_h1_time` | `limit_open` | +7.84% | $10,784.48 | +0.00% | 0 | — | 48 | 2 | 0 | 2 | 17.4% |
| `flatten_live_h1_time` | `market_mid` | -0.23% | $9,977.26 | -0.23% | 6 | — | 48 | 0 | 0 | 0 | 13.0% |
| `flatten_live_h1_time` | `market_adverse` | -14.76% | $8,523.64 | -14.76% | 18 | — | 48 | 0 | 0 | 0 | 0.0% |
| `flatten_live_h1_time` | `market_favorable` | +24.22% | $12,421.60 | +0.00% | 0 | — | 48 | 0 | 0 | 0 | 21.7% |
| `flatten_live_h1_time` | `partial_50` | -2.16% | $9,784.31 | -2.17% | 6 | — | 155 | 16 | 139 | 16 | 26.1% |
| `flatten_live_h1_time` | `gap_miss` | +3.51% | $10,350.59 | +0.00% | 0 | — | 46 | 1 | 0 | 1 | 8.7% |
| `flatten_live_h1_cut` | `ideal` | +8.22% | $10,821.57 | +0.00% | 0 | — | 48 | 0 | 0 | 0 | 13.0% |
| `flatten_live_h1_cut` | `limit_prior` | +7.88% | $10,788.48 | +0.00% | 0 | — | 40 | 8 | 0 | 8 | 17.4% |
| `flatten_live_h1_cut` | `limit_open` | +7.84% | $10,784.48 | +0.00% | 0 | — | 48 | 2 | 0 | 2 | 17.4% |
| `flatten_live_h1_cut` | `market_mid` | -0.23% | $9,977.26 | -0.23% | 6 | — | 48 | 0 | 0 | 0 | 13.0% |
| `flatten_live_h1_cut` | `market_adverse` | -14.76% | $8,523.64 | -14.76% | 18 | — | 48 | 0 | 0 | 0 | 0.0% |
| `flatten_live_h1_cut` | `market_favorable` | +24.22% | $12,421.60 | +0.00% | 0 | — | 48 | 0 | 0 | 0 | 21.7% |
| `flatten_live_h1_cut` | `partial_50` | -2.16% | $9,784.31 | -2.17% | 6 | — | 155 | 16 | 139 | 16 | 26.1% |
| `flatten_live_h1_cut` | `gap_miss` | +3.51% | $10,350.59 | +0.00% | 0 | — | 46 | 1 | 0 | 1 | 8.7% |
| `flatten_live_h1_trail` | `ideal` | +8.22% | $10,821.57 | +0.00% | 0 | — | 48 | 0 | 0 | 0 | 13.0% |
| `flatten_live_h1_trail` | `limit_prior` | +7.88% | $10,788.48 | +0.00% | 0 | — | 40 | 8 | 0 | 8 | 17.4% |
| `flatten_live_h1_trail` | `limit_open` | +7.84% | $10,784.48 | +0.00% | 0 | — | 48 | 2 | 0 | 2 | 17.4% |
| `flatten_live_h1_trail` | `market_mid` | -0.23% | $9,977.26 | -0.23% | 6 | — | 48 | 0 | 0 | 0 | 13.0% |
| `flatten_live_h1_trail` | `market_adverse` | -14.76% | $8,523.64 | -14.76% | 18 | — | 48 | 0 | 0 | 0 | 0.0% |
| `flatten_live_h1_trail` | `market_favorable` | +24.22% | $12,421.60 | +0.00% | 0 | — | 48 | 0 | 0 | 0 | 21.7% |
| `flatten_live_h1_trail` | `partial_50` | -2.16% | $9,784.31 | -2.17% | 6 | — | 155 | 16 | 139 | 16 | 26.1% |
| `flatten_live_h1_trail` | `gap_miss` | +3.51% | $10,350.59 | +0.00% | 0 | — | 46 | 1 | 0 | 1 | 8.7% |
| `flatten_live_h1_sboost` | `ideal` | +8.22% | $10,821.57 | +0.00% | 0 | — | 48 | 0 | 0 | 0 | 13.0% |
| `flatten_live_h1_sboost` | `limit_prior` | +7.88% | $10,788.48 | +0.00% | 0 | — | 40 | 8 | 0 | 8 | 17.4% |
| `flatten_live_h1_sboost` | `limit_open` | +7.84% | $10,784.48 | +0.00% | 0 | — | 48 | 2 | 0 | 2 | 17.4% |
| `flatten_live_h1_sboost` | `market_mid` | -0.23% | $9,977.26 | -0.23% | 6 | — | 48 | 0 | 0 | 0 | 13.0% |
| `flatten_live_h1_sboost` | `market_adverse` | -14.76% | $8,523.64 | -14.76% | 18 | — | 48 | 0 | 0 | 0 | 0.0% |
| `flatten_live_h1_sboost` | `market_favorable` | +24.22% | $12,421.60 | +0.00% | 0 | — | 48 | 0 | 0 | 0 | 21.7% |
| `flatten_live_h1_sboost` | `partial_50` | -2.16% | $9,784.31 | -2.17% | 6 | — | 155 | 16 | 139 | 16 | 26.1% |
| `flatten_live_h1_sboost` | `gap_miss` | +3.51% | $10,350.59 | +0.00% | 0 | — | 46 | 1 | 0 | 1 | 8.7% |
| `union_h5_rankw` | `ideal` | -0.93% | $9,907.14 | -0.97% | 4 | — | 108 | 12 | 0 | 340 | 56.5% |
| `union_h5_rankw` | `limit_prior` | -3.42% | $9,658.04 | -3.81% | 5 | — | 116 | 37 | 0 | 359 | 43.5% |
| `union_h5_rankw` | `limit_open` | -4.05% | $9,594.45 | -4.05% | 5 | — | 133 | 19 | 0 | 378 | 52.2% |
| `union_h5_rankw` | `market_mid` | -8.09% | $9,191.54 | -8.09% | 7 | — | 96 | 12 | 0 | 326 | 39.1% |
| `union_h5_rankw` | `market_adverse` | -20.37% | $7,962.82 | -20.37% | 23 | — | 76 | 12 | 0 | 298 | 39.1% |
| `union_h5_rankw` | `market_favorable` | +17.53% | $11,752.65 | +0.00% | 0 | — | 118 | 12 | 0 | 351 | 60.9% |
| `union_h5_rankw` | `partial_50` | -8.75% | $9,124.85 | -8.75% | 14 | — | 375 | 35 | 321 | 406 | 30.4% |
| `union_h5_rankw` | `gap_miss` | -0.93% | $9,906.70 | -0.97% | 4 | — | 106 | 13 | 0 | 337 | 56.5% |
| `union_h5_topheavy` | `ideal` | +1.73% | $10,172.57 | +0.00% | 0 | — | 116 | 12 | 0 | 348 | 56.5% |
| `union_h5_topheavy` | `limit_prior` | -0.40% | $9,960.16 | -1.07% | 5 | — | 116 | 37 | 0 | 359 | 52.2% |
| `union_h5_topheavy` | `limit_open` | -1.28% | $9,872.38 | -1.40% | 4 | — | 133 | 19 | 0 | 378 | 56.5% |
| `union_h5_topheavy` | `market_mid` | -5.02% | $9,498.23 | -5.02% | 6 | — | 96 | 12 | 0 | 322 | 39.1% |
| `union_h5_topheavy` | `market_adverse` | -15.77% | $8,422.72 | -15.77% | 21 | — | 94 | 12 | 0 | 325 | 34.8% |
| `union_h5_topheavy` | `market_favorable` | +23.36% | $12,335.93 | +0.00% | 0 | — | 122 | 12 | 0 | 357 | 60.9% |
| `union_h5_topheavy` | `partial_50` | -6.58% | $9,341.71 | -6.59% | 14 | — | 376 | 37 | 318 | 411 | 34.8% |
| `union_h5_topheavy` | `gap_miss` | +1.69% | $10,168.94 | +0.00% | 0 | — | 116 | 13 | 0 | 348 | 56.5% |
| `union_h5_half` | `ideal` | +1.55% | $10,154.95 | +0.00% | 0 | — | 144 | 12 | 0 | 388 | 56.5% |
| `union_h5_half` | `limit_prior` | -1.90% | $9,810.16 | -2.36% | 6 | — | 116 | 37 | 0 | 359 | 52.2% |
| `union_h5_half` | `limit_open` | -0.37% | $9,963.32 | -0.63% | 4 | — | 139 | 19 | 0 | 387 | 52.2% |
| `union_h5_half` | `market_mid` | -4.17% | $9,583.12 | -4.17% | 6 | — | 140 | 12 | 0 | 382 | 39.1% |
| `union_h5_half` | `market_adverse` | -14.51% | $8,548.91 | -14.51% | 21 | — | 140 | 12 | 0 | 382 | 39.1% |
| `union_h5_half` | `market_favorable` | +18.39% | $11,838.66 | +0.00% | 0 | — | 144 | 12 | 0 | 388 | 65.2% |
| `union_h5_half` | `partial_50` | -3.87% | $9,612.72 | -3.87% | 10 | — | 361 | 33 | 301 | 407 | 39.1% |
| `union_h5_half` | `gap_miss` | +0.46% | $10,045.82 | +0.00% | 0 | — | 142 | 13 | 0 | 385 | 56.5% |
| `union_h5_time` | `ideal` | +5.01% | $10,501.25 | +0.00% | 0 | — | 120 | 12 | 0 | 359 | 60.9% |
| `union_h5_time` | `limit_prior` | -0.10% | $9,989.77 | -0.64% | 4 | — | 116 | 37 | 0 | 360 | 52.2% |
| `union_h5_time` | `limit_open` | +3.05% | $10,304.95 | +0.00% | 0 | — | 131 | 19 | 0 | 378 | 56.5% |
| `union_h5_time` | `market_mid` | -3.27% | $9,673.43 | -3.27% | 5 | — | 102 | 12 | 0 | 336 | 47.8% |
| `union_h5_time` | `market_adverse` | -17.26% | $8,274.34 | -17.26% | 18 | — | 100 | 12 | 0 | 333 | 47.8% |
| `union_h5_time` | `market_favorable` | +28.18% | $12,818.40 | +0.00% | 0 | — | 122 | 12 | 0 | 358 | 69.6% |
| `union_h5_time` | `partial_50` | -5.03% | $9,497.11 | -5.03% | 7 | — | 389 | 36 | 331 | 410 | 39.1% |
| `union_h5_time` | `gap_miss` | +4.98% | $10,498.08 | +0.00% | 0 | — | 118 | 13 | 0 | 356 | 56.5% |
| `union_h5_cut` | `ideal` | +3.83% | $10,382.58 | +0.00% | 0 | — | 118 | 12 | 0 | 355 | 60.9% |
| `union_h5_cut` | `limit_prior` | -1.27% | $9,873.04 | -1.97% | 4 | — | 114 | 37 | 0 | 356 | 47.8% |
| `union_h5_cut` | `limit_open` | +2.00% | $10,199.72 | +0.00% | 0 | — | 129 | 19 | 0 | 374 | 56.5% |
| `union_h5_cut` | `market_mid` | -4.24% | $9,576.00 | -4.24% | 6 | — | 100 | 12 | 0 | 332 | 43.5% |
| `union_h5_cut` | `market_adverse` | -17.77% | $8,223.52 | -17.77% | 18 | — | 98 | 12 | 0 | 329 | 43.5% |
| `union_h5_cut` | `market_favorable` | +26.01% | $12,601.09 | +0.00% | 0 | — | 120 | 12 | 0 | 354 | 69.6% |
| `union_h5_cut` | `partial_50` | -5.01% | $9,499.04 | -5.01% | 7 | — | 388 | 35 | 330 | 409 | 39.1% |
| `union_h5_cut` | `gap_miss` | +3.77% | $10,376.71 | +0.00% | 0 | — | 116 | 13 | 0 | 352 | 56.5% |
| `union_h5_trail` | `ideal` | +3.83% | $10,382.58 | +0.00% | 0 | — | 118 | 12 | 0 | 355 | 60.9% |
| `union_h5_trail` | `limit_prior` | -1.27% | $9,873.04 | -1.97% | 4 | — | 114 | 37 | 0 | 356 | 47.8% |
| `union_h5_trail` | `limit_open` | +2.00% | $10,199.72 | +0.00% | 0 | — | 129 | 19 | 0 | 374 | 56.5% |
| `union_h5_trail` | `market_mid` | -4.24% | $9,576.00 | -4.24% | 6 | — | 100 | 12 | 0 | 332 | 43.5% |
| `union_h5_trail` | `market_adverse` | -17.77% | $8,223.52 | -17.77% | 18 | — | 98 | 12 | 0 | 329 | 43.5% |
| `union_h5_trail` | `market_favorable` | +26.01% | $12,601.09 | +0.00% | 0 | — | 120 | 12 | 0 | 354 | 69.6% |
| `union_h5_trail` | `partial_50` | -5.01% | $9,499.04 | -5.01% | 7 | — | 388 | 35 | 330 | 409 | 39.1% |
| `union_h5_trail` | `gap_miss` | +3.77% | $10,376.71 | +0.00% | 0 | — | 116 | 13 | 0 | 352 | 56.5% |
| `union_h5_sizeup` | `ideal` | +3.83% | $10,382.58 | +0.00% | 0 | — | 118 | 12 | 0 | 355 | 60.9% |
| `union_h5_sizeup` | `limit_prior` | -1.27% | $9,873.04 | -1.97% | 4 | — | 114 | 37 | 0 | 356 | 47.8% |
| `union_h5_sizeup` | `limit_open` | +2.00% | $10,199.72 | +0.00% | 0 | — | 129 | 19 | 0 | 374 | 56.5% |
| `union_h5_sizeup` | `market_mid` | -4.24% | $9,576.00 | -4.24% | 6 | — | 100 | 12 | 0 | 332 | 43.5% |
| `union_h5_sizeup` | `market_adverse` | -17.77% | $8,223.52 | -17.77% | 18 | — | 98 | 12 | 0 | 329 | 43.5% |
| `union_h5_sizeup` | `market_favorable` | +26.01% | $12,601.09 | +0.00% | 0 | — | 120 | 12 | 0 | 354 | 69.6% |
| `union_h5_sizeup` | `partial_50` | -5.01% | $9,499.04 | -5.01% | 7 | — | 388 | 35 | 330 | 409 | 39.1% |
| `union_h5_sizeup` | `gap_miss` | +3.77% | $10,376.71 | +0.00% | 0 | — | 116 | 13 | 0 | 352 | 56.5% |
| `union_h3_rankw` | `ideal` | -2.72% | $9,728.06 | -2.72% | 5 | — | 124 | 0 | 0 | 226 | 47.8% |
| `union_h3_rankw` | `limit_prior` | -5.59% | $9,440.97 | -5.59% | 8 | — | 120 | 32 | 0 | 241 | 43.5% |
| `union_h3_rankw` | `limit_open` | -8.13% | $9,187.29 | -8.13% | 12 | — | 129 | 9 | 0 | 236 | 43.5% |
| `union_h3_rankw` | `market_mid` | -10.98% | $8,902.00 | -10.98% | 19 | — | 106 | 0 | 0 | 217 | 39.1% |
| `union_h3_rankw` | `market_adverse` | -26.32% | $7,368.16 | -26.32% | 23 | — | 86 | 0 | 0 | 209 | 34.8% |
| `union_h3_rankw` | `market_favorable` | +25.67% | $12,566.70 | +0.00% | 0 | — | 128 | 0 | 0 | 228 | 73.9% |
| `union_h3_rankw` | `partial_50` | -12.11% | $8,788.69 | -12.11% | 20 | — | 426 | 21 | 362 | 255 | 26.1% |
| `union_h3_rankw` | `gap_miss` | -3.57% | $9,642.74 | -3.57% | 5 | — | 122 | 1 | 0 | 227 | 47.8% |
| `union_h3_topheavy` | `ideal` | +1.12% | $10,112.00 | +0.00% | 0 | — | 124 | 0 | 0 | 228 | 43.5% |
| `union_h3_topheavy` | `limit_prior` | -4.55% | $9,545.13 | -4.56% | 10 | — | 116 | 31 | 0 | 240 | 52.2% |
| `union_h3_topheavy` | `limit_open` | -6.65% | $9,335.37 | -6.65% | 13 | — | 131 | 9 | 0 | 239 | 47.8% |
| `union_h3_topheavy` | `market_mid` | -7.53% | $9,247.04 | -7.53% | 13 | — | 108 | 0 | 0 | 220 | 34.8% |
| `union_h3_topheavy` | `market_adverse` | -21.50% | $7,850.41 | -21.50% | 23 | — | 104 | 0 | 0 | 216 | 30.4% |
| `union_h3_topheavy` | `market_favorable` | +29.23% | $12,922.73 | +0.00% | 0 | — | 126 | 0 | 0 | 227 | 69.6% |
| `union_h3_topheavy` | `partial_50` | -10.29% | $8,971.28 | -10.29% | 17 | — | 428 | 25 | 364 | 259 | 39.1% |
| `union_h3_topheavy` | `gap_miss` | +1.11% | $10,111.07 | +0.00% | 0 | — | 122 | 1 | 0 | 227 | 43.5% |
| `union_h3_half` | `ideal` | -2.93% | $9,707.02 | -2.93% | 5 | — | 152 | 0 | 0 | 236 | 47.8% |
| `union_h3_half` | `limit_prior` | -5.88% | $9,411.88 | -5.98% | 11 | — | 124 | 37 | 0 | 243 | 47.8% |
| `union_h3_half` | `limit_open` | -5.93% | $9,407.00 | -5.93% | 8 | — | 147 | 9 | 0 | 241 | 39.1% |
| `union_h3_half` | `market_mid` | -9.80% | $9,019.62 | -9.80% | 16 | — | 150 | 0 | 0 | 235 | 34.8% |
| `union_h3_half` | `market_adverse` | -21.27% | $7,872.95 | -21.27% | 23 | — | 150 | 0 | 0 | 235 | 21.7% |
| `union_h3_half` | `market_favorable` | +18.90% | $11,890.20 | +0.00% | 0 | — | 152 | 0 | 0 | 236 | 73.9% |
| `union_h3_half` | `partial_50` | -6.63% | $9,336.66 | -6.63% | 16 | — | 404 | 25 | 340 | 259 | 39.1% |
| `union_h3_half` | `gap_miss` | -3.47% | $9,653.35 | -3.47% | 6 | — | 150 | 1 | 0 | 235 | 47.8% |
| `union_h3_time` | `ideal` | +4.53% | $10,453.34 | +0.00% | 0 | — | 126 | 0 | 0 | 231 | 52.2% |
| `union_h3_time` | `limit_prior` | -3.15% | $9,684.52 | -3.15% | 8 | — | 120 | 32 | 0 | 245 | 43.5% |
| `union_h3_time` | `limit_open` | -5.03% | $9,496.81 | -5.03% | 6 | — | 129 | 10 | 0 | 240 | 47.8% |
| `union_h3_time` | `market_mid` | -5.24% | $9,475.71 | -5.24% | 10 | — | 110 | 0 | 0 | 221 | 39.1% |
| `union_h3_time` | `market_adverse` | -23.95% | $7,604.82 | -23.95% | 23 | — | 92 | 0 | 0 | 214 | 34.8% |
| `union_h3_time` | `market_favorable` | +41.61% | $14,161.03 | +0.00% | 0 | — | 128 | 0 | 0 | 230 | 78.3% |
| `union_h3_time` | `partial_50` | -9.24% | $9,076.29 | -9.24% | 15 | — | 444 | 26 | 380 | 260 | 39.1% |
| `union_h3_time` | `gap_miss` | +5.54% | $10,554.27 | +0.00% | 0 | — | 126 | 1 | 0 | 229 | 52.2% |
| `union_h3_trail` | `ideal` | +4.07% | $10,406.49 | +0.00% | 0 | — | 124 | 0 | 0 | 229 | 52.2% |
| `union_h3_trail` | `limit_prior` | -4.49% | $9,550.56 | -4.50% | 10 | — | 116 | 32 | 0 | 241 | 43.5% |
| `union_h3_trail` | `limit_open` | -5.24% | $9,476.14 | -5.24% | 7 | — | 127 | 10 | 0 | 238 | 47.8% |
| `union_h3_trail` | `market_mid` | -5.58% | $9,441.72 | -5.58% | 9 | — | 108 | 0 | 0 | 219 | 39.1% |
| `union_h3_trail` | `market_adverse` | -23.90% | $7,609.67 | -23.90% | 23 | — | 94 | 0 | 0 | 214 | 34.8% |
| `union_h3_trail` | `market_favorable` | +39.95% | $13,994.88 | +0.00% | 0 | — | 126 | 0 | 0 | 228 | 78.3% |
| `union_h3_trail` | `partial_50` | -9.21% | $9,079.20 | -9.21% | 15 | — | 444 | 25 | 380 | 259 | 39.1% |
| `union_h3_trail` | `gap_miss` | +4.99% | $10,499.15 | +0.00% | 0 | — | 124 | 1 | 0 | 227 | 52.2% |
| `union_h3_sboost` | `ideal` | +2.39% | $10,239.06 | +0.00% | 0 | — | 132 | 0 | 0 | 237 | 52.2% |
| `union_h3_sboost` | `limit_prior` | -5.95% | $9,405.10 | -6.01% | 7 | — | 122 | 33 | 0 | 249 | 43.5% |
| `union_h3_sboost` | `limit_open` | -5.23% | $9,476.81 | -5.23% | 7 | — | 135 | 8 | 0 | 246 | 43.5% |
| `union_h3_sboost` | `market_mid` | -7.08% | $9,291.85 | -7.08% | 9 | — | 112 | 0 | 0 | 225 | 39.1% |
| `union_h3_sboost` | `market_adverse` | -23.68% | $7,631.90 | -23.68% | 23 | — | 110 | 0 | 0 | 224 | 34.8% |
| `union_h3_sboost` | `market_favorable` | +37.71% | $13,770.56 | +0.00% | 0 | — | 136 | 0 | 0 | 237 | 73.9% |
| `union_h3_sboost` | `partial_50` | -10.11% | $8,989.39 | -10.11% | 16 | — | 472 | 25 | 403 | 269 | 39.1% |
| `union_h3_sboost` | `gap_miss` | +3.46% | $10,345.75 | +0.00% | 0 | — | 130 | 1 | 0 | 234 | 52.2% |
| `union_h3_sizeup` | `ideal` | +2.45% | $10,244.95 | +0.00% | 0 | — | 124 | 0 | 0 | 228 | 52.2% |
| `union_h3_sizeup` | `limit_prior` | -6.40% | $9,359.74 | -6.47% | 10 | — | 116 | 33 | 0 | 241 | 43.5% |
| `union_h3_sizeup` | `limit_open` | -5.24% | $9,476.14 | -5.24% | 7 | — | 127 | 9 | 0 | 237 | 47.8% |
| `union_h3_sizeup` | `market_mid` | -6.86% | $9,313.90 | -6.86% | 9 | — | 108 | 0 | 0 | 218 | 39.1% |
| `union_h3_sizeup` | `market_adverse` | -24.52% | $7,547.77 | -24.52% | 23 | — | 94 | 0 | 0 | 213 | 34.8% |
| `union_h3_sizeup` | `market_favorable` | +38.03% | $13,803.43 | +0.00% | 0 | — | 126 | 0 | 0 | 227 | 73.9% |
| `union_h3_sizeup` | `partial_50` | -9.54% | $9,045.58 | -9.54% | 15 | — | 443 | 25 | 379 | 259 | 39.1% |
| `union_h3_sizeup` | `gap_miss` | +3.51% | $10,351.27 | +0.00% | 0 | — | 124 | 1 | 0 | 226 | 52.2% |
| `union_h1_rankw` | `ideal` | +3.23% | $10,323.20 | +0.00% | 0 | — | 156 | 15 | 0 | 103 | 39.1% |
| `union_h1_rankw` | `limit_prior` | +1.77% | $10,176.57 | +0.00% | 0 | — | 124 | 36 | 0 | 125 | 52.2% |
| `union_h1_rankw` | `limit_open` | -1.04% | $9,896.03 | -1.04% | 3 | — | 149 | 24 | 0 | 113 | 30.4% |
| `union_h1_rankw` | `market_mid` | -13.42% | $8,657.94 | -13.42% | 23 | — | 156 | 15 | 0 | 103 | 17.4% |
| `union_h1_rankw` | `market_adverse` | -37.76% | $6,223.56 | -37.76% | 23 | — | 152 | 15 | 0 | 105 | 0.0% |
| `union_h1_rankw` | `market_favorable` | +56.14% | $15,614.35 | +0.00% | 0 | — | 156 | 15 | 0 | 103 | 65.2% |
| `union_h1_rankw` | `partial_50` | -11.07% | $8,892.97 | -11.07% | 23 | — | 485 | 34 | 420 | 119 | 21.7% |
| `union_h1_rankw` | `gap_miss` | +2.28% | $10,227.67 | +0.00% | 0 | — | 154 | 16 | 0 | 104 | 34.8% |
| `union_h1_half` | `ideal` | +0.94% | $10,094.28 | +0.00% | 0 | — | 156 | 15 | 0 | 103 | 30.4% |
| `union_h1_half` | `limit_prior` | +0.20% | $10,020.05 | -0.16% | 3 | — | 124 | 36 | 0 | 125 | 34.8% |
| `union_h1_half` | `limit_open` | -1.24% | $9,875.57 | -1.24% | 9 | — | 149 | 24 | 0 | 113 | 26.1% |
| `union_h1_half` | `market_mid` | -9.29% | $9,071.08 | -9.29% | 23 | — | 156 | 15 | 0 | 103 | 13.0% |
| `union_h1_half` | `market_adverse` | -25.47% | $7,452.84 | -25.47% | 23 | — | 156 | 15 | 0 | 103 | 0.0% |
| `union_h1_half` | `market_favorable` | +33.20% | $13,319.94 | +0.00% | 0 | — | 156 | 15 | 0 | 103 | 69.6% |
| `union_h1_half` | `partial_50` | -6.57% | $9,342.74 | -6.57% | 23 | — | 442 | 33 | 375 | 119 | 17.4% |
| `union_h1_half` | `gap_miss` | -1.47% | $9,852.99 | -1.47% | 7 | — | 154 | 16 | 0 | 104 | 26.1% |
| `union_h1_time` | `ideal` | +3.94% | $10,393.91 | +0.00% | 0 | — | 184 | 16 | 0 | 103 | 34.8% |
| `union_h1_time` | `limit_prior` | +3.88% | $10,388.08 | +0.00% | 0 | — | 146 | 37 | 0 | 125 | 43.5% |
| `union_h1_time` | `limit_open` | -0.25% | $9,975.01 | -0.25% | 5 | — | 173 | 25 | 0 | 113 | 26.1% |
| `union_h1_time` | `market_mid` | -17.24% | $8,275.77 | -17.24% | 23 | — | 184 | 16 | 0 | 103 | 17.4% |
| `union_h1_time` | `market_adverse` | -45.66% | $5,433.80 | -45.66% | 23 | — | 182 | 16 | 0 | 104 | 0.0% |
| `union_h1_time` | `market_favorable` | +84.64% | $18,463.75 | +0.00% | 0 | — | 184 | 16 | 0 | 103 | 69.6% |
| `union_h1_time` | `partial_50` | -10.68% | $8,932.21 | -10.68% | 23 | — | 506 | 36 | 440 | 121 | 26.1% |
| `union_h1_time` | `gap_miss` | -0.76% | $9,924.30 | -0.76% | 6 | — | 182 | 17 | 0 | 104 | 30.4% |
| `union_h1_cut` | `ideal` | +4.54% | $10,454.17 | +0.00% | 0 | — | 160 | 15 | 0 | 102 | 34.8% |
| `union_h1_cut` | `limit_prior` | +3.36% | $10,335.61 | +0.00% | 0 | — | 128 | 36 | 0 | 124 | 43.5% |
| `union_h1_cut` | `limit_open` | +0.05% | $10,005.09 | -0.24% | 2 | — | 153 | 24 | 0 | 112 | 21.7% |
| `union_h1_cut` | `market_mid` | -15.01% | $8,499.03 | -15.01% | 23 | — | 160 | 15 | 0 | 102 | 17.4% |
| `union_h1_cut` | `market_adverse` | -42.41% | $5,759.16 | -42.41% | 23 | — | 162 | 15 | 0 | 103 | 0.0% |
| `union_h1_cut` | `market_favorable` | +75.73% | $17,573.22 | +0.00% | 0 | — | 158 | 15 | 0 | 102 | 69.6% |
| `union_h1_cut` | `partial_50` | -9.59% | $9,041.16 | -9.59% | 23 | — | 499 | 34 | 436 | 119 | 21.7% |
| `union_h1_cut` | `gap_miss` | -0.15% | $9,984.54 | -0.15% | 3 | — | 158 | 16 | 0 | 103 | 30.4% |
| `union_h1_trail` | `ideal` | +4.63% | $10,463.52 | +0.00% | 0 | — | 158 | 15 | 0 | 102 | 34.8% |
| `union_h1_trail` | `limit_prior` | +3.37% | $10,336.68 | +0.00% | 0 | — | 126 | 36 | 0 | 124 | 43.5% |
| `union_h1_trail` | `limit_open` | +0.14% | $10,013.55 | -0.24% | 2 | — | 151 | 24 | 0 | 112 | 21.7% |
| `union_h1_trail` | `market_mid` | -15.01% | $8,499.03 | -15.01% | 23 | — | 160 | 15 | 0 | 102 | 17.4% |
| `union_h1_trail` | `market_adverse` | -41.81% | $5,818.52 | -41.81% | 23 | — | 158 | 15 | 0 | 103 | 0.0% |
| `union_h1_trail` | `market_favorable` | +75.73% | $17,573.22 | +0.00% | 0 | — | 158 | 15 | 0 | 102 | 69.6% |
| `union_h1_trail` | `partial_50` | -9.59% | $9,041.16 | -9.59% | 23 | — | 499 | 34 | 436 | 119 | 21.7% |
| `union_h1_trail` | `gap_miss` | -0.07% | $9,992.98 | -0.07% | 3 | — | 156 | 16 | 0 | 103 | 30.4% |
| `union_h1_sboost` | `ideal` | +4.70% | $10,469.61 | +0.00% | 0 | — | 168 | 15 | 0 | 102 | 34.8% |
| `union_h1_sboost` | `limit_prior` | +3.64% | $10,364.18 | +0.00% | 0 | — | 134 | 37 | 0 | 125 | 39.1% |
| `union_h1_sboost` | `limit_open` | +0.22% | $10,022.03 | -0.15% | 2 | — | 161 | 25 | 0 | 113 | 21.7% |
| `union_h1_sboost` | `market_mid` | -15.13% | $8,487.43 | -15.13% | 23 | — | 168 | 15 | 0 | 102 | 17.4% |
| `union_h1_sboost` | `market_adverse` | -42.00% | $5,799.87 | -42.00% | 23 | — | 166 | 15 | 0 | 103 | 0.0% |
| `union_h1_sboost` | `market_favorable` | +76.96% | $17,696.06 | +0.00% | 0 | — | 168 | 15 | 0 | 102 | 69.6% |
| `union_h1_sboost` | `partial_50` | -10.19% | $8,981.01 | -10.19% | 23 | — | 532 | 34 | 462 | 119 | 17.4% |
| `union_h1_sboost` | `gap_miss` | -0.02% | $9,998.03 | -0.02% | 3 | — | 166 | 16 | 0 | 103 | 30.4% |
| `union_h1_sizeup` | `ideal` | +4.63% | $10,463.52 | +0.00% | 0 | — | 158 | 15 | 0 | 102 | 34.8% |
| `union_h1_sizeup` | `limit_prior` | +3.37% | $10,336.68 | +0.00% | 0 | — | 126 | 36 | 0 | 124 | 43.5% |
| `union_h1_sizeup` | `limit_open` | +0.14% | $10,013.55 | -0.24% | 2 | — | 151 | 24 | 0 | 112 | 21.7% |
| `union_h1_sizeup` | `market_mid` | -14.70% | $8,529.88 | -14.70% | 23 | — | 158 | 15 | 0 | 102 | 17.4% |
| `union_h1_sizeup` | `market_adverse` | -41.32% | $5,868.27 | -41.32% | 23 | — | 156 | 15 | 0 | 103 | 0.0% |
| `union_h1_sizeup` | `market_favorable` | +75.73% | $17,573.22 | +0.00% | 0 | — | 158 | 15 | 0 | 102 | 69.6% |
| `union_h1_sizeup` | `partial_50` | -9.88% | $9,012.01 | -9.88% | 23 | — | 497 | 34 | 434 | 119 | 21.7% |
| `union_h1_sizeup` | `gap_miss` | -0.07% | $9,992.98 | -0.07% | 3 | — | 156 | 16 | 0 | 103 | 30.4% |
| `union_news_pack_h3` | `ideal` | -7.10% | $9,289.64 | -16.60% | 20 | — | 41 | 0 | 0 | 72 | 30.4% |
| `union_news_pack_h3` | `limit_prior` | +13.83% | $11,382.72 | -0.00% | 1 | — | 35 | 10 | 0 | 72 | 52.2% |
| `union_news_pack_h3` | `limit_open` | -3.04% | $9,696.52 | -13.08% | 20 | — | 35 | 2 | 0 | 72 | 30.4% |
| `union_news_pack_h3` | `market_mid` | -16.34% | $8,366.01 | -21.59% | 21 | — | 35 | 0 | 0 | 71 | 21.7% |
| `union_news_pack_h3` | `market_adverse` | -30.72% | $6,927.93 | -32.43% | 22 | — | 31 | 0 | 0 | 69 | 8.7% |
| `union_news_pack_h3` | `market_favorable` | +10.73% | $11,072.80 | -7.29% | 14 | — | 41 | 0 | 0 | 72 | 47.8% |
| `union_news_pack_h3` | `partial_50` | -8.09% | $9,191.33 | -10.14% | 21 | — | 108 | 5 | 83 | 76 | 26.1% |
| `union_news_pack_h3` | `gap_miss` | -7.10% | $9,289.64 | -16.60% | 20 | — | 41 | 0 | 0 | 72 | 30.4% |
| `union_news_head_h1` | `ideal` | -4.70% | $9,529.64 | -6.65% | 21 | — | 103 | 10 | 0 | 61 | 26.1% |
| `union_news_head_h1` | `limit_prior` | -10.95% | $8,904.89 | -10.95% | 22 | — | 91 | 26 | 0 | 77 | 17.4% |
| `union_news_head_h1` | `limit_open` | -10.95% | $8,905.03 | -10.95% | 22 | — | 99 | 16 | 0 | 67 | 17.4% |
| `union_news_head_h1` | `market_mid` | -24.42% | $7,558.42 | -24.42% | 22 | — | 103 | 10 | 0 | 61 | 8.7% |
| `union_news_head_h1` | `market_adverse` | -50.68% | $4,932.05 | -50.68% | 22 | — | 103 | 10 | 0 | 61 | 0.0% |
| `union_news_head_h1` | `market_favorable` | +66.35% | $16,634.70 | +0.00% | 0 | — | 103 | 10 | 0 | 61 | 69.6% |
| `union_news_head_h1` | `partial_50` | -15.96% | $8,404.41 | -15.96% | 22 | — | 326 | 20 | 285 | 66 | 8.7% |
| `union_news_head_h1` | `gap_miss` | -6.08% | $9,391.81 | -7.85% | 21 | — | 101 | 11 | 0 | 62 | 26.1% |
| `union_news_head_h3` | `ideal` | -14.13% | $8,586.84 | -14.13% | 20 | — | 93 | 0 | 0 | 145 | 17.4% |
| `union_news_head_h3` | `limit_prior` | -12.40% | $8,759.83 | -12.40% | 22 | — | 89 | 16 | 0 | 151 | 21.7% |
| `union_news_head_h3` | `limit_open` | -12.81% | $8,718.74 | -12.81% | 22 | — | 93 | 5 | 0 | 146 | 17.4% |
| `union_news_head_h3` | `market_mid` | -25.24% | $7,476.33 | -25.24% | 22 | — | 95 | 0 | 0 | 144 | 17.4% |
| `union_news_head_h3` | `market_adverse` | -41.51% | $5,848.59 | -41.51% | 22 | — | 85 | 0 | 0 | 141 | 13.0% |
| `union_news_head_h3` | `market_favorable` | +20.81% | $12,081.43 | +0.00% | 0 | — | 97 | 0 | 0 | 145 | 47.8% |
| `union_news_head_h3` | `partial_50` | -16.47% | $8,353.14 | -16.47% | 22 | — | 299 | 16 | 260 | 156 | 8.7% |
| `union_news_head_h3` | `gap_miss` | -15.10% | $8,490.37 | -15.10% | 21 | — | 97 | 1 | 0 | 147 | 17.4% |
| `union_news_g_cond_h1` | `ideal` | +2.98% | $10,297.46 | -1.77% | 3 | — | 131 | 10 | 0 | 76 | 34.8% |
| `union_news_g_cond_h1` | `limit_prior` | -3.65% | $9,635.20 | -4.53% | 21 | — | 111 | 31 | 0 | 97 | 30.4% |
| `union_news_g_cond_h1` | `limit_open` | -2.19% | $9,781.55 | -4.26% | 19 | — | 125 | 16 | 0 | 82 | 30.4% |
| `union_news_g_cond_h1` | `market_mid` | -17.40% | $8,260.10 | -17.40% | 21 | — | 131 | 10 | 0 | 76 | 13.0% |
| `union_news_g_cond_h1` | `market_adverse` | -45.11% | $5,489.05 | -45.11% | 22 | — | 131 | 10 | 0 | 76 | 0.0% |
| `union_news_g_cond_h1` | `market_favorable` | +71.90% | $17,189.84 | +0.00% | 0 | — | 135 | 10 | 0 | 74 | 69.6% |
| `union_news_g_cond_h1` | `partial_50` | -12.41% | $8,758.91 | -12.41% | 21 | — | 358 | 22 | 301 | 85 | 13.0% |
| `union_news_g_cond_h1` | `gap_miss` | +1.75% | $10,175.48 | -2.99% | 7 | — | 129 | 11 | 0 | 77 | 34.8% |
| `short_news_pack_h3` | `ideal` | +6.25% | $10,625.31 | -0.64% | 3 | — | 21 | 0 | 0 | 22 | 34.8% |
| `short_news_pack_h3` | `limit_prior` | +0.54% | $10,054.28 | -0.45% | 3 | — | 15 | 2 | 0 | 19 | 26.1% |
| `short_news_pack_h3` | `limit_open` | +4.25% | $10,425.20 | -1.86% | 5 | — | 19 | 3 | 0 | 24 | 26.1% |
| `short_news_pack_h3` | `market_mid` | +2.73% | $10,273.16 | -2.19% | 6 | — | 21 | 0 | 0 | 22 | 30.4% |
| `short_news_pack_h3` | `market_adverse` | -3.86% | $9,614.36 | -5.89% | 18 | — | 21 | 0 | 0 | 22 | 17.4% |
| `short_news_pack_h3` | `market_favorable` | +8.87% | $10,886.52 | +0.00% | 0 | — | 18 | 0 | 0 | 20 | 30.4% |
| `short_news_pack_h3` | `partial_50` | +1.60% | $10,159.71 | -1.23% | 6 | — | 58 | 2 | 49 | 24 | 39.1% |
| `short_news_pack_h3` | `gap_miss` | +6.25% | $10,625.31 | -0.64% | 3 | — | 21 | 0 | 0 | 22 | 34.8% |
| `union_news_pack_net3_h1` | `ideal` | +19.12% | $11,911.66 | +0.00% | 0 | — | 54 | 0 | 0 | 16 | 39.1% |
| `union_news_pack_net3_h1` | `limit_prior` | +8.18% | $10,818.25 | +0.00% | 0 | — | 40 | 11 | 0 | 27 | 43.5% |
| `union_news_pack_net3_h1` | `limit_open` | +13.35% | $11,335.22 | +0.00% | 0 | — | 52 | 2 | 0 | 18 | 39.1% |
| `union_news_pack_net3_h1` | `market_mid` | +0.27% | $10,026.71 | -4.93% | 6 | — | 54 | 0 | 0 | 16 | 30.4% |
| `union_news_pack_net3_h1` | `market_adverse` | -27.20% | $7,279.94 | -27.20% | 22 | — | 52 | 0 | 0 | 17 | 4.3% |
| `union_news_pack_net3_h1` | `market_favorable` | +55.59% | $15,558.64 | +0.00% | 0 | — | 54 | 0 | 0 | 16 | 60.9% |
| `union_news_pack_net3_h1` | `partial_50` | -4.11% | $9,589.39 | -6.64% | 20 | — | 115 | 2 | 89 | 18 | 30.4% |
| `union_news_pack_net3_h1` | `gap_miss` | +19.12% | $11,911.66 | +0.00% | 0 | — | 54 | 0 | 0 | 16 | 39.1% |
| `union_news_or_net2_h1` | `ideal` | -1.08% | $9,892.16 | -3.37% | 8 | — | 113 | 6 | 0 | 44 | 30.4% |
| `union_news_or_net2_h1` | `limit_prior` | -6.15% | $9,385.06 | -6.86% | 21 | — | 95 | 26 | 0 | 64 | 26.1% |
| `union_news_or_net2_h1` | `limit_open` | -4.84% | $9,516.16 | -6.42% | 19 | — | 109 | 11 | 0 | 49 | 26.1% |
| `union_news_or_net2_h1` | `market_mid` | -17.90% | $8,210.06 | -17.90% | 21 | — | 113 | 6 | 0 | 44 | 17.4% |
| `union_news_or_net2_h1` | `market_adverse` | -41.80% | $5,819.86 | -41.80% | 22 | — | 111 | 6 | 0 | 45 | 0.0% |
| `union_news_or_net2_h1` | `market_favorable` | +62.54% | $16,254.13 | +0.00% | 0 | — | 115 | 6 | 0 | 43 | 69.6% |
| `union_news_or_net2_h1` | `partial_50` | -12.25% | $8,774.86 | -12.25% | 21 | — | 293 | 16 | 244 | 54 | 21.7% |
| `union_news_or_net2_h1` | `gap_miss` | -2.44% | $9,755.75 | -4.73% | 15 | — | 111 | 7 | 0 | 45 | 26.1% |
| `short_news_or_h3` | `ideal` | +15.06% | $11,506.12 | +0.00% | 0 | — | 72 | 0 | 0 | 91 | 56.5% |
| `short_news_or_h3` | `limit_prior` | +11.09% | $11,108.60 | -0.51% | 3 | — | 57 | 11 | 0 | 86 | 52.2% |
| `short_news_or_h3` | `limit_open` | +10.79% | $11,078.87 | -0.52% | 4 | — | 66 | 6 | 0 | 92 | 56.5% |
| `short_news_or_h3` | `market_mid` | +5.74% | $10,574.14 | -1.69% | 5 | — | 72 | 0 | 0 | 91 | 47.8% |
| `short_news_or_h3` | `market_adverse` | -9.95% | $9,004.84 | -10.13% | 20 | — | 72 | 0 | 0 | 91 | 30.4% |
| `short_news_or_h3` | `market_favorable` | +46.64% | $14,664.19 | +0.00% | 0 | — | 72 | 0 | 0 | 91 | 82.6% |
| `short_news_or_h3` | `partial_50` | +2.96% | $10,296.12 | -0.63% | 5 | — | 207 | 8 | 179 | 94 | 47.8% |
| `short_news_or_h3` | `gap_miss` | +14.65% | $11,464.79 | +0.00% | 0 | — | 70 | 1 | 0 | 90 | 56.5% |
| `union_rsi_os_h1` | `ideal` | -10.85% | $8,914.86 | -11.13% | 21 | — | 51 | 6 | 0 | 17 | 26.1% |
| `union_rsi_os_h1` | `limit_prior` | +2.88% | $10,288.00 | -2.26% | 10 | — | 46 | 8 | 0 | 19 | 43.5% |
| `union_rsi_os_h1` | `limit_open` | -13.64% | $8,636.11 | -13.91% | 21 | — | 47 | 9 | 0 | 20 | 26.1% |
| `union_rsi_os_h1` | `market_mid` | -25.50% | $7,449.64 | -25.50% | 21 | — | 51 | 6 | 0 | 17 | 8.7% |
| `union_rsi_os_h1` | `market_adverse` | -47.10% | $5,289.86 | -47.10% | 22 | — | 51 | 6 | 0 | 17 | 4.3% |
| `union_rsi_os_h1` | `market_favorable` | +74.36% | $17,435.48 | +0.00% | 0 | — | 51 | 6 | 0 | 17 | 60.9% |
| `union_rsi_os_h1` | `partial_50` | -13.28% | $8,672.26 | -13.28% | 21 | — | 174 | 22 | 160 | 32 | 13.0% |
| `union_rsi_os_h1` | `gap_miss` | -4.93% | $9,506.77 | -6.73% | 21 | — | 47 | 8 | 0 | 19 | 34.8% |
| `union_rsi_os_h3` | `ideal` | -11.95% | $8,804.64 | -12.23% | 20 | — | 35 | 0 | 0 | 54 | 30.4% |
| `union_rsi_os_h3` | `limit_prior` | -9.17% | $9,083.31 | -9.17% | 18 | — | 37 | 11 | 0 | 62 | 43.5% |
| `union_rsi_os_h3` | `limit_open` | -13.13% | $8,686.82 | -13.50% | 21 | — | 39 | 2 | 0 | 56 | 34.8% |
| `union_rsi_os_h3` | `market_mid` | -23.90% | $7,609.57 | -23.90% | 21 | — | 35 | 0 | 0 | 54 | 21.7% |
| `union_rsi_os_h3` | `market_adverse` | -42.27% | $5,773.32 | -42.27% | 22 | — | 35 | 0 | 0 | 54 | 13.0% |
| `union_rsi_os_h3` | `market_favorable` | +10.94% | $11,094.33 | -0.30% | 2 | — | 47 | 0 | 0 | 60 | 56.5% |
| `union_rsi_os_h3` | `partial_50` | -14.71% | $8,528.78 | -14.71% | 21 | — | 151 | 16 | 137 | 74 | 17.4% |
| `union_rsi_os_h3` | `gap_miss` | -10.28% | $8,972.50 | -10.56% | 20 | — | 33 | 2 | 0 | 53 | 34.8% |
| `union_macd_up_h1` | `ideal` | +1.33% | $10,132.66 | -3.27% | 5 | — | 154 | 15 | 0 | 102 | 26.1% |
| `union_macd_up_h1` | `limit_prior` | +3.71% | $10,371.36 | -0.93% | 3 | — | 129 | 36 | 0 | 124 | 39.1% |
| `union_macd_up_h1` | `limit_open` | -2.08% | $9,792.30 | -3.42% | 12 | — | 151 | 22 | 0 | 110 | 26.1% |
| `union_macd_up_h1` | `market_mid` | -17.37% | $8,262.76 | -17.37% | 23 | — | 154 | 15 | 0 | 102 | 17.4% |
| `union_macd_up_h1` | `market_adverse` | -43.00% | $5,700.23 | -43.00% | 23 | — | 152 | 15 | 0 | 103 | 0.0% |
| `union_macd_up_h1` | `market_favorable` | +68.93% | $16,892.73 | +0.00% | 0 | — | 154 | 15 | 0 | 102 | 69.6% |
| `union_macd_up_h1` | `partial_50` | -12.57% | $8,742.64 | -12.57% | 23 | — | 477 | 32 | 415 | 118 | 17.4% |
| `union_macd_up_h1` | `gap_miss` | -3.39% | $9,661.21 | -3.39% | 13 | — | 150 | 17 | 0 | 104 | 21.7% |
| `union_macd_up_h3` | `ideal` | -1.16% | $9,884.52 | -2.37% | 11 | — | 114 | 0 | 0 | 221 | 43.5% |
| `union_macd_up_h3` | `limit_prior` | -4.32% | $9,568.22 | -4.55% | 12 | — | 111 | 26 | 0 | 238 | 39.1% |
| `union_macd_up_h3` | `limit_open` | -8.77% | $9,122.99 | -8.77% | 17 | — | 111 | 7 | 0 | 227 | 30.4% |
| `union_macd_up_h3` | `market_mid` | -10.70% | $8,929.92 | -10.70% | 21 | — | 96 | 0 | 0 | 212 | 34.8% |
| `union_macd_up_h3` | `market_adverse` | -27.38% | $7,262.07 | -27.38% | 23 | — | 88 | 0 | 0 | 210 | 26.1% |
| `union_macd_up_h3` | `market_favorable` | +30.33% | $13,032.64 | +0.00% | 0 | — | 124 | 0 | 0 | 226 | 69.6% |
| `union_macd_up_h3` | `partial_50` | -13.62% | $8,638.51 | -13.62% | 23 | — | 422 | 19 | 358 | 251 | 21.7% |
| `union_macd_up_h3` | `gap_miss` | -0.77% | $9,923.47 | -2.37% | 10 | — | 110 | 2 | 0 | 219 | 43.5% |
| `union_macd_xup_h1` | `ideal` | -12.05% | $8,794.58 | -14.02% | 22 | — | 79 | 6 | 0 | 55 | 30.4% |
| `union_macd_xup_h1` | `limit_prior` | -17.19% | $8,281.40 | -17.19% | 22 | — | 66 | 21 | 0 | 72 | 21.7% |
| `union_macd_xup_h1` | `limit_open` | -15.54% | $8,446.16 | -16.42% | 22 | — | 77 | 10 | 0 | 60 | 21.7% |
| `union_macd_xup_h1` | `market_mid` | -30.33% | $6,966.56 | -30.33% | 22 | — | 79 | 6 | 0 | 55 | 4.3% |
| `union_macd_xup_h1` | `market_adverse` | -54.93% | $4,507.33 | -54.93% | 22 | — | 79 | 6 | 0 | 55 | 0.0% |
| `union_macd_xup_h1` | `market_favorable` | +71.04% | $17,103.60 | +0.00% | 0 | — | 79 | 6 | 0 | 55 | 65.2% |
| `union_macd_xup_h1` | `partial_50` | -19.23% | $8,077.02 | -19.23% | 22 | — | 252 | 20 | 220 | 69 | 13.0% |
| `union_macd_xup_h1` | `gap_miss` | -12.05% | $8,794.58 | -14.02% | 22 | — | 79 | 6 | 0 | 55 | 30.4% |
| `union_flow_in_h1` | `ideal` | +5.56% | $10,556.18 | +0.00% | 0 | — | 63 | 3 | 0 | 24 | 34.8% |
| `union_flow_in_h1` | `limit_prior` | +16.63% | $11,663.13 | +0.00% | 0 | — | 55 | 13 | 0 | 34 | 39.1% |
| `union_flow_in_h1` | `limit_open` | -3.15% | $9,684.82 | -3.91% | 10 | — | 55 | 12 | 0 | 33 | 30.4% |
| `union_flow_in_h1` | `market_mid` | -14.96% | $8,503.63 | -15.28% | 16 | — | 63 | 3 | 0 | 24 | 26.1% |
| `union_flow_in_h1` | `market_adverse` | -42.63% | $5,736.59 | -42.63% | 23 | — | 63 | 3 | 0 | 24 | 0.0% |
| `union_flow_in_h1` | `market_favorable` | +93.35% | $19,334.81 | +0.00% | 0 | — | 63 | 3 | 0 | 24 | 65.2% |
| `union_flow_in_h1` | `partial_50` | -9.67% | $9,032.69 | -9.80% | 20 | — | 208 | 17 | 184 | 38 | 30.4% |
| `union_flow_in_h1` | `gap_miss` | +12.22% | $11,222.36 | +0.00% | 0 | — | 61 | 4 | 0 | 25 | 39.1% |
| `union_flow_in_h3` | `ideal` | -4.18% | $9,581.73 | -6.10% | 13 | — | 47 | 1 | 0 | 78 | 26.1% |
| `union_flow_in_h3` | `limit_prior` | +5.85% | $10,584.62 | +0.00% | 0 | — | 49 | 12 | 0 | 86 | 34.8% |
| `union_flow_in_h3` | `limit_open` | -3.71% | $9,629.52 | -5.66% | 13 | — | 41 | 6 | 0 | 76 | 21.7% |
| `union_flow_in_h3` | `market_mid` | -11.88% | $8,811.65 | -12.24% | 21 | — | 43 | 1 | 0 | 76 | 21.7% |
| `union_flow_in_h3` | `market_adverse` | -23.23% | $7,676.95 | -23.23% | 23 | — | 49 | 1 | 0 | 79 | 13.0% |
| `union_flow_in_h3` | `market_favorable` | +33.49% | $13,348.73 | +0.00% | 0 | — | 59 | 1 | 0 | 84 | 65.2% |
| `union_flow_in_h3` | `partial_50` | -9.37% | $9,062.62 | -9.49% | 21 | — | 178 | 23 | 158 | 108 | 34.8% |
| `union_flow_in_h3` | `gap_miss` | -4.18% | $9,581.73 | -6.10% | 13 | — | 47 | 1 | 0 | 78 | 26.1% |
| `union_rsi_os_macd_h1` | `ideal` | -5.91% | $9,408.58 | -7.71% | 8 | — | 4 | 0 | 0 | 0 | 4.3% |
| `union_rsi_os_macd_h1` | `limit_prior` | -1.07% | $9,893.44 | -3.91% | 8 | — | 2 | 1 | 0 | 1 | 4.3% |
| `union_rsi_os_macd_h1` | `limit_open` | -5.91% | $9,408.58 | -7.71% | 8 | — | 4 | 0 | 0 | 0 | 4.3% |
| `union_rsi_os_macd_h1` | `market_mid` | -8.88% | $9,111.99 | -9.27% | 8 | — | 4 | 0 | 0 | 0 | 4.3% |
| `union_rsi_os_macd_h1` | `market_adverse` | -14.31% | $8,569.32 | -14.31% | 8 | — | 4 | 0 | 0 | 0 | 4.3% |
| `union_rsi_os_macd_h1` | `market_favorable` | +5.97% | $10,596.73 | +0.00% | 0 | — | 4 | 0 | 0 | 0 | 13.0% |
| `union_rsi_os_macd_h1` | `partial_50` | -4.24% | $9,575.91 | -4.75% | 8 | — | 11 | 4 | 11 | 4 | 8.7% |
| `union_rsi_os_macd_h1` | `gap_miss` | -1.41% | $9,859.29 | -3.91% | 8 | — | 2 | 1 | 0 | 1 | 4.3% |
| `union_flow_in_white_h1` | `ideal` | +0.64% | $10,063.49 | -0.96% | 2 | — | 20 | 0 | 0 | 0 | 17.4% |
| `union_flow_in_white_h1` | `limit_prior` | +1.03% | $10,103.12 | -0.90% | 12 | — | 20 | 4 | 0 | 4 | 21.7% |
| `union_flow_in_white_h1` | `limit_open` | -2.91% | $9,708.71 | -2.96% | 20 | — | 16 | 5 | 0 | 5 | 13.0% |
| `union_flow_in_white_h1` | `market_mid` | -7.48% | $9,251.68 | -7.82% | 22 | — | 20 | 0 | 0 | 0 | 8.7% |
| `union_flow_in_white_h1` | `market_adverse` | -20.67% | $7,932.90 | -20.67% | 22 | — | 20 | 0 | 0 | 0 | 0.0% |
| `union_flow_in_white_h1` | `market_favorable` | +21.31% | $12,130.73 | +0.00% | 0 | — | 20 | 0 | 0 | 0 | 34.8% |
| `union_flow_in_white_h1` | `partial_50` | -5.80% | $9,419.87 | -5.80% | 22 | — | 83 | 8 | 77 | 8 | 4.3% |
| `union_flow_in_white_h1` | `gap_miss` | +0.64% | $10,063.49 | -0.96% | 2 | — | 20 | 0 | 0 | 0 | 17.4% |
| `union_rsi_h1` | `ideal` | -11.77% | $8,823.42 | -11.77% | 21 | — | 174 | 14 | 0 | 100 | 17.4% |
| `union_rsi_h1` | `limit_prior` | -5.19% | $9,480.89 | -7.29% | 21 | — | 149 | 34 | 0 | 119 | 43.5% |
| `union_rsi_h1` | `limit_open` | -12.97% | $8,702.92 | -12.97% | 21 | — | 164 | 20 | 0 | 107 | 21.7% |
| `union_rsi_h1` | `market_mid` | -32.59% | $6,741.05 | -32.59% | 22 | — | 174 | 14 | 0 | 100 | 13.0% |
| `union_rsi_h1` | `market_adverse` | -59.21% | $4,079.01 | -59.21% | 23 | — | 172 | 14 | 0 | 101 | 4.3% |
| `union_rsi_h1` | `market_favorable` | +125.57% | $22,556.70 | +0.00% | 0 | — | 176 | 14 | 0 | 99 | 73.9% |
| `union_rsi_h1` | `partial_50` | -18.68% | $8,131.79 | -18.68% | 22 | — | 546 | 38 | 481 | 123 | 8.7% |
| `union_rsi_h1` | `gap_miss` | -9.32% | $9,068.06 | -9.32% | 21 | — | 170 | 16 | 0 | 102 | 21.7% |
| `union_macd_hist_h1` | `ideal` | -2.78% | $9,722.40 | -5.04% | 19 | — | 165 | 12 | 0 | 91 | 34.8% |
| `union_macd_hist_h1` | `limit_prior` | -8.71% | $9,128.69 | -9.66% | 21 | — | 146 | 40 | 0 | 119 | 34.8% |
| `union_macd_hist_h1` | `limit_open` | -5.70% | $9,430.43 | -6.61% | 20 | — | 163 | 17 | 0 | 96 | 34.8% |
| `union_macd_hist_h1` | `market_mid` | -22.45% | $7,754.83 | -22.45% | 22 | — | 165 | 12 | 0 | 91 | 26.1% |
| `union_macd_hist_h1` | `market_adverse` | -48.16% | $5,184.25 | -48.16% | 23 | — | 163 | 12 | 0 | 92 | 8.7% |
| `union_macd_hist_h1` | `market_favorable` | +63.57% | $16,356.96 | +0.00% | 0 | — | 165 | 12 | 0 | 91 | 78.3% |
| `union_macd_hist_h1` | `partial_50` | -14.07% | $8,593.02 | -14.07% | 23 | — | 344 | 12 | 266 | 89 | 17.4% |
| `union_macd_hist_h1` | `gap_miss` | -3.41% | $9,658.90 | -5.67% | 19 | — | 163 | 13 | 0 | 92 | 34.8% |
| `short_rsi_ob_h1` | `ideal` | -8.19% | $9,181.29 | -9.45% | 19 | — | 148 | 13 | 0 | 94 | 34.8% |
| `short_rsi_ob_h1` | `limit_prior` | -5.54% | $9,446.35 | -7.36% | 19 | — | 126 | 30 | 0 | 110 | 43.5% |
| `short_rsi_ob_h1` | `limit_open` | -10.66% | $8,933.72 | -10.99% | 21 | — | 144 | 18 | 0 | 98 | 34.8% |
| `short_rsi_ob_h1` | `market_mid` | -19.90% | $8,009.65 | -19.90% | 23 | — | 148 | 13 | 0 | 94 | 13.0% |
| `short_rsi_ob_h1` | `market_adverse` | -38.10% | $6,189.49 | -38.10% | 23 | — | 148 | 13 | 0 | 94 | 0.0% |
| `short_rsi_ob_h1` | `market_favorable` | +33.83% | $13,382.89 | +0.00% | 0 | — | 148 | 13 | 0 | 94 | 60.9% |
| `short_rsi_ob_h1` | `partial_50` | -11.47% | $8,853.18 | -11.47% | 23 | — | 408 | 31 | 355 | 95 | 30.4% |
| `short_rsi_ob_h1` | `gap_miss` | -7.52% | $9,248.12 | -8.91% | 20 | — | 144 | 15 | 0 | 96 | 34.8% |
| `short_rsi_ob_h3` | `ideal` | -3.70% | $9,630.09 | -11.06% | 18 | — | 144 | 0 | 0 | 220 | 39.1% |
| `short_rsi_ob_h3` | `limit_prior` | -4.66% | $9,533.90 | -10.98% | 18 | — | 124 | 16 | 0 | 216 | 39.1% |
| `short_rsi_ob_h3` | `limit_open` | -6.33% | $9,366.74 | -11.99% | 18 | — | 140 | 5 | 0 | 221 | 39.1% |
| `short_rsi_ob_h3` | `market_mid` | -15.88% | $8,411.99 | -18.62% | 22 | — | 144 | 0 | 0 | 220 | 30.4% |
| `short_rsi_ob_h3` | `market_adverse` | -35.09% | $6,491.37 | -35.63% | 23 | — | 144 | 0 | 0 | 220 | 13.0% |
| `short_rsi_ob_h3` | `market_favorable` | +34.84% | $13,484.06 | +0.00% | 0 | — | 144 | 0 | 0 | 220 | 65.2% |
| `short_rsi_ob_h3` | `partial_50` | -9.98% | $9,001.94 | -11.55% | 22 | — | 384 | 20 | 333 | 212 | 34.8% |
| `short_rsi_ob_h3` | `gap_miss` | -3.88% | $9,611.81 | -11.22% | 18 | — | 142 | 1 | 0 | 219 | 39.1% |
| `short_macd_dn_h3` | `ideal` | -8.74% | $9,126.02 | -11.48% | 23 | — | 170 | 0 | 0 | 258 | 39.1% |
| `short_macd_dn_h3` | `limit_prior` | -10.14% | $8,986.18 | -12.68% | 23 | — | 143 | 20 | 0 | 252 | 34.8% |
| `short_macd_dn_h3` | `limit_open` | -10.69% | $8,930.82 | -13.32% | 23 | — | 161 | 9 | 0 | 257 | 39.1% |
| `short_macd_dn_h3` | `market_mid` | -19.30% | $8,070.12 | -20.76% | 23 | — | 170 | 0 | 0 | 258 | 26.1% |
| `short_macd_dn_h3` | `market_adverse` | -36.02% | $6,398.32 | -36.02% | 23 | — | 170 | 0 | 0 | 258 | 8.7% |
| `short_macd_dn_h3` | `market_favorable` | +27.28% | $12,727.86 | -1.27% | 1 | — | 172 | 0 | 0 | 259 | 78.3% |
| `short_macd_dn_h3` | `partial_50` | -11.49% | $8,850.78 | -12.84% | 23 | — | 472 | 30 | 407 | 284 | 26.1% |
| `short_macd_dn_h3` | `gap_miss` | -8.74% | $9,126.02 | -11.48% | 23 | — | 170 | 0 | 0 | 258 | 39.1% |
| `combo_se_5050_shared` | `ideal` | +27.06% | $12,706.44 | +0.00% | 0 | — | 160 | 0 | 0 | 250 | 52.2% |
| `combo_se_5050_shared` | `limit_prior` | +13.25% | $11,325.14 | +0.00% | 0 | — | 156 | 28 | 0 | 254 | 56.5% |
| `combo_se_5050_shared` | `limit_open` | +22.89% | $12,288.96 | +0.00% | 0 | — | 153 | 9 | 0 | 242 | 52.2% |
| `combo_se_5050_shared` | `market_mid` | +3.25% | $10,324.57 | +0.00% | 0 | — | 164 | 0 | 0 | 252 | 43.5% |
| `combo_se_5050_shared` | `market_adverse` | -30.60% | $6,940.12 | -30.60% | 17 | — | 152 | 0 | 0 | 236 | 21.7% |
| `combo_se_5050_shared` | `market_favorable` | +97.10% | $19,710.29 | +0.00% | 0 | — | 168 | 0 | 0 | 254 | 78.3% |
| `combo_se_5050_shared` | `partial_50` | +3.50% | $10,350.38 | +0.00% | 0 | — | 532 | 34 | 455 | 296 | 52.2% |
| `combo_se_5050_shared` | `gap_miss` | +24.08% | $12,407.90 | +0.00% | 0 | — | 158 | 3 | 0 | 249 | 56.5% |
| `combo_se_5050_weather` | `ideal` | +27.06% | $12,706.44 | +0.00% | 0 | — | 160 | 0 | 0 | 250 | 52.2% |
| `combo_se_5050_weather` | `limit_prior` | +13.25% | $11,325.14 | +0.00% | 0 | — | 156 | 28 | 0 | 254 | 56.5% |
| `combo_se_5050_weather` | `limit_open` | +22.89% | $12,288.96 | +0.00% | 0 | — | 153 | 9 | 0 | 242 | 52.2% |
| `combo_se_5050_weather` | `market_mid` | +3.25% | $10,324.57 | +0.00% | 0 | — | 164 | 0 | 0 | 252 | 43.5% |
| `combo_se_5050_weather` | `market_adverse` | -30.60% | $6,940.12 | -30.60% | 17 | — | 152 | 0 | 0 | 236 | 21.7% |
| `combo_se_5050_weather` | `market_favorable` | +97.10% | $19,710.29 | +0.00% | 0 | — | 168 | 0 | 0 | 254 | 78.3% |
| `combo_se_5050_weather` | `partial_50` | +3.50% | $10,350.38 | +0.00% | 0 | — | 532 | 34 | 455 | 296 | 52.2% |
| `combo_se_5050_weather` | `gap_miss` | +24.08% | $12,407.90 | +0.00% | 0 | — | 158 | 3 | 0 | 249 | 56.5% |
| `combo_nse_333_shared` | `ideal` | +24.72% | $12,471.96 | +0.00% | 0 | — | 269 | 6 | 0 | 338 | 47.8% |
| `combo_nse_333_shared` | `limit_prior` | +3.27% | $10,327.37 | +0.00% | 0 | — | 245 | 56 | 0 | 346 | 52.2% |
| `combo_nse_333_shared` | `limit_open` | +20.47% | $12,046.77 | +0.00% | 0 | — | 260 | 19 | 0 | 344 | 47.8% |
| `combo_nse_333_shared` | `market_mid` | -0.69% | $9,930.62 | -0.69% | 3 | — | 267 | 6 | 0 | 337 | 30.4% |
| `combo_nse_333_shared` | `market_adverse` | -35.08% | $6,491.60 | -35.08% | 19 | — | 253 | 6 | 0 | 334 | 17.4% |
| `combo_nse_333_shared` | `market_favorable` | +105.19% | $20,518.81 | +0.00% | 0 | — | 277 | 6 | 0 | 340 | 82.6% |
| `combo_nse_333_shared` | `partial_50` | -2.61% | $9,738.61 | -2.61% | 8 | — | 765 | 38 | 636 | 360 | 39.1% |
| `combo_nse_333_shared` | `gap_miss` | +23.38% | $12,338.16 | +0.00% | 0 | — | 257 | 12 | 0 | 335 | 47.8% |
| `combo_en_7030_shared` | `ideal` | +20.71% | $12,071.20 | +0.00% | 0 | — | 209 | 6 | 0 | 264 | 39.1% |
| `combo_en_7030_shared` | `limit_prior` | -1.81% | $9,818.77 | -1.88% | 11 | — | 192 | 48 | 0 | 273 | 30.4% |
| `combo_en_7030_shared` | `limit_open` | +18.14% | $11,813.50 | +0.00% | 0 | — | 206 | 15 | 0 | 268 | 30.4% |
| `combo_en_7030_shared` | `market_mid` | +0.68% | $10,067.48 | +0.00% | 0 | — | 203 | 6 | 0 | 265 | 30.4% |
| `combo_en_7030_shared` | `market_adverse` | -27.89% | $7,210.64 | -27.89% | 17 | — | 193 | 6 | 0 | 267 | 17.4% |
| `combo_en_7030_shared` | `market_favorable` | +83.02% | $18,301.80 | +0.00% | 0 | — | 213 | 6 | 0 | 262 | 82.6% |
| `combo_en_7030_shared` | `partial_50` | -5.61% | $9,438.93 | -5.61% | 10 | — | 619 | 36 | 518 | 284 | 30.4% |
| `combo_en_7030_shared` | `gap_miss` | +17.14% | $11,713.80 | +0.00% | 0 | — | 199 | 12 | 0 | 260 | 39.1% |
| `combo_se_7030_shared` | `ideal` | +24.23% | $12,423.42 | +0.00% | 0 | — | 158 | 0 | 0 | 239 | 52.2% |
| `combo_se_7030_shared` | `limit_prior` | +12.70% | $11,269.57 | +0.00% | 0 | — | 154 | 28 | 0 | 253 | 65.2% |
| `combo_se_7030_shared` | `limit_open` | +20.84% | $12,083.66 | +0.00% | 0 | — | 151 | 9 | 0 | 241 | 52.2% |
| `combo_se_7030_shared` | `market_mid` | +3.66% | $10,365.63 | +0.00% | 0 | — | 158 | 0 | 0 | 239 | 39.1% |
| `combo_se_7030_shared` | `market_adverse` | -26.95% | $7,304.86 | -26.95% | 17 | — | 146 | 0 | 0 | 233 | 26.1% |
| `combo_se_7030_shared` | `market_favorable` | +81.09% | $18,109.47 | +0.00% | 0 | — | 162 | 0 | 0 | 241 | 73.9% |
| `combo_se_7030_shared` | `partial_50` | +5.56% | $10,556.39 | +0.00% | 0 | — | 498 | 30 | 420 | 290 | 47.8% |
| `combo_se_7030_shared` | `gap_miss` | +22.49% | $12,248.74 | +0.00% | 0 | — | 152 | 3 | 0 | 236 | 52.2% |
| `combo_en_5050_shared` | `ideal` | +19.99% | $11,999.21 | +0.00% | 0 | — | 211 | 6 | 0 | 261 | 39.1% |
| `combo_en_5050_shared` | `limit_prior` | -3.38% | $9,661.66 | -3.38% | 12 | — | 194 | 50 | 0 | 273 | 34.8% |
| `combo_en_5050_shared` | `limit_open` | +16.86% | $11,685.97 | +0.00% | 0 | — | 206 | 15 | 0 | 266 | 30.4% |
| `combo_en_5050_shared` | `market_mid` | -0.93% | $9,906.61 | -0.93% | 3 | — | 209 | 6 | 0 | 260 | 21.7% |
| `combo_en_5050_shared` | `market_adverse` | -31.31% | $6,868.67 | -31.31% | 19 | — | 207 | 6 | 0 | 261 | 8.7% |
| `combo_en_5050_shared` | `market_favorable` | +84.51% | $18,451.16 | +0.00% | 0 | — | 215 | 6 | 0 | 261 | 78.3% |
| `combo_en_5050_shared` | `partial_50` | -6.21% | $9,379.14 | -6.21% | 12 | — | 612 | 34 | 509 | 282 | 26.1% |
| `combo_en_5050_shared` | `gap_miss` | +16.94% | $11,693.86 | +0.00% | 0 | — | 199 | 12 | 0 | 258 | 43.5% |
| `combo_ner_5050_shared` | `ideal` | +18.65% | $11,865.34 | +0.00% | 0 | — | 216 | 4 | 0 | 265 | 39.1% |
| `combo_ner_5050_shared` | `limit_prior` | -5.33% | $9,466.95 | -5.33% | 18 | — | 195 | 47 | 0 | 270 | 30.4% |
| `combo_ner_5050_shared` | `limit_open` | +14.95% | $11,494.79 | +0.00% | 0 | — | 211 | 15 | 0 | 272 | 30.4% |
| `combo_ner_5050_shared` | `market_mid` | -1.57% | $9,842.90 | -1.57% | 3 | — | 216 | 4 | 0 | 265 | 21.7% |
| `combo_ner_5050_shared` | `market_adverse` | -31.05% | $6,894.65 | -31.05% | 19 | — | 210 | 4 | 0 | 264 | 13.0% |
| `combo_ner_5050_shared` | `market_favorable` | +81.68% | $18,168.11 | +0.00% | 0 | — | 218 | 4 | 0 | 264 | 82.6% |
| `combo_ner_5050_shared` | `partial_50` | -6.62% | $9,337.47 | -6.62% | 12 | — | 613 | 30 | 504 | 278 | 21.7% |
| `combo_ner_5050_shared` | `gap_miss` | +16.80% | $11,680.23 | +0.00% | 0 | — | 202 | 11 | 0 | 261 | 39.1% |
| `combo_se_5050_split` | `ideal` | +17.80% | $11,779.74 | +0.00% | 0 | — | 168 | 0 | 0 | 259 | 56.5% |
| `combo_se_5050_split` | `limit_prior` | +5.84% | $10,584.39 | -0.16% | 1 | — | 156 | 30 | 0 | 260 | 60.9% |
| `combo_se_5050_split` | `limit_open` | +15.07% | $11,507.36 | +0.00% | 0 | — | 159 | 10 | 0 | 260 | 52.2% |
| `combo_se_5050_split` | `market_mid` | +3.57% | $10,356.76 | +0.00% | 0 | — | 160 | 0 | 0 | 255 | 52.2% |
| `combo_se_5050_split` | `market_adverse` | -18.28% | $8,172.37 | -18.28% | 19 | — | 156 | 0 | 0 | 253 | 34.8% |
| `combo_se_5050_split` | `market_favorable` | +62.92% | $16,291.71 | +0.00% | 0 | — | 182 | 0 | 0 | 266 | 87.0% |
| `combo_se_5050_split` | `partial_50` | +0.17% | $10,016.85 | +0.00% | 0 | — | 501 | 28 | 413 | 301 | 39.1% |
| `combo_se_5050_split` | `gap_miss` | +16.60% | $11,660.41 | +0.00% | 0 | — | 166 | 5 | 0 | 258 | 52.2% |
| `combo_en_3070_shared` | `ideal` | +18.53% | $11,852.83 | +0.00% | 0 | — | 209 | 6 | 0 | 258 | 34.8% |
| `combo_en_3070_shared` | `limit_prior` | -4.79% | $9,521.31 | -4.79% | 12 | — | 192 | 50 | 0 | 270 | 39.1% |
| `combo_en_3070_shared` | `limit_open` | +14.57% | $11,457.14 | +0.00% | 0 | — | 204 | 15 | 0 | 263 | 30.4% |
| `combo_en_3070_shared` | `market_mid` | -3.49% | $9,650.98 | -3.49% | 5 | — | 207 | 6 | 0 | 259 | 21.7% |
| `combo_en_3070_shared` | `market_adverse` | -34.54% | $6,546.32 | -34.54% | 19 | — | 201 | 6 | 0 | 256 | 8.7% |
| `combo_en_3070_shared` | `market_favorable` | +88.23% | $18,822.90 | +0.00% | 0 | — | 217 | 6 | 0 | 260 | 78.3% |
| `combo_en_3070_shared` | `partial_50` | -6.52% | $9,347.72 | -6.52% | 12 | — | 587 | 30 | 483 | 273 | 30.4% |
| `combo_en_3070_shared` | `gap_miss` | +16.28% | $11,628.38 | +0.00% | 0 | — | 197 | 12 | 0 | 255 | 43.5% |
| `combo_hn_5050_shared` | `ideal` | +13.47% | $11,347.38 | +0.00% | 0 | — | 205 | 14 | 0 | 118 | 39.1% |
| `combo_hn_5050_shared` | `limit_prior` | +9.15% | $10,915.23 | +0.00% | 0 | — | 168 | 51 | 0 | 156 | 43.5% |
| `combo_hn_5050_shared` | `limit_open` | +4.51% | $10,451.08 | -1.49% | 1 | — | 191 | 27 | 0 | 132 | 39.1% |
| `combo_hn_5050_shared` | `market_mid` | -19.71% | $8,029.38 | -19.71% | 22 | — | 201 | 14 | 0 | 120 | 26.1% |
| `combo_hn_5050_shared` | `market_adverse` | -58.18% | $4,182.31 | -58.18% | 23 | — | 201 | 14 | 0 | 120 | 4.3% |
| `combo_hn_5050_shared` | `market_favorable` | +148.09% | $24,808.94 | +0.00% | 0 | — | 209 | 14 | 0 | 116 | 78.3% |
| `combo_hn_5050_shared` | `partial_50` | -14.52% | $8,547.62 | -14.52% | 21 | — | 530 | 28 | 453 | 112 | 17.4% |
| `combo_hn_5050_shared` | `gap_miss` | +13.63% | $11,363.15 | +0.00% | 0 | — | 201 | 16 | 0 | 120 | 43.5% |
| `combo_seh_502525_split` | `ideal` | +15.57% | $11,557.09 | +0.00% | 0 | — | 250 | 4 | 0 | 300 | 65.2% |
| `combo_seh_502525_split` | `limit_prior` | +10.11% | $11,010.70 | +0.00% | 0 | — | 223 | 42 | 0 | 310 | 56.5% |
| `combo_seh_502525_split` | `limit_open` | +10.66% | $11,065.58 | +0.00% | 0 | — | 233 | 21 | 0 | 308 | 56.5% |
| `combo_seh_502525_split` | `market_mid` | -3.96% | $9,603.94 | -3.96% | 15 | — | 246 | 4 | 0 | 298 | 39.1% |
| `combo_seh_502525_split` | `market_adverse` | -28.98% | $7,101.45 | -28.98% | 23 | — | 245 | 4 | 0 | 297 | 8.7% |
| `combo_seh_502525_split` | `market_favorable` | +91.75% | $19,175.39 | +0.00% | 0 | — | 260 | 4 | 0 | 305 | 87.0% |
| `combo_seh_502525_split` | `partial_50` | -2.53% | $9,747.06 | -2.53% | 12 | — | 662 | 32 | 540 | 328 | 39.1% |
| `combo_seh_502525_split` | `gap_miss` | +15.46% | $11,545.92 | +0.00% | 0 | — | 244 | 10 | 0 | 299 | 69.6% |
| `combo_hj_5050_shared` | `ideal` | +10.95% | $11,094.52 | -2.53% | 4 | — | 189 | 14 | 0 | 91 | 34.8% |
| `combo_hj_5050_shared` | `limit_prior` | +12.51% | $11,250.62 | +0.00% | 0 | — | 167 | 41 | 0 | 119 | 39.1% |
| `combo_hj_5050_shared` | `limit_open` | +4.24% | $10,424.36 | -2.74% | 4 | — | 177 | 24 | 0 | 101 | 34.8% |
| `combo_hj_5050_shared` | `market_mid` | -22.95% | $7,705.26 | -22.95% | 22 | — | 189 | 14 | 0 | 91 | 30.4% |
| `combo_hj_5050_shared` | `market_adverse` | -60.96% | $3,904.30 | -60.96% | 23 | — | 185 | 14 | 0 | 93 | 4.3% |
| `combo_hj_5050_shared` | `market_favorable` | +167.71% | $26,770.92 | +0.00% | 0 | — | 189 | 14 | 0 | 91 | 73.9% |
| `combo_hj_5050_shared` | `partial_50` | -14.12% | $8,587.59 | -14.12% | 22 | — | 580 | 38 | 511 | 100 | 26.1% |
| `combo_hj_5050_shared` | `gap_miss` | +12.76% | $11,275.55 | -2.18% | 3 | — | 185 | 16 | 0 | 93 | 34.8% |
| `combo_ef_7030_shared` | `ideal` | +14.68% | $11,467.56 | +0.00% | 0 | — | 173 | 12 | 0 | 419 | 56.5% |
| `combo_ef_7030_shared` | `limit_prior` | +4.79% | $10,478.64 | +0.00% | 0 | — | 174 | 44 | 0 | 435 | 52.2% |
| `combo_ef_7030_shared` | `limit_open` | +13.57% | $11,356.95 | +0.00% | 0 | — | 191 | 21 | 0 | 440 | 56.5% |
| `combo_ef_7030_shared` | `market_mid` | +0.23% | $10,023.25 | +0.00% | 0 | — | 172 | 2 | 0 | 400 | 39.1% |
| `combo_ef_7030_shared` | `market_adverse` | -23.25% | $7,674.84 | -23.25% | 18 | — | 176 | 10 | 0 | 418 | 34.8% |
| `combo_ef_7030_shared` | `market_favorable` | +56.66% | $15,665.83 | +0.00% | 0 | — | 199 | 12 | 0 | 440 | 73.9% |
| `combo_ef_7030_shared` | `partial_50` | -3.87% | $9,612.97 | -3.87% | 5 | — | 601 | 43 | 498 | 510 | 34.8% |
| `combo_ef_7030_shared` | `gap_miss` | +12.12% | $11,212.01 | +0.00% | 0 | — | 162 | 14 | 0 | 407 | 52.2% |
| `combo_hn_3070_shared` | `ideal` | +11.42% | $11,142.12 | +0.00% | 0 | — | 207 | 14 | 0 | 117 | 47.8% |
| `combo_hn_3070_shared` | `limit_prior` | +3.69% | $10,369.22 | +0.00% | 0 | — | 170 | 51 | 0 | 155 | 47.8% |
| `combo_hn_3070_shared` | `limit_open` | +3.17% | $10,316.79 | +0.00% | 0 | — | 195 | 27 | 0 | 130 | 34.8% |
| `combo_hn_3070_shared` | `market_mid` | -19.04% | $8,095.87 | -19.04% | 22 | — | 207 | 14 | 0 | 117 | 26.1% |
| `combo_hn_3070_shared` | `market_adverse` | -54.69% | $4,530.76 | -54.69% | 23 | — | 201 | 14 | 0 | 120 | 4.3% |
| `combo_hn_3070_shared` | `market_favorable` | +124.45% | $22,445.29 | +0.00% | 0 | — | 209 | 14 | 0 | 116 | 78.3% |
| `combo_hn_3070_shared` | `partial_50` | -14.16% | $8,583.55 | -14.16% | 21 | — | 529 | 30 | 451 | 113 | 21.7% |
| `combo_hn_3070_shared` | `gap_miss` | +10.83% | $11,083.24 | +0.00% | 0 | — | 203 | 16 | 0 | 119 | 47.8% |
| `combo_eer_5050_shared` | `ideal` | +14.58% | $11,458.38 | +0.00% | 0 | — | 113 | 0 | 0 | 237 | 34.8% |
| `combo_eer_5050_shared` | `limit_prior` | -3.43% | $9,656.68 | -4.85% | 21 | — | 124 | 24 | 0 | 248 | 34.8% |
| `combo_eer_5050_shared` | `limit_open` | +13.84% | $11,383.63 | +0.00% | 0 | — | 108 | 5 | 0 | 237 | 34.8% |
| `combo_eer_5050_shared` | `market_mid` | -2.51% | $9,749.05 | -2.51% | 7 | — | 99 | 0 | 0 | 230 | 39.1% |
| `combo_eer_5050_shared` | `market_adverse` | -29.38% | $7,061.86 | -29.38% | 19 | — | 91 | 0 | 0 | 226 | 26.1% |
| `combo_eer_5050_shared` | `market_favorable` | +74.41% | $17,441.39 | +0.00% | 0 | — | 131 | 0 | 0 | 246 | 78.3% |
| `combo_eer_5050_shared` | `partial_50` | -5.38% | $9,461.76 | -5.38% | 12 | — | 435 | 26 | 368 | 288 | 34.8% |
| `combo_eer_5050_shared` | `gap_miss` | +16.18% | $11,617.78 | +0.00% | 0 | — | 125 | 6 | 0 | 243 | 39.1% |
| `combo_sn_3070_shared` | `ideal` | +14.41% | $11,441.28 | +0.00% | 0 | — | 207 | 10 | 0 | 165 | 65.2% |
| `combo_sn_3070_shared` | `limit_prior` | +2.60% | $10,259.93 | -1.58% | 9 | — | 164 | 48 | 0 | 187 | 52.2% |
| `combo_sn_3070_shared` | `limit_open` | +5.30% | $10,529.89 | -0.05% | 1 | — | 195 | 23 | 0 | 173 | 56.5% |
| `combo_sn_3070_shared` | `market_mid` | -14.03% | $8,597.47 | -14.03% | 20 | — | 207 | 10 | 0 | 165 | 30.4% |
| `combo_sn_3070_shared` | `market_adverse` | -50.03% | $4,997.29 | -50.03% | 22 | — | 205 | 10 | 0 | 166 | 13.0% |
| `combo_sn_3070_shared` | `market_favorable` | +120.88% | $22,088.41 | +0.00% | 0 | — | 207 | 10 | 0 | 165 | 87.0% |
| `combo_sn_3070_shared` | `partial_50` | -10.16% | $8,984.37 | -10.16% | 20 | — | 556 | 28 | 472 | 172 | 13.0% |
| `combo_sn_3070_shared` | `gap_miss` | +13.46% | $11,346.40 | +0.00% | 0 | — | 203 | 12 | 0 | 165 | 65.2% |
| `combo_sf_7030_shared` | `ideal` | +15.28% | $11,527.62 | +0.00% | 0 | — | 163 | 10 | 0 | 356 | 60.9% |
| `combo_sf_7030_shared` | `limit_prior` | +7.22% | $10,721.91 | -0.35% | 1 | — | 142 | 40 | 0 | 354 | 43.5% |
| `combo_sf_7030_shared` | `limit_open` | +9.72% | $10,971.64 | +0.00% | 0 | — | 160 | 21 | 0 | 368 | 56.5% |
| `combo_sf_7030_shared` | `market_mid` | +0.01% | $10,000.51 | -0.47% | 1 | — | 147 | 10 | 0 | 339 | 39.1% |
| `combo_sf_7030_shared` | `market_adverse` | -24.23% | $7,577.22 | -24.23% | 22 | — | 147 | 10 | 0 | 341 | 34.8% |
| `combo_sf_7030_shared` | `market_favorable` | +54.63% | $15,462.67 | +0.00% | 0 | — | 167 | 10 | 0 | 362 | 73.9% |
| `combo_sf_7030_shared` | `partial_50` | +1.15% | $10,114.79 | -0.29% | 1 | — | 495 | 31 | 418 | 402 | 34.8% |
| `combo_sf_7030_shared` | `gap_miss` | +12.82% | $11,282.28 | +0.00% | 0 | — | 159 | 12 | 0 | 352 | 56.5% |
| `combo_sn_7030_shared` | `ideal` | +14.35% | $11,434.51 | +0.00% | 0 | — | 205 | 10 | 0 | 166 | 65.2% |
| `combo_sn_7030_shared` | `limit_prior` | +5.21% | $10,520.54 | -1.24% | 7 | — | 160 | 48 | 0 | 189 | 47.8% |
| `combo_sn_7030_shared` | `limit_open` | +7.27% | $10,726.98 | -1.04% | 5 | — | 191 | 23 | 0 | 175 | 47.8% |
| `combo_sn_7030_shared` | `market_mid` | -6.84% | $9,316.15 | -8.98% | 20 | — | 199 | 10 | 0 | 169 | 39.1% |
| `combo_sn_7030_shared` | `market_adverse` | -35.93% | $6,406.66 | -35.93% | 22 | — | 199 | 10 | 0 | 169 | 13.0% |
| `combo_sn_7030_shared` | `market_favorable` | +85.29% | $18,529.45 | +0.00% | 0 | — | 205 | 10 | 0 | 166 | 87.0% |
| `combo_sn_7030_shared` | `partial_50` | -5.94% | $9,405.68 | -6.25% | 19 | — | 495 | 20 | 407 | 169 | 26.1% |
| `combo_sn_7030_shared` | `gap_miss` | +13.30% | $11,329.78 | +0.00% | 0 | — | 199 | 12 | 0 | 167 | 65.2% |
| `combo_fse_333_shared` | `ideal` | +12.64% | $11,264.55 | +0.00% | 0 | — | 270 | 10 | 0 | 523 | 60.9% |
| `combo_fse_333_shared` | `limit_prior` | +5.00% | $10,500.22 | +0.00% | 0 | — | 222 | 54 | 0 | 497 | 47.8% |
| `combo_fse_333_shared` | `limit_open` | +10.11% | $11,011.40 | +0.00% | 0 | — | 272 | 24 | 0 | 536 | 47.8% |
| `combo_fse_333_shared` | `market_mid` | -2.85% | $9,715.51 | -2.85% | 4 | — | 266 | 10 | 0 | 519 | 39.1% |
| `combo_fse_333_shared` | `market_adverse` | -26.86% | $7,314.51 | -26.86% | 20 | — | 256 | 10 | 0 | 508 | 26.1% |
| `combo_fse_333_shared` | `market_favorable` | +56.41% | $15,640.74 | +0.00% | 0 | — | 272 | 10 | 0 | 526 | 73.9% |
| `combo_fse_333_shared` | `partial_50` | -3.06% | $9,694.30 | -3.06% | 5 | — | 745 | 49 | 622 | 582 | 43.5% |
| `combo_fse_333_shared` | `gap_miss` | +9.91% | $10,991.14 | +0.00% | 0 | — | 258 | 15 | 0 | 515 | 60.9% |
| `combo_sf_5050_shared` | `ideal` | +12.96% | $11,295.92 | +0.00% | 0 | — | 163 | 10 | 0 | 360 | 60.9% |
| `combo_sf_5050_shared` | `limit_prior` | +4.90% | $10,489.64 | -0.16% | 1 | — | 144 | 40 | 0 | 357 | 47.8% |
| `combo_sf_5050_shared` | `limit_open` | +8.84% | $10,884.00 | +0.00% | 0 | — | 168 | 21 | 0 | 378 | 56.5% |
| `combo_sf_5050_shared` | `market_mid` | -1.29% | $9,870.54 | -1.29% | 5 | — | 147 | 10 | 0 | 341 | 47.8% |
| `combo_sf_5050_shared` | `market_adverse` | -23.57% | $7,643.26 | -23.57% | 22 | — | 143 | 10 | 0 | 341 | 39.1% |
| `combo_sf_5050_shared` | `market_favorable` | +50.17% | $15,017.12 | +0.00% | 0 | — | 171 | 10 | 0 | 366 | 73.9% |
| `combo_sf_5050_shared` | `partial_50` | -0.39% | $9,960.98 | -0.39% | 4 | — | 516 | 33 | 443 | 404 | 39.1% |
| `combo_sf_5050_shared` | `gap_miss` | +9.04% | $10,903.63 | +0.00% | 0 | — | 163 | 12 | 0 | 358 | 60.9% |
| `combo_fes_403030_shared` | `ideal` | +11.07% | $11,106.77 | +0.00% | 0 | — | 268 | 10 | 0 | 522 | 60.9% |
| `combo_fes_403030_shared` | `limit_prior` | +4.47% | $10,446.79 | +0.00% | 0 | — | 222 | 54 | 0 | 499 | 47.8% |
| `combo_fes_403030_shared` | `limit_open` | +8.66% | $10,866.42 | +0.00% | 0 | — | 270 | 24 | 0 | 535 | 52.2% |
| `combo_fes_403030_shared` | `market_mid` | -3.67% | $9,632.88 | -3.67% | 5 | — | 254 | 10 | 0 | 507 | 43.5% |
| `combo_fes_403030_shared` | `market_adverse` | -26.56% | $7,344.08 | -26.56% | 20 | — | 233 | 10 | 0 | 496 | 30.4% |
| `combo_fes_403030_shared` | `market_favorable` | +50.58% | $15,057.77 | +0.00% | 0 | — | 274 | 10 | 0 | 527 | 73.9% |
| `combo_fes_403030_shared` | `partial_50` | -3.77% | $9,623.34 | -3.77% | 6 | — | 744 | 49 | 620 | 582 | 43.5% |
| `combo_fes_403030_shared` | `gap_miss` | +8.11% | $10,810.49 | +0.00% | 0 | — | 258 | 16 | 0 | 515 | 60.9% |
| `combo_ef_5050_shared` | `ideal` | +10.16% | $11,016.45 | +0.00% | 0 | — | 185 | 12 | 0 | 433 | 56.5% |
| `combo_ef_5050_shared` | `limit_prior` | +4.06% | $10,406.00 | +0.00% | 0 | — | 174 | 44 | 0 | 437 | 52.2% |
| `combo_ef_5050_shared` | `limit_open` | +9.28% | $10,928.34 | +0.00% | 0 | — | 195 | 21 | 0 | 444 | 56.5% |
| `combo_ef_5050_shared` | `market_mid` | -1.94% | $9,806.21 | -1.94% | 4 | — | 177 | 12 | 0 | 423 | 34.8% |
| `combo_ef_5050_shared` | `market_adverse` | -21.83% | $7,816.61 | -21.83% | 20 | — | 162 | 10 | 0 | 403 | 30.4% |
| `combo_ef_5050_shared` | `market_favorable` | +43.79% | $14,379.21 | +0.00% | 0 | — | 195 | 12 | 0 | 438 | 65.2% |
| `combo_ef_5050_shared` | `partial_50` | -6.12% | $9,387.78 | -6.12% | 8 | — | 594 | 41 | 482 | 514 | 39.1% |
| `combo_ef_5050_shared` | `gap_miss` | +8.27% | $10,826.63 | +0.00% | 0 | — | 175 | 16 | 0 | 426 | 47.8% |
| `combo_fe_5050_shared` | `ideal` | +10.16% | $11,016.45 | +0.00% | 0 | — | 185 | 12 | 0 | 433 | 56.5% |
| `combo_fe_5050_shared` | `limit_prior` | +4.06% | $10,406.00 | +0.00% | 0 | — | 174 | 44 | 0 | 437 | 52.2% |
| `combo_fe_5050_shared` | `limit_open` | +9.28% | $10,928.34 | +0.00% | 0 | — | 195 | 21 | 0 | 444 | 56.5% |
| `combo_fe_5050_shared` | `market_mid` | -1.94% | $9,806.21 | -1.94% | 4 | — | 177 | 12 | 0 | 423 | 34.8% |
| `combo_fe_5050_shared` | `market_adverse` | -21.83% | $7,816.61 | -21.83% | 20 | — | 162 | 10 | 0 | 403 | 30.4% |
| `combo_fe_5050_shared` | `market_favorable` | +43.79% | $14,379.21 | +0.00% | 0 | — | 195 | 12 | 0 | 438 | 65.2% |
| `combo_fe_5050_shared` | `partial_50` | -6.12% | $9,387.78 | -6.12% | 8 | — | 594 | 41 | 482 | 514 | 39.1% |
| `combo_fe_5050_shared` | `gap_miss` | +8.27% | $10,826.63 | +0.00% | 0 | — | 175 | 16 | 0 | 426 | 47.8% |
| `combo_ne1_5050_shared` | `ideal` | +9.35% | $10,934.88 | +0.00% | 0 | — | 251 | 22 | 0 | 134 | 34.8% |
| `combo_ne1_5050_shared` | `limit_prior` | -5.07% | $9,493.25 | -5.07% | 13 | — | 198 | 70 | 0 | 182 | 30.4% |
| `combo_ne1_5050_shared` | `limit_open` | +2.84% | $10,283.57 | +0.00% | 0 | — | 238 | 35 | 0 | 148 | 30.4% |
| `combo_ne1_5050_shared` | `market_mid` | -18.69% | $8,131.20 | -18.69% | 18 | — | 249 | 22 | 0 | 135 | 8.7% |
| `combo_ne1_5050_shared` | `market_adverse` | -53.30% | $4,669.55 | -53.30% | 23 | — | 241 | 22 | 0 | 139 | 0.0% |
| `combo_ne1_5050_shared` | `market_favorable` | +117.55% | $21,754.56 | +0.00% | 0 | — | 253 | 22 | 0 | 133 | 73.9% |
| `combo_ne1_5050_shared` | `partial_50` | -13.40% | $8,659.50 | -13.40% | 16 | — | 660 | 42 | 552 | 151 | 8.7% |
| `combo_ne1_5050_shared` | `gap_miss` | +7.04% | $10,703.95 | +0.00% | 0 | — | 237 | 29 | 0 | 141 | 39.1% |
| `combo_sf_3070_shared` | `ideal` | +11.28% | $11,127.79 | +0.00% | 0 | — | 155 | 10 | 0 | 351 | 56.5% |
| `combo_sf_3070_shared` | `limit_prior` | +1.63% | $10,163.43 | +0.00% | 0 | — | 146 | 40 | 0 | 360 | 56.5% |
| `combo_sf_3070_shared` | `limit_open` | +8.12% | $10,811.51 | +0.00% | 0 | — | 166 | 21 | 0 | 379 | 47.8% |
| `combo_sf_3070_shared` | `market_mid` | -1.19% | $9,881.13 | -1.19% | 5 | — | 141 | 10 | 0 | 335 | 43.5% |
| `combo_sf_3070_shared` | `market_adverse` | -19.21% | $8,079.17 | -19.21% | 21 | — | 131 | 10 | 0 | 322 | 39.1% |
| `combo_sf_3070_shared` | `market_favorable` | +41.44% | $14,143.76 | +0.00% | 0 | — | 161 | 10 | 0 | 356 | 78.3% |
| `combo_sf_3070_shared` | `partial_50` | -2.31% | $9,768.45 | -2.31% | 5 | — | 518 | 33 | 443 | 410 | 47.8% |
| `combo_sf_3070_shared` | `gap_miss` | +7.27% | $10,726.64 | +0.00% | 0 | — | 153 | 12 | 0 | 348 | 56.5% |
| `combo_hf_5050_shared` | `ideal` | +5.29% | $10,528.80 | -0.38% | 1 | — | 190 | 16 | 0 | 331 | 47.8% |
| `combo_hf_5050_shared` | `limit_prior` | +9.02% | $10,902.21 | +0.00% | 0 | — | 157 | 44 | 0 | 321 | 56.5% |
| `combo_hf_5050_shared` | `limit_open` | -0.40% | $9,960.32 | -1.37% | 5 | — | 175 | 28 | 0 | 333 | 39.1% |
| `combo_hf_5050_shared` | `market_mid` | -13.19% | $8,681.39 | -13.19% | 19 | — | 188 | 16 | 0 | 330 | 26.1% |
| `combo_hf_5050_shared` | `market_adverse` | -39.22% | $6,077.84 | -39.22% | 23 | — | 188 | 16 | 0 | 330 | 8.7% |
| `combo_hf_5050_shared` | `market_favorable` | +62.32% | $16,232.06 | +0.00% | 0 | — | 192 | 16 | 0 | 334 | 69.6% |
| `combo_hf_5050_shared` | `partial_50` | -9.28% | $9,072.06 | -9.28% | 17 | — | 489 | 33 | 416 | 328 | 34.8% |
| `combo_hf_5050_shared` | `gap_miss` | +5.58% | $10,558.11 | -0.12% | 1 | — | 188 | 17 | 0 | 332 | 47.8% |
| `combo_fer_5050_shared` | `ideal` | +8.46% | $10,846.15 | +0.00% | 0 | — | 193 | 12 | 0 | 439 | 52.2% |
| `combo_fer_5050_shared` | `limit_prior` | +2.92% | $10,292.30 | +0.00% | 0 | — | 174 | 43 | 0 | 438 | 47.8% |
| `combo_fer_5050_shared` | `limit_open` | +7.67% | $10,767.53 | +0.00% | 0 | — | 193 | 21 | 0 | 447 | 56.5% |
| `combo_fer_5050_shared` | `market_mid` | -3.42% | $9,658.27 | -3.42% | 5 | — | 177 | 12 | 0 | 427 | 34.8% |
| `combo_fer_5050_shared` | `market_adverse` | -22.94% | $7,705.79 | -22.94% | 20 | — | 169 | 12 | 0 | 415 | 30.4% |
| `combo_fer_5050_shared` | `market_favorable` | +41.73% | $14,173.36 | +0.00% | 0 | — | 199 | 12 | 0 | 444 | 65.2% |
| `combo_fer_5050_shared` | `partial_50` | -6.67% | $9,332.72 | -6.67% | 10 | — | 589 | 39 | 474 | 516 | 39.1% |
| `combo_fer_5050_shared` | `gap_miss` | +8.01% | $10,800.60 | +0.00% | 0 | — | 179 | 17 | 0 | 430 | 47.8% |
| `combo_nj_5050_shared` | `ideal` | +4.32% | $10,432.12 | -3.96% | 7 | — | 251 | 26 | 0 | 128 | 34.8% |
| `combo_nj_5050_shared` | `limit_prior` | -0.61% | $9,939.05 | -2.12% | 12 | — | 210 | 68 | 0 | 172 | 30.4% |
| `combo_nj_5050_shared` | `limit_open` | -0.40% | $9,959.94 | -4.81% | 10 | — | 239 | 37 | 0 | 139 | 34.8% |
| `combo_nj_5050_shared` | `market_mid` | -19.12% | $8,087.84 | -19.12% | 22 | — | 245 | 26 | 0 | 131 | 13.0% |
| `combo_nj_5050_shared` | `market_adverse` | -50.29% | $4,970.66 | -50.29% | 22 | — | 241 | 26 | 0 | 133 | 0.0% |
| `combo_nj_5050_shared` | `market_favorable` | +118.94% | $21,894.12 | +0.00% | 0 | — | 251 | 26 | 0 | 128 | 69.6% |
| `combo_nj_5050_shared` | `partial_50` | -15.77% | $8,422.90 | -15.77% | 22 | — | 674 | 46 | 569 | 142 | 13.0% |
| `combo_nj_5050_shared` | `gap_miss` | +2.52% | $10,251.69 | -3.27% | 7 | — | 243 | 30 | 0 | 132 | 30.4% |
| `combo_jf_5050_shared` | `ideal` | +5.78% | $10,577.76 | +0.00% | 0 | — | 180 | 28 | 0 | 295 | 60.9% |
| `combo_jf_5050_shared` | `limit_prior` | +2.61% | $10,260.71 | -0.36% | 3 | — | 175 | 58 | 0 | 300 | 52.2% |
| `combo_jf_5050_shared` | `limit_open` | +2.69% | $10,269.29 | +0.00% | 0 | — | 189 | 36 | 0 | 299 | 52.2% |
| `combo_jf_5050_shared` | `market_mid` | -6.94% | $9,306.23 | -6.94% | 6 | — | 158 | 28 | 0 | 286 | 34.8% |
| `combo_jf_5050_shared` | `market_adverse` | -26.32% | $7,367.60 | -26.32% | 22 | — | 156 | 28 | 0 | 287 | 30.4% |
| `combo_jf_5050_shared` | `market_favorable` | +40.35% | $14,035.50 | +0.00% | 0 | — | 194 | 28 | 0 | 288 | 65.2% |
| `combo_jf_5050_shared` | `partial_50` | -8.31% | $9,169.00 | -8.31% | 10 | — | 557 | 49 | 469 | 328 | 26.1% |
| `combo_jf_5050_shared` | `gap_miss` | +4.73% | $10,473.21 | +0.00% | 0 | — | 172 | 31 | 0 | 299 | 52.2% |
| `combo_ef_3070_shared` | `ideal` | +6.75% | $10,674.97 | +0.00% | 0 | — | 173 | 12 | 0 | 429 | 56.5% |
| `combo_ef_3070_shared` | `limit_prior` | +3.50% | $10,349.60 | +0.00% | 0 | — | 168 | 44 | 0 | 436 | 47.8% |
| `combo_ef_3070_shared` | `limit_open` | +5.90% | $10,590.25 | +0.00% | 0 | — | 189 | 20 | 0 | 447 | 56.5% |
| `combo_ef_3070_shared` | `market_mid` | -3.39% | $9,660.83 | -3.39% | 5 | — | 155 | 8 | 0 | 401 | 43.5% |
| `combo_ef_3070_shared` | `market_adverse` | -20.50% | $7,949.66 | -20.50% | 21 | — | 149 | 12 | 0 | 403 | 34.8% |
| `combo_ef_3070_shared` | `market_favorable` | +32.68% | $13,267.76 | +0.00% | 0 | — | 185 | 12 | 0 | 435 | 65.2% |
| `combo_ef_3070_shared` | `partial_50` | -8.10% | $9,189.61 | -8.10% | 13 | — | 575 | 39 | 460 | 512 | 34.8% |
| `combo_ef_3070_shared` | `gap_miss` | +4.98% | $10,497.93 | +0.00% | 0 | — | 167 | 15 | 0 | 424 | 52.2% |
| `combo_nf_5050_shared` | `ideal` | +6.08% | $10,608.54 | +0.00% | 0 | — | 196 | 22 | 0 | 349 | 56.5% |
| `combo_nf_5050_shared` | `limit_prior` | +2.57% | $10,256.99 | -0.05% | 1 | — | 170 | 63 | 0 | 362 | 52.2% |
| `combo_nf_5050_shared` | `limit_open` | +4.09% | $10,409.49 | +0.00% | 0 | — | 197 | 31 | 0 | 355 | 47.8% |
| `combo_nf_5050_shared` | `market_mid` | -6.72% | $9,327.61 | -6.72% | 7 | — | 182 | 22 | 0 | 336 | 43.5% |
| `combo_nf_5050_shared` | `market_adverse` | -26.83% | $7,317.05 | -26.83% | 22 | — | 182 | 22 | 0 | 336 | 30.4% |
| `combo_nf_5050_shared` | `market_favorable` | +40.57% | $14,057.10 | +0.00% | 0 | — | 204 | 22 | 0 | 349 | 65.2% |
| `combo_nf_5050_shared` | `partial_50` | -6.91% | $9,308.99 | -6.91% | 9 | — | 541 | 37 | 442 | 356 | 34.8% |
| `combo_nf_5050_shared` | `gap_miss` | +3.57% | $10,357.26 | +0.00% | 0 | — | 190 | 24 | 0 | 348 | 56.5% |
| `combo_fh_7030_shared` | `ideal` | +1.84% | $10,183.90 | +0.00% | 0 | — | 188 | 16 | 0 | 330 | 56.5% |
| `combo_fh_7030_shared` | `limit_prior` | +5.24% | $10,523.67 | +0.00% | 0 | — | 157 | 44 | 0 | 321 | 56.5% |
| `combo_fh_7030_shared` | `limit_open` | -1.80% | $9,820.22 | -2.52% | 5 | — | 181 | 28 | 0 | 342 | 52.2% |
| `combo_fh_7030_shared` | `market_mid` | -10.74% | $8,925.91 | -10.74% | 12 | — | 186 | 16 | 0 | 327 | 30.4% |
| `combo_fh_7030_shared` | `market_adverse` | -30.43% | $6,956.85 | -30.43% | 23 | — | 186 | 16 | 0 | 327 | 21.7% |
| `combo_fh_7030_shared` | `market_favorable` | +41.22% | $14,121.65 | +0.00% | 0 | — | 188 | 16 | 0 | 328 | 60.9% |
| `combo_fh_7030_shared` | `partial_50` | -8.77% | $9,122.94 | -8.77% | 15 | — | 480 | 33 | 407 | 328 | 30.4% |
| `combo_fh_7030_shared` | `gap_miss` | +1.93% | $10,192.58 | +0.00% | 0 | — | 186 | 17 | 0 | 331 | 56.5% |
| `combo_fe1_5050_shared` | `ideal` | +2.03% | $10,202.71 | +0.00% | 0 | — | 247 | 28 | 0 | 367 | 52.2% |
| `combo_fe1_5050_shared` | `limit_prior` | +1.58% | $10,158.28 | +0.00% | 0 | — | 192 | 71 | 0 | 367 | 52.2% |
| `combo_fe1_5050_shared` | `limit_open` | -0.33% | $9,967.39 | -0.98% | 4 | — | 237 | 38 | 0 | 370 | 47.8% |
| `combo_fe1_5050_shared` | `market_mid` | -12.88% | $8,712.17 | -12.88% | 14 | — | 247 | 28 | 0 | 367 | 30.4% |
| `combo_fe1_5050_shared` | `market_adverse` | -35.18% | $6,481.55 | -35.18% | 23 | — | 247 | 28 | 0 | 369 | 4.3% |
| `combo_fe1_5050_shared` | `market_favorable` | +49.67% | $14,966.84 | +0.00% | 0 | — | 249 | 28 | 0 | 366 | 65.2% |
| `combo_fe1_5050_shared` | `partial_50` | -9.59% | $9,041.16 | -9.59% | 13 | — | 630 | 53 | 519 | 390 | 30.4% |
| `combo_fe1_5050_shared` | `gap_miss` | -0.94% | $9,905.58 | -2.14% | 4 | — | 233 | 35 | 0 | 370 | 43.5% |
| `combo_e1er_5050_shared` | `ideal` | -3.12% | $9,687.86 | -4.86% | 17 | — | 163 | 16 | 0 | 139 | 17.4% |
| `combo_e1er_5050_shared` | `limit_prior` | -9.82% | $9,017.73 | -10.25% | 21 | — | 128 | 44 | 0 | 161 | 30.4% |
| `combo_e1er_5050_shared` | `limit_open` | -10.05% | $8,995.06 | -10.05% | 19 | — | 158 | 24 | 0 | 147 | 13.0% |
| `combo_e1er_5050_shared` | `market_mid` | -24.92% | $7,507.65 | -24.92% | 22 | — | 163 | 16 | 0 | 139 | 8.7% |
| `combo_e1er_5050_shared` | `market_adverse` | -53.91% | $4,608.55 | -53.91% | 23 | — | 163 | 16 | 0 | 139 | 0.0% |
| `combo_e1er_5050_shared` | `market_favorable` | +91.16% | $19,115.98 | +0.00% | 0 | — | 163 | 16 | 0 | 139 | 65.2% |
| `combo_e1er_5050_shared` | `partial_50` | -13.37% | $8,662.86 | -13.37% | 20 | — | 479 | 32 | 408 | 155 | 17.4% |
| `combo_e1er_5050_shared` | `gap_miss` | -1.52% | $9,848.22 | -1.52% | 14 | — | 147 | 24 | 0 | 143 | 30.4% |

## Spotlight `combo_sh_5050_shared` daily close equity

Starts YES is a **start-date** chip (wake $10k on that date, run to the end). Sess win% is the share of sessions whose close beat the prior close. Do not write Starts YES as a daily win rate.

| Date | ideal (published 09:30) | market mid (PR 241 α) | market adverse / WORST | limit @ official open | partial 50% of market_mid | market favorable / BEST (unlikely) |
|---|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | $10,345.37 | $10,027.58 | $9,546.05 | $10,345.37 | $10,014.93 | $10,588.95 |
| 2026-08-14 | $10,223.22 | $9,690.38 | $8,823.57 | $10,237.39 | $9,992.78 | $11,653.81 |
| 2026-08-17 | $10,119.93 | $9,310.50 | $8,048.41 | $10,059.67 | $9,825.99 | $12,530.35 |
| 2026-08-18 | $10,449.47 | $9,487.65 | $7,980.87 | $10,256.10 | $9,965.52 | $13,344.73 |
| 2026-08-19 | $10,461.73 | $9,465.05 | $7,907.23 | $10,230.65 | $9,978.76 | $13,638.08 |
| 2026-08-20 | $10,327.03 | $9,085.10 | $7,168.80 | $10,018.11 | $9,787.63 | $14,654.96 |
| 2026-08-21 | $10,360.30 | $8,983.42 | $6,932.11 | $9,997.59 | $9,861.12 | $15,678.91 |
| 2026-08-24 | $11,590.89 | $9,673.02 | $6,790.01 | $10,460.88 | $10,018.44 | $17,741.80 |
| 2026-08-25 | $12,291.73 | $9,790.85 | $6,325.51 | $10,837.59 | $10,221.57 | $19,305.83 |
| 2026-08-26 | $12,065.23 | $9,362.09 | $5,747.74 | $10,814.33 | $9,994.03 | $20,588.17 |
| 2026-08-27 | $12,556.76 | $9,437.39 | $5,391.10 | $10,974.65 | $10,147.72 | $23,723.52 |
| 2026-08-28 | $13,035.89 | $9,407.13 | $4,945.24 | $11,303.21 | $9,984.74 | $26,044.20 |
| 2026-08-31 | $13,024.80 | $9,315.68 | $4,797.21 | $11,292.58 | $10,104.10 | $26,911.56 |
| 2026-09-01 | $13,123.94 | $9,364.23 | $4,784.03 | $11,373.68 | $10,122.32 | $27,142.93 |
| 2026-09-02 | $13,182.97 | $9,371.19 | $4,756.24 | $11,423.60 | $10,054.99 | $27,557.24 |
| 2026-09-03 | $12,993.72 | $9,119.94 | $4,522.64 | $11,258.58 | $9,867.30 | $28,439.03 |
| 2026-09-04 | $13,495.42 | $9,285.76 | $4,463.65 | $11,492.18 | $9,961.96 | $30,652.26 |
| 2026-09-08 | $13,552.65 | $9,207.93 | $4,288.19 | $11,616.15 | $9,974.16 | $31,926.28 |
| 2026-09-09 | $13,601.13 | $9,212.15 | $4,258.08 | $11,658.33 | $9,909.41 | $32,359.65 |
| 2026-09-10 | $13,688.36 | $9,237.86 | $4,233.77 | $11,733.72 | $9,886.88 | $32,759.17 |
| 2026-09-11 | $13,748.76 | $9,049.37 | $3,971.97 | $11,788.46 | $9,761.95 | $34,215.44 |
| 2026-09-14 | $13,748.76 | $9,049.37 | $3,971.97 | $11,788.46 | $9,761.95 | $34,215.44 |
| 2026-09-15 | $13,748.76 | $9,049.37 | $3,971.97 | $11,788.46 | $9,761.95 | $34,215.44 |

## Not yet run

All listed Action blotters were graded.

Elapsed 266.21s. Starts policy: `featured`.

