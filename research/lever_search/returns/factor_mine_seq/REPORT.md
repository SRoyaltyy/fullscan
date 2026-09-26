# Group 3 initial score

study: initial

This is the initial Group 3 score of the 110 Factor Mine recipes. The preregistration was not edited. No locked ledger was rewritten.

Price features are the frozen engine on `data/prices/ohlc.parquet`, sha256 `559c8cf099808930bef2b4de4280b4e902883c9a1de85c8a417074f11aaefa55`, blob `3456f7f489a6fa7033e8ae5cc942d8279f0113e3`. Feature bars are dated strictly before the session. Every recipe trades at the 09:30 open, so the session open is the fill and the gap. The session close is the mark only.

The pinned stock_book blob is the candidate list on a usable stock_book day. That blob has buy lists and does not carry blue, alarm, zero_red, or camera counts. Those flags are not recomputed, so they stay false. AB Part A is not recomputed. AB tone is the enriched checklist score on days both AB files are usable.

On days without usable stock_book, the union list is prior-day gainers (top 25), prior-day movers (top 20), the hot list (top 30), continuation (top 8), and the overnight gap list (top 20). Liquidity is a 20-session average volume of at least 500,000 shares. Market cap is not on the bar file.

Every recipe reads the price store. `rebuild_match` on that store is null, so every recipe is `struck`. A struck recipe is not something good.

Sessions written: 35 (2026-08-07 through 2026-09-25).
Luck-test denominator: 9280. p is a one-sided t-test that the mean check-day return is above zero, multiplied by 9,280.

Line (a) cumulative Futubull check-day return >= 20%: 3
Line (b) best stock removed still positive: 1
Line (c) Futubull compound 2026-09-14 through 2026-09-25 >= 0: 57
Line (d) Futubull luck p < 0.05 on 9,280: 0
All four lines: 0

Verdict: `nothing proven yet`

## Top 10

| rank | recipe | label | check days | Futubull cum | Futubull mean | 15bp cum | 15bp mean | best removed | from 09-14 | under $3 | luck p |
| ---: | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | `probable_h3` | struck | 16 | 78.27% | 6.95% | 91.51% | 7.22% | -28.86% | -0.66% | 40.2% | 1 |
| 2 | `probable_h5` | struck | 16 | 54.55% | 4.77% | 58.59% | 4.84% | -25.75% | 1.04% | 45.9% | 1 |
| 3 | `short_extended_h3` | struck | 16 | 26.17% | 1.62% | 32.44% | 1.90% | 20.81% | 12.35% | 38.7% | 1 |
| 4 | `short_news_r_h3` | struck | 6 | 0.97% | 0.17% | 1.08% | 0.18% | -0.09% | 0.00% | 0.0% | 1 |
| 5 | `union_news_missing_h1` | struck | 1 | 0.40% | 0.40% | 0.55% | 0.55% | -0.15% | 0.00% | 6.2% | 1 |
| 6 | `union_news_missing_h3` | struck | 1 | 0.37% | 0.37% | 0.52% | 0.52% | -0.16% | 0.00% | 11.1% | 1 |
| 7 | `union_news_g_h1` | struck | 1 | 0.11% | 0.11% | 0.18% | 0.18% | -0.35% | 0.00% | 0.0% | 1 |
| 8 | `union_news_present_h1` | struck | 1 | 0.11% | 0.11% | 0.18% | 0.18% | -0.35% | 0.00% | 0.0% | 1 |
| 9 | `union_news_g_h3` | struck | 1 | 0.11% | 0.11% | 0.17% | 0.17% | -0.35% | 0.00% | 0.0% | 1 |
| 10 | `union_news_present_h3` | struck | 1 | 0.11% | 0.11% | 0.17% | 0.17% | -0.35% | 0.00% | 0.0% | 1 |

## All 110

| recipe | label | search | check | a | b | c | d | Futubull cum | Futubull mean | 15bp cum | 15bp mean | from 09-14 | under $3 |
| --- | --- | ---: | ---: | --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `probable_h3` | struck | 25 | 16 | yes | no | no | no | 78.27% | 6.95% | 91.51% | 7.22% | -0.66% | 40.2% |
| `probable_h5` | struck | 25 | 16 | yes | no | yes | no | 54.55% | 4.77% | 58.59% | 4.84% | 1.04% | 45.9% |
| `short_extended_h3` | struck | 25 | 16 | yes | yes | yes | no | 26.17% | 1.62% | 32.44% | 1.90% | 12.35% | 38.7% |
| `short_news_r_h3` | struck | 11 | 6 | no | no | yes | no | 0.97% | 0.17% | 1.08% | 0.18% | 0.00% | 0.0% |
| `union_news_missing_h1` | struck | 4 | 1 | no | no | yes | no | 0.40% | 0.40% | 0.55% | 0.55% | 0.00% | 6.2% |
| `union_news_missing_h3` | struck | 4 | 1 | no | no | yes | no | 0.37% | 0.37% | 0.52% | 0.52% | 0.00% | 11.1% |
| `union_news_g_h1` | struck | 4 | 1 | no | no | yes | no | 0.11% | 0.11% | 0.18% | 0.18% | 0.00% | 0.0% |
| `union_news_present_h1` | struck | 4 | 1 | no | no | yes | no | 0.11% | 0.11% | 0.18% | 0.18% | 0.00% | 0.0% |
| `union_news_g_h3` | struck | 4 | 1 | no | no | yes | no | 0.11% | 0.11% | 0.17% | 0.17% | 0.00% | 0.0% |
| `union_news_present_h3` | struck | 4 | 1 | no | no | yes | no | 0.11% | 0.11% | 0.17% | 0.17% | 0.00% | 0.0% |
| `union_news_g_h5` | struck | 4 | 1 | no | no | yes | no | 0.11% | 0.11% | 0.18% | 0.18% | 0.00% | 0.0% |
| `union_e_green_h3` | struck | 3 | 0 | no | no | yes | no | 0.00% | 0.00% | 0.00% | 0.00% | 0.00% | 20.0% |
| `union_e_green_h1` | struck | 3 | 0 | no | no | yes | no | 0.00% | 0.00% | 0.00% | 0.00% | 0.00% | 20.0% |
| `short_alarm_h1` | struck | 10 | 7 | no | no | yes | no | 0.00% | 0.00% | 0.00% | 0.00% | 0.00% |  |
| `short_alarm_h3` | struck | 10 | 7 | no | no | yes | no | 0.00% | 0.00% | 0.00% | 0.00% | 0.00% |  |
| `short_r_down_h1` | struck | 14 | 11 | no | no | yes | no | 0.00% | 0.00% | 0.00% | 0.00% | 0.00% |  |
| `short_r_down_h3` | struck | 14 | 11 | no | no | yes | no | 0.00% | 0.00% | 0.00% | 0.00% | 0.00% |  |
| `union_blue_coil_h1` | struck | 4 | 1 | no | no | yes | no | 0.00% | 0.00% | 0.00% | 0.00% | 0.00% |  |
| `union_blue_coil_h3` | struck | 4 | 1 | no | no | yes | no | 0.00% | 0.00% | 0.00% | 0.00% | 0.00% |  |
| `union_blue_h1` | struck | 10 | 7 | no | no | yes | no | 0.00% | 0.00% | 0.00% | 0.00% | 0.00% |  |
| `union_blue_h3` | struck | 10 | 7 | no | no | yes | no | 0.00% | 0.00% | 0.00% | 0.00% | 0.00% |  |
| `union_blue_vol_h1` | struck | 4 | 1 | no | no | yes | no | 0.00% | 0.00% | 0.00% | 0.00% | 0.00% |  |
| `union_blue_vol_h3` | struck | 4 | 1 | no | no | yes | no | 0.00% | 0.00% | 0.00% | 0.00% | 0.00% |  |
| `union_catal_present_h1` | struck | 0 | 0 | no | no | yes | no | 0.00% | 0.00% | 0.00% | 0.00% | 0.00% |  |
| `union_catal_present_h3` | struck | 0 | 0 | no | no | yes | no | 0.00% | 0.00% | 0.00% | 0.00% | 0.00% |  |
| `union_join_g_h1` | struck | 0 | 0 | no | no | yes | no | 0.00% | 0.00% | 0.00% | 0.00% | 0.00% |  |
| `union_join_g_h3` | struck | 0 | 0 | no | no | yes | no | 0.00% | 0.00% | 0.00% | 0.00% | 0.00% |  |
| `union_join_present_h1` | struck | 0 | 0 | no | no | yes | no | 0.00% | 0.00% | 0.00% | 0.00% | 0.00% |  |
| `union_join_present_h3` | struck | 0 | 0 | no | no | yes | no | 0.00% | 0.00% | 0.00% | 0.00% | 0.00% |  |
| `union_join_vol_green_h1` | struck | 0 | 0 | no | no | yes | no | 0.00% | 0.00% | 0.00% | 0.00% | 0.00% |  |
| `union_join_vol_green_h3` | struck | 0 | 0 | no | no | yes | no | 0.00% | 0.00% | 0.00% | 0.00% | 0.00% |  |
| `union_news_vol_h1` | struck | 4 | 1 | no | no | yes | no | 0.00% | 0.00% | 0.00% | 0.00% | 0.00% |  |
| `union_news_vol_h3` | struck | 4 | 1 | no | no | yes | no | 0.00% | 0.00% | 0.00% | 0.00% | 0.00% |  |
| `union_r_up_h1` | struck | 8 | 5 | no | no | yes | no | 0.00% | 0.00% | 0.00% | 0.00% | 0.00% |  |
| `union_r_up_h3` | struck | 8 | 5 | no | no | yes | no | 0.00% | 0.00% | 0.00% | 0.00% | 0.00% |  |
| `union_vol_missing_h1` | struck | 10 | 7 | no | no | yes | no | 0.00% | 0.00% | 0.00% | 0.00% | 0.00% |  |
| `union_vol_missing_h3` | struck | 10 | 7 | no | no | yes | no | 0.00% | 0.00% | 0.00% | 0.00% | 0.00% |  |
| `union_white_coil_h1` | struck | 4 | 1 | no | no | yes | no | 0.00% | 0.00% | 0.00% | 0.00% | 0.00% |  |
| `union_white_coil_h3` | struck | 4 | 1 | no | no | yes | no | 0.00% | 0.00% | 0.00% | 0.00% | 0.00% |  |
| `union_white_h1` | struck | 10 | 7 | no | no | yes | no | 0.00% | 0.00% | 0.00% | 0.00% | 0.00% |  |
| `union_white_h3` | struck | 10 | 7 | no | no | yes | no | 0.00% | 0.00% | 0.00% | 0.00% | 0.00% |  |
| `union_white_h5` | struck | 10 | 7 | no | no | yes | no | 0.00% | 0.00% | 0.00% | 0.00% | 0.00% |  |
| `union_coil_green_h1` | struck | 4 | 1 | no | no | yes | no | -0.03% | -0.03% | 0.14% | 0.14% | 0.00% | 0.0% |
| `union_coil_green_h3` | struck | 4 | 1 | no | no | yes | no | -0.03% | -0.03% | 0.14% | 0.14% | 0.00% | 0.0% |
| `union_h3_exit_news_r` | struck | 4 | 1 | no | no | yes | no | -0.19% | -0.19% | -0.03% | -0.03% | 0.00% | 10.5% |
| `union_ab_g_h3` | struck | 5 | 5 | no | no | no | no | -0.33% | -0.07% | 0.24% | 0.05% | -10.87% | 2.6% |
| `probable_probable_ok_h1` | struck | 4 | 1 | no | no | yes | no | -0.91% | -0.91% | -0.22% | -0.22% | 0.00% | 34.5% |
| `probable_probable_ok_h3` | struck | 4 | 1 | no | no | yes | no | -0.91% | -0.91% | -0.23% | -0.23% | 0.00% | 36.5% |
| `short_news_r_h1` | struck | 11 | 6 | no | no | yes | no | -1.10% | -0.18% | -0.97% | -0.16% | 0.00% | 0.0% |
| `union_coil_off_h5` | struck | 10 | 7 | no | no | no | no | -1.69% | -0.24% | -1.33% | -0.19% | -4.33% | 2.2% |
| `union_candle_score_h1` | struck | 10 | 7 | no | no | no | no | -1.87% | -0.26% | 0.11% | 0.02% | -9.18% | 6.7% |
| `flatten_h5` | struck | 10 | 7 | no | no | no | no | -2.16% | -0.30% | -1.77% | -0.25% | -3.79% | 7.9% |
| `union_break10_h1` | struck | 10 | 7 | no | no | no | no | -2.17% | -0.29% | -0.66% | -0.08% | -7.67% | 7.0% |
| `union_last_green_h5` | struck | 10 | 7 | no | no | yes | no | -2.25% | -0.32% | -1.91% | -0.27% | 2.33% | 8.3% |
| `union_h5_exit_alarm` | struck | 10 | 7 | no | no | no | no | -2.39% | -0.34% | -2.00% | -0.28% | -0.69% | 6.1% |
| `union_candle_h1` | struck | 10 | 7 | no | no | no | no | -2.55% | -0.37% | -1.16% | -0.16% | -8.79% | 4.8% |
| `union_hot_n4_h1` | struck | 10 | 7 | no | no | no | no | -2.62% | -0.36% | -0.83% | -0.10% | -12.69% | 7.9% |
| `flatten_h3` | struck | 10 | 7 | no | no | no | no | -2.78% | -0.39% | -2.16% | -0.30% | -11.60% | 4.8% |
| `union_last_green_h1` | struck | 10 | 7 | no | no | no | no | -2.81% | -0.39% | -1.09% | -0.15% | -12.76% | 3.7% |
| `union_ab_g_h1` | struck | 5 | 5 | no | no | no | no | -3.18% | -0.64% | -2.18% | -0.44% | -9.21% | 1.2% |
| `union_vol_green_h3` | struck | 4 | 1 | no | no | yes | no | -3.31% | -3.31% | -3.16% | -3.16% | 0.00% | 25.0% |
| `union_vol_green_h1` | struck | 4 | 1 | no | no | yes | no | -3.32% | -3.32% | -3.12% | -3.12% | 0.00% | 26.7% |
| `union_vol_ab_h1` | struck | 1 | 1 | no | no | yes | no | -3.32% | -3.32% | -3.20% | -3.20% | 0.00% | 0.0% |
| `union_vol_ab_h3` | struck | 1 | 1 | no | no | yes | no | -3.32% | -3.32% | -3.20% | -3.20% | 0.00% | 0.0% |
| `union_h3_exit_red` | struck | 10 | 7 | no | no | no | no | -3.36% | -0.47% | -2.53% | -0.35% | -9.63% | 5.6% |
| `union_last_green_h3` | struck | 10 | 7 | no | no | no | no | -3.44% | -0.49% | -2.85% | -0.40% | -8.86% | 6.3% |
| `union_break10_h3` | struck | 10 | 7 | no | no | no | no | -3.60% | -0.50% | -3.11% | -0.43% | -9.54% | 11.0% |
| `union_candle_score_h3` | struck | 10 | 7 | no | no | no | no | -3.76% | -0.54% | -2.99% | -0.43% | -3.29% | 10.6% |
| `coil_h3_exit_alarm` | struck | 10 | 7 | no | no | no | no | -4.00% | -0.58% | -3.26% | -0.47% | -7.45% | 3.0% |
| `union_w_hot_candle_h1` | struck | 10 | 7 | no | no | no | no | -4.40% | -0.64% | -2.22% | -0.32% | -12.24% | 6.7% |
| `union_coil_off_h3` | struck | 10 | 7 | no | no | no | no | -4.41% | -0.64% | -3.67% | -0.53% | -5.44% | 1.9% |
| `union_h3_exit_alarm` | struck | 10 | 7 | no | no | no | no | -4.53% | -0.64% | -3.82% | -0.54% | -8.54% | 4.8% |
| `flatten_h1` | struck | 10 | 7 | no | no | no | no | -4.66% | -0.68% | -3.57% | -0.51% | -15.43% | 3.5% |
| `union_candle_h3` | struck | 10 | 7 | no | no | no | no | -4.74% | -0.69% | -4.12% | -0.59% | -1.73% | 6.5% |
| `union_hot_score_h1` | struck | 10 | 7 | no | no | no | no | -4.94% | -0.72% | -2.45% | -0.35% | -13.52% | 8.7% |
| `union_w_hot_cond_h1` | struck | 10 | 7 | no | no | no | no | -4.94% | -0.72% | -2.45% | -0.35% | -13.52% | 8.7% |
| `union_vol_g_h5` | struck | 10 | 7 | no | no | no | no | -5.81% | -0.84% | -5.63% | -0.81% | -6.57% | 14.9% |
| `flatten_vol_g_h3` | struck | 10 | 7 | no | no | no | no | -5.83% | -0.84% | -5.46% | -0.79% | -12.92% | 3.6% |
| `union_hot_n12_h1` | struck | 10 | 7 | no | no | no | no | -6.31% | -0.92% | -3.06% | -0.44% | -15.75% | 9.2% |
| `union_w_hot_candle_h3` | struck | 10 | 7 | no | no | no | no | -6.32% | -0.92% | -5.56% | -0.81% | -5.28% | 10.7% |
| `union_ret_5_h1` | struck | 10 | 7 | no | no | no | no | -6.34% | -0.92% | -3.87% | -0.56% | -15.76% | 9.4% |
| `union_cond_h1` | struck | 10 | 7 | no | no | no | no | -6.39% | -0.94% | -4.20% | -0.61% | -12.57% | 5.4% |
| `union_last_red_h1` | struck | 10 | 7 | no | no | no | no | -6.73% | -0.99% | -4.01% | -0.58% | -15.03% | 6.7% |
| `union_coil_off_h1` | struck | 10 | 7 | no | no | no | no | -6.93% | -1.02% | -4.97% | -0.72% | -6.66% | 1.5% |
| `union_hot_score_h3` | struck | 10 | 7 | no | no | no | no | -7.16% | -1.05% | -6.27% | -0.91% | -8.33% | 9.3% |
| `union_w_hot_cond_h3` | struck | 10 | 7 | no | no | no | no | -7.16% | -1.05% | -6.27% | -0.91% | -8.33% | 9.3% |
| `union_ret_5_h3` | struck | 10 | 7 | no | no | no | no | -7.42% | -1.08% | -6.51% | -0.95% | -8.85% | 16.5% |
| `union_vol_g_h3` | struck | 10 | 7 | no | no | no | no | -7.69% | -1.12% | -7.17% | -1.04% | -9.34% | 10.5% |
| `union_cond_h3` | struck | 10 | 7 | no | no | no | no | -7.73% | -1.14% | -6.95% | -1.02% | -8.41% | 7.4% |
| `union_last_red_h3` | struck | 10 | 7 | no | no | no | no | -7.78% | -1.15% | -6.88% | -1.01% | -6.05% | 12.4% |
| `ohlc_hot_coil_h1` | struck | 10 | 7 | no | no | no | no | -7.90% | -1.10% | -5.65% | -0.77% | -6.27% | 12.0% |
| `union_vol_g_h1` | struck | 10 | 7 | no | no | no | no | -8.33% | -1.22% | -6.97% | -1.01% | -10.72% | 7.0% |
| `union_cond_n4_h3` | struck | 10 | 7 | no | no | no | no | -8.34% | -1.23% | -7.94% | -1.17% | -9.31% | 5.2% |
| `union_earn_react_h1` | struck | 8 | 5 | no | no | no | no | -8.54% | -1.71% | -8.37% | -1.67% | -8.55% | 44.4% |
| `union_e_fresh_h3` | struck | 8 | 5 | no | no | no | no | -8.56% | -1.71% | -8.47% | -1.69% | -6.81% | 37.5% |
| `union_earn_react_h3` | struck | 8 | 5 | no | no | no | no | -8.56% | -1.71% | -8.47% | -1.69% | -6.81% | 37.5% |
| `union_e_fresh_h1` | struck | 8 | 5 | no | no | no | no | -8.60% | -1.72% | -8.43% | -1.69% | -8.60% | 40.0% |
| `short_extended_h1` | struck | 25 | 16 | no | no | yes | no | -12.25% | -0.77% | -5.26% | -0.30% | 9.06% | 38.8% |
| `yday_gainer_h3` | struck | 25 | 16 | no | no | no | no | -20.02% | -1.03% | -12.01% | -0.48% | -0.00% | 56.5% |
| `yday_gainer_h5` | struck | 25 | 16 | no | no | yes | no | -26.24% | -1.81% | -23.01% | -1.56% | 2.10% | 43.8% |
| `yday_gainer_h1` | struck | 25 | 16 | no | no | yes | no | -26.87% | -1.83% | -11.17% | -0.66% | 0.00% | 36.4% |
| `ohlc_hot_h3` | struck | 25 | 16 | no | no | no | no | -30.68% | -2.16% | -26.38% | -1.80% | -6.46% | 20.5% |
| `union_h5` | struck | 25 | 16 | no | no | no | no | -32.72% | -2.36% | -29.92% | -2.12% | -0.97% | 30.4% |
| `ohlc_hot_h5` | struck | 25 | 16 | no | no | no | no | -35.10% | -2.26% | -32.90% | -2.07% | -0.25% | 23.9% |
| `union_h3` | struck | 25 | 16 | no | no | no | no | -35.82% | -2.61% | -27.30% | -1.87% | -11.98% | 22.8% |
| `probable_h1` | struck | 25 | 16 | no | no | no | no | -36.60% | -2.68% | -22.03% | -1.45% | -1.13% | 33.2% |
| `union_h1` | struck | 25 | 16 | no | no | no | no | -39.12% | -2.96% | -25.46% | -1.74% | -17.15% | 17.6% |
| `ohlc_hot_h1` | struck | 25 | 16 | no | no | no | no | -41.75% | -3.24% | -31.79% | -2.30% | -5.15% | 18.2% |
| `short_last_red_h3` | struck | 25 | 16 | no | no | yes | no | -246.92% | -24.64% | -232.03% | -23.54% | 0.00% | 32.6% |
| `short_last_red_h1` | struck | 25 | 16 | no | no | yes | no | -431.45% | -26.97% | -407.15% | -25.45% | 0.00% | 32.6% |
