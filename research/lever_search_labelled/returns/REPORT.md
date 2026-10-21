# Group 3 labelled score

study: factor_mine_seq_labelled

Study label: `assumed pre-open, not server-proven`.

This is the labelled score of the 110 Factor Mine recipes. The preregistration was not edited. No locked ledger was rewritten. `rebuild_match` does not strike.

Price features are the frozen engine on `data/prices/ohlc.parquet`, blob `3456f7f489a6fa7033e8ae5cc942d8279f0113e3`. Feature bars are dated strictly before the session. Every recipe trades at the 09:30 open, so the session open is the fill and the gap. The session close is the mark only. The earliest panel row's open and close are not read.

The candidate list is the earliest `panel.json` blob that contains rows labelled that day. An earlier version exists for every session in the window, so the per-day source-file candidate list is not used. Actions, AB, and the Finviz export are the earliest blobs. Stock book, join, and catalyst are presence pins.

Sessions written: 31 (2026-08-13 through 2026-09-25).
Luck-test denominator: 9390. p is a one-sided t-test that the mean check-day return is above zero, multiplied by 9,390.

Line (a) cumulative Futubull check-day return >= 20%: 6
Line (b) best stock removed still positive: 31
Line (c) Futubull compound 2026-09-14 through 2026-09-25 >= 0: 32
Line (d) Futubull luck p < 0.05 on 9,390: 0
All four lines: 0
Proven (all four lines and at least 10 check days): 0

Verdict: `nothing proven yet`

## Top 10

| rank | recipe | side | label | check days | Futubull cum | Futubull mean | 15bp cum | 15bp mean | best removed | best stock | from 09-14 | under $3 | luck p |
| ---: | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- | ---: | ---: | ---: |
| 1 | `union_hot_score_h1` | long | not something good | 12 | 140.08% | 10.34% | 150.20% | 10.61% | 10.86% | REAX | 4.71% | 20.8% | 1 |
| 2 | `union_w_hot_cond_h1` | long | not something good | 12 | 138.89% | 10.51% | 145.28% | 10.64% | 13.02% | REAX | 12.31% | 14.4% | 1 |
| 3 | `ohlc_hot_h1` | long | not something good | 16 | 90.45% | 6.41% | 103.51% | 6.79% | -8.52% | REAX | -11.88% | 12.3% | 1 |
| 4 | `union_hot_n12_h1` | long | not something good | 12 | 87.88% | 6.85% | 97.47% | 7.23% | 5.73% | REAX | -2.77% | 19.1% | 1 |
| 5 | `ohlc_hot_h3` | long | not something good | 16 | 85.14% | 6.15% | 89.26% | 6.26% | -9.44% | REAX | -9.16% | 15.7% | 1 |
| 6 | `probable_probable_ok_h3` | long | not something good | 12 | 33.94% | 2.53% | 34.48% | 2.56% | 13.68% | BAND | -24.49% | 24.8% | 1 |
| 7 | `union_break10_h3` | long | not something good | 12 | 16.55% | 1.31% | 19.45% | 1.52% | 12.14% | IRD | -8.21% | 26.2% | 1 |
| 8 | `union_hot_score_h3` | long | not something good | 12 | 15.82% | 1.26% | 18.21% | 1.43% | 10.05% | IRD | 7.78% | 23.3% | 1 |
| 9 | `union_vol_ab_h1` | long | not something good | 12 | 14.10% | 1.15% | 19.51% | 1.54% | 8.82% | INDP | -11.40% | 13.5% | 1 |
| 10 | `union_hot_n4_h1` | long | not something good | 12 | 14.06% | 1.19% | 21.33% | 1.70% | 3.73% | INDP | 19.60% | 24.1% | 1 |

## Top 5 long

| rank | recipe | side | label | check days | Futubull cum | Futubull mean | 15bp cum | 15bp mean | best removed | best stock | from 09-14 | under $3 | luck p |
| ---: | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- | ---: | ---: | ---: |
| 1 | `union_hot_score_h1` | long | not something good | 12 | 140.08% | 10.34% | 150.20% | 10.61% | 10.86% | REAX | 4.71% | 20.8% | 1 |
| 2 | `union_w_hot_cond_h1` | long | not something good | 12 | 138.89% | 10.51% | 145.28% | 10.64% | 13.02% | REAX | 12.31% | 14.4% | 1 |
| 3 | `ohlc_hot_h1` | long | not something good | 16 | 90.45% | 6.41% | 103.51% | 6.79% | -8.52% | REAX | -11.88% | 12.3% | 1 |
| 4 | `union_hot_n12_h1` | long | not something good | 12 | 87.88% | 6.85% | 97.47% | 7.23% | 5.73% | REAX | -2.77% | 19.1% | 1 |
| 5 | `ohlc_hot_h3` | long | not something good | 16 | 85.14% | 6.15% | 89.26% | 6.26% | -9.44% | REAX | -9.16% | 15.7% | 1 |

## All 110

| recipe | side | label | search | check | a | b | c | d | Futubull cum | Futubull mean | 15bp cum | 15bp mean | best removed | best stock | from 09-14 | under $3 | luck p |
| --- | --- | --- | ---: | ---: | --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | --- | ---: | ---: | ---: |
| `union_hot_score_h1` | long | not something good | 17 | 12 | yes | yes | yes | no | 140.08% | 10.34% | 150.20% | 10.61% | 10.86% | REAX | 4.71% | 20.8% | 1 |
| `union_w_hot_cond_h1` | long | not something good | 17 | 12 | yes | yes | yes | no | 138.89% | 10.51% | 145.28% | 10.64% | 13.02% | REAX | 12.31% | 14.4% | 1 |
| `ohlc_hot_h1` | long | not something good | 21 | 16 | yes | no | no | no | 90.45% | 6.41% | 103.51% | 6.79% | -8.52% | REAX | -11.88% | 12.3% | 1 |
| `union_hot_n12_h1` | long | not something good | 17 | 12 | yes | yes | no | no | 87.88% | 6.85% | 97.47% | 7.23% | 5.73% | REAX | -2.77% | 19.1% | 1 |
| `ohlc_hot_h3` | long | not something good | 21 | 16 | yes | no | no | no | 85.14% | 6.15% | 89.26% | 6.26% | -9.44% | REAX | -9.16% | 15.7% | 1 |
| `probable_probable_ok_h3` | long | not something good | 17 | 12 | yes | yes | no | no | 33.94% | 2.53% | 34.48% | 2.56% | 13.68% | BAND | -24.49% | 24.8% | 1 |
| `union_break10_h3` | long | not something good | 17 | 12 | no | yes | no | no | 16.55% | 1.31% | 19.45% | 1.52% | 12.14% | IRD | -8.21% | 26.2% | 1 |
| `union_hot_score_h3` | long | not something good | 17 | 12 | no | yes | yes | no | 15.82% | 1.26% | 18.21% | 1.43% | 10.05% | IRD | 7.78% | 23.3% | 1 |
| `union_vol_ab_h1` | long | not something good | 13 | 12 | no | yes | no | no | 14.10% | 1.15% | 19.51% | 1.54% | 8.82% | INDP | -11.40% | 13.5% | 1 |
| `union_hot_n4_h1` | long | not something good | 17 | 12 | no | yes | yes | no | 14.06% | 1.19% | 21.33% | 1.70% | 3.73% | INDP | 19.60% | 24.1% | 1 |
| `union_w_hot_candle_h3` | long | not something good | 17 | 12 | no | yes | yes | no | 13.47% | 1.07% | 15.38% | 1.21% | 8.68% | BRR | 10.25% | 16.9% | 1 |
| `union_last_green_h3` | long | not something good | 17 | 12 | no | yes | no | no | 13.03% | 1.05% | 15.48% | 1.23% | 4.74% | CYPH | -6.90% | 21.5% | 1 |
| `union_vol_g_h1` | long | not something good | 17 | 12 | no | yes | no | no | 12.20% | 1.01% | 18.90% | 1.50% | 6.99% | INDP | -4.03% | 19.8% | 1 |
| `union_last_red_h1` | long | not something good | 17 | 12 | no | yes | no | no | 11.08% | 0.91% | 19.01% | 1.49% | 5.40% | GPRO | -8.90% | 17.5% | 1 |
| `union_vol_g_h3` | long | not something good | 17 | 12 | no | yes | no | no | 9.59% | 0.81% | 12.24% | 1.01% | 3.72% | INDP | -7.44% | 23.8% | 1 |
| `union_ret_5_h3` | long | not something good | 17 | 12 | no | yes | yes | no | 9.43% | 0.78% | 12.26% | 1.00% | 4.55% | IRD | 6.98% | 26.6% | 1 |
| `union_w_hot_candle_h1` | long | not something good | 17 | 12 | no | yes | yes | no | 9.15% | 0.77% | 15.25% | 1.22% | 4.11% | INDP | 8.38% | 15.6% | 1 |
| `union_break10_h1` | long | not something good | 17 | 12 | no | yes | no | no | 8.92% | 0.76% | 16.20% | 1.30% | 5.77% | GPRO | -8.59% | 18.4% | 1 |
| `union_vol_g_h5` | long | not something good | 17 | 12 | no | yes | yes | no | 8.40% | 0.70% | 9.50% | 0.78% | 2.14% | NVDA | 4.46% | 29.3% | 1 |
| `union_ret_5_h1` | long | not something good | 17 | 12 | no | yes | yes | no | 8.33% | 0.72% | 15.81% | 1.28% | 3.29% | INDP | 6.67% | 20.6% | 1 |
| `yday_gainer_h1` | long | not something good | 21 | 16 | no | yes | no | no | 8.08% | 0.52% | 19.87% | 1.17% | 3.83% | CYPH | -10.37% | 24.5% | 1 |
| `short_news_r_h3` | short | not something good | 21 | 16 | no | yes | no | no | 6.93% | 0.43% | 7.25% | 0.45% | 1.38% | FIG | -0.70% | 0.0% | 1 |
| `union_join_vol_green_h1` | long | not something good | 17 | 12 | no | yes | yes | no | 6.92% | 0.59% | 12.16% | 0.99% | 1.13% | CYPH | 2.41% | 12.0% | 1 |
| `union_vol_ab_h3` | long | not something good | 13 | 12 | no | yes | no | no | 6.46% | 0.55% | 8.27% | 0.69% | 3.93% | BAND | -7.54% | 17.7% | 1 |
| `union_news_present_h1` | long | not something good | 17 | 12 | no | yes | no | no | 5.95% | 0.49% | 6.37% | 0.53% | 2.53% | QRVO | -3.46% | 0.0% | 1 |
| `union_h3_exit_red` | long | not something good | 17 | 12 | no | yes | no | no | 5.72% | 0.48% | 9.09% | 0.75% | 1.76% | CYPH | -7.59% | 18.1% | 1 |
| `union_news_vol_h1` | long | not something good | 17 | 12 | no | yes | no | no | 5.37% | 0.44% | 5.50% | 0.45% | 3.09% | AVGO | -0.21% | 0.0% | 1 |
| `union_candle_h1` | long | not something good | 17 | 12 | no | yes | no | no | 5.36% | 0.46% | 10.81% | 0.88% | 2.79% | IRD | -11.54% | 12.1% | 1 |
| `union_ab_g_h1` | long | not something good | 13 | 12 | no | yes | no | no | 4.77% | 0.41% | 10.33% | 0.85% | 2.65% | CYPH | -11.99% | 13.3% | 1 |
| `union_news_vol_h3` | long | not something good | 17 | 12 | no | yes | no | no | 4.73% | 0.40% | 4.81% | 0.40% | 0.72% | AVGO | -3.97% | 0.0% | 1 |
| `union_candle_h3` | long | not something good | 17 | 12 | no | yes | no | no | 4.18% | 0.36% | 6.20% | 0.52% | 1.16% | XHLD | -10.73% | 15.4% | 1 |
| `union_blue_vol_h3` | long | not something good | 17 | 12 | no | no | no | no | 4.13% | 0.38% | 5.78% | 0.50% | -5.20% | DPRO | -9.54% | 22.6% | 1 |
| `union_news_present_h3` | long | not something good | 17 | 12 | no | no | no | no | 4.04% | 0.34% | 4.53% | 0.38% | -1.05% | NVDA | -4.40% | 0.0% | 1 |
| `union_catal_present_h1` | long | not something good | 8 | 8 | no | no | yes | no | 4.00% | 0.53% | 4.31% | 0.57% | -1.13% | FMC | 0.15% | 0.0% | 1 |
| `union_news_g_h3` | long | not something good | 17 | 12 | no | no | no | no | 3.68% | 0.31% | 4.08% | 0.34% | -1.40% | NVDA | -4.41% | 0.0% | 1 |
| `union_catal_present_h3` | long | not something good | 8 | 8 | no | no | no | no | 3.58% | 0.48% | 3.76% | 0.50% | -0.30% | FMC | -6.88% | 0.0% | 1 |
| `union_blue_vol_h1` | long | not something good | 17 | 12 | no | no | no | no | 3.00% | 0.29% | 11.51% | 0.95% | -2.77% | GPRO | -9.09% | 18.9% | 1 |
| `union_news_missing_h1` | long | not something good | 17 | 12 | no | yes | no | no | 2.62% | 0.25% | 9.08% | 0.75% | 0.12% | USDE | -16.20% | 15.1% | 1 |
| `union_white_coil_h3` | long | not something good | 17 | 12 | no | no | no | no | 2.45% | 0.21% | 3.30% | 0.28% | -0.26% | BAND | -16.17% | 12.5% | 1 |
| `union_cond_h1` | long | not something good | 17 | 12 | no | no | no | no | 2.26% | 0.22% | 6.70% | 0.57% | -0.27% | ARCT | -13.99% | 8.4% | 1 |
| `union_white_h1` | long | not something good | 17 | 12 | no | yes | no | no | 2.09% | 0.19% | 4.36% | 0.37% | 0.44% | ARCT | -11.65% | 12.5% | 1 |
| `union_w_hot_cond_h3` | long | not something good | 17 | 12 | no | no | yes | no | 2.03% | 0.20% | 3.92% | 0.35% | -0.78% | ARCT | 10.31% | 14.8% | 1 |
| `union_h3` | long | not something good | 21 | 16 | no | no | yes | no | 1.65% | 0.13% | 4.10% | 0.28% | -4.32% | CYPH | 6.20% | 17.3% | 1 |
| `union_join_present_h1` | long | not something good | 17 | 12 | no | no | no | no | 1.58% | 0.16% | 8.00% | 0.67% | -0.90% | USDE | -16.91% | 14.1% | 1 |
| `short_news_r_h1` | short | not something good | 21 | 16 | no | no | yes | no | 1.53% | 0.10% | 1.91% | 0.12% | -2.58% | FIG | 1.10% | 0.0% | 1 |
| `probable_probable_ok_h1` | long | not something good | 17 | 12 | no | no | no | no | 1.23% | 0.15% | 6.90% | 0.60% | -7.31% | BAND | -9.05% | 25.0% | 1 |
| `union_candle_score_h1` | long | not something good | 17 | 12 | no | no | no | no | 1.19% | 0.12% | 5.89% | 0.50% | -2.36% | INDP | -6.11% | 7.7% | 1 |
| `union_news_g_h1` | long | not something good | 17 | 12 | no | no | no | no | 1.06% | 0.09% | 1.38% | 0.12% | -1.14% | AVGO | -1.33% | 0.0% | 1 |
| `union_white_coil_h1` | long | not something good | 17 | 12 | no | no | no | no | 0.97% | 0.09% | 2.73% | 0.24% | -1.69% | BAND | -3.72% | 8.5% | 1 |
| `union_white_h3` | long | not something good | 17 | 12 | no | no | no | no | 0.74% | 0.07% | 1.64% | 0.14% | -0.51% | KGC | -14.24% | 17.0% | 1 |
| `union_h5_exit_alarm` | long | not something good | 17 | 12 | no | no | no | no | 0.54% | 0.06% | 2.40% | 0.21% | -1.00% | GGB | -4.30% | 19.0% | 1 |
| `union_e_green_h1` | long | not something good | 17 | 12 | no | no | no | no | 0.38% | 0.13% | 5.33% | 0.52% | -4.68% | DBI | -4.74% | 16.7% | 1 |
| `union_last_green_h1` | long | not something good | 17 | 12 | no | no | no | no | 0.32% | 0.05% | 6.32% | 0.54% | -1.31% | ARCT | -14.44% | 16.4% | 1 |
| `short_r_down_h1` | short | not something good | 20 | 15 | no | no | yes | no | 0.00% | 0.00% | 0.00% | 0.00% |  |  | 0.00% |  | 1 |
| `short_r_down_h3` | short | not something good | 20 | 15 | no | no | yes | no | 0.00% | 0.00% | 0.00% | 0.00% |  |  | 0.00% |  | 1 |
| `union_r_up_h1` | long | not something good | 17 | 12 | no | no | yes | no | 0.00% | 0.00% | 0.00% | 0.00% |  |  | 0.00% |  | 1 |
| `union_r_up_h3` | long | not something good | 17 | 12 | no | no | yes | no | 0.00% | 0.00% | 0.00% | 0.00% |  |  | 0.00% |  | 1 |
| `flatten_h5` | long | not something good | 17 | 12 | no | no | no | no | -0.16% | 0.00% | 1.52% | 0.14% | -2.40% | MOS | -1.28% | 21.2% | 1 |
| `probable_h1` | long | not something good | 21 | 16 | no | no | no | no | -0.36% | 0.00% | 10.61% | 0.65% | -2.76% | IRD | -9.40% | 22.3% | 1 |
| `short_alarm_h1` | short | not something good | 17 | 12 | no | no | no | no | -0.42% | -0.03% | 2.42% | 0.20% | -1.44% | SAIC | -3.51% | 19.4% | 1 |
| `union_vol_green_h3` | long | not something good | 17 | 12 | no | no | yes | no | -0.45% | -0.01% | 1.60% | 0.16% | -5.75% | INDP | 9.81% | 24.2% | 1 |
| `union_e_fresh_h1` | long | not something good | 17 | 12 | no | no | no | no | -0.48% | 0.03% | 6.95% | 0.62% | -4.53% | CHPT | -5.49% | 9.0% | 1 |
| `union_join_g_h1` | long | not something good | 17 | 12 | no | no | no | no | -0.85% | -0.05% | 4.84% | 0.42% | -2.71% | CYPH | -15.14% | 13.0% | 1 |
| `union_coil_green_h3` | long | not something good | 17 | 12 | no | no | no | no | -0.88% | -0.05% | 1.35% | 0.13% | -7.51% | CYPH | -4.29% | 16.7% | 1 |
| `flatten_h1` | long | not something good | 17 | 12 | no | no | no | no | -0.90% | -0.06% | 3.92% | 0.34% | -3.39% | ARCT | -18.16% | 9.8% | 1 |
| `yday_gainer_h5` | long | not something good | 21 | 16 | no | no | yes | no | -0.93% | -0.02% | 1.66% | 0.14% | -5.36% | ARCT | 8.95% | 32.7% | 1 |
| `probable_h3` | long | not something good | 21 | 16 | no | no | no | no | -0.95% | -0.03% | 3.99% | 0.27% | -2.84% | CRML | -8.90% | 24.9% | 1 |
| `yday_gainer_h3` | long | not something good | 21 | 16 | no | no | no | no | -0.97% | -0.03% | 3.75% | 0.26% | -2.92% | CRML | -10.44% | 34.1% | 1 |
| `union_vol_missing_h1` | long | not something good | 17 | 12 | no | no | yes | no | -1.03% | -0.09% | -0.57% | -0.05% | 0.00% | SGLD | 0.00% | 0.0% | 1 |
| `union_ab_g_h3` | long | not something good | 13 | 12 | no | no | no | no | -1.09% | -0.06% | 1.13% | 0.12% | -3.53% | BAND | -10.07% | 19.2% | 1 |
| `union_blue_h1` | long | not something good | 17 | 12 | no | no | no | no | -1.10% | -0.07% | 7.01% | 0.59% | -2.71% | ARCT | -8.19% | 15.8% | 1 |
| `union_h1` | long | not something good | 21 | 16 | no | no | no | no | -1.24% | -0.05% | 8.20% | 0.52% | -5.57% | CYPH | -15.82% | 13.0% | 1 |
| `union_news_g_h5` | long | not something good | 17 | 12 | no | no | no | no | -1.29% | -0.10% | -0.89% | -0.07% | -3.89% | AVGO | -2.35% | 0.0% | 1 |
| `union_cond_n4_h3` | long | not something good | 17 | 12 | no | no | no | no | -1.33% | -0.08% | 0.12% | 0.04% | -6.19% | ARCT | -0.63% | 11.3% | 1 |
| `union_vol_green_h1` | long | not something good | 17 | 12 | no | no | no | no | -1.57% | -0.10% | 4.87% | 0.43% | -6.14% | INDP | -3.60% | 19.7% | 1 |
| `flatten_h3` | long | not something good | 17 | 12 | no | no | no | no | -1.77% | -0.12% | 0.74% | 0.09% | -6.10% | CYPH | -2.37% | 18.9% | 1 |
| `union_join_vol_green_h3` | long | not something good | 17 | 12 | no | no | yes | no | -1.83% | -0.13% | -0.15% | 0.01% | -7.04% | INDP | 1.43% | 17.2% | 1 |
| `union_last_red_h3` | long | not something good | 17 | 12 | no | no | yes | no | -2.02% | -0.14% | 1.59% | 0.16% | -6.97% | INDP | 5.24% | 24.8% | 1 |
| `short_alarm_h3` | short | not something good | 17 | 12 | no | no | no | no | -2.05% | -0.16% | 0.59% | 0.06% | -3.05% | EYPT | -10.94% | 18.6% | 1 |
| `union_candle_score_h3` | long | not something good | 17 | 12 | no | no | no | no | -2.35% | -0.17% | -0.35% | -0.01% | -5.00% | XHLD | -3.84% | 8.1% | 1 |
| `union_news_missing_h3` | long | not something good | 17 | 12 | no | no | no | no | -2.56% | -0.18% | 0.13% | 0.05% | -6.88% | CYPH | -3.09% | 24.0% | 1 |
| `union_earn_react_h1` | long | not something good | 17 | 12 | no | no | no | no | -2.56% | -0.14% | 4.29% | 0.42% | -6.53% | CHPT | -5.55% | 8.5% | 1 |
| `union_h3_exit_news_r` | long | not something good | 17 | 12 | no | no | no | no | -3.27% | -0.23% | -0.32% | 0.02% | -7.51% | CYPH | -3.36% | 20.2% | 1 |
| `union_h5` | long | not something good | 21 | 16 | no | no | yes | no | -3.47% | -0.20% | -1.47% | -0.07% | -4.76% | CMRC | 0.45% | 22.5% | 1 |
| `union_join_present_h3` | long | not something good | 17 | 12 | no | no | no | no | -3.93% | -0.29% | -1.00% | -0.04% | -8.16% | CYPH | -3.47% | 20.1% | 1 |
| `union_h3_exit_alarm` | long | not something good | 17 | 12 | no | no | no | no | -4.24% | -0.33% | -0.67% | -0.03% | -7.79% | CYPH | -7.82% | 16.1% | 1 |
| `ohlc_hot_h5` | long | not something good | 21 | 16 | no | no | yes | no | -4.33% | -0.25% | -2.37% | -0.12% | -5.79% | MRNA | 4.88% | 19.0% | 1 |
| `union_last_green_h5` | long | not something good | 17 | 12 | no | no | no | no | -4.77% | -0.39% | -2.52% | -0.19% | -6.24% | GGB | -1.09% | 23.1% | 1 |
| `union_coil_off_h5` | long | not something good | 17 | 12 | no | no | no | no | -4.92% | -0.40% | -3.25% | -0.26% | -6.11% | KGC | -13.34% | 22.1% | 1 |
| `union_white_h5` | long | not something good | 17 | 12 | no | no | no | no | -5.34% | -0.44% | -3.91% | -0.31% | -6.48% | KGC | -1.20% | 18.2% | 1 |
| `union_blue_coil_h1` | long | not something good | 17 | 12 | no | no | no | no | -5.68% | -0.47% | 2.37% | 0.21% | -6.99% | CTMX | -13.95% | 18.2% | 1 |
| `union_coil_green_h1` | long | not something good | 17 | 12 | no | no | no | no | -6.03% | -0.50% | -0.81% | -0.05% | -7.45% | YEXT | -8.05% | 12.0% | 1 |
| `union_cond_h3` | long | not something good | 17 | 12 | no | no | no | no | -6.46% | -0.53% | -4.33% | -0.35% | -8.74% | ARCT | -4.96% | 11.5% | 1 |
| `union_e_fresh_h3` | long | not something good | 17 | 12 | no | no | yes | no | -7.40% | -0.61% | -5.70% | -0.46% | -9.09% | NVDA | 2.15% | 13.2% | 1 |
| `union_coil_off_h1` | long | not something good | 17 | 12 | no | no | no | no | -7.45% | -0.64% | -1.80% | -0.14% | -8.38% | KGC | -8.83% | 12.1% | 1 |
| `union_blue_h3` | long | not something good | 17 | 12 | no | no | yes | no | -7.86% | -0.66% | -5.09% | -0.42% | -9.53% | BAND | 3.39% | 17.5% | 1 |
| `ohlc_hot_coil_h1` | long | not something good | 17 | 12 | no | no | no | no | -8.14% | -0.69% | -2.83% | -0.22% | -9.58% | AUGO | -9.91% | 11.1% | 1 |
| `union_coil_off_h3` | long | not something good | 17 | 12 | no | no | no | no | -8.37% | -0.70% | -6.05% | -0.50% | -11.66% | CYPH | -5.39% | 15.9% | 1 |
| `union_earn_react_h3` | long | not something good | 17 | 12 | no | no | yes | no | -8.39% | -0.70% | -6.68% | -0.55% | -11.00% | SHOE | 2.45% | 14.2% | 1 |
| `coil_h3_exit_alarm` | long | not something good | 17 | 12 | no | no | no | no | -8.54% | -0.72% | -6.05% | -0.49% | -11.89% | CYPH | -7.98% | 16.0% | 1 |
| `union_join_g_h3` | long | not something good | 17 | 12 | no | no | yes | no | -9.53% | -0.80% | -6.81% | -0.56% | -11.74% | ARCT | 0.48% | 18.6% | 1 |
| `union_blue_coil_h3` | long | not something good | 17 | 12 | no | no | no | no | -9.80% | -0.84% | -7.03% | -0.60% | -11.36% | BAND | -2.77% | 19.3% | 1 |
| `probable_h5` | long | not something good | 21 | 16 | no | no | yes | no | -12.07% | -0.78% | -9.58% | -0.61% | -14.37% | GORO | 7.84% | 26.9% | 1 |
| `union_vol_missing_h3` | long | not something good | 17 | 12 | no | no | yes | no | -13.20% | -1.14% | -12.74% | -1.10% | 0.00% | SGLD | 0.00% | 0.0% | 1 |
| `short_last_red_h3` | short | not something good | 21 | 16 | no | no | no | no | -13.92% | -0.89% | -6.49% | -0.38% | -14.81% | CABA | -2.29% | 17.9% | 1 |
| `short_last_red_h1` | short | not something good | 21 | 16 | no | no | yes | no | -16.61% | -1.12% | -8.96% | -0.58% | -17.23% | CABA | 0.22% | 18.5% | 1 |
| `short_extended_h1` | short | not something good | 21 | 16 | no | no | no | no | -18.47% | -1.25% | -11.21% | -0.72% | -19.07% | NEOV | -5.53% | 24.4% | 1 |
| `short_extended_h3` | short | not something good | 21 | 16 | no | no | no | no | -18.61% | -1.20% | -12.08% | -0.73% | -20.86% | USDE | -7.58% | 22.5% | 1 |
| `flatten_vol_g_h3` | long | not something good | 17 | 12 | no | no | no | no | -21.43% | -1.96% | -20.64% | -1.88% | -21.48% | ATRC | -13.14% | 18.6% | 1 |
| `union_e_green_h3` | long | not something good | 17 | 12 | no | no | no | no | -24.86% | -2.18% | -21.23% | -1.80% | -25.45% | BJ | -3.41% | 21.0% | 1 |
