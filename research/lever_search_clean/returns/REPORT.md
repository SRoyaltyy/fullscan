# Group 3 clean-tape score

study: factor_mine_seq_clean_tape

Study label: `assumed pre-open, clean tape (split-artefact names absent or corrected)`.

This is the clean-tape re-score of the 110 Factor Mine recipes. The labelled study's preregistration was not edited, and this study's preregistration is the fingerprinted protocol for this run. No locked ledger was rewritten. `rebuild_match` does not strike.

The only changed input is the price tape: `research/breadth_rank_v1c/bars/ohlc.parquet`, blob `e7bbac335cfa87243f9d0ddf8c331a0739dd0df8`. Split artefacts are repaired, and unrepairable names are absent. Feature bars are dated strictly before the session. Every recipe trades at the 09:30 open, so the session open is the fill and the gap. The session close is the mark only. The earliest panel row's open and close are not read.

The candidate list is the earliest `panel.json` blob that contains rows labelled that day. Actions, AB, and the Finviz export are the earliest blobs, the same pins as the labelled study. Stock book, join, and catalyst are presence pins.

Sessions written: 31 (2026-08-13 through 2026-09-25).
Luck-test denominator: 9500. p is a one-sided t-test that the mean check-day return is above zero, multiplied by 9,500.

Line (a) cumulative Futubull check-day return >= 20%: 2
Line (b) best stock removed still positive: 29
Line (c) Futubull compound 2026-09-14 through 2026-09-25 >= 0: 31
Line (d) Futubull luck p < 0.05 on 9,500: 0
All four lines: 0
Proven (all four lines and at least 10 check days): 0

Verdict: `nothing proven yet`

## Top 10

| rank | recipe | side | label | check days | Futubull cum | Futubull mean | 15bp cum | 15bp mean | best removed | best stock | from 09-14 | under $3 | luck p |
| ---: | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- | ---: | ---: | ---: |
| 1 | `probable_probable_ok_h3` | long | not something good | 12 | 33.80% | 2.52% | 34.34% | 2.55% | 13.54% | BAND | -24.49% | 24.8% | 1 |
| 2 | `union_hot_n4_h1` | long | not something good | 12 | 29.20% | 2.34% | 37.34% | 2.84% | 17.50% | INDP | 19.88% | 25.9% | 1 |
| 3 | `union_vol_g_h3` | long | not something good | 12 | 18.81% | 1.49% | 21.36% | 1.66% | 12.87% | IRD | -7.49% | 23.8% | 1 |
| 4 | `union_vol_ab_h1` | long | not something good | 12 | 15.49% | 1.25% | 20.84% | 1.63% | 10.15% | INDP | -10.88% | 12.8% | 1 |
| 5 | `union_hot_score_h1` | long | not something good | 12 | 14.55% | 1.21% | 21.95% | 1.73% | 9.24% | INDP | 5.41% | 21.0% | 1 |
| 6 | `union_blue_vol_h3` | long | not something good | 12 | 13.98% | 1.12% | 15.23% | 1.21% | 3.74% | DPRO | -10.63% | 25.0% | 1 |
| 7 | `union_vol_g_h1` | long | not something good | 12 | 13.92% | 1.14% | 20.65% | 1.62% | 8.64% | INDP | -3.98% | 19.5% | 1 |
| 8 | `union_break10_h1` | long | not something good | 12 | 13.01% | 1.06% | 20.27% | 1.59% | 7.72% | INDP | -9.15% | 18.4% | 1 |
| 9 | `union_w_hot_cond_h1` | long | not something good | 12 | 12.45% | 1.04% | 18.29% | 1.46% | 7.29% | INDP | 5.83% | 14.6% | 1 |
| 10 | `union_w_hot_candle_h1` | long | not something good | 12 | 11.55% | 0.95% | 18.09% | 1.43% | 7.41% | INDP | 11.92% | 16.6% | 1 |

## Top 5 long

| rank | recipe | side | label | check days | Futubull cum | Futubull mean | 15bp cum | 15bp mean | best removed | best stock | from 09-14 | under $3 | luck p |
| ---: | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- | ---: | ---: | ---: |
| 1 | `probable_probable_ok_h3` | long | not something good | 12 | 33.80% | 2.52% | 34.34% | 2.55% | 13.54% | BAND | -24.49% | 24.8% | 1 |
| 2 | `union_hot_n4_h1` | long | not something good | 12 | 29.20% | 2.34% | 37.34% | 2.84% | 17.50% | INDP | 19.88% | 25.9% | 1 |
| 3 | `union_vol_g_h3` | long | not something good | 12 | 18.81% | 1.49% | 21.36% | 1.66% | 12.87% | IRD | -7.49% | 23.8% | 1 |
| 4 | `union_vol_ab_h1` | long | not something good | 12 | 15.49% | 1.25% | 20.84% | 1.63% | 10.15% | INDP | -10.88% | 12.8% | 1 |
| 5 | `union_hot_score_h1` | long | not something good | 12 | 14.55% | 1.21% | 21.95% | 1.73% | 9.24% | INDP | 5.41% | 21.0% | 1 |

## All 110

| recipe | side | label | search | check | a | b | c | d | Futubull cum | Futubull mean | 15bp cum | 15bp mean | best removed | best stock | from 09-14 | under $3 | luck p |
| --- | --- | --- | ---: | ---: | --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | --- | ---: | ---: | ---: |
| `probable_probable_ok_h3` | long | not something good | 17 | 12 | yes | yes | no | no | 33.80% | 2.52% | 34.34% | 2.55% | 13.54% | BAND | -24.49% | 24.8% | 1 |
| `union_hot_n4_h1` | long | not something good | 17 | 12 | yes | yes | yes | no | 29.20% | 2.34% | 37.34% | 2.84% | 17.50% | INDP | 19.88% | 25.9% | 1 |
| `union_vol_g_h3` | long | not something good | 17 | 12 | no | yes | no | no | 18.81% | 1.49% | 21.36% | 1.66% | 12.87% | IRD | -7.49% | 23.8% | 1 |
| `union_vol_ab_h1` | long | not something good | 13 | 12 | no | yes | no | no | 15.49% | 1.25% | 20.84% | 1.63% | 10.15% | INDP | -10.88% | 12.8% | 1 |
| `union_hot_score_h1` | long | not something good | 17 | 12 | no | yes | yes | no | 14.55% | 1.21% | 21.95% | 1.73% | 9.24% | INDP | 5.41% | 21.0% | 1 |
| `union_blue_vol_h3` | long | not something good | 17 | 12 | no | yes | no | no | 13.98% | 1.12% | 15.23% | 1.21% | 3.74% | DPRO | -10.63% | 25.0% | 1 |
| `union_vol_g_h1` | long | not something good | 17 | 12 | no | yes | no | no | 13.92% | 1.14% | 20.65% | 1.62% | 8.64% | INDP | -3.98% | 19.5% | 1 |
| `union_break10_h1` | long | not something good | 17 | 12 | no | yes | no | no | 13.01% | 1.06% | 20.27% | 1.59% | 7.72% | INDP | -9.15% | 18.4% | 1 |
| `union_w_hot_cond_h1` | long | not something good | 17 | 12 | no | yes | yes | no | 12.45% | 1.04% | 18.29% | 1.46% | 7.29% | INDP | 5.83% | 14.6% | 1 |
| `union_w_hot_candle_h1` | long | not something good | 17 | 12 | no | yes | yes | no | 11.55% | 0.95% | 18.09% | 1.43% | 7.41% | INDP | 11.92% | 16.6% | 1 |
| `union_hot_score_h3` | long | not something good | 17 | 12 | no | yes | yes | no | 11.40% | 0.95% | 13.83% | 1.13% | 5.57% | IRD | 9.26% | 25.6% | 1 |
| `union_break10_h3` | long | not something good | 17 | 12 | no | yes | no | no | 11.33% | 0.93% | 14.21% | 1.14% | 6.98% | IRD | -4.18% | 25.0% | 1 |
| `union_ret_5_h1` | long | not something good | 17 | 12 | no | yes | yes | no | 11.31% | 0.95% | 18.90% | 1.50% | 7.15% | INDP | 7.02% | 20.9% | 1 |
| `union_ret_5_h3` | long | not something good | 17 | 12 | no | yes | yes | no | 11.24% | 0.92% | 14.36% | 1.15% | 6.25% | IRD | 9.18% | 24.8% | 1 |
| `union_last_red_h1` | long | not something good | 17 | 12 | no | yes | no | no | 11.08% | 0.91% | 19.09% | 1.49% | 5.40% | GPRO | -8.48% | 17.8% | 1 |
| `union_vol_g_h5` | long | not something good | 17 | 12 | no | yes | yes | no | 9.14% | 0.75% | 10.31% | 0.84% | 2.94% | NVDA | 4.37% | 27.1% | 1 |
| `union_join_vol_green_h1` | long | not something good | 17 | 12 | no | yes | yes | no | 8.58% | 0.72% | 13.70% | 1.11% | 2.71% | CYPH | 2.45% | 10.9% | 1 |
| `union_last_green_h3` | long | not something good | 17 | 12 | no | yes | no | no | 8.24% | 0.69% | 10.60% | 0.87% | 0.29% | CYPH | -2.64% | 20.4% | 1 |
| `short_news_r_h3` | short | not something good | 21 | 16 | no | yes | no | no | 6.93% | 0.43% | 7.26% | 0.45% | 1.38% | FIG | -0.70% | 0.0% | 1 |
| `union_hot_n12_h1` | long | not something good | 17 | 12 | no | yes | no | no | 6.46% | 0.56% | 15.09% | 1.21% | 3.11% | INDP | -2.84% | 19.7% | 1 |
| `union_vol_ab_h3` | long | not something good | 13 | 12 | no | yes | no | no | 6.42% | 0.55% | 8.18% | 0.69% | 3.92% | BAND | -8.64% | 16.6% | 1 |
| `union_news_present_h1` | long | not something good | 17 | 12 | no | yes | no | no | 5.95% | 0.49% | 6.37% | 0.53% | 2.53% | QRVO | -3.46% | 0.0% | 1 |
| `yday_gainer_h1` | long | not something good | 21 | 16 | no | yes | no | no | 5.54% | 0.37% | 17.19% | 1.02% | 1.39% | CYPH | -10.93% | 25.2% | 1 |
| `union_candle_h1` | long | not something good | 17 | 12 | no | yes | no | no | 5.40% | 0.46% | 10.54% | 0.86% | 2.83% | IRD | -11.56% | 11.1% | 1 |
| `union_news_vol_h1` | long | not something good | 17 | 12 | no | yes | no | no | 5.37% | 0.44% | 5.50% | 0.45% | 3.09% | AVGO | -0.21% | 0.0% | 1 |
| `union_ab_g_h1` | long | not something good | 13 | 12 | no | yes | no | no | 4.77% | 0.41% | 10.33% | 0.85% | 2.65% | CYPH | -11.86% | 13.3% | 1 |
| `union_news_vol_h3` | long | not something good | 17 | 12 | no | yes | no | no | 4.73% | 0.40% | 4.81% | 0.40% | 0.72% | AVGO | -3.97% | 0.0% | 1 |
| `union_blue_vol_h1` | long | not something good | 17 | 12 | no | no | no | no | 4.36% | 0.40% | 12.89% | 1.05% | -1.47% | GPRO | -9.23% | 17.9% | 1 |
| `union_news_present_h3` | long | not something good | 17 | 12 | no | no | no | no | 4.04% | 0.34% | 4.52% | 0.38% | -1.05% | NVDA | -4.40% | 0.0% | 1 |
| `union_catal_present_h1` | long | not something good | 8 | 8 | no | no | yes | no | 4.00% | 0.53% | 4.31% | 0.57% | -1.13% | FMC | 0.15% | 0.0% | 1 |
| `union_news_g_h3` | long | not something good | 17 | 12 | no | no | no | no | 3.68% | 0.31% | 4.08% | 0.34% | -1.40% | NVDA | -4.41% | 0.0% | 1 |
| `union_catal_present_h3` | long | not something good | 8 | 8 | no | no | no | no | 3.58% | 0.48% | 3.76% | 0.50% | -0.30% | FMC | -6.88% | 0.0% | 1 |
| `union_candle_score_h1` | long | not something good | 17 | 12 | no | no | no | no | 3.17% | 0.28% | 7.83% | 0.65% | -0.51% | INDP | -6.19% | 8.2% | 1 |
| `union_news_missing_h1` | long | not something good | 17 | 12 | no | yes | no | no | 2.52% | 0.24% | 8.96% | 0.74% | 0.01% | USDE | -16.07% | 15.2% | 1 |
| `union_cond_h1` | long | not something good | 17 | 12 | no | no | no | no | 2.22% | 0.22% | 6.61% | 0.57% | -0.30% | ARCT | -13.99% | 8.4% | 1 |
| `union_white_h1` | long | not something good | 17 | 12 | no | yes | no | no | 2.10% | 0.19% | 4.37% | 0.37% | 0.45% | ARCT | -11.53% | 12.5% | 1 |
| `union_h3_exit_red` | long | not something good | 17 | 12 | no | no | no | no | 1.85% | 0.18% | 5.18% | 0.44% | -1.96% | CYPH | -4.41% | 17.1% | 1 |
| `short_news_r_h1` | short | not something good | 21 | 16 | no | no | yes | no | 1.66% | 0.11% | 2.03% | 0.13% | -2.46% | FIG | 1.10% | 0.0% | 1 |
| `union_h3` | long | not something good | 21 | 16 | no | no | yes | no | 1.57% | 0.12% | 3.98% | 0.27% | -4.41% | CYPH | 6.98% | 18.1% | 1 |
| `union_join_present_h1` | long | not something good | 17 | 12 | no | no | no | no | 1.48% | 0.15% | 7.89% | 0.66% | -1.00% | USDE | -16.89% | 14.1% | 1 |
| `union_w_hot_cond_h3` | long | not something good | 17 | 12 | no | no | yes | no | 1.40% | 0.16% | 3.23% | 0.31% | -1.87% | ARCT | 9.37% | 15.6% | 1 |
| `probable_probable_ok_h1` | long | not something good | 17 | 12 | no | no | no | no | 1.11% | 0.14% | 6.77% | 0.59% | -7.43% | BAND | -7.90% | 25.0% | 1 |
| `union_white_coil_h3` | long | not something good | 17 | 12 | no | no | no | no | 1.08% | 0.10% | 1.91% | 0.17% | -1.60% | BAND | -16.07% | 12.8% | 1 |
| `union_news_g_h1` | long | not something good | 17 | 12 | no | no | no | no | 1.06% | 0.09% | 1.38% | 0.12% | -1.14% | AVGO | -1.33% | 0.0% | 1 |
| `union_w_hot_candle_h3` | long | not something good | 17 | 12 | no | no | yes | no | 0.98% | 0.11% | 2.91% | 0.27% | -1.85% | ARCT | 17.82% | 18.5% | 1 |
| `union_last_green_h1` | long | not something good | 17 | 12 | no | no | no | no | 0.86% | 0.10% | 6.73% | 0.57% | -0.76% | ARCT | -14.38% | 15.6% | 1 |
| `union_white_h3` | long | not something good | 17 | 12 | no | no | no | no | 0.74% | 0.07% | 1.64% | 0.14% | -0.51% | KGC | -14.13% | 17.0% | 1 |
| `union_vol_green_h1` | long | not something good | 17 | 12 | no | no | no | no | 0.67% | 0.09% | 6.94% | 0.59% | -4.01% | INDP | -4.56% | 18.3% | 1 |
| `union_h5_exit_alarm` | long | not something good | 17 | 12 | no | no | no | no | 0.53% | 0.06% | 2.39% | 0.21% | -1.01% | GGB | -6.17% | 19.1% | 1 |
| `union_white_coil_h1` | long | not something good | 17 | 12 | no | no | no | no | 0.40% | 0.05% | 2.16% | 0.19% | -2.26% | BAND | -3.66% | 8.5% | 1 |
| `short_alarm_h1` | short | not something good | 17 | 12 | no | no | no | no | 0.28% | 0.03% | 2.75% | 0.23% | -0.74% | SAIC | -5.25% | 19.3% | 1 |
| `short_r_down_h1` | short | not something good | 20 | 15 | no | no | yes | no | 0.00% | 0.00% | 0.00% | 0.00% |  |  | 0.00% |  | 1 |
| `short_r_down_h3` | short | not something good | 20 | 15 | no | no | yes | no | 0.00% | 0.00% | 0.00% | 0.00% |  |  | 0.00% |  | 1 |
| `union_r_up_h1` | long | not something good | 17 | 12 | no | no | yes | no | 0.00% | 0.00% | 0.00% | 0.00% |  |  | 0.00% |  | 1 |
| `union_r_up_h3` | long | not something good | 17 | 12 | no | no | yes | no | 0.00% | 0.00% | 0.00% | 0.00% |  |  | 0.00% |  | 1 |
| `flatten_h5` | long | not something good | 17 | 12 | no | no | no | no | -0.15% | 0.00% | 1.53% | 0.14% | -2.38% | MOS | -1.06% | 20.9% | 1 |
| `union_vol_missing_h1` | long | not something good | 17 | 12 | no | no | yes | no | -0.17% | -0.01% | -0.09% | -0.01% | 0.00% | SGLD | 0.00% | 0.0% | 1 |
| `union_vol_green_h3` | long | not something good | 17 | 12 | no | no | yes | no | -0.43% | -0.01% | 1.64% | 0.16% | -5.72% | INDP | 7.96% | 21.7% | 1 |
| `flatten_h1` | long | not something good | 17 | 12 | no | no | no | no | -0.86% | -0.05% | 3.94% | 0.34% | -3.35% | ARCT | -17.96% | 9.8% | 1 |
| `union_join_g_h1` | long | not something good | 17 | 12 | no | no | no | no | -0.86% | -0.05% | 4.84% | 0.42% | -2.71% | CYPH | -14.74% | 12.9% | 1 |
| `union_coil_green_h3` | long | not something good | 17 | 12 | no | no | no | no | -0.87% | -0.05% | 1.37% | 0.13% | -7.50% | CYPH | -4.19% | 16.5% | 1 |
| `yday_gainer_h5` | long | not something good | 21 | 16 | no | no | yes | no | -0.95% | -0.02% | 1.63% | 0.14% | -5.39% | ARCT | 10.08% | 33.5% | 1 |
| `union_ab_g_h3` | long | not something good | 13 | 12 | no | no | no | no | -1.09% | -0.06% | 1.13% | 0.12% | -3.53% | BAND | -10.13% | 19.6% | 1 |
| `yday_gainer_h3` | long | not something good | 21 | 16 | no | no | no | no | -1.09% | -0.04% | 3.63% | 0.25% | -3.04% | CRML | -10.53% | 34.6% | 1 |
| `probable_h3` | long | not something good | 21 | 16 | no | no | no | no | -1.18% | -0.05% | 3.75% | 0.25% | -3.07% | CRML | -9.06% | 26.2% | 1 |
| `union_e_fresh_h1` | long | not something good | 17 | 12 | no | no | no | no | -1.21% | -0.03% | 6.14% | 0.56% | -5.22% | CHPT | -5.50% | 9.2% | 1 |
| `union_blue_h1` | long | not something good | 17 | 12 | no | no | no | no | -1.21% | -0.07% | 6.90% | 0.58% | -2.81% | ARCT | -8.08% | 15.8% | 1 |
| `union_news_g_h5` | long | not something good | 17 | 12 | no | no | no | no | -1.29% | -0.10% | -0.89% | -0.07% | -3.89% | AVGO | -2.35% | 0.0% | 1 |
| `union_cond_n4_h3` | long | not something good | 17 | 12 | no | no | no | no | -1.39% | -0.09% | 0.05% | 0.03% | -6.25% | ARCT | -0.70% | 10.9% | 1 |
| `union_candle_h3` | long | not something good | 17 | 12 | no | no | no | no | -1.52% | -0.11% | 0.39% | 0.05% | -4.20% | BAND | -6.53% | 14.3% | 1 |
| `union_e_green_h1` | long | not something good | 17 | 12 | no | no | no | no | -1.66% | -0.03% | 3.25% | 0.36% | -6.62% | DBI | -4.75% | 17.0% | 1 |
| `flatten_h3` | long | not something good | 17 | 12 | no | no | no | no | -1.84% | -0.12% | 0.70% | 0.09% | -6.17% | CYPH | -2.45% | 18.2% | 1 |
| `union_join_vol_green_h3` | long | not something good | 17 | 12 | no | no | yes | no | -1.85% | -0.13% | -0.16% | 0.01% | -7.05% | INDP | 1.30% | 13.7% | 1 |
| `probable_h1` | long | not something good | 21 | 16 | no | no | no | no | -2.14% | -0.11% | 8.67% | 0.54% | -4.50% | IRD | -9.75% | 22.9% | 1 |
| `union_vol_missing_h3` | long | not something good | 17 | 12 | no | no | yes | no | -2.21% | -0.18% | -2.13% | -0.18% | 0.00% | SGLD | 0.00% | 0.0% | 1 |
| `union_news_missing_h3` | long | not something good | 17 | 12 | no | no | no | no | -2.31% | -0.16% | 0.32% | 0.06% | -6.44% | CYPH | -3.10% | 22.6% | 1 |
| `union_last_red_h3` | long | not something good | 17 | 12 | no | no | yes | no | -2.35% | -0.17% | 1.26% | 0.14% | -7.32% | INDP | 15.33% | 25.9% | 1 |
| `short_alarm_h3` | short | not something good | 17 | 12 | no | no | no | no | -2.40% | -0.19% | -0.06% | 0.01% | -3.35% | SAIC | -11.68% | 19.5% | 1 |
| `union_h1` | long | not something good | 21 | 16 | no | no | no | no | -3.03% | -0.16% | 6.33% | 0.41% | -7.28% | CYPH | -15.56% | 13.1% | 1 |
| `union_h3_exit_news_r` | long | not something good | 17 | 12 | no | no | no | no | -3.26% | -0.23% | -0.31% | 0.02% | -7.51% | CYPH | -3.50% | 20.5% | 1 |
| `union_earn_react_h1` | long | not something good | 17 | 12 | no | no | no | no | -3.34% | -0.20% | 3.43% | 0.35% | -7.28% | CHPT | -5.55% | 8.7% | 1 |
| `union_h5` | long | not something good | 21 | 16 | no | no | yes | no | -3.53% | -0.21% | -1.53% | -0.08% | -4.82% | CMRC | 0.53% | 23.8% | 1 |
| `union_join_present_h3` | long | not something good | 17 | 12 | no | no | no | no | -3.94% | -0.29% | -1.00% | -0.05% | -8.17% | CYPH | -3.60% | 20.4% | 1 |
| `union_h3_exit_alarm` | long | not something good | 17 | 12 | no | no | no | no | -4.36% | -0.34% | -0.81% | -0.04% | -7.91% | CYPH | -7.35% | 16.5% | 1 |
| `union_coil_green_h1` | long | not something good | 17 | 12 | no | no | no | no | -4.77% | -0.39% | 0.48% | 0.05% | -6.21% | YEXT | -7.98% | 12.0% | 1 |
| `union_last_green_h5` | long | not something good | 17 | 12 | no | no | no | no | -4.80% | -0.39% | -2.55% | -0.20% | -6.28% | GGB | -1.11% | 22.8% | 1 |
| `union_coil_off_h5` | long | not something good | 17 | 12 | no | no | no | no | -4.80% | -0.39% | -3.12% | -0.25% | -5.96% | KGC | -13.35% | 22.0% | 1 |
| `union_candle_score_h3` | long | not something good | 17 | 12 | no | no | no | no | -4.90% | -0.40% | -2.85% | -0.22% | -7.01% | SWKS | -4.67% | 8.8% | 1 |
| `union_white_h5` | long | not something good | 17 | 12 | no | no | no | no | -5.34% | -0.44% | -3.91% | -0.31% | -6.49% | KGC | -1.11% | 18.2% | 1 |
| `ohlc_hot_h5` | long | not something good | 21 | 16 | no | no | yes | no | -5.69% | -0.33% | -3.39% | -0.18% | -6.96% | TRON | 9.13% | 18.3% | 1 |
| `union_blue_coil_h1` | long | not something good | 17 | 12 | no | no | no | no | -5.78% | -0.48% | 2.27% | 0.20% | -7.09% | CTMX | -16.32% | 18.9% | 1 |
| `union_coil_off_h1` | long | not something good | 17 | 12 | no | no | no | no | -6.14% | -0.52% | -0.49% | -0.03% | -7.08% | KGC | -8.71% | 12.2% | 1 |
| `union_cond_h3` | long | not something good | 17 | 12 | no | no | no | no | -6.46% | -0.53% | -4.33% | -0.35% | -8.74% | ARCT | -5.05% | 11.6% | 1 |
| `union_coil_off_h3` | long | not something good | 17 | 12 | no | no | no | no | -6.71% | -0.56% | -4.45% | -0.36% | -10.08% | CYPH | -5.37% | 15.9% | 1 |
| `union_blue_h3` | long | not something good | 17 | 12 | no | no | yes | no | -7.96% | -0.67% | -5.20% | -0.43% | -9.64% | BAND | 3.41% | 17.5% | 1 |
| `union_e_fresh_h3` | long | not something good | 17 | 12 | no | no | yes | no | -8.28% | -0.69% | -6.39% | -0.52% | -9.94% | NVDA | 1.67% | 12.3% | 1 |
| `coil_h3_exit_alarm` | long | not something good | 17 | 12 | no | no | no | no | -8.51% | -0.71% | -6.06% | -0.50% | -11.84% | CYPH | -7.67% | 16.5% | 1 |
| `ohlc_hot_coil_h1` | long | not something good | 17 | 12 | no | no | no | no | -8.53% | -0.72% | -3.34% | -0.26% | -9.91% | YEXT | -10.97% | 10.3% | 1 |
| `union_earn_react_h3` | long | not something good | 17 | 12 | no | no | yes | no | -9.24% | -0.78% | -7.35% | -0.61% | -11.50% | SHOE | 1.73% | 12.9% | 1 |
| `ohlc_hot_h3` | long | not something good | 21 | 16 | no | no | no | no | -9.43% | -0.60% | -6.14% | -0.37% | -11.36% | ASST | -10.45% | 15.4% | 1 |
| `union_join_g_h3` | long | not something good | 17 | 12 | no | no | yes | no | -9.54% | -0.80% | -6.82% | -0.56% | -11.76% | ARCT | 0.42% | 18.8% | 1 |
| `union_blue_coil_h3` | long | not something good | 17 | 12 | no | no | no | no | -9.89% | -0.85% | -7.11% | -0.60% | -11.45% | BAND | -4.44% | 20.5% | 1 |
| `probable_h5` | long | not something good | 21 | 16 | no | no | yes | no | -12.17% | -0.79% | -9.69% | -0.61% | -14.46% | GORO | 8.93% | 28.0% | 1 |
| `ohlc_hot_h1` | long | not something good | 21 | 16 | no | no | no | no | -13.97% | -0.92% | -5.89% | -0.37% | -15.91% | USDE | -11.30% | 12.6% | 1 |
| `short_last_red_h3` | short | not something good | 21 | 16 | no | no | no | no | -15.92% | -1.04% | -8.60% | -0.52% | -16.79% | CABA | -2.15% | 19.7% | 1 |
| `short_last_red_h1` | short | not something good | 21 | 16 | no | no | no | no | -16.27% | -1.10% | -8.62% | -0.56% | -16.89% | CABA | -0.91% | 19.9% | 1 |
| `short_extended_h1` | short | not something good | 21 | 16 | no | no | no | no | -19.16% | -1.30% | -11.98% | -0.78% | -19.76% | NEOV | -7.61% | 23.2% | 1 |
| `short_extended_h3` | short | not something good | 21 | 16 | no | no | no | no | -19.94% | -1.30% | -13.58% | -0.84% | -22.17% | USDE | -7.73% | 21.1% | 1 |
| `flatten_vol_g_h3` | long | not something good | 17 | 12 | no | no | no | no | -21.38% | -1.95% | -20.58% | -1.87% | -21.43% | ATRC | -13.09% | 14.6% | 1 |
| `union_e_green_h3` | long | not something good | 17 | 12 | no | no | no | no | -24.87% | -2.18% | -21.24% | -1.80% | -25.47% | BJ | -3.43% | 21.7% | 1 |
