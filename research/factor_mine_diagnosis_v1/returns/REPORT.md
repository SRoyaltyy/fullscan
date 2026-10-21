# factor_mine_diagnosis_v1

Diagnosis only. No recipe and no combination is selected for trading.

The cash book runs from 2026-08-13 through 2026-09-25 and does not reset on 2026-09-14. Futubull fees are the main figure. Flat 15bp uses the same share counts.

A row with fewer than 30 closed trades is flagged and kept.

## Reading

These tables describe the nineteen books. The order of a table is for reading. No recipe and no combination is selected for trading.

On the cleaned v1c tape, through 2026-09-11: 9 of 19 books compound above zero. After the largest name's dollars are removed, 3 of those stay above zero (`union_hot_score_h3`, `union_ret_5_h3`, `union_hot_n4_holdup`). 13 of 19 compounds sit above the mean of that book's 1,000 RANDOM4 paths. The weather sit lowers the compound on 15 of 19 books. The recipe sort, against a seeded shuffle of the same matched list, raises the compound on 15 of 19. Flagged under 30 closed trades: 1 (`union_cond_n4_h3`).

On the cleaned v1c tape, from 2026-09-14: 12 of 19 books compound above zero. After the largest name's dollars are removed, 2 of those stay above zero (`union_w_hot_cond_h3`, `union_hot_n4_holdup`). 15 of 19 compounds sit above the mean of that book's 1,000 RANDOM4 paths. The weather sit lowers the compound on 13 of 19 books. The recipe sort, against a seeded shuffle of the same matched list, raises the compound on 16 of 19. Flagged under 30 closed trades: 9 (`probable_h3`, `probable_h5`, `short_extended_h3`, `union_candle_score_h3`, `union_cond_n4_h3`, `union_hot_n4_h1`, `union_ret_5_h3`, `union_w_hot_candle_h3`, `union_hot_n4_holdup`).

On the Yahoo pin tape, through 2026-09-11: 9 of 19 books compound above zero. After the largest name's dollars are removed, 5 of those stay above zero (`union_candle_score_h3`, `union_hot_n4_h1`, `union_hot_score_h3`, `union_ret_5_h3`, `union_hot_n4_holdup`). 14 of 19 compounds sit above the mean of that book's 1,000 RANDOM4 paths. The weather sit lowers the compound on 15 of 19 books. The recipe sort, against a seeded shuffle of the same matched list, raises the compound on 15 of 19. Flagged under 30 closed trades: 1 (`union_cond_n4_h3`).

On the Yahoo pin tape, from 2026-09-14: 12 of 19 books compound above zero. After the largest name's dollars are removed, 2 of those stay above zero (`union_w_hot_cond_h3`, `union_hot_n4_holdup`). 15 of 19 compounds sit above the mean of that book's 1,000 RANDOM4 paths. The weather sit lowers the compound on 14 of 19 books. The recipe sort, against a seeded shuffle of the same matched list, raises the compound on 15 of 19. Flagged under 30 closed trades: 12 (`probable_h3`, `probable_h5`, `short_extended_h3`, `union_candle_score_h3`, `union_cond_h3`, `union_cond_n4_h3`, `union_hot_n4_h1`, `union_hot_score_h3`, `union_ret_5_h3`, `union_w_hot_candle_h3`, `union_w_hot_cond_h3`, `union_hot_n4_holdup`).

`union_hot_n4_holdup` was created on 2026-09-21, so the window through 2026-09-11 is before that sleeve existed. It is described with the others. It is not promoted. Its forward row is under 30 closed trades.

A few tickers carry a large share of the net. On the cleaned tape through 2026-09-11, CYPH is the largest name on several union books, and the holdup book's CYPH dollars are about half of that book's net. From 2026-09-14 the largest names are GLND and TJGC. A share above 100% means that name's gain was larger than the book's net, because other names lost money.

Timing on the cleaned tape through 2026-09-11: `union_hot_n4_holdup` books $2532 on later days and $808 on the entry day, with $2635 from 09:30 to the close and $706 from the overnight gap. `probable_h3` books $-784 on later days and $-177 on the entry day, with $-883 from 09:30 to the close and $-78 from the overnight gap. Up and down days, with morning S and the missing-IWM count, are under `conditions` in the results file.

Yahoo IWM is an outcome, and it is missing on 3 tune sessions and on all 10 sessions from 2026-09-14. Those sessions are counted as missing. The cleaned tape has an IWM bar on the sessions in this study.

The tune tree's earnings-react leaf has 47 pooled trades and a 72.3% win rate. The same leaf on forward trades has 5 trades and is flagged under 30. Pooled counts can repeat a name-day once per recipe. Joint buckets with at least 30 trades that sit farthest from a 50% win rate are losing buckets. No leaf is selected.

## Baseline

| recipe | tape | window | compound | flat 15bp | win rate | closed | up | down | flat | <30 | ex-best |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | ---: |
| `probable_h3` | clean | tune | -9.61% | -6.35% | 27.42% | 62 | 7 | 13 | 1 |  | -11.19% |
| `probable_h5` | clean | tune | -17.23% | -14.80% | 25.00% | 52 | 6 | 14 | 1 |  | -19.40% |
| `short_extended_h3` | clean | tune | -4.87% | -0.77% | 52.54% | 59 | 12 | 9 | 0 |  | -7.11% |
| `union_candle_score_h1` | clean | tune | 3.07% | 7.56% | 41.03% | 78 | 5 | 11 | 5 |  | -3.83% |
| `union_candle_score_h3` | clean | tune | 4.21% | 7.06% | 40.00% | 55 | 9 | 12 | 0 |  | -0.43% |
| `union_cond_h1` | clean | tune | -4.46% | 1.16% | 39.02% | 82 | 7 | 9 | 5 |  | -8.62% |
| `union_cond_h3` | clean | tune | -3.07% | -0.49% | 34.48% | 58 | 11 | 10 | 0 |  | -7.43% |
| `union_cond_n4_h3` | clean | tune | -2.44% | -0.22% | 26.92% | 26 | 9 | 12 | 0 | yes | -10.97% |
| `union_hot_n12_h1` | clean | tune | -7.74% | -0.85% | 44.14% | 111 | 7 | 11 | 3 |  | -11.42% |
| `union_hot_n4_h1` | clean | tune | 13.92% | 19.09% | 52.94% | 34 | 7 | 10 | 4 |  | -0.53% |
| `union_hot_score_h1` | clean | tune | -4.16% | 1.95% | 45.21% | 73 | 7 | 11 | 3 |  | -9.70% |
| `union_hot_score_h3` | clean | tune | 4.97% | 8.80% | 36.84% | 57 | 10 | 11 | 0 |  | 0.29% |
| `union_ret_5_h1` | clean | tune | -0.44% | 6.24% | 43.84% | 73 | 5 | 11 | 5 |  | -6.12% |
| `union_ret_5_h3` | clean | tune | 9.68% | 13.94% | 39.34% | 61 | 11 | 10 | 0 |  | 4.77% |
| `union_w_hot_candle_h1` | clean | tune | 0.22% | 5.38% | 49.33% | 75 | 6 | 11 | 4 |  | -6.45% |
| `union_w_hot_candle_h3` | clean | tune | 2.80% | 6.15% | 40.68% | 59 | 9 | 12 | 0 |  | -1.79% |
| `union_w_hot_cond_h1` | clean | tune | -3.17% | 2.22% | 46.75% | 77 | 7 | 10 | 4 |  | -9.48% |
| `union_w_hot_cond_h3` | clean | tune | 3.19% | 6.50% | 34.48% | 58 | 10 | 11 | 0 |  | -1.28% |
| `union_hot_n4_holdup` | clean | tune | 33.40% | 38.28% | 60.00% | 30 | 10 | 10 | 1 |  | 13.84% |
| `probable_h3` | clean | forward | -2.96% | -1.52% | 22.73% | 22 | 3 | 5 | 2 | yes | -4.69% |
| `probable_h5` | clean | forward | -5.72% | -4.73% | 43.75% | 16 | 3 | 7 | 0 | yes | -7.15% |
| `short_extended_h3` | clean | forward | -14.67% | -12.87% | 61.90% | 21 | 2 | 6 | 2 | yes | -15.51% |
| `union_candle_score_h1` | clean | forward | -5.78% | -3.56% | 38.10% | 42 | 4 | 6 | 0 |  | -6.85% |
| `union_candle_score_h3` | clean | forward | 8.50% | 9.13% | 44.00% | 25 | 6 | 4 | 0 | yes | -0.55% |
| `union_cond_h1` | clean | forward | -11.51% | -8.88% | 30.43% | 46 | 3 | 6 | 1 |  | -11.91% |
| `union_cond_h3` | clean | forward | -9.16% | -7.88% | 20.00% | 30 | 4 | 6 | 0 |  | -10.86% |
| `union_cond_n4_h3` | clean | forward | -0.13% | 0.57% | 40.00% | 20 | 4 | 6 | 0 | yes | -4.03% |
| `union_hot_n12_h1` | clean | forward | 0.85% | 3.64% | 33.33% | 54 | 3 | 7 | 0 |  | -8.94% |
| `union_hot_n4_h1` | clean | forward | 23.21% | 24.70% | 44.00% | 25 | 5 | 5 | 0 | yes | -5.96% |
| `union_hot_score_h1` | clean | forward | 7.81% | 10.05% | 35.00% | 40 | 3 | 7 | 0 |  | -7.01% |
| `union_hot_score_h3` | clean | forward | 6.69% | 7.77% | 29.03% | 31 | 6 | 4 | 0 |  | -0.19% |
| `union_ret_5_h1` | clean | forward | 5.21% | 7.44% | 37.21% | 43 | 5 | 4 | 1 |  | -9.56% |
| `union_ret_5_h3` | clean | forward | 7.33% | 8.11% | 30.43% | 23 | 6 | 4 | 0 | yes | -2.11% |
| `union_w_hot_candle_h1` | clean | forward | 8.37% | 10.29% | 40.48% | 42 | 5 | 4 | 1 |  | -7.13% |
| `union_w_hot_candle_h3` | clean | forward | 4.69% | 5.59% | 25.93% | 27 | 7 | 3 | 0 | yes | -2.00% |
| `union_w_hot_cond_h1` | clean | forward | 11.10% | 12.84% | 39.53% | 43 | 4 | 5 | 1 |  | -4.34% |
| `union_w_hot_cond_h3` | clean | forward | 12.56% | 13.22% | 32.26% | 31 | 7 | 3 | 0 |  | 3.34% |
| `union_hot_n4_holdup` | clean | forward | 37.22% | 37.84% | 45.45% | 22 | 7 | 3 | 0 | yes | 8.66% |
| `probable_h3` | yahoo | tune | -5.75% | -2.62% | 32.00% | 50 | 8 | 11 | 2 |  | -9.52% |
| `probable_h5` | yahoo | tune | -16.81% | -14.31% | 28.57% | 49 | 6 | 14 | 1 |  | -18.99% |
| `short_extended_h3` | yahoo | tune | -6.51% | -2.02% | 49.23% | 65 | 12 | 9 | 0 |  | -8.71% |
| `union_candle_score_h1` | yahoo | tune | 2.13% | 6.87% | 40.74% | 81 | 5 | 11 | 5 |  | -4.71% |
| `union_candle_score_h3` | yahoo | tune | 5.19% | 7.87% | 40.00% | 55 | 9 | 12 | 0 |  | 0.50% |
| `union_cond_h1` | yahoo | tune | -5.15% | 0.53% | 38.55% | 83 | 7 | 9 | 5 |  | -9.28% |
| `union_cond_h3` | yahoo | tune | -3.00% | -0.41% | 33.33% | 60 | 11 | 10 | 0 |  | -7.36% |
| `union_cond_n4_h3` | yahoo | tune | -2.45% | -0.22% | 25.93% | 27 | 9 | 12 | 0 | yes | -10.97% |
| `union_hot_n12_h1` | yahoo | tune | -5.85% | 1.51% | 45.00% | 120 | 8 | 11 | 2 |  | -9.60% |
| `union_hot_n4_h1` | yahoo | tune | 15.98% | 21.93% | 50.00% | 40 | 8 | 10 | 3 |  | 1.38% |
| `union_hot_score_h1` | yahoo | tune | -1.03% | 5.70% | 46.34% | 82 | 8 | 11 | 2 |  | -6.73% |
| `union_hot_score_h3` | yahoo | tune | 6.12% | 9.93% | 37.93% | 58 | 11 | 10 | 0 |  | 1.39% |
| `union_ret_5_h1` | yahoo | tune | 0.34% | 7.94% | 44.58% | 83 | 7 | 10 | 4 |  | -5.29% |
| `union_ret_5_h3` | yahoo | tune | 9.40% | 13.58% | 44.26% | 61 | 12 | 9 | 0 |  | 4.54% |
| `union_w_hot_candle_h1` | yahoo | tune | -0.51% | 5.01% | 48.15% | 81 | 7 | 11 | 3 |  | -7.15% |
| `union_w_hot_candle_h3` | yahoo | tune | 2.93% | 6.29% | 41.67% | 60 | 10 | 11 | 0 |  | -1.68% |
| `union_w_hot_cond_h1` | yahoo | tune | -1.62% | 4.28% | 46.43% | 84 | 8 | 10 | 3 |  | -8.01% |
| `union_w_hot_cond_h3` | yahoo | tune | 3.73% | 7.10% | 37.70% | 61 | 10 | 11 | 0 |  | -0.77% |
| `union_hot_n4_holdup` | yahoo | tune | 35.82% | 41.28% | 64.52% | 31 | 12 | 8 | 1 |  | 15.85% |
| `probable_h3` | yahoo | forward | -2.79% | -1.37% | 25.00% | 20 | 3 | 5 | 2 | yes | -4.52% |
| `probable_h5` | yahoo | forward | -5.02% | -4.12% | 46.67% | 15 | 3 | 7 | 0 | yes | -6.31% |
| `short_extended_h3` | yahoo | forward | -15.74% | -13.82% | 60.00% | 20 | 1 | 7 | 2 | yes | -16.57% |
| `union_candle_score_h1` | yahoo | forward | -5.86% | -3.60% | 38.10% | 42 | 4 | 6 | 0 |  | -6.94% |
| `union_candle_score_h3` | yahoo | forward | 8.38% | 9.06% | 40.74% | 27 | 6 | 4 | 0 | yes | -0.59% |
| `union_cond_h1` | yahoo | forward | -11.47% | -8.87% | 31.11% | 45 | 3 | 6 | 1 |  | -11.87% |
| `union_cond_h3` | yahoo | forward | -9.23% | -7.95% | 20.69% | 29 | 4 | 6 | 0 | yes | -10.92% |
| `union_cond_n4_h3` | yahoo | forward | -0.41% | 0.29% | 40.00% | 20 | 4 | 6 | 0 | yes | -4.30% |
| `union_hot_n12_h1` | yahoo | forward | 0.98% | 3.73% | 35.19% | 54 | 2 | 8 | 0 |  | -8.70% |
| `union_hot_n4_h1` | yahoo | forward | 24.09% | 25.45% | 46.15% | 26 | 5 | 5 | 0 | yes | -4.60% |
| `union_hot_score_h1` | yahoo | forward | 8.32% | 10.48% | 37.50% | 40 | 4 | 6 | 0 |  | -6.33% |
| `union_hot_score_h3` | yahoo | forward | 6.44% | 7.49% | 26.32% | 19 | 6 | 4 | 0 | yes | -0.52% |
| `union_ret_5_h1` | yahoo | forward | 5.81% | 7.96% | 39.53% | 43 | 5 | 4 | 1 |  | -8.76% |
| `union_ret_5_h3` | yahoo | forward | 7.33% | 8.13% | 30.43% | 23 | 6 | 4 | 0 | yes | -2.12% |
| `union_w_hot_candle_h1` | yahoo | forward | 8.79% | 10.74% | 41.86% | 43 | 5 | 4 | 1 |  | -6.45% |
| `union_w_hot_candle_h3` | yahoo | forward | 4.70% | 5.63% | 28.57% | 28 | 7 | 3 | 0 | yes | -1.99% |
| `union_w_hot_cond_h1` | yahoo | forward | 11.75% | 13.39% | 41.86% | 43 | 4 | 5 | 1 |  | -3.46% |
| `union_w_hot_cond_h3` | yahoo | forward | 12.63% | 13.26% | 27.59% | 29 | 6 | 4 | 0 | yes | 3.34% |
| `union_hot_n4_holdup` | yahoo | forward | 39.32% | 39.79% | 45.83% | 24 | 7 | 3 | 0 | yes | 11.07% |

## Profit source

| recipe | tape | window | top name | top share of net | under $3 | $3–$10 | $10+ | without best 1 | best 3 | best 5 | 76 share | <30 |
| --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| `probable_h3` | clean | tune | MRVI | -17.52% | 0.21% | 35.71% | 67.16% | -11.19% | -13.76% | -16.10% | -0.00% |  |
| `probable_h5` | clean | tune | GORO | -13.95% | 15.18% | 33.47% | 32.53% | -19.40% | -21.08% | -21.86% | -0.00% |  |
| `short_extended_h3` | clean | tune | TNDM | -50.23% | 58.39% | 52.80% | -65.91% | -7.11% | -9.28% | -11.17% | -0.00% |  |
| `union_candle_score_h1` | clean | tune | CYPH | 229.32% | 216.50% | -3.08% | -203.03% | -3.83% | -6.55% | -8.87% | 0.00% |  |
| `union_candle_score_h3` | clean | tune | INO | 112.04% | 198.10% | -76.59% | -88.14% | -0.43% | -5.78% | -8.05% | 0.00% |  |
| `union_cond_h1` | clean | tune | CYPH | -101.77% | -12.97% | -7.73% | 122.17% | -8.62% | -11.30% | -13.00% | -0.00% |  |
| `union_cond_h3` | clean | tune | INO | -153.84% | -181.57% | 19.94% | 265.19% | -7.43% | -9.25% | -11.03% | -0.00% |  |
| `union_cond_n4_h3` | clean | tune | INO | -386.01% | -383.33% | 51.24% | 413.71% | -10.97% | -13.24% | -14.97% | -0.00% | yes |
| `union_hot_n12_h1` | clean | tune | CYPH | -50.54% | 3.79% | -21.29% | 93.17% | -11.42% | -14.61% | -16.16% | -0.00% |  |
| `union_hot_n4_h1` | clean | tune | CYPH | 101.47% | 94.72% | 19.84% | -4.50% | -0.53% | -6.47% | -10.61% | 0.00% |  |
| `union_hot_score_h1` | clean | tune | CYPH | -141.68% | -31.19% | -44.17% | 142.13% | -9.70% | -14.60% | -16.97% | -0.00% |  |
| `union_hot_score_h3` | clean | tune | INO | 94.90% | 73.11% | 86.23% | -32.09% | 0.29% | -6.36% | -9.66% | 0.00% |  |
| `union_ret_5_h1` | clean | tune | CYPH | -1363.22% | -1152.39% | -182.45% | 1171.53% | -6.12% | -11.30% | -14.50% | -0.00% |  |
| `union_ret_5_h3` | clean | tune | INO | 49.23% | 53.29% | 37.38% | 13.64% | 4.77% | -2.35% | -6.42% | 0.00% |  |
| `union_w_hot_candle_h1` | clean | tune | CYPH | 3180.34% | 977.41% | 1187.41% | -1798.38% | -6.45% | -10.04% | -12.32% | 0.00% |  |
| `union_w_hot_candle_h3` | clean | tune | CYPH | 170.88% | 118.85% | 35.44% | -33.19% | -1.79% | -9.07% | -11.56% | 0.00% |  |
| `union_w_hot_cond_h1` | clean | tune | CYPH | -206.88% | -19.88% | -30.86% | 107.08% | -9.48% | -12.19% | -14.70% | -0.00% |  |
| `union_w_hot_cond_h3` | clean | tune | CYPH | 150.14% | 197.93% | -0.76% | -55.34% | -1.28% | -7.55% | -9.50% | 0.00% |  |
| `union_hot_n4_holdup` | clean | tune | CYPH | 55.00% | 68.57% | 9.48% | 26.85% | 13.84% | -0.94% | -5.43% | 0.00% |  |
| `probable_h3` | clean | forward | TYRA | -61.50% | 141.17% | -20.19% | 41.36% | -4.69% | -6.10% | -7.11% | -0.00% | yes |
| `probable_h5` | clean | forward | FEAM | -27.35% | 41.33% | 38.34% | 32.71% | -7.15% | -7.56% | -7.80% | -0.00% | yes |
| `short_extended_h3` | clean | forward | BRVE | -5.79% | 20.30% | 4.49% | 31.24% | -15.51% | -16.67% | -17.19% | -0.00% | yes |
| `union_candle_score_h1` | clean | forward | SECZ | -19.15% | 31.58% | -14.44% | 48.10% | -6.85% | -8.46% | -9.43% | -0.00% |  |
| `union_candle_score_h3` | clean | forward | TJGC | 105.45% | 0.00% | 23.40% | 99.01% | -0.55% | -5.83% | -6.71% | 0.00% | yes |
| `union_cond_h1` | clean | forward | VICR | -3.56% | -0.00% | 25.11% | 76.80% | -11.91% | -12.54% | -13.05% | -0.00% |  |
| `union_cond_h3` | clean | forward | VICR | -19.45% | -0.00% | 28.42% | 72.82% | -10.86% | -11.50% | -11.87% | -0.00% |  |
| `union_cond_n4_h3` | clean | forward | VICR | -3169.50% | -0.00% | 27.49% | 1157.76% | -4.03% | -5.72% | -6.58% | -0.00% | yes |
| `union_hot_n12_h1` | clean | forward | GLND | 1169.95% | 651.00% | -58.69% | -461.62% | -8.94% | -11.71% | -12.60% | 0.00% |  |
| `union_hot_n4_h1` | clean | forward | GLND | 127.47% | 98.72% | 0.24% | 4.73% | -5.96% | -12.68% | -14.25% | 0.00% | yes |
| `union_hot_score_h1` | clean | forward | GLND | 191.15% | 113.68% | 14.90% | -35.09% | -7.01% | -11.23% | -12.27% | 0.00% |  |
| `union_hot_score_h3` | clean | forward | TJGC | 102.02% | 23.55% | -53.19% | -3.89% | -0.19% | -9.19% | -9.69% | 0.00% |  |
| `union_ret_5_h1` | clean | forward | GLND | 284.23% | 170.43% | -36.41% | -47.67% | -9.56% | -11.22% | -12.39% | 0.00% |  |
| `union_ret_5_h3` | clean | forward | TJGC | 129.00% | 35.39% | -54.94% | 52.06% | -2.11% | -10.67% | -11.72% | 0.00% | yes |
| `union_w_hot_candle_h1` | clean | forward | GLND | 187.62% | 155.01% | -13.26% | -29.05% | -7.13% | -11.09% | -12.40% | 0.00% |  |
| `union_w_hot_candle_h3` | clean | forward | TJGC | 142.62% | 36.38% | -94.17% | -0.09% | -2.00% | -10.32% | -10.72% | 0.00% | yes |
| `union_w_hot_cond_h1` | clean | forward | GLND | 139.19% | 126.45% | 11.17% | -50.19% | -4.34% | -8.24% | -9.24% | 0.00% |  |
| `union_w_hot_cond_h3` | clean | forward | TJGC | 73.07% | 34.80% | -17.75% | 34.32% | 3.34% | -5.20% | -7.69% | 0.00% |  |
| `union_hot_n4_holdup` | clean | forward | GLND | 77.82% | 91.95% | 13.70% | -2.71% | 8.66% | -7.43% | -8.68% | 0.00% | yes |
| `probable_h3` | yahoo | tune | CAPR | -70.35% | -0.21% | -5.77% | 111.87% | -9.52% | -12.53% | -14.93% | -70.35% |  |
| `probable_h5` | yahoo | tune | GORO | -14.30% | 15.60% | 31.87% | 33.49% | -18.99% | -20.65% | -21.49% | -2.38% |  |
| `short_extended_h3` | yahoo | tune | TNDM | -37.61% | 62.56% | 47.43% | -50.14% | -8.71% | -11.15% | -13.08% | 9.40% |  |
| `union_candle_score_h1` | yahoo | tune | CYPH | 332.48% | 303.77% | -58.29% | -273.82% | -4.71% | -7.41% | -9.74% | -35.76% |  |
| `union_candle_score_h3` | yahoo | tune | INO | 90.95% | 160.77% | -57.29% | -57.88% | 0.50% | -4.90% | -7.18% | -2.29% |  |
| `union_cond_h1` | yahoo | tune | CYPH | -87.45% | -10.97% | 8.98% | 103.27% | -9.28% | -11.94% | -13.74% | 15.69% |  |
| `union_cond_h3` | yahoo | tune | INO | -157.24% | -185.48% | 20.78% | 268.33% | -7.36% | -9.18% | -10.96% | 0.07% |  |
| `union_cond_n4_h3` | yahoo | tune | INO | -385.70% | -383.02% | 51.28% | 413.37% | -10.97% | -13.24% | -14.97% | 0.08% | yes |
| `union_hot_n12_h1` | yahoo | tune | CYPH | -66.78% | -0.83% | -51.66% | 118.44% | -9.60% | -13.51% | -15.67% | -26.75% |  |
| `union_hot_n4_h1` | yahoo | tune | CYPH | 88.18% | 81.89% | 28.01% | -0.98% | 1.38% | -6.17% | -11.01% | 9.96% |  |
| `union_hot_score_h1` | yahoo | tune | CYPH | -571.96% | -174.19% | -392.41% | 528.87% | -6.73% | -12.74% | -16.09% | -245.83% |  |
| `union_hot_score_h3` | yahoo | tune | INO | 77.07% | 58.73% | 77.83% | -14.28% | 1.39% | -5.65% | -9.01% | 6.93% |  |
| `union_ret_5_h1` | yahoo | tune | CYPH | 1788.37% | 1838.97% | -11.06% | -1381.17% | -5.29% | -10.50% | -14.39% | -157.82% |  |
| `union_ret_5_h3` | yahoo | tune | INO | 50.20% | 48.14% | 53.24% | 10.33% | 4.54% | -2.56% | -7.38% | 1.99% |  |
| `union_w_hot_candle_h1` | yahoo | tune | CYPH | -1338.32% | -417.25% | -246.11% | 652.40% | -7.15% | -10.72% | -12.98% | 177.87% |  |
| `union_w_hot_candle_h3` | yahoo | tune | CYPH | 163.93% | 113.55% | 27.89% | -21.14% | -1.68% | -9.26% | -11.25% | 14.01% |  |
| `union_w_hot_cond_h1` | yahoo | tune | CYPH | -405.02% | -75.44% | -110.85% | 201.13% | -8.01% | -11.49% | -14.02% | -48.46% |  |
| `union_w_hot_cond_h3` | yahoo | tune | CYPH | 128.67% | 170.14% | 4.76% | -33.05% | -0.77% | -7.36% | -9.26% | 11.91% |  |
| `union_hot_n4_holdup` | yahoo | tune | CYPH | 52.28% | 65.11% | 18.79% | 20.75% | 15.85% | 0.84% | -5.46% | 14.07% |  |
| `probable_h3` | yahoo | forward | TYRA | -65.42% | 149.69% | -22.47% | 41.43% | -4.52% | -6.03% | -7.04% | -0.00% | yes |
| `probable_h5` | yahoo | forward | FEAM | -27.70% | 47.14% | 42.73% | 17.96% | -6.31% | -6.64% | -6.84% | -0.00% | yes |
| `short_extended_h3` | yahoo | forward | BRVE | -5.29% | 18.86% | 4.25% | 30.35% | -16.57% | -17.67% | -18.20% | 5.41% | yes |
| `union_candle_score_h1` | yahoo | forward | SECZ | -18.85% | 31.05% | -14.24% | 47.97% | -6.94% | -8.56% | -9.53% | 1.25% |  |
| `union_candle_score_h3` | yahoo | forward | TJGC | 105.97% | 0.00% | 23.80% | 99.59% | -0.59% | -5.86% | -6.73% | -1.00% | yes |
| `union_cond_h1` | yahoo | forward | VICR | -3.60% | -0.00% | 25.18% | 76.08% | -11.87% | -12.50% | -13.02% | -0.00% |  |
| `union_cond_h3` | yahoo | forward | VICR | -19.30% | -0.00% | 28.20% | 72.09% | -10.92% | -11.56% | -11.93% | -0.00% | yes |
| `union_cond_n4_h3` | yahoo | forward | VICR | -975.39% | -0.00% | 8.46% | 356.42% | -4.30% | -5.74% | -6.58% | -0.00% | yes |
| `union_hot_n12_h1` | yahoo | forward | GLND | 1010.24% | 557.01% | -62.86% | -364.53% | -8.70% | -11.37% | -12.36% | 42.26% |  |
| `union_hot_n4_h1` | yahoo | forward | GLND | 120.86% | 93.16% | 0.25% | 10.58% | -4.60% | -11.45% | -13.72% | 6.08% | yes |
| `union_hot_score_h1` | yahoo | forward | GLND | 177.81% | 104.96% | 12.04% | -21.27% | -6.33% | -10.43% | -11.64% | 7.67% |  |
| `union_hot_score_h3` | yahoo | forward | TJGC | 107.50% | 23.58% | -56.53% | -2.49% | -0.52% | -9.55% | -10.06% | -1.20% | yes |
| `union_ret_5_h1` | yahoo | forward | GLND | 251.77% | 149.52% | -32.61% | -27.29% | -8.76% | -10.43% | -11.75% | 10.98% |  |
| `union_ret_5_h3` | yahoo | forward | TJGC | 129.30% | 37.15% | -63.01% | 51.06% | -2.12% | -10.79% | -11.84% | -0.91% | yes |
| `union_w_hot_candle_h1` | yahoo | forward | GLND | 175.66% | 144.68% | -12.46% | -19.27% | -6.45% | -10.44% | -11.93% | 7.70% |  |
| `union_w_hot_candle_h3` | yahoo | forward | TJGC | 142.38% | 36.66% | -93.84% | 0.30% | -1.99% | -10.31% | -10.73% | -1.32% | yes |
| `union_w_hot_cond_h1` | yahoo | forward | GLND | 129.53% | 117.51% | 10.56% | -39.63% | -3.46% | -7.39% | -8.62% | 6.39% |  |
| `union_w_hot_cond_h3` | yahoo | forward | TJGC | 73.20% | 34.71% | -17.99% | 32.22% | 3.34% | -5.25% | -7.75% | 0.22% | yes |
| `union_hot_n4_holdup` | yahoo | forward | GLND | 72.86% | 86.15% | 12.86% | 4.04% | 11.07% | -6.03% | -8.13% | 6.54% | yes |

YAAS is not in the 76. Its dollars are in the machine-readable results, apart from that share. ALP, NFE, TNMG, and WCT are the matched splits inside the 76.

## Timing

| recipe | tape | window | overnight $ | 09:30-to-close $ | first-day $ | later-day $ |
| --- | --- | --- | ---: | ---: | ---: | ---: |
| `probable_h3` | clean | tune | -78.07 | -882.50 | -176.50 | -784.07 |
| `probable_h5` | clean | tune | -518.49 | -1204.83 | -384.52 | -1338.80 |
| `short_extended_h3` | clean | tune | -40.45 | -446.63 | -32.27 | -454.81 |
| `union_candle_score_h1` | clean | tune | 184.20 | 122.39 | 262.04 | 44.54 |
| `union_candle_score_h3` | clean | tune | -199.81 | 621.00 | 316.99 | 104.20 |
| `union_cond_h1` | clean | tune | 414.49 | -860.62 | -598.35 | 152.23 |
| `union_cond_h3` | clean | tune | -430.02 | 123.29 | -55.41 | -251.33 |
| `union_cond_n4_h3` | clean | tune | -227.46 | -17.04 | -337.61 | 93.12 |
| `union_hot_n12_h1` | clean | tune | -314.68 | -459.17 | -472.11 | -301.74 |
| `union_hot_n4_h1` | clean | tune | 424.07 | 967.59 | 550.03 | 841.64 |
| `union_hot_score_h1` | clean | tune | -90.79 | -324.83 | -599.77 | 184.15 |
| `union_hot_score_h3` | clean | tune | -39.65 | 536.88 | -106.38 | 603.61 |
| `union_ret_5_h1` | clean | tune | 164.64 | -208.69 | -229.45 | 185.40 |
| `union_ret_5_h3` | clean | tune | 224.17 | 744.19 | -85.49 | 1053.85 |
| `union_w_hot_candle_h1` | clean | tune | 101.04 | -79.45 | -347.71 | 369.29 |
| `union_w_hot_candle_h3` | clean | tune | -203.78 | 483.93 | -34.93 | 315.08 |
| `union_w_hot_cond_h1` | clean | tune | -26.24 | -290.27 | -313.70 | -2.80 |
| `union_w_hot_cond_h3` | clean | tune | -205.78 | 524.63 | 60.29 | 258.56 |
| `union_hot_n4_holdup` | clean | tune | 705.51 | 2634.92 | 808.45 | 2531.99 |
| `probable_h3` | clean | forward | -495.35 | 227.56 | 32.92 | -300.71 |
| `probable_h5` | clean | forward | -320.37 | -152.75 | -143.49 | -329.64 |
| `short_extended_h3` | clean | forward | -145.03 | -1250.11 | -28.19 | -1366.95 |
| `union_candle_score_h1` | clean | forward | -268.29 | -326.98 | -203.76 | -391.51 |
| `union_candle_score_h3` | clean | forward | -160.40 | 1046.46 | 215.02 | 671.04 |
| `union_cond_h1` | clean | forward | -159.00 | -940.76 | -839.64 | -260.12 |
| `union_cond_h3` | clean | forward | -301.92 | -586.32 | -312.29 | -575.94 |
| `union_cond_n4_h3` | clean | forward | -268.91 | 256.48 | -29.76 | 17.33 |
| `union_hot_n12_h1` | clean | forward | 244.30 | -165.63 | -583.29 | 661.97 |
| `union_hot_n4_h1` | clean | forward | 1131.44 | 1512.67 | -232.81 | 2876.91 |
| `union_hot_score_h1` | clean | forward | 495.36 | 253.50 | -507.55 | 1256.40 |
| `union_hot_score_h3` | clean | forward | 88.61 | 613.41 | -162.17 | 864.19 |
| `union_ret_5_h1` | clean | forward | 327.61 | 191.48 | -563.26 | 1082.35 |
| `union_ret_5_h3` | clean | forward | 47.55 | 756.46 | -243.69 | 1047.70 |
| `union_w_hot_candle_h1` | clean | forward | 395.01 | 443.66 | -395.88 | 1234.56 |
| `union_w_hot_candle_h3` | clean | forward | -66.48 | 549.03 | -100.91 | 583.46 |
| `union_w_hot_cond_h1` | clean | forward | 387.74 | 686.88 | -101.60 | 1176.22 |
| `union_w_hot_cond_h3` | clean | forward | -10.95 | 1307.21 | -81.03 | 1377.29 |
| `union_hot_n4_holdup` | clean | forward | 1611.89 | 3353.79 | 370.60 | 4595.08 |
| `probable_h3` | yahoo | tune | -186.58 | -387.99 | -28.31 | -546.26 |
| `probable_h5` | yahoo | tune | -511.59 | -1169.52 | -375.85 | -1305.26 |
| `short_extended_h3` | yahoo | tune | 36.63 | -687.18 | -119.54 | -531.02 |
| `union_candle_score_h1` | yahoo | tune | 201.19 | 11.45 | 163.34 | 49.31 |
| `union_candle_score_h3` | yahoo | tune | -192.26 | 711.10 | 229.42 | 289.42 |
| `union_cond_h1` | yahoo | tune | 430.20 | -945.47 | -679.53 | 164.26 |
| `union_cond_h3` | yahoo | tune | -417.19 | 117.09 | -58.73 | -241.37 |
| `union_cond_n4_h3` | yahoo | tune | -227.62 | -17.08 | -337.94 | 93.25 |
| `union_hot_n12_h1` | yahoo | tune | -174.56 | -410.79 | -403.76 | -181.58 |
| `union_hot_n4_h1` | yahoo | tune | 919.41 | 678.26 | 273.00 | 1324.67 |
| `union_hot_score_h1` | yahoo | tune | 114.81 | -217.62 | -472.04 | 369.23 |
| `union_hot_score_h3` | yahoo | tune | 47.73 | 564.59 | -30.72 | 643.04 |
| `union_ret_5_h1` | yahoo | tune | 514.45 | -480.81 | -384.61 | 418.25 |
| `union_ret_5_h3` | yahoo | tune | 233.27 | 707.08 | -138.65 | 1079.00 |
| `union_w_hot_candle_h1` | yahoo | tune | 55.93 | -107.32 | -357.02 | 305.63 |
| `union_w_hot_candle_h3` | yahoo | tune | -118.12 | 410.67 | 30.54 | 262.00 |
| `union_w_hot_cond_h1` | yahoo | tune | 220.35 | -381.90 | -384.25 | 222.70 |
| `union_w_hot_cond_h3` | yahoo | tune | -106.39 | 479.10 | 77.15 | 295.56 |
| `union_hot_n4_holdup` | yahoo | tune | 1034.05 | 2547.99 | 683.10 | 2898.95 |
| `probable_h3` | yahoo | forward | -514.84 | 252.24 | 40.52 | -303.11 |
| `probable_h5` | yahoo | forward | -275.97 | -141.90 | -126.40 | -291.48 |
| `short_extended_h3` | yahoo | forward | -146.86 | -1324.78 | -80.59 | -1391.05 |
| `union_candle_score_h1` | yahoo | forward | -267.88 | -330.95 | -207.68 | -391.15 |
| `union_candle_score_h3` | yahoo | forward | -161.86 | 1042.86 | 209.67 | 671.34 |
| `union_cond_h1` | yahoo | forward | -172.09 | -916.27 | -817.39 | -270.97 |
| `union_cond_h3` | yahoo | forward | -301.40 | -593.73 | -319.17 | -575.95 |
| `union_cond_n4_h3` | yahoo | forward | -268.96 | 228.59 | -57.65 | 17.28 |
| `union_hot_n12_h1` | yahoo | forward | 215.77 | -123.70 | -554.84 | 646.91 |
| `union_hot_n4_h1` | yahoo | forward | 1113.93 | 1680.43 | -91.50 | 2885.86 |
| `union_hot_score_h1` | yahoo | forward | 473.45 | 349.66 | -442.71 | 1265.83 |
| `union_hot_score_h3` | yahoo | forward | 78.96 | 604.64 | -173.08 | 856.69 |
| `union_ret_5_h1` | yahoo | forward | 305.16 | 277.50 | -484.56 | 1067.22 |
| `union_ret_5_h3` | yahoo | forward | 46.96 | 754.57 | -252.36 | 1053.89 |
| `union_w_hot_candle_h1` | yahoo | forward | 374.68 | 500.29 | -329.14 | 1204.11 |
| `union_w_hot_candle_h3` | yahoo | forward | -66.48 | 549.84 | -106.77 | 590.13 |
| `union_w_hot_cond_h1` | yahoo | forward | 358.14 | 797.58 | -3.66 | 1159.38 |
| `union_w_hot_cond_h3` | yahoo | forward | -21.62 | 1332.18 | -81.43 | 1391.99 |
| `union_hot_n4_holdup` | yahoo | forward | 1603.19 | 3737.17 | 540.33 | 4800.03 |

Up, down, and flat days, and the market around them, are in the results file under `conditions`. Morning S and last-green breadth are knowable at 09:30. IWM open-to-close and list open-to-close are outcomes. Yahoo IWM is missing after the pin ends.

## Drop-one contribution

Contribution is the full book minus the book with that part removed. A negative contribution means the book did better without the part.

| recipe | tape | window | part | compound contribution | win-rate contribution | dropped closed | <30 |
| --- | --- | --- | --- | ---: | ---: | ---: | --- |
| `probable_h3` | clean | tune | must_have | 0.00% | 0.00% | 62 |  |
| `probable_h3` | clean | forward | must_have | 0.00% | 0.00% | 22 | yes |
| `probable_h3` | clean | tune | must_not | 0.00% | 0.00% | 62 |  |
| `probable_h3` | clean | forward | must_not | 0.00% | 0.00% | 22 | yes |
| `probable_h3` | clean | tune | sort | 0.00% | 0.00% | 62 |  |
| `probable_h3` | clean | forward | sort | 0.00% | 0.00% | 22 | yes |
| `probable_h3` | clean | tune | weather | -4.51% | -8.43% | 106 |  |
| `probable_h3` | clean | forward | weather | 6.11% | -0.53% | 43 |  |
| `probable_h3` | clean | tune | hold_rule | -0.43% | -1.61% | 62 |  |
| `probable_h3` | clean | forward | hold_rule | 1.36% | -3.36% | 23 | yes |
| `probable_h5` | clean | tune | must_have | 0.00% | 0.00% | 52 |  |
| `probable_h5` | clean | forward | must_have | 0.00% | 0.00% | 16 | yes |
| `probable_h5` | clean | tune | must_not | 0.00% | 0.00% | 52 |  |
| `probable_h5` | clean | forward | must_not | 0.00% | 0.00% | 16 | yes |
| `probable_h5` | clean | tune | sort | 0.00% | 0.00% | 52 |  |
| `probable_h5` | clean | forward | sort | -0.00% | 0.00% | 16 | yes |
| `probable_h5` | clean | tune | weather | 0.26% | -16.43% | 70 |  |
| `probable_h5` | clean | forward | weather | -15.37% | 2.84% | 44 |  |
| `probable_h5` | clean | tune | hold_rule | 0.00% | 0.00% | 52 |  |
| `probable_h5` | clean | forward | hold_rule | 0.04% | 0.00% | 16 | yes |
| `short_extended_h3` | clean | tune | must_have | -1.85% | -0.88% | 73 |  |
| `short_extended_h3` | clean | forward | must_have | -15.06% | -2.96% | 37 |  |
| `short_extended_h3` | clean | tune | must_not | 0.00% | 0.00% | 59 |  |
| `short_extended_h3` | clean | forward | must_not | 0.00% | 0.00% | 21 | yes |
| `short_extended_h3` | clean | tune | sort | -11.95% | -4.60% | 63 |  |
| `short_extended_h3` | clean | forward | sort | -12.28% | -6.28% | 22 | yes |
| `short_extended_h3` | clean | tune | weather | 7.81% | -0.65% | 94 |  |
| `short_extended_h3` | clean | forward | weather | -4.31% | -0.26% | 37 |  |
| `short_extended_h3` | clean | tune | hold_rule | 0.66% | 2.54% | 62 |  |
| `short_extended_h3` | clean | forward | hold_rule | -3.07% | -1.73% | 22 | yes |
| `union_candle_score_h1` | clean | tune | must_have | 0.00% | 0.00% | 78 |  |
| `union_candle_score_h1` | clean | forward | must_have | 0.00% | 0.00% | 42 |  |
| `union_candle_score_h1` | clean | tune | must_not | -1.53% | -0.53% | 77 |  |
| `union_candle_score_h1` | clean | forward | must_not | 1.93% | -0.93% | 41 |  |
| `union_candle_score_h1` | clean | tune | sort | 6.87% | -5.22% | 80 |  |
| `union_candle_score_h1` | clean | forward | sort | 5.46% | 7.86% | 43 |  |
| `union_candle_score_h1` | clean | tune | weather | -11.44% | -6.42% | 137 |  |
| `union_candle_score_h1` | clean | forward | weather | -4.14% | -5.97% | 59 |  |
| `union_candle_score_h1` | clean | tune | hold_rule | 1.24% | -1.33% | 85 |  |
| `union_candle_score_h1` | clean | forward | hold_rule | -0.40% | -1.04% | 46 |  |
| `union_candle_score_h3` | clean | tune | must_have | 0.00% | 0.00% | 55 |  |
| `union_candle_score_h3` | clean | forward | must_have | 0.00% | 0.00% | 25 | yes |
| `union_candle_score_h3` | clean | tune | must_not | 0.37% | 0.71% | 56 |  |
| `union_candle_score_h3` | clean | forward | must_not | 6.60% | 6.07% | 29 | yes |
| `union_candle_score_h3` | clean | tune | sort | 13.17% | 8.42% | 57 |  |
| `union_candle_score_h3` | clean | forward | sort | 20.58% | 12.00% | 25 | yes |
| `union_candle_score_h3` | clean | tune | weather | 2.93% | 3.54% | 96 |  |
| `union_candle_score_h3` | clean | forward | weather | 5.80% | -2.30% | 54 |  |
| `union_candle_score_h3` | clean | tune | hold_rule | 0.51% | 0.71% | 56 |  |
| `union_candle_score_h3` | clean | forward | hold_rule | 5.80% | 3.26% | 27 | yes |
| `union_cond_h1` | clean | tune | must_have | 0.00% | 0.00% | 82 |  |
| `union_cond_h1` | clean | forward | must_have | 0.00% | 0.00% | 46 |  |
| `union_cond_h1` | clean | tune | must_not | 0.54% | 1.22% | 82 |  |
| `union_cond_h1` | clean | forward | must_not | -0.20% | 0.00% | 46 |  |
| `union_cond_h1` | clean | tune | sort | -0.66% | -7.23% | 80 |  |
| `union_cond_h1` | clean | forward | sort | -0.28% | 0.20% | 43 |  |
| `union_cond_h1` | clean | tune | weather | 0.99% | -0.98% | 145 |  |
| `union_cond_h1` | clean | forward | weather | -0.09% | -8.70% | 69 |  |
| `union_cond_h1` | clean | tune | hold_rule | 0.11% | 0.65% | 86 |  |
| `union_cond_h1` | clean | forward | hold_rule | 0.01% | 0.00% | 46 |  |
| `union_cond_h3` | clean | tune | must_have | 0.00% | 0.00% | 58 |  |
| `union_cond_h3` | clean | forward | must_have | 0.00% | 0.00% | 30 |  |
| `union_cond_h3` | clean | tune | must_not | 0.00% | 0.00% | 58 |  |
| `union_cond_h3` | clean | forward | must_not | -0.06% | 0.00% | 30 |  |
| `union_cond_h3` | clean | tune | sort | 5.90% | 2.90% | 57 |  |
| `union_cond_h3` | clean | forward | sort | 2.92% | -12.00% | 25 | yes |
| `union_cond_h3` | clean | tune | weather | -7.13% | 3.31% | 77 |  |
| `union_cond_h3` | clean | forward | weather | -4.11% | -10.61% | 49 |  |
| `union_cond_h3` | clean | tune | hold_rule | 0.00% | 0.00% | 58 |  |
| `union_cond_h3` | clean | forward | hold_rule | -0.00% | -3.33% | 30 |  |
| `union_cond_n4_h3` | clean | tune | must_have | 0.00% | 0.00% | 26 | yes |
| `union_cond_n4_h3` | clean | forward | must_have | 0.00% | 0.00% | 20 | yes |
| `union_cond_n4_h3` | clean | tune | must_not | 0.00% | 0.00% | 26 | yes |
| `union_cond_n4_h3` | clean | forward | must_not | 0.00% | 0.00% | 20 | yes |
| `union_cond_n4_h3` | clean | tune | sort | 5.52% | -13.08% | 30 |  |
| `union_cond_n4_h3` | clean | forward | sort | 12.38% | 10.00% | 20 | yes |
| `union_cond_n4_h3` | clean | tune | weather | -23.60% | -4.90% | 44 |  |
| `union_cond_n4_h3` | clean | forward | weather | 4.08% | 1.29% | 31 |  |
| `union_cond_n4_h3` | clean | tune | hold_rule | 0.00% | 0.00% | 26 | yes |
| `union_cond_n4_h3` | clean | forward | hold_rule | 0.06% | 1.90% | 21 | yes |
| `union_hot_n12_h1` | clean | tune | must_have | 0.00% | 0.00% | 111 |  |
| `union_hot_n12_h1` | clean | forward | must_have | 0.00% | 0.00% | 54 |  |
| `union_hot_n12_h1` | clean | tune | must_not | -2.14% | -2.28% | 112 |  |
| `union_hot_n12_h1` | clean | forward | must_not | -1.96% | -0.63% | 53 |  |
| `union_hot_n12_h1` | clean | tune | sort | 0.52% | -2.47% | 118 |  |
| `union_hot_n12_h1` | clean | forward | sort | 12.24% | 6.21% | 59 |  |
| `union_hot_n12_h1` | clean | tune | weather | -4.32% | -1.89% | 189 |  |
| `union_hot_n12_h1` | clean | forward | weather | -1.28% | -8.64% | 81 |  |
| `union_hot_n12_h1` | clean | tune | hold_rule | 1.12% | 0.08% | 118 |  |
| `union_hot_n12_h1` | clean | forward | hold_rule | 8.88% | 3.83% | 61 |  |
| `union_hot_n4_h1` | clean | tune | must_have | 0.00% | 0.00% | 34 |  |
| `union_hot_n4_h1` | clean | forward | must_have | 0.00% | 0.00% | 25 | yes |
| `union_hot_n4_h1` | clean | tune | must_not | 0.86% | 0.00% | 34 |  |
| `union_hot_n4_h1` | clean | forward | must_not | -5.75% | 1.69% | 26 | yes |
| `union_hot_n4_h1` | clean | tune | sort | 16.52% | -1.82% | 42 |  |
| `union_hot_n4_h1` | clean | forward | sort | 38.80% | 14.37% | 27 | yes |
| `union_hot_n4_h1` | clean | tune | weather | -25.00% | 0.48% | 61 |  |
| `union_hot_n4_h1` | clean | forward | weather | -12.81% | -9.12% | 32 |  |
| `union_hot_n4_h1` | clean | tune | hold_rule | 0.40% | -3.82% | 37 |  |
| `union_hot_n4_h1` | clean | forward | hold_rule | 23.52% | 3.26% | 27 | yes |
| `union_hot_score_h1` | clean | tune | must_have | 0.00% | 0.00% | 73 |  |
| `union_hot_score_h1` | clean | forward | must_have | 0.00% | 0.00% | 40 |  |
| `union_hot_score_h1` | clean | tune | must_not | -5.44% | -1.37% | 73 |  |
| `union_hot_score_h1` | clean | forward | must_not | -5.41% | -4.02% | 41 |  |
| `union_hot_score_h1` | clean | tune | sort | -0.35% | -1.04% | 80 |  |
| `union_hot_score_h1` | clean | forward | sort | 19.05% | 4.77% | 43 |  |
| `union_hot_score_h1` | clean | tune | weather | -10.63% | -2.79% | 125 |  |
| `union_hot_score_h1` | clean | forward | weather | -2.91% | -8.86% | 57 |  |
| `union_hot_score_h1` | clean | tune | hold_rule | 1.43% | 0.33% | 78 |  |
| `union_hot_score_h1` | clean | forward | hold_rule | 13.05% | 3.89% | 45 |  |
| `union_hot_score_h3` | clean | tune | must_have | 0.00% | 0.00% | 57 |  |
| `union_hot_score_h3` | clean | forward | must_have | 0.00% | 0.00% | 31 |  |
| `union_hot_score_h3` | clean | tune | must_not | 0.26% | -1.75% | 57 |  |
| `union_hot_score_h3` | clean | forward | must_not | 0.08% | -0.97% | 30 |  |
| `union_hot_score_h3` | clean | tune | sort | 13.94% | 5.26% | 57 |  |
| `union_hot_score_h3` | clean | forward | sort | 18.77% | -2.97% | 25 | yes |
| `union_hot_score_h3` | clean | tune | weather | -15.42% | -4.02% | 93 |  |
| `union_hot_score_h3` | clean | forward | weather | 0.92% | -3.70% | 55 |  |
| `union_hot_score_h3` | clean | tune | hold_rule | -1.13% | -2.14% | 59 |  |
| `union_hot_score_h3` | clean | forward | hold_rule | 5.92% | 4.03% | 20 | yes |
| `union_ret_5_h1` | clean | tune | must_have | 0.00% | 0.00% | 73 |  |
| `union_ret_5_h1` | clean | forward | must_have | 0.00% | 0.00% | 43 |  |
| `union_ret_5_h1` | clean | tune | must_not | -1.97% | -2.83% | 75 |  |
| `union_ret_5_h1` | clean | forward | must_not | -4.01% | -3.27% | 42 |  |
| `union_ret_5_h1` | clean | tune | sort | 3.36% | -2.41% | 80 |  |
| `union_ret_5_h1` | clean | forward | sort | 16.45% | 6.98% | 43 |  |
| `union_ret_5_h1` | clean | tune | weather | -6.15% | -3.89% | 132 |  |
| `union_ret_5_h1` | clean | forward | weather | -8.53% | -10.33% | 61 |  |
| `union_ret_5_h1` | clean | tune | hold_rule | 0.97% | -1.62% | 77 |  |
| `union_ret_5_h1` | clean | forward | hold_rule | 12.93% | 1.65% | 45 |  |
| `union_ret_5_h3` | clean | tune | must_have | 0.00% | 0.00% | 61 |  |
| `union_ret_5_h3` | clean | forward | must_have | 0.00% | 0.00% | 23 | yes |
| `union_ret_5_h3` | clean | tune | must_not | 1.71% | 0.00% | 61 |  |
| `union_ret_5_h3` | clean | forward | must_not | -2.60% | 0.43% | 20 | yes |
| `union_ret_5_h3` | clean | tune | sort | 18.65% | 7.77% | 57 |  |
| `union_ret_5_h3` | clean | forward | sort | 19.41% | -1.57% | 25 | yes |
| `union_ret_5_h3` | clean | tune | weather | -11.02% | -4.47% | 105 |  |
| `union_ret_5_h3` | clean | forward | weather | 4.21% | -11.38% | 55 |  |
| `union_ret_5_h3` | clean | tune | hold_rule | -2.33% | -1.93% | 63 |  |
| `union_ret_5_h3` | clean | forward | hold_rule | 6.65% | 1.27% | 24 | yes |
| `union_w_hot_candle_h1` | clean | tune | must_have | 0.00% | 0.00% | 75 |  |
| `union_w_hot_candle_h1` | clean | forward | must_have | 0.00% | 0.00% | 42 |  |
| `union_w_hot_candle_h1` | clean | tune | must_not | -1.50% | -0.67% | 76 |  |
| `union_w_hot_candle_h1` | clean | forward | must_not | -0.74% | -2.02% | 40 |  |
| `union_w_hot_candle_h1` | clean | tune | sort | 4.02% | 3.08% | 80 |  |
| `union_w_hot_candle_h1` | clean | forward | sort | 19.60% | 10.24% | 43 |  |
| `union_w_hot_candle_h1` | clean | tune | weather | -14.36% | -3.42% | 127 |  |
| `union_w_hot_candle_h1` | clean | forward | weather | -3.82% | -8.70% | 61 |  |
| `union_w_hot_candle_h1` | clean | tune | hold_rule | 0.52% | -0.05% | 81 |  |
| `union_w_hot_candle_h1` | clean | forward | hold_rule | 13.19% | 2.70% | 45 |  |
| `union_w_hot_candle_h3` | clean | tune | must_have | 0.00% | 0.00% | 59 |  |
| `union_w_hot_candle_h3` | clean | forward | must_have | 0.00% | 0.00% | 27 | yes |
| `union_w_hot_candle_h3` | clean | tune | must_not | -0.70% | -0.31% | 61 |  |
| `union_w_hot_candle_h3` | clean | forward | must_not | -0.01% | 2.59% | 30 |  |
| `union_w_hot_candle_h3` | clean | tune | sort | 11.76% | 9.10% | 57 |  |
| `union_w_hot_candle_h3` | clean | forward | sort | 16.77% | -6.07% | 25 | yes |
| `union_w_hot_candle_h3` | clean | tune | weather | -12.04% | -4.68% | 97 |  |
| `union_w_hot_candle_h3` | clean | forward | weather | -4.34% | -10.61% | 52 |  |
| `union_w_hot_candle_h3` | clean | tune | hold_rule | 0.10% | 0.00% | 59 |  |
| `union_w_hot_candle_h3` | clean | forward | hold_rule | 4.73% | -4.84% | 26 | yes |
| `union_w_hot_cond_h1` | clean | tune | must_have | 0.00% | 0.00% | 77 |  |
| `union_w_hot_cond_h1` | clean | forward | must_have | 0.00% | 0.00% | 43 |  |
| `union_w_hot_cond_h1` | clean | tune | must_not | -6.34% | -1.30% | 77 |  |
| `union_w_hot_cond_h1` | clean | forward | must_not | -5.33% | -3.32% | 42 |  |
| `union_w_hot_cond_h1` | clean | tune | sort | 0.64% | 0.50% | 80 |  |
| `union_w_hot_cond_h1` | clean | forward | sort | 22.33% | 9.30% | 43 |  |
| `union_w_hot_cond_h1` | clean | tune | weather | -7.31% | 1.30% | 132 |  |
| `union_w_hot_cond_h1` | clean | forward | weather | -3.66% | -2.40% | 62 |  |
| `union_w_hot_cond_h1` | clean | tune | hold_rule | 0.18% | -2.00% | 80 |  |
| `union_w_hot_cond_h1` | clean | forward | hold_rule | 13.29% | 1.76% | 45 |  |
| `union_w_hot_cond_h3` | clean | tune | must_have | 0.00% | 0.00% | 58 |  |
| `union_w_hot_cond_h3` | clean | forward | must_have | 0.00% | 0.00% | 31 |  |
| `union_w_hot_cond_h3` | clean | tune | must_not | -0.03% | -1.72% | 58 |  |
| `union_w_hot_cond_h3` | clean | forward | must_not | -0.15% | 1.22% | 29 | yes |
| `union_w_hot_cond_h3` | clean | tune | sort | 12.15% | 2.90% | 57 |  |
| `union_w_hot_cond_h3` | clean | forward | sort | 24.64% | 0.26% | 25 | yes |
| `union_w_hot_cond_h3` | clean | tune | weather | -11.55% | -1.52% | 100 |  |
| `union_w_hot_cond_h3` | clean | forward | weather | 8.72% | -1.70% | 53 |  |
| `union_w_hot_cond_h3` | clean | tune | hold_rule | -0.92% | -1.72% | 58 |  |
| `union_w_hot_cond_h3` | clean | forward | hold_rule | 6.08% | -2.12% | 32 |  |
| `union_hot_n4_holdup` | clean | tune | must_have | 0.00% | 0.00% | 30 |  |
| `union_hot_n4_holdup` | clean | forward | must_have | 0.00% | 0.00% | 22 | yes |
| `union_hot_n4_holdup` | clean | tune | must_not | 4.64% | 1.38% | 29 | yes |
| `union_hot_n4_holdup` | clean | forward | must_not | 2.71% | -0.38% | 24 | yes |
| `union_hot_n4_holdup` | clean | tune | sort | 37.52% | 11.35% | 37 |  |
| `union_hot_n4_holdup` | clean | forward | sort | 47.33% | 9.45% | 25 | yes |
| `union_hot_n4_holdup` | clean | tune | weather | -21.21% | 4.64% | 56 |  |
| `union_hot_n4_holdup` | clean | forward | weather | -1.60% | -7.88% | 30 |  |
| `union_hot_n4_holdup` | clean | tune | hold_rule | 19.89% | 3.24% | 37 |  |
| `union_hot_n4_holdup` | clean | forward | hold_rule | 37.53% | 4.71% | 27 | yes |
| `probable_h3` | yahoo | tune | must_have | 0.00% | 0.00% | 50 |  |
| `probable_h3` | yahoo | forward | must_have | 0.00% | 0.00% | 20 | yes |
| `probable_h3` | yahoo | tune | must_not | 0.00% | 0.00% | 50 |  |
| `probable_h3` | yahoo | forward | must_not | 0.00% | 0.00% | 20 | yes |
| `probable_h3` | yahoo | tune | sort | -0.00% | 0.00% | 50 |  |
| `probable_h3` | yahoo | forward | sort | -0.00% | 0.00% | 20 | yes |
| `probable_h3` | yahoo | tune | weather | -0.87% | -4.11% | 108 |  |
| `probable_h3` | yahoo | forward | weather | 6.14% | 0.00% | 40 |  |
| `probable_h3` | yahoo | tune | hold_rule | -0.50% | -0.69% | 52 |  |
| `probable_h3` | yahoo | forward | hold_rule | 1.31% | -3.57% | 21 | yes |
| `probable_h5` | yahoo | tune | must_have | 0.00% | 0.00% | 49 |  |
| `probable_h5` | yahoo | forward | must_have | 0.00% | 0.00% | 15 | yes |
| `probable_h5` | yahoo | tune | must_not | 0.00% | 0.00% | 49 |  |
| `probable_h5` | yahoo | forward | must_not | 0.00% | 0.00% | 15 | yes |
| `probable_h5` | yahoo | tune | sort | 0.00% | 0.00% | 49 |  |
| `probable_h5` | yahoo | forward | sort | 0.00% | 0.00% | 15 | yes |
| `probable_h5` | yahoo | tune | weather | 0.60% | -12.86% | 70 |  |
| `probable_h5` | yahoo | forward | weather | -13.91% | -0.83% | 40 |  |
| `probable_h5` | yahoo | tune | hold_rule | 0.00% | 0.00% | 49 |  |
| `probable_h5` | yahoo | forward | hold_rule | 0.03% | 0.00% | 15 | yes |
| `short_extended_h3` | yahoo | tune | must_have | -1.26% | -3.47% | 74 |  |
| `short_extended_h3` | yahoo | forward | must_have | -16.17% | -4.86% | 37 |  |
| `short_extended_h3` | yahoo | tune | must_not | 0.00% | 0.00% | 65 |  |
| `short_extended_h3` | yahoo | forward | must_not | 0.00% | 0.00% | 20 | yes |
| `short_extended_h3` | yahoo | tune | sort | -13.27% | -5.84% | 69 |  |
| `short_extended_h3` | yahoo | forward | sort | -11.95% | -8.18% | 22 | yes |
| `short_extended_h3` | yahoo | tune | weather | 7.93% | -2.77% | 100 |  |
| `short_extended_h3` | yahoo | forward | weather | -7.27% | -3.16% | 38 |  |
| `short_extended_h3` | yahoo | tune | hold_rule | 0.47% | 2.17% | 68 |  |
| `short_extended_h3` | yahoo | forward | hold_rule | -3.24% | -1.90% | 21 | yes |
| `union_candle_score_h1` | yahoo | tune | must_have | 0.00% | 0.00% | 81 |  |
| `union_candle_score_h1` | yahoo | forward | must_have | 0.00% | 0.00% | 42 |  |
| `union_candle_score_h1` | yahoo | tune | must_not | -1.39% | 0.00% | 81 |  |
| `union_candle_score_h1` | yahoo | forward | must_not | 1.90% | -0.93% | 41 |  |
| `union_candle_score_h1` | yahoo | tune | sort | 6.48% | -3.97% | 85 |  |
| `union_candle_score_h1` | yahoo | forward | sort | 4.75% | 6.28% | 44 |  |
| `union_candle_score_h1` | yahoo | tune | weather | -11.51% | -6.40% | 140 |  |
| `union_candle_score_h1` | yahoo | forward | weather | -2.61% | -5.24% | 60 |  |
| `union_candle_score_h1` | yahoo | tune | hold_rule | 1.16% | -1.30% | 88 |  |
| `union_candle_score_h1` | yahoo | forward | hold_rule | -0.47% | -1.04% | 46 |  |
| `union_candle_score_h3` | yahoo | tune | must_have | 0.00% | 0.00% | 55 |  |
| `union_candle_score_h3` | yahoo | forward | must_have | 0.00% | 0.00% | 27 | yes |
| `union_candle_score_h3` | yahoo | tune | must_not | 0.34% | 0.00% | 55 |  |
| `union_candle_score_h3` | yahoo | forward | must_not | 6.58% | 1.46% | 28 | yes |
| `union_candle_score_h3` | yahoo | tune | sort | 15.97% | 6.67% | 63 |  |
| `union_candle_score_h3` | yahoo | forward | sort | 20.51% | 12.74% | 25 | yes |
| `union_candle_score_h3` | yahoo | tune | weather | 5.25% | 4.58% | 96 |  |
| `union_candle_score_h3` | yahoo | forward | weather | 7.77% | -7.26% | 50 |  |
| `union_candle_score_h3` | yahoo | tune | hold_rule | 0.42% | 0.00% | 55 |  |
| `union_candle_score_h3` | yahoo | forward | hold_rule | 5.74% | 0.00% | 27 | yes |
| `union_cond_h1` | yahoo | tune | must_have | 0.00% | 0.00% | 83 |  |
| `union_cond_h1` | yahoo | forward | must_have | 0.00% | 0.00% | 45 |  |
| `union_cond_h1` | yahoo | tune | must_not | 0.61% | 1.20% | 83 |  |
| `union_cond_h1` | yahoo | forward | must_not | -0.34% | 0.00% | 45 |  |
| `union_cond_h1` | yahoo | tune | sort | -0.80% | -6.15% | 85 |  |
| `union_cond_h1` | yahoo | forward | sort | -0.86% | -0.71% | 44 |  |
| `union_cond_h1` | yahoo | tune | weather | 0.95% | -1.17% | 146 |  |
| `union_cond_h1` | yahoo | forward | weather | -0.10% | -8.59% | 68 |  |
| `union_cond_h1` | yahoo | tune | hold_rule | 0.12% | 0.62% | 87 |  |
| `union_cond_h1` | yahoo | forward | hold_rule | -0.06% | 0.00% | 45 |  |
| `union_cond_h3` | yahoo | tune | must_have | 0.00% | 0.00% | 60 |  |
| `union_cond_h3` | yahoo | forward | must_have | 0.00% | 0.00% | 29 | yes |
| `union_cond_h3` | yahoo | tune | must_not | 0.00% | 0.00% | 60 |  |
| `union_cond_h3` | yahoo | forward | must_not | -0.06% | 0.00% | 29 | yes |
| `union_cond_h3` | yahoo | tune | sort | 7.78% | 0.00% | 63 |  |
| `union_cond_h3` | yahoo | forward | sort | 2.91% | -7.31% | 25 | yes |
| `union_cond_h3` | yahoo | tune | weather | -7.13% | 2.56% | 78 |  |
| `union_cond_h3` | yahoo | forward | weather | -4.29% | -6.97% | 47 |  |
| `union_cond_h3` | yahoo | tune | hold_rule | 0.00% | 0.00% | 60 |  |
| `union_cond_h3` | yahoo | forward | hold_rule | -0.00% | -3.45% | 29 | yes |
| `union_cond_n4_h3` | yahoo | tune | must_have | 0.00% | 0.00% | 27 | yes |
| `union_cond_n4_h3` | yahoo | forward | must_have | 0.00% | 0.00% | 20 | yes |
| `union_cond_n4_h3` | yahoo | tune | must_not | 0.00% | 0.00% | 27 | yes |
| `union_cond_n4_h3` | yahoo | forward | must_not | 0.00% | 0.00% | 20 | yes |
| `union_cond_n4_h3` | yahoo | tune | sort | 8.17% | -12.78% | 31 |  |
| `union_cond_n4_h3` | yahoo | forward | sort | 12.06% | 22.35% | 17 | yes |
| `union_cond_n4_h3` | yahoo | tune | weather | -23.60% | -5.19% | 45 |  |
| `union_cond_n4_h3` | yahoo | forward | weather | 3.01% | 3.33% | 30 |  |
| `union_cond_n4_h3` | yahoo | tune | hold_rule | 0.00% | 0.00% | 27 | yes |
| `union_cond_n4_h3` | yahoo | forward | hold_rule | 0.06% | 1.90% | 21 | yes |
| `union_hot_n12_h1` | yahoo | tune | must_have | 0.00% | 0.00% | 120 |  |
| `union_hot_n12_h1` | yahoo | forward | must_have | 0.00% | 0.00% | 54 |  |
| `union_hot_n12_h1` | yahoo | tune | must_not | -2.06% | -2.50% | 120 |  |
| `union_hot_n12_h1` | yahoo | forward | must_not | -2.21% | -0.66% | 53 |  |
| `union_hot_n12_h1` | yahoo | tune | sort | 2.38% | -0.97% | 124 |  |
| `union_hot_n12_h1` | yahoo | forward | sort | 11.60% | 6.37% | 59 |  |
| `union_hot_n12_h1` | yahoo | tune | weather | -85.38% | -2.74% | 199 |  |
| `union_hot_n12_h1` | yahoo | forward | weather | -2.69% | -9.39% | 83 |  |
| `union_hot_n12_h1` | yahoo | tune | hold_rule | 0.73% | -0.31% | 128 |  |
| `union_hot_n12_h1` | yahoo | forward | hold_rule | 8.54% | 4.04% | 61 |  |
| `union_hot_n4_h1` | yahoo | tune | must_have | 0.00% | 0.00% | 40 |  |
| `union_hot_n4_h1` | yahoo | forward | must_have | 0.00% | 0.00% | 26 | yes |
| `union_hot_n4_h1` | yahoo | tune | must_not | 1.44% | -1.28% | 39 |  |
| `union_hot_n4_h1` | yahoo | forward | must_not | -5.96% | 1.71% | 27 | yes |
| `union_hot_n4_h1` | yahoo | tune | sort | 20.27% | -3.49% | 43 |  |
| `union_hot_n4_h1` | yahoo | forward | sort | 39.56% | 16.52% | 27 | yes |
| `union_hot_n4_h1` | yahoo | tune | weather | -25.34% | -0.75% | 67 |  |
| `union_hot_n4_h1` | yahoo | forward | weather | -17.23% | -9.73% | 34 |  |
| `union_hot_n4_h1` | yahoo | tune | hold_rule | -0.83% | -4.55% | 44 |  |
| `union_hot_n4_h1` | yahoo | forward | hold_rule | 22.89% | 3.30% | 28 | yes |
| `union_hot_score_h1` | yahoo | tune | must_have | 0.00% | 0.00% | 82 |  |
| `union_hot_score_h1` | yahoo | forward | must_have | 0.00% | 0.00% | 40 |  |
| `union_hot_score_h1` | yahoo | tune | must_not | -4.85% | -1.81% | 81 |  |
| `union_hot_score_h1` | yahoo | forward | must_not | -5.25% | -3.96% | 41 |  |
| `union_hot_score_h1` | yahoo | tune | sort | 3.33% | 1.64% | 85 |  |
| `union_hot_score_h1` | yahoo | forward | sort | 18.93% | 5.68% | 44 |  |
| `union_hot_score_h1` | yahoo | tune | weather | -138.96% | -2.91% | 134 |  |
| `union_hot_score_h1` | yahoo | forward | weather | -3.57% | -8.26% | 59 |  |
| `union_hot_score_h1` | yahoo | tune | hold_rule | 0.78% | -0.25% | 88 |  |
| `union_hot_score_h1` | yahoo | forward | hold_rule | 12.70% | 4.17% | 45 |  |
| `union_hot_score_h3` | yahoo | tune | must_have | 0.00% | 0.00% | 58 |  |
| `union_hot_score_h3` | yahoo | forward | must_have | 0.00% | 0.00% | 19 | yes |
| `union_hot_score_h3` | yahoo | tune | must_not | 0.39% | -0.67% | 57 |  |
| `union_hot_score_h3` | yahoo | forward | must_not | 0.02% | 0.00% | 19 | yes |
| `union_hot_score_h3` | yahoo | tune | sort | 16.91% | 4.60% | 63 |  |
| `union_hot_score_h3` | yahoo | forward | sort | 18.58% | -1.68% | 25 | yes |
| `union_hot_score_h3` | yahoo | tune | weather | -26.31% | -5.37% | 97 |  |
| `union_hot_score_h3` | yahoo | forward | weather | -8.76% | -8.87% | 54 |  |
| `union_hot_score_h3` | yahoo | tune | hold_rule | 0.72% | -4.44% | 59 |  |
| `union_hot_score_h3` | yahoo | forward | hold_rule | 5.42% | -2.72% | 31 |  |
| `union_ret_5_h1` | yahoo | tune | must_have | 0.00% | 0.00% | 83 |  |
| `union_ret_5_h1` | yahoo | forward | must_have | 0.00% | 0.00% | 43 |  |
| `union_ret_5_h1` | yahoo | tune | must_not | -2.03% | -1.85% | 84 |  |
| `union_ret_5_h1` | yahoo | forward | must_not | -4.07% | -3.32% | 42 |  |
| `union_ret_5_h1` | yahoo | tune | sort | 4.69% | -0.13% | 85 |  |
| `union_ret_5_h1` | yahoo | forward | sort | 16.42% | 7.72% | 44 |  |
| `union_ret_5_h1` | yahoo | tune | weather | -6.16% | -3.31% | 142 |  |
| `union_ret_5_h1` | yahoo | forward | weather | -8.46% | -9.67% | 63 |  |
| `union_ret_5_h1` | yahoo | tune | hold_rule | 0.26% | -1.40% | 87 |  |
| `union_ret_5_h1` | yahoo | forward | hold_rule | 12.51% | 1.76% | 45 |  |
| `union_ret_5_h3` | yahoo | tune | must_have | 0.00% | 0.00% | 61 |  |
| `union_ret_5_h3` | yahoo | forward | must_have | 0.00% | 0.00% | 23 | yes |
| `union_ret_5_h3` | yahoo | tune | must_not | 1.57% | 1.41% | 63 |  |
| `union_ret_5_h3` | yahoo | forward | must_not | -2.56% | 4.12% | 19 | yes |
| `union_ret_5_h3` | yahoo | tune | sort | 20.19% | 10.93% | 63 |  |
| `union_ret_5_h3` | yahoo | forward | sort | 19.46% | 2.43% | 25 | yes |
| `union_ret_5_h3` | yahoo | tune | weather | -20.20% | -2.55% | 94 |  |
| `union_ret_5_h3` | yahoo | forward | weather | 2.17% | -12.70% | 51 |  |
| `union_ret_5_h3` | yahoo | tune | hold_rule | -3.05% | -1.05% | 64 |  |
| `union_ret_5_h3` | yahoo | forward | hold_rule | 6.65% | -2.90% | 30 |  |
| `union_w_hot_candle_h1` | yahoo | tune | must_have | 0.00% | 0.00% | 81 |  |
| `union_w_hot_candle_h1` | yahoo | forward | must_have | 0.00% | 0.00% | 43 |  |
| `union_w_hot_candle_h1` | yahoo | tune | must_not | -0.43% | -0.60% | 80 |  |
| `union_w_hot_candle_h1` | yahoo | forward | must_not | -0.79% | -2.04% | 41 |  |
| `union_w_hot_candle_h1` | yahoo | tune | sort | 3.84% | 3.44% | 85 |  |
| `union_w_hot_candle_h1` | yahoo | forward | sort | 19.41% | 10.04% | 44 |  |
| `union_w_hot_candle_h1` | yahoo | tune | weather | -148.55% | -4.48% | 133 |  |
| `union_w_hot_candle_h1` | yahoo | forward | weather | -4.65% | -8.14% | 64 |  |
| `union_w_hot_candle_h1` | yahoo | tune | hold_rule | -0.14% | -0.72% | 88 |  |
| `union_w_hot_candle_h1` | yahoo | forward | hold_rule | 12.92% | 2.73% | 46 |  |
| `union_w_hot_candle_h3` | yahoo | tune | must_have | 0.00% | 0.00% | 60 |  |
| `union_w_hot_candle_h3` | yahoo | forward | must_have | 0.00% | 0.00% | 28 | yes |
| `union_w_hot_candle_h3` | yahoo | tune | must_not | -0.75% | 0.99% | 59 |  |
| `union_w_hot_candle_h3` | yahoo | forward | must_not | 0.00% | 5.24% | 30 |  |
| `union_w_hot_candle_h3` | yahoo | tune | sort | 13.71% | 8.33% | 63 |  |
| `union_w_hot_candle_h3` | yahoo | forward | sort | 16.83% | 0.57% | 25 | yes |
| `union_w_hot_candle_h3` | yahoo | tune | weather | -14.42% | -5.65% | 93 |  |
| `union_w_hot_candle_h3` | yahoo | forward | weather | -9.02% | -11.81% | 52 |  |
| `union_w_hot_candle_h3` | yahoo | tune | hold_rule | 0.37% | -1.19% | 56 |  |
| `union_w_hot_candle_h3` | yahoo | forward | hold_rule | 4.64% | -1.43% | 30 |  |
| `union_w_hot_cond_h1` | yahoo | tune | must_have | 0.00% | 0.00% | 84 |  |
| `union_w_hot_cond_h1` | yahoo | forward | must_have | 0.00% | 0.00% | 43 |  |
| `union_w_hot_cond_h1` | yahoo | tune | must_not | -5.50% | -1.76% | 83 |  |
| `union_w_hot_cond_h1` | yahoo | forward | must_not | -5.28% | -3.38% | 42 |  |
| `union_w_hot_cond_h1` | yahoo | tune | sort | 2.74% | 1.72% | 85 |  |
| `union_w_hot_cond_h1` | yahoo | forward | sort | 22.36% | 10.04% | 44 |  |
| `union_w_hot_cond_h1` | yahoo | tune | weather | -133.02% | -0.33% | 139 |  |
| `union_w_hot_cond_h1` | yahoo | forward | weather | -6.61% | -4.17% | 63 |  |
| `union_w_hot_cond_h1` | yahoo | tune | hold_rule | -0.36% | -2.44% | 88 |  |
| `union_w_hot_cond_h1` | yahoo | forward | hold_rule | 13.07% | 1.86% | 45 |  |
| `union_w_hot_cond_h3` | yahoo | tune | must_have | 0.00% | 0.00% | 61 |  |
| `union_w_hot_cond_h3` | yahoo | forward | must_have | 0.00% | 0.00% | 29 | yes |
| `union_w_hot_cond_h3` | yahoo | tune | must_not | -0.14% | -1.98% | 63 |  |
| `union_w_hot_cond_h3` | yahoo | forward | must_not | -0.11% | -2.04% | 27 | yes |
| `union_w_hot_cond_h3` | yahoo | tune | sort | 14.51% | 4.37% | 63 |  |
| `union_w_hot_cond_h3` | yahoo | forward | sort | 24.77% | -0.41% | 25 | yes |
| `union_w_hot_cond_h3` | yahoo | tune | weather | -18.69% | -1.51% | 102 |  |
| `union_w_hot_cond_h3` | yahoo | forward | weather | 4.80% | -10.88% | 52 |  |
| `union_w_hot_cond_h3` | yahoo | tune | hold_rule | -1.50% | -2.30% | 60 |  |
| `union_w_hot_cond_h3` | yahoo | forward | hold_rule | 6.09% | -3.66% | 32 |  |
| `union_hot_n4_holdup` | yahoo | tune | must_have | 0.00% | 0.00% | 31 |  |
| `union_hot_n4_holdup` | yahoo | forward | must_have | 0.00% | 0.00% | 24 | yes |
| `union_hot_n4_holdup` | yahoo | tune | must_not | 4.50% | 1.18% | 30 |  |
| `union_hot_n4_holdup` | yahoo | forward | must_not | 2.71% | -1.99% | 23 | yes |
| `union_hot_n4_holdup` | yahoo | tune | sort | 39.40% | 21.27% | 37 |  |
| `union_hot_n4_holdup` | yahoo | forward | sort | 49.21% | 9.83% | 25 | yes |
| `union_hot_n4_holdup` | yahoo | tune | weather | -22.64% | 7.37% | 56 |  |
| `union_hot_n4_holdup` | yahoo | forward | weather | -4.20% | -14.17% | 30 |  |
| `union_hot_n4_holdup` | yahoo | tune | hold_rule | 19.02% | 9.97% | 44 |  |
| `union_hot_n4_holdup` | yahoo | forward | hold_rule | 38.12% | 2.98% | 28 | yes |

## Pair interactions

Gain = combined effect minus the two single effects. Zero means the two removals added. This is not a ranking.

| recipe | tape | window | pair | compound gain | win-rate gain |
| --- | --- | --- | --- | ---: | ---: |
| `probable_h3` | clean | tune | must_have+must_not | 0.00% | 0.00% |
| `probable_h3` | clean | forward | must_have+must_not | 0.00% | 0.00% |
| `probable_h3` | clean | tune | must_have+sort | 0.00% | 0.00% |
| `probable_h3` | clean | forward | must_have+sort | 0.00% | 0.00% |
| `probable_h3` | clean | tune | must_have+weather | 0.00% | -0.00% |
| `probable_h3` | clean | forward | must_have+weather | 0.00% | 0.00% |
| `probable_h3` | clean | tune | must_have+hold_rule | 0.00% | -0.00% |
| `probable_h3` | clean | forward | must_have+hold_rule | 0.00% | -0.00% |
| `probable_h3` | clean | tune | must_not+sort | 0.00% | 0.00% |
| `probable_h3` | clean | forward | must_not+sort | 0.00% | 0.00% |
| `probable_h3` | clean | tune | must_not+weather | 0.00% | -0.00% |
| `probable_h3` | clean | forward | must_not+weather | 0.00% | 0.00% |
| `probable_h3` | clean | tune | must_not+hold_rule | 0.00% | -0.00% |
| `probable_h3` | clean | forward | must_not+hold_rule | 0.00% | -0.00% |
| `probable_h3` | clean | tune | sort+weather | -0.00% | -0.00% |
| `probable_h3` | clean | forward | sort+weather | -0.00% | 0.00% |
| `probable_h3` | clean | tune | sort+hold_rule | -0.00% | -0.00% |
| `probable_h3` | clean | forward | sort+hold_rule | -0.00% | -0.00% |
| `probable_h3` | clean | tune | weather+hold_rule | 0.43% | 1.61% |
| `probable_h3` | clean | forward | weather+hold_rule | -1.38% | 1.62% |
| `probable_h5` | clean | tune | must_have+must_not | 0.00% | 0.00% |
| `probable_h5` | clean | forward | must_have+must_not | 0.00% | 0.00% |
| `probable_h5` | clean | tune | must_have+sort | 0.00% | 0.00% |
| `probable_h5` | clean | forward | must_have+sort | 0.00% | 0.00% |
| `probable_h5` | clean | tune | must_have+weather | 0.00% | 0.00% |
| `probable_h5` | clean | forward | must_have+weather | 0.00% | 0.00% |
| `probable_h5` | clean | tune | must_have+hold_rule | 0.00% | 0.00% |
| `probable_h5` | clean | forward | must_have+hold_rule | 0.00% | 0.00% |
| `probable_h5` | clean | tune | must_not+sort | 0.00% | 0.00% |
| `probable_h5` | clean | forward | must_not+sort | 0.00% | 0.00% |
| `probable_h5` | clean | tune | must_not+weather | 0.00% | 0.00% |
| `probable_h5` | clean | forward | must_not+weather | 0.00% | 0.00% |
| `probable_h5` | clean | tune | must_not+hold_rule | 0.00% | 0.00% |
| `probable_h5` | clean | forward | must_not+hold_rule | 0.00% | 0.00% |
| `probable_h5` | clean | tune | sort+weather | 0.00% | 0.00% |
| `probable_h5` | clean | forward | sort+weather | 0.00% | 0.00% |
| `probable_h5` | clean | tune | sort+hold_rule | 0.00% | 0.00% |
| `probable_h5` | clean | forward | sort+hold_rule | 0.00% | 0.00% |
| `probable_h5` | clean | tune | weather+hold_rule | 0.00% | 0.00% |
| `probable_h5` | clean | forward | weather+hold_rule | 0.69% | -1.31% |
| `short_extended_h3` | clean | tune | must_have+must_not | 0.00% | 0.00% |
| `short_extended_h3` | clean | forward | must_have+must_not | 0.00% | 0.00% |
| `short_extended_h3` | clean | tune | must_have+sort | -3.46% | -3.22% |
| `short_extended_h3` | clean | forward | must_have+sort | 17.14% | 18.51% |
| `short_extended_h3` | clean | tune | must_have+weather | 5.65% | 7.17% |
| `short_extended_h3` | clean | forward | must_have+weather | 5.30% | 12.81% |
| `short_extended_h3` | clean | tune | must_have+hold_rule | 0.60% | -1.82% |
| `short_extended_h3` | clean | forward | must_have+hold_rule | 5.20% | 6.07% |
| `short_extended_h3` | clean | tune | must_not+sort | 0.00% | 0.00% |
| `short_extended_h3` | clean | forward | must_not+sort | 0.00% | -0.00% |
| `short_extended_h3` | clean | tune | must_not+weather | 0.00% | -0.00% |
| `short_extended_h3` | clean | forward | must_not+weather | 0.00% | -0.00% |
| `short_extended_h3` | clean | tune | must_not+hold_rule | 0.00% | -0.00% |
| `short_extended_h3` | clean | forward | must_not+hold_rule | 0.00% | -0.00% |
| `short_extended_h3` | clean | tune | sort+weather | -4.08% | -0.79% |
| `short_extended_h3` | clean | forward | sort+weather | 1.82% | 1.77% |
| `short_extended_h3` | clean | tune | sort+hold_rule | -0.66% | -2.54% |
| `short_extended_h3` | clean | forward | sort+hold_rule | 1.83% | 3.25% |
| `short_extended_h3` | clean | tune | weather+hold_rule | -1.22% | -0.35% |
| `short_extended_h3` | clean | forward | weather+hold_rule | 3.68% | 7.48% |
| `union_candle_score_h1` | clean | tune | must_have+must_not | 0.00% | 0.00% |
| `union_candle_score_h1` | clean | forward | must_have+must_not | 0.00% | 0.00% |
| `union_candle_score_h1` | clean | tune | must_have+sort | 0.00% | 0.00% |
| `union_candle_score_h1` | clean | forward | must_have+sort | 0.00% | 0.00% |
| `union_candle_score_h1` | clean | tune | must_have+weather | 0.00% | 0.00% |
| `union_candle_score_h1` | clean | forward | must_have+weather | 0.00% | -0.00% |
| `union_candle_score_h1` | clean | tune | must_have+hold_rule | 0.00% | -0.00% |
| `union_candle_score_h1` | clean | forward | must_have+hold_rule | 0.00% | -0.00% |
| `union_candle_score_h1` | clean | tune | must_not+sort | 7.12% | 6.04% |
| `union_candle_score_h1` | clean | forward | must_not+sort | -1.20% | 3.89% |
| `union_candle_score_h1` | clean | tune | must_not+weather | 9.30% | 1.47% |
| `union_candle_score_h1` | clean | forward | must_not+weather | -1.59% | 0.17% |
| `union_candle_score_h1` | clean | tune | must_not+hold_rule | 0.53% | 0.03% |
| `union_candle_score_h1` | clean | forward | must_not+hold_rule | 0.67% | 0.93% |
| `union_candle_score_h1` | clean | tune | sort+weather | 11.91% | 5.65% |
| `union_candle_score_h1` | clean | forward | sort+weather | -1.12% | -1.29% |
| `union_candle_score_h1` | clean | tune | sort+hold_rule | -1.19% | 1.90% |
| `union_candle_score_h1` | clean | forward | sort+hold_rule | 0.43% | 1.04% |
| `union_candle_score_h1` | clean | tune | weather+hold_rule | 0.80% | -0.27% |
| `union_candle_score_h1` | clean | forward | weather+hold_rule | 0.85% | 3.07% |
| `union_candle_score_h3` | clean | tune | must_have+must_not | 0.00% | 0.00% |
| `union_candle_score_h3` | clean | forward | must_have+must_not | 0.00% | 0.00% |
| `union_candle_score_h3` | clean | tune | must_have+sort | 0.00% | 0.00% |
| `union_candle_score_h3` | clean | forward | must_have+sort | 0.00% | 0.00% |
| `union_candle_score_h3` | clean | tune | must_have+weather | 0.00% | 0.00% |
| `union_candle_score_h3` | clean | forward | must_have+weather | 0.00% | 0.00% |
| `union_candle_score_h3` | clean | tune | must_have+hold_rule | 0.00% | 0.00% |
| `union_candle_score_h3` | clean | forward | must_have+hold_rule | 0.00% | 0.00% |
| `union_candle_score_h3` | clean | tune | must_not+sort | -1.75% | 4.64% |
| `union_candle_score_h3` | clean | forward | must_not+sort | -12.14% | -4.07% |
| `union_candle_score_h3` | clean | tune | must_not+weather | 3.67% | 0.33% |
| `union_candle_score_h3` | clean | forward | must_not+weather | -3.66% | 1.01% |
| `union_candle_score_h3` | clean | tune | must_not+hold_rule | -0.02% | -0.03% |
| `union_candle_score_h3` | clean | forward | must_not+hold_rule | -1.81% | -4.61% |
| `union_candle_score_h3` | clean | tune | sort+weather | -4.49% | -2.73% |
| `union_candle_score_h3` | clean | forward | sort+weather | -12.11% | -6.03% |
| `union_candle_score_h3` | clean | tune | sort+hold_rule | -0.51% | -0.71% |
| `union_candle_score_h3` | clean | forward | sort+hold_rule | -5.80% | -3.26% |
| `union_candle_score_h3` | clean | tune | weather+hold_rule | -0.59% | -1.63% |
| `union_candle_score_h3` | clean | forward | weather+hold_rule | -5.58% | -4.13% |
| `union_cond_h1` | clean | tune | must_have+must_not | 0.00% | 0.00% |
| `union_cond_h1` | clean | forward | must_have+must_not | 0.00% | 0.00% |
| `union_cond_h1` | clean | tune | must_have+sort | 0.00% | 0.00% |
| `union_cond_h1` | clean | forward | must_have+sort | 0.00% | 0.00% |
| `union_cond_h1` | clean | tune | must_have+weather | 0.00% | 0.00% |
| `union_cond_h1` | clean | forward | must_have+weather | 0.00% | -0.00% |
| `union_cond_h1` | clean | tune | must_have+hold_rule | 0.00% | 0.00% |
| `union_cond_h1` | clean | forward | must_have+hold_rule | 0.00% | 0.00% |
| `union_cond_h1` | clean | tune | must_not+sort | 5.06% | 4.29% |
| `union_cond_h1` | clean | forward | must_not+sort | 0.92% | 2.96% |
| `union_cond_h1` | clean | tune | must_not+weather | -0.10% | -0.38% |
| `union_cond_h1` | clean | forward | must_not+weather | 0.00% | -0.00% |
| `union_cond_h1` | clean | tune | must_not+hold_rule | -0.54% | -1.22% |
| `union_cond_h1` | clean | forward | must_not+hold_rule | -0.01% | 0.00% |
| `union_cond_h1` | clean | tune | sort+weather | -0.52% | 0.21% |
| `union_cond_h1` | clean | forward | sort+weather | -5.17% | 1.43% |
| `union_cond_h1` | clean | tune | sort+hold_rule | -0.07% | -0.08% |
| `union_cond_h1` | clean | forward | sort+hold_rule | 0.01% | 0.00% |
| `union_cond_h1` | clean | tune | weather+hold_rule | 0.64% | 0.74% |
| `union_cond_h1` | clean | forward | weather+hold_rule | 0.02% | -0.87% |
| `union_cond_h3` | clean | tune | must_have+must_not | 0.00% | 0.00% |
| `union_cond_h3` | clean | forward | must_have+must_not | 0.00% | 0.00% |
| `union_cond_h3` | clean | tune | must_have+sort | 0.00% | 0.00% |
| `union_cond_h3` | clean | forward | must_have+sort | 0.00% | 0.00% |
| `union_cond_h3` | clean | tune | must_have+weather | 0.00% | 0.00% |
| `union_cond_h3` | clean | forward | must_have+weather | 0.00% | 0.00% |
| `union_cond_h3` | clean | tune | must_have+hold_rule | 0.00% | 0.00% |
| `union_cond_h3` | clean | forward | must_have+hold_rule | 0.00% | 0.00% |
| `union_cond_h3` | clean | tune | must_not+sort | -1.38% | 5.35% |
| `union_cond_h3` | clean | forward | must_not+sort | -5.47% | 2.00% |
| `union_cond_h3` | clean | tune | must_not+weather | -0.01% | -0.88% |
| `union_cond_h3` | clean | forward | must_not+weather | 0.04% | 0.00% |
| `union_cond_h3` | clean | tune | must_not+hold_rule | 0.00% | 0.00% |
| `union_cond_h3` | clean | forward | must_not+hold_rule | -0.00% | 0.00% |
| `union_cond_h3` | clean | tune | sort+weather | 5.57% | -2.50% |
| `union_cond_h3` | clean | forward | sort+weather | -2.20% | 2.29% |
| `union_cond_h3` | clean | tune | sort+hold_rule | 0.00% | 0.00% |
| `union_cond_h3` | clean | forward | sort+hold_rule | 0.00% | 3.33% |
| `union_cond_h3` | clean | tune | weather+hold_rule | 0.00% | 0.00% |
| `union_cond_h3` | clean | forward | weather+hold_rule | 0.83% | 1.95% |
| `union_cond_n4_h3` | clean | tune | must_have+must_not | 0.00% | 0.00% |
| `union_cond_n4_h3` | clean | forward | must_have+must_not | 0.00% | 0.00% |
| `union_cond_n4_h3` | clean | tune | must_have+sort | 0.00% | 0.00% |
| `union_cond_n4_h3` | clean | forward | must_have+sort | 0.00% | -0.00% |
| `union_cond_n4_h3` | clean | tune | must_have+weather | 0.00% | 0.00% |
| `union_cond_n4_h3` | clean | forward | must_have+weather | 0.00% | 0.00% |
| `union_cond_n4_h3` | clean | tune | must_have+hold_rule | 0.00% | 0.00% |
| `union_cond_n4_h3` | clean | forward | must_have+hold_rule | 0.00% | 0.00% |
| `union_cond_n4_h3` | clean | tune | must_not+sort | -0.61% | 11.88% |
| `union_cond_n4_h3` | clean | forward | must_not+sort | -5.36% | 3.68% |
| `union_cond_n4_h3` | clean | tune | must_not+weather | -0.03% | 0.00% |
| `union_cond_n4_h3` | clean | forward | must_not+weather | -0.66% | 2.04% |
| `union_cond_n4_h3` | clean | tune | must_not+hold_rule | 0.00% | 0.00% |
| `union_cond_n4_h3` | clean | forward | must_not+hold_rule | 0.00% | 0.00% |
| `union_cond_n4_h3` | clean | tune | sort+weather | 4.83% | -2.72% |
| `union_cond_n4_h3` | clean | forward | sort+weather | -16.52% | -18.17% |
| `union_cond_n4_h3` | clean | tune | sort+hold_rule | 0.00% | 0.00% |
| `union_cond_n4_h3` | clean | forward | sort+hold_rule | -0.06% | -1.90% |
| `union_cond_n4_h3` | clean | tune | weather+hold_rule | 0.00% | 0.00% |
| `union_cond_n4_h3` | clean | forward | weather+hold_rule | 0.08% | -0.70% |
| `union_hot_n12_h1` | clean | tune | must_have+must_not | 0.00% | 0.00% |
| `union_hot_n12_h1` | clean | forward | must_have+must_not | 0.00% | 0.00% |
| `union_hot_n12_h1` | clean | tune | must_have+sort | 0.00% | 0.00% |
| `union_hot_n12_h1` | clean | forward | must_have+sort | 0.00% | 0.00% |
| `union_hot_n12_h1` | clean | tune | must_have+weather | 0.00% | 0.00% |
| `union_hot_n12_h1` | clean | forward | must_have+weather | 0.00% | 0.00% |
| `union_hot_n12_h1` | clean | tune | must_have+hold_rule | 0.00% | 0.00% |
| `union_hot_n12_h1` | clean | forward | must_have+hold_rule | 0.00% | 0.00% |
| `union_hot_n12_h1` | clean | tune | must_not+sort | 5.67% | 7.23% |
| `union_hot_n12_h1` | clean | forward | must_not+sort | 2.52% | 0.63% |
| `union_hot_n12_h1` | clean | tune | must_not+weather | 3.65% | 3.74% |
| `union_hot_n12_h1` | clean | forward | must_not+weather | -1.78% | 0.30% |
| `union_hot_n12_h1` | clean | tune | must_not+hold_rule | -0.08% | 0.59% |
| `union_hot_n12_h1` | clean | forward | must_not+hold_rule | 2.69% | 0.63% |
| `union_hot_n12_h1` | clean | tune | sort+weather | 5.38% | 1.47% |
| `union_hot_n12_h1` | clean | forward | sort+weather | -2.77% | 0.60% |
| `union_hot_n12_h1` | clean | tune | sort+hold_rule | -1.04% | 0.70% |
| `union_hot_n12_h1` | clean | forward | sort+hold_rule | -8.87% | -3.83% |
| `union_hot_n12_h1` | clean | tune | weather+hold_rule | 1.36% | -1.16% |
| `union_hot_n12_h1` | clean | forward | weather+hold_rule | -6.56% | 1.31% |
| `union_hot_n4_h1` | clean | tune | must_have+must_not | 0.00% | 0.00% |
| `union_hot_n4_h1` | clean | forward | must_have+must_not | 0.00% | 0.00% |
| `union_hot_n4_h1` | clean | tune | must_have+sort | 0.00% | 0.00% |
| `union_hot_n4_h1` | clean | forward | must_have+sort | 0.00% | -0.00% |
| `union_hot_n4_h1` | clean | tune | must_have+weather | 0.00% | 0.00% |
| `union_hot_n4_h1` | clean | forward | must_have+weather | 0.00% | -0.00% |
| `union_hot_n4_h1` | clean | tune | must_have+hold_rule | 0.00% | -0.00% |
| `union_hot_n4_h1` | clean | forward | must_have+hold_rule | 0.00% | 0.00% |
| `union_hot_n4_h1` | clean | tune | must_not+sort | 3.90% | 8.42% |
| `union_hot_n4_h1` | clean | forward | must_not+sort | 3.13% | -0.63% |
| `union_hot_n4_h1` | clean | tune | must_not+weather | 9.07% | -1.11% |
| `union_hot_n4_h1` | clean | forward | must_not+weather | 2.12% | 3.05% |
| `union_hot_n4_h1` | clean | tune | must_not+hold_rule | -1.92% | -0.00% |
| `union_hot_n4_h1` | clean | forward | must_not+hold_rule | 11.54% | -1.69% |
| `union_hot_n4_h1` | clean | tune | sort+weather | 13.08% | -2.13% |
| `union_hot_n4_h1` | clean | forward | sort+weather | 6.11% | -0.72% |
| `union_hot_n4_h1` | clean | tune | sort+hold_rule | -0.37% | 5.09% |
| `union_hot_n4_h1` | clean | forward | sort+hold_rule | -24.30% | -2.20% |
| `union_hot_n4_h1` | clean | tune | weather+hold_rule | 3.65% | 2.11% |
| `union_hot_n4_h1` | clean | forward | weather+hold_rule | -19.00% | 2.50% |
| `union_hot_score_h1` | clean | tune | must_have+must_not | 0.00% | -0.00% |
| `union_hot_score_h1` | clean | forward | must_have+must_not | 0.00% | 0.00% |
| `union_hot_score_h1` | clean | tune | must_have+sort | 0.00% | -0.00% |
| `union_hot_score_h1` | clean | forward | must_have+sort | 0.00% | 0.00% |
| `union_hot_score_h1` | clean | tune | must_have+weather | 0.00% | 0.00% |
| `union_hot_score_h1` | clean | forward | must_have+weather | 0.00% | -0.00% |
| `union_hot_score_h1` | clean | tune | must_have+hold_rule | 0.00% | 0.00% |
| `union_hot_score_h1` | clean | forward | must_have+hold_rule | 0.00% | 0.00% |
| `union_hot_score_h1` | clean | tune | must_not+sort | 11.03% | 6.88% |
| `union_hot_score_h1` | clean | forward | must_not+sort | 6.13% | 6.98% |
| `union_hot_score_h1` | clean | tune | must_not+weather | 4.69% | 3.28% |
| `union_hot_score_h1` | clean | forward | must_not+weather | -3.42% | -0.26% |
| `union_hot_score_h1` | clean | tune | must_not+hold_rule | -0.87% | 0.09% |
| `union_hot_score_h1` | clean | forward | must_not+hold_rule | 4.68% | -0.42% |
| `union_hot_score_h1` | clean | tune | sort+weather | 11.10% | 2.02% |
| `union_hot_score_h1` | clean | forward | sort+weather | -2.35% | 1.59% |
| `union_hot_score_h1` | clean | tune | sort+hold_rule | -1.38% | 0.24% |
| `union_hot_score_h1` | clean | forward | sort+hold_rule | -13.03% | -3.89% |
| `union_hot_score_h1` | clean | tune | weather+hold_rule | 1.41% | -1.33% |
| `union_hot_score_h1` | clean | forward | weather+hold_rule | -9.79% | 1.16% |
| `union_hot_score_h3` | clean | tune | must_have+must_not | 0.00% | -0.00% |
| `union_hot_score_h3` | clean | forward | must_have+must_not | 0.00% | 0.00% |
| `union_hot_score_h3` | clean | tune | must_have+sort | 0.00% | -0.00% |
| `union_hot_score_h3` | clean | forward | must_have+sort | 0.00% | 0.00% |
| `union_hot_score_h3` | clean | tune | must_have+weather | 0.00% | 0.00% |
| `union_hot_score_h3` | clean | forward | must_have+weather | 0.00% | 0.00% |
| `union_hot_score_h3` | clean | tune | must_have+hold_rule | 0.00% | 0.00% |
| `union_hot_score_h3` | clean | forward | must_have+hold_rule | 0.00% | -0.00% |
| `union_hot_score_h3` | clean | tune | must_not+sort | -1.64% | 7.10% |
| `union_hot_score_h3` | clean | forward | must_not+sort | -5.62% | 2.97% |
| `union_hot_score_h3` | clean | tune | must_not+weather | 4.97% | -1.57% |
| `union_hot_score_h3` | clean | forward | must_not+weather | -0.68% | -2.30% |
| `union_hot_score_h3` | clean | tune | must_not+hold_rule | 0.10% | 0.06% |
| `union_hot_score_h3` | clean | forward | must_not+hold_rule | -0.29% | -3.06% |
| `union_hot_score_h3` | clean | tune | sort+weather | 13.86% | 4.83% |
| `union_hot_score_h3` | clean | forward | sort+weather | -7.23% | -4.63% |
| `union_hot_score_h3` | clean | tune | sort+hold_rule | 1.13% | 2.14% |
| `union_hot_score_h3` | clean | forward | sort+hold_rule | -5.92% | -4.03% |
| `union_hot_score_h3` | clean | tune | weather+hold_rule | 2.75% | 1.00% |
| `union_hot_score_h3` | clean | forward | weather+hold_rule | -6.88% | -8.80% |
| `union_ret_5_h1` | clean | tune | must_have+must_not | 0.00% | 0.00% |
| `union_ret_5_h1` | clean | forward | must_have+must_not | 0.00% | 0.00% |
| `union_ret_5_h1` | clean | tune | must_have+sort | 0.00% | 0.00% |
| `union_ret_5_h1` | clean | forward | must_have+sort | 0.00% | 0.00% |
| `union_ret_5_h1` | clean | tune | must_have+weather | 0.00% | 0.00% |
| `union_ret_5_h1` | clean | forward | must_have+weather | 0.00% | 0.00% |
| `union_ret_5_h1` | clean | tune | must_have+hold_rule | 0.00% | 0.00% |
| `union_ret_5_h1` | clean | forward | must_have+hold_rule | 0.00% | 0.00% |
| `union_ret_5_h1` | clean | tune | must_not+sort | 7.57% | 8.34% |
| `union_ret_5_h1` | clean | forward | must_not+sort | 4.73% | 6.23% |
| `union_ret_5_h1` | clean | tune | must_not+weather | 7.60% | 6.46% |
| `union_ret_5_h1` | clean | forward | must_not+weather | 0.46% | 6.17% |
| `union_ret_5_h1` | clean | tune | must_not+hold_rule | -1.01% | 0.18% |
| `union_ret_5_h1` | clean | forward | must_not+hold_rule | 4.49% | 1.04% |
| `union_ret_5_h1` | clean | tune | sort+weather | 6.63% | 3.12% |
| `union_ret_5_h1` | clean | forward | sort+weather | 3.28% | 3.06% |
| `union_ret_5_h1` | clean | tune | sort+hold_rule | -0.92% | 2.19% |
| `union_ret_5_h1` | clean | forward | sort+hold_rule | -12.91% | -1.65% |
| `union_ret_5_h1` | clean | tune | weather+hold_rule | 1.41% | 1.37% |
| `union_ret_5_h1` | clean | forward | weather+hold_rule | -10.27% | -0.38% |
| `union_ret_5_h3` | clean | tune | must_have+must_not | 0.00% | 0.00% |
| `union_ret_5_h3` | clean | forward | must_have+must_not | 0.00% | 0.00% |
| `union_ret_5_h3` | clean | tune | must_have+sort | 0.00% | 0.00% |
| `union_ret_5_h3` | clean | forward | must_have+sort | 0.00% | 0.00% |
| `union_ret_5_h3` | clean | tune | must_have+weather | 0.00% | 0.00% |
| `union_ret_5_h3` | clean | forward | must_have+weather | 0.00% | 0.00% |
| `union_ret_5_h3` | clean | tune | must_have+hold_rule | 0.00% | 0.00% |
| `union_ret_5_h3` | clean | forward | must_have+hold_rule | 0.00% | 0.00% |
| `union_ret_5_h3` | clean | tune | must_not+sort | -3.09% | 5.35% |
| `union_ret_5_h3` | clean | forward | must_not+sort | -2.94% | 1.57% |
| `union_ret_5_h3` | clean | tune | must_not+weather | 6.29% | 2.99% |
| `union_ret_5_h3` | clean | forward | must_not+weather | 0.68% | 5.38% |
| `union_ret_5_h3` | clean | tune | must_not+hold_rule | 0.58% | 0.00% |
| `union_ret_5_h3` | clean | forward | must_not+hold_rule | 1.32% | -1.70% |
| `union_ret_5_h3` | clean | tune | sort+weather | 9.46% | 5.27% |
| `union_ret_5_h3` | clean | forward | sort+weather | -10.52% | 3.06% |
| `union_ret_5_h3` | clean | tune | sort+hold_rule | 2.33% | 1.93% |
| `union_ret_5_h3` | clean | forward | sort+hold_rule | -6.65% | -1.27% |
| `union_ret_5_h3` | clean | tune | weather+hold_rule | 3.59% | 2.10% |
| `union_ret_5_h3` | clean | forward | weather+hold_rule | -7.41% | -5.88% |
| `union_w_hot_candle_h1` | clean | tune | must_have+must_not | 0.00% | 0.00% |
| `union_w_hot_candle_h1` | clean | forward | must_have+must_not | 0.00% | -0.00% |
| `union_w_hot_candle_h1` | clean | tune | must_have+sort | 0.00% | -0.00% |
| `union_w_hot_candle_h1` | clean | forward | must_have+sort | 0.00% | 0.00% |
| `union_w_hot_candle_h1` | clean | tune | must_have+weather | 0.00% | 0.00% |
| `union_w_hot_candle_h1` | clean | forward | must_have+weather | 0.00% | 0.00% |
| `union_w_hot_candle_h1` | clean | tune | must_have+hold_rule | 0.00% | 0.00% |
| `union_w_hot_candle_h1` | clean | forward | must_have+hold_rule | 0.00% | 0.00% |
| `union_w_hot_candle_h1` | clean | tune | must_not+sort | 7.09% | 6.18% |
| `union_w_hot_candle_h1` | clean | forward | must_not+sort | 1.47% | 4.98% |
| `union_w_hot_candle_h1` | clean | tune | must_not+weather | 3.01% | 0.84% |
| `union_w_hot_candle_h1` | clean | forward | must_not+weather | -0.72% | 2.99% |
| `union_w_hot_candle_h1` | clean | tune | must_not+hold_rule | -1.38% | 0.05% |
| `union_w_hot_candle_h1` | clean | forward | must_not+hold_rule | 4.03% | -0.20% |
| `union_w_hot_candle_h1` | clean | tune | sort+weather | 14.84% | 2.65% |
| `union_w_hot_candle_h1` | clean | forward | sort+weather | -1.44% | 1.44% |
| `union_w_hot_candle_h1` | clean | tune | sort+hold_rule | -0.48% | 0.62% |
| `union_w_hot_candle_h1` | clean | forward | sort+hold_rule | -13.16% | -2.70% |
| `union_w_hot_candle_h1` | clean | tune | weather+hold_rule | 2.32% | -0.48% |
| `union_w_hot_candle_h1` | clean | forward | weather+hold_rule | -10.26% | 1.71% |
| `union_w_hot_candle_h3` | clean | tune | must_have+must_not | 0.00% | 0.00% |
| `union_w_hot_candle_h3` | clean | forward | must_have+must_not | 0.00% | 0.00% |
| `union_w_hot_candle_h3` | clean | tune | must_have+sort | 0.00% | 0.00% |
| `union_w_hot_candle_h3` | clean | forward | must_have+sort | 0.00% | 0.00% |
| `union_w_hot_candle_h3` | clean | tune | must_have+weather | 0.00% | -0.00% |
| `union_w_hot_candle_h3` | clean | forward | must_have+weather | 0.00% | 0.00% |
| `union_w_hot_candle_h3` | clean | tune | must_have+hold_rule | 0.00% | 0.00% |
| `union_w_hot_candle_h3` | clean | forward | must_have+hold_rule | 0.00% | 0.00% |
| `union_w_hot_candle_h3` | clean | tune | must_not+sort | -0.68% | 5.66% |
| `union_w_hot_candle_h3` | clean | forward | must_not+sort | -5.52% | -0.59% |
| `union_w_hot_candle_h3` | clean | tune | must_not+weather | 1.25% | -2.16% |
| `union_w_hot_candle_h3` | clean | forward | must_not+weather | -1.88% | 0.61% |
| `union_w_hot_candle_h3` | clean | tune | must_not+hold_rule | -0.13% | 0.00% |
| `union_w_hot_candle_h3` | clean | forward | must_not+hold_rule | -0.05% | 2.37% |
| `union_w_hot_candle_h3` | clean | tune | sort+weather | 10.48% | 5.49% |
| `union_w_hot_candle_h3` | clean | forward | sort+weather | -1.97% | 2.29% |
| `union_w_hot_candle_h3` | clean | tune | sort+hold_rule | -0.10% | 0.00% |
| `union_w_hot_candle_h3` | clean | forward | sort+hold_rule | -4.73% | 4.84% |
| `union_w_hot_candle_h3` | clean | tune | weather+hold_rule | -3.96% | -0.72% |
| `union_w_hot_candle_h3` | clean | forward | weather+hold_rule | -1.39% | -0.13% |
| `union_w_hot_cond_h1` | clean | tune | must_have+must_not | 0.00% | 0.00% |
| `union_w_hot_cond_h1` | clean | forward | must_have+must_not | 0.00% | 0.00% |
| `union_w_hot_cond_h1` | clean | tune | must_have+sort | 0.00% | 0.00% |
| `union_w_hot_cond_h1` | clean | forward | must_have+sort | 0.00% | -0.00% |
| `union_w_hot_cond_h1` | clean | tune | must_have+weather | 0.00% | 0.00% |
| `union_w_hot_cond_h1` | clean | forward | must_have+weather | 0.00% | 0.00% |
| `union_w_hot_cond_h1` | clean | tune | must_have+hold_rule | 0.00% | 0.00% |
| `union_w_hot_cond_h1` | clean | forward | must_have+hold_rule | 0.00% | 0.00% |
| `union_w_hot_cond_h1` | clean | tune | must_not+sort | 11.94% | 6.81% |
| `union_w_hot_cond_h1` | clean | forward | must_not+sort | 6.05% | 6.28% |
| `union_w_hot_cond_h1` | clean | tune | must_not+weather | 4.11% | -0.02% |
| `union_w_hot_cond_h1` | clean | forward | must_not+weather | -0.79% | 1.40% |
| `union_w_hot_cond_h1` | clean | tune | must_not+hold_rule | 0.01% | 0.05% |
| `union_w_hot_cond_h1` | clean | forward | must_not+hold_rule | 4.86% | 1.10% |
| `union_w_hot_cond_h1` | clean | tune | sort+weather | 7.78% | -2.07% |
| `union_w_hot_cond_h1` | clean | forward | sort+weather | -1.60% | -4.87% |
| `union_w_hot_cond_h1` | clean | tune | sort+hold_rule | -0.13% | 2.57% |
| `union_w_hot_cond_h1` | clean | forward | sort+hold_rule | -13.27% | -1.76% |
| `union_w_hot_cond_h1` | clean | tune | weather+hold_rule | 1.77% | 0.43% |
| `union_w_hot_cond_h1` | clean | forward | weather+hold_rule | -10.93% | -2.47% |
| `union_w_hot_cond_h3` | clean | tune | must_have+must_not | 0.00% | -0.00% |
| `union_w_hot_cond_h3` | clean | forward | must_have+must_not | 0.00% | 0.00% |
| `union_w_hot_cond_h3` | clean | tune | must_have+sort | 0.00% | 0.00% |
| `union_w_hot_cond_h3` | clean | forward | must_have+sort | 0.00% | 0.00% |
| `union_w_hot_cond_h3` | clean | tune | must_have+weather | 0.00% | 0.00% |
| `union_w_hot_cond_h3` | clean | forward | must_have+weather | 0.00% | 0.00% |
| `union_w_hot_cond_h3` | clean | tune | must_have+hold_rule | 0.00% | -0.00% |
| `union_w_hot_cond_h3` | clean | forward | must_have+hold_rule | 0.00% | -0.00% |
| `union_w_hot_cond_h3` | clean | tune | must_not+sort | -1.34% | 7.07% |
| `union_w_hot_cond_h3` | clean | forward | must_not+sort | -5.39% | 0.78% |
| `union_w_hot_cond_h3` | clean | tune | must_not+weather | 4.51% | 3.32% |
| `union_w_hot_cond_h3` | clean | forward | must_not+weather | -3.67% | -1.26% |
| `union_w_hot_cond_h3` | clean | tune | must_not+hold_rule | 0.01% | 0.00% |
| `union_w_hot_cond_h3` | clean | forward | must_not+hold_rule | 0.06% | -0.18% |
| `union_w_hot_cond_h3` | clean | tune | sort+weather | 9.99% | 2.33% |
| `union_w_hot_cond_h3` | clean | forward | sort+weather | -15.03% | -6.62% |
| `union_w_hot_cond_h3` | clean | tune | sort+hold_rule | 0.92% | 1.72% |
| `union_w_hot_cond_h3` | clean | forward | sort+hold_rule | -6.08% | 2.12% |
| `union_w_hot_cond_h3` | clean | tune | weather+hold_rule | -0.68% | 1.19% |
| `union_w_hot_cond_h3` | clean | forward | weather+hold_rule | -6.97% | -2.10% |
| `union_hot_n4_holdup` | clean | tune | must_have+must_not | 0.00% | 0.00% |
| `union_hot_n4_holdup` | clean | forward | must_have+must_not | 0.00% | 0.00% |
| `union_hot_n4_holdup` | clean | tune | must_have+sort | 0.00% | 0.00% |
| `union_hot_n4_holdup` | clean | forward | must_have+sort | 0.00% | -0.00% |
| `union_hot_n4_holdup` | clean | tune | must_have+weather | 0.00% | -0.00% |
| `union_hot_n4_holdup` | clean | forward | must_have+weather | 0.00% | -0.00% |
| `union_hot_n4_holdup` | clean | tune | must_have+hold_rule | 0.00% | 0.00% |
| `union_hot_n4_holdup` | clean | forward | must_have+hold_rule | 0.00% | 0.00% |
| `union_hot_n4_holdup` | clean | tune | must_not+sort | 0.70% | 1.12% |
| `union_hot_n4_holdup` | clean | forward | must_not+sort | -3.48% | 0.38% |
| `union_hot_n4_holdup` | clean | tune | must_not+weather | 9.93% | -3.17% |
| `union_hot_n4_holdup` | clean | forward | must_not+weather | -8.00% | -1.84% |
| `union_hot_n4_holdup` | clean | tune | must_not+hold_rule | -5.70% | -1.38% |
| `union_hot_n4_holdup` | clean | forward | must_not+hold_rule | 3.08% | 0.38% |
| `union_hot_n4_holdup` | clean | tune | sort+weather | 9.25% | -5.29% |
| `union_hot_n4_holdup` | clean | forward | sort+weather | -2.34% | -6.12% |
| `union_hot_n4_holdup` | clean | tune | sort+hold_rule | -21.38% | -8.08% |
| `union_hot_n4_holdup` | clean | forward | sort+hold_rule | -32.83% | 2.71% |
| `union_hot_n4_holdup` | clean | tune | weather+hold_rule | -0.14% | -2.05% |
| `union_hot_n4_holdup` | clean | forward | weather+hold_rule | -30.21% | 1.25% |
| `probable_h3` | yahoo | tune | must_have+must_not | 0.00% | 0.00% |
| `probable_h3` | yahoo | forward | must_have+must_not | 0.00% | 0.00% |
| `probable_h3` | yahoo | tune | must_have+sort | 0.00% | 0.00% |
| `probable_h3` | yahoo | forward | must_have+sort | 0.00% | 0.00% |
| `probable_h3` | yahoo | tune | must_have+weather | 0.00% | 0.00% |
| `probable_h3` | yahoo | forward | must_have+weather | 0.00% | 0.00% |
| `probable_h3` | yahoo | tune | must_have+hold_rule | 0.00% | 0.00% |
| `probable_h3` | yahoo | forward | must_have+hold_rule | 0.00% | 0.00% |
| `probable_h3` | yahoo | tune | must_not+sort | 0.00% | 0.00% |
| `probable_h3` | yahoo | forward | must_not+sort | 0.00% | 0.00% |
| `probable_h3` | yahoo | tune | must_not+weather | 0.00% | 0.00% |
| `probable_h3` | yahoo | forward | must_not+weather | 0.00% | 0.00% |
| `probable_h3` | yahoo | tune | must_not+hold_rule | 0.00% | 0.00% |
| `probable_h3` | yahoo | forward | must_not+hold_rule | 0.00% | 0.00% |
| `probable_h3` | yahoo | tune | sort+weather | 0.00% | 0.00% |
| `probable_h3` | yahoo | forward | sort+weather | 0.00% | 0.00% |
| `probable_h3` | yahoo | tune | sort+hold_rule | 0.00% | 0.00% |
| `probable_h3` | yahoo | forward | sort+hold_rule | 0.00% | 0.00% |
| `probable_h3` | yahoo | tune | weather+hold_rule | 0.50% | 0.69% |
| `probable_h3` | yahoo | forward | weather+hold_rule | -1.33% | 1.74% |
| `probable_h5` | yahoo | tune | must_have+must_not | 0.00% | 0.00% |
| `probable_h5` | yahoo | forward | must_have+must_not | 0.00% | 0.00% |
| `probable_h5` | yahoo | tune | must_have+sort | 0.00% | 0.00% |
| `probable_h5` | yahoo | forward | must_have+sort | 0.00% | 0.00% |
| `probable_h5` | yahoo | tune | must_have+weather | 0.00% | -0.00% |
| `probable_h5` | yahoo | forward | must_have+weather | 0.00% | 0.00% |
| `probable_h5` | yahoo | tune | must_have+hold_rule | 0.00% | 0.00% |
| `probable_h5` | yahoo | forward | must_have+hold_rule | 0.00% | 0.00% |
| `probable_h5` | yahoo | tune | must_not+sort | 0.00% | 0.00% |
| `probable_h5` | yahoo | forward | must_not+sort | 0.00% | 0.00% |
| `probable_h5` | yahoo | tune | must_not+weather | 0.00% | -0.00% |
| `probable_h5` | yahoo | forward | must_not+weather | 0.00% | 0.00% |
| `probable_h5` | yahoo | tune | must_not+hold_rule | 0.00% | 0.00% |
| `probable_h5` | yahoo | forward | must_not+hold_rule | 0.00% | 0.00% |
| `probable_h5` | yahoo | tune | sort+weather | -0.00% | -0.00% |
| `probable_h5` | yahoo | forward | sort+weather | 0.00% | 0.00% |
| `probable_h5` | yahoo | tune | sort+hold_rule | 0.00% | 0.00% |
| `probable_h5` | yahoo | forward | sort+hold_rule | -0.00% | 0.00% |
| `probable_h5` | yahoo | tune | weather+hold_rule | 0.00% | -0.00% |
| `probable_h5` | yahoo | forward | weather+hold_rule | 1.01% | 0.00% |
| `short_extended_h3` | yahoo | tune | must_have+must_not | 0.00% | 0.00% |
| `short_extended_h3` | yahoo | forward | must_have+must_not | 0.00% | 0.00% |
| `short_extended_h3` | yahoo | tune | must_have+sort | -6.28% | -3.36% |
| `short_extended_h3` | yahoo | forward | must_have+sort | 17.54% | 20.42% |
| `short_extended_h3` | yahoo | tune | must_have+weather | 18.94% | 9.38% |
| `short_extended_h3` | yahoo | forward | must_have+weather | 8.51% | 15.64% |
| `short_extended_h3` | yahoo | tune | must_have+hold_rule | 0.77% | -1.47% |
| `short_extended_h3` | yahoo | forward | must_have+hold_rule | 5.31% | 6.24% |
| `short_extended_h3` | yahoo | tune | must_not+sort | 0.00% | 0.00% |
| `short_extended_h3` | yahoo | forward | must_not+sort | 0.00% | 0.00% |
| `short_extended_h3` | yahoo | tune | must_not+weather | 0.00% | 0.00% |
| `short_extended_h3` | yahoo | forward | must_not+weather | 0.00% | -0.00% |
| `short_extended_h3` | yahoo | tune | must_not+hold_rule | 0.00% | 0.00% |
| `short_extended_h3` | yahoo | forward | must_not+hold_rule | 0.00% | 0.00% |
| `short_extended_h3` | yahoo | tune | sort+weather | 72.63% | 7.84% |
| `short_extended_h3` | yahoo | forward | sort+weather | 3.94% | 7.24% |
| `short_extended_h3` | yahoo | tune | sort+hold_rule | -0.47% | -2.17% |
| `short_extended_h3` | yahoo | forward | sort+hold_rule | 2.02% | 3.42% |
| `short_extended_h3` | yahoo | tune | weather+hold_rule | -1.37% | 0.77% |
| `short_extended_h3` | yahoo | forward | weather+hold_rule | 3.65% | 7.56% |
| `union_candle_score_h1` | yahoo | tune | must_have+must_not | 0.00% | 0.00% |
| `union_candle_score_h1` | yahoo | forward | must_have+must_not | 0.00% | 0.00% |
| `union_candle_score_h1` | yahoo | tune | must_have+sort | 0.00% | 0.00% |
| `union_candle_score_h1` | yahoo | forward | must_have+sort | 0.00% | -0.00% |
| `union_candle_score_h1` | yahoo | tune | must_have+weather | 0.00% | -0.00% |
| `union_candle_score_h1` | yahoo | forward | must_have+weather | 0.00% | 0.00% |
| `union_candle_score_h1` | yahoo | tune | must_have+hold_rule | 0.00% | 0.00% |
| `union_candle_score_h1` | yahoo | forward | must_have+hold_rule | 0.00% | -0.00% |
| `union_candle_score_h1` | yahoo | tune | must_not+sort | 8.57% | 4.71% |
| `union_candle_score_h1` | yahoo | forward | must_not+sort | -1.17% | 3.86% |
| `union_candle_score_h1` | yahoo | tune | must_not+weather | 9.28% | 1.28% |
| `union_candle_score_h1` | yahoo | forward | must_not+weather | -1.34% | 0.19% |
| `union_candle_score_h1` | yahoo | tune | must_not+hold_rule | 0.63% | 0.00% |
| `union_candle_score_h1` | yahoo | forward | must_not+hold_rule | 0.76% | 0.93% |
| `union_candle_score_h1` | yahoo | tune | sort+weather | 12.04% | 4.95% |
| `union_candle_score_h1` | yahoo | forward | sort+weather | 0.51% | -0.44% |
| `union_candle_score_h1` | yahoo | tune | sort+hold_rule | -1.12% | 1.82% |
| `union_candle_score_h1` | yahoo | forward | sort+hold_rule | 0.48% | 1.04% |
| `union_candle_score_h1` | yahoo | tune | weather+hold_rule | 0.75% | -0.30% |
| `union_candle_score_h1` | yahoo | forward | weather+hold_rule | 1.13% | 2.94% |
| `union_candle_score_h3` | yahoo | tune | must_have+must_not | 0.00% | 0.00% |
| `union_candle_score_h3` | yahoo | forward | must_have+must_not | 0.00% | 0.00% |
| `union_candle_score_h3` | yahoo | tune | must_have+sort | 0.00% | 0.00% |
| `union_candle_score_h3` | yahoo | forward | must_have+sort | 0.00% | 0.00% |
| `union_candle_score_h3` | yahoo | tune | must_have+weather | 0.00% | -0.00% |
| `union_candle_score_h3` | yahoo | forward | must_have+weather | 0.00% | 0.00% |
| `union_candle_score_h3` | yahoo | tune | must_have+hold_rule | 0.00% | 0.00% |
| `union_candle_score_h3` | yahoo | forward | must_have+hold_rule | 0.00% | 0.00% |
| `union_candle_score_h3` | yahoo | tune | must_not+sort | -2.92% | 7.94% |
| `union_candle_score_h3` | yahoo | forward | must_not+sort | -12.01% | -5.60% |
| `union_candle_score_h3` | yahoo | tune | must_not+weather | -134.85% | 1.30% |
| `union_candle_score_h3` | yahoo | forward | must_not+weather | -8.52% | 6.54% |
| `union_candle_score_h3` | yahoo | tune | must_not+hold_rule | 0.06% | 0.71% |
| `union_candle_score_h3` | yahoo | forward | must_not+hold_rule | -1.79% | -1.46% |
| `union_candle_score_h3` | yahoo | tune | sort+weather | -11.20% | -6.41% |
| `union_candle_score_h3` | yahoo | forward | sort+weather | -14.19% | -2.92% |
| `union_candle_score_h3` | yahoo | tune | sort+hold_rule | -0.42% | 0.00% |
| `union_candle_score_h3` | yahoo | forward | sort+hold_rule | -5.74% | 0.00% |
| `union_candle_score_h3` | yahoo | tune | weather+hold_rule | -0.48% | -0.95% |
| `union_candle_score_h3` | yahoo | forward | weather+hold_rule | -5.77% | 0.00% |
| `union_cond_h1` | yahoo | tune | must_have+must_not | 0.00% | 0.00% |
| `union_cond_h1` | yahoo | forward | must_have+must_not | 0.00% | 0.00% |
| `union_cond_h1` | yahoo | tune | must_have+sort | 0.00% | 0.00% |
| `union_cond_h1` | yahoo | forward | must_have+sort | 0.00% | -0.00% |
| `union_cond_h1` | yahoo | tune | must_have+weather | 0.00% | -0.00% |
| `union_cond_h1` | yahoo | forward | must_have+weather | 0.00% | -0.00% |
| `union_cond_h1` | yahoo | tune | must_have+hold_rule | 0.00% | 0.00% |
| `union_cond_h1` | yahoo | forward | must_have+hold_rule | 0.00% | 0.00% |
| `union_cond_h1` | yahoo | tune | must_not+sort | 6.56% | 3.50% |
| `union_cond_h1` | yahoo | forward | must_not+sort | 1.07% | 2.93% |
| `union_cond_h1` | yahoo | tune | must_not+weather | -0.10% | -0.37% |
| `union_cond_h1` | yahoo | forward | must_not+weather | 0.16% | -0.00% |
| `union_cond_h1` | yahoo | tune | must_not+hold_rule | -0.61% | -1.20% |
| `union_cond_h1` | yahoo | forward | must_not+hold_rule | 0.12% | 0.00% |
| `union_cond_h1` | yahoo | tune | sort+weather | -0.42% | -0.28% |
| `union_cond_h1` | yahoo | forward | sort+weather | -2.00% | 2.91% |
| `union_cond_h1` | yahoo | tune | sort+hold_rule | -0.07% | -0.10% |
| `union_cond_h1` | yahoo | forward | sort+hold_rule | 0.07% | -0.00% |
| `union_cond_h1` | yahoo | tune | weather+hold_rule | 0.67% | 0.74% |
| `union_cond_h1` | yahoo | forward | weather+hold_rule | 0.13% | -0.87% |
| `union_cond_h3` | yahoo | tune | must_have+must_not | 0.00% | 0.00% |
| `union_cond_h3` | yahoo | forward | must_have+must_not | 0.00% | 0.00% |
| `union_cond_h3` | yahoo | tune | must_have+sort | 0.00% | 0.00% |
| `union_cond_h3` | yahoo | forward | must_have+sort | 0.00% | -0.00% |
| `union_cond_h3` | yahoo | tune | must_have+weather | 0.00% | -0.00% |
| `union_cond_h3` | yahoo | forward | must_have+weather | 0.00% | 0.00% |
| `union_cond_h3` | yahoo | tune | must_have+hold_rule | 0.00% | 0.00% |
| `union_cond_h3` | yahoo | forward | must_have+hold_rule | 0.00% | 0.00% |
| `union_cond_h3` | yahoo | tune | must_not+sort | -2.58% | 7.94% |
| `union_cond_h3` | yahoo | forward | must_not+sort | -5.37% | -4.14% |
| `union_cond_h3` | yahoo | tune | must_not+weather | -0.01% | -0.88% |
| `union_cond_h3` | yahoo | forward | must_not+weather | 0.04% | 0.00% |
| `union_cond_h3` | yahoo | tune | must_not+hold_rule | 0.00% | 0.00% |
| `union_cond_h3` | yahoo | forward | must_not+hold_rule | -0.00% | 0.00% |
| `union_cond_h3` | yahoo | tune | sort+weather | 1.19% | -4.40% |
| `union_cond_h3` | yahoo | forward | sort+weather | -2.13% | -3.21% |
| `union_cond_h3` | yahoo | tune | sort+hold_rule | 0.00% | 0.00% |
| `union_cond_h3` | yahoo | forward | sort+hold_rule | 0.00% | 3.45% |
| `union_cond_h3` | yahoo | tune | weather+hold_rule | 0.00% | -0.00% |
| `union_cond_h3` | yahoo | forward | weather+hold_rule | 0.56% | 1.94% |
| `union_cond_n4_h3` | yahoo | tune | must_have+must_not | 0.00% | 0.00% |
| `union_cond_n4_h3` | yahoo | forward | must_have+must_not | 0.00% | 0.00% |
| `union_cond_n4_h3` | yahoo | tune | must_have+sort | 0.00% | -0.00% |
| `union_cond_n4_h3` | yahoo | forward | must_have+sort | 0.00% | 0.00% |
| `union_cond_n4_h3` | yahoo | tune | must_have+weather | 0.00% | 0.00% |
| `union_cond_n4_h3` | yahoo | forward | must_have+weather | 0.00% | -0.00% |
| `union_cond_n4_h3` | yahoo | tune | must_have+hold_rule | 0.00% | 0.00% |
| `union_cond_n4_h3` | yahoo | forward | must_have+hold_rule | 0.00% | 0.00% |
| `union_cond_n4_h3` | yahoo | tune | must_not+sort | -0.83% | 10.58% |
| `union_cond_n4_h3` | yahoo | forward | must_not+sort | -5.54% | -8.67% |
| `union_cond_n4_h3` | yahoo | tune | must_not+weather | -0.03% | 0.00% |
| `union_cond_n4_h3` | yahoo | forward | must_not+weather | -0.00% | -0.00% |
| `union_cond_n4_h3` | yahoo | tune | must_not+hold_rule | 0.00% | 0.00% |
| `union_cond_n4_h3` | yahoo | forward | must_not+hold_rule | 0.00% | 0.00% |
| `union_cond_n4_h3` | yahoo | tune | sort+weather | 2.20% | -2.62% |
| `union_cond_n4_h3` | yahoo | forward | sort+weather | -15.48% | -34.07% |
| `union_cond_n4_h3` | yahoo | tune | sort+hold_rule | 0.00% | -0.00% |
| `union_cond_n4_h3` | yahoo | forward | sort+hold_rule | -0.06% | -1.90% |
| `union_cond_n4_h3` | yahoo | tune | weather+hold_rule | 0.00% | 0.00% |
| `union_cond_n4_h3` | yahoo | forward | weather+hold_rule | 0.19% | -0.72% |
| `union_hot_n12_h1` | yahoo | tune | must_have+must_not | 0.00% | 0.00% |
| `union_hot_n12_h1` | yahoo | forward | must_have+must_not | 0.00% | -0.00% |
| `union_hot_n12_h1` | yahoo | tune | must_have+sort | 0.00% | 0.00% |
| `union_hot_n12_h1` | yahoo | forward | must_have+sort | 0.00% | 0.00% |
| `union_hot_n12_h1` | yahoo | tune | must_have+weather | 0.00% | 0.00% |
| `union_hot_n12_h1` | yahoo | forward | must_have+weather | 0.00% | 0.00% |
| `union_hot_n12_h1` | yahoo | tune | must_have+hold_rule | 0.00% | -0.00% |
| `union_hot_n12_h1` | yahoo | forward | must_have+hold_rule | 0.00% | 0.00% |
| `union_hot_n12_h1` | yahoo | tune | must_not+sort | 6.27% | 6.87% |
| `union_hot_n12_h1` | yahoo | forward | must_not+sort | 3.06% | 1.14% |
| `union_hot_n12_h1` | yahoo | tune | must_not+weather | 8.13% | 4.04% |
| `union_hot_n12_h1` | yahoo | forward | must_not+weather | 1.50% | 2.74% |
| `union_hot_n12_h1` | yahoo | tune | must_not+hold_rule | -0.32% | 0.94% |
| `union_hot_n12_h1` | yahoo | forward | must_not+hold_rule | 3.01% | 0.66% |
| `union_hot_n12_h1` | yahoo | tune | sort+weather | 86.36% | 2.04% |
| `union_hot_n12_h1` | yahoo | forward | sort+weather | 0.82% | 2.65% |
| `union_hot_n12_h1` | yahoo | tune | sort+hold_rule | -0.64% | 1.04% |
| `union_hot_n12_h1` | yahoo | forward | sort+hold_rule | -8.53% | -4.04% |
| `union_hot_n12_h1` | yahoo | tune | weather+hold_rule | 3.65% | -0.47% |
| `union_hot_n12_h1` | yahoo | forward | weather+hold_rule | -6.29% | 0.33% |
| `union_hot_n4_h1` | yahoo | tune | must_have+must_not | 0.00% | 0.00% |
| `union_hot_n4_h1` | yahoo | forward | must_have+must_not | 0.00% | 0.00% |
| `union_hot_n4_h1` | yahoo | tune | must_have+sort | 0.00% | 0.00% |
| `union_hot_n4_h1` | yahoo | forward | must_have+sort | 0.00% | 0.00% |
| `union_hot_n4_h1` | yahoo | tune | must_have+weather | 0.00% | 0.00% |
| `union_hot_n4_h1` | yahoo | forward | must_have+weather | 0.00% | -0.00% |
| `union_hot_n4_h1` | yahoo | tune | must_have+hold_rule | 0.00% | 0.00% |
| `union_hot_n4_h1` | yahoo | forward | must_have+hold_rule | 0.00% | -0.00% |
| `union_hot_n4_h1` | yahoo | tune | must_not+sort | 3.26% | 9.53% |
| `union_hot_n4_h1` | yahoo | forward | must_not+sort | 3.22% | -0.65% |
| `union_hot_n4_h1` | yahoo | tune | must_not+weather | 10.45% | -0.43% |
| `union_hot_n4_h1` | yahoo | forward | must_not+weather | 12.90% | 5.69% |
| `union_hot_n4_h1` | yahoo | tune | must_not+hold_rule | -2.55% | 1.28% |
| `union_hot_n4_h1` | yahoo | forward | must_not+hold_rule | 11.83% | -1.71% |
| `union_hot_n4_h1` | yahoo | tune | sort+weather | 13.63% | -1.46% |
| `union_hot_n4_h1` | yahoo | forward | sort+weather | 10.68% | -0.12% |
| `union_hot_n4_h1` | yahoo | tune | sort+hold_rule | 0.84% | 5.76% |
| `union_hot_n4_h1` | yahoo | forward | sort+hold_rule | -23.66% | -2.24% |
| `union_hot_n4_h1` | yahoo | tune | weather+hold_rule | 4.86% | 2.79% |
| `union_hot_n4_h1` | yahoo | forward | weather+hold_rule | -18.58% | 2.59% |
| `union_hot_score_h1` | yahoo | tune | must_have+must_not | 0.00% | 0.00% |
| `union_hot_score_h1` | yahoo | forward | must_have+must_not | 0.00% | -0.00% |
| `union_hot_score_h1` | yahoo | tune | must_have+sort | 0.00% | 0.00% |
| `union_hot_score_h1` | yahoo | forward | must_have+sort | 0.00% | -0.00% |
| `union_hot_score_h1` | yahoo | tune | must_have+weather | 0.00% | 0.00% |
| `union_hot_score_h1` | yahoo | forward | must_have+weather | 0.00% | 0.00% |
| `union_hot_score_h1` | yahoo | tune | must_have+hold_rule | 0.00% | 0.00% |
| `union_hot_score_h1` | yahoo | forward | must_have+hold_rule | 0.00% | -0.00% |
| `union_hot_score_h1` | yahoo | tune | must_not+sort | 12.03% | 6.51% |
| `union_hot_score_h1` | yahoo | forward | must_not+sort | 5.98% | 6.89% |
| `union_hot_score_h1` | yahoo | tune | must_not+weather | 132.95% | 3.91% |
| `union_hot_score_h1` | yahoo | forward | must_not+weather | 1.82% | 1.51% |
| `union_hot_score_h1` | yahoo | tune | must_not+hold_rule | -1.35% | 0.67% |
| `union_hot_score_h1` | yahoo | forward | must_not+hold_rule | 4.64% | -0.48% |
| `union_hot_score_h1` | yahoo | tune | sort+weather | 139.49% | 1.46% |
| `union_hot_score_h1` | yahoo | forward | sort+weather | 1.47% | 2.58% |
| `union_hot_score_h1` | yahoo | tune | sort+hold_rule | -0.73% | 0.77% |
| `union_hot_score_h1` | yahoo | forward | sort+hold_rule | -12.69% | -4.17% |
| `union_hot_score_h1` | yahoo | tune | weather+hold_rule | 3.59% | -0.50% |
| `union_hot_score_h1` | yahoo | forward | weather+hold_rule | -9.86% | -0.43% |
| `union_hot_score_h3` | yahoo | tune | must_have+must_not | 0.00% | 0.00% |
| `union_hot_score_h3` | yahoo | forward | must_have+must_not | 0.00% | 0.00% |
| `union_hot_score_h3` | yahoo | tune | must_have+sort | 0.00% | 0.00% |
| `union_hot_score_h3` | yahoo | forward | must_have+sort | 0.00% | -0.00% |
| `union_hot_score_h3` | yahoo | tune | must_have+weather | 0.00% | 0.00% |
| `union_hot_score_h3` | yahoo | forward | must_have+weather | 0.00% | -0.00% |
| `union_hot_score_h3` | yahoo | tune | must_have+hold_rule | 0.00% | 0.00% |
| `union_hot_score_h3` | yahoo | forward | must_have+hold_rule | 0.00% | 0.00% |
| `union_hot_score_h3` | yahoo | tune | must_not+sort | -2.97% | 8.60% |
| `union_hot_score_h3` | yahoo | forward | must_not+sort | -5.45% | -4.14% |
| `union_hot_score_h3` | yahoo | tune | must_not+weather | 10.38% | -2.70% |
| `union_hot_score_h3` | yahoo | forward | must_not+weather | 7.86% | -0.11% |
| `union_hot_score_h3` | yahoo | tune | must_not+hold_rule | -0.92% | 0.93% |
| `union_hot_score_h3` | yahoo | forward | must_not+hold_rule | 0.03% | -0.97% |
| `union_hot_score_h3` | yahoo | tune | sort+weather | 20.37% | 3.54% |
| `union_hot_score_h3` | yahoo | forward | sort+weather | 2.34% | -1.31% |
| `union_hot_score_h3` | yahoo | tune | sort+hold_rule | -0.72% | 4.44% |
| `union_hot_score_h3` | yahoo | forward | sort+hold_rule | -5.42% | 2.72% |
| `union_hot_score_h3` | yahoo | tune | weather+hold_rule | 1.68% | 2.58% |
| `union_hot_score_h3` | yahoo | forward | weather+hold_rule | 3.78% | 3.94% |
| `union_ret_5_h1` | yahoo | tune | must_have+must_not | 0.00% | 0.00% |
| `union_ret_5_h1` | yahoo | forward | must_have+must_not | 0.00% | 0.00% |
| `union_ret_5_h1` | yahoo | tune | must_have+sort | 0.00% | -0.00% |
| `union_ret_5_h1` | yahoo | forward | must_have+sort | 0.00% | 0.00% |
| `union_ret_5_h1` | yahoo | tune | must_have+weather | 0.00% | 0.00% |
| `union_ret_5_h1` | yahoo | forward | must_have+weather | 0.00% | 0.00% |
| `union_ret_5_h1` | yahoo | tune | must_have+hold_rule | 0.00% | 0.00% |
| `union_ret_5_h1` | yahoo | forward | must_have+hold_rule | 0.00% | 0.00% |
| `union_ret_5_h1` | yahoo | tune | must_not+sort | 9.20% | 6.56% |
| `union_ret_5_h1` | yahoo | forward | must_not+sort | 4.80% | 6.25% |
| `union_ret_5_h1` | yahoo | tune | must_not+weather | 7.74% | 5.62% |
| `union_ret_5_h1` | yahoo | forward | must_not+weather | 5.11% | 7.70% |
| `union_ret_5_h1` | yahoo | tune | must_not+hold_rule | -0.49% | 0.10% |
| `union_ret_5_h1` | yahoo | forward | must_not+hold_rule | 4.83% | 1.10% |
| `union_ret_5_h1` | yahoo | tune | sort+weather | 6.70% | 1.86% |
| `union_ret_5_h1` | yahoo | forward | sort+weather | 6.36% | 3.99% |
| `union_ret_5_h1` | yahoo | tune | sort+hold_rule | -0.22% | 1.92% |
| `union_ret_5_h1` | yahoo | forward | sort+hold_rule | -12.50% | -1.76% |
| `union_ret_5_h1` | yahoo | tune | weather+hold_rule | 2.03% | 1.49% |
| `union_ret_5_h1` | yahoo | forward | weather+hold_rule | -9.90% | -0.38% |
| `union_ret_5_h3` | yahoo | tune | must_have+must_not | 0.00% | 0.00% |
| `union_ret_5_h3` | yahoo | forward | must_have+must_not | 0.00% | -0.00% |
| `union_ret_5_h3` | yahoo | tune | must_have+sort | 0.00% | 0.00% |
| `union_ret_5_h3` | yahoo | forward | must_have+sort | 0.00% | 0.00% |
| `union_ret_5_h3` | yahoo | tune | must_have+weather | 0.00% | 0.00% |
| `union_ret_5_h3` | yahoo | forward | must_have+weather | 0.00% | 0.00% |
| `union_ret_5_h3` | yahoo | tune | must_have+hold_rule | 0.00% | 0.00% |
| `union_ret_5_h3` | yahoo | forward | must_have+hold_rule | 0.00% | -0.00% |
| `union_ret_5_h3` | yahoo | tune | must_not+sort | -4.15% | 6.53% |
| `union_ret_5_h3` | yahoo | forward | must_not+sort | -2.87% | -8.26% |
| `union_ret_5_h3` | yahoo | tune | must_not+weather | 2.70% | 2.55% |
| `union_ret_5_h3` | yahoo | forward | must_not+weather | 5.27% | 2.85% |
| `union_ret_5_h3` | yahoo | tune | must_not+hold_rule | 0.70% | -1.41% |
| `union_ret_5_h3` | yahoo | forward | must_not+hold_rule | 1.31% | 0.05% |
| `union_ret_5_h3` | yahoo | tune | sort+weather | 14.26% | 0.71% |
| `union_ret_5_h3` | yahoo | forward | sort+weather | -8.59% | 2.52% |
| `union_ret_5_h3` | yahoo | tune | sort+hold_rule | 3.05% | 1.05% |
| `union_ret_5_h3` | yahoo | forward | sort+hold_rule | -6.65% | 2.90% |
| `union_ret_5_h3` | yahoo | tune | weather+hold_rule | 3.15% | 0.92% |
| `union_ret_5_h3` | yahoo | forward | weather+hold_rule | -6.66% | 1.81% |
| `union_w_hot_candle_h1` | yahoo | tune | must_have+must_not | 0.00% | 0.00% |
| `union_w_hot_candle_h1` | yahoo | forward | must_have+must_not | 0.00% | 0.00% |
| `union_w_hot_candle_h1` | yahoo | tune | must_have+sort | 0.00% | 0.00% |
| `union_w_hot_candle_h1` | yahoo | forward | must_have+sort | 0.00% | 0.00% |
| `union_w_hot_candle_h1` | yahoo | tune | must_have+weather | 0.00% | 0.00% |
| `union_w_hot_candle_h1` | yahoo | forward | must_have+weather | 0.00% | 0.00% |
| `union_w_hot_candle_h1` | yahoo | tune | must_have+hold_rule | 0.00% | 0.00% |
| `union_w_hot_candle_h1` | yahoo | forward | must_have+hold_rule | 0.00% | 0.00% |
| `union_w_hot_candle_h1` | yahoo | tune | must_not+sort | 7.60% | 5.31% |
| `union_w_hot_candle_h1` | yahoo | forward | must_not+sort | 1.52% | 4.97% |
| `union_w_hot_candle_h1` | yahoo | tune | must_not+weather | 138.17% | 1.99% |
| `union_w_hot_candle_h1` | yahoo | forward | must_not+weather | 4.59% | 4.58% |
| `union_w_hot_candle_h1` | yahoo | tune | must_not+hold_rule | -1.19% | 0.60% |
| `union_w_hot_candle_h1` | yahoo | forward | must_not+hold_rule | 4.05% | -0.13% |
| `union_w_hot_candle_h1` | yahoo | tune | sort+weather | 149.08% | 3.04% |
| `union_w_hot_candle_h1` | yahoo | forward | sort+weather | 2.55% | 2.46% |
| `union_w_hot_candle_h1` | yahoo | tune | sort+hold_rule | 0.18% | 1.24% |
| `union_w_hot_candle_h1` | yahoo | forward | sort+hold_rule | -12.92% | -2.73% |
| `union_w_hot_candle_h1` | yahoo | tune | weather+hold_rule | 5.57% | 0.22% |
| `union_w_hot_candle_h1` | yahoo | forward | weather+hold_rule | -10.33% | 1.56% |
| `union_w_hot_candle_h3` | yahoo | tune | must_have+must_not | 0.00% | 0.00% |
| `union_w_hot_candle_h3` | yahoo | forward | must_have+must_not | 0.00% | -0.00% |
| `union_w_hot_candle_h3` | yahoo | tune | must_have+sort | 0.00% | 0.00% |
| `union_w_hot_candle_h3` | yahoo | forward | must_have+sort | 0.00% | 0.00% |
| `union_w_hot_candle_h3` | yahoo | tune | must_have+weather | 0.00% | 0.00% |
| `union_w_hot_candle_h3` | yahoo | forward | must_have+weather | 0.00% | 0.00% |
| `union_w_hot_candle_h3` | yahoo | tune | must_have+hold_rule | 0.00% | 0.00% |
| `union_w_hot_candle_h3` | yahoo | forward | must_have+hold_rule | 0.00% | -0.00% |
| `union_w_hot_candle_h3` | yahoo | tune | must_not+sort | -1.83% | 6.95% |
| `union_w_hot_candle_h3` | yahoo | forward | must_not+sort | -5.43% | -9.38% |
| `union_w_hot_candle_h3` | yahoo | tune | must_not+weather | 3.84% | -0.49% |
| `union_w_hot_candle_h3` | yahoo | forward | must_not+weather | 0.66% | 1.15% |
| `union_w_hot_candle_h3` | yahoo | tune | must_not+hold_rule | -0.05% | 1.52% |
| `union_w_hot_candle_h3` | yahoo | forward | must_not+hold_rule | 0.11% | 1.43% |
| `union_w_hot_candle_h3` | yahoo | tune | sort+weather | 8.47% | 3.81% |
| `union_w_hot_candle_h3` | yahoo | forward | sort+weather | 2.60% | 1.63% |
| `union_w_hot_candle_h3` | yahoo | tune | sort+hold_rule | -0.37% | 1.19% |
| `union_w_hot_candle_h3` | yahoo | forward | sort+hold_rule | -4.64% | 1.43% |
| `union_w_hot_candle_h3` | yahoo | tune | weather+hold_rule | -4.84% | -0.31% |
| `union_w_hot_candle_h3` | yahoo | forward | weather+hold_rule | 5.78% | 0.64% |
| `union_w_hot_cond_h1` | yahoo | tune | must_have+must_not | 0.00% | 0.00% |
| `union_w_hot_cond_h1` | yahoo | forward | must_have+must_not | 0.00% | 0.00% |
| `union_w_hot_cond_h1` | yahoo | tune | must_have+sort | 0.00% | 0.00% |
| `union_w_hot_cond_h1` | yahoo | forward | must_have+sort | 0.00% | 0.00% |
| `union_w_hot_cond_h1` | yahoo | tune | must_have+weather | 0.00% | 0.00% |
| `union_w_hot_cond_h1` | yahoo | forward | must_have+weather | 0.00% | 0.00% |
| `union_w_hot_cond_h1` | yahoo | tune | must_have+hold_rule | 0.00% | 0.00% |
| `union_w_hot_cond_h1` | yahoo | forward | must_have+hold_rule | 0.00% | -0.00% |
| `union_w_hot_cond_h1` | yahoo | tune | must_not+sort | 12.67% | 6.47% |
| `union_w_hot_cond_h1` | yahoo | forward | must_not+sort | 6.01% | 6.31% |
| `union_w_hot_cond_h1` | yahoo | tune | must_not+weather | 129.65% | 1.60% |
| `union_w_hot_cond_h1` | yahoo | forward | must_not+weather | 5.19% | 4.58% |
| `union_w_hot_cond_h1` | yahoo | tune | must_not+hold_rule | -0.59% | 0.63% |
| `union_w_hot_cond_h1` | yahoo | forward | must_not+hold_rule | 4.95% | 1.16% |
| `union_w_hot_cond_h1` | yahoo | tune | sort+weather | 133.55% | -1.11% |
| `union_w_hot_cond_h1` | yahoo | forward | sort+weather | 4.51% | -1.51% |
| `union_w_hot_cond_h1` | yahoo | tune | sort+hold_rule | 0.40% | 2.95% |
| `union_w_hot_cond_h1` | yahoo | forward | sort+hold_rule | -13.07% | -1.86% |
| `union_w_hot_cond_h1` | yahoo | tune | weather+hold_rule | 4.64% | 1.07% |
| `union_w_hot_cond_h1` | yahoo | forward | weather+hold_rule | -10.25% | -2.21% |
| `union_w_hot_cond_h3` | yahoo | tune | must_have+must_not | 0.00% | -0.00% |
| `union_w_hot_cond_h3` | yahoo | forward | must_have+must_not | 0.00% | 0.00% |
| `union_w_hot_cond_h3` | yahoo | tune | must_have+sort | 0.00% | -0.00% |
| `union_w_hot_cond_h3` | yahoo | forward | must_have+sort | 0.00% | 0.00% |
| `union_w_hot_cond_h3` | yahoo | tune | must_have+weather | 0.00% | -0.00% |
| `union_w_hot_cond_h3` | yahoo | forward | must_have+weather | 0.00% | 0.00% |
| `union_w_hot_cond_h3` | yahoo | tune | must_have+hold_rule | 0.00% | 0.00% |
| `union_w_hot_cond_h3` | yahoo | forward | must_have+hold_rule | 0.00% | 0.00% |
| `union_w_hot_cond_h3` | yahoo | tune | must_not+sort | -2.44% | 9.91% |
| `union_w_hot_cond_h3` | yahoo | forward | must_not+sort | -5.32% | -2.10% |
| `union_w_hot_cond_h3` | yahoo | tune | must_not+weather | 7.34% | 2.25% |
| `union_w_hot_cond_h3` | yahoo | forward | must_not+weather | -0.48% | 6.50% |
| `union_w_hot_cond_h3` | yahoo | tune | must_not+hold_rule | 1.34% | 1.98% |
| `union_w_hot_cond_h3` | yahoo | forward | must_not+hold_rule | -0.04% | -1.19% |
| `union_w_hot_cond_h3` | yahoo | tune | sort+weather | 12.74% | -0.32% |
| `union_w_hot_cond_h3` | yahoo | forward | sort+weather | -11.22% | 0.69% |
| `union_w_hot_cond_h3` | yahoo | tune | sort+hold_rule | 1.50% | 2.30% |
| `union_w_hot_cond_h3` | yahoo | forward | sort+hold_rule | -6.09% | 3.66% |
| `union_w_hot_cond_h3` | yahoo | tune | weather+hold_rule | 3.67% | 1.11% |
| `union_w_hot_cond_h3` | yahoo | forward | weather+hold_rule | -2.61% | 2.91% |
| `union_hot_n4_holdup` | yahoo | tune | must_have+must_not | 0.00% | -0.00% |
| `union_hot_n4_holdup` | yahoo | forward | must_have+must_not | 0.00% | 0.00% |
| `union_hot_n4_holdup` | yahoo | tune | must_have+sort | 0.00% | -0.00% |
| `union_hot_n4_holdup` | yahoo | forward | must_have+sort | 0.00% | 0.00% |
| `union_hot_n4_holdup` | yahoo | tune | must_have+weather | 0.00% | 0.00% |
| `union_hot_n4_holdup` | yahoo | forward | must_have+weather | 0.00% | 0.00% |
| `union_hot_n4_holdup` | yahoo | tune | must_have+hold_rule | 0.00% | -0.00% |
| `union_hot_n4_holdup` | yahoo | forward | must_have+hold_rule | 0.00% | 0.00% |
| `union_hot_n4_holdup` | yahoo | tune | must_not+sort | 0.99% | -0.04% |
| `union_hot_n4_holdup` | yahoo | forward | must_not+sort | -3.50% | -0.47% |
| `union_hot_n4_holdup` | yahoo | tune | must_not+weather | 14.97% | -4.04% |
| `union_hot_n4_holdup` | yahoo | forward | must_not+weather | 7.70% | 17.17% |
| `union_hot_n4_holdup` | yahoo | tune | must_not+hold_rule | -5.61% | -1.18% |
| `union_hot_n4_holdup` | yahoo | forward | must_not+hold_rule | 3.17% | 1.99% |
| `union_hot_n4_holdup` | yahoo | tune | sort+weather | 10.62% | -11.27% |
| `union_hot_n4_holdup` | yahoo | forward | sort+weather | 0.43% | 3.29% |
| `union_hot_n4_holdup` | yahoo | tune | sort+hold_rule | -18.28% | -19.00% |
| `union_hot_n4_holdup` | yahoo | forward | sort+hold_rule | -33.31% | 4.45% |
| `union_hot_n4_holdup` | yahoo | tune | weather+hold_rule | 2.16% | -5.33% |
| `union_hot_n4_holdup` | yahoo | forward | weather+hold_rule | -31.60% | 7.02% |

## Build-up

Each subset of a recipe's own parts is walked once from the bare mixed list. Permutations reuse those walks. No order is chosen. The full grid is in `RESULTS.json` under `build`.

| recipe | tape | window | parts on | compound | win rate | closed | <30 |
| --- | --- | --- | --- | ---: | ---: | ---: | --- |
| `probable_h3` | clean | tune | bare | 1.06% | 43.67% | 158 |  |
| `probable_h3` | clean | forward | bare | -15.56% | 22.86% | 70 |  |
| `probable_h3` | clean | tune | list_source,weather,hold_rule | -9.61% | 27.42% | 62 |  |
| `probable_h3` | clean | forward | list_source,weather,hold_rule | -2.96% | 22.73% | 22 | yes |
| `probable_h5` | clean | tune | bare | 1.06% | 43.67% | 158 |  |
| `probable_h5` | clean | forward | bare | -15.56% | 22.86% | 70 |  |
| `probable_h5` | clean | tune | list_source,weather,hold_rule | -17.23% | 25.00% | 52 |  |
| `probable_h5` | clean | forward | list_source,weather,hold_rule | -5.72% | 43.75% | 16 | yes |
| `short_extended_h3` | clean | tune | bare | -15.14% | 38.22% | 157 |  |
| `short_extended_h3` | clean | forward | bare | 2.07% | 57.97% | 69 |  |
| `short_extended_h3` | clean | tune | must_have,weather,hold_rule | -4.87% | 52.54% | 59 |  |
| `short_extended_h3` | clean | forward | must_have,weather,hold_rule | -14.67% | 61.90% | 21 | yes |
| `union_candle_score_h1` | clean | tune | bare | 1.06% | 43.67% | 158 |  |
| `union_candle_score_h1` | clean | forward | bare | -15.56% | 22.86% | 70 |  |
| `union_candle_score_h1` | clean | tune | must_not,sort,weather,hold_rule | 3.07% | 41.03% | 78 |  |
| `union_candle_score_h1` | clean | forward | must_not,sort,weather,hold_rule | -5.78% | 38.10% | 42 |  |
| `union_candle_score_h3` | clean | tune | bare | 1.06% | 43.67% | 158 |  |
| `union_candle_score_h3` | clean | forward | bare | -15.56% | 22.86% | 70 |  |
| `union_candle_score_h3` | clean | tune | must_not,sort,weather,hold_rule | 4.21% | 40.00% | 55 |  |
| `union_candle_score_h3` | clean | forward | must_not,sort,weather,hold_rule | 8.50% | 44.00% | 25 | yes |
| `union_cond_h1` | clean | tune | bare | 1.06% | 43.67% | 158 |  |
| `union_cond_h1` | clean | forward | bare | -15.56% | 22.86% | 70 |  |
| `union_cond_h1` | clean | tune | must_not,sort,weather,hold_rule | -4.46% | 39.02% | 82 |  |
| `union_cond_h1` | clean | forward | must_not,sort,weather,hold_rule | -11.51% | 30.43% | 46 |  |
| `union_cond_h3` | clean | tune | bare | 1.06% | 43.67% | 158 |  |
| `union_cond_h3` | clean | forward | bare | -15.56% | 22.86% | 70 |  |
| `union_cond_h3` | clean | tune | must_not,sort,weather,hold_rule | -3.07% | 34.48% | 58 |  |
| `union_cond_h3` | clean | forward | must_not,sort,weather,hold_rule | -9.16% | 20.00% | 30 |  |
| `union_cond_n4_h3` | clean | tune | bare | -1.13% | 47.50% | 80 |  |
| `union_cond_n4_h3` | clean | forward | bare | -13.65% | 25.00% | 40 |  |
| `union_cond_n4_h3` | clean | tune | must_not,sort,weather,hold_rule | -2.44% | 26.92% | 26 | yes |
| `union_cond_n4_h3` | clean | forward | must_not,sort,weather,hold_rule | -0.13% | 40.00% | 20 | yes |
| `union_hot_n12_h1` | clean | tune | bare | -3.88% | 41.30% | 230 |  |
| `union_hot_n12_h1` | clean | forward | bare | -15.18% | 23.47% | 98 |  |
| `union_hot_n12_h1` | clean | tune | must_not,sort,weather,hold_rule | -7.74% | 44.14% | 111 |  |
| `union_hot_n12_h1` | clean | forward | must_not,sort,weather,hold_rule | 0.85% | 33.33% | 54 |  |
| `union_hot_n4_h1` | clean | tune | bare | -1.13% | 47.50% | 80 |  |
| `union_hot_n4_h1` | clean | forward | bare | -13.65% | 25.00% | 40 |  |
| `union_hot_n4_h1` | clean | tune | must_not,sort,weather,hold_rule | 13.92% | 52.94% | 34 |  |
| `union_hot_n4_h1` | clean | forward | must_not,sort,weather,hold_rule | 23.21% | 44.00% | 25 | yes |
| `union_hot_score_h1` | clean | tune | bare | 1.06% | 43.67% | 158 |  |
| `union_hot_score_h1` | clean | forward | bare | -15.56% | 22.86% | 70 |  |
| `union_hot_score_h1` | clean | tune | must_not,sort,weather,hold_rule | -4.16% | 45.21% | 73 |  |
| `union_hot_score_h1` | clean | forward | must_not,sort,weather,hold_rule | 7.81% | 35.00% | 40 |  |
| `union_hot_score_h3` | clean | tune | bare | 1.06% | 43.67% | 158 |  |
| `union_hot_score_h3` | clean | forward | bare | -15.56% | 22.86% | 70 |  |
| `union_hot_score_h3` | clean | tune | must_not,sort,weather,hold_rule | 4.97% | 36.84% | 57 |  |
| `union_hot_score_h3` | clean | forward | must_not,sort,weather,hold_rule | 6.69% | 29.03% | 31 |  |
| `union_ret_5_h1` | clean | tune | bare | 1.06% | 43.67% | 158 |  |
| `union_ret_5_h1` | clean | forward | bare | -15.56% | 22.86% | 70 |  |
| `union_ret_5_h1` | clean | tune | must_not,sort,weather,hold_rule | -0.44% | 43.84% | 73 |  |
| `union_ret_5_h1` | clean | forward | must_not,sort,weather,hold_rule | 5.21% | 37.21% | 43 |  |
| `union_ret_5_h3` | clean | tune | bare | 1.06% | 43.67% | 158 |  |
| `union_ret_5_h3` | clean | forward | bare | -15.56% | 22.86% | 70 |  |
| `union_ret_5_h3` | clean | tune | must_not,sort,weather,hold_rule | 9.68% | 39.34% | 61 |  |
| `union_ret_5_h3` | clean | forward | must_not,sort,weather,hold_rule | 7.33% | 30.43% | 23 | yes |
| `union_w_hot_candle_h1` | clean | tune | bare | 1.06% | 43.67% | 158 |  |
| `union_w_hot_candle_h1` | clean | forward | bare | -15.56% | 22.86% | 70 |  |
| `union_w_hot_candle_h1` | clean | tune | must_not,sort,weather,hold_rule | 0.22% | 49.33% | 75 |  |
| `union_w_hot_candle_h1` | clean | forward | must_not,sort,weather,hold_rule | 8.37% | 40.48% | 42 |  |
| `union_w_hot_candle_h3` | clean | tune | bare | 1.06% | 43.67% | 158 |  |
| `union_w_hot_candle_h3` | clean | forward | bare | -15.56% | 22.86% | 70 |  |
| `union_w_hot_candle_h3` | clean | tune | must_not,sort,weather,hold_rule | 2.80% | 40.68% | 59 |  |
| `union_w_hot_candle_h3` | clean | forward | must_not,sort,weather,hold_rule | 4.69% | 25.93% | 27 | yes |
| `union_w_hot_cond_h1` | clean | tune | bare | 1.06% | 43.67% | 158 |  |
| `union_w_hot_cond_h1` | clean | forward | bare | -15.56% | 22.86% | 70 |  |
| `union_w_hot_cond_h1` | clean | tune | must_not,sort,weather,hold_rule | -3.17% | 46.75% | 77 |  |
| `union_w_hot_cond_h1` | clean | forward | must_not,sort,weather,hold_rule | 11.10% | 39.53% | 43 |  |
| `union_w_hot_cond_h3` | clean | tune | bare | 1.06% | 43.67% | 158 |  |
| `union_w_hot_cond_h3` | clean | forward | bare | -15.56% | 22.86% | 70 |  |
| `union_w_hot_cond_h3` | clean | tune | must_not,sort,weather,hold_rule | 3.19% | 34.48% | 58 |  |
| `union_w_hot_cond_h3` | clean | forward | must_not,sort,weather,hold_rule | 12.56% | 32.26% | 31 |  |
| `union_hot_n4_holdup` | clean | tune | bare | -1.13% | 47.50% | 80 |  |
| `union_hot_n4_holdup` | clean | forward | bare | -13.65% | 25.00% | 40 |  |
| `union_hot_n4_holdup` | clean | tune | must_not,sort,weather,hold_rule | 33.40% | 60.00% | 30 |  |
| `union_hot_n4_holdup` | clean | forward | must_not,sort,weather,hold_rule | 37.22% | 45.45% | 22 | yes |
| `probable_h3` | yahoo | tune | bare | 3.03% | 43.75% | 160 |  |
| `probable_h3` | yahoo | forward | bare | -15.82% | 22.86% | 70 |  |
| `probable_h3` | yahoo | tune | list_source,weather,hold_rule | -5.75% | 32.00% | 50 |  |
| `probable_h3` | yahoo | forward | list_source,weather,hold_rule | -2.79% | 25.00% | 20 | yes |
| `probable_h5` | yahoo | tune | bare | 3.03% | 43.75% | 160 |  |
| `probable_h5` | yahoo | forward | bare | -15.82% | 22.86% | 70 |  |
| `probable_h5` | yahoo | tune | list_source,weather,hold_rule | -16.81% | 28.57% | 49 |  |
| `probable_h5` | yahoo | forward | list_source,weather,hold_rule | -5.02% | 46.67% | 15 | yes |
| `short_extended_h3` | yahoo | tune | bare | -16.12% | 37.74% | 159 |  |
| `short_extended_h3` | yahoo | forward | bare | 2.07% | 57.97% | 69 |  |
| `short_extended_h3` | yahoo | tune | must_have,weather,hold_rule | -6.51% | 49.23% | 65 |  |
| `short_extended_h3` | yahoo | forward | must_have,weather,hold_rule | -15.74% | 60.00% | 20 | yes |
| `union_candle_score_h1` | yahoo | tune | bare | 3.03% | 43.75% | 160 |  |
| `union_candle_score_h1` | yahoo | forward | bare | -15.82% | 22.86% | 70 |  |
| `union_candle_score_h1` | yahoo | tune | must_not,sort,weather,hold_rule | 2.13% | 40.74% | 81 |  |
| `union_candle_score_h1` | yahoo | forward | must_not,sort,weather,hold_rule | -5.86% | 38.10% | 42 |  |
| `union_candle_score_h3` | yahoo | tune | bare | 3.03% | 43.75% | 160 |  |
| `union_candle_score_h3` | yahoo | forward | bare | -15.82% | 22.86% | 70 |  |
| `union_candle_score_h3` | yahoo | tune | must_not,sort,weather,hold_rule | 5.19% | 40.00% | 55 |  |
| `union_candle_score_h3` | yahoo | forward | must_not,sort,weather,hold_rule | 8.38% | 40.74% | 27 | yes |
| `union_cond_h1` | yahoo | tune | bare | 3.03% | 43.75% | 160 |  |
| `union_cond_h1` | yahoo | forward | bare | -15.82% | 22.86% | 70 |  |
| `union_cond_h1` | yahoo | tune | must_not,sort,weather,hold_rule | -5.15% | 38.55% | 83 |  |
| `union_cond_h1` | yahoo | forward | must_not,sort,weather,hold_rule | -11.47% | 31.11% | 45 |  |
| `union_cond_h3` | yahoo | tune | bare | 3.03% | 43.75% | 160 |  |
| `union_cond_h3` | yahoo | forward | bare | -15.82% | 22.86% | 70 |  |
| `union_cond_h3` | yahoo | tune | must_not,sort,weather,hold_rule | -3.00% | 33.33% | 60 |  |
| `union_cond_h3` | yahoo | forward | must_not,sort,weather,hold_rule | -9.23% | 20.69% | 29 | yes |
| `union_cond_n4_h3` | yahoo | tune | bare | -0.81% | 47.50% | 80 |  |
| `union_cond_n4_h3` | yahoo | forward | bare | -13.86% | 25.00% | 40 |  |
| `union_cond_n4_h3` | yahoo | tune | must_not,sort,weather,hold_rule | -2.45% | 25.93% | 27 | yes |
| `union_cond_n4_h3` | yahoo | forward | must_not,sort,weather,hold_rule | -0.41% | 40.00% | 20 | yes |
| `union_hot_n12_h1` | yahoo | tune | bare | -3.16% | 41.28% | 235 |  |
| `union_hot_n12_h1` | yahoo | forward | bare | -15.06% | 23.71% | 97 |  |
| `union_hot_n12_h1` | yahoo | tune | must_not,sort,weather,hold_rule | -5.85% | 45.00% | 120 |  |
| `union_hot_n12_h1` | yahoo | forward | must_not,sort,weather,hold_rule | 0.98% | 35.19% | 54 |  |
| `union_hot_n4_h1` | yahoo | tune | bare | -0.81% | 47.50% | 80 |  |
| `union_hot_n4_h1` | yahoo | forward | bare | -13.86% | 25.00% | 40 |  |
| `union_hot_n4_h1` | yahoo | tune | must_not,sort,weather,hold_rule | 15.98% | 50.00% | 40 |  |
| `union_hot_n4_h1` | yahoo | forward | must_not,sort,weather,hold_rule | 24.09% | 46.15% | 26 | yes |
| `union_hot_score_h1` | yahoo | tune | bare | 3.03% | 43.75% | 160 |  |
| `union_hot_score_h1` | yahoo | forward | bare | -15.82% | 22.86% | 70 |  |
| `union_hot_score_h1` | yahoo | tune | must_not,sort,weather,hold_rule | -1.03% | 46.34% | 82 |  |
| `union_hot_score_h1` | yahoo | forward | must_not,sort,weather,hold_rule | 8.32% | 37.50% | 40 |  |
| `union_hot_score_h3` | yahoo | tune | bare | 3.03% | 43.75% | 160 |  |
| `union_hot_score_h3` | yahoo | forward | bare | -15.82% | 22.86% | 70 |  |
| `union_hot_score_h3` | yahoo | tune | must_not,sort,weather,hold_rule | 6.12% | 37.93% | 58 |  |
| `union_hot_score_h3` | yahoo | forward | must_not,sort,weather,hold_rule | 6.44% | 26.32% | 19 | yes |
| `union_ret_5_h1` | yahoo | tune | bare | 3.03% | 43.75% | 160 |  |
| `union_ret_5_h1` | yahoo | forward | bare | -15.82% | 22.86% | 70 |  |
| `union_ret_5_h1` | yahoo | tune | must_not,sort,weather,hold_rule | 0.34% | 44.58% | 83 |  |
| `union_ret_5_h1` | yahoo | forward | must_not,sort,weather,hold_rule | 5.81% | 39.53% | 43 |  |
| `union_ret_5_h3` | yahoo | tune | bare | 3.03% | 43.75% | 160 |  |
| `union_ret_5_h3` | yahoo | forward | bare | -15.82% | 22.86% | 70 |  |
| `union_ret_5_h3` | yahoo | tune | must_not,sort,weather,hold_rule | 9.40% | 44.26% | 61 |  |
| `union_ret_5_h3` | yahoo | forward | must_not,sort,weather,hold_rule | 7.33% | 30.43% | 23 | yes |
| `union_w_hot_candle_h1` | yahoo | tune | bare | 3.03% | 43.75% | 160 |  |
| `union_w_hot_candle_h1` | yahoo | forward | bare | -15.82% | 22.86% | 70 |  |
| `union_w_hot_candle_h1` | yahoo | tune | must_not,sort,weather,hold_rule | -0.51% | 48.15% | 81 |  |
| `union_w_hot_candle_h1` | yahoo | forward | must_not,sort,weather,hold_rule | 8.79% | 41.86% | 43 |  |
| `union_w_hot_candle_h3` | yahoo | tune | bare | 3.03% | 43.75% | 160 |  |
| `union_w_hot_candle_h3` | yahoo | forward | bare | -15.82% | 22.86% | 70 |  |
| `union_w_hot_candle_h3` | yahoo | tune | must_not,sort,weather,hold_rule | 2.93% | 41.67% | 60 |  |
| `union_w_hot_candle_h3` | yahoo | forward | must_not,sort,weather,hold_rule | 4.70% | 28.57% | 28 | yes |
| `union_w_hot_cond_h1` | yahoo | tune | bare | 3.03% | 43.75% | 160 |  |
| `union_w_hot_cond_h1` | yahoo | forward | bare | -15.82% | 22.86% | 70 |  |
| `union_w_hot_cond_h1` | yahoo | tune | must_not,sort,weather,hold_rule | -1.62% | 46.43% | 84 |  |
| `union_w_hot_cond_h1` | yahoo | forward | must_not,sort,weather,hold_rule | 11.75% | 41.86% | 43 |  |
| `union_w_hot_cond_h3` | yahoo | tune | bare | 3.03% | 43.75% | 160 |  |
| `union_w_hot_cond_h3` | yahoo | forward | bare | -15.82% | 22.86% | 70 |  |
| `union_w_hot_cond_h3` | yahoo | tune | must_not,sort,weather,hold_rule | 3.73% | 37.70% | 61 |  |
| `union_w_hot_cond_h3` | yahoo | forward | must_not,sort,weather,hold_rule | 12.63% | 27.59% | 29 | yes |
| `union_hot_n4_holdup` | yahoo | tune | bare | -0.81% | 47.50% | 80 |  |
| `union_hot_n4_holdup` | yahoo | forward | bare | -13.86% | 25.00% | 40 |  |
| `union_hot_n4_holdup` | yahoo | tune | must_not,sort,weather,hold_rule | 35.82% | 64.52% | 31 |  |
| `union_hot_n4_holdup` | yahoo | forward | must_not,sort,weather,hold_rule | 39.32% | 45.83% | 24 | yes |

## N

N is 4, 8, or 12, with every other part left on.

| recipe | tape | window | N | compound | win rate | closed | <30 |
| --- | --- | --- | ---: | ---: | ---: | ---: | --- |
| `probable_h3` | clean | tune | 4 | -7.30% | 23.53% | 34 |  |
| `probable_h3` | clean | forward | 4 | -7.80% | 37.50% | 8 | yes |
| `probable_h3` | clean | tune | 8 | -9.61% | 27.42% | 62 |  |
| `probable_h3` | clean | forward | 8 | -2.96% | 22.73% | 22 | yes |
| `probable_h3` | clean | tune | 12 | -9.61% | 27.42% | 62 |  |
| `probable_h3` | clean | forward | 12 | -2.96% | 22.73% | 22 | yes |
| `probable_h5` | clean | tune | 4 | -16.20% | 30.00% | 30 |  |
| `probable_h5` | clean | forward | 4 | -4.50% | 37.50% | 8 | yes |
| `probable_h5` | clean | tune | 8 | -17.23% | 25.00% | 52 |  |
| `probable_h5` | clean | forward | 8 | -5.72% | 43.75% | 16 | yes |
| `probable_h5` | clean | tune | 12 | -17.23% | 25.00% | 52 |  |
| `probable_h5` | clean | forward | 12 | -5.72% | 43.75% | 16 | yes |
| `short_extended_h3` | clean | tune | 4 | -9.39% | 50.00% | 30 |  |
| `short_extended_h3` | clean | forward | 4 | -15.32% | 60.00% | 10 | yes |
| `short_extended_h3` | clean | tune | 8 | -4.87% | 52.54% | 59 |  |
| `short_extended_h3` | clean | forward | 8 | -14.67% | 61.90% | 21 | yes |
| `short_extended_h3` | clean | tune | 12 | -2.28% | 49.44% | 89 |  |
| `short_extended_h3` | clean | forward | 12 | -9.78% | 67.74% | 31 |  |
| `union_candle_score_h1` | clean | tune | 4 | -11.47% | 33.33% | 42 |  |
| `union_candle_score_h1` | clean | forward | 4 | -7.21% | 38.46% | 26 | yes |
| `union_candle_score_h1` | clean | tune | 8 | 3.07% | 41.03% | 78 |  |
| `union_candle_score_h1` | clean | forward | 8 | -5.78% | 38.10% | 42 |  |
| `union_candle_score_h1` | clean | tune | 12 | -2.91% | 39.47% | 114 |  |
| `union_candle_score_h1` | clean | forward | 12 | -9.39% | 33.93% | 56 |  |
| `union_candle_score_h3` | clean | tune | 4 | -8.50% | 28.57% | 28 | yes |
| `union_candle_score_h3` | clean | forward | 4 | -9.13% | 26.67% | 15 | yes |
| `union_candle_score_h3` | clean | tune | 8 | 4.21% | 40.00% | 55 |  |
| `union_candle_score_h3` | clean | forward | 8 | 8.50% | 44.00% | 25 | yes |
| `union_candle_score_h3` | clean | tune | 12 | 2.60% | 33.33% | 81 |  |
| `union_candle_score_h3` | clean | forward | 12 | 1.95% | 31.43% | 35 |  |
| `union_cond_h1` | clean | tune | 4 | -11.33% | 32.56% | 43 |  |
| `union_cond_h1` | clean | forward | 4 | -7.39% | 39.29% | 28 | yes |
| `union_cond_h1` | clean | tune | 8 | -4.46% | 39.02% | 82 |  |
| `union_cond_h1` | clean | forward | 8 | -11.51% | 30.43% | 46 |  |
| `union_cond_h1` | clean | tune | 12 | -4.53% | 40.68% | 118 |  |
| `union_cond_h1` | clean | forward | 12 | -12.23% | 25.42% | 59 |  |
| `union_cond_h3` | clean | tune | 4 | -2.44% | 26.92% | 26 | yes |
| `union_cond_h3` | clean | forward | 4 | -0.13% | 40.00% | 20 | yes |
| `union_cond_h3` | clean | tune | 8 | -3.07% | 34.48% | 58 |  |
| `union_cond_h3` | clean | forward | 8 | -9.16% | 20.00% | 30 |  |
| `union_cond_h3` | clean | tune | 12 | -1.09% | 37.78% | 90 |  |
| `union_cond_h3` | clean | forward | 12 | -10.05% | 20.51% | 39 |  |
| `union_cond_n4_h3` | clean | tune | 4 | -2.44% | 26.92% | 26 | yes |
| `union_cond_n4_h3` | clean | forward | 4 | -0.13% | 40.00% | 20 | yes |
| `union_cond_n4_h3` | clean | tune | 8 | -3.07% | 34.48% | 58 |  |
| `union_cond_n4_h3` | clean | forward | 8 | -9.16% | 20.00% | 30 |  |
| `union_cond_n4_h3` | clean | tune | 12 | -1.09% | 37.78% | 90 |  |
| `union_cond_n4_h3` | clean | forward | 12 | -10.05% | 20.51% | 39 |  |
| `union_hot_n12_h1` | clean | tune | 4 | 13.92% | 52.94% | 34 |  |
| `union_hot_n12_h1` | clean | forward | 4 | 23.21% | 44.00% | 25 | yes |
| `union_hot_n12_h1` | clean | tune | 8 | -4.16% | 45.21% | 73 |  |
| `union_hot_n12_h1` | clean | forward | 8 | 7.81% | 35.00% | 40 |  |
| `union_hot_n12_h1` | clean | tune | 12 | -7.74% | 44.14% | 111 |  |
| `union_hot_n12_h1` | clean | forward | 12 | 0.85% | 33.33% | 54 |  |
| `union_hot_n4_h1` | clean | tune | 4 | 13.92% | 52.94% | 34 |  |
| `union_hot_n4_h1` | clean | forward | 4 | 23.21% | 44.00% | 25 | yes |
| `union_hot_n4_h1` | clean | tune | 8 | -4.16% | 45.21% | 73 |  |
| `union_hot_n4_h1` | clean | forward | 8 | 7.81% | 35.00% | 40 |  |
| `union_hot_n4_h1` | clean | tune | 12 | -7.74% | 44.14% | 111 |  |
| `union_hot_n4_h1` | clean | forward | 12 | 0.85% | 33.33% | 54 |  |
| `union_hot_score_h1` | clean | tune | 4 | 13.92% | 52.94% | 34 |  |
| `union_hot_score_h1` | clean | forward | 4 | 23.21% | 44.00% | 25 | yes |
| `union_hot_score_h1` | clean | tune | 8 | -4.16% | 45.21% | 73 |  |
| `union_hot_score_h1` | clean | forward | 8 | 7.81% | 35.00% | 40 |  |
| `union_hot_score_h1` | clean | tune | 12 | -7.74% | 44.14% | 111 |  |
| `union_hot_score_h1` | clean | forward | 12 | 0.85% | 33.33% | 54 |  |
| `union_hot_score_h3` | clean | tune | 4 | 16.47% | 45.83% | 24 | yes |
| `union_hot_score_h3` | clean | forward | 4 | 19.54% | 31.25% | 16 | yes |
| `union_hot_score_h3` | clean | tune | 8 | 4.97% | 36.84% | 57 |  |
| `union_hot_score_h3` | clean | forward | 8 | 6.69% | 29.03% | 31 |  |
| `union_hot_score_h3` | clean | tune | 12 | -1.46% | 33.73% | 83 |  |
| `union_hot_score_h3` | clean | forward | 12 | 0.22% | 26.47% | 34 |  |
| `union_ret_5_h1` | clean | tune | 4 | 18.13% | 58.82% | 34 |  |
| `union_ret_5_h1` | clean | forward | 4 | 19.86% | 37.04% | 27 | yes |
| `union_ret_5_h1` | clean | tune | 8 | -0.44% | 43.84% | 73 |  |
| `union_ret_5_h1` | clean | forward | 8 | 5.21% | 37.21% | 43 |  |
| `union_ret_5_h1` | clean | tune | 12 | -4.70% | 45.87% | 109 |  |
| `union_ret_5_h1` | clean | forward | 12 | -1.86% | 32.73% | 55 |  |
| `union_ret_5_h3` | clean | tune | 4 | 5.42% | 36.00% | 25 | yes |
| `union_ret_5_h3` | clean | forward | 4 | 21.99% | 22.22% | 18 | yes |
| `union_ret_5_h3` | clean | tune | 8 | 9.68% | 39.34% | 61 |  |
| `union_ret_5_h3` | clean | forward | 8 | 7.33% | 30.43% | 23 | yes |
| `union_ret_5_h3` | clean | tune | 12 | 3.35% | 38.64% | 88 |  |
| `union_ret_5_h3` | clean | forward | 12 | -1.03% | 24.32% | 37 |  |
| `union_w_hot_candle_h1` | clean | tune | 4 | 14.10% | 50.00% | 34 |  |
| `union_w_hot_candle_h1` | clean | forward | 4 | 20.57% | 46.15% | 26 | yes |
| `union_w_hot_candle_h1` | clean | tune | 8 | 0.22% | 49.33% | 75 |  |
| `union_w_hot_candle_h1` | clean | forward | 8 | 8.37% | 40.48% | 42 |  |
| `union_w_hot_candle_h1` | clean | tune | 12 | -3.12% | 45.54% | 112 |  |
| `union_w_hot_candle_h1` | clean | forward | 12 | 1.28% | 35.71% | 56 |  |
| `union_w_hot_candle_h3` | clean | tune | 4 | 5.97% | 39.29% | 28 | yes |
| `union_w_hot_candle_h3` | clean | forward | 4 | 18.94% | 33.33% | 15 | yes |
| `union_w_hot_candle_h3` | clean | tune | 8 | 2.80% | 40.68% | 59 |  |
| `union_w_hot_candle_h3` | clean | forward | 8 | 4.69% | 25.93% | 27 | yes |
| `union_w_hot_candle_h3` | clean | tune | 12 | -1.34% | 32.56% | 86 |  |
| `union_w_hot_candle_h3` | clean | forward | 12 | -0.11% | 29.41% | 34 |  |
| `union_w_hot_cond_h1` | clean | tune | 4 | 8.69% | 48.57% | 35 |  |
| `union_w_hot_cond_h1` | clean | forward | 4 | 22.04% | 40.74% | 27 | yes |
| `union_w_hot_cond_h1` | clean | tune | 8 | -3.17% | 46.75% | 77 |  |
| `union_w_hot_cond_h1` | clean | forward | 8 | 11.10% | 39.53% | 43 |  |
| `union_w_hot_cond_h1` | clean | tune | 12 | -4.00% | 46.49% | 114 |  |
| `union_w_hot_cond_h1` | clean | forward | 12 | 2.52% | 37.50% | 56 |  |
| `union_w_hot_cond_h3` | clean | tune | 4 | 3.48% | 36.00% | 25 | yes |
| `union_w_hot_cond_h3` | clean | forward | 4 | 28.43% | 25.00% | 12 | yes |
| `union_w_hot_cond_h3` | clean | tune | 8 | 3.19% | 34.48% | 58 |  |
| `union_w_hot_cond_h3` | clean | forward | 8 | 12.56% | 32.26% | 31 |  |
| `union_w_hot_cond_h3` | clean | tune | 12 | 0.58% | 32.94% | 85 |  |
| `union_w_hot_cond_h3` | clean | forward | 12 | 2.99% | 29.73% | 37 |  |
| `union_hot_n4_holdup` | clean | tune | 4 | 33.40% | 60.00% | 30 |  |
| `union_hot_n4_holdup` | clean | forward | 4 | 37.22% | 45.45% | 22 | yes |
| `union_hot_n4_holdup` | clean | tune | 8 | 6.06% | 45.31% | 64 |  |
| `union_hot_n4_holdup` | clean | forward | 8 | 14.99% | 35.29% | 34 |  |
| `union_hot_n4_holdup` | clean | tune | 12 | -1.76% | 37.11% | 97 |  |
| `union_hot_n4_holdup` | clean | forward | 12 | 9.40% | 32.56% | 43 |  |
| `probable_h3` | yahoo | tune | 4 | 0.67% | 23.53% | 34 |  |
| `probable_h3` | yahoo | forward | 4 | -7.50% | 37.50% | 8 | yes |
| `probable_h3` | yahoo | tune | 8 | -5.75% | 32.00% | 50 |  |
| `probable_h3` | yahoo | forward | 8 | -2.79% | 25.00% | 20 | yes |
| `probable_h3` | yahoo | tune | 12 | -5.75% | 32.00% | 50 |  |
| `probable_h3` | yahoo | forward | 12 | -2.79% | 25.00% | 20 | yes |
| `probable_h5` | yahoo | tune | 4 | -15.35% | 37.04% | 27 | yes |
| `probable_h5` | yahoo | forward | 4 | -3.17% | 42.86% | 7 | yes |
| `probable_h5` | yahoo | tune | 8 | -16.81% | 28.57% | 49 |  |
| `probable_h5` | yahoo | forward | 8 | -5.02% | 46.67% | 15 | yes |
| `probable_h5` | yahoo | tune | 12 | -16.81% | 28.57% | 49 |  |
| `probable_h5` | yahoo | forward | 12 | -5.02% | 46.67% | 15 | yes |
| `short_extended_h3` | yahoo | tune | 4 | -9.63% | 47.06% | 34 |  |
| `short_extended_h3` | yahoo | forward | 4 | -17.06% | 60.00% | 10 | yes |
| `short_extended_h3` | yahoo | tune | 8 | -6.51% | 49.23% | 65 |  |
| `short_extended_h3` | yahoo | forward | 8 | -15.74% | 60.00% | 20 | yes |
| `short_extended_h3` | yahoo | tune | 12 | -3.52% | 46.88% | 96 |  |
| `short_extended_h3` | yahoo | forward | 12 | -10.64% | 66.67% | 30 |  |
| `union_candle_score_h1` | yahoo | tune | 4 | -11.16% | 33.33% | 42 |  |
| `union_candle_score_h1` | yahoo | forward | 4 | -7.20% | 38.46% | 26 | yes |
| `union_candle_score_h1` | yahoo | tune | 8 | 2.13% | 40.74% | 81 |  |
| `union_candle_score_h1` | yahoo | forward | 8 | -5.86% | 38.10% | 42 |  |
| `union_candle_score_h1` | yahoo | tune | 12 | -4.24% | 38.98% | 118 |  |
| `union_candle_score_h1` | yahoo | forward | 12 | -9.44% | 33.93% | 56 |  |
| `union_candle_score_h3` | yahoo | tune | 4 | -8.24% | 28.57% | 28 | yes |
| `union_candle_score_h3` | yahoo | forward | 4 | -9.11% | 25.00% | 16 | yes |
| `union_candle_score_h3` | yahoo | tune | 8 | 5.19% | 40.00% | 55 |  |
| `union_candle_score_h3` | yahoo | forward | 8 | 8.38% | 40.74% | 27 | yes |
| `union_candle_score_h3` | yahoo | tune | 12 | 3.23% | 32.53% | 83 |  |
| `union_candle_score_h3` | yahoo | forward | 12 | 1.87% | 32.35% | 34 |  |
| `union_cond_h1` | yahoo | tune | 4 | -13.01% | 31.82% | 44 |  |
| `union_cond_h1` | yahoo | forward | 4 | -7.85% | 39.29% | 28 | yes |
| `union_cond_h1` | yahoo | tune | 8 | -5.15% | 38.55% | 83 |  |
| `union_cond_h1` | yahoo | forward | 8 | -11.47% | 31.11% | 45 |  |
| `union_cond_h1` | yahoo | tune | 12 | -5.08% | 40.00% | 120 |  |
| `union_cond_h1` | yahoo | forward | 12 | -11.37% | 26.32% | 57 |  |
| `union_cond_h3` | yahoo | tune | 4 | -2.45% | 25.93% | 27 | yes |
| `union_cond_h3` | yahoo | forward | 4 | -0.41% | 40.00% | 20 | yes |
| `union_cond_h3` | yahoo | tune | 8 | -3.00% | 33.33% | 60 |  |
| `union_cond_h3` | yahoo | forward | 8 | -9.23% | 20.69% | 29 | yes |
| `union_cond_h3` | yahoo | tune | 12 | -0.95% | 37.36% | 91 |  |
| `union_cond_h3` | yahoo | forward | 12 | -9.35% | 21.05% | 38 |  |
| `union_cond_n4_h3` | yahoo | tune | 4 | -2.45% | 25.93% | 27 | yes |
| `union_cond_n4_h3` | yahoo | forward | 4 | -0.41% | 40.00% | 20 | yes |
| `union_cond_n4_h3` | yahoo | tune | 8 | -3.00% | 33.33% | 60 |  |
| `union_cond_n4_h3` | yahoo | forward | 8 | -9.23% | 20.69% | 29 | yes |
| `union_cond_n4_h3` | yahoo | tune | 12 | -0.95% | 37.36% | 91 |  |
| `union_cond_n4_h3` | yahoo | forward | 12 | -9.35% | 21.05% | 38 |  |
| `union_hot_n12_h1` | yahoo | tune | 4 | 15.98% | 50.00% | 40 |  |
| `union_hot_n12_h1` | yahoo | forward | 4 | 24.09% | 46.15% | 26 | yes |
| `union_hot_n12_h1` | yahoo | tune | 8 | -1.03% | 46.34% | 82 |  |
| `union_hot_n12_h1` | yahoo | forward | 8 | 8.32% | 37.50% | 40 |  |
| `union_hot_n12_h1` | yahoo | tune | 12 | -5.85% | 45.00% | 120 |  |
| `union_hot_n12_h1` | yahoo | forward | 12 | 0.98% | 35.19% | 54 |  |
| `union_hot_n4_h1` | yahoo | tune | 4 | 15.98% | 50.00% | 40 |  |
| `union_hot_n4_h1` | yahoo | forward | 4 | 24.09% | 46.15% | 26 | yes |
| `union_hot_n4_h1` | yahoo | tune | 8 | -1.03% | 46.34% | 82 |  |
| `union_hot_n4_h1` | yahoo | forward | 8 | 8.32% | 37.50% | 40 |  |
| `union_hot_n4_h1` | yahoo | tune | 12 | -5.85% | 45.00% | 120 |  |
| `union_hot_n4_h1` | yahoo | forward | 12 | 0.98% | 35.19% | 54 |  |
| `union_hot_score_h1` | yahoo | tune | 4 | 15.98% | 50.00% | 40 |  |
| `union_hot_score_h1` | yahoo | forward | 4 | 24.09% | 46.15% | 26 | yes |
| `union_hot_score_h1` | yahoo | tune | 8 | -1.03% | 46.34% | 82 |  |
| `union_hot_score_h1` | yahoo | forward | 8 | 8.32% | 37.50% | 40 |  |
| `union_hot_score_h1` | yahoo | tune | 12 | -5.85% | 45.00% | 120 |  |
| `union_hot_score_h1` | yahoo | forward | 12 | 0.98% | 35.19% | 54 |  |
| `union_hot_score_h3` | yahoo | tune | 4 | 18.52% | 47.83% | 23 | yes |
| `union_hot_score_h3` | yahoo | forward | 4 | 19.59% | 38.89% | 18 | yes |
| `union_hot_score_h3` | yahoo | tune | 8 | 6.12% | 37.93% | 58 |  |
| `union_hot_score_h3` | yahoo | forward | 8 | 6.44% | 26.32% | 19 | yes |
| `union_hot_score_h3` | yahoo | tune | 12 | -1.36% | 36.14% | 83 |  |
| `union_hot_score_h3` | yahoo | forward | 12 | 0.26% | 24.24% | 33 |  |
| `union_ret_5_h1` | yahoo | tune | 4 | 20.11% | 57.50% | 40 |  |
| `union_ret_5_h1` | yahoo | forward | 4 | 21.24% | 40.74% | 27 | yes |
| `union_ret_5_h1` | yahoo | tune | 8 | 0.34% | 44.58% | 83 |  |
| `union_ret_5_h1` | yahoo | forward | 8 | 5.81% | 39.53% | 43 |  |
| `union_ret_5_h1` | yahoo | tune | 12 | -3.21% | 46.67% | 120 |  |
| `union_ret_5_h1` | yahoo | forward | 12 | -1.25% | 34.55% | 55 |  |
| `union_ret_5_h3` | yahoo | tune | 4 | 7.29% | 44.83% | 29 | yes |
| `union_ret_5_h3` | yahoo | forward | 4 | 22.06% | 31.58% | 19 | yes |
| `union_ret_5_h3` | yahoo | tune | 8 | 9.40% | 44.26% | 61 |  |
| `union_ret_5_h3` | yahoo | forward | 8 | 7.33% | 30.43% | 23 | yes |
| `union_ret_5_h3` | yahoo | tune | 12 | 3.70% | 39.53% | 86 |  |
| `union_ret_5_h3` | yahoo | forward | 12 | -0.97% | 23.68% | 38 |  |
| `union_w_hot_candle_h1` | yahoo | tune | 4 | 11.41% | 46.15% | 39 |  |
| `union_w_hot_candle_h1` | yahoo | forward | 4 | 21.41% | 48.15% | 27 | yes |
| `union_w_hot_candle_h1` | yahoo | tune | 8 | -0.51% | 48.15% | 81 |  |
| `union_w_hot_candle_h1` | yahoo | forward | 8 | 8.79% | 41.86% | 43 |  |
| `union_w_hot_candle_h1` | yahoo | tune | 12 | -4.46% | 44.54% | 119 |  |
| `union_w_hot_candle_h1` | yahoo | forward | 12 | 1.72% | 37.50% | 56 |  |
| `union_w_hot_candle_h3` | yahoo | tune | 4 | 3.80% | 36.00% | 25 | yes |
| `union_w_hot_candle_h3` | yahoo | forward | 4 | 18.81% | 35.71% | 14 | yes |
| `union_w_hot_candle_h3` | yahoo | tune | 8 | 2.93% | 41.67% | 60 |  |
| `union_w_hot_candle_h3` | yahoo | forward | 8 | 4.70% | 28.57% | 28 | yes |
| `union_w_hot_candle_h3` | yahoo | tune | 12 | -0.21% | 34.52% | 84 |  |
| `union_w_hot_candle_h3` | yahoo | forward | 12 | -0.29% | 25.81% | 31 |  |
| `union_w_hot_cond_h1` | yahoo | tune | 4 | 6.44% | 45.00% | 40 |  |
| `union_w_hot_cond_h1` | yahoo | forward | 4 | 21.78% | 42.31% | 26 | yes |
| `union_w_hot_cond_h1` | yahoo | tune | 8 | -1.62% | 46.43% | 84 |  |
| `union_w_hot_cond_h1` | yahoo | forward | 8 | 11.75% | 41.86% | 43 |  |
| `union_w_hot_cond_h1` | yahoo | tune | 12 | -3.04% | 47.11% | 121 |  |
| `union_w_hot_cond_h1` | yahoo | forward | 12 | 2.87% | 39.29% | 56 |  |
| `union_w_hot_cond_h3` | yahoo | tune | 4 | 3.32% | 37.50% | 24 | yes |
| `union_w_hot_cond_h3` | yahoo | forward | 4 | 28.39% | 25.00% | 12 | yes |
| `union_w_hot_cond_h3` | yahoo | tune | 8 | 3.73% | 37.70% | 61 |  |
| `union_w_hot_cond_h3` | yahoo | forward | 8 | 12.63% | 27.59% | 29 | yes |
| `union_w_hot_cond_h3` | yahoo | tune | 12 | 0.99% | 34.44% | 90 |  |
| `union_w_hot_cond_h3` | yahoo | forward | 12 | 3.10% | 27.03% | 37 |  |
| `union_hot_n4_holdup` | yahoo | tune | 4 | 35.82% | 64.52% | 31 |  |
| `union_hot_n4_holdup` | yahoo | forward | 4 | 39.32% | 45.83% | 24 | yes |
| `union_hot_n4_holdup` | yahoo | tune | 8 | 10.89% | 50.75% | 67 |  |
| `union_hot_n4_holdup` | yahoo | forward | 8 | 15.64% | 35.14% | 37 |  |
| `union_hot_n4_holdup` | yahoo | tune | 12 | 1.60% | 42.27% | 97 |  |
| `union_hot_n4_holdup` | yahoo | forward | 12 | 10.29% | 33.33% | 42 |  |

## RANDOM4 from the same morning list

| recipe | tape | window | recipe compound | random mean | random median | recipe win | <30 |
| --- | --- | --- | ---: | ---: | ---: | ---: | --- |
| `probable_h3` | clean | tune | -9.61% | -0.04% | -0.96% | 27.42% |  |
| `probable_h3` | clean | forward | -2.96% | -8.01% | -8.64% | 22.73% | yes |
| `probable_h5` | clean | tune | -17.23% | 5.52% | 4.39% | 25.00% |  |
| `probable_h5` | clean | forward | -5.72% | 0.24% | -0.21% | 43.75% | yes |
| `short_extended_h3` | clean | tune | -4.87% | 2.30% | 2.41% | 52.54% |  |
| `short_extended_h3` | clean | forward | -14.67% | 1.20% | 2.16% | 61.90% | yes |
| `union_candle_score_h1` | clean | tune | 3.07% | -6.15% | -6.63% | 41.03% |  |
| `union_candle_score_h1` | clean | forward | -5.78% | -10.81% | -10.72% | 38.10% |  |
| `union_candle_score_h3` | clean | tune | 4.21% | -0.04% | -0.96% | 40.00% |  |
| `union_candle_score_h3` | clean | forward | 8.50% | -8.01% | -8.64% | 44.00% | yes |
| `union_cond_h1` | clean | tune | -4.46% | -6.15% | -6.63% | 39.02% |  |
| `union_cond_h1` | clean | forward | -11.51% | -10.81% | -10.72% | 30.43% |  |
| `union_cond_h3` | clean | tune | -3.07% | -0.04% | -0.96% | 34.48% |  |
| `union_cond_h3` | clean | forward | -9.16% | -8.01% | -8.64% | 20.00% |  |
| `union_cond_n4_h3` | clean | tune | -2.44% | -0.04% | -0.96% | 26.92% | yes |
| `union_cond_n4_h3` | clean | forward | -0.13% | -8.01% | -8.64% | 40.00% | yes |
| `union_hot_n12_h1` | clean | tune | -7.74% | -6.15% | -6.63% | 44.14% |  |
| `union_hot_n12_h1` | clean | forward | 0.85% | -10.81% | -10.72% | 33.33% |  |
| `union_hot_n4_h1` | clean | tune | 13.92% | -6.15% | -6.63% | 52.94% |  |
| `union_hot_n4_h1` | clean | forward | 23.21% | -10.81% | -10.72% | 44.00% | yes |
| `union_hot_score_h1` | clean | tune | -4.16% | -6.15% | -6.63% | 45.21% |  |
| `union_hot_score_h1` | clean | forward | 7.81% | -10.81% | -10.72% | 35.00% |  |
| `union_hot_score_h3` | clean | tune | 4.97% | -0.04% | -0.96% | 36.84% |  |
| `union_hot_score_h3` | clean | forward | 6.69% | -8.01% | -8.64% | 29.03% |  |
| `union_ret_5_h1` | clean | tune | -0.44% | -6.15% | -6.63% | 43.84% |  |
| `union_ret_5_h1` | clean | forward | 5.21% | -10.81% | -10.72% | 37.21% |  |
| `union_ret_5_h3` | clean | tune | 9.68% | -0.04% | -0.96% | 39.34% |  |
| `union_ret_5_h3` | clean | forward | 7.33% | -8.01% | -8.64% | 30.43% | yes |
| `union_w_hot_candle_h1` | clean | tune | 0.22% | -6.15% | -6.63% | 49.33% |  |
| `union_w_hot_candle_h1` | clean | forward | 8.37% | -10.81% | -10.72% | 40.48% |  |
| `union_w_hot_candle_h3` | clean | tune | 2.80% | -0.04% | -0.96% | 40.68% |  |
| `union_w_hot_candle_h3` | clean | forward | 4.69% | -8.01% | -8.64% | 25.93% | yes |
| `union_w_hot_cond_h1` | clean | tune | -3.17% | -6.15% | -6.63% | 46.75% |  |
| `union_w_hot_cond_h1` | clean | forward | 11.10% | -10.81% | -10.72% | 39.53% |  |
| `union_w_hot_cond_h3` | clean | tune | 3.19% | -0.04% | -0.96% | 34.48% |  |
| `union_w_hot_cond_h3` | clean | forward | 12.56% | -8.01% | -8.64% | 32.26% |  |
| `union_hot_n4_holdup` | clean | tune | 33.40% | -2.41% | -3.08% | 60.00% |  |
| `union_hot_n4_holdup` | clean | forward | 37.22% | -4.59% | -5.40% | 45.45% | yes |
| `probable_h3` | yahoo | tune | -5.75% | -0.60% | -1.20% | 32.00% |  |
| `probable_h3` | yahoo | forward | -2.79% | -7.83% | -8.20% | 25.00% | yes |
| `probable_h5` | yahoo | tune | -16.81% | 5.11% | 4.26% | 28.57% |  |
| `probable_h5` | yahoo | forward | -5.02% | 0.33% | -0.14% | 46.67% | yes |
| `short_extended_h3` | yahoo | tune | -6.51% | 2.27% | 2.52% | 49.23% |  |
| `short_extended_h3` | yahoo | forward | -15.74% | 0.89% | 1.81% | 60.00% | yes |
| `union_candle_score_h1` | yahoo | tune | 2.13% | -6.36% | -6.47% | 40.74% |  |
| `union_candle_score_h1` | yahoo | forward | -5.86% | -10.26% | -10.17% | 38.10% |  |
| `union_candle_score_h3` | yahoo | tune | 5.19% | -0.60% | -1.20% | 40.00% |  |
| `union_candle_score_h3` | yahoo | forward | 8.38% | -7.83% | -8.20% | 40.74% | yes |
| `union_cond_h1` | yahoo | tune | -5.15% | -6.36% | -6.47% | 38.55% |  |
| `union_cond_h1` | yahoo | forward | -11.47% | -10.26% | -10.17% | 31.11% |  |
| `union_cond_h3` | yahoo | tune | -3.00% | -0.60% | -1.20% | 33.33% |  |
| `union_cond_h3` | yahoo | forward | -9.23% | -7.83% | -8.20% | 20.69% | yes |
| `union_cond_n4_h3` | yahoo | tune | -2.45% | -0.60% | -1.20% | 25.93% | yes |
| `union_cond_n4_h3` | yahoo | forward | -0.41% | -7.83% | -8.20% | 40.00% | yes |
| `union_hot_n12_h1` | yahoo | tune | -5.85% | -6.36% | -6.47% | 45.00% |  |
| `union_hot_n12_h1` | yahoo | forward | 0.98% | -10.26% | -10.17% | 35.19% |  |
| `union_hot_n4_h1` | yahoo | tune | 15.98% | -6.36% | -6.47% | 50.00% |  |
| `union_hot_n4_h1` | yahoo | forward | 24.09% | -10.26% | -10.17% | 46.15% | yes |
| `union_hot_score_h1` | yahoo | tune | -1.03% | -6.36% | -6.47% | 46.34% |  |
| `union_hot_score_h1` | yahoo | forward | 8.32% | -10.26% | -10.17% | 37.50% |  |
| `union_hot_score_h3` | yahoo | tune | 6.12% | -0.60% | -1.20% | 37.93% |  |
| `union_hot_score_h3` | yahoo | forward | 6.44% | -7.83% | -8.20% | 26.32% | yes |
| `union_ret_5_h1` | yahoo | tune | 0.34% | -6.36% | -6.47% | 44.58% |  |
| `union_ret_5_h1` | yahoo | forward | 5.81% | -10.26% | -10.17% | 39.53% |  |
| `union_ret_5_h3` | yahoo | tune | 9.40% | -0.60% | -1.20% | 44.26% |  |
| `union_ret_5_h3` | yahoo | forward | 7.33% | -7.83% | -8.20% | 30.43% | yes |
| `union_w_hot_candle_h1` | yahoo | tune | -0.51% | -6.36% | -6.47% | 48.15% |  |
| `union_w_hot_candle_h1` | yahoo | forward | 8.79% | -10.26% | -10.17% | 41.86% |  |
| `union_w_hot_candle_h3` | yahoo | tune | 2.93% | -0.60% | -1.20% | 41.67% |  |
| `union_w_hot_candle_h3` | yahoo | forward | 4.70% | -7.83% | -8.20% | 28.57% | yes |
| `union_w_hot_cond_h1` | yahoo | tune | -1.62% | -6.36% | -6.47% | 46.43% |  |
| `union_w_hot_cond_h1` | yahoo | forward | 11.75% | -10.26% | -10.17% | 41.86% |  |
| `union_w_hot_cond_h3` | yahoo | tune | 3.73% | -0.60% | -1.20% | 37.70% |  |
| `union_w_hot_cond_h3` | yahoo | forward | 12.63% | -7.83% | -8.20% | 27.59% | yes |
| `union_hot_n4_holdup` | yahoo | tune | 35.82% | -2.51% | -2.76% | 64.52% |  |
| `union_hot_n4_holdup` | yahoo | forward | 39.32% | -3.99% | -4.75% | 45.83% | yes |

## Winners versus losers at 09:30

Means are entry-morning fields on closed trades. Float is not on the earliest board, so it is omitted. Prior volume is the last bar before the session.

| recipe pool | tape | window | side | n | entry px | ret 5 | cond | hot | days on list | news good | earn | S |
| --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| all subjects | clean | tune | winners | 486 | 29.33 | 36.13 | 3.97 | 7.64 | 1.77 | 3.70% | 7.00% | 2.67 |
| all subjects | clean | tune | losers | 694 | 51.99 | 25.24 | 4.14 | 6.79 | 1.87 | 2.88% | 1.87% | 2.45 |
| all subjects | clean | forward | winners | 216 | 51.57 | 21.63 | 3.93 | 4.63 | 3.27 | 0.00% | 0.00% | 4.63 |
| all subjects | clean | forward | losers | 387 | 62.73 | 14.56 | 4.46 | 3.45 | 3.28 | 2.58% | 1.29% | 4.92 |
| all subjects | yahoo | tune | winners | 520 | 27.63 | 37.44 | 3.85 | 7.82 | 1.90 | 3.46% | 6.54% | 2.63 |
| all subjects | yahoo | tune | losers | 711 | 50.14 | 30.66 | 4.11 | 7.68 | 1.90 | 2.53% | 1.83% | 2.41 |
| all subjects | yahoo | forward | winners | 217 | 51.15 | 22.49 | 3.83 | 4.74 | 3.29 | 0.00% | 0.00% | 4.61 |
| all subjects | yahoo | forward | losers | 373 | 64.32 | 14.11 | 4.54 | 3.43 | 3.25 | 2.68% | 1.34% | 5.05 |

Most common camera tones on those trades:

- clean tune winners: catal:missing 100.00%, buy:neutral 85.39%, digest:good 72.22%, gen:good 69.34%, join:good 67.08%, news:neutral 64.40%
- clean tune losers: catal:missing 99.71%, buy:neutral 85.01%, news:neutral 70.46%, join:good 70.17%, gen:good 65.71%, digest:good 64.55%
- clean forward winners: catal:missing 100.00%, buy:neutral 97.22%, ab:good 80.09%, join:good 78.24%, vol:good 72.69%, peer:good 69.91%
- clean forward losers: catal:missing 100.00%, buy:neutral 93.54%, ab:good 87.86%, peer:good 79.59%, join:good 77.52%, heat:good 72.35%
- yahoo tune winners: catal:missing 100.00%, buy:neutral 86.35%, digest:good 70.38%, gen:good 70.00%, news:neutral 67.31%, join:good 64.42%
- yahoo tune losers: catal:missing 99.72%, buy:neutral 85.94%, news:neutral 71.87%, join:good 70.60%, digest:good 64.14%, gen:good 63.99%
- yahoo forward winners: catal:missing 100.00%, buy:neutral 97.24%, ab:good 80.65%, join:good 75.58%, vol:good 71.43%, heat:good 70.05%
- yahoo forward losers: catal:missing 100.00%, buy:neutral 93.30%, ab:good 88.74%, peer:good 79.62%, join:good 78.28%, heat:good 71.85%

## Combo descriptions

Trees and joint buckets describe trades the books already took. A leaf under 30 trades is flagged and is not listed as a combo. The table order is distance from a 50% win rate. That order is for reading. It selects nothing. Tune leaves are scored again on forward trades. A separate forward tree is a description of that window only.

### clean tune tree depth 2

Leaves 3, flagged under 30: 0.

| conditions | tune n | tune win | tune mean | forward n | forward win | forward mean | forward <30 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| earn == no; vol == neutral | 165 | 20.00% | -4.17% | 208 | 28.37% | -1.21% |  |
| earn != no | 47 | 72.34% | 5.69% | 5 | 0.00% | -14.82% | yes |
| earn == no; vol != neutral | 968 | 43.29% | -0.30% | 390 | 40.26% | 1.56% |  |

Full combination, each condition alone, and RANDOM4 on the leaf's entry dates. A mixed-hold leaf uses the long hold-1 weather-on random books.

| leaf | part | n | win | mean | <30 | random mean | random book |
| --- | --- | ---: | ---: | ---: | --- | ---: | --- |
| earn==no; vol==neutral | full combo | 165 | 20.00% | -4.17% |  | -0.58% | hold-1 mixed |
| earn==no; vol==neutral | earn == no | 1133 | 39.89% | -0.86% |  | -0.58% | hold-1 mixed |
| earn==no; vol==neutral | vol == neutral | 177 | 21.47% | -4.65% |  | -0.58% | hold-1 mixed |
| earn!=no | full combo | 47 | 72.34% | 5.69% |  | -0.69% | hold-1 mixed |
| earn!=no | earn != no | 47 | 72.34% | 5.69% |  | -0.69% | hold-1 mixed |
| earn==no; vol!=neutral | full combo | 968 | 43.29% | -0.30% |  | -0.27% | hold-1 mixed |
| earn==no; vol!=neutral | earn == no | 1133 | 39.89% | -0.86% |  | -0.27% | hold-1 mixed |
| earn==no; vol!=neutral | vol != neutral | 1003 | 44.67% | 0.12% |  | -0.27% | hold-1 mixed |

### clean tune tree depth 3

Leaves 5, flagged under 30: 0.

| conditions | tune n | tune win | tune mean | forward n | forward win | forward mean | forward <30 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| earn == no; vol == neutral; days_on_list == 1 | 103 | 11.65% | -5.03% | 54 | 51.85% | -0.60% |  |
| earn == no; vol != neutral; days_on_list == ge6 | 37 | 27.03% | -2.49% | 61 | 29.51% | -2.24% |  |
| earn != no | 47 | 72.34% | 5.69% | 5 | 0.00% | -14.82% | yes |
| earn == no; vol == neutral; days_on_list != 1 | 62 | 33.87% | -2.73% | 154 | 20.13% | -1.43% |  |
| earn == no; vol != neutral; days_on_list != ge6 | 931 | 43.93% | -0.21% | 329 | 42.25% | 2.27% |  |

Full combination, each condition alone, and RANDOM4 on the leaf's entry dates. A mixed-hold leaf uses the long hold-1 weather-on random books.

| leaf | part | n | win | mean | <30 | random mean | random book |
| --- | --- | ---: | ---: | ---: | --- | ---: | --- |
| earn==no; vol==neutral; days_on_list==1 | full combo | 103 | 11.65% | -5.03% |  | -0.58% | hold-1 mixed |
| earn==no; vol==neutral; days_on_list==1 | earn == no | 1133 | 39.89% | -0.86% |  | -0.58% | hold-1 mixed |
| earn==no; vol==neutral; days_on_list==1 | vol == neutral | 177 | 21.47% | -4.65% |  | -0.58% | hold-1 mixed |
| earn==no; vol==neutral; days_on_list==1 | days_on_list == 1 | 650 | 41.69% | -0.71% |  | -0.58% | hold-1 mixed |
| earn==no; vol!=neutral; days_on_list==ge6 | full combo | 37 | 27.03% | -2.49% |  | -0.56% | hold-1 mixed |
| earn==no; vol!=neutral; days_on_list==ge6 | earn == no | 1133 | 39.89% | -0.86% |  | -0.56% | hold-1 mixed |
| earn==no; vol!=neutral; days_on_list==ge6 | vol != neutral | 1003 | 44.67% | 0.12% |  | -0.56% | hold-1 mixed |
| earn==no; vol!=neutral; days_on_list==ge6 | days_on_list == ge6 | 37 | 27.03% | -2.49% |  | -0.56% | hold-1 mixed |
| earn!=no | full combo | 47 | 72.34% | 5.69% |  | -0.69% | hold-1 mixed |
| earn!=no | earn != no | 47 | 72.34% | 5.69% |  | -0.69% | hold-1 mixed |
| earn==no; vol==neutral; days_on_list!=1 | full combo | 62 | 33.87% | -2.73% |  | -0.34% | hold-1 mixed |
| earn==no; vol==neutral; days_on_list!=1 | earn == no | 1133 | 39.89% | -0.86% |  | -0.34% | hold-1 mixed |
| earn==no; vol==neutral; days_on_list!=1 | vol == neutral | 177 | 21.47% | -4.65% |  | -0.34% | hold-1 mixed |
| earn==no; vol==neutral; days_on_list!=1 | days_on_list != 1 | 530 | 40.57% | -0.46% |  | -0.34% | hold-1 mixed |
| earn==no; vol!=neutral; days_on_list!=ge6 | full combo | 931 | 43.93% | -0.21% |  | -0.27% | hold-1 mixed |
| earn==no; vol!=neutral; days_on_list!=ge6 | earn == no | 1133 | 39.89% | -0.86% |  | -0.27% | hold-1 mixed |
| earn==no; vol!=neutral; days_on_list!=ge6 | vol != neutral | 1003 | 44.67% | 0.12% |  | -0.27% | hold-1 mixed |
| earn==no; vol!=neutral; days_on_list!=ge6 | days_on_list != ge6 | 1143 | 41.64% | -0.54% |  | -0.27% | hold-1 mixed |

### clean forward tree depth 2

Grown on forward trades only. A description of that window. Not a selection.

Leaves 4, flagged under 30: 0.

| conditions | n | win | mean |
| --- | ---: | ---: | ---: |
| vol != good; days_on_list == 2_5 | 157 | 11.46% | -4.34% |
| vol == good; weather != up | 193 | 33.16% | -0.31% |
| vol != good; days_on_list != 2_5 | 76 | 53.95% | 2.81% |
| vol == good; weather == up | 177 | 52.54% | 4.58% |

Full combination, each condition alone, and RANDOM4 on the leaf's entry dates. A mixed-hold leaf uses the long hold-1 weather-on random books.

| leaf | part | n | win | mean | <30 | random mean | random book |
| --- | --- | ---: | ---: | ---: | --- | ---: | --- |
| vol!=good; days_on_list==2_5 | full combo | 157 | 11.46% | -4.34% |  | -0.76% | hold-1 mixed |
| vol!=good; days_on_list==2_5 | vol != good | 233 | 25.32% | -2.01% |  | -0.76% | hold-1 mixed |
| vol!=good; days_on_list==2_5 | days_on_list == 2_5 | 384 | 34.11% | 0.90% |  | -0.76% | hold-1 mixed |
| vol==good; weather!=up | full combo | 193 | 33.16% | -0.31% |  | -0.32% | hold-1 mixed |
| vol==good; weather!=up | vol == good | 370 | 42.43% | 2.03% |  | -0.32% | hold-1 mixed |
| vol==good; weather!=up | weather != up | 350 | 34.00% | -0.22% |  | -0.32% | hold-1 mixed |
| vol!=good; days_on_list!=2_5 | full combo | 76 | 53.95% | 2.81% |  | -0.76% | hold-1 mixed |
| vol!=good; days_on_list!=2_5 | vol != good | 233 | 25.32% | -2.01% |  | -0.76% | hold-1 mixed |
| vol!=good; days_on_list!=2_5 | days_on_list != 2_5 | 219 | 38.81% | -0.28% |  | -0.76% | hold-1 mixed |
| vol==good; weather==up | full combo | 177 | 52.54% | 4.58% |  | -1.50% | hold-1 mixed |
| vol==good; weather==up | vol == good | 370 | 42.43% | 2.03% |  | -1.50% | hold-1 mixed |
| vol==good; weather==up | weather == up | 253 | 38.34% | 1.43% |  | -1.50% | hold-1 mixed |

### clean forward tree depth 3

Grown on forward trades only. A description of that window. Not a selection.

Leaves 8, flagged under 30: 0.

| conditions | n | win | mean |
| --- | ---: | ---: | ---: |
| vol != good; days_on_list == 2_5; ret5_band != neg | 120 | 8.33% | -5.15% |
| vol == good; weather != up; candle == no | 56 | 10.71% | -3.05% |
| vol != good; days_on_list == 2_5; ret5_band == neg | 37 | 21.62% | -1.68% |
| vol != good; days_on_list != 2_5; zero_red != no | 45 | 60.00% | 0.19% |
| vol == good; weather != up; candle != no | 137 | 42.34% | 0.81% |
| vol == good; weather == up; cond_band != m1_3 | 105 | 57.14% | 7.85% |
| vol != good; days_on_list != 2_5; zero_red == no | 31 | 45.16% | 6.61% |
| vol == good; weather == up; cond_band == m1_3 | 72 | 45.83% | -0.19% |

Full combination, each condition alone, and RANDOM4 on the leaf's entry dates. A mixed-hold leaf uses the long hold-1 weather-on random books.

| leaf | part | n | win | mean | <30 | random mean | random book |
| --- | --- | ---: | ---: | ---: | --- | ---: | --- |
| vol!=good; days_on_list==2_5; ret5_band!=neg | full combo | 120 | 8.33% | -5.15% |  | -1.01% | hold-1 mixed |
| vol!=good; days_on_list==2_5; ret5_band!=neg | vol != good | 233 | 25.32% | -2.01% |  | -1.01% | hold-1 mixed |
| vol!=good; days_on_list==2_5; ret5_band!=neg | days_on_list == 2_5 | 384 | 34.11% | 0.90% |  | -1.01% | hold-1 mixed |
| vol!=good; days_on_list==2_5; ret5_band!=neg | ret5_band != neg | 414 | 33.82% | 0.70% |  | -1.01% | hold-1 mixed |
| vol==good; weather!=up; candle==no | full combo | 56 | 10.71% | -3.05% |  | -0.32% | hold-1 mixed |
| vol==good; weather!=up; candle==no | vol == good | 370 | 42.43% | 2.03% |  | -0.32% | hold-1 mixed |
| vol==good; weather!=up; candle==no | weather != up | 350 | 34.00% | -0.22% |  | -0.32% | hold-1 mixed |
| vol==good; weather!=up; candle==no | candle == no | 279 | 33.33% | 2.18% |  | -0.32% | hold-1 mixed |
| vol!=good; days_on_list==2_5; ret5_band==neg | full combo | 37 | 21.62% | -1.68% |  | -0.06% | hold-1 mixed |
| vol!=good; days_on_list==2_5; ret5_band==neg | vol != good | 233 | 25.32% | -2.01% |  | -0.06% | hold-1 mixed |
| vol!=good; days_on_list==2_5; ret5_band==neg | days_on_list == 2_5 | 384 | 34.11% | 0.90% |  | -0.06% | hold-1 mixed |
| vol!=good; days_on_list==2_5; ret5_band==neg | ret5_band == neg | 189 | 40.21% | -0.04% |  | -0.06% | hold-1 mixed |
| vol!=good; days_on_list!=2_5; zero_red!=no | full combo | 45 | 60.00% | 0.19% |  | -1.33% | hold-1 mixed |
| vol!=good; days_on_list!=2_5; zero_red!=no | vol != good | 233 | 25.32% | -2.01% |  | -1.33% | hold-1 mixed |
| vol!=good; days_on_list!=2_5; zero_red!=no | days_on_list != 2_5 | 219 | 38.81% | -0.28% |  | -1.33% | hold-1 mixed |
| vol!=good; days_on_list!=2_5; zero_red!=no | zero_red != no | 247 | 31.58% | -2.36% |  | -1.33% | hold-1 mixed |
| vol==good; weather!=up; candle!=no | full combo | 137 | 42.34% | 0.81% |  | -0.14% | hold-1 mixed |
| vol==good; weather!=up; candle!=no | vol == good | 370 | 42.43% | 2.03% |  | -0.14% | hold-1 mixed |
| vol==good; weather!=up; candle!=no | weather != up | 350 | 34.00% | -0.22% |  | -0.14% | hold-1 mixed |
| vol==good; weather!=up; candle!=no | candle != no | 324 | 37.96% | -1.00% |  | -0.14% | hold-1 mixed |
| vol==good; weather==up; cond_band!=m1_3 | full combo | 105 | 57.14% | 7.85% |  | -1.50% | hold-1 mixed |
| vol==good; weather==up; cond_band!=m1_3 | vol == good | 370 | 42.43% | 2.03% |  | -1.50% | hold-1 mixed |
| vol==good; weather==up; cond_band!=m1_3 | weather == up | 253 | 38.34% | 1.43% |  | -1.50% | hold-1 mixed |
| vol==good; weather==up; cond_band!=m1_3 | cond_band != m1_3 | 465 | 35.91% | 0.98% |  | -1.50% | hold-1 mixed |
| vol!=good; days_on_list!=2_5; zero_red==no | full combo | 31 | 45.16% | 6.61% |  | -0.00% | hold-1 mixed |
| vol!=good; days_on_list!=2_5; zero_red==no | vol != good | 233 | 25.32% | -2.01% |  | -0.00% | hold-1 mixed |
| vol!=good; days_on_list!=2_5; zero_red==no | days_on_list != 2_5 | 219 | 38.81% | -0.28% |  | -0.00% | hold-1 mixed |
| vol!=good; days_on_list!=2_5; zero_red==no | zero_red == no | 356 | 38.76% | 2.43% |  | -0.00% | hold-1 mixed |
| vol==good; weather==up; cond_band==m1_3 | full combo | 72 | 45.83% | -0.19% |  | -1.35% | hold-1 mixed |
| vol==good; weather==up; cond_band==m1_3 | vol == good | 370 | 42.43% | 2.03% |  | -1.35% | hold-1 mixed |
| vol==good; weather==up; cond_band==m1_3 | weather == up | 253 | 38.34% | 1.43% |  | -1.35% | hold-1 mixed |
| vol==good; weather==up; cond_band==m1_3 | cond_band == m1_3 | 138 | 35.51% | -1.24% |  | -1.35% | hold-1 mixed |

### clean tune joint price_weather_news

Cells under 30, counted and not listed: 7.

| bucket | n | win | mean |
| --- | ---: | ---: | ---: |
| lt3 · down · missing | 36 | 19.44% | -6.76% |
| ge10 · down · missing | 92 | 31.52% | -0.84% |
| ge10 · strong · missing | 167 | 32.93% | -1.70% |
| ge10 · up · missing | 371 | 42.59% | -0.81% |
| lt3 · up · missing | 108 | 42.59% | 3.57% |
| m3_10 · up · missing | 215 | 45.12% | -0.35% |
| lt3 · strong · missing | 36 | 47.22% | 3.70% |
| ge10 · missing · missing | 61 | 52.46% | -1.23% |

### clean tune joint price_cond_ret5

Cells under 30, counted and not listed: 16.

| bucket | n | win | mean |
| --- | ---: | ---: | ---: |
| lt3 · m1_3 · hot | 60 | 26.67% | -7.40% |
| ge10 · m1_3 · mid | 36 | 33.33% | -5.06% |
| ge10 · ge4 · hot | 286 | 34.62% | -1.22% |
| m3_10 · m1_3 · hot | 48 | 64.58% | 1.45% |
| ge10 · ge4 · neg | 48 | 35.42% | 0.47% |
| lt3 · ge4 · hot | 98 | 59.18% | 9.40% |
| ge10 · ge4 · mid | 167 | 43.11% | -1.24% |
| m3_10 · ge4 · hot | 125 | 43.20% | -0.38% |
| ge10 · m1_3 · hot | 127 | 44.09% | -0.95% |

### clean forward joint price_weather_news

Cells under 30, counted and not listed: 4.

| bucket | n | win | mean |
| --- | ---: | ---: | ---: |
| m3_10 · strong · missing | 35 | 25.71% | -5.14% |
| ge10 · strong · missing | 209 | 34.45% | 0.81% |
| ge10 · up · missing | 131 | 35.88% | -2.07% |
| lt3 · up · missing | 53 | 37.74% | 14.14% |
| m3_10 · down · missing | 45 | 37.78% | 2.87% |
| ge10 · down · missing | 46 | 43.48% | -0.55% |
| m3_10 · up · missing | 52 | 46.15% | -0.24% |

### clean forward joint price_cond_ret5

Cells under 30, counted and not listed: 19.

| bucket | n | win | mean |
| --- | ---: | ---: | ---: |
| ge10 · ge4 · mid | 73 | 24.66% | -2.66% |
| ge10 · ge4 · neg | 163 | 39.26% | -0.75% |
| ge10 · ge4 · hot | 109 | 40.37% | 1.56% |
| lt3 · m1_3 · hot | 30 | 43.33% | -0.06% |
| m3_10 · m1_3 · hot | 51 | 45.10% | -0.51% |

### yahoo tune tree depth 2

Leaves 3, flagged under 30: 0.

| conditions | tune n | tune win | tune mean | forward n | forward win | forward mean | forward <30 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| earn == no; vol == neutral | 168 | 20.83% | -3.88% | 196 | 28.06% | -1.37% |  |
| earn != no | 47 | 72.34% | 5.69% | 5 | 0.00% | -14.81% | yes |
| earn == no; vol != neutral | 1016 | 44.39% | 0.03% | 389 | 41.65% | 1.78% |  |

Full combination, each condition alone, and RANDOM4 on the leaf's entry dates. A mixed-hold leaf uses the long hold-1 weather-on random books.

| leaf | part | n | win | mean | <30 | random mean | random book |
| --- | --- | ---: | ---: | ---: | --- | ---: | --- |
| earn==no; vol==neutral | full combo | 168 | 20.83% | -3.88% |  | -0.63% | hold-1 mixed |
| earn==no; vol==neutral | earn == no | 1184 | 41.05% | -0.52% |  | -0.63% | hold-1 mixed |
| earn==no; vol==neutral | vol == neutral | 180 | 22.22% | -4.36% |  | -0.63% | hold-1 mixed |
| earn!=no | full combo | 47 | 72.34% | 5.69% |  | -0.65% | hold-1 mixed |
| earn!=no | earn != no | 47 | 72.34% | 5.69% |  | -0.65% | hold-1 mixed |
| earn==no; vol!=neutral | full combo | 1016 | 44.39% | 0.03% |  | -0.31% | hold-1 mixed |
| earn==no; vol!=neutral | earn == no | 1184 | 41.05% | -0.52% |  | -0.31% | hold-1 mixed |
| earn==no; vol!=neutral | vol != neutral | 1051 | 45.67% | 0.41% |  | -0.31% | hold-1 mixed |

### yahoo tune tree depth 3

Leaves 5, flagged under 30: 0.

| conditions | tune n | tune win | tune mean | forward n | forward win | forward mean | forward <30 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| earn == no; vol == neutral; days_on_list == 1 | 106 | 11.32% | -4.96% | 54 | 48.15% | -0.85% |  |
| earn != no | 47 | 72.34% | 5.69% | 5 | 0.00% | -14.81% | yes |
| earn == no; vol != neutral; weather == strong | 171 | 29.82% | -2.21% | 158 | 29.75% | -0.36% |  |
| earn == no; vol == neutral; days_on_list != 1 | 62 | 37.10% | -2.01% | 142 | 20.42% | -1.57% |  |
| earn == no; vol != neutral; weather != strong | 845 | 47.34% | 0.49% | 231 | 49.78% | 3.24% |  |

Full combination, each condition alone, and RANDOM4 on the leaf's entry dates. A mixed-hold leaf uses the long hold-1 weather-on random books.

| leaf | part | n | win | mean | <30 | random mean | random book |
| --- | --- | ---: | ---: | ---: | --- | ---: | --- |
| earn==no; vol==neutral; days_on_list==1 | full combo | 106 | 11.32% | -4.96% |  | -0.56% | hold-1 mixed |
| earn==no; vol==neutral; days_on_list==1 | earn == no | 1184 | 41.05% | -0.52% |  | -0.56% | hold-1 mixed |
| earn==no; vol==neutral; days_on_list==1 | vol == neutral | 180 | 22.22% | -4.36% |  | -0.56% | hold-1 mixed |
| earn==no; vol==neutral; days_on_list==1 | days_on_list == 1 | 657 | 41.10% | -0.73% |  | -0.56% | hold-1 mixed |
| earn!=no | full combo | 47 | 72.34% | 5.69% |  | -0.65% | hold-1 mixed |
| earn!=no | earn != no | 47 | 72.34% | 5.69% |  | -0.65% | hold-1 mixed |
| earn==no; vol!=neutral; weather==strong | full combo | 171 | 29.82% | -2.21% |  | 0.48% | hold-1 mixed |
| earn==no; vol!=neutral; weather==strong | earn == no | 1184 | 41.05% | -0.52% |  | 0.48% | hold-1 mixed |
| earn==no; vol!=neutral; weather==strong | vol != neutral | 1051 | 45.67% | 0.41% |  | 0.48% | hold-1 mixed |
| earn==no; vol!=neutral; weather==strong | weather == strong | 223 | 35.87% | -0.87% |  | 0.48% | hold-1 mixed |
| earn==no; vol==neutral; days_on_list!=1 | full combo | 62 | 37.10% | -2.01% |  | -0.42% | hold-1 mixed |
| earn==no; vol==neutral; days_on_list!=1 | earn == no | 1184 | 41.05% | -0.52% |  | -0.42% | hold-1 mixed |
| earn==no; vol==neutral; days_on_list!=1 | vol == neutral | 180 | 22.22% | -4.36% |  | -0.42% | hold-1 mixed |
| earn==no; vol==neutral; days_on_list!=1 | days_on_list != 1 | 574 | 43.55% | 0.23% |  | -0.42% | hold-1 mixed |
| earn==no; vol!=neutral; weather!=strong | full combo | 845 | 47.34% | 0.49% |  | -0.49% | hold-1 mixed |
| earn==no; vol!=neutral; weather!=strong | earn == no | 1184 | 41.05% | -0.52% |  | -0.49% | hold-1 mixed |
| earn==no; vol!=neutral; weather!=strong | vol != neutral | 1051 | 45.67% | 0.41% |  | -0.49% | hold-1 mixed |
| earn==no; vol!=neutral; weather!=strong | weather != strong | 1008 | 43.65% | -0.15% |  | -0.49% | hold-1 mixed |

### yahoo forward tree depth 2

Grown on forward trades only. A description of that window. Not a selection.

Leaves 4, flagged under 30: 0.

| conditions | n | win | mean |
| --- | ---: | ---: | ---: |
| vol != good; days_on_list == 2_5 | 152 | 15.79% | -3.92% |
| vol == good; days_on_list != 2_5 | 139 | 30.94% | -1.85% |
| vol != good; days_on_list != 2_5 | 75 | 50.67% | 2.24% |
| vol == good; days_on_list == 2_5 | 224 | 50.00% | 4.62% |

Full combination, each condition alone, and RANDOM4 on the leaf's entry dates. A mixed-hold leaf uses the long hold-1 weather-on random books.

| leaf | part | n | win | mean | <30 | random mean | random book |
| --- | --- | ---: | ---: | ---: | --- | ---: | --- |
| vol!=good; days_on_list==2_5 | full combo | 152 | 15.79% | -3.92% |  | -0.77% | hold-1 mixed |
| vol!=good; days_on_list==2_5 | vol != good | 227 | 27.31% | -1.88% |  | -0.77% | hold-1 mixed |
| vol!=good; days_on_list==2_5 | days_on_list == 2_5 | 376 | 36.17% | 1.17% |  | -0.77% | hold-1 mixed |
| vol==good; days_on_list!=2_5 | full combo | 139 | 30.94% | -1.85% |  | -1.09% | hold-1 mixed |
| vol==good; days_on_list!=2_5 | vol == good | 363 | 42.70% | 2.14% |  | -1.09% | hold-1 mixed |
| vol==good; days_on_list!=2_5 | days_on_list != 2_5 | 214 | 37.85% | -0.41% |  | -1.09% | hold-1 mixed |
| vol!=good; days_on_list!=2_5 | full combo | 75 | 50.67% | 2.24% |  | -0.70% | hold-1 mixed |
| vol!=good; days_on_list!=2_5 | vol != good | 227 | 27.31% | -1.88% |  | -0.70% | hold-1 mixed |
| vol!=good; days_on_list!=2_5 | days_on_list != 2_5 | 214 | 37.85% | -0.41% |  | -0.70% | hold-1 mixed |
| vol==good; days_on_list==2_5 | full combo | 224 | 50.00% | 4.62% |  | -0.83% | hold-1 mixed |
| vol==good; days_on_list==2_5 | vol == good | 363 | 42.70% | 2.14% |  | -0.83% | hold-1 mixed |
| vol==good; days_on_list==2_5 | days_on_list == 2_5 | 376 | 36.17% | 1.17% |  | -0.83% | hold-1 mixed |

### yahoo forward tree depth 3

Grown on forward trades only. A description of that window. Not a selection.

Leaves 8, flagged under 30: 0.

| conditions | n | win | mean |
| --- | ---: | ---: | ---: |
| vol != good; days_on_list == 2_5; zero_red != no | 74 | 5.41% | -3.82% |
| vol == good; days_on_list != 2_5; weather != up | 71 | 14.08% | -3.77% |
| vol != good; days_on_list == 2_5; zero_red == no | 78 | 25.64% | -4.01% |
| vol != good; days_on_list != 2_5; zero_red == no | 30 | 40.00% | 5.74% |
| vol == good; days_on_list == 2_5; price_band != ge10 | 101 | 41.58% | 6.86% |
| vol != good; days_on_list != 2_5; zero_red != no | 45 | 57.78% | -0.09% |
| vol == good; days_on_list == 2_5; price_band == ge10 | 123 | 56.91% | 2.77% |
| vol == good; days_on_list != 2_5; weather == up | 68 | 48.53% | 0.16% |

Full combination, each condition alone, and RANDOM4 on the leaf's entry dates. A mixed-hold leaf uses the long hold-1 weather-on random books.

| leaf | part | n | win | mean | <30 | random mean | random book |
| --- | --- | ---: | ---: | ---: | --- | ---: | --- |
| vol!=good; days_on_list==2_5; zero_red!=no | full combo | 74 | 5.41% | -3.82% |  | -1.14% | hold-1 mixed |
| vol!=good; days_on_list==2_5; zero_red!=no | vol != good | 227 | 27.31% | -1.88% |  | -1.14% | hold-1 mixed |
| vol!=good; days_on_list==2_5; zero_red!=no | days_on_list == 2_5 | 376 | 36.17% | 1.17% |  | -1.14% | hold-1 mixed |
| vol!=good; days_on_list==2_5; zero_red!=no | zero_red != no | 242 | 30.99% | -2.42% |  | -1.14% | hold-1 mixed |
| vol==good; days_on_list!=2_5; weather!=up | full combo | 71 | 14.08% | -3.77% |  | -0.35% | hold-1 mixed |
| vol==good; days_on_list!=2_5; weather!=up | vol == good | 363 | 42.70% | 2.14% |  | -0.35% | hold-1 mixed |
| vol==good; days_on_list!=2_5; weather!=up | days_on_list != 2_5 | 214 | 37.85% | -0.41% |  | -0.35% | hold-1 mixed |
| vol==good; days_on_list!=2_5; weather!=up | weather != up | 334 | 34.43% | -0.16% |  | -0.35% | hold-1 mixed |
| vol!=good; days_on_list==2_5; zero_red==no | full combo | 78 | 25.64% | -4.01% |  | -0.73% | hold-1 mixed |
| vol!=good; days_on_list==2_5; zero_red==no | vol != good | 227 | 27.31% | -1.88% |  | -0.73% | hold-1 mixed |
| vol!=good; days_on_list==2_5; zero_red==no | days_on_list == 2_5 | 376 | 36.17% | 1.17% |  | -0.73% | hold-1 mixed |
| vol!=good; days_on_list==2_5; zero_red==no | zero_red == no | 348 | 40.80% | 2.69% |  | -0.73% | hold-1 mixed |
| vol!=good; days_on_list!=2_5; zero_red==no | full combo | 30 | 40.00% | 5.74% |  | 0.05% | hold-1 mixed |
| vol!=good; days_on_list!=2_5; zero_red==no | vol != good | 227 | 27.31% | -1.88% |  | 0.05% | hold-1 mixed |
| vol!=good; days_on_list!=2_5; zero_red==no | days_on_list != 2_5 | 214 | 37.85% | -0.41% |  | 0.05% | hold-1 mixed |
| vol!=good; days_on_list!=2_5; zero_red==no | zero_red == no | 348 | 40.80% | 2.69% |  | 0.05% | hold-1 mixed |
| vol==good; days_on_list==2_5; price_band!=ge10 | full combo | 101 | 41.58% | 6.86% |  | -0.78% | hold-1 mixed |
| vol==good; days_on_list==2_5; price_band!=ge10 | vol == good | 363 | 42.70% | 2.14% |  | -0.78% | hold-1 mixed |
| vol==good; days_on_list==2_5; price_band!=ge10 | days_on_list == 2_5 | 376 | 36.17% | 1.17% |  | -0.78% | hold-1 mixed |
| vol==good; days_on_list==2_5; price_band!=ge10 | price_band != ge10 | 195 | 35.38% | 2.63% |  | -0.78% | hold-1 mixed |
| vol!=good; days_on_list!=2_5; zero_red!=no | full combo | 45 | 57.78% | -0.09% |  | -1.28% | hold-1 mixed |
| vol!=good; days_on_list!=2_5; zero_red!=no | vol != good | 227 | 27.31% | -1.88% |  | -1.28% | hold-1 mixed |
| vol!=good; days_on_list!=2_5; zero_red!=no | days_on_list != 2_5 | 214 | 37.85% | -0.41% |  | -1.28% | hold-1 mixed |
| vol!=good; days_on_list!=2_5; zero_red!=no | zero_red != no | 242 | 30.99% | -2.42% |  | -1.28% | hold-1 mixed |
| vol==good; days_on_list==2_5; price_band==ge10 | full combo | 123 | 56.91% | 2.77% |  | -0.83% | hold-1 mixed |
| vol==good; days_on_list==2_5; price_band==ge10 | vol == good | 363 | 42.70% | 2.14% |  | -0.83% | hold-1 mixed |
| vol==good; days_on_list==2_5; price_band==ge10 | days_on_list == 2_5 | 376 | 36.17% | 1.17% |  | -0.83% | hold-1 mixed |
| vol==good; days_on_list==2_5; price_band==ge10 | price_band == ge10 | 395 | 37.47% | -0.41% |  | -0.83% | hold-1 mixed |
| vol==good; days_on_list!=2_5; weather==up | full combo | 68 | 48.53% | 0.16% |  | -1.82% | hold-1 mixed |
| vol==good; days_on_list!=2_5; weather==up | vol == good | 363 | 42.70% | 2.14% |  | -1.82% | hold-1 mixed |
| vol==good; days_on_list!=2_5; weather==up | days_on_list != 2_5 | 214 | 37.85% | -0.41% |  | -1.82% | hold-1 mixed |
| vol==good; days_on_list!=2_5; weather==up | weather == up | 256 | 39.84% | 1.57% |  | -1.82% | hold-1 mixed |

### yahoo tune joint price_weather_news

Cells under 30, counted and not listed: 7.

| bucket | n | win | mean |
| --- | ---: | ---: | ---: |
| lt3 · down · missing | 36 | 19.44% | -6.76% |
| ge10 · down · missing | 92 | 31.52% | -0.84% |
| ge10 · strong · missing | 168 | 32.74% | -1.71% |
| ge10 · missing · missing | 45 | 55.56% | -0.88% |
| ge10 · up · missing | 364 | 45.05% | -0.55% |
| m3_10 · up · missing | 273 | 45.79% | 0.37% |
| lt3 · up · missing | 122 | 45.90% | 3.61% |
| lt3 · strong · missing | 35 | 48.57% | 4.21% |

### yahoo tune joint price_cond_ret5

Cells under 30, counted and not listed: 15.

| bucket | n | win | mean |
| --- | ---: | ---: | ---: |
| lt3 · m1_3 · hot | 62 | 29.03% | -7.02% |
| ge10 · ge4 · hot | 283 | 34.98% | -1.24% |
| ge10 · ge4 · neg | 48 | 35.42% | 0.47% |
| ge10 · m1_3 · mid | 33 | 36.36% | -5.15% |
| ge10 · le0 · hot | 33 | 60.61% | 0.69% |
| lt3 · ge4 · hot | 109 | 57.80% | 8.86% |
| m3_10 · ge4 · hot | 149 | 42.28% | 0.06% |
| m3_10 · m1_3 · hot | 73 | 57.53% | 0.63% |
| ge10 · ge4 · mid | 153 | 44.44% | -0.79% |
| ge10 · m1_3 · hot | 126 | 44.44% | -0.90% |

### yahoo forward joint price_weather_news

Cells under 30, counted and not listed: 4.

| bucket | n | win | mean |
| --- | ---: | ---: | ---: |
| m3_10 · strong · missing | 35 | 25.71% | -5.14% |
| ge10 · strong · missing | 206 | 34.47% | 0.83% |
| m3_10 · down · missing | 41 | 36.59% | 3.10% |
| lt3 · up · missing | 53 | 37.74% | 13.89% |
| ge10 · up · missing | 134 | 38.81% | -1.62% |
| m3_10 · up · missing | 52 | 46.15% | -0.26% |
| ge10 · down · missing | 38 | 50.00% | -0.32% |

### yahoo forward joint price_cond_ret5

Cells under 30, counted and not listed: 19.

| bucket | n | win | mean |
| --- | ---: | ---: | ---: |
| ge10 · ge4 · mid | 72 | 23.61% | -2.75% |
| ge10 · ge4 · neg | 158 | 39.24% | -0.76% |
| ge10 · ge4 · hot | 110 | 40.00% | 1.51% |
| m3_10 · m1_3 · hot | 51 | 43.14% | -0.52% |
| lt3 · m1_3 · hot | 30 | 43.33% | -0.06% |

No leaf and no bucket is selected for trading.

