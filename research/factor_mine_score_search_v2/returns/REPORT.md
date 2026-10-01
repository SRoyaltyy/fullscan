# factor_mine_score_search_v2

Diagnosis of a frozen ranking. The order was fixed through 2026-09-11. This file does not sort again.
Preregistration fingerprint `eeca5d61cf6f2f6d6723b4e084fe1b560e5b7ff455e8e5f6c68059e91ce6cb55`.
Every session is `designed_after`. The study was created 2026-09-26.
Luck N = 68. That is v1's 34 formulas plus these 34. They are not added to 9,280.

Passers (every required cell has at least 30 closed trades): 34.
Best: `only_earn`.
Top 10: `only_earn`, `only_yday`, `cam_rsi`, `cam_yday`, `w2_yday`, `flow_earn`, `only_macd`, `rsi_mom`, `yday_macd`, `drop_flow`.
Top 20: `only_earn`, `only_yday`, `cam_rsi`, `cam_yday`, `w2_yday`, `flow_earn`, `only_macd`, `rsi_mom`, `yday_macd`, `drop_flow`, `cam_macd`, `cap_yday_10_macd_0_5`, `cap_yday_20`, `all_equal`, `cap_macd_2`, `cap_yday_10`, `w2_rsi`, `cam_flow`, `cap_macd_0_5`, `drop_earn`.

The rank key is the worse of trade win rate and winning-day share, then the worst of those values across the required fresh starts. A flagged formula stays in the top 10 or top 20 when fewer than that many formulas clear every required cell.

| rank | formula | passer | rank key | worst start | worst X |
| ---: | --- | --- | ---: | --- | ---: |
| 1 | `only_earn` | yes | 42.19% | 2026-08-20 | 8 |
| 2 | `only_yday` | yes | 40.00% | 2026-08-26 | 8 |
| 3 | `cam_rsi` | yes | 37.50% | 2026-08-20 | 4 |
| 4 | `cam_yday` | yes | 37.50% | 2026-08-26 | 8 |
| 5 | `w2_yday` | yes | 37.50% | 2026-08-20 | 4 |
| 6 | `flow_earn` | yes | 33.33% | 2026-08-25 | 8 |
| 7 | `only_macd` | yes | 25.00% | 2026-08-27 | 8 |
| 8 | `rsi_mom` | yes | 25.00% | 2026-08-20 | 4 |
| 9 | `yday_macd` | yes | 20.00% | 2026-08-26 | 8 |
| 10 | `drop_flow` | yes | 20.00% | 2026-08-26 | 8 |
| 11 | `cam_macd` | yes | 20.00% | 2026-08-26 | 8 |
| 12 | `cap_yday_10_macd_0_5` | yes | 20.00% | 2026-08-26 | 8 |
| 13 | `cap_yday_20` | yes | 20.00% | 2026-08-26 | 8 |
| 14 | `all_equal` | yes | 20.00% | 2026-08-26 | 8 |
| 15 | `cap_macd_2` | yes | 20.00% | 2026-08-26 | 8 |
| 16 | `cap_yday_10` | yes | 20.00% | 2026-08-26 | 8 |
| 17 | `w2_rsi` | yes | 20.00% | 2026-08-26 | 8 |
| 18 | `cam_flow` | yes | 20.00% | 2026-08-26 | 8 |
| 19 | `cap_macd_0_5` | yes | 20.00% | 2026-08-26 | 8 |
| 20 | `drop_earn` | yes | 20.00% | 2026-08-26 | 8 |

The tables below are the continuous $10,000 book from 2026-08-13. It does not reset on 2026-09-14. W/T is up days divided by days with a buy or a sell. Flat 15bp uses the same share counts.
A `too few` count above 0 is how many formulas in that set have fewer than 30 closed trades. Those formulas stay.

## Continuous book through 2026-09-11

| set | version | X | compound | flat 15bp | win | W/T | up | down | flat | days entered | entries | closed | ex-best | positive | too few |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| best | clean | 2 | -5.84% | -0.50% | 54.55% | 63.64% | 7.00 | 4.00 | 10.00 | 11.00 | 22.00 | 22.00 | -9.50% | 0.00% | 1 |
| best | clean | 4 | 0.17% | 5.18% | 52.27% | 54.55% | 6.00 | 5.00 | 10.00 | 11.00 | 44.00 | 44.00 | -3.10% | 100.00% | 0 |
| best | clean | 8 | -4.67% | 0.78% | 44.32% | 54.55% | 6.00 | 5.00 | 10.00 | 11.00 | 88.00 | 88.00 | -6.20% | 0.00% | 0 |
| top10 | clean | 2 | -5.29% | -1.50% | 48.64% | 49.09% | 5.40 | 5.60 | 10.00 | 11.00 | 22.00 | 22.00 | -10.37% | 30.00% | 10 |
| top10 | clean | 4 | -4.76% | -0.61% | 48.18% | 50.00% | 5.50 | 5.50 | 10.00 | 11.00 | 44.00 | 44.00 | -7.91% | 30.00% | 0 |
| top10 | clean | 8 | -5.05% | 0.43% | 47.15% | 47.27% | 5.20 | 5.80 | 10.00 | 11.00 | 87.40 | 87.40 | -6.88% | 10.00% | 0 |
| top20 | clean | 2 | -9.02% | -6.19% | 43.64% | 43.18% | 4.75 | 6.25 | 10.00 | 11.00 | 22.00 | 22.00 | -13.40% | 20.00% | 20 |
| top20 | clean | 4 | -5.48% | -1.79% | 47.05% | 46.82% | 5.15 | 5.85 | 10.00 | 11.00 | 44.00 | 44.00 | -8.58% | 15.00% | 0 |
| top20 | clean | 8 | -5.48% | -0.35% | 45.91% | 46.82% | 5.15 | 5.85 | 10.00 | 11.00 | 87.25 | 87.25 | -7.79% | 5.00% | 0 |
| IWM | clean |  | -4.87% |  |  |  | 9 | 12 | 0 |  |  |  |  |  |  |
| RANDOM4 | clean | 4 | -3.74% |  |  |  |  |  |  |  |  |  |  |  |  |

RANDOM4 on clean, median compound -4.55%, n=1000.
On clean, the 77 dropped names are 0.00% of summed ticker dollars (0.00 / -39970.12), across the frozen top 20 at X=2, 4, and 8.

yahoo halted. 10 unexplained jumps on kept names. No compound is written. First jumps: ABVX 2025-07-23 open_over_prev_close, BNC 2025-07-28 open_over_prev_close, BYND 2026-08-17 open_over_prev_close, CELC 2025-07-28 open_over_prev_close, CYPH 2025-11-12 close_over_open, FJET 2025-12-22 close_over_open, ORBS 2025-09-08 open_over_prev_close, OTLK 2025-08-28 open_over_prev_close, REAX 2026-08-20 open_over_prev_close, RZLT 2025-12-11 open_over_prev_close.

## 2026-09-14 through 2026-09-25

| set | version | X | compound | flat 15bp | win | W/T | up | down | flat | days entered | entries | closed | ex-best | positive | too few |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| best | clean | 2 | -11.54% | -8.48% | 7.14% | 14.29% | 1.00 | 6.00 | 3.00 | 7.00 | 14.00 | 14.00 | -12.25% | 0.00% | 1 |
| best | clean | 4 | -9.35% | -6.40% | 14.29% | 28.57% | 2.00 | 5.00 | 3.00 | 7.00 | 28.00 | 28.00 | -10.35% | 0.00% | 1 |
| best | clean | 8 | -8.02% | -5.01% | 28.26% | 14.29% | 1.00 | 6.00 | 3.00 | 7.00 | 46.00 | 46.00 | -8.76% | 0.00% | 0 |
| top10 | clean | 2 | -12.63% | -11.17% | 15.71% | 20.00% | 1.40 | 5.60 | 3.00 | 7.00 | 14.00 | 14.00 | -14.30% | 20.00% | 10 |
| top10 | clean | 4 | -10.50% | -8.66% | 18.93% | 17.14% | 1.20 | 5.80 | 3.00 | 7.00 | 28.00 | 28.00 | -11.67% | 0.00% | 10 |
| top10 | clean | 8 | -10.13% | -7.81% | 26.38% | 8.57% | 0.60 | 6.40 | 3.00 | 7.00 | 44.50 | 44.50 | -10.99% | 0.00% | 0 |
| top20 | clean | 2 | -8.90% | -7.69% | 19.64% | 24.29% | 1.70 | 5.30 | 3.00 | 7.00 | 14.00 | 14.00 | -10.97% | 20.00% | 20 |
| top20 | clean | 4 | -9.24% | -7.49% | 20.36% | 20.00% | 1.40 | 5.60 | 3.00 | 7.00 | 28.00 | 28.00 | -10.33% | 0.00% | 20 |
| top20 | clean | 8 | -9.57% | -7.38% | 26.49% | 10.00% | 0.70 | 6.30 | 3.00 | 7.00 | 44.05 | 44.05 | -10.32% | 0.00% | 0 |
| IWM | clean |  | -2.33% |  |  |  | 4 | 6 | 0 |  |  |  |  |  |  |
| RANDOM4 | clean | 4 | -9.91% |  |  |  |  |  |  |  |  |  |  |  |  |

RANDOM4 on clean, median compound -10.05%, n=1000.
On clean, the 77 dropped names are 0.00% of summed ticker dollars (0.00 / -50897.94), across the frozen top 20 at X=2, 4, and 8.

yahoo halted. 10 unexplained jumps on kept names. No compound is written. First jumps: ABVX 2025-07-23 open_over_prev_close, BNC 2025-07-28 open_over_prev_close, BYND 2026-08-17 open_over_prev_close, CELC 2025-07-28 open_over_prev_close, CYPH 2025-11-12 close_over_open, FJET 2025-12-22 close_over_open, ORBS 2025-09-08 open_over_prev_close, OTLK 2025-08-28 open_over_prev_close, REAX 2026-08-20 open_over_prev_close, RZLT 2025-12-11 open_over_prev_close.

No formula is added after this freeze. v1's preregistration and pick rule are not edited.

