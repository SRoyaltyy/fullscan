# factor_mine_score_search_v1

Preregistration fingerprint `c03d665afb3873767263ef6e33bb95106ffb3f7fbe8b2adf7b06e80747fc0d31`.
Every session is `designed_after`. The study was created 2026-09-26.
Luck N = 34. The freeze order was committed before this file.

Best: `flow_earn`.
Top 10: `flow_earn`, `only_macd`, `only_flow`, `drop_yday`, `cam_macd`, `board_list`, `drop_flow`, `cap_yday_10`, `only_earn`, `w2_macd`.
Top 20: `flow_earn`, `only_macd`, `only_flow`, `drop_yday`, `cam_macd`, `board_list`, `drop_flow`, `cap_yday_10`, `only_earn`, `w2_macd`, `yday_macd`, `drop_cam`, `rsi_macd`, `cap_yday_20`, `cap_yday_10_macd_0_5`, `w2_yday`, `cam_earn`, `drop_rsi`, `all_equal`, `only_yday`.

Clean bars are the #362 parquet. Yahoo keeps the 77 names that parquet omits.
A red morning (S missing or S <= -3) does not buy. Exit is the session close.

## Through 2026-09-11

| set | version | X | compound | flat 15bp | up | down | flat | days entered | entries | closed | win | ex-best | positive | too few |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| best | clean | 2 | 16.53% | 19.24% | 8.00 | 3.00 | 10.00 | 11.00 | 22.00 | 22.00 | 50.00% | 4.17% | 100.00% | 1 |
| best | clean | 4 | 2.49% | 6.77% | 6.00 | 5.00 | 10.00 | 11.00 | 44.00 | 44.00 | 50.00% | -3.16% | 100.00% | 0 |
| best | clean | 8 | -5.34% | 0.10% | 6.00 | 5.00 | 10.00 | 11.00 | 88.00 | 88.00 | 43.18% | -8.07% | 0.00% | 0 |
| top10 | clean | 2 | 3.31% | 5.69% | 5.70 | 5.30 | 10.00 | 11.00 | 22.00 | 22.00 | 47.27% | -2.42% | 60.00% | 10 |
| top10 | clean | 4 | -1.21% | 2.10% | 5.10 | 5.90 | 10.00 | 11.00 | 44.00 | 44.00 | 47.05% | -4.99% | 40.00% | 0 |
| top10 | clean | 8 | -3.90% | 1.10% | 5.30 | 5.70 | 10.00 | 11.00 | 87.40 | 87.40 | 46.46% | -6.20% | 10.00% | 0 |
| top20 | clean | 2 | -3.25% | -0.69% | 4.95 | 6.05 | 10.00 | 11.00 | 22.00 | 22.00 | 44.55% | -8.31% | 30.00% | 20 |
| top20 | clean | 4 | -2.66% | 1.02% | 5.35 | 5.65 | 10.00 | 11.00 | 44.00 | 44.00 | 46.48% | -6.13% | 20.00% | 0 |
| top20 | clean | 8 | -4.98% | 0.25% | 5.00 | 6.00 | 10.00 | 11.00 | 87.25 | 87.25 | 46.08% | -7.25% | 5.00% | 0 |
| IWM | clean |  | -4.87% |  | 9 | 12 | 0 |  |  |  |  |  |  |  |
| RANDOM4 | clean | 4 | -3.74% |  |  |  |  |  |  |  |  |  |  |  |

RANDOM4 on clean, median compound -4.55%, n=1000.
On clean, the 77 names absent from the cleaned file are 0.00% of summed ticker dollars (0.00 / -21772.32), across the frozen top 20 at X=2, 4, and 8.

| set | version | X | compound | flat 15bp | up | down | flat | days entered | entries | closed | win | ex-best | positive | too few |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| best | yahoo | 2 | -5.41% | -4.03% | 7.00 | 4.00 | 10.00 | 11.00 | 22.00 | 22.00 | 40.91% | -10.46% | 0.00% | 1 |
| best | yahoo | 4 | -1.13% | 3.24% | 6.00 | 5.00 | 10.00 | 11.00 | 44.00 | 44.00 | 50.00% | -6.73% | 0.00% | 0 |
| best | yahoo | 8 | -7.10% | -1.93% | 5.00 | 6.00 | 10.00 | 11.00 | 88.00 | 88.00 | 42.05% | -9.81% | 0.00% | 0 |
| top10 | yahoo | 2 | 0.35% | 2.54% | 5.60 | 5.40 | 10.00 | 11.00 | 22.00 | 22.00 | 45.91% | -4.66% | 50.00% | 10 |
| top10 | yahoo | 4 | -2.74% | 0.40% | 5.20 | 5.80 | 10.00 | 11.00 | 44.00 | 44.00 | 45.45% | -6.23% | 40.00% | 0 |
| top10 | yahoo | 8 | -4.92% | -0.03% | 5.10 | 5.90 | 10.00 | 11.00 | 87.40 | 87.40 | 46.35% | -7.10% | 10.00% | 0 |
| top20 | yahoo | 2 | -3.22% | -0.84% | 4.95 | 6.05 | 10.00 | 11.00 | 22.00 | 22.00 | 43.86% | -8.27% | 35.00% | 20 |
| top20 | yahoo | 4 | -3.47% | 0.01% | 5.50 | 5.50 | 10.00 | 11.00 | 44.00 | 44.00 | 44.89% | -7.14% | 35.00% | 0 |
| top20 | yahoo | 8 | -5.29% | -0.20% | 5.15 | 5.85 | 10.00 | 11.00 | 87.25 | 87.25 | 46.08% | -7.45% | 5.00% | 0 |
| IWM | yahoo |  | -3.02% |  | 8 | 10 | 0 |  |  |  |  |  |  |  |
| RANDOM4 | yahoo | 4 | -4.59% |  |  |  |  |  |  |  |  |  |  |  |

RANDOM4 on yahoo, median compound -4.29%, n=1000.
IWM on yahoo has no close on: 2026-09-09, 2026-09-10, 2026-09-11. Marked sessions: 18.
On yahoo, the 77 names absent from the cleaned file are 14.61% of summed ticker dollars (-3498.93 / -23953.15), across the frozen top 20 at X=2, 4, and 8.

## 2026-09-14 through 2026-09-25

| set | version | X | compound | flat 15bp | up | down | flat | days entered | entries | closed | win | ex-best | positive | too few |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| best | clean | 2 | -11.14% | -9.30% | 1.00 | 6.00 | 3.00 | 7.00 | 14.00 | 14.00 | 7.14% | -11.86% | 0.00% | 1 |
| best | clean | 4 | -9.88% | -7.59% | 1.00 | 6.00 | 3.00 | 7.00 | 28.00 | 28.00 | 10.71% | -10.87% | 0.00% | 1 |
| best | clean | 8 | -8.34% | -6.02% | 1.00 | 6.00 | 3.00 | 7.00 | 46.00 | 46.00 | 26.09% | -9.09% | 0.00% | 0 |
| top10 | clean | 2 | -3.60% | -2.57% | 1.90 | 5.10 | 3.00 | 7.00 | 14.00 | 14.00 | 20.00% | -5.75% | 40.00% | 10 |
| top10 | clean | 4 | -7.24% | -5.55% | 1.50 | 5.50 | 3.00 | 7.00 | 28.00 | 28.00 | 18.57% | -8.35% | 0.00% | 10 |
| top10 | clean | 8 | -8.89% | -6.72% | 0.70 | 6.30 | 3.00 | 7.00 | 44.20 | 44.20 | 26.60% | -9.72% | 0.00% | 0 |
| top20 | clean | 2 | -5.92% | -4.90% | 1.90 | 5.10 | 3.00 | 7.00 | 14.00 | 14.00 | 20.71% | -8.18% | 30.00% | 20 |
| top20 | clean | 4 | -8.46% | -6.73% | 1.40 | 5.60 | 3.00 | 7.00 | 28.00 | 28.00 | 20.18% | -9.52% | 0.00% | 20 |
| top20 | clean | 8 | -9.88% | -7.73% | 0.80 | 6.20 | 3.00 | 7.00 | 43.95 | 43.95 | 25.68% | -10.60% | 0.00% | 0 |
| IWM | clean |  | -2.33% |  | 4 | 6 | 0 |  |  |  |  |  |  |  |
| RANDOM4 | clean | 4 | -9.91% |  |  |  |  |  |  |  |  |  |  |  |

RANDOM4 on clean, median compound -10.05%, n=1000.
On clean, the 77 names absent from the cleaned file are 0.00% of summed ticker dollars (0.00 / -46166.81), across the frozen top 20 at X=2, 4, and 8.

| set | version | X | compound | flat 15bp | up | down | flat | days entered | entries | closed | win | ex-best | positive | too few |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| best | yahoo | 2 | -11.19% | -9.27% | 1.00 | 6.00 | 3.00 | 7.00 | 14.00 | 14.00 | 7.14% | -11.88% | 0.00% | 1 |
| best | yahoo | 4 | -9.88% | -7.72% | 1.00 | 6.00 | 3.00 | 7.00 | 28.00 | 28.00 | 10.71% | -10.87% | 0.00% | 1 |
| best | yahoo | 8 | -8.55% | -6.01% | 1.00 | 6.00 | 3.00 | 7.00 | 46.00 | 46.00 | 26.09% | -9.15% | 0.00% | 0 |
| top10 | yahoo | 2 | -3.64% | -2.49% | 1.90 | 5.10 | 3.00 | 7.00 | 14.00 | 14.00 | 20.00% | -5.79% | 40.00% | 10 |
| top10 | yahoo | 4 | -7.14% | -5.47% | 1.50 | 5.50 | 3.00 | 7.00 | 28.00 | 28.00 | 18.57% | -8.23% | 0.00% | 10 |
| top10 | yahoo | 8 | -8.78% | -6.53% | 0.80 | 6.20 | 3.00 | 7.00 | 44.20 | 44.20 | 26.60% | -9.59% | 0.00% | 0 |
| top20 | yahoo | 2 | -5.81% | -4.78% | 1.90 | 5.10 | 3.00 | 7.00 | 14.00 | 14.00 | 20.71% | -8.07% | 30.00% | 20 |
| top20 | yahoo | 4 | -8.25% | -6.54% | 1.40 | 5.60 | 3.00 | 7.00 | 28.00 | 28.00 | 20.18% | -9.29% | 0.00% | 20 |
| top20 | yahoo | 8 | -9.66% | -7.42% | 0.80 | 6.20 | 3.00 | 7.00 | 43.95 | 43.95 | 25.79% | -10.42% | 0.00% | 0 |
| IWM | yahoo |  | n/a |  | 0 | 0 | 0 |  |  |  |  |  |  |  |
| RANDOM4 | yahoo | 4 | -9.50% |  |  |  |  |  |  |  |  |  |  |  |

RANDOM4 on yahoo, median compound -9.72%, n=1000.
IWM on yahoo has no close on: 2026-09-14, 2026-09-15, 2026-09-16, 2026-09-17, 2026-09-18, 2026-09-21, 2026-09-22, 2026-09-23, 2026-09-24, 2026-09-25. Marked sessions: 0.
On yahoo, the 77 names absent from the cleaned file are 0.10% of summed ticker dollars (-45.97 / -44884.59), across the frozen top 20 at X=2, 4, and 8.

## Inputs in the frozen list

| input | top 10 | top 20 | top 20 with clean tune mean > 0 | top 20 with clean forward mean > 0 |
| --- | ---: | ---: | ---: | ---: |
| `board_list` | 1 | 1 | 0 | 0 |
| `cam` | 5 | 11 | 1 | 0 |
| `yday` | 3 | 11 | 0 | 0 |
| `rsi` | 4 | 10 | 1 | 0 |
| `macd` | 6 | 14 | 2 | 0 |
| `flow` | 5 | 11 | 3 | 0 |
| `earn` | 6 | 13 | 2 | 0 |
| `cap_yday` | 1 | 3 | 0 | 0 |
| `cap_macd` | 0 | 1 | 0 | 0 |
| `rsi_mode` | 0 | 0 | 0 | 0 |

The positive columns are a reading of the frozen top 20. They do not change the order.
The mean is the equal-weight mean of the X=2, X=4, and X=8 Futubull compounds.

| formula | tune mean clean | forward mean clean | forward mean yahoo | inputs |
| --- | ---: | ---: | ---: | --- |
| `flow_earn` | 4.56% | -9.79% | -9.87% | flow=1, earn=1 |
| `only_macd` | 4.34% | -0.97% | -0.98% | macd=1 |
| `only_flow` | 0.23% | -8.91% | -8.19% | flow=1 |
| `drop_yday` | 0.01% | -6.06% | -6.03% | cam=1, rsi=1, macd=1, flow=1, earn=1 |
| `cam_macd` | -0.18% | -2.29% | -2.25% | cam=1, macd=1 |
| `board_list` | -2.23% | -10.29% | -10.37% | board_list=1 |
| `drop_flow` | -2.66% | -7.57% | -7.36% | cam=1, yday=1, rsi=1, macd=1, earn=1 |
| `cap_yday_10` | -2.82% | -5.13% | -5.41% | cam=1, yday=1, rsi=1, macd=1, flow=1, earn=1, cap_yday=10 |
| `only_earn` | -3.45% | -9.64% | -9.63% | earn=1 |
| `w2_macd` | -3.80% | -5.11% | -5.09% | cam=1, yday=1, rsi=1, macd=2, flow=1, earn=1 |
| `yday_macd` | -4.13% | -10.22% | -9.83% | yday=1, macd=1 |
| `drop_cam` | -4.75% | -8.27% | -7.28% | yday=1, rsi=1, macd=1, flow=1, earn=1 |
| `rsi_macd` | -5.78% | -8.64% | -6.96% | rsi=1, macd=1 |
| `cap_yday_20` | -5.91% | -5.55% | -5.50% | cam=1, yday=1, rsi=1, macd=1, flow=1, earn=1, cap_yday=20 |
| `cap_yday_10_macd_0_5` | -6.80% | -9.77% | -9.83% | cam=1, yday=1, rsi=1, macd=1, flow=1, earn=1, cap_yday=10, cap_macd=0.5 |
| `w2_yday` | -7.07% | -11.65% | -11.36% | cam=1, yday=2, rsi=1, macd=1, flow=1, earn=1 |
| `cam_earn` | -7.13% | -10.55% | -10.90% | cam=1, earn=1 |
| `drop_rsi` | -7.78% | -5.08% | -5.38% | cam=1, yday=1, macd=1, flow=1, earn=1 |
| `all_equal` | -8.30% | -8.10% | -8.10% | cam=1, yday=1, rsi=1, macd=1, flow=1, earn=1 |
| `only_yday` | -8.93% | -18.08% | -17.77% | yday=1 |

Flat 15bp is the `flat 15bp` column and `compound_15_mean` in `summary.json`. It is not the rank.
A row with `too few` above 0 has at least that many formulas under 30 closed trades.
