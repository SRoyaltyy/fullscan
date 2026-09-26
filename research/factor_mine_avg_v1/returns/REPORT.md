# factor_mine_avg_v1

study: factor_mine_avg_v1

Preregistration fingerprint `3d6f2127f9e4107f5f3d864ae8cd8578d004dbf403b4d1d84098620f598775cd`.
Every session is `designed_after`. The study was created 2026-09-26.

Universe: 15 long top-N ranked buys from the #357 110 that also sit in the #336 book.

| recipe | rank | top N | hold |
| --- | --- | ---: | ---: |
| `union_candle_score_h1` | candle_score | 8 | 1 |
| `union_candle_score_h3` | candle_score | 8 | 3 |
| `union_cond_h1` | cond | 8 | 1 |
| `union_cond_h3` | cond | 8 | 3 |
| `union_cond_n4_h3` | cond | 4 | 3 |
| `union_hot_n12_h1` | hot_score | 12 | 1 |
| `union_hot_n4_h1` | hot_score | 4 | 1 |
| `union_hot_score_h1` | hot_score | 8 | 1 |
| `union_hot_score_h3` | hot_score | 8 | 3 |
| `union_ret_5_h1` | ret_5 | 8 | 1 |
| `union_ret_5_h3` | ret_5 | 8 | 3 |
| `union_w_hot_candle_h1` | w_hot_candle | 8 | 1 |
| `union_w_hot_candle_h3` | w_hot_candle | 8 | 3 |
| `union_w_hot_cond_h1` | w_hot_cond | 8 | 1 |
| `union_w_hot_cond_h3` | w_hot_cond | 8 | 3 |

Bars: `data/prices/ohlc.parquet` sha256 `559c8cf099808930bef2b4de4280b4e902883c9a1de85c8a417074f11aaefa55`.
Drop list: #362 `ad10f862ad51df88f4dd03ff3dbcdf628f7e2685`, cleaned snapshot sha256 `5c272584309e14496dc006a3a356c6960ed5b340f945af3fd6f299301b261ef2`.
Version (a) removes 77 names. 77 of them have bars on this pin.
Version (b) keeps them. Flagged matched splits: ALP, NFE, TNMG, WCT.
The 52-name open/previous-close subset is inside the 77. YAAS is in the 77 and outside the 52.

## Before 2026-09-14, through 2026-09-11

| version | compound mean | compound median | up mean/median | down mean/median | flat mean/median | win mean | win median | win omitted | ex-best mean | ex-best median | positive share |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| a | -7.75% | -7.91% | 3.67 / 3.00 | 10.33 / 10.00 | 7.00 / 8.00 | 34.49% | 34.72% | 0 | -9.13% | -9.44% | 0.00% |
| b | -8.15% | -7.95% | 3.87 / 4.00 | 10.00 / 10.00 | 7.13 / 8.00 | 34.70% | 34.48% | 0 | -9.53% | -9.60% | 0.00% |

Version a flat 15bp compound mean -4.08%, median -4.18%. Closed trades mean 54.07, median 39.00.
Version b flat 15bp compound mean -4.75%, median -5.39%. Closed trades mean 53.73, median 39.00.

Mean compound (b) minus (a): -0.40%.
Median compound (b) minus (a): -0.03%.
P&L share of all 77 dropped names on (b): 11.74% of summed ticker dollars (-1435.60 / -12224.16). Equal-weight mean of per-recipe shares: 11.50%.
P&L share of the 52-name subset on (b): 6.12%. Equal-weight mean of per-recipe shares: 7.14%.
P&L share of ALP, NFE, TNMG, WCT on (b): 0.00%. Equal-weight mean of per-recipe shares: 0.00%.

| benchmark | version | compound mean | compound median | up | down | flat |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| IWM | a | -3.02% |  | 8 | 10 | 0 |
| RANDOM4 | a | -8.14% | -8.10% |  |  |  |
| IWM | b | -3.02% |  | 8 | 10 | 0 |
| RANDOM4 | b | -8.76% | -8.81% |  |  |  |

IWM on version a has no bar on: 2026-09-09, 2026-09-10, 2026-09-11. Those sessions are not a flat day. Marked sessions: 18.
IWM on version b has no bar on: 2026-09-09, 2026-09-10, 2026-09-11. Those sessions are not a flat day. Marked sessions: 18.

Per recipe, compound then ex-best, version (a) then (b). Order is the locked name order.

| recipe | a compound | a ex-best | a win | b compound | b ex-best | b win |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `union_candle_score_h1` | -4.44% | -5.52% | 40.28% | -6.29% | -7.36% | 40.28% |
| `union_candle_score_h3` | -8.59% | -9.24% | 28.21% | -8.60% | -9.24% | 28.21% |
| `union_cond_h1` | -8.36% | -9.44% | 34.72% | -9.95% | -11.02% | 33.33% |
| `union_cond_h3` | -6.78% | -7.89% | 33.33% | -6.82% | -7.93% | 33.33% |
| `union_cond_n4_h3` | -7.91% | -9.81% | 47.62% | -7.95% | -9.85% | 47.62% |
| `union_hot_n12_h1` | -10.25% | -10.91% | 32.41% | -11.62% | -12.28% | 31.48% |
| `union_hot_n4_h1` | -6.70% | -8.52% | 36.11% | -4.18% | -6.04% | 38.89% |
| `union_hot_score_h1` | -8.79% | -9.79% | 38.89% | -9.89% | -10.90% | 38.89% |
| `union_hot_score_h3` | -7.52% | -9.60% | 33.33% | -7.52% | -9.60% | 34.48% |
| `union_ret_5_h1` | -8.03% | -9.04% | 36.11% | -8.24% | -9.26% | 37.50% |
| `union_ret_5_h3` | -10.00% | -12.05% | 15.38% | -10.03% | -12.08% | 15.79% |
| `union_w_hot_candle_h1` | -6.22% | -7.29% | 41.67% | -7.42% | -8.49% | 41.67% |
| `union_w_hot_candle_h3` | -6.32% | -8.45% | 27.03% | -6.34% | -8.47% | 25.71% |
| `union_w_hot_cond_h1` | -8.79% | -9.79% | 38.89% | -9.89% | -10.90% | 38.89% |
| `union_w_hot_cond_h3` | -7.52% | -9.60% | 33.33% | -7.52% | -9.60% | 34.48% |

## 2026-09-14 through 2026-09-25

| version | compound mean | compound median | up mean/median | down mean/median | flat mean/median | win mean | win median | win omitted | ex-best mean | ex-best median | positive share |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| a | -10.47% | -9.41% | 2.13 / 2.00 | 7.87 / 8.00 | 0.00 / 0.00 | 24.88% | 25.97% | 0 | -11.24% | -10.75% | 0.00% |
| b | -10.47% | -9.31% | 2.13 / 2.00 | 7.87 / 8.00 | 0.00 / 0.00 | 24.95% | 25.97% | 0 | -11.24% | -10.65% | 0.00% |

Version a flat 15bp compound mean -7.76%, median -8.32%. Closed trades mean 60.47, median 48.00.
Version b flat 15bp compound mean -7.77%, median -8.45%. Closed trades mean 60.33, median 47.00.

Mean compound (b) minus (a): 0.00%.
Median compound (b) minus (a): 0.11%.
P&L share of all 77 dropped names on (b): 0.00% of summed ticker dollars (0.00 / -14388.52). Equal-weight mean of per-recipe shares: 0.00%.
P&L share of the 52-name subset on (b): 0.00%. Equal-weight mean of per-recipe shares: 0.00%.
P&L share of ALP, NFE, TNMG, WCT on (b): 0.00%. Equal-weight mean of per-recipe shares: 0.00%.

| benchmark | version | compound mean | compound median | up | down | flat |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| IWM | a | n/a |  | 0 | 0 | 0 |
| RANDOM4 | a | -11.63% | -11.52% |  |  |  |
| IWM | b | n/a |  | 0 | 0 | 0 |
| RANDOM4 | b | -11.72% | -11.65% |  |  |  |

IWM on version a has no bar on: 2026-09-14, 2026-09-15, 2026-09-16, 2026-09-17, 2026-09-18, 2026-09-21, 2026-09-22, 2026-09-23, 2026-09-24, 2026-09-25. Those sessions are not a flat day. Marked sessions: 0.
IWM on version b has no bar on: 2026-09-14, 2026-09-15, 2026-09-16, 2026-09-17, 2026-09-18, 2026-09-21, 2026-09-22, 2026-09-23, 2026-09-24, 2026-09-25. Those sessions are not a flat day. Marked sessions: 0.

Per recipe, compound then ex-best, version (a) then (b). Order is the locked name order.

| recipe | a compound | a ex-best | a win | b compound | b ex-best | b win |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `union_candle_score_h1` | -9.23% | -9.73% | 32.47% | -9.18% | -9.67% | 32.47% |
| `union_candle_score_h3` | -3.29% | -4.71% | 24.44% | -3.29% | -4.70% | 24.44% |
| `union_cond_h1` | -12.61% | -13.06% | 26.32% | -12.57% | -13.04% | 26.32% |
| `union_cond_h3` | -8.43% | -9.17% | 22.92% | -8.41% | -9.15% | 23.40% |
| `union_cond_n4_h3` | -9.41% | -10.75% | 15.38% | -9.31% | -10.65% | 16.00% |
| `union_hot_n12_h1` | -15.62% | -16.01% | 22.94% | -15.75% | -16.15% | 22.94% |
| `union_hot_n4_h1` | -12.72% | -13.50% | 27.50% | -12.69% | -13.48% | 27.50% |
| `union_hot_score_h1` | -13.53% | -14.00% | 25.97% | -13.52% | -13.99% | 25.97% |
| `union_hot_score_h3` | -8.33% | -9.02% | 21.74% | -8.33% | -9.02% | 21.74% |
| `union_ret_5_h1` | -15.74% | -16.18% | 25.97% | -15.76% | -16.20% | 25.97% |
| `union_ret_5_h3` | -8.85% | -10.18% | 26.19% | -8.85% | -10.18% | 26.19% |
| `union_w_hot_candle_h1` | -12.12% | -12.60% | 28.57% | -12.24% | -12.71% | 28.57% |
| `union_w_hot_candle_h3` | -5.28% | -6.67% | 25.00% | -5.28% | -6.67% | 25.00% |
| `union_w_hot_cond_h1` | -13.53% | -14.00% | 25.97% | -13.52% | -13.99% | 25.97% |
| `union_w_hot_cond_h3` | -8.33% | -9.02% | 21.74% | -8.33% | -9.02% | 21.74% |

IWM is one buy at the 2026-08-13 open, marked at each close, still held on 2026-09-25. The buy fee is in the path. There is no exit fee.
RANDOM4 is 1000 draws, seed 20260813 plus the draw index, 4 names, hold 1, from that day's stock_book list. Version (a) draws after the 77 names are removed.
A closed trade wins when its Futubull round trip is positive. Lots still open on 2026-09-25 are not in the win rate.
