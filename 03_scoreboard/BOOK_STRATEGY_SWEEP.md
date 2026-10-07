# Stock-book paper trading — strategy sweep (daily re-rank)

Every lever combo re-scored on the latest payload. **Sorted by trimmed compound** — the 2 best and 2 worst trades are dropped before compounding, so one lottery winner cannot put a combo on top. `raw` is the untrimmed number (watch the gap: big gap = lottery-driven). `dip` rank pairs only with close entry (same-day change is only knowable at 16:00 ET). Gate = morning predict score >= 1.0 (missing = allowed). 0.15% round-trip fee drag.

| # | Side | Filter | Rank | N | Entry | Hold | Gate | Trimmed % | Raw % | Hit | Trades | Days |
|---:|---|---|---|---:|---|---|---|---:|---:|---:|---:|---:|
| 1 | long | book | book | 5 | close | 3d | score | **8.2** | 12.5 | 0.557 | 70 | 14 |
| 2 | long | book | book | 10 | close | 3d | score | **8.2** | 23.9 | 0.537 | 136 | 15 |
| 3 | long | book | book | 15 | close | 3d | score | **5.7** | 10.6 | 0.508 | 197 | 15 |
| 4 | long | book | book | 15 | close | 1w | score | **1.9** | 6.1 | 0.531 | 192 | 14 |
| 5 | long | book | book | 5 | close | 1w | score | **1.7** | 3.5 | 0.538 | 65 | 13 |
| 6 | long | book | book | 5 | close | 1d | score | **0.9** | 2.2 | 0.48 | 75 | 15 |
| 7 | long | book | book | 15 | close | 1d | score | **-2.8** | -0.2 | 0.458 | 212 | 16 |
| 8 | long | book | book | 10 | close | 1d | score | **-4.9** | 5.5 | 0.438 | 146 | 16 |
| 9 | long | book | book | 10 | close | 1w | score | **-5.8** | 11.7 | 0.542 | 131 | 14 |
| 10 | long | book | book | 5 | close | 1d | none | **-8.5** | -9.9 | 0.465 | 144 | 32 |
| 11 | long | book | book | 10 | close | 3d | none | **-9.6** | -8.8 | 0.49 | 241 | 31 |
| 12 | long | book | book | 15 | close | 1d | none | **-10.1** | -10.2 | 0.456 | 333 | 33 |
| 13 | long | book | book | 15 | close | 3d | none | **-11.0** | -18.4 | 0.478 | 312 | 31 |
| 14 | long | book | book | 10 | close | 1d | none | **-13.1** | -5.9 | 0.436 | 257 | 33 |
| 15 | long | book | book | 5 | close | 3d | none | **-17.1** | -23.2 | 0.463 | 134 | 30 |
| 16 | long | book | book | 15 | close | 1w | none | **-21.3** | -18.1 | 0.465 | 301 | 29 |
| 17 | long | book | book | 5 | close | 1w | none | **-25.7** | -24.4 | 0.427 | 124 | 28 |
| 18 | long | book | book | 10 | close | 1w | none | **-27.8** | -14.4 | 0.461 | 230 | 29 |

