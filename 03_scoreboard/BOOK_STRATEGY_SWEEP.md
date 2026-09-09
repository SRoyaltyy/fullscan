# Stock-book paper trading — strategy sweep (daily re-rank)

Every lever combo re-scored on the latest payload. **Sorted by trimmed compound** — the 2 best and 2 worst trades are dropped before compounding, so one lottery winner cannot put a combo on top. `raw` is the untrimmed number (watch the gap: big gap = lottery-driven). `dip` rank pairs only with close entry (same-day change is only knowable at 16:00 ET). Gate = morning predict score >= 1.0 (missing = allowed). 0.15% round-trip fee drag.

| # | Side | Filter | Rank | N | Entry | Hold | Gate | Trimmed % | Raw % | Hit | Trades | Days |
|---:|---|---|---|---:|---|---|---|---:|---:|---:|---:|---:|
| 1 | long | book | book | 10 | close | 3d | score | **6.8** | 20.6 | 0.619 | 42 | 5 |
| 2 | long | book | book | 5 | close | 3d | score | **4.6** | 2.1 | 0.65 | 20 | 4 |
| 3 | long | book | book | 15 | close | 3d | score | **4.1** | 7.5 | 0.569 | 65 | 5 |
| 4 | long | book | book | 10 | close | 3d | none | **3.0** | 16.4 | 0.5 | 100 | 11 |
| 5 | long | book | book | 15 | close | 1w | score | **1.9** | 5.9 | 0.537 | 80 | 6 |
| 6 | long | book | book | 5 | close | 1w | score | **1.7** | -0.9 | 0.52 | 25 | 5 |
| 7 | long | book | book | 15 | close | 1w | none | **0.9** | 4.8 | 0.496 | 115 | 9 |
| 8 | long | book | book | 15 | close | 3d | none | **0.8** | 4.1 | 0.489 | 133 | 11 |
| 9 | long | book | book | 5 | close | 1d | score | **-0.2** | -0.4 | 0.486 | 35 | 7 |
| 10 | long | book | book | 5 | close | 3d | none | **-0.5** | -2.9 | 0.5 | 50 | 10 |
| 11 | long | book | book | 5 | close | 1w | none | **-1.1** | -3.1 | 0.45 | 40 | 8 |
| 12 | long | book | book | 10 | close | 1w | score | **-1.3** | 15.2 | 0.558 | 52 | 6 |
| 13 | long | book | book | 15 | close | 1d | score | **-1.4** | 0.8 | 0.465 | 101 | 8 |
| 14 | long | book | book | 15 | close | 1d | none | **-2.0** | -0.3 | 0.462 | 169 | 14 |
| 15 | long | book | book | 10 | close | 1d | score | **-2.6** | 7.8 | 0.485 | 68 | 8 |
| 16 | long | book | book | 10 | close | 1w | none | **-3.3** | 12.9 | 0.5 | 82 | 9 |
| 17 | long | book | book | 5 | close | 1d | none | **-3.6** | -4.4 | 0.462 | 65 | 13 |
| 18 | long | book | book | 10 | close | 1d | none | **-4.1** | 5.7 | 0.452 | 126 | 14 |

