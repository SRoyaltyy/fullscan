# Stock-book paper trading — strategy sweep (daily re-rank)

Every lever combo re-scored on the latest payload. **Sorted by trimmed compound** — the 2 best and 2 worst trades are dropped before compounding, so one lottery winner cannot put a combo on top. `raw` is the untrimmed number (watch the gap: big gap = lottery-driven). `dip` rank pairs only with close entry (same-day change is only knowable at 16:00 ET). Gate = morning predict score >= 1.0 (missing = allowed). 0.15% round-trip fee drag.

| # | Side | Filter | Rank | N | Entry | Hold | Gate | Trimmed % | Raw % | Hit | Trades | Days |
|---:|---|---|---|---:|---|---|---|---:|---:|---:|---:|---:|
| 1 | long | book | book | 10 | close | 3d | none | **7.0** | 20.9 | 0.597 | 72 | 8 |
| 2 | long | book | book | 10 | close | 3d | score | **6.8** | 20.6 | 0.619 | 42 | 5 |
| 3 | long | book | book | 15 | close | 3d | none | **4.7** | 8.1 | 0.552 | 105 | 8 |
| 4 | long | book | book | 5 | close | 3d | score | **4.6** | 2.1 | 0.65 | 20 | 4 |
| 5 | long | book | book | 5 | close | 3d | none | **4.3** | 1.8 | 0.6 | 35 | 7 |
| 6 | long | book | book | 15 | close | 3d | score | **4.1** | 7.5 | 0.569 | 65 | 5 |
| 7 | long | book | book | 15 | close | 1w | none | **2.2** | 6.2 | 0.495 | 95 | 7 |
| 8 | long | book | book | 15 | close | 1w | score | **1.9** | 5.9 | 0.537 | 80 | 6 |
| 9 | long | book | book | 5 | close | 1w | score | **1.7** | -0.9 | 0.52 | 25 | 5 |
| 10 | long | book | book | 5 | close | 1d | score | **1.2** | 1.0 | 0.6 | 25 | 5 |
| 11 | long | book | book | 5 | close | 1w | none | **1.2** | -1.3 | 0.467 | 30 | 6 |
| 12 | long | book | book | 15 | close | 1d | score | **1.1** | 3.4 | 0.55 | 80 | 6 |
| 13 | long | book | book | 15 | close | 1d | none | **0.8** | 2.6 | 0.514 | 140 | 11 |
| 14 | long | book | book | 10 | close | 1d | score | **-0.4** | 10.3 | 0.577 | 52 | 6 |
| 15 | long | book | book | 10 | close | 1w | score | **-1.3** | 15.2 | 0.558 | 52 | 6 |
| 16 | long | book | book | 10 | close | 1d | none | **-1.6** | 8.5 | 0.5 | 102 | 11 |
| 17 | long | book | book | 5 | close | 1d | none | **-1.7** | -2.5 | 0.52 | 50 | 10 |
| 18 | long | book | book | 10 | close | 1w | none | **-2.0** | 14.4 | 0.5 | 62 | 7 |

