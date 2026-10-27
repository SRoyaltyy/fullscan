# Stock-book paper trading — strategy sweep (daily re-rank)

Every lever combo re-scored on the latest payload. **Sorted by trimmed compound** — the 2 best and 2 worst trades are dropped before compounding, so one lottery winner cannot put a combo on top. `raw` is the untrimmed number (watch the gap: big gap = lottery-driven). `dip` rank pairs only with close entry (same-day change is only knowable at 16:00 ET). Gate = morning predict score >= 1.0 (missing = allowed). 0.15% round-trip fee drag.

| # | Side | Filter | Rank | N | Entry | Hold | Gate | Trimmed % | Raw % | Hit | Trades | Days |
|---:|---|---|---|---:|---|---|---|---:|---:|---:|---:|---:|
| 1 | long | book | book | 5 | close | 1w | score | **38.3** | 38.8 | 0.653 | 98 | 20 |
| 2 | long | book | book | 5 | close | 3d | score | **29.5** | 33.9 | 0.621 | 103 | 21 |
| 3 | long | book | book | 15 | close | 1w | score | **28.8** | 28.7 | 0.588 | 250 | 21 |
| 4 | long | book | book | 5 | close | 1w | none | **24.6** | 23.8 | 0.564 | 181 | 40 |
| 5 | long | book | book | 15 | close | 3d | score | **21.8** | 26.2 | 0.547 | 265 | 22 |
| 6 | long | book | book | 10 | close | 3d | score | **21.6** | 38.6 | 0.573 | 192 | 22 |
| 7 | long | book | book | 15 | close | 1w | none | **19.2** | 17.9 | 0.542 | 393 | 41 |
| 8 | long | book | book | 10 | close | 1w | score | **17.3** | 34.2 | 0.602 | 181 | 21 |
| 9 | long | book | book | 5 | close | 3d | none | **14.2** | 5.9 | 0.561 | 189 | 43 |
| 10 | long | book | book | 15 | close | 3d | none | **14.1** | 4.6 | 0.533 | 411 | 44 |
| 11 | long | book | book | 10 | close | 3d | none | **13.6** | 14.6 | 0.549 | 328 | 44 |
| 12 | long | book | book | 10 | close | 1w | none | **7.7** | 22.1 | 0.548 | 314 | 41 |
| 13 | long | book | book | 5 | close | 1d | none | **5.8** | 5.4 | 0.5 | 196 | 45 |
| 14 | long | book | book | 5 | close | 1d | score | **2.7** | 4.9 | 0.5 | 108 | 22 |
| 15 | long | book | book | 15 | close | 1d | none | **-0.1** | 1.1 | 0.485 | 423 | 46 |
| 16 | long | book | book | 15 | close | 1d | score | **-1.2** | 1.4 | 0.48 | 275 | 23 |
| 17 | long | book | book | 10 | close | 1d | none | **-4.7** | 4.5 | 0.467 | 338 | 46 |
| 18 | long | book | book | 10 | close | 1d | score | **-4.7** | 5.7 | 0.46 | 200 | 23 |

