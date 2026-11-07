# Stock-book paper trading — strategy sweep (daily re-rank)

Every lever combo re-scored on the latest payload. **Sorted by trimmed compound** — the 2 best and 2 worst trades are dropped before compounding, so one lottery winner cannot put a combo on top. `raw` is the untrimmed number (watch the gap: big gap = lottery-driven). `dip` rank pairs only with close entry (same-day change is only knowable at 16:00 ET). Gate = morning predict score >= 1.0 (missing = allowed). 0.15% round-trip fee drag.

| # | Side | Filter | Rank | N | Entry | Hold | Gate | Trimmed % | Raw % | Hit | Trades | Days |
|---:|---|---|---|---:|---|---|---|---:|---:|---:|---:|---:|
| 1 | long | book | book | 5 | close | 1w | none | **50.7** | 90.9 | 0.562 | 208 | 50 |
| 2 | long | book | book | 5 | close | 1w | score | **49.2** | 60.0 | 0.637 | 113 | 24 |
| 3 | long | book | book | 5 | close | 3d | none | **44.6** | 45.2 | 0.555 | 220 | 52 |
| 4 | long | book | book | 10 | close | 3d | none | **32.4** | 38.3 | 0.538 | 385 | 53 |
| 5 | long | book | book | 5 | close | 3d | score | **32.0** | 41.8 | 0.588 | 119 | 25 |
| 6 | long | book | book | 15 | close | 1w | score | **28.4** | 28.2 | 0.556 | 297 | 25 |
| 7 | long | book | book | 10 | close | 1w | none | **25.7** | 53.4 | 0.527 | 364 | 51 |
| 8 | long | book | book | 10 | close | 1w | score | **20.4** | 37.8 | 0.573 | 213 | 25 |
| 9 | long | book | book | 10 | close | 3d | score | **19.1** | 36.6 | 0.547 | 225 | 26 |
| 10 | long | book | book | 15 | close | 1w | none | **17.8** | 43.8 | 0.52 | 458 | 51 |
| 11 | long | book | book | 15 | close | 3d | score | **17.1** | 22.5 | 0.522 | 314 | 26 |
| 12 | long | book | book | 15 | close | 3d | none | **14.2** | 24.2 | 0.521 | 484 | 53 |
| 13 | long | book | book | 5 | close | 1d | none | **11.1** | 20.9 | 0.511 | 231 | 54 |
| 14 | long | book | book | 5 | close | 1d | score | **3.6** | 5.1 | 0.52 | 123 | 26 |
| 15 | long | book | book | 15 | close | 1d | none | **0.8** | 6.9 | 0.483 | 505 | 55 |
| 16 | long | book | book | 10 | close | 1d | none | **-1.8** | 12.7 | 0.473 | 404 | 55 |
| 17 | long | book | book | 15 | close | 1d | score | **-4.9** | -2.8 | 0.474 | 327 | 27 |
| 18 | long | book | book | 10 | close | 1d | score | **-6.5** | 3.5 | 0.462 | 236 | 27 |

