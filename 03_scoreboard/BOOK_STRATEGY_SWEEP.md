# Stock-book paper trading — strategy sweep (daily re-rank)

Every lever combo re-scored on the latest payload. **Sorted by trimmed compound** — the 2 best and 2 worst trades are dropped before compounding, so one lottery winner cannot put a combo on top. `raw` is the untrimmed number (watch the gap: big gap = lottery-driven). `dip` rank pairs only with close entry (same-day change is only knowable at 16:00 ET). Gate = morning predict score >= 1.0 (missing = allowed). 0.15% round-trip fee drag.

| # | Side | Filter | Rank | N | Entry | Hold | Gate | Trimmed % | Raw % | Hit | Trades | Days |
|---:|---|---|---|---:|---|---|---|---:|---:|---:|---:|---:|
| 1 | long | book | book | 5 | close | 1w | score | **40.0** | 40.5 | 0.65 | 100 | 21 |
| 2 | long | book | book | 5 | close | 3d | score | **31.0** | 35.5 | 0.617 | 107 | 22 |
| 3 | long | book | book | 15 | close | 1w | score | **30.7** | 30.6 | 0.587 | 259 | 22 |
| 4 | long | book | book | 5 | close | 1w | none | **28.8** | 88.8 | 0.565 | 184 | 42 |
| 5 | long | book | book | 5 | close | 3d | none | **24.6** | 15.5 | 0.562 | 194 | 45 |
| 6 | long | book | book | 15 | close | 1w | none | **23.0** | 80.2 | 0.543 | 403 | 43 |
| 7 | long | book | book | 10 | close | 3d | none | **22.7** | 23.7 | 0.548 | 336 | 46 |
| 8 | long | book | book | 15 | close | 3d | none | **22.2** | 12.0 | 0.531 | 422 | 46 |
| 9 | long | book | book | 10 | close | 3d | score | **21.7** | 38.8 | 0.568 | 199 | 23 |
| 10 | long | book | book | 15 | close | 3d | score | **21.0** | 25.3 | 0.542 | 275 | 23 |
| 11 | long | book | book | 10 | close | 1w | score | **17.8** | 34.9 | 0.596 | 188 | 22 |
| 12 | long | book | book | 10 | close | 1w | none | **11.0** | 84.8 | 0.547 | 322 | 43 |
| 13 | long | book | book | 5 | close | 1d | none | **7.0** | 54.2 | 0.503 | 199 | 47 |
| 14 | long | book | book | 5 | close | 1d | score | **2.7** | 4.9 | 0.5 | 108 | 22 |
| 15 | long | book | book | 15 | close | 1d | none | **2.3** | 18.0 | 0.488 | 432 | 48 |
| 16 | long | book | book | 15 | close | 1d | score | **-1.6** | 1.0 | 0.478 | 278 | 23 |
| 17 | long | book | book | 10 | close | 1d | none | **-1.7** | 22.5 | 0.474 | 346 | 48 |
| 18 | long | book | book | 10 | close | 1d | score | **-4.6** | 5.8 | 0.46 | 202 | 23 |

