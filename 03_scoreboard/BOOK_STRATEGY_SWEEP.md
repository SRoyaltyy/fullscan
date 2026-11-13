# Stock-book paper trading — strategy sweep (daily re-rank)

Every lever combo re-scored on the latest payload. **Sorted by trimmed compound** — the 2 best and 2 worst trades are dropped before compounding, so one lottery winner cannot put a combo on top. `raw` is the untrimmed number (watch the gap: big gap = lottery-driven). `dip` rank pairs only with close entry (same-day change is only knowable at 16:00 ET). Gate = morning predict score >= 1.0 (missing = allowed). 0.15% round-trip fee drag.

| # | Side | Filter | Rank | N | Entry | Hold | Gate | Trimmed % | Raw % | Hit | Trades | Days |
|---:|---|---|---|---:|---|---|---|---:|---:|---:|---:|---:|
| 1 | long | book | book | 5 | close | 1w | score | **55.5** | 61.2 | 0.64 | 125 | 26 |
| 2 | long | book | book | 5 | close | 3d | score | **31.7** | 38.9 | 0.585 | 135 | 28 |
| 3 | long | book | book | 15 | close | 1w | score | **24.8** | 27.3 | 0.552 | 335 | 27 |
| 4 | long | book | book | 5 | close | 3d | none | **22.9** | 36.0 | 0.553 | 246 | 57 |
| 5 | long | book | book | 10 | close | 3d | score | **19.8** | 37.5 | 0.547 | 256 | 29 |
| 6 | long | book | book | 10 | close | 1w | score | **19.7** | 38.8 | 0.577 | 241 | 27 |
| 7 | long | book | book | 15 | close | 3d | score | **18.5** | 24.2 | 0.523 | 350 | 29 |
| 8 | long | book | book | 10 | close | 3d | none | **16.2** | 33.2 | 0.535 | 430 | 58 |
| 9 | long | book | book | 15 | close | 3d | none | **15.6** | 20.5 | 0.519 | 534 | 58 |
| 10 | long | book | book | 5 | close | 1w | none | **12.1** | 70.7 | 0.562 | 235 | 55 |
| 11 | long | book | book | 5 | close | 1d | none | **11.6** | 21.4 | 0.519 | 258 | 59 |
| 12 | long | book | book | 5 | close | 1d | score | **5.6** | 7.0 | 0.538 | 145 | 30 |
| 13 | long | book | book | 15 | close | 1d | none | **1.1** | 7.2 | 0.486 | 566 | 60 |
| 14 | long | book | book | 10 | close | 1d | none | **-1.9** | 12.7 | 0.478 | 452 | 60 |
| 15 | long | book | book | 15 | close | 1d | score | **-3.1** | -0.8 | 0.484 | 380 | 31 |
| 16 | long | book | book | 10 | close | 1w | none | **-4.7** | 41.9 | 0.527 | 414 | 56 |
| 17 | long | book | book | 10 | close | 1d | score | **-4.9** | 5.3 | 0.478 | 276 | 31 |
| 18 | long | book | book | 15 | close | 1w | none | **-11.9** | 31.1 | 0.515 | 518 | 56 |

