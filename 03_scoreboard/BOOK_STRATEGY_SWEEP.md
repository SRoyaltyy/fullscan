# Stock-book paper trading — strategy sweep (daily re-rank)

Every lever combo re-scored on the latest payload. **Sorted by trimmed compound** — the 2 best and 2 worst trades are dropped before compounding, so one lottery winner cannot put a combo on top. `raw` is the untrimmed number (watch the gap: big gap = lottery-driven). `dip` rank pairs only with close entry (same-day change is only knowable at 16:00 ET). Gate = morning predict score >= 1.0 (missing = allowed). 0.15% round-trip fee drag.

| # | Side | Filter | Rank | N | Entry | Hold | Gate | Trimmed % | Raw % | Hit | Trades | Days |
|---:|---|---|---|---:|---|---|---|---:|---:|---:|---:|---:|
| 1 | long | book | book | 5 | close | 1w | score | **55.4** | 61.1 | 0.636 | 121 | 25 |
| 2 | long | book | book | 5 | close | 3d | score | **29.1** | 36.2 | 0.584 | 125 | 26 |
| 3 | long | book | book | 15 | close | 1w | score | **25.9** | 28.4 | 0.556 | 324 | 26 |
| 4 | long | book | book | 5 | close | 3d | none | **20.5** | 33.3 | 0.551 | 236 | 55 |
| 5 | long | book | book | 10 | close | 1w | score | **19.8** | 38.9 | 0.578 | 232 | 26 |
| 6 | long | book | book | 10 | close | 3d | score | **17.2** | 34.6 | 0.539 | 241 | 27 |
| 7 | long | book | book | 15 | close | 3d | score | **15.9** | 21.5 | 0.516 | 335 | 27 |
| 8 | long | book | book | 10 | close | 3d | none | **13.7** | 30.3 | 0.53 | 415 | 56 |
| 9 | long | book | book | 5 | close | 1d | none | **13.2** | 23.1 | 0.516 | 248 | 57 |
| 10 | long | book | book | 15 | close | 3d | none | **13.1** | 17.9 | 0.514 | 519 | 56 |
| 11 | long | book | book | 5 | close | 1w | none | **10.3** | 68.1 | 0.558 | 226 | 53 |
| 12 | long | book | book | 5 | close | 1d | score | **7.0** | 8.5 | 0.533 | 135 | 28 |
| 13 | long | book | book | 15 | close | 1d | none | **2.5** | 8.7 | 0.489 | 536 | 58 |
| 14 | long | book | book | 10 | close | 1d | none | **-0.3** | 14.6 | 0.477 | 432 | 58 |
| 15 | long | book | book | 15 | close | 1d | score | **-1.7** | 0.6 | 0.489 | 350 | 29 |
| 16 | long | book | book | 10 | close | 1d | score | **-3.4** | 7.0 | 0.477 | 256 | 29 |
| 17 | long | book | book | 10 | close | 1w | none | **-4.9** | 41.6 | 0.529 | 397 | 54 |
| 18 | long | book | book | 15 | close | 1w | none | **-11.5** | 31.8 | 0.519 | 499 | 54 |

