# Stock-book paper trading — strategy sweep (daily re-rank)

Every lever combo re-scored on the latest payload. **Sorted by trimmed compound** — the 2 best and 2 worst trades are dropped before compounding, so one lottery winner cannot put a combo on top. `raw` is the untrimmed number (watch the gap: big gap = lottery-driven). `dip` rank pairs only with close entry (same-day change is only knowable at 16:00 ET). Gate = morning predict score >= 1.0 (missing = allowed). 0.15% round-trip fee drag.

| # | Side | Filter | Rank | N | Entry | Hold | Gate | Trimmed % | Raw % | Hit | Trades | Days |
|---:|---|---|---|---:|---|---|---|---:|---:|---:|---:|---:|
| 1 | long | book | book | 5 | close | 1w | none | **50.7** | 90.9 | 0.562 | 208 | 50 |
| 2 | long | book | book | 5 | close | 1w | score | **49.2** | 60.0 | 0.637 | 113 | 24 |
| 3 | long | book | book | 5 | close | 3d | none | **39.0** | 39.7 | 0.555 | 218 | 52 |
| 4 | long | book | book | 15 | close | 1w | score | **28.4** | 28.2 | 0.556 | 297 | 25 |
| 5 | long | book | book | 5 | close | 3d | score | **26.9** | 36.4 | 0.59 | 117 | 25 |
| 6 | long | book | book | 10 | close | 1w | none | **25.7** | 53.4 | 0.527 | 364 | 51 |
| 7 | long | book | book | 10 | close | 3d | none | **22.9** | 27.9 | 0.536 | 379 | 53 |
| 8 | long | book | book | 10 | close | 1w | score | **20.4** | 37.8 | 0.573 | 213 | 25 |
| 9 | long | book | book | 15 | close | 1w | none | **17.8** | 43.8 | 0.52 | 458 | 51 |
| 10 | long | book | book | 5 | close | 1d | none | **15.9** | 14.0 | 0.518 | 228 | 54 |
| 11 | long | book | book | 15 | close | 3d | score | **13.9** | 18.7 | 0.519 | 308 | 26 |
| 12 | long | book | book | 15 | close | 3d | none | **11.0** | 20.3 | 0.519 | 478 | 53 |
| 13 | long | book | book | 10 | close | 3d | score | **10.5** | 26.3 | 0.543 | 219 | 26 |
| 14 | long | book | book | 5 | close | 1d | score | **8.1** | -0.9 | 0.533 | 120 | 26 |
| 15 | long | book | book | 10 | close | 1d | none | **2.4** | 11.6 | 0.481 | 397 | 55 |
| 16 | long | book | book | 15 | close | 1d | none | **0.3** | 6.3 | 0.49 | 498 | 55 |
| 17 | long | book | book | 10 | close | 1d | score | **-2.5** | 2.5 | 0.476 | 229 | 27 |
| 18 | long | book | book | 15 | close | 1d | score | **-4.0** | -3.3 | 0.484 | 320 | 27 |

