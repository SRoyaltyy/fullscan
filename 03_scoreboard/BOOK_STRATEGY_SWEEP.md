# Stock-book paper trading — strategy sweep (daily re-rank)

Every lever combo re-scored on the latest payload. **Sorted by trimmed compound** — the 2 best and 2 worst trades are dropped before compounding, so one lottery winner cannot put a combo on top. `raw` is the untrimmed number (watch the gap: big gap = lottery-driven). `dip` rank pairs only with close entry (same-day change is only knowable at 16:00 ET). Gate = morning predict score >= 1.0 (missing = allowed). 0.15% round-trip fee drag.

| # | Side | Filter | Rank | N | Entry | Hold | Gate | Trimmed % | Raw % | Hit | Trades | Days |
|---:|---|---|---|---:|---|---|---|---:|---:|---:|---:|---:|
| 1 | long | book | book | 5 | close | 3d | none | **58.4** | 56.6 | 0.563 | 215 | 51 |
| 2 | long | book | book | 5 | close | 1w | none | **55.8** | 142.7 | 0.565 | 207 | 49 |
| 3 | long | book | book | 5 | close | 1w | score | **49.2** | 60.0 | 0.637 | 113 | 24 |
| 4 | long | book | book | 5 | close | 3d | score | **44.6** | 52.9 | 0.605 | 114 | 24 |
| 5 | long | book | book | 10 | close | 3d | none | **36.4** | 42.2 | 0.539 | 375 | 52 |
| 6 | long | book | book | 15 | close | 3d | none | **33.9** | 27.6 | 0.521 | 470 | 52 |
| 7 | long | book | book | 10 | close | 1w | none | **30.3** | 93.7 | 0.528 | 362 | 50 |
| 8 | long | book | book | 15 | close | 1w | score | **27.5** | 27.4 | 0.554 | 296 | 25 |
| 9 | long | book | book | 10 | close | 3d | score | **24.7** | 41.6 | 0.551 | 216 | 25 |
| 10 | long | book | book | 15 | close | 1w | none | **22.2** | 81.6 | 0.52 | 456 | 50 |
| 11 | long | book | book | 15 | close | 3d | score | **21.3** | 26.9 | 0.525 | 301 | 25 |
| 12 | long | book | book | 10 | close | 1w | score | **19.6** | 36.9 | 0.571 | 212 | 25 |
| 13 | long | book | book | 5 | close | 1d | none | **18.4** | 29.0 | 0.518 | 224 | 53 |
| 14 | long | book | book | 5 | close | 1d | score | **9.3** | 11.7 | 0.538 | 119 | 25 |
| 15 | long | book | book | 15 | close | 1d | none | **5.5** | 11.8 | 0.49 | 490 | 54 |
| 16 | long | book | book | 10 | close | 1d | none | **3.0** | 17.9 | 0.478 | 391 | 54 |
| 17 | long | book | book | 15 | close | 1d | score | **-0.9** | 1.4 | 0.486 | 315 | 26 |
| 18 | long | book | book | 10 | close | 1d | score | **-2.7** | 7.9 | 0.473 | 226 | 26 |

