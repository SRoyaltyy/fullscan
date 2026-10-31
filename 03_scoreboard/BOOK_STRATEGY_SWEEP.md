# Stock-book paper trading — strategy sweep (daily re-rank)

Every lever combo re-scored on the latest payload. **Sorted by trimmed compound** — the 2 best and 2 worst trades are dropped before compounding, so one lottery winner cannot put a combo on top. `raw` is the untrimmed number (watch the gap: big gap = lottery-driven). `dip` rank pairs only with close entry (same-day change is only knowable at 16:00 ET). Gate = morning predict score >= 1.0 (missing = allowed). 0.15% round-trip fee drag.

| # | Side | Filter | Rank | N | Entry | Hold | Gate | Trimmed % | Raw % | Hit | Trades | Days |
|---:|---|---|---|---:|---|---|---|---:|---:|---:|---:|---:|
| 1 | long | book | book | 5 | close | 1w | none | **40.0** | 118.1 | 0.571 | 196 | 46 |
| 2 | long | book | book | 5 | close | 3d | none | **39.0** | 37.4 | 0.556 | 207 | 48 |
| 3 | long | book | book | 5 | close | 1w | score | **38.2** | 38.8 | 0.642 | 106 | 22 |
| 4 | long | book | book | 5 | close | 3d | score | **37.1** | 45.0 | 0.613 | 111 | 23 |
| 5 | long | book | book | 10 | close | 1w | none | **34.7** | 100.2 | 0.544 | 338 | 47 |
| 6 | long | book | book | 10 | close | 3d | none | **32.3** | 37.9 | 0.542 | 356 | 49 |
| 7 | long | book | book | 15 | close | 1w | none | **30.1** | 93.4 | 0.539 | 421 | 47 |
| 8 | long | book | book | 10 | close | 3d | score | **29.2** | 47.3 | 0.569 | 204 | 24 |
| 9 | long | book | book | 15 | close | 3d | none | **28.9** | 23.0 | 0.528 | 445 | 49 |
| 10 | long | book | book | 15 | close | 1w | score | **28.6** | 28.5 | 0.581 | 267 | 23 |
| 11 | long | book | book | 15 | close | 3d | score | **26.6** | 31.1 | 0.544 | 283 | 24 |
| 12 | long | book | book | 10 | close | 1w | score | **17.1** | 34.0 | 0.593 | 194 | 23 |
| 13 | long | book | book | 5 | close | 1d | none | **14.6** | 24.9 | 0.509 | 212 | 50 |
| 14 | long | book | book | 5 | close | 1d | score | **6.4** | 8.8 | 0.518 | 114 | 24 |
| 15 | long | book | book | 15 | close | 1d | none | **5.8** | 12.2 | 0.493 | 456 | 51 |
| 16 | long | book | book | 10 | close | 1d | none | **2.6** | 17.5 | 0.477 | 363 | 51 |
| 17 | long | book | book | 15 | close | 1d | score | **0.6** | 3.3 | 0.493 | 292 | 25 |
| 18 | long | book | book | 10 | close | 1d | score | **-1.6** | 9.1 | 0.474 | 209 | 25 |

