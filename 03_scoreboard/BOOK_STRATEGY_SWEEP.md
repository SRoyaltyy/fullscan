# Stock-book paper trading — strategy sweep (daily re-rank)

Every lever combo re-scored on the latest payload. **Sorted by trimmed compound** — the 2 best and 2 worst trades are dropped before compounding, so one lottery winner cannot put a combo on top. `raw` is the untrimmed number (watch the gap: big gap = lottery-driven). `dip` rank pairs only with close entry (same-day change is only knowable at 16:00 ET). Gate = morning predict score >= 1.0 (missing = allowed). 0.15% round-trip fee drag.

| # | Side | Filter | Rank | N | Entry | Hold | Gate | Trimmed % | Raw % | Hit | Trades | Days |
|---:|---|---|---|---:|---|---|---|---:|---:|---:|---:|---:|
| 1 | long | book | book | 5 | close | 1w | score | **41.0** | 41.8 | 0.642 | 95 | 20 |
| 2 | long | book | book | 5 | close | 3d | score | **31.4** | 35.8 | 0.62 | 100 | 21 |
| 3 | long | book | book | 15 | close | 1w | score | **30.3** | 30.2 | 0.58 | 245 | 21 |
| 4 | long | book | book | 5 | close | 1w | none | **27.1** | 26.4 | 0.556 | 178 | 40 |
| 5 | long | book | book | 10 | close | 3d | score | **22.5** | 39.6 | 0.569 | 188 | 22 |
| 6 | long | book | book | 15 | close | 3d | score | **22.3** | 26.6 | 0.542 | 260 | 22 |
| 7 | long | book | book | 15 | close | 1w | none | **20.5** | 19.3 | 0.536 | 388 | 41 |
| 8 | long | book | book | 10 | close | 1w | score | **18.8** | 35.9 | 0.593 | 177 | 21 |
| 9 | long | book | book | 5 | close | 3d | none | **15.9** | 7.4 | 0.559 | 186 | 43 |
| 10 | long | book | book | 10 | close | 3d | none | **14.5** | 15.5 | 0.546 | 324 | 44 |
| 11 | long | book | book | 15 | close | 3d | none | **14.5** | 4.9 | 0.53 | 406 | 44 |
| 12 | long | book | book | 10 | close | 1w | none | **9.1** | 23.6 | 0.542 | 310 | 41 |
| 13 | long | book | book | 5 | close | 1d | none | **7.0** | 7.0 | 0.505 | 194 | 45 |
| 14 | long | book | book | 5 | close | 1d | score | **3.8** | 6.6 | 0.509 | 106 | 22 |
| 15 | long | book | book | 15 | close | 1d | none | **0.6** | 1.8 | 0.487 | 419 | 46 |
| 16 | long | book | book | 15 | close | 1d | score | **-0.6** | 2.0 | 0.483 | 271 | 23 |
| 17 | long | book | book | 10 | close | 1d | none | **-4.1** | 5.2 | 0.47 | 334 | 46 |
| 18 | long | book | book | 10 | close | 1d | score | **-4.1** | 6.4 | 0.464 | 196 | 23 |

