# Stock-book paper trading — strategy sweep (daily re-rank)

Every lever combo re-scored on the latest payload. **Sorted by trimmed compound** — the 2 best and 2 worst trades are dropped before compounding, so one lottery winner cannot put a combo on top. `raw` is the untrimmed number (watch the gap: big gap = lottery-driven). `dip` rank pairs only with close entry (same-day change is only knowable at 16:00 ET). Gate = morning predict score >= 1.0 (missing = allowed). 0.15% round-trip fee drag.

| # | Side | Filter | Rank | N | Entry | Hold | Gate | Trimmed % | Raw % | Hit | Trades | Days |
|---:|---|---|---|---:|---|---|---|---:|---:|---:|---:|---:|
| 1 | long | book | book | 5 | close | 3d | none | **46.6** | 44.9 | 0.552 | 210 | 49 |
| 2 | long | book | book | 5 | close | 3d | score | **44.6** | 52.9 | 0.605 | 114 | 24 |
| 3 | long | book | book | 5 | close | 1w | score | **36.3** | 36.8 | 0.636 | 107 | 22 |
| 4 | long | book | book | 5 | close | 1w | none | **33.3** | 107.6 | 0.562 | 201 | 47 |
| 5 | long | book | book | 10 | close | 3d | none | **27.4** | 32.8 | 0.531 | 367 | 50 |
| 6 | long | book | book | 10 | close | 3d | score | **25.5** | 41.9 | 0.549 | 215 | 25 |
| 7 | long | book | book | 15 | close | 3d | none | **25.0** | 19.1 | 0.515 | 462 | 50 |
| 8 | long | book | book | 15 | close | 1w | score | **23.6** | 23.5 | 0.565 | 276 | 23 |
| 9 | long | book | book | 10 | close | 1w | none | **23.4** | 83.4 | 0.53 | 349 | 48 |
| 10 | long | book | book | 15 | close | 3d | score | **21.5** | 27.1 | 0.523 | 300 | 25 |
| 11 | long | book | book | 15 | close | 1w | none | **18.4** | 76.0 | 0.525 | 436 | 48 |
| 12 | long | book | book | 5 | close | 1d | none | **13.5** | 23.7 | 0.502 | 217 | 51 |
| 13 | long | book | book | 10 | close | 1w | score | **13.3** | 29.6 | 0.578 | 199 | 23 |
| 14 | long | book | book | 5 | close | 1d | score | **6.4** | 8.8 | 0.518 | 114 | 24 |
| 15 | long | book | book | 15 | close | 1d | none | **3.8** | 10.0 | 0.486 | 473 | 52 |
| 16 | long | book | book | 10 | close | 1d | none | **0.7** | 15.4 | 0.471 | 378 | 52 |
| 17 | long | book | book | 15 | close | 1d | score | **-1.0** | 1.3 | 0.485 | 301 | 25 |
| 18 | long | book | book | 10 | close | 1d | score | **-3.3** | 7.2 | 0.468 | 216 | 25 |

