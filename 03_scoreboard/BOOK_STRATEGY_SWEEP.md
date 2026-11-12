# Stock-book paper trading — strategy sweep (daily re-rank)

Every lever combo re-scored on the latest payload. **Sorted by trimmed compound** — the 2 best and 2 worst trades are dropped before compounding, so one lottery winner cannot put a combo on top. `raw` is the untrimmed number (watch the gap: big gap = lottery-driven). `dip` rank pairs only with close entry (same-day change is only knowable at 16:00 ET). Gate = morning predict score >= 1.0 (missing = allowed). 0.15% round-trip fee drag.

| # | Side | Filter | Rank | N | Entry | Hold | Gate | Trimmed % | Raw % | Hit | Trades | Days |
|---:|---|---|---|---:|---|---|---|---:|---:|---:|---:|---:|
| 1 | long | book | book | 5 | close | 1w | score | **55.5** | 61.2 | 0.64 | 125 | 26 |
| 2 | long | book | book | 5 | close | 3d | score | **32.0** | 39.2 | 0.592 | 130 | 27 |
| 3 | long | book | book | 15 | close | 1w | score | **24.8** | 27.3 | 0.552 | 335 | 27 |
| 4 | long | book | book | 5 | close | 3d | none | **23.2** | 36.3 | 0.556 | 241 | 56 |
| 5 | long | book | book | 10 | close | 3d | score | **19.9** | 37.7 | 0.55 | 249 | 28 |
| 6 | long | book | book | 10 | close | 1w | score | **19.7** | 38.8 | 0.577 | 241 | 27 |
| 7 | long | book | book | 15 | close | 3d | score | **18.6** | 24.3 | 0.525 | 343 | 28 |
| 8 | long | book | book | 10 | close | 3d | none | **16.3** | 33.3 | 0.537 | 423 | 57 |
| 9 | long | book | book | 15 | close | 3d | none | **15.7** | 20.6 | 0.52 | 527 | 57 |
| 10 | long | book | book | 5 | close | 1d | none | **14.7** | 24.8 | 0.526 | 253 | 58 |
| 11 | long | book | book | 5 | close | 1w | none | **10.4** | 68.2 | 0.561 | 230 | 54 |
| 12 | long | book | book | 5 | close | 1d | score | **8.5** | 10.0 | 0.55 | 140 | 29 |
| 13 | long | book | book | 15 | close | 1d | none | **2.9** | 9.2 | 0.494 | 551 | 59 |
| 14 | long | book | book | 10 | close | 1d | none | **0.5** | 15.4 | 0.484 | 442 | 59 |
| 15 | long | book | book | 15 | close | 1d | score | **-1.3** | 1.0 | 0.496 | 365 | 30 |
| 16 | long | book | book | 10 | close | 1d | score | **-2.7** | 7.8 | 0.489 | 266 | 30 |
| 17 | long | book | book | 10 | close | 1w | none | **-4.9** | 41.4 | 0.53 | 406 | 55 |
| 18 | long | book | book | 15 | close | 1w | none | **-12.2** | 30.7 | 0.518 | 510 | 55 |

