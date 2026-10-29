# Stock-book paper trading — strategy sweep (daily re-rank)

Every lever combo re-scored on the latest payload. **Sorted by trimmed compound** — the 2 best and 2 worst trades are dropped before compounding, so one lottery winner cannot put a combo on top. `raw` is the untrimmed number (watch the gap: big gap = lottery-driven). `dip` rank pairs only with close entry (same-day change is only knowable at 16:00 ET). Gate = morning predict score >= 1.0 (missing = allowed). 0.15% round-trip fee drag.

| # | Side | Filter | Rank | N | Entry | Hold | Gate | Trimmed % | Raw % | Hit | Trades | Days |
|---:|---|---|---|---:|---|---|---|---:|---:|---:|---:|---:|
| 1 | long | book | book | 5 | close | 1w | score | **40.8** | 41.4 | 0.65 | 103 | 22 |
| 2 | long | book | book | 5 | close | 1w | none | **32.1** | 93.7 | 0.569 | 188 | 44 |
| 3 | long | book | book | 5 | close | 3d | score | **31.0** | 35.5 | 0.617 | 107 | 22 |
| 4 | long | book | book | 5 | close | 3d | none | **30.9** | 70.3 | 0.564 | 195 | 46 |
| 5 | long | book | book | 15 | close | 1w | score | **29.3** | 29.2 | 0.583 | 264 | 23 |
| 6 | long | book | book | 10 | close | 3d | none | **26.8** | 39.1 | 0.55 | 340 | 47 |
| 7 | long | book | book | 15 | close | 3d | none | **25.4** | 25.9 | 0.533 | 426 | 47 |
| 8 | long | book | book | 15 | close | 1w | none | **24.1** | 81.9 | 0.543 | 409 | 45 |
| 9 | long | book | book | 10 | close | 3d | score | **21.7** | 38.8 | 0.568 | 199 | 23 |
| 10 | long | book | book | 15 | close | 3d | score | **21.0** | 25.3 | 0.542 | 275 | 23 |
| 11 | long | book | book | 10 | close | 1w | score | **18.6** | 35.7 | 0.597 | 191 | 23 |
| 12 | long | book | book | 10 | close | 1w | none | **13.9** | 89.6 | 0.549 | 326 | 45 |
| 13 | long | book | book | 5 | close | 1d | none | **9.9** | 58.4 | 0.51 | 202 | 48 |
| 14 | long | book | book | 5 | close | 1d | score | **5.5** | 7.8 | 0.514 | 111 | 23 |
| 15 | long | book | book | 15 | close | 1d | none | **3.2** | 15.4 | 0.495 | 440 | 49 |
| 16 | long | book | book | 10 | close | 1d | none | **-0.1** | 20.7 | 0.479 | 351 | 49 |
| 17 | long | book | book | 15 | close | 1d | score | **-0.1** | 2.5 | 0.491 | 285 | 24 |
| 18 | long | book | book | 10 | close | 1d | score | **-2.5** | 8.2 | 0.471 | 206 | 24 |

