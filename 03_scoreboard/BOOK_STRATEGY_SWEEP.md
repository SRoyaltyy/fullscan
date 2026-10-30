# Stock-book paper trading — strategy sweep (daily re-rank)

Every lever combo re-scored on the latest payload. **Sorted by trimmed compound** — the 2 best and 2 worst trades are dropped before compounding, so one lottery winner cannot put a combo on top. `raw` is the untrimmed number (watch the gap: big gap = lottery-driven). `dip` rank pairs only with close entry (same-day change is only knowable at 16:00 ET). Gate = morning predict score >= 1.0 (missing = allowed). 0.15% round-trip fee drag.

| # | Side | Filter | Rank | N | Entry | Hold | Gate | Trimmed % | Raw % | Hit | Trades | Days |
|---:|---|---|---|---:|---|---|---|---:|---:|---:|---:|---:|
| 1 | long | book | book | 5 | close | 1w | score | **40.8** | 41.4 | 0.65 | 103 | 22 |
| 2 | long | book | book | 5 | close | 1w | none | **34.8** | 97.7 | 0.571 | 189 | 45 |
| 3 | long | book | book | 5 | close | 3d | none | **31.8** | 71.5 | 0.563 | 197 | 47 |
| 4 | long | book | book | 5 | close | 3d | score | **31.0** | 35.5 | 0.617 | 107 | 22 |
| 5 | long | book | book | 15 | close | 1w | score | **29.3** | 29.2 | 0.583 | 264 | 23 |
| 6 | long | book | book | 15 | close | 1w | none | **26.7** | 85.6 | 0.544 | 410 | 46 |
| 7 | long | book | book | 10 | close | 3d | none | **26.6** | 39.0 | 0.548 | 343 | 48 |
| 8 | long | book | book | 15 | close | 3d | none | **25.2** | 25.8 | 0.531 | 429 | 48 |
| 9 | long | book | book | 10 | close | 3d | score | **21.7** | 38.8 | 0.568 | 199 | 23 |
| 10 | long | book | book | 15 | close | 3d | score | **21.0** | 25.3 | 0.542 | 275 | 23 |
| 11 | long | book | book | 10 | close | 1w | score | **18.6** | 35.7 | 0.597 | 191 | 23 |
| 12 | long | book | book | 10 | close | 1w | none | **16.2** | 93.5 | 0.55 | 327 | 46 |
| 13 | long | book | book | 5 | close | 1d | none | **10.9** | 59.8 | 0.512 | 205 | 49 |
| 14 | long | book | book | 5 | close | 1d | score | **6.4** | 8.8 | 0.518 | 114 | 24 |
| 15 | long | book | book | 15 | close | 1d | none | **3.9** | 16.2 | 0.497 | 447 | 50 |
| 16 | long | book | book | 10 | close | 1d | none | **0.8** | 21.7 | 0.48 | 354 | 50 |
| 17 | long | book | book | 15 | close | 1d | score | **0.6** | 3.3 | 0.493 | 292 | 25 |
| 18 | long | book | book | 10 | close | 1d | score | **-1.6** | 9.1 | 0.474 | 209 | 25 |

