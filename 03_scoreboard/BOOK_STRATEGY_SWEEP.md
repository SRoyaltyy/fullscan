# Stock-book paper trading — strategy sweep (daily re-rank)

Every lever combo re-scored on the latest payload. **Sorted by trimmed compound** — the 2 best and 2 worst trades are dropped before compounding, so one lottery winner cannot put a combo on top. `raw` is the untrimmed number (watch the gap: big gap = lottery-driven). `dip` rank pairs only with close entry (same-day change is only knowable at 16:00 ET). Gate = morning predict score >= 1.0 (missing = allowed). 0.15% round-trip fee drag.

| # | Side | Filter | Rank | N | Entry | Hold | Gate | Trimmed % | Raw % | Hit | Trades | Days |
|---:|---|---|---|---:|---|---|---|---:|---:|---:|---:|---:|
| 1 | long | book | book | 10 | close | 3d | none | **7.8** | 21.6 | 0.52 | 102 | 11 |
| 2 | long | book | book | 10 | close | 3d | score | **6.1** | 19.7 | 0.538 | 52 | 6 |
| 3 | long | book | book | 5 | close | 3d | none | **5.8** | 3.4 | 0.52 | 50 | 10 |
| 4 | long | book | book | 15 | close | 3d | none | **4.8** | 8.7 | 0.486 | 140 | 11 |
| 5 | long | book | book | 5 | close | 3d | score | **3.9** | 1.6 | 0.52 | 25 | 5 |
| 6 | long | book | book | 15 | close | 3d | score | **2.9** | 6.8 | 0.487 | 80 | 6 |
| 7 | long | book | book | 15 | close | 1w | score | **0.9** | 5.1 | 0.512 | 80 | 6 |
| 8 | long | book | book | 5 | close | 1w | score | **0.1** | -2.8 | 0.56 | 25 | 5 |
| 9 | long | book | book | 15 | close | 1w | none | **-1.3** | 2.9 | 0.45 | 120 | 9 |
| 10 | long | book | book | 5 | close | 1d | score | **-1.4** | -1.9 | 0.467 | 30 | 6 |
| 11 | long | book | book | 15 | close | 1d | score | **-2.1** | 0.1 | 0.453 | 95 | 7 |
| 12 | long | book | book | 15 | close | 1d | none | **-2.6** | -0.4 | 0.466 | 163 | 13 |
| 13 | long | book | book | 10 | close | 1d | score | **-3.3** | 6.4 | 0.452 | 62 | 7 |
| 14 | long | book | book | 5 | close | 1d | none | **-3.4** | -3.5 | 0.467 | 60 | 12 |
| 15 | long | book | book | 5 | close | 1w | none | **-3.5** | -6.2 | 0.425 | 40 | 8 |
| 16 | long | book | book | 10 | close | 1w | score | **-3.5** | 12.4 | 0.558 | 52 | 6 |
| 17 | long | book | book | 10 | close | 1d | none | **-5.1** | 4.9 | 0.45 | 120 | 13 |
| 18 | long | book | book | 10 | close | 1w | none | **-6.3** | 9.3 | 0.476 | 82 | 9 |

