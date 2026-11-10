# Stock-book paper trading — strategy sweep (daily re-rank)

Every lever combo re-scored on the latest payload. **Sorted by trimmed compound** — the 2 best and 2 worst trades are dropped before compounding, so one lottery winner cannot put a combo on top. `raw` is the untrimmed number (watch the gap: big gap = lottery-driven). `dip` rank pairs only with close entry (same-day change is only knowable at 16:00 ET). Gate = morning predict score >= 1.0 (missing = allowed). 0.15% round-trip fee drag.

| # | Side | Filter | Rank | N | Entry | Hold | Gate | Trimmed % | Raw % | Hit | Trades | Days |
|---:|---|---|---|---:|---|---|---|---:|---:|---:|---:|---:|
| 1 | long | book | book | 5 | close | 1w | score | **51.2** | 58.1 | 0.647 | 116 | 24 |
| 2 | long | book | book | 5 | close | 1w | none | **40.6** | 76.0 | 0.565 | 216 | 51 |
| 3 | long | book | book | 5 | close | 3d | score | **31.4** | 38.6 | 0.595 | 121 | 25 |
| 4 | long | book | book | 15 | close | 1w | score | **25.3** | 26.3 | 0.553 | 309 | 25 |
| 5 | long | book | book | 5 | close | 3d | none | **22.1** | 35.1 | 0.555 | 227 | 53 |
| 6 | long | book | book | 10 | close | 1w | none | **20.1** | 46.8 | 0.526 | 382 | 52 |
| 7 | long | book | book | 10 | close | 3d | score | **18.1** | 35.6 | 0.543 | 232 | 26 |
| 8 | long | book | book | 15 | close | 3d | score | **17.1** | 22.8 | 0.522 | 324 | 26 |
| 9 | long | book | book | 10 | close | 1w | score | **16.9** | 35.0 | 0.572 | 222 | 25 |
| 10 | long | book | book | 10 | close | 3d | none | **14.9** | 31.7 | 0.535 | 398 | 54 |
| 11 | long | book | book | 15 | close | 3d | none | **14.5** | 19.4 | 0.52 | 500 | 54 |
| 12 | long | book | book | 15 | close | 1w | none | **13.2** | 38.3 | 0.518 | 479 | 52 |
| 13 | long | book | book | 5 | close | 1d | none | **9.6** | 19.3 | 0.508 | 238 | 55 |
| 14 | long | book | book | 5 | close | 1d | score | **3.7** | 5.2 | 0.52 | 125 | 26 |
| 15 | long | book | book | 15 | close | 1d | none | **-0.8** | 5.2 | 0.482 | 521 | 56 |
| 16 | long | book | book | 10 | close | 1d | none | **-3.5** | 10.8 | 0.468 | 417 | 56 |
| 17 | long | book | book | 15 | close | 1d | score | **-4.9** | -2.7 | 0.478 | 335 | 27 |
| 18 | long | book | book | 10 | close | 1d | score | **-6.5** | 3.5 | 0.461 | 241 | 27 |

