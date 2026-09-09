# Mover paper — empty list + skip days defer to live .io 2w_size — strategy sweep (daily re-rank)

Every lever combo re-scored on the latest payload. **Sorted by trimmed compound** — the 2 best and 2 worst trades are dropped before compounding, so one lottery winner cannot put a combo on top. `raw` is the untrimmed number (watch the gap: big gap = lottery-driven). `dip` rank pairs only with close entry (same-day change is only knowable at 16:00 ET). Gate = morning predict score >= 1.0 (missing = allowed). 0.15% round-trip fee drag.

| # | Side | Filter | Rank | N | Entry | Hold | Gate | Trimmed % | Raw % | Hit | Trades | Days |
|---:|---|---|---|---:|---|---|---|---:|---:|---:|---:|---:|
| 1 | long | all | conviction | 10 | open | 1d | none | **24.7** | 27.5 | 0.573 | 131 | 14 |
| 2 | long | tone_good | conviction | 15 | open | 1d | none | **21.7** | 23.3 | 0.602 | 181 | 13 |
| 3 | long | all | conviction | 10 | open | 3d | score | **21.2** | 21.7 | 0.634 | 41 | 5 |
| 4 | long | all | conviction | 10 | open | 3d | none | **20.9** | 19.8 | 0.532 | 111 | 12 |
| 5 | long | tone_good | conviction | 10 | open | 1d | none | **20.6** | 23.9 | 0.595 | 121 | 13 |
| 6 | long | all | conviction | 15 | open | 3d | score | **20.4** | 20.9 | 0.639 | 61 | 5 |
| 7 | long | all | conviction | 15 | open | 1d | none | **20.3** | 22.1 | 0.566 | 196 | 14 |
| 8 | long | all | conviction | 10 | open | 1d | score | **20.1** | 23.4 | 0.623 | 61 | 7 |
| 9 | long | g3 | conviction | 15 | open | 1d | none | **19.5** | 21.1 | 0.58 | 181 | 13 |
| 10 | both | all | conviction | 10 | open | 1d | none | **18.9** | 21.8 | 0.547 | 150 | 15 |
| 11 | long | g3 | conviction | 10 | open | 1d | none | **18.5** | 21.8 | 0.57 | 121 | 13 |
| 12 | long | all | conviction | 5 | open | 3d | score | **17.6** | 17.5 | 0.619 | 21 | 5 |
| 13 | long | all | conviction | 5 | open | 3d | none | **16.8** | 18.1 | 0.571 | 56 | 12 |
| 14 | long | all | conviction | 15 | open | 1d | score | **16.5** | 18.2 | 0.571 | 91 | 7 |
| 15 | long | all | conviction | 15 | open | 3d | none | **15.8** | 12.2 | 0.548 | 166 | 12 |
| 16 | both | tone_good | conviction | 15 | open | 1d | none | **15.4** | 16.9 | 0.566 | 212 | 15 |
| 17 | both | tone_good | conviction | 10 | open | 1d | none | **15.2** | 18.4 | 0.549 | 142 | 15 |
| 18 | both | all | conviction | 15 | open | 1d | none | **14.1** | 15.9 | 0.542 | 225 | 15 |
| 19 | both | all | conviction | 10 | open | 1d | score | **13.9** | 17.2 | 0.562 | 80 | 8 |
| 20 | both | g3 | conviction | 15 | open | 1d | none | **13.4** | 14.8 | 0.547 | 212 | 15 |
| 21 | long | all | cond | 10 | open | 1d | none | **13.3** | 16.2 | 0.504 | 131 | 14 |
| 22 | both | g3 | conviction | 10 | open | 1d | none | **13.3** | 16.4 | 0.528 | 142 | 15 |
| 23 | long | all | cond | 15 | open | 1d | none | **11.8** | 13.5 | 0.52 | 196 | 14 |
| 24 | long | g3 | conviction | 10 | open | 1d | score | **11.6** | 14.7 | 0.617 | 60 | 6 |
| 25 | long | tone_good | conviction | 10 | open | 1d | score | **11.6** | 14.7 | 0.617 | 60 | 6 |
| 26 | long | all | conviction | 5 | open | 1d | none | **11.2** | 13.9 | 0.5 | 66 | 14 |
| 27 | long | all | cond | 10 | open | 3d | none | **11.2** | 11.0 | 0.523 | 111 | 12 |
| 28 | long | all | conviction | 5 | open | 1d | score | **10.7** | 12.2 | 0.516 | 31 | 7 |
| 29 | long | all | conviction | 10 | open | eod | none | **10.4** | 11.3 | 0.464 | 151 | 16 |
| 30 | both | nored | conviction | 10 | open | 3d | none | **10.4** | -1.2 | 0.604 | 53 | 6 |
| 31 | long | all | cond | 15 | open | 3d | none | **10.3** | 9.0 | 0.548 | 166 | 12 |
| 32 | long | g3 | cond | 15 | open | 1d | none | **10.2** | 11.7 | 0.519 | 181 | 13 |
| 33 | long | tone_good | cond | 15 | open | 1d | none | **10.2** | 11.7 | 0.519 | 181 | 13 |
| 34 | long | all | conviction | 10 | open | eod | score | **10.1** | 11.7 | 0.606 | 71 | 8 |
| 35 | both | g3 | dip | 10 | close | 1d | none | **9.8** | 111.8 | 0.542 | 142 | 15 |
| 36 | both | g3 | conviction | 10 | open | 1d | score | **9.5** | 12.5 | 0.537 | 80 | 8 |
| 37 | both | tone_good | conviction | 10 | open | 1d | score | **9.5** | 12.5 | 0.537 | 80 | 8 |
| 38 | long | g3 | conviction | 10 | open | 3d | score | **9.2** | 9.6 | 0.625 | 40 | 4 |
| 39 | long | tone_good | conviction | 10 | open | 3d | score | **9.2** | 9.6 | 0.625 | 40 | 4 |
| 40 | long | all | conviction | 15 | open | eod | none | **9.1** | 9.1 | 0.456 | 226 | 16 |

