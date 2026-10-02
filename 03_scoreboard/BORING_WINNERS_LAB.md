# Boring winners — scenario lab

Same overlay engine, different knobs. Hold-N locks a seat for N sessions; today's new buys only fill empties. HARD_RED rules only fire when the lattice prints `hard_red` (from 2026-08-31).

Dashboard: `dashboard/boring-winners/index.html` → https://sroyaltyy.github.io/fullscan/dashboard/boring-winners/

## Leaderboard

| sleeve | source | n | hold | color | hard_red | mean day | cum | p(loss day) | final $10k | orig 8/13–8/21 |
|---|---|---:|---:|---|---|---:|---:|---:|---:|---:|
| `mine_25_h1` | mine | 25 | 1 | all | none | +8.64 | +336.86 | 84.6% | 840 | 70,574 (+605.74 / 7d) |
| `book_25_h3` | book | 25 | 3 | all | none | -2.52 | -98.14 | 59.0% | 3,376 | 10,213 (+2.13 / 7d) |
| `overlay_10_h3` | overlay | 10 | 3 | all | none | -3.57 | -139.10 | 64.1% | 1,991 | 10,493 (+4.93 / 7d) |
| `overlay_25_h5` | overlay | 25 | 5 | all | none | -3.70 | -144.36 | 69.2% | 1,687 | 10,173 (+1.73 / 7d) |
| `overlay_25_h3_blue` | overlay | 25 | 3 | blue | none | -3.86 | -150.68 | 79.5% | 1,961 | 10,223 (+2.23 / 7d) |
| `overlay_25_h3` | overlay | 25 | 3 | all | none | -4.00 | -156.00 | 76.9% | 1,816 | 10,160 (+1.60 / 7d) |
| `overlay_25_h1_cut5` | overlay | 25 | 1 | all | haircut_5 | -6.67 | -260.08 | 82.0% | 317 | 9,770 (-2.30 / 7d) |
| `overlay_25_h1_lim5` | overlay | 25 | 1 | all | limit_5 | -6.74 | -262.65 | 66.7% | 305 | 9,770 (-2.30 / 7d) |
| `overlay_50_h1` | overlay | 50 | 1 | all | none | -6.84 | -266.79 | 84.6% | 288 | 9,721 (-2.79 / 7d) |
| `overlay_25_h1_stand` | overlay | 25 | 1 | all | stand_down | -6.86 | -267.39 | 64.1% | 285 | 9,770 (-2.30 / 7d) |
| `overlay_25_h1_green` | overlay | 25 | 1 | green | none | -6.94 | -270.78 | 82.0% | 241 | 9,786 (-2.14 / 7d) |
| `overlay_50_h1_cut5` | overlay | 50 | 1 | all | haircut_5 | -6.99 | -272.48 | 84.6% | 268 | 9,721 (-2.79 / 7d) |
| `book_25_h1` | book | 25 | 1 | all | none | -7.01 | -273.29 | 79.5% | 228 | 9,915 (-0.85 / 7d) |
| `overlay_25_h1` | overlay | 25 | 1 | all | none | -7.02 | -273.94 | 82.0% | 262 | 9,770 (-2.30 / 7d) |
| `overlay_10_h1_green` | overlay | 10 | 1 | green | none | -7.03 | -274.06 | 82.0% | 219 | 10,019 (+0.19 / 7d) |
| `overlay_10_h1` | overlay | 10 | 1 | all | none | -7.07 | -275.80 | 87.2% | 233 | 9,958 (-0.42 / 7d) |
| `overlay_25_h2` | overlay | 25 | 2 | all | none | -7.35 | -286.49 | 89.7% | 265 | 9,936 (-0.64 / 7d) |
| `overlay_25_h1_blue` | overlay | 25 | 1 | blue | none | -7.36 | -287.11 | 87.2% | 182 | 9,632 (-3.68 / 7d) |

## Original window 2026-08-13 → 2026-08-21

Live overlay 25 daily, pre-drawdown window before HARD_RED lattice (2026-08-31).

- overlay 25 daily: $10000.0 → $9769.69 (-2.30 over 7d, mean -0.33, fees $402.34)
- SPY $10k: $10000.0 → $10007.35

## Daily overlay 25 · daily (live)

| date | n held | buy | skip | sell | 1d | equity |
|---|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | 17 | 17 | 8 | 0 | -0.45 | 9,955 |
| 2026-08-14 | 17 | 17 | 8 | 17 | +0.19 | 9,974 |
| 2026-08-17 | 23 | 10 | 2 | 6 | +0.01 | 9,975 |
| 2026-08-18 | 22 | 19 | 3 | 21 | -1.60 | 9,816 |
| 2026-08-19 | 25 | 10 | 3 | 7 | +0.76 | 9,890 |
| 2026-08-20 | 17 | 17 | 0 | 22 | -1.30 | 9,761 |
| 2026-08-21 | 24 | 16 | 0 | 9 | +0.09 | 9,770 |
| 2026-08-27 | 19 | 14 | 0 | 19 | -1.13 | 9,659 |
| 2026-08-30 | 25 | 0 | 20 | 14 | -48.08 | 5,015 |
| 2026-08-31 | 12 | 9 | 3 | 5 | -0.69 | 4,981 |
| 2026-09-01 | 11 | 10 | 0 | 8 | -1.01 | 4,930 |
| 2026-09-02 | 13 | 11 | 1 | 10 | -0.46 | 4,908 |
| 2026-09-03 | 12 | 10 | 1 | 11 | -1.32 | 4,843 |
| 2026-09-04 | 16 | 15 | 1 | 11 | -1.35 | 4,778 |
| 2026-09-06 | 12 | 0 | 12 | 15 | -58.91 | 1,963 |
| 2026-09-07 | 11 | 0 | 11 | 0 | +0.00 | 1,963 |
| 2026-09-08 | 6 | 6 | 0 | 0 | -0.54 | 1,953 |
| 2026-09-09 | 13 | 11 | 2 | 6 | -3.04 | 1,894 |
| 2026-09-10 | 6 | 6 | 0 | 11 | -1.96 | 1,856 |
| 2026-09-11 | 6 | 4 | 1 | 5 | -0.62 | 1,845 |
| 2026-09-12 | 6 | 0 | 6 | 5 | -56.42 | 804 |
| 2026-09-13 | 10 | 0 | 10 | 0 | +0.00 | 804 |
| 2026-09-14 | 5 | 3 | 2 | 0 | -0.43 | 801 |
| 2026-09-15 | 12 | 7 | 5 | 3 | +0.11 | 801 |
| 2026-09-16 | 12 | 5 | 6 | 5 | -0.79 | 795 |
| 2026-09-17 | 17 | 9 | 8 | 7 | -1.05 | 787 |
| 2026-09-18 | 14 | 4 | 8 | 7 | -1.21 | 777 |
| 2026-09-20 | 13 | 0 | 13 | 6 | -29.68 | 547 |
| 2026-09-21 | 12 | 5 | 7 | 0 | -0.30 | 545 |
| 2026-09-22 | 9 | 5 | 4 | 5 | -0.18 | 544 |
| 2026-09-23 | 15 | 10 | 4 | 4 | -1.19 | 537 |
| 2026-09-24 | 9 | 5 | 4 | 11 | -0.57 | 534 |
| 2026-09-25 | 20 | 7 | 13 | 5 | -1.82 | 525 |
| 2026-09-27 | 11 | 0 | 11 | 7 | -24.39 | 397 |
| 2026-09-28 | 11 | 3 | 8 | 0 | -0.17 | 396 |
| 2026-09-29 | 22 | 6 | 15 | 2 | -0.35 | 395 |
| 2026-09-30 | 10 | 6 | 4 | 7 | -0.69 | 392 |
| 2026-10-01 | 7 | 3 | 4 | 6 | -0.78 | 389 |
| 2026-10-02 | 19 | 0 | 19 | 3 | -32.62 | 262 |

Live book is still `overlay_25_h1`. The lab is how we pick the next default.

Notes: `limit_5` / `haircut_5` now require the session low to print through open×0.95. No print → limit skips, haircut fills at the close. Fills are priced from ohlc.parquet. Exact minute of the low is not in the daily bar — time is `intraday (low ≤ open×0.95)` or `16:00 ET close`.

