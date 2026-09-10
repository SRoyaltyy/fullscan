# Boring winners — scenario lab

Same overlay engine, different knobs. Hold-N locks a seat for N sessions; today's new buys only fill empties. HARD_RED rules only fire when the lattice prints `hard_red` (from 2026-08-31).

Dashboard: `dashboard/boring-winners/index.html` → https://sroyaltyy.github.io/fullscan/dashboard/boring-winners/

## Leaderboard

| sleeve | source | n | hold | color | hard_red | mean day | cum | p(loss day) | final $10k | orig 8/13–8/21 |
|---|---|---:|---:|---|---|---:|---:|---:|---:|---:|
| `mine_25_h1` | mine | 25 | 1 | all | none | +20.03 | +380.51 | 79.0% | 337 | 70,574 (+605.74 / 7d) |
| `book_25_h3` | book | 25 | 3 | all | none | -2.65 | -50.37 | 52.6% | 5,712 | 10,213 (+2.13 / 7d) |
| `overlay_25_h3` | overlay | 25 | 3 | all | none | -2.85 | -54.19 | 63.2% | 5,515 | 10,160 (+1.60 / 7d) |
| `overlay_25_h3_blue` | overlay | 25 | 3 | blue | none | -2.86 | -54.29 | 63.2% | 5,569 | 10,223 (+2.23 / 7d) |
| `overlay_25_h5` | overlay | 25 | 5 | all | none | -2.92 | -55.42 | 52.6% | 5,188 | 10,173 (+1.73 / 7d) |
| `overlay_10_h3` | overlay | 10 | 3 | all | none | -3.08 | -58.50 | 47.4% | 4,857 | 10,493 (+4.93 / 7d) |
| `overlay_25_h1_stand` | overlay | 25 | 1 | all | stand_down | -5.94 | -112.85 | 47.4% | 2,001 | 9,770 (-2.30 / 7d) |
| `overlay_25_h1_lim5` | overlay | 25 | 1 | all | limit_5 | -6.01 | -114.13 | 52.6% | 1,980 | 9,770 (-2.30 / 7d) |
| `overlay_10_h1` | overlay | 10 | 1 | all | none | -8.98 | -170.57 | 73.7% | 810 | 9,958 (-0.42 / 7d) |
| `overlay_25_h2` | overlay | 25 | 2 | all | none | -9.21 | -174.97 | 84.2% | 1,033 | 9,936 (-0.64 / 7d) |
| `overlay_25_h1` | overlay | 25 | 1 | all | none | -9.36 | -177.85 | 68.4% | 741 | 9,770 (-2.30 / 7d) |
| `overlay_25_h1_cut5` | overlay | 25 | 1 | all | haircut_5 | -9.37 | -178.11 | 68.4% | 737 | 9,770 (-2.30 / 7d) |
| `overlay_50_h1_cut5` | overlay | 50 | 1 | all | haircut_5 | -9.39 | -178.50 | 68.4% | 735 | 9,721 (-2.79 / 7d) |
| `overlay_50_h1` | overlay | 50 | 1 | all | none | -9.42 | -179.02 | 68.4% | 728 | 9,721 (-2.79 / 7d) |
| `overlay_10_h1_green` | overlay | 10 | 1 | green | none | -9.54 | -181.26 | 68.4% | 650 | 10,019 (+0.19 / 7d) |
| `overlay_25_h1_green` | overlay | 25 | 1 | green | none | -9.59 | -182.14 | 68.4% | 667 | 9,786 (-2.14 / 7d) |
| `overlay_25_h1_blue` | overlay | 25 | 1 | blue | none | -10.30 | -195.67 | 79.0% | 500 | 9,632 (-3.68 / 7d) |
| `book_25_h1` | book | 25 | 1 | all | none | -11.23 | -213.46 | 68.4% | 15 | 9,915 (-0.85 / 7d) |

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
| 2026-09-09 | 0 | 0 | 13 | 6 | -62.07 | 741 |
| 2026-09-10 | 6 | 0 | 6 | 0 | +0.00 | 741 |

Live book is still `overlay_25_h1`. The lab is how we pick the next default.

Notes: `limit_5` / `haircut_5` now require the session low to print through open×0.95. No print → limit skips, haircut fills at the close. Fills are priced from ohlc.parquet. Exact minute of the low is not in the daily bar — time is `intraday (low ≤ open×0.95)` or `16:00 ET close`.

