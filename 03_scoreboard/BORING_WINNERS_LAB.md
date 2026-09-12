# Boring winners — scenario lab

Same overlay engine, different knobs. Hold-N locks a seat for N sessions; today's new buys only fill empties. HARD_RED rules only fire when the lattice prints `hard_red` (from 2026-08-31).

Dashboard: `dashboard/boring-winners/index.html` → https://sroyaltyy.github.io/fullscan/dashboard/boring-winners/

## Leaderboard

| sleeve | source | n | hold | color | hard_red | mean day | cum | p(loss day) | final $10k | orig 8/13–8/21 |
|---|---|---:|---:|---|---|---:|---:|---:|---:|---:|
| `mine_25_h1` | mine | 25 | 1 | all | none | +21.20 | +445.19 | 81.0% | 3,160 | 70,574 (+605.74 / 7d) |
| `book_25_h3` | book | 25 | 3 | all | none | -2.49 | -52.24 | 57.1% | 5,604 | 10,213 (+2.13 / 7d) |
| `overlay_25_h5` | overlay | 25 | 5 | all | none | -2.64 | -55.50 | 57.1% | 5,173 | 10,173 (+1.73 / 7d) |
| `overlay_25_h3` | overlay | 25 | 3 | all | none | -5.67 | -119.11 | 76.2% | 2,536 | 10,160 (+1.60 / 7d) |
| `overlay_10_h3` | overlay | 10 | 3 | all | none | -5.97 | -125.48 | 61.9% | 2,165 | 10,493 (+4.93 / 7d) |
| `overlay_25_h3_blue` | overlay | 25 | 3 | blue | none | -6.53 | -137.16 | 76.2% | 1,928 | 10,223 (+2.23 / 7d) |
| `overlay_25_h1_stand` | overlay | 25 | 1 | all | stand_down | -7.96 | -167.12 | 52.4% | 919 | 9,770 (-2.30 / 7d) |
| `overlay_25_h1_lim5` | overlay | 25 | 1 | all | limit_5 | -8.05 | -168.98 | 57.1% | 898 | 9,770 (-2.30 / 7d) |
| `book_25_h1` | book | 25 | 1 | all | none | -8.75 | -183.67 | 71.4% | 641 | 9,915 (-0.85 / 7d) |
| `overlay_25_h2` | overlay | 25 | 2 | all | none | -9.37 | -196.68 | 90.5% | 833 | 9,936 (-0.64 / 7d) |
| `overlay_50_h1` | overlay | 50 | 1 | all | none | -9.51 | -199.68 | 71.4% | 643 | 9,721 (-2.79 / 7d) |
| `overlay_10_h1` | overlay | 10 | 1 | all | none | -9.61 | -201.86 | 76.2% | 558 | 9,958 (-0.42 / 7d) |
| `overlay_50_h1_cut5` | overlay | 50 | 1 | all | haircut_5 | -9.72 | -204.06 | 71.4% | 586 | 9,721 (-2.79 / 7d) |
| `overlay_25_h1` | overlay | 25 | 1 | all | none | -9.85 | -206.75 | 71.4% | 541 | 9,770 (-2.30 / 7d) |
| `overlay_25_h1_cut5` | overlay | 25 | 1 | all | haircut_5 | -9.86 | -207.13 | 71.4% | 537 | 9,770 (-2.30 / 7d) |
| `overlay_25_h1_blue` | overlay | 25 | 1 | blue | none | -11.20 | -235.25 | 81.0% | 310 | 9,632 (-3.68 / 7d) |
| `overlay_25_h1_green` | overlay | 25 | 1 | green | none | -11.78 | -247.34 | 71.4% | 52 | 9,786 (-2.14 / 7d) |
| `overlay_10_h1_green` | overlay | 10 | 1 | green | none | -11.80 | -247.75 | 71.4% | 33 | 10,019 (+0.19 / 7d) |

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
| 2026-09-09 | 7 | 6 | 7 | 6 | -29.07 | 1,385 |
| 2026-09-10 | 3 | 3 | 3 | 6 | -1.88 | 1,359 |
| 2026-09-11 | 5 | 3 | 2 | 2 | +0.24 | 1,362 |
| 2026-09-12 | 6 | 0 | 6 | 4 | -60.26 | 541 |

Live book is still `overlay_25_h1`. The lab is how we pick the next default.

Notes: `limit_5` / `haircut_5` now require the session low to print through open×0.95. No print → limit skips, haircut fills at the close. Fills are priced from ohlc.parquet. Exact minute of the low is not in the daily bar — time is `intraday (low ≤ open×0.95)` or `16:00 ET close`.

