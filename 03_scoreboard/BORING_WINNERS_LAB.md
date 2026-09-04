# Boring winners — scenario lab

Same overlay engine, different knobs. Hold-N locks a seat for N sessions; today's new buys only fill empties. HARD_RED rules only fire when the lattice prints `hard_red` (from 2026-08-31).

Dashboard: `dashboard/boring-winners/index.html` → https://sroyaltyy.github.io/fullscan/dashboard/boring-winners/

## Leaderboard

| sleeve | source | n | hold | color | hard_red | mean day | cum | p(loss day) | final $10k | orig 8/13–8/21 |
|---|---|---:|---:|---|---|---:|---:|---:|---:|---:|
| `mine_25_h1` | mine | 25 | 1 | all | none | +41.64 | +582.95 | 57.1% | 24,747 | 70,574 (+605.74 / 7d) |
| `overlay_10_h1_green` | overlay | 10 | 1 | green | none | -5.39 | -75.52 | 35.7% | 2,439 | 10,007 (+0.07 / 7d) |
| `overlay_10_h1` | overlay | 10 | 1 | all | none | -5.43 | -76.01 | 35.7% | 2,437 | 9,946 (-0.54 / 7d) |
| `overlay_25_h1_blue` | overlay | 25 | 1 | blue | none | -5.47 | -76.62 | 42.9% | 2,614 | 9,626 (-3.74 / 7d) |
| `overlay_25_h1_green` | overlay | 25 | 1 | green | none | -5.89 | -82.40 | 42.9% | 2,784 | 9,780 (-2.20 / 7d) |
| `overlay_50_h1` | overlay | 50 | 1 | all | none | -6.64 | -93.00 | 50.0% | 2,809 | 9,714 (-2.86 / 7d) |
| `overlay_50_h1_cut5` | overlay | 50 | 1 | all | haircut_5 | -6.64 | -93.00 | 50.0% | 2,809 | 9,714 (-2.86 / 7d) |
| `overlay_25_h1` | overlay | 25 | 1 | all | none | -6.75 | -94.45 | 42.9% | 2,716 | 9,765 (-2.35 / 7d) |
| `overlay_25_h1_stand` | overlay | 25 | 1 | all | stand_down | -6.75 | -94.45 | 42.9% | 2,716 | 9,765 (-2.35 / 7d) |
| `overlay_25_h1_cut5` | overlay | 25 | 1 | all | haircut_5 | -6.75 | -94.45 | 42.9% | 2,716 | 9,765 (-2.35 / 7d) |
| `overlay_25_h1_lim5` | overlay | 25 | 1 | all | limit_5 | -6.75 | -94.45 | 42.9% | 2,716 | 9,765 (-2.35 / 7d) |
| `book_25_h1` | book | 25 | 1 | all | none | -7.07 | -99.01 | 42.9% | 2,592 | 9,918 (-0.82 / 7d) |
| `book_25_h3` | book | 25 | 3 | all | none | -7.54 | -105.52 | 42.9% | 2,139 | 10,213 (+2.13 / 7d) |
| `overlay_10_h3` | overlay | 10 | 3 | all | none | -7.70 | -107.78 | 35.7% | 1,747 | 10,488 (+4.88 / 7d) |
| `overlay_25_h3` | overlay | 25 | 3 | all | none | -7.80 | -109.23 | 42.9% | 2,033 | 10,154 (+1.54 / 7d) |
| `overlay_25_h5` | overlay | 25 | 5 | all | none | -8.04 | -112.49 | 42.9% | 1,827 | 10,166 (+1.66 / 7d) |
| `overlay_25_h2` | overlay | 25 | 2 | all | none | -8.05 | -112.67 | 57.1% | 2,243 | 9,945 (-0.55 / 7d) |
| `overlay_25_h3_blue` | overlay | 25 | 3 | blue | none | -13.27 | -185.72 | 42.9% | 482 | 10,219 (+2.19 / 7d) |

## Original window 2026-08-13 → 2026-08-21

Live overlay 25 daily, pre-drawdown window before HARD_RED lattice (2026-08-31).

- overlay 25 daily: $10000.0 → $9764.85 (-2.35 over 7d, mean -0.34, fees $400.52)
- SPY $10k: $10000.0 → $10007.35

## Daily overlay 25 · daily (live)

| date | n held | buy | skip | sell | 1d | equity |
|---|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | 17 | 17 | 8 | 0 | -0.45 | 9,955 |
| 2026-08-14 | 17 | 17 | 8 | 17 | +0.19 | 9,974 |
| 2026-08-17 | 23 | 10 | 2 | 6 | +0.01 | 9,975 |
| 2026-08-18 | 22 | 19 | 3 | 21 | -1.60 | 9,816 |
| 2026-08-19 | 25 | 10 | 3 | 7 | +0.76 | 9,890 |
| 2026-08-20 | 16 | 16 | 1 | 22 | -1.28 | 9,763 |
| 2026-08-21 | 23 | 16 | 1 | 9 | +0.02 | 9,765 |
| 2026-08-27 | 19 | 0 | 14 | 18 | -60.97 | 3,812 |
| 2026-08-30 | 25 | 0 | 23 | 3 | -17.38 | 3,149 |
| 2026-08-31 | 12 | 0 | 12 | 2 | -13.74 | 2,716 |
| 2026-09-01 | 11 | 0 | 11 | 0 | +0.00 | 2,716 |
| 2026-09-02 | 13 | 0 | 13 | 0 | +0.00 | 2,716 |
| 2026-09-03 | 12 | 0 | 12 | 0 | +0.00 | 2,716 |
| 2026-09-04 | 16 | 0 | 16 | 0 | +0.00 | 2,716 |

Live book is still `overlay_25_h1`. The lab is how we pick the next default.

Notes: `limit_5` / `haircut_5` now require the session low to print through open×0.95. No print → limit skips, haircut fills at the close. Fills are priced from ohlc.parquet. Exact minute of the low is not in the daily bar — time is `intraday (low ≤ open×0.95)` or `16:00 ET close`.

