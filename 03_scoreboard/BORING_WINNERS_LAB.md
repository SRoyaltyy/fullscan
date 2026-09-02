# Boring winners — scenario lab

Same overlay engine, different knobs. Hold-N locks a seat for N sessions; today's new buys only fill empties. HARD_RED rules only fire when the lattice prints `hard_red` (from 2026-08-31).

Dashboard: `dashboard/boring-winners/index.html` → https://sroyaltyy.github.io/fullscan/dashboard/boring-winners/

## Leaderboard

| sleeve | source | n | hold | color | hard_red | mean day | cum | p(loss day) | final $10k |
|---|---|---:|---:|---|---|---:|---:|---:|---:|
| `overlay_25_h1_cut5` | overlay | 25 | 1 | all | haircut_5 | +1.03 | +11.37 | 36.4% | 11,178 |
| `overlay_50_h1_cut5` | overlay | 50 | 1 | all | haircut_5 | +0.96 | +10.59 | 36.4% | 11,094 |
| `book_25_h3` | book | 25 | 3 | all | none | +0.46 | +5.02 | 45.5% | 10,505 |
| `overlay_25_h3` | overlay | 25 | 3 | all | none | +0.45 | +4.92 | 45.5% | 10,495 |
| `overlay_25_h5` | overlay | 25 | 5 | all | none | +0.41 | +4.50 | 45.5% | 10,443 |
| `overlay_10_h3` | overlay | 10 | 3 | all | none | +0.40 | +4.35 | 54.5% | 10,434 |
| `overlay_25_h1_stand` | overlay | 25 | 1 | all | stand_down | +0.35 | +3.17 | 44.4% | 10,315 |
| `mine_25_h1` | mine | 25 | 1 | all | none | +0.29 | +3.23 | 54.5% | 10,316 |
| `overlay_25_h2` | overlay | 25 | 2 | all | none | +0.28 | +3.10 | 45.5% | 10,308 |
| `overlay_25_h1_green` | overlay | 25 | 1 | green | none | +0.28 | +3.08 | 54.5% | 10,302 |
| `book_25_h1` | book | 25 | 1 | all | none | +0.20 | +2.21 | 45.5% | 10,215 |
| `overlay_25_h1` | overlay | 25 | 1 | all | none | +0.17 | +1.83 | 54.5% | 10,177 |
| `overlay_10_h1_green` | overlay | 10 | 1 | green | none | +0.10 | +1.09 | 54.5% | 10,096 |
| `overlay_50_h1` | overlay | 50 | 1 | all | none | +0.10 | +1.05 | 54.5% | 10,098 |
| `overlay_25_h3_blue` | overlay | 25 | 3 | blue | none | -0.07 | -0.68 | 60.0% | 9,928 |
| `overlay_10_h1` | overlay | 10 | 1 | all | none | -0.10 | -1.15 | 45.5% | 9,879 |
| `overlay_25_h1_lim5` | overlay | 25 | 1 | all | limit_5 | -0.55 | -5.50 | 50.0% | 9,420 |
| `overlay_25_h1_blue` | overlay | 25 | 1 | blue | none | -0.70 | -6.27 | 55.6% | 9,383 |

## Daily overlay 25 · daily (live)

| date | n held | buy | skip | sell | 1d | equity |
|---|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | 25 | 25 | 0 | 0 | +2.00 | 10,200 |
| 2026-08-14 | 25 | 18 | 0 | 18 | +0.52 | 10,253 |
| 2026-08-17 | 23 | 12 | 0 | 14 | -1.56 | 10,093 |
| 2026-08-18 | 22 | 22 | 0 | 23 | +2.32 | 10,328 |
| 2026-08-19 | 25 | 10 | 0 | 7 | -0.64 | 10,261 |
| 2026-08-20 | 17 | 17 | 0 | 25 | +0.93 | 10,357 |
| 2026-08-21 | 24 | 16 | 0 | 9 | -0.41 | 10,314 |
| 2026-08-27 | 19 | 14 | 0 | 19 | -0.13 | 10,301 |
| 2026-08-30 | 25 | 20 | 0 | 14 | +0.14 | 10,315 |
| 2026-08-31 | 12 | 12 | 0 | 25 | -0.33 | 10,281 |
| 2026-09-01 | 11 | 10 | 0 | 11 | -1.01 | 10,177 |

Live book is still `overlay_25_h1`. The lab is how we pick the next default.

Notes: `haircut_5` assumes we actually got 5% cheaper on every new HARD_RED buy — that is an entry model, not a print. `hold 3d` is the more honest lift (less churn in the late-August grind). `blue only` lost because this window's 🔵 names were the energy tape.

