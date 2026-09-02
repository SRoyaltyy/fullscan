# Camera combo mine

_Generated 2026-09-01T22:52:53.406517-04:00 · as-of 09:30 ET_

Liquid universe **2656** names (mcap > $100M, adv > 500k). **31821** printed name-days (≥3 cameras + a forward). Sessions 2026-08-13 → 2026-08-31.

Win = close-to-close return > 0. Mean = arithmetic average of those returns. Lift = combo minus the all-name base rate on the same horizon. Excess = name minus that session's universe median. `sess` means the light is mostly the morning market essay, not a stock-specific camera — treat those as weather filters.

Min n = 80 (lag / rare buckets 40). Combos are AND of green or not-red on join, sector, gen, ab, peer, vol, heat. Same-day Finviz and same-day book never color a cell.

## Base rate

| horizon | n | hit | mean | median | mean xs |
|---|---:|---:|---:|---:|---:|
| 1d | 29169 | 45.1% | -0.02 | -0.17 | +0.17 |
| 2d | 26513 | 43.0% | -0.24 | -0.37 | +0.14 |
| 3d | 23859 | 45.2% | +0.02 | -0.31 | +0.39 |
| 1w | 21205 | 45.2% | +0.15 | -0.37 | +0.56 |
| 2w | 0 | — | — | — | — |

## Zero reds — is a clean card better?

| pattern | n | 1d hit | hit lift | 1d mean | mean lift | 1d xs | family |
|---|---:|---:|---:|---:|---:|---:|---|
| `zero_red` | 3662 | 45.4% | 0.2% | +0.70 | +0.72 | +0.75 | zero_red |
| `has_red` | 25507 | 45.1% | -0.0% | -0.12 | -0.10 | +0.08 | zero_red |
| `n_red=0` | 3662 | 45.4% | 0.2% | +0.70 | +0.72 | +0.75 | zero_red |
| `n_red=1` | 5709 | 42.9% | -2.2% | -0.38 | -0.37 | -0.03 | zero_red |
| `n_red=2` | 6323 | 47.3% | 2.1% | +0.03 | +0.05 | +0.24 | zero_red |
| `n_red=3+` | 13475 | 45.0% | -0.1% | -0.08 | -0.06 | +0.06 | zero_red |
| `zero_red@D0 & zero_red@D-1` | 819 | 30.8% | -14.4% | -1.09 | -1.07 | -0.45 | zero_red |
| `zero_red@D0 & join@D0=good` | 3188 | 46.6% | 1.5% | +1.02 | +1.03 | +1.02 | zero_red |
| `has_red@D0 & join@D0=good` | 10298 | 43.8% | -1.3% | -0.29 | -0.28 | -0.03 | zero_red |

## 1d — highest win rate

| pattern | n | 1d hit | hit lift | 1d mean | mean lift | 1d xs | family |
|---|---:|---:|---:|---:|---:|---:|---|
| `gen@D0=good & gen@D-1=bad` · sess | 2652 | 65.3% | 20.1% | +1.14 | +1.15 | +0.42 | lag |
| `gen@D0=bad & gen@D-1=good` · sess | 5303 | 61.0% | 15.9% | +1.03 | +1.05 | +0.43 | lag |
| `sect@D0=good & gen@D0=good & heat@D0=good` · sess | 738 | 60.8% | 15.7% | +1.28 | +1.30 | +1.43 | combo3 |
| `join@D0=good & gen@D-1=bad` · sess | 1518 | 60.2% | 15.1% | +0.90 | +0.91 | +0.49 | lag_cross |
| `sect@D0=good & gen@D0=good & heat@D0=not_red` · sess | 784 | 59.4% | 14.3% | +1.19 | +1.21 | +1.34 | combo3 |
| `gen@D0=good & peer@D0=not_red & vol@D0=not_red` · sess | 864 | 59.1% | 14.0% | +0.80 | +0.82 | +0.49 | combo3 |
| `gen@D0=good & peer@D0=not_red & vol@D0=good` · sess | 202 | 58.4% | 13.3% | +1.50 | +1.51 | +1.18 | combo3 |
| `gen@D0=good & peer@D0=good & vol@D0=not_red` · sess | 746 | 58.3% | 13.2% | +0.86 | +0.87 | +0.54 | combo3 |
| `gen@D0=good & peer@D0=good & vol@D0=good` · sess | 191 | 57.1% | 11.9% | +1.50 | +1.52 | +1.19 | combo3 |
| `sect@D0=not_red & gen@D0=good & peer@D0=good` · sess | 1890 | 56.0% | 10.9% | +0.53 | +0.55 | +0.40 | combo3 |
| `gen@D0=good & AB@D0=good & vol@D0=not_red` · sess | 1285 | 55.7% | 10.6% | +0.49 | +0.50 | +0.17 | combo3 |
| `sect@D0=not_red & gen@D0=good & peer@D0=not_red` · sess | 2327 | 55.6% | 10.5% | +0.46 | +0.47 | +0.33 | combo3 |
| `gen@D0=good & AB@D0=not_red & vol@D0=not_red` · sess | 1400 | 55.1% | 10.0% | +0.50 | +0.52 | +0.19 | combo3 |
| `sect@D0=not_red & gen@D0=good & AB@D0=good` · sess | 3685 | 54.6% | 9.5% | +0.41 | +0.42 | +0.29 | combo3 |
| `sect@D0=good & sect@D-1=bad` | 2099 | 54.3% | 9.2% | +1.06 | +1.07 | +1.20 | lag |
| `sect@D0=not_red & gen@D0=not_red & peer@D0=good` · sess | 2074 | 54.0% | 8.9% | +0.35 | +0.37 | +0.29 | combo3 |
| `sect@D0=not_red & gen@D0=good & AB@D0=not_red` · sess | 3947 | 54.0% | 8.9% | +0.40 | +0.42 | +0.28 | combo3 |
| `gen@D0=good & peer@D0=good` · sess | 2879 | 54.0% | 8.9% | +0.41 | +0.42 | +0.28 | combo2 |
| `gen@D0=good & AB@D0=good & peer@D0=good` · sess | 2337 | 53.8% | 8.7% | +0.34 | +0.35 | +0.21 | combo3 |
| `gen@D0=good & AB@D0=not_red & peer@D0=good` · sess | 2425 | 53.8% | 8.6% | +0.34 | +0.35 | +0.21 | combo3 |

## 1d — highest average return

| pattern | n | 1d hit | hit lift | 1d mean | mean lift | 1d xs | family |
|---|---:|---:|---:|---:|---:|---:|---|
| `AB@D0=good & peer@D0=good & vol@D0=good` | 273 | 50.2% | 5.0% | +3.83 | +3.84 | +3.81 | combo3 |
| `AB@D0=not_red & peer@D0=good & vol@D0=good` | 291 | 49.8% | 4.7% | +3.63 | +3.65 | +3.61 | combo3 |
| `AB@D0=good & peer@D0=not_red & vol@D0=good` | 297 | 50.5% | 5.4% | +3.56 | +3.57 | +3.55 | combo3 |
| `AB@D0=not_red & peer@D0=not_red & vol@D0=good` | 317 | 50.2% | 5.0% | +3.34 | +3.35 | +3.33 | combo3 |
| `peer@D0=good & vol@D0=good` | 446 | 47.1% | 1.9% | +2.43 | +2.44 | +2.54 | combo2 |
| `peer@D0=not_red & vol@D0=good` | 480 | 47.5% | 2.4% | +2.24 | +2.26 | +2.36 | combo2 |
| `AB@D0=good & vol@D0=good` | 522 | 48.9% | 3.7% | +1.88 | +1.89 | +1.89 | combo2 |
| `AB@D0=not_red & vol@D0=good` | 574 | 48.8% | 3.6% | +1.82 | +1.83 | +1.83 | combo2 |
| `gen@D0=good & peer@D0=good & vol@D0=good` · sess | 191 | 57.1% | 11.9% | +1.50 | +1.52 | +1.19 | combo3 |
| `gen@D0=good & peer@D0=not_red & vol@D0=good` · sess | 202 | 58.4% | 13.3% | +1.50 | +1.51 | +1.18 | combo3 |
| `sect@D0=good & gen@D0=good & heat@D0=good` · sess | 738 | 60.8% | 15.7% | +1.28 | +1.30 | +1.43 | combo3 |
| `sect@D0=good & gen@D0=good & heat@D0=not_red` · sess | 784 | 59.4% | 14.3% | +1.19 | +1.21 | +1.34 | combo3 |
| `gen@D0=good & gen@D-1=bad` · sess | 2652 | 65.3% | 20.1% | +1.14 | +1.15 | +0.42 | lag |
| `vol@D0=good & vol@D-1=not_red` | 964 | 47.0% | 1.8% | +1.09 | +1.11 | +1.13 | lag |
| `sect@D0=good & sect@D-1=bad` | 2099 | 54.3% | 9.2% | +1.06 | +1.07 | +1.20 | lag |
| `gen@D0=bad & gen@D-1=good` · sess | 5303 | 61.0% | 15.9% | +1.03 | +1.05 | +0.43 | lag |
| `zero_red@D0 & join@D0=good` | 3188 | 46.6% | 1.5% | +1.02 | +1.03 | +1.02 | zero_red |
| `gen@D0=good & AB@D0=not_red & vol@D0=good` · sess | 277 | 53.1% | 7.9% | +0.97 | +0.99 | +0.63 | combo3 |
| `join@D0=good & gen@D-1=bad` · sess | 1518 | 60.2% | 15.1% | +0.90 | +0.91 | +0.49 | lag_cross |
| `gen@D0=good & peer@D0=good & vol@D0=not_red` · sess | 746 | 58.3% | 13.2% | +0.86 | +0.87 | +0.54 | combo3 |

## 2d — highest win rate

| pattern | n | 2d hit | hit lift | 2d mean | mean lift | 2d xs | family |
|---|---:|---:|---:|---:|---:|---:|---|
| `gen@D0=good & peer@D0=not_red & vol@D0=good` · sess | 202 | 58.4% | 15.4% | +1.49 | +1.73 | +1.29 | combo3 |
| `gen@D0=good & peer@D0=good & vol@D0=good` · sess | 191 | 58.1% | 15.1% | +1.39 | +1.62 | +1.19 | combo3 |
| `gen@D0=good & gen@D-1=bad` · sess | 2652 | 56.5% | 13.5% | +0.56 | +0.80 | +0.18 | lag |
| `gen@D0=good & peer@D0=not_red & vol@D0=not_red` · sess | 864 | 55.6% | 12.5% | +1.57 | +1.81 | +1.36 | combo3 |
| `gen@D0=good & gen@D-1=good & gen@D-2=good` · sess | 2650 | 55.2% | 12.2% | +0.31 | +0.55 | -0.12 | lag |
| `sect@D0=not_red & peer@D0=not_red & vol@D0=good` | 291 | 54.6% | 11.6% | +0.89 | +1.13 | +0.88 | combo3 |
| `sect@D0=not_red & peer@D0=good & vol@D0=good` | 280 | 54.6% | 11.6% | +0.81 | +1.05 | +0.80 | combo3 |
| `gen@D0=good & peer@D0=good & vol@D0=not_red` · sess | 746 | 54.6% | 11.5% | +1.74 | +1.98 | +1.53 | combo3 |
| `gen@D0=good & AB@D0=not_red & vol@D0=good` · sess | 277 | 53.8% | 10.7% | +0.79 | +1.03 | +0.57 | combo3 |
| `gen@D0=good & AB@D0=good & vol@D0=not_red` · sess | 1285 | 53.1% | 10.1% | +0.89 | +1.13 | +0.68 | combo3 |
| `vol@D0=good & vol@D-1=good & vol@D-2=good` | 198 | 53.0% | 10.0% | +0.47 | +0.71 | +0.53 | lag |
| `gen@D0=good & AB@D0=good & vol@D0=good` · sess | 251 | 53.0% | 9.9% | +0.65 | +0.89 | +0.44 | combo3 |
| `sect@D0=not_red & AB@D0=not_red & vol@D0=good` | 340 | 52.9% | 9.9% | +0.63 | +0.87 | +0.57 | combo3 |
| `dig=neutral` | 1462 | 52.8% | 9.8% | +0.36 | +0.60 | +0.71 | single |
| `gen@D0=good & AB@D0=good & peer@D0=good` · sess | 2337 | 52.7% | 9.6% | +0.54 | +0.78 | +0.71 | combo3 |
| `sect@D0=not_red & gen@D0=good & peer@D0=not_red` · sess | 2327 | 52.6% | 9.5% | +0.51 | +0.75 | +0.70 | combo3 |
| `sect@D0=not_red & gen@D0=good & peer@D0=good` · sess | 1890 | 52.5% | 9.4% | +0.59 | +0.83 | +0.79 | combo3 |
| `gen@D0=good & AB@D0=good & peer@D0=not_red` · sess | 2879 | 52.3% | 9.3% | +0.45 | +0.69 | +0.62 | combo3 |
| `gen@D0=good & AB@D0=not_red & vol@D0=not_red` · sess | 1400 | 52.3% | 9.2% | +0.82 | +1.06 | +0.61 | combo3 |
| `sect@D0=good & sect@D-1=bad` | 1681 | 52.2% | 9.2% | +1.06 | +1.30 | +1.52 | lag |

## 2d — highest average return

| pattern | n | 2d hit | hit lift | 2d mean | mean lift | 2d xs | family |
|---|---:|---:|---:|---:|---:|---:|---|
| `sect@D0=good & peer@D0=good & vol@D0=not_red` | 377 | 46.4% | 3.4% | +1.84 | +2.08 | +2.04 | combo3 |
| `gen@D0=good & peer@D0=good & vol@D0=not_red` · sess | 746 | 54.6% | 11.5% | +1.74 | +1.98 | +1.53 | combo3 |
| `gen@D0=good & peer@D0=not_red & vol@D0=not_red` · sess | 864 | 55.6% | 12.5% | +1.57 | +1.81 | +1.36 | combo3 |
| `sect@D0=good & peer@D0=not_red & vol@D0=not_red` | 428 | 47.0% | 3.9% | +1.50 | +1.74 | +1.71 | combo3 |
| `gen@D0=good & peer@D0=not_red & vol@D0=good` · sess | 202 | 58.4% | 15.4% | +1.49 | +1.73 | +1.29 | combo3 |
| `gen@D0=good & peer@D0=good & vol@D0=good` · sess | 191 | 58.1% | 15.1% | +1.39 | +1.62 | +1.19 | combo3 |
| `sect@D0=not_red & peer@D0=good & vol@D0=not_red` | 1219 | 50.4% | 7.4% | +1.08 | +1.32 | +1.18 | combo3 |
| `sect@D0=good & sect@D-1=bad` | 1681 | 52.2% | 9.2% | +1.06 | +1.30 | +1.52 | lag |
| `sect@D0=not_red & peer@D0=not_red & vol@D0=not_red` | 1412 | 51.3% | 8.3% | +1.00 | +1.24 | +1.10 | combo3 |
| `sect@D0=not_red & peer@D0=not_red & vol@D0=good` | 291 | 54.6% | 11.6% | +0.89 | +1.13 | +0.88 | combo3 |
| `gen@D0=good & AB@D0=good & vol@D0=not_red` · sess | 1285 | 53.1% | 10.1% | +0.89 | +1.13 | +0.68 | combo3 |
| `gen@D0=good & AB@D0=not_red & vol@D0=not_red` · sess | 1400 | 52.3% | 9.2% | +0.82 | +1.06 | +0.61 | combo3 |
| `sect@D0=not_red & peer@D0=good & vol@D0=good` | 280 | 54.6% | 11.6% | +0.81 | +1.05 | +0.80 | combo3 |
| `gen@D0=good & AB@D0=not_red & vol@D0=good` · sess | 277 | 53.8% | 10.7% | +0.79 | +1.03 | +0.57 | combo3 |
| `sect@D0=good & gen@D0=good & peer@D0=good` · sess | 1128 | 51.4% | 8.4% | +0.79 | +1.03 | +1.06 | combo3 |
| `jdg=good` | 3118 | 51.3% | 8.3% | +0.78 | +1.02 | +1.26 | single |
| `sect@D0=not_red & AB@D0=not_red & vol@D0=not_red` | 1606 | 51.6% | 8.5% | +0.76 | +1.00 | +0.76 | combo3 |
| `sect@D0=not_red & AB@D0=good & vol@D0=not_red` | 1467 | 51.6% | 8.6% | +0.75 | +0.99 | +0.76 | combo3 |
| `join@D0=good & gen@D0=good & peer@D0=good` · sess | 1443 | 50.7% | 7.7% | +0.66 | +0.90 | +0.84 | combo3 |
| `join@D0=not_red & gen@D0=good & peer@D0=good` · sess | 1602 | 50.9% | 7.9% | +0.66 | +0.90 | +0.84 | combo3 |

## 3d — highest win rate

| pattern | n | 3d hit | hit lift | 3d mean | mean lift | 3d xs | family |
|---|---:|---:|---:|---:|---:|---:|---|
| `gen@D0=good & peer@D0=not_red & vol@D0=good` · sess | 120 | 69.2% | 24.0% | +2.91 | +2.90 | +2.28 | combo3 |
| `gen@D0=good & peer@D0=good & vol@D0=good` · sess | 113 | 68.1% | 23.0% | +2.77 | +2.75 | +2.13 | combo3 |
| `join@D0=not_red & vol@D0=not_red & heat@D0=good` | 9 | 66.7% | 21.5% | +5.92 | +5.91 | +7.63 | combo3 |
| `join@D0=good & vol@D0=not_red & heat@D0=good` | 9 | 66.7% | 21.5% | +5.92 | +5.91 | +7.63 | combo3 |
| `AB@D0=not_red & vol@D0=good & heat@D0=not_red` | 3 | 66.7% | 21.5% | +11.06 | +11.04 | +12.77 | combo3 |
| `gen@D0=not_red & peer@D0=not_red & vol@D0=good` · sess | 187 | 65.8% | 20.6% | +2.79 | +2.78 | +2.43 | combo3 |
| `gen@D0=not_red & peer@D0=good & vol@D0=good` · sess | 177 | 65.5% | 20.4% | +2.78 | +2.76 | +2.42 | combo3 |
| `gen@D0=good & AB@D0=not_red & vol@D0=good` · sess | 169 | 64.5% | 19.3% | +2.21 | +2.20 | +1.55 | combo3 |
| `gen@D0=good & AB@D0=good & vol@D0=good` · sess | 152 | 63.8% | 18.6% | +2.02 | +2.00 | +1.35 | combo3 |
| `gen@D0=good & peer@D0=not_red & vol@D0=not_red` · sess | 508 | 63.6% | 18.4% | +3.08 | +3.06 | +2.42 | combo3 |
| `join@D0=bad & join@D-1=good` | 751 | 63.2% | 18.1% | +3.86 | +3.84 | +3.49 | lag |
| `gen@D0=good & peer@D0=good & vol@D0=not_red` · sess | 440 | 62.7% | 17.6% | +3.33 | +3.32 | +2.68 | combo3 |
| `jdg=good` | 2521 | 62.0% | 16.9% | +2.25 | +2.24 | +2.69 | single |
| `gen@D0=good & AB@D0=good & vol@D0=not_red` · sess | 748 | 60.6% | 15.4% | +2.13 | +2.12 | +1.46 | combo3 |
| `gen@D0=good & gen@D-1=bad` · sess | 2652 | 60.5% | 15.3% | +1.22 | +1.21 | +0.51 | lag |
| `AB@D0=not_red & vol@D0=not_red & heat@D0=good` | 10 | 60.0% | 14.8% | +4.37 | +4.35 | +6.08 | combo3 |
| `vol@D0=good & heat@D0=good` | 5 | 60.0% | 14.8% | +6.67 | +6.66 | +8.39 | combo2 |
| `gen@D0=good & AB@D0=not_red & vol@D0=not_red` · sess | 811 | 59.9% | 14.8% | +2.07 | +2.06 | +1.40 | combo3 |
| `AB@D0=not_red & peer@D0=not_red & vol@D0=good` | 211 | 59.2% | 14.1% | +6.90 | +6.88 | +6.59 | combo3 |
| `join@D0=good & join@D-1=bad` | 1038 | 58.7% | 13.5% | +1.64 | +1.62 | +1.11 | lag |

## 3d — highest average return

| pattern | n | 3d hit | hit lift | 3d mean | mean lift | 3d xs | family |
|---|---:|---:|---:|---:|---:|---:|---|
| `AB@D0=not_red & vol@D0=good & heat@D0=not_red` | 3 | 66.7% | 21.5% | +11.06 | +11.04 | +12.77 | combo3 |
| `AB@D0=good & peer@D0=good & vol@D0=good` | 179 | 57.5% | 12.4% | +7.67 | +7.65 | +7.34 | combo3 |
| `AB@D0=good & peer@D0=not_red & vol@D0=good` | 193 | 58.0% | 12.9% | +7.27 | +7.25 | +6.96 | combo3 |
| `AB@D0=not_red & peer@D0=good & vol@D0=good` | 196 | 58.7% | 13.5% | +7.27 | +7.25 | +6.95 | combo3 |
| `AB@D0=not_red & peer@D0=not_red & vol@D0=good` | 211 | 59.2% | 14.1% | +6.90 | +6.88 | +6.59 | combo3 |
| `vol@D0=good & heat@D0=good` | 5 | 60.0% | 14.8% | +6.67 | +6.66 | +8.39 | combo2 |
| `join@D0=not_red & vol@D0=not_red & heat@D0=good` | 9 | 66.7% | 21.5% | +5.92 | +5.91 | +7.63 | combo3 |
| `join@D0=good & vol@D0=not_red & heat@D0=good` | 9 | 66.7% | 21.5% | +5.92 | +5.91 | +7.63 | combo3 |
| `peer@D0=good & vol@D0=good` | 309 | 56.3% | 11.1% | +5.15 | +5.13 | +5.01 | combo2 |
| `peer@D0=not_red & vol@D0=good` | 330 | 56.7% | 11.5% | +4.95 | +4.93 | +4.81 | combo2 |
| `join@D0=good & vol@D-1=good` · sess | 238 | 56.3% | 11.1% | +4.78 | +4.77 | +4.77 | lag_cross |
| `AB@D0=not_red & vol@D0=good` | 386 | 57.8% | 12.6% | +4.53 | +4.51 | +4.24 | combo2 |
| `AB@D0=good & vol@D0=good` | 342 | 55.9% | 10.7% | +4.48 | +4.47 | +4.19 | combo2 |
| `AB@D0=not_red & vol@D0=not_red & heat@D0=good` | 10 | 60.0% | 14.8% | +4.37 | +4.35 | +6.08 | combo3 |
| `join@D0=not_red & vol@D0=not_red & heat@D0=not_red` | 12 | 58.3% | 13.2% | +3.92 | +3.90 | +5.63 | combo3 |
| `join@D0=good & vol@D0=not_red & heat@D0=not_red` | 12 | 58.3% | 13.2% | +3.92 | +3.90 | +5.63 | combo3 |
| `vol@D0=good & heat@D0=not_red` | 7 | 57.1% | 12.0% | +3.88 | +3.86 | +5.59 | combo2 |
| `join@D0=bad & join@D-1=good` | 751 | 63.2% | 18.1% | +3.86 | +3.84 | +3.49 | lag |
| `AB@D0=not_red & vol@D0=not_red & heat@D0=not_red` | 11 | 54.5% | 9.4% | +3.39 | +3.38 | +5.11 | combo3 |
| `gen@D0=good & peer@D0=good & vol@D0=not_red` · sess | 440 | 62.7% | 17.6% | +3.33 | +3.32 | +2.68 | combo3 |

## 1w — highest win rate

| pattern | n | 1w hit | hit lift | 1w mean | mean lift | 1w xs | family |
|---|---:|---:|---:|---:|---:|---:|---|
| `gen@D0=good & peer@D0=good & vol@D0=good` · sess | 187 | 60.4% | 15.3% | +3.56 | +3.40 | +3.33 | combo3 |
| `gen@D0=good & peer@D0=not_red & vol@D0=good` · sess | 198 | 60.1% | 14.9% | +3.45 | +3.29 | +3.22 | combo3 |
| `gen@D0=good & AB@D0=good & vol@D0=good` · sess | 248 | 59.7% | 14.5% | +1.98 | +1.83 | +1.74 | combo3 |
| `sect@D0=not_red & gen@D0=good & AB@D0=good` · sess | 2412 | 59.5% | 14.3% | +1.50 | +1.34 | +1.31 | combo3 |
| `sect@D0=not_red & gen@D0=good & AB@D0=not_red` · sess | 2583 | 59.4% | 14.2% | +1.50 | +1.35 | +1.31 | combo3 |
| `gen@D0=good & AB@D0=not_red & vol@D0=good` · sess | 273 | 59.3% | 14.2% | +2.08 | +1.93 | +1.84 | combo3 |
| `jdg=good` | 2504 | 58.7% | 13.5% | +2.38 | +2.23 | +2.75 | single |
| `join@D0=good & gen@D0=good & vol@D0=good` · sess | 451 | 57.6% | 12.5% | +1.88 | +1.72 | +2.05 | combo3 |
| `sect@D0=not_red & gen@D0=good & peer@D0=not_red` · sess | 1518 | 57.0% | 11.9% | +1.63 | +1.47 | +1.43 | combo3 |
| `gen@D0=good & gen@D-1=bad` · sess | 2652 | 56.9% | 11.7% | +1.30 | +1.15 | +0.78 | lag |
| `sect@D0=not_red & gen@D0=good & peer@D0=good` · sess | 1224 | 55.9% | 10.7% | +1.77 | +1.62 | +1.58 | combo3 |
| `jdg=bad` | 1343 | 55.9% | 10.7% | +0.90 | +0.74 | +0.97 | single |
| `join@D0=not_red & gen@D0=good & vol@D0=good` · sess | 612 | 55.4% | 10.2% | +1.21 | +1.06 | +1.33 | combo3 |
| `sect@D0=not_red & gen@D0=good & vol@D0=good` · sess | 578 | 55.4% | 10.2% | +1.12 | +0.97 | +1.13 | combo3 |
| `join@D0=good & join@D-1=bad` | 1149 | 55.3% | 10.1% | +2.04 | +1.89 | +1.89 | lag |
| `vol@D0=good & vol@D-1=bad` | 185 | 55.1% | 10.0% | +0.66 | +0.50 | +0.66 | lag |
| `sect@D0=good & sect@D-1=bad` | 1340 | 55.1% | 9.9% | +2.10 | +1.95 | +2.28 | lag |
| `sect@D0=bad & sect@D-1=good` | 2666 | 54.9% | 9.7% | +1.94 | +1.79 | +2.03 | lag |
| `join@D0=good & gen@D0=good & AB@D0=not_red` · sess | 2109 | 54.4% | 9.3% | +1.48 | +1.32 | +1.31 | combo3 |
| `gen@D0=good & AB@D0=good & vol@D0=not_red` · sess | 1273 | 54.3% | 9.1% | +2.00 | +1.84 | +1.77 | combo3 |

## 1w — highest average return

| pattern | n | 1w hit | hit lift | 1w mean | mean lift | 1w xs | family |
|---|---:|---:|---:|---:|---:|---:|---|
| `AB@D0=good & peer@D0=good & vol@D0=good` | 228 | 46.1% | 0.9% | +4.21 | +4.05 | +4.50 | combo3 |
| `AB@D0=good & peer@D0=not_red & vol@D0=good` | 245 | 46.5% | 1.4% | +4.10 | +3.95 | +4.40 | combo3 |
| `AB@D0=not_red & peer@D0=good & vol@D0=good` | 248 | 46.4% | 1.2% | +3.97 | +3.81 | +4.27 | combo3 |
| `peer@D0=good & vol@D0=good` | 332 | 49.4% | 4.2% | +3.96 | +3.80 | +4.28 | combo2 |
| `AB@D0=not_red & peer@D0=not_red & vol@D0=good` | 266 | 47.0% | 1.8% | +3.87 | +3.72 | +4.18 | combo3 |
| `peer@D0=not_red & vol@D0=good` | 353 | 49.3% | 4.1% | +3.77 | +3.62 | +4.09 | combo2 |
| `gen@D0=good & peer@D0=good & vol@D0=good` · sess | 187 | 60.4% | 15.3% | +3.56 | +3.40 | +3.33 | combo3 |
| `join@D0=good & vol@D-1=good` · sess | 386 | 49.5% | 4.3% | +3.50 | +3.35 | +3.88 | lag_cross |
| `gen@D0=good & peer@D0=not_red & vol@D0=good` · sess | 198 | 60.1% | 14.9% | +3.45 | +3.29 | +3.22 | combo3 |
| `gen@D0=good & peer@D0=good & vol@D0=not_red` · sess | 734 | 51.9% | 6.8% | +3.44 | +3.28 | +3.21 | combo3 |
| `vol@D0=good & vol@D-1=good` | 445 | 49.4% | 4.3% | +3.25 | +3.10 | +3.46 | lag |
| `gen@D0=good & peer@D0=not_red & vol@D0=not_red` · sess | 852 | 52.6% | 7.4% | +3.05 | +2.90 | +2.83 | combo3 |
| `sect@D0=good & peer@D0=good & vol@D0=not_red` | 359 | 47.9% | 2.8% | +2.97 | +2.82 | +3.24 | combo3 |
| `vol@D0=good & vol@D-1=not_red` | 851 | 49.2% | 4.1% | +2.96 | +2.80 | +3.14 | lag |
| `join@D0=good & peer@D0=good & vol@D0=not_red` | 728 | 47.7% | 2.5% | +2.79 | +2.63 | +3.14 | combo3 |
| `sect@D0=good & peer@D0=not_red & vol@D0=not_red` | 405 | 48.1% | 3.0% | +2.65 | +2.50 | +2.93 | combo3 |
| `join@D0=not_red & peer@D0=good & vol@D0=not_red` | 812 | 47.4% | 2.2% | +2.52 | +2.36 | +2.87 | combo3 |
| `join@D0=good & peer@D0=not_red & vol@D0=not_red` | 844 | 48.9% | 3.8% | +2.51 | +2.35 | +2.86 | combo3 |
| `join@D0=bad & join@D-1=good` | 848 | 53.5% | 8.4% | +2.50 | +2.34 | +2.51 | lag |
| `join@D0=good & gen@D0=good & peer@D0=good` · sess | 970 | 50.7% | 5.6% | +2.45 | +2.29 | +2.27 | combo3 |

## 2w — highest win rate

_nothing cleared min_n._

## 2w — highest average return

_nothing cleared min_n._

## Lag interactions (D0 × D-1 × D-2)

Same camera yesterday vs today. `good@D0 & bad@D-1` is a turn. `good@D0 & good@D-1` is persistence. Cross-light rows mix join/vol with yesterday's gen (weather).

| pattern | n | 1d hit | hit lift | 1d mean | mean lift | 1d xs | family |
|---|---:|---:|---:|---:|---:|---:|---|
| `join@D0=good & join@D-1=good` | 7729 | 39.0% | -6.2% | -0.52 | -0.50 | -0.10 | lag |
| `join@D0=good & join@D-1=bad` | 2300 | 50.4% | 5.3% | +0.05 | +0.06 | +0.15 | lag |
| `join@D0=bad & join@D-1=good` | 1909 | 46.8% | 1.6% | +0.57 | +0.58 | +0.77 | lag |
| `join@D0=good & join@D-1=good & join@D-2=good` | 3663 | 41.9% | -3.2% | -0.45 | -0.44 | -0.21 | lag |
| `join@D0=good & join@D-1=not_red` | 8711 | 40.5% | -4.6% | -0.44 | -0.43 | -0.11 | lag |
| `join@D0=not_red & join@D-1=good` | 10343 | 39.1% | -6.0% | -0.57 | -0.55 | -0.13 | lag |
| `sect@D0=good & sect@D-1=good` | 5954 | 35.5% | -9.6% | -0.87 | -0.85 | -0.48 | lag |
| `sect@D0=good & sect@D-1=bad` | 2099 | 54.3% | 9.2% | +1.06 | +1.07 | +1.20 | lag |
| `sect@D0=bad & sect@D-1=good` | 3324 | 52.0% | 6.9% | +0.39 | +0.40 | +0.29 | lag |
| `sect@D0=good & sect@D-1=good & sect@D-2=good` | 2526 | 37.0% | -8.1% | -0.90 | -0.88 | -0.66 | lag |
| `sect@D0=good & sect@D-1=not_red` | 7501 | 38.6% | -6.5% | -0.68 | -0.66 | -0.38 | lag |
| `sect@D0=not_red & sect@D-1=good` | 8067 | 36.1% | -9.0% | -0.85 | -0.83 | -0.45 | lag |
| `gen@D0=good & gen@D-1=good` · sess | 10606 | 39.5% | -5.7% | -0.49 | -0.48 | -0.05 | lag |
| `gen@D0=good & gen@D-1=bad` · sess | 2652 | 65.3% | 20.1% | +1.14 | +1.15 | +0.42 | lag |
| `gen@D0=bad & gen@D-1=good` · sess | 5303 | 61.0% | 15.9% | +1.03 | +1.05 | +0.43 | lag |
| `gen@D0=good & gen@D-1=good & gen@D-2=good` · sess | 2650 | 36.1% | -9.1% | -1.02 | -1.00 | -0.43 | lag |
| `gen@D0=good & gen@D-1=not_red` · sess | 10606 | 39.5% | -5.7% | -0.49 | -0.48 | -0.05 | lag |
| `gen@D0=not_red & gen@D-1=good` · sess | 13260 | 38.9% | -6.3% | -0.61 | -0.60 | -0.15 | lag |
| `AB@D0=good & AB@D-1=good` | 8595 | 44.8% | -0.4% | -0.14 | -0.12 | +0.14 | lag |
| `AB@D0=good & AB@D-1=bad` | 182 | 34.1% | -11.1% | -0.97 | -0.96 | -0.56 | lag |
| `AB@D0=bad & AB@D-1=good` | 285 | 35.4% | -9.7% | -0.63 | -0.62 | -0.14 | lag |
| `AB@D0=good & AB@D-1=good & AB@D-2=good` | 6634 | 44.3% | -0.8% | -0.09 | -0.07 | +0.21 | lag |
| `AB@D0=good & AB@D-1=not_red` | 8788 | 44.7% | -0.5% | -0.16 | -0.14 | +0.12 | lag |
| `AB@D0=not_red & AB@D-1=good` | 8838 | 44.7% | -0.4% | -0.15 | -0.13 | +0.14 | lag |
| `vol@D0=good & vol@D-1=good` | 501 | 45.3% | 0.2% | -0.07 | -0.06 | -0.02 | lag |
| `vol@D0=good & vol@D-1=bad` | 338 | 48.2% | 3.1% | -0.14 | -0.13 | +0.04 | lag |
| `vol@D0=bad & vol@D-1=good` | 145 | 39.3% | -5.8% | -0.59 | -0.58 | -0.48 | lag |
| `vol@D0=good & vol@D-1=good & vol@D-2=good` | 212 | 48.6% | 3.4% | +0.43 | +0.45 | +0.28 | lag |
| `vol@D0=good & vol@D-1=not_red` | 964 | 47.0% | 1.8% | +1.09 | +1.11 | +1.13 | lag |
| `vol@D0=not_red & vol@D-1=good` | 1050 | 43.5% | -1.6% | -0.24 | -0.22 | -0.14 | lag |

## Singles (every printed camera)

| pattern | n | 1d hit | hit lift | 1d mean | mean lift | 1d xs | family |
|---|---:|---:|---:|---:|---:|---:|---|
| `join=good` | 13486 | 44.5% | -0.7% | +0.02 | +0.03 | +0.22 | single |
| `join=neutral` | 4843 | 43.4% | -1.8% | -0.31 | -0.29 | +0.01 | single |
| `join=bad` | 10833 | 46.8% | 1.6% | +0.07 | +0.09 | +0.17 | single |
| `sect=good` | 11807 | 43.5% | -1.7% | +0.06 | +0.07 | +0.24 | single |
| `sect=neutral` | 6167 | 45.0% | -0.1% | -0.18 | -0.17 | +0.07 | single |
| `sect=bad` | 11195 | 47.0% | 1.8% | +0.00 | +0.02 | +0.14 | single |
| `gen=good` · sess | 15904 | 46.1% | 0.9% | +0.12 | +0.13 | +0.26 | single |
| `gen=neutral` · sess | 2654 | 36.4% | -8.7% | -1.11 | -1.09 | -0.51 | single |
| `gen=bad` · sess | 10611 | 46.0% | 0.8% | +0.06 | +0.07 | +0.20 | single |
| `news=good` | 471 | 47.1% | 2.0% | -0.11 | -0.09 | +0.07 | single |
| `news=bad` | 214 | 43.0% | -2.1% | -0.04 | -0.02 | -0.01 | single |
| `dig=good` | 13304 | 46.6% | 1.4% | -0.11 | -0.09 | +0.03 | single |
| `dig=neutral` | 1713 | 51.5% | 6.3% | +0.06 | +0.07 | +0.14 | single |
| `jdg=good` | 3769 | 51.7% | 6.5% | +0.48 | +0.49 | +0.74 | single |
| `jdg=neutral` | 2996 | 45.2% | 0.0% | -0.28 | -0.27 | -0.13 | single |
| `jdg=bad` | 1849 | 50.1% | 4.9% | +0.01 | +0.03 | -0.14 | single |
| `AB=good` | 10888 | 48.1% | 3.0% | +0.03 | +0.04 | +0.14 | single |
| `AB=neutral` | 777 | 44.1% | -1.0% | -0.20 | -0.18 | -0.05 | single |
| `AB=bad` | 4160 | 43.8% | -1.3% | -0.21 | -0.20 | -0.05 | single |
| `peer=good` | 6946 | 45.8% | 0.6% | -0.06 | -0.04 | +0.20 | single |
| `peer=neutral` | 1699 | 43.3% | -1.8% | -0.26 | -0.24 | +0.02 | single |
| `peer=bad` | 7994 | 43.7% | -1.4% | -0.27 | -0.25 | -0.04 | single |
| `heat=good` | 3349 | 40.2% | -5.0% | -0.27 | -0.25 | +0.19 | single |
| `heat=neutral` | 357 | 33.1% | -12.1% | -0.79 | -0.77 | -0.27 | single |
| `heat=bad` | 4258 | 36.9% | -8.3% | -0.69 | -0.67 | -0.13 | single |
| `vol=good` | 1484 | 46.5% | 1.4% | +0.64 | +0.65 | +0.80 | single |
| `vol=neutral` | 6921 | 44.0% | -1.2% | -0.27 | -0.26 | -0.00 | single |
| `vol=bad` | 18106 | 44.3% | -0.9% | -0.20 | -0.18 | +0.01 | single |
| `buy=good` | 328 | 49.1% | 4.0% | -0.13 | -0.11 | -0.01 | single |
| `buy=neutral` | 26195 | 44.3% | -0.9% | -0.17 | -0.15 | +0.05 | single |
