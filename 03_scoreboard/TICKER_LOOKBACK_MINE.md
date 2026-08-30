# Ticker lookback mine — market-wide 09:30

_Generated 2026-08-30T15:42:51.570146-04:00_

Liquid universe **2659** names (mcap > $100M, avg vol > 500K). **29244** printed days (≥3 factor boxes + a forward). Sessions 2026-07-31 → 2026-08-27.

Excess = name return minus that session's universe median. Edge = that excess minus the sample's own mean excess (right-skewed names pull the raw mean above the median). Long / fade need n≥80 (n≥40 for rarer tags) and |edge|≥0.15. Same-day Finviz and same-day book do not color the factors.

## Read this first

Biggest edges vs the sample's own mean. `turn` (blue on a red row) did **not** clear the bar market-wide. The useful blue is a blue tag on a mixed 3-day stretch. Alarm on a still-green row (`first_crack`) is the clean fade.

| pattern | n | read | 1d hit | 1d xs-hit | 1d edge | 1d xs | 3d xs | 1w xs |
|---|---:|---|---:|---:|---:|---:|---:|---:|
| `blue|heat=bad` | 138 | long | 78.3% | 79.7% | +3.09 | +3.36 | — | — |
| `vol=good|ab=good` | 349 | long | 52.7% | 50.7% | +2.76 | +3.03 | +4.22 | +1.75 |
| `gen=bad|vol=good` | 441 | long | 47.2% | 49.4% | +1.91 | +2.17 | +2.66 | +5.99 |
| `blue|heat=good` | 372 | long | 66.9% | 68.5% | +1.76 | +2.03 | — | — |
| `join=bad|vol=good` | 467 | long | 47.3% | 48.6% | +1.74 | +2.01 | +3.14 | +3.44 |
| `alarm|heat=bad` | 87 | fade | 18.4% | 20.7% | -1.51 | -1.24 | — | — |
| `judge=neutral` | 762 | long | 70.2% | 68.2% | +1.35 | +1.62 | +1.63 | — |
| `alarm|good` | 929 | fade | 39.3% | 42.7% | -1.32 | -1.05 | -1.64 | -2.45 |
| `white|join=neutral` | 158 | fade | 36.7% | 43.0% | -1.24 | -0.97 | -1.93 | -2.12 |
| `first_crack` | 984 | fade | 39.7% | 43.7% | -1.22 | -0.95 | -1.26 | -2.03 |

## Base rate

| n | 1d hit | 1d xs-hit | 1d mean | 1d xs | 3d xs | 1w xs |
|---|---|---|---|---|---|---|
| 26589 | 48.6% | 50.0% | +0.24 | +0.27 | +0.41 | +0.61 |

## Usable tags × region

_The same 🔵 / 🚨 / ⚪ flips meaning on green vs red mass._

| pattern | n | read | 1d hit | 1d xs-hit | 1d edge | 1d xs | 3d xs | 1w xs |
|---|---:|---|---:|---:|---:|---:|---:|---:|
| `alarm|good` | 929 | fade | 39.3% | 42.7% | -1.32 | -1.05 | -1.64 | -2.45 |
| `white|good` | 538 | fade | 41.6% | 43.9% | -0.72 | -0.45 | -0.53 | -0.64 |
| `alarm|bad` | 3054 | fade | 51.5% | 47.6% | -0.45 | -0.18 | +0.15 | +0.78 |
| `blue|good` | 7718 | long | 49.1% | 49.1% | +0.34 | +0.61 | +0.33 | +0.64 |
| `alarm|neutral` | 3402 | fade | 43.2% | 51.4% | -0.26 | +0.01 | +0.94 | +1.01 |
| `blue|neutral` | 3164 | long | 55.6% | 53.6% | +0.16 | +0.43 | +0.42 | +0.73 |

## Usable tag context

_turn / late / first_crack / continuation / crowded / clean_chop._

| pattern | n | read | 1d hit | 1d xs-hit | 1d edge | 1d xs | 3d xs | 1w xs |
|---|---:|---|---:|---:|---:|---:|---:|---:|
| `first_crack` | 984 | fade | 39.7% | 43.7% | -1.22 | -0.95 | -1.26 | -2.03 |
| `continuation` | 3054 | fade | 51.5% | 47.6% | -0.45 | -0.18 | +0.15 | +0.78 |
| `late` | 7718 | long | 49.1% | 49.1% | +0.34 | +0.61 | +0.33 | +0.64 |

## Usable tags × stretch

_Trailing 3-session green/red balance._

| pattern | n | read | 1d hit | 1d xs-hit | 1d edge | 1d xs | 3d xs | 1w xs |
|---|---:|---|---:|---:|---:|---:|---:|---:|
| `blue|neutral` | 4149 | long | 53.0% | 50.6% | +0.88 | +1.15 | +1.19 | +1.69 |
| `white|good` | 540 | fade | 41.5% | 43.9% | -0.72 | -0.46 | -0.52 | -0.63 |
| `alarm|bad` | 1276 | fade | 43.3% | 45.9% | -0.57 | -0.30 | -0.32 | +0.18 |
| `alarm|good` | 3918 | fade | 43.5% | 49.9% | -0.47 | -0.21 | +0.51 | +0.53 |
| `alarm|neutral` | 2191 | fade | 52.6% | 48.4% | -0.41 | -0.14 | +0.23 | +0.43 |
| `blue|good` | 4173 | fade | 41.9% | 49.5% | -0.21 | +0.06 | -0.43 | -0.07 |

## Usable tags alone

_Bare tag without the row color. Usually weaker._

| pattern | n | read | 1d hit | 1d xs-hit | 1d edge | 1d xs | 3d xs | 1w xs |
|---|---:|---|---:|---:|---:|---:|---:|---:|
| `white` | 542 | fade | 41.5% | 43.9% | -0.72 | -0.45 | -0.53 | -0.62 |
| `alarm` | 7385 | fade | 46.2% | 48.8% | -0.47 | -0.20 | +0.30 | +0.47 |
| `blue` | 12063 | long | 49.4% | 50.6% | +0.25 | +0.52 | +0.37 | +0.64 |

## Usable factor tones

_Single box, printed cells only._

| pattern | n | read | 1d hit | 1d xs-hit | 1d edge | 1d xs | 3d xs | 1w xs |
|---|---:|---|---:|---:|---:|---:|---:|---:|
| `judge=neutral` | 762 | long | 70.2% | 68.2% | +1.35 | +1.62 | +1.63 | — |
| `vol=good` | 1205 | long | 48.2% | 51.1% | +0.79 | +1.06 | +1.47 | +1.90 |
| `heat=neutral` | 83 | fade | 30.1% | 41.0% | -0.57 | -0.30 | — | — |
| `judge=good` | 2730 | fade | 45.8% | 46.8% | -0.52 | -0.25 | -1.42 | -2.56 |
| `judge=bad` | 1849 | fade | 50.2% | 47.4% | -0.41 | -0.14 | +0.01 | +0.72 |
| `heat=good` | 1538 | long | 50.3% | 54.7% | +0.38 | +0.65 | — | — |
| `digest=neutral` | 1210 | fade | 54.1% | 49.9% | -0.33 | -0.06 | -0.08 | +0.43 |
| `peer=neutral` | 1171 | fade | 48.0% | 50.0% | -0.25 | +0.02 | -0.06 | -0.01 |
| `buy=good` | 263 | fade | 50.9% | 50.2% | -0.25 | +0.02 | +0.53 | +0.12 |
| `vol=neutral` | 4656 | fade | 47.8% | 49.5% | -0.24 | +0.03 | +0.63 | +1.10 |
| `sector=neutral` | 5983 | fade | 44.7% | 48.8% | -0.23 | +0.04 | +0.43 | +1.40 |
| `news=good` | 397 | fade | 51.1% | 44.8% | -0.23 | +0.04 | +0.19 | -0.06 |
| `peer=bad` | 5889 | fade | 47.6% | 49.4% | -0.22 | +0.05 | +0.35 | +0.45 |
| `vol=bad` | 15366 | fade | 46.4% | 50.0% | -0.20 | +0.07 | +0.14 | +0.16 |
| `join=neutral` | 5999 | fade | 47.0% | 51.3% | -0.15 | +0.12 | +0.24 | +0.24 |
| `buy=neutral` | 20974 | fade | 46.8% | 49.9% | -0.15 | +0.12 | +0.34 | +0.51 |

## Usable factor pairs

_Two printed cores on the same 09:30 row._

| pattern | n | read | 1d hit | 1d xs-hit | 1d edge | 1d xs | 3d xs | 1w xs |
|---|---:|---|---:|---:|---:|---:|---:|---:|
| `vol=good|ab=good` | 349 | long | 52.7% | 50.7% | +2.76 | +3.03 | +4.22 | +1.75 |
| `gen=bad|vol=good` | 441 | long | 47.2% | 49.4% | +1.91 | +2.17 | +2.66 | +5.99 |
| `join=bad|vol=good` | 467 | long | 47.3% | 48.6% | +1.74 | +2.01 | +3.14 | +3.44 |
| `heat=bad|join=good` | 476 | fade | 29.2% | 32.8% | -0.73 | -0.46 | — | — |
| `heat=good|join=neutral` | 82 | long | 57.3% | 59.8% | +0.70 | +0.97 | — | — |
| `heat=bad|join=bad` | 469 | long | 53.5% | 56.3% | +0.65 | +0.91 | — | — |
| `heat=good|join=bad` | 799 | long | 52.4% | 56.3% | +0.62 | +0.89 | — | — |
| `vol=neutral|ab=neutral` | 118 | fade | 48.3% | 43.2% | -0.55 | -0.28 | +0.35 | -0.04 |
| `vol=good|ab=bad` | 202 | long | 61.4% | 57.4% | +0.44 | +0.71 | +2.23 | +3.02 |
| `ab=bad|peer=neutral` | 151 | fade | 46.4% | 41.7% | -0.41 | -0.14 | -0.71 | -0.16 |
| `gen=good|vol=neutral` | 2685 | fade | 45.0% | 48.6% | -0.38 | -0.11 | +0.56 | +0.99 |
| `join=bad|ab=good` | 2908 | long | 56.0% | 52.6% | +0.31 | +0.58 | +0.88 | +0.04 |
| `join=good|vol=bad` | 5997 | fade | 44.2% | 48.3% | -0.30 | -0.03 | +0.04 | +0.30 |
| `join=good|vol=good` | 526 | long | 48.9% | 52.3% | +0.29 | +0.56 | +0.71 | +1.99 |
| `ab=good|peer=neutral` | 716 | fade | 53.6% | 48.2% | -0.29 | -0.02 | -0.17 | -0.02 |
| `join=neutral|vol=bad` | 3257 | fade | 42.2% | 51.9% | -0.28 | -0.01 | +0.03 | -0.07 |
| `join=good|ab=good` | 4078 | fade | 52.0% | 47.7% | -0.27 | -0.00 | +0.35 | +1.30 |
| `buy=neutral|join=good` | 8264 | fade | 44.8% | 48.7% | -0.25 | +0.02 | +0.28 | +0.88 |
| `join=bad|vol=neutral` | 1900 | fade | 50.1% | 48.2% | -0.24 | +0.03 | +0.47 | +0.11 |
| `join=good|vol=neutral` | 1897 | fade | 46.1% | 49.3% | -0.22 | +0.05 | +0.89 | +1.98 |
| `gen=good|vol=bad` | 9817 | fade | 44.2% | 50.1% | -0.22 | +0.05 | -0.03 | +0.11 |
| `join=neutral|gen=good` | 4560 | fade | 46.4% | 50.7% | -0.21 | +0.06 | +0.14 | +0.15 |
| `ab=good|peer=bad` | 3016 | fade | 52.3% | 49.2% | -0.21 | +0.06 | +0.36 | +0.45 |
| `ab=good|peer=good` | 3116 | long | 54.7% | 50.0% | +0.19 | +0.46 | +0.81 | +1.20 |

## Usable tag × factor

_Tag plus one printed box._

| pattern | n | read | 1d hit | 1d xs-hit | 1d edge | 1d xs | 3d xs | 1w xs |
|---|---:|---|---:|---:|---:|---:|---:|---:|
| `blue|heat=bad` | 138 | long | 78.3% | 79.7% | +3.09 | +3.36 | — | — |
| `blue|heat=good` | 372 | long | 66.9% | 68.5% | +1.76 | +2.03 | — | — |
| `alarm|heat=bad` | 87 | fade | 18.4% | 20.7% | -1.51 | -1.24 | — | — |
| `white|join=neutral` | 158 | fade | 36.7% | 43.0% | -1.24 | -0.97 | -1.93 | -2.12 |
| `white|ab=good` | 43 | long | 55.8% | 65.1% | +1.05 | +1.32 | +0.49 | +1.96 |
| `white|vol=neutral` | 185 | fade | 37.8% | 49.7% | -0.98 | -0.71 | -0.83 | -0.74 |
| `alarm|vol=good` | 190 | fade | 42.1% | 43.2% | -0.86 | -0.59 | -0.24 | +5.96 |
| `white|gen=good` | 542 | fade | 41.5% | 43.9% | -0.72 | -0.45 | -0.74 | -0.62 |
| `alarm|join=neutral` | 2683 | fade | 41.6% | 50.4% | -0.56 | -0.29 | -0.22 | -0.30 |
| `alarm|join=bad` | 3103 | fade | 49.4% | 46.2% | -0.56 | -0.29 | +0.07 | +0.58 |
| `alarm|vol=neutral` | 1110 | fade | 47.7% | 47.7% | -0.53 | -0.26 | +0.84 | +0.68 |
| `white|join=good` | 378 | fade | 43.6% | 44.2% | -0.51 | -0.24 | +0.02 | -0.01 |
| `alarm|gen=good` | 3349 | fade | 39.0% | 50.9% | -0.51 | -0.24 | +0.56 | +0.39 |
| `alarm|vol=bad` | 5944 | fade | 45.6% | 48.9% | -0.47 | -0.20 | +0.20 | +0.25 |
| `alarm|gen=bad` | 4036 | fade | 52.1% | 46.9% | -0.45 | -0.18 | +0.13 | +0.62 |
| `alarm|ab=neutral` | 84 | fade | 46.4% | 45.2% | -0.40 | -0.13 | -0.25 | -0.48 |
| `alarm|ab=good` | 1429 | fade | 54.6% | 48.8% | -0.38 | -0.11 | +0.34 | +0.16 |
| `blue|ab=bad` | 974 | long | 56.8% | 53.0% | +0.35 | +0.62 | +0.50 | +0.83 |
| `blue|join=good` | 6565 | long | 47.2% | 49.2% | +0.32 | +0.59 | +0.30 | +0.83 |
| `blue|gen=good` | 10581 | long | 51.8% | 50.0% | +0.30 | +0.57 | +0.34 | +0.64 |
| `blue|ab=neutral` | 186 | long | 56.5% | 49.5% | +0.28 | +0.55 | +0.41 | +0.67 |
| `blue|join=neutral` | 2359 | long | 52.4% | 52.1% | +0.18 | +0.45 | +0.67 | +0.81 |
| `blue|join=bad` | 3131 | long | 51.8% | 52.1% | +0.17 | +0.43 | +0.27 | -0.19 |
| `alarm|join=good` | 1553 | fade | 46.9% | 50.9% | -0.16 | +0.11 | +1.68 | +2.56 |

## Region / stretch / cond / class

_Row color without a tag._

| pattern | n | read | 1d hit | 1d xs-hit | 1d edge | 1d xs | 3d xs | 1w xs |
|---|---:|---|---:|---:|---:|---:|---:|---:|
| `reg=bad` | 5864 | fade | 48.7% | 49.5% | -0.19 | +0.08 | +0.44 | +0.76 |
| `stretch=neutral` | 8937 | long | 52.5% | 50.0% | +0.33 | +0.60 | +0.87 | +1.30 |
| `stretch=good` | 11042 | fade | 43.7% | 49.4% | -0.24 | +0.03 | +0.14 | +0.24 |
| `cond=bad` | 6156 | fade | 51.5% | 49.1% | -0.19 | +0.07 | +0.29 | +0.41 |
| `class=gated_out` | 2480 | long | 46.3% | 49.8% | +1.46 | +1.73 | +2.60 | +4.21 |
| `class=overnight_sell` | 252 | fade | 48.0% | 50.4% | -0.25 | +0.02 | +0.34 | +0.95 |
| `class=overnight_buy` | 251 | fade | 51.8% | 51.0% | -0.24 | +0.03 | +0.48 | +0.07 |
| `class=asof_0930` | 23214 | fade | 48.8% | 49.9% | -0.15 | +0.12 | +0.18 | +0.18 |

## Also-ran (biggest |excess|, still noise or thin)

_These printed but did not clear long/fade. Useful as 'do not trade this bare.'_

| pattern | n | read | 1d hit | 1d xs-hit | 1d edge | 1d xs | 3d xs | 1w xs |
|---|---:|---|---:|---:|---:|---:|---:|---:|
| `pair:heat=neutral|join=neutral` | 2 | thin | 0.0% | 0.0% | — | -3.29 | — | — |
| `pair:vol=good|ab=neutral` | 36 | thin | 61.1% | 52.8% | — | +2.09 | +4.02 | +2.83 |
| `pair:join=neutral|ab=neutral` | 26 | thin | 69.2% | 69.2% | — | +0.96 | +1.50 | +3.81 |
| `tag_context:crowded` | 5096 | noise | 48.0% | 46.8% | +0.39 | +0.66 | +0.38 | +0.66 |
| `pair:heat=neutral|join=bad` | 17 | thin | 11.8% | 11.8% | — | -0.65 | — | — |
| `pair:buy=good|join=neutral` | 48 | thin | 39.6% | 43.8% | — | -0.63 | -0.91 | -2.68 |
| `tag_context:clean_chop` | 5 | thin | 40.0% | 60.0% | — | +0.51 | +1.47 | +1.14 |
| `pair:gen=good|vol=good` | 764 | noise | 48.8% | 52.1% | +0.14 | +0.41 | +0.25 | +1.17 |
| `factor:catal=neutral` | 5 | thin | 40.0% | 60.0% | — | +0.40 | — | — |
| `tag_region:white|neutral` | 4 | thin | 25.0% | 50.0% | — | -0.39 | +0.07 | +1.10 |
| `pair:join=good|gen=good` | 9494 | noise | 45.6% | 49.0% | +0.12 | +0.39 | +0.56 | +1.01 |
| `factor:news=bad` | 206 | noise | 52.4% | 50.5% | +0.11 | +0.38 | -0.08 | -0.44 |
| `factor:sector=bad` | 8308 | noise | 54.5% | 51.9% | +0.09 | +0.35 | +1.17 | +1.26 |
| `factor:peer=good` | 4855 | noise | 49.8% | 50.7% | +0.08 | +0.35 | +0.63 | +1.16 |
| `factor:join=good` | 11294 | noise | 47.2% | 49.0% | +0.08 | +0.35 | +0.45 | +1.00 |
| `pair:heat=bad|join=neutral` | 91 | noise | 34.1% | 36.3% | +0.07 | +0.34 | — | — |
| `pair:join=neutral|ab=good` | 426 | noise | 52.8% | 48.8% | +0.07 | +0.34 | +0.00 | +0.46 |
| `pair:heat=good|join=good` | 657 | noise | 46.7% | 52.0% | +0.06 | +0.33 | — | — |
| `factor:sector=good` | 12298 | noise | 46.6% | 49.2% | +0.06 | +0.32 | -0.09 | +0.13 |
| `pair:join=good|ab=bad` | 713 | noise | 51.3% | 52.3% | +0.04 | +0.31 | +0.79 | +1.80 |
