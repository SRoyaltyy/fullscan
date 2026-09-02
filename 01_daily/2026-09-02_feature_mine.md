# Full feature mine

_Generated 2026-09-02T00:58:53.513135-04:00 · as-of 09:30 ET_

**47432** printed name-days · sessions 2026-08-13 → 2026-09-01. Liquid filter = True.

Win = close-to-close > 0. Lift vs all-name base on the same horizon. Finviz numerics are the **prior session** export. Same-day book never colors a cell.

🔵 blue = objectively better vs prior session (or +≥3 box points). ⚪ white = zero_red. 🚨 alarm = purely worse. fade = featured fade setups.

## Base rate

| horizon | n | hit | mean | median | mean xs |
|---|---:|---:|---:|---:|---:|
| 1d | 33692 | 42.1% | +1.08 | -0.09 | +1.16 |
| 2d | 30938 | 34.7% | +1.95 | -0.43 | +2.38 |
| 3d | 28253 | 34.7% | +2.02 | -0.58 | +2.54 |
| 1w | 22861 | 33.9% | +2.56 | -0.57 | +3.14 |
| 2w | 0 | — | — | — | — |

## Lookback marks (🔵 / ⚪ / 🚨)

| pattern | n | 1d hit | hit lift | 1d mean | mean lift | 1d xs | family |
|---|---:|---:|---:|---:|---:|---:|---|
| `blue` | 3387 | 57.7% | 15.5% | +4.46 | +3.38 | +4.04 | mark |
| `white` | 22758 | 38.5% | -3.6% | +1.07 | -0.02 | +1.20 | mark |
| `alarm` | 4745 | 46.7% | 4.5% | +1.98 | +0.90 | +1.84 | mark |
| `fade` | 477 | 38.2% | -4.0% | -0.72 | -1.80 | -0.14 | mark |
| `blue+white` | 1246 | 49.4% | 7.2% | +10.48 | +9.40 | +10.34 | mark |
| `blue+not_alarm` | 3387 | 57.7% | 15.5% | +4.46 | +3.38 | +4.04 | mark |
| `alarm+not_white` | 4276 | 47.7% | 5.5% | +2.27 | +1.19 | +2.06 | mark |
| `first_crack` | 477 | 38.2% | -4.0% | -0.72 | -1.80 | -0.14 | mark |
| `cond=good` | 26435 | 40.4% | -1.7% | +1.32 | +0.24 | +1.43 | mark |
| `cond=bad` | 5228 | 47.3% | 5.2% | +0.14 | -0.94 | +0.12 | mark |
| `region=good` | 23473 | 39.2% | -2.9% | +1.06 | -0.02 | +1.17 | mark |
| `region=bad` | 2927 | 48.4% | 6.3% | +0.21 | -0.87 | +0.15 | mark |

## Strategy stacks

| pattern | n | 1d hit | hit lift | 1d mean | mean lift | 1d xs | family |
|---|---:|---:|---:|---:|---:|---:|---|
| `steady_daily` | 20861 | 39.6% | -2.5% | +1.07 | -0.01 | +1.18 | stack |
| `steady+blue` | 1394 | 52.0% | 9.9% | +9.54 | +8.45 | +9.25 | stack |

## 1d — highest win rate

| pattern | n | 1d hit | hit lift | 1d mean | mean lift | 1d xs | family |
|---|---:|---:|---:|---:|---:|---:|---|
| `hot+ab+peer` | 51 | 70.6% | 28.4% | +3.14 | +2.05 | +2.42 | cross |
| `peer=good` | 922 | 65.8% | 23.7% | +1.01 | -0.07 | +0.30 | camera |
| `ab=good` | 1668 | 64.6% | 22.4% | +1.00 | -0.08 | +0.28 | camera |
| `ab_up` | 574 | 63.4% | 21.3% | +0.92 | -0.17 | +0.20 | ab |
| `blue+relvol=hot` | 180 | 62.2% | 20.1% | +74.67 | +73.58 | +74.21 | cross |
| `blue` | 3387 | 57.7% | 15.5% | +4.46 | +3.38 | +4.04 | mark |
| `blue+not_alarm` | 3387 | 57.7% | 15.5% | +4.46 | +3.38 | +4.04 | mark |
| `steady+blue` | 1394 | 52.0% | 9.9% | +9.54 | +8.45 | +9.25 | stack |
| `blue+white` | 1246 | 49.4% | 7.2% | +10.48 | +9.40 | +10.34 | mark |
| `region=bad` | 2927 | 48.4% | 6.3% | +0.21 | -0.87 | +0.15 | mark |
| `ins_buy` | 176 | 48.3% | 6.2% | +0.16 | -0.93 | -0.04 | insider |
| `alarm+not_white` | 4276 | 47.7% | 5.5% | +2.27 | +1.19 | +2.06 | mark |
| `ins_sell` | 606 | 47.5% | 5.4% | -0.11 | -1.19 | -0.32 | insider |
| `cond=bad` | 5228 | 47.3% | 5.2% | +0.14 | -0.94 | +0.12 | mark |
| `alarm` | 4745 | 46.7% | 4.5% | +1.98 | +0.90 | +1.84 | mark |
| `perf_w=extended` | 3220 | 46.6% | 4.4% | +3.27 | +2.19 | +3.33 | finviz |
| `short=high` | 7373 | 46.1% | 4.0% | +1.62 | +0.54 | +1.67 | finviz |
| `gen=bad` | 5353 | 45.9% | 3.8% | +0.10 | -0.99 | +0.14 | camera |
| `hot+short=high` | 380 | 45.0% | 2.9% | +12.73 | +11.64 | +12.74 | cross |
| `sma20=below` | 10163 | 44.5% | 2.4% | +2.57 | +1.49 | +2.63 | finviz |

## 1d — highest average return

| pattern | n | 1d hit | hit lift | 1d mean | mean lift | 1d xs | family |
|---|---:|---:|---:|---:|---:|---:|---|
| `blue+relvol=hot` | 180 | 62.2% | 20.1% | +74.67 | +73.58 | +74.21 | cross |
| `rsi=oversold` | 664 | 42.5% | 0.3% | +35.87 | +34.79 | +35.95 | finviz |
| `gap=down` | 1246 | 40.3% | -1.8% | +15.57 | +14.49 | +15.85 | finviz |
| `perf_w=washed` | 1900 | 42.3% | 0.2% | +13.07 | +11.99 | +13.15 | finviz |
| `hot+short=high` | 380 | 45.0% | 2.9% | +12.73 | +11.64 | +12.74 | cross |
| `relvol=hot` | 2020 | 43.3% | 1.2% | +11.07 | +9.98 | +11.14 | finviz |
| `blue+white` | 1246 | 49.4% | 7.2% | +10.48 | +9.40 | +10.34 | mark |
| `steady+blue` | 1394 | 52.0% | 9.9% | +9.54 | +8.45 | +9.25 | stack |
| `gap=up` | 896 | 43.8% | 1.6% | +4.75 | +3.67 | +4.69 | finviz |
| `blue` | 3387 | 57.7% | 15.5% | +4.46 | +3.38 | +4.04 | mark |
| `blue+not_alarm` | 3387 | 57.7% | 15.5% | +4.46 | +3.38 | +4.04 | mark |
| `perf_w=extended` | 3220 | 46.6% | 4.4% | +3.27 | +2.19 | +3.33 | finviz |
| `hot+ab+peer` | 51 | 70.6% | 28.4% | +3.14 | +2.05 | +2.42 | cross |
| `sma20=below` | 10163 | 44.5% | 2.4% | +2.57 | +1.49 | +2.63 | finviz |
| `alarm+not_white` | 4276 | 47.7% | 5.5% | +2.27 | +1.19 | +2.06 | mark |
| `alarm` | 4745 | 46.7% | 4.5% | +1.98 | +0.90 | +1.84 | mark |
| `short=high` | 7373 | 46.1% | 4.0% | +1.62 | +0.54 | +1.67 | finviz |
| `cond=good` | 26435 | 40.4% | -1.7% | +1.32 | +0.24 | +1.43 | mark |
| `join=good` | 26816 | 40.5% | -1.7% | +1.31 | +0.23 | +1.42 | camera |
| `white+join=good` | 22236 | 38.5% | -3.6% | +1.10 | +0.02 | +1.23 | combo |

## 2d — highest win rate

| pattern | n | 2d hit | hit lift | 2d mean | mean lift | 2d xs | family |
|---|---:|---:|---:|---:|---:|---:|---|
| `fade` | 477 | 53.9% | 19.2% | +0.10 | -1.85 | -0.27 | mark |
| `first_crack` | 477 | 53.9% | 19.2% | +0.10 | -1.85 | -0.27 | mark |
| `region=bad` | 2572 | 48.1% | 13.4% | +0.28 | -1.67 | +0.46 | mark |
| `alarm` | 4744 | 46.9% | 12.2% | +2.38 | +0.43 | +2.67 | mark |
| `gen=bad` | 5352 | 46.3% | 11.6% | +0.08 | -1.87 | +0.26 | camera |
| `alarm+not_white` | 4275 | 46.2% | 11.6% | +2.63 | +0.68 | +2.99 | mark |
| `cond=bad` | 4636 | 46.1% | 11.4% | +0.05 | -1.90 | +0.22 | mark |
| `blue` | 837 | 46.0% | 11.3% | +12.89 | +10.94 | +13.44 | mark |
| `blue+not_alarm` | 837 | 46.0% | 11.3% | +12.89 | +10.94 | +13.44 | mark |
| `blue+white` | 683 | 45.0% | 10.2% | +15.77 | +13.82 | +16.38 | mark |
| `steady+blue` | 567 | 44.3% | 9.6% | +18.71 | +16.76 | +19.31 | stack |
| `short=high` | 6291 | 42.7% | 8.0% | +3.53 | +1.58 | +3.81 | finviz |
| `ins_buy` | 117 | 41.9% | 7.2% | -0.59 | -2.54 | -0.40 | insider |
| `ins_sell` | 405 | 41.0% | 6.3% | -1.04 | -3.00 | -0.86 | insider |
| `sma20=below` | 9038 | 40.6% | 5.9% | +5.84 | +3.89 | +6.24 | finviz |
| `rsi=oversold` | 620 | 40.0% | 5.3% | +80.80 | +78.84 | +81.22 | finviz |
| `blue+relvol=hot` | 46 | 39.1% | 4.4% | +238.47 | +236.52 | +239.21 | cross |
| `perf_w=extended` | 2973 | 38.6% | 3.9% | +4.85 | +2.90 | +5.28 | finviz |
| `perf_w=washed` | 1691 | 38.4% | 3.7% | +29.65 | +27.70 | +30.05 | finviz |
| `gap=down` | 1224 | 37.7% | 3.0% | +34.05 | +32.09 | +34.51 | finviz |

## 2d — highest average return

| pattern | n | 2d hit | hit lift | 2d mean | mean lift | 2d xs | family |
|---|---:|---:|---:|---:|---:|---:|---|
| `blue+relvol=hot` | 46 | 39.1% | 4.4% | +238.47 | +236.52 | +239.21 | cross |
| `rsi=oversold` | 620 | 40.0% | 5.3% | +80.80 | +78.84 | +81.22 | finviz |
| `gap=down` | 1224 | 37.7% | 3.0% | +34.05 | +32.09 | +34.51 | finviz |
| `perf_w=washed` | 1691 | 38.4% | 3.7% | +29.65 | +27.70 | +30.05 | finviz |
| `hot+short=high` | 310 | 34.8% | 0.1% | +27.33 | +25.38 | +27.67 | cross |
| `relvol=hot` | 1873 | 34.4% | -0.3% | +21.25 | +19.30 | +21.75 | finviz |
| `steady+blue` | 567 | 44.3% | 9.6% | +18.71 | +16.76 | +19.31 | stack |
| `blue+white` | 683 | 45.0% | 10.2% | +15.77 | +13.82 | +16.38 | mark |
| `blue` | 837 | 46.0% | 11.3% | +12.89 | +10.94 | +13.44 | mark |
| `blue+not_alarm` | 837 | 46.0% | 11.3% | +12.89 | +10.94 | +13.44 | mark |
| `gap=up` | 703 | 28.9% | -5.8% | +9.73 | +7.78 | +10.18 | finviz |
| `sma20=below` | 9038 | 40.6% | 5.9% | +5.84 | +3.89 | +6.24 | finviz |
| `perf_w=extended` | 2973 | 38.6% | 3.9% | +4.85 | +2.90 | +5.28 | finviz |
| `short=high` | 6291 | 42.7% | 8.0% | +3.53 | +1.58 | +3.81 | finviz |
| `alarm+not_white` | 4275 | 46.2% | 11.6% | +2.63 | +0.68 | +2.99 | mark |
| `alarm` | 4744 | 46.9% | 12.2% | +2.38 | +0.43 | +2.67 | mark |
| `cond=good` | 25005 | 31.4% | -3.3% | +2.35 | +0.40 | +2.86 | mark |
| `join=good` | 25465 | 31.8% | -2.9% | +2.33 | +0.38 | +2.83 | camera |
| `steady_daily` | 19973 | 30.7% | -4.0% | +2.04 | +0.09 | +2.52 | stack |
| `white+join=good` | 21647 | 29.2% | -5.5% | +1.46 | -0.49 | +1.96 | combo |

## 3d — highest win rate

| pattern | n | 3d hit | hit lift | 3d mean | mean lift | 3d xs | family |
|---|---:|---:|---:|---:|---:|---:|---|
| `region=bad` | 1312 | 57.7% | 23.0% | +1.34 | -0.69 | +0.73 | mark |
| `blue+relvol=hot` | 46 | 56.5% | 21.8% | +263.42 | +261.39 | +263.85 | cross |
| `gen=bad` | 2674 | 56.4% | 21.7% | +1.26 | -0.77 | +0.65 | camera |
| `cond=bad` | 2373 | 55.2% | 20.5% | +1.16 | -0.86 | +0.63 | mark |
| `alarm+not_white` | 4134 | 54.5% | 19.8% | +3.45 | +1.43 | +3.40 | mark |
| `alarm` | 4603 | 53.4% | 18.8% | +3.01 | +0.99 | +3.02 | mark |
| `blue` | 685 | 49.5% | 14.8% | +17.50 | +15.47 | +17.96 | mark |
| `blue+not_alarm` | 685 | 49.5% | 14.8% | +17.50 | +15.47 | +17.96 | mark |
| `blue+white` | 685 | 49.5% | 14.8% | +17.50 | +15.47 | +17.96 | mark |
| `ins_buy` | 58 | 48.3% | 13.6% | +0.51 | -1.51 | -0.09 | insider |
| `steady+blue` | 568 | 48.2% | 13.6% | +20.93 | +18.91 | +21.39 | stack |
| `fade` | 477 | 44.6% | 10.0% | -0.85 | -2.87 | -0.24 | mark |
| `first_crack` | 477 | 44.6% | 10.0% | -0.85 | -2.87 | -0.24 | mark |
| `short=high` | 5213 | 44.4% | 9.7% | +4.59 | +2.56 | +4.97 | finviz |
| `ins_sell` | 203 | 43.8% | 9.2% | -0.37 | -2.39 | -0.97 | insider |
| `sma20=below` | 7910 | 42.9% | 8.2% | +6.96 | +4.94 | +7.44 | finviz |
| `rsi=oversold` | 557 | 41.5% | 6.8% | +91.21 | +89.19 | +91.70 | finviz |
| `perf_w=washed` | 1486 | 39.6% | 5.0% | +34.19 | +32.16 | +34.69 | finviz |
| `hot+short=high` | 275 | 34.9% | 0.2% | +26.64 | +24.62 | +27.12 | cross |
| `gap=down` | 945 | 34.6% | -0.1% | +43.69 | +41.66 | +44.27 | finviz |

## 3d — highest average return

| pattern | n | 3d hit | hit lift | 3d mean | mean lift | 3d xs | family |
|---|---:|---:|---:|---:|---:|---:|---|
| `blue+relvol=hot` | 46 | 56.5% | 21.8% | +263.42 | +261.39 | +263.85 | cross |
| `rsi=oversold` | 557 | 41.5% | 6.8% | +91.21 | +89.19 | +91.70 | finviz |
| `gap=down` | 945 | 34.6% | -0.1% | +43.69 | +41.66 | +44.27 | finviz |
| `perf_w=washed` | 1486 | 39.6% | 5.0% | +34.19 | +32.16 | +34.69 | finviz |
| `hot+short=high` | 275 | 34.9% | 0.2% | +26.64 | +24.62 | +27.12 | cross |
| `steady+blue` | 568 | 48.2% | 13.6% | +20.93 | +18.91 | +21.39 | stack |
| `relvol=hot` | 1787 | 33.7% | -0.9% | +20.63 | +18.61 | +21.22 | finviz |
| `blue` | 685 | 49.5% | 14.8% | +17.50 | +15.47 | +17.96 | mark |
| `blue+not_alarm` | 685 | 49.5% | 14.8% | +17.50 | +15.47 | +17.96 | mark |
| `blue+white` | 685 | 49.5% | 14.8% | +17.50 | +15.47 | +17.96 | mark |
| `gap=up` | 682 | 29.0% | -5.7% | +7.94 | +5.91 | +8.48 | finviz |
| `sma20=below` | 7910 | 42.9% | 8.2% | +6.96 | +4.94 | +7.44 | finviz |
| `short=high` | 5213 | 44.4% | 9.7% | +4.59 | +2.56 | +4.97 | finviz |
| `perf_w=extended` | 2793 | 34.3% | -0.4% | +3.79 | +1.77 | +4.31 | finviz |
| `alarm+not_white` | 4134 | 54.5% | 19.8% | +3.45 | +1.43 | +3.40 | mark |
| `alarm` | 4603 | 53.4% | 18.8% | +3.01 | +0.99 | +3.02 | mark |
| `cond=good` | 24795 | 32.1% | -2.6% | +2.19 | +0.16 | +2.81 | mark |
| `join=good` | 24998 | 32.3% | -2.4% | +2.18 | +0.16 | +2.80 | camera |
| `steady_daily` | 19792 | 30.7% | -4.0% | +1.97 | -0.05 | +2.62 | stack |
| `region=bad` | 1312 | 57.7% | 23.0% | +1.34 | -0.69 | +0.73 | mark |

## 1w — highest win rate

| pattern | n | 1w hit | hit lift | 1w mean | mean lift | 1w xs | family |
|---|---:|---:|---:|---:|---:|---:|---|
| `cond=bad` | 1 | 100.0% | 66.1% | +1.00 | -1.55 | +1.40 | mark |
| `alarm` | 1608 | 51.0% | 17.1% | +6.39 | +3.83 | +6.79 | mark |
| `alarm+not_white` | 1608 | 51.0% | 17.1% | +6.39 | +3.83 | +6.79 | mark |
| `blue` | 507 | 49.1% | 15.2% | +19.71 | +17.16 | +20.12 | mark |
| `blue+not_alarm` | 507 | 49.1% | 15.2% | +19.71 | +17.16 | +20.12 | mark |
| `blue+white` | 507 | 49.1% | 15.2% | +19.71 | +17.16 | +20.12 | mark |
| `steady+blue` | 418 | 48.8% | 14.9% | +23.81 | +21.26 | +24.21 | stack |
| `blue+relvol=hot` | 39 | 46.2% | 12.2% | +260.30 | +257.74 | +260.70 | cross |
| `sma20=below` | 5964 | 44.8% | 10.9% | +9.39 | +6.83 | +9.97 | finviz |
| `short=high` | 3050 | 40.8% | 6.9% | +5.96 | +3.40 | +6.54 | finviz |
| `rsi=oversold` | 441 | 40.8% | 6.9% | +104.08 | +101.53 | +104.67 | finviz |
| `perf_w=washed` | 1148 | 39.4% | 5.5% | +40.13 | +37.57 | +40.71 | finviz |
| `perf_w=extended` | 2240 | 37.8% | 3.9% | +4.80 | +2.24 | +5.38 | finviz |
| `gap=up` | 564 | 37.1% | 3.1% | +8.85 | +6.29 | +9.43 | finviz |
| `relvol=hot` | 1621 | 36.5% | 2.6% | +21.23 | +18.67 | +21.81 | finviz |
| `hot+short=high` | 192 | 36.5% | 2.5% | +34.11 | +31.56 | +34.70 | cross |
| `gap=down` | 849 | 36.3% | 2.4% | +41.87 | +39.32 | +42.45 | finviz |
| `rsi=overbought` | 1498 | 34.0% | 0.1% | -0.71 | -3.27 | -0.13 | finviz |
| `join=good` | 22811 | 33.9% | -0.1% | +2.56 | +0.00 | +3.14 | camera |
| `cond=good` | 22811 | 33.9% | -0.1% | +2.56 | +0.00 | +3.14 | mark |

## 1w — highest average return

| pattern | n | 1w hit | hit lift | 1w mean | mean lift | 1w xs | family |
|---|---:|---:|---:|---:|---:|---:|---|
| `blue+relvol=hot` | 39 | 46.2% | 12.2% | +260.30 | +257.74 | +260.70 | cross |
| `rsi=oversold` | 441 | 40.8% | 6.9% | +104.08 | +101.53 | +104.67 | finviz |
| `gap=down` | 849 | 36.3% | 2.4% | +41.87 | +39.32 | +42.45 | finviz |
| `perf_w=washed` | 1148 | 39.4% | 5.5% | +40.13 | +37.57 | +40.71 | finviz |
| `hot+short=high` | 192 | 36.5% | 2.5% | +34.11 | +31.56 | +34.70 | cross |
| `steady+blue` | 418 | 48.8% | 14.9% | +23.81 | +21.26 | +24.21 | stack |
| `relvol=hot` | 1621 | 36.5% | 2.6% | +21.23 | +18.67 | +21.81 | finviz |
| `blue` | 507 | 49.1% | 15.2% | +19.71 | +17.16 | +20.12 | mark |
| `blue+not_alarm` | 507 | 49.1% | 15.2% | +19.71 | +17.16 | +20.12 | mark |
| `blue+white` | 507 | 49.1% | 15.2% | +19.71 | +17.16 | +20.12 | mark |
| `sma20=below` | 5964 | 44.8% | 10.9% | +9.39 | +6.83 | +9.97 | finviz |
| `gap=up` | 564 | 37.1% | 3.1% | +8.85 | +6.29 | +9.43 | finviz |
| `alarm` | 1608 | 51.0% | 17.1% | +6.39 | +3.83 | +6.79 | mark |
| `alarm+not_white` | 1608 | 51.0% | 17.1% | +6.39 | +3.83 | +6.79 | mark |
| `short=high` | 3050 | 40.8% | 6.9% | +5.96 | +3.40 | +6.54 | finviz |
| `perf_w=extended` | 2240 | 37.8% | 3.9% | +4.80 | +2.24 | +5.38 | finviz |
| `join=good` | 22811 | 33.9% | -0.1% | +2.56 | +0.00 | +3.14 | camera |
| `cond=good` | 22811 | 33.9% | -0.1% | +2.56 | +0.00 | +3.14 | mark |
| `steady_daily` | 18213 | 32.6% | -1.3% | +2.45 | -0.10 | +3.05 | stack |
| `white+join=good` | 20182 | 31.4% | -2.5% | +1.61 | -0.94 | +2.20 | combo |

## 2w — highest win rate

_nothing cleared min_n._

## 2w — highest average return

_nothing cleared min_n._

## Cameras

| pattern | n | 1d hit | hit lift | 1d mean | mean lift | 1d xs | family |
|---|---:|---:|---:|---:|---:|---:|---|
| `join=good` | 26816 | 40.5% | -1.7% | +1.31 | +0.23 | +1.42 | camera |
| `ab=good` | 1668 | 64.6% | 22.4% | +1.00 | -0.08 | +0.28 | camera |
| `peer=good` | 922 | 65.8% | 23.7% | +1.01 | -0.07 | +0.30 | camera |
| `gen=bad` | 5353 | 45.9% | 3.8% | +0.10 | -0.99 | +0.14 | camera |

## Light combos

| pattern | n | 1d hit | hit lift | 1d mean | mean lift | 1d xs | family |
|---|---:|---:|---:|---:|---:|---:|---|
| `white+join=good` | 22236 | 38.5% | -3.6% | +1.10 | +0.02 | +1.23 | combo |

## Finviz buckets

| pattern | n | 1d hit | hit lift | 1d mean | mean lift | 1d xs | family |
|---|---:|---:|---:|---:|---:|---:|---|
| `rsi=oversold` | 664 | 42.5% | 0.3% | +35.87 | +34.79 | +35.95 | finviz |
| `rsi=overbought` | 2172 | 40.6% | -1.5% | -0.09 | -1.18 | -0.03 | finviz |
| `relvol=hot` | 2020 | 43.3% | 1.2% | +11.07 | +9.98 | +11.14 | finviz |
| `relvol=dead` | 19884 | 40.8% | -1.4% | +0.30 | -0.78 | +0.39 | finviz |
| `sma20=above` | 23519 | 41.1% | -1.0% | +0.44 | -0.64 | +0.53 | finviz |
| `sma20=below` | 10163 | 44.5% | 2.4% | +2.57 | +1.49 | +2.63 | finviz |
| `short=high` | 7373 | 46.1% | 4.0% | +1.62 | +0.54 | +1.67 | finviz |
| `gap=up` | 896 | 43.8% | 1.6% | +4.75 | +3.67 | +4.69 | finviz |
| `gap=down` | 1246 | 40.3% | -1.8% | +15.57 | +14.49 | +15.85 | finviz |
| `perf_w=extended` | 3220 | 46.6% | 4.4% | +3.27 | +2.19 | +3.33 | finviz |
| `perf_w=washed` | 1900 | 42.3% | 0.2% | +13.07 | +11.99 | +13.15 | finviz |

## Cross (lights × Finviz)

| pattern | n | 1d hit | hit lift | 1d mean | mean lift | 1d xs | family |
|---|---:|---:|---:|---:|---:|---:|---|
| `white+rsi=mid` | 20825 | 38.8% | -3.4% | +0.09 | -1.00 | +0.22 | cross |
| `hot+short=high` | 380 | 45.0% | 2.9% | +12.73 | +11.64 | +12.74 | cross |
| `hot+ab+peer` | 51 | 70.6% | 28.4% | +3.14 | +2.05 | +2.42 | cross |
| `blue+relvol=hot` | 180 | 62.2% | 20.1% | +74.67 | +73.58 | +74.21 | cross |
| `white+not_extended` | 19677 | 37.6% | -4.6% | -0.20 | -1.28 | -0.06 | cross |

## Quote-color card

_nothing cleared min_n._

## Insider clusters

| pattern | n | 1d hit | hit lift | 1d mean | mean lift | 1d xs | family |
|---|---:|---:|---:|---:|---:|---:|---|
| `ins_buy` | 176 | 48.3% | 6.2% | +0.16 | -0.93 | -0.04 | insider |
| `ins_sell` | 606 | 47.5% | 5.4% | -0.11 | -1.19 | -0.32 | insider |

## Peer RS

_nothing cleared min_n._

## Join rank

_nothing cleared min_n._

## AB delta

| pattern | n | 1d hit | hit lift | 1d mean | mean lift | 1d xs | family |
|---|---:|---:|---:|---:|---:|---:|---|
| `ab_up` | 574 | 63.4% | 21.3% | +0.92 | -0.17 | +0.20 | ab |

## Catalyst

_nothing cleared min_n._
