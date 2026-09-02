# Boring winners — 25-seat mined stacks

Daily-rebalanced long book from **FEATURE_MINE AND-stacks**, not the wide OR dump.
Equal-weight, close-to-close, clip ±30 on the book line. Per-name returns below are raw.

Fill order: `hot+ab+peer` → `ab AND peer` → `blue AND A` → `steady+blue` → `blue+white` → `A AND B` → `blue` → `join AND Band`.
Fade / first_crack vetoed. Hot names stay out except on `hot+ab+peer`. Sector cap 6. Max 25 — thin books are allowed.

A cameras only print from **2026-08-20**. Settled `1d` only through **2026-08-20**.
Current method = stock-book 1d BUY, graded on the same as-of panel (not the yfinance book backtest).

## Daily book returns

| date | stacks | n | mine 1d | book BUY 1d | uni 1d | 2d | 3d | 1w | W | L |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | `join_band` | 25 | +2.26 | +2.00 | +0.00 | +1.69 | +0.33 | -0.18 | 16 | 9 |
| 2026-08-14 | `steady_blue` | 17 | -2.28 | +0.52 | -0.24 | -3.67 | -2.32 | -1.93 | 5 | 12 |
| 2026-08-17 | `steady_blue+join_band` | 15 | -2.08 | -1.59 | -0.60 | -2.52 | -3.02 | — | 2 | 13 |
| 2026-08-18 | `join_band` | 16 | +1.35 | +2.07 | +0.88 | +0.32 | +0.33 | — | 13 | 3 |
| 2026-08-19 | `steady_blue+blue` | 25 | -0.95 | -0.78 | -0.98 | -0.11 | — | — | 7 | 17 |
| 2026-08-20 | `hot_ab_peer` | 25 | +2.09 | +1.10 | +0.72 | — | — | — | 16 | 8 |
| 2026-08-21 | `hot_ab_peer+steady_blue` | 22 | — | — | — | — | — | — | 0 | 0 |
| 2026-08-27 | `hot_ab_peer+ab_and_peer` | 24 | — | — | — | — | — | — | 0 | 0 |
| 2026-08-30 | `hot_ab_peer+ab_and_peer` | 23 | — | — | — | — | — | — | 0 | 0 |
| 2026-08-31 | `hot_ab_peer+ab_and_peer` | 25 | — | — | — | — | — | — | 0 | 0 |
| 2026-09-01 | `ab_and_peer` | 25 | — | — | — | — | — | — | 0 | 0 |

Mine book 1d: 6 priced days · p(loss day)=50.0% · mean=+0.07 · cum=+0.40.
Stock-book BUY 1d (same panel): 6 priced days · p(loss day)=33.3% · mean=+0.55 · cum=+3.33.
Mine names 1d: n=121 · p_win=48.8% · p_loss=51.2% · avg_win=+3.45 · avg_loss=-2.71 · mean=+0.29 · clip30=+0.29 · payoff=1.27.
Mine names 2d: n=96 · p_win=41.7% · p_loss=56.2% · avg_win=+72.51 · avg_loss=-3.88 · mean=+28.03 · clip30=-0.59 · payoff=18.70.

## Daily short book (inverse, −1 × clipped name return)

| date | rule | n | 1d | 2d | new | covered |
|---|---|---:|---:|---:|---:|---:|
| 2026-08-13 | `none` | 0 | — | — | 0 | 0 |
| 2026-08-14 | `none` | 0 | — | — | 0 | 0 |
| 2026-08-17 | `fade` | 25 | +0.68 | +0.27 | 25 | 0 |
| 2026-08-18 | `fade` | 6 | -0.81 | -2.66 | 6 | 25 |
| 2026-08-19 | `none` | 0 | — | — | 0 | 6 |
| 2026-08-20 | `A_bad` | 25 | -2.32 | — | 25 | 0 |
| 2026-08-21 | `fade` | 21 | — | — | 21 | 25 |
| 2026-08-27 | `fade` | 25 | — | — | 25 | 21 |
| 2026-08-30 | `fade` | 24 | — | — | 23 | 24 |
| 2026-08-31 | `fade` | 25 | — | — | 25 | 24 |
| 2026-09-01 | `fade` | 25 | — | — | 24 | 24 |

Short book 1d: 3 priced days · p(loss day)=66.7% · mean=-0.82 · cum=-2.45.

## Each day's stocks

One table per session. `buy` = new that morning, `hold` = still seated. `sell` rows are names dropped overnight (last seated returns).

### 2026-08-13 · `join_band` · n=25

no hit camera; `join=good AND short=high AND sma20=below`, not hot

Book 1d +2.26 · 2d +1.69 · 3d +0.33 · 1w -0.18 · stock-book BUY 1d +2.00 · universe med +0.00.

| # | action | Ticker | stack | sector | relvol | score | 1d | 2d | 3d | 1w |
|---:|---|---|---|---|---|---:|---:|---:|---:|---:|
| 1 | buy | AAP | `join_band` | Consumer Cyclical | normal | 8 | -1.41 | -1.12 | -0.70 | -26.10 |
| 2 | buy | ABX | `join_band` | Financial | normal | 8 | +8.80 | +7.04 | +5.75 | +7.51 |
| 3 | buy | AEO | `join_band` | Consumer Cyclical | normal | 8 | -0.80 | +0.31 | +0.31 | -1.84 |
| 4 | buy | AHCO | `join_band` | Healthcare | normal | 8 | +1.93 | +1.93 | +2.10 | -5.44 |
| 5 | buy | AIOT | `join_band` | Technology | normal | 8 | -0.95 | -8.28 | -4.78 | -3.82 |
| 6 | buy | AIRS | `join_band` | Healthcare | normal | 8 | +1.48 | -8.73 | -20.27 | -21.01 |
| 7 | buy | AISP | `join_band` | Technology | normal | 8 | +1.03 | +4.12 | +5.16 | +4.64 |
| 8 | buy | ALT | `join_band` | Healthcare | normal | 8 | +2.08 | +4.17 | +3.12 | +2.43 |
| 9 | buy | AMZE | `join_band` | Technology | normal | 8 | +18.93 | +2.43 | -1.94 | -7.28 |
| 10 | buy | AOSL | `join_band` | Technology | normal | 8 | +0.49 | +1.47 | -7.32 | -14.37 |
| 11 | buy | APEI | `join_band` | Consumer Defensive | normal | 8 | -0.02 | -2.06 | -1.22 | -1.88 |
| 12 | buy | ARDX | `join_band` | Healthcare | normal | 8 | +3.84 | +3.84 | +0.26 | -0.26 |
| 13 | buy | ARKO | `join_band` | Consumer Cyclical | normal | 8 | +7.78 | +7.31 | +4.22 | +2.00 |
| 14 | buy | ARQT | `join_band` | Healthcare | normal | 8 | +1.39 | -0.85 | +2.75 | -1.86 |
| 15 | buy | AUPH | `join_band` | Healthcare | normal | 8 | +1.91 | +0.72 | +0.79 | +13.77 |
| 16 | buy | BBBY | `join_band` | Consumer Cyclical | normal | 8 | -2.03 | — | — | — |
| 17 | buy | BKKT | `join_band` | Technology | normal | 8 | +3.38 | -3.78 | -6.35 | +1.35 |
| 18 | buy | BLBD | `join_band` | Industrials | normal | 8 | +0.22 | -1.22 | -4.96 | -5.17 |
| 19 | buy | BLND | `join_band` | Technology | normal | 8 | -3.87 | -4.52 | -6.45 | -5.81 |
| 20 | buy | BROS | `join_band` | Consumer Cyclical | normal | 8 | +1.74 | -1.39 | -5.26 | -3.44 |
| 21 | buy | BXBL | `join_band` | Consumer Cyclical | normal | 8 | -0.84 | -9.47 | -14.11 | -10.95 |
| 22 | buy | CDNL | `join_band` | Industrials | normal | 8 | +13.73 | +13.19 | +30.24 | +21.90 |
| 23 | buy | CISS | `join_band` | Industrials | normal | 8 | -3.66 | +2778.05 | +3265.85 | +3473.17 |
| 24 | buy | CLSK | `join_band` | Financial | normal | 8 | +4.95 | +7.64 | +1.82 | +9.38 |
| 25 | buy | COIN | `join_band` | Financial | normal | 8 | -3.53 | -2.18 | -4.98 | +11.99 |

Seats 1d n=25 · p_win=64.0% · p_loss=36.0% · avg_win=+4.61 · avg_loss=-1.90 · mean=+2.26 · clip30=+2.26 · payoff=2.42.

### 2026-08-14 · `steady_blue` · n=17

mined `steady+blue`, not hot

Book 1d -2.28 · 2d -3.67 · 3d -2.32 · 1w -1.93 · stock-book BUY 1d +0.52 · universe med -0.24.

| # | action | Ticker | stack | sector | relvol | score | 1d | 2d | 3d | 1w |
|---:|---|---|---|---|---|---:|---:|---:|---:|---:|
| 1 | buy | ANGI | `steady_blue` | Communication Services | normal | 11 | -5.51 | -1.91 | +1.70 | +4.24 |
| 2 | buy | BMBL | `steady_blue` | Communication Services | normal | 11 | -4.06 | +0.37 | +2.95 | +3.32 |
| 3 | buy | BORR | `steady_blue` | Energy | normal | 11 | +1.58 | +0.00 | -0.68 | +1.58 |
| 4 | buy | CCOI | `steady_blue` | Communication Services | normal | 11 | -5.14 | -10.61 | -8.49 | -18.92 |
| 5 | buy | DJT | `steady_blue` | Communication Services | normal | 11 | -2.06 | -2.54 | +0.97 | +10.04 |
| 6 | buy | FLNG | `steady_blue` | Energy | normal | 11 | +0.26 | +0.97 | +5.46 | +3.99 |
| 7 | buy | FVRR | `steady_blue` | Communication Services | normal | 11 | -3.00 | -1.55 | +3.00 | +2.22 |
| 8 | buy | GETY | `steady_blue` | Communication Services | normal | 11 | -10.39 | -12.01 | -13.64 | -14.29 |
| 9 | buy | IEP | `steady_blue` | Energy | normal | 11 | -4.18 | -7.95 | -10.38 | -8.49 |
| 10 | buy | IMPP | `steady_blue` | Energy | normal | 11 | +3.76 | +5.43 | +4.38 | +8.56 |
| 11 | buy | LB | `steady_blue` | Energy | normal | 11 | +1.02 | +2.71 | +4.46 | +14.00 |
| 12 | buy | NEXT | `steady_blue` | Energy | normal | 11 | -3.34 | -3.75 | -6.54 | -0.97 |
| 13 | buy | NEE | `steady_blue` | Utilities | normal | 10 | +0.04 | +0.04 | -0.33 | -2.95 |
| 14 | buy | NRG | `steady_blue` | Utilities | dead | 9 | -3.07 | -8.46 | -4.48 | -10.40 |
| 15 | buy | VST | `steady_blue` | Utilities | dead | 9 | -1.36 | -5.14 | -3.67 | -8.05 |
| 16 | buy | CEG | `steady_blue` | Utilities | dead | 8 | -1.67 | -5.55 | -2.95 | -3.40 |
| 17 | buy | TLN | `steady_blue` | Utilities | dead | 8 | -1.60 | -12.43 | -11.13 | -13.31 |
| — | sell | AAP | `join_band` | Consumer Cyclical | normal | 8 | -1.41 | -1.12 | -0.70 | -26.10 |
| — | sell | ABX | `join_band` | Financial | normal | 8 | +8.80 | +7.04 | +5.75 | +7.51 |
| — | sell | AEO | `join_band` | Consumer Cyclical | normal | 8 | -0.80 | +0.31 | +0.31 | -1.84 |
| — | sell | AHCO | `join_band` | Healthcare | normal | 8 | +1.93 | +1.93 | +2.10 | -5.44 |
| — | sell | AIOT | `join_band` | Technology | normal | 8 | -0.95 | -8.28 | -4.78 | -3.82 |
| — | sell | AIRS | `join_band` | Healthcare | normal | 8 | +1.48 | -8.73 | -20.27 | -21.01 |
| — | sell | AISP | `join_band` | Technology | normal | 8 | +1.03 | +4.12 | +5.16 | +4.64 |
| — | sell | ALT | `join_band` | Healthcare | normal | 8 | +2.08 | +4.17 | +3.12 | +2.43 |
| — | sell | AMZE | `join_band` | Technology | normal | 8 | +18.93 | +2.43 | -1.94 | -7.28 |
| — | sell | AOSL | `join_band` | Technology | normal | 8 | +0.49 | +1.47 | -7.32 | -14.37 |
| — | sell | APEI | `join_band` | Consumer Defensive | normal | 8 | -0.02 | -2.06 | -1.22 | -1.88 |
| — | sell | ARDX | `join_band` | Healthcare | normal | 8 | +3.84 | +3.84 | +0.26 | -0.26 |
| — | sell | ARKO | `join_band` | Consumer Cyclical | normal | 8 | +7.78 | +7.31 | +4.22 | +2.00 |
| — | sell | ARQT | `join_band` | Healthcare | normal | 8 | +1.39 | -0.85 | +2.75 | -1.86 |
| — | sell | AUPH | `join_band` | Healthcare | normal | 8 | +1.91 | +0.72 | +0.79 | +13.77 |
| — | sell | BBBY | `join_band` | Consumer Cyclical | normal | 8 | -2.03 | — | — | — |
| — | sell | BKKT | `join_band` | Technology | normal | 8 | +3.38 | -3.78 | -6.35 | +1.35 |
| — | sell | BLBD | `join_band` | Industrials | normal | 8 | +0.22 | -1.22 | -4.96 | -5.17 |
| — | sell | BLND | `join_band` | Technology | normal | 8 | -3.87 | -4.52 | -6.45 | -5.81 |
| — | sell | BROS | `join_band` | Consumer Cyclical | normal | 8 | +1.74 | -1.39 | -5.26 | -3.44 |
| — | sell | BXBL | `join_band` | Consumer Cyclical | normal | 8 | -0.84 | -9.47 | -14.11 | -10.95 |
| — | sell | CDNL | `join_band` | Industrials | normal | 8 | +13.73 | +13.19 | +30.24 | +21.90 |
| — | sell | CISS | `join_band` | Industrials | normal | 8 | -3.66 | +2778.05 | +3265.85 | +3473.17 |
| — | sell | CLSK | `join_band` | Financial | normal | 8 | +4.95 | +7.64 | +1.82 | +9.38 |
| — | sell | COIN | `join_band` | Financial | normal | 8 | -3.53 | -2.18 | -4.98 | +11.99 |

Seats 1d n=17 · p_win=29.4% · p_loss=70.6% · avg_win=+1.33 · avg_loss=-3.78 · mean=-2.28 · clip30=-2.28 · payoff=0.35.

### 2026-08-17 · `steady_blue+join_band` · n=15

mined `steady+blue`, not hot → no hit camera; `join=good AND short=high AND sma20=below`, not hot

Book 1d -2.08 · 2d -2.52 · 3d -3.02 · 1w — · stock-book BUY 1d -1.59 · universe med -0.60.

| # | action | Ticker | stack | sector | relvol | score | 1d | 2d | 3d | 1w |
|---:|---|---|---|---|---|---:|---:|---:|---:|---:|
| 1 | buy | UAMY | `steady_blue` | Basic Materials | normal | 11 | -7.47 | +1.96 | -6.23 | — |
| 2 | buy | AMAT | `steady_blue` | Technology | normal | 10 | -3.92 | -7.31 | -7.21 | — |
| 3 | buy | AVGO | `steady_blue` | Technology | normal | 10 | -3.17 | -7.63 | -7.24 | — |
| 4 | buy | ALB | `steady_blue` | Basic Materials | normal | 10 | -0.95 | +0.22 | +0.15 | — |
| 5 | buy | ALM | `steady_blue` | Basic Materials | normal | 10 | -4.64 | -1.10 | +8.13 | — |
| 6 | buy | AMRZ | `steady_blue` | Basic Materials | normal | 10 | -2.17 | -0.79 | -2.19 | — |
| 7 | buy | BAK | `steady_blue` | Basic Materials | normal | 10 | -1.99 | +0.00 | -1.49 | — |
| 8 | buy | CRML | `steady_blue` | Basic Materials | normal | 10 | -6.39 | -6.09 | -11.72 | — |
| 9 | buy | AMD | `steady_blue` | Technology | normal | 9 | -4.27 | -7.82 | -7.22 | — |
| 10 | buy | ASML | `steady_blue` | Technology | normal | 9 | -4.26 | -6.98 | -7.05 | — |
| 11 | buy | INTC | `steady_blue` | Technology | normal | 9 | -6.57 | -10.33 | -10.98 | — |
| 12 | buy | LRCX | `steady_blue` | Technology | normal | 9 | -4.63 | -10.66 | -9.69 | — |
| 13 | buy | AHR | `join_band` | Real Estate | normal | 8 | -0.47 | +0.95 | +1.99 | — |
| 14 | hold | BMBL | `join_band` | Communication Services | normal | 8 | +4.62 | +7.31 | +7.69 | — |
| 15 | buy | CDNL | `join_band` | Industrials | normal | 8 | +15.06 | +10.45 | +7.70 | — |
| — | sell | ANGI | `steady_blue` | Communication Services | normal | 11 | -5.51 | -1.91 | +1.70 | +4.24 |
| — | sell | BORR | `steady_blue` | Energy | normal | 11 | +1.58 | +0.00 | -0.68 | +1.58 |
| — | sell | CCOI | `steady_blue` | Communication Services | normal | 11 | -5.14 | -10.61 | -8.49 | -18.92 |
| — | sell | CEG | `steady_blue` | Utilities | dead | 8 | -1.67 | -5.55 | -2.95 | -3.40 |
| — | sell | DJT | `steady_blue` | Communication Services | normal | 11 | -2.06 | -2.54 | +0.97 | +10.04 |
| — | sell | FLNG | `steady_blue` | Energy | normal | 11 | +0.26 | +0.97 | +5.46 | +3.99 |
| — | sell | FVRR | `steady_blue` | Communication Services | normal | 11 | -3.00 | -1.55 | +3.00 | +2.22 |
| — | sell | GETY | `steady_blue` | Communication Services | normal | 11 | -10.39 | -12.01 | -13.64 | -14.29 |
| — | sell | IEP | `steady_blue` | Energy | normal | 11 | -4.18 | -7.95 | -10.38 | -8.49 |
| — | sell | IMPP | `steady_blue` | Energy | normal | 11 | +3.76 | +5.43 | +4.38 | +8.56 |
| — | sell | LB | `steady_blue` | Energy | normal | 11 | +1.02 | +2.71 | +4.46 | +14.00 |
| — | sell | NEE | `steady_blue` | Utilities | normal | 10 | +0.04 | +0.04 | -0.33 | -2.95 |
| — | sell | NEXT | `steady_blue` | Energy | normal | 11 | -3.34 | -3.75 | -6.54 | -0.97 |
| — | sell | NRG | `steady_blue` | Utilities | dead | 9 | -3.07 | -8.46 | -4.48 | -10.40 |
| — | sell | TLN | `steady_blue` | Utilities | dead | 8 | -1.60 | -12.43 | -11.13 | -13.31 |
| — | sell | VST | `steady_blue` | Utilities | dead | 9 | -1.36 | -5.14 | -3.67 | -8.05 |

Seats 1d n=15 · p_win=13.3% · p_loss=86.7% · avg_win=+9.84 · avg_loss=-3.92 · mean=-2.08 · clip30=-2.08 · payoff=2.51.

### 2026-08-18 · `join_band` · n=16

no hit camera; `join=good AND short=high AND sma20=below`, not hot

Book 1d +1.35 · 2d +0.32 · 3d +0.33 · 1w — · stock-book BUY 1d +2.07 · universe med +0.88.

| # | action | Ticker | stack | sector | relvol | score | 1d | 2d | 3d | 1w |
|---:|---|---|---|---|---|---:|---:|---:|---:|---:|
| 1 | buy | EVRG | `join_band` | Utilities | dead | 5 | +0.19 | -0.31 | -3.00 | — |
| 2 | buy | EXEL | `join_band` | Healthcare | dead | 5 | +2.39 | +1.07 | +1.81 | — |
| 3 | buy | LNT | `join_band` | Utilities | dead | 5 | +0.24 | -0.74 | -3.03 | — |
| 4 | buy | LNTH | `join_band` | Healthcare | dead | 5 | +0.22 | -0.02 | -0.29 | — |
| 5 | buy | PBH | `join_band` | Healthcare | dead | 5 | +2.19 | -0.10 | +1.46 | — |
| 6 | buy | PNW | `join_band` | Utilities | dead | 5 | -0.72 | -0.77 | -2.79 | — |
| 7 | buy | AOS | `join_band` | Industrials | dead | 3 | +4.83 | +2.58 | +3.65 | — |
| 8 | buy | DPZ | `join_band` | Consumer Cyclical | normal | 3 | +0.08 | -0.56 | +1.67 | — |
| 9 | buy | LULU | `join_band` | Consumer Cyclical | normal | 3 | +0.37 | -2.79 | +1.73 | — |
| 10 | buy | BF-B | `join_band` | Consumer Defensive | dead | 1 | +4.71 | +4.64 | +4.75 | — |
| 11 | buy | COCO | `join_band` | Consumer Defensive | dead | 1 | +6.34 | +2.56 | -0.30 | — |
| 12 | buy | KMB | `join_band` | Consumer Defensive | dead | 1 | +2.00 | +0.90 | +1.48 | — |
| 13 | buy | RRC | `join_band` | Energy | dead | 1 | +1.15 | +2.03 | +3.01 | — |
| 14 | buy | OZK | `join_band` | Financial | normal | 1 | -2.48 | -2.88 | -2.60 | — |
| 15 | buy | FHB | `join_band` | Financial | dead | 0 | -1.65 | -2.24 | -3.31 | — |
| 16 | buy | TROW | `join_band` | Financial | dead | 0 | +1.73 | +1.70 | +1.08 | — |
| — | sell | AHR | `join_band` | Real Estate | normal | 8 | -0.47 | +0.95 | +1.99 | — |
| — | sell | ALB | `steady_blue` | Basic Materials | normal | 10 | -0.95 | +0.22 | +0.15 | — |
| — | sell | ALM | `steady_blue` | Basic Materials | normal | 10 | -4.64 | -1.10 | +8.13 | — |
| — | sell | AMAT | `steady_blue` | Technology | normal | 10 | -3.92 | -7.31 | -7.21 | — |
| — | sell | AMD | `steady_blue` | Technology | normal | 9 | -4.27 | -7.82 | -7.22 | — |
| — | sell | AMRZ | `steady_blue` | Basic Materials | normal | 10 | -2.17 | -0.79 | -2.19 | — |
| — | sell | ASML | `steady_blue` | Technology | normal | 9 | -4.26 | -6.98 | -7.05 | — |
| — | sell | AVGO | `steady_blue` | Technology | normal | 10 | -3.17 | -7.63 | -7.24 | — |
| — | sell | BAK | `steady_blue` | Basic Materials | normal | 10 | -1.99 | +0.00 | -1.49 | — |
| — | sell | BMBL | `join_band` | Communication Services | normal | 8 | +4.62 | +7.31 | +7.69 | — |
| — | sell | CDNL | `join_band` | Industrials | normal | 8 | +15.06 | +10.45 | +7.70 | — |
| — | sell | CRML | `steady_blue` | Basic Materials | normal | 10 | -6.39 | -6.09 | -11.72 | — |
| — | sell | INTC | `steady_blue` | Technology | normal | 9 | -6.57 | -10.33 | -10.98 | — |
| — | sell | LRCX | `steady_blue` | Technology | normal | 9 | -4.63 | -10.66 | -9.69 | — |
| — | sell | UAMY | `steady_blue` | Basic Materials | normal | 11 | -7.47 | +1.96 | -6.23 | — |

Seats 1d n=16 · p_win=81.2% · p_loss=18.8% · avg_win=+2.04 · avg_loss=-1.62 · mean=+1.35 · clip30=+1.35 · payoff=1.26.

### 2026-08-19 · `steady_blue+blue` · n=25

mined `steady+blue`, not hot → blue, not hot

Book 1d -0.95 · 2d -0.11 · 3d — · 1w — · stock-book BUY 1d -0.78 · universe med -0.98.

| # | action | Ticker | stack | sector | relvol | score | 1d | 2d | 3d | 1w |
|---:|---|---|---|---|---|---:|---:|---:|---:|---:|
| 1 | buy | NEE | `steady_blue` | Utilities | normal | 8 | -1.02 | -2.63 | — | — |
| 2 | buy | LOGI | `blue` | Technology | normal | 6 | -5.11 | -4.42 | — | — |
| 3 | buy | WB | `blue` | Communication Services | normal | 6 | -4.00 | -6.14 | — | — |
| 4 | buy | ARM | `blue` | Technology | normal | 5 | +0.55 | -2.41 | — | — |
| 5 | buy | CHTR | `blue` | Communication Services | normal | 5 | -3.09 | -1.51 | — | — |
| 6 | buy | JD | `blue` | Consumer Cyclical | normal | 5 | +0.20 | +0.07 | — | — |
| 7 | buy | LEN | `blue` | Consumer Cyclical | normal | 5 | -2.15 | -0.32 | — | — |
| 8 | buy | MAR | `blue` | Consumer Cyclical | normal | 5 | -0.73 | -0.79 | — | — |
| 9 | buy | MCD | `blue` | Consumer Cyclical | normal | 5 | +0.63 | +1.31 | — | — |
| 10 | buy | SWKS | `blue` | Technology | normal | 5 | -0.79 | -1.96 | — | — |
| 11 | buy | TCOM | `blue` | Consumer Cyclical | normal | 5 | -0.17 | -0.86 | — | — |
| 12 | buy | ABNB | `blue` | Consumer Cyclical | normal | 4 | -0.75 | +0.49 | — | — |
| 13 | buy | ADBE | `blue` | Technology | normal | 4 | -0.09 | +1.04 | — | — |
| 14 | buy | AEM | `blue` | Basic Materials | normal | 4 | +2.08 | +4.01 | — | — |
| 15 | buy | ASML | `blue` | Technology | normal | 4 | -0.08 | +0.69 | — | — |
| 16 | buy | DOCU | `blue` | Technology | normal | 4 | +0.27 | -0.18 | — | — |
| 17 | buy | NEM | `blue` | Basic Materials | normal | 4 | +2.05 | +5.20 | — | — |
| 18 | buy | NWSA | `blue` | Communication Services | normal | 4 | +0.68 | +3.16 | — | — |
| 19 | buy | SKM | `blue` | Communication Services | normal | 4 | -1.11 | +4.00 | — | — |
| 20 | buy | AAL | `blue` | Industrials | dead | 4 | -2.45 | -0.29 | — | — |
| 21 | buy | ALK | `blue` | Industrials | dead | 4 | -2.55 | -1.99 | — | — |
| 22 | buy | AXP | `blue` | Financial | dead | 4 | -2.57 | -1.15 | — | — |
| 23 | buy | NMR | `blue` | Financial | dead | 4 | -0.63 | +2.21 | — | — |
| 24 | buy | VMC | `blue` | Basic Materials | dead | 4 | -2.04 | -0.09 | — | — |
| 25 | buy | CRCL | `blue` | Financial | normal | 4 | — | — | — | — |
| — | sell | AOS | `join_band` | Industrials | dead | 3 | +4.83 | +2.58 | +3.65 | — |
| — | sell | BF-B | `join_band` | Consumer Defensive | dead | 1 | +4.71 | +4.64 | +4.75 | — |
| — | sell | COCO | `join_band` | Consumer Defensive | dead | 1 | +6.34 | +2.56 | -0.30 | — |
| — | sell | DPZ | `join_band` | Consumer Cyclical | normal | 3 | +0.08 | -0.56 | +1.67 | — |
| — | sell | EVRG | `join_band` | Utilities | dead | 5 | +0.19 | -0.31 | -3.00 | — |
| — | sell | EXEL | `join_band` | Healthcare | dead | 5 | +2.39 | +1.07 | +1.81 | — |
| — | sell | FHB | `join_band` | Financial | dead | 0 | -1.65 | -2.24 | -3.31 | — |
| — | sell | KMB | `join_band` | Consumer Defensive | dead | 1 | +2.00 | +0.90 | +1.48 | — |
| — | sell | LNT | `join_band` | Utilities | dead | 5 | +0.24 | -0.74 | -3.03 | — |
| — | sell | LNTH | `join_band` | Healthcare | dead | 5 | +0.22 | -0.02 | -0.29 | — |
| — | sell | LULU | `join_band` | Consumer Cyclical | normal | 3 | +0.37 | -2.79 | +1.73 | — |
| — | sell | OZK | `join_band` | Financial | normal | 1 | -2.48 | -2.88 | -2.60 | — |
| — | sell | PBH | `join_band` | Healthcare | dead | 5 | +2.19 | -0.10 | +1.46 | — |
| — | sell | PNW | `join_band` | Utilities | dead | 5 | -0.72 | -0.77 | -2.79 | — |
| — | sell | RRC | `join_band` | Energy | dead | 1 | +1.15 | +2.03 | +3.01 | — |
| — | sell | TROW | `join_band` | Financial | dead | 0 | +1.73 | +1.70 | +1.08 | — |

Seats 1d n=24 · p_win=29.2% · p_loss=70.8% · avg_win=+0.92 · avg_loss=-1.73 · mean=-0.95 · clip30=-0.95 · payoff=0.54.

### 2026-08-20 · `hot_ab_peer` · n=25

mined `hot+ab+peer` (70.6% 1d hit)

Book 1d +2.09 · 2d — · 3d — · 1w — · stock-book BUY 1d +1.10 · universe med +0.72.

| # | action | Ticker | stack | sector | relvol | score | 1d | 2d | 3d | 1w |
|---:|---|---|---|---|---|---:|---:|---:|---:|---:|
| 1 | buy | BMEA | `hot_ab_peer` | Healthcare | hot | 23 | +0.73 | — | — | — |
| 2 | buy | CRSP | `hot_ab_peer` | Healthcare | hot | 23 | +2.37 | — | — | — |
| 3 | buy | PUK | `hot_ab_peer` | Financial | hot | 23 | +2.14 | — | — | — |
| 4 | buy | UBS | `hot_ab_peer` | Financial | hot | 23 | +0.17 | — | — | — |
| 5 | buy | KC | `hot_ab_peer` | Technology | hot | 22 | -2.45 | — | — | — |
| 6 | buy | TEM | `hot_ab_peer` | Healthcare | hot | 22 | +9.06 | — | — | — |
| 7 | buy | WBS | `hot_ab_peer` | Financial | hot | 22 | — | — | — | — |
| 8 | buy | DUOT | `hot_ab_peer` | Technology | hot | 21 | -0.86 | — | — | — |
| 9 | buy | ABTC | `hot_ab_peer` | Financial | hot | 21 | -6.38 | — | — | — |
| 10 | buy | ABUS | `hot_ab_peer` | Healthcare | hot | 21 | +9.22 | — | — | — |
| 11 | buy | ALEC | `hot_ab_peer` | Healthcare | hot | 21 | +4.42 | — | — | — |
| 12 | buy | CABA | `hot_ab_peer` | Healthcare | hot | 21 | -1.88 | — | — | — |
| 13 | buy | EL | `hot_ab_peer` | Consumer Defensive | hot | 21 | +6.02 | — | — | — |
| 14 | buy | PPC | `hot_ab_peer` | Consumer Defensive | hot | 21 | +3.23 | — | — | — |
| 15 | buy | TGT | `hot_ab_peer` | Consumer Defensive | hot | 21 | +4.54 | — | — | — |
| 16 | buy | DUOL | `hot_ab_peer` | Technology | hot | 20 | -0.61 | — | — | — |
| 17 | buy | IAG | `hot_ab_peer` | Basic Materials | hot | 20 | +3.12 | — | — | — |
| 18 | buy | KGC | `hot_ab_peer` | Basic Materials | hot | 20 | +4.23 | — | — | — |
| 19 | buy | WPM | `hot_ab_peer` | Basic Materials | hot | 20 | +5.01 | — | — | — |
| 20 | buy | LTC | `hot_ab_peer` | Real Estate | hot | 20 | -0.71 | — | — | — |
| 21 | buy | MSTR | `hot_ab_peer` | Technology | hot | 19 | +6.10 | — | — | — |
| 22 | buy | COIN | `hot_ab_peer` | Financial | hot | 19 | +8.20 | — | — | — |
| 23 | hold | AEM | `hot_ab_peer` | Basic Materials | hot | 18 | +1.90 | — | — | — |
| 24 | buy | AXTI | `hot_ab_peer` | Technology | hot | 18 | -3.25 | — | — | — |
| 25 | buy | BTBT | `hot_ab_peer` | Financial | hot | 18 | -4.08 | — | — | — |
| — | sell | AAL | `blue` | Industrials | dead | 4 | -2.45 | -0.29 | — | — |
| — | sell | ABNB | `blue` | Consumer Cyclical | normal | 4 | -0.75 | +0.49 | — | — |
| — | sell | ADBE | `blue` | Technology | normal | 4 | -0.09 | +1.04 | — | — |
| — | sell | ALK | `blue` | Industrials | dead | 4 | -2.55 | -1.99 | — | — |
| — | sell | ARM | `blue` | Technology | normal | 5 | +0.55 | -2.41 | — | — |
| — | sell | ASML | `blue` | Technology | normal | 4 | -0.08 | +0.69 | — | — |
| — | sell | AXP | `blue` | Financial | dead | 4 | -2.57 | -1.15 | — | — |
| — | sell | CHTR | `blue` | Communication Services | normal | 5 | -3.09 | -1.51 | — | — |
| — | sell | CRCL | `blue` | Financial | normal | 4 | — | — | — | — |
| — | sell | DOCU | `blue` | Technology | normal | 4 | +0.27 | -0.18 | — | — |
| — | sell | JD | `blue` | Consumer Cyclical | normal | 5 | +0.20 | +0.07 | — | — |
| — | sell | LEN | `blue` | Consumer Cyclical | normal | 5 | -2.15 | -0.32 | — | — |
| — | sell | LOGI | `blue` | Technology | normal | 6 | -5.11 | -4.42 | — | — |
| — | sell | MAR | `blue` | Consumer Cyclical | normal | 5 | -0.73 | -0.79 | — | — |
| — | sell | MCD | `blue` | Consumer Cyclical | normal | 5 | +0.63 | +1.31 | — | — |
| — | sell | NEE | `steady_blue` | Utilities | normal | 8 | -1.02 | -2.63 | — | — |
| — | sell | NEM | `blue` | Basic Materials | normal | 4 | +2.05 | +5.20 | — | — |
| — | sell | NMR | `blue` | Financial | dead | 4 | -0.63 | +2.21 | — | — |
| — | sell | NWSA | `blue` | Communication Services | normal | 4 | +0.68 | +3.16 | — | — |
| — | sell | SKM | `blue` | Communication Services | normal | 4 | -1.11 | +4.00 | — | — |
| — | sell | SWKS | `blue` | Technology | normal | 5 | -0.79 | -1.96 | — | — |
| — | sell | TCOM | `blue` | Consumer Cyclical | normal | 5 | -0.17 | -0.86 | — | — |
| — | sell | VMC | `blue` | Basic Materials | dead | 4 | -2.04 | -0.09 | — | — |
| — | sell | WB | `blue` | Communication Services | normal | 6 | -4.00 | -6.14 | — | — |

Seats 1d n=24 · p_win=66.7% · p_loss=33.3% · avg_win=+4.41 · avg_loss=-2.53 · mean=+2.09 · clip30=+2.09 · payoff=1.74.

### 2026-08-21 · `hot_ab_peer+steady_blue` · n=22

mined `hot+ab+peer` (70.6% 1d hit) → mined `steady+blue`, not hot

Book 1d — · 2d — · 3d — · 1w — · stock-book BUY 1d — · universe med —.

| # | action | Ticker | stack | sector | relvol | score | 1d | 2d | 3d | 1w |
|---:|---|---|---|---|---|---:|---:|---:|---:|---:|
| 1 | buy | ALVO | `hot_ab_peer` | Healthcare | hot | 23 | — | — | — | — |
| 2 | buy | MT | `hot_ab_peer` | Basic Materials | hot | 23 | — | — | — | — |
| 3 | buy | VIRT | `hot_ab_peer` | Financial | hot | 23 | — | — | — | — |
| 4 | buy | AKBA | `hot_ab_peer` | Healthcare | hot | 22 | — | — | — | — |
| 5 | buy | AZN | `hot_ab_peer` | Healthcare | hot | 22 | — | — | — | — |
| 6 | hold | CRSP | `hot_ab_peer` | Healthcare | hot | 22 | — | — | — | — |
| 7 | buy | FUTU | `hot_ab_peer` | Financial | hot | 22 | — | — | — | — |
| 8 | buy | PCG | `hot_ab_peer` | Utilities | hot | 22 | — | — | — | — |
| 9 | buy | SUZ | `hot_ab_peer` | Basic Materials | hot | 22 | — | — | — | — |
| 10 | buy | AMLX | `hot_ab_peer` | Healthcare | hot | 21 | — | — | — | — |
| 11 | buy | ARCT | `hot_ab_peer` | Healthcare | hot | 21 | — | — | — | — |
| 12 | hold | COIN | `hot_ab_peer` | Financial | hot | 21 | — | — | — | — |
| 13 | buy | DE | `hot_ab_peer` | Industrials | hot | 18 | — | — | — | — |
| 14 | buy | BTDR | `hot_ab_peer` | Technology | hot | 16 | — | — | — | — |
| 15 | buy | ABEV | `hot_ab_peer` | Consumer Defensive | hot | 15 | — | — | — | — |
| 16 | buy | CSAN | `hot_ab_peer` | Energy | hot | 15 | — | — | — | — |
| 17 | buy | TRON | `hot_ab_peer` | Consumer Cyclical | hot | 15 | — | — | — | — |
| 18 | buy | UPXI | `hot_ab_peer` | Communication Services | hot | 15 | — | — | — | — |
| 19 | buy | BYND | `hot_ab_peer` | Consumer Defensive | hot | 14 | — | — | — | — |
| 20 | buy | CNH | `hot_ab_peer` | Industrials | hot | 14 | — | — | — | — |
| 21 | buy | AGRO | `hot_ab_peer` | Consumer Defensive | hot | 12 | — | — | — | — |
| 22 | buy | SOFI | `steady_blue` | Financial | normal | 19 | — | — | — | — |
| — | sell | ABTC | `hot_ab_peer` | Financial | hot | 21 | -6.38 | — | — | — |
| — | sell | ABUS | `hot_ab_peer` | Healthcare | hot | 21 | +9.22 | — | — | — |
| — | sell | AEM | `hot_ab_peer` | Basic Materials | hot | 18 | +1.90 | — | — | — |
| — | sell | ALEC | `hot_ab_peer` | Healthcare | hot | 21 | +4.42 | — | — | — |
| — | sell | AXTI | `hot_ab_peer` | Technology | hot | 18 | -3.25 | — | — | — |
| — | sell | BMEA | `hot_ab_peer` | Healthcare | hot | 23 | +0.73 | — | — | — |
| — | sell | BTBT | `hot_ab_peer` | Financial | hot | 18 | -4.08 | — | — | — |
| — | sell | CABA | `hot_ab_peer` | Healthcare | hot | 21 | -1.88 | — | — | — |
| — | sell | DUOL | `hot_ab_peer` | Technology | hot | 20 | -0.61 | — | — | — |
| — | sell | DUOT | `hot_ab_peer` | Technology | hot | 21 | -0.86 | — | — | — |
| — | sell | EL | `hot_ab_peer` | Consumer Defensive | hot | 21 | +6.02 | — | — | — |
| — | sell | IAG | `hot_ab_peer` | Basic Materials | hot | 20 | +3.12 | — | — | — |
| — | sell | KC | `hot_ab_peer` | Technology | hot | 22 | -2.45 | — | — | — |
| — | sell | KGC | `hot_ab_peer` | Basic Materials | hot | 20 | +4.23 | — | — | — |
| — | sell | LTC | `hot_ab_peer` | Real Estate | hot | 20 | -0.71 | — | — | — |
| — | sell | MSTR | `hot_ab_peer` | Technology | hot | 19 | +6.10 | — | — | — |
| — | sell | PPC | `hot_ab_peer` | Consumer Defensive | hot | 21 | +3.23 | — | — | — |
| — | sell | PUK | `hot_ab_peer` | Financial | hot | 23 | +2.14 | — | — | — |
| — | sell | TEM | `hot_ab_peer` | Healthcare | hot | 22 | +9.06 | — | — | — |
| — | sell | TGT | `hot_ab_peer` | Consumer Defensive | hot | 21 | +4.54 | — | — | — |
| — | sell | UBS | `hot_ab_peer` | Financial | hot | 23 | +0.17 | — | — | — |
| — | sell | WBS | `hot_ab_peer` | Financial | hot | 22 | — | — | — | — |
| — | sell | WPM | `hot_ab_peer` | Basic Materials | hot | 20 | +5.01 | — | — | — |

1d not settled — names only.

### 2026-08-27 · `hot_ab_peer+ab_and_peer` · n=24

mined `hot+ab+peer` (70.6% 1d hit) → mined `ab=good AND peer=good`

Book 1d — · 2d — · 3d — · 1w — · stock-book BUY 1d — · universe med —.

| # | action | Ticker | stack | sector | relvol | score | 1d | 2d | 3d | 1w |
|---:|---|---|---|---|---|---:|---:|---:|---:|---:|
| 1 | buy | BKKT | `hot_ab_peer` | Technology | hot | 21 | — | — | — | — |
| 2 | hold | BTDR | `hot_ab_peer` | Technology | hot | 21 | — | — | — | — |
| 3 | buy | G | `hot_ab_peer` | Technology | hot | 21 | — | — | — | — |
| 4 | buy | ROST | `hot_ab_peer` | Consumer Cyclical | hot | 21 | — | — | — | — |
| 5 | buy | SBS | `hot_ab_peer` | Utilities | hot | 19 | — | — | — | — |
| 6 | hold | TRON | `hot_ab_peer` | Consumer Cyclical | hot | 19 | — | — | — | — |
| 7 | buy | BJ | `hot_ab_peer` | Consumer Defensive | hot | 18 | — | — | — | — |
| 8 | buy | CRML | `hot_ab_peer` | Basic Materials | hot | 18 | — | — | — | — |
| 9 | buy | DSX | `hot_ab_peer` | Industrials | hot | 17 | — | — | — | — |
| 10 | buy | LAR | `hot_ab_peer` | Basic Materials | hot | 17 | — | — | — | — |
| 11 | buy | TECK | `hot_ab_peer` | Basic Materials | hot | 17 | — | — | — | — |
| 12 | buy | BTBT | `hot_ab_peer` | Financial | hot | 15 | — | — | — | — |
| 13 | hold | UPXI | `hot_ab_peer` | Communication Services | hot | 15 | — | — | — | — |
| 14 | buy | RYAAY | `hot_ab_peer` | Industrials | hot | 15 | — | — | — | — |
| 15 | buy | DFDV | `hot_ab_peer` | Financial | hot | 14 | — | — | — | — |
| 16 | buy | ALT | `hot_ab_peer` | Healthcare | hot | 13 | — | — | — | — |
| 17 | hold | ARCT | `hot_ab_peer` | Healthcare | hot | 13 | — | — | — | — |
| 18 | buy | IOVA | `hot_ab_peer` | Healthcare | hot | 13 | — | — | — | — |
| 19 | buy | NVAX | `hot_ab_peer` | Healthcare | hot | 13 | — | — | — | — |
| 20 | buy | RZLT | `hot_ab_peer` | Healthcare | hot | 13 | — | — | — | — |
| 21 | buy | TEM | `hot_ab_peer` | Healthcare | hot | 13 | — | — | — | — |
| 22 | buy | KD | `ab_and_peer` | Technology | normal | 20 | — | — | — | — |
| 23 | buy | VYX | `ab_and_peer` | Technology | normal | 20 | — | — | — | — |
| 24 | buy | ANET | `ab_and_peer` | Technology | normal | 19 | — | — | — | — |
| — | sell | ABEV | `hot_ab_peer` | Consumer Defensive | hot | 15 | — | — | — | — |
| — | sell | AGRO | `hot_ab_peer` | Consumer Defensive | hot | 12 | — | — | — | — |
| — | sell | AKBA | `hot_ab_peer` | Healthcare | hot | 22 | — | — | — | — |
| — | sell | ALVO | `hot_ab_peer` | Healthcare | hot | 23 | — | — | — | — |
| — | sell | AMLX | `hot_ab_peer` | Healthcare | hot | 21 | — | — | — | — |
| — | sell | AZN | `hot_ab_peer` | Healthcare | hot | 22 | — | — | — | — |
| — | sell | BYND | `hot_ab_peer` | Consumer Defensive | hot | 14 | — | — | — | — |
| — | sell | CNH | `hot_ab_peer` | Industrials | hot | 14 | — | — | — | — |
| — | sell | COIN | `hot_ab_peer` | Financial | hot | 21 | — | — | — | — |
| — | sell | CRSP | `hot_ab_peer` | Healthcare | hot | 22 | — | — | — | — |
| — | sell | CSAN | `hot_ab_peer` | Energy | hot | 15 | — | — | — | — |
| — | sell | DE | `hot_ab_peer` | Industrials | hot | 18 | — | — | — | — |
| — | sell | FUTU | `hot_ab_peer` | Financial | hot | 22 | — | — | — | — |
| — | sell | MT | `hot_ab_peer` | Basic Materials | hot | 23 | — | — | — | — |
| — | sell | PCG | `hot_ab_peer` | Utilities | hot | 22 | — | — | — | — |
| — | sell | SOFI | `steady_blue` | Financial | normal | 19 | — | — | — | — |
| — | sell | SUZ | `hot_ab_peer` | Basic Materials | hot | 22 | — | — | — | — |
| — | sell | VIRT | `hot_ab_peer` | Financial | hot | 23 | — | — | — | — |

1d not settled — names only.

### 2026-08-30 · `hot_ab_peer+ab_and_peer` · n=23

mined `hot+ab+peer` (70.6% 1d hit) → mined `ab=good AND peer=good`

Book 1d — · 2d — · 3d — · 1w — · stock-book BUY 1d — · universe med —.

| # | action | Ticker | stack | sector | relvol | score | 1d | 2d | 3d | 1w |
|---:|---|---|---|---|---|---:|---:|---:|---:|---:|
| 1 | buy | BOX | `hot_ab_peer` | Technology | hot | 20 | — | — | — | — |
| 2 | buy | DTM | `hot_ab_peer` | Energy | hot | 18 | — | — | — | — |
| 3 | buy | SMTC | `hot_ab_peer` | Technology | hot | 17 | — | — | — | — |
| 4 | buy | TRMD | `hot_ab_peer` | Energy | hot | 17 | — | — | — | — |
| 5 | buy | BZ | `hot_ab_peer` | Communication Services | hot | 16 | — | — | — | — |
| 6 | buy | EDU | `hot_ab_peer` | Consumer Defensive | hot | 16 | — | — | — | — |
| 7 | hold | ARCT | `hot_ab_peer` | Healthcare | hot | 14 | — | — | — | — |
| 8 | buy | BMEA | `hot_ab_peer` | Healthcare | hot | 14 | — | — | — | — |
| 9 | buy | GDRX | `hot_ab_peer` | Healthcare | hot | 14 | — | — | — | — |
| 10 | buy | ANF | `hot_ab_peer` | Consumer Cyclical | hot | 12 | — | — | — | — |
| 11 | hold | CRML | `hot_ab_peer` | Basic Materials | hot | 12 | — | — | — | — |
| 12 | buy | URBN | `hot_ab_peer` | Consumer Cyclical | hot | 12 | — | — | — | — |
| 13 | buy | MATV | `hot_ab_peer` | Basic Materials | hot | 11 | — | — | — | — |
| 14 | buy | UPBD | `ab_and_peer` | Technology | normal | 20 | — | — | — | — |
| 15 | buy | CRWD | `ab_and_peer` | Technology | normal | 19 | — | — | — | — |
| 16 | buy | BRZE | `ab_and_peer` | Technology | normal | 19 | — | — | — | — |
| 17 | buy | CRCT | `ab_and_peer` | Technology | normal | 19 | — | — | — | — |
| 18 | buy | AGNC | `ab_and_peer` | Real Estate | normal | 18 | — | — | — | — |
| 19 | buy | ES | `ab_and_peer` | Utilities | normal | 18 | — | — | — | — |
| 20 | buy | GLPI | `ab_and_peer` | Real Estate | normal | 18 | — | — | — | — |
| 21 | hold | SBS | `ab_and_peer` | Utilities | normal | 18 | — | — | — | — |
| 22 | buy | ZIP | `ab_and_peer` | Communication Services | normal | 18 | — | — | — | — |
| 23 | buy | ATHM | `ab_and_peer` | Communication Services | normal | 17 | — | — | — | — |
| — | sell | ALT | `hot_ab_peer` | Healthcare | hot | 13 | — | — | — | — |
| — | sell | ANET | `ab_and_peer` | Technology | normal | 19 | — | — | — | — |
| — | sell | BJ | `hot_ab_peer` | Consumer Defensive | hot | 18 | — | — | — | — |
| — | sell | BKKT | `hot_ab_peer` | Technology | hot | 21 | — | — | — | — |
| — | sell | BTBT | `hot_ab_peer` | Financial | hot | 15 | — | — | — | — |
| — | sell | BTDR | `hot_ab_peer` | Technology | hot | 21 | — | — | — | — |
| — | sell | DFDV | `hot_ab_peer` | Financial | hot | 14 | — | — | — | — |
| — | sell | DSX | `hot_ab_peer` | Industrials | hot | 17 | — | — | — | — |
| — | sell | G | `hot_ab_peer` | Technology | hot | 21 | — | — | — | — |
| — | sell | IOVA | `hot_ab_peer` | Healthcare | hot | 13 | — | — | — | — |
| — | sell | KD | `ab_and_peer` | Technology | normal | 20 | — | — | — | — |
| — | sell | LAR | `hot_ab_peer` | Basic Materials | hot | 17 | — | — | — | — |
| — | sell | NVAX | `hot_ab_peer` | Healthcare | hot | 13 | — | — | — | — |
| — | sell | ROST | `hot_ab_peer` | Consumer Cyclical | hot | 21 | — | — | — | — |
| — | sell | RYAAY | `hot_ab_peer` | Industrials | hot | 15 | — | — | — | — |
| — | sell | RZLT | `hot_ab_peer` | Healthcare | hot | 13 | — | — | — | — |
| — | sell | TECK | `hot_ab_peer` | Basic Materials | hot | 17 | — | — | — | — |
| — | sell | TEM | `hot_ab_peer` | Healthcare | hot | 13 | — | — | — | — |
| — | sell | TRON | `hot_ab_peer` | Consumer Cyclical | hot | 19 | — | — | — | — |
| — | sell | UPXI | `hot_ab_peer` | Communication Services | hot | 15 | — | — | — | — |
| — | sell | VYX | `ab_and_peer` | Technology | normal | 20 | — | — | — | — |

1d not settled — names only.

### 2026-08-31 · `hot_ab_peer+ab_and_peer` · n=25

mined `hot+ab+peer` (70.6% 1d hit) → mined `ab=good AND peer=good`

Book 1d — · 2d — · 3d — · 1w — · stock-book BUY 1d — · universe med —.

| # | action | Ticker | stack | sector | relvol | score | 1d | 2d | 3d | 1w |
|---:|---|---|---|---|---|---:|---:|---:|---:|---:|
| 1 | buy | CRM | `hot_ab_peer` | Technology | hot | 19 | — | — | — | — |
| 2 | buy | AFRM | `hot_ab_peer` | Financial | hot | 17 | — | — | — | — |
| 3 | buy | ESI | `hot_ab_peer` | Basic Materials | hot | 17 | — | — | — | — |
| 4 | buy | TENB | `hot_ab_peer` | Technology | hot | 17 | — | — | — | — |
| 5 | buy | ESTC | `hot_ab_peer` | Technology | hot | 16 | — | — | — | — |
| 6 | buy | PDFS | `hot_ab_peer` | Technology | hot | 16 | — | — | — | — |
| 7 | buy | DFDV | `hot_ab_peer` | Financial | hot | 16 | — | — | — | — |
| 8 | buy | PD | `hot_ab_peer` | Technology | hot | 16 | — | — | — | — |
| 9 | hold | BZ | `hot_ab_peer` | Communication Services | hot | 15 | — | — | — | — |
| 10 | buy | UPXI | `hot_ab_peer` | Communication Services | hot | 15 | — | — | — | — |
| 11 | buy | BEKE | `hot_ab_peer` | Real Estate | hot | 14 | — | — | — | — |
| 12 | hold | ARCT | `hot_ab_peer` | Healthcare | hot | 14 | — | — | — | — |
| 13 | hold | SBS | `hot_ab_peer` | Utilities | hot | 14 | — | — | — | — |
| 14 | buy | ULTA | `hot_ab_peer` | Consumer Cyclical | hot | 14 | — | — | — | — |
| 15 | buy | SGI | `hot_ab_peer` | Consumer Cyclical | hot | 12 | — | — | — | — |
| 16 | buy | BMO | `ab_and_peer` | Financial | normal | 17 | — | — | — | — |
| 17 | buy | RWAY | `ab_and_peer` | Financial | normal | 17 | — | — | — | — |
| 18 | buy | CVE | `ab_and_peer` | Energy | normal | 16 | — | — | — | — |
| 19 | buy | CVX | `ab_and_peer` | Energy | normal | 16 | — | — | — | — |
| 20 | buy | COST | `ab_and_peer` | Consumer Defensive | normal | 16 | — | — | — | — |
| 21 | hold | DTM | `ab_and_peer` | Energy | normal | 16 | — | — | — | — |
| 22 | buy | AMZN | `ab_and_peer` | Consumer Cyclical | normal | 15 | — | — | — | — |
| 23 | buy | TCBI | `ab_and_peer` | Financial | normal | 15 | — | — | — | — |
| 24 | buy | VOD | `ab_and_peer` | Communication Services | dead | 15 | — | — | — | — |
| 25 | buy | VTS | `ab_and_peer` | Energy | normal | 15 | — | — | — | — |
| — | sell | AGNC | `ab_and_peer` | Real Estate | normal | 18 | — | — | — | — |
| — | sell | ANF | `hot_ab_peer` | Consumer Cyclical | hot | 12 | — | — | — | — |
| — | sell | ATHM | `ab_and_peer` | Communication Services | normal | 17 | — | — | — | — |
| — | sell | BMEA | `hot_ab_peer` | Healthcare | hot | 14 | — | — | — | — |
| — | sell | BOX | `hot_ab_peer` | Technology | hot | 20 | — | — | — | — |
| — | sell | BRZE | `ab_and_peer` | Technology | normal | 19 | — | — | — | — |
| — | sell | CRCT | `ab_and_peer` | Technology | normal | 19 | — | — | — | — |
| — | sell | CRML | `hot_ab_peer` | Basic Materials | hot | 12 | — | — | — | — |
| — | sell | CRWD | `ab_and_peer` | Technology | normal | 19 | — | — | — | — |
| — | sell | EDU | `hot_ab_peer` | Consumer Defensive | hot | 16 | — | — | — | — |
| — | sell | ES | `ab_and_peer` | Utilities | normal | 18 | — | — | — | — |
| — | sell | GDRX | `hot_ab_peer` | Healthcare | hot | 14 | — | — | — | — |
| — | sell | GLPI | `ab_and_peer` | Real Estate | normal | 18 | — | — | — | — |
| — | sell | MATV | `hot_ab_peer` | Basic Materials | hot | 11 | — | — | — | — |
| — | sell | SMTC | `hot_ab_peer` | Technology | hot | 17 | — | — | — | — |
| — | sell | TRMD | `hot_ab_peer` | Energy | hot | 17 | — | — | — | — |
| — | sell | UPBD | `ab_and_peer` | Technology | normal | 20 | — | — | — | — |
| — | sell | URBN | `hot_ab_peer` | Consumer Cyclical | hot | 12 | — | — | — | — |
| — | sell | ZIP | `ab_and_peer` | Communication Services | normal | 18 | — | — | — | — |

1d not settled — names only.

### 2026-09-01 · `ab_and_peer` · n=25

mined `ab=good AND peer=good`

Book 1d — · 2d — · 3d — · 1w — · stock-book BUY 1d — · universe med —.

| # | action | Ticker | stack | sector | relvol | score | 1d | 2d | 3d | 1w |
|---:|---|---|---|---|---|---:|---:|---:|---:|---:|
| 1 | buy | IEP | `ab_and_peer` | Energy | dead | 17 | — | — | — | — |
| 2 | buy | KMI | `ab_and_peer` | Energy | dead | 16 | — | — | — | — |
| 3 | buy | DHT | `ab_and_peer` | Energy | dead | 16 | — | — | — | — |
| 4 | buy | LBRT | `ab_and_peer` | Energy | dead | 16 | — | — | — | — |
| 5 | buy | ASX | `ab_and_peer` | Technology | dead | 16 | — | — | — | — |
| 6 | buy | KEYS | `ab_and_peer` | Technology | dead | 16 | — | — | — | — |
| 7 | hold | DTM | `ab_and_peer` | Energy | dead | 16 | — | — | — | — |
| 8 | buy | CRK | `ab_and_peer` | Energy | dead | 16 | — | — | — | — |
| 9 | buy | CGNX | `ab_and_peer` | Technology | dead | 16 | — | — | — | — |
| 10 | buy | TEL | `ab_and_peer` | Technology | dead | 16 | — | — | — | — |
| 11 | buy | MU | `ab_and_peer` | Technology | dead | 15 | — | — | — | — |
| 12 | buy | ALAB | `ab_and_peer` | Technology | dead | 15 | — | — | — | — |
| 13 | buy | CTGO | `ab_and_peer` | Basic Materials | dead | 15 | — | — | — | — |
| 14 | buy | ANNX | `ab_and_peer` | Healthcare | dead | 15 | — | — | — | — |
| 15 | buy | ADMA | `ab_and_peer` | Healthcare | dead | 15 | — | — | — | — |
| 16 | buy | ALLO | `ab_and_peer` | Healthcare | dead | 15 | — | — | — | — |
| 17 | buy | COGT | `ab_and_peer` | Healthcare | dead | 15 | — | — | — | — |
| 18 | buy | FULC | `ab_and_peer` | Healthcare | dead | 15 | — | — | — | — |
| 19 | buy | INDV | `ab_and_peer` | Healthcare | dead | 15 | — | — | — | — |
| 20 | buy | ODFL | `ab_and_peer` | Industrials | dead | 14 | — | — | — | — |
| 21 | buy | OSK | `ab_and_peer` | Industrials | dead | 14 | — | — | — | — |
| 22 | buy | NEM | `ab_and_peer` | Basic Materials | dead | 13 | — | — | — | — |
| 23 | buy | DE | `ab_and_peer` | Industrials | dead | 13 | — | — | — | — |
| 24 | buy | NVT | `ab_and_peer` | Industrials | dead | 13 | — | — | — | — |
| 25 | buy | FNV | `ab_and_peer` | Basic Materials | dead | 13 | — | — | — | — |
| — | sell | AFRM | `hot_ab_peer` | Financial | hot | 17 | — | — | — | — |
| — | sell | AMZN | `ab_and_peer` | Consumer Cyclical | normal | 15 | — | — | — | — |
| — | sell | ARCT | `hot_ab_peer` | Healthcare | hot | 14 | — | — | — | — |
| — | sell | BEKE | `hot_ab_peer` | Real Estate | hot | 14 | — | — | — | — |
| — | sell | BMO | `ab_and_peer` | Financial | normal | 17 | — | — | — | — |
| — | sell | BZ | `hot_ab_peer` | Communication Services | hot | 15 | — | — | — | — |
| — | sell | COST | `ab_and_peer` | Consumer Defensive | normal | 16 | — | — | — | — |
| — | sell | CRM | `hot_ab_peer` | Technology | hot | 19 | — | — | — | — |
| — | sell | CVE | `ab_and_peer` | Energy | normal | 16 | — | — | — | — |
| — | sell | CVX | `ab_and_peer` | Energy | normal | 16 | — | — | — | — |
| — | sell | DFDV | `hot_ab_peer` | Financial | hot | 16 | — | — | — | — |
| — | sell | ESI | `hot_ab_peer` | Basic Materials | hot | 17 | — | — | — | — |
| — | sell | ESTC | `hot_ab_peer` | Technology | hot | 16 | — | — | — | — |
| — | sell | PD | `hot_ab_peer` | Technology | hot | 16 | — | — | — | — |
| — | sell | PDFS | `hot_ab_peer` | Technology | hot | 16 | — | — | — | — |
| — | sell | RWAY | `ab_and_peer` | Financial | normal | 17 | — | — | — | — |
| — | sell | SBS | `hot_ab_peer` | Utilities | hot | 14 | — | — | — | — |
| — | sell | SGI | `hot_ab_peer` | Consumer Cyclical | hot | 12 | — | — | — | — |
| — | sell | TCBI | `ab_and_peer` | Financial | normal | 15 | — | — | — | — |
| — | sell | TENB | `hot_ab_peer` | Technology | hot | 17 | — | — | — | — |
| — | sell | ULTA | `hot_ab_peer` | Consumer Cyclical | hot | 14 | — | — | — | — |
| — | sell | UPXI | `hot_ab_peer` | Communication Services | hot | 15 | — | — | — | — |
| — | sell | VOD | `ab_and_peer` | Communication Services | dead | 15 | — | — | — | — |
| — | sell | VTS | `ab_and_peer` | Energy | normal | 15 | — | — | — | — |

1d not settled — names only.

## Notes

1. Board `ab=good` / `peer=good` hit-rates are almost entirely the A-camera window (from 20 Aug) and one broad up day. AND them; do not OR-dump 1,800 names.
2. `blue+relvol=hot` and `rsi=oversold` print huge means because of squeezes. They are not the boring book.
3. `join_band` is a last resort on mornings with no hit camera. It will track the tape.
4. Re-score after 21 Aug / 27 Aug / 30 Aug 1d settles.

