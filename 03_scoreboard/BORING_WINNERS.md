# Boring winners — 25-seat book

Daily-rebalanced long book from the FEATURE_MINE high-n edges. Equal-weight, close-to-close.
The short sleeve is the inverse filter (fade / bad A), disjoint from the longs. Empty when those cameras did not print.

**Long:** Hit A = `ab=good` OR `peer=good`. Scale B = `short=high` OR `sma20=below`. Blue overlay. Fade / first_crack vetoed.
**Seats:** 25 long + 25 short. **Sector cap:** 6. Score = `3·blue + 2·ab + 2·peer + 1·short_high + 1·sma_below + 1·ab_up`.

A cameras only print from **2026-08-20**. Before that the live rule falls through to blue, then `short AND sma20=below`.
Settled `1d` only through **2026-08-20**. Later sessions have names, no close-to-close yet.

## Daily book returns

Equal-weight, clip ±30. Tickers and per-name 1d/2d/3d/1w are in the ledger below and in `boring_winners_picks.csv`.

| date | rule | n | 1d | 2d | 3d | 1w | W | L | bought | sold | held |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | `Band` | 25 | +0.28 | -2.92 | -3.88 | -5.00 | 12 | 12 | 25 | 0 | 0 |
| 2026-08-14 | `blue` | 17 | -2.26 | -3.95 | -1.99 | -1.88 | 5 | 11 | 17 | 25 | 0 |
| 2026-08-17 | `blue` | 12 | -3.21 | -3.67 | -4.34 | — | 2 | 10 | 12 | 17 | 0 |
| 2026-08-18 | `Band` | 25 | +2.15 | -0.50 | -0.65 | — | 17 | 7 | 25 | 12 | 0 |
| 2026-08-19 | `blue` | 25 | -1.67 | -1.24 | — | — | 4 | 21 | 25 | 25 | 0 |
| 2026-08-20 | `A` | 25 | +1.89 | — | — | — | 15 | 10 | 24 | 24 | 1 |
| 2026-08-21 | `A` | 25 | — | — | — | — | 0 | 0 | 24 | 24 | 1 |
| 2026-08-27 | `A` | 25 | — | — | — | — | 0 | 0 | 25 | 25 | 0 |
| 2026-08-30 | `A` | 25 | — | — | — | — | 0 | 0 | 23 | 23 | 2 |
| 2026-08-31 | `A` | 25 | — | — | — | — | 0 | 0 | 24 | 24 | 1 |
| 2026-09-01 | `A` | 25 | — | — | — | — | 0 | 0 | 25 | 25 | 0 |

Long book 1d: 6 priced days · p(loss day)=50.0% · mean=-0.47 · cum=-2.83 · avg win day=+1.44 · avg loss day=-2.38.
Long book 2d: 5 priced days · p(loss day)=100.0% · mean=-2.46 · cum=-12.29.
Long names 1d: n=127 · p_win=43.3% · p_loss=55.9% · avg_win=+4.34 · avg_loss=-3.49 · mean=-0.07 · clip30=-0.10 · payoff=1.24.
Long names 2d: n=100 · p_win=32.0% · p_loss=67.0% · avg_win=+3.01 · avg_loss=-4.90 · mean=-2.32 · clip30=-2.19 · payoff=0.61.

## Daily short book (inverse, −1 × clipped name return)

Shorts only when fade / first_crack printed, or when A cameras printed bad. Empty otherwise.

| date | rule | n | 1d | 2d | new | covered |
|---|---|---:|---:|---:|---:|---:|
| 2026-08-13 | `none` | 0 | — | — | 0 | 0 |
| 2026-08-14 | `none` | 0 | — | — | 0 | 0 |
| 2026-08-17 | `fade` | 25 | +0.45 | +0.07 | 25 | 0 |
| 2026-08-18 | `fade` | 6 | -0.81 | -2.66 | 6 | 25 |
| 2026-08-19 | `none` | 0 | — | — | 0 | 6 |
| 2026-08-20 | `A_bad` | 25 | -2.13 | — | 25 | 0 |
| 2026-08-21 | `fade` | 21 | — | — | 21 | 25 |
| 2026-08-27 | `fade` | 25 | — | — | 25 | 21 |
| 2026-08-30 | `fade` | 24 | — | — | 23 | 24 |
| 2026-08-31 | `fade` | 25 | — | — | 25 | 24 |
| 2026-09-01 | `fade` | 25 | — | — | 24 | 24 |

Short book 1d: 3 priced days · p(loss day)=66.7% · mean=-0.83 · cum=-2.49.

## Each stock bought / held / sold

Long seats only. `buy` = new that morning, `hold` = still in the book, `sell` = dropped at the next rebalance (returns are the last seated close-to-close). Shorts are in the daily short table and the CSV (`side=sell`).

| date | action | Ticker | sector | score | 1d | 2d | 3d | 1w |
|---|---|---|---|---:|---:|---:|---:|---:|
| 2026-08-13 | buy | ABEO | Healthcare | 2 | -5.67 | -6.46 | -2.99 | -6.14 |
| 2026-08-13 | buy | ANDG | Consumer Cyclical | 2 | +1.22 | +1.45 | -5.28 | +4.18 |
| 2026-08-13 | buy | ATRA | Healthcare | 2 | -4.93 | -1.76 | +2.47 | +10.22 |
| 2026-08-13 | buy | BETR | Financial | 2 | -8.47 | -9.73 | -13.00 | -22.67 |
| 2026-08-13 | buy | BW | Industrials | 2 | +10.05 | +6.10 | -2.25 | -9.84 |
| 2026-08-13 | buy | BYND | Consumer Defensive | 2 | — | — | — | — |
| 2026-08-13 | buy | CRIS | Healthcare | 2 | -3.74 | — | — | — |
| 2026-08-13 | buy | CVRX | Healthcare | 2 | +4.76 | -6.46 | -6.46 | -3.40 |
| 2026-08-13 | buy | CYN | Technology | 2 | +4.22 | -0.47 | -0.47 | +0.47 |
| 2026-08-13 | buy | DJCO | Technology | 2 | +0.35 | -0.87 | -0.11 | +2.24 |
| 2026-08-13 | buy | EMPD | Consumer Cyclical | 2 | +2.12 | -1.06 | -1.06 | +0.71 |
| 2026-08-13 | buy | EVTL | Industrials | 2 | -1.07 | -4.30 | -7.40 | -10.50 |
| 2026-08-13 | buy | FBIO | Healthcare | 2 | -1.68 | -5.37 | -2.35 | -7.05 |
| 2026-08-13 | buy | GSUN | Consumer Defensive | 2 | +7.46 | — | — | — |
| 2026-08-13 | buy | IMDX | Healthcare | 2 | +16.44 | +15.09 | +15.36 | +15.90 |
| 2026-08-13 | buy | KSCP | Industrials | 2 | -6.17 | -11.73 | -11.11 | -12.35 |
| 2026-08-13 | buy | LESL | Consumer Cyclical | 2 | -16.23 | -14.80 | -18.18 | -20.78 |
| 2026-08-13 | buy | MVIS | Technology | 2 | -40.58 | -43.24 | -52.26 | -53.58 |
| 2026-08-13 | buy | ONON | Consumer Cyclical | 2 | +1.99 | -0.89 | -1.77 | -5.35 |
| 2026-08-13 | buy | OPEN | Real Estate | 2 | -0.27 | -3.01 | -7.95 | -4.93 |
| 2026-08-13 | buy | TPR | Consumer Cyclical | 2 | +0.46 | +0.49 | +3.01 | +1.11 |
| 2026-08-13 | buy | VWAV | Industrials | 2 | +44.72 | +16.26 | +8.13 | -12.20 |
| 2026-08-13 | buy | YETI | Consumer Cyclical | 2 | -2.00 | -5.43 | -4.90 | -3.23 |
| 2026-08-13 | buy | ABX | Financial | 2 | +8.80 | +7.04 | +5.75 | +7.51 |
| 2026-08-13 | buy | AIOT | Technology | 2 | -0.95 | -8.28 | -4.78 | -3.82 |
| 2026-08-14 | buy | GLND | Energy | 5 | +0.00 | +1.56 | +10.16 | +16.41 |
| 2026-08-14 | buy | NCMI | Communication Services | 5 | -4.54 | -11.89 | -7.69 | -11.19 |
| 2026-08-14 | buy | STUB | Communication Services | 5 | -11.88 | -11.76 | -14.11 | -17.33 |
| 2026-08-14 | buy | ANGI | Communication Services | 5 | -5.51 | -1.91 | +1.70 | +4.24 |
| 2026-08-14 | buy | BMBL | Communication Services | 5 | -4.06 | +0.37 | +2.95 | +3.32 |
| 2026-08-14 | buy | BORR | Energy | 5 | +1.58 | +0.00 | -0.68 | +1.58 |
| 2026-08-14 | buy | CCOI | Communication Services | 5 | -5.14 | -10.61 | -8.49 | -18.92 |
| 2026-08-14 | buy | DJT | Communication Services | 5 | -2.06 | -2.54 | +0.97 | +10.04 |
| 2026-08-14 | buy | FLNG | Energy | 5 | +0.26 | +0.97 | +5.46 | +3.99 |
| 2026-08-14 | buy | IEP | Energy | 5 | -4.18 | -7.95 | -10.38 | -8.49 |
| 2026-08-14 | buy | IMPP | Energy | 5 | +3.76 | +5.43 | +4.38 | +8.56 |
| 2026-08-14 | buy | LB | Energy | 5 | +1.02 | +2.71 | +4.46 | +14.00 |
| 2026-08-14 | buy | NEE | Utilities | 4 | +0.04 | +0.04 | -0.33 | -2.95 |
| 2026-08-14 | buy | NRG | Utilities | 4 | -3.07 | -8.46 | -4.48 | -10.40 |
| 2026-08-14 | buy | VST | Utilities | 4 | -1.36 | -5.14 | -3.67 | -8.05 |
| 2026-08-14 | buy | CEG | Utilities | 3 | -1.67 | -5.55 | -2.95 | -3.40 |
| 2026-08-14 | buy | TLN | Utilities | 3 | -1.60 | -12.43 | -11.13 | -13.31 |
| 2026-08-14 | sell | ABEO | Healthcare | 2 | -5.67 | -6.46 | -2.99 | -6.14 |
| 2026-08-14 | sell | ABX | Financial | 2 | +8.80 | +7.04 | +5.75 | +7.51 |
| 2026-08-14 | sell | AIOT | Technology | 2 | -0.95 | -8.28 | -4.78 | -3.82 |
| 2026-08-14 | sell | ANDG | Consumer Cyclical | 2 | +1.22 | +1.45 | -5.28 | +4.18 |
| 2026-08-14 | sell | ATRA | Healthcare | 2 | -4.93 | -1.76 | +2.47 | +10.22 |
| 2026-08-14 | sell | BETR | Financial | 2 | -8.47 | -9.73 | -13.00 | -22.67 |
| 2026-08-14 | sell | BW | Industrials | 2 | +10.05 | +6.10 | -2.25 | -9.84 |
| 2026-08-14 | sell | BYND | Consumer Defensive | 2 | — | — | — | — |
| 2026-08-14 | sell | CRIS | Healthcare | 2 | -3.74 | — | — | — |
| 2026-08-14 | sell | CVRX | Healthcare | 2 | +4.76 | -6.46 | -6.46 | -3.40 |
| 2026-08-14 | sell | CYN | Technology | 2 | +4.22 | -0.47 | -0.47 | +0.47 |
| 2026-08-14 | sell | DJCO | Technology | 2 | +0.35 | -0.87 | -0.11 | +2.24 |
| 2026-08-14 | sell | EMPD | Consumer Cyclical | 2 | +2.12 | -1.06 | -1.06 | +0.71 |
| 2026-08-14 | sell | EVTL | Industrials | 2 | -1.07 | -4.30 | -7.40 | -10.50 |
| 2026-08-14 | sell | FBIO | Healthcare | 2 | -1.68 | -5.37 | -2.35 | -7.05 |
| 2026-08-14 | sell | GSUN | Consumer Defensive | 2 | +7.46 | — | — | — |
| 2026-08-14 | sell | IMDX | Healthcare | 2 | +16.44 | +15.09 | +15.36 | +15.90 |
| 2026-08-14 | sell | KSCP | Industrials | 2 | -6.17 | -11.73 | -11.11 | -12.35 |
| 2026-08-14 | sell | LESL | Consumer Cyclical | 2 | -16.23 | -14.80 | -18.18 | -20.78 |
| 2026-08-14 | sell | MVIS | Technology | 2 | -40.58 | -43.24 | -52.26 | -53.58 |
| 2026-08-14 | sell | ONON | Consumer Cyclical | 2 | +1.99 | -0.89 | -1.77 | -5.35 |
| 2026-08-14 | sell | OPEN | Real Estate | 2 | -0.27 | -3.01 | -7.95 | -4.93 |
| 2026-08-14 | sell | TPR | Consumer Cyclical | 2 | +0.46 | +0.49 | +3.01 | +1.11 |
| 2026-08-14 | sell | VWAV | Industrials | 2 | +44.72 | +16.26 | +8.13 | -12.20 |
| 2026-08-14 | sell | YETI | Consumer Cyclical | 2 | -2.00 | -5.43 | -4.90 | -3.23 |
| 2026-08-17 | buy | UAMY | Basic Materials | 5 | -7.47 | +1.96 | -6.23 | — |
| 2026-08-17 | buy | ALTO | Basic Materials | 5 | +0.47 | -0.71 | -0.95 | — |
| 2026-08-17 | buy | CC | Basic Materials | 5 | -1.91 | +1.21 | -0.19 | — |
| 2026-08-17 | buy | FMC | Basic Materials | 5 | -0.59 | +6.19 | +6.88 | — |
| 2026-08-17 | buy | GPRE | Basic Materials | 5 | +0.25 | -0.68 | -0.74 | — |
| 2026-08-17 | buy | HUN | Basic Materials | 5 | -2.52 | -1.26 | -1.46 | — |
| 2026-08-17 | buy | AMAT | Technology | 4 | -3.92 | -7.31 | -7.21 | — |
| 2026-08-17 | buy | AVGO | Technology | 4 | -3.17 | -7.63 | -7.24 | — |
| 2026-08-17 | buy | AMD | Technology | 3 | -4.27 | -7.82 | -7.22 | — |
| 2026-08-17 | buy | ASML | Technology | 3 | -4.26 | -6.98 | -7.05 | — |
| 2026-08-17 | buy | INTC | Technology | 3 | -6.57 | -10.33 | -10.98 | — |
| 2026-08-17 | buy | LRCX | Technology | 3 | -4.63 | -10.66 | -9.69 | — |
| 2026-08-17 | sell | ANGI | Communication Services | 5 | -5.51 | -1.91 | +1.70 | +4.24 |
| 2026-08-17 | sell | BMBL | Communication Services | 5 | -4.06 | +0.37 | +2.95 | +3.32 |
| 2026-08-17 | sell | BORR | Energy | 5 | +1.58 | +0.00 | -0.68 | +1.58 |
| 2026-08-17 | sell | CCOI | Communication Services | 5 | -5.14 | -10.61 | -8.49 | -18.92 |
| 2026-08-17 | sell | CEG | Utilities | 3 | -1.67 | -5.55 | -2.95 | -3.40 |
| 2026-08-17 | sell | DJT | Communication Services | 5 | -2.06 | -2.54 | +0.97 | +10.04 |
| 2026-08-17 | sell | FLNG | Energy | 5 | +0.26 | +0.97 | +5.46 | +3.99 |
| 2026-08-17 | sell | GLND | Energy | 5 | +0.00 | +1.56 | +10.16 | +16.41 |
| 2026-08-17 | sell | IEP | Energy | 5 | -4.18 | -7.95 | -10.38 | -8.49 |
| 2026-08-17 | sell | IMPP | Energy | 5 | +3.76 | +5.43 | +4.38 | +8.56 |
| 2026-08-17 | sell | LB | Energy | 5 | +1.02 | +2.71 | +4.46 | +14.00 |
| 2026-08-17 | sell | NCMI | Communication Services | 5 | -4.54 | -11.89 | -7.69 | -11.19 |
| 2026-08-17 | sell | NEE | Utilities | 4 | +0.04 | +0.04 | -0.33 | -2.95 |
| 2026-08-17 | sell | NRG | Utilities | 4 | -3.07 | -8.46 | -4.48 | -10.40 |
| 2026-08-17 | sell | STUB | Communication Services | 5 | -11.88 | -11.76 | -14.11 | -17.33 |
| 2026-08-17 | sell | TLN | Utilities | 3 | -1.60 | -12.43 | -11.13 | -13.31 |
| 2026-08-17 | sell | VST | Utilities | 4 | -1.36 | -5.14 | -3.67 | -8.05 |
| 2026-08-18 | buy | AOS | Industrials | 2 | +4.83 | +2.58 | +3.65 | — |
| 2026-08-18 | buy | BF-B | Consumer Defensive | 2 | +4.71 | +4.64 | +4.75 | — |
| 2026-08-18 | buy | COCO | Consumer Defensive | 2 | +6.34 | +2.56 | -0.30 | — |
| 2026-08-18 | buy | EVRG | Utilities | 2 | +0.19 | -0.31 | -3.00 | — |
| 2026-08-18 | buy | EXEL | Healthcare | 2 | +2.39 | +1.07 | +1.81 | — |
| 2026-08-18 | buy | KMB | Consumer Defensive | 2 | +2.00 | +0.90 | +1.48 | — |
| 2026-08-18 | buy | LNT | Utilities | 2 | +0.24 | -0.74 | -3.03 | — |
| 2026-08-18 | buy | LNTH | Healthcare | 2 | +0.22 | -0.02 | -0.29 | — |
| 2026-08-18 | buy | PBH | Healthcare | 2 | +2.19 | -0.10 | +1.46 | — |
| 2026-08-18 | buy | PNW | Utilities | 2 | -0.72 | -0.77 | -2.79 | — |
| 2026-08-18 | buy | RRC | Energy | 2 | +1.15 | +2.03 | +3.01 | — |
| 2026-08-18 | buy | TAP | Consumer Defensive | 2 | +3.31 | +3.60 | +4.40 | — |
| 2026-08-18 | buy | BKH | Utilities | 2 | +0.06 | +0.19 | -2.00 | — |
| 2026-08-18 | buy | CALM | Consumer Defensive | 2 | -0.79 | +1.53 | +1.98 | — |
| 2026-08-18 | buy | JBS | Consumer Defensive | 2 | +0.66 | -0.81 | +1.17 | — |
| 2026-08-18 | buy | CAPR | Healthcare | 2 | +12.71 | -3.39 | -11.16 | — |
| 2026-08-18 | buy | ENVX | Industrials | 2 | +8.45 | +4.31 | +7.82 | — |
| 2026-08-18 | buy | EVTL | Industrials | 2 | -1.29 | -3.35 | -1.16 | — |
| 2026-08-18 | buy | EYPT | Healthcare | 2 | +15.17 | -1.28 | -2.74 | — |
| 2026-08-18 | buy | IEP | Energy | 2 | -2.63 | -2.63 | -0.59 | — |
| 2026-08-18 | buy | OTLK | Healthcare | 2 | -1.43 | -3.43 | -7.57 | — |
| 2026-08-18 | buy | SERV | Industrials | 2 | — | — | — | — |
| 2026-08-18 | buy | YSS | Industrials | 2 | -2.30 | -11.71 | -10.56 | — |
| 2026-08-18 | buy | CDNL | Industrials | 2 | -4.01 | -6.40 | -3.63 | — |
| 2026-08-18 | buy | DPZ | Consumer Cyclical | 2 | +0.08 | -0.56 | +1.67 | — |
| 2026-08-18 | sell | ALTO | Basic Materials | 5 | +0.47 | -0.71 | -0.95 | — |
| 2026-08-18 | sell | AMAT | Technology | 4 | -3.92 | -7.31 | -7.21 | — |
| 2026-08-18 | sell | AMD | Technology | 3 | -4.27 | -7.82 | -7.22 | — |
| 2026-08-18 | sell | ASML | Technology | 3 | -4.26 | -6.98 | -7.05 | — |
| 2026-08-18 | sell | AVGO | Technology | 4 | -3.17 | -7.63 | -7.24 | — |
| 2026-08-18 | sell | CC | Basic Materials | 5 | -1.91 | +1.21 | -0.19 | — |
| 2026-08-18 | sell | FMC | Basic Materials | 5 | -0.59 | +6.19 | +6.88 | — |
| 2026-08-18 | sell | GPRE | Basic Materials | 5 | +0.25 | -0.68 | -0.74 | — |
| 2026-08-18 | sell | HUN | Basic Materials | 5 | -2.52 | -1.26 | -1.46 | — |
| 2026-08-18 | sell | INTC | Technology | 3 | -6.57 | -10.33 | -10.98 | — |
| 2026-08-18 | sell | LRCX | Technology | 3 | -4.63 | -10.66 | -9.69 | — |
| 2026-08-18 | sell | UAMY | Basic Materials | 5 | -7.47 | +1.96 | -6.23 | — |
| 2026-08-19 | buy | LOGI | Technology | 5 | -5.11 | -4.42 | — | — |
| 2026-08-19 | buy | WB | Communication Services | 5 | -4.00 | -6.14 | — | — |
| 2026-08-19 | buy | AAL | Industrials | 5 | -2.45 | -0.29 | — | — |
| 2026-08-19 | buy | ALK | Industrials | 5 | -2.55 | -1.99 | — | — |
| 2026-08-19 | buy | AKAM | Technology | 5 | -2.44 | -2.15 | — | — |
| 2026-08-19 | buy | MGM | Consumer Cyclical | 5 | -0.12 | +1.20 | — | — |
| 2026-08-19 | buy | NYT | Communication Services | 5 | -1.00 | -0.41 | — | — |
| 2026-08-19 | buy | SIRI | Communication Services | 5 | -0.98 | +0.07 | — | — |
| 2026-08-19 | buy | WLY | Communication Services | 5 | +0.21 | -0.75 | — | — |
| 2026-08-19 | buy | NEE | Utilities | 4 | -1.02 | -2.63 | — | — |
| 2026-08-19 | buy | NRG | Utilities | 4 | -4.33 | -6.20 | — | — |
| 2026-08-19 | buy | ARM | Technology | 4 | +0.55 | -2.41 | — | — |
| 2026-08-19 | buy | CHTR | Communication Services | 4 | -3.09 | -1.51 | — | — |
| 2026-08-19 | buy | JD | Consumer Cyclical | 4 | +0.20 | +0.07 | — | — |
| 2026-08-19 | buy | LEN | Consumer Cyclical | 4 | -2.15 | -0.32 | — | — |
| 2026-08-19 | buy | MAR | Consumer Cyclical | 4 | -0.73 | -0.79 | — | — |
| 2026-08-19 | buy | MCD | Consumer Cyclical | 4 | +0.63 | +1.31 | — | — |
| 2026-08-19 | buy | SWKS | Technology | 4 | -0.79 | -1.96 | — | — |
| 2026-08-19 | buy | TCOM | Consumer Cyclical | 4 | -0.17 | -0.86 | — | — |
| 2026-08-19 | buy | AXP | Financial | 4 | -2.57 | -1.15 | — | — |
| 2026-08-19 | buy | CDNS | Technology | 4 | -0.44 | +1.28 | — | — |
| 2026-08-19 | buy | CDW | Technology | 4 | -2.67 | +0.04 | — | — |
| 2026-08-19 | buy | DAL | Industrials | 4 | -2.68 | -1.06 | — | — |
| 2026-08-19 | buy | NMR | Financial | 4 | -0.63 | +2.21 | — | — |
| 2026-08-19 | buy | UAL | Industrials | 4 | -3.52 | -2.25 | — | — |
| 2026-08-19 | sell | AOS | Industrials | 2 | +4.83 | +2.58 | +3.65 | — |
| 2026-08-19 | sell | BF-B | Consumer Defensive | 2 | +4.71 | +4.64 | +4.75 | — |
| 2026-08-19 | sell | BKH | Utilities | 2 | +0.06 | +0.19 | -2.00 | — |
| 2026-08-19 | sell | CALM | Consumer Defensive | 2 | -0.79 | +1.53 | +1.98 | — |
| 2026-08-19 | sell | CAPR | Healthcare | 2 | +12.71 | -3.39 | -11.16 | — |
| 2026-08-19 | sell | CDNL | Industrials | 2 | -4.01 | -6.40 | -3.63 | — |
| 2026-08-19 | sell | COCO | Consumer Defensive | 2 | +6.34 | +2.56 | -0.30 | — |
| 2026-08-19 | sell | DPZ | Consumer Cyclical | 2 | +0.08 | -0.56 | +1.67 | — |
| 2026-08-19 | sell | ENVX | Industrials | 2 | +8.45 | +4.31 | +7.82 | — |
| 2026-08-19 | sell | EVRG | Utilities | 2 | +0.19 | -0.31 | -3.00 | — |
| 2026-08-19 | sell | EVTL | Industrials | 2 | -1.29 | -3.35 | -1.16 | — |
| 2026-08-19 | sell | EXEL | Healthcare | 2 | +2.39 | +1.07 | +1.81 | — |
| 2026-08-19 | sell | EYPT | Healthcare | 2 | +15.17 | -1.28 | -2.74 | — |
| 2026-08-19 | sell | IEP | Energy | 2 | -2.63 | -2.63 | -0.59 | — |
| 2026-08-19 | sell | JBS | Consumer Defensive | 2 | +0.66 | -0.81 | +1.17 | — |
| 2026-08-19 | sell | KMB | Consumer Defensive | 2 | +2.00 | +0.90 | +1.48 | — |
| 2026-08-19 | sell | LNT | Utilities | 2 | +0.24 | -0.74 | -3.03 | — |
| 2026-08-19 | sell | LNTH | Healthcare | 2 | +0.22 | -0.02 | -0.29 | — |
| 2026-08-19 | sell | OTLK | Healthcare | 2 | -1.43 | -3.43 | -7.57 | — |
| 2026-08-19 | sell | PBH | Healthcare | 2 | +2.19 | -0.10 | +1.46 | — |
| 2026-08-19 | sell | PNW | Utilities | 2 | -0.72 | -0.77 | -2.79 | — |
| 2026-08-19 | sell | RRC | Energy | 2 | +1.15 | +2.03 | +3.01 | — |
| 2026-08-19 | sell | SERV | Industrials | 2 | — | — | — | — |
| 2026-08-19 | sell | TAP | Consumer Defensive | 2 | +3.31 | +3.60 | +4.40 | — |
| 2026-08-19 | sell | YSS | Industrials | 2 | -2.30 | -11.71 | -10.56 | — |
| 2026-08-20 | buy | HIMS | Healthcare | 10 | +6.03 | — | — | — |
| 2026-08-20 | buy | PACB | Healthcare | 10 | +9.76 | — | — | — |
| 2026-08-20 | buy | VERA | Healthcare | 10 | +3.25 | — | — | — |
| 2026-08-20 | buy | FSLR | Technology | 10 | +0.10 | — | — | — |
| 2026-08-20 | buy | GRND | Technology | 10 | -0.83 | — | — | — |
| 2026-08-20 | buy | SOC | Energy | 10 | +0.59 | — | — | — |
| 2026-08-20 | buy | RIOT | Financial | 10 | -5.48 | — | — | — |
| 2026-08-20 | hold | NYT | Communication Services | 10 | +0.60 | — | — | — |
| 2026-08-20 | buy | LZ | Industrials | 10 | -0.43 | — | — | — |
| 2026-08-20 | buy | TPR | Consumer Cyclical | 10 | +0.27 | — | — | — |
| 2026-08-20 | buy | ETSY | Consumer Cyclical | 10 | -0.22 | — | — | — |
| 2026-08-20 | buy | PLNT | Consumer Cyclical | 10 | +2.03 | — | — | — |
| 2026-08-20 | buy | YETI | Consumer Cyclical | 10 | -0.55 | — | — | — |
| 2026-08-20 | buy | JPM | Financial | 9 | +0.01 | — | — | — |
| 2026-08-20 | buy | TEM | Healthcare | 9 | +9.06 | — | — | — |
| 2026-08-20 | buy | ACHV | Healthcare | 9 | -0.37 | — | — | — |
| 2026-08-20 | buy | AUPH | Healthcare | 9 | -3.59 | — | — | — |
| 2026-08-20 | buy | EZPW | Financial | 9 | +12.03 | — | — | — |
| 2026-08-20 | buy | HNST | Consumer Defensive | 9 | +1.81 | — | — | — |
| 2026-08-20 | buy | METC | Basic Materials | 9 | +6.73 | — | — | — |
| 2026-08-20 | buy | MOS | Basic Materials | 9 | +4.54 | — | — | — |
| 2026-08-20 | buy | WIX | Technology | 9 | -0.39 | — | — | — |
| 2026-08-20 | buy | ABX | Financial | 9 | +5.46 | — | — | — |
| 2026-08-20 | buy | AEE | Utilities | 9 | -2.44 | — | — | — |
| 2026-08-20 | buy | CBSH | Financial | 9 | -0.71 | — | — | — |
| 2026-08-20 | sell | AAL | Industrials | 5 | -2.45 | -0.29 | — | — |
| 2026-08-20 | sell | AKAM | Technology | 5 | -2.44 | -2.15 | — | — |
| 2026-08-20 | sell | ALK | Industrials | 5 | -2.55 | -1.99 | — | — |
| 2026-08-20 | sell | ARM | Technology | 4 | +0.55 | -2.41 | — | — |
| 2026-08-20 | sell | AXP | Financial | 4 | -2.57 | -1.15 | — | — |
| 2026-08-20 | sell | CDNS | Technology | 4 | -0.44 | +1.28 | — | — |
| 2026-08-20 | sell | CDW | Technology | 4 | -2.67 | +0.04 | — | — |
| 2026-08-20 | sell | CHTR | Communication Services | 4 | -3.09 | -1.51 | — | — |
| 2026-08-20 | sell | DAL | Industrials | 4 | -2.68 | -1.06 | — | — |
| 2026-08-20 | sell | JD | Consumer Cyclical | 4 | +0.20 | +0.07 | — | — |
| 2026-08-20 | sell | LEN | Consumer Cyclical | 4 | -2.15 | -0.32 | — | — |
| 2026-08-20 | sell | LOGI | Technology | 5 | -5.11 | -4.42 | — | — |
| 2026-08-20 | sell | MAR | Consumer Cyclical | 4 | -0.73 | -0.79 | — | — |
| 2026-08-20 | sell | MCD | Consumer Cyclical | 4 | +0.63 | +1.31 | — | — |
| 2026-08-20 | sell | MGM | Consumer Cyclical | 5 | -0.12 | +1.20 | — | — |
| 2026-08-20 | sell | NEE | Utilities | 4 | -1.02 | -2.63 | — | — |
| 2026-08-20 | sell | NMR | Financial | 4 | -0.63 | +2.21 | — | — |
| 2026-08-20 | sell | NRG | Utilities | 4 | -4.33 | -6.20 | — | — |
| 2026-08-20 | sell | SIRI | Communication Services | 5 | -0.98 | +0.07 | — | — |
| 2026-08-20 | sell | SWKS | Technology | 4 | -0.79 | -1.96 | — | — |
| 2026-08-20 | sell | TCOM | Consumer Cyclical | 4 | -0.17 | -0.86 | — | — |
| 2026-08-20 | sell | UAL | Industrials | 4 | -3.52 | -2.25 | — | — |
| 2026-08-20 | sell | WB | Communication Services | 5 | -4.00 | -6.14 | — | — |
| 2026-08-20 | sell | WLY | Communication Services | 5 | +0.21 | -0.75 | — | — |
| 2026-08-21 | buy | CTRE | Real Estate | 10 | — | — | — | — |
| 2026-08-21 | hold | EZPW | Financial | 10 | — | — | — | — |
| 2026-08-21 | buy | RZLT | Healthcare | 10 | — | — | — | — |
| 2026-08-21 | buy | TCBI | Financial | 10 | — | — | — | — |
| 2026-08-21 | buy | DLO | Technology | 10 | — | — | — | — |
| 2026-08-21 | buy | OLLI | Consumer Defensive | 10 | — | — | — | — |
| 2026-08-21 | buy | RR | Industrials | 10 | — | — | — | — |
| 2026-08-21 | buy | AKBA | Healthcare | 9 | — | — | — | — |
| 2026-08-21 | buy | CRSP | Healthcare | 9 | — | — | — | — |
| 2026-08-21 | buy | ABR | Real Estate | 9 | — | — | — | — |
| 2026-08-21 | buy | ALLY | Financial | 9 | — | — | — | — |
| 2026-08-21 | buy | ALT | Healthcare | 9 | — | — | — | — |
| 2026-08-21 | buy | AQST | Healthcare | 9 | — | — | — | — |
| 2026-08-21 | buy | AVTR | Healthcare | 9 | — | — | — | — |
| 2026-08-21 | buy | SOFI | Financial | 9 | — | — | — | — |
| 2026-08-21 | buy | TMC | Basic Materials | 9 | — | — | — | — |
| 2026-08-21 | buy | WAL | Financial | 9 | — | — | — | — |
| 2026-08-21 | buy | XP | Financial | 9 | — | — | — | — |
| 2026-08-21 | buy | ALTO | Basic Materials | 9 | — | — | — | — |
| 2026-08-21 | buy | APLE | Real Estate | 9 | — | — | — | — |
| 2026-08-21 | buy | DRH | Real Estate | 9 | — | — | — | — |
| 2026-08-21 | buy | GTY | Real Estate | 9 | — | — | — | — |
| 2026-08-21 | buy | RLJ | Real Estate | 9 | — | — | — | — |
| 2026-08-21 | buy | VMC | Basic Materials | 9 | — | — | — | — |
| 2026-08-21 | buy | FOUR | Technology | 9 | — | — | — | — |
| 2026-08-21 | sell | ABX | Financial | 9 | +5.46 | — | — | — |
| 2026-08-21 | sell | ACHV | Healthcare | 9 | -0.37 | — | — | — |
| 2026-08-21 | sell | AEE | Utilities | 9 | -2.44 | — | — | — |
| 2026-08-21 | sell | AUPH | Healthcare | 9 | -3.59 | — | — | — |
| 2026-08-21 | sell | CBSH | Financial | 9 | -0.71 | — | — | — |
| 2026-08-21 | sell | ETSY | Consumer Cyclical | 10 | -0.22 | — | — | — |
| 2026-08-21 | sell | FSLR | Technology | 10 | +0.10 | — | — | — |
| 2026-08-21 | sell | GRND | Technology | 10 | -0.83 | — | — | — |
| 2026-08-21 | sell | HIMS | Healthcare | 10 | +6.03 | — | — | — |
| 2026-08-21 | sell | HNST | Consumer Defensive | 9 | +1.81 | — | — | — |
| 2026-08-21 | sell | JPM | Financial | 9 | +0.01 | — | — | — |
| 2026-08-21 | sell | LZ | Industrials | 10 | -0.43 | — | — | — |
| 2026-08-21 | sell | METC | Basic Materials | 9 | +6.73 | — | — | — |
| 2026-08-21 | sell | MOS | Basic Materials | 9 | +4.54 | — | — | — |
| 2026-08-21 | sell | NYT | Communication Services | 10 | +0.60 | — | — | — |
| 2026-08-21 | sell | PACB | Healthcare | 10 | +9.76 | — | — | — |
| 2026-08-21 | sell | PLNT | Consumer Cyclical | 10 | +2.03 | — | — | — |
| 2026-08-21 | sell | RIOT | Financial | 10 | -5.48 | — | — | — |
| 2026-08-21 | sell | SOC | Energy | 10 | +0.59 | — | — | — |
| 2026-08-21 | sell | TEM | Healthcare | 9 | +9.06 | — | — | — |
| 2026-08-21 | sell | TPR | Consumer Cyclical | 10 | +0.27 | — | — | — |
| 2026-08-21 | sell | VERA | Healthcare | 10 | +3.25 | — | — | — |
| 2026-08-21 | sell | WIX | Technology | 9 | -0.39 | — | — | — |
| 2026-08-21 | sell | YETI | Consumer Cyclical | 10 | -0.55 | — | — | — |
| 2026-08-27 | buy | KD | Technology | 10 | — | — | — | — |
| 2026-08-27 | buy | VYX | Technology | 10 | — | — | — | — |
| 2026-08-27 | buy | AVT | Technology | 10 | — | — | — | — |
| 2026-08-27 | buy | DAVE | Technology | 10 | — | — | — | — |
| 2026-08-27 | buy | GRND | Technology | 10 | — | — | — | — |
| 2026-08-27 | buy | ITRI | Technology | 10 | — | — | — | — |
| 2026-08-27 | buy | SLM | Financial | 10 | — | — | — | — |
| 2026-08-27 | buy | DJT | Communication Services | 10 | — | — | — | — |
| 2026-08-27 | buy | CBRL | Consumer Cyclical | 10 | — | — | — | — |
| 2026-08-27 | buy | CXT | Industrials | 10 | — | — | — | — |
| 2026-08-27 | buy | DPZ | Consumer Cyclical | 10 | — | — | — | — |
| 2026-08-27 | buy | IP | Consumer Cyclical | 10 | — | — | — | — |
| 2026-08-27 | buy | PBI | Industrials | 10 | — | — | — | — |
| 2026-08-27 | buy | RVLV | Consumer Cyclical | 10 | — | — | — | — |
| 2026-08-27 | buy | SFM | Consumer Defensive | 10 | — | — | — | — |
| 2026-08-27 | buy | XRX | Industrials | 10 | — | — | — | — |
| 2026-08-27 | buy | BRBR | Consumer Defensive | 10 | — | — | — | — |
| 2026-08-27 | buy | ESAB | Industrials | 10 | — | — | — | — |
| 2026-08-27 | buy | JBLU | Industrials | 10 | — | — | — | — |
| 2026-08-27 | buy | NCLH | Consumer Cyclical | 10 | — | — | — | — |
| 2026-08-27 | buy | POST | Consumer Defensive | 10 | — | — | — | — |
| 2026-08-27 | buy | SKIN | Consumer Defensive | 10 | — | — | — | — |
| 2026-08-27 | buy | VFC | Consumer Cyclical | 10 | — | — | — | — |
| 2026-08-27 | buy | ABX | Financial | 9 | — | — | — | — |
| 2026-08-27 | buy | KMPR | Financial | 9 | — | — | — | — |
| 2026-08-27 | sell | ABR | Real Estate | 9 | — | — | — | — |
| 2026-08-27 | sell | AKBA | Healthcare | 9 | — | — | — | — |
| 2026-08-27 | sell | ALLY | Financial | 9 | — | — | — | — |
| 2026-08-27 | sell | ALT | Healthcare | 9 | — | — | — | — |
| 2026-08-27 | sell | ALTO | Basic Materials | 9 | — | — | — | — |
| 2026-08-27 | sell | APLE | Real Estate | 9 | — | — | — | — |
| 2026-08-27 | sell | AQST | Healthcare | 9 | — | — | — | — |
| 2026-08-27 | sell | AVTR | Healthcare | 9 | — | — | — | — |
| 2026-08-27 | sell | CRSP | Healthcare | 9 | — | — | — | — |
| 2026-08-27 | sell | CTRE | Real Estate | 10 | — | — | — | — |
| 2026-08-27 | sell | DLO | Technology | 10 | — | — | — | — |
| 2026-08-27 | sell | DRH | Real Estate | 9 | — | — | — | — |
| 2026-08-27 | sell | EZPW | Financial | 10 | — | — | — | — |
| 2026-08-27 | sell | FOUR | Technology | 9 | — | — | — | — |
| 2026-08-27 | sell | GTY | Real Estate | 9 | — | — | — | — |
| 2026-08-27 | sell | OLLI | Consumer Defensive | 10 | — | — | — | — |
| 2026-08-27 | sell | RLJ | Real Estate | 9 | — | — | — | — |
| 2026-08-27 | sell | RR | Industrials | 10 | — | — | — | — |
| 2026-08-27 | sell | RZLT | Healthcare | 10 | — | — | — | — |
| 2026-08-27 | sell | SOFI | Financial | 9 | — | — | — | — |
| 2026-08-27 | sell | TCBI | Financial | 10 | — | — | — | — |
| 2026-08-27 | sell | TMC | Basic Materials | 9 | — | — | — | — |
| 2026-08-27 | sell | VMC | Basic Materials | 9 | — | — | — | — |
| 2026-08-27 | sell | WAL | Financial | 9 | — | — | — | — |
| 2026-08-27 | sell | XP | Financial | 9 | — | — | — | — |
| 2026-08-30 | buy | UPBD | Technology | 10 | — | — | — | — |
| 2026-08-30 | buy | BAND | Technology | 10 | — | — | — | — |
| 2026-08-30 | buy | DXC | Technology | 10 | — | — | — | — |
| 2026-08-30 | buy | PARR | Energy | 10 | — | — | — | — |
| 2026-08-30 | buy | FND | Consumer Cyclical | 10 | — | — | — | — |
| 2026-08-30 | buy | LULU | Consumer Cyclical | 10 | — | — | — | — |
| 2026-08-30 | buy | POOL | Industrials | 10 | — | — | — | — |
| 2026-08-30 | buy | CRWD | Technology | 9 | — | — | — | — |
| 2026-08-30 | buy | BRZE | Technology | 9 | — | — | — | — |
| 2026-08-30 | buy | CRCT | Technology | 9 | — | — | — | — |
| 2026-08-30 | buy | AGNC | Real Estate | 9 | — | — | — | — |
| 2026-08-30 | buy | ES | Utilities | 9 | — | — | — | — |
| 2026-08-30 | buy | GLPI | Real Estate | 9 | — | — | — | — |
| 2026-08-30 | buy | SBS | Utilities | 9 | — | — | — | — |
| 2026-08-30 | buy | ZIP | Communication Services | 9 | — | — | — | — |
| 2026-08-30 | buy | GENI | Communication Services | 9 | — | — | — | — |
| 2026-08-30 | buy | PEB | Real Estate | 9 | — | — | — | — |
| 2026-08-30 | buy | SRE | Utilities | 9 | — | — | — | — |
| 2026-08-30 | buy | DG | Consumer Defensive | 9 | — | — | — | — |
| 2026-08-30 | hold | POST | Consumer Defensive | 9 | — | — | — | — |
| 2026-08-30 | buy | VITL | Consumer Defensive | 9 | — | — | — | — |
| 2026-08-30 | hold | BRBR | Consumer Defensive | 9 | — | — | — | — |
| 2026-08-30 | buy | NWL | Consumer Defensive | 9 | — | — | — | — |
| 2026-08-30 | buy | SNN | Healthcare | 9 | — | — | — | — |
| 2026-08-30 | buy | SYK | Healthcare | 9 | — | — | — | — |
| 2026-08-30 | sell | ABX | Financial | 9 | — | — | — | — |
| 2026-08-30 | sell | AVT | Technology | 10 | — | — | — | — |
| 2026-08-30 | sell | CBRL | Consumer Cyclical | 10 | — | — | — | — |
| 2026-08-30 | sell | CXT | Industrials | 10 | — | — | — | — |
| 2026-08-30 | sell | DAVE | Technology | 10 | — | — | — | — |
| 2026-08-30 | sell | DJT | Communication Services | 10 | — | — | — | — |
| 2026-08-30 | sell | DPZ | Consumer Cyclical | 10 | — | — | — | — |
| 2026-08-30 | sell | ESAB | Industrials | 10 | — | — | — | — |
| 2026-08-30 | sell | GRND | Technology | 10 | — | — | — | — |
| 2026-08-30 | sell | IP | Consumer Cyclical | 10 | — | — | — | — |
| 2026-08-30 | sell | ITRI | Technology | 10 | — | — | — | — |
| 2026-08-30 | sell | JBLU | Industrials | 10 | — | — | — | — |
| 2026-08-30 | sell | KD | Technology | 10 | — | — | — | — |
| 2026-08-30 | sell | KMPR | Financial | 9 | — | — | — | — |
| 2026-08-30 | sell | NCLH | Consumer Cyclical | 10 | — | — | — | — |
| 2026-08-30 | sell | PBI | Industrials | 10 | — | — | — | — |
| 2026-08-30 | sell | RVLV | Consumer Cyclical | 10 | — | — | — | — |
| 2026-08-30 | sell | SFM | Consumer Defensive | 10 | — | — | — | — |
| 2026-08-30 | sell | SKIN | Consumer Defensive | 10 | — | — | — | — |
| 2026-08-30 | sell | SLM | Financial | 10 | — | — | — | — |
| 2026-08-30 | sell | VFC | Consumer Cyclical | 10 | — | — | — | — |
| 2026-08-30 | sell | VYX | Technology | 10 | — | — | — | — |
| 2026-08-30 | sell | XRX | Industrials | 10 | — | — | — | — |
| 2026-08-31 | buy | BMO | Financial | 9 | — | — | — | — |
| 2026-08-31 | buy | AMZN | Consumer Cyclical | 9 | — | — | — | — |
| 2026-08-31 | buy | RWAY | Financial | 9 | — | — | — | — |
| 2026-08-31 | buy | VTS | Energy | 9 | — | — | — | — |
| 2026-08-31 | buy | SDGR | Healthcare | 9 | — | — | — | — |
| 2026-08-31 | buy | TCOM | Consumer Cyclical | 9 | — | — | — | — |
| 2026-08-31 | buy | HDSN | Basic Materials | 9 | — | — | — | — |
| 2026-08-31 | buy | BAH | Industrials | 9 | — | — | — | — |
| 2026-08-31 | buy | CRM | Technology | 8 | — | — | — | — |
| 2026-08-31 | buy | CVE | Energy | 8 | — | — | — | — |
| 2026-08-31 | buy | CVX | Energy | 8 | — | — | — | — |
| 2026-08-31 | buy | LIN | Basic Materials | 8 | — | — | — | — |
| 2026-08-31 | buy | OXY | Energy | 8 | — | — | — | — |
| 2026-08-31 | buy | COST | Consumer Defensive | 8 | — | — | — | — |
| 2026-08-31 | buy | DTM | Energy | 8 | — | — | — | — |
| 2026-08-31 | buy | APD | Basic Materials | 8 | — | — | — | — |
| 2026-08-31 | buy | VOD | Communication Services | 8 | — | — | — | — |
| 2026-08-31 | buy | TALO | Energy | 8 | — | — | — | — |
| 2026-08-31 | buy | FIBK | Financial | 8 | — | — | — | — |
| 2026-08-31 | buy | TCBI | Financial | 7 | — | — | — | — |
| 2026-08-31 | buy | COR | Healthcare | 7 | — | — | — | — |
| 2026-08-31 | hold | UPBD | Technology | 7 | — | — | — | — |
| 2026-08-31 | buy | HWM | Industrials | 7 | — | — | — | — |
| 2026-08-31 | buy | FHB | Financial | 7 | — | — | — | — |
| 2026-08-31 | buy | HASI | Financial | 7 | — | — | — | — |
| 2026-08-31 | sell | AGNC | Real Estate | 9 | — | — | — | — |
| 2026-08-31 | sell | BAND | Technology | 10 | — | — | — | — |
| 2026-08-31 | sell | BRBR | Consumer Defensive | 9 | — | — | — | — |
| 2026-08-31 | sell | BRZE | Technology | 9 | — | — | — | — |
| 2026-08-31 | sell | CRCT | Technology | 9 | — | — | — | — |
| 2026-08-31 | sell | CRWD | Technology | 9 | — | — | — | — |
| 2026-08-31 | sell | DG | Consumer Defensive | 9 | — | — | — | — |
| 2026-08-31 | sell | DXC | Technology | 10 | — | — | — | — |
| 2026-08-31 | sell | ES | Utilities | 9 | — | — | — | — |
| 2026-08-31 | sell | FND | Consumer Cyclical | 10 | — | — | — | — |
| 2026-08-31 | sell | GENI | Communication Services | 9 | — | — | — | — |
| 2026-08-31 | sell | GLPI | Real Estate | 9 | — | — | — | — |
| 2026-08-31 | sell | LULU | Consumer Cyclical | 10 | — | — | — | — |
| 2026-08-31 | sell | NWL | Consumer Defensive | 9 | — | — | — | — |
| 2026-08-31 | sell | PARR | Energy | 10 | — | — | — | — |
| 2026-08-31 | sell | PEB | Real Estate | 9 | — | — | — | — |
| 2026-08-31 | sell | POOL | Industrials | 10 | — | — | — | — |
| 2026-08-31 | sell | POST | Consumer Defensive | 9 | — | — | — | — |
| 2026-08-31 | sell | SBS | Utilities | 9 | — | — | — | — |
| 2026-08-31 | sell | SNN | Healthcare | 9 | — | — | — | — |
| 2026-08-31 | sell | SRE | Utilities | 9 | — | — | — | — |
| 2026-08-31 | sell | SYK | Healthcare | 9 | — | — | — | — |
| 2026-08-31 | sell | VITL | Consumer Defensive | 9 | — | — | — | — |
| 2026-08-31 | sell | ZIP | Communication Services | 9 | — | — | — | — |
| 2026-09-01 | buy | CTGO | Basic Materials | 10 | — | — | — | — |
| 2026-09-01 | buy | GPRO | Technology | 10 | — | — | — | — |
| 2026-09-01 | buy | IEP | Energy | 10 | — | — | — | — |
| 2026-09-01 | buy | ANNX | Healthcare | 10 | — | — | — | — |
| 2026-09-01 | buy | WOLF | Technology | 10 | — | — | — | — |
| 2026-09-01 | buy | ADMA | Healthcare | 10 | — | — | — | — |
| 2026-09-01 | buy | ALLO | Healthcare | 10 | — | — | — | — |
| 2026-09-01 | buy | COGT | Healthcare | 10 | — | — | — | — |
| 2026-09-01 | buy | DDD | Technology | 10 | — | — | — | — |
| 2026-09-01 | buy | FULC | Healthcare | 10 | — | — | — | — |
| 2026-09-01 | buy | INDV | Healthcare | 10 | — | — | — | — |
| 2026-09-01 | buy | AVAV | Industrials | 10 | — | — | — | — |
| 2026-09-01 | buy | ENVX | Industrials | 10 | — | — | — | — |
| 2026-09-01 | buy | VRRM | Technology | 10 | — | — | — | — |
| 2026-09-01 | buy | DKS | Consumer Cyclical | 10 | — | — | — | — |
| 2026-09-01 | buy | FWRG | Consumer Cyclical | 10 | — | — | — | — |
| 2026-09-01 | buy | GME | Consumer Cyclical | 10 | — | — | — | — |
| 2026-09-01 | buy | MIR | Industrials | 10 | — | — | — | — |
| 2026-09-01 | buy | ATRO | Industrials | 10 | — | — | — | — |
| 2026-09-01 | buy | LNT | Utilities | 10 | — | — | — | — |
| 2026-09-01 | buy | UAA | Consumer Cyclical | 10 | — | — | — | — |
| 2026-09-01 | buy | KMI | Energy | 9 | — | — | — | — |
| 2026-09-01 | buy | DHT | Energy | 9 | — | — | — | — |
| 2026-09-01 | buy | LBRT | Energy | 9 | — | — | — | — |
| 2026-09-01 | buy | ASX | Technology | 9 | — | — | — | — |
| 2026-09-01 | sell | AMZN | Consumer Cyclical | 9 | — | — | — | — |
| 2026-09-01 | sell | APD | Basic Materials | 8 | — | — | — | — |
| 2026-09-01 | sell | BAH | Industrials | 9 | — | — | — | — |
| 2026-09-01 | sell | BMO | Financial | 9 | — | — | — | — |
| 2026-09-01 | sell | COR | Healthcare | 7 | — | — | — | — |
| 2026-09-01 | sell | COST | Consumer Defensive | 8 | — | — | — | — |
| 2026-09-01 | sell | CRM | Technology | 8 | — | — | — | — |
| 2026-09-01 | sell | CVE | Energy | 8 | — | — | — | — |
| 2026-09-01 | sell | CVX | Energy | 8 | — | — | — | — |
| 2026-09-01 | sell | DTM | Energy | 8 | — | — | — | — |
| 2026-09-01 | sell | FHB | Financial | 7 | — | — | — | — |
| 2026-09-01 | sell | FIBK | Financial | 8 | — | — | — | — |
| 2026-09-01 | sell | HASI | Financial | 7 | — | — | — | — |
| 2026-09-01 | sell | HDSN | Basic Materials | 9 | — | — | — | — |
| 2026-09-01 | sell | HWM | Industrials | 7 | — | — | — | — |
| 2026-09-01 | sell | LIN | Basic Materials | 8 | — | — | — | — |
| 2026-09-01 | sell | OXY | Energy | 8 | — | — | — | — |
| 2026-09-01 | sell | RWAY | Financial | 9 | — | — | — | — |
| 2026-09-01 | sell | SDGR | Healthcare | 9 | — | — | — | — |
| 2026-09-01 | sell | TALO | Energy | 8 | — | — | — | — |
| 2026-09-01 | sell | TCBI | Financial | 7 | — | — | — | — |
| 2026-09-01 | sell | TCOM | Consumer Cyclical | 9 | — | — | — | — |
| 2026-09-01 | sell | UPBD | Technology | 7 | — | — | — | — |
| 2026-09-01 | sell | VOD | Communication Services | 8 | — | — | — | — |
| 2026-09-01 | sell | VTS | Energy | 9 | — | — | — | — |

## Daily long books (compact)

### 2026-08-13 · `Band` · pool 547 · A and blue thin; pool=short=high AND sma20=below

ABEO -5.67/-6.46 ANDG +1.22/+1.45 ATRA -4.93/-1.76 BETR -8.47/-9.73 BW +10.05/+6.10 BYND —/— CRIS -3.74/— CVRX +4.76/-6.46 CYN +4.22/-0.47 DJCO +0.35/-0.87 EMPD +2.12/-1.06 EVTL -1.07/-4.30 FBIO -1.68/-5.37 GSUN +7.46/— IMDX +16.44/+15.09 KSCP -6.17/-11.73 LESL -16.23/-14.80 MVIS -40.58/-43.24 ONON +1.99/-0.89 OPEN -0.27/-3.01 TPR +0.46/+0.49 VWAV +44.72/+16.26 YETI -2.00/-5.43 ABX +8.80/+7.04 AIOT -0.95/-8.28

Seats 1d n=24 · p_win=50.0% · p_loss=50.0% · avg_win=+8.55 · avg_loss=-7.65 · mean=+0.45 · clip30=+0.28 · payoff=1.12 · universe hit=46.4% med=+0.00.

### 2026-08-14 · `blue` · pool 517 · A missing; pool=blue

GLND +0.00/+1.56 NCMI -4.54/-11.89 STUB -11.88/-11.76 ANGI -5.51/-1.91 BMBL -4.06/+0.37 BORR +1.58/+0.00 CCOI -5.14/-10.61 DJT -2.06/-2.54 FLNG +0.26/+0.97 IEP -4.18/-7.95 IMPP +3.76/+5.43 LB +1.02/+2.71 NEE +0.04/+0.04 NRG -3.07/-8.46 VST -1.36/-5.14 CEG -1.67/-5.55 TLN -1.60/-12.43

Seats 1d n=17 · p_win=29.4% · p_loss=64.7% · avg_win=+1.33 · avg_loss=-4.10 · mean=-2.26 · clip30=-2.26 · payoff=0.33 · universe hit=32.2% med=-0.24.

### 2026-08-17 · `blue` · pool 175 · A missing; pool=blue

UAMY -7.47/+1.96 ALTO +0.47/-0.71 CC -1.91/+1.21 FMC -0.59/+6.19 GPRE +0.25/-0.68 HUN -2.52/-1.26 AMAT -3.92/-7.31 AVGO -3.17/-7.63 AMD -4.27/-7.82 ASML -4.26/-6.98 INTC -6.57/-10.33 LRCX -4.63/-10.66

Seats 1d n=12 · p_win=16.7% · p_loss=83.3% · avg_win=+0.36 · avg_loss=-3.93 · mean=-3.21 · clip30=-3.21 · payoff=0.09 · universe hit=35.7% med=-0.60.

### 2026-08-18 · `Band` · pool 408 · A and blue thin; pool=short=high AND sma20=below

AOS +4.83/+2.58 BF-B +4.71/+4.64 COCO +6.34/+2.56 EVRG +0.19/-0.31 EXEL +2.39/+1.07 KMB +2.00/+0.90 LNT +0.24/-0.74 LNTH +0.22/-0.02 PBH +2.19/-0.10 PNW -0.72/-0.77 RRC +1.15/+2.03 TAP +3.31/+3.60 BKH +0.06/+0.19 CALM -0.79/+1.53 JBS +0.66/-0.81 CAPR +12.71/-3.39 ENVX +8.45/+4.31 EVTL -1.29/-3.35 EYPT +15.17/-1.28 IEP -2.63/-2.63 OTLK -1.43/-3.43 SERV —/— YSS -2.30/-11.71 CDNL -4.01/-6.40 DPZ +0.08/-0.56

Seats 1d n=24 · p_win=70.8% · p_loss=29.2% · avg_win=+3.81 · avg_loss=-1.88 · mean=+2.15 · clip30=+2.15 · payoff=2.02 · universe hit=62.4% med=+0.88.

### 2026-08-19 · `blue` · pool 156 · A missing; pool=blue

LOGI -5.11/-4.42 WB -4.00/-6.14 AAL -2.45/-0.29 ALK -2.55/-1.99 AKAM -2.44/-2.15 MGM -0.12/+1.20 NYT -1.00/-0.41 SIRI -0.98/+0.07 WLY +0.21/-0.75 NEE -1.02/-2.63 NRG -4.33/-6.20 ARM +0.55/-2.41 CHTR -3.09/-1.51 JD +0.20/+0.07 LEN -2.15/-0.32 MAR -0.73/-0.79 MCD +0.63/+1.31 SWKS -0.79/-1.96 TCOM -0.17/-0.86 AXP -2.57/-1.15 CDNS -0.44/+1.28 CDW -2.67/+0.04 DAL -2.68/-1.06 NMR -0.63/+2.21 UAL -3.52/-2.25

Seats 1d n=25 · p_win=16.0% · p_loss=84.0% · avg_win=+0.40 · avg_loss=-2.07 · mean=-1.67 · clip30=-1.67 · payoff=0.19 · universe hit=29.4% med=-0.98.

### 2026-08-20 · `A` · pool 1821 · A cameras printed; pool=ab|peer good

HIMS +6.03/— PACB +9.76/— VERA +3.25/— FSLR +0.10/— GRND -0.83/— SOC +0.59/— RIOT -5.48/— NYT +0.60/— LZ -0.43/— TPR +0.27/— ETSY -0.22/— PLNT +2.03/— YETI -0.55/— JPM +0.01/— TEM +9.06/— ACHV -0.37/— AUPH -3.59/— EZPW +12.03/— HNST +1.81/— METC +6.73/— MOS +4.54/— WIX -0.39/— ABX +5.46/— AEE -2.44/— CBSH -0.71/—

Seats 1d n=25 · p_win=60.0% · p_loss=40.0% · avg_win=+4.15 · avg_loss=-1.50 · mean=+1.89 · clip30=+1.89 · payoff=2.76 · universe hit=65.1% med=+0.72.

### 2026-08-21 · `A` · pool 1898 · A cameras printed; pool=ab|peer good

CTRE —/— EZPW —/— RZLT —/— TCBI —/— DLO —/— OLLI —/— RR —/— AKBA —/— CRSP —/— ABR —/— ALLY —/— ALT —/— AQST —/— AVTR —/— SOFI —/— TMC —/— WAL —/— XP —/— ALTO —/— APLE —/— DRH —/— GTY —/— RLJ —/— VMC —/— FOUR —/—

1d not settled — names only.

### 2026-08-27 · `A` · pool 1686 · A cameras printed; pool=ab|peer good

KD —/— VYX —/— AVT —/— DAVE —/— GRND —/— ITRI —/— SLM —/— DJT —/— CBRL —/— CXT —/— DPZ —/— IP —/— PBI —/— RVLV —/— SFM —/— XRX —/— BRBR —/— ESAB —/— JBLU —/— NCLH —/— POST —/— SKIN —/— VFC —/— ABX —/— KMPR —/—

1d not settled — names only.

### 2026-08-30 · `A` · pool 1840 · A cameras printed; pool=ab|peer good

UPBD —/— BAND —/— DXC —/— PARR —/— FND —/— LULU —/— POOL —/— CRWD —/— BRZE —/— CRCT —/— AGNC —/— ES —/— GLPI —/— SBS —/— ZIP —/— GENI —/— PEB —/— SRE —/— DG —/— POST —/— VITL —/— BRBR —/— NWL —/— SNN —/— SYK —/—

1d not settled — names only.

### 2026-08-31 · `A` · pool 1798 · A cameras printed; pool=ab|peer good

BMO —/— AMZN —/— RWAY —/— VTS —/— SDGR —/— TCOM —/— HDSN —/— BAH —/— CRM —/— CVE —/— CVX —/— LIN —/— OXY —/— COST —/— DTM —/— APD —/— VOD —/— TALO —/— FIBK —/— TCBI —/— COR —/— UPBD —/— HWM —/— FHB —/— HASI —/—

1d not settled — names only.

### 2026-09-01 · `A` · pool 1700 · A cameras printed; pool=ab|peer good

CTGO —/— GPRO —/— IEP —/— ANNX —/— WOLF —/— ADMA —/— ALLO —/— COGT —/— DDD —/— FULC —/— INDV —/— AVAV —/— ENVX —/— VRRM —/— DKS —/— FWRG —/— GME —/— MIR —/— ATRO —/— LNT —/— UAA —/— KMI —/— DHT —/— LBRT —/— ASX —/—

1d not settled — names only.

## Notes

1. **A is not a multi-week edge in this panel.** One priced A-session, and that session was a broad up day.
2. **Blue is not constantly winning.** It can lift vs a red tape and still lose hard the next session.
3. **short AND sma20=below tracks the tape**, not a separate engine.
4. Use A as a same-morning seat-filler when the cameras printed. Re-score after later 1d prints settle.

