# Boring winners backtest

Filter-and-seat the FEATURE_MINE high-n edges. Not a flashy squeeze hunt.

**Hit engine A** = `ab=good` OR `peer=good`. **Scale B** = `short=high` OR `sma20=below`.
**Blue** overlay. **Fade / first_crack** vetoed. 15 seats, 4 per sector.

Score = `3·blue + 2·ab + 2·peer + 1·short_high + 1·sma_below + 1·ab_up`.
Tie-break: lookback points, fewer reds, relvol hot>normal>dead, ticker.

## Read this first

- Settled `1d` only through **2026-08-20**. 8/21 → 9/1 have names, no close-to-close yet.
- A cameras only print from **2026-08-20**. Before that the live rule falls through to blue, then `short AND sma20=below`.
- Board `ab=good` 64.6% / `peer=good` 65.8% is almost entirely **one day** (20 Aug). Universe that morning was already **65.1%** up. A matched the tape; it did not beat it.
- Board `blue` +4.46 mean is squeeze-contaminated. Clip at ±30 and the same sleeve is about **+0.55**. That is why both raw and clip-30 print here.
- Pool EW = every name the filter kept. Seats EW = the 15 the ranker kept. Do not treat a 500-name Band pile as a 15-name strategy.

## Session tape

| date | rule | book | pool | seats | uni 1d hit | uni 1d med | pool 1d hit | pool 1d clip | seats 1d hit | seats 1d clip |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | `Band` | 11579 | 547 | 15 | 46.4% | +0.00 | 52.2% | +0.31 | 50.0% | -1.15 |
| 2026-08-14 | `blue` | 11586 | 517 | 12 | 32.2% | -0.24 | 43.7% | -0.54 | 25.0% | -2.87 |
| 2026-08-17 | `blue` | 2697 | 175 | 8 | 35.7% | -0.60 | 10.3% | -2.86 | 12.5% | -3.14 |
| 2026-08-18 | `Band` | 2698 | 408 | 15 | 62.4% | +0.88 | 73.2% | +2.05 | 93.3% | +3.21 |
| 2026-08-19 | `blue` | 2702 | 156 | 15 | 29.4% | -0.98 | 37.0% | -0.67 | 20.0% | -1.73 |
| 2026-08-20 | `A` | 2707 | 1821 | 15 | 65.1% | +0.72 | 64.8% | +1.03 | 66.7% | +1.61 |
| 2026-08-21 | `A` | 2707 | 1898 | 15 | — | — | — | — | — | — |
| 2026-08-27 | `A` | 2698 | 1686 | 15 | — | — | — | — | — | — |
| 2026-08-30 | `A` | 2690 | 1840 | 15 | — | — | — | — | — | — |
| 2026-08-31 | `A` | 2685 | 1798 | 15 | — | — | — | — | — | — |
| 2026-09-01 | `A` | 2683 | 1700 | 15 | — | — | — | — | — | — |

## 15-seat books

### 2026-08-13 · rule `Band` · pool 547 · A printed=false

| # | Ticker | score | blue | ab | peer | short | sma20 | ab_up | pts | reds | relvol | sector | 1d |
|---:|---|---:|:---:|---|---|---|---|:---:|---:|---:|---|---|---:|
| 1 | ABEO | 2 |  | missing | missing | high | below |  | 11 | 0 | hot | Healthcare | -5.67 |
| 2 | ANDG | 2 |  | missing | missing | high | below |  | 11 | 0 | hot | Consumer Cyclical | +1.22 |
| 3 | ATRA | 2 |  | missing | missing | high | below |  | 11 | 0 | hot | Healthcare | -4.93 |
| 4 | BETR | 2 |  | missing | missing | high | below |  | 11 | 0 | hot | Financial | -8.47 |
| 5 | BW | 2 |  | missing | missing | high | below |  | 11 | 0 | hot | Industrials | +10.05 |
| 6 | BYND | 2 |  | missing | missing | high | below |  | 11 | 0 | hot | Consumer Defensive | — |
| 7 | CRIS | 2 |  | missing | missing | high | below |  | 11 | 0 | hot | Healthcare | -3.74 |
| 8 | CVRX | 2 |  | missing | missing | high | below |  | 11 | 0 | hot | Healthcare | +4.76 |
| 9 | CYN | 2 |  | missing | missing | high | below |  | 11 | 0 | hot | Technology | +4.22 |
| 10 | DJCO | 2 |  | missing | missing | high | below |  | 11 | 0 | hot | Technology | +0.35 |
| 11 | EMPD | 2 |  | missing | missing | high | below |  | 11 | 0 | hot | Consumer Cyclical | +2.12 |
| 12 | EVTL | 2 |  | missing | missing | high | below |  | 11 | 0 | hot | Industrials | -1.07 |
| 13 | GSUN | 2 |  | missing | missing | high | below |  | 11 | 0 | hot | Consumer Defensive | +7.46 |
| 14 | KSCP | 2 |  | missing | missing | high | below |  | 11 | 0 | hot | Industrials | -6.17 |
| 15 | LESL | 2 |  | missing | missing | high | below |  | 11 | 0 | hot | Consumer Cyclical | -16.23 |

Seats 1d n=14 hit=50.0% raw=-1.15 clip30=-1.15 med=-0.36 · universe hit=46.4% med=+0.00.

### 2026-08-14 · rule `blue` · pool 517 · A printed=false

| # | Ticker | score | blue | ab | peer | short | sma20 | ab_up | pts | reds | relvol | sector | 1d |
|---:|---|---:|:---:|---|---|---|---|:---:|---:|---:|---|---|---:|
| 1 | GLND | 5 | Y | missing | missing | high | below |  | 11 | 0 | hot | Energy | +0.00 |
| 2 | NCMI | 5 | Y | missing | missing | high | below |  | 11 | 0 | hot | Communication Services | -4.54 |
| 3 | STUB | 5 | Y | missing | missing | high | below |  | 11 | 0 | hot | Communication Services | -11.88 |
| 4 | ANGI | 5 | Y | missing | missing | high | below |  | 11 | 0 | normal | Communication Services | -5.51 |
| 5 | BMBL | 5 | Y | missing | missing | high | below |  | 11 | 0 | normal | Communication Services | -4.06 |
| 6 | BORR | 5 | Y | missing | missing | high | below |  | 11 | 0 | normal | Energy | +1.58 |
| 7 | FLNG | 5 | Y | missing | missing | high | below |  | 11 | 0 | normal | Energy | +0.26 |
| 8 | IEP | 5 | Y | missing | missing | high | below |  | 11 | 0 | normal | Energy | -4.18 |
| 9 | NEE | 4 | Y | missing | missing | mid | below |  | 12 | 0 | normal | Utilities | +0.04 |
| 10 | NRG | 4 | Y | missing | missing | mid | below |  | 12 | 0 | dead | Utilities | -3.07 |
| 11 | VST | 4 | Y | missing | missing | low | below |  | 12 | 0 | dead | Utilities | -1.36 |
| 12 | CEG | 3 | Y | missing | missing | mid | above |  | 12 | 0 | dead | Utilities | -1.67 |

Seats 1d n=12 hit=25.0% raw=-2.87 clip30=-2.87 med=-2.37 · universe hit=32.2% med=-0.24.

### 2026-08-17 · rule `blue` · pool 175 · A printed=false

| # | Ticker | score | blue | ab | peer | short | sma20 | ab_up | pts | reds | relvol | sector | 1d |
|---:|---|---:|:---:|---|---|---|---|:---:|---:|---:|---|---|---:|
| 1 | UAMY | 5 | Y | missing | missing | high | below |  | 11 | 0 | normal | Basic Materials | -7.47 |
| 2 | ALTO | 5 | Y | missing | missing | high | below |  | 11 | 0 | dead | Basic Materials | +0.47 |
| 3 | CC | 5 | Y | missing | missing | high | below |  | 11 | 0 | dead | Basic Materials | -1.91 |
| 4 | FMC | 5 | Y | missing | missing | high | below |  | 11 | 0 | dead | Basic Materials | -0.59 |
| 5 | AMAT | 4 | Y | missing | missing | low | below |  | 12 | 0 | normal | Technology | -3.92 |
| 6 | AVGO | 4 | Y | missing | missing | low | below |  | 12 | 0 | normal | Technology | -3.17 |
| 7 | AMD | 3 | Y | missing | missing | low | above |  | 12 | 0 | normal | Technology | -4.27 |
| 8 | ASML | 3 | Y | missing | missing | low | above |  | 12 | 0 | normal | Technology | -4.26 |

Seats 1d n=8 hit=12.5% raw=-3.14 clip30=-3.14 med=-3.54 · universe hit=35.7% med=-0.60.

### 2026-08-18 · rule `Band` · pool 408 · A printed=false

| # | Ticker | score | blue | ab | peer | short | sma20 | ab_up | pts | reds | relvol | sector | 1d |
|---:|---|---:|:---:|---|---|---|---|:---:|---:|---:|---|---|---:|
| 1 | AOS | 2 |  | missing | missing | high | below |  | 9 | 1 | dead | Industrials | +4.83 |
| 2 | BF-B | 2 |  | missing | missing | high | below |  | 9 | 1 | dead | Consumer Defensive | +4.71 |
| 3 | COCO | 2 |  | missing | missing | high | below |  | 9 | 1 | dead | Consumer Defensive | +6.34 |
| 4 | EVRG | 2 |  | missing | missing | high | below |  | 9 | 1 | dead | Utilities | +0.19 |
| 5 | EXEL | 2 |  | missing | missing | high | below |  | 9 | 1 | dead | Healthcare | +2.39 |
| 6 | KMB | 2 |  | missing | missing | high | below |  | 9 | 1 | dead | Consumer Defensive | +2.00 |
| 7 | LNT | 2 |  | missing | missing | high | below |  | 9 | 1 | dead | Utilities | +0.24 |
| 8 | LNTH | 2 |  | missing | missing | high | below |  | 9 | 1 | dead | Healthcare | +0.22 |
| 9 | PBH | 2 |  | missing | missing | high | below |  | 9 | 1 | dead | Healthcare | +2.19 |
| 10 | PNW | 2 |  | missing | missing | high | below |  | 9 | 1 | dead | Utilities | -0.72 |
| 11 | RRC | 2 |  | missing | missing | high | below |  | 9 | 1 | dead | Energy | +1.15 |
| 12 | TAP | 2 |  | missing | missing | high | below |  | 8 | 1 | normal | Consumer Defensive | +3.31 |
| 13 | BKH | 2 |  | missing | missing | high | below |  | 8 | 1 | dead | Utilities | +0.06 |
| 14 | CAPR | 2 |  | missing | missing | high | below |  | 7 | 2 | hot | Healthcare | +12.71 |
| 15 | ENVX | 2 |  | missing | missing | high | below |  | 7 | 2 | hot | Industrials | +8.45 |

Seats 1d n=15 hit=93.3% raw=+3.21 clip30=+3.21 med=+2.19 · universe hit=62.4% med=+0.88.

### 2026-08-19 · rule `blue` · pool 156 · A printed=false

| # | Ticker | score | blue | ab | peer | short | sma20 | ab_up | pts | reds | relvol | sector | 1d |
|---:|---|---:|:---:|---|---|---|---|:---:|---:|---:|---|---|---:|
| 1 | LOGI | 5 | Y | missing | missing | high | below |  | 7 | 2 | normal | Technology | -5.11 |
| 2 | WB | 5 | Y | missing | missing | high | below |  | 7 | 2 | normal | Communication Services | -4.00 |
| 3 | AAL | 5 | Y | missing | missing | high | below |  | 7 | 2 | dead | Industrials | -2.45 |
| 4 | ALK | 5 | Y | missing | missing | high | below |  | 7 | 2 | dead | Industrials | -2.55 |
| 5 | AKAM | 5 | Y | missing | missing | high | below |  | 6 | 2 | dead | Technology | -2.44 |
| 6 | MGM | 5 | Y | missing | missing | high | below |  | 6 | 2 | dead | Consumer Cyclical | -0.12 |
| 7 | NYT | 5 | Y | missing | missing | high | below |  | 6 | 2 | dead | Communication Services | -1.00 |
| 8 | SIRI | 5 | Y | missing | missing | high | below |  | 6 | 2 | dead | Communication Services | -0.98 |
| 9 | WLY | 5 | Y | missing | missing | high | below |  | 6 | 2 | dead | Communication Services | +0.21 |
| 10 | NEE | 4 | Y | missing | missing | mid | below |  | 10 | 1 | normal | Utilities | -1.02 |
| 11 | NRG | 4 | Y | missing | missing | mid | below |  | 8 | 2 | dead | Utilities | -4.33 |
| 12 | ARM | 4 | Y | missing | missing | low | below |  | 7 | 2 | normal | Technology | +0.55 |
| 13 | JD | 4 | Y | missing | missing | low | below |  | 7 | 2 | normal | Consumer Cyclical | +0.20 |
| 14 | LEN | 4 | Y | missing | missing | high | above |  | 7 | 2 | normal | Consumer Cyclical | -2.15 |
| 15 | MAR | 4 | Y | missing | missing | low | below |  | 7 | 2 | normal | Consumer Cyclical | -0.73 |

Seats 1d n=15 hit=20.0% raw=-1.73 clip30=-1.73 med=-1.02 · universe hit=29.4% med=-0.98.

### 2026-08-20 · rule `A` · pool 1821 · A printed=true

| # | Ticker | score | blue | ab | peer | short | sma20 | ab_up | pts | reds | relvol | sector | 1d |
|---:|---|---:|:---:|---|---|---|---|:---:|---:|---:|---|---|---:|
| 1 | HIMS | 10 | Y | good | good | high | below | Y | 16 | 0 | normal | Healthcare | +6.03 |
| 2 | PACB | 10 | Y | good | good | high | below | Y | 15 | 0 | hot | Healthcare | +9.76 |
| 3 | VERA | 10 | Y | good | good | high | below | Y | 15 | 0 | normal | Healthcare | +3.25 |
| 4 | FSLR | 10 | Y | good | good | high | below | Y | 15 | 1 | normal | Technology | +0.10 |
| 5 | GRND | 10 | Y | good | good | high | below | Y | 15 | 1 | dead | Technology | -0.83 |
| 6 | SOC | 10 | Y | good | good | high | below | Y | 15 | 1 | dead | Energy | +0.59 |
| 7 | RIOT | 10 | Y | good | good | high | below | Y | 14 | 1 | normal | Financial | -5.48 |
| 8 | NYT | 10 | Y | good | good | high | below | Y | 14 | 1 | dead | Communication Services | +0.60 |
| 9 | LZ | 10 | Y | good | good | high | below | Y | 13 | 2 | normal | Industrials | -0.43 |
| 10 | TPR | 10 | Y | good | good | high | below | Y | 13 | 2 | normal | Consumer Cyclical | +0.27 |
| 11 | ETSY | 10 | Y | good | good | high | below | Y | 13 | 2 | dead | Consumer Cyclical | -0.22 |
| 12 | PLNT | 10 | Y | good | good | high | below | Y | 13 | 2 | dead | Consumer Cyclical | +2.03 |
| 13 | YETI | 10 | Y | good | good | high | below | Y | 13 | 2 | dead | Consumer Cyclical | -0.55 |
| 14 | JPM | 9 | Y | good | good | low | below | Y | 17 | 0 | normal | Financial | +0.01 |
| 15 | TEM | 9 | Y | good | good | high | above | Y | 16 | 0 | hot | Healthcare | +9.06 |

Seats 1d n=15 hit=66.7% raw=+1.61 clip30=+1.61 med=+0.27 · universe hit=65.1% med=+0.72.

### 2026-08-21 · rule `A` · pool 1898 · A printed=true

| # | Ticker | score | blue | ab | peer | short | sma20 | ab_up | pts | reds | relvol | sector | 1d |
|---:|---|---:|:---:|---|---|---|---|:---:|---:|---:|---|---|---:|
| 1 | CTRE | 10 | Y | good | good | high | below | Y | 17 | 0 | dead | Real Estate | — |
| 2 | EZPW | 10 | Y | good | good | high | below | Y | 17 | 0 | dead | Financial | — |
| 3 | RZLT | 10 | Y | good | good | high | below | Y | 17 | 0 | dead | Healthcare | — |
| 4 | TCBI | 10 | Y | good | good | high | below | Y | 17 | 0 | dead | Financial | — |
| 5 | DLO | 10 | Y | good | good | high | below | Y | 15 | 1 | normal | Technology | — |
| 6 | OLLI | 10 | Y | good | good | high | below | Y | 15 | 1 | dead | Consumer Defensive | — |
| 7 | RR | 10 | Y | good | good | high | below | Y | 13 | 2 | dead | Industrials | — |
| 8 | AKBA | 9 | Y | good | good | mid | below | Y | 17 | 0 | hot | Healthcare | — |
| 9 | CRSP | 9 | Y | good | good | high | above | Y | 17 | 0 | hot | Healthcare | — |
| 10 | ABR | 9 | Y | good | good | high | below |  | 17 | 0 | normal | Real Estate | — |
| 11 | ALLY | 9 | Y | good | good | mid | below | Y | 17 | 0 | normal | Financial | — |
| 12 | ALT | 9 | Y | good | good | high | below |  | 17 | 0 | normal | Healthcare | — |
| 13 | SOFI | 9 | Y | good | good | high | above | Y | 17 | 0 | normal | Financial | — |
| 14 | TMC | 9 | Y | good | good | high | below |  | 17 | 0 | normal | Basic Materials | — |
| 15 | ALTO | 9 | Y | good | good | high | below |  | 17 | 0 | dead | Basic Materials | — |

1d not settled — names only.

### 2026-08-27 · rule `A` · pool 1686 · A printed=true

| # | Ticker | score | blue | ab | peer | short | sma20 | ab_up | pts | reds | relvol | sector | 1d |
|---:|---|---:|:---:|---|---|---|---|:---:|---:|---:|---|---|---:|
| 1 | KD | 10 | Y | good | good | high | below | Y | 18 | 0 | normal | Technology | — |
| 2 | VYX | 10 | Y | good | good | high | below | Y | 18 | 0 | normal | Technology | — |
| 3 | AVT | 10 | Y | good | good | high | below | Y | 18 | 0 | dead | Technology | — |
| 4 | DAVE | 10 | Y | good | good | high | below | Y | 18 | 0 | dead | Technology | — |
| 5 | SLM | 10 | Y | good | good | high | below | Y | 18 | 0 | dead | Financial | — |
| 6 | DJT | 10 | Y | good | good | high | below | Y | 17 | 0 | normal | Communication Services | — |
| 7 | CBRL | 10 | Y | good | good | high | below | Y | 16 | 1 | dead | Consumer Cyclical | — |
| 8 | CXT | 10 | Y | good | good | high | below | Y | 16 | 1 | dead | Industrials | — |
| 9 | DPZ | 10 | Y | good | good | high | below | Y | 16 | 1 | dead | Consumer Cyclical | — |
| 10 | IP | 10 | Y | good | good | high | below | Y | 16 | 1 | dead | Consumer Cyclical | — |
| 11 | PBI | 10 | Y | good | good | high | below | Y | 16 | 1 | dead | Industrials | — |
| 12 | RVLV | 10 | Y | good | good | high | below | Y | 16 | 1 | dead | Consumer Cyclical | — |
| 13 | SFM | 10 | Y | good | good | high | below | Y | 16 | 1 | dead | Consumer Defensive | — |
| 14 | XRX | 10 | Y | good | good | high | below | Y | 16 | 1 | dead | Industrials | — |
| 15 | BRBR | 10 | Y | good | good | high | below | Y | 15 | 1 | dead | Consumer Defensive | — |

1d not settled — names only.

### 2026-08-30 · rule `A` · pool 1840 · A printed=true

| # | Ticker | score | blue | ab | peer | short | sma20 | ab_up | pts | reds | relvol | sector | 1d |
|---:|---|---:|:---:|---|---|---|---|:---:|---:|---:|---|---|---:|
| 1 | UPBD | 10 | Y | good | good | high | below | Y | 18 | 0 | normal | Technology | — |
| 2 | BAND | 10 | Y | good | good | high | below | Y | 18 | 0 | dead | Technology | — |
| 3 | DXC | 10 | Y | good | good | high | below | Y | 18 | 0 | dead | Technology | — |
| 4 | PARR | 10 | Y | good | good | high | below | Y | 16 | 1 | normal | Energy | — |
| 5 | FND | 10 | Y | good | good | high | below | Y | 14 | 2 | normal | Consumer Cyclical | — |
| 6 | LULU | 10 | Y | good | good | high | below | Y | 14 | 2 | normal | Consumer Cyclical | — |
| 7 | POOL | 10 | Y | good | good | high | below | Y | 14 | 2 | dead | Industrials | — |
| 8 | CRWD | 9 | Y | good | good | low | below | Y | 19 | 0 | normal | Technology | — |
| 9 | AGNC | 9 | Y | good | good | high | below |  | 17 | 0 | normal | Real Estate | — |
| 10 | ES | 9 | Y | good | good | low | below | Y | 17 | 0 | normal | Utilities | — |
| 11 | GLPI | 9 | Y | good | good | mid | below | Y | 17 | 0 | normal | Real Estate | — |
| 12 | SBS | 9 | Y | good | good | low | below | Y | 17 | 0 | normal | Utilities | — |
| 13 | ZIP | 9 | Y | good | good | mid | below | Y | 17 | 0 | normal | Communication Services | — |
| 14 | GENI | 9 | Y | good | good | high | below |  | 17 | 0 | dead | Communication Services | — |
| 15 | PEB | 9 | Y | good | good | high | below |  | 17 | 0 | dead | Real Estate | — |

1d not settled — names only.

### 2026-08-31 · rule `A` · pool 1798 · A printed=true

| # | Ticker | score | blue | ab | peer | short | sma20 | ab_up | pts | reds | relvol | sector | 1d |
|---:|---|---:|:---:|---|---|---|---|:---:|---:|---:|---|---|---:|
| 1 | BMO | 9 | Y | good | good | low | below | Y | 19 | 1 | normal | Financial | — |
| 2 | AMZN | 9 | Y | good | good | low | below | Y | 19 | 2 | normal | Consumer Cyclical | — |
| 3 | RWAY | 9 | Y | good | good | high | above | Y | 17 | 1 | normal | Financial | — |
| 4 | VTS | 9 | Y | good | good | high | above | Y | 16 | 2 | normal | Energy | — |
| 5 | SDGR | 9 | Y | good | good | high | above | Y | 15 | 2 | normal | Healthcare | — |
| 6 | TCOM | 9 | Y | good | good | low | below | Y | 15 | 2 | dead | Consumer Cyclical | — |
| 7 | HDSN | 9 | Y | good | good | mid | below | Y | 14 | 2 | normal | Basic Materials | — |
| 8 | BAH | 9 | Y | good | good | mid | below | Y | 14 | 3 | dead | Industrials | — |
| 9 | CRM | 8 | Y | good | good | mid | above | Y | 25 | 2 | hot | Technology | — |
| 10 | CVE | 8 | Y | good | good | mid | above | Y | 20 | 1 | normal | Energy | — |
| 11 | CVX | 8 | Y | good | good | low | above | Y | 20 | 1 | normal | Energy | — |
| 12 | LIN | 8 | Y | good | good | low | above | Y | 19 | 2 | dead | Basic Materials | — |
| 13 | OXY | 8 | Y | good | good | low | above | Y | 19 | 2 | dead | Energy | — |
| 14 | COST | 8 | Y | good | good | low | below |  | 17 | 1 | normal | Consumer Defensive | — |
| 15 | APD | 8 | Y | good | good | low | above | Y | 17 | 2 | normal | Basic Materials | — |

1d not settled — names only.

### 2026-09-01 · rule `A` · pool 1700 · A printed=true

| # | Ticker | score | blue | ab | peer | short | sma20 | ab_up | pts | reds | relvol | sector | 1d |
|---:|---|---:|:---:|---|---|---|---|:---:|---:|---:|---|---|---:|
| 1 | CTGO | 10 | Y | good | good | high | below | Y | 17 | 3 | dead | Basic Materials | — |
| 2 | GPRO | 10 | Y | good | good | high | below | Y | 16 | 1 | dead | Technology | — |
| 3 | IEP | 10 | Y | good | good | high | below | Y | 16 | 1 | dead | Energy | — |
| 4 | ANNX | 10 | Y | good | good | high | below | Y | 15 | 2 | dead | Healthcare | — |
| 5 | WOLF | 10 | Y | good | good | high | below | Y | 15 | 2 | dead | Technology | — |
| 6 | ADMA | 10 | Y | good | good | high | below | Y | 13 | 2 | dead | Healthcare | — |
| 7 | ALLO | 10 | Y | good | good | high | below | Y | 13 | 2 | dead | Healthcare | — |
| 8 | COGT | 10 | Y | good | good | high | below | Y | 13 | 2 | dead | Healthcare | — |
| 9 | DDD | 10 | Y | good | good | high | below | Y | 13 | 2 | dead | Technology | — |
| 10 | AVAV | 10 | Y | good | good | high | below | Y | 13 | 3 | dead | Industrials | — |
| 11 | ENVX | 10 | Y | good | good | high | below | Y | 13 | 3 | dead | Industrials | — |
| 12 | VRRM | 10 | Y | good | good | high | below | Y | 12 | 2 | dead | Technology | — |
| 13 | DKS | 10 | Y | good | good | high | below | Y | 12 | 4 | dead | Consumer Cyclical | — |
| 14 | FWRG | 10 | Y | good | good | high | below | Y | 12 | 4 | dead | Consumer Cyclical | — |
| 15 | GME | 10 | Y | good | good | high | below | Y | 12 | 4 | dead | Consumer Cyclical | — |

1d not settled — names only.

## What this actually says

1. **A is not a multi-week edge in this panel.** One priced session, and that session was a broad up day.
2. **Blue is not constantly winning.** 14 Aug hit-lift vs a red tape, 17 Aug 10% hit vs a 36% universe, 19 Aug modest hit-lift on a down day, 20 Aug = the tape.
3. **short AND sma20=below tracks the tape**, not a separate engine. It won 13/18/20 and lost 14/17/19 with the market.
4. Use A as a **same-morning seat-filler when the cameras printed**, not as a published expectancy. Re-score after 21 Aug / 27 Aug / 30 Aug 1d settles.
5. If you want one mechanical book tomorrow: run this file against the latest panel and take the 15. Do not hand-merge white / cond=good / join=good — those printed below the base hit on the mine board.

