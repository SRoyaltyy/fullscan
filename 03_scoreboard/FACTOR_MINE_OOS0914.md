# Factor Mine OOS-0914

`oos0914_break10_h2_sx` made +0.28% on the 9 locked test sessions (2026-09-14 through 2026-09-24) after fees, versus random picks -6.87% and IWM -1.44%. Without its best stock (SDGR) that test window was -3.04%. `oos0914_break10_h2_s8` made -3.72% on the 9 locked test sessions (2026-09-14 through 2026-09-24) after fees, versus random picks -6.87% and IWM -1.44%. Without its best stock (SDGR) that test window was -7.23%. `oos0914_rvol_lg_h1_sx` made -7.57% on the 9 locked test sessions (2026-09-14 through 2026-09-24) after fees, versus random picks -6.87% and IWM -1.44%. Without its best stock (CYPH) that test window was -9.19%. `oos0914_break10_h1_sx` made -15.35% on the 9 locked test sessions (2026-09-14 through 2026-09-24) after fees, versus random picks -6.87% and IWM -1.44%. Without its best stock (HLP) that test window was -16.68%. `oos0914_zero_candle_h2_s8` made +3.87% on the 9 locked test sessions (2026-09-14 through 2026-09-24) after fees, versus random picks -6.87% and IWM -1.44%. Without its best stock (WGS) that test window was +1.33%. 2026-09-25 is not in that total: the session is not closed and it has no frozen 09:30 snapshot. The nightly land appends it once that snapshot locks. On the train window, `oos0914_break10_h2_sx` also cleared the best-of-40 random null (+4.89%). The other frozen rules cleared the pre-registered bar (random average, IWM, and a positive result with the best stock removed) and sit below that luck check.

## Train

Train sessions: 2026-08-13 through 2026-09-11 (21 days). RANDOM4 mean -2.79% (seed 20260813, 1000 draws). IWM buy-and-hold -3.02%. Best-of-40 null +4.89% (the random-book percentile a search of this size should expect to win).

| rule | after fees | start-day win rate | best stock | without best stock | pass |
| --- | ---: | ---: | --- | ---: | --- |
| `lg_hot_h1_sx` | -0.94% | 23.8% | ASST | -1.15% | no |
| `lg_hot_h1_s8` | -3.70% | 0.0% | ASST | -3.91% | no |
| `lg_hot_h2_sx` | +1.58% | 85.7% | ASST | -1.53% | no |
| `lg_hot_h2_s8` | -1.25% | 9.5% | ASST | -5.95% | no |
| `rsi_os_h1_sx` | -8.38% | 14.3% | SION | -11.78% | no |
| `rsi_os_h1_s8` | -7.34% | 33.3% | SION | -10.22% | no |
| `rsi_os_h2_sx` | -5.41% | 33.3% | AIIO | -7.91% | no |
| `rsi_os_h2_s8` | -5.69% | 33.3% | AIIO | -8.33% | no |
| `rvol_lg_h1_sx` | +3.10% | 100.0% | CMRC | +0.48% | yes |
| `rvol_lg_h1_s8` | +0.34% | 95.2% | CMRC | -2.29% | no |
| `rvol_lg_h2_sx` | +1.92% | 100.0% | CMRC | -0.68% | no |
| `rvol_lg_h2_s8` | -2.50% | 33.3% | CMRC | -5.05% | no |
| `ret5_up_h1_sx` | -0.02% | 23.8% | GPRO | -1.12% | no |
| `ret5_up_h1_s8` | -2.80% | 0.0% | GPRO | -3.87% | no |
| `ret5_up_h2_sx` | -6.63% | 0.0% | CAPR | -11.34% | no |
| `ret5_up_h2_s8` | -7.75% | 0.0% | CAPR | -12.08% | no |
| `pullback_h1_sx` | -10.38% | 0.0% | FCEL | -14.26% | no |
| `pullback_h1_s8` | -9.20% | 0.0% | FCEL | -12.44% | no |
| `pullback_h2_sx` | -6.15% | 4.8% | FCEL | -15.74% | no |
| `pullback_h2_s8` | -4.99% | 19.1% | FCEL | -13.98% | no |
| `break10_h1_sx` | +2.35% | 100.0% | CMRC | +0.66% | yes |
| `break10_h1_s8` | -0.38% | 42.9% | CMRC | -2.07% | no |
| `break10_h2_sx` | +5.97% | 100.0% | CMRC | +4.22% | yes |
| `break10_h2_s8` | +3.15% | 100.0% | CMRC | +1.40% | yes |
| `macd_x_h1_sx` | +0.56% | 100.0% | EOSE | -2.24% | no |
| `macd_x_h1_s8` | +0.56% | 100.0% | EOSE | -2.24% | no |
| `macd_x_h2_sx` | +1.73% | 95.2% | EOSE | -3.55% | no |
| `macd_x_h2_s8` | +1.19% | 95.2% | EOSE | -4.06% | no |
| `flow_lg_h1_sx` | -5.48% | 0.0% | HAFN | -3.80% | no |
| `flow_lg_h1_s8` | -5.48% | 0.0% | HAFN | -3.80% | no |
| `flow_lg_h2_sx` | -2.22% | 23.8% | HAFN | -2.59% | no |
| `flow_lg_h2_s8` | -2.22% | 23.8% | HAFN | -2.59% | no |
| `yday_white_h1_sx` | +1.68% | 100.0% | TARS | -0.53% | no |
| `yday_white_h1_s8` | +1.35% | 100.0% | TARS | -0.85% | no |
| `yday_white_h2_sx` | -1.59% | 42.9% | BAND | -3.09% | no |
| `yday_white_h2_s8` | -1.12% | 42.9% | BAND | -2.61% | no |
| `zero_candle_h1_sx` | +2.67% | 100.0% | TARS | -0.23% | no |
| `zero_candle_h1_s8` | +2.67% | 100.0% | TARS | -0.23% | no |
| `zero_candle_h2_sx` | +0.95% | 100.0% | SWKS | +1.80% | yes |
| `zero_candle_h2_s8` | +0.95% | 100.0% | SWKS | +1.80% | yes |

## Test days

Each day uses that morning's frozen 09:30 snapshot and the prior close (cash, holdings, fees). A missing print is left on the snapshot's dropped list. The day still locks. Fills: buy at the open; a stop fills at the level, or at the open if the open gaps through it; if the same bar also hits a take-profit, the stop fills first.

### `oos0914_break10_h2_sx`

| date | buys | sells | fees | cash | equity | day |
| --- | --- | --- | ---: | ---: | ---: | ---: |
| 2026-09-14 | — | — | 0 | 10000.0 | 10000.0 | +0.00% |
| 2026-09-15 | — | — | 0 | 10000.0 | 10000.0 | +0.00% |
| 2026-09-16 | INDP,HLP,SDGR,CAI | — | 31.2809 | 0.43 | 10109.01 | +1.09% |
| 2026-09-17 | — | — | 0 | 0.43 | 11445.02 | +13.22% |
| 2026-09-18 | INDP,SDGR,TEM,LVWR | CAI,HLP,INDP,SDGR | 69.4955 | 0.0 | 10979.12 | -4.07% |
| 2026-09-21 | — | — | 0 | 0.0 | 10581.54 | -3.62% |
| 2026-09-22 | GRAL,NUAI,ARM,ARQQ | INDP,LVWR,SDGR,TEM | 49.3574 | 127.57 | 10480.04 | -0.96% |
| 2026-09-23 | FEAM,SVIA | — | 0.6573 | 66.28 | 10418.17 | -0.59% |
| 2026-09-24 | — | ARM,ARQQ,GRAL,NUAI | 11.2467 | 9972.65 | 10028.12 | -3.74% |

### `oos0914_break10_h2_s8`

| date | buys | sells | fees | cash | equity | day |
| --- | --- | --- | ---: | ---: | ---: | ---: |
| 2026-09-14 | — | — | 0 | 10000.0 | 10000.0 | +0.00% |
| 2026-09-15 | — | — | 0 | 10000.0 | 10000.0 | +0.00% |
| 2026-09-16 | INDP,HLP,SDGR,CAI | INDP | 40.2234 | 2291.28 | 10207.43 | +2.07% |
| 2026-09-17 | BBNX,IQ,IOVA | BBNX | 15.6107 | 703.42 | 10979.58 | +7.56% |
| 2026-09-18 | INDP,SDGR,TEM,LVWR | CAI,HLP,SDGR,INDP | 62.5354 | 2113.53 | 10541.71 | -3.99% |
| 2026-09-21 | FEAM,CYPH,TJGC | IOVA,IQ,LVWR,CYPH,TJGC | 50.1482 | 4278.35 | 10019.75 | -4.95% |
| 2026-09-22 | GRAL,NUAI,ARM,ARQQ | SDGR,TEM,NUAI | 18.594 | 2379.89 | 10112.23 | +0.92% |
| 2026-09-23 | FEAM,VICR,VKTX,SVIA | FEAM,FEAM,VKTX,SVIA | 26.3131 | 2740.05 | 9851.51 | -2.58% |
| 2026-09-24 | — | ARM,ARQQ,GRAL | 6.411 | 8799.44 | 9627.62 | -2.27% |

### `oos0914_rvol_lg_h1_sx`

| date | buys | sells | fees | cash | equity | day |
| --- | --- | --- | ---: | ---: | ---: | ---: |
| 2026-09-14 | — | — | 0 | 10000.0 | 10000.0 | +0.00% |
| 2026-09-15 | — | — | 0 | 10000.0 | 10000.0 | +0.00% |
| 2026-09-16 | HLP,SDGR,CAI,SWKS | — | 24.5412 | 93.69 | 10320.77 | +3.21% |
| 2026-09-17 | HLP,BBNX,IQ,ARQT | CAI,HLP,SDGR,SWKS | 76.9133 | 7.76 | 10273.95 | -0.45% |
| 2026-09-18 | SDGR,TEM,LVWR,CYPH | ARQT,BBNX,HLP,IQ | 89.7761 | 2.77 | 10684.59 | +4.00% |
| 2026-09-21 | FEAM,CYPH,LVWR,USDE | CYPH,LVWR,SDGR,TEM | 85.3223 | 3.07 | 10340.7 | -3.22% |
| 2026-09-22 | GRAL,NUAI,ARM,IVVD | CYPH,FEAM,LVWR,USDE | 90.3268 | 36.85 | 10289.4 | -0.50% |
| 2026-09-23 | VKTX,SVIA,BFLY,INOD | ARM,GRAL,IVVD,NUAI | 56.288 | 77.45 | 9728.25 | -5.45% |
| 2026-09-24 | — | BFLY,INOD,SVIA,VKTX | 15.0856 | 9243.36 | 9243.36 | -4.98% |

### `oos0914_break10_h1_sx`

| date | buys | sells | fees | cash | equity | day |
| --- | --- | --- | ---: | ---: | ---: | ---: |
| 2026-09-14 | — | — | 0 | 10000.0 | 10000.0 | +0.00% |
| 2026-09-15 | — | — | 0 | 10000.0 | 10000.0 | +0.00% |
| 2026-09-16 | INDP,HLP,SDGR,CAI | — | 31.2809 | 0.43 | 10109.01 | +1.09% |
| 2026-09-17 | HLP,BBNX,IQ,IOVA | CAI,HLP,INDP,SDGR | 83.7847 | 4.76 | 9980.63 | -1.27% |
| 2026-09-18 | INDP,SDGR,TEM,LVWR | BBNX,HLP,IOVA,IQ | 86.8533 | 20.11 | 9786.33 | -1.95% |
| 2026-09-21 | FEAM,CYPH,TJGC,LVWR | INDP,LVWR,SDGR,TEM | 76.8282 | 0.46 | 9374.8 | -4.21% |
| 2026-09-22 | GRAL,NUAI,ARM,ARQQ | CYPH,FEAM,LVWR,TJGC | 53.4028 | 194.51 | 9339.71 | -0.37% |
| 2026-09-23 | FEAM,VICR,VKTX,SVIA | ARM,ARQQ,GRAL,NUAI | 31.7874 | 202.44 | 8943.42 | -4.24% |
| 2026-09-24 | — | FEAM,SVIA,VICR,VKTX | 21.4111 | 8464.79 | 8464.79 | -5.35% |

### `oos0914_zero_candle_h2_s8`

| date | buys | sells | fees | cash | equity | day |
| --- | --- | --- | ---: | ---: | ---: | ---: |
| 2026-09-14 | — | — | 0 | 10000.0 | 10000.0 | +0.00% |
| 2026-09-15 | — | — | 0 | 10000.0 | 10000.0 | +0.00% |
| 2026-09-16 | CAI,BLFS,ADPT,WGS | — | 8.782 | 91.6 | 10108.12 | +1.08% |
| 2026-09-17 | SABR | — | 0.999 | 1.8 | 10669.7 | +5.56% |
| 2026-09-18 | CRWD,NEO,SDGR,TEM | ADPT,BLFS,CAI,WGS | 17.6633 | 200.95 | 10342.94 | -3.06% |
| 2026-09-21 | IOVA | SABR | 1.611 | 220.38 | 10506.05 | +1.58% |
| 2026-09-22 | — | CRWD,NEO,SDGR,TEM | 8.8841 | 10396.53 | 10460.37 | -0.43% |
| 2026-09-23 | A,DXCM,NICE,NTSK | IOVA | 9.2574 | 158.23 | 10373.38 | -0.83% |
| 2026-09-24 | — | — | 0 | 158.23 | 10387.12 | +0.13% |

## Baselines

Same test sessions. RANDOM4 is 1,000 draws of four names from that morning's snapshot, seed 20260813. IWM is buy-and-hold. The plus-15bp column is the Futubull schedule plus 7.5 bp per side.

| baseline | Futubull | Futubull + 15 bp |
| --- | ---: | ---: |
| RANDOM4 mean | -6.05% | -6.87% |
| IWM buy-and-hold | -1.41% | -1.44% |

## Luck check

On the train window the best-of-40 null (RANDOM4) is +4.89%. RANDOM4 itself averaged -2.79% (5th–95th -8.60% to +3.48%). A frozen rule had to clear the RANDOM4 mean and IWM, and stay positive with its best stock removed, before it was named.
