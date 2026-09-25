# Factor Mine OOS-0914

Rules for this mine: [IRONCLAD_RULES.md](../IRONCLAD_RULES.md).

The frozen rules are logged experiments, not keepers. `keep_bar_met` is false. 12 train fires against the 30-fire bar. Excel's luck test p=0.87.

`oos0914_break10_h2_sx` made +0.28% on the 9 locked test sessions (2026-09-14 through 2026-09-24) after fees, versus random picks -6.87% and IWM -1.44%. Without its best stock (SDGR) that test window was -3.04%. `oos0914_rvol_lg_h1_sx` made -7.57% on the 9 locked test sessions (2026-09-14 through 2026-09-24) after fees, versus random picks -6.87% and IWM -1.44%. Without its best stock (CYPH) that test window was -9.19%. `oos0914_break10_h1_sx` made -15.35% on the 9 locked test sessions (2026-09-14 through 2026-09-24) after fees, versus random picks -6.87% and IWM -1.44%. Without its best stock (HLP) that test window was -16.68%. `oos0914_zero_candle_h2_sx` made +3.87% on the 9 locked test sessions (2026-09-14 through 2026-09-24) after fees, versus random picks -6.87% and IWM -1.44%. Without its best stock (WGS) that test window was +1.33%.

## Train

Train sessions: 2026-08-13 through 2026-09-11 (21 days). RANDOM4 mean -2.79% (seed 20260813, 1000 draws). IWM buy-and-hold -3.02%. Best-of-37 null +4.66% (the random-book percentile a search of this size should expect to win).
War room rules were seen on 2026-09-14 through 2026-09-25. Their clean record starts 2026-09-28. The train numbers are the pre-registered selection window.
Each train row's daily path keeps the Futubull return in `ret_pct`. `ret_pct_flat_15bp` is those same picks and fills priced with a flat 15 bp fee (7.5 bp per side) instead.

| rule | after fees | start-day win rate | fires | win rate | asymmetric | best stock | without best stock | pass |
| --- | ---: | ---: | ---: | ---: | ---: | --- | ---: | --- |
| `lg_hot_h1_sx` | -0.94% | 23.8% | 11 | 50.0% | 0.916 | ASST | -1.15% | no |
| `lg_hot_h2_sx` | +1.58% | 85.7% | 11 | 50.0% | 1.582 | ASST | -1.53% | no |
| `rsi_os_h1_sx` | -8.38% | 14.3% | 9 | 33.3% | 0.453 | SION | -11.78% | no |
| `rsi_os_h2_sx` | -5.41% | 33.3% | 9 | 33.3% | 0.955 | AIIO | -7.91% | no |
| `rvol_lg_h1_sx` | +3.10% | 100.0% | 12 | 50.0% | 1.013 | CMRC | +0.48% | yes |
| `rvol_lg_h2_sx` | +1.92% | 100.0% | 12 | 37.5% | 1.266 | CMRC | -0.68% | no |
| `ret5_up_h1_sx` | -0.02% | 23.8% | 12 | 75.0% | 0.387 | GPRO | -1.12% | no |
| `ret5_up_h2_sx` | -6.63% | 0.0% | 12 | 25.0% | 0.89 | CAPR | -11.34% | no |
| `pullback_h1_sx` | -10.38% | 0.0% | 9 | 20.0% | 0.6 | FCEL | -14.26% | no |
| `pullback_h2_sx` | -6.15% | 4.8% | 9 | 20.0% | 2.687 | FCEL | -15.74% | no |
| `break10_h1_sx` | +2.35% | 100.0% | 12 | 37.5% | 1.041 | CMRC | +0.66% | yes |
| `break10_h2_sx` | +5.97% | 100.0% | 12 | 50.0% | 1.485 | CMRC | +4.22% | yes |
| `macd_x_h1_sx` | +0.56% | 100.0% | 11 | 37.5% | 1.603 | EOSE | -2.24% | no |
| `macd_x_h2_sx` | +1.73% | 95.2% | 11 | 37.5% | 1.986 | EOSE | -3.55% | no |
| `flow_lg_h1_sx` | -5.48% | 0.0% | 2 | 0.0% |  | HAFN | -3.80% | no |
| `flow_lg_h2_sx` | -2.22% | 23.8% | 2 | 50.0% | 0.143 | HAFN | -2.59% | no |
| `yday_white_h1_sx` | +1.68% | 100.0% | 12 | 37.5% | 1.04 | TARS | -0.53% | no |
| `yday_white_h2_sx` | -1.59% | 42.9% | 12 | 25.0% | 0.856 | BAND | -3.09% | no |
| `zero_candle_h1_sx` | +2.67% | 100.0% | 12 | 50.0% | 1.428 | TARS | -0.23% | no |
| `zero_candle_h2_sx` | +0.95% | 100.0% | 12 | 50.0% | 0.755 | SWKS | +1.80% | yes |
| `ridge_px` | -11.77% | 0.0% | 80 | 18.8% | 0.831 | XHG | -13.77% | no |
| `logit_px` | -42.57% | 0.0% | 80 | 40.0% | 0.724 | BYND | -45.54% | no |
| `gbm_px` | -43.74% | 4.8% | 80 | 17.5% | 0.477 | CMRC | -45.71% | no |
| `ens_px` | -22.42% | 4.8% | 80 | 23.8% | 0.498 | CMRC | -24.79% | no |
| `ridge_px_exmega` | -11.77% | 0.0% | 80 | 18.8% | 0.831 | XHG | -13.77% | no |
| `tr01_fpe_top_earn_h3` | -6.03% | 0.0% | 11 | 37.5% | 0.725 | MRNA | -10.69% | no |
| `tr02_dfpe_t3_top_earn_h3` | -6.03% | 0.0% | 11 | 37.5% | 0.725 | MRNA | -10.69% | no |
| `tr03_dfpe_t2_top_earn_h3` | -6.03% | 0.0% | 11 | 37.5% | 0.725 | MRNA | -10.69% | no |
| `tr04_dfpe_t3_pos_earn_h3` | -6.03% | 0.0% | 11 | 37.5% | 0.725 | MRNA | -10.69% | no |
| `tr05_dmcap_t3_top_earn_h3` | -6.03% | 0.0% | 11 | 37.5% | 0.725 | MRNA | -10.69% | no |
| `tr06_dfpe_first_top_earn_h3` | -6.03% | 0.0% | 11 | 37.5% | 0.725 | MRNA | -10.69% | no |
| `tr07_drecom_t3_down_er_h3` | -5.99% | 0.0% | 6 | 33.3% | 0.345 | ASST | -8.70% | no |
| `tr08_dfpe_t1_top_hammer_h2` | +2.00% | 85.7% | 7 | 75.0% | 1.284 | ASST | -1.45% | no |
| `tr09_dfpe_t1_top_hammer_h3` | -6.29% | 0.0% | 7 | 25.0% | 0.383 | ASST | -8.30% | no |
| `tr10_dfpe_t2_pos_hammer_h2` | +2.00% | 85.7% | 7 | 75.0% | 1.284 | ASST | -1.45% | no |
| `tr11_dfpe_first_top_hammer_h2` | +2.00% | 85.7% | 7 | 75.0% | 1.284 | ASST | -1.45% | no |
| `tr12_dfpe_first_top_hammer_h3` | -6.29% | 0.0% | 7 | 25.0% | 0.383 | ASST | -8.30% | no |

## Test days

Each day uses that morning's frozen 09:30 snapshot and the prior close (cash, holdings, fees). A missing print is left on the snapshot's dropped list. The day still locks. Fills: buy at the open; a stop fills at the level, or at the open if the open gaps through it; if the same bar also hits a take-profit, the stop fills first. The flat 15bp column prices those same fills at 7.5 bp per side.

### `oos0914_break10_h2_sx`

Logged experiment, not a keeper. `keep_bar_met` is false.

| date | buys | sells | fees | cash | equity | day | flat 15bp |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |
| 2026-09-14 | — | — | 0 | 10000.0 | 10000.0 | +0.00% | +0.00% |
| 2026-09-15 | — | — | 0 | 10000.0 | 10000.0 | +0.00% | +0.00% |
| 2026-09-16 | INDP,HLP,SDGR,CAI | — | 31.2809 | 0.43 | 10109.01 | +1.09% | +1.33% |
| 2026-09-17 | — | — | 0 | 0.43 | 11445.02 | +13.22% | +13.19% |
| 2026-09-18 | INDP,SDGR,TEM,LVWR | CAI,HLP,INDP,SDGR | 69.4955 | 0.0 | 10979.12 | -4.07% | -3.60% |
| 2026-09-21 | — | — | 0 | 0.0 | 10581.54 | -3.62% | -3.60% |
| 2026-09-22 | GRAL,NUAI,ARM,ARQQ | INDP,LVWR,SDGR,TEM | 49.3574 | 127.57 | 10480.04 | -0.96% | -0.64% |
| 2026-09-23 | FEAM,SVIA | — | 0.6573 | 66.28 | 10418.17 | -0.59% | -0.58% |
| 2026-09-24 | — | ARM,ARQQ,GRAL,NUAI | 11.2467 | 9972.65 | 10028.12 | -3.74% | -3.67% |

### `oos0914_rvol_lg_h1_sx`

Logged experiment, not a keeper. `keep_bar_met` is false.

| date | buys | sells | fees | cash | equity | day | flat 15bp |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |
| 2026-09-14 | — | — | 0 | 10000.0 | 10000.0 | +0.00% | +0.00% |
| 2026-09-15 | — | — | 0 | 10000.0 | 10000.0 | +0.00% | +0.00% |
| 2026-09-16 | HLP,SDGR,CAI,SWKS | — | 24.5412 | 93.69 | 10320.77 | +3.21% | +3.38% |
| 2026-09-17 | HLP,BBNX,IQ,ARQT | CAI,HLP,SDGR,SWKS | 76.9133 | 7.76 | 10273.95 | -0.45% | +0.14% |
| 2026-09-18 | SDGR,TEM,LVWR,CYPH | ARQT,BBNX,HLP,IQ | 89.7761 | 2.77 | 10684.59 | +4.00% | +4.69% |
| 2026-09-21 | FEAM,CYPH,LVWR,USDE | CYPH,LVWR,SDGR,TEM | 85.3223 | 3.07 | 10340.7 | -3.22% | -2.54% |
| 2026-09-22 | GRAL,NUAI,ARM,IVVD | CYPH,FEAM,LVWR,USDE | 90.3268 | 36.85 | 10289.4 | -0.50% | +0.22% |
| 2026-09-23 | VKTX,SVIA,BFLY,INOD | ARM,GRAL,IVVD,NUAI | 56.288 | 77.45 | 9728.25 | -5.45% | -4.91% |
| 2026-09-24 | — | BFLY,INOD,SVIA,VKTX | 15.0856 | 9243.36 | 9243.36 | -4.98% | -4.74% |

### `oos0914_break10_h1_sx`

Logged experiment, not a keeper. `keep_bar_met` is false.

| date | buys | sells | fees | cash | equity | day | flat 15bp |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |
| 2026-09-14 | — | — | 0 | 10000.0 | 10000.0 | +0.00% | +0.00% |
| 2026-09-15 | — | — | 0 | 10000.0 | 10000.0 | +0.00% | +0.00% |
| 2026-09-16 | INDP,HLP,SDGR,CAI | — | 31.2809 | 0.43 | 10109.01 | +1.09% | +1.33% |
| 2026-09-17 | HLP,BBNX,IQ,IOVA | CAI,HLP,INDP,SDGR | 83.7847 | 4.76 | 9980.63 | -1.27% | -0.59% |
| 2026-09-18 | INDP,SDGR,TEM,LVWR | BBNX,HLP,IOVA,IQ | 86.8533 | 20.11 | 9786.33 | -1.95% | -1.21% |
| 2026-09-21 | FEAM,CYPH,TJGC,LVWR | INDP,LVWR,SDGR,TEM | 76.8282 | 0.46 | 9374.8 | -4.21% | -3.51% |
| 2026-09-22 | GRAL,NUAI,ARM,ARQQ | CYPH,FEAM,LVWR,TJGC | 53.4028 | 194.51 | 9339.71 | -0.37% | +0.05% |
| 2026-09-23 | FEAM,VICR,VKTX,SVIA | ARM,ARQQ,GRAL,NUAI | 31.7874 | 202.44 | 8943.42 | -4.24% | -3.94% |
| 2026-09-24 | — | FEAM,SVIA,VICR,VKTX | 21.4111 | 8464.79 | 8464.79 | -5.35% | -5.02% |

### `oos0914_zero_candle_h2_sx`

Logged experiment, not a keeper. `keep_bar_met` is false.

| date | buys | sells | fees | cash | equity | day | flat 15bp |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |
| 2026-09-14 | — | — | 0 | 10000.0 | 10000.0 | +0.00% | +0.00% |
| 2026-09-15 | — | — | 0 | 10000.0 | 10000.0 | +0.00% | +0.00% |
| 2026-09-16 | CAI,BLFS,ADPT,WGS | — | 8.782 | 91.6 | 10108.12 | +1.08% | +1.09% |
| 2026-09-17 | SABR | — | 0.999 | 1.8 | 10669.7 | +5.56% | +5.56% |
| 2026-09-18 | CRWD,NEO,SDGR,TEM | ADPT,BLFS,CAI,WGS | 17.6633 | 200.95 | 10342.94 | -3.06% | -3.04% |
| 2026-09-21 | IOVA | SABR | 1.611 | 220.38 | 10506.05 | +1.58% | +1.59% |
| 2026-09-22 | — | CRWD,NEO,SDGR,TEM | 8.8841 | 10396.53 | 10460.37 | -0.43% | -0.42% |
| 2026-09-23 | A,DXCM,NICE,NTSK | IOVA | 9.2574 | 158.23 | 10373.38 | -0.83% | -0.82% |
| 2026-09-24 | — | — | 0 | 158.23 | 10387.12 | +0.13% | +0.13% |

## Baselines

Same test sessions. RANDOM4 is 1,000 draws of four names from that morning's snapshot, seed 20260813. IWM is buy-and-hold. The plus-15bp column is the Futubull schedule plus 7.5 bp per side.

| baseline | Futubull | Futubull + 15 bp |
| --- | ---: | ---: |
| RANDOM4 mean | -6.05% | -6.87% |
| IWM buy-and-hold | -1.41% | -1.44% |

## Luck check

On the train window the best-of-37 null (RANDOM4) is +4.66%. RANDOM4 itself averaged -2.79% (5th–95th -8.60% to +3.48%). A frozen rule had to clear the RANDOM4 mean and IWM, and stay positive with its best stock removed, before it was named.

## Research shorts

Theme Radar's short books are not in the 37 and were not frozen. Each enters at that session's close and covers h sessions later, or stays open and is marked at the last close when the cover falls past this window. Cost is 15 bp round trip plus 0.3 percent borrow. Days before 2026-09-28 are designed_after.

| rule | after fees | fires |
| --- | ---: | ---: |
| `tr01_fpe_top_earn_h3` | -56.74% | 10 |
| `tr02_dfpe_t3_top_earn_h3` | +7.03% | 15 |
| `tr03_dfpe_t2_top_earn_h3` | +11.21% | 13 |
| `tr04_dfpe_t3_pos_earn_h3` | +0.76% | 32 |
| `tr05_dmcap_t3_top_earn_h3` | -22.53% | 31 |
| `tr06_dfpe_first_top_earn_h3` | +5.88% | 5 |
| `tr07_drecom_t3_down_er_h3` | +0.00% | 0 |
| `tr08_dfpe_t1_top_hammer_h2` | +0.00% | 0 |
| `tr09_dfpe_t1_top_hammer_h3` | +0.00% | 0 |
| `tr10_dfpe_t2_pos_hammer_h2` | +0.00% | 0 |
| `tr11_dfpe_first_top_hammer_h2` | +0.00% | 0 |
| `tr12_dfpe_first_top_hammer_h3` | +0.00% | 0 |
