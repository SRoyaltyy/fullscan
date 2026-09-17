# Excel factor-mine Phase B — fee-aware combo prove

status=DONE verdict=**FAIL** TIME-SPLIT cutoff=2026-07-06 tickers=3940 name-days=1084066 univ=mcap>$50M & vol>100000 combos_scored=407 KEEP=0

Research only. Live `flatten_robust` is not imported and is not written.

## Headline

FAIL. No open-knowable combo cleared the Cyrus KEEP bar on holdout (cutoff 2026-07-06). Best material prove after-fee WR: `FQ|J_lt0` n=432 after-fee WR 54.9%. Hypothesis `J_ge0|ER_m1` prove n=602 after-fee WR 45.2%. Lift-only is not KEEP.

## KEEP bar

Cyrus KEEP: **≥30 prove fires** and **after-fee H win rate > 55%**. After-fee H = open-to-close minus 15 bp Futubull (`FEE_RT=0.0015`). A fire is a name-day where every atom is true at the 09:30 open. **Lift-only is never KEEP.** Thin n that prints >55% is FAIL. Discovery may rank and expand combinations; it cannot KEEP. After-fee H is the equal-weight day-mean minus 15 bp Futubull, not dollar-weighted.

## Clock lock

- Gate: `OPEN_SAME_ROW_LABELS + CLOCK_MAP`
- Fill OPEN same-row: `A, B, C, G, J, K, L, M, O, IR, IS, IT`
- Value OPEN same-row (44): `A, C, J, Q, Z, AC, AH, BT, BV, CG, CH, DC, DE, EB, EK, EN, EP, EQ, ER, ES, ET, EU, EV, FQ, FR, FS, FU, GD, GE, GF, HF, HG, HW, II, IR, IT, IY, IZ, JB, JC, JD, JE, JF, JL`
- Lag-only close: `DF, DG, DH, BB, BQ, BU`
- Same-row leak abort: `DF, BB, BQ`
- Open-44 tally subs: `AH, JB, JC, FQ, FR, ER, EP, EN`
- Leak check: **PASS**
- Split: TIME-SPLIT last 30% of session dates (locked cutoff `2026-07-06` when present on tape). Discovery feature date is strictly before cutoff. Same-day H/I labels on a discovery date do not cross the cutoff.
- Live: `flatten_robust` not imported, not written.

## What was scored

Open-knowable singles from holdout-surviving deep-corr atoms that Yahoo OHLC can reconstruct (`J_ge0`, `ER_m1`, `prior_bear_engulf`, `el_neg`, `prior_bearish`, `prior_hanging`) plus open-gate letters (J_lt0, J_le-1, ER_p1, JC, JB, FR_ge1, EP_ge03, EN_ge2, prior_bullish, prior_doji, prior_hammer, prior_bull_engulf, prior_morning, prior_evening, BQ_l1_neg, BU_l1_neg, el_pos, BB_l1). Expansion is systematic: every single, every pair among 26 seeds, every triple among 8 core seeds. Fill-paint atoms from the lift board (fill0_M_green, fill0_A_red, fill0_B_red, fill0_L_red, N_last20_r>=6, N_last10_r>=3, …) need Excel grids and are **not scored** on this cut — they are not invented from Yahoo.

## Hypothesis cards

### `J_ge0`

FAIL — n=88262, after-fee WR 41.6% ≤ 55%.
Discovery n=464999 after-fee WR 44.6% mean net=-0.0015. Prove mean net=-0.0027.
Deep-corr lift on I-green is discarded as a KEEP input.

### `ER_m1`

FAIL — n=19083, after-fee WR 46.8% ≤ 55%.
Discovery n=100230 after-fee WR 47.4% mean net=-0.0008. Prove mean net=-0.0013.
Deep-corr lift on I-green is discarded as a KEEP input.

### `prior_bear_engulf`

FAIL — n=20525, after-fee WR 43.8% ≤ 55%.
Discovery n=106877 after-fee WR 46.2% mean net=-0.0012. Prove mean net=-0.0019.
Deep-corr lift on I-green is discarded as a KEEP input.

### `J_ge0|ER_m1`

FAIL — n=602, after-fee WR 45.2% ≤ 55%.
Discovery n=2622 after-fee WR 39.4% mean net=-0.0059. Prove mean net=-0.0023.
Deep-corr lift on I-green is discarded as a KEEP input.

### `J_ge0|prior_bear_engulf`

FAIL — n=1875, after-fee WR 41.1% ≤ 55%.
Discovery n=7854 after-fee WR 42.2% mean net=-0.0035. Prove mean net=-0.0016.
Deep-corr lift on I-green is discarded as a KEEP input.

### `J_ge0|el_neg`

FAIL — n=9640, after-fee WR 40.5% ≤ 55%.
Discovery n=40090 after-fee WR 42.0% mean net=-0.0024. Prove mean net=-0.0026.
Deep-corr lift on I-green is discarded as a KEEP input.

### `J_ge0|prior_bearish`

FAIL — n=10524, after-fee WR 41.3% ≤ 55%.
Discovery n=43850 after-fee WR 42.5% mean net=-0.0022. Prove mean net=-0.0024.
Deep-corr lift on I-green is discarded as a KEEP input.

### `J_ge0|prior_hanging`

FAIL — n=1069, after-fee WR 41.3% ≤ 55%.
Discovery n=5417 after-fee WR 42.9% mean net=-0.0018. Prove mean net=-0.0022.
Deep-corr lift on I-green is discarded as a KEEP input.

### `ER_m1|prior_bear_engulf`

FAIL — n=7062, after-fee WR 47.0% ≤ 55%.
Discovery n=37406 after-fee WR 47.4% mean net=-0.0008. Prove mean net=-0.0009.
Deep-corr lift on I-green is discarded as a KEEP input.

### `J_ge0|ER_m1|prior_bear_engulf`

FAIL — n=155, after-fee WR 51.0% ≤ 55%.
Discovery n=782 after-fee WR 38.9% mean net=-0.0102. Prove mean net=+0.0086.
Deep-corr lift on I-green is discarded as a KEEP input.

## KEEP cards

**None.** No combo cleared ≥30 prove fires and >55% after-fee WR.

## Prove (holdout) — after-fee H

| rule | kind | prove n | prove after-fee WR | prove verdict | disc n | disc WR | walk folds >50% |
|---|---|---:|---:|---|---:|---:|---:|
| `ER_m1|prior_hanging|AH_ge1` | combo3 | 24 | 58.3% | **FAIL** | 173 | 51.4% | 2/3 |
| `FQ|J_lt0` | combo2 | 432 | 54.9% | **FAIL** | 1765 | 53.5% | 2/3 |
| `prior_hanging|JB` | combo2 | 11 | 54.5% | **FAIL** | 134 | 45.5% | 0/3 |
| `JB|prior_hammer` | combo2 | 11 | 54.5% | **FAIL** | 98 | 39.8% | 0/3 |
| `FQ|J_le-1` | combo2 | 298 | 54.0% | **FAIL** | 1177 | 52.2% | 2/3 |
| `J_le-1|prior_bull_engulf` | combo2 | 527 | 53.3% | **FAIL** | 1965 | 52.0% | 1/3 |
| `prior_bear_engulf|JB` | combo2 | 240 | 52.5% | **FAIL** | 1695 | 41.7% | 0/3 |
| `J_le-1|prior_hammer` | combo2 | 288 | 52.1% | **FAIL** | 1430 | 54.7% | 2/3 |
| `ER_m1|prior_hanging` | combo2 | 39 | 51.3% | **FAIL** | 258 | 48.4% | 1/3 |
| `ER_m1|el_neg|prior_hanging` | combo3 | 39 | 51.3% | **FAIL** | 258 | 48.4% | 1/3 |
| `ER_m1|prior_bearish|prior_hanging` | combo3 | 39 | 51.3% | **FAIL** | 258 | 48.4% | 1/3 |
| `JB|prior_evening` | combo2 | 197 | 51.3% | **FAIL** | 1361 | 41.3% | 0/3 |
| `J_ge0|ER_m1|prior_bear_engulf` | combo3 | 155 | 51.0% | **FAIL** | 782 | 38.9% | 0/3 |
| `J_lt0|prior_bull_engulf` | combo2 | 1470 | 50.5% | **FAIL** | 5918 | 50.4% | 1/3 |
| `EN_ge2|prior_hammer` | combo2 | 184 | 50.0% | **FAIL** | 1146 | 47.4% | 0/3 |
| `J_le-1|el_pos` | combo2 | 5407 | 48.9% | **FAIL** | 22531 | 51.9% | 3/3 |
| `J_le-1|prior_bullish` | combo2 | 5792 | 48.7% | **FAIL** | 24370 | 51.8% | 3/3 |
| `J_le-1|prior_morning` | combo2 | 410 | 48.3% | **FAIL** | 1563 | 51.8% | 2/3 |
| `ER_m1|prior_bear_engulf|AH_ge1` | combo3 | 3800 | 47.8% | **FAIL** | 21019 | 46.0% | 0/3 |
| `prior_bear_engulf|EP_ge03` | combo2 | 4752 | 47.7% | **FAIL** | 25574 | 46.7% | 0/3 |
| `J_lt0|prior_morning` | combo2 | 1150 | 47.7% | **FAIL** | 5016 | 51.0% | 2/3 |
| `ER_m1|EP_ge03` | combo2 | 13994 | 47.4% | **FAIL** | 75034 | 47.3% | 0/3 |
| `J_le-1|prior_doji` | combo2 | 3385 | 47.4% | **FAIL** | 14699 | 50.5% | 2/3 |
| `J_le-1|BB_l1` | combo2 | 3385 | 47.4% | **FAIL** | 14699 | 50.5% | 2/3 |
| `ER_m1|prior_bearish|AH_ge1` | combo3 | 9452 | 47.3% | **FAIL** | 51847 | 46.4% | 0/3 |
| `ER_m1|el_neg|AH_ge1` | combo3 | 9451 | 47.3% | **FAIL** | 51845 | 46.4% | 0/3 |
| `prior_bear_engulf|AH_ge1` | combo2 | 4908 | 47.2% | **FAIL** | 26339 | 45.7% | 0/3 |
| `prior_bear_engulf|prior_bearish|AH_ge1` | combo3 | 4908 | 47.2% | **FAIL** | 26339 | 45.7% | 0/3 |
| `prior_bear_engulf|el_neg|AH_ge1` | combo3 | 4887 | 47.2% | **FAIL** | 26192 | 45.7% | 0/3 |
| `J_lt0|EP_ge03` | combo2 | 18082 | 47.2% | **FAIL** | 95414 | 47.9% | 0/3 |
| `J_lt0|ER_p1` | combo2 | 106 | 47.2% | **FAIL** | 485 | 51.8% | 3/3 |
| `J_lt0|EN_ge2` | combo2 | 5829 | 47.2% | **FAIL** | 31841 | 46.8% | 0/3 |
| `prior_bearish|prior_bullish` | combo2 | 1839 | 47.1% | **FAIL** | 9189 | 47.2% | 0/3 |
| `prior_bearish|prior_doji` | combo2 | 1839 | 47.1% | **FAIL** | 9189 | 47.2% | 0/3 |
| `prior_bearish|BB_l1` | combo2 | 1839 | 47.1% | **FAIL** | 9189 | 47.2% | 0/3 |
| `J_le-1|EN_ge2` | combo2 | 4608 | 47.1% | **FAIL** | 25063 | 47.1% | 0/3 |
| `J_le-1|EP_ge03` | combo2 | 16955 | 47.1% | **FAIL** | 89575 | 47.9% | 0/3 |
| `prior_hanging|EN_ge2` | combo2 | 202 | 47.0% | **FAIL** | 1259 | 46.8% | 0/3 |
| `ER_m1|prior_bear_engulf` | combo2 | 7062 | 47.0% | **FAIL** | 37406 | 47.4% | 0/3 |
| `ER_m1|prior_bear_engulf|el_neg` | combo3 | 7062 | 47.0% | **FAIL** | 37406 | 47.4% | 0/3 |
| `ER_m1|prior_bear_engulf|prior_bearish` | combo3 | 7062 | 47.0% | **FAIL** | 37406 | 47.4% | 0/3 |
| `ER_m1|prior_bearish` | combo2 | 16551 | 47.0% | **FAIL** | 86841 | 47.4% | 0/3 |
| `J_ge0|ER_m1|el_neg` | combo3 | 500 | 47.0% | **FAIL** | 2085 | 38.8% | 0/3 |
| `J_ge0|ER_m1|prior_bearish` | combo3 | 500 | 47.0% | **FAIL** | 2085 | 38.8% | 0/3 |
| `ER_m1|el_neg` | combo2 | 16550 | 47.0% | **FAIL** | 86839 | 47.4% | 0/3 |
| `ER_m1|el_neg|prior_bearish` | combo3 | 16550 | 47.0% | **FAIL** | 86839 | 47.4% | 0/3 |
| `ER_m1|AH_ge1` | combo2 | 10927 | 47.0% | **FAIL** | 59914 | 46.5% | 0/3 |
| `ER_m1|J_le-1` | combo2 | 18087 | 46.9% | **FAIL** | 95912 | 47.7% | 0/3 |
| `ER_m1|J_lt0` | combo2 | 18481 | 46.9% | **FAIL** | 97608 | 47.6% | 0/3 |
| `ER_m1` | single | 19083 | 46.8% | **FAIL** | 100230 | 47.4% | 0/3 |
| `el_neg|EP_ge03` | combo2 | 14791 | 46.8% | **FAIL** | 78755 | 47.1% | 0/3 |
| `J_lt0|prior_hammer` | combo2 | 876 | 46.8% | **FAIL** | 4845 | 49.6% | 1/3 |
| `JB|prior_bull_engulf` | combo2 | 231 | 46.8% | **FAIL** | 1741 | 41.6% | 0/3 |
| `prior_bearish|EP_ge03` | combo2 | 14853 | 46.7% | **FAIL** | 79120 | 47.1% | 0/3 |
| `ER_m1|EN_ge2` | combo2 | 2480 | 46.7% | **FAIL** | 15418 | 46.2% | 0/3 |
| `AH_ge1|EN_ge2` | combo2 | 4316 | 46.6% | **FAIL** | 27796 | 44.8% | 0/3 |
| `ER_m1|FR_ge1` | combo2 | 10454 | 46.6% | **FAIL** | 55343 | 46.9% | 0/3 |
| `EP_ge03|prior_evening` | combo2 | 3676 | 46.6% | **FAIL** | 19738 | 46.1% | 0/3 |
| `J_le-1|ER_p1` | combo2 | 88 | 46.6% | **FAIL** | 380 | 51.1% | 3/3 |
| `ER_m1|BQ_l1_neg` | combo2 | 13644 | 46.5% | **FAIL** | 71037 | 47.8% | 1/3 |

## Discovery (not KEEP)

Discovery is for expansion and honesty only. A discovery >55% print is not a call.

| rule | disc n | disc after-fee WR | disc mean net |
|---|---:|---:|---:|
| `J_le-1|prior_hammer` | 1430 | 54.7% | +0.0043 |
| `FQ|J_lt0` | 1765 | 53.5% | +0.0010 |
| `FQ|J_le-1` | 1177 | 52.2% | -0.0006 |
| `J_le-1|prior_bull_engulf` | 1965 | 52.0% | -0.0006 |
| `J_le-1|el_pos` | 22531 | 51.9% | +0.0013 |
| `J_le-1|prior_morning` | 1563 | 51.8% | -0.0005 |
| `J_le-1|prior_bullish` | 24370 | 51.8% | +0.0012 |
| `J_lt0|ER_p1` | 485 | 51.8% | +0.0015 |
| `ER_m1|prior_hanging|AH_ge1` | 173 | 51.4% | +0.0006 |
| `J_le-1|ER_p1` | 380 | 51.1% | +0.0025 |
| `J_lt0|prior_morning` | 5016 | 51.0% | -0.0002 |
| `J_le-1|prior_doji` | 14699 | 50.5% | +0.0005 |
| `J_le-1|BB_l1` | 14699 | 50.5% | +0.0005 |
| `J_lt0|prior_bull_engulf` | 5918 | 50.4% | -0.0003 |
| `FQ|prior_hammer` | 175 | 49.7% | +0.0026 |
| `J_lt0|prior_hammer` | 4845 | 49.6% | +0.0012 |
| `JC|JB` | 119 | 48.7% | +0.0174 |
| `J_lt0|el_pos` | 73197 | 48.6% | -0.0003 |
| `prior_hanging|J_le-1` | 6712 | 48.6% | -0.0001 |
| `J_lt0|prior_bullish` | 78505 | 48.5% | -0.0004 |
| `ER_m1|prior_hanging` | 258 | 48.4% | -0.0037 |
| `ER_m1|el_neg|prior_hanging` | 258 | 48.4% | -0.0037 |
| `ER_m1|prior_bearish|prior_hanging` | 258 | 48.4% | -0.0037 |
| `prior_hanging|EP_ge03` | 1571 | 48.2% | -0.0004 |
| `J_le-1|JC` | 101687 | 48.1% | -0.0006 |

## Walk-forward (discovery folds, not KEEP)

Three chronological folds **inside discovery**. Holdout dates are never in these folds. Used to see whether a disc print was one lucky slice.

| rule | fold1 n / WR | fold2 n / WR | fold3 n / WR |
|---|---|---|---|
| `J_ge0` | 151653 / 42.3% | 155005 / 46.3% | 158341 / 45.0% |
| `ER_m1` | 31015 / 45.8% | 32838 / 49.6% | 36377 / 46.8% |
| `prior_bear_engulf` | 35469 / 44.6% | 35234 / 47.5% | 36174 / 46.5% |
| `J_ge0|ER_m1` | 851 / 39.7% | 741 / 40.6% | 1030 / 38.3% |
| `J_ge0|prior_bear_engulf` | 2430 / 43.0% | 2210 / 40.3% | 3214 / 42.9% |
| `J_ge0|el_neg` | 13200 / 42.0% | 11534 / 40.9% | 15356 / 42.9% |
| `J_ge0|prior_bearish` | 14502 / 42.6% | 12745 / 41.3% | 16603 / 43.4% |
| `J_ge0|prior_hanging` | 2146 / 41.3% | 1647 / 43.2% | 1624 / 44.8% |
| `ER_m1|prior_bear_engulf` | 11259 / 46.5% | 12543 / 48.4% | 13604 / 47.2% |
| `J_ge0|ER_m1|prior_bear_engulf` | 228 / 36.4% | 202 / 38.1% | 352 / 40.9% |

## Day-book overlay (j_winrate, context)

Same constants (`WIN_BAR=0.55`, `MIN_FIRES=30`). Fire = the presence book ticker set differs from the same-day no-rule set. Win = rule book mean after-fee H beats that no-rule book. This does **not** rescue a name-day FAIL. Reported for hypothesis + KEEP rows only.

| rule | unranked fire | vol_top8 fire |
|---|---|---|
| `J_ge0|ER_m1|prior_bear_engulf` | FAIL 54.3% (19/35) | FAIL 48.6% (17/35) |
| `ER_m1|prior_bear_engulf` | FAIL 49.0% (24/49) | CLEAR 59.2% (29/49) |
| `ER_m1` | FAIL 40.8% (20/49) | CLEAR 59.2% (29/49) |
| `J_ge0|ER_m1` | FAIL 29.8% (14/47) | FAIL 34.0% (16/47) |
| `prior_bear_engulf` | FAIL 51.0% (25/49) | CLEAR 55.1% (27/49) |
| `J_ge0` | FAIL 40.8% (20/49) | FAIL 43.8% (21/48) |
| `J_ge0|prior_hanging` | FAIL 46.9% (23/49) | FAIL 46.9% (23/49) |
| `J_ge0|prior_bearish` | FAIL 34.7% (17/49) | FAIL 49.0% (24/49) |
| `J_ge0|prior_bear_engulf` | FAIL 44.9% (22/49) | FAIL 51.0% (25/49) |
| `J_ge0|el_neg` | FAIL 28.6% (14/49) | FAIL 46.9% (23/49) |

## Fill-paint atoms not scored

Deep-corr lift survivors that need Excel fill grids (not reconstructed from Yahoo OHLC on this cut): `fill0_M_green`, `fill0_A_red`, `fill0_B_red`, `fill0_L_red`, `N_last20_r>=6`, `N_last10_r>=3`, `N_last20_r>=3`, `M_last10_g>=3`, `M_last5_g>r`, `M_last10_g>r`, `H_last5_r>=3`, `I_last5_r>=3`, `J_last10_g>=8`, `in_A_red_region`, `in_A_green_region`. Do not invent fill clocks.

## Explicitly not live

No combo is wired into `flatten_robust` or cash/paper. A KEEP here is a research card, not a ship.

## Source

`excel_clock_gate.py` / `CLOCK_MAP.md` / `OPEN_SAME_ROW_LABELS.md` · `excel_open_features.py` · `j_winrate.py` (`WIN_BAR`, `MIN_FIRES`, `FEE_RT=0.0015`) · deep-corr board `EXCEL_DEEP_CORR_MINE.md` (lift only; finished FAIL on this bar). Research only.
