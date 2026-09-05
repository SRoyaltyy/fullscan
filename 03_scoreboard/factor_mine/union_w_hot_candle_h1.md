# Factor mine action — `union_w_hot_candle_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `w_hot_candle` · size `leftover` · sell `list` · S-boost `none` · rank by w_hot_candle

Cash book **+0.71%** ($10,071) · signal-only (no cash/fees) was +0.69%. Starts YES **8/17**. Fills 138 · skips 56 · realized $+232.24.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `w_hot_candle` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $30.59.

## Each session (cash + holdings state)

| Date | S | Open cash | Open held | Bought | Sold | Close cash | Stock | Equity | Close held | Why |
|---|---:|---:|---|---|---|---:|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | TNDM, IREN, TPG, HIMS, INO, VOR, SLS, BTSG | — | $107.38 | $10,161.33 | $10,268.71 | TNDM×53, IREN×27, TPG×24, HIMS×42, INO×1543, VOR×56, SLS×106, BTSG×20 | BUY TNDM x53 @ 23.33; BUY IREN x27 @ 45.98; BUY TPG x24 @ 50.62; BUY HIMS x42 @ 29.74; BUY INO x1543 @ 0.81; BUY VOR x56 @ 22.01; BUY SLS x106 @ 11.70; BUY BTSG x20 @ 59.80 |
| 2026-08-14 | +5.50 | $107.38 | TNDM×53, IREN×27, TPG×24, HIMS×42, INO×1543, VOR×56, SLS×106, BTSG×20 | QMCO, ZENA, AIRO, ARX, LIFE, BETA, LUNR, VOYG | TNDM, IREN, TPG, HIMS, INO, VOR, SLS, BTSG | $86.84 | $9,923.42 | $10,010.26 | QMCO×52, ZENA×583, AIRO×115, ARX×65, LIFE×36, BETA×50, LUNR×67, VOYG×28 | SELL TNDM (dropped from list after 1 sess (min 1)); SELL IREN (dropped from list after 1 sess (min 1)); SELL TPG (dropped from list after 1 sess (min 1)); SELL HIMS (dropped from list after 1 sess (min 1)); SELL INO (dropped from list after 1 sess (min 1)); SELL VOR (dropped from list after 1 sess (min 1)); SELL SLS (dropped from list after 1 sess (min 1)); SELL BTSG (dropped from list after 1 sess (min 1)); BUY QMCO x52 @ 24.68; BUY ZENA x583 @ 2.20; BUY AIRO x115 @ 11.12; BUY ARX x65 @ 19.57; BUY LIFE x36 @ 35.04; BUY BETA x50 @ 25.21; BUY LUNR x67 @ 19.17; BUY VOYG x28 @ 44.49 |
| 2026-08-17 | +2.25 | $86.84 | QMCO×52, ZENA×583, AIRO×115, ARX×65, LIFE×36, BETA×50, LUNR×67, VOYG×28 | XHG, STDN, HTFL, SMJF, NPWR, NMAX, CAPR, UMAC | QMCO, ZENA, AIRO, ARX, LIFE, BETA, LUNR, VOYG | $5.07 | $9,676.63 | $9,681.70 | XHG×296, STDN×91, HTFL×30, SMJF×122, NPWR×646, NMAX×113, CAPR×180, UMAC×38 | SELL QMCO (dropped from list after 1 sess (min 1)); SELL ZENA (dropped from list after 1 sess (min 1)); SELL AIRO (dropped from list after 1 sess (min 1)); SELL ARX (dropped from list after 1 sess (min 1)); SELL LIFE (dropped from list after 1 sess (min 1)); SELL BETA (dropped from list after 1 sess (min 1)); SELL LUNR (dropped from list after 1 sess (min 1)); SELL VOYG (dropped from list after 1 sess (min 1)); BUY XHG x296 @ 4.19; BUY STDN x91 @ 13.64; BUY HTFL x30 @ 41.23; BUY SMJF x122 @ 10.10; BUY NPWR x646 @ 1.92; BUY NMAX x113 @ 10.97; BUY CAPR x180 @ 6.87; BUY UMAC x38 @ 32.55 |
| 2026-08-18 | -6.20 | $5.07 | XHG×296, STDN×91, HTFL×30, SMJF×122, NPWR×646, NMAX×113, CAPR×180, UMAC×38 | — | XHG, STDN, HTFL, SMJF, NPWR, NMAX, UMAC | $8,228.49 | $1,274.40 | $9,502.89 | CAPR×180 | SELL XHG (dropped from list after 1 sess (min 1)); SELL STDN (dropped from list after 1 sess (min 1)); SELL HTFL (dropped from list after 1 sess (min 1)); SELL SMJF (dropped from list after 1 sess (min 1)); SELL NPWR (dropped from list after 1 sess (min 1)); SELL NMAX (dropped from list after 1 sess (min 1)); SELL UMAC (dropped from list after 1 sess (min 1)); hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | -7.20 | $8,228.49 | CAPR×180 | — | CAPR | $9,520.12 | $0.00 | $9,520.12 | — | SELL CAPR (dropped from list after 2 sess (min 1)); hard-red S=-7.20 sit; no new buys |
| 2026-08-20 | +1.12 | $9,520.12 | — | MRNA, CYPH, ABCL, SENS, ALEC, BTGO, IMMX, BBNX | — | $142.06 | $9,147.34 | $9,289.40 | MRNA×7, CYPH×1034, ABCL×100, SENS×133, ALEC×495, BTGO×180, IMMX×91, BBNX×59 | BUY MRNA x7 @ 150.14; BUY CYPH x1034 @ 1.15; BUY ABCL x100 @ 11.81; BUY SENS x133 @ 8.91; BUY ALEC x495 @ 2.40; BUY BTGO x180 @ 6.61; BUY IMMX x91 @ 12.98; BUY BBNX x59 @ 20.00 |
| 2026-08-21 | +3.25 | $142.06 | MRNA×7, CYPH×1034, ABCL×100, SENS×133, ALEC×495, BTGO×180, IMMX×91, BBNX×59 | XHG, ARCT, IOVA, DFDV, XXI, INO | ABCL, SENS, ALEC, BTGO, IMMX, BBNX | $0.48 | $9,770.79 | $9,771.27 | MRNA×7, CYPH×1034, XHG×269, ARCT×108, IOVA×133, DFDV×299, XXI×188, INO×972 | SELL ABCL (dropped from list after 1 sess (min 1)); SELL SENS (dropped from list after 1 sess (min 1)); SELL ALEC (dropped from list after 1 sess (min 1)); SELL BTGO (dropped from list after 1 sess (min 1)); SELL IMMX (dropped from list after 1 sess (min 1)); SELL BBNX (dropped from list after 1 sess (min 1)); BUY XHG x269 @ 4.49; BUY ARCT x108 @ 11.13; BUY IOVA x133 @ 9.08; BUY DFDV x299 @ 4.04; BUY XXI x188 @ 6.42; BUY INO x972 @ 1.23 |
| 2026-08-24 | -5.17 | $0.48 | MRNA×7, CYPH×1034, XHG×269, ARCT×108, IOVA×133, DFDV×299, XXI×188, INO×972 | — | MRNA, CYPH, XHG, ARCT, IOVA, DFDV, XXI, INO | $10,139.87 | $0.00 | $10,139.87 | — | SELL MRNA (dropped from list after 2 sess (min 1)); SELL CYPH (dropped from list after 2 sess (min 1)); SELL XHG (dropped from list after 1 sess (min 1)); SELL ARCT (dropped from list after 1 sess (min 1)); SELL IOVA (dropped from list after 1 sess (min 1)); SELL DFDV (dropped from list after 1 sess (min 1)); SELL XXI (dropped from list after 1 sess (min 1)); SELL INO (dropped from list after 1 sess (min 1)); hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | +1.80 | $10,139.87 | — | CYPH, XHG, ASST, AU, RUM, OMER, BMNR, TRLV | — | $83.17 | $9,934.32 | $10,017.49 | CYPH×745, XHG×315, ASST×60, AU×10, RUM×135, OMER×67, BMNR×51, TRLV×115 | BUY CYPH x745 @ 1.70; BUY XHG x315 @ 4.02; BUY ASST x60 @ 20.90; BUY AU x10 @ 119.46; BUY RUM x135 @ 9.36; BUY OMER x67 @ 18.75; BUY BMNR x51 @ 24.73; BUY TRLV x115 @ 11.02 |
| 2026-08-26 | +2.02 | $83.17 | CYPH×745, XHG×315, ASST×60, AU×10, RUM×135, OMER×67, BMNR×51, TRLV×115 | — | — | $83.17 | $10,029.78 | $10,112.95 | CYPH×745, XHG×315, ASST×60, AU×10, RUM×135, OMER×67, BMNR×51, TRLV×115 | hold CYPH,XHG,ASST,AU,RUM,OMER,BMNR,TRLV |
| 2026-08-27 | — | $83.17 | CYPH×745, XHG×315, ASST×60, AU×10, RUM×135, OMER×67, BMNR×51, TRLV×115 | MOS, DLO, RRC, GEN, SLI, PLTR, CRK, PGY | CYPH, XHG, ASST, AU, RUM, OMER, BMNR, TRLV | $116.39 | $10,031.43 | $10,147.82 | MOS×50, DLO×80, RRC×30, GEN×43, SLI×484, PLTR×7, CRK×89, PGY×57 | SELL CYPH (dropped from list after 2 sess (min 1)); SELL XHG (dropped from list after 2 sess (min 1)); SELL ASST (dropped from list after 2 sess (min 1)); SELL AU (dropped from list after 2 sess (min 1)); SELL RUM (dropped from list after 2 sess (min 1)); SELL OMER (dropped from list after 2 sess (min 1)); SELL BMNR (dropped from list after 2 sess (min 1)); SELL TRLV (dropped from list after 2 sess (min 1)); BUY MOS x50 @ 24.84; BUY DLO x80 @ 15.60; BUY RRC x30 @ 40.72; BUY GEN x43 @ 28.89; BUY SLI x484 @ 2.59; BUY PLTR x7 @ 170.60; BUY CRK x89 @ 14.09; BUY PGY x57 @ 21.97 |
| 2026-08-28 | +0.75 | $116.39 | MOS×50, DLO×80, RRC×30, GEN×43, SLI×484, PLTR×7, CRK×89, PGY×57 | FIGR, TRLV, VIRT, ZYME, NIQ, AMTX, NVAX, WPM | MOS, DLO, RRC, GEN, SLI, PLTR, CRK, PGY | $79.50 | $10,067.60 | $10,147.10 | FIGR×33, TRLV×111, VIRT×19, ZYME×43, NIQ×67, AMTX×678, NVAX×139, WPM×8 | SELL MOS (dropped from list after 1 sess (min 1)); SELL DLO (dropped from list after 1 sess (min 1)); SELL RRC (dropped from list after 1 sess (min 1)); SELL GEN (dropped from list after 1 sess (min 1)); SELL SLI (dropped from list after 1 sess (min 1)); SELL PLTR (dropped from list after 1 sess (min 1)); SELL CRK (dropped from list after 1 sess (min 1)); SELL PGY (dropped from list after 1 sess (min 1)); BUY FIGR x33 @ 37.42; BUY TRLV x111 @ 11.38; BUY VIRT x19 @ 65.42; BUY ZYME x43 @ 29.33; BUY NIQ x67 @ 18.79; BUY AMTX x678 @ 1.87; BUY NVAX x139 @ 9.12; BUY WPM x8 @ 155.89 |
| 2026-08-31 | -5.85 | $79.50 | FIGR×33, TRLV×111, VIRT×19, ZYME×43, NIQ×67, AMTX×678, NVAX×139, WPM×8 | — | FIGR, TRLV, ZYME, NIQ, AMTX, NVAX, WPM | $8,899.45 | $1,261.41 | $10,160.86 | VIRT×19 | SELL FIGR (dropped from list after 1 sess (min 1)); SELL TRLV (dropped from list after 1 sess (min 1)); SELL ZYME (dropped from list after 1 sess (min 1)); SELL NIQ (dropped from list after 1 sess (min 1)); SELL AMTX (dropped from list after 1 sess (min 1)); SELL NVAX (dropped from list after 1 sess (min 1)); SELL WPM (dropped from list after 1 sess (min 1)); hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | -6.30 | $8,899.45 | VIRT×19 | — | VIRT | $10,144.55 | $0.00 | $10,144.55 | — | SELL VIRT (dropped from list after 2 sess (min 1)); hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | -3.83 | $10,144.55 | — | — | — | $10,144.55 | $0.00 | $10,144.55 | — | hard-red S=-3.83 sit; no new buys |
| 2026-09-03 | -0.90 | $10,144.55 | — | MRNA, XHG, ARCT, CAN, OMER, TRLV, SG, VIRT | — | $61.13 | $9,996.63 | $10,057.76 | MRNA×8, XHG×355, ARCT×77, CAN×4226, OMER×66, TRLV×107, SG×197, VIRT×19 | BUY MRNA x8 @ 151.40; BUY XHG x355 @ 3.57; BUY ARCT x77 @ 16.46; BUY CAN x4226 @ 0.30; BUY OMER x66 @ 18.97; BUY TRLV x107 @ 11.78; BUY SG x197 @ 6.43; BUY VIRT x19 @ 65.64 |
| 2026-09-04 | — | $61.13 | MRNA×8, XHG×355, ARCT×77, CAN×4226, OMER×66, TRLV×107, SG×197, VIRT×19 | HQ, ZYME, NIQ | MRNA, ARCT, CAN | $30.59 | $10,040.75 | $10,071.34 | XHG×355, OMER×66, TRLV×107, SG×197, VIRT×19, HQ×76, ZYME×41, NIQ×70 | SELL MRNA (dropped from list after 1 sess (min 1)); SELL ARCT (dropped from list after 1 sess (min 1)); SELL CAN (dropped from list after 1 sess (min 1)); BUY HQ x76 @ 17.06; BUY ZYME x41 @ 31.34; BUY NIQ x70 @ 18.66 |

## Fills (what was bought / sold)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 53 | $23.33 | $2.15 | — | $8,761.36 | rank by w_hot_candle; rank w_hot_candle; list flatten; ⚪; ret5=+19.7; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 27 | $45.98 | $2.07 | — | $7,517.83 | rank by w_hot_candle; rank w_hot_candle; list flatten; ⚪; ret5=+12.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 24 | $50.62 | $2.06 | — | $6,300.81 | rank by w_hot_candle; rank w_hot_candle; list flatten; ⚪; ret5=+6.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 42 | $29.74 | $2.12 | — | $5,049.62 | rank by w_hot_candle; rank w_hot_candle; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 1543 | $0.81 | $17.13 | — | $3,782.66 | rank by w_hot_candle; rank w_hot_candle; list flatten; ⚪; ret5=+13.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `VOR` | 56 | $22.01 | $2.16 | — | $2,547.94 | rank by w_hot_candle; rank w_hot_candle; list flatten; ⚪; ret5=+0.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 106 | $11.70 | $2.31 | — | $1,305.43 | rank by w_hot_candle; rank w_hot_candle; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 20 | $59.80 | $2.05 | — | $107.38 | rank by w_hot_candle; rank w_hot_candle; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-14 09:30 ET | **SELL** | `TNDM` | 53 | $22.92 | $2.17 | $-26.05 | $1,319.97 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `IREN` | 27 | $44.09 | $2.09 | $-55.19 | $2,508.31 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TPG` | 24 | $55.29 | $2.08 | $+107.86 | $3,833.19 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **SELL** | `HIMS` | 42 | $29.15 | $2.14 | $-29.03 | $5,055.35 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `INO` | 1543 | $0.93 | $19.25 | $+148.79 | $6,471.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `VOR` | 56 | $23.33 | $2.18 | $+69.58 | $7,775.40 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `SLS` | 106 | $12.40 | $2.34 | $+69.56 | $9,087.46 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `BTSG` | 20 | $59.65 | $2.07 | $-7.12 | $10,278.39 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **BUY** | `QMCO` | 52 | $24.68 | $2.15 | — | $8,992.89 | rank by w_hot_candle; rank w_hot_candle; list yday_gainer; 🔵; ⚪; ret5=+111.3; leftover $1284.80 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ZENA` | 583 | $2.20 | $7.52 | — | $7,702.77 | rank by w_hot_candle; rank w_hot_candle; list yday_gainer; 🔵; ⚪; ret5=+54.3; leftover $1284.80 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `AIRO` | 115 | $11.12 | $2.33 | — | $6,421.63 | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; leftover $1284.80 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 65 | $19.57 | $2.19 | — | $5,147.40 | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $1284.80 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `LIFE` | 36 | $35.04 | $2.10 | — | $3,883.86 | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ⚪; ret5=+16.7; leftover $1284.80 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BETA` | 50 | $25.21 | $2.14 | — | $2,621.22 | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ⚪; ret5=+15.5; leftover $1284.80 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `LUNR` | 67 | $19.17 | $2.19 | — | $1,334.64 | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ⚪; ret5=+17.6; leftover $1284.80 | join🟢 sector🟢 gen🟢 news🔴 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `VOYG` | 28 | $44.49 | $2.07 | — | $86.84 | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ret5=+15.6; leftover $1284.80 | join🟢 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-17 09:30 ET | **SELL** | `QMCO` | 52 | $24.83 | $2.17 | $+3.49 | $1,375.84 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ZENA` | 583 | $2.08 | $7.63 | $-82.19 | $2,583.76 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `AIRO` | 115 | $9.57 | $2.36 | $-182.95 | $3,681.95 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 65 | $19.57 | $2.21 | $-4.39 | $4,951.79 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `LIFE` | 36 | $34.03 | $2.12 | $-40.58 | $6,174.76 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BETA` | 50 | $24.61 | $2.16 | $-34.30 | $7,403.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `LUNR` | 67 | $20.25 | $2.21 | $+67.96 | $8,757.63 | dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SELL** | `VOYG` | 28 | $42.12 | $2.09 | $-70.53 | $9,934.90 | dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `XHG` | 296 | $4.19 | $3.82 | — | $8,690.84 | rank by w_hot_candle; rank w_hot_candle; list yday_mover; ⚪; ret5=+291.8; leftover $1241.86 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `STDN` | 91 | $13.64 | $2.26 | — | $7,447.34 | rank by w_hot_candle; rank w_hot_candle; list yday_gainer; 🔵; ⚪; ret5=+57.7; leftover $1241.86 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `HTFL` | 30 | $41.23 | $2.08 | — | $6,208.36 | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; ret5=+46.0; leftover $1241.86 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `SMJF` | 122 | $10.10 | $2.36 | — | $4,973.80 | rank by w_hot_candle; rank w_hot_candle; list mover_buy; ret5=+22.8; leftover $1241.86 | join🔴 sector🔴 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `NPWR` | 646 | $1.92 | $8.33 | — | $3,725.15 | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; ⚪; ret5=+28.3; leftover $1241.86 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `NMAX` | 113 | $10.97 | $2.33 | — | $2,483.21 | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; ⚪; ret5=+21.2; leftover $1241.86 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `CAPR` | 180 | $6.87 | $2.53 | — | $1,244.08 | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; ret5=+62.6; leftover $1241.86 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `UMAC` | 38 | $32.55 | $2.10 | — | $5.07 | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; ⚪; ret5=+30.4; leftover $1241.86 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `XHG` | 296 | $3.94 | $3.88 | $-81.70 | $1,167.44 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `STDN` | 91 | $13.31 | $2.29 | $-34.58 | $2,376.36 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🔴 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `HTFL` | 30 | $41.50 | $2.10 | $+3.92 | $3,619.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `SMJF` | 122 | $10.45 | $2.39 | $+37.96 | $4,891.77 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `NPWR` | 646 | $1.70 | $8.45 | $-158.90 | $5,981.52 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `NMAX` | 113 | $10.31 | $2.36 | $-79.27 | $7,144.19 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `UMAC` | 38 | $28.59 | $2.12 | $-154.71 | $8,228.49 | dropped from list after 1 sess (min 1) | — |
| 2026-08-19 09:30 ET | **SELL** | `CAPR` | 180 | $7.19 | $2.57 | $+52.50 | $9,520.12 | dropped from list after 2 sess (min 1) | — |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 7 | $150.14 | $2.01 | — | $8,467.13 | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; 🔵; ret5=+173.9; leftover $1190.02 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CYPH` | 1034 | $1.15 | $13.34 | — | $7,264.69 | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+62.6; leftover $1190.02 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ABCL` | 100 | $11.81 | $2.29 | — | $6,080.90 | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $1190.02 | join🟢 sector🟡 gen🟢 news🔴 digest🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `SENS` | 133 | $8.91 | $2.39 | — | $4,893.48 | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ⚪; ret5=+14.9; leftover $1190.02 | join🟡 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ALEC` | 495 | $2.40 | $6.39 | — | $3,699.10 | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ret5=+13.0; leftover $1190.02 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BTGO` | 180 | $6.61 | $2.53 | — | $2,507.67 | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; 🔵; ret5=+25.3; leftover $1190.02 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟡 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IMMX` | 91 | $12.98 | $2.26 | — | $1,324.22 | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $1190.02 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BBNX` | 59 | $20.00 | $2.17 | — | $142.06 | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ⚪; ret5=+15.5; leftover $1190.02 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `ABCL` | 100 | $11.57 | $2.32 | $-29.11 | $1,296.74 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `SENS` | 133 | $9.24 | $2.42 | $+39.08 | $2,523.24 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟡 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `ALEC` | 495 | $2.28 | $6.48 | $-72.26 | $3,645.36 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BTGO` | 180 | $6.95 | $2.57 | $+57.00 | $4,893.79 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IMMX` | 91 | $13.36 | $2.29 | $+30.03 | $6,107.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BBNX` | 59 | $19.50 | $2.19 | $-33.85 | $7,255.58 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `XHG` | 269 | $4.49 | $3.47 | — | $6,044.30 | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ret5=+12.7; leftover $1209.26 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🔴 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 108 | $11.13 | $2.31 | — | $4,839.94 | rank by w_hot_candle; rank w_hot_candle; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $1209.26 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `IOVA` | 133 | $9.08 | $2.39 | — | $3,629.91 | rank by w_hot_candle; rank w_hot_candle; list yday_gainer; 🔵; ⚪; ret5=+37.9; leftover $1209.26 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `DFDV` | 299 | $4.04 | $3.86 | — | $2,418.10 | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+23.6; leftover $1209.26 | join🟡 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `XXI` | 188 | $6.42 | $2.55 | — | $1,208.58 | rank by w_hot_candle; rank w_hot_candle; list yday_gainer; ret5=+23.8; leftover $1209.26 | join🔴 sector🔴 gen🟢 news🟡 digest🟡 ab🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `INO` | 972 | $1.23 | $12.54 | — | $0.48 | rank by w_hot_candle; rank w_hot_candle; list mover_buy; 🔵; ⚪; ret5=+34.4; leftover $1209.26 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `MRNA` | 7 | $142.70 | $2.03 | $-56.12 | $997.35 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 1034 | $1.83 | $13.53 | $+676.26 | $2,876.05 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `XHG` | 269 | $4.24 | $3.52 | $-74.24 | $4,013.08 | dropped from list after 1 sess (min 1) | join🟡 sector🟡 gen🔴 news🟡 digest🟢 ab🔴 peer🟢 vol🔴 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 108 | $13.26 | $2.34 | $+225.38 | $5,442.82 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `IOVA` | 133 | $8.05 | $2.42 | $-141.80 | $6,511.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `DFDV` | 299 | $4.15 | $3.92 | $+25.12 | $7,747.98 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `XXI` | 188 | $6.60 | $2.60 | $+28.69 | $8,986.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `INO` | 972 | $1.20 | $12.71 | $-54.41 | $10,139.87 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟡 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `CYPH` | 745 | $1.70 | $9.61 | — | $8,863.76 | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+120.5; leftover $1267.48 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `XHG` | 315 | $4.02 | $4.06 | — | $7,593.40 | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ret5=+16.1; leftover $1267.48 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ASST` | 60 | $20.90 | $2.17 | — | $6,337.23 | rank by w_hot_candle; rank w_hot_candle; list yday_gainer; 🔵; ret5=+47.9; leftover $1267.48 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 10 | $119.46 | $2.02 | — | $5,140.61 | rank by w_hot_candle; rank w_hot_candle; list mover_buy; 🔵; ⚪; ret5=+25.9; leftover $1267.48 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `RUM` | 135 | $9.36 | $2.40 | — | $3,874.62 | rank by w_hot_candle; rank w_hot_candle; list yday_gainer; 🔵; ret5=+21.3; leftover $1267.48 | join🔴 sector🟡 gen🟡 news🟢 digest🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `OMER` | 67 | $18.75 | $2.19 | — | $2,616.17 | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; ret5=+12.1; leftover $1267.48 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BMNR` | 51 | $24.73 | $2.14 | — | $1,352.80 | rank by w_hot_candle; rank w_hot_candle; list yday_gainer; ret5=+26.3; leftover $1267.48 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟡 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `TRLV` | 115 | $11.02 | $2.33 | — | $83.17 | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ret5=+15.0; leftover $1267.48 | join🔴 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 vol🟡 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `CYPH` | 745 | $1.60 | $9.74 | $-93.85 | $1,265.42 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `XHG` | 315 | $3.81 | $4.13 | $-74.34 | $2,461.45 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ASST` | 60 | $20.72 | $2.19 | $-15.16 | $3,702.46 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `AU` | 10 | $119.80 | $2.04 | $-0.66 | $4,898.42 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `RUM` | 135 | $10.07 | $2.43 | $+91.03 | $6,255.44 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `OMER` | 67 | $18.96 | $2.21 | $+9.67 | $7,523.55 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BMNR` | 51 | $24.24 | $2.16 | $-29.30 | $8,757.62 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `TRLV` | 115 | $11.22 | $2.36 | $+18.30 | $10,045.56 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `MOS` | 50 | $24.84 | $2.14 | — | $8,801.42 | rank by w_hot_candle; rank w_hot_candle; list flatten; ret5=+13.0; leftover $1255.69 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟢 |
| 2026-08-27 09:30 ET | **BUY** | `DLO` | 80 | $15.60 | $2.23 | — | $7,551.19 | rank by w_hot_candle; rank w_hot_candle; list mover_buy; 🔵; ret5=+7.1; leftover $1255.69 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 30 | $40.72 | $2.08 | — | $6,327.51 | rank by w_hot_candle; rank w_hot_candle; list flatten; ret5=+1.8; leftover $1255.69 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `GEN` | 43 | $28.89 | $2.12 | — | $5,083.12 | rank by w_hot_candle; rank w_hot_candle; list mover_buy; 🔵; ret5=+1.6; leftover $1255.69 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 484 | $2.59 | $6.24 | — | $3,823.32 | rank by w_hot_candle; rank w_hot_candle; list flatten; ret5=+4.2; leftover $1255.69 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `PLTR` | 7 | $170.60 | $2.01 | — | $2,627.11 | rank by w_hot_candle; rank w_hot_candle; list mover_buy; 🔵; ret5=+3.4; leftover $1255.69 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 89 | $14.09 | $2.26 | — | $1,370.84 | rank by w_hot_candle; rank w_hot_candle; list flatten; ret5=+1.1; leftover $1255.69 | join🟢 sector🔴 gen🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `PGY` | 57 | $21.97 | $2.16 | — | $116.39 | rank by w_hot_candle; rank w_hot_candle; list mover_buy; 🔵; ret5=+0.6; leftover $1255.69 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `MOS` | 50 | $24.00 | $2.16 | $-46.30 | $1,314.23 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `DLO` | 80 | $15.33 | $2.25 | $-26.08 | $2,538.37 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `RRC` | 30 | $41.44 | $2.10 | $+17.42 | $3,779.47 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `GEN` | 43 | $29.83 | $2.14 | $+36.16 | $5,060.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `SLI` | 484 | $2.60 | $6.33 | $-7.74 | $6,312.09 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `PLTR` | 7 | $178.75 | $2.03 | $+53.01 | $7,561.31 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `CRK` | 89 | $14.42 | $2.28 | $+24.83 | $8,842.41 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `PGY` | 57 | $22.93 | $2.18 | $+50.38 | $10,147.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `FIGR` | 33 | $37.42 | $2.09 | — | $8,910.29 | rank by w_hot_candle; rank w_hot_candle; list yday_mover; ret5=+24.4; leftover $1268.40 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `TRLV` | 111 | $11.38 | $2.32 | — | $7,644.78 | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; ret5=+15.0; leftover $1268.40 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 heat🟡 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `VIRT` | 19 | $65.42 | $2.05 | — | $6,399.76 | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; ret5=+13.2; leftover $1268.40 | join🟢 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ZYME` | 43 | $29.33 | $2.12 | — | $5,136.45 | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,ohlc_hot; ret5=+14.1; leftover $1268.40 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `NIQ` | 67 | $18.79 | $2.19 | — | $3,875.33 | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; ret5=+7.6; leftover $1268.40 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `AMTX` | 678 | $1.87 | $8.75 | — | $2,598.72 | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; ret5=+16.9; leftover $1268.40 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `NVAX` | 139 | $9.12 | $2.41 | — | $1,328.63 | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; ret5=+11.1; leftover $1268.40 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `WPM` | 8 | $155.89 | $2.01 | — | $79.50 | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; ret5=+17.6; leftover $1268.40 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `FIGR` | 33 | $35.50 | $2.11 | $-67.56 | $1,248.89 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `TRLV` | 111 | $12.41 | $2.35 | $+109.65 | $2,624.05 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟢 digest🟢 judge🟢 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `ZYME` | 43 | $28.27 | $2.14 | $-49.84 | $3,837.52 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🔴 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `NIQ` | 67 | $19.20 | $2.21 | $+23.07 | $5,121.71 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🔴 news🔴 digest🟢 judge🟡 ab🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `AMTX` | 678 | $1.90 | $8.87 | $+2.72 | $6,401.04 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `NVAX` | 139 | $9.23 | $2.44 | $+10.44 | $7,681.57 | dropped from list after 1 sess (min 1) | join🟡 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `WPM` | 8 | $152.49 | $2.03 | $-31.25 | $8,899.45 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-01 09:30 ET | **SELL** | `VIRT` | 19 | $65.64 | $2.07 | $+0.07 | $10,144.55 | dropped from list after 2 sess (min 1) | — |
| 2026-09-03 09:30 ET | **BUY** | `MRNA` | 8 | $151.40 | $2.01 | — | $8,931.33 | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; 🔵; ret5=+129.2; leftover $1268.07 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `XHG` | 355 | $3.57 | $4.58 | — | $7,659.40 | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; ret5=+16.1; leftover $1268.07 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 77 | $16.46 | $2.22 | — | $6,389.76 | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+63.4; leftover $1268.07 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CAN` | 4226 | $0.30 | $25.36 | — | $5,096.61 | rank by w_hot_candle; rank w_hot_candle; list yday_mover; 🔵; ret5=+54.3; leftover $1268.07 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `OMER` | 66 | $18.97 | $2.19 | — | $3,842.40 | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; ret5=+12.1; leftover $1268.07 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `TRLV` | 107 | $11.78 | $2.31 | — | $2,579.63 | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ret5=+15.0; leftover $1268.07 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `SG` | 197 | $6.43 | $2.58 | — | $1,310.34 | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; ret5=+11.3; leftover $1268.07 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `VIRT` | 19 | $65.64 | $2.05 | — | $61.13 | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ret5=+13.2; leftover $1268.07 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `MRNA` | 8 | $145.95 | $2.03 | $-47.65 | $1,226.70 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `ARCT` | 77 | $16.77 | $2.24 | $+19.40 | $2,515.74 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CAN` | 4226 | $0.34 | $27.76 | $+115.92 | $3,924.82 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `HQ` | 76 | $17.06 | $2.22 | — | $2,626.04 | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; ret5=+17.3; leftover $1308.27 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 ab🟡 heat🔴 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `ZYME` | 41 | $31.34 | $2.11 | — | $1,338.99 | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ⚪; ret5=+14.1; leftover $1308.27 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `NIQ` | 70 | $18.66 | $2.20 | — | $30.59 | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ret5=+7.6; leftover $1308.27 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🟡 buy🟡 |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `FDMT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AVAH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OABI` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `IOVA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CRDL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `KYTX` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OVID` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `IVVD` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `NMAX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SENS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ARX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `KURA` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `HAE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `REAX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `OMER` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ALM` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `TRLV` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SG` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INFQ` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ZYME` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `KGC` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HMY` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-26 | `CYPH` | no_price | no 09:30 open — carry |
| 2026-08-26 | `ASST` | no_price | no 09:30 open — carry |
| 2026-08-26 | `AU` | no_price | no 09:30 open — carry |
| 2026-08-26 | `RUM` | no_price | no 09:30 open — carry |
| 2026-08-26 | `BMNR` | no_price | no 09:30 open — carry |
| 2026-08-26 | `USDE` | no_price | no 09:30 open |
| 2026-08-26 | `SENS` | no_price | no 09:30 open |
| 2026-08-26 | `SG` | no_price | no 09:30 open |
| 2026-08-26 | `ZYME` | no_price | no 09:30 open |
| 2026-08-26 | `NIQ` | no_price | no 09:30 open |
| 2026-08-31 | `USDE` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CYPH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `XHG` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PURR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CAN` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `OMER` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `SG` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `CYPH` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `OMER` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `TRLV` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `SG` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NVAX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CELH` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `TXG` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `INO` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `USDE` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `XHG` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `OMER` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `TRLV` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `SG` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ZYME` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `NOG` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `NVAX` | hard_red | hard-red S=-3.83 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `XHG` | 355 | 2026-09-03 @ $3.57 | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; ret5=+16.1; leftover $1268.07 |
| `OMER` | 66 | 2026-09-03 @ $18.97 | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; ret5=+12.1; leftover $1268.07 |
| `TRLV` | 107 | 2026-09-03 @ $11.78 | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ret5=+15.0; leftover $1268.07 |
| `SG` | 197 | 2026-09-03 @ $6.43 | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; ret5=+11.3; leftover $1268.07 |
| `VIRT` | 19 | 2026-09-03 @ $65.64 | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ret5=+13.2; leftover $1268.07 |
| `HQ` | 76 | 2026-09-04 @ $17.06 | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; ret5=+17.3; leftover $1308.27 |
| `ZYME` | 41 | 2026-09-04 @ $31.34 | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ⚪; ret5=+14.1; leftover $1308.27 |
| `NIQ` | 70 | 2026-09-04 @ $18.66 | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ret5=+7.6; leftover $1308.27 |
