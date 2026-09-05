# Factor mine action — `union_w_hot_cond_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `w_hot_cond` · size `leftover` · sell `list` · S-boost `none` · rank by w_hot_cond

Cash book **+8.44%** ($10,844) · signal-only (no cash/fees) was +10.80%. Starts YES **11/17**. Fills 100 · skips 152 · realized $+578.82.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `w_hot_cond` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $41.76.

## Each session (cash + holdings state)

| Date | S | Open cash | Open held | Bought | Sold | Close cash | Stock | Equity | Close held | Why |
|---|---:|---:|---|---|---|---:|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | IREN, TNDM, TPG, INO, HIMS, SLS, VOR, BTSG | — | $107.38 | $10,161.33 | $10,268.71 | IREN×27, TNDM×53, TPG×24, INO×1543, HIMS×42, SLS×106, VOR×56, BTSG×20 | BUY IREN x27 @ 45.98; BUY TNDM x53 @ 23.33; BUY TPG x24 @ 50.62; BUY INO x1543 @ 0.81; BUY HIMS x42 @ 29.74; BUY SLS x106 @ 11.70; BUY VOR x56 @ 22.01; BUY BTSG x20 @ 59.80 |
| 2026-08-14 | +5.50 | $107.38 | IREN×27, TNDM×53, TPG×24, INO×1543, HIMS×42, SLS×106, VOR×56, BTSG×20 | ZENA, AIRO, BZAI | — | $69.59 | $10,441.72 | $10,511.32 | IREN×27, TNDM×53, TPG×24, INO×1543, HIMS×42, SLS×106, VOR×56, BTSG×20, ZENA×6, AIRO×1, BZAI×17 | BUY ZENA x6 @ 2.20; BUY AIRO x1 @ 11.12; BUY BZAI x17 @ 0.77 |
| 2026-08-17 | +2.25 | $69.59 | IREN×27, TNDM×53, TPG×24, INO×1543, HIMS×42, SLS×106, VOR×56, BTSG×20, ZENA×6, AIRO×1, BZAI×17 | XHG, CAPR, NPWR | — | $46.41 | $10,537.28 | $10,583.70 | IREN×27, TNDM×53, TPG×24, INO×1543, HIMS×42, SLS×106, VOR×56, BTSG×20, ZENA×6, AIRO×1, BZAI×17, XHG×2, CAPR×1, NPWR×4 | BUY XHG x2 @ 4.19; BUY CAPR x1 @ 6.87; BUY NPWR x4 @ 1.92 |
| 2026-08-18 | -6.20 | $46.41 | IREN×27, TNDM×53, TPG×24, INO×1543, HIMS×42, SLS×106, VOR×56, BTSG×20, ZENA×6, AIRO×1, BZAI×17, XHG×2, CAPR×1, NPWR×4 | — | IREN, TNDM, TPG, INO, HIMS, SLS, VOR, BTSG | $10,352.86 | $52.95 | $10,405.80 | ZENA×6, AIRO×1, BZAI×17, XHG×2, CAPR×1, NPWR×4 | SELL IREN (dropped from list after 3 sess (min 3)); SELL TNDM (dropped from list after 3 sess (min 3)); SELL TPG (dropped from list after 3 sess (min 3)); SELL INO (dropped from list after 3 sess (min 3)); SELL HIMS (dropped from list after 3 sess (min 3)); SELL SLS (dropped from list after 3 sess (min 3)); SELL VOR (dropped from list after 3 sess (min 3)); SELL BTSG (dropped from list after 3 sess (min 3)); hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | -7.20 | $10,352.86 | ZENA×6, AIRO×1, BZAI×17, XHG×2, CAPR×1, NPWR×4 | — | ZENA, AIRO, BZAI | $10,383.27 | $23.32 | $10,406.59 | XHG×2, CAPR×1, NPWR×4 | SELL ZENA (dropped from list after 3 sess (min 3)); SELL AIRO (dropped from list after 3 sess (min 3)); SELL BZAI (dropped from list after 3 sess (min 3)); hard-red S=-7.20 sit; no new buys |
| 2026-08-20 | +1.12 | $10,383.27 | XHG×2, CAPR×1, NPWR×4 | MRNA, CYPH, ABCL, SENS, AUTL, TEM, WPM, IAG | XHG, CAPR, NPWR | $228.07 | $10,212.66 | $10,440.73 | MRNA×8, CYPH×1131, ABCL×110, SENS×145, AUTL×526, TEM×21, WPM×8, IAG×66 | SELL XHG (dropped from list after 3 sess (min 3)); SELL CAPR (dropped from list after 3 sess (min 3)); SELL NPWR (dropped from list after 3 sess (min 3)); BUY MRNA x8 @ 150.14; BUY CYPH x1131 @ 1.15; BUY ABCL x110 @ 11.81; BUY SENS x145 @ 8.91; BUY AUTL x526 @ 2.47; BUY TEM x21 @ 61.83; BUY WPM x8 @ 144.54; BUY IAG x66 @ 19.63 |
| 2026-08-21 | +3.25 | $228.07 | MRNA×8, CYPH×1131, ABCL×110, SENS×145, AUTL×526, TEM×21, WPM×8, IAG×66 | XHG, ARCT, IOVA, CAPR | — | $50.56 | $11,048.93 | $11,099.49 | MRNA×8, CYPH×1131, ABCL×110, SENS×145, AUTL×526, TEM×21, WPM×8, IAG×66, XHG×10, ARCT×4, IOVA×5, CAPR×6 | BUY XHG x10 @ 4.49; BUY ARCT x4 @ 11.13; BUY IOVA x5 @ 9.08; BUY CAPR x6 @ 6.81 |
| 2026-08-24 | -5.17 | $50.56 | MRNA×8, CYPH×1131, ABCL×110, SENS×145, AUTL×526, TEM×21, WPM×8, IAG×66, XHG×10, ARCT×4, IOVA×5, CAPR×6 | — | — | $50.56 | $11,050.83 | $11,101.39 | MRNA×8, CYPH×1131, ABCL×110, SENS×145, AUTL×526, TEM×21, WPM×8, IAG×66, XHG×10, ARCT×4, IOVA×5, CAPR×6 | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | +1.80 | $50.56 | MRNA×8, CYPH×1131, ABCL×110, SENS×145, AUTL×526, TEM×21, WPM×8, IAG×66, XHG×10, ARCT×4, IOVA×5, CAPR×6 | AU, ERO, ASST, HMY, FCX | MRNA, ABCL, SENS, AUTL, TEM, IAG | $24.44 | $10,998.02 | $11,022.46 | CYPH×1131, WPM×8, XHG×10, ARCT×4, IOVA×5, CAPR×6, AU×13, ERO×41, ASST×74, HMY×68, FCX×20 | SELL MRNA (dropped from list after 3 sess (min 3)); SELL ABCL (dropped from list after 3 sess (min 3)); SELL SENS (dropped from list after 3 sess (min 3)); SELL AUTL (dropped from list after 3 sess (min 3)); SELL TEM (dropped from list after 3 sess (min 3)); SELL IAG (dropped from list after 3 sess (min 3)); BUY AU x13 @ 119.46; BUY ERO x41 @ 38.00; BUY ASST x74 @ 20.90; BUY HMY x68 @ 22.65; BUY FCX x20 @ 77.90 |
| 2026-08-26 | +2.02 | $24.44 | CYPH×1131, WPM×8, XHG×10, ARCT×4, IOVA×5, CAPR×6, AU×13, ERO×41, ASST×74, HMY×68, FCX×20 | — | — | $24.44 | $11,136.78 | $11,161.22 | CYPH×1131, WPM×8, XHG×10, ARCT×4, IOVA×5, CAPR×6, AU×13, ERO×41, ASST×74, HMY×68, FCX×20 | hold CYPH,WPM,XHG,ARCT,IOVA,CAPR,AU,ERO,ASST,HMY,FCX |
| 2026-08-27 | — | $24.44 | CYPH×1131, WPM×8, XHG×10, ARCT×4, IOVA×5, CAPR×6, AU×13, ERO×41, ASST×74, HMY×68, FCX×20 | MOS, SLI, DLO, TX, MRVL, PLTR, MT | CYPH, WPM, XHG, ARCT, IOVA, CAPR | $723.50 | $10,397.48 | $11,120.98 | AU×13, ERO×41, ASST×74, HMY×68, FCX×20, MOS×16, SLI×158, DLO×26, TX×7, MRVL×1, PLTR×2, MT×5 | SELL CYPH (dropped from list after 5 sess (min 3)); SELL WPM (dropped from list after 5 sess (min 3)); SELL XHG (dropped from list after 4 sess (min 3)); SELL ARCT (dropped from list after 4 sess (min 3)); SELL IOVA (dropped from list after 4 sess (min 3)); SELL CAPR (dropped from list after 4 sess (min 3)); BUY MOS x16 @ 24.84; BUY SLI x158 @ 2.59; BUY DLO x26 @ 15.60; BUY TX x7 @ 55.20; BUY MRVL x1 @ 240.00; BUY PLTR x2 @ 170.60; BUY MT x5 @ 75.12 |
| 2026-08-28 | +0.75 | $723.50 | AU×13, ERO×41, ASST×74, HMY×68, FCX×20, MOS×16, SLI×158, DLO×26, TX×7, MRVL×1, PLTR×2, MT×5 | BKKT, QMCO, NIQ, FIGR, TIGR, SMTC | AU, ASST, HMY | $170.97 | $10,814.17 | $10,985.14 | ERO×41, FCX×20, MOS×16, SLI×158, DLO×26, TX×7, MRVL×1, PLTR×2, MT×5, BKKT×104, QMCO×37, NIQ×47, FIGR×23, TIGR×161, SMTC×5 | SELL AU (dropped from list after 3 sess (min 3)); SELL ASST (dropped from list after 3 sess (min 3)); SELL HMY (dropped from list after 3 sess (min 3)); BUY BKKT x104 @ 8.50; BUY QMCO x37 @ 23.50; BUY NIQ x47 @ 18.79; BUY FIGR x23 @ 37.42; BUY TIGR x161 @ 5.49; BUY SMTC x5 @ 149.40 |
| 2026-08-31 | -5.85 | $170.97 | ERO×41, FCX×20, MOS×16, SLI×158, DLO×26, TX×7, MRVL×1, PLTR×2, MT×5, BKKT×104, QMCO×37, NIQ×47, FIGR×23, TIGR×161, SMTC×5 | — | ERO, FCX | $3,271.36 | $7,345.41 | $10,616.77 | MOS×16, SLI×158, DLO×26, TX×7, MRVL×1, PLTR×2, MT×5, BKKT×104, QMCO×37, NIQ×47, FIGR×23, TIGR×161, SMTC×5 | SELL ERO (dropped from list after 4 sess (min 3)); SELL FCX (dropped from list after 4 sess (min 3)); hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | -6.30 | $3,271.36 | MOS×16, SLI×158, DLO×26, TX×7, MRVL×1, PLTR×2, MT×5, BKKT×104, QMCO×37, NIQ×47, FIGR×23, TIGR×161, SMTC×5 | — | MOS, SLI, DLO, TX, MRVL, PLTR, MT | $5,791.01 | $4,826.85 | $10,617.86 | BKKT×104, QMCO×37, NIQ×47, FIGR×23, TIGR×161, SMTC×5 | SELL MOS (dropped from list after 3 sess (min 3)); SELL SLI (dropped from list after 3 sess (min 3)); SELL DLO (dropped from list after 3 sess (min 3)); SELL TX (dropped from list after 3 sess (min 3)); SELL MRVL (dropped from list after 3 sess (min 3)); SELL PLTR (dropped from list after 3 sess (min 3)); SELL MT (dropped from list after 3 sess (min 3)); hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | -3.83 | $5,791.01 | BKKT×104, QMCO×37, NIQ×47, FIGR×23, TIGR×161, SMTC×5 | — | BKKT, QMCO, NIQ, FIGR, TIGR, SMTC | $10,578.83 | $0.00 | $10,578.83 | — | SELL BKKT (dropped from list after 3 sess (min 3)); SELL QMCO (dropped from list after 3 sess (min 3)); SELL NIQ (dropped from list after 3 sess (min 3)); SELL FIGR (dropped from list after 3 sess (min 3)); SELL TIGR (dropped from list after 3 sess (min 3)); SELL SMTC (dropped from list after 3 sess (min 3)); hard-red S=-3.83 sit; no new buys |
| 2026-09-03 | -0.90 | $10,578.83 | — | MRNA, ARCT, XHG, CAN, NVAX, INO, RVTY, ZYME | — | $137.93 | $10,477.77 | $10,615.70 | MRNA×8, ARCT×80, XHG×370, CAN×4407, NVAX×128, INO×986, RVTY×10, ZYME×44 | BUY MRNA x8 @ 151.40; BUY ARCT x80 @ 16.46; BUY XHG x370 @ 3.57; BUY CAN x4407 @ 0.30; BUY NVAX x128 @ 10.27; BUY INO x986 @ 1.34; BUY RVTY x10 @ 125.94; BUY ZYME x44 @ 30.00 |
| 2026-09-04 | — | $137.93 | MRNA×8, ARCT×80, XHG×370, CAN×4407, NVAX×128, INO×986, RVTY×10, ZYME×44 | OABI, TRLV, ALEC, OMER | — | $41.76 | $10,801.91 | $10,843.67 | MRNA×8, ARCT×80, XHG×370, CAN×4407, NVAX×128, INO×986, RVTY×10, ZYME×44, OABI×5, TRLV×2, ALEC×10, OMER×1 | BUY OABI x5 @ 5.08; BUY TRLV x2 @ 11.89; BUY ALEC x10 @ 2.70; BUY OMER x1 @ 18.99 |

## Fills (what was bought / sold)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 27 | $45.98 | $2.07 | — | $8,756.47 | rank by w_hot_cond; rank w_hot_cond; list flatten; ⚪; ret5=+12.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 53 | $23.33 | $2.15 | — | $7,517.83 | rank by w_hot_cond; rank w_hot_cond; list flatten; ⚪; ret5=+19.7; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 24 | $50.62 | $2.06 | — | $6,300.81 | rank by w_hot_cond; rank w_hot_cond; list flatten; ⚪; ret5=+6.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 1543 | $0.81 | $17.13 | — | $5,033.85 | rank by w_hot_cond; rank w_hot_cond; list flatten; ⚪; ret5=+13.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 42 | $29.74 | $2.12 | — | $3,782.66 | rank by w_hot_cond; rank w_hot_cond; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 106 | $11.70 | $2.31 | — | $2,540.15 | rank by w_hot_cond; rank w_hot_cond; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `VOR` | 56 | $22.01 | $2.16 | — | $1,305.43 | rank by w_hot_cond; rank w_hot_cond; list flatten; ⚪; ret5=+0.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 20 | $59.80 | $2.05 | — | $107.38 | rank by w_hot_cond; rank w_hot_cond; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-14 09:30 ET | **BUY** | `ZENA` | 6 | $2.20 | $0.15 | — | $94.03 | rank by w_hot_cond; rank w_hot_cond; list yday_gainer; 🔵; ⚪; ret5=+54.3; leftover $13.42 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `AIRO` | 1 | $11.12 | $0.11 | — | $82.80 | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; leftover $13.42 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BZAI` | 17 | $0.77 | $0.18 | — | $69.59 | rank by w_hot_cond; rank w_hot_cond; list earn_react; 🔵; ⚪; ret5=+20.4; leftover $13.42 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `XHG` | 2 | $4.19 | $0.09 | — | $61.12 | rank by w_hot_cond; rank w_hot_cond; list yday_mover; ⚪; ret5=+291.8; leftover $8.70 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `CAPR` | 1 | $6.87 | $0.07 | — | $54.18 | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; ret5=+62.6; leftover $8.70 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `NPWR` | 4 | $1.92 | $0.09 | — | $46.41 | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; ⚪; ret5=+28.3; leftover $8.70 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `IREN` | 27 | $43.56 | $2.09 | $-69.50 | $1,220.44 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TNDM` | 53 | $22.16 | $2.17 | $-66.33 | $2,392.75 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TPG` | 24 | $51.77 | $2.08 | $+23.38 | $3,633.15 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `INO` | 1543 | $1.14 | $20.17 | $+471.89 | $5,372.00 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `HIMS` | 42 | $27.85 | $2.14 | $-83.63 | $6,539.56 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `SLS` | 106 | $12.66 | $2.34 | $+97.12 | $7,879.19 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `VOR` | 56 | $22.82 | $2.18 | $+41.02 | $9,154.93 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `BTSG` | 20 | $60.00 | $2.07 | $-0.12 | $10,352.86 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `ZENA` | 6 | $2.01 | $0.16 | $-1.45 | $10,364.76 | dropped from list after 3 sess (min 3) | join🔴 sector🟡 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-19 09:30 ET | **SELL** | `AIRO` | 1 | $9.10 | $0.11 | $-2.25 | $10,373.74 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `BZAI` | 17 | $0.57 | $0.17 | $-3.68 | $10,383.27 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `XHG` | 2 | $4.10 | $0.11 | $-0.38 | $10,391.36 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `CAPR` | 1 | $7.66 | $0.10 | $+0.62 | $10,398.92 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `NPWR` | 4 | $1.64 | $0.10 | $-1.31 | $10,405.38 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 8 | $150.14 | $2.01 | — | $9,202.25 | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ret5=+173.9; leftover $1300.67 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CYPH` | 1131 | $1.15 | $14.59 | — | $7,887.01 | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+62.6; leftover $1300.67 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ABCL` | 110 | $11.81 | $2.32 | — | $6,585.04 | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $1300.67 | join🟢 sector🟡 gen🟢 news🔴 digest🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `SENS` | 145 | $8.91 | $2.42 | — | $5,290.66 | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; 🔵; ⚪; ret5=+14.9; leftover $1300.67 | join🟡 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AUTL` | 526 | $2.47 | $6.79 | — | $3,984.66 | rank by w_hot_cond; rank w_hot_cond; list mover_buy; 🔵; ⚪; ret5=+19.8; leftover $1300.67 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `TEM` | 21 | $61.83 | $2.05 | — | $2,684.17 | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+12.2; leftover $1300.67 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $1,525.84 | rank by w_hot_cond; rank w_hot_cond; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1300.67 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 66 | $19.63 | $2.19 | — | $228.07 | rank by w_hot_cond; rank w_hot_cond; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1300.67 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `XHG` | 10 | $4.49 | $0.48 | — | $182.69 | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; 🔵; ret5=+12.7; leftover $45.61 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🔴 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 4 | $11.13 | $0.46 | — | $137.72 | rank by w_hot_cond; rank w_hot_cond; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $45.61 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `IOVA` | 5 | $9.08 | $0.47 | — | $91.85 | rank by w_hot_cond; rank w_hot_cond; list yday_gainer; 🔵; ⚪; ret5=+37.9; leftover $45.61 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CAPR` | 6 | $6.81 | $0.43 | — | $50.56 | rank by w_hot_cond; rank w_hot_cond; list yday_mover; 🔵; ret5=+62.5; leftover $45.61 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **SELL** | `MRNA` | 8 | $141.19 | $2.03 | $-75.65 | $1,178.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ABCL` | 110 | $10.77 | $2.35 | $-119.62 | $2,360.40 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `SENS` | 145 | $9.66 | $2.46 | $+103.86 | $3,758.64 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `AUTL` | 526 | $2.32 | $6.88 | $-92.57 | $4,972.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `TEM` | 21 | $66.45 | $2.07 | $+92.89 | $6,365.45 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `IAG` | 66 | $21.63 | $2.21 | $+127.60 | $7,790.82 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 13 | $119.46 | $2.03 | — | $6,235.81 | rank by w_hot_cond; rank w_hot_cond; list mover_buy; 🔵; ⚪; ret5=+25.9; leftover $1558.16 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ERO` | 41 | $38.00 | $2.11 | — | $4,675.70 | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot,mover_buy; ⚪; ret5=+16.6; leftover $1558.16 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `ASST` | 74 | $20.90 | $2.21 | — | $3,126.89 | rank by w_hot_cond; rank w_hot_cond; list yday_gainer; 🔵; ret5=+47.9; leftover $1558.16 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `HMY` | 68 | $22.65 | $2.19 | — | $1,584.49 | rank by w_hot_cond; rank w_hot_cond; list mover_buy; ⚪; ret5=+21.1; leftover $1558.16 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 20 | $77.90 | $2.05 | — | $24.44 | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot,mover_buy; ⚪; ret5=+15.3; leftover $1558.16 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `CYPH` | 1131 | $1.60 | $14.79 | $+479.57 | $1,819.25 | dropped from list after 5 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `WPM` | 8 | $160.93 | $2.03 | $+127.07 | $3,104.66 | dropped from list after 5 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `XHG` | 10 | $3.81 | $0.43 | $-7.71 | $3,142.32 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `ARCT` | 4 | $15.35 | $0.65 | $+15.78 | $3,203.08 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `IOVA` | 5 | $8.34 | $0.45 | $-4.62 | $3,244.33 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `CAPR` | 6 | $8.29 | $0.54 | $+7.92 | $3,293.53 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **BUY** | `MOS` | 16 | $24.84 | $2.04 | — | $2,894.05 | rank by w_hot_cond; rank w_hot_cond; list flatten; ret5=+13.0; leftover $411.69 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟢 |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 158 | $2.59 | $2.46 | — | $2,482.37 | rank by w_hot_cond; rank w_hot_cond; list flatten; ret5=+4.2; leftover $411.69 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `DLO` | 26 | $15.60 | $2.07 | — | $2,074.70 | rank by w_hot_cond; rank w_hot_cond; list mover_buy; 🔵; ret5=+7.1; leftover $411.69 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `TX` | 7 | $55.20 | $2.01 | — | $1,686.29 | rank by w_hot_cond; rank w_hot_cond; list mover_buy; ret5=+3.0; leftover $411.69 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MRVL` | 1 | $240.00 | $1.99 | — | $1,444.30 | rank by w_hot_cond; rank w_hot_cond; list mover_buy; 🔵; ret5=+6.8; leftover $411.69 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `PLTR` | 2 | $170.60 | $2.00 | — | $1,101.10 | rank by w_hot_cond; rank w_hot_cond; list mover_buy; 🔵; ret5=+3.4; leftover $411.69 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MT` | 5 | $75.12 | $2.00 | — | $723.50 | rank by w_hot_cond; rank w_hot_cond; list mover_buy; ret5=-2.2; leftover $411.69 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `AU` | 13 | $117.41 | $2.05 | $-30.73 | $2,247.78 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ASST` | 74 | $22.45 | $2.24 | $+110.25 | $3,906.84 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `HMY` | 68 | $20.70 | $2.22 | $-137.01 | $5,312.22 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `BKKT` | 104 | $8.50 | $2.30 | — | $4,425.92 | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; ret5=+12.3; leftover $885.37 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `QMCO` | 37 | $23.50 | $2.10 | — | $3,554.32 | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; 🔵; ret5=-14.8; leftover $885.37 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟡 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `NIQ` | 47 | $18.79 | $2.13 | — | $2,669.06 | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; ret5=+7.6; leftover $885.37 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `FIGR` | 23 | $37.42 | $2.06 | — | $1,806.34 | rank by w_hot_cond; rank w_hot_cond; list yday_mover; ret5=+24.4; leftover $885.37 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `TIGR` | 161 | $5.49 | $2.47 | — | $919.98 | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; ret5=+15.9; leftover $885.37 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 5 | $149.40 | $2.00 | — | $170.97 | rank by w_hot_cond; rank w_hot_cond; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=-11.6; leftover $885.37 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `ERO` | 41 | $38.60 | $2.14 | $+20.35 | $1,751.43 | dropped from list after 4 sess (min 3) | join🟢 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-31 09:30 ET | **SELL** | `FCX` | 20 | $76.10 | $2.07 | $-40.12 | $3,271.36 | dropped from list after 4 sess (min 3) | join🟢 sector🔴 gen🔴 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-01 09:30 ET | **SELL** | `MOS` | 16 | $24.00 | $2.06 | $-17.54 | $3,653.30 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `SLI` | 158 | $2.70 | $2.50 | $+12.42 | $4,077.40 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `DLO` | 26 | $14.88 | $2.09 | $-22.88 | $4,462.20 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `TX` | 7 | $54.82 | $2.03 | $-6.70 | $4,843.90 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `MRVL` | 1 | $210.57 | $2.01 | $-33.44 | $5,052.46 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `PLTR` | 2 | $185.52 | $2.02 | $+25.83 | $5,421.49 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `MT` | 5 | $74.31 | $2.02 | $-8.08 | $5,791.01 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `BKKT` | 104 | $7.42 | $2.33 | $-116.95 | $6,560.36 | dropped from list after 3 sess (min 3) | join🔴 sector🟢 gen🔴 news🟡 digest🟢 ab🔴 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-02 09:30 ET | **SELL** | `QMCO` | 37 | $23.85 | $2.12 | $+8.73 | $7,440.69 | dropped from list after 3 sess (min 3) | join🔴 sector🟢 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-09-02 09:30 ET | **SELL** | `NIQ` | 47 | $19.00 | $2.15 | $+5.59 | $8,331.54 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `FIGR` | 23 | $35.46 | $2.08 | $-49.22 | $9,145.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `TIGR` | 161 | $4.97 | $2.51 | $-88.70 | $9,942.70 | dropped from list after 3 sess (min 3) | join🟢 sector🔴 gen🔴 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-02 09:30 ET | **SELL** | `SMTC` | 5 | $127.63 | $2.02 | $-112.88 | $10,578.83 | dropped from list after 3 sess (min 3) | — |
| 2026-09-03 09:30 ET | **BUY** | `MRNA` | 8 | $151.40 | $2.01 | — | $9,365.61 | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ret5=+129.2; leftover $1322.35 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 80 | $16.46 | $2.23 | — | $8,046.58 | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+63.4; leftover $1322.35 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `XHG` | 370 | $3.57 | $4.77 | — | $6,720.91 | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; ret5=+16.1; leftover $1322.35 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CAN` | 4407 | $0.30 | $26.44 | — | $5,372.37 | rank by w_hot_cond; rank w_hot_cond; list yday_mover; 🔵; ret5=+54.3; leftover $1322.35 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `NVAX` | 128 | $10.27 | $2.37 | — | $4,055.43 | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+11.1; leftover $1322.35 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `INO` | 986 | $1.34 | $12.72 | — | $2,721.47 | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; 🔵; ⚪; ret5=+8.3; leftover $1322.35 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 10 | $125.94 | $2.02 | — | $1,460.05 | rank by w_hot_cond; rank w_hot_cond; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $1322.35 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ZYME` | 44 | $30.00 | $2.12 | — | $137.93 | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; ⚪; ret5=+14.1; leftover $1322.35 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟡 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 5 | $5.08 | $0.27 | — | $112.26 | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+28.1; leftover $27.59 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `TRLV` | 2 | $11.89 | $0.24 | — | $88.24 | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; 🔵; ⚪; ret5=+15.0; leftover $27.59 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 10 | $2.70 | $0.30 | — | $60.94 | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.4; leftover $27.59 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OMER` | 1 | $18.99 | $0.19 | — | $41.76 | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; 🔵; ret5=+12.1; leftover $27.59 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `IREN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `TNDM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `TPG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `INO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `HIMS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `SLS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `VOR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `BTSG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `QMCO` | cash | leftover split 13.42 < 1 share @ 24.68 |
| 2026-08-14 | `ARX` | cash | leftover split 13.42 < 1 share @ 19.57 |
| 2026-08-14 | `LIFE` | cash | leftover split 13.42 < 1 share @ 35.04 |
| 2026-08-14 | `LUNR` | cash | leftover split 13.42 < 1 share @ 19.17 |
| 2026-08-14 | `TBBB` | cash | leftover split 13.42 < 1 share @ 48.82 |
| 2026-08-17 | `IREN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `TNDM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `TPG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `INO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `HIMS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `SLS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `VOR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `BTSG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `ZENA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `AIRO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `BZAI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `STDN` | cash | leftover split 8.70 < 1 share @ 13.64 |
| 2026-08-17 | `HTFL` | cash | leftover split 8.70 < 1 share @ 41.23 |
| 2026-08-17 | `UMAC` | cash | leftover split 8.70 < 1 share @ 32.55 |
| 2026-08-17 | `ALOY` | cash | leftover split 8.70 < 1 share @ 14.66 |
| 2026-08-17 | `LPTH` | cash | leftover split 8.70 < 1 share @ 14.94 |
| 2026-08-18 | `ZENA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `AIRO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `BZAI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `XHG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `NPWR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `OABI` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FDMT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AVAH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OCUL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AXTI` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FIGR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `IOVA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `XHG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `CAPR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `NPWR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ARX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `IVVD` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `WEAV` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `KURA` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SENS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `REAX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `ABCL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `SENS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AUTL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `WPM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `IAG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AU` | cash | leftover split 45.61 < 1 share @ 119.43 |
| 2026-08-24 | `MRNA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `CYPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ABCL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `SENS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AUTL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `TEM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `WPM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `IAG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `XHG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `ARCT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `IOVA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `CAPR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `ERO` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ALM` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HMY` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `KGC` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `GFI` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SCCO` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `ARCT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `IOVA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `CAPR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `CYPH` | no_price | no 09:30 open — carry |
| 2026-08-26 | `ARCT` | no_price | no 09:30 open — carry |
| 2026-08-26 | `IOVA` | no_price | no 09:30 open — carry |
| 2026-08-26 | `CAPR` | no_price | no 09:30 open — carry |
| 2026-08-26 | `AU` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ASST` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `HMY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `USDE` | no_price | no 09:30 open |
| 2026-08-26 | `HOOD` | no_price | no 09:30 open |
| 2026-08-26 | `AEM` | no_price | no 09:30 open |
| 2026-08-26 | `SCCO` | no_price | no 09:30 open |
| 2026-08-27 | `AU` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ERO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ASST` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `HMY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `FCX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `MU` | cash | leftover split 411.69 < 1 share @ 925.74 |
| 2026-08-28 | `MOS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `SLI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `DLO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `TX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `MRVL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `PLTR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `MT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `MOS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `SLI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `DLO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `TX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `MRVL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `PLTR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `MT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `BKKT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `QMCO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `NIQ` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `FIGR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SMTC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `USDE` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CYPH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `XHG` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PURR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CAN` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CVI` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `DK` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `BKKT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `QMCO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `NIQ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `FIGR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `TIGR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SMTC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `CYPH` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NVAX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `DK` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `OMER` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `UEC` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `VFF` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `OBE` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `INO` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `USDE` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `XHG` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `TXG` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ZYME` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `DK` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `OMER` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `NVAX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ALVO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-04 | `MRNA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `ARCT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CAN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `NVAX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `RVTY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `ATRC` | cash | leftover split 27.59 < 1 share @ 52.88 |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `MRNA` | 8 | 2026-09-03 @ $151.40 | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ret5=+129.2; leftover $1322.35 |
| `ARCT` | 80 | 2026-09-03 @ $16.46 | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+63.4; leftover $1322.35 |
| `XHG` | 370 | 2026-09-03 @ $3.57 | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; ret5=+16.1; leftover $1322.35 |
| `CAN` | 4407 | 2026-09-03 @ $0.30 | rank by w_hot_cond; rank w_hot_cond; list yday_mover; 🔵; ret5=+54.3; leftover $1322.35 |
| `NVAX` | 128 | 2026-09-03 @ $10.27 | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+11.1; leftover $1322.35 |
| `INO` | 986 | 2026-09-03 @ $1.34 | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; 🔵; ⚪; ret5=+8.3; leftover $1322.35 |
| `RVTY` | 10 | 2026-09-03 @ $125.94 | rank by w_hot_cond; rank w_hot_cond; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $1322.35 |
| `ZYME` | 44 | 2026-09-03 @ $30.00 | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; ⚪; ret5=+14.1; leftover $1322.35 |
| `OABI` | 5 | 2026-09-04 @ $5.08 | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+28.1; leftover $27.59 |
| `TRLV` | 2 | 2026-09-04 @ $11.89 | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; 🔵; ⚪; ret5=+15.0; leftover $27.59 |
| `ALEC` | 10 | 2026-09-04 @ $2.70 | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.4; leftover $27.59 |
| `OMER` | 1 | 2026-09-04 @ $18.99 | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; 🔵; ret5=+12.1; leftover $27.59 |
