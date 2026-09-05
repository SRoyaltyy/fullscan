# Factor mine action — `union_hot_score_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `hot_score` · size `leftover` · sell `list` · S-boost `none` · rank by hot_score

Cash book **+6.61%** ($10,661) · signal-only (no cash/fees) was +17.08%. Starts YES **14/17**. Fills 105 · skips 150 · realized $+442.40.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `hot_score` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $15.71.

## Each session (cash + holdings state)

| Date | S | Open cash | Open held | Bought | Sold | Close cash | Stock | Equity | Close held | Why |
|---|---:|---:|---|---|---|---:|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | IREN, TNDM, TPG, INO, HIMS, SLS, VOR, BTSG | — | $107.38 | $10,161.33 | $10,268.71 | IREN×27, TNDM×53, TPG×24, INO×1543, HIMS×42, SLS×106, VOR×56, BTSG×20 | BUY IREN x27 @ 45.98; BUY TNDM x53 @ 23.33; BUY TPG x24 @ 50.62; BUY INO x1543 @ 0.81; BUY HIMS x42 @ 29.74; BUY SLS x106 @ 11.70; BUY VOR x56 @ 22.01; BUY BTSG x20 @ 59.80 |
| 2026-08-14 | +5.50 | $107.38 | IREN×27, TNDM×53, TPG×24, INO×1543, HIMS×42, SLS×106, VOR×56, BTSG×20 | ZENA, AIRO, BZAI | — | $69.59 | $10,441.72 | $10,511.32 | IREN×27, TNDM×53, TPG×24, INO×1543, HIMS×42, SLS×106, VOR×56, BTSG×20, ZENA×6, AIRO×1, BZAI×17 | BUY ZENA x6 @ 2.20; BUY AIRO x1 @ 11.12; BUY BZAI x17 @ 0.77 |
| 2026-08-17 | +2.25 | $69.59 | IREN×27, TNDM×53, TPG×24, INO×1543, HIMS×42, SLS×106, VOR×56, BTSG×20, ZENA×6, AIRO×1, BZAI×17 | XHG, CAPR, NPWR | — | $46.41 | $10,537.28 | $10,583.70 | IREN×27, TNDM×53, TPG×24, INO×1543, HIMS×42, SLS×106, VOR×56, BTSG×20, ZENA×6, AIRO×1, BZAI×17, XHG×2, CAPR×1, NPWR×4 | BUY XHG x2 @ 4.19; BUY CAPR x1 @ 6.87; BUY NPWR x4 @ 1.92 |
| 2026-08-18 | -6.20 | $46.41 | IREN×27, TNDM×53, TPG×24, INO×1543, HIMS×42, SLS×106, VOR×56, BTSG×20, ZENA×6, AIRO×1, BZAI×17, XHG×2, CAPR×1, NPWR×4 | — | IREN, TNDM, TPG, INO, HIMS, SLS, VOR, BTSG | $10,352.86 | $52.95 | $10,405.80 | ZENA×6, AIRO×1, BZAI×17, XHG×2, CAPR×1, NPWR×4 | SELL IREN (dropped from list after 3 sess (min 3)); SELL TNDM (dropped from list after 3 sess (min 3)); SELL TPG (dropped from list after 3 sess (min 3)); SELL INO (dropped from list after 3 sess (min 3)); SELL HIMS (dropped from list after 3 sess (min 3)); SELL SLS (dropped from list after 3 sess (min 3)); SELL VOR (dropped from list after 3 sess (min 3)); SELL BTSG (dropped from list after 3 sess (min 3)); hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | -7.20 | $10,352.86 | ZENA×6, AIRO×1, BZAI×17, XHG×2, CAPR×1, NPWR×4 | — | ZENA, AIRO, BZAI | $10,383.27 | $23.32 | $10,406.59 | XHG×2, CAPR×1, NPWR×4 | SELL ZENA (dropped from list after 3 sess (min 3)); SELL AIRO (dropped from list after 3 sess (min 3)); SELL BZAI (dropped from list after 3 sess (min 3)); hard-red S=-7.20 sit; no new buys |
| 2026-08-20 | +1.12 | $10,383.27 | XHG×2, CAPR×1, NPWR×4 | MRNA, CYPH, ABCL, AZI, SENS, ALEC, BTGO, AUTL | XHG, CAPR, NPWR | $69.75 | $10,140.83 | $10,210.58 | MRNA×8, CYPH×1131, ABCL×110, AZI×949, SENS×145, ALEC×541, BTGO×196, AUTL×526 | SELL XHG (dropped from list after 3 sess (min 3)); SELL CAPR (dropped from list after 3 sess (min 3)); SELL NPWR (dropped from list after 3 sess (min 3)); BUY MRNA x8 @ 150.14; BUY CYPH x1131 @ 1.15; BUY ABCL x110 @ 11.81; BUY AZI x949 @ 1.37; BUY SENS x145 @ 8.91; BUY ALEC x541 @ 2.40; BUY BTGO x196 @ 6.61; BUY AUTL x526 @ 2.47 |
| 2026-08-21 | +3.25 | $69.75 | MRNA×8, CYPH×1131, ABCL×110, AZI×949, SENS×145, ALEC×541, BTGO×196, AUTL×526 | XHG, CAPR, ARCT, IOVA, CAN | — | $21.68 | $10,732.02 | $10,753.69 | MRNA×8, CYPH×1131, ABCL×110, AZI×949, SENS×145, ALEC×541, BTGO×196, AUTL×526, XHG×2, CAPR×1, ARCT×1, IOVA×1, CAN×39 | BUY XHG x2 @ 4.49; BUY CAPR x1 @ 6.81; BUY ARCT x1 @ 11.13; BUY IOVA x1 @ 9.08; BUY CAN x39 @ 0.29 |
| 2026-08-24 | -5.17 | $21.68 | MRNA×8, CYPH×1131, ABCL×110, AZI×949, SENS×145, ALEC×541, BTGO×196, AUTL×526, XHG×2, CAPR×1, ARCT×1, IOVA×1, CAN×39 | — | — | $21.68 | $10,822.81 | $10,844.49 | MRNA×8, CYPH×1131, ABCL×110, AZI×949, SENS×145, ALEC×541, BTGO×196, AUTL×526, XHG×2, CAPR×1, ARCT×1, IOVA×1, CAN×39 | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | +1.80 | $21.68 | MRNA×8, CYPH×1131, ABCL×110, AZI×949, SENS×145, ALEC×541, BTGO×196, AUTL×526, XHG×2, CAPR×1, ARCT×1, IOVA×1, CAN×39 | ASST, AU, RUM, BMNR, NIQ, DEFT | MRNA, ABCL, AZI, SENS, ALEC, BTGO, AUTL | $40.76 | $10,466.59 | $10,507.35 | CYPH×1131, XHG×2, CAPR×1, ARCT×1, IOVA×1, CAN×39, ASST×69, AU×12, RUM×156, BMNR×59, NIQ×74, DEFT×2285 | SELL MRNA (dropped from list after 3 sess (min 3)); SELL ABCL (dropped from list after 3 sess (min 3)); SELL AZI (dropped from list after 3 sess (min 3)); SELL SENS (dropped from list after 3 sess (min 3)); SELL ALEC (dropped from list after 3 sess (min 3)); SELL BTGO (dropped from list after 3 sess (min 3)); SELL AUTL (dropped from list after 3 sess (min 3)); BUY ASST x69 @ 20.90; BUY AU x12 @ 119.46; BUY RUM x156 @ 9.36; BUY BMNR x59 @ 24.73; BUY NIQ x74 @ 19.56; BUY DEFT x2285 @ 0.64 |
| 2026-08-26 | +2.02 | $40.76 | CYPH×1131, XHG×2, CAPR×1, ARCT×1, IOVA×1, CAN×39, ASST×69, AU×12, RUM×156, BMNR×59, NIQ×74, DEFT×2285 | — | — | $40.76 | $10,679.38 | $10,720.14 | CYPH×1131, XHG×2, CAPR×1, ARCT×1, IOVA×1, CAN×39, ASST×69, AU×12, RUM×156, BMNR×59, NIQ×74, DEFT×2285 | hold CYPH,XHG,CAPR,ARCT,IOVA,CAN,ASST,AU,RUM,BMNR,NIQ,DEFT |
| 2026-08-27 | — | $40.76 | CYPH×1131, XHG×2, CAPR×1, ARCT×1, IOVA×1, CAN×39, ASST×69, AU×12, RUM×156, BMNR×59, NIQ×74, DEFT×2285 | MOS, DLO, SLI, CRK, PLTR, RRC, GEN | CYPH, XHG, CAPR, ARCT, IOVA, CAN | $351.89 | $10,108.42 | $10,460.31 | ASST×69, AU×12, RUM×156, BMNR×59, NIQ×74, DEFT×2285, MOS×9, DLO×15, SLI×91, CRK×16, PLTR×1, RRC×5, GEN×8 | SELL CYPH (dropped from list after 5 sess (min 3)); SELL XHG (dropped from list after 4 sess (min 3)); SELL CAPR (dropped from list after 4 sess (min 3)); SELL ARCT (dropped from list after 4 sess (min 3)); SELL IOVA (dropped from list after 4 sess (min 3)); SELL CAN (dropped from list after 4 sess (min 3)); BUY MOS x9 @ 24.84; BUY DLO x15 @ 15.60; BUY SLI x91 @ 2.59; BUY CRK x16 @ 14.09; BUY PLTR x1 @ 170.60; BUY RRC x5 @ 40.72; BUY GEN x8 @ 28.89 |
| 2026-08-28 | +0.75 | $351.89 | ASST×69, AU×12, RUM×156, BMNR×59, NIQ×74, DEFT×2285, MOS×9, DLO×15, SLI×91, CRK×16, PLTR×1, RRC×5, GEN×8 | FIGR, ERO, TRLV, CVI, VIRT, TXG, GUTS | ASST, AU, RUM, BMNR, DEFT | $87.23 | $10,544.08 | $10,631.31 | NIQ×74, MOS×9, DLO×15, SLI×91, CRK×16, PLTR×1, RRC×5, GEN×8, FIGR×29, ERO×27, TRLV×96, CVI×27, VIRT×16, TXG×17, GUTS×1479 | SELL ASST (dropped from list after 3 sess (min 3)); SELL AU (dropped from list after 3 sess (min 3)); SELL RUM (dropped from list after 3 sess (min 3)); SELL BMNR (dropped from list after 3 sess (min 3)); SELL DEFT (dropped from list after 3 sess (min 3)); BUY FIGR x29 @ 37.42; BUY ERO x27 @ 39.20; BUY TRLV x96 @ 11.38; BUY CVI x27 @ 40.04; BUY VIRT x16 @ 65.42; BUY TXG x17 @ 64.10; BUY GUTS x1479 @ 0.74 |
| 2026-08-31 | -5.85 | $87.23 | NIQ×74, MOS×9, DLO×15, SLI×91, CRK×16, PLTR×1, RRC×5, GEN×8, FIGR×29, ERO×27, TRLV×96, CVI×27, VIRT×16, TXG×17, GUTS×1479 | — | — | $87.23 | $10,472.22 | $10,559.45 | NIQ×74, MOS×9, DLO×15, SLI×91, CRK×16, PLTR×1, RRC×5, GEN×8, FIGR×29, ERO×27, TRLV×96, CVI×27, VIRT×16, TXG×17, GUTS×1479 | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | -6.30 | $87.23 | NIQ×74, MOS×9, DLO×15, SLI×91, CRK×16, PLTR×1, RRC×5, GEN×8, FIGR×29, ERO×27, TRLV×96, CVI×27, VIRT×16, TXG×17, GUTS×1479 | — | NIQ, MOS, DLO, SLI, CRK, PLTR, RRC, GEN | $3,031.52 | $7,476.20 | $10,507.72 | FIGR×29, ERO×27, TRLV×96, CVI×27, VIRT×16, TXG×17, GUTS×1479 | SELL NIQ (dropped from list after 5 sess (min 3)); SELL MOS (dropped from list after 3 sess (min 3)); SELL DLO (dropped from list after 3 sess (min 3)); SELL SLI (dropped from list after 3 sess (min 3)); SELL CRK (dropped from list after 3 sess (min 3)); SELL PLTR (dropped from list after 3 sess (min 3)); SELL RRC (dropped from list after 3 sess (min 3)); SELL GEN (dropped from list after 3 sess (min 3)); hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | -3.83 | $3,031.52 | FIGR×29, ERO×27, TRLV×96, CVI×27, VIRT×16, TXG×17, GUTS×1479 | — | FIGR, ERO, CVI, VIRT | $7,227.63 | $3,196.79 | $10,424.42 | TRLV×96, TXG×17, GUTS×1479 | SELL FIGR (dropped from list after 3 sess (min 3)); SELL ERO (dropped from list after 3 sess (min 3)); SELL CVI (dropped from list after 3 sess (min 3)); SELL VIRT (dropped from list after 3 sess (min 3)); hard-red S=-3.83 sit; no new buys |
| 2026-09-03 | -0.90 | $7,227.63 | TRLV×96, TXG×17, GUTS×1479 | MRNA, XHG, ARCT, CAN, NIQ, DEFT, OMER, ERO | TRLV, TXG, GUTS | $82.63 | $10,174.09 | $10,256.72 | MRNA×8, XHG×365, ARCT×79, CAN×4351, NIQ×70, DEFT×1948, OMER×68, ERO×36 | SELL TRLV (dropped from list after 4 sess (min 3)); SELL TXG (dropped from list after 4 sess (min 3)); SELL GUTS (dropped from list after 4 sess (min 3)); BUY MRNA x8 @ 151.40; BUY XHG x365 @ 3.57; BUY ARCT x79 @ 16.46; BUY CAN x4351 @ 0.30; BUY NIQ x70 @ 18.60; BUY DEFT x1948 @ 0.67; BUY OMER x68 @ 18.97; BUY ERO x36 @ 35.62 |
| 2026-09-04 | — | $82.63 | MRNA×8, XHG×365, ARCT×79, CAN×4351, NIQ×70, DEFT×1948, OMER×68, ERO×36 | HQ, OABI, TRLV | — | $15.71 | $10,645.60 | $10,661.31 | MRNA×8, XHG×365, ARCT×79, CAN×4351, NIQ×70, DEFT×1948, OMER×68, ERO×36, HQ×1, OABI×5, TRLV×2 | BUY HQ x1 @ 17.06; BUY OABI x5 @ 5.08; BUY TRLV x2 @ 11.89 |

## Fills (what was bought / sold)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 27 | $45.98 | $2.07 | — | $8,756.47 | rank by hot_score; rank hot_score; list flatten; ⚪; ret5=+12.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 53 | $23.33 | $2.15 | — | $7,517.83 | rank by hot_score; rank hot_score; list flatten; ⚪; ret5=+19.7; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 24 | $50.62 | $2.06 | — | $6,300.81 | rank by hot_score; rank hot_score; list flatten; ⚪; ret5=+6.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 1543 | $0.81 | $17.13 | — | $5,033.85 | rank by hot_score; rank hot_score; list flatten; ⚪; ret5=+13.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 42 | $29.74 | $2.12 | — | $3,782.66 | rank by hot_score; rank hot_score; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 106 | $11.70 | $2.31 | — | $2,540.15 | rank by hot_score; rank hot_score; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `VOR` | 56 | $22.01 | $2.16 | — | $1,305.43 | rank by hot_score; rank hot_score; list flatten; ⚪; ret5=+0.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 20 | $59.80 | $2.05 | — | $107.38 | rank by hot_score; rank hot_score; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-14 09:30 ET | **BUY** | `ZENA` | 6 | $2.20 | $0.15 | — | $94.03 | rank by hot_score; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+54.3; leftover $13.42 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `AIRO` | 1 | $11.12 | $0.11 | — | $82.80 | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; leftover $13.42 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BZAI` | 17 | $0.77 | $0.18 | — | $69.59 | rank by hot_score; rank hot_score; list earn_react; 🔵; ⚪; ret5=+20.4; leftover $13.42 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `XHG` | 2 | $4.19 | $0.09 | — | $61.12 | rank by hot_score; rank hot_score; list yday_mover; ⚪; ret5=+291.8; leftover $8.70 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `CAPR` | 1 | $6.87 | $0.07 | — | $54.18 | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; ret5=+62.6; leftover $8.70 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `NPWR` | 4 | $1.92 | $0.09 | — | $46.41 | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; ⚪; ret5=+28.3; leftover $8.70 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
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
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 8 | $150.14 | $2.01 | — | $9,202.25 | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+173.9; leftover $1300.67 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CYPH` | 1131 | $1.15 | $14.59 | — | $7,887.01 | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+62.6; leftover $1300.67 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ABCL` | 110 | $11.81 | $2.32 | — | $6,585.04 | rank by hot_score; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $1300.67 | join🟢 sector🟡 gen🟢 news🔴 digest🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AZI` | 949 | $1.37 | $12.24 | — | $5,272.67 | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+30.7; leftover $1300.67 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `SENS` | 145 | $8.91 | $2.42 | — | $3,978.29 | rank by hot_score; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+14.9; leftover $1300.67 | join🟡 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ALEC` | 541 | $2.40 | $6.98 | — | $2,672.91 | rank by hot_score; rank hot_score; list ohlc_hot; 🔵; ret5=+13.0; leftover $1300.67 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BTGO` | 196 | $6.61 | $2.58 | — | $1,375.75 | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+25.3; leftover $1300.67 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟡 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AUTL` | 526 | $2.47 | $6.79 | — | $69.75 | rank by hot_score; rank hot_score; list mover_buy; 🔵; ⚪; ret5=+19.8; leftover $1300.67 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `XHG` | 2 | $4.49 | $0.10 | — | $60.67 | rank by hot_score; rank hot_score; list ohlc_hot; 🔵; ret5=+12.7; leftover $11.62 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🔴 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CAPR` | 1 | $6.81 | $0.07 | — | $53.79 | rank by hot_score; rank hot_score; list yday_mover; 🔵; ret5=+62.5; leftover $11.62 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 1 | $11.13 | $0.11 | — | $42.55 | rank by hot_score; rank hot_score; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $11.62 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `IOVA` | 1 | $9.08 | $0.09 | — | $33.37 | rank by hot_score; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+37.9; leftover $11.62 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CAN` | 39 | $0.29 | $0.23 | — | $21.68 | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+30.4; leftover $11.62 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **SELL** | `MRNA` | 8 | $141.19 | $2.03 | $-75.65 | $1,149.16 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ABCL` | 110 | $10.77 | $2.35 | $-119.62 | $2,331.51 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `AZI` | 949 | $1.33 | $12.41 | $-62.61 | $3,581.27 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `SENS` | 145 | $9.66 | $2.46 | $+103.86 | $4,979.51 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ALEC` | 541 | $2.30 | $7.08 | $-68.16 | $6,216.73 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BTGO` | 196 | $6.89 | $2.62 | $+50.66 | $7,564.55 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `AUTL` | 526 | $2.32 | $6.88 | $-92.57 | $8,777.99 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `ASST` | 69 | $20.90 | $2.20 | — | $7,333.69 | rank by hot_score; rank hot_score; list yday_gainer; 🔵; ret5=+47.9; leftover $1463.00 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 12 | $119.46 | $2.03 | — | $5,898.15 | rank by hot_score; rank hot_score; list mover_buy; 🔵; ⚪; ret5=+25.9; leftover $1463.00 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `RUM` | 156 | $9.36 | $2.46 | — | $4,435.53 | rank by hot_score; rank hot_score; list yday_gainer; 🔵; ret5=+21.3; leftover $1463.00 | join🔴 sector🟡 gen🟡 news🟢 digest🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BMNR` | 59 | $24.73 | $2.17 | — | $2,974.29 | rank by hot_score; rank hot_score; list yday_gainer; ret5=+26.3; leftover $1463.00 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟡 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `NIQ` | 74 | $19.56 | $2.21 | — | $1,524.64 | rank by hot_score; rank hot_score; list ohlc_hot; 🔵; ret5=+7.6; leftover $1463.00 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `DEFT` | 2285 | $0.64 | $21.48 | — | $40.76 | rank by hot_score; rank hot_score; list yday_gainer,yday_mover,ohlc_hot; 🔵; ⚪; ret5=+17.6; leftover $1463.00 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `CYPH` | 1131 | $1.60 | $14.79 | $+479.57 | $1,835.57 | dropped from list after 5 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `XHG` | 2 | $3.81 | $0.10 | $-1.56 | $1,843.09 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `CAPR` | 1 | $8.29 | $0.11 | $+1.30 | $1,851.27 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `ARCT` | 1 | $15.35 | $0.18 | $+3.93 | $1,866.44 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `IOVA` | 1 | $8.34 | $0.11 | $-0.94 | $1,874.68 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `CAN` | 39 | $0.40 | $0.29 | $+3.61 | $1,889.99 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **BUY** | `MOS` | 9 | $24.84 | $2.02 | — | $1,664.41 | rank by hot_score; rank hot_score; list flatten; ret5=+13.0; leftover $236.25 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟢 |
| 2026-08-27 09:30 ET | **BUY** | `DLO` | 15 | $15.60 | $2.04 | — | $1,428.37 | rank by hot_score; rank hot_score; list mover_buy; 🔵; ret5=+7.1; leftover $236.25 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 91 | $2.59 | $2.26 | — | $1,190.42 | rank by hot_score; rank hot_score; list flatten; ret5=+4.2; leftover $236.25 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 16 | $14.09 | $2.04 | — | $962.94 | rank by hot_score; rank hot_score; list flatten; ret5=+1.1; leftover $236.25 | join🟢 sector🔴 gen🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `PLTR` | 1 | $170.60 | $1.71 | — | $790.63 | rank by hot_score; rank hot_score; list mover_buy; 🔵; ret5=+3.4; leftover $236.25 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 5 | $40.72 | $2.00 | — | $585.03 | rank by hot_score; rank hot_score; list flatten; ret5=+1.8; leftover $236.25 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `GEN` | 8 | $28.89 | $2.01 | — | $351.89 | rank by hot_score; rank hot_score; list mover_buy; 🔵; ret5=+1.6; leftover $236.25 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `ASST` | 69 | $22.45 | $2.22 | $+102.53 | $1,898.72 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `AU` | 12 | $117.41 | $2.05 | $-28.67 | $3,305.60 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `RUM` | 156 | $9.51 | $2.50 | $+18.45 | $4,786.66 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `BMNR` | 59 | $25.91 | $2.19 | $+65.26 | $6,313.16 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `DEFT` | 2285 | $0.60 | $20.96 | $-133.83 | $7,663.21 | dropped from list after 3 sess (min 3) | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `FIGR` | 29 | $37.42 | $2.08 | — | $6,575.95 | rank by hot_score; rank hot_score; list yday_mover; ret5=+24.4; leftover $1094.74 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `ERO` | 27 | $39.20 | $2.07 | — | $5,515.48 | rank by hot_score; rank hot_score; list ohlc_hot; ret5=+16.6; leftover $1094.74 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `TRLV` | 96 | $11.38 | $2.28 | — | $4,420.72 | rank by hot_score; rank hot_score; list ohlc_hot; ret5=+15.0; leftover $1094.74 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 heat🟡 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CVI` | 27 | $40.04 | $2.07 | — | $3,337.57 | rank by hot_score; rank hot_score; list ohlc_hot; ret5=+12.1; leftover $1094.74 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `VIRT` | 16 | $65.42 | $2.04 | — | $2,288.81 | rank by hot_score; rank hot_score; list ohlc_hot; ret5=+13.2; leftover $1094.74 | join🟢 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `TXG` | 17 | $64.10 | $2.04 | — | $1,197.07 | rank by hot_score; rank hot_score; list ohlc_hot; ret5=+16.1; leftover $1094.74 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `GUTS` | 1479 | $0.74 | $15.38 | — | $87.23 | rank by hot_score; rank hot_score; list ohlc_hot; ret5=+14.7; leftover $1094.74 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-01 09:30 ET | **SELL** | `NIQ` | 74 | $19.06 | $2.24 | $-41.45 | $1,495.43 | dropped from list after 5 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `MOS` | 9 | $24.00 | $2.04 | $-11.61 | $1,709.40 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `DLO` | 15 | $14.88 | $2.06 | $-14.89 | $1,930.54 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `SLI` | 91 | $2.70 | $2.29 | $+5.46 | $2,173.95 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `CRK` | 16 | $14.31 | $2.06 | $-0.58 | $2,400.85 | dropped from list after 3 sess (min 3) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-01 09:30 ET | **SELL** | `PLTR` | 1 | $185.52 | $1.88 | $+11.33 | $2,584.50 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `RRC` | 5 | $41.32 | $2.02 | $-1.03 | $2,789.07 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `GEN` | 8 | $30.56 | $2.03 | $+9.31 | $3,031.52 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `FIGR` | 29 | $35.46 | $2.10 | $-61.01 | $4,057.76 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `ERO` | 27 | $35.95 | $2.09 | $-91.91 | $5,026.32 | dropped from list after 3 sess (min 3) | join🟢 sector🔴 gen🔴 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-02 09:30 ET | **SELL** | `CVI` | 27 | $42.94 | $2.09 | $+74.14 | $6,183.61 | dropped from list after 3 sess (min 3) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-02 09:30 ET | **SELL** | `VIRT` | 16 | $65.38 | $2.06 | $-4.74 | $7,227.63 | dropped from list after 3 sess (min 3) | — |
| 2026-09-03 09:30 ET | **SELL** | `TRLV` | 96 | $11.78 | $2.30 | $+33.82 | $8,356.21 | dropped from list after 4 sess (min 3) | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **SELL** | `TXG` | 17 | $60.24 | $2.06 | $-69.72 | $9,378.23 | dropped from list after 4 sess (min 3) | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-09-03 09:30 ET | **SELL** | `GUTS` | 1479 | $0.73 | $15.49 | $-45.66 | $10,442.41 | dropped from list after 4 sess (min 3) | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `MRNA` | 8 | $151.40 | $2.01 | — | $9,229.19 | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+129.2; leftover $1305.30 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `XHG` | 365 | $3.57 | $4.71 | — | $7,921.43 | rank by hot_score; rank hot_score; list ohlc_hot; ret5=+16.1; leftover $1305.30 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 79 | $16.46 | $2.23 | — | $6,618.87 | rank by hot_score; rank hot_score; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+63.4; leftover $1305.30 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CAN` | 4351 | $0.30 | $26.11 | — | $5,287.46 | rank by hot_score; rank hot_score; list yday_mover; 🔵; ret5=+54.3; leftover $1305.30 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `NIQ` | 70 | $18.60 | $2.20 | — | $3,983.26 | rank by hot_score; rank hot_score; list ohlc_hot; 🔵; ret5=+7.6; leftover $1305.30 | join🟢 sector🟢 gen🟡 news🔴 digest🟢 judge🟡 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DEFT` | 1948 | $0.67 | $18.90 | — | $2,659.20 | rank by hot_score; rank hot_score; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+17.6; leftover $1305.30 | join🟡 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `OMER` | 68 | $18.97 | $2.19 | — | $1,367.05 | rank by hot_score; rank hot_score; list ohlc_hot; ret5=+12.1; leftover $1305.30 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ERO` | 36 | $35.62 | $2.10 | — | $82.63 | rank by hot_score; rank hot_score; list ohlc_hot; 🔵; ret5=+16.6; leftover $1305.30 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🔴 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `HQ` | 1 | $17.06 | $0.17 | — | $65.40 | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; ret5=+17.3; leftover $27.54 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 ab🟡 heat🔴 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 5 | $5.08 | $0.27 | — | $39.73 | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+28.1; leftover $27.54 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `TRLV` | 2 | $11.89 | $0.24 | — | $15.71 | rank by hot_score; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+15.0; leftover $27.54 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |

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
| 2026-08-14 | `VOYG` | cash | leftover split 13.42 < 1 share @ 44.49 |
| 2026-08-14 | `LUNR` | cash | leftover split 13.42 < 1 share @ 19.17 |
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
| 2026-08-17 | `SMJF` | cash | leftover split 8.70 < 1 share @ 10.10 |
| 2026-08-17 | `ALOY` | cash | leftover split 8.70 < 1 share @ 14.66 |
| 2026-08-18 | `ZENA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `AIRO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `BZAI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `XHG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `NPWR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `OABI` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FDMT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AVAH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AXTI` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FIGR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `IOVA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ALM` | hard_red | hard-red S=-6.20 sit; no new buys |
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
| 2026-08-21 | `AZI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `SENS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `ALEC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `BTGO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AUTL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `TEM` | cash | leftover split 11.62 < 1 share @ 65.60 |
| 2026-08-24 | `MRNA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `CYPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ABCL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AZI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `SENS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ALEC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BTGO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AUTL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `XHG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `CAPR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `ARCT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `IOVA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `CAN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `ALM` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DEFT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `OMER` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HMY` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ERO` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `TRLV` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `TMC` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `KGC` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `CAPR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `ARCT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `IOVA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `CAN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `CYPH` | no_price | no 09:30 open — carry |
| 2026-08-26 | `CAPR` | no_price | no 09:30 open — carry |
| 2026-08-26 | `ARCT` | no_price | no 09:30 open — carry |
| 2026-08-26 | `IOVA` | no_price | no 09:30 open — carry |
| 2026-08-26 | `CAN` | no_price | no 09:30 open — carry |
| 2026-08-26 | `ASST` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `AU` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `RUM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `BMNR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `USDE` | no_price | no 09:30 open |
| 2026-08-26 | `SENS` | no_price | no 09:30 open |
| 2026-08-26 | `OMER` | no_price | no 09:30 open |
| 2026-08-26 | `ERO` | no_price | no 09:30 open |
| 2026-08-26 | `TRLV` | no_price | no 09:30 open |
| 2026-08-27 | `ASST` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `AU` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `RUM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `BMNR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `NIQ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `DEFT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `MRVL` | cash | leftover split 236.25 < 1 share @ 240.00 |
| 2026-08-28 | `MOS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `DLO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `SLI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `CRK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `PLTR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `RRC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `GEN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `MOS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `DLO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `SLI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `CRK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `PLTR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `RRC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `GEN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `FIGR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `ERO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `TRLV` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `CVI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `VIRT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `TXG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `GUTS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `USDE` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CYPH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `XHG` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CAN` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PURR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `MSTR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `DEFT` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `FIGR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `ERO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `CVI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `VIRT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `CYPH` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `OMER` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `DK` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `INO` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `UEC` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `USDE` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `XHG` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `OMER` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `WPM` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ZYME` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-04 | `MRNA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `ARCT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CAN` | min_hold | dropped but min-hold 1/3 sess — no sell |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `MRNA` | 8 | 2026-09-03 @ $151.40 | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+129.2; leftover $1305.30 |
| `XHG` | 365 | 2026-09-03 @ $3.57 | rank by hot_score; rank hot_score; list ohlc_hot; ret5=+16.1; leftover $1305.30 |
| `ARCT` | 79 | 2026-09-03 @ $16.46 | rank by hot_score; rank hot_score; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+63.4; leftover $1305.30 |
| `CAN` | 4351 | 2026-09-03 @ $0.30 | rank by hot_score; rank hot_score; list yday_mover; 🔵; ret5=+54.3; leftover $1305.30 |
| `NIQ` | 70 | 2026-09-03 @ $18.60 | rank by hot_score; rank hot_score; list ohlc_hot; 🔵; ret5=+7.6; leftover $1305.30 |
| `DEFT` | 1948 | 2026-09-03 @ $0.67 | rank by hot_score; rank hot_score; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+17.6; leftover $1305.30 |
| `OMER` | 68 | 2026-09-03 @ $18.97 | rank by hot_score; rank hot_score; list ohlc_hot; ret5=+12.1; leftover $1305.30 |
| `ERO` | 36 | 2026-09-03 @ $35.62 | rank by hot_score; rank hot_score; list ohlc_hot; 🔵; ret5=+16.6; leftover $1305.30 |
| `HQ` | 1 | 2026-09-04 @ $17.06 | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; ret5=+17.3; leftover $27.54 |
| `OABI` | 5 | 2026-09-04 @ $5.08 | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+28.1; leftover $27.54 |
| `TRLV` | 2 | 2026-09-04 @ $11.89 | rank by hot_score; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+15.0; leftover $27.54 |
