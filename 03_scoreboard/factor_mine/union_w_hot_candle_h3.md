# Factor mine action — `union_w_hot_candle_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `w_hot_candle` · size `leftover` · sell `list` · S-boost `none` · rank by w_hot_candle

Cash book **+13.09%** ($11,309) · signal-only (no cash/fees) was +27.64%. Starts YES **16/17**. Fills 106 · skips 160 · realized $+1035.18.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `w_hot_candle` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $4.55.

## Each session (cash + holdings state)

| Date | S | Open cash | Open held | Bought | Sold | Close cash | Stock | Equity | Close held | Why |
|---|---:|---:|---|---|---|---:|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | TNDM, IREN, TPG, HIMS, INO, VOR, SLS, BTSG | — | $107.38 | $10,161.33 | $10,268.71 | TNDM×53, IREN×27, TPG×24, HIMS×42, INO×1543, VOR×56, SLS×106, BTSG×20 | BUY TNDM x53 @ 23.33; BUY IREN x27 @ 45.98; BUY TPG x24 @ 50.62; BUY HIMS x42 @ 29.74; BUY INO x1543 @ 0.81; BUY VOR x56 @ 22.01; BUY SLS x106 @ 11.70; BUY BTSG x20 @ 59.80 |
| 2026-08-14 | +5.50 | $107.38 | TNDM×53, IREN×27, TPG×24, HIMS×42, INO×1543, VOR×56, SLS×106, BTSG×20 | ZENA, AIRO | — | $82.80 | $10,431.64 | $10,514.44 | TNDM×53, IREN×27, TPG×24, HIMS×42, INO×1543, VOR×56, SLS×106, BTSG×20, ZENA×6, AIRO×1 | BUY ZENA x6 @ 2.20; BUY AIRO x1 @ 11.12 |
| 2026-08-17 | +2.25 | $82.80 | TNDM×53, IREN×27, TPG×24, HIMS×42, INO×1543, VOR×56, SLS×106, BTSG×20, ZENA×6, AIRO×1 | XHG, SMJF, NPWR, CAPR | — | $47.47 | $10,540.60 | $10,588.08 | TNDM×53, IREN×27, TPG×24, HIMS×42, INO×1543, VOR×56, SLS×106, BTSG×20, ZENA×6, AIRO×1, XHG×2, SMJF×1, NPWR×5, CAPR×1 | BUY XHG x2 @ 4.19; BUY SMJF x1 @ 10.10; BUY NPWR x5 @ 1.92; BUY CAPR x1 @ 6.87 |
| 2026-08-18 | -6.20 | $47.47 | TNDM×53, IREN×27, TPG×24, HIMS×42, INO×1543, VOR×56, SLS×106, BTSG×20, ZENA×6, AIRO×1, XHG×2, SMJF×1, NPWR×5, CAPR×1 | — | TNDM, IREN, TPG, HIMS, INO, VOR, SLS, BTSG | $10,353.91 | $55.99 | $10,409.90 | ZENA×6, AIRO×1, XHG×2, SMJF×1, NPWR×5, CAPR×1 | SELL TNDM (dropped from list after 3 sess (min 3)); SELL IREN (dropped from list after 3 sess (min 3)); SELL TPG (dropped from list after 3 sess (min 3)); SELL HIMS (dropped from list after 3 sess (min 3)); SELL INO (dropped from list after 3 sess (min 3)); SELL VOR (dropped from list after 3 sess (min 3)); SELL SLS (dropped from list after 3 sess (min 3)); SELL BTSG (dropped from list after 3 sess (min 3)); hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | -7.20 | $10,353.91 | ZENA×6, AIRO×1, XHG×2, SMJF×1, NPWR×5, CAPR×1 | — | ZENA, AIRO | $10,374.80 | $35.71 | $10,410.51 | XHG×2, SMJF×1, NPWR×5, CAPR×1 | SELL ZENA (dropped from list after 3 sess (min 3)); SELL AIRO (dropped from list after 3 sess (min 3)); hard-red S=-7.20 sit; no new buys |
| 2026-08-20 | +1.12 | $10,374.80 | XHG×2, SMJF×1, NPWR×5, CAPR×1 | MRNA, CYPH, ABCL, SENS, ALEC, BTGO, IMMX, BBNX | XHG, SMJF, NPWR, CAPR | $78.07 | $10,073.59 | $10,151.66 | MRNA×8, CYPH×1131, ABCL×110, SENS×146, ALEC×542, BTGO×196, IMMX×100, BBNX×65 | SELL XHG (dropped from list after 3 sess (min 3)); SELL SMJF (dropped from list after 3 sess (min 3)); SELL NPWR (dropped from list after 3 sess (min 3)); SELL CAPR (dropped from list after 3 sess (min 3)); BUY MRNA x8 @ 150.14; BUY CYPH x1131 @ 1.15; BUY ABCL x110 @ 11.81; BUY SENS x146 @ 8.91; BUY ALEC x542 @ 2.40; BUY BTGO x196 @ 6.61; BUY IMMX x100 @ 12.98; BUY BBNX x65 @ 20.00 |
| 2026-08-21 | +3.25 | $78.07 | MRNA×8, CYPH×1131, ABCL×110, SENS×146, ALEC×542, BTGO×196, IMMX×100, BBNX×65 | XHG, ARCT, IOVA, DFDV, XXI, INO | — | $10.90 | $10,721.09 | $10,731.99 | MRNA×8, CYPH×1131, ABCL×110, SENS×146, ALEC×542, BTGO×196, IMMX×100, BBNX×65, XHG×2, ARCT×1, IOVA×1, DFDV×3, XXI×2, INO×10 | BUY XHG x2 @ 4.49; BUY ARCT x1 @ 11.13; BUY IOVA x1 @ 9.08; BUY DFDV x3 @ 4.04; BUY XXI x2 @ 6.42; BUY INO x10 @ 1.23 |
| 2026-08-24 | -5.17 | $10.90 | MRNA×8, CYPH×1131, ABCL×110, SENS×146, ALEC×542, BTGO×196, IMMX×100, BBNX×65, XHG×2, ARCT×1, IOVA×1, DFDV×3, XXI×2, INO×10 | — | — | $10.90 | $10,865.49 | $10,876.39 | MRNA×8, CYPH×1131, ABCL×110, SENS×146, ALEC×542, BTGO×196, IMMX×100, BBNX×65, XHG×2, ARCT×1, IOVA×1, DFDV×3, XXI×2, INO×10 | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | +1.80 | $10.90 | MRNA×8, CYPH×1131, ABCL×110, SENS×146, ALEC×542, BTGO×196, IMMX×100, BBNX×65, XHG×2, ARCT×1, IOVA×1, DFDV×3, XXI×2, INO×10 | ASST, AU, RUM, OMER, BMNR, TRLV | MRNA, ABCL, SENS, ALEC, BTGO, IMMX, BBNX | $83.33 | $10,617.01 | $10,700.34 | CYPH×1131, XHG×2, ARCT×1, IOVA×1, DFDV×3, XXI×2, INO×10, ASST×70, AU×12, RUM×157, OMER×78, BMNR×59, TRLV×134 | SELL MRNA (dropped from list after 3 sess (min 3)); SELL ABCL (dropped from list after 3 sess (min 3)); SELL SENS (dropped from list after 3 sess (min 3)); SELL ALEC (dropped from list after 3 sess (min 3)); SELL BTGO (dropped from list after 3 sess (min 3)); SELL IMMX (dropped from list after 3 sess (min 3)); SELL BBNX (dropped from list after 3 sess (min 3)); BUY ASST x70 @ 20.90; BUY AU x12 @ 119.46; BUY RUM x157 @ 9.36; BUY OMER x78 @ 18.75; BUY BMNR x59 @ 24.73; BUY TRLV x134 @ 11.02 |
| 2026-08-26 | +2.02 | $83.33 | CYPH×1131, XHG×2, ARCT×1, IOVA×1, DFDV×3, XXI×2, INO×10, ASST×70, AU×12, RUM×157, OMER×78, BMNR×59, TRLV×134 | — | — | $83.33 | $10,755.96 | $10,839.29 | CYPH×1131, XHG×2, ARCT×1, IOVA×1, DFDV×3, XXI×2, INO×10, ASST×70, AU×12, RUM×157, OMER×78, BMNR×59, TRLV×134 | hold CYPH,XHG,ARCT,IOVA,DFDV,XXI,INO,ASST,AU,RUM,OMER,BMNR,TRLV |
| 2026-08-27 | — | $83.33 | CYPH×1131, XHG×2, ARCT×1, IOVA×1, DFDV×3, XXI×2, INO×10, ASST×70, AU×12, RUM×157, OMER×78, BMNR×59, TRLV×134 | MOS, DLO, RRC, GEN, SLI, PLTR, CRK, PGY | CYPH, XHG, ARCT, IOVA, DFDV, XXI, INO | $146.08 | $10,623.40 | $10,769.48 | ASST×70, AU×12, RUM×157, OMER×78, BMNR×59, TRLV×134, MOS×9, DLO×15, RRC×5, GEN×8, SLI×93, PLTR×1, CRK×17, PGY×11 | SELL CYPH (dropped from list after 5 sess (min 3)); SELL XHG (dropped from list after 4 sess (min 3)); SELL ARCT (dropped from list after 4 sess (min 3)); SELL IOVA (dropped from list after 4 sess (min 3)); SELL DFDV (dropped from list after 4 sess (min 3)); SELL XXI (dropped from list after 4 sess (min 3)); SELL INO (dropped from list after 4 sess (min 3)); BUY MOS x9 @ 24.84; BUY DLO x15 @ 15.60; BUY RRC x5 @ 40.72; BUY GEN x8 @ 28.89; BUY SLI x93 @ 2.59; BUY PLTR x1 @ 170.60; BUY CRK x17 @ 14.09; BUY PGY x11 @ 21.97 |
| 2026-08-28 | +0.75 | $146.08 | ASST×70, AU×12, RUM×157, OMER×78, BMNR×59, TRLV×134, MOS×9, DLO×15, RRC×5, GEN×8, SLI×93, PLTR×1, CRK×17, PGY×11 | FIGR, VIRT, ZYME, NIQ, AMTX, NVAX, WPM | ASST, AU, RUM, OMER, BMNR | $227.72 | $10,670.39 | $10,898.11 | TRLV×134, MOS×9, DLO×15, RRC×5, GEN×8, SLI×93, PLTR×1, CRK×17, PGY×11, FIGR×28, VIRT×16, ZYME×36, NIQ×57, AMTX×577, NVAX×118, WPM×6 | SELL ASST (dropped from list after 3 sess (min 3)); SELL AU (dropped from list after 3 sess (min 3)); SELL RUM (dropped from list after 3 sess (min 3)); SELL OMER (dropped from list after 3 sess (min 3)); SELL BMNR (dropped from list after 3 sess (min 3)); BUY FIGR x28 @ 37.42; BUY VIRT x16 @ 65.42; BUY ZYME x36 @ 29.33; BUY NIQ x57 @ 18.79; BUY AMTX x577 @ 1.87; BUY NVAX x118 @ 9.12; BUY WPM x6 @ 155.89 |
| 2026-08-31 | -5.85 | $227.72 | TRLV×134, MOS×9, DLO×15, RRC×5, GEN×8, SLI×93, PLTR×1, CRK×17, PGY×11, FIGR×28, VIRT×16, ZYME×36, NIQ×57, AMTX×577, NVAX×118, WPM×6 | — | TRLV | $1,888.23 | $9,097.75 | $10,985.98 | MOS×9, DLO×15, RRC×5, GEN×8, SLI×93, PLTR×1, CRK×17, PGY×11, FIGR×28, VIRT×16, ZYME×36, NIQ×57, AMTX×577, NVAX×118, WPM×6 | SELL TRLV (dropped from list after 4 sess (min 3)); hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | -6.30 | $1,888.23 | MOS×9, DLO×15, RRC×5, GEN×8, SLI×93, PLTR×1, CRK×17, PGY×11, FIGR×28, VIRT×16, ZYME×36, NIQ×57, AMTX×577, NVAX×118, WPM×6 | — | MOS, DLO, RRC, GEN, SLI, PLTR, CRK, PGY | $3,681.01 | $7,255.55 | $10,936.56 | FIGR×28, VIRT×16, ZYME×36, NIQ×57, AMTX×577, NVAX×118, WPM×6 | SELL MOS (dropped from list after 3 sess (min 3)); SELL DLO (dropped from list after 3 sess (min 3)); SELL RRC (dropped from list after 3 sess (min 3)); SELL GEN (dropped from list after 3 sess (min 3)); SELL SLI (dropped from list after 3 sess (min 3)); SELL PLTR (dropped from list after 3 sess (min 3)); SELL CRK (dropped from list after 3 sess (min 3)); SELL PGY (dropped from list after 3 sess (min 3)); hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | -3.83 | $3,681.01 | FIGR×28, VIRT×16, ZYME×36, NIQ×57, AMTX×577, NVAX×118, WPM×6 | — | FIGR, VIRT, NIQ, AMTX, WPM | $8,747.82 | $2,262.28 | $11,010.10 | ZYME×36, NVAX×118 | SELL FIGR (dropped from list after 3 sess (min 3)); SELL VIRT (dropped from list after 3 sess (min 3)); SELL NIQ (dropped from list after 3 sess (min 3)); SELL AMTX (dropped from list after 3 sess (min 3)); SELL WPM (dropped from list after 3 sess (min 3)); hard-red S=-3.83 sit; no new buys |
| 2026-09-03 | -0.90 | $8,747.82 | ZYME×36, NVAX×118 | MRNA, XHG, ARCT, CAN, OMER, TRLV, SG, VIRT | ZYME, NVAX | $4.55 | $10,935.66 | $10,940.21 | MRNA×9, XHG×386, ARCT×83, CAN×4597, OMER×72, TRLV×117, SG×214, VIRT×21 | SELL ZYME (dropped from list after 4 sess (min 3)); SELL NVAX (dropped from list after 4 sess (min 3)); BUY MRNA x9 @ 151.40; BUY XHG x386 @ 3.57; BUY ARCT x83 @ 16.46; BUY CAN x4597 @ 0.30; BUY OMER x72 @ 18.97; BUY TRLV x117 @ 11.78; BUY SG x214 @ 6.43; BUY VIRT x21 @ 65.64 |
| 2026-09-04 | — | $4.55 | MRNA×9, XHG×386, ARCT×83, CAN×4597, OMER×72, TRLV×117, SG×214, VIRT×21 | — | — | $4.55 | $11,304.38 | $11,308.93 | MRNA×9, XHG×386, ARCT×83, CAN×4597, OMER×72, TRLV×117, SG×214, VIRT×21 | hold MRNA,XHG,ARCT,CAN,OMER,TRLV,SG,VIRT |

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
| 2026-08-14 09:30 ET | **BUY** | `ZENA` | 6 | $2.20 | $0.15 | — | $94.03 | rank by w_hot_candle; rank w_hot_candle; list yday_gainer; 🔵; ⚪; ret5=+54.3; leftover $13.42 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `AIRO` | 1 | $11.12 | $0.11 | — | $82.80 | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; leftover $13.42 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `XHG` | 2 | $4.19 | $0.09 | — | $74.33 | rank by w_hot_candle; rank w_hot_candle; list yday_mover; ⚪; ret5=+291.8; leftover $10.35 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `SMJF` | 1 | $10.10 | $0.10 | — | $64.12 | rank by w_hot_candle; rank w_hot_candle; list mover_buy; ret5=+22.8; leftover $10.35 | join🔴 sector🔴 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `NPWR` | 5 | $1.92 | $0.11 | — | $54.41 | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; ⚪; ret5=+28.3; leftover $10.35 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `CAPR` | 1 | $6.87 | $0.07 | — | $47.47 | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; ret5=+62.6; leftover $10.35 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `TNDM` | 53 | $22.16 | $2.17 | $-66.33 | $1,219.78 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `IREN` | 27 | $43.56 | $2.09 | $-69.50 | $2,393.81 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TPG` | 24 | $51.77 | $2.08 | $+23.38 | $3,634.21 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `HIMS` | 42 | $27.85 | $2.14 | $-83.63 | $4,801.77 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `INO` | 1543 | $1.14 | $20.17 | $+471.89 | $6,540.62 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `VOR` | 56 | $22.82 | $2.18 | $+41.02 | $7,816.36 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `SLS` | 106 | $12.66 | $2.34 | $+97.12 | $9,155.98 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `BTSG` | 20 | $60.00 | $2.07 | $-0.12 | $10,353.91 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `ZENA` | 6 | $2.01 | $0.16 | $-1.45 | $10,365.82 | dropped from list after 3 sess (min 3) | join🔴 sector🟡 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-19 09:30 ET | **SELL** | `AIRO` | 1 | $9.10 | $0.11 | $-2.25 | $10,374.80 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `XHG` | 2 | $4.10 | $0.11 | $-0.38 | $10,382.89 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `SMJF` | 1 | $10.72 | $0.13 | $+0.39 | $10,393.48 | dropped from list after 3 sess (min 3) | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SELL** | `NPWR` | 5 | $1.64 | $0.12 | $-1.63 | $10,401.57 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `CAPR` | 1 | $7.66 | $0.10 | $+0.62 | $10,409.13 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 8 | $150.14 | $2.01 | — | $9,205.99 | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; 🔵; ret5=+173.9; leftover $1301.14 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CYPH` | 1131 | $1.15 | $14.59 | — | $7,890.75 | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+62.6; leftover $1301.14 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ABCL` | 110 | $11.81 | $2.32 | — | $6,588.78 | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $1301.14 | join🟢 sector🟡 gen🟢 news🔴 digest🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `SENS` | 146 | $8.91 | $2.43 | — | $5,285.49 | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ⚪; ret5=+14.9; leftover $1301.14 | join🟡 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ALEC` | 542 | $2.40 | $6.99 | — | $3,977.70 | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ret5=+13.0; leftover $1301.14 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BTGO` | 196 | $6.61 | $2.58 | — | $2,680.54 | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; 🔵; ret5=+25.3; leftover $1301.14 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟡 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IMMX` | 100 | $12.98 | $2.29 | — | $1,380.25 | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $1301.14 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BBNX` | 65 | $20.00 | $2.19 | — | $78.07 | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ⚪; ret5=+15.5; leftover $1301.14 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `XHG` | 2 | $4.49 | $0.10 | — | $68.99 | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ret5=+12.7; leftover $13.01 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🔴 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 1 | $11.13 | $0.11 | — | $57.75 | rank by w_hot_candle; rank w_hot_candle; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $13.01 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `IOVA` | 1 | $9.08 | $0.09 | — | $48.58 | rank by w_hot_candle; rank w_hot_candle; list yday_gainer; 🔵; ⚪; ret5=+37.9; leftover $13.01 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `DFDV` | 3 | $4.04 | $0.13 | — | $36.33 | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+23.6; leftover $13.01 | join🟡 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `XXI` | 2 | $6.42 | $0.13 | — | $23.35 | rank by w_hot_candle; rank w_hot_candle; list yday_gainer; ret5=+23.8; leftover $13.01 | join🔴 sector🔴 gen🟢 news🟡 digest🟡 ab🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `INO` | 10 | $1.23 | $0.15 | — | $10.90 | rank by w_hot_candle; rank w_hot_candle; list mover_buy; 🔵; ⚪; ret5=+34.4; leftover $13.01 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **SELL** | `MRNA` | 8 | $141.19 | $2.03 | $-75.65 | $1,138.38 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ABCL` | 110 | $10.77 | $2.35 | $-119.62 | $2,320.74 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `SENS` | 146 | $9.66 | $2.46 | $+104.61 | $3,728.63 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ALEC` | 542 | $2.30 | $7.09 | $-68.28 | $4,968.14 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BTGO` | 196 | $6.89 | $2.62 | $+50.66 | $6,315.96 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `IMMX` | 100 | $13.40 | $2.32 | $+37.39 | $7,653.64 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BBNX` | 65 | $18.61 | $2.21 | $-94.74 | $8,861.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `ASST` | 70 | $20.90 | $2.20 | — | $7,395.89 | rank by w_hot_candle; rank w_hot_candle; list yday_gainer; 🔵; ret5=+47.9; leftover $1476.85 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 12 | $119.46 | $2.03 | — | $5,960.34 | rank by w_hot_candle; rank w_hot_candle; list mover_buy; 🔵; ⚪; ret5=+25.9; leftover $1476.85 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `RUM` | 157 | $9.36 | $2.46 | — | $4,488.36 | rank by w_hot_candle; rank w_hot_candle; list yday_gainer; 🔵; ret5=+21.3; leftover $1476.85 | join🔴 sector🟡 gen🟡 news🟢 digest🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `OMER` | 78 | $18.75 | $2.22 | — | $3,023.64 | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; ret5=+12.1; leftover $1476.85 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BMNR` | 59 | $24.73 | $2.17 | — | $1,562.40 | rank by w_hot_candle; rank w_hot_candle; list yday_gainer; ret5=+26.3; leftover $1476.85 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟡 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `TRLV` | 134 | $11.02 | $2.39 | — | $83.33 | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ret5=+15.0; leftover $1476.85 | join🔴 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 vol🟡 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `CYPH` | 1131 | $1.60 | $14.79 | $+479.57 | $1,878.13 | dropped from list after 5 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `XHG` | 2 | $3.81 | $0.10 | $-1.56 | $1,885.65 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `ARCT` | 1 | $15.35 | $0.18 | $+3.93 | $1,900.83 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `IOVA` | 1 | $8.34 | $0.11 | $-0.94 | $1,909.06 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `DFDV` | 3 | $4.35 | $0.16 | $+0.64 | $1,921.95 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `XXI` | 2 | $6.36 | $0.15 | $-0.41 | $1,934.52 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `INO` | 10 | $1.28 | $0.18 | $+0.17 | $1,947.14 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **BUY** | `MOS` | 9 | $24.84 | $2.02 | — | $1,721.56 | rank by w_hot_candle; rank w_hot_candle; list flatten; ret5=+13.0; leftover $243.39 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟢 |
| 2026-08-27 09:30 ET | **BUY** | `DLO` | 15 | $15.60 | $2.04 | — | $1,485.53 | rank by w_hot_candle; rank w_hot_candle; list mover_buy; 🔵; ret5=+7.1; leftover $243.39 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 5 | $40.72 | $2.00 | — | $1,279.92 | rank by w_hot_candle; rank w_hot_candle; list flatten; ret5=+1.8; leftover $243.39 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `GEN` | 8 | $28.89 | $2.01 | — | $1,046.79 | rank by w_hot_candle; rank w_hot_candle; list mover_buy; 🔵; ret5=+1.6; leftover $243.39 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 93 | $2.59 | $2.27 | — | $803.65 | rank by w_hot_candle; rank w_hot_candle; list flatten; ret5=+4.2; leftover $243.39 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `PLTR` | 1 | $170.60 | $1.71 | — | $631.34 | rank by w_hot_candle; rank w_hot_candle; list mover_buy; 🔵; ret5=+3.4; leftover $243.39 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 17 | $14.09 | $2.04 | — | $389.77 | rank by w_hot_candle; rank w_hot_candle; list flatten; ret5=+1.1; leftover $243.39 | join🟢 sector🔴 gen🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `PGY` | 11 | $21.97 | $2.02 | — | $146.08 | rank by w_hot_candle; rank w_hot_candle; list mover_buy; 🔵; ret5=+0.6; leftover $243.39 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `ASST` | 70 | $22.45 | $2.22 | $+104.08 | $1,715.35 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `AU` | 12 | $117.41 | $2.05 | $-28.67 | $3,122.22 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `RUM` | 157 | $9.51 | $2.50 | $+18.59 | $4,612.79 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `OMER` | 78 | $18.24 | $2.25 | $-44.25 | $6,033.27 | dropped from list after 3 sess (min 3) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `BMNR` | 59 | $25.91 | $2.19 | $+65.26 | $7,559.77 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `FIGR` | 28 | $37.42 | $2.07 | — | $6,509.93 | rank by w_hot_candle; rank w_hot_candle; list yday_mover; ret5=+24.4; leftover $1079.97 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `VIRT` | 16 | $65.42 | $2.04 | — | $5,461.18 | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; ret5=+13.2; leftover $1079.97 | join🟢 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ZYME` | 36 | $29.33 | $2.10 | — | $4,403.20 | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,ohlc_hot; ret5=+14.1; leftover $1079.97 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `NIQ` | 57 | $18.79 | $2.16 | — | $3,330.01 | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; ret5=+7.6; leftover $1079.97 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `AMTX` | 577 | $1.87 | $7.44 | — | $2,243.57 | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; ret5=+16.9; leftover $1079.97 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `NVAX` | 118 | $9.12 | $2.34 | — | $1,165.07 | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; ret5=+11.1; leftover $1079.97 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `WPM` | 6 | $155.89 | $2.01 | — | $227.72 | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; ret5=+17.6; leftover $1079.97 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `TRLV` | 134 | $12.41 | $2.43 | $+181.44 | $1,888.23 | dropped from list after 4 sess (min 3) | join🔴 sector🔴 gen🔴 news🟢 digest🟢 judge🟢 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-01 09:30 ET | **SELL** | `MOS` | 9 | $24.00 | $2.04 | $-11.61 | $2,102.20 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `DLO` | 15 | $14.88 | $2.06 | $-14.89 | $2,323.34 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `RRC` | 5 | $41.32 | $2.02 | $-1.03 | $2,527.92 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `GEN` | 8 | $30.56 | $2.03 | $+9.31 | $2,770.36 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `SLI` | 93 | $2.70 | $2.29 | $+5.67 | $3,019.17 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `PLTR` | 1 | $185.52 | $1.88 | $+11.33 | $3,202.81 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `CRK` | 17 | $14.31 | $2.06 | $-0.36 | $3,444.02 | dropped from list after 3 sess (min 3) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-01 09:30 ET | **SELL** | `PGY` | 11 | $21.73 | $2.04 | $-6.71 | $3,681.01 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `FIGR` | 28 | $35.46 | $2.09 | $-59.05 | $4,671.79 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `VIRT` | 16 | $65.38 | $2.06 | $-4.74 | $5,715.81 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `NIQ` | 57 | $19.00 | $2.18 | $+7.63 | $6,796.63 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `AMTX` | 577 | $1.88 | $7.55 | $-9.22 | $7,873.84 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `WPM` | 6 | $146.00 | $2.03 | $-63.38 | $8,747.82 | dropped from list after 3 sess (min 3) | join🟢 sector🔴 gen🔴 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-03 09:30 ET | **SELL** | `ZYME` | 36 | $30.00 | $2.12 | $+19.90 | $9,825.70 | dropped from list after 4 sess (min 3) | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟡 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **SELL** | `NVAX` | 118 | $10.27 | $2.37 | $+130.98 | $11,035.18 | dropped from list after 4 sess (min 3) | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `MRNA` | 9 | $151.40 | $2.02 | — | $9,670.57 | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; 🔵; ret5=+129.2; leftover $1379.40 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `XHG` | 386 | $3.57 | $4.98 | — | $8,287.57 | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; ret5=+16.1; leftover $1379.40 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 83 | $16.46 | $2.24 | — | $6,919.15 | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+63.4; leftover $1379.40 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CAN` | 4597 | $0.30 | $27.58 | — | $5,512.47 | rank by w_hot_candle; rank w_hot_candle; list yday_mover; 🔵; ret5=+54.3; leftover $1379.40 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `OMER` | 72 | $18.97 | $2.21 | — | $4,144.42 | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; ret5=+12.1; leftover $1379.40 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `TRLV` | 117 | $11.78 | $2.34 | — | $2,763.82 | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ret5=+15.0; leftover $1379.40 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `SG` | 214 | $6.43 | $2.76 | — | $1,385.04 | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; ret5=+11.3; leftover $1379.40 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `VIRT` | 21 | $65.64 | $2.05 | — | $4.55 | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ret5=+13.2; leftover $1379.40 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `TNDM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `IREN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `TPG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `HIMS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `INO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `VOR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `SLS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `BTSG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `QMCO` | cash | leftover split 13.42 < 1 share @ 24.68 |
| 2026-08-14 | `ARX` | cash | leftover split 13.42 < 1 share @ 19.57 |
| 2026-08-14 | `LIFE` | cash | leftover split 13.42 < 1 share @ 35.04 |
| 2026-08-14 | `BETA` | cash | leftover split 13.42 < 1 share @ 25.21 |
| 2026-08-14 | `LUNR` | cash | leftover split 13.42 < 1 share @ 19.17 |
| 2026-08-14 | `VOYG` | cash | leftover split 13.42 < 1 share @ 44.49 |
| 2026-08-17 | `TNDM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `IREN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `TPG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `HIMS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `INO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `VOR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `SLS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `BTSG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `ZENA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `AIRO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `STDN` | cash | leftover split 10.35 < 1 share @ 13.64 |
| 2026-08-17 | `HTFL` | cash | leftover split 10.35 < 1 share @ 41.23 |
| 2026-08-17 | `NMAX` | cash | leftover split 10.35 < 1 share @ 10.97 |
| 2026-08-17 | `UMAC` | cash | leftover split 10.35 < 1 share @ 32.55 |
| 2026-08-18 | `ZENA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `AIRO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `XHG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `SMJF` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `NPWR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `FDMT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AVAH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OABI` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `IOVA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CRDL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `KYTX` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OVID` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `XHG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `SMJF` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `NPWR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `CAPR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `IVVD` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `NMAX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SENS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ARX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `KURA` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `HAE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `REAX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `ABCL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `SENS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `ALEC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `BTGO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `IMMX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `BBNX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `MRNA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `CYPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ABCL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `SENS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ALEC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BTGO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `IMMX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BBNX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `XHG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `ARCT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `IOVA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `DFDV` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `XXI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `INO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `OMER` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ALM` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `TRLV` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SG` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INFQ` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ZYME` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `KGC` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HMY` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `ARCT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `IOVA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `DFDV` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `XXI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `INO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `CYPH` | no_price | no 09:30 open — carry |
| 2026-08-26 | `ARCT` | no_price | no 09:30 open — carry |
| 2026-08-26 | `IOVA` | no_price | no 09:30 open — carry |
| 2026-08-26 | `DFDV` | no_price | no 09:30 open — carry |
| 2026-08-26 | `XXI` | no_price | no 09:30 open — carry |
| 2026-08-26 | `INO` | no_price | no 09:30 open — carry |
| 2026-08-26 | `ASST` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `AU` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `RUM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `BMNR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `USDE` | no_price | no 09:30 open |
| 2026-08-26 | `SENS` | no_price | no 09:30 open |
| 2026-08-26 | `SG` | no_price | no 09:30 open |
| 2026-08-26 | `ZYME` | no_price | no 09:30 open |
| 2026-08-26 | `NIQ` | no_price | no 09:30 open |
| 2026-08-27 | `ASST` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `AU` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `RUM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `OMER` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `BMNR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `TRLV` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `MOS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `DLO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `RRC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `GEN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `SLI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `PLTR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `CRK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `PGY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `MOS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `DLO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `RRC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `GEN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `SLI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `PLTR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `CRK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `PGY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `FIGR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `ZYME` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `NIQ` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `AMTX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `NVAX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `WPM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `USDE` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CYPH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `XHG` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PURR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CAN` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `OMER` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `SG` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `FIGR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `VIRT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `ZYME` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `NIQ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `AMTX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `WPM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `CYPH` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `OMER` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `TRLV` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `SG` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CELH` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `TXG` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `INO` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `USDE` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `XHG` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `OMER` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `TRLV` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `SG` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `NOG` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-04 | `MRNA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `ARCT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CAN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `HQ` | cash | leftover split 1.52 < 1 share @ 17.06 |
| 2026-09-04 | `ZYME` | cash | leftover split 1.52 < 1 share @ 31.34 |
| 2026-09-04 | `NIQ` | cash | leftover split 1.52 < 1 share @ 18.66 |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `MRNA` | 9 | 2026-09-03 @ $151.40 | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; 🔵; ret5=+129.2; leftover $1379.40 |
| `XHG` | 386 | 2026-09-03 @ $3.57 | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; ret5=+16.1; leftover $1379.40 |
| `ARCT` | 83 | 2026-09-03 @ $16.46 | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+63.4; leftover $1379.40 |
| `CAN` | 4597 | 2026-09-03 @ $0.30 | rank by w_hot_candle; rank w_hot_candle; list yday_mover; 🔵; ret5=+54.3; leftover $1379.40 |
| `OMER` | 72 | 2026-09-03 @ $18.97 | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; ret5=+12.1; leftover $1379.40 |
| `TRLV` | 117 | 2026-09-03 @ $11.78 | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ret5=+15.0; leftover $1379.40 |
| `SG` | 214 | 2026-09-03 @ $6.43 | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; ret5=+11.3; leftover $1379.40 |
| `VIRT` | 21 | 2026-09-03 @ $65.64 | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ret5=+13.2; leftover $1379.40 |
