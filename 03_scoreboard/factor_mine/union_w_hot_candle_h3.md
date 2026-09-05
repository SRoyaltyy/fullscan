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

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | vs yday close | Bought | Sold | Close cash | Close equity | Close held | 09:30 why |
|---|---:|---:|---|---:|---:|---|---|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | TNDM, IREN, TPG, HIMS, INO, VOR, SLS, BTSG | — | $107.38 | $10,268.71 | TNDM×53, IREN×27, TPG×24, HIMS×42, INO×1543, VOR×56, SLS×106, BTSG×20 | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-14 | +5.50 | $107.38 | TNDM×53, IREN×27, TPG×24, HIMS×42, INO×1543, VOR×56, SLS×106, BTSG×20 | $10,312.70 | +43.99 | ZENA, AIRO | — | $82.80 | $10,514.44 | TNDM×53, IREN×27, TPG×24, HIMS×42, INO×1543, VOR×56, SLS×106, BTSG×20, ZENA×6, AIRO×1 | 09:30 open · cash $107.38 (unchanged overnight, no fees) · equity $10,312.70 vs prior close $10,268.71 (+43.99) because holdings re-marked: TNDM×53 yday $23.13 → 09:30 $22.92 -11.13; IREN×27 yday $44.76 → 09:30 $44.09 -18.09; TPG×24 yday $54.62 → 09:30 $55.29 +16.08; HIMS×42 yday $28.77 → 09:30 $29.15 +15.96; INO×1543 yday $0.90 → 09:30 $0.93 +46.29; VOR×56 yday $23.29 → 09:30 $23.33 +2.24; SLS×106 yday $12.36 → 09:30 $12.40 +4.24; BTSG×20 yday $60.23 → 09:30 $59.65 -11.60 |
| 2026-08-17 | +2.25 | $82.80 | TNDM×53, IREN×27, TPG×24, HIMS×42, INO×1543, VOR×56, SLS×106, BTSG×20, ZENA×6, AIRO×1 | $10,487.00 | -27.44 | XHG, SMJF, NPWR, CAPR | — | $47.47 | $10,588.08 | TNDM×53, IREN×27, TPG×24, HIMS×42, INO×1543, VOR×56, SLS×106, BTSG×20, ZENA×6, AIRO×1, XHG×2, SMJF×1, NPWR×5, CAPR×1 | 09:30 open · cash $82.80 (unchanged overnight, no fees) · equity $10,487.00 vs prior close $10,514.44 (-27.44) because holdings re-marked: TNDM×53 yday $22.72 → 09:30 $22.50 -11.66; IREN×27 yday $44.06 → 09:30 $45.23 +31.59; TPG×24 yday $53.03 → 09:30 $52.67 -8.64; HIMS×42 yday $28.15 → 09:30 $28.14 -0.42; INO×1543 yday $1.09 → 09:30 $1.07 -30.86; VOR×56 yday $23.03 → 09:30 $22.91 -6.72; SLS×106 yday $12.78 → 09:30 $12.78 +0.00; BTSG×20 yday $61.71 → 09:30 $61.69 -0.40; ZENA×6 yday $2.14 → 09:30 $2.08 -0.33; AIRO×1 yday $9.57 → 09:30 $9.57 +0.00 |
| 2026-08-18 | -6.20 | $47.47 | TNDM×53, IREN×27, TPG×24, HIMS×42, INO×1543, VOR×56, SLS×106, BTSG×20, ZENA×6, AIRO×1, XHG×2, SMJF×1, NPWR×5, CAPR×1 | $10,444.16 | -143.92 | — | TNDM, IREN, TPG, HIMS, INO, VOR, SLS, BTSG | $10,353.91 | $10,409.90 | ZENA×6, AIRO×1, XHG×2, SMJF×1, NPWR×5, CAPR×1 | 09:30 open · cash $47.47 (unchanged overnight, no fees) · equity $10,444.16 vs prior close $10,588.08 (-143.92) because holdings re-marked: TNDM×53 yday $22.25 → 09:30 $22.16 -5.03; IREN×27 yday $44.90 → 09:30 $43.56 -36.18; TPG×24 yday $51.77 → 09:30 $51.77 +0.00; HIMS×42 yday $28.61 → 09:30 $27.85 -31.92; INO×1543 yday $1.15 → 09:30 $1.14 -15.43; VOR×56 yday $23.01 → 09:30 $22.82 -10.64; SLS×106 yday $13.00 → 09:30 $12.66 -36.04; BTSG×20 yday $60.38 → 09:30 $60.00 -7.60; ZENA×6 yday $2.05 → 09:30 $1.95 -0.63; AIRO×1 yday $9.41 → 09:30 $9.01 -0.40; XHG×2 yday $3.91 → 09:30 $3.94 +0.06; SMJF×1 yday $10.45 → 09:30 $10.45 +0.00; NPWR×5 yday $1.73 → 09:30 $1.70 -0.15; CAPR×1 yday $7.45 → 09:30 $7.50 +0.05 |
| 2026-08-19 | -7.20 | $10,353.91 | ZENA×6, AIRO×1, XHG×2, SMJF×1, NPWR×5, CAPR×1 | $10,410.11 | +0.21 | — | ZENA, AIRO | $10,374.80 | $10,410.51 | XHG×2, SMJF×1, NPWR×5, CAPR×1 | 09:30 open · cash $10,353.91 (unchanged overnight, no fees) · equity $10,410.11 vs prior close $10,409.90 (+0.21) because holdings re-marked: ZENA×6 yday $2.04 → 09:30 $2.01 -0.18; AIRO×1 yday $8.98 → 09:30 $9.10 +0.12; XHG×2 yday $4.28 → 09:30 $4.32 +0.08; SMJF×1 yday $10.88 → 09:30 $10.71 -0.17; NPWR×5 yday $1.65 → 09:30 $1.70 +0.25; CAPR×1 yday $7.08 → 09:30 $7.19 +0.11 |
| 2026-08-20 | +1.12 | $10,374.80 | XHG×2, SMJF×1, NPWR×5, CAPR×1 | $10,409.58 | -0.93 | MRNA, CYPH, ABCL, SENS, ALEC, BTGO, IMMX, BBNX | XHG, SMJF, NPWR, CAPR | $78.07 | $10,151.66 | MRNA×8, CYPH×1131, ABCL×110, SENS×146, ALEC×542, BTGO×196, IMMX×100, BBNX×65 | 09:30 open · cash $10,374.80 (unchanged overnight, no fees) · equity $10,409.58 vs prior close $10,410.51 (-0.93) because holdings re-marked: XHG×2 yday $4.33 → 09:30 $4.10 -0.46; SMJF×1 yday $10.72 → 09:30 $10.72 +0.00; NPWR×5 yday $1.67 → 09:30 $1.64 -0.15; CAPR×1 yday $7.98 → 09:30 $7.66 -0.32 |
| 2026-08-21 | +3.25 | $78.07 | MRNA×8, CYPH×1131, ABCL×110, SENS×146, ALEC×542, BTGO×196, IMMX×100, BBNX×65 | $10,459.07 | +307.41 | XHG, ARCT, IOVA, DFDV, XXI, INO | — | $10.90 | $10,731.99 | MRNA×8, CYPH×1131, ABCL×110, SENS×146, ALEC×542, BTGO×196, IMMX×100, BBNX×65, XHG×2, ARCT×1, IOVA×1, DFDV×3, XXI×2, INO×10 | 09:30 open · cash $78.07 (unchanged overnight, no fees) · equity $10,459.07 vs prior close $10,151.66 (+307.41) because holdings re-marked: MRNA×8 yday $133.32 → 09:30 $133.11 -1.68; CYPH×1131 yday $1.19 → 09:30 $1.32 +147.03; ABCL×110 yday $11.57 → 09:30 $11.57 +0.00; SENS×146 yday $8.82 → 09:30 $9.24 +61.32; ALEC×542 yday $2.26 → 09:30 $2.28 +10.84; BTGO×196 yday $6.60 → 09:30 $6.95 +68.60; IMMX×100 yday $13.16 → 09:30 $13.36 +20.00; BBNX×65 yday $19.48 → 09:30 $19.50 +1.30 |
| 2026-08-24 | -5.17 | $10.90 | MRNA×8, CYPH×1131, ABCL×110, SENS×146, ALEC×542, BTGO×196, IMMX×100, BBNX×65, XHG×2, ARCT×1, IOVA×1, DFDV×3, XXI×2, INO×10 | $11,123.23 | +391.24 | — | — | $10.90 | $10,876.39 | MRNA×8, CYPH×1131, ABCL×110, SENS×146, ALEC×542, BTGO×196, IMMX×100, BBNX×65, XHG×2, ARCT×1, IOVA×1, DFDV×3, XXI×2, INO×10 | 09:30 open · cash $10.90 (unchanged overnight, no fees) · equity $11,123.23 vs prior close $10,731.99 (+391.24) because holdings re-marked: MRNA×8 yday $145.13 → 09:30 $142.70 -19.44; CYPH×1131 yday $1.42 → 09:30 $1.83 +463.71; ABCL×110 yday $11.32 → 09:30 $10.97 -38.50; SENS×146 yday $9.71 → 09:30 $9.57 -20.44; ALEC×542 yday $2.36 → 09:30 $2.36 +0.00; BTGO×196 yday $6.84 → 09:30 $6.87 +5.88; IMMX×100 yday $13.66 → 09:30 $13.69 +3.00; BBNX×65 yday $19.05 → 09:30 $19.00 -3.25; XHG×2 yday $4.41 → 09:30 $4.24 -0.34; ARCT×1 yday $13.45 → 09:30 $13.26 -0.19; IOVA×1 yday $8.29 → 09:30 $8.05 -0.24; DFDV×3 yday $3.94 → 09:30 $4.15 +0.63; XXI×2 yday $6.49 → 09:30 $6.60 +0.22; INO×10 yday $1.18 → 09:30 $1.20 +0.20 |
| 2026-08-25 | +1.80 | $10.90 | MRNA×8, CYPH×1131, ABCL×110, SENS×146, ALEC×542, BTGO×196, IMMX×100, BBNX×65, XHG×2, ARCT×1, IOVA×1, DFDV×3, XXI×2, INO×10 | $10,873.84 | -2.55 | ASST, AU, RUM, OMER, BMNR, TRLV | MRNA, ABCL, SENS, ALEC, BTGO, IMMX, BBNX | $83.33 | $10,700.34 | CYPH×1131, XHG×2, ARCT×1, IOVA×1, DFDV×3, XXI×2, INO×10, ASST×70, AU×12, RUM×157, OMER×78, BMNR×59, TRLV×134 | 09:30 open · cash $10.90 (unchanged overnight, no fees) · equity $10,873.84 vs prior close $10,876.39 (-2.55) because holdings re-marked: MRNA×8 yday $139.27 → 09:30 $141.19 +15.36; CYPH×1131 yday $1.64 → 09:30 $1.70 +67.86; ABCL×110 yday $10.52 → 09:30 $10.77 +27.50; SENS×146 yday $9.73 → 09:30 $9.66 -10.22; ALEC×542 yday $2.38 → 09:30 $2.30 -43.36; BTGO×196 yday $6.97 → 09:30 $6.89 -15.68; IMMX×100 yday $13.35 → 09:30 $13.40 +5.00; BBNX×65 yday $19.38 → 09:30 $18.61 -50.05; XHG×2 yday $4.06 → 09:30 $4.02 -0.08; ARCT×1 yday $13.76 → 09:30 $14.34 +0.58; IOVA×1 yday $8.22 → 09:30 $8.00 -0.22; DFDV×3 yday $4.19 → 09:30 $4.29 +0.30; XXI×2 yday $6.53 → 09:30 $6.61 +0.16; INO×10 yday $1.22 → 09:30 $1.25 +0.30 |
| 2026-08-26 | +2.02 | $83.33 | CYPH×1131, XHG×2, ARCT×1, IOVA×1, DFDV×3, XXI×2, INO×10, ASST×70, AU×12, RUM×157, OMER×78, BMNR×59, TRLV×134 | $10,700.34 | -0.00 | — | — | $83.33 | $10,839.29 | CYPH×1131, XHG×2, ARCT×1, IOVA×1, DFDV×3, XXI×2, INO×10, ASST×70, AU×12, RUM×157, OMER×78, BMNR×59, TRLV×134 | 09:30 open · cash $83.33 (unchanged overnight, no fees) · equity $10,700.34 vs prior close $10,700.34 (-0.00) because holdings re-marked: CYPH×1131 yday $1.64 → 09:30 $1.64 +0.00; XHG×2 yday $4.05 → 09:30 $4.05 +0.00; ARCT×1 yday $14.21 → 09:30 $14.21 +0.00; IOVA×1 yday $8.08 → 09:30 $8.08 +0.00; DFDV×3 yday $4.16 → 09:30 $4.16 +0.00; XXI×2 yday $6.42 → 09:30 $6.42 +0.00; INO×10 yday $1.25 → 09:30 $1.25 +0.00; ASST×70 yday $20.20 → 09:30 $20.20 +0.00; AU×12 yday $118.55 → 09:30 $118.55 +0.00; RUM×157 yday $9.35 → 09:30 $9.35 +0.00; OMER×78 yday $19.03 → 09:30 $19.03 +0.00; BMNR×59 yday $24.21 → 09:30 $24.21 +0.00; TRLV×134 yday $11.02 → 09:30 $11.02 +0.00 |
| 2026-08-27 | — | $83.33 | CYPH×1131, XHG×2, ARCT×1, IOVA×1, DFDV×3, XXI×2, INO×10, ASST×70, AU×12, RUM×157, OMER×78, BMNR×59, TRLV×134 | $10,844.32 | +5.03 | MOS, DLO, RRC, GEN, SLI, PLTR, CRK, PGY | CYPH, XHG, ARCT, IOVA, DFDV, XXI, INO | $146.08 | $10,769.48 | ASST×70, AU×12, RUM×157, OMER×78, BMNR×59, TRLV×134, MOS×9, DLO×15, RRC×5, GEN×8, SLI×93, PLTR×1, CRK×17, PGY×11 | 09:30 open · cash $83.33 (unchanged overnight, no fees) · equity $10,844.32 vs prior close $10,839.29 (+5.03) because holdings re-marked: CYPH×1131 yday $1.64 → 09:30 $1.60 -45.24; XHG×2 yday $4.05 → 09:30 $3.81 -0.48; ARCT×1 yday $14.21 → 09:30 $15.35 +1.14; IOVA×1 yday $8.08 → 09:30 $8.34 +0.26; DFDV×3 yday $4.16 → 09:30 $4.35 +0.57; XXI×2 yday $6.42 → 09:30 $6.36 -0.12; INO×10 yday $1.25 → 09:30 $1.28 +0.30; ASST×70 yday $20.20 → 09:30 $20.72 +36.40; AU×12 yday $118.55 → 09:30 $119.80 +15.00; RUM×157 yday $9.35 → 09:30 $10.07 +113.04; OMER×78 yday $19.03 → 09:30 $18.96 -5.46; BMNR×59 yday $24.21 → 09:30 $24.24 +1.77; TRLV×134 yday $11.02 → 09:30 $11.22 +26.80 |
| 2026-08-28 | +0.75 | $146.08 | ASST×70, AU×12, RUM×157, OMER×78, BMNR×59, TRLV×134, MOS×9, DLO×15, RRC×5, GEN×8, SLI×93, PLTR×1, CRK×17, PGY×11 | $10,905.61 | +136.13 | FIGR, VIRT, ZYME, NIQ, AMTX, NVAX, WPM | ASST, AU, RUM, OMER, BMNR | $227.72 | $10,898.11 | TRLV×134, MOS×9, DLO×15, RRC×5, GEN×8, SLI×93, PLTR×1, CRK×17, PGY×11, FIGR×28, VIRT×16, ZYME×36, NIQ×57, AMTX×577, NVAX×118, WPM×6 | 09:30 open · cash $146.08 (unchanged overnight, no fees) · equity $10,905.61 vs prior close $10,769.48 (+136.13) because holdings re-marked: ASST×70 yday $21.50 → 09:30 $22.45 +66.50; AU×12 yday $118.11 → 09:30 $117.41 -8.40; RUM×157 yday $9.38 → 09:30 $9.51 +20.41; OMER×78 yday $18.22 → 09:30 $18.24 +1.56; BMNR×59 yday $24.91 → 09:30 $25.91 +59.00; TRLV×134 yday $11.43 → 09:30 $11.38 -6.70; MOS×9 yday $24.16 → 09:30 $24.00 -1.44; DLO×15 yday $15.36 → 09:30 $15.33 -0.45; RRC×5 yday $41.55 → 09:30 $41.44 -0.55; GEN×8 yday $29.64 → 09:30 $29.83 +1.52; SLI×93 yday $2.61 → 09:30 $2.60 -0.93; PLTR×1 yday $177.50 → 09:30 $178.75 +1.25; CRK×17 yday $14.50 → 09:30 $14.42 -1.36; PGY×11 yday $22.41 → 09:30 $22.93 +5.72 |
| 2026-08-31 | -5.85 | $227.72 | TRLV×134, MOS×9, DLO×15, RRC×5, GEN×8, SLI×93, PLTR×1, CRK×17, PGY×11, FIGR×28, VIRT×16, ZYME×36, NIQ×57, AMTX×577, NVAX×118, WPM×6 | $10,953.61 | +55.50 | — | TRLV | $1,888.23 | $10,985.98 | MOS×9, DLO×15, RRC×5, GEN×8, SLI×93, PLTR×1, CRK×17, PGY×11, FIGR×28, VIRT×16, ZYME×36, NIQ×57, AMTX×577, NVAX×118, WPM×6 | 09:30 open · cash $227.72 (unchanged overnight, no fees) · equity $10,953.61 vs prior close $10,898.11 (+55.50) because holdings re-marked: TRLV×134 yday $11.03 → 09:30 $12.41 +184.92; MOS×9 yday $23.76 → 09:30 $23.75 -0.09; DLO×15 yday $15.14 → 09:30 $15.01 -1.95; RRC×5 yday $41.64 → 09:30 $41.11 -2.65; GEN×8 yday $30.50 → 09:30 $31.02 +4.16; SLI×93 yday $2.64 → 09:30 $2.51 -12.09; PLTR×1 yday $185.93 → 09:30 $184.04 -1.89; CRK×17 yday $14.62 → 09:30 $14.56 -1.02; PGY×11 yday $23.26 → 09:30 $21.51 -19.25; FIGR×28 yday $38.02 → 09:30 $35.50 -70.56; VIRT×16 yday $67.04 → 09:30 $66.39 -10.40; ZYME×36 yday $29.01 → 09:30 $28.27 -26.64; NIQ×57 yday $19.07 → 09:30 $19.20 +7.41; AMTX×577 yday $1.87 → 09:30 $1.90 +17.31; NVAX×118 yday $9.05 → 09:30 $9.23 +21.24; WPM×6 yday $157.99 → 09:30 $152.49 -33.00 |
| 2026-09-01 | -6.30 | $1,888.23 | MOS×9, DLO×15, RRC×5, GEN×8, SLI×93, PLTR×1, CRK×17, PGY×11, FIGR×28, VIRT×16, ZYME×36, NIQ×57, AMTX×577, NVAX×118, WPM×6 | $11,009.34 | +23.36 | — | MOS, DLO, RRC, GEN, SLI, PLTR, CRK, PGY | $3,681.01 | $10,936.56 | FIGR×28, VIRT×16, ZYME×36, NIQ×57, AMTX×577, NVAX×118, WPM×6 | 09:30 open · cash $1,888.23 (unchanged overnight, no fees) · equity $11,009.34 vs prior close $10,985.98 (+23.36) because holdings re-marked: MOS×9 yday $23.78 → 09:30 $24.00 +1.98; DLO×15 yday $15.00 → 09:30 $14.88 -1.80; RRC×5 yday $41.78 → 09:30 $41.32 -2.30; GEN×8 yday $31.02 → 09:30 $30.56 -3.68; SLI×93 yday $2.51 → 09:30 $2.70 +17.67; PLTR×1 yday $183.80 → 09:30 $185.52 +1.72; CRK×17 yday $14.51 → 09:30 $14.31 -3.40; PGY×11 yday $21.95 → 09:30 $21.73 -2.42; FIGR×28 yday $36.41 → 09:30 $36.80 +10.92; VIRT×16 yday $66.39 → 09:30 $65.64 -12.00; ZYME×36 yday $28.27 → 09:30 $29.32 +37.80; NIQ×57 yday $19.20 → 09:30 $19.06 -7.98; AMTX×577 yday $1.90 → 09:30 $1.87 -17.31; NVAX×118 yday $9.26 → 09:30 $9.37 +12.98; WPM×6 yday $152.25 → 09:30 $150.78 -8.82 |
| 2026-09-02 | -3.83 | $3,681.01 | FIGR×28, VIRT×16, ZYME×36, NIQ×57, AMTX×577, NVAX×118, WPM×6 | $10,904.85 | -31.71 | — | FIGR, VIRT, NIQ, AMTX, WPM | $8,747.82 | $11,010.10 | ZYME×36, NVAX×118 | 09:30 open · cash $3,681.01 (unchanged overnight, no fees) · equity $10,904.85 vs prior close $10,936.56 (-31.71) because holdings re-marked: FIGR×28 yday $35.70 → 09:30 $35.46 -6.72; VIRT×16 yday $65.64 → 09:30 $65.38 -4.16; ZYME×36 yday $29.33 → 09:30 $29.32 -0.36; NIQ×57 yday $19.06 → 09:30 $19.00 -3.42; AMTX×577 yday $1.87 → 09:30 $1.88 +5.77; NVAX×118 yday $9.37 → 09:30 $9.20 -20.06; WPM×6 yday $146.46 → 09:30 $146.00 -2.76 |
| 2026-09-03 | -0.90 | $8,747.82 | ZYME×36, NVAX×118 | $11,039.68 | +29.58 | MRNA, XHG, ARCT, CAN, OMER, TRLV, SG, VIRT | ZYME, NVAX | $4.55 | $10,940.21 | MRNA×9, XHG×386, ARCT×83, CAN×4597, OMER×72, TRLV×117, SG×214, VIRT×21 | 09:30 open · cash $8,747.82 (unchanged overnight, no fees) · equity $11,039.68 vs prior close $11,010.10 (+29.58) because holdings re-marked: ZYME×36 yday $29.67 → 09:30 $30.00 +11.88; NVAX×118 yday $10.12 → 09:30 $10.27 +17.70 |
| 2026-09-04 | — | $4.55 | MRNA×9, XHG×386, ARCT×83, CAN×4597, OMER×72, TRLV×117, SG×214, VIRT×21 | $11,111.35 | +171.14 | — | — | $4.55 | $11,308.93 | MRNA×9, XHG×386, ARCT×83, CAN×4597, OMER×72, TRLV×117, SG×214, VIRT×21 | 09:30 open · cash $4.55 (unchanged overnight, no fees) · equity $11,111.35 vs prior close $10,940.21 (+171.14) because holdings re-marked: MRNA×9 yday $150.81 → 09:30 $145.95 -43.74; XHG×386 yday $3.32 → 09:30 $3.38 +23.16; ARCT×83 yday $16.74 → 09:30 $16.77 +2.49; CAN×4597 yday $0.31 → 09:30 $0.34 +137.91; OMER×72 yday $18.86 → 09:30 $18.99 +9.36; TRLV×117 yday $11.69 → 09:30 $11.89 +23.40; SG×214 yday $6.73 → 09:30 $6.75 +4.28; VIRT×21 yday $62.69 → 09:30 $63.37 +14.28 |

## Fills (09:30 open snapshot, then buys / sells)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 53 | $23.33 | $2.15 | — | $8,761.36 | — | rank by w_hot_candle; rank w_hot_candle; list flatten; ⚪; ret5=+19.7; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 27 | $45.98 | $2.07 | — | $7,517.83 | — | rank by w_hot_candle; rank w_hot_candle; list flatten; ⚪; ret5=+12.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 24 | $50.62 | $2.06 | — | $6,300.81 | — | rank by w_hot_candle; rank w_hot_candle; list flatten; ⚪; ret5=+6.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 42 | $29.74 | $2.12 | — | $5,049.62 | — | rank by w_hot_candle; rank w_hot_candle; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 1543 | $0.81 | $17.13 | — | $3,782.66 | — | rank by w_hot_candle; rank w_hot_candle; list flatten; ⚪; ret5=+13.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `VOR` | 56 | $22.01 | $2.16 | — | $2,547.94 | — | rank by w_hot_candle; rank w_hot_candle; list flatten; ⚪; ret5=+0.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 106 | $11.70 | $2.31 | — | $1,305.43 | — | rank by w_hot_candle; rank w_hot_candle; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 20 | $59.80 | $2.05 | — | $107.38 | — | rank by w_hot_candle; rank w_hot_candle; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $107.38 | ▲ 09:30 equity $10,312.70 vs yday $10,268.71 (+43.99) | 09:30 open · cash $107.38 (unchanged overnight, no fees) · equity $10,312.70 vs prior close $10,268.71 (+43.99) because holdings re-marked: TNDM×53 yday $23.13 → 09:30 $22.92 -11.13; IREN×27 yday $44.76 → 09:30 $44.09 -18.09; TPG×24 yday $54.62 → 09:30 $55.29 +16.08; HIMS×42 yday $28.77 → 09:30 $29.15 +15.96; INO×1543 yday $0.90 → 09:30 $0.93 +46.29; VOR×56 yday $23.29 → 09:30 $23.33 +2.24; SLS×106 yday $12.36 → 09:30 $12.40 +4.24; BTSG×20 yday $60.23 → 09:30 $59.65 -11.60 | — |
| 2026-08-14 09:30 ET | **BUY** | `ZENA` | 6 | $2.20 | $0.15 | — | $94.03 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer; 🔵; ⚪; ret5=+54.3; leftover $13.42 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `AIRO` | 1 | $11.12 | $0.11 | — | $82.80 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; leftover $13.42 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $82.80 | ▼ 09:30 equity $10,487.00 vs yday $10,514.44 (-27.44) | 09:30 open · cash $82.80 (unchanged overnight, no fees) · equity $10,487.00 vs prior close $10,514.44 (-27.44) because holdings re-marked: TNDM×53 yday $22.72 → 09:30 $22.50 -11.66; IREN×27 yday $44.06 → 09:30 $45.23 +31.59; TPG×24 yday $53.03 → 09:30 $52.67 -8.64; HIMS×42 yday $28.15 → 09:30 $28.14 -0.42; INO×1543 yday $1.09 → 09:30 $1.07 -30.86; VOR×56 yday $23.03 → 09:30 $22.91 -6.72; SLS×106 yday $12.78 → 09:30 $12.78 +0.00; BTSG×20 yday $61.71 → 09:30 $61.69 -0.40; ZENA×6 yday $2.14 → 09:30 $2.08 -0.33; AIRO×1 yday $9.57 → 09:30 $9.57 +0.00 | — |
| 2026-08-17 09:30 ET | **BUY** | `XHG` | 2 | $4.19 | $0.09 | — | $74.33 | — | rank by w_hot_candle; rank w_hot_candle; list yday_mover; ⚪; ret5=+291.8; leftover $10.35 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `SMJF` | 1 | $10.10 | $0.10 | — | $64.12 | — | rank by w_hot_candle; rank w_hot_candle; list mover_buy; ret5=+22.8; leftover $10.35 | join🔴 sector🔴 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `NPWR` | 5 | $1.92 | $0.11 | — | $54.41 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; ⚪; ret5=+28.3; leftover $10.35 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `CAPR` | 1 | $6.87 | $0.07 | — | $47.47 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; ret5=+62.6; leftover $10.35 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $47.47 | ▼ 09:30 equity $10,444.16 vs yday $10,588.08 (-143.92) | 09:30 open · cash $47.47 (unchanged overnight, no fees) · equity $10,444.16 vs prior close $10,588.08 (-143.92) because holdings re-marked: TNDM×53 yday $22.25 → 09:30 $22.16 -5.03; IREN×27 yday $44.90 → 09:30 $43.56 -36.18; TPG×24 yday $51.77 → 09:30 $51.77 +0.00; HIMS×42 yday $28.61 → 09:30 $27.85 -31.92; INO×1543 yday $1.15 → 09:30 $1.14 -15.43; VOR×56 yday $23.01 → 09:30 $22.82 -10.64; SLS×106 yday $13.00 → 09:30 $12.66 -36.04; BTSG×20 yday $60.38 → 09:30 $60.00 -7.60; ZENA×6 yday $2.05 → 09:30 $1.95 -0.63; AIRO×1 yday $9.41 → 09:30 $9.01 -0.40; XHG×2 yday $3.91 → 09:30 $3.94 +0.06; SMJF×1 yday $10.45 → 09:30 $10.45 +0.00; NPWR×5 yday $1.73 → 09:30 $1.70 -0.15; CAPR×1 yday $7.45 → 09:30 $7.50 +0.05 | — |
| 2026-08-18 09:30 ET | **SELL** | `TNDM` | 53 | $22.16 | $2.17 | $-66.33 | $1,219.78 | ▼ -66.33 after sell → book $10,441.99; vs 09:30 mark -2.17 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `IREN` | 27 | $43.56 | $2.09 | $-69.50 | $2,393.81 | ▼ -69.50 after sell → book $10,439.90; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TPG` | 24 | $51.77 | $2.08 | $+23.38 | $3,634.21 | ▲ +23.38 after sell → book $10,437.82; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `HIMS` | 42 | $27.85 | $2.14 | $-83.63 | $4,801.77 | ▼ -83.63 after sell → book $10,435.68; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `INO` | 1543 | $1.14 | $20.17 | $+471.89 | $6,540.62 | ▲ +471.89 after sell → book $10,415.51; vs 09:30 mark -20.17 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `VOR` | 56 | $22.82 | $2.18 | $+41.02 | $7,816.36 | ▲ +41.02 after sell → book $10,413.33; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `SLS` | 106 | $12.66 | $2.34 | $+97.12 | $9,155.98 | ▲ +97.12 after sell → book $10,410.99; vs 09:30 mark -2.34 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `BTSG` | 20 | $60.00 | $2.07 | $-0.12 | $10,353.91 | ▼ -0.12 after sell → book $10,408.92; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,353.91 | ▲ 09:30 equity $10,410.11 vs yday $10,409.90 (+0.21) | 09:30 open · cash $10,353.91 (unchanged overnight, no fees) · equity $10,410.11 vs prior close $10,409.90 (+0.21) because holdings re-marked: ZENA×6 yday $2.04 → 09:30 $2.01 -0.18; AIRO×1 yday $8.98 → 09:30 $9.10 +0.12; XHG×2 yday $4.28 → 09:30 $4.32 +0.08; SMJF×1 yday $10.88 → 09:30 $10.71 -0.17; NPWR×5 yday $1.65 → 09:30 $1.70 +0.25; CAPR×1 yday $7.08 → 09:30 $7.19 +0.11 | — |
| 2026-08-19 09:30 ET | **SELL** | `ZENA` | 6 | $2.01 | $0.16 | $-1.45 | $10,365.82 | ▼ -1.45 after sell → book $10,409.96; vs 09:30 mark -0.15 | dropped from list after 3 sess (min 3) | join🔴 sector🟡 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-19 09:30 ET | **SELL** | `AIRO` | 1 | $9.10 | $0.11 | $-2.25 | $10,374.80 | ▼ -2.25 after sell → book $10,409.84; vs 09:30 mark -0.12 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,374.80 | ▼ 09:30 equity $10,409.58 vs yday $10,410.51 (-0.93) | 09:30 open · cash $10,374.80 (unchanged overnight, no fees) · equity $10,409.58 vs prior close $10,410.51 (-0.93) because holdings re-marked: XHG×2 yday $4.33 → 09:30 $4.10 -0.46; SMJF×1 yday $10.72 → 09:30 $10.72 +0.00; NPWR×5 yday $1.67 → 09:30 $1.64 -0.15; CAPR×1 yday $7.98 → 09:30 $7.66 -0.32 | — |
| 2026-08-20 09:30 ET | **SELL** | `XHG` | 2 | $4.10 | $0.11 | $-0.38 | $10,382.89 | ▼ -0.38 after sell → book $10,409.47; vs 09:30 mark -0.11 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `SMJF` | 1 | $10.72 | $0.13 | $+0.39 | $10,393.48 | ▲ +0.39 after sell → book $10,409.34; vs 09:30 mark -0.13 | dropped from list after 3 sess (min 3) | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SELL** | `NPWR` | 5 | $1.64 | $0.12 | $-1.63 | $10,401.57 | ▼ -1.63 after sell → book $10,409.23; vs 09:30 mark -0.11 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `CAPR` | 1 | $7.66 | $0.10 | $+0.62 | $10,409.13 | ▲ +0.62 after sell → book $10,409.13; vs 09:30 mark -0.10 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 8 | $150.14 | $2.01 | — | $9,205.99 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; 🔵; ret5=+173.9; leftover $1301.14 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CYPH` | 1131 | $1.15 | $14.59 | — | $7,890.75 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+62.6; leftover $1301.14 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ABCL` | 110 | $11.81 | $2.32 | — | $6,588.78 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $1301.14 | join🟢 sector🟡 gen🟢 news🔴 digest🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `SENS` | 146 | $8.91 | $2.43 | — | $5,285.49 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ⚪; ret5=+14.9; leftover $1301.14 | join🟡 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ALEC` | 542 | $2.40 | $6.99 | — | $3,977.70 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ret5=+13.0; leftover $1301.14 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BTGO` | 196 | $6.61 | $2.58 | — | $2,680.54 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; 🔵; ret5=+25.3; leftover $1301.14 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟡 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IMMX` | 100 | $12.98 | $2.29 | — | $1,380.25 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $1301.14 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BBNX` | 65 | $20.00 | $2.19 | — | $78.07 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ⚪; ret5=+15.5; leftover $1301.14 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $78.07 | ▲ 09:30 equity $10,459.07 vs yday $10,151.66 (+307.41) | 09:30 open · cash $78.07 (unchanged overnight, no fees) · equity $10,459.07 vs prior close $10,151.66 (+307.41) because holdings re-marked: MRNA×8 yday $133.32 → 09:30 $133.11 -1.68; CYPH×1131 yday $1.19 → 09:30 $1.32 +147.03; ABCL×110 yday $11.57 → 09:30 $11.57 +0.00; SENS×146 yday $8.82 → 09:30 $9.24 +61.32; ALEC×542 yday $2.26 → 09:30 $2.28 +10.84; BTGO×196 yday $6.60 → 09:30 $6.95 +68.60; IMMX×100 yday $13.16 → 09:30 $13.36 +20.00; BBNX×65 yday $19.48 → 09:30 $19.50 +1.30 | — |
| 2026-08-21 09:30 ET | **BUY** | `XHG` | 2 | $4.49 | $0.10 | — | $68.99 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ret5=+12.7; leftover $13.01 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🔴 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 1 | $11.13 | $0.11 | — | $57.75 | — | rank by w_hot_candle; rank w_hot_candle; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $13.01 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `IOVA` | 1 | $9.08 | $0.09 | — | $48.58 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer; 🔵; ⚪; ret5=+37.9; leftover $13.01 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `DFDV` | 3 | $4.04 | $0.13 | — | $36.33 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+23.6; leftover $13.01 | join🟡 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `XXI` | 2 | $6.42 | $0.13 | — | $23.35 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer; ret5=+23.8; leftover $13.01 | join🔴 sector🔴 gen🟢 news🟡 digest🟡 ab🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `INO` | 10 | $1.23 | $0.15 | — | $10.90 | — | rank by w_hot_candle; rank w_hot_candle; list mover_buy; 🔵; ⚪; ret5=+34.4; leftover $13.01 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10.90 | ▲ 09:30 equity $11,123.23 vs yday $10,731.99 (+391.24) | 09:30 open · cash $10.90 (unchanged overnight, no fees) · equity $11,123.23 vs prior close $10,731.99 (+391.24) because holdings re-marked: MRNA×8 yday $145.13 → 09:30 $142.70 -19.44; CYPH×1131 yday $1.42 → 09:30 $1.83 +463.71; ABCL×110 yday $11.32 → 09:30 $10.97 -38.50; SENS×146 yday $9.71 → 09:30 $9.57 -20.44; ALEC×542 yday $2.36 → 09:30 $2.36 +0.00; BTGO×196 yday $6.84 → 09:30 $6.87 +5.88; IMMX×100 yday $13.66 → 09:30 $13.69 +3.00; BBNX×65 yday $19.05 → 09:30 $19.00 -3.25; XHG×2 yday $4.41 → 09:30 $4.24 -0.34; ARCT×1 yday $13.45 → 09:30 $13.26 -0.19; IOVA×1 yday $8.29 → 09:30 $8.05 -0.24; DFDV×3 yday $3.94 → 09:30 $4.15 +0.63; XXI×2 yday $6.49 → 09:30 $6.60 +0.22; INO×10 yday $1.18 → 09:30 $1.20 +0.20 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10.90 | ▼ 09:30 equity $10,873.84 vs yday $10,876.39 (-2.55) | 09:30 open · cash $10.90 (unchanged overnight, no fees) · equity $10,873.84 vs prior close $10,876.39 (-2.55) because holdings re-marked: MRNA×8 yday $139.27 → 09:30 $141.19 +15.36; CYPH×1131 yday $1.64 → 09:30 $1.70 +67.86; ABCL×110 yday $10.52 → 09:30 $10.77 +27.50; SENS×146 yday $9.73 → 09:30 $9.66 -10.22; ALEC×542 yday $2.38 → 09:30 $2.30 -43.36; BTGO×196 yday $6.97 → 09:30 $6.89 -15.68; IMMX×100 yday $13.35 → 09:30 $13.40 +5.00; BBNX×65 yday $19.38 → 09:30 $18.61 -50.05; XHG×2 yday $4.06 → 09:30 $4.02 -0.08; ARCT×1 yday $13.76 → 09:30 $14.34 +0.58; IOVA×1 yday $8.22 → 09:30 $8.00 -0.22; DFDV×3 yday $4.19 → 09:30 $4.29 +0.30; XXI×2 yday $6.53 → 09:30 $6.61 +0.16; INO×10 yday $1.22 → 09:30 $1.25 +0.30 | — |
| 2026-08-25 09:30 ET | **SELL** | `MRNA` | 8 | $141.19 | $2.03 | $-75.65 | $1,138.38 | ▼ -75.65 after sell → book $10,871.80; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ABCL` | 110 | $10.77 | $2.35 | $-119.62 | $2,320.74 | ▼ -119.62 after sell → book $10,869.46; vs 09:30 mark -2.34 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `SENS` | 146 | $9.66 | $2.46 | $+104.61 | $3,728.63 | ▲ +104.61 after sell → book $10,866.99; vs 09:30 mark -2.47 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ALEC` | 542 | $2.30 | $7.09 | $-68.28 | $4,968.14 | ▼ -68.28 after sell → book $10,859.90; vs 09:30 mark -7.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BTGO` | 196 | $6.89 | $2.62 | $+50.66 | $6,315.96 | ▲ +50.66 after sell → book $10,857.28; vs 09:30 mark -2.62 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `IMMX` | 100 | $13.40 | $2.32 | $+37.39 | $7,653.64 | ▲ +37.39 after sell → book $10,854.96; vs 09:30 mark -2.32 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BBNX` | 65 | $18.61 | $2.21 | $-94.74 | $8,861.09 | ▼ -94.74 after sell → book $10,852.76; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `ASST` | 70 | $20.90 | $2.20 | — | $7,395.89 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer; 🔵; ret5=+47.9; leftover $1476.85 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 12 | $119.46 | $2.03 | — | $5,960.34 | — | rank by w_hot_candle; rank w_hot_candle; list mover_buy; 🔵; ⚪; ret5=+25.9; leftover $1476.85 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `RUM` | 157 | $9.36 | $2.46 | — | $4,488.36 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer; 🔵; ret5=+21.3; leftover $1476.85 | join🔴 sector🟡 gen🟡 news🟢 digest🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `OMER` | 78 | $18.75 | $2.22 | — | $3,023.64 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; ret5=+12.1; leftover $1476.85 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BMNR` | 59 | $24.73 | $2.17 | — | $1,562.40 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer; ret5=+26.3; leftover $1476.85 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟡 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `TRLV` | 134 | $11.02 | $2.39 | — | $83.33 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ret5=+15.0; leftover $1476.85 | join🔴 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 vol🟡 buy🟡 |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $83.33 | ▲ 09:30 equity $10,700.34 vs yday $10,700.34 (-0.00) | 09:30 open · cash $83.33 (unchanged overnight, no fees) · equity $10,700.34 vs prior close $10,700.34 (-0.00) because holdings re-marked: CYPH×1131 yday $1.64 → 09:30 $1.64 +0.00; XHG×2 yday $4.05 → 09:30 $4.05 +0.00; ARCT×1 yday $14.21 → 09:30 $14.21 +0.00; IOVA×1 yday $8.08 → 09:30 $8.08 +0.00; DFDV×3 yday $4.16 → 09:30 $4.16 +0.00; XXI×2 yday $6.42 → 09:30 $6.42 +0.00; INO×10 yday $1.25 → 09:30 $1.25 +0.00; ASST×70 yday $20.20 → 09:30 $20.20 +0.00; AU×12 yday $118.55 → 09:30 $118.55 +0.00; RUM×157 yday $9.35 → 09:30 $9.35 +0.00; OMER×78 yday $19.03 → 09:30 $19.03 +0.00; BMNR×59 yday $24.21 → 09:30 $24.21 +0.00; TRLV×134 yday $11.02 → 09:30 $11.02 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $83.33 | ▲ 09:30 equity $10,844.32 vs yday $10,839.29 (+5.03) | 09:30 open · cash $83.33 (unchanged overnight, no fees) · equity $10,844.32 vs prior close $10,839.29 (+5.03) because holdings re-marked: CYPH×1131 yday $1.64 → 09:30 $1.60 -45.24; XHG×2 yday $4.05 → 09:30 $3.81 -0.48; ARCT×1 yday $14.21 → 09:30 $15.35 +1.14; IOVA×1 yday $8.08 → 09:30 $8.34 +0.26; DFDV×3 yday $4.16 → 09:30 $4.35 +0.57; XXI×2 yday $6.42 → 09:30 $6.36 -0.12; INO×10 yday $1.25 → 09:30 $1.28 +0.30; ASST×70 yday $20.20 → 09:30 $20.72 +36.40; AU×12 yday $118.55 → 09:30 $119.80 +15.00; RUM×157 yday $9.35 → 09:30 $10.07 +113.04; OMER×78 yday $19.03 → 09:30 $18.96 -5.46; BMNR×59 yday $24.21 → 09:30 $24.24 +1.77; TRLV×134 yday $11.02 → 09:30 $11.22 +26.80 | — |
| 2026-08-27 09:30 ET | **SELL** | `CYPH` | 1131 | $1.60 | $14.79 | $+479.57 | $1,878.13 | ▲ +479.57 after sell → book $10,829.52; vs 09:30 mark -14.80 | dropped from list after 5 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `XHG` | 2 | $3.81 | $0.10 | $-1.56 | $1,885.65 | ▼ -1.56 after sell → book $10,829.42; vs 09:30 mark -0.10 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `ARCT` | 1 | $15.35 | $0.18 | $+3.93 | $1,900.83 | ▲ +3.93 after sell → book $10,829.25; vs 09:30 mark -0.17 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `IOVA` | 1 | $8.34 | $0.11 | $-0.94 | $1,909.06 | ▼ -0.94 after sell → book $10,829.14; vs 09:30 mark -0.11 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `DFDV` | 3 | $4.35 | $0.16 | $+0.64 | $1,921.95 | ▲ +0.64 after sell → book $10,828.98; vs 09:30 mark -0.16 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `XXI` | 2 | $6.36 | $0.15 | $-0.41 | $1,934.52 | ▼ -0.41 after sell → book $10,828.83; vs 09:30 mark -0.15 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `INO` | 10 | $1.28 | $0.18 | $+0.17 | $1,947.14 | ▲ +0.17 after sell → book $10,828.65; vs 09:30 mark -0.18 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **BUY** | `MOS` | 9 | $24.84 | $2.02 | — | $1,721.56 | — | rank by w_hot_candle; rank w_hot_candle; list flatten; ret5=+13.0; leftover $243.39 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟢 |
| 2026-08-27 09:30 ET | **BUY** | `DLO` | 15 | $15.60 | $2.04 | — | $1,485.53 | — | rank by w_hot_candle; rank w_hot_candle; list mover_buy; 🔵; ret5=+7.1; leftover $243.39 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 5 | $40.72 | $2.00 | — | $1,279.92 | — | rank by w_hot_candle; rank w_hot_candle; list flatten; ret5=+1.8; leftover $243.39 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `GEN` | 8 | $28.89 | $2.01 | — | $1,046.79 | — | rank by w_hot_candle; rank w_hot_candle; list mover_buy; 🔵; ret5=+1.6; leftover $243.39 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 93 | $2.59 | $2.27 | — | $803.65 | — | rank by w_hot_candle; rank w_hot_candle; list flatten; ret5=+4.2; leftover $243.39 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `PLTR` | 1 | $170.60 | $1.71 | — | $631.34 | — | rank by w_hot_candle; rank w_hot_candle; list mover_buy; 🔵; ret5=+3.4; leftover $243.39 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 17 | $14.09 | $2.04 | — | $389.77 | — | rank by w_hot_candle; rank w_hot_candle; list flatten; ret5=+1.1; leftover $243.39 | join🟢 sector🔴 gen🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `PGY` | 11 | $21.97 | $2.02 | — | $146.08 | — | rank by w_hot_candle; rank w_hot_candle; list mover_buy; 🔵; ret5=+0.6; leftover $243.39 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $146.08 | ▲ 09:30 equity $10,905.61 vs yday $10,769.48 (+136.13) | 09:30 open · cash $146.08 (unchanged overnight, no fees) · equity $10,905.61 vs prior close $10,769.48 (+136.13) because holdings re-marked: ASST×70 yday $21.50 → 09:30 $22.45 +66.50; AU×12 yday $118.11 → 09:30 $117.41 -8.40; RUM×157 yday $9.38 → 09:30 $9.51 +20.41; OMER×78 yday $18.22 → 09:30 $18.24 +1.56; BMNR×59 yday $24.91 → 09:30 $25.91 +59.00; TRLV×134 yday $11.43 → 09:30 $11.38 -6.70; MOS×9 yday $24.16 → 09:30 $24.00 -1.44; DLO×15 yday $15.36 → 09:30 $15.33 -0.45; RRC×5 yday $41.55 → 09:30 $41.44 -0.55; GEN×8 yday $29.64 → 09:30 $29.83 +1.52; SLI×93 yday $2.61 → 09:30 $2.60 -0.93; PLTR×1 yday $177.50 → 09:30 $178.75 +1.25; CRK×17 yday $14.50 → 09:30 $14.42 -1.36; PGY×11 yday $22.41 → 09:30 $22.93 +5.72 | — |
| 2026-08-28 09:30 ET | **SELL** | `ASST` | 70 | $22.45 | $2.22 | $+104.08 | $1,715.35 | ▲ +104.08 after sell → book $10,903.38; vs 09:30 mark -2.23 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `AU` | 12 | $117.41 | $2.05 | $-28.67 | $3,122.22 | ▼ -28.67 after sell → book $10,901.33; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `RUM` | 157 | $9.51 | $2.50 | $+18.59 | $4,612.79 | ▲ +18.59 after sell → book $10,898.83; vs 09:30 mark -2.50 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `OMER` | 78 | $18.24 | $2.25 | $-44.25 | $6,033.27 | ▼ -44.25 after sell → book $10,896.59; vs 09:30 mark -2.24 | dropped from list after 3 sess (min 3) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `BMNR` | 59 | $25.91 | $2.19 | $+65.26 | $7,559.77 | ▲ +65.26 after sell → book $10,894.40; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `FIGR` | 28 | $37.42 | $2.07 | — | $6,509.93 | — | rank by w_hot_candle; rank w_hot_candle; list yday_mover; ret5=+24.4; leftover $1079.97 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 heat🟡 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `VIRT` | 16 | $65.42 | $2.04 | — | $5,461.18 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; ret5=+13.2; leftover $1079.97 | join🟢 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟡 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ZYME` | 36 | $29.33 | $2.10 | — | $4,403.20 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,ohlc_hot; ret5=+14.1; leftover $1079.97 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `NIQ` | 57 | $18.79 | $2.16 | — | $3,330.01 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; ret5=+7.6; leftover $1079.97 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `AMTX` | 577 | $1.87 | $7.44 | — | $2,243.57 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; ret5=+16.9; leftover $1079.97 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `NVAX` | 118 | $9.12 | $2.34 | — | $1,165.07 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; ret5=+11.1; leftover $1079.97 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `WPM` | 6 | $155.89 | $2.01 | — | $227.72 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; ret5=+17.6; leftover $1079.97 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟡 vol🟡 buy🟡 |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $227.72 | ▲ 09:30 equity $10,953.61 vs yday $10,898.11 (+55.50) | 09:30 open · cash $227.72 (unchanged overnight, no fees) · equity $10,953.61 vs prior close $10,898.11 (+55.50) because holdings re-marked: TRLV×134 yday $11.03 → 09:30 $12.41 +184.92; MOS×9 yday $23.76 → 09:30 $23.75 -0.09; DLO×15 yday $15.14 → 09:30 $15.01 -1.95; RRC×5 yday $41.64 → 09:30 $41.11 -2.65; GEN×8 yday $30.50 → 09:30 $31.02 +4.16; SLI×93 yday $2.64 → 09:30 $2.51 -12.09; PLTR×1 yday $185.93 → 09:30 $184.04 -1.89; CRK×17 yday $14.62 → 09:30 $14.56 -1.02; PGY×11 yday $23.26 → 09:30 $21.51 -19.25; FIGR×28 yday $38.02 → 09:30 $35.50 -70.56; VIRT×16 yday $67.04 → 09:30 $66.39 -10.40; ZYME×36 yday $29.01 → 09:30 $28.27 -26.64; NIQ×57 yday $19.07 → 09:30 $19.20 +7.41; AMTX×577 yday $1.87 → 09:30 $1.90 +17.31; NVAX×118 yday $9.05 → 09:30 $9.23 +21.24; WPM×6 yday $157.99 → 09:30 $152.49 -33.00 | — |
| 2026-08-31 09:30 ET | **SELL** | `TRLV` | 134 | $12.41 | $2.43 | $+181.44 | $1,888.23 | ▲ +181.44 after sell → book $10,951.18; vs 09:30 mark -2.43 | dropped from list after 4 sess (min 3) | join🔴 sector🔴 gen🔴 news🟢 digest🟢 judge🟢 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,888.23 | ▲ 09:30 equity $11,009.34 vs yday $10,985.98 (+23.36) | 09:30 open · cash $1,888.23 (unchanged overnight, no fees) · equity $11,009.34 vs prior close $10,985.98 (+23.36) because holdings re-marked: MOS×9 yday $23.78 → 09:30 $24.00 +1.98; DLO×15 yday $15.00 → 09:30 $14.88 -1.80; RRC×5 yday $41.78 → 09:30 $41.32 -2.30; GEN×8 yday $31.02 → 09:30 $30.56 -3.68; SLI×93 yday $2.51 → 09:30 $2.70 +17.67; PLTR×1 yday $183.80 → 09:30 $185.52 +1.72; CRK×17 yday $14.51 → 09:30 $14.31 -3.40; PGY×11 yday $21.95 → 09:30 $21.73 -2.42; FIGR×28 yday $36.41 → 09:30 $36.80 +10.92; VIRT×16 yday $66.39 → 09:30 $65.64 -12.00; ZYME×36 yday $28.27 → 09:30 $29.32 +37.80; NIQ×57 yday $19.20 → 09:30 $19.06 -7.98; AMTX×577 yday $1.90 → 09:30 $1.87 -17.31; NVAX×118 yday $9.26 → 09:30 $9.37 +12.98; WPM×6 yday $152.25 → 09:30 $150.78 -8.82 | — |
| 2026-09-01 09:30 ET | **SELL** | `MOS` | 9 | $24.00 | $2.04 | $-11.61 | $2,102.20 | ▼ -11.61 after sell → book $11,007.31; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `DLO` | 15 | $14.88 | $2.06 | $-14.89 | $2,323.34 | ▼ -14.89 after sell → book $11,005.25; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `RRC` | 5 | $41.32 | $2.02 | $-1.03 | $2,527.92 | ▼ -1.03 after sell → book $11,003.23; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `GEN` | 8 | $30.56 | $2.03 | $+9.31 | $2,770.36 | ▲ +9.31 after sell → book $11,001.19; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `SLI` | 93 | $2.70 | $2.29 | $+5.67 | $3,019.17 | ▲ +5.67 after sell → book $10,998.90; vs 09:30 mark -2.29 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `PLTR` | 1 | $185.52 | $1.88 | $+11.33 | $3,202.81 | ▲ +11.33 after sell → book $10,997.02; vs 09:30 mark -1.88 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `CRK` | 17 | $14.31 | $2.06 | $-0.36 | $3,444.02 | ▼ -0.36 after sell → book $10,994.96; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-01 09:30 ET | **SELL** | `PGY` | 11 | $21.73 | $2.04 | $-6.71 | $3,681.01 | ▼ -6.71 after sell → book $10,992.92; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3,681.01 | ▼ 09:30 equity $10,904.85 vs yday $10,936.56 (-31.71) | 09:30 open · cash $3,681.01 (unchanged overnight, no fees) · equity $10,904.85 vs prior close $10,936.56 (-31.71) because holdings re-marked: FIGR×28 yday $35.70 → 09:30 $35.46 -6.72; VIRT×16 yday $65.64 → 09:30 $65.38 -4.16; ZYME×36 yday $29.33 → 09:30 $29.32 -0.36; NIQ×57 yday $19.06 → 09:30 $19.00 -3.42; AMTX×577 yday $1.87 → 09:30 $1.88 +5.77; NVAX×118 yday $9.37 → 09:30 $9.20 -20.06; WPM×6 yday $146.46 → 09:30 $146.00 -2.76 | — |
| 2026-09-02 09:30 ET | **SELL** | `FIGR` | 28 | $35.46 | $2.09 | $-59.05 | $4,671.79 | ▼ -59.05 after sell → book $10,902.75; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `VIRT` | 16 | $65.38 | $2.06 | $-4.74 | $5,715.81 | ▼ -4.74 after sell → book $10,900.69; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `NIQ` | 57 | $19.00 | $2.18 | $+7.63 | $6,796.63 | ▲ +7.63 after sell → book $10,898.51; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `AMTX` | 577 | $1.88 | $7.55 | $-9.22 | $7,873.84 | ▼ -9.22 after sell → book $10,890.96; vs 09:30 mark -7.55 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `WPM` | 6 | $146.00 | $2.03 | $-63.38 | $8,747.82 | ▼ -63.38 after sell → book $10,888.94; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | join🟢 sector🔴 gen🔴 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,747.82 | ▲ 09:30 equity $11,039.68 vs yday $11,010.10 (+29.58) | 09:30 open · cash $8,747.82 (unchanged overnight, no fees) · equity $11,039.68 vs prior close $11,010.10 (+29.58) because holdings re-marked: ZYME×36 yday $29.67 → 09:30 $30.00 +11.88; NVAX×118 yday $10.12 → 09:30 $10.27 +17.70 | — |
| 2026-09-03 09:30 ET | **SELL** | `ZYME` | 36 | $30.00 | $2.12 | $+19.90 | $9,825.70 | ▲ +19.90 after sell → book $11,037.56; vs 09:30 mark -2.12 | dropped from list after 4 sess (min 3) | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟡 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **SELL** | `NVAX` | 118 | $10.27 | $2.37 | $+130.98 | $11,035.18 | ▲ +130.98 after sell → book $11,035.18; vs 09:30 mark -2.38 | dropped from list after 4 sess (min 3) | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `MRNA` | 9 | $151.40 | $2.02 | — | $9,670.57 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; 🔵; ret5=+129.2; leftover $1379.40 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `XHG` | 386 | $3.57 | $4.98 | — | $8,287.57 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; ret5=+16.1; leftover $1379.40 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 83 | $16.46 | $2.24 | — | $6,919.15 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+63.4; leftover $1379.40 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CAN` | 4597 | $0.30 | $27.58 | — | $5,512.47 | — | rank by w_hot_candle; rank w_hot_candle; list yday_mover; 🔵; ret5=+54.3; leftover $1379.40 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `OMER` | 72 | $18.97 | $2.21 | — | $4,144.42 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; ret5=+12.1; leftover $1379.40 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `TRLV` | 117 | $11.78 | $2.34 | — | $2,763.82 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ret5=+15.0; leftover $1379.40 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `SG` | 214 | $6.43 | $2.76 | — | $1,385.04 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; ret5=+11.3; leftover $1379.40 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `VIRT` | 21 | $65.64 | $2.05 | — | $4.55 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ret5=+13.2; leftover $1379.40 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4.55 | ▲ 09:30 equity $11,111.35 vs yday $10,940.21 (+171.14) | 09:30 open · cash $4.55 (unchanged overnight, no fees) · equity $11,111.35 vs prior close $10,940.21 (+171.14) because holdings re-marked: MRNA×9 yday $150.81 → 09:30 $145.95 -43.74; XHG×386 yday $3.32 → 09:30 $3.38 +23.16; ARCT×83 yday $16.74 → 09:30 $16.77 +2.49; CAN×4597 yday $0.31 → 09:30 $0.34 +137.91; OMER×72 yday $18.86 → 09:30 $18.99 +9.36; TRLV×117 yday $11.69 → 09:30 $11.89 +23.40; SG×214 yday $6.73 → 09:30 $6.75 +4.28; VIRT×21 yday $62.69 → 09:30 $63.37 +14.28 | — |

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
