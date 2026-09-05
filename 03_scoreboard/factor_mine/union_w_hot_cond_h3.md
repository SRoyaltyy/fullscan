# Factor mine action — `union_w_hot_cond_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `w_hot_cond` · size `leftover` · sell `list` · S-boost `none` · rank by w_hot_cond

Cash book **+9.54%** ($10,954) · signal-only (no cash/fees) was +13.41%. Starts YES **11/17**. Fills 100 · skips 152 · realized $+687.10.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `w_hot_cond` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $60.80.

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | vs yday close | Bought | Sold | Close cash | Close equity | Close held | 09:30 why |
|---|---:|---:|---|---:|---:|---|---|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | IREN, TNDM, TPG, INO, HIMS, SLS, VOR, BTSG | — | $107.38 | $10,268.71 | IREN×27, TNDM×53, TPG×24, INO×1543, HIMS×42, SLS×106, VOR×56, BTSG×20 | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-14 | +5.50 | $107.38 | IREN×27, TNDM×53, TPG×24, INO×1543, HIMS×42, SLS×106, VOR×56, BTSG×20 | $10,312.70 | +43.99 | ZENA, AIRO, BZAI | — | $69.59 | $10,511.32 | IREN×27, TNDM×53, TPG×24, INO×1543, HIMS×42, SLS×106, VOR×56, BTSG×20, ZENA×6, AIRO×1, BZAI×17 | 09:30 open · cash $107.38 (unchanged overnight, no fees) · equity $10,312.70 vs prior close $10,268.71 (+43.99) because holdings re-marked: IREN×27 yday $44.76 → 09:30 $44.09 -18.09; TNDM×53 yday $23.13 → 09:30 $22.92 -11.13; TPG×24 yday $54.62 → 09:30 $55.29 +16.08; INO×1543 yday $0.90 → 09:30 $0.93 +46.29; HIMS×42 yday $28.77 → 09:30 $29.15 +15.96; SLS×106 yday $12.36 → 09:30 $12.40 +4.24; VOR×56 yday $23.29 → 09:30 $23.33 +2.24; BTSG×20 yday $60.23 → 09:30 $59.65 -11.60 |
| 2026-08-17 | +2.25 | $69.59 | IREN×27, TNDM×53, TPG×24, INO×1543, HIMS×42, SLS×106, VOR×56, BTSG×20, ZENA×6, AIRO×1, BZAI×17 | $10,483.18 | -28.14 | XHG, CAPR, NPWR | — | $46.41 | $10,583.70 | IREN×27, TNDM×53, TPG×24, INO×1543, HIMS×42, SLS×106, VOR×56, BTSG×20, ZENA×6, AIRO×1, BZAI×17, XHG×2, CAPR×1, NPWR×4 | 09:30 open · cash $69.59 (unchanged overnight, no fees) · equity $10,483.18 vs prior close $10,511.32 (-28.14) because holdings re-marked: IREN×27 yday $44.06 → 09:30 $45.23 +31.59; TNDM×53 yday $22.72 → 09:30 $22.50 -11.66; TPG×24 yday $53.03 → 09:30 $52.67 -8.64; INO×1543 yday $1.09 → 09:30 $1.07 -30.86; HIMS×42 yday $28.15 → 09:30 $28.14 -0.42; SLS×106 yday $12.78 → 09:30 $12.78 +0.00; VOR×56 yday $23.03 → 09:30 $22.91 -6.72; BTSG×20 yday $61.71 → 09:30 $61.69 -0.40; ZENA×6 yday $2.14 → 09:30 $2.08 -0.33; AIRO×1 yday $9.57 → 09:30 $9.57 +0.00; BZAI×17 yday $0.59 → 09:30 $0.55 -0.70 |
| 2026-08-18 | -6.20 | $46.41 | IREN×27, TNDM×53, TPG×24, INO×1543, HIMS×42, SLS×106, VOR×56, BTSG×20, ZENA×6, AIRO×1, BZAI×17, XHG×2, CAPR×1, NPWR×4 | $10,439.30 | -144.40 | — | IREN, TNDM, TPG, INO, HIMS, SLS, VOR, BTSG | $10,352.86 | $10,405.80 | ZENA×6, AIRO×1, BZAI×17, XHG×2, CAPR×1, NPWR×4 | 09:30 open · cash $46.41 (unchanged overnight, no fees) · equity $10,439.30 vs prior close $10,583.70 (-144.40) because holdings re-marked: IREN×27 yday $44.90 → 09:30 $43.56 -36.18; TNDM×53 yday $22.25 → 09:30 $22.16 -5.03; TPG×24 yday $51.77 → 09:30 $51.77 +0.00; INO×1543 yday $1.15 → 09:30 $1.14 -15.43; HIMS×42 yday $28.61 → 09:30 $27.85 -31.92; SLS×106 yday $13.00 → 09:30 $12.66 -36.04; VOR×56 yday $23.01 → 09:30 $22.82 -10.64; BTSG×20 yday $60.38 → 09:30 $60.00 -7.60; ZENA×6 yday $2.05 → 09:30 $1.95 -0.63; AIRO×1 yday $9.41 → 09:30 $9.01 -0.40; BZAI×17 yday $0.52 → 09:30 $0.49 -0.51; XHG×2 yday $3.91 → 09:30 $3.94 +0.06; CAPR×1 yday $7.45 → 09:30 $7.50 +0.05; NPWR×4 yday $1.73 → 09:30 $1.70 -0.12 |
| 2026-08-19 | -7.20 | $10,352.86 | ZENA×6, AIRO×1, BZAI×17, XHG×2, CAPR×1, NPWR×4 | $10,406.34 | +0.54 | — | ZENA, AIRO, BZAI | $10,383.27 | $10,406.59 | XHG×2, CAPR×1, NPWR×4 | 09:30 open · cash $10,352.86 (unchanged overnight, no fees) · equity $10,406.34 vs prior close $10,405.80 (+0.54) because holdings re-marked: ZENA×6 yday $2.04 → 09:30 $2.01 -0.18; AIRO×1 yday $8.98 → 09:30 $9.10 +0.12; BZAI×17 yday $0.56 → 09:30 $0.57 +0.20; XHG×2 yday $4.28 → 09:30 $4.32 +0.08; CAPR×1 yday $7.08 → 09:30 $7.19 +0.11; NPWR×4 yday $1.65 → 09:30 $1.70 +0.20 |
| 2026-08-20 | +1.12 | $10,383.27 | XHG×2, CAPR×1, NPWR×4 | $10,405.69 | -0.90 | MRNA, CYPH, ABCL, SENS, AUTL, TEM, WPM, IAG | XHG, CAPR, NPWR | $228.07 | $10,440.73 | MRNA×8, CYPH×1131, ABCL×110, SENS×145, AUTL×526, TEM×21, WPM×8, IAG×66 | 09:30 open · cash $10,383.27 (unchanged overnight, no fees) · equity $10,405.69 vs prior close $10,406.59 (-0.90) because holdings re-marked: XHG×2 yday $4.33 → 09:30 $4.10 -0.46; CAPR×1 yday $7.98 → 09:30 $7.66 -0.32; NPWR×4 yday $1.67 → 09:30 $1.64 -0.12 |
| 2026-08-21 | +3.25 | $228.07 | MRNA×8, CYPH×1131, ABCL×110, SENS×145, AUTL×526, TEM×21, WPM×8, IAG×66 | $10,710.01 | +269.28 | XHG, ARCT, IOVA, CAPR | — | $50.56 | $11,099.49 | MRNA×8, CYPH×1131, ABCL×110, SENS×145, AUTL×526, TEM×21, WPM×8, IAG×66, XHG×10, ARCT×4, IOVA×5, CAPR×6 | 09:30 open · cash $228.07 (unchanged overnight, no fees) · equity $10,710.01 vs prior close $10,440.73 (+269.28) because holdings re-marked: MRNA×8 yday $133.32 → 09:30 $133.11 -1.68; CYPH×1131 yday $1.19 → 09:30 $1.32 +147.03; ABCL×110 yday $11.57 → 09:30 $11.57 +0.00; SENS×145 yday $8.82 → 09:30 $9.24 +60.90; AUTL×526 yday $2.46 → 09:30 $2.47 +5.26; TEM×21 yday $66.65 → 09:30 $65.60 -22.05; WPM×8 yday $150.25 → 09:30 $154.70 +35.60; IAG×66 yday $20.50 → 09:30 $21.17 +44.22 |
| 2026-08-24 | -5.17 | $50.56 | MRNA×8, CYPH×1131, ABCL×110, SENS×145, AUTL×526, TEM×21, WPM×8, IAG×66, XHG×10, ARCT×4, IOVA×5, CAPR×6 | $11,439.54 | +340.05 | — | — | $50.56 | $11,101.39 | MRNA×8, CYPH×1131, ABCL×110, SENS×145, AUTL×526, TEM×21, WPM×8, IAG×66, XHG×10, ARCT×4, IOVA×5, CAPR×6 | 09:30 open · cash $50.56 (unchanged overnight, no fees) · equity $11,439.54 vs prior close $11,099.49 (+340.05) because holdings re-marked: MRNA×8 yday $145.13 → 09:30 $142.70 -19.44; CYPH×1131 yday $1.42 → 09:30 $1.83 +463.71; ABCL×110 yday $11.32 → 09:30 $10.97 -38.50; SENS×145 yday $9.71 → 09:30 $9.57 -20.30; AUTL×526 yday $2.41 → 09:30 $2.36 -26.30; TEM×21 yday $72.69 → 09:30 $70.07 -55.02; WPM×8 yday $157.78 → 09:30 $158.96 +9.44; IAG×66 yday $21.14 → 09:30 $21.44 +19.80; XHG×10 yday $4.41 → 09:30 $4.24 -1.70; ARCT×4 yday $13.45 → 09:30 $13.26 -0.76; IOVA×5 yday $8.29 → 09:30 $8.05 -1.20; CAPR×6 yday $6.29 → 09:30 $8.01 +10.32 |
| 2026-08-25 | +1.80 | $50.56 | MRNA×8, CYPH×1131, ABCL×110, SENS×145, AUTL×526, TEM×21, WPM×8, IAG×66, XHG×10, ARCT×4, IOVA×5, CAPR×6 | $11,189.83 | +88.44 | AU, ERO, ASST, HMY, FCX | MRNA, ABCL, SENS, AUTL, TEM, IAG | $24.44 | $11,022.46 | CYPH×1131, WPM×8, XHG×10, ARCT×4, IOVA×5, CAPR×6, AU×13, ERO×41, ASST×74, HMY×68, FCX×20 | 09:30 open · cash $50.56 (unchanged overnight, no fees) · equity $11,189.83 vs prior close $11,101.39 (+88.44) because holdings re-marked: MRNA×8 yday $139.27 → 09:30 $141.19 +15.36; CYPH×1131 yday $1.64 → 09:30 $1.70 +67.86; ABCL×110 yday $10.52 → 09:30 $10.77 +27.50; SENS×145 yday $9.73 → 09:30 $9.66 -10.15; AUTL×526 yday $2.38 → 09:30 $2.32 -31.56; TEM×21 yday $67.10 → 09:30 $66.45 -13.65; WPM×8 yday $158.00 → 09:30 $160.00 +16.00; IAG×66 yday $21.36 → 09:30 $21.63 +17.82; XHG×10 yday $4.06 → 09:30 $4.02 -0.40; ARCT×4 yday $13.76 → 09:30 $14.34 +2.32; IOVA×5 yday $8.22 → 09:30 $8.00 -1.10; CAPR×6 yday $7.05 → 09:30 $6.79 -1.56 |
| 2026-08-26 | +2.02 | $24.44 | CYPH×1131, WPM×8, XHG×10, ARCT×4, IOVA×5, CAPR×6, AU×13, ERO×41, ASST×74, HMY×68, FCX×20 | $11,022.46 | +0.00 | — | — | $24.44 | $11,161.22 | CYPH×1131, WPM×8, XHG×10, ARCT×4, IOVA×5, CAPR×6, AU×13, ERO×41, ASST×74, HMY×68, FCX×20 | 09:30 open · cash $24.44 (unchanged overnight, no fees) · equity $11,022.46 vs prior close $11,022.46 (+0.00) because holdings re-marked: CYPH×1131 yday $1.64 → 09:30 $1.64 +0.00; WPM×8 yday $158.25 → 09:30 $158.25 +0.00; XHG×10 yday $4.05 → 09:30 $4.05 +0.00; ARCT×4 yday $14.21 → 09:30 $14.21 +0.00; IOVA×5 yday $8.08 → 09:30 $8.08 +0.00; CAPR×6 yday $7.19 → 09:30 $7.19 +0.00; AU×13 yday $118.55 → 09:30 $118.55 +0.00; ERO×41 yday $38.55 → 09:30 $38.55 +0.00; ASST×74 yday $20.20 → 09:30 $20.20 +0.00; HMY×68 yday $22.50 → 09:30 $22.50 +0.00; FCX×20 yday $77.49 → 09:30 $77.49 +0.00 |
| 2026-08-27 | — | $24.44 | CYPH×1131, WPM×8, XHG×10, ARCT×4, IOVA×5, CAPR×6, AU×13, ERO×41, ASST×74, HMY×68, FCX×20 | $11,173.33 | +12.11 | MOS, SLI, DLO, TX, MRVL, PLTR, MT | CYPH, WPM, XHG, ARCT, IOVA, CAPR | $723.50 | $11,120.98 | AU×13, ERO×41, ASST×74, HMY×68, FCX×20, MOS×16, SLI×158, DLO×26, TX×7, MRVL×1, PLTR×2, MT×5 | 09:30 open · cash $24.44 (unchanged overnight, no fees) · equity $11,173.33 vs prior close $11,161.22 (+12.11) because holdings re-marked: CYPH×1131 yday $1.64 → 09:30 $1.60 -45.24; WPM×8 yday $158.25 → 09:30 $160.93 +21.44; XHG×10 yday $4.05 → 09:30 $3.81 -2.40; ARCT×4 yday $14.21 → 09:30 $15.35 +4.56; IOVA×5 yday $8.08 → 09:30 $8.34 +1.30; CAPR×6 yday $7.19 → 09:30 $8.29 +6.60; AU×13 yday $118.55 → 09:30 $119.80 +16.25; ERO×41 yday $38.55 → 09:30 $40.51 +80.36; ASST×74 yday $20.20 → 09:30 $20.72 +38.48; HMY×68 yday $22.50 → 09:30 $22.39 -7.48; FCX×20 yday $77.49 → 09:30 $79.34 +37.00 |
| 2026-08-28 | +0.75 | $723.50 | AU×13, ERO×41, ASST×74, HMY×68, FCX×20, MOS×16, SLI×158, DLO×26, TX×7, MRVL×1, PLTR×2, MT×5 | $11,066.30 | -54.68 | FIGR, BKKT, QMCO, TIGR, NIQ, VIRT | AU, ASST, HMY | $67.49 | $11,041.03 | ERO×41, FCX×20, MOS×16, SLI×158, DLO×26, TX×7, MRVL×1, PLTR×2, MT×5, FIGR×23, BKKT×104, QMCO×37, TIGR×161, NIQ×47, VIRT×13 | 09:30 open · cash $723.50 (unchanged overnight, no fees) · equity $11,066.30 vs prior close $11,120.98 (-54.68) because holdings re-marked: AU×13 yday $118.11 → 09:30 $117.41 -9.10; ERO×41 yday $39.24 → 09:30 $39.20 -1.64; ASST×74 yday $21.50 → 09:30 $22.45 +70.30; HMY×68 yday $22.43 → 09:30 $20.70 -117.64; FCX×20 yday $79.00 → 09:30 $78.83 -3.40; MOS×16 yday $24.16 → 09:30 $24.00 -2.56; SLI×158 yday $2.61 → 09:30 $2.60 -1.58; DLO×26 yday $15.36 → 09:30 $15.33 -0.78; TX×7 yday $55.13 → 09:30 $55.25 +0.84; MRVL×1 yday $245.11 → 09:30 $253.44 +8.33; PLTR×2 yday $177.50 → 09:30 $178.75 +2.50; MT×5 yday $74.53 → 09:30 $74.54 +0.05 |
| 2026-08-31 | -5.85 | $67.49 | ERO×41, FCX×20, MOS×16, SLI×158, DLO×26, TX×7, MRVL×1, PLTR×2, MT×5, FIGR×23, BKKT×104, QMCO×37, TIGR×161, NIQ×47, VIRT×13 | $10,654.68 | -386.35 | — | ERO, FCX | $3,167.88 | $10,713.66 | MOS×16, SLI×158, DLO×26, TX×7, MRVL×1, PLTR×2, MT×5, FIGR×23, BKKT×104, QMCO×37, TIGR×161, NIQ×47, VIRT×13 | 09:30 open · cash $67.49 (unchanged overnight, no fees) · equity $10,654.68 vs prior close $11,041.03 (-386.35) because holdings re-marked: ERO×41 yday $39.82 → 09:30 $38.60 -50.02; FCX×20 yday $78.42 → 09:30 $76.10 -46.40; MOS×16 yday $23.76 → 09:30 $23.75 -0.16; SLI×158 yday $2.64 → 09:30 $2.51 -20.54; DLO×26 yday $15.14 → 09:30 $15.01 -3.38; TX×7 yday $55.83 → 09:30 $54.84 -6.93; MRVL×1 yday $241.45 → 09:30 $216.69 -24.76; PLTR×2 yday $185.93 → 09:30 $184.04 -3.78; MT×5 yday $74.63 → 09:30 $75.07 +2.20; FIGR×23 yday $38.02 → 09:30 $35.50 -57.96; BKKT×104 yday $8.42 → 09:30 $7.58 -87.36; QMCO×37 yday $23.56 → 09:30 $21.70 -68.82; TIGR×161 yday $5.06 → 09:30 $4.96 -16.10; NIQ×47 yday $19.07 → 09:30 $19.20 +6.11; VIRT×13 yday $67.04 → 09:30 $66.39 -8.45 |
| 2026-09-01 | -6.30 | $3,167.88 | MOS×16, SLI×158, DLO×26, TX×7, MRVL×1, PLTR×2, MT×5, FIGR×23, BKKT×104, QMCO×37, TIGR×161, NIQ×47, VIRT×13 | $10,820.37 | +106.71 | — | MOS, SLI, DLO, TX, MRVL, PLTR, MT | $5,687.53 | $10,720.20 | FIGR×23, BKKT×104, QMCO×37, TIGR×161, NIQ×47, VIRT×13 | 09:30 open · cash $3,167.88 (unchanged overnight, no fees) · equity $10,820.37 vs prior close $10,713.66 (+106.71) because holdings re-marked: MOS×16 yday $23.78 → 09:30 $24.00 +3.52; SLI×158 yday $2.51 → 09:30 $2.70 +30.02; DLO×26 yday $15.00 → 09:30 $14.88 -3.12; TX×7 yday $54.84 → 09:30 $54.82 -0.14; MRVL×1 yday $216.35 → 09:30 $210.57 -5.78; PLTR×2 yday $183.80 → 09:30 $185.52 +3.44; MT×5 yday $75.06 → 09:30 $74.31 -3.75; FIGR×23 yday $36.41 → 09:30 $36.80 +8.97; BKKT×104 yday $7.78 → 09:30 $7.75 -3.12; QMCO×37 yday $22.08 → 09:30 $24.55 +91.39; TIGR×161 yday $5.01 → 09:30 $5.02 +1.61; NIQ×47 yday $19.20 → 09:30 $19.06 -6.58; VIRT×13 yday $66.39 → 09:30 $65.64 -9.75 |
| 2026-09-02 | -3.83 | $5,687.53 | FIGR×23, BKKT×104, QMCO×37, TIGR×161, NIQ×47, VIRT×13 | $10,700.35 | -19.85 | — | FIGR, BKKT, QMCO, TIGR, NIQ, VIRT | $10,687.11 | $10,687.11 | — | 09:30 open · cash $5,687.53 (unchanged overnight, no fees) · equity $10,700.35 vs prior close $10,720.20 (-19.85) because holdings re-marked: FIGR×23 yday $35.70 → 09:30 $35.46 -5.52; BKKT×104 yday $7.53 → 09:30 $7.42 -11.44; QMCO×37 yday $23.63 → 09:30 $23.85 +8.14; TIGR×161 yday $5.00 → 09:30 $4.97 -4.83; NIQ×47 yday $19.06 → 09:30 $19.00 -2.82; VIRT×13 yday $65.64 → 09:30 $65.38 -3.38 |
| 2026-09-03 | -0.90 | $10,687.11 | — | $10,687.11 | -0.00 | MRNA, ARCT, XHG, CAN, NVAX, INO, RVTY, ZYME | — | $167.57 | $10,723.55 | MRNA×8, ARCT×81, XHG×374, CAN×4452, NVAX×130, INO×996, RVTY×10, ZYME×44 | 09:30 open · cash $10,687.11 · no holdings · equity $10,687.11 vs prior close $10,687.11 (-0.00). Cash unchanged overnight; no fees. |
| 2026-09-04 | — | $167.57 | MRNA×8, ARCT×81, XHG×374, CAN×4452, NVAX×130, INO×996, RVTY×10, ZYME×44 | $10,892.62 | +169.07 | OABI, TRLV, ALEC, OMER | — | $60.80 | $10,953.59 | MRNA×8, ARCT×81, XHG×374, CAN×4452, NVAX×130, INO×996, RVTY×10, ZYME×44, OABI×6, TRLV×2, ALEC×12, OMER×1 | 09:30 open · cash $167.57 (unchanged overnight, no fees) · equity $10,892.62 vs prior close $10,723.55 (+169.07) because holdings re-marked: MRNA×8 yday $150.81 → 09:30 $145.95 -38.88; ARCT×81 yday $16.74 → 09:30 $16.77 +2.43; XHG×374 yday $3.32 → 09:30 $3.38 +22.44; CAN×4452 yday $0.31 → 09:30 $0.34 +133.56; NVAX×130 yday $10.32 → 09:30 $10.41 +11.70; INO×996 yday $1.36 → 09:30 $1.37 +9.96; RVTY×10 yday $130.94 → 09:30 $132.45 +15.10; ZYME×44 yday $31.05 → 09:30 $31.34 +12.76 |

## Fills (09:30 open snapshot, then buys / sells)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 27 | $45.98 | $2.07 | — | $8,756.47 | — | rank by w_hot_cond; rank w_hot_cond; list flatten; ⚪; ret5=+12.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 53 | $23.33 | $2.15 | — | $7,517.83 | — | rank by w_hot_cond; rank w_hot_cond; list flatten; ⚪; ret5=+19.7; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 24 | $50.62 | $2.06 | — | $6,300.81 | — | rank by w_hot_cond; rank w_hot_cond; list flatten; ⚪; ret5=+6.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 1543 | $0.81 | $17.13 | — | $5,033.85 | — | rank by w_hot_cond; rank w_hot_cond; list flatten; ⚪; ret5=+13.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 42 | $29.74 | $2.12 | — | $3,782.66 | — | rank by w_hot_cond; rank w_hot_cond; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 106 | $11.70 | $2.31 | — | $2,540.15 | — | rank by w_hot_cond; rank w_hot_cond; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `VOR` | 56 | $22.01 | $2.16 | — | $1,305.43 | — | rank by w_hot_cond; rank w_hot_cond; list flatten; ⚪; ret5=+0.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 20 | $59.80 | $2.05 | — | $107.38 | — | rank by w_hot_cond; rank w_hot_cond; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $107.38 | ▲ 09:30 equity $10,312.70 vs yday $10,268.71 (+43.99) | 09:30 open · cash $107.38 (unchanged overnight, no fees) · equity $10,312.70 vs prior close $10,268.71 (+43.99) because holdings re-marked: IREN×27 yday $44.76 → 09:30 $44.09 -18.09; TNDM×53 yday $23.13 → 09:30 $22.92 -11.13; TPG×24 yday $54.62 → 09:30 $55.29 +16.08; INO×1543 yday $0.90 → 09:30 $0.93 +46.29; HIMS×42 yday $28.77 → 09:30 $29.15 +15.96; SLS×106 yday $12.36 → 09:30 $12.40 +4.24; VOR×56 yday $23.29 → 09:30 $23.33 +2.24; BTSG×20 yday $60.23 → 09:30 $59.65 -11.60 | — |
| 2026-08-14 09:30 ET | **BUY** | `ZENA` | 6 | $2.20 | $0.15 | — | $94.03 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer; 🔵; ⚪; ret5=+54.3; leftover $13.42 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `AIRO` | 1 | $11.12 | $0.11 | — | $82.80 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; leftover $13.42 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BZAI` | 17 | $0.77 | $0.18 | — | $69.59 | — | rank by w_hot_cond; rank w_hot_cond; list earn_react; 🔵; ⚪; ret5=+20.4; leftover $13.42 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $69.59 | ▼ 09:30 equity $10,483.18 vs yday $10,511.32 (-28.14) | 09:30 open · cash $69.59 (unchanged overnight, no fees) · equity $10,483.18 vs prior close $10,511.32 (-28.14) because holdings re-marked: IREN×27 yday $44.06 → 09:30 $45.23 +31.59; TNDM×53 yday $22.72 → 09:30 $22.50 -11.66; TPG×24 yday $53.03 → 09:30 $52.67 -8.64; INO×1543 yday $1.09 → 09:30 $1.07 -30.86; HIMS×42 yday $28.15 → 09:30 $28.14 -0.42; SLS×106 yday $12.78 → 09:30 $12.78 +0.00; VOR×56 yday $23.03 → 09:30 $22.91 -6.72; BTSG×20 yday $61.71 → 09:30 $61.69 -0.40; ZENA×6 yday $2.14 → 09:30 $2.08 -0.33; AIRO×1 yday $9.57 → 09:30 $9.57 +0.00; BZAI×17 yday $0.59 → 09:30 $0.55 -0.70 | — |
| 2026-08-17 09:30 ET | **BUY** | `XHG` | 2 | $4.19 | $0.09 | — | $61.12 | — | rank by w_hot_cond; rank w_hot_cond; list yday_mover; ⚪; ret5=+291.8; leftover $8.70 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `CAPR` | 1 | $6.87 | $0.07 | — | $54.18 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; ret5=+62.6; leftover $8.70 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `NPWR` | 4 | $1.92 | $0.09 | — | $46.41 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; ⚪; ret5=+28.3; leftover $8.70 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $46.41 | ▼ 09:30 equity $10,439.30 vs yday $10,583.70 (-144.40) | 09:30 open · cash $46.41 (unchanged overnight, no fees) · equity $10,439.30 vs prior close $10,583.70 (-144.40) because holdings re-marked: IREN×27 yday $44.90 → 09:30 $43.56 -36.18; TNDM×53 yday $22.25 → 09:30 $22.16 -5.03; TPG×24 yday $51.77 → 09:30 $51.77 +0.00; INO×1543 yday $1.15 → 09:30 $1.14 -15.43; HIMS×42 yday $28.61 → 09:30 $27.85 -31.92; SLS×106 yday $13.00 → 09:30 $12.66 -36.04; VOR×56 yday $23.01 → 09:30 $22.82 -10.64; BTSG×20 yday $60.38 → 09:30 $60.00 -7.60; ZENA×6 yday $2.05 → 09:30 $1.95 -0.63; AIRO×1 yday $9.41 → 09:30 $9.01 -0.40; BZAI×17 yday $0.52 → 09:30 $0.49 -0.51; XHG×2 yday $3.91 → 09:30 $3.94 +0.06; CAPR×1 yday $7.45 → 09:30 $7.50 +0.05; NPWR×4 yday $1.73 → 09:30 $1.70 -0.12 | — |
| 2026-08-18 09:30 ET | **SELL** | `IREN` | 27 | $43.56 | $2.09 | $-69.50 | $1,220.44 | ▼ -69.50 after sell → book $10,437.21; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TNDM` | 53 | $22.16 | $2.17 | $-66.33 | $2,392.75 | ▼ -66.33 after sell → book $10,435.04; vs 09:30 mark -2.17 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TPG` | 24 | $51.77 | $2.08 | $+23.38 | $3,633.15 | ▲ +23.38 after sell → book $10,432.96; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `INO` | 1543 | $1.14 | $20.17 | $+471.89 | $5,372.00 | ▲ +471.89 after sell → book $10,412.78; vs 09:30 mark -20.18 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `HIMS` | 42 | $27.85 | $2.14 | $-83.63 | $6,539.56 | ▼ -83.63 after sell → book $10,410.65; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `SLS` | 106 | $12.66 | $2.34 | $+97.12 | $7,879.19 | ▲ +97.12 after sell → book $10,408.31; vs 09:30 mark -2.34 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `VOR` | 56 | $22.82 | $2.18 | $+41.02 | $9,154.93 | ▲ +41.02 after sell → book $10,406.13; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `BTSG` | 20 | $60.00 | $2.07 | $-0.12 | $10,352.86 | ▼ -0.12 after sell → book $10,404.06; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,352.86 | ▲ 09:30 equity $10,406.34 vs yday $10,405.80 (+0.54) | 09:30 open · cash $10,352.86 (unchanged overnight, no fees) · equity $10,406.34 vs prior close $10,405.80 (+0.54) because holdings re-marked: ZENA×6 yday $2.04 → 09:30 $2.01 -0.18; AIRO×1 yday $8.98 → 09:30 $9.10 +0.12; BZAI×17 yday $0.56 → 09:30 $0.57 +0.20; XHG×2 yday $4.28 → 09:30 $4.32 +0.08; CAPR×1 yday $7.08 → 09:30 $7.19 +0.11; NPWR×4 yday $1.65 → 09:30 $1.70 +0.20 | — |
| 2026-08-19 09:30 ET | **SELL** | `ZENA` | 6 | $2.01 | $0.16 | $-1.45 | $10,364.76 | ▼ -1.45 after sell → book $10,406.18; vs 09:30 mark -0.16 | dropped from list after 3 sess (min 3) | join🔴 sector🟡 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-19 09:30 ET | **SELL** | `AIRO` | 1 | $9.10 | $0.11 | $-2.25 | $10,373.74 | ▼ -2.25 after sell → book $10,406.06; vs 09:30 mark -0.12 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `BZAI` | 17 | $0.57 | $0.17 | $-3.68 | $10,383.27 | ▼ -3.68 after sell → book $10,405.90; vs 09:30 mark -0.16 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,383.27 | ▼ 09:30 equity $10,405.69 vs yday $10,406.59 (-0.90) | 09:30 open · cash $10,383.27 (unchanged overnight, no fees) · equity $10,405.69 vs prior close $10,406.59 (-0.90) because holdings re-marked: XHG×2 yday $4.33 → 09:30 $4.10 -0.46; CAPR×1 yday $7.98 → 09:30 $7.66 -0.32; NPWR×4 yday $1.67 → 09:30 $1.64 -0.12 | — |
| 2026-08-20 09:30 ET | **SELL** | `XHG` | 2 | $4.10 | $0.11 | $-0.38 | $10,391.36 | ▼ -0.38 after sell → book $10,405.58; vs 09:30 mark -0.11 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `CAPR` | 1 | $7.66 | $0.10 | $+0.62 | $10,398.92 | ▲ +0.62 after sell → book $10,405.48; vs 09:30 mark -0.10 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `NPWR` | 4 | $1.64 | $0.10 | $-1.31 | $10,405.38 | ▼ -1.31 after sell → book $10,405.38; vs 09:30 mark -0.10 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 8 | $150.14 | $2.01 | — | $9,202.25 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ret5=+173.9; leftover $1300.67 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CYPH` | 1131 | $1.15 | $14.59 | — | $7,887.01 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+62.6; leftover $1300.67 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ABCL` | 110 | $11.81 | $2.32 | — | $6,585.04 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $1300.67 | join🟢 sector🟡 gen🟢 news🔴 digest🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `SENS` | 145 | $8.91 | $2.42 | — | $5,290.66 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; 🔵; ⚪; ret5=+14.9; leftover $1300.67 | join🟡 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AUTL` | 526 | $2.47 | $6.79 | — | $3,984.66 | — | rank by w_hot_cond; rank w_hot_cond; list mover_buy; 🔵; ⚪; ret5=+19.8; leftover $1300.67 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `TEM` | 21 | $61.83 | $2.05 | — | $2,684.17 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+12.2; leftover $1300.67 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $1,525.84 | — | rank by w_hot_cond; rank w_hot_cond; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1300.67 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 66 | $19.63 | $2.19 | — | $228.07 | — | rank by w_hot_cond; rank w_hot_cond; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1300.67 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $228.07 | ▲ 09:30 equity $10,710.01 vs yday $10,440.73 (+269.28) | 09:30 open · cash $228.07 (unchanged overnight, no fees) · equity $10,710.01 vs prior close $10,440.73 (+269.28) because holdings re-marked: MRNA×8 yday $133.32 → 09:30 $133.11 -1.68; CYPH×1131 yday $1.19 → 09:30 $1.32 +147.03; ABCL×110 yday $11.57 → 09:30 $11.57 +0.00; SENS×145 yday $8.82 → 09:30 $9.24 +60.90; AUTL×526 yday $2.46 → 09:30 $2.47 +5.26; TEM×21 yday $66.65 → 09:30 $65.60 -22.05; WPM×8 yday $150.25 → 09:30 $154.70 +35.60; IAG×66 yday $20.50 → 09:30 $21.17 +44.22 | — |
| 2026-08-21 09:30 ET | **BUY** | `XHG` | 10 | $4.49 | $0.48 | — | $182.69 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; 🔵; ret5=+12.7; leftover $45.61 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🔴 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 4 | $11.13 | $0.46 | — | $137.72 | — | rank by w_hot_cond; rank w_hot_cond; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $45.61 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `IOVA` | 5 | $9.08 | $0.47 | — | $91.85 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer; 🔵; ⚪; ret5=+37.9; leftover $45.61 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CAPR` | 6 | $6.81 | $0.43 | — | $50.56 | — | rank by w_hot_cond; rank w_hot_cond; list yday_mover; 🔵; ret5=+62.5; leftover $45.61 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $50.56 | ▲ 09:30 equity $11,439.54 vs yday $11,099.49 (+340.05) | 09:30 open · cash $50.56 (unchanged overnight, no fees) · equity $11,439.54 vs prior close $11,099.49 (+340.05) because holdings re-marked: MRNA×8 yday $145.13 → 09:30 $142.70 -19.44; CYPH×1131 yday $1.42 → 09:30 $1.83 +463.71; ABCL×110 yday $11.32 → 09:30 $10.97 -38.50; SENS×145 yday $9.71 → 09:30 $9.57 -20.30; AUTL×526 yday $2.41 → 09:30 $2.36 -26.30; TEM×21 yday $72.69 → 09:30 $70.07 -55.02; WPM×8 yday $157.78 → 09:30 $158.96 +9.44; IAG×66 yday $21.14 → 09:30 $21.44 +19.80; XHG×10 yday $4.41 → 09:30 $4.24 -1.70; ARCT×4 yday $13.45 → 09:30 $13.26 -0.76; IOVA×5 yday $8.29 → 09:30 $8.05 -1.20; CAPR×6 yday $6.29 → 09:30 $8.01 +10.32 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $50.56 | ▲ 09:30 equity $11,189.83 vs yday $11,101.39 (+88.44) | 09:30 open · cash $50.56 (unchanged overnight, no fees) · equity $11,189.83 vs prior close $11,101.39 (+88.44) because holdings re-marked: MRNA×8 yday $139.27 → 09:30 $141.19 +15.36; CYPH×1131 yday $1.64 → 09:30 $1.70 +67.86; ABCL×110 yday $10.52 → 09:30 $10.77 +27.50; SENS×145 yday $9.73 → 09:30 $9.66 -10.15; AUTL×526 yday $2.38 → 09:30 $2.32 -31.56; TEM×21 yday $67.10 → 09:30 $66.45 -13.65; WPM×8 yday $158.00 → 09:30 $160.00 +16.00; IAG×66 yday $21.36 → 09:30 $21.63 +17.82; XHG×10 yday $4.06 → 09:30 $4.02 -0.40; ARCT×4 yday $13.76 → 09:30 $14.34 +2.32; IOVA×5 yday $8.22 → 09:30 $8.00 -1.10; CAPR×6 yday $7.05 → 09:30 $6.79 -1.56 | — |
| 2026-08-25 09:30 ET | **SELL** | `MRNA` | 8 | $141.19 | $2.03 | $-75.65 | $1,178.05 | ▼ -75.65 after sell → book $11,187.80; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ABCL` | 110 | $10.77 | $2.35 | $-119.62 | $2,360.40 | ▼ -119.62 after sell → book $11,185.45; vs 09:30 mark -2.35 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `SENS` | 145 | $9.66 | $2.46 | $+103.86 | $3,758.64 | ▲ +103.86 after sell → book $11,182.99; vs 09:30 mark -2.46 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `AUTL` | 526 | $2.32 | $6.88 | $-92.57 | $4,972.07 | ▼ -92.57 after sell → book $11,176.10; vs 09:30 mark -6.89 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `TEM` | 21 | $66.45 | $2.07 | $+92.89 | $6,365.45 | ▲ +92.89 after sell → book $11,174.03; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `IAG` | 66 | $21.63 | $2.21 | $+127.60 | $7,790.82 | ▲ +127.60 after sell → book $11,171.82; vs 09:30 mark -2.21 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 13 | $119.46 | $2.03 | — | $6,235.81 | — | rank by w_hot_cond; rank w_hot_cond; list mover_buy; 🔵; ⚪; ret5=+25.9; leftover $1558.16 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ERO` | 41 | $38.00 | $2.11 | — | $4,675.70 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot,mover_buy; ⚪; ret5=+16.6; leftover $1558.16 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `ASST` | 74 | $20.90 | $2.21 | — | $3,126.89 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer; 🔵; ret5=+47.9; leftover $1558.16 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `HMY` | 68 | $22.65 | $2.19 | — | $1,584.49 | — | rank by w_hot_cond; rank w_hot_cond; list mover_buy; ⚪; ret5=+21.1; leftover $1558.16 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 20 | $77.90 | $2.05 | — | $24.44 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot,mover_buy; ⚪; ret5=+15.3; leftover $1558.16 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $24.44 | ▲ 09:30 equity $11,022.46 vs yday $11,022.46 (+0.00) | 09:30 open · cash $24.44 (unchanged overnight, no fees) · equity $11,022.46 vs prior close $11,022.46 (+0.00) because holdings re-marked: CYPH×1131 yday $1.64 → 09:30 $1.64 +0.00; WPM×8 yday $158.25 → 09:30 $158.25 +0.00; XHG×10 yday $4.05 → 09:30 $4.05 +0.00; ARCT×4 yday $14.21 → 09:30 $14.21 +0.00; IOVA×5 yday $8.08 → 09:30 $8.08 +0.00; CAPR×6 yday $7.19 → 09:30 $7.19 +0.00; AU×13 yday $118.55 → 09:30 $118.55 +0.00; ERO×41 yday $38.55 → 09:30 $38.55 +0.00; ASST×74 yday $20.20 → 09:30 $20.20 +0.00; HMY×68 yday $22.50 → 09:30 $22.50 +0.00; FCX×20 yday $77.49 → 09:30 $77.49 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $24.44 | ▲ 09:30 equity $11,173.33 vs yday $11,161.22 (+12.11) | 09:30 open · cash $24.44 (unchanged overnight, no fees) · equity $11,173.33 vs prior close $11,161.22 (+12.11) because holdings re-marked: CYPH×1131 yday $1.64 → 09:30 $1.60 -45.24; WPM×8 yday $158.25 → 09:30 $160.93 +21.44; XHG×10 yday $4.05 → 09:30 $3.81 -2.40; ARCT×4 yday $14.21 → 09:30 $15.35 +4.56; IOVA×5 yday $8.08 → 09:30 $8.34 +1.30; CAPR×6 yday $7.19 → 09:30 $8.29 +6.60; AU×13 yday $118.55 → 09:30 $119.80 +16.25; ERO×41 yday $38.55 → 09:30 $40.51 +80.36; ASST×74 yday $20.20 → 09:30 $20.72 +38.48; HMY×68 yday $22.50 → 09:30 $22.39 -7.48; FCX×20 yday $77.49 → 09:30 $79.34 +37.00 | — |
| 2026-08-27 09:30 ET | **SELL** | `CYPH` | 1131 | $1.60 | $14.79 | $+479.57 | $1,819.25 | ▲ +479.57 after sell → book $11,158.54; vs 09:30 mark -14.79 | dropped from list after 5 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `WPM` | 8 | $160.93 | $2.03 | $+127.07 | $3,104.66 | ▲ +127.07 after sell → book $11,156.51; vs 09:30 mark -2.03 | dropped from list after 5 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `XHG` | 10 | $3.81 | $0.43 | $-7.71 | $3,142.32 | ▼ -7.71 after sell → book $11,156.07; vs 09:30 mark -0.44 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `ARCT` | 4 | $15.35 | $0.65 | $+15.78 | $3,203.08 | ▲ +15.78 after sell → book $11,155.43; vs 09:30 mark -0.64 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `IOVA` | 5 | $8.34 | $0.45 | $-4.62 | $3,244.33 | ▼ -4.62 after sell → book $11,154.98; vs 09:30 mark -0.45 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `CAPR` | 6 | $8.29 | $0.54 | $+7.92 | $3,293.53 | ▲ +7.92 after sell → book $11,154.44; vs 09:30 mark -0.54 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **BUY** | `MOS` | 16 | $24.84 | $2.04 | — | $2,894.05 | — | rank by w_hot_cond; rank w_hot_cond; list flatten; ret5=+13.0; leftover $411.69 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟢 |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 158 | $2.59 | $2.46 | — | $2,482.37 | — | rank by w_hot_cond; rank w_hot_cond; list flatten; ret5=+4.2; leftover $411.69 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `DLO` | 26 | $15.60 | $2.07 | — | $2,074.70 | — | rank by w_hot_cond; rank w_hot_cond; list mover_buy; 🔵; ret5=+7.1; leftover $411.69 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `TX` | 7 | $55.20 | $2.01 | — | $1,686.29 | — | rank by w_hot_cond; rank w_hot_cond; list mover_buy; 🔵; ret5=+3.0; leftover $411.69 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MRVL` | 1 | $240.00 | $1.99 | — | $1,444.30 | — | rank by w_hot_cond; rank w_hot_cond; list mover_buy; 🔵; ret5=+6.8; leftover $411.69 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `PLTR` | 2 | $170.60 | $2.00 | — | $1,101.10 | — | rank by w_hot_cond; rank w_hot_cond; list mover_buy; 🔵; ret5=+3.4; leftover $411.69 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MT` | 5 | $75.12 | $2.00 | — | $723.50 | — | rank by w_hot_cond; rank w_hot_cond; list mover_buy; 🔵; ret5=-2.2; leftover $411.69 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $723.50 | ▼ 09:30 equity $11,066.30 vs yday $11,120.98 (-54.68) | 09:30 open · cash $723.50 (unchanged overnight, no fees) · equity $11,066.30 vs prior close $11,120.98 (-54.68) because holdings re-marked: AU×13 yday $118.11 → 09:30 $117.41 -9.10; ERO×41 yday $39.24 → 09:30 $39.20 -1.64; ASST×74 yday $21.50 → 09:30 $22.45 +70.30; HMY×68 yday $22.43 → 09:30 $20.70 -117.64; FCX×20 yday $79.00 → 09:30 $78.83 -3.40; MOS×16 yday $24.16 → 09:30 $24.00 -2.56; SLI×158 yday $2.61 → 09:30 $2.60 -1.58; DLO×26 yday $15.36 → 09:30 $15.33 -0.78; TX×7 yday $55.13 → 09:30 $55.25 +0.84; MRVL×1 yday $245.11 → 09:30 $253.44 +8.33; PLTR×2 yday $177.50 → 09:30 $178.75 +2.50; MT×5 yday $74.53 → 09:30 $74.54 +0.05 | — |
| 2026-08-28 09:30 ET | **SELL** | `AU` | 13 | $117.41 | $2.05 | $-30.73 | $2,247.78 | ▼ -30.73 after sell → book $11,064.25; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ASST` | 74 | $22.45 | $2.24 | $+110.25 | $3,906.84 | ▲ +110.25 after sell → book $11,062.01; vs 09:30 mark -2.24 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `HMY` | 68 | $20.70 | $2.22 | $-137.01 | $5,312.22 | ▼ -137.01 after sell → book $11,059.79; vs 09:30 mark -2.22 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `FIGR` | 23 | $37.42 | $2.06 | — | $4,449.50 | — | rank by w_hot_cond; rank w_hot_cond; list yday_mover; ret5=+24.4; leftover $885.37 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 heat🟡 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `BKKT` | 104 | $8.50 | $2.30 | — | $3,563.20 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; ret5=+12.3; leftover $885.37 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `QMCO` | 37 | $23.50 | $2.10 | — | $2,691.60 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; 🔵; ret5=-14.8; leftover $885.37 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟡 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `TIGR` | 161 | $5.49 | $2.47 | — | $1,805.24 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; ret5=+15.9; leftover $885.37 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟡 vol🟢 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `NIQ` | 47 | $18.79 | $2.13 | — | $919.98 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; ret5=+7.6; leftover $885.37 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `VIRT` | 13 | $65.42 | $2.03 | — | $67.49 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; ret5=+13.2; leftover $885.37 | join🟢 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟡 vol🟡 buy🟡 |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $67.49 | ▼ 09:30 equity $10,654.68 vs yday $11,041.03 (-386.35) | 09:30 open · cash $67.49 (unchanged overnight, no fees) · equity $10,654.68 vs prior close $11,041.03 (-386.35) because holdings re-marked: ERO×41 yday $39.82 → 09:30 $38.60 -50.02; FCX×20 yday $78.42 → 09:30 $76.10 -46.40; MOS×16 yday $23.76 → 09:30 $23.75 -0.16; SLI×158 yday $2.64 → 09:30 $2.51 -20.54; DLO×26 yday $15.14 → 09:30 $15.01 -3.38; TX×7 yday $55.83 → 09:30 $54.84 -6.93; MRVL×1 yday $241.45 → 09:30 $216.69 -24.76; PLTR×2 yday $185.93 → 09:30 $184.04 -3.78; MT×5 yday $74.63 → 09:30 $75.07 +2.20; FIGR×23 yday $38.02 → 09:30 $35.50 -57.96; BKKT×104 yday $8.42 → 09:30 $7.58 -87.36; QMCO×37 yday $23.56 → 09:30 $21.70 -68.82; TIGR×161 yday $5.06 → 09:30 $4.96 -16.10; NIQ×47 yday $19.07 → 09:30 $19.20 +6.11; VIRT×13 yday $67.04 → 09:30 $66.39 -8.45 | — |
| 2026-08-31 09:30 ET | **SELL** | `ERO` | 41 | $38.60 | $2.14 | $+20.35 | $1,647.95 | ▲ +20.35 after sell → book $10,652.54; vs 09:30 mark -2.14 | dropped from list after 4 sess (min 3) | join🟢 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-31 09:30 ET | **SELL** | `FCX` | 20 | $76.10 | $2.07 | $-40.12 | $3,167.88 | ▼ -40.12 after sell → book $10,650.47; vs 09:30 mark -2.07 | dropped from list after 4 sess (min 3) | join🟢 sector🔴 gen🔴 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3,167.88 | ▲ 09:30 equity $10,820.37 vs yday $10,713.66 (+106.71) | 09:30 open · cash $3,167.88 (unchanged overnight, no fees) · equity $10,820.37 vs prior close $10,713.66 (+106.71) because holdings re-marked: MOS×16 yday $23.78 → 09:30 $24.00 +3.52; SLI×158 yday $2.51 → 09:30 $2.70 +30.02; DLO×26 yday $15.00 → 09:30 $14.88 -3.12; TX×7 yday $54.84 → 09:30 $54.82 -0.14; MRVL×1 yday $216.35 → 09:30 $210.57 -5.78; PLTR×2 yday $183.80 → 09:30 $185.52 +3.44; MT×5 yday $75.06 → 09:30 $74.31 -3.75; FIGR×23 yday $36.41 → 09:30 $36.80 +8.97; BKKT×104 yday $7.78 → 09:30 $7.75 -3.12; QMCO×37 yday $22.08 → 09:30 $24.55 +91.39; TIGR×161 yday $5.01 → 09:30 $5.02 +1.61; NIQ×47 yday $19.20 → 09:30 $19.06 -6.58; VIRT×13 yday $66.39 → 09:30 $65.64 -9.75 | — |
| 2026-09-01 09:30 ET | **SELL** | `MOS` | 16 | $24.00 | $2.06 | $-17.54 | $3,549.82 | ▼ -17.54 after sell → book $10,818.31; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `SLI` | 158 | $2.70 | $2.50 | $+12.42 | $3,973.92 | ▲ +12.42 after sell → book $10,815.81; vs 09:30 mark -2.50 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `DLO` | 26 | $14.88 | $2.09 | $-22.88 | $4,358.71 | ▼ -22.88 after sell → book $10,813.72; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `TX` | 7 | $54.82 | $2.03 | $-6.70 | $4,740.42 | ▼ -6.70 after sell → book $10,811.69; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `MRVL` | 1 | $210.57 | $2.01 | $-33.44 | $4,948.98 | ▼ -33.44 after sell → book $10,809.68; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `PLTR` | 2 | $185.52 | $2.02 | $+25.83 | $5,318.00 | ▲ +25.83 after sell → book $10,807.66; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `MT` | 5 | $74.31 | $2.02 | $-8.08 | $5,687.53 | ▼ -8.08 after sell → book $10,805.64; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,687.53 | ▼ 09:30 equity $10,700.35 vs yday $10,720.20 (-19.85) | 09:30 open · cash $5,687.53 (unchanged overnight, no fees) · equity $10,700.35 vs prior close $10,720.20 (-19.85) because holdings re-marked: FIGR×23 yday $35.70 → 09:30 $35.46 -5.52; BKKT×104 yday $7.53 → 09:30 $7.42 -11.44; QMCO×37 yday $23.63 → 09:30 $23.85 +8.14; TIGR×161 yday $5.00 → 09:30 $4.97 -4.83; NIQ×47 yday $19.06 → 09:30 $19.00 -2.82; VIRT×13 yday $65.64 → 09:30 $65.38 -3.38 | — |
| 2026-09-02 09:30 ET | **SELL** | `FIGR` | 23 | $35.46 | $2.08 | $-49.22 | $6,501.03 | ▼ -49.22 after sell → book $10,698.27; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `BKKT` | 104 | $7.42 | $2.33 | $-116.95 | $7,270.38 | ▼ -116.95 after sell → book $10,695.94; vs 09:30 mark -2.33 | dropped from list after 3 sess (min 3) | join🔴 sector🟢 gen🔴 news🟡 digest🟢 ab🔴 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-02 09:30 ET | **SELL** | `QMCO` | 37 | $23.85 | $2.12 | $+8.73 | $8,150.71 | ▲ +8.73 after sell → book $10,693.82; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 3) | join🔴 sector🟢 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-09-02 09:30 ET | **SELL** | `TIGR` | 161 | $4.97 | $2.51 | $-88.70 | $8,948.37 | ▼ -88.70 after sell → book $10,691.31; vs 09:30 mark -2.51 | dropped from list after 3 sess (min 3) | join🟢 sector🔴 gen🔴 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-02 09:30 ET | **SELL** | `NIQ` | 47 | $19.00 | $2.15 | $+5.59 | $9,839.22 | ▲ +5.59 after sell → book $10,689.16; vs 09:30 mark -2.15 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `VIRT` | 13 | $65.38 | $2.05 | $-4.60 | $10,687.11 | ▼ -4.60 after sell → book $10,687.11; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,687.11 | ▲ 09:30 equity $10,687.11 vs yday $10,687.11 (-0.00) | 09:30 open · cash $10,687.11 · no holdings · equity $10,687.11 vs prior close $10,687.11 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `MRNA` | 8 | $151.40 | $2.01 | — | $9,473.89 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ret5=+129.2; leftover $1335.89 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 81 | $16.46 | $2.23 | — | $8,138.40 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+63.4; leftover $1335.89 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `XHG` | 374 | $3.57 | $4.82 | — | $6,798.40 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; ret5=+16.1; leftover $1335.89 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CAN` | 4452 | $0.30 | $26.71 | — | $5,436.08 | — | rank by w_hot_cond; rank w_hot_cond; list yday_mover; 🔵; ret5=+54.3; leftover $1335.89 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `NVAX` | 130 | $10.27 | $2.38 | — | $4,098.60 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+11.1; leftover $1335.89 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `INO` | 996 | $1.34 | $12.85 | — | $2,751.12 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; 🔵; ⚪; ret5=+8.3; leftover $1335.89 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 10 | $125.94 | $2.02 | — | $1,489.70 | — | rank by w_hot_cond; rank w_hot_cond; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $1335.89 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ZYME` | 44 | $30.00 | $2.12 | — | $167.57 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; ⚪; ret5=+14.1; leftover $1335.89 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟡 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $167.57 | ▲ 09:30 equity $10,892.62 vs yday $10,723.55 (+169.07) | 09:30 open · cash $167.57 (unchanged overnight, no fees) · equity $10,892.62 vs prior close $10,723.55 (+169.07) because holdings re-marked: MRNA×8 yday $150.81 → 09:30 $145.95 -38.88; ARCT×81 yday $16.74 → 09:30 $16.77 +2.43; XHG×374 yday $3.32 → 09:30 $3.38 +22.44; CAN×4452 yday $0.31 → 09:30 $0.34 +133.56; NVAX×130 yday $10.32 → 09:30 $10.41 +11.70; INO×996 yday $1.36 → 09:30 $1.37 +9.96; RVTY×10 yday $130.94 → 09:30 $132.45 +15.10; ZYME×44 yday $31.05 → 09:30 $31.34 +12.76 | — |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 6 | $5.08 | $0.32 | — | $136.77 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+28.1; leftover $33.51 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `TRLV` | 2 | $11.89 | $0.24 | — | $112.75 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; 🔵; ⚪; ret5=+15.0; leftover $33.51 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 12 | $2.70 | $0.36 | — | $79.99 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.4; leftover $33.51 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OMER` | 1 | $18.99 | $0.19 | — | $60.80 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; 🔵; ret5=+12.1; leftover $33.51 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |

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
| 2026-08-31 | `FIGR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `BKKT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `QMCO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `NIQ` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `VIRT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `USDE` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CYPH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `XHG` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PURR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CAN` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CVI` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `DK` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `FIGR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `BKKT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `QMCO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `TIGR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `NIQ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `VIRT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `CYPH` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NVAX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `DK` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `OMER` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `VFF` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `OBE` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `INO` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `TRGP` | hard_red | hard-red S=-6.30 sit; no new buys |
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
| 2026-09-04 | `ATRC` | cash | leftover split 33.51 < 1 share @ 52.88 |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `MRNA` | 8 | 2026-09-03 @ $151.40 | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ret5=+129.2; leftover $1335.89 |
| `ARCT` | 81 | 2026-09-03 @ $16.46 | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+63.4; leftover $1335.89 |
| `XHG` | 374 | 2026-09-03 @ $3.57 | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; ret5=+16.1; leftover $1335.89 |
| `CAN` | 4452 | 2026-09-03 @ $0.30 | rank by w_hot_cond; rank w_hot_cond; list yday_mover; 🔵; ret5=+54.3; leftover $1335.89 |
| `NVAX` | 130 | 2026-09-03 @ $10.27 | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+11.1; leftover $1335.89 |
| `INO` | 996 | 2026-09-03 @ $1.34 | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; 🔵; ⚪; ret5=+8.3; leftover $1335.89 |
| `RVTY` | 10 | 2026-09-03 @ $125.94 | rank by w_hot_cond; rank w_hot_cond; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $1335.89 |
| `ZYME` | 44 | 2026-09-03 @ $30.00 | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; ⚪; ret5=+14.1; leftover $1335.89 |
| `OABI` | 6 | 2026-09-04 @ $5.08 | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+28.1; leftover $33.51 |
| `TRLV` | 2 | 2026-09-04 @ $11.89 | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; 🔵; ⚪; ret5=+15.0; leftover $33.51 |
| `ALEC` | 12 | 2026-09-04 @ $2.70 | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.4; leftover $33.51 |
| `OMER` | 1 | 2026-09-04 @ $18.99 | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; 🔵; ret5=+12.1; leftover $33.51 |
