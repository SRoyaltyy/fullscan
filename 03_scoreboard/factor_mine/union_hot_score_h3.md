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

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | vs yday close | Bought | Sold | Close cash | Close equity | Close held | 09:30 why |
|---|---:|---:|---|---:|---:|---|---|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | IREN, TNDM, TPG, INO, HIMS, SLS, VOR, BTSG | — | $107.38 | $10,268.71 | IREN×27, TNDM×53, TPG×24, INO×1543, HIMS×42, SLS×106, VOR×56, BTSG×20 | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-14 | +5.50 | $107.38 | IREN×27, TNDM×53, TPG×24, INO×1543, HIMS×42, SLS×106, VOR×56, BTSG×20 | $10,312.70 | +43.99 | ZENA, AIRO, BZAI | — | $69.59 | $10,511.32 | IREN×27, TNDM×53, TPG×24, INO×1543, HIMS×42, SLS×106, VOR×56, BTSG×20, ZENA×6, AIRO×1, BZAI×17 | 09:30 open · cash $107.38 (unchanged overnight, no fees) · equity $10,312.70 vs prior close $10,268.71 (+43.99) because holdings re-marked: IREN×27 yday $44.76 → 09:30 $44.09 -18.09; TNDM×53 yday $23.13 → 09:30 $22.92 -11.13; TPG×24 yday $54.62 → 09:30 $55.29 +16.08; INO×1543 yday $0.90 → 09:30 $0.93 +46.29; HIMS×42 yday $28.77 → 09:30 $29.15 +15.96; SLS×106 yday $12.36 → 09:30 $12.40 +4.24; VOR×56 yday $23.29 → 09:30 $23.33 +2.24; BTSG×20 yday $60.23 → 09:30 $59.65 -11.60 |
| 2026-08-17 | +2.25 | $69.59 | IREN×27, TNDM×53, TPG×24, INO×1543, HIMS×42, SLS×106, VOR×56, BTSG×20, ZENA×6, AIRO×1, BZAI×17 | $10,483.18 | -28.14 | XHG, CAPR, NPWR | — | $46.41 | $10,583.70 | IREN×27, TNDM×53, TPG×24, INO×1543, HIMS×42, SLS×106, VOR×56, BTSG×20, ZENA×6, AIRO×1, BZAI×17, XHG×2, CAPR×1, NPWR×4 | 09:30 open · cash $69.59 (unchanged overnight, no fees) · equity $10,483.18 vs prior close $10,511.32 (-28.14) because holdings re-marked: IREN×27 yday $44.06 → 09:30 $45.23 +31.59; TNDM×53 yday $22.72 → 09:30 $22.50 -11.66; TPG×24 yday $53.03 → 09:30 $52.67 -8.64; INO×1543 yday $1.09 → 09:30 $1.07 -30.86; HIMS×42 yday $28.15 → 09:30 $28.14 -0.42; SLS×106 yday $12.78 → 09:30 $12.78 +0.00; VOR×56 yday $23.03 → 09:30 $22.91 -6.72; BTSG×20 yday $61.71 → 09:30 $61.69 -0.40; ZENA×6 yday $2.14 → 09:30 $2.08 -0.33; AIRO×1 yday $9.57 → 09:30 $9.57 +0.00; BZAI×17 yday $0.59 → 09:30 $0.55 -0.70 |
| 2026-08-18 | -6.20 | $46.41 | IREN×27, TNDM×53, TPG×24, INO×1543, HIMS×42, SLS×106, VOR×56, BTSG×20, ZENA×6, AIRO×1, BZAI×17, XHG×2, CAPR×1, NPWR×4 | $10,439.30 | -144.40 | — | IREN, TNDM, TPG, INO, HIMS, SLS, VOR, BTSG | $10,352.86 | $10,405.80 | ZENA×6, AIRO×1, BZAI×17, XHG×2, CAPR×1, NPWR×4 | 09:30 open · cash $46.41 (unchanged overnight, no fees) · equity $10,439.30 vs prior close $10,583.70 (-144.40) because holdings re-marked: IREN×27 yday $44.90 → 09:30 $43.56 -36.18; TNDM×53 yday $22.25 → 09:30 $22.16 -5.03; TPG×24 yday $51.77 → 09:30 $51.77 +0.00; INO×1543 yday $1.15 → 09:30 $1.14 -15.43; HIMS×42 yday $28.61 → 09:30 $27.85 -31.92; SLS×106 yday $13.00 → 09:30 $12.66 -36.04; VOR×56 yday $23.01 → 09:30 $22.82 -10.64; BTSG×20 yday $60.38 → 09:30 $60.00 -7.60; ZENA×6 yday $2.05 → 09:30 $1.95 -0.63; AIRO×1 yday $9.41 → 09:30 $9.01 -0.40; BZAI×17 yday $0.52 → 09:30 $0.49 -0.51; XHG×2 yday $3.91 → 09:30 $3.94 +0.06; CAPR×1 yday $7.45 → 09:30 $7.50 +0.05; NPWR×4 yday $1.73 → 09:30 $1.70 -0.12 |
| 2026-08-19 | -7.20 | $10,352.86 | ZENA×6, AIRO×1, BZAI×17, XHG×2, CAPR×1, NPWR×4 | $10,406.34 | +0.54 | — | ZENA, AIRO, BZAI | $10,383.27 | $10,406.59 | XHG×2, CAPR×1, NPWR×4 | 09:30 open · cash $10,352.86 (unchanged overnight, no fees) · equity $10,406.34 vs prior close $10,405.80 (+0.54) because holdings re-marked: ZENA×6 yday $2.04 → 09:30 $2.01 -0.18; AIRO×1 yday $8.98 → 09:30 $9.10 +0.12; BZAI×17 yday $0.56 → 09:30 $0.57 +0.20; XHG×2 yday $4.28 → 09:30 $4.32 +0.08; CAPR×1 yday $7.08 → 09:30 $7.19 +0.11; NPWR×4 yday $1.65 → 09:30 $1.70 +0.20 |
| 2026-08-20 | +1.12 | $10,383.27 | XHG×2, CAPR×1, NPWR×4 | $10,405.69 | -0.90 | MRNA, CYPH, ABCL, AZI, SENS, ALEC, BTGO, AUTL | XHG, CAPR, NPWR | $69.75 | $10,210.58 | MRNA×8, CYPH×1131, ABCL×110, AZI×949, SENS×145, ALEC×541, BTGO×196, AUTL×526 | 09:30 open · cash $10,383.27 (unchanged overnight, no fees) · equity $10,405.69 vs prior close $10,406.59 (-0.90) because holdings re-marked: XHG×2 yday $4.33 → 09:30 $4.10 -0.46; CAPR×1 yday $7.98 → 09:30 $7.66 -0.32; NPWR×4 yday $1.67 → 09:30 $1.64 -0.12 |
| 2026-08-21 | +3.25 | $69.75 | MRNA×8, CYPH×1131, ABCL×110, AZI×949, SENS×145, ALEC×541, BTGO×196, AUTL×526 | $10,520.49 | +309.91 | XHG, CAPR, ARCT, IOVA, CAN | — | $21.68 | $10,753.69 | MRNA×8, CYPH×1131, ABCL×110, AZI×949, SENS×145, ALEC×541, BTGO×196, AUTL×526, XHG×2, CAPR×1, ARCT×1, IOVA×1, CAN×39 | 09:30 open · cash $69.75 (unchanged overnight, no fees) · equity $10,520.49 vs prior close $10,210.58 (+309.91) because holdings re-marked: MRNA×8 yday $133.32 → 09:30 $133.11 -1.68; CYPH×1131 yday $1.19 → 09:30 $1.32 +147.03; ABCL×110 yday $11.57 → 09:30 $11.57 +0.00; AZI×949 yday $1.44 → 09:30 $1.46 +18.98; SENS×145 yday $8.82 → 09:30 $9.24 +60.90; ALEC×541 yday $2.26 → 09:30 $2.28 +10.82; BTGO×196 yday $6.60 → 09:30 $6.95 +68.60; AUTL×526 yday $2.46 → 09:30 $2.47 +5.26 |
| 2026-08-24 | -5.17 | $21.68 | MRNA×8, CYPH×1131, ABCL×110, AZI×949, SENS×145, ALEC×541, BTGO×196, AUTL×526, XHG×2, CAPR×1, ARCT×1, IOVA×1, CAN×39 | $11,130.16 | +376.47 | — | — | $21.68 | $10,844.49 | MRNA×8, CYPH×1131, ABCL×110, AZI×949, SENS×145, ALEC×541, BTGO×196, AUTL×526, XHG×2, CAPR×1, ARCT×1, IOVA×1, CAN×39 | 09:30 open · cash $21.68 (unchanged overnight, no fees) · equity $11,130.16 vs prior close $10,753.69 (+376.47) because holdings re-marked: MRNA×8 yday $145.13 → 09:30 $142.70 -19.44; CYPH×1131 yday $1.42 → 09:30 $1.83 +463.71; ABCL×110 yday $11.32 → 09:30 $10.97 -38.50; AZI×949 yday $1.45 → 09:30 $1.46 +9.49; SENS×145 yday $9.71 → 09:30 $9.57 -20.30; ALEC×541 yday $2.36 → 09:30 $2.36 +0.00; BTGO×196 yday $6.84 → 09:30 $6.87 +5.88; AUTL×526 yday $2.41 → 09:30 $2.36 -26.30; XHG×2 yday $4.41 → 09:30 $4.24 -0.34; CAPR×1 yday $6.29 → 09:30 $8.01 +1.72; ARCT×1 yday $13.45 → 09:30 $13.26 -0.19; IOVA×1 yday $8.29 → 09:30 $8.05 -0.24; CAN×39 yday $0.35 → 09:30 $0.38 +0.98 |
| 2026-08-25 | +1.80 | $21.68 | MRNA×8, CYPH×1131, ABCL×110, AZI×949, SENS×145, ALEC×541, BTGO×196, AUTL×526, XHG×2, CAPR×1, ARCT×1, IOVA×1, CAN×39 | $10,788.52 | -55.97 | ASST, AU, RUM, BMNR, NIQ, DEFT | MRNA, ABCL, AZI, SENS, ALEC, BTGO, AUTL | $40.76 | $10,507.35 | CYPH×1131, XHG×2, CAPR×1, ARCT×1, IOVA×1, CAN×39, ASST×69, AU×12, RUM×156, BMNR×59, NIQ×74, DEFT×2285 | 09:30 open · cash $21.68 (unchanged overnight, no fees) · equity $10,788.52 vs prior close $10,844.49 (-55.97) because holdings re-marked: MRNA×8 yday $139.27 → 09:30 $141.19 +15.36; CYPH×1131 yday $1.64 → 09:30 $1.70 +67.86; ABCL×110 yday $10.52 → 09:30 $10.77 +27.50; AZI×949 yday $1.40 → 09:30 $1.33 -66.43; SENS×145 yday $9.73 → 09:30 $9.66 -10.15; ALEC×541 yday $2.38 → 09:30 $2.30 -43.28; BTGO×196 yday $6.97 → 09:30 $6.89 -15.68; AUTL×526 yday $2.38 → 09:30 $2.32 -31.56; XHG×2 yday $4.06 → 09:30 $4.02 -0.08; CAPR×1 yday $7.05 → 09:30 $6.79 -0.26; ARCT×1 yday $13.76 → 09:30 $14.34 +0.58; IOVA×1 yday $8.22 → 09:30 $8.00 -0.22; CAN×39 yday $0.37 → 09:30 $0.38 +0.39 |
| 2026-08-26 | +2.02 | $40.76 | CYPH×1131, XHG×2, CAPR×1, ARCT×1, IOVA×1, CAN×39, ASST×69, AU×12, RUM×156, BMNR×59, NIQ×74, DEFT×2285 | $10,507.35 | +0.00 | — | — | $40.76 | $10,720.14 | CYPH×1131, XHG×2, CAPR×1, ARCT×1, IOVA×1, CAN×39, ASST×69, AU×12, RUM×156, BMNR×59, NIQ×74, DEFT×2285 | 09:30 open · cash $40.76 (unchanged overnight, no fees) · equity $10,507.35 vs prior close $10,507.35 (+0.00) because holdings re-marked: CYPH×1131 yday $1.64 → 09:30 $1.64 +0.00; XHG×2 yday $4.05 → 09:30 $4.05 +0.00; CAPR×1 yday $7.19 → 09:30 $7.19 +0.00; ARCT×1 yday $14.21 → 09:30 $14.21 +0.00; IOVA×1 yday $8.08 → 09:30 $8.08 +0.00; CAN×39 yday $0.36 → 09:30 $0.36 +0.00; ASST×69 yday $20.20 → 09:30 $20.20 +0.00; AU×12 yday $118.55 → 09:30 $118.55 +0.00; RUM×156 yday $9.35 → 09:30 $9.35 +0.00; BMNR×59 yday $24.21 → 09:30 $24.21 +0.00; NIQ×74 yday $19.46 → 09:30 $19.46 +0.00; DEFT×2285 yday $0.62 → 09:30 $0.62 +0.00 |
| 2026-08-27 | — | $40.76 | CYPH×1131, XHG×2, CAPR×1, ARCT×1, IOVA×1, CAN×39, ASST×69, AU×12, RUM×156, BMNR×59, NIQ×74, DEFT×2285 | $10,565.72 | -154.42 | MOS, DLO, SLI, CRK, PLTR, RRC, GEN | CYPH, XHG, CAPR, ARCT, IOVA, CAN | $351.89 | $10,460.31 | ASST×69, AU×12, RUM×156, BMNR×59, NIQ×74, DEFT×2285, MOS×9, DLO×15, SLI×91, CRK×16, PLTR×1, RRC×5, GEN×8 | 09:30 open · cash $40.76 (unchanged overnight, no fees) · equity $10,565.72 vs prior close $10,720.14 (-154.42) because holdings re-marked: CYPH×1131 yday $1.64 → 09:30 $1.60 -45.24; XHG×2 yday $4.05 → 09:30 $3.81 -0.48; CAPR×1 yday $7.19 → 09:30 $8.29 +1.10; ARCT×1 yday $14.21 → 09:30 $15.35 +1.14; IOVA×1 yday $8.08 → 09:30 $8.34 +0.26; CAN×39 yday $0.36 → 09:30 $0.40 +1.56; ASST×69 yday $20.20 → 09:30 $20.72 +35.88; AU×12 yday $118.55 → 09:30 $119.80 +15.00; RUM×156 yday $9.35 → 09:30 $10.07 +112.32; BMNR×59 yday $24.21 → 09:30 $24.24 +1.77; NIQ×74 yday $19.46 → 09:30 $19.20 -19.24; DEFT×2285 yday $0.62 → 09:30 $0.60 -45.70 |
| 2026-08-28 | +0.75 | $351.89 | ASST×69, AU×12, RUM×156, BMNR×59, NIQ×74, DEFT×2285, MOS×9, DLO×15, SLI×91, CRK×16, PLTR×1, RRC×5, GEN×8 | $10,621.43 | +161.12 | FIGR, ERO, TRLV, CVI, VIRT, TXG, GUTS | ASST, AU, RUM, BMNR, DEFT | $87.23 | $10,631.31 | NIQ×74, MOS×9, DLO×15, SLI×91, CRK×16, PLTR×1, RRC×5, GEN×8, FIGR×29, ERO×27, TRLV×96, CVI×27, VIRT×16, TXG×17, GUTS×1479 | 09:30 open · cash $351.89 (unchanged overnight, no fees) · equity $10,621.43 vs prior close $10,460.31 (+161.12) because holdings re-marked: ASST×69 yday $21.50 → 09:30 $22.45 +65.55; AU×12 yday $118.11 → 09:30 $117.41 -8.40; RUM×156 yday $9.38 → 09:30 $9.51 +20.28; BMNR×59 yday $24.91 → 09:30 $25.91 +59.00; NIQ×74 yday $18.74 → 09:30 $18.79 +3.70; DEFT×2285 yday $0.59 → 09:30 $0.60 +22.85; MOS×9 yday $24.16 → 09:30 $24.00 -1.44; DLO×15 yday $15.36 → 09:30 $15.33 -0.45; SLI×91 yday $2.61 → 09:30 $2.60 -0.91; CRK×16 yday $14.50 → 09:30 $14.42 -1.28; PLTR×1 yday $177.50 → 09:30 $178.75 +1.25; RRC×5 yday $41.55 → 09:30 $41.44 -0.55; GEN×8 yday $29.64 → 09:30 $29.83 +1.52 |
| 2026-08-31 | -5.85 | $87.23 | NIQ×74, MOS×9, DLO×15, SLI×91, CRK×16, PLTR×1, RRC×5, GEN×8, FIGR×29, ERO×27, TRLV×96, CVI×27, VIRT×16, TXG×17, GUTS×1479 | $10,525.10 | -106.21 | — | — | $87.23 | $10,559.45 | NIQ×74, MOS×9, DLO×15, SLI×91, CRK×16, PLTR×1, RRC×5, GEN×8, FIGR×29, ERO×27, TRLV×96, CVI×27, VIRT×16, TXG×17, GUTS×1479 | 09:30 open · cash $87.23 (unchanged overnight, no fees) · equity $10,525.10 vs prior close $10,631.31 (-106.21) because holdings re-marked: NIQ×74 yday $19.07 → 09:30 $19.20 +9.62; MOS×9 yday $23.76 → 09:30 $23.75 -0.09; DLO×15 yday $15.14 → 09:30 $15.01 -1.95; SLI×91 yday $2.64 → 09:30 $2.51 -11.83; CRK×16 yday $14.62 → 09:30 $14.56 -0.96; PLTR×1 yday $185.93 → 09:30 $184.04 -1.89; RRC×5 yday $41.64 → 09:30 $41.11 -2.65; GEN×8 yday $30.50 → 09:30 $31.02 +4.16; FIGR×29 yday $38.02 → 09:30 $35.50 -73.08; ERO×27 yday $39.82 → 09:30 $38.60 -32.94; TRLV×96 yday $11.03 → 09:30 $12.41 +132.48; CVI×27 yday $39.76 → 09:30 $41.76 +54.00; VIRT×16 yday $67.04 → 09:30 $66.39 -10.40; TXG×17 yday $64.85 → 09:30 $60.90 -67.15; GUTS×1479 yday $0.74 → 09:30 $0.67 -103.53 |
| 2026-09-01 | -6.30 | $87.23 | NIQ×74, MOS×9, DLO×15, SLI×91, CRK×16, PLTR×1, RRC×5, GEN×8, FIGR×29, ERO×27, TRLV×96, CVI×27, VIRT×16, TXG×17, GUTS×1479 | $10,695.78 | +136.33 | — | NIQ, MOS, DLO, SLI, CRK, PLTR, RRC, GEN | $3,031.52 | $10,507.72 | FIGR×29, ERO×27, TRLV×96, CVI×27, VIRT×16, TXG×17, GUTS×1479 | 09:30 open · cash $87.23 (unchanged overnight, no fees) · equity $10,695.78 vs prior close $10,559.45 (+136.33) because holdings re-marked: NIQ×74 yday $19.20 → 09:30 $19.06 -10.36; MOS×9 yday $23.78 → 09:30 $24.00 +1.98; DLO×15 yday $15.00 → 09:30 $14.88 -1.80; SLI×91 yday $2.51 → 09:30 $2.70 +17.29; CRK×16 yday $14.51 → 09:30 $14.31 -3.20; PLTR×1 yday $183.80 → 09:30 $185.52 +1.72; RRC×5 yday $41.78 → 09:30 $41.32 -2.30; GEN×8 yday $31.02 → 09:30 $30.56 -3.68; FIGR×29 yday $36.41 → 09:30 $36.80 +11.31; ERO×27 yday $38.49 → 09:30 $37.30 -32.13; TRLV×96 yday $12.41 → 09:30 $11.89 -49.92; CVI×27 yday $41.76 → 09:30 $42.86 +29.70; VIRT×16 yday $66.39 → 09:30 $65.64 -12.00; TXG×17 yday $61.40 → 09:30 $62.99 +27.03; GUTS×1479 yday $0.67 → 09:30 $0.78 +162.69 |
| 2026-09-02 | -3.83 | $3,031.52 | FIGR×29, ERO×27, TRLV×96, CVI×27, VIRT×16, TXG×17, GUTS×1479 | $10,414.75 | -92.97 | — | FIGR, ERO, CVI, VIRT | $7,227.63 | $10,424.42 | TRLV×96, TXG×17, GUTS×1479 | 09:30 open · cash $3,031.52 (unchanged overnight, no fees) · equity $10,414.75 vs prior close $10,507.72 (-92.97) because holdings re-marked: FIGR×29 yday $35.70 → 09:30 $35.46 -6.96; ERO×27 yday $36.01 → 09:30 $35.95 -1.62; TRLV×96 yday $11.89 → 09:30 $11.54 -33.60; CVI×27 yday $42.86 → 09:30 $42.94 +2.16; VIRT×16 yday $65.64 → 09:30 $65.38 -4.16; TXG×17 yday $62.92 → 09:30 $61.79 -19.21; GUTS×1479 yday $0.71 → 09:30 $0.69 -29.58 |
| 2026-09-03 | -0.90 | $7,227.63 | TRLV×96, TXG×17, GUTS×1479 | $10,462.26 | +37.84 | MRNA, XHG, ARCT, CAN, NIQ, DEFT, OMER, ERO | TRLV, TXG, GUTS | $82.63 | $10,256.72 | MRNA×8, XHG×365, ARCT×79, CAN×4351, NIQ×70, DEFT×1948, OMER×68, ERO×36 | 09:30 open · cash $7,227.63 (unchanged overnight, no fees) · equity $10,462.26 vs prior close $10,424.42 (+37.84) because holdings re-marked: TRLV×96 yday $11.74 → 09:30 $11.78 +3.84; TXG×17 yday $59.98 → 09:30 $60.24 +4.42; GUTS×1479 yday $0.71 → 09:30 $0.73 +29.58 |
| 2026-09-04 | — | $82.63 | MRNA×8, XHG×365, ARCT×79, CAN×4351, NIQ×70, DEFT×1948, OMER×68, ERO×36 | $10,441.34 | +184.62 | HQ, OABI, TRLV | — | $15.71 | $10,661.31 | MRNA×8, XHG×365, ARCT×79, CAN×4351, NIQ×70, DEFT×1948, OMER×68, ERO×36, HQ×1, OABI×5, TRLV×2 | 09:30 open · cash $82.63 (unchanged overnight, no fees) · equity $10,441.34 vs prior close $10,256.72 (+184.62) because holdings re-marked: MRNA×8 yday $150.81 → 09:30 $145.95 -38.88; XHG×365 yday $3.32 → 09:30 $3.38 +21.90; ARCT×79 yday $16.74 → 09:30 $16.77 +2.37; CAN×4351 yday $0.31 → 09:30 $0.34 +130.53; NIQ×70 yday $18.35 → 09:30 $18.66 +21.70; DEFT×1948 yday $0.65 → 09:30 $0.65 +0.00; OMER×68 yday $18.86 → 09:30 $18.99 +8.84; ERO×36 yday $34.76 → 09:30 $35.82 +38.16 |

## Fills (09:30 open snapshot, then buys / sells)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 27 | $45.98 | $2.07 | — | $8,756.47 | — | rank by hot_score; rank hot_score; list flatten; ⚪; ret5=+12.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 53 | $23.33 | $2.15 | — | $7,517.83 | — | rank by hot_score; rank hot_score; list flatten; ⚪; ret5=+19.7; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 24 | $50.62 | $2.06 | — | $6,300.81 | — | rank by hot_score; rank hot_score; list flatten; ⚪; ret5=+6.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 1543 | $0.81 | $17.13 | — | $5,033.85 | — | rank by hot_score; rank hot_score; list flatten; ⚪; ret5=+13.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 42 | $29.74 | $2.12 | — | $3,782.66 | — | rank by hot_score; rank hot_score; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 106 | $11.70 | $2.31 | — | $2,540.15 | — | rank by hot_score; rank hot_score; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `VOR` | 56 | $22.01 | $2.16 | — | $1,305.43 | — | rank by hot_score; rank hot_score; list flatten; ⚪; ret5=+0.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 20 | $59.80 | $2.05 | — | $107.38 | — | rank by hot_score; rank hot_score; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $107.38 | ▲ 09:30 equity $10,312.70 vs yday $10,268.71 (+43.99) | 09:30 open · cash $107.38 (unchanged overnight, no fees) · equity $10,312.70 vs prior close $10,268.71 (+43.99) because holdings re-marked: IREN×27 yday $44.76 → 09:30 $44.09 -18.09; TNDM×53 yday $23.13 → 09:30 $22.92 -11.13; TPG×24 yday $54.62 → 09:30 $55.29 +16.08; INO×1543 yday $0.90 → 09:30 $0.93 +46.29; HIMS×42 yday $28.77 → 09:30 $29.15 +15.96; SLS×106 yday $12.36 → 09:30 $12.40 +4.24; VOR×56 yday $23.29 → 09:30 $23.33 +2.24; BTSG×20 yday $60.23 → 09:30 $59.65 -11.60 | — |
| 2026-08-14 09:30 ET | **BUY** | `ZENA` | 6 | $2.20 | $0.15 | — | $94.03 | — | rank by hot_score; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+54.3; leftover $13.42 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `AIRO` | 1 | $11.12 | $0.11 | — | $82.80 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; leftover $13.42 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BZAI` | 17 | $0.77 | $0.18 | — | $69.59 | — | rank by hot_score; rank hot_score; list earn_react; 🔵; ⚪; ret5=+20.4; leftover $13.42 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $69.59 | ▼ 09:30 equity $10,483.18 vs yday $10,511.32 (-28.14) | 09:30 open · cash $69.59 (unchanged overnight, no fees) · equity $10,483.18 vs prior close $10,511.32 (-28.14) because holdings re-marked: IREN×27 yday $44.06 → 09:30 $45.23 +31.59; TNDM×53 yday $22.72 → 09:30 $22.50 -11.66; TPG×24 yday $53.03 → 09:30 $52.67 -8.64; INO×1543 yday $1.09 → 09:30 $1.07 -30.86; HIMS×42 yday $28.15 → 09:30 $28.14 -0.42; SLS×106 yday $12.78 → 09:30 $12.78 +0.00; VOR×56 yday $23.03 → 09:30 $22.91 -6.72; BTSG×20 yday $61.71 → 09:30 $61.69 -0.40; ZENA×6 yday $2.14 → 09:30 $2.08 -0.33; AIRO×1 yday $9.57 → 09:30 $9.57 +0.00; BZAI×17 yday $0.59 → 09:30 $0.55 -0.70 | — |
| 2026-08-17 09:30 ET | **BUY** | `XHG` | 2 | $4.19 | $0.09 | — | $61.12 | — | rank by hot_score; rank hot_score; list yday_mover; ⚪; ret5=+291.8; leftover $8.70 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `CAPR` | 1 | $6.87 | $0.07 | — | $54.18 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; ret5=+62.6; leftover $8.70 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `NPWR` | 4 | $1.92 | $0.09 | — | $46.41 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; ⚪; ret5=+28.3; leftover $8.70 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
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
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 8 | $150.14 | $2.01 | — | $9,202.25 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+173.9; leftover $1300.67 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CYPH` | 1131 | $1.15 | $14.59 | — | $7,887.01 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+62.6; leftover $1300.67 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ABCL` | 110 | $11.81 | $2.32 | — | $6,585.04 | — | rank by hot_score; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $1300.67 | join🟢 sector🟡 gen🟢 news🔴 digest🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AZI` | 949 | $1.37 | $12.24 | — | $5,272.67 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+30.7; leftover $1300.67 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `SENS` | 145 | $8.91 | $2.42 | — | $3,978.29 | — | rank by hot_score; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+14.9; leftover $1300.67 | join🟡 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ALEC` | 541 | $2.40 | $6.98 | — | $2,672.91 | — | rank by hot_score; rank hot_score; list ohlc_hot; 🔵; ret5=+13.0; leftover $1300.67 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BTGO` | 196 | $6.61 | $2.58 | — | $1,375.75 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+25.3; leftover $1300.67 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟡 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AUTL` | 526 | $2.47 | $6.79 | — | $69.75 | — | rank by hot_score; rank hot_score; list mover_buy; 🔵; ⚪; ret5=+19.8; leftover $1300.67 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $69.75 | ▲ 09:30 equity $10,520.49 vs yday $10,210.58 (+309.91) | 09:30 open · cash $69.75 (unchanged overnight, no fees) · equity $10,520.49 vs prior close $10,210.58 (+309.91) because holdings re-marked: MRNA×8 yday $133.32 → 09:30 $133.11 -1.68; CYPH×1131 yday $1.19 → 09:30 $1.32 +147.03; ABCL×110 yday $11.57 → 09:30 $11.57 +0.00; AZI×949 yday $1.44 → 09:30 $1.46 +18.98; SENS×145 yday $8.82 → 09:30 $9.24 +60.90; ALEC×541 yday $2.26 → 09:30 $2.28 +10.82; BTGO×196 yday $6.60 → 09:30 $6.95 +68.60; AUTL×526 yday $2.46 → 09:30 $2.47 +5.26 | — |
| 2026-08-21 09:30 ET | **BUY** | `XHG` | 2 | $4.49 | $0.10 | — | $60.67 | — | rank by hot_score; rank hot_score; list ohlc_hot; 🔵; ret5=+12.7; leftover $11.62 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🔴 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CAPR` | 1 | $6.81 | $0.07 | — | $53.79 | — | rank by hot_score; rank hot_score; list yday_mover; 🔵; ret5=+62.5; leftover $11.62 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 1 | $11.13 | $0.11 | — | $42.55 | — | rank by hot_score; rank hot_score; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $11.62 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `IOVA` | 1 | $9.08 | $0.09 | — | $33.37 | — | rank by hot_score; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+37.9; leftover $11.62 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CAN` | 39 | $0.29 | $0.23 | — | $21.68 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+30.4; leftover $11.62 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $21.68 | ▲ 09:30 equity $11,130.16 vs yday $10,753.69 (+376.47) | 09:30 open · cash $21.68 (unchanged overnight, no fees) · equity $11,130.16 vs prior close $10,753.69 (+376.47) because holdings re-marked: MRNA×8 yday $145.13 → 09:30 $142.70 -19.44; CYPH×1131 yday $1.42 → 09:30 $1.83 +463.71; ABCL×110 yday $11.32 → 09:30 $10.97 -38.50; AZI×949 yday $1.45 → 09:30 $1.46 +9.49; SENS×145 yday $9.71 → 09:30 $9.57 -20.30; ALEC×541 yday $2.36 → 09:30 $2.36 +0.00; BTGO×196 yday $6.84 → 09:30 $6.87 +5.88; AUTL×526 yday $2.41 → 09:30 $2.36 -26.30; XHG×2 yday $4.41 → 09:30 $4.24 -0.34; CAPR×1 yday $6.29 → 09:30 $8.01 +1.72; ARCT×1 yday $13.45 → 09:30 $13.26 -0.19; IOVA×1 yday $8.29 → 09:30 $8.05 -0.24; CAN×39 yday $0.35 → 09:30 $0.38 +0.98 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $21.68 | ▼ 09:30 equity $10,788.52 vs yday $10,844.49 (-55.97) | 09:30 open · cash $21.68 (unchanged overnight, no fees) · equity $10,788.52 vs prior close $10,844.49 (-55.97) because holdings re-marked: MRNA×8 yday $139.27 → 09:30 $141.19 +15.36; CYPH×1131 yday $1.64 → 09:30 $1.70 +67.86; ABCL×110 yday $10.52 → 09:30 $10.77 +27.50; AZI×949 yday $1.40 → 09:30 $1.33 -66.43; SENS×145 yday $9.73 → 09:30 $9.66 -10.15; ALEC×541 yday $2.38 → 09:30 $2.30 -43.28; BTGO×196 yday $6.97 → 09:30 $6.89 -15.68; AUTL×526 yday $2.38 → 09:30 $2.32 -31.56; XHG×2 yday $4.06 → 09:30 $4.02 -0.08; CAPR×1 yday $7.05 → 09:30 $6.79 -0.26; ARCT×1 yday $13.76 → 09:30 $14.34 +0.58; IOVA×1 yday $8.22 → 09:30 $8.00 -0.22; CAN×39 yday $0.37 → 09:30 $0.38 +0.39 | — |
| 2026-08-25 09:30 ET | **SELL** | `MRNA` | 8 | $141.19 | $2.03 | $-75.65 | $1,149.16 | ▼ -75.65 after sell → book $10,786.48; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ABCL` | 110 | $10.77 | $2.35 | $-119.62 | $2,331.51 | ▼ -119.62 after sell → book $10,784.13; vs 09:30 mark -2.35 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `AZI` | 949 | $1.33 | $12.41 | $-62.61 | $3,581.27 | ▼ -62.61 after sell → book $10,771.72; vs 09:30 mark -12.41 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `SENS` | 145 | $9.66 | $2.46 | $+103.86 | $4,979.51 | ▲ +103.86 after sell → book $10,769.26; vs 09:30 mark -2.46 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ALEC` | 541 | $2.30 | $7.08 | $-68.16 | $6,216.73 | ▼ -68.16 after sell → book $10,762.18; vs 09:30 mark -7.08 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BTGO` | 196 | $6.89 | $2.62 | $+50.66 | $7,564.55 | ▲ +50.66 after sell → book $10,759.56; vs 09:30 mark -2.62 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `AUTL` | 526 | $2.32 | $6.88 | $-92.57 | $8,777.99 | ▼ -92.57 after sell → book $10,752.68; vs 09:30 mark -6.88 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `ASST` | 69 | $20.90 | $2.20 | — | $7,333.69 | — | rank by hot_score; rank hot_score; list yday_gainer; 🔵; ret5=+47.9; leftover $1463.00 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 12 | $119.46 | $2.03 | — | $5,898.15 | — | rank by hot_score; rank hot_score; list mover_buy; 🔵; ⚪; ret5=+25.9; leftover $1463.00 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `RUM` | 156 | $9.36 | $2.46 | — | $4,435.53 | — | rank by hot_score; rank hot_score; list yday_gainer; 🔵; ret5=+21.3; leftover $1463.00 | join🔴 sector🟡 gen🟡 news🟢 digest🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BMNR` | 59 | $24.73 | $2.17 | — | $2,974.29 | — | rank by hot_score; rank hot_score; list yday_gainer; ret5=+26.3; leftover $1463.00 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟡 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `NIQ` | 74 | $19.56 | $2.21 | — | $1,524.64 | — | rank by hot_score; rank hot_score; list ohlc_hot; 🔵; ret5=+7.6; leftover $1463.00 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `DEFT` | 2285 | $0.64 | $21.48 | — | $40.76 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover,ohlc_hot; 🔵; ⚪; ret5=+17.6; leftover $1463.00 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $40.76 | ▲ 09:30 equity $10,507.35 vs yday $10,507.35 (+0.00) | 09:30 open · cash $40.76 (unchanged overnight, no fees) · equity $10,507.35 vs prior close $10,507.35 (+0.00) because holdings re-marked: CYPH×1131 yday $1.64 → 09:30 $1.64 +0.00; XHG×2 yday $4.05 → 09:30 $4.05 +0.00; CAPR×1 yday $7.19 → 09:30 $7.19 +0.00; ARCT×1 yday $14.21 → 09:30 $14.21 +0.00; IOVA×1 yday $8.08 → 09:30 $8.08 +0.00; CAN×39 yday $0.36 → 09:30 $0.36 +0.00; ASST×69 yday $20.20 → 09:30 $20.20 +0.00; AU×12 yday $118.55 → 09:30 $118.55 +0.00; RUM×156 yday $9.35 → 09:30 $9.35 +0.00; BMNR×59 yday $24.21 → 09:30 $24.21 +0.00; NIQ×74 yday $19.46 → 09:30 $19.46 +0.00; DEFT×2285 yday $0.62 → 09:30 $0.62 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $40.76 | ▼ 09:30 equity $10,565.72 vs yday $10,720.14 (-154.42) | 09:30 open · cash $40.76 (unchanged overnight, no fees) · equity $10,565.72 vs prior close $10,720.14 (-154.42) because holdings re-marked: CYPH×1131 yday $1.64 → 09:30 $1.60 -45.24; XHG×2 yday $4.05 → 09:30 $3.81 -0.48; CAPR×1 yday $7.19 → 09:30 $8.29 +1.10; ARCT×1 yday $14.21 → 09:30 $15.35 +1.14; IOVA×1 yday $8.08 → 09:30 $8.34 +0.26; CAN×39 yday $0.36 → 09:30 $0.40 +1.56; ASST×69 yday $20.20 → 09:30 $20.72 +35.88; AU×12 yday $118.55 → 09:30 $119.80 +15.00; RUM×156 yday $9.35 → 09:30 $10.07 +112.32; BMNR×59 yday $24.21 → 09:30 $24.24 +1.77; NIQ×74 yday $19.46 → 09:30 $19.20 -19.24; DEFT×2285 yday $0.62 → 09:30 $0.60 -45.70 | — |
| 2026-08-27 09:30 ET | **SELL** | `CYPH` | 1131 | $1.60 | $14.79 | $+479.57 | $1,835.57 | ▲ +479.57 after sell → book $10,550.93; vs 09:30 mark -14.79 | dropped from list after 5 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `XHG` | 2 | $3.81 | $0.10 | $-1.56 | $1,843.09 | ▼ -1.56 after sell → book $10,550.83; vs 09:30 mark -0.10 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `CAPR` | 1 | $8.29 | $0.11 | $+1.30 | $1,851.27 | ▲ +1.30 after sell → book $10,550.72; vs 09:30 mark -0.11 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `ARCT` | 1 | $15.35 | $0.18 | $+3.93 | $1,866.44 | ▲ +3.93 after sell → book $10,550.54; vs 09:30 mark -0.18 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `IOVA` | 1 | $8.34 | $0.11 | $-0.94 | $1,874.68 | ▼ -0.94 after sell → book $10,550.44; vs 09:30 mark -0.10 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `CAN` | 39 | $0.40 | $0.29 | $+3.61 | $1,889.99 | ▲ +3.61 after sell → book $10,550.15; vs 09:30 mark -0.29 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **BUY** | `MOS` | 9 | $24.84 | $2.02 | — | $1,664.41 | — | rank by hot_score; rank hot_score; list flatten; ret5=+13.0; leftover $236.25 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟢 |
| 2026-08-27 09:30 ET | **BUY** | `DLO` | 15 | $15.60 | $2.04 | — | $1,428.37 | — | rank by hot_score; rank hot_score; list mover_buy; 🔵; ret5=+7.1; leftover $236.25 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 91 | $2.59 | $2.26 | — | $1,190.42 | — | rank by hot_score; rank hot_score; list flatten; ret5=+4.2; leftover $236.25 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 16 | $14.09 | $2.04 | — | $962.94 | — | rank by hot_score; rank hot_score; list flatten; ret5=+1.1; leftover $236.25 | join🟢 sector🔴 gen🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `PLTR` | 1 | $170.60 | $1.71 | — | $790.63 | — | rank by hot_score; rank hot_score; list mover_buy; 🔵; ret5=+3.4; leftover $236.25 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 5 | $40.72 | $2.00 | — | $585.03 | — | rank by hot_score; rank hot_score; list flatten; ret5=+1.8; leftover $236.25 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `GEN` | 8 | $28.89 | $2.01 | — | $351.89 | — | rank by hot_score; rank hot_score; list mover_buy; 🔵; ret5=+1.6; leftover $236.25 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $351.89 | ▲ 09:30 equity $10,621.43 vs yday $10,460.31 (+161.12) | 09:30 open · cash $351.89 (unchanged overnight, no fees) · equity $10,621.43 vs prior close $10,460.31 (+161.12) because holdings re-marked: ASST×69 yday $21.50 → 09:30 $22.45 +65.55; AU×12 yday $118.11 → 09:30 $117.41 -8.40; RUM×156 yday $9.38 → 09:30 $9.51 +20.28; BMNR×59 yday $24.91 → 09:30 $25.91 +59.00; NIQ×74 yday $18.74 → 09:30 $18.79 +3.70; DEFT×2285 yday $0.59 → 09:30 $0.60 +22.85; MOS×9 yday $24.16 → 09:30 $24.00 -1.44; DLO×15 yday $15.36 → 09:30 $15.33 -0.45; SLI×91 yday $2.61 → 09:30 $2.60 -0.91; CRK×16 yday $14.50 → 09:30 $14.42 -1.28; PLTR×1 yday $177.50 → 09:30 $178.75 +1.25; RRC×5 yday $41.55 → 09:30 $41.44 -0.55; GEN×8 yday $29.64 → 09:30 $29.83 +1.52 | — |
| 2026-08-28 09:30 ET | **SELL** | `ASST` | 69 | $22.45 | $2.22 | $+102.53 | $1,898.72 | ▲ +102.53 after sell → book $10,619.21; vs 09:30 mark -2.22 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `AU` | 12 | $117.41 | $2.05 | $-28.67 | $3,305.60 | ▼ -28.67 after sell → book $10,617.17; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `RUM` | 156 | $9.51 | $2.50 | $+18.45 | $4,786.66 | ▲ +18.45 after sell → book $10,614.67; vs 09:30 mark -2.50 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `BMNR` | 59 | $25.91 | $2.19 | $+65.26 | $6,313.16 | ▲ +65.26 after sell → book $10,612.48; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `DEFT` | 2285 | $0.60 | $20.96 | $-133.83 | $7,663.21 | ▼ -133.83 after sell → book $10,591.53; vs 09:30 mark -20.95 | dropped from list after 3 sess (min 3) | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 heat🟡 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `FIGR` | 29 | $37.42 | $2.08 | — | $6,575.95 | — | rank by hot_score; rank hot_score; list yday_mover; ret5=+24.4; leftover $1094.74 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 heat🟡 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `ERO` | 27 | $39.20 | $2.07 | — | $5,515.48 | — | rank by hot_score; rank hot_score; list ohlc_hot; ret5=+16.6; leftover $1094.74 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `TRLV` | 96 | $11.38 | $2.28 | — | $4,420.72 | — | rank by hot_score; rank hot_score; list ohlc_hot; ret5=+15.0; leftover $1094.74 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 heat🟡 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CVI` | 27 | $40.04 | $2.07 | — | $3,337.57 | — | rank by hot_score; rank hot_score; list ohlc_hot; ret5=+12.1; leftover $1094.74 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `VIRT` | 16 | $65.42 | $2.04 | — | $2,288.81 | — | rank by hot_score; rank hot_score; list ohlc_hot; ret5=+13.2; leftover $1094.74 | join🟢 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟡 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `TXG` | 17 | $64.10 | $2.04 | — | $1,197.07 | — | rank by hot_score; rank hot_score; list ohlc_hot; ret5=+16.1; leftover $1094.74 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `GUTS` | 1479 | $0.74 | $15.38 | — | $87.23 | — | rank by hot_score; rank hot_score; list ohlc_hot; ret5=+14.7; leftover $1094.74 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $87.23 | ▼ 09:30 equity $10,525.10 vs yday $10,631.31 (-106.21) | 09:30 open · cash $87.23 (unchanged overnight, no fees) · equity $10,525.10 vs prior close $10,631.31 (-106.21) because holdings re-marked: NIQ×74 yday $19.07 → 09:30 $19.20 +9.62; MOS×9 yday $23.76 → 09:30 $23.75 -0.09; DLO×15 yday $15.14 → 09:30 $15.01 -1.95; SLI×91 yday $2.64 → 09:30 $2.51 -11.83; CRK×16 yday $14.62 → 09:30 $14.56 -0.96; PLTR×1 yday $185.93 → 09:30 $184.04 -1.89; RRC×5 yday $41.64 → 09:30 $41.11 -2.65; GEN×8 yday $30.50 → 09:30 $31.02 +4.16; FIGR×29 yday $38.02 → 09:30 $35.50 -73.08; ERO×27 yday $39.82 → 09:30 $38.60 -32.94; TRLV×96 yday $11.03 → 09:30 $12.41 +132.48; CVI×27 yday $39.76 → 09:30 $41.76 +54.00; VIRT×16 yday $67.04 → 09:30 $66.39 -10.40; TXG×17 yday $64.85 → 09:30 $60.90 -67.15; GUTS×1479 yday $0.74 → 09:30 $0.67 -103.53 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $87.23 | ▲ 09:30 equity $10,695.78 vs yday $10,559.45 (+136.33) | 09:30 open · cash $87.23 (unchanged overnight, no fees) · equity $10,695.78 vs prior close $10,559.45 (+136.33) because holdings re-marked: NIQ×74 yday $19.20 → 09:30 $19.06 -10.36; MOS×9 yday $23.78 → 09:30 $24.00 +1.98; DLO×15 yday $15.00 → 09:30 $14.88 -1.80; SLI×91 yday $2.51 → 09:30 $2.70 +17.29; CRK×16 yday $14.51 → 09:30 $14.31 -3.20; PLTR×1 yday $183.80 → 09:30 $185.52 +1.72; RRC×5 yday $41.78 → 09:30 $41.32 -2.30; GEN×8 yday $31.02 → 09:30 $30.56 -3.68; FIGR×29 yday $36.41 → 09:30 $36.80 +11.31; ERO×27 yday $38.49 → 09:30 $37.30 -32.13; TRLV×96 yday $12.41 → 09:30 $11.89 -49.92; CVI×27 yday $41.76 → 09:30 $42.86 +29.70; VIRT×16 yday $66.39 → 09:30 $65.64 -12.00; TXG×17 yday $61.40 → 09:30 $62.99 +27.03; GUTS×1479 yday $0.67 → 09:30 $0.78 +162.69 | — |
| 2026-09-01 09:30 ET | **SELL** | `NIQ` | 74 | $19.06 | $2.24 | $-41.45 | $1,495.43 | ▼ -41.45 after sell → book $10,693.54; vs 09:30 mark -2.24 | dropped from list after 5 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `MOS` | 9 | $24.00 | $2.04 | $-11.61 | $1,709.40 | ▼ -11.61 after sell → book $10,691.51; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `DLO` | 15 | $14.88 | $2.06 | $-14.89 | $1,930.54 | ▼ -14.89 after sell → book $10,689.45; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `SLI` | 91 | $2.70 | $2.29 | $+5.46 | $2,173.95 | ▲ +5.46 after sell → book $10,687.16; vs 09:30 mark -2.29 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `CRK` | 16 | $14.31 | $2.06 | $-0.58 | $2,400.85 | ▼ -0.58 after sell → book $10,685.10; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-01 09:30 ET | **SELL** | `PLTR` | 1 | $185.52 | $1.88 | $+11.33 | $2,584.50 | ▲ +11.33 after sell → book $10,683.23; vs 09:30 mark -1.87 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `RRC` | 5 | $41.32 | $2.02 | $-1.03 | $2,789.07 | ▼ -1.03 after sell → book $10,681.20; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `GEN` | 8 | $30.56 | $2.03 | $+9.31 | $3,031.52 | ▲ +9.31 after sell → book $10,679.17; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3,031.52 | ▼ 09:30 equity $10,414.75 vs yday $10,507.72 (-92.97) | 09:30 open · cash $3,031.52 (unchanged overnight, no fees) · equity $10,414.75 vs prior close $10,507.72 (-92.97) because holdings re-marked: FIGR×29 yday $35.70 → 09:30 $35.46 -6.96; ERO×27 yday $36.01 → 09:30 $35.95 -1.62; TRLV×96 yday $11.89 → 09:30 $11.54 -33.60; CVI×27 yday $42.86 → 09:30 $42.94 +2.16; VIRT×16 yday $65.64 → 09:30 $65.38 -4.16; TXG×17 yday $62.92 → 09:30 $61.79 -19.21; GUTS×1479 yday $0.71 → 09:30 $0.69 -29.58 | — |
| 2026-09-02 09:30 ET | **SELL** | `FIGR` | 29 | $35.46 | $2.10 | $-61.01 | $4,057.76 | ▼ -61.01 after sell → book $10,412.65; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `ERO` | 27 | $35.95 | $2.09 | $-91.91 | $5,026.32 | ▼ -91.91 after sell → book $10,410.56; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | join🟢 sector🔴 gen🔴 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-02 09:30 ET | **SELL** | `CVI` | 27 | $42.94 | $2.09 | $+74.14 | $6,183.61 | ▲ +74.14 after sell → book $10,408.47; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-02 09:30 ET | **SELL** | `VIRT` | 16 | $65.38 | $2.06 | $-4.74 | $7,227.63 | ▼ -4.74 after sell → book $10,406.41; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,227.63 | ▲ 09:30 equity $10,462.26 vs yday $10,424.42 (+37.84) | 09:30 open · cash $7,227.63 (unchanged overnight, no fees) · equity $10,462.26 vs prior close $10,424.42 (+37.84) because holdings re-marked: TRLV×96 yday $11.74 → 09:30 $11.78 +3.84; TXG×17 yday $59.98 → 09:30 $60.24 +4.42; GUTS×1479 yday $0.71 → 09:30 $0.73 +29.58 | — |
| 2026-09-03 09:30 ET | **SELL** | `TRLV` | 96 | $11.78 | $2.30 | $+33.82 | $8,356.21 | ▲ +33.82 after sell → book $10,459.96; vs 09:30 mark -2.30 | dropped from list after 4 sess (min 3) | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **SELL** | `TXG` | 17 | $60.24 | $2.06 | $-69.72 | $9,378.23 | ▼ -69.72 after sell → book $10,457.90; vs 09:30 mark -2.06 | dropped from list after 4 sess (min 3) | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-09-03 09:30 ET | **SELL** | `GUTS` | 1479 | $0.73 | $15.49 | $-45.66 | $10,442.41 | ▼ -45.66 after sell → book $10,442.41; vs 09:30 mark -15.49 | dropped from list after 4 sess (min 3) | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `MRNA` | 8 | $151.40 | $2.01 | — | $9,229.19 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+129.2; leftover $1305.30 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `XHG` | 365 | $3.57 | $4.71 | — | $7,921.43 | — | rank by hot_score; rank hot_score; list ohlc_hot; ret5=+16.1; leftover $1305.30 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 79 | $16.46 | $2.23 | — | $6,618.87 | — | rank by hot_score; rank hot_score; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+63.4; leftover $1305.30 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CAN` | 4351 | $0.30 | $26.11 | — | $5,287.46 | — | rank by hot_score; rank hot_score; list yday_mover; 🔵; ret5=+54.3; leftover $1305.30 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `NIQ` | 70 | $18.60 | $2.20 | — | $3,983.26 | — | rank by hot_score; rank hot_score; list ohlc_hot; 🔵; ret5=+7.6; leftover $1305.30 | join🟢 sector🟢 gen🟡 news🔴 digest🟢 judge🟡 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DEFT` | 1948 | $0.67 | $18.90 | — | $2,659.20 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+17.6; leftover $1305.30 | join🟡 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `OMER` | 68 | $18.97 | $2.19 | — | $1,367.05 | — | rank by hot_score; rank hot_score; list ohlc_hot; ret5=+12.1; leftover $1305.30 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ERO` | 36 | $35.62 | $2.10 | — | $82.63 | — | rank by hot_score; rank hot_score; list ohlc_hot; 🔵; ret5=+16.6; leftover $1305.30 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🔴 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $82.63 | ▲ 09:30 equity $10,441.34 vs yday $10,256.72 (+184.62) | 09:30 open · cash $82.63 (unchanged overnight, no fees) · equity $10,441.34 vs prior close $10,256.72 (+184.62) because holdings re-marked: MRNA×8 yday $150.81 → 09:30 $145.95 -38.88; XHG×365 yday $3.32 → 09:30 $3.38 +21.90; ARCT×79 yday $16.74 → 09:30 $16.77 +2.37; CAN×4351 yday $0.31 → 09:30 $0.34 +130.53; NIQ×70 yday $18.35 → 09:30 $18.66 +21.70; DEFT×1948 yday $0.65 → 09:30 $0.65 +0.00; OMER×68 yday $18.86 → 09:30 $18.99 +8.84; ERO×36 yday $34.76 → 09:30 $35.82 +38.16 | — |
| 2026-09-04 09:30 ET | **BUY** | `HQ` | 1 | $17.06 | $0.17 | — | $65.40 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; ret5=+17.3; leftover $27.54 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 ab🟡 heat🔴 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 5 | $5.08 | $0.27 | — | $39.73 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+28.1; leftover $27.54 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `TRLV` | 2 | $11.89 | $0.24 | — | $15.71 | — | rank by hot_score; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+15.0; leftover $27.54 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |

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
