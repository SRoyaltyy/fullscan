# Factor mine action — `union_ret_5_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `ret_5` · size `leftover` · sell `list` · S-boost `none` · rank by ret_5

Cash book **+15.42%** ($11,542) · signal-only (no cash/fees) was +21.11%. Starts YES **14/17**. Fills 109 · skips 164 · realized $+1189.04.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `ret_5` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $7.87.

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | vs yday close | Bought | Sold | Close cash | Close equity | Close held | 09:30 why |
|---|---:|---:|---|---:|---:|---|---|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | TNDM, INO, IREN, TPG, VOR, SLS, TGTX, BTSG | — | $114.01 | $10,265.50 | TNDM×53, INO×1543, IREN×27, TPG×24, VOR×56, SLS×106, TGTX×25, BTSG×20 | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-14 | +5.50 | $114.01 | TNDM×53, INO×1543, IREN×27, TPG×24, VOR×56, SLS×106, TGTX×25, BTSG×20 | $10,276.78 | +11.28 | ZENA, AIRO, BCAR | — | $77.12 | $10,556.62 | TNDM×53, INO×1543, IREN×27, TPG×24, VOR×56, SLS×106, TGTX×25, BTSG×20, ZENA×6, AIRO×1, BCAR×2 | 09:30 open · cash $114.01 (unchanged overnight, no fees) · equity $10,276.78 vs prior close $10,265.50 (+11.28) because holdings re-marked: TNDM×53 yday $23.13 → 09:30 $22.92 -11.13; INO×1543 yday $0.90 → 09:30 $0.93 +46.29; IREN×27 yday $44.76 → 09:30 $44.09 -18.09; TPG×24 yday $54.62 → 09:30 $55.29 +16.08; VOR×56 yday $23.29 → 09:30 $23.33 +2.24; SLS×106 yday $12.36 → 09:30 $12.40 +4.24; TGTX×25 yday $47.94 → 09:30 $47.27 -16.75; BTSG×20 yday $60.23 → 09:30 $59.65 -11.60 |
| 2026-08-17 | +2.25 | $77.12 | TNDM×53, INO×1543, IREN×27, TPG×24, VOR×56, SLS×106, TGTX×25, BTSG×20, ZENA×6, AIRO×1, BCAR×2 | $10,529.92 | -26.70 | XHG, CAPR, KOPN, NPWR | — | $46.51 | $10,624.01 | TNDM×53, INO×1543, IREN×27, TPG×24, VOR×56, SLS×106, TGTX×25, BTSG×20, ZENA×6, AIRO×1, BCAR×2, XHG×2, CAPR×1, KOPN×1, NPWR×5 | 09:30 open · cash $77.12 (unchanged overnight, no fees) · equity $10,529.92 vs prior close $10,556.62 (-26.70) because holdings re-marked: TNDM×53 yday $22.72 → 09:30 $22.50 -11.66; INO×1543 yday $1.09 → 09:30 $1.07 -30.86; IREN×27 yday $44.06 → 09:30 $45.23 +31.59; TPG×24 yday $53.03 → 09:30 $52.67 -8.64; VOR×56 yday $23.03 → 09:30 $22.91 -6.72; SLS×106 yday $12.78 → 09:30 $12.78 +0.00; TGTX×25 yday $48.74 → 09:30 $48.74 +0.00; BTSG×20 yday $61.71 → 09:30 $61.69 -0.40; ZENA×6 yday $2.14 → 09:30 $2.08 -0.33; AIRO×1 yday $9.57 → 09:30 $9.57 +0.00; BCAR×2 yday $5.83 → 09:30 $5.99 +0.32 |
| 2026-08-18 | -6.20 | $46.51 | TNDM×53, INO×1543, IREN×27, TPG×24, VOR×56, SLS×106, TGTX×25, BTSG×20, ZENA×6, AIRO×1, BCAR×2, XHG×2, CAPR×1, KOPN×1, NPWR×5 | $10,511.12 | -112.89 | — | TNDM, INO, IREN, TPG, VOR, SLS, TGTX, BTSG | $10,415.30 | $10,476.11 | ZENA×6, AIRO×1, BCAR×2, XHG×2, CAPR×1, KOPN×1, NPWR×5 | 09:30 open · cash $46.51 (unchanged overnight, no fees) · equity $10,511.12 vs prior close $10,624.01 (-112.89) because holdings re-marked: TNDM×53 yday $22.25 → 09:30 $22.16 -5.03; INO×1543 yday $1.15 → 09:30 $1.14 -15.43; IREN×27 yday $44.90 → 09:30 $43.56 -36.18; TPG×24 yday $51.77 → 09:30 $51.77 +0.00; VOR×56 yday $23.01 → 09:30 $22.82 -10.64; SLS×106 yday $13.00 → 09:30 $12.66 -36.04; TGTX×25 yday $49.28 → 09:30 $49.28 +0.00; BTSG×20 yday $60.38 → 09:30 $60.00 -7.60; ZENA×6 yday $2.05 → 09:30 $1.95 -0.63; AIRO×1 yday $9.41 → 09:30 $9.01 -0.40; BCAR×2 yday $5.82 → 09:30 $5.52 -0.60; XHG×2 yday $3.91 → 09:30 $3.94 +0.06; CAPR×1 yday $7.45 → 09:30 $7.50 +0.05; KOPN×1 yday $5.32 → 09:30 $5.03 -0.29; NPWR×5 yday $1.73 → 09:30 $1.70 -0.15 |
| 2026-08-19 | -7.20 | $10,415.30 | ZENA×6, AIRO×1, BCAR×2, XHG×2, CAPR×1, KOPN×1, NPWR×5 | $10,476.57 | +0.46 | — | ZENA, AIRO, BCAR | $10,446.70 | $10,476.62 | XHG×2, CAPR×1, KOPN×1, NPWR×5 | 09:30 open · cash $10,415.30 (unchanged overnight, no fees) · equity $10,476.57 vs prior close $10,476.11 (+0.46) because holdings re-marked: ZENA×6 yday $2.04 → 09:30 $2.01 -0.18; AIRO×1 yday $8.98 → 09:30 $9.10 +0.12; BCAR×2 yday $5.33 → 09:30 $5.32 -0.02; XHG×2 yday $4.28 → 09:30 $4.32 +0.08; CAPR×1 yday $7.08 → 09:30 $7.19 +0.11; KOPN×1 yday $5.04 → 09:30 $5.14 +0.10; NPWR×5 yday $1.65 → 09:30 $1.70 +0.25 |
| 2026-08-20 | +1.12 | $10,446.70 | XHG×2, CAPR×1, KOPN×1, NPWR×5 | $10,475.63 | -0.99 | MRNA, CYPH, AZI, BTGO, BNTX, AUTL, ASST, BRR | XHG, CAPR, KOPN, NPWR | $76.33 | $10,529.08 | MRNA×8, CYPH×1138, AZI×955, BTGO×198, BNTX×12, AUTL×530, ASST×81, BRR×629 | 09:30 open · cash $10,446.70 (unchanged overnight, no fees) · equity $10,475.63 vs prior close $10,476.62 (-0.99) because holdings re-marked: XHG×2 yday $4.33 → 09:30 $4.10 -0.46; CAPR×1 yday $7.98 → 09:30 $7.66 -0.32; KOPN×1 yday $4.93 → 09:30 $4.87 -0.06; NPWR×5 yday $1.67 → 09:30 $1.64 -0.15 |
| 2026-08-21 | +3.25 | $76.33 | MRNA×8, CYPH×1138, AZI×955, BTGO×198, BNTX×12, AUTL×530, ASST×81, BRR×629 | $10,899.62 | +370.54 | CAPR, ARCT, IOVA, INO, CAN, INDP | — | $11.01 | $11,098.10 | MRNA×8, CYPH×1138, AZI×955, BTGO×198, BNTX×12, AUTL×530, ASST×81, BRR×629, CAPR×1, ARCT×1, IOVA×1, INO×10, CAN×43, INDP×9 | 09:30 open · cash $76.33 (unchanged overnight, no fees) · equity $10,899.62 vs prior close $10,529.08 (+370.54) because holdings re-marked: MRNA×8 yday $133.32 → 09:30 $133.11 -1.68; CYPH×1138 yday $1.19 → 09:30 $1.32 +147.94; AZI×955 yday $1.44 → 09:30 $1.46 +19.10; BTGO×198 yday $6.60 → 09:30 $6.95 +69.30; BNTX×12 yday $110.89 → 09:30 $110.92 +0.36; AUTL×530 yday $2.46 → 09:30 $2.47 +5.30; ASST×81 yday $16.13 → 09:30 $17.66 +123.93; BRR×629 yday $2.24 → 09:30 $2.25 +6.29 |
| 2026-08-24 | -5.17 | $11.01 | MRNA×8, CYPH×1138, AZI×955, BTGO×198, BNTX×12, AUTL×530, ASST×81, BRR×629, CAPR×1, ARCT×1, IOVA×1, INO×10, CAN×43, INDP×9 | $11,550.56 | +452.46 | — | — | $11.01 | $11,337.67 | MRNA×8, CYPH×1138, AZI×955, BTGO×198, BNTX×12, AUTL×530, ASST×81, BRR×629, CAPR×1, ARCT×1, IOVA×1, INO×10, CAN×43, INDP×9 | 09:30 open · cash $11.01 (unchanged overnight, no fees) · equity $11,550.56 vs prior close $11,098.10 (+452.46) because holdings re-marked: MRNA×8 yday $145.13 → 09:30 $142.70 -19.44; CYPH×1138 yday $1.42 → 09:30 $1.83 +466.58; AZI×955 yday $1.45 → 09:30 $1.46 +9.55; BTGO×198 yday $6.84 → 09:30 $6.87 +5.94; BNTX×12 yday $116.57 → 09:30 $114.11 -29.52; AUTL×530 yday $2.41 → 09:30 $2.36 -26.50; ASST×81 yday $18.22 → 09:30 $18.76 +43.74; BRR×629 yday $2.15 → 09:30 $2.15 +0.00; CAPR×1 yday $6.29 → 09:30 $8.01 +1.72; ARCT×1 yday $13.45 → 09:30 $13.26 -0.19; IOVA×1 yday $8.29 → 09:30 $8.05 -0.24; INO×10 yday $1.18 → 09:30 $1.20 +0.20; CAN×43 yday $0.35 → 09:30 $0.38 +1.08; INDP×9 yday $1.29 → 09:30 $1.24 -0.45 |
| 2026-08-25 | +1.80 | $11.01 | MRNA×8, CYPH×1138, AZI×955, BTGO×198, BNTX×12, AUTL×530, ASST×81, BRR×629, CAPR×1, ARCT×1, IOVA×1, INO×10, CAN×43, INDP×9 | $11,473.40 | +135.73 | DFDV, BMNR, AU, RUM, HMY, FWDI | MRNA, AZI, BTGO, BNTX, AUTL, BRR | $121.45 | $11,185.95 | CYPH×1138, ASST×81, CAPR×1, ARCT×1, IOVA×1, INO×10, CAN×43, INDP×9, DFDV×300, BMNR×52, AU×10, RUM×137, HMY×56, FWDI×215 | 09:30 open · cash $11.01 (unchanged overnight, no fees) · equity $11,473.40 vs prior close $11,337.67 (+135.73) because holdings re-marked: MRNA×8 yday $139.27 → 09:30 $141.19 +15.36; CYPH×1138 yday $1.64 → 09:30 $1.70 +68.28; AZI×955 yday $1.40 → 09:30 $1.33 -66.85; BTGO×198 yday $6.97 → 09:30 $6.89 -15.84; BNTX×12 yday $111.34 → 09:30 $113.13 +21.48; AUTL×530 yday $2.38 → 09:30 $2.32 -31.80; ASST×81 yday $19.82 → 09:30 $20.90 +87.48; BRR×629 yday $2.16 → 09:30 $2.25 +56.61; CAPR×1 yday $7.05 → 09:30 $6.79 -0.26; ARCT×1 yday $13.76 → 09:30 $14.34 +0.58; IOVA×1 yday $8.22 → 09:30 $8.00 -0.22; INO×10 yday $1.22 → 09:30 $1.25 +0.30; CAN×43 yday $0.37 → 09:30 $0.38 +0.43; INDP×9 yday $1.16 → 09:30 $1.18 +0.18 |
| 2026-08-26 | +2.02 | $121.45 | CYPH×1138, ASST×81, CAPR×1, ARCT×1, IOVA×1, INO×10, CAN×43, INDP×9, DFDV×300, BMNR×52, AU×10, RUM×137, HMY×56, FWDI×215 | $11,185.95 | +0.00 | — | — | $121.45 | $11,423.67 | CYPH×1138, ASST×81, CAPR×1, ARCT×1, IOVA×1, INO×10, CAN×43, INDP×9, DFDV×300, BMNR×52, AU×10, RUM×137, HMY×56, FWDI×215 | 09:30 open · cash $121.45 (unchanged overnight, no fees) · equity $11,185.95 vs prior close $11,185.95 (+0.00) because holdings re-marked: CYPH×1138 yday $1.64 → 09:30 $1.64 +0.00; ASST×81 yday $20.20 → 09:30 $20.20 +0.00; CAPR×1 yday $7.19 → 09:30 $7.19 +0.00; ARCT×1 yday $14.21 → 09:30 $14.21 +0.00; IOVA×1 yday $8.08 → 09:30 $8.08 +0.00; INO×10 yday $1.25 → 09:30 $1.25 +0.00; CAN×43 yday $0.36 → 09:30 $0.36 +0.00; INDP×9 yday $1.25 → 09:30 $1.25 +0.00; DFDV×300 yday $4.16 → 09:30 $4.16 +0.00; BMNR×52 yday $24.21 → 09:30 $24.21 +0.00; AU×10 yday $118.55 → 09:30 $118.55 +0.00; RUM×137 yday $9.35 → 09:30 $9.35 +0.00; HMY×56 yday $22.50 → 09:30 $22.50 +0.00; FWDI×215 yday $5.86 → 09:30 $5.86 +0.00 |
| 2026-08-27 | — | $121.45 | CYPH×1138, ASST×81, CAPR×1, ARCT×1, IOVA×1, INO×10, CAN×43, INDP×9, DFDV×300, BMNR×52, AU×10, RUM×137, HMY×56, FWDI×215 | $11,372.82 | -50.85 | MOS, DLO, MRVL, SLI, PLTR, TX, RRC, GEN | CYPH, ASST, CAPR, ARCT, IOVA, INO, CAN, INDP | $395.46 | $11,323.06 | DFDV×300, BMNR×52, AU×10, RUM×137, HMY×56, FWDI×215, MOS×18, DLO×29, MRVL×1, SLI×177, PLTR×2, TX×8, RRC×11, GEN×15 | 09:30 open · cash $121.45 (unchanged overnight, no fees) · equity $11,372.82 vs prior close $11,423.67 (-50.85) because holdings re-marked: CYPH×1138 yday $1.64 → 09:30 $1.60 -45.52; ASST×81 yday $20.20 → 09:30 $20.72 +42.12; CAPR×1 yday $7.19 → 09:30 $8.29 +1.10; ARCT×1 yday $14.21 → 09:30 $15.35 +1.14; IOVA×1 yday $8.08 → 09:30 $8.34 +0.26; INO×10 yday $1.25 → 09:30 $1.28 +0.30; CAN×43 yday $0.36 → 09:30 $0.40 +1.72; INDP×9 yday $1.25 → 09:30 $1.09 -1.44; DFDV×300 yday $4.16 → 09:30 $4.35 +57.00; BMNR×52 yday $24.21 → 09:30 $24.24 +1.56; AU×10 yday $118.55 → 09:30 $119.80 +12.50; RUM×137 yday $9.35 → 09:30 $10.07 +98.64; HMY×56 yday $22.50 → 09:30 $22.39 -6.16; FWDI×215 yday $5.86 → 09:30 $5.97 +23.65 |
| 2026-08-28 | +0.75 | $395.46 | DFDV×300, BMNR×52, AU×10, RUM×137, HMY×56, FWDI×215, MOS×18, DLO×29, MRVL×1, SLI×177, PLTR×2, TX×8, RRC×11, GEN×15 | $11,488.80 | +165.74 | FIGR, WPM, SCCO, AMTX, SBSW, EQX, ERO, TXG | DFDV, BMNR, AU, RUM, HMY, FWDI | $313.27 | $11,542.32 | MOS×18, DLO×29, MRVL×1, SLI×177, PLTR×2, TX×8, RRC×11, GEN×15, FIGR×27, WPM×6, SCCO×4, AMTX×546, SBSW×85, EQX×75, ERO×26, TXG×15 | 09:30 open · cash $395.46 (unchanged overnight, no fees) · equity $11,488.80 vs prior close $11,323.06 (+165.74) because holdings re-marked: DFDV×300 yday $4.50 → 09:30 $4.81 +93.00; BMNR×52 yday $24.91 → 09:30 $25.91 +52.00; AU×10 yday $118.11 → 09:30 $117.41 -7.00; RUM×137 yday $9.38 → 09:30 $9.51 +17.81; HMY×56 yday $22.43 → 09:30 $20.70 -96.88; FWDI×215 yday $5.93 → 09:30 $6.39 +98.90; MOS×18 yday $24.16 → 09:30 $24.00 -2.88; DLO×29 yday $15.36 → 09:30 $15.33 -0.87; MRVL×1 yday $245.11 → 09:30 $253.44 +8.33; SLI×177 yday $2.61 → 09:30 $2.60 -1.77; PLTR×2 yday $177.50 → 09:30 $178.75 +2.50; TX×8 yday $55.13 → 09:30 $55.25 +0.96; RRC×11 yday $41.55 → 09:30 $41.44 -1.21; GEN×15 yday $29.64 → 09:30 $29.83 +2.85 |
| 2026-08-31 | -5.85 | $313.27 | MOS×18, DLO×29, MRVL×1, SLI×177, PLTR×2, TX×8, RRC×11, GEN×15, FIGR×27, WPM×6, SCCO×4, AMTX×546, SBSW×85, EQX×75, ERO×26, TXG×15 | $11,213.72 | -328.60 | — | — | $313.27 | $11,236.49 | MOS×18, DLO×29, MRVL×1, SLI×177, PLTR×2, TX×8, RRC×11, GEN×15, FIGR×27, WPM×6, SCCO×4, AMTX×546, SBSW×85, EQX×75, ERO×26, TXG×15 | 09:30 open · cash $313.27 (unchanged overnight, no fees) · equity $11,213.72 vs prior close $11,542.32 (-328.60) because holdings re-marked: MOS×18 yday $23.76 → 09:30 $23.75 -0.18; DLO×29 yday $15.14 → 09:30 $15.01 -3.77; MRVL×1 yday $241.45 → 09:30 $216.69 -24.76; SLI×177 yday $2.64 → 09:30 $2.51 -23.01; PLTR×2 yday $185.93 → 09:30 $184.04 -3.78; TX×8 yday $55.83 → 09:30 $54.84 -7.92; RRC×11 yday $41.64 → 09:30 $41.11 -5.83; GEN×15 yday $30.50 → 09:30 $31.02 +7.80; FIGR×27 yday $38.02 → 09:30 $35.50 -68.04; WPM×6 yday $157.99 → 09:30 $152.49 -33.00; SCCO×4 yday $216.28 → 09:30 $207.95 -33.32; AMTX×546 yday $1.87 → 09:30 $1.90 +16.38; SBSW×85 yday $12.26 → 09:30 $12.14 -10.20; EQX×75 yday $13.45 → 09:30 $12.81 -48.00; ERO×26 yday $39.82 → 09:30 $38.60 -31.72; TXG×15 yday $64.85 → 09:30 $60.90 -59.25 |
| 2026-09-01 | -6.30 | $313.27 | MOS×18, DLO×29, MRVL×1, SLI×177, PLTR×2, TX×8, RRC×11, GEN×15, FIGR×27, WPM×6, SCCO×4, AMTX×546, SBSW×85, EQX×75, ERO×26, TXG×15 | $11,233.63 | -2.86 | — | MOS, DLO, MRVL, SLI, PLTR, TX, RRC, GEN | $3,570.89 | $11,047.23 | FIGR×27, WPM×6, SCCO×4, AMTX×546, SBSW×85, EQX×75, ERO×26, TXG×15 | 09:30 open · cash $313.27 (unchanged overnight, no fees) · equity $11,233.63 vs prior close $11,236.49 (-2.86) because holdings re-marked: MOS×18 yday $23.78 → 09:30 $24.00 +3.96; DLO×29 yday $15.00 → 09:30 $14.88 -3.48; MRVL×1 yday $216.35 → 09:30 $210.57 -5.78; SLI×177 yday $2.51 → 09:30 $2.70 +33.63; PLTR×2 yday $183.80 → 09:30 $185.52 +3.44; TX×8 yday $54.84 → 09:30 $54.82 -0.16; RRC×11 yday $41.78 → 09:30 $41.32 -5.06; GEN×15 yday $31.02 → 09:30 $30.56 -6.90; FIGR×27 yday $36.41 → 09:30 $36.80 +10.53; WPM×6 yday $152.25 → 09:30 $150.78 -8.82; SCCO×4 yday $209.00 → 09:30 $210.05 +4.20; AMTX×546 yday $1.90 → 09:30 $1.87 -16.38; SBSW×85 yday $12.04 → 09:30 $11.92 -10.20; EQX×75 yday $12.71 → 09:30 $12.78 +5.25; ERO×26 yday $38.49 → 09:30 $37.30 -30.94; TXG×15 yday $61.40 → 09:30 $62.99 +23.85 |
| 2026-09-02 | -3.83 | $3,570.89 | FIGR×27, WPM×6, SCCO×4, AMTX×546, SBSW×85, EQX×75, ERO×26, TXG×15 | $10,999.34 | -47.89 | — | FIGR, AMTX, ERO, TXG | $7,402.97 | $10,983.59 | WPM×6, SCCO×4, SBSW×85, EQX×75 | 09:30 open · cash $3,570.89 (unchanged overnight, no fees) · equity $10,999.34 vs prior close $11,047.23 (-47.89) because holdings re-marked: FIGR×27 yday $35.70 → 09:30 $35.46 -6.48; WPM×6 yday $146.46 → 09:30 $146.00 -2.76; SCCO×4 yday $205.00 → 09:30 $203.10 -7.60; AMTX×546 yday $1.87 → 09:30 $1.88 +5.46; SBSW×85 yday $11.56 → 09:30 $11.41 -12.75; EQX×75 yday $12.40 → 09:30 $12.33 -5.25; ERO×26 yday $36.01 → 09:30 $35.95 -1.56; TXG×15 yday $62.92 → 09:30 $61.79 -16.95 |
| 2026-09-03 | -0.90 | $7,402.97 | WPM×6, SCCO×4, SBSW×85, EQX×75 | $11,106.26 | +122.67 | MRNA, ARCT, CAN, ALEC, DEFT, FUTU | SBSW, EQX | $116.46 | $11,262.47 | WPM×6, SCCO×4, MRNA×10, ARCT×95, CAN×5216, ALEC×652, DEFT×2335, FUTU×12 | 09:30 open · cash $7,402.97 (unchanged overnight, no fees) · equity $11,106.26 vs prior close $10,983.59 (+122.67) because holdings re-marked: WPM×6 yday $144.93 → 09:30 $148.89 +23.76; SCCO×4 yday $201.61 → 09:30 $204.50 +11.56; SBSW×85 yday $11.66 → 09:30 $12.37 +60.35; EQX×75 yday $12.18 → 09:30 $12.54 +27.00 |
| 2026-09-04 | — | $116.46 | WPM×6, SCCO×4, MRNA×10, ARCT×95, CAN×5216, ALEC×652, DEFT×2335, FUTU×12 | $11,407.88 | +145.41 | OABI, BRR, HQ | — | $7.87 | $11,542.26 | WPM×6, SCCO×4, MRNA×10, ARCT×95, CAN×5216, ALEC×652, DEFT×2335, FUTU×12, OABI×7, BRR×16, HQ×2 | 09:30 open · cash $116.46 (unchanged overnight, no fees) · equity $11,407.88 vs prior close $11,262.47 (+145.41) because holdings re-marked: WPM×6 yday $150.91 → 09:30 $155.85 +29.64; SCCO×4 yday $204.26 → 09:30 $208.45 +16.76; MRNA×10 yday $150.81 → 09:30 $145.95 -48.60; ARCT×95 yday $16.74 → 09:30 $16.77 +2.85; CAN×5216 yday $0.31 → 09:30 $0.34 +156.48; ALEC×652 yday $2.72 → 09:30 $2.70 -13.04; DEFT×2335 yday $0.65 → 09:30 $0.65 +0.00; FUTU×12 yday $118.08 → 09:30 $118.19 +1.32 |

## Fills (09:30 open snapshot, then buys / sells)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 53 | $23.33 | $2.15 | — | $8,761.36 | — | rank by ret_5; rank ret_5; list flatten; ⚪; ret5=+19.7; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 1543 | $0.81 | $17.13 | — | $7,494.40 | — | rank by ret_5; rank ret_5; list flatten; ⚪; ret5=+13.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 27 | $45.98 | $2.07 | — | $6,250.87 | — | rank by ret_5; rank ret_5; list flatten; ⚪; ret5=+12.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 24 | $50.62 | $2.06 | — | $5,033.85 | — | rank by ret_5; rank ret_5; list flatten; ⚪; ret5=+6.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `VOR` | 56 | $22.01 | $2.16 | — | $3,799.14 | — | rank by ret_5; rank ret_5; list flatten; ⚪; ret5=+0.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 106 | $11.70 | $2.31 | — | $2,556.63 | — | rank by ret_5; rank ret_5; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TGTX` | 25 | $49.70 | $2.06 | — | $1,312.06 | — | rank by ret_5; rank ret_5; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 20 | $59.80 | $2.05 | — | $114.01 | — | rank by ret_5; rank ret_5; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $114.01 | ▲ 09:30 equity $10,276.78 vs yday $10,265.50 (+11.28) | 09:30 open · cash $114.01 (unchanged overnight, no fees) · equity $10,276.78 vs prior close $10,265.50 (+11.28) because holdings re-marked: TNDM×53 yday $23.13 → 09:30 $22.92 -11.13; INO×1543 yday $0.90 → 09:30 $0.93 +46.29; IREN×27 yday $44.76 → 09:30 $44.09 -18.09; TPG×24 yday $54.62 → 09:30 $55.29 +16.08; VOR×56 yday $23.29 → 09:30 $23.33 +2.24; SLS×106 yday $12.36 → 09:30 $12.40 +4.24; TGTX×25 yday $47.94 → 09:30 $47.27 -16.75; BTSG×20 yday $60.23 → 09:30 $59.65 -11.60 | — |
| 2026-08-14 09:30 ET | **BUY** | `ZENA` | 6 | $2.20 | $0.15 | — | $100.66 | — | rank by ret_5; rank ret_5; list yday_gainer; 🔵; ⚪; ret5=+54.3; leftover $14.25 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `AIRO` | 1 | $11.12 | $0.11 | — | $89.43 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; leftover $14.25 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BCAR` | 2 | $6.09 | $0.13 | — | $77.12 | — | rank by ret_5; rank ret_5; list yday_gainer; 🔵; ⚪; ret5=+27.6; leftover $14.25 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $77.12 | ▼ 09:30 equity $10,529.92 vs yday $10,556.62 (-26.70) | 09:30 open · cash $77.12 (unchanged overnight, no fees) · equity $10,529.92 vs prior close $10,556.62 (-26.70) because holdings re-marked: TNDM×53 yday $22.72 → 09:30 $22.50 -11.66; INO×1543 yday $1.09 → 09:30 $1.07 -30.86; IREN×27 yday $44.06 → 09:30 $45.23 +31.59; TPG×24 yday $53.03 → 09:30 $52.67 -8.64; VOR×56 yday $23.03 → 09:30 $22.91 -6.72; SLS×106 yday $12.78 → 09:30 $12.78 +0.00; TGTX×25 yday $48.74 → 09:30 $48.74 +0.00; BTSG×20 yday $61.71 → 09:30 $61.69 -0.40; ZENA×6 yday $2.14 → 09:30 $2.08 -0.33; AIRO×1 yday $9.57 → 09:30 $9.57 +0.00; BCAR×2 yday $5.83 → 09:30 $5.99 +0.32 | — |
| 2026-08-17 09:30 ET | **BUY** | `XHG` | 2 | $4.19 | $0.09 | — | $68.65 | — | rank by ret_5; rank ret_5; list yday_mover; ⚪; ret5=+291.8; leftover $9.64 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `CAPR` | 1 | $6.87 | $0.07 | — | $61.71 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; ret5=+62.6; leftover $9.64 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `KOPN` | 1 | $5.43 | $0.06 | — | $56.22 | — | rank by ret_5; rank ret_5; list yday_gainer; ⚪; ret5=+28.8; leftover $9.64 | join🟡 sector🟢 gen🟢 news🟡 vol🟡 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `NPWR` | 5 | $1.92 | $0.11 | — | $46.51 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; ⚪; ret5=+28.3; leftover $9.64 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $46.51 | ▼ 09:30 equity $10,511.12 vs yday $10,624.01 (-112.89) | 09:30 open · cash $46.51 (unchanged overnight, no fees) · equity $10,511.12 vs prior close $10,624.01 (-112.89) because holdings re-marked: TNDM×53 yday $22.25 → 09:30 $22.16 -5.03; INO×1543 yday $1.15 → 09:30 $1.14 -15.43; IREN×27 yday $44.90 → 09:30 $43.56 -36.18; TPG×24 yday $51.77 → 09:30 $51.77 +0.00; VOR×56 yday $23.01 → 09:30 $22.82 -10.64; SLS×106 yday $13.00 → 09:30 $12.66 -36.04; TGTX×25 yday $49.28 → 09:30 $49.28 +0.00; BTSG×20 yday $60.38 → 09:30 $60.00 -7.60; ZENA×6 yday $2.05 → 09:30 $1.95 -0.63; AIRO×1 yday $9.41 → 09:30 $9.01 -0.40; BCAR×2 yday $5.82 → 09:30 $5.52 -0.60; XHG×2 yday $3.91 → 09:30 $3.94 +0.06; CAPR×1 yday $7.45 → 09:30 $7.50 +0.05; KOPN×1 yday $5.32 → 09:30 $5.03 -0.29; NPWR×5 yday $1.73 → 09:30 $1.70 -0.15 | — |
| 2026-08-18 09:30 ET | **SELL** | `TNDM` | 53 | $22.16 | $2.17 | $-66.33 | $1,218.82 | ▼ -66.33 after sell → book $10,508.95; vs 09:30 mark -2.17 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `INO` | 1543 | $1.14 | $20.17 | $+471.89 | $2,957.67 | ▲ +471.89 after sell → book $10,488.78; vs 09:30 mark -20.17 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `IREN` | 27 | $43.56 | $2.09 | $-69.50 | $4,131.70 | ▼ -69.50 after sell → book $10,486.69; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TPG` | 24 | $51.77 | $2.08 | $+23.38 | $5,372.09 | ▲ +23.38 after sell → book $10,484.60; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `VOR` | 56 | $22.82 | $2.18 | $+41.02 | $6,647.84 | ▲ +41.02 after sell → book $10,482.43; vs 09:30 mark -2.17 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `SLS` | 106 | $12.66 | $2.34 | $+97.12 | $7,987.46 | ▲ +97.12 after sell → book $10,480.09; vs 09:30 mark -2.34 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TGTX` | 25 | $49.28 | $2.08 | $-14.65 | $9,217.37 | ▼ -14.65 after sell → book $10,478.00; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `BTSG` | 20 | $60.00 | $2.07 | $-0.12 | $10,415.30 | ▼ -0.12 after sell → book $10,475.93; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,415.30 | ▲ 09:30 equity $10,476.57 vs yday $10,476.11 (+0.46) | 09:30 open · cash $10,415.30 (unchanged overnight, no fees) · equity $10,476.57 vs prior close $10,476.11 (+0.46) because holdings re-marked: ZENA×6 yday $2.04 → 09:30 $2.01 -0.18; AIRO×1 yday $8.98 → 09:30 $9.10 +0.12; BCAR×2 yday $5.33 → 09:30 $5.32 -0.02; XHG×2 yday $4.28 → 09:30 $4.32 +0.08; CAPR×1 yday $7.08 → 09:30 $7.19 +0.11; KOPN×1 yday $5.04 → 09:30 $5.14 +0.10; NPWR×5 yday $1.65 → 09:30 $1.70 +0.25 | — |
| 2026-08-19 09:30 ET | **SELL** | `ZENA` | 6 | $2.01 | $0.16 | $-1.45 | $10,427.21 | ▼ -1.45 after sell → book $10,476.42; vs 09:30 mark -0.15 | dropped from list after 3 sess (min 3) | join🔴 sector🟡 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-19 09:30 ET | **SELL** | `AIRO` | 1 | $9.10 | $0.11 | $-2.25 | $10,436.19 | ▼ -2.25 after sell → book $10,476.30; vs 09:30 mark -0.12 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `BCAR` | 2 | $5.32 | $0.13 | $-1.80 | $10,446.70 | ▼ -1.80 after sell → book $10,476.17; vs 09:30 mark -0.13 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,446.70 | ▼ 09:30 equity $10,475.63 vs yday $10,476.62 (-0.99) | 09:30 open · cash $10,446.70 (unchanged overnight, no fees) · equity $10,475.63 vs prior close $10,476.62 (-0.99) because holdings re-marked: XHG×2 yday $4.33 → 09:30 $4.10 -0.46; CAPR×1 yday $7.98 → 09:30 $7.66 -0.32; KOPN×1 yday $4.93 → 09:30 $4.87 -0.06; NPWR×5 yday $1.67 → 09:30 $1.64 -0.15 | — |
| 2026-08-20 09:30 ET | **SELL** | `XHG` | 2 | $4.10 | $0.11 | $-0.38 | $10,454.79 | ▼ -0.38 after sell → book $10,475.52; vs 09:30 mark -0.11 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `CAPR` | 1 | $7.66 | $0.10 | $+0.62 | $10,462.35 | ▲ +0.62 after sell → book $10,475.42; vs 09:30 mark -0.10 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `KOPN` | 1 | $4.87 | $0.07 | $-0.69 | $10,467.15 | ▼ -0.69 after sell → book $10,475.35; vs 09:30 mark -0.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `NPWR` | 5 | $1.64 | $0.12 | $-1.63 | $10,475.23 | ▼ -1.63 after sell → book $10,475.23; vs 09:30 mark -0.12 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 8 | $150.14 | $2.01 | — | $9,272.10 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ret5=+173.9; leftover $1309.40 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CYPH` | 1138 | $1.15 | $14.68 | — | $7,948.72 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+62.6; leftover $1309.40 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AZI` | 955 | $1.37 | $12.32 | — | $6,628.05 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ret5=+30.7; leftover $1309.40 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BTGO` | 198 | $6.61 | $2.58 | — | $5,317.68 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ret5=+25.3; leftover $1309.40 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟡 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BNTX` | 12 | $109.06 | $2.03 | — | $4,006.93 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ret5=+22.0; leftover $1309.40 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AUTL` | 530 | $2.47 | $6.84 | — | $2,690.99 | — | rank by ret_5; rank ret_5; list mover_buy; 🔵; ⚪; ret5=+19.8; leftover $1309.40 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ASST` | 81 | $16.00 | $2.23 | — | $1,392.76 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ret5=+18.9; leftover $1309.40 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BRR` | 629 | $2.08 | $8.11 | — | $76.33 | — | rank by ret_5; rank ret_5; list ohlc_hot; 🔵; ret5=+18.0; leftover $1309.40 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $76.33 | ▲ 09:30 equity $10,899.62 vs yday $10,529.08 (+370.54) | 09:30 open · cash $76.33 (unchanged overnight, no fees) · equity $10,899.62 vs prior close $10,529.08 (+370.54) because holdings re-marked: MRNA×8 yday $133.32 → 09:30 $133.11 -1.68; CYPH×1138 yday $1.19 → 09:30 $1.32 +147.94; AZI×955 yday $1.44 → 09:30 $1.46 +19.10; BTGO×198 yday $6.60 → 09:30 $6.95 +69.30; BNTX×12 yday $110.89 → 09:30 $110.92 +0.36; AUTL×530 yday $2.46 → 09:30 $2.47 +5.30; ASST×81 yday $16.13 → 09:30 $17.66 +123.93; BRR×629 yday $2.24 → 09:30 $2.25 +6.29 | — |
| 2026-08-21 09:30 ET | **BUY** | `CAPR` | 1 | $6.81 | $0.07 | — | $69.44 | — | rank by ret_5; rank ret_5; list yday_mover; 🔵; ret5=+62.5; leftover $12.72 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 1 | $11.13 | $0.11 | — | $58.20 | — | rank by ret_5; rank ret_5; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $12.72 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `IOVA` | 1 | $9.08 | $0.09 | — | $49.03 | — | rank by ret_5; rank ret_5; list yday_gainer; 🔵; ⚪; ret5=+37.9; leftover $12.72 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `INO` | 10 | $1.23 | $0.15 | — | $36.57 | — | rank by ret_5; rank ret_5; list mover_buy; 🔵; ⚪; ret5=+34.4; leftover $12.72 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CAN` | 43 | $0.29 | $0.26 | — | $23.68 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ret5=+30.4; leftover $12.72 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `INDP` | 9 | $1.39 | $0.15 | — | $11.01 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+30.2; leftover $12.72 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11.01 | ▲ 09:30 equity $11,550.56 vs yday $11,098.10 (+452.46) | 09:30 open · cash $11.01 (unchanged overnight, no fees) · equity $11,550.56 vs prior close $11,098.10 (+452.46) because holdings re-marked: MRNA×8 yday $145.13 → 09:30 $142.70 -19.44; CYPH×1138 yday $1.42 → 09:30 $1.83 +466.58; AZI×955 yday $1.45 → 09:30 $1.46 +9.55; BTGO×198 yday $6.84 → 09:30 $6.87 +5.94; BNTX×12 yday $116.57 → 09:30 $114.11 -29.52; AUTL×530 yday $2.41 → 09:30 $2.36 -26.50; ASST×81 yday $18.22 → 09:30 $18.76 +43.74; BRR×629 yday $2.15 → 09:30 $2.15 +0.00; CAPR×1 yday $6.29 → 09:30 $8.01 +1.72; ARCT×1 yday $13.45 → 09:30 $13.26 -0.19; IOVA×1 yday $8.29 → 09:30 $8.05 -0.24; INO×10 yday $1.18 → 09:30 $1.20 +0.20; CAN×43 yday $0.35 → 09:30 $0.38 +1.08; INDP×9 yday $1.29 → 09:30 $1.24 -0.45 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11.01 | ▲ 09:30 equity $11,473.40 vs yday $11,337.67 (+135.73) | 09:30 open · cash $11.01 (unchanged overnight, no fees) · equity $11,473.40 vs prior close $11,337.67 (+135.73) because holdings re-marked: MRNA×8 yday $139.27 → 09:30 $141.19 +15.36; CYPH×1138 yday $1.64 → 09:30 $1.70 +68.28; AZI×955 yday $1.40 → 09:30 $1.33 -66.85; BTGO×198 yday $6.97 → 09:30 $6.89 -15.84; BNTX×12 yday $111.34 → 09:30 $113.13 +21.48; AUTL×530 yday $2.38 → 09:30 $2.32 -31.80; ASST×81 yday $19.82 → 09:30 $20.90 +87.48; BRR×629 yday $2.16 → 09:30 $2.25 +56.61; CAPR×1 yday $7.05 → 09:30 $6.79 -0.26; ARCT×1 yday $13.76 → 09:30 $14.34 +0.58; IOVA×1 yday $8.22 → 09:30 $8.00 -0.22; INO×10 yday $1.22 → 09:30 $1.25 +0.30; CAN×43 yday $0.37 → 09:30 $0.38 +0.43; INDP×9 yday $1.16 → 09:30 $1.18 +0.18 | — |
| 2026-08-25 09:30 ET | **SELL** | `MRNA` | 8 | $141.19 | $2.03 | $-75.65 | $1,138.50 | ▼ -75.65 after sell → book $11,471.37; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `AZI` | 955 | $1.33 | $12.49 | $-63.01 | $2,396.16 | ▼ -63.01 after sell → book $11,458.88; vs 09:30 mark -12.49 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BTGO` | 198 | $6.89 | $2.63 | $+51.22 | $3,757.75 | ▲ +51.22 after sell → book $11,456.25; vs 09:30 mark -2.63 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BNTX` | 12 | $113.13 | $2.05 | $+44.77 | $5,113.27 | ▲ +44.77 after sell → book $11,454.21; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `AUTL` | 530 | $2.32 | $6.93 | $-93.27 | $6,335.93 | ▼ -93.27 after sell → book $11,447.27; vs 09:30 mark -6.94 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BRR` | 629 | $2.25 | $8.23 | $+90.59 | $7,742.95 | ▲ +90.59 after sell → book $11,439.04; vs 09:30 mark -8.23 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `DFDV` | 300 | $4.29 | $3.87 | — | $6,452.08 | — | rank by ret_5; rank ret_5; list yday_gainer; 🔵; ret5=+28.3; leftover $1290.49 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BMNR` | 52 | $24.73 | $2.15 | — | $5,163.98 | — | rank by ret_5; rank ret_5; list yday_gainer; ret5=+26.3; leftover $1290.49 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟡 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 10 | $119.46 | $2.02 | — | $3,967.36 | — | rank by ret_5; rank ret_5; list mover_buy; 🔵; ⚪; ret5=+25.9; leftover $1290.49 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `RUM` | 137 | $9.36 | $2.40 | — | $2,682.64 | — | rank by ret_5; rank ret_5; list yday_gainer; 🔵; ret5=+21.3; leftover $1290.49 | join🔴 sector🟡 gen🟡 news🟢 digest🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `HMY` | 56 | $22.65 | $2.16 | — | $1,412.08 | — | rank by ret_5; rank ret_5; list mover_buy; ⚪; ret5=+21.1; leftover $1290.49 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `FWDI` | 215 | $5.99 | $2.77 | — | $121.45 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ret5=+20.7; leftover $1290.49 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $121.45 | ▲ 09:30 equity $11,185.95 vs yday $11,185.95 (+0.00) | 09:30 open · cash $121.45 (unchanged overnight, no fees) · equity $11,185.95 vs prior close $11,185.95 (+0.00) because holdings re-marked: CYPH×1138 yday $1.64 → 09:30 $1.64 +0.00; ASST×81 yday $20.20 → 09:30 $20.20 +0.00; CAPR×1 yday $7.19 → 09:30 $7.19 +0.00; ARCT×1 yday $14.21 → 09:30 $14.21 +0.00; IOVA×1 yday $8.08 → 09:30 $8.08 +0.00; INO×10 yday $1.25 → 09:30 $1.25 +0.00; CAN×43 yday $0.36 → 09:30 $0.36 +0.00; INDP×9 yday $1.25 → 09:30 $1.25 +0.00; DFDV×300 yday $4.16 → 09:30 $4.16 +0.00; BMNR×52 yday $24.21 → 09:30 $24.21 +0.00; AU×10 yday $118.55 → 09:30 $118.55 +0.00; RUM×137 yday $9.35 → 09:30 $9.35 +0.00; HMY×56 yday $22.50 → 09:30 $22.50 +0.00; FWDI×215 yday $5.86 → 09:30 $5.86 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $121.45 | ▼ 09:30 equity $11,372.82 vs yday $11,423.67 (-50.85) | 09:30 open · cash $121.45 (unchanged overnight, no fees) · equity $11,372.82 vs prior close $11,423.67 (-50.85) because holdings re-marked: CYPH×1138 yday $1.64 → 09:30 $1.60 -45.52; ASST×81 yday $20.20 → 09:30 $20.72 +42.12; CAPR×1 yday $7.19 → 09:30 $8.29 +1.10; ARCT×1 yday $14.21 → 09:30 $15.35 +1.14; IOVA×1 yday $8.08 → 09:30 $8.34 +0.26; INO×10 yday $1.25 → 09:30 $1.28 +0.30; CAN×43 yday $0.36 → 09:30 $0.40 +1.72; INDP×9 yday $1.25 → 09:30 $1.09 -1.44; DFDV×300 yday $4.16 → 09:30 $4.35 +57.00; BMNR×52 yday $24.21 → 09:30 $24.24 +1.56; AU×10 yday $118.55 → 09:30 $119.80 +12.50; RUM×137 yday $9.35 → 09:30 $10.07 +98.64; HMY×56 yday $22.50 → 09:30 $22.39 -6.16; FWDI×215 yday $5.86 → 09:30 $5.97 +23.65 | — |
| 2026-08-27 09:30 ET | **SELL** | `CYPH` | 1138 | $1.60 | $14.88 | $+482.54 | $1,927.37 | ▲ +482.54 after sell → book $11,357.94; vs 09:30 mark -14.88 | dropped from list after 5 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `ASST` | 81 | $20.72 | $2.26 | $+377.83 | $3,603.43 | ▲ +377.83 after sell → book $11,355.68; vs 09:30 mark -2.26 | dropped from list after 5 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `CAPR` | 1 | $8.29 | $0.11 | $+1.30 | $3,611.61 | ▲ +1.30 after sell → book $11,355.57; vs 09:30 mark -0.11 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `ARCT` | 1 | $15.35 | $0.18 | $+3.93 | $3,626.79 | ▲ +3.93 after sell → book $11,355.40; vs 09:30 mark -0.17 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `IOVA` | 1 | $8.34 | $0.11 | $-0.94 | $3,635.02 | ▼ -0.94 after sell → book $11,355.29; vs 09:30 mark -0.11 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `INO` | 10 | $1.28 | $0.18 | $+0.17 | $3,647.64 | ▲ +0.17 after sell → book $11,355.11; vs 09:30 mark -0.18 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `CAN` | 43 | $0.40 | $0.32 | $+3.98 | $3,664.52 | ▲ +3.98 after sell → book $11,354.79; vs 09:30 mark -0.32 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `INDP` | 9 | $1.09 | $0.15 | $-3.00 | $3,674.19 | ▼ -3.00 after sell → book $11,354.65; vs 09:30 mark -0.14 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **BUY** | `MOS` | 18 | $24.84 | $2.04 | — | $3,225.02 | — | rank by ret_5; rank ret_5; list flatten; ret5=+13.0; leftover $459.27 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟢 |
| 2026-08-27 09:30 ET | **BUY** | `DLO` | 29 | $15.60 | $2.08 | — | $2,770.55 | — | rank by ret_5; rank ret_5; list mover_buy; 🔵; ret5=+7.1; leftover $459.27 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MRVL` | 1 | $240.00 | $1.99 | — | $2,528.55 | — | rank by ret_5; rank ret_5; list mover_buy; 🔵; ret5=+6.8; leftover $459.27 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 177 | $2.59 | $2.52 | — | $2,067.60 | — | rank by ret_5; rank ret_5; list flatten; ret5=+4.2; leftover $459.27 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `PLTR` | 2 | $170.60 | $2.00 | — | $1,724.41 | — | rank by ret_5; rank ret_5; list mover_buy; 🔵; ret5=+3.4; leftover $459.27 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `TX` | 8 | $55.20 | $2.01 | — | $1,280.79 | — | rank by ret_5; rank ret_5; list mover_buy; 🔵; ret5=+3.0; leftover $459.27 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 11 | $40.72 | $2.02 | — | $830.85 | — | rank by ret_5; rank ret_5; list flatten; ret5=+1.8; leftover $459.27 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `GEN` | 15 | $28.89 | $2.04 | — | $395.46 | — | rank by ret_5; rank ret_5; list mover_buy; 🔵; ret5=+1.6; leftover $459.27 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $395.46 | ▲ 09:30 equity $11,488.80 vs yday $11,323.06 (+165.74) | 09:30 open · cash $395.46 (unchanged overnight, no fees) · equity $11,488.80 vs prior close $11,323.06 (+165.74) because holdings re-marked: DFDV×300 yday $4.50 → 09:30 $4.81 +93.00; BMNR×52 yday $24.91 → 09:30 $25.91 +52.00; AU×10 yday $118.11 → 09:30 $117.41 -7.00; RUM×137 yday $9.38 → 09:30 $9.51 +17.81; HMY×56 yday $22.43 → 09:30 $20.70 -96.88; FWDI×215 yday $5.93 → 09:30 $6.39 +98.90; MOS×18 yday $24.16 → 09:30 $24.00 -2.88; DLO×29 yday $15.36 → 09:30 $15.33 -0.87; MRVL×1 yday $245.11 → 09:30 $253.44 +8.33; SLI×177 yday $2.61 → 09:30 $2.60 -1.77; PLTR×2 yday $177.50 → 09:30 $178.75 +2.50; TX×8 yday $55.13 → 09:30 $55.25 +0.96; RRC×11 yday $41.55 → 09:30 $41.44 -1.21; GEN×15 yday $29.64 → 09:30 $29.83 +2.85 | — |
| 2026-08-28 09:30 ET | **SELL** | `DFDV` | 300 | $4.81 | $3.93 | $+148.20 | $1,834.53 | ▲ +148.20 after sell → book $11,484.87; vs 09:30 mark -3.93 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `BMNR` | 52 | $25.91 | $2.17 | $+57.05 | $3,179.69 | ▲ +57.05 after sell → book $11,482.71; vs 09:30 mark -2.16 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `AU` | 10 | $117.41 | $2.04 | $-24.56 | $4,351.75 | ▼ -24.56 after sell → book $11,480.67; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `RUM` | 137 | $9.51 | $2.43 | $+15.71 | $5,652.18 | ▲ +15.71 after sell → book $11,478.23; vs 09:30 mark -2.44 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `HMY` | 56 | $20.70 | $2.18 | $-113.54 | $6,809.20 | ▼ -113.54 after sell → book $11,476.05; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `FWDI` | 215 | $6.39 | $2.82 | $+80.41 | $8,180.23 | ▲ +80.41 after sell → book $11,473.23; vs 09:30 mark -2.82 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `FIGR` | 27 | $37.42 | $2.07 | — | $7,167.82 | — | rank by ret_5; rank ret_5; list yday_mover; ret5=+24.4; leftover $1022.53 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 heat🟡 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `WPM` | 6 | $155.89 | $2.01 | — | $6,230.47 | — | rank by ret_5; rank ret_5; list ohlc_hot; ret5=+17.6; leftover $1022.53 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟡 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SCCO` | 4 | $214.82 | $2.00 | — | $5,369.19 | — | rank by ret_5; rank ret_5; list ohlc_hot; ret5=+17.0; leftover $1022.53 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `AMTX` | 546 | $1.87 | $7.04 | — | $4,341.13 | — | rank by ret_5; rank ret_5; list ohlc_hot; ret5=+16.9; leftover $1022.53 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SBSW` | 85 | $12.01 | $2.25 | — | $3,318.03 | — | rank by ret_5; rank ret_5; list ohlc_hot; ret5=+16.8; leftover $1022.53 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟡 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `EQX` | 75 | $13.57 | $2.21 | — | $2,298.07 | — | rank by ret_5; rank ret_5; list ohlc_hot; ret5=+16.6; leftover $1022.53 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟡 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ERO` | 26 | $39.20 | $2.07 | — | $1,276.80 | — | rank by ret_5; rank ret_5; list ohlc_hot; ret5=+16.6; leftover $1022.53 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `TXG` | 15 | $64.10 | $2.04 | — | $313.27 | — | rank by ret_5; rank ret_5; list ohlc_hot; ret5=+16.1; leftover $1022.53 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $313.27 | ▼ 09:30 equity $11,213.72 vs yday $11,542.32 (-328.60) | 09:30 open · cash $313.27 (unchanged overnight, no fees) · equity $11,213.72 vs prior close $11,542.32 (-328.60) because holdings re-marked: MOS×18 yday $23.76 → 09:30 $23.75 -0.18; DLO×29 yday $15.14 → 09:30 $15.01 -3.77; MRVL×1 yday $241.45 → 09:30 $216.69 -24.76; SLI×177 yday $2.64 → 09:30 $2.51 -23.01; PLTR×2 yday $185.93 → 09:30 $184.04 -3.78; TX×8 yday $55.83 → 09:30 $54.84 -7.92; RRC×11 yday $41.64 → 09:30 $41.11 -5.83; GEN×15 yday $30.50 → 09:30 $31.02 +7.80; FIGR×27 yday $38.02 → 09:30 $35.50 -68.04; WPM×6 yday $157.99 → 09:30 $152.49 -33.00; SCCO×4 yday $216.28 → 09:30 $207.95 -33.32; AMTX×546 yday $1.87 → 09:30 $1.90 +16.38; SBSW×85 yday $12.26 → 09:30 $12.14 -10.20; EQX×75 yday $13.45 → 09:30 $12.81 -48.00; ERO×26 yday $39.82 → 09:30 $38.60 -31.72; TXG×15 yday $64.85 → 09:30 $60.90 -59.25 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $313.27 | ▼ 09:30 equity $11,233.63 vs yday $11,236.49 (-2.86) | 09:30 open · cash $313.27 (unchanged overnight, no fees) · equity $11,233.63 vs prior close $11,236.49 (-2.86) because holdings re-marked: MOS×18 yday $23.78 → 09:30 $24.00 +3.96; DLO×29 yday $15.00 → 09:30 $14.88 -3.48; MRVL×1 yday $216.35 → 09:30 $210.57 -5.78; SLI×177 yday $2.51 → 09:30 $2.70 +33.63; PLTR×2 yday $183.80 → 09:30 $185.52 +3.44; TX×8 yday $54.84 → 09:30 $54.82 -0.16; RRC×11 yday $41.78 → 09:30 $41.32 -5.06; GEN×15 yday $31.02 → 09:30 $30.56 -6.90; FIGR×27 yday $36.41 → 09:30 $36.80 +10.53; WPM×6 yday $152.25 → 09:30 $150.78 -8.82; SCCO×4 yday $209.00 → 09:30 $210.05 +4.20; AMTX×546 yday $1.90 → 09:30 $1.87 -16.38; SBSW×85 yday $12.04 → 09:30 $11.92 -10.20; EQX×75 yday $12.71 → 09:30 $12.78 +5.25; ERO×26 yday $38.49 → 09:30 $37.30 -30.94; TXG×15 yday $61.40 → 09:30 $62.99 +23.85 | — |
| 2026-09-01 09:30 ET | **SELL** | `MOS` | 18 | $24.00 | $2.06 | $-19.23 | $743.20 | ▼ -19.23 after sell → book $11,231.56; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `DLO` | 29 | $14.88 | $2.10 | $-25.05 | $1,172.63 | ▼ -25.05 after sell → book $11,229.47; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `MRVL` | 1 | $210.57 | $2.01 | $-33.44 | $1,381.18 | ▼ -33.44 after sell → book $11,227.45; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `SLI` | 177 | $2.70 | $2.56 | $+14.39 | $1,856.52 | ▲ +14.39 after sell → book $11,224.89; vs 09:30 mark -2.56 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `PLTR` | 2 | $185.52 | $2.02 | $+25.83 | $2,225.55 | ▲ +25.83 after sell → book $11,222.88; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `TX` | 8 | $54.82 | $2.03 | $-7.09 | $2,662.07 | ▼ -7.09 after sell → book $11,220.84; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `RRC` | 11 | $41.32 | $2.04 | $+2.53 | $3,114.55 | ▲ +2.53 after sell → book $11,218.80; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `GEN` | 15 | $30.56 | $2.06 | $+20.96 | $3,570.89 | ▲ +20.96 after sell → book $11,216.74; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3,570.89 | ▼ 09:30 equity $10,999.34 vs yday $11,047.23 (-47.89) | 09:30 open · cash $3,570.89 (unchanged overnight, no fees) · equity $10,999.34 vs prior close $11,047.23 (-47.89) because holdings re-marked: FIGR×27 yday $35.70 → 09:30 $35.46 -6.48; WPM×6 yday $146.46 → 09:30 $146.00 -2.76; SCCO×4 yday $205.00 → 09:30 $203.10 -7.60; AMTX×546 yday $1.87 → 09:30 $1.88 +5.46; SBSW×85 yday $11.56 → 09:30 $11.41 -12.75; EQX×75 yday $12.40 → 09:30 $12.33 -5.25; ERO×26 yday $36.01 → 09:30 $35.95 -1.56; TXG×15 yday $62.92 → 09:30 $61.79 -16.95 | — |
| 2026-09-02 09:30 ET | **SELL** | `FIGR` | 27 | $35.46 | $2.09 | $-57.08 | $4,526.22 | ▼ -57.08 after sell → book $10,997.25; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `AMTX` | 546 | $1.88 | $7.14 | $-8.73 | $5,545.56 | ▼ -8.73 after sell → book $10,990.11; vs 09:30 mark -7.14 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `ERO` | 26 | $35.95 | $2.09 | $-88.66 | $6,478.17 | ▼ -88.66 after sell → book $10,988.02; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | join🟢 sector🔴 gen🔴 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-02 09:30 ET | **SELL** | `TXG` | 15 | $61.79 | $2.06 | $-38.74 | $7,402.97 | ▼ -38.74 after sell → book $10,985.97; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | join🟢 sector🟡 gen🔴 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,402.97 | ▲ 09:30 equity $11,106.26 vs yday $10,983.59 (+122.67) | 09:30 open · cash $7,402.97 (unchanged overnight, no fees) · equity $11,106.26 vs prior close $10,983.59 (+122.67) because holdings re-marked: WPM×6 yday $144.93 → 09:30 $148.89 +23.76; SCCO×4 yday $201.61 → 09:30 $204.50 +11.56; SBSW×85 yday $11.66 → 09:30 $12.37 +60.35; EQX×75 yday $12.18 → 09:30 $12.54 +27.00 | — |
| 2026-09-03 09:30 ET | **SELL** | `SBSW` | 85 | $12.37 | $2.27 | $+26.09 | $8,452.15 | ▲ +26.09 after sell → book $11,103.99; vs 09:30 mark -2.27 | dropped from list after 4 sess (min 3) | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **SELL** | `EQX` | 75 | $12.54 | $2.24 | $-81.70 | $9,390.41 | ▼ -81.70 after sell → book $11,101.75; vs 09:30 mark -2.24 | dropped from list after 4 sess (min 3) | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🔴 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `MRNA` | 10 | $151.40 | $2.02 | — | $7,874.39 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ret5=+129.2; leftover $1565.07 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 95 | $16.46 | $2.27 | — | $6,308.41 | — | rank by ret_5; rank ret_5; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+63.4; leftover $1565.07 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CAN` | 5216 | $0.30 | $31.30 | — | $4,712.32 | — | rank by ret_5; rank ret_5; list yday_mover; 🔵; ret5=+54.3; leftover $1565.07 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ALEC` | 652 | $2.40 | $8.41 | — | $3,139.11 | — | rank by ret_5; rank ret_5; list yday_gainer; 🔵; ret5=+20.4; leftover $1565.07 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DEFT` | 2335 | $0.67 | $22.65 | — | $1,552.01 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+17.6; leftover $1565.07 | join🟡 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FUTU` | 12 | $119.46 | $2.03 | — | $116.46 | — | rank by ret_5; rank ret_5; list ohlc_hot; 🔵; ret5=+17.5; leftover $1565.07 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $116.46 | ▲ 09:30 equity $11,407.88 vs yday $11,262.47 (+145.41) | 09:30 open · cash $116.46 (unchanged overnight, no fees) · equity $11,407.88 vs prior close $11,262.47 (+145.41) because holdings re-marked: WPM×6 yday $150.91 → 09:30 $155.85 +29.64; SCCO×4 yday $204.26 → 09:30 $208.45 +16.76; MRNA×10 yday $150.81 → 09:30 $145.95 -48.60; ARCT×95 yday $16.74 → 09:30 $16.77 +2.85; CAN×5216 yday $0.31 → 09:30 $0.34 +156.48; ALEC×652 yday $2.72 → 09:30 $2.70 -13.04; DEFT×2335 yday $0.65 → 09:30 $0.65 +0.00; FUTU×12 yday $118.08 → 09:30 $118.19 +1.32 | — |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 7 | $5.08 | $0.38 | — | $80.53 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+28.1; leftover $38.82 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BRR` | 16 | $2.36 | $0.43 | — | $42.34 | — | rank by ret_5; rank ret_5; list yday_gainer; 🔵; ⚪; ret5=+28.0; leftover $38.82 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `HQ` | 2 | $17.06 | $0.35 | — | $7.87 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; ret5=+17.3; leftover $38.82 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 ab🟡 heat🔴 vol🔴 buy🟡 |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `TNDM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `INO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `IREN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `TPG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `VOR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `SLS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `TGTX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `BTSG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `QMCO` | cash | leftover split 14.25 < 1 share @ 24.68 |
| 2026-08-14 | `ARX` | cash | leftover split 14.25 < 1 share @ 19.57 |
| 2026-08-14 | `BRUN` | cash | leftover split 14.25 < 1 share @ 26.25 |
| 2026-08-14 | `SNDK` | cash | leftover split 14.25 < 1 share @ 1646.93 |
| 2026-08-14 | `TBBB` | cash | leftover split 14.25 < 1 share @ 48.82 |
| 2026-08-17 | `TNDM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `INO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `IREN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `TPG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `VOR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `SLS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `TGTX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `BTSG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `ZENA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `AIRO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `BCAR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `STDN` | cash | leftover split 9.64 < 1 share @ 13.64 |
| 2026-08-17 | `HTFL` | cash | leftover split 9.64 < 1 share @ 41.23 |
| 2026-08-17 | `UMAC` | cash | leftover split 9.64 < 1 share @ 32.55 |
| 2026-08-17 | `SMJF` | cash | leftover split 9.64 < 1 share @ 10.10 |
| 2026-08-18 | `ZENA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `AIRO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `BCAR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `XHG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `KOPN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `NPWR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `AVAH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FDMT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AXTI` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FIGR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CRDL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `QTRX` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ALM` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `XHG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `CAPR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `KOPN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `NPWR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `ARX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `WEAV` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `NMAX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ANGX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `IVVD` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DVLT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `AZI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `BTGO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `BNTX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AUTL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `ASST` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `BRR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `MRNA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `CYPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AZI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BTGO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BNTX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AUTL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ASST` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BRR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `CAPR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `ARCT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `IOVA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `INO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `CAN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `INDP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `ALM` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HMY` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `KGC` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `TMC` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `GFI` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DEFT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SCCO` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SBSW` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `CAPR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `ARCT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `IOVA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `INO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `CAN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `INDP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `CYPH` | no_price | no 09:30 open — carry |
| 2026-08-26 | `ASST` | no_price | no 09:30 open — carry |
| 2026-08-26 | `CAPR` | no_price | no 09:30 open — carry |
| 2026-08-26 | `ARCT` | no_price | no 09:30 open — carry |
| 2026-08-26 | `IOVA` | no_price | no 09:30 open — carry |
| 2026-08-26 | `INO` | no_price | no 09:30 open — carry |
| 2026-08-26 | `CAN` | no_price | no 09:30 open — carry |
| 2026-08-26 | `DFDV` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `BMNR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `AU` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `RUM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `HMY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `FWDI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `USDE` | no_price | no 09:30 open |
| 2026-08-26 | `BRR` | no_price | no 09:30 open |
| 2026-08-26 | `SENS` | no_price | no 09:30 open |
| 2026-08-26 | `SUJA` | no_price | no 09:30 open |
| 2026-08-26 | `DEFT` | no_price | no 09:30 open |
| 2026-08-26 | `WPM` | no_price | no 09:30 open |
| 2026-08-26 | `FUTU` | no_price | no 09:30 open |
| 2026-08-27 | `DFDV` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `BMNR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `AU` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `RUM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `HMY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `FWDI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `MOS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `DLO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `MRVL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `SLI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `PLTR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `TX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `RRC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `GEN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `MOS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `DLO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `MRVL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `SLI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `PLTR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `TX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `RRC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `GEN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `FIGR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `WPM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SCCO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `AMTX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SBSW` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `EQX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `ERO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `TXG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `USDE` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CYPH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PURR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CAN` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `DFDV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `MSTR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `FWDI` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `MPC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `FIGR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `WPM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SCCO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `AMTX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `EQX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `ERO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `CYPH` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `ALEC` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `SUJA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `KOS` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `TRLV` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `GUTS` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `USDE` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PRQR` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `SUJA` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `SSRM` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-04 | `MRNA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `ARCT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CAN` | min_hold | dropped but min-hold 1/3 sess — no sell |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `WPM` | 6 | 2026-08-28 @ $155.89 | rank by ret_5; rank ret_5; list ohlc_hot; ret5=+17.6; leftover $1022.53 |
| `SCCO` | 4 | 2026-08-28 @ $214.82 | rank by ret_5; rank ret_5; list ohlc_hot; ret5=+17.0; leftover $1022.53 |
| `MRNA` | 10 | 2026-09-03 @ $151.40 | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ret5=+129.2; leftover $1565.07 |
| `ARCT` | 95 | 2026-09-03 @ $16.46 | rank by ret_5; rank ret_5; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+63.4; leftover $1565.07 |
| `CAN` | 5216 | 2026-09-03 @ $0.30 | rank by ret_5; rank ret_5; list yday_mover; 🔵; ret5=+54.3; leftover $1565.07 |
| `ALEC` | 652 | 2026-09-03 @ $2.40 | rank by ret_5; rank ret_5; list yday_gainer; 🔵; ret5=+20.4; leftover $1565.07 |
| `DEFT` | 2335 | 2026-09-03 @ $0.67 | rank by ret_5; rank ret_5; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+17.6; leftover $1565.07 |
| `FUTU` | 12 | 2026-09-03 @ $119.46 | rank by ret_5; rank ret_5; list ohlc_hot; 🔵; ret5=+17.5; leftover $1565.07 |
| `OABI` | 7 | 2026-09-04 @ $5.08 | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+28.1; leftover $38.82 |
| `BRR` | 16 | 2026-09-04 @ $2.36 | rank by ret_5; rank ret_5; list yday_gainer; 🔵; ⚪; ret5=+28.0; leftover $38.82 |
| `HQ` | 2 | 2026-09-04 @ $17.06 | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; ret5=+17.3; leftover $38.82 |
