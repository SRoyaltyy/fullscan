# Factor mine action — `union_hot_n12_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 12 · rank `hot_score` · size `leftover` · sell `list` · S-boost `none` · top 12 by hot

Cash book **-3.12%** ($9,688) · signal-only (no cash/fees) was -2.24%. Starts YES **6/17**. Fills 200 · skips 83 · realized $-249.73.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `hot_score` · **top_n** 12.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $96.86.

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | vs yday close | Bought | Sold | Close cash | Close equity | Close held | 09:30 why |
|---|---:|---:|---|---:|---:|---|---|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | IREN, TNDM, TPG, INO, HIMS, SLS, VOR, BTSG, TGTX | — | $123.82 | $10,195.74 | IREN×24, TNDM×47, TPG×21, INO×1371, HIMS×37, SLS×94, VOR×50, BTSG×18, TGTX×22 | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-14 | +5.50 | $123.82 | IREN×24, TNDM×47, TPG×21, INO×1371, HIMS×37, SLS×94, VOR×50, BTSG×18, TGTX×22 | $10,219.63 | +23.89 | QMCO, ARX, ZENA, AIRO, LIFE, BZAI, VOYG, LUNR, TBBB, BRUN, BETA, FORM | IREN, TNDM, TPG, INO, HIMS, SLS, VOR, BTSG, TGTX | $118.02 | $9,681.67 | QMCO×34, ARX×43, ZENA×385, AIRO×76, LIFE×24, BZAI×1108, VOYG×19, LUNR×44, TBBB×17, BRUN×32, BETA×33, FORM×6 | 09:30 open · cash $123.82 (unchanged overnight, no fees) · equity $10,219.63 vs prior close $10,195.74 (+23.89) because holdings re-marked: IREN×24 yday $44.76 → 09:30 $44.09 -16.08; TNDM×47 yday $23.13 → 09:30 $22.92 -9.87; TPG×21 yday $54.62 → 09:30 $55.29 +14.07; INO×1371 yday $0.90 → 09:30 $0.93 +41.13; HIMS×37 yday $28.77 → 09:30 $29.15 +14.06; SLS×94 yday $12.36 → 09:30 $12.40 +3.76; VOR×50 yday $23.29 → 09:30 $23.33 +2.00; BTSG×18 yday $60.23 → 09:30 $59.65 -10.44; TGTX×22 yday $47.94 → 09:30 $47.27 -14.74 |
| 2026-08-17 | +2.25 | $118.02 | QMCO×34, ARX×43, ZENA×385, AIRO×76, LIFE×24, BZAI×1108, VOYG×19, LUNR×44, TBBB×17, BRUN×32, BETA×33, FORM×6 | $9,611.47 | -70.20 | XHG, CAPR, STDN, HTFL, UMAC, SMJF, ALOY, NPWR, NMAX, LPTH, INDI, KOPN | QMCO, ARX, ZENA, AIRO, LIFE, BZAI, VOYG, LUNR, TBBB, BRUN, BETA, FORM | $40.88 | $9,344.54 | XHG×190, CAPR×116, STDN×58, HTFL×19, UMAC×24, SMJF×79, ALOY×54, NPWR×415, NMAX×72, LPTH×53, INDI×171, KOPN×146 | 09:30 open · cash $118.02 (unchanged overnight, no fees) · equity $9,611.47 vs prior close $9,681.67 (-70.20) because holdings re-marked: QMCO×34 yday $26.11 → 09:30 $24.83 -43.52; ARX×43 yday $19.58 → 09:30 $19.57 -0.43; ZENA×385 yday $2.14 → 09:30 $2.08 -21.18; AIRO×76 yday $9.57 → 09:30 $9.57 +0.00; LIFE×24 yday $34.02 → 09:30 $34.03 +0.24; BZAI×1108 yday $0.59 → 09:30 $0.55 -45.43; VOYG×19 yday $42.98 → 09:30 $42.12 -16.34; LUNR×44 yday $19.01 → 09:30 $20.25 +54.56; TBBB×17 yday $47.79 → 09:30 $47.39 -6.80; BRUN×32 yday $22.93 → 09:30 $23.00 +2.24; BETA×33 yday $24.86 → 09:30 $24.61 -8.25; FORM×6 yday $131.60 → 09:30 $134.05 +14.70 |
| 2026-08-18 | -6.20 | $40.88 | XHG×190, CAPR×116, STDN×58, HTFL×19, UMAC×24, SMJF×79, ALOY×54, NPWR×415, NMAX×72, LPTH×53, INDI×171, KOPN×146 | $9,134.74 | -209.80 | — | XHG, STDN, HTFL, UMAC, SMJF, ALOY, NPWR, NMAX, LPTH, INDI, KOPN | $8,236.55 | $9,057.83 | CAPR×116 | 09:30 open · cash $40.88 (unchanged overnight, no fees) · equity $9,134.74 vs prior close $9,344.54 (-209.80) because holdings re-marked: XHG×190 yday $3.91 → 09:30 $3.94 +5.70; CAPR×116 yday $7.45 → 09:30 $7.50 +5.80; STDN×58 yday $13.31 → 09:30 $13.31 +0.00; HTFL×19 yday $41.94 → 09:30 $41.50 -8.36; UMAC×24 yday $30.15 → 09:30 $28.59 -37.44; SMJF×79 yday $10.45 → 09:30 $10.45 +0.00; ALOY×54 yday $13.86 → 09:30 $13.19 -35.91; NPWR×415 yday $1.73 → 09:30 $1.70 -12.45; NMAX×72 yday $10.36 → 09:30 $10.31 -3.60; LPTH×53 yday $14.80 → 09:30 $14.01 -41.87; INDI×171 yday $4.71 → 09:30 $4.48 -39.33; KOPN×146 yday $5.32 → 09:30 $5.03 -42.34 |
| 2026-08-19 | -7.20 | $8,236.55 | CAPR×116 | $9,070.59 | +12.76 | — | CAPR | $9,068.22 | $9,068.22 | — | 09:30 open · cash $8,236.55 (unchanged overnight, no fees) · equity $9,070.59 vs prior close $9,057.83 (+12.76) because holdings re-marked: CAPR×116 yday $7.08 → 09:30 $7.19 +12.76 |
| 2026-08-20 | +1.12 | $9,068.22 | — | $9,068.22 | +0.00 | MRNA, CYPH, ABCL, AZI, SENS, ALEC, BTGO, AUTL, BNTX, TEAM, BBNX, EMBC | — | $168.15 | $8,954.63 | MRNA×5, CYPH×657, ABCL×63, AZI×551, SENS×84, ALEC×314, BTGO×114, AUTL×305, BNTX×6, TEAM×4, BBNX×37, EMBC×142 | 09:30 open · cash $9,068.22 · no holdings · equity $9,068.22 vs prior close $9,068.22 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-21 | +3.25 | $168.15 | MRNA×5, CYPH×657, ABCL×63, AZI×551, SENS×84, ALEC×314, BTGO×114, AUTL×305, BNTX×6, TEAM×4, BBNX×37, EMBC×142 | $9,127.00 | +172.37 | XHG, CAPR, ARCT, IOVA, CAN, TEM, INO, DFDV, XXI, AU | ABCL, AZI, SENS, ALEC, BTGO, AUTL, BNTX, TEAM, BBNX, EMBC | $56.31 | $9,403.79 | MRNA×5, CYPH×657, XHG×168, CAPR×111, ARCT×67, IOVA×83, CAN×2572, TEM×11, INO×614, DFDV×187, XXI×117, AU×6 | 09:30 open · cash $168.15 (unchanged overnight, no fees) · equity $9,127.00 vs prior close $8,954.63 (+172.37) because holdings re-marked: MRNA×5 yday $133.32 → 09:30 $133.11 -1.05; CYPH×657 yday $1.19 → 09:30 $1.32 +85.41; ABCL×63 yday $11.57 → 09:30 $11.57 +0.00; AZI×551 yday $1.44 → 09:30 $1.46 +11.02; SENS×84 yday $8.82 → 09:30 $9.24 +35.28; ALEC×314 yday $2.26 → 09:30 $2.28 +6.28; BTGO×114 yday $6.60 → 09:30 $6.95 +39.90; AUTL×305 yday $2.46 → 09:30 $2.47 +3.05; BNTX×6 yday $110.89 → 09:30 $110.92 +0.18; TEAM×4 yday $174.91 → 09:30 $174.22 -2.76; BBNX×37 yday $19.48 → 09:30 $19.50 +0.74; EMBC×142 yday $5.47 → 09:30 $5.43 -5.68 |
| 2026-08-24 | -5.17 | $56.31 | MRNA×5, CYPH×657, XHG×168, CAPR×111, ARCT×67, IOVA×83, CAN×2572, TEM×11, INO×614, DFDV×187, XXI×117, AU×6 | $9,886.30 | +482.51 | — | MRNA, CYPH, XHG, CAPR, ARCT, IOVA, CAN, TEM, INO, DFDV, XXI, AU | $9,831.32 | $9,831.32 | — | 09:30 open · cash $56.31 (unchanged overnight, no fees) · equity $9,886.30 vs prior close $9,403.79 (+482.51) because holdings re-marked: MRNA×5 yday $145.13 → 09:30 $142.70 -12.15; CYPH×657 yday $1.42 → 09:30 $1.83 +269.37; XHG×168 yday $4.41 → 09:30 $4.24 -28.56; CAPR×111 yday $6.29 → 09:30 $8.01 +190.92; ARCT×67 yday $13.45 → 09:30 $13.26 -12.73; IOVA×83 yday $8.29 → 09:30 $8.05 -19.92; CAN×2572 yday $0.35 → 09:30 $0.38 +64.30; TEM×11 yday $72.69 → 09:30 $70.07 -28.82; INO×614 yday $1.18 → 09:30 $1.20 +12.28; DFDV×187 yday $3.94 → 09:30 $4.15 +39.27; XXI×117 yday $6.49 → 09:30 $6.60 +12.87; AU×6 yday $121.22 → 09:30 $120.50 -4.32 |
| 2026-08-25 | +1.80 | $9,831.32 | — | $9,831.32 | +0.00 | CYPH, XHG, ASST, AU, RUM, BMNR, NIQ, DEFT, OMER, HMY, DFDV, ERO | — | $139.13 | $9,681.30 | CYPH×481, XHG×203, ASST×39, AU×6, RUM×87, BMNR×33, NIQ×41, DEFT×1280, OMER×43, HMY×36, DFDV×190, ERO×21 | 09:30 open · cash $9,831.32 · no holdings · equity $9,831.32 vs prior close $9,831.32 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-26 | +2.02 | $139.13 | CYPH×481, XHG×203, ASST×39, AU×6, RUM×87, BMNR×33, NIQ×41, DEFT×1280, OMER×43, HMY×36, DFDV×190, ERO×21 | $9,681.30 | +0.00 | — | — | $139.13 | $9,791.07 | CYPH×481, XHG×203, ASST×39, AU×6, RUM×87, BMNR×33, NIQ×41, DEFT×1280, OMER×43, HMY×36, DFDV×190, ERO×21 | 09:30 open · cash $139.13 (unchanged overnight, no fees) · equity $9,681.30 vs prior close $9,681.30 (+0.00) because holdings re-marked: CYPH×481 yday $1.64 → 09:30 $1.64 +0.00; XHG×203 yday $4.05 → 09:30 $4.05 +0.00; ASST×39 yday $20.20 → 09:30 $20.20 +0.00; AU×6 yday $118.55 → 09:30 $118.55 +0.00; RUM×87 yday $9.35 → 09:30 $9.35 +0.00; BMNR×33 yday $24.21 → 09:30 $24.21 +0.00; NIQ×41 yday $19.46 → 09:30 $19.46 +0.00; DEFT×1280 yday $0.62 → 09:30 $0.62 +0.00; OMER×43 yday $19.03 → 09:30 $19.03 +0.00; HMY×36 yday $22.50 → 09:30 $22.50 +0.00; DFDV×190 yday $4.16 → 09:30 $4.16 +0.00; ERO×21 yday $38.55 → 09:30 $38.55 +0.00 |
| 2026-08-27 | — | $139.13 | CYPH×481, XHG×203, ASST×39, AU×6, RUM×87, BMNR×33, NIQ×41, DEFT×1280, OMER×43, HMY×36, DFDV×190, ERO×21 | $9,738.78 | -52.29 | MOS, DLO, SLI, MRVL, CRK, PLTR, RRC, GEN, TX, PGY, ANET, NUE | CYPH, XHG, ASST, AU, RUM, BMNR, NIQ, DEFT, OMER, HMY, DFDV, ERO | $439.96 | $9,818.30 | MOS×32, DLO×51, SLI×312, MRVL×3, CRK×57, PLTR×4, RRC×19, GEN×27, TX×14, PGY×36, ANET×4, NUE×3 | 09:30 open · cash $139.13 (unchanged overnight, no fees) · equity $9,738.78 vs prior close $9,791.07 (-52.29) because holdings re-marked: CYPH×481 yday $1.64 → 09:30 $1.60 -19.24; XHG×203 yday $4.05 → 09:30 $3.81 -48.72; ASST×39 yday $20.20 → 09:30 $20.72 +20.28; AU×6 yday $118.55 → 09:30 $119.80 +7.50; RUM×87 yday $9.35 → 09:30 $10.07 +62.64; BMNR×33 yday $24.21 → 09:30 $24.24 +0.99; NIQ×41 yday $19.46 → 09:30 $19.20 -10.66; DEFT×1280 yday $0.62 → 09:30 $0.60 -25.60; OMER×43 yday $19.03 → 09:30 $18.96 -3.01; HMY×36 yday $22.50 → 09:30 $22.39 -3.96; DFDV×190 yday $4.16 → 09:30 $4.35 +36.10; ERO×21 yday $38.55 → 09:30 $40.51 +41.16 |
| 2026-08-28 | +0.75 | $439.96 | MOS×32, DLO×51, SLI×312, MRVL×3, CRK×57, PLTR×4, RRC×19, GEN×27, TX×14, PGY×36, ANET×4, NUE×3 | $9,869.60 | +51.30 | FIGR, NIQ, ERO, TRLV, CVI, VIRT, TXG, GUTS, WPM, AMTX, EGO, ZYME | MOS, DLO, SLI, MRVL, CRK, PLTR, RRC, GEN, TX, PGY, ANET, NUE | $245.52 | $9,861.10 | FIGR×21, NIQ×43, ERO×20, TRLV×72, CVI×20, VIRT×12, TXG×12, GUTS×1108, WPM×5, AMTX×438, EGO×17, ZYME×27 | 09:30 open · cash $439.96 (unchanged overnight, no fees) · equity $9,869.60 vs prior close $9,818.30 (+51.30) because holdings re-marked: MOS×32 yday $24.16 → 09:30 $24.00 -5.12; DLO×51 yday $15.36 → 09:30 $15.33 -1.53; SLI×312 yday $2.61 → 09:30 $2.60 -3.12; MRVL×3 yday $245.11 → 09:30 $253.44 +24.99; CRK×57 yday $14.50 → 09:30 $14.42 -4.56; PLTR×4 yday $177.50 → 09:30 $178.75 +5.00; RRC×19 yday $41.55 → 09:30 $41.44 -2.09; GEN×27 yday $29.64 → 09:30 $29.83 +5.13; TX×14 yday $55.13 → 09:30 $55.25 +1.68; PGY×36 yday $22.41 → 09:30 $22.93 +18.72; ANET×4 yday $202.25 → 09:30 $205.90 +14.60; NUE×3 yday $252.80 → 09:30 $252.00 -2.40 |
| 2026-08-31 | -5.85 | $245.52 | FIGR×21, NIQ×43, ERO×20, TRLV×72, CVI×20, VIRT×12, TXG×12, GUTS×1108, WPM×5, AMTX×438, EGO×17, ZYME×27 | $9,718.28 | -142.82 | — | FIGR, ERO, TRLV, VIRT, TXG, GUTS, WPM, AMTX, EGO, ZYME | $8,024.16 | $9,684.96 | NIQ×43, CVI×20 | 09:30 open · cash $245.52 (unchanged overnight, no fees) · equity $9,718.28 vs prior close $9,861.10 (-142.82) because holdings re-marked: FIGR×21 yday $38.02 → 09:30 $35.50 -52.92; NIQ×43 yday $19.07 → 09:30 $19.20 +5.59; ERO×20 yday $39.82 → 09:30 $38.60 -24.40; TRLV×72 yday $11.03 → 09:30 $12.41 +99.36; CVI×20 yday $39.76 → 09:30 $41.76 +40.00; VIRT×12 yday $67.04 → 09:30 $66.39 -7.80; TXG×12 yday $64.85 → 09:30 $60.90 -47.40; GUTS×1108 yday $0.74 → 09:30 $0.67 -77.56; WPM×5 yday $157.99 → 09:30 $152.49 -27.50; AMTX×438 yday $1.87 → 09:30 $1.90 +13.14; EGO×17 yday $48.03 → 09:30 $45.48 -43.35; ZYME×27 yday $29.01 → 09:30 $28.27 -19.98 |
| 2026-09-01 | -6.30 | $8,024.16 | NIQ×43, CVI×20 | $9,700.94 | +15.98 | — | NIQ, CVI | $9,696.73 | $9,696.73 | — | 09:30 open · cash $8,024.16 (unchanged overnight, no fees) · equity $9,700.94 vs prior close $9,684.96 (+15.98) because holdings re-marked: NIQ×43 yday $19.20 → 09:30 $19.06 -6.02; CVI×20 yday $41.76 → 09:30 $42.86 +22.00 |
| 2026-09-02 | -3.83 | $9,696.73 | — | $9,696.73 | +0.00 | — | — | $9,696.73 | $9,696.73 | — | 09:30 open · cash $9,696.73 · no holdings · equity $9,696.73 vs prior close $9,696.73 (+0.00). Cash unchanged overnight; no fees. |
| 2026-09-03 | -0.90 | $9,696.73 | — | $9,696.73 | +0.00 | MRNA, XHG, ARCT, CAN, NIQ, DEFT, OMER, ERO, TRLV, FUTU, CVI, VIRT | — | $208.88 | $9,526.37 | MRNA×5, XHG×226, ARCT×49, CAN×2693, NIQ×43, DEFT×1206, OMER×42, ERO×22, TRLV×68, FUTU×6, CVI×18, VIRT×12 | 09:30 open · cash $9,696.73 · no holdings · equity $9,696.73 vs prior close $9,696.73 (+0.00). Cash unchanged overnight; no fees. |
| 2026-09-04 | — | $208.88 | MRNA×5, XHG×226, ARCT×49, CAN×2693, NIQ×43, DEFT×1206, OMER×42, ERO×22, TRLV×68, FUTU×6, CVI×18, VIRT×12 | $9,653.96 | +127.59 | HQ, OABI, HOOD | MRNA, ARCT, CAN | $96.86 | $9,687.97 | XHG×226, NIQ×43, DEFT×1206, OMER×42, ERO×22, TRLV×68, FUTU×6, CVI×18, VIRT×12, HQ×51, OABI×174, HOOD×7 | 09:30 open · cash $208.88 (unchanged overnight, no fees) · equity $9,653.96 vs prior close $9,526.37 (+127.59) because holdings re-marked: MRNA×5 yday $150.81 → 09:30 $145.95 -24.30; XHG×226 yday $3.32 → 09:30 $3.38 +13.56; ARCT×49 yday $16.74 → 09:30 $16.77 +1.47; CAN×2693 yday $0.31 → 09:30 $0.34 +80.79; NIQ×43 yday $18.35 → 09:30 $18.66 +13.33; DEFT×1206 yday $0.65 → 09:30 $0.65 +0.00; OMER×42 yday $18.86 → 09:30 $18.99 +5.46; ERO×22 yday $34.76 → 09:30 $35.82 +23.32; TRLV×68 yday $11.69 → 09:30 $11.89 +13.60; FUTU×6 yday $118.08 → 09:30 $118.19 +0.66; CVI×18 yday $42.92 → 09:30 $42.45 -8.46; VIRT×12 yday $62.69 → 09:30 $63.37 +8.16 |

## Fills (09:30 open snapshot, then buys / sells)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 24 | $45.98 | $2.06 | — | $8,894.42 | — | top 12 by hot; rank hot_score; list flatten; ⚪; ret5=+12.3; leftover $1111.11 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 47 | $23.33 | $2.13 | — | $7,795.78 | — | top 12 by hot; rank hot_score; list flatten; ⚪; ret5=+19.7; leftover $1111.11 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 21 | $50.62 | $2.05 | — | $6,730.64 | — | top 12 by hot; rank hot_score; list flatten; ⚪; ret5=+6.2; leftover $1111.11 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 1371 | $0.81 | $15.22 | — | $5,604.91 | — | top 12 by hot; rank hot_score; list flatten; ⚪; ret5=+13.2; leftover $1111.11 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 37 | $29.74 | $2.10 | — | $4,502.43 | — | top 12 by hot; rank hot_score; list flatten; ⚪; ret5=-5.3; leftover $1111.11 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 94 | $11.70 | $2.27 | — | $3,400.36 | — | top 12 by hot; rank hot_score; list flatten; ⚪; ret5=-0.8; leftover $1111.11 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `VOR` | 50 | $22.01 | $2.14 | — | $2,297.72 | — | top 12 by hot; rank hot_score; list flatten; ⚪; ret5=+0.3; leftover $1111.11 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 18 | $59.80 | $2.04 | — | $1,219.27 | — | top 12 by hot; rank hot_score; list flatten; ⚪; ret5=-5.3; leftover $1111.11 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TGTX` | 22 | $49.70 | $2.06 | — | $123.82 | — | top 12 by hot; rank hot_score; list flatten; ⚪; ret5=-0.8; leftover $1111.11 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $123.82 | ▲ 09:30 equity $10,219.63 vs yday $10,195.74 (+23.89) | 09:30 open · cash $123.82 (unchanged overnight, no fees) · equity $10,219.63 vs prior close $10,195.74 (+23.89) because holdings re-marked: IREN×24 yday $44.76 → 09:30 $44.09 -16.08; TNDM×47 yday $23.13 → 09:30 $22.92 -9.87; TPG×21 yday $54.62 → 09:30 $55.29 +14.07; INO×1371 yday $0.90 → 09:30 $0.93 +41.13; HIMS×37 yday $28.77 → 09:30 $29.15 +14.06; SLS×94 yday $12.36 → 09:30 $12.40 +3.76; VOR×50 yday $23.29 → 09:30 $23.33 +2.00; BTSG×18 yday $60.23 → 09:30 $59.65 -10.44; TGTX×22 yday $47.94 → 09:30 $47.27 -14.74 | — |
| 2026-08-14 09:30 ET | **SELL** | `IREN` | 24 | $44.09 | $2.08 | $-49.50 | $1,179.89 | ▼ -49.50 after sell → book $10,217.54; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TNDM` | 47 | $22.92 | $2.15 | $-23.55 | $2,254.98 | ▼ -23.55 after sell → book $10,215.39; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TPG` | 21 | $55.29 | $2.07 | $+93.88 | $3,414.00 | ▲ +93.88 after sell → book $10,213.32; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **SELL** | `INO` | 1371 | $0.93 | $17.10 | $+132.20 | $4,671.93 | ▲ +132.20 after sell → book $10,196.22; vs 09:30 mark -17.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `HIMS` | 37 | $29.15 | $2.12 | $-26.05 | $5,748.36 | ▼ -26.05 after sell → book $10,194.10; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `SLS` | 94 | $12.40 | $2.30 | $+61.23 | $6,911.66 | ▲ +61.23 after sell → book $10,191.80; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `VOR` | 50 | $23.33 | $2.16 | $+61.70 | $8,076.00 | ▲ +61.70 after sell → book $10,189.64; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `BTSG` | 18 | $59.65 | $2.06 | $-6.81 | $9,147.64 | ▼ -6.81 after sell → book $10,187.58; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TGTX` | 22 | $47.27 | $2.08 | $-57.59 | $10,185.50 | ▼ -57.59 after sell → book $10,185.50; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **BUY** | `QMCO` | 34 | $24.68 | $2.09 | — | $9,344.29 | — | top 12 by hot; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+111.3; leftover $848.79 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 43 | $19.57 | $2.12 | — | $8,500.66 | — | top 12 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $848.79 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ZENA` | 385 | $2.20 | $4.97 | — | $7,648.69 | — | top 12 by hot; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+54.3; leftover $848.79 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `AIRO` | 76 | $11.12 | $2.22 | — | $6,801.35 | — | top 12 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; leftover $848.79 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `LIFE` | 24 | $35.04 | $2.06 | — | $5,958.33 | — | top 12 by hot; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+16.7; leftover $848.79 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BZAI` | 1108 | $0.77 | $11.81 | — | $5,097.79 | — | top 12 by hot; rank hot_score; list earn_react; 🔵; ⚪; ret5=+20.4; leftover $848.79 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `VOYG` | 19 | $44.49 | $2.05 | — | $4,250.44 | — | top 12 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+15.6; leftover $848.79 | join🟢 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `LUNR` | 44 | $19.17 | $2.12 | — | $3,404.83 | — | top 12 by hot; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+17.6; leftover $848.79 | join🟢 sector🟢 gen🟢 news🔴 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `TBBB` | 17 | $48.82 | $2.04 | — | $2,572.85 | — | top 12 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.0; leftover $848.79 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BRUN` | 32 | $26.25 | $2.09 | — | $1,730.93 | — | top 12 by hot; rank hot_score; list earn_react; 🔵; ⚪; ret5=+31.2; leftover $848.79 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BETA` | 33 | $25.21 | $2.09 | — | $896.91 | — | top 12 by hot; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+15.5; leftover $848.79 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `FORM` | 6 | $129.48 | $2.01 | — | $118.02 | — | top 12 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+14.3; leftover $848.79 | join🟢 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $118.02 | ▼ 09:30 equity $9,611.47 vs yday $9,681.67 (-70.20) | 09:30 open · cash $118.02 (unchanged overnight, no fees) · equity $9,611.47 vs prior close $9,681.67 (-70.20) because holdings re-marked: QMCO×34 yday $26.11 → 09:30 $24.83 -43.52; ARX×43 yday $19.58 → 09:30 $19.57 -0.43; ZENA×385 yday $2.14 → 09:30 $2.08 -21.18; AIRO×76 yday $9.57 → 09:30 $9.57 +0.00; LIFE×24 yday $34.02 → 09:30 $34.03 +0.24; BZAI×1108 yday $0.59 → 09:30 $0.55 -45.43; VOYG×19 yday $42.98 → 09:30 $42.12 -16.34; LUNR×44 yday $19.01 → 09:30 $20.25 +54.56; TBBB×17 yday $47.79 → 09:30 $47.39 -6.80; BRUN×32 yday $22.93 → 09:30 $23.00 +2.24; BETA×33 yday $24.86 → 09:30 $24.61 -8.25; FORM×6 yday $131.60 → 09:30 $134.05 +14.70 | — |
| 2026-08-17 09:30 ET | **SELL** | `QMCO` | 34 | $24.83 | $2.11 | $+0.90 | $960.13 | ▲ +0.90 after sell → book $9,609.36; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 43 | $19.57 | $2.14 | $-4.26 | $1,799.50 | ▼ -4.26 after sell → book $9,607.22; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ZENA` | 385 | $2.08 | $5.04 | $-54.28 | $2,597.18 | ▼ -54.28 after sell → book $9,602.18; vs 09:30 mark -5.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `AIRO` | 76 | $9.57 | $2.24 | $-122.26 | $3,322.26 | ▼ -122.26 after sell → book $9,599.94; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `LIFE` | 24 | $34.03 | $2.08 | $-28.38 | $4,136.90 | ▼ -28.38 after sell → book $9,597.86; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BZAI` | 1108 | $0.55 | $9.63 | $-258.56 | $4,738.88 | ▼ -258.56 after sell → book $9,588.22; vs 09:30 mark -9.64 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `VOYG` | 19 | $42.12 | $2.07 | $-49.14 | $5,537.10 | ▼ -49.14 after sell → book $9,586.16; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-17 09:30 ET | **SELL** | `LUNR` | 44 | $20.25 | $2.14 | $+43.26 | $6,425.95 | ▲ +43.26 after sell → book $9,584.01; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SELL** | `TBBB` | 17 | $47.39 | $2.06 | $-28.41 | $7,229.52 | ▼ -28.41 after sell → book $9,581.95; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BRUN` | 32 | $23.00 | $2.11 | $-108.03 | $7,963.42 | ▼ -108.03 after sell → book $9,579.85; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BETA` | 33 | $24.61 | $2.11 | $-24.00 | $8,773.44 | ▼ -24.00 after sell → book $9,577.74; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `FORM` | 6 | $134.05 | $2.03 | $+23.38 | $9,575.71 | ▲ +23.38 after sell → book $9,575.71; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `XHG` | 190 | $4.19 | $2.56 | — | $8,777.05 | — | top 12 by hot; rank hot_score; list yday_mover; ⚪; ret5=+291.8; leftover $797.98 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `CAPR` | 116 | $6.87 | $2.34 | — | $7,977.79 | — | top 12 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+62.6; leftover $797.98 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `STDN` | 58 | $13.64 | $2.16 | — | $7,184.51 | — | top 12 by hot; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+57.7; leftover $797.98 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `HTFL` | 19 | $41.23 | $2.05 | — | $6,399.09 | — | top 12 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+46.0; leftover $797.98 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `UMAC` | 24 | $32.55 | $2.06 | — | $5,615.83 | — | top 12 by hot; rank hot_score; list yday_gainer,yday_mover; ⚪; ret5=+30.4; leftover $797.98 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `SMJF` | 79 | $10.10 | $2.23 | — | $4,815.70 | — | top 12 by hot; rank hot_score; list mover_buy; ret5=+22.8; leftover $797.98 | join🔴 sector🔴 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ALOY` | 54 | $14.66 | $2.15 | — | $4,021.91 | — | top 12 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.0; leftover $797.98 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `NPWR` | 415 | $1.92 | $5.35 | — | $3,219.76 | — | top 12 by hot; rank hot_score; list yday_gainer,yday_mover; ⚪; ret5=+28.3; leftover $797.98 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `NMAX` | 72 | $10.97 | $2.21 | — | $2,427.71 | — | top 12 by hot; rank hot_score; list yday_gainer,yday_mover; ⚪; ret5=+21.2; leftover $797.98 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `LPTH` | 53 | $14.94 | $2.15 | — | $1,633.74 | — | top 12 by hot; rank hot_score; list yday_gainer,yday_mover,ohlc_hot; ⚪; ret5=+16.2; leftover $797.98 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `INDI` | 171 | $4.65 | $2.50 | — | $836.09 | — | top 12 by hot; rank hot_score; list ohlc_hot; ⚪; ret5=+16.6; leftover $797.98 | join🟡 sector🟢 gen🟢 news🟡 vol🟡 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `KOPN` | 146 | $5.43 | $2.43 | — | $40.88 | — | top 12 by hot; rank hot_score; list yday_gainer; ⚪; ret5=+28.8; leftover $797.98 | join🟡 sector🟢 gen🟢 news🟡 vol🟡 buy🟢 |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $40.88 | ▼ 09:30 equity $9,134.74 vs yday $9,344.54 (-209.80) | 09:30 open · cash $40.88 (unchanged overnight, no fees) · equity $9,134.74 vs prior close $9,344.54 (-209.80) because holdings re-marked: XHG×190 yday $3.91 → 09:30 $3.94 +5.70; CAPR×116 yday $7.45 → 09:30 $7.50 +5.80; STDN×58 yday $13.31 → 09:30 $13.31 +0.00; HTFL×19 yday $41.94 → 09:30 $41.50 -8.36; UMAC×24 yday $30.15 → 09:30 $28.59 -37.44; SMJF×79 yday $10.45 → 09:30 $10.45 +0.00; ALOY×54 yday $13.86 → 09:30 $13.19 -35.91; NPWR×415 yday $1.73 → 09:30 $1.70 -12.45; NMAX×72 yday $10.36 → 09:30 $10.31 -3.60; LPTH×53 yday $14.80 → 09:30 $14.01 -41.87; INDI×171 yday $4.71 → 09:30 $4.48 -39.33; KOPN×146 yday $5.32 → 09:30 $5.03 -42.34 | — |
| 2026-08-18 09:30 ET | **SELL** | `XHG` | 190 | $3.94 | $2.60 | $-52.66 | $786.88 | ▼ -52.66 after sell → book $9,132.14; vs 09:30 mark -2.60 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `STDN` | 58 | $13.31 | $2.18 | $-23.49 | $1,556.68 | ▼ -23.49 after sell → book $9,129.96; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🔴 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `HTFL` | 19 | $41.50 | $2.07 | $+1.02 | $2,343.11 | ▲ +1.02 after sell → book $9,127.89; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `UMAC` | 24 | $28.59 | $2.08 | $-99.18 | $3,027.19 | ▼ -99.18 after sell → book $9,125.81; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `SMJF` | 79 | $10.45 | $2.25 | $+23.17 | $3,850.49 | ▲ +23.17 after sell → book $9,123.56; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ALOY` | 54 | $13.19 | $2.17 | $-83.70 | $4,560.57 | ▼ -83.70 after sell → book $9,121.38; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `NPWR` | 415 | $1.70 | $5.43 | $-102.09 | $5,260.64 | ▼ -102.09 after sell → book $9,115.95; vs 09:30 mark -5.43 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `NMAX` | 72 | $10.31 | $2.23 | $-51.95 | $6,000.73 | ▼ -51.95 after sell → book $9,113.72; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `LPTH` | 53 | $14.01 | $2.17 | $-53.61 | $6,741.09 | ▼ -53.61 after sell → book $9,111.55; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `INDI` | 171 | $4.48 | $2.54 | $-34.11 | $7,504.63 | ▼ -34.11 after sell → book $9,109.01; vs 09:30 mark -2.54 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `KOPN` | 146 | $5.03 | $2.46 | $-63.29 | $8,236.55 | ▼ -63.29 after sell → book $9,106.55; vs 09:30 mark -2.46 | dropped from list after 1 sess (min 1) | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,236.55 | ▲ 09:30 equity $9,070.59 vs yday $9,057.83 (+12.76) | 09:30 open · cash $8,236.55 (unchanged overnight, no fees) · equity $9,070.59 vs prior close $9,057.83 (+12.76) because holdings re-marked: CAPR×116 yday $7.08 → 09:30 $7.19 +12.76 | — |
| 2026-08-19 09:30 ET | **SELL** | `CAPR` | 116 | $7.19 | $2.37 | $+32.41 | $9,068.22 | ▲ +32.41 after sell → book $9,068.22; vs 09:30 mark -2.37 | dropped from list after 2 sess (min 1) | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,068.22 | ▲ 09:30 equity $9,068.22 vs yday $9,068.22 (+0.00) | 09:30 open · cash $9,068.22 · no holdings · equity $9,068.22 vs prior close $9,068.22 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 5 | $150.14 | $2.00 | — | $8,315.52 | — | top 12 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+173.9; leftover $755.69 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CYPH` | 657 | $1.15 | $8.48 | — | $7,551.49 | — | top 12 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+62.6; leftover $755.69 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ABCL` | 63 | $11.81 | $2.18 | — | $6,804.97 | — | top 12 by hot; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $755.69 | join🟢 sector🟡 gen🟢 news🔴 digest🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AZI` | 551 | $1.37 | $7.11 | — | $6,042.99 | — | top 12 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+30.7; leftover $755.69 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `SENS` | 84 | $8.91 | $2.24 | — | $5,292.31 | — | top 12 by hot; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+14.9; leftover $755.69 | join🟡 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ALEC` | 314 | $2.40 | $4.05 | — | $4,534.66 | — | top 12 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+13.0; leftover $755.69 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BTGO` | 114 | $6.61 | $2.33 | — | $3,779.36 | — | top 12 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+25.3; leftover $755.69 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟡 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AUTL` | 305 | $2.47 | $3.93 | — | $3,022.07 | — | top 12 by hot; rank hot_score; list mover_buy; 🔵; ⚪; ret5=+19.8; leftover $755.69 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BNTX` | 6 | $109.06 | $2.01 | — | $2,365.70 | — | top 12 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+22.0; leftover $755.69 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `TEAM` | 4 | $173.90 | $2.00 | — | $1,668.10 | — | top 12 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+12.2; leftover $755.69 | join🔴 sector🟢 gen🟢 news🔴 digest🟢 judge🔴 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BBNX` | 37 | $20.00 | $2.10 | — | $926.00 | — | top 12 by hot; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+15.5; leftover $755.69 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `EMBC` | 142 | $5.32 | $2.42 | — | $168.15 | — | top 12 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+14.1; leftover $755.69 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $168.15 | ▲ 09:30 equity $9,127.00 vs yday $8,954.63 (+172.37) | 09:30 open · cash $168.15 (unchanged overnight, no fees) · equity $9,127.00 vs prior close $8,954.63 (+172.37) because holdings re-marked: MRNA×5 yday $133.32 → 09:30 $133.11 -1.05; CYPH×657 yday $1.19 → 09:30 $1.32 +85.41; ABCL×63 yday $11.57 → 09:30 $11.57 +0.00; AZI×551 yday $1.44 → 09:30 $1.46 +11.02; SENS×84 yday $8.82 → 09:30 $9.24 +35.28; ALEC×314 yday $2.26 → 09:30 $2.28 +6.28; BTGO×114 yday $6.60 → 09:30 $6.95 +39.90; AUTL×305 yday $2.46 → 09:30 $2.47 +3.05; BNTX×6 yday $110.89 → 09:30 $110.92 +0.18; TEAM×4 yday $174.91 → 09:30 $174.22 -2.76; BBNX×37 yday $19.48 → 09:30 $19.50 +0.74; EMBC×142 yday $5.47 → 09:30 $5.43 -5.68 | — |
| 2026-08-21 09:30 ET | **SELL** | `ABCL` | 63 | $11.57 | $2.20 | $-19.81 | $894.86 | ▼ -19.81 after sell → book $9,124.80; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `AZI` | 551 | $1.46 | $7.21 | $+35.27 | $1,692.11 | ▲ +35.27 after sell → book $9,117.59; vs 09:30 mark -7.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `SENS` | 84 | $9.24 | $2.27 | $+23.21 | $2,466.00 | ▲ +23.21 after sell → book $9,115.32; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟡 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `ALEC` | 314 | $2.28 | $4.11 | $-45.84 | $3,177.81 | ▼ -45.84 after sell → book $9,111.21; vs 09:30 mark -4.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BTGO` | 114 | $6.95 | $2.36 | $+34.64 | $3,967.75 | ▲ +34.64 after sell → book $9,108.85; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `AUTL` | 305 | $2.47 | $4.00 | $-7.93 | $4,717.10 | ▼ -7.93 after sell → book $9,104.85; vs 09:30 mark -4.00 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `BNTX` | 6 | $110.92 | $2.03 | $+7.12 | $5,380.59 | ▲ +7.12 after sell → book $9,102.82; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `TEAM` | 4 | $174.22 | $2.02 | $-2.74 | $6,075.45 | ▼ -2.74 after sell → book $9,100.80; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BBNX` | 37 | $19.50 | $2.12 | $-22.72 | $6,794.83 | ▼ -22.72 after sell → book $9,098.68; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `EMBC` | 142 | $5.43 | $2.45 | $+10.75 | $7,563.44 | ▲ +10.75 after sell → book $9,096.23; vs 09:30 mark -2.45 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `XHG` | 168 | $4.49 | $2.49 | — | $6,806.63 | — | top 12 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+12.7; leftover $756.34 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🔴 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CAPR` | 111 | $6.81 | $2.32 | — | $6,048.39 | — | top 12 by hot; rank hot_score; list yday_mover; 🔵; ret5=+62.5; leftover $756.34 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 67 | $11.13 | $2.19 | — | $5,300.49 | — | top 12 by hot; rank hot_score; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $756.34 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `IOVA` | 83 | $9.08 | $2.24 | — | $4,544.61 | — | top 12 by hot; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+37.9; leftover $756.34 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CAN` | 2572 | $0.29 | $15.28 | — | $3,773.17 | — | top 12 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+30.4; leftover $756.34 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `TEM` | 11 | $65.60 | $2.02 | — | $3,049.55 | — | top 12 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+22.8; leftover $756.34 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `INO` | 614 | $1.23 | $7.92 | — | $2,286.41 | — | top 12 by hot; rank hot_score; list mover_buy; 🔵; ⚪; ret5=+34.4; leftover $756.34 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `DFDV` | 187 | $4.04 | $2.55 | — | $1,528.37 | — | top 12 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+23.6; leftover $756.34 | join🟡 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `XXI` | 117 | $6.42 | $2.34 | — | $774.89 | — | top 12 by hot; rank hot_score; list yday_gainer; ret5=+23.8; leftover $756.34 | join🔴 sector🔴 gen🟢 news🟡 digest🟡 ab🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 6 | $119.43 | $2.01 | — | $56.31 | — | top 12 by hot; rank hot_score; list flatten,mover_buy; 🔵; ⚪; ret5=+20.4; leftover $756.34 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $56.31 | ▲ 09:30 equity $9,886.30 vs yday $9,403.79 (+482.51) | 09:30 open · cash $56.31 (unchanged overnight, no fees) · equity $9,886.30 vs prior close $9,403.79 (+482.51) because holdings re-marked: MRNA×5 yday $145.13 → 09:30 $142.70 -12.15; CYPH×657 yday $1.42 → 09:30 $1.83 +269.37; XHG×168 yday $4.41 → 09:30 $4.24 -28.56; CAPR×111 yday $6.29 → 09:30 $8.01 +190.92; ARCT×67 yday $13.45 → 09:30 $13.26 -12.73; IOVA×83 yday $8.29 → 09:30 $8.05 -19.92; CAN×2572 yday $0.35 → 09:30 $0.38 +64.30; TEM×11 yday $72.69 → 09:30 $70.07 -28.82; INO×614 yday $1.18 → 09:30 $1.20 +12.28; DFDV×187 yday $3.94 → 09:30 $4.15 +39.27; XXI×117 yday $6.49 → 09:30 $6.60 +12.87; AU×6 yday $121.22 → 09:30 $120.50 -4.32 | — |
| 2026-08-24 09:30 ET | **SELL** | `MRNA` | 5 | $142.70 | $2.02 | $-41.23 | $767.78 | ▼ -41.23 after sell → book $9,884.27; vs 09:30 mark -2.03 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 657 | $1.83 | $8.59 | $+429.69 | $1,961.50 | ▲ +429.69 after sell → book $9,875.68; vs 09:30 mark -8.59 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `XHG` | 168 | $4.24 | $2.53 | $-47.03 | $2,671.28 | ▼ -47.03 after sell → book $9,873.14; vs 09:30 mark -2.54 | dropped from list after 1 sess (min 1) | join🟡 sector🟡 gen🔴 news🟡 digest🟢 ab🔴 peer🟢 vol🔴 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `CAPR` | 111 | $8.01 | $2.35 | $+128.53 | $3,558.04 | ▲ +128.53 after sell → book $9,870.79; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 67 | $13.26 | $2.21 | $+138.31 | $4,444.25 | ▲ +138.31 after sell → book $9,868.58; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `IOVA` | 83 | $8.05 | $2.26 | $-89.99 | $5,110.14 | ▼ -89.99 after sell → book $9,866.32; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CAN` | 2572 | $0.38 | $17.93 | $+187.99 | $6,069.57 | ▲ +187.99 after sell → book $9,848.39; vs 09:30 mark -17.93 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 judge🔴 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `TEM` | 11 | $70.07 | $2.04 | $+45.10 | $6,838.30 | ▲ +45.10 after sell → book $9,846.35; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `INO` | 614 | $1.20 | $8.03 | $-34.37 | $7,567.07 | ▼ -34.37 after sell → book $9,838.32; vs 09:30 mark -8.03 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟡 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `DFDV` | 187 | $4.15 | $2.59 | $+15.43 | $8,340.52 | ▲ +15.43 after sell → book $9,835.72; vs 09:30 mark -2.60 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `XXI` | 117 | $6.60 | $2.37 | $+16.35 | $9,110.35 | ▲ +16.35 after sell → book $9,833.35; vs 09:30 mark -2.37 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 6 | $120.50 | $2.03 | $+2.38 | $9,831.32 | ▲ +2.38 after sell → book $9,831.32; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,831.32 | ▲ 09:30 equity $9,831.32 vs yday $9,831.32 (+0.00) | 09:30 open · cash $9,831.32 · no holdings · equity $9,831.32 vs prior close $9,831.32 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `CYPH` | 481 | $1.70 | $6.20 | — | $9,007.42 | — | top 12 by hot; rank hot_score; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+120.5; leftover $819.28 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `XHG` | 203 | $4.02 | $2.62 | — | $8,188.74 | — | top 12 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+16.1; leftover $819.28 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ASST` | 39 | $20.90 | $2.11 | — | $7,371.53 | — | top 12 by hot; rank hot_score; list yday_gainer; 🔵; ret5=+47.9; leftover $819.28 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 6 | $119.46 | $2.01 | — | $6,652.77 | — | top 12 by hot; rank hot_score; list mover_buy; 🔵; ⚪; ret5=+25.9; leftover $819.28 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `RUM` | 87 | $9.36 | $2.25 | — | $5,836.20 | — | top 12 by hot; rank hot_score; list yday_gainer; 🔵; ret5=+21.3; leftover $819.28 | join🔴 sector🟡 gen🟡 news🟢 digest🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BMNR` | 33 | $24.73 | $2.09 | — | $5,018.02 | — | top 12 by hot; rank hot_score; list yday_gainer; ret5=+26.3; leftover $819.28 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟡 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `NIQ` | 41 | $19.56 | $2.11 | — | $4,213.94 | — | top 12 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+7.6; leftover $819.28 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `DEFT` | 1280 | $0.64 | $12.03 | — | $3,382.71 | — | top 12 by hot; rank hot_score; list yday_gainer,yday_mover,ohlc_hot; 🔵; ⚪; ret5=+17.6; leftover $819.28 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `OMER` | 43 | $18.75 | $2.12 | — | $2,574.34 | — | top 12 by hot; rank hot_score; list ohlc_hot; ret5=+12.1; leftover $819.28 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `HMY` | 36 | $22.65 | $2.10 | — | $1,756.84 | — | top 12 by hot; rank hot_score; list mover_buy; ⚪; ret5=+21.1; leftover $819.28 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `DFDV` | 190 | $4.29 | $2.56 | — | $939.18 | — | top 12 by hot; rank hot_score; list yday_gainer; 🔵; ret5=+28.3; leftover $819.28 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ERO` | 21 | $38.00 | $2.05 | — | $139.13 | — | top 12 by hot; rank hot_score; list ohlc_hot,mover_buy; ⚪; ret5=+16.6; leftover $819.28 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $139.13 | ▲ 09:30 equity $9,681.30 vs yday $9,681.30 (+0.00) | 09:30 open · cash $139.13 (unchanged overnight, no fees) · equity $9,681.30 vs prior close $9,681.30 (+0.00) because holdings re-marked: CYPH×481 yday $1.64 → 09:30 $1.64 +0.00; XHG×203 yday $4.05 → 09:30 $4.05 +0.00; ASST×39 yday $20.20 → 09:30 $20.20 +0.00; AU×6 yday $118.55 → 09:30 $118.55 +0.00; RUM×87 yday $9.35 → 09:30 $9.35 +0.00; BMNR×33 yday $24.21 → 09:30 $24.21 +0.00; NIQ×41 yday $19.46 → 09:30 $19.46 +0.00; DEFT×1280 yday $0.62 → 09:30 $0.62 +0.00; OMER×43 yday $19.03 → 09:30 $19.03 +0.00; HMY×36 yday $22.50 → 09:30 $22.50 +0.00; DFDV×190 yday $4.16 → 09:30 $4.16 +0.00; ERO×21 yday $38.55 → 09:30 $38.55 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $139.13 | ▼ 09:30 equity $9,738.78 vs yday $9,791.07 (-52.29) | 09:30 open · cash $139.13 (unchanged overnight, no fees) · equity $9,738.78 vs prior close $9,791.07 (-52.29) because holdings re-marked: CYPH×481 yday $1.64 → 09:30 $1.60 -19.24; XHG×203 yday $4.05 → 09:30 $3.81 -48.72; ASST×39 yday $20.20 → 09:30 $20.72 +20.28; AU×6 yday $118.55 → 09:30 $119.80 +7.50; RUM×87 yday $9.35 → 09:30 $10.07 +62.64; BMNR×33 yday $24.21 → 09:30 $24.24 +0.99; NIQ×41 yday $19.46 → 09:30 $19.20 -10.66; DEFT×1280 yday $0.62 → 09:30 $0.60 -25.60; OMER×43 yday $19.03 → 09:30 $18.96 -3.01; HMY×36 yday $22.50 → 09:30 $22.39 -3.96; DFDV×190 yday $4.16 → 09:30 $4.35 +36.10; ERO×21 yday $38.55 → 09:30 $40.51 +41.16 | — |
| 2026-08-27 09:30 ET | **SELL** | `CYPH` | 481 | $1.60 | $6.29 | $-60.60 | $902.44 | ▼ -60.60 after sell → book $9,732.49; vs 09:30 mark -6.29 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `XHG` | 203 | $3.81 | $2.66 | $-47.91 | $1,673.20 | ▼ -47.91 after sell → book $9,729.82; vs 09:30 mark -2.67 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ASST` | 39 | $20.72 | $2.13 | $-11.25 | $2,479.16 | ▼ -11.25 after sell → book $9,727.70; vs 09:30 mark -2.12 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `AU` | 6 | $119.80 | $2.03 | $-2.00 | $3,195.93 | ▼ -2.00 after sell → book $9,725.67; vs 09:30 mark -2.03 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `RUM` | 87 | $10.07 | $2.28 | $+57.24 | $4,069.74 | ▲ +57.24 after sell → book $9,723.39; vs 09:30 mark -2.28 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BMNR` | 33 | $24.24 | $2.11 | $-20.37 | $4,867.55 | ▼ -20.37 after sell → book $9,721.28; vs 09:30 mark -2.11 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `NIQ` | 41 | $19.20 | $2.13 | $-19.01 | $5,652.62 | ▼ -19.01 after sell → book $9,719.15; vs 09:30 mark -2.13 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `DEFT` | 1280 | $0.60 | $11.74 | $-74.97 | $6,408.88 | ▼ -74.97 after sell → book $9,707.41; vs 09:30 mark -11.74 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `OMER` | 43 | $18.96 | $2.14 | $+4.77 | $7,222.02 | ▲ +4.77 after sell → book $9,705.27; vs 09:30 mark -2.14 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `HMY` | 36 | $22.39 | $2.12 | $-13.58 | $8,025.94 | ▼ -13.58 after sell → book $9,703.15; vs 09:30 mark -2.12 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `DFDV` | 190 | $4.35 | $2.60 | $+6.24 | $8,849.84 | ▲ +6.24 after sell → book $9,700.55; vs 09:30 mark -2.60 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ERO` | 21 | $40.51 | $2.07 | $+48.58 | $9,698.48 | ▲ +48.58 after sell → book $9,698.48; vs 09:30 mark -2.07 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `MOS` | 32 | $24.84 | $2.09 | — | $8,901.51 | — | top 12 by hot; rank hot_score; list flatten; ret5=+13.0; leftover $808.21 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟢 |
| 2026-08-27 09:30 ET | **BUY** | `DLO` | 51 | $15.60 | $2.14 | — | $8,103.77 | — | top 12 by hot; rank hot_score; list mover_buy; 🔵; ret5=+7.1; leftover $808.21 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 312 | $2.59 | $4.02 | — | $7,291.66 | — | top 12 by hot; rank hot_score; list flatten; ret5=+4.2; leftover $808.21 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MRVL` | 3 | $240.00 | $2.00 | — | $6,569.66 | — | top 12 by hot; rank hot_score; list mover_buy; 🔵; ret5=+6.8; leftover $808.21 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 57 | $14.09 | $2.16 | — | $5,764.37 | — | top 12 by hot; rank hot_score; list flatten; ret5=+1.1; leftover $808.21 | join🟢 sector🔴 gen🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `PLTR` | 4 | $170.60 | $2.00 | — | $5,079.97 | — | top 12 by hot; rank hot_score; list mover_buy; 🔵; ret5=+3.4; leftover $808.21 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 19 | $40.72 | $2.05 | — | $4,304.24 | — | top 12 by hot; rank hot_score; list flatten; ret5=+1.8; leftover $808.21 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `GEN` | 27 | $28.89 | $2.07 | — | $3,522.14 | — | top 12 by hot; rank hot_score; list mover_buy; 🔵; ret5=+1.6; leftover $808.21 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `TX` | 14 | $55.20 | $2.03 | — | $2,747.31 | — | top 12 by hot; rank hot_score; list mover_buy; 🔵; ret5=+3.0; leftover $808.21 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `PGY` | 36 | $21.97 | $2.10 | — | $1,954.29 | — | top 12 by hot; rank hot_score; list mover_buy; 🔵; ret5=+0.6; leftover $808.21 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `ANET` | 4 | $190.90 | $2.00 | — | $1,188.69 | — | top 12 by hot; rank hot_score; list mover_buy; 🔵; ret5=-5.1; leftover $808.21 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `NUE` | 3 | $248.91 | $2.00 | — | $439.96 | — | top 12 by hot; rank hot_score; list mover_buy; 🔵; ret5=-9.4; leftover $808.21 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟡 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $439.96 | ▲ 09:30 equity $9,869.60 vs yday $9,818.30 (+51.30) | 09:30 open · cash $439.96 (unchanged overnight, no fees) · equity $9,869.60 vs prior close $9,818.30 (+51.30) because holdings re-marked: MOS×32 yday $24.16 → 09:30 $24.00 -5.12; DLO×51 yday $15.36 → 09:30 $15.33 -1.53; SLI×312 yday $2.61 → 09:30 $2.60 -3.12; MRVL×3 yday $245.11 → 09:30 $253.44 +24.99; CRK×57 yday $14.50 → 09:30 $14.42 -4.56; PLTR×4 yday $177.50 → 09:30 $178.75 +5.00; RRC×19 yday $41.55 → 09:30 $41.44 -2.09; GEN×27 yday $29.64 → 09:30 $29.83 +5.13; TX×14 yday $55.13 → 09:30 $55.25 +1.68; PGY×36 yday $22.41 → 09:30 $22.93 +18.72; ANET×4 yday $202.25 → 09:30 $205.90 +14.60; NUE×3 yday $252.80 → 09:30 $252.00 -2.40 | — |
| 2026-08-28 09:30 ET | **SELL** | `MOS` | 32 | $24.00 | $2.11 | $-31.07 | $1,205.86 | ▼ -31.07 after sell → book $9,867.50; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `DLO` | 51 | $15.33 | $2.16 | $-18.08 | $1,985.52 | ▼ -18.08 after sell → book $9,865.33; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `SLI` | 312 | $2.60 | $4.09 | $-4.99 | $2,792.64 | ▼ -4.99 after sell → book $9,861.25; vs 09:30 mark -4.08 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `MRVL` | 3 | $253.44 | $2.02 | $+36.30 | $3,550.94 | ▲ +36.30 after sell → book $9,859.23; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `CRK` | 57 | $14.42 | $2.18 | $+14.47 | $4,370.70 | ▲ +14.47 after sell → book $9,857.05; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `PLTR` | 4 | $178.75 | $2.02 | $+28.58 | $5,083.68 | ▲ +28.58 after sell → book $9,855.03; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `RRC` | 19 | $41.44 | $2.07 | $+9.57 | $5,868.97 | ▲ +9.57 after sell → book $9,852.96; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `GEN` | 27 | $29.83 | $2.09 | $+21.22 | $6,672.29 | ▲ +21.22 after sell → book $9,850.87; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `TX` | 14 | $55.25 | $2.05 | $-3.38 | $7,443.74 | ▼ -3.38 after sell → book $9,848.82; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `PGY` | 36 | $22.93 | $2.12 | $+30.34 | $8,267.10 | ▲ +30.34 after sell → book $9,846.70; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `ANET` | 4 | $205.90 | $2.02 | $+55.98 | $9,088.68 | ▲ +55.98 after sell → book $9,844.68; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `NUE` | 3 | $252.00 | $2.02 | $+5.25 | $9,842.66 | ▲ +5.25 after sell → book $9,842.66; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `FIGR` | 21 | $37.42 | $2.05 | — | $9,054.78 | — | top 12 by hot; rank hot_score; list yday_mover; ret5=+24.4; leftover $820.22 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 heat🟡 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `NIQ` | 43 | $18.79 | $2.12 | — | $8,244.69 | — | top 12 by hot; rank hot_score; list ohlc_hot; ret5=+7.6; leftover $820.22 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ERO` | 20 | $39.20 | $2.05 | — | $7,458.64 | — | top 12 by hot; rank hot_score; list ohlc_hot; ret5=+16.6; leftover $820.22 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `TRLV` | 72 | $11.38 | $2.21 | — | $6,637.08 | — | top 12 by hot; rank hot_score; list ohlc_hot; ret5=+15.0; leftover $820.22 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 heat🟡 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CVI` | 20 | $40.04 | $2.05 | — | $5,834.23 | — | top 12 by hot; rank hot_score; list ohlc_hot; ret5=+12.1; leftover $820.22 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `VIRT` | 12 | $65.42 | $2.03 | — | $5,047.16 | — | top 12 by hot; rank hot_score; list ohlc_hot; ret5=+13.2; leftover $820.22 | join🟢 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟡 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `TXG` | 12 | $64.10 | $2.03 | — | $4,275.94 | — | top 12 by hot; rank hot_score; list ohlc_hot; ret5=+16.1; leftover $820.22 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `GUTS` | 1108 | $0.74 | $11.52 | — | $3,444.49 | — | top 12 by hot; rank hot_score; list ohlc_hot; ret5=+14.7; leftover $820.22 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `WPM` | 5 | $155.89 | $2.00 | — | $2,663.04 | — | top 12 by hot; rank hot_score; list ohlc_hot; ret5=+17.6; leftover $820.22 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟡 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `AMTX` | 438 | $1.87 | $5.65 | — | $1,838.33 | — | top 12 by hot; rank hot_score; list ohlc_hot; ret5=+16.9; leftover $820.22 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `EGO` | 17 | $46.87 | $2.04 | — | $1,039.50 | — | top 12 by hot; rank hot_score; list ohlc_hot; ret5=+15.1; leftover $820.22 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟡 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ZYME` | 27 | $29.33 | $2.07 | — | $245.52 | — | top 12 by hot; rank hot_score; list yday_gainer,ohlc_hot; ret5=+14.1; leftover $820.22 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $245.52 | ▼ 09:30 equity $9,718.28 vs yday $9,861.10 (-142.82) | 09:30 open · cash $245.52 (unchanged overnight, no fees) · equity $9,718.28 vs prior close $9,861.10 (-142.82) because holdings re-marked: FIGR×21 yday $38.02 → 09:30 $35.50 -52.92; NIQ×43 yday $19.07 → 09:30 $19.20 +5.59; ERO×20 yday $39.82 → 09:30 $38.60 -24.40; TRLV×72 yday $11.03 → 09:30 $12.41 +99.36; CVI×20 yday $39.76 → 09:30 $41.76 +40.00; VIRT×12 yday $67.04 → 09:30 $66.39 -7.80; TXG×12 yday $64.85 → 09:30 $60.90 -47.40; GUTS×1108 yday $0.74 → 09:30 $0.67 -77.56; WPM×5 yday $157.99 → 09:30 $152.49 -27.50; AMTX×438 yday $1.87 → 09:30 $1.90 +13.14; EGO×17 yday $48.03 → 09:30 $45.48 -43.35; ZYME×27 yday $29.01 → 09:30 $28.27 -19.98 | — |
| 2026-08-31 09:30 ET | **SELL** | `FIGR` | 21 | $35.50 | $2.07 | $-44.45 | $988.94 | ▼ -44.45 after sell → book $9,716.20; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ERO` | 20 | $38.60 | $2.07 | $-16.12 | $1,758.87 | ▼ -16.12 after sell → book $9,714.13; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-31 09:30 ET | **SELL** | `TRLV` | 72 | $12.41 | $2.23 | $+69.73 | $2,650.16 | ▲ +69.73 after sell → book $9,711.90; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟢 digest🟢 judge🟢 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `VIRT` | 12 | $66.39 | $2.05 | $+7.57 | $3,444.80 | ▲ +7.57 after sell → book $9,709.86; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `TXG` | 12 | $60.90 | $2.05 | $-42.47 | $4,173.55 | ▼ -42.47 after sell → book $9,707.81; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `GUTS` | 1108 | $0.67 | $10.94 | $-100.02 | $4,904.97 | ▼ -100.02 after sell → book $9,696.87; vs 09:30 mark -10.94 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `WPM` | 5 | $152.49 | $2.02 | $-21.03 | $5,665.40 | ▼ -21.03 after sell → book $9,694.85; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `AMTX` | 438 | $1.90 | $5.73 | $+1.76 | $6,491.86 | ▲ +1.76 after sell → book $9,689.11; vs 09:30 mark -5.74 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `EGO` | 17 | $45.48 | $2.06 | $-27.73 | $7,262.96 | ▼ -27.73 after sell → book $9,687.05; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `ZYME` | 27 | $28.27 | $2.09 | $-32.78 | $8,024.16 | ▼ -32.78 after sell → book $9,684.96; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🔴 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,024.16 | ▲ 09:30 equity $9,700.94 vs yday $9,684.96 (+15.98) | 09:30 open · cash $8,024.16 (unchanged overnight, no fees) · equity $9,700.94 vs prior close $9,684.96 (+15.98) because holdings re-marked: NIQ×43 yday $19.20 → 09:30 $19.06 -6.02; CVI×20 yday $41.76 → 09:30 $42.86 +22.00 | — |
| 2026-09-01 09:30 ET | **SELL** | `NIQ` | 43 | $19.06 | $2.14 | $+7.35 | $8,841.60 | ▲ +7.35 after sell → book $9,698.80; vs 09:30 mark -2.14 | dropped from list after 2 sess (min 1) | — |
| 2026-09-01 09:30 ET | **SELL** | `CVI` | 20 | $42.86 | $2.07 | $+52.28 | $9,696.73 | ▲ +52.28 after sell → book $9,696.73; vs 09:30 mark -2.07 | dropped from list after 2 sess (min 1) | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,696.73 | ▲ 09:30 equity $9,696.73 vs yday $9,696.73 (+0.00) | 09:30 open · cash $9,696.73 · no holdings · equity $9,696.73 vs prior close $9,696.73 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,696.73 | ▲ 09:30 equity $9,696.73 vs yday $9,696.73 (+0.00) | 09:30 open · cash $9,696.73 · no holdings · equity $9,696.73 vs prior close $9,696.73 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `MRNA` | 5 | $151.40 | $2.00 | — | $8,937.73 | — | top 12 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+129.2; leftover $808.06 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `XHG` | 226 | $3.57 | $2.92 | — | $8,127.99 | — | top 12 by hot; rank hot_score; list ohlc_hot; ret5=+16.1; leftover $808.06 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 49 | $16.46 | $2.14 | — | $7,319.32 | — | top 12 by hot; rank hot_score; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+63.4; leftover $808.06 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CAN` | 2693 | $0.30 | $16.16 | — | $6,495.26 | — | top 12 by hot; rank hot_score; list yday_mover; 🔵; ret5=+54.3; leftover $808.06 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `NIQ` | 43 | $18.60 | $2.12 | — | $5,693.34 | — | top 12 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+7.6; leftover $808.06 | join🟢 sector🟢 gen🟡 news🔴 digest🟢 judge🟡 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DEFT` | 1206 | $0.67 | $11.70 | — | $4,873.62 | — | top 12 by hot; rank hot_score; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+17.6; leftover $808.06 | join🟡 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `OMER` | 42 | $18.97 | $2.12 | — | $4,074.76 | — | top 12 by hot; rank hot_score; list ohlc_hot; ret5=+12.1; leftover $808.06 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ERO` | 22 | $35.62 | $2.06 | — | $3,289.07 | — | top 12 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+16.6; leftover $808.06 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🔴 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `TRLV` | 68 | $11.78 | $2.19 | — | $2,485.83 | — | top 12 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+15.0; leftover $808.06 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FUTU` | 6 | $119.46 | $2.01 | — | $1,767.07 | — | top 12 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+17.5; leftover $808.06 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CVI` | 18 | $42.58 | $2.04 | — | $998.58 | — | top 12 by hot; rank hot_score; list ohlc_hot; ret5=+12.1; leftover $808.06 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `VIRT` | 12 | $65.64 | $2.03 | — | $208.88 | — | top 12 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+13.2; leftover $808.06 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $208.88 | ▲ 09:30 equity $9,653.96 vs yday $9,526.37 (+127.59) | 09:30 open · cash $208.88 (unchanged overnight, no fees) · equity $9,653.96 vs prior close $9,526.37 (+127.59) because holdings re-marked: MRNA×5 yday $150.81 → 09:30 $145.95 -24.30; XHG×226 yday $3.32 → 09:30 $3.38 +13.56; ARCT×49 yday $16.74 → 09:30 $16.77 +1.47; CAN×2693 yday $0.31 → 09:30 $0.34 +80.79; NIQ×43 yday $18.35 → 09:30 $18.66 +13.33; DEFT×1206 yday $0.65 → 09:30 $0.65 +0.00; OMER×42 yday $18.86 → 09:30 $18.99 +5.46; ERO×22 yday $34.76 → 09:30 $35.82 +23.32; TRLV×68 yday $11.69 → 09:30 $11.89 +13.60; FUTU×6 yday $118.08 → 09:30 $118.19 +0.66; CVI×18 yday $42.92 → 09:30 $42.45 -8.46; VIRT×12 yday $62.69 → 09:30 $63.37 +8.16 | — |
| 2026-09-04 09:30 ET | **SELL** | `MRNA` | 5 | $145.95 | $2.02 | $-31.28 | $936.60 | ▼ -31.28 after sell → book $9,651.93; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `ARCT` | 49 | $16.77 | $2.16 | $+10.90 | $1,756.17 | ▲ +10.90 after sell → book $9,649.77; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CAN` | 2693 | $0.34 | $17.69 | $+73.87 | $2,654.10 | ▲ +73.87 after sell → book $9,632.08; vs 09:30 mark -17.69 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `HQ` | 51 | $17.06 | $2.14 | — | $1,781.90 | — | top 12 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+17.3; leftover $884.70 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 ab🟡 heat🔴 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 174 | $5.08 | $2.51 | — | $895.47 | — | top 12 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+28.1; leftover $884.70 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `HOOD` | 7 | $113.80 | $2.01 | — | $96.86 | — | top 12 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+13.2; leftover $884.70 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `OABI` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FDMT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AVAH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AXTI` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FIGR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `IOVA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ALM` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `SMTC` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OCUL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FN` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CRDL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ARX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `IVVD` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `WEAV` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `KURA` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SENS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `REAX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `HAE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `NMAX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DVLT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ALEC` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `ALM` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DEFT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `OMER` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HMY` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ERO` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `TRLV` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `TMC` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `KGC` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `GUTS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ZYME` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ABUS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-26 | `CYPH` | no_price | no 09:30 open — carry |
| 2026-08-26 | `ASST` | no_price | no 09:30 open — carry |
| 2026-08-26 | `AU` | no_price | no 09:30 open — carry |
| 2026-08-26 | `RUM` | no_price | no 09:30 open — carry |
| 2026-08-26 | `BMNR` | no_price | no 09:30 open — carry |
| 2026-08-26 | `HMY` | no_price | no 09:30 open — carry |
| 2026-08-26 | `DFDV` | no_price | no 09:30 open — carry |
| 2026-08-26 | `USDE` | no_price | no 09:30 open |
| 2026-08-26 | `SENS` | no_price | no 09:30 open |
| 2026-08-26 | `TRLV` | no_price | no 09:30 open |
| 2026-08-26 | `FUTU` | no_price | no 09:30 open |
| 2026-08-26 | `HOOD` | no_price | no 09:30 open |
| 2026-08-26 | `TXG` | no_price | no 09:30 open |
| 2026-08-26 | `GUTS` | no_price | no 09:30 open |
| 2026-08-31 | `USDE` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CYPH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `XHG` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CAN` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PURR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `MSTR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `DEFT` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `OMER` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `DFDV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `FUTU` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `CYPH` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `OMER` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `TRLV` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `TXG` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `GUTS` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `DK` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `INO` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `UEC` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `SBSW` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `SG` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `QMCO` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NVAX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `USDE` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `XHG` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `OMER` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `TRLV` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `TXG` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `GUTS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `WPM` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ZYME` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `NOG` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `DK` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `BKKT` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `INO` | hard_red | hard-red S=-3.83 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `XHG` | 226 | 2026-09-03 @ $3.57 | top 12 by hot; rank hot_score; list ohlc_hot; ret5=+16.1; leftover $808.06 |
| `NIQ` | 43 | 2026-09-03 @ $18.60 | top 12 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+7.6; leftover $808.06 |
| `DEFT` | 1206 | 2026-09-03 @ $0.67 | top 12 by hot; rank hot_score; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+17.6; leftover $808.06 |
| `OMER` | 42 | 2026-09-03 @ $18.97 | top 12 by hot; rank hot_score; list ohlc_hot; ret5=+12.1; leftover $808.06 |
| `ERO` | 22 | 2026-09-03 @ $35.62 | top 12 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+16.6; leftover $808.06 |
| `TRLV` | 68 | 2026-09-03 @ $11.78 | top 12 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+15.0; leftover $808.06 |
| `FUTU` | 6 | 2026-09-03 @ $119.46 | top 12 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+17.5; leftover $808.06 |
| `CVI` | 18 | 2026-09-03 @ $42.58 | top 12 by hot; rank hot_score; list ohlc_hot; ret5=+12.1; leftover $808.06 |
| `VIRT` | 12 | 2026-09-03 @ $65.64 | top 12 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+13.2; leftover $808.06 |
| `HQ` | 51 | 2026-09-04 @ $17.06 | top 12 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+17.3; leftover $884.70 |
| `OABI` | 174 | 2026-09-04 @ $5.08 | top 12 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+28.1; leftover $884.70 |
| `HOOD` | 7 | 2026-09-04 @ $113.80 | top 12 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+13.2; leftover $884.70 |
