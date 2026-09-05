# Factor mine action — `union_ret_5_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `ret_5` · size `leftover` · sell `list` · S-boost `none` · rank by ret_5

Cash book **+4.96%** ($10,496) · signal-only (no cash/fees) was -1.46%. Starts YES **11/17**. Fills 136 · skips 64 · realized $+438.34.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `ret_5` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $10.38.

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | vs yday close | Bought | Sold | Close cash | Close equity | Close held | 09:30 why |
|---|---:|---:|---|---:|---:|---|---|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | TNDM, INO, IREN, TPG, VOR, SLS, TGTX, BTSG | — | $114.01 | $10,265.50 | TNDM×53, INO×1543, IREN×27, TPG×24, VOR×56, SLS×106, TGTX×25, BTSG×20 | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-14 | +5.50 | $114.01 | TNDM×53, INO×1543, IREN×27, TPG×24, VOR×56, SLS×106, TGTX×25, BTSG×20 | $10,276.78 | +11.28 | QMCO, ARX, ZENA, AIRO, BRUN, BCAR, TBBB | TNDM, INO, IREN, TPG, VOR, SLS, TGTX, BTSG | $1,325.75 | $9,841.43 | QMCO×51, ARX×65, ZENA×581, AIRO×115, BRUN×48, BCAR×210, TBBB×26 | 09:30 open · cash $114.01 (unchanged overnight, no fees) · equity $10,276.78 vs prior close $10,265.50 (+11.28) because holdings re-marked: TNDM×53 yday $23.13 → 09:30 $22.92 -11.13; INO×1543 yday $0.90 → 09:30 $0.93 +46.29; IREN×27 yday $44.76 → 09:30 $44.09 -18.09; TPG×24 yday $54.62 → 09:30 $55.29 +16.08; VOR×56 yday $23.29 → 09:30 $23.33 +2.24; SLS×106 yday $12.36 → 09:30 $12.40 +4.24; TGTX×25 yday $47.94 → 09:30 $47.27 -16.75; BTSG×20 yday $60.23 → 09:30 $59.65 -11.60 |
| 2026-08-17 | +2.25 | $1,325.75 | QMCO×51, ARX×65, ZENA×581, AIRO×115, BRUN×48, BCAR×210, TBBB×26 | $9,770.10 | -71.33 | XHG, CAPR, STDN, HTFL, UMAC, KOPN, NPWR, SMJF | QMCO, ARX, ZENA, AIRO, BRUN, BCAR, TBBB | $31.98 | $9,543.43 | XHG×290, CAPR×177, STDN×89, HTFL×29, UMAC×37, KOPN×224, NPWR×634, SMJF×120 | 09:30 open · cash $1,325.75 (unchanged overnight, no fees) · equity $9,770.10 vs prior close $9,841.43 (-71.33) because holdings re-marked: QMCO×51 yday $26.11 → 09:30 $24.83 -65.28; ARX×65 yday $19.58 → 09:30 $19.57 -0.65; ZENA×581 yday $2.14 → 09:30 $2.08 -31.96; AIRO×115 yday $9.57 → 09:30 $9.57 +0.00; BRUN×48 yday $22.93 → 09:30 $23.00 +3.36; BCAR×210 yday $5.83 → 09:30 $5.99 +33.60; TBBB×26 yday $47.79 → 09:30 $47.39 -10.40 |
| 2026-08-18 | -6.20 | $31.98 | XHG×290, CAPR×177, STDN×89, HTFL×29, UMAC×37, KOPN×224, NPWR×634, SMJF×120 | $9,406.52 | -136.91 | — | XHG, STDN, HTFL, UMAC, KOPN, NPWR, SMJF | $8,055.12 | $9,308.28 | CAPR×177 | 09:30 open · cash $31.98 (unchanged overnight, no fees) · equity $9,406.52 vs prior close $9,543.43 (-136.91) because holdings re-marked: XHG×290 yday $3.91 → 09:30 $3.94 +8.70; CAPR×177 yday $7.45 → 09:30 $7.50 +8.85; STDN×89 yday $13.31 → 09:30 $13.31 +0.00; HTFL×29 yday $41.94 → 09:30 $41.50 -12.76; UMAC×37 yday $30.15 → 09:30 $28.59 -57.72; KOPN×224 yday $5.32 → 09:30 $5.03 -64.96; NPWR×634 yday $1.73 → 09:30 $1.70 -19.02; SMJF×120 yday $10.45 → 09:30 $10.45 +0.00 |
| 2026-08-19 | -7.20 | $8,055.12 | CAPR×177 | $9,327.75 | +19.47 | — | CAPR | $9,325.18 | $9,325.18 | — | 09:30 open · cash $8,055.12 (unchanged overnight, no fees) · equity $9,327.75 vs prior close $9,308.28 (+19.47) because holdings re-marked: CAPR×177 yday $7.08 → 09:30 $7.19 +19.47 |
| 2026-08-20 | +1.12 | $9,325.18 | — | $9,325.18 | +0.00 | MRNA, CYPH, AZI, BTGO, BNTX, AUTL, ASST, BRR | — | $165.42 | $9,373.05 | MRNA×7, CYPH×1013, AZI×850, BTGO×176, BNTX×10, AUTL×471, ASST×72, BRR×560 | 09:30 open · cash $9,325.18 · no holdings · equity $9,325.18 vs prior close $9,325.18 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-21 | +3.25 | $165.42 | MRNA×7, CYPH×1013, AZI×850, BTGO×176, BNTX×10, AUTL×471, ASST×72, BRR×560 | $9,702.64 | +329.59 | CAPR, ARCT, IOVA, INO, CAN, INDP | AZI, BTGO, BNTX, AUTL, ASST, BRR | $0.65 | $9,974.68 | MRNA×7, CYPH×1013, CAPR×181, ARCT×110, IOVA×135, INO×1003, CAN×4196, INDP×860 | 09:30 open · cash $165.42 (unchanged overnight, no fees) · equity $9,702.64 vs prior close $9,373.05 (+329.59) because holdings re-marked: MRNA×7 yday $133.32 → 09:30 $133.11 -1.47; CYPH×1013 yday $1.19 → 09:30 $1.32 +131.69; AZI×850 yday $1.44 → 09:30 $1.46 +17.00; BTGO×176 yday $6.60 → 09:30 $6.95 +61.60; BNTX×10 yday $110.89 → 09:30 $110.92 +0.30; AUTL×471 yday $2.46 → 09:30 $2.47 +4.71; ASST×72 yday $16.13 → 09:30 $17.66 +110.16; BRR×560 yday $2.24 → 09:30 $2.25 +5.60 |
| 2026-08-24 | -5.17 | $0.65 | MRNA×7, CYPH×1013, CAPR×181, ARCT×110, IOVA×135, INO×1003, CAN×4196, INDP×860 | $10,712.98 | +738.30 | — | MRNA, CYPH, CAPR, ARCT, IOVA, INO, CAN, INDP | $10,636.74 | $10,636.74 | — | 09:30 open · cash $0.65 (unchanged overnight, no fees) · equity $10,712.98 vs prior close $9,974.68 (+738.30) because holdings re-marked: MRNA×7 yday $145.13 → 09:30 $142.70 -17.01; CYPH×1013 yday $1.42 → 09:30 $1.83 +415.33; CAPR×181 yday $6.29 → 09:30 $8.01 +311.32; ARCT×110 yday $13.45 → 09:30 $13.26 -20.90; IOVA×135 yday $8.29 → 09:30 $8.05 -32.40; INO×1003 yday $1.18 → 09:30 $1.20 +20.06; CAN×4196 yday $0.35 → 09:30 $0.38 +104.90; INDP×860 yday $1.29 → 09:30 $1.24 -43.00 |
| 2026-08-25 | +1.80 | $10,636.74 | — | $10,636.74 | -0.00 | CYPH, ASST, DFDV, BMNR, AU, RUM, HMY, FWDI | — | $45.81 | $10,401.27 | CYPH×782, ASST×63, DFDV×309, BMNR×53, AU×11, RUM×142, HMY×58, FWDI×221 | 09:30 open · cash $10,636.74 · no holdings · equity $10,636.74 vs prior close $10,636.74 (-0.00). Cash unchanged overnight; no fees. |
| 2026-08-26 | +2.02 | $45.81 | CYPH×782, ASST×63, DFDV×309, BMNR×53, AU×11, RUM×142, HMY×58, FWDI×221 | $10,401.27 | +0.00 | — | — | $45.81 | $10,608.88 | CYPH×782, ASST×63, DFDV×309, BMNR×53, AU×11, RUM×142, HMY×58, FWDI×221 | 09:30 open · cash $45.81 (unchanged overnight, no fees) · equity $10,401.27 vs prior close $10,401.27 (+0.00) because holdings re-marked: CYPH×782 yday $1.64 → 09:30 $1.64 +0.00; ASST×63 yday $20.20 → 09:30 $20.20 +0.00; DFDV×309 yday $4.16 → 09:30 $4.16 +0.00; BMNR×53 yday $24.21 → 09:30 $24.21 +0.00; AU×11 yday $118.55 → 09:30 $118.55 +0.00; RUM×142 yday $9.35 → 09:30 $9.35 +0.00; HMY×58 yday $22.50 → 09:30 $22.50 +0.00; FWDI×221 yday $5.86 → 09:30 $5.86 +0.00 |
| 2026-08-27 | — | $45.81 | CYPH×782, ASST×63, DFDV×309, BMNR×53, AU×11, RUM×142, HMY×58, FWDI×221 | $10,596.97 | -11.91 | MOS, DLO, MRVL, SLI, PLTR, TX, RRC, GEN | CYPH, ASST, DFDV, BMNR, AU, RUM, HMY, FWDI | $332.78 | $10,634.04 | MOS×53, DLO×84, MRVL×5, SLI×510, PLTR×7, TX×23, RRC×32, GEN×45 | 09:30 open · cash $45.81 (unchanged overnight, no fees) · equity $10,596.97 vs prior close $10,608.88 (-11.91) because holdings re-marked: CYPH×782 yday $1.64 → 09:30 $1.60 -31.28; ASST×63 yday $20.20 → 09:30 $20.72 +32.76; DFDV×309 yday $4.16 → 09:30 $4.35 +58.71; BMNR×53 yday $24.21 → 09:30 $24.24 +1.59; AU×11 yday $118.55 → 09:30 $119.80 +13.75; RUM×142 yday $9.35 → 09:30 $10.07 +102.24; HMY×58 yday $22.50 → 09:30 $22.39 -6.38; FWDI×221 yday $5.86 → 09:30 $5.97 +24.31 |
| 2026-08-28 | +0.75 | $332.78 | MOS×53, DLO×84, MRVL×5, SLI×510, PLTR×7, TX×23, RRC×32, GEN×45 | $10,676.13 | +42.09 | FIGR, WPM, SCCO, AMTX, SBSW, EQX, ERO, TXG | MOS, DLO, MRVL, SLI, PLTR, TX, RRC, GEN | $226.85 | $10,728.35 | FIGR×35, WPM×8, SCCO×6, AMTX×712, SBSW×110, EQX×98, ERO×33, TXG×20 | 09:30 open · cash $332.78 (unchanged overnight, no fees) · equity $10,676.13 vs prior close $10,634.04 (+42.09) because holdings re-marked: MOS×53 yday $24.16 → 09:30 $24.00 -8.48; DLO×84 yday $15.36 → 09:30 $15.33 -2.52; MRVL×5 yday $245.11 → 09:30 $253.44 +41.65; SLI×510 yday $2.61 → 09:30 $2.60 -5.10; PLTR×7 yday $177.50 → 09:30 $178.75 +8.75; TX×23 yday $55.13 → 09:30 $55.25 +2.76; RRC×32 yday $41.55 → 09:30 $41.44 -3.52; GEN×45 yday $29.64 → 09:30 $29.83 +8.55 |
| 2026-08-31 | -5.85 | $226.85 | FIGR×35, WPM×8, SCCO×6, AMTX×712, SBSW×110, EQX×98, ERO×33, TXG×20 | $10,372.35 | -356.00 | — | FIGR, WPM, SCCO, AMTX, SBSW, EQX, ERO, TXG | $10,348.02 | $10,348.02 | — | 09:30 open · cash $226.85 (unchanged overnight, no fees) · equity $10,372.35 vs prior close $10,728.35 (-356.00) because holdings re-marked: FIGR×35 yday $38.02 → 09:30 $35.50 -88.20; WPM×8 yday $157.99 → 09:30 $152.49 -44.00; SCCO×6 yday $216.28 → 09:30 $207.95 -49.98; AMTX×712 yday $1.87 → 09:30 $1.90 +21.36; SBSW×110 yday $12.26 → 09:30 $12.14 -13.20; EQX×98 yday $13.45 → 09:30 $12.81 -62.72; ERO×33 yday $39.82 → 09:30 $38.60 -40.26; TXG×20 yday $64.85 → 09:30 $60.90 -79.00 |
| 2026-09-01 | -6.30 | $10,348.02 | — | $10,348.02 | +0.00 | — | — | $10,348.02 | $10,348.02 | — | 09:30 open · cash $10,348.02 · no holdings · equity $10,348.02 vs prior close $10,348.02 (+0.00). Cash unchanged overnight; no fees. |
| 2026-09-02 | -3.83 | $10,348.02 | — | $10,348.02 | +0.00 | — | — | $10,348.02 | $10,348.02 | — | 09:30 open · cash $10,348.02 · no holdings · equity $10,348.02 vs prior close $10,348.02 (+0.00). Cash unchanged overnight; no fees. |
| 2026-09-03 | -0.90 | $10,348.02 | — | $10,348.02 | +0.00 | MRNA, ARCT, CAN, ALEC, DEFT, WPM, FUTU, SCCO | — | $300.82 | $10,480.93 | MRNA×8, ARCT×78, CAN×4311, ALEC×538, DEFT×1930, WPM×8, FUTU×10, SCCO×6 | 09:30 open · cash $10,348.02 · no holdings · equity $10,348.02 vs prior close $10,348.02 (+0.00). Cash unchanged overnight; no fees. |
| 2026-09-04 | — | $300.82 | MRNA×8, ARCT×78, CAN×4311, ALEC×538, DEFT×1930, WPM×8, FUTU×10, SCCO×6 | $10,628.72 | +147.79 | OABI, BRR, HQ | MRNA, ARCT, CAN | $10.38 | $10,495.69 | ALEC×538, DEFT×1930, WPM×8, FUTU×10, SCCO×6, OABI×276, BRR×594, HQ×81 | 09:30 open · cash $300.82 (unchanged overnight, no fees) · equity $10,628.72 vs prior close $10,480.93 (+147.79) because holdings re-marked: MRNA×8 yday $150.81 → 09:30 $145.95 -38.88; ARCT×78 yday $16.74 → 09:30 $16.77 +2.34; CAN×4311 yday $0.31 → 09:30 $0.34 +129.33; ALEC×538 yday $2.72 → 09:30 $2.70 -10.76; DEFT×1930 yday $0.65 → 09:30 $0.65 +0.00; WPM×8 yday $150.91 → 09:30 $155.85 +39.52; FUTU×10 yday $118.08 → 09:30 $118.19 +1.10; SCCO×6 yday $204.26 → 09:30 $208.45 +25.14 |

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
| 2026-08-14 09:30 ET | **SELL** | `TNDM` | 53 | $22.92 | $2.17 | $-26.05 | $1,326.60 | ▼ -26.05 after sell → book $10,274.61; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `INO` | 1543 | $0.93 | $19.25 | $+148.79 | $2,742.35 | ▲ +148.79 after sell → book $10,255.37; vs 09:30 mark -19.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `IREN` | 27 | $44.09 | $2.09 | $-55.19 | $3,930.69 | ▼ -55.19 after sell → book $10,253.28; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TPG` | 24 | $55.29 | $2.08 | $+107.86 | $5,255.56 | ▲ +107.86 after sell → book $10,251.19; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **SELL** | `VOR` | 56 | $23.33 | $2.18 | $+69.58 | $6,559.87 | ▲ +69.58 after sell → book $10,249.02; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `SLS` | 106 | $12.40 | $2.34 | $+69.56 | $7,871.93 | ▲ +69.56 after sell → book $10,246.68; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TGTX` | 25 | $47.27 | $2.08 | $-64.90 | $9,051.59 | ▼ -64.90 after sell → book $10,244.59; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `BTSG` | 20 | $59.65 | $2.07 | $-7.12 | $10,242.52 | ▼ -7.12 after sell → book $10,242.52; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **BUY** | `QMCO` | 51 | $24.68 | $2.14 | — | $8,981.70 | — | rank by ret_5; rank ret_5; list yday_gainer; 🔵; ⚪; ret5=+111.3; leftover $1280.32 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 65 | $19.57 | $2.19 | — | $7,707.47 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $1280.32 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ZENA` | 581 | $2.20 | $7.49 | — | $6,421.77 | — | rank by ret_5; rank ret_5; list yday_gainer; 🔵; ⚪; ret5=+54.3; leftover $1280.32 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `AIRO` | 115 | $11.12 | $2.33 | — | $5,140.64 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; leftover $1280.32 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BRUN` | 48 | $26.25 | $2.13 | — | $3,878.74 | — | rank by ret_5; rank ret_5; list earn_react; 🔵; ⚪; ret5=+31.2; leftover $1280.32 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BCAR` | 210 | $6.09 | $2.71 | — | $2,597.13 | — | rank by ret_5; rank ret_5; list yday_gainer; 🔵; ⚪; ret5=+27.6; leftover $1280.32 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `TBBB` | 26 | $48.82 | $2.07 | — | $1,325.75 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.0; leftover $1280.32 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,325.75 | ▼ 09:30 equity $9,770.10 vs yday $9,841.43 (-71.33) | 09:30 open · cash $1,325.75 (unchanged overnight, no fees) · equity $9,770.10 vs prior close $9,841.43 (-71.33) because holdings re-marked: QMCO×51 yday $26.11 → 09:30 $24.83 -65.28; ARX×65 yday $19.58 → 09:30 $19.57 -0.65; ZENA×581 yday $2.14 → 09:30 $2.08 -31.96; AIRO×115 yday $9.57 → 09:30 $9.57 +0.00; BRUN×48 yday $22.93 → 09:30 $23.00 +3.36; BCAR×210 yday $5.83 → 09:30 $5.99 +33.60; TBBB×26 yday $47.79 → 09:30 $47.39 -10.40 | — |
| 2026-08-17 09:30 ET | **SELL** | `QMCO` | 51 | $24.83 | $2.16 | $+3.34 | $2,589.91 | ▲ +3.34 after sell → book $9,767.94; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 65 | $19.57 | $2.21 | $-4.39 | $3,859.76 | ▼ -4.39 after sell → book $9,765.73; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ZENA` | 581 | $2.08 | $7.60 | $-81.91 | $5,063.54 | ▼ -81.91 after sell → book $9,758.13; vs 09:30 mark -7.60 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `AIRO` | 115 | $9.57 | $2.36 | $-182.95 | $6,161.73 | ▼ -182.95 after sell → book $9,755.77; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BRUN` | 48 | $23.00 | $2.15 | $-160.05 | $7,263.57 | ▼ -160.05 after sell → book $9,753.61; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BCAR` | 210 | $5.99 | $2.75 | $-26.46 | $8,518.72 | ▼ -26.46 after sell → book $9,750.86; vs 09:30 mark -2.75 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `TBBB` | 26 | $47.39 | $2.09 | $-41.34 | $9,748.77 | ▼ -41.34 after sell → book $9,748.77; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `XHG` | 290 | $4.19 | $3.74 | — | $8,529.93 | — | rank by ret_5; rank ret_5; list yday_mover; ⚪; ret5=+291.8; leftover $1218.60 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `CAPR` | 177 | $6.87 | $2.52 | — | $7,311.42 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; ret5=+62.6; leftover $1218.60 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `STDN` | 89 | $13.64 | $2.26 | — | $6,095.20 | — | rank by ret_5; rank ret_5; list yday_gainer; 🔵; ⚪; ret5=+57.7; leftover $1218.60 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `HTFL` | 29 | $41.23 | $2.08 | — | $4,897.45 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; ret5=+46.0; leftover $1218.60 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `UMAC` | 37 | $32.55 | $2.10 | — | $3,691.00 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; ⚪; ret5=+30.4; leftover $1218.60 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `KOPN` | 224 | $5.43 | $2.89 | — | $2,471.79 | — | rank by ret_5; rank ret_5; list yday_gainer; ⚪; ret5=+28.8; leftover $1218.60 | join🟡 sector🟢 gen🟢 news🟡 vol🟡 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `NPWR` | 634 | $1.92 | $8.18 | — | $1,246.33 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; ⚪; ret5=+28.3; leftover $1218.60 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `SMJF` | 120 | $10.10 | $2.35 | — | $31.98 | — | rank by ret_5; rank ret_5; list mover_buy; ret5=+22.8; leftover $1218.60 | join🔴 sector🔴 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $31.98 | ▼ 09:30 equity $9,406.52 vs yday $9,543.43 (-136.91) | 09:30 open · cash $31.98 (unchanged overnight, no fees) · equity $9,406.52 vs prior close $9,543.43 (-136.91) because holdings re-marked: XHG×290 yday $3.91 → 09:30 $3.94 +8.70; CAPR×177 yday $7.45 → 09:30 $7.50 +8.85; STDN×89 yday $13.31 → 09:30 $13.31 +0.00; HTFL×29 yday $41.94 → 09:30 $41.50 -12.76; UMAC×37 yday $30.15 → 09:30 $28.59 -57.72; KOPN×224 yday $5.32 → 09:30 $5.03 -64.96; NPWR×634 yday $1.73 → 09:30 $1.70 -19.02; SMJF×120 yday $10.45 → 09:30 $10.45 +0.00 | — |
| 2026-08-18 09:30 ET | **SELL** | `XHG` | 290 | $3.94 | $3.80 | $-80.04 | $1,170.79 | ▼ -80.04 after sell → book $9,402.73; vs 09:30 mark -3.79 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `STDN` | 89 | $13.31 | $2.28 | $-33.91 | $2,353.09 | ▼ -33.91 after sell → book $9,400.44; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🔴 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `HTFL` | 29 | $41.50 | $2.10 | $+3.66 | $3,554.50 | ▲ +3.66 after sell → book $9,398.35; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `UMAC` | 37 | $28.59 | $2.12 | $-150.74 | $4,610.21 | ▼ -150.74 after sell → book $9,396.23; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `KOPN` | 224 | $5.03 | $2.94 | $-95.43 | $5,733.99 | ▼ -95.43 after sell → book $9,393.29; vs 09:30 mark -2.94 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `NPWR` | 634 | $1.70 | $8.29 | $-155.95 | $6,803.50 | ▼ -155.95 after sell → book $9,385.00; vs 09:30 mark -8.29 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `SMJF` | 120 | $10.45 | $2.38 | $+37.27 | $8,055.12 | ▲ +37.27 after sell → book $9,382.62; vs 09:30 mark -2.38 | dropped from list after 1 sess (min 1) | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,055.12 | ▲ 09:30 equity $9,327.75 vs yday $9,308.28 (+19.47) | 09:30 open · cash $8,055.12 (unchanged overnight, no fees) · equity $9,327.75 vs prior close $9,308.28 (+19.47) because holdings re-marked: CAPR×177 yday $7.08 → 09:30 $7.19 +19.47 | — |
| 2026-08-19 09:30 ET | **SELL** | `CAPR` | 177 | $7.19 | $2.56 | $+51.56 | $9,325.18 | ▲ +51.56 after sell → book $9,325.18; vs 09:30 mark -2.57 | dropped from list after 2 sess (min 1) | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,325.18 | ▲ 09:30 equity $9,325.18 vs yday $9,325.18 (+0.00) | 09:30 open · cash $9,325.18 · no holdings · equity $9,325.18 vs prior close $9,325.18 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 7 | $150.14 | $2.01 | — | $8,272.19 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ret5=+173.9; leftover $1165.65 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CYPH` | 1013 | $1.15 | $13.07 | — | $7,094.18 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+62.6; leftover $1165.65 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AZI` | 850 | $1.37 | $10.96 | — | $5,918.71 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ret5=+30.7; leftover $1165.65 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BTGO` | 176 | $6.61 | $2.52 | — | $4,753.71 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ret5=+25.3; leftover $1165.65 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟡 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BNTX` | 10 | $109.06 | $2.02 | — | $3,661.09 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ret5=+22.0; leftover $1165.65 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AUTL` | 471 | $2.47 | $6.08 | — | $2,491.65 | — | rank by ret_5; rank ret_5; list mover_buy; 🔵; ⚪; ret5=+19.8; leftover $1165.65 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ASST` | 72 | $16.00 | $2.21 | — | $1,337.44 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ret5=+18.9; leftover $1165.65 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BRR` | 560 | $2.08 | $7.22 | — | $165.42 | — | rank by ret_5; rank ret_5; list ohlc_hot; 🔵; ret5=+18.0; leftover $1165.65 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $165.42 | ▲ 09:30 equity $9,702.64 vs yday $9,373.05 (+329.59) | 09:30 open · cash $165.42 (unchanged overnight, no fees) · equity $9,702.64 vs prior close $9,373.05 (+329.59) because holdings re-marked: MRNA×7 yday $133.32 → 09:30 $133.11 -1.47; CYPH×1013 yday $1.19 → 09:30 $1.32 +131.69; AZI×850 yday $1.44 → 09:30 $1.46 +17.00; BTGO×176 yday $6.60 → 09:30 $6.95 +61.60; BNTX×10 yday $110.89 → 09:30 $110.92 +0.30; AUTL×471 yday $2.46 → 09:30 $2.47 +4.71; ASST×72 yday $16.13 → 09:30 $17.66 +110.16; BRR×560 yday $2.24 → 09:30 $2.25 +5.60 | — |
| 2026-08-21 09:30 ET | **SELL** | `AZI` | 850 | $1.46 | $11.12 | $+54.42 | $1,395.30 | ▲ +54.42 after sell → book $9,691.52; vs 09:30 mark -11.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BTGO` | 176 | $6.95 | $2.56 | $+55.64 | $2,615.94 | ▲ +55.64 after sell → book $9,688.96; vs 09:30 mark -2.56 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BNTX` | 10 | $110.92 | $2.04 | $+14.54 | $3,723.10 | ▲ +14.54 after sell → book $9,686.92; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `AUTL` | 471 | $2.47 | $6.16 | $-12.24 | $4,880.31 | ▼ -12.24 after sell → book $9,680.76; vs 09:30 mark -6.16 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `ASST` | 72 | $17.66 | $2.23 | $+115.09 | $6,149.60 | ▲ +115.09 after sell → book $9,678.53; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BRR` | 560 | $2.25 | $7.33 | $+80.65 | $7,402.27 | ▲ +80.65 after sell → book $9,671.20; vs 09:30 mark -7.33 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `CAPR` | 181 | $6.81 | $2.53 | — | $6,167.13 | — | rank by ret_5; rank ret_5; list yday_mover; 🔵; ret5=+62.5; leftover $1233.71 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 110 | $11.13 | $2.32 | — | $4,940.51 | — | rank by ret_5; rank ret_5; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $1233.71 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `IOVA` | 135 | $9.08 | $2.40 | — | $3,712.32 | — | rank by ret_5; rank ret_5; list yday_gainer; 🔵; ⚪; ret5=+37.9; leftover $1233.71 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `INO` | 1003 | $1.23 | $12.94 | — | $2,465.69 | — | rank by ret_5; rank ret_5; list mover_buy; 🔵; ⚪; ret5=+34.4; leftover $1233.71 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CAN` | 4196 | $0.29 | $24.92 | — | $1,207.14 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ret5=+30.4; leftover $1233.71 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `INDP` | 860 | $1.39 | $11.09 | — | $0.65 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+30.2; leftover $1233.71 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.65 | ▲ 09:30 equity $10,712.98 vs yday $9,974.68 (+738.30) | 09:30 open · cash $0.65 (unchanged overnight, no fees) · equity $10,712.98 vs prior close $9,974.68 (+738.30) because holdings re-marked: MRNA×7 yday $145.13 → 09:30 $142.70 -17.01; CYPH×1013 yday $1.42 → 09:30 $1.83 +415.33; CAPR×181 yday $6.29 → 09:30 $8.01 +311.32; ARCT×110 yday $13.45 → 09:30 $13.26 -20.90; IOVA×135 yday $8.29 → 09:30 $8.05 -32.40; INO×1003 yday $1.18 → 09:30 $1.20 +20.06; CAN×4196 yday $0.35 → 09:30 $0.38 +104.90; INDP×860 yday $1.29 → 09:30 $1.24 -43.00 | — |
| 2026-08-24 09:30 ET | **SELL** | `MRNA` | 7 | $142.70 | $2.03 | $-56.12 | $997.51 | ▼ -56.12 after sell → book $10,710.94; vs 09:30 mark -2.04 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 1013 | $1.83 | $13.25 | $+662.52 | $2,838.05 | ▲ +662.52 after sell → book $10,697.69; vs 09:30 mark -13.25 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CAPR` | 181 | $8.01 | $2.57 | $+212.09 | $4,285.29 | ▲ +212.09 after sell → book $10,695.12; vs 09:30 mark -2.57 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 110 | $13.26 | $2.35 | $+229.63 | $5,741.54 | ▲ +229.63 after sell → book $10,692.77; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `IOVA` | 135 | $8.05 | $2.43 | $-143.87 | $6,825.86 | ▼ -143.87 after sell → book $10,690.34; vs 09:30 mark -2.43 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `INO` | 1003 | $1.20 | $13.12 | $-56.14 | $8,016.35 | ▼ -56.14 after sell → book $10,677.23; vs 09:30 mark -13.11 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟡 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `CAN` | 4196 | $0.38 | $29.24 | $+306.69 | $9,581.58 | ▲ +306.69 after sell → book $10,647.98; vs 09:30 mark -29.25 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 judge🔴 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `INDP` | 860 | $1.24 | $11.25 | $-151.34 | $10,636.74 | ▼ -151.34 after sell → book $10,636.74; vs 09:30 mark -11.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,636.74 | ▲ 09:30 equity $10,636.74 vs yday $10,636.74 (-0.00) | 09:30 open · cash $10,636.74 · no holdings · equity $10,636.74 vs prior close $10,636.74 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `CYPH` | 782 | $1.70 | $10.09 | — | $9,297.25 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+120.5; leftover $1329.59 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ASST` | 63 | $20.90 | $2.18 | — | $7,978.37 | — | rank by ret_5; rank ret_5; list yday_gainer; 🔵; ret5=+47.9; leftover $1329.59 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `DFDV` | 309 | $4.29 | $3.99 | — | $6,648.78 | — | rank by ret_5; rank ret_5; list yday_gainer; 🔵; ret5=+28.3; leftover $1329.59 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BMNR` | 53 | $24.73 | $2.15 | — | $5,335.94 | — | rank by ret_5; rank ret_5; list yday_gainer; ret5=+26.3; leftover $1329.59 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟡 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 11 | $119.46 | $2.02 | — | $4,019.85 | — | rank by ret_5; rank ret_5; list mover_buy; 🔵; ⚪; ret5=+25.9; leftover $1329.59 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `RUM` | 142 | $9.36 | $2.42 | — | $2,688.32 | — | rank by ret_5; rank ret_5; list yday_gainer; 🔵; ret5=+21.3; leftover $1329.59 | join🔴 sector🟡 gen🟡 news🟢 digest🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `HMY` | 58 | $22.65 | $2.16 | — | $1,372.45 | — | rank by ret_5; rank ret_5; list mover_buy; ⚪; ret5=+21.1; leftover $1329.59 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `FWDI` | 221 | $5.99 | $2.85 | — | $45.81 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ret5=+20.7; leftover $1329.59 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $45.81 | ▲ 09:30 equity $10,401.27 vs yday $10,401.27 (+0.00) | 09:30 open · cash $45.81 (unchanged overnight, no fees) · equity $10,401.27 vs prior close $10,401.27 (+0.00) because holdings re-marked: CYPH×782 yday $1.64 → 09:30 $1.64 +0.00; ASST×63 yday $20.20 → 09:30 $20.20 +0.00; DFDV×309 yday $4.16 → 09:30 $4.16 +0.00; BMNR×53 yday $24.21 → 09:30 $24.21 +0.00; AU×11 yday $118.55 → 09:30 $118.55 +0.00; RUM×142 yday $9.35 → 09:30 $9.35 +0.00; HMY×58 yday $22.50 → 09:30 $22.50 +0.00; FWDI×221 yday $5.86 → 09:30 $5.86 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $45.81 | ▼ 09:30 equity $10,596.97 vs yday $10,608.88 (-11.91) | 09:30 open · cash $45.81 (unchanged overnight, no fees) · equity $10,596.97 vs prior close $10,608.88 (-11.91) because holdings re-marked: CYPH×782 yday $1.64 → 09:30 $1.60 -31.28; ASST×63 yday $20.20 → 09:30 $20.72 +32.76; DFDV×309 yday $4.16 → 09:30 $4.35 +58.71; BMNR×53 yday $24.21 → 09:30 $24.24 +1.59; AU×11 yday $118.55 → 09:30 $119.80 +13.75; RUM×142 yday $9.35 → 09:30 $10.07 +102.24; HMY×58 yday $22.50 → 09:30 $22.39 -6.38; FWDI×221 yday $5.86 → 09:30 $5.97 +24.31 | — |
| 2026-08-27 09:30 ET | **SELL** | `CYPH` | 782 | $1.60 | $10.23 | $-98.52 | $1,286.78 | ▼ -98.52 after sell → book $10,586.74; vs 09:30 mark -10.23 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ASST` | 63 | $20.72 | $2.20 | $-15.72 | $2,589.94 | ▼ -15.72 after sell → book $10,584.54; vs 09:30 mark -2.20 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `DFDV` | 309 | $4.35 | $4.05 | $+10.51 | $3,930.05 | ▲ +10.51 after sell → book $10,580.50; vs 09:30 mark -4.04 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BMNR` | 53 | $24.24 | $2.17 | $-30.29 | $5,212.60 | ▼ -30.29 after sell → book $10,578.33; vs 09:30 mark -2.17 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `AU` | 11 | $119.80 | $2.04 | $-0.33 | $6,528.35 | ▼ -0.33 after sell → book $10,576.28; vs 09:30 mark -2.05 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `RUM` | 142 | $10.07 | $2.45 | $+95.95 | $7,955.84 | ▲ +95.95 after sell → book $10,573.83; vs 09:30 mark -2.45 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `HMY` | 58 | $22.39 | $2.18 | $-19.43 | $9,252.28 | ▼ -19.43 after sell → book $10,571.65; vs 09:30 mark -2.18 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `FWDI` | 221 | $5.97 | $2.90 | $-10.17 | $10,568.75 | ▼ -10.17 after sell → book $10,568.75; vs 09:30 mark -2.90 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `MOS` | 53 | $24.84 | $2.15 | — | $9,250.08 | — | rank by ret_5; rank ret_5; list flatten; ret5=+13.0; leftover $1321.09 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟢 |
| 2026-08-27 09:30 ET | **BUY** | `DLO` | 84 | $15.60 | $2.24 | — | $7,937.44 | — | rank by ret_5; rank ret_5; list mover_buy; 🔵; ret5=+7.1; leftover $1321.09 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MRVL` | 5 | $240.00 | $2.00 | — | $6,735.43 | — | rank by ret_5; rank ret_5; list mover_buy; 🔵; ret5=+6.8; leftover $1321.09 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 510 | $2.59 | $6.58 | — | $5,407.96 | — | rank by ret_5; rank ret_5; list flatten; ret5=+4.2; leftover $1321.09 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `PLTR` | 7 | $170.60 | $2.01 | — | $4,211.74 | — | rank by ret_5; rank ret_5; list mover_buy; 🔵; ret5=+3.4; leftover $1321.09 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `TX` | 23 | $55.20 | $2.06 | — | $2,940.09 | — | rank by ret_5; rank ret_5; list mover_buy; 🔵; ret5=+3.0; leftover $1321.09 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 32 | $40.72 | $2.09 | — | $1,634.96 | — | rank by ret_5; rank ret_5; list flatten; ret5=+1.8; leftover $1321.09 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `GEN` | 45 | $28.89 | $2.12 | — | $332.78 | — | rank by ret_5; rank ret_5; list mover_buy; 🔵; ret5=+1.6; leftover $1321.09 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $332.78 | ▲ 09:30 equity $10,676.13 vs yday $10,634.04 (+42.09) | 09:30 open · cash $332.78 (unchanged overnight, no fees) · equity $10,676.13 vs prior close $10,634.04 (+42.09) because holdings re-marked: MOS×53 yday $24.16 → 09:30 $24.00 -8.48; DLO×84 yday $15.36 → 09:30 $15.33 -2.52; MRVL×5 yday $245.11 → 09:30 $253.44 +41.65; SLI×510 yday $2.61 → 09:30 $2.60 -5.10; PLTR×7 yday $177.50 → 09:30 $178.75 +8.75; TX×23 yday $55.13 → 09:30 $55.25 +2.76; RRC×32 yday $41.55 → 09:30 $41.44 -3.52; GEN×45 yday $29.64 → 09:30 $29.83 +8.55 | — |
| 2026-08-28 09:30 ET | **SELL** | `MOS` | 53 | $24.00 | $2.17 | $-48.84 | $1,602.62 | ▼ -48.84 after sell → book $10,673.97; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `DLO` | 84 | $15.33 | $2.27 | $-27.19 | $2,888.07 | ▼ -27.19 after sell → book $10,671.70; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MRVL` | 5 | $253.44 | $2.03 | $+63.17 | $4,153.24 | ▲ +63.17 after sell → book $10,669.67; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `SLI` | 510 | $2.60 | $6.67 | $-8.15 | $5,472.57 | ▼ -8.15 after sell → book $10,663.00; vs 09:30 mark -6.67 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `PLTR` | 7 | $178.75 | $2.03 | $+53.01 | $6,721.79 | ▲ +53.01 after sell → book $10,660.97; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `TX` | 23 | $55.25 | $2.08 | $-2.99 | $7,990.46 | ▼ -2.99 after sell → book $10,658.89; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `RRC` | 32 | $41.44 | $2.11 | $+18.85 | $9,314.43 | ▲ +18.85 after sell → book $10,656.78; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `GEN` | 45 | $29.83 | $2.15 | $+38.03 | $10,654.64 | ▲ +38.03 after sell → book $10,654.64; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `FIGR` | 35 | $37.42 | $2.10 | — | $9,342.84 | — | rank by ret_5; rank ret_5; list yday_mover; ret5=+24.4; leftover $1331.83 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 heat🟡 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `WPM` | 8 | $155.89 | $2.01 | — | $8,093.71 | — | rank by ret_5; rank ret_5; list ohlc_hot; ret5=+17.6; leftover $1331.83 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟡 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SCCO` | 6 | $214.82 | $2.01 | — | $6,802.78 | — | rank by ret_5; rank ret_5; list ohlc_hot; ret5=+17.0; leftover $1331.83 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `AMTX` | 712 | $1.87 | $9.18 | — | $5,462.16 | — | rank by ret_5; rank ret_5; list ohlc_hot; ret5=+16.9; leftover $1331.83 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SBSW` | 110 | $12.01 | $2.32 | — | $4,138.74 | — | rank by ret_5; rank ret_5; list ohlc_hot; ret5=+16.8; leftover $1331.83 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟡 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `EQX` | 98 | $13.57 | $2.28 | — | $2,806.59 | — | rank by ret_5; rank ret_5; list ohlc_hot; ret5=+16.6; leftover $1331.83 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟡 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ERO` | 33 | $39.20 | $2.09 | — | $1,510.90 | — | rank by ret_5; rank ret_5; list ohlc_hot; ret5=+16.6; leftover $1331.83 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `TXG` | 20 | $64.10 | $2.05 | — | $226.85 | — | rank by ret_5; rank ret_5; list ohlc_hot; ret5=+16.1; leftover $1331.83 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $226.85 | ▼ 09:30 equity $10,372.35 vs yday $10,728.35 (-356.00) | 09:30 open · cash $226.85 (unchanged overnight, no fees) · equity $10,372.35 vs prior close $10,728.35 (-356.00) because holdings re-marked: FIGR×35 yday $38.02 → 09:30 $35.50 -88.20; WPM×8 yday $157.99 → 09:30 $152.49 -44.00; SCCO×6 yday $216.28 → 09:30 $207.95 -49.98; AMTX×712 yday $1.87 → 09:30 $1.90 +21.36; SBSW×110 yday $12.26 → 09:30 $12.14 -13.20; EQX×98 yday $13.45 → 09:30 $12.81 -62.72; ERO×33 yday $39.82 → 09:30 $38.60 -40.26; TXG×20 yday $64.85 → 09:30 $60.90 -79.00 | — |
| 2026-08-31 09:30 ET | **SELL** | `FIGR` | 35 | $35.50 | $2.12 | $-71.41 | $1,467.24 | ▼ -71.41 after sell → book $10,370.24; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `WPM` | 8 | $152.49 | $2.03 | $-31.25 | $2,685.12 | ▼ -31.25 after sell → book $10,368.20; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `SCCO` | 6 | $207.95 | $2.03 | $-45.26 | $3,930.80 | ▼ -45.26 after sell → book $10,366.18; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `AMTX` | 712 | $1.90 | $9.31 | $+2.86 | $5,274.28 | ▲ +2.86 after sell → book $10,356.86; vs 09:30 mark -9.32 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `SBSW` | 110 | $12.14 | $2.35 | $+9.63 | $6,607.33 | ▲ +9.63 after sell → book $10,354.51; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🟡 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `EQX` | 98 | $12.81 | $2.31 | $-79.07 | $7,860.40 | ▼ -79.07 after sell → book $10,352.20; vs 09:30 mark -2.31 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `ERO` | 33 | $38.60 | $2.11 | $-24.00 | $9,132.09 | ▼ -24.00 after sell → book $10,350.09; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-31 09:30 ET | **SELL** | `TXG` | 20 | $60.90 | $2.07 | $-68.12 | $10,348.02 | ▼ -68.12 after sell → book $10,348.02; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,348.02 | ▲ 09:30 equity $10,348.02 vs yday $10,348.02 (+0.00) | 09:30 open · cash $10,348.02 · no holdings · equity $10,348.02 vs prior close $10,348.02 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,348.02 | ▲ 09:30 equity $10,348.02 vs yday $10,348.02 (+0.00) | 09:30 open · cash $10,348.02 · no holdings · equity $10,348.02 vs prior close $10,348.02 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,348.02 | ▲ 09:30 equity $10,348.02 vs yday $10,348.02 (+0.00) | 09:30 open · cash $10,348.02 · no holdings · equity $10,348.02 vs prior close $10,348.02 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `MRNA` | 8 | $151.40 | $2.01 | — | $9,134.81 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ret5=+129.2; leftover $1293.50 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 78 | $16.46 | $2.22 | — | $7,848.70 | — | rank by ret_5; rank ret_5; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+63.4; leftover $1293.50 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CAN` | 4311 | $0.30 | $25.87 | — | $6,529.54 | — | rank by ret_5; rank ret_5; list yday_mover; 🔵; ret5=+54.3; leftover $1293.50 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ALEC` | 538 | $2.40 | $6.94 | — | $5,231.40 | — | rank by ret_5; rank ret_5; list yday_gainer; 🔵; ret5=+20.4; leftover $1293.50 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DEFT` | 1930 | $0.67 | $18.72 | — | $3,919.58 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+17.6; leftover $1293.50 | join🟡 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `WPM` | 8 | $148.89 | $2.01 | — | $2,726.44 | — | rank by ret_5; rank ret_5; list ohlc_hot; 🔵; ret5=+17.6; leftover $1293.50 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🔴 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FUTU` | 10 | $119.46 | $2.02 | — | $1,529.82 | — | rank by ret_5; rank ret_5; list ohlc_hot; 🔵; ret5=+17.5; leftover $1293.50 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `SCCO` | 6 | $204.50 | $2.01 | — | $300.82 | — | rank by ret_5; rank ret_5; list ohlc_hot; 🔵; ret5=+17.0; leftover $1293.50 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🔴 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $300.82 | ▲ 09:30 equity $10,628.72 vs yday $10,480.93 (+147.79) | 09:30 open · cash $300.82 (unchanged overnight, no fees) · equity $10,628.72 vs prior close $10,480.93 (+147.79) because holdings re-marked: MRNA×8 yday $150.81 → 09:30 $145.95 -38.88; ARCT×78 yday $16.74 → 09:30 $16.77 +2.34; CAN×4311 yday $0.31 → 09:30 $0.34 +129.33; ALEC×538 yday $2.72 → 09:30 $2.70 -10.76; DEFT×1930 yday $0.65 → 09:30 $0.65 +0.00; WPM×8 yday $150.91 → 09:30 $155.85 +39.52; FUTU×10 yday $118.08 → 09:30 $118.19 +1.10; SCCO×6 yday $204.26 → 09:30 $208.45 +25.14 | — |
| 2026-09-04 09:30 ET | **SELL** | `MRNA` | 8 | $145.95 | $2.03 | $-47.65 | $1,466.38 | ▼ -47.65 after sell → book $10,626.68; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `ARCT` | 78 | $16.77 | $2.25 | $+19.71 | $2,772.19 | ▲ +19.71 after sell → book $10,624.43; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CAN` | 4311 | $0.34 | $28.32 | $+118.26 | $4,209.62 | ▲ +118.26 after sell → book $10,596.12; vs 09:30 mark -28.31 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 276 | $5.08 | $3.56 | — | $2,803.98 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+28.1; leftover $1403.21 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BRR` | 594 | $2.36 | $7.66 | — | $1,394.47 | — | rank by ret_5; rank ret_5; list yday_gainer; 🔵; ⚪; ret5=+28.0; leftover $1403.21 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `HQ` | 81 | $17.06 | $2.23 | — | $10.38 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; ret5=+17.3; leftover $1403.21 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 ab🟡 heat🔴 vol🔴 buy🟡 |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `SNDK` | cash | leftover split 1280.32 < 1 share @ 1646.93 |
| 2026-08-18 | `AVAH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FDMT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AXTI` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FIGR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CRDL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `QTRX` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ALM` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `ARX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `WEAV` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `NMAX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ANGX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `IVVD` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DVLT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `ALM` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HMY` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `KGC` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `TMC` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `GFI` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DEFT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SCCO` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SBSW` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-26 | `CYPH` | no_price | no 09:30 open — carry |
| 2026-08-26 | `ASST` | no_price | no 09:30 open — carry |
| 2026-08-26 | `DFDV` | no_price | no 09:30 open — carry |
| 2026-08-26 | `BMNR` | no_price | no 09:30 open — carry |
| 2026-08-26 | `AU` | no_price | no 09:30 open — carry |
| 2026-08-26 | `RUM` | no_price | no 09:30 open — carry |
| 2026-08-26 | `HMY` | no_price | no 09:30 open — carry |
| 2026-08-26 | `FWDI` | no_price | no 09:30 open — carry |
| 2026-08-26 | `USDE` | no_price | no 09:30 open |
| 2026-08-26 | `BRR` | no_price | no 09:30 open |
| 2026-08-26 | `SENS` | no_price | no 09:30 open |
| 2026-08-26 | `INDP` | no_price | no 09:30 open |
| 2026-08-26 | `SUJA` | no_price | no 09:30 open |
| 2026-08-26 | `DEFT` | no_price | no 09:30 open |
| 2026-08-26 | `WPM` | no_price | no 09:30 open |
| 2026-08-26 | `FUTU` | no_price | no 09:30 open |
| 2026-08-31 | `USDE` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CYPH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PURR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CAN` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `DFDV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `MSTR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `FWDI` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `MPC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `CYPH` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `ALEC` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `SUJA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `SBSW` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `KOS` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `TXG` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `TRLV` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `GUTS` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `USDE` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PRQR` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `SUJA` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `WPM` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `SSRM` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `SCCO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `SBSW` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `EQX` | hard_red | hard-red S=-3.83 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `ALEC` | 538 | 2026-09-03 @ $2.40 | rank by ret_5; rank ret_5; list yday_gainer; 🔵; ret5=+20.4; leftover $1293.50 |
| `DEFT` | 1930 | 2026-09-03 @ $0.67 | rank by ret_5; rank ret_5; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+17.6; leftover $1293.50 |
| `WPM` | 8 | 2026-09-03 @ $148.89 | rank by ret_5; rank ret_5; list ohlc_hot; 🔵; ret5=+17.6; leftover $1293.50 |
| `FUTU` | 10 | 2026-09-03 @ $119.46 | rank by ret_5; rank ret_5; list ohlc_hot; 🔵; ret5=+17.5; leftover $1293.50 |
| `SCCO` | 6 | 2026-09-03 @ $204.50 | rank by ret_5; rank ret_5; list ohlc_hot; 🔵; ret5=+17.0; leftover $1293.50 |
| `OABI` | 276 | 2026-09-04 @ $5.08 | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+28.1; leftover $1403.21 |
| `BRR` | 594 | 2026-09-04 @ $2.36 | rank by ret_5; rank ret_5; list yday_gainer; 🔵; ⚪; ret5=+28.0; leftover $1403.21 |
| `HQ` | 81 | 2026-09-04 @ $17.06 | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; ret5=+17.3; leftover $1403.21 |
