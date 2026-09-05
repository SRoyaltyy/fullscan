# Factor mine action — `union_h1_sboost`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `both` · S≥+5: sizeup + more names

Cash book **+14.59%** ($11,459) · signal-only (no cash/fees) was +18.57%. Starts YES **16/17**. Fills 146 · skips 52 · realized $+1084.89.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `list order` · **top_n** 8 (S≥+5 may raise this when S-boost is `both`).
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $66.06.

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | vs yday close | Bought | Sold | Close cash | Close equity | Close held | 09:30 why |
|---|---:|---:|---|---:|---:|---|---|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | BTSG, IREN, TPG, TGTX, SLS, HIMS, INO, TNDM, VOR | — | $123.82 | $10,195.74 | BTSG×18, IREN×24, TPG×21, TGTX×22, SLS×94, HIMS×37, INO×1371, TNDM×47, VOR×50 | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-14 | +5.50 | $123.82 | BTSG×18, IREN×24, TPG×21, TGTX×22, SLS×94, HIMS×37, INO×1371, TNDM×47, VOR×50 | $10,219.63 | +23.89 | TLN, VST, NRG, DAVE, SLG, MARA, LDI, BTBT, BETR, ANGX, WWW, HYLN | BTSG, IREN, TPG, TGTX, SLS, HIMS, INO, TNDM, VOR | $458.79 | $10,152.17 | TLN×2, VST×5, NRG×7, DAVE×2, SLG×14, MARA×94, LDI×905, BTBT×565, BETR×57, ANGX×196, WWW×41, HYLN×203 | 09:30 open · cash $123.82 (unchanged overnight, no fees) · equity $10,219.63 vs prior close $10,195.74 (+23.89) because holdings re-marked: BTSG×18 yday $60.23 → 09:30 $59.65 -10.44; IREN×24 yday $44.76 → 09:30 $44.09 -16.08; TPG×21 yday $54.62 → 09:30 $55.29 +14.07; TGTX×22 yday $47.94 → 09:30 $47.27 -14.74; SLS×94 yday $12.36 → 09:30 $12.40 +3.76; HIMS×37 yday $28.77 → 09:30 $29.15 +14.06; INO×1371 yday $0.90 → 09:30 $0.93 +41.13; TNDM×47 yday $23.13 → 09:30 $22.92 -9.87; VOR×50 yday $23.29 → 09:30 $23.33 +2.00 |
| 2026-08-17 | +2.25 | $458.79 | TLN×2, VST×5, NRG×7, DAVE×2, SLG×14, MARA×94, LDI×905, BTBT×565, BETR×57, ANGX×196, WWW×41, HYLN×203 | $10,201.84 | +49.67 | DVN, EOG, FANG, TMC, TGB, ELF, DNN, HNST | TLN, VST, NRG, DAVE, SLG, MARA, LDI, BTBT, BETR, ANGX, WWW, HYLN | $188.92 | $10,178.28 | DVN×27, EOG×8, FANG×6, TMC×313, TGB×150, ELF×14, DNN×392, HNST×264 | 09:30 open · cash $458.79 (unchanged overnight, no fees) · equity $10,201.84 vs prior close $10,152.17 (+49.67) because holdings re-marked: TLN×2 yday $362.74 → 09:30 $367.88 +10.28; VST×5 yday $148.13 → 09:30 $149.37 +6.20; NRG×7 yday $126.24 → 09:30 $127.40 +8.12; DAVE×2 yday $334.57 → 09:30 $336.94 +4.74; SLG×14 yday $56.09 → 09:30 $55.37 -10.08; MARA×94 yday $9.20 → 09:30 $9.22 +1.88; LDI×905 yday $0.90 → 09:30 $0.91 +9.05; BTBT×565 yday $1.57 → 09:30 $1.52 -28.25; BETR×57 yday $13.73 → 09:30 $13.67 -3.42; ANGX×196 yday $4.37 → 09:30 $4.60 +45.08; WWW×41 yday $21.03 → 09:30 $20.98 -2.05; HYLN×203 yday $4.06 → 09:30 $4.10 +8.12 |
| 2026-08-18 | -6.20 | $188.92 | DVN×27, EOG×8, FANG×6, TMC×313, TGB×150, ELF×14, DNN×392, HNST×264 | $10,129.84 | -48.44 | — | DVN, EOG, FANG, TMC, TGB, ELF, DNN, HNST | $10,106.47 | $10,106.47 | — | 09:30 open · cash $188.92 (unchanged overnight, no fees) · equity $10,129.84 vs prior close $10,178.28 (-48.44) because holdings re-marked: DVN×27 yday $47.57 → 09:30 $48.00 +11.61; EOG×8 yday $146.15 → 09:30 $148.04 +15.12; FANG×6 yday $206.29 → 09:30 $208.93 +15.84; TMC×313 yday $3.77 → 09:30 $3.72 -15.65; TGB×150 yday $8.77 → 09:30 $8.55 -33.00; ELF×14 yday $93.66 → 09:30 $93.44 -3.08; DNN×392 yday $3.19 → 09:30 $3.11 -31.36; HNST×264 yday $4.70 → 09:30 $4.67 -7.92 |
| 2026-08-19 | -7.20 | $10,106.47 | — | $10,106.47 | -0.00 | — | — | $10,106.47 | $10,106.47 | — | 09:30 open · cash $10,106.47 · no holdings · equity $10,106.47 vs prior close $10,106.47 (-0.00). Cash unchanged overnight; no fees. |
| 2026-08-20 | +1.12 | $10,106.47 | — | $10,106.47 | -0.00 | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | — | $208.63 | $10,316.19 | AG×61, BHP×13, CDE×61, HDSN×218, IAG×64, KGC×42, NFGC×721, WPM×8 | 09:30 open · cash $10,106.47 · no holdings · equity $10,106.47 vs prior close $10,106.47 (-0.00). Cash unchanged overnight; no fees. |
| 2026-08-21 | +3.25 | $208.63 | AG×61, BHP×13, CDE×61, HDSN×218, IAG×64, KGC×42, NFGC×721, WPM×8 | $10,585.91 | +269.72 | AU, AUPH, AEM, ARCT, AUTL, CRDL, CRSP, CYPH | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | $18.48 | $10,787.08 | AU×11, AUPH×76, AEM×6, ARCT×118, AUTL×534, CRDL×683, CRSP×22, CYPH×1000 | 09:30 open · cash $208.63 (unchanged overnight, no fees) · equity $10,585.91 vs prior close $10,316.19 (+269.72) because holdings re-marked: AG×61 yday $21.19 → 09:30 $21.90 +43.31; BHP×13 yday $93.63 → 09:30 $95.72 +27.17; CDE×61 yday $21.11 → 09:30 $21.75 +39.04; HDSN×218 yday $5.57 → 09:30 $5.67 +21.80; IAG×64 yday $20.50 → 09:30 $21.17 +42.88; KGC×42 yday $31.43 → 09:30 $32.17 +31.08; NFGC×721 yday $1.75 → 09:30 $1.79 +28.84; WPM×8 yday $150.25 → 09:30 $154.70 +35.60 |
| 2026-08-24 | -5.17 | $18.48 | AU×11, AUPH×76, AEM×6, ARCT×118, AUTL×534, CRDL×683, CRSP×22, CYPH×1000 | $11,133.27 | +346.19 | — | AU, AUPH, AEM, ARCT, AUTL, CRDL, CRSP, CYPH | $11,093.50 | $11,093.50 | — | 09:30 open · cash $18.48 (unchanged overnight, no fees) · equity $11,133.27 vs prior close $10,787.08 (+346.19) because holdings re-marked: AU×11 yday $121.22 → 09:30 $120.50 -7.92; AUPH×76 yday $16.65 → 09:30 $16.60 -3.80; AEM×6 yday $216.06 → 09:30 $217.03 +5.82; ARCT×118 yday $13.45 → 09:30 $13.26 -22.42; AUTL×534 yday $2.41 → 09:30 $2.36 -26.70; CRDL×683 yday $1.86 → 09:30 $1.87 +6.83; CRSP×22 yday $59.50 → 09:30 $58.79 -15.62; CYPH×1000 yday $1.42 → 09:30 $1.83 +410.00 |
| 2026-08-25 | +1.80 | $11,093.50 | — | $11,093.50 | +0.00 | MOS, OCUL, INSP, CRMD, RZLT, HCA, BMEA, NPWR | — | $135.25 | $11,063.78 | MOS×57, OCUL×126, INSP×22, CRMD×167, RZLT×265, HCA×3, BMEA×855, NPWR×693 | 09:30 open · cash $11,093.50 · no holdings · equity $11,093.50 vs prior close $11,093.50 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-26 | +2.02 | $135.25 | MOS×57, OCUL×126, INSP×22, CRMD×167, RZLT×265, HCA×3, BMEA×855, NPWR×693 | $11,063.78 | -0.00 | — | — | $135.25 | $11,059.04 | MOS×57, OCUL×126, INSP×22, CRMD×167, RZLT×265, HCA×3, BMEA×855, NPWR×693 | 09:30 open · cash $135.25 (unchanged overnight, no fees) · equity $11,063.78 vs prior close $11,063.78 (-0.00) because holdings re-marked: MOS×57 yday $23.75 → 09:30 $23.75 +0.00; OCUL×126 yday $10.92 → 09:30 $10.92 +0.00; INSP×22 yday $61.47 → 09:30 $61.47 +0.00; CRMD×167 yday $8.28 → 09:30 $8.28 +0.00; RZLT×265 yday $5.29 → 09:30 $5.29 +0.00; HCA×3 yday $428.50 → 09:30 $428.50 +0.00; BMEA×855 yday $1.61 → 09:30 $1.61 +0.00; NPWR×693 yday $2.02 → 09:30 $2.02 +0.00 |
| 2026-08-27 | — | $135.25 | MOS×57, OCUL×126, INSP×22, CRMD×167, RZLT×265, HCA×3, BMEA×855, NPWR×693 | $11,112.30 | +53.26 | RRC, CRK, SLI, ACMR, GGB, MT, MU | OCUL, INSP, CRMD, RZLT, HCA, BMEA, NPWR | $517.93 | $11,080.16 | MOS×57, RRC×33, CRK×97, SLI×533, ACMR×17, GGB×312, MT×18, MU×1 | 09:30 open · cash $135.25 (unchanged overnight, no fees) · equity $11,112.30 vs prior close $11,059.04 (+53.26) because holdings re-marked: MOS×57 yday $23.75 → 09:30 $24.84 +62.13; OCUL×126 yday $10.92 → 09:30 $10.79 -16.38; INSP×22 yday $61.47 → 09:30 $60.07 -30.80; CRMD×167 yday $8.28 → 09:30 $8.60 +53.44; RZLT×265 yday $5.29 → 09:30 $5.01 -74.20; HCA×3 yday $428.50 → 09:30 $427.50 -3.00; BMEA×855 yday $1.61 → 09:30 $1.75 +119.70; NPWR×693 yday $2.02 → 09:30 $1.93 -62.37 |
| 2026-08-28 | +0.75 | $517.93 | MOS×57, RRC×33, CRK×97, SLI×533, ACMR×17, GGB×312, MT×18, MU×1 | $11,160.61 | +80.45 | ANF, BHVN, BZ, CAPR | ACMR, GGB, MT, MU | $100.18 | $11,210.77 | MOS×57, RRC×33, CRK×97, SLI×533, ANF×9, BHVN×83, BZ×76, CAPR×153 | 09:30 open · cash $517.93 (unchanged overnight, no fees) · equity $11,160.61 vs prior close $11,080.16 (+80.45) because holdings re-marked: MOS×57 yday $24.16 → 09:30 $24.00 -9.12; RRC×33 yday $41.55 → 09:30 $41.44 -3.63; CRK×97 yday $14.50 → 09:30 $14.42 -7.76; SLI×533 yday $2.61 → 09:30 $2.60 -5.33; ACMR×17 yday $79.11 → 09:30 $81.65 +43.18; GGB×312 yday $4.46 → 09:30 $4.57 +34.32; MT×18 yday $74.53 → 09:30 $74.54 +0.18; MU×1 yday $938.40 → 09:30 $967.01 +28.61 |
| 2026-08-31 | -5.85 | $100.18 | MOS×57, RRC×33, CRK×97, SLI×533, ANF×9, BHVN×83, BZ×76, CAPR×153 | $10,984.22 | -226.55 | — | MOS, RRC, CRK, SLI, ANF, BHVN, BZ, CAPR | $10,961.62 | $10,961.62 | — | 09:30 open · cash $100.18 (unchanged overnight, no fees) · equity $10,984.22 vs prior close $11,210.77 (-226.55) because holdings re-marked: MOS×57 yday $23.76 → 09:30 $23.75 -0.57; RRC×33 yday $41.64 → 09:30 $41.11 -17.49; CRK×97 yday $14.62 → 09:30 $14.56 -5.82; SLI×533 yday $2.64 → 09:30 $2.51 -69.29; ANF×9 yday $145.75 → 09:30 $148.67 +26.28; BHVN×83 yday $16.12 → 09:30 $15.44 -56.44; BZ×76 yday $18.00 → 09:30 $17.89 -8.36; CAPR×153 yday $10.06 → 09:30 $9.44 -94.86 |
| 2026-09-01 | -6.30 | $10,961.62 | — | $10,961.62 | -0.00 | — | — | $10,961.62 | $10,961.62 | — | 09:30 open · cash $10,961.62 · no holdings · equity $10,961.62 vs prior close $10,961.62 (-0.00). Cash unchanged overnight; no fees. |
| 2026-09-02 | -3.83 | $10,961.62 | — | $10,961.62 | -0.00 | — | — | $10,961.62 | $10,961.62 | — | 09:30 open · cash $10,961.62 · no holdings · equity $10,961.62 vs prior close $10,961.62 (-0.00). Cash unchanged overnight; no fees. |
| 2026-09-03 | -0.90 | $10,961.62 | — | $10,961.62 | -0.00 | ATRC, HRMY, CABA, VSTM, RVTY, GPRO, FRVO, CRK | — | $131.82 | $11,771.27 | ATRC×27, HRMY×33, CABA×419, VSTM×177, RVTY×10, GPRO×1123, FRVO×74, CRK×87 | 09:30 open · cash $10,961.62 · no holdings · equity $10,961.62 vs prior close $10,961.62 (-0.00). Cash unchanged overnight; no fees. |
| 2026-09-04 | — | $131.82 | ATRC×27, HRMY×33, CABA×419, VSTM×177, RVTY×10, GPRO×1123, FRVO×74, CRK×87 | $11,938.12 | +166.85 | ASND, OSCR, NVAX, BVS, BAK | HRMY, VSTM, RVTY, FRVO, CRK | $66.06 | $11,459.19 | ATRC×27, CABA×419, GPRO×1123, ASND×5, OSCR×45, NVAX×134, BVS×96, BAK×715 | 09:30 open · cash $131.82 (unchanged overnight, no fees) · equity $11,938.12 vs prior close $11,771.27 (+166.85) because holdings re-marked: ATRC×27 yday $52.59 → 09:30 $52.88 +7.83; HRMY×33 yday $42.86 → 09:30 $42.93 +2.31; CABA×419 yday $3.57 → 09:30 $3.63 +25.14; VSTM×177 yday $8.02 → 09:30 $8.03 +1.77; RVTY×10 yday $130.94 → 09:30 $132.45 +15.10; GPRO×1123 yday $1.69 → 09:30 $1.78 +101.07; FRVO×74 yday $17.98 → 09:30 $18.27 +21.46; CRK×87 yday $15.54 → 09:30 $15.45 -7.83 |

## Fills (09:30 open snapshot, then buys / sells)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 18 | $59.80 | $2.04 | — | $8,921.56 | — | S≥+5: sizeup + more names; list flatten; ⚪; ret5=-5.3; leftover $1111.11 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 24 | $45.98 | $2.06 | — | $7,815.97 | — | S≥+5: sizeup + more names; list flatten; ⚪; ret5=+12.3; leftover $1111.11 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 21 | $50.62 | $2.05 | — | $6,750.83 | — | S≥+5: sizeup + more names; list flatten; ⚪; ret5=+6.2; leftover $1111.11 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TGTX` | 22 | $49.70 | $2.06 | — | $5,655.38 | — | S≥+5: sizeup + more names; list flatten; ⚪; ret5=-0.8; leftover $1111.11 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 94 | $11.70 | $2.27 | — | $4,553.31 | — | S≥+5: sizeup + more names; list flatten; ⚪; ret5=-0.8; leftover $1111.11 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 37 | $29.74 | $2.10 | — | $3,450.82 | — | S≥+5: sizeup + more names; list flatten; ⚪; ret5=-5.3; leftover $1111.11 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 1371 | $0.81 | $15.22 | — | $2,325.10 | — | S≥+5: sizeup + more names; list flatten; ⚪; ret5=+13.2; leftover $1111.11 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 47 | $23.33 | $2.13 | — | $1,226.46 | — | S≥+5: sizeup + more names; list flatten; ⚪; ret5=+19.7; leftover $1111.11 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `VOR` | 50 | $22.01 | $2.14 | — | $123.82 | — | S≥+5: sizeup + more names; list flatten; ⚪; ret5=+0.3; leftover $1111.11 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $123.82 | ▲ 09:30 equity $10,219.63 vs yday $10,195.74 (+23.89) | 09:30 open · cash $123.82 (unchanged overnight, no fees) · equity $10,219.63 vs prior close $10,195.74 (+23.89) because holdings re-marked: BTSG×18 yday $60.23 → 09:30 $59.65 -10.44; IREN×24 yday $44.76 → 09:30 $44.09 -16.08; TPG×21 yday $54.62 → 09:30 $55.29 +14.07; TGTX×22 yday $47.94 → 09:30 $47.27 -14.74; SLS×94 yday $12.36 → 09:30 $12.40 +3.76; HIMS×37 yday $28.77 → 09:30 $29.15 +14.06; INO×1371 yday $0.90 → 09:30 $0.93 +41.13; TNDM×47 yday $23.13 → 09:30 $22.92 -9.87; VOR×50 yday $23.29 → 09:30 $23.33 +2.00 | — |
| 2026-08-14 09:30 ET | **SELL** | `BTSG` | 18 | $59.65 | $2.06 | $-6.81 | $1,195.45 | ▼ -6.81 after sell → book $10,217.56; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `IREN` | 24 | $44.09 | $2.08 | $-49.50 | $2,251.53 | ▼ -49.50 after sell → book $10,215.48; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TPG` | 21 | $55.29 | $2.07 | $+93.88 | $3,410.55 | ▲ +93.88 after sell → book $10,213.41; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **SELL** | `TGTX` | 22 | $47.27 | $2.08 | $-57.59 | $4,448.41 | ▼ -57.59 after sell → book $10,211.33; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `SLS` | 94 | $12.40 | $2.30 | $+61.23 | $5,611.71 | ▲ +61.23 after sell → book $10,209.03; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `HIMS` | 37 | $29.15 | $2.12 | $-26.05 | $6,688.14 | ▼ -26.05 after sell → book $10,206.91; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `INO` | 1371 | $0.93 | $17.10 | $+132.20 | $7,946.07 | ▲ +132.20 after sell → book $10,189.81; vs 09:30 mark -17.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TNDM` | 47 | $22.92 | $2.15 | $-23.55 | $9,021.16 | ▼ -23.55 after sell → book $10,187.66; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `VOR` | 50 | $23.33 | $2.16 | $+61.70 | $10,185.50 | ▲ +61.70 after sell → book $10,185.50; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **BUY** | `TLN` | 2 | $359.83 | $2.00 | — | $9,463.84 | — | S≥+5: sizeup + more names; list flatten; 🔵; ret5=+5.9; leftover $848.79 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `VST` | 5 | $146.90 | $2.00 | — | $8,727.34 | — | S≥+5: sizeup + more names; list flatten; 🔵; ret5=+3.6; leftover $848.79 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `NRG` | 7 | $120.00 | $2.01 | — | $7,885.33 | — | S≥+5: sizeup + more names; list flatten; 🔵; ret5=+0.6; leftover $848.79 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `DAVE` | 2 | $330.91 | $2.00 | — | $7,221.51 | — | S≥+5: sizeup + more names; list flatten; 🔵; ⚪; ret5=-8.6; leftover $848.79 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `SLG` | 14 | $57.61 | $2.03 | — | $6,412.94 | — | S≥+5: sizeup + more names; list flatten; 🔵; ret5=+5.7; leftover $848.79 | join🟢 sector🟢 gen🟢 news🟡 judge🔴 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `MARA` | 94 | $9.01 | $2.27 | — | $5,563.73 | — | S≥+5: sizeup + more names; list flatten; 🔵; ⚪; ret5=-13.5; leftover $848.79 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 905 | $0.94 | $11.19 | — | $4,704.55 | — | S≥+5: sizeup + more names; list flatten; 🔵; ⚪; ret5=+0.5; leftover $848.79 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 565 | $1.50 | $7.29 | — | $3,849.76 | — | S≥+5: sizeup + more names; list flatten; 🔵; ⚪; ret5=+9.2; leftover $848.79 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BETR` | 57 | $14.80 | $2.16 | — | $3,004.00 | — | S≥+5: sizeup + more names; list flatten; 🔵; ⚪; ret5=-9.9; leftover $848.79 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 196 | $4.31 | $2.58 | — | $2,156.66 | — | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $848.79 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `WWW` | 41 | $20.60 | $2.11 | — | $1,309.95 | — | S≥+5: sizeup + more names; list probable,yday_gainer; ret5=+4.4; leftover $848.79 | join🟢 sector🔴 gen🟢 news🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `HYLN` | 203 | $4.18 | $2.62 | — | $458.79 | — | S≥+5: sizeup + more names; list probable,yday_gainer; 🔵; ⚪; ret5=+4.1; leftover $848.79 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $458.79 | ▲ 09:30 equity $10,201.84 vs yday $10,152.17 (+49.67) | 09:30 open · cash $458.79 (unchanged overnight, no fees) · equity $10,201.84 vs prior close $10,152.17 (+49.67) because holdings re-marked: TLN×2 yday $362.74 → 09:30 $367.88 +10.28; VST×5 yday $148.13 → 09:30 $149.37 +6.20; NRG×7 yday $126.24 → 09:30 $127.40 +8.12; DAVE×2 yday $334.57 → 09:30 $336.94 +4.74; SLG×14 yday $56.09 → 09:30 $55.37 -10.08; MARA×94 yday $9.20 → 09:30 $9.22 +1.88; LDI×905 yday $0.90 → 09:30 $0.91 +9.05; BTBT×565 yday $1.57 → 09:30 $1.52 -28.25; BETR×57 yday $13.73 → 09:30 $13.67 -3.42; ANGX×196 yday $4.37 → 09:30 $4.60 +45.08; WWW×41 yday $21.03 → 09:30 $20.98 -2.05; HYLN×203 yday $4.06 → 09:30 $4.10 +8.12 | — |
| 2026-08-17 09:30 ET | **SELL** | `TLN` | 2 | $367.88 | $2.02 | $+12.09 | $1,192.53 | ▲ +12.09 after sell → book $10,199.83; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `VST` | 5 | $149.37 | $2.02 | $+8.32 | $1,937.36 | ▲ +8.32 after sell → book $10,197.80; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `NRG` | 7 | $127.40 | $2.03 | $+47.76 | $2,827.13 | ▲ +47.76 after sell → book $10,195.77; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `DAVE` | 2 | $336.94 | $2.02 | $+8.05 | $3,498.99 | ▲ +8.05 after sell → book $10,193.76; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `SLG` | 14 | $55.37 | $2.05 | $-35.44 | $4,272.12 | ▼ -35.44 after sell → book $10,191.70; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `MARA` | 94 | $9.22 | $2.30 | $+15.17 | $5,136.50 | ▲ +15.17 after sell → book $10,189.41; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `LDI` | 905 | $0.91 | $11.08 | $-49.43 | $5,946.25 | ▼ -49.43 after sell → book $10,178.32; vs 09:30 mark -11.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BTBT` | 565 | $1.52 | $7.39 | $-3.38 | $6,797.66 | ▼ -3.38 after sell → book $10,170.93; vs 09:30 mark -7.39 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BETR` | 57 | $13.67 | $2.18 | $-68.75 | $7,574.67 | ▼ -68.75 after sell → book $10,168.75; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 196 | $4.60 | $2.62 | $+51.64 | $8,473.65 | ▲ +51.64 after sell → book $10,166.13; vs 09:30 mark -2.62 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `WWW` | 41 | $20.98 | $2.13 | $+11.33 | $9,331.70 | ▲ +11.33 after sell → book $10,164.00; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `HYLN` | 203 | $4.10 | $2.66 | $-21.52 | $10,161.33 | ▼ -21.52 after sell → book $10,161.33; vs 09:30 mark -2.67 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 27 | $46.18 | $2.07 | — | $8,912.40 | — | S≥+5: sizeup + more names; list flatten; 🔵; ret5=+6.7; leftover $1270.17 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 8 | $142.77 | $2.01 | — | $7,768.23 | — | S≥+5: sizeup + more names; list flatten; 🔵; ret5=+5.8; leftover $1270.17 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 6 | $202.70 | $2.01 | — | $6,550.02 | — | S≥+5: sizeup + more names; list flatten; 🔵; ret5=+8.3; leftover $1270.17 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 313 | $4.05 | $4.04 | — | $5,278.33 | — | S≥+5: sizeup + more names; list flatten; 🔵; ⚪; ret5=-12.3; leftover $1270.17 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `TGB` | 150 | $8.46 | $2.44 | — | $4,006.89 | — | S≥+5: sizeup + more names; list flatten; 🔵; ⚪; ret5=+0.4; leftover $1270.17 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ELF` | 14 | $90.54 | $2.03 | — | $2,737.30 | — | S≥+5: sizeup + more names; list flatten; ret5=-7.2; leftover $1270.17 | join🟢 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 392 | $3.24 | $5.06 | — | $1,462.16 | — | S≥+5: sizeup + more names; list flatten; ⚪; ret5=+0.3; leftover $1270.17 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `HNST` | 264 | $4.81 | $3.41 | — | $188.92 | — | S≥+5: sizeup + more names; list flatten; ⚪; ret5=-11.4; leftover $1270.17 | join🟡 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $188.92 | ▼ 09:30 equity $10,129.84 vs yday $10,178.28 (-48.44) | 09:30 open · cash $188.92 (unchanged overnight, no fees) · equity $10,129.84 vs prior close $10,178.28 (-48.44) because holdings re-marked: DVN×27 yday $47.57 → 09:30 $48.00 +11.61; EOG×8 yday $146.15 → 09:30 $148.04 +15.12; FANG×6 yday $206.29 → 09:30 $208.93 +15.84; TMC×313 yday $3.77 → 09:30 $3.72 -15.65; TGB×150 yday $8.77 → 09:30 $8.55 -33.00; ELF×14 yday $93.66 → 09:30 $93.44 -3.08; DNN×392 yday $3.19 → 09:30 $3.11 -31.36; HNST×264 yday $4.70 → 09:30 $4.67 -7.92 | — |
| 2026-08-18 09:30 ET | **SELL** | `DVN` | 27 | $48.00 | $2.09 | $+44.98 | $1,482.83 | ▲ +44.98 after sell → book $10,127.75; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `EOG` | 8 | $148.04 | $2.03 | $+38.11 | $2,665.11 | ▲ +38.11 after sell → book $10,125.71; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `FANG` | 6 | $208.93 | $2.03 | $+33.34 | $3,916.67 | ▲ +33.34 after sell → book $10,123.69; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `TMC` | 313 | $3.72 | $4.10 | $-111.43 | $5,076.93 | ▼ -111.43 after sell → book $10,119.59; vs 09:30 mark -4.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `TGB` | 150 | $8.55 | $2.48 | $+8.58 | $6,356.95 | ▲ +8.58 after sell → book $10,117.11; vs 09:30 mark -2.48 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ELF` | 14 | $93.44 | $2.05 | $+36.52 | $7,663.06 | ▲ +36.52 after sell → book $10,115.06; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `DNN` | 392 | $3.11 | $5.13 | $-61.15 | $8,877.05 | ▼ -61.15 after sell → book $10,109.93; vs 09:30 mark -5.13 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🔴 news🟡 vol🟢 buy🟢 |
| 2026-08-18 09:30 ET | **SELL** | `HNST` | 264 | $4.67 | $3.46 | $-43.83 | $10,106.47 | ▼ -43.83 after sell → book $10,106.47; vs 09:30 mark -3.46 | dropped from list after 1 sess (min 1) | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,106.47 | ▲ 09:30 equity $10,106.47 vs yday $10,106.47 (-0.00) | 09:30 open · cash $10,106.47 · no holdings · equity $10,106.47 vs prior close $10,106.47 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,106.47 | ▲ 09:30 equity $10,106.47 vs yday $10,106.47 (-0.00) | 09:30 open · cash $10,106.47 · no holdings · equity $10,106.47 vs prior close $10,106.47 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 61 | $20.55 | $2.17 | — | $8,850.74 | — | S≥+5: sizeup + more names; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1263.31 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $7,665.58 | — | S≥+5: sizeup + more names; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1263.31 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 61 | $20.65 | $2.17 | — | $6,403.76 | — | S≥+5: sizeup + more names; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1263.31 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 218 | $5.77 | $2.81 | — | $5,143.09 | — | S≥+5: sizeup + more names; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1263.31 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 64 | $19.63 | $2.18 | — | $3,884.59 | — | S≥+5: sizeup + more names; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1263.31 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 42 | $29.63 | $2.12 | — | $2,638.01 | — | S≥+5: sizeup + more names; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1263.31 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 721 | $1.75 | $9.30 | — | $1,366.96 | — | S≥+5: sizeup + more names; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1263.31 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $208.63 | — | S≥+5: sizeup + more names; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1263.31 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $208.63 | ▲ 09:30 equity $10,585.91 vs yday $10,316.19 (+269.72) | 09:30 open · cash $208.63 (unchanged overnight, no fees) · equity $10,585.91 vs prior close $10,316.19 (+269.72) because holdings re-marked: AG×61 yday $21.19 → 09:30 $21.90 +43.31; BHP×13 yday $93.63 → 09:30 $95.72 +27.17; CDE×61 yday $21.11 → 09:30 $21.75 +39.04; HDSN×218 yday $5.57 → 09:30 $5.67 +21.80; IAG×64 yday $20.50 → 09:30 $21.17 +42.88; KGC×42 yday $31.43 → 09:30 $32.17 +31.08; NFGC×721 yday $1.75 → 09:30 $1.79 +28.84; WPM×8 yday $150.25 → 09:30 $154.70 +35.60 | — |
| 2026-08-21 09:30 ET | **SELL** | `AG` | 61 | $21.90 | $2.19 | $+77.98 | $1,542.33 | ▲ +77.98 after sell → book $10,583.71; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 13 | $95.72 | $2.05 | $+57.15 | $2,784.64 | ▲ +57.15 after sell → book $10,581.66; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `CDE` | 61 | $21.75 | $2.19 | $+62.73 | $4,109.20 | ▲ +62.73 after sell → book $10,579.47; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HDSN` | 218 | $5.67 | $2.86 | $-27.47 | $5,342.40 | ▼ -27.47 after sell → book $10,576.61; vs 09:30 mark -2.86 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 64 | $21.17 | $2.20 | $+94.17 | $6,695.08 | ▲ +94.17 after sell → book $10,574.41; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 42 | $32.17 | $2.14 | $+102.43 | $8,044.08 | ▲ +102.43 after sell → book $10,572.27; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `NFGC` | 721 | $1.79 | $9.43 | $+10.11 | $9,325.24 | ▲ +10.11 after sell → book $10,562.84; vs 09:30 mark -9.43 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `WPM` | 8 | $154.70 | $2.03 | $+77.23 | $10,560.81 | ▲ +77.23 after sell → book $10,560.81; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 11 | $119.43 | $2.02 | — | $9,245.05 | — | S≥+5: sizeup + more names; list flatten,mover_buy; 🔵; ⚪; ret5=+20.4; leftover $1320.10 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 76 | $17.20 | $2.22 | — | $7,935.64 | — | S≥+5: sizeup + more names; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $1320.10 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-21 09:30 ET | **BUY** | `AEM` | 6 | $216.30 | $2.01 | — | $6,635.83 | — | S≥+5: sizeup + more names; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+17.6; leftover $1320.10 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 118 | $11.13 | $2.34 | — | $5,320.14 | — | S≥+5: sizeup + more names; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $1320.10 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 534 | $2.47 | $6.89 | — | $3,994.27 | — | S≥+5: sizeup + more names; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $1320.10 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 683 | $1.93 | $8.81 | — | $2,667.27 | — | S≥+5: sizeup + more names; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $1320.10 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRSP` | 22 | $59.72 | $2.06 | — | $1,351.38 | — | S≥+5: sizeup + more names; list flatten,mover_buy; 🔵; ⚪; ret5=+7.6; leftover $1320.10 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 1000 | $1.32 | $12.90 | — | $18.48 | — | S≥+5: sizeup + more names; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $1320.10 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $18.48 | ▲ 09:30 equity $11,133.27 vs yday $10,787.08 (+346.19) | 09:30 open · cash $18.48 (unchanged overnight, no fees) · equity $11,133.27 vs prior close $10,787.08 (+346.19) because holdings re-marked: AU×11 yday $121.22 → 09:30 $120.50 -7.92; AUPH×76 yday $16.65 → 09:30 $16.60 -3.80; AEM×6 yday $216.06 → 09:30 $217.03 +5.82; ARCT×118 yday $13.45 → 09:30 $13.26 -22.42; AUTL×534 yday $2.41 → 09:30 $2.36 -26.70; CRDL×683 yday $1.86 → 09:30 $1.87 +6.83; CRSP×22 yday $59.50 → 09:30 $58.79 -15.62; CYPH×1000 yday $1.42 → 09:30 $1.83 +410.00 | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 11 | $120.50 | $2.04 | $+7.70 | $1,341.93 | ▲ +7.70 after sell → book $11,131.22; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUPH` | 76 | $16.60 | $2.24 | $-50.06 | $2,601.29 | ▼ -50.06 after sell → book $11,128.98; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AEM` | 6 | $217.03 | $2.03 | $+0.34 | $3,901.45 | ▲ +0.34 after sell → book $11,126.96; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 118 | $13.26 | $2.38 | $+246.62 | $5,463.75 | ▲ +246.62 after sell → book $11,124.58; vs 09:30 mark -2.38 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 534 | $2.36 | $6.99 | $-72.62 | $6,717.00 | ▼ -72.62 after sell → book $11,117.59; vs 09:30 mark -6.99 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRDL` | 683 | $1.87 | $8.93 | $-58.72 | $7,985.28 | ▼ -58.72 after sell → book $11,108.66; vs 09:30 mark -8.93 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRSP` | 22 | $58.79 | $2.08 | $-24.59 | $9,276.58 | ▼ -24.59 after sell → book $11,106.58; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 1000 | $1.83 | $13.08 | $+484.02 | $11,093.50 | ▲ +484.02 after sell → book $11,093.50; vs 09:30 mark -13.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,093.50 | ▲ 09:30 equity $11,093.50 vs yday $11,093.50 (+0.00) | 09:30 open · cash $11,093.50 · no holdings · equity $11,093.50 vs prior close $11,093.50 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `MOS` | 57 | $24.00 | $2.16 | — | $9,723.34 | — | S≥+5: sizeup + more names; list flatten; ⚪; ret5=+13.0; leftover $1386.69 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟡 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 126 | $10.92 | $2.37 | — | $8,345.05 | — | S≥+5: sizeup + more names; list flatten; 🔵; ret5=+10.4; leftover $1386.69 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `INSP` | 22 | $61.47 | $2.06 | — | $6,990.66 | — | S≥+5: sizeup + more names; list flatten; 🔵; ret5=+9.2; leftover $1386.69 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 167 | $8.28 | $2.49 | — | $5,605.40 | — | S≥+5: sizeup + more names; list flatten; 🔵; ⚪; ret5=+8.8; leftover $1386.69 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 265 | $5.23 | $3.42 | — | $4,216.04 | — | S≥+5: sizeup + more names; list flatten; ret5=+10.7; leftover $1386.69 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `HCA` | 3 | $429.24 | $2.00 | — | $2,926.32 | — | S≥+5: sizeup + more names; list flatten; ret5=+6.1; leftover $1386.69 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 855 | $1.62 | $11.03 | — | $1,530.19 | — | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.8; leftover $1386.69 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `NPWR` | 693 | $2.00 | $8.94 | — | $135.25 | — | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover; 🔵; ret5=-12.8; leftover $1386.69 | join🟡 sector🟢 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $135.25 | ▲ 09:30 equity $11,063.78 vs yday $11,063.78 (-0.00) | 09:30 open · cash $135.25 (unchanged overnight, no fees) · equity $11,063.78 vs prior close $11,063.78 (-0.00) because holdings re-marked: MOS×57 yday $23.75 → 09:30 $23.75 +0.00; OCUL×126 yday $10.92 → 09:30 $10.92 +0.00; INSP×22 yday $61.47 → 09:30 $61.47 +0.00; CRMD×167 yday $8.28 → 09:30 $8.28 +0.00; RZLT×265 yday $5.29 → 09:30 $5.29 +0.00; HCA×3 yday $428.50 → 09:30 $428.50 +0.00; BMEA×855 yday $1.61 → 09:30 $1.61 +0.00; NPWR×693 yday $2.02 → 09:30 $2.02 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $135.25 | ▲ 09:30 equity $11,112.30 vs yday $11,059.04 (+53.26) | 09:30 open · cash $135.25 (unchanged overnight, no fees) · equity $11,112.30 vs prior close $11,059.04 (+53.26) because holdings re-marked: MOS×57 yday $23.75 → 09:30 $24.84 +62.13; OCUL×126 yday $10.92 → 09:30 $10.79 -16.38; INSP×22 yday $61.47 → 09:30 $60.07 -30.80; CRMD×167 yday $8.28 → 09:30 $8.60 +53.44; RZLT×265 yday $5.29 → 09:30 $5.01 -74.20; HCA×3 yday $428.50 → 09:30 $427.50 -3.00; BMEA×855 yday $1.61 → 09:30 $1.75 +119.70; NPWR×693 yday $2.02 → 09:30 $1.93 -62.37 | — |
| 2026-08-27 09:30 ET | **SELL** | `OCUL` | 126 | $10.79 | $2.40 | $-21.15 | $1,492.39 | ▼ -21.15 after sell → book $11,109.90; vs 09:30 mark -2.40 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `INSP` | 22 | $60.07 | $2.08 | $-34.93 | $2,811.85 | ▼ -34.93 after sell → book $11,107.82; vs 09:30 mark -2.08 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `CRMD` | 167 | $8.60 | $2.53 | $+48.42 | $4,245.52 | ▲ +48.42 after sell → book $11,105.29; vs 09:30 mark -2.53 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `RZLT` | 265 | $5.01 | $3.47 | $-65.19 | $5,569.70 | ▼ -65.19 after sell → book $11,101.82; vs 09:30 mark -3.47 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `HCA` | 3 | $427.50 | $2.02 | $-9.24 | $6,850.18 | ▼ -9.24 after sell → book $11,099.80; vs 09:30 mark -2.02 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BMEA` | 855 | $1.75 | $11.18 | $+88.94 | $8,335.25 | ▲ +88.94 after sell → book $11,088.62; vs 09:30 mark -11.18 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `NPWR` | 693 | $1.93 | $9.07 | $-66.52 | $9,663.67 | ▼ -66.52 after sell → book $11,079.55; vs 09:30 mark -9.07 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 33 | $40.72 | $2.09 | — | $8,317.82 | — | S≥+5: sizeup + more names; list flatten; ret5=+1.8; leftover $1380.52 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 97 | $14.09 | $2.28 | — | $6,948.81 | — | S≥+5: sizeup + more names; list flatten; ret5=+1.1; leftover $1380.52 | join🟢 sector🔴 gen🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 533 | $2.59 | $6.88 | — | $5,561.46 | — | S≥+5: sizeup + more names; list flatten; ret5=+4.2; leftover $1380.52 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `ACMR` | 17 | $80.97 | $2.04 | — | $4,182.93 | — | S≥+5: sizeup + more names; list mover_buy; 🔵; ret5=-1.3; leftover $1380.52 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `GGB` | 312 | $4.42 | $4.02 | — | $2,799.87 | — | S≥+5: sizeup + more names; list mover_buy; 🔵; ret5=-8.6; leftover $1380.52 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MT` | 18 | $75.12 | $2.04 | — | $1,445.66 | — | S≥+5: sizeup + more names; list mover_buy; 🔵; ret5=-2.2; leftover $1380.52 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MU` | 1 | $925.74 | $1.99 | — | $517.93 | — | S≥+5: sizeup + more names; list mover_buy; 🔵; ret5=-0.5; leftover $1380.52 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $517.93 | ▲ 09:30 equity $11,160.61 vs yday $11,080.16 (+80.45) | 09:30 open · cash $517.93 (unchanged overnight, no fees) · equity $11,160.61 vs prior close $11,080.16 (+80.45) because holdings re-marked: MOS×57 yday $24.16 → 09:30 $24.00 -9.12; RRC×33 yday $41.55 → 09:30 $41.44 -3.63; CRK×97 yday $14.50 → 09:30 $14.42 -7.76; SLI×533 yday $2.61 → 09:30 $2.60 -5.33; ACMR×17 yday $79.11 → 09:30 $81.65 +43.18; GGB×312 yday $4.46 → 09:30 $4.57 +34.32; MT×18 yday $74.53 → 09:30 $74.54 +0.18; MU×1 yday $938.40 → 09:30 $967.01 +28.61 | — |
| 2026-08-28 09:30 ET | **SELL** | `ACMR` | 17 | $81.65 | $2.06 | $+7.46 | $1,903.92 | ▲ +7.46 after sell → book $11,158.55; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `GGB` | 312 | $4.57 | $4.09 | $+38.69 | $3,325.67 | ▲ +38.69 after sell → book $11,154.46; vs 09:30 mark -4.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MT` | 18 | $74.54 | $2.06 | $-14.55 | $4,665.33 | ▼ -14.55 after sell → book $11,152.40; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MU` | 1 | $967.01 | $2.01 | $+37.26 | $5,630.32 | ▲ +37.26 after sell → book $11,150.38; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 9 | $144.70 | $2.02 | — | $4,326.01 | — | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover; 🔵; ret5=+0.8; leftover $1407.58 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BHVN` | 83 | $16.95 | $2.24 | — | $2,916.92 | — | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover; ret5=-5.4; leftover $1407.58 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BZ` | 76 | $18.50 | $2.22 | — | $1,508.70 | — | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover; ret5=+2.8; leftover $1407.58 | join🟢 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `CAPR` | 153 | $9.19 | $2.45 | — | $100.18 | — | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover; ret5=-5.4; leftover $1407.58 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $100.18 | ▼ 09:30 equity $10,984.22 vs yday $11,210.77 (-226.55) | 09:30 open · cash $100.18 (unchanged overnight, no fees) · equity $10,984.22 vs prior close $11,210.77 (-226.55) because holdings re-marked: MOS×57 yday $23.76 → 09:30 $23.75 -0.57; RRC×33 yday $41.64 → 09:30 $41.11 -17.49; CRK×97 yday $14.62 → 09:30 $14.56 -5.82; SLI×533 yday $2.64 → 09:30 $2.51 -69.29; ANF×9 yday $145.75 → 09:30 $148.67 +26.28; BHVN×83 yday $16.12 → 09:30 $15.44 -56.44; BZ×76 yday $18.00 → 09:30 $17.89 -8.36; CAPR×153 yday $10.06 → 09:30 $9.44 -94.86 | — |
| 2026-08-31 09:30 ET | **SELL** | `MOS` | 57 | $23.75 | $2.18 | $-18.59 | $1,451.75 | ▼ -18.59 after sell → book $10,982.04; vs 09:30 mark -2.18 | dropped from list after 4 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `RRC` | 33 | $41.11 | $2.11 | $+8.67 | $2,806.27 | ▲ +8.67 after sell → book $10,979.93; vs 09:30 mark -2.11 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CRK` | 97 | $14.56 | $2.31 | $+41.00 | $4,216.28 | ▲ +41.00 after sell → book $10,977.62; vs 09:30 mark -2.31 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SLI` | 533 | $2.51 | $6.97 | $-56.49 | $5,547.14 | ▼ -56.49 after sell → book $10,970.65; vs 09:30 mark -6.97 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ANF` | 9 | $148.67 | $2.04 | $+31.68 | $6,883.13 | ▲ +31.68 after sell → book $10,968.61; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BHVN` | 83 | $15.44 | $2.26 | $-129.83 | $8,162.38 | ▼ -129.83 after sell → book $10,966.34; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BZ` | 76 | $17.89 | $2.24 | $-50.82 | $9,519.78 | ▼ -50.82 after sell → book $10,964.10; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CAPR` | 153 | $9.44 | $2.49 | $+33.31 | $10,961.62 | ▲ +33.31 after sell → book $10,961.62; vs 09:30 mark -2.48 | dropped from list after 1 sess (min 1) | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,961.62 | ▲ 09:30 equity $10,961.62 vs yday $10,961.62 (-0.00) | 09:30 open · cash $10,961.62 · no holdings · equity $10,961.62 vs prior close $10,961.62 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,961.62 | ▲ 09:30 equity $10,961.62 vs yday $10,961.62 (-0.00) | 09:30 open · cash $10,961.62 · no holdings · equity $10,961.62 vs prior close $10,961.62 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,961.62 | ▲ 09:30 equity $10,961.62 vs yday $10,961.62 (-0.00) | 09:30 open · cash $10,961.62 · no holdings · equity $10,961.62 vs prior close $10,961.62 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 27 | $49.76 | $2.07 | — | $9,616.03 | — | S≥+5: sizeup + more names; list flatten; 🔵; ⚪; ret5=+10.6; leftover $1370.20 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 33 | $41.31 | $2.09 | — | $8,250.71 | — | S≥+5: sizeup + more names; list flatten,mover_buy; 🔵; ⚪; ret5=+0.1; leftover $1370.20 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 419 | $3.27 | $5.41 | — | $6,875.17 | — | S≥+5: sizeup + more names; list flatten; 🔵; ⚪; ret5=+13.8; leftover $1370.20 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 177 | $7.70 | $2.52 | — | $5,509.75 | — | S≥+5: sizeup + more names; list flatten,mover_buy; 🔵; ⚪; ret5=+4.7; leftover $1370.20 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 10 | $125.94 | $2.02 | — | $4,248.33 | — | S≥+5: sizeup + more names; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $1370.20 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `GPRO` | 1123 | $1.22 | $14.49 | — | $2,863.78 | — | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover; ret5=+5.9; leftover $1370.20 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FRVO` | 74 | $18.40 | $2.21 | — | $1,499.97 | — | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover; ret5=-14.4; leftover $1370.20 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 87 | $15.70 | $2.25 | — | $131.82 | — | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+1.1; leftover $1370.20 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $131.82 | ▲ 09:30 equity $11,938.12 vs yday $11,771.27 (+166.85) | 09:30 open · cash $131.82 (unchanged overnight, no fees) · equity $11,938.12 vs prior close $11,771.27 (+166.85) because holdings re-marked: ATRC×27 yday $52.59 → 09:30 $52.88 +7.83; HRMY×33 yday $42.86 → 09:30 $42.93 +2.31; CABA×419 yday $3.57 → 09:30 $3.63 +25.14; VSTM×177 yday $8.02 → 09:30 $8.03 +1.77; RVTY×10 yday $130.94 → 09:30 $132.45 +15.10; GPRO×1123 yday $1.69 → 09:30 $1.78 +101.07; FRVO×74 yday $17.98 → 09:30 $18.27 +21.46; CRK×87 yday $15.54 → 09:30 $15.45 -7.83 | — |
| 2026-09-04 09:30 ET | **SELL** | `HRMY` | 33 | $42.93 | $2.11 | $+49.26 | $1,546.40 | ▲ +49.26 after sell → book $11,936.01; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `VSTM` | 177 | $8.03 | $2.56 | $+53.33 | $2,965.15 | ▲ +53.33 after sell → book $11,933.45; vs 09:30 mark -2.56 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `RVTY` | 10 | $132.45 | $2.04 | $+61.04 | $4,287.61 | ▲ +61.04 after sell → book $11,931.41; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `FRVO` | 74 | $18.27 | $2.24 | $-14.07 | $5,637.35 | ▼ -14.07 after sell → book $11,929.17; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CRK` | 87 | $15.45 | $2.28 | $-26.28 | $6,979.23 | ▼ -26.28 after sell → book $11,926.90; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `ASND` | 5 | $266.94 | $2.00 | — | $5,642.52 | — | S≥+5: sizeup + more names; list flatten; ret5=+1.9; leftover $1395.85 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OSCR` | 45 | $30.65 | $2.12 | — | $4,261.15 | — | S≥+5: sizeup + more names; list flatten; 🔵; ret5=-2.2; leftover $1395.85 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `NVAX` | 134 | $10.41 | $2.39 | — | $2,863.82 | — | S≥+5: sizeup + more names; list flatten,ohlc_hot; ⚪; ret5=+11.1; leftover $1395.85 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BVS` | 96 | $14.50 | $2.28 | — | $1,469.54 | — | S≥+5: sizeup + more names; list flatten; 🔵; ⚪; ret5=+0.8; leftover $1395.85 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 715 | $1.95 | $9.22 | — | $66.06 | — | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $1395.85 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `OXY` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `APA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `COP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `MUR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `MLYS` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `TRMD` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OBE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CYPH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `STE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DHR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SYK` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MUR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `TRMD` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MLYS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `TBPH` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `RZLT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `MOS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `OCUL` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INSP` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CRMD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HCA` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CRML` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SAFX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-26 | `BMEA` | no_price | no 09:30 open — carry |
| 2026-08-26 | `NPWR` | no_price | no 09:30 open — carry |
| 2026-08-26 | `KURA` | no_price | no 09:30 open |
| 2026-08-26 | `AVBP` | no_price | no 09:30 open |
| 2026-08-31 | `RES` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PBF` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `WTTR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `OKTA` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRWD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `VEEV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `DK` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `BTE` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `MTDR` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `RES` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `KOS` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `OIS` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `FTI` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `KMI` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `PCRX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `HRMY` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PBH` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `VSTM` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `MGTX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PBR-A` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PBR` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `GPRO` | hard_red | hard-red S=-3.83 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `ATRC` | 27 | 2026-09-03 @ $49.76 | S≥+5: sizeup + more names; list flatten; 🔵; ⚪; ret5=+10.6; leftover $1370.20 |
| `CABA` | 419 | 2026-09-03 @ $3.27 | S≥+5: sizeup + more names; list flatten; 🔵; ⚪; ret5=+13.8; leftover $1370.20 |
| `GPRO` | 1123 | 2026-09-03 @ $1.22 | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover; ret5=+5.9; leftover $1370.20 |
| `ASND` | 5 | 2026-09-04 @ $266.94 | S≥+5: sizeup + more names; list flatten; ret5=+1.9; leftover $1395.85 |
| `OSCR` | 45 | 2026-09-04 @ $30.65 | S≥+5: sizeup + more names; list flatten; 🔵; ret5=-2.2; leftover $1395.85 |
| `NVAX` | 134 | 2026-09-04 @ $10.41 | S≥+5: sizeup + more names; list flatten,ohlc_hot; ⚪; ret5=+11.1; leftover $1395.85 |
| `BVS` | 96 | 2026-09-04 @ $14.50 | S≥+5: sizeup + more names; list flatten; 🔵; ⚪; ret5=+0.8; leftover $1395.85 |
| `BAK` | 715 | 2026-09-04 @ $1.95 | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $1395.85 |
