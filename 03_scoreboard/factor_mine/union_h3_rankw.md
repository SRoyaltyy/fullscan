# Factor mine action — `union_h3_rankw`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `rank_w` · sell `list` · S-boost `none` · rank-weighted leftover

Cash book **+5.94%** ($10,594) · signal-only (no cash/fees) was +34.19%. Starts YES **15/17**. Fills 94 · skips 159 · realized $+203.76.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `list order` · **top_n** 8.
- **Size** `rank_w` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $18.69.

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | vs yday close | Bought | Sold | Close cash | Close equity | Close held | 09:30 why |
|---|---:|---:|---|---:|---:|---|---|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | BTSG, IREN, TPG, TGTX, SLS, HIMS, INO, TNDM | — | $128.05 | $10,117.03 | BTSG×37, IREN×42, TPG×32, TGTX×27, SLS×94, HIMS×28, INO×685, TNDM×11 | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-14 | +5.50 | $128.05 | BTSG×37, IREN×42, TPG×32, TGTX×27, SLS×94, HIMS×28, INO×685, TNDM×11 | $10,103.42 | -13.61 | MARA, LDI, BTBT | — | $109.27 | $10,260.71 | BTSG×37, IREN×42, TPG×32, TGTX×27, SLS×94, HIMS×28, INO×685, TNDM×11, MARA×1, LDI×7, BTBT×2 | 09:30 open · cash $128.05 (unchanged overnight, no fees) · equity $10,103.42 vs prior close $10,117.03 (-13.61) because holdings re-marked: BTSG×37 yday $60.23 → 09:30 $59.65 -21.46; IREN×42 yday $44.76 → 09:30 $44.09 -28.14; TPG×32 yday $54.62 → 09:30 $55.29 +21.44; TGTX×27 yday $47.94 → 09:30 $47.27 -18.09; SLS×94 yday $12.36 → 09:30 $12.40 +3.76; HIMS×28 yday $28.77 → 09:30 $29.15 +10.64; INO×685 yday $0.90 → 09:30 $0.93 +20.55; TNDM×11 yday $23.13 → 09:30 $22.92 -2.31 |
| 2026-08-17 | +2.25 | $109.27 | BTSG×37, IREN×42, TPG×32, TGTX×27, SLS×94, HIMS×28, INO×685, TNDM×11, MARA×1, LDI×7, BTBT×2 | $10,281.18 | +20.47 | TMC, TGB, DNN | — | $85.16 | $10,290.17 | BTSG×37, IREN×42, TPG×32, TGTX×27, SLS×94, HIMS×28, INO×685, TNDM×11, MARA×1, LDI×7, BTBT×2, TMC×3, TGB×1, DNN×1 | 09:30 open · cash $109.27 (unchanged overnight, no fees) · equity $10,281.18 vs prior close $10,260.71 (+20.47) because holdings re-marked: BTSG×37 yday $61.71 → 09:30 $61.69 -0.74; IREN×42 yday $44.06 → 09:30 $45.23 +49.14; TPG×32 yday $53.03 → 09:30 $52.67 -11.52; TGTX×27 yday $48.74 → 09:30 $48.74 +0.00; SLS×94 yday $12.78 → 09:30 $12.78 +0.00; HIMS×28 yday $28.15 → 09:30 $28.14 -0.28; INO×685 yday $1.09 → 09:30 $1.07 -13.70; TNDM×11 yday $22.72 → 09:30 $22.50 -2.42; MARA×1 yday $9.20 → 09:30 $9.22 +0.02; LDI×7 yday $0.90 → 09:30 $0.91 +0.07; BTBT×2 yday $1.57 → 09:30 $1.52 -0.10 |
| 2026-08-18 | -6.20 | $85.16 | BTSG×37, IREN×42, TPG×32, TGTX×27, SLS×94, HIMS×28, INO×685, TNDM×11, MARA×1, LDI×7, BTBT×2, TMC×3, TGB×1, DNN×1 | $10,157.73 | -132.44 | — | BTSG, IREN, TPG, TGTX, SLS, HIMS, INO, TNDM | $10,092.52 | $10,133.65 | MARA×1, LDI×7, BTBT×2, TMC×3, TGB×1, DNN×1 | 09:30 open · cash $85.16 (unchanged overnight, no fees) · equity $10,157.73 vs prior close $10,290.17 (-132.44) because holdings re-marked: BTSG×37 yday $60.38 → 09:30 $60.00 -14.06; IREN×42 yday $44.90 → 09:30 $43.56 -56.28; TPG×32 yday $51.77 → 09:30 $51.77 +0.00; TGTX×27 yday $49.28 → 09:30 $49.28 +0.00; SLS×94 yday $13.00 → 09:30 $12.66 -31.96; HIMS×28 yday $28.61 → 09:30 $27.85 -21.28; INO×685 yday $1.15 → 09:30 $1.14 -6.85; TNDM×11 yday $22.25 → 09:30 $22.16 -1.04; MARA×1 yday $9.72 → 09:30 $9.36 -0.36; LDI×7 yday $0.88 → 09:30 $0.87 -0.04; BTBT×2 yday $1.60 → 09:30 $1.54 -0.12; TMC×3 yday $3.77 → 09:30 $3.72 -0.15; TGB×1 yday $8.77 → 09:30 $8.55 -0.22; DNN×1 yday $3.19 → 09:30 $3.11 -0.08 |
| 2026-08-19 | -7.20 | $10,092.52 | MARA×1, LDI×7, BTBT×2, TMC×3, TGB×1, DNN×1 | $10,134.11 | +0.46 | — | MARA, LDI, BTBT | $10,110.16 | $10,133.76 | TMC×3, TGB×1, DNN×1 | 09:30 open · cash $10,092.52 (unchanged overnight, no fees) · equity $10,134.11 vs prior close $10,133.65 (+0.46) because holdings re-marked: MARA×1 yday $8.96 → 09:30 $8.91 -0.05; LDI×7 yday $0.86 → 09:30 $0.88 +0.15; BTBT×2 yday $1.45 → 09:30 $1.42 -0.06; TMC×3 yday $3.92 → 09:30 $3.93 +0.03; TGB×1 yday $8.36 → 09:30 $8.70 +0.34; DNN×1 yday $3.15 → 09:30 $3.19 +0.04 |
| 2026-08-20 | +1.12 | $10,110.16 | TMC×3, TGB×1, DNN×1 | $10,133.47 | -0.29 | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | TMC, TGB, DNN | $232.84 | $10,332.74 | AG×109, BHP×21, CDE×81, HDSN×243, IAG×57, KGC×28, NFGC×321, WPM×1 | 09:30 open · cash $10,110.16 (unchanged overnight, no fees) · equity $10,133.47 vs prior close $10,133.76 (-0.29) because holdings re-marked: TMC×3 yday $3.97 → 09:30 $3.92 -0.15; TGB×1 yday $8.47 → 09:30 $8.35 -0.12; DNN×1 yday $3.22 → 09:30 $3.20 -0.02 |
| 2026-08-21 | +3.25 | $232.84 | AG×109, BHP×21, CDE×81, HDSN×243, IAG×57, KGC×28, NFGC×321, WPM×1 | $10,606.36 | +273.62 | AUPH, ARCT, AUTL, CRDL, CYPH | — | $125.76 | $10,508.12 | AG×109, BHP×21, CDE×81, HDSN×243, IAG×57, KGC×28, NFGC×321, WPM×1, AUPH×2, ARCT×2, AUTL×10, CRDL×10, CYPH×4 | 09:30 open · cash $232.84 (unchanged overnight, no fees) · equity $10,606.36 vs prior close $10,332.74 (+273.62) because holdings re-marked: AG×109 yday $21.19 → 09:30 $21.90 +77.39; BHP×21 yday $93.63 → 09:30 $95.72 +43.89; CDE×81 yday $21.11 → 09:30 $21.75 +51.84; HDSN×243 yday $5.57 → 09:30 $5.67 +24.30; IAG×57 yday $20.50 → 09:30 $21.17 +38.19; KGC×28 yday $31.43 → 09:30 $32.17 +20.72; NFGC×321 yday $1.75 → 09:30 $1.79 +12.84; WPM×1 yday $150.25 → 09:30 $154.70 +4.45 |
| 2026-08-24 | -5.17 | $125.76 | AG×109, BHP×21, CDE×81, HDSN×243, IAG×57, KGC×28, NFGC×321, WPM×1, AUPH×2, ARCT×2, AUTL×10, CRDL×10, CYPH×4 | $10,632.18 | +124.06 | — | — | $125.76 | $10,414.61 | AG×109, BHP×21, CDE×81, HDSN×243, IAG×57, KGC×28, NFGC×321, WPM×1, AUPH×2, ARCT×2, AUTL×10, CRDL×10, CYPH×4 | 09:30 open · cash $125.76 (unchanged overnight, no fees) · equity $10,632.18 vs prior close $10,508.12 (+124.06) because holdings re-marked: AG×109 yday $21.09 → 09:30 $21.47 +41.42; BHP×21 yday $97.03 → 09:30 $97.34 +6.51; CDE×81 yday $20.97 → 09:30 $21.26 +23.49; HDSN×243 yday $5.63 → 09:30 $5.69 +14.58; IAG×57 yday $21.14 → 09:30 $21.44 +17.10; KGC×28 yday $32.76 → 09:30 $33.21 +12.60; NFGC×321 yday $1.84 → 09:30 $1.86 +6.42; WPM×1 yday $157.78 → 09:30 $158.96 +1.18; AUPH×2 yday $16.65 → 09:30 $16.60 -0.10; ARCT×2 yday $13.45 → 09:30 $13.26 -0.38; AUTL×10 yday $2.41 → 09:30 $2.36 -0.50; CRDL×10 yday $1.86 → 09:30 $1.87 +0.10; CYPH×4 yday $1.42 → 09:30 $1.83 +1.64 |
| 2026-08-25 | +1.80 | $125.76 | AG×109, BHP×21, CDE×81, HDSN×243, IAG×57, KGC×28, NFGC×321, WPM×1, AUPH×2, ARCT×2, AUTL×10, CRDL×10, CYPH×4 | $10,467.32 | +52.71 | MOS, OCUL, INSP, CRMD, RZLT, HCA, BMEA, NPWR | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | $8.82 | $10,413.06 | AUPH×2, ARCT×2, AUTL×10, CRDL×10, CYPH×4, MOS×95, OCUL×184, INSP×28, CRMD×173, RZLT×219, HCA×2, BMEA×354, NPWR×143 | 09:30 open · cash $125.76 (unchanged overnight, no fees) · equity $10,467.32 vs prior close $10,414.61 (+52.71) because holdings re-marked: AG×109 yday $20.57 → 09:30 $20.73 +17.44; BHP×21 yday $96.66 → 09:30 $95.95 -14.91; CDE×81 yday $20.49 → 09:30 $20.85 +29.16; HDSN×243 yday $5.57 → 09:30 $5.53 -9.72; IAG×57 yday $21.36 → 09:30 $21.63 +15.39; KGC×28 yday $32.47 → 09:30 $32.76 +8.12; NFGC×321 yday $1.90 → 09:30 $1.91 +3.21; WPM×1 yday $158.00 → 09:30 $160.00 +2.00; AUPH×2 yday $16.60 → 09:30 $16.71 +0.22; ARCT×2 yday $13.76 → 09:30 $14.34 +1.16; AUTL×10 yday $2.38 → 09:30 $2.32 -0.60; CRDL×10 yday $1.80 → 09:30 $1.90 +1.00; CYPH×4 yday $1.64 → 09:30 $1.70 +0.24 |
| 2026-08-26 | +2.02 | $8.82 | AUPH×2, ARCT×2, AUTL×10, CRDL×10, CYPH×4, MOS×95, OCUL×184, INSP×28, CRMD×173, RZLT×219, HCA×2, BMEA×354, NPWR×143 | $10,413.06 | -0.00 | — | — | $8.82 | $10,426.13 | AUPH×2, ARCT×2, AUTL×10, CRDL×10, CYPH×4, MOS×95, OCUL×184, INSP×28, CRMD×173, RZLT×219, HCA×2, BMEA×354, NPWR×143 | 09:30 open · cash $8.82 (unchanged overnight, no fees) · equity $10,413.06 vs prior close $10,413.06 (-0.00) because holdings re-marked: AUPH×2 yday $16.71 → 09:30 $16.71 +0.00; ARCT×2 yday $14.21 → 09:30 $14.21 +0.00; AUTL×10 yday $2.34 → 09:30 $2.34 +0.00; CRDL×10 yday $1.90 → 09:30 $1.90 +0.00; CYPH×4 yday $1.64 → 09:30 $1.64 +0.00; MOS×95 yday $23.75 → 09:30 $23.75 +0.00; OCUL×184 yday $10.92 → 09:30 $10.92 +0.00; INSP×28 yday $61.47 → 09:30 $61.47 +0.00; CRMD×173 yday $8.28 → 09:30 $8.28 +0.00; RZLT×219 yday $5.29 → 09:30 $5.29 +0.00; HCA×2 yday $428.50 → 09:30 $428.50 +0.00; BMEA×354 yday $1.61 → 09:30 $1.61 +0.00; NPWR×143 yday $2.02 → 09:30 $2.02 +0.00 |
| 2026-08-27 | — | $8.82 | AUPH×2, ARCT×2, AUTL×10, CRDL×10, CYPH×4, MOS×95, OCUL×184, INSP×28, CRMD×173, RZLT×219, HCA×2, BMEA×354, NPWR×143 | $10,486.12 | +59.99 | CRK, SLI, GGB | AUPH, ARCT, AUTL, CRDL, CYPH | $78.07 | $10,403.37 | MOS×95, OCUL×184, INSP×28, CRMD×173, RZLT×219, HCA×2, BMEA×354, NPWR×143, CRK×1, SLI×8, GGB×2 | 09:30 open · cash $8.82 (unchanged overnight, no fees) · equity $10,486.12 vs prior close $10,426.13 (+59.99) because holdings re-marked: AUPH×2 yday $16.71 → 09:30 $16.60 -0.22; ARCT×2 yday $14.21 → 09:30 $15.35 +2.28; AUTL×10 yday $2.34 → 09:30 $2.41 +0.70; CRDL×10 yday $1.90 → 09:30 $2.03 +1.30; CYPH×4 yday $1.64 → 09:30 $1.60 -0.16; MOS×95 yday $23.75 → 09:30 $24.84 +103.55; OCUL×184 yday $10.92 → 09:30 $10.79 -23.92; INSP×28 yday $61.47 → 09:30 $60.07 -39.20; CRMD×173 yday $8.28 → 09:30 $8.60 +55.36; RZLT×219 yday $5.29 → 09:30 $5.01 -61.32; HCA×2 yday $428.50 → 09:30 $427.50 -2.00; BMEA×354 yday $1.61 → 09:30 $1.75 +49.56; NPWR×143 yday $2.02 → 09:30 $1.93 -12.87 |
| 2026-08-28 | +0.75 | $78.07 | MOS×95, OCUL×184, INSP×28, CRMD×173, RZLT×219, HCA×2, BMEA×354, NPWR×143, CRK×1, SLI×8, GGB×2 | $10,403.12 | -0.25 | RRC, ANF, BHVN, BZ, CAPR | OCUL, INSP, CRMD, RZLT, HCA, BMEA, NPWR | $154.50 | $10,321.18 | MOS×95, CRK×1, SLI×8, GGB×2, RRC×64, ANF×14, BHVN×95, BZ×58, CAPR×58 | 09:30 open · cash $78.07 (unchanged overnight, no fees) · equity $10,403.12 vs prior close $10,403.37 (-0.25) because holdings re-marked: MOS×95 yday $24.16 → 09:30 $24.00 -15.20; OCUL×184 yday $10.77 → 09:30 $10.63 -25.76; INSP×28 yday $61.80 → 09:30 $62.10 +8.40; CRMD×173 yday $8.39 → 09:30 $8.49 +17.30; RZLT×219 yday $5.04 → 09:30 $5.07 +6.57; HCA×2 yday $427.16 → 09:30 $424.61 -5.10; BMEA×354 yday $1.71 → 09:30 $1.74 +10.62; NPWR×143 yday $1.81 → 09:30 $1.83 +2.86; CRK×1 yday $14.50 → 09:30 $14.42 -0.08; SLI×8 yday $2.61 → 09:30 $2.60 -0.08; GGB×2 yday $4.46 → 09:30 $4.57 +0.22 |
| 2026-08-31 | -5.85 | $154.50 | MOS×95, CRK×1, SLI×8, GGB×2, RRC×64, ANF×14, BHVN×95, BZ×58, CAPR×58 | $10,218.85 | -102.33 | — | MOS | $2,408.44 | $10,260.05 | CRK×1, SLI×8, GGB×2, RRC×64, ANF×14, BHVN×95, BZ×58, CAPR×58 | 09:30 open · cash $154.50 (unchanged overnight, no fees) · equity $10,218.85 vs prior close $10,321.18 (-102.33) because holdings re-marked: MOS×95 yday $23.76 → 09:30 $23.75 -0.95; CRK×1 yday $14.62 → 09:30 $14.56 -0.06; SLI×8 yday $2.64 → 09:30 $2.51 -1.04; GGB×2 yday $4.70 → 09:30 $4.55 -0.30; RRC×64 yday $41.64 → 09:30 $41.11 -33.92; ANF×14 yday $145.75 → 09:30 $148.67 +40.88; BHVN×95 yday $16.12 → 09:30 $15.44 -64.60; BZ×58 yday $18.00 → 09:30 $17.89 -6.38; CAPR×58 yday $10.06 → 09:30 $9.44 -35.96 |
| 2026-09-01 | -6.30 | $2,408.44 | CRK×1, SLI×8, GGB×2, RRC×64, ANF×14, BHVN×95, BZ×58, CAPR×58 | $10,172.78 | -87.27 | — | CRK, SLI, GGB | $2,453.03 | $10,154.14 | RRC×64, ANF×14, BHVN×95, BZ×58, CAPR×58 | 09:30 open · cash $2,408.44 (unchanged overnight, no fees) · equity $10,172.78 vs prior close $10,260.05 (-87.27) because holdings re-marked: CRK×1 yday $14.51 → 09:30 $14.31 -0.20; SLI×8 yday $2.51 → 09:30 $2.70 +1.52; GGB×2 yday $4.55 → 09:30 $4.61 +0.12; RRC×64 yday $41.78 → 09:30 $41.32 -29.44; ANF×14 yday $149.28 → 09:30 $142.47 -95.34; BHVN×95 yday $15.40 → 09:30 $15.45 +4.75; BZ×58 yday $17.90 → 09:30 $17.37 -30.74; CAPR×58 yday $9.36 → 09:30 $10.43 +62.06 |
| 2026-09-02 | -3.83 | $2,453.03 | RRC×64, ANF×14, BHVN×95, BZ×58, CAPR×58 | $10,214.72 | +60.58 | — | RRC, ANF, BHVN, BZ, CAPR | $10,203.77 | $10,203.77 | — | 09:30 open · cash $2,453.03 (unchanged overnight, no fees) · equity $10,214.72 vs prior close $10,154.14 (+60.58) because holdings re-marked: RRC×64 yday $41.32 → 09:30 $41.94 +39.68; ANF×14 yday $143.00 → 09:30 $142.00 -14.00; BHVN×95 yday $15.45 → 09:30 $15.39 -5.70; BZ×58 yday $17.17 → 09:30 $17.29 +6.96; CAPR×58 yday $10.19 → 09:30 $10.77 +33.64 |
| 2026-09-03 | -0.90 | $10,203.77 | — | $10,203.77 | +0.00 | ATRC, HRMY, CABA, VSTM, RVTY, GPRO, FRVO, CRK | — | $18.69 | $10,948.42 | ATRC×45, HRMY×48, CABA×520, VSTM×184, RVTY×9, GPRO×696, FRVO×30, CRK×18 | 09:30 open · cash $10,203.77 · no holdings · equity $10,203.77 vs prior close $10,203.77 (+0.00). Cash unchanged overnight; no fees. |
| 2026-09-04 | — | $18.69 | ATRC×45, HRMY×48, CABA×520, VSTM×184, RVTY×9, GPRO×696, FRVO×30, CRK×18 | $11,081.18 | +132.76 | — | — | $18.69 | $10,593.60 | ATRC×45, HRMY×48, CABA×520, VSTM×184, RVTY×9, GPRO×696, FRVO×30, CRK×18 | 09:30 open · cash $18.69 (unchanged overnight, no fees) · equity $11,081.18 vs prior close $10,948.42 (+132.76) because holdings re-marked: ATRC×45 yday $52.59 → 09:30 $52.88 +13.05; HRMY×48 yday $42.86 → 09:30 $42.93 +3.36; CABA×520 yday $3.57 → 09:30 $3.63 +31.20; VSTM×184 yday $8.02 → 09:30 $8.03 +1.84; RVTY×9 yday $130.94 → 09:30 $132.45 +13.59; GPRO×696 yday $1.69 → 09:30 $1.78 +62.64; FRVO×30 yday $17.98 → 09:30 $18.27 +8.70; CRK×18 yday $15.54 → 09:30 $15.45 -1.62 |

## Fills (09:30 open snapshot, then buys / sells)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 37 | $59.80 | $2.10 | — | $7,785.30 | — | rank-weighted leftover; list flatten; ⚪; ret5=-5.3; leftover $2222.22 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 42 | $45.98 | $2.12 | — | $5,852.02 | — | rank-weighted leftover; list flatten; ⚪; ret5=+12.3; leftover $1944.44 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 32 | $50.62 | $2.09 | — | $4,229.99 | — | rank-weighted leftover; list flatten; ⚪; ret5=+6.2; leftover $1666.67 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TGTX` | 27 | $49.70 | $2.07 | — | $2,886.02 | — | rank-weighted leftover; list flatten; ⚪; ret5=-0.8; leftover $1388.89 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 94 | $11.70 | $2.27 | — | $1,783.95 | — | rank-weighted leftover; list flatten; ⚪; ret5=-0.8; leftover $1111.11 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 28 | $29.74 | $2.07 | — | $949.16 | — | rank-weighted leftover; list flatten; ⚪; ret5=-5.3; leftover $833.33 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 685 | $0.81 | $7.60 | — | $386.70 | — | rank-weighted leftover; list flatten; ⚪; ret5=+13.2; leftover $555.56 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 11 | $23.33 | $2.02 | — | $128.05 | — | rank-weighted leftover; list flatten; ⚪; ret5=+19.7; leftover $277.78 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $128.05 | ▼ 09:30 equity $10,103.42 vs yday $10,117.03 (-13.61) | 09:30 open · cash $128.05 (unchanged overnight, no fees) · equity $10,103.42 vs prior close $10,117.03 (-13.61) because holdings re-marked: BTSG×37 yday $60.23 → 09:30 $59.65 -21.46; IREN×42 yday $44.76 → 09:30 $44.09 -28.14; TPG×32 yday $54.62 → 09:30 $55.29 +21.44; TGTX×27 yday $47.94 → 09:30 $47.27 -18.09; SLS×94 yday $12.36 → 09:30 $12.40 +3.76; HIMS×28 yday $28.77 → 09:30 $29.15 +10.64; INO×685 yday $0.90 → 09:30 $0.93 +20.55; TNDM×11 yday $23.13 → 09:30 $22.92 -2.31 | — |
| 2026-08-14 09:30 ET | **BUY** | `MARA` | 1 | $9.01 | $0.09 | — | $118.95 | — | rank-weighted leftover; list flatten; 🔵; ⚪; ret5=-13.5; leftover $10.67 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 7 | $0.94 | $0.09 | — | $112.30 | — | rank-weighted leftover; list flatten; 🔵; ⚪; ret5=+0.5; leftover $7.11 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 2 | $1.50 | $0.04 | — | $109.27 | — | rank-weighted leftover; list flatten; 🔵; ⚪; ret5=+9.2; leftover $3.56 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $109.27 | ▲ 09:30 equity $10,281.18 vs yday $10,260.71 (+20.47) | 09:30 open · cash $109.27 (unchanged overnight, no fees) · equity $10,281.18 vs prior close $10,260.71 (+20.47) because holdings re-marked: BTSG×37 yday $61.71 → 09:30 $61.69 -0.74; IREN×42 yday $44.06 → 09:30 $45.23 +49.14; TPG×32 yday $53.03 → 09:30 $52.67 -11.52; TGTX×27 yday $48.74 → 09:30 $48.74 +0.00; SLS×94 yday $12.78 → 09:30 $12.78 +0.00; HIMS×28 yday $28.15 → 09:30 $28.14 -0.28; INO×685 yday $1.09 → 09:30 $1.07 -13.70; TNDM×11 yday $22.72 → 09:30 $22.50 -2.42; MARA×1 yday $9.20 → 09:30 $9.22 +0.02; LDI×7 yday $0.90 → 09:30 $0.91 +0.07; BTBT×2 yday $1.57 → 09:30 $1.52 -0.10 | — |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 3 | $4.05 | $0.13 | — | $96.99 | — | rank-weighted leftover; list flatten; 🔵; ⚪; ret5=-12.3; leftover $15.18 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `TGB` | 1 | $8.46 | $0.09 | — | $88.44 | — | rank-weighted leftover; list flatten; 🔵; ⚪; ret5=+0.4; leftover $12.14 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 1 | $3.24 | $0.04 | — | $85.16 | — | rank-weighted leftover; list flatten; ⚪; ret5=+0.3; leftover $6.07 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $85.16 | ▼ 09:30 equity $10,157.73 vs yday $10,290.17 (-132.44) | 09:30 open · cash $85.16 (unchanged overnight, no fees) · equity $10,157.73 vs prior close $10,290.17 (-132.44) because holdings re-marked: BTSG×37 yday $60.38 → 09:30 $60.00 -14.06; IREN×42 yday $44.90 → 09:30 $43.56 -56.28; TPG×32 yday $51.77 → 09:30 $51.77 +0.00; TGTX×27 yday $49.28 → 09:30 $49.28 +0.00; SLS×94 yday $13.00 → 09:30 $12.66 -31.96; HIMS×28 yday $28.61 → 09:30 $27.85 -21.28; INO×685 yday $1.15 → 09:30 $1.14 -6.85; TNDM×11 yday $22.25 → 09:30 $22.16 -1.04; MARA×1 yday $9.72 → 09:30 $9.36 -0.36; LDI×7 yday $0.88 → 09:30 $0.87 -0.04; BTBT×2 yday $1.60 → 09:30 $1.54 -0.12; TMC×3 yday $3.77 → 09:30 $3.72 -0.15; TGB×1 yday $8.77 → 09:30 $8.55 -0.22; DNN×1 yday $3.19 → 09:30 $3.11 -0.08 | — |
| 2026-08-18 09:30 ET | **SELL** | `BTSG` | 37 | $60.00 | $2.13 | $+3.17 | $2,303.03 | ▲ +3.17 after sell → book $10,155.60; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `IREN` | 42 | $43.56 | $2.14 | $-105.90 | $4,130.41 | ▼ -105.90 after sell → book $10,153.46; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TPG` | 32 | $51.77 | $2.11 | $+32.50 | $5,784.94 | ▲ +32.50 after sell → book $10,151.35; vs 09:30 mark -2.11 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TGTX` | 27 | $49.28 | $2.09 | $-15.50 | $7,113.41 | ▼ -15.50 after sell → book $10,149.26; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `SLS` | 94 | $12.66 | $2.30 | $+85.67 | $8,301.16 | ▲ +85.67 after sell → book $10,146.97; vs 09:30 mark -2.29 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `HIMS` | 28 | $27.85 | $2.09 | $-57.09 | $9,078.86 | ▼ -57.09 after sell → book $10,144.87; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `INO` | 685 | $1.14 | $8.96 | $+209.49 | $9,850.80 | ▲ +209.49 after sell → book $10,135.91; vs 09:30 mark -8.96 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TNDM` | 11 | $22.16 | $2.04 | $-16.94 | $10,092.52 | ▼ -16.94 after sell → book $10,133.87; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,092.52 | ▲ 09:30 equity $10,134.11 vs yday $10,133.65 (+0.46) | 09:30 open · cash $10,092.52 (unchanged overnight, no fees) · equity $10,134.11 vs prior close $10,133.65 (+0.46) because holdings re-marked: MARA×1 yday $8.96 → 09:30 $8.91 -0.05; LDI×7 yday $0.86 → 09:30 $0.88 +0.15; BTBT×2 yday $1.45 → 09:30 $1.42 -0.06; TMC×3 yday $3.92 → 09:30 $3.93 +0.03; TGB×1 yday $8.36 → 09:30 $8.70 +0.34; DNN×1 yday $3.15 → 09:30 $3.19 +0.04 | — |
| 2026-08-19 09:30 ET | **SELL** | `MARA` | 1 | $8.91 | $0.11 | $-0.31 | $10,101.32 | ▼ -0.31 after sell → book $10,134.00; vs 09:30 mark -0.11 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `LDI` | 7 | $0.88 | $0.10 | $-0.59 | $10,107.37 | ▼ -0.59 after sell → book $10,133.89; vs 09:30 mark -0.11 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `BTBT` | 2 | $1.42 | $0.05 | $-0.25 | $10,110.16 | ▼ -0.25 after sell → book $10,133.84; vs 09:30 mark -0.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,110.16 | ▼ 09:30 equity $10,133.47 vs yday $10,133.76 (-0.29) | 09:30 open · cash $10,110.16 (unchanged overnight, no fees) · equity $10,133.47 vs prior close $10,133.76 (-0.29) because holdings re-marked: TMC×3 yday $3.97 → 09:30 $3.92 -0.15; TGB×1 yday $8.47 → 09:30 $8.35 -0.12; DNN×1 yday $3.22 → 09:30 $3.20 -0.02 | — |
| 2026-08-20 09:30 ET | **SELL** | `TMC` | 3 | $3.92 | $0.15 | $-0.67 | $10,121.77 | ▼ -0.67 after sell → book $10,133.32; vs 09:30 mark -0.15 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `TGB` | 1 | $8.35 | $0.11 | $-0.30 | $10,130.02 | ▼ -0.30 after sell → book $10,133.22; vs 09:30 mark -0.10 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `DNN` | 1 | $3.20 | $0.06 | $-0.13 | $10,133.16 | ▼ -0.13 after sell → book $10,133.16; vs 09:30 mark -0.06 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 109 | $20.55 | $2.32 | — | $7,890.89 | — | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $2251.81 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 21 | $91.01 | $2.05 | — | $5,977.63 | — | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1970.34 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 81 | $20.65 | $2.23 | — | $4,302.75 | — | rank-weighted leftover; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1688.86 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 243 | $5.77 | $3.13 | — | $2,897.50 | — | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1407.38 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 57 | $19.63 | $2.16 | — | $1,776.43 | — | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1125.91 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 28 | $29.63 | $2.07 | — | $944.72 | — | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $844.43 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 321 | $1.75 | $4.14 | — | $378.83 | — | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $562.95 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 1 | $144.54 | $1.45 | — | $232.84 | — | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $281.48 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $232.84 | ▲ 09:30 equity $10,606.36 vs yday $10,332.74 (+273.62) | 09:30 open · cash $232.84 (unchanged overnight, no fees) · equity $10,606.36 vs prior close $10,332.74 (+273.62) because holdings re-marked: AG×109 yday $21.19 → 09:30 $21.90 +77.39; BHP×21 yday $93.63 → 09:30 $95.72 +43.89; CDE×81 yday $21.11 → 09:30 $21.75 +51.84; HDSN×243 yday $5.57 → 09:30 $5.67 +24.30; IAG×57 yday $20.50 → 09:30 $21.17 +38.19; KGC×28 yday $31.43 → 09:30 $32.17 +20.72; NFGC×321 yday $1.75 → 09:30 $1.79 +12.84; WPM×1 yday $150.25 → 09:30 $154.70 +4.45 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 2 | $17.20 | $0.35 | — | $198.09 | — | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $45.27 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 2 | $11.13 | $0.23 | — | $175.60 | — | rank-weighted leftover; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $32.34 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 10 | $2.47 | $0.28 | — | $150.62 | — | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $25.87 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 10 | $1.93 | $0.22 | — | $131.10 | — | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $19.40 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 4 | $1.32 | $0.06 | — | $125.76 | — | rank-weighted leftover; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $6.47 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $125.76 | ▲ 09:30 equity $10,632.18 vs yday $10,508.12 (+124.06) | 09:30 open · cash $125.76 (unchanged overnight, no fees) · equity $10,632.18 vs prior close $10,508.12 (+124.06) because holdings re-marked: AG×109 yday $21.09 → 09:30 $21.47 +41.42; BHP×21 yday $97.03 → 09:30 $97.34 +6.51; CDE×81 yday $20.97 → 09:30 $21.26 +23.49; HDSN×243 yday $5.63 → 09:30 $5.69 +14.58; IAG×57 yday $21.14 → 09:30 $21.44 +17.10; KGC×28 yday $32.76 → 09:30 $33.21 +12.60; NFGC×321 yday $1.84 → 09:30 $1.86 +6.42; WPM×1 yday $157.78 → 09:30 $158.96 +1.18; AUPH×2 yday $16.65 → 09:30 $16.60 -0.10; ARCT×2 yday $13.45 → 09:30 $13.26 -0.38; AUTL×10 yday $2.41 → 09:30 $2.36 -0.50; CRDL×10 yday $1.86 → 09:30 $1.87 +0.10; CYPH×4 yday $1.42 → 09:30 $1.83 +1.64 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $125.76 | ▲ 09:30 equity $10,467.32 vs yday $10,414.61 (+52.71) | 09:30 open · cash $125.76 (unchanged overnight, no fees) · equity $10,467.32 vs prior close $10,414.61 (+52.71) because holdings re-marked: AG×109 yday $20.57 → 09:30 $20.73 +17.44; BHP×21 yday $96.66 → 09:30 $95.95 -14.91; CDE×81 yday $20.49 → 09:30 $20.85 +29.16; HDSN×243 yday $5.57 → 09:30 $5.53 -9.72; IAG×57 yday $21.36 → 09:30 $21.63 +15.39; KGC×28 yday $32.47 → 09:30 $32.76 +8.12; NFGC×321 yday $1.90 → 09:30 $1.91 +3.21; WPM×1 yday $158.00 → 09:30 $160.00 +2.00; AUPH×2 yday $16.60 → 09:30 $16.71 +0.22; ARCT×2 yday $13.76 → 09:30 $14.34 +1.16; AUTL×10 yday $2.38 → 09:30 $2.32 -0.60; CRDL×10 yday $1.80 → 09:30 $1.90 +1.00; CYPH×4 yday $1.64 → 09:30 $1.70 +0.24 | — |
| 2026-08-25 09:30 ET | **SELL** | `AG` | 109 | $20.73 | $2.35 | $+14.95 | $2,382.97 | ▲ +14.95 after sell → book $10,464.96; vs 09:30 mark -2.36 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BHP` | 21 | $95.95 | $2.08 | $+99.61 | $4,395.84 | ▲ +99.61 after sell → book $10,462.88; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `CDE` | 81 | $20.85 | $2.26 | $+11.71 | $6,082.43 | ▲ +11.71 after sell → book $10,460.62; vs 09:30 mark -2.26 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `HDSN` | 243 | $5.53 | $3.19 | $-64.64 | $7,423.04 | ▼ -64.64 after sell → book $10,457.44; vs 09:30 mark -3.18 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `IAG` | 57 | $21.63 | $2.18 | $+109.66 | $8,653.77 | ▲ +109.66 after sell → book $10,455.26; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `KGC` | 28 | $32.76 | $2.09 | $+83.47 | $9,568.95 | ▲ +83.47 after sell → book $10,453.16; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `NFGC` | 321 | $1.91 | $4.20 | $+43.01 | $10,177.86 | ▲ +43.01 after sell → book $10,448.96; vs 09:30 mark -4.20 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `WPM` | 1 | $160.00 | $1.62 | $+12.39 | $10,336.23 | ▲ +12.39 after sell → book $10,447.33; vs 09:30 mark -1.63 | dropped from list after 3 sess (min 3) | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `MOS` | 95 | $24.00 | $2.27 | — | $8,053.96 | — | rank-weighted leftover; list flatten; ⚪; ret5=+13.0; leftover $2296.94 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟡 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 184 | $10.92 | $2.54 | — | $6,042.14 | — | rank-weighted leftover; list flatten; 🔵; ret5=+10.4; leftover $2009.82 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `INSP` | 28 | $61.47 | $2.07 | — | $4,318.90 | — | rank-weighted leftover; list flatten; 🔵; ret5=+9.2; leftover $1722.71 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 173 | $8.28 | $2.51 | — | $2,883.95 | — | rank-weighted leftover; list flatten; 🔵; ⚪; ret5=+8.8; leftover $1435.59 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 219 | $5.23 | $2.83 | — | $1,735.76 | — | rank-weighted leftover; list flatten; ret5=+10.7; leftover $1148.47 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `HCA` | 2 | $429.24 | $2.00 | — | $875.28 | — | rank-weighted leftover; list flatten; ret5=+6.1; leftover $861.35 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 354 | $1.62 | $4.57 | — | $297.24 | — | rank-weighted leftover; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.8; leftover $574.24 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `NPWR` | 143 | $2.00 | $2.42 | — | $8.82 | — | rank-weighted leftover; list probable,yday_gainer,yday_mover; 🔵; ret5=-12.8; leftover $287.12 | join🟡 sector🟢 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8.82 | ▲ 09:30 equity $10,413.06 vs yday $10,413.06 (-0.00) | 09:30 open · cash $8.82 (unchanged overnight, no fees) · equity $10,413.06 vs prior close $10,413.06 (-0.00) because holdings re-marked: AUPH×2 yday $16.71 → 09:30 $16.71 +0.00; ARCT×2 yday $14.21 → 09:30 $14.21 +0.00; AUTL×10 yday $2.34 → 09:30 $2.34 +0.00; CRDL×10 yday $1.90 → 09:30 $1.90 +0.00; CYPH×4 yday $1.64 → 09:30 $1.64 +0.00; MOS×95 yday $23.75 → 09:30 $23.75 +0.00; OCUL×184 yday $10.92 → 09:30 $10.92 +0.00; INSP×28 yday $61.47 → 09:30 $61.47 +0.00; CRMD×173 yday $8.28 → 09:30 $8.28 +0.00; RZLT×219 yday $5.29 → 09:30 $5.29 +0.00; HCA×2 yday $428.50 → 09:30 $428.50 +0.00; BMEA×354 yday $1.61 → 09:30 $1.61 +0.00; NPWR×143 yday $2.02 → 09:30 $2.02 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8.82 | ▲ 09:30 equity $10,486.12 vs yday $10,426.13 (+59.99) | 09:30 open · cash $8.82 (unchanged overnight, no fees) · equity $10,486.12 vs prior close $10,426.13 (+59.99) because holdings re-marked: AUPH×2 yday $16.71 → 09:30 $16.60 -0.22; ARCT×2 yday $14.21 → 09:30 $15.35 +2.28; AUTL×10 yday $2.34 → 09:30 $2.41 +0.70; CRDL×10 yday $1.90 → 09:30 $2.03 +1.30; CYPH×4 yday $1.64 → 09:30 $1.60 -0.16; MOS×95 yday $23.75 → 09:30 $24.84 +103.55; OCUL×184 yday $10.92 → 09:30 $10.79 -23.92; INSP×28 yday $61.47 → 09:30 $60.07 -39.20; CRMD×173 yday $8.28 → 09:30 $8.60 +55.36; RZLT×219 yday $5.29 → 09:30 $5.01 -61.32; HCA×2 yday $428.50 → 09:30 $427.50 -2.00; BMEA×354 yday $1.61 → 09:30 $1.75 +49.56; NPWR×143 yday $2.02 → 09:30 $1.93 -12.87 | — |
| 2026-08-27 09:30 ET | **SELL** | `AUPH` | 2 | $16.60 | $0.36 | $-1.91 | $41.66 | ▼ -1.91 after sell → book $10,485.76; vs 09:30 mark -0.36 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `ARCT` | 2 | $15.35 | $0.33 | $+7.88 | $72.03 | ▲ +7.88 after sell → book $10,485.43; vs 09:30 mark -0.33 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `AUTL` | 10 | $2.41 | $0.29 | $-1.17 | $95.84 | ▼ -1.17 after sell → book $10,485.14; vs 09:30 mark -0.29 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `CRDL` | 10 | $2.03 | $0.25 | $+0.52 | $115.88 | ▲ +0.52 after sell → book $10,484.88; vs 09:30 mark -0.26 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `CYPH` | 4 | $1.60 | $0.10 | $+0.96 | $122.19 | ▲ +0.96 after sell → book $10,484.79; vs 09:30 mark -0.09 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 1 | $14.09 | $0.14 | — | $107.95 | — | rank-weighted leftover; list flatten; ret5=+1.1; leftover $26.18 | join🟢 sector🔴 gen🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 8 | $2.59 | $0.23 | — | $87.00 | — | rank-weighted leftover; list flatten; ret5=+4.2; leftover $21.82 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `GGB` | 2 | $4.42 | $0.09 | — | $78.07 | — | rank-weighted leftover; list mover_buy; 🔵; ret5=-8.6; leftover $13.09 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $78.07 | ▼ 09:30 equity $10,403.12 vs yday $10,403.37 (-0.25) | 09:30 open · cash $78.07 (unchanged overnight, no fees) · equity $10,403.12 vs prior close $10,403.37 (-0.25) because holdings re-marked: MOS×95 yday $24.16 → 09:30 $24.00 -15.20; OCUL×184 yday $10.77 → 09:30 $10.63 -25.76; INSP×28 yday $61.80 → 09:30 $62.10 +8.40; CRMD×173 yday $8.39 → 09:30 $8.49 +17.30; RZLT×219 yday $5.04 → 09:30 $5.07 +6.57; HCA×2 yday $427.16 → 09:30 $424.61 -5.10; BMEA×354 yday $1.71 → 09:30 $1.74 +10.62; NPWR×143 yday $1.81 → 09:30 $1.83 +2.86; CRK×1 yday $14.50 → 09:30 $14.42 -0.08; SLI×8 yday $2.61 → 09:30 $2.60 -0.08; GGB×2 yday $4.46 → 09:30 $4.57 +0.22 | — |
| 2026-08-28 09:30 ET | **SELL** | `OCUL` | 184 | $10.63 | $2.59 | $-58.49 | $2,031.40 | ▼ -58.49 after sell → book $10,400.53; vs 09:30 mark -2.59 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `INSP` | 28 | $62.10 | $2.10 | $+13.47 | $3,768.10 | ▲ +13.47 after sell → book $10,398.43; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `CRMD` | 173 | $8.49 | $2.55 | $+31.27 | $5,234.32 | ▲ +31.27 after sell → book $10,395.88; vs 09:30 mark -2.55 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `RZLT` | 219 | $5.07 | $2.87 | $-40.74 | $6,341.78 | ▼ -40.74 after sell → book $10,393.01; vs 09:30 mark -2.87 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `HCA` | 2 | $424.61 | $2.02 | $-13.27 | $7,188.98 | ▼ -13.27 after sell → book $10,390.99; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `BMEA` | 354 | $1.74 | $4.64 | $+33.28 | $7,800.31 | ▲ +33.28 after sell → book $10,386.36; vs 09:30 mark -4.63 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `NPWR` | 143 | $1.83 | $2.45 | $-29.18 | $8,059.55 | ▼ -29.18 after sell → book $10,383.91; vs 09:30 mark -2.45 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `RRC` | 64 | $41.44 | $2.18 | — | $5,405.20 | — | rank-weighted leftover; list flatten; ret5=+1.8; leftover $2686.52 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 14 | $144.70 | $2.03 | — | $3,377.37 | — | rank-weighted leftover; list probable,yday_gainer,yday_mover; 🔵; ret5=+0.8; leftover $2149.21 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BHVN` | 95 | $16.95 | $2.27 | — | $1,764.85 | — | rank-weighted leftover; list probable,yday_gainer,yday_mover; ret5=-5.4; leftover $1611.91 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BZ` | 58 | $18.50 | $2.16 | — | $689.68 | — | rank-weighted leftover; list probable,yday_gainer,yday_mover; ret5=+2.8; leftover $1074.61 | join🟢 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `CAPR` | 58 | $9.19 | $2.16 | — | $154.50 | — | rank-weighted leftover; list probable,yday_gainer,yday_mover; ret5=-5.4; leftover $537.30 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $154.50 | ▼ 09:30 equity $10,218.85 vs yday $10,321.18 (-102.33) | 09:30 open · cash $154.50 (unchanged overnight, no fees) · equity $10,218.85 vs prior close $10,321.18 (-102.33) because holdings re-marked: MOS×95 yday $23.76 → 09:30 $23.75 -0.95; CRK×1 yday $14.62 → 09:30 $14.56 -0.06; SLI×8 yday $2.64 → 09:30 $2.51 -1.04; GGB×2 yday $4.70 → 09:30 $4.55 -0.30; RRC×64 yday $41.64 → 09:30 $41.11 -33.92; ANF×14 yday $145.75 → 09:30 $148.67 +40.88; BHVN×95 yday $16.12 → 09:30 $15.44 -64.60; BZ×58 yday $18.00 → 09:30 $17.89 -6.38; CAPR×58 yday $10.06 → 09:30 $9.44 -35.96 | — |
| 2026-08-31 09:30 ET | **SELL** | `MOS` | 95 | $23.75 | $2.31 | $-28.33 | $2,408.44 | ▼ -28.33 after sell → book $10,216.54; vs 09:30 mark -2.31 | dropped from list after 4 sess (min 3) | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,408.44 | ▼ 09:30 equity $10,172.78 vs yday $10,260.05 (-87.27) | 09:30 open · cash $2,408.44 (unchanged overnight, no fees) · equity $10,172.78 vs prior close $10,260.05 (-87.27) because holdings re-marked: CRK×1 yday $14.51 → 09:30 $14.31 -0.20; SLI×8 yday $2.51 → 09:30 $2.70 +1.52; GGB×2 yday $4.55 → 09:30 $4.61 +0.12; RRC×64 yday $41.78 → 09:30 $41.32 -29.44; ANF×14 yday $149.28 → 09:30 $142.47 -95.34; BHVN×95 yday $15.40 → 09:30 $15.45 +4.75; BZ×58 yday $17.90 → 09:30 $17.37 -30.74; CAPR×58 yday $9.36 → 09:30 $10.43 +62.06 | — |
| 2026-09-01 09:30 ET | **SELL** | `CRK` | 1 | $14.31 | $0.17 | $-0.09 | $2,422.58 | ▼ -0.09 after sell → book $10,172.61; vs 09:30 mark -0.17 | dropped from list after 3 sess (min 3) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-01 09:30 ET | **SELL** | `SLI` | 8 | $2.70 | $0.26 | $+0.39 | $2,443.92 | ▲ +0.39 after sell → book $10,172.35; vs 09:30 mark -0.26 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `GGB` | 2 | $4.61 | $0.12 | $+0.17 | $2,453.03 | ▲ +0.17 after sell → book $10,172.24; vs 09:30 mark -0.11 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,453.03 | ▲ 09:30 equity $10,214.72 vs yday $10,154.14 (+60.58) | 09:30 open · cash $2,453.03 (unchanged overnight, no fees) · equity $10,214.72 vs prior close $10,154.14 (+60.58) because holdings re-marked: RRC×64 yday $41.32 → 09:30 $41.94 +39.68; ANF×14 yday $143.00 → 09:30 $142.00 -14.00; BHVN×95 yday $15.45 → 09:30 $15.39 -5.70; BZ×58 yday $17.17 → 09:30 $17.29 +6.96; CAPR×58 yday $10.19 → 09:30 $10.77 +33.64 | — |
| 2026-09-02 09:30 ET | **SELL** | `RRC` | 64 | $41.94 | $2.21 | $+27.60 | $5,134.97 | ▲ +27.60 after sell → book $10,212.50; vs 09:30 mark -2.22 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `ANF` | 14 | $142.00 | $2.06 | $-41.89 | $7,120.91 | ▼ -41.89 after sell → book $10,210.44; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `BHVN` | 95 | $15.39 | $2.30 | $-152.78 | $8,580.66 | ▼ -152.78 after sell → book $10,208.14; vs 09:30 mark -2.30 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `BZ` | 58 | $17.29 | $2.18 | $-74.53 | $9,581.30 | ▼ -74.53 after sell → book $10,205.96; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `CAPR` | 58 | $10.77 | $2.18 | $+87.29 | $10,203.77 | ▲ +87.29 after sell → book $10,203.77; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,203.77 | ▲ 09:30 equity $10,203.77 vs yday $10,203.77 (+0.00) | 09:30 open · cash $10,203.77 · no holdings · equity $10,203.77 vs prior close $10,203.77 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 45 | $49.76 | $2.12 | — | $7,962.45 | — | rank-weighted leftover; list flatten; 🔵; ⚪; ret5=+10.6; leftover $2267.51 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 48 | $41.31 | $2.13 | — | $5,977.43 | — | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+0.1; leftover $1984.07 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 520 | $3.27 | $6.71 | — | $4,270.33 | — | rank-weighted leftover; list flatten; 🔵; ⚪; ret5=+13.8; leftover $1700.63 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 184 | $7.70 | $2.54 | — | $2,850.98 | — | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+4.7; leftover $1417.19 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 9 | $125.94 | $2.02 | — | $1,715.51 | — | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $1133.75 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `GPRO` | 696 | $1.22 | $8.98 | — | $857.41 | — | rank-weighted leftover; list probable,yday_gainer,yday_mover; ret5=+5.9; leftover $850.31 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FRVO` | 30 | $18.40 | $2.08 | — | $303.33 | — | rank-weighted leftover; list probable,yday_gainer,yday_mover; ret5=-14.4; leftover $566.88 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 18 | $15.70 | $2.04 | — | $18.69 | — | rank-weighted leftover; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+1.1; leftover $283.44 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $18.69 | ▲ 09:30 equity $11,081.18 vs yday $10,948.42 (+132.76) | 09:30 open · cash $18.69 (unchanged overnight, no fees) · equity $11,081.18 vs prior close $10,948.42 (+132.76) because holdings re-marked: ATRC×45 yday $52.59 → 09:30 $52.88 +13.05; HRMY×48 yday $42.86 → 09:30 $42.93 +3.36; CABA×520 yday $3.57 → 09:30 $3.63 +31.20; VSTM×184 yday $8.02 → 09:30 $8.03 +1.84; RVTY×9 yday $130.94 → 09:30 $132.45 +13.59; GPRO×696 yday $1.69 → 09:30 $1.78 +62.64; FRVO×30 yday $17.98 → 09:30 $18.27 +8.70; CRK×18 yday $15.54 → 09:30 $15.45 -1.62 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `BTSG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `IREN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `TPG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `TGTX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `SLS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `HIMS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `INO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `TNDM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `TLN` | cash | leftover split 28.46 < 1 share @ 359.83 |
| 2026-08-14 | `VST` | cash | leftover split 24.90 < 1 share @ 146.90 |
| 2026-08-14 | `NRG` | cash | leftover split 21.34 < 1 share @ 120.00 |
| 2026-08-14 | `DAVE` | cash | leftover split 17.78 < 1 share @ 330.91 |
| 2026-08-14 | `SLG` | cash | leftover split 14.23 < 1 share @ 57.61 |
| 2026-08-17 | `BTSG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `IREN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `TPG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `TGTX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `SLS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `HIMS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `INO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `TNDM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `MARA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `LDI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `BTBT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `DVN` | cash | leftover split 24.28 < 1 share @ 46.18 |
| 2026-08-17 | `EOG` | cash | leftover split 21.25 < 1 share @ 142.77 |
| 2026-08-17 | `FANG` | cash | leftover split 18.21 < 1 share @ 202.70 |
| 2026-08-17 | `ELF` | cash | leftover split 9.11 < 1 share @ 90.54 |
| 2026-08-17 | `HNST` | cash | leftover split 3.04 < 1 share @ 4.81 |
| 2026-08-18 | `MARA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `LDI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `BTBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `TMC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `TGB` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `DNN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `OXY` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `APA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `COP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `MUR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `MLYS` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `TRMD` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OBE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CYPH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `TMC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `TGB` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `DNN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `STE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DHR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SYK` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MUR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `TRMD` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MLYS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `TBPH` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `AG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `BHP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `CDE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `HDSN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `IAG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `KGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `NFGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `WPM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AU` | cash | leftover split 51.74 < 1 share @ 119.43 |
| 2026-08-21 | `AEM` | cash | leftover split 38.81 < 1 share @ 216.30 |
| 2026-08-21 | `CRSP` | cash | leftover split 12.94 < 1 share @ 59.72 |
| 2026-08-24 | `AG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BHP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `CDE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `HDSN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `IAG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `KGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `NFGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `WPM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AUPH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `ARCT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `AUTL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `CRDL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `CYPH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `RZLT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `MOS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `OCUL` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INSP` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CRMD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HCA` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CRML` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SAFX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `AUPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `ARCT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `AUTL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `CRDL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `CYPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `AUPH` | no_price | no 09:30 open — carry |
| 2026-08-26 | `ARCT` | no_price | no 09:30 open — carry |
| 2026-08-26 | `AUTL` | no_price | no 09:30 open — carry |
| 2026-08-26 | `CRDL` | no_price | no 09:30 open — carry |
| 2026-08-26 | `CYPH` | no_price | no 09:30 open — carry |
| 2026-08-26 | `BMEA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `NPWR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `KURA` | no_price | no 09:30 open |
| 2026-08-26 | `AVBP` | no_price | no 09:30 open |
| 2026-08-27 | `OCUL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `INSP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `CRMD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `RZLT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `HCA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `BMEA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `NPWR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `RRC` | cash | leftover split 30.55 < 1 share @ 40.72 |
| 2026-08-27 | `ACMR` | cash | leftover split 17.46 < 1 share @ 80.97 |
| 2026-08-27 | `MT` | cash | leftover split 8.73 < 1 share @ 75.12 |
| 2026-08-27 | `MU` | cash | leftover split 4.36 < 1 share @ 925.74 |
| 2026-08-28 | `GGB` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `CRK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `SLI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `GGB` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `RRC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `ANF` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `BHVN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `BZ` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `CAPR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `RES` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PBF` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `WTTR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `OKTA` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRWD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `VEEV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `RRC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `ANF` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `BHVN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `BZ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `CAPR` | min_hold | dropped but min-hold 2/3 sess — no sell |
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
| 2026-09-04 | `HRMY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `VSTM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `RVTY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `FRVO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CRK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `ASND` | cash | leftover split 6.23 < 1 share @ 266.94 |
| 2026-09-04 | `OSCR` | cash | leftover split 4.98 < 1 share @ 30.65 |
| 2026-09-04 | `NVAX` | cash | leftover split 3.74 < 1 share @ 10.41 |
| 2026-09-04 | `BVS` | cash | leftover split 2.49 < 1 share @ 14.50 |
| 2026-09-04 | `BAK` | cash | leftover split 1.25 < 1 share @ 1.95 |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `ATRC` | 45 | 2026-09-03 @ $49.76 | rank-weighted leftover; list flatten; 🔵; ⚪; ret5=+10.6; leftover $2267.51 |
| `HRMY` | 48 | 2026-09-03 @ $41.31 | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+0.1; leftover $1984.07 |
| `CABA` | 520 | 2026-09-03 @ $3.27 | rank-weighted leftover; list flatten; 🔵; ⚪; ret5=+13.8; leftover $1700.63 |
| `VSTM` | 184 | 2026-09-03 @ $7.70 | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+4.7; leftover $1417.19 |
| `RVTY` | 9 | 2026-09-03 @ $125.94 | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $1133.75 |
| `GPRO` | 696 | 2026-09-03 @ $1.22 | rank-weighted leftover; list probable,yday_gainer,yday_mover; ret5=+5.9; leftover $850.31 |
| `FRVO` | 30 | 2026-09-03 @ $18.40 | rank-weighted leftover; list probable,yday_gainer,yday_mover; ret5=-14.4; leftover $566.88 |
| `CRK` | 18 | 2026-09-03 @ $15.70 | rank-weighted leftover; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+1.1; leftover $283.44 |
