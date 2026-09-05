# Factor mine action — `union_h5_sboost`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **5** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `both` · S≥+5: sizeup + more names

Cash book **+18.18%** ($11,818) · signal-only (no cash/fees) was +58.01%. Starts YES **14/17**. Fills 108 · skips 259 · realized $+1479.28.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `list order` · **top_n** 8 (S≥+5 may raise this when S-boost is `both`).
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **5**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $213.05.

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | vs yday close | Bought | Sold | Close cash | Close equity | Close held | 09:30 why |
|---|---:|---:|---|---:|---:|---|---|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | BTSG, IREN, TPG, TGTX, SLS, HIMS, INO, TNDM, VOR | — | $123.82 | $10,195.74 | BTSG×18, IREN×24, TPG×21, TGTX×22, SLS×94, HIMS×37, INO×1371, TNDM×47, VOR×50 | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-14 | +5.50 | $123.82 | BTSG×18, IREN×24, TPG×21, TGTX×22, SLS×94, HIMS×37, INO×1371, TNDM×47, VOR×50 | $10,219.63 | +23.89 | MARA, LDI, BTBT, ANGX, HYLN | — | $78.00 | $10,434.08 | BTSG×18, IREN×24, TPG×21, TGTX×22, SLS×94, HIMS×37, INO×1371, TNDM×47, VOR×50, MARA×1, LDI×11, BTBT×6, ANGX×2, HYLN×2 | 09:30 open · cash $123.82 (unchanged overnight, no fees) · equity $10,219.63 vs prior close $10,195.74 (+23.89) because holdings re-marked: BTSG×18 yday $60.23 → 09:30 $59.65 -10.44; IREN×24 yday $44.76 → 09:30 $44.09 -16.08; TPG×21 yday $54.62 → 09:30 $55.29 +14.07; TGTX×22 yday $47.94 → 09:30 $47.27 -14.74; SLS×94 yday $12.36 → 09:30 $12.40 +3.76; HIMS×37 yday $28.77 → 09:30 $29.15 +14.06; INO×1371 yday $0.90 → 09:30 $0.93 +41.13; TNDM×47 yday $23.13 → 09:30 $22.92 -9.87; VOR×50 yday $23.29 → 09:30 $23.33 +2.00 |
| 2026-08-17 | +2.25 | $78.00 | BTSG×18, IREN×24, TPG×21, TGTX×22, SLS×94, HIMS×37, INO×1371, TNDM×47, VOR×50, MARA×1, LDI×11, BTBT×6, ANGX×2, HYLN×2 | $10,410.48 | -23.60 | TMC, TGB, DNN, HNST | — | $41.72 | $10,513.02 | BTSG×18, IREN×24, TPG×21, TGTX×22, SLS×94, HIMS×37, INO×1371, TNDM×47, VOR×50, MARA×1, LDI×11, BTBT×6, ANGX×2, HYLN×2, TMC×2, TGB×1, DNN×3, HNST×2 | 09:30 open · cash $78.00 (unchanged overnight, no fees) · equity $10,410.48 vs prior close $10,434.08 (-23.60) because holdings re-marked: BTSG×18 yday $61.71 → 09:30 $61.69 -0.36; IREN×24 yday $44.06 → 09:30 $45.23 +28.08; TPG×21 yday $53.03 → 09:30 $52.67 -7.56; TGTX×22 yday $48.74 → 09:30 $48.74 +0.00; SLS×94 yday $12.78 → 09:30 $12.78 +0.00; HIMS×37 yday $28.15 → 09:30 $28.14 -0.37; INO×1371 yday $1.09 → 09:30 $1.07 -27.42; TNDM×47 yday $22.72 → 09:30 $22.50 -10.34; VOR×50 yday $23.03 → 09:30 $22.91 -6.00; MARA×1 yday $9.20 → 09:30 $9.22 +0.02; LDI×11 yday $0.90 → 09:30 $0.91 +0.11; BTBT×6 yday $1.57 → 09:30 $1.52 -0.30; ANGX×2 yday $4.37 → 09:30 $4.60 +0.46; HYLN×2 yday $4.06 → 09:30 $4.10 +0.08 |
| 2026-08-18 | -6.20 | $41.72 | BTSG×18, IREN×24, TPG×21, TGTX×22, SLS×94, HIMS×37, INO×1371, TNDM×47, VOR×50, MARA×1, LDI×11, BTBT×6, ANGX×2, HYLN×2, TMC×2, TGB×1, DNN×3, HNST×2 | $10,384.75 | -128.27 | — | — | $41.72 | $10,567.86 | BTSG×18, IREN×24, TPG×21, TGTX×22, SLS×94, HIMS×37, INO×1371, TNDM×47, VOR×50, MARA×1, LDI×11, BTBT×6, ANGX×2, HYLN×2, TMC×2, TGB×1, DNN×3, HNST×2 | 09:30 open · cash $41.72 (unchanged overnight, no fees) · equity $10,384.75 vs prior close $10,513.02 (-128.27) because holdings re-marked: BTSG×18 yday $60.38 → 09:30 $60.00 -6.84; IREN×24 yday $44.90 → 09:30 $43.56 -32.16; TPG×21 yday $51.77 → 09:30 $51.77 +0.00; TGTX×22 yday $49.28 → 09:30 $49.28 +0.00; SLS×94 yday $13.00 → 09:30 $12.66 -31.96; HIMS×37 yday $28.61 → 09:30 $27.85 -28.12; INO×1371 yday $1.15 → 09:30 $1.14 -13.71; TNDM×47 yday $22.25 → 09:30 $22.16 -4.46; VOR×50 yday $23.01 → 09:30 $22.82 -9.50; MARA×1 yday $9.72 → 09:30 $9.36 -0.36; LDI×11 yday $0.88 → 09:30 $0.87 -0.06; BTBT×6 yday $1.60 → 09:30 $1.54 -0.36; ANGX×2 yday $4.71 → 09:30 $4.79 +0.16; HYLN×2 yday $4.09 → 09:30 $3.95 -0.28; TMC×2 yday $3.77 → 09:30 $3.72 -0.10; TGB×1 yday $8.77 → 09:30 $8.55 -0.22; DNN×3 yday $3.19 → 09:30 $3.11 -0.24; HNST×2 yday $4.70 → 09:30 $4.67 -0.06 |
| 2026-08-19 | -7.20 | $41.72 | BTSG×18, IREN×24, TPG×21, TGTX×22, SLS×94, HIMS×37, INO×1371, TNDM×47, VOR×50, MARA×1, LDI×11, BTBT×6, ANGX×2, HYLN×2, TMC×2, TGB×1, DNN×3, HNST×2 | $10,728.75 | +160.89 | — | — | $41.72 | $10,988.26 | BTSG×18, IREN×24, TPG×21, TGTX×22, SLS×94, HIMS×37, INO×1371, TNDM×47, VOR×50, MARA×1, LDI×11, BTBT×6, ANGX×2, HYLN×2, TMC×2, TGB×1, DNN×3, HNST×2 | 09:30 open · cash $41.72 (unchanged overnight, no fees) · equity $10,728.75 vs prior close $10,567.86 (+160.89) because holdings re-marked: BTSG×18 yday $59.50 → 09:30 $60.15 +11.70; IREN×24 yday $42.00 → 09:30 $41.41 -14.04; TPG×21 yday $52.02 → 09:30 $52.26 +5.04; TGTX×22 yday $50.26 → 09:30 $51.62 +29.92; SLS×94 yday $13.10 → 09:30 $13.46 +33.84; HIMS×37 yday $27.39 → 09:30 $27.55 +5.92; INO×1371 yday $1.20 → 09:30 $1.22 +27.42; TNDM×47 yday $23.73 → 09:30 $24.20 +22.09; VOR×50 yday $23.28 → 09:30 $24.05 +38.50; MARA×1 yday $8.96 → 09:30 $8.91 -0.05; LDI×11 yday $0.86 → 09:30 $0.88 +0.24; BTBT×6 yday $1.45 → 09:30 $1.42 -0.18; ANGX×2 yday $4.85 → 09:30 $4.79 -0.12; HYLN×2 yday $3.86 → 09:30 $3.87 +0.02; TMC×2 yday $3.92 → 09:30 $3.93 +0.02; TGB×1 yday $8.36 → 09:30 $8.70 +0.34; DNN×3 yday $3.15 → 09:30 $3.19 +0.12; HNST×2 yday $4.75 → 09:30 $4.80 +0.10 |
| 2026-08-20 | +1.12 | $41.72 | BTSG×18, IREN×24, TPG×21, TGTX×22, SLS×94, HIMS×37, INO×1371, TNDM×47, VOR×50, MARA×1, LDI×11, BTBT×6, ANGX×2, HYLN×2, TMC×2, TGB×1, DNN×3, HNST×2 | $10,903.82 | -84.44 | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | BTSG, IREN, TPG, TGTX, SLS, HIMS, INO, TNDM, VOR | $149.46 | $11,097.43 | MARA×1, LDI×11, BTBT×6, ANGX×2, HYLN×2, TMC×2, TGB×1, DNN×3, HNST×2, AG×65, BHP×14, CDE×65, HDSN×233, IAG×68, KGC×45, NFGC×770, WPM×9 | 09:30 open · cash $41.72 (unchanged overnight, no fees) · equity $10,903.82 vs prior close $10,988.26 (-84.44) because holdings re-marked: BTSG×18 yday $59.33 → 09:30 $58.64 -12.42; IREN×24 yday $42.84 → 09:30 $42.46 -9.12; TPG×21 yday $53.18 → 09:30 $53.06 -2.52; TGTX×22 yday $51.69 → 09:30 $51.65 -0.88; SLS×94 yday $13.85 → 09:30 $13.84 -0.94; HIMS×37 yday $31.09 → 09:30 $30.66 -15.91; INO×1371 yday $1.30 → 09:30 $1.30 +0.00; TNDM×47 yday $23.46 → 09:30 $23.11 -16.45; VOR×50 yday $23.58 → 09:30 $23.05 -26.50; MARA×1 yday $9.65 → 09:30 $10.21 +0.56; LDI×11 yday $0.88 → 09:30 $0.87 -0.06; BTBT×6 yday $1.40 → 09:30 $1.46 +0.33; ANGX×2 yday $4.60 → 09:30 $4.57 -0.06; HYLN×2 yday $3.67 → 09:30 $3.61 -0.12; TMC×2 yday $3.97 → 09:30 $3.92 -0.10; TGB×1 yday $8.47 → 09:30 $8.35 -0.12; DNN×3 yday $3.22 → 09:30 $3.20 -0.06; HNST×2 yday $5.02 → 09:30 $4.98 -0.08 |
| 2026-08-21 | +3.25 | $149.46 | MARA×1, LDI×11, BTBT×6, ANGX×2, HYLN×2, TMC×2, TGB×1, DNN×3, HNST×2, AG×65, BHP×14, CDE×65, HDSN×233, IAG×68, KGC×45, NFGC×770, WPM×9 | $11,389.43 | +292.00 | AUPH, ARCT, AUTL, CRDL, CYPH | MARA, LDI, BTBT, ANGX, HYLN | $85.90 | $11,391.04 | TMC×2, TGB×1, DNN×3, HNST×2, AG×65, BHP×14, CDE×65, HDSN×233, IAG×68, KGC×45, NFGC×770, WPM×9, AUPH×1, ARCT×2, AUTL×9, CRDL×12, CYPH×18 | 09:30 open · cash $149.46 (unchanged overnight, no fees) · equity $11,389.43 vs prior close $11,097.43 (+292.00) because holdings re-marked: MARA×1 yday $11.15 → 09:30 $11.70 +0.55; LDI×11 yday $0.87 → 09:30 $0.87 -0.03; BTBT×6 yday $1.59 → 09:30 $1.66 +0.39; ANGX×2 yday $4.37 → 09:30 $4.43 +0.12; HYLN×2 yday $3.37 → 09:30 $3.42 +0.10; TMC×2 yday $3.97 → 09:30 $4.10 +0.26; TGB×1 yday $8.69 → 09:30 $9.00 +0.31; DNN×3 yday $3.14 → 09:30 $3.23 +0.27; HNST×2 yday $4.96 → 09:30 $4.97 +0.02; AG×65 yday $21.19 → 09:30 $21.90 +46.15; BHP×14 yday $93.63 → 09:30 $95.72 +29.26; CDE×65 yday $21.11 → 09:30 $21.75 +41.60; HDSN×233 yday $5.57 → 09:30 $5.67 +23.30; IAG×68 yday $20.50 → 09:30 $21.17 +45.56; KGC×45 yday $31.43 → 09:30 $32.17 +33.30; NFGC×770 yday $1.75 → 09:30 $1.79 +30.80; WPM×9 yday $150.25 → 09:30 $154.70 +40.05 |
| 2026-08-24 | -5.17 | $85.90 | TMC×2, TGB×1, DNN×3, HNST×2, AG×65, BHP×14, CDE×65, HDSN×233, IAG×68, KGC×45, NFGC×770, WPM×9, AUPH×1, ARCT×2, AUTL×9, CRDL×12, CYPH×18 | $11,525.83 | +134.79 | — | TMC, TGB, DNN, HNST | $124.40 | $11,359.64 | AG×65, BHP×14, CDE×65, HDSN×233, IAG×68, KGC×45, NFGC×770, WPM×9, AUPH×1, ARCT×2, AUTL×9, CRDL×12, CYPH×18 | 09:30 open · cash $85.90 (unchanged overnight, no fees) · equity $11,525.83 vs prior close $11,391.04 (+134.79) because holdings re-marked: TMC×2 yday $4.79 → 09:30 $4.57 -0.44; TGB×1 yday $9.19 → 09:30 $9.26 +0.07; DNN×3 yday $3.50 → 09:30 $3.50 +0.00; HNST×2 yday $5.05 → 09:30 $5.05 +0.00; AG×65 yday $21.09 → 09:30 $21.47 +24.70; BHP×14 yday $97.03 → 09:30 $97.34 +4.34; CDE×65 yday $20.97 → 09:30 $21.26 +18.85; HDSN×233 yday $5.63 → 09:30 $5.69 +13.98; IAG×68 yday $21.14 → 09:30 $21.44 +20.40; KGC×45 yday $32.76 → 09:30 $33.21 +20.25; NFGC×770 yday $1.84 → 09:30 $1.86 +15.40; WPM×9 yday $157.78 → 09:30 $158.96 +10.62; AUPH×1 yday $16.65 → 09:30 $16.60 -0.05; ARCT×2 yday $13.45 → 09:30 $13.26 -0.38; AUTL×9 yday $2.41 → 09:30 $2.36 -0.45; CRDL×12 yday $1.86 → 09:30 $1.87 +0.12; CYPH×18 yday $1.42 → 09:30 $1.83 +7.38 |
| 2026-08-25 | +1.80 | $124.40 | AG×65, BHP×14, CDE×65, HDSN×233, IAG×68, KGC×45, NFGC×770, WPM×9, AUPH×1, ARCT×2, AUTL×9, CRDL×12, CYPH×18 | $11,434.30 | +74.66 | OCUL, CRMD, RZLT, BMEA, NPWR | — | $65.52 | $11,369.30 | AG×65, BHP×14, CDE×65, HDSN×233, IAG×68, KGC×45, NFGC×770, WPM×9, AUPH×1, ARCT×2, AUTL×9, CRDL×12, CYPH×18, OCUL×1, CRMD×1, RZLT×2, BMEA×9, NPWR×7 | 09:30 open · cash $124.40 (unchanged overnight, no fees) · equity $11,434.30 vs prior close $11,359.64 (+74.66) because holdings re-marked: AG×65 yday $20.57 → 09:30 $20.73 +10.40; BHP×14 yday $96.66 → 09:30 $95.95 -9.94; CDE×65 yday $20.49 → 09:30 $20.85 +23.40; HDSN×233 yday $5.57 → 09:30 $5.53 -9.32; IAG×68 yday $21.36 → 09:30 $21.63 +18.36; KGC×45 yday $32.47 → 09:30 $32.76 +13.05; NFGC×770 yday $1.90 → 09:30 $1.91 +7.70; WPM×9 yday $158.00 → 09:30 $160.00 +18.00; AUPH×1 yday $16.60 → 09:30 $16.71 +0.11; ARCT×2 yday $13.76 → 09:30 $14.34 +1.16; AUTL×9 yday $2.38 → 09:30 $2.32 -0.54; CRDL×12 yday $1.80 → 09:30 $1.90 +1.20; CYPH×18 yday $1.64 → 09:30 $1.70 +1.08 |
| 2026-08-26 | +2.02 | $65.52 | AG×65, BHP×14, CDE×65, HDSN×233, IAG×68, KGC×45, NFGC×770, WPM×9, AUPH×1, ARCT×2, AUTL×9, CRDL×12, CYPH×18, OCUL×1, CRMD×1, RZLT×2, BMEA×9, NPWR×7 | $11,369.30 | +0.00 | — | — | $65.52 | $11,433.66 | AG×65, BHP×14, CDE×65, HDSN×233, IAG×68, KGC×45, NFGC×770, WPM×9, AUPH×1, ARCT×2, AUTL×9, CRDL×12, CYPH×18, OCUL×1, CRMD×1, RZLT×2, BMEA×9, NPWR×7 | 09:30 open · cash $65.52 (unchanged overnight, no fees) · equity $11,369.30 vs prior close $11,369.30 (+0.00) because holdings re-marked: AG×65 yday $20.68 → 09:30 $20.68 +0.00; BHP×14 yday $96.05 → 09:30 $96.05 +0.00; CDE×65 yday $20.71 → 09:30 $20.71 +0.00; HDSN×233 yday $5.49 → 09:30 $5.49 +0.00; IAG×68 yday $21.48 → 09:30 $21.48 +0.00; KGC×45 yday $32.55 → 09:30 $32.55 +0.00; NFGC×770 yday $1.90 → 09:30 $1.90 +0.00; WPM×9 yday $158.25 → 09:30 $158.25 +0.00; AUPH×1 yday $16.71 → 09:30 $16.71 +0.00; ARCT×2 yday $14.21 → 09:30 $14.21 +0.00; AUTL×9 yday $2.34 → 09:30 $2.34 +0.00; CRDL×12 yday $1.90 → 09:30 $1.90 +0.00; CYPH×18 yday $1.64 → 09:30 $1.64 +0.00; OCUL×1 yday $10.92 → 09:30 $10.92 +0.00; CRMD×1 yday $8.28 → 09:30 $8.28 +0.00; RZLT×2 yday $5.29 → 09:30 $5.29 +0.00; BMEA×9 yday $1.61 → 09:30 $1.61 +0.00; NPWR×7 yday $2.02 → 09:30 $2.02 +0.00 |
| 2026-08-27 | — | $65.52 | AG×65, BHP×14, CDE×65, HDSN×233, IAG×68, KGC×45, NFGC×770, WPM×9, AUPH×1, ARCT×2, AUTL×9, CRDL×12, CYPH×18, OCUL×1, CRMD×1, RZLT×2, BMEA×9, NPWR×7 | $11,534.37 | +100.71 | RRC, CRK, MOS, SLI, ACMR, GGB, MT, MU | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | $611.47 | $11,509.85 | AUPH×1, ARCT×2, AUTL×9, CRDL×12, CYPH×18, OCUL×1, CRMD×1, RZLT×2, BMEA×9, NPWR×7, RRC×34, CRK×100, MOS×57, SLI×546, ACMR×17, GGB×320, MT×18, MU×1 | 09:30 open · cash $65.52 (unchanged overnight, no fees) · equity $11,534.37 vs prior close $11,433.66 (+100.71) because holdings re-marked: AG×65 yday $20.68 → 09:30 $20.63 -3.25; BHP×14 yday $96.05 → 09:30 $96.99 +13.16; CDE×65 yday $20.71 → 09:30 $21.00 +18.85; HDSN×233 yday $5.49 → 09:30 $5.51 +4.66; IAG×68 yday $21.48 → 09:30 $21.64 +10.88; KGC×45 yday $32.55 → 09:30 $32.90 +15.75; NFGC×770 yday $1.90 → 09:30 $2.00 +77.00; WPM×9 yday $158.25 → 09:30 $160.93 +24.12; AUPH×1 yday $16.71 → 09:30 $16.60 -0.11; ARCT×2 yday $14.21 → 09:30 $15.35 +2.28; AUTL×9 yday $2.34 → 09:30 $2.41 +0.63; CRDL×12 yday $1.90 → 09:30 $2.03 +1.56; CYPH×18 yday $1.64 → 09:30 $1.60 -0.72; OCUL×1 yday $10.92 → 09:30 $10.79 -0.13; CRMD×1 yday $8.28 → 09:30 $8.60 +0.32; RZLT×2 yday $5.29 → 09:30 $5.01 -0.56; BMEA×9 yday $1.61 → 09:30 $1.75 +1.26; NPWR×7 yday $2.02 → 09:30 $1.93 -0.63 |
| 2026-08-28 | +0.75 | $611.47 | AUPH×1, ARCT×2, AUTL×9, CRDL×12, CYPH×18, OCUL×1, CRMD×1, RZLT×2, BMEA×9, NPWR×7, RRC×34, CRK×100, MOS×57, SLI×546, ACMR×17, GGB×320, MT×18, MU×1 | $11,592.35 | +82.50 | ANF, BHVN, BZ, CAPR | AUPH, ARCT, AUTL, CRDL, CYPH | $64.14 | $11,616.31 | OCUL×1, CRMD×1, RZLT×2, BMEA×9, NPWR×7, RRC×34, CRK×100, MOS×57, SLI×546, ACMR×17, GGB×320, MT×18, MU×1, ANF×1, BHVN×10, BZ×9, CAPR×20 | 09:30 open · cash $611.47 (unchanged overnight, no fees) · equity $11,592.35 vs prior close $11,509.85 (+82.50) because holdings re-marked: AUPH×1 yday $16.54 → 09:30 $16.47 -0.07; ARCT×2 yday $15.83 → 09:30 $15.74 -0.18; AUTL×9 yday $2.33 → 09:30 $2.32 -0.09; CRDL×12 yday $2.14 → 09:30 $2.09 -0.60; CYPH×18 yday $1.63 → 09:30 $1.75 +2.16; OCUL×1 yday $10.77 → 09:30 $10.63 -0.14; CRMD×1 yday $8.39 → 09:30 $8.49 +0.10; RZLT×2 yday $5.04 → 09:30 $5.07 +0.06; BMEA×9 yday $1.71 → 09:30 $1.74 +0.27; NPWR×7 yday $1.81 → 09:30 $1.83 +0.14; RRC×34 yday $41.55 → 09:30 $41.44 -3.74; CRK×100 yday $14.50 → 09:30 $14.42 -8.00; MOS×57 yday $24.16 → 09:30 $24.00 -9.12; SLI×546 yday $2.61 → 09:30 $2.60 -5.46; ACMR×17 yday $79.11 → 09:30 $81.65 +43.18; GGB×320 yday $4.46 → 09:30 $4.57 +35.20; MT×18 yday $74.53 → 09:30 $74.54 +0.18; MU×1 yday $938.40 → 09:30 $967.01 +28.61 |
| 2026-08-31 | -5.85 | $64.14 | OCUL×1, CRMD×1, RZLT×2, BMEA×9, NPWR×7, RRC×34, CRK×100, MOS×57, SLI×546, ACMR×17, GGB×320, MT×18, MU×1, ANF×1, BHVN×10, BZ×9, CAPR×20 | $11,368.03 | -248.28 | — | — | $64.14 | $11,384.62 | OCUL×1, CRMD×1, RZLT×2, BMEA×9, NPWR×7, RRC×34, CRK×100, MOS×57, SLI×546, ACMR×17, GGB×320, MT×18, MU×1, ANF×1, BHVN×10, BZ×9, CAPR×20 | 09:30 open · cash $64.14 (unchanged overnight, no fees) · equity $11,368.03 vs prior close $11,616.31 (-248.28) because holdings re-marked: OCUL×1 yday $10.82 → 09:30 $10.36 -0.46; CRMD×1 yday $8.31 → 09:30 $8.29 -0.02; RZLT×2 yday $4.98 → 09:30 $4.62 -0.72; BMEA×9 yday $1.68 → 09:30 $1.71 +0.27; NPWR×7 yday $1.89 → 09:30 $1.83 -0.42; RRC×34 yday $41.64 → 09:30 $41.11 -18.02; CRK×100 yday $14.62 → 09:30 $14.56 -6.00; MOS×57 yday $23.76 → 09:30 $23.75 -0.57; SLI×546 yday $2.64 → 09:30 $2.51 -70.98; ACMR×17 yday $80.49 → 09:30 $75.10 -91.63; GGB×320 yday $4.70 → 09:30 $4.55 -48.00; MT×18 yday $74.63 → 09:30 $75.07 +7.92; MU×1 yday $935.39 → 09:30 $933.01 -2.38; ANF×1 yday $145.75 → 09:30 $148.67 +2.92; BHVN×10 yday $16.12 → 09:30 $15.44 -6.80; BZ×9 yday $18.00 → 09:30 $17.89 -0.99; CAPR×20 yday $10.06 → 09:30 $9.44 -12.40 |
| 2026-09-01 | -6.30 | $64.14 | OCUL×1, CRMD×1, RZLT×2, BMEA×9, NPWR×7, RRC×34, CRK×100, MOS×57, SLI×546, ACMR×17, GGB×320, MT×18, MU×1, ANF×1, BHVN×10, BZ×9, CAPR×20 | $11,439.21 | +54.59 | — | OCUL, CRMD, RZLT, BMEA, NPWR | $118.86 | $11,481.68 | RRC×34, CRK×100, MOS×57, SLI×546, ACMR×17, GGB×320, MT×18, MU×1, ANF×1, BHVN×10, BZ×9, CAPR×20 | 09:30 open · cash $64.14 (unchanged overnight, no fees) · equity $11,439.21 vs prior close $11,384.62 (+54.59) because holdings re-marked: OCUL×1 yday $10.36 → 09:30 $10.49 +0.13; CRMD×1 yday $8.30 → 09:30 $8.26 -0.04; RZLT×2 yday $4.62 → 09:30 $4.69 +0.14; BMEA×9 yday $1.71 → 09:30 $1.65 -0.54; NPWR×7 yday $1.82 → 09:30 $1.78 -0.28; RRC×34 yday $41.78 → 09:30 $41.32 -15.64; CRK×100 yday $14.51 → 09:30 $14.31 -20.00; MOS×57 yday $23.78 → 09:30 $24.00 +12.54; SLI×546 yday $2.51 → 09:30 $2.70 +103.74; ACMR×17 yday $75.02 → 09:30 $71.24 -64.26; GGB×320 yday $4.55 → 09:30 $4.61 +19.20; MT×18 yday $75.06 → 09:30 $74.31 -13.50; MU×1 yday $933.01 → 09:30 $955.79 +22.78; ANF×1 yday $149.28 → 09:30 $142.47 -6.81; BHVN×10 yday $15.40 → 09:30 $15.45 +0.50; BZ×9 yday $17.90 → 09:30 $17.37 -4.77; CAPR×20 yday $9.36 → 09:30 $10.43 +21.40 |
| 2026-09-02 | -3.83 | $118.86 | RRC×34, CRK×100, MOS×57, SLI×546, ACMR×17, GGB×320, MT×18, MU×1, ANF×1, BHVN×10, BZ×9, CAPR×20 | $11,552.09 | +70.41 | — | — | $118.86 | $11,535.19 | RRC×34, CRK×100, MOS×57, SLI×546, ACMR×17, GGB×320, MT×18, MU×1, ANF×1, BHVN×10, BZ×9, CAPR×20 | 09:30 open · cash $118.86 (unchanged overnight, no fees) · equity $11,552.09 vs prior close $11,481.68 (+70.41) because holdings re-marked: RRC×34 yday $41.32 → 09:30 $41.94 +21.08; CRK×100 yday $14.90 → 09:30 $15.82 +92.00; MOS×57 yday $24.25 → 09:30 $23.94 -17.67; SLI×546 yday $2.70 → 09:30 $2.67 -16.38; ACMR×17 yday $71.88 → 09:30 $71.44 -7.48; GGB×320 yday $4.61 → 09:30 $4.57 -12.80; MT×18 yday $73.25 → 09:30 $73.22 -0.54; MU×1 yday $940.00 → 09:30 $941.12 +1.12; ANF×1 yday $143.00 → 09:30 $142.00 -1.00; BHVN×10 yday $15.45 → 09:30 $15.39 -0.60; BZ×9 yday $17.17 → 09:30 $17.29 +1.08; CAPR×20 yday $10.19 → 09:30 $10.77 +11.60 |
| 2026-09-03 | -0.90 | $118.86 | RRC×34, CRK×100, MOS×57, SLI×546, ACMR×17, GGB×320, MT×18, MU×1, ANF×1, BHVN×10, BZ×9, CAPR×20 | $11,545.65 | +10.46 | ATRC, HRMY, CABA, VSTM, RVTY, GPRO, FRVO | RRC, MOS, SLI, ACMR, GGB, MT, MU | $84.86 | $12,296.26 | CRK×100, ANF×1, BHVN×10, BZ×9, CAPR×20, ATRC×26, HRMY×32, CABA×406, VSTM×172, RVTY×10, GPRO×1088, FRVO×72 | 09:30 open · cash $118.86 (unchanged overnight, no fees) · equity $11,545.65 vs prior close $11,535.19 (+10.46) because holdings re-marked: RRC×34 yday $42.40 → 09:30 $42.10 -10.20; CRK×100 yday $16.02 → 09:30 $15.70 -32.00; MOS×57 yday $24.78 → 09:30 $24.70 -4.56; SLI×546 yday $2.49 → 09:30 $2.49 +0.00; ACMR×17 yday $70.04 → 09:30 $70.52 +8.16; GGB×320 yday $4.69 → 09:30 $4.81 +38.40; MT×18 yday $73.31 → 09:30 $73.86 +9.90; MU×1 yday $933.44 → 09:30 $930.83 -2.61; ANF×1 yday $140.68 → 09:30 $139.65 -1.03; BHVN×10 yday $15.74 → 09:30 $15.97 +2.30; BZ×9 yday $17.55 → 09:30 $17.65 +0.90; CAPR×20 yday $10.01 → 09:30 $10.07 +1.20 |
| 2026-09-04 | — | $84.86 | CRK×100, ANF×1, BHVN×10, BZ×9, CAPR×20, ATRC×26, HRMY×32, CABA×406, VSTM×172, RVTY×10, GPRO×1088, FRVO×72 | $12,459.01 | +162.75 | ASND, OSCR, NVAX, BVS, BAK | CRK, ANF, BHVN, BZ, CAPR | $213.05 | $11,818.45 | ATRC×26, HRMY×32, CABA×406, VSTM×172, RVTY×10, GPRO×1088, FRVO×72, ASND×1, OSCR×14, NVAX×43, BVS×31, BAK×232 | 09:30 open · cash $84.86 (unchanged overnight, no fees) · equity $12,459.01 vs prior close $12,296.26 (+162.75) because holdings re-marked: CRK×100 yday $15.54 → 09:30 $15.45 -9.00; ANF×1 yday $136.60 → 09:30 $137.70 +1.10; BHVN×10 yday $15.69 → 09:30 $15.89 +2.00; BZ×9 yday $17.30 → 09:30 $17.31 +0.09; CAPR×20 yday $9.89 → 09:30 $9.83 -1.20; ATRC×26 yday $52.59 → 09:30 $52.88 +7.54; HRMY×32 yday $42.86 → 09:30 $42.93 +2.24; CABA×406 yday $3.57 → 09:30 $3.63 +24.36; VSTM×172 yday $8.02 → 09:30 $8.03 +1.72; RVTY×10 yday $130.94 → 09:30 $132.45 +15.10; GPRO×1088 yday $1.69 → 09:30 $1.78 +97.92; FRVO×72 yday $17.98 → 09:30 $18.27 +20.88 |

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
| 2026-08-14 09:30 ET | **BUY** | `MARA` | 1 | $9.01 | $0.09 | — | $114.71 | — | S≥+5: sizeup + more names; list flatten; 🔵; ⚪; ret5=-13.5; leftover $10.32 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 11 | $0.94 | $0.14 | — | $104.27 | — | S≥+5: sizeup + more names; list flatten; 🔵; ⚪; ret5=+0.5; leftover $10.32 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 6 | $1.50 | $0.11 | — | $95.16 | — | S≥+5: sizeup + more names; list flatten; 🔵; ⚪; ret5=+9.2; leftover $10.32 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 2 | $4.31 | $0.09 | — | $86.45 | — | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $10.32 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `HYLN` | 2 | $4.18 | $0.09 | — | $78.00 | — | S≥+5: sizeup + more names; list probable,yday_gainer; 🔵; ⚪; ret5=+4.1; leftover $10.32 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $78.00 | ▼ 09:30 equity $10,410.48 vs yday $10,434.08 (-23.60) | 09:30 open · cash $78.00 (unchanged overnight, no fees) · equity $10,410.48 vs prior close $10,434.08 (-23.60) because holdings re-marked: BTSG×18 yday $61.71 → 09:30 $61.69 -0.36; IREN×24 yday $44.06 → 09:30 $45.23 +28.08; TPG×21 yday $53.03 → 09:30 $52.67 -7.56; TGTX×22 yday $48.74 → 09:30 $48.74 +0.00; SLS×94 yday $12.78 → 09:30 $12.78 +0.00; HIMS×37 yday $28.15 → 09:30 $28.14 -0.37; INO×1371 yday $1.09 → 09:30 $1.07 -27.42; TNDM×47 yday $22.72 → 09:30 $22.50 -10.34; VOR×50 yday $23.03 → 09:30 $22.91 -6.00; MARA×1 yday $9.20 → 09:30 $9.22 +0.02; LDI×11 yday $0.90 → 09:30 $0.91 +0.11; BTBT×6 yday $1.57 → 09:30 $1.52 -0.30; ANGX×2 yday $4.37 → 09:30 $4.60 +0.46; HYLN×2 yday $4.06 → 09:30 $4.10 +0.08 | — |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 2 | $4.05 | $0.09 | — | $69.81 | — | S≥+5: sizeup + more names; list flatten; 🔵; ⚪; ret5=-12.3; leftover $9.75 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `TGB` | 1 | $8.46 | $0.09 | — | $61.27 | — | S≥+5: sizeup + more names; list flatten; 🔵; ⚪; ret5=+0.4; leftover $9.75 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 3 | $3.24 | $0.11 | — | $51.44 | — | S≥+5: sizeup + more names; list flatten; ⚪; ret5=+0.3; leftover $9.75 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `HNST` | 2 | $4.81 | $0.10 | — | $41.72 | — | S≥+5: sizeup + more names; list flatten; ⚪; ret5=-11.4; leftover $9.75 | join🟡 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $41.72 | ▼ 09:30 equity $10,384.75 vs yday $10,513.02 (-128.27) | 09:30 open · cash $41.72 (unchanged overnight, no fees) · equity $10,384.75 vs prior close $10,513.02 (-128.27) because holdings re-marked: BTSG×18 yday $60.38 → 09:30 $60.00 -6.84; IREN×24 yday $44.90 → 09:30 $43.56 -32.16; TPG×21 yday $51.77 → 09:30 $51.77 +0.00; TGTX×22 yday $49.28 → 09:30 $49.28 +0.00; SLS×94 yday $13.00 → 09:30 $12.66 -31.96; HIMS×37 yday $28.61 → 09:30 $27.85 -28.12; INO×1371 yday $1.15 → 09:30 $1.14 -13.71; TNDM×47 yday $22.25 → 09:30 $22.16 -4.46; VOR×50 yday $23.01 → 09:30 $22.82 -9.50; MARA×1 yday $9.72 → 09:30 $9.36 -0.36; LDI×11 yday $0.88 → 09:30 $0.87 -0.06; BTBT×6 yday $1.60 → 09:30 $1.54 -0.36; ANGX×2 yday $4.71 → 09:30 $4.79 +0.16; HYLN×2 yday $4.09 → 09:30 $3.95 -0.28; TMC×2 yday $3.77 → 09:30 $3.72 -0.10; TGB×1 yday $8.77 → 09:30 $8.55 -0.22; DNN×3 yday $3.19 → 09:30 $3.11 -0.24; HNST×2 yday $4.70 → 09:30 $4.67 -0.06 | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $41.72 | ▲ 09:30 equity $10,728.75 vs yday $10,567.86 (+160.89) | 09:30 open · cash $41.72 (unchanged overnight, no fees) · equity $10,728.75 vs prior close $10,567.86 (+160.89) because holdings re-marked: BTSG×18 yday $59.50 → 09:30 $60.15 +11.70; IREN×24 yday $42.00 → 09:30 $41.41 -14.04; TPG×21 yday $52.02 → 09:30 $52.26 +5.04; TGTX×22 yday $50.26 → 09:30 $51.62 +29.92; SLS×94 yday $13.10 → 09:30 $13.46 +33.84; HIMS×37 yday $27.39 → 09:30 $27.55 +5.92; INO×1371 yday $1.20 → 09:30 $1.22 +27.42; TNDM×47 yday $23.73 → 09:30 $24.20 +22.09; VOR×50 yday $23.28 → 09:30 $24.05 +38.50; MARA×1 yday $8.96 → 09:30 $8.91 -0.05; LDI×11 yday $0.86 → 09:30 $0.88 +0.24; BTBT×6 yday $1.45 → 09:30 $1.42 -0.18; ANGX×2 yday $4.85 → 09:30 $4.79 -0.12; HYLN×2 yday $3.86 → 09:30 $3.87 +0.02; TMC×2 yday $3.92 → 09:30 $3.93 +0.02; TGB×1 yday $8.36 → 09:30 $8.70 +0.34; DNN×3 yday $3.15 → 09:30 $3.19 +0.12; HNST×2 yday $4.75 → 09:30 $4.80 +0.10 | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $41.72 | ▼ 09:30 equity $10,903.82 vs yday $10,988.26 (-84.44) | 09:30 open · cash $41.72 (unchanged overnight, no fees) · equity $10,903.82 vs prior close $10,988.26 (-84.44) because holdings re-marked: BTSG×18 yday $59.33 → 09:30 $58.64 -12.42; IREN×24 yday $42.84 → 09:30 $42.46 -9.12; TPG×21 yday $53.18 → 09:30 $53.06 -2.52; TGTX×22 yday $51.69 → 09:30 $51.65 -0.88; SLS×94 yday $13.85 → 09:30 $13.84 -0.94; HIMS×37 yday $31.09 → 09:30 $30.66 -15.91; INO×1371 yday $1.30 → 09:30 $1.30 +0.00; TNDM×47 yday $23.46 → 09:30 $23.11 -16.45; VOR×50 yday $23.58 → 09:30 $23.05 -26.50; MARA×1 yday $9.65 → 09:30 $10.21 +0.56; LDI×11 yday $0.88 → 09:30 $0.87 -0.06; BTBT×6 yday $1.40 → 09:30 $1.46 +0.33; ANGX×2 yday $4.60 → 09:30 $4.57 -0.06; HYLN×2 yday $3.67 → 09:30 $3.61 -0.12; TMC×2 yday $3.97 → 09:30 $3.92 -0.10; TGB×1 yday $8.47 → 09:30 $8.35 -0.12; DNN×3 yday $3.22 → 09:30 $3.20 -0.06; HNST×2 yday $5.02 → 09:30 $4.98 -0.08 | — |
| 2026-08-20 09:30 ET | **SELL** | `BTSG` | 18 | $58.64 | $2.06 | $-24.99 | $1,095.17 | ▼ -24.99 after sell → book $10,901.75; vs 09:30 mark -2.07 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `IREN` | 24 | $42.46 | $2.08 | $-88.62 | $2,112.13 | ▼ -88.62 after sell → book $10,899.67; vs 09:30 mark -2.08 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `TPG` | 21 | $53.06 | $2.07 | $+47.05 | $3,224.32 | ▲ +47.05 after sell → book $10,897.60; vs 09:30 mark -2.07 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `TGTX` | 22 | $51.65 | $2.08 | $+38.77 | $4,358.54 | ▲ +38.77 after sell → book $10,895.52; vs 09:30 mark -2.08 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `SLS` | 94 | $13.84 | $2.30 | $+196.59 | $5,657.20 | ▲ +196.59 after sell → book $10,893.22; vs 09:30 mark -2.30 | dropped from list after 5 sess (min 5) | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **SELL** | `HIMS` | 37 | $30.66 | $2.12 | $+29.82 | $6,789.50 | ▲ +29.82 after sell → book $10,891.10; vs 09:30 mark -2.12 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `INO` | 1371 | $1.30 | $17.93 | $+638.64 | $8,553.88 | ▲ +638.64 after sell → book $10,873.18; vs 09:30 mark -17.92 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `TNDM` | 47 | $23.11 | $2.15 | $-14.62 | $9,637.89 | ▼ -14.62 after sell → book $10,871.02; vs 09:30 mark -2.16 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `VOR` | 50 | $23.05 | $2.16 | $+47.70 | $10,788.23 | ▲ +47.70 after sell → book $10,868.86; vs 09:30 mark -2.16 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 65 | $20.55 | $2.19 | — | $9,450.30 | — | S≥+5: sizeup + more names; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1348.53 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 14 | $91.01 | $2.03 | — | $8,174.13 | — | S≥+5: sizeup + more names; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1348.53 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 65 | $20.65 | $2.19 | — | $6,829.69 | — | S≥+5: sizeup + more names; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1348.53 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 233 | $5.77 | $3.01 | — | $5,482.28 | — | S≥+5: sizeup + more names; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1348.53 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 68 | $19.63 | $2.19 | — | $4,145.24 | — | S≥+5: sizeup + more names; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1348.53 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 45 | $29.63 | $2.12 | — | $2,809.77 | — | S≥+5: sizeup + more names; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1348.53 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 770 | $1.75 | $9.93 | — | $1,452.33 | — | S≥+5: sizeup + more names; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1348.53 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 9 | $144.54 | $2.02 | — | $149.46 | — | S≥+5: sizeup + more names; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1348.53 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $149.46 | ▲ 09:30 equity $11,389.43 vs yday $11,097.43 (+292.00) | 09:30 open · cash $149.46 (unchanged overnight, no fees) · equity $11,389.43 vs prior close $11,097.43 (+292.00) because holdings re-marked: MARA×1 yday $11.15 → 09:30 $11.70 +0.55; LDI×11 yday $0.87 → 09:30 $0.87 -0.03; BTBT×6 yday $1.59 → 09:30 $1.66 +0.39; ANGX×2 yday $4.37 → 09:30 $4.43 +0.12; HYLN×2 yday $3.37 → 09:30 $3.42 +0.10; TMC×2 yday $3.97 → 09:30 $4.10 +0.26; TGB×1 yday $8.69 → 09:30 $9.00 +0.31; DNN×3 yday $3.14 → 09:30 $3.23 +0.27; HNST×2 yday $4.96 → 09:30 $4.97 +0.02; AG×65 yday $21.19 → 09:30 $21.90 +46.15; BHP×14 yday $93.63 → 09:30 $95.72 +29.26; CDE×65 yday $21.11 → 09:30 $21.75 +41.60; HDSN×233 yday $5.57 → 09:30 $5.67 +23.30; IAG×68 yday $20.50 → 09:30 $21.17 +45.56; KGC×45 yday $31.43 → 09:30 $32.17 +33.30; NFGC×770 yday $1.75 → 09:30 $1.79 +30.80; WPM×9 yday $150.25 → 09:30 $154.70 +40.05 | — |
| 2026-08-21 09:30 ET | **SELL** | `MARA` | 1 | $11.70 | $0.14 | $+2.46 | $161.02 | ▲ +2.46 after sell → book $11,389.29; vs 09:30 mark -0.14 | dropped from list after 5 sess (min 5) | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `LDI` | 11 | $0.87 | $0.15 | $-1.05 | $170.41 | ▼ -1.05 after sell → book $11,389.15; vs 09:30 mark -0.14 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `BTBT` | 6 | $1.66 | $0.14 | $+0.71 | $180.23 | ▲ +0.71 after sell → book $11,389.01; vs 09:30 mark -0.14 | dropped from list after 5 sess (min 5) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `ANGX` | 2 | $4.43 | $0.11 | $+0.03 | $188.97 | ▲ +0.03 after sell → book $11,388.89; vs 09:30 mark -0.12 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `HYLN` | 2 | $3.42 | $0.09 | $-1.70 | $195.72 | ▼ -1.70 after sell → book $11,388.80; vs 09:30 mark -0.09 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 1 | $17.20 | $0.17 | — | $178.34 | — | S≥+5: sizeup + more names; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $24.46 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 2 | $11.13 | $0.23 | — | $155.86 | — | S≥+5: sizeup + more names; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $24.46 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 9 | $2.47 | $0.25 | — | $133.38 | — | S≥+5: sizeup + more names; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $24.46 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 12 | $1.93 | $0.27 | — | $109.95 | — | S≥+5: sizeup + more names; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $24.46 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 18 | $1.32 | $0.29 | — | $85.90 | — | S≥+5: sizeup + more names; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $24.46 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $85.90 | ▲ 09:30 equity $11,525.83 vs yday $11,391.04 (+134.79) | 09:30 open · cash $85.90 (unchanged overnight, no fees) · equity $11,525.83 vs prior close $11,391.04 (+134.79) because holdings re-marked: TMC×2 yday $4.79 → 09:30 $4.57 -0.44; TGB×1 yday $9.19 → 09:30 $9.26 +0.07; DNN×3 yday $3.50 → 09:30 $3.50 +0.00; HNST×2 yday $5.05 → 09:30 $5.05 +0.00; AG×65 yday $21.09 → 09:30 $21.47 +24.70; BHP×14 yday $97.03 → 09:30 $97.34 +4.34; CDE×65 yday $20.97 → 09:30 $21.26 +18.85; HDSN×233 yday $5.63 → 09:30 $5.69 +13.98; IAG×68 yday $21.14 → 09:30 $21.44 +20.40; KGC×45 yday $32.76 → 09:30 $33.21 +20.25; NFGC×770 yday $1.84 → 09:30 $1.86 +15.40; WPM×9 yday $157.78 → 09:30 $158.96 +10.62; AUPH×1 yday $16.65 → 09:30 $16.60 -0.05; ARCT×2 yday $13.45 → 09:30 $13.26 -0.38; AUTL×9 yday $2.41 → 09:30 $2.36 -0.45; CRDL×12 yday $1.86 → 09:30 $1.87 +0.12; CYPH×18 yday $1.42 → 09:30 $1.83 +7.38 | — |
| 2026-08-24 09:30 ET | **SELL** | `TMC` | 2 | $4.57 | $0.12 | $+0.84 | $94.92 | ▲ +0.84 after sell → book $11,525.71; vs 09:30 mark -0.12 | dropped from list after 5 sess (min 5) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `TGB` | 1 | $9.26 | $0.12 | $+0.60 | $104.06 | ▲ +0.60 after sell → book $11,525.59; vs 09:30 mark -0.12 | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `DNN` | 3 | $3.50 | $0.13 | $+0.54 | $114.43 | ▲ +0.54 after sell → book $11,525.46; vs 09:30 mark -0.13 | dropped from list after 5 sess (min 5) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 judge🟡 ab🟡 peer🟢 vol🟢 buy🟢 |
| 2026-08-24 09:30 ET | **SELL** | `HNST` | 2 | $5.05 | $0.13 | $+0.25 | $124.40 | ▲ +0.25 after sell → book $11,525.33; vs 09:30 mark -0.13 | dropped from list after 5 sess (min 5) | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $124.40 | ▲ 09:30 equity $11,434.30 vs yday $11,359.64 (+74.66) | 09:30 open · cash $124.40 (unchanged overnight, no fees) · equity $11,434.30 vs prior close $11,359.64 (+74.66) because holdings re-marked: AG×65 yday $20.57 → 09:30 $20.73 +10.40; BHP×14 yday $96.66 → 09:30 $95.95 -9.94; CDE×65 yday $20.49 → 09:30 $20.85 +23.40; HDSN×233 yday $5.57 → 09:30 $5.53 -9.32; IAG×68 yday $21.36 → 09:30 $21.63 +18.36; KGC×45 yday $32.47 → 09:30 $32.76 +13.05; NFGC×770 yday $1.90 → 09:30 $1.91 +7.70; WPM×9 yday $158.00 → 09:30 $160.00 +18.00; AUPH×1 yday $16.60 → 09:30 $16.71 +0.11; ARCT×2 yday $13.76 → 09:30 $14.34 +1.16; AUTL×9 yday $2.38 → 09:30 $2.32 -0.54; CRDL×12 yday $1.80 → 09:30 $1.90 +1.20; CYPH×18 yday $1.64 → 09:30 $1.70 +1.08 | — |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 1 | $10.92 | $0.11 | — | $113.37 | — | S≥+5: sizeup + more names; list flatten; 🔵; ret5=+10.4; leftover $15.55 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 1 | $8.28 | $0.09 | — | $105.01 | — | S≥+5: sizeup + more names; list flatten; 🔵; ⚪; ret5=+8.8; leftover $15.55 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 2 | $5.23 | $0.11 | — | $94.43 | — | S≥+5: sizeup + more names; list flatten; ret5=+10.7; leftover $15.55 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 9 | $1.62 | $0.17 | — | $79.68 | — | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.8; leftover $15.55 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `NPWR` | 7 | $2.00 | $0.16 | — | $65.52 | — | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover; 🔵; ret5=-12.8; leftover $15.55 | join🟡 sector🟢 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $65.52 | ▲ 09:30 equity $11,369.30 vs yday $11,369.30 (+0.00) | 09:30 open · cash $65.52 (unchanged overnight, no fees) · equity $11,369.30 vs prior close $11,369.30 (+0.00) because holdings re-marked: AG×65 yday $20.68 → 09:30 $20.68 +0.00; BHP×14 yday $96.05 → 09:30 $96.05 +0.00; CDE×65 yday $20.71 → 09:30 $20.71 +0.00; HDSN×233 yday $5.49 → 09:30 $5.49 +0.00; IAG×68 yday $21.48 → 09:30 $21.48 +0.00; KGC×45 yday $32.55 → 09:30 $32.55 +0.00; NFGC×770 yday $1.90 → 09:30 $1.90 +0.00; WPM×9 yday $158.25 → 09:30 $158.25 +0.00; AUPH×1 yday $16.71 → 09:30 $16.71 +0.00; ARCT×2 yday $14.21 → 09:30 $14.21 +0.00; AUTL×9 yday $2.34 → 09:30 $2.34 +0.00; CRDL×12 yday $1.90 → 09:30 $1.90 +0.00; CYPH×18 yday $1.64 → 09:30 $1.64 +0.00; OCUL×1 yday $10.92 → 09:30 $10.92 +0.00; CRMD×1 yday $8.28 → 09:30 $8.28 +0.00; RZLT×2 yday $5.29 → 09:30 $5.29 +0.00; BMEA×9 yday $1.61 → 09:30 $1.61 +0.00; NPWR×7 yday $2.02 → 09:30 $2.02 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $65.52 | ▲ 09:30 equity $11,534.37 vs yday $11,433.66 (+100.71) | 09:30 open · cash $65.52 (unchanged overnight, no fees) · equity $11,534.37 vs prior close $11,433.66 (+100.71) because holdings re-marked: AG×65 yday $20.68 → 09:30 $20.63 -3.25; BHP×14 yday $96.05 → 09:30 $96.99 +13.16; CDE×65 yday $20.71 → 09:30 $21.00 +18.85; HDSN×233 yday $5.49 → 09:30 $5.51 +4.66; IAG×68 yday $21.48 → 09:30 $21.64 +10.88; KGC×45 yday $32.55 → 09:30 $32.90 +15.75; NFGC×770 yday $1.90 → 09:30 $2.00 +77.00; WPM×9 yday $158.25 → 09:30 $160.93 +24.12; AUPH×1 yday $16.71 → 09:30 $16.60 -0.11; ARCT×2 yday $14.21 → 09:30 $15.35 +2.28; AUTL×9 yday $2.34 → 09:30 $2.41 +0.63; CRDL×12 yday $1.90 → 09:30 $2.03 +1.56; CYPH×18 yday $1.64 → 09:30 $1.60 -0.72; OCUL×1 yday $10.92 → 09:30 $10.79 -0.13; CRMD×1 yday $8.28 → 09:30 $8.60 +0.32; RZLT×2 yday $5.29 → 09:30 $5.01 -0.56; BMEA×9 yday $1.61 → 09:30 $1.75 +1.26; NPWR×7 yday $2.02 → 09:30 $1.93 -0.63 | — |
| 2026-08-27 09:30 ET | **SELL** | `AG` | 65 | $20.63 | $2.21 | $+0.81 | $1,404.26 | ▲ +0.81 after sell → book $11,532.16; vs 09:30 mark -2.21 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `BHP` | 14 | $96.99 | $2.05 | $+79.64 | $2,760.07 | ▲ +79.64 after sell → book $11,530.11; vs 09:30 mark -2.05 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `CDE` | 65 | $21.00 | $2.21 | $+18.36 | $4,122.86 | ▲ +18.36 after sell → book $11,527.90; vs 09:30 mark -2.21 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `HDSN` | 233 | $5.51 | $3.05 | $-66.64 | $5,403.64 | ▼ -66.64 after sell → book $11,524.85; vs 09:30 mark -3.05 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `IAG` | 68 | $21.64 | $2.22 | $+132.27 | $6,872.94 | ▲ +132.27 after sell → book $11,522.63; vs 09:30 mark -2.22 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `KGC` | 45 | $32.90 | $2.15 | $+142.88 | $8,351.30 | ▲ +142.88 after sell → book $11,520.49; vs 09:30 mark -2.14 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `NFGC` | 770 | $2.00 | $10.07 | $+172.49 | $9,881.22 | ▲ +172.49 after sell → book $11,510.41; vs 09:30 mark -10.08 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `WPM` | 9 | $160.93 | $2.04 | $+143.45 | $11,327.55 | ▲ +143.45 after sell → book $11,508.37; vs 09:30 mark -2.04 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 34 | $40.72 | $2.09 | — | $9,940.98 | — | S≥+5: sizeup + more names; list flatten; ret5=+1.8; leftover $1415.94 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 100 | $14.09 | $2.29 | — | $8,529.69 | — | S≥+5: sizeup + more names; list flatten; ret5=+1.1; leftover $1415.94 | join🟢 sector🔴 gen🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MOS` | 57 | $24.84 | $2.16 | — | $7,111.65 | — | S≥+5: sizeup + more names; list flatten; ret5=+13.0; leftover $1415.94 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟢 |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 546 | $2.59 | $7.04 | — | $5,690.47 | — | S≥+5: sizeup + more names; list flatten; ret5=+4.2; leftover $1415.94 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `ACMR` | 17 | $80.97 | $2.04 | — | $4,311.94 | — | S≥+5: sizeup + more names; list mover_buy; 🔵; ret5=-1.3; leftover $1415.94 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `GGB` | 320 | $4.42 | $4.13 | — | $2,893.41 | — | S≥+5: sizeup + more names; list mover_buy; 🔵; ret5=-8.6; leftover $1415.94 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MT` | 18 | $75.12 | $2.04 | — | $1,539.21 | — | S≥+5: sizeup + more names; list mover_buy; 🔵; ret5=-2.2; leftover $1415.94 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MU` | 1 | $925.74 | $1.99 | — | $611.47 | — | S≥+5: sizeup + more names; list mover_buy; 🔵; ret5=-0.5; leftover $1415.94 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $611.47 | ▲ 09:30 equity $11,592.35 vs yday $11,509.85 (+82.50) | 09:30 open · cash $611.47 (unchanged overnight, no fees) · equity $11,592.35 vs prior close $11,509.85 (+82.50) because holdings re-marked: AUPH×1 yday $16.54 → 09:30 $16.47 -0.07; ARCT×2 yday $15.83 → 09:30 $15.74 -0.18; AUTL×9 yday $2.33 → 09:30 $2.32 -0.09; CRDL×12 yday $2.14 → 09:30 $2.09 -0.60; CYPH×18 yday $1.63 → 09:30 $1.75 +2.16; OCUL×1 yday $10.77 → 09:30 $10.63 -0.14; CRMD×1 yday $8.39 → 09:30 $8.49 +0.10; RZLT×2 yday $5.04 → 09:30 $5.07 +0.06; BMEA×9 yday $1.71 → 09:30 $1.74 +0.27; NPWR×7 yday $1.81 → 09:30 $1.83 +0.14; RRC×34 yday $41.55 → 09:30 $41.44 -3.74; CRK×100 yday $14.50 → 09:30 $14.42 -8.00; MOS×57 yday $24.16 → 09:30 $24.00 -9.12; SLI×546 yday $2.61 → 09:30 $2.60 -5.46; ACMR×17 yday $79.11 → 09:30 $81.65 +43.18; GGB×320 yday $4.46 → 09:30 $4.57 +35.20; MT×18 yday $74.53 → 09:30 $74.54 +0.18; MU×1 yday $938.40 → 09:30 $967.01 +28.61 | — |
| 2026-08-28 09:30 ET | **SELL** | `AUPH` | 1 | $16.47 | $0.19 | $-1.09 | $627.75 | ▼ -1.09 after sell → book $11,592.16; vs 09:30 mark -0.19 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `ARCT` | 2 | $15.74 | $0.34 | $+8.65 | $658.89 | ▲ +8.65 after sell → book $11,591.82; vs 09:30 mark -0.34 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `AUTL` | 9 | $2.32 | $0.26 | $-1.86 | $679.52 | ▼ -1.86 after sell → book $11,591.57; vs 09:30 mark -0.25 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `CRDL` | 12 | $2.09 | $0.31 | $+1.35 | $704.29 | ▲ +1.35 after sell → book $11,591.26; vs 09:30 mark -0.31 | dropped from list after 5 sess (min 5) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `CYPH` | 18 | $1.75 | $0.39 | $+7.06 | $735.40 | ▲ +7.06 after sell → book $11,590.87; vs 09:30 mark -0.39 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 1 | $144.70 | $1.45 | — | $589.25 | — | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover; 🔵; ret5=+0.8; leftover $183.85 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BHVN` | 10 | $16.95 | $1.73 | — | $418.03 | — | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover; ret5=-5.4; leftover $183.85 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BZ` | 9 | $18.50 | $1.69 | — | $249.84 | — | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover; ret5=+2.8; leftover $183.85 | join🟢 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `CAPR` | 20 | $9.19 | $1.90 | — | $64.14 | — | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover; ret5=-5.4; leftover $183.85 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $64.14 | ▼ 09:30 equity $11,368.03 vs yday $11,616.31 (-248.28) | 09:30 open · cash $64.14 (unchanged overnight, no fees) · equity $11,368.03 vs prior close $11,616.31 (-248.28) because holdings re-marked: OCUL×1 yday $10.82 → 09:30 $10.36 -0.46; CRMD×1 yday $8.31 → 09:30 $8.29 -0.02; RZLT×2 yday $4.98 → 09:30 $4.62 -0.72; BMEA×9 yday $1.68 → 09:30 $1.71 +0.27; NPWR×7 yday $1.89 → 09:30 $1.83 -0.42; RRC×34 yday $41.64 → 09:30 $41.11 -18.02; CRK×100 yday $14.62 → 09:30 $14.56 -6.00; MOS×57 yday $23.76 → 09:30 $23.75 -0.57; SLI×546 yday $2.64 → 09:30 $2.51 -70.98; ACMR×17 yday $80.49 → 09:30 $75.10 -91.63; GGB×320 yday $4.70 → 09:30 $4.55 -48.00; MT×18 yday $74.63 → 09:30 $75.07 +7.92; MU×1 yday $935.39 → 09:30 $933.01 -2.38; ANF×1 yday $145.75 → 09:30 $148.67 +2.92; BHVN×10 yday $16.12 → 09:30 $15.44 -6.80; BZ×9 yday $18.00 → 09:30 $17.89 -0.99; CAPR×20 yday $10.06 → 09:30 $9.44 -12.40 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $64.14 | ▲ 09:30 equity $11,439.21 vs yday $11,384.62 (+54.59) | 09:30 open · cash $64.14 (unchanged overnight, no fees) · equity $11,439.21 vs prior close $11,384.62 (+54.59) because holdings re-marked: OCUL×1 yday $10.36 → 09:30 $10.49 +0.13; CRMD×1 yday $8.30 → 09:30 $8.26 -0.04; RZLT×2 yday $4.62 → 09:30 $4.69 +0.14; BMEA×9 yday $1.71 → 09:30 $1.65 -0.54; NPWR×7 yday $1.82 → 09:30 $1.78 -0.28; RRC×34 yday $41.78 → 09:30 $41.32 -15.64; CRK×100 yday $14.51 → 09:30 $14.31 -20.00; MOS×57 yday $23.78 → 09:30 $24.00 +12.54; SLI×546 yday $2.51 → 09:30 $2.70 +103.74; ACMR×17 yday $75.02 → 09:30 $71.24 -64.26; GGB×320 yday $4.55 → 09:30 $4.61 +19.20; MT×18 yday $75.06 → 09:30 $74.31 -13.50; MU×1 yday $933.01 → 09:30 $955.79 +22.78; ANF×1 yday $149.28 → 09:30 $142.47 -6.81; BHVN×10 yday $15.40 → 09:30 $15.45 +0.50; BZ×9 yday $17.90 → 09:30 $17.37 -4.77; CAPR×20 yday $9.36 → 09:30 $10.43 +21.40 | — |
| 2026-09-01 09:30 ET | **SELL** | `OCUL` | 1 | $10.49 | $0.13 | $-0.67 | $74.50 | ▼ -0.67 after sell → book $11,439.08; vs 09:30 mark -0.13 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `CRMD` | 1 | $8.26 | $0.11 | $-0.21 | $82.65 | ▼ -0.21 after sell → book $11,438.97; vs 09:30 mark -0.11 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `RZLT` | 2 | $4.69 | $0.12 | $-1.31 | $91.91 | ▼ -1.31 after sell → book $11,438.85; vs 09:30 mark -0.12 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `BMEA` | 9 | $1.65 | $0.20 | $-0.10 | $106.57 | ▼ -0.10 after sell → book $11,438.66; vs 09:30 mark -0.19 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `NPWR` | 7 | $1.78 | $0.17 | $-1.87 | $118.86 | ▼ -1.87 after sell → book $11,438.49; vs 09:30 mark -0.17 | dropped from list after 5 sess (min 5) | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $118.86 | ▲ 09:30 equity $11,552.09 vs yday $11,481.68 (+70.41) | 09:30 open · cash $118.86 (unchanged overnight, no fees) · equity $11,552.09 vs prior close $11,481.68 (+70.41) because holdings re-marked: RRC×34 yday $41.32 → 09:30 $41.94 +21.08; CRK×100 yday $14.90 → 09:30 $15.82 +92.00; MOS×57 yday $24.25 → 09:30 $23.94 -17.67; SLI×546 yday $2.70 → 09:30 $2.67 -16.38; ACMR×17 yday $71.88 → 09:30 $71.44 -7.48; GGB×320 yday $4.61 → 09:30 $4.57 -12.80; MT×18 yday $73.25 → 09:30 $73.22 -0.54; MU×1 yday $940.00 → 09:30 $941.12 +1.12; ANF×1 yday $143.00 → 09:30 $142.00 -1.00; BHVN×10 yday $15.45 → 09:30 $15.39 -0.60; BZ×9 yday $17.17 → 09:30 $17.29 +1.08; CAPR×20 yday $10.19 → 09:30 $10.77 +11.60 | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $118.86 | ▲ 09:30 equity $11,545.65 vs yday $11,535.19 (+10.46) | 09:30 open · cash $118.86 (unchanged overnight, no fees) · equity $11,545.65 vs prior close $11,535.19 (+10.46) because holdings re-marked: RRC×34 yday $42.40 → 09:30 $42.10 -10.20; CRK×100 yday $16.02 → 09:30 $15.70 -32.00; MOS×57 yday $24.78 → 09:30 $24.70 -4.56; SLI×546 yday $2.49 → 09:30 $2.49 +0.00; ACMR×17 yday $70.04 → 09:30 $70.52 +8.16; GGB×320 yday $4.69 → 09:30 $4.81 +38.40; MT×18 yday $73.31 → 09:30 $73.86 +9.90; MU×1 yday $933.44 → 09:30 $930.83 -2.61; ANF×1 yday $140.68 → 09:30 $139.65 -1.03; BHVN×10 yday $15.74 → 09:30 $15.97 +2.30; BZ×9 yday $17.55 → 09:30 $17.65 +0.90; CAPR×20 yday $10.01 → 09:30 $10.07 +1.20 | — |
| 2026-09-03 09:30 ET | **SELL** | `RRC` | 34 | $42.10 | $2.11 | $+42.71 | $1,548.15 | ▲ +42.71 after sell → book $11,543.54; vs 09:30 mark -2.11 | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `MOS` | 57 | $24.70 | $2.18 | $-12.32 | $2,953.87 | ▼ -12.32 after sell → book $11,541.36; vs 09:30 mark -2.18 | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `SLI` | 546 | $2.49 | $7.14 | $-68.79 | $4,306.26 | ▼ -68.79 after sell → book $11,534.21; vs 09:30 mark -7.15 | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `ACMR` | 17 | $70.52 | $2.06 | $-181.75 | $5,503.04 | ▼ -181.75 after sell → book $11,532.15; vs 09:30 mark -2.06 | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `GGB` | 320 | $4.81 | $4.19 | $+116.48 | $7,038.05 | ▲ +116.48 after sell → book $11,527.96; vs 09:30 mark -4.19 | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `MT` | 18 | $73.86 | $2.06 | $-26.79 | $8,365.46 | ▼ -26.79 after sell → book $11,525.89; vs 09:30 mark -2.07 | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `MU` | 1 | $930.83 | $2.01 | $+1.08 | $9,294.28 | ▲ +1.08 after sell → book $11,523.88; vs 09:30 mark -2.01 | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 26 | $49.76 | $2.07 | — | $7,998.45 | — | S≥+5: sizeup + more names; list flatten; 🔵; ⚪; ret5=+10.6; leftover $1327.75 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 32 | $41.31 | $2.09 | — | $6,674.45 | — | S≥+5: sizeup + more names; list flatten,mover_buy; 🔵; ⚪; ret5=+0.1; leftover $1327.75 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 406 | $3.27 | $5.24 | — | $5,341.59 | — | S≥+5: sizeup + more names; list flatten; 🔵; ⚪; ret5=+13.8; leftover $1327.75 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 172 | $7.70 | $2.51 | — | $4,014.68 | — | S≥+5: sizeup + more names; list flatten,mover_buy; 🔵; ⚪; ret5=+4.7; leftover $1327.75 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 10 | $125.94 | $2.02 | — | $2,753.26 | — | S≥+5: sizeup + more names; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $1327.75 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `GPRO` | 1088 | $1.22 | $14.04 | — | $1,411.87 | — | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover; ret5=+5.9; leftover $1327.75 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FRVO` | 72 | $18.40 | $2.21 | — | $84.86 | — | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover; ret5=-14.4; leftover $1327.75 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $84.86 | ▲ 09:30 equity $12,459.01 vs yday $12,296.26 (+162.75) | 09:30 open · cash $84.86 (unchanged overnight, no fees) · equity $12,459.01 vs prior close $12,296.26 (+162.75) because holdings re-marked: CRK×100 yday $15.54 → 09:30 $15.45 -9.00; ANF×1 yday $136.60 → 09:30 $137.70 +1.10; BHVN×10 yday $15.69 → 09:30 $15.89 +2.00; BZ×9 yday $17.30 → 09:30 $17.31 +0.09; CAPR×20 yday $9.89 → 09:30 $9.83 -1.20; ATRC×26 yday $52.59 → 09:30 $52.88 +7.54; HRMY×32 yday $42.86 → 09:30 $42.93 +2.24; CABA×406 yday $3.57 → 09:30 $3.63 +24.36; VSTM×172 yday $8.02 → 09:30 $8.03 +1.72; RVTY×10 yday $130.94 → 09:30 $132.45 +15.10; GPRO×1088 yday $1.69 → 09:30 $1.78 +97.92; FRVO×72 yday $17.98 → 09:30 $18.27 +20.88 | — |
| 2026-09-04 09:30 ET | **SELL** | `CRK` | 100 | $15.45 | $2.32 | $+131.39 | $1,627.54 | ▲ +131.39 after sell → book $12,456.69; vs 09:30 mark -2.32 | dropped from list after 6 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `ANF` | 1 | $137.70 | $1.40 | $-9.85 | $1,763.84 | ▼ -9.85 after sell → book $12,455.29; vs 09:30 mark -1.40 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `BHVN` | 10 | $15.89 | $1.64 | $-13.96 | $1,921.10 | ▼ -13.96 after sell → book $12,453.65; vs 09:30 mark -1.64 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `BZ` | 9 | $17.31 | $1.60 | $-14.01 | $2,075.29 | ▼ -14.01 after sell → book $12,452.05; vs 09:30 mark -1.60 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `CAPR` | 20 | $9.83 | $2.05 | $+8.86 | $2,269.84 | ▲ +8.86 after sell → book $12,450.00; vs 09:30 mark -2.05 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **BUY** | `ASND` | 1 | $266.94 | $1.99 | — | $2,000.91 | — | S≥+5: sizeup + more names; list flatten; ret5=+1.9; leftover $453.97 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OSCR` | 14 | $30.65 | $2.03 | — | $1,569.78 | — | S≥+5: sizeup + more names; list flatten; 🔵; ret5=-2.2; leftover $453.97 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `NVAX` | 43 | $10.41 | $2.12 | — | $1,120.03 | — | S≥+5: sizeup + more names; list flatten,ohlc_hot; ⚪; ret5=+11.1; leftover $453.97 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BVS` | 31 | $14.50 | $2.08 | — | $668.45 | — | S≥+5: sizeup + more names; list flatten; 🔵; ⚪; ret5=+0.8; leftover $453.97 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 232 | $1.95 | $2.99 | — | $213.05 | — | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $453.97 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `BTSG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-14 | `IREN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-14 | `TPG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-14 | `TGTX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-14 | `SLS` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-14 | `HIMS` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-14 | `INO` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-14 | `TNDM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-14 | `VOR` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-14 | `TLN` | cash | leftover split 10.32 < 1 share @ 359.83 |
| 2026-08-14 | `VST` | cash | leftover split 10.32 < 1 share @ 146.90 |
| 2026-08-14 | `NRG` | cash | leftover split 10.32 < 1 share @ 120.00 |
| 2026-08-14 | `DAVE` | cash | leftover split 10.32 < 1 share @ 330.91 |
| 2026-08-14 | `SLG` | cash | leftover split 10.32 < 1 share @ 57.61 |
| 2026-08-14 | `BETR` | cash | leftover split 10.32 < 1 share @ 14.80 |
| 2026-08-14 | `WWW` | cash | leftover split 10.32 < 1 share @ 20.60 |
| 2026-08-17 | `BTSG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `IREN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `TPG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `TGTX` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `SLS` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `HIMS` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `INO` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `TNDM` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `VOR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `MARA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `LDI` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `BTBT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `ANGX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `HYLN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `DVN` | cash | leftover split 9.75 < 1 share @ 46.18 |
| 2026-08-17 | `EOG` | cash | leftover split 9.75 < 1 share @ 142.77 |
| 2026-08-17 | `FANG` | cash | leftover split 9.75 < 1 share @ 202.70 |
| 2026-08-17 | `ELF` | cash | leftover split 9.75 < 1 share @ 90.54 |
| 2026-08-18 | `BTSG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `IREN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `TPG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `TGTX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `SLS` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `HIMS` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `INO` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `TNDM` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `VOR` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `MARA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `LDI` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `BTBT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `ANGX` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `HYLN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `TMC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-18 | `TGB` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-18 | `DNN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-18 | `HNST` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-18 | `OXY` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `APA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `COP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `MUR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `MLYS` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `TRMD` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OBE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CYPH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `BTSG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-19 | `IREN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-19 | `TPG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-19 | `TGTX` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-19 | `SLS` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-19 | `HIMS` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-19 | `INO` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-19 | `TNDM` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-19 | `VOR` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-19 | `MARA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `LDI` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `BTBT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `ANGX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `HYLN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `TMC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-19 | `TGB` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-19 | `DNN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-19 | `HNST` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `STE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DHR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SYK` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MUR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `TRMD` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MLYS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `TBPH` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-20 | `MARA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `LDI` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `BTBT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `ANGX` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `HYLN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `TMC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-20 | `TGB` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-20 | `DNN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-20 | `HNST` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-21 | `TMC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `TGB` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `DNN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `HNST` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `AG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `BHP` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `CDE` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `HDSN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `IAG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `KGC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `NFGC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `WPM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `AU` | cash | leftover split 24.46 < 1 share @ 119.43 |
| 2026-08-21 | `AEM` | cash | leftover split 24.46 < 1 share @ 216.30 |
| 2026-08-21 | `CRSP` | cash | leftover split 24.46 < 1 share @ 59.72 |
| 2026-08-24 | `AG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `BHP` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `CDE` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `HDSN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `IAG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `KGC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `NFGC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `WPM` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `AUPH` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `ARCT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `AUTL` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `CRDL` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `CYPH` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `RZLT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `MOS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `OCUL` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INSP` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CRMD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HCA` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CRML` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SAFX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `AG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `BHP` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `CDE` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `HDSN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `IAG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `KGC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `NFGC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `WPM` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `AUPH` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `ARCT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `AUTL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `CRDL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `CYPH` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `MOS` | cash | leftover split 15.55 < 1 share @ 24.00 |
| 2026-08-25 | `INSP` | cash | leftover split 15.55 < 1 share @ 61.47 |
| 2026-08-25 | `HCA` | cash | leftover split 15.55 < 1 share @ 429.24 |
| 2026-08-26 | `AG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `BHP` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `CDE` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `HDSN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `IAG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `KGC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `NFGC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `WPM` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `AUPH` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `ARCT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `AUTL` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `CRDL` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `CYPH` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `BMEA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-26 | `NPWR` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-26 | `HCA` | no_price | no 09:30 open |
| 2026-08-26 | `MOS` | no_price | no 09:30 open |
| 2026-08-26 | `INSP` | no_price | no 09:30 open |
| 2026-08-26 | `KURA` | no_price | no 09:30 open |
| 2026-08-26 | `AVBP` | no_price | no 09:30 open |
| 2026-08-27 | `AUPH` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `ARCT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `AUTL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `CRDL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `CYPH` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `OCUL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `CRMD` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `RZLT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `BMEA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `NPWR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-28 | `OCUL` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `CRMD` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `RZLT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `BMEA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `NPWR` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `ACMR` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-28 | `GGB` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-28 | `MT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-28 | `MU` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `OCUL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `CRMD` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `RZLT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `BMEA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `NPWR` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `RRC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `CRK` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `MOS` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `SLI` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `ACMR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `GGB` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `MT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `MU` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `ANF` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `BHVN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `BZ` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `CAPR` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `RES` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PBF` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `WTTR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `OKTA` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRWD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `VEEV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `RRC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-01 | `CRK` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-01 | `MOS` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-01 | `SLI` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-01 | `ACMR` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-01 | `GGB` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-01 | `MT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-01 | `MU` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-01 | `ANF` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `BHVN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `BZ` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `CAPR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `DK` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `BTE` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `MTDR` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `RES` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `KOS` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `OIS` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `FTI` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `KMI` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `RRC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-02 | `CRK` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-02 | `MOS` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-02 | `SLI` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-02 | `ACMR` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-02 | `GGB` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-02 | `MT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-02 | `MU` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-02 | `ANF` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `BHVN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `BZ` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `CAPR` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `PCRX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `HRMY` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PBH` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `VSTM` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `MGTX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PBR-A` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PBR` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `GPRO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-03 | `ANF` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-03 | `BHVN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-03 | `BZ` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-03 | `CAPR` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-04 | `HRMY` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `VSTM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `RVTY` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `FRVO` | min_hold | dropped but min-hold 1/5 sess — no sell |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `ATRC` | 26 | 2026-09-03 @ $49.76 | S≥+5: sizeup + more names; list flatten; 🔵; ⚪; ret5=+10.6; leftover $1327.75 |
| `HRMY` | 32 | 2026-09-03 @ $41.31 | S≥+5: sizeup + more names; list flatten,mover_buy; 🔵; ⚪; ret5=+0.1; leftover $1327.75 |
| `CABA` | 406 | 2026-09-03 @ $3.27 | S≥+5: sizeup + more names; list flatten; 🔵; ⚪; ret5=+13.8; leftover $1327.75 |
| `VSTM` | 172 | 2026-09-03 @ $7.70 | S≥+5: sizeup + more names; list flatten,mover_buy; 🔵; ⚪; ret5=+4.7; leftover $1327.75 |
| `RVTY` | 10 | 2026-09-03 @ $125.94 | S≥+5: sizeup + more names; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $1327.75 |
| `GPRO` | 1088 | 2026-09-03 @ $1.22 | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover; ret5=+5.9; leftover $1327.75 |
| `FRVO` | 72 | 2026-09-03 @ $18.40 | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover; ret5=-14.4; leftover $1327.75 |
| `ASND` | 1 | 2026-09-04 @ $266.94 | S≥+5: sizeup + more names; list flatten; ret5=+1.9; leftover $453.97 |
| `OSCR` | 14 | 2026-09-04 @ $30.65 | S≥+5: sizeup + more names; list flatten; 🔵; ret5=-2.2; leftover $453.97 |
| `NVAX` | 43 | 2026-09-04 @ $10.41 | S≥+5: sizeup + more names; list flatten,ohlc_hot; ⚪; ret5=+11.1; leftover $453.97 |
| `BVS` | 31 | 2026-09-04 @ $14.50 | S≥+5: sizeup + more names; list flatten; 🔵; ⚪; ret5=+0.8; leftover $453.97 |
| `BAK` | 232 | 2026-09-04 @ $1.95 | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $453.97 |
