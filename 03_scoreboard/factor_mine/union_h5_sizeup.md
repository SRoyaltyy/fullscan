# Factor mine action — `union_h5_sizeup`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **5** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `sizeup` · S≥+5: 1.35× leftover

Cash book **+18.84%** ($11,884) · signal-only (no cash/fees) was +58.01%. Starts YES **14/17**. Fills 100 · skips 242 · realized $+1543.64.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `list order` · **top_n** 8 (S≥+5 may raise this when S-boost is `sizeup`).
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **5**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $205.65.

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | vs yday close | Bought | Sold | Close cash | Close equity | Close held | 09:30 why |
|---|---:|---:|---|---:|---:|---|---|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | BTSG, IREN, TPG, TGTX, SLS, HIMS, INO, TNDM | — | $97.53 | $10,153.12 | BTSG×20, IREN×27, TPG×24, TGTX×25, SLS×106, HIMS×42, INO×1543, TNDM×53 | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-14 | +5.50 | $97.53 | BTSG×20, IREN×27, TPG×24, TGTX×25, SLS×106, HIMS×42, INO×1543, TNDM×53 | $10,178.12 | +25.00 | MARA, LDI, BTBT | — | $63.95 | $10,435.42 | BTSG×20, IREN×27, TPG×24, TGTX×25, SLS×106, HIMS×42, INO×1543, TNDM×53, MARA×1, LDI×13, BTBT×8 | 09:30 open · cash $97.53 (unchanged overnight, no fees) · equity $10,178.12 vs prior close $10,153.12 (+25.00) because holdings re-marked: BTSG×20 yday $60.23 → 09:30 $59.65 -11.60; IREN×27 yday $44.76 → 09:30 $44.09 -18.09; TPG×24 yday $54.62 → 09:30 $55.29 +16.08; TGTX×25 yday $47.94 → 09:30 $47.27 -16.75; SLS×106 yday $12.36 → 09:30 $12.40 +4.24; HIMS×42 yday $28.77 → 09:30 $29.15 +15.96; INO×1543 yday $0.90 → 09:30 $0.93 +46.29; TNDM×53 yday $23.13 → 09:30 $22.92 -11.13 |
| 2026-08-17 | +2.25 | $63.95 | BTSG×20, IREN×27, TPG×24, TGTX×25, SLS×106, HIMS×42, INO×1543, TNDM×53, MARA×1, LDI×13, BTBT×8 | $10,414.78 | -20.64 | TMC, DNN, HNST | — | $48.44 | $10,525.15 | BTSG×20, IREN×27, TPG×24, TGTX×25, SLS×106, HIMS×42, INO×1543, TNDM×53, MARA×1, LDI×13, BTBT×8, TMC×1, DNN×2, HNST×1 | 09:30 open · cash $63.95 (unchanged overnight, no fees) · equity $10,414.78 vs prior close $10,435.42 (-20.64) because holdings re-marked: BTSG×20 yday $61.71 → 09:30 $61.69 -0.40; IREN×27 yday $44.06 → 09:30 $45.23 +31.59; TPG×24 yday $53.03 → 09:30 $52.67 -8.64; TGTX×25 yday $48.74 → 09:30 $48.74 +0.00; SLS×106 yday $12.78 → 09:30 $12.78 +0.00; HIMS×42 yday $28.15 → 09:30 $28.14 -0.42; INO×1543 yday $1.09 → 09:30 $1.07 -30.86; TNDM×53 yday $22.72 → 09:30 $22.50 -11.66; MARA×1 yday $9.20 → 09:30 $9.22 +0.02; LDI×13 yday $0.90 → 09:30 $0.91 +0.13; BTBT×8 yday $1.57 → 09:30 $1.52 -0.40 |
| 2026-08-18 | -6.20 | $48.44 | BTSG×20, IREN×27, TPG×24, TGTX×25, SLS×106, HIMS×42, INO×1543, TNDM×53, MARA×1, LDI×13, BTBT×8, TMC×1, DNN×2, HNST×1 | $10,391.80 | -133.35 | — | — | $48.44 | $10,572.37 | BTSG×20, IREN×27, TPG×24, TGTX×25, SLS×106, HIMS×42, INO×1543, TNDM×53, MARA×1, LDI×13, BTBT×8, TMC×1, DNN×2, HNST×1 | 09:30 open · cash $48.44 (unchanged overnight, no fees) · equity $10,391.80 vs prior close $10,525.15 (-133.35) because holdings re-marked: BTSG×20 yday $60.38 → 09:30 $60.00 -7.60; IREN×27 yday $44.90 → 09:30 $43.56 -36.18; TPG×24 yday $51.77 → 09:30 $51.77 +0.00; TGTX×25 yday $49.28 → 09:30 $49.28 +0.00; SLS×106 yday $13.00 → 09:30 $12.66 -36.04; HIMS×42 yday $28.61 → 09:30 $27.85 -31.92; INO×1543 yday $1.15 → 09:30 $1.14 -15.43; TNDM×53 yday $22.25 → 09:30 $22.16 -5.03; MARA×1 yday $9.72 → 09:30 $9.36 -0.36; LDI×13 yday $0.88 → 09:30 $0.87 -0.07; BTBT×8 yday $1.60 → 09:30 $1.54 -0.48; TMC×1 yday $3.77 → 09:30 $3.72 -0.05; DNN×2 yday $3.19 → 09:30 $3.11 -0.16; HNST×1 yday $4.70 → 09:30 $4.67 -0.03 |
| 2026-08-19 | -7.20 | $48.44 | BTSG×20, IREN×27, TPG×24, TGTX×25, SLS×106, HIMS×42, INO×1543, TNDM×53, MARA×1, LDI×13, BTBT×8, TMC×1, DNN×2, HNST×1 | $10,710.13 | +137.76 | — | — | $48.44 | $11,031.12 | BTSG×20, IREN×27, TPG×24, TGTX×25, SLS×106, HIMS×42, INO×1543, TNDM×53, MARA×1, LDI×13, BTBT×8, TMC×1, DNN×2, HNST×1 | 09:30 open · cash $48.44 (unchanged overnight, no fees) · equity $10,710.13 vs prior close $10,572.37 (+137.76) because holdings re-marked: BTSG×20 yday $59.50 → 09:30 $60.15 +13.00; IREN×27 yday $42.00 → 09:30 $41.41 -15.80; TPG×24 yday $52.02 → 09:30 $52.26 +5.76; TGTX×25 yday $50.26 → 09:30 $51.62 +34.00; SLS×106 yday $13.10 → 09:30 $13.46 +38.16; HIMS×42 yday $27.39 → 09:30 $27.55 +6.72; INO×1543 yday $1.20 → 09:30 $1.22 +30.86; TNDM×53 yday $23.73 → 09:30 $24.20 +24.91; MARA×1 yday $8.96 → 09:30 $8.91 -0.05; LDI×13 yday $0.86 → 09:30 $0.88 +0.29; BTBT×8 yday $1.45 → 09:30 $1.42 -0.24; TMC×1 yday $3.92 → 09:30 $3.93 +0.01; DNN×2 yday $3.15 → 09:30 $3.19 +0.08; HNST×1 yday $4.75 → 09:30 $4.80 +0.05 |
| 2026-08-20 | +1.12 | $48.44 | BTSG×20, IREN×27, TPG×24, TGTX×25, SLS×106, HIMS×42, INO×1543, TNDM×53, MARA×1, LDI×13, BTBT×8, TMC×1, DNN×2, HNST×1 | $10,966.31 | -64.81 | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | BTSG, IREN, TPG, TGTX, SLS, HIMS, INO, TNDM | $179.82 | $11,161.56 | MARA×1, LDI×13, BTBT×8, TMC×1, DNN×2, HNST×1, AG×66, BHP×14, CDE×65, HDSN×235, IAG×69, KGC×45, NFGC×777, WPM×9 | 09:30 open · cash $48.44 (unchanged overnight, no fees) · equity $10,966.31 vs prior close $11,031.12 (-64.81) because holdings re-marked: BTSG×20 yday $59.33 → 09:30 $58.64 -13.80; IREN×27 yday $42.84 → 09:30 $42.46 -10.26; TPG×24 yday $53.18 → 09:30 $53.06 -2.88; TGTX×25 yday $51.69 → 09:30 $51.65 -1.00; SLS×106 yday $13.85 → 09:30 $13.84 -1.06; HIMS×42 yday $31.09 → 09:30 $30.66 -18.06; INO×1543 yday $1.30 → 09:30 $1.30 +0.00; TNDM×53 yday $23.46 → 09:30 $23.11 -18.55; MARA×1 yday $9.65 → 09:30 $10.21 +0.56; LDI×13 yday $0.88 → 09:30 $0.87 -0.07; BTBT×8 yday $1.40 → 09:30 $1.46 +0.44; TMC×1 yday $3.97 → 09:30 $3.92 -0.05; DNN×2 yday $3.22 → 09:30 $3.20 -0.04; HNST×1 yday $5.02 → 09:30 $4.98 -0.04 |
| 2026-08-21 | +3.25 | $179.82 | MARA×1, LDI×13, BTBT×8, TMC×1, DNN×2, HNST×1, AG×66, BHP×14, CDE×65, HDSN×235, IAG×69, KGC×45, NFGC×777, WPM×9 | $11,454.79 | +293.23 | AUPH, ARCT, AUTL, CRDL, CYPH | MARA, LDI, BTBT | $98.64 | $11,454.73 | TMC×1, DNN×2, HNST×1, AG×66, BHP×14, CDE×65, HDSN×235, IAG×69, KGC×45, NFGC×777, WPM×9, AUPH×1, ARCT×2, AUTL×10, CRDL×13, CYPH×20 | 09:30 open · cash $179.82 (unchanged overnight, no fees) · equity $11,454.79 vs prior close $11,161.56 (+293.23) because holdings re-marked: MARA×1 yday $11.15 → 09:30 $11.70 +0.55; LDI×13 yday $0.87 → 09:30 $0.87 -0.04; BTBT×8 yday $1.59 → 09:30 $1.66 +0.52; TMC×1 yday $3.97 → 09:30 $4.10 +0.13; DNN×2 yday $3.14 → 09:30 $3.23 +0.18; HNST×1 yday $4.96 → 09:30 $4.97 +0.01; AG×66 yday $21.19 → 09:30 $21.90 +46.86; BHP×14 yday $93.63 → 09:30 $95.72 +29.26; CDE×65 yday $21.11 → 09:30 $21.75 +41.60; HDSN×235 yday $5.57 → 09:30 $5.67 +23.50; IAG×69 yday $20.50 → 09:30 $21.17 +46.23; KGC×45 yday $31.43 → 09:30 $32.17 +33.30; NFGC×777 yday $1.75 → 09:30 $1.79 +31.08; WPM×9 yday $150.25 → 09:30 $154.70 +40.05 |
| 2026-08-24 | -5.17 | $98.64 | TMC×1, DNN×2, HNST×1, AG×66, BHP×14, CDE×65, HDSN×235, IAG×69, KGC×45, NFGC×777, WPM×9, AUPH×1, ARCT×2, AUTL×10, CRDL×13, CYPH×20 | $11,591.39 | +136.66 | — | TMC, DNN, HNST | $115.02 | $11,424.09 | AG×66, BHP×14, CDE×65, HDSN×235, IAG×69, KGC×45, NFGC×777, WPM×9, AUPH×1, ARCT×2, AUTL×10, CRDL×13, CYPH×20 | 09:30 open · cash $98.64 (unchanged overnight, no fees) · equity $11,591.39 vs prior close $11,454.73 (+136.66) because holdings re-marked: TMC×1 yday $4.79 → 09:30 $4.57 -0.22; DNN×2 yday $3.50 → 09:30 $3.50 +0.00; HNST×1 yday $5.05 → 09:30 $5.05 +0.00; AG×66 yday $21.09 → 09:30 $21.47 +25.08; BHP×14 yday $97.03 → 09:30 $97.34 +4.34; CDE×65 yday $20.97 → 09:30 $21.26 +18.85; HDSN×235 yday $5.63 → 09:30 $5.69 +14.10; IAG×69 yday $21.14 → 09:30 $21.44 +20.70; KGC×45 yday $32.76 → 09:30 $33.21 +20.25; NFGC×777 yday $1.84 → 09:30 $1.86 +15.54; WPM×9 yday $157.78 → 09:30 $158.96 +10.62; AUPH×1 yday $16.65 → 09:30 $16.60 -0.05; ARCT×2 yday $13.45 → 09:30 $13.26 -0.38; AUTL×10 yday $2.41 → 09:30 $2.36 -0.50; CRDL×13 yday $1.86 → 09:30 $1.87 +0.13; CYPH×20 yday $1.42 → 09:30 $1.83 +8.20 |
| 2026-08-25 | +1.80 | $115.02 | AG×66, BHP×14, CDE×65, HDSN×235, IAG×69, KGC×45, NFGC×777, WPM×9, AUPH×1, ARCT×2, AUTL×10, CRDL×13, CYPH×20 | $11,499.33 | +75.24 | OCUL, CRMD, RZLT, BMEA, NPWR | — | $57.78 | $11,433.91 | AG×66, BHP×14, CDE×65, HDSN×235, IAG×69, KGC×45, NFGC×777, WPM×9, AUPH×1, ARCT×2, AUTL×10, CRDL×13, CYPH×20, OCUL×1, CRMD×1, RZLT×2, BMEA×8, NPWR×7 | 09:30 open · cash $115.02 (unchanged overnight, no fees) · equity $11,499.33 vs prior close $11,424.09 (+75.24) because holdings re-marked: AG×66 yday $20.57 → 09:30 $20.73 +10.56; BHP×14 yday $96.66 → 09:30 $95.95 -9.94; CDE×65 yday $20.49 → 09:30 $20.85 +23.40; HDSN×235 yday $5.57 → 09:30 $5.53 -9.40; IAG×69 yday $21.36 → 09:30 $21.63 +18.63; KGC×45 yday $32.47 → 09:30 $32.76 +13.05; NFGC×777 yday $1.90 → 09:30 $1.91 +7.77; WPM×9 yday $158.00 → 09:30 $160.00 +18.00; AUPH×1 yday $16.60 → 09:30 $16.71 +0.11; ARCT×2 yday $13.76 → 09:30 $14.34 +1.16; AUTL×10 yday $2.38 → 09:30 $2.32 -0.60; CRDL×13 yday $1.80 → 09:30 $1.90 +1.30; CYPH×20 yday $1.64 → 09:30 $1.70 +1.20 |
| 2026-08-26 | +2.02 | $57.78 | AG×66, BHP×14, CDE×65, HDSN×235, IAG×69, KGC×45, NFGC×777, WPM×9, AUPH×1, ARCT×2, AUTL×10, CRDL×13, CYPH×20, OCUL×1, CRMD×1, RZLT×2, BMEA×8, NPWR×7 | $11,433.91 | +0.00 | — | — | $57.78 | $11,498.71 | AG×66, BHP×14, CDE×65, HDSN×235, IAG×69, KGC×45, NFGC×777, WPM×9, AUPH×1, ARCT×2, AUTL×10, CRDL×13, CYPH×20, OCUL×1, CRMD×1, RZLT×2, BMEA×8, NPWR×7 | 09:30 open · cash $57.78 (unchanged overnight, no fees) · equity $11,433.91 vs prior close $11,433.91 (+0.00) because holdings re-marked: AG×66 yday $20.68 → 09:30 $20.68 +0.00; BHP×14 yday $96.05 → 09:30 $96.05 +0.00; CDE×65 yday $20.71 → 09:30 $20.71 +0.00; HDSN×235 yday $5.49 → 09:30 $5.49 +0.00; IAG×69 yday $21.48 → 09:30 $21.48 +0.00; KGC×45 yday $32.55 → 09:30 $32.55 +0.00; NFGC×777 yday $1.90 → 09:30 $1.90 +0.00; WPM×9 yday $158.25 → 09:30 $158.25 +0.00; AUPH×1 yday $16.71 → 09:30 $16.71 +0.00; ARCT×2 yday $14.21 → 09:30 $14.21 +0.00; AUTL×10 yday $2.34 → 09:30 $2.34 +0.00; CRDL×13 yday $1.90 → 09:30 $1.90 +0.00; CYPH×20 yday $1.64 → 09:30 $1.64 +0.00; OCUL×1 yday $10.92 → 09:30 $10.92 +0.00; CRMD×1 yday $8.28 → 09:30 $8.28 +0.00; RZLT×2 yday $5.29 → 09:30 $5.29 +0.00; BMEA×8 yday $1.61 → 09:30 $1.61 +0.00; NPWR×7 yday $2.02 → 09:30 $2.02 +0.00 |
| 2026-08-27 | — | $57.78 | AG×66, BHP×14, CDE×65, HDSN×235, IAG×69, KGC×45, NFGC×777, WPM×9, AUPH×1, ARCT×2, AUTL×10, CRDL×13, CYPH×20, OCUL×1, CRMD×1, RZLT×2, BMEA×8, NPWR×7 | $11,599.81 | +101.10 | RRC, CRK, MOS, SLI, ACMR, GGB, MT, MU | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | $640.13 | $11,575.78 | AUPH×1, ARCT×2, AUTL×10, CRDL×13, CYPH×20, OCUL×1, CRMD×1, RZLT×2, BMEA×8, NPWR×7, RRC×34, CRK×101, MOS×57, SLI×549, ACMR×17, GGB×322, MT×18, MU×1 | 09:30 open · cash $57.78 (unchanged overnight, no fees) · equity $11,599.81 vs prior close $11,498.71 (+101.10) because holdings re-marked: AG×66 yday $20.68 → 09:30 $20.63 -3.30; BHP×14 yday $96.05 → 09:30 $96.99 +13.16; CDE×65 yday $20.71 → 09:30 $21.00 +18.85; HDSN×235 yday $5.49 → 09:30 $5.51 +4.70; IAG×69 yday $21.48 → 09:30 $21.64 +11.04; KGC×45 yday $32.55 → 09:30 $32.90 +15.75; NFGC×777 yday $1.90 → 09:30 $2.00 +77.70; WPM×9 yday $158.25 → 09:30 $160.93 +24.12; AUPH×1 yday $16.71 → 09:30 $16.60 -0.11; ARCT×2 yday $14.21 → 09:30 $15.35 +2.28; AUTL×10 yday $2.34 → 09:30 $2.41 +0.70; CRDL×13 yday $1.90 → 09:30 $2.03 +1.69; CYPH×20 yday $1.64 → 09:30 $1.60 -0.80; OCUL×1 yday $10.92 → 09:30 $10.79 -0.13; CRMD×1 yday $8.28 → 09:30 $8.60 +0.32; RZLT×2 yday $5.29 → 09:30 $5.01 -0.56; BMEA×8 yday $1.61 → 09:30 $1.75 +1.12; NPWR×7 yday $2.02 → 09:30 $1.93 -0.63 |
| 2026-08-28 | +0.75 | $640.13 | AUPH×1, ARCT×2, AUTL×10, CRDL×13, CYPH×20, OCUL×1, CRMD×1, RZLT×2, BMEA×8, NPWR×7, RRC×34, CRK×101, MOS×57, SLI×549, ACMR×17, GGB×322, MT×18, MU×1 | $11,658.54 | +82.76 | ANF, BHVN, BZ, CAPR | AUPH, ARCT, AUTL, CRDL, CYPH | $64.80 | $11,681.35 | OCUL×1, CRMD×1, RZLT×2, BMEA×8, NPWR×7, RRC×34, CRK×101, MOS×57, SLI×549, ACMR×17, GGB×322, MT×18, MU×1, ANF×1, BHVN×11, BZ×10, CAPR×20 | 09:30 open · cash $640.13 (unchanged overnight, no fees) · equity $11,658.54 vs prior close $11,575.78 (+82.76) because holdings re-marked: AUPH×1 yday $16.54 → 09:30 $16.47 -0.07; ARCT×2 yday $15.83 → 09:30 $15.74 -0.18; AUTL×10 yday $2.33 → 09:30 $2.32 -0.10; CRDL×13 yday $2.14 → 09:30 $2.09 -0.65; CYPH×20 yday $1.63 → 09:30 $1.75 +2.40; OCUL×1 yday $10.77 → 09:30 $10.63 -0.14; CRMD×1 yday $8.39 → 09:30 $8.49 +0.10; RZLT×2 yday $5.04 → 09:30 $5.07 +0.06; BMEA×8 yday $1.71 → 09:30 $1.74 +0.24; NPWR×7 yday $1.81 → 09:30 $1.83 +0.14; RRC×34 yday $41.55 → 09:30 $41.44 -3.74; CRK×101 yday $14.50 → 09:30 $14.42 -8.08; MOS×57 yday $24.16 → 09:30 $24.00 -9.12; SLI×549 yday $2.61 → 09:30 $2.60 -5.49; ACMR×17 yday $79.11 → 09:30 $81.65 +43.18; GGB×322 yday $4.46 → 09:30 $4.57 +35.42; MT×18 yday $74.53 → 09:30 $74.54 +0.18; MU×1 yday $938.40 → 09:30 $967.01 +28.61 |
| 2026-08-31 | -5.85 | $64.80 | OCUL×1, CRMD×1, RZLT×2, BMEA×8, NPWR×7, RRC×34, CRK×101, MOS×57, SLI×549, ACMR×17, GGB×322, MT×18, MU×1, ANF×1, BHVN×11, BZ×10, CAPR×20 | $11,431.50 | -249.85 | — | — | $64.80 | $11,448.01 | OCUL×1, CRMD×1, RZLT×2, BMEA×8, NPWR×7, RRC×34, CRK×101, MOS×57, SLI×549, ACMR×17, GGB×322, MT×18, MU×1, ANF×1, BHVN×11, BZ×10, CAPR×20 | 09:30 open · cash $64.80 (unchanged overnight, no fees) · equity $11,431.50 vs prior close $11,681.35 (-249.85) because holdings re-marked: OCUL×1 yday $10.82 → 09:30 $10.36 -0.46; CRMD×1 yday $8.31 → 09:30 $8.29 -0.02; RZLT×2 yday $4.98 → 09:30 $4.62 -0.72; BMEA×8 yday $1.68 → 09:30 $1.71 +0.24; NPWR×7 yday $1.89 → 09:30 $1.83 -0.42; RRC×34 yday $41.64 → 09:30 $41.11 -18.02; CRK×101 yday $14.62 → 09:30 $14.56 -6.06; MOS×57 yday $23.76 → 09:30 $23.75 -0.57; SLI×549 yday $2.64 → 09:30 $2.51 -71.37; ACMR×17 yday $80.49 → 09:30 $75.10 -91.63; GGB×322 yday $4.70 → 09:30 $4.55 -48.30; MT×18 yday $74.63 → 09:30 $75.07 +7.92; MU×1 yday $935.39 → 09:30 $933.01 -2.38; ANF×1 yday $145.75 → 09:30 $148.67 +2.92; BHVN×11 yday $16.12 → 09:30 $15.44 -7.48; BZ×10 yday $18.00 → 09:30 $17.89 -1.10; CAPR×20 yday $10.06 → 09:30 $9.44 -12.40 |
| 2026-09-01 | -6.30 | $64.80 | OCUL×1, CRMD×1, RZLT×2, BMEA×8, NPWR×7, RRC×34, CRK×101, MOS×57, SLI×549, ACMR×17, GGB×322, MT×18, MU×1, ANF×1, BHVN×11, BZ×10, CAPR×20 | $11,502.67 | +54.66 | — | OCUL, CRMD, RZLT, BMEA, NPWR | $117.90 | $11,545.56 | RRC×34, CRK×101, MOS×57, SLI×549, ACMR×17, GGB×322, MT×18, MU×1, ANF×1, BHVN×11, BZ×10, CAPR×20 | 09:30 open · cash $64.80 (unchanged overnight, no fees) · equity $11,502.67 vs prior close $11,448.01 (+54.66) because holdings re-marked: OCUL×1 yday $10.36 → 09:30 $10.49 +0.13; CRMD×1 yday $8.30 → 09:30 $8.26 -0.04; RZLT×2 yday $4.62 → 09:30 $4.69 +0.14; BMEA×8 yday $1.71 → 09:30 $1.65 -0.48; NPWR×7 yday $1.82 → 09:30 $1.78 -0.28; RRC×34 yday $41.78 → 09:30 $41.32 -15.64; CRK×101 yday $14.51 → 09:30 $14.31 -20.20; MOS×57 yday $23.78 → 09:30 $24.00 +12.54; SLI×549 yday $2.51 → 09:30 $2.70 +104.31; ACMR×17 yday $75.02 → 09:30 $71.24 -64.26; GGB×322 yday $4.55 → 09:30 $4.61 +19.32; MT×18 yday $75.06 → 09:30 $74.31 -13.50; MU×1 yday $933.01 → 09:30 $955.79 +22.78; ANF×1 yday $149.28 → 09:30 $142.47 -6.81; BHVN×11 yday $15.40 → 09:30 $15.45 +0.55; BZ×10 yday $17.90 → 09:30 $17.37 -5.30; CAPR×20 yday $9.36 → 09:30 $10.43 +21.40 |
| 2026-09-02 | -3.83 | $117.90 | RRC×34, CRK×101, MOS×57, SLI×549, ACMR×17, GGB×322, MT×18, MU×1, ANF×1, BHVN×11, BZ×10, CAPR×20 | $11,616.78 | +71.22 | — | — | $117.90 | $11,600.39 | RRC×34, CRK×101, MOS×57, SLI×549, ACMR×17, GGB×322, MT×18, MU×1, ANF×1, BHVN×11, BZ×10, CAPR×20 | 09:30 open · cash $117.90 (unchanged overnight, no fees) · equity $11,616.78 vs prior close $11,545.56 (+71.22) because holdings re-marked: RRC×34 yday $41.32 → 09:30 $41.94 +21.08; CRK×101 yday $14.90 → 09:30 $15.82 +92.92; MOS×57 yday $24.25 → 09:30 $23.94 -17.67; SLI×549 yday $2.70 → 09:30 $2.67 -16.47; ACMR×17 yday $71.88 → 09:30 $71.44 -7.48; GGB×322 yday $4.61 → 09:30 $4.57 -12.88; MT×18 yday $73.25 → 09:30 $73.22 -0.54; MU×1 yday $940.00 → 09:30 $941.12 +1.12; ANF×1 yday $143.00 → 09:30 $142.00 -1.00; BHVN×11 yday $15.45 → 09:30 $15.39 -0.66; BZ×10 yday $17.17 → 09:30 $17.29 +1.20; CAPR×20 yday $10.19 → 09:30 $10.77 +11.60 |
| 2026-09-03 | -0.90 | $117.90 | RRC×34, CRK×101, MOS×57, SLI×549, ACMR×17, GGB×322, MT×18, MU×1, ANF×1, BHVN×11, BZ×10, CAPR×20 | $11,611.10 | +10.71 | ATRC, HRMY, CABA, VSTM, RVTY, GPRO, FRVO | RRC, MOS, SLI, ACMR, GGB, MT, MU | $98.46 | $12,361.77 | CRK×101, ANF×1, BHVN×11, BZ×10, CAPR×20, ATRC×26, HRMY×32, CABA×406, VSTM×172, RVTY×10, GPRO×1090, FRVO×72 | 09:30 open · cash $117.90 (unchanged overnight, no fees) · equity $11,611.10 vs prior close $11,600.39 (+10.71) because holdings re-marked: RRC×34 yday $42.40 → 09:30 $42.10 -10.20; CRK×101 yday $16.02 → 09:30 $15.70 -32.32; MOS×57 yday $24.78 → 09:30 $24.70 -4.56; SLI×549 yday $2.49 → 09:30 $2.49 +0.00; ACMR×17 yday $70.04 → 09:30 $70.52 +8.16; GGB×322 yday $4.69 → 09:30 $4.81 +38.64; MT×18 yday $73.31 → 09:30 $73.86 +9.90; MU×1 yday $933.44 → 09:30 $930.83 -2.61; ANF×1 yday $140.68 → 09:30 $139.65 -1.03; BHVN×11 yday $15.74 → 09:30 $15.97 +2.53; BZ×10 yday $17.55 → 09:30 $17.65 +1.00; CAPR×20 yday $10.01 → 09:30 $10.07 +1.20 |
| 2026-09-04 | — | $98.46 | CRK×101, ANF×1, BHVN×11, BZ×10, CAPR×20, ATRC×26, HRMY×32, CABA×406, VSTM×172, RVTY×10, GPRO×1090, FRVO×72 | $12,524.82 | +163.05 | ASND, OSCR, NVAX, BVS, BAK | CRK, ANF, BHVN, BZ, CAPR | $205.65 | $11,884.35 | ATRC×26, HRMY×32, CABA×406, VSTM×172, RVTY×10, GPRO×1090, FRVO×72, ASND×1, OSCR×15, NVAX×44, BVS×32, BAK×239 | 09:30 open · cash $98.46 (unchanged overnight, no fees) · equity $12,524.82 vs prior close $12,361.77 (+163.05) because holdings re-marked: CRK×101 yday $15.54 → 09:30 $15.45 -9.09; ANF×1 yday $136.60 → 09:30 $137.70 +1.10; BHVN×11 yday $15.69 → 09:30 $15.89 +2.20; BZ×10 yday $17.30 → 09:30 $17.31 +0.10; CAPR×20 yday $9.89 → 09:30 $9.83 -1.20; ATRC×26 yday $52.59 → 09:30 $52.88 +7.54; HRMY×32 yday $42.86 → 09:30 $42.93 +2.24; CABA×406 yday $3.57 → 09:30 $3.63 +24.36; VSTM×172 yday $8.02 → 09:30 $8.03 +1.72; RVTY×10 yday $130.94 → 09:30 $132.45 +15.10; GPRO×1090 yday $1.69 → 09:30 $1.78 +98.10; FRVO×72 yday $17.98 → 09:30 $18.27 +20.88 |

## Fills (09:30 open snapshot, then buys / sells)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 20 | $59.80 | $2.05 | — | $8,801.95 | — | S≥+5: 1.35× leftover; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 27 | $45.98 | $2.07 | — | $7,558.42 | — | S≥+5: 1.35× leftover; list flatten; ⚪; ret5=+12.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 24 | $50.62 | $2.06 | — | $6,341.40 | — | S≥+5: 1.35× leftover; list flatten; ⚪; ret5=+6.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TGTX` | 25 | $49.70 | $2.06 | — | $5,096.84 | — | S≥+5: 1.35× leftover; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 106 | $11.70 | $2.31 | — | $3,854.33 | — | S≥+5: 1.35× leftover; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 42 | $29.74 | $2.12 | — | $2,603.13 | — | S≥+5: 1.35× leftover; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 1543 | $0.81 | $17.13 | — | $1,336.17 | — | S≥+5: 1.35× leftover; list flatten; ⚪; ret5=+13.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 53 | $23.33 | $2.15 | — | $97.53 | — | S≥+5: 1.35× leftover; list flatten; ⚪; ret5=+19.7; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $97.53 | ▲ 09:30 equity $10,178.12 vs yday $10,153.12 (+25.00) | 09:30 open · cash $97.53 (unchanged overnight, no fees) · equity $10,178.12 vs prior close $10,153.12 (+25.00) because holdings re-marked: BTSG×20 yday $60.23 → 09:30 $59.65 -11.60; IREN×27 yday $44.76 → 09:30 $44.09 -18.09; TPG×24 yday $54.62 → 09:30 $55.29 +16.08; TGTX×25 yday $47.94 → 09:30 $47.27 -16.75; SLS×106 yday $12.36 → 09:30 $12.40 +4.24; HIMS×42 yday $28.77 → 09:30 $29.15 +15.96; INO×1543 yday $0.90 → 09:30 $0.93 +46.29; TNDM×53 yday $23.13 → 09:30 $22.92 -11.13 | — |
| 2026-08-14 09:30 ET | **BUY** | `MARA` | 1 | $9.01 | $0.09 | — | $88.43 | — | S≥+5: 1.35× leftover; list flatten; 🔵; ⚪; ret5=-13.5; leftover $12.19 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 13 | $0.94 | $0.16 | — | $76.09 | — | S≥+5: 1.35× leftover; list flatten; 🔵; ⚪; ret5=+0.5; leftover $12.19 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 8 | $1.50 | $0.14 | — | $63.95 | — | S≥+5: 1.35× leftover; list flatten; 🔵; ⚪; ret5=+9.2; leftover $12.19 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $63.95 | ▼ 09:30 equity $10,414.78 vs yday $10,435.42 (-20.64) | 09:30 open · cash $63.95 (unchanged overnight, no fees) · equity $10,414.78 vs prior close $10,435.42 (-20.64) because holdings re-marked: BTSG×20 yday $61.71 → 09:30 $61.69 -0.40; IREN×27 yday $44.06 → 09:30 $45.23 +31.59; TPG×24 yday $53.03 → 09:30 $52.67 -8.64; TGTX×25 yday $48.74 → 09:30 $48.74 +0.00; SLS×106 yday $12.78 → 09:30 $12.78 +0.00; HIMS×42 yday $28.15 → 09:30 $28.14 -0.42; INO×1543 yday $1.09 → 09:30 $1.07 -30.86; TNDM×53 yday $22.72 → 09:30 $22.50 -11.66; MARA×1 yday $9.20 → 09:30 $9.22 +0.02; LDI×13 yday $0.90 → 09:30 $0.91 +0.13; BTBT×8 yday $1.57 → 09:30 $1.52 -0.40 | — |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 1 | $4.05 | $0.04 | — | $59.85 | — | S≥+5: 1.35× leftover; list flatten; 🔵; ⚪; ret5=-12.3; leftover $7.99 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 2 | $3.24 | $0.07 | — | $53.30 | — | S≥+5: 1.35× leftover; list flatten; ⚪; ret5=+0.3; leftover $7.99 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `HNST` | 1 | $4.81 | $0.05 | — | $48.44 | — | S≥+5: 1.35× leftover; list flatten; ⚪; ret5=-11.4; leftover $7.99 | join🟡 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $48.44 | ▼ 09:30 equity $10,391.80 vs yday $10,525.15 (-133.35) | 09:30 open · cash $48.44 (unchanged overnight, no fees) · equity $10,391.80 vs prior close $10,525.15 (-133.35) because holdings re-marked: BTSG×20 yday $60.38 → 09:30 $60.00 -7.60; IREN×27 yday $44.90 → 09:30 $43.56 -36.18; TPG×24 yday $51.77 → 09:30 $51.77 +0.00; TGTX×25 yday $49.28 → 09:30 $49.28 +0.00; SLS×106 yday $13.00 → 09:30 $12.66 -36.04; HIMS×42 yday $28.61 → 09:30 $27.85 -31.92; INO×1543 yday $1.15 → 09:30 $1.14 -15.43; TNDM×53 yday $22.25 → 09:30 $22.16 -5.03; MARA×1 yday $9.72 → 09:30 $9.36 -0.36; LDI×13 yday $0.88 → 09:30 $0.87 -0.07; BTBT×8 yday $1.60 → 09:30 $1.54 -0.48; TMC×1 yday $3.77 → 09:30 $3.72 -0.05; DNN×2 yday $3.19 → 09:30 $3.11 -0.16; HNST×1 yday $4.70 → 09:30 $4.67 -0.03 | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $48.44 | ▲ 09:30 equity $10,710.13 vs yday $10,572.37 (+137.76) | 09:30 open · cash $48.44 (unchanged overnight, no fees) · equity $10,710.13 vs prior close $10,572.37 (+137.76) because holdings re-marked: BTSG×20 yday $59.50 → 09:30 $60.15 +13.00; IREN×27 yday $42.00 → 09:30 $41.41 -15.80; TPG×24 yday $52.02 → 09:30 $52.26 +5.76; TGTX×25 yday $50.26 → 09:30 $51.62 +34.00; SLS×106 yday $13.10 → 09:30 $13.46 +38.16; HIMS×42 yday $27.39 → 09:30 $27.55 +6.72; INO×1543 yday $1.20 → 09:30 $1.22 +30.86; TNDM×53 yday $23.73 → 09:30 $24.20 +24.91; MARA×1 yday $8.96 → 09:30 $8.91 -0.05; LDI×13 yday $0.86 → 09:30 $0.88 +0.29; BTBT×8 yday $1.45 → 09:30 $1.42 -0.24; TMC×1 yday $3.92 → 09:30 $3.93 +0.01; DNN×2 yday $3.15 → 09:30 $3.19 +0.08; HNST×1 yday $4.75 → 09:30 $4.80 +0.05 | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $48.44 | ▼ 09:30 equity $10,966.31 vs yday $11,031.12 (-64.81) | 09:30 open · cash $48.44 (unchanged overnight, no fees) · equity $10,966.31 vs prior close $11,031.12 (-64.81) because holdings re-marked: BTSG×20 yday $59.33 → 09:30 $58.64 -13.80; IREN×27 yday $42.84 → 09:30 $42.46 -10.26; TPG×24 yday $53.18 → 09:30 $53.06 -2.88; TGTX×25 yday $51.69 → 09:30 $51.65 -1.00; SLS×106 yday $13.85 → 09:30 $13.84 -1.06; HIMS×42 yday $31.09 → 09:30 $30.66 -18.06; INO×1543 yday $1.30 → 09:30 $1.30 +0.00; TNDM×53 yday $23.46 → 09:30 $23.11 -18.55; MARA×1 yday $9.65 → 09:30 $10.21 +0.56; LDI×13 yday $0.88 → 09:30 $0.87 -0.07; BTBT×8 yday $1.40 → 09:30 $1.46 +0.44; TMC×1 yday $3.97 → 09:30 $3.92 -0.05; DNN×2 yday $3.22 → 09:30 $3.20 -0.04; HNST×1 yday $5.02 → 09:30 $4.98 -0.04 | — |
| 2026-08-20 09:30 ET | **SELL** | `BTSG` | 20 | $58.64 | $2.07 | $-27.32 | $1,219.17 | ▼ -27.32 after sell → book $10,964.24; vs 09:30 mark -2.07 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `IREN` | 27 | $42.46 | $2.09 | $-99.20 | $2,363.50 | ▼ -99.20 after sell → book $10,962.15; vs 09:30 mark -2.09 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `TPG` | 24 | $53.06 | $2.08 | $+54.34 | $3,634.86 | ▲ +54.34 after sell → book $10,960.07; vs 09:30 mark -2.08 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `TGTX` | 25 | $51.65 | $2.09 | $+44.60 | $4,924.02 | ▲ +44.60 after sell → book $10,957.99; vs 09:30 mark -2.08 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `SLS` | 106 | $13.84 | $2.34 | $+222.19 | $6,388.72 | ▲ +222.19 after sell → book $10,955.65; vs 09:30 mark -2.34 | dropped from list after 5 sess (min 5) | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **SELL** | `HIMS` | 42 | $30.66 | $2.14 | $+34.39 | $7,674.31 | ▲ +34.39 after sell → book $10,953.51; vs 09:30 mark -2.14 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `INO` | 1543 | $1.30 | $20.18 | $+718.77 | $9,660.03 | ▲ +718.77 after sell → book $10,933.33; vs 09:30 mark -20.18 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `TNDM` | 53 | $23.11 | $2.17 | $-15.98 | $10,882.69 | ▼ -15.98 after sell → book $10,931.17; vs 09:30 mark -2.16 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 66 | $20.55 | $2.19 | — | $9,524.20 | — | S≥+5: 1.35× leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1360.34 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 14 | $91.01 | $2.03 | — | $8,248.03 | — | S≥+5: 1.35× leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1360.34 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 65 | $20.65 | $2.19 | — | $6,903.60 | — | S≥+5: 1.35× leftover; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1360.34 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 235 | $5.77 | $3.03 | — | $5,544.62 | — | S≥+5: 1.35× leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1360.34 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 69 | $19.63 | $2.20 | — | $4,187.95 | — | S≥+5: 1.35× leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1360.34 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 45 | $29.63 | $2.12 | — | $2,852.47 | — | S≥+5: 1.35× leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1360.34 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 777 | $1.75 | $10.02 | — | $1,482.70 | — | S≥+5: 1.35× leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1360.34 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 9 | $144.54 | $2.02 | — | $179.82 | — | S≥+5: 1.35× leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1360.34 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $179.82 | ▲ 09:30 equity $11,454.79 vs yday $11,161.56 (+293.23) | 09:30 open · cash $179.82 (unchanged overnight, no fees) · equity $11,454.79 vs prior close $11,161.56 (+293.23) because holdings re-marked: MARA×1 yday $11.15 → 09:30 $11.70 +0.55; LDI×13 yday $0.87 → 09:30 $0.87 -0.04; BTBT×8 yday $1.59 → 09:30 $1.66 +0.52; TMC×1 yday $3.97 → 09:30 $4.10 +0.13; DNN×2 yday $3.14 → 09:30 $3.23 +0.18; HNST×1 yday $4.96 → 09:30 $4.97 +0.01; AG×66 yday $21.19 → 09:30 $21.90 +46.86; BHP×14 yday $93.63 → 09:30 $95.72 +29.26; CDE×65 yday $21.11 → 09:30 $21.75 +41.60; HDSN×235 yday $5.57 → 09:30 $5.67 +23.50; IAG×69 yday $20.50 → 09:30 $21.17 +46.23; KGC×45 yday $31.43 → 09:30 $32.17 +33.30; NFGC×777 yday $1.75 → 09:30 $1.79 +31.08; WPM×9 yday $150.25 → 09:30 $154.70 +40.05 | — |
| 2026-08-21 09:30 ET | **SELL** | `MARA` | 1 | $11.70 | $0.14 | $+2.46 | $191.38 | ▲ +2.46 after sell → book $11,454.65; vs 09:30 mark -0.14 | dropped from list after 5 sess (min 5) | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `LDI` | 13 | $0.87 | $0.17 | $-1.24 | $202.48 | ▼ -1.24 after sell → book $11,454.48; vs 09:30 mark -0.17 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `BTBT` | 8 | $1.66 | $0.18 | $+0.96 | $215.59 | ▲ +0.96 after sell → book $11,454.31; vs 09:30 mark -0.17 | dropped from list after 5 sess (min 5) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 1 | $17.20 | $0.17 | — | $198.21 | — | S≥+5: 1.35× leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $26.95 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 2 | $11.13 | $0.23 | — | $175.72 | — | S≥+5: 1.35× leftover; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $26.95 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 10 | $2.47 | $0.28 | — | $150.75 | — | S≥+5: 1.35× leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $26.95 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 13 | $1.93 | $0.29 | — | $125.37 | — | S≥+5: 1.35× leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $26.95 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 20 | $1.32 | $0.32 | — | $98.64 | — | S≥+5: 1.35× leftover; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $26.95 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $98.64 | ▲ 09:30 equity $11,591.39 vs yday $11,454.73 (+136.66) | 09:30 open · cash $98.64 (unchanged overnight, no fees) · equity $11,591.39 vs prior close $11,454.73 (+136.66) because holdings re-marked: TMC×1 yday $4.79 → 09:30 $4.57 -0.22; DNN×2 yday $3.50 → 09:30 $3.50 +0.00; HNST×1 yday $5.05 → 09:30 $5.05 +0.00; AG×66 yday $21.09 → 09:30 $21.47 +25.08; BHP×14 yday $97.03 → 09:30 $97.34 +4.34; CDE×65 yday $20.97 → 09:30 $21.26 +18.85; HDSN×235 yday $5.63 → 09:30 $5.69 +14.10; IAG×69 yday $21.14 → 09:30 $21.44 +20.70; KGC×45 yday $32.76 → 09:30 $33.21 +20.25; NFGC×777 yday $1.84 → 09:30 $1.86 +15.54; WPM×9 yday $157.78 → 09:30 $158.96 +10.62; AUPH×1 yday $16.65 → 09:30 $16.60 -0.05; ARCT×2 yday $13.45 → 09:30 $13.26 -0.38; AUTL×10 yday $2.41 → 09:30 $2.36 -0.50; CRDL×13 yday $1.86 → 09:30 $1.87 +0.13; CYPH×20 yday $1.42 → 09:30 $1.83 +8.20 | — |
| 2026-08-24 09:30 ET | **SELL** | `TMC` | 1 | $4.57 | $0.07 | $+0.41 | $103.14 | ▲ +0.41 after sell → book $11,591.32; vs 09:30 mark -0.07 | dropped from list after 5 sess (min 5) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `DNN` | 2 | $3.50 | $0.10 | $+0.35 | $110.05 | ▲ +0.35 after sell → book $11,591.23; vs 09:30 mark -0.09 | dropped from list after 5 sess (min 5) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 judge🟡 ab🟡 peer🟢 vol🟢 buy🟢 |
| 2026-08-24 09:30 ET | **SELL** | `HNST` | 1 | $5.05 | $0.07 | $+0.12 | $115.02 | ▲ +0.12 after sell → book $11,591.15; vs 09:30 mark -0.08 | dropped from list after 5 sess (min 5) | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $115.02 | ▲ 09:30 equity $11,499.33 vs yday $11,424.09 (+75.24) | 09:30 open · cash $115.02 (unchanged overnight, no fees) · equity $11,499.33 vs prior close $11,424.09 (+75.24) because holdings re-marked: AG×66 yday $20.57 → 09:30 $20.73 +10.56; BHP×14 yday $96.66 → 09:30 $95.95 -9.94; CDE×65 yday $20.49 → 09:30 $20.85 +23.40; HDSN×235 yday $5.57 → 09:30 $5.53 -9.40; IAG×69 yday $21.36 → 09:30 $21.63 +18.63; KGC×45 yday $32.47 → 09:30 $32.76 +13.05; NFGC×777 yday $1.90 → 09:30 $1.91 +7.77; WPM×9 yday $158.00 → 09:30 $160.00 +18.00; AUPH×1 yday $16.60 → 09:30 $16.71 +0.11; ARCT×2 yday $13.76 → 09:30 $14.34 +1.16; AUTL×10 yday $2.38 → 09:30 $2.32 -0.60; CRDL×13 yday $1.80 → 09:30 $1.90 +1.30; CYPH×20 yday $1.64 → 09:30 $1.70 +1.20 | — |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 1 | $10.92 | $0.11 | — | $103.99 | — | S≥+5: 1.35× leftover; list flatten; 🔵; ret5=+10.4; leftover $14.38 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 1 | $8.28 | $0.09 | — | $95.63 | — | S≥+5: 1.35× leftover; list flatten; 🔵; ⚪; ret5=+8.8; leftover $14.38 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 2 | $5.23 | $0.11 | — | $85.06 | — | S≥+5: 1.35× leftover; list flatten; ret5=+10.7; leftover $14.38 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 8 | $1.62 | $0.15 | — | $71.94 | — | S≥+5: 1.35× leftover; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.8; leftover $14.38 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `NPWR` | 7 | $2.00 | $0.16 | — | $57.78 | — | S≥+5: 1.35× leftover; list probable,yday_gainer,yday_mover; 🔵; ret5=-12.8; leftover $14.38 | join🟡 sector🟢 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $57.78 | ▲ 09:30 equity $11,433.91 vs yday $11,433.91 (+0.00) | 09:30 open · cash $57.78 (unchanged overnight, no fees) · equity $11,433.91 vs prior close $11,433.91 (+0.00) because holdings re-marked: AG×66 yday $20.68 → 09:30 $20.68 +0.00; BHP×14 yday $96.05 → 09:30 $96.05 +0.00; CDE×65 yday $20.71 → 09:30 $20.71 +0.00; HDSN×235 yday $5.49 → 09:30 $5.49 +0.00; IAG×69 yday $21.48 → 09:30 $21.48 +0.00; KGC×45 yday $32.55 → 09:30 $32.55 +0.00; NFGC×777 yday $1.90 → 09:30 $1.90 +0.00; WPM×9 yday $158.25 → 09:30 $158.25 +0.00; AUPH×1 yday $16.71 → 09:30 $16.71 +0.00; ARCT×2 yday $14.21 → 09:30 $14.21 +0.00; AUTL×10 yday $2.34 → 09:30 $2.34 +0.00; CRDL×13 yday $1.90 → 09:30 $1.90 +0.00; CYPH×20 yday $1.64 → 09:30 $1.64 +0.00; OCUL×1 yday $10.92 → 09:30 $10.92 +0.00; CRMD×1 yday $8.28 → 09:30 $8.28 +0.00; RZLT×2 yday $5.29 → 09:30 $5.29 +0.00; BMEA×8 yday $1.61 → 09:30 $1.61 +0.00; NPWR×7 yday $2.02 → 09:30 $2.02 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $57.78 | ▲ 09:30 equity $11,599.81 vs yday $11,498.71 (+101.10) | 09:30 open · cash $57.78 (unchanged overnight, no fees) · equity $11,599.81 vs prior close $11,498.71 (+101.10) because holdings re-marked: AG×66 yday $20.68 → 09:30 $20.63 -3.30; BHP×14 yday $96.05 → 09:30 $96.99 +13.16; CDE×65 yday $20.71 → 09:30 $21.00 +18.85; HDSN×235 yday $5.49 → 09:30 $5.51 +4.70; IAG×69 yday $21.48 → 09:30 $21.64 +11.04; KGC×45 yday $32.55 → 09:30 $32.90 +15.75; NFGC×777 yday $1.90 → 09:30 $2.00 +77.70; WPM×9 yday $158.25 → 09:30 $160.93 +24.12; AUPH×1 yday $16.71 → 09:30 $16.60 -0.11; ARCT×2 yday $14.21 → 09:30 $15.35 +2.28; AUTL×10 yday $2.34 → 09:30 $2.41 +0.70; CRDL×13 yday $1.90 → 09:30 $2.03 +1.69; CYPH×20 yday $1.64 → 09:30 $1.60 -0.80; OCUL×1 yday $10.92 → 09:30 $10.79 -0.13; CRMD×1 yday $8.28 → 09:30 $8.60 +0.32; RZLT×2 yday $5.29 → 09:30 $5.01 -0.56; BMEA×8 yday $1.61 → 09:30 $1.75 +1.12; NPWR×7 yday $2.02 → 09:30 $1.93 -0.63 | — |
| 2026-08-27 09:30 ET | **SELL** | `AG` | 66 | $20.63 | $2.21 | $+0.88 | $1,417.15 | ▲ +0.88 after sell → book $11,597.60; vs 09:30 mark -2.21 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `BHP` | 14 | $96.99 | $2.05 | $+79.64 | $2,772.96 | ▲ +79.64 after sell → book $11,595.55; vs 09:30 mark -2.05 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `CDE` | 65 | $21.00 | $2.21 | $+18.36 | $4,135.75 | ▲ +18.36 after sell → book $11,593.34; vs 09:30 mark -2.21 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `HDSN` | 235 | $5.51 | $3.08 | $-67.21 | $5,427.52 | ▼ -67.21 after sell → book $11,590.26; vs 09:30 mark -3.08 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `IAG` | 69 | $21.64 | $2.22 | $+134.27 | $6,918.46 | ▲ +134.27 after sell → book $11,588.04; vs 09:30 mark -2.22 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `KGC` | 45 | $32.90 | $2.15 | $+142.88 | $8,396.81 | ▲ +142.88 after sell → book $11,585.89; vs 09:30 mark -2.15 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `NFGC` | 777 | $2.00 | $10.16 | $+174.06 | $9,940.65 | ▲ +174.06 after sell → book $11,575.73; vs 09:30 mark -10.16 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `WPM` | 9 | $160.93 | $2.04 | $+143.45 | $11,386.98 | ▲ +143.45 after sell → book $11,573.69; vs 09:30 mark -2.04 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 34 | $40.72 | $2.09 | — | $10,000.41 | — | S≥+5: 1.35× leftover; list flatten; ret5=+1.8; leftover $1423.37 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 101 | $14.09 | $2.29 | — | $8,575.02 | — | S≥+5: 1.35× leftover; list flatten; ret5=+1.1; leftover $1423.37 | join🟢 sector🔴 gen🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MOS` | 57 | $24.84 | $2.16 | — | $7,156.98 | — | S≥+5: 1.35× leftover; list flatten; ret5=+13.0; leftover $1423.37 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟢 |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 549 | $2.59 | $7.08 | — | $5,727.99 | — | S≥+5: 1.35× leftover; list flatten; ret5=+4.2; leftover $1423.37 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `ACMR` | 17 | $80.97 | $2.04 | — | $4,349.46 | — | S≥+5: 1.35× leftover; list mover_buy; 🔵; ret5=-1.3; leftover $1423.37 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `GGB` | 322 | $4.42 | $4.15 | — | $2,922.07 | — | S≥+5: 1.35× leftover; list mover_buy; 🔵; ret5=-8.6; leftover $1423.37 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MT` | 18 | $75.12 | $2.04 | — | $1,567.86 | — | S≥+5: 1.35× leftover; list mover_buy; 🔵; ret5=-2.2; leftover $1423.37 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MU` | 1 | $925.74 | $1.99 | — | $640.13 | — | S≥+5: 1.35× leftover; list mover_buy; 🔵; ret5=-0.5; leftover $1423.37 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $640.13 | ▲ 09:30 equity $11,658.54 vs yday $11,575.78 (+82.76) | 09:30 open · cash $640.13 (unchanged overnight, no fees) · equity $11,658.54 vs prior close $11,575.78 (+82.76) because holdings re-marked: AUPH×1 yday $16.54 → 09:30 $16.47 -0.07; ARCT×2 yday $15.83 → 09:30 $15.74 -0.18; AUTL×10 yday $2.33 → 09:30 $2.32 -0.10; CRDL×13 yday $2.14 → 09:30 $2.09 -0.65; CYPH×20 yday $1.63 → 09:30 $1.75 +2.40; OCUL×1 yday $10.77 → 09:30 $10.63 -0.14; CRMD×1 yday $8.39 → 09:30 $8.49 +0.10; RZLT×2 yday $5.04 → 09:30 $5.07 +0.06; BMEA×8 yday $1.71 → 09:30 $1.74 +0.24; NPWR×7 yday $1.81 → 09:30 $1.83 +0.14; RRC×34 yday $41.55 → 09:30 $41.44 -3.74; CRK×101 yday $14.50 → 09:30 $14.42 -8.08; MOS×57 yday $24.16 → 09:30 $24.00 -9.12; SLI×549 yday $2.61 → 09:30 $2.60 -5.49; ACMR×17 yday $79.11 → 09:30 $81.65 +43.18; GGB×322 yday $4.46 → 09:30 $4.57 +35.42; MT×18 yday $74.53 → 09:30 $74.54 +0.18; MU×1 yday $938.40 → 09:30 $967.01 +28.61 | — |
| 2026-08-28 09:30 ET | **SELL** | `AUPH` | 1 | $16.47 | $0.19 | $-1.09 | $656.41 | ▼ -1.09 after sell → book $11,658.35; vs 09:30 mark -0.19 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `ARCT` | 2 | $15.74 | $0.34 | $+8.65 | $687.55 | ▲ +8.65 after sell → book $11,658.01; vs 09:30 mark -0.34 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `AUTL` | 10 | $2.32 | $0.28 | $-2.06 | $710.47 | ▼ -2.06 after sell → book $11,657.73; vs 09:30 mark -0.28 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `CRDL` | 13 | $2.09 | $0.33 | $+1.46 | $737.31 | ▲ +1.46 after sell → book $11,657.40; vs 09:30 mark -0.33 | dropped from list after 5 sess (min 5) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `CYPH` | 20 | $1.75 | $0.43 | $+7.85 | $771.88 | ▲ +7.85 after sell → book $11,656.97; vs 09:30 mark -0.43 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 1 | $144.70 | $1.45 | — | $625.73 | — | S≥+5: 1.35× leftover; list probable,yday_gainer,yday_mover; 🔵; ret5=+0.8; leftover $192.97 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BHVN` | 11 | $16.95 | $1.90 | — | $437.38 | — | S≥+5: 1.35× leftover; list probable,yday_gainer,yday_mover; ret5=-5.4; leftover $192.97 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BZ` | 10 | $18.50 | $1.88 | — | $250.50 | — | S≥+5: 1.35× leftover; list probable,yday_gainer,yday_mover; ret5=+2.8; leftover $192.97 | join🟢 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `CAPR` | 20 | $9.19 | $1.90 | — | $64.80 | — | S≥+5: 1.35× leftover; list probable,yday_gainer,yday_mover; ret5=-5.4; leftover $192.97 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $64.80 | ▼ 09:30 equity $11,431.50 vs yday $11,681.35 (-249.85) | 09:30 open · cash $64.80 (unchanged overnight, no fees) · equity $11,431.50 vs prior close $11,681.35 (-249.85) because holdings re-marked: OCUL×1 yday $10.82 → 09:30 $10.36 -0.46; CRMD×1 yday $8.31 → 09:30 $8.29 -0.02; RZLT×2 yday $4.98 → 09:30 $4.62 -0.72; BMEA×8 yday $1.68 → 09:30 $1.71 +0.24; NPWR×7 yday $1.89 → 09:30 $1.83 -0.42; RRC×34 yday $41.64 → 09:30 $41.11 -18.02; CRK×101 yday $14.62 → 09:30 $14.56 -6.06; MOS×57 yday $23.76 → 09:30 $23.75 -0.57; SLI×549 yday $2.64 → 09:30 $2.51 -71.37; ACMR×17 yday $80.49 → 09:30 $75.10 -91.63; GGB×322 yday $4.70 → 09:30 $4.55 -48.30; MT×18 yday $74.63 → 09:30 $75.07 +7.92; MU×1 yday $935.39 → 09:30 $933.01 -2.38; ANF×1 yday $145.75 → 09:30 $148.67 +2.92; BHVN×11 yday $16.12 → 09:30 $15.44 -7.48; BZ×10 yday $18.00 → 09:30 $17.89 -1.10; CAPR×20 yday $10.06 → 09:30 $9.44 -12.40 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $64.80 | ▲ 09:30 equity $11,502.67 vs yday $11,448.01 (+54.66) | 09:30 open · cash $64.80 (unchanged overnight, no fees) · equity $11,502.67 vs prior close $11,448.01 (+54.66) because holdings re-marked: OCUL×1 yday $10.36 → 09:30 $10.49 +0.13; CRMD×1 yday $8.30 → 09:30 $8.26 -0.04; RZLT×2 yday $4.62 → 09:30 $4.69 +0.14; BMEA×8 yday $1.71 → 09:30 $1.65 -0.48; NPWR×7 yday $1.82 → 09:30 $1.78 -0.28; RRC×34 yday $41.78 → 09:30 $41.32 -15.64; CRK×101 yday $14.51 → 09:30 $14.31 -20.20; MOS×57 yday $23.78 → 09:30 $24.00 +12.54; SLI×549 yday $2.51 → 09:30 $2.70 +104.31; ACMR×17 yday $75.02 → 09:30 $71.24 -64.26; GGB×322 yday $4.55 → 09:30 $4.61 +19.32; MT×18 yday $75.06 → 09:30 $74.31 -13.50; MU×1 yday $933.01 → 09:30 $955.79 +22.78; ANF×1 yday $149.28 → 09:30 $142.47 -6.81; BHVN×11 yday $15.40 → 09:30 $15.45 +0.55; BZ×10 yday $17.90 → 09:30 $17.37 -5.30; CAPR×20 yday $9.36 → 09:30 $10.43 +21.40 | — |
| 2026-09-01 09:30 ET | **SELL** | `OCUL` | 1 | $10.49 | $0.13 | $-0.67 | $75.17 | ▼ -0.67 after sell → book $11,502.55; vs 09:30 mark -0.12 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `CRMD` | 1 | $8.26 | $0.11 | $-0.21 | $83.32 | ▼ -0.21 after sell → book $11,502.44; vs 09:30 mark -0.11 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `RZLT` | 2 | $4.69 | $0.12 | $-1.31 | $92.58 | ▼ -1.31 after sell → book $11,502.32; vs 09:30 mark -0.12 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `BMEA` | 8 | $1.65 | $0.18 | $-0.09 | $105.60 | ▼ -0.09 after sell → book $11,502.14; vs 09:30 mark -0.18 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `NPWR` | 7 | $1.78 | $0.17 | $-1.87 | $117.90 | ▼ -1.87 after sell → book $11,501.98; vs 09:30 mark -0.16 | dropped from list after 5 sess (min 5) | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $117.90 | ▲ 09:30 equity $11,616.78 vs yday $11,545.56 (+71.22) | 09:30 open · cash $117.90 (unchanged overnight, no fees) · equity $11,616.78 vs prior close $11,545.56 (+71.22) because holdings re-marked: RRC×34 yday $41.32 → 09:30 $41.94 +21.08; CRK×101 yday $14.90 → 09:30 $15.82 +92.92; MOS×57 yday $24.25 → 09:30 $23.94 -17.67; SLI×549 yday $2.70 → 09:30 $2.67 -16.47; ACMR×17 yday $71.88 → 09:30 $71.44 -7.48; GGB×322 yday $4.61 → 09:30 $4.57 -12.88; MT×18 yday $73.25 → 09:30 $73.22 -0.54; MU×1 yday $940.00 → 09:30 $941.12 +1.12; ANF×1 yday $143.00 → 09:30 $142.00 -1.00; BHVN×11 yday $15.45 → 09:30 $15.39 -0.66; BZ×10 yday $17.17 → 09:30 $17.29 +1.20; CAPR×20 yday $10.19 → 09:30 $10.77 +11.60 | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $117.90 | ▲ 09:30 equity $11,611.10 vs yday $11,600.39 (+10.71) | 09:30 open · cash $117.90 (unchanged overnight, no fees) · equity $11,611.10 vs prior close $11,600.39 (+10.71) because holdings re-marked: RRC×34 yday $42.40 → 09:30 $42.10 -10.20; CRK×101 yday $16.02 → 09:30 $15.70 -32.32; MOS×57 yday $24.78 → 09:30 $24.70 -4.56; SLI×549 yday $2.49 → 09:30 $2.49 +0.00; ACMR×17 yday $70.04 → 09:30 $70.52 +8.16; GGB×322 yday $4.69 → 09:30 $4.81 +38.64; MT×18 yday $73.31 → 09:30 $73.86 +9.90; MU×1 yday $933.44 → 09:30 $930.83 -2.61; ANF×1 yday $140.68 → 09:30 $139.65 -1.03; BHVN×11 yday $15.74 → 09:30 $15.97 +2.53; BZ×10 yday $17.55 → 09:30 $17.65 +1.00; CAPR×20 yday $10.01 → 09:30 $10.07 +1.20 | — |
| 2026-09-03 09:30 ET | **SELL** | `RRC` | 34 | $42.10 | $2.11 | $+42.71 | $1,547.18 | ▲ +42.71 after sell → book $11,608.98; vs 09:30 mark -2.12 | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `MOS` | 57 | $24.70 | $2.18 | $-12.32 | $2,952.90 | ▼ -12.32 after sell → book $11,606.80; vs 09:30 mark -2.18 | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `SLI` | 549 | $2.49 | $7.18 | $-69.17 | $4,312.73 | ▼ -69.17 after sell → book $11,599.62; vs 09:30 mark -7.18 | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `ACMR` | 17 | $70.52 | $2.06 | $-181.75 | $5,509.51 | ▼ -181.75 after sell → book $11,597.56; vs 09:30 mark -2.06 | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `GGB` | 322 | $4.81 | $4.22 | $+117.21 | $7,054.11 | ▲ +117.21 after sell → book $11,593.34; vs 09:30 mark -4.22 | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `MT` | 18 | $73.86 | $2.06 | $-26.79 | $8,381.52 | ▼ -26.79 after sell → book $11,591.27; vs 09:30 mark -2.07 | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `MU` | 1 | $930.83 | $2.01 | $+1.08 | $9,310.34 | ▲ +1.08 after sell → book $11,589.26; vs 09:30 mark -2.01 | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 26 | $49.76 | $2.07 | — | $8,014.51 | — | S≥+5: 1.35× leftover; list flatten; 🔵; ⚪; ret5=+10.6; leftover $1330.05 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 32 | $41.31 | $2.09 | — | $6,690.51 | — | S≥+5: 1.35× leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+0.1; leftover $1330.05 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 406 | $3.27 | $5.24 | — | $5,357.65 | — | S≥+5: 1.35× leftover; list flatten; 🔵; ⚪; ret5=+13.8; leftover $1330.05 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 172 | $7.70 | $2.51 | — | $4,030.74 | — | S≥+5: 1.35× leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+4.7; leftover $1330.05 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 10 | $125.94 | $2.02 | — | $2,769.32 | — | S≥+5: 1.35× leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $1330.05 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `GPRO` | 1090 | $1.22 | $14.06 | — | $1,425.46 | — | S≥+5: 1.35× leftover; list probable,yday_gainer,yday_mover; ret5=+5.9; leftover $1330.05 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FRVO` | 72 | $18.40 | $2.21 | — | $98.46 | — | S≥+5: 1.35× leftover; list probable,yday_gainer,yday_mover; ret5=-14.4; leftover $1330.05 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $98.46 | ▲ 09:30 equity $12,524.82 vs yday $12,361.77 (+163.05) | 09:30 open · cash $98.46 (unchanged overnight, no fees) · equity $12,524.82 vs prior close $12,361.77 (+163.05) because holdings re-marked: CRK×101 yday $15.54 → 09:30 $15.45 -9.09; ANF×1 yday $136.60 → 09:30 $137.70 +1.10; BHVN×11 yday $15.69 → 09:30 $15.89 +2.20; BZ×10 yday $17.30 → 09:30 $17.31 +0.10; CAPR×20 yday $9.89 → 09:30 $9.83 -1.20; ATRC×26 yday $52.59 → 09:30 $52.88 +7.54; HRMY×32 yday $42.86 → 09:30 $42.93 +2.24; CABA×406 yday $3.57 → 09:30 $3.63 +24.36; VSTM×172 yday $8.02 → 09:30 $8.03 +1.72; RVTY×10 yday $130.94 → 09:30 $132.45 +15.10; GPRO×1090 yday $1.69 → 09:30 $1.78 +98.10; FRVO×72 yday $17.98 → 09:30 $18.27 +20.88 | — |
| 2026-09-04 09:30 ET | **SELL** | `CRK` | 101 | $15.45 | $2.32 | $+132.74 | $1,656.58 | ▲ +132.74 after sell → book $12,522.49; vs 09:30 mark -2.33 | dropped from list after 6 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `ANF` | 1 | $137.70 | $1.40 | $-9.85 | $1,792.88 | ▼ -9.85 after sell → book $12,521.09; vs 09:30 mark -1.40 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `BHVN` | 11 | $15.89 | $1.80 | $-15.36 | $1,965.87 | ▼ -15.36 after sell → book $12,519.29; vs 09:30 mark -1.80 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `BZ` | 10 | $17.31 | $1.78 | $-15.56 | $2,137.19 | ▼ -15.56 after sell → book $12,517.51; vs 09:30 mark -1.78 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `CAPR` | 20 | $9.83 | $2.05 | $+8.86 | $2,331.75 | ▲ +8.86 after sell → book $12,515.47; vs 09:30 mark -2.04 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **BUY** | `ASND` | 1 | $266.94 | $1.99 | — | $2,062.81 | — | S≥+5: 1.35× leftover; list flatten; ret5=+1.9; leftover $466.35 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OSCR` | 15 | $30.65 | $2.04 | — | $1,601.03 | — | S≥+5: 1.35× leftover; list flatten; 🔵; ret5=-2.2; leftover $466.35 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `NVAX` | 44 | $10.41 | $2.12 | — | $1,140.87 | — | S≥+5: 1.35× leftover; list flatten,ohlc_hot; ⚪; ret5=+11.1; leftover $466.35 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BVS` | 32 | $14.50 | $2.09 | — | $674.78 | — | S≥+5: 1.35× leftover; list flatten; 🔵; ⚪; ret5=+0.8; leftover $466.35 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 239 | $1.95 | $3.08 | — | $205.65 | — | S≥+5: 1.35× leftover; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $466.35 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |

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
| 2026-08-14 | `TLN` | cash | leftover split 12.19 < 1 share @ 359.83 |
| 2026-08-14 | `VST` | cash | leftover split 12.19 < 1 share @ 146.90 |
| 2026-08-14 | `NRG` | cash | leftover split 12.19 < 1 share @ 120.00 |
| 2026-08-14 | `DAVE` | cash | leftover split 12.19 < 1 share @ 330.91 |
| 2026-08-14 | `SLG` | cash | leftover split 12.19 < 1 share @ 57.61 |
| 2026-08-17 | `BTSG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `IREN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `TPG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `TGTX` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `SLS` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `HIMS` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `INO` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `TNDM` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `MARA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `LDI` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `BTBT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `DVN` | cash | leftover split 7.99 < 1 share @ 46.18 |
| 2026-08-17 | `EOG` | cash | leftover split 7.99 < 1 share @ 142.77 |
| 2026-08-17 | `FANG` | cash | leftover split 7.99 < 1 share @ 202.70 |
| 2026-08-17 | `TGB` | cash | leftover split 7.99 < 1 share @ 8.46 |
| 2026-08-17 | `ELF` | cash | leftover split 7.99 < 1 share @ 90.54 |
| 2026-08-18 | `BTSG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `IREN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `TPG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `TGTX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `SLS` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `HIMS` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `INO` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `TNDM` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `MARA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `LDI` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `BTBT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `TMC` | min_hold | dropped but min-hold 1/5 sess — no sell |
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
| 2026-08-19 | `MARA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `LDI` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `BTBT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `TMC` | min_hold | dropped but min-hold 2/5 sess — no sell |
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
| 2026-08-20 | `TMC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-20 | `DNN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-20 | `HNST` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-21 | `TMC` | min_hold | dropped but min-hold 4/5 sess — no sell |
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
| 2026-08-21 | `AU` | cash | leftover split 26.95 < 1 share @ 119.43 |
| 2026-08-21 | `AEM` | cash | leftover split 26.95 < 1 share @ 216.30 |
| 2026-08-21 | `CRSP` | cash | leftover split 26.95 < 1 share @ 59.72 |
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
| 2026-08-25 | `MOS` | cash | leftover split 14.38 < 1 share @ 24.00 |
| 2026-08-25 | `INSP` | cash | leftover split 14.38 < 1 share @ 61.47 |
| 2026-08-25 | `HCA` | cash | leftover split 14.38 < 1 share @ 429.24 |
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
| `ATRC` | 26 | 2026-09-03 @ $49.76 | S≥+5: 1.35× leftover; list flatten; 🔵; ⚪; ret5=+10.6; leftover $1330.05 |
| `HRMY` | 32 | 2026-09-03 @ $41.31 | S≥+5: 1.35× leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+0.1; leftover $1330.05 |
| `CABA` | 406 | 2026-09-03 @ $3.27 | S≥+5: 1.35× leftover; list flatten; 🔵; ⚪; ret5=+13.8; leftover $1330.05 |
| `VSTM` | 172 | 2026-09-03 @ $7.70 | S≥+5: 1.35× leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+4.7; leftover $1330.05 |
| `RVTY` | 10 | 2026-09-03 @ $125.94 | S≥+5: 1.35× leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $1330.05 |
| `GPRO` | 1090 | 2026-09-03 @ $1.22 | S≥+5: 1.35× leftover; list probable,yday_gainer,yday_mover; ret5=+5.9; leftover $1330.05 |
| `FRVO` | 72 | 2026-09-03 @ $18.40 | S≥+5: 1.35× leftover; list probable,yday_gainer,yday_mover; ret5=-14.4; leftover $1330.05 |
| `ASND` | 1 | 2026-09-04 @ $266.94 | S≥+5: 1.35× leftover; list flatten; ret5=+1.9; leftover $466.35 |
| `OSCR` | 15 | 2026-09-04 @ $30.65 | S≥+5: 1.35× leftover; list flatten; 🔵; ret5=-2.2; leftover $466.35 |
| `NVAX` | 44 | 2026-09-04 @ $10.41 | S≥+5: 1.35× leftover; list flatten,ohlc_hot; ⚪; ret5=+11.1; leftover $466.35 |
| `BVS` | 32 | 2026-09-04 @ $14.50 | S≥+5: 1.35× leftover; list flatten; 🔵; ⚪; ret5=+0.8; leftover $466.35 |
| `BAK` | 239 | 2026-09-04 @ $1.95 | S≥+5: 1.35× leftover; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $466.35 |
