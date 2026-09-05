# Factor mine action — `union_h5_exit_alarm`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **5** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · hold 5d, sell next 09:30 if 🚨

Cash book **+13.73%** ($11,374) · signal-only (no cash/fees) was +61.86%. Starts YES **14/17**. Fills 110 · skips 240 · realized $+1034.08.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **5**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $119.21.

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | vs yday close | Bought | Sold | Close cash | Close equity | Close held | 09:30 why |
|---|---:|---:|---|---:|---:|---|---|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | BTSG, IREN, TPG, TGTX, SLS, HIMS, INO, TNDM | — | $97.53 | $10,153.12 | BTSG×20, IREN×27, TPG×24, TGTX×25, SLS×106, HIMS×42, INO×1543, TNDM×53 | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-14 | +5.50 | $97.53 | BTSG×20, IREN×27, TPG×24, TGTX×25, SLS×106, HIMS×42, INO×1543, TNDM×53 | $10,178.12 | +25.00 | MARA, LDI, BTBT | — | $63.95 | $10,435.42 | BTSG×20, IREN×27, TPG×24, TGTX×25, SLS×106, HIMS×42, INO×1543, TNDM×53, MARA×1, LDI×13, BTBT×8 | 09:30 open · cash $97.53 (unchanged overnight, no fees) · equity $10,178.12 vs prior close $10,153.12 (+25.00) because holdings re-marked: BTSG×20 yday $60.23 → 09:30 $59.65 -11.60; IREN×27 yday $44.76 → 09:30 $44.09 -18.09; TPG×24 yday $54.62 → 09:30 $55.29 +16.08; TGTX×25 yday $47.94 → 09:30 $47.27 -16.75; SLS×106 yday $12.36 → 09:30 $12.40 +4.24; HIMS×42 yday $28.77 → 09:30 $29.15 +15.96; INO×1543 yday $0.90 → 09:30 $0.93 +46.29; TNDM×53 yday $23.13 → 09:30 $22.92 -11.13 |
| 2026-08-17 | +2.25 | $63.95 | BTSG×20, IREN×27, TPG×24, TGTX×25, SLS×106, HIMS×42, INO×1543, TNDM×53, MARA×1, LDI×13, BTBT×8 | $10,414.78 | -20.64 | DVN, EOG, FANG, TMC, TGB, ELF, DNN, NB | INO | $127.33 | $10,364.63 | BTSG×20, IREN×27, TPG×24, TGTX×25, SLS×106, HIMS×42, TNDM×53, MARA×1, LDI×13, BTBT×8, DVN×4, EOG×1, FANG×1, TMC×52, TGB×25, ELF×2, DNN×65, NB×41 | 09:30 open · cash $63.95 (unchanged overnight, no fees) · equity $10,414.78 vs prior close $10,435.42 (-20.64) because holdings re-marked: BTSG×20 yday $61.71 → 09:30 $61.69 -0.40; IREN×27 yday $44.06 → 09:30 $45.23 +31.59; TPG×24 yday $53.03 → 09:30 $52.67 -8.64; TGTX×25 yday $48.74 → 09:30 $48.74 +0.00; SLS×106 yday $12.78 → 09:30 $12.78 +0.00; HIMS×42 yday $28.15 → 09:30 $28.14 -0.42; INO×1543 yday $1.09 → 09:30 $1.07 -30.86; TNDM×53 yday $22.72 → 09:30 $22.50 -11.66; MARA×1 yday $9.20 → 09:30 $9.22 +0.02; LDI×13 yday $0.90 → 09:30 $0.91 +0.13; BTBT×8 yday $1.57 → 09:30 $1.52 -0.40 |
| 2026-08-18 | -6.20 | $127.33 | BTSG×20, IREN×27, TPG×24, TGTX×25, SLS×106, HIMS×42, TNDM×53, MARA×1, LDI×13, BTBT×8, DVN×4, EOG×1, FANG×1, TMC×52, TGB×25, ELF×2, DNN×65, NB×41 | $10,233.31 | -131.32 | — | — | $127.33 | $10,323.08 | BTSG×20, IREN×27, TPG×24, TGTX×25, SLS×106, HIMS×42, TNDM×53, MARA×1, LDI×13, BTBT×8, DVN×4, EOG×1, FANG×1, TMC×52, TGB×25, ELF×2, DNN×65, NB×41 | 09:30 open · cash $127.33 (unchanged overnight, no fees) · equity $10,233.31 vs prior close $10,364.63 (-131.32) because holdings re-marked: BTSG×20 yday $60.38 → 09:30 $60.00 -7.60; IREN×27 yday $44.90 → 09:30 $43.56 -36.18; TPG×24 yday $51.77 → 09:30 $51.77 +0.00; TGTX×25 yday $49.28 → 09:30 $49.28 +0.00; SLS×106 yday $13.00 → 09:30 $12.66 -36.04; HIMS×42 yday $28.61 → 09:30 $27.85 -31.92; TNDM×53 yday $22.25 → 09:30 $22.16 -5.03; MARA×1 yday $9.72 → 09:30 $9.36 -0.36; LDI×13 yday $0.88 → 09:30 $0.87 -0.07; BTBT×8 yday $1.60 → 09:30 $1.54 -0.48; DVN×4 yday $47.57 → 09:30 $48.00 +1.72; EOG×1 yday $146.15 → 09:30 $148.04 +1.89; FANG×1 yday $206.29 → 09:30 $208.93 +2.64; TMC×52 yday $3.77 → 09:30 $3.72 -2.60; TGB×25 yday $8.77 → 09:30 $8.55 -5.50; ELF×2 yday $93.66 → 09:30 $93.44 -0.44; DNN×65 yday $3.19 → 09:30 $3.11 -5.20; NB×41 yday $4.81 → 09:30 $4.66 -6.15 |
| 2026-08-19 | -7.20 | $127.33 | BTSG×20, IREN×27, TPG×24, TGTX×25, SLS×106, HIMS×42, TNDM×53, MARA×1, LDI×13, BTBT×8, DVN×4, EOG×1, FANG×1, TMC×52, TGB×25, ELF×2, DNN×65, NB×41 | $10,454.84 | +131.76 | — | — | $127.33 | $10,649.12 | BTSG×20, IREN×27, TPG×24, TGTX×25, SLS×106, HIMS×42, TNDM×53, MARA×1, LDI×13, BTBT×8, DVN×4, EOG×1, FANG×1, TMC×52, TGB×25, ELF×2, DNN×65, NB×41 | 09:30 open · cash $127.33 (unchanged overnight, no fees) · equity $10,454.84 vs prior close $10,323.08 (+131.76) because holdings re-marked: BTSG×20 yday $59.50 → 09:30 $60.15 +13.00; IREN×27 yday $42.00 → 09:30 $41.41 -15.80; TPG×24 yday $52.02 → 09:30 $52.26 +5.76; TGTX×25 yday $50.26 → 09:30 $51.62 +34.00; SLS×106 yday $13.10 → 09:30 $13.46 +38.16; HIMS×42 yday $27.39 → 09:30 $27.55 +6.72; TNDM×53 yday $23.73 → 09:30 $24.20 +24.91; MARA×1 yday $8.96 → 09:30 $8.91 -0.05; LDI×13 yday $0.86 → 09:30 $0.88 +0.29; BTBT×8 yday $1.45 → 09:30 $1.42 -0.24; DVN×4 yday $47.83 → 09:30 $48.22 +1.56; EOG×1 yday $148.70 → 09:30 $149.86 +1.16; FANG×1 yday $210.02 → 09:30 $210.84 +0.82; TMC×52 yday $3.92 → 09:30 $3.93 +0.52; TGB×25 yday $8.36 → 09:30 $8.70 +8.50; ELF×2 yday $92.51 → 09:30 $96.00 +6.98; DNN×65 yday $3.15 → 09:30 $3.19 +2.60; NB×41 yday $4.53 → 09:30 $4.60 +2.87 |
| 2026-08-20 | +1.12 | $127.33 | BTSG×20, IREN×27, TPG×24, TGTX×25, SLS×106, HIMS×42, TNDM×53, MARA×1, LDI×13, BTBT×8, DVN×4, EOG×1, FANG×1, TMC×52, TGB×25, ELF×2, DNN×65, NB×41 | $10,584.39 | -64.73 | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | BTSG, IREN, TPG, TGTX, SLS, HIMS, TNDM | $167.37 | $10,756.85 | MARA×1, LDI×13, BTBT×8, DVN×4, EOG×1, FANG×1, TMC×52, TGB×25, ELF×2, DNN×65, NB×41, AG×54, BHP×12, CDE×54, HDSN×194, IAG×57, KGC×37, NFGC×641, WPM×7 | 09:30 open · cash $127.33 (unchanged overnight, no fees) · equity $10,584.39 vs prior close $10,649.12 (-64.73) because holdings re-marked: BTSG×20 yday $59.33 → 09:30 $58.64 -13.80; IREN×27 yday $42.84 → 09:30 $42.46 -10.26; TPG×24 yday $53.18 → 09:30 $53.06 -2.88; TGTX×25 yday $51.69 → 09:30 $51.65 -1.00; SLS×106 yday $13.85 → 09:30 $13.84 -1.06; HIMS×42 yday $31.09 → 09:30 $30.66 -18.06; TNDM×53 yday $23.46 → 09:30 $23.11 -18.55; MARA×1 yday $9.65 → 09:30 $10.21 +0.56; LDI×13 yday $0.88 → 09:30 $0.87 -0.07; BTBT×8 yday $1.40 → 09:30 $1.46 +0.44; DVN×4 yday $48.19 → 09:30 $49.02 +3.32; EOG×1 yday $149.48 → 09:30 $151.45 +1.97; FANG×1 yday $208.55 → 09:30 $213.51 +4.96; TMC×52 yday $3.97 → 09:30 $3.92 -2.60; TGB×25 yday $8.47 → 09:30 $8.35 -3.00; ELF×2 yday $99.65 → 09:30 $98.15 -3.00; DNN×65 yday $3.22 → 09:30 $3.20 -1.30; NB×41 yday $4.46 → 09:30 $4.45 -0.41 |
| 2026-08-21 | +3.25 | $167.37 | MARA×1, LDI×13, BTBT×8, DVN×4, EOG×1, FANG×1, TMC×52, TGB×25, ELF×2, DNN×65, NB×41, AG×54, BHP×12, CDE×54, HDSN×194, IAG×57, KGC×37, NFGC×641, WPM×7 | $11,026.36 | +269.51 | AUPH, ARCT, AUTL, CRDL, CYPH | MARA, LDI, BTBT | $87.52 | $11,095.77 | DVN×4, EOG×1, FANG×1, TMC×52, TGB×25, ELF×2, DNN×65, NB×41, AG×54, BHP×12, CDE×54, HDSN×194, IAG×57, KGC×37, NFGC×641, WPM×7, AUPH×1, ARCT×2, AUTL×10, CRDL×13, CYPH×19 | 09:30 open · cash $167.37 (unchanged overnight, no fees) · equity $11,026.36 vs prior close $10,756.85 (+269.51) because holdings re-marked: MARA×1 yday $11.15 → 09:30 $11.70 +0.55; LDI×13 yday $0.87 → 09:30 $0.87 -0.04; BTBT×8 yday $1.59 → 09:30 $1.66 +0.52; DVN×4 yday $49.30 → 09:30 $49.45 +0.60; EOG×1 yday $152.19 → 09:30 $152.29 +0.10; FANG×1 yday $211.02 → 09:30 $211.84 +0.82; TMC×52 yday $3.97 → 09:30 $4.10 +6.76; TGB×25 yday $8.69 → 09:30 $9.00 +7.75; ELF×2 yday $98.46 → 09:30 $99.02 +1.12; DNN×65 yday $3.14 → 09:30 $3.23 +5.85; NB×41 yday $4.29 → 09:30 $4.43 +5.74; AG×54 yday $21.19 → 09:30 $21.90 +38.34; BHP×12 yday $93.63 → 09:30 $95.72 +25.08; CDE×54 yday $21.11 → 09:30 $21.75 +34.56; HDSN×194 yday $5.57 → 09:30 $5.67 +19.40; IAG×57 yday $20.50 → 09:30 $21.17 +38.19; KGC×37 yday $31.43 → 09:30 $32.17 +27.38; NFGC×641 yday $1.75 → 09:30 $1.79 +25.64; WPM×7 yday $150.25 → 09:30 $154.70 +31.15 |
| 2026-08-24 | -5.17 | $87.52 | DVN×4, EOG×1, FANG×1, TMC×52, TGB×25, ELF×2, DNN×65, NB×41, AG×54, BHP×12, CDE×54, HDSN×194, IAG×57, KGC×37, NFGC×641, WPM×7, AUPH×1, ARCT×2, AUTL×10, CRDL×13, CYPH×19 | $11,192.61 | +96.84 | — | DVN, EOG, FANG, TMC, TGB, ELF, NB, WPM, ARCT | $2,627.21 | $11,046.82 | DNN×65, AG×54, BHP×12, CDE×54, HDSN×194, IAG×57, KGC×37, NFGC×641, AUPH×1, AUTL×10, CRDL×13, CYPH×19 | 09:30 open · cash $87.52 (unchanged overnight, no fees) · equity $11,192.61 vs prior close $11,095.77 (+96.84) because holdings re-marked: DVN×4 yday $49.10 → 09:30 $48.84 -1.04; EOG×1 yday $153.05 → 09:30 $152.61 -0.44; FANG×1 yday $210.72 → 09:30 $209.47 -1.25; TMC×52 yday $4.79 → 09:30 $4.57 -11.44; TGB×25 yday $9.19 → 09:30 $9.26 +1.75; ELF×2 yday $101.94 → 09:30 $101.53 -0.82; DNN×65 yday $3.50 → 09:30 $3.50 +0.00; NB×41 yday $4.64 → 09:30 $4.56 -3.28; AG×54 yday $21.09 → 09:30 $21.47 +20.52; BHP×12 yday $97.03 → 09:30 $97.34 +3.72; CDE×54 yday $20.97 → 09:30 $21.26 +15.66; HDSN×194 yday $5.63 → 09:30 $5.69 +11.64; IAG×57 yday $21.14 → 09:30 $21.44 +17.10; KGC×37 yday $32.76 → 09:30 $33.21 +16.65; NFGC×641 yday $1.84 → 09:30 $1.86 +12.82; WPM×7 yday $157.78 → 09:30 $158.96 +8.26; AUPH×1 yday $16.65 → 09:30 $16.60 -0.05; ARCT×2 yday $13.45 → 09:30 $13.26 -0.38; AUTL×10 yday $2.41 → 09:30 $2.36 -0.50; CRDL×13 yday $1.86 → 09:30 $1.87 +0.13; CYPH×19 yday $1.42 → 09:30 $1.83 +7.79 |
| 2026-08-25 | +1.80 | $2,627.21 | DNN×65, AG×54, BHP×12, CDE×54, HDSN×194, IAG×57, KGC×37, NFGC×641, AUPH×1, AUTL×10, CRDL×13, CYPH×19 | $11,093.10 | +46.28 | MOS, OCUL, INSP, CRMD, RZLT, BMEA, NPWR | DNN | $422.44 | $11,036.55 | AG×54, BHP×12, CDE×54, HDSN×194, IAG×57, KGC×37, NFGC×641, AUPH×1, AUTL×10, CRDL×13, CYPH×19, MOS×14, OCUL×32, INSP×5, CRMD×43, RZLT×68, BMEA×220, NPWR×178 | 09:30 open · cash $2,627.21 (unchanged overnight, no fees) · equity $11,093.10 vs prior close $11,046.82 (+46.28) because holdings re-marked: DNN×65 yday $3.54 → 09:30 $3.54 +0.00; AG×54 yday $20.57 → 09:30 $20.73 +8.64; BHP×12 yday $96.66 → 09:30 $95.95 -8.52; CDE×54 yday $20.49 → 09:30 $20.85 +19.44; HDSN×194 yday $5.57 → 09:30 $5.53 -7.76; IAG×57 yday $21.36 → 09:30 $21.63 +15.39; KGC×37 yday $32.47 → 09:30 $32.76 +10.73; NFGC×641 yday $1.90 → 09:30 $1.91 +6.41; AUPH×1 yday $16.60 → 09:30 $16.71 +0.11; AUTL×10 yday $2.38 → 09:30 $2.32 -0.60; CRDL×13 yday $1.80 → 09:30 $1.90 +1.30; CYPH×19 yday $1.64 → 09:30 $1.70 +1.14 |
| 2026-08-26 | +2.02 | $422.44 | AG×54, BHP×12, CDE×54, HDSN×194, IAG×57, KGC×37, NFGC×641, AUPH×1, AUTL×10, CRDL×13, CYPH×19, MOS×14, OCUL×32, INSP×5, CRMD×43, RZLT×68, BMEA×220, NPWR×178 | $11,036.55 | -0.00 | — | — | $422.44 | $11,075.10 | AG×54, BHP×12, CDE×54, HDSN×194, IAG×57, KGC×37, NFGC×641, AUPH×1, AUTL×10, CRDL×13, CYPH×19, MOS×14, OCUL×32, INSP×5, CRMD×43, RZLT×68, BMEA×220, NPWR×178 | 09:30 open · cash $422.44 (unchanged overnight, no fees) · equity $11,036.55 vs prior close $11,036.55 (-0.00) because holdings re-marked: AG×54 yday $20.68 → 09:30 $20.68 +0.00; BHP×12 yday $96.05 → 09:30 $96.05 +0.00; CDE×54 yday $20.71 → 09:30 $20.71 +0.00; HDSN×194 yday $5.49 → 09:30 $5.49 +0.00; IAG×57 yday $21.48 → 09:30 $21.48 +0.00; KGC×37 yday $32.55 → 09:30 $32.55 +0.00; NFGC×641 yday $1.90 → 09:30 $1.90 +0.00; AUPH×1 yday $16.71 → 09:30 $16.71 +0.00; AUTL×10 yday $2.34 → 09:30 $2.34 +0.00; CRDL×13 yday $1.90 → 09:30 $1.90 +0.00; CYPH×19 yday $1.64 → 09:30 $1.64 +0.00; MOS×14 yday $23.75 → 09:30 $23.75 +0.00; OCUL×32 yday $10.92 → 09:30 $10.92 +0.00; INSP×5 yday $61.47 → 09:30 $61.47 +0.00; CRMD×43 yday $8.28 → 09:30 $8.28 +0.00; RZLT×68 yday $5.29 → 09:30 $5.29 +0.00; BMEA×220 yday $1.61 → 09:30 $1.61 +0.00; NPWR×178 yday $2.02 → 09:30 $2.02 +0.00 |
| 2026-08-27 | — | $422.44 | AG×54, BHP×12, CDE×54, HDSN×194, IAG×57, KGC×37, NFGC×641, AUPH×1, AUTL×10, CRDL×13, CYPH×19, MOS×14, OCUL×32, INSP×5, CRMD×43, RZLT×68, BMEA×220, NPWR×178 | $11,165.96 | +90.86 | RRC, CRK, SLI, ACMR, GGB, MT, MU | AG, BHP, CDE, HDSN, IAG, KGC, NFGC | $345.61 | $11,143.15 | AUPH×1, AUTL×10, CRDL×13, CYPH×19, MOS×14, OCUL×32, INSP×5, CRMD×43, RZLT×68, BMEA×220, NPWR×178, RRC×30, CRK×87, SLI×475, ACMR×15, GGB×278, MT×16, MU×1 | 09:30 open · cash $422.44 (unchanged overnight, no fees) · equity $11,165.96 vs prior close $11,075.10 (+90.86) because holdings re-marked: AG×54 yday $20.68 → 09:30 $20.63 -2.70; BHP×12 yday $96.05 → 09:30 $96.99 +11.28; CDE×54 yday $20.71 → 09:30 $21.00 +15.66; HDSN×194 yday $5.49 → 09:30 $5.51 +3.88; IAG×57 yday $21.48 → 09:30 $21.64 +9.12; KGC×37 yday $32.55 → 09:30 $32.90 +12.95; NFGC×641 yday $1.90 → 09:30 $2.00 +64.10; AUPH×1 yday $16.71 → 09:30 $16.60 -0.11; AUTL×10 yday $2.34 → 09:30 $2.41 +0.70; CRDL×13 yday $1.90 → 09:30 $2.03 +1.69; CYPH×19 yday $1.64 → 09:30 $1.60 -0.76; MOS×14 yday $23.75 → 09:30 $24.84 +15.26; OCUL×32 yday $10.92 → 09:30 $10.79 -4.16; INSP×5 yday $61.47 → 09:30 $60.07 -7.00; CRMD×43 yday $8.28 → 09:30 $8.60 +13.76; RZLT×68 yday $5.29 → 09:30 $5.01 -19.04; BMEA×220 yday $1.61 → 09:30 $1.75 +30.80; NPWR×178 yday $2.02 → 09:30 $1.93 -16.02 |
| 2026-08-28 | +0.75 | $345.61 | AUPH×1, AUTL×10, CRDL×13, CYPH×19, MOS×14, OCUL×32, INSP×5, CRMD×43, RZLT×68, BMEA×220, NPWR×178, RRC×30, CRK×87, SLI×475, ACMR×15, GGB×278, MT×16, MU×1 | $11,238.33 | +95.18 | BHVN, BZ, CAPR | AUPH, AUTL, CRDL, CYPH | $118.21 | $11,247.17 | MOS×14, OCUL×32, INSP×5, CRMD×43, RZLT×68, BMEA×220, NPWR×178, RRC×30, CRK×87, SLI×475, ACMR×15, GGB×278, MT×16, MU×1, BHVN×6, BZ×6, CAPR×12 | 09:30 open · cash $345.61 (unchanged overnight, no fees) · equity $11,238.33 vs prior close $11,143.15 (+95.18) because holdings re-marked: AUPH×1 yday $16.54 → 09:30 $16.47 -0.07; AUTL×10 yday $2.33 → 09:30 $2.32 -0.10; CRDL×13 yday $2.14 → 09:30 $2.09 -0.65; CYPH×19 yday $1.63 → 09:30 $1.75 +2.28; MOS×14 yday $24.16 → 09:30 $24.00 -2.24; OCUL×32 yday $10.77 → 09:30 $10.63 -4.48; INSP×5 yday $61.80 → 09:30 $62.10 +1.50; CRMD×43 yday $8.39 → 09:30 $8.49 +4.30; RZLT×68 yday $5.04 → 09:30 $5.07 +2.04; BMEA×220 yday $1.71 → 09:30 $1.74 +6.60; NPWR×178 yday $1.81 → 09:30 $1.83 +3.56; RRC×30 yday $41.55 → 09:30 $41.44 -3.30; CRK×87 yday $14.50 → 09:30 $14.42 -6.96; SLI×475 yday $2.61 → 09:30 $2.60 -4.75; ACMR×15 yday $79.11 → 09:30 $81.65 +38.10; GGB×278 yday $4.46 → 09:30 $4.57 +30.58; MT×16 yday $74.53 → 09:30 $74.54 +0.16; MU×1 yday $938.40 → 09:30 $967.01 +28.61 |
| 2026-08-31 | -5.85 | $118.21 | MOS×14, OCUL×32, INSP×5, CRMD×43, RZLT×68, BMEA×220, NPWR×178, RRC×30, CRK×87, SLI×475, ACMR×15, GGB×278, MT×16, MU×1, BHVN×6, BZ×6, CAPR×12 | $10,993.06 | -254.11 | — | — | $118.21 | $11,005.38 | MOS×14, OCUL×32, INSP×5, CRMD×43, RZLT×68, BMEA×220, NPWR×178, RRC×30, CRK×87, SLI×475, ACMR×15, GGB×278, MT×16, MU×1, BHVN×6, BZ×6, CAPR×12 | 09:30 open · cash $118.21 (unchanged overnight, no fees) · equity $10,993.06 vs prior close $11,247.17 (-254.11) because holdings re-marked: MOS×14 yday $23.76 → 09:30 $23.75 -0.14; OCUL×32 yday $10.82 → 09:30 $10.36 -14.72; INSP×5 yday $60.82 → 09:30 $61.44 +3.10; CRMD×43 yday $8.31 → 09:30 $8.29 -0.86; RZLT×68 yday $4.98 → 09:30 $4.62 -24.48; BMEA×220 yday $1.68 → 09:30 $1.71 +6.60; NPWR×178 yday $1.89 → 09:30 $1.83 -10.68; RRC×30 yday $41.64 → 09:30 $41.11 -15.90; CRK×87 yday $14.62 → 09:30 $14.56 -5.22; SLI×475 yday $2.64 → 09:30 $2.51 -61.75; ACMR×15 yday $80.49 → 09:30 $75.10 -80.85; GGB×278 yday $4.70 → 09:30 $4.55 -41.70; MT×16 yday $74.63 → 09:30 $75.07 +7.04; MU×1 yday $935.39 → 09:30 $933.01 -2.38; BHVN×6 yday $16.12 → 09:30 $15.44 -4.08; BZ×6 yday $18.00 → 09:30 $17.89 -0.66; CAPR×12 yday $10.06 → 09:30 $9.44 -7.44 |
| 2026-09-01 | -6.30 | $118.21 | MOS×14, OCUL×32, INSP×5, CRMD×43, RZLT×68, BMEA×220, NPWR×178, RRC×30, CRK×87, SLI×475, ACMR×15, GGB×278, MT×16, MU×1, BHVN×6, BZ×6, CAPR×12 | $11,043.16 | +37.78 | — | MOS, OCUL, INSP, CRMD, RZLT, BMEA, NPWR | $2,443.09 | $11,051.27 | RRC×30, CRK×87, SLI×475, ACMR×15, GGB×278, MT×16, MU×1, BHVN×6, BZ×6, CAPR×12 | 09:30 open · cash $118.21 (unchanged overnight, no fees) · equity $11,043.16 vs prior close $11,005.38 (+37.78) because holdings re-marked: MOS×14 yday $23.78 → 09:30 $24.00 +3.08; OCUL×32 yday $10.36 → 09:30 $10.49 +4.16; INSP×5 yday $61.44 → 09:30 $63.05 +8.05; CRMD×43 yday $8.30 → 09:30 $8.26 -1.72; RZLT×68 yday $4.62 → 09:30 $4.69 +4.76; BMEA×220 yday $1.71 → 09:30 $1.65 -13.20; NPWR×178 yday $1.82 → 09:30 $1.78 -7.12; RRC×30 yday $41.78 → 09:30 $41.32 -13.80; CRK×87 yday $14.51 → 09:30 $14.31 -17.40; SLI×475 yday $2.51 → 09:30 $2.70 +90.25; ACMR×15 yday $75.02 → 09:30 $71.24 -56.70; GGB×278 yday $4.55 → 09:30 $4.61 +16.68; MT×16 yday $75.06 → 09:30 $74.31 -12.00; MU×1 yday $933.01 → 09:30 $955.79 +22.78; BHVN×6 yday $15.40 → 09:30 $15.45 +0.30; BZ×6 yday $17.90 → 09:30 $17.37 -3.18; CAPR×12 yday $9.36 → 09:30 $10.43 +12.84 |
| 2026-09-02 | -3.83 | $2,443.09 | RRC×30, CRK×87, SLI×475, ACMR×15, GGB×278, MT×16, MU×1, BHVN×6, BZ×6, CAPR×12 | $11,125.90 | +74.63 | — | — | $2,443.09 | $11,072.26 | RRC×30, CRK×87, SLI×475, ACMR×15, GGB×278, MT×16, MU×1, BHVN×6, BZ×6, CAPR×12 | 09:30 open · cash $2,443.09 (unchanged overnight, no fees) · equity $11,125.90 vs prior close $11,051.27 (+74.63) because holdings re-marked: RRC×30 yday $41.32 → 09:30 $41.94 +18.60; CRK×87 yday $14.90 → 09:30 $15.82 +80.04; SLI×475 yday $2.70 → 09:30 $2.67 -14.25; ACMR×15 yday $71.88 → 09:30 $71.44 -6.60; GGB×278 yday $4.61 → 09:30 $4.57 -11.12; MT×16 yday $73.25 → 09:30 $73.22 -0.48; MU×1 yday $940.00 → 09:30 $941.12 +1.12; BHVN×6 yday $15.45 → 09:30 $15.39 -0.36; BZ×6 yday $17.17 → 09:30 $17.29 +0.72; CAPR×12 yday $10.19 → 09:30 $10.77 +6.96 |
| 2026-09-03 | -0.90 | $2,443.09 | RRC×30, CRK×87, SLI×475, ACMR×15, GGB×278, MT×16, MU×1, BHVN×6, BZ×6, CAPR×12 | $11,084.87 | +12.61 | ATRC, HRMY, CABA, VSTM, RVTY, GPRO, FRVO | RRC, SLI, ACMR, GGB, MT, MU | $139.02 | $11,853.65 | CRK×87, BHVN×6, BZ×6, CAPR×12, ATRC×26, HRMY×32, CABA×409, VSTM×173, RVTY×10, GPRO×1098, FRVO×72 | 09:30 open · cash $2,443.09 (unchanged overnight, no fees) · equity $11,084.87 vs prior close $11,072.26 (+12.61) because holdings re-marked: RRC×30 yday $42.40 → 09:30 $42.10 -9.00; CRK×87 yday $16.02 → 09:30 $15.70 -27.84; SLI×475 yday $2.49 → 09:30 $2.49 +0.00; ACMR×15 yday $70.04 → 09:30 $70.52 +7.20; GGB×278 yday $4.69 → 09:30 $4.81 +33.36; MT×16 yday $73.31 → 09:30 $73.86 +8.80; MU×1 yday $933.44 → 09:30 $930.83 -2.61; BHVN×6 yday $15.74 → 09:30 $15.97 +1.38; BZ×6 yday $17.55 → 09:30 $17.65 +0.60; CAPR×12 yday $10.01 → 09:30 $10.07 +0.72 |
| 2026-09-04 | — | $139.02 | CRK×87, BHVN×6, BZ×6, CAPR×12, ATRC×26, HRMY×32, CABA×409, VSTM×173, RVTY×10, GPRO×1098, FRVO×72 | $12,017.21 | +163.56 | ASND, OSCR, NVAX, BVS, BAK | CRK, BHVN, BZ, CAPR | $119.21 | $11,373.51 | ATRC×26, HRMY×32, CABA×409, VSTM×173, RVTY×10, GPRO×1098, FRVO×72, ASND×1, OSCR×11, NVAX×34, BVS×24, BAK×184 | 09:30 open · cash $139.02 (unchanged overnight, no fees) · equity $12,017.21 vs prior close $11,853.65 (+163.56) because holdings re-marked: CRK×87 yday $15.54 → 09:30 $15.45 -7.83; BHVN×6 yday $15.69 → 09:30 $15.89 +1.20; BZ×6 yday $17.30 → 09:30 $17.31 +0.06; CAPR×12 yday $9.89 → 09:30 $9.83 -0.72; ATRC×26 yday $52.59 → 09:30 $52.88 +7.54; HRMY×32 yday $42.86 → 09:30 $42.93 +2.24; CABA×409 yday $3.57 → 09:30 $3.63 +24.54; VSTM×173 yday $8.02 → 09:30 $8.03 +1.73; RVTY×10 yday $130.94 → 09:30 $132.45 +15.10; GPRO×1098 yday $1.69 → 09:30 $1.78 +98.82; FRVO×72 yday $17.98 → 09:30 $18.27 +20.88 |

## Fills (09:30 open snapshot, then buys / sells)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 20 | $59.80 | $2.05 | — | $8,801.95 | — | hold 5d, sell next 09:30 if 🚨; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 27 | $45.98 | $2.07 | — | $7,558.42 | — | hold 5d, sell next 09:30 if 🚨; list flatten; ⚪; ret5=+12.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 24 | $50.62 | $2.06 | — | $6,341.40 | — | hold 5d, sell next 09:30 if 🚨; list flatten; ⚪; ret5=+6.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TGTX` | 25 | $49.70 | $2.06 | — | $5,096.84 | — | hold 5d, sell next 09:30 if 🚨; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 106 | $11.70 | $2.31 | — | $3,854.33 | — | hold 5d, sell next 09:30 if 🚨; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 42 | $29.74 | $2.12 | — | $2,603.13 | — | hold 5d, sell next 09:30 if 🚨; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 1543 | $0.81 | $17.13 | — | $1,336.17 | — | hold 5d, sell next 09:30 if 🚨; list flatten; ⚪; ret5=+13.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 53 | $23.33 | $2.15 | — | $97.53 | — | hold 5d, sell next 09:30 if 🚨; list flatten; ⚪; ret5=+19.7; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $97.53 | ▲ 09:30 equity $10,178.12 vs yday $10,153.12 (+25.00) | 09:30 open · cash $97.53 (unchanged overnight, no fees) · equity $10,178.12 vs prior close $10,153.12 (+25.00) because holdings re-marked: BTSG×20 yday $60.23 → 09:30 $59.65 -11.60; IREN×27 yday $44.76 → 09:30 $44.09 -18.09; TPG×24 yday $54.62 → 09:30 $55.29 +16.08; TGTX×25 yday $47.94 → 09:30 $47.27 -16.75; SLS×106 yday $12.36 → 09:30 $12.40 +4.24; HIMS×42 yday $28.77 → 09:30 $29.15 +15.96; INO×1543 yday $0.90 → 09:30 $0.93 +46.29; TNDM×53 yday $23.13 → 09:30 $22.92 -11.13 | — |
| 2026-08-14 09:30 ET | **BUY** | `MARA` | 1 | $9.01 | $0.09 | — | $88.43 | — | hold 5d, sell next 09:30 if 🚨; list flatten; 🔵; ⚪; ret5=-13.5; leftover $12.19 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 13 | $0.94 | $0.16 | — | $76.09 | — | hold 5d, sell next 09:30 if 🚨; list flatten; 🔵; ⚪; ret5=+0.5; leftover $12.19 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 8 | $1.50 | $0.14 | — | $63.95 | — | hold 5d, sell next 09:30 if 🚨; list flatten; 🔵; ⚪; ret5=+9.2; leftover $12.19 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $63.95 | ▼ 09:30 equity $10,414.78 vs yday $10,435.42 (-20.64) | 09:30 open · cash $63.95 (unchanged overnight, no fees) · equity $10,414.78 vs prior close $10,435.42 (-20.64) because holdings re-marked: BTSG×20 yday $61.71 → 09:30 $61.69 -0.40; IREN×27 yday $44.06 → 09:30 $45.23 +31.59; TPG×24 yday $53.03 → 09:30 $52.67 -8.64; TGTX×25 yday $48.74 → 09:30 $48.74 +0.00; SLS×106 yday $12.78 → 09:30 $12.78 +0.00; HIMS×42 yday $28.15 → 09:30 $28.14 -0.42; INO×1543 yday $1.09 → 09:30 $1.07 -30.86; TNDM×53 yday $22.72 → 09:30 $22.50 -11.66; MARA×1 yday $9.20 → 09:30 $9.22 +0.02; LDI×13 yday $0.90 → 09:30 $0.91 +0.13; BTBT×8 yday $1.57 → 09:30 $1.52 -0.40 | — |
| 2026-08-17 09:30 ET | **SELL** | `INO` | 1543 | $1.07 | $20.17 | $+363.88 | $1,694.78 | ▲ +363.88 after sell → book $10,394.60; vs 09:30 mark -20.18 | exit 🚨 after 2 sess | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 4 | $46.18 | $1.86 | — | $1,508.20 | — | hold 5d, sell next 09:30 if 🚨; list flatten; 🔵; ret5=+6.7; leftover $211.85 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 1 | $142.77 | $1.43 | — | $1,364.00 | — | hold 5d, sell next 09:30 if 🚨; list flatten; 🔵; ret5=+5.8; leftover $211.85 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 1 | $202.70 | $1.99 | — | $1,159.31 | — | hold 5d, sell next 09:30 if 🚨; list flatten; 🔵; ret5=+8.3; leftover $211.85 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 52 | $4.05 | $2.15 | — | $946.56 | — | hold 5d, sell next 09:30 if 🚨; list flatten; 🔵; ⚪; ret5=-12.3; leftover $211.85 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `TGB` | 25 | $8.46 | $2.06 | — | $733.00 | — | hold 5d, sell next 09:30 if 🚨; list flatten; 🔵; ⚪; ret5=+0.4; leftover $211.85 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ELF` | 2 | $90.54 | $1.82 | — | $550.10 | — | hold 5d, sell next 09:30 if 🚨; list flatten; ret5=-7.2; leftover $211.85 | join🟢 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 65 | $3.24 | $2.19 | — | $337.32 | — | hold 5d, sell next 09:30 if 🚨; list flatten; ⚪; ret5=+0.3; leftover $211.85 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `NB` | 41 | $5.07 | $2.11 | — | $127.33 | — | hold 5d, sell next 09:30 if 🚨; list flatten; ret5=-4.7; leftover $211.85 | join🟢 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $127.33 | ▼ 09:30 equity $10,233.31 vs yday $10,364.63 (-131.32) | 09:30 open · cash $127.33 (unchanged overnight, no fees) · equity $10,233.31 vs prior close $10,364.63 (-131.32) because holdings re-marked: BTSG×20 yday $60.38 → 09:30 $60.00 -7.60; IREN×27 yday $44.90 → 09:30 $43.56 -36.18; TPG×24 yday $51.77 → 09:30 $51.77 +0.00; TGTX×25 yday $49.28 → 09:30 $49.28 +0.00; SLS×106 yday $13.00 → 09:30 $12.66 -36.04; HIMS×42 yday $28.61 → 09:30 $27.85 -31.92; TNDM×53 yday $22.25 → 09:30 $22.16 -5.03; MARA×1 yday $9.72 → 09:30 $9.36 -0.36; LDI×13 yday $0.88 → 09:30 $0.87 -0.07; BTBT×8 yday $1.60 → 09:30 $1.54 -0.48; DVN×4 yday $47.57 → 09:30 $48.00 +1.72; EOG×1 yday $146.15 → 09:30 $148.04 +1.89; FANG×1 yday $206.29 → 09:30 $208.93 +2.64; TMC×52 yday $3.77 → 09:30 $3.72 -2.60; TGB×25 yday $8.77 → 09:30 $8.55 -5.50; ELF×2 yday $93.66 → 09:30 $93.44 -0.44; DNN×65 yday $3.19 → 09:30 $3.11 -5.20; NB×41 yday $4.81 → 09:30 $4.66 -6.15 | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $127.33 | ▲ 09:30 equity $10,454.84 vs yday $10,323.08 (+131.76) | 09:30 open · cash $127.33 (unchanged overnight, no fees) · equity $10,454.84 vs prior close $10,323.08 (+131.76) because holdings re-marked: BTSG×20 yday $59.50 → 09:30 $60.15 +13.00; IREN×27 yday $42.00 → 09:30 $41.41 -15.80; TPG×24 yday $52.02 → 09:30 $52.26 +5.76; TGTX×25 yday $50.26 → 09:30 $51.62 +34.00; SLS×106 yday $13.10 → 09:30 $13.46 +38.16; HIMS×42 yday $27.39 → 09:30 $27.55 +6.72; TNDM×53 yday $23.73 → 09:30 $24.20 +24.91; MARA×1 yday $8.96 → 09:30 $8.91 -0.05; LDI×13 yday $0.86 → 09:30 $0.88 +0.29; BTBT×8 yday $1.45 → 09:30 $1.42 -0.24; DVN×4 yday $47.83 → 09:30 $48.22 +1.56; EOG×1 yday $148.70 → 09:30 $149.86 +1.16; FANG×1 yday $210.02 → 09:30 $210.84 +0.82; TMC×52 yday $3.92 → 09:30 $3.93 +0.52; TGB×25 yday $8.36 → 09:30 $8.70 +8.50; ELF×2 yday $92.51 → 09:30 $96.00 +6.98; DNN×65 yday $3.15 → 09:30 $3.19 +2.60; NB×41 yday $4.53 → 09:30 $4.60 +2.87 | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $127.33 | ▼ 09:30 equity $10,584.39 vs yday $10,649.12 (-64.73) | 09:30 open · cash $127.33 (unchanged overnight, no fees) · equity $10,584.39 vs prior close $10,649.12 (-64.73) because holdings re-marked: BTSG×20 yday $59.33 → 09:30 $58.64 -13.80; IREN×27 yday $42.84 → 09:30 $42.46 -10.26; TPG×24 yday $53.18 → 09:30 $53.06 -2.88; TGTX×25 yday $51.69 → 09:30 $51.65 -1.00; SLS×106 yday $13.85 → 09:30 $13.84 -1.06; HIMS×42 yday $31.09 → 09:30 $30.66 -18.06; TNDM×53 yday $23.46 → 09:30 $23.11 -18.55; MARA×1 yday $9.65 → 09:30 $10.21 +0.56; LDI×13 yday $0.88 → 09:30 $0.87 -0.07; BTBT×8 yday $1.40 → 09:30 $1.46 +0.44; DVN×4 yday $48.19 → 09:30 $49.02 +3.32; EOG×1 yday $149.48 → 09:30 $151.45 +1.97; FANG×1 yday $208.55 → 09:30 $213.51 +4.96; TMC×52 yday $3.97 → 09:30 $3.92 -2.60; TGB×25 yday $8.47 → 09:30 $8.35 -3.00; ELF×2 yday $99.65 → 09:30 $98.15 -3.00; DNN×65 yday $3.22 → 09:30 $3.20 -1.30; NB×41 yday $4.46 → 09:30 $4.45 -0.41 | — |
| 2026-08-20 09:30 ET | **SELL** | `BTSG` | 20 | $58.64 | $2.07 | $-27.32 | $1,298.06 | ▼ -27.32 after sell → book $10,582.32; vs 09:30 mark -2.07 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `IREN` | 27 | $42.46 | $2.09 | $-99.20 | $2,442.39 | ▼ -99.20 after sell → book $10,580.23; vs 09:30 mark -2.09 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `TPG` | 24 | $53.06 | $2.08 | $+54.34 | $3,713.75 | ▲ +54.34 after sell → book $10,578.14; vs 09:30 mark -2.09 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `TGTX` | 25 | $51.65 | $2.09 | $+44.60 | $5,002.91 | ▲ +44.60 after sell → book $10,576.06; vs 09:30 mark -2.08 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `SLS` | 106 | $13.84 | $2.34 | $+222.19 | $6,467.62 | ▲ +222.19 after sell → book $10,573.72; vs 09:30 mark -2.34 | dropped from list after 5 sess (min 5) | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **SELL** | `HIMS` | 42 | $30.66 | $2.14 | $+34.39 | $7,753.20 | ▲ +34.39 after sell → book $10,571.58; vs 09:30 mark -2.14 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `TNDM` | 53 | $23.11 | $2.17 | $-15.98 | $8,975.86 | ▼ -15.98 after sell → book $10,569.42; vs 09:30 mark -2.16 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 54 | $20.55 | $2.15 | — | $7,864.01 | — | hold 5d, sell next 09:30 if 🚨; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1121.98 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 12 | $91.01 | $2.03 | — | $6,769.86 | — | hold 5d, sell next 09:30 if 🚨; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1121.98 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 54 | $20.65 | $2.15 | — | $5,652.61 | — | hold 5d, sell next 09:30 if 🚨; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1121.98 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 194 | $5.77 | $2.57 | — | $4,530.66 | — | hold 5d, sell next 09:30 if 🚨; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1121.98 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 57 | $19.63 | $2.16 | — | $3,409.59 | — | hold 5d, sell next 09:30 if 🚨; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1121.98 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 37 | $29.63 | $2.10 | — | $2,311.18 | — | hold 5d, sell next 09:30 if 🚨; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1121.98 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 641 | $1.75 | $8.27 | — | $1,181.16 | — | hold 5d, sell next 09:30 if 🚨; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1121.98 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 7 | $144.54 | $2.01 | — | $167.37 | — | hold 5d, sell next 09:30 if 🚨; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1121.98 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $167.37 | ▲ 09:30 equity $11,026.36 vs yday $10,756.85 (+269.51) | 09:30 open · cash $167.37 (unchanged overnight, no fees) · equity $11,026.36 vs prior close $10,756.85 (+269.51) because holdings re-marked: MARA×1 yday $11.15 → 09:30 $11.70 +0.55; LDI×13 yday $0.87 → 09:30 $0.87 -0.04; BTBT×8 yday $1.59 → 09:30 $1.66 +0.52; DVN×4 yday $49.30 → 09:30 $49.45 +0.60; EOG×1 yday $152.19 → 09:30 $152.29 +0.10; FANG×1 yday $211.02 → 09:30 $211.84 +0.82; TMC×52 yday $3.97 → 09:30 $4.10 +6.76; TGB×25 yday $8.69 → 09:30 $9.00 +7.75; ELF×2 yday $98.46 → 09:30 $99.02 +1.12; DNN×65 yday $3.14 → 09:30 $3.23 +5.85; NB×41 yday $4.29 → 09:30 $4.43 +5.74; AG×54 yday $21.19 → 09:30 $21.90 +38.34; BHP×12 yday $93.63 → 09:30 $95.72 +25.08; CDE×54 yday $21.11 → 09:30 $21.75 +34.56; HDSN×194 yday $5.57 → 09:30 $5.67 +19.40; IAG×57 yday $20.50 → 09:30 $21.17 +38.19; KGC×37 yday $31.43 → 09:30 $32.17 +27.38; NFGC×641 yday $1.75 → 09:30 $1.79 +25.64; WPM×7 yday $150.25 → 09:30 $154.70 +31.15 | — |
| 2026-08-21 09:30 ET | **SELL** | `MARA` | 1 | $11.70 | $0.14 | $+2.46 | $178.93 | ▲ +2.46 after sell → book $11,026.22; vs 09:30 mark -0.14 | dropped from list after 5 sess (min 5) | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `LDI` | 13 | $0.87 | $0.17 | $-1.24 | $190.03 | ▼ -1.24 after sell → book $11,026.05; vs 09:30 mark -0.17 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `BTBT` | 8 | $1.66 | $0.18 | $+0.96 | $203.13 | ▲ +0.96 after sell → book $11,025.87; vs 09:30 mark -0.18 | dropped from list after 5 sess (min 5) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 1 | $17.20 | $0.17 | — | $185.76 | — | hold 5d, sell next 09:30 if 🚨; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $25.39 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 2 | $11.13 | $0.23 | — | $163.27 | — | hold 5d, sell next 09:30 if 🚨; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $25.39 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 10 | $2.47 | $0.28 | — | $138.29 | — | hold 5d, sell next 09:30 if 🚨; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $25.39 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 13 | $1.93 | $0.29 | — | $112.91 | — | hold 5d, sell next 09:30 if 🚨; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $25.39 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 19 | $1.32 | $0.31 | — | $87.52 | — | hold 5d, sell next 09:30 if 🚨; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $25.39 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $87.52 | ▲ 09:30 equity $11,192.61 vs yday $11,095.77 (+96.84) | 09:30 open · cash $87.52 (unchanged overnight, no fees) · equity $11,192.61 vs prior close $11,095.77 (+96.84) because holdings re-marked: DVN×4 yday $49.10 → 09:30 $48.84 -1.04; EOG×1 yday $153.05 → 09:30 $152.61 -0.44; FANG×1 yday $210.72 → 09:30 $209.47 -1.25; TMC×52 yday $4.79 → 09:30 $4.57 -11.44; TGB×25 yday $9.19 → 09:30 $9.26 +1.75; ELF×2 yday $101.94 → 09:30 $101.53 -0.82; DNN×65 yday $3.50 → 09:30 $3.50 +0.00; NB×41 yday $4.64 → 09:30 $4.56 -3.28; AG×54 yday $21.09 → 09:30 $21.47 +20.52; BHP×12 yday $97.03 → 09:30 $97.34 +3.72; CDE×54 yday $20.97 → 09:30 $21.26 +15.66; HDSN×194 yday $5.63 → 09:30 $5.69 +11.64; IAG×57 yday $21.14 → 09:30 $21.44 +17.10; KGC×37 yday $32.76 → 09:30 $33.21 +16.65; NFGC×641 yday $1.84 → 09:30 $1.86 +12.82; WPM×7 yday $157.78 → 09:30 $158.96 +8.26; AUPH×1 yday $16.65 → 09:30 $16.60 -0.05; ARCT×2 yday $13.45 → 09:30 $13.26 -0.38; AUTL×10 yday $2.41 → 09:30 $2.36 -0.50; CRDL×13 yday $1.86 → 09:30 $1.87 +0.13; CYPH×19 yday $1.42 → 09:30 $1.83 +7.79 | — |
| 2026-08-24 09:30 ET | **SELL** | `DVN` | 4 | $48.84 | $1.99 | $+6.80 | $280.90 | ▲ +6.80 after sell → book $11,190.63; vs 09:30 mark -1.98 | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `EOG` | 1 | $152.61 | $1.55 | $+6.86 | $431.96 | ▲ +6.86 after sell → book $11,189.08; vs 09:30 mark -1.55 | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `FANG` | 1 | $209.47 | $2.01 | $+2.76 | $639.41 | ▲ +2.76 after sell → book $11,187.06; vs 09:30 mark -2.02 | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `TMC` | 52 | $4.57 | $2.17 | $+22.73 | $874.89 | ▲ +22.73 after sell → book $11,184.90; vs 09:30 mark -2.16 | dropped from list after 5 sess (min 5) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `TGB` | 25 | $9.26 | $2.08 | $+15.85 | $1,104.30 | ▲ +15.85 after sell → book $11,182.81; vs 09:30 mark -2.09 | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `ELF` | 2 | $101.53 | $2.02 | $+18.15 | $1,305.35 | ▲ +18.15 after sell → book $11,180.80; vs 09:30 mark -2.01 | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `NB` | 41 | $4.56 | $2.01 | $-25.04 | $1,490.30 | ▼ -25.04 after sell → book $11,178.79; vs 09:30 mark -2.01 | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `WPM` | 7 | $158.96 | $2.03 | $+96.90 | $2,600.98 | ▲ +96.90 after sell → book $11,176.75; vs 09:30 mark -2.04 | exit 🚨 after 2 sess | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 2 | $13.26 | $0.29 | $+3.74 | $2,627.21 | ▲ +3.74 after sell → book $11,176.46; vs 09:30 mark -0.29 | exit 🚨 after 1 sess | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,627.21 | ▲ 09:30 equity $11,093.10 vs yday $11,046.82 (+46.28) | 09:30 open · cash $2,627.21 (unchanged overnight, no fees) · equity $11,093.10 vs prior close $11,046.82 (+46.28) because holdings re-marked: DNN×65 yday $3.54 → 09:30 $3.54 +0.00; AG×54 yday $20.57 → 09:30 $20.73 +8.64; BHP×12 yday $96.66 → 09:30 $95.95 -8.52; CDE×54 yday $20.49 → 09:30 $20.85 +19.44; HDSN×194 yday $5.57 → 09:30 $5.53 -7.76; IAG×57 yday $21.36 → 09:30 $21.63 +15.39; KGC×37 yday $32.47 → 09:30 $32.76 +10.73; NFGC×641 yday $1.90 → 09:30 $1.91 +6.41; AUPH×1 yday $16.60 → 09:30 $16.71 +0.11; AUTL×10 yday $2.38 → 09:30 $2.32 -0.60; CRDL×13 yday $1.80 → 09:30 $1.90 +1.30; CYPH×19 yday $1.64 → 09:30 $1.70 +1.14 | — |
| 2026-08-25 09:30 ET | **SELL** | `DNN` | 65 | $3.54 | $2.21 | $+15.11 | $2,855.11 | ▲ +15.11 after sell → book $11,090.90; vs 09:30 mark -2.20 | dropped from list after 6 sess (min 5) | join🔴 sector🟢 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `MOS` | 14 | $24.00 | $2.03 | — | $2,517.08 | — | hold 5d, sell next 09:30 if 🚨; list flatten; ⚪; ret5=+13.0; leftover $356.89 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟡 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 32 | $10.92 | $2.09 | — | $2,165.55 | — | hold 5d, sell next 09:30 if 🚨; list flatten; 🔵; ret5=+10.4; leftover $356.89 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `INSP` | 5 | $61.47 | $2.00 | — | $1,856.19 | — | hold 5d, sell next 09:30 if 🚨; list flatten; 🔵; ret5=+9.2; leftover $356.89 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 43 | $8.28 | $2.12 | — | $1,498.04 | — | hold 5d, sell next 09:30 if 🚨; list flatten; 🔵; ⚪; ret5=+8.8; leftover $356.89 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 68 | $5.23 | $2.19 | — | $1,140.20 | — | hold 5d, sell next 09:30 if 🚨; list flatten; ret5=+10.7; leftover $356.89 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 220 | $1.62 | $2.84 | — | $780.96 | — | hold 5d, sell next 09:30 if 🚨; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.8; leftover $356.89 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `NPWR` | 178 | $2.00 | $2.52 | — | $422.44 | — | hold 5d, sell next 09:30 if 🚨; list probable,yday_gainer,yday_mover; 🔵; ret5=-12.8; leftover $356.89 | join🟡 sector🟢 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $422.44 | ▲ 09:30 equity $11,036.55 vs yday $11,036.55 (-0.00) | 09:30 open · cash $422.44 (unchanged overnight, no fees) · equity $11,036.55 vs prior close $11,036.55 (-0.00) because holdings re-marked: AG×54 yday $20.68 → 09:30 $20.68 +0.00; BHP×12 yday $96.05 → 09:30 $96.05 +0.00; CDE×54 yday $20.71 → 09:30 $20.71 +0.00; HDSN×194 yday $5.49 → 09:30 $5.49 +0.00; IAG×57 yday $21.48 → 09:30 $21.48 +0.00; KGC×37 yday $32.55 → 09:30 $32.55 +0.00; NFGC×641 yday $1.90 → 09:30 $1.90 +0.00; AUPH×1 yday $16.71 → 09:30 $16.71 +0.00; AUTL×10 yday $2.34 → 09:30 $2.34 +0.00; CRDL×13 yday $1.90 → 09:30 $1.90 +0.00; CYPH×19 yday $1.64 → 09:30 $1.64 +0.00; MOS×14 yday $23.75 → 09:30 $23.75 +0.00; OCUL×32 yday $10.92 → 09:30 $10.92 +0.00; INSP×5 yday $61.47 → 09:30 $61.47 +0.00; CRMD×43 yday $8.28 → 09:30 $8.28 +0.00; RZLT×68 yday $5.29 → 09:30 $5.29 +0.00; BMEA×220 yday $1.61 → 09:30 $1.61 +0.00; NPWR×178 yday $2.02 → 09:30 $2.02 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $422.44 | ▲ 09:30 equity $11,165.96 vs yday $11,075.10 (+90.86) | 09:30 open · cash $422.44 (unchanged overnight, no fees) · equity $11,165.96 vs prior close $11,075.10 (+90.86) because holdings re-marked: AG×54 yday $20.68 → 09:30 $20.63 -2.70; BHP×12 yday $96.05 → 09:30 $96.99 +11.28; CDE×54 yday $20.71 → 09:30 $21.00 +15.66; HDSN×194 yday $5.49 → 09:30 $5.51 +3.88; IAG×57 yday $21.48 → 09:30 $21.64 +9.12; KGC×37 yday $32.55 → 09:30 $32.90 +12.95; NFGC×641 yday $1.90 → 09:30 $2.00 +64.10; AUPH×1 yday $16.71 → 09:30 $16.60 -0.11; AUTL×10 yday $2.34 → 09:30 $2.41 +0.70; CRDL×13 yday $1.90 → 09:30 $2.03 +1.69; CYPH×19 yday $1.64 → 09:30 $1.60 -0.76; MOS×14 yday $23.75 → 09:30 $24.84 +15.26; OCUL×32 yday $10.92 → 09:30 $10.79 -4.16; INSP×5 yday $61.47 → 09:30 $60.07 -7.00; CRMD×43 yday $8.28 → 09:30 $8.60 +13.76; RZLT×68 yday $5.29 → 09:30 $5.01 -19.04; BMEA×220 yday $1.61 → 09:30 $1.75 +30.80; NPWR×178 yday $2.02 → 09:30 $1.93 -16.02 | — |
| 2026-08-27 09:30 ET | **SELL** | `AG` | 54 | $20.63 | $2.17 | $-0.00 | $1,534.29 | ▼ -0.00 after sell → book $11,163.79; vs 09:30 mark -2.17 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `BHP` | 12 | $96.99 | $2.05 | $+67.69 | $2,696.12 | ▲ +67.69 after sell → book $11,161.74; vs 09:30 mark -2.05 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `CDE` | 54 | $21.00 | $2.17 | $+14.58 | $3,827.95 | ▲ +14.58 after sell → book $11,159.57; vs 09:30 mark -2.17 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `HDSN` | 194 | $5.51 | $2.61 | $-55.63 | $4,894.28 | ▼ -55.63 after sell → book $11,156.96; vs 09:30 mark -2.61 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `IAG` | 57 | $21.64 | $2.18 | $+110.23 | $6,125.57 | ▲ +110.23 after sell → book $11,154.77; vs 09:30 mark -2.19 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `KGC` | 37 | $32.90 | $2.12 | $+116.77 | $7,340.75 | ▲ +116.77 after sell → book $11,152.65; vs 09:30 mark -2.12 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `NFGC` | 641 | $2.00 | $8.39 | $+143.60 | $8,614.37 | ▲ +143.60 after sell → book $11,144.27; vs 09:30 mark -8.38 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 30 | $40.72 | $2.08 | — | $7,390.69 | — | hold 5d, sell next 09:30 if 🚨; list flatten; ret5=+1.8; leftover $1230.62 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 87 | $14.09 | $2.25 | — | $6,162.61 | — | hold 5d, sell next 09:30 if 🚨; list flatten; ret5=+1.1; leftover $1230.62 | join🟢 sector🔴 gen🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 475 | $2.59 | $6.13 | — | $4,926.23 | — | hold 5d, sell next 09:30 if 🚨; list flatten; ret5=+4.2; leftover $1230.62 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `ACMR` | 15 | $80.97 | $2.04 | — | $3,709.64 | — | hold 5d, sell next 09:30 if 🚨; list mover_buy; 🔵; ret5=-1.3; leftover $1230.62 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `GGB` | 278 | $4.42 | $3.59 | — | $2,477.30 | — | hold 5d, sell next 09:30 if 🚨; list mover_buy; 🔵; ret5=-8.6; leftover $1230.62 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MT` | 16 | $75.12 | $2.04 | — | $1,273.34 | — | hold 5d, sell next 09:30 if 🚨; list mover_buy; 🔵; ret5=-2.2; leftover $1230.62 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MU` | 1 | $925.74 | $1.99 | — | $345.61 | — | hold 5d, sell next 09:30 if 🚨; list mover_buy; 🔵; ret5=-0.5; leftover $1230.62 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $345.61 | ▲ 09:30 equity $11,238.33 vs yday $11,143.15 (+95.18) | 09:30 open · cash $345.61 (unchanged overnight, no fees) · equity $11,238.33 vs prior close $11,143.15 (+95.18) because holdings re-marked: AUPH×1 yday $16.54 → 09:30 $16.47 -0.07; AUTL×10 yday $2.33 → 09:30 $2.32 -0.10; CRDL×13 yday $2.14 → 09:30 $2.09 -0.65; CYPH×19 yday $1.63 → 09:30 $1.75 +2.28; MOS×14 yday $24.16 → 09:30 $24.00 -2.24; OCUL×32 yday $10.77 → 09:30 $10.63 -4.48; INSP×5 yday $61.80 → 09:30 $62.10 +1.50; CRMD×43 yday $8.39 → 09:30 $8.49 +4.30; RZLT×68 yday $5.04 → 09:30 $5.07 +2.04; BMEA×220 yday $1.71 → 09:30 $1.74 +6.60; NPWR×178 yday $1.81 → 09:30 $1.83 +3.56; RRC×30 yday $41.55 → 09:30 $41.44 -3.30; CRK×87 yday $14.50 → 09:30 $14.42 -6.96; SLI×475 yday $2.61 → 09:30 $2.60 -4.75; ACMR×15 yday $79.11 → 09:30 $81.65 +38.10; GGB×278 yday $4.46 → 09:30 $4.57 +30.58; MT×16 yday $74.53 → 09:30 $74.54 +0.16; MU×1 yday $938.40 → 09:30 $967.01 +28.61 | — |
| 2026-08-28 09:30 ET | **SELL** | `AUPH` | 1 | $16.47 | $0.19 | $-1.09 | $361.89 | ▼ -1.09 after sell → book $11,238.14; vs 09:30 mark -0.19 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `AUTL` | 10 | $2.32 | $0.28 | $-2.06 | $384.81 | ▼ -2.06 after sell → book $11,237.86; vs 09:30 mark -0.28 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `CRDL` | 13 | $2.09 | $0.33 | $+1.46 | $411.65 | ▲ +1.46 after sell → book $11,237.53; vs 09:30 mark -0.33 | dropped from list after 5 sess (min 5) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `CYPH` | 19 | $1.75 | $0.41 | $+7.45 | $444.49 | ▲ +7.45 after sell → book $11,237.12; vs 09:30 mark -0.41 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **BUY** | `BHVN` | 6 | $16.95 | $1.03 | — | $341.75 | — | hold 5d, sell next 09:30 if 🚨; list probable,yday_gainer,yday_mover; ret5=-5.4; leftover $111.12 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BZ` | 6 | $18.50 | $1.13 | — | $229.62 | — | hold 5d, sell next 09:30 if 🚨; list probable,yday_gainer,yday_mover; ret5=+2.8; leftover $111.12 | join🟢 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `CAPR` | 12 | $9.19 | $1.14 | — | $118.21 | — | hold 5d, sell next 09:30 if 🚨; list probable,yday_gainer,yday_mover; ret5=-5.4; leftover $111.12 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $118.21 | ▼ 09:30 equity $10,993.06 vs yday $11,247.17 (-254.11) | 09:30 open · cash $118.21 (unchanged overnight, no fees) · equity $10,993.06 vs prior close $11,247.17 (-254.11) because holdings re-marked: MOS×14 yday $23.76 → 09:30 $23.75 -0.14; OCUL×32 yday $10.82 → 09:30 $10.36 -14.72; INSP×5 yday $60.82 → 09:30 $61.44 +3.10; CRMD×43 yday $8.31 → 09:30 $8.29 -0.86; RZLT×68 yday $4.98 → 09:30 $4.62 -24.48; BMEA×220 yday $1.68 → 09:30 $1.71 +6.60; NPWR×178 yday $1.89 → 09:30 $1.83 -10.68; RRC×30 yday $41.64 → 09:30 $41.11 -15.90; CRK×87 yday $14.62 → 09:30 $14.56 -5.22; SLI×475 yday $2.64 → 09:30 $2.51 -61.75; ACMR×15 yday $80.49 → 09:30 $75.10 -80.85; GGB×278 yday $4.70 → 09:30 $4.55 -41.70; MT×16 yday $74.63 → 09:30 $75.07 +7.04; MU×1 yday $935.39 → 09:30 $933.01 -2.38; BHVN×6 yday $16.12 → 09:30 $15.44 -4.08; BZ×6 yday $18.00 → 09:30 $17.89 -0.66; CAPR×12 yday $10.06 → 09:30 $9.44 -7.44 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $118.21 | ▲ 09:30 equity $11,043.16 vs yday $11,005.38 (+37.78) | 09:30 open · cash $118.21 (unchanged overnight, no fees) · equity $11,043.16 vs prior close $11,005.38 (+37.78) because holdings re-marked: MOS×14 yday $23.78 → 09:30 $24.00 +3.08; OCUL×32 yday $10.36 → 09:30 $10.49 +4.16; INSP×5 yday $61.44 → 09:30 $63.05 +8.05; CRMD×43 yday $8.30 → 09:30 $8.26 -1.72; RZLT×68 yday $4.62 → 09:30 $4.69 +4.76; BMEA×220 yday $1.71 → 09:30 $1.65 -13.20; NPWR×178 yday $1.82 → 09:30 $1.78 -7.12; RRC×30 yday $41.78 → 09:30 $41.32 -13.80; CRK×87 yday $14.51 → 09:30 $14.31 -17.40; SLI×475 yday $2.51 → 09:30 $2.70 +90.25; ACMR×15 yday $75.02 → 09:30 $71.24 -56.70; GGB×278 yday $4.55 → 09:30 $4.61 +16.68; MT×16 yday $75.06 → 09:30 $74.31 -12.00; MU×1 yday $933.01 → 09:30 $955.79 +22.78; BHVN×6 yday $15.40 → 09:30 $15.45 +0.30; BZ×6 yday $17.90 → 09:30 $17.37 -3.18; CAPR×12 yday $9.36 → 09:30 $10.43 +12.84 | — |
| 2026-09-01 09:30 ET | **SELL** | `MOS` | 14 | $24.00 | $2.05 | $-4.08 | $452.15 | ▼ -4.08 after sell → book $11,041.10; vs 09:30 mark -2.06 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `OCUL` | 32 | $10.49 | $2.11 | $-17.95 | $785.73 | ▼ -17.95 after sell → book $11,039.00; vs 09:30 mark -2.10 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `INSP` | 5 | $63.05 | $2.02 | $+3.87 | $1,098.95 | ▲ +3.87 after sell → book $11,036.97; vs 09:30 mark -2.03 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `CRMD` | 43 | $8.26 | $2.14 | $-5.12 | $1,451.99 | ▼ -5.12 after sell → book $11,034.83; vs 09:30 mark -2.14 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `RZLT` | 68 | $4.69 | $2.22 | $-41.13 | $1,768.70 | ▼ -41.13 after sell → book $11,032.62; vs 09:30 mark -2.21 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `BMEA` | 220 | $1.65 | $2.88 | $+0.88 | $2,128.81 | ▲ +0.88 after sell → book $11,029.73; vs 09:30 mark -2.89 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `NPWR` | 178 | $1.78 | $2.56 | $-44.25 | $2,443.09 | ▼ -44.25 after sell → book $11,027.17; vs 09:30 mark -2.56 | dropped from list after 5 sess (min 5) | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,443.09 | ▲ 09:30 equity $11,125.90 vs yday $11,051.27 (+74.63) | 09:30 open · cash $2,443.09 (unchanged overnight, no fees) · equity $11,125.90 vs prior close $11,051.27 (+74.63) because holdings re-marked: RRC×30 yday $41.32 → 09:30 $41.94 +18.60; CRK×87 yday $14.90 → 09:30 $15.82 +80.04; SLI×475 yday $2.70 → 09:30 $2.67 -14.25; ACMR×15 yday $71.88 → 09:30 $71.44 -6.60; GGB×278 yday $4.61 → 09:30 $4.57 -11.12; MT×16 yday $73.25 → 09:30 $73.22 -0.48; MU×1 yday $940.00 → 09:30 $941.12 +1.12; BHVN×6 yday $15.45 → 09:30 $15.39 -0.36; BZ×6 yday $17.17 → 09:30 $17.29 +0.72; CAPR×12 yday $10.19 → 09:30 $10.77 +6.96 | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,443.09 | ▲ 09:30 equity $11,084.87 vs yday $11,072.26 (+12.61) | 09:30 open · cash $2,443.09 (unchanged overnight, no fees) · equity $11,084.87 vs prior close $11,072.26 (+12.61) because holdings re-marked: RRC×30 yday $42.40 → 09:30 $42.10 -9.00; CRK×87 yday $16.02 → 09:30 $15.70 -27.84; SLI×475 yday $2.49 → 09:30 $2.49 +0.00; ACMR×15 yday $70.04 → 09:30 $70.52 +7.20; GGB×278 yday $4.69 → 09:30 $4.81 +33.36; MT×16 yday $73.31 → 09:30 $73.86 +8.80; MU×1 yday $933.44 → 09:30 $930.83 -2.61; BHVN×6 yday $15.74 → 09:30 $15.97 +1.38; BZ×6 yday $17.55 → 09:30 $17.65 +0.60; CAPR×12 yday $10.01 → 09:30 $10.07 +0.72 | — |
| 2026-09-03 09:30 ET | **SELL** | `RRC` | 30 | $42.10 | $2.10 | $+37.22 | $3,703.99 | ▲ +37.22 after sell → book $11,082.77; vs 09:30 mark -2.10 | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `SLI` | 475 | $2.49 | $6.22 | $-59.84 | $4,880.52 | ▼ -59.84 after sell → book $11,076.55; vs 09:30 mark -6.22 | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `ACMR` | 15 | $70.52 | $2.06 | $-160.84 | $5,936.27 | ▼ -160.84 after sell → book $11,074.50; vs 09:30 mark -2.05 | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `GGB` | 278 | $4.81 | $3.64 | $+101.19 | $7,269.81 | ▲ +101.19 after sell → book $11,070.86; vs 09:30 mark -3.64 | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `MT` | 16 | $73.86 | $2.06 | $-24.26 | $8,449.51 | ▼ -24.26 after sell → book $11,068.80; vs 09:30 mark -2.06 | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `MU` | 1 | $930.83 | $2.01 | $+1.08 | $9,378.32 | ▲ +1.08 after sell → book $11,066.78; vs 09:30 mark -2.02 | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 26 | $49.76 | $2.07 | — | $8,082.50 | — | hold 5d, sell next 09:30 if 🚨; list flatten; 🔵; ⚪; ret5=+10.6; leftover $1339.76 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 32 | $41.31 | $2.09 | — | $6,758.49 | — | hold 5d, sell next 09:30 if 🚨; list flatten,mover_buy; 🔵; ⚪; ret5=+0.1; leftover $1339.76 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 409 | $3.27 | $5.28 | — | $5,415.78 | — | hold 5d, sell next 09:30 if 🚨; list flatten; 🔵; ⚪; ret5=+13.8; leftover $1339.76 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 173 | $7.70 | $2.51 | — | $4,081.18 | — | hold 5d, sell next 09:30 if 🚨; list flatten,mover_buy; 🔵; ⚪; ret5=+4.7; leftover $1339.76 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 10 | $125.94 | $2.02 | — | $2,819.76 | — | hold 5d, sell next 09:30 if 🚨; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $1339.76 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `GPRO` | 1098 | $1.22 | $14.16 | — | $1,466.03 | — | hold 5d, sell next 09:30 if 🚨; list probable,yday_gainer,yday_mover; ret5=+5.9; leftover $1339.76 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FRVO` | 72 | $18.40 | $2.21 | — | $139.02 | — | hold 5d, sell next 09:30 if 🚨; list probable,yday_gainer,yday_mover; ret5=-14.4; leftover $1339.76 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $139.02 | ▲ 09:30 equity $12,017.21 vs yday $11,853.65 (+163.56) | 09:30 open · cash $139.02 (unchanged overnight, no fees) · equity $12,017.21 vs prior close $11,853.65 (+163.56) because holdings re-marked: CRK×87 yday $15.54 → 09:30 $15.45 -7.83; BHVN×6 yday $15.69 → 09:30 $15.89 +1.20; BZ×6 yday $17.30 → 09:30 $17.31 +0.06; CAPR×12 yday $9.89 → 09:30 $9.83 -0.72; ATRC×26 yday $52.59 → 09:30 $52.88 +7.54; HRMY×32 yday $42.86 → 09:30 $42.93 +2.24; CABA×409 yday $3.57 → 09:30 $3.63 +24.54; VSTM×173 yday $8.02 → 09:30 $8.03 +1.73; RVTY×10 yday $130.94 → 09:30 $132.45 +15.10; GPRO×1098 yday $1.69 → 09:30 $1.78 +98.82; FRVO×72 yday $17.98 → 09:30 $18.27 +20.88 | — |
| 2026-09-04 09:30 ET | **SELL** | `CRK` | 87 | $15.45 | $2.28 | $+113.79 | $1,480.90 | ▲ +113.79 after sell → book $12,014.94; vs 09:30 mark -2.27 | dropped from list after 6 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `BHVN` | 6 | $15.89 | $0.99 | $-8.39 | $1,575.25 | ▼ -8.39 after sell → book $12,013.95; vs 09:30 mark -0.99 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `BZ` | 6 | $17.31 | $1.08 | $-9.34 | $1,678.03 | ▼ -9.34 after sell → book $12,012.87; vs 09:30 mark -1.08 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `CAPR` | 12 | $9.83 | $1.24 | $+5.31 | $1,794.76 | ▲ +5.31 after sell → book $12,011.64; vs 09:30 mark -1.23 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **BUY** | `ASND` | 1 | $266.94 | $1.99 | — | $1,525.82 | — | hold 5d, sell next 09:30 if 🚨; list flatten; ret5=+1.9; leftover $358.95 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OSCR` | 11 | $30.65 | $2.02 | — | $1,186.65 | — | hold 5d, sell next 09:30 if 🚨; list flatten; 🔵; ret5=-2.2; leftover $358.95 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `NVAX` | 34 | $10.41 | $2.09 | — | $830.62 | — | hold 5d, sell next 09:30 if 🚨; list flatten,ohlc_hot; ⚪; ret5=+11.1; leftover $358.95 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BVS` | 24 | $14.50 | $2.06 | — | $480.56 | — | hold 5d, sell next 09:30 if 🚨; list flatten; 🔵; ⚪; ret5=+0.8; leftover $358.95 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 184 | $1.95 | $2.54 | — | $119.21 | — | hold 5d, sell next 09:30 if 🚨; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $358.95 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |

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
| 2026-08-17 | `TNDM` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `MARA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `LDI` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `BTBT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-18 | `BTSG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `IREN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `TPG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `TGTX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `SLS` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `HIMS` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `TNDM` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `MARA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `LDI` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `BTBT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `DVN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-18 | `EOG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-18 | `FANG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-18 | `TMC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-18 | `TGB` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-18 | `ELF` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-18 | `DNN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-18 | `NB` | min_hold | dropped but min-hold 1/5 sess — no sell |
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
| 2026-08-19 | `TNDM` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-19 | `MARA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `LDI` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `BTBT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `DVN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-19 | `EOG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-19 | `FANG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-19 | `TMC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-19 | `TGB` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-19 | `ELF` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-19 | `DNN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-19 | `NB` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `STE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DHR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SYK` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MLYS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `GUTS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BBWI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-20 | `MARA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `LDI` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `BTBT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `DVN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-20 | `EOG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-20 | `FANG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-20 | `TMC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-20 | `TGB` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-20 | `ELF` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-20 | `DNN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-20 | `NB` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-21 | `DVN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `EOG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `FANG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `TMC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `TGB` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `ELF` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `DNN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `NB` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `AG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `BHP` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `CDE` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `HDSN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `IAG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `KGC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `NFGC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `WPM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `AU` | cash | leftover split 25.39 < 1 share @ 119.43 |
| 2026-08-21 | `AEM` | cash | leftover split 25.39 < 1 share @ 216.30 |
| 2026-08-21 | `CRSP` | cash | leftover split 25.39 < 1 share @ 59.72 |
| 2026-08-24 | `AG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `BHP` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `CDE` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `HDSN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `IAG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `KGC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `NFGC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `AUPH` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `AUTL` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `CRDL` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `CYPH` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `RZLT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `MOS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INSP` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HCA` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CRML` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `LAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INFQ` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `AG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `BHP` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `CDE` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `HDSN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `IAG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `KGC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `NFGC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `AUPH` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `AUTL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `CRDL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `CYPH` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `HCA` | cash | leftover split 356.89 < 1 share @ 429.24 |
| 2026-08-26 | `AG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `BHP` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `CDE` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `HDSN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `IAG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `KGC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `NFGC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `AUPH` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `AUTL` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `CRDL` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `CYPH` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `BMEA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-26 | `NPWR` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-26 | `HCA` | no_price | no 09:30 open |
| 2026-08-26 | `KURA` | no_price | no 09:30 open |
| 2026-08-26 | `AVBP` | no_price | no 09:30 open |
| 2026-08-27 | `AUPH` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `AUTL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `CRDL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `CYPH` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `OCUL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `INSP` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `CRMD` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `RZLT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `BMEA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `NPWR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-28 | `OCUL` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `INSP` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `CRMD` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `RZLT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `BMEA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `NPWR` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `ACMR` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-28 | `GGB` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-28 | `MT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-28 | `MU` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-28 | `ANF` | cash | leftover split 111.12 < 1 share @ 144.70 |
| 2026-08-31 | `MOS` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `OCUL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `INSP` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `CRMD` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `RZLT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `BMEA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `NPWR` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `RRC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `CRK` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `SLI` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `ACMR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `GGB` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `MT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `MU` | min_hold | dropped but min-hold 2/5 sess — no sell |
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
| 2026-09-01 | `SLI` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-01 | `ACMR` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-01 | `GGB` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-01 | `MT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-01 | `MU` | min_hold | dropped but min-hold 3/5 sess — no sell |
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
| 2026-09-02 | `SLI` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-02 | `ACMR` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-02 | `GGB` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-02 | `MT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-02 | `MU` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-02 | `BHVN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `BZ` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `CAPR` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `PCRX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `HRMY` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PBH` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `VSTM` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `MGTX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `GPRO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FRVO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FATE` | hard_red | hard-red S=-3.83 sit; no new buys |
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
| `ATRC` | 26 | 2026-09-03 @ $49.76 | hold 5d, sell next 09:30 if 🚨; list flatten; 🔵; ⚪; ret5=+10.6; leftover $1339.76 |
| `HRMY` | 32 | 2026-09-03 @ $41.31 | hold 5d, sell next 09:30 if 🚨; list flatten,mover_buy; 🔵; ⚪; ret5=+0.1; leftover $1339.76 |
| `CABA` | 409 | 2026-09-03 @ $3.27 | hold 5d, sell next 09:30 if 🚨; list flatten; 🔵; ⚪; ret5=+13.8; leftover $1339.76 |
| `VSTM` | 173 | 2026-09-03 @ $7.70 | hold 5d, sell next 09:30 if 🚨; list flatten,mover_buy; 🔵; ⚪; ret5=+4.7; leftover $1339.76 |
| `RVTY` | 10 | 2026-09-03 @ $125.94 | hold 5d, sell next 09:30 if 🚨; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $1339.76 |
| `GPRO` | 1098 | 2026-09-03 @ $1.22 | hold 5d, sell next 09:30 if 🚨; list probable,yday_gainer,yday_mover; ret5=+5.9; leftover $1339.76 |
| `FRVO` | 72 | 2026-09-03 @ $18.40 | hold 5d, sell next 09:30 if 🚨; list probable,yday_gainer,yday_mover; ret5=-14.4; leftover $1339.76 |
| `ASND` | 1 | 2026-09-04 @ $266.94 | hold 5d, sell next 09:30 if 🚨; list flatten; ret5=+1.9; leftover $358.95 |
| `OSCR` | 11 | 2026-09-04 @ $30.65 | hold 5d, sell next 09:30 if 🚨; list flatten; 🔵; ret5=-2.2; leftover $358.95 |
| `NVAX` | 34 | 2026-09-04 @ $10.41 | hold 5d, sell next 09:30 if 🚨; list flatten,ohlc_hot; ⚪; ret5=+11.1; leftover $358.95 |
| `BVS` | 24 | 2026-09-04 @ $14.50 | hold 5d, sell next 09:30 if 🚨; list flatten; 🔵; ⚪; ret5=+0.8; leftover $358.95 |
| `BAK` | 184 | 2026-09-04 @ $1.95 | hold 5d, sell next 09:30 if 🚨; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $358.95 |
