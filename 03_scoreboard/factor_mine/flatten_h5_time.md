# Factor mine action — `flatten_h5_time`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **5** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Buys the flatten **wish-list** even on io/HOLD mornings — live `flatten_robust` would not send 09:30 tickets those days. See `flatten_live_*` for the gated book.

Side **long** · universe `flatten` · top 8 · rank `list` · size `leftover` · sell `time` · S-boost `none` · sell at min-hold even if still listed

Cash book **+22.84%** ($12,284) · signal-only (no cash/fees) was +67.92%. Starts YES **16/17**. Fills 75 · skips 194 · realized $+1824.45.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `flatten` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `time` after min-hold **5**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Buys the flatten **wish-list** even on io/HOLD mornings — live `flatten_robust` would not send 09:30 tickets those days. See `flatten_live_*` for the gated book.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $66.06.

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
| 2026-08-25 | +1.80 | $115.02 | AG×66, BHP×14, CDE×65, HDSN×235, IAG×69, KGC×45, NFGC×777, WPM×9, AUPH×1, ARCT×2, AUTL×10, CRDL×13, CYPH×20 | $11,499.33 | +75.24 | OCUL, CRMD, RZLT | — | $71.40 | $11,434.08 | AG×66, BHP×14, CDE×65, HDSN×235, IAG×69, KGC×45, NFGC×777, WPM×9, AUPH×1, ARCT×2, AUTL×10, CRDL×13, CYPH×20, OCUL×1, CRMD×2, RZLT×3 | 09:30 open · cash $115.02 (unchanged overnight, no fees) · equity $11,499.33 vs prior close $11,424.09 (+75.24) because holdings re-marked: AG×66 yday $20.57 → 09:30 $20.73 +10.56; BHP×14 yday $96.66 → 09:30 $95.95 -9.94; CDE×65 yday $20.49 → 09:30 $20.85 +23.40; HDSN×235 yday $5.57 → 09:30 $5.53 -9.40; IAG×69 yday $21.36 → 09:30 $21.63 +18.63; KGC×45 yday $32.47 → 09:30 $32.76 +13.05; NFGC×777 yday $1.90 → 09:30 $1.91 +7.77; WPM×9 yday $158.00 → 09:30 $160.00 +18.00; AUPH×1 yday $16.60 → 09:30 $16.71 +0.11; ARCT×2 yday $13.76 → 09:30 $14.34 +1.16; AUTL×10 yday $2.38 → 09:30 $2.32 -0.60; CRDL×13 yday $1.80 → 09:30 $1.90 +1.30; CYPH×20 yday $1.64 → 09:30 $1.70 +1.20 |
| 2026-08-26 | +2.02 | $71.40 | AG×66, BHP×14, CDE×65, HDSN×235, IAG×69, KGC×45, NFGC×777, WPM×9, AUPH×1, ARCT×2, AUTL×10, CRDL×13, CYPH×20, OCUL×1, CRMD×2, RZLT×3 | $11,434.08 | +0.00 | — | — | $71.40 | $11,498.88 | AG×66, BHP×14, CDE×65, HDSN×235, IAG×69, KGC×45, NFGC×777, WPM×9, AUPH×1, ARCT×2, AUTL×10, CRDL×13, CYPH×20, OCUL×1, CRMD×2, RZLT×3 | 09:30 open · cash $71.40 (unchanged overnight, no fees) · equity $11,434.08 vs prior close $11,434.08 (+0.00) because holdings re-marked: AG×66 yday $20.68 → 09:30 $20.68 +0.00; BHP×14 yday $96.05 → 09:30 $96.05 +0.00; CDE×65 yday $20.71 → 09:30 $20.71 +0.00; HDSN×235 yday $5.49 → 09:30 $5.49 +0.00; IAG×69 yday $21.48 → 09:30 $21.48 +0.00; KGC×45 yday $32.55 → 09:30 $32.55 +0.00; NFGC×777 yday $1.90 → 09:30 $1.90 +0.00; WPM×9 yday $158.25 → 09:30 $158.25 +0.00; AUPH×1 yday $16.71 → 09:30 $16.71 +0.00; ARCT×2 yday $14.21 → 09:30 $14.21 +0.00; AUTL×10 yday $2.34 → 09:30 $2.34 +0.00; CRDL×13 yday $1.90 → 09:30 $1.90 +0.00; CYPH×20 yday $1.64 → 09:30 $1.64 +0.00; OCUL×1 yday $10.92 → 09:30 $10.92 +0.00; CRMD×2 yday $8.28 → 09:30 $8.28 +0.00; RZLT×3 yday $5.29 → 09:30 $5.29 +0.00 |
| 2026-08-27 | — | $71.40 | AG×66, BHP×14, CDE×65, HDSN×235, IAG×69, KGC×45, NFGC×777, WPM×9, AUPH×1, ARCT×2, AUTL×10, CRDL×13, CYPH×20, OCUL×1, CRMD×2, RZLT×3 | $11,599.53 | +100.65 | RRC, CRK, MOS, SLI | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | $42.66 | $11,638.44 | AUPH×1, ARCT×2, AUTL×10, CRDL×13, CYPH×20, OCUL×1, CRMD×2, RZLT×3, RRC×69, CRK×202, MOS×114, SLI×1100 | 09:30 open · cash $71.40 (unchanged overnight, no fees) · equity $11,599.53 vs prior close $11,498.88 (+100.65) because holdings re-marked: AG×66 yday $20.68 → 09:30 $20.63 -3.30; BHP×14 yday $96.05 → 09:30 $96.99 +13.16; CDE×65 yday $20.71 → 09:30 $21.00 +18.85; HDSN×235 yday $5.49 → 09:30 $5.51 +4.70; IAG×69 yday $21.48 → 09:30 $21.64 +11.04; KGC×45 yday $32.55 → 09:30 $32.90 +15.75; NFGC×777 yday $1.90 → 09:30 $2.00 +77.70; WPM×9 yday $158.25 → 09:30 $160.93 +24.12; AUPH×1 yday $16.71 → 09:30 $16.60 -0.11; ARCT×2 yday $14.21 → 09:30 $15.35 +2.28; AUTL×10 yday $2.34 → 09:30 $2.41 +0.70; CRDL×13 yday $1.90 → 09:30 $2.03 +1.69; CYPH×20 yday $1.64 → 09:30 $1.60 -0.80; OCUL×1 yday $10.92 → 09:30 $10.79 -0.13; CRMD×2 yday $8.28 → 09:30 $8.60 +0.64; RZLT×3 yday $5.29 → 09:30 $5.01 -0.84 |
| 2026-08-28 | +0.75 | $42.66 | AUPH×1, ARCT×2, AUTL×10, CRDL×13, CYPH×20, OCUL×1, CRMD×2, RZLT×3, RRC×69, CRK×202, MOS×114, SLI×1100 | $11,587.00 | -51.44 | — | AUPH, ARCT, AUTL, CRDL, CYPH | $174.41 | $11,655.83 | OCUL×1, CRMD×2, RZLT×3, RRC×69, CRK×202, MOS×114, SLI×1100 | 09:30 open · cash $42.66 (unchanged overnight, no fees) · equity $11,587.00 vs prior close $11,638.44 (-51.44) because holdings re-marked: AUPH×1 yday $16.54 → 09:30 $16.47 -0.07; ARCT×2 yday $15.83 → 09:30 $15.74 -0.18; AUTL×10 yday $2.33 → 09:30 $2.32 -0.10; CRDL×13 yday $2.14 → 09:30 $2.09 -0.65; CYPH×20 yday $1.63 → 09:30 $1.75 +2.40; OCUL×1 yday $10.77 → 09:30 $10.63 -0.14; CRMD×2 yday $8.39 → 09:30 $8.49 +0.20; RZLT×3 yday $5.04 → 09:30 $5.07 +0.09; RRC×69 yday $41.55 → 09:30 $41.44 -7.59; CRK×202 yday $14.50 → 09:30 $14.42 -16.16; MOS×114 yday $24.16 → 09:30 $24.00 -18.24; SLI×1100 yday $2.61 → 09:30 $2.60 -11.00 |
| 2026-08-31 | -5.85 | $174.41 | OCUL×1, CRMD×2, RZLT×3, RRC×69, CRK×202, MOS×114, SLI×1100 | $11,461.42 | -194.41 | — | — | $174.41 | $11,500.99 | OCUL×1, CRMD×2, RZLT×3, RRC×69, CRK×202, MOS×114, SLI×1100 | 09:30 open · cash $174.41 (unchanged overnight, no fees) · equity $11,461.42 vs prior close $11,655.83 (-194.41) because holdings re-marked: OCUL×1 yday $10.82 → 09:30 $10.36 -0.46; CRMD×2 yday $8.31 → 09:30 $8.29 -0.04; RZLT×3 yday $4.98 → 09:30 $4.62 -1.08; RRC×69 yday $41.64 → 09:30 $41.11 -36.57; CRK×202 yday $14.62 → 09:30 $14.56 -12.12; MOS×114 yday $23.76 → 09:30 $23.75 -1.14; SLI×1100 yday $2.64 → 09:30 $2.51 -143.00 |
| 2026-09-01 | -6.30 | $174.41 | OCUL×1, CRMD×2, RZLT×3, RRC×69, CRK×202, MOS×114, SLI×1100 | $11,663.19 | +162.20 | — | OCUL, CRMD, RZLT | $215.00 | $11,810.38 | RRC×69, CRK×202, MOS×114, SLI×1100 | 09:30 open · cash $174.41 (unchanged overnight, no fees) · equity $11,663.19 vs prior close $11,500.99 (+162.20) because holdings re-marked: OCUL×1 yday $10.36 → 09:30 $10.49 +0.13; CRMD×2 yday $8.30 → 09:30 $8.26 -0.08; RZLT×3 yday $4.62 → 09:30 $4.69 +0.21; RRC×69 yday $41.78 → 09:30 $41.32 -31.74; CRK×202 yday $14.51 → 09:30 $14.31 -40.40; MOS×114 yday $23.78 → 09:30 $24.00 +25.08; SLI×1100 yday $2.51 → 09:30 $2.70 +209.00 |
| 2026-09-02 | -3.83 | $215.00 | RRC×69, CRK×202, MOS×114, SLI×1100 | $11,970.66 | +160.28 | — | — | $215.00 | $11,940.56 | RRC×69, CRK×202, MOS×114, SLI×1100 | 09:30 open · cash $215.00 (unchanged overnight, no fees) · equity $11,970.66 vs prior close $11,810.38 (+160.28) because holdings re-marked: RRC×69 yday $41.32 → 09:30 $41.94 +42.78; CRK×202 yday $14.90 → 09:30 $15.82 +185.84; MOS×114 yday $24.25 → 09:30 $23.94 -35.34; SLI×1100 yday $2.70 → 09:30 $2.67 -33.00 |
| 2026-09-03 | -0.90 | $215.00 | RRC×69, CRK×202, MOS×114, SLI×1100 | $11,846.10 | -94.46 | ATRC, HRMY, CABA, VSTM, RVTY | RRC, CRK, MOS, SLI | $116.39 | $12,431.31 | ATRC×47, HRMY×57, CABA×723, VSTM×307, RVTY×18 | 09:30 open · cash $215.00 (unchanged overnight, no fees) · equity $11,846.10 vs prior close $11,940.56 (-94.46) because holdings re-marked: RRC×69 yday $42.40 → 09:30 $42.10 -20.70; CRK×202 yday $16.02 → 09:30 $15.70 -64.64; MOS×114 yday $24.78 → 09:30 $24.70 -9.12; SLI×1100 yday $2.49 → 09:30 $2.49 +0.00 |
| 2026-09-04 | — | $116.39 | ATRC×47, HRMY×57, CABA×723, VSTM×307, RVTY×18 | $12,522.56 | +91.25 | NVAX, BVS | — | $66.06 | $12,284.34 | ATRC×47, HRMY×57, CABA×723, VSTM×307, RVTY×18, NVAX×2, BVS×2 | 09:30 open · cash $116.39 (unchanged overnight, no fees) · equity $12,522.56 vs prior close $12,431.31 (+91.25) because holdings re-marked: ATRC×47 yday $52.59 → 09:30 $52.88 +13.63; HRMY×57 yday $42.86 → 09:30 $42.93 +3.99; CABA×723 yday $3.57 → 09:30 $3.63 +43.38; VSTM×307 yday $8.02 → 09:30 $8.03 +3.07; RVTY×18 yday $130.94 → 09:30 $132.45 +27.18 |

## Fills (09:30 open snapshot, then buys / sells)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 20 | $59.80 | $2.05 | — | $8,801.95 | — | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-5.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 27 | $45.98 | $2.07 | — | $7,558.42 | — | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+12.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 24 | $50.62 | $2.06 | — | $6,341.40 | — | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+6.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TGTX` | 25 | $49.70 | $2.06 | — | $5,096.84 | — | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-0.8; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 106 | $11.70 | $2.31 | — | $3,854.33 | — | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-0.8; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 42 | $29.74 | $2.12 | — | $2,603.13 | — | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-5.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 1543 | $0.81 | $17.13 | — | $1,336.17 | — | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+13.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 53 | $23.33 | $2.15 | — | $97.53 | — | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+19.7; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $97.53 | ▲ 09:30 equity $10,178.12 vs yday $10,153.12 (+25.00) | 09:30 open · cash $97.53 (unchanged overnight, no fees) · equity $10,178.12 vs prior close $10,153.12 (+25.00) because holdings re-marked: BTSG×20 yday $60.23 → 09:30 $59.65 -11.60; IREN×27 yday $44.76 → 09:30 $44.09 -18.09; TPG×24 yday $54.62 → 09:30 $55.29 +16.08; TGTX×25 yday $47.94 → 09:30 $47.27 -16.75; SLS×106 yday $12.36 → 09:30 $12.40 +4.24; HIMS×42 yday $28.77 → 09:30 $29.15 +15.96; INO×1543 yday $0.90 → 09:30 $0.93 +46.29; TNDM×53 yday $23.13 → 09:30 $22.92 -11.13 | — |
| 2026-08-14 09:30 ET | **BUY** | `MARA` | 1 | $9.01 | $0.09 | — | $88.43 | — | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=-13.5; leftover $12.19 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 13 | $0.94 | $0.16 | — | $76.09 | — | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+0.5; leftover $12.19 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 8 | $1.50 | $0.14 | — | $63.95 | — | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+9.2; leftover $12.19 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $63.95 | ▼ 09:30 equity $10,414.78 vs yday $10,435.42 (-20.64) | 09:30 open · cash $63.95 (unchanged overnight, no fees) · equity $10,414.78 vs prior close $10,435.42 (-20.64) because holdings re-marked: BTSG×20 yday $61.71 → 09:30 $61.69 -0.40; IREN×27 yday $44.06 → 09:30 $45.23 +31.59; TPG×24 yday $53.03 → 09:30 $52.67 -8.64; TGTX×25 yday $48.74 → 09:30 $48.74 +0.00; SLS×106 yday $12.78 → 09:30 $12.78 +0.00; HIMS×42 yday $28.15 → 09:30 $28.14 -0.42; INO×1543 yday $1.09 → 09:30 $1.07 -30.86; TNDM×53 yday $22.72 → 09:30 $22.50 -11.66; MARA×1 yday $9.20 → 09:30 $9.22 +0.02; LDI×13 yday $0.90 → 09:30 $0.91 +0.13; BTBT×8 yday $1.57 → 09:30 $1.52 -0.40 | — |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 1 | $4.05 | $0.04 | — | $59.85 | — | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=-12.3; leftover $7.99 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 2 | $3.24 | $0.07 | — | $53.30 | — | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+0.3; leftover $7.99 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `HNST` | 1 | $4.81 | $0.05 | — | $48.44 | — | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-11.4; leftover $7.99 | join🟡 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $48.44 | ▼ 09:30 equity $10,391.80 vs yday $10,525.15 (-133.35) | 09:30 open · cash $48.44 (unchanged overnight, no fees) · equity $10,391.80 vs prior close $10,525.15 (-133.35) because holdings re-marked: BTSG×20 yday $60.38 → 09:30 $60.00 -7.60; IREN×27 yday $44.90 → 09:30 $43.56 -36.18; TPG×24 yday $51.77 → 09:30 $51.77 +0.00; TGTX×25 yday $49.28 → 09:30 $49.28 +0.00; SLS×106 yday $13.00 → 09:30 $12.66 -36.04; HIMS×42 yday $28.61 → 09:30 $27.85 -31.92; INO×1543 yday $1.15 → 09:30 $1.14 -15.43; TNDM×53 yday $22.25 → 09:30 $22.16 -5.03; MARA×1 yday $9.72 → 09:30 $9.36 -0.36; LDI×13 yday $0.88 → 09:30 $0.87 -0.07; BTBT×8 yday $1.60 → 09:30 $1.54 -0.48; TMC×1 yday $3.77 → 09:30 $3.72 -0.05; DNN×2 yday $3.19 → 09:30 $3.11 -0.16; HNST×1 yday $4.70 → 09:30 $4.67 -0.03 | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $48.44 | ▲ 09:30 equity $10,710.13 vs yday $10,572.37 (+137.76) | 09:30 open · cash $48.44 (unchanged overnight, no fees) · equity $10,710.13 vs prior close $10,572.37 (+137.76) because holdings re-marked: BTSG×20 yday $59.50 → 09:30 $60.15 +13.00; IREN×27 yday $42.00 → 09:30 $41.41 -15.80; TPG×24 yday $52.02 → 09:30 $52.26 +5.76; TGTX×25 yday $50.26 → 09:30 $51.62 +34.00; SLS×106 yday $13.10 → 09:30 $13.46 +38.16; HIMS×42 yday $27.39 → 09:30 $27.55 +6.72; INO×1543 yday $1.20 → 09:30 $1.22 +30.86; TNDM×53 yday $23.73 → 09:30 $24.20 +24.91; MARA×1 yday $8.96 → 09:30 $8.91 -0.05; LDI×13 yday $0.86 → 09:30 $0.88 +0.29; BTBT×8 yday $1.45 → 09:30 $1.42 -0.24; TMC×1 yday $3.92 → 09:30 $3.93 +0.01; DNN×2 yday $3.15 → 09:30 $3.19 +0.08; HNST×1 yday $4.75 → 09:30 $4.80 +0.05 | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $48.44 | ▼ 09:30 equity $10,966.31 vs yday $11,031.12 (-64.81) | 09:30 open · cash $48.44 (unchanged overnight, no fees) · equity $10,966.31 vs prior close $11,031.12 (-64.81) because holdings re-marked: BTSG×20 yday $59.33 → 09:30 $58.64 -13.80; IREN×27 yday $42.84 → 09:30 $42.46 -10.26; TPG×24 yday $53.18 → 09:30 $53.06 -2.88; TGTX×25 yday $51.69 → 09:30 $51.65 -1.00; SLS×106 yday $13.85 → 09:30 $13.84 -1.06; HIMS×42 yday $31.09 → 09:30 $30.66 -18.06; INO×1543 yday $1.30 → 09:30 $1.30 +0.00; TNDM×53 yday $23.46 → 09:30 $23.11 -18.55; MARA×1 yday $9.65 → 09:30 $10.21 +0.56; LDI×13 yday $0.88 → 09:30 $0.87 -0.07; BTBT×8 yday $1.40 → 09:30 $1.46 +0.44; TMC×1 yday $3.97 → 09:30 $3.92 -0.05; DNN×2 yday $3.22 → 09:30 $3.20 -0.04; HNST×1 yday $5.02 → 09:30 $4.98 -0.04 | — |
| 2026-08-20 09:30 ET | **SELL** | `BTSG` | 20 | $58.64 | $2.07 | $-27.32 | $1,219.17 | ▼ -27.32 after sell → book $10,964.24; vs 09:30 mark -2.07 | time-stop after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `IREN` | 27 | $42.46 | $2.09 | $-99.20 | $2,363.50 | ▼ -99.20 after sell → book $10,962.15; vs 09:30 mark -2.09 | time-stop after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `TPG` | 24 | $53.06 | $2.08 | $+54.34 | $3,634.86 | ▲ +54.34 after sell → book $10,960.07; vs 09:30 mark -2.08 | time-stop after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `TGTX` | 25 | $51.65 | $2.09 | $+44.60 | $4,924.02 | ▲ +44.60 after sell → book $10,957.99; vs 09:30 mark -2.08 | time-stop after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `SLS` | 106 | $13.84 | $2.34 | $+222.19 | $6,388.72 | ▲ +222.19 after sell → book $10,955.65; vs 09:30 mark -2.34 | time-stop after 5 sess (min 5) | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **SELL** | `HIMS` | 42 | $30.66 | $2.14 | $+34.39 | $7,674.31 | ▲ +34.39 after sell → book $10,953.51; vs 09:30 mark -2.14 | time-stop after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `INO` | 1543 | $1.30 | $20.18 | $+718.77 | $9,660.03 | ▲ +718.77 after sell → book $10,933.33; vs 09:30 mark -20.18 | time-stop after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `TNDM` | 53 | $23.11 | $2.17 | $-15.98 | $10,882.69 | ▼ -15.98 after sell → book $10,931.17; vs 09:30 mark -2.16 | time-stop after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 66 | $20.55 | $2.19 | — | $9,524.20 | — | sell at min-hold even if still listed; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+8.9; leftover $1360.34 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 14 | $91.01 | $2.03 | — | $8,248.03 | — | sell at min-hold even if still listed; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+2.4; leftover $1360.34 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 65 | $20.65 | $2.19 | — | $6,903.60 | — | sell at min-hold even if still listed; list flatten,yday_gainer,yday_mover,mover_buy; live flatten mover; 🔵; ⚪; ret5=+11.3; leftover $1360.34 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 235 | $5.77 | $3.03 | — | $5,544.62 | — | sell at min-hold even if still listed; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+4.6; leftover $1360.34 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 69 | $19.63 | $2.20 | — | $4,187.95 | — | sell at min-hold even if still listed; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+9.1; leftover $1360.34 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 45 | $29.63 | $2.12 | — | $2,852.47 | — | sell at min-hold even if still listed; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+8.7; leftover $1360.34 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 777 | $1.75 | $10.02 | — | $1,482.70 | — | sell at min-hold even if still listed; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.9; leftover $1360.34 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 9 | $144.54 | $2.02 | — | $179.82 | — | sell at min-hold even if still listed; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+9.2; leftover $1360.34 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $179.82 | ▲ 09:30 equity $11,454.79 vs yday $11,161.56 (+293.23) | 09:30 open · cash $179.82 (unchanged overnight, no fees) · equity $11,454.79 vs prior close $11,161.56 (+293.23) because holdings re-marked: MARA×1 yday $11.15 → 09:30 $11.70 +0.55; LDI×13 yday $0.87 → 09:30 $0.87 -0.04; BTBT×8 yday $1.59 → 09:30 $1.66 +0.52; TMC×1 yday $3.97 → 09:30 $4.10 +0.13; DNN×2 yday $3.14 → 09:30 $3.23 +0.18; HNST×1 yday $4.96 → 09:30 $4.97 +0.01; AG×66 yday $21.19 → 09:30 $21.90 +46.86; BHP×14 yday $93.63 → 09:30 $95.72 +29.26; CDE×65 yday $21.11 → 09:30 $21.75 +41.60; HDSN×235 yday $5.57 → 09:30 $5.67 +23.50; IAG×69 yday $20.50 → 09:30 $21.17 +46.23; KGC×45 yday $31.43 → 09:30 $32.17 +33.30; NFGC×777 yday $1.75 → 09:30 $1.79 +31.08; WPM×9 yday $150.25 → 09:30 $154.70 +40.05 | — |
| 2026-08-21 09:30 ET | **SELL** | `MARA` | 1 | $11.70 | $0.14 | $+2.46 | $191.38 | ▲ +2.46 after sell → book $11,454.65; vs 09:30 mark -0.14 | time-stop after 5 sess (min 5) | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `LDI` | 13 | $0.87 | $0.17 | $-1.24 | $202.48 | ▼ -1.24 after sell → book $11,454.48; vs 09:30 mark -0.17 | time-stop after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `BTBT` | 8 | $1.66 | $0.18 | $+0.96 | $215.59 | ▲ +0.96 after sell → book $11,454.31; vs 09:30 mark -0.17 | time-stop after 5 sess (min 5) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 1 | $17.20 | $0.17 | — | $198.21 | — | sell at min-hold even if still listed; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+13.8; leftover $26.95 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 2 | $11.13 | $0.23 | — | $175.72 | — | sell at min-hold even if still listed; list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+39.8; leftover $26.95 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 10 | $2.47 | $0.28 | — | $150.75 | — | sell at min-hold even if still listed; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+10.8; leftover $26.95 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 13 | $1.93 | $0.29 | — | $125.37 | — | sell at min-hold even if still listed; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+10.2; leftover $26.95 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 20 | $1.32 | $0.32 | — | $98.64 | — | sell at min-hold even if still listed; list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+83.6; leftover $26.95 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $98.64 | ▲ 09:30 equity $11,591.39 vs yday $11,454.73 (+136.66) | 09:30 open · cash $98.64 (unchanged overnight, no fees) · equity $11,591.39 vs prior close $11,454.73 (+136.66) because holdings re-marked: TMC×1 yday $4.79 → 09:30 $4.57 -0.22; DNN×2 yday $3.50 → 09:30 $3.50 +0.00; HNST×1 yday $5.05 → 09:30 $5.05 +0.00; AG×66 yday $21.09 → 09:30 $21.47 +25.08; BHP×14 yday $97.03 → 09:30 $97.34 +4.34; CDE×65 yday $20.97 → 09:30 $21.26 +18.85; HDSN×235 yday $5.63 → 09:30 $5.69 +14.10; IAG×69 yday $21.14 → 09:30 $21.44 +20.70; KGC×45 yday $32.76 → 09:30 $33.21 +20.25; NFGC×777 yday $1.84 → 09:30 $1.86 +15.54; WPM×9 yday $157.78 → 09:30 $158.96 +10.62; AUPH×1 yday $16.65 → 09:30 $16.60 -0.05; ARCT×2 yday $13.45 → 09:30 $13.26 -0.38; AUTL×10 yday $2.41 → 09:30 $2.36 -0.50; CRDL×13 yday $1.86 → 09:30 $1.87 +0.13; CYPH×20 yday $1.42 → 09:30 $1.83 +8.20 | — |
| 2026-08-24 09:30 ET | **SELL** | `TMC` | 1 | $4.57 | $0.07 | $+0.41 | $103.14 | ▲ +0.41 after sell → book $11,591.32; vs 09:30 mark -0.07 | time-stop after 5 sess (min 5) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `DNN` | 2 | $3.50 | $0.10 | $+0.35 | $110.05 | ▲ +0.35 after sell → book $11,591.23; vs 09:30 mark -0.09 | time-stop after 5 sess (min 5) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 judge🟡 ab🟡 peer🟢 vol🟢 buy🟢 |
| 2026-08-24 09:30 ET | **SELL** | `HNST` | 1 | $5.05 | $0.07 | $+0.12 | $115.02 | ▲ +0.12 after sell → book $11,591.15; vs 09:30 mark -0.08 | time-stop after 5 sess (min 5) | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $115.02 | ▲ 09:30 equity $11,499.33 vs yday $11,424.09 (+75.24) | 09:30 open · cash $115.02 (unchanged overnight, no fees) · equity $11,499.33 vs prior close $11,424.09 (+75.24) because holdings re-marked: AG×66 yday $20.57 → 09:30 $20.73 +10.56; BHP×14 yday $96.66 → 09:30 $95.95 -9.94; CDE×65 yday $20.49 → 09:30 $20.85 +23.40; HDSN×235 yday $5.57 → 09:30 $5.53 -9.40; IAG×69 yday $21.36 → 09:30 $21.63 +18.63; KGC×45 yday $32.47 → 09:30 $32.76 +13.05; NFGC×777 yday $1.90 → 09:30 $1.91 +7.77; WPM×9 yday $158.00 → 09:30 $160.00 +18.00; AUPH×1 yday $16.60 → 09:30 $16.71 +0.11; ARCT×2 yday $13.76 → 09:30 $14.34 +1.16; AUTL×10 yday $2.38 → 09:30 $2.32 -0.60; CRDL×13 yday $1.80 → 09:30 $1.90 +1.30; CYPH×20 yday $1.64 → 09:30 $1.70 +1.20 | — |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 1 | $10.92 | $0.11 | — | $103.99 | — | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+10.4; leftover $19.17 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 2 | $8.28 | $0.17 | — | $87.26 | — | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+8.8; leftover $19.17 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 3 | $5.23 | $0.17 | — | $71.40 | — | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); ret5=+10.7; leftover $19.17 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $71.40 | ▲ 09:30 equity $11,434.08 vs yday $11,434.08 (+0.00) | 09:30 open · cash $71.40 (unchanged overnight, no fees) · equity $11,434.08 vs prior close $11,434.08 (+0.00) because holdings re-marked: AG×66 yday $20.68 → 09:30 $20.68 +0.00; BHP×14 yday $96.05 → 09:30 $96.05 +0.00; CDE×65 yday $20.71 → 09:30 $20.71 +0.00; HDSN×235 yday $5.49 → 09:30 $5.49 +0.00; IAG×69 yday $21.48 → 09:30 $21.48 +0.00; KGC×45 yday $32.55 → 09:30 $32.55 +0.00; NFGC×777 yday $1.90 → 09:30 $1.90 +0.00; WPM×9 yday $158.25 → 09:30 $158.25 +0.00; AUPH×1 yday $16.71 → 09:30 $16.71 +0.00; ARCT×2 yday $14.21 → 09:30 $14.21 +0.00; AUTL×10 yday $2.34 → 09:30 $2.34 +0.00; CRDL×13 yday $1.90 → 09:30 $1.90 +0.00; CYPH×20 yday $1.64 → 09:30 $1.64 +0.00; OCUL×1 yday $10.92 → 09:30 $10.92 +0.00; CRMD×2 yday $8.28 → 09:30 $8.28 +0.00; RZLT×3 yday $5.29 → 09:30 $5.29 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $71.40 | ▲ 09:30 equity $11,599.53 vs yday $11,498.88 (+100.65) | 09:30 open · cash $71.40 (unchanged overnight, no fees) · equity $11,599.53 vs prior close $11,498.88 (+100.65) because holdings re-marked: AG×66 yday $20.68 → 09:30 $20.63 -3.30; BHP×14 yday $96.05 → 09:30 $96.99 +13.16; CDE×65 yday $20.71 → 09:30 $21.00 +18.85; HDSN×235 yday $5.49 → 09:30 $5.51 +4.70; IAG×69 yday $21.48 → 09:30 $21.64 +11.04; KGC×45 yday $32.55 → 09:30 $32.90 +15.75; NFGC×777 yday $1.90 → 09:30 $2.00 +77.70; WPM×9 yday $158.25 → 09:30 $160.93 +24.12; AUPH×1 yday $16.71 → 09:30 $16.60 -0.11; ARCT×2 yday $14.21 → 09:30 $15.35 +2.28; AUTL×10 yday $2.34 → 09:30 $2.41 +0.70; CRDL×13 yday $1.90 → 09:30 $2.03 +1.69; CYPH×20 yday $1.64 → 09:30 $1.60 -0.80; OCUL×1 yday $10.92 → 09:30 $10.79 -0.13; CRMD×2 yday $8.28 → 09:30 $8.60 +0.64; RZLT×3 yday $5.29 → 09:30 $5.01 -0.84 | — |
| 2026-08-27 09:30 ET | **SELL** | `AG` | 66 | $20.63 | $2.21 | $+0.88 | $1,430.77 | ▲ +0.88 after sell → book $11,597.32; vs 09:30 mark -2.21 | time-stop after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `BHP` | 14 | $96.99 | $2.05 | $+79.64 | $2,786.58 | ▲ +79.64 after sell → book $11,595.27; vs 09:30 mark -2.05 | time-stop after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `CDE` | 65 | $21.00 | $2.21 | $+18.36 | $4,149.37 | ▲ +18.36 after sell → book $11,593.06; vs 09:30 mark -2.21 | time-stop after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `HDSN` | 235 | $5.51 | $3.08 | $-67.21 | $5,441.14 | ▼ -67.21 after sell → book $11,589.98; vs 09:30 mark -3.08 | time-stop after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `IAG` | 69 | $21.64 | $2.22 | $+134.27 | $6,932.08 | ▲ +134.27 after sell → book $11,587.76; vs 09:30 mark -2.22 | time-stop after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `KGC` | 45 | $32.90 | $2.15 | $+142.88 | $8,410.44 | ▲ +142.88 after sell → book $11,585.62; vs 09:30 mark -2.14 | time-stop after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `NFGC` | 777 | $2.00 | $10.16 | $+174.06 | $9,954.27 | ▲ +174.06 after sell → book $11,575.45; vs 09:30 mark -10.17 | time-stop after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `WPM` | 9 | $160.93 | $2.04 | $+143.45 | $11,400.60 | ▲ +143.45 after sell → book $11,573.41; vs 09:30 mark -2.04 | time-stop after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 69 | $40.72 | $2.20 | — | $8,588.73 | — | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); ret5=+1.8; leftover $2850.15 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 202 | $14.09 | $2.61 | — | $5,739.94 | — | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); ret5=+1.1; leftover $2850.15 | join🟢 sector🔴 gen🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MOS` | 114 | $24.84 | $2.33 | — | $2,905.85 | — | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); ret5=+13.0; leftover $2850.15 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟢 |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 1100 | $2.59 | $14.19 | — | $42.66 | — | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); ret5=+4.2; leftover $2850.15 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $42.66 | ▼ 09:30 equity $11,587.00 vs yday $11,638.44 (-51.44) | 09:30 open · cash $42.66 (unchanged overnight, no fees) · equity $11,587.00 vs prior close $11,638.44 (-51.44) because holdings re-marked: AUPH×1 yday $16.54 → 09:30 $16.47 -0.07; ARCT×2 yday $15.83 → 09:30 $15.74 -0.18; AUTL×10 yday $2.33 → 09:30 $2.32 -0.10; CRDL×13 yday $2.14 → 09:30 $2.09 -0.65; CYPH×20 yday $1.63 → 09:30 $1.75 +2.40; OCUL×1 yday $10.77 → 09:30 $10.63 -0.14; CRMD×2 yday $8.39 → 09:30 $8.49 +0.20; RZLT×3 yday $5.04 → 09:30 $5.07 +0.09; RRC×69 yday $41.55 → 09:30 $41.44 -7.59; CRK×202 yday $14.50 → 09:30 $14.42 -16.16; MOS×114 yday $24.16 → 09:30 $24.00 -18.24; SLI×1100 yday $2.61 → 09:30 $2.60 -11.00 | — |
| 2026-08-28 09:30 ET | **SELL** | `AUPH` | 1 | $16.47 | $0.19 | $-1.09 | $58.94 | ▼ -1.09 after sell → book $11,586.81; vs 09:30 mark -0.19 | time-stop after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `ARCT` | 2 | $15.74 | $0.34 | $+8.65 | $90.08 | ▲ +8.65 after sell → book $11,586.47; vs 09:30 mark -0.34 | time-stop after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `AUTL` | 10 | $2.32 | $0.28 | $-2.06 | $113.00 | ▼ -2.06 after sell → book $11,586.19; vs 09:30 mark -0.28 | time-stop after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `CRDL` | 13 | $2.09 | $0.33 | $+1.46 | $139.84 | ▲ +1.46 after sell → book $11,585.86; vs 09:30 mark -0.33 | time-stop after 5 sess (min 5) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `CYPH` | 20 | $1.75 | $0.43 | $+7.85 | $174.41 | ▲ +7.85 after sell → book $11,585.43; vs 09:30 mark -0.43 | time-stop after 5 sess (min 5) | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $174.41 | ▼ 09:30 equity $11,461.42 vs yday $11,655.83 (-194.41) | 09:30 open · cash $174.41 (unchanged overnight, no fees) · equity $11,461.42 vs prior close $11,655.83 (-194.41) because holdings re-marked: OCUL×1 yday $10.82 → 09:30 $10.36 -0.46; CRMD×2 yday $8.31 → 09:30 $8.29 -0.04; RZLT×3 yday $4.98 → 09:30 $4.62 -1.08; RRC×69 yday $41.64 → 09:30 $41.11 -36.57; CRK×202 yday $14.62 → 09:30 $14.56 -12.12; MOS×114 yday $23.76 → 09:30 $23.75 -1.14; SLI×1100 yday $2.64 → 09:30 $2.51 -143.00 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $174.41 | ▲ 09:30 equity $11,663.19 vs yday $11,500.99 (+162.20) | 09:30 open · cash $174.41 (unchanged overnight, no fees) · equity $11,663.19 vs prior close $11,500.99 (+162.20) because holdings re-marked: OCUL×1 yday $10.36 → 09:30 $10.49 +0.13; CRMD×2 yday $8.30 → 09:30 $8.26 -0.08; RZLT×3 yday $4.62 → 09:30 $4.69 +0.21; RRC×69 yday $41.78 → 09:30 $41.32 -31.74; CRK×202 yday $14.51 → 09:30 $14.31 -40.40; MOS×114 yday $23.78 → 09:30 $24.00 +25.08; SLI×1100 yday $2.51 → 09:30 $2.70 +209.00 | — |
| 2026-09-01 09:30 ET | **SELL** | `OCUL` | 1 | $10.49 | $0.13 | $-0.67 | $184.77 | ▼ -0.67 after sell → book $11,663.06; vs 09:30 mark -0.13 | time-stop after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `CRMD` | 2 | $8.26 | $0.19 | $-0.40 | $201.10 | ▼ -0.40 after sell → book $11,662.87; vs 09:30 mark -0.19 | time-stop after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `RZLT` | 3 | $4.69 | $0.17 | $-1.96 | $215.00 | ▼ -1.96 after sell → book $11,662.70; vs 09:30 mark -0.17 | time-stop after 5 sess (min 5) | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $215.00 | ▲ 09:30 equity $11,970.66 vs yday $11,810.38 (+160.28) | 09:30 open · cash $215.00 (unchanged overnight, no fees) · equity $11,970.66 vs prior close $11,810.38 (+160.28) because holdings re-marked: RRC×69 yday $41.32 → 09:30 $41.94 +42.78; CRK×202 yday $14.90 → 09:30 $15.82 +185.84; MOS×114 yday $24.25 → 09:30 $23.94 -35.34; SLI×1100 yday $2.70 → 09:30 $2.67 -33.00 | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $215.00 | ▼ 09:30 equity $11,846.10 vs yday $11,940.56 (-94.46) | 09:30 open · cash $215.00 (unchanged overnight, no fees) · equity $11,846.10 vs prior close $11,940.56 (-94.46) because holdings re-marked: RRC×69 yday $42.40 → 09:30 $42.10 -20.70; CRK×202 yday $16.02 → 09:30 $15.70 -64.64; MOS×114 yday $24.78 → 09:30 $24.70 -9.12; SLI×1100 yday $2.49 → 09:30 $2.49 +0.00 | — |
| 2026-09-03 09:30 ET | **SELL** | `RRC` | 69 | $42.10 | $2.23 | $+90.79 | $3,117.67 | ▲ +90.79 after sell → book $11,843.87; vs 09:30 mark -2.23 | time-stop after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `CRK` | 202 | $15.70 | $2.66 | $+319.95 | $6,286.40 | ▲ +319.95 after sell → book $11,841.20; vs 09:30 mark -2.67 | time-stop after 5 sess (min 5) | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-03 09:30 ET | **SELL** | `MOS` | 114 | $24.70 | $2.37 | $-20.67 | $9,099.83 | ▼ -20.67 after sell → book $11,838.83; vs 09:30 mark -2.37 | time-stop after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `SLI` | 1100 | $2.49 | $14.39 | $-138.58 | $11,824.43 | ▼ -138.58 after sell → book $11,824.43; vs 09:30 mark -14.40 | time-stop after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 47 | $49.76 | $2.13 | — | $9,483.58 | — | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+10.6; leftover $2364.89 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 57 | $41.31 | $2.16 | — | $7,126.75 | — | sell at min-hold even if still listed; list flatten,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+0.1; leftover $2364.89 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 723 | $3.27 | $9.33 | — | $4,753.21 | — | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+13.8; leftover $2364.89 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 307 | $7.70 | $3.96 | — | $2,385.35 | — | sell at min-hold even if still listed; list flatten,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+4.7; leftover $2364.89 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 18 | $125.94 | $2.04 | — | $116.39 | — | sell at min-hold even if still listed; list flatten,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+6.8; leftover $2364.89 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $116.39 | ▲ 09:30 equity $12,522.56 vs yday $12,431.31 (+91.25) | 09:30 open · cash $116.39 (unchanged overnight, no fees) · equity $12,522.56 vs prior close $12,431.31 (+91.25) because holdings re-marked: ATRC×47 yday $52.59 → 09:30 $52.88 +13.63; HRMY×57 yday $42.86 → 09:30 $42.93 +3.99; CABA×723 yday $3.57 → 09:30 $3.63 +43.38; VSTM×307 yday $8.02 → 09:30 $8.03 +3.07; RVTY×18 yday $130.94 → 09:30 $132.45 +27.18 | — |
| 2026-09-04 09:30 ET | **BUY** | `NVAX` | 2 | $10.41 | $0.21 | — | $95.36 | — | sell at min-hold even if still listed; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); ⚪; ret5=+11.1; leftover $29.10 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BVS` | 2 | $14.50 | $0.30 | — | $66.06 | — | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+0.8; leftover $29.10 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |

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
| 2026-08-25 | `MOS` | cash | leftover split 19.17 < 1 share @ 24.00 |
| 2026-08-25 | `INSP` | cash | leftover split 19.17 < 1 share @ 61.47 |
| 2026-08-25 | `HCA` | cash | leftover split 19.17 < 1 share @ 429.24 |
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
| 2026-08-26 | `HCA` | no_price | no 09:30 open |
| 2026-08-26 | `MOS` | no_price | no 09:30 open |
| 2026-08-26 | `INSP` | no_price | no 09:30 open |
| 2026-08-27 | `AUPH` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `ARCT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `AUTL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `CRDL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `CYPH` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `OCUL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `CRMD` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `RZLT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-28 | `OCUL` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `CRMD` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `RZLT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-31 | `OCUL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `CRMD` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `RZLT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `RRC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `CRK` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `MOS` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `SLI` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `RES` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PBF` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `WTTR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `RRC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-01 | `CRK` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-01 | `MOS` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-01 | `SLI` | min_hold | dropped but min-hold 3/5 sess — no sell |
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
| 2026-09-02 | `PCRX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `HRMY` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PBH` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `VSTM` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `MGTX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PBR-A` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PBR` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-04 | `HRMY` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `VSTM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `RVTY` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `ASND` | cash | leftover split 29.10 < 1 share @ 266.94 |
| 2026-09-04 | `OSCR` | cash | leftover split 29.10 < 1 share @ 30.65 |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `ATRC` | 47 | 2026-09-03 @ $49.76 | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+10.6; leftover $2364.89 |
| `HRMY` | 57 | 2026-09-03 @ $41.31 | sell at min-hold even if still listed; list flatten,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+0.1; leftover $2364.89 |
| `CABA` | 723 | 2026-09-03 @ $3.27 | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+13.8; leftover $2364.89 |
| `VSTM` | 307 | 2026-09-03 @ $7.70 | sell at min-hold even if still listed; list flatten,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+4.7; leftover $2364.89 |
| `RVTY` | 18 | 2026-09-03 @ $125.94 | sell at min-hold even if still listed; list flatten,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+6.8; leftover $2364.89 |
| `NVAX` | 2 | 2026-09-04 @ $10.41 | sell at min-hold even if still listed; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); ⚪; ret5=+11.1; leftover $29.10 |
| `BVS` | 2 | 2026-09-04 @ $14.50 | sell at min-hold even if still listed; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+0.8; leftover $29.10 |
