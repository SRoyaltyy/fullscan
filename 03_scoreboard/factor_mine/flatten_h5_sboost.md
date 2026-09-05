# Factor mine action — `flatten_h5_sboost`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **5** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Buys the flatten **wish-list** even on io/HOLD mornings — live `flatten_robust` would not send 09:30 tickets those days. See `flatten_live_*` for the gated book.

Side **long** · universe `flatten` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `both` · S≥+5: sizeup + more names

Cash book **+22.18%** ($12,218) · signal-only (no cash/fees) was +67.92%. Starts YES **16/17**. Fills 79 · skips 202 · realized $+1759.87.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `flatten` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `list order` · **top_n** 8 (S≥+5 may raise this when S-boost is `both`).
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **5**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Buys the flatten **wish-list** even on io/HOLD mornings — live `flatten_robust` would not send 09:30 tickets those days. See `flatten_live_*` for the gated book.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $71.34.

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | vs yday close | Bought | Sold | Close cash | Close equity | Close held | 09:30 why |
|---|---:|---:|---|---:|---:|---|---|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | BTSG, IREN, TPG, TGTX, SLS, HIMS, INO, TNDM, VOR | — | $123.82 | $10,195.74 | BTSG×18, IREN×24, TPG×21, TGTX×22, SLS×94, HIMS×37, INO×1371, TNDM×47, VOR×50 | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-14 | +5.50 | $123.82 | BTSG×18, IREN×24, TPG×21, TGTX×22, SLS×94, HIMS×37, INO×1371, TNDM×47, VOR×50 | $10,219.63 | +23.89 | MARA, LDI, BTBT | — | $87.76 | $10,434.38 | BTSG×18, IREN×24, TPG×21, TGTX×22, SLS×94, HIMS×37, INO×1371, TNDM×47, VOR×50, MARA×1, LDI×14, BTBT×9 | 09:30 open · cash $123.82 (unchanged overnight, no fees) · equity $10,219.63 vs prior close $10,195.74 (+23.89) because holdings re-marked: BTSG×18 yday $60.23 → 09:30 $59.65 -10.44; IREN×24 yday $44.76 → 09:30 $44.09 -16.08; TPG×21 yday $54.62 → 09:30 $55.29 +14.07; TGTX×22 yday $47.94 → 09:30 $47.27 -14.74; SLS×94 yday $12.36 → 09:30 $12.40 +3.76; HIMS×37 yday $28.77 → 09:30 $29.15 +14.06; INO×1371 yday $0.90 → 09:30 $0.93 +41.13; TNDM×47 yday $23.13 → 09:30 $22.92 -9.87; VOR×50 yday $23.29 → 09:30 $23.33 +2.00 |
| 2026-08-17 | +2.25 | $87.76 | BTSG×18, IREN×24, TPG×21, TGTX×22, SLS×94, HIMS×37, INO×1371, TNDM×47, VOR×50, MARA×1, LDI×14, BTBT×9 | $10,410.12 | -24.26 | TMC, TGB, DNN, HNST | — | $51.48 | $10,512.60 | BTSG×18, IREN×24, TPG×21, TGTX×22, SLS×94, HIMS×37, INO×1371, TNDM×47, VOR×50, MARA×1, LDI×14, BTBT×9, TMC×2, TGB×1, DNN×3, HNST×2 | 09:30 open · cash $87.76 (unchanged overnight, no fees) · equity $10,410.12 vs prior close $10,434.38 (-24.26) because holdings re-marked: BTSG×18 yday $61.71 → 09:30 $61.69 -0.36; IREN×24 yday $44.06 → 09:30 $45.23 +28.08; TPG×21 yday $53.03 → 09:30 $52.67 -7.56; TGTX×22 yday $48.74 → 09:30 $48.74 +0.00; SLS×94 yday $12.78 → 09:30 $12.78 +0.00; HIMS×37 yday $28.15 → 09:30 $28.14 -0.37; INO×1371 yday $1.09 → 09:30 $1.07 -27.42; TNDM×47 yday $22.72 → 09:30 $22.50 -10.34; VOR×50 yday $23.03 → 09:30 $22.91 -6.00; MARA×1 yday $9.20 → 09:30 $9.22 +0.02; LDI×14 yday $0.90 → 09:30 $0.91 +0.14; BTBT×9 yday $1.57 → 09:30 $1.52 -0.45 |
| 2026-08-18 | -6.20 | $51.48 | BTSG×18, IREN×24, TPG×21, TGTX×22, SLS×94, HIMS×37, INO×1371, TNDM×47, VOR×50, MARA×1, LDI×14, BTBT×9, TMC×2, TGB×1, DNN×3, HNST×2 | $10,384.26 | -128.34 | — | — | $51.48 | $10,567.13 | BTSG×18, IREN×24, TPG×21, TGTX×22, SLS×94, HIMS×37, INO×1371, TNDM×47, VOR×50, MARA×1, LDI×14, BTBT×9, TMC×2, TGB×1, DNN×3, HNST×2 | 09:30 open · cash $51.48 (unchanged overnight, no fees) · equity $10,384.26 vs prior close $10,512.60 (-128.34) because holdings re-marked: BTSG×18 yday $60.38 → 09:30 $60.00 -6.84; IREN×24 yday $44.90 → 09:30 $43.56 -32.16; TPG×21 yday $51.77 → 09:30 $51.77 +0.00; TGTX×22 yday $49.28 → 09:30 $49.28 +0.00; SLS×94 yday $13.00 → 09:30 $12.66 -31.96; HIMS×37 yday $28.61 → 09:30 $27.85 -28.12; INO×1371 yday $1.15 → 09:30 $1.14 -13.71; TNDM×47 yday $22.25 → 09:30 $22.16 -4.46; VOR×50 yday $23.01 → 09:30 $22.82 -9.50; MARA×1 yday $9.72 → 09:30 $9.36 -0.36; LDI×14 yday $0.88 → 09:30 $0.87 -0.07; BTBT×9 yday $1.60 → 09:30 $1.54 -0.54; TMC×2 yday $3.77 → 09:30 $3.72 -0.10; TGB×1 yday $8.77 → 09:30 $8.55 -0.22; DNN×3 yday $3.19 → 09:30 $3.11 -0.24; HNST×2 yday $4.70 → 09:30 $4.67 -0.06 |
| 2026-08-19 | -7.20 | $51.48 | BTSG×18, IREN×24, TPG×21, TGTX×22, SLS×94, HIMS×37, INO×1371, TNDM×47, VOR×50, MARA×1, LDI×14, BTBT×9, TMC×2, TGB×1, DNN×3, HNST×2 | $10,728.09 | +160.96 | — | — | $51.48 | $10,988.31 | BTSG×18, IREN×24, TPG×21, TGTX×22, SLS×94, HIMS×37, INO×1371, TNDM×47, VOR×50, MARA×1, LDI×14, BTBT×9, TMC×2, TGB×1, DNN×3, HNST×2 | 09:30 open · cash $51.48 (unchanged overnight, no fees) · equity $10,728.09 vs prior close $10,567.13 (+160.96) because holdings re-marked: BTSG×18 yday $59.50 → 09:30 $60.15 +11.70; IREN×24 yday $42.00 → 09:30 $41.41 -14.04; TPG×21 yday $52.02 → 09:30 $52.26 +5.04; TGTX×22 yday $50.26 → 09:30 $51.62 +29.92; SLS×94 yday $13.10 → 09:30 $13.46 +33.84; HIMS×37 yday $27.39 → 09:30 $27.55 +5.92; INO×1371 yday $1.20 → 09:30 $1.22 +27.42; TNDM×47 yday $23.73 → 09:30 $24.20 +22.09; VOR×50 yday $23.28 → 09:30 $24.05 +38.50; MARA×1 yday $8.96 → 09:30 $8.91 -0.05; LDI×14 yday $0.86 → 09:30 $0.88 +0.31; BTBT×9 yday $1.45 → 09:30 $1.42 -0.27; TMC×2 yday $3.92 → 09:30 $3.93 +0.02; TGB×1 yday $8.36 → 09:30 $8.70 +0.34; DNN×3 yday $3.15 → 09:30 $3.19 +0.12; HNST×2 yday $4.75 → 09:30 $4.80 +0.10 |
| 2026-08-20 | +1.12 | $51.48 | BTSG×18, IREN×24, TPG×21, TGTX×22, SLS×94, HIMS×37, INO×1371, TNDM×47, VOR×50, MARA×1, LDI×14, BTBT×9, TMC×2, TGB×1, DNN×3, HNST×2 | $10,904.20 | -84.11 | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | BTSG, IREN, TPG, TGTX, SLS, HIMS, INO, TNDM, VOR | $157.45 | $11,099.09 | MARA×1, LDI×14, BTBT×9, TMC×2, TGB×1, DNN×3, HNST×2, AG×65, BHP×14, CDE×65, HDSN×233, IAG×68, KGC×45, NFGC×771, WPM×9 | 09:30 open · cash $51.48 (unchanged overnight, no fees) · equity $10,904.20 vs prior close $10,988.31 (-84.11) because holdings re-marked: BTSG×18 yday $59.33 → 09:30 $58.64 -12.42; IREN×24 yday $42.84 → 09:30 $42.46 -9.12; TPG×21 yday $53.18 → 09:30 $53.06 -2.52; TGTX×22 yday $51.69 → 09:30 $51.65 -0.88; SLS×94 yday $13.85 → 09:30 $13.84 -0.94; HIMS×37 yday $31.09 → 09:30 $30.66 -15.91; INO×1371 yday $1.30 → 09:30 $1.30 +0.00; TNDM×47 yday $23.46 → 09:30 $23.11 -16.45; VOR×50 yday $23.58 → 09:30 $23.05 -26.50; MARA×1 yday $9.65 → 09:30 $10.21 +0.56; LDI×14 yday $0.88 → 09:30 $0.87 -0.07; BTBT×9 yday $1.40 → 09:30 $1.46 +0.50; TMC×2 yday $3.97 → 09:30 $3.92 -0.10; TGB×1 yday $8.47 → 09:30 $8.35 -0.12; DNN×3 yday $3.22 → 09:30 $3.20 -0.06; HNST×2 yday $5.02 → 09:30 $4.98 -0.08 |
| 2026-08-21 | +3.25 | $157.45 | MARA×1, LDI×14, BTBT×9, TMC×2, TGB×1, DNN×3, HNST×2, AG×65, BHP×14, CDE×65, HDSN×233, IAG×68, KGC×45, NFGC×771, WPM×9 | $11,391.10 | +292.01 | AUPH, ARCT, AUTL, CRDL, CYPH | MARA, LDI, BTBT | $85.89 | $11,392.87 | TMC×2, TGB×1, DNN×3, HNST×2, AG×65, BHP×14, CDE×65, HDSN×233, IAG×68, KGC×45, NFGC×771, WPM×9, AUPH×1, ARCT×2, AUTL×9, CRDL×12, CYPH×18 | 09:30 open · cash $157.45 (unchanged overnight, no fees) · equity $11,391.10 vs prior close $11,099.09 (+292.01) because holdings re-marked: MARA×1 yday $11.15 → 09:30 $11.70 +0.55; LDI×14 yday $0.87 → 09:30 $0.87 -0.04; BTBT×9 yday $1.59 → 09:30 $1.66 +0.58; TMC×2 yday $3.97 → 09:30 $4.10 +0.26; TGB×1 yday $8.69 → 09:30 $9.00 +0.31; DNN×3 yday $3.14 → 09:30 $3.23 +0.27; HNST×2 yday $4.96 → 09:30 $4.97 +0.02; AG×65 yday $21.19 → 09:30 $21.90 +46.15; BHP×14 yday $93.63 → 09:30 $95.72 +29.26; CDE×65 yday $21.11 → 09:30 $21.75 +41.60; HDSN×233 yday $5.57 → 09:30 $5.67 +23.30; IAG×68 yday $20.50 → 09:30 $21.17 +45.56; KGC×45 yday $31.43 → 09:30 $32.17 +33.30; NFGC×771 yday $1.75 → 09:30 $1.79 +30.84; WPM×9 yday $150.25 → 09:30 $154.70 +40.05 |
| 2026-08-24 | -5.17 | $85.89 | TMC×2, TGB×1, DNN×3, HNST×2, AG×65, BHP×14, CDE×65, HDSN×233, IAG×68, KGC×45, NFGC×771, WPM×9, AUPH×1, ARCT×2, AUTL×9, CRDL×12, CYPH×18 | $11,527.68 | +134.81 | — | TMC, TGB, DNN, HNST | $124.40 | $11,361.54 | AG×65, BHP×14, CDE×65, HDSN×233, IAG×68, KGC×45, NFGC×771, WPM×9, AUPH×1, ARCT×2, AUTL×9, CRDL×12, CYPH×18 | 09:30 open · cash $85.89 (unchanged overnight, no fees) · equity $11,527.68 vs prior close $11,392.87 (+134.81) because holdings re-marked: TMC×2 yday $4.79 → 09:30 $4.57 -0.44; TGB×1 yday $9.19 → 09:30 $9.26 +0.07; DNN×3 yday $3.50 → 09:30 $3.50 +0.00; HNST×2 yday $5.05 → 09:30 $5.05 +0.00; AG×65 yday $21.09 → 09:30 $21.47 +24.70; BHP×14 yday $97.03 → 09:30 $97.34 +4.34; CDE×65 yday $20.97 → 09:30 $21.26 +18.85; HDSN×233 yday $5.63 → 09:30 $5.69 +13.98; IAG×68 yday $21.14 → 09:30 $21.44 +20.40; KGC×45 yday $32.76 → 09:30 $33.21 +20.25; NFGC×771 yday $1.84 → 09:30 $1.86 +15.42; WPM×9 yday $157.78 → 09:30 $158.96 +10.62; AUPH×1 yday $16.65 → 09:30 $16.60 -0.05; ARCT×2 yday $13.45 → 09:30 $13.26 -0.38; AUTL×9 yday $2.41 → 09:30 $2.36 -0.45; CRDL×12 yday $1.86 → 09:30 $1.87 +0.12; CYPH×18 yday $1.42 → 09:30 $1.83 +7.38 |
| 2026-08-25 | +1.80 | $124.40 | AG×65, BHP×14, CDE×65, HDSN×233, IAG×68, KGC×45, NFGC×771, WPM×9, AUPH×1, ARCT×2, AUTL×9, CRDL×12, CYPH×18 | $11,436.21 | +74.67 | OCUL, CRMD, RZLT | — | $80.78 | $11,371.40 | AG×65, BHP×14, CDE×65, HDSN×233, IAG×68, KGC×45, NFGC×771, WPM×9, AUPH×1, ARCT×2, AUTL×9, CRDL×12, CYPH×18, OCUL×1, CRMD×2, RZLT×3 | 09:30 open · cash $124.40 (unchanged overnight, no fees) · equity $11,436.21 vs prior close $11,361.54 (+74.67) because holdings re-marked: AG×65 yday $20.57 → 09:30 $20.73 +10.40; BHP×14 yday $96.66 → 09:30 $95.95 -9.94; CDE×65 yday $20.49 → 09:30 $20.85 +23.40; HDSN×233 yday $5.57 → 09:30 $5.53 -9.32; IAG×68 yday $21.36 → 09:30 $21.63 +18.36; KGC×45 yday $32.47 → 09:30 $32.76 +13.05; NFGC×771 yday $1.90 → 09:30 $1.91 +7.71; WPM×9 yday $158.00 → 09:30 $160.00 +18.00; AUPH×1 yday $16.60 → 09:30 $16.71 +0.11; ARCT×2 yday $13.76 → 09:30 $14.34 +1.16; AUTL×9 yday $2.38 → 09:30 $2.32 -0.54; CRDL×12 yday $1.80 → 09:30 $1.90 +1.20; CYPH×18 yday $1.64 → 09:30 $1.70 +1.08 |
| 2026-08-26 | +2.02 | $80.78 | AG×65, BHP×14, CDE×65, HDSN×233, IAG×68, KGC×45, NFGC×771, WPM×9, AUPH×1, ARCT×2, AUTL×9, CRDL×12, CYPH×18, OCUL×1, CRMD×2, RZLT×3 | $11,371.40 | -0.00 | — | — | $80.78 | $11,435.76 | AG×65, BHP×14, CDE×65, HDSN×233, IAG×68, KGC×45, NFGC×771, WPM×9, AUPH×1, ARCT×2, AUTL×9, CRDL×12, CYPH×18, OCUL×1, CRMD×2, RZLT×3 | 09:30 open · cash $80.78 (unchanged overnight, no fees) · equity $11,371.40 vs prior close $11,371.40 (-0.00) because holdings re-marked: AG×65 yday $20.68 → 09:30 $20.68 +0.00; BHP×14 yday $96.05 → 09:30 $96.05 +0.00; CDE×65 yday $20.71 → 09:30 $20.71 +0.00; HDSN×233 yday $5.49 → 09:30 $5.49 +0.00; IAG×68 yday $21.48 → 09:30 $21.48 +0.00; KGC×45 yday $32.55 → 09:30 $32.55 +0.00; NFGC×771 yday $1.90 → 09:30 $1.90 +0.00; WPM×9 yday $158.25 → 09:30 $158.25 +0.00; AUPH×1 yday $16.71 → 09:30 $16.71 +0.00; ARCT×2 yday $14.21 → 09:30 $14.21 +0.00; AUTL×9 yday $2.34 → 09:30 $2.34 +0.00; CRDL×12 yday $1.90 → 09:30 $1.90 +0.00; CYPH×18 yday $1.64 → 09:30 $1.64 +0.00; OCUL×1 yday $10.92 → 09:30 $10.92 +0.00; CRMD×2 yday $8.28 → 09:30 $8.28 +0.00; RZLT×3 yday $5.29 → 09:30 $5.29 +0.00 |
| 2026-08-27 | — | $80.78 | AG×65, BHP×14, CDE×65, HDSN×233, IAG×68, KGC×45, NFGC×771, WPM×9, AUPH×1, ARCT×2, AUTL×9, CRDL×12, CYPH×18, OCUL×1, CRMD×2, RZLT×3 | $11,535.98 | +100.22 | RRC, CRK, MOS, SLI | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | $13.96 | $11,574.46 | AUPH×1, ARCT×2, AUTL×9, CRDL×12, CYPH×18, OCUL×1, CRMD×2, RZLT×3, RRC×69, CRK×201, MOS×114, SLI×1095 | 09:30 open · cash $80.78 (unchanged overnight, no fees) · equity $11,535.98 vs prior close $11,435.76 (+100.22) because holdings re-marked: AG×65 yday $20.68 → 09:30 $20.63 -3.25; BHP×14 yday $96.05 → 09:30 $96.99 +13.16; CDE×65 yday $20.71 → 09:30 $21.00 +18.85; HDSN×233 yday $5.49 → 09:30 $5.51 +4.66; IAG×68 yday $21.48 → 09:30 $21.64 +10.88; KGC×45 yday $32.55 → 09:30 $32.90 +15.75; NFGC×771 yday $1.90 → 09:30 $2.00 +77.10; WPM×9 yday $158.25 → 09:30 $160.93 +24.12; AUPH×1 yday $16.71 → 09:30 $16.60 -0.11; ARCT×2 yday $14.21 → 09:30 $15.35 +2.28; AUTL×9 yday $2.34 → 09:30 $2.41 +0.63; CRDL×12 yday $1.90 → 09:30 $2.03 +1.56; CYPH×18 yday $1.64 → 09:30 $1.60 -0.72; OCUL×1 yday $10.92 → 09:30 $10.79 -0.13; CRMD×2 yday $8.28 → 09:30 $8.60 +0.64; RZLT×3 yday $5.29 → 09:30 $5.01 -0.84 |
| 2026-08-28 | +0.75 | $13.96 | AUPH×1, ARCT×2, AUTL×9, CRDL×12, CYPH×18, OCUL×1, CRMD×2, RZLT×3, RRC×69, CRK×201, MOS×114, SLI×1095 | $11,522.97 | -51.49 | — | AUPH, ARCT, AUTL, CRDL, CYPH | $137.89 | $11,591.49 | OCUL×1, CRMD×2, RZLT×3, RRC×69, CRK×201, MOS×114, SLI×1095 | 09:30 open · cash $13.96 (unchanged overnight, no fees) · equity $11,522.97 vs prior close $11,574.46 (-51.49) because holdings re-marked: AUPH×1 yday $16.54 → 09:30 $16.47 -0.07; ARCT×2 yday $15.83 → 09:30 $15.74 -0.18; AUTL×9 yday $2.33 → 09:30 $2.32 -0.09; CRDL×12 yday $2.14 → 09:30 $2.09 -0.60; CYPH×18 yday $1.63 → 09:30 $1.75 +2.16; OCUL×1 yday $10.77 → 09:30 $10.63 -0.14; CRMD×2 yday $8.39 → 09:30 $8.49 +0.20; RZLT×3 yday $5.04 → 09:30 $5.07 +0.09; RRC×69 yday $41.55 → 09:30 $41.44 -7.59; CRK×201 yday $14.50 → 09:30 $14.42 -16.08; MOS×114 yday $24.16 → 09:30 $24.00 -18.24; SLI×1095 yday $2.61 → 09:30 $2.60 -10.95 |
| 2026-08-31 | -5.85 | $137.89 | OCUL×1, CRMD×2, RZLT×3, RRC×69, CRK×201, MOS×114, SLI×1095 | $11,397.79 | -193.70 | — | — | $137.89 | $11,437.41 | OCUL×1, CRMD×2, RZLT×3, RRC×69, CRK×201, MOS×114, SLI×1095 | 09:30 open · cash $137.89 (unchanged overnight, no fees) · equity $11,397.79 vs prior close $11,591.49 (-193.70) because holdings re-marked: OCUL×1 yday $10.82 → 09:30 $10.36 -0.46; CRMD×2 yday $8.31 → 09:30 $8.29 -0.04; RZLT×3 yday $4.98 → 09:30 $4.62 -1.08; RRC×69 yday $41.64 → 09:30 $41.11 -36.57; CRK×201 yday $14.62 → 09:30 $14.56 -12.06; MOS×114 yday $23.76 → 09:30 $23.75 -1.14; SLI×1095 yday $2.64 → 09:30 $2.51 -142.35 |
| 2026-09-01 | -6.30 | $137.89 | OCUL×1, CRMD×2, RZLT×3, RRC×69, CRK×201, MOS×114, SLI×1095 | $11,598.86 | +161.45 | — | OCUL, CRMD, RZLT | $178.49 | $11,745.47 | RRC×69, CRK×201, MOS×114, SLI×1095 | 09:30 open · cash $137.89 (unchanged overnight, no fees) · equity $11,598.86 vs prior close $11,437.41 (+161.45) because holdings re-marked: OCUL×1 yday $10.36 → 09:30 $10.49 +0.13; CRMD×2 yday $8.30 → 09:30 $8.26 -0.08; RZLT×3 yday $4.62 → 09:30 $4.69 +0.21; RRC×69 yday $41.78 → 09:30 $41.32 -31.74; CRK×201 yday $14.51 → 09:30 $14.31 -40.20; MOS×114 yday $23.78 → 09:30 $24.00 +25.08; SLI×1095 yday $2.51 → 09:30 $2.70 +208.05 |
| 2026-09-02 | -3.83 | $178.49 | RRC×69, CRK×201, MOS×114, SLI×1095 | $11,904.98 | +159.51 | — | — | $178.49 | $11,875.58 | RRC×69, CRK×201, MOS×114, SLI×1095 | 09:30 open · cash $178.49 (unchanged overnight, no fees) · equity $11,904.98 vs prior close $11,745.47 (+159.51) because holdings re-marked: RRC×69 yday $41.32 → 09:30 $41.94 +42.78; CRK×201 yday $14.90 → 09:30 $15.82 +184.92; MOS×114 yday $24.25 → 09:30 $23.94 -35.34; SLI×1095 yday $2.70 → 09:30 $2.67 -32.85 |
| 2026-09-03 | -0.90 | $178.49 | RRC×69, CRK×201, MOS×114, SLI×1095 | $11,781.44 | -94.14 | ATRC, HRMY, CABA, VSTM, RVTY | RRC, CRK, MOS, SLI | $121.67 | $12,363.41 | ATRC×47, HRMY×56, CABA×719, VSTM×305, RVTY×18 | 09:30 open · cash $178.49 (unchanged overnight, no fees) · equity $11,781.44 vs prior close $11,875.58 (-94.14) because holdings re-marked: RRC×69 yday $42.40 → 09:30 $42.10 -20.70; CRK×201 yday $16.02 → 09:30 $15.70 -64.32; MOS×114 yday $24.78 → 09:30 $24.70 -9.12; SLI×1095 yday $2.49 → 09:30 $2.49 +0.00 |
| 2026-09-04 | — | $121.67 | ATRC×47, HRMY×56, CABA×719, VSTM×305, RVTY×18 | $12,454.33 | +90.92 | NVAX, BVS | — | $71.34 | $12,217.88 | ATRC×47, HRMY×56, CABA×719, VSTM×305, RVTY×18, NVAX×2, BVS×2 | 09:30 open · cash $121.67 (unchanged overnight, no fees) · equity $12,454.33 vs prior close $12,363.41 (+90.92) because holdings re-marked: ATRC×47 yday $52.59 → 09:30 $52.88 +13.63; HRMY×56 yday $42.86 → 09:30 $42.93 +3.92; CABA×719 yday $3.57 → 09:30 $3.63 +43.14; VSTM×305 yday $8.02 → 09:30 $8.03 +3.05; RVTY×18 yday $130.94 → 09:30 $132.45 +27.18 |

## Fills (09:30 open snapshot, then buys / sells)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 18 | $59.80 | $2.04 | — | $8,921.56 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-5.3; leftover $1111.11 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 24 | $45.98 | $2.06 | — | $7,815.97 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+12.3; leftover $1111.11 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 21 | $50.62 | $2.05 | — | $6,750.83 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+6.2; leftover $1111.11 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TGTX` | 22 | $49.70 | $2.06 | — | $5,655.38 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-0.8; leftover $1111.11 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 94 | $11.70 | $2.27 | — | $4,553.31 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-0.8; leftover $1111.11 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 37 | $29.74 | $2.10 | — | $3,450.82 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-5.3; leftover $1111.11 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 1371 | $0.81 | $15.22 | — | $2,325.10 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+13.2; leftover $1111.11 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 47 | $23.33 | $2.13 | — | $1,226.46 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+19.7; leftover $1111.11 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `VOR` | 50 | $22.01 | $2.14 | — | $123.82 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+0.3; leftover $1111.11 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $123.82 | ▲ 09:30 equity $10,219.63 vs yday $10,195.74 (+23.89) | 09:30 open · cash $123.82 (unchanged overnight, no fees) · equity $10,219.63 vs prior close $10,195.74 (+23.89) because holdings re-marked: BTSG×18 yday $60.23 → 09:30 $59.65 -10.44; IREN×24 yday $44.76 → 09:30 $44.09 -16.08; TPG×21 yday $54.62 → 09:30 $55.29 +14.07; TGTX×22 yday $47.94 → 09:30 $47.27 -14.74; SLS×94 yday $12.36 → 09:30 $12.40 +3.76; HIMS×37 yday $28.77 → 09:30 $29.15 +14.06; INO×1371 yday $0.90 → 09:30 $0.93 +41.13; TNDM×47 yday $23.13 → 09:30 $22.92 -9.87; VOR×50 yday $23.29 → 09:30 $23.33 +2.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `MARA` | 1 | $9.01 | $0.09 | — | $114.71 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=-13.5; leftover $13.76 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 14 | $0.94 | $0.17 | — | $101.42 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+0.5; leftover $13.76 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 9 | $1.50 | $0.16 | — | $87.76 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+9.2; leftover $13.76 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $87.76 | ▼ 09:30 equity $10,410.12 vs yday $10,434.38 (-24.26) | 09:30 open · cash $87.76 (unchanged overnight, no fees) · equity $10,410.12 vs prior close $10,434.38 (-24.26) because holdings re-marked: BTSG×18 yday $61.71 → 09:30 $61.69 -0.36; IREN×24 yday $44.06 → 09:30 $45.23 +28.08; TPG×21 yday $53.03 → 09:30 $52.67 -7.56; TGTX×22 yday $48.74 → 09:30 $48.74 +0.00; SLS×94 yday $12.78 → 09:30 $12.78 +0.00; HIMS×37 yday $28.15 → 09:30 $28.14 -0.37; INO×1371 yday $1.09 → 09:30 $1.07 -27.42; TNDM×47 yday $22.72 → 09:30 $22.50 -10.34; VOR×50 yday $23.03 → 09:30 $22.91 -6.00; MARA×1 yday $9.20 → 09:30 $9.22 +0.02; LDI×14 yday $0.90 → 09:30 $0.91 +0.14; BTBT×9 yday $1.57 → 09:30 $1.52 -0.45 | — |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 2 | $4.05 | $0.09 | — | $79.57 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=-12.3; leftover $10.97 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `TGB` | 1 | $8.46 | $0.09 | — | $71.02 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+0.4; leftover $10.97 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 3 | $3.24 | $0.11 | — | $61.20 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=+0.3; leftover $10.97 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `HNST` | 2 | $4.81 | $0.10 | — | $51.48 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); ⚪; ret5=-11.4; leftover $10.97 | join🟡 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $51.48 | ▼ 09:30 equity $10,384.26 vs yday $10,512.60 (-128.34) | 09:30 open · cash $51.48 (unchanged overnight, no fees) · equity $10,384.26 vs prior close $10,512.60 (-128.34) because holdings re-marked: BTSG×18 yday $60.38 → 09:30 $60.00 -6.84; IREN×24 yday $44.90 → 09:30 $43.56 -32.16; TPG×21 yday $51.77 → 09:30 $51.77 +0.00; TGTX×22 yday $49.28 → 09:30 $49.28 +0.00; SLS×94 yday $13.00 → 09:30 $12.66 -31.96; HIMS×37 yday $28.61 → 09:30 $27.85 -28.12; INO×1371 yday $1.15 → 09:30 $1.14 -13.71; TNDM×47 yday $22.25 → 09:30 $22.16 -4.46; VOR×50 yday $23.01 → 09:30 $22.82 -9.50; MARA×1 yday $9.72 → 09:30 $9.36 -0.36; LDI×14 yday $0.88 → 09:30 $0.87 -0.07; BTBT×9 yday $1.60 → 09:30 $1.54 -0.54; TMC×2 yday $3.77 → 09:30 $3.72 -0.10; TGB×1 yday $8.77 → 09:30 $8.55 -0.22; DNN×3 yday $3.19 → 09:30 $3.11 -0.24; HNST×2 yday $4.70 → 09:30 $4.67 -0.06 | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $51.48 | ▲ 09:30 equity $10,728.09 vs yday $10,567.13 (+160.96) | 09:30 open · cash $51.48 (unchanged overnight, no fees) · equity $10,728.09 vs prior close $10,567.13 (+160.96) because holdings re-marked: BTSG×18 yday $59.50 → 09:30 $60.15 +11.70; IREN×24 yday $42.00 → 09:30 $41.41 -14.04; TPG×21 yday $52.02 → 09:30 $52.26 +5.04; TGTX×22 yday $50.26 → 09:30 $51.62 +29.92; SLS×94 yday $13.10 → 09:30 $13.46 +33.84; HIMS×37 yday $27.39 → 09:30 $27.55 +5.92; INO×1371 yday $1.20 → 09:30 $1.22 +27.42; TNDM×47 yday $23.73 → 09:30 $24.20 +22.09; VOR×50 yday $23.28 → 09:30 $24.05 +38.50; MARA×1 yday $8.96 → 09:30 $8.91 -0.05; LDI×14 yday $0.86 → 09:30 $0.88 +0.31; BTBT×9 yday $1.45 → 09:30 $1.42 -0.27; TMC×2 yday $3.92 → 09:30 $3.93 +0.02; TGB×1 yday $8.36 → 09:30 $8.70 +0.34; DNN×3 yday $3.15 → 09:30 $3.19 +0.12; HNST×2 yday $4.75 → 09:30 $4.80 +0.10 | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $51.48 | ▼ 09:30 equity $10,904.20 vs yday $10,988.31 (-84.11) | 09:30 open · cash $51.48 (unchanged overnight, no fees) · equity $10,904.20 vs prior close $10,988.31 (-84.11) because holdings re-marked: BTSG×18 yday $59.33 → 09:30 $58.64 -12.42; IREN×24 yday $42.84 → 09:30 $42.46 -9.12; TPG×21 yday $53.18 → 09:30 $53.06 -2.52; TGTX×22 yday $51.69 → 09:30 $51.65 -0.88; SLS×94 yday $13.85 → 09:30 $13.84 -0.94; HIMS×37 yday $31.09 → 09:30 $30.66 -15.91; INO×1371 yday $1.30 → 09:30 $1.30 +0.00; TNDM×47 yday $23.46 → 09:30 $23.11 -16.45; VOR×50 yday $23.58 → 09:30 $23.05 -26.50; MARA×1 yday $9.65 → 09:30 $10.21 +0.56; LDI×14 yday $0.88 → 09:30 $0.87 -0.07; BTBT×9 yday $1.40 → 09:30 $1.46 +0.50; TMC×2 yday $3.97 → 09:30 $3.92 -0.10; TGB×1 yday $8.47 → 09:30 $8.35 -0.12; DNN×3 yday $3.22 → 09:30 $3.20 -0.06; HNST×2 yday $5.02 → 09:30 $4.98 -0.08 | — |
| 2026-08-20 09:30 ET | **SELL** | `BTSG` | 18 | $58.64 | $2.06 | $-24.99 | $1,104.93 | ▼ -24.99 after sell → book $10,902.13; vs 09:30 mark -2.07 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `IREN` | 24 | $42.46 | $2.08 | $-88.62 | $2,121.89 | ▼ -88.62 after sell → book $10,900.05; vs 09:30 mark -2.08 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `TPG` | 21 | $53.06 | $2.07 | $+47.05 | $3,234.08 | ▲ +47.05 after sell → book $10,897.98; vs 09:30 mark -2.07 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `TGTX` | 22 | $51.65 | $2.08 | $+38.77 | $4,368.30 | ▲ +38.77 after sell → book $10,895.90; vs 09:30 mark -2.08 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `SLS` | 94 | $13.84 | $2.30 | $+196.59 | $5,666.96 | ▲ +196.59 after sell → book $10,893.60; vs 09:30 mark -2.30 | dropped from list after 5 sess (min 5) | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **SELL** | `HIMS` | 37 | $30.66 | $2.12 | $+29.82 | $6,799.26 | ▲ +29.82 after sell → book $10,891.48; vs 09:30 mark -2.12 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `INO` | 1371 | $1.30 | $17.93 | $+638.64 | $8,563.63 | ▲ +638.64 after sell → book $10,873.55; vs 09:30 mark -17.93 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `TNDM` | 47 | $23.11 | $2.15 | $-14.62 | $9,647.65 | ▼ -14.62 after sell → book $10,871.40; vs 09:30 mark -2.15 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `VOR` | 50 | $23.05 | $2.16 | $+47.70 | $10,797.99 | ▲ +47.70 after sell → book $10,869.24; vs 09:30 mark -2.16 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 65 | $20.55 | $2.19 | — | $9,460.06 | — | S≥+5: sizeup + more names; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+8.9; leftover $1349.75 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 14 | $91.01 | $2.03 | — | $8,183.89 | — | S≥+5: sizeup + more names; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+2.4; leftover $1349.75 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 65 | $20.65 | $2.19 | — | $6,839.45 | — | S≥+5: sizeup + more names; list flatten,yday_gainer,yday_mover,mover_buy; live flatten mover; 🔵; ⚪; ret5=+11.3; leftover $1349.75 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 233 | $5.77 | $3.01 | — | $5,492.04 | — | S≥+5: sizeup + more names; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+4.6; leftover $1349.75 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 68 | $19.63 | $2.19 | — | $4,155.00 | — | S≥+5: sizeup + more names; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+9.1; leftover $1349.75 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 45 | $29.63 | $2.12 | — | $2,819.53 | — | S≥+5: sizeup + more names; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+8.7; leftover $1349.75 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 771 | $1.75 | $9.95 | — | $1,460.33 | — | S≥+5: sizeup + more names; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+7.9; leftover $1349.75 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 9 | $144.54 | $2.02 | — | $157.45 | — | S≥+5: sizeup + more names; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+9.2; leftover $1349.75 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $157.45 | ▲ 09:30 equity $11,391.10 vs yday $11,099.09 (+292.01) | 09:30 open · cash $157.45 (unchanged overnight, no fees) · equity $11,391.10 vs prior close $11,099.09 (+292.01) because holdings re-marked: MARA×1 yday $11.15 → 09:30 $11.70 +0.55; LDI×14 yday $0.87 → 09:30 $0.87 -0.04; BTBT×9 yday $1.59 → 09:30 $1.66 +0.58; TMC×2 yday $3.97 → 09:30 $4.10 +0.26; TGB×1 yday $8.69 → 09:30 $9.00 +0.31; DNN×3 yday $3.14 → 09:30 $3.23 +0.27; HNST×2 yday $4.96 → 09:30 $4.97 +0.02; AG×65 yday $21.19 → 09:30 $21.90 +46.15; BHP×14 yday $93.63 → 09:30 $95.72 +29.26; CDE×65 yday $21.11 → 09:30 $21.75 +41.60; HDSN×233 yday $5.57 → 09:30 $5.67 +23.30; IAG×68 yday $20.50 → 09:30 $21.17 +45.56; KGC×45 yday $31.43 → 09:30 $32.17 +33.30; NFGC×771 yday $1.75 → 09:30 $1.79 +30.84; WPM×9 yday $150.25 → 09:30 $154.70 +40.05 | — |
| 2026-08-21 09:30 ET | **SELL** | `MARA` | 1 | $11.70 | $0.14 | $+2.46 | $169.01 | ▲ +2.46 after sell → book $11,390.96; vs 09:30 mark -0.14 | dropped from list after 5 sess (min 5) | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `LDI` | 14 | $0.87 | $0.18 | $-1.34 | $180.97 | ▼ -1.34 after sell → book $11,390.78; vs 09:30 mark -0.18 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `BTBT` | 9 | $1.66 | $0.20 | $+1.08 | $195.71 | ▲ +1.08 after sell → book $11,390.58; vs 09:30 mark -0.20 | dropped from list after 5 sess (min 5) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 1 | $17.20 | $0.17 | — | $178.34 | — | S≥+5: sizeup + more names; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+13.8; leftover $24.46 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 2 | $11.13 | $0.23 | — | $155.85 | — | S≥+5: sizeup + more names; list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+39.8; leftover $24.46 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 9 | $2.47 | $0.25 | — | $133.37 | — | S≥+5: sizeup + more names; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+10.8; leftover $24.46 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 12 | $1.93 | $0.27 | — | $109.94 | — | S≥+5: sizeup + more names; list flatten,mover_buy; live flatten mover; 🔵; ⚪; ret5=+10.2; leftover $24.46 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 18 | $1.32 | $0.29 | — | $85.89 | — | S≥+5: sizeup + more names; list flatten,yday_gainer,mover_buy; live flatten mover; 🔵; ⚪; ret5=+83.6; leftover $24.46 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $85.89 | ▲ 09:30 equity $11,527.68 vs yday $11,392.87 (+134.81) | 09:30 open · cash $85.89 (unchanged overnight, no fees) · equity $11,527.68 vs prior close $11,392.87 (+134.81) because holdings re-marked: TMC×2 yday $4.79 → 09:30 $4.57 -0.44; TGB×1 yday $9.19 → 09:30 $9.26 +0.07; DNN×3 yday $3.50 → 09:30 $3.50 +0.00; HNST×2 yday $5.05 → 09:30 $5.05 +0.00; AG×65 yday $21.09 → 09:30 $21.47 +24.70; BHP×14 yday $97.03 → 09:30 $97.34 +4.34; CDE×65 yday $20.97 → 09:30 $21.26 +18.85; HDSN×233 yday $5.63 → 09:30 $5.69 +13.98; IAG×68 yday $21.14 → 09:30 $21.44 +20.40; KGC×45 yday $32.76 → 09:30 $33.21 +20.25; NFGC×771 yday $1.84 → 09:30 $1.86 +15.42; WPM×9 yday $157.78 → 09:30 $158.96 +10.62; AUPH×1 yday $16.65 → 09:30 $16.60 -0.05; ARCT×2 yday $13.45 → 09:30 $13.26 -0.38; AUTL×9 yday $2.41 → 09:30 $2.36 -0.45; CRDL×12 yday $1.86 → 09:30 $1.87 +0.12; CYPH×18 yday $1.42 → 09:30 $1.83 +7.38 | — |
| 2026-08-24 09:30 ET | **SELL** | `TMC` | 2 | $4.57 | $0.12 | $+0.84 | $94.91 | ▲ +0.84 after sell → book $11,527.56; vs 09:30 mark -0.12 | dropped from list after 5 sess (min 5) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `TGB` | 1 | $9.26 | $0.12 | $+0.60 | $104.06 | ▲ +0.60 after sell → book $11,527.45; vs 09:30 mark -0.11 | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `DNN` | 3 | $3.50 | $0.13 | $+0.54 | $114.42 | ▲ +0.54 after sell → book $11,527.31; vs 09:30 mark -0.14 | dropped from list after 5 sess (min 5) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 judge🟡 ab🟡 peer🟢 vol🟢 buy🟢 |
| 2026-08-24 09:30 ET | **SELL** | `HNST` | 2 | $5.05 | $0.13 | $+0.25 | $124.40 | ▲ +0.25 after sell → book $11,527.19; vs 09:30 mark -0.12 | dropped from list after 5 sess (min 5) | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $124.40 | ▲ 09:30 equity $11,436.21 vs yday $11,361.54 (+74.67) | 09:30 open · cash $124.40 (unchanged overnight, no fees) · equity $11,436.21 vs prior close $11,361.54 (+74.67) because holdings re-marked: AG×65 yday $20.57 → 09:30 $20.73 +10.40; BHP×14 yday $96.66 → 09:30 $95.95 -9.94; CDE×65 yday $20.49 → 09:30 $20.85 +23.40; HDSN×233 yday $5.57 → 09:30 $5.53 -9.32; IAG×68 yday $21.36 → 09:30 $21.63 +18.36; KGC×45 yday $32.47 → 09:30 $32.76 +13.05; NFGC×771 yday $1.90 → 09:30 $1.91 +7.71; WPM×9 yday $158.00 → 09:30 $160.00 +18.00; AUPH×1 yday $16.60 → 09:30 $16.71 +0.11; ARCT×2 yday $13.76 → 09:30 $14.34 +1.16; AUTL×9 yday $2.38 → 09:30 $2.32 -0.54; CRDL×12 yday $1.80 → 09:30 $1.90 +1.20; CYPH×18 yday $1.64 → 09:30 $1.70 +1.08 | — |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 1 | $10.92 | $0.11 | — | $113.36 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ret5=+10.4; leftover $20.73 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 2 | $8.28 | $0.17 | — | $96.63 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+8.8; leftover $20.73 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 3 | $5.23 | $0.17 | — | $80.78 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); ret5=+10.7; leftover $20.73 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $80.78 | ▲ 09:30 equity $11,371.40 vs yday $11,371.40 (-0.00) | 09:30 open · cash $80.78 (unchanged overnight, no fees) · equity $11,371.40 vs prior close $11,371.40 (-0.00) because holdings re-marked: AG×65 yday $20.68 → 09:30 $20.68 +0.00; BHP×14 yday $96.05 → 09:30 $96.05 +0.00; CDE×65 yday $20.71 → 09:30 $20.71 +0.00; HDSN×233 yday $5.49 → 09:30 $5.49 +0.00; IAG×68 yday $21.48 → 09:30 $21.48 +0.00; KGC×45 yday $32.55 → 09:30 $32.55 +0.00; NFGC×771 yday $1.90 → 09:30 $1.90 +0.00; WPM×9 yday $158.25 → 09:30 $158.25 +0.00; AUPH×1 yday $16.71 → 09:30 $16.71 +0.00; ARCT×2 yday $14.21 → 09:30 $14.21 +0.00; AUTL×9 yday $2.34 → 09:30 $2.34 +0.00; CRDL×12 yday $1.90 → 09:30 $1.90 +0.00; CYPH×18 yday $1.64 → 09:30 $1.64 +0.00; OCUL×1 yday $10.92 → 09:30 $10.92 +0.00; CRMD×2 yday $8.28 → 09:30 $8.28 +0.00; RZLT×3 yday $5.29 → 09:30 $5.29 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $80.78 | ▲ 09:30 equity $11,535.98 vs yday $11,435.76 (+100.22) | 09:30 open · cash $80.78 (unchanged overnight, no fees) · equity $11,535.98 vs prior close $11,435.76 (+100.22) because holdings re-marked: AG×65 yday $20.68 → 09:30 $20.63 -3.25; BHP×14 yday $96.05 → 09:30 $96.99 +13.16; CDE×65 yday $20.71 → 09:30 $21.00 +18.85; HDSN×233 yday $5.49 → 09:30 $5.51 +4.66; IAG×68 yday $21.48 → 09:30 $21.64 +10.88; KGC×45 yday $32.55 → 09:30 $32.90 +15.75; NFGC×771 yday $1.90 → 09:30 $2.00 +77.10; WPM×9 yday $158.25 → 09:30 $160.93 +24.12; AUPH×1 yday $16.71 → 09:30 $16.60 -0.11; ARCT×2 yday $14.21 → 09:30 $15.35 +2.28; AUTL×9 yday $2.34 → 09:30 $2.41 +0.63; CRDL×12 yday $1.90 → 09:30 $2.03 +1.56; CYPH×18 yday $1.64 → 09:30 $1.60 -0.72; OCUL×1 yday $10.92 → 09:30 $10.79 -0.13; CRMD×2 yday $8.28 → 09:30 $8.60 +0.64; RZLT×3 yday $5.29 → 09:30 $5.01 -0.84 | — |
| 2026-08-27 09:30 ET | **SELL** | `AG` | 65 | $20.63 | $2.21 | $+0.81 | $1,419.52 | ▲ +0.81 after sell → book $11,533.77; vs 09:30 mark -2.21 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `BHP` | 14 | $96.99 | $2.05 | $+79.64 | $2,775.33 | ▲ +79.64 after sell → book $11,531.72; vs 09:30 mark -2.05 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `CDE` | 65 | $21.00 | $2.21 | $+18.36 | $4,138.12 | ▲ +18.36 after sell → book $11,529.51; vs 09:30 mark -2.21 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `HDSN` | 233 | $5.51 | $3.05 | $-66.64 | $5,418.90 | ▼ -66.64 after sell → book $11,526.46; vs 09:30 mark -3.05 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `IAG` | 68 | $21.64 | $2.22 | $+132.27 | $6,888.20 | ▲ +132.27 after sell → book $11,524.24; vs 09:30 mark -2.22 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `KGC` | 45 | $32.90 | $2.15 | $+142.88 | $8,366.55 | ▲ +142.88 after sell → book $11,522.09; vs 09:30 mark -2.15 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `NFGC` | 771 | $2.00 | $10.09 | $+172.72 | $9,898.47 | ▲ +172.72 after sell → book $11,512.01; vs 09:30 mark -10.08 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `WPM` | 9 | $160.93 | $2.04 | $+143.45 | $11,344.80 | ▲ +143.45 after sell → book $11,509.97; vs 09:30 mark -2.04 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 69 | $40.72 | $2.20 | — | $8,532.92 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); ret5=+1.8; leftover $2836.20 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 201 | $14.09 | $2.60 | — | $5,698.23 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); ret5=+1.1; leftover $2836.20 | join🟢 sector🔴 gen🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MOS` | 114 | $24.84 | $2.33 | — | $2,864.14 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); ret5=+13.0; leftover $2836.20 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟢 |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 1095 | $2.59 | $14.13 | — | $13.96 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); ret5=+4.2; leftover $2836.20 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13.96 | ▼ 09:30 equity $11,522.97 vs yday $11,574.46 (-51.49) | 09:30 open · cash $13.96 (unchanged overnight, no fees) · equity $11,522.97 vs prior close $11,574.46 (-51.49) because holdings re-marked: AUPH×1 yday $16.54 → 09:30 $16.47 -0.07; ARCT×2 yday $15.83 → 09:30 $15.74 -0.18; AUTL×9 yday $2.33 → 09:30 $2.32 -0.09; CRDL×12 yday $2.14 → 09:30 $2.09 -0.60; CYPH×18 yday $1.63 → 09:30 $1.75 +2.16; OCUL×1 yday $10.77 → 09:30 $10.63 -0.14; CRMD×2 yday $8.39 → 09:30 $8.49 +0.20; RZLT×3 yday $5.04 → 09:30 $5.07 +0.09; RRC×69 yday $41.55 → 09:30 $41.44 -7.59; CRK×201 yday $14.50 → 09:30 $14.42 -16.08; MOS×114 yday $24.16 → 09:30 $24.00 -18.24; SLI×1095 yday $2.61 → 09:30 $2.60 -10.95 | — |
| 2026-08-28 09:30 ET | **SELL** | `AUPH` | 1 | $16.47 | $0.19 | $-1.09 | $30.25 | ▼ -1.09 after sell → book $11,522.79; vs 09:30 mark -0.18 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `ARCT` | 2 | $15.74 | $0.34 | $+8.65 | $61.39 | ▲ +8.65 after sell → book $11,522.45; vs 09:30 mark -0.34 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `AUTL` | 9 | $2.32 | $0.26 | $-1.86 | $82.01 | ▼ -1.86 after sell → book $11,522.19; vs 09:30 mark -0.26 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `CRDL` | 12 | $2.09 | $0.31 | $+1.35 | $106.78 | ▲ +1.35 after sell → book $11,521.88; vs 09:30 mark -0.31 | dropped from list after 5 sess (min 5) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `CYPH` | 18 | $1.75 | $0.39 | $+7.06 | $137.89 | ▲ +7.06 after sell → book $11,521.49; vs 09:30 mark -0.39 | dropped from list after 5 sess (min 5) | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $137.89 | ▼ 09:30 equity $11,397.79 vs yday $11,591.49 (-193.70) | 09:30 open · cash $137.89 (unchanged overnight, no fees) · equity $11,397.79 vs prior close $11,591.49 (-193.70) because holdings re-marked: OCUL×1 yday $10.82 → 09:30 $10.36 -0.46; CRMD×2 yday $8.31 → 09:30 $8.29 -0.04; RZLT×3 yday $4.98 → 09:30 $4.62 -1.08; RRC×69 yday $41.64 → 09:30 $41.11 -36.57; CRK×201 yday $14.62 → 09:30 $14.56 -12.06; MOS×114 yday $23.76 → 09:30 $23.75 -1.14; SLI×1095 yday $2.64 → 09:30 $2.51 -142.35 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $137.89 | ▲ 09:30 equity $11,598.86 vs yday $11,437.41 (+161.45) | 09:30 open · cash $137.89 (unchanged overnight, no fees) · equity $11,598.86 vs prior close $11,437.41 (+161.45) because holdings re-marked: OCUL×1 yday $10.36 → 09:30 $10.49 +0.13; CRMD×2 yday $8.30 → 09:30 $8.26 -0.08; RZLT×3 yday $4.62 → 09:30 $4.69 +0.21; RRC×69 yday $41.78 → 09:30 $41.32 -31.74; CRK×201 yday $14.51 → 09:30 $14.31 -40.20; MOS×114 yday $23.78 → 09:30 $24.00 +25.08; SLI×1095 yday $2.51 → 09:30 $2.70 +208.05 | — |
| 2026-09-01 09:30 ET | **SELL** | `OCUL` | 1 | $10.49 | $0.13 | $-0.67 | $148.26 | ▼ -0.67 after sell → book $11,598.74; vs 09:30 mark -0.12 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `CRMD` | 2 | $8.26 | $0.19 | $-0.40 | $164.59 | ▼ -0.40 after sell → book $11,598.55; vs 09:30 mark -0.19 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `RZLT` | 3 | $4.69 | $0.17 | $-1.96 | $178.49 | ▼ -1.96 after sell → book $11,598.38; vs 09:30 mark -0.17 | dropped from list after 5 sess (min 5) | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $178.49 | ▲ 09:30 equity $11,904.98 vs yday $11,745.47 (+159.51) | 09:30 open · cash $178.49 (unchanged overnight, no fees) · equity $11,904.98 vs prior close $11,745.47 (+159.51) because holdings re-marked: RRC×69 yday $41.32 → 09:30 $41.94 +42.78; CRK×201 yday $14.90 → 09:30 $15.82 +184.92; MOS×114 yday $24.25 → 09:30 $23.94 -35.34; SLI×1095 yday $2.70 → 09:30 $2.67 -32.85 | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $178.49 | ▼ 09:30 equity $11,781.44 vs yday $11,875.58 (-94.14) | 09:30 open · cash $178.49 (unchanged overnight, no fees) · equity $11,781.44 vs prior close $11,875.58 (-94.14) because holdings re-marked: RRC×69 yday $42.40 → 09:30 $42.10 -20.70; CRK×201 yday $16.02 → 09:30 $15.70 -64.32; MOS×114 yday $24.78 → 09:30 $24.70 -9.12; SLI×1095 yday $2.49 → 09:30 $2.49 +0.00 | — |
| 2026-09-03 09:30 ET | **SELL** | `RRC` | 69 | $42.10 | $2.23 | $+90.79 | $3,081.15 | ▲ +90.79 after sell → book $11,779.20; vs 09:30 mark -2.24 | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `CRK` | 201 | $15.70 | $2.66 | $+318.36 | $6,234.20 | ▲ +318.36 after sell → book $11,776.55; vs 09:30 mark -2.65 | dropped from list after 5 sess (min 5) | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-03 09:30 ET | **SELL** | `MOS` | 114 | $24.70 | $2.37 | $-20.67 | $9,047.62 | ▼ -20.67 after sell → book $11,774.17; vs 09:30 mark -2.38 | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `SLI` | 1095 | $2.49 | $14.33 | $-137.95 | $11,759.84 | ▼ -137.95 after sell → book $11,759.84; vs 09:30 mark -14.33 | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 47 | $49.76 | $2.13 | — | $9,418.99 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+10.6; leftover $2351.97 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 56 | $41.31 | $2.16 | — | $7,103.48 | — | S≥+5: sizeup + more names; list flatten,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+0.1; leftover $2351.97 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 719 | $3.27 | $9.28 | — | $4,743.07 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+13.8; leftover $2351.97 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 305 | $7.70 | $3.93 | — | $2,390.64 | — | S≥+5: sizeup + more names; list flatten,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+4.7; leftover $2351.97 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 18 | $125.94 | $2.04 | — | $121.67 | — | S≥+5: sizeup + more names; list flatten,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+6.8; leftover $2351.97 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $121.67 | ▲ 09:30 equity $12,454.33 vs yday $12,363.41 (+90.92) | 09:30 open · cash $121.67 (unchanged overnight, no fees) · equity $12,454.33 vs prior close $12,363.41 (+90.92) because holdings re-marked: ATRC×47 yday $52.59 → 09:30 $52.88 +13.63; HRMY×56 yday $42.86 → 09:30 $42.93 +3.92; CABA×719 yday $3.57 → 09:30 $3.63 +43.14; VSTM×305 yday $8.02 → 09:30 $8.03 +3.05; RVTY×18 yday $130.94 → 09:30 $132.45 +27.18 | — |
| 2026-09-04 09:30 ET | **BUY** | `NVAX` | 2 | $10.41 | $0.21 | — | $100.64 | — | S≥+5: sizeup + more names; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); ⚪; ret5=+11.1; leftover $30.42 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BVS` | 2 | $14.50 | $0.30 | — | $71.34 | — | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+0.8; leftover $30.42 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |

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
| 2026-08-14 | `TLN` | cash | leftover split 13.76 < 1 share @ 359.83 |
| 2026-08-14 | `VST` | cash | leftover split 13.76 < 1 share @ 146.90 |
| 2026-08-14 | `NRG` | cash | leftover split 13.76 < 1 share @ 120.00 |
| 2026-08-14 | `DAVE` | cash | leftover split 13.76 < 1 share @ 330.91 |
| 2026-08-14 | `SLG` | cash | leftover split 13.76 < 1 share @ 57.61 |
| 2026-08-14 | `BETR` | cash | leftover split 13.76 < 1 share @ 14.80 |
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
| 2026-08-17 | `DVN` | cash | leftover split 10.97 < 1 share @ 46.18 |
| 2026-08-17 | `EOG` | cash | leftover split 10.97 < 1 share @ 142.77 |
| 2026-08-17 | `FANG` | cash | leftover split 10.97 < 1 share @ 202.70 |
| 2026-08-17 | `ELF` | cash | leftover split 10.97 < 1 share @ 90.54 |
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
| 2026-08-25 | `MOS` | cash | leftover split 20.73 < 1 share @ 24.00 |
| 2026-08-25 | `INSP` | cash | leftover split 20.73 < 1 share @ 61.47 |
| 2026-08-25 | `HCA` | cash | leftover split 20.73 < 1 share @ 429.24 |
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
| 2026-09-04 | `ASND` | cash | leftover split 30.42 < 1 share @ 266.94 |
| 2026-09-04 | `OSCR` | cash | leftover split 30.42 < 1 share @ 30.65 |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `ATRC` | 47 | 2026-09-03 @ $49.76 | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+10.6; leftover $2351.97 |
| `HRMY` | 56 | 2026-09-03 @ $41.31 | S≥+5: sizeup + more names; list flatten,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+0.1; leftover $2351.97 |
| `CABA` | 719 | 2026-09-03 @ $3.27 | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+13.8; leftover $2351.97 |
| `VSTM` | 305 | 2026-09-03 @ $7.70 | S≥+5: sizeup + more names; list flatten,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+4.7; leftover $2351.97 |
| `RVTY` | 18 | 2026-09-03 @ $125.94 | S≥+5: sizeup + more names; list flatten,mover_buy; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+6.8; leftover $2351.97 |
| `NVAX` | 2 | 2026-09-04 @ $10.41 | S≥+5: sizeup + more names; list flatten,ohlc_hot; wish-list (live io HOLD — not a ticket); ⚪; ret5=+11.1; leftover $30.42 |
| `BVS` | 2 | 2026-09-04 @ $14.50 | S≥+5: sizeup + more names; list flatten; wish-list (live io HOLD — not a ticket); 🔵; ⚪; ret5=+0.8; leftover $30.42 |
